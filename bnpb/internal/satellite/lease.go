package satellite

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/logging"
)

const serviceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"

// Kubernetes MicroTime.
const microTime = "2006-01-02T15:04:05.000000Z07:00"

type elector interface {
	Run(ctx context.Context, lead func(context.Context))
	Leading() bool
}

// Without a lease (local and Compose runs) this replica always leads.
type soleElector struct{ leading atomic.Bool }

func (sole *soleElector) Run(ctx context.Context, lead func(context.Context)) {
	sole.leading.Store(true)
	defer sole.leading.Store(false)
	lead(ctx)
}

func (sole *soleElector) Leading() bool { return sole.leading.Load() }

type leaseSpec struct {
	HolderIdentity       *string `json:"holderIdentity,omitempty"`
	LeaseDurationSeconds *int32  `json:"leaseDurationSeconds,omitempty"`
	AcquireTime          *string `json:"acquireTime,omitempty"`
	RenewTime            *string `json:"renewTime,omitempty"`
	LeaseTransitions     *int32  `json:"leaseTransitions,omitempty"`
}

type leaseObject struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		Name            string `json:"name"`
		Namespace       string `json:"namespace,omitempty"`
		ResourceVersion string `json:"resourceVersion,omitempty"`
	} `json:"metadata"`
	Spec leaseSpec `json:"spec"`
}

type leaseElector struct {
	cfg     LeaseConfig
	api     string
	client  *http.Client
	token   func() (string, error)
	logger  *slog.Logger
	now     func() time.Time
	leading atomic.Bool
}

func newInClusterElector(cfg LeaseConfig, logger *slog.Logger) (*leaseElector, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, errors.New("BNPB_SATELLITE_LEASE_NAME is set but the pod is not running in Kubernetes")
	}
	authority, err := os.ReadFile(serviceAccountDir + "/ca.crt")
	if err != nil {
		return nil, fmt.Errorf("read service account CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(authority) {
		return nil, errors.New("service account CA is not PEM")
	}
	client := &http.Client{Timeout: cfg.RenewEvery, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}}
	// The token file is rotated by the kubelet, so it is re-read on every call.
	token := func() (string, error) {
		raw, err := os.ReadFile(serviceAccountDir + "/token")
		return strings.TrimSpace(string(raw)), err
	}
	return newLeaseElector(cfg, "https://"+net.JoinHostPort(host, port), client, token, logger), nil
}

func newLeaseElector(cfg LeaseConfig, api string, client *http.Client, token func() (string, error), logger *slog.Logger) *leaseElector {
	return &leaseElector{cfg: cfg, api: api, client: client, token: token, logger: logger, now: time.Now}
}

func (elector *leaseElector) Leading() bool { return elector.leading.Load() }

// Leads only between a successful acquire and the step-down deadline, which falls before others may take over.
func (elector *leaseElector) Run(ctx context.Context, lead func(context.Context)) {
	if elector.cfg.Duration < time.Second || elector.cfg.StepDownAfter >= elector.cfg.Duration || elector.cfg.RenewEvery >= elector.cfg.StepDownAfter {
		logging.Event(ctx, elector.logger, logging.Record{Level: slog.LevelError, Event: "lease_misconfigured", Message: "lease timings cannot guarantee a single holder"})
		return
	}
	ticker := time.NewTicker(elector.cfg.RenewEvery)
	defer ticker.Stop()
	for ctx.Err() == nil {
		if elector.tryAcquireOrRenew(ctx) {
			elector.leadTerm(ctx, ticker, lead)
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (elector *leaseElector) leadTerm(ctx context.Context, ticker *time.Ticker, lead func(context.Context)) {
	termCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	elector.leading.Store(true)
	logging.Event(ctx, elector.logger, logging.Record{Level: slog.LevelInfo, Event: "lease_acquired", Message: "this replica now holds the feed"}, "identity", elector.cfg.Identity)
	go func() { defer close(done); lead(termCtx) }()
	lastRenew := elector.now()
	for {
		select {
		case <-ctx.Done():
		case <-done:
		case <-ticker.C:
			if elector.tryAcquireOrRenew(ctx) {
				lastRenew = elector.now()
				continue
			}
			if elector.now().Sub(lastRenew) < elector.cfg.StepDownAfter {
				continue
			}
		}
		break
	}
	cancel()
	<-done
	elector.leading.Store(false)
	logging.Event(ctx, elector.logger, logging.Record{Level: slog.LevelWarn, Event: "lease_released", Message: "this replica stopped holding the feed"}, "identity", elector.cfg.Identity)
}

func (elector *leaseElector) path() string {
	return elector.api + "/apis/coordination.k8s.io/v1/namespaces/" + elector.cfg.Namespace + "/leases"
}

func (elector *leaseElector) tryAcquireOrRenew(ctx context.Context) bool {
	now := elector.now().UTC()
	current, status, err := elector.request(ctx, http.MethodGet, elector.path()+"/"+elector.cfg.Name, nil)
	if err != nil {
		return false
	}
	stamp, seconds := now.Format(microTime), int32(elector.cfg.Duration/time.Second)
	identity := elector.cfg.Identity
	if status == http.StatusNotFound {
		lease := leaseObject{APIVersion: "coordination.k8s.io/v1", Kind: "Lease"}
		lease.Metadata.Name, lease.Metadata.Namespace = elector.cfg.Name, elector.cfg.Namespace
		zero := int32(0)
		lease.Spec = leaseSpec{HolderIdentity: &identity, LeaseDurationSeconds: &seconds, AcquireTime: &stamp, RenewTime: &stamp, LeaseTransitions: &zero}
		_, status, err = elector.request(ctx, http.MethodPost, elector.path(), &lease)
		return err == nil && status == http.StatusCreated
	}
	if status != http.StatusOK || current == nil {
		return false
	}
	holder := ""
	if current.Spec.HolderIdentity != nil {
		holder = *current.Spec.HolderIdentity
	}
	if holder != identity && !leaseExpired(current.Spec, now) {
		return false
	}
	if holder != identity {
		transitions := int32(1)
		if current.Spec.LeaseTransitions != nil {
			transitions += *current.Spec.LeaseTransitions
		}
		current.Spec.HolderIdentity, current.Spec.AcquireTime, current.Spec.LeaseTransitions = &identity, &stamp, &transitions
	}
	current.Spec.RenewTime, current.Spec.LeaseDurationSeconds = &stamp, &seconds
	// resourceVersion makes the update conditional: a concurrent change answers 409.
	_, status, err = elector.request(ctx, http.MethodPut, elector.path()+"/"+elector.cfg.Name, current)
	return err == nil && status == http.StatusOK
}

func leaseExpired(spec leaseSpec, now time.Time) bool {
	if spec.HolderIdentity == nil || *spec.HolderIdentity == "" || spec.RenewTime == nil || spec.LeaseDurationSeconds == nil {
		return true
	}
	renewed, err := time.Parse(microTime, *spec.RenewTime)
	if err != nil {
		renewed, err = time.Parse(time.RFC3339Nano, *spec.RenewTime)
	}
	return err != nil || !now.Before(renewed.Add(time.Duration(*spec.LeaseDurationSeconds)*time.Second))
}

func (elector *leaseElector) request(ctx context.Context, method, address string, body *leaseObject) (*leaseObject, int, error) {
	var payload io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, 0, err
		}
		payload = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, address, payload)
	if err != nil {
		return nil, 0, err
	}
	token, err := elector.token()
	if err != nil {
		return nil, 0, err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := elector.client.Do(request)
	if err != nil {
		return nil, 0, err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil || response.StatusCode != http.StatusOK {
		return nil, response.StatusCode, err
	}
	var lease leaseObject
	if err := json.Unmarshal(raw, &lease); err != nil {
		return nil, response.StatusCode, err
	}
	return &lease, response.StatusCode, nil
}
