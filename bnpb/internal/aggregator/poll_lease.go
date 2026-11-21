package aggregator

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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/logging"
)

const serviceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"

// Kubernetes MicroTime.
const microTime = "2006-01-02T15:04:05.000000Z07:00"

var pollLeading = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "bnpb_aggregator_poll_leading",
	Help: "1 while this Aggregator replica polls BMKG, PVMBG and Sensor Pantai.",
})

type PollLeaseConfig struct {
	Name, Namespace, Identity           string
	Duration, RenewEvery, StepDownAfter time.Duration
}

func loadPollLeaseConfig(lookup func(string) (string, bool), routed bool) (PollLeaseConfig, error) {
	cfg := PollLeaseConfig{}
	cfg.Name, _ = lookup("BNPB_AGGREGATOR_LEASE_NAME")
	if cfg.Name == "" {
		return cfg, nil
	}
	if routed {
		return PollLeaseConfig{}, errors.New("BNPB_AGGREGATOR_LEASE_NAME: sharded stores use the poll authority instead; leave it empty")
	}
	cfg.Namespace, _ = lookup("BNPB_AGGREGATOR_LEASE_NAMESPACE")
	if cfg.Namespace == "" {
		if raw, err := os.ReadFile(serviceAccountDir + "/namespace"); err == nil {
			cfg.Namespace = strings.TrimSpace(string(raw))
		}
	}
	cfg.Identity, _ = lookup("BNPB_AGGREGATOR_IDENTITY")
	if cfg.Identity == "" {
		cfg.Identity, _ = os.Hostname()
	}
	if cfg.Namespace == "" || cfg.Identity == "" {
		return PollLeaseConfig{}, errors.New("BNPB_AGGREGATOR_LEASE_NAME: needs a namespace and an identity")
	}
	cfg.Duration, cfg.RenewEvery, cfg.StepDownAfter = 15*time.Second, 5*time.Second, 10*time.Second
	return cfg, nil
}

// Identifies one holder's term; the store refuses poll writes from any older term.
type pollLeaseToken struct {
	leaseUID   string
	generation int64
	holder     string
}

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
		UID             string `json:"uid,omitempty"`
		ResourceVersion string `json:"resourceVersion,omitempty"`
	} `json:"metadata"`
	Spec leaseSpec `json:"spec"`
}

type pollLeaseElector struct {
	cfg     PollLeaseConfig
	api     string
	client  *http.Client
	token   func() (string, error)
	logger  *slog.Logger
	now     func() time.Time
	leading atomic.Bool
}

func newInClusterPollElector(cfg PollLeaseConfig, logger *slog.Logger) (*pollLeaseElector, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, errors.New("BNPB_AGGREGATOR_LEASE_NAME is set but the pod is not running in Kubernetes")
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
	return newPollLeaseElector(cfg, "https://"+net.JoinHostPort(host, port), client, token, logger), nil
}

func newPollLeaseElector(cfg PollLeaseConfig, api string, client *http.Client, token func() (string, error), logger *slog.Logger) *pollLeaseElector {
	return &pollLeaseElector{cfg: cfg, api: api, client: client, token: token, logger: logger, now: time.Now}
}

func (elector *pollLeaseElector) Leading() bool { return elector.leading.Load() }

// Leads only between a successful acquire and the step-down deadline, which falls before others may take over.
func (elector *pollLeaseElector) Run(ctx context.Context, lead func(context.Context, pollLeaseToken)) {
	if elector.cfg.Duration < time.Second || elector.cfg.StepDownAfter >= elector.cfg.Duration || elector.cfg.RenewEvery >= elector.cfg.StepDownAfter {
		logging.Event(ctx, elector.logger, logging.Record{Level: slog.LevelError, Event: "lease_misconfigured", Message: "lease timings cannot guarantee a single holder"})
		return
	}
	ticker := time.NewTicker(elector.cfg.RenewEvery)
	defer ticker.Stop()
	for ctx.Err() == nil {
		if token, ok := elector.tryAcquireOrRenew(ctx); ok {
			elector.leadTerm(ctx, ticker, token, lead)
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (elector *pollLeaseElector) leadTerm(ctx context.Context, ticker *time.Ticker, token pollLeaseToken, lead func(context.Context, pollLeaseToken)) {
	termCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	elector.leading.Store(true)
	pollLeading.Set(1)
	logging.Event(ctx, elector.logger, logging.Record{Level: slog.LevelInfo, Event: "lease_acquired", Message: "this replica now polls the sources"},
		"identity", elector.cfg.Identity, "lease_generation", token.generation)
	go func() { defer close(done); lead(termCtx, token) }()
	lastRenew := elector.now()
	for {
		select {
		case <-ctx.Done():
		case <-done:
		case <-ticker.C:
			renewed, ok := elector.tryAcquireOrRenew(ctx)
			if ok && renewed == token {
				lastRenew = elector.now()
				continue
			}
			// A renewal under a different term means someone else held the lease in between.
			if !ok && elector.now().Sub(lastRenew) < elector.cfg.StepDownAfter {
				continue
			}
		}
		break
	}
	cancel()
	<-done
	elector.leading.Store(false)
	pollLeading.Set(0)
	logging.Event(ctx, elector.logger, logging.Record{Level: slog.LevelWarn, Event: "lease_released", Message: "this replica stopped polling the sources"},
		"identity", elector.cfg.Identity, "lease_generation", token.generation)
}

func (elector *pollLeaseElector) path() string {
	return elector.api + "/apis/coordination.k8s.io/v1/namespaces/" + elector.cfg.Namespace + "/leases"
}

func (elector *pollLeaseElector) tryAcquireOrRenew(ctx context.Context) (pollLeaseToken, bool) {
	now := elector.now().UTC()
	current, status, err := elector.request(ctx, http.MethodGet, elector.path()+"/"+elector.cfg.Name, nil)
	if err != nil {
		return pollLeaseToken{}, false
	}
	stamp, seconds := now.Format(microTime), int32(elector.cfg.Duration/time.Second)
	identity := elector.cfg.Identity
	if status == http.StatusNotFound {
		lease := leaseObject{APIVersion: "coordination.k8s.io/v1", Kind: "Lease"}
		lease.Metadata.Name, lease.Metadata.Namespace = elector.cfg.Name, elector.cfg.Namespace
		zero := int32(0)
		lease.Spec = leaseSpec{HolderIdentity: &identity, LeaseDurationSeconds: &seconds, AcquireTime: &stamp, RenewTime: &stamp, LeaseTransitions: &zero}
		created, status, err := elector.request(ctx, http.MethodPost, elector.path(), &lease)
		return elector.termOf(created), err == nil && status == http.StatusCreated && created != nil
	}
	if status != http.StatusOK || current == nil {
		return pollLeaseToken{}, false
	}
	holder := ""
	if current.Spec.HolderIdentity != nil {
		holder = *current.Spec.HolderIdentity
	}
	if holder != identity && !leaseExpired(current.Spec, now) {
		return pollLeaseToken{}, false
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
	updated, status, err := elector.request(ctx, http.MethodPut, elector.path()+"/"+elector.cfg.Name, current)
	return elector.termOf(updated), err == nil && status == http.StatusOK && updated != nil
}

func (elector *pollLeaseElector) termOf(lease *leaseObject) pollLeaseToken {
	if lease == nil {
		return pollLeaseToken{}
	}
	token := pollLeaseToken{leaseUID: lease.Metadata.UID, holder: elector.cfg.Identity}
	if lease.Spec.LeaseTransitions != nil {
		token.generation = int64(*lease.Spec.LeaseTransitions)
	}
	return token
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

func (elector *pollLeaseElector) request(ctx context.Context, method, address string, body *leaseObject) (*leaseObject, int, error) {
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
	if err != nil || (response.StatusCode != http.StatusOK && response.StatusCode != http.StatusCreated) {
		return nil, response.StatusCode, err
	}
	var lease leaseObject
	if err := json.Unmarshal(raw, &lease); err != nil {
		return nil, response.StatusCode, err
	}
	return &lease, response.StatusCode, nil
}
