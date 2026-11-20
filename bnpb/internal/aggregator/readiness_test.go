package aggregator

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/httpserver"
)

func TestReadyProbeAdmittedWithStalledDiagnosticsAndBusyData(t *testing.T) {
	authorityStarted := make(chan struct{}, 3)
	host := &memoryHostFence{}
	app := application{store: &Store{
		readTimeout:   30 * time.Second,
		storeIdentity: storeIdentity{role: "control"},
		authority: &PollAuthority{
			kubernetes: &stalledReadinessAuthority{started: authorityStarted},
			host:       host,
		},
	}}
	dataStarted := make(chan struct{})
	probeEntered := false
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ready", app.ready)
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, request *http.Request) {
		probeEntered = true
		app.readyProbe(w, request)
	})
	mux.HandleFunc("GET /data", func(_ http.ResponseWriter, request *http.Request) {
		close(dataStarted)
		<-request.Context().Done()
	})
	handler := httpserver.Handler(mux, quietLogger(), 1)

	ctx, cancel := context.WithCancel(t.Context())
	var inFlight sync.WaitGroup
	defer func() { cancel(); inFlight.Wait() }()
	serve := func(path string) int {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequestWithContext(ctx, http.MethodGet, path, nil))
		return response.Code
	}
	inFlight.Go(func() { serve("/data") })
	waitForCompletion(t, dataStarted)
	for range 2 {
		inFlight.Go(func() { serve("/ready") })
		waitForCompletion(t, authorityStarted)
	}

	if status := serve("/ready"); status != http.StatusTooManyRequests {
		t.Fatalf("saturated diagnostic readiness = HTTP %d, want 429", status)
	}
	if status := serve("/readyz"); status != http.StatusServiceUnavailable || !probeEntered {
		t.Fatalf("routing readiness without a data primary = HTTP %d, handler admitted = %t; want admitted 503", status, probeEntered)
	}
	select {
	case <-authorityStarted:
		t.Fatal("routing readiness checked Kubernetes polling authority")
	default:
	}
	if host.currentCalls() != 0 {
		t.Fatal("routing readiness checked the host fence")
	}
}

type stalledReadinessAuthority struct {
	memoryAuthorityKubernetes
	started chan<- struct{}
}

func (client *stalledReadinessAuthority) Read(ctx context.Context) (authorityRecord, string, error) {
	client.started <- struct{}{}
	<-ctx.Done()
	return authorityRecord{}, "", ctx.Err()
}
