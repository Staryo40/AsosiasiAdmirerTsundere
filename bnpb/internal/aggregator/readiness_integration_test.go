//go:build integration

package aggregator

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestReadinessIntegrationWrongRoleAndPartialAccess(t *testing.T) {
	store := openRoutedM2Store(t, routedM2Config(t))
	store.shards[1].storeIdentity = storeIdentity{role: "control"}
	store.shards[2].read.Close()

	report := store.readiness(t.Context())
	if !report.Ready || report.PollOwner != "unavailable" {
		t.Fatalf("wrong-role and partial stores affected overall readiness: %+v", report)
	}
	want := []readinessUnit{
		{ID: 0, Unit: "shard-0", Status: "ready"},
		{ID: 1, Unit: "shard-1", Status: "unavailable"},
		{ID: 2, Unit: "shard-2", Status: "unavailable"},
	}
	if !reflect.DeepEqual(report.LogicalShards, want) {
		t.Fatalf("logical readiness = %+v, want %+v", report.LogicalShards, want)
	}
	app := application{store: store}
	probe := httptest.NewRecorder()
	app.readyProbe(probe, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if probe.Code != http.StatusOK || probe.Body.Len() != 0 {
		t.Fatalf("partial routing readiness = HTTP %d %q, want empty 200", probe.Code, probe.Body)
	}
	store.shards[0].read.Close()
	response := httptest.NewRecorder()
	app.ready(response, httptest.NewRequest(http.MethodGet, "/ready", nil))
	var unavailable readinessReport
	if err := json.Unmarshal(response.Body.Bytes(), &unavailable); err != nil {
		t.Fatalf("decode unavailable readiness response: %v", err)
	}
	if response.Code != http.StatusServiceUnavailable || unavailable.Ready {
		t.Fatalf("no verified primary readiness = HTTP %d %+v", response.Code, unavailable)
	}
	probe = httptest.NewRecorder()
	app.readyProbe(probe, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if probe.Code != http.StatusServiceUnavailable || probe.Body.Len() != 0 {
		t.Fatalf("routing readiness without a primary = HTTP %d %q, want empty 503", probe.Code, probe.Body)
	}
}

func TestReadinessProbeDoesNotWaitForBusyShard(t *testing.T) {
	cfg := routedM2Config(t)
	cfg.DBReadMaxConns = 1
	cfg.DBReadTimeout = time.Second
	store := openRoutedM2Store(t, cfg)
	held, err := store.shards[1].read.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()
	store.shards[2].read.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	response := httptest.NewRecorder()
	(application{store: store}).readyProbe(response, httptest.NewRequestWithContext(ctx, http.MethodGet, "/readyz", nil))
	if response.Code != http.StatusOK || ctx.Err() != nil {
		t.Fatalf("routing readiness with one verified primary = HTTP %d, request context: %v", response.Code, ctx.Err())
	}
}

func TestReadinessIntegrationOwnerOutageDoesNotGateHealthyFollower(t *testing.T) {
	store := openRoutedM2Store(t, routedM2Config(t))
	kubernetes, host := liveAuthorityFixture("remote-owner", 7)
	store.authority = &PollAuthority{
		owner: "healthy-follower", ownerDomain: authorityTestDomain,
		domains: []string{authorityTestDomain}, kubernetes: kubernetes, host: host,
	}

	checkReadiness := func() (int, readinessReport) {
		response := httptest.NewRecorder()
		(application{store: store}).ready(response, httptest.NewRequest(http.MethodGet, "/ready", nil))
		var report readinessReport
		if err := json.Unmarshal(response.Body.Bytes(), &report); err != nil {
			t.Fatalf("decode readiness response: %v", err)
		}
		return response.Code, report
	}

	status, report := checkReadiness()
	if status != http.StatusOK || !report.Ready || report.PollOwner != "follower" {
		t.Fatalf("healthy follower readiness = HTTP %d %+v", status, report)
	}
	kubernetes.mu.Lock()
	kubernetes.readErr = errors.New("authority service unavailable")
	kubernetes.mu.Unlock()
	status, report = checkReadiness()
	if status != http.StatusOK || !report.Ready || report.PollOwner != "unavailable" {
		t.Fatalf("authority outage readiness = HTTP %d %+v", status, report)
	}
	if report.ProcessStatus != "ok" || len(report.LogicalShards) != logicalShardCount {
		t.Fatalf("readiness response omitted process or logical unit status: %+v", report)
	}

	probe := httptest.NewRecorder()
	(application{store: store}).readyProbe(probe, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if probe.Code != http.StatusOK || probe.Body.Len() != 0 {
		t.Fatalf("follower routing readiness during authority outage = HTTP %d %q, want empty 200", probe.Code, probe.Body)
	}
}
func TestReadinessTelemetryMetricsUseRoutePatterns(t *testing.T) {
	telemetry := NewTelemetry()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/hazards/{hazard_id}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusEarlyHints)
		w.WriteHeader(http.StatusNotFound)
	})
	telemetry.Register(mux)
	handler := telemetry.Wrap(mux)
	response := &telemetryCaptureWriter{}
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/internal/hazards/private-id", nil))
	if !reflect.DeepEqual(response.statuses, []int{http.StatusEarlyHints, http.StatusNotFound}) {
		t.Fatalf("forwarded interim/final statuses = %v", response.statuses)
	}

	scrape := &telemetryCaptureWriter{}
	handler.ServeHTTP(scrape, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, metric := range []string{
		`route="GET /internal/hazards/{hazard_id}",method="GET",status="404"`,
		"bnpb_http_request_duration_seconds_bucket",
	} {
		if !strings.Contains(scrape.output.String(), metric) {
			t.Errorf("metrics response omitted %q", metric)
		}
	}
	if strings.Contains(scrape.output.String(), "private-id") || strings.Contains(scrape.output.String(), `status="103"`) {
		t.Fatal("metrics exposed a request path value or informational status")
	}
}

type telemetryCaptureWriter struct {
	header   http.Header
	statuses []int
	output   strings.Builder
}

func (writer *telemetryCaptureWriter) Header() http.Header {
	if writer.header == nil {
		writer.header = make(http.Header)
	}
	return writer.header
}

func (writer *telemetryCaptureWriter) WriteHeader(status int) {
	writer.statuses = append(writer.statuses, status)
}

func (writer *telemetryCaptureWriter) Write(body []byte) (int, error) {
	return writer.output.Write(body)
}
