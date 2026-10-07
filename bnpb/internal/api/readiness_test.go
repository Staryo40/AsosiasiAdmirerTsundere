package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/config"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/correlation"
)

func setRequiredAPISecrets(t *testing.T) {
	t.Helper()
	t.Setenv("BNPB_AUTH_INTERNAL_KEY", strings.Repeat("a", 32))
	t.Setenv("BNPB_AGGREGATOR_INTERNAL_KEY", strings.Repeat("g", 32))
}

func TestReadinessDependencyLifecycle(t *testing.T) {
	setRequiredAPISecrets(t)
	auth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			t.Errorf("Auth readiness called %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer auth.Close()
	t.Setenv("BNPB_AUTH_BASE_URL", auth.URL)
	observed := make(chan string, 1)
	aggregator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ready" || r.Header.Get("X-BNPB-Internal-Key") != "" {
			t.Error("readiness must use the public dependency check without credentials")
		}
		observed <- r.Header.Get(correlation.Header)
		w.WriteHeader(http.StatusOK)
	}))
	defer aggregator.Close()
	t.Setenv("BNPB_AGGREGATOR_BASE_URL", aggregator.URL)
	mux := http.NewServeMux()
	shutdown, err := Initialize(t.Context(), mux, config.Config{OutboundHTTPTimeout: time.Second}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(t.Context())
	request := httptest.NewRequest(http.MethodGet, "/ready", nil)
	request = request.WithContext(correlation.WithID(request.Context(), "batch10-readiness"))
	ready := httptest.NewRecorder()
	mux.ServeHTTP(ready, request)
	if ready.Code != 200 || <-observed != "batch10-readiness" {
		t.Fatal("healthy dependency or correlation propagation failed")
	}
	aggregator.Close()
	unavailable := httptest.NewRecorder()
	mux.ServeHTTP(unavailable, request)
	if unavailable.Code != 503 || strings.Contains(unavailable.Body.String(), aggregator.URL) {
		t.Fatal("stopped dependency must return a sanitized 503")
	}
}

func TestReadinessRejectsNonOKAndBoundsWait(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		status int
	}{
		{"unready", 503}, {"unexpected-success", 204}, {"redirect", 302}, {"timeout", 0},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			setRequiredAPISecrets(t)
			auth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/health" {
					t.Errorf("Auth readiness called %s", r.URL.Path)
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer auth.Close()
			t.Setenv("BNPB_AUTH_BASE_URL", auth.URL)
			aggregator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if scenario.status == 0 {
					<-r.Context().Done()
					return
				}
				w.Header().Set("Location", "/ready")
				w.WriteHeader(scenario.status)
			}))
			defer aggregator.Close()
			t.Setenv("BNPB_AGGREGATOR_BASE_URL", aggregator.URL)
			mux := http.NewServeMux()
			shutdown, err := Initialize(t.Context(), mux, config.Config{OutboundHTTPTimeout: 100 * time.Millisecond}, slog.New(slog.NewTextHandler(io.Discard, nil)))
			if err != nil {
				t.Fatal(err)
			}
			defer shutdown(t.Context())
			response := httptest.NewRecorder()
			started := time.Now()
			mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/ready", nil))
			if response.Code != 503 || time.Since(started) > time.Second {
				t.Fatal("failed dependency must promptly return 503")
			}
		})
	}
}

func TestReadinessPropagatesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	response := httptest.NewRecorder()
	dependencyReadiness(&http.Client{Timeout: time.Second}, "http://127.0.0.1:1/ready")(response, httptest.NewRequest(http.MethodGet, "/ready", nil).WithContext(ctx))
	if response.Code != 503 {
		t.Fatal("cancelled readiness must fail")
	}
}

func TestReadinessRejectsUnsafeConfiguration(t *testing.T) {
	for _, scenario := range []struct{ name, address string }{
		{"empty", ""}, {"credentials", "http://user:secret@aggregator:8083"},
		{"query", "http://aggregator:8083?secret=private"}, {"path", "http://aggregator:8083/internal"},
		{"fragment", "http://aggregator:8083#private"}, {"scheme", "file:///private"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			t.Setenv("BNPB_AGGREGATOR_BASE_URL", scenario.address)
			_, err := aggregatorReadyURL()
			if err == nil || strings.Contains(err.Error(), "private") || strings.Contains(err.Error(), "user:secret") {
				t.Fatal("unsafe configuration must fail without echoing it")
			}
		})
	}
}
