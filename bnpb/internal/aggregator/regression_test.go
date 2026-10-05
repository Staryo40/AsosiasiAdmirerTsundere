package aggregator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/config"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/correlation"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/httpserver"
)

func requiredEnvironment() map[string]string {
	return map[string]string{
		"BNPB_BMKG_API_KEY":               "bmkg-test-secret",
		"BNPB_PVMBG_READER_CLIENT_ID":     "reader-test",
		"BNPB_PVMBG_READER_CLIENT_SECRET": "pvmbg-test-secret",
		"BNPB_AGGREGATOR_INTERNAL_KEY":    "internal-test-secret",
		"BNPB_CANONICAL_DATABASE_URL":     "postgres://writer:db-test-secret@postgres:5432/bnpb",
		"BNPB_AMQP_URL":                   "amqp://writer:amqp-test-secret@rabbitmq:5672/events",
	}
}

func configFromEnvironment(env map[string]string) (Config, error) {
	return loadConfig(func(name string) (string, bool) {
		configured, exists := env[name]
		return configured, exists
	})
}

func testConfig(t *testing.T) Config {
	t.Helper()
	cfg, err := configFromEnvironment(requiredEnvironment())
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func quietLogger() *slog.Logger { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }

func TestConfigDefaultsAndOverrides(t *testing.T) {
	cfg := testConfig(t)
	if cfg.BMKG.HTTPTimeout != time.Second || cfg.PVMBG.HTTPTimeout != 5*time.Second || cfg.MaxResponseBytes != 32<<20 || cfg.HazardExchange != "hazard.events" {
		t.Fatalf("unexpected nonsecret defaults: BMKG=%s PVMBG=%s bytes=%d", cfg.BMKG.HTTPTimeout, cfg.PVMBG.HTTPTimeout, cfg.MaxResponseBytes)
	}
	env := requiredEnvironment()
	env["BNPB_BMKG_BASE_URL"] = "https://localhost:8443/"
	env["BNPB_BMKG_HTTP_TIMEOUT"] = "250ms"
	env["BNPB_INGEST_MAX_RESPONSE_BYTES"] = "128"
	cfg, err := configFromEnvironment(env)
	if err != nil || cfg.BMKG.HTTPTimeout != 250*time.Millisecond || cfg.MaxResponseBytes != 128 {
		t.Fatalf("override not applied: %v", err)
	}
}

func TestConfigRejectsInvalidSettingsWithoutSecrets(t *testing.T) {
	for _, test := range []struct{ name, setting, configured string }{
		{"missing secret", "BNPB_BMKG_API_KEY", ""},
		{"blank identity", "BNPB_PVMBG_READER_CLIENT_ID", " "},
		{"header injection", "BNPB_BMKG_API_KEY", "private\r\nheader"},
		{"control character", "BNPB_PVMBG_READER_CLIENT_SECRET", "private\x00secret"},
		{"relative origin", "BNPB_BMKG_BASE_URL", "private/relative"},
		{"source credentials", "BNPB_PVMBG_BASE_URL", "http://reader:private@pvmbg:8082"},
		{"source query", "BNPB_BMKG_BASE_URL", "http://bmkg:8081?secret=private"},
		{"source path", "BNPB_BMKG_BASE_URL", "http://bmkg:8081/private"},
		{"source fragment", "BNPB_BMKG_BASE_URL", "http://bmkg:8081#private"},
		{"invalid port", "BNPB_BMKG_BASE_URL", "http://bmkg:65536"},
		{"wrong database scheme", "BNPB_CANONICAL_DATABASE_URL", "http://writer:private@postgres"},
		{"invalid broker URL", "BNPB_AMQP_URL", "amqp://writer:private@broker:invalid"},
		{"empty duration", "BNPB_BMKG_HTTP_TIMEOUT", ""},
		{"negative duration", "BNPB_BMKG_HTTP_TIMEOUT", "-1s"},
		{"zero duration", "BNPB_PVMBG_POLL_INTERVAL", "0s"},
		{"tiny duration", "BNPB_BMKG_POLL_INTERVAL", "1ns"},
		{"excessive timeout", "BNPB_PVMBG_HTTP_TIMEOUT", "2m"},
		{"invalid duration", "BNPB_DB_READ_TIMEOUT", "private"},
		{"zero count", "BNPB_INGEST_CHUNK_RECORDS", "0"},
		{"negative count", "BNPB_DB_WRITE_MAX_CONNS", "-1"},
		{"excessive bytes", "BNPB_INGEST_MAX_RESPONSE_BYTES", "134217729"},
		{"overflow", "BNPB_OUTBOX_BATCH_SIZE", "99999999999999999999999"},
		{"invalid integer", "BNPB_READ_MAX_IN_FLIGHT", "private"},
		{"contract page ceiling", "BNPB_LIST_MAX_LIMIT", "501"},
		{"page cross bound", "BNPB_LIST_MAX_LIMIT", "99"},
		{"cycle cross bound", "BNPB_PVMBG_CYCLE_TIMEOUT", "9s"},
		{"staleness cross bound", "BNPB_SOURCE_STALE_AFTER", "12s"},
		{"pool cross bound", "BNPB_DB_READ_MAX_CONNS", "64"},
	} {
		t.Run(test.name, func(t *testing.T) {
			env := requiredEnvironment()
			env[test.setting] = test.configured
			_, err := configFromEnvironment(env)
			if err == nil || !strings.Contains(err.Error(), "BNPB_") || strings.Contains(err.Error(), "private") || strings.Contains(err.Error(), "test-secret") {
				t.Fatalf("expected useful sanitized configuration error, got %v", err)
			}
		})
	}
	for setting := range requiredEnvironment() {
		t.Run("unset "+setting, func(t *testing.T) {
			env := requiredEnvironment()
			delete(env, setting)
			_, err := configFromEnvironment(env)
			if err == nil || !strings.Contains(err.Error(), setting) {
				t.Fatalf("missing variable must be named: %v", err)
			}
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return fn(request) }

type trackedBody struct {
	io.Reader
	closed bool
}

func (body *trackedBody) Close() error { body.closed = true; return nil }

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestSourceJSONBoundsAndBodyClosure(t *testing.T) {
	// Boundaries: empty, exact limit, limit+1, known/unknown length, partial/trailing JSON.
	for _, test := range []struct {
		name, body, category string
		status               int
		length               int64
	}{
		{"complete", `{"ok":true}`, "", 200, -1},
		{"exact limit", `{"ok":true}     `, "", 200, -1},
		{"unknown oversized", `{"ok":true}      `, "response_too_large", 200, -1},
		{"known oversized", "", "response_too_large", 200, 17},
		{"empty", "", "invalid_json", 200, -1},
		{"truncated JSON", `{"ok":`, "invalid_json", 200, -1},
		{"second JSON", `{} {}`, "invalid_json", 200, -1},
		{"trailing garbage", `{} private`, "invalid_json", 200, -1},
		{"non success", "private", "http_status", 503, -1},
		{"redirect", "private", "http_status", 302, -1},
		{"no content", "", "invalid_json", 204, -1},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := testConfig(t)
			cfg.MaxResponseBytes = 16
			clients := newSourceClients(cfg, quietLogger())
			defer clients.closeIdleConnections()
			body := &trackedBody{Reader: strings.NewReader(test.body)}
			clients.bmkg.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: test.status, ContentLength: test.length, Body: body}, nil
			})
			var payload json.RawMessage
			err := clients.bmkg.doJSON(t.Context(), sourceRequest{method: "GET", path: "/seismic-events"}, &payload)
			assertSourceCategory(t, err, test.category)
			if !body.closed {
				t.Fatal("response body was not closed")
			}
		})
	}
}

func assertSourceCategory(t *testing.T, err error, category string) {
	t.Helper()
	if category == "" {
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	var sourceErr *sourceError
	if !errors.As(err, &sourceErr) || sourceErr.category != category || strings.Contains(err.Error(), "private") {
		t.Fatalf("expected sanitized %s, got %v", category, err)
	}
}

func TestSourceReadAndTransportErrors(t *testing.T) {
	clients := newSourceClients(testConfig(t), quietLogger())
	defer clients.closeIdleConnections()
	body := &trackedBody{Reader: failingReader{}}
	clients.bmkg.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, ContentLength: -1, Body: body}, nil
	})
	var payload json.RawMessage
	err := clients.bmkg.doJSON(t.Context(), sourceRequest{method: "GET", path: "/seismic-events"}, &payload)
	assertSourceCategory(t, err, "read_error")
	if !body.closed || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("read error lost or body leaked: %v", err)
	}
	clients.bmkg.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("private transport diagnostic")
	})
	err = clients.bmkg.doJSON(t.Context(), sourceRequest{method: "GET", path: "/seismic-events"}, &payload)
	assertSourceCategory(t, err, "transport_error")
}

func TestSourceClientsCorrelationCredentialsAndRedirects(t *testing.T) {
	seen := make(chan *http.Request, 4)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		seen <- request
		_, _ = io.WriteString(w, `[]`)
	}))
	defer target.Close()
	agency := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		seen <- request
		if request.URL.Path == "/redirect" {
			http.Redirect(w, request, target.URL, http.StatusFound)
			return
		}
		_, _ = io.WriteString(w, `[]`)
	}))
	defer agency.Close()
	cfg := testConfig(t)
	cfg.BMKG.BaseURL, cfg.PVMBG.BaseURL = agency.URL, agency.URL
	clients := newSourceClients(cfg, quietLogger())
	defer clients.closeIdleConnections()
	ctx := correlation.WithID(t.Context(), "source-test-correlation")
	var payload []json.RawMessage
	if err := clients.bmkg.doJSON(ctx, sourceRequest{method: "GET", path: "/seismic-events", query: url.Values{"since": {"2026-10-03T00:00:00Z"}}}, &payload); err != nil {
		t.Fatal(err)
	}
	bmkg := <-seen
	if bmkg.Header.Get("X-BMKG-Key") != cfg.BMKGAPIKey || bmkg.Header.Get("Authorization") != "" || bmkg.Header.Get(correlation.Header) != correlation.ID(ctx) || bmkg.URL.Query().Get("since") == "" {
		t.Fatal("BMKG authentication, query or correlation incorrect")
	}
	if err := clients.pvmbg.doJSON(ctx, sourceRequest{method: "POST", path: "/auth/login", body: []byte(`{"client_id":"reader-test","client_secret":"pvmbg-test-secret"}`)}, &payload); err != nil {
		t.Fatal(err)
	}
	pvmbg := <-seen
	if pvmbg.Header.Get("X-BMKG-Key") != "" || pvmbg.Header.Get(correlation.Header) != correlation.ID(ctx) || pvmbg.Header.Get("Content-Type") != "application/json" {
		t.Fatal("credentials crossed source boundaries or login lost correlation")
	}
	if err := clients.pvmbg.doJSON(ctx, sourceRequest{method: "GET", path: "/volcanic-reports", header: http.Header{"Authorization": {"Bearer pvmbg-test-token"}}}, &payload); err != nil {
		t.Fatal(err)
	}
	pvmbg = <-seen
	if pvmbg.Header.Get("Authorization") != "Bearer pvmbg-test-token" || pvmbg.Header.Get("X-BMKG-Key") != "" {
		t.Fatal("PVMBG bearer credential was lost or mixed with the BMKG key")
	}
	err := clients.bmkg.doJSON(ctx, sourceRequest{method: "GET", path: "/redirect"}, &payload)
	assertSourceCategory(t, err, "http_status")
	<-seen
	select {
	case <-seen:
		t.Fatal("redirect reached a second origin")
	default:
	}
	for _, path := range []string{"http://private", "//private", "/events?private", "/events#private"} {
		err := clients.bmkg.doJSON(ctx, sourceRequest{method: "GET", path: path}, &payload)
		assertSourceCategory(t, err, "invalid_request")
	}
}

func TestSlowSourceBodyHasDeadline(t *testing.T) {
	canceled := make(chan struct{})
	agency := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		_, _ = io.WriteString(w, "[")
		w.(http.Flusher).Flush()
		<-request.Context().Done()
		close(canceled)
	}))
	defer agency.Close()
	cfg := testConfig(t)
	cfg.BMKG.BaseURL, cfg.BMKG.HTTPTimeout = agency.URL, 100*time.Millisecond
	clients := newSourceClients(cfg, quietLogger())
	defer clients.closeIdleConnections()
	var payload json.RawMessage
	err := clients.bmkg.doJSON(t.Context(), sourceRequest{method: "GET", path: "/seismic-events"}, &payload)
	assertSourceCategory(t, err, "timeout")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lost timeout cause: %v", err)
	}
	waitForCompletion(t, canceled)
}

func waitForCompletion(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("work did not finish within the test shutdown budget")
	}
}

func TestApplicationCancelsInFlightSourceAndJoinsWorkers(t *testing.T) {
	started, sourceCanceled, workerDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	agency := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		_, _ = io.WriteString(w, "[")
		w.(http.Flusher).Flush()
		close(started)
		<-request.Context().Done()
		close(sourceCanceled)
	}))
	defer agency.Close()
	cfg := testConfig(t)
	cfg.PVMBG.BaseURL = agency.URL
	app := application{sources: newSourceClients(cfg, quietLogger())}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	workerErrors := make(chan error, 1)
	shutdown := app.start(ctx, func(ctx context.Context) {
		defer close(workerDone)
		var payload json.RawMessage
		workerErrors <- app.sources.pvmbg.doJSON(ctx, sourceRequest{method: "GET", path: "/volcanic-reports"}, &payload)
	})
	waitForCompletion(t, started)
	cancel()
	shutdownCtx, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()
	if err := shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	waitForCompletion(t, workerDone)
	waitForCompletion(t, sourceCanceled)
	if err := <-workerErrors; !errors.Is(err, context.Canceled) {
		t.Fatalf("source request not canceled: %v", err)
	}
	if err := shutdown(shutdownCtx); err != nil {
		t.Fatalf("repeated cleanup failed: %v", err)
	}
}

func TestSlowPVMBGDoesNotBlockBMKG(t *testing.T) {
	started := make(chan struct{})
	pvmbg := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		close(started)
		<-request.Context().Done()
	}))
	defer pvmbg.Close()
	bmkg := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `[]`)
	}))
	defer bmkg.Close()
	cfg := testConfig(t)
	cfg.BMKG.BaseURL, cfg.PVMBG.BaseURL = bmkg.URL, pvmbg.URL
	app := application{sources: newSourceClients(cfg, quietLogger())}
	workerDone := make(chan struct{})
	shutdown := app.start(t.Context(), func(ctx context.Context) {
		defer close(workerDone)
		var payload json.RawMessage
		_ = app.sources.pvmbg.doJSON(ctx, sourceRequest{method: "GET", path: "/volcanic-reports"}, &payload)
	})
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := shutdown(shutdownCtx); err != nil {
			t.Error(err)
		}
	}()
	waitForCompletion(t, started)
	var payload []json.RawMessage
	if err := app.sources.bmkg.doJSON(t.Context(), sourceRequest{method: "GET", path: "/seismic-events"}, &payload); err != nil {
		t.Fatal(err)
	}
	select {
	case <-workerDone:
		t.Fatal("PVMBG request ended before the independent BMKG request completed")
	default:
	}
}

func TestApplicationCleanupDeadlineIsReported(t *testing.T) {
	app := application{sources: newSourceClients(testConfig(t), quietLogger())}
	release := make(chan struct{})
	shutdown := app.start(t.Context(), func(context.Context) { <-release })
	defer func() {
		close(release)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := shutdown(shutdownCtx); err != nil {
			t.Error(err)
		}
	}()
	shutdownCtx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := shutdown(shutdownCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("uncooperative worker must report deadline failure: %v", err)
	}
}

func TestReadinessAndLivenessWithoutDependencies(t *testing.T) {
	mux := http.NewServeMux()
	httpserver.RegisterHealth(mux, "bnpb-aggregator")
	app := application{}
	mux.HandleFunc("GET /ready", app.ready)
	for path, want := range map[string]int{"/health": 200, "/ready": 503} {
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, httptest.NewRequest("GET", path, nil))
		if response.Code != want {
			t.Errorf("%s: got %d, want %d", path, response.Code, want)
		}
	}
}

func TestInitializeFailsWithUnavailableStoreWithoutExposingCredentials(t *testing.T) {
	for name, configured := range requiredEnvironment() {
		t.Setenv(name, configured)
	}
	t.Setenv("BNPB_CANONICAL_DATABASE_URL", "postgres://writer:private@127.0.0.1:1/bnpb")
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	shutdown, err := Initialize(t.Context(), http.NewServeMux(), config.Config{MaxInFlight: 64}, logger)
	if shutdown != nil || err == nil || strings.Contains(err.Error(), "private") {
		t.Fatalf("expected sanitized startup failure and no acquired resources, got %v", err)
	}
	if !strings.Contains(logs.String(), "canonical_store_initialization_failed") || strings.Contains(logs.String(), "private") {
		t.Fatalf("startup diagnostic is missing or unsafe: %s", logs.String())
	}
}
