package httpserver

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/textproto"
	"strings"
	"testing"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/correlation"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/logging"
)

// Keep regression helpers independent of ignored local test files.
func regressionLogger(buffer *bytes.Buffer) *slog.Logger {
	return logging.New("test-service", slog.LevelDebug, buffer)
}

func decodeRegressionLogLines(t *testing.T, data []byte) []map[string]any {
	t.Helper()
	lines := bytes.Split(bytes.TrimSpace(data), []byte("\n"))
	result := make([]map[string]any, 0, len(lines))
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("invalid JSON log %q: %v", line, err)
		}
		result = append(result, record)
	}
	return result
}

func TestInformationalHeadersPreserveFinalResponse(t *testing.T) {
	var output bytes.Buffer
	server := httptest.NewServer(Handler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusEarlyHints)
		w.WriteHeader(http.StatusProcessing)
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, "created")
	}), regressionLogger(&output), 1))
	defer server.Close()
	var hints []int
	trace := &httptrace.ClientTrace{Got1xxResponse: func(code int, _ textproto.MIMEHeader) error {
		hints = append(hints, code)
		return nil
	}}
	request, _ := http.NewRequest(http.MethodGet, server.URL, nil)
	request = request.WithContext(httptrace.WithClientTrace(request.Context(), trace))
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	server.Close()
	if err != nil || response.StatusCode != http.StatusCreated || string(body) != "created" || len(hints) != 2 {
		t.Fatalf("status=%d body=%q hints=%v error=%v", response.StatusCode, body, hints, err)
	}
	if records := decodeRegressionLogLines(t, output.Bytes()); records[0]["status"] != float64(http.StatusCreated) {
		t.Fatalf("wrong final status: %v", records)
	}
}

func TestPanicAfterFlushAborts(t *testing.T) {
	var output bytes.Buffer
	handler := Handler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Fatal(err)
		}
		panic("private")
	}), regressionLogger(&output), 1)
	defer func() {
		if recovered := recover(); recovered != http.ErrAbortHandler {
			t.Errorf("got %v, want abort after flushed headers", recovered)
		}
	}()
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
}

func TestExplicitAbortIsPreserved(t *testing.T) {
	var output bytes.Buffer
	handler := Handler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(http.ErrAbortHandler)
	}), regressionLogger(&output), 1)
	defer func() {
		if recovered := recover(); recovered != http.ErrAbortHandler {
			t.Errorf("got %v, want explicit abort", recovered)
		}
	}()
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
}

func TestPanicDiscardsUncommittedResponseHeaders(t *testing.T) {
	var output bytes.Buffer
	server := httptest.NewServer(Handler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "1")
		w.Header().Set("Content-Encoding", "gzip")
		panic("private")
	}), regressionLogger(&output), 1))
	defer server.Close()
	response, err := server.Client().Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != 500 || string(body) != "{\"error\":\"internal server error\"}\n" {
		t.Fatalf("status=%d body=%q error=%v", response.StatusCode, body, err)
	}
	if response.Header.Get(correlation.Header) == "" {
		t.Fatal("missing correlation header")
	}
}

func TestOnlyHealthMethodsBypassLimit(t *testing.T) {
	handler := limitRequests(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}), 0)
	// A full limiter must admit GET/HEAD probes, but reject other methods.
	for method, want := range map[string]int{"GET": 204, "HEAD": 204, "POST": 429} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(method, "/health", nil))
		if response.Code != want {
			t.Errorf("%s: got %d, want %d", method, response.Code, want)
		}
	}
}

func TestHealthCheckIPv6Loopback(t *testing.T) {
	listener, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skipf("IPv6 unavailable: %v", err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	server.Listener.Close()
	server.Listener = listener
	server.Start()
	defer server.Close()
	if err := HealthCheck(listener.Addr().String()); err != nil {
		t.Fatal(err)
	}
}

func TestServerDiagnosticsAreSanitizedJSON(t *testing.T) {
	var diagnostics, output bytes.Buffer
	previousOutput := log.Writer()
	log.SetOutput(&diagnostics)
	defer log.SetOutput(previousOutput)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "private-header-value")
		w.WriteHeader(http.StatusOK)
	}))
	server.Config = New(Options{}, server.Config.Handler, regressionLogger(&output))
	server.Start()
	defer server.Close()
	response, err := server.Client().Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	server.Close()
	if strings.Contains(diagnostics.String(), "private-header-value") {
		t.Fatal("server leaked header value through the default text logger")
	}
	records := decodeRegressionLogLines(t, output.Bytes())
	if len(records) != 1 || records[0]["event"] != "http_server_error" || strings.Contains(output.String(), "private-header-value") {
		t.Fatalf("unexpected diagnostic: %s", output.String())
	}
}
