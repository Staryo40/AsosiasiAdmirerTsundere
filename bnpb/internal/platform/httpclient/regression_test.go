package httpclient

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/logging"
)

func testLogger(buffer *bytes.Buffer) *slog.Logger {
	return logging.New("test-service", slog.LevelDebug, buffer)
}

func decodeLogLines(t *testing.T, data []byte) []map[string]any {
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

func TestClientClosesIdleConnections(t *testing.T) {
	closed := make(chan struct{}, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			closed <- struct{}{}
		}
	}
	server.Start()
	defer server.Close()
	var output bytes.Buffer
	client := New(time.Second, testLogger(&output))
	response, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	response.Body.Close()
	client.CloseIdleConnections()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("CloseIdleConnections did not reach the owned transport")
	}
}

func TestOutboundStatusLogLevels(t *testing.T) {
	for status, level := range map[int]string{200: "INFO", 429: "WARN", 503: "ERROR"} {
		var output bytes.Buffer
		request := httptest.NewRequest(http.MethodGet, "http://agency/events", nil)
		transport := instrumentedTransport{logger: testLogger(&output)}
		transport.logSuccess(request, &http.Response{StatusCode: status}, time.Millisecond)
		if records := decodeLogLines(t, output.Bytes()); records[0]["level"] != level {
			t.Errorf("status %d: %v", status, records)
		}
	}
}
