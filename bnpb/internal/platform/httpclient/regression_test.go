package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/correlation"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/logging"
)

func testLogger(buffer *bytes.Buffer) *slog.Logger {
	return logging.New("test-service", slog.LevelDebug, buffer)
}

func TestOutboundCorrelationLatencyAndRedactionOverNetwork(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		status int
	}{
		{"success", 200}, {"error", 503}, {"timeout", 0},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			seen := make(chan string, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				seen <- r.Header.Get(correlation.Header)
				if scenario.status == 0 {
					<-r.Context().Done()
					return
				}
				w.WriteHeader(scenario.status)
			}))
			defer server.Close()
			var output bytes.Buffer
			client := New(time.Second, testLogger(&output))
			defer client.CloseIdleConnections()
			ctx, cancel := context.WithTimeout(correlation.WithID(t.Context(), "original-cid"), 100*time.Millisecond)
			defer cancel()
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/events?token=private-query", nil)
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Authorization", "Bearer private-token")
			response, err := client.Do(request)
			if response != nil {
				response.Body.Close()
			}
			if (err != nil) != (scenario.status == 0) {
				t.Fatalf("unexpected outcome: %v", err)
			}
			if <-seen != "original-cid" {
				t.Fatal("CID not propagated")
			}
			record := decodeLogLines(t, output.Bytes())[0]
			if record["correlation_id"] != "original-cid" || record["duration_ms"] == nil {
				t.Fatalf("invalid trace: %v", record)
			}
			if scenario.status == 0 && record["failure"] != "timeout" {
				t.Fatalf("timeout not classified: %v", record)
			}
			if strings.Contains(output.String(), "private-") {
				t.Fatal("credential/query leaked")
			}
		})
	}
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
