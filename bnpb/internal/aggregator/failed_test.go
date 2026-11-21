package aggregator

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func testSourceClient(t *testing.T, maxBytes int64, handler http.HandlerFunc) *sourceClient {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	origin, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	return &sourceClient{client: &http.Client{Timeout: 5 * time.Second}, origin: origin, maxBytes: maxBytes}
}

func sourceFailure(t *testing.T, err error) *sourceError {
	t.Helper()
	var failure *sourceError
	if !errors.As(err, &failure) {
		t.Fatalf("expected a source error, got %v", err)
	}
	return failure
}

func TestRecordFetchKeepsTheUnreadableBody(t *testing.T) {
	body := []byte(`[{"report_id":"r-1","volcano_id":"mer`)
	client := testSourceClient(t, 1<<20, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(body) })
	_, err := client.fetchRecords(context.Background(), sourceRequest{method: http.MethodGet, path: "/volcanic-reports"})
	failure := sourceFailure(t, err)
	if failure.category != "invalid_json" || !bytes.Equal(failure.payload, body) || failure.payloadBytes != int64(len(body)) || failure.payloadTruncated {
		t.Fatalf("unexpected capture: %+v", failure)
	}
}

func TestLoginStyleRequestsNeverKeepTheBody(t *testing.T) {
	client := testSourceClient(t, 1<<20, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"access_token":"secret`)) })
	var target map[string]any
	err := client.doJSON(context.Background(), sourceRequest{method: http.MethodPost, path: "/auth/login", body: []byte(`{}`)}, &target)
	if failure := sourceFailure(t, err); failure.payload != nil {
		t.Fatal("a login body must not be captured")
	}
}

func TestOversizedBodiesKeepOnlyAPrefix(t *testing.T) {
	for name, chunked := range map[string]bool{"content-length": false, "chunked": true} {
		big := bytes.Repeat([]byte("x"), failedPayloadLimit+4096)
		client := testSourceClient(t, int64(failedPayloadLimit+1024), func(w http.ResponseWriter, _ *http.Request) {
			if chunked {
				w.(http.Flusher).Flush()
			}
			_, _ = w.Write(big)
		})
		_, err := client.fetchRecords(context.Background(), sourceRequest{method: http.MethodGet, path: "/seismic-events"})
		failure := sourceFailure(t, err)
		if failure.category != "response_too_large" || len(failure.payload) != failedPayloadLimit || !failure.payloadTruncated ||
			failure.payloadBytes < int64(failedPayloadLimit) {
			t.Fatalf("%s: category=%s payload=%d bytes=%d truncated=%v", name, failure.category, len(failure.payload), failure.payloadBytes, failure.payloadTruncated)
		}
	}
}

func TestFailedMessageIdentityDependsOnEndpointKindAndBytes(t *testing.T) {
	payload := []byte(`{"report_id":"r-1"}`)
	base := failedMessageKey(VolcanicEndpoint, "record", payload)
	for _, other := range [][]byte{
		failedMessageKey(SeismicEndpoint, "record", payload),
		failedMessageKey(VolcanicEndpoint, "response", payload),
		failedMessageKey(VolcanicEndpoint, "record", []byte(`{"report_id":"r-2"}`)),
	} {
		if bytes.Equal(base, other) {
			t.Fatal("distinct messages share a key")
		}
	}
	if !bytes.Equal(base, failedMessageKey(VolcanicEndpoint, "record", payload)) {
		t.Fatal("the same message must keep its key")
	}
	if id := failedMessageID(base); !strings.HasPrefix(id, "msg-") || len(id) != 20 {
		t.Fatalf("unexpected message id %q", id)
	}
}

func TestFailedCursorRoundTripAndRejection(t *testing.T) {
	message := failedMessage{MessageID: "msg-0011223344556677", ReceivedAt: time.Date(2026, 11, 21, 1, 2, 3, 456000, time.UTC)}
	cursor, err := decodeFailedCursor(encodeFailedCursor(message))
	if err != nil || cursor.MessageID != message.MessageID || !cursor.ReceivedAt.Equal(message.ReceivedAt) {
		t.Fatalf("round trip failed: %+v %v", cursor, err)
	}
	for _, raw := range []string{"%%%", "e30", strings.Repeat("A", 1000)} {
		if _, err := decodeFailedCursor(raw); err == nil {
			t.Errorf("cursor %.20q must be rejected", raw)
		}
	}
}

func TestUnprocessedRouteChecksTokenAndQuery(t *testing.T) {
	cfg := Config{DBReadTimeout: time.Second}
	cfg.Audit.UnprocessedToken = "metadata-token"
	mux := http.NewServeMux()
	registerDLQRoutes(mux, nil, cfg, 10*time.Second, nil)
	for _, test := range []struct {
		query, header string
		want          int
	}{
		{"", "", http.StatusUnauthorized},
		{"", "Bearer wrong", http.StatusForbidden},
		{"", "Basic metadata-token", http.StatusForbidden},
		{"?limit=0", "Bearer metadata-token", http.StatusBadRequest},
		{"?limit=1001", "Bearer metadata-token", http.StatusBadRequest},
		{"?source=NASA", "Bearer metadata-token", http.StatusBadRequest},
		{"?cursor=%25%25", "Bearer metadata-token", http.StatusBadRequest},
		{"?status=parked", "Bearer metadata-token", http.StatusBadRequest},
	} {
		request := httptest.NewRequest(http.MethodGet, "/internal/dlq/unprocessed"+test.query, nil)
		if test.header != "" {
			request.Header.Set("Authorization", test.header)
		}
		recorder := httptest.NewRecorder()
		mux.ServeHTTP(recorder, request)
		if recorder.Code != test.want || !strings.Contains(recorder.Body.String(), `"error":{"code"`) {
			t.Errorf("%s %q: HTTP %d %s", test.query, test.header, recorder.Code, recorder.Body)
		}
	}
}

func TestUnprocessedRouteIsAbsentWithoutAToken(t *testing.T) {
	mux := http.NewServeMux()
	registerDLQRoutes(mux, nil, Config{}, 10*time.Second, nil)
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/internal/dlq/unprocessed", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", recorder.Code)
	}
}
