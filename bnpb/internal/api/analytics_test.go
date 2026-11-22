package api

import (
	"io"
	"net/http"
	"sync/atomic"
	"testing"
)

const analyticsBody = `{"from":"2026-11-21T00:00:00Z","to":"2026-11-22T00:00:00Z","bucket":"hour","as_of":"2026-11-22T00:00:01Z",` +
	`"total":0,"series":[],"severity":{"NORMAL":0,"WASPADA":0,"SIAGA":0,"AWAS":0},"areas":[],"partial":true,"unavailable_units":["shard-1"]}`

func TestAnalyticsAreForwardedToEveryRole(t *testing.T) {
	var calls atomic.Int32
	var last atomic.Value
	handler := testPublicHandler(t, authDependency(t), opsAggregator(t, &calls, &last, http.StatusOK, analyticsBody))
	for _, token := range []string{"media-token", "field-token", "ops-token"} {
		got := opsRequest(handler, http.MethodGet, "/v1/analytics?from=2026-11-21T00:00:00Z&to=2026-11-22T00:00:00Z&bucket=hour", token, "")
		if got.Code != http.StatusOK || got.Body.String() != analyticsBody {
			t.Fatalf("%s: HTTP %d %s", token, got.Code, got.Body.String())
		}
	}
	call := last.Load().(recordedCall)
	if call.path != "/internal/analytics" || call.query != "bucket=hour&from=2026-11-21T00%3A00%3A00Z&to=2026-11-22T00%3A00%3A00Z" {
		t.Fatalf("forwarded %+v", call)
	}
}

func TestAnalyticsRejectsBadQueriesBeforeTheAggregator(t *testing.T) {
	var calls atomic.Int32
	var last atomic.Value
	handler := testPublicHandler(t, authDependency(t), opsAggregator(t, &calls, &last, http.StatusOK, analyticsBody))
	for _, target := range []string{"/v1/analytics", "/v1/analytics?from=2026-11-21T00:00:00Z", "/v1/analytics?from=a&to=b&source=BMKG"} {
		if got := opsRequest(handler, http.MethodGet, target, "field-token", ""); got.Code != http.StatusBadRequest {
			t.Errorf("%s: HTTP %d, want 400", target, got.Code)
		}
	}
	if got := opsRequest(handler, http.MethodGet, "/v1/analytics?from=a&to=b", "", ""); got.Code != http.StatusUnauthorized {
		t.Errorf("anonymous: HTTP %d, want 401", got.Code)
	}
	if calls.Load() != 0 {
		t.Fatalf("the Aggregator was called %d times", calls.Load())
	}
}

func TestAnalyticsErrorsPassOrBecome503(t *testing.T) {
	for _, test := range []struct {
		status int
		body   string
		want   int
	}{
		{http.StatusBadRequest, `{"error":{"code":"invalid_query"}}`, http.StatusBadRequest},
		{http.StatusTooManyRequests, `{"error":{"code":"admission_limited"}}`, http.StatusTooManyRequests},
		{http.StatusServiceUnavailable, `{"error":{"code":"analytics_unavailable"}}`, http.StatusServiceUnavailable},
		{http.StatusInternalServerError, `{"error":{"code":"x"}}`, http.StatusServiceUnavailable},
		{http.StatusOK, `not json`, http.StatusServiceUnavailable},
	} {
		var calls atomic.Int32
		var last atomic.Value
		handler := testPublicHandler(t, authDependency(t), opsAggregator(t, &calls, &last, test.status, test.body))
		got := opsRequest(handler, http.MethodGet, "/v1/analytics?from=a&to=b", "media-token", "")
		body, _ := io.ReadAll(got.Body)
		if got.Code != test.want {
			t.Errorf("Aggregator %d: API %d %s, want %d", test.status, got.Code, body, test.want)
		}
	}
}
