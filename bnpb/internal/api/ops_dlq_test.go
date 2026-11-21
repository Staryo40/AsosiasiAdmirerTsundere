package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

type recordedCall struct {
	method, path, query, body string
}

func opsAggregator(t *testing.T, calls *atomic.Int32, last *atomic.Value, status int, response string) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		if request.Header.Get(aggregatorInternalKeyHeader) != testAggregatorKey {
			t.Error("Aggregator credential missing")
		}
		body, _ := io.ReadAll(request.Body)
		last.Store(recordedCall{request.Method, request.URL.Path, request.URL.RawQuery, string(body)})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, response)
	})
}

func opsRequest(handler http.Handler, method, target, token, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, target, strings.NewReader(body))
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

func TestOpsRoutesRejectEveryRoleButOps(t *testing.T) {
	var calls atomic.Int32
	var last atomic.Value
	handler := testPublicHandler(t, authDependency(t), opsAggregator(t, &calls, &last, http.StatusOK, `{"items":[],"next_cursor":null}`))
	for _, test := range []struct {
		method, target, token, body string
		want                        int
	}{
		{http.MethodGet, "/v1/ops/failed-messages", "", "", http.StatusUnauthorized},
		{http.MethodGet, "/v1/ops/failed-messages", "media-token", "", http.StatusForbidden},
		{http.MethodGet, "/v1/ops/failed-messages", "field-token", "", http.StatusForbidden},
		{http.MethodPost, "/v1/ops/failed-messages/replay", "media-token", "{}", http.StatusForbidden},
		{http.MethodPost, "/v1/ops/failed-messages/replay", "field-token", "{}", http.StatusForbidden},
	} {
		if got := opsRequest(handler, test.method, test.target, test.token, test.body); got.Code != test.want {
			t.Errorf("%s %s as %q: HTTP %d, want %d", test.method, test.target, test.token, got.Code, test.want)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("a refused caller reached the Aggregator %d times", calls.Load())
	}
}

func TestOpsCanListAndReplayFailedMessages(t *testing.T) {
	var calls atomic.Int32
	var last atomic.Value
	handler := testPublicHandler(t, authDependency(t), opsAggregator(t, &calls, &last, http.StatusOK, `{"attempted":1}`))

	got := opsRequest(handler, http.MethodGet, "/v1/ops/failed-messages?source=PVMBG&limit=5", "ops-token", "")
	call := last.Load().(recordedCall)
	if got.Code != http.StatusOK || call.method != http.MethodGet || call.path != "/internal/dlq/messages" || call.query != "limit=5&source=PVMBG" {
		t.Fatalf("list: HTTP %d, forwarded %+v", got.Code, call)
	}

	got = opsRequest(handler, http.MethodPost, "/v1/ops/failed-messages/replay", "ops-token", `{"source":"PVMBG"}`)
	call = last.Load().(recordedCall)
	if got.Code != http.StatusOK || got.Body.String() != `{"attempted":1}` || call.method != http.MethodPost ||
		call.path != "/internal/dlq/replay" || call.body != `{"source":"PVMBG"}` {
		t.Fatalf("replay: HTTP %d %s, forwarded %+v", got.Code, got.Body, call)
	}
}

func TestOpsRoutesValidateInputBeforeCallingTheAggregator(t *testing.T) {
	var calls atomic.Int32
	var last atomic.Value
	handler := testPublicHandler(t, authDependency(t), opsAggregator(t, &calls, &last, http.StatusOK, `{}`))
	for _, test := range []struct {
		method, target, body string
		want                 int
	}{
		{http.MethodGet, "/v1/ops/failed-messages?status=parked", "", http.StatusBadRequest},
		{http.MethodGet, "/v1/ops/failed-messages?limit=1&limit=2", "", http.StatusBadRequest},
		{http.MethodPost, "/v1/ops/failed-messages/replay", `{"source":"PVMBG","force":true}`, http.StatusBadRequest},
		{http.MethodPost, "/v1/ops/failed-messages/replay", "not json", http.StatusBadRequest},
	} {
		if got := opsRequest(handler, test.method, test.target, "ops-token", test.body); got.Code != test.want {
			t.Errorf("%s %s %q: HTTP %d, want %d", test.method, test.target, test.body, got.Code, test.want)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("invalid input reached the Aggregator %d times", calls.Load())
	}
}

func TestOpsRoutesHideAggregatorErrors(t *testing.T) {
	for upstream, want := range map[int]int{http.StatusBadRequest: http.StatusBadRequest, http.StatusForbidden: http.StatusServiceUnavailable,
		http.StatusServiceUnavailable: http.StatusServiceUnavailable} {
		var calls atomic.Int32
		var last atomic.Value
		handler := testPublicHandler(t, authDependency(t), opsAggregator(t, &calls, &last, upstream, `{"error":{"code":"invalid_credential"}}`))
		got := opsRequest(handler, http.MethodGet, "/v1/ops/failed-messages", "ops-token", "")
		if got.Code != want || strings.Contains(got.Body.String(), "invalid_credential") {
			t.Errorf("upstream %d: HTTP %d %s", upstream, got.Code, got.Body)
		}
	}
}
