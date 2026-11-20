package httpserver

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLimiterUsesAuditErrorEnvelopeOnlyForAuditRoutes(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	handler := limitRequests(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(http.StatusNoContent)
	}), 1)

	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/ordinary", nil))
	}()
	<-entered

	auditResponse := httptest.NewRecorder()
	handler.ServeHTTP(auditResponse, httptest.NewRequest(http.MethodGet, "/internal/audit/events", nil))
	if auditResponse.Code != http.StatusTooManyRequests || auditResponse.Header().Get("Content-Type") != "application/json" || auditResponse.Body.String() != "{\"error\":{\"code\":\"too_many_requests\",\"message\":\"too many requests\"}}\n" {
		t.Fatalf("unexpected audit overload response: status=%d content-type=%q body=%q", auditResponse.Code, auditResponse.Header().Get("Content-Type"), auditResponse.Body.String())
	}

	ordinaryResponse := httptest.NewRecorder()
	handler.ServeHTTP(ordinaryResponse, httptest.NewRequest(http.MethodGet, "/ordinary", nil))
	if ordinaryResponse.Code != http.StatusTooManyRequests || ordinaryResponse.Body.String() != "{\"error\":\"too many requests\"}\n" {
		t.Fatalf("unexpected ordinary overload response: status=%d body=%q", ordinaryResponse.Code, ordinaryResponse.Body.String())
	}

	close(release)
	<-firstDone
}
