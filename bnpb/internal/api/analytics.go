package api

import (
	"encoding/json"
	"net/http"
	"net/url"
)

var analyticsParams = map[string]bool{"from": true, "to": true, "bucket": true}

func (service *publicService) registerAnalyticsRoute(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/analytics", service.requireAccess(service.withAdmission(service.handleAnalytics)))
}

// handleAnalytics forwards aggregate counts; every role may read them since they carry no hazard records.
func (service *publicService) handleAnalytics(w http.ResponseWriter, request *http.Request, _ principal) {
	query, err := url.ParseQuery(request.URL.RawQuery)
	if err != nil || query.Get("from") == "" || query.Get("to") == "" {
		writePublicError(w, request, http.StatusBadRequest, "invalid_query", "from and to are required RFC 3339 timestamps")
		return
	}
	for name := range query {
		if !analyticsParams[name] {
			writePublicError(w, request, http.StatusBadRequest, "invalid_query", "only from, to and bucket are allowed")
			return
		}
	}
	status, payload, err := service.aggregatorCall(request.Context(), "/internal/analytics", query.Encode())
	switch {
	case err != nil || !json.Valid(payload):
		writePublicError(w, request, http.StatusServiceUnavailable, "dependency_unavailable", "analytics are unavailable")
	case status == http.StatusOK || status == http.StatusBadRequest || status == http.StatusTooManyRequests:
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(status)
		_, _ = w.Write(payload)
	default:
		writePublicError(w, request, http.StatusServiceUnavailable, "dependency_unavailable", "analytics are unavailable")
	}
}
