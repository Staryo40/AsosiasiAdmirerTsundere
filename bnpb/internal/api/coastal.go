package api

import (
	"encoding/json"
	"net/http"
)

func (service *publicService) registerCoastalRoute(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/coastal/status", service.requireAccess(service.withAdmission(service.handleCoastalStatus)))
}

func (service *publicService) handleCoastalStatus(w http.ResponseWriter, request *http.Request, _ principal) {
	status, payload, err := service.aggregatorCall(request.Context(), "/internal/coastal/status", "")
	if err != nil {
		writePublicError(w, request, http.StatusServiceUnavailable, "dependency_unavailable", "coastal status is unavailable")
		return
	}
	if status == http.StatusOK {
		if !json.Valid(payload) {
			writePublicError(w, request, http.StatusServiceUnavailable, "dependency_unavailable", "coastal status returned an invalid response")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
		return
	}
	if status == http.StatusServiceUnavailable || status == http.StatusTooManyRequests {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(payload)
		return
	}
	writePublicError(w, request, http.StatusServiceUnavailable, "dependency_unavailable", "coastal status is unavailable")
}