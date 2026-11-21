package api

import (
	"encoding/json"
	"net/http"
	"net/url"
)

type replayPayload struct {
	Source   string `json:"source,omitempty"`
	Continue string `json:"continue,omitempty"`
}

func (service *publicService) registerOpsRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/ops/failed-messages", service.requireAccess(requireOps(service.handleFailedMessages)))
	mux.HandleFunc("POST /v1/ops/failed-messages/replay", service.requireAccess(requireOps(service.handleReplay)))
}

// Failed payloads are raw partner data, so only BNPB Internal Ops may read or replay them.
func requireOps(next func(http.ResponseWriter, *http.Request, principal)) func(http.ResponseWriter, *http.Request, principal) {
	return func(w http.ResponseWriter, request *http.Request, identity principal) {
		preventCaching(w)
		if identity.role != roleOps {
			writePublicError(w, request, http.StatusForbidden, "forbidden", "only internal operations may access failed messages")
			return
		}
		next(w, request, identity)
	}
}

func (service *publicService) handleFailedMessages(w http.ResponseWriter, request *http.Request, _ principal) {
	values, err := url.ParseQuery(request.URL.RawQuery)
	if err != nil {
		writePublicError(w, request, http.StatusBadRequest, "invalid_query", "query parameters are invalid")
		return
	}
	for key, value := range values {
		if key != "source" && key != "limit" && key != "cursor" || len(value) != 1 {
			writePublicError(w, request, http.StatusBadRequest, "invalid_query", "allowed parameters are source, limit and cursor, once each")
			return
		}
	}
	status, payload, err := service.aggregatorCall(request.Context(), "/internal/dlq/messages", values.Encode())
	service.writeOpsResult(w, request, status, payload, err, "invalid_query")
}

func (service *publicService) handleReplay(w http.ResponseWriter, request *http.Request, _ principal) {
	var input replayPayload
	if err := decodePublicJSON(w, request, &input); err != nil {
		writeDecodeError(w, request, err, "request body must be a JSON object with optional source and continue")
		return
	}
	body, err := json.Marshal(input)
	if err != nil {
		writePublicError(w, request, http.StatusBadRequest, "invalid_request", "request body is invalid")
		return
	}
	status, payload, err := service.aggregatorRequest(request.Context(), http.MethodPost, "/internal/dlq/replay", "", body)
	service.writeOpsResult(w, request, status, payload, err, "invalid_request")
}

// Aggregator errors are not passed through: they name internal routes and credentials.
func (service *publicService) writeOpsResult(w http.ResponseWriter, request *http.Request, status int, payload []byte, callErr error, badRequestCode string) {
	switch {
	case callErr == nil && status == http.StatusOK && json.Valid(payload):
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	case callErr == nil && status == http.StatusBadRequest:
		writePublicError(w, request, http.StatusBadRequest, badRequestCode, "the request parameters were rejected")
	default:
		writePublicError(w, request, http.StatusServiceUnavailable, "dependency_unavailable", "failed message service is unavailable")
	}
}
