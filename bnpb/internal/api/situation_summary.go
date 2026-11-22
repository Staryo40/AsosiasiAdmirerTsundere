package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/tracing"
)

const (
	// The summary is decoration; a slow summarizer must not hold an API worker.
	summarizerTimeout   = 2 * time.Second
	maximumSummaryBytes = 64 << 10
)

func (service *publicService) registerSituationSummaryRoute(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/situation-summary", service.requireAccess(service.withAdmission(service.handleSituationSummary)))
}

func (service *publicService) handleSituationSummary(w http.ResponseWriter, request *http.Request, _ principal) {
	if service.config.summarizerKey == "" {
		writePublicError(w, request, http.StatusServiceUnavailable, "dependency_unavailable", "situation summary is not configured")
		return
	}
	status, payload, err := service.summarizerCall(request.Context())
	if err != nil || status != http.StatusOK || !json.Valid(payload) {
		writePublicError(w, request, http.StatusServiceUnavailable, "dependency_unavailable", "situation summary is unavailable")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(payload)
}

func (service *publicService) summarizerCall(ctx context.Context) (int, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, summarizerTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, joinedURL(service.config.summarizerBaseURL, "/internal/situation-summary", ""), nil)
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set(aggregatorInternalKeyHeader, service.config.summarizerKey)
	if parent := tracing.Parent(ctx); parent != "" {
		request.Header.Set(tracing.Header, parent)
	}
	response, err := service.client.Do(request)
	if err != nil {
		return 0, nil, fmt.Errorf("summarizer request: %w", err)
	}
	defer response.Body.Close()
	payload, err := readBounded(response.Body, maximumSummaryBytes)
	if err != nil {
		return response.StatusCode, nil, fmt.Errorf("summarizer response: %w", err)
	}
	return response.StatusCode, payload, nil
}
