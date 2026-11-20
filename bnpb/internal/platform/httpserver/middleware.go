// Package httpserver provides HTTP middleware, health probes, and server lifecycle.
package httpserver

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/correlation"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/tracing"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/logging"
)

// RegisterHealth adds the GET /health endpoint to mux.
func RegisterHealth(mux *http.ServeMux, service string) {
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"service": service, "status": "ok"})
	})
}

// Handler applies the shared request middleware to ordinary HTTP routes.
func Handler(next http.Handler, logger *slog.Logger, maxInFlight int) http.Handler {
	handler := limitRequests(next, maxInFlight)
	handler = recoverPanics(handler, logger)
	handler = logAccess(handler, logger)
	return correlate(handler)
}

func correlate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		values := r.Header.Values(correlation.Header)
		correlationID := ""
		if len(values) == 1 && correlation.ValidID(values[0]) {
			correlationID = values[0]
		} else {
			correlationID = correlation.NewID()
		}
		w.Header().Set(correlation.Header, correlationID)
		ctx := correlation.WithTrace(correlation.WithID(r.Context(), correlationID), r.Header)
		ctx, traceparent := tracing.Ensure(r.WithContext(ctx))
		w.Header().Set(tracing.Header, traceparent)
		correlated := r.WithContext(ctx)
		next.ServeHTTP(w, correlated)
		r.Pattern = correlated.Pattern
	})
}

func limitRequests(next http.Handler, maximum int) http.Handler {
	semaphore := make(chan struct{}, maximum)
	diagnosticSlots := make(chan struct{}, 2)
	readinessSlots := make(chan struct{}, 1)
	metricsSlots := make(chan struct{}, 1)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		slots := semaphore
		if r.Method == http.MethodGet || r.Method == http.MethodHead {
			switch r.URL.Path {
			case "/health":
				next.ServeHTTP(w, r)
				return
			case "/ready":
				slots = diagnosticSlots
			case "/readyz":
				// Diagnostic and scrape traffic must not crowd out the kube probe.
				slots = readinessSlots
			case "/metrics":
				slots = metricsSlots
			}
		}
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
			next.ServeHTTP(w, r)
		default:
			if slots == readinessSlots {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			if strings.HasPrefix(r.URL.Path, "/internal/audit/") {
				writeAuditRateLimitError(w)
			} else {
				writeJSONError(w, http.StatusTooManyRequests, "too many requests")
			}
		}
	})
}

func recoverPanics(next http.Handler, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			recovered := recover()
			if recovered == nil {
				return
			}
			logging.Event(r.Context(), logger, logging.Record{Level: slog.LevelError, Event: "http_panic", Message: "recovered HTTP handler panic"})
			if recovered == http.ErrAbortHandler || responseStarted(w) {
				panic(http.ErrAbortHandler)
			}
			// Headers prepared for the failed response may corrupt the JSON error.
			clear(w.Header())
			w.Header().Set(correlation.Header, correlation.ID(r.Context()))
			writeJSONError(w, http.StatusInternalServerError, "internal server error")
		}()
		next.ServeHTTP(w, r)
	})
}

func logAccess(next http.Handler, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &responseRecorder{ResponseWriter: w}
		defer logRequest(r, logger, recorder, start)
		next.ServeHTTP(recorder, r)
	})
}

func logRequest(request *http.Request, logger *slog.Logger, recorder *responseRecorder, start time.Time) {
	status := recorder.Status()
	duration := time.Since(start)
	observeRequest(request, status, duration)
	logging.Event(request.Context(), logger, logging.Record{Level: accessLevel(request.URL.Path, status), Event: "http_request", Message: "HTTP request completed"},
		"method", request.Method,
		"path", request.URL.Path,
		"status", status,
		"duration_ms", duration.Milliseconds(),
		"traceparent", tracing.Parent(request.Context()),
	)
}

func accessLevel(path string, status int) slog.Level {
	if path == "/health" && status < 400 {
		return slog.LevelDebug
	}
	return logging.HTTPLevel(status)
}

type responseRecorder struct {
	http.ResponseWriter
	status int
}

func (w *responseRecorder) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.ResponseWriter.WriteHeader(status)
	// Informational responses leave the final response open, except upgrades.
	if status >= 200 || status == http.StatusSwitchingProtocols {
		w.status = status
	}
}

func (w *responseRecorder) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(body)
}

func (w *responseRecorder) Status() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

func (w *responseRecorder) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *responseRecorder) FlushError() error {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return http.NewResponseController(w.ResponseWriter).Flush()
}

func responseStarted(w http.ResponseWriter) bool {
	recorder, ok := w.(*responseRecorder)
	return ok && recorder.status != 0
}

func writeJSONError(w http.ResponseWriter, status int, publicMessage string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": publicMessage})
}
func writeAuditRateLimitError(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusTooManyRequests)
	_ = json.NewEncoder(w).Encode(map[string]map[string]string{
		"error": {"code": "too_many_requests", "message": "too many requests"},
	})
}
