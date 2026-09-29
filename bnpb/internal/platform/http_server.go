package platform

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

func RegisterHealth(mux *http.ServeMux, service string) {
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"service": service, "status": "ok"})
	})
}

// HTTPHandler applies the shared request middleware to ordinary HTTP routes.
func HTTPHandler(next http.Handler, logger *slog.Logger, maxInFlight int) http.Handler {
	handler := limitRequests(next, maxInFlight)
	handler = recoverPanics(handler, logger)
	handler = logAccess(handler, logger)
	return correlate(handler)
}

func correlate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		values := r.Header.Values(CorrelationHeader)
		correlationID := ""
		if len(values) == 1 && correlationPattern.MatchString(values[0]) {
			correlationID = values[0]
		} else {
			correlationID = newCorrelationID()
		}
		w.Header().Set(CorrelationHeader, correlationID)
		next.ServeHTTP(w, r.WithContext(WithCorrelationID(r.Context(), correlationID)))
	})
}

func limitRequests(next http.Handler, maximum int) http.Handler {
	semaphore := make(chan struct{}, maximum)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" && (r.Method == http.MethodGet || r.Method == http.MethodHead) {
			next.ServeHTTP(w, r)
			return
		}
		select {
		case semaphore <- struct{}{}:
			defer func() { <-semaphore }()
			next.ServeHTTP(w, r)
		default:
			writeJSONError(w, http.StatusTooManyRequests, "too many requests")
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
			logEvent(r.Context(), logger, logRecord{slog.LevelError, "http_panic", "recovered HTTP handler panic"})
			if recovered == http.ErrAbortHandler || responseStarted(w) {
				panic(http.ErrAbortHandler)
			}
			// Headers prepared for the failed response may corrupt the JSON error.
			clear(w.Header())
			w.Header().Set(CorrelationHeader, CorrelationID(r.Context()))
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
	logEvent(request.Context(), logger, logRecord{accessLevel(request.URL.Path, status), "http_request", "HTTP request completed"},
		"method", request.Method,
		"path", request.URL.Path,
		"status", status,
		"duration_ms", time.Since(start).Milliseconds(),
	)
}

func accessLevel(path string, status int) slog.Level {
	switch {
	case status >= 500:
		return slog.LevelError
	case status >= 400:
		return slog.LevelWarn
	case path == "/health":
		return slog.LevelDebug
	default:
		return slog.LevelInfo
	}
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
