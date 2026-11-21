package aggregator

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type dlqAPI struct {
	store *Store
	cfg   Config
}

// Served by every instance from the control store; the Audit proxy reaches it through the Aggregator Service.
func registerDLQRoutes(mux *http.ServeMux, store *Store, cfg Config, writeTimeout time.Duration) {
	if cfg.Audit.UnprocessedToken == "" {
		return
	}
	api := dlqAPI{store: store, cfg: cfg}
	budget := cfg.DBReadTimeout
	if budget <= 0 {
		budget = defaultDBReadTimeout
	}
	mux.Handle("GET /internal/dlq/unprocessed", api.authenticate(withAuditReadBudget(http.HandlerFunc(api.unprocessed), budget, writeTimeout)))
}

func (api dlqAPI) authenticate(next http.Handler) http.Handler {
	expected := sha256.Sum256([]byte(api.cfg.Audit.UnprocessedToken))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		credentials := r.Header.Values("Authorization")
		if len(credentials) == 0 {
			auditWriteError(w, http.StatusUnauthorized, "unauthorized", "token tidak valid")
			return
		}
		token, found := strings.CutPrefix(credentials[0], "Bearer ")
		provided := sha256.Sum256([]byte(token))
		if len(credentials) != 1 || !found || token == "" || subtle.ConstantTimeCompare(expected[:], provided[:]) != 1 {
			auditWriteError(w, http.StatusForbidden, "forbidden", "token tidak valid")
			return
		}
		next.ServeHTTP(w, r)
	})
}

type unprocessedPage struct {
	Items      []unprocessedItem `json:"items"`
	NextCursor *string           `json:"next_cursor"`
}

func (api dlqAPI) unprocessed(w http.ResponseWriter, r *http.Request) {
	query, ok := parseFailedQuery(w, r.URL.RawQuery)
	if !ok {
		return
	}
	messages, next, err := api.store.listFailedMessages(r.Context(), query)
	if err != nil {
		auditWriteError(w, http.StatusServiceUnavailable, "unavailable", "failed message store is unavailable")
		return
	}
	page := unprocessedPage{Items: make([]unprocessedItem, 0, len(messages)), NextCursor: next}
	for _, message := range messages {
		page.Items = append(page.Items, unprocessedItem{
			MessageID: message.MessageID, Source: string(message.Source),
			ReceivedAt: message.ReceivedAt.UTC().Format(time.RFC3339Nano), Reason: message.Reason,
			Retryable: message.Retryable, Attempts: message.Attempts, Status: message.Status,
		})
	}
	auditWriteJSON(w, http.StatusOK, page)
}

func parseFailedQuery(w http.ResponseWriter, rawQuery string) (failedQuery, bool) {
	query := failedQuery{limit: 100}
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		auditWriteError(w, http.StatusBadRequest, "invalid_query", "query parameters are invalid")
		return query, false
	}
	for key, value := range values {
		if key != "source" && key != "limit" && key != "cursor" || len(value) != 1 || value[0] == "" {
			auditWriteError(w, http.StatusBadRequest, "invalid_query", "query parameters are invalid")
			return query, false
		}
	}
	if source := values.Get("source"); source != "" {
		if !validUnprocessedSource(source) {
			auditWriteError(w, http.StatusBadRequest, "invalid_query", "source is invalid")
			return query, false
		}
		query.source = Source(source)
	}
	if raw := values.Get("limit"); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > 1000 {
			auditWriteError(w, http.StatusBadRequest, "invalid_query", "limit must be between 1 and 1000")
			return query, false
		}
		query.limit = limit
	}
	if raw := values.Get("cursor"); raw != "" {
		cursor, err := decodeFailedCursor(raw)
		if err != nil {
			auditWriteError(w, http.StatusBadRequest, "invalid_query", "cursor is invalid")
			return query, false
		}
		query.after = &cursor
	}
	return query, true
}
