package aggregator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/correlation"
)

const (
	dlqForwardedHeader = "X-BNPB-DLQ-Forwarded"
	// Below the Client-Facing API's 5s outbound timeout, leaving room for one owner hop.
	replayBudget     = 3 * time.Second
	messagesMaxLimit = 20
)

type dlqAPI struct {
	store     *Store
	cfg       Config
	authority *PollAuthority
}

// Served by every instance from the control store; the Audit proxy reaches it through the Aggregator Service.
func registerDLQRoutes(mux *http.ServeMux, store *Store, cfg Config, writeTimeout time.Duration, authority *PollAuthority) {
	api := dlqAPI{store: store, cfg: cfg, authority: authority}
	mux.Handle("GET /internal/dlq/messages", api.internalOnly(http.HandlerFunc(api.messages)))
	mux.Handle("POST /internal/dlq/replay", api.internalOnly(http.HandlerFunc(api.replay)))
	if cfg.Audit.UnprocessedToken == "" {
		return
	}
	budget := cfg.DBReadTimeout
	if budget <= 0 {
		budget = defaultDBReadTimeout
	}
	mux.Handle("GET /internal/dlq/unprocessed", api.authenticate(withAuditReadBudget(http.HandlerFunc(api.unprocessed), budget, writeTimeout)))
}

func (api dlqAPI) internalOnly(next http.Handler) http.Handler {
	expected := sha256.Sum256([]byte(api.cfg.InternalKey))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if code := (snapshotAPI{cfg: api.cfg}).credentialErrorCode(r, expected); code != "" {
			status := http.StatusForbidden
			if code == "missing_credential" {
				status = http.StatusUnauthorized
			}
			writeReadError(w, r, status, code)
			return
		}
		next.ServeHTTP(w, r)
	})
}

type failedMessageView struct {
	MessageID        string  `json:"message_id"`
	Source           string  `json:"source"`
	Endpoint         string  `json:"endpoint"`
	Kind             string  `json:"kind"`
	SourceRefID      *string `json:"source_ref_id"`
	ReceivedAt       string  `json:"received_at"`
	LastSeenAt       string  `json:"last_seen_at"`
	Reason           string  `json:"reason"`
	Retryable        bool    `json:"retryable"`
	Attempts         int     `json:"attempts"`
	Status           string  `json:"status"`
	PayloadBase64    string  `json:"payload_base64"`
	PayloadBytes     int64   `json:"payload_bytes"`
	PayloadTruncated bool    `json:"payload_truncated"`
}

func (api dlqAPI) messages(w http.ResponseWriter, r *http.Request) {
	query, ok := parseFailedQuery(w, r.URL.RawQuery, messagesMaxLimit, messagesMaxLimit)
	if !ok {
		return
	}
	query.withPayloads = true
	messages, next, err := api.store.listFailedMessages(r.Context(), query)
	if err != nil {
		writeReadError(w, r, http.StatusServiceUnavailable, "storage_unavailable")
		return
	}
	items := make([]failedMessageView, 0, len(messages))
	for _, message := range messages {
		items = append(items, failedMessageView{
			MessageID: message.MessageID, Source: string(message.Source), Endpoint: string(message.Endpoint), Kind: message.Kind,
			SourceRefID: message.SourceRefID, ReceivedAt: message.ReceivedAt.UTC().Format(time.RFC3339Nano),
			LastSeenAt: message.LastSeenAt.UTC().Format(time.RFC3339Nano), Reason: message.Reason, Retryable: message.Retryable,
			Attempts: message.Attempts, Status: message.Status, PayloadBase64: base64.StdEncoding.EncodeToString(message.Payload),
			PayloadBytes: message.PayloadBytes, PayloadTruncated: message.PayloadTruncated,
		})
	}
	writeReadJSON(w, http.StatusOK, map[string]any{"items": items, "next_cursor": next})
}

type replayRequest struct {
	Source   string `json:"source"`
	Continue string `json:"continue"`
}

func (api dlqAPI) replay(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 4096))
	if err != nil {
		writeReadError(w, r, http.StatusBadRequest, "invalid_request")
		return
	}
	var request replayRequest
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if len(bytes.TrimSpace(body)) > 0 && decoder.Decode(&request) != nil ||
		request.Source != "" && !validUnprocessedSource(request.Source) {
		writeReadError(w, r, http.StatusBadRequest, "invalid_request")
		return
	}
	round := databaseTime(time.Now())
	if request.Continue != "" {
		round, err = time.Parse(time.RFC3339Nano, request.Continue)
		if err != nil {
			writeReadError(w, r, http.StatusBadRequest, "invalid_request")
			return
		}
	}
	ctx, local := api.ownerContext(w, r, body)
	if !local {
		return
	}
	result, err := api.store.replayParked(ctx, Source(request.Source), round, replayBudget)
	if err != nil {
		code := "storage_unavailable"
		if errors.Is(err, errAuthorityUnavailable) || errors.Is(err, errStaleMutationAuthority) {
			code = "authority_unavailable"
		}
		writeReadError(w, r, http.StatusServiceUnavailable, code)
		return
	}
	writeReadJSON(w, http.StatusOK, result)
}

// Replay writes like a poll, so only the poll owner may run it; other instances forward once.
func (api dlqAPI) ownerContext(w http.ResponseWriter, r *http.Request, body []byte) (context.Context, bool) {
	if api.authority == nil {
		return r.Context(), true
	}
	ctx, err := api.authority.currentContext(r.Context())
	if err == nil {
		return ctx, true
	}
	endpoint, ownerErr := api.authority.activeOwnerEndpoint(r.Context())
	if r.Header.Get(dlqForwardedHeader) != "" || ownerErr != nil || endpoint == "" {
		writeReadError(w, r, http.StatusServiceUnavailable, "authority_unavailable")
		return nil, false
	}
	forward, err := http.NewRequestWithContext(r.Context(), http.MethodPost, strings.TrimRight(endpoint, "/")+"/internal/dlq/replay", bytes.NewReader(body))
	if err != nil {
		writeReadError(w, r, http.StatusServiceUnavailable, "authority_unavailable")
		return nil, false
	}
	forward.Header.Set(internalKeyHeader, r.Header.Get(internalKeyHeader))
	forward.Header.Set("Content-Type", "application/json")
	forward.Header.Set(correlation.Header, correlation.ID(r.Context()))
	correlation.InjectTraceHeaders(r.Context(), forward.Header)
	forward.Header.Set(dlqForwardedHeader, "1")
	catalogAPI{}.relayCatalog(w, r, forward)
	return nil, false
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
	query, ok := parseFailedQuery(w, r.URL.RawQuery, 100, 1000)
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

func parseFailedQuery(w http.ResponseWriter, rawQuery string, defaultLimit, maxLimit int) (failedQuery, bool) {
	query := failedQuery{limit: defaultLimit}
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
		if err != nil || limit < 1 || limit > maxLimit {
			auditWriteError(w, http.StatusBadRequest, "invalid_query", "limit must be between 1 and "+strconv.Itoa(maxLimit))
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
