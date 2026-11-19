package aggregator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/correlation"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/httpclient"
)

const maxUnprocessedResponseBytes = 1 << 20

var auditUnprocessedHTTPClient = httpclient.New(time.Minute, slog.Default())

type unprocessedEnvelope struct {
	Items      []unprocessedItem `json:"items"`
	NextCursor *string           `json:"next_cursor"`
}

type unprocessedItem struct {
	MessageID  string `json:"message_id"`
	Source     string `json:"source"`
	ReceivedAt string `json:"received_at"`
	Reason     string `json:"reason"`
	Retryable  bool   `json:"retryable"`
	Attempts   int    `json:"attempts"`
	Status     string `json:"status"`
}

func (api auditAPI) unprocessed(w http.ResponseWriter, request *http.Request) {
	query, queryErr := url.ParseQuery(request.URL.RawQuery)
	if queryErr != nil {
		auditWriteError(w, http.StatusBadRequest, "invalid_query", "query parameters are invalid")
		return
	}
	for key, values := range query {
		if key != "source" && key != "limit" && key != "cursor" || len(values) != 1 || values[0] == "" {
			auditWriteError(w, http.StatusBadRequest, "invalid_query", "query parameters are invalid")
			return
		}
	}
	if raw, ok := query["cursor"]; ok && len(raw[0]) > 4096 {
		auditWriteError(w, http.StatusBadRequest, "invalid_query", "cursor is invalid")
		return
	}
	limit := 100
	if raw, ok := query["limit"]; ok {
		parsed, err := strconv.Atoi(raw[0])
		if err != nil || parsed < 1 || parsed > 1000 {
			auditWriteError(w, http.StatusBadRequest, "invalid_query", "limit must be between 1 and 1000")
			return
		}
		limit = parsed
	}
	source := query.Get("source")
	if source != "" && !validUnprocessedSource(source) {
		auditWriteError(w, http.StatusBadRequest, "invalid_query", "source is invalid")
		return
	}
	if api.cfg.Audit.UnprocessedURL == "" || api.cfg.Audit.UnprocessedToken == "" {
		auditWriteError(w, http.StatusServiceUnavailable, "unavailable", "unprocessed metadata service is unavailable")
		return
	}
	endpoint, err := url.Parse(api.cfg.Audit.UnprocessedURL)
	if err != nil || endpoint.Scheme == "" || endpoint.Host == "" {
		auditWriteError(w, http.StatusServiceUnavailable, "unavailable", "unprocessed metadata service is unavailable")
		return
	}
	forward := endpoint.Query()
	if source != "" {
		forward.Set("source", source)
	}
	forward.Set("limit", strconv.Itoa(limit))
	if value, ok := query["cursor"]; ok {
		forward.Set("cursor", value[0])
	}
	endpoint.RawQuery = forward.Encode()
	budget := api.cfg.DBReadTimeout
	if budget <= 0 {
		budget = 200 * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(request.Context(), budget)
	defer cancel()
	outgoing, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		auditWriteError(w, http.StatusServiceUnavailable, "unavailable", "unprocessed metadata service is unavailable")
		return
	}
	outgoing.Header.Set("Authorization", "Bearer "+api.cfg.Audit.UnprocessedToken)
	outgoing.Header.Set(correlation.Header, correlation.ID(request.Context()))
	response, err := auditUnprocessedHTTPClient.Do(outgoing)
	if err != nil {
		auditWriteError(w, http.StatusServiceUnavailable, "unavailable", "unprocessed metadata service is unavailable")
		return
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		auditWriteError(w, http.StatusServiceUnavailable, "unavailable", "unprocessed metadata service is unavailable")
		return
	}
	maxBytes := api.cfg.MaxResponseBytes
	if maxBytes <= 0 || maxBytes > maxUnprocessedResponseBytes {
		maxBytes = maxUnprocessedResponseBytes
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxBytes+1))
	if err != nil || int64(len(body)) > maxBytes {
		auditWriteError(w, http.StatusServiceUnavailable, "unavailable", "unprocessed metadata service is unavailable")
		return
	}
	upstream, ok := decodeUnprocessedEnvelope(body, limit, source)
	if !ok {
		auditWriteError(w, http.StatusServiceUnavailable, "unavailable", "unprocessed metadata service returned invalid metadata")
		return
	}
	auditWriteJSON(w, http.StatusOK, upstream)
}

func decodeUnprocessedEnvelope(body []byte, limit int, source string) (unprocessedEnvelope, bool) {
	var raw map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(&raw); err != nil || raw == nil {
		return unprocessedEnvelope{}, false
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return unprocessedEnvelope{}, false
	}
	itemsRaw, hasItems := raw["items"]
	cursorRaw, hasCursor := raw["next_cursor"]
	if !hasItems || !hasCursor {
		return unprocessedEnvelope{}, false
	}
	var rawItems []json.RawMessage
	if err := json.Unmarshal(itemsRaw, &rawItems); err != nil || rawItems == nil || len(rawItems) > limit {
		return unprocessedEnvelope{}, false
	}
	var nextCursor *string
	if !bytes.Equal(bytes.TrimSpace(cursorRaw), []byte("null")) {
		var cursor string
		if err := json.Unmarshal(cursorRaw, &cursor); err != nil || len(cursor) == 0 || len(cursor) > 4096 {
			return unprocessedEnvelope{}, false
		}
		nextCursor = &cursor
	}
	items := make([]unprocessedItem, 0, len(rawItems))
	for _, rawItem := range rawItems {
		item, ok := decodeUnprocessedItem(rawItem)
		if !ok || (source != "" && item.Source != source) {
			return unprocessedEnvelope{}, false
		}
		items = append(items, item)
	}
	return unprocessedEnvelope{Items: items, NextCursor: nextCursor}, true
}

func decodeUnprocessedItem(raw json.RawMessage) (unprocessedItem, bool) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return unprocessedItem{}, false
	}
	var item unprocessedItem
	if !decodeField(fields, "message_id", &item.MessageID) ||
		!decodeField(fields, "source", &item.Source) ||
		!decodeField(fields, "received_at", &item.ReceivedAt) ||
		!decodeField(fields, "reason", &item.Reason) ||
		!decodeField(fields, "retryable", &item.Retryable) ||
		!decodeField(fields, "attempts", &item.Attempts) ||
		!decodeField(fields, "status", &item.Status) {
		return unprocessedItem{}, false
	}
	return item, validUnprocessedItem(item)
}

func decodeField(fields map[string]json.RawMessage, name string, target any) bool {
	value, exists := fields[name]
	return exists && !bytes.Equal(bytes.TrimSpace(value), []byte("null")) && json.Unmarshal(value, target) == nil
}

func validUnprocessedSource(source string) bool {
	switch Source(source) {
	case BMKG, PVMBG, Satelit, SensorPantai:
		return true
	default:
		return false
	}
}

func validUnprocessedItem(item unprocessedItem) bool {
	if item.MessageID == "" || !validUnprocessedSource(item.Source) || item.Reason == "" || item.Attempts < 0 {
		return false
	}
	if item.Status != "retrying" && item.Status != "parked" {
		return false
	}
	parsed, err := time.Parse(time.RFC3339Nano, item.ReceivedAt)
	if err != nil {
		return false
	}
	_, offset := parsed.Zone()
	return offset == 0
}
