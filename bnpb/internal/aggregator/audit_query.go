package aggregator

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

var errInvalidAuditQuery = errors.New("invalid audit query")

// Cursor tokens encode only the position; clients must reuse the same source
// and since filters for every page in a cursor sequence.
type auditQuery struct {
	Source      *Source
	Since       *time.Time
	Limit       int
	Cursor      *auditCursor
	CursorToken string
}
type auditCursor struct {
	Start      bool      `json:"start,omitempty"`
	IngestedAt time.Time `json:"ingested_at,omitempty"`
	HazardID   string    `json:"hazard_id,omitempty"`
}
type auditCursorWire struct {
	V          int    `json:"v"`
	Start      bool   `json:"start,omitempty"`
	IngestedAt string `json:"ingested_at,omitempty"`
	HazardID   string `json:"hazard_id,omitempty"`
}

func parseAuditQuery(raw string, events bool) (auditQuery, error) {
	query := auditQuery{Limit: 100}
	values, err := url.ParseQuery(raw)
	if err != nil {
		return query, errInvalidAuditQuery
	}
	allowed := map[string]bool{"source": true}
	if events {
		allowed["since"] = true
	}
	if events {
		allowed["limit"] = true
		allowed["cursor"] = true
	}
	for key, items := range values {
		if !allowed[key] || len(items) != 1 || items[0] == "" {
			return query, errInvalidAuditQuery
		}
	}
	if text, ok := values["source"]; ok {
		source := Source(text[0])
		if source != BMKG && source != PVMBG && source != Satelit && source != SensorPantai {
			return query, errInvalidAuditQuery
		}
		query.Source = &source
	}
	if text, ok := values["since"]; ok {
		parsed, parseErr := time.Parse(time.RFC3339Nano, text[0])
		if parseErr != nil || !strings.HasSuffix(text[0], "Z") && !hasExplicitOffset(text[0]) {
			return query, errInvalidAuditQuery
		}
		stamp := parsed.UTC()
		query.Since = &stamp
	}
	if text, ok := values["limit"]; ok {
		if len(text[0]) > 4 {
			return query, errInvalidAuditQuery
		}
		limit, parseErr := strconv.Atoi(text[0])
		if parseErr != nil || limit < 1 || limit > 1000 {
			return query, errInvalidAuditQuery
		}
		query.Limit = limit
	}
	if token, ok := values["cursor"]; ok {
		if len(token[0]) > 8192 || !utf8.ValidString(token[0]) {
			return query, errInvalidAuditQuery
		}
		cursor, parseErr := parseAuditCursor(token[0])
		if parseErr != nil {
			return query, errInvalidAuditQuery
		}
		query.Cursor, query.CursorToken = cursor, token[0]
	}
	return query, nil
}

func hasExplicitOffset(text string) bool {
	if len(text) < 6 {
		return false
	}
	zone := text[len(text)-6:]
	return (zone[0] == '+' || zone[0] == '-') && zone[3] == ':'
}

func parseAuditCursor(token string) (*auditCursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(raw) > 6144 {
		return nil, errInvalidAuditQuery
	}
	var wire auditCursorWire
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil || wire.V != 1 {
		return nil, errInvalidAuditQuery
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, errInvalidAuditQuery
	}
	if wire.Start {
		if wire.IngestedAt != "" || wire.HazardID != "" {
			return nil, errInvalidAuditQuery
		}
		return &auditCursor{Start: true}, nil
	}
	if wire.IngestedAt == "" || wire.HazardID == "" || len(wire.HazardID) > 2048 {
		return nil, errInvalidAuditQuery
	}
	stamp, err := time.Parse(time.RFC3339Nano, wire.IngestedAt)
	if err != nil || !strings.HasSuffix(wire.IngestedAt, "Z") || !validText(wire.HazardID) {
		return nil, errInvalidAuditQuery
	}
	return &auditCursor{IngestedAt: stamp.UTC(), HazardID: wire.HazardID}, nil
}

func encodeAuditStartCursor() string {
	encoded, _ := json.Marshal(auditCursorWire{V: 1, Start: true})
	return base64.RawURLEncoding.EncodeToString(encoded)
}
func encodeAuditCursor(item HazardEvent) string {
	encoded, _ := json.Marshal(auditCursorWire{V: 1, IngestedAt: item.IngestedAt.UTC().Format(time.RFC3339Nano), HazardID: item.HazardID})
	return base64.RawURLEncoding.EncodeToString(encoded)
}
