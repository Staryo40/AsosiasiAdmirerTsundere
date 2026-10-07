package aggregator

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode"
)

// Only the volcanic worker owns this cache; no lock spans a network request.
type readerSession struct {
	client                 *sourceClient
	clientID, clientSecret string
	token                  string
	renewAt                time.Time
}

func (session *readerSession) login(ctx context.Context) error {
	credentials, err := json.Marshal(struct {
		ClientID     string `json:"client_id"`
		ClientSecret string `json:"client_secret"`
	}{session.clientID, session.clientSecret})
	if err != nil {
		return &sourceError{category: "invalid_request"}
	}
	started := time.Now()
	var raw json.RawMessage
	client := *session.client
	client.maxBytes = min(client.maxBytes, 16<<10)
	if err := client.doJSON(ctx, sourceRequest{method: "POST", path: "/auth/login", body: credentials}, &raw); err != nil {
		return err
	}
	return session.cacheToken(raw, started)
}

func (session *readerSession) cacheToken(raw json.RawMessage, started time.Time) error {
	fields, err := parseObject(raw)
	if err != nil {
		return &sourceError{category: "invalid_json"}
	}
	token, tokenErr := textField(fields, "access_token")
	kind, kindErr := textField(fields, "token_type")
	seconds, ttlErr := required[int64](fields, "expires_in")
	if tokenErr != nil || kindErr != nil || ttlErr != nil || !strings.EqualFold(kind, "Bearer") ||
		seconds <= 0 || seconds > int64((1<<63-1)/time.Second) ||
		strings.IndexFunc(token, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
		return &sourceError{category: "invalid_json"}
	}
	ttl := time.Duration(seconds) * time.Second
	session.token, session.renewAt = token, started.Add(ttl-min(5*time.Second, ttl/10))
	return nil
}

func (session *readerSession) reports(ctx context.Context, query url.Values) ([]json.RawMessage, error) {
	if session.token == "" || !time.Now().Before(session.renewAt) {
		if err := session.login(ctx); err != nil {
			return nil, err
		}
	}
	records, err := session.fetchReports(ctx, query)
	var failure *sourceError
	if !errors.As(err, &failure) || failure.statusCode != http.StatusUnauthorized {
		return records, err
	}
	// The sole immediate retry is one fresh reader login after a rejected token.
	session.token = ""
	if err := session.login(ctx); err != nil {
		return nil, err
	}
	records, err = session.fetchReports(ctx, query)
	if errors.As(err, &failure) && failure.statusCode == http.StatusUnauthorized {
		session.token = ""
	}
	return records, err
}

func (session *readerSession) fetchReports(ctx context.Context, query url.Values) ([]json.RawMessage, error) {
	return session.client.fetchRecords(ctx, sourceRequest{method: "GET", path: "/volcanic-reports", query: query,
		header: http.Header{"Authorization": {"Bearer " + session.token}}})
}

func (client *sourceClient) fetchRecords(ctx context.Context, request sourceRequest) ([]json.RawMessage, error) {
	var records []json.RawMessage
	if err := client.doJSON(ctx, request, &records); err != nil {
		return nil, err
	}
	if records == nil {
		return nil, &sourceError{category: "invalid_json"}
	}
	return records, nil
}
