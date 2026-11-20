package aggregator

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/correlation"
)

func TestAuditUnprocessedMetadataProxyLifecycle(t *testing.T) {
	state := "retrying"
	calls := 0
	var seenQueries []url.Values
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Authorization") != "Bearer member3-service-secret" {
			t.Errorf("upstream authorization = %q", r.Header.Get("Authorization"))
		}
		if r.Header.Get("Authorization") == "Bearer audit-secret" {
			t.Error("audit credential crossed service boundary")
		}
		if r.Header.Get(correlation.Header) != "audit-correlation-1" {
			t.Errorf("correlation header = %q", r.Header.Get(correlation.Header))
		}
		seenQueries = append(seenQueries, r.URL.Query())
		w.Header().Set("Content-Type", "application/json")
		if state == "gone" {
			_, _ = w.Write([]byte(`{"items":[],"next_cursor":null,"partial":false,"unavailable_units":[]}`))
			return
		}
		items := `{"message_id":"msg-1","source":"PVMBG","received_at":"2026-11-18T12:13:14Z","reason":"schema incompatible","retryable":true,"attempts":2,"status":"` + state + `","raw_payload":"SECRET-RAW"}`
		if state == "parked" {
			items += `,{"message_id":"msg-2","source":"BMKG","received_at":"2026-11-18T12:13:15Z","reason":"schema incompatible","retryable":false,"attempts":2,"status":"parked"}`
		}
		_, _ = w.Write([]byte(`{"items":[` + items + `],"next_cursor":"opaque/next?x=1"}`))
	}))
	defer upstream.Close()
	api := auditAPI{cfg: Config{DBReadTimeout: time.Second, MaxResponseBytes: 2048, Audit: AuditConfig{Token: "audit-secret", UnprocessedURL: upstream.URL + "/metadata", UnprocessedToken: "member3-service-secret"}}}
	ctx := correlation.WithID(t.Context(), "audit-correlation-1")
	request := func(query string) *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/internal/audit/unprocessed?"+query, nil).WithContext(ctx)
		api.unprocessed(recorder, r)
		return recorder
	}
	first := request("source=PVMBG&limit=17&cursor=old%2Fcursor%3Fx%3D1")
	if first.Code != http.StatusOK {
		t.Fatalf("retrying status=%d body=%s", first.Code, first.Body)
	}
	if got := first.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("content type=%q", got)
	}
	var result unprocessedEnvelope
	if err := json.Unmarshal(first.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 1 || result.Items[0].Status != "retrying" || result.NextCursor == nil || *result.NextCursor != "opaque/next?x=1" {
		t.Fatalf("retrying response=%+v", result)
	}
	if strings.Contains(first.Body.String(), "SECRET-RAW") {
		t.Fatal("raw payload escaped metadata projection")
	}
	if q := seenQueries[0]; q.Get("source") != "PVMBG" || q.Get("limit") != "17" || q.Get("cursor") != "old/cursor?x=1" {
		t.Fatalf("forwarded query=%v", q)
	}
	state = "parked"
	second := request("limit=100")
	if second.Code != http.StatusOK {
		t.Fatalf("parked status=%d body=%s", second.Code, second.Body)
	}
	if err := json.Unmarshal(second.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 2 || result.Items[0].Source != "PVMBG" || result.Items[0].Status != "parked" || result.Items[1].Source != "BMKG" {
		t.Fatalf("parked response=%+v", result)
	}
	mismatch := request("source=PVMBG")
	if mismatch.Code != http.StatusServiceUnavailable || mismatch.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("mismatched source status=%d content type=%q body=%s", mismatch.Code, mismatch.Header().Get("Content-Type"), mismatch.Body)
	}
	var mismatchError auditError
	wantError := auditError{Error: auditErrorDetail{Code: "unavailable", Message: "unprocessed metadata service returned invalid metadata"}}
	if err := json.Unmarshal(mismatch.Body.Bytes(), &mismatchError); err != nil || mismatchError != wantError {
		t.Fatalf("mismatched source error=%s decode err=%v", mismatch.Body, err)
	}
	state = "gone"
	last := request("")
	if last.Code != http.StatusOK {
		t.Fatalf("gone status=%d body=%s", last.Code, last.Body)
	}
	if err := json.Unmarshal(last.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(last.Body.String()) != `{"items":[],"next_cursor":null}` || calls != 4 {
		t.Fatalf("recovered response=%+v calls=%d", result, calls)
	}
}

func TestAuditUnprocessedRejectsInvalidQueryAndMetadata(t *testing.T) {
	for _, query := range []string{"unknown=x", "source=PVMBG&source=BMKG", "limit=1001", "limit=0", "source=", "%zz"} {
		t.Run(query, func(t *testing.T) {
			api := auditAPI{cfg: Config{Audit: AuditConfig{UnprocessedURL: "https://member3.invalid/metadata", UnprocessedToken: "token"}}}
			recorder := httptest.NewRecorder()
			api.unprocessed(recorder, &http.Request{Method: http.MethodGet, URL: &url.URL{Path: "/", RawQuery: query}, Header: make(http.Header), Body: http.NoBody})
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body)
			}
		})
	}
	valid := unprocessedItem{MessageID: "m", Source: "PVMBG", ReceivedAt: "2026-11-18T00:00:00Z", Reason: "reason", Attempts: 0, Status: "retrying"}
	if !validUnprocessedItem(valid) {
		t.Fatal("valid metadata rejected")
	}
	for name, mutate := range map[string]func(*unprocessedItem){
		"offset":   func(item *unprocessedItem) { item.ReceivedAt = "2026-11-18T01:00:00+01:00" },
		"source":   func(item *unprocessedItem) { item.Source = "UNKNOWN" },
		"status":   func(item *unprocessedItem) { item.Status = "gone" },
		"attempts": func(item *unprocessedItem) { item.Attempts = -1 },
	} {
		t.Run(name, func(t *testing.T) {
			item := valid
			mutate(&item)
			if validUnprocessedItem(item) {
				t.Fatal("invalid metadata accepted")
			}
		})
	}
}

func TestAuditUnprocessedUnavailableAndMalformedResponsesFailClosed(t *testing.T) {
	for name, handler := range map[string]http.HandlerFunc{
		"missing_fields": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"items":[{"message_id":"m"}],"next_cursor":null}`))
		},
		"missing_retryable": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"items":[{"message_id":"m","source":"PVMBG","received_at":"2026-11-18T00:00:00Z","reason":"r","attempts":0,"status":"retrying"}],"next_cursor":null}`))
		},
		"missing_attempts": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"items":[{"message_id":"m","source":"PVMBG","received_at":"2026-11-18T00:00:00Z","reason":"r","retryable":false,"status":"parked"}],"next_cursor":null}`))
		},
		"missing_cursor": func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"items":[]}`)) },
		"empty_cursor": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"items":[],"next_cursor":""}`))
		},
		"partial_terminal": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"items":[],"next_cursor":null,"partial":true,"unavailable_units":["owner-shard-1"]}`))
		},
		"unavailable_without_partial": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"items":[],"next_cursor":null,"unavailable_units":["owner-shard-1"]}`))
		},
		"malformed_partial": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"items":[],"next_cursor":null,"partial":null}`))
		},
		"duplicate_partial": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"items":[],"next_cursor":null,"partial":true,"partial":false}`))
		},
		"null_retryable": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"items":[{"message_id":"m","source":"PVMBG","received_at":"2026-11-18T00:00:00Z","reason":"r","retryable":null,"attempts":0,"status":"retrying"}],"next_cursor":null}`))
		},
		"oversized":   func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(strings.Repeat("x", 1024))) },
		"unavailable": func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "down", http.StatusServiceUnavailable) },
	} {
		t.Run(name, func(t *testing.T) {
			upstream := httptest.NewServer(handler)
			defer upstream.Close()
			api := auditAPI{cfg: Config{DBReadTimeout: time.Second, MaxResponseBytes: 512, Audit: AuditConfig{UnprocessedURL: upstream.URL, UnprocessedToken: "service-token"}}}
			recorder := httptest.NewRecorder()
			api.unprocessed(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
			if recorder.Code != http.StatusServiceUnavailable {
				t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body)
			}
			if strings.Contains(recorder.Body.String(), "down") || strings.Contains(recorder.Body.String(), "message_id") {
				t.Fatalf("upstream data leaked: %s", recorder.Body)
			}
		})
	}
}
