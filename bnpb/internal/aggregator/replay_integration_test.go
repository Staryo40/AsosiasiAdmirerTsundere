//go:build integration

package aggregator

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func replayAll(t *testing.T, ctx context.Context, store *Store) {
	t.Helper()
	round := databaseTime(time.Now())
	for range 100 {
		result, err := store.replayParked(ctx, PVMBG, round, 3*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if !result.Remaining {
			return
		}
	}
	t.Fatal("replay did not finish")
}

func setAdaptation(store *Store, adaptation pvmbgAdaptation) {
	store.pvmbg = adaptation
	for _, shard := range store.shards {
		if shard != nil {
			shard.pvmbg = adaptation
		}
	}
}

type replayFixture struct {
	renamedA, renamedB, retyped, firstVersion, secondVersion json.RawMessage
	sharedID                                                 string
}

func parkReplayFixture(t *testing.T, ctx context.Context, store *Store) replayFixture {
	t.Helper()
	rename := func(r map[string]any) { r["level"] = r["alert_level"]; delete(r, "alert_level") }
	fixture := replayFixture{
		renamedA: volcanicRecordWith(t, rename),
		renamedB: volcanicRecordWith(t, rename),
		retyped:  volcanicRecordWith(t, func(r map[string]any) { r["eruption_count_24h"] = "4" }),
		sharedID: "rpt-versions-" + rand.Text(),
	}
	version := func(level string) json.RawMessage {
		return volcanicRecordWith(t, func(r map[string]any) { rename(r); r["report_id"] = fixture.sharedID; r["level"] = level })
	}
	fixture.firstVersion, fixture.secondVersion = version("Waspada"), version("Awas")
	for _, records := range [][]json.RawMessage{{fixture.renamedA, fixture.renamedB, fixture.retyped}, {fixture.firstVersion}, {fixture.secondVersion}} {
		if err := ingestOnce(t, ctx, store, VolcanicEndpoint, records...); err != nil {
			t.Fatal(err)
		}
	}
	return fixture
}

func (fixture replayFixture) records() []json.RawMessage {
	return []json.RawMessage{fixture.renamedA, fixture.renamedB, fixture.retyped, fixture.firstVersion, fixture.secondVersion}
}

func hazardRevisions(t *testing.T, ctx context.Context, store *Store, reportID string) (count int, severity string, revision int) {
	t.Helper()
	for _, candidate := range append([]*Store{store}, store.shards[:]...) {
		if candidate == nil {
			continue
		}
		var found int
		var foundSeverity *string
		var foundRevision *int
		if err := candidate.read.QueryRow(ctx, `SELECT count(*), max(severity), max(revision) FROM hazard_events
            WHERE source = 'PVMBG' AND source_ref_id = $1`, reportID).Scan(&found, &foundSeverity, &foundRevision); err != nil {
			continue
		}
		count += found
		if foundSeverity != nil {
			severity, revision = *foundSeverity, *foundRevision
		}
	}
	return count, severity, revision
}

func reportID(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var record map[string]any
	if err := json.Unmarshal(raw, &record); err != nil {
		t.Fatal(err)
	}
	return record["report_id"].(string)
}

func assertReplayStory(t *testing.T, ctx context.Context, store *Store) {
	t.Helper()
	fixture := parkReplayFixture(t, ctx, store)
	replayAll(t, ctx, store)
	for _, raw := range fixture.records() {
		if row, _ := readFailedRow(t, ctx, store, VolcanicEndpoint, "record", raw); row.status != "parked" {
			t.Fatalf("without a fix the record must stay parked: %+v", row)
		}
	}

	setAdaptation(store, pvmbgAdaptation{aliases: map[string]string{"level": "alert_level"}, numericStrings: []string{"eruption_count_24h"}})
	replayAll(t, ctx, store)
	for _, raw := range fixture.records() {
		if row, _ := readFailedRow(t, ctx, store, VolcanicEndpoint, "record", raw); row.status != "resolved" {
			t.Fatalf("after the fix every record must recover: %+v", row)
		}
	}
	for _, raw := range []json.RawMessage{fixture.renamedA, fixture.renamedB, fixture.retyped} {
		if count, _, revision := hazardRevisions(t, ctx, store, reportID(t, raw)); count != 1 || revision != 1 {
			t.Fatalf("expected one first-revision hazard, got count=%d revision=%d", count, revision)
		}
	}
	count, severity, revision := hazardRevisions(t, ctx, store, fixture.sharedID)
	if count != 1 || severity != string(Awas) || revision != 2 {
		t.Fatalf("versions of one report must apply in order: count=%d severity=%s revision=%d", count, severity, revision)
	}

	replayAll(t, ctx, store)
	if count, severity, revision := hazardRevisions(t, ctx, store, fixture.sharedID); count != 1 || severity != string(Awas) || revision != 2 {
		t.Fatalf("a second replay must change nothing: count=%d severity=%s revision=%d", count, severity, revision)
	}
	open, _, err := store.listFailedMessages(ctx, failedQuery{source: PVMBG, limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	for _, message := range open {
		for _, raw := range fixture.records() {
			if message.MessageID == failedMessageID(failedMessageKey(VolcanicEndpoint, "record", raw)) {
				t.Fatalf("recovered message %s is still listed", message.MessageID)
			}
		}
	}
}

func TestReplayRecoversParkedRecordsOnceAndInOrder(t *testing.T) {
	assertReplayStory(t, t.Context(), legacyFailedStore(t, pvmbgAdaptation{}))
}

func TestReplayRecoversParkedRecordsOnceAndInOrderWhenRouted(t *testing.T) {
	cfg := routedM2Config(t)
	store := openRoutedM2Store(t, cfg)
	ctx, cancel := context.WithTimeout(activateM2ShardFixtureOwner(t, cfg, store), time.Minute)
	defer cancel()
	assertReplayStory(t, ctx, store)
}

func TestReplayAndMessagesRoutesNeedTheInternalKey(t *testing.T) {
	ctx := t.Context()
	store := legacyFailedStore(t, pvmbgAdaptation{})
	parked := volcanicRecordWith(t, func(r map[string]any) { delete(r, "reported_at") })
	if err := ingestOnce(t, ctx, store, VolcanicEndpoint, parked); err != nil {
		t.Fatal(err)
	}
	cfg := testConfig(t)
	mux := http.NewServeMux()
	registerDLQRoutes(mux, store, cfg, 10*time.Second, nil)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	call := func(method, path, key, body string) (int, string) {
		request, _ := http.NewRequestWithContext(ctx, method, server.URL+path, strings.NewReader(body))
		if key != "" {
			request.Header.Set(internalKeyHeader, key)
		}
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		payload, _ := io.ReadAll(response.Body)
		return response.StatusCode, string(payload)
	}
	if status, _ := call(http.MethodGet, "/internal/dlq/messages", "", ""); status != http.StatusUnauthorized {
		t.Fatalf("messages without a key: HTTP %d", status)
	}
	if status, _ := call(http.MethodPost, "/internal/dlq/replay", "wrong", "{}"); status != http.StatusForbidden {
		t.Fatalf("replay with a wrong key: HTTP %d", status)
	}
	if status, _ := call(http.MethodGet, "/internal/dlq/messages?limit=21", cfg.InternalKey, ""); status != http.StatusBadRequest {
		t.Fatalf("a page above 20 must be refused: HTTP %d", status)
	}
	status, body := call(http.MethodGet, "/internal/dlq/messages?source=PVMBG&limit=20", cfg.InternalKey, "")
	if status != http.StatusOK || !strings.Contains(body, `"payload_base64"`) {
		t.Fatalf("messages: HTTP %d %s", status, body)
	}
	status, body = call(http.MethodPost, "/internal/dlq/replay", cfg.InternalKey, `{"source":"PVMBG"}`)
	var result replayResult
	if status != http.StatusOK || json.Unmarshal([]byte(body), &result) != nil || result.Attempted == 0 {
		t.Fatalf("replay: HTTP %d %s", status, body)
	}
	if status, _ := call(http.MethodPost, "/internal/dlq/replay", cfg.InternalKey, `{"source":"NASA"}`); status != http.StatusBadRequest {
		t.Fatalf("an unknown source must be refused: HTTP %d", status)
	}
}
