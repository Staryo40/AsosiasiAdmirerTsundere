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

	"github.com/jackc/pgx/v5"
)

type failedRow struct {
	status, reason string
	retryable      bool
	attempts       int
	payload        []byte
	sourceRefID    *string
}

func readFailedRow(t *testing.T, ctx context.Context, store *Store, endpoint Endpoint, kind string, payload []byte) (failedRow, bool) {
	t.Helper()
	var row failedRow
	err := store.read.QueryRow(ctx, `SELECT status, reason, retryable, attempts, payload, source_ref_id FROM failed_messages WHERE message_key = $1`,
		failedMessageKey(endpoint, kind, payload)).Scan(&row.status, &row.reason, &row.retryable, &row.attempts, &row.payload, &row.sourceRefID)
	if err == pgx.ErrNoRows {
		return row, false
	}
	if err != nil {
		t.Fatal(err)
	}
	return row, true
}

func volcanicRecordWith(t *testing.T, edit func(map[string]any)) json.RawMessage {
	t.Helper()
	record := map[string]any{"report_id": "rpt-failed-" + rand.Text(), "volcano_id": "merapi", "alert_level": "Siaga",
		"eruption_count_24h": 3, "ash_column_height_m": 1200.5, "reported_at": "2026-11-21T01:02:03Z"}
	edit(record)
	raw, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func ingestOnce(t *testing.T, ctx context.Context, store *Store, endpoint Endpoint, records ...json.RawMessage) error {
	t.Helper()
	return store.Ingest(ctx, IngestChunk{Endpoint: endpoint, Records: records, ReceivedAt: time.Now().UTC(), InputID: "input-" + rand.Text()})
}

func legacyFailedStore(t *testing.T, adaptation pvmbgAdaptation) *Store {
	t.Helper()
	cfg := testConfig(t)
	cfg.DatabaseURL = p1TestDatabaseURL(t)
	cfg.PVMBGAdaptation = adaptation
	store, err := OpenStore(t.Context(), cfg, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	return store
}

func TestFailedMessagesParkRejectedRecordsAndResolveThemOnceMapped(t *testing.T) {
	ctx := t.Context()
	store := legacyFailedStore(t, pvmbgAdaptation{})
	renamed := volcanicRecordWith(t, func(r map[string]any) { r["level"] = r["alert_level"]; delete(r, "alert_level") })
	deep := json.RawMessage(`{"report_id":"rpt-deep-` + rand.Text() + `","x":` + strings.Repeat("[", 40) + strings.Repeat("]", 40) + `}`)
	invalidUTF8 := json.RawMessage(append([]byte(`{"report_id":"fault-`+rand.Text()), 0xC3, 0x28, '"', '}'))
	valid := volcanicRecordWith(t, func(map[string]any) {})

	if err := ingestOnce(t, ctx, store, VolcanicEndpoint, renamed, deep, invalidUTF8, valid); err != nil {
		t.Fatal(err)
	}
	for _, want := range []struct {
		raw    json.RawMessage
		reason string
	}{{renamed, "missing_field: alert_level"}, {deep, "too_deep: record"}, {invalidUTF8, "invalid_json: record"}} {
		row, found := readFailedRow(t, ctx, store, VolcanicEndpoint, "record", want.raw)
		if !found || row.status != "parked" || row.retryable || row.attempts != 1 || row.reason != want.reason || string(row.payload) != string(want.raw) {
			t.Fatalf("%s: found=%v row=%+v", want.reason, found, row)
		}
	}
	if _, found := readFailedRow(t, ctx, store, VolcanicEndpoint, "record", valid); found {
		t.Fatal("a mapped record must not become a failed message")
	}

	// PVMBG re-sends the same bytes under a new poll input.
	if err := ingestOnce(t, ctx, store, VolcanicEndpoint, renamed); err != nil {
		t.Fatal(err)
	}
	if row, _ := readFailedRow(t, ctx, store, VolcanicEndpoint, "record", renamed); row.attempts != 2 || row.status != "parked" {
		t.Fatalf("re-sent record: %+v", row)
	}

	adapted := legacyFailedStore(t, pvmbgAdaptation{aliases: map[string]string{"level": "alert_level"}})
	if err := ingestOnce(t, ctx, adapted, VolcanicEndpoint, renamed); err != nil {
		t.Fatal(err)
	}
	if row, _ := readFailedRow(t, ctx, store, VolcanicEndpoint, "record", renamed); row.status != "resolved" {
		t.Fatalf("mapped record must resolve its row: %+v", row)
	}
}

func TestFailedMessagesTrackUnreadableResponsesUntilAPollSucceeds(t *testing.T) {
	ctx := t.Context()
	store := legacyFailedStore(t, pvmbgAdaptation{})
	body := []byte(`[{"event_id":"truncated-` + rand.Text())
	failure := &sourceError{category: "invalid_json", payload: body, payloadBytes: int64(len(body))}
	for range 2 {
		if err := store.recordUnreadableResponse(ctx, SeismicEndpoint, time.Now(), failure); err != nil {
			t.Fatal(err)
		}
	}
	row, found := readFailedRow(t, ctx, store, SeismicEndpoint, "response", body)
	if !found || row.status != "retrying" || !row.retryable || row.attempts != 2 || row.reason != "invalid_json: body" || row.sourceRefID != nil {
		t.Fatalf("found=%v row=%+v", found, row)
	}
	if err := store.resolveUnreadableResponses(ctx, SeismicEndpoint); err != nil {
		t.Fatal(err)
	}
	if row, _ := readFailedRow(t, ctx, store, SeismicEndpoint, "response", body); row.status != "resolved" {
		t.Fatalf("a successful poll must resolve the row: %+v", row)
	}
}

func TestUnprocessedPagesSatisfyTheAuditProxy(t *testing.T) {
	ctx := t.Context()
	store := legacyFailedStore(t, pvmbgAdaptation{})
	var parked []json.RawMessage
	for range 3 {
		parked = append(parked, volcanicRecordWith(t, func(r map[string]any) { r["eruption_count_24h"] = "3" }))
	}
	if err := ingestOnce(t, ctx, store, VolcanicEndpoint, parked...); err != nil {
		t.Fatal(err)
	}
	cfg := testConfig(t)
	cfg.DBReadTimeout = 2 * time.Second
	cfg.Audit.UnprocessedToken = "metadata-token-" + rand.Text()
	mux := http.NewServeMux()
	registerDLQRoutes(mux, store, cfg, 10*time.Second)
	owner := httptest.NewServer(mux)
	t.Cleanup(owner.Close)

	wanted := map[string]bool{}
	for _, raw := range parked {
		wanted[failedMessageID(failedMessageKey(VolcanicEndpoint, "record", raw))] = true
	}
	seen := map[string]int{}
	cursor := ""
	for pages := 0; ; pages++ {
		if pages > 10000 {
			t.Fatal("pagination did not terminate")
		}
		address := owner.URL + "/internal/dlq/unprocessed?source=PVMBG&limit=2"
		if cursor != "" {
			address += "&cursor=" + cursor
		}
		request, _ := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
		request.Header.Set("Authorization", "Bearer "+cfg.Audit.UnprocessedToken)
		response, err := owner.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		page, ok := decodeUnprocessedEnvelope(body, 2, "PVMBG")
		if response.StatusCode != http.StatusOK || !ok {
			t.Fatalf("HTTP %d, proxy-valid=%v: %s", response.StatusCode, ok, body)
		}
		for _, item := range page.Items {
			seen[item.MessageID]++
			if wanted[item.MessageID] && (item.Status != "parked" || item.Reason != "wrong_type: eruption_count_24h" || item.Attempts != 1) {
				t.Fatalf("unexpected item %+v", item)
			}
		}
		if page.NextCursor == nil {
			break
		}
		cursor = *page.NextCursor
	}
	for id := range wanted {
		if seen[id] != 1 {
			t.Fatalf("message %s listed %d times", id, seen[id])
		}
	}
}

func TestFailedMessagesRoutedShardFailureRetriesThenResolves(t *testing.T) {
	cfg := routedM2Config(t)
	store := openRoutedM2Store(t, cfg)
	ownerCtx := activateM2ShardFixtureOwner(t, cfg, store)
	ctx, cancel := context.WithTimeout(ownerCtx, 30*time.Second)
	defer cancel()
	identities := routedShardIdentities(t)
	stamp := time.Date(2026, 11, 21, 2, 0, 0, 0, time.UTC)
	records := []json.RawMessage{routedRecord(t, identities[0], stamp), routedRecord(t, identities[1], stamp.Add(time.Minute))}
	admin, err := pgx.Connect(ctx, cfg.ShardURLs[1])
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(context.Background())
	if _, err := admin.Exec(ctx, `CREATE FUNCTION fail_handled_input() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected isolated shard failure'; END $$; CREATE TRIGGER fail_handled_input BEFORE INSERT ON handled_inputs FOR EACH ROW EXECUTE FUNCTION fail_handled_input()`); err != nil {
		t.Fatal(err)
	}
	chunk := IngestChunk{Endpoint: SeismicEndpoint, Records: records, ReceivedAt: stamp.Add(3 * time.Minute), InputID: "failed-routed-" + rand.Text()}
	if err := store.Ingest(ctx, chunk); err == nil {
		t.Fatal("ingest succeeded despite the failing shard")
	}
	row, found := readFailedRow(t, ctx, store, SeismicEndpoint, "record", records[1])
	if !found || row.status != "retrying" || !row.retryable || row.reason != "shard_unavailable: shard-1" {
		t.Fatalf("found=%v row=%+v", found, row)
	}
	if _, found := readFailedRow(t, ctx, store, SeismicEndpoint, "record", records[0]); found {
		t.Fatal("the record on the healthy shard must not be listed")
	}

	store.Close()
	for _, address := range append([]string{cfg.DatabaseURL}, cfg.ShardURLs[:]...) {
		closeFixtureMutationGuard(t, address)
	}
	store = openRoutedM2Store(t, cfg)
	if _, err := admin.Exec(ctx, "DROP TRIGGER fail_handled_input ON handled_inputs; DROP FUNCTION fail_handled_input()"); err != nil {
		t.Fatal(err)
	}
	activateM2ShardFixtureOwner(t, cfg, store)
	ctx = fixtureMutationContext(t, ctx, fixtureMutationOwner, fixtureMutationEpoch, allFixtureStores(store)...)
	if err := store.Ingest(ctx, chunk); err != nil {
		t.Fatalf("replay same receipt: %v", err)
	}
	if row, _ := readFailedRow(t, ctx, store, SeismicEndpoint, "record", records[1]); row.status != "resolved" {
		t.Fatalf("the replayed receipt must resolve the row: %+v", row)
	}
}

func TestFailedMessagesRoutedRejectionsLandInTheControlStore(t *testing.T) {
	cfg := routedM2Config(t)
	store := openRoutedM2Store(t, cfg)
	ownerCtx := activateM2ShardFixtureOwner(t, cfg, store)
	ctx, cancel := context.WithTimeout(ownerCtx, 30*time.Second)
	defer cancel()
	renamed := volcanicRecordWith(t, func(r map[string]any) { r["level"] = r["alert_level"]; delete(r, "alert_level") })
	unroutable := json.RawMessage(append([]byte(`{"report_id":"fault-`+rand.Text()), 0xC3, 0x28, '"', '}'))
	if err := ingestOnce(t, ctx, store, VolcanicEndpoint, renamed, unroutable); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []json.RawMessage{renamed, unroutable} {
		row, found := readFailedRow(t, ctx, store, VolcanicEndpoint, "record", raw)
		if !found || row.status != "parked" || string(row.payload) != string(raw) {
			t.Fatalf("found=%v row=%+v", found, row)
		}
	}
}
