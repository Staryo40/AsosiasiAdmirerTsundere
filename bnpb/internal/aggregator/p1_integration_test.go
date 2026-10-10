//go:build integration

package aggregator

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"
)

func p1TestStore(t *testing.T) (*Store, Config) {
	t.Helper()
	cfg := testConfig(t)
	cfg.DatabaseURL = p1TestDatabaseURL(t)
	store, err := OpenStore(t.Context(), cfg, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	return store, cfg
}

func p1TestDatabaseURL(t *testing.T) string {
	t.Helper()
	address := os.Getenv("BNPB_TEST_DATABASE_URL")
	parsed, err := url.Parse(address)
	if err != nil || parsed.Hostname() != "postgres-test" || parsed.Path != "/bnpb_test" {
		t.Fatal("BNPB_TEST_DATABASE_URL must target postgres-test/bnpb_test in the isolated Compose tests profile")
	}
	return address
}

func p1Ingest(t *testing.T, store *Store, endpoint Endpoint, records ...json.RawMessage) {
	t.Helper()
	if err := store.Ingest(t.Context(), IngestChunk{Endpoint: endpoint, Records: records, ReceivedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
}

func p1ReadServer(t *testing.T, store *Store, cfg Config) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	registerSnapshotRoutes(mux, store, cfg)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

func p1ReadPage(t *testing.T, server *httptest.Server, cfg Config, source Source) []HazardEvent {
	t.Helper()
	request, err := http.NewRequest("GET", server.URL+"/internal/hazards?source="+string(source)+"&limit=500", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set(internalKeyHeader, cfg.InternalKey)
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatalf("snapshot HTTP %d", response.StatusCode)
	}
	var page hazardList
	if err := json.NewDecoder(response.Body).Decode(&page); err != nil {
		t.Fatal(err)
	}
	return page.Items
}

func p1Stored(t *testing.T, page []HazardEvent, sourceRef string) HazardEvent {
	t.Helper()
	var matches []HazardEvent
	for _, hazard := range page {
		if hazard.SourceRefID == sourceRef {
			matches = append(matches, hazard)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("expected one stored hazard for %s, got %d", sourceRef, len(matches))
	}
	return matches[0]
}

func p1UniqueRecord(t *testing.T, fixture p1MappingFixture) json.RawMessage {
	t.Helper()
	fields := ingestEndpoints[SeismicEndpoint]
	if fixture.Source == PVMBG {
		fields = ingestEndpoints[VolcanicEndpoint]
	}
	raw := p1WithField(t, fixture.Input, fields.identity, p1Encode(t, newReceipt(time.Now()).HazardID))
	return p1WithField(t, raw, fields.timestamp, p1Encode(t, time.Now().UTC().Format(time.RFC3339Nano)))
}

func p1RecordID(t *testing.T, raw json.RawMessage, field string) string {
	t.Helper()
	fields, err := parseObject(raw)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := textField(fields, field)
	if err != nil {
		t.Fatal(err)
	}
	return identity
}

func TestP1IntegrationSchemaCoexistence(t *testing.T) {
	store, cfg := p1TestStore(t)
	server := p1ReadServer(t, store, cfg)
	oldRaw := p1UniqueRecord(t, p1Fixtures(t)[2])
	p1Ingest(t, store, VolcanicEndpoint, oldRaw)
	oldID := p1RecordID(t, oldRaw, "report_id")
	oldHazard := p1Stored(t, p1ReadPage(t, server, cfg, PVMBG), oldID)
	newRaw := p1UniqueRecord(t, p1Fixtures(t)[2])
	newRaw = p1WithField(t, newRaw, "confidence_level", json.RawMessage("0"))
	p1Ingest(t, store, VolcanicEndpoint, newRaw)
	page := p1ReadPage(t, server, cfg, PVMBG)
	newHazard := p1Stored(t, page, p1RecordID(t, newRaw, "report_id"))
	p1EqualJSON(t, p1Encode(t, p1Stored(t, page, oldID)), p1Encode(t, oldHazard))
	if _, present := oldHazard.Attributes["confidence_level"]; present {
		t.Fatal("V1 gained confidence")
	}
	p1EqualJSON(t, newHazard.Attributes["confidence_level"], []byte("0"))
	p1EqualJSON(t, newHazard.Attributes["extra"], []byte(`{"readings":[9007199254740993,null]}`))
	if err := store.Ready(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func p1Notifications(t *testing.T, store *Store, hazardID string) []outboxMessage {
	t.Helper()
	messages, err := store.pendingOutbox(t.Context(), 10000)
	if err != nil {
		t.Fatal(err)
	}
	var matched []outboxMessage
	for _, message := range messages {
		if message.HazardID == hazardID {
			matched = append(matched, message)
		}
	}
	return matched
}

func TestP1IntegrationLateWarningsAndDeduplication(t *testing.T) {
	store, cfg := p1TestStore(t)
	server := p1ReadServer(t, store, cfg)
	raw := p1UniqueRecord(t, p1Fixtures(t)[1])
	id := p1RecordID(t, raw, "event_id")
	p1Ingest(t, store, SeismicEndpoint, raw)
	before := p1Stored(t, p1ReadPage(t, server, cfg, BMKG), id)
	if before.Severity != Siaga {
		t.Fatal("magnitude fallback missing")
	}
	p1Ingest(t, store, SeismicEndpoint, raw, raw)
	if len(p1Notifications(t, store, before.HazardID)) != 1 {
		t.Fatal("duplicate polls produced revisions")
	}
	warning := p1WithField(t, p1Fixtures(t)[1].Warnings[0], "related_event_id", p1Encode(t, id))
	warning = p1WithField(t, warning, "warning_id", p1Encode(t, id+"-warning"))
	p1Ingest(t, store, WarningEndpoint, warning)
	after := p1Stored(t, p1ReadPage(t, server, cfg, BMKG), id)
	if after.Severity != Waspada || after.HazardID != before.HazardID || !after.IngestedAt.Equal(before.IngestedAt) {
		t.Fatal("late warning must update the same hazard and replace magnitude severity")
	}
	p1Ingest(t, store, WarningEndpoint, warning)
	revisions := p1Notifications(t, store, before.HazardID)
	if len(revisions) != 2 || revisions[0].Revision != 1 || revisions[1].Revision != 2 || revisions[0].MessageID == revisions[1].MessageID {
		t.Fatal("revisions need distinct message IDs and stable hazard identity")
	}
}

func TestP1IntegrationInvalidRecordsAndCheckpoint(t *testing.T) {
	store, cfg := p1TestStore(t)
	server := p1ReadServer(t, store, cfg)
	raw := p1UniqueRecord(t, p1Fixtures(t)[2])
	missingTime := p1WithField(t, raw, "reported_at", nil)
	before, err := store.pollPosition(t.Context(), VolcanicEndpoint)
	if err != nil {
		t.Fatal(err)
	}
	p1Ingest(t, store, VolcanicEndpoint, missingTime)
	after, err := store.pollPosition(t.Context(), VolcanicEndpoint)
	if err != nil {
		t.Fatal(err)
	}
	if !p1SameCheckpoint(before.checkpoint, after.checkpoint) {
		t.Fatal("record without a usable timestamp advanced checkpoint")
	}
	invalid := p1WithField(t, raw, "volcano_id", p1Encode(t, "unknown"))
	p1Ingest(t, store, VolcanicEndpoint, invalid, raw)
	hazard := p1Stored(t, p1ReadPage(t, server, cfg, PVMBG), p1RecordID(t, raw, "report_id"))
	if len(p1Notifications(t, store, hazard.HazardID)) != 1 {
		t.Fatal("invalid record produced a canonical revision")
	}
	var rejected int
	err = store.read.QueryRow(t.Context(), `SELECT count(*) FROM ingest_rejects
        WHERE endpoint = 'volcanic' AND source_ref_id = $1 AND category IN ('missing_field', 'unknown_volcano')`, hazard.SourceRefID).Scan(&rejected)
	if err != nil {
		t.Fatal(err)
	}
	if rejected != 2 {
		t.Fatalf("expected both invalid records quarantined, got %d", rejected)
	}
}

func p1SameCheckpoint(before, after *time.Time) bool {
	if before == nil || after == nil {
		return before == nil && after == nil
	}
	return before.Equal(*after)
}

func TestP1IntegrationSourceIdentityAndOrphanWarning(t *testing.T) {
	store, cfg := p1TestStore(t)
	server := p1ReadServer(t, store, cfg)
	seismic := p1UniqueRecord(t, p1Fixtures(t)[1])
	id := p1RecordID(t, seismic, "event_id")
	warning := p1WithField(t, p1Fixtures(t)[1].Warnings[0], "related_event_id", p1Encode(t, id))
	warning = p1WithField(t, warning, "warning_id", p1Encode(t, id+"-orphan"))
	p1Ingest(t, store, WarningEndpoint, warning)
	p1Ingest(t, store, SeismicEndpoint, seismic)
	seismicHazard := p1Stored(t, p1ReadPage(t, server, cfg, BMKG), id)
	if seismicHazard.Severity != Waspada {
		t.Fatal("warning received before event was lost")
	}
	volcanic := p1WithField(t, p1UniqueRecord(t, p1Fixtures(t)[2]), "report_id", p1Encode(t, id))
	p1Ingest(t, store, VolcanicEndpoint, volcanic)
	volcanicHazard := p1Stored(t, p1ReadPage(t, server, cfg, PVMBG), id)
	if volcanicHazard.HazardID == seismicHazard.HazardID {
		t.Fatal("source identity must include agency")
	}
	reused := p1WithField(t, seismic, "occurred_at", p1Encode(t, time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)))
	p1Ingest(t, store, SeismicEndpoint, reused)
	unchanged := p1Stored(t, p1ReadPage(t, server, cfg, BMKG), id)
	p1EqualJSON(t, p1Encode(t, unchanged), p1Encode(t, seismicHazard))
	var rejected int
	if err := store.read.QueryRow(t.Context(), `SELECT count(*) FROM ingest_rejects WHERE source_ref_id=$1 AND category='source_identity_reused'`, id).Scan(&rejected); err != nil {
		t.Fatal(err)
	}
	if rejected != 1 {
		t.Fatal("reused source identity was not quarantined")
	}
}

func TestP1IntegrationNetworkPolling(t *testing.T) {
	store, cfg := p1TestStore(t)
	raw := p1UniqueRecord(t, p1Fixtures(t)[2])
	payload := p1Encode(t, []json.RawMessage{raw})
	agency := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/auth/login" {
			_, _ = io.WriteString(w, `{"access_token":"fixture-reader","token_type":"Bearer","expires_in":300}`)
			return
		}
		if request.URL.Path != "/volcanic-reports" || request.Header.Get("Authorization") != "Bearer fixture-reader" {
			w.WriteHeader(403)
			return
		}
		_, _ = w.Write(payload)
	}))
	defer agency.Close()
	cfg.PVMBG.BaseURL = agency.URL
	clients := newSourceClients(cfg, quietLogger())
	defer clients.closeIdleConnections()
	poller := &poller{store: store, sources: clients, cfg: cfg, logger: quietLogger(),
		reader: readerSession{client: clients.pvmbg, clientID: cfg.PVMBGReaderClientID, clientSecret: cfg.PVMBGReaderClientSecret}}
	if err := poller.pollEndpoint(t.Context(), VolcanicEndpoint); err != nil {
		t.Fatal(err)
	}
	server := p1ReadServer(t, store, cfg)
	p1Stored(t, p1ReadPage(t, server, cfg, PVMBG), p1RecordID(t, raw, "report_id"))
}
