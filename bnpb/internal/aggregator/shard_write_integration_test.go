//go:build integration

package aggregator

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func TestShardWriterIntegrationReceiptReplayAndNewReceipt(t *testing.T) {
	cfg := m2IsolatedConfig(t)
	store := m2OpenStore(t, cfg)
	server := p1ReadServer(t, store, cfg)
	initial := p1UniqueRecord(t, p1Fixtures(t)[1])
	identity := p1RecordID(t, initial, "event_id")
	ingestReceipt(t, store, SeismicEndpoint, "response-a", 0, initial)
	before := p1Stored(t, p1ReadPage(t, server, cfg, BMKG), identity)
	ingestReceipt(t, store, SeismicEndpoint, "response-a", 0, initial)
	if count := len(p1Notifications(t, store, before.HazardID)); count != 1 {
		t.Fatalf("same-receipt retry produced %d notifications, want 1", count)
	}
	mismatched := p1WithField(t, initial, "magnitude", []byte("7.2"))
	if err := store.Ingest(t.Context(), IngestChunk{
		Endpoint: SeismicEndpoint, Records: []json.RawMessage{mismatched}, ReceivedAt: time.Now().UTC(),
		InputID: "response-a", InputOffset: 0,
	}); err == nil {
		t.Fatal("same receipt and ordinal with different bytes was accepted")
	}
	if afterMismatch := p1Stored(t, p1ReadPage(t, server, cfg, BMKG), identity); afterMismatch.Severity != before.Severity {
		t.Fatal("mismatched receipt changed canonical state")
	}

	updated := p1WithField(t, initial, "magnitude", []byte("7.2"))
	ingestReceipt(t, store, SeismicEndpoint, "response-b", 0, updated)
	current := p1Stored(t, p1ReadPage(t, server, cfg, BMKG), identity)
	ingestReceipt(t, store, SeismicEndpoint, "response-a", 0, initial)
	replayed := p1Stored(t, p1ReadPage(t, server, cfg, BMKG), identity)
	if replayed.Severity != current.Severity || replayed.HazardID != current.HazardID {
		t.Fatal("same-receipt old input replay rolled back canonical state")
	}
	if count := len(p1Notifications(t, store, before.HazardID)); count != 2 {
		t.Fatalf("same-receipt old replay produced %d notifications, want 2", count)
	}

	ingestReceipt(t, store, SeismicEndpoint, "response-c", 0, initial)
	restored := p1Stored(t, p1ReadPage(t, server, cfg, BMKG), identity)
	if restored.Severity != before.Severity || restored.HazardID != before.HazardID {
		t.Fatal("new receipt carrying old bytes did not restore the earlier canonical value")
	}
}

func TestShardWriterIntegrationRetainedInputCanBeAppliedAfterReferenceUpdate(t *testing.T) {
	store := m2OpenStore(t, m2IsolatedConfig(t))
	raw := p1UniqueRecord(t, p1Fixtures(t)[2])
	reportID := p1RecordID(t, raw, "report_id")
	const volcanoID = "integration-retained-volcano"
	raw = p1WithField(t, raw, "volcano_id", p1Encode(t, volcanoID))
	chunk := IngestChunk{Endpoint: VolcanicEndpoint, Records: []json.RawMessage{raw}, ReceivedAt: time.Now().UTC(), InputID: "retained-reference", InputOffset: 0}
	if err := store.Ingest(t.Context(), chunk); err != nil {
		t.Fatal(err)
	}
	var status string
	var retainedRaw []byte
	var firstSeen time.Time
	if err := store.read.QueryRow(t.Context(), `SELECT status, raw_payload, received_at FROM handled_inputs
		WHERE endpoint=$1 AND input_hash=$2`, string(VolcanicEndpoint), inputHash(chunk)).Scan(&status, &retainedRaw, &firstSeen); err != nil {
		t.Fatal(err)
	}
	if status != "retained" || string(retainedRaw) != string(raw) {
		t.Fatalf("initial outcome status=%q raw preserved=%v", status, string(retainedRaw) == string(raw))
	}
	var rejectCount int
	if err := store.read.QueryRow(t.Context(), `SELECT count(*) FROM ingest_rejects
		WHERE endpoint=$1 AND payload_sha256=$2`, string(VolcanicEndpoint), fmt.Sprintf("%x", sha256.Sum256(raw))).Scan(&rejectCount); err != nil {
		t.Fatal(err)
	}
	if rejectCount != 1 {
		t.Fatalf("initial reject count=%d, want 1", rejectCount)
	}

	volcanoReference[volcanoID] = volcanoLocation{Name: "Isolated Test Volcano", Latitude: 1, Longitude: 2}
	defer delete(volcanoReference, volcanoID)
	if _, err := store.write.Exec(t.Context(), `INSERT INTO volcano_reference (volcano_id,name,latitude,longitude,provenance)
		VALUES ($1,'Isolated Test Volcano',1,2,'{}'::jsonb)`, volcanoID); err != nil {
		t.Fatal(err)
	}
	if err := store.Ingest(t.Context(), chunk); err != nil {
		t.Fatal(err)
	}
	var appliedFirst time.Time
	if err := store.read.QueryRow(t.Context(), `SELECT status, raw_payload, received_at FROM handled_inputs
		WHERE endpoint=$1 AND input_hash=$2`, string(VolcanicEndpoint), inputHash(chunk)).Scan(&status, &retainedRaw, &appliedFirst); err != nil {
		t.Fatal(err)
	}
	if status != "applied" || string(retainedRaw) != string(raw) || !appliedFirst.Equal(firstSeen) {
		t.Fatalf("replayed outcome status=%q raw preserved=%v first timestamp preserved=%v", status, string(retainedRaw) == string(raw), appliedFirst.Equal(firstSeen))
	}
	var hazardID string
	if err := store.read.QueryRow(t.Context(), "SELECT hazard_id FROM hazard_events WHERE source_ref_id=$1", reportID).Scan(&hazardID); err != nil {
		t.Fatal(err)
	}
	var notifications int
	if err := store.read.QueryRow(t.Context(), "SELECT count(*) FROM outbox WHERE hazard_id=$1", hazardID).Scan(&notifications); err != nil {
		t.Fatal(err)
	}
	if notifications != 1 {
		t.Fatalf("successful retry notifications=%d, want 1", notifications)
	}
	if err := store.Ingest(t.Context(), chunk); err != nil {
		t.Fatal(err)
	}
	var afterRetry, afterRejects int
	if err := store.read.QueryRow(t.Context(), "SELECT count(*) FROM outbox WHERE hazard_id=$1", hazardID).Scan(&afterRetry); err != nil {
		t.Fatal(err)
	}
	if err := store.read.QueryRow(t.Context(), `SELECT count(*) FROM ingest_rejects
		WHERE endpoint=$1 AND payload_sha256=$2`, string(VolcanicEndpoint), fmt.Sprintf("%x", sha256.Sum256(raw))).Scan(&afterRejects); err != nil {
		t.Fatal(err)
	}
	if afterRetry != 1 || afterRejects != rejectCount {
		t.Fatalf("duplicate replay notifications=%d rejects=%d; want 1 and %d", afterRetry, afterRejects, rejectCount)
	}

	invalid := p1WithField(t, raw, "report_id", p1Encode(t, reportID+"-still-invalid"))
	invalid = p1WithField(t, invalid, "volcano_id", p1Encode(t, "still-unknown"))
	invalidChunk := chunk
	invalidChunk.InputID = "still-invalid-reference"
	invalidChunk.Records = []json.RawMessage{invalid}
	if err := store.Ingest(t.Context(), invalidChunk); err != nil {
		t.Fatal(err)
	}
	for name, statement := range map[string]string{
		"payload":   "UPDATE handled_inputs SET raw_payload='changed' WHERE endpoint='volcanic' AND input_hash=$1",
		"timestamp": "UPDATE handled_inputs SET received_at=received_at + interval '1 second' WHERE endpoint='volcanic' AND input_hash=$1",
		"status":    "UPDATE handled_inputs SET status='retained', reason='edited' WHERE endpoint='volcanic' AND input_hash=$1",
		"delete":    "DELETE FROM handled_inputs WHERE endpoint='volcanic' AND input_hash=$1",
	} {
		t.Run("immutable_"+name, func(t *testing.T) {
			if _, err := store.write.Exec(t.Context(), statement, inputHash(chunk)); err == nil {
				t.Fatalf("handled input %s mutation succeeded", name)
			}
		})
	}
	if _, err := store.write.Exec(t.Context(), "TRUNCATE handled_inputs"); err == nil {
		t.Fatal("handled input truncate succeeded")
	}
	var beforeInvalidRejects int64
	if err := store.read.QueryRow(t.Context(), `SELECT occurrences FROM ingest_rejects
		WHERE endpoint=$1 AND payload_sha256=$2`, string(VolcanicEndpoint), fmt.Sprintf("%x", sha256.Sum256(invalid))).Scan(&beforeInvalidRejects); err != nil {
		t.Fatal(err)
	}
	var invalidRaw []byte
	var invalidFirst time.Time
	if err := store.read.QueryRow(t.Context(), `SELECT raw_payload,received_at FROM handled_inputs
		WHERE endpoint=$1 AND input_hash=$2`, string(VolcanicEndpoint), inputHash(invalidChunk)).Scan(&invalidRaw, &invalidFirst); err != nil {
		t.Fatal(err)
	}
	if err := store.Ingest(t.Context(), invalidChunk); err != nil {
		t.Fatal(err)
	}
	var replayRaw []byte
	var replayFirst time.Time
	if err := store.read.QueryRow(t.Context(), `SELECT raw_payload,received_at FROM handled_inputs
		WHERE endpoint=$1 AND input_hash=$2`, string(VolcanicEndpoint), inputHash(invalidChunk)).Scan(&replayRaw, &replayFirst); err != nil {
		t.Fatal(err)
	}
	var afterInvalidRejects int64
	if err := store.read.QueryRow(t.Context(), `SELECT occurrences FROM ingest_rejects
		WHERE endpoint=$1 AND payload_sha256=$2`, string(VolcanicEndpoint), fmt.Sprintf("%x", sha256.Sum256(invalid))).Scan(&afterInvalidRejects); err != nil {
		t.Fatal(err)
	}
	if string(replayRaw) != string(invalid) || !replayFirst.Equal(invalidFirst) || afterInvalidRejects != beforeInvalidRejects {
		t.Fatal("still-invalid retry changed retained raw, first timestamp, or reject count")
	}
}

func ingestReceipt(t *testing.T, store *Store, endpoint Endpoint, inputID string, offset int, raw json.RawMessage) {
	t.Helper()
	if err := store.Ingest(t.Context(), IngestChunk{
		Endpoint: endpoint, Records: []json.RawMessage{raw}, ReceivedAt: time.Now().UTC(),
		InputID: inputID, InputOffset: offset,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestShardWriterIntegrationWarningSharesEventIdentity(t *testing.T) {
	cfg := m2IsolatedConfig(t)
	store := m2OpenStore(t, cfg)
	server := p1ReadServer(t, store, cfg)
	seismic := p1UniqueRecord(t, p1Fixtures(t)[1])
	eventID := p1RecordID(t, seismic, "event_id")
	ingestReceipt(t, store, SeismicEndpoint, "seismic-response", 0, seismic)
	warning := p1WithField(t, p1Fixtures(t)[1].Warnings[0], "related_event_id", p1Encode(t, eventID))
	warning = p1WithField(t, warning, "warning_id", p1Encode(t, eventID+"-warning"))
	ingestReceipt(t, store, WarningEndpoint, "warning-response", 0, warning)
	hazard := p1Stored(t, p1ReadPage(t, server, cfg, BMKG), eventID)
	if hazard.Severity != Waspada {
		t.Fatal("warning did not update its related seismic event")
	}
}
