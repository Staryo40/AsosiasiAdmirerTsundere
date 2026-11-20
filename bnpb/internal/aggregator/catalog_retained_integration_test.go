//go:build integration

package aggregator

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCatalogIntegrationRejectsRetainedSourceReferenceReuse(t *testing.T) {
	store := m2OpenStore(t, m2IsolatedConfig(t))
	seed := p1UniqueRecord(t, p1Fixtures(t)[1])
	eventID := p1RecordID(t, seed, "event_id")
	oldOccurrence := time.Date(2026, 11, 18, 1, 0, 0, 0, time.UTC)
	seed = p1WithField(t, seed, "occurred_at", p1Encode(t, oldOccurrence.Format(time.RFC3339Nano)))
	if err := store.Ingest(t.Context(), IngestChunk{
		Endpoint: SeismicEndpoint, Records: []json.RawMessage{seed}, ReceivedAt: oldOccurrence.Add(time.Minute), InputID: "catalog-seed-" + eventID,
	}); err != nil {
		t.Fatal(err)
	}

	cfg := testConfig(t)
	cfg.Catalog.Token = "catalog-secret"
	mux := http.NewServeMux()
	registerCatalogRoutes(mux, store, cfg, nil)
	auditMux := http.NewServeMux()
	cfg.Audit.Token = "audit-secret"
	registerAuditRoutes4(auditMux, store, cfg, 0)
	auditChecksum := func() auditChecksumResponse {
		t.Helper()
		request := httptest.NewRequest(http.MethodGet, "/internal/audit/checksum", nil)
		request.Header.Set("Authorization", "Bearer audit-secret")
		response := httptest.NewRecorder()
		auditMux.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("audit checksum status=%d", response.Code)
		}
		var checksum auditChecksumResponse
		if err := json.Unmarshal(response.Body.Bytes(), &checksum); err != nil {
			t.Fatal(err)
		}
		return checksum
	}
	beforeChecksum := auditChecksum()
	var notificationsBefore int
	if err := store.read.QueryRow(t.Context(), "SELECT count(*) FROM outbox").Scan(&notificationsBefore); err != nil {
		t.Fatal(err)
	}
	var pollStateBefore []byte
	if err := store.read.QueryRow(t.Context(), "SELECT to_jsonb(poll_state) FROM poll_state WHERE endpoint=$1", string(SeismicEndpoint)).Scan(&pollStateBefore); err != nil {
		t.Fatal(err)
	}

	newOccurrence := oldOccurrence.Add(time.Hour)
	conflicting := p1WithField(t, seed, "occurred_at", p1Encode(t, newOccurrence.Format(time.RFC3339Nano)))
	receipt := catalogReceipt{
		InputID: "catalog-conflict-" + eventID, InputOffset: 4,
		ReceivedAt: newOccurrence.Add(time.Minute), Records: []json.RawMessage{conflicting},
	}
	body, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/internal/catalog/seismic", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer catalog-secret")
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("retained catalogue receipt status=%d body=%s, want %d", response.Code, response.Body, http.StatusConflict)
	}
	if strings.Contains(response.Body.String(), string(conflicting)) {
		t.Fatal("catalogue rejection exposed the failed payload")
	}
	var status string
	if err := store.read.QueryRow(t.Context(), `SELECT status FROM handled_inputs
		WHERE endpoint=$1 AND input_hash=$2`, string(SeismicEndpoint), inputHash(IngestChunk{
		Endpoint: SeismicEndpoint, InputID: receipt.InputID, InputOffset: receipt.InputOffset,
	})).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "retained" {
		t.Fatalf("handled-input status=%q, want retained", status)
	}
	afterChecksum := auditChecksum()
	if afterChecksum.Count != beforeChecksum.Count || afterChecksum.Checksum != beforeChecksum.Checksum {
		t.Fatalf("ID checksum changed after retained catalogue input: before=%+v after=%+v", beforeChecksum, afterChecksum)
	}
	var notificationsAfter int
	if err := store.read.QueryRow(t.Context(), "SELECT count(*) FROM outbox").Scan(&notificationsAfter); err != nil {
		t.Fatal(err)
	}
	if notificationsAfter != notificationsBefore {
		t.Fatalf("retained catalogue input created %d notifications, want none", notificationsAfter-notificationsBefore)
	}
	var pollStateAfter []byte
	if err := store.read.QueryRow(t.Context(), "SELECT to_jsonb(poll_state) FROM poll_state WHERE endpoint=$1", string(SeismicEndpoint)).Scan(&pollStateAfter); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(pollStateAfter, pollStateBefore) {
		t.Fatalf("catalogue ingest changed poll_state from %s to %s", pollStateBefore, pollStateAfter)
	}
}

func TestCatalogIntegrationSameReceiptRetryRemainsIdempotent(t *testing.T) {
	store := m2OpenStore(t, m2IsolatedConfig(t))
	cfg := testConfig(t)
	cfg.Catalog.Token = "catalog-secret"
	mux := http.NewServeMux()
	registerCatalogRoutes(mux, store, cfg, nil)

	record := p1UniqueRecord(t, p1Fixtures(t)[1])
	receipt := catalogReceipt{
		InputID: "catalog-retry", InputOffset: 0, ReceivedAt: time.Date(2026, 11, 18, 2, 0, 0, 0, time.UTC),
		Records: []json.RawMessage{record},
	}
	body, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	for attempt := range 2 {
		request := httptest.NewRequest(http.MethodPost, "/internal/catalog/seismic", bytes.NewReader(body))
		request.Header.Set("Authorization", "Bearer catalog-secret")
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("attempt %d status=%d body=%s, want %d", attempt+1, response.Code, response.Body, http.StatusOK)
		}
	}
	var notifications int
	if err := store.read.QueryRow(t.Context(), "SELECT count(*) FROM outbox").Scan(&notifications); err != nil {
		t.Fatal(err)
	}
	if notifications != 1 {
		t.Fatalf("same-receipt retry created %d notifications, want 1", notifications)
	}
}
