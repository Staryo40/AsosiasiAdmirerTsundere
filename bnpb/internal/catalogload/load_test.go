package catalogload

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const completeMeasurements = `{"partial":false,"missing_settings":[],"unavailable_units":[],"items":[{"unit":"db","node":"n1","role":"primary","settings_complete":true,"tables":[{"name":"outbox","row_count":0,"relation_bytes":0,"stats_inserted":0,"stats_updated":0},{"name":"hazard_events","row_count":0,"relation_bytes":0,"stats_inserted":0,"stats_updated":0},{"name":"handled_inputs","row_count":0,"relation_bytes":0,"stats_inserted":0,"stats_updated":0},{"name":"source_warnings","row_count":0,"relation_bytes":0,"stats_inserted":0,"stats_updated":0},{"name":"source_seismic","row_count":0,"relation_bytes":0,"stats_inserted":0,"stats_updated":0}]}]}`

const fixtureAsOf = "2026-11-20T01:02:03.123456789Z"

func fixtureDigest(input string) string {
	sum := sha256.Sum256([]byte(input))
	return hex.EncodeToString(sum[:])
}

func fixtureAudit(w http.ResponseWriter, count int64, checksum string) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"source": "BMKG", "count": count, "checksum": checksum, "algorithm": "sha256",
		"as_of": fixtureAsOf, "partial": false, "unavailable_units": []string{},
	})
}

func TestIsPartialRequiresCompleteOwnedTableMeasurements(t *testing.T) {
	t.Parallel()
	if isPartial(json.RawMessage(completeMeasurements)) {
		t.Fatal("complete five-table measurements were rejected")
	}
	withFutureTable := strings.Replace(completeMeasurements, `]}]}`, `,{"name":"future_table","row_count":0,"relation_bytes":0,"stats_inserted":0,"stats_updated":0}]}]}`, 1)
	if isPartial(json.RawMessage(withFutureTable)) {
		t.Fatal("additive table measurement was rejected")
	}
	for _, invalid := range []string{
		strings.Replace(completeMeasurements, `,{"name":"source_seismic","row_count":0,"relation_bytes":0,"stats_inserted":0,"stats_updated":0}`, "", 1),
		strings.Replace(completeMeasurements, `{"name":"outbox","row_count":0,"relation_bytes":0,"stats_inserted":0,"stats_updated":0},`, `{"name":"outbox","row_count":0,"relation_bytes":0,"stats_inserted":0,"stats_updated":0},{"name":"outbox","row_count":0,"relation_bytes":0,"stats_inserted":0,"stats_updated":0},`, 1),
		strings.Replace(completeMeasurements, `"row_count":0`, `"row_count":-1`, 1),
		strings.Replace(completeMeasurements, `"row_count":0,`, "", 1),
	} {
		if !isPartial(json.RawMessage(invalid)) {
			t.Fatalf("incomplete or invalid measurements were accepted: %s", invalid)
		}
	}
}

func TestRunRetriesIdenticalAPIReceipt(t *testing.T) {
	t.Parallel()
	expected := fixtureDigest("event-1")
	var postBodies []string
	var postAuth, auditAuth, catalogAuth string
	var acknowledged atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/internal/audit/checksum":
			auditAuth = r.Header.Get("Authorization")
			if acknowledged.Load() {
				fixtureAudit(w, 1, expected)
			} else {
				fixtureAudit(w, 0, fixtureDigest(""))
			}
		case r.URL.Path == "/internal/storage/measurements":
			catalogAuth = r.Header.Get("Authorization")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(completeMeasurements))
		case r.URL.Path == "/internal/catalog/seismic":
			postAuth = r.Header.Get("Authorization")
			body, _ := io.ReadAll(r.Body)
			postBodies = append(postBodies, string(body))
			if len(postBodies) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			var batch batchRequest
			_ = json.Unmarshal(body, &batch)
			acknowledged.Store(true)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"accepted": len(batch.Records), "input_id": batch.InputID, "input_offset": batch.InputOffset})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	dir := t.TempDir()
	artifact := filepath.Join(dir, "seismic.jsonl")
	artifactBytes := []byte("{\"event_id\":\"event-1\",\"occurred_at\":\"2025-01-01T00:00:00Z\"}\n")
	if err := os.WriteFile(artifact, artifactBytes, 0600); err != nil {
		t.Fatal(err)
	}
	catalogFile, auditFile := filepath.Join(dir, "catalog.token"), filepath.Join(dir, "audit.token")
	if err := os.WriteFile(catalogFile, []byte("catalog-secret\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(auditFile, []byte("audit-secret\n"), 0600); err != nil {
		t.Fatal(err)
	}
	manifest := filepath.Join(dir, "resources.json")
	if err := os.WriteFile(manifest, []byte(`{"reference_checksum":"`+expected+`","resources":{"cpu":"observed-fixture","memory":"observed-fixture"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(dir, "report.json")
	report, err := Run(context.Background(), Options{Artifact: artifact, BaseURL: server.URL, TokenFile: catalogFile, AuditTokenFile: auditFile, ResourcesManifest: manifest, RunOutput: output})
	if err != nil {
		t.Fatal(err)
	}
	if len(postBodies) != 2 || postBodies[0] != postBodies[1] {
		t.Fatalf("retry did not preserve exact request body: %q", postBodies)
	}
	var first batchRequest
	if err := json.Unmarshal([]byte(postBodies[0]), &first); err != nil {
		t.Fatal(err)
	}
	wantHash := sha256.Sum256(artifactBytes)
	if first.InputID != hex.EncodeToString(wantHash[:]) || first.InputOffset != 0 {
		t.Fatalf("unstable receipt identity: %+v", first)
	}
	if postAuth != "Bearer catalog-secret" || catalogAuth != "Bearer catalog-secret" || auditAuth != "Bearer audit-secret" {
		t.Fatalf("credentials crossed API boundaries: POST=%q catalog=%q audit=%q", postAuth, catalogAuth, auditAuth)
	}
	if report.Rows != 1 || report.ArtifactSHA256 != hex.EncodeToString(wantHash[:]) || !report.DatasetAvailable ||
		report.ExpectedPopulation.Count != 1 || report.ExpectedPopulation.Checksum != expected || report.AuditAfter.Checksum != expected {
		t.Fatalf("unexpected report: %+v", report)
	}
	if report.SubmissionStartedAt.IsZero() || report.SubmissionStartedAt.Location() != time.UTC ||
		report.SubmissionFinishedAt.Location() != time.UTC || first.ReceivedAt.Before(report.SubmissionStartedAt) ||
		first.ReceivedAt.After(report.SubmissionFinishedAt) {
		t.Fatalf("submission window does not contain receipt timestamp %s: %s to %s", first.ReceivedAt, report.SubmissionStartedAt, report.SubmissionFinishedAt)
	}
	if _, err := os.Stat(output); err != nil {
		t.Fatal(err)
	}
}

func TestRunUsesOriginalSnapshotWhenSourceChangesDuringPreflight(t *testing.T) {
	snapshotDir := t.TempDir()
	t.Setenv("TMPDIR", snapshotDir)
	t.Setenv("TMP", snapshotDir)
	expected := fixtureDigest("original")
	var acknowledged atomic.Bool
	dir := t.TempDir()
	artifact := filepath.Join(dir, "seismic.jsonl")
	original := []byte("{\"event_id\":\"original\",\"occurred_at\":\"2025-01-01T00:00:00Z\"}\n")
	changed := []byte("{\"event_id\":\"modified\",\"occurred_at\":\"2025-01-01T00:00:00Z\"}\n")
	if len(original) != len(changed) {
		t.Fatal("fixture must replace bytes without changing file size")
	}
	if err := os.WriteFile(artifact, original, 0600); err != nil {
		t.Fatal(err)
	}
	catalogFile, auditFile := filepath.Join(dir, "catalog.token"), filepath.Join(dir, "audit.token")
	if err := os.WriteFile(catalogFile, []byte("catalog-secret\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(auditFile, []byte("audit-secret\n"), 0600); err != nil {
		t.Fatal(err)
	}
	manifest := filepath.Join(dir, "resources.json")
	if err := os.WriteFile(manifest, []byte(`{"reference_checksum":"`+expected+`","resources":{"cpu":"synthetic-fixture"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(dir, "report.json")
	digest := sha256.Sum256(original)
	wantID := hex.EncodeToString(digest[:])
	rewriteSource := sync.OnceValue(func() error {
		return os.WriteFile(artifact, changed, 0600)
	})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/internal/audit/checksum":
			if acknowledged.Load() {
				fixtureAudit(w, 1, expected)
			} else {
				fixtureAudit(w, 0, fixtureDigest(""))
			}
		case "/internal/storage/measurements":
			if err := rewriteSource(); err != nil {
				t.Errorf("mutate source during preflight: %v", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_, _ = w.Write([]byte(completeMeasurements))
		case "/internal/catalog/seismic":
			if contents, err := os.ReadFile(artifact); err != nil || string(contents) != string(changed) {
				t.Errorf("source was not rewritten before catalog POST: %v", err)
			}
			var batch batchRequest
			if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
				t.Errorf("decode catalog request body: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if batch.InputID != wantID || batch.InputOffset != 0 || len(batch.Records) != 1 || string(batch.Records[0]) != strings.TrimSpace(string(original)) {
				t.Errorf("catalog POST did not use the original snapshot and receipt ID: %+v", batch)
			}
			acknowledged.Store(true)
			_ = json.NewEncoder(w).Encode(map[string]any{"accepted": len(batch.Records), "input_id": batch.InputID, "input_offset": batch.InputOffset})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	report, err := Run(context.Background(), Options{Artifact: artifact, BaseURL: server.URL, TokenFile: catalogFile, AuditTokenFile: auditFile, ResourcesManifest: manifest, RunOutput: output})
	if err != nil {
		t.Fatal(err)
	}
	if report.Artifact != artifact || report.ArtifactSHA256 != wantID || report.Rows != 1 ||
		report.ReferenceChecksum != expected || !report.DatasetAvailable {
		t.Fatalf("unexpected snapshot report: %+v", report)
	}
	encoded, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	var saved Report
	if err := json.Unmarshal(encoded, &saved); err != nil {
		t.Fatal(err)
	}
	if saved.ArtifactSHA256 != wantID || saved.Rows != 1 || !saved.DatasetAvailable ||
		saved.AuditBefore.AsOf != fixtureAsOf || saved.AuditAfter.AsOf != fixtureAsOf ||
		!saved.SubmissionStartedAt.Equal(report.SubmissionStartedAt) || !saved.SubmissionFinishedAt.Equal(report.SubmissionFinishedAt) {
		t.Fatalf("unexpected saved snapshot report: %+v", saved)
	}
	if files, err := os.ReadDir(snapshotDir); err != nil || len(files) != 0 {
		t.Fatalf("snapshot directory is not empty after Run: %v, %v", files, err)
	}
}

func TestLoadArrayKeepsBatchRowOffset(t *testing.T) {
	t.Parallel()
	// The streaming parser accepts an array without materializing the input document.
	input := strings.NewReader(`[ {"event_id":"a"}, {"event_id":"b"} ]`)
	br := bufio.NewReader(input)
	first, err := firstNonSpace(br)
	if err != nil || first != '[' {
		t.Fatalf("first token = %q, %v", first, err)
	}
	var sent batchRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/catalog/seismic" {
			http.NotFound(w, r)
			return
		}
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &sent)
		json.NewEncoder(w).Encode(map[string]any{"accepted": len(sent.Records), "input_id": sent.InputID, "input_offset": sent.InputOffset})
	}))
	defer server.Close()
	base, _ := url.Parse(server.URL)
	var rows int64
	if err := loadArtifact(context.Background(), br, server.Client(), base, "token", "artifact-hash", &rows); err != nil {
		t.Fatal(err)
	}
	if rows != 2 || sent.InputOffset != 0 || len(sent.Records) != 2 {
		t.Fatalf("rows=%d batch=%+v", rows, sent)
	}
}

func TestLoadFormatsFlushByConservativeBodyBytes(t *testing.T) {
	for _, format := range []string{"JSONL", "array"} {
		t.Run(format, func(t *testing.T) {
			payload := strings.Repeat("x", 80<<10)
			records := make([]string, 10)
			for i := range records {
				records[i] = `{"event_id":"event-` + string(rune('a'+i)) + `","payload":"` + payload + `"}`
			}
			var input string
			if format == "JSONL" {
				input = strings.Join(records, "\n")
			} else {
				input = "[" + strings.Join(records, ",") + "]"
			}

			var batchSizes []int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("read request body: %v", err)
				}
				if len(body) > maxWireBodyBytes {
					t.Errorf("request body has %d bytes; limit is %d", len(body), maxWireBodyBytes)
				}
				var batch batchRequest
				if err := json.Unmarshal(body, &batch); err != nil {
					t.Errorf("decode batch: %v", err)
				}
				batchSizes = append(batchSizes, len(batch.Records))
				_ = json.NewEncoder(w).Encode(map[string]any{"accepted": len(batch.Records), "input_id": batch.InputID, "input_offset": batch.InputOffset})
			}))
			defer server.Close()
			base, err := url.Parse(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			var rows int64
			err = loadArtifact(context.Background(), strings.NewReader(input), server.Client(), base, "token", "artifact-hash", &rows)
			if err != nil {
				t.Fatal(err)
			}
			if rows != int64(len(records)) || len(batchSizes) < 2 {
				t.Fatalf("rows=%d batches=%v; expected byte-bounded flushes below count cap", rows, batchSizes)
			}
			for _, size := range batchSizes {
				if size == 0 || size >= batchRecords {
					t.Fatalf("batch size %d did not flush below count cap", size)
				}
			}
		})
	}
}

func TestSendBatchRejectsOversizedSingleRecordBeforePost(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests++
	}))
	defer server.Close()
	base, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	record := json.RawMessage(`{"payload":"` + strings.Repeat("x", maxWireBodyBytes) + `"}`)
	err = sendBatch(context.Background(), server.Client(), base, "token", "artifact-hash", 0, []json.RawMessage{record})
	if err == nil {
		t.Fatal("expected oversized batch to be rejected")
	}
	if requests != 0 {
		t.Fatalf("oversized batch attempted %d POST requests", requests)
	}
}

func TestChecksumRequiresCompleteAppendixAEnvelope(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, key string
		value     any
		remove    bool
	}{
		{name: "healthy"},
		{name: "explicit zero offset", key: "as_of", value: "2026-11-20T01:02:03+00:00"},
		{name: "missing source", key: "source", remove: true},
		{name: "wrong source", key: "source", value: "PVMBG"},
		{name: "null source", key: "source"},
		{name: "missing count", key: "count", remove: true},
		{name: "null count", key: "count"},
		{name: "negative count", key: "count", value: -1},
		{name: "fraction count", key: "count", value: 0.5},
		{name: "bool count", key: "count", value: false},
		{name: "missing checksum", key: "checksum", remove: true},
		{name: "uppercase checksum", key: "checksum", value: strings.ToUpper(fixtureDigest(""))},
		{name: "nonempty digest for zero count", key: "checksum", value: fixtureDigest("missing")},
		{name: "missing algorithm", key: "algorithm", remove: true},
		{name: "wrong algorithm", key: "algorithm", value: "SHA256"},
		{name: "missing timestamp", key: "as_of", remove: true},
		{name: "null timestamp", key: "as_of"},
		{name: "numeric timestamp", key: "as_of", value: 1},
		{name: "non-UTC timestamp", key: "as_of", value: "2026-11-20T01:02:03+07:00"},
		{name: "invalid timestamp", key: "as_of", value: "2026-11-31T01:02:03Z"},
		{name: "missing partial", key: "partial", remove: true},
		{name: "null partial", key: "partial"},
		{name: "string partial", key: "partial", value: "false"},
		{name: "partial checksum", key: "partial", value: true},
		{name: "missing unavailable units", key: "unavailable_units", remove: true},
		{name: "null unavailable units", key: "unavailable_units"},
		{name: "non-array unavailable units", key: "unavailable_units", value: map[string]any{}},
		{name: "non-empty unavailable units", key: "unavailable_units", value: []string{"store-a"}},
		{name: "unknown key", key: "Partial", value: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := httptest.NewRecorder()
			fixtureAudit(fixture, 0, fixtureDigest(""))
			var envelope map[string]any
			if err := json.Unmarshal(fixture.Body.Bytes(), &envelope); err != nil {
				t.Fatal(err)
			}
			if tc.remove {
				delete(envelope, tc.key)
			} else if tc.key != "" {
				envelope[tc.key] = tc.value
			}
			raw, err := json.Marshal(envelope)
			if err != nil {
				t.Fatal(err)
			}
			wantValid := tc.name == "healthy" || tc.name == "explicit zero offset"
			audit, err := parseAuditChecksum(raw)
			if (err == nil) != wantValid {
				t.Fatalf("valid=%t, want %t for %s: %v", err == nil, wantValid, raw, err)
			}
			if wantValid && audit.AsOf != envelope["as_of"] {
				t.Fatalf("as_of was not preserved: %+v", audit)
			}
		})
	}

	fixture := httptest.NewRecorder()
	fixtureAudit(fixture, 0, fixtureDigest(""))
	duplicate := strings.TrimSuffix(strings.TrimSpace(fixture.Body.String()), "}") + `,"partial":false}`
	if _, err := parseAuditChecksum([]byte(duplicate)); err == nil {
		t.Fatal("accepted a duplicate Appendix A key")
	}
}

func TestRunVerifiesFinalPopulationIndependentlyOfReceipt(t *testing.T) {
	t.Parallel()
	const baseline = `[{"event_id":"é"}]`
	const artifact = "{\"event_id\":\"a \"}\n{\"event_id\":\"é\"}\n{\"event_id\":\"a \"}\n"
	expected := fixtureDigest("a \né")
	for _, tc := range []struct {
		name, reportedIDs, reference, wantError string
		finalCount                              int64
		resume                                  bool
	}{
		{name: "new distinct ID plus duplicate rows", reportedIDs: "a \né", reference: expected, finalCount: 2},
		{name: "idempotent resume with unchanged totals", reportedIDs: "a \né", reference: expected, finalCount: 2, resume: true},
		{name: "acknowledged without new population", reportedIDs: "é", reference: expected, finalCount: 1, wantError: "post-load Audit population"},
		{name: "right digest but wrong count", reportedIDs: "a \né", reference: expected, finalCount: 3, wantError: "post-load Audit population"},
		{name: "right count but wrong digest", reportedIDs: "a \ny", reference: expected, finalCount: 2, wantError: "post-load Audit population"},
		{name: "reference matches only existing population", reportedIDs: "é", reference: fixtureDigest("é"), finalCount: 1, wantError: "supplied reference"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			for name, contents := range map[string]string{
				"baseline.json": baseline, "upload.jsonl": artifact, "catalog.token": "catalog",
				"audit.token": "audit", "resources.json": `{"resources":{"cpu":"fixture"}}`,
			} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0600); err != nil {
					t.Fatal(err)
				}
			}
			var acknowledged atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/internal/audit/checksum":
					if acknowledged.Load() || tc.resume {
						fixtureAudit(w, tc.finalCount, fixtureDigest(tc.reportedIDs))
					} else {
						fixtureAudit(w, 1, fixtureDigest("é"))
					}
				case "/internal/storage/measurements":
					_, _ = w.Write([]byte(completeMeasurements))
				case "/internal/catalog/seismic":
					var batch batchRequest
					if err := json.NewDecoder(r.Body).Decode(&batch); err != nil || len(batch.Records) != 3 {
						t.Errorf("unexpected batch: %+v, %v", batch, err)
					}
					acknowledged.Store(true)
					_ = json.NewEncoder(w).Encode(map[string]any{"accepted": len(batch.Records), "input_id": batch.InputID, "input_offset": batch.InputOffset})
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			options := Options{
				Artifact: filepath.Join(dir, "upload.jsonl"), BaselineArtifact: filepath.Join(dir, "baseline.json"),
				BaseURL: server.URL, TokenFile: filepath.Join(dir, "catalog.token"),
				AuditTokenFile: filepath.Join(dir, "audit.token"), ExpectedChecksum: tc.reference,
				ResourcesManifest: filepath.Join(dir, "resources.json"), RunOutput: filepath.Join(dir, "report.json"),
			}
			report, err := Run(t.Context(), options)
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) || report.DatasetAvailable {
					t.Fatalf("Run failed to reject invalid population: %+v, %v", report, err)
				}
				if acknowledged.Load() != (tc.reference == expected) {
					t.Fatal("reference validation and batch acknowledgement happened in the wrong order")
				}
				if _, err := os.Stat(options.RunOutput); !os.IsNotExist(err) {
					t.Fatalf("unsuccessful Run wrote a report: %v", err)
				}
				return
			}
			if err != nil || !acknowledged.Load() || !report.DatasetAvailable || report.Rows != 3 ||
				report.ExpectedPopulation.Count != 2 || report.ExpectedPopulation.Checksum != expected ||
				report.BaselineArtifactSHA256 != fixtureDigest(baseline) {
				t.Fatalf("unexpected successful population report: %+v, %v", report, err)
			}
			if tc.resume && report.AuditBefore.Count != report.AuditAfter.Count {
				t.Fatalf("resume unexpectedly added logical records: %+v", report)
			}
		})
	}
}
