//go:build integration

package aggregator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const (
	catalogCLIReference      = "712b744466f9b29cb92ee1d952e2cd9221080f8f946597f9e363ce1fb4b60fdd"
	catalogCLIArtifactSHA256 = "c60f337f946681c41f5fca028f220a9c5c038c548daef947478cc9a4ccc38826"
)

func TestCatalogLoadCLIThroughRoutedAggregator(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	artifact, resources, _ := readCatalogCLIFixture(t)
	cfg, follower, posts := newRoutedCatalogFixture(t, ctx)
	before := readCatalogCLIProof(t, ctx, follower.URL, cfg.Catalog.Token, cfg.Audit.Token)

	temp := t.TempDir()
	catalogToken := writeCatalogCLIToken(t, temp, "catalog.token", cfg.Catalog.Token)
	auditToken := writeCatalogCLIToken(t, temp, "audit.token", cfg.Audit.Token)
	output := filepath.Join(temp, "run.json")
	command := exec.CommandContext(ctx, "go", "run", "../../cmd/catalog-load",
		"-artifact", artifact,
		"-resources-manifest", resources,
		"-expected-checksum", catalogCLIReference,
		"-base-url", follower.URL,
		"-token-file", catalogToken,
		"-audit-token-file", auditToken,
		"-run-output", output)
	combined, err := command.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || !strings.Contains(string(combined), "pre-load measurements are partial") {
		t.Fatalf("catalog-load did not refuse partial preflight: %v: %s", err, combined)
	}
	if _, err := os.Stat(output); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("catalog-load created a run report or its absence could not be verified: %v", err)
	}
	after := readCatalogCLIProof(t, ctx, follower.URL, cfg.Catalog.Token, cfg.Audit.Token)
	if posts.Load() != 0 || !sameCatalogCLIState(before, after) {
		t.Fatalf("partial preflight wrote catalog state: posts=%d before=%+v after=%+v", posts.Load(), before, after)
	}
}

func TestCatalogPostThroughRoutedAggregator(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	_, _, artifact := readCatalogCLIFixture(t)
	cfg, follower, _ := newRoutedCatalogFixture(t, ctx)
	before := readCatalogCLIProof(t, ctx, follower.URL, cfg.Catalog.Token, cfg.Audit.Token)
	receipt := catalogReceipt{InputID: catalogCLIArtifactSHA256, ReceivedAt: time.Now().UTC()}
	for _, line := range bytes.Split(bytes.TrimSpace(artifact), []byte{'\n'}) {
		receipt.Records = append(receipt.Records, json.RawMessage(line))
	}
	payload, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}

	postRoutedCatalogFixture(t, ctx, follower.URL, cfg.Catalog.Token, payload)
	after := readCatalogCLIProof(t, ctx, follower.URL, cfg.Catalog.Token, cfg.Audit.Token)
	if after.AuditCount != 6 || after.AuditChecksum != catalogCLIReference {
		t.Fatalf("synthetic catalog Audit differs from fixture: %+v", after)
	}
	assertCatalogCLIMetricsDelta(t, before, after, 6)
	if after.OutboxRevisions-before.OutboxRevisions != 6 {
		t.Fatalf("outbox revisions changed by %d, want 6", after.OutboxRevisions-before.OutboxRevisions)
	}

	postRoutedCatalogFixture(t, ctx, follower.URL, cfg.Catalog.Token, payload)
	afterRetry := readCatalogCLIProof(t, ctx, follower.URL, cfg.Catalog.Token, cfg.Audit.Token)
	if !sameCatalogCLIState(after, afterRetry) {
		t.Fatalf("retry changed catalog state: before=%+v after=%+v", after, afterRetry)
	}
}

func postRoutedCatalogFixture(t *testing.T, ctx context.Context, baseURL, token string, payload []byte) {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/internal/catalog/seismic", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var accepted struct {
		Count   int    `json:"accepted"`
		InputID string `json:"input_id"`
	}
	if err := json.NewDecoder(response.Body).Decode(&accepted); err != nil ||
		response.StatusCode != http.StatusOK || accepted.Count != 6 || accepted.InputID != catalogCLIArtifactSHA256 {
		t.Fatalf("synthetic routed POST status=%d receipt=%+v decode error=%v", response.StatusCode, accepted, err)
	}
}

func readCatalogCLIFixture(t *testing.T) (string, string, []byte) {
	t.Helper()
	artifact := filepath.Join("..", "placementcompare", "testdata", "catalog.jsonl")
	resources := filepath.Join("..", "placementcompare", "testdata", "resources.json")
	artifactBytes, err := os.ReadFile(artifact)
	if err != nil {
		t.Fatal(err)
	}
	artifactHash := sha256.Sum256(artifactBytes)
	if got := hex.EncodeToString(artifactHash[:]); got != catalogCLIArtifactSHA256 {
		t.Fatalf("catalog fixture SHA-256 = %s, want %s", got, catalogCLIArtifactSHA256)
	}
	resourceBytes, err := os.ReadFile(resources)
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		ReferenceChecksum string                     `json:"reference_checksum"`
		Resources         map[string]json.RawMessage `json:"resources"`
	}
	if err := json.Unmarshal(resourceBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.ReferenceChecksum != catalogCLIReference || string(manifest.Resources["fixture_kind"]) != `"synthetic"` ||
		string(manifest.Resources["execution"]) != `"single local process and isolated PostgreSQL 18.1 test instance"` {
		t.Fatalf("resource manifest does not describe the expected synthetic one-instance fixture")
	}
	return artifact, resources, artifactBytes
}

func newRoutedCatalogFixture(t *testing.T, ctx context.Context) (Config, *httptest.Server, *atomic.Int32) {
	t.Helper()
	cfg := routedM2Config(t)
	for _, address := range []*string{&cfg.DatabaseURL, &cfg.ShardURLs[0], &cfg.ShardURLs[1], &cfg.ShardURLs[2], &cfg.WriterURL, &cfg.ShardWriterURLs[0], &cfg.ShardWriterURLs[1], &cfg.ShardWriterURLs[2]} {
		*address = withSyntheticMeasurementSettings(t, *address)
	}
	cfg.Catalog.Token = "catalog-cli-synthetic-token-7bb2"
	cfg.Audit.Token = "audit-cli-synthetic-token-c23f"
	store := openRoutedM2Store(t, cfg)

	kube, host := newRecoveryAuthorityKubernetes(recoveryClosedRecord(0)), newRecoveryHostFence()
	ownerMux := http.NewServeMux()
	ownerServer := httptest.NewServer(ownerMux)
	t.Cleanup(ownerServer.Close)
	ownerAuthority := openRecoveryAuthority(t, cfg, store, kube, host, "catalog-cli-owner")
	ownerAuthority.ownerEndpoint = ownerServer.URL
	registerCatalogRoutes(ownerMux, store, cfg, ownerAuthority)
	active, _, err := ownerAuthority.activate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if active.State != authorityActive || active.OwnerEndpoint != ownerServer.URL {
		t.Fatalf("synthetic owner endpoint was not published in the active authority record")
	}

	followerAuthority := openRecoveryAuthority(t, cfg, store, kube, host, "catalog-cli-follower")
	followerMux := http.NewServeMux()
	registerAuditRoutes4(followerMux, store, cfg, 0)
	registerCatalogRoutes(followerMux, store, cfg, followerAuthority)
	posts := new(atomic.Int32)
	follower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/internal/catalog/seismic" {
			posts.Add(1)
		}
		followerMux.ServeHTTP(w, r)
	}))
	t.Cleanup(follower.Close)
	return cfg, follower, posts
}

type catalogCLIAudit struct {
	Count    int64  `json:"count"`
	Checksum string `json:"checksum"`
	Partial  bool   `json:"partial"`
}

type catalogCLIProof struct {
	AuditCount      int64
	AuditChecksum   string
	OutboxRevisions int64
	Tables          map[string]int64
}

func withSyntheticMeasurementSettings(t *testing.T, raw string) string {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("options", "-c bnpb.node_name=synthetic-kernel-node -c bnpb.unit_name=synthetic-kernel-unit")
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func writeCatalogCLIToken(t *testing.T, dir, name, token string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(token+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func readCatalogCLIProof(t *testing.T, ctx context.Context, baseURL, catalogToken, auditToken string) catalogCLIProof {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/internal/storage/measurements", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+catalogToken)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("storage measurements status = %d", response.StatusCode)
	}
	var measurements storageMeasurements
	if err := json.NewDecoder(response.Body).Decode(&measurements); err != nil {
		t.Fatal(err)
	}
	if !measurements.Partial || len(measurements.Items) != 3 ||
		!slices.Contains(measurements.UnavailableUnits, unverifiedMeasurementInventory) ||
		!slices.Contains(measurements.UnavailableUnits, "synthetic-kernel-unit") {
		t.Fatalf("single-instance measurement inventory was not reported as partial and duplicate: %+v", measurements)
	}
	proof := catalogCLIProof{Tables: make(map[string]int64)}
	for _, connection := range measurements.Items {
		if !connection.Partial || !connection.SettingsComplete || connection.Node != "synthetic-kernel-node" || connection.Unit != "synthetic-kernel-unit" || connection.Role != "primary" {
			t.Fatalf("expected observed synthetic primary with unverified placement: %+v", connection)
		}
		proof.OutboxRevisions += connection.CanonicalRevisions
		for _, table := range connection.Tables {
			proof.Tables[table.Name] += table.Rows
		}
	}
	auditRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/internal/audit/checksum?source=BMKG", nil)
	if err != nil {
		t.Fatal(err)
	}
	auditRequest.Header.Set("Authorization", "Bearer "+auditToken)
	auditResponse, err := http.DefaultClient.Do(auditRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer auditResponse.Body.Close()
	if auditResponse.StatusCode != http.StatusOK {
		t.Fatalf("Audit checksum status = %d", auditResponse.StatusCode)
	}
	var audit catalogCLIAudit
	if err := json.NewDecoder(auditResponse.Body).Decode(&audit); err != nil {
		t.Fatal(err)
	}
	if audit.Partial {
		t.Fatal("fixture logical Audit checksum is partial")
	}
	proof.AuditCount, proof.AuditChecksum = audit.Count, audit.Checksum
	return proof
}

func assertCatalogCLIMetricsDelta(t *testing.T, before, after catalogCLIProof, want int64) {
	t.Helper()
	for _, table := range []string{"hazard_events", "source_seismic", "outbox", "handled_inputs"} {
		if got := after.Tables[table] - before.Tables[table]; got != want {
			t.Fatalf("%s relation metric delta = %d, want %d", table, got, want)
		}
	}
}

func sameCatalogCLIState(left, right catalogCLIProof) bool {
	return left.AuditCount == right.AuditCount && left.AuditChecksum == right.AuditChecksum &&
		left.OutboxRevisions == right.OutboxRevisions && reflect.DeepEqual(left.Tables, right.Tables)
}
