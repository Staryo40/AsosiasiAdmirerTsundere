package aggregator

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRepairConfirmsUnresponsiveReplicaWithinCycle(t *testing.T) {
	t.Parallel()
	var probes atomic.Int32
	patroni := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		probes.Add(1)
		_, _ = w.Write([]byte(`{"patroni":{"scope":"s0","name":"replica"},"role":"replica","state":"running","database_system_identifier":"1","timeline":1}`))
	}))
	defer patroni.Close()
	// Leave TCP open without answering the PostgreSQL startup message.
	database, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	member := repairMember{Unit: "s0-b", Node: "node-b", Scope: "s0", Name: "replica", PatroniURL: patroni.URL,
		DSN: "postgresql://fixture@" + database.Addr().String() + "/fixture?sslmode=disable"}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := confirmKnownReplicaUnhealthy(ctx, 0, member); err != nil || ctx.Err() != nil {
		t.Fatalf("confirmation did not leave time for repair: probe=%v cycle=%v", err, ctx.Err())
	}
	if count := probes.Load(); count != 2 {
		t.Fatalf("Patroni observed %d replica probes, want 2", count)
	}
}

func TestRepairRejectsMalformedWALPositions(t *testing.T) {
	t.Parallel()
	for _, position := range []string{"0/30junk", "0/30/40", "0/ 30", "+0/30", "0/000000030", "0/100000000", "0/", "/30"} {
		t.Run(position, func(t *testing.T) {
			if lsnAtLeast(position, "0/30") || lsnAtLeast("0/30", position) {
				t.Fatalf("malformed WAL position %q accepted", position)
			}
		})
	}
}

func TestRepairIntentPreservesPersistedJSON(t *testing.T) {
	candidate := standbyRepairCandidate{
		ShardID: 1, Target: repairMember{Unit: "s1-d", Node: "node-d", Scope: "shard-1", Name: "replacement"},
		StatefulSet: "standby-1", PatroniConfigMap: "patroni-1", Claim: "disk-1",
		Source: repairMember{Unit: "s1-b", Node: "node-b"}, OldReplica: repairMember{Unit: "s1-c", Node: "node-c"},
	}
	want := `{"shard_id":1,"unit":"s1-d","node":"node-d","scope":"shard-1","name":"replacement","stateful_set":"standby-1","patroni_config_map":"patroni-1","claim":"disk-1","system_id":"system-1","source_unit":"s1-b","source_node":"node-b","old_replica_unit":"s1-c","old_replica_node":"node-c"}`
	if got := string(repairIntent(candidate, repairEvidence{SystemID: "system-1"})); got != want {
		t.Fatalf("persisted repair intent changed: got %s, want %s", got, want)
	}
}

func TestPatroniRepairConfigRequiresLiteralIdentityAndSafeReplicaMethod(t *testing.T) {
	member := repairMember{Scope: "shard-0", Name: "store-0-replica"}
	for _, test := range []struct {
		name   string
		config string
		valid  bool
	}{
		{name: "basebackup", config: `{"scope":"shard-0","name":"store-0-replica","tags":{"nofailover":true},"kubernetes":{"role_label":"pg-role"},"postgresql":{"data_dir":"/data","create_replica_methods":["basebackup"]}}`, valid: true},
		{name: "default method", config: `{"scope":"shard-0","name":"store-0-replica","tags":{"nofailover":true},"kubernetes":{"role_label":"pg-role"},"postgresql":{"data_dir":"/data"}}`, valid: true},
		{name: "identity override", config: `{"scope":"shard-0","name":"store-0-primary","tags":{"nofailover":true},"kubernetes":{"role_label":"pg-role"},"postgresql":{"data_dir":"/data"}}`},
		{name: "unsafe method", config: `{"scope":"shard-0","name":"store-0-replica","tags":{"nofailover":true},"kubernetes":{"role_label":"pg-role"},"postgresql":{"data_dir":"/data","create_replica_methods":["custom"]}}`},
		{name: "unsupported singular replica method", config: `{"scope":"shard-0","name":"store-0-replica","tags":{"nofailover":true},"kubernetes":{"role_label":"pg-role"},"postgresql":{"data_dir":"/data","create_replica_method":["basebackup"]}}`},
		{name: "temporary role label overwrites store role", config: `{"scope":"shard-0","name":"store-0-replica","tags":{"nofailover":true},"kubernetes":{"role_label":"pg-role","tmp_role_label":"role"},"postgresql":{"data_dir":"/data"}}`},
		{name: "bootstrap label overwrites store role", config: `{"scope":"shard-0","name":"store-0-replica","tags":{"nofailover":true},"kubernetes":{"role_label":"pg-role","bootstrap_labels":{"role":"replica"}},"postgresql":{"data_dir":"/data"}}`},
		{name: "duplicate nofailover", config: `{"scope":"shard-0","name":"store-0-replica","tags":{"nofailover":true,"nofailover":false},"kubernetes":{"role_label":"pg-role"},"postgresql":{"data_dir":"/data"}}`},
		{name: "mismatched data directory", config: `{"scope":"shard-0","name":"store-0-replica","tags":{"nofailover":true},"kubernetes":{"role_label":"pg-role"},"postgresql":{"data_dir":"/elsewhere"}}`},
		{name: "overridden effective data directory", config: `{"scope":"shard-0","name":"store-0-replica","tags":{"nofailover":true},"kubernetes":{"role_label":"pg-role"},"postgresql":{"data_dir":"/data","parameters":{"data_directory":"/elsewhere"}}}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := validRepairPatroniConfig(test.config, member, repairPodConfig{dataDir: "/data"}); got != test.valid {
				t.Fatalf("validRepairPatroniConfig() = %t, want %t", got, test.valid)
			}
		})
	}
}

func TestPatroniRepairRejectsDataDirectoryEnvOverride(t *testing.T) {
	container := map[string]any{"env": []any{
		map[string]any{"name": "PGDATA", "value": "/data"},
		map[string]any{"name": "PATRONI_POSTGRESQL_DATA_DIR", "value": "/elsewhere"},
	}}
	if _, valid := postgresRepairEnv(container); valid {
		t.Fatal("Patroni data directory override was accepted")
	}
}

func TestPatroniRepairRequiresReadOnlyConfigMount(t *testing.T) {
	podSpec := map[string]any{
		"volumes": []any{
			map[string]any{"name": "data", "persistentVolumeClaim": map[string]any{"claimName": "data"}},
			map[string]any{"name": "patroni-config", "configMap": map[string]any{
				"name": "patroni", "items": []any{map[string]any{"key": "patroni.yaml", "path": "patroni.yaml"}},
			}},
		},
		"containers": []any{map[string]any{"name": "postgres", "command": []any{"patroni"},
			"args": []any{"/etc/patroni/patroni.yaml"}, "volumeMounts": []any{map[string]any{
				"name": "patroni-config", "mountPath": "/etc/patroni", "readOnly": true,
			}},
		}},
	}
	claimVolume, configVolume, bindingsValid := podVolumeBindings(podSpec, "data", "patroni")
	container := postgresRepairContainer(podSpec)
	if !bindingsValid || claimVolume != "data" || !postgresMountsPatroniConfig(container, configVolume) {
		t.Fatal("read-only Patroni config mount was not accepted")
	}
	container["volumeMounts"] = []any{}
	if postgresMountsPatroniConfig(container, configVolume) {
		t.Fatal("missing Patroni config mount was accepted")
	}
}

func TestRepairScaleResourceVersionConflictIsRetryable(t *testing.T) {
	patches, err := runRepairScale(t, repairScaleFixture{
		scaleVersion: "rv-2",
		scaleSpec:    map[string]any{"replicas": 0},
	})
	if err == nil || err == errRepairUnsafeEvidence {
		t.Fatalf("scaleUp() error = %v, want retryable resourceVersion conflict", err)
	}
	if patches != 0 {
		t.Fatalf("scale was patched %d times after the scale resourceVersion changed", patches)
	}
}

func TestRepairScaleWithMatchingResourceVersion(t *testing.T) {
	for _, test := range []struct {
		name        string
		fixture     repairScaleFixture
		wantPatches int
	}{
		{
			name:        "omitted zero replicas",
			fixture:     repairScaleFixture{scaleVersion: "rv-1", scaleSpec: map[string]any{}},
			wantPatches: 1,
		},
		{
			name:    "already scaled",
			fixture: repairScaleFixture{statefulSetReplicas: 1, scaleVersion: "rv-1", scaleSpec: map[string]any{"replicas": 1}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			patches, err := runRepairScale(t, test.fixture)
			if err != nil || patches != test.wantPatches {
				t.Fatalf("scaleUp() error = %v, PATCH requests = %d; want nil error and %d PATCH requests", err, patches, test.wantPatches)
			}
		})
	}
}

func TestRepairScaleRequiresPreboundClaimWithoutTemplates(t *testing.T) {
	patches, err := runRepairScale(t, repairScaleFixture{
		volumeClaimTemplates: []any{map[string]any{"metadata": map[string]any{"name": "data"}}},
		scaleVersion:         "rv-1",
		scaleSpec:            map[string]any{},
	})
	if err != errRepairUnsafeEvidence || patches != 0 {
		t.Fatalf("scale with a generated data claim: error=%v PATCHes=%d; want unsafe evidence and no PATCH", err, patches)
	}
}

type repairScaleFixture struct {
	statefulSetReplicas  int
	scaleVersion         string
	scaleSpec            map[string]any
	volumeClaimTemplates []any
}

func runRepairScale(t *testing.T, fixture repairScaleFixture) (int, error) {
	t.Helper()
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("test-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	const statefulSetPath = "/apis/apps/v1/namespaces/ns/statefulsets/ss"
	const scalePath = statefulSetPath + "/scale"
	const statefulSetVersion = "rv-1"
	var requestMu sync.Mutex
	requestCounts := make(map[string]int)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestPath := r.Method + " " + r.URL.Path
		requestMu.Lock()
		requestCounts[requestPath]++
		requestMu.Unlock()
		var response any
		switch requestPath {
		case "GET " + statefulSetPath:
			response = map[string]any{
				"metadata": map[string]any{"resourceVersion": statefulSetVersion},
				"spec": map[string]any{"replicas": fixture.statefulSetReplicas, "volumeClaimTemplates": fixture.volumeClaimTemplates, "template": map[string]any{
					"metadata": map[string]any{"labels": map[string]any{"role": "store"}},
					"spec": map[string]any{
						"nodeName": "node-1",
						"volumes": []any{
							map[string]any{"name": "data", "persistentVolumeClaim": map[string]any{"claimName": "pvc"}},
							map[string]any{"name": "patroni-config", "configMap": map[string]any{
								"name": "cm", "items": []any{map[string]any{"key": "patroni.yaml", "path": "patroni.yaml"}},
							}},
						},
						"containers": []any{map[string]any{
							"name": "postgres", "command": []any{"patroni"}, "args": []any{"/etc/patroni/patroni.yaml"},
							"env": []any{map[string]any{"name": "PGDATA", "value": "/data"}},
							"volumeMounts": []any{
								map[string]any{"name": "data", "mountPath": "/data"},
								map[string]any{"name": "patroni-config", "mountPath": "/etc/patroni", "readOnly": true},
							},
						}},
					},
				}},
			}
		case "GET /api/v1/namespaces/ns/configmaps/cm":
			response = map[string]any{
				"immutable": true,
				"data":      map[string]any{"patroni.yaml": `{"scope":"shard-0","name":"replica","tags":{"nofailover":true},"kubernetes":{"role_label":"pg-role"},"postgresql":{"data_dir":"/data"}}`},
			}
		case "GET /api/v1/namespaces/ns/persistentvolumeclaims/pvc":
			response = map[string]any{
				"metadata": map[string]any{"uid": "uid-1"},
				"status":   map[string]any{"phase": "Bound"},
				"spec":     map[string]any{"volumeName": "pv"},
			}
		case "GET /api/v1/persistentvolumes/pv":
			response = map[string]any{"spec": map[string]any{
				"local":    map[string]any{"path": "/data"},
				"claimRef": map[string]any{"namespace": "ns", "name": "pvc", "uid": "uid-1"},
				"nodeAffinity": map[string]any{"required": map[string]any{"nodeSelectorTerms": []any{
					map[string]any{"matchFields": []any{map[string]any{
						"key": "metadata.name", "operator": "In", "values": []any{"node-1"},
					}}},
				}}},
			}}
		case "GET " + scalePath:
			response = map[string]any{
				"metadata": map[string]any{"resourceVersion": fixture.scaleVersion},
				"spec":     fixture.scaleSpec,
			}
		case "PATCH " + scalePath:
			assertRepairScalePatch(t, r, statefulSetVersion)
			w.WriteHeader(http.StatusOK)
			return
		default:
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()
	executor := &standbyRepairExecutor{
		kubernetes: KubernetesConfig{ServerURL: server.URL, Namespace: "ns", TokenFile: tokenFile},
		http:       server.Client(),
	}
	candidate := standbyRepairCandidate{
		Namespace: "ns", StatefulSet: "ss", PatroniConfigMap: "cm", Claim: "pvc",
		Target: repairMember{Node: "node-1", Name: "replica", Scope: "shard-0"},
	}
	err := executor.scaleUp(t.Context(), candidate)
	requestMu.Lock()
	defer requestMu.Unlock()
	if requestCounts["GET "+statefulSetPath] != 1 {
		t.Error("missing StatefulSet preflight")
	}
	return requestCounts["PATCH "+scalePath], err
}

func assertRepairScalePatch(t *testing.T, r *http.Request, version string) {
	t.Helper()
	if got := r.Header.Get("Content-Type"); got != "application/json-patch+json" {
		t.Errorf("PATCH Content-Type = %q, want application/json-patch+json", got)
	}
	var patch []map[string]any
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		t.Errorf("decode scale patch: %v", err)
		return
	}
	want := []map[string]any{
		{"op": "test", "path": "/metadata/resourceVersion", "value": version},
		{"op": "add", "path": "/spec/replicas", "value": float64(1)},
	}
	if !reflect.DeepEqual(patch, want) {
		t.Errorf("scale patch = %v, want %v", patch, want)
	}
}

func TestPatroniRepairStoppedReplicaIsUnavailable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/patroni" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"patroni": map[string]string{"scope": "shard-0", "name": "replica"},
			"role":    "replica",
			"state":   "stopped",
		})
	}))
	defer server.Close()
	member := repairMember{Scope: "shard-0", Name: "replica", PatroniURL: server.URL, DSN: "invalid-dsn"}
	if _, err := probeRepairMember(t.Context(), 0, member, false); err != errRepairMemberUnavailable || !unavailableRepairProbe(err) {
		t.Fatalf("stopped replica probe error = %v, want unavailable", err)
	}
}

func TestPatroniRepairKnownReplicaMayBePromotionEligible(t *testing.T) {
	member := repairMember{Scope: "shard-0", Name: "replica"}
	observed := patroniObservation{
		SystemID: "system-0", Role: "replica", State: "running", Timeline: 1,
		Tags: map[string]any{"nofailover": false},
	}
	observed.Identity.Scope, observed.Identity.Name = member.Scope, member.Name
	if err := observed.verify(member, repairProbeReplica); err != nil {
		t.Fatalf("promotion-eligible known replica rejected: %v", err)
	}
	if err := observed.verify(member, repairProbeTarget); err != errRepairUnsafeEvidence {
		t.Fatalf("promotion-eligible repair target error = %v, want unsafe evidence", err)
	}
}

func TestVerifyRepairEvidenceRejectsReplayBehindFixedBoundary(t *testing.T) {
	const fixedLSN = "0/30"
	candidate := standbyRepairCandidate{
		ShardID: 1, Target: repairMember{Unit: "replica", Node: "node-2", Scope: "shard-1", Name: "replacement"},
	}
	primary := repairEvidence{Unit: "primary", Node: "node-1", SystemID: "system-1", Timeline: 1, PrimaryLSN: "0/20"}
	replacement := repairEvidence{
		ShardID: candidate.ShardID, Unit: candidate.Target.Unit, Node: candidate.Target.Node,
		Scope: candidate.Target.Scope, Name: candidate.Target.Name, SystemID: primary.SystemID,
		PrimaryUnit: primary.Unit, PrimaryNode: primary.Node, Timeline: primary.Timeline, PrimaryTimeline: primary.Timeline,
		PrimaryLSN: fixedLSN, ReplayLSN: fixedLSN, Synchronous: true,
	}
	if err := verifyRepairEvidence(primary, replacement, candidate); err != nil {
		t.Fatalf("replay at the fixed boundary rejected: %v", err)
	}
	replacement.ReplayLSN = "0/28" // Past the initial primary LSN, but short of the fixed boundary.
	if err := verifyRepairEvidence(primary, replacement, candidate); err != errRepairUnsafeEvidence {
		t.Fatalf("replay behind the fixed boundary error = %v, want unsafe evidence", err)
	}
}
