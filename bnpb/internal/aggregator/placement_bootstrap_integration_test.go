//go:build integration

package aggregator

import (
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"
)

func configuredBootstrap(t *testing.T, cfg Config, revision int64, copies []shardPlacement) Config {
	t.Helper()
	env := requiredEnvironment()
	env["BNPB_CANONICAL_DATABASE_URL"] = cfg.DatabaseURL
	env["BNPB_SHARD_PLACEMENT"] = bootstrapJSON(t, revision, copies)
	configured, err := configFromEnvironment(env)
	if err != nil {
		t.Fatal(err)
	}
	configured.DBWriteTimeout = 10 * time.Second
	return configured
}

func TestPlacementBootstrapPersistsAcrossStartup(t *testing.T) {
	cfg := configuredBootstrap(t, m2IsolatedConfig(t), 0, rotatingPlacement())
	store := m2OpenStore(t, cfg)
	before, err := store.placement(t.Context())
	if err != nil || len(before.Copies) != 6 || before.Revision <= 0 {
		t.Fatalf("startup placement not persisted: %+v, %v", before, err)
	}
	want := map[string]shardPlacement{}
	for _, copy := range rotatingPlacement() {
		want[copy.Unit] = copy
	}
	for _, copy := range before.Copies {
		if want[copy.Unit] != copy {
			t.Fatalf("stored copy differs: %+v", copy)
		}
	}
	if err := store.Ready(t.Context()); err != nil {
		t.Fatal(err)
	}
	store.Close()
	reordered := rotatingPlacement()
	slices.Reverse(reordered)
	restartCfg := configuredBootstrap(t, cfg, 0, reordered)
	restarted := m2OpenStore(t, restartCfg)
	after, err := restarted.placement(t.Context())
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("same-config restart changed inventory/revision: %+v, %v", after, err)
	}
	preserveCfg := restartCfg
	preserveCfg.ShardPlacement = ""
	preserved := m2OpenStore(t, preserveCfg)
	afterOmission, err := preserved.placement(t.Context())
	if err != nil || !reflect.DeepEqual(before, afterOmission) {
		t.Fatalf("omitted configuration erased placement: %+v, %v", afterOmission, err)
	}

	changed := rotatingPlacement()
	changed[1].State = "retiring"
	staleCfg := configuredBootstrap(t, cfg, 0, changed)
	if rejected, err := OpenStore(t.Context(), staleCfg, quietLogger()); rejected != nil || !errors.Is(err, errPlacementChanged) {
		if rejected != nil {
			rejected.Close()
		}
		t.Fatalf("stale different config accepted: %v", err)
	}
	updatedCfg := configuredBootstrap(t, cfg, before.Revision, changed)
	updated := m2OpenStore(t, updatedCfg)
	updatedPlacement, err := updated.placement(t.Context())
	if err != nil || updatedPlacement.Revision <= before.Revision {
		t.Fatalf("current revision change not persisted: %+v, %v", updatedPlacement, err)
	}
	for _, copy := range updatedPlacement.Copies {
		if copy.Unit == "s0-b" && copy.State != "retiring" {
			t.Fatalf("configured state not applied: %+v", copy)
		}
	}
	repeated := m2OpenStore(t, updatedCfg)
	repeatedPlacement, err := repeated.placement(t.Context())
	if err != nil || !reflect.DeepEqual(updatedPlacement, repeatedPlacement) {
		t.Fatalf("updated configuration not idempotent: %+v, %v", repeatedPlacement, err)
	}
	futureCfg := configuredBootstrap(t, cfg, repeatedPlacement.Revision+100, repeatedPlacement.Copies)
	if rejected, err := OpenStore(t.Context(), futureCfg, quietLogger()); rejected != nil || !errors.Is(err, errPlacementChanged) {
		if rejected != nil {
			rejected.Close()
		}
		t.Fatalf("future revision accepted: %v", err)
	}
}

func TestPlacementBootstrapRestartPreservesRepair(t *testing.T) {
	cfg := configuredBootstrap(t, m2IsolatedConfig(t), 0, rotatingPlacement())
	store := m2OpenStore(t, cfg)
	initial, err := store.placement(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	candidate := repairCandidateFixture()
	evidence := repairEvidenceFixture(candidate)
	intent := repairIntent(candidate, evidence)
	reservedRevision, err := store.reserveStandbyRepair(t.Context(), initial, candidate, intent)
	if err != nil {
		t.Fatal(err)
	}
	reserved, err := store.placement(t.Context())
	if err != nil || reserved.Revision != reservedRevision {
		t.Fatalf("read reserved placement: %+v, %v", reserved, err)
	}
	store.Close()

	reordered := rotatingPlacement()
	slices.Reverse(reordered)
	restartCfg := configuredBootstrap(t, cfg, 0, reordered)
	restarted := m2OpenStore(t, restartCfg)
	afterReserve, err := restarted.placement(t.Context())
	if err != nil || !reflect.DeepEqual(reserved, afterReserve) {
		t.Fatalf("restart changed reserved placement: %+v, %v", afterReserve, err)
	}
	if revision, err := restarted.reserveStandbyRepair(t.Context(), afterReserve, candidate, intent); err != nil || revision != reservedRevision {
		t.Fatalf("restart could not resume reservation: revision=%d err=%v", revision, err)
	}
	observedCfg := configuredBootstrap(t, cfg, afterReserve.Revision, afterReserve.Copies)
	restarted.Close()
	restarted = m2OpenStore(t, observedCfg)
	for _, status := range []string{"provisioning", "copying"} {
		if err := restarted.setStandbyRepairStatus(t.Context(), candidate, reservedRevision, status); err != nil {
			t.Fatal(err)
		}
	}
	if err := restarted.completeStandbyRepair(t.Context(), candidate, reservedRevision, evidence); err != nil {
		t.Fatal(err)
	}
	completed, err := restarted.placement(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	restarted.Close()

	afterCompletion := m2OpenStore(t, observedCfg)
	current, err := afterCompletion.placement(t.Context())
	if err != nil || !reflect.DeepEqual(completed, current) {
		t.Fatalf("restart changed completed placement: %+v, %v", current, err)
	}
	if done, err := afterCompletion.repairAlreadyVerified(t.Context(), candidate, current.Copies); err != nil || !done {
		t.Fatalf("restart lost verified repair: done=%t err=%v", done, err)
	}
	stale := configuredBootstrap(t, cfg, initial.Revision, reordered)
	rejected, err := OpenStore(t.Context(), stale, quietLogger())
	if rejected != nil {
		rejected.Close()
		t.Fatal("restart accepted a stale revision with the same original inventory")
	}
	if !errors.Is(err, errPlacementChanged) {
		t.Fatalf("stale bootstrap revision: %v", err)
	}
}

func TestPlacementBootstrapConcurrentReplicas(t *testing.T) {
	cfg := configuredBootstrap(t, m2IsolatedConfig(t), 0, rotatingPlacement())
	type opened struct {
		store *Store
		err   error
	}
	outcomes := make(chan opened, 2)
	start := make(chan struct{})
	for range 2 {
		go func() {
			<-start
			store, err := OpenStore(t.Context(), cfg, quietLogger())
			outcomes <- opened{store, err}
		}()
	}
	close(start)
	var initialized []*Store
	for range 2 {
		outcome := <-outcomes
		if outcome.store != nil {
			initialized = append(initialized, outcome.store)
			t.Cleanup(outcome.store.Close)
		}
		if outcome.err != nil {
			t.Errorf("concurrent identical bootstrap failed: %v", outcome.err)
		}
	}
	if t.Failed() {
		return
	}
	first, err := initialized[0].placement(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	second, err := initialized[1].placement(t.Context())
	if err != nil || len(first.Copies) != 6 || !reflect.DeepEqual(first, second) {
		t.Fatalf("replicas disagree: %+v / %+v, %v", first, second, err)
	}
	var revisions int
	if err := initialized[0].read.QueryRow(t.Context(), "SELECT count(*) FROM schema_migrations").Scan(&revisions); err != nil || revisions != currentSchemaVersion {
		t.Fatalf("concurrent schema bootstrap duplicated history: %d, %v", revisions, err)
	}
}

func TestPlacementBootstrapRejectsOneCopyInitialization(t *testing.T) {
	copies := []shardPlacement{rotatingPlacement()[0], rotatingPlacement()[2], rotatingPlacement()[4]}
	cfg := configuredBootstrap(t, m2IsolatedConfig(t), 0, copies)
	store, err := OpenStore(t.Context(), cfg, quietLogger())
	if store != nil || !errors.Is(err, errInvalidPlacement) {
		if store != nil {
			store.Close()
		}
		t.Fatalf("unreplicated initial placement accepted: %v", err)
	}
	cfg.ShardPlacement = ""
	preserved := m2OpenStore(t, cfg)
	placement, err := preserved.placement(t.Context())
	if err != nil || placement.Revision != 0 || len(placement.Copies) != 0 {
		t.Fatalf("failed bootstrap left partial placement: %+v, %v", placement, err)
	}
}
