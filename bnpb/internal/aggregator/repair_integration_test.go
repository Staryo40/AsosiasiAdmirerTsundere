//go:build integration

package aggregator

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRepairReservationResumeAndCompletionRejectsPlacementABA(t *testing.T) {
	store := m2OpenStore(t, m2IsolatedConfig(t))
	if err := store.recordPlacement(t.Context(), placementSnapshot{Copies: rotatingPlacement()}); err != nil {
		t.Fatal(err)
	}
	candidate := repairCandidateFixture()
	current, err := store.placement(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	evidence := repairEvidenceFixture(candidate)
	intent := repairIntent(candidate, evidence)
	reservedRevision, err := store.reserveStandbyRepair(t.Context(), current, candidate, intent)
	if err != nil || reservedRevision <= current.Revision {
		t.Fatalf("initial reservation: revision=%d err=%v", reservedRevision, err)
	}
	reserved, err := store.placement(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if repeatedRevision, err := store.reserveStandbyRepair(t.Context(), reserved, candidate, intent); err != nil ||
		repeatedRevision != reservedRevision || reserved.Revision != reservedRevision {
		t.Fatalf("repeated reservation: revision=%d want=%d err=%v", repeatedRevision, reservedRevision, err)
	}
	mismatched := candidate
	mismatched.Target.Node = "node-other"
	if _, err := store.reserveStandbyRepair(t.Context(), reserved, mismatched,
		repairIntent(mismatched, evidence)); !errors.Is(err, errInvalidPlacement) {
		t.Fatalf("mismatched target node accepted: %v", err)
	}
	if err := store.setStandbyRepairStatus(t.Context(), candidate, reservedRevision, "provisioning"); err != nil {
		t.Fatal(err)
	}
	if err := store.setStandbyRepairStatus(t.Context(), candidate, reservedRevision, "copying"); err != nil {
		t.Fatal(err)
	}
	digest := strings.Repeat("a", 64)
	if _, err := store.write.Exec(t.Context(), `INSERT INTO shard_copy_loss_attestations
		(unit,shard_id,node,evidence_sha256) VALUES ('s0-b',0,'node-b',$1)`, digest); err != nil {
		t.Fatal(err)
	}
	changed := placementSnapshot{Revision: reserved.Revision, Copies: append([]shardPlacement(nil), reserved.Copies...)}
	for i := range changed.Copies {
		switch changed.Copies[i].Unit {
		case "s0-a":
			changed.Copies[i].Role = "replica"
		case "s0-b":
			changed.Copies[i].Role = "primary"
		}
	}
	start := make(chan struct{})
	outcomes := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			outcomes <- store.recordPlacement(t.Context(), changed)
		}()
	}
	close(start)
	first, second := <-outcomes, <-outcomes
	if !((first == nil && errors.Is(second, errPlacementChanged)) ||
		(second == nil && errors.Is(first, errPlacementChanged))) {
		t.Fatalf("concurrent stale revision outcomes: %v / %v", first, second)
	}
	if err := store.recordPlacement(t.Context(), reserved); !errors.Is(err, errPlacementChanged) {
		t.Fatalf("stale placement revision accepted: %v", err)
	}
	if err := store.completeStandbyRepair(t.Context(), candidate, reservedRevision, evidence); !errors.Is(err, errPlacementChanged) {
		t.Fatalf("completion accepted changed source primary: %v", err)
	}
	var status, targetState string
	if err := store.write.QueryRow(t.Context(), `SELECT j.status,p.state FROM shard_repair_jobs j
		JOIN shard_placements p ON p.unit=j.unit WHERE j.shard_id=$1`, candidate.ShardID).Scan(&status, &targetState); err != nil {
		t.Fatal(err)
	}
	if status != "copying" || targetState != "copying" {
		t.Fatalf("failed completion was published: job=%q target=%q", status, targetState)
	}
	var oldCopyCount, proofCount int
	if err := store.write.QueryRow(t.Context(), `SELECT count(*),count(retirement_evidence_sha256)
		FROM shard_placements WHERE unit='s0-b'`).Scan(&oldCopyCount, &proofCount); err != nil {
		t.Fatal(err)
	}
	if oldCopyCount != 1 || proofCount != 0 {
		t.Fatalf("stale repair changed old copy retention or proof: copies=%d proofs=%d", oldCopyCount, proofCount)
	}
	recovered, err := store.placement(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	rebound := candidate
	rebound.Source, rebound.OldReplica, err = repairPlacementMembers(candidate, recovered.Copies)
	if err != nil || rebound.Source.Unit != "s0-b" || rebound.OldReplica.Unit != "s0-a" {
		t.Fatalf("resolve changed source: %v", err)
	}
	reboundEvidence := repairEvidenceFixture(rebound)
	reboundEvidence.Timeline = 2
	reboundRevision, err := store.reserveStandbyRepair(t.Context(), recovered, rebound, repairIntent(rebound, reboundEvidence))
	if err != nil || reboundRevision <= reservedRevision {
		t.Fatalf("reserve against the new source: revision=%d err=%v", reboundRevision, err)
	}
	for _, status := range []string{"provisioning", "copying"} {
		if err := store.setStandbyRepairStatus(t.Context(), rebound, reboundRevision, status); err != nil {
			t.Fatal(err)
		}
	}
	for i := range recovered.Copies {
		switch recovered.Copies[i].Unit {
		case "s0-a":
			recovered.Copies[i].Role = "primary"
		case "s0-b":
			recovered.Copies[i].Role = "replica"
		}
	}
	if err := store.recordPlacement(t.Context(), recovered); err != nil {
		t.Fatal(err)
	}
	if err := store.completeStandbyRepair(t.Context(), candidate, reservedRevision, evidence); !errors.Is(err, errPlacementChanged) {
		t.Fatalf("completion accepted stale evidence after the original roles were restored: %v", err)
	}
	restored, err := store.placement(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	freshRevision, err := store.reserveStandbyRepair(t.Context(), restored, candidate, intent)
	if err != nil || freshRevision != restored.Revision || freshRevision <= reservedRevision {
		t.Fatalf("fresh reservation: revision=%d prior=%d err=%v", freshRevision, reservedRevision, err)
	}
	if err := store.completeStandbyRepair(t.Context(), candidate, reservedRevision, evidence); !errors.Is(err, errPlacementChanged) {
		t.Fatalf("fresh reservation revalidated stale evidence: %v", err)
	}
	for _, status := range []string{"provisioning", "copying"} {
		if err := store.setStandbyRepairStatus(t.Context(), candidate, freshRevision, status); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.completeStandbyRepair(t.Context(), candidate, freshRevision, repairEvidenceFixture(candidate)); err != nil {
		t.Fatalf("completion with fresh evidence: %v", err)
	}
	var jobRevision, placementRevision int64
	if err := store.write.QueryRow(t.Context(), `SELECT j.placement_revision,r.revision
		FROM shard_repair_jobs j CROSS JOIN placement_revision r
		WHERE j.shard_id=$1 AND r.singleton`, candidate.ShardID).Scan(&jobRevision, &placementRevision); err != nil {
		t.Fatal(err)
	}
	if jobRevision != placementRevision {
		t.Fatalf("repair job revision=%d, actual placement revision=%d", jobRevision, placementRevision)
	}
	if err := store.write.QueryRow(t.Context(), `SELECT j.status,p.state FROM shard_repair_jobs j
		JOIN shard_placements p ON p.unit=j.unit WHERE j.shard_id=$1`, candidate.ShardID).Scan(&status, &targetState); err != nil {
		t.Fatal(err)
	}
	if status != "verified" || targetState != "ready" {
		t.Fatalf("valid completion not published: job=%q target=%q", status, targetState)
	}
	var oldState, recordedDigest string
	if err := store.write.QueryRow(t.Context(), `SELECT state,retirement_evidence_sha256
		FROM shard_placements WHERE unit='s0-b'`).Scan(&oldState, &recordedDigest); err != nil {
		t.Fatal(err)
	}
	if oldState != "retiring" || recordedDigest != digest {
		t.Fatalf("old copy did not retain its retirement proof: state=%q digest=%q", oldState, recordedDigest)
	}
	primaryLocation := auditLocation{Node: candidate.Source.Node, Unit: candidate.Source.Unit, Role: "primary"}
	targetLocation := auditLocation{Node: candidate.Target.Node, Unit: candidate.Target.Unit, Role: "replica"}
	locationAPI := auditAPI{store: store, cfg: Config{ShardURLs: [logicalShardCount]string{"routed"}}}
	copies, missing := locationAPI.auditLocationPlacement(t.Context())
	if missing {
		t.Fatal("audit could not read verified placement")
	}
	locations, unavailable := reconcileLocationPlacement(copies, []locationProbeResult{
		{location: primaryLocation, shardID: candidate.ShardID, found: true},
		{location: targetLocation, shardID: candidate.ShardID, found: true},
		{probeUnit: "copy-1", boundUnit: candidate.OldReplica.Unit, partial: true},
	})
	if len(locations) != 2 || len(unavailable) != 0 {
		t.Fatalf("verified replacement and proven loss: locations=%+v unavailable=%v", locations, unavailable)
	}
	if _, err := store.write.Exec(t.Context(), `UPDATE shard_repair_jobs SET candidate=candidate-'source_timeline'
		WHERE shard_id=$1`, candidate.ShardID); err != nil {
		t.Fatal(err)
	}

	// A replacement disk can have the same placement identity while Patroni is still initializing it.
	patroni := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"patroni":{"scope":"scope","name":"target"},"state":"stopped","role":"uninitialized"}`))
	}))
	t.Cleanup(patroni.Close)
	candidate.Target.PatroniURL = patroni.URL
	primary := repairEvidence{Unit: candidate.Source.Unit, Node: candidate.Source.Node, SystemID: evidence.SystemID,
		Timeline: evidence.PrimaryTimeline, PrimaryLSN: evidence.PrimaryLSN}
	if verified, err := store.repairStillVerified(t.Context(), candidate, primary); err != nil || verified {
		t.Fatalf("uninitialized replacement treated as verified: verified=%t err=%v", verified, err)
	}
	completed, err := store.placement(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	candidate.Source, candidate.OldReplica, err = repairPlacementMembers(candidate, completed.Copies)
	if err != nil || candidate.OldReplica.Unit != "" {
		t.Fatalf("lost target was selected as its own old replica: %v", err)
	}
	replacedRevision, err := store.reserveStandbyRepair(t.Context(), completed, candidate, repairIntent(candidate, primary))
	if err != nil || replacedRevision <= completed.Revision {
		t.Fatalf("replacement reservation: revision=%d err=%v", replacedRevision, err)
	}
	if err := store.write.QueryRow(t.Context(), `SELECT j.status,p.state FROM shard_repair_jobs j
		JOIN shard_placements p ON p.unit=j.unit WHERE j.shard_id=$1`, candidate.ShardID).Scan(&status, &targetState); err != nil {
		t.Fatal(err)
	}
	if status != "reserved" || targetState != "copying" {
		t.Fatalf("replacement was not reserved: job=%q target=%q", status, targetState)
	}
	copying, err := store.placement(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	primary.Timeline++
	newTimelineRevision, err := store.reserveStandbyRepair(t.Context(), copying, candidate, repairIntent(candidate, primary))
	if err != nil || newTimelineRevision <= replacedRevision {
		t.Fatalf("same member on a new timeline reused the old revision: revision=%d err=%v", newTimelineRevision, err)
	}
	if err := store.setStandbyRepairStatus(t.Context(), candidate, replacedRevision, "provisioning"); !errors.Is(err, errRepairUnsafeEvidence) {
		t.Fatalf("previous timeline advanced the replacement job: %v", err)
	}
}

func TestRepairResolvesReversedPlacementRoles(t *testing.T) {
	store := m2OpenStore(t, m2IsolatedConfig(t))
	copies := rotatingPlacement()
	copies[0].Role, copies[1].Role = copies[1].Role, copies[0].Role
	if err := store.recordPlacement(t.Context(), placementSnapshot{Copies: copies}); err != nil {
		t.Fatal(err)
	}
	current, err := store.placement(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	candidate := repairCandidateFixture()
	candidate.Source, candidate.OldReplica, err = repairPlacementMembers(candidate, current.Copies)
	if err != nil || candidate.Source != candidate.KnownReplica || candidate.OldReplica != candidate.Sources[0] {
		t.Fatalf("reversed roles not resolved: source=%+v old=%+v err=%v", candidate.Source, candidate.OldReplica, err)
	}
	evidence := repairEvidenceFixture(candidate)
	revision, err := store.reserveStandbyRepair(t.Context(), current, candidate, repairIntent(candidate, evidence))
	if err != nil {
		t.Fatal(err)
	}
	for _, status := range []string{"provisioning", "copying"} {
		if err := store.setStandbyRepairStatus(t.Context(), candidate, revision, status); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.write.Exec(t.Context(), `INSERT INTO shard_copy_loss_attestations
		(unit,shard_id,node,evidence_sha256) VALUES ('s0-a',0,'wrong-node',$1)`, strings.Repeat("b", 64)); err != nil {
		t.Fatal(err)
	}
	if err := store.completeStandbyRepair(t.Context(), candidate, revision, evidence); err != nil {
		t.Fatal(err)
	}
	completed, err := store.placement(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]shardPlacement{
		"s0-a": {0, "s0-a", "node-a", "replica", "retiring"},
		"s0-b": {0, "s0-b", "node-b", "primary", "ready"},
		"s0-d": {0, "s0-d", "node-d", "replica", "ready"},
	}
	for _, copy := range completed.Copies {
		if expected, ok := want[copy.Unit]; ok {
			if copy != expected {
				t.Fatalf("copy=%+v want=%+v", copy, expected)
			}
			delete(want, copy.Unit)
		}
	}
	if len(want) != 0 {
		t.Fatalf("physical copy rows missing: %+v", want)
	}
	isRetired := func() bool {
		t.Helper()
		var retired bool
		if err := store.read.QueryRow(t.Context(), `SELECT retirement_evidence_sha256 IS NOT NULL
			FROM shard_placements WHERE unit='s0-a'`).Scan(&retired); err != nil {
			t.Fatal(err)
		}
		return retired
	}
	if isRetired() {
		t.Fatal("wrong-node attestation accepted")
	}
	if _, err := store.write.Exec(t.Context(), "UPDATE shard_copy_loss_attestations SET node='node-a' WHERE unit='s0-a'"); err != nil {
		t.Fatal(err)
	}
	if err := store.confirmLateRetirements(t.Context(), candidate.ShardID, revision); err != nil || isRetired() {
		t.Fatalf("stale late retirement accepted: err=%v", err)
	}
	if err := store.confirmLateRetirements(t.Context(), candidate.ShardID, completed.Revision); err != nil || !isRetired() {
		t.Fatalf("late exact retirement not recorded: err=%v", err)
	}
}

func repairCandidateFixture() standbyRepairCandidate {
	primary := repairMember{Unit: "s0-a", Node: "node-a"}
	replica := repairMember{Unit: "s0-b", Node: "node-b"}
	return standbyRepairCandidate{
		ShardID: 0, Sources: []repairMember{primary, replica}, Source: primary, KnownReplica: replica, OldReplica: replica,
		Target:      repairMember{Unit: "s0-d", Node: "node-d", Scope: "scope", Name: "target"},
		StatefulSet: "postgres-0", PatroniConfigMap: "patroni-0", Claim: "data-s0-d",
	}
}

func repairEvidenceFixture(candidate standbyRepairCandidate) repairEvidence {
	return repairEvidence{
		ShardID: candidate.ShardID, Unit: candidate.Target.Unit, Node: candidate.Target.Node, Scope: candidate.Target.Scope,
		Name: candidate.Target.Name, SystemID: "system-0", PrimaryUnit: candidate.Source.Unit, PrimaryNode: candidate.Source.Node,
		Timeline: 1, PrimaryTimeline: 1, ReplayLSN: "0/30", PrimaryLSN: "0/20", Synchronous: true,
	}
}

func TestRepairAdmissionRejectsFullNode(t *testing.T) {
	store := m2OpenStore(t, m2IsolatedConfig(t))
	if err := store.recordPlacement(t.Context(), placementSnapshot{Copies: rotatingPlacement()}); err != nil {
		t.Fatal(err)
	}
	current, err := store.placement(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	candidate := standbyRepairCandidate{
		ShardID:     1,
		Target:      repairMember{Unit: "s1-a", Node: "node-a", Scope: "scope", Name: "target"},
		StatefulSet: "postgres-1", PatroniConfigMap: "patroni-1", Claim: "data-s1-a",
	}
	if _, err := store.reserveStandbyRepair(t.Context(), current, candidate,
		repairIntent(candidate, repairEvidence{SystemID: "system-1"})); !errors.Is(err, errInvalidPlacement) {
		t.Fatalf("third shard admitted on full node: %v", err)
	}
}
