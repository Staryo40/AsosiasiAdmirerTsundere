package aggregator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	errRepairConfiguration  = errors.New("BNPB_SHARD_REPAIR_CANDIDATES: invalid configuration")
	errRepairUnsafeEvidence = errors.New("standby repair evidence is incomplete or unsafe")
	errRepairReplicaHealthy = errors.New("known replica is healthy")
)

const maxRepairCandidatesBytes = 64 << 10

type repairMember struct {
	Unit       string `json:"unit"`
	Node       string `json:"node"`
	DSN        string `json:"dsn"`
	PatroniURL string `json:"patroni_url"`
	Scope      string `json:"scope"`
	Name       string `json:"name"`
}

type standbyRepairCandidate struct {
	ShardID          int16          `json:"shard_id"`
	Sources          []repairMember `json:"sources"`
	Source           repairMember   `json:"-"`
	OldReplica       repairMember   `json:"-"`
	KnownReplica     repairMember   `json:"known_replica,omitempty"`
	Target           repairMember   `json:"target"`
	Namespace        string         `json:"namespace"`
	StatefulSet      string         `json:"stateful_set"`
	PatroniConfigMap string         `json:"patroni_config_map"`
	Claim            string         `json:"claim"`
}

type repairEvidence struct {
	Unit, Node, Scope, Name, SystemID string
	ShardID                           int16
	Timeline, PrimaryTimeline         uint32
	ReplayLSN, PrimaryLSN             string
	PrimaryUnit, PrimaryNode          string
	Synchronous                       bool
}

type repairTargetIdentity struct {
	ShardID          int16  `json:"shard_id"`
	Unit             string `json:"unit"`
	Node             string `json:"node"`
	Scope            string `json:"scope"`
	Name             string `json:"name"`
	StatefulSet      string `json:"stateful_set"`
	PatroniConfigMap string `json:"patroni_config_map"`
	Claim            string `json:"claim"`
}

type repairJobIntent struct {
	repairTargetIdentity
	SystemID       string          `json:"system_id"`
	SourceUnit     string          `json:"source_unit"`
	SourceNode     string          `json:"source_node"`
	OldReplicaUnit string          `json:"old_replica_unit"`
	OldReplicaNode string          `json:"old_replica_node"`
	SourceTimeline uint32          `json:"source_timeline"`
	Evidence       *repairEvidence `json:"evidence,omitempty"`
}

func (candidate standbyRepairCandidate) targetIdentity() repairTargetIdentity {
	return repairTargetIdentity{
		ShardID: candidate.ShardID, Unit: candidate.Target.Unit, Node: candidate.Target.Node,
		Scope: candidate.Target.Scope, Name: candidate.Target.Name, StatefulSet: candidate.StatefulSet,
		PatroniConfigMap: candidate.PatroniConfigMap, Claim: candidate.Claim,
	}
}

type repairExecutor interface {
	Reconcile(context.Context, standbyRepairCandidate, repairEvidence) (repairEvidence, error)
}

func parseStandbyRepairCandidates(raw string) ([]standbyRepairCandidate, error) {
	if raw == "" {
		return nil, nil
	}
	if len(raw) > maxRepairCandidatesBytes || validateJSON(json.RawMessage(raw)) != nil {
		return nil, errRepairConfiguration
	}
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.DisallowUnknownFields()
	var candidates []standbyRepairCandidate
	if err := decoder.Decode(&candidates); err != nil || len(candidates) == 0 || len(candidates) > logicalShardCount {
		return nil, errRepairConfiguration
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errRepairConfiguration
	}
	slices.SortFunc(candidates, func(a, b standbyRepairCandidate) int { return int(a.ShardID - b.ShardID) })
	seenUnits := make(map[string]bool, len(candidates))
	for i, candidate := range candidates {
		noKnownReplica := candidate.KnownReplica == (repairMember{})
		if candidate.ShardID < 0 || candidate.ShardID >= logicalShardCount || (i > 0 && candidates[i-1].ShardID == candidate.ShardID) ||
			len(candidate.Sources) == 0 || len(candidate.Sources) > logicalShardCount || !validRepairMember(candidate.Target) ||
			!validText(candidate.Namespace) || !validText(candidate.StatefulSet) || !validText(candidate.PatroniConfigMap) || !validText(candidate.Claim) ||
			seenUnits[candidate.Target.Unit] || (!noKnownReplica && (!validRepairMember(candidate.KnownReplica) ||
			candidate.KnownReplica.Unit == candidate.Target.Unit || candidate.KnownReplica.Scope != candidate.Target.Scope)) {
			return nil, errRepairConfiguration
		}
		seenSources := make(map[string]bool, len(candidate.Sources))
		for _, source := range candidate.Sources {
			if !validRepairMember(source) || source.Scope != candidate.Target.Scope || source.Name == candidate.Target.Name ||
				source.Unit == candidate.Target.Unit || source.Node == candidate.Target.Node || seenSources[source.Unit] {
				return nil, errRepairConfiguration
			}
			seenSources[source.Unit] = true
		}
		seenUnits[candidate.Target.Unit] = true
	}
	return candidates, nil
}

func validateRepairKubernetesConfig(cfg KubernetesConfig) error {
	if !strings.HasPrefix(cfg.ServerURL, "https://") || cfg.Namespace == "" || cfg.TokenFile == "" || cfg.CAFile == "" {
		return errRepairConfiguration
	}
	if _, err := readBearerToken(cfg.TokenFile, "Kubernetes service-account"); err != nil {
		return errRepairConfiguration
	}
	_, err := tlsTransport(cfg.CAFile)
	return err
}

func validRepairMember(member repairMember) bool {
	parsed, err := url.Parse(member.DSN)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") || parsed.Host == "" || parsed.User == nil {
		return false
	}
	patroni, err := url.Parse(member.PatroniURL)
	return validText(member.Unit) && validText(member.Node) && validText(member.Scope) && validText(member.Name) &&
		validText(member.DSN) && err == nil && (patroni.Scheme == "http" || patroni.Scheme == "https") && patroni.Host != "" &&
		patroni.User == nil && patroni.RawQuery == "" && patroni.Fragment == ""
}

// One invocation performs one bounded pass; the caller owns the supervised ticker/lifecycle.
func newRepairWorker(store *Store, raw string, kube KubernetesConfig) (func(context.Context) error, error) {
	candidates, err := parseStandbyRepairCandidates(raw)
	if err != nil || len(candidates) == 0 {
		return nil, err
	}
	if err := validateRepairKubernetesConfig(kube); err != nil {
		return nil, err
	}
	for _, candidate := range candidates {
		if candidate.Namespace != kube.Namespace {
			return nil, errRepairConfiguration
		}
	}
	executor, err := newStandbyRepairExecutor(kube)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context) error { return store.reconcileStandbyRepairs(ctx, candidates, executor) }, nil
}

func (store *Store) repairPool() (*pgxpool.Pool, error) {
	if store.storeIdentity.role == "control" {
		if len(store.authorityPools) == 0 {
			return nil, errors.New("repair control connection is unavailable")
		}
		return store.authorityPools[0], nil
	}
	if store.storeIdentity.role == "" && store.write != nil {
		return store.write, nil // Local M1 and isolated integration fixtures only.
	}
	return nil, errors.New("repair is only enabled on the control store")
}

func (store *Store) reconcileStandbyRepairs(ctx context.Context, candidates []standbyRepairCandidate, executor repairExecutor) error {
	if len(candidates) > logicalShardCount {
		return errRepairConfiguration
	}
	var wait sync.WaitGroup
	failures := make([]error, len(candidates))
	for i := range candidates {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			candidateCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
			defer cancel()
			failures[index] = store.reconcileStandbyRepair(candidateCtx, candidates[index], executor)
		}(i)
	}
	wait.Wait()
	return errors.Join(failures...)
}

func (store *Store) reconcileStandbyRepair(ctx context.Context, candidate standbyRepairCandidate, executor repairExecutor) error {
	current, err := store.placement(ctx)
	if err != nil {
		return err
	}
	if validatePlacement(current.Copies) != nil {
		return errInvalidPlacement
	}
	source, oldReplica, err := repairPlacementMembers(candidate, current.Copies)
	if err != nil {
		return err
	}
	candidate.Source, candidate.OldReplica = source, oldReplica
	var targetState string
	for _, copy := range current.Copies {
		if copy.Unit == candidate.Target.Unit {
			if copy.ShardID != candidate.ShardID || copy.Node != candidate.Target.Node || copy.Role != "replica" {
				return errInvalidPlacement
			}
			targetState = copy.State
			break
		}
	}
	if targetState != "copying" && targetState != "ready" {
		if err := confirmKnownReplicaUnhealthy(ctx, candidate.ShardID, oldReplica); errors.Is(err, errRepairReplicaHealthy) {
			return nil
		} else if err != nil {
			return err
		}
	}
	primaryEvidence, err := probeRepairMember(ctx, candidate.ShardID, candidate.Source, true)
	if err != nil {
		return sanitizeRepairError(err)
	}
	if targetState == "ready" {
		verified, err := store.repairStillVerified(ctx, candidate, primaryEvidence)
		if err != nil {
			return err
		}
		if verified {
			return store.confirmLateRetirements(ctx, candidate.ShardID, current.Revision)
		}
	}
	reservedRevision, err := store.reserveStandbyRepair(ctx, current, candidate, repairIntent(candidate, primaryEvidence))
	if err != nil {
		return err
	}
	if err := store.setStandbyRepairStatus(ctx, candidate, reservedRevision, "provisioning"); err != nil {
		return err
	}
	observed, err := executor.Reconcile(ctx, candidate, primaryEvidence)
	if err != nil {
		if errors.Is(err, errRepairUnsafeEvidence) {
			if blockErr := store.blockStandbyRepair(ctx, candidate, reservedRevision, errRepairUnsafeEvidence); blockErr != nil {
				return blockErr
			}
		}
		return sanitizeRepairError(err)
	}
	if err := store.setStandbyRepairStatus(ctx, candidate, reservedRevision, "copying"); err != nil {
		return err
	}
	if err := verifyRepairEvidence(primaryEvidence, observed, candidate); err != nil {
		if blockErr := store.blockStandbyRepair(ctx, candidate, reservedRevision, errRepairUnsafeEvidence); blockErr != nil {
			return blockErr
		}
		return errRepairUnsafeEvidence
	}
	return store.completeStandbyRepair(ctx, candidate, reservedRevision, observed)
}

const confirmCopyRetirementsSQL = `UPDATE shard_placements AS placement
	SET retirement_evidence_sha256 = attestation.evidence_sha256
	FROM shard_copy_loss_attestations AS attestation
	WHERE placement.shard_id=$1 AND placement.unit=attestation.unit AND placement.shard_id=attestation.shard_id
		AND placement.node=attestation.node AND placement.state='retiring'
		AND placement.retirement_evidence_sha256 IS NULL`

func (store *Store) confirmLateRetirements(ctx context.Context, shardID int16, revision int64) error {
	pool, err := store.repairPool()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, store.writeTimeout)
	defer cancel()
	// Recheck the placement after probing the ready primary and replacement.
	// Lock in placement CAS order before the update trigger advances the revision.
	_, err = pool.Exec(ctx, `WITH verified_revision AS MATERIALIZED (
		SELECT revision FROM placement_revision WHERE singleton AND revision=$2 FOR UPDATE
	) `+confirmCopyRetirementsSQL+` AND EXISTS (SELECT 1 FROM verified_revision)`, shardID, revision)
	return storeError("confirm late copy retirement", err)
}

func (store *Store) repairStillVerified(ctx context.Context, candidate standbyRepairCandidate, primary repairEvidence) (bool, error) {
	pool, err := store.repairPool()
	if err != nil {
		return false, err
	}
	queryCtx, cancel := context.WithTimeout(ctx, store.writeTimeout)
	var raw []byte
	err = pool.QueryRow(queryCtx, `SELECT candidate FROM shard_repair_jobs
		WHERE shard_id=$1 AND unit=$2 AND node=$3 AND status='verified'`,
		candidate.ShardID, candidate.Target.Unit, candidate.Target.Node).Scan(&raw)
	cancel()
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, errPlacementChanged
		}
		return false, storeError("read verified standby repair", err)
	}
	var stored repairJobIntent
	if json.Unmarshal(raw, &stored) != nil || stored.repairTargetIdentity != candidate.targetIdentity() ||
		stored.SystemID != primary.SystemID || stored.Evidence == nil {
		return false, errInvalidPlacement
	}
	prior := *stored.Evidence
	if prior.PrimaryUnit != primary.Unit || prior.PrimaryNode != primary.Node || prior.PrimaryTimeline != primary.Timeline {
		return false, nil
	}
	if !lsnAtLeast(primary.PrimaryLSN, prior.PrimaryLSN) {
		return false, errRepairUnsafeEvidence
	}
	target, err := synchronousRepairTarget(ctx, candidate, primary)
	if unavailableRepairProbe(err) {
		return false, ctx.Err()
	}
	return err == nil && lsnAtLeast(target.ReplayLSN, prior.PrimaryLSN), sanitizeRepairError(err)
}

func repairPlacementMembers(candidate standbyRepairCandidate, copies []shardPlacement) (source, oldReplica repairMember, err error) {
	for _, copy := range copies {
		if copy.ShardID != candidate.ShardID || copy.State != "ready" || copy.Unit == candidate.Target.Unit {
			continue
		}
		if copy.Role == "primary" {
			source = configuredRepairMember(copy, candidate.Sources)
			continue
		}
		if oldReplica.Unit != "" {
			return repairMember{}, repairMember{}, errInvalidPlacement
		}
		oldReplica = candidate.KnownReplica
		if oldReplica.Unit != copy.Unit || oldReplica.Node != copy.Node {
			oldReplica = configuredRepairMember(copy, candidate.Sources)
		}
		if oldReplica.Unit == "" {
			return repairMember{}, repairMember{}, errInvalidPlacement
		}
	}
	if source.Unit == "" {
		return repairMember{}, repairMember{}, errInvalidPlacement
	}
	return source, oldReplica, nil
}

func configuredRepairMember(copy shardPlacement, members []repairMember) repairMember {
	for _, member := range members {
		if member.Unit == copy.Unit && member.Node == copy.Node {
			return member
		}
	}
	return repairMember{}
}

func confirmKnownReplicaUnhealthy(ctx context.Context, shardID int16, member repairMember) error {
	if member.Unit == "" {
		return nil
	}
	_, firstErr := probeRepairMember(ctx, shardID, member, false)
	if firstErr == nil {
		return errRepairReplicaHealthy
	}
	if !unavailableRepairProbe(firstErr) {
		return errRepairUnsafeEvidence
	}
	timer := time.NewTimer(500 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}
	_, secondErr := probeRepairMember(ctx, shardID, member, false)
	if err := ctx.Err(); err != nil {
		return err
	}
	if secondErr == nil {
		return errRepairReplicaHealthy
	}
	if !unavailableRepairProbe(secondErr) {
		return errRepairUnsafeEvidence
	}
	return nil
}

func unavailableRepairProbe(err error) bool {
	if errors.Is(err, errRepairMemberUnavailable) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var networkError net.Error
	if errors.As(err, &networkError) {
		return true
	}
	var postgresError *pgconn.PgError
	return errors.As(err, &postgresError) && (postgresError.Code == "57P03" || (len(postgresError.Code) >= 2 && postgresError.Code[:2] == "08"))
}
func sanitizeRepairError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled):
		return context.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		return context.DeadlineExceeded
	case errors.Is(err, errRepairUnsafeEvidence):
		return errRepairUnsafeEvidence
	case errors.Is(err, errRepairConfiguration):
		return errRepairConfiguration
	}
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) {
		return fmt.Errorf("standby repair database operation failed (SQLSTATE %s)", postgresError.Code)
	}
	var networkError net.Error
	if errors.As(err, &networkError) {
		return errors.New("standby repair dependency unavailable")
	}
	return errors.New("standby repair dependency failed")
}

func (store *Store) reserveStandbyRepair(ctx context.Context, current placementSnapshot, candidate standbyRepairCandidate, intent repairJobIntent) (revision int64, err error) {
	pool, err := store.repairPool()
	if err != nil {
		return 0, err
	}
	ctx, cancel := context.WithTimeout(ctx, store.writeTimeout)
	defer cancel()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, storeError("begin standby repair reservation", err)
	}
	defer func() { err = errors.Join(err, rollbackTransaction(ctx, tx)) }()
	locked, err := lockedPlacement(ctx, tx)
	if err != nil {
		return 0, err
	}
	if locked.Revision != current.Revision {
		return 0, errPlacementChanged
	}
	if validatePlacement(locked.Copies) != nil {
		return 0, errInvalidPlacement
	}
	encoded, err := json.Marshal(intent)
	if err != nil {
		return 0, err
	}
	var previousRaw []byte
	var previousRevision int64
	var resume bool
	err = tx.QueryRow(ctx, `SELECT candidate, placement_revision, placement_revision=$2 AND candidate=$3::jsonb
		AND status IN ('reserved','provisioning','copying')
		FROM shard_repair_jobs WHERE shard_id=$1 FOR UPDATE`, candidate.ShardID, current.Revision, encoded).
		Scan(&previousRaw, &previousRevision, &resume)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return 0, storeError("lock standby repair", err)
	}
	var previous repairJobIntent
	if err == nil && (json.Unmarshal(previousRaw, &previous) != nil || previous.SystemID != intent.SystemID ||
		(previous.Unit == intent.Unit && previous.repairTargetIdentity != intent.repairTargetIdentity)) {
		return 0, errInvalidPlacement
	}
	if resume {
		return current.Revision, storeError("resume standby repair", tx.Commit(ctx))
	}
	revision, err = reservePlacementTarget(ctx, tx, locked, candidate, previous.Unit)
	if err != nil {
		return 0, err
	}
	// A different intent can reuse a copying placement, but never the previous job's revision.
	if revision == previousRevision {
		if err := tx.QueryRow(ctx, `UPDATE placement_revision SET revision=revision+1 WHERE singleton RETURNING revision`).Scan(&revision); err != nil {
			return 0, storeError("advance standby repair revision", err)
		}
	}
	_, err = tx.Exec(ctx, `INSERT INTO shard_repair_jobs (shard_id,unit,node,placement_revision,status,candidate)
        VALUES ($1,$2,$3,$4,'reserved',$5::jsonb) ON CONFLICT (shard_id) DO UPDATE
        SET unit=EXCLUDED.unit,node=EXCLUDED.node,placement_revision=EXCLUDED.placement_revision,
            status='reserved',candidate=EXCLUDED.candidate,last_error=NULL,updated_at=now()`,
		candidate.ShardID, candidate.Target.Unit, candidate.Target.Node, revision, encoded)
	if err != nil {
		return 0, storeError("reserve standby repair", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, storeError("commit standby repair reservation", err)
	}
	return revision, nil
}

func reservePlacementTarget(ctx context.Context, tx pgx.Tx, current placementSnapshot, candidate standbyRepairCandidate, previousUnit string) (int64, error) {
	target := shardPlacement{ShardID: candidate.ShardID, Unit: candidate.Target.Unit, Node: candidate.Target.Node, Role: "replica", State: "copying"}
	proposed := placementSnapshot{Revision: current.Revision, Copies: slices.Clone(current.Copies)}
	found := false
	for i, copy := range proposed.Copies {
		switch copy.Unit {
		case target.Unit:
			if copy.ShardID != target.ShardID || copy.Node != target.Node || copy.Role != target.Role {
				return 0, errInvalidPlacement
			}
			proposed.Copies[i] = target
			found = true
		case previousUnit:
			if copy.State == "copying" {
				proposed.Copies[i].State = "retiring"
			}
		}
	}
	if !found {
		proposed.Copies = append(proposed.Copies, target)
	}
	if slices.Equal(current.Copies, proposed.Copies) {
		return current.Revision, nil
	}
	if validatePlacement(proposed.Copies) != nil {
		return 0, errInvalidPlacement
	}
	if err := recordPlacementChange(ctx, tx, proposed, current); err != nil {
		return 0, err
	}
	var revision int64
	if err := tx.QueryRow(ctx, "SELECT revision FROM placement_revision WHERE singleton").Scan(&revision); err != nil {
		return 0, storeError("read reserved placement revision", err)
	}
	return revision, nil
}

func (store *Store) setStandbyRepairStatus(ctx context.Context, candidate standbyRepairCandidate, revision int64, status string) error {
	var allowed string
	switch status {
	case "provisioning":
		allowed = "('reserved','provisioning','copying')"
	case "copying":
		allowed = "('provisioning','copying')"
	default:
		return errRepairUnsafeEvidence
	}
	pool, err := store.repairPool()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, store.writeTimeout)
	defer cancel()
	tag, err := pool.Exec(ctx, `UPDATE shard_repair_jobs SET status=$3,updated_at=now()
        WHERE shard_id=$1 AND unit=$2 AND placement_revision=$4
        AND $4=(SELECT revision FROM placement_revision WHERE singleton) AND status IN `+allowed,
		candidate.ShardID, candidate.Target.Unit, status, revision)
	if err != nil {
		return storeError("advance standby repair", err)
	}
	if tag.RowsAffected() != 1 {
		return errRepairUnsafeEvidence
	}
	return nil
}

func (store *Store) blockStandbyRepair(ctx context.Context, candidate standbyRepairCandidate, revision int64, cause error) error {
	pool, err := store.repairPool()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, store.writeTimeout)
	defer cancel()
	tag, err := pool.Exec(ctx, `UPDATE shard_repair_jobs SET status='blocked',last_error=$3,updated_at=now()
        WHERE shard_id=$1 AND unit=$2 AND placement_revision=$4 AND status IN ('reserved','provisioning','copying')
        AND $4=(SELECT revision FROM placement_revision WHERE singleton)`,
		candidate.ShardID, candidate.Target.Unit, cause.Error(), revision)
	if err != nil {
		return storeError("block standby repair", err)
	}
	if tag.RowsAffected() != 1 {
		return errRepairUnsafeEvidence
	}
	return nil
}

func (store *Store) completeStandbyRepair(ctx context.Context, candidate standbyRepairCandidate, reservedRevision int64, evidence repairEvidence) (err error) {
	pool, err := store.repairPool()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, store.writeTimeout)
	defer cancel()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return storeError("begin standby repair completion", err)
	}
	defer func() { err = errors.Join(err, rollbackTransaction(ctx, tx)) }()
	current, err := lockedPlacement(ctx, tx)
	if err != nil {
		return err
	}
	if current.Revision != reservedRevision {
		return errPlacementChanged
	}
	proposed := placementSnapshot{Revision: current.Revision, Copies: slices.Clone(current.Copies)}
	targetFound, sourceFound := false, false
	knownFound := candidate.OldReplica.Unit == ""
	for i, copy := range proposed.Copies {
		if copy.ShardID == candidate.ShardID && copy.Role == "primary" && copy.State == "ready" &&
			copy.Unit == candidate.Source.Unit && copy.Node == candidate.Source.Node &&
			copy.Unit == evidence.PrimaryUnit && copy.Node == evidence.PrimaryNode {
			sourceFound = true
		}
		if copy.Unit == candidate.Target.Unit {
			if copy.ShardID != candidate.ShardID || copy.Node != candidate.Target.Node || copy.State != "copying" || copy.Role != "replica" {
				return errInvalidPlacement
			}
			proposed.Copies[i].State = "ready"
			targetFound = true
		}
		if candidate.OldReplica.Unit != "" && copy.Unit == candidate.OldReplica.Unit {
			if copy.ShardID != candidate.ShardID || copy.Node != candidate.OldReplica.Node || copy.Role != "replica" || copy.State != "ready" {
				return errInvalidPlacement
			}
			proposed.Copies[i].State = "retiring"
			knownFound = true
		}
	}
	if !targetFound || !sourceFound || !knownFound || validatePlacement(proposed.Copies) != nil {
		return errInvalidPlacement
	}
	if err := recordPlacementChange(ctx, tx, proposed, current); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, confirmCopyRetirementsSQL, candidate.ShardID); err != nil {
		return storeError("confirm copy retirement", err)
	}
	var revision int64
	if err := tx.QueryRow(ctx, "SELECT revision FROM placement_revision WHERE singleton").Scan(&revision); err != nil {
		return storeError("read completed placement revision", err)
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		return err
	}
	intent, err := json.Marshal(repairIntent(candidate, evidence))
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE shard_repair_jobs SET status='verified',placement_revision=$3,
        candidate=candidate || jsonb_build_object('evidence',$4::jsonb),updated_at=now(),last_error=NULL
        WHERE shard_id=$1 AND unit=$2 AND status='copying' AND placement_revision=$5 AND candidate=$6::jsonb`,
		candidate.ShardID, candidate.Target.Unit, revision, encoded, reservedRevision, intent)
	if err != nil {
		return storeError("complete standby repair", err)
	}
	if tag.RowsAffected() != 1 {
		return errRepairUnsafeEvidence
	}
	return storeError("commit standby repair", tx.Commit(ctx))
}

func repairIntent(candidate standbyRepairCandidate, evidence repairEvidence) repairJobIntent {
	return repairJobIntent{
		repairTargetIdentity: candidate.targetIdentity(), SystemID: evidence.SystemID, SourceTimeline: evidence.Timeline,
		SourceUnit: candidate.Source.Unit, SourceNode: candidate.Source.Node,
		OldReplicaUnit: candidate.OldReplica.Unit, OldReplicaNode: candidate.OldReplica.Node,
	}
}

func verifyRepairEvidence(primary, replacement repairEvidence, candidate standbyRepairCandidate) error {
	if replacement.ShardID != candidate.ShardID || replacement.Unit != candidate.Target.Unit || replacement.Node != candidate.Target.Node ||
		replacement.Scope != candidate.Target.Scope || replacement.Name != candidate.Target.Name || replacement.SystemID == "" || replacement.SystemID != primary.SystemID ||
		replacement.PrimaryUnit != primary.Unit || replacement.PrimaryNode != primary.Node || replacement.PrimaryTimeline != primary.Timeline ||
		replacement.Timeline != primary.Timeline || !replacement.Synchronous ||
		!lsnAtLeast(replacement.PrimaryLSN, primary.PrimaryLSN) || !lsnAtLeast(replacement.ReplayLSN, replacement.PrimaryLSN) {
		return errRepairUnsafeEvidence
	}
	return nil
}

func lsnAtLeast(replay, boundary string) bool {
	left, err := parseLSN(replay)
	if err != nil {
		return false
	}
	right, err := parseLSN(boundary)
	return err == nil && left >= right
}

func parseLSN(position string) (uint64, error) {

	highPart, lowPart, found := strings.Cut(position, "/")
	if !found || len(highPart) > 8 || len(lowPart) > 8 {
		return 0, errRepairUnsafeEvidence
	}
	high, err := strconv.ParseUint(highPart, 16, 32)
	if err != nil {
		return 0, err
	}
	low, err := strconv.ParseUint(lowPart, 16, 32)
	if err != nil {
		return 0, err
	}
	return high<<32 | low, nil
}
