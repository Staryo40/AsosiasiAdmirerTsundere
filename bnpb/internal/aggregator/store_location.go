package aggregator

import (
	"cmp"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const maxConcurrentAuditLocationProbes = 8
const noAuditShard int16 = -1
const auditCopyApplicationPrefix = "bnpb-audit-copy:"

type auditLocation struct {
	Node      string `json:"node"`
	Unit      string `json:"unit"`
	Role      string `json:"role"`
	RoleDrift bool   `json:"role_drift,omitempty"`
}

type locationPlacementCopy struct {
	shardPlacement
	retired bool
}

type locationProbeJob struct {
	pool    *pgxpool.Pool
	owner   *Store
	unit    string
	routed  bool
	shardID int16
}

type locationProbeResult struct {
	location            auditLocation
	probeUnit           string
	boundUnit           string
	shardID             int16
	found               bool
	partial             bool
	routeReportsReplica bool
}

func (api auditAPI) location(w http.ResponseWriter, request *http.Request) {
	query, err := url.ParseQuery(request.URL.RawQuery)
	if err != nil || len(query) != 1 || len(query["hazard_id"]) != 1 || query.Get("hazard_id") == "" {
		auditWriteError(w, http.StatusBadRequest, "invalid_query", "hazard_id is required")
		return
	}
	hazardID := query.Get("hazard_id")
	jobs := api.locationProbeJobs()
	if len(jobs) == 0 {
		writeAuditLocation(w, hazardID, []auditLocation{}, []string{"local"})
		return
	}
	budget := api.cfg.DBReadTimeout
	if budget <= 0 {
		budget = 200 * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(request.Context(), budget)
	defer cancel()
	var copies []locationPlacementCopy
	var placementMissing bool
	var placementRead sync.WaitGroup
	placementRead.Go(func() { copies, placementMissing = api.auditLocationPlacement(ctx) })
	observed := observeAuditLocations(ctx, hazardID, jobs)
	placementRead.Wait()
	// Missing placement cannot certify completeness, but doesn't erase a live observation.
	locations, unavailable := reconcileLocationPlacement(copies, observed)
	if placementMissing {
		unavailable = append(unavailable, "control")
	}
	if len(locations) == 0 && len(unavailable) == 0 {
		auditWriteError(w, http.StatusNotFound, "not_found", "hazard was not found")
		return
	}
	writeAuditLocation(w, hazardID, locations, unavailable)
}

func writeAuditLocation(w http.ResponseWriter, hazardID string, locations []auditLocation, unavailable []string) {
	slices.Sort(unavailable)
	auditWriteJSON(w, http.StatusOK, struct {
		HazardID         string          `json:"hazard_id"`
		Locations        []auditLocation `json:"locations"`
		Partial          bool            `json:"partial"`
		UnavailableUnits []string        `json:"unavailable_units,omitempty"`
	}{hazardID, locations, len(unavailable) > 0, slices.Compact(unavailable)})
}

func (api auditAPI) routedLocationAudit() bool {
	return api.cfg.ShardURLs != [logicalShardCount]string{}
}

func (api auditAPI) auditLocationPlacement(ctx context.Context) ([]locationPlacementCopy, bool) {
	if !api.routedLocationAudit() {
		return nil, false
	}
	if api.store.read == nil {
		return nil, true
	}
	rows, err := api.store.read.Query(ctx, `SELECT shard_id, unit, node, role, state,
		retirement_evidence_sha256 IS NOT NULL FROM shard_placements`)
	if err != nil {
		return nil, true
	}
	defer rows.Close()
	var copies []locationPlacementCopy
	for rows.Next() {
		var copy locationPlacementCopy
		if err := rows.Scan(&copy.ShardID, &copy.Unit, &copy.Node, &copy.Role, &copy.State, &copy.retired); err != nil {
			return nil, true
		}
		copies = append(copies, copy)
	}
	if rows.Err() != nil {
		return nil, true
	}
	return copies, len(copies) == 0
}

func (api auditAPI) locationProbeJobs() []locationProbeJob {
	store := api.store
	if store == nil {
		return nil
	}
	routed := api.routedLocationAudit()
	jobs := make([]locationProbeJob, 0, len(store.shards)+len(store.physicalCopies))
	if routed {
		for shardID, shard := range store.shards {
			job := locationProbeJob{owner: shard, unit: fmt.Sprintf("shard-%d", shardID), routed: true, shardID: int16(shardID)}
			if shard != nil {
				job.pool = shard.read
			}
			jobs = append(jobs, job)
		}
	} else {
		jobs = append(jobs, locationProbeJob{pool: store.read, owner: store, unit: "local", shardID: noAuditShard})
	}
	for index, pool := range store.physicalCopies {
		jobs = append(jobs, locationProbeJob{pool: pool, unit: fmt.Sprintf("copy-%d", index+1), routed: routed, shardID: noAuditShard})
	}
	return jobs
}

func observeAuditLocations(ctx context.Context, hazardID string, queued []locationProbeJob) []locationProbeResult {
	observed := make([]locationProbeResult, len(queued))
	jobs := make(chan int)
	var workers sync.WaitGroup
	for range min(len(queued), maxConcurrentAuditLocationProbes) {
		workers.Go(func() {
			for index := range jobs {
				observed[index] = queued[index].probe(ctx, hazardID)
			}
		})
	}
	for index := range queued {
		jobs <- index
	}
	close(jobs)
	workers.Wait()
	return observed
}

func reconcileLocationPlacement(expected []locationPlacementCopy, observed []locationProbeResult) ([]auditLocation, []string) {
	byUnit, unavailable := verifiedLocationProbes(expected, observed)
	locations := make([]auditLocation, 0, len(byUnit))
	targetShard := noAuditShard
	conflictingShards := false
	for _, probe := range byUnit {
		if !probe.found {
			continue
		}
		if targetShard != noAuditShard && probe.shardID != targetShard {
			// The same canonical ID cannot establish a location on two logical shards.
			conflictingShards = true
		}
		targetShard = probe.shardID
		locations = append(locations, probe.location)
	}
	if conflictingShards {
		for _, location := range locations {
			unavailable = append(unavailable, location.Unit)
		}
		return locations[:0], unavailable
	}
	for _, copy := range expected {
		if targetShard != noAuditShard && copy.ShardID != targetShard {
			continue
		}
		probe, seen := byUnit[copy.Unit]
		if copy.retired {
			if seen {
				unavailable = append(unavailable, copy.Unit)
			}
			continue
		}
		if !seen || probe.partial || (targetShard != noAuditShard && !probe.found) {
			unavailable = append(unavailable, copy.Unit)
		}
	}
	slices.SortFunc(locations, func(a, b auditLocation) int { return cmp.Compare(a.Unit, b.Unit) })
	return locations, unavailable
}

func verifiedLocationProbes(expected []locationPlacementCopy, observed []locationProbeResult) (map[string]locationProbeResult, []string) {
	placements := make(map[string]locationPlacementCopy, len(expected))
	for _, copy := range expected {
		placements[copy.Unit] = copy
	}
	byUnit := make(map[string]locationProbeResult, len(observed))
	var unavailable []string
	for _, probe := range observed {
		unit := probe.location.Unit
		if probe.partial {
			if placement, bound := placements[probe.boundUnit]; bound {
				if placement.retired {
					continue
				}
				probe.probeUnit = placement.Unit
			}
			unavailable = append(unavailable, probe.probeUnit)
			continue
		}
		placement, known := placements[unit]
		if len(placements) != 0 && (!known || !probe.matchesPlacement(placement.shardPlacement)) {
			unavailable = append(unavailable, probe.probeUnit)
			continue
		}
		if probe.routeReportsReplica {
			unavailable = append(unavailable, probe.probeUnit)
		}
		if known && probe.location.Role != placement.Role {
			probe.location.RoleDrift = true
			unavailable = append(unavailable, unit)
		}
		if previous, seen := byUnit[unit]; seen {
			var inconsistent bool
			probe, inconsistent = mergeLocationProbes(previous, probe)
			if inconsistent {
				unavailable = append(unavailable, unit)
			}
		}
		byUnit[unit] = probe
	}
	return byUnit, unavailable
}

func (probe locationProbeResult) matchesPlacement(copy shardPlacement) bool {
	return probe.shardID == copy.ShardID && probe.location.Unit == copy.Unit && probe.location.Node == copy.Node
}

func mergeLocationProbes(previous, current locationProbeResult) (locationProbeResult, bool) {
	if previous.partial || previous.location.Node != current.location.Node || previous.shardID != current.shardID {
		return locationProbeResult{partial: true}, true
	}
	roleConflict := previous.location.Role != current.location.Role
	drift := previous.location.RoleDrift || current.location.RoleDrift || roleConflict
	inconsistent := roleConflict || previous.found != current.found
	// Prefer a positive read; between equal observations prefer the promoted role.
	if previous.found && !current.found || previous.found == current.found && previous.location.Role == "primary" {
		current = previous
	}
	current.location.RoleDrift = drift
	return current, inconsistent
}

const auditLocationServerIdentity = `SELECT COALESCE(pg_catalog.current_setting('bnpb.node_name', true), ''),
	COALESCE(pg_catalog.current_setting('bnpb.unit_name', true), ''), pg_catalog.pg_is_in_recovery(), `

// Read the backend identity and presence in one statement: the route can
// change after startup and a pool need not reuse the same connection.
const routedAuditLocationProbe = auditLocationServerIdentity + `
	backend.shard_id, hazard.hazard_id IS NOT NULL
	FROM store_identity backend
	LEFT JOIN hazard_events hazard ON hazard.hazard_id = $1
	WHERE backend.singleton AND backend.role = 'shard'
		AND backend.shard_id >= 0 AND backend.shard_id < $3::smallint
		AND ($2::smallint = -1 OR backend.shard_id = $2)
		AND (hazard.hazard_id IS NULL OR hazard.logical_shard_id = backend.shard_id)`

const standaloneAuditLocationProbe = auditLocationServerIdentity + `EXISTS (SELECT 1 FROM hazard_events WHERE hazard_id = $1)`

func (job locationProbeJob) probe(ctx context.Context, hazardID string) locationProbeResult {
	if job.pool == nil || (job.owner != nil && job.owner.verifyStartupResource(ctx) != nil) {
		return locationProbeResult{partial: true, probeUnit: job.unit}
	}
	probe := locationProbeResult{shardID: noAuditShard, probeUnit: job.unit}
	var replica bool
	var err error
	if job.routed {
		err = job.pool.QueryRow(ctx, routedAuditLocationProbe, hazardID, job.shardID, int16(logicalShardCount)).
			Scan(&probe.location.Node, &probe.location.Unit, &replica, &probe.shardID, &probe.found)
	} else {
		err = job.pool.QueryRow(ctx, standaloneAuditLocationProbe, hazardID).
			Scan(&probe.location.Node, &probe.location.Unit, &replica, &probe.found)
	}
	if err != nil {
		failed := locationProbeResult{partial: true, probeUnit: job.unit}
		if job.routed && job.owner == nil && unavailableRepairProbe(err) {
			// Only a declared direct-member binding can explain a failed slot.
			application := job.pool.Config().ConnConfig.RuntimeParams["application_name"]
			if unit, bound := strings.CutPrefix(application, auditCopyApplicationPrefix); bound {
				failed.boundUnit = unit
			}
		}
		return failed
	}
	if !validText(probe.location.Node) || !validText(probe.location.Unit) {
		return locationProbeResult{partial: true, probeUnit: job.unit}
	}
	probe.location.Role = "primary"
	if replica {
		probe.location.Role = "replica"
		probe.routeReportsReplica = job.routed && job.owner != nil
	}
	return probe
}
