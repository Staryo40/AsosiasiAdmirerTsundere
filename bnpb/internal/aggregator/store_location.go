package aggregator

import (
	"cmp"
	"context"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const maxConcurrentAuditLocationProbes = 8
const noAuditShard int16 = -1

type auditLocation struct {
	Node string `json:"node"`
	Unit string `json:"unit"`
	Role string `json:"role"`
}

type locationProbeJob struct {
	pool    *pgxpool.Pool
	owner   *Store
	routed  bool
	shardID int16
}

type locationProbeResult struct {
	location auditLocation
	shardID  int16
	found    bool
	partial  bool
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
		writeAuditLocation(w, hazardID, []auditLocation{}, true)
		return
	}
	budget := api.cfg.DBReadTimeout
	if budget <= 0 {
		budget = 200 * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(request.Context(), budget)
	defer cancel()
	observed := observeAuditLocations(ctx, hazardID, jobs)
	// Missing placement cannot certify completeness, but doesn't erase a live observation.
	expected, placementMissing := api.auditLocationPlacement(ctx)
	locations, probesPartial := reconcileLocationPlacement(expected, observed)
	partial := placementMissing || probesPartial
	if len(locations) == 0 && !partial {
		auditWriteError(w, http.StatusNotFound, "not_found", "hazard was not found")
		return
	}
	writeAuditLocation(w, hazardID, locations, partial)
}

func writeAuditLocation(w http.ResponseWriter, hazardID string, locations []auditLocation, partial bool) {
	auditWriteJSON(w, http.StatusOK, struct {
		HazardID  string          `json:"hazard_id"`
		Locations []auditLocation `json:"locations"`
		Partial   bool            `json:"partial"`
	}{hazardID, locations, partial})
}

func (api auditAPI) routedLocationAudit() bool {
	return api.cfg.ShardURLs != [logicalShardCount]string{}
}

func (api auditAPI) auditLocationPlacement(ctx context.Context) ([]shardPlacement, bool) {
	if !api.routedLocationAudit() {
		return nil, false
	}
	if api.store.read == nil {
		return nil, true
	}
	placement, err := api.store.placement(ctx)
	if err != nil {
		return nil, true
	}
	return placement.Copies, len(placement.Copies) == 0
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
			job := locationProbeJob{owner: shard, routed: true, shardID: int16(shardID)}
			if shard != nil {
				job.pool = shard.read
			}
			jobs = append(jobs, job)
		}
	} else {
		jobs = append(jobs, locationProbeJob{pool: store.read, owner: store, shardID: noAuditShard})
	}
	for _, pool := range store.physicalCopies {
		jobs = append(jobs, locationProbeJob{pool: pool, routed: routed, shardID: noAuditShard})
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

func reconcileLocationPlacement(expected []shardPlacement, observed []locationProbeResult) ([]auditLocation, bool) {
	byUnit, partial := verifiedLocationProbes(expected, observed)
	locations := make([]auditLocation, 0, len(byUnit))
	targetShard := noAuditShard
	for _, probe := range byUnit {
		if !probe.found {
			continue
		}
		if targetShard != noAuditShard && probe.shardID != targetShard {
			// The same canonical ID cannot establish a location on two logical shards.
			return locations[:0], true
		}
		targetShard = probe.shardID
		locations = append(locations, probe.location)
	}
	partial = partial || !locationInventoryCovered(expected, byUnit, targetShard)
	slices.SortFunc(locations, func(a, b auditLocation) int { return cmp.Compare(a.Unit, b.Unit) })
	return locations, partial
}

func verifiedLocationProbes(expected []shardPlacement, observed []locationProbeResult) (map[string]locationProbeResult, bool) {
	placements := make(map[string]shardPlacement, len(expected))
	for _, copy := range expected {
		placements[copy.Unit] = copy
	}
	byUnit := make(map[string]locationProbeResult, len(observed))
	partial := false
	for _, probe := range observed {
		unit := probe.location.Unit
		if probe.partial || (len(placements) != 0 && !probe.matchesPlacement(placements[unit])) {
			partial = true
			continue
		}
		previous, seen := byUnit[unit]
		switch {
		case !seen:
			byUnit[unit] = probe
		case previous.partial || previous.location != probe.location || previous.shardID != probe.shardID:
			byUnit[unit] = locationProbeResult{partial: true}
			partial = true
		case previous.found != probe.found:
			probe.found = true
			byUnit[unit] = probe
			partial = true
		}
	}
	return byUnit, partial
}

func (probe locationProbeResult) matchesPlacement(copy shardPlacement) bool {
	return probe.shardID == copy.ShardID && probe.location == (auditLocation{Node: copy.Node, Unit: copy.Unit, Role: copy.Role})
}

func locationInventoryCovered(expected []shardPlacement, byUnit map[string]locationProbeResult, targetShard int16) bool {
	for _, copy := range expected {
		if targetShard != noAuditShard && copy.ShardID != targetShard {
			continue
		}
		probe, seen := byUnit[copy.Unit]
		if !seen || probe.partial || (targetShard != noAuditShard && !probe.found) {
			return false
		}
	}
	return true
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
		return locationProbeResult{partial: true}
	}
	probe := locationProbeResult{shardID: noAuditShard}
	var replica bool
	var err error
	if job.routed {
		err = job.pool.QueryRow(ctx, routedAuditLocationProbe, hazardID, job.shardID, int16(logicalShardCount)).
			Scan(&probe.location.Node, &probe.location.Unit, &replica, &probe.shardID, &probe.found)
	} else {
		err = job.pool.QueryRow(ctx, standaloneAuditLocationProbe, hazardID).
			Scan(&probe.location.Node, &probe.location.Unit, &replica, &probe.found)
	}
	if err != nil || !validText(probe.location.Node) || !validText(probe.location.Unit) ||
		(job.routed && job.owner != nil && replica) {
		return locationProbeResult{partial: true}
	}
	probe.location.Role = "primary"
	if replica {
		probe.location.Role = "replica"
	}
	return probe
}
