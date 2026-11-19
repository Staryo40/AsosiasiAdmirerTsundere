package aggregator

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Measurements report per-relation database evidence, not host or filesystem totals.
type storageMeasurement struct {
	ConnectionIndex    int                `json:"connection_index"`
	Unit               string             `json:"unit,omitempty"`
	Node               string             `json:"node,omitempty"`
	Role               string             `json:"role,omitempty"`
	InRecovery         bool               `json:"in_recovery"`
	SettingsComplete   bool               `json:"settings_complete"`
	Partial            bool               `json:"partial"`
	CanonicalRevisions int64              `json:"canonical_revisions"`
	Tables             []tableMeasurement `json:"tables"`
	logicalShardID     int16
}

type tableMeasurement struct {
	Name             string `json:"name"`
	Rows             int64  `json:"row_count"`
	Bytes            int64  `json:"relation_bytes"`
	Inserted         int64  `json:"stats_inserted"`
	Updated          int64  `json:"stats_updated"`
	StatsApproximate bool   `json:"stats_approximate"`
}

type storageMeasurements struct {
	Items            []storageMeasurement `json:"items"`
	Partial          bool                 `json:"partial"`
	MissingSettings  []string             `json:"missing_settings"`
	UnavailableUnits []string             `json:"unavailable_units"`
}

type poolMeasurementResult struct {
	measurement storageMeasurement
	err         error
}

var canonicalMeasurementTables = []string{"hazard_events", "source_seismic", "source_warnings", "outbox", "handled_inputs"}

const unverifiedMeasurementInventory = "physical_pool_inventory"

func (api catalogAPI) measurements(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), api.cfg.DBReadTimeout)
	defer cancel()
	routed := api.store.shards[0] != nil
	response := api.measurePhysicalStorage(ctx)
	if !routed && len(response.Items) == 0 {
		failure := readFailure(r, "measurements_unavailable")
		failure.Message = "physical storage measurements are unavailable"
		writeReadJSON(w, http.StatusServiceUnavailable, readErrorEnvelope{Error: failure})
		return
	}
	writeReadJSON(w, http.StatusOK, response)
}

func (api catalogAPI) measurePhysicalStorage(ctx context.Context) storageMeasurements {
	routed := api.store.shards[0] != nil
	var copies []shardPlacement
	var wait sync.WaitGroup
	if routed {
		wait.Go(func() { copies = api.store.measurementPlacement(ctx) })
	}
	batch := api.store.measurePhysicalPools(ctx)
	wait.Wait()
	response := storageMeasurements{Items: make([]storageMeasurement, 0, len(batch))}
	if !routed && len(batch) != 1+len(api.cfg.Audit.CopyURLs) {
		response.addUnavailable(unverifiedMeasurementInventory)
	}
	for _, result := range batch {
		response.addPoolMeasurement(result, copies)
	}
	if routed {
		response.reconcilePlacement(copies)
	}
	return response
}

func (store *Store) measurementPlacement(ctx context.Context) []shardPlacement {
	if store.read == nil {
		return nil
	}
	placement, err := store.placement(ctx)
	if err != nil {
		return nil // The response marks unverified placement explicitly, including deadline failures.
	}
	return placement.Copies
}

func (store *Store) measurePhysicalPools(ctx context.Context) []poolMeasurementResult {
	// The same inventory order is used for the read pools and their logical identities.
	pools := store.physicalReadPools()
	primaries := physicalReadStores(store)
	var copyIdentity storeIdentity
	if store.shards[0] != nil {
		copyIdentity.role = "shard"
	}
	return measurePoolBatch(ctx, len(pools), func(ctx context.Context, index int) (storageMeasurement, error) {
		if index < len(primaries) {
			return measurePool(ctx, pools[index], index, primaries[index].storeIdentity)
		}
		return measurePool(ctx, pools[index], index, copyIdentity)
	})
}

func physicalReadStores(store *Store) []*Store {
	if store.shards[0] == nil {
		return []*Store{store}
	}
	stores := make([]*Store, 0, logicalShardCount)
	for _, shard := range store.shards {
		if shard != nil && shard.read != nil {
			stores = append(stores, shard)
		}
	}
	return stores
}

func (response *storageMeasurements) addPoolMeasurement(result poolMeasurementResult, copies []shardPlacement) {
	measured := result.measurement
	if result.err != nil {
		response.addUnavailable(result.unavailableLabel(copies))
		return
	}
	response.Items = append(response.Items, measured)
	response.Partial = response.Partial || measured.Partial
	if !measured.SettingsComplete {
		response.MissingSettings = append(response.MissingSettings, fmt.Sprintf("physical_connection_%d", measured.ConnectionIndex))
	}
}

func (result poolMeasurementResult) unavailableLabel(copies []shardPlacement) string {
	if len(copies) != 0 && result.measurement.Unit != "" {
		return result.measurement.Unit
	}
	for _, copy := range copies {
		if copy.ShardID == result.measurement.logicalShardID && copy.Role == "primary" && copy.State == "ready" {
			return copy.Unit
		}
	}
	return fmt.Sprintf("physical_connection_%d", result.measurement.ConnectionIndex)
}

func (response *storageMeasurements) addUnavailable(unit string) {
	response.Partial = true
	if !slices.Contains(response.UnavailableUnits, unit) {
		response.UnavailableUnits = append(response.UnavailableUnits, unit)
	}
}

func (response *storageMeasurements) reconcilePlacement(copies []shardPlacement) {
	// Placement validation requires a ready primary and distinct nodes per shard.
	if validatePlacement(copies) != nil || !replicatedBootstrap(copies) {
		response.addUnavailable(unverifiedMeasurementInventory)
	}
	unmatched := make(map[string]shardPlacement, len(copies))
	for _, copy := range copies {
		unmatched[copy.Unit] = copy
	}
	response.matchMeasuredUnits(unmatched)
	for _, copy := range copies {
		if _, missing := unmatched[copy.Unit]; missing {
			response.addUnavailable(copy.Unit)
		}
	}
}

func (response *storageMeasurements) matchMeasuredUnits(unmatched map[string]shardPlacement) {
	counts := make(map[string]int, len(response.Items))
	for _, measured := range response.Items {
		counts[measured.Unit]++
	}
	hasInventory := len(unmatched) != 0
	for index := range response.Items {
		measured := &response.Items[index]
		copy, expected := unmatched[measured.Unit]
		switch {
		case measured.Unit != "" && counts[measured.Unit] > 1:
			measured.Partial = true
			response.addUnavailable(measured.Unit)
		case !measured.SettingsComplete || !hasInventory:
			measured.Partial = true
		case measured.Partial || !expected || !measured.matchesPlacement(copy):
			measured.Partial = true
			response.addUnavailable(measured.Unit)
		default:
			delete(unmatched, measured.Unit)
		}
	}
}

func (measured storageMeasurement) matchesPlacement(copy shardPlacement) bool {
	return measured.logicalShardID == copy.ShardID && measured.Node == copy.Node && measured.Role == copy.Role
}

func measurePoolBatch(ctx context.Context, count int, measure func(context.Context, int) (storageMeasurement, error)) []poolMeasurementResult {
	batchResults := make([]poolMeasurementResult, count)
	var wait sync.WaitGroup
	for index := range count {
		wait.Go(func() {
			batchResults[index].measurement, batchResults[index].err = measure(ctx, index)
		})
	}
	wait.Wait()
	return batchResults
}

func measurePool(ctx context.Context, pool *pgxpool.Pool, index int, expected storeIdentity) (storageMeasurement, error) {
	measurement := storageMeasurement{ConnectionIndex: index, logicalShardID: -1, Tables: make([]tableMeasurement, 0, len(canonicalMeasurementTables))}
	if shardID, ok := expected.shard(); ok {
		measurement.logicalShardID = shardID // A failed connection can still name its expected primary.
	}
	err := pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		if err := measurement.readIdentity(ctx, tx, expected); err != nil {
			return err
		}
		return measurement.readTables(ctx, tx)
	})
	return measurement, err
}

func (measured *storageMeasurement) readIdentity(ctx context.Context, tx pgx.Tx, expected storeIdentity) error {
	var node, unit string
	if err := tx.QueryRow(ctx, `SELECT COALESCE(pg_catalog.current_setting('bnpb.node_name', true), ''), COALESCE(pg_catalog.current_setting('bnpb.unit_name', true), ''), pg_catalog.pg_is_in_recovery()`).Scan(&node, &unit, &measured.InRecovery); err != nil {
		return err
	}
	setMeasurementIdentity(measured, node, unit)
	measured.Role = "primary"
	if measured.InRecovery {
		measured.Role = "replica"
	}
	shardID, err := readMeasurementShard(ctx, tx, expected)
	if err != nil {
		return err
	}
	measured.logicalShardID = shardID
	return nil
}

func readMeasurementShard(ctx context.Context, tx pgx.Tx, expected storeIdentity) (int16, error) {
	if expected.role == "" {
		return -1, nil
	}
	var actualRole string
	var actualShard int16
	if err := tx.QueryRow(ctx, `SELECT role, COALESCE(shard_id, -1) FROM store_identity WHERE singleton`).Scan(&actualRole, &actualShard); err != nil {
		return -1, err
	}
	shardID, fixedShard := expected.shard()
	switch {
	case actualRole != expected.role,
		actualRole == "shard" && (actualShard < 0 || actualShard >= logicalShardCount),
		actualRole == "control" && actualShard != -1,
		fixedShard && actualShard != shardID:
		return -1, errors.New("physical pool does not match its logical store identity")
	}
	return actualShard, nil
}

func (measured *storageMeasurement) readTables(ctx context.Context, tx pgx.Tx) error {
	for _, name := range canonicalMeasurementTables {
		measuredTable, err := measureTable(ctx, tx, name)
		if err != nil {
			return err
		}
		if name == "outbox" {
			measured.CanonicalRevisions = measuredTable.Rows
		}
		measured.Tables = append(measured.Tables, measuredTable)
	}
	return nil
}

func measureTable(ctx context.Context, tx pgx.Tx, name string) (tableMeasurement, error) {
	measuredTable := tableMeasurement{Name: name}
	identifier := pgx.Identifier{name}.Sanitize()
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM `+identifier).Scan(&measuredTable.Rows); err != nil {
		return measuredTable, err
	}
	err := tx.QueryRow(ctx, `SELECT pg_total_relation_size(to_regclass($1)), COALESCE(n_tup_ins,0), COALESCE(n_tup_upd,0) FROM pg_stat_all_tables WHERE relid=to_regclass($1)`, name).Scan(&measuredTable.Bytes, &measuredTable.Inserted, &measuredTable.Updated)
	measuredTable.StatsApproximate = err == nil
	return measuredTable, err
}

func setMeasurementIdentity(measurement *storageMeasurement, node, unit string) {
	measurement.Node, measurement.Unit = strings.TrimSpace(node), strings.TrimSpace(unit)
	measurement.SettingsComplete = measurement.Node != "" && measurement.Unit != ""
	measurement.Partial = !measurement.SettingsComplete
}
