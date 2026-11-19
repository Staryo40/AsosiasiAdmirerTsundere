package aggregator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	errHazardNotFound    = errors.New("hazard not found")
	errHazardUnavailable = errors.New("hazard unavailable")
)

type hazardList struct {
	Items               []HazardEvent          `json:"items"`
	NextCursor          *string                `json:"next_cursor"`
	Sources             map[Source]sourceState `json:"sources"`
	AsOf                time.Time              `json:"as_of"`
	Partial             bool                   `json:"partial"`
	UnavailableUnits    []string               `json:"unavailable_units"`
	MetadataUnavailable bool                   `json:"-"`
}

type hazardDetail struct {
	Item             HazardEvent            `json:"item"`
	Sources          map[Source]sourceState `json:"sources"`
	AsOf             time.Time              `json:"as_of"`
	Partial          bool                   `json:"partial"`
	UnavailableUnits []string               `json:"unavailable_units"`
}

func (store *Store) snapshot(ctx context.Context, read func(context.Context, pgx.Tx) error) error {
	ctx, cancel := context.WithTimeout(ctx, store.readTimeout)
	defer cancel()
	return store.primarySnapshot(ctx, read)
}

// Check the backend on every snapshot: startup verification can predate a
// route change. PostgreSQL recovery state does not establish Patroni leadership.
func (store *Store) primarySnapshot(ctx context.Context, read func(context.Context, pgx.Tx) error) error {
	return store.snapshotWithContext(ctx, func(ctx context.Context, tx pgx.Tx) error {
		identity := store.storeIdentity
		if identity.role != "" {
			var matches bool
			err := tx.QueryRow(ctx, `SELECT NOT pg_catalog.pg_is_in_recovery()
				AND role = $1 AND shard_id IS NOT DISTINCT FROM $2::smallint
				FROM store_identity WHERE singleton`, identity.role, identity.shardID).Scan(&matches)
			if err != nil {
				return storeError("verify logical read target", err)
			}
			if !matches {
				return errors.New("logical read target is in recovery or has a different store identity")
			}
		}
		return read(ctx, tx)
	})
}

// snapshotWithContext uses only the caller's deadline. Logical data snapshots
// and control metadata reads share the request's one cfg.DBReadTimeout context.
func (store *Store) snapshotWithContext(ctx context.Context, read func(context.Context, pgx.Tx) error) (err error) {
	if err := store.verifyStartupResource(ctx); err != nil {
		return storeError("verify startup resource", err)
	}
	tx, err := store.read.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return storeError("begin snapshot", err)
	}
	defer func() {
		if cleanupErr := rollbackTransaction(ctx, tx); cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
		}
	}()
	if err := read(ctx, tx); err != nil {
		return err
	}
	return storeError("commit snapshot", tx.Commit(ctx))
}

type shardHazards struct {
	Rows   []HazardEvent
	Cached map[Source]bool
}

func (store *Store) listHazards(requestCtx context.Context, query hazardQuery, cfg Config) (hazardList, error) {
	ctx, cancel := context.WithTimeout(requestCtx, cfg.DBReadTimeout)
	defer cancel()
	page := hazardList{Items: make([]HazardEvent, 0, query.limit+1), UnavailableUnits: []string{}}
	stores := store.logicalReadStores()
	selected := query.filters.selectedSources()
	results := fanoutLogicalReads(ctx, stores, func(ctx context.Context, unit logicalReadStore) (shardHazards, error) {
		value := shardHazards{Rows: make([]HazardEvent, 0, query.limit+1), Cached: make(map[Source]bool, len(selected))}
		err := unit.Store.primarySnapshot(ctx, func(ctx context.Context, tx pgx.Tx) error {
			var err error
			value.Rows, err = readHazardPage(ctx, tx, query)
			if err != nil {
				return err
			}
			for _, source := range selected {
				var cached bool
				if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM hazard_events WHERE source = $1)", string(source)).Scan(&cached); err != nil {
					return storeError("read shard cache", err)
				}
				value.Cached[source] = cached
			}
			return nil
		})
		return value, err
	})
	page.UnavailableUnits = unavailableReadUnits(results)
	page.Partial = len(page.UnavailableUnits) != 0

	cached := make(map[Source]bool, len(selected))
	for _, result := range results {
		if result.Err != nil {
			continue
		}
		for source, exists := range result.Value.Cached {
			cached[source] = cached[source] || exists
		}
	}
	// Each shard batch is already in canonical order; advance the best of three
	// heads rather than concatenating and sorting up to three full batches.
	positions := make([]int, len(results))
	for len(page.Items) < query.limit+1 {
		best := -1
		for index, result := range results {
			if result.Err != nil || positions[index] >= len(result.Value.Rows) {
				continue
			}
			if best < 0 || hazardBefore(result.Value.Rows[positions[index]], results[best].Value.Rows[positions[best]]) {
				best = index
			}
		}
		if best < 0 {
			break
		}
		page.Items = append(page.Items, results[best].Value.Rows[positions[best]])
		positions[best]++
	}
	more := len(page.Items) > query.limit
	if more {
		page.Items = page.Items[:query.limit]
	}

	var metadataErr error
	page.Sources, page.AsOf, metadataErr = store.sourceMetadata(ctx, selected, cfg)
	page.MetadataUnavailable = metadataErr != nil
	for source, exists := range cached {
		state := page.Sources[source]
		state.HasCachedData = state.HasCachedData || exists
		if metadataErr == nil && exists && state.Status == "unavailable" {
			state.Status = "stale"
		}
		page.Sources[source] = state
	}

	if page.Partial {
		if query.cursor != nil {
			page.NextCursor = &query.cursorToken
		} else {
			token, err := startHazardCursor(query.filters)
			if err != nil {
				return page, storeError("encode start cursor", err)
			}
			page.NextCursor = &token
		}
	} else if more {
		token, err := nextHazardCursor(page.Items[len(page.Items)-1], query.filters)
		if err != nil {
			return page, storeError("encode hazard cursor", err)
		}
		page.NextCursor = &token
	}
	return page, nil
}

func hazardBefore(left, right HazardEvent) bool {
	if left.OccurredAt.Equal(right.OccurredAt) {
		return left.HazardID > right.HazardID
	}
	return left.OccurredAt.After(right.OccurredAt)
}

func (store *Store) hazardDetail(requestCtx context.Context, hazardID string, cfg Config) (hazardDetail, error) {
	ctx, cancel := context.WithTimeout(requestCtx, cfg.DBReadTimeout)
	defer cancel()
	stores := store.logicalReadStores()
	type located struct {
		Hazard HazardEvent
		Found  bool
	}
	results := fanoutLogicalReads(ctx, stores, func(ctx context.Context, unit logicalReadStore) (located, error) {
		var value located
		err := unit.Store.primarySnapshot(ctx, func(ctx context.Context, tx pgx.Tx) error {
			hazard, err := scanHazard(tx.QueryRow(ctx, "SELECT "+hazardColumns+" FROM hazard_events WHERE hazard_id = $1", hazardID))
			if errors.Is(err, pgx.ErrNoRows) {
				return nil
			}
			if err != nil {
				return storeError("read hazard", err)
			}
			value = located{Hazard: hazard, Found: true}
			return nil
		})
		return value, err
	})
	units := unavailableReadUnits(results)
	for _, result := range results {
		if result.Err != nil || !result.Value.Found {
			continue
		}
		detail := hazardDetail{Item: result.Value.Hazard, Partial: len(units) != 0, UnavailableUnits: units}
		detail.Sources, detail.AsOf, _ = store.sourceMetadata(ctx, []Source{detail.Item.Source}, cfg)
		return detail, nil
	}
	if len(units) != 0 {
		return hazardDetail{Partial: true, UnavailableUnits: units}, errHazardUnavailable
	}
	return hazardDetail{}, errHazardNotFound
}

func (store *Store) sourceMetadata(ctx context.Context, selected []Source, cfg Config) (map[Source]sourceState, time.Time, error) {
	var states map[Source]sourceState
	var asOf time.Time
	err := store.primarySnapshot(ctx, func(ctx context.Context, tx pgx.Tx) error {
		var readErr error
		states, asOf, readErr = readFreshness(ctx, tx, selected, cfg)
		return readErr
	})
	if err != nil {
		// AI-assisted fix: even a failed control snapshot must leave reachable shard rows servable.
		states, asOf = unavailableSourceMetadata(selected)
	}
	return states, asOf, err
}

func unavailableSourceMetadata(sources []Source) (map[Source]sourceState, time.Time) {
	states := make(map[Source]sourceState, len(sources))
	for _, source := range sources {
		category := "metadata_unavailable"
		states[source] = sourceState{pollObservation: pollObservation{Status: "unavailable", ErrorCategory: &category}, Endpoints: map[string]pollObservation{}}
	}
	return states, time.Now().UTC()
}

const hazardColumns = `hazard_id, source, source_ref_id, hazard_type, severity, area_name,
    latitude, longitude, occurred_at, ingested_at, attributes`

func scanHazard(row pgx.Row) (HazardEvent, error) {
	var hazard HazardEvent
	err := row.Scan(&hazard.HazardID, &hazard.Source, &hazard.SourceRefID, &hazard.HazardType,
		&hazard.Severity, &hazard.AreaName, &hazard.Latitude, &hazard.Longitude,
		&hazard.OccurredAt, &hazard.IngestedAt, &hazard.Attributes)
	hazard.OccurredAt, hazard.IngestedAt = hazard.OccurredAt.UTC(), hazard.IngestedAt.UTC()
	return hazard, err
}

func readHazardPage(ctx context.Context, tx pgx.Tx, query hazardQuery) ([]HazardEvent, error) {
	sql, args := hazardPageSQL(query)
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, storeError("read hazard page", err)
	}
	defer rows.Close()
	hazards := make([]HazardEvent, 0, query.limit+1)
	for rows.Next() {
		hazard, err := scanHazard(rows)
		if err != nil {
			return nil, storeError("scan hazard page", err)
		}
		hazards = append(hazards, hazard)
	}
	return hazards, storeError("read hazard page", rows.Err())
}

func hazardPageSQL(query hazardQuery) (string, []any) {
	clauses, args := []string{"TRUE"}, []any{}
	add := func(expression string, argument any) {
		args = append(args, argument)
		clauses = append(clauses, fmt.Sprintf(expression, len(args)))
	}
	filters := query.filters
	if filters.Source != nil {
		add("source = $%d", string(*filters.Source))
	}
	if filters.HazardType != nil {
		add("hazard_type = $%d", string(*filters.HazardType))
	}
	if filters.Severity != nil {
		add("severity = $%d", string(*filters.Severity))
	}
	if filters.OccurredFrom != nil {
		// pgx timestamps have microsecond precision; round the lower bound up, not down.
		lower := filters.OccurredFrom.Truncate(time.Microsecond)
		if lower.Before(*filters.OccurredFrom) {
			lower = lower.Add(time.Microsecond)
		}
		add("occurred_at >= $%d", lower)
	}
	if filters.OccurredTo != nil {
		add("occurred_at <= $%d", filters.OccurredTo.Truncate(time.Microsecond))
	}
	if query.cursor != nil && !query.cursor.Start {
		args = append(args, *query.cursor.OccurredAt, *query.cursor.HazardID)
		clauses = append(clauses, fmt.Sprintf(`(occurred_at, hazard_id COLLATE "C") < ($%d, $%d::text COLLATE "C")`, len(args)-1, len(args)))
	}
	args = append(args, query.limit+1)
	sql := "SELECT " + hazardColumns + " FROM hazard_events WHERE " + strings.Join(clauses, " AND ")
	return sql + fmt.Sprintf(` ORDER BY occurred_at DESC, hazard_id COLLATE "C" DESC LIMIT $%d`, len(args)), args
}

func readFreshness(ctx context.Context, tx pgx.Tx, selected []Source, cfg Config) (map[Source]sourceState, time.Time, error) {
	var asOf time.Time
	if err := tx.QueryRow(ctx, "SELECT statement_timestamp()").Scan(&asOf); err != nil {
		return nil, asOf, storeError("snapshot timestamp", err)
	}
	asOf = asOf.UTC()
	snapshots, err := readEndpointSnapshots(ctx, tx, selected)
	if err != nil {
		return nil, asOf, err
	}
	sources := make(map[Source]sourceState, len(selected))
	for _, source := range selected {
		if source == Satelit || source == SensorPantai {
			state, err := readUnconfiguredSource(ctx, tx, source)
			if err != nil {
				return nil, asOf, err
			}
			sources[source] = state
			continue
		}
		if len(snapshots[source]) != len(sourceEndpoints(source)) {
			return nil, asOf, storeError("read poll state", errors.New("incomplete endpoint state"))
		}
		sources[source] = sourceFreshness(snapshots[source], asOf, cfg)
	}
	return sources, asOf, nil
}

func readEndpointSnapshots(ctx context.Context, tx pgx.Tx, selected []Source) (map[Source][]endpointSnapshot, error) {
	names := make([]string, 0, len(selected))
	for _, source := range selected {
		names = append(names, string(source))
	}
	rows, err := tx.Query(ctx, `SELECT endpoint, source, last_successful_poll, stale_since, last_attempt_at,
        error_category, EXISTS (SELECT 1 FROM hazard_events h WHERE h.source = p.source)
        FROM poll_state p WHERE source = ANY($1::text[])
        ORDER BY CASE endpoint WHEN 'seismic' THEN 1 WHEN 'warnings' THEN 2 ELSE 3 END`, names)
	if err != nil {
		return nil, storeError("read poll state", err)
	}
	defer rows.Close()
	snapshots := make(map[Source][]endpointSnapshot)
	for rows.Next() {
		var snapshot endpointSnapshot
		var source Source
		if err := rows.Scan(&snapshot.endpoint, &source, &snapshot.LastSuccessfulPoll,
			&snapshot.StaleSince, &snapshot.LastAttemptAt, &snapshot.ErrorCategory, &snapshot.cached); err != nil {
			return nil, storeError("scan poll state", err)
		}
		snapshot.LastSuccessfulPoll = utcTime(snapshot.LastSuccessfulPoll)
		snapshot.StaleSince, snapshot.LastAttemptAt = utcTime(snapshot.StaleSince), utcTime(snapshot.LastAttemptAt)
		snapshots[source] = append(snapshots[source], snapshot)
	}
	return snapshots, storeError("read poll state", rows.Err())
}

func sourceEndpoints(source Source) []Endpoint {
	switch source {
	case BMKG:
		return []Endpoint{SeismicEndpoint, WarningEndpoint}
	case PVMBG:
		return []Endpoint{VolcanicEndpoint}
	default:
		return nil
	}
}

func readUnconfiguredSource(ctx context.Context, tx pgx.Tx, source Source) (sourceState, error) {
	category := "source_not_configured"
	state := sourceState{pollObservation: pollObservation{Status: "unavailable", ErrorCategory: &category},
		Endpoints: make(map[string]pollObservation)}
	err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM hazard_events WHERE source = $1)", string(source)).Scan(&state.HasCachedData)
	if state.HasCachedData {
		state.Status = "stale"
	}
	return state, storeError("read unconfigured source", err)
}

func utcTime(stamp *time.Time) *time.Time {
	if stamp == nil {
		return nil
	}
	utc := stamp.UTC()
	return &utc
}
