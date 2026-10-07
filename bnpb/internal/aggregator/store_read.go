package aggregator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

var errHazardNotFound = errors.New("hazard not found")

type hazardList struct {
	Items      []HazardEvent          `json:"items"`
	NextCursor *string                `json:"next_cursor"`
	Sources    map[Source]sourceState `json:"sources"`
	AsOf       time.Time              `json:"as_of"`
}

type hazardDetail struct {
	Item    HazardEvent            `json:"item"`
	Sources map[Source]sourceState `json:"sources"`
	AsOf    time.Time              `json:"as_of"`
}

func (store *Store) snapshot(ctx context.Context, read func(context.Context, pgx.Tx) error) (err error) {
	ctx, cancel := context.WithTimeout(ctx, store.readTimeout)
	defer cancel()
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

func (store *Store) listHazards(ctx context.Context, query hazardQuery, cfg Config) (hazardList, error) {
	page := hazardList{Items: []HazardEvent{}}
	err := store.snapshot(ctx, func(ctx context.Context, tx pgx.Tx) error {
		var err error
		page.Sources, page.AsOf, err = readFreshness(ctx, tx, query.filters.selectedSources(), cfg)
		if err != nil {
			return err
		}
		page.Items, err = readHazardPage(ctx, tx, query)
		return err
	})
	if err == nil && len(page.Items) > query.limit {
		page.Items = page.Items[:query.limit]
		cursor, cursorErr := nextHazardCursor(page.Items[len(page.Items)-1], query.filters)
		err = storeError("encode hazard cursor", cursorErr)
		page.NextCursor = &cursor
	}
	return page, err
}

func (store *Store) hazardDetail(ctx context.Context, hazardID string, cfg Config) (hazardDetail, error) {
	detail := hazardDetail{}
	err := store.snapshot(ctx, func(ctx context.Context, tx pgx.Tx) error {
		hazard, err := scanHazard(tx.QueryRow(ctx, "SELECT "+hazardColumns+" FROM hazard_events WHERE hazard_id = $1", hazardID))
		if errors.Is(err, pgx.ErrNoRows) {
			return errHazardNotFound
		}
		if err != nil {
			return storeError("read hazard", err)
		}
		detail.Item = hazard
		detail.Sources, detail.AsOf, err = readFreshness(ctx, tx, []Source{hazard.Source}, cfg)
		return err
	})
	return detail, err
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
	if query.cursor != nil {
		args = append(args, query.cursor.OccurredAt, query.cursor.HazardID)
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
	if source == BMKG {
		return []Endpoint{SeismicEndpoint, WarningEndpoint}
	}
	return []Endpoint{VolcanicEndpoint}
}

func utcTime(stamp *time.Time) *time.Time {
	if stamp == nil {
		return nil
	}
	utc := stamp.UTC()
	return &utc
}
