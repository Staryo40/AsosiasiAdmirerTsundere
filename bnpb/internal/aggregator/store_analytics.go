package aggregator

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

const analyticsMaxGroupsPerStore = 10000

func (store *Store) readAnalytics(ctx context.Context, query analyticsQuery, maxBytes int64) ([]analyticsGroup, error) {
	var groups []analyticsGroup
	err := store.primarySnapshot(ctx, func(ctx context.Context, tx pgx.Tx) error {
		var err error
		groups, err = readAnalyticsGroups(ctx, tx, query, maxBytes)
		return err
	})
	return groups, err
}
func analyticsCeilMicrosecond(stamp time.Time) time.Time {
	truncated := stamp.Truncate(time.Microsecond)
	if truncated.Equal(stamp) {
		return truncated
	}
	return truncated.Add(time.Microsecond)
}

// Only canonical warning attributes seen by Audit classify seismic rows.
const analyticsGroupsSQL = `WITH classified AS (
    SELECT %s AS bucket,
           CASE
             WHEN h.source = 'BMKG' AND h.hazard_type = 'SEISMIC' AND
                  h.attributes @> jsonb_build_object('tsunami_warnings',
                      jsonb_build_array(jsonb_build_object('related_event_id', h.source_ref_id)))
             THEN 'TSUNAMI'
             WHEN h.source = 'BMKG' AND h.hazard_type = 'SEISMIC' THEN 'EARTHQUAKE'
             WHEN h.source = 'PVMBG' AND h.hazard_type = 'VOLCANIC' THEN 'VOLCANIC'
             WHEN h.source = 'SATELIT' AND h.hazard_type = 'WILDFIRE' THEN 'WILDFIRE'
             WHEN h.source = 'SENSOR_PANTAI' AND h.hazard_type = 'COASTAL' THEN 'COASTAL'
           END AS category,
           h.severity,
           h.area_name COLLATE "C" AS area_name
    FROM hazard_events h
    WHERE h.occurred_at >= $1 AND h.occurred_at < $2 AND h.withdrawn_at IS NULL
), grouped AS (
    SELECT bucket, category, severity, area_name, count(*) AS event_count
    FROM classified
    GROUP BY GROUPING SETS ((bucket, category), (severity), (area_name))
    LIMIT $4
), sized AS (
    SELECT *, COALESCE(sum(octet_length(area_name)) OVER (), 0) AS area_bytes
    FROM grouped
)
SELECT bucket, category, severity, CASE WHEN area_bytes <= $3 THEN area_name END,
       event_count, area_bytes > $3
FROM sized`

func readAnalyticsGroups(ctx context.Context, tx pgx.Tx, query analyticsQuery, maxBytes int64) ([]analyticsGroup, error) {
	sql := fmt.Sprintf(analyticsGroupsSQL, analyticsBucketExpression(query.Bucket))
	// Raw label bytes are a lower bound on JSON size; don't transfer groups that cannot fit.
	rows, err := tx.Query(ctx, sql, analyticsCeilMicrosecond(query.From), analyticsCeilMicrosecond(query.To), maxBytes, analyticsMaxGroupsPerStore+1)
	if err != nil {
		return nil, storeError("read analytics groups", err)
	}
	defer rows.Close()
	groups := make([]analyticsGroup, 0, 64)
	for rows.Next() {
		group, err := scanAnalyticsGroup(rows)
		if err != nil {
			return nil, err
		}
		groups = append(groups, group)
		if len(groups) > analyticsMaxGroupsPerStore {
			return nil, errAnalyticsGroupLimit
		}
	}
	if err := rows.Err(); err != nil {
		return nil, storeError("read analytics groups", err)
	}
	return groups, nil
}

func scanAnalyticsGroup(row pgx.Row) (analyticsGroup, error) {
	var bucket pgtype.Timestamptz
	var category, severity, area pgtype.Text
	var count int64
	var oversized bool
	if err := row.Scan(&bucket, &category, &severity, &area, &count, &oversized); err != nil {
		return analyticsGroup{}, storeError("scan analytics group", err)
	}
	if oversized {
		return analyticsGroup{}, errAnalyticsResponseLimit
	}
	switch {
	case bucket.Valid:
		return analyticsGroup{Kind: analyticsSeriesGroup, Bucket: bucket.Time.UTC(), Category: category.String, Count: count}, nil
	case severity.Valid:
		return analyticsGroup{Kind: analyticsSeverityGroup, Severity: severity.String, Count: count}, nil
	default:
		return analyticsGroup{Kind: analyticsAreaGroup, AreaName: area.String, Count: count}, nil
	}
}
