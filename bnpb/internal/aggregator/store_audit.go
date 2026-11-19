package aggregator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
)

type auditEventsResponse struct {
	Items            []HazardEvent `json:"items"`
	NextCursor       *string       `json:"next_cursor"`
	Partial          bool          `json:"partial"`
	UnavailableUnits []string      `json:"unavailable_units"`
}
type auditChecksumResponse struct {
	Source           *Source   `json:"source"`
	Count            int64     `json:"count"`
	Checksum         string    `json:"checksum"`
	Algorithm        string    `json:"algorithm"`
	AsOf             time.Time `json:"as_of"`
	Partial          bool      `json:"partial"`
	UnavailableUnits []string  `json:"unavailable_units"`
}

func (api auditAPI) events(w http.ResponseWriter, r *http.Request) {
	query, err := parseAuditQuery(r.URL.RawQuery, true)
	if err != nil {
		auditWriteError(w, http.StatusBadRequest, "invalid_query", "invalid query parameters")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), api.cfg.DBReadTimeout)
	defer cancel()
	results := fanoutLogicalReads(ctx, api.store.logicalReadStores(), func(ctx context.Context, unit logicalReadStore) ([]HazardEvent, error) {
		return unit.Store.readAuditEvents(ctx, query)
	})
	batches := make([][]HazardEvent, 0, len(results))
	unavailable := make([]string, 0)
	for _, result := range results {
		if result.Err != nil {
			unavailable = append(unavailable, result.Unit.Unit)
			continue
		}
		batches = append(batches, result.Value)
	}
	partial := len(unavailable) > 0
	items := mergeAuditEvents(batches, query.Limit+1)
	hasMore := len(items) > query.Limit
	if hasMore {
		items = items[:query.Limit]
	}
	response := auditEventsResponse{Items: items, Partial: partial, UnavailableUnits: unavailable}
	if partial {
		cursor := query.CursorToken
		if cursor == "" {
			cursor = encodeAuditStartCursor()
		}
		response.NextCursor = &cursor
	} else if hasMore {
		cursor := encodeAuditCursor(items[len(items)-1])
		response.NextCursor = &cursor
	}
	auditWriteJSON(w, http.StatusOK, response)
}

func (store *Store) readAuditEvents(ctx context.Context, query auditQuery) ([]HazardEvent, error) {
	var events []HazardEvent
	err := store.primarySnapshot(ctx, func(ctx context.Context, tx pgx.Tx) error {
		var err error
		events, err = readAuditEventPage(ctx, tx, query)
		return err
	})
	return events, err
}

func readAuditEventPage(ctx context.Context, tx pgx.Tx, query auditQuery) ([]HazardEvent, error) {
	clauses, args := []string{"TRUE"}, []any{}
	add := func(expression string, value any) {
		args = append(args, value)
		clauses = append(clauses, fmt.Sprintf(expression, len(args)))
	}
	if query.Source != nil {
		add("source = $%d", string(*query.Source))
	}
	if query.Since != nil {
		since := query.Since.Truncate(time.Microsecond)
		if !since.Equal(*query.Since) {
			since = since.Add(time.Microsecond)
		}
		add("ingested_at >= $%d", since)
	}
	if query.Cursor != nil && !query.Cursor.Start {
		args = append(args, query.Cursor.IngestedAt, query.Cursor.HazardID)
		clauses = append(clauses, fmt.Sprintf(`(ingested_at, hazard_id COLLATE "C") > ($%d, $%d::text COLLATE "C")`, len(args)-1, len(args)))
	}
	args = append(args, query.Limit+1)
	sql := "SELECT " + hazardColumns + " FROM hazard_events WHERE " + joinSQL(clauses) + fmt.Sprintf(` ORDER BY ingested_at ASC, hazard_id COLLATE "C" ASC LIMIT $%d`, len(args))
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, storeError("read audit events", err)
	}
	defer rows.Close()
	items := make([]HazardEvent, 0, query.Limit+1)
	for rows.Next() {
		item, scanErr := scanHazard(rows)
		if scanErr != nil {
			return nil, storeError("scan audit events", scanErr)
		}
		items = append(items, item)
	}
	if rows.Err() != nil {
		return nil, storeError("read audit events", rows.Err())
	}
	return items, nil
}

func joinSQL(items []string) string {
	if len(items) == 0 {
		return "TRUE"
	}
	result := items[0]
	for _, item := range items[1:] {
		result += " AND " + item
	}
	return result
}

func mergeAuditEvents(batches [][]HazardEvent, max int) []HazardEvent {
	positions := make([]int, len(batches))
	merged := make([]HazardEvent, 0, max)
	for len(merged) < max {
		best := -1
		for i, batch := range batches {
			if positions[i] >= len(batch) {
				continue
			}
			if best < 0 || auditEventLess(batch[positions[i]], batches[best][positions[best]]) {
				best = i
			}
		}
		if best < 0 {
			break
		}
		merged = append(merged, batches[best][positions[best]])
		positions[best]++
	}
	return merged
}
func auditEventLess(a, b HazardEvent) bool {
	if !a.IngestedAt.Equal(b.IngestedAt) {
		return a.IngestedAt.Before(b.IngestedAt)
	}
	return a.HazardID < b.HazardID
}

func (api auditAPI) checksum(w http.ResponseWriter, r *http.Request) {
	query, err := parseAuditQuery(r.URL.RawQuery, false)
	if err != nil {
		auditWriteError(w, http.StatusBadRequest, "invalid_query", "invalid query parameters")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), api.cfg.Audit.ChecksumTimeout)
	defer cancel()
	asOf := time.Now().UTC()
	response := auditChecksumResponse{Source: query.Source, Algorithm: "sha256", AsOf: asOf, UnavailableUnits: []string{}}
	var streams [logicalShardCount]auditStream
	stores := api.store.logicalReadStores()
	for index, unit := range stores {
		streams[index] = startAuditRefStream(ctx, unit, query.Source)
	}
	response.Count, response.Checksum = checksumAuditRefs(streams[:len(stores)])
	for _, stream := range streams[:len(stores)] {
		if stream.err != nil {
			response.UnavailableUnits = append(response.UnavailableUnits, stream.unit.Unit)
		}
	}
	response.Partial = len(response.UnavailableUnits) > 0
	auditWriteJSON(w, http.StatusOK, response)
}

type auditStream struct {
	unit logicalReadStore
	refs <-chan string
	done <-chan error
	err  error
}

type auditRefHead struct {
	ref    string
	active bool
}

// Merge one stream per logical shard, so equal reference strings still count
// as separate events. An empty merge hashes zero bytes.
func checksumAuditRefs(streams []auditStream) (int64, string) {
	var heads [logicalShardCount]auditRefHead
	for index := range streams {
		heads[index] = streams[index].next()
	}
	digest := sha256.New()
	var count int64
	for best := nextAuditRefIndex(heads[:len(streams)]); best >= 0; best = nextAuditRefIndex(heads[:len(streams)]) {
		if count != 0 {
			_, _ = digest.Write([]byte{'\n'})
		}
		_, _ = digest.Write([]byte(heads[best].ref))
		count++
		heads[best] = streams[best].next()
	}
	return count, hex.EncodeToString(digest.Sum(nil))
}

func nextAuditRefIndex(heads []auditRefHead) int {
	best := -1
	for index, head := range heads {
		if head.active && (best < 0 || head.ref < heads[best].ref) {
			best = index
		}
	}
	return best
}

func startAuditRefStream(ctx context.Context, unit logicalReadStore, source *Source) auditStream {
	refs := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- unit.Store.primarySnapshot(ctx, func(ctx context.Context, tx pgx.Tx) error {
			return streamAuditRefs(ctx, tx, source, refs)
		})
		close(refs)
	}()
	return auditStream{unit: unit, refs: refs, done: done}
}

func (stream *auditStream) next() auditRefHead {
	ref, ok := <-stream.refs
	if !ok {
		stream.err = <-stream.done
	}
	return auditRefHead{ref: ref, active: ok}
}

func streamAuditRefs(ctx context.Context, tx pgx.Tx, source *Source, refs chan<- string) error {
	query := "SELECT source_ref_id FROM hazard_events"
	args := []any{}
	if source != nil {
		query += " WHERE source = $1"
		args = append(args, string(*source))
	}
	query += ` ORDER BY source_ref_id COLLATE "C" ASC`
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return storeError("read audit checksum", err)
	}
	defer rows.Close()
	for rows.Next() {
		var sourceRef string
		if err := rows.Scan(&sourceRef); err != nil {
			return storeError("scan audit checksum", err)
		}
		select {
		case refs <- sourceRef:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := rows.Err(); err != nil {
		return storeError("read audit checksum", err)
	}
	return nil
}
