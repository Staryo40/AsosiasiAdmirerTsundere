package aggregator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type Endpoint string

const (
	SeismicEndpoint  Endpoint = "seismic"
	WarningEndpoint  Endpoint = "warnings"
	VolcanicEndpoint Endpoint = "volcanic"
)

type IngestChunk struct {
	Endpoint   Endpoint
	Records    []json.RawMessage
	ReceivedAt time.Time
}

type endpointFields struct {
	identity, timestamp string
	lock                int
}

type rejectedRecord struct {
	raw    json.RawMessage
	reason ValidationError
}

var ingestEndpoints = map[Endpoint]endpointFields{
	SeismicEndpoint:  {"event_id", "occurred_at", 1},
	WarningEndpoint:  {"warning_id", "issued_at", 1},
	VolcanicEndpoint: {"report_id", "reported_at", 2},
}

// Ingest commits one bounded chunk. Callers must decode a complete response before submitting chunks.
func (store *Store) Ingest(ctx context.Context, chunk IngestChunk) (err error) {
	return store.ingestChunk(ctx, chunk, checkpointPolicy{completePoll: true})
}

// Pollers can commit chunks without advertising a successful incomplete response.
type checkpointPolicy struct {
	ceiling      *time.Time
	started      *time.Time
	completed    *time.Time
	hold         bool
	completePoll bool
}

func (store *Store) ingestChunk(ctx context.Context, chunk IngestChunk, policy checkpointPolicy) (err error) {
	if err := store.validateChunk(chunk); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, store.writeTimeout)
	defer cancel()
	tx, err := store.write.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return storeError("begin ingest", err)
	}
	defer func() { err = storeError("commit ingest", err) }()
	defer func() {
		if cleanupErr := rollbackTransaction(ctx, tx); cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
		}
	}()
	// Only database work is serialized; BMKG warnings and events share source identity state.
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(4031, $1)", ingestEndpoints[chunk.Endpoint].lock); err != nil {
		return err
	}
	if err = store.writeChunk(ctx, tx, chunk, policy); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (store *Store) validateChunk(chunk IngestChunk) error {
	if _, known := ingestEndpoints[chunk.Endpoint]; !known {
		return errors.New("ingest: unknown endpoint")
	}
	if !validInstant(chunk.ReceivedAt) {
		return errors.New("ingest: invalid receipt time")
	}
	if len(chunk.Records) > store.chunkRecords {
		return errors.New("ingest: chunk record limit exceeded")
	}
	var bytes int64
	for _, raw := range chunk.Records {
		bytes += int64(len(raw))
	}
	if bytes > store.maxBytes {
		return errors.New("ingest: chunk byte limit exceeded")
	}
	return nil
}

func (store *Store) writeChunk(ctx context.Context, tx pgx.Tx, chunk IngestChunk, policy checkpointPolicy) error {
	var checkpoint *time.Time
	completeTimestamps := true
	for _, raw := range chunk.Records {
		stamp := recordTimestamp(raw, chunk.Endpoint)
		if stamp == nil {
			completeTimestamps = false
		}
		if stamp != nil && (checkpoint == nil || stamp.After(*checkpoint)) {
			checkpoint = stamp
		}
		if err := store.writeRecord(ctx, tx, chunk, raw); err != nil {
			return err
		}
	}
	if !completeTimestamps || policy.hold {
		checkpoint = nil
	}
	_, err := tx.Exec(ctx, `UPDATE poll_state
        SET checkpoint = CASE WHEN GREATEST(checkpoint, $2::timestamptz) IS NULL THEN NULL
            ELSE LEAST(GREATEST(checkpoint, $2::timestamptz), $4::timestamptz) END,
		last_attempt_at = COALESCE($6::timestamptz, $3),
        last_successful_poll = CASE WHEN $5 THEN COALESCE($7::timestamptz, $3) ELSE last_successful_poll END,
        stale_since = CASE WHEN $5 THEN NULL ELSE stale_since END,
        error_category = CASE WHEN $5 THEN NULL ELSE error_category END
        WHERE endpoint = $1`, string(chunk.Endpoint), checkpoint, databaseTime(chunk.ReceivedAt), policy.ceiling, policy.completePoll, policy.started, policy.completed)
	return err
}

func (store *Store) writeRecord(ctx context.Context, tx pgx.Tx, chunk IngestChunk, raw json.RawMessage) error {
	recordTx, err := tx.Begin(ctx)
	if err != nil {
		return err
	}
	err = store.writeSourceRecord(ctx, recordTx, chunk, raw)
	if err == nil {
		return recordTx.Commit(ctx)
	}
	if cleanupErr := rollbackTransaction(ctx, recordTx); cleanupErr != nil {
		err = errors.Join(err, cleanupErr)
	}
	category, field := rejectReason(err)
	if category == "" {
		return err
	}
	return writeReject(ctx, tx, chunk, rejectedRecord{raw, ValidationError{Code: ValidationCode(category), Field: field}})
}

func (store *Store) writeSourceRecord(ctx context.Context, tx pgx.Tx, chunk IngestChunk, raw json.RawMessage) error {
	switch chunk.Endpoint {
	case WarningEndpoint:
		return writeWarning(ctx, tx, raw)
	case SeismicEndpoint:
		return store.writeSeismic(ctx, tx, chunk, raw)
	default:
		hazard, err := MapVolcanic(raw, newReceipt(chunk.ReceivedAt))
		if err != nil {
			return err
		}
		return writeHazard(ctx, tx, hazard)
	}
}

func (store *Store) writeSeismic(ctx context.Context, tx pgx.Tx, chunk IngestChunk, raw json.RawMessage) error {
	hazard, err := MapSeismic(raw, nil, newReceipt(chunk.ReceivedAt))
	if err != nil {
		return err
	}
	if len(hazard.SourceRefID) > 2048 {
		return invalidField("event_id", "storage_limit")
	}
	warnings, err := store.storedWarnings(ctx, tx, hazard.SourceRefID)
	if err != nil {
		return err
	}
	hazard, err = MapSeismic(raw, warnings, Receipt{hazard.HazardID, hazard.IngestedAt})
	if err != nil {
		return err
	}
	if err := writeSeismicSource(ctx, tx, raw, hazard); err != nil {
		return err
	}
	return writeHazard(ctx, tx, hazard)
}

func writeSeismicSource(ctx context.Context, tx pgx.Tx, raw json.RawMessage, hazard HazardEvent) error {
	command, err := tx.Exec(ctx, `INSERT INTO source_seismic (event_id, occurred_at, payload)
        VALUES ($1, $2, $3) ON CONFLICT (event_id) DO UPDATE SET payload = EXCLUDED.payload
        WHERE source_seismic.occurred_at = EXCLUDED.occurred_at`, hazard.SourceRefID, databaseTime(hazard.OccurredAt), raw)
	if err != nil {
		return err
	}
	if command.RowsAffected() == 0 {
		return invalidField("event_id", "source_identity_reused")
	}
	return nil
}

func writeWarning(ctx context.Context, tx pgx.Tx, raw json.RawMessage) error {
	warning, err := parseWarning(raw)
	if err != nil {
		return err
	}
	if len(warning.id) > 2048 || len(warning.eventID) > 2048 {
		return invalidField("warning_id", "storage_limit")
	}
	command, err := tx.Exec(ctx, `INSERT INTO source_warnings (warning_id, related_event_id, issued_at, payload)
        VALUES ($1, $2, $3, $4) ON CONFLICT (warning_id) DO UPDATE SET payload = EXCLUDED.payload
        WHERE (source_warnings.related_event_id, source_warnings.issued_at) =
              (EXCLUDED.related_event_id, EXCLUDED.issued_at)`, warning.id, warning.eventID, databaseTime(warning.issuedAt), raw)
	if err != nil {
		return err
	}
	if command.RowsAffected() == 0 {
		return invalidField("warning_id", "source_identity_reused")
	}
	return nil
}

func (store *Store) storedWarnings(ctx context.Context, tx pgx.Tx, eventID string) ([]json.RawMessage, error) {
	rows, err := tx.Query(ctx, "SELECT payload FROM source_warnings WHERE related_event_id = $1 ORDER BY issued_at, warning_id", eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	warnings := make([]json.RawMessage, 0)
	var bytes int64
	for rows.Next() {
		var raw json.RawMessage
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		bytes += int64(len(raw))
		if bytes > store.maxBytes {
			return nil, invalidField("tsunami_warnings", "storage_limit")
		}
		warnings = append(warnings, raw)
	}
	return warnings, rows.Err()
}

func rejectReason(err error) (string, string) {
	var validation *ValidationError
	if errors.As(err, &validation) {
		return string(validation.Code), validation.Field
	}
	var postgresErr *pgconn.PgError
	if errors.As(err, &postgresErr) {
		switch postgresErr.Code {
		case "22003", "22P05", "54000", "22001", "22P02":
			return "storage_limit", "record"
		}
	}
	return "", ""
}

func writeReject(ctx context.Context, tx pgx.Tx, chunk IngestChunk, rejected rejectedRecord) error {
	hash := sha256.Sum256(rejected.raw)
	_, err := tx.Exec(ctx, `INSERT INTO ingest_rejects
        (endpoint, source_ref_id, source_timestamp, category, field, payload_sha256, payload_bytes, first_seen_at, last_seen_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)
        ON CONFLICT (endpoint, payload_sha256, category, field) DO UPDATE
        SET last_seen_at = EXCLUDED.last_seen_at, occurrences = ingest_rejects.occurrences + 1`,
		string(chunk.Endpoint), rejectedIdentity(rejected.raw, chunk.Endpoint), recordTimestamp(rejected.raw, chunk.Endpoint),
		string(rejected.reason.Code), rejected.reason.Field, hex.EncodeToString(hash[:]), len(rejected.raw), databaseTime(chunk.ReceivedAt))
	return err
}

func rejectedIdentity(raw json.RawMessage, endpoint Endpoint) *string {
	fields, err := parseObject(raw)
	if err != nil {
		return nil
	}
	identity, err := textField(fields, ingestEndpoints[endpoint].identity)
	if err != nil {
		return nil
	}
	runes := []rune(identity)
	bounded := string(runes[:min(len(runes), 512)])
	return &bounded
}

func recordTimestamp(raw json.RawMessage, endpoint Endpoint) *time.Time {
	fields, err := parseObject(raw)
	if err != nil {
		return nil
	}
	stamp, err := timeField(fields, ingestEndpoints[endpoint].timestamp)
	if err != nil {
		return nil
	}
	stamp = databaseTime(stamp)
	return &stamp
}

// PostgreSQL stores microseconds; use the same precision in typed rows and notification snapshots.
func databaseTime(stamp time.Time) time.Time { return stamp.UTC().Truncate(time.Microsecond) }
