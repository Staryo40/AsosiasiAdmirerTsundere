package consumer1

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// processedMessageWindow must exceed the broker's 3h message TTL: a message
// older than that can never be redelivered, so its dedupe row is safe to
// prune.
const processedMessageWindow = 4 * time.Hour

// errDuplicateMessage marks a message_id already recorded in
// processed_messages; the caller acks it without touching hazard_state.
var errDuplicateMessage = errors.New("duplicate message_id")

type Store struct {
	db *sql.DB
}

func OpenStore(ctx context.Context, path string) (*Store, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	if err := migrate(ctx, db); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate sqlite: %w", err)
	}
	return &Store{db: db}, nil
}

func (store *Store) Close() error { return store.db.Close() }

func migrate(ctx context.Context, db *sql.DB) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS hazard_state (
			hazard_id   TEXT PRIMARY KEY,
			source      TEXT NOT NULL,
			hazard_type TEXT NOT NULL,
			severity    TEXT NOT NULL,
			area_name   TEXT NOT NULL,
			occurred_at TEXT NOT NULL,
			ingested_at TEXT NOT NULL,
			revision    INTEGER NOT NULL,
			updated_at  TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS processed_messages (
			message_id   TEXT PRIMARY KEY,
			processed_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS delivery_attempts (
			message_id TEXT PRIMARY KEY,
			attempts   INTEGER NOT NULL,
			updated_at TEXT NOT NULL
		)`,
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

// HazardUpdate is the dashboard-relevant projection of one HazardNotification.
type HazardUpdate struct {
	MessageID  string
	HazardID   string
	Source     string
	HazardType string
	Severity   string
	AreaName   string
	OccurredAt time.Time
	IngestedAt time.Time
	Revision   int64
}

// ApplyResult distinguishes a real state change from a guarded no-op, so
// callers can log (and the demo can show) the newest-wins guard actually
// doing something, not just assume every non-error outcome changed state.
type ApplyResult struct {
	Applied bool // false when the newest-wins guard rejected a stale revision
}

// Apply records the update transactionally: a duplicate message_id rolls
// back and reports errDuplicateMessage; otherwise hazard_state is upserted
// with a newest-wins guard on revision, so a requeued older delivery can
// never roll severity back down.
func (store *Store) Apply(ctx context.Context, update HazardUpdate) (ApplyResult, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return ApplyResult{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	result, err := tx.ExecContext(ctx,
		`INSERT INTO processed_messages (message_id, processed_at) VALUES (?, ?)
		 ON CONFLICT (message_id) DO NOTHING`, update.MessageID, now)
	if err != nil {
		return ApplyResult{}, fmt.Errorf("insert processed_messages: %w", err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return ApplyResult{}, fmt.Errorf("rows affected: %w", err)
	}
	if inserted == 0 {
		return ApplyResult{}, errDuplicateMessage
	}

	stateResult, err := tx.ExecContext(ctx,
		`INSERT INTO hazard_state (hazard_id, source, hazard_type, severity, area_name, occurred_at, ingested_at, revision, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT (hazard_id) DO UPDATE SET
			source = excluded.source, hazard_type = excluded.hazard_type, severity = excluded.severity,
			area_name = excluded.area_name, occurred_at = excluded.occurred_at, ingested_at = excluded.ingested_at,
			revision = excluded.revision, updated_at = excluded.updated_at
		 WHERE excluded.revision > hazard_state.revision`,
		update.HazardID, update.Source, update.HazardType, update.Severity, update.AreaName,
		update.OccurredAt.UTC().Format(time.RFC3339Nano), update.IngestedAt.UTC().Format(time.RFC3339Nano),
		update.Revision, now)
	if err != nil {
		return ApplyResult{}, fmt.Errorf("upsert hazard_state: %w", err)
	}
	changed, err := stateResult.RowsAffected()
	if err != nil {
		return ApplyResult{}, fmt.Errorf("rows affected: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return ApplyResult{}, fmt.Errorf("commit: %w", err)
	}
	return ApplyResult{Applied: changed > 0}, nil
}

// IncrementAttempts records one more failed delivery attempt for messageID
// and returns the new total, independent of RabbitMQ's own delivery-count
// (basic.nack does not increment it, and crash-based redelivery semantics
// are undocumented, so the 3-attempt rule is tracked here instead).
func (store *Store) IncrementAttempts(ctx context.Context, messageID string) (int64, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	var attempts int64
	err := store.db.QueryRowContext(ctx,
		`INSERT INTO delivery_attempts (message_id, attempts, updated_at) VALUES (?, 1, ?)
		 ON CONFLICT (message_id) DO UPDATE SET attempts = delivery_attempts.attempts + 1, updated_at = excluded.updated_at
		 RETURNING attempts`, messageID, now).Scan(&attempts)
	if err != nil {
		return 0, fmt.Errorf("increment delivery_attempts: %w", err)
	}
	return attempts, nil
}

// Stats reports counters for GET /stats.
type Stats struct {
	HazardsTracked int64 `json:"hazards_tracked"`
	MessagesSeen   int64 `json:"messages_seen"`
}

func (store *Store) Stats(ctx context.Context) (Stats, error) {
	var stats Stats
	if err := store.db.QueryRowContext(ctx, `SELECT count(*) FROM hazard_state`).Scan(&stats.HazardsTracked); err != nil {
		return Stats{}, fmt.Errorf("count hazard_state: %w", err)
	}
	if err := store.db.QueryRowContext(ctx, `SELECT count(*) FROM processed_messages`).Scan(&stats.MessagesSeen); err != nil {
		return Stats{}, fmt.Errorf("count processed_messages: %w", err)
	}
	return stats, nil
}

// PruneOldRecords removes dedupe and attempt-tracking rows old enough that
// the broker's 3h message TTL guarantees they can never be touched again.
func (store *Store) PruneOldRecords(ctx context.Context) error {
	cutoff := time.Now().Add(-processedMessageWindow).UTC().Format(time.RFC3339Nano)
	if _, err := store.db.ExecContext(ctx, `DELETE FROM processed_messages WHERE processed_at < ?`, cutoff); err != nil {
		return fmt.Errorf("prune processed_messages: %w", err)
	}
	if _, err := store.db.ExecContext(ctx, `DELETE FROM delivery_attempts WHERE updated_at < ?`, cutoff); err != nil {
		return fmt.Errorf("prune delivery_attempts: %w", err)
	}
	return nil
}
