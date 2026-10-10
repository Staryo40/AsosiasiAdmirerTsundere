package consumer2

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
// processed_messages; the caller acks it without touching alerts_sent.
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
		`CREATE TABLE IF NOT EXISTS alerts_sent (
			message_id TEXT PRIMARY KEY,
			hazard_id  TEXT NOT NULL,
			severity   TEXT NOT NULL,
			area_name  TEXT NOT NULL,
			occurred_at TEXT NOT NULL,
			sent_at    TEXT NOT NULL
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

// alertSeverities are the only severities the notifier acts on; every other
// event is still dedupe-tracked (so a redelivery is a safe no-op) but never
// produces an alerts_sent row.
var alertSeverities = map[string]bool{"SIAGA": true, "AWAS": true}

// HazardAlert is the notifier-relevant projection of one HazardNotification.
type HazardAlert struct {
	MessageID  string
	HazardID   string
	Severity   string
	AreaName   string
	OccurredAt time.Time
}

// ApplyResult reports whether this delivery actually produced an alert, so
// the caller can log "alert sent" only when the DB really recorded one.
type ApplyResult struct {
	AlertSent bool
}

// Apply records the delivery transactionally: a duplicate message_id rolls
// back and reports errDuplicateMessage; otherwise, only SIAGA/AWAS events
// get an alerts_sent row.
func (store *Store) Apply(ctx context.Context, alert HazardAlert) (ApplyResult, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return ApplyResult{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	result, err := tx.ExecContext(ctx,
		`INSERT INTO processed_messages (message_id, processed_at) VALUES (?, ?)
		 ON CONFLICT (message_id) DO NOTHING`, alert.MessageID, now)
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

	alertSent := alertSeverities[alert.Severity]
	if alertSent {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO alerts_sent (message_id, hazard_id, severity, area_name, occurred_at, sent_at)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			alert.MessageID, alert.HazardID, alert.Severity, alert.AreaName,
			alert.OccurredAt.UTC().Format(time.RFC3339Nano), now); err != nil {
			return ApplyResult{}, fmt.Errorf("insert alerts_sent: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return ApplyResult{}, fmt.Errorf("commit: %w", err)
	}
	return ApplyResult{AlertSent: alertSent}, nil
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
	AlertsSent   int64 `json:"alerts_sent"`
	MessagesSeen int64 `json:"messages_seen"`
}

func (store *Store) Stats(ctx context.Context) (Stats, error) {
	var stats Stats
	if err := store.db.QueryRowContext(ctx, `SELECT count(*) FROM alerts_sent`).Scan(&stats.AlertsSent); err != nil {
		return Stats{}, fmt.Errorf("count alerts_sent: %w", err)
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
