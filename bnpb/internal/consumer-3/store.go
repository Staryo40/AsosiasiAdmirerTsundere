package consumer3

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// processedMessageWindow must exceed the broker's 3h message TTL (Decision
// 1): a message older than that can never be redelivered, so its dedupe row
// is safe to prune.
const processedMessageWindow = 4 * time.Hour

// errDuplicateMessage marks a message_id already recorded in
// processed_messages; the caller acks it without touching portal_feed.
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
		// Append-only: unlike the dashboard's latest-state table, a portal
		// feed shows every event as it happened, so there is no upsert or
		// newest-wins guard here, only one row per distinct message_id.
		`CREATE TABLE IF NOT EXISTS portal_feed (
			message_id  TEXT PRIMARY KEY,
			hazard_id   TEXT NOT NULL,
			source      TEXT NOT NULL,
			hazard_type TEXT NOT NULL,
			severity    TEXT NOT NULL,
			area_name   TEXT NOT NULL,
			occurred_at TEXT NOT NULL,
			received_at TEXT NOT NULL
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

// FeedEntry is the portal-feed projection of one HazardNotification.
type FeedEntry struct {
	MessageID  string
	HazardID   string
	Source     string
	HazardType string
	Severity   string
	AreaName   string
	OccurredAt time.Time
}

// Apply records the entry transactionally: a duplicate message_id rolls
// back and reports errDuplicateMessage; otherwise it appends a feed row.
func (store *Store) Apply(ctx context.Context, entry FeedEntry) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	result, err := tx.ExecContext(ctx,
		`INSERT INTO processed_messages (message_id, processed_at) VALUES (?, ?)
		 ON CONFLICT (message_id) DO NOTHING`, entry.MessageID, now)
	if err != nil {
		return fmt.Errorf("insert processed_messages: %w", err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected: %w", err)
	}
	if inserted == 0 {
		return errDuplicateMessage
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO portal_feed (message_id, hazard_id, source, hazard_type, severity, area_name, occurred_at, received_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		entry.MessageID, entry.HazardID, entry.Source, entry.HazardType, entry.Severity, entry.AreaName,
		entry.OccurredAt.UTC().Format(time.RFC3339Nano), now); err != nil {
		return fmt.Errorf("insert portal_feed: %w", err)
	}

	return tx.Commit()
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
	FeedEntries  int64 `json:"feed_entries"`
	MessagesSeen int64 `json:"messages_seen"`
}

func (store *Store) Stats(ctx context.Context) (Stats, error) {
	var stats Stats
	if err := store.db.QueryRowContext(ctx, `SELECT count(*) FROM portal_feed`).Scan(&stats.FeedEntries); err != nil {
		return Stats{}, fmt.Errorf("count portal_feed: %w", err)
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
