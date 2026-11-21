package satellite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

type Store struct {
	db *sql.DB
}

type loggedEvent struct {
	offset int64
	kind   string
	body   []byte
}

type outboxRow struct {
	seq          int64
	fireID       string
	feedRevision int64
	record       []byte
	receivedAt   time.Time
}

func OpenStore(ctx context.Context, path string) (*Store, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(FULL)")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	for _, statement := range []string{
		`CREATE TABLE IF NOT EXISTS checkpoint (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			next_offset INTEGER NOT NULL CHECK (next_offset >= 0)
		)`,
		// Every event that reached the engine, so a restart replays it into exactly the same state.
		`CREATE TABLE IF NOT EXISTS events (
			offset_id INTEGER PRIMARY KEY,
			kind TEXT NOT NULL,
			body BLOB NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS outbox (
			seq INTEGER PRIMARY KEY AUTOINCREMENT,
			fire_id TEXT NOT NULL,
			feed_revision INTEGER NOT NULL,
			record BLOB NOT NULL,
			received_at TEXT NOT NULL,
			sent INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE INDEX IF NOT EXISTS outbox_unsent ON outbox (seq) WHERE sent = 0`,
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			db.Close()
			return nil, fmt.Errorf("migrate sqlite: %w", err)
		}
	}
	return &Store{db: db}, nil
}

func (store *Store) Close() error { return store.db.Close() }

func (store *Store) NextOffset(ctx context.Context) (int64, error) {
	var next int64
	err := store.db.QueryRowContext(ctx, "SELECT next_offset FROM checkpoint WHERE id = 1").Scan(&next)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return next, err
}

func (store *Store) SaveNextOffset(ctx context.Context, next int64) error {
	_, err := store.db.ExecContext(ctx, `INSERT INTO checkpoint (id, next_offset) VALUES (1, ?)
		ON CONFLICT (id) DO UPDATE SET next_offset = excluded.next_offset`, next)
	return err
}

// Events, the fire changes they caused and the checkpoint land together or not at all.
func (store *Store) Commit(ctx context.Context, events []loggedEvent, changes []outboxRow, next int64) (err error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	for _, event := range events {
		if _, err = tx.ExecContext(ctx, "INSERT INTO events (offset_id, kind, body) VALUES (?, ?, ?)", event.offset, event.kind, event.body); err != nil {
			return err
		}
	}
	for _, change := range changes {
		if _, err = tx.ExecContext(ctx, "INSERT INTO outbox (fire_id, feed_revision, record, received_at) VALUES (?, ?, ?, ?)",
			change.fireID, change.feedRevision, change.record, change.receivedAt.UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO checkpoint (id, next_offset) VALUES (1, ?)
		ON CONFLICT (id) DO UPDATE SET next_offset = excluded.next_offset`, next); err != nil {
		return err
	}
	return tx.Commit()
}

func (store *Store) ReplayEvents(ctx context.Context, apply func(loggedEvent) error) error {
	rows, err := store.db.QueryContext(ctx, "SELECT offset_id, kind, body FROM events ORDER BY offset_id")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var event loggedEvent
		if err := rows.Scan(&event.offset, &event.kind, &event.body); err != nil {
			return err
		}
		if err := apply(event); err != nil {
			return err
		}
	}
	return rows.Err()
}

// Oldest unsent changes first, bounded by count and bytes so one request stays well under the route's body limit.
func (store *Store) PendingOutbox(ctx context.Context, limit, maxBytes int) ([]outboxRow, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT seq, fire_id, feed_revision, record, received_at FROM outbox
		WHERE sent = 0 ORDER BY seq LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pending []outboxRow
	size := 0
	for rows.Next() {
		var row outboxRow
		var received string
		if err := rows.Scan(&row.seq, &row.fireID, &row.feedRevision, &row.record, &received); err != nil {
			return nil, err
		}
		row.receivedAt, _ = time.Parse(time.RFC3339Nano, received)
		if size += len(row.record); size > maxBytes && len(pending) > 0 {
			break
		}
		pending = append(pending, row)
	}
	return pending, rows.Err()
}

func (store *Store) MarkSent(ctx context.Context, first, last int64) error {
	_, err := store.db.ExecContext(ctx, "UPDATE outbox SET sent = 1 WHERE seq BETWEEN ? AND ? AND sent = 0", first, last)
	return err
}

func (store *Store) Backlog(ctx context.Context) (int64, error) {
	var count int64
	err := store.db.QueryRowContext(ctx, "SELECT count(*) FROM outbox WHERE sent = 0").Scan(&count)
	return count, err
}
