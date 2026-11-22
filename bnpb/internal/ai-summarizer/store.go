package aisummarizer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

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
		// One row per hazard: an update replaces the older revision.
		`CREATE TABLE IF NOT EXISTS events (
			hazard_id   TEXT PRIMARY KEY,
			revision    INTEGER NOT NULL,
			source      TEXT NOT NULL,
			hazard_type TEXT NOT NULL,
			severity    TEXT NOT NULL,
			area_name   TEXT NOT NULL,
			magnitude   REAL,
			wave_height REAL,
			received_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS processed_messages (
			message_id   TEXT PRIMARY KEY,
			processed_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS summaries (
			id           INTEGER PRIMARY KEY AUTOINCREMENT,
			text         TEXT NOT NULL,
			provider     TEXT NOT NULL,
			model        TEXT NOT NULL,
			event_count  INTEGER NOT NULL,
			digest_hash  TEXT NOT NULL,
			generated_at TEXT NOT NULL
		)`,
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

type Event struct {
	MessageID  string
	HazardID   string
	Revision   int64
	Source     string
	HazardType string
	Severity   string
	AreaName   string
	Magnitude  *float64
	WaveHeight *float64
	ReceivedAt time.Time
}

func (store *Store) Apply(ctx context.Context, event Event) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()
	received := event.ReceivedAt.UTC().Format(time.RFC3339Nano)
	result, err := tx.ExecContext(ctx, `INSERT INTO processed_messages (message_id, processed_at) VALUES (?, ?)
		ON CONFLICT (message_id) DO NOTHING`, event.MessageID, received)
	if err != nil {
		return fmt.Errorf("insert processed_messages: %w", err)
	}
	if inserted, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("rows affected: %w", err)
	} else if inserted == 0 {
		return errDuplicateMessage
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO events
		(hazard_id, revision, source, hazard_type, severity, area_name, magnitude, wave_height, received_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (hazard_id) DO UPDATE SET revision = excluded.revision, source = excluded.source,
			hazard_type = excluded.hazard_type, severity = excluded.severity, area_name = excluded.area_name,
			magnitude = excluded.magnitude, wave_height = excluded.wave_height, received_at = excluded.received_at
		WHERE excluded.revision > events.revision`,
		event.HazardID, event.Revision, event.Source, event.HazardType, event.Severity, event.AreaName,
		event.Magnitude, event.WaveHeight, received); err != nil {
		return fmt.Errorf("upsert events: %w", err)
	}
	return tx.Commit()
}

// Recent returns the events received since the cutoff and drops older rows.
func (store *Store) Recent(ctx context.Context, since time.Time) ([]Event, error) {
	cutoff := since.UTC().Format(time.RFC3339Nano)
	if _, err := store.db.ExecContext(ctx, `DELETE FROM events WHERE received_at < ?`, cutoff); err != nil {
		return nil, fmt.Errorf("prune events: %w", err)
	}
	// Message IDs are kept a day so a late redelivery is still recognised.
	if _, err := store.db.ExecContext(ctx, `DELETE FROM processed_messages WHERE processed_at < ?`,
		since.Add(-24*time.Hour).UTC().Format(time.RFC3339Nano)); err != nil {
		return nil, fmt.Errorf("prune processed_messages: %w", err)
	}
	rows, err := store.db.QueryContext(ctx, `SELECT hazard_id, revision, source, hazard_type, severity, area_name,
		magnitude, wave_height, received_at FROM events WHERE received_at >= ? ORDER BY received_at DESC`, cutoff)
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	defer rows.Close()
	var events []Event
	for rows.Next() {
		var event Event
		var received string
		var magnitude, wave sql.NullFloat64
		if err := rows.Scan(&event.HazardID, &event.Revision, &event.Source, &event.HazardType, &event.Severity,
			&event.AreaName, &magnitude, &wave, &received); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		if magnitude.Valid {
			event.Magnitude = &magnitude.Float64
		}
		if wave.Valid {
			event.WaveHeight = &wave.Float64
		}
		event.ReceivedAt, _ = time.Parse(time.RFC3339Nano, received)
		events = append(events, event)
	}
	return events, rows.Err()
}

type Summary struct {
	Text        string
	Provider    string
	Model       string
	EventCount  int
	DigestHash  string
	GeneratedAt time.Time
}

func (store *Store) SaveSummary(ctx context.Context, summary Summary) error {
	_, err := store.db.ExecContext(ctx, `INSERT INTO summaries (text, provider, model, event_count, digest_hash, generated_at)
		VALUES (?, ?, ?, ?, ?, ?)`, summary.Text, summary.Provider, summary.Model, summary.EventCount, summary.DigestHash,
		summary.GeneratedAt.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("insert summary: %w", err)
	}
	// Only the latest summaries matter; keep a short history for the report.
	_, err = store.db.ExecContext(ctx, `DELETE FROM summaries WHERE id <= (SELECT max(id) - 200 FROM summaries)`)
	return err
}

// Latest returns false when no summary has been written yet.
func (store *Store) Latest(ctx context.Context) (Summary, bool, error) {
	var summary Summary
	var generated string
	err := store.db.QueryRowContext(ctx, `SELECT text, provider, model, event_count, digest_hash, generated_at
		FROM summaries ORDER BY id DESC LIMIT 1`).Scan(&summary.Text, &summary.Provider, &summary.Model,
		&summary.EventCount, &summary.DigestHash, &generated)
	if errors.Is(err, sql.ErrNoRows) {
		return Summary{}, false, nil
	}
	if err != nil {
		return Summary{}, false, fmt.Errorf("query summary: %w", err)
	}
	summary.GeneratedAt, _ = time.Parse(time.RFC3339Nano, generated)
	return summary, true, nil
}
