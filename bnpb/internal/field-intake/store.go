package fieldintake

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

type Report struct {
	ClientID        string    `json:"client_id"`
	ReportID        string    `json:"report_id"`
	ReportedAt      time.Time `json:"reported_at"`
	Latitude        float64   `json:"latitude"`
	Longitude       float64   `json:"longitude"`
	HazardType      string    `json:"hazard_type"`
	Description     string    `json:"description"`
	RelatedHazardID *string   `json:"related_hazard_id"`
	AcceptedAt      time.Time `json:"accepted_at"`
}

type Outcome string

const (
	Stored    Outcome = "stored"
	Duplicate Outcome = "duplicate"
	Conflict  Outcome = "conflict"
)

// A client retry is re-accepted with a new accepted_at, so that field is not part of the content.
func (report Report) contentDigest() string {
	content := report
	content.AcceptedAt = time.Time{}
	encoded, _ := json.Marshal(content)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func (report Report) valid() bool {
	return report.ClientID != "" && report.ReportID != "" && !report.ReportedAt.IsZero() && !report.AcceptedAt.IsZero() &&
		report.HazardType != "" && report.Description != ""
}

type Store struct {
	db *sql.DB
}

func OpenStore(ctx context.Context, path string) (*Store, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(FULL)")
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
	for _, statement := range []string{
		`CREATE TABLE IF NOT EXISTS field_reports (
			client_id TEXT NOT NULL,
			report_id TEXT NOT NULL,
			message_id TEXT NOT NULL,
			content_sha256 TEXT NOT NULL,
			reported_at TEXT NOT NULL,
			hazard_type TEXT NOT NULL,
			latitude REAL NOT NULL,
			longitude REAL NOT NULL,
			description TEXT NOT NULL,
			related_hazard_id TEXT,
			accepted_at TEXT NOT NULL,
			stored_at TEXT NOT NULL,
			deliveries INTEGER NOT NULL DEFAULT 1,
			PRIMARY KEY (client_id, report_id)
		)`,
		`CREATE TABLE IF NOT EXISTS report_conflicts (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			client_id TEXT NOT NULL,
			report_id TEXT NOT NULL,
			content_sha256 TEXT NOT NULL,
			payload BLOB NOT NULL,
			seen_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS rejected_reports (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			message_id TEXT NOT NULL,
			reason TEXT NOT NULL,
			payload BLOB NOT NULL,
			received_at TEXT NOT NULL
		)`,
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

// The first copy of (client_id, report_id) wins; later copies are counted, never stored twice.
func (store *Store) Apply(ctx context.Context, messageID string, report Report, payload []byte) (outcome Outcome, err error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	digest := report.contentDigest()
	result, err := tx.ExecContext(ctx, `INSERT INTO field_reports (client_id, report_id, message_id, content_sha256, reported_at,
			hazard_type, latitude, longitude, description, related_hazard_id, accepted_at, stored_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (client_id, report_id) DO NOTHING`,
		report.ClientID, report.ReportID, messageID, digest, report.ReportedAt.UTC().Format(time.RFC3339Nano), report.HazardType,
		report.Latitude, report.Longitude, report.Description, report.RelatedHazardID, report.AcceptedAt.UTC().Format(time.RFC3339Nano), now)
	if err != nil {
		return "", err
	}
	if inserted, err := result.RowsAffected(); err != nil {
		return "", err
	} else if inserted == 1 {
		return Stored, tx.Commit()
	}
	var stored string
	if err = tx.QueryRowContext(ctx, "SELECT content_sha256 FROM field_reports WHERE client_id = ? AND report_id = ?",
		report.ClientID, report.ReportID).Scan(&stored); err != nil {
		return "", err
	}
	outcome = Duplicate
	if stored == digest {
		_, err = tx.ExecContext(ctx, "UPDATE field_reports SET deliveries = deliveries + 1 WHERE client_id = ? AND report_id = ?",
			report.ClientID, report.ReportID)
	} else {
		outcome = Conflict
		_, err = tx.ExecContext(ctx, `INSERT INTO report_conflicts (client_id, report_id, content_sha256, payload, seen_at)
			VALUES (?, ?, ?, ?, ?)`, report.ClientID, report.ReportID, digest, payload, now)
	}
	if err != nil {
		return "", err
	}
	return outcome, tx.Commit()
}

func (store *Store) Reject(ctx context.Context, messageID, reason string, payload []byte) error {
	_, err := store.db.ExecContext(ctx, "INSERT INTO rejected_reports (message_id, reason, payload, received_at) VALUES (?, ?, ?, ?)",
		messageID, reason, payload, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

type StoredReport struct {
	Seq        int64  `json:"seq"`
	ClientID   string `json:"client_id"`
	ReportID   string `json:"report_id"`
	MessageID  string `json:"message_id"`
	HazardType string `json:"hazard_type"`
	ReportedAt string `json:"reported_at"`
	AcceptedAt string `json:"accepted_at"`
	StoredAt   string `json:"stored_at"`
	Deliveries int    `json:"deliveries"`
}

type ReportPage struct {
	Items     []StoredReport `json:"items"`
	NextAfter *int64         `json:"next_after"`
	Stored    int            `json:"stored"`
	Conflicts int            `json:"conflicts"`
	Rejected  int            `json:"rejected"`
}

var errInvalidPage = errors.New("invalid page")

func (store *Store) Page(ctx context.Context, after int64, limit int) (ReportPage, error) {
	if after < 0 || limit < 1 || limit > 1000 {
		return ReportPage{}, errInvalidPage
	}
	page := ReportPage{Items: []StoredReport{}}
	if err := store.db.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM field_reports), (SELECT count(*) FROM report_conflicts),
		(SELECT count(*) FROM rejected_reports)`).Scan(&page.Stored, &page.Conflicts, &page.Rejected); err != nil {
		return ReportPage{}, err
	}
	rows, err := store.db.QueryContext(ctx, `SELECT rowid, client_id, report_id, message_id, hazard_type, reported_at, accepted_at,
		stored_at, deliveries FROM field_reports WHERE rowid > ? ORDER BY rowid LIMIT ?`, after, limit+1)
	if err != nil {
		return ReportPage{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var item StoredReport
		if err := rows.Scan(&item.Seq, &item.ClientID, &item.ReportID, &item.MessageID, &item.HazardType, &item.ReportedAt,
			&item.AcceptedAt, &item.StoredAt, &item.Deliveries); err != nil {
			return ReportPage{}, err
		}
		page.Items = append(page.Items, item)
	}
	if err := rows.Err(); err != nil {
		return ReportPage{}, err
	}
	if len(page.Items) > limit {
		page.Items = page.Items[:limit]
		next := page.Items[limit-1].Seq
		page.NextAfter = &next
	}
	return page, nil
}
