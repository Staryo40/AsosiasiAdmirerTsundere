package satellite

import (
	"context"
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

type Store struct {
	db *sql.DB
}

func OpenStore(ctx context.Context, path string) (*Store, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(FULL)")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS checkpoint (
		id INTEGER PRIMARY KEY CHECK (id = 1),
		next_offset INTEGER NOT NULL CHECK (next_offset >= 0)
	)`); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate sqlite: %w", err)
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
