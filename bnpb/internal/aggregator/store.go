package aggregator

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/001_initial.sql
var initialSchema string

//go:embed migrations/002_outbox_identity.sql
var outboxIdentitySchema string

//go:embed migrations/003_snapshot_indexes.sql
var snapshotIndexSchema string

//go:embed migrations/004_outbox_sequence.sql
var outboxSequenceSchema string

const currentSchemaVersion = 4

const schemaHistoryQuery = `SELECT COALESCE(max(version), 0), count(*),
    COALESCE(min(version), 0) FROM schema_migrations`

var errSchemaVersion = errors.New("unsupported canonical schema version")

type Store struct {
	read, write               *pgxpool.Pool
	readTimeout, writeTimeout time.Duration
	chunkRecords              int
	maxBytes                  int64
}

func OpenStore(ctx context.Context, cfg Config, logger *slog.Logger) (*Store, error) {
	ctx, cancel := context.WithTimeout(ctx, cfg.DBWriteTimeout)
	defer cancel()
	store := &Store{readTimeout: cfg.DBReadTimeout, writeTimeout: cfg.DBWriteTimeout,
		chunkRecords: cfg.IngestChunkRecords, maxBytes: cfg.MaxResponseBytes}
	var err error
	if store.write, err = openPool(ctx, cfg.DatabaseURL, cfg.DBWriteMaxConns, logger); err != nil {
		return nil, storeError("open write pool", err)
	}
	if err = store.migrate(ctx); err == nil {
		store.read, err = openPool(ctx, cfg.DatabaseURL, cfg.DBReadMaxConns, logger)
	}
	if err != nil {
		store.Close()
		return nil, storeError("initialize canonical store", err)
	}
	return store, nil
}

func openPool(ctx context.Context, address string, maximum int, logger *slog.Logger) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(address)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns, cfg.MinConns, cfg.MinIdleConns = int32(maximum), 0, 0
	cfg.ConnConfig.Tracer = databaseTracer{logger: logger}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pingPool(ctx, pool, logger); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

func pingPool(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) error {
	// pgx Pool.Ping bypasses QueryTracer, so its complete startup probe needs a separate measurement.
	ctx = databaseTraceContext(ctx)
	err := pool.Ping(ctx)
	databaseTracer{logger: logger}.recordCompletion(ctx, "ping", err)
	return err
}

func (store *Store) Close() {
	if store.read != nil {
		store.read.Close()
	}
	if store.write != nil {
		store.write.Close()
	}
}

func (store *Store) Ready(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, store.readTimeout)
	defer cancel()
	version, err := schemaVersion(store.read.QueryRow(ctx, schemaHistoryQuery))
	if err == nil && version != currentSchemaVersion {
		err = errSchemaVersion
	}
	return storeError("canonical store readiness", err)
}

func (store *Store) migrate(ctx context.Context) (err error) {
	tx, err := store.write.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() {
		if cleanupErr := rollbackTransaction(ctx, tx); cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
		}
	}()
	if err = migrateSchema(ctx, tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func migrateSchema(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(4031, 0);
        CREATE TABLE IF NOT EXISTS schema_migrations (
        version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	version, err := schemaVersion(tx.QueryRow(ctx, schemaHistoryQuery))
	if err != nil {
		return err
	}
	return applyPendingMigrations(ctx, tx, version)
}

func schemaVersion(row pgx.Row) (int, error) {
	var version, count, minimum int
	if err := row.Scan(&version, &count, &minimum); err != nil {
		return 0, err
	}
	if version > currentSchemaVersion || count != version || (count > 0 && minimum != 1) {
		return 0, errSchemaVersion
	}
	return version, nil
}

func applyPendingMigrations(ctx context.Context, tx pgx.Tx, version int) error {
	if version == 0 {
		if err := applyInitialSchema(ctx, tx); err != nil {
			return err
		}
	}
	for _, migration := range []struct {
		version int
		sql     string
	}{{2, outboxIdentitySchema}, {3, snapshotIndexSchema}, {4, outboxSequenceSchema}} {
		if version < migration.version {
			if _, err := tx.Exec(ctx, migration.sql); err != nil {
				return err
			}
		}
	}
	return nil
}

func applyInitialSchema(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, initialSchema); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO volcano_reference
        SELECT key, value->>'name', (value->>'latitude')::float8,
               (value->>'longitude')::float8, value FROM jsonb_each($1::jsonb)`, volcanoData); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, "INSERT INTO schema_migrations (version) VALUES (1)")
	return err
}

// Native pgx cancellation does not roll back a transaction; cleanup needs its own bounded context.
func rollbackTransaction(ctx context.Context, tx pgx.Tx) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer cancel()
	if err := tx.Rollback(cleanupCtx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		return storeError("rollback", err)
	}
	return nil
}

// Driver errors may contain a DSN or rejected source values. Expose only the operation and SQLSTATE.
func storeError(operation string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, errSchemaVersion) {
		return fmt.Errorf("%s: %w", operation, errSchemaVersion)
	}
	if errors.Is(err, context.Canceled) {
		return fmt.Errorf("%s: %w", operation, context.Canceled)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%s: %w", operation, context.DeadlineExceeded)
	}
	var postgresErr *pgconn.PgError
	if errors.As(err, &postgresErr) {
		return fmt.Errorf("%s: SQLSTATE %s", operation, postgresErr.Code)
	}
	return fmt.Errorf("%s: database operation failed", operation)
}
