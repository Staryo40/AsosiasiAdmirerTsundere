package aggregator

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/logging"
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

//go:embed migrations/005_coastal.sql
var coastalSchema string

//go:embed migrations/006_canonical_enums.sql
var m2EnumSchema string

//go:embed migrations/007_shards.sql
var shardSchema string

//go:embed migrations/008_placement_guards.sql
var placementGuardSchema string

//go:embed migrations/009_handled_inputs.sql
var handledInputsSchema string

//go:embed migrations/010_mutation_fence.sql
var mutationFenceSchema string

//go:embed migrations/011_audit_indexes.sql
var auditIndexSchema string

//go:embed migrations/012_poll_receipts.sql
var pollReceiptSchema string

//go:embed migrations/013_live_mutation_generation.sql
var liveMutationGenerationSchema string

//go:embed migrations/014_shard_repair.sql
var shardRepairSchema string

//go:embed migrations/014_receipt_binding.sql
var receiptBindingSchema string

//go:embed migrations/015_failed_messages.sql
var failedMessagesSchema string

//go:embed migrations/015_source_wildfire.sql
var sourceWildfireSchema string

//go:embed migrations/016_poll_fence.sql
var pollFenceSchema string

const currentSchemaVersion = 15

const schemaHistoryQuery = `SELECT COALESCE(max(version), 0), count(*),
    COALESCE(min(version), 0) FROM schema_migrations`

var errSchemaVersion = errors.New("unsupported canonical schema version")

type Store struct {
	read, write               *pgxpool.Pool
	authorityPools            []*pgxpool.Pool
	authority                 *PollAuthority
	readTimeout, writeTimeout time.Duration
	chunkRecords              int
	maxBytes                  int64
	shards                    [logicalShardCount]*Store
	storeIdentity             storeIdentity
	startupUnverified         atomic.Bool
	physicalCopies            []*pgxpool.Pool
	coastalStale              time.Duration
	coastal                   coastalIngest
	pantaiClient              *pantaiClient
	pvmbg                     pvmbgAdaptation
	pollLeader                interface{ Leading() bool }
}

func OpenStore(ctx context.Context, cfg Config, logger *slog.Logger) (*Store, error) {
	routed, err := validateShardURLs(cfg)
	if err != nil {
		return nil, err
	}
	startupBudget := cfg.DBWriteTimeout
	if routed {
		startupBudget *= time.Duration(1 + logicalShardCount)
	}
	identity := storeIdentity{}
	if routed {
		identity.role = "control"
	}
	controlCtx, cancel := context.WithTimeout(ctx, startupBudget)
	store, err := openStore(controlCtx, cfg, logger, identity)
	cancel()
	if err != nil {
		return nil, err
	}
	if routed {
		if err := openShardStores(ctx, store, cfg, logger); err != nil {
			store.Close()
			return nil, err
		}
	}
	primaryURLs := []string{cfg.DatabaseURL}
	if cfg.WriterURL != "" {
		primaryURLs = append(primaryURLs, cfg.WriterURL)
	}
	for index := range cfg.ShardURLs {
		if cfg.ShardURLs[index] != "" {
			primaryURLs = append(primaryURLs, cfg.ShardURLs[index])
		}
		if cfg.ShardWriterURLs[index] != "" {
			primaryURLs = append(primaryURLs, cfg.ShardWriterURLs[index])
		}
	}
	store.physicalCopies, err = newPhysicalCopyPools(cfg.Audit.CopyURLs, primaryURLs, logger)
	if err != nil {
		store.Close()
		return nil, err
	}
	return store, nil
}

func openShardStores(ctx context.Context, parent *Store, cfg Config, logger *slog.Logger) error {

	var workers sync.WaitGroup
	var failures [logicalShardCount]error
	// Shards may share a PostgreSQL database during development and serialize migrations there.
	startupBudget := cfg.DBWriteTimeout * time.Duration(logicalShardCount)
	for shardID, address := range cfg.ShardURLs {
		workers.Go(func() {
			shardConfig := cfg
			shardConfig.DatabaseURL = address
			shardConfig.WriterURL = cfg.ShardWriterURLs[shardID]
			childID := int16(shardID)
			identity := storeIdentity{role: "shard", shardID: &childID}
			shardCtx, cancel := context.WithTimeout(ctx, startupBudget)
			defer cancel()
			child, err := openStore(shardCtx, shardConfig, logger, identity)
			if ctx.Err() == nil && (errors.Is(err, errStartupDatabaseUnavailable) || errors.Is(err, context.DeadlineExceeded)) {
				child, err = openUnavailableShardStore(shardConfig, logger, identity)
			}
			parent.shards[shardID], failures[shardID] = child, err
		})
	}
	workers.Wait()
	if err := ctx.Err(); err != nil {
		return err
	}
	for shardID, err := range failures {
		if err != nil {
			return fmt.Errorf("initialize shard %d: %w", shardID, err)
		}
	}
	return nil
}

func openStore(ctx context.Context, cfg Config, logger *slog.Logger, identity storeIdentity) (*Store, error) {
	var bootstrap *placementSnapshot
	var err error
	if identity.role != "shard" {
		bootstrap, err = parsePlacementBootstrap(cfg.ShardPlacement)
		if err != nil {
			return nil, err
		}
	}
	if identity.role != "" && cfg.WriterURL == "" {
		return nil, errors.New("routed store requires a separate runtime writer connection")
	}
	if identity.shardID != nil {
		expectedShard := *identity.shardID
		identity.shardID = &expectedShard
	}
	store := &Store{readTimeout: cfg.DBReadTimeout, writeTimeout: cfg.DBWriteTimeout,
		chunkRecords: cfg.IngestChunkRecords, maxBytes: cfg.MaxResponseBytes, coastalStale: cfg.SourceStaleAfter, storeIdentity: identity,
		pvmbg: cfg.PVMBGAdaptation}
	connect := func(address string, maximum int) (*pgxpool.Pool, error) {
		connectCtx, cancel := context.WithTimeout(ctx, cfg.DBWriteTimeout)
		defer cancel()
		return openPool(connectCtx, address, maximum, logger)
	}
	admin, err := connect(cfg.DatabaseURL, cfg.DBWriteMaxConns)
	if err == nil {
		store.authorityPools = append(store.authorityPools, admin)
		err = store.migrate(ctx, admin, bootstrap, identity)
	}
	writerURL := cfg.WriterURL
	if writerURL == "" {
		writerURL = cfg.DatabaseURL
	}
	if err == nil {
		store.write, err = connect(writerURL, cfg.DBWriteMaxConns)
	}
	if err == nil && cfg.WriterURL != "" {
		err = validatePoolTarget(ctx, admin, store.write)
	}
	if err == nil && cfg.WriterURL != "" {
		err = validateRuntimeWriter(ctx, store.write)
	}
	if err == nil && identity.role == "control" {
		err = grantReceiptBindingPrivileges(ctx, admin, store.write)
	}
	if err == nil && identity.role != "shard" {
		err = grantFailedMessagePrivileges(ctx, admin, store.write)
	}
	if err == nil {
		store.read, err = connect(writerURL, cfg.DBReadMaxConns)
	}
	if err == nil && bootstrap != nil {
		var persisted placementSnapshot
		persisted, err = store.placement(ctx)
		if err == nil {
			logging.Event(ctx, logger, logging.Record{Level: slog.LevelInfo, Event: "placement_loaded", Message: "configured placement is persisted"},
				"placement_revision", persisted.Revision, "placement_copies", len(persisted.Copies))
		}
	}
	if err != nil {
		unavailable := isStartupConnectionUnavailable(err)
		store.Close()
		if unavailable {
			return nil, errStartupDatabaseUnavailable
		}
		return nil, storeError("initialize store", err)
	}
	if identity.role == "" {
		admin.Close()
		store.authorityPools = nil
	}
	return store, nil
}

func grantReceiptBindingPrivileges(ctx context.Context, admin, writer *pgxpool.Pool) error {
	var role string
	if err := writer.QueryRow(ctx, "SELECT current_user").Scan(&role); err != nil {
		return err
	}
	_, err := admin.Exec(ctx, "GRANT SELECT, INSERT ON receipt_bindings TO "+pgx.Identifier{role}.Sanitize())
	return err
}

// Concurrent GRANTs on one table can fail with "tuple concurrently updated"; the migration lock serializes them.
func grantFailedMessagePrivileges(ctx context.Context, admin, writer *pgxpool.Pool) (err error) {
	var role string
	if err := writer.QueryRow(ctx, "SELECT current_user").Scan(&role); err != nil {
		return err
	}
	tx, err := admin.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if cleanupErr := rollbackTransaction(ctx, tx); cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
		}
	}()
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(4031, 0)"); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "GRANT SELECT, INSERT, UPDATE ON failed_messages TO "+pgx.Identifier{role}.Sanitize()); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "GRANT SELECT, INSERT, UPDATE ON poll_fence TO "+pgx.Identifier{role}.Sanitize()); err != nil {
		return err
	}
	return tx.Commit(ctx)
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
func validatePoolTarget(ctx context.Context, admin, writer *pgxpool.Pool) error {
	const targetQuery = `SELECT current_database(), current_schema(),
		COALESCE(inet_server_addr()::text, ''), COALESCE(inet_server_port(), 0)`
	var adminDB, adminSchema, adminNode string
	var adminPort int
	if err := admin.QueryRow(ctx, targetQuery).Scan(&adminDB, &adminSchema, &adminNode, &adminPort); err != nil {
		return err
	}
	var writerDB, writerSchema, writerNode string
	var writerPort int
	if err := writer.QueryRow(ctx, targetQuery).Scan(&writerDB, &writerSchema, &writerNode, &writerPort); err != nil {
		return err
	}
	if adminDB == "" || adminSchema == "" || adminNode == "" ||
		adminDB != writerDB || adminSchema != writerSchema || adminNode != writerNode || adminPort != writerPort {
		return errors.New("runtime writer does not reach the migration database, schema, and node")
	}
	return nil
}
func validateRuntimeWriter(ctx context.Context, pool *pgxpool.Pool) error {
	var isOwner bool
	err := pool.QueryRow(ctx, `SELECT r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR
		r.rolreplication OR r.rolbypassrls OR d.datdba = r.oid OR EXISTS (
			SELECT 1 FROM pg_namespace n
			WHERE n.nspowner = r.oid AND n.nspname = ANY(current_schemas(false))
		) OR EXISTS (
			SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE c.relowner = r.oid AND c.relkind IN ('r', 'p', 'v', 'm', 'S')
				AND n.nspname = ANY(current_schemas(false))
		)
		FROM pg_roles r JOIN pg_database d ON d.datname = current_database()
		WHERE r.rolname = current_user`).Scan(&isOwner)
	if err != nil {
		return err
	}
	if isOwner {
		return errors.New("runtime database credentials must not own the database or canonical objects")
	}
	return nil
}

func pingPool(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) error {
	// pgx Pool.Ping bypasses QueryTracer, so its complete startup probe needs a separate measurement.
	ctx = databaseTraceContext(ctx)
	err := pool.Ping(ctx)
	databaseTracer{logger: logger}.recordCompletion(ctx, "ping", err)
	return err
}

func (store *Store) Close() {
	for _, shard := range store.shards {
		if shard != nil {
			shard.Close()
		}
	}
	if store.read != nil {
		store.read.Close()
	}
	if store.write != nil {
		store.write.Close()
	}
	for _, pool := range store.authorityPools {
		if pool != nil {
			pool.Close()
		}
	}
	closePools(store.physicalCopies)
}

func (store *Store) Ready(ctx context.Context) error {
	return store.readiness(ctx).err()
}

func (store *Store) migrate(ctx context.Context, admin *pgxpool.Pool, bootstrap *placementSnapshot, identity storeIdentity) (err error) {
	tx, err := admin.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
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
	if identity.role != "" {
		if err = configureStoreIdentity(ctx, tx, identity); err != nil {
			return err
		}
	}
	// Repair metadata must not change the canonical version required to recover an offline shard.
	if identity.role == "control" || identity.role == "" {
		if _, err := tx.Exec(ctx, shardRepairSchema); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, failedMessagesSchema); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, pollFenceSchema); err != nil {
			return err
		}
	}
	if bootstrap != nil {
		if err := bootstrapPlacement(ctx, tx, *bootstrap); err != nil {
			return err
		}
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
	}{{2, outboxIdentitySchema}, {3, snapshotIndexSchema}, {4, outboxSequenceSchema},
		{5, coastalSchema}, {6, m2EnumSchema}, {7, shardSchema}, {8, placementGuardSchema}, {9, handledInputsSchema},
		{10, mutationFenceSchema}, {11, auditIndexSchema}, {12, pollReceiptSchema}, {13, liveMutationGenerationSchema},
		{14, receiptBindingSchema}, {15, sourceWildfireSchema}} {
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

const rollbackTimeout = time.Second

type rollbackDeadlineKey struct{}

// Native pgx cancellation does not roll back a transaction; cleanup needs its own bounded context.
func rollbackTransaction(ctx context.Context, tx pgx.Tx) error {
	deadline := time.Now().Add(rollbackTimeout)
	if requestDeadline, ok := ctx.Value(rollbackDeadlineKey{}).(time.Time); ok && requestDeadline.Before(deadline) {
		deadline = requestDeadline
	}
	cleanupCtx, cancel := context.WithDeadline(context.WithoutCancel(ctx), deadline)
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
	if errors.Is(err, errStaleMutationAuthority) {
		return fmt.Errorf("%s: %w", operation, errStaleMutationAuthority)
	}
	if errors.Is(err, errSchemaVersion) {
		return fmt.Errorf("%s: %w", operation, errSchemaVersion)
	}
	if errors.Is(err, errStalePollLease) {
		return fmt.Errorf("%s: %w", operation, errStalePollLease)
	}
	if errors.Is(err, errPlacementChanged) {
		return fmt.Errorf("%s: %w", operation, errPlacementChanged)
	}
	if errors.Is(err, errInvalidPlacement) {
		return fmt.Errorf("%s: %w", operation, errInvalidPlacement)
	}
	if errors.Is(err, errReplicatedDurability) {
		return fmt.Errorf("%s: %w", operation, errReplicatedDurability)
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
