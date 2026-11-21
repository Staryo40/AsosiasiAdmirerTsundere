
package aggregator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var errStartupDatabaseUnavailable = errors.New("database unavailable during startup")

func isStartupConnectionUnavailable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var networkError net.Error
	if errors.As(err, &networkError) {
		return true
	}
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) {
		return strings.HasPrefix(postgresError.Code, "08") || postgresError.Code == "57P03"
	}
	return false
}

type storeIdentity struct {
	role    string
	shardID *int16
}

func (identity storeIdentity) shard() (int16, bool) {
	if identity.shardID == nil {
		return 0, false
	}
	return *identity.shardID, true
}

func (store *Store) shardStore(ctx context.Context, id int16) (*Store, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if id < 0 || id >= logicalShardCount {
		return nil, fmt.Errorf("logical shard ID out of range")
	}
	if store.shards[0] == nil {
		return store, nil
	}
	child := store.shards[id]
	if child == nil {
		return nil, fmt.Errorf("logical shard store is unavailable")
	}
	return child, nil
}

func configureStoreIdentity(ctx context.Context, tx pgx.Tx, identity storeIdentity) error {
	role := identity.role
	configuredShard := int16(-1)
	var shardValue any
	if identity.shardID != nil {
		configuredShard = *identity.shardID
		shardValue = *identity.shardID
	}
	if _, err := tx.Exec(ctx, `INSERT INTO store_identity (singleton, role, shard_id)
		VALUES (true, $1, $2) ON CONFLICT (singleton) DO NOTHING`, role, shardValue); err != nil {
		return err
	}
	var storedRole string
	var storedShard int16
	if err := tx.QueryRow(ctx, "SELECT role, COALESCE(shard_id, -1) FROM store_identity WHERE singleton").Scan(&storedRole, &storedShard); err != nil {
		return err
	}
	if storedRole != role || storedShard != configuredShard {
		return fmt.Errorf("database is configured for a different store identity")
	}
	var containsWrongData bool
	query := `SELECT EXISTS (
		SELECT 1 FROM hazard_events
		UNION ALL SELECT 1 FROM source_seismic
		UNION ALL SELECT 1 FROM source_warnings
	)`
	if role == "shard" {
		query = `SELECT EXISTS (
			SELECT 1 FROM hazard_events WHERE logical_shard_id <> $1
			UNION ALL SELECT 1 FROM source_seismic WHERE logical_shard_id <> $1
			UNION ALL SELECT 1 FROM source_warnings WHERE logical_shard_id <> $1
		)`
		if err := tx.QueryRow(ctx, query, configuredShard).Scan(&containsWrongData); err != nil {
			return err
		}
	} else if err := tx.QueryRow(ctx, query).Scan(&containsWrongData); err != nil {
		return err
	}
	if containsWrongData {
		return fmt.Errorf("database contains canonical rows incompatible with its configured identity")
	}
	return nil
}

func openUnavailableShardStore(cfg Config, logger *slog.Logger, identity storeIdentity) (*Store, error) {
	if identity.shardID != nil {
		expectedShard := *identity.shardID
		identity.shardID = &expectedShard
	}
	store := &Store{
		readTimeout: cfg.DBReadTimeout, writeTimeout: cfg.DBWriteTimeout,
		chunkRecords: cfg.IngestChunkRecords, maxBytes: cfg.MaxResponseBytes,
		storeIdentity: identity, pvmbg: cfg.PVMBGAdaptation,
	}
	store.startupUnverified.Store(true)
	admin, err := newLazyPool(cfg.DatabaseURL, cfg.DBWriteMaxConns, logger)
	if err != nil {
		return nil, err
	}
	writerURL := cfg.WriterURL
	if writerURL == "" {
		writerURL = cfg.DatabaseURL
	}
	store.write, err = newLazyPool(writerURL, cfg.DBWriteMaxConns, logger)
	if err != nil {
		admin.Close()
		return nil, err
	}
	store.read, err = newLazyPool(writerURL, cfg.DBReadMaxConns, logger)
	if err != nil {
		admin.Close()
		store.write.Close()
		return nil, err
	}
	store.authorityPools = append(store.authorityPools, admin)
	return store, nil
}
