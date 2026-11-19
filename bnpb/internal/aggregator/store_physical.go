package aggregator

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

const maxPhysicalCopyURLs = 6

func validatePhysicalCopyURLs(copyURLs, primaryURLs []string) error {
	if len(copyURLs) > maxPhysicalCopyURLs {
		return fmt.Errorf("audit copy database URLs: at most %d are allowed", maxPhysicalCopyURLs)
	}
	primaryTargets := make(map[physicalDatabaseTarget]struct{}, len(primaryURLs))
	for _, address := range primaryURLs {
		target, err := physicalDatabaseURLTarget(address)
		if err != nil {
			return fmt.Errorf("primary database URL is invalid")
		}
		primaryTargets[target] = struct{}{}
	}
	seen := make(map[physicalDatabaseTarget]struct{}, len(copyURLs))
	for index, address := range copyURLs {
		target, err := physicalDatabaseURLTarget(address)
		if err != nil {
			return fmt.Errorf("audit copy database URL %d is invalid", index)
		}
		if _, exists := seen[target]; exists {
			return fmt.Errorf("audit copy database URLs must be distinct")
		}
		seen[target] = struct{}{}
		if _, exists := primaryTargets[target]; exists {
			return fmt.Errorf("audit copy database URL %d duplicates a primary database", index)
		}
	}
	return nil
}

type physicalDatabaseTarget struct {
	host     string
	port     uint16
	database string
}

func physicalDatabaseURLTarget(address string) (physicalDatabaseTarget, error) {
	config, err := pgxpool.ParseConfig(address)
	if err != nil {
		return physicalDatabaseTarget{}, err
	}
	parsed, err := url.Parse(address)
	if err != nil || parsed.Hostname() == "" || parsed.Fragment != "" || parsed.Opaque != "" ||
		(parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") ||
		strings.TrimSpace(address) != address ||
		strings.IndexFunc(address, func(r rune) bool { return r < 0x20 || r == 0x7f }) >= 0 {
		return physicalDatabaseTarget{}, fmt.Errorf("invalid database URL")
	}
	return physicalDatabaseTarget{
		host: strings.ToLower(config.ConnConfig.Host), port: config.ConnConfig.Port,
		database: config.ConnConfig.Database,
	}, nil
}

func newPhysicalCopyPools(copyURLs []string, primaryURLs []string, logger *slog.Logger) ([]*pgxpool.Pool, error) {
	if err := validatePhysicalCopyURLs(copyURLs, primaryURLs); err != nil {
		return nil, err
	}
	pools := make([]*pgxpool.Pool, 0, len(copyURLs))
	for index, address := range copyURLs {
		pool, err := newLazyPool(address, 1, logger)
		if err != nil {
			closePools(pools)
			return nil, fmt.Errorf("initialize audit copy database pool %d", index)
		}
		pools = append(pools, pool)
	}
	return pools, nil
}

func closePools(pools []*pgxpool.Pool) {
	for _, pool := range pools {
		if pool != nil {
			pool.Close()
		}
	}
}

func newLazyPool(address string, maximum int, logger *slog.Logger) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(address)
	if err != nil {
		return nil, fmt.Errorf("invalid database URL")
	}
	if maximum < 1 {
		return nil, fmt.Errorf("database pool maximum must be positive")
	}
	config.MaxConns = int32(maximum)
	config.MinConns = 0
	config.MinIdleConns = 0
	config.ConnConfig.Tracer = databaseTracer{logger: logger}
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("initialize database pool")
	}
	return pool, nil
}

func (store *Store) physicalReadPools() []*pgxpool.Pool {
	if store == nil {
		return nil
	}
	pools := make([]*pgxpool.Pool, 0, 4+len(store.physicalCopies))
	if store.shards[0] != nil {
		for _, shard := range store.shards {
			if shard != nil && shard.read != nil {
				pools = append(pools, shard.read)
			}
		}
	} else if store.read != nil {
		pools = append(pools, store.read)
	}
	for _, pool := range store.physicalCopies {
		if pool != nil {
			pools = append(pools, pool)
		}
	}
	return pools
}

func (store *Store) verifyStartupResource(ctx context.Context) error {
	if store == nil {
		return fmt.Errorf("startup database resource is unavailable")
	}
	if !store.startupUnverified.Load() {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if store.read == nil || store.write == nil || len(store.authorityPools) == 0 || store.authorityPools[0] == nil {
		return fmt.Errorf("startup database resource is unavailable")
	}
	admin := store.authorityPools[0]
	if err := store.verifyRecoveredIdentity(ctx, admin); err != nil {
		return err
	}
	if err := validatePoolTarget(ctx, admin, store.write); err != nil {
		return storeError("verify recovered runtime target", err)
	}
	if err := validatePoolTarget(ctx, admin, store.read); err != nil {
		return storeError("verify recovered read target", err)
	}
	if err := validateRuntimeWriter(ctx, store.write); err != nil {
		return storeError("verify recovered runtime role", err)
	}
	if err := validateRuntimeWriter(ctx, store.read); err != nil {
		return storeError("verify recovered read role", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	store.startupUnverified.Store(false)
	return nil
}

func (store *Store) verifyRecoveredIdentity(ctx context.Context, admin *pgxpool.Pool) error {
	version, err := schemaVersion(admin.QueryRow(ctx, schemaHistoryQuery))
	if err != nil {
		return storeError("verify recovered database schema", err)
	}
	if version != currentSchemaVersion {
		return errSchemaVersion
	}
	var storedRole string
	var storedShard int16
	err = admin.QueryRow(ctx, `SELECT role, COALESCE(shard_id, -1)
		FROM store_identity WHERE singleton`).Scan(&storedRole, &storedShard)
	if err != nil {
		return storeError("verify recovered database identity", err)
	}
	expectedShard, hasExpectedShard := store.storeIdentity.shard()
	if storedRole != store.storeIdentity.role ||
		(hasExpectedShard && storedShard != expectedShard) || (!hasExpectedShard && storedShard != -1) {
		return fmt.Errorf("recovered database has a different store identity")
	}
	return nil
}
