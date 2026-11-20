package aggregator

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var errReplicatedDurability = errors.New("replicated PostgreSQL durability is unavailable")

// Keep the connection until the post-commit WAL barrier is confirmed. A pool Tx
// releases it at Commit, too early to verify an uncertain synchronous reply.
type replicatedMutation struct {
	pgx.Tx
	connection *pgxpool.Conn
}

func (store *Store) beginReplicatedMutation(ctx context.Context) (pgx.Tx, error) {
	connection, err := store.write.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := connection.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		connection.Release()
		return nil, err
	}
	mutation := &replicatedMutation{Tx: tx, connection: connection}
	var configured bool
	err = tx.QueryRow(ctx, `SELECT NOT pg_is_in_recovery()
		AND current_setting('fsync')::boolean
		AND current_setting('full_page_writes')::boolean
		AND current_setting('synchronous_standby_names') <> ''
		AND EXISTS (SELECT 1 FROM pg_stat_replication r
			JOIN pg_replication_slots s ON s.active_pid = r.pid AND s.slot_type = 'physical'
			WHERE r.state = 'streaming' AND r.sync_state IN ('sync', 'quorum'))`).Scan(&configured)
	if err == nil && !configured {
		err = errReplicatedDurability
	}
	if err == nil {
		_, err = tx.Exec(ctx, "SET LOCAL synchronous_commit = 'on'")
	}
	if err != nil {
		_ = rollbackTransaction(ctx, mutation)
		return nil, err
	}
	return mutation, nil
}

func (tx *replicatedMutation) release() {
	if tx.connection != nil {
		tx.connection.Release()
		tx.connection = nil
	}
}

func (tx *replicatedMutation) Rollback(ctx context.Context) error {
	if tx.connection == nil {
		return pgx.ErrTxClosed
	}
	defer tx.release()
	return tx.Tx.Rollback(ctx)
}

func (tx *replicatedMutation) Commit(ctx context.Context) error {
	if tx.connection == nil {
		return pgx.ErrTxClosed
	}
	defer tx.release()
	if err := tx.Tx.Commit(ctx); err != nil {
		return err
	}
	// A cancelled COMMIT may already be locally visible. An idempotent retry
	// can then have no WAL of its own, so its native COMMIT need not wait for
	// replication. Also cover synchronous_standby_names changing during work.
	var barrier string
	if err := tx.connection.QueryRow(ctx, "SELECT pg_current_wal_insert_lsn()::text").Scan(&barrier); err != nil {
		return err
	}
	var ticker *time.Ticker
	defer func() {
		if ticker != nil {
			ticker.Stop()
		}
	}()
	for {
		var durable bool
		err := tx.connection.QueryRow(ctx, `SELECT current_setting('fsync')::boolean
			AND current_setting('full_page_writes')::boolean
			AND EXISTS (SELECT 1 FROM pg_stat_replication r
				JOIN pg_replication_slots s ON s.active_pid = r.pid AND s.slot_type = 'physical'
				WHERE r.state = 'streaming' AND r.sync_state IN ('sync', 'quorum')
				AND r.flush_lsn >= $1::pg_lsn)`, barrier).Scan(&durable)
		if err != nil {
			return err
		}
		if durable {
			return nil
		}
		if ticker == nil {
			ticker = time.NewTicker(10 * time.Millisecond)
		}
		select {
		case <-ctx.Done():
			return errors.Join(errReplicatedDurability, ctx.Err())
		case <-ticker.C:
		}
	}
}
