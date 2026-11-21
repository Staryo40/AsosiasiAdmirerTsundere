package aggregator

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/jackc/pgx/v5"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/logging"
)

var errStalePollLease = errors.New("a newer poll lease term has claimed the store")

type pollLeaseContextKey struct{}

func withPollLease(ctx context.Context, token pollLeaseToken) context.Context {
	return context.WithValue(ctx, pollLeaseContextKey{}, token)
}

func pollLeaseFromContext(ctx context.Context) (pollLeaseToken, bool) {
	token, ok := ctx.Value(pollLeaseContextKey{}).(pollLeaseToken)
	return token, ok
}

// A new term replaces any older one. A different lease UID means the Lease object was recreated,
// so its counter restarted and cannot be compared.
const claimPollFenceQuery = `INSERT INTO poll_fence (singleton, lease_uid, generation, holder) VALUES (true, $1, $2, $3)
	ON CONFLICT (singleton) DO UPDATE SET lease_uid = EXCLUDED.lease_uid, generation = EXCLUDED.generation,
		holder = EXCLUDED.holder, claimed_at = now()
	WHERE poll_fence.lease_uid <> EXCLUDED.lease_uid OR poll_fence.generation < EXCLUDED.generation
		OR (poll_fence.generation = EXCLUDED.generation AND poll_fence.holder = EXCLUDED.holder)`

func (store *Store) claimPollFence(ctx context.Context, token pollLeaseToken) error {
	ctx, cancel := context.WithTimeout(ctx, store.writeTimeout)
	defer cancel()
	tag, err := store.write.Exec(ctx, claimPollFenceQuery, token.leaseUID, token.generation, token.holder)
	if err != nil {
		return storeError("claim poll fence", err)
	}
	if tag.RowsAffected() == 0 {
		return errStalePollLease
	}
	return nil
}

// FOR SHARE makes a takeover claim wait for this transaction, so no write commits after a newer claim.
func checkPollFence(ctx context.Context, tx pgx.Tx, token pollLeaseToken) error {
	var leaseUID, holder string
	var generation int64
	err := tx.QueryRow(ctx, `SELECT lease_uid, generation, holder FROM poll_fence WHERE singleton FOR SHARE`).
		Scan(&leaseUID, &generation, &holder)
	if errors.Is(err, pgx.ErrNoRows) {
		return errStalePollLease
	}
	if err != nil {
		return storeError("check poll fence", err)
	}
	if leaseUID != token.leaseUID || generation != token.generation || holder != token.holder {
		return errStalePollLease
	}
	return nil
}

func (store *Store) beginFencedPollWrite(ctx context.Context, token pollLeaseToken) (pgx.Tx, error) {
	tx, err := store.write.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, err
	}
	if err := checkPollFence(ctx, tx, token); err != nil {
		_ = rollbackTransaction(ctx, tx)
		return nil, err
	}
	return tx, nil
}

// Runs one write, fenced when the caller polls under a lease term.
func (store *Store) execPollWrite(ctx context.Context, query string, args ...any) error {
	token, ok := pollLeaseFromContext(ctx)
	if !ok {
		_, err := store.write.Exec(ctx, query, args...)
		return err
	}
	tx, err := store.beginFencedPollWrite(ctx, token)
	if err != nil {
		return err
	}
	defer func() { _ = rollbackTransaction(ctx, tx) }()
	if _, err := tx.Exec(ctx, query, args...); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// One lease term: claim the store first, then poll until the term ends.
func leadPolls(ctx context.Context, store *Store, logger *slog.Logger, token pollLeaseToken, pollers []func(context.Context)) {
	if err := store.claimPollFence(ctx, token); err != nil {
		logging.Event(ctx, logger, logging.Record{Level: slog.LevelWarn, Event: "poll_fence_claim_failed", Message: "this lease term may not store poll results"},
			"lease_generation", token.generation, "category", pollCategory(err))
		// Hold the term without polling; its next failed renewal ends it.
		<-ctx.Done()
		return
	}
	ctx = withPollLease(ctx, token)
	var group sync.WaitGroup
	for _, poller := range pollers {
		group.Go(func() { poller(ctx) })
	}
	group.Wait()
}
