package aggregator

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func (store *Store) beginMutation(ctx context.Context) (pgx.Tx, error) {
	if err := store.verifyStartupResource(ctx); err != nil {
		return nil, err
	}
	token, ok := ctx.Value(mutationContextKey{}).(mutationToken)
	if !ok || token.owner == "" || token.epoch < 1 {
		return nil, errStaleMutationAuthority
	}
	tx, err := store.write.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, storeError("begin guarded mutation", err)
	}
	fail := func(cause error) (pgx.Tx, error) {
		_ = rollbackTransaction(ctx, tx)
		return nil, cause
	}
	for _, expected := range token.guards {
		if expected.store != store {
			continue
		}
		_, err = tx.Exec(ctx, `SELECT begin_mutation_guard($1,$2,$3,$4)`,
			token.owner, token.epoch, expected.generation, expected.resource)
		if err != nil {
			var databaseError *pgconn.PgError
			if errors.As(err, &databaseError) && databaseError.Code == "42501" {
				return fail(errStaleMutationAuthority)
			}
			return fail(storeError("bind mutation guard", err))
		}
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		return tx, nil
	}
	return fail(errStaleMutationAuthority)
}
