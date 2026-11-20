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
	var tx pgx.Tx
	var err error
	if store.storeIdentity.role != "" {
		tx, err = store.beginReplicatedMutation(ctx)
	} else {
		tx, err = store.write.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	}
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

const mutationGenerationQuery = `SELECT NOT pg_is_in_recovery(),
	state='active' AND owner=$1 AND epoch=$2,
	CASE WHEN pg_is_in_recovery() THEN false ELSE
		pg_generation=$3 AND resource_identity=$4
		AND resource_identity=live_mutation_resource('mutation_guard'::regclass)
	END FROM mutation_guard WHERE singleton`

func verifyMutationGeneration(row pgx.Row) error {
	var primary, ownerMatches, generationMatches bool
	if err := row.Scan(&primary, &ownerMatches, &generationMatches); err != nil {
		return storeError("verify PostgreSQL mutation generation", err)
	}
	if !primary {
		return errAuthorityUnavailable
	}
	if !ownerMatches {
		return errStaleMutationAuthority
	}
	if !generationMatches {
		return errMutationGenerationChanged
	}
	return nil
}
