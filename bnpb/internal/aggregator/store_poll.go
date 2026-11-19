
package aggregator

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

type pollPosition struct {
	checkpoint  *time.Time
	observation pollObservation
}

func (store *Store) pollPosition(ctx context.Context, endpoint Endpoint) (pollPosition, error) {
	ctx, cancel := context.WithTimeout(ctx, store.writeTimeout)
	defer cancel()
	var position pollPosition
	err := store.write.QueryRow(ctx, `SELECT checkpoint, last_successful_poll, stale_since, last_attempt_at,
		error_category FROM poll_state WHERE endpoint = $1`, string(endpoint)).Scan(&position.checkpoint,
		&position.observation.LastSuccessfulPoll, &position.observation.StaleSince,
		&position.observation.LastAttemptAt, &position.observation.ErrorCategory)
	return position, storeError("load poll position", err)
}

func (store *Store) pendingPollReceipt(ctx context.Context, endpoint Endpoint) (pollReceipt, error) {
	var receipt pollReceipt
	err := store.withPollReceiptLock(ctx, endpoint, func(ctx context.Context, tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `SELECT input_id, started_at, received_at, records, hold_checkpoint, reconciles_warnings
			FROM pending_poll_receipts WHERE endpoint = $1`, string(endpoint)).Scan(&receipt.response.InputID,
			&receipt.started, &receipt.response.ReceivedAt, &receipt.response.Records,
			&receipt.holdCheckpoint, &receipt.reconcilesWarnings)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		receipt.response.Endpoint = endpoint
		return nil
	})
	return receipt, storeError("load pending poll receipt", err)
}

func (store *Store) stagePollReceipt(ctx context.Context, receipt pollReceipt) error {
	chunk := receipt.response
	// A non-nil slice encodes an empty response as an array rather than SQL NULL.
	records := make([][]byte, len(chunk.Records))
	for index, raw := range chunk.Records {
		records[index] = raw
	}
	err := store.withPollReceiptLock(ctx, chunk.Endpoint, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO pending_poll_receipts
			(endpoint, input_id, started_at, received_at, records, hold_checkpoint, reconciles_warnings)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`, string(chunk.Endpoint), chunk.InputID,
			databaseTime(receipt.started), databaseTime(chunk.ReceivedAt), records, receipt.holdCheckpoint, receipt.reconcilesWarnings)
		return err
	})
	return storeError("stage poll receipt", err)
}

func (store *Store) clearPollReceipt(ctx context.Context, response IngestChunk) error {
	err := store.withPollReceiptLock(ctx, response.Endpoint, func(ctx context.Context, tx pgx.Tx) error {
		var clearedID string
		return tx.QueryRow(ctx, `DELETE FROM pending_poll_receipts WHERE endpoint = $1 AND input_id = $2
			RETURNING input_id`, string(response.Endpoint), response.InputID).Scan(&clearedID)
	})
	return storeError("clear poll receipt", err)
}

func (store *Store) withPollReceiptLock(ctx context.Context, endpoint Endpoint, operation func(context.Context, pgx.Tx) error) (err error) {
	ctx, cancel := context.WithTimeout(ctx, store.writeTimeout)
	defer cancel()
	tx, err := store.beginMutation(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if cleanupErr := rollbackTransaction(ctx, tx); cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
		}
	}()
	// A fresh statement after this lock observes even a stage or clear whose commit reply was lost.
	if _, err = tx.Exec(ctx, `SELECT endpoint FROM poll_state WHERE endpoint = $1 FOR UPDATE`, string(endpoint)); err != nil {
		return err
	}
	if err = operation(ctx, tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (store *Store) recordPollAttempt(ctx context.Context, endpoint Endpoint, started time.Time, previous pollObservation) (err error) {
	ctx, cancel := context.WithTimeout(ctx, store.writeTimeout)
	defer cancel()
	tx, err := store.beginWriteMutation(ctx)
	if err != nil {
		return storeError("begin poll attempt", err)
	}
	defer func() {
		if cleanupErr := rollbackTransaction(ctx, tx); cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
		}
	}()
	// Preserve overdue knowledge before replacing the attempt that established it.
	_, err = tx.Exec(ctx, `UPDATE poll_state SET last_attempt_at = $2,
		stale_since = COALESCE(stale_since, $3), error_category = COALESCE(error_category, $4)
		WHERE endpoint = $1`, string(endpoint), databaseTime(started), previous.StaleSince, previous.ErrorCategory)
	if err != nil {
		return storeError("record poll attempt", err)
	}
	return storeError("record poll attempt", tx.Commit(ctx))
}

func (store *Store) recordPollFailure(ctx context.Context, endpoint Endpoint, started time.Time, category string) (err error) {
	ctx, cancel := context.WithTimeout(ctx, store.writeTimeout)
	defer cancel()
	tx, err := store.beginWriteMutation(ctx)
	if err != nil {
		return storeError("begin poll failure", err)
	}
	defer func() {
		if cleanupErr := rollbackTransaction(ctx, tx); cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
		}
	}()
	_, err = tx.Exec(ctx, `UPDATE poll_state SET last_attempt_at = $2,
		stale_since = COALESCE(stale_since, $2), error_category = $3 WHERE endpoint = $1`,
		string(endpoint), databaseTime(started), category)
	if err != nil {
		return storeError("record poll failure", err)
	}
	return storeError("record poll failure", tx.Commit(ctx))
}
