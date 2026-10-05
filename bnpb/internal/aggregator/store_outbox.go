package aggregator

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// Materialize and close rows before returning: no DB connection crosses broker I/O.
func (store *Store) pendingOutbox(ctx context.Context, limit int) ([]outboxMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, store.writeTimeout)
	defer cancel()
	rows, err := store.write.Query(ctx, `SELECT sequence, payload, attempts, next_attempt_at
        FROM outbox WHERE sent_at IS NULL ORDER BY sequence LIMIT $1`, limit)
	if err != nil {
		return nil, storeError("read pending outbox", err)
	}
	defer rows.Close()
	messages := make([]outboxMessage, 0)
	var totalBytes int64
	for rows.Next() {
		var message outboxMessage
		if err := rows.Scan(&message.Sequence, &message.Payload, &message.Attempts, &message.NextAttemptAt); err != nil {
			return nil, storeError("scan pending outbox", err)
		}
		// Always allow the head, even if envelope overhead exceeds the source cap.
		if len(messages) > 0 && totalBytes+int64(len(message.Payload)) > store.maxBytes {
			break
		}
		totalBytes += int64(len(message.Payload))
		if err := json.Unmarshal(message.Payload, &message.Publication); err != nil {
			return nil, errors.New("invalid stored outbox envelope")
		}
		messages = append(messages, message)
	}
	return messages, storeError("read pending outbox", rows.Err())
}

func (store *Store) attemptOutbox(ctx context.Context, sequence int64) error {
	return store.updateOutbox(ctx, `UPDATE outbox SET attempts = LEAST(attempts::bigint + 1, 2147483647)::integer
        WHERE sequence = $1 AND sent_at IS NULL`, sequence)
}

func (store *Store) sentOutbox(ctx context.Context, sequence int64) error {
	return store.updateOutbox(ctx, `UPDATE outbox SET sent_at = clock_timestamp()
        WHERE sequence = $1 AND sent_at IS NULL`, sequence)
}

func (store *Store) retryOutbox(ctx context.Context, sequence int64, nextAttempt time.Time) error {
	return store.updateOutbox(ctx, `UPDATE outbox SET next_attempt_at = $2
        WHERE sequence = $1 AND sent_at IS NULL`, sequence, nextAttempt)
}

func (store *Store) updateOutbox(ctx context.Context, query string, args ...any) error {
	ctx, cancel := context.WithTimeout(ctx, store.writeTimeout)
	defer cancel()
	updated, err := store.write.Exec(ctx, query, args...)
	if err == nil && updated.RowsAffected() != 1 {
		err = errors.New("pending outbox row missing")
	}
	return storeError("update outbox delivery", err)
}
