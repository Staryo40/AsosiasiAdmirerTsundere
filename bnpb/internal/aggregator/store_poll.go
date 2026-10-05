package aggregator

import (
	"context"
	"time"
)

func (store *Store) pollCheckpoint(ctx context.Context, endpoint Endpoint) (*time.Time, error) {
	ctx, cancel := context.WithTimeout(ctx, store.writeTimeout)
	defer cancel()
	var checkpoint *time.Time
	err := store.write.QueryRow(ctx, "SELECT checkpoint FROM poll_state WHERE endpoint = $1", string(endpoint)).Scan(&checkpoint)
	return checkpoint, storeError("load poll checkpoint", err)
}

func (store *Store) recordPollAttempt(ctx context.Context, endpoint Endpoint, started time.Time) error {
	ctx, cancel := context.WithTimeout(ctx, store.writeTimeout)
	defer cancel()
	_, err := store.write.Exec(ctx, "UPDATE poll_state SET last_attempt_at = $2 WHERE endpoint = $1", string(endpoint), databaseTime(started))
	return storeError("record poll attempt", err)
}

func (store *Store) recordPollFailure(ctx context.Context, endpoint Endpoint, started time.Time, category string) error {
	ctx, cancel := context.WithTimeout(ctx, store.writeTimeout)
	defer cancel()
	_, err := store.write.Exec(ctx, `UPDATE poll_state SET last_attempt_at = $2,
        stale_since = COALESCE(stale_since, $2), error_category = $3 WHERE endpoint = $1`,
		string(endpoint), databaseTime(started), category)
	return storeError("record poll failure", err)
}
