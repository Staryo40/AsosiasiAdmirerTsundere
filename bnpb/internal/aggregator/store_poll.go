package aggregator

import (
	"context"
	"time"
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

func (store *Store) recordPollAttempt(ctx context.Context, endpoint Endpoint, started time.Time, previous pollObservation) error {
	ctx, cancel := context.WithTimeout(ctx, store.writeTimeout)
	defer cancel()
	// Preserve overdue knowledge before replacing the attempt that established it.
	_, err := store.write.Exec(ctx, `UPDATE poll_state SET last_attempt_at = $2,
		stale_since = COALESCE(stale_since, $3), error_category = COALESCE(error_category, $4)
		WHERE endpoint = $1`, string(endpoint), databaseTime(started), previous.StaleSince, previous.ErrorCategory)
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
