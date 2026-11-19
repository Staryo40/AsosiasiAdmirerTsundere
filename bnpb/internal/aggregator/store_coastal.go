package aggregator

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
)

type coastalIngest struct {
	client      *pantaiClient
	generatedAt time.Time
	staleAfter  time.Duration
}

func (store *Store) ingestCoastalChunk(ctx context.Context, chunk IngestChunk, response pantaiObservations, client *pantaiClient, policy checkpointPolicy) error {
	store.coastal = coastalIngest{client: client, generatedAt: response.GeneratedAt, staleAfter: store.coastalStale}
	defer func() { store.coastal = coastalIngest{} }()
	return store.ingestChunk(ctx, chunk, policy)
}

func (store *Store) writeCoastal(ctx context.Context, tx pgx.Tx, chunk IngestChunk, raw json.RawMessage) error {
	fields, err := parseObject(raw)
	if err != nil {
		return invalidField("record", InvalidJSON)
	}
	stationID, err := textField(fields, "station_id")
	if err != nil {
		return err
	}
	station, err := store.coastal.client.station(ctx, stationID)
	if err != nil {
		return err
	}
	hazard, err := MapCoastal(raw, station, store.coastal.generatedAt, store.coastal.staleAfter, newReceipt(chunk.ReceivedAt))
	if err != nil {
		return err
	}
	return writeHazard(ctx, tx, hazard)
}

func (store *Store) pollResumeToken(ctx context.Context, endpoint Endpoint) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, store.readTimeout)
	defer cancel()
	var token *string
	err := store.read.QueryRow(ctx, `SELECT resume_token FROM poll_state WHERE endpoint = $1`, string(endpoint)).Scan(&token)
	if err != nil {
		return "", storeError("load poll resume token", err)
	}
	if token == nil {
		return "", nil
	}
	return *token, nil
}

func (store *Store) completeCoastalPoll(ctx context.Context, endpoint Endpoint, started time.Time, nextCursor string) error {
	ctx, cancel := context.WithTimeout(ctx, store.writeTimeout)
	defer cancel()
	_, err := store.write.Exec(ctx, `UPDATE poll_state SET resume_token = NULLIF($2, ''),
		last_attempt_at = $3, last_successful_poll = $3, stale_since = NULL, error_category = NULL
		WHERE endpoint = $1`, string(endpoint), nextCursor, databaseTime(started))
	return storeError("complete coastal poll", err)
}