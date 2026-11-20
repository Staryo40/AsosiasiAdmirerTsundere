package aggregator

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

func (store *Store) latestCoastalHazard(ctx context.Context) (HazardEvent, error) {
	ctx, cancel := context.WithTimeout(ctx, store.readTimeout)
	defer cancel()
	row := store.read.QueryRow(ctx, `SELECT `+hazardColumns+` FROM hazard_events
        WHERE source = $1 AND hazard_type = $2
        ORDER BY occurred_at DESC, hazard_id COLLATE "C" DESC LIMIT 1`,
		string(SensorPantai), string(Coastal))
	event, err := scanHazard(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return HazardEvent{}, nil
	}
	return event, storeError("latest coastal hazard", err)
}

func (store *Store) attachPantaiBreaker(client *pantaiClient) {
	store.pantaiClient = client
}

func (store *Store) coastalBreakerState() string {
	if store.pantaiClient == nil {
		return "disabled"
	}
	return store.pantaiClient.breakerState()
}