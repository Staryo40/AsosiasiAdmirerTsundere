package aggregator

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
)

// One fire lineage as the satellite service sends it; feed_revision grows with every change it publishes.
type wildfireRecord struct {
	SourceRefID  string                     `json:"source_ref_id"`
	FeedRevision int64                      `json:"feed_revision"`
	Active       bool                       `json:"active"`
	Severity     Severity                   `json:"severity"`
	AreaName     string                     `json:"area_name"`
	Latitude     float64                    `json:"latitude"`
	Longitude    float64                    `json:"longitude"`
	OccurredAt   time.Time                  `json:"occurred_at"`
	Attributes   map[string]json.RawMessage `json:"attributes"`
}

var wildfireAttributes = []string{"detection_count", "satellites", "max_confidence", "total_frp_mw", "detection_keys"}

func parseWildfire(raw json.RawMessage) (wildfireRecord, error) {
	if _, err := parseObject(raw); err != nil {
		return wildfireRecord{}, err
	}
	var record wildfireRecord
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return wildfireRecord{}, invalidField("record", WrongType)
	}
	switch {
	case !validText(record.SourceRefID):
		return record, invalidField("source_ref_id", InvalidValue)
	case record.FeedRevision < 0:
		return record, invalidField("feed_revision", InvalidValue)
	case record.Severity != Waspada && record.Severity != Siaga && record.Severity != Awas:
		return record, invalidField("severity", InvalidValue)
	case !validText(record.AreaName):
		return record, invalidField("area_name", InvalidValue)
	case math.IsNaN(record.Latitude) || record.Latitude < -90 || record.Latitude > 90:
		return record, invalidField("latitude", InvalidValue)
	case math.IsNaN(record.Longitude) || record.Longitude < -180 || record.Longitude > 180:
		return record, invalidField("longitude", InvalidValue)
	case !validInstant(record.OccurredAt):
		return record, invalidField("occurred_at", InvalidValue)
	}
	for _, name := range wildfireAttributes {
		if _, present := record.Attributes[name]; !present {
			return record, invalidField("attributes."+name, MissingField)
		}
	}
	if len(record.Attributes) != len(wildfireAttributes) {
		return record, invalidField("attributes", InvalidValue)
	}
	return record, nil
}

// Unlike other sources a fire's earliest detection can arrive late, so occurred_at may move; a stale revision is a no-op.
func (store *Store) writeWildfire(ctx context.Context, tx pgx.Tx, chunk IngestChunk, raw json.RawMessage) error {
	record, err := parseWildfire(raw)
	if err != nil {
		return err
	}
	var hazardID string
	var storedRevision int64
	err = tx.QueryRow(ctx, "SELECT hazard_id, feed_revision FROM source_wildfire WHERE source_ref_id = $1 FOR UPDATE",
		record.SourceRefID).Scan(&hazardID, &storedRevision)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		hazardID = "haz-" + rand.Text()
	case err != nil:
		return err
	case record.FeedRevision <= storedRevision:
		return nil
	}
	hazard := HazardEvent{HazardID: hazardID, Source: Satelit, SourceRefID: record.SourceRefID, HazardType: Wildfire,
		Severity: record.Severity, AreaName: record.AreaName, Latitude: record.Latitude, Longitude: record.Longitude,
		OccurredAt: databaseTime(record.OccurredAt), IngestedAt: databaseTime(chunk.ReceivedAt), Attributes: record.Attributes}
	// A withdrawn fire keeps its row and lineage but leaves the active population; reviving it clears withdrawn_at.
	var withdrawnAt *time.Time
	if !record.Active {
		stamp := databaseTime(chunk.ReceivedAt)
		withdrawnAt = &stamp
		hazard.Severity = Normal
		hazard.Attributes = map[string]json.RawMessage{"status": json.RawMessage(`"withdrawn"`)}
		for name, value := range record.Attributes {
			hazard.Attributes[name] = value
		}
	}
	if err := upsertWildfireHazard(ctx, tx, &hazard, withdrawnAt); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO source_wildfire (source_ref_id, hazard_id, feed_revision, active, payload, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (source_ref_id) DO UPDATE SET feed_revision = EXCLUDED.feed_revision, active = EXCLUDED.active,
            payload = EXCLUDED.payload, updated_at = EXCLUDED.updated_at`,
		record.SourceRefID, hazard.HazardID, record.FeedRevision, record.Active, []byte(raw), databaseTime(chunk.ReceivedAt))
	return err
}

const wildfireUpsert = `INSERT INTO hazard_events AS stored
    (hazard_id, source, source_ref_id, hazard_type, severity, area_name, latitude, longitude,
     occurred_at, ingested_at, attributes, revision, updated_at, withdrawn_at)
    VALUES ($1, 'SATELIT', $2, 'WILDFIRE', $3, $4, $5, $6, $7, $8, $9, 1, $8, $10)
    ON CONFLICT (source, source_ref_id) DO UPDATE SET
        severity = EXCLUDED.severity, area_name = EXCLUDED.area_name, latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude, occurred_at = EXCLUDED.occurred_at, attributes = EXCLUDED.attributes,
        revision = stored.revision + 1, updated_at = EXCLUDED.updated_at, withdrawn_at = EXCLUDED.withdrawn_at
    WHERE ROW(stored.severity, stored.area_name, stored.latitude, stored.longitude, stored.occurred_at, stored.attributes,
            stored.withdrawn_at IS NULL)
        IS DISTINCT FROM
        ROW(EXCLUDED.severity, EXCLUDED.area_name, EXCLUDED.latitude, EXCLUDED.longitude, EXCLUDED.occurred_at, EXCLUDED.attributes,
            EXCLUDED.withdrawn_at IS NULL)
    RETURNING hazard_id, ingested_at, revision`

func upsertWildfireHazard(ctx context.Context, tx pgx.Tx, hazard *HazardEvent, withdrawnAt *time.Time) error {
	attributes, err := json.Marshal(hazard.Attributes)
	if err != nil {
		return err
	}
	updatedAt := hazard.IngestedAt
	var revision int64
	err = tx.QueryRow(ctx, wildfireUpsert, hazard.HazardID, hazard.SourceRefID, string(hazard.Severity), hazard.AreaName,
		hazard.Latitude, hazard.Longitude, hazard.OccurredAt, hazard.IngestedAt, attributes, withdrawnAt).Scan(&hazard.HazardID, &hazard.IngestedAt, &revision)
	if errors.Is(err, pgx.ErrNoRows) {
		// Same content under a newer revision: nothing to publish, but keep the stored identity.
		return tx.QueryRow(ctx, "SELECT hazard_id FROM hazard_events WHERE source = 'SATELIT' AND source_ref_id = $1",
			hazard.SourceRefID).Scan(&hazard.HazardID)
	}
	if err != nil {
		return err
	}
	hazard.IngestedAt = databaseTime(hazard.IngestedAt)
	if hazard.IngestedAt.After(updatedAt) {
		updatedAt = hazard.IngestedAt
	}
	return writeNotification(ctx, tx, hazardNotification{SchemaVersion: 1, MessageID: "msg-" + rand.Text(),
		HazardID: hazard.HazardID, Revision: revision, ProducedAt: updatedAt, HazardEvent: *hazard})
}
