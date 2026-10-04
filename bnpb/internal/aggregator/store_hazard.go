package aggregator

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/correlation"
	"github.com/jackc/pgx/v5"
)

type hazardNotification struct {
	SchemaVersion int         `json:"schema_version"`
	MessageID     string      `json:"message_id"`
	HazardID      string      `json:"hazard_id"`
	Revision      int64       `json:"revision"`
	CorrelationID string      `json:"correlation_id"`
	EventType     string      `json:"event_type"`
	ProducedAt    time.Time   `json:"produced_at"`
	HazardEvent   HazardEvent `json:"hazard_event"`
}

func newReceipt(receivedAt time.Time) Receipt {
	return Receipt{HazardID: "haz-" + rand.Text(), IngestedAt: databaseTime(receivedAt)}
}

func writeHazard(ctx context.Context, tx pgx.Tx, hazard HazardEvent) error {
	if len(hazard.SourceRefID) > 2048 {
		return invalidField("source_ref_id", "storage_limit")
	}
	hazard.OccurredAt = databaseTime(hazard.OccurredAt)
	attributes, err := json.Marshal(hazard.Attributes)
	if err != nil {
		return err
	}
	var revision int64
	updatedAt := hazard.IngestedAt
	err = tx.QueryRow(ctx, hazardUpsert, hazard.HazardID, string(hazard.Source), hazard.SourceRefID,
		string(hazard.HazardType), string(hazard.Severity), hazard.AreaName, hazard.Latitude,
		hazard.Longitude, hazard.OccurredAt, hazard.IngestedAt, attributes).Scan(&hazard.HazardID, &hazard.IngestedAt, &revision)
	if errors.Is(err, pgx.ErrNoRows) {
		return checkSourceIdentity(ctx, tx, hazard)
	}
	if err != nil {
		return err
	}
	hazard.IngestedAt = databaseTime(hazard.IngestedAt)
	return writeNotification(ctx, tx, hazardNotification{SchemaVersion: 1, MessageID: "msg-" + rand.Text(),
		HazardID: hazard.HazardID, Revision: revision, ProducedAt: updatedAt, HazardEvent: hazard})
}

const hazardUpsert = `INSERT INTO hazard_events AS stored
    (hazard_id, source, source_ref_id, hazard_type, severity, area_name, latitude, longitude,
     occurred_at, ingested_at, attributes, revision, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 1, $10)
    ON CONFLICT (source, source_ref_id) DO UPDATE SET
        hazard_type = EXCLUDED.hazard_type, severity = EXCLUDED.severity, area_name = EXCLUDED.area_name,
        latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude, attributes = EXCLUDED.attributes,
        revision = stored.revision + 1, updated_at = EXCLUDED.updated_at
    WHERE stored.occurred_at = EXCLUDED.occurred_at AND
        ROW(stored.hazard_type, stored.severity, stored.area_name, stored.latitude, stored.longitude, stored.attributes)
        IS DISTINCT FROM
        ROW(EXCLUDED.hazard_type, EXCLUDED.severity, EXCLUDED.area_name, EXCLUDED.latitude, EXCLUDED.longitude, EXCLUDED.attributes)
    RETURNING hazard_id, ingested_at, revision`

func checkSourceIdentity(ctx context.Context, tx pgx.Tx, hazard HazardEvent) error {
	var occurredAt time.Time
	err := tx.QueryRow(ctx, "SELECT occurred_at FROM hazard_events WHERE source = $1 AND source_ref_id = $2",
		string(hazard.Source), hazard.SourceRefID).Scan(&occurredAt)
	if err != nil {
		return err
	}
	if !occurredAt.Equal(hazard.OccurredAt) {
		return invalidField("source_ref_id", "source_identity_reused")
	}
	return nil
}

func writeNotification(ctx context.Context, tx pgx.Tx, notification hazardNotification) error {
	notification.CorrelationID = correlation.ID(ctx)
	if !correlation.ValidID(notification.CorrelationID) {
		notification.CorrelationID = correlation.NewID()
	}
	notification.EventType = "hazard.updated"
	if notification.Revision == 1 {
		notification.EventType = "hazard.created"
	}
	payload, err := json.Marshal(notification)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO outbox
        (message_id, hazard_id, revision, correlation_id, payload, created_at, next_attempt_at)
        VALUES ($1, $2, $3, $4, $5, $6, $6)`, notification.MessageID, notification.HazardID,
		notification.Revision, notification.CorrelationID, payload, notification.ProducedAt)
	return err
}
