package aggregator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

func (store *Store) writeWarning(ctx context.Context, tx pgx.Tx, chunk IngestChunk, raw json.RawMessage) error {
	warning, err := parseWarning(raw)
	if err != nil {
		return err
	}
	if len(warning.id) > 2048 || len(warning.eventID) > 2048 {
		return invalidField("warning_id", "storage_limit")
	}
	if err := writeWarningSource(ctx, tx, raw, warning); err != nil {
		return err
	}
	return store.recomputeSeismic(ctx, tx, warning.eventID, chunk.ReceivedAt)
}

func writeWarningSource(ctx context.Context, tx pgx.Tx, raw json.RawMessage, warning tsunamiWarning) error {
	command, err := tx.Exec(ctx, `INSERT INTO source_warnings (warning_id, related_event_id, issued_at, payload)
        VALUES ($1, $2, $3, $4) ON CONFLICT (warning_id) DO UPDATE SET payload = EXCLUDED.payload
        WHERE (source_warnings.related_event_id, source_warnings.issued_at) =
              (EXCLUDED.related_event_id, EXCLUDED.issued_at)`, warning.id, warning.eventID, databaseTime(warning.issuedAt), raw)
	if err != nil {
		return err
	}
	if command.RowsAffected() == 0 {
		return invalidField("warning_id", "source_identity_reused")
	}
	return nil
}

func (store *Store) recomputeSeismic(ctx context.Context, tx pgx.Tx, eventID string, receivedAt time.Time) error {
	var raw json.RawMessage
	err := tx.QueryRow(ctx, "SELECT payload FROM source_seismic WHERE event_id = $1", eventID).Scan(&raw)
	// Unmatched warnings remain durable until the event arrives, including across restart.
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	hazard, err := MapSeismic(raw, nil, newReceipt(receivedAt))
	if err != nil {
		return err
	}
	return store.writeEnrichedSeismic(ctx, tx, raw, hazard)
}

func (store *Store) writeEnrichedSeismic(ctx context.Context, tx pgx.Tx, raw json.RawMessage, hazard HazardEvent) error {
	warnings, err := store.storedWarnings(ctx, tx, hazard.SourceRefID)
	if err != nil {
		return err
	}
	if !bytes.Equal(bytes.TrimSpace(hazard.Attributes["potential_tsunami"]), []byte("true")) {
		if err := rejectIncompatibleWarnings(ctx, tx, warnings, hazard.IngestedAt); err != nil {
			return err
		}
		warnings = nil
	}
	hazard, err = MapSeismic(raw, warnings, Receipt{hazard.HazardID, hazard.IngestedAt})
	if err != nil {
		return err
	}
	return writeHazard(ctx, tx, hazard)
}

func rejectIncompatibleWarnings(ctx context.Context, tx pgx.Tx, warnings []json.RawMessage, receivedAt time.Time) error {
	// Quarantine the association, preserving both valid seismic data and the warning source payload.
	chunk := IngestChunk{Endpoint: WarningEndpoint, ReceivedAt: receivedAt}
	for _, raw := range warnings {
		if err := writeReject(ctx, tx, chunk, rejectedRecord{raw, ValidationError{Code: "incompatible_warning", Field: "related_event_id"}}); err != nil {
			return err
		}
	}
	return nil
}

func (store *Store) storedWarnings(ctx context.Context, tx pgx.Tx, eventID string) ([]json.RawMessage, error) {
	rows, err := tx.Query(ctx, "SELECT payload FROM source_warnings WHERE related_event_id = $1 ORDER BY issued_at, warning_id", eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	warnings := make([]json.RawMessage, 0)
	var totalBytes int64
	for rows.Next() {
		var raw json.RawMessage
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		totalBytes += int64(len(raw))
		if totalBytes > store.maxBytes {
			return nil, invalidField("tsunami_warnings", "storage_limit")
		}
		warnings = append(warnings, raw)
	}
	return warnings, rows.Err()
}
