package aggregator

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const failedPayloadLimit = 1 << 20

var endpointSources = map[Endpoint]Source{
	SeismicEndpoint: BMKG, WarningEndpoint: BMKG, VolcanicEndpoint: PVMBG, CoastalEndpoint: SensorPantai,
}

type execer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// One record's result as decided inside its shard transaction.
type recordOutcome struct {
	offset  int
	raw     json.RawMessage
	applied bool
	reason  string
}

type failedMessage struct {
	MessageID        string
	Source           Source
	Endpoint         Endpoint
	Kind             string
	InputID          string
	InputOffset      int
	SourceRefID      *string
	SourceTimestamp  *time.Time
	ReceivedAt       time.Time
	LastSeenAt       time.Time
	Reason           string
	Retryable        bool
	Attempts         int
	Status           string
	Payload          []byte
	PayloadBytes     int64
	PayloadTruncated bool
}

// The same bytes on the same endpoint are the same message, however many polls fetch them again.
func failedMessageKey(endpoint Endpoint, kind string, payload []byte) []byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte(endpoint))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(kind))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(payload)
	return hash.Sum(nil)
}

func failedMessageID(key []byte) string { return "msg-" + hex.EncodeToString(key[:8]) }

// Reasons go to the Audit credential, so they name our own codes and fields, never source values.
func failedReason(category, field string) string {
	if field == "" {
		return category
	}
	return category + ": " + field
}

func boundedPayload(payload []byte) ([]byte, bool) {
	if len(payload) > failedPayloadLimit {
		return payload[:failedPayloadLimit], true
	}
	return payload, false
}

const upsertFailedMessage = `INSERT INTO failed_messages (message_key, message_id, source, endpoint, kind, input_id, input_offset,
        source_ref_id, source_timestamp, received_at, last_seen_at, reason, retryable, attempts, status,
        payload, payload_bytes, payload_truncated)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10, $11, $12, 1, $13, $14, $15, $16)
    ON CONFLICT (message_key) DO UPDATE SET
        last_seen_at = GREATEST(failed_messages.last_seen_at, EXCLUDED.last_seen_at),
        attempts = failed_messages.attempts + 1,
        reason = CASE WHEN EXCLUDED.status = 'retrying' AND failed_messages.status = 'parked'
            THEN failed_messages.reason ELSE EXCLUDED.reason END,
        retryable = CASE WHEN EXCLUDED.status = 'retrying' AND failed_messages.status = 'parked'
            THEN false ELSE EXCLUDED.retryable END,
        status = CASE WHEN EXCLUDED.status = 'retrying' AND failed_messages.status = 'parked'
            THEN 'parked' ELSE EXCLUDED.status END,
        resolved_at = NULL`

func writeFailedRecord(ctx context.Context, db execer, chunk IngestChunk, offset int, raw []byte, reason string, retryable bool) error {
	key := failedMessageKey(chunk.Endpoint, "record", raw)
	status := "parked"
	if retryable {
		status = "retrying"
	}
	payload, truncated := boundedPayload(raw)
	_, err := db.Exec(ctx, upsertFailedMessage, key, failedMessageID(key), string(endpointSources[chunk.Endpoint]),
		string(chunk.Endpoint), "record", chunk.InputID, chunk.InputOffset+offset,
		rejectedIdentity(raw, chunk.Endpoint), recordTimestamp(raw, chunk.Endpoint), databaseTime(chunk.ReceivedAt),
		reason, retryable, status, payload, int64(len(raw)), truncated)
	return err
}

func writeFailedResponse(ctx context.Context, db execer, endpoint Endpoint, receivedAt time.Time, failure *sourceError) error {
	key := failedMessageKey(endpoint, "response", failure.payload)
	_, err := db.Exec(ctx, upsertFailedMessage, key, failedMessageID(key), string(endpointSources[endpoint]),
		string(endpoint), "response", "", 0, nil, nil, databaseTime(receivedAt),
		failedReason(failure.category, "body"), true, "retrying", failure.payload, failure.payloadBytes, failure.payloadTruncated)
	return err
}

// Applied records close any open row for the same bytes, including rows left retrying by an unreachable shard.
func recordOutcomes(ctx context.Context, db execer, chunk IngestChunk, outcomes []recordOutcome) error {
	var resolved [][]byte
	for _, outcome := range outcomes {
		if outcome.applied {
			resolved = append(resolved, failedMessageKey(chunk.Endpoint, "record", outcome.raw))
			continue
		}
		if err := writeFailedRecord(ctx, db, chunk, outcome.offset, outcome.raw, outcome.reason, false); err != nil {
			return err
		}
	}
	if len(resolved) == 0 {
		return nil
	}
	_, err := db.Exec(ctx, `UPDATE failed_messages SET status = 'resolved', resolved_at = now()
        WHERE message_key = ANY($1) AND status <> 'resolved'`, resolved)
	return err
}

func (store *Store) recordUnreadableResponse(ctx context.Context, endpoint Endpoint, receivedAt time.Time, failure *sourceError) error {
	ctx, cancel := context.WithTimeout(ctx, store.writeTimeout)
	defer cancel()
	return writeFailedResponse(ctx, store.write, endpoint, receivedAt, failure)
}

// A complete poll re-fetched whatever the unreadable responses carried, so they no longer wait on anything.
func (store *Store) resolveUnreadableResponses(ctx context.Context, endpoint Endpoint) error {
	ctx, cancel := context.WithTimeout(ctx, store.writeTimeout)
	defer cancel()
	_, err := store.write.Exec(ctx, `UPDATE failed_messages SET status = 'resolved', resolved_at = now()
        WHERE endpoint = $1 AND kind = 'response' AND status <> 'resolved'`, string(endpoint))
	return err
}

func (store *Store) recordRoutedOutcomes(ctx context.Context, chunk IngestChunk, outcomes []recordOutcome) (err error) {
	ctx, cancel := context.WithTimeout(ctx, store.writeTimeout)
	defer cancel()
	tx, err := store.write.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer func() {
		if cleanupErr := rollbackTransaction(ctx, tx); cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
		}
	}()
	if err := recordOutcomes(ctx, tx, chunk, outcomes); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Best effort: the shard error is still returned and the receipt retried, so a lost row only hides the wait.
func (store *Store) noteShardFailure(ctx context.Context, chunk IngestChunk, raw []byte, shardID int16, cause error) {
	if errors.Is(cause, errStaleMutationAuthority) || errors.Is(cause, errReceiptConflict) || ctx.Err() != nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, store.writeTimeout)
	defer cancel()
	_ = writeFailedRecord(ctx, store.write, chunk, 0, raw, failedReason("shard_unavailable", fmt.Sprintf("shard-%d", shardID)), true)
}

type failedCursor struct {
	ReceivedAt time.Time `json:"r"`
	MessageID  string    `json:"m"`
}

var errInvalidFailedCursor = errors.New("failed message cursor is invalid")

func encodeFailedCursor(message failedMessage) string {
	encoded, _ := json.Marshal(failedCursor{ReceivedAt: message.ReceivedAt.UTC(), MessageID: message.MessageID})
	return base64.RawURLEncoding.EncodeToString(encoded)
}

func decodeFailedCursor(raw string) (failedCursor, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil || len(decoded) > 512 {
		return failedCursor{}, errInvalidFailedCursor
	}
	var cursor failedCursor
	if err := json.Unmarshal(decoded, &cursor); err != nil || cursor.MessageID == "" || cursor.ReceivedAt.IsZero() {
		return failedCursor{}, errInvalidFailedCursor
	}
	return cursor, nil
}

type failedQuery struct {
	source       Source
	limit        int
	after        *failedCursor
	withPayloads bool
}

// Open rows only: a resolved message no longer appears anywhere in the listing.
func (store *Store) listFailedMessages(ctx context.Context, query failedQuery) ([]failedMessage, *string, error) {
	var afterAt *time.Time
	afterID := ""
	if query.after != nil {
		afterAt, afterID = &query.after.ReceivedAt, query.after.MessageID
	}
	payload := "''::bytea"
	if query.withPayloads {
		payload = "payload"
	}
	rows, err := store.read.Query(ctx, `SELECT message_id, source, endpoint, kind, input_id, input_offset, source_ref_id,
            source_timestamp, received_at, last_seen_at, reason, retryable, attempts, status, `+payload+`,
            payload_bytes, payload_truncated
        FROM failed_messages
        WHERE status <> 'resolved' AND ($1 = '' OR source = $1)
            AND ($2::timestamptz IS NULL OR (received_at, message_id COLLATE "C") > ($2, $3 COLLATE "C"))
        ORDER BY received_at, message_id COLLATE "C"
        LIMIT $4`, string(query.source), afterAt, afterID, query.limit+1)
	if err != nil {
		return nil, nil, err
	}
	messages, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (failedMessage, error) {
		var message failedMessage
		var source, endpoint string
		err := row.Scan(&message.MessageID, &source, &endpoint, &message.Kind, &message.InputID, &message.InputOffset,
			&message.SourceRefID, &message.SourceTimestamp, &message.ReceivedAt, &message.LastSeenAt, &message.Reason,
			&message.Retryable, &message.Attempts, &message.Status, &message.Payload, &message.PayloadBytes, &message.PayloadTruncated)
		message.Source, message.Endpoint = Source(source), Endpoint(endpoint)
		return message, err
	})
	if err != nil {
		return nil, nil, err
	}
	if len(messages) <= query.limit {
		return messages, nil, nil
	}
	messages = messages[:query.limit]
	next := encodeFailedCursor(messages[len(messages)-1])
	return messages, &next, nil
}
