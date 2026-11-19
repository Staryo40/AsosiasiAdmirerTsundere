package aggregator

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Routed writes have per-record durability; the control checkpoint follows only after every outcome.
func (store *Store) ingestShardChunk(ctx context.Context, chunk IngestChunk, policy checkpointPolicy) error {

	if _, _, ok := pollAuthorityToken(ctx); !ok {
		return fmt.Errorf("routed ingest: %w", errStaleMutationAuthority)
	}
	if err := store.validateChunk(chunk); err != nil {
		return err
	}
	for index, raw := range chunk.Records {
		recordChunk := chunk
		recordChunk.InputOffset += index
		shardID, identityErr := inputShard(chunk.Endpoint, raw)
		if identityErr != nil {
			if err := store.writeUnroutable(ctx, recordChunk, raw, identityErr); err != nil {
				return storeError("retain unroutable input", err)
			}
			continue
		}
		shard, err := store.shardStore(ctx, shardID)
		if err != nil {
			return storeError("resolve input shard", err)
		}
		if err := shard.writeShardInput(ctx, recordChunk, raw); err != nil {
			return storeError("write shard input", err)
		}
	}
	return store.writeShardCheckpoint(ctx, chunk, policy)
}

func inputShard(endpoint Endpoint, raw json.RawMessage) (int16, error) {
	fields, err := parseObject(raw)
	if err != nil {
		return 0, err
	}
	identityField := ingestEndpoints[endpoint].identity
	if endpoint == WarningEndpoint {
		identityField = "related_event_id"
	}
	identity, err := textField(fields, identityField)
	if err != nil || identity == "" {
		return 0, fmt.Errorf("missing routable %s", identityField)
	}
	source := BMKG
	if endpoint == VolcanicEndpoint {
		source = PVMBG
	}
	return logicalShard(source, identity)
}

func (store *Store) writeShardInput(ctx context.Context, chunk IngestChunk, raw json.RawMessage) (err error) {
	ctx, cancel := context.WithTimeout(ctx, store.writeTimeout)
	defer cancel()
	tx, err := store.beginWriteMutation(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if cleanupErr := rollbackTransaction(ctx, tx); cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
		}
	}()
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(4031, $1)", ingestEndpoints[chunk.Endpoint].lock); err != nil {
		return err
	}
	status, exists, err := handledInputStatus(ctx, tx, chunk, raw)
	if err != nil {
		return err
	}
	if exists {
		if status == "retained" {
			if err := retryRetainedInput(ctx, tx, store, chunk, raw); err != nil {
				return err
			}
		}
		return tx.Commit(ctx)
	}
	applyTx, err := tx.Begin(ctx)
	if err != nil {
		return err
	}
	applyErr := store.writeSourceRecord(ctx, applyTx, chunk, raw)
	status, reason := "applied", ""
	if applyErr == nil {
		if err = applyTx.Commit(ctx); err != nil {
			return err
		}
	} else {
		if cleanupErr := rollbackTransaction(ctx, applyTx); cleanupErr != nil {
			return errors.Join(applyErr, cleanupErr)
		}
		category, field := rejectReason(applyErr)
		if category == "" {
			return applyErr
		}
		status, reason = "retained", category+":"+field
		if err = writeReject(ctx, tx, chunk, rejectedRecord{raw, ValidationError{Code: ValidationCode(category), Field: field}}); err != nil {
			return err
		}
	}
	if err := insertHandledInput(ctx, tx, chunk, raw, status, reason); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// A receipt retained as unroutable in the control store stays there; schema fixes cannot infer its owner.
func (store *Store) writeUnroutable(ctx context.Context, chunk IngestChunk, raw json.RawMessage, cause error) (err error) {
	ctx, cancel := context.WithTimeout(ctx, store.writeTimeout)
	defer cancel()
	tx, err := store.beginWriteMutation(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if cleanupErr := rollbackTransaction(ctx, tx); cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
		}
	}()
	_, exists, err := handledInputStatus(ctx, tx, chunk, raw)
	if err != nil {
		return err
	}
	if !exists {
		category, field := rejectReason(cause)
		if category == "" {
			category, field = "invalid_value", "identity"
		}
		if err = writeReject(ctx, tx, chunk, rejectedRecord{raw, ValidationError{Code: ValidationCode(category), Field: field}}); err != nil {
			return err
		}
		if err = insertHandledInput(ctx, tx, chunk, raw, "retained", "unroutable_identity"); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func inputHash(chunk IngestChunk) []byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte(chunk.Endpoint))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(chunk.InputID))
	_, _ = hash.Write([]byte{0})
	var offset [8]byte
	binary.BigEndian.PutUint64(offset[:], uint64(chunk.InputOffset))
	_, _ = hash.Write(offset[:])
	return hash.Sum(nil)
}

func handledInputStatus(ctx context.Context, tx pgx.Tx, chunk IngestChunk, raw []byte) (string, bool, error) {
	hash := inputHash(chunk)
	var old []byte
	var status string
	var reason *string
	err := tx.QueryRow(ctx, "SELECT raw_payload,status,reason FROM handled_inputs WHERE endpoint=$1 AND input_hash=$2 FOR UPDATE", string(chunk.Endpoint), hash).Scan(&old, &status, &reason)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if string(old) != string(raw) || (status != "applied" && status != "retained") || (status == "applied" && reason != nil) || (status == "retained" && reason == nil) {
		return "", false, errors.New("handled input state mismatch")
	}
	return status, true, nil
}

// Retried retained payloads never create duplicate reject records; only successful mapping advances the ledger.
func retryRetainedInput(ctx context.Context, tx pgx.Tx, store *Store, chunk IngestChunk, raw json.RawMessage) error {
	applyTx, err := tx.Begin(ctx)
	if err != nil {
		return err
	}
	if err := store.writeSourceRecord(ctx, applyTx, chunk, raw); err != nil {
		if cleanupErr := rollbackTransaction(ctx, applyTx); cleanupErr != nil {
			return errors.Join(err, cleanupErr)
		}
		if category, _ := rejectReason(err); category != "" {
			return nil
		}
		return err
	}
	if err := applyTx.Commit(ctx); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE handled_inputs SET status='applied', reason=NULL
		WHERE endpoint=$1 AND input_hash=$2 AND status='retained'`, string(chunk.Endpoint), inputHash(chunk))
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("retained handled input transition lost")
	}
	return nil
}

func insertHandledInput(ctx context.Context, tx pgx.Tx, chunk IngestChunk, raw []byte, status, reason string) error {
	_, err := tx.Exec(ctx, `INSERT INTO handled_inputs(endpoint,input_hash,raw_payload,received_at,status,reason)
		VALUES($1,$2,$3,$4,$5,NULLIF($6,''))`, string(chunk.Endpoint), inputHash(chunk), []byte(raw), databaseTime(chunk.ReceivedAt), status, reason)
	return err
}
func (store *Store) writeShardCheckpoint(ctx context.Context, chunk IngestChunk, policy checkpointPolicy) (err error) {
	ctx, cancel := context.WithTimeout(ctx, store.writeTimeout)
	defer cancel()
	tx, err := store.beginWriteMutation(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if cleanupErr := rollbackTransaction(ctx, tx); cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
		}
	}()
	if policy.omitPollState {
		return tx.Commit(ctx)
	}
	_, err = tx.Exec(ctx, `UPDATE poll_state SET
		checkpoint = CASE WHEN GREATEST(checkpoint, $2::timestamptz) IS NULL THEN NULL ELSE LEAST(GREATEST(checkpoint, $2::timestamptz), COALESCE($4::timestamptz, 'infinity'::timestamptz)) END,
		last_attempt_at=COALESCE($6::timestamptz,$3),
		last_successful_poll=CASE WHEN $5 THEN COALESCE($7::timestamptz,$3) ELSE last_successful_poll END,
		stale_since=CASE WHEN $5 THEN NULL ELSE stale_since END,
		error_category=CASE WHEN $5 THEN NULL ELSE error_category END WHERE endpoint=$1`, string(chunk.Endpoint), chunkCheckpoint(chunk, policy), databaseTime(chunk.ReceivedAt), policy.ceiling, policy.completePoll, policy.started, policy.completed)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func chunkCheckpoint(chunk IngestChunk, policy checkpointPolicy) *time.Time {
	if policy.hold {
		return nil
	}
	var checkpoint *time.Time
	for _, raw := range chunk.Records {
		stamp := recordTimestamp(raw, chunk.Endpoint)
		if stamp == nil {
			return nil
		}
		if checkpoint == nil || stamp.After(*checkpoint) {
			checkpoint = stamp
		}
	}
	return checkpoint
}
