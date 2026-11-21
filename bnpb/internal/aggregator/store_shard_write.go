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
	if len(chunk.Records) == 0 {
		return store.writeShardCheckpoint(ctx, chunk, policy)
	}
	bindings := planReceiptBindings(chunk)
	if err := store.bindReceipt(ctx, bindings); err != nil {
		if errors.Is(err, errReceiptConflict) {
			return errReceiptConflict
		}
		return storeError("bind routed receipt", err)
	}
	outcomes := make([]recordOutcome, 0, len(chunk.Records))
	for index, raw := range chunk.Records {
		recordChunk := chunk
		recordChunk.InputOffset += index
		shardID := bindings.destinations[index]
		if shardID == controlReceiptDestination {
			outcome, err := store.writeUnroutable(ctx, recordChunk, raw, bindings.routeErrors[index])
			if err != nil {
				return storeError("retain unroutable input", err)
			}
			outcome.offset = index
			outcomes = append(outcomes, outcome)
			continue
		}
		shard, err := store.shardStore(ctx, shardID)
		if err != nil {
			store.noteShardFailure(ctx, recordChunk, raw, shardID, err)
			return storeError("resolve input shard", err)
		}
		outcome, err := shard.writeShardInput(ctx, recordChunk, raw)
		if err != nil {
			store.noteShardFailure(ctx, recordChunk, raw, shardID, err)
			return storeError("write shard input", err)
		}
		outcome.offset = index
		outcomes = append(outcomes, outcome)
	}
	// Shard outcomes are already durable; a lost write here is repeated when the receipt is retried.
	if err := store.recordRoutedOutcomes(ctx, chunk, outcomes); err != nil {
		return storeError("record failed messages", err)
	}
	return store.writeShardCheckpoint(ctx, chunk, policy)
}

const controlReceiptDestination int16 = -1

var errReceiptConflict = errors.New("receipt ordinal already binds different content or destination")

type receiptBindings struct {
	endpoint        string
	hashes, digests [][]byte
	destinations    []int16
	routeErrors     []error
}

func planReceiptBindings(chunk IngestChunk) receiptBindings {
	count := len(chunk.Records)
	bindings := receiptBindings{
		endpoint: string(chunk.Endpoint),
		hashes:   make([][]byte, count), digests: make([][]byte, count), destinations: make([]int16, count),
	}
	digests := make([][sha256.Size]byte, count)
	for index, raw := range chunk.Records {
		bindings.hashes[index] = inputHash(chunk)
		digests[index] = sha256.Sum256(raw)
		bindings.digests[index] = digests[index][:]
		destination, err := inputShard(chunk.Endpoint, raw)
		if err != nil {
			if bindings.routeErrors == nil {
				bindings.routeErrors = make([]error, count)
			}
			bindings.routeErrors[index], destination = err, controlReceiptDestination
		}
		bindings.destinations[index] = destination
		if index+1 < count {
			chunk.InputOffset++
		}
	}
	return bindings
}

const receiptBindingEntries = `unnest($2::bytea[], $3::bytea[], $4::smallint[])
	WITH ORDINALITY AS expected(input_hash, raw_digest, destination, ordinal)`

func (store *Store) bindReceipt(ctx context.Context, bindings receiptBindings) (err error) {
	ctx, cancel := context.WithTimeout(ctx, store.writeTimeout)
	defer cancel()
	tx, err := store.beginMutation(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if cleanupErr := rollbackTransaction(ctx, tx); cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
		}
	}()
	inserted, err := bindings.reserve(ctx, tx)
	if err != nil {
		return err
	}
	if len(inserted) != 0 {
		if err := store.verifyLegacyReceipt(ctx, tx, bindings, inserted); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (bindings receiptBindings) reserve(ctx context.Context, tx pgx.Tx) ([][]byte, error) {
	var inserted [][]byte
	// An overlapping retry must reserve its ordinals in the same order. DO NOTHING
	// waits for a concurrent winner; the next Read Committed statement sees its binding.
	err := tx.QueryRow(ctx, `WITH inserted AS (
		INSERT INTO receipt_bindings (endpoint, input_hash, raw_digest, destination)
		SELECT $1, input_hash, raw_digest, destination FROM `+receiptBindingEntries+` ORDER BY ordinal
		ON CONFLICT (endpoint, input_hash) DO NOTHING RETURNING input_hash
	) SELECT array_agg(input_hash) FROM inserted`,
		bindings.endpoint, bindings.hashes, bindings.digests, bindings.destinations).Scan(&inserted)
	if err != nil || len(inserted) == len(bindings.hashes) {
		return inserted, err
	}
	err = rejectReceiptConflict(tx.QueryRow(ctx, `SELECT EXISTS (
		SELECT FROM `+receiptBindingEntries+` WHERE NOT EXISTS (
			SELECT FROM receipt_bindings actual WHERE actual.endpoint=$1
			AND (actual.input_hash, actual.raw_digest, actual.destination)
				= (expected.input_hash, expected.raw_digest, expected.destination)
		))`, bindings.endpoint, bindings.hashes, bindings.digests, bindings.destinations))
	return inserted, err
}

type receiptBindingReader interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func (bindings receiptBindings) checkLegacy(ctx context.Context, db receiptBindingReader, inserted [][]byte, destination int16) error {
	return rejectReceiptConflict(db.QueryRow(ctx, `SELECT EXISTS (
		SELECT FROM `+receiptBindingEntries+` JOIN handled_inputs old USING (input_hash)
		WHERE old.endpoint=$1 AND old.input_hash=ANY($5::bytea[])
		AND (sha256(old.raw_payload)<>expected.raw_digest OR $6::smallint<>expected.destination)
	)`, bindings.endpoint, bindings.hashes, bindings.digests, bindings.destinations, inserted, destination))
}

func (store *Store) verifyLegacyReceipt(ctx context.Context, tx pgx.Tx, bindings receiptBindings, inserted [][]byte) error {
	// Pre-binding ledgers may hold these keys on any store. A missing store is
	// not evidence of absence, and a second placement is always a conflict.
	if err := bindings.checkLegacy(ctx, tx, inserted, controlReceiptDestination); err != nil {
		return err
	}
	for id := range store.shards {
		destination := int16(id)
		shard, err := store.shardStore(ctx, destination)
		if err != nil {
			return err
		}
		if err := shard.verifyStartupResource(ctx); err != nil {
			return err
		}
		if err := bindings.checkLegacy(ctx, shard.read, inserted, destination); err != nil {
			return err
		}
	}
	return nil
}

func rejectReceiptConflict(row pgx.Row) error {
	var conflict bool
	if err := row.Scan(&conflict); err != nil {
		return err
	}
	if conflict {
		return errReceiptConflict
	}
	return nil
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

func (store *Store) writeShardInput(ctx context.Context, chunk IngestChunk, raw json.RawMessage) (outcome recordOutcome, err error) {
	ctx, cancel := context.WithTimeout(ctx, store.writeTimeout)
	defer cancel()
	outcome.raw = raw
	tx, err := store.beginWriteMutation(ctx)
	if err != nil {
		return outcome, err
	}
	defer func() {
		if cleanupErr := rollbackTransaction(ctx, tx); cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
		}
	}()
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(4031, $1)", ingestEndpoints[chunk.Endpoint].lock); err != nil {
		return outcome, err
	}
	status, exists, err := handledInputStatus(ctx, tx, chunk, raw)
	if err != nil {
		return outcome, err
	}
	if exists {
		outcome.applied = status == "applied"
		if status == "retained" {
			if outcome.applied, outcome.reason, err = retryRetainedInput(ctx, tx, store, chunk, raw); err != nil {
				return outcome, err
			}
		}
		return outcome, tx.Commit(ctx)
	}
	applyTx, err := tx.Begin(ctx)
	if err != nil {
		return outcome, err
	}
	applyErr := store.writeSourceRecord(ctx, applyTx, chunk, raw)
	status, reason := "applied", ""
	if applyErr == nil {
		if err = applyTx.Commit(ctx); err != nil {
			return outcome, err
		}
	} else {
		if cleanupErr := rollbackTransaction(ctx, applyTx); cleanupErr != nil {
			return outcome, errors.Join(applyErr, cleanupErr)
		}
		category, field := rejectReason(applyErr)
		if category == "" {
			return outcome, applyErr
		}
		status, reason = "retained", category+":"+field
		if err = writeReject(ctx, tx, chunk, rejectedRecord{raw, ValidationError{Code: ValidationCode(category), Field: field}}); err != nil {
			return outcome, err
		}
	}
	if err := insertHandledInput(ctx, tx, chunk, raw, status, reason); err != nil {
		return outcome, err
	}
	outcome.applied, outcome.reason = status == "applied", displayReason(reason)
	return outcome, tx.Commit(ctx)
}

// A receipt retained as unroutable in the control store stays there; schema fixes cannot infer its owner.
func (store *Store) writeUnroutable(ctx context.Context, chunk IngestChunk, raw json.RawMessage, cause error) (outcome recordOutcome, err error) {
	ctx, cancel := context.WithTimeout(ctx, store.writeTimeout)
	defer cancel()
	category, field := rejectReason(cause)
	if category == "" {
		category, field = "invalid_value", "identity"
	}
	outcome = recordOutcome{raw: raw, reason: failedReason(category, field)}
	tx, err := store.beginWriteMutation(ctx)
	if err != nil {
		return outcome, err
	}
	defer func() {
		if cleanupErr := rollbackTransaction(ctx, tx); cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
		}
	}()
	// An identical retry can overlap before the first retained outcome exists.
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(4031, $1)", ingestEndpoints[chunk.Endpoint].lock); err != nil {
		return outcome, err
	}
	_, exists, err := handledInputStatus(ctx, tx, chunk, raw)
	if err != nil {
		return outcome, err
	}
	if !exists {
		if err = writeReject(ctx, tx, chunk, rejectedRecord{raw, ValidationError{Code: ValidationCode(category), Field: field}}); err != nil {
			return outcome, err
		}
		if err = insertHandledInput(ctx, tx, chunk, raw, "retained", "unroutable_identity"); err != nil {
			return outcome, err
		}
	}
	return outcome, tx.Commit(ctx)
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
func retryRetainedInput(ctx context.Context, tx pgx.Tx, store *Store, chunk IngestChunk, raw json.RawMessage) (bool, string, error) {
	applyTx, err := tx.Begin(ctx)
	if err != nil {
		return false, "", err
	}
	if err := store.writeSourceRecord(ctx, applyTx, chunk, raw); err != nil {
		if cleanupErr := rollbackTransaction(ctx, applyTx); cleanupErr != nil {
			return false, "", errors.Join(err, cleanupErr)
		}
		if category, field := rejectReason(err); category != "" {
			return false, failedReason(category, field), nil
		}
		return false, "", err
	}
	if err := applyTx.Commit(ctx); err != nil {
		return false, "", err
	}
	tag, err := tx.Exec(ctx, `UPDATE handled_inputs SET status='applied', reason=NULL
		WHERE endpoint=$1 AND input_hash=$2 AND status='retained'`, string(chunk.Endpoint), inputHash(chunk))
	if err != nil {
		return false, "", err
	}
	if tag.RowsAffected() != 1 {
		return false, "", errors.New("retained handled input transition lost")
	}
	return true, "", nil
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
