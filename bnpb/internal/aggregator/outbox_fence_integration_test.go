//go:build integration

package aggregator

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

type captureOutboxPublisher struct {
	published []Publication
}

func (publisher *captureOutboxPublisher) Publish(_ context.Context, message Publication) error {
	message.Payload = append([]byte(nil), message.Payload...)
	publisher.published = append(publisher.published, message)
	return nil
}

func (publisher *captureOutboxPublisher) Close() {}

func TestOutboxFenceIntegrationRetriesIdenticalPublishAfterSentFailure(t *testing.T) {
	cfg := routedM2Config(t)
	store := openRoutedM2Store(t, cfg)
	ownerContext := activateOutboxFixtureOwner(t, cfg, store, fixtureMutationOwner)
	identity := routedShardIdentities(t)[2]
	stamp := time.Date(2026, 11, 18, 1, 0, 0, 0, time.UTC)
	record := routedRecord(t, identity, stamp)
	if err := store.Ingest(ownerContext, IngestChunk{Endpoint: SeismicEndpoint, Records: []json.RawMessage{record}, ReceivedAt: stamp.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	shard := store.shards[2]
	admin, err := pgx.Connect(t.Context(), cfg.ShardURLs[2])
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(t.Context())
	if _, err := admin.Exec(t.Context(), `CREATE FUNCTION fail_outbox_sent() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.sent_at IS NOT NULL AND NEW.attempts = 1 THEN RAISE EXCEPTION 'injected sent-state failure'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER fail_outbox_sent BEFORE UPDATE ON outbox FOR EACH ROW EXECUTE FUNCTION fail_outbox_sent()`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = admin.Exec(context.Background(), "DROP TRIGGER IF EXISTS fail_outbox_sent ON outbox; DROP FUNCTION IF EXISTS fail_outbox_sent()")
	}()
	messages, err := shard.pendingOutbox(t.Context(), 10)
	if err != nil || len(messages) != 1 {
		t.Fatalf("pending outbox messages=%d err=%v", len(messages), err)
	}
	publisher := &captureOutboxPublisher{}
	dispatcher := outboxDispatcher{store: store, publisher: publisher, cfg: cfg, logger: quietLogger()}
	message := messages[0]
	message.store = shard
	dispatchErr := dispatcher.dispatchMessage(ownerContext, message)
	if dispatchErr == nil {
		t.Fatal("sent-state trigger failure did not escape dispatch")
	}
	_, retryErr := dispatcher.retryMessage(ownerContext, message, dispatchErr)
	if retryErr == nil || !errors.Is(retryErr, dispatchErr) {
		t.Fatalf("retry result=%v, want original dispatch failure %v", retryErr, dispatchErr)
	}
	if _, err := admin.Exec(t.Context(), "DROP TRIGGER fail_outbox_sent ON outbox"); err != nil {
		t.Fatal(err)
	}
	var retryAt time.Time
	if err := shard.read.QueryRow(t.Context(), "SELECT next_attempt_at FROM outbox WHERE sequence=$1", message.Sequence).Scan(&retryAt); err != nil || !retryAt.After(time.Now()) {
		t.Fatalf("retry state was not persisted: next_attempt_at=%v err=%v", retryAt, err)
	}
	pending, err := dispatcher.pendingMessages(ownerContext)
	if err != nil || len(pending) != 1 || !samePublication(message.Publication, pending[0].Publication) {
		t.Fatalf("retry did not reload the identical stored publication: messages=%d err=%v", len(pending), err)
	}
	if err := dispatcher.dispatchMessage(ownerContext, pending[0]); err != nil {
		t.Fatal(err)
	}
	if len(publisher.published) != 2 || !samePublication(publisher.published[0], publisher.published[1]) {
		t.Fatalf("uncertain publish did not retry identical publication: %d deliveries", len(publisher.published))
	}
	var sent int
	if err := shard.read.QueryRow(t.Context(), "SELECT count(*) FROM outbox WHERE sequence=$1 AND sent_at IS NOT NULL", message.Sequence).Scan(&sent); err != nil || sent != 1 {
		t.Fatalf("sent row count=%d err=%v", sent, err)
	}
}

func TestOutboxFenceIntegrationRejectsStaleMutationAndFollowerDispatch(t *testing.T) {
	cfg := routedM2Config(t)
	store := openRoutedM2Store(t, cfg)
	ownerContext := activateOutboxFixtureOwner(t, cfg, store, fixtureMutationOwner)
	_, epoch, ok := pollAuthorityToken(ownerContext)
	if !ok {
		t.Fatal("activated owner context has no mutation token")
	}
	identity := routedShardIdentities(t)[0]
	stamp := time.Date(2026, 11, 18, 2, 0, 0, 0, time.UTC)
	record := routedRecord(t, identity, stamp)
	if err := store.Ingest(ownerContext, IngestChunk{Endpoint: SeismicEndpoint, Records: []json.RawMessage{record}, ReceivedAt: stamp.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	shard := store.shards[0]
	messages, err := shard.pendingOutbox(t.Context(), 10)
	if err != nil || len(messages) != 1 {
		t.Fatalf("pending outbox messages=%d err=%v", len(messages), err)
	}
	stale := fixtureMutationContext(t, t.Context(), fixtureMutationOwner, epoch-1, shard)
	if err := shard.attemptOutbox(stale, messages[0].Sequence); err == nil {
		t.Fatal("stale owner incremented outbox attempts")
	}
	if err := shard.sentOutbox(stale, messages[0].Sequence); err == nil {
		t.Fatal("stale owner marked outbox sent")
	}
	if err := shard.retryOutbox(stale, messages[0].Sequence, time.Now()); err == nil {
		t.Fatal("stale owner changed retry time")
	}
	publisher := &captureOutboxPublisher{}
	dispatcher := outboxDispatcher{store: store, publisher: publisher, cfg: cfg, logger: quietLogger()}
	store.authority = nil
	if _, err := dispatcher.dispatchPending(ownerContext); err == nil {
		t.Fatal("routed follower dispatch did not fail closed without owner authority")
	}
	if len(publisher.published) != 0 {
		t.Fatalf("follower published %d messages", len(publisher.published))
	}
	var attempts int
	if err := shard.read.QueryRow(t.Context(), "SELECT attempts FROM outbox WHERE sequence=$1", messages[0].Sequence).Scan(&attempts); err != nil || attempts != 0 {
		t.Fatalf("stale mutations changed attempts to %d: %v", attempts, err)
	}
}

func TestOutboxFenceIntegrationDispatchesHealthyShardWhenSiblingIsUnavailable(t *testing.T) {
	cfg := routedM2Config(t)
	store := openRoutedM2Store(t, cfg)
	ownerContext := activateOutboxFixtureOwner(t, cfg, store, fixtureMutationOwner)
	stamp := time.Now().UTC()
	record := routedRecord(t, routedShardIdentities(t)[2], stamp)
	if err := store.Ingest(ownerContext, IngestChunk{Endpoint: SeismicEndpoint, Records: []json.RawMessage{record}, ReceivedAt: stamp}); err != nil {
		t.Fatal(err)
	}
	store.shards[0].write.Close()
	publisher := &captureOutboxPublisher{}
	dispatcher := outboxDispatcher{store: store, publisher: publisher, cfg: cfg, logger: quietLogger()}
	_, err := dispatcher.dispatchPending(ownerContext)
	if err == nil {
		t.Fatal("unavailable shard was not reported")
	}
	if len(publisher.published) != 1 {
		t.Fatalf("healthy shard delivered %d publications, want 1", len(publisher.published))
	}
	var sent int
	if err := store.shards[2].read.QueryRow(t.Context(), "SELECT count(*) FROM outbox WHERE sent_at IS NOT NULL").Scan(&sent); err != nil || sent != 1 {
		t.Fatalf("healthy shard sent row count=%d err=%v", sent, err)
	}
}

func activateOutboxFixtureOwner(t *testing.T, cfg Config, store *Store, owner string) context.Context {
	t.Helper()
	kube := newRecoveryAuthorityKubernetes(recoveryClosedRecord(0))
	host := newRecoveryHostFence()
	authority := openRecoveryAuthority(t, cfg, store, kube, host, owner)
	if _, _, err := authority.activate(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, err := authority.currentContext(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	store.authority = authority
	return ctx
}

func samePublication(left, right Publication) bool {
	return left.MessageID == right.MessageID && left.HazardID == right.HazardID && left.Revision == right.Revision && left.CorrelationID == right.CorrelationID && left.EventType == right.EventType && left.SchemaVersion == right.SchemaVersion && left.ProducedAt.Equal(right.ProducedAt) && string(left.Payload) == string(right.Payload)
}
