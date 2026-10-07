-- The dispatcher must wait for the oldest pending row, including during backoff.
DROP INDEX outbox_pending;
CREATE INDEX outbox_pending ON outbox (sequence) WHERE sent_at IS NULL;

INSERT INTO schema_migrations (version) VALUES (4);
