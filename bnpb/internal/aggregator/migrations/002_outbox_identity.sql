ALTER TABLE outbox DROP CONSTRAINT outbox_check;
ALTER TABLE outbox ADD CONSTRAINT outbox_notification_identity CHECK (
    (payload->>'message_id' = message_id AND payload->>'hazard_id' = hazard_id
     AND (payload->>'revision')::bigint = revision
     AND payload->>'correlation_id' = correlation_id) IS TRUE
);

INSERT INTO schema_migrations (version) VALUES (2);
