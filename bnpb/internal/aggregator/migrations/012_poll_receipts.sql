CREATE TABLE pending_poll_receipts (
    endpoint text PRIMARY KEY REFERENCES poll_state(endpoint),
    input_id text NOT NULL CHECK (input_id <> ''),
    started_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL,
    records bytea[] NOT NULL,
    hold_checkpoint boolean NOT NULL,
    reconciles_warnings boolean NOT NULL CHECK (NOT reconciles_warnings OR endpoint = 'warnings')
);
REVOKE ALL ON pending_poll_receipts FROM PUBLIC;
CREATE TRIGGER pending_poll_receipts_mutation_fence
    BEFORE INSERT OR UPDATE OR DELETE ON pending_poll_receipts
    FOR EACH ROW EXECUTE FUNCTION require_mutation_authority();
CREATE TRIGGER pending_poll_receipts_truncate_fence
    BEFORE TRUNCATE ON pending_poll_receipts
    FOR EACH STATEMENT EXECUTE FUNCTION require_mutation_authority();
INSERT INTO schema_migrations (version) VALUES (12);
