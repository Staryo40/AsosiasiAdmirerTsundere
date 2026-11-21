-- SATELIT fires: written by the satellite service, never polled; the endpoint exists for receipts and rejects.
ALTER TABLE poll_state DROP CONSTRAINT IF EXISTS poll_state_endpoint_check;
ALTER TABLE poll_state ADD CONSTRAINT poll_state_endpoint_check
    CHECK (endpoint IN ('seismic', 'warnings', 'volcanic', 'coastal', 'wildfire'));

ALTER TABLE poll_state DROP CONSTRAINT IF EXISTS poll_state_source_endpoint_check;
ALTER TABLE poll_state ADD CONSTRAINT poll_state_source_endpoint_check CHECK (
    (source = 'BMKG' AND endpoint IN ('seismic', 'warnings')) OR
    (source = 'PVMBG' AND endpoint = 'volcanic') OR
    (source = 'SENSOR_PANTAI' AND endpoint = 'coastal') OR
    (source = 'SATELIT' AND endpoint = 'wildfire')
);

-- An upgraded routed store already has an identity, so the mutation fence would refuse this schema row.
ALTER TABLE poll_state DISABLE TRIGGER poll_state_mutation_fence;
INSERT INTO poll_state (endpoint, source) VALUES ('wildfire', 'SATELIT') ON CONFLICT (endpoint) DO NOTHING;
ALTER TABLE poll_state ENABLE TRIGGER poll_state_mutation_fence;

-- A withdrawn fire keeps its row and hazard_id for revival, but readers count only the active population.
ALTER TABLE hazard_events ADD COLUMN withdrawn_at timestamptz;

-- One row per fire lineage; feed_revision only grows, so a resent or replayed change is a no-op.
CREATE TABLE source_wildfire (
    source_ref_id text PRIMARY KEY CHECK (source_ref_id <> '' AND octet_length(source_ref_id) <= 2048),
    hazard_id text NOT NULL UNIQUE,
    feed_revision bigint NOT NULL CHECK (feed_revision >= 0),
    active boolean NOT NULL,
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    updated_at timestamptz NOT NULL
);
CREATE TRIGGER source_wildfire_mutation_fence BEFORE INSERT OR UPDATE OR DELETE ON source_wildfire
    FOR EACH ROW EXECUTE FUNCTION require_mutation_authority();
CREATE TRIGGER source_wildfire_truncate_fence BEFORE TRUNCATE ON source_wildfire
    FOR EACH STATEMENT EXECUTE FUNCTION require_mutation_authority();

INSERT INTO schema_migrations (version) VALUES (15);
