CREATE TABLE volcano_reference (
    volcano_id text PRIMARY KEY,
    name text NOT NULL,
    latitude double precision NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude double precision NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    provenance jsonb NOT NULL CHECK (jsonb_typeof(provenance) = 'object')
);

CREATE TABLE hazard_events (
    hazard_id text PRIMARY KEY,
    source text NOT NULL CHECK (source IN ('BMKG', 'PVMBG')),
    source_ref_id text NOT NULL CHECK (octet_length(source_ref_id) BETWEEN 1 AND 2048),
    hazard_type text NOT NULL,
    severity text NOT NULL CHECK (severity IN ('NORMAL', 'WASPADA', 'SIAGA', 'AWAS')),
    area_name text NOT NULL CHECK (area_name <> ''),
    latitude double precision NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude double precision NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    occurred_at timestamptz NOT NULL,
    ingested_at timestamptz NOT NULL,
    attributes jsonb NOT NULL CHECK (jsonb_typeof(attributes) = 'object'),
    revision bigint NOT NULL CHECK (revision > 0),
    updated_at timestamptz NOT NULL,
    UNIQUE (source, source_ref_id),
    CHECK ((source = 'BMKG' AND hazard_type = 'SEISMIC') OR
           (source = 'PVMBG' AND hazard_type = 'VOLCANIC'))
);
CREATE INDEX hazard_source_page ON hazard_events (source, occurred_at DESC, hazard_id DESC);
CREATE INDEX hazard_page ON hazard_events (occurred_at DESC, hazard_id DESC);

CREATE TABLE source_seismic (
    event_id text PRIMARY KEY CHECK (octet_length(event_id) BETWEEN 1 AND 2048),
    occurred_at timestamptz NOT NULL,
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object')
);

-- Warnings can arrive before their seismic event, so this is deliberately not a foreign key.
CREATE TABLE source_warnings (
    warning_id text PRIMARY KEY CHECK (octet_length(warning_id) BETWEEN 1 AND 2048),
    related_event_id text NOT NULL CHECK (octet_length(related_event_id) BETWEEN 1 AND 2048),
    issued_at timestamptz NOT NULL,
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object')
);
CREATE INDEX warning_related_event ON source_warnings (related_event_id, issued_at, warning_id);

CREATE TABLE poll_state (
    endpoint text PRIMARY KEY CHECK (endpoint IN ('seismic', 'warnings', 'volcanic')),
    source text NOT NULL,
    checkpoint timestamptz,
    last_attempt_at timestamptz,
    last_successful_poll timestamptz,
    stale_since timestamptz,
    error_category text,
    CHECK ((source = 'BMKG' AND endpoint IN ('seismic', 'warnings')) OR
           (source = 'PVMBG' AND endpoint = 'volcanic'))
);
INSERT INTO poll_state (endpoint, source) VALUES
    ('seismic', 'BMKG'), ('warnings', 'BMKG'), ('volcanic', 'PVMBG');

-- Fingerprints retain traceability without retaining credentials or unbounded rejected payloads.
CREATE TABLE ingest_rejects (
    reject_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    endpoint text NOT NULL REFERENCES poll_state (endpoint),
    source_ref_id text CHECK (octet_length(source_ref_id) <= 2048),
    source_timestamp timestamptz,
    category text NOT NULL,
    field text NOT NULL,
    payload_sha256 text NOT NULL CHECK (length(payload_sha256) = 64),
    payload_bytes bigint NOT NULL CHECK (payload_bytes >= 0),
    first_seen_at timestamptz NOT NULL,
    last_seen_at timestamptz NOT NULL,
    occurrences bigint NOT NULL DEFAULT 1 CHECK (occurrences > 0),
    repair_status text NOT NULL DEFAULT 'open' CHECK (repair_status IN ('open', 'repaired')),
    UNIQUE (endpoint, payload_sha256, category, field)
);

CREATE TABLE outbox (
    sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    message_id text NOT NULL UNIQUE,
    hazard_id text NOT NULL REFERENCES hazard_events (hazard_id),
    revision bigint NOT NULL CHECK (revision > 0),
    correlation_id text NOT NULL,
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    created_at timestamptz NOT NULL,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at timestamptz NOT NULL,
    sent_at timestamptz,
    UNIQUE (hazard_id, revision),
    CHECK (payload->>'message_id' = message_id AND payload->>'hazard_id' = hazard_id
           AND (payload->>'revision')::bigint = revision
           AND payload->>'correlation_id' = correlation_id)
);
CREATE INDEX outbox_pending ON outbox (next_attempt_at, sequence) WHERE sent_at IS NULL;

CREATE FUNCTION protect_outbox_notification() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.sequence, NEW.message_id, NEW.hazard_id, NEW.revision,
           NEW.correlation_id, NEW.payload, NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.sequence, OLD.message_id, OLD.hazard_id, OLD.revision,
           OLD.correlation_id, OLD.payload, OLD.created_at) THEN
        RAISE EXCEPTION 'outbox notification is immutable' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER outbox_immutable BEFORE UPDATE ON outbox
    FOR EACH ROW EXECUTE FUNCTION protect_outbox_notification();
