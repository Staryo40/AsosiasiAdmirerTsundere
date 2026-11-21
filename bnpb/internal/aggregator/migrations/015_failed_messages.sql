-- Control store only, applied idempotently like shard repair metadata: shards keep their own handled_inputs.
CREATE TABLE IF NOT EXISTS failed_messages (
    message_key bytea PRIMARY KEY CHECK (octet_length(message_key) = 32),
    message_id text NOT NULL UNIQUE CHECK (message_id <> ''),
    source text NOT NULL CHECK (source IN ('BMKG', 'PVMBG', 'SATELIT', 'SENSOR_PANTAI')),
    endpoint text NOT NULL CHECK (endpoint <> ''),
    kind text NOT NULL CHECK (kind IN ('record', 'response')),
    input_id text NOT NULL,
    input_offset bigint NOT NULL CHECK (input_offset >= 0),
    source_ref_id text,
    source_timestamp timestamptz,
    received_at timestamptz NOT NULL,
    last_seen_at timestamptz NOT NULL,
    reason text NOT NULL CHECK (reason <> ''),
    retryable boolean NOT NULL,
    attempts integer NOT NULL CHECK (attempts >= 1),
    status text NOT NULL CHECK (status IN ('retrying', 'parked', 'resolved')),
    payload bytea NOT NULL,
    payload_bytes bigint NOT NULL CHECK (payload_bytes >= octet_length(payload)),
    payload_truncated boolean NOT NULL,
    resolved_at timestamptz,
    CHECK ((status = 'resolved') = (resolved_at IS NOT NULL)),
    CHECK (retryable = (status <> 'parked') OR status = 'resolved')
);
REVOKE ALL ON failed_messages FROM PUBLIC;

CREATE INDEX IF NOT EXISTS failed_messages_open
    ON failed_messages (received_at, message_id COLLATE "C") WHERE status <> 'resolved';
CREATE INDEX IF NOT EXISTS failed_messages_replay
    ON failed_messages (endpoint, source_ref_id COLLATE "C", source_timestamp, received_at) WHERE status = 'parked';
