-- AI-assisted implementation; see contracts/v1/README.md. No physical deployment is implied.
CREATE TABLE logical_shards (
    shard_id smallint PRIMARY KEY CHECK (shard_id BETWEEN 0 AND 2),
    name text NOT NULL UNIQUE,
    hash_algorithm text NOT NULL CHECK (hash_algorithm = 'sha256-32be-v1'),
    CHECK (name = 'shard-' || shard_id::text)
);
INSERT INTO logical_shards VALUES
    (0, 'shard-0', 'sha256-32be-v1'),
    (1, 'shard-1', 'sha256-32be-v1'),
    (2, 'shard-2', 'sha256-32be-v1');

CREATE FUNCTION canonical_shard(source_name text, source_reference text)
RETURNS smallint LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE digest bytea;
BEGIN
    digest := sha256(convert_to(source_name, 'UTF8') || decode('00', 'hex') || convert_to(source_reference, 'UTF8'));
    RETURN ((get_byte(digest, 0)::bigint * 16777216 + get_byte(digest, 1)::bigint * 65536
        + get_byte(digest, 2)::bigint * 256 + get_byte(digest, 3)::bigint) % 3)::smallint;
END;
$$;

-- Generated metadata backfills existing identities without changing their public fields.
ALTER TABLE hazard_events ADD COLUMN logical_shard_id smallint
    GENERATED ALWAYS AS (canonical_shard(source, source_ref_id)) STORED
    REFERENCES logical_shards (shard_id);
ALTER TABLE source_seismic ADD COLUMN logical_shard_id smallint
    GENERATED ALWAYS AS (canonical_shard('BMKG', event_id)) STORED
    REFERENCES logical_shards (shard_id);
ALTER TABLE source_warnings ADD COLUMN logical_shard_id smallint
    GENERATED ALWAYS AS (canonical_shard('BMKG', related_event_id)) STORED
    REFERENCES logical_shards (shard_id);

CREATE TABLE placement_revision (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    revision bigint NOT NULL DEFAULT 0 CHECK (revision >= 0)
);
INSERT INTO placement_revision DEFAULT VALUES;

-- Empty until the Aggregator records real node/unit identities; never seed invented locations.
CREATE TABLE shard_placements (
    unit text PRIMARY KEY CHECK (unit <> ''),
    shard_id smallint NOT NULL REFERENCES logical_shards (shard_id),
    node text NOT NULL CHECK (node <> ''),
    role text NOT NULL CHECK (role IN ('primary', 'replica')),
    state text NOT NULL CHECK (state IN ('copying', 'ready', 'retiring')),
    UNIQUE (shard_id, node),
    CHECK (role <> 'primary' OR state <> 'copying')
);
CREATE UNIQUE INDEX shard_ready_primary ON shard_placements (shard_id)
    WHERE role = 'primary' AND state = 'ready';

-- All copies, including incomplete and retiring copies, consume a node's shard budget.
-- The singleton lock serializes concurrent placement changes, not canonical business writes.
CREATE FUNCTION protect_shard_placement() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM 1 FROM placement_revision WHERE singleton FOR UPDATE;
    IF TG_OP = 'UPDATE' AND ROW(NEW.unit, NEW.shard_id, NEW.node)
        IS DISTINCT FROM ROW(OLD.unit, OLD.shard_id, OLD.node) THEN
        RAISE EXCEPTION 'copy identity is immutable; add a new physical copy' USING ERRCODE = '23514';
    END IF;
    IF (SELECT count(DISTINCT shard_id) FROM shard_placements
        WHERE node = NEW.node AND unit <> NEW.unit AND shard_id <> NEW.shard_id) >= 2 THEN
        RAISE EXCEPTION 'a node cannot hold all logical shards' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER shard_placement_guard BEFORE INSERT OR UPDATE ON shard_placements
    FOR EACH ROW EXECUTE FUNCTION protect_shard_placement();

INSERT INTO schema_migrations (version) VALUES (6);
