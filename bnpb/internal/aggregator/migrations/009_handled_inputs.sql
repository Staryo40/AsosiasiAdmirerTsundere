
CREATE TABLE handled_inputs (
    endpoint text NOT NULL CHECK (endpoint <> ''),
    input_hash bytea NOT NULL CHECK (octet_length(input_hash) = 32),
    raw_payload bytea NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    status text NOT NULL CHECK (status IN ('applied', 'retained')),
    reason text,
    PRIMARY KEY (endpoint, input_hash),
    CHECK ((status = 'applied' AND reason IS NULL) OR (status = 'retained' AND reason IS NOT NULL))
);

CREATE FUNCTION prevent_handled_input_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE'
       AND OLD.status = 'retained'
       AND NEW.status = 'applied'
       AND NEW.reason IS NULL
       AND ROW(NEW.endpoint, NEW.input_hash, NEW.raw_payload, NEW.received_at)
           IS NOT DISTINCT FROM ROW(OLD.endpoint, OLD.input_hash, OLD.raw_payload, OLD.received_at) THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'handled input outcome is immutable' USING ERRCODE = '23514';
END;
$$;
CREATE TRIGGER handled_inputs_immutable BEFORE UPDATE OR DELETE ON handled_inputs
    FOR EACH ROW EXECUTE FUNCTION prevent_handled_input_mutation();
CREATE TRIGGER handled_inputs_immutable_truncate BEFORE TRUNCATE ON handled_inputs
    FOR EACH STATEMENT EXECUTE FUNCTION prevent_handled_input_mutation();

CREATE TABLE store_identity (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    role text NOT NULL CHECK (role IN ('control', 'shard')),
    shard_id smallint REFERENCES logical_shards (shard_id),
    CHECK ((role = 'control' AND shard_id IS NULL) OR (role = 'shard' AND shard_id IS NOT NULL))
);

CREATE FUNCTION enforce_store_shard_identity() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE configured_role text; configured_shard smallint; row_shard smallint;
BEGIN
    SELECT role, shard_id INTO configured_role, configured_shard FROM store_identity WHERE singleton;
    IF FOUND AND configured_role = 'control' THEN
        RAISE EXCEPTION 'control database cannot store canonical rows' USING ERRCODE = '23514';
    ELSIF FOUND THEN
        CASE TG_TABLE_NAME
            WHEN 'hazard_events' THEN row_shard := canonical_shard(NEW.source, NEW.source_ref_id);
            WHEN 'source_seismic' THEN row_shard := canonical_shard('BMKG', NEW.event_id);
            WHEN 'source_warnings' THEN row_shard := canonical_shard('BMKG', NEW.related_event_id);
        END CASE;
        IF row_shard <> configured_shard THEN
            RAISE EXCEPTION 'row belongs to a different logical shard' USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER hazard_events_store_shard BEFORE INSERT OR UPDATE ON hazard_events
    FOR EACH ROW EXECUTE FUNCTION enforce_store_shard_identity();
CREATE TRIGGER source_seismic_store_shard BEFORE INSERT OR UPDATE ON source_seismic
    FOR EACH ROW EXECUTE FUNCTION enforce_store_shard_identity();
CREATE TRIGGER source_warnings_store_shard BEFORE INSERT OR UPDATE ON source_warnings
    FOR EACH ROW EXECUTE FUNCTION enforce_store_shard_identity();

CREATE FUNCTION prevent_store_identity_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'store identity is immutable' USING ERRCODE = '23514';
END;
$$;
CREATE TRIGGER store_identity_immutable BEFORE UPDATE OR DELETE ON store_identity
    FOR EACH ROW EXECUTE FUNCTION prevent_store_identity_mutation();
CREATE TRIGGER store_identity_no_truncate BEFORE TRUNCATE ON store_identity
    FOR EACH STATEMENT EXECUTE FUNCTION prevent_store_identity_mutation();

INSERT INTO schema_migrations (version) VALUES (9);
