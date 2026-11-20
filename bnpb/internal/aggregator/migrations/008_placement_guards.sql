-- Refuse an already-invalid inventory rather than erase copies to make it fit.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM placement_revision WHERE singleton) OR EXISTS (
        SELECT node FROM shard_placements GROUP BY node HAVING count(DISTINCT shard_id) >= 3
    ) THEN
        RAISE EXCEPTION 'placement inventory requires reconciliation' USING ERRCODE = '23514';
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION protect_shard_placement() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP IN ('DELETE', 'TRUNCATE') THEN
        RAISE EXCEPTION 'physical copies cannot be forgotten without retirement proof' USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'UPDATE' AND ROW(NEW.unit, NEW.shard_id, NEW.node)
        IS DISTINCT FROM ROW(OLD.unit, OLD.shard_id, OLD.node) THEN
        RAISE EXCEPTION 'copy identity is immutable; add a new physical copy' USING ERRCODE = '23514';
    END IF;

    -- A real write forces stale repeatable-read snapshots to abort; a row lock alone does not.
    -- The opaque revision also invalidates CAS after any direct SQL mutation. Upserts can
    -- run both BEFORE INSERT and BEFORE UPDATE, so callers must not assume a +1 delta.
    UPDATE placement_revision SET revision = revision + 1 WHERE singleton;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'placement revision is unavailable' USING ERRCODE = '23514';
    END IF;
    IF (SELECT count(DISTINCT shard_id) FROM shard_placements
        WHERE node = NEW.node AND unit <> NEW.unit AND shard_id <> NEW.shard_id) >= 2 THEN
        RAISE EXCEPTION 'a node cannot hold all logical shards' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER shard_placement_no_delete BEFORE DELETE ON shard_placements
    FOR EACH ROW EXECUTE FUNCTION protect_shard_placement();
CREATE TRIGGER shard_placement_no_truncate BEFORE TRUNCATE ON shard_placements
    FOR EACH STATEMENT EXECUTE FUNCTION protect_shard_placement();

INSERT INTO schema_migrations (version) VALUES (8);
