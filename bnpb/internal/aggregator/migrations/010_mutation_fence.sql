CREATE TABLE mutation_guard (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    state text NOT NULL CHECK (state IN ('closed', 'active')),
    owner text NOT NULL,
    epoch bigint NOT NULL CHECK (epoch >= 0),
    pg_generation bigint NOT NULL CHECK (pg_generation >= 0),
    resource_identity text NOT NULL,
    CHECK ((state = 'closed' AND owner = '') OR (state = 'active' AND owner <> ''))
);
INSERT INTO mutation_guard (singleton, state, owner, epoch, pg_generation, resource_identity)
    VALUES (true, 'closed', '', 0, 0, '');
REVOKE ALL ON mutation_guard FROM PUBLIC;

CREATE FUNCTION require_mutation_authority() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
    SET search_path = pg_catalog AS $$
DECLARE
    routed boolean := false;
    guard_state text;
    guard_owner text;
    guard_epoch bigint;
    guard_generation bigint;
    guard_resource text;
BEGIN
    IF to_regclass(format('%I.store_identity', TG_TABLE_SCHEMA)) IS NOT NULL THEN
        EXECUTE format('SELECT EXISTS (SELECT 1 FROM %I.store_identity)', TG_TABLE_SCHEMA) INTO routed;
    END IF;
    IF NOT routed THEN
        RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
    END IF;

    EXECUTE format('SELECT state, owner, epoch, pg_generation, resource_identity
        FROM %I.mutation_guard WHERE singleton FOR SHARE', TG_TABLE_SCHEMA)
        INTO guard_state, guard_owner, guard_epoch, guard_generation, guard_resource;
    IF guard_state IS DISTINCT FROM 'active'
        OR current_setting('app.mutation_owner', true) IS DISTINCT FROM guard_owner
        OR current_setting('app.mutation_epoch', true) IS DISTINCT FROM guard_epoch::text
        OR current_setting('app.pg_generation', true) IS DISTINCT FROM guard_generation::text
        OR current_setting('app.resource_identity', true) IS DISTINCT FROM guard_resource THEN
        RAISE EXCEPTION 'distributed mutation lacks current owner authority' USING ERRCODE = '42501';
    END IF;
    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$;
REVOKE ALL ON FUNCTION require_mutation_authority() FROM PUBLIC;

CREATE FUNCTION prepare_mutation_generation(guard_table regclass) RETURNS bigint
    LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
    system_identifier text;
    timeline_id integer;
    current_resource text;
    existing_state text;
    existing_owner text;
    existing_generation bigint;
    existing_resource text;
BEGIN
    SELECT s.system_identifier::text INTO system_identifier FROM pg_control_system() s;
    SELECT c.timeline_id INTO timeline_id FROM pg_control_checkpoint() c;
    current_resource := current_database() || ':' || guard_table::text || ':system:' || system_identifier || ':timeline:' || timeline_id::text;
    EXECUTE format('SELECT state, owner, pg_generation, resource_identity FROM %s WHERE singleton FOR UPDATE', guard_table)
        INTO existing_state, existing_owner, existing_generation, existing_resource;
    IF existing_state IS DISTINCT FROM 'closed' OR existing_owner IS DISTINCT FROM '' THEN
        RAISE EXCEPTION 'mutation guard must be closed before PostgreSQL generation reconciliation' USING ERRCODE = '42501';
    END IF;
    IF existing_resource IS DISTINCT FROM current_resource THEN
        IF existing_generation = 9223372036854775807 THEN
            RAISE EXCEPTION 'PostgreSQL generation exhausted' USING ERRCODE = '22003';
        END IF;
        existing_generation := GREATEST(existing_generation + 1, 1);
        EXECUTE format('UPDATE %s SET pg_generation = $1, resource_identity = $2 WHERE singleton', guard_table)
            USING existing_generation, current_resource;
    END IF;
    RETURN existing_generation;
END;
$$;
REVOKE ALL ON FUNCTION prepare_mutation_generation(regclass) FROM PUBLIC;

DO $mutation_guard_function$
DECLARE
    guard_schema text := current_schema();
BEGIN
    EXECUTE format('CREATE FUNCTION %I.begin_mutation_guard(text,bigint,bigint,text) RETURNS void
        LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
        DECLARE guard_state text; guard_owner text; guard_epoch bigint; guard_generation bigint; guard_resource text;
        BEGIN
            EXECUTE format(''SELECT state, owner, epoch, pg_generation, resource_identity FROM %%I.mutation_guard WHERE singleton FOR SHARE'', %L)
                INTO guard_state, guard_owner, guard_epoch, guard_generation, guard_resource;
            IF guard_state IS DISTINCT FROM ''active'' OR guard_owner IS DISTINCT FROM $1 OR guard_epoch IS DISTINCT FROM $2
                OR guard_generation IS DISTINCT FROM $3 OR guard_resource IS DISTINCT FROM $4 THEN
                RAISE EXCEPTION ''distributed mutation lacks current owner token'' USING ERRCODE = ''42501'';
            END IF;
            PERFORM set_config(''app.mutation_owner'', $1, true);
            PERFORM set_config(''app.mutation_epoch'', $2::text, true);
            PERFORM set_config(''app.pg_generation'', $3::text, true);
            PERFORM set_config(''app.resource_identity'', $4, true);
        END;
        $fn$', guard_schema, guard_schema);
    EXECUTE format('REVOKE ALL ON FUNCTION %I.begin_mutation_guard(text,bigint,bigint,text) FROM PUBLIC', guard_schema);
END;
$mutation_guard_function$;

CREATE TRIGGER hazard_events_mutation_fence BEFORE INSERT OR UPDATE OR DELETE ON hazard_events
    FOR EACH ROW EXECUTE FUNCTION require_mutation_authority();
CREATE TRIGGER source_seismic_mutation_fence BEFORE INSERT OR UPDATE OR DELETE ON source_seismic
    FOR EACH ROW EXECUTE FUNCTION require_mutation_authority();
CREATE TRIGGER source_warnings_mutation_fence BEFORE INSERT OR UPDATE OR DELETE ON source_warnings
    FOR EACH ROW EXECUTE FUNCTION require_mutation_authority();
CREATE TRIGGER poll_state_mutation_fence BEFORE INSERT OR UPDATE OR DELETE ON poll_state
    FOR EACH ROW EXECUTE FUNCTION require_mutation_authority();
CREATE TRIGGER outbox_mutation_fence BEFORE INSERT OR UPDATE OR DELETE ON outbox
    FOR EACH ROW EXECUTE FUNCTION require_mutation_authority();
CREATE TRIGGER handled_inputs_mutation_fence BEFORE INSERT OR UPDATE OR DELETE ON handled_inputs
    FOR EACH ROW EXECUTE FUNCTION require_mutation_authority();
CREATE TRIGGER hazard_events_truncate_fence BEFORE TRUNCATE ON hazard_events
    FOR EACH STATEMENT EXECUTE FUNCTION require_mutation_authority();
CREATE TRIGGER source_seismic_truncate_fence BEFORE TRUNCATE ON source_seismic
    FOR EACH STATEMENT EXECUTE FUNCTION require_mutation_authority();
CREATE TRIGGER source_warnings_truncate_fence BEFORE TRUNCATE ON source_warnings
    FOR EACH STATEMENT EXECUTE FUNCTION require_mutation_authority();
CREATE TRIGGER poll_state_truncate_fence BEFORE TRUNCATE ON poll_state
    FOR EACH STATEMENT EXECUTE FUNCTION require_mutation_authority();
CREATE TRIGGER outbox_truncate_fence BEFORE TRUNCATE ON outbox
    FOR EACH STATEMENT EXECUTE FUNCTION require_mutation_authority();
CREATE TRIGGER handled_inputs_truncate_fence BEFORE TRUNCATE ON handled_inputs
    FOR EACH STATEMENT EXECUTE FUNCTION require_mutation_authority();

INSERT INTO schema_migrations (version) VALUES (10);
