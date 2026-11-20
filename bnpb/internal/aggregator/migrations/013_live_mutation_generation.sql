CREATE FUNCTION live_mutation_resource(guard_table regclass) RETURNS text
    LANGUAGE sql SET search_path = pg_catalog AS $$
    -- The insertion timeline changes at promotion, before the next checkpoint.
    SELECT current_database() || ':' || guard_table::text || ':system:' || control.system_identifier::text
        || ':timeline:' || wal.timeline_id::text
    FROM pg_control_system() AS control
    CROSS JOIN pg_split_walfile_name(pg_walfile_name(pg_current_wal_insert_lsn())) AS wal;
$$;
REVOKE ALL ON FUNCTION live_mutation_resource(regclass) FROM PUBLIC;

DO $mutation_generation$
DECLARE
    guard_schema text := current_schema();
BEGIN
    EXECUTE format($ddl$
        CREATE OR REPLACE FUNCTION %1$I.prepare_mutation_generation(guard_table regclass) RETURNS bigint
            LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $fn$
        DECLARE
            current_resource text;
            existing_state text;
            existing_owner text;
            existing_generation bigint;
            existing_resource text;
        BEGIN
            IF pg_is_in_recovery() THEN
                RAISE EXCEPTION 'cannot prepare a mutation generation on a PostgreSQL standby' USING ERRCODE = '42501';
            END IF;
            EXECUTE format('SELECT state, owner, pg_generation, resource_identity FROM %%s WHERE singleton FOR UPDATE', guard_table)
                INTO existing_state, existing_owner, existing_generation, existing_resource;
            IF existing_state IS DISTINCT FROM 'closed' OR existing_owner IS DISTINCT FROM '' THEN
                RAISE EXCEPTION 'mutation guard must be closed before PostgreSQL generation reconciliation' USING ERRCODE = '42501';
            END IF;
            current_resource := %1$I.live_mutation_resource(guard_table);
            IF existing_resource IS DISTINCT FROM current_resource THEN
                IF existing_generation = 9223372036854775807 THEN
                    RAISE EXCEPTION 'PostgreSQL generation exhausted' USING ERRCODE = '22003';
                END IF;
                existing_generation := GREATEST(existing_generation + 1, 1);
                EXECUTE format('UPDATE %%s SET pg_generation = $1, resource_identity = $2 WHERE singleton', guard_table)
                    USING existing_generation, current_resource;
            END IF;
            RETURN existing_generation;
        END;
        $fn$
    $ddl$, guard_schema);

    EXECUTE format($ddl$
        CREATE OR REPLACE FUNCTION %1$I.begin_mutation_guard(text, bigint, bigint, text) RETURNS void
            LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $fn$
        DECLARE
            guard_state text;
            guard_owner text;
            guard_epoch bigint;
            guard_generation bigint;
            guard_resource text;
        BEGIN
            -- A standby cannot take the row lock or inspect its WAL insertion position.
            IF pg_is_in_recovery() THEN
                RAISE EXCEPTION 'cannot bind a mutation guard on a PostgreSQL standby' USING ERRCODE = '42501';
            END IF;
            SELECT state, owner, epoch, pg_generation, resource_identity FROM %1$I.mutation_guard WHERE singleton FOR SHARE
                INTO guard_state, guard_owner, guard_epoch, guard_generation, guard_resource;
            IF guard_state IS DISTINCT FROM 'active' OR guard_owner IS DISTINCT FROM $1 OR guard_epoch IS DISTINCT FROM $2
                OR guard_generation IS DISTINCT FROM $3 OR guard_resource IS DISTINCT FROM $4 THEN
                RAISE EXCEPTION 'distributed mutation lacks current owner token' USING ERRCODE = '42501';
            END IF;
            IF guard_resource IS DISTINCT FROM %1$I.live_mutation_resource(%2$L::regclass) THEN
                RAISE EXCEPTION 'distributed mutation token targets a stale PostgreSQL resource' USING ERRCODE = '42501';
            END IF;
            PERFORM set_config('app.mutation_owner', $1, true);
            PERFORM set_config('app.mutation_epoch', $2::text, true);
            PERFORM set_config('app.pg_generation', $3::text, true);
            PERFORM set_config('app.resource_identity', $4, true);
        END;
        $fn$
    $ddl$, guard_schema, format('%I.mutation_guard', guard_schema));
END;
$mutation_generation$;

INSERT INTO schema_migrations (version) VALUES (13);
