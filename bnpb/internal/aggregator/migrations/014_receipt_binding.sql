CREATE TABLE receipt_bindings (
    endpoint text NOT NULL CHECK (endpoint <> ''),
    input_hash bytea NOT NULL CHECK (octet_length(input_hash) = 32),
    raw_digest bytea NOT NULL CHECK (octet_length(raw_digest) = 32),
    -- -1 identifies the control store's unroutable ledger, not a logical shard.
    destination smallint NOT NULL CHECK (destination BETWEEN -1 AND 2),
    PRIMARY KEY (endpoint, input_hash)
);
REVOKE ALL ON receipt_bindings FROM PUBLIC;

CREATE TRIGGER receipt_bindings_mutation_fence BEFORE INSERT ON receipt_bindings
    FOR EACH ROW EXECUTE FUNCTION require_mutation_authority();

CREATE FUNCTION prevent_receipt_binding_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'receipt binding is immutable' USING ERRCODE = '23514';
END;
$$;
CREATE TRIGGER receipt_bindings_immutable BEFORE UPDATE OR DELETE ON receipt_bindings
    FOR EACH ROW EXECUTE FUNCTION prevent_receipt_binding_mutation();
CREATE TRIGGER receipt_bindings_no_truncate BEFORE TRUNCATE ON receipt_bindings
    FOR EACH STATEMENT EXECUTE FUNCTION prevent_receipt_binding_mutation();

INSERT INTO schema_migrations (version) VALUES (14);
