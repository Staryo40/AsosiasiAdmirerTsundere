CREATE TABLE IF NOT EXISTS shard_repair_jobs (
    shard_id smallint PRIMARY KEY REFERENCES logical_shards (shard_id),
    unit text NOT NULL CHECK (unit <> ''),
    node text NOT NULL CHECK (node <> ''),
    placement_revision bigint NOT NULL CHECK (placement_revision >= 0),
    status text NOT NULL CHECK (status IN ('reserved', 'provisioning', 'copying', 'verified', 'blocked')),
    candidate jsonb NOT NULL CHECK (jsonb_typeof(candidate) = 'object'),
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (unit)
);
REVOKE ALL ON shard_repair_jobs FROM PUBLIC;

ALTER TABLE placement_revision ADD COLUMN IF NOT EXISTS bootstrap_config jsonb;
