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

-- A storage authority records independent evidence of irreversible loss/decommission.
-- A failed probe or a Kubernetes node becoming NotReady is not such evidence.
CREATE TABLE IF NOT EXISTS shard_copy_loss_attestations (
    unit text PRIMARY KEY REFERENCES shard_placements (unit),
    shard_id smallint NOT NULL REFERENCES logical_shards (shard_id),
    node text NOT NULL CHECK (node <> ''),
    evidence_sha256 text NOT NULL CHECK (evidence_sha256 ~ '^[0-9a-f]{64}$'),
    attested_at timestamptz NOT NULL DEFAULT now()
);
REVOKE ALL ON shard_copy_loss_attestations FROM PUBLIC;

-- Retain the placement row and evidence even if a newer repair replaces the job.
ALTER TABLE shard_placements ADD COLUMN IF NOT EXISTS retirement_evidence_sha256 text
    CHECK (retirement_evidence_sha256 IS NULL OR
           (state = 'retiring' AND retirement_evidence_sha256 ~ '^[0-9a-f]{64}$'));
