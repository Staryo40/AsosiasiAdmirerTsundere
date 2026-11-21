-- Single-store mode only, applied idempotently: the newest poll lease term allowed to store poll results.
CREATE TABLE IF NOT EXISTS poll_fence (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    lease_uid text NOT NULL,
    generation bigint NOT NULL CHECK (generation >= 0),
    holder text NOT NULL CHECK (holder <> ''),
    claimed_at timestamptz NOT NULL DEFAULT now()
);
