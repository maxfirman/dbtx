ALTER TABLE invocations
ADD COLUMN IF NOT EXISTS claim_deadline_at TIMESTAMPTZ NULL;

CREATE INDEX IF NOT EXISTS idx_invocations_claim_deadline
    ON invocations (claim_deadline_at)
    WHERE status = 'running' AND claimed_by IS NULL;
