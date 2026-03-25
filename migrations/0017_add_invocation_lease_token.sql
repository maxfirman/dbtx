ALTER TABLE invocations
ADD COLUMN lease_token UUID;

ALTER TABLE invocations
ADD CONSTRAINT invocations_claim_lease_consistency CHECK (
    (claimed_by IS NULL AND lease_token IS NULL)
    OR (claimed_by IS NOT NULL AND lease_token IS NOT NULL)
);
