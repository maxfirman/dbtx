ALTER TABLE invocations
ADD COLUMN IF NOT EXISTS next_event_sequence BIGINT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS invocation_events (
    id BIGSERIAL PRIMARY KEY,
    invocation_id UUID NOT NULL REFERENCES invocations(invocation_id) ON DELETE CASCADE,
    sequence_no BIGINT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    UNIQUE (invocation_id, sequence_no)
);

CREATE INDEX IF NOT EXISTS idx_invocation_events_invocation_sequence
    ON invocation_events (invocation_id, sequence_no);
