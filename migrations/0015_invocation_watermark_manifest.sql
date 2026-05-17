-- Stable manifest graph used for real-time per-node watermark updates.

ALTER TABLE invocations
ADD COLUMN IF NOT EXISTS watermark_manifest_run_id UUID REFERENCES runs(run_id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_invocations_watermark_manifest_run_id
    ON invocations (watermark_manifest_run_id);
