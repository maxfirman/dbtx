CREATE TABLE IF NOT EXISTS worker_registrations (
    worker_id TEXT NOT NULL,
    execution_mode TEXT NOT NULL,
    worker_queue TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (worker_id, worker_queue),
    CONSTRAINT chk_worker_registrations_execution_mode CHECK (execution_mode IN ('local', 'server')),
    CONSTRAINT chk_worker_registrations_worker_queue CHECK (btrim(worker_queue) <> '')
);

INSERT INTO worker_registrations (worker_id, execution_mode, worker_queue, first_seen_at, last_seen_at)
SELECT worker_id, execution_mode, worker_queue, first_seen_at, last_seen_at
FROM workers
ON CONFLICT (worker_id, worker_queue) DO UPDATE
SET execution_mode = EXCLUDED.execution_mode,
    first_seen_at = LEAST(worker_registrations.first_seen_at, EXCLUDED.first_seen_at),
    last_seen_at = GREATEST(worker_registrations.last_seen_at, EXCLUDED.last_seen_at);

DROP TABLE IF EXISTS workers;

ALTER TABLE worker_registrations RENAME TO workers;
