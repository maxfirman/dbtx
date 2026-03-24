ALTER TABLE environments
ADD COLUMN IF NOT EXISTS worker_queue TEXT;

UPDATE environments
SET worker_queue = 'generic'
WHERE worker_queue IS NULL OR btrim(worker_queue) = '';

ALTER TABLE environments
ALTER COLUMN worker_queue SET NOT NULL;

ALTER TABLE environments
DROP CONSTRAINT IF EXISTS chk_environments_worker_queue;

ALTER TABLE environments
ADD CONSTRAINT chk_environments_worker_queue
CHECK (btrim(worker_queue) <> '');
