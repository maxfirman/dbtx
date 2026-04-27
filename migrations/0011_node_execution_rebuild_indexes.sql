-- Composite index to support the rebuild_current_state_up_to_in_tx query's
-- DISTINCT ON (ne.unique_id) ... ORDER BY ne.unique_id, r.id DESC pattern.
-- The rebuild joins node_executions to runs and filters by (project_id, environment_id),
-- then needs the latest execution per unique_id. This index lets Postgres satisfy the
-- DISTINCT ON ordering directly after the join, avoiding a full sort.
--
-- For large existing tables, consider creating this index CONCURRENTLY before deploying
-- the migration so that IF NOT EXISTS makes this a no-op.
CREATE INDEX IF NOT EXISTS idx_node_executions_unique_id_run_id
    ON node_executions (unique_id, run_id DESC);
