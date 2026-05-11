-- Per-node source watermark tracking tables

CREATE TABLE IF NOT EXISTS node_source_watermarks (
    project_id            BIGINT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    environment_id        BIGINT NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
    unique_id             TEXT NOT NULL,
    source_key            TEXT NOT NULL,
    watermark_event_id    BIGINT NOT NULL REFERENCES source_state_events(id) ON DELETE CASCADE,
    watermark_observed_at TIMESTAMPTZ,
    run_id                UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (project_id, environment_id, unique_id, source_key)
);

CREATE INDEX IF NOT EXISTS idx_node_source_watermarks_source_key
    ON node_source_watermarks (project_id, environment_id, source_key);

CREATE TABLE IF NOT EXISTS node_source_watermark_candidates (
    run_id                UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    unique_id             TEXT NOT NULL,
    source_key            TEXT NOT NULL,
    watermark_event_id    BIGINT NOT NULL,
    watermark_observed_at TIMESTAMPTZ,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, unique_id, source_key)
);

CREATE TABLE IF NOT EXISTS node_source_watermark_log (
    id                  BIGSERIAL PRIMARY KEY,
    project_id          BIGINT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    environment_id      BIGINT NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
    unique_id           TEXT NOT NULL,
    source_key          TEXT NOT NULL,
    watermark_event_id  BIGINT NOT NULL,
    previous_event_id   BIGINT,
    run_id              UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    invocation_id       UUID,
    recorded_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_watermark_log_source_event
    ON node_source_watermark_log (project_id, environment_id, source_key, watermark_event_id);

CREATE TABLE IF NOT EXISTS node_ancestor_sources (
    run_id      UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    unique_id   TEXT NOT NULL,
    source_key  TEXT NOT NULL,
    PRIMARY KEY (run_id, unique_id, source_key)
);

CREATE INDEX IF NOT EXISTS idx_node_ancestor_sources_source
    ON node_ancestor_sources (run_id, source_key);
