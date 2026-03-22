use crate::config::{InvocationContext, RuntimeConfig};
use crate::error::{AppError, AppResult};
use crate::event::LogEvent;
use crate::manifest::{CurrentNodeState, ManifestEdge, ManifestSnapshot, ReconstructedManifest};
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Executor, PgPool, Row};
use std::ffi::OsString;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use uuid::Uuid;

const SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS projects (
    id BIGSERIAL PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS environments (
    id BIGSERIAL PRIMARY KEY,
    project_id BIGINT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    slug TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(project_id, slug)
);

CREATE TABLE IF NOT EXISTS runs (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL UNIQUE,
    project_id BIGINT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    environment_id BIGINT NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
    dbt_invocation_id UUID,
    command TEXT NOT NULL,
    args JSONB NOT NULL,
    dbt_version TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    exit_code INTEGER,
    terminal_status TEXT
);

CREATE TABLE IF NOT EXISTS run_events (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    sequence_no BIGINT NOT NULL,
    event_name TEXT,
    event_code TEXT,
    unique_id TEXT,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(run_id, sequence_no)
);

CREATE TABLE IF NOT EXISTS node_executions (
    run_id UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    unique_id TEXT NOT NULL,
    resource_type TEXT,
    node_name TEXT,
    node_path TEXT,
    materialized TEXT,
    status TEXT,
    relation_database TEXT,
    relation_schema TEXT,
    relation_alias TEXT,
    relation_name TEXT,
    checksum TEXT,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    execution_time_seconds DOUBLE PRECISION,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, unique_id)
);

CREATE TABLE IF NOT EXISTS manifest_snapshots (
    run_id UUID PRIMARY KEY REFERENCES runs(run_id) ON DELETE CASCADE,
    manifest JSONB NOT NULL,
    manifest_size_bytes BIGINT NOT NULL,
    checksum TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS manifest_nodes (
    run_id UUID NOT NULL REFERENCES manifest_snapshots(run_id) ON DELETE CASCADE,
    unique_id TEXT NOT NULL,
    resource_type TEXT,
    name TEXT,
    package_name TEXT,
    original_file_path TEXT,
    tags JSONB NOT NULL,
    fqn JSONB NOT NULL,
    config JSONB NOT NULL,
    checksum TEXT,
    database_name TEXT,
    schema_name TEXT,
    alias TEXT,
    relation_name TEXT,
    PRIMARY KEY (run_id, unique_id)
);

CREATE TABLE IF NOT EXISTS manifest_edges (
    run_id UUID NOT NULL REFERENCES manifest_snapshots(run_id) ON DELETE CASCADE,
    parent_unique_id TEXT NOT NULL,
    child_unique_id TEXT NOT NULL,
    PRIMARY KEY (run_id, parent_unique_id, child_unique_id)
);

CREATE TABLE IF NOT EXISTS current_node_state (
    project_id BIGINT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    environment_id BIGINT NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
    unique_id TEXT NOT NULL,
    last_run_id UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    status TEXT,
    resource_type TEXT,
    node_name TEXT,
    node_path TEXT,
    materialized TEXT,
    relation_database TEXT,
    relation_schema TEXT,
    relation_alias TEXT,
    relation_name TEXT,
    checksum TEXT,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    execution_time_seconds DOUBLE PRECISION,
    last_success_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (project_id, environment_id, unique_id)
);

CREATE INDEX IF NOT EXISTS idx_runs_project_env ON runs(project_id, environment_id, id DESC);
CREATE INDEX IF NOT EXISTS idx_run_events_run ON run_events(run_id, sequence_no);
CREATE INDEX IF NOT EXISTS idx_node_executions_run ON node_executions(run_id);
"#;

pub struct Db {
    pool: PgPool,
}

impl Db {
    pub async fn connect(database_url: &str) -> AppResult<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await?;
        Ok(Self { pool })
    }

    pub async fn init(&self) -> AppResult<()> {
        self.pool.execute(SCHEMA_SQL).await?;
        Ok(())
    }

    pub async fn run_invocation(
        &self,
        config: &RuntimeConfig,
        incoming_args: &[OsString],
    ) -> AppResult<()> {
        let ctx = InvocationContext::from_args(incoming_args, true)?;
        let run_id = Uuid::new_v4();
        let reconstructed_manifest = self
            .load_reconstructed_manifest(&ctx.project_slug, &ctx.environment_slug)
            .await?;
        let dbt_args = append_invocation_id(
            append_state_dir(ctx.dbt_args.clone(), reconstructed_manifest.as_ref()),
            run_id,
        );
        let args_json = Value::Array(
            dbt_args
                .iter()
                .map(|value| Value::String(value.to_string_lossy().into_owned()))
                .collect(),
        );
        let (project_id, environment_id) = self
            .ensure_environment(&ctx.project_slug, &ctx.environment_slug)
            .await?;

        sqlx::query(
            r#"
            INSERT INTO runs (run_id, project_id, environment_id, dbt_invocation_id, command, args)
            VALUES ($1, $2, $3, $1, 'run', $4)
            "#,
        )
        .bind(run_id)
        .bind(project_id)
        .bind(environment_id)
        .bind(args_json)
        .execute(&self.pool)
        .await?;

        let mut child = spawn_dbt_child(&config.dbt_path, "run", &dbt_args, &ctx.project_dir)?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| AppError::Io(std::io::Error::other("missing child stdout")))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| AppError::Io(std::io::Error::other("missing child stderr")))?;

        let stderr_handle = tokio::spawn(async move {
            let mut reader = BufReader::new(stderr).lines();
            while let Some(line) = reader.next_line().await? {
                eprintln!("{line}");
            }
            Result::<(), std::io::Error>::Ok(())
        });

        let mut reader = BufReader::new(stdout).lines();
        let mut sequence_no: i64 = 0;
        let mut dbt_version: Option<String> = None;
        while let Some(line) = reader.next_line().await? {
            println!("{line}");
            sequence_no += 1;
            if let Some(event) = LogEvent::parse(&line) {
                if dbt_version.is_none() && event.info.name == "MainReportVersion" {
                    dbt_version = event
                        .data
                        .get("version")
                        .and_then(Value::as_str)
                        .map(ToString::to_string);
                }
                self.persist_log_event(run_id, project_id, environment_id, sequence_no, &event)
                    .await?;
            } else {
                self.persist_raw_line(run_id, sequence_no, &line).await?;
            }
        }

        let status = child.wait().await?;
        stderr_handle.await.map_err(|err| {
            AppError::Io(std::io::Error::other(format!(
                "stderr task failed: {err}"
            )))
        })??;

        let manifest_path = ctx.target_path.join("manifest.json");
        let manifest_result = ManifestSnapshot::from_path(&manifest_path).await;
        let terminal_status = if status.success() {
            "success"
        } else {
            "failed"
        };
        let exit_code = status.code().unwrap_or(1);

        sqlx::query(
            r#"
            UPDATE runs
            SET dbt_version = COALESCE($2, dbt_version),
                finished_at = NOW(),
                exit_code = $3,
                terminal_status = $4
            WHERE run_id = $1
            "#,
        )
        .bind(run_id)
        .bind(dbt_version)
        .bind(exit_code)
        .bind(terminal_status)
        .execute(&self.pool)
        .await?;

        if let Ok(manifest) = manifest_result {
            self.persist_manifest(run_id, &manifest).await?;
        }

        self.rebuild_current_state(project_id, environment_id).await?;

        if status.success() {
            Ok(())
        } else {
            Err(AppError::DbtFailed(exit_code))
        }
    }

    pub async fn ls_invocation(
        &self,
        config: &RuntimeConfig,
        incoming_args: &[OsString],
    ) -> AppResult<()> {
        let ctx = InvocationContext::from_args(incoming_args, false)?;
        let reconstructed_manifest = self
            .load_reconstructed_manifest(&ctx.project_slug, &ctx.environment_slug)
            .await?;
        let dbt_args = append_state_dir(ctx.dbt_args.clone(), reconstructed_manifest.as_ref());
        let status = self
            .execute_passthrough_command(&config.dbt_path, "ls", &dbt_args, &ctx.project_dir)
            .await?;

        if status.success() {
            Ok(())
        } else {
            Err(AppError::DbtFailed(status.code().unwrap_or(1)))
        }
    }

    pub async fn replay_projection(&self, run_id: Uuid) -> AppResult<u64> {
        let row = sqlx::query(
            r#"
            SELECT id, project_id, environment_id
            FROM runs
            WHERE run_id = $1
            "#,
        )
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(AppError::RunNotFound(run_id))?;

        let run_pk: i64 = row.get("id");
        let project_id: i64 = row.get("project_id");
        let environment_id: i64 = row.get("environment_id");

        sqlx::query(
            r#"
            DELETE FROM current_node_state
            WHERE project_id = $1 AND environment_id = $2
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .execute(&self.pool)
        .await?;

        let inserted = sqlx::query(
            r#"
            INSERT INTO current_node_state (
                project_id, environment_id, unique_id, last_run_id, status, resource_type,
                node_name, node_path, materialized, relation_database, relation_schema,
                relation_alias, relation_name, checksum, started_at, finished_at,
                execution_time_seconds, last_success_at, updated_at
            )
            SELECT DISTINCT ON (ne.unique_id)
                r.project_id,
                r.environment_id,
                ne.unique_id,
                ne.run_id,
                ne.status,
                ne.resource_type,
                ne.node_name,
                ne.node_path,
                ne.materialized,
                ne.relation_database,
                ne.relation_schema,
                ne.relation_alias,
                ne.relation_name,
                ne.checksum,
                ne.started_at,
                ne.finished_at,
                ne.execution_time_seconds,
                CASE WHEN ne.status = 'success' THEN ne.finished_at END,
                NOW()
            FROM node_executions ne
            JOIN runs r ON r.run_id = ne.run_id
            WHERE r.project_id = $1
              AND r.environment_id = $2
              AND r.id <= $3
            ORDER BY ne.unique_id, r.id DESC
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .bind(run_pk)
        .execute(&self.pool)
        .await?;

        Ok(inserted.rows_affected())
    }

    async fn ensure_environment(&self, project_slug: &str, environment_slug: &str) -> AppResult<(i64, i64)> {
        let project_id: i64 = sqlx::query_scalar(
            r#"
            INSERT INTO projects (slug)
            VALUES ($1)
            ON CONFLICT (slug) DO UPDATE SET slug = EXCLUDED.slug
            RETURNING id
            "#,
        )
        .bind(project_slug)
        .fetch_one(&self.pool)
        .await?;

        let environment_id: i64 = sqlx::query_scalar(
            r#"
            INSERT INTO environments (project_id, slug)
            VALUES ($1, $2)
            ON CONFLICT (project_id, slug) DO UPDATE SET slug = EXCLUDED.slug
            RETURNING id
            "#,
        )
        .bind(project_id)
        .bind(environment_slug)
        .fetch_one(&self.pool)
        .await?;

        Ok((project_id, environment_id))
    }

    async fn persist_log_event(
        &self,
        run_id: Uuid,
        project_id: i64,
        environment_id: i64,
        sequence_no: i64,
        event: &LogEvent,
    ) -> AppResult<()> {
        let unique_id = event
            .normalized_node_event()
            .as_ref()
            .map(|node| node.unique_id.clone());

        sqlx::query(
            r#"
            INSERT INTO run_events (run_id, sequence_no, event_name, event_code, unique_id, payload)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(run_id)
        .bind(sequence_no)
        .bind(null_if_empty(&event.info.name))
        .bind(null_if_empty(&event.info.code))
        .bind(unique_id.clone())
        .bind(sqlx::types::Json(&event))
        .execute(&self.pool)
        .await?;

        if let Some(node) = event.normalized_node_event() {
            let resource_type = node.resource_type.clone();
            let node_name = node.node_name.clone();
            let node_path = node.node_path.clone();
            let materialized = node.materialized.clone();
            let status = node.status.clone();
            let relation_database = node.relation_database.clone();
            let relation_schema = node.relation_schema.clone();
            let relation_alias = node.relation_alias.clone();
            let relation_name = node.relation_name.clone();
            let node_checksum = node.node_checksum.clone();
            let started_at = node.started_at;
            let finished_at = node.finished_at;
            let execution_time_seconds = node.execution_time_seconds;

            sqlx::query(
                r#"
                INSERT INTO node_executions (
                    run_id, unique_id, resource_type, node_name, node_path, materialized, status,
                    relation_database, relation_schema, relation_alias, relation_name, checksum,
                    started_at, finished_at, execution_time_seconds, updated_at
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7,
                    $8, $9, $10, $11, $12,
                    $13, $14, $15, NOW()
                )
                ON CONFLICT (run_id, unique_id) DO UPDATE SET
                    resource_type = COALESCE(EXCLUDED.resource_type, node_executions.resource_type),
                    node_name = COALESCE(EXCLUDED.node_name, node_executions.node_name),
                    node_path = COALESCE(EXCLUDED.node_path, node_executions.node_path),
                    materialized = COALESCE(EXCLUDED.materialized, node_executions.materialized),
                    status = COALESCE(EXCLUDED.status, node_executions.status),
                    relation_database = COALESCE(EXCLUDED.relation_database, node_executions.relation_database),
                    relation_schema = COALESCE(EXCLUDED.relation_schema, node_executions.relation_schema),
                    relation_alias = COALESCE(EXCLUDED.relation_alias, node_executions.relation_alias),
                    relation_name = COALESCE(EXCLUDED.relation_name, node_executions.relation_name),
                    checksum = COALESCE(EXCLUDED.checksum, node_executions.checksum),
                    started_at = COALESCE(EXCLUDED.started_at, node_executions.started_at),
                    finished_at = COALESCE(EXCLUDED.finished_at, node_executions.finished_at),
                    execution_time_seconds = COALESCE(EXCLUDED.execution_time_seconds, node_executions.execution_time_seconds),
                    updated_at = NOW()
                "#,
            )
            .bind(run_id)
            .bind(&node.unique_id)
            .bind(resource_type.clone())
            .bind(node_name.clone())
            .bind(node_path.clone())
            .bind(materialized.clone())
            .bind(status.clone())
            .bind(relation_database.clone())
            .bind(relation_schema.clone())
            .bind(relation_alias.clone())
            .bind(relation_name.clone())
            .bind(node_checksum.clone())
            .bind(started_at)
            .bind(finished_at)
            .bind(execution_time_seconds)
            .execute(&self.pool)
            .await?;

            sqlx::query(
                r#"
                INSERT INTO current_node_state (
                    project_id, environment_id, unique_id, last_run_id, status, resource_type,
                    node_name, node_path, materialized, relation_database, relation_schema,
                    relation_alias, relation_name, checksum, started_at, finished_at,
                    execution_time_seconds, last_success_at, updated_at
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6,
                    $7, $8, $9, $10, $11,
                    $12, $13, $14, $15, $16,
                    $17, CASE WHEN $5 = 'success' THEN $16 END, NOW()
                )
                ON CONFLICT (project_id, environment_id, unique_id) DO UPDATE SET
                    last_run_id = EXCLUDED.last_run_id,
                    status = COALESCE(EXCLUDED.status, current_node_state.status),
                    resource_type = COALESCE(EXCLUDED.resource_type, current_node_state.resource_type),
                    node_name = COALESCE(EXCLUDED.node_name, current_node_state.node_name),
                    node_path = COALESCE(EXCLUDED.node_path, current_node_state.node_path),
                    materialized = COALESCE(EXCLUDED.materialized, current_node_state.materialized),
                    relation_database = COALESCE(EXCLUDED.relation_database, current_node_state.relation_database),
                    relation_schema = COALESCE(EXCLUDED.relation_schema, current_node_state.relation_schema),
                    relation_alias = COALESCE(EXCLUDED.relation_alias, current_node_state.relation_alias),
                    relation_name = COALESCE(EXCLUDED.relation_name, current_node_state.relation_name),
                    checksum = COALESCE(EXCLUDED.checksum, current_node_state.checksum),
                    started_at = COALESCE(EXCLUDED.started_at, current_node_state.started_at),
                    finished_at = COALESCE(EXCLUDED.finished_at, current_node_state.finished_at),
                    execution_time_seconds = COALESCE(EXCLUDED.execution_time_seconds, current_node_state.execution_time_seconds),
                    last_success_at = COALESCE(EXCLUDED.last_success_at, current_node_state.last_success_at),
                    updated_at = NOW()
                "#,
            )
            .bind(project_id)
            .bind(environment_id)
            .bind(&node.unique_id)
            .bind(run_id)
            .bind(status)
            .bind(resource_type)
            .bind(node_name)
            .bind(node_path)
            .bind(materialized)
            .bind(relation_database)
            .bind(relation_schema)
            .bind(relation_alias)
            .bind(relation_name)
            .bind(node_checksum)
            .bind(started_at)
            .bind(finished_at)
            .bind(execution_time_seconds)
            .execute(&self.pool)
            .await?;
        }

        Ok(())
    }

    async fn persist_raw_line(&self, run_id: Uuid, sequence_no: i64, line: &str) -> AppResult<()> {
        sqlx::query(
            r#"
            INSERT INTO run_events (run_id, sequence_no, event_name, event_code, unique_id, payload)
            VALUES ($1, $2, 'RawLine', NULL, NULL, $3)
            "#,
        )
        .bind(run_id)
        .bind(sequence_no)
        .bind(sqlx::types::Json(serde_json::json!({ "raw_line": line })))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn persist_manifest(&self, run_id: Uuid, manifest: &ManifestSnapshot) -> AppResult<()> {
        let manifest_raw = serde_json::to_vec(&manifest.raw)?;
        let checksum = format!("{:x}", md5::compute(&manifest_raw));
        sqlx::query(
            r#"
            INSERT INTO manifest_snapshots (run_id, manifest, manifest_size_bytes, checksum)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (run_id) DO UPDATE SET
                manifest = EXCLUDED.manifest,
                manifest_size_bytes = EXCLUDED.manifest_size_bytes,
                checksum = EXCLUDED.checksum
            "#,
        )
        .bind(run_id)
        .bind(sqlx::types::Json(&manifest.raw))
        .bind(manifest_raw.len() as i64)
        .bind(checksum)
        .execute(&self.pool)
        .await?;

        sqlx::query("DELETE FROM manifest_nodes WHERE run_id = $1")
            .bind(run_id)
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM manifest_edges WHERE run_id = $1")
            .bind(run_id)
            .execute(&self.pool)
            .await?;

        for node in &manifest.nodes {
            sqlx::query(
                r#"
                INSERT INTO manifest_nodes (
                    run_id, unique_id, resource_type, name, package_name, original_file_path,
                    tags, fqn, config, checksum, database_name, schema_name, alias, relation_name
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6,
                    $7, $8, $9, $10, $11, $12, $13, $14
                )
                "#,
            )
            .bind(run_id)
            .bind(&node.unique_id)
            .bind(&node.resource_type)
            .bind(&node.name)
            .bind(&node.package_name)
            .bind(&node.original_file_path)
            .bind(sqlx::types::Json(&node.tags))
            .bind(sqlx::types::Json(&node.fqn))
            .bind(sqlx::types::Json(&node.config))
            .bind(&node.checksum)
            .bind(&node.database_name)
            .bind(&node.schema_name)
            .bind(&node.alias)
            .bind(&node.relation_name)
            .execute(&self.pool)
            .await?;
        }

        for edge in &manifest.edges {
            sqlx::query(
                r#"
                INSERT INTO manifest_edges (run_id, parent_unique_id, child_unique_id)
                VALUES ($1, $2, $3)
                "#,
            )
            .bind(run_id)
            .bind(&edge.parent_unique_id)
            .bind(&edge.child_unique_id)
            .execute(&self.pool)
            .await?;
        }

        Ok(())
    }

    async fn rebuild_current_state(&self, project_id: i64, environment_id: i64) -> AppResult<()> {
        sqlx::query(
            r#"
            DELETE FROM current_node_state
            WHERE project_id = $1 AND environment_id = $2
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO current_node_state (
                project_id, environment_id, unique_id, last_run_id, status, resource_type,
                node_name, node_path, materialized, relation_database, relation_schema,
                relation_alias, relation_name, checksum, started_at, finished_at,
                execution_time_seconds, last_success_at, updated_at
            )
            SELECT DISTINCT ON (ne.unique_id)
                r.project_id,
                r.environment_id,
                ne.unique_id,
                ne.run_id,
                ne.status,
                ne.resource_type,
                ne.node_name,
                ne.node_path,
                ne.materialized,
                ne.relation_database,
                ne.relation_schema,
                ne.relation_alias,
                ne.relation_name,
                ne.checksum,
                ne.started_at,
                ne.finished_at,
                ne.execution_time_seconds,
                CASE WHEN ne.status = 'success' THEN ne.finished_at END,
                NOW()
            FROM node_executions ne
            JOIN runs r ON r.run_id = ne.run_id
            WHERE r.project_id = $1
              AND r.environment_id = $2
            ORDER BY ne.unique_id, r.id DESC
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn load_reconstructed_manifest(
        &self,
        project_slug: &str,
        environment_slug: &str,
    ) -> AppResult<Option<ReconstructedManifest>> {
        let snapshot_row = sqlx::query(
            r#"
            SELECT
                r.run_id,
                r.project_id,
                r.environment_id,
                ms.manifest
            FROM runs r
            JOIN projects p ON p.id = r.project_id
            JOIN environments e ON e.id = r.environment_id
            JOIN manifest_snapshots ms ON ms.run_id = r.run_id
            WHERE p.slug = $1
              AND e.slug = $2
            ORDER BY r.id DESC
            LIMIT 1
            "#,
        )
        .bind(project_slug)
        .bind(environment_slug)
        .fetch_optional(&self.pool)
        .await?;

        let Some(snapshot_row) = snapshot_row else {
            return Ok(None);
        };

        let snapshot_run_id: Uuid = snapshot_row.get("run_id");
        let project_id: i64 = snapshot_row.get("project_id");
        let environment_id: i64 = snapshot_row.get("environment_id");
        let manifest_json: sqlx::types::Json<Value> = snapshot_row.get("manifest");

        let current_nodes = sqlx::query(
            r#"
            SELECT
                unique_id,
                materialized,
                relation_database,
                relation_schema,
                relation_alias,
                relation_name,
                checksum
            FROM current_node_state
            WHERE project_id = $1
              AND environment_id = $2
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| CurrentNodeState {
            unique_id: row.get("unique_id"),
            materialized: row.get("materialized"),
            relation_database: row.get("relation_database"),
            relation_schema: row.get("relation_schema"),
            relation_alias: row.get("relation_alias"),
            relation_name: row.get("relation_name"),
            checksum: row.get("checksum"),
        })
        .collect::<Vec<_>>();

        let edges = sqlx::query(
            r#"
            SELECT parent_unique_id, child_unique_id
            FROM manifest_edges
            WHERE run_id = $1
            "#,
        )
        .bind(snapshot_run_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| ManifestEdge {
            parent_unique_id: row.get("parent_unique_id"),
            child_unique_id: row.get("child_unique_id"),
        })
        .collect::<Vec<_>>();

        let reconstructed = ManifestSnapshot::reconstruct(manifest_json.0, &current_nodes, &edges);
        Ok(Some(ReconstructedManifest::write(&reconstructed).await?))
    }

    async fn execute_passthrough_command(
        &self,
        dbt_path: &str,
        subcommand: &str,
        args: &[OsString],
        project_dir: &std::path::Path,
    ) -> AppResult<std::process::ExitStatus> {
        let mut child = spawn_dbt_child(dbt_path, subcommand, args, project_dir)?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| AppError::Io(std::io::Error::other("missing child stdout")))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| AppError::Io(std::io::Error::other("missing child stderr")))?;

        let stdout_handle = tokio::spawn(async move {
            let mut reader = BufReader::new(stdout).lines();
            while let Some(line) = reader.next_line().await? {
                println!("{line}");
            }
            Result::<(), std::io::Error>::Ok(())
        });

        let stderr_handle = tokio::spawn(async move {
            let mut reader = BufReader::new(stderr).lines();
            while let Some(line) = reader.next_line().await? {
                eprintln!("{line}");
            }
            Result::<(), std::io::Error>::Ok(())
        });

        let status = child.wait().await?;
        stdout_handle.await.map_err(|err| {
            AppError::Io(std::io::Error::other(format!(
                "stdout task failed: {err}"
            )))
        })??;
        stderr_handle.await.map_err(|err| {
            AppError::Io(std::io::Error::other(format!(
                "stderr task failed: {err}"
            )))
        })??;

        Ok(status)
    }
}

fn append_invocation_id(mut args: Vec<OsString>, run_id: Uuid) -> Vec<OsString> {
    args.push("--invocation-id".into());
    args.push(run_id.to_string().into());
    args
}

fn append_state_dir(
    mut args: Vec<OsString>,
    reconstructed_manifest: Option<&ReconstructedManifest>,
) -> Vec<OsString> {
    if let Some(reconstructed_manifest) = reconstructed_manifest {
        args.push("--state".into());
        args.push(reconstructed_manifest.temp_dir.path().as_os_str().to_os_string());
    }
    args
}

fn spawn_dbt_child(
    dbt_path: &str,
    subcommand: &str,
    args: &[OsString],
    project_dir: &std::path::Path,
) -> AppResult<Child> {
    let child = Command::new(dbt_path)
        .arg(subcommand)
        .args(args)
        .current_dir(project_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    Ok(child)
}

fn null_if_empty(value: &str) -> Option<&str> {
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}
