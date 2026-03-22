use crate::config::{InvocationContext, RuntimeConfig};
use crate::error::{AppError, AppResult};
use crate::event::LogEvent;
use crate::manifest::{CurrentNodeState, ManifestSnapshot, ReconstructedManifest};
use serde_json::Value;
use sqlx::migrate::Migrator;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::Path;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use uuid::Uuid;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

pub struct Db {
    pool: PgPool,
}

struct RunFinalization<'a> {
    run_id: Uuid,
    project_id: i64,
    environment_id: i64,
    subcommand: &'a str,
    dbt_version: Option<&'a str>,
    exit_code: i32,
    terminal_status: &'a str,
    manifest: Option<&'a ManifestSnapshot>,
    promote_base_manifest: bool,
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
        MIGRATOR.run(&self.pool).await?;
        Ok(())
    }

    pub async fn persisting_invocation(
        &self,
        subcommand: &str,
        config: &RuntimeConfig,
        incoming_args: &[OsString],
    ) -> AppResult<()> {
        let ctx = InvocationContext::from_args(incoming_args, true)?;
        let run_id = Uuid::new_v4();
        let reconstructed_manifest = self
            .load_reconstructed_manifest(&ctx.project_slug, &ctx.environment_slug)
            .await?
            .or(if ctx.wants_state_modified {
                Some(
                    ReconstructedManifest::write_empty_state(
                        &read_dbt_project_name(&ctx.project_dir),
                        &read_adapter_type(&ctx.profiles_dir),
                    )
                    .await?,
                )
            } else {
                None
            });
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
            INSERT INTO runs (run_id, project_id, environment_id, dbt_invocation_id, command, args, is_full_graph_run)
            VALUES ($1, $2, $3, $1, $4, $5, $6)
            "#,
        )
        .bind(run_id)
        .bind(project_id)
        .bind(environment_id)
        .bind(subcommand)
        .bind(args_json)
        .bind(ctx.is_full_graph_run)
        .execute(&self.pool)
        .await?;

        let mut child = match spawn_dbt_child(&config.dbt_path, subcommand, &dbt_args, &ctx.project_dir)
        {
            Ok(child) => child,
            Err(err) => {
                self.mark_run_finished(run_id, None, 1, "wrapper_failed")
                    .await?;
                return Err(err);
            }
        };

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
            sequence_no += 1;
            if let Some(event) = LogEvent::parse(&line) {
                if let Some(rendered) = event.render_text_line() {
                    println!("{rendered}");
                }
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
                println!("{line}");
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

        self.finalize_run(RunFinalization {
            run_id,
            project_id,
            environment_id,
            subcommand,
            dbt_version: dbt_version.as_deref(),
            exit_code,
            terminal_status,
            manifest: manifest_result.ok().as_ref(),
            promote_base_manifest: ctx.is_full_graph_run && status.success(),
        })
        .await?;

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
            .await?
            .or(if ctx.wants_state_modified {
                Some(
                    ReconstructedManifest::write_empty_state(
                        &read_dbt_project_name(&ctx.project_dir),
                        &read_adapter_type(&ctx.profiles_dir),
                    )
                    .await?,
                )
            } else {
                None
            });
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

        self.rebuild_promoted_manifest_state_up_to(project_id, environment_id, Some(run_pk))
            .await?;
        self.rebuild_current_state_up_to(project_id, environment_id, Some(run_pk))
            .await
    }

    async fn finalize_run(&self, finalization: RunFinalization<'_>) -> AppResult<()> {
        let mut tx = self.pool.begin().await?;
        self.mark_run_finished_in_tx(
            &mut tx,
            finalization.run_id,
            finalization.dbt_version,
            finalization.exit_code,
            finalization.terminal_status,
        )
            .await?;

        if let Some(manifest) = finalization.manifest {
            self.persist_manifest_in_tx(&mut tx, finalization.run_id, manifest)
                .await?;
            if should_promote_manifest(finalization.subcommand) {
                self.promote_manifest_state_in_tx(
                    &mut tx,
                    finalization.run_id,
                    finalization.project_id,
                    finalization.environment_id,
                    finalization.promote_base_manifest,
                )
                .await?;
            }
        }

        self.rebuild_current_state_up_to_in_tx(
            &mut tx,
            finalization.project_id,
            finalization.environment_id,
            None,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn mark_run_finished(
        &self,
        run_id: Uuid,
        dbt_version: Option<&str>,
        exit_code: i32,
        terminal_status: &str,
    ) -> AppResult<()> {
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
        Ok(())
    }

    async fn mark_run_finished_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        run_id: Uuid,
        dbt_version: Option<&str>,
        exit_code: i32,
        terminal_status: &str,
    ) -> AppResult<()> {
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
        .execute(&mut **tx)
        .await?;
        Ok(())
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
            let promote_manifest_state = matches!(node.status.as_deref(), Some("success" | "pass"));
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
            let promoted_materialized = promote_manifest_state
                .then(|| materialized.clone())
                .flatten();
            let promoted_relation_database = promote_manifest_state
                .then(|| relation_database.clone())
                .flatten();
            let promoted_relation_schema = promote_manifest_state
                .then(|| relation_schema.clone())
                .flatten();
            let promoted_relation_alias = promote_manifest_state
                .then(|| relation_alias.clone())
                .flatten();
            let promoted_relation_name = promote_manifest_state
                .then(|| relation_name.clone())
                .flatten();
            let promoted_checksum = promote_manifest_state
                .then(|| node_checksum.clone())
                .flatten();
            let last_success_at = promote_manifest_state.then_some(finished_at).flatten();

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
                    $17, $18, NOW()
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
            .bind(promoted_materialized)
            .bind(promoted_relation_database)
            .bind(promoted_relation_schema)
            .bind(promoted_relation_alias)
            .bind(promoted_relation_name)
            .bind(promoted_checksum)
            .bind(started_at)
            .bind(finished_at)
            .bind(execution_time_seconds)
            .bind(last_success_at)
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

    async fn persist_manifest_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        run_id: Uuid,
        manifest: &ManifestSnapshot,
    ) -> AppResult<()> {
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
        .execute(&mut **tx)
        .await?;

        sqlx::query("DELETE FROM manifest_nodes WHERE run_id = $1")
            .bind(run_id)
            .execute(&mut **tx)
            .await?;
        sqlx::query("DELETE FROM manifest_edges WHERE run_id = $1")
            .bind(run_id)
            .execute(&mut **tx)
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
            .execute(&mut **tx)
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
            .execute(&mut **tx)
            .await?;
        }

        Ok(())
    }

    async fn promote_manifest_state(
        &self,
        run_id: Uuid,
        project_id: i64,
        environment_id: i64,
        promote_base_manifest: bool,
    ) -> AppResult<()> {
        let mut tx = self.pool.begin().await?;
        self.promote_manifest_state_in_tx(
            &mut tx,
            run_id,
            project_id,
            environment_id,
            promote_base_manifest,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn promote_manifest_state_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        run_id: Uuid,
        project_id: i64,
        environment_id: i64,
        promote_base_manifest: bool,
    ) -> AppResult<()> {
        if promote_base_manifest {
            sqlx::query(
                r#"
                INSERT INTO promoted_manifest_meta (project_id, environment_id, source_run_id, base_manifest)
                SELECT $2, $3, $1, manifest
                FROM manifest_snapshots
                WHERE run_id = $1
                ON CONFLICT (project_id, environment_id) DO UPDATE SET
                    source_run_id = EXCLUDED.source_run_id,
                    base_manifest = EXCLUDED.base_manifest,
                    promoted_at = NOW()
                "#,
            )
            .bind(run_id)
            .bind(project_id)
            .bind(environment_id)
            .execute(&mut **tx)
            .await?;
        }

        sqlx::query(
            r#"
            INSERT INTO promoted_manifest_nodes (
                project_id, environment_id, unique_id, source_run_id, checksum, raw_node
            )
            SELECT
                $2,
                $3,
                ne.unique_id,
                ne.run_id,
                ne.checksum,
                ms.manifest -> 'nodes' -> ne.unique_id
            FROM node_executions ne
            JOIN manifest_snapshots ms ON ms.run_id = ne.run_id
            WHERE ne.run_id = $1
              AND ne.status = 'success'
              AND ms.manifest -> 'nodes' -> ne.unique_id IS NOT NULL
            ON CONFLICT (project_id, environment_id, unique_id) DO UPDATE SET
                source_run_id = EXCLUDED.source_run_id,
                checksum = EXCLUDED.checksum,
                raw_node = EXCLUDED.raw_node,
                promoted_at = NOW()
            "#,
        )
        .bind(run_id)
        .bind(project_id)
        .bind(environment_id)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    async fn rebuild_promoted_manifest_state_up_to(
        &self,
        project_id: i64,
        environment_id: i64,
        max_run_pk: Option<i64>,
    ) -> AppResult<()> {
        sqlx::query(
            r#"
            DELETE FROM promoted_manifest_nodes
            WHERE project_id = $1 AND environment_id = $2
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            DELETE FROM promoted_manifest_meta
            WHERE project_id = $1 AND environment_id = $2
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .execute(&self.pool)
        .await?;

        let base_run = sqlx::query(
            r#"
            SELECT r.run_id
            FROM runs r
            JOIN manifest_snapshots ms ON ms.run_id = r.run_id
            WHERE r.project_id = $1
              AND r.environment_id = $2
              AND r.terminal_status = 'success'
              AND r.is_full_graph_run = TRUE
              AND r.command IN ('run', 'build')
              AND ($3::BIGINT IS NULL OR r.id <= $3)
            ORDER BY r.id DESC
            LIMIT 1
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .bind(max_run_pk)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(base_run) = base_run {
            let run_id: Uuid = base_run.get("run_id");
            self.promote_manifest_state(run_id, project_id, environment_id, true)
                .await?;
        }

        sqlx::query(
            r#"
            WITH latest_success AS (
                SELECT DISTINCT ON (ne.unique_id)
                    ne.unique_id,
                    ne.run_id,
                    ne.checksum
                FROM node_executions ne
                JOIN runs r ON r.run_id = ne.run_id
                WHERE r.project_id = $1
                  AND r.environment_id = $2
                  AND r.command IN ('run', 'build')
                  AND ne.status = 'success'
                  AND ($3::BIGINT IS NULL OR r.id <= $3)
                ORDER BY ne.unique_id, r.id DESC
            )
            INSERT INTO promoted_manifest_nodes (
                project_id, environment_id, unique_id, source_run_id, checksum, raw_node
            )
            SELECT
                $1,
                $2,
                ls.unique_id,
                ls.run_id,
                ls.checksum,
                ms.manifest -> 'nodes' -> ls.unique_id
            FROM latest_success ls
            JOIN manifest_snapshots ms ON ms.run_id = ls.run_id
            WHERE ms.manifest -> 'nodes' -> ls.unique_id IS NOT NULL
            ON CONFLICT (project_id, environment_id, unique_id) DO UPDATE SET
                source_run_id = EXCLUDED.source_run_id,
                checksum = EXCLUDED.checksum,
                raw_node = EXCLUDED.raw_node,
                promoted_at = NOW()
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .bind(max_run_pk)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn rebuild_current_state_up_to(
        &self,
        project_id: i64,
        environment_id: i64,
        max_run_pk: Option<i64>,
    ) -> AppResult<u64> {
        let mut tx = self.pool.begin().await?;
        let rows = self
            .rebuild_current_state_up_to_in_tx(&mut tx, project_id, environment_id, max_run_pk)
            .await?;
        tx.commit().await?;
        Ok(rows)
    }

    async fn rebuild_current_state_up_to_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        project_id: i64,
        environment_id: i64,
        max_run_pk: Option<i64>,
    ) -> AppResult<u64> {
        sqlx::query(
            r#"
            DELETE FROM current_node_state
            WHERE project_id = $1 AND environment_id = $2
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .execute(&mut **tx)
        .await?;

        let inserted = sqlx::query(
            r#"
            WITH latest_execution AS (
                SELECT DISTINCT ON (ne.unique_id)
                    r.project_id,
                    r.environment_id,
                    ne.unique_id,
                    ne.run_id,
                    ne.status,
                    ne.resource_type,
                    ne.node_name,
                    ne.node_path,
                    ne.started_at,
                    ne.finished_at,
                    ne.execution_time_seconds
                FROM node_executions ne
                JOIN runs r ON r.run_id = ne.run_id
                WHERE r.project_id = $1
                  AND r.environment_id = $2
                  AND ($3::BIGINT IS NULL OR r.id <= $3)
                ORDER BY ne.unique_id, r.id DESC
            ),
            latest_success AS (
                SELECT DISTINCT ON (ne.unique_id)
                    ne.unique_id,
                    ne.materialized,
                    ne.relation_database,
                    ne.relation_schema,
                    ne.relation_alias,
                    ne.relation_name,
                    ne.checksum,
                    ne.finished_at
                FROM node_executions ne
                JOIN runs r ON r.run_id = ne.run_id
                WHERE r.project_id = $1
                  AND r.environment_id = $2
                  AND ($3::BIGINT IS NULL OR r.id <= $3)
                  AND ne.status = 'success'
                ORDER BY ne.unique_id, r.id DESC
            ),
            latest_state AS (
                SELECT DISTINCT ON (ne.unique_id)
                    ne.unique_id,
                    ne.materialized,
                    ne.relation_database,
                    ne.relation_schema,
                    ne.relation_alias,
                    ne.relation_name,
                    ne.checksum
                FROM node_executions ne
                JOIN runs r ON r.run_id = ne.run_id
                WHERE r.project_id = $1
                  AND r.environment_id = $2
                  AND ($3::BIGINT IS NULL OR r.id <= $3)
                ORDER BY ne.unique_id, r.id DESC
            )
            INSERT INTO current_node_state (
                project_id, environment_id, unique_id, last_run_id, status, resource_type,
                node_name, node_path, materialized, relation_database, relation_schema,
                relation_alias, relation_name, checksum, started_at, finished_at,
                execution_time_seconds, last_success_at, updated_at
            )
            SELECT
                le.project_id,
                le.environment_id,
                le.unique_id,
                le.run_id,
                le.status,
                le.resource_type,
                le.node_name,
                le.node_path,
                COALESCE(ls.materialized, state.materialized),
                COALESCE(ls.relation_database, state.relation_database),
                COALESCE(ls.relation_schema, state.relation_schema),
                COALESCE(ls.relation_alias, state.relation_alias),
                COALESCE(ls.relation_name, state.relation_name),
                COALESCE(ls.checksum, state.checksum),
                le.started_at,
                le.finished_at,
                le.execution_time_seconds,
                ls.finished_at,
                NOW()
            FROM latest_execution le
            LEFT JOIN latest_success ls ON ls.unique_id = le.unique_id
            LEFT JOIN latest_state state ON state.unique_id = le.unique_id
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .bind(max_run_pk)
        .execute(&mut **tx)
        .await?;

        Ok(inserted.rows_affected())
    }

    async fn load_reconstructed_manifest(
        &self,
        project_slug: &str,
        environment_slug: &str,
    ) -> AppResult<Option<ReconstructedManifest>> {
        let base_row = sqlx::query(
            r#"
            SELECT
                pmm.project_id,
                pmm.environment_id,
                pmm.base_manifest
            FROM promoted_manifest_meta pmm
            JOIN projects p ON p.id = pmm.project_id
            JOIN environments e ON e.id = pmm.environment_id
            WHERE p.slug = $1
              AND e.slug = $2
            "#,
        )
        .bind(project_slug)
        .bind(environment_slug)
        .fetch_optional(&self.pool)
        .await?;

        let Some(base_row) = base_row else {
            return Ok(None);
        };

        let project_id: i64 = base_row.get("project_id");
        let environment_id: i64 = base_row.get("environment_id");
        let base_manifest: sqlx::types::Json<Value> = base_row.get("base_manifest");

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

        let promoted_nodes = sqlx::query(
            r#"
            SELECT
                unique_id,
                raw_node
            FROM promoted_manifest_nodes
            WHERE project_id = $1
              AND environment_id = $2
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| {
            let unique_id: String = row.get("unique_id");
            let raw_node: sqlx::types::Json<Value> = row.get("raw_node");
            (unique_id, raw_node.0)
        })
        .collect::<BTreeMap<_, _>>();

        let reconstructed = ManifestSnapshot::reconstruct(
            base_manifest.0,
            &promoted_nodes,
            &current_nodes,
        );
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

fn should_promote_manifest(subcommand: &str) -> bool {
    matches!(subcommand, "run" | "build")
}

fn read_dbt_project_name(project_dir: &Path) -> String {
    read_yaml_scalar(&project_dir.join("dbt_project.yml"), "name")
        .unwrap_or_else(|| {
            project_dir
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned()
        })
}

fn read_adapter_type(profiles_dir: &Path) -> String {
    read_yaml_scalar(&profiles_dir.join("profiles.yml"), "type")
        .unwrap_or_else(|| "duckdb".to_string())
}

fn read_yaml_scalar(path: &Path, key: &str) -> Option<String> {
    let content = std::fs::read_to_string(path).ok()?;
    content.lines().find_map(|line| {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            return None;
        }
        let prefix = format!("{key}:");
        trimmed
            .strip_prefix(&prefix)
            .map(|value| value.trim().trim_matches('"').trim_matches('\'').to_string())
            .filter(|value| !value.is_empty())
    })
}
