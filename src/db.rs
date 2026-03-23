use crate::config::{InvocationContext, RuntimeConfig};
use crate::error::{AppError, AppResult};
use crate::event::LogEvent;
use crate::manifest::{ManifestSnapshot, ReconstructedManifest};
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

#[derive(Debug, Clone)]
pub struct ProjectRecord {
    pub id: i64,
    pub project_id: String,
    pub project_name: String,
    pub slug: String,
    pub git_repo_url: Option<String>,
    pub default_branch: Option<String>,
    pub project_root: Option<String>,
    pub metadata: Value,
}

#[derive(Debug, Clone)]
pub struct EnvironmentRecord {
    pub id: i64,
    pub project_id: i64,
    pub project_ref: String,
    pub project_slug: String,
    pub slug: String,
    pub kind: String,
    pub baseline_environment_id: Option<i64>,
    pub baseline_environment_slug: Option<String>,
    pub git_branch: Option<String>,
    pub git_commit_sha: Option<String>,
    pub git_ref: Option<String>,
    pub pr_number: Option<i32>,
    pub immutable: bool,
    pub protected: bool,
    pub status: String,
    pub schema_prefix: Option<String>,
    pub metadata: Value,
}

#[derive(Debug, Clone)]
pub struct CreateProjectInput {
    pub project_id: String,
    pub project_name: String,
    pub slug: String,
    pub git_repo_url: String,
    pub default_branch: Option<String>,
    pub project_root: String,
}

#[derive(Debug, Clone)]
pub struct CreateEnvironmentInput {
    pub project: String,
    pub slug: String,
    pub kind: String,
    pub baseline_slug: Option<String>,
    pub git_branch: Option<String>,
    pub git_commit_sha: Option<String>,
    pub git_ref: Option<String>,
    pub pr_number: Option<i32>,
    pub immutable: bool,
    pub protected: bool,
    pub status: String,
    pub schema_prefix: Option<String>,
}

#[derive(Debug, Clone)]
pub struct UpdateEnvironmentInput {
    pub project: String,
    pub slug: String,
    pub kind: Option<String>,
    pub baseline_slug: Option<String>,
    pub git_branch: Option<String>,
    pub git_commit_sha: Option<String>,
    pub git_ref: Option<String>,
    pub pr_number: Option<i32>,
    pub immutable: bool,
    pub protected: bool,
    pub status: Option<String>,
    pub schema_prefix: Option<String>,
}

#[derive(Debug, Clone)]
struct GitState {
    branch: Option<String>,
    commit_sha: Option<String>,
    repo_url: Option<String>,
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

    pub async fn create_project(&self, input: CreateProjectInput) -> AppResult<ProjectRecord> {
        let row = sqlx::query(
            r#"
            INSERT INTO projects (project_id, project_name, slug, git_repo_url, default_branch, project_root)
            VALUES ($1, $2, $3, $4, COALESCE($5, 'main'), $6)
            ON CONFLICT (project_id) DO UPDATE SET
                project_name = EXCLUDED.project_name,
                slug = EXCLUDED.slug,
                git_repo_url = EXCLUDED.git_repo_url,
                default_branch = EXCLUDED.default_branch,
                project_root = EXCLUDED.project_root
            RETURNING id, project_id, project_name, slug, git_repo_url, default_branch, project_root, metadata
            "#,
        )
        .bind(&input.project_id)
        .bind(&input.project_name)
        .bind(&input.slug)
        .bind(&input.git_repo_url)
        .bind(input.default_branch.as_deref())
        .bind(&input.project_root)
        .fetch_one(&self.pool)
        .await?;
        Ok(project_record_from_row(&row))
    }

    pub async fn reinitialize_project_id(
        &self,
        existing_project_id: &str,
        input: CreateProjectInput,
    ) -> AppResult<ProjectRecord> {
        let mut tx = self.pool.begin().await?;
        let existing_row = sqlx::query("SELECT id FROM projects WHERE project_id = $1")
            .bind(existing_project_id)
            .fetch_optional(&mut *tx)
            .await?;

        let Some(existing_row) = existing_row else {
            tx.rollback().await?;
            return self.create_project(input).await;
        };

        let project_pk: i64 = existing_row.get("id");

        sqlx::query("DELETE FROM environments WHERE project_id = $1")
            .bind(project_pk)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM runs WHERE project_id = $1")
            .bind(project_pk)
            .execute(&mut *tx)
            .await?;

        let row = sqlx::query(
            r#"
            UPDATE projects
            SET project_id = $2,
                project_name = $3,
                slug = $4,
                git_repo_url = $5,
                default_branch = COALESCE($6, 'main'),
                project_root = $7
            WHERE id = $1
            RETURNING id, project_id, project_name, slug, git_repo_url, default_branch, project_root, metadata
            "#,
        )
        .bind(project_pk)
        .bind(&input.project_id)
        .bind(&input.project_name)
        .bind(&input.slug)
        .bind(&input.git_repo_url)
        .bind(input.default_branch.as_deref())
        .bind(&input.project_root)
        .fetch_one(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(project_record_from_row(&row))
    }

    pub async fn list_projects(&self) -> AppResult<Vec<ProjectRecord>> {
        let rows = sqlx::query(
            "SELECT id, project_id, project_name, slug, git_repo_url, default_branch, project_root, metadata FROM projects ORDER BY slug",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(project_record_from_row).collect())
    }

    pub async fn get_project_by_identifier(&self, project: &str) -> AppResult<ProjectRecord> {
        let row = sqlx::query(
            "SELECT id, project_id, project_name, slug, git_repo_url, default_branch, project_root, metadata FROM projects WHERE project_id = $1 OR slug = $1",
        )
        .bind(project)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| AppError::ProjectNotFound(project.to_string()))?;
        Ok(project_record_from_row(&row))
    }

    pub async fn get_project_by_project_id(&self, project_id: &str) -> AppResult<ProjectRecord> {
        let row = sqlx::query(
            "SELECT id, project_id, project_name, slug, git_repo_url, default_branch, project_root, metadata FROM projects WHERE project_id = $1",
        )
        .bind(project_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| AppError::ProjectIdNotFound(project_id.to_string()))?;
        Ok(project_record_from_row(&row))
    }

    async fn get_project_by_repo_and_root(
        &self,
        git_repo_url: &str,
        project_root: &str,
    ) -> AppResult<Option<ProjectRecord>> {
        let row = sqlx::query(
            "SELECT id, project_id, project_name, slug, git_repo_url, default_branch, project_root, metadata FROM projects WHERE git_repo_url = $1 AND project_root = $2",
        )
        .bind(git_repo_url)
        .bind(project_root)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.as_ref().map(project_record_from_row))
    }

    pub async fn create_environment(
        &self,
        input: CreateEnvironmentInput,
    ) -> AppResult<EnvironmentRecord> {
        validate_environment_input(&input.kind, input.git_commit_sha.as_deref())?;
        let project = self.get_project_by_identifier(&input.project).await?;
        let baseline = match input.baseline_slug.as_deref() {
            Some(baseline_slug) => Some(
                self.get_environment_by_project_id(project.id, &project.slug, baseline_slug)
                    .await?,
            ),
            None => None,
        };

        let row = sqlx::query(
            r#"
            INSERT INTO environments (
                project_id, slug, kind, baseline_environment_id, git_branch, git_commit_sha,
                git_ref, pr_number, immutable, protected, status, schema_prefix
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (project_id, slug) DO UPDATE SET
                kind = EXCLUDED.kind,
                baseline_environment_id = EXCLUDED.baseline_environment_id,
                git_branch = EXCLUDED.git_branch,
                git_commit_sha = EXCLUDED.git_commit_sha,
                git_ref = EXCLUDED.git_ref,
                pr_number = EXCLUDED.pr_number,
                immutable = EXCLUDED.immutable,
                protected = EXCLUDED.protected,
                status = EXCLUDED.status,
                schema_prefix = EXCLUDED.schema_prefix
            RETURNING id
            "#,
        )
        .bind(project.id)
        .bind(&input.slug)
        .bind(&input.kind)
        .bind(baseline.as_ref().map(|env| env.id))
        .bind(input.git_branch.as_deref())
        .bind(input.git_commit_sha.as_deref())
        .bind(input.git_ref.as_deref())
        .bind(input.pr_number)
        .bind(input.immutable)
        .bind(input.protected)
        .bind(&input.status)
        .bind(input.schema_prefix.as_deref())
        .fetch_one(&self.pool)
        .await?;
        let environment_id: i64 = row.get("id");
        let environment = self.get_environment_by_id(environment_id).await?;
        self.record_environment_version(&environment, "created").await?;
        Ok(environment)
    }

    pub async fn update_environment(
        &self,
        input: UpdateEnvironmentInput,
    ) -> AppResult<EnvironmentRecord> {
        let project = self.get_project_by_identifier(&input.project).await?;
        let existing = self
            .get_environment_by_project_id(project.id, &project.slug, &input.slug)
            .await?;

        if existing.immutable {
            let changing_identity = input.kind.is_some()
                || input.baseline_slug.is_some()
                || input.git_branch.is_some()
                || input.git_commit_sha.is_some()
                || input.immutable;
            if changing_identity {
                return Err(AppError::ImmutableEnvironment(
                    project.project_id,
                    existing.slug,
                ));
            }
        }

        let kind = input.kind.as_deref().unwrap_or(&existing.kind).to_string();
        let baseline_environment_id = match input.baseline_slug.as_deref() {
            Some(baseline_slug) => Some(
                self.get_environment_by_project_id(project.id, &project.slug, baseline_slug)
                    .await?
                    .id,
            ),
            None => existing.baseline_environment_id,
        };
        let git_branch = input.git_branch.or(existing.git_branch.clone());
        let git_commit_sha = input.git_commit_sha.or(existing.git_commit_sha.clone());
        validate_environment_input(&kind, git_commit_sha.as_deref())?;
        let immutable = existing.immutable || input.immutable;
        let status = input.status.unwrap_or(existing.status.clone());

        sqlx::query(
            r#"
            UPDATE environments
            SET kind = $3,
                baseline_environment_id = $4,
                git_branch = $5,
                git_commit_sha = $6,
                git_ref = $7,
                pr_number = $8,
                immutable = $9,
                protected = $10,
                status = $11,
                schema_prefix = $12
            WHERE id = $1 AND project_id = $2
            "#,
        )
        .bind(existing.id)
        .bind(project.id)
        .bind(&kind)
        .bind(baseline_environment_id)
        .bind(git_branch.as_deref())
        .bind(git_commit_sha.as_deref())
        .bind(input.git_ref.as_deref().or(existing.git_ref.as_deref()))
        .bind(input.pr_number.or(existing.pr_number))
        .bind(immutable)
        .bind(input.protected || existing.protected)
        .bind(&status)
        .bind(input.schema_prefix.as_deref().or(existing.schema_prefix.as_deref()))
        .execute(&self.pool)
        .await?;

        let environment = self.get_environment_by_id(existing.id).await?;
        self.record_environment_version(&environment, "updated").await?;
        Ok(environment)
    }

    pub async fn list_environments(&self, project: &str) -> AppResult<Vec<EnvironmentRecord>> {
        let project = self.get_project_by_identifier(project).await?;
        let rows = sqlx::query(
            r#"
            SELECT
                e.id,
                e.project_id,
                p.project_id AS project_ref,
                p.slug AS project_slug,
                e.slug,
                e.kind,
                e.baseline_environment_id,
                be.slug AS baseline_environment_slug,
                e.git_branch,
                e.git_commit_sha,
                e.git_ref,
                e.pr_number,
                e.immutable,
                e.protected,
                e.status,
                e.schema_prefix,
                e.metadata
            FROM environments e
            JOIN projects p ON p.id = e.project_id
            LEFT JOIN environments be ON be.id = e.baseline_environment_id
            WHERE e.project_id = $1
            ORDER BY e.slug
            "#,
        )
        .bind(project.id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(environment_record_from_row).collect())
    }

    pub async fn get_environment(
        &self,
        project: &str,
        environment_slug: &str,
    ) -> AppResult<EnvironmentRecord> {
        let project = self.get_project_by_identifier(project).await?;
        self.get_environment_by_project_id(project.id, &project.slug, environment_slug)
            .await
    }

    pub async fn seed_environment_from(
        &self,
        project: &str,
        target_environment_slug: &str,
        source_environment_slug: &str,
        seed_type: &str,
    ) -> AppResult<()> {
        let project = self.get_project_by_identifier(project).await?;
        let target = self
            .get_environment_by_project_id(project.id, &project.slug, target_environment_slug)
            .await?;
        let source = self
            .get_environment_by_project_id(project.id, &project.slug, source_environment_slug)
            .await?;

        let mut tx = self.pool.begin().await?;

        sqlx::query(
            "DELETE FROM promoted_manifest_nodes WHERE project_id = $1 AND environment_id = $2",
        )
        .bind(project.id)
        .bind(target.id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "DELETE FROM promoted_manifest_meta WHERE project_id = $1 AND environment_id = $2",
        )
        .bind(project.id)
        .bind(target.id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "DELETE FROM current_node_state WHERE project_id = $1 AND environment_id = $2",
        )
        .bind(project.id)
        .bind(target.id)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO promoted_manifest_meta (project_id, environment_id, source_run_id, base_manifest, promoted_at)
            SELECT $1, $2, source_run_id, base_manifest, NOW()
            FROM promoted_manifest_meta
            WHERE project_id = $1 AND environment_id = $3
            "#,
        )
        .bind(project.id)
        .bind(target.id)
        .bind(source.id)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO promoted_manifest_nodes (
                project_id, environment_id, unique_id, source_run_id, checksum, raw_node, promoted_at
            )
            SELECT $1, $2, unique_id, source_run_id, checksum, raw_node, NOW()
            FROM promoted_manifest_nodes
            WHERE project_id = $1 AND environment_id = $3
            "#,
        )
        .bind(project.id)
        .bind(target.id)
        .bind(source.id)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO current_node_state (
                project_id, environment_id, unique_id, last_run_id, status, resource_type,
                node_name, node_path, materialized, relation_database, relation_schema,
                relation_alias, relation_name, checksum, started_at, finished_at,
                execution_time_seconds, last_success_at, updated_at
            )
            SELECT
                $1, $2, unique_id, last_run_id, status, resource_type,
                node_name, node_path, materialized, relation_database, relation_schema,
                relation_alias, relation_name, checksum, started_at, finished_at,
                execution_time_seconds, last_success_at, NOW()
            FROM current_node_state
            WHERE project_id = $1 AND environment_id = $3
            "#,
        )
        .bind(project.id)
        .bind(target.id)
        .bind(source.id)
        .execute(&mut *tx)
        .await?;

        let source_run_id: Option<Uuid> = sqlx::query_scalar(
            "SELECT source_run_id FROM promoted_manifest_meta WHERE project_id = $1 AND environment_id = $2",
        )
        .bind(project.id)
        .bind(source.id)
        .fetch_optional(&mut *tx)
        .await?
        .flatten();

        sqlx::query(
            r#"
            INSERT INTO environment_seeds (
                project_id, target_environment_id, source_environment_id, seed_type, source_run_id, metadata
            )
            VALUES ($1, $2, $3, $4, $5, '{}'::jsonb)
            "#,
        )
        .bind(project.id)
        .bind(target.id)
        .bind(source.id)
        .bind(seed_type)
        .bind(source_run_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        let target = self.get_environment_by_id(target.id).await?;
        self.record_environment_version(&target, "seeded").await?;
        Ok(())
    }

    pub async fn persisting_invocation(
        &self,
        subcommand: &str,
        config: &RuntimeConfig,
        incoming_args: &[OsString],
    ) -> AppResult<()> {
        let ctx = InvocationContext::from_args(incoming_args, true)?;
        let project = self
            .resolve_local_project(&ctx.project_dir, &ctx.project_slug)
            .await?;
        let git_state = read_git_state(&ctx.project_dir);
        let run_id = Uuid::new_v4();
        let reconstructed_manifest = self
            .load_reconstructed_manifest(&project.slug, &ctx.environment_slug)
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
            .ensure_environment(&project, &ctx.environment_slug)
            .await?;
        let environment = self.get_environment_by_id(environment_id).await?;
        validate_environment_git_state(&project, &environment, &git_state)?;

        sqlx::query(
            r#"
            INSERT INTO runs (
                run_id, project_id, environment_id, dbt_invocation_id, command, args, is_full_graph_run,
                git_branch, git_commit_sha, git_repo_url, project_root, project_name, project_ref
            )
            VALUES ($1, $2, $3, $1, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            "#,
        )
        .bind(run_id)
        .bind(project_id)
        .bind(environment_id)
        .bind(subcommand)
        .bind(args_json)
        .bind(ctx.is_full_graph_run)
        .bind(git_state.branch.as_deref())
        .bind(git_state.commit_sha.as_deref())
        .bind(git_state.repo_url.as_deref().or(project.git_repo_url.as_deref()))
        .bind(project.project_root.as_deref())
        .bind(&project.project_name)
        .bind(&project.project_id)
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
        let project = self
            .resolve_local_project(&ctx.project_dir, &ctx.project_slug)
            .await?;
        let git_state = read_git_state(&ctx.project_dir);
        let (_, environment_id) = self
            .ensure_environment(&project, &ctx.environment_slug)
            .await?;
        let environment = self.get_environment_by_id(environment_id).await?;
        validate_environment_git_state(&project, &environment, &git_state)?;
        let reconstructed_manifest = self
            .load_reconstructed_manifest(&project.slug, &ctx.environment_slug)
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

    async fn resolve_local_project(
        &self,
        project_dir: &Path,
        fallback_slug: &str,
    ) -> AppResult<ProjectRecord> {
        let project_name = read_dbt_project_name(project_dir);
        if let Some(project_id) = read_dbtx_project_id(project_dir)? {
            let project = self.get_project_by_project_id(&project_id).await?;
            validate_project_record(&project, project_dir)?;
            return Ok(project);
        }

        let git_context = git_repo_root(project_dir)
            .and_then(|repo_root| {
                let git_repo_url = git_remote_origin_url(&repo_root)?;
                let project_root = relative_project_root(&repo_root, project_dir);
                Ok((git_repo_url, project_root))
            })
            .ok();

        if let Some((git_repo_url, project_root)) = git_context.clone()
            && let Some(project) = self
                .get_project_by_repo_and_root(&git_repo_url, &project_root)
                .await?
        {
            validate_project_record(&project, project_dir)?;
            return Ok(project);
        }

        Ok(ProjectRecord {
            id: 0,
            project_id: fallback_slug.to_string(),
            project_name,
            slug: fallback_slug.to_string(),
            git_repo_url: git_context.as_ref().map(|(git_repo_url, _)| git_repo_url.clone()),
            default_branch: None,
            project_root: git_context.map(|(_, project_root)| project_root),
            metadata: Value::Object(Default::default()),
        })
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

    async fn record_environment_version(
        &self,
        environment: &EnvironmentRecord,
        reason: &str,
    ) -> AppResult<()> {
        sqlx::query(
            r#"
            INSERT INTO environment_versions (
                environment_id, project_id, reason, git_branch, git_commit_sha, kind,
                immutable, baseline_environment_id, metadata
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#,
        )
        .bind(environment.id)
        .bind(environment.project_id)
        .bind(reason)
        .bind(environment.git_branch.as_deref())
        .bind(environment.git_commit_sha.as_deref())
        .bind(&environment.kind)
        .bind(environment.immutable)
        .bind(environment.baseline_environment_id)
        .bind(sqlx::types::Json(&environment.metadata))
        .execute(&self.pool)
        .await?;
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

    async fn ensure_environment(
        &self,
        project: &ProjectRecord,
        environment_slug: &str,
    ) -> AppResult<(i64, i64)> {
        let project_id: i64 = sqlx::query_scalar(
            r#"
            INSERT INTO projects (project_id, project_name, slug, git_repo_url, default_branch, project_root)
            VALUES ($1, $2, $3, $4, COALESCE($5, 'main'), $6)
            ON CONFLICT (project_id) DO UPDATE SET
                project_name = EXCLUDED.project_name,
                slug = EXCLUDED.slug,
                git_repo_url = COALESCE(EXCLUDED.git_repo_url, projects.git_repo_url),
                default_branch = COALESCE(EXCLUDED.default_branch, projects.default_branch),
                project_root = COALESCE(EXCLUDED.project_root, projects.project_root)
            RETURNING id
            "#,
        )
        .bind(&project.project_id)
        .bind(&project.project_name)
        .bind(&project.slug)
        .bind(project.git_repo_url.as_deref())
        .bind(project.default_branch.as_deref())
        .bind(project.project_root.as_deref())
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

    async fn get_environment_by_project_id(
        &self,
        project_id: i64,
        project_slug: &str,
        environment_slug: &str,
    ) -> AppResult<EnvironmentRecord> {
        let row = sqlx::query(
            r#"
            SELECT
                e.id,
                e.project_id,
                p.project_id AS project_ref,
                p.slug AS project_slug,
                e.slug,
                e.kind,
                e.baseline_environment_id,
                be.slug AS baseline_environment_slug,
                e.git_branch,
                e.git_commit_sha,
                e.git_ref,
                e.pr_number,
                e.immutable,
                e.protected,
                e.status,
                e.schema_prefix,
                e.metadata
            FROM environments e
            JOIN projects p ON p.id = e.project_id
            LEFT JOIN environments be ON be.id = e.baseline_environment_id
            WHERE e.project_id = $1
              AND e.slug = $2
            "#,
        )
        .bind(project_id)
        .bind(environment_slug)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| {
            AppError::EnvironmentNotFound(project_slug.to_string(), environment_slug.to_string())
        })?;
        Ok(environment_record_from_row(&row))
    }

    async fn get_environment_by_id(&self, environment_id: i64) -> AppResult<EnvironmentRecord> {
        let row = sqlx::query(
            r#"
            SELECT
                e.id,
                e.project_id,
                p.project_id AS project_ref,
                p.slug AS project_slug,
                e.slug,
                e.kind,
                e.baseline_environment_id,
                be.slug AS baseline_environment_slug,
                e.git_branch,
                e.git_commit_sha,
                e.git_ref,
                e.pr_number,
                e.immutable,
                e.protected,
                e.status,
                e.schema_prefix,
                e.metadata
            FROM environments e
            JOIN projects p ON p.id = e.project_id
            LEFT JOIN environments be ON be.id = e.baseline_environment_id
            WHERE e.id = $1
            "#,
        )
        .bind(environment_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(environment_record_from_row(&row))
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
            let promote_manifest_state = node
                .status
                .as_deref()
                .is_some_and(is_promotable_status);
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
              AND ne.status IN ('success', 'pass', 'created')
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
                  AND ne.status IN ('success', 'pass', 'created')
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
                  AND ne.status IN ('success', 'pass', 'created')
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

        let reconstructed = ManifestSnapshot::reconstruct(base_manifest.0, &promoted_nodes);
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

fn is_promotable_status(status: &str) -> bool {
    matches!(status, "success" | "pass" | "created")
}

fn validate_environment_input(kind: &str, git_commit_sha: Option<&str>) -> AppResult<()> {
    if kind == "commit" && git_commit_sha.is_none() {
        return Err(AppError::CommitEnvironmentRequiresSha);
    }
    Ok(())
}

fn project_record_from_row(row: &sqlx::postgres::PgRow) -> ProjectRecord {
    let metadata: sqlx::types::Json<Value> = row.get("metadata");
    ProjectRecord {
        id: row.get("id"),
        project_id: row.get("project_id"),
        project_name: row.get("project_name"),
        slug: row.get("slug"),
        git_repo_url: row.get("git_repo_url"),
        default_branch: row.get("default_branch"),
        project_root: row.get("project_root"),
        metadata: metadata.0,
    }
}

fn environment_record_from_row(row: &sqlx::postgres::PgRow) -> EnvironmentRecord {
    let metadata: sqlx::types::Json<Value> = row.get("metadata");
    EnvironmentRecord {
        id: row.get("id"),
        project_id: row.get("project_id"),
        project_ref: row.get("project_ref"),
        project_slug: row.get("project_slug"),
        slug: row.get("slug"),
        kind: row.get("kind"),
        baseline_environment_id: row.get("baseline_environment_id"),
        baseline_environment_slug: row.get("baseline_environment_slug"),
        git_branch: row.get("git_branch"),
        git_commit_sha: row.get("git_commit_sha"),
        git_ref: row.get("git_ref"),
        pr_number: row.get("pr_number"),
        immutable: row.get("immutable"),
        protected: row.get("protected"),
        status: row.get("status"),
        schema_prefix: row.get("schema_prefix"),
        metadata: metadata.0,
    }
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

fn read_dbtx_project_id(project_dir: &Path) -> AppResult<Option<String>> {
    let path = project_dir.join("dbt_project.yml");
    let content = std::fs::read_to_string(path)?;
    let mut in_vars = false;
    let mut vars_indent = 0usize;
    let mut in_dbtx = false;
    let mut dbtx_indent = 0usize;

    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let indent = line.len() - line.trim_start().len();
        if in_dbtx && indent <= dbtx_indent {
            in_dbtx = false;
        }
        if in_vars && indent <= vars_indent && trimmed != "vars:" {
            in_vars = false;
            in_dbtx = false;
        }
        if trimmed == "vars:" {
            in_vars = true;
            vars_indent = indent;
            in_dbtx = false;
            continue;
        }
        if in_vars && trimmed == "dbtx:" {
            in_dbtx = true;
            dbtx_indent = indent;
            continue;
        }
        if in_dbtx
            && let Some(rest) = trimmed.strip_prefix("project_id:")
        {
            let value = rest.trim().trim_matches('"').trim_matches('\'');
            if !value.is_empty() {
                return Ok(Some(value.to_string()));
            }
        }
    }

    Ok(None)
}

fn git_repo_root(current_dir: &Path) -> AppResult<std::path::PathBuf> {
    let output = run_git(["rev-parse", "--show-toplevel"], current_dir)?;
    Ok(output.into())
}

fn git_remote_origin_url(repo_root: &Path) -> AppResult<String> {
    run_git(["config", "--get", "remote.origin.url"], repo_root)
        .map_err(|_| AppError::GitRemoteNotFound)
}

fn relative_project_root(repo_root: &Path, project_root: &Path) -> String {
    match project_root.strip_prefix(repo_root) {
        Ok(path) if path.as_os_str().is_empty() => ".".to_string(),
        Ok(path) => path.to_string_lossy().into_owned(),
        Err(_) => project_root.to_string_lossy().into_owned(),
    }
}

fn validate_project_record(project: &ProjectRecord, project_dir: &Path) -> AppResult<()> {
    let repo_root = git_repo_root(project_dir)?;
    let current_name = read_dbt_project_name(project_dir);
    let current_git_repo_url = git_remote_origin_url(&repo_root)?;
    let current_project_root = relative_project_root(&repo_root, project_dir);

    let matches = project.project_name == current_name
        && project.git_repo_url.as_deref() == Some(current_git_repo_url.as_str())
        && project.project_root.as_deref() == Some(current_project_root.as_str());

    if matches {
        Ok(())
    } else {
        Err(AppError::ProjectValidationFailed(project.project_id.clone()))
    }
}

fn read_git_state(project_dir: &Path) -> GitState {
    let repo_root = git_repo_root(project_dir).ok();
    let repo_url = repo_root
        .as_deref()
        .and_then(|root| git_remote_origin_url(root).ok());
    let branch = repo_root.as_deref().and_then(|root| {
        run_git(["rev-parse", "--abbrev-ref", "HEAD"], root)
            .ok()
            .filter(|value| value != "HEAD")
    });
    let commit_sha = repo_root
        .as_deref()
        .and_then(|root| run_git(["rev-parse", "HEAD"], root).ok());
    GitState {
        branch,
        commit_sha,
        repo_url,
    }
}

fn validate_environment_git_state(
    project: &ProjectRecord,
    environment: &EnvironmentRecord,
    git_state: &GitState,
) -> AppResult<()> {
    if !environment.immutable {
        return Ok(());
    }

    let branch_matches = environment.git_branch.is_none()
        || environment.git_branch == git_state.branch;
    let commit_matches = environment.git_commit_sha.is_none()
        || environment.git_commit_sha == git_state.commit_sha;

    if branch_matches && commit_matches {
        Ok(())
    } else {
        Err(AppError::ImmutableEnvironmentGitMismatch(
            project.project_id.clone(),
            environment.slug.clone(),
        ))
    }
}

fn run_git<const N: usize>(args: [&str; N], cwd: &Path) -> AppResult<String> {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()?;
    if !output.status.success() {
        return Err(AppError::GitRepoNotFound);
    }
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if stdout.is_empty() {
        return Err(AppError::GitRepoNotFound);
    }
    Ok(stdout)
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
