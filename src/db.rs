use crate::api::{
    InvocationCancelStateApi, InvocationClaimResponse, InvocationEvent, InvocationExecutionModeApi,
    InvocationExecutionSpecApi, InvocationLifecycleStatus, InvocationListApiRequest,
    InvocationStatusResponse, InvocationWorkerHealthApi, QueueStatusResponse, WorkerStatusResponse,
};
use crate::error::{AppError, AppResult};
use crate::event::LogEvent;
use crate::execution::{ExecutionMode, heartbeat_stale_timeout};
use crate::manifest::{ManifestSnapshot, ReconstructedManifest};
use crate::profile::{
    EnvironmentProfileRecord, GeneratedProfiles, resolve_runtime_profile,
    validate_environment_profile,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::migrate::Migrator;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::path::Path;
use std::process::Stdio;
use tokio::process::{Child, Command};
use utoipa::ToSchema;
use uuid::Uuid;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AppliedMigration {
    pub version: i64,
    pub description: String,
}

#[derive(Clone)]
pub struct Db {
    pool: PgPool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ProjectRecord {
    pub id: i64,
    pub project_id: String,
    pub project_name: String,
    pub mode: String,
    pub git_repo_url: Option<String>,
    pub default_branch: Option<String>,
    pub project_root: Option<String>,
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct EnvironmentRecord {
    pub id: i64,
    pub project_id: i64,
    pub project_ref: String,
    pub project_name: String,
    pub slug: String,
    pub profile_name: String,
    pub target_name: String,
    pub baseline_environment_id: Option<i64>,
    pub baseline_environment_slug: Option<String>,
    pub git_branch: Option<String>,
    pub git_commit_sha: Option<String>,
    pub pr_number: Option<i32>,
    pub status: String,
    pub adapter_type: String,
    pub worker_queue: String,
    pub schema_name: String,
    pub threads: Option<i32>,
    pub profile_config: Value,
    pub profile_secrets: Value,
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct EnvironmentVersionRecord {
    pub id: i64,
    pub environment_id: i64,
    pub project_id: i64,
    pub recorded_at: chrono::DateTime<Utc>,
    pub reason: String,
    pub git_branch: Option<String>,
    pub git_commit_sha: Option<String>,
    pub baseline_environment_id: Option<i64>,
    pub metadata: Value,
}

#[derive(Debug, Clone)]
pub struct CreateProjectInput {
    pub project_id: String,
    pub project_name: String,
    pub mode: String,
    pub git_repo_url: Option<String>,
    pub default_branch: Option<String>,
    pub project_root: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CreateEnvironmentInput {
    pub project: String,
    pub slug: String,
    pub profile_name: String,
    pub target_name: String,
    pub baseline_slug: Option<String>,
    pub git_branch: Option<String>,
    pub git_commit_sha: Option<String>,
    pub pr_number: Option<i32>,
    pub status: String,
    pub adapter_type: String,
    pub worker_queue: Option<String>,
    pub schema_name: Option<String>,
    pub threads: Option<i32>,
    pub profile_config: Value,
    pub profile_secrets: Value,
}

#[derive(Debug, Clone)]
pub struct UpdateEnvironmentInput {
    pub project: String,
    pub slug: String,
    pub baseline_slug: Option<String>,
    pub git_branch: Option<String>,
    pub git_commit_sha: Option<String>,
    pub pr_number: Option<i32>,
    pub status: Option<String>,
    pub adapter_type: Option<String>,
    pub worker_queue: Option<String>,
    pub profile_name: Option<String>,
    pub target_name: Option<String>,
    pub schema_name: Option<String>,
    pub threads: Option<i32>,
    pub profile_config: Option<Value>,
    pub profile_secrets: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct EnvironmentReleaseInput {
    pub project: String,
    pub slug: String,
    pub git_branch: Option<String>,
    pub git_commit_sha: String,
}

#[derive(Debug, Clone)]
pub(crate) struct GitState {
    pub(crate) branch: Option<String>,
    pub(crate) commit_sha: Option<String>,
    pub(crate) repo_url: Option<String>,
}

pub(crate) struct RunFinalization<'a> {
    pub(crate) run_id: Uuid,
    pub(crate) project_id: i64,
    pub(crate) environment_id: i64,
    pub(crate) subcommand: &'a str,
    pub(crate) dbt_version: Option<&'a str>,
    pub(crate) exit_code: i32,
    pub(crate) terminal_status: &'a str,
    pub(crate) manifest: Option<&'a ManifestSnapshot>,
    pub(crate) promote_base_manifest: bool,
}

pub(crate) struct RunStart<'a> {
    pub(crate) run_id: Uuid,
    pub(crate) project: &'a ProjectRecord,
    pub(crate) environment: &'a EnvironmentRecord,
    pub(crate) subcommand: &'a str,
    pub(crate) args_json: Value,
    pub(crate) is_full_graph_run: bool,
    pub(crate) execution_mode: ExecutionMode,
    pub(crate) git_state: &'a GitState,
}

#[derive(Debug, Clone)]
pub(crate) struct CreateInvocationInput {
    pub(crate) invocation_id: Uuid,
    pub(crate) run_id: Option<Uuid>,
    pub(crate) project_id: i64,
    pub(crate) environment_id: i64,
    pub(crate) command: String,
    pub(crate) execution_mode: InvocationExecutionModeApi,
    pub(crate) worker_queue: String,
    pub(crate) execution_spec: Option<InvocationExecutionSpecApi>,
    pub(crate) promote_base_manifest: bool,
    pub(crate) claim_deadline_at: Option<chrono::DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub(crate) struct InvocationPersistenceRecord {
    pub(crate) run_id: Option<Uuid>,
    pub(crate) project_id: i64,
    pub(crate) environment_id: i64,
    pub(crate) command: String,
    pub(crate) promote_base_manifest: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct LocalEnvironmentUpsertInput<'a> {
    pub(crate) project: &'a ProjectRecord,
    pub(crate) profile_name: &'a str,
    pub(crate) target_name: &'a str,
    pub(crate) adapter_type: &'a str,
    pub(crate) schema_name: &'a str,
    pub(crate) threads: Option<i32>,
    pub(crate) profile_config: &'a Value,
    pub(crate) profile_secrets: &'a Value,
}

#[derive(Debug, Clone)]
pub(crate) struct TimedOutInvocationRecord {
    pub(crate) invocation_id: Uuid,
    pub(crate) status: InvocationLifecycleStatus,
    pub(crate) exit_code: i32,
    pub(crate) error: String,
}

#[derive(Debug, Clone)]
pub(crate) struct InvocationCancellationRecord {
    pub(crate) invocation_id: Uuid,
    pub(crate) status: InvocationLifecycleStatus,
    pub(crate) exit_code: i32,
    pub(crate) error: String,
}

#[derive(Debug, Clone)]
struct InvocationReadModel {
    execution_mode: InvocationExecutionModeApi,
    worker_queue: String,
    status: InvocationLifecycleStatus,
    started_at: chrono::DateTime<Utc>,
    claimed_at: Option<chrono::DateTime<Utc>>,
    last_heartbeat_at: Option<chrono::DateTime<Utc>>,
    claimed_by: Option<String>,
}

impl Db {
    pub async fn connect(database_url: &str) -> AppResult<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(20)
            .connect(database_url)
            .await?;
        Ok(Self { pool })
    }

    pub async fn require_current_schema(&self) -> AppResult<()> {
        let applied = self.migration_versions().await?;
        let expected: BTreeSet<i64> = MIGRATOR.iter().map(|migration| migration.version).collect();
        if applied == expected {
            Ok(())
        } else {
            Err(AppError::SchemaOutOfDate)
        }
    }

    pub async fn ping(&self) -> AppResult<()> {
        sqlx::query_scalar::<_, i32>("SELECT 1")
            .fetch_one(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn migrate(&self) -> AppResult<Vec<AppliedMigration>> {
        let before_versions = self.migration_versions().await?;
        MIGRATOR.run(&self.pool).await?;
        let after = self.migration_rows().await?;
        Ok(after
            .into_iter()
            .filter(|migration| !before_versions.contains(&migration.version))
            .collect())
    }

    pub async fn create_project(&self, input: CreateProjectInput) -> AppResult<ProjectRecord> {
        validate_project_input(&input.mode, input.project_root.as_deref())?;
        let row = sqlx::query(
            r#"
            INSERT INTO projects (project_id, project_name, mode, git_repo_url, default_branch, project_root)
            VALUES ($1, $2, $3, $4, COALESCE($5, 'main'), $6)
            RETURNING id, project_id, project_name, mode, git_repo_url, default_branch, project_root, metadata
            "#,
        )
        .bind(&input.project_id)
        .bind(&input.project_name)
        .bind(&input.mode)
        .bind(input.git_repo_url.as_deref())
        .bind(input.default_branch.as_deref())
        .bind(input.project_root.as_deref())
        .fetch_one(&self.pool)
        .await?;
        Ok(project_record_from_row(&row))
    }

    pub async fn update_project(&self, input: CreateProjectInput) -> AppResult<ProjectRecord> {
        validate_project_input(&input.mode, input.project_root.as_deref())?;
        let row = sqlx::query(
            r#"
            UPDATE projects
            SET project_name = $2,
                mode = $3,
                git_repo_url = $4,
                default_branch = COALESCE($5, 'main'),
                project_root = $6
            WHERE project_id = $1
            RETURNING id, project_id, project_name, mode, git_repo_url, default_branch, project_root, metadata
            "#,
        )
        .bind(&input.project_id)
        .bind(&input.project_name)
        .bind(&input.mode)
        .bind(input.git_repo_url.as_deref())
        .bind(input.default_branch.as_deref())
        .bind(input.project_root.as_deref())
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| AppError::ProjectIdNotFound(input.project_id.clone()))?;
        Ok(project_record_from_row(&row))
    }

    pub async fn upsert_project(&self, input: CreateProjectInput) -> AppResult<ProjectRecord> {
        validate_project_input(&input.mode, input.project_root.as_deref())?;
        let row = sqlx::query(
            r#"
            INSERT INTO projects (project_id, project_name, mode, git_repo_url, default_branch, project_root)
            VALUES ($1, $2, $3, $4, COALESCE($5, 'main'), $6)
            ON CONFLICT (project_id) DO UPDATE
            SET project_name = EXCLUDED.project_name,
                mode = EXCLUDED.mode,
                git_repo_url = EXCLUDED.git_repo_url,
                default_branch = EXCLUDED.default_branch,
                project_root = EXCLUDED.project_root
            RETURNING id, project_id, project_name, mode, git_repo_url, default_branch, project_root, metadata
            "#,
        )
        .bind(&input.project_id)
        .bind(&input.project_name)
        .bind(&input.mode)
        .bind(input.git_repo_url.as_deref())
        .bind(input.default_branch.as_deref())
        .bind(input.project_root.as_deref())
        .fetch_one(&self.pool)
        .await?;
        Ok(project_record_from_row(&row))
    }

    pub async fn reinitialize_project_id(
        &self,
        existing_project_id: &str,
        input: CreateProjectInput,
    ) -> AppResult<ProjectRecord> {
        validate_project_input(&input.mode, input.project_root.as_deref())?;
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
                mode = $4,
                git_repo_url = $5,
                default_branch = COALESCE($6, 'main'),
                project_root = $7
            WHERE id = $1
            RETURNING id, project_id, project_name, mode, git_repo_url, default_branch, project_root, metadata
            "#,
        )
        .bind(project_pk)
        .bind(&input.project_id)
        .bind(&input.project_name)
        .bind(&input.mode)
        .bind(input.git_repo_url.as_deref())
        .bind(input.default_branch.as_deref())
        .bind(input.project_root.as_deref())
        .fetch_one(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(project_record_from_row(&row))
    }

    pub async fn list_projects(&self) -> AppResult<Vec<ProjectRecord>> {
        let rows = sqlx::query(
            "SELECT id, project_id, project_name, mode, git_repo_url, default_branch, project_root, metadata FROM projects ORDER BY project_name",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(project_record_from_row).collect())
    }

    pub async fn get_project_by_project_id(&self, project_id: &str) -> AppResult<ProjectRecord> {
        let row = sqlx::query(
            "SELECT id, project_id, project_name, mode, git_repo_url, default_branch, project_root, metadata FROM projects WHERE project_id = $1",
        )
        .bind(project_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| AppError::ProjectIdNotFound(project_id.to_string()))?;
        Ok(project_record_from_row(&row))
    }

    pub async fn create_environment(
        &self,
        input: CreateEnvironmentInput,
    ) -> AppResult<EnvironmentRecord> {
        validate_environment_status(&input.status)?;
        let project = self.get_project_by_project_id(&input.project).await?;
        validate_environment_git_metadata(&project, &input.slug, input.git_commit_sha.as_deref())?;
        validate_environment_profile(
            &input.adapter_type,
            input.schema_name.as_deref().unwrap_or(""),
            input.threads,
            &input.profile_config,
            &input.profile_secrets,
            false,
        )?;
        let worker_queue = input
            .worker_queue
            .clone()
            .unwrap_or_else(|| "generic".to_string());
        let baseline = match input.baseline_slug.as_deref() {
            Some(baseline_slug) => Some(
                self.get_environment_by_project_id(project.id, &project.project_id, baseline_slug)
                    .await?,
            ),
            None => None,
        };
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            r#"
            INSERT INTO environments (
                project_id, slug, profile_name, target_name, baseline_environment_id, git_branch, git_commit_sha,
                pr_number, status, adapter_type, worker_queue, schema_name, threads,
                profile_config, profile_secrets
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            RETURNING id
            "#,
        )
        .bind(project.id)
        .bind(&input.slug)
        .bind(&input.profile_name)
        .bind(&input.target_name)
        .bind(baseline.as_ref().map(|env| env.id))
        .bind(input.git_branch.as_deref())
        .bind(input.git_commit_sha.as_deref())
        .bind(input.pr_number)
        .bind(&input.status)
        .bind(&input.adapter_type)
        .bind(&worker_queue)
        .bind(input.schema_name.as_deref())
        .bind(input.threads)
        .bind(sqlx::types::Json(&input.profile_config))
        .bind(sqlx::types::Json(&input.profile_secrets))
        .fetch_one(&mut *tx)
        .await
        .map_err(|err| match &err {
            sqlx::Error::Database(db_err) if db_err.code().as_deref() == Some("23505") => {
                AppError::EnvironmentAlreadyExists(project.project_id.clone(), input.slug.clone())
            }
            _ => AppError::Sqlx(err),
        })?;
        let environment_id: i64 = row.get("id");
        let environment = self
            .get_environment_by_id_in_tx(&mut tx, environment_id)
            .await?;
        if let Some(source) = baseline.as_ref() {
            self.seed_environment_from_tx(&mut tx, &project, &environment, source, "clone")
                .await?;
        }
        tx.commit().await?;
        self.record_environment_version(&environment, "created")
            .await?;
        Ok(environment)
    }

    pub async fn update_environment(
        &self,
        input: UpdateEnvironmentInput,
    ) -> AppResult<EnvironmentRecord> {
        let project = self.get_project_by_project_id(&input.project).await?;
        let existing = self
            .get_environment_by_project_id(project.id, &project.project_id, &input.slug)
            .await?;

        let baseline_environment_id = match input.baseline_slug.as_deref() {
            Some(baseline_slug) => Some(
                self.get_environment_by_project_id(project.id, &project.project_id, baseline_slug)
                    .await?
                    .id,
            ),
            None => existing.baseline_environment_id,
        };
        let git_branch = input.git_branch.or(existing.git_branch.clone());
        let git_commit_sha = input.git_commit_sha.or(existing.git_commit_sha.clone());
        validate_environment_git_metadata(&project, &existing.slug, git_commit_sha.as_deref())?;
        let adapter_type = input
            .adapter_type
            .as_deref()
            .unwrap_or(&existing.adapter_type)
            .to_string();
        let worker_queue = input
            .worker_queue
            .as_deref()
            .unwrap_or(&existing.worker_queue)
            .to_string();
        let profile_name = input
            .profile_name
            .as_deref()
            .unwrap_or(&existing.profile_name)
            .to_string();
        let target_name = input
            .target_name
            .as_deref()
            .unwrap_or(&existing.target_name)
            .to_string();
        let schema_name = input
            .schema_name
            .as_deref()
            .unwrap_or(&existing.schema_name)
            .to_string();
        let threads = input.threads.or(existing.threads);
        validate_environment_profile(
            &adapter_type,
            &schema_name,
            threads,
            input
                .profile_config
                .as_ref()
                .unwrap_or(&existing.profile_config),
            input
                .profile_secrets
                .as_ref()
                .unwrap_or(&existing.profile_secrets),
            true,
        )?;
        let status = input.status.unwrap_or(existing.status.clone());
        validate_environment_status(&status)?;

        sqlx::query(
            r#"
            UPDATE environments
            SET baseline_environment_id = $3,
                git_branch = $4,
                git_commit_sha = $5,
                pr_number = $6,
                status = $7,
                adapter_type = $8,
                worker_queue = $9,
                profile_name = $10,
                target_name = $11,
                schema_name = $12,
                threads = $13,
                profile_config = $14,
                profile_secrets = $15
            WHERE id = $1 AND project_id = $2
            "#,
        )
        .bind(existing.id)
        .bind(project.id)
        .bind(baseline_environment_id)
        .bind(git_branch.as_deref())
        .bind(git_commit_sha.as_deref())
        .bind(input.pr_number.or(existing.pr_number))
        .bind(&status)
        .bind(&adapter_type)
        .bind(&worker_queue)
        .bind(&profile_name)
        .bind(&target_name)
        .bind(&schema_name)
        .bind(threads)
        .bind(sqlx::types::Json(
            input
                .profile_config
                .as_ref()
                .unwrap_or(&existing.profile_config),
        ))
        .bind(sqlx::types::Json(
            input
                .profile_secrets
                .as_ref()
                .unwrap_or(&existing.profile_secrets),
        ))
        .execute(&self.pool)
        .await?;

        let environment = self.get_environment_by_id(existing.id).await?;
        self.record_environment_version(&environment, "updated")
            .await?;
        Ok(environment)
    }

    pub async fn release_environment(
        &self,
        input: EnvironmentReleaseInput,
    ) -> AppResult<EnvironmentRecord> {
        let project = self.get_project_by_project_id(&input.project).await?;
        let existing = self
            .get_environment_by_project_id(project.id, &project.project_id, &input.slug)
            .await?;

        validate_environment_git_metadata(&project, &existing.slug, Some(&input.git_commit_sha))?;

        if existing.git_commit_sha.as_deref() == Some(input.git_commit_sha.as_str()) {
            return Ok(existing);
        }

        sqlx::query(
            r#"
            UPDATE environments
            SET git_branch = $3,
                git_commit_sha = $4
            WHERE id = $1 AND project_id = $2
            "#,
        )
        .bind(existing.id)
        .bind(project.id)
        .bind(input.git_branch.as_deref())
        .bind(&input.git_commit_sha)
        .execute(&self.pool)
        .await?;

        let environment = self.get_environment_by_id(existing.id).await?;
        self.record_environment_version(&environment, "released")
            .await?;
        Ok(environment)
    }

    pub async fn list_environments(&self, project: &str) -> AppResult<Vec<EnvironmentRecord>> {
        let project = self.get_project_by_project_id(project).await?;
        let rows = sqlx::query(
            r#"
            SELECT
                e.id,
                e.project_id,
                p.project_id AS project_ref,
                p.project_name,
                e.slug,
                e.profile_name,
                e.target_name,
                e.baseline_environment_id,
                be.slug AS baseline_environment_slug,
                e.git_branch,
                e.git_commit_sha,
                e.pr_number,
                e.status,
                e.adapter_type,
                e.worker_queue,
                e.schema_name,
                e.threads,
                e.profile_config,
                e.profile_secrets,
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

    pub async fn list_environment_versions(
        &self,
        project: &str,
        slug: &str,
    ) -> AppResult<Vec<EnvironmentVersionRecord>> {
        let project = self.get_project_by_project_id(project).await?;
        let environment = self
            .get_environment_by_project_id(project.id, &project.project_id, slug)
            .await?;
        let rows = sqlx::query(
            r#"
            SELECT id, environment_id, project_id, recorded_at, reason, git_branch, git_commit_sha,
                   baseline_environment_id, metadata
            FROM environment_versions
            WHERE environment_id = $1
            ORDER BY id DESC
            "#,
        )
        .bind(environment.id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .iter()
            .map(environment_version_record_from_row)
            .collect())
    }

    pub async fn rollback_environment_to_version(
        &self,
        project: &str,
        slug: &str,
        version_id: i64,
    ) -> AppResult<EnvironmentRecord> {
        let project = self.get_project_by_project_id(project).await?;
        let existing = self
            .get_environment_by_project_id(project.id, &project.project_id, slug)
            .await?;
        let version = sqlx::query(
            r#"
            SELECT id, environment_id, project_id, recorded_at, reason, git_branch, git_commit_sha,
                   baseline_environment_id, metadata
            FROM environment_versions
            WHERE id = $1 AND environment_id = $2
            "#,
        )
        .bind(version_id)
        .bind(existing.id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| {
            AppError::EnvironmentVersionNotFound(
                project.project_id.clone(),
                slug.to_string(),
                version_id,
            )
        })?;
        let version = environment_version_record_from_row(&version);
        validate_environment_git_metadata(
            &project,
            &existing.slug,
            version.git_commit_sha.as_deref(),
        )?;

        sqlx::query(
            r#"
            UPDATE environments
            SET baseline_environment_id = $3,
                git_branch = $4,
                git_commit_sha = $5
            WHERE id = $1 AND project_id = $2
            "#,
        )
        .bind(existing.id)
        .bind(project.id)
        .bind(version.baseline_environment_id)
        .bind(version.git_branch.as_deref())
        .bind(version.git_commit_sha.as_deref())
        .execute(&self.pool)
        .await?;

        let environment = self.get_environment_by_id(existing.id).await?;
        self.record_environment_version(&environment, "rolled_back")
            .await?;
        Ok(environment)
    }

    pub async fn get_environment(
        &self,
        project: &str,
        environment_slug: &str,
    ) -> AppResult<EnvironmentRecord> {
        let project = self.get_project_by_project_id(project).await?;
        self.get_environment_by_project_id(project.id, &project.project_id, environment_slug)
            .await
    }

    pub(crate) async fn create_invocation(
        &self,
        input: CreateInvocationInput,
    ) -> AppResult<InvocationStatusResponse> {
        let row = sqlx::query(
            r#"
            INSERT INTO invocations (
                invocation_id, run_id, project_id, environment_id, command, execution_mode,
                worker_queue, status, execution_spec, promote_base_manifest, claim_deadline_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, 'running', $8, $9, $10)
            RETURNING invocation_id, execution_mode, worker_queue, status, exit_code, error,
                started_at, claimed_at, last_heartbeat_at, cancel_requested_at, completed_at,
                cancel_requested, claimed_by
            "#,
        )
        .bind(input.invocation_id)
        .bind(input.run_id)
        .bind(input.project_id)
        .bind(input.environment_id)
        .bind(&input.command)
        .bind(match input.execution_mode {
            InvocationExecutionModeApi::Server => "server",
            InvocationExecutionModeApi::Local => "local",
        })
        .bind(&input.worker_queue)
        .bind(input.execution_spec.as_ref().map(sqlx::types::Json))
        .bind(input.promote_base_manifest)
        .bind(input.claim_deadline_at)
        .fetch_one(&self.pool)
        .await?;
        Ok(invocation_status_from_row(&row))
    }

    pub(crate) async fn list_invocations(
        &self,
        filter: InvocationListApiRequest,
    ) -> AppResult<Vec<InvocationStatusResponse>> {
        let rows = sqlx::query(
            r#"
            SELECT invocation_id, execution_mode, worker_queue, status, exit_code, error,
                started_at, claimed_at, last_heartbeat_at, cancel_requested_at, completed_at,
                cancel_requested, claimed_by
            FROM invocations
            WHERE ($1::TEXT IS NULL OR status = $1)
              AND ($2::TEXT IS NULL OR execution_mode = $2)
              AND ($3::TEXT IS NULL OR worker_queue = $3)
              AND ($4::TEXT IS NULL OR claimed_by = $4)
              AND (
                $5::TEXT IS NULL
                OR ($5 = 'none' AND status <> 'canceled' AND cancel_requested = FALSE)
                OR ($5 = 'requested' AND status = 'running' AND cancel_requested = TRUE)
                OR ($5 = 'completed' AND status = 'canceled')
              )
            ORDER BY started_at DESC, invocation_id DESC
            LIMIT COALESCE($6, 100)
            "#,
        )
        .bind(filter.status.map(invocation_status_to_db))
        .bind(filter.execution_mode.map(|mode| match mode {
            InvocationExecutionModeApi::Server => "server",
            InvocationExecutionModeApi::Local => "local",
        }))
        .bind(filter.worker_queue)
        .bind(filter.claimed_by)
        .bind(filter.cancel_state.map(|state| match state {
            InvocationCancelStateApi::None => "none",
            InvocationCancelStateApi::Requested => "requested",
            InvocationCancelStateApi::Completed => "completed",
        }))
        .bind(filter.limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(invocation_status_from_row).collect())
    }

    pub(crate) async fn get_invocation_status(
        &self,
        invocation_id: Uuid,
    ) -> AppResult<InvocationStatusResponse> {
        let row = sqlx::query(
            r#"
            SELECT invocation_id, execution_mode, worker_queue, status, exit_code, error,
                started_at, claimed_at, last_heartbeat_at, cancel_requested_at, completed_at,
                cancel_requested, claimed_by
            FROM invocations
            WHERE invocation_id = $1
            "#,
        )
        .bind(invocation_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| {
            AppError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "invocation not found",
            ))
        })?;
        Ok(invocation_status_from_row(&row))
    }

    pub(crate) async fn list_workers(&self) -> AppResult<Vec<WorkerStatusResponse>> {
        let rows = sqlx::query(
            r#"
            SELECT execution_mode, worker_queue, claimed_by, claimed_at, last_heartbeat_at, cancel_requested, status, started_at
            FROM invocations
            WHERE status = 'running'
              AND claimed_by IS NOT NULL
            ORDER BY claimed_by ASC, execution_mode ASC, worker_queue ASC, started_at ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut grouped: BTreeMap<(String, String, String), Vec<InvocationReadModel>> =
            BTreeMap::new();
        for row in rows {
            let model = invocation_read_model_from_row(&row);
            let worker_id = model
                .claimed_by
                .clone()
                .expect("claimed invocation should have worker id");
            let mode = match model.execution_mode {
                InvocationExecutionModeApi::Server => "server".to_string(),
                InvocationExecutionModeApi::Local => "local".to_string(),
            };
            let queue = model.worker_queue.clone();
            grouped
                .entry((worker_id, mode, queue))
                .or_default()
                .push(model);
        }

        Ok(grouped
            .into_iter()
            .map(|((worker_id, _, worker_queue), models)| {
                let execution_mode = models[0].execution_mode;
                let last_heartbeat_at = models.iter().filter_map(|m| m.last_heartbeat_at).max();
                let health = if models.iter().any(|m| {
                    matches!(
                        compute_worker_health_from_model(m),
                        InvocationWorkerHealthApi::Claimed
                    )
                }) {
                    InvocationWorkerHealthApi::Claimed
                } else {
                    InvocationWorkerHealthApi::Stale
                };
                WorkerStatusResponse {
                    worker_id,
                    execution_mode,
                    worker_queue,
                    claimed_invocation_count: models.len() as i64,
                    last_heartbeat_at,
                    health,
                }
            })
            .collect())
    }

    pub(crate) async fn list_queues(&self) -> AppResult<Vec<QueueStatusResponse>> {
        let rows = sqlx::query(
            r#"
            SELECT execution_mode, worker_queue, claimed_by, claimed_at, last_heartbeat_at, cancel_requested, status, started_at
            FROM invocations
            WHERE status = 'running'
            ORDER BY execution_mode ASC, worker_queue ASC, started_at ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut grouped: BTreeMap<(String, String), Vec<InvocationReadModel>> = BTreeMap::new();
        for row in rows {
            let model = invocation_read_model_from_row(&row);
            let mode = match model.execution_mode {
                InvocationExecutionModeApi::Server => "server".to_string(),
                InvocationExecutionModeApi::Local => "local".to_string(),
            };
            let queue = model.worker_queue.clone();
            grouped.entry((mode, queue)).or_default().push(model);
        }

        Ok(grouped
            .into_iter()
            .map(|((_, worker_queue), models)| {
                let execution_mode = models[0].execution_mode;
                let pending_count = models.iter().filter(|m| m.claimed_by.is_none()).count() as i64;
                let claimed_count = models.iter().filter(|m| m.claimed_by.is_some()).count() as i64;
                let stale_claim_count = models
                    .iter()
                    .filter(|m| {
                        m.claimed_by.is_some()
                            && matches!(
                                compute_worker_health_from_model(m),
                                InvocationWorkerHealthApi::Stale
                            )
                    })
                    .count() as i64;
                let oldest_pending_at = models
                    .iter()
                    .filter(|m| m.claimed_by.is_none())
                    .map(|m| m.started_at)
                    .min();
                QueueStatusResponse {
                    worker_queue,
                    execution_mode,
                    pending_count,
                    claimed_count,
                    stale_claim_count,
                    oldest_pending_at,
                }
            })
            .collect())
    }

    pub(crate) async fn claim_next_invocation(
        &self,
        worker_id: &str,
        execution_mode: Option<InvocationExecutionModeApi>,
        worker_queue: Option<&str>,
    ) -> AppResult<Option<InvocationClaimResponse>> {
        let mut tx = self.pool.begin().await?;
        let lease_token = Uuid::new_v4();
        let row = sqlx::query(
            r#"
            WITH next_invocation AS (
                SELECT invocation_id
                FROM invocations
                WHERE status = 'running'
                  AND execution_spec IS NOT NULL
                  AND ($1::TEXT IS NULL OR execution_mode = $1)
                  AND ($2::TEXT IS NULL OR worker_queue = $2)
                  AND (claim_deadline_at IS NULL OR claim_deadline_at >= NOW())
                  AND claimed_by IS NULL
                ORDER BY started_at ASC, invocation_id ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE invocations inv
            SET claimed_by = $3,
                lease_token = $4,
                claimed_at = NOW(),
                last_heartbeat_at = NOW()
            FROM next_invocation
            WHERE inv.invocation_id = next_invocation.invocation_id
            RETURNING inv.invocation_id, inv.lease_token, inv.execution_mode, inv.execution_spec
            "#,
        )
        .bind(execution_mode.map(|mode| match mode {
            InvocationExecutionModeApi::Server => "server",
            InvocationExecutionModeApi::Local => "local",
        }))
        .bind(worker_queue)
        .bind(worker_id)
        .bind(lease_token)
        .fetch_optional(&mut *tx)
        .await?;
        tx.commit().await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let execution_spec: sqlx::types::Json<InvocationExecutionSpecApi> =
            row.get("execution_spec");
        Ok(Some(InvocationClaimResponse {
            invocation_id: row.get("invocation_id"),
            worker_id: worker_id.to_string(),
            lease_token: row.get("lease_token"),
            execution_mode: execution_mode_from_db(&row.get::<String, _>("execution_mode")),
            execution_spec: execution_spec.0,
        }))
    }

    pub(crate) async fn heartbeat_invocation(
        &self,
        invocation_id: Uuid,
        worker_id: &str,
        lease_token: Uuid,
    ) -> AppResult<bool> {
        let row = sqlx::query(
            r#"
            UPDATE invocations
            SET last_heartbeat_at = NOW()
            WHERE invocation_id = $1
              AND claimed_by = $2
              AND lease_token = $3
              AND status = 'running'
            RETURNING cancel_requested
            "#,
        )
        .bind(invocation_id)
        .bind(worker_id)
        .bind(lease_token)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Err(AppError::Io(std::io::Error::other(
                "invocation is owned by a different worker or is not running",
            )));
        };
        Ok(row.get("cancel_requested"))
    }

    pub(crate) async fn request_cancel_invocation(
        &self,
        invocation_id: Uuid,
    ) -> AppResult<Option<InvocationCancellationRecord>> {
        let row = sqlx::query(
            r#"
            UPDATE invocations
            SET cancel_requested = CASE
                    WHEN status = 'running' THEN TRUE
                    ELSE cancel_requested
                END,
                cancel_requested_at = CASE
                    WHEN status = 'running' THEN COALESCE(cancel_requested_at, NOW())
                    ELSE cancel_requested_at
                END,
                status = CASE
                    WHEN status = 'running' AND claimed_by IS NULL THEN 'canceled'
                    ELSE status
                END,
                exit_code = CASE
                    WHEN status = 'running' AND claimed_by IS NULL THEN 130
                    ELSE exit_code
                END,
                error = CASE
                    WHEN status = 'running' AND claimed_by IS NULL THEN 'invocation canceled'
                    ELSE error
                END,
                completed_at = CASE
                    WHEN status = 'running' AND claimed_by IS NULL THEN NOW()
                    ELSE completed_at
                END
            WHERE invocation_id = $1
            RETURNING status, exit_code, error, claimed_by
            "#,
        )
        .bind(invocation_id)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Err(AppError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "invocation not found",
            )));
        };
        if row.get::<String, _>("status") == "canceled"
            && row.get::<Option<String>, _>("claimed_by").is_none()
        {
            return Ok(Some(InvocationCancellationRecord {
                invocation_id,
                status: InvocationLifecycleStatus::Canceled,
                exit_code: row.get("exit_code"),
                error: row.get("error"),
            }));
        }
        Ok(None)
    }

    pub(crate) async fn reconcile_timed_out_invocations(
        &self,
        local_heartbeat_timeout: std::time::Duration,
        server_heartbeat_timeout: std::time::Duration,
    ) -> AppResult<Vec<TimedOutInvocationRecord>> {
        let mut tx = self.pool.begin().await?;
        let local_stale_at = Utc::now()
            - chrono::Duration::from_std(local_heartbeat_timeout)
                .unwrap_or_else(|_| chrono::Duration::seconds(15));
        let server_stale_at = Utc::now()
            - chrono::Duration::from_std(server_heartbeat_timeout)
                .unwrap_or_else(|_| chrono::Duration::seconds(60));
        let mut timed_out = Vec::new();

        let unclaimed_rows = sqlx::query(
            r#"
            UPDATE invocations
            SET status = 'failed',
                exit_code = 1,
                error = 'worker did not claim invocation before startup deadline',
                completed_at = NOW(),
                lease_token = NULL
            WHERE status = 'running'
              AND claimed_by IS NULL
              AND claim_deadline_at IS NOT NULL
              AND claim_deadline_at < NOW()
            RETURNING invocation_id, status, exit_code, error
            "#,
        )
        .fetch_all(&mut *tx)
        .await?;
        timed_out.extend(
            unclaimed_rows
                .into_iter()
                .map(timed_out_invocation_from_row),
        );

        let claimed_rows = sqlx::query(
            r#"
            UPDATE invocations
            SET status = 'failed',
                exit_code = 1,
                error = 'worker heartbeat timed out',
                completed_at = NOW(),
                lease_token = NULL
            WHERE status = 'running'
              AND claimed_by IS NOT NULL
              AND (
                (execution_mode = 'local' AND COALESCE(last_heartbeat_at, claimed_at, started_at) < $1)
                OR
                (execution_mode = 'server' AND COALESCE(last_heartbeat_at, claimed_at, started_at) < $2)
              )
            RETURNING invocation_id, status, exit_code, error
            "#,
        )
        .bind(local_stale_at)
        .bind(server_stale_at)
        .fetch_all(&mut *tx)
        .await?;
        timed_out.extend(claimed_rows.into_iter().map(timed_out_invocation_from_row));

        tx.commit().await?;
        Ok(timed_out)
    }

    pub(crate) async fn cleanup_terminal_invocations_older_than(
        &self,
        cutoff: chrono::DateTime<Utc>,
    ) -> AppResult<u64> {
        let result = sqlx::query(
            r#"
            DELETE FROM invocations
            WHERE status IN ('succeeded', 'failed', 'canceled')
              AND completed_at IS NOT NULL
              AND completed_at < $1
            "#,
        )
        .bind(cutoff)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    pub(crate) async fn get_invocation_persistence(
        &self,
        invocation_id: Uuid,
        worker_id: Option<&str>,
        lease_token: Option<Uuid>,
    ) -> AppResult<InvocationPersistenceRecord> {
        let row = sqlx::query(
            r#"
            SELECT run_id, project_id, environment_id, command, promote_base_manifest
            FROM invocations
            WHERE invocation_id = $1
              AND ($2::TEXT IS NULL OR claimed_by = $2)
              AND ($3::UUID IS NULL OR lease_token = $3)
            "#,
        )
        .bind(invocation_id)
        .bind(worker_id)
        .bind(lease_token)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| {
            AppError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "invocation not found",
            ))
        })?;
        Ok(InvocationPersistenceRecord {
            run_id: row.get("run_id"),
            project_id: row.get("project_id"),
            environment_id: row.get("environment_id"),
            command: row.get("command"),
            promote_base_manifest: row.get("promote_base_manifest"),
        })
    }

    pub(crate) async fn append_invocation_event(
        &self,
        invocation_id: Uuid,
        event: &InvocationEvent,
    ) -> AppResult<u64> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            r#"
            UPDATE invocations
            SET next_event_sequence = next_event_sequence + 1
            WHERE invocation_id = $1
            RETURNING next_event_sequence
            "#,
        )
        .bind(invocation_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| {
            AppError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "invocation not found",
            ))
        })?;
        let sequence_no: i64 = row.get("next_event_sequence");
        sqlx::query(
            r#"
            INSERT INTO invocation_events (
                invocation_id, sequence_no, occurred_at, event_type, payload
            )
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(invocation_id)
        .bind(sequence_no)
        .bind(event.timestamp)
        .bind(&event.event_type)
        .bind(sqlx::types::Json(event))
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(sequence_no as u64)
    }

    pub(crate) async fn load_invocation_events_since(
        &self,
        invocation_id: Uuid,
        after_sequence: u64,
    ) -> AppResult<Vec<(u64, InvocationEvent)>> {
        let rows = sqlx::query(
            r#"
            SELECT sequence_no, payload
            FROM invocation_events
            WHERE invocation_id = $1
              AND sequence_no > $2
            ORDER BY sequence_no ASC
            "#,
        )
        .bind(invocation_id)
        .bind(after_sequence as i64)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                let payload: sqlx::types::Json<InvocationEvent> = row.get("payload");
                (row.get::<i64, _>("sequence_no") as u64, payload.0)
            })
            .collect())
    }

    pub(crate) async fn complete_invocation(
        &self,
        invocation_id: Uuid,
        worker_id: &str,
        lease_token: Uuid,
        completion: &crate::execution::ExecutionCompletion,
    ) -> AppResult<()> {
        let persistence = self
            .get_invocation_persistence(invocation_id, Some(worker_id), Some(lease_token))
            .await?;
        let mut tx = self.pool.begin().await?;

        if let Some(run_id) = persistence.run_id {
            let manifest = completion.manifest.clone().map(ManifestSnapshot::from_raw);
            self.finalize_run_in_tx(
                &mut tx,
                RunFinalization {
                    run_id,
                    project_id: persistence.project_id,
                    environment_id: persistence.environment_id,
                    subcommand: &persistence.command,
                    dbt_version: completion.dbt_version.as_deref(),
                    exit_code: completion.exit_code,
                    terminal_status: match completion.status {
                        InvocationLifecycleStatus::Succeeded => "success",
                        InvocationLifecycleStatus::Canceled => "canceled",
                        InvocationLifecycleStatus::Failed => "failed",
                        InvocationLifecycleStatus::Running => "running",
                    },
                    manifest: manifest.as_ref(),
                    promote_base_manifest: persistence.promote_base_manifest
                        && matches!(completion.status, InvocationLifecycleStatus::Succeeded),
                },
            )
            .await?;
        }

        if persistence.command == "release"
            && matches!(completion.status, InvocationLifecycleStatus::Succeeded)
        {
            self.apply_release_completion_in_tx(
                &mut tx,
                persistence.project_id,
                persistence.environment_id,
                completion.result.as_ref(),
            )
            .await?;
        }

        sqlx::query(
            r#"
            UPDATE invocations
            SET status = $3,
                exit_code = $4,
                error = $5,
                completed_at = NOW(),
                lease_token = NULL
            WHERE invocation_id = $1
              AND claimed_by = $2
              AND lease_token = $6
            "#,
        )
        .bind(invocation_id)
        .bind(worker_id)
        .bind(invocation_status_to_db(completion.status.clone()))
        .bind(completion.exit_code)
        .bind(completion.error.as_deref())
        .bind(lease_token)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn apply_release_completion_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        project_id: i64,
        environment_id: i64,
        result: Option<&Value>,
    ) -> AppResult<()> {
        let result = result.ok_or_else(|| {
            AppError::Io(std::io::Error::other(
                "release validation completed without resolved commit metadata",
            ))
        })?;
        let resolved_commit_sha = result
            .get("resolved_commit_sha")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                AppError::Io(std::io::Error::other(
                    "release validation missing resolved_commit_sha",
                ))
            })?;
        let git_branch = result.get("git_branch").and_then(Value::as_str);

        let existing = self.get_environment_by_id_in_tx(tx, environment_id).await?;
        if existing.git_commit_sha.as_deref() == Some(resolved_commit_sha) {
            return Ok(());
        }

        sqlx::query(
            r#"
            UPDATE environments
            SET git_branch = $3,
                git_commit_sha = $4
            WHERE id = $1 AND project_id = $2
            "#,
        )
        .bind(environment_id)
        .bind(project_id)
        .bind(git_branch)
        .bind(resolved_commit_sha)
        .execute(&mut **tx)
        .await?;

        let environment = self.get_environment_by_id_in_tx(tx, environment_id).await?;
        self.record_environment_version_in_tx(tx, &environment, "released")
            .await?;
        Ok(())
    }

    pub(crate) async fn insert_run_started(&self, run: RunStart<'_>) -> AppResult<()> {
        sqlx::query(
            r#"
            INSERT INTO runs (
                run_id, project_id, environment_id, command, args, is_full_graph_run,
                execution_mode, git_branch, git_commit_sha, git_repo_url, project_root, project_name, project_ref
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            "#,
        )
        .bind(run.run_id)
        .bind(run.project.id)
        .bind(run.environment.id)
        .bind(run.subcommand)
        .bind(run.args_json)
        .bind(run.is_full_graph_run)
        .bind(match run.execution_mode {
            ExecutionMode::Server => "server",
            ExecutionMode::Local => "local",
        })
        .bind(run.git_state.branch.as_deref())
        .bind(run.git_state.commit_sha.as_deref())
        .bind(
            run.git_state
                .repo_url
                .as_deref()
                .or(run.project.git_repo_url.as_deref()),
        )
        .bind(run.project.project_root.as_deref())
        .bind(&run.project.project_name)
        .bind(&run.project.project_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn seed_environment_from_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        project: &ProjectRecord,
        target: &EnvironmentRecord,
        source: &EnvironmentRecord,
        seed_type: &str,
    ) -> AppResult<()> {
        sqlx::query(
            "DELETE FROM promoted_manifest_nodes WHERE project_id = $1 AND environment_id = $2",
        )
        .bind(project.id)
        .bind(target.id)
        .execute(&mut **tx)
        .await?;
        sqlx::query(
            "DELETE FROM promoted_manifest_meta WHERE project_id = $1 AND environment_id = $2",
        )
        .bind(project.id)
        .bind(target.id)
        .execute(&mut **tx)
        .await?;
        sqlx::query("DELETE FROM current_node_state WHERE project_id = $1 AND environment_id = $2")
            .bind(project.id)
            .bind(target.id)
            .execute(&mut **tx)
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
        .execute(&mut **tx)
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
        .execute(&mut **tx)
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
        .execute(&mut **tx)
        .await?;

        let source_run_id: Option<Uuid> = sqlx::query_scalar(
            "SELECT source_run_id FROM promoted_manifest_meta WHERE project_id = $1 AND environment_id = $2",
        )
        .bind(project.id)
        .bind(source.id)
        .fetch_optional(&mut **tx)
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
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    pub(crate) async fn upsert_local_environment(
        &self,
        input: LocalEnvironmentUpsertInput<'_>,
    ) -> AppResult<EnvironmentRecord> {
        let LocalEnvironmentUpsertInput {
            project,
            profile_name,
            target_name,
            adapter_type,
            schema_name,
            threads,
            profile_config,
            profile_secrets,
        } = input;
        validate_environment_profile(
            adapter_type,
            schema_name,
            threads,
            profile_config,
            profile_secrets,
            false,
        )?;
        let slug = format!("{profile_name}__{target_name}");
        let row = sqlx::query(
            r#"
            INSERT INTO environments (
                project_id, slug, profile_name, target_name, status, adapter_type,
                worker_queue, schema_name, threads, profile_config, profile_secrets
            )
            VALUES ($1, $2, $3, $4, 'active', $5, 'generic', $6, $7, $8, $9)
            ON CONFLICT (project_id, slug) DO UPDATE
            SET slug = EXCLUDED.slug,
                profile_name = EXCLUDED.profile_name,
                target_name = EXCLUDED.target_name,
                adapter_type = EXCLUDED.adapter_type,
                schema_name = EXCLUDED.schema_name,
                threads = EXCLUDED.threads,
                profile_config = EXCLUDED.profile_config,
                profile_secrets = EXCLUDED.profile_secrets
            RETURNING id
            "#,
        )
        .bind(project.id)
        .bind(&slug)
        .bind(profile_name)
        .bind(target_name)
        .bind(adapter_type)
        .bind(schema_name)
        .bind(threads)
        .bind(sqlx::types::Json(profile_config))
        .bind(sqlx::types::Json(profile_secrets))
        .fetch_one(&self.pool)
        .await?;
        let environment_id: i64 = row.get("id");
        self.get_environment_by_id(environment_id).await
    }

    pub(crate) async fn finalize_run(&self, finalization: RunFinalization<'_>) -> AppResult<()> {
        let mut tx = self.pool.begin().await?;
        self.finalize_run_in_tx(&mut tx, finalization).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn finalize_run_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        finalization: RunFinalization<'_>,
    ) -> AppResult<()> {
        self.mark_run_finished_in_tx(
            tx,
            finalization.run_id,
            finalization.dbt_version,
            finalization.exit_code,
            finalization.terminal_status,
        )
        .await?;

        if let Some(manifest) = finalization.manifest {
            self.persist_manifest_in_tx(tx, finalization.run_id, manifest)
                .await?;
            if should_promote_manifest(finalization.subcommand) {
                self.promote_manifest_state_in_tx(
                    tx,
                    finalization.run_id,
                    finalization.project_id,
                    finalization.environment_id,
                    finalization.promote_base_manifest,
                )
                .await?;
            }
        }

        self.rebuild_current_state_up_to_in_tx(
            tx,
            finalization.project_id,
            finalization.environment_id,
            None,
        )
        .await?;
        Ok(())
    }

    async fn record_environment_version(
        &self,
        environment: &EnvironmentRecord,
        reason: &str,
    ) -> AppResult<()> {
        let mut tx = self.pool.begin().await?;
        self.record_environment_version_in_tx(&mut tx, environment, reason)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn record_environment_version_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        environment: &EnvironmentRecord,
        reason: &str,
    ) -> AppResult<()> {
        sqlx::query(
            r#"
            INSERT INTO environment_versions (
                environment_id, project_id, reason, git_branch, git_commit_sha,
                baseline_environment_id, metadata
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(environment.id)
        .bind(environment.project_id)
        .bind(reason)
        .bind(environment.git_branch.as_deref())
        .bind(environment.git_commit_sha.as_deref())
        .bind(environment.baseline_environment_id)
        .bind(sqlx::types::Json(serde_json::json!({
            "environment_slug": environment.slug,
            "target_name": environment.target_name,
            "environment_metadata": environment.metadata,
        })))
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    pub(crate) async fn mark_run_finished(
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

    async fn get_environment_by_project_id(
        &self,
        project_id: i64,
        project_ref: &str,
        environment_slug: &str,
    ) -> AppResult<EnvironmentRecord> {
        let row = sqlx::query(
            r#"
            SELECT
                e.id,
                e.project_id,
                p.project_id AS project_ref,
                p.project_name,
                e.slug,
                e.profile_name,
                e.target_name,
                e.baseline_environment_id,
                be.slug AS baseline_environment_slug,
                e.git_branch,
                e.git_commit_sha,
                e.pr_number,
                e.status,
                e.adapter_type,
                e.worker_queue,
                e.schema_name,
                e.threads,
                e.profile_config,
                e.profile_secrets,
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
            AppError::EnvironmentNotFound(project_ref.to_string(), environment_slug.to_string())
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
                p.project_name,
                e.slug,
                e.profile_name,
                e.target_name,
                e.baseline_environment_id,
                be.slug AS baseline_environment_slug,
                e.git_branch,
                e.git_commit_sha,
                e.pr_number,
                e.status,
                e.adapter_type,
                e.worker_queue,
                e.schema_name,
                e.threads,
                e.profile_config,
                e.profile_secrets,
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

    async fn get_environment_by_id_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        environment_id: i64,
    ) -> AppResult<EnvironmentRecord> {
        let row = sqlx::query(
            r#"
            SELECT
                e.id,
                e.project_id,
                p.project_id AS project_ref,
                p.project_name,
                e.slug,
                e.profile_name,
                e.target_name,
                e.baseline_environment_id,
                be.slug AS baseline_environment_slug,
                e.git_branch,
                e.git_commit_sha,
                e.pr_number,
                e.status,
                e.adapter_type,
                e.worker_queue,
                e.schema_name,
                e.threads,
                e.profile_config,
                e.profile_secrets,
                e.metadata
            FROM environments e
            JOIN projects p ON p.id = e.project_id
            LEFT JOIN environments be ON be.id = e.baseline_environment_id
            WHERE e.id = $1
            "#,
        )
        .bind(environment_id)
        .fetch_one(&mut **tx)
        .await?;
        Ok(environment_record_from_row(&row))
    }

    pub(crate) async fn persist_log_event(
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
            let promote_manifest_state = node.status.as_deref().is_some_and(is_promotable_status);
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

    pub(crate) async fn persist_raw_line(
        &self,
        run_id: Uuid,
        sequence_no: i64,
        line: &str,
    ) -> AppResult<()> {
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

    pub(crate) async fn load_reconstructed_manifest(
        &self,
        project_id: i64,
        environment_id: i64,
    ) -> AppResult<Option<ReconstructedManifest>> {
        let base_row = sqlx::query(
            r#"
            SELECT
                pmm.project_id,
                pmm.environment_id,
                pmm.base_manifest
            FROM promoted_manifest_meta pmm
            WHERE pmm.project_id = $1
              AND pmm.environment_id = $2
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
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
}

impl Db {
    async fn migration_versions(&self) -> AppResult<BTreeSet<i64>> {
        Ok(self
            .migration_rows()
            .await?
            .into_iter()
            .map(|migration| migration.version)
            .collect())
    }

    async fn migration_rows(&self) -> AppResult<Vec<AppliedMigration>> {
        let rows =
            sqlx::query("SELECT version, description FROM _sqlx_migrations ORDER BY version")
                .fetch_all(&self.pool)
                .await;
        match rows {
            Ok(rows) => Ok(rows
                .into_iter()
                .map(|row| AppliedMigration {
                    version: row.get("version"),
                    description: row.get("description"),
                })
                .collect()),
            Err(sqlx::Error::Database(db_err)) if db_err.code().as_deref() == Some("42P01") => {
                Ok(Vec::new())
            }
            Err(err) => Err(AppError::Sqlx(err)),
        }
    }
}

pub(crate) fn append_invocation_id(mut args: Vec<OsString>, run_id: Uuid) -> Vec<OsString> {
    args.push("--invocation-id".into());
    args.push(run_id.to_string().into());
    args
}

pub(crate) fn append_state_dir(
    mut args: Vec<OsString>,
    reconstructed_manifest: Option<&ReconstructedManifest>,
) -> Vec<OsString> {
    if let Some(reconstructed_manifest) = reconstructed_manifest {
        args.push("--state".into());
        args.push(
            reconstructed_manifest
                .temp_dir
                .path()
                .as_os_str()
                .to_os_string(),
        );
    }
    args
}

pub(crate) fn append_profiles_dir(
    mut args: Vec<OsString>,
    generated_profiles: &GeneratedProfiles,
) -> Vec<OsString> {
    args.push("--profiles-dir".into());
    args.push(
        generated_profiles
            .temp_dir
            .path()
            .as_os_str()
            .to_os_string(),
    );
    args
}

pub(crate) fn spawn_dbt_child(
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
    if value.is_empty() { None } else { Some(value) }
}

fn should_promote_manifest(subcommand: &str) -> bool {
    matches!(subcommand, "run" | "build")
}

fn is_promotable_status(status: &str) -> bool {
    matches!(status, "success" | "pass" | "created")
}

fn validate_project_mode(mode: &str) -> AppResult<()> {
    if matches!(mode, "local" | "remote") {
        Ok(())
    } else {
        Err(AppError::InvalidProjectMode(mode.to_string()))
    }
}

fn validate_project_input(mode: &str, project_root: Option<&str>) -> AppResult<()> {
    validate_project_mode(mode)?;
    if mode == "remote" {
        let project_root =
            project_root.ok_or_else(|| AppError::InvalidRemoteProjectRoot(String::new()))?;
        validate_remote_project_root_value(project_root)?;
    }
    Ok(())
}

fn validate_remote_project_root_value(project_root: &str) -> AppResult<()> {
    let path = Path::new(project_root);
    if path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        Err(AppError::InvalidRemoteProjectRoot(project_root.to_string()))
    } else {
        Ok(())
    }
}

fn validate_environment_git_metadata(
    project: &ProjectRecord,
    environment_slug: &str,
    git_commit_sha: Option<&str>,
) -> AppResult<()> {
    validate_project_mode(&project.mode)?;
    if project.mode != "remote" {
        return Ok(());
    }
    let git_commit_sha = git_commit_sha.ok_or_else(|| {
        AppError::RemoteProjectEnvironmentRequiresSha(
            project.project_id.clone(),
            environment_slug.to_string(),
        )
    })?;
    if is_valid_git_commit_sha(git_commit_sha) {
        Ok(())
    } else {
        Err(AppError::InvalidRemoteProjectCommitSha(
            project.project_id.clone(),
            environment_slug.to_string(),
            git_commit_sha.to_string(),
        ))
    }
}

fn is_valid_git_commit_sha(value: &str) -> bool {
    let trimmed = value.trim();
    (7..=64).contains(&trimmed.len()) && trimmed.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn validate_environment_status(status: &str) -> AppResult<()> {
    if matches!(status, "active" | "archived" | "failed" | "deleting") {
        Ok(())
    } else {
        Err(AppError::InvalidEnvironmentStatus(status.to_string()))
    }
}

fn project_record_from_row(row: &sqlx::postgres::PgRow) -> ProjectRecord {
    let metadata: sqlx::types::Json<Value> = row.get("metadata");
    ProjectRecord {
        id: row.get("id"),
        project_id: row.get("project_id"),
        project_name: row.get("project_name"),
        mode: row.get("mode"),
        git_repo_url: row.get("git_repo_url"),
        default_branch: row.get("default_branch"),
        project_root: row.get("project_root"),
        metadata: metadata.0,
    }
}

fn environment_record_from_row(row: &sqlx::postgres::PgRow) -> EnvironmentRecord {
    let metadata: sqlx::types::Json<Value> = row.get("metadata");
    let profile_config: sqlx::types::Json<Value> = row.get("profile_config");
    let profile_secrets: sqlx::types::Json<Value> = row.get("profile_secrets");
    EnvironmentRecord {
        id: row.get("id"),
        project_id: row.get("project_id"),
        project_ref: row.get("project_ref"),
        project_name: row.get("project_name"),
        slug: row.get("slug"),
        profile_name: row.get("profile_name"),
        target_name: row.get("target_name"),
        baseline_environment_id: row.get("baseline_environment_id"),
        baseline_environment_slug: row.get("baseline_environment_slug"),
        git_branch: row.get("git_branch"),
        git_commit_sha: row.get("git_commit_sha"),
        pr_number: row.get("pr_number"),
        status: row.get("status"),
        adapter_type: row.get("adapter_type"),
        worker_queue: row.get("worker_queue"),
        schema_name: row.get("schema_name"),
        threads: row.get("threads"),
        profile_config: profile_config.0,
        profile_secrets: profile_secrets.0,
        metadata: metadata.0,
    }
}

fn environment_version_record_from_row(row: &sqlx::postgres::PgRow) -> EnvironmentVersionRecord {
    EnvironmentVersionRecord {
        id: row.get("id"),
        environment_id: row.get("environment_id"),
        project_id: row.get("project_id"),
        recorded_at: row.get("recorded_at"),
        reason: row.get("reason"),
        git_branch: row.get("git_branch"),
        git_commit_sha: row.get("git_commit_sha"),
        baseline_environment_id: row.get("baseline_environment_id"),
        metadata: row.get::<sqlx::types::Json<Value>, _>("metadata").0,
    }
}

fn invocation_status_from_row(row: &sqlx::postgres::PgRow) -> InvocationStatusResponse {
    let mut status = InvocationStatusResponse {
        invocation_id: row.get("invocation_id"),
        execution_mode: execution_mode_from_db(&row.get::<String, _>("execution_mode")),
        worker_queue: row.get("worker_queue"),
        worker_health: InvocationWorkerHealthApi::Unclaimed,
        cancel_state: InvocationCancelStateApi::None,
        status: invocation_status_from_db(&row.get::<String, _>("status")),
        exit_code: row.get("exit_code"),
        error: row.get("error"),
        started_at: row.get("started_at"),
        claimed_at: row.get("claimed_at"),
        last_heartbeat_at: row.get("last_heartbeat_at"),
        cancel_requested_at: row.get("cancel_requested_at"),
        completed_at: row.get("completed_at"),
        cancel_requested: row.get("cancel_requested"),
        claimed_by: row.get("claimed_by"),
    };
    status.worker_health = compute_worker_health(&status);
    status.cancel_state = compute_cancel_state(&status);
    status
}

fn invocation_read_model_from_row(row: &sqlx::postgres::PgRow) -> InvocationReadModel {
    InvocationReadModel {
        execution_mode: execution_mode_from_db(&row.get::<String, _>("execution_mode")),
        worker_queue: row.get("worker_queue"),
        status: invocation_status_from_db(&row.get::<String, _>("status")),
        started_at: row.get("started_at"),
        claimed_at: row.get("claimed_at"),
        last_heartbeat_at: row.get("last_heartbeat_at"),
        claimed_by: row.get("claimed_by"),
    }
}

fn timed_out_invocation_from_row(row: sqlx::postgres::PgRow) -> TimedOutInvocationRecord {
    TimedOutInvocationRecord {
        invocation_id: row.get("invocation_id"),
        status: invocation_status_from_db(&row.get::<String, _>("status")),
        exit_code: row.get("exit_code"),
        error: row.get("error"),
    }
}

fn execution_mode_from_db(value: &str) -> InvocationExecutionModeApi {
    match value {
        "local" => InvocationExecutionModeApi::Local,
        _ => InvocationExecutionModeApi::Server,
    }
}

fn invocation_status_from_db(value: &str) -> InvocationLifecycleStatus {
    match value {
        "succeeded" => InvocationLifecycleStatus::Succeeded,
        "failed" => InvocationLifecycleStatus::Failed,
        "canceled" => InvocationLifecycleStatus::Canceled,
        _ => InvocationLifecycleStatus::Running,
    }
}

fn invocation_status_to_db(status: InvocationLifecycleStatus) -> &'static str {
    match status {
        InvocationLifecycleStatus::Running => "running",
        InvocationLifecycleStatus::Succeeded => "succeeded",
        InvocationLifecycleStatus::Failed => "failed",
        InvocationLifecycleStatus::Canceled => "canceled",
    }
}

fn compute_worker_health(status: &InvocationStatusResponse) -> InvocationWorkerHealthApi {
    compute_worker_health_from_model(&InvocationReadModel {
        execution_mode: status.execution_mode,
        worker_queue: status.worker_queue.clone(),
        status: status.status.clone(),
        started_at: status.started_at,
        claimed_at: status.claimed_at,
        last_heartbeat_at: status.last_heartbeat_at,
        claimed_by: status.claimed_by.clone(),
    })
}

fn compute_worker_health_from_model(status: &InvocationReadModel) -> InvocationWorkerHealthApi {
    if !matches!(status.status, InvocationLifecycleStatus::Running) {
        return InvocationWorkerHealthApi::Completed;
    }
    let stale_after = chrono::Duration::from_std(heartbeat_stale_timeout(status.execution_mode))
        .unwrap_or_else(|_| chrono::Duration::seconds(15));
    match (
        status.claimed_at,
        status.last_heartbeat_at.as_ref(),
        status.claimed_by.as_ref(),
    ) {
        (_, _, None) => InvocationWorkerHealthApi::Unclaimed,
        (_, Some(last_heartbeat), Some(_)) if Utc::now() - *last_heartbeat > stale_after => {
            InvocationWorkerHealthApi::Stale
        }
        (Some(claimed_at), None, Some(_)) if Utc::now() - claimed_at > stale_after => {
            InvocationWorkerHealthApi::Stale
        }
        (_, _, Some(_)) => InvocationWorkerHealthApi::Claimed,
    }
}

fn compute_cancel_state(status: &InvocationStatusResponse) -> InvocationCancelStateApi {
    if matches!(status.status, InvocationLifecycleStatus::Canceled) {
        InvocationCancelStateApi::Completed
    } else if status.cancel_requested {
        InvocationCancelStateApi::Requested
    } else {
        InvocationCancelStateApi::None
    }
}

pub(crate) fn read_dbt_project_name(project_dir: &Path) -> String {
    read_dbt_project_yaml(project_dir)
        .ok()
        .and_then(|yaml| {
            yaml.get("name")
                .and_then(serde_yaml::Value::as_str)
                .map(ToString::to_string)
        })
        .unwrap_or_else(|| {
            project_dir
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned()
        })
}

fn read_dbt_project_yaml(project_dir: &Path) -> AppResult<serde_yaml::Value> {
    let path = project_dir.join("dbt_project.yml");
    if !path.is_file() {
        return Err(AppError::NotDbtProjectRoot);
    }
    let content = std::fs::read_to_string(path)?;
    Ok(serde_yaml::from_str(&content)?)
}

pub(crate) fn git_repo_root(current_dir: &Path) -> AppResult<std::path::PathBuf> {
    let output = run_git(["rev-parse", "--show-toplevel"], current_dir)?;
    Ok(output.into())
}

fn git_remote_origin_url(repo_root: &Path) -> AppResult<String> {
    run_git(["config", "--get", "remote.origin.url"], repo_root)
        .map_err(|_| AppError::GitRemoteNotFound)
}

pub(crate) fn read_git_state(project_dir: &Path) -> GitState {
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

pub(crate) fn build_generated_profiles(
    _project_dir: &Path,
    environment: &EnvironmentRecord,
) -> AppResult<GeneratedProfiles> {
    let resolved = resolve_runtime_profile(
        &environment.profile_name,
        &environment.target_name,
        &EnvironmentProfileRecord {
            adapter_type: environment.adapter_type.clone(),
            schema_name: environment.schema_name.clone(),
            threads: environment.threads,
            profile_config: environment.profile_config.clone(),
            profile_secrets: environment.profile_secrets.clone(),
        },
    )?;
    resolved.generate()
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

#[cfg(test)]
mod tests {
    use super::{ProjectRecord, is_valid_git_commit_sha, validate_environment_git_metadata};
    use crate::error::AppError;
    use serde_json::json;

    fn remote_project() -> ProjectRecord {
        ProjectRecord {
            id: 1,
            project_id: "prj_remote_example".to_string(),
            project_name: "example".to_string(),
            mode: "remote".to_string(),
            git_repo_url: Some("git@github.com:example/repo.git".to_string()),
            default_branch: Some("main".to_string()),
            project_root: Some(".".to_string()),
            metadata: json!({}),
        }
    }

    #[test]
    fn accepts_commit_like_sha_values() {
        assert!(is_valid_git_commit_sha("deadbeef"));
        assert!(is_valid_git_commit_sha(
            "0123456789abcdef0123456789abcdef01234567"
        ));
        assert!(is_valid_git_commit_sha(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        ));
    }

    #[test]
    fn rejects_non_commit_like_sha_values() {
        assert!(!is_valid_git_commit_sha(""));
        assert!(!is_valid_git_commit_sha("abc123"));
        assert!(!is_valid_git_commit_sha("prj_remote_dd74eb7ac24320658c98"));
        assert!(!is_valid_git_commit_sha("main"));
        assert!(!is_valid_git_commit_sha("dead beef"));
    }

    #[test]
    fn remote_environment_requires_commit_like_sha() {
        let project = remote_project();
        let error = validate_environment_git_metadata(
            &project,
            "dev",
            Some("prj_remote_dd74eb7ac24320658c98"),
        )
        .expect_err("expected invalid commit sha");
        assert!(matches!(
            error,
            AppError::InvalidRemoteProjectCommitSha(project_id, slug, _)
                if project_id == "prj_remote_example" && slug == "dev"
        ));
    }
}
