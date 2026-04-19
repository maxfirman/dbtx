//! Database access layer: queries, row mapping, and schema migrations.
use crate::api::{
    EnvironmentActiveResourcePhaseApi, InvocationCancelStateApi, InvocationClaimResponse,
    InvocationEvent, InvocationExecutionModeApi, InvocationExecutionSpecApi,
    InvocationLifecycleStatus, InvocationListApiRequest, InvocationStatusResponse,
    InvocationWorkerHealthApi, QueueStatusResponse, WorkerStatusResponse,
};
use crate::error::{AppError, AppResult};
use crate::event::LogEvent;
use crate::execution::{ExecutionMode, heartbeat_stale_timeout};
use crate::manifest::{ManifestSnapshot, ReconstructedManifest};
use crate::profile::validate_environment_profile;
use chrono::Utc;
use serde_json::Value;
use sqlx::migrate::Migrator;
use sqlx::postgres::PgPoolOptions;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use uuid::Uuid;

mod records;
pub use records::*;

// Re-export dbt utility functions for backward compatibility.
// New code should import from crate::dbt_utils directly.
pub(crate) use crate::dbt_utils::{
    append_invocation_id, append_profiles_dir, append_state_dir, build_generated_profiles,
    git_repo_root, read_dbt_project_name, read_git_state,
};

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

#[derive(Clone)]
pub struct Db {
    pool: PgPool,
}

impl Db {
    pub async fn connect(database_url: &str) -> AppResult<Self> {
        let max_connections = std::env::var("DBTX_DB_MAX_CONNECTIONS")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(20);
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
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
}

mod projects;
mod environments;
mod reconciliation;
mod invocations;
mod runs;


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

pub(crate) fn validate_remote_project_root(project_root: &str) -> AppResult<()> {
    validate_remote_project_root_value(project_root)
}

pub(crate) fn remote_project_id(repo_url: &str, project_root: &str, project_name: &str) -> String {
    let digest = md5::compute(format!(
        "{}\u{1f}{}\u{1f}{}",
        repo_url.trim(),
        project_root.trim(),
        project_name.trim()
    ));
    let hex = format!("{:x}", digest);
    format!("prj_remote_{}", &hex[..16])
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
    EnvironmentStatus::parse(status)
        .map(|_| ())
        .ok_or_else(|| AppError::InvalidEnvironmentStatus(status.to_string()))
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

fn project_draft_record_from_row(row: &sqlx::postgres::PgRow) -> ProjectDraftRecord {
    ProjectDraftRecord {
        id: row.get("id"),
        git_repo_url: row.get("git_repo_url"),
        project_root: row.get("project_root"),
        status: DraftStatus::parse(&row.get::<String, _>("status")),
        validation_error: row.get("validation_error"),
        project_name: row.get("project_name"),
        default_branch: row.get("default_branch"),
        validation_invocation_id: row.get("validation_invocation_id"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        validated_at: row.get("validated_at"),
    }
}

fn environment_draft_record_from_row(row: &sqlx::postgres::PgRow) -> EnvironmentDraftRecord {
    EnvironmentDraftRecord {
        id: row.get("id"),
        project_id: row.get("project_id"),
        slug: row.get("slug"),
        git_branch: row.get("git_branch"),
        git_commit_sha: row.get("git_commit_sha"),
        use_latest_commit: row.get("use_latest_commit"),
        auto_deploy: row.get("auto_deploy"),
        immutable: row.get("immutable"),
        adapter_type: row.get("adapter_type"),
        schema_name: row.get("schema_name"),
        threads: row.get("threads"),
        profile_config: row.get::<sqlx::types::Json<Value>, _>("profile_config").0,
        profile_secrets: row.get::<sqlx::types::Json<Value>, _>("profile_secrets").0,
        branch_options: row.get::<sqlx::types::Json<Value>, _>("branch_options").0,
        commit_options: row.get::<sqlx::types::Json<Value>, _>("commit_options").0,
        status: DraftStatus::parse(&row.get::<String, _>("status")),
        validation_error: row.get("validation_error"),
        validation_invocation_id: row.get("validation_invocation_id"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        validated_at: row.get("validated_at"),
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
        use_latest_commit: row.get("use_latest_commit"),
        auto_deploy: row.get("auto_deploy"),
        immutable: row.get("immutable"),
        pr_number: row.get("pr_number"),
        status: EnvironmentStatus::parse(&row.get::<String, _>("status")).unwrap_or(EnvironmentStatus::Active),
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
        use_latest_commit: row.get("use_latest_commit"),
        auto_deploy: row.get("auto_deploy"),
        immutable: row.get("immutable"),
        baseline_environment_id: row.get("baseline_environment_id"),
        metadata: row.get::<sqlx::types::Json<Value>, _>("metadata").0,
    }
}

fn environment_actual_state_from_row(row: &sqlx::postgres::PgRow) -> EnvironmentActualStateRecord {
    EnvironmentActualStateRecord {
        project_id: row.get("project_id"),
        environment_id: row.get("environment_id"),
        last_attempted_run_id: row.get("last_attempted_run_id"),
        last_attempted_commit_sha: row.get("last_attempted_commit_sha"),
        last_attempted_at: row.get("last_attempted_at"),
        last_successful_run_id: row.get("last_successful_run_id"),
        last_successful_commit_sha: row.get("last_successful_commit_sha"),
        last_successful_at: row.get("last_successful_at"),
        last_admitted_plan_id: row.get("last_admitted_plan_id"),
        last_completed_plan_id: row.get("last_completed_plan_id"),
        updated_at: row.get("updated_at"),
    }
}

fn environment_reconcile_preparation_from_row(
    row: &sqlx::postgres::PgRow,
) -> EnvironmentReconcilePreparationRecord {
    EnvironmentReconcilePreparationRecord {
        project_id: row.get("project_id"),
        environment_id: row.get("environment_id"),
        kind: row.get("kind"),
        input_fingerprint: row.get("input_fingerprint"),
        target_git_commit_sha: row.get("target_git_commit_sha"),
        status: PreparationStatus::parse(&row.get::<String, _>("status")),
        invocation_id: row.get("invocation_id"),
        error: row.get("error"),
        failure_count: row.get("failure_count"),
        next_attempt_at: row.get("next_attempt_at"),
        started_at: row.get("started_at"),
        completed_at: row.get("completed_at"),
        updated_at: row.get("updated_at"),
    }
}

fn environment_run_plan_from_row(row: &sqlx::postgres::PgRow) -> EnvironmentRunPlanRecord {
    EnvironmentRunPlanRecord {
        plan_id: row.get("plan_id"),
        project_id: row.get("project_id"),
        environment_id: row.get("environment_id"),
        status: PlanStatus::parse(&row.get::<String, _>("status")),
        reason: row.get("reason"),
        input_fingerprint: row.get("input_fingerprint"),
        target_git_branch: row.get("target_git_branch"),
        target_git_commit_sha: row.get("target_git_commit_sha"),
        baseline_run_id: row.get("baseline_run_id"),
        selection_spec: row.get("selection_spec"),
        selected_resources: row
            .get::<sqlx::types::Json<Vec<String>>, _>("selected_resources")
            .0,
        resource_count: row.get("resource_count"),
        superseded_by_plan_id: row.get("superseded_by_plan_id"),
        retry_count: row.get("retry_count"),
        blocked_by_invocation_id: row.get("blocked_by_invocation_id"),
        admitted_invocation_id: row.get("admitted_invocation_id"),
        source_event_id: row.get("source_event_id"),
        error: row.get("error"),
        failure_count: row.get("failure_count"),
        next_attempt_at: row.get("next_attempt_at"),
        first_blocked_at: row.get("first_blocked_at"),
        last_blocked_at: row.get("last_blocked_at"),
        last_checked_at: row.get("last_checked_at"),
        admitted_at: row.get("admitted_at"),
        completed_at: row.get("completed_at"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        metadata: row.get::<sqlx::types::Json<Value>, _>("metadata").0,
    }
}

fn source_state_event_from_row(row: &sqlx::postgres::PgRow) -> SourceStateEventRecord {
    SourceStateEventRecord {
        id: row.get("id"),
        project_id: row.get("project_id"),
        environment_id: row.get("environment_id"),
        source_key: row.get("source_key"),
        provider: row.get("provider"),
        state_version: row.get("state_version"),
        payload: row.get::<sqlx::types::Json<Value>, _>("payload").0,
        observed_at: row.get("observed_at"),
        created_at: row.get("created_at"),
    }
}

fn automatic_retry_backoff(failure_count: i32) -> chrono::Duration {
    let exponent = failure_count.saturating_sub(1).clamp(0, 6) as u32;
    let seconds = (5_i64 * (1_i64 << exponent)).min(300);
    chrono::Duration::seconds(seconds)
}

fn plan_source_event_ids(source_event_id: Option<i64>, metadata: &Value) -> Vec<i64> {
    let mut event_ids = metadata
        .get("source_event_ids")
        .and_then(|value| value.as_array())
        .into_iter()
        .flatten()
        .filter_map(|value| value.as_i64())
        .collect::<Vec<_>>();
    if event_ids.is_empty() && let Some(source_event_id) = source_event_id {
        event_ids.push(source_event_id);
    }
    event_ids.sort_unstable();
    event_ids.dedup();
    event_ids
}

fn active_environment_resource_from_row(row: &sqlx::postgres::PgRow) -> EnvironmentActiveResourceRecord {
    EnvironmentActiveResourceRecord {
        invocation_id: row.get("invocation_id"),
        run_id: row.get("run_id"),
        unique_id: row.get("unique_id"),
        resource_type: row.get("resource_type"),
        phase: match row.get::<String, _>("phase").as_str() {
            "running" => EnvironmentActiveResourcePhaseApi::Running,
            _ => EnvironmentActiveResourcePhaseApi::Selected,
        },
        selected_at: row.get("selected_at"),
        node_started_at: row.get("node_started_at"),
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

fn worker_registry_read_model_from_row(row: PgRow) -> WorkerRegistryReadModel {
    WorkerRegistryReadModel {
        worker_id: row.get("worker_id"),
        execution_mode: execution_mode_from_db(&row.get::<String, _>("execution_mode")),
        worker_queue: row.get("worker_queue"),
        last_seen_at: row.get("last_seen_at"),
    }
}

fn compute_worker_registry_health(
    worker: &WorkerRegistryReadModel,
    claimed_invocation_count: i64,
    last_seen_at: chrono::DateTime<Utc>,
) -> InvocationWorkerHealthApi {
    let stale_after = chrono::Duration::from_std(heartbeat_stale_timeout(worker.execution_mode))
        .unwrap_or_else(|_| chrono::Duration::seconds(15));
    let is_stale = Utc::now() - last_seen_at > stale_after;
    if claimed_invocation_count > 0 {
        if is_stale {
            InvocationWorkerHealthApi::Stale
        } else {
            InvocationWorkerHealthApi::Claimed
        }
    } else if is_stale {
        InvocationWorkerHealthApi::Stale
    } else {
        InvocationWorkerHealthApi::Idle
    }
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

fn invocation_mode_value(value: InvocationExecutionModeApi) -> &'static str {
    match value {
        InvocationExecutionModeApi::Server => "server",
        InvocationExecutionModeApi::Local => "local",
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

#[cfg(test)]
mod tests {
    use super::{
        ProjectRecord, WorkerRegistryReadModel, compute_worker_registry_health,
        is_valid_git_commit_sha, validate_environment_git_metadata,
    };
    use crate::api::{InvocationExecutionModeApi, InvocationWorkerHealthApi};
    use crate::error::AppError;
    use chrono::{Duration, Utc};
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

    #[test]
    fn worker_registry_health_reports_idle_without_claims() {
        let worker = WorkerRegistryReadModel {
            worker_id: "worker-1".to_string(),
            execution_mode: InvocationExecutionModeApi::Server,
            worker_queue: "generic".to_string(),
            last_seen_at: Utc::now(),
        };
        assert_eq!(
            compute_worker_registry_health(&worker, 0, worker.last_seen_at),
            InvocationWorkerHealthApi::Idle
        );
    }

    #[test]
    fn worker_registry_health_reports_stale_when_last_seen_is_old() {
        let worker = WorkerRegistryReadModel {
            worker_id: "worker-1".to_string(),
            execution_mode: InvocationExecutionModeApi::Server,
            worker_queue: "generic".to_string(),
            last_seen_at: Utc::now() - Duration::seconds(60),
        };
        assert_eq!(
            compute_worker_registry_health(&worker, 0, worker.last_seen_at),
            InvocationWorkerHealthApi::Stale
        );
    }

    #[test]
    fn automatic_retry_backoff_scales_exponentially() {
        use super::automatic_retry_backoff;
        assert_eq!(automatic_retry_backoff(0), Duration::seconds(5));
        assert_eq!(automatic_retry_backoff(1), Duration::seconds(5));
        assert_eq!(automatic_retry_backoff(2), Duration::seconds(10));
        assert_eq!(automatic_retry_backoff(3), Duration::seconds(20));
        assert_eq!(automatic_retry_backoff(4), Duration::seconds(40));
        // Caps at 300s
        assert_eq!(automatic_retry_backoff(10), Duration::seconds(300));
    }

    #[test]
    fn plan_source_event_ids_extracts_from_metadata() {
        use super::plan_source_event_ids;
        let metadata = json!({"source_event_ids": [3, 1, 2, 1]});
        let ids = plan_source_event_ids(None, &metadata);
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn plan_source_event_ids_falls_back_to_source_event_id() {
        use super::plan_source_event_ids;
        let ids = plan_source_event_ids(Some(42), &json!({}));
        assert_eq!(ids, vec![42]);
    }

    #[test]
    fn plan_source_event_ids_returns_empty_when_no_source() {
        use super::plan_source_event_ids;
        let ids = plan_source_event_ids(None, &json!({}));
        assert!(ids.is_empty());
    }

    #[test]
    fn remote_project_id_is_deterministic() {
        use super::remote_project_id;
        let id1 = remote_project_id("git@github.com:org/repo.git", ".", "my_project");
        let id2 = remote_project_id("git@github.com:org/repo.git", ".", "my_project");
        assert_eq!(id1, id2);
        assert!(id1.starts_with("prj_remote_"));
        assert_eq!(id1.len(), "prj_remote_".len() + 16);
    }

    #[test]
    fn remote_project_id_differs_for_different_inputs() {
        use super::remote_project_id;
        let id1 = remote_project_id("git@github.com:org/repo.git", ".", "proj_a");
        let id2 = remote_project_id("git@github.com:org/repo.git", ".", "proj_b");
        assert_ne!(id1, id2);
    }

    #[test]
    fn compute_cancel_state_returns_completed_for_canceled() {
        use super::compute_cancel_state;
        use crate::api::{InvocationCancelStateApi, InvocationLifecycleStatus, InvocationStatusResponse};
        let status = InvocationStatusResponse {
            invocation_id: uuid::Uuid::nil(),
            execution_mode: InvocationExecutionModeApi::Server,
            worker_queue: "q".to_string(),
            status: InvocationLifecycleStatus::Canceled,
            exit_code: Some(130),
            error: None,
            started_at: Utc::now(),
            claimed_at: None,
            last_heartbeat_at: None,
            cancel_requested_at: None,
            completed_at: None,
            cancel_requested: true,
            claimed_by: None,
            cancel_state: InvocationCancelStateApi::None,
            worker_health: InvocationWorkerHealthApi::Idle,
        };
        assert_eq!(compute_cancel_state(&status), InvocationCancelStateApi::Completed);
    }

    #[test]
    fn compute_cancel_state_returns_requested_when_pending() {
        use super::compute_cancel_state;
        use crate::api::{InvocationCancelStateApi, InvocationLifecycleStatus, InvocationStatusResponse};
        let status = InvocationStatusResponse {
            invocation_id: uuid::Uuid::nil(),
            execution_mode: InvocationExecutionModeApi::Server,
            worker_queue: "q".to_string(),
            status: InvocationLifecycleStatus::Running,
            exit_code: None,
            error: None,
            started_at: Utc::now(),
            claimed_at: Some(Utc::now()),
            last_heartbeat_at: None,
            cancel_requested_at: Some(Utc::now()),
            completed_at: None,
            cancel_requested: true,
            claimed_by: Some("w".to_string()),
            cancel_state: InvocationCancelStateApi::None,
            worker_health: InvocationWorkerHealthApi::Claimed,
        };
        assert_eq!(compute_cancel_state(&status), InvocationCancelStateApi::Requested);
    }
}
