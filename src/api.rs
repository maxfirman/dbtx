use crate::db::{AppliedMigration, EnvironmentRecord, EnvironmentVersionRecord, ProjectRecord};
use crate::execution::{ExecutionCompletion, ExecutionEvent};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MigrateResponse {
    pub applied: Vec<AppliedMigration>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct HealthResponse {
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReadyResponse {
    pub status: String,
    pub database: String,
    pub schema: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ProjectResponse {
    pub project: ProjectRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ProjectDeleteResponse {
    pub deleted_project_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ProjectsResponse {
    pub projects: Vec<ProjectRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ProjectDraftResponse {
    pub draft: crate::db::ProjectDraftRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct EnvironmentResponse {
    pub environment: EnvironmentRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct EnvironmentsResponse {
    pub environments: Vec<EnvironmentRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct EnvironmentVersionsResponse {
    pub versions: Vec<EnvironmentVersionRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ProjectUpdateApiRequest {
    pub git_repo_url: Option<String>,
    pub project_root: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ProjectDraftCreateApiRequest {
    pub git_repo_url: String,
    pub project_root: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ProjectDraftValidateResponse {
    pub draft: crate::db::ProjectDraftRecord,
    pub invocation_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct EnvironmentDraftResponse {
    pub draft: crate::db::EnvironmentDraftRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct EnvironmentDraftUpdateApiRequest {
    pub slug: String,
    pub git_branch: Option<String>,
    pub git_commit_sha: Option<String>,
    pub use_latest_commit: bool,
    pub auto_deploy: bool,
    pub immutable: bool,
    pub adapter_type: String,
    pub schema_name: String,
    pub threads: Option<i32>,
    pub profile_config: serde_json::Value,
    pub profile_secrets: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct EnvironmentDraftStartResponse {
    pub draft: crate::db::EnvironmentDraftRecord,
    pub invocation_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct EnvironmentReleaseApiRequest {
    pub git_branch: Option<String>,
    pub git_commit_sha: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct EnvironmentRollbackApiRequest {
    pub version_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct InvocationCreateApiRequest {
    pub command: InvocationCommandApi,
    pub args: Vec<String>,
    pub current_dir: Option<String>,
    pub project_id: Option<String>,
    pub environment_slug: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum InvocationCommandApi {
    Build,
    Run,
    Ls,
    Test,
    Seed,
    Release,
    ProjectValidate,
    EnvironmentPrepare,
    EnvironmentValidate,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum InvocationExecutionModeApi {
    Server,
    Local,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct InvocationCreateResponse {
    pub invocation_id: Uuid,
    pub execution_mode: InvocationExecutionModeApi,
    pub worker_queue: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct InvocationsResponse {
    pub invocations: Vec<InvocationStatusResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, ToSchema)]
pub struct InvocationListApiRequest {
    pub status: Option<InvocationLifecycleStatus>,
    pub execution_mode: Option<InvocationExecutionModeApi>,
    pub worker_queue: Option<String>,
    pub claimed_by: Option<String>,
    pub cancel_state: Option<InvocationCancelStateApi>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct InvocationCleanupApiRequest {
    pub older_than_seconds: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct InvocationCleanupResponse {
    pub deleted: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InvocationExecutionSpecApi {
    Local {
        command: InvocationCommandApi,
        args: Vec<String>,
        project_dir: String,
        profiles_yml: String,
        state_manifest: Option<serde_json::Value>,
    },
    Remote {
        command: InvocationCommandApi,
        args: Vec<String>,
        repo_url: String,
        commit_sha: String,
        project_root: String,
        profiles_yml: String,
        state_manifest: Option<serde_json::Value>,
    },
    ReleaseValidation {
        repo_url: String,
        git_ref: Option<String>,
        git_commit_sha: Option<String>,
        git_branch: Option<String>,
    },
    ProjectValidation {
        repo_url: String,
        project_root: String,
    },
    EnvironmentPrepare {
        repo_url: String,
        selected_branch: Option<String>,
    },
    EnvironmentValidate {
        repo_url: String,
        commit_sha: String,
        project_root: String,
        selected_branch: Option<String>,
        profiles_yml: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct InvocationClaimResponse {
    pub invocation_id: Uuid,
    pub worker_id: String,
    pub lease_token: Uuid,
    pub execution_mode: InvocationExecutionModeApi,
    pub execution_spec: InvocationExecutionSpecApi,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct InvocationClaimNextApiRequest {
    pub execution_mode: Option<InvocationExecutionModeApi>,
    pub worker_id: String,
    pub worker_queue: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct InvocationEventBatchApiRequest {
    pub worker_id: String,
    pub lease_token: Uuid,
    pub events: Vec<ExecutionEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct InvocationCompleteApiRequest {
    pub worker_id: String,
    pub lease_token: Uuid,
    pub completion: ExecutionCompletion,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct InvocationStatusResponse {
    pub invocation_id: Uuid,
    pub execution_mode: InvocationExecutionModeApi,
    pub worker_queue: String,
    pub worker_health: InvocationWorkerHealthApi,
    pub cancel_state: InvocationCancelStateApi,
    pub status: InvocationLifecycleStatus,
    pub exit_code: Option<i32>,
    pub error: Option<String>,
    pub started_at: DateTime<Utc>,
    pub claimed_at: Option<DateTime<Utc>>,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub cancel_requested_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub cancel_requested: bool,
    pub claimed_by: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum InvocationWorkerHealthApi {
    Unclaimed,
    Claimed,
    Idle,
    Stale,
    Completed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum InvocationCancelStateApi {
    None,
    Requested,
    Completed,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum InvocationLifecycleStatus {
    Running,
    Succeeded,
    Failed,
    Canceled,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct InvocationHeartbeatApiRequest {
    pub worker_id: String,
    pub lease_token: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct InvocationHeartbeatResponse {
    pub cancel_requested: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct WorkerStatusResponse {
    pub worker_id: String,
    pub execution_mode: InvocationExecutionModeApi,
    pub worker_queue: String,
    pub claimed_invocation_count: i64,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub health: InvocationWorkerHealthApi,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct WorkersResponse {
    pub workers: Vec<WorkerStatusResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct QueueStatusResponse {
    pub worker_queue: String,
    pub execution_mode: InvocationExecutionModeApi,
    pub pending_count: i64,
    pub claimed_count: i64,
    pub stale_claim_count: i64,
    pub oldest_pending_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct QueuesResponse {
    pub queues: Vec<QueueStatusResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, ToSchema)]
pub struct InvocationCancelApiRequest {}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct InvocationEvent {
    pub event_type: String,
    pub timestamp: DateTime<Utc>,
    pub text: Option<String>,
    pub stream: Option<String>,
    pub dbt_event_name: Option<String>,
    pub node_unique_id: Option<String>,
    pub level: Option<String>,
    pub exit_code: Option<i32>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiErrorResponse {
    pub error: String,
}
