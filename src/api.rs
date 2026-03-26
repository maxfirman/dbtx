use crate::db::{AppliedMigration, EnvironmentRecord, EnvironmentVersionRecord, ProjectRecord};
use crate::execution::{ExecutionCompletion, ExecutionEvent};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrateResponse {
    pub applied: Vec<AppliedMigration>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadyResponse {
    pub status: String,
    pub database: String,
    pub schema: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectResponse {
    pub project: ProjectRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectsResponse {
    pub projects: Vec<ProjectRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentResponse {
    pub environment: EnvironmentRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentsResponse {
    pub environments: Vec<EnvironmentRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentVersionsResponse {
    pub versions: Vec<EnvironmentVersionRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectInitApiRequest {
    pub current_dir: String,
    pub mode: Option<String>,
    pub git_repo_url: Option<String>,
    pub project_root: Option<String>,
    pub default_branch: Option<String>,
    pub force: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectUpdateApiRequest {
    pub current_dir: String,
    pub mode: Option<String>,
    pub git_repo_url: Option<String>,
    pub project_root: Option<String>,
    pub default_branch: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectShowApiRequest {
    pub current_dir: String,
    pub project: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentCreateApiRequest {
    pub current_dir: String,
    pub project: Option<String>,
    pub slug: Option<String>,
    pub target: Option<String>,
    pub baseline: Option<String>,
    pub git_branch: Option<String>,
    pub git_commit_sha: Option<String>,
    pub pr_number: Option<i32>,
    pub status: String,
    pub worker_queue: Option<String>,
    pub schema_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentUpdateApiRequest {
    pub current_dir: String,
    pub project: String,
    pub slug: String,
    pub baseline: Option<String>,
    pub git_branch: Option<String>,
    pub git_commit_sha: Option<String>,
    pub pr_number: Option<i32>,
    pub status: Option<String>,
    pub adapter_type: Option<String>,
    pub worker_queue: Option<String>,
    pub schema_name: Option<String>,
    pub threads: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentReleaseApiRequest {
    pub current_dir: String,
    pub project: String,
    pub slug: String,
    pub git_branch: Option<String>,
    pub git_commit_sha: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentRollbackApiRequest {
    pub current_dir: String,
    pub project: String,
    pub slug: String,
    pub version_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationCreateApiRequest {
    pub command: InvocationCommandApi,
    pub args: Vec<String>,
    pub current_dir: Option<String>,
    pub project_id: Option<String>,
    pub environment_slug: Option<String>,
    pub execution_mode: InvocationExecutionModeApi,
    pub worker_queue: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvocationCommandApi {
    Build,
    Run,
    Ls,
    Test,
    Seed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InvocationExecutionModeApi {
    Server,
    Local,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationCreateResponse {
    pub invocation_id: Uuid,
    pub execution_mode: InvocationExecutionModeApi,
    pub worker_queue: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationsResponse {
    pub invocations: Vec<InvocationStatusResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct InvocationListApiRequest {
    pub status: Option<InvocationLifecycleStatus>,
    pub execution_mode: Option<InvocationExecutionModeApi>,
    pub worker_queue: Option<String>,
    pub claimed_by: Option<String>,
    pub cancel_state: Option<InvocationCancelStateApi>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationCleanupApiRequest {
    pub older_than_seconds: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationCleanupResponse {
    pub deleted: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationClaimResponse {
    pub invocation_id: Uuid,
    pub worker_id: String,
    pub lease_token: Uuid,
    pub execution_mode: InvocationExecutionModeApi,
    pub execution_spec: InvocationExecutionSpecApi,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationClaimNextApiRequest {
    pub execution_mode: Option<InvocationExecutionModeApi>,
    pub worker_id: String,
    pub worker_queue: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationEventBatchApiRequest {
    pub worker_id: String,
    pub lease_token: Uuid,
    pub events: Vec<ExecutionEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationCompleteApiRequest {
    pub worker_id: String,
    pub lease_token: Uuid,
    pub completion: ExecutionCompletion,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InvocationWorkerHealthApi {
    Unclaimed,
    Claimed,
    Stale,
    Completed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InvocationCancelStateApi {
    None,
    Requested,
    Completed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvocationLifecycleStatus {
    Running,
    Succeeded,
    Failed,
    Canceled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationHeartbeatApiRequest {
    pub worker_id: String,
    pub lease_token: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationHeartbeatResponse {
    pub cancel_requested: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerStatusResponse {
    pub worker_id: String,
    pub execution_mode: InvocationExecutionModeApi,
    pub worker_queue: String,
    pub claimed_invocation_count: i64,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub health: InvocationWorkerHealthApi,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkersResponse {
    pub workers: Vec<WorkerStatusResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueStatusResponse {
    pub worker_queue: String,
    pub execution_mode: InvocationExecutionModeApi,
    pub pending_count: i64,
    pub claimed_count: i64,
    pub stale_claim_count: i64,
    pub oldest_pending_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuesResponse {
    pub queues: Vec<QueueStatusResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct InvocationCancelApiRequest {}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
