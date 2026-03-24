use crate::db::{AppliedMigration, EnvironmentRecord, ProjectRecord};
use crate::execution::{ExecutionCompletion, ExecutionEvent};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrateResponse {
    pub applied: Vec<AppliedMigration>,
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
pub struct ProjectInitApiRequest {
    pub current_dir: String,
    pub git_repo_url: Option<String>,
    pub project_root: Option<String>,
    pub default_branch: Option<String>,
    pub force: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectUpdateApiRequest {
    pub current_dir: String,
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
    pub kind: String,
    pub baseline: Option<String>,
    pub git_branch: Option<String>,
    pub git_commit_sha: Option<String>,
    pub pr_number: Option<i32>,
    pub immutable: bool,
    pub status: String,
    pub schema_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentUpdateApiRequest {
    pub current_dir: String,
    pub project: String,
    pub slug: String,
    pub kind: Option<String>,
    pub baseline: Option<String>,
    pub git_branch: Option<String>,
    pub git_commit_sha: Option<String>,
    pub pr_number: Option<i32>,
    pub immutable: bool,
    pub status: Option<String>,
    pub adapter_type: Option<String>,
    pub schema_name: Option<String>,
    pub threads: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationCreateApiRequest {
    pub command: InvocationCommandApi,
    pub args: Vec<String>,
    pub current_dir: String,
    pub environment_slug: String,
    pub execution_mode: InvocationExecutionModeApi,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationExecutionSpecApi {
    pub command: InvocationCommandApi,
    pub args: Vec<String>,
    pub project_dir: String,
    pub profiles_yml: String,
    pub state_manifest: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationClaimResponse {
    pub invocation_id: Uuid,
    pub execution_mode: InvocationExecutionModeApi,
    pub execution_spec: InvocationExecutionSpecApi,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct InvocationClaimNextApiRequest {
    pub execution_mode: Option<InvocationExecutionModeApi>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationEventBatchApiRequest {
    pub events: Vec<ExecutionEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationCompleteApiRequest {
    pub completion: ExecutionCompletion,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationStatusResponse {
    pub invocation_id: Uuid,
    pub status: InvocationLifecycleStatus,
    pub exit_code: Option<i32>,
    pub error: Option<String>,
    pub started_at: DateTime<Utc>,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub cancel_requested: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvocationLifecycleStatus {
    Running,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct InvocationHeartbeatApiRequest {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationHeartbeatResponse {
    pub cancel_requested: bool,
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
