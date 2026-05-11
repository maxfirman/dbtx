use crate::api::{
    EnvironmentActiveResourcePhaseApi, InvocationExecutionModeApi, InvocationExecutionSpecApi,
    InvocationLifecycleStatus,
};
use crate::execution::ExecutionMode;
use crate::manifest::ManifestSnapshot;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;
use uuid::Uuid;

/// Status of an environment reconciliation plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PlanStatus {
    Planned,
    Blocked,
    Admitted,
    Completed,
    Failed,
    Canceled,
    Superseded,
}

impl PlanStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Blocked => "blocked",
            Self::Admitted => "admitted",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
            Self::Superseded => "superseded",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "planned" => Some(Self::Planned),
            "blocked" => Some(Self::Blocked),
            "admitted" => Some(Self::Admitted),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "canceled" => Some(Self::Canceled),
            "superseded" => Some(Self::Superseded),
            _ => None,
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Canceled | Self::Superseded
        )
    }

    pub fn is_admissible(self) -> bool {
        matches!(self, Self::Planned | Self::Blocked)
    }
}

impl std::fmt::Display for PlanStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Status of a remote environment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum EnvironmentStatus {
    Active,
    Archived,
    Failed,
    Deleting,
}

impl EnvironmentStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Archived => "archived",
            Self::Failed => "failed",
            Self::Deleting => "deleting",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "active" => Some(Self::Active),
            "archived" => Some(Self::Archived),
            "failed" => Some(Self::Failed),
            "deleting" => Some(Self::Deleting),
            _ => None,
        }
    }
}

impl std::fmt::Display for EnvironmentStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Status of a project or environment onboarding draft.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DraftStatus {
    Draft,
    LoadingGit,
    Ready,
    Validating,
    Validated,
    Failed,
}

impl DraftStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::LoadingGit => "loading_git",
            Self::Ready => "ready",
            Self::Validating => "validating",
            Self::Validated => "validated",
            Self::Failed => "failed",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "draft" => Some(Self::Draft),
            "loading_git" => Some(Self::LoadingGit),
            "ready" => Some(Self::Ready),
            "validating" => Some(Self::Validating),
            "validated" => Some(Self::Validated),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Validated | Self::Failed)
    }
}

impl std::fmt::Display for DraftStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Status of an environment reconcile preparation step.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PreparationStatus {
    Running,
    Succeeded,
    Failed,
}

impl PreparationStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "running" => Some(Self::Running),
            "succeeded" => Some(Self::Succeeded),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

impl std::fmt::Display for PreparationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Node execution status from dbt log events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeExecutionStatus {
    Success,
    Pass,
    Created,
    Error,
    Fail,
    Skipped,
}

impl NodeExecutionStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Pass => "pass",
            Self::Created => "created",
            Self::Error => "error",
            Self::Fail => "fail",
            Self::Skipped => "skipped",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "success" => Some(Self::Success),
            "pass" => Some(Self::Pass),
            "created" => Some(Self::Created),
            "error" => Some(Self::Error),
            "fail" | "failed" => Some(Self::Fail),
            "skipped" => Some(Self::Skipped),
            _ => None,
        }
    }

    pub fn is_promotable(self) -> bool {
        matches!(self, Self::Success | Self::Pass | Self::Created)
    }

    /// SQL literal list for use in queries that filter on promotable statuses.
    pub const PROMOTABLE_SQL: &str = "'success', 'pass', 'created'";
}

impl std::fmt::Display for NodeExecutionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AppliedMigration {
    pub version: i64,
    pub description: String,
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
pub struct ProjectDraftRecord {
    pub id: Uuid,
    pub git_repo_url: String,
    pub project_root: String,
    pub status: DraftStatus,
    pub validation_error: Option<String>,
    pub project_name: Option<String>,
    pub default_branch: Option<String>,
    pub validation_invocation_id: Option<Uuid>,
    pub created_at: chrono::DateTime<Utc>,
    pub updated_at: chrono::DateTime<Utc>,
    pub validated_at: Option<chrono::DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct EnvironmentDraftRecord {
    pub id: Uuid,
    pub project_id: i64,
    pub slug: String,
    pub git_branch: Option<String>,
    pub git_commit_sha: Option<String>,
    pub use_latest_commit: bool,
    pub auto_reconcile: bool,
    pub immutable: bool,
    pub adapter_type: Option<String>,
    pub schema_name: Option<String>,
    pub threads: Option<i32>,
    pub profile_config: Value,
    pub profile_secrets: Value,
    pub branch_options: Value,
    pub commit_options: Value,
    pub status: DraftStatus,
    pub validation_error: Option<String>,
    pub validation_invocation_id: Option<Uuid>,
    pub created_at: chrono::DateTime<Utc>,
    pub updated_at: chrono::DateTime<Utc>,
    pub validated_at: Option<chrono::DateTime<Utc>>,
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
    pub use_latest_commit: bool,
    pub auto_reconcile: bool,
    pub immutable: bool,
    pub pr_number: Option<i32>,
    pub status: EnvironmentStatus,
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
    pub use_latest_commit: bool,
    pub auto_reconcile: bool,
    pub immutable: bool,
    pub baseline_environment_id: Option<i64>,
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct EnvironmentActualStateRecord {
    pub project_id: i64,
    pub environment_id: i64,
    pub last_attempted_run_id: Option<Uuid>,
    pub last_attempted_commit_sha: Option<String>,
    pub last_attempted_at: Option<chrono::DateTime<Utc>>,
    pub last_successful_run_id: Option<Uuid>,
    pub last_successful_commit_sha: Option<String>,
    pub last_successful_at: Option<chrono::DateTime<Utc>>,
    pub last_admitted_plan_id: Option<Uuid>,
    pub last_completed_plan_id: Option<Uuid>,
    pub updated_at: chrono::DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct EnvironmentReconcilePreparationRecord {
    pub project_id: i64,
    pub environment_id: i64,
    pub kind: String,
    pub input_fingerprint: Option<String>,
    pub target_git_commit_sha: Option<String>,
    pub status: PreparationStatus,
    pub invocation_id: Option<Uuid>,
    pub error: Option<String>,
    pub failure_count: i32,
    pub next_attempt_at: Option<chrono::DateTime<Utc>>,
    pub started_at: Option<chrono::DateTime<Utc>>,
    pub completed_at: Option<chrono::DateTime<Utc>>,
    pub updated_at: chrono::DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct EnvironmentRunPlanRecord {
    pub plan_id: Uuid,
    pub project_id: i64,
    pub environment_id: i64,
    pub status: PlanStatus,
    pub reason: String,
    pub input_fingerprint: Option<String>,
    pub target_git_branch: Option<String>,
    pub target_git_commit_sha: Option<String>,
    pub baseline_run_id: Option<Uuid>,
    pub selection_spec: Option<String>,
    pub selected_resources: Vec<String>,
    pub resource_count: i32,
    pub superseded_by_plan_id: Option<Uuid>,
    pub retry_count: i32,
    pub blocked_by_invocation_id: Option<Uuid>,
    pub admitted_invocation_id: Option<Uuid>,
    pub source_event_id: Option<i64>,
    pub error: Option<String>,
    pub failure_count: i32,
    pub next_attempt_at: Option<chrono::DateTime<Utc>>,
    pub first_blocked_at: Option<chrono::DateTime<Utc>>,
    pub last_blocked_at: Option<chrono::DateTime<Utc>>,
    pub last_checked_at: Option<chrono::DateTime<Utc>>,
    pub admitted_at: Option<chrono::DateTime<Utc>>,
    pub completed_at: Option<chrono::DateTime<Utc>>,
    pub created_at: chrono::DateTime<Utc>,
    pub updated_at: chrono::DateTime<Utc>,
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SourceStateEventRecord {
    pub id: i64,
    pub project_id: i64,
    pub environment_id: Option<i64>,
    pub source_key: String,
    pub provider: String,
    pub state_version: Option<String>,
    pub payload: Value,
    pub observed_at: chrono::DateTime<Utc>,
    pub created_at: chrono::DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct EnvironmentActiveResourceRecord {
    pub invocation_id: Uuid,
    pub run_id: Option<Uuid>,
    pub unique_id: String,
    pub resource_type: String,
    pub phase: EnvironmentActiveResourcePhaseApi,
    pub selected_at: chrono::DateTime<Utc>,
    pub node_started_at: Option<chrono::DateTime<Utc>>,
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
pub struct CreateProjectDraftInput {
    pub git_repo_url: String,
    pub project_root: String,
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
    pub use_latest_commit: bool,
    pub auto_reconcile: bool,
    pub immutable: bool,
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
    pub use_latest_commit: Option<bool>,
    pub auto_reconcile: Option<bool>,
    pub immutable: Option<bool>,
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
pub struct CreateEnvironmentDraftInput {
    pub project_id: i64,
    pub default_branch: Option<String>,
}

#[derive(Debug, Clone)]
pub struct UpdateEnvironmentDraftInput {
    pub slug: String,
    pub git_branch: Option<String>,
    pub git_commit_sha: Option<String>,
    pub use_latest_commit: bool,
    pub auto_reconcile: bool,
    pub immutable: bool,
    pub adapter_type: Option<String>,
    pub schema_name: Option<String>,
    pub threads: Option<i32>,
    pub profile_config: Value,
    pub profile_secrets: Value,
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
    pub(crate) plan_id: Option<Uuid>,
    pub(crate) run_id: Option<Uuid>,
    pub(crate) project_id: Option<i64>,
    pub(crate) environment_id: Option<i64>,
    pub(crate) project_draft_id: Option<Uuid>,
    pub(crate) environment_draft_id: Option<Uuid>,
    pub(crate) command: String,
    pub(crate) execution_mode: InvocationExecutionModeApi,
    pub(crate) worker_queue: String,
    pub(crate) execution_spec: Option<InvocationExecutionSpecApi>,
    pub(crate) promote_base_manifest: bool,
    pub(crate) updates_actual_state: bool,
    pub(crate) claim_deadline_at: Option<chrono::DateTime<Utc>>,
}

pub(crate) struct SourceStateEventCreateInput {
    pub(crate) project: String,
    pub(crate) environment_slug: String,
    pub(crate) source_key: String,
    pub(crate) provider: String,
    pub(crate) state_version: Option<String>,
    pub(crate) observed_at: Option<chrono::DateTime<Utc>>,
    pub(crate) payload: Value,
}

pub(crate) struct CreateEnvironmentRunPlanInput<'a> {
    pub(crate) environment: &'a EnvironmentRecord,
    pub(crate) reason: &'a str,
    pub(crate) input_fingerprint: &'a str,
    pub(crate) baseline_run_id: Option<Uuid>,
    pub(crate) selection_spec: Option<&'a str>,
    pub(crate) selected_resources: &'a [String],
    pub(crate) source_event_id: Option<i64>,
    pub(crate) metadata: Value,
}

#[derive(Debug, Clone)]
pub(crate) struct EquivalentPlanLookup<'a> {
    pub(crate) project_id: i64,
    pub(crate) environment_id: i64,
    pub(crate) reason: &'a str,
    pub(crate) input_fingerprint: &'a str,
    pub(crate) target_git_branch: Option<&'a str>,
    pub(crate) target_git_commit_sha: Option<&'a str>,
    pub(crate) baseline_run_id: Option<Uuid>,
    pub(crate) selection_spec: Option<&'a str>,
    pub(crate) selected_resources: &'a [String],
}

#[derive(Debug, Clone)]
pub(crate) struct InvocationPersistenceRecord {
    pub(crate) plan_id: Option<Uuid>,
    pub(crate) run_id: Option<Uuid>,
    pub(crate) project_id: Option<i64>,
    pub(crate) environment_id: Option<i64>,
    pub(crate) project_draft_id: Option<Uuid>,
    pub(crate) environment_draft_id: Option<Uuid>,
    pub(crate) command: String,
    pub(crate) promote_base_manifest: bool,
    pub(crate) updates_actual_state: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct LocalEnvironmentUpsertInput<'a> {
    pub(crate) project: &'a ProjectRecord,
    pub(crate) profile_name: &'a str,
    pub(crate) target_name: &'a str,
    pub(crate) adapter_type: &'a str,
    pub(crate) worker_queue: &'a str,
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
pub(crate) struct InvocationReadModel {
    pub(crate) execution_mode: InvocationExecutionModeApi,
    pub(crate) worker_queue: String,
    pub(crate) status: InvocationLifecycleStatus,
    pub(crate) started_at: chrono::DateTime<Utc>,
    pub(crate) claimed_at: Option<chrono::DateTime<Utc>>,
    pub(crate) last_heartbeat_at: Option<chrono::DateTime<Utc>>,
    pub(crate) claimed_by: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct WorkerRegistryReadModel {
    pub(crate) worker_id: String,
    pub(crate) execution_mode: InvocationExecutionModeApi,
    pub(crate) worker_queue: String,
    pub(crate) last_seen_at: chrono::DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub(crate) struct PlanningManifestNodeRecord {
    pub(crate) unique_id: String,
    pub(crate) resource_type: Option<String>,
    pub(crate) checksum: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct CurrentNodeStatePlanningRecord {
    pub(crate) unique_id: String,
    pub(crate) checksum: Option<String>,
    pub(crate) last_success_at: Option<chrono::DateTime<Utc>>,
}

pub(crate) struct InvocationListFilters<'a> {
    pub(crate) display_statuses: &'a [String],
    pub(crate) execution_modes: &'a [String],
    pub(crate) worker_queues: &'a [String],
    pub(crate) claimed_bys: &'a [String],
}

// --- Model UI records ---

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct ModelSummaryRecord {
    pub(crate) unique_id: String,
    pub(crate) node_name: Option<String>,
    pub(crate) node_path: Option<String>,
    pub(crate) resource_type: Option<String>,
    pub(crate) status: Option<String>,
    pub(crate) materialized: Option<String>,
    pub(crate) relation_schema: Option<String>,
    pub(crate) relation_database: Option<String>,
    pub(crate) last_success_at: Option<chrono::DateTime<Utc>>,
    pub(crate) finished_at: Option<chrono::DateTime<Utc>>,
    pub(crate) execution_time_seconds: Option<f64>,
    pub(crate) package_name: Option<String>,
    pub(crate) group: Option<String>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct ModelDetailRecord {
    pub(crate) latest_manifest_node: Option<Value>,
    pub(crate) promoted_manifest_node: Option<Value>,
    pub(crate) status: Option<String>,
    pub(crate) last_success_at: Option<chrono::DateTime<Utc>>,
    pub(crate) finished_at: Option<chrono::DateTime<Utc>>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct ModelNodeExecutionRecord {
    pub(crate) run_id: Uuid,
    pub(crate) invocation_id: Option<Uuid>,
    pub(crate) status: Option<String>,
    pub(crate) started_at: Option<chrono::DateTime<Utc>>,
    pub(crate) finished_at: Option<chrono::DateTime<Utc>>,
    pub(crate) execution_time_seconds: Option<f64>,
    pub(crate) git_commit_sha: Option<String>,
    pub(crate) command: String,
}

#[derive(Debug, Clone)]
pub(crate) struct LineageNodeRecord {
    pub(crate) unique_id: String,
    pub(crate) name: Option<String>,
    pub(crate) resource_type: Option<String>,
    pub(crate) package_name: Option<String>,
    pub(crate) status: Option<String>,
    pub(crate) materialized: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct ModelLineageRecord {
    pub(crate) nodes: Vec<LineageNodeRecord>,
    pub(crate) edges: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct ModelTestRecord {
    pub(crate) unique_id: String,
    pub(crate) name: Option<String>,
    pub(crate) test_type: Option<String>,
    pub(crate) status: Option<String>,
    pub(crate) finished_at: Option<chrono::DateTime<Utc>>,
    pub(crate) last_success_at: Option<chrono::DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub(crate) struct ModelHistoryRecord {
    pub(crate) run_id: Uuid,
    pub(crate) checksum: Option<String>,
    pub(crate) prev_checksum: Option<String>,
    pub(crate) git_commit_sha: Option<String>,
    pub(crate) git_repo_url: Option<String>,
    pub(crate) started_at: chrono::DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_status_roundtrips_through_parse_and_as_str() {
        for status in [
            PlanStatus::Planned,
            PlanStatus::Blocked,
            PlanStatus::Admitted,
            PlanStatus::Completed,
            PlanStatus::Failed,
            PlanStatus::Canceled,
            PlanStatus::Superseded,
        ] {
            assert_eq!(PlanStatus::parse(status.as_str()), Some(status));
            assert_eq!(status.to_string(), status.as_str());
        }
    }

    #[test]
    fn plan_status_parse_returns_none_for_unknown() {
        assert_eq!(PlanStatus::parse("unknown"), None);
        assert_eq!(PlanStatus::parse(""), None);
    }

    #[test]
    fn plan_status_terminal_states() {
        assert!(PlanStatus::Completed.is_terminal());
        assert!(PlanStatus::Failed.is_terminal());
        assert!(PlanStatus::Canceled.is_terminal());
        assert!(PlanStatus::Superseded.is_terminal());
        assert!(!PlanStatus::Planned.is_terminal());
        assert!(!PlanStatus::Blocked.is_terminal());
        assert!(!PlanStatus::Admitted.is_terminal());
    }

    #[test]
    fn plan_status_admissible_states() {
        assert!(PlanStatus::Planned.is_admissible());
        assert!(PlanStatus::Blocked.is_admissible());
        assert!(!PlanStatus::Admitted.is_admissible());
        assert!(!PlanStatus::Completed.is_admissible());
        assert!(!PlanStatus::Failed.is_admissible());
    }

    #[test]
    fn environment_status_roundtrips() {
        for (s, expected) in [
            ("active", Some(EnvironmentStatus::Active)),
            ("archived", Some(EnvironmentStatus::Archived)),
            ("failed", Some(EnvironmentStatus::Failed)),
            ("deleting", Some(EnvironmentStatus::Deleting)),
            ("unknown", None),
        ] {
            assert_eq!(EnvironmentStatus::parse(s), expected);
        }
        assert_eq!(EnvironmentStatus::Active.as_str(), "active");
        assert_eq!(EnvironmentStatus::Active.to_string(), "active");
    }

    #[test]
    fn draft_status_roundtrips() {
        for status in [
            DraftStatus::Draft,
            DraftStatus::LoadingGit,
            DraftStatus::Ready,
            DraftStatus::Validating,
            DraftStatus::Validated,
            DraftStatus::Failed,
        ] {
            assert_eq!(DraftStatus::parse(status.as_str()), Some(status));
        }
        assert_eq!(DraftStatus::parse("bogus"), None);
    }

    #[test]
    fn draft_status_terminal() {
        assert!(DraftStatus::Validated.is_terminal());
        assert!(DraftStatus::Failed.is_terminal());
        assert!(!DraftStatus::Draft.is_terminal());
        assert!(!DraftStatus::Validating.is_terminal());
    }

    #[test]
    fn preparation_status_roundtrips() {
        for status in [
            PreparationStatus::Running,
            PreparationStatus::Succeeded,
            PreparationStatus::Failed,
        ] {
            assert_eq!(PreparationStatus::parse(status.as_str()), Some(status));
        }
        assert_eq!(PreparationStatus::parse("invalid"), None);
    }

    #[test]
    fn node_execution_status_roundtrips() {
        for status in [
            NodeExecutionStatus::Success,
            NodeExecutionStatus::Pass,
            NodeExecutionStatus::Created,
            NodeExecutionStatus::Error,
            NodeExecutionStatus::Fail,
            NodeExecutionStatus::Skipped,
        ] {
            assert_eq!(NodeExecutionStatus::parse(status.as_str()), Some(status));
            assert_eq!(status.to_string(), status.as_str());
        }
    }

    #[test]
    fn node_execution_status_parse_aliases() {
        assert_eq!(
            NodeExecutionStatus::parse("failed"),
            Some(NodeExecutionStatus::Fail)
        );
        assert_eq!(NodeExecutionStatus::parse("unknown"), None);
    }

    #[test]
    fn node_execution_status_promotable() {
        assert!(NodeExecutionStatus::Success.is_promotable());
        assert!(NodeExecutionStatus::Pass.is_promotable());
        assert!(NodeExecutionStatus::Created.is_promotable());
        assert!(!NodeExecutionStatus::Error.is_promotable());
        assert!(!NodeExecutionStatus::Fail.is_promotable());
        assert!(!NodeExecutionStatus::Skipped.is_promotable());
    }
}
