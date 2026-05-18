//! Domain services: invocation orchestration, reconciliation planning, and environment management.
use crate::config::InvocationContext;
use crate::db::{
    CreateEnvironmentDraftInput, CreateEnvironmentRunPlanInput, CreateProjectDraftInput,
    CreateProjectInput, CurrentNodeStatePlanningRecord, Db, EnvironmentActualStateRecord,
    EnvironmentDraftRecord, EnvironmentRecord, EnvironmentReleaseInput, EnvironmentRunPlanRecord,
    EquivalentPlanLookup, GitState, PlanStatus, PlanningManifestNodeRecord, ProjectDraftRecord,
    ProjectRecord, RunStart, SourceStateEventCreateInput, SourceStateEventRecord,
    UpdateEnvironmentDraftInput,
};
use crate::dbt_utils::{
    append_invocation_id, build_generated_profiles, git_repo_root, read_git_state,
};
use crate::error::{AppError, AppResult};
use crate::execution::ExecutionMode;
use crate::manifest::ReconstructedManifest;
use crate::profile::{
    EnvironmentProfileRecord, resolve_runtime_profile, validate_environment_profile,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::path::Component;
use std::path::{Path, PathBuf};
use uuid::Uuid;

const RECONCILE_LEASE_DURATION: std::time::Duration = std::time::Duration::from_secs(30);

#[derive(Debug, Clone, Copy)]
pub enum InvocationCommand {
    Build,
    Run,
    Ls,
    Test,
    Seed,
    Release,
    ProjectValidate,
    EnvironmentPrepare,
    EnvironmentValidate,
    ManifestPrepare,
}

impl InvocationCommand {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Build => "build",
            Self::Run => "run",
            Self::Ls => "ls",
            Self::Test => "test",
            Self::Seed => "seed",
            Self::Release => "release",
            Self::ProjectValidate => "project_validate",
            Self::EnvironmentPrepare => "environment_prepare",
            Self::EnvironmentValidate => "environment_validate",
            Self::ManifestPrepare => "manifest_prepare",
        }
    }

    pub fn persists_state(self) -> bool {
        !matches!(
            self,
            Self::Ls
                | Self::Release
                | Self::ProjectValidate
                | Self::EnvironmentPrepare
                | Self::EnvironmentValidate
        )
    }
}

impl From<crate::api::InvocationCommandApi> for InvocationCommand {
    fn from(command: crate::api::InvocationCommandApi) -> Self {
        match command {
            crate::api::InvocationCommandApi::Build => Self::Build,
            crate::api::InvocationCommandApi::Run => Self::Run,
            crate::api::InvocationCommandApi::Ls => Self::Ls,
            crate::api::InvocationCommandApi::Test => Self::Test,
            crate::api::InvocationCommandApi::Seed => Self::Seed,
            crate::api::InvocationCommandApi::Release => Self::Release,
            crate::api::InvocationCommandApi::ProjectValidate => Self::ProjectValidate,
            crate::api::InvocationCommandApi::EnvironmentPrepare => Self::EnvironmentPrepare,
            crate::api::InvocationCommandApi::EnvironmentValidate => Self::EnvironmentValidate,
            crate::api::InvocationCommandApi::ManifestPrepare => Self::ManifestPrepare,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RemoteExecutionSpec {
    pub command: InvocationCommand,
    pub args: Vec<OsString>,
    pub repo_url: String,
    pub commit_sha: String,
    pub project_root: String,
    pub profiles_yml: String,
    pub state_manifest: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct ReleaseValidationSpec {
    pub repo_url: String,
    pub git_ref: Option<String>,
    pub git_commit_sha: Option<String>,
    pub git_branch: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ProjectValidationSpec {
    pub repo_url: String,
    pub project_root: String,
}

#[derive(Debug, Clone)]
pub struct EnvironmentPrepareSpec {
    pub repo_url: String,
    pub selected_branch: Option<String>,
}

#[derive(Debug, Clone)]
pub struct EnvironmentValidationSpec {
    pub repo_url: String,
    pub commit_sha: String,
    pub project_root: String,
    pub selected_branch: Option<String>,
    pub profiles_yml: String,
}

#[derive(Debug, Clone)]
pub struct LocalExecutionSpec {
    pub command: InvocationCommand,
    pub args: Vec<OsString>,
    pub state_manifest: Option<Value>,
}

#[derive(Debug, Clone)]
pub enum PreparedExecutionSpec {
    Remote(RemoteExecutionSpec),
    Local(LocalExecutionSpec),
    ReleaseValidation(ReleaseValidationSpec),
    ProjectValidation(ProjectValidationSpec),
    EnvironmentPrepare(EnvironmentPrepareSpec),
    EnvironmentValidate(EnvironmentValidationSpec),
}

#[derive(Debug, Clone)]
pub struct LocalExecutionPrepared {
    pub spec: PreparedExecutionSpec,
    pub persistence: Option<LocalExecutionPersistence>,
    pub worker_queue: String,
    pub project_id: Option<i64>,
    pub environment_id: Option<i64>,
    pub project_draft_id: Option<Uuid>,
    pub environment_draft_id: Option<Uuid>,
}

#[derive(Debug, Clone)]
pub struct LocalExecutionPersistence {
    pub run_id: Uuid,
    pub project_id: i64,
    pub environment_id: i64,
    pub subcommand: String,
    pub promote_base_manifest: bool,
    pub updates_actual_state: bool,
}

#[derive(Debug, Clone)]
pub struct ProjectCreateRequest {
    pub git_repo_url: String,
    pub project_root: String,
}

#[derive(Debug, Clone)]
pub struct ProjectDraftValidationPrepared {
    pub draft: ProjectDraftRecord,
    pub invocation_id: Uuid,
    pub spec: ProjectValidationSpec,
    pub worker_queue: String,
}

#[derive(Debug, Clone)]
pub struct EnvironmentDraftCreatePrepared {
    pub draft: EnvironmentDraftRecord,
    pub invocation_id: Uuid,
    pub spec: EnvironmentPrepareSpec,
    pub worker_queue: String,
}

#[derive(Debug, Clone)]
pub struct EnvironmentDraftValidationPrepared {
    pub draft: EnvironmentDraftRecord,
    pub invocation_id: Uuid,
    pub spec: EnvironmentValidationSpec,
    pub worker_queue: String,
}

#[derive(Debug, Clone)]
pub struct EnvironmentDraftUpdateRequest {
    pub project: String,
    pub slug: String,
    pub git_branch: Option<String>,
    pub git_commit_sha: Option<String>,
    pub use_latest_commit: bool,
    pub auto_reconcile: bool,
    pub immutable: bool,
    pub adapter_type: String,
    pub schema_name: String,
    pub threads: Option<i32>,
    pub profile_config: Value,
    pub profile_secrets: Value,
}

#[derive(Debug, Clone)]
pub struct ProjectUpdateRequest {
    pub project: String,
    pub git_repo_url: Option<String>,
    pub project_root: Option<String>,
}

#[derive(Debug, Clone)]
pub struct EnvironmentReleaseRequest {
    pub project: String,
    pub slug: String,
    pub git_branch: Option<String>,
    pub git_commit_sha: Option<String>,
}

#[derive(Debug, Clone)]
pub struct EnvironmentRollbackRequest {
    pub project: String,
    pub slug: String,
    pub version_id: i64,
}

#[derive(Debug, Clone)]
pub struct SourceStateEventCreateRequest {
    pub project: String,
    pub slug: String,
    pub source_key: String,
    pub provider: String,
    pub state_version: Option<String>,
    pub observed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub payload: Value,
}

#[derive(Debug, Clone)]
pub struct EnvironmentPlanAdmission {
    pub plan: EnvironmentRunPlanRecord,
    pub invocation_id: Option<Uuid>,
}

/// Capability trait for starting prepared invocations.
///
/// Services accept this trait to complete full workflows (e.g. admit + start)
/// without needing direct access to AppState or the InvocationManager.
pub trait InvocationStarter: Send + Sync {
    fn start_prepared_invocation(
        &self,
        invocation_id: Uuid,
        command: crate::api::InvocationCommandApi,
        plan_id: Option<Uuid>,
        prepared: LocalExecutionPrepared,
    ) -> impl std::future::Future<Output = AppResult<Uuid>> + Send;
}

/// Capability trait for querying node staleness relative to source watermarks.
///
/// Planning logic uses this to determine which downstream nodes need
/// re-execution after source state changes, without reaching into the
/// DB layer directly.
pub trait StalenessOracle: Send + Sync {
    /// Return unique_ids of downstream nodes that are stale relative to the
    /// given source events.
    fn list_stale_downstream_nodes(
        &self,
        project_id: i64,
        environment_id: i64,
        source_keys: &[String],
        target_event_ids: &[i64],
        manifest_run_id: Uuid,
    ) -> impl std::future::Future<Output = AppResult<Vec<String>>> + Send;
}

#[derive(Debug, Clone)]
struct ReleaseTargetRequest {
    git_branch: Option<String>,
    git_commit_sha: Option<String>,
    git_ref: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct InferredProjectInput {
    pub project_id: String,
    pub project_name: String,
    pub mode: String,
    pub git_repo_url: Option<String>,
    pub default_branch: Option<String>,
    pub project_root: Option<String>,
}

mod environments;
mod invocations;
mod planning;
mod prepared_execution;
mod projects;
pub(crate) mod source_state;

pub use environments::{EnvironmentPlanAdmitPrepared, EnvironmentService};
pub use invocations::InvocationService;
pub use invocations::{
    code_change_input_fingerprint, code_change_input_fingerprint_for_baseline,
    source_state_change_input_fingerprint, target_manifest_input_fingerprint,
};
pub use projects::ProjectService;

pub fn infer_local_project_defaults(
    current_dir: &Path,
    git_repo_url: Option<&str>,
    project_root: Option<&str>,
    default_branch: Option<&str>,
) -> AppResult<InferredProjectInput> {
    let project_name = read_dbt_project_name_from_root(current_dir)?;
    let canonical_project_dir = current_dir.canonicalize()?;
    let identity_hash = infer_local_identity_hash(current_dir, &project_name)?;
    let project_id = format!("prj_local_{identity_hash}");
    let git_state = read_git_state(current_dir);

    Ok(InferredProjectInput {
        project_id,
        project_name,
        mode: "local".to_string(),
        git_repo_url: git_repo_url.map(ToString::to_string).or(git_state.repo_url),
        default_branch: default_branch.map(ToString::to_string),
        project_root: project_root
            .map(ToString::to_string)
            .or_else(|| Some(canonical_project_dir.display().to_string())),
    })
}

pub fn infer_remote_project_defaults(
    current_dir: &Path,
    git_repo_url: Option<&str>,
    project_root: Option<&str>,
    default_branch: Option<&str>,
) -> AppResult<InferredProjectInput> {
    let project_name = read_dbt_project_name_from_root(current_dir)?;
    let canonical_project_dir = current_dir.canonicalize()?;
    let git_state = read_git_state(current_dir);
    let repo_url = git_repo_url
        .map(ToString::to_string)
        .or(git_state.repo_url)
        .ok_or(AppError::RemoteProjectRequiresGitRepo)?;
    let repo_root =
        git_repo_root(current_dir).map_err(|_| AppError::RemoteProjectRequiresGitRepo)?;
    let inferred_project_root = project_root
        .map(ToString::to_string)
        .unwrap_or_else(|| relative_project_root(&repo_root, &canonical_project_dir));
    validate_remote_project_root(&inferred_project_root)?;
    let project_id = crate::db::remote_project_id(&repo_url, &inferred_project_root, &project_name);

    Ok(InferredProjectInput {
        project_id,
        project_name,
        mode: "remote".to_string(),
        git_repo_url: Some(repo_url),
        default_branch: default_branch.map(ToString::to_string),
        project_root: Some(inferred_project_root),
    })
}

pub fn read_dbt_project_name_from_root(project_root: &Path) -> AppResult<String> {
    let yaml = read_dbt_project_yaml(project_root)?;
    yaml.get("name")
        .and_then(serde_yaml::Value::as_str)
        .map(ToString::to_string)
        .or_else(|| {
            project_root
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
        })
        .ok_or(AppError::NotDbtProjectRoot)
}

pub fn relative_project_root(repo_root: &Path, project_root: &Path) -> String {
    match project_root.strip_prefix(repo_root) {
        Ok(path) if path.as_os_str().is_empty() => ".".to_string(),
        Ok(path) => path.to_string_lossy().into_owned(),
        Err(_) => project_root.to_string_lossy().into_owned(),
    }
}

fn read_dbt_project_yaml(project_root: &Path) -> AppResult<serde_yaml::Value> {
    let path = project_root.join("dbt_project.yml");
    if !path.is_file() {
        return Err(AppError::NotDbtProjectRoot);
    }
    let content = std::fs::read_to_string(path)?;
    Ok(serde_yaml::from_str(&content)?)
}

pub fn validate_remote_project_root(project_root: &str) -> AppResult<()> {
    let path = Path::new(project_root);
    if path.is_absolute() {
        return Err(AppError::InvalidRemoteProjectRoot(project_root.to_string()));
    }
    if path
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(AppError::InvalidRemoteProjectRoot(project_root.to_string()));
    }
    Ok(())
}

fn validate_release_target_request(
    request: ReleaseTargetRequest,
) -> AppResult<ReleaseTargetRequest> {
    if request.git_commit_sha.is_some() == request.git_ref.is_some() {
        return Err(AppError::InvalidReleaseTarget(
            "provide exactly one of --git-commit-sha or --git-ref".to_string(),
        ));
    }
    if let Some(git_commit_sha) = request.git_commit_sha.as_deref()
        && !crate::db::is_valid_git_commit_sha(git_commit_sha)
    {
        return Err(AppError::InvalidReleaseTarget(format!(
            "invalid git commit sha '{git_commit_sha}': expected 7 to 64 hexadecimal characters"
        )));
    }
    Ok(request)
}

fn parse_release_target_args(args: &[OsString]) -> AppResult<ReleaseTargetRequest> {
    let mut git_branch = None;
    let mut git_commit_sha = None;
    let mut git_ref = None;
    let mut idx = 0;
    while idx < args.len() {
        match args[idx].to_string_lossy().as_ref() {
            "--git-branch" => {
                idx += 1;
                let value = args.get(idx).ok_or_else(|| {
                    AppError::InvalidReleaseTarget("--git-branch requires a value".to_string())
                })?;
                git_branch = Some(value.to_string_lossy().into_owned());
            }
            "--git-commit-sha" => {
                idx += 1;
                let value = args.get(idx).ok_or_else(|| {
                    AppError::InvalidReleaseTarget("--git-commit-sha requires a value".to_string())
                })?;
                git_commit_sha = Some(value.to_string_lossy().into_owned());
            }
            "--git-ref" => {
                idx += 1;
                let value = args.get(idx).ok_or_else(|| {
                    AppError::InvalidReleaseTarget("--git-ref requires a value".to_string())
                })?;
                git_ref = Some(value.to_string_lossy().into_owned());
            }
            other => {
                return Err(AppError::InvalidReleaseTarget(format!(
                    "unsupported release argument '{other}'"
                )));
            }
        }
        idx += 1;
    }
    validate_release_target_request(ReleaseTargetRequest {
        git_branch,
        git_commit_sha,
        git_ref,
    })
}

pub fn local_machine_scope() -> AppResult<String> {
    if let Ok(value) = std::env::var("DBTX_LOCAL_MACHINE_ID")
        && !value.trim().is_empty()
    {
        return Ok(value);
    }

    for path in ["/etc/machine-id", "/var/lib/dbus/machine-id"] {
        if let Ok(value) = std::fs::read_to_string(path) {
            let value = value.trim();
            if !value.is_empty() {
                return Ok(value.to_string());
            }
        }
    }

    if let Ok(value) = std::env::var("HOSTNAME")
        && !value.trim().is_empty()
    {
        return Ok(value);
    }

    if let Ok(value) = std::fs::read_to_string("/etc/hostname") {
        let value = value.trim();
        if !value.is_empty() {
            return Ok(value.to_string());
        }
    }

    Err(AppError::Internal(
        "failed to determine local machine scope".to_string(),
    ))
}

fn validation_worker_queue() -> String {
    validation_worker_queue_from_env(std::env::var("DBTX_VALIDATION_QUEUE").ok().as_deref())
}

fn validation_worker_queue_from_env(value: Option<&str>) -> String {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("generic")
        .to_string()
}

fn infer_local_identity_hash(current_dir: &Path, project_name: &str) -> AppResult<String> {
    let canonical_project_dir = current_dir.canonicalize()?;
    let machine_scope = local_machine_scope()?;
    Ok(short_hash(&format!(
        "{machine_scope}\n{}\n{project_name}",
        canonical_project_dir.display()
    )))
}

fn short_hash(input: &str) -> String {
    let digest = Sha256::digest(input.as_bytes());
    format!("{digest:x}").chars().take(20).collect()
}

#[cfg(test)]
mod tests {
    use super::{parse_release_target_args, validation_worker_queue_from_env};
    use crate::db::is_valid_git_commit_sha;
    use std::ffi::OsString;

    #[test]
    fn release_commit_sha_requires_hex_shape() {
        assert!(is_valid_git_commit_sha("deadbeef"));
        assert!(is_valid_git_commit_sha(
            "0123456789abcdef0123456789abcdef01234567"
        ));
        assert!(!is_valid_git_commit_sha("abc123"));
        assert!(!is_valid_git_commit_sha("main"));
        assert!(!is_valid_git_commit_sha("dead beef"));
    }

    #[test]
    fn release_target_args_reject_malformed_commit_sha() {
        let args = vec![
            OsString::from("--git-commit-sha"),
            OsString::from("not-a-sha"),
        ];
        let error = parse_release_target_args(&args).expect_err("expected malformed sha error");
        assert!(
            error
                .to_string()
                .contains("invalid git commit sha 'not-a-sha'")
        );
    }

    #[test]
    fn release_target_args_rejects_missing_and_duplicate_target() {
        let error = parse_release_target_args(&[]).expect_err("expected missing target error");
        assert!(
            error
                .to_string()
                .contains("provide exactly one of --git-commit-sha or --git-ref")
        );

        let args = vec![
            OsString::from("--git-commit-sha"),
            OsString::from("deadbeef"),
            OsString::from("--git-ref"),
            OsString::from("main"),
        ];
        let error = parse_release_target_args(&args).expect_err("expected duplicate target error");
        assert!(
            error
                .to_string()
                .contains("provide exactly one of --git-commit-sha or --git-ref")
        );
    }

    #[test]
    fn validation_worker_queue_defaults_and_trims() {
        assert_eq!(validation_worker_queue_from_env(None), "generic");
        assert_eq!(validation_worker_queue_from_env(Some("")), "generic");
        assert_eq!(validation_worker_queue_from_env(Some("   ")), "generic");
        assert_eq!(
            validation_worker_queue_from_env(Some("  validation-q  ")),
            "validation-q"
        );
    }

    #[test]
    fn input_fingerprint_deterministic() {
        use super::{code_change_input_fingerprint, source_state_change_input_fingerprint};
        let id = uuid::Uuid::nil();
        let fp1 = code_change_input_fingerprint("abc123", id);
        let fp2 = code_change_input_fingerprint("abc123", id);
        assert_eq!(fp1, fp2);

        let sfp1 = source_state_change_input_fingerprint(&[3, 1, 2]);
        let sfp2 = source_state_change_input_fingerprint(&[1, 2, 3]);
        assert_eq!(sfp1, sfp2);
    }
}
