//! Domain services: invocation orchestration, reconciliation planning, and environment management.
use crate::config::{InvocationContext, RuntimeConfig};
use crate::db::{
    CreateEnvironmentDraftInput, CreateEnvironmentRunPlanInput, CreateProjectDraftInput,
    CreateProjectInput, CurrentNodeStatePlanningRecord, Db, EnvironmentActualStateRecord,
    EnvironmentDraftRecord, EnvironmentRecord, EnvironmentReleaseInput, EnvironmentRunPlanRecord,
    EnvironmentVersionRecord, EquivalentPlanLookup, GitState, LocalEnvironmentUpsertInput,
    PlanStatus, PlanningManifestNodeRecord, ProjectDraftRecord, ProjectRecord, RunFinalization,
    RunStart, SourceStateEventCreateInput, SourceStateEventRecord, UpdateEnvironmentDraftInput,
};
use crate::dbt_utils::{
    append_invocation_id, append_profiles_dir, append_state_dir, build_generated_profiles,
    git_repo_root, read_dbt_project_name, read_git_state,
};
use crate::error::{AppError, AppResult};
use crate::event::LogEvent;
use crate::execution::ExecutionMode;
use crate::manifest::{ManifestSnapshot, ReconstructedManifest};
use crate::profile::{
    EnvironmentProfileRecord, LocalTargetProfile, resolve_runtime_profile,
    validate_environment_profile,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::path::Component;
use std::path::{Path, PathBuf};
use uuid::Uuid;

const RECONCILE_LEASE_DURATION: std::time::Duration = std::time::Duration::from_secs(30);

pub trait InvocationObserver {
    fn stdout_line(&mut self, line: &str);
    fn stderr_line(&mut self, line: &str);
    fn dbt_log(&mut self, _event: &LogEvent, _rendered: Option<&str>) {}
}

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

#[derive(Debug, Clone)]
pub struct InvocationRequest {
    pub command: InvocationCommand,
    pub args: Vec<OsString>,
    pub config: RuntimeConfig,
    pub current_dir: Option<PathBuf>,
    pub environment_slug: String,
    pub execution_mode: ExecutionMode,
}

#[derive(Debug, Clone)]
#[must_use]
pub struct InvocationResult {
    pub exit_code: i32,
}

#[derive(Debug, Clone)]
pub struct LocalExecutionSpec {
    pub command: InvocationCommand,
    pub args: Vec<OsString>,
    pub project_dir: PathBuf,
    pub profiles_yml: String,
    pub state_manifest: Option<Value>,
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
pub enum PreparedExecutionSpec {
    Local(LocalExecutionSpec),
    Remote(RemoteExecutionSpec),
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

pub fn infer_local_worker_queue(current_dir: &Path) -> AppResult<String> {
    let project_name = read_dbt_project_name_from_root(current_dir)?;
    let identity_hash = infer_local_identity_hash(current_dir, &project_name)?;
    Ok(format!("local-{identity_hash}"))
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

fn local_machine_scope() -> AppResult<String> {
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

fn plan_code_change_selected_resources(
    baseline_nodes: &[PlanningManifestNodeRecord],
    target_nodes: &[PlanningManifestNodeRecord],
    target_edges: &[(String, String)],
    current_nodes: &[CurrentNodeStatePlanningRecord],
) -> Vec<String> {
    let baseline_checksums = baseline_nodes
        .iter()
        .map(|node| (node.unique_id.clone(), node.checksum.clone()))
        .collect::<BTreeMap<_, _>>();
    let target_by_id = target_nodes
        .iter()
        .map(|node| (node.unique_id.clone(), node))
        .collect::<BTreeMap<_, _>>();
    let current_by_id = current_nodes
        .iter()
        .map(|node| (node.unique_id.clone(), node))
        .collect::<BTreeMap<_, _>>();

    let directly_modified = target_nodes
        .iter()
        .filter(|node| is_build_plannable_resource_type(node.resource_type.as_deref()))
        .filter(|node| baseline_checksums.get(&node.unique_id).cloned().flatten() != node.checksum)
        .map(|node| node.unique_id.clone())
        .collect::<BTreeSet<_>>();

    if directly_modified.is_empty() {
        return Vec::new();
    }

    let mut child_map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut parent_map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (parent, child) in target_edges {
        child_map
            .entry(parent.clone())
            .or_default()
            .push(child.clone());
        parent_map
            .entry(child.clone())
            .or_default()
            .push(parent.clone());
    }

    let mut candidate = directly_modified.clone();
    let mut stack = directly_modified.iter().cloned().collect::<Vec<_>>();
    while let Some(parent) = stack.pop() {
        for child in child_map.get(&parent).into_iter().flatten() {
            if target_by_id.contains_key(child) && candidate.insert(child.clone()) {
                stack.push(child.clone());
            }
        }
    }

    let mut memo: BTreeMap<String, AncestorRequirement> = BTreeMap::new();
    let mut selected = candidate
        .iter()
        .filter_map(|unique_id| {
            let target = target_by_id.get(unique_id)?;
            let current = current_by_id.get(unique_id).copied();
            let requirement = compute_ancestor_requirement(
                unique_id,
                &candidate,
                &directly_modified,
                &parent_map,
                &target_by_id,
                &current_by_id,
                &mut memo,
            );
            let current_checksum = current.and_then(|node| node.checksum.clone());
            let current_success_at = current.and_then(|node| node.last_success_at);
            let matches_target = current_checksum == target.checksum;
            let is_stale = !matches_target
                || requirement.has_unreconciled_ancestor
                || requirement
                    .latest_reconciled_ancestor_success_at
                    .is_some_and(|ancestor_time| {
                        current_success_at
                            .map(|node_time| node_time < ancestor_time)
                            .unwrap_or(true)
                    });
            is_stale.then(|| unique_id.clone())
        })
        .collect::<Vec<_>>();
    selected.sort();
    selected
}

#[derive(Clone, Copy, Default)]
struct AncestorRequirement {
    has_unreconciled_ancestor: bool,
    latest_reconciled_ancestor_success_at: Option<chrono::DateTime<chrono::Utc>>,
}

fn compute_ancestor_requirement(
    unique_id: &str,
    candidate: &BTreeSet<String>,
    directly_modified: &BTreeSet<String>,
    parent_map: &BTreeMap<String, Vec<String>>,
    target_by_id: &BTreeMap<String, &PlanningManifestNodeRecord>,
    current_by_id: &BTreeMap<String, &CurrentNodeStatePlanningRecord>,
    memo: &mut BTreeMap<String, AncestorRequirement>,
) -> AncestorRequirement {
    if let Some(existing) = memo.get(unique_id).copied() {
        return existing;
    }

    let mut requirement = AncestorRequirement::default();
    if directly_modified.contains(unique_id) {
        let target_checksum = target_by_id
            .get(unique_id)
            .and_then(|node| node.checksum.clone());
        let current = current_by_id.get(unique_id).copied();
        let current_checksum = current.and_then(|node| node.checksum.clone());
        let current_success_at = current.and_then(|node| node.last_success_at);
        let root_reconciled = current_checksum == target_checksum && current_success_at.is_some();
        if root_reconciled {
            requirement.latest_reconciled_ancestor_success_at = current_success_at;
        } else {
            requirement.has_unreconciled_ancestor = true;
        }
    }

    for parent in parent_map.get(unique_id).into_iter().flatten() {
        if !candidate.contains(parent) {
            continue;
        }
        let parent_requirement = compute_ancestor_requirement(
            parent,
            candidate,
            directly_modified,
            parent_map,
            target_by_id,
            current_by_id,
            memo,
        );
        requirement.has_unreconciled_ancestor |= parent_requirement.has_unreconciled_ancestor;
        requirement.latest_reconciled_ancestor_success_at = match (
            requirement.latest_reconciled_ancestor_success_at,
            parent_requirement.latest_reconciled_ancestor_success_at,
        ) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (Some(left), None) => Some(left),
            (None, Some(right)) => Some(right),
            (None, None) => None,
        };
    }

    memo.insert(unique_id.to_string(), requirement);
    requirement
}

fn is_build_plannable_resource_type(resource_type: Option<&str>) -> bool {
    matches!(
        resource_type,
        Some("model" | "seed" | "snapshot" | "test" | "unit_test")
    )
}

fn plan_source_event_ids(source_event_id: Option<i64>, metadata: &Value) -> Vec<i64> {
    let mut event_ids = metadata
        .get("source_event_ids")
        .and_then(|value| value.as_array())
        .into_iter()
        .flatten()
        .filter_map(|value| value.as_i64())
        .collect::<Vec<_>>();
    if event_ids.is_empty()
        && let Some(source_event_id) = source_event_id
    {
        event_ids.push(source_event_id);
    }
    event_ids.sort_unstable();
    event_ids.dedup();
    event_ids
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
    use super::{
        parse_release_target_args, plan_code_change_selected_resources, plan_source_event_ids,
        validation_worker_queue_from_env,
    };
    use crate::db::{
        CurrentNodeStatePlanningRecord, PlanningManifestNodeRecord, is_valid_git_commit_sha,
    };
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
    fn code_change_planning_uses_live_current_state_for_completed_roots() {
        let baseline = vec![
            PlanningManifestNodeRecord {
                unique_id: "model.pkg.orders".to_string(),
                resource_type: Some("model".to_string()),
                checksum: Some("old-orders".to_string()),
            },
            PlanningManifestNodeRecord {
                unique_id: "model.pkg.customers".to_string(),
                resource_type: Some("model".to_string()),
                checksum: Some("same-customers".to_string()),
            },
        ];
        let target = vec![
            PlanningManifestNodeRecord {
                unique_id: "model.pkg.orders".to_string(),
                resource_type: Some("model".to_string()),
                checksum: Some("new-orders".to_string()),
            },
            PlanningManifestNodeRecord {
                unique_id: "model.pkg.customers".to_string(),
                resource_type: Some("model".to_string()),
                checksum: Some("same-customers".to_string()),
            },
        ];
        let edges = vec![(
            "model.pkg.orders".to_string(),
            "model.pkg.customers".to_string(),
        )];
        let current = vec![
            CurrentNodeStatePlanningRecord {
                unique_id: "model.pkg.orders".to_string(),
                checksum: Some("new-orders".to_string()),
                last_success_at: Some(chrono::Utc::now()),
            },
            CurrentNodeStatePlanningRecord {
                unique_id: "model.pkg.customers".to_string(),
                checksum: Some("same-customers".to_string()),
                last_success_at: Some(chrono::Utc::now() - chrono::Duration::minutes(5)),
            },
        ];

        let selected = plan_code_change_selected_resources(&baseline, &target, &edges, &current);
        assert_eq!(selected, vec!["model.pkg.customers".to_string()]);
    }

    #[test]
    fn code_change_planning_selects_modified_node_and_downstream() {
        let baseline = vec![
            PlanningManifestNodeRecord {
                unique_id: "model.pkg.stg_orders".to_string(),
                resource_type: Some("model".to_string()),
                checksum: Some("old-stg".to_string()),
            },
            PlanningManifestNodeRecord {
                unique_id: "model.pkg.orders".to_string(),
                resource_type: Some("model".to_string()),
                checksum: Some("orders-v1".to_string()),
            },
            PlanningManifestNodeRecord {
                unique_id: "model.pkg.revenue".to_string(),
                resource_type: Some("model".to_string()),
                checksum: Some("revenue-v1".to_string()),
            },
        ];
        let target = vec![
            PlanningManifestNodeRecord {
                unique_id: "model.pkg.stg_orders".to_string(),
                resource_type: Some("model".to_string()),
                checksum: Some("new-stg".to_string()),
            },
            PlanningManifestNodeRecord {
                unique_id: "model.pkg.orders".to_string(),
                resource_type: Some("model".to_string()),
                checksum: Some("orders-v1".to_string()),
            },
            PlanningManifestNodeRecord {
                unique_id: "model.pkg.revenue".to_string(),
                resource_type: Some("model".to_string()),
                checksum: Some("revenue-v1".to_string()),
            },
        ];
        let edges = vec![
            (
                "model.pkg.stg_orders".to_string(),
                "model.pkg.orders".to_string(),
            ),
            (
                "model.pkg.orders".to_string(),
                "model.pkg.revenue".to_string(),
            ),
        ];
        let current = vec![]; // No current state = never run

        let selected = plan_code_change_selected_resources(&baseline, &target, &edges, &current);
        assert_eq!(
            selected,
            vec![
                "model.pkg.orders".to_string(),
                "model.pkg.revenue".to_string(),
                "model.pkg.stg_orders".to_string(),
            ]
        );
    }

    #[test]
    fn code_change_planning_returns_empty_when_no_changes() {
        let nodes = vec![PlanningManifestNodeRecord {
            unique_id: "model.pkg.orders".to_string(),
            resource_type: Some("model".to_string()),
            checksum: Some("same".to_string()),
        }];
        let selected = plan_code_change_selected_resources(&nodes, &nodes, &[], &[]);
        assert!(selected.is_empty());
    }

    #[test]
    fn code_change_planning_skips_non_plannable_resource_types() {
        let baseline = vec![PlanningManifestNodeRecord {
            unique_id: "source.pkg.raw_orders".to_string(),
            resource_type: Some("source".to_string()),
            checksum: Some("old".to_string()),
        }];
        let target = vec![PlanningManifestNodeRecord {
            unique_id: "source.pkg.raw_orders".to_string(),
            resource_type: Some("source".to_string()),
            checksum: Some("new".to_string()),
        }];
        let selected = plan_code_change_selected_resources(&baseline, &target, &[], &[]);
        assert!(selected.is_empty());
    }

    #[test]
    fn code_change_planning_includes_new_nodes_not_in_baseline() {
        let baseline = vec![];
        let target = vec![PlanningManifestNodeRecord {
            unique_id: "model.pkg.new_model".to_string(),
            resource_type: Some("model".to_string()),
            checksum: Some("abc".to_string()),
        }];
        let selected = plan_code_change_selected_resources(&baseline, &target, &[], &[]);
        assert_eq!(selected, vec!["model.pkg.new_model".to_string()]);
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
        assert_eq!(sfp1, sfp2); // sorted + deduped
    }

    #[test]
    fn plan_source_event_ids_reads_from_metadata_array() {
        let metadata = serde_json::json!({"source_event_ids": [5, 3, 1]});
        let ids = plan_source_event_ids(None, &metadata);
        assert_eq!(ids, vec![1, 3, 5]);
    }

    #[test]
    fn plan_source_event_ids_falls_back_to_source_event_id() {
        let metadata = serde_json::json!({});
        let ids = plan_source_event_ids(Some(42), &metadata);
        assert_eq!(ids, vec![42]);
    }

    #[test]
    fn plan_source_event_ids_empty_when_no_source() {
        let metadata = serde_json::json!({});
        let ids = plan_source_event_ids(None, &metadata);
        assert!(ids.is_empty());
    }

    #[test]
    fn plan_source_event_ids_deduplicates() {
        let metadata = serde_json::json!({"source_event_ids": [3, 3, 1, 1]});
        let ids = plan_source_event_ids(None, &metadata);
        assert_eq!(ids, vec![1, 3]);
    }

    #[test]
    fn plan_source_event_ids_ignores_fallback_when_metadata_present() {
        let metadata = serde_json::json!({"source_event_ids": [10]});
        let ids = plan_source_event_ids(Some(99), &metadata);
        assert_eq!(ids, vec![10]);
    }
}

#[cfg(test)]
mod proptests {
    use super::{is_build_plannable_resource_type, plan_code_change_selected_resources};
    use crate::db::{CurrentNodeStatePlanningRecord, PlanningManifestNodeRecord};
    use proptest::prelude::*;
    use std::collections::BTreeSet;

    fn node_id(i: usize) -> String {
        format!("model.pkg.node_{i}")
    }

    /// Generate a random DAG with n nodes and random edges (parent -> child, no cycles).
    /// Nodes are numbered 0..n, edges only go from lower to higher index (acyclic).
    fn arb_dag(
        max_nodes: usize,
    ) -> impl Strategy<Value = (Vec<PlanningManifestNodeRecord>, Vec<(String, String)>)> {
        (1..=max_nodes)
            .prop_flat_map(|n| {
                let checksums = proptest::collection::vec(proptest::option::of("[a-f0-9]{8}"), n);
                // For edges: each node i can have edges to nodes j > i
                let edge_bits = proptest::collection::vec(proptest::bool::ANY, n * n);
                (Just(n), checksums, edge_bits)
            })
            .prop_map(|(n, checksums, edge_bits)| {
                let nodes: Vec<PlanningManifestNodeRecord> = (0..n)
                    .map(|i| PlanningManifestNodeRecord {
                        unique_id: node_id(i),
                        resource_type: Some("model".to_string()),
                        checksum: checksums[i].clone(),
                    })
                    .collect();
                let mut edges = Vec::new();
                for i in 0..n {
                    for j in (i + 1)..n {
                        if edge_bits[i * n + j] {
                            edges.push((node_id(i), node_id(j)));
                        }
                    }
                }
                (nodes, edges)
            })
    }

    proptest! {
        #[test]
        fn empty_when_no_checksums_changed(
            (nodes, edges) in arb_dag(8)
        ) {
            // baseline == target means no changes
            let result = plan_code_change_selected_resources(&nodes, &nodes, &edges, &[]);
            prop_assert!(result.is_empty(), "expected empty but got {:?}", result);
        }

        #[test]
        fn output_is_sorted_and_deduplicated(
            (baseline, edges) in arb_dag(8),
            modifications in proptest::collection::vec(0..8usize, 0..4)
        ) {
            let mut target = baseline.clone();
            for &idx in &modifications {
                if idx < target.len() {
                    target[idx].checksum = Some("modified".to_string());
                }
            }
            let result = plan_code_change_selected_resources(&baseline, &target, &edges, &[]);
            let mut sorted = result.clone();
            sorted.sort();
            sorted.dedup();
            prop_assert_eq!(&result, &sorted);
        }

        #[test]
        fn directly_modified_nodes_are_included_when_not_reconciled(
            (baseline, edges) in arb_dag(8),
            modifications in proptest::collection::vec(0..8usize, 1..4)
        ) {
            let mut target = baseline.clone();
            let mut modified_ids = BTreeSet::new();
            for &idx in &modifications {
                if idx < target.len() {
                    target[idx].checksum = Some("modified".to_string());
                    modified_ids.insert(target[idx].unique_id.clone());
                }
            }
            // No current state = nothing reconciled
            let result = plan_code_change_selected_resources(&baseline, &target, &edges, &[]);
            let result_set: BTreeSet<_> = result.into_iter().collect();
            for id in &modified_ids {
                if is_build_plannable_resource_type(Some("model")) {
                    prop_assert!(
                        result_set.contains(id),
                        "modified node {id} missing from result"
                    );
                }
            }
        }

        #[test]
        fn output_nodes_are_reachable_from_modified(
            (baseline, edges) in arb_dag(8),
            modifications in proptest::collection::vec(0..8usize, 1..3)
        ) {
            let mut target = baseline.clone();
            let mut modified_ids = BTreeSet::new();
            for &idx in &modifications {
                if idx < target.len() {
                    target[idx].checksum = Some("modified".to_string());
                    modified_ids.insert(target[idx].unique_id.clone());
                }
            }
            let result = plan_code_change_selected_resources(&baseline, &target, &edges, &[]);

            // Build reachability from modified nodes
            let mut reachable = modified_ids.clone();
            let mut stack: Vec<_> = modified_ids.iter().cloned().collect();
            let target_ids: BTreeSet<_> = target.iter().map(|n| n.unique_id.clone()).collect();
            while let Some(parent) = stack.pop() {
                for (p, c) in &edges {
                    if *p == parent && target_ids.contains(c) && reachable.insert(c.clone()) {
                        stack.push(c.clone());
                    }
                }
            }

            for id in &result {
                prop_assert!(
                    reachable.contains(id),
                    "output node {id} is not reachable from any modified node"
                );
            }
        }

        #[test]
        fn fully_reconciled_nodes_are_excluded(
            (baseline, edges) in arb_dag(6),
            mod_idx in 0..6usize
        ) {
            if mod_idx >= baseline.len() {
                return Ok(());
            }
            let mut target = baseline.clone();
            target[mod_idx].checksum = Some("modified".to_string());

            // Mark the modified node as already reconciled to the new checksum
            let current = vec![CurrentNodeStatePlanningRecord {
                unique_id: target[mod_idx].unique_id.clone(),
                checksum: Some("modified".to_string()),
                last_success_at: Some(chrono::Utc::now()),
            }];

            let result = plan_code_change_selected_resources(&baseline, &target, &edges, &current);
            // The modified node itself should NOT be in the result (it's reconciled)
            prop_assert!(
                !result.contains(&target[mod_idx].unique_id),
                "reconciled node should be excluded"
            );
        }
    }
}
