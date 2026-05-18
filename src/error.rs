//! Application error types and result aliases.
use std::io;

/// Reconciliation-specific errors.
///
/// These are produced by the reconciler and planning logic. The reconciler
/// uses pattern matching on these to decide which errors to ignore vs propagate.
#[derive(Debug, thiserror::Error)]
pub enum ReconcileError {
    #[error("environment is already reconciled to known desired state")]
    AlreadyReconciled,
    #[error("environment reconciliation is already in progress")]
    InProgress,
    #[error("plan {0} is not admissible from status {1}")]
    PlanNotAdmissible(String, String),
    #[error("reconciliation requires a successful baseline run")]
    RequiresBaseline,
    #[error("reconciliation requires a desired git commit sha")]
    RequiresCommitSha,
    #[error("reconciliation plan resolved to no selected resources")]
    EmptyPlan,
}

/// Profile and encryption errors.
///
/// Produced during profile validation, encryption, and generation.
#[derive(Debug, thiserror::Error)]
pub enum ProfileError {
    #[error("missing DBTX_SECRET_KEY for encrypted profile secrets")]
    MissingSecretKey,
    #[error("dbt project is missing a profile name")]
    MissingDbtProfile,
    #[error("profiles.yml was not found at {0}")]
    FileNotFound(String),
    #[error("dbt profile '{0}' was not found in profiles.yml")]
    NotFound(String),
    #[error("dbt profile '{0}' target '{1}' was not found in profiles.yml")]
    TargetNotFound(String, String),
    #[error("profile adapter type is missing")]
    MissingAdapterType,
    #[error("unsupported dbt adapter '{0}'")]
    UnsupportedAdapter(String),
    #[error("invalid profile config: {0}")]
    InvalidConfig(String),
    #[error("invalid profile secrets: {0}")]
    InvalidSecret(String),
    #[error("failed to encrypt secret data: {0}")]
    Encryption(String),
    #[error("invalid encrypted secret payload: {0}")]
    InvalidEncryptedSecret(String),
}

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("")]
    SilentExit(i32),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("database error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("yaml error: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("toml error: {0}")]
    TomlDe(#[from] toml::de::Error),
    #[error("toml serialization error: {0}")]
    TomlSer(#[from] toml::ser::Error),

    // --- Profile errors (kept as flat variants for backward compat) ---
    #[error("missing DBTX_SECRET_KEY for encrypted profile secrets")]
    MissingSecretKey,
    #[error("dbt project is missing a profile name")]
    MissingDbtProfile,
    #[error("profiles.yml was not found at {0}")]
    ProfilesFileNotFound(String),
    #[error("dbt profile '{0}' was not found in profiles.yml")]
    ProfileNotFound(String),
    #[error("dbt profile '{0}' target '{1}' was not found in profiles.yml")]
    ProfileTargetNotFound(String, String),
    #[error("profile adapter type is missing")]
    MissingAdapterType,
    #[error("unsupported dbt adapter '{0}'")]
    UnsupportedAdapter(String),
    #[error("invalid profile config: {0}")]
    InvalidProfileConfig(String),
    #[error("invalid profile secrets: {0}")]
    InvalidProfileSecret(String),
    #[error("failed to encrypt secret data: {0}")]
    Encryption(String),
    #[error("invalid encrypted secret payload: {0}")]
    InvalidEncryptedSecret(String),

    // --- Invocation errors ---
    #[error("local execution is not supported for command '{0}' yet")]
    UnsupportedLocalExecution(String),
    #[error("invocation '{0}' is not claimable")]
    InvocationNotClaimable(String),
    #[error("invocation '{0}' has already been claimed")]
    InvocationAlreadyClaimed(String),
    #[error("dbt invocation failed with exit code {0}")]
    DbtFailed(i32),
    #[error("invocation was canceled")]
    InvocationCanceled,
    #[error("invocation is owned by a different worker or is not running")]
    InvocationOwnershipMismatch,
    #[error("invocation '{0}' was not found")]
    InvocationNotFound(String),

    // --- Project/environment lookup errors ---
    #[error("missing manifest at {0}")]
    MissingManifest(String),
    #[error("current directory is not a dbt project root: missing dbt_project.yml")]
    NotDbtProjectRoot,
    #[error("failed to infer git repository from current directory")]
    GitRepoNotFound,
    #[error("failed to infer git remote origin url from current repository")]
    GitRemoteNotFound,
    #[error("dbtx project id is already configured in dbtx.toml: {0}.")]
    ProjectIdAlreadyConfigured(String),
    #[error("dbtx project id is missing from dbtx.toml.")]
    ProjectIdMissing,
    #[error("project id '{0}' was not found in the database.")]
    ProjectIdNotFound(String),
    #[error(
        "no project found for repo '{0}' with root '{1}'. Create one with `dbtx project create`."
    )]
    ProjectNotFoundByRepo(String, String),
    #[error("environment '{1}' for project '{0}' was not found.")]
    EnvironmentNotFound(String, String),
    #[error("environment '{1}' for project '{0}' already exists")]
    EnvironmentAlreadyExists(String, String),
    #[error("environment version '{2}' for environment '{1}' in project '{0}' was not found")]
    EnvironmentVersionNotFound(String, String, i64),
    #[error("plan '{0}' was not found")]
    PlanNotFound(String),
    #[error("project draft '{0}' was not found")]
    ProjectDraftNotFound(String),
    #[error("environment draft '{0}' was not found")]
    EnvironmentDraftNotFound(String),

    // --- Remote execution precondition errors ---
    #[error("remote execution requires --project or project_id")]
    RemoteExecutionRequiresProjectId,
    #[error("remote execution requires an environment slug")]
    RemoteExecutionRequiresEnvironmentSlug,
    #[error("remote project registration requires a git repository")]
    RemoteProjectRequiresGitRepo,
    #[error("project '{0}' is missing git_repo_url required for remote execution")]
    RemoteExecutionRequiresGitRepoUrl(String),
    #[error("project '{0}' is missing project_root required for remote execution")]
    RemoteExecutionRequiresProjectRoot(String),
    #[error("invalid remote project_root '{0}': must be relative and must not traverse parents")]
    InvalidRemoteProjectRoot(String),
    #[error(
        "environment '{1}' for project '{0}' is missing git_commit_sha required for remote execution"
    )]
    RemoteExecutionRequiresCommitSha(String, String),

    // --- Validation errors ---
    #[error("invalid environment status '{0}'")]
    InvalidEnvironmentStatus(String),
    #[error("invalid database value for {0}: '{1}'")]
    InvalidDatabaseValue(&'static str, String),
    #[error("project '{0}' cannot be deleted because dependent records still exist")]
    ProjectDeleteBlocked(String),
    #[error("invalid release target: {0}")]
    InvalidReleaseTarget(String),
    #[error("environment '{1}' for remote project '{0}' requires --git-commit-sha")]
    RemoteProjectEnvironmentRequiresSha(String, String),
    #[error(
        "environment '{1}' for remote project '{0}' has invalid git_commit_sha '{2}': expected a commit SHA"
    )]
    InvalidRemoteProjectCommitSha(String, String, String),
    #[error(
        "registered project metadata does not match the current repo state for project id '{0}'. Run `dbtx project update` to sync the database."
    )]
    ProjectValidationFailed(String),
    #[error("dbtx manages --state internally; remove the user-supplied --state argument")]
    UserStateNotAllowed,
    #[error(
        "dbtx manages dbt target selection through the registered environment; remove the user-supplied --target argument"
    )]
    UserTargetNotAllowed,
    #[error(
        "dbtx manages warehouse profiles internally; remove the user-supplied --profiles-dir argument"
    )]
    UserProfilesDirNotAllowed,
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("environment '{0}' is immutable and cannot be modified")]
    ImmutableEnvironment(String),

    // --- Reconciliation errors ---
    #[error("environment is already reconciled to known desired state")]
    EnvironmentAlreadyReconciled,
    #[error("environment reconciliation is already in progress")]
    ReconciliationInProgress,
    #[error("plan {0} is not admissible from status {1}")]
    PlanNotAdmissible(String, String),
    #[error("reconciliation requires a successful baseline run")]
    ReconciliationRequiresBaseline,
    #[error("reconciliation requires a desired git commit sha")]
    ReconciliationRequiresCommitSha,
    #[error("reconciliation plan resolved to no selected resources")]
    ReconciliationEmptyPlan,

    // --- Config errors ---
    #[error(
        "database url is not configured. Set --database-url or DBTX_DATABASE_URL for dbtx-server."
    )]
    MissingDatabaseUrl,
    #[error(
        "dbtx service url is not configured. Start `dbtx-server` and set --service-url, DBTX_SERVICE_URL, or service.url in dbtx.toml."
    )]
    MissingServiceUrl,
    #[error(
        "database schema is not up to date. Run `dbtx state migrate` before invoking other commands."
    )]
    SchemaOutOfDate,

    // --- Worker/execution errors ---
    #[error("timed out: {0}")]
    TimedOut(String),
    #[error("worker setup failed: {0}")]
    WorkerSetupFailed(String),
    #[error("git target not found: {0}")]
    GitTargetNotFound(String),

    // --- HTTP client errors ---
    #[error("{0}")]
    Internal(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("service unavailable: {0}")]
    ServiceUnavailable(String),
    #[error("request timed out: {0}")]
    RequestTimeout(String),
}

impl From<ReconcileError> for AppError {
    fn from(err: ReconcileError) -> Self {
        match err {
            ReconcileError::AlreadyReconciled => Self::EnvironmentAlreadyReconciled,
            ReconcileError::InProgress => Self::ReconciliationInProgress,
            ReconcileError::PlanNotAdmissible(plan, status) => {
                Self::PlanNotAdmissible(plan, status)
            }
            ReconcileError::RequiresBaseline => Self::ReconciliationRequiresBaseline,
            ReconcileError::RequiresCommitSha => Self::ReconciliationRequiresCommitSha,
            ReconcileError::EmptyPlan => Self::ReconciliationEmptyPlan,
        }
    }
}

impl From<ProfileError> for AppError {
    fn from(err: ProfileError) -> Self {
        match err {
            ProfileError::MissingSecretKey => Self::MissingSecretKey,
            ProfileError::MissingDbtProfile => Self::MissingDbtProfile,
            ProfileError::FileNotFound(path) => Self::ProfilesFileNotFound(path),
            ProfileError::NotFound(name) => Self::ProfileNotFound(name),
            ProfileError::TargetNotFound(profile, target) => {
                Self::ProfileTargetNotFound(profile, target)
            }
            ProfileError::MissingAdapterType => Self::MissingAdapterType,
            ProfileError::UnsupportedAdapter(adapter) => Self::UnsupportedAdapter(adapter),
            ProfileError::InvalidConfig(msg) => Self::InvalidProfileConfig(msg),
            ProfileError::InvalidSecret(msg) => Self::InvalidProfileSecret(msg),
            ProfileError::Encryption(msg) => Self::Encryption(msg),
            ProfileError::InvalidEncryptedSecret(msg) => Self::InvalidEncryptedSecret(msg),
        }
    }
}

pub type AppResult<T> = Result<T, AppError>;

impl AppError {
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::SilentExit(code) => *code,
            Self::DbtFailed(code) => *code,
            _ => 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_code_returns_dbt_exit_code() {
        assert_eq!(AppError::DbtFailed(42).exit_code(), 42);
        assert_eq!(AppError::DbtFailed(0).exit_code(), 0);
    }

    #[test]
    fn exit_code_returns_silent_exit_code() {
        assert_eq!(AppError::SilentExit(0).exit_code(), 0);
        assert_eq!(AppError::SilentExit(2).exit_code(), 2);
    }

    #[test]
    fn exit_code_defaults_to_1_for_other_errors() {
        assert_eq!(AppError::Internal("oops".to_string()).exit_code(), 1);
        assert_eq!(AppError::NotDbtProjectRoot.exit_code(), 1);
        assert_eq!(AppError::SchemaOutOfDate.exit_code(), 1);
    }

    #[test]
    #[allow(clippy::type_complexity)]
    fn reconcile_error_converts_to_app_error() {
        let cases: Vec<(ReconcileError, fn(&AppError) -> bool)> = vec![
            (ReconcileError::AlreadyReconciled, |e| {
                matches!(e, AppError::EnvironmentAlreadyReconciled)
            }),
            (ReconcileError::InProgress, |e| {
                matches!(e, AppError::ReconciliationInProgress)
            }),
            (
                ReconcileError::PlanNotAdmissible("p1".into(), "done".into()),
                |e| matches!(e, AppError::PlanNotAdmissible(p, s) if p == "p1" && s == "done"),
            ),
            (ReconcileError::RequiresBaseline, |e| {
                matches!(e, AppError::ReconciliationRequiresBaseline)
            }),
            (ReconcileError::RequiresCommitSha, |e| {
                matches!(e, AppError::ReconciliationRequiresCommitSha)
            }),
            (ReconcileError::EmptyPlan, |e| {
                matches!(e, AppError::ReconciliationEmptyPlan)
            }),
        ];
        for (reconcile_err, check) in cases {
            let app_err: AppError = reconcile_err.into();
            assert!(check(&app_err), "conversion failed for: {app_err}");
        }
    }

    #[test]
    #[allow(clippy::type_complexity)]
    fn profile_error_converts_to_app_error() {
        let cases: Vec<(ProfileError, fn(&AppError) -> bool)> = vec![
            (ProfileError::MissingSecretKey, |e| {
                matches!(e, AppError::MissingSecretKey)
            }),
            (ProfileError::MissingDbtProfile, |e| {
                matches!(e, AppError::MissingDbtProfile)
            }),
            (
                ProfileError::FileNotFound("/tmp/profiles.yml".into()),
                |e| matches!(e, AppError::ProfilesFileNotFound(p) if p.contains("profiles.yml")),
            ),
            (
                ProfileError::NotFound("default".into()),
                |e| matches!(e, AppError::ProfileNotFound(n) if n == "default"),
            ),
            (
                ProfileError::TargetNotFound("default".into(), "prod".into()),
                |e| matches!(e, AppError::ProfileTargetNotFound(p, t) if p == "default" && t == "prod"),
            ),
            (ProfileError::MissingAdapterType, |e| {
                matches!(e, AppError::MissingAdapterType)
            }),
            (
                ProfileError::UnsupportedAdapter("oracle".into()),
                |e| matches!(e, AppError::UnsupportedAdapter(a) if a == "oracle"),
            ),
            (
                ProfileError::InvalidConfig("bad".into()),
                |e| matches!(e, AppError::InvalidProfileConfig(m) if m == "bad"),
            ),
            (
                ProfileError::InvalidSecret("nope".into()),
                |e| matches!(e, AppError::InvalidProfileSecret(m) if m == "nope"),
            ),
            (
                ProfileError::Encryption("aes failed".into()),
                |e| matches!(e, AppError::Encryption(m) if m == "aes failed"),
            ),
            (
                ProfileError::InvalidEncryptedSecret("corrupt".into()),
                |e| matches!(e, AppError::InvalidEncryptedSecret(m) if m == "corrupt"),
            ),
        ];
        for (profile_err, check) in cases {
            let app_err: AppError = profile_err.into();
            assert!(check(&app_err), "conversion failed for: {app_err}");
        }
    }
}
