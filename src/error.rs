use std::io;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
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
    #[error("local execution is not supported for command '{0}' yet")]
    UnsupportedLocalExecution(String),
    #[error("dbt invocation failed with exit code {0}")]
    DbtFailed(i32),
    #[error("missing manifest at {0}")]
    MissingManifest(String),
    #[error("current directory is not a dbt project root: missing dbt_project.yml")]
    NotDbtProjectRoot,
    #[error("failed to infer git repository from current directory")]
    GitRepoNotFound,
    #[error("failed to infer git remote origin url from current repository")]
    GitRemoteNotFound,
    #[error(
        "dbtx project id is already configured in dbtx.toml: {0}. Use `dbtx project init --force` to overwrite it."
    )]
    ProjectIdAlreadyConfigured(String),
    #[error("dbtx project id is missing from dbtx.toml. Run `dbtx project init` first.")]
    ProjectIdMissing,
    #[error(
        "database url is not configured. Set --database-url or DBTX_DATABASE_URL for dbtx-server."
    )]
    MissingDatabaseUrl,
    #[error(
        "dbtx service url is not configured. Start `dbtx-server` and set --service-url, DBTX_SERVICE_URL, or service.url in dbtx.toml."
    )]
    #[allow(dead_code)]
    MissingServiceUrl,
    #[error(
        "database schema is not up to date. Run `dbtx state migrate` before invoking other commands."
    )]
    SchemaOutOfDate,
    #[error(
        "project id '{0}' was not found in the database. Run `dbtx project init --force` to re-initialize the project or `dbtx project update` to sync it."
    )]
    ProjectIdNotFound(String),
    #[error(
        "environment '{1}' for project '{0}' was not found. Create it with `dbtx environment create --project {0} --slug {1}`."
    )]
    EnvironmentNotFound(String, String),
    #[error("environment '{1}' for project '{0}' already exists")]
    EnvironmentAlreadyExists(String, String),
    #[error("invalid environment kind '{0}'")]
    InvalidEnvironmentKind(String),
    #[error("invalid environment status '{0}'")]
    InvalidEnvironmentStatus(String),
    #[error("commit environments require --git-commit-sha")]
    CommitEnvironmentRequiresSha,
    #[error("environment '{1}' for project '{0}' is immutable and cannot be updated")]
    ImmutableEnvironment(String, String),
    #[error("immutable environment '{1}' for project '{0}' does not match the current git state")]
    ImmutableEnvironmentGitMismatch(String, String),
    #[error(
        "registered project metadata does not match the current repo state for project id '{0}'. Run `dbtx project update` to sync the database, or `dbtx project init --force` to re-initialize the project."
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
}

pub type AppResult<T> = Result<T, AppError>;

impl AppError {
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::DbtFailed(code) => *code,
            _ => 1,
        }
    }
}
