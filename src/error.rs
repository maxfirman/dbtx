use std::io;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("missing required environment variable {0}")]
    MissingEnv(&'static str),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("database error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("dbt invocation failed with exit code {0}")]
    DbtFailed(i32),
    #[error("missing manifest at {0}")]
    MissingManifest(String),
    #[error("run {0} was not found")]
    RunNotFound(uuid::Uuid),
    #[error("current directory is not a dbt project root: missing dbt_project.yml")]
    NotDbtProjectRoot,
    #[error("failed to infer git repository from current directory")]
    GitRepoNotFound,
    #[error("failed to infer git remote origin url from current repository")]
    GitRemoteNotFound,
    #[error("dbtx project id is already configured in dbt_project.yml: {0}. Use `dbtx project init --force` to overwrite it.")]
    ProjectIdAlreadyConfigured(String),
    #[error("dbtx project id is missing from dbt_project.yml. Run `dbtx project init` first.")]
    ProjectIdMissing,
    #[error("project '{0}' was not found")]
    ProjectNotFound(String),
    #[error("project id '{0}' was not found")]
    ProjectIdNotFound(String),
    #[error("environment '{1}' for project '{0}' was not found")]
    EnvironmentNotFound(String, String),
    #[error("registered project metadata does not match the current repo state for project id '{0}'. Run `dbtx project update` to sync the database, or `dbtx project init --force` to re-initialize the project.")]
    ProjectValidationFailed(String),
    #[error("dbtx manages --state internally; remove the user-supplied --state argument")]
    UserStateNotAllowed,
}

pub type AppResult<T> = Result<T, AppError>;

impl AppError {
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::MissingEnv(_) => 2,
            Self::DbtFailed(code) => *code,
            _ => 1,
        }
    }
}
