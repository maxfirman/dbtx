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
