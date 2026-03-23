use crate::error::{AppError, AppResult};
use std::env;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub database_url: String,
    pub dbt_path: String,
}

#[derive(Debug, Clone)]
pub struct InvocationContext {
    pub project_slug: String,
    pub environment_slug: String,
    pub project_dir: PathBuf,
    pub profiles_dir: PathBuf,
    pub target_path: PathBuf,
    pub is_full_graph_run: bool,
    pub wants_state_modified: bool,
    pub dbt_args: Vec<OsString>,
}

impl RuntimeConfig {
    pub fn from_env() -> AppResult<Self> {
        let database_url =
            env::var("DBTX_DATABASE_URL").map_err(|_| AppError::MissingEnv("DBTX_DATABASE_URL"))?;
        let dbt_path = env::var("DBTX_DBT_PATH").unwrap_or_else(|_| "dbt".to_string());
        Ok(Self {
            database_url,
            dbt_path,
        })
    }

    pub fn from_optional_database_url(database_url: Option<String>) -> AppResult<Self> {
        let database_url = database_url
            .or_else(|| env::var("DBTX_DATABASE_URL").ok())
            .ok_or(AppError::MissingEnv("DBTX_DATABASE_URL"))?;
        let dbt_path = env::var("DBTX_DBT_PATH").unwrap_or_else(|_| "dbt".to_string());
        Ok(Self {
            database_url,
            dbt_path,
        })
    }
}

impl InvocationContext {
    pub fn from_args(args: &[OsString], inject_json_logging: bool) -> AppResult<Self> {
        if has_option(args, "--state") {
            return Err(AppError::UserStateNotAllowed);
        }

        let current_dir = env::current_dir().map_err(AppError::Io)?;
        let project_dir = parse_path_option(args, "--project-dir")
            .map(|path| absolutize(&current_dir, &path))
            .unwrap_or(current_dir.clone());
        let profiles_dir = parse_path_option(args, "--profiles-dir")
            .map(|path| absolutize(&current_dir, &path))
            .unwrap_or_else(|| current_dir.clone());
        let target_path = parse_path_option(args, "--target-path")
            .map(|path| absolutize(&current_dir, &path))
            .unwrap_or_else(|| project_dir.join("target"));

        let project_slug = env::var("DBTX_PROJECT_SLUG").unwrap_or_else(|_| {
            project_dir
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .replace(' ', "-")
        });
        let environment_slug = env::var("DBTX_ENVIRONMENT_SLUG").unwrap_or_else(|_| {
            parse_string_option(args, "--target")
                .or_else(|| env::var("DBT_TARGET").ok())
                .unwrap_or_else(|| "default".to_string())
        });
        let is_full_graph_run =
            !has_any_option(args, &["--select", "-s", "--exclude", "-x", "--selector"]);
        let wants_state_modified = args.iter().any(|arg| {
            let value = arg.to_string_lossy();
            value.contains("state:modified")
        });

        let mut dbt_args = args.to_vec();
        if inject_json_logging {
            dbt_args.push("--log-format".into());
            dbt_args.push("json".into());
            dbt_args.push("--write-json".into());
        }

        Ok(Self {
            project_slug,
            environment_slug,
            project_dir,
            profiles_dir,
            target_path,
            is_full_graph_run,
            wants_state_modified,
            dbt_args,
        })
    }
}

fn parse_path_option(args: &[OsString], flag: &str) -> Option<PathBuf> {
    parse_string_option(args, flag).map(PathBuf::from)
}

fn has_option(args: &[OsString], flag: &str) -> bool {
    args.iter().any(|value| {
        let value = value.to_string_lossy();
        value == flag || value.starts_with(&format!("{flag}="))
    })
}

fn has_any_option(args: &[OsString], flags: &[&str]) -> bool {
    flags.iter().any(|flag| has_option(args, flag))
}

fn parse_string_option(args: &[OsString], flag: &str) -> Option<String> {
    let mut idx = 0;
    while idx < args.len() {
        let current = args[idx].to_string_lossy();
        if current == flag {
            return args
                .get(idx + 1)
                .map(|value| value.to_string_lossy().into_owned());
        }
        if let Some((prefix, value)) = current.split_once('=')
            && prefix == flag
        {
            return Some(value.to_string());
        }
        idx += 1;
    }
    None
}

fn absolutize(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

#[cfg(test)]
mod tests {
    use super::InvocationContext;
    use crate::error::AppError;
    use std::ffi::OsString;

    #[test]
    fn derives_context_from_args() {
        let args = vec![
            OsString::from("--project-dir"),
            OsString::from("/tmp/example"),
            OsString::from("--profiles-dir"),
            OsString::from("/tmp/profiles"),
            OsString::from("--target"),
            OsString::from("prod"),
            OsString::from("--target-path=artifacts"),
            OsString::from("--select"),
            OsString::from("state:modified"),
        ];

        let ctx = InvocationContext::from_args(&args, true).expect("context should build");
        assert_eq!(ctx.project_slug, "example");
        assert_eq!(ctx.environment_slug, "prod");
        assert_eq!(ctx.profiles_dir, std::path::PathBuf::from("/tmp/profiles"));
        assert!(ctx.target_path.ends_with("artifacts"));
        assert!(!ctx.is_full_graph_run);
        assert!(ctx.wants_state_modified);
        assert!(ctx.dbt_args.iter().any(|arg| arg == "--log-format"));
    }

    #[test]
    fn rejects_user_supplied_state() {
        let args = vec![OsString::from("--state"), OsString::from("target/state")];
        let err = InvocationContext::from_args(&args, false).expect_err("state should fail");
        assert!(matches!(err, AppError::UserStateNotAllowed));
    }
}
