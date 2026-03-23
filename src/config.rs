use crate::error::{AppError, AppResult};
use serde::{Deserialize, Serialize};
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
    pub environment_slug: String,
    pub project_dir: PathBuf,
    pub target_path: PathBuf,
    pub is_full_graph_run: bool,
    pub wants_state_modified: bool,
    pub dbt_args: Vec<OsString>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DbtxToml {
    #[serde(default)]
    pub project: ProjectConfig,
    #[serde(default)]
    pub service: ServiceConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProjectConfig {
    pub id: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ServiceConfig {
    pub url: Option<String>,
}

impl RuntimeConfig {
    pub fn from_database_url(database_url: String) -> Self {
        let dbt_path = env::var("DBTX_DBT_PATH").unwrap_or_else(|_| "dbt".to_string());
        Self {
            database_url,
            dbt_path,
        }
    }
}

impl InvocationContext {
    #[allow(dead_code)]
    pub fn from_args(args: &[OsString], inject_json_logging: bool) -> AppResult<Self> {
        let current_dir = env::current_dir().map_err(AppError::Io)?;
        Self::from_args_in_dir(args, inject_json_logging, &current_dir)
    }

    pub fn from_args_in_dir(
        args: &[OsString],
        inject_json_logging: bool,
        current_dir: &Path,
    ) -> AppResult<Self> {
        if has_option(args, "--state") {
            return Err(AppError::UserStateNotAllowed);
        }
        if has_any_option(args, &["--target"]) {
            return Err(AppError::UserTargetNotAllowed);
        }
        if has_option(args, "--profiles-dir") {
            return Err(AppError::UserProfilesDirNotAllowed);
        }

        let project_dir = parse_path_option(args, "--project-dir")
            .map(|path| absolutize(current_dir, &path))
            .unwrap_or_else(|| current_dir.to_path_buf());
        let target_path = parse_path_option(args, "--target-path")
            .map(|path| absolutize(current_dir, &path))
            .unwrap_or_else(|| project_dir.join("target"));

        let environment_slug =
            env::var("DBTX_ENVIRONMENT_SLUG").unwrap_or_else(|_| "default".to_string());
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
            environment_slug,
            project_dir,
            target_path,
            is_full_graph_run,
            wants_state_modified,
            dbt_args,
        })
    }
}

pub fn read_dbtx_toml(project_root: &Path) -> AppResult<Option<DbtxToml>> {
    let path = dbtx_toml_path(project_root);
    if !path.is_file() {
        return Ok(None);
    }
    let content = std::fs::read_to_string(path)?;
    Ok(Some(toml::from_str(&content)?))
}

pub fn read_dbtx_project_id(project_root: &Path) -> AppResult<Option<String>> {
    Ok(read_dbtx_toml(project_root)?
        .and_then(|config| config.project.id)
        .filter(|value| !value.is_empty()))
}

pub fn resolve_database_url(
    database_url_override: Option<String>,
    _project_dir: Option<&Path>,
) -> AppResult<String> {
    database_url_override
        .or_else(|| env::var("DBTX_DATABASE_URL").ok())
        .ok_or(AppError::MissingDatabaseUrl)
}

pub fn resolve_service_url(
    service_url_override: Option<String>,
    project_dir: Option<&Path>,
) -> AppResult<Option<String>> {
    let file_service_url = project_dir
        .map(read_dbtx_toml)
        .transpose()?
        .flatten()
        .and_then(|config| config.service.url);
    Ok(service_url_override
        .or_else(|| env::var("DBTX_SERVICE_URL").ok())
        .or(file_service_url))
}

pub fn write_dbtx_toml(
    project_root: &Path,
    project_id: Option<&str>,
    force_project_id: bool,
) -> AppResult<()> {
    let path = dbtx_toml_path(project_root);
    let mut config = read_dbtx_toml(project_root)?.unwrap_or_default();

    if let Some(existing) = config.project.id.as_deref()
        && !force_project_id
        && project_id.is_some()
    {
        return Err(AppError::ProjectIdAlreadyConfigured(existing.to_string()));
    }

    if let Some(project_id) = project_id {
        config.project.id = Some(project_id.to_string());
    }

    std::fs::write(path, toml::to_string_pretty(&config)?)?;
    Ok(())
}

pub fn dbtx_toml_path(project_root: &Path) -> PathBuf {
    project_root.join("dbtx.toml")
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
    use super::{DbtxToml, InvocationContext, read_dbtx_project_id, write_dbtx_toml};
    use crate::error::AppError;
    use std::env;
    use std::ffi::OsString;
    use tempfile::TempDir;

    #[test]
    fn derives_context_from_args() {
        unsafe { env::set_var("DBTX_ENVIRONMENT_SLUG", "prod") };
        let args = vec![
            OsString::from("--project-dir"),
            OsString::from("/tmp/example"),
            OsString::from("--target-path=artifacts"),
            OsString::from("--select"),
            OsString::from("state:modified"),
        ];

        let ctx = InvocationContext::from_args(&args, true).expect("context should build");
        assert_eq!(ctx.environment_slug, "prod");
        assert!(ctx.target_path.ends_with("artifacts"));
        assert!(!ctx.is_full_graph_run);
        assert!(ctx.wants_state_modified);
        assert!(ctx.dbt_args.iter().any(|arg| arg == "--log-format"));
        unsafe { env::remove_var("DBTX_ENVIRONMENT_SLUG") };
    }

    #[test]
    fn rejects_user_supplied_state() {
        let args = vec![OsString::from("--state"), OsString::from("target/state")];
        let err = InvocationContext::from_args(&args, false).expect_err("state should fail");
        assert!(matches!(err, AppError::UserStateNotAllowed));
    }

    #[test]
    fn rejects_user_supplied_profiles_dir() {
        let args = vec![
            OsString::from("--profiles-dir"),
            OsString::from("/tmp/profiles"),
        ];
        let err = InvocationContext::from_args(&args, false).expect_err("profiles dir should fail");
        assert!(matches!(err, AppError::UserProfilesDirNotAllowed));
    }

    #[test]
    fn rejects_user_supplied_target() {
        let args = vec![OsString::from("--target"), OsString::from("prod")];
        let err = InvocationContext::from_args(&args, false).expect_err("target should fail");
        assert!(matches!(err, AppError::UserTargetNotAllowed));
    }

    #[test]
    fn writes_and_reads_dbtx_toml() {
        let temp = TempDir::new().expect("temp dir");
        write_dbtx_toml(temp.path(), Some("prj_123"), false).expect("write dbtx.toml");
        assert_eq!(
            read_dbtx_project_id(temp.path()).expect("read project id"),
            Some("prj_123".to_string())
        );
        let config = super::read_dbtx_toml(temp.path()).expect("read config");
        assert!(config.is_some());
    }

    #[test]
    fn serializes_expected_shape() {
        let config = DbtxToml {
            project: super::ProjectConfig {
                id: Some("prj_123".to_string()),
            },
            service: super::ServiceConfig {
                url: Some("http://127.0.0.1:8585".to_string()),
            },
        };
        let rendered = toml::to_string_pretty(&config).expect("render toml");
        assert!(rendered.contains("[project]"));
        assert!(rendered.contains("id = \"prj_123\""));
        assert!(rendered.contains("[service]"));
    }
}
