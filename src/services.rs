use crate::config::{
    InvocationContext, RuntimeConfig, read_dbtx_project_id, write_dbtx_toml,
};
use crate::db::{
    CreateEnvironmentInput, CreateProjectInput, Db, EnvironmentRecord, GitState, ProjectRecord,
    RunFinalization, RunStart, UpdateEnvironmentInput, append_invocation_id,
    append_profiles_dir, append_state_dir, build_generated_profiles, read_dbt_project_name,
    read_git_state, spawn_dbt_child, validate_environment_git_state,
};
use crate::error::{AppError, AppResult};
use crate::event::LogEvent;
use crate::manifest::{ManifestSnapshot, ReconstructedManifest};
use crate::profile::LocalTargetProfile;
use serde_json::Value;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncBufReadExt, BufReader};
use uuid::Uuid;

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
}

impl InvocationCommand {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Build => "build",
            Self::Run => "run",
            Self::Ls => "ls",
            Self::Test => "test",
            Self::Seed => "seed",
        }
    }

    pub fn persists_state(self) -> bool {
        !matches!(self, Self::Ls)
    }
}

#[derive(Debug, Clone)]
pub struct InvocationRequest {
    pub command: InvocationCommand,
    pub args: Vec<OsString>,
    pub config: RuntimeConfig,
    pub current_dir: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct InvocationResult {
    pub exit_code: i32,
}

#[derive(Debug, Clone)]
pub struct ProjectInitRequest {
    pub current_dir: PathBuf,
    pub git_repo_url: Option<String>,
    pub project_root: Option<String>,
    pub default_branch: Option<String>,
    pub force: bool,
    pub database_url: String,
}

#[derive(Debug, Clone)]
pub struct ProjectUpdateRequest {
    pub current_dir: PathBuf,
    pub git_repo_url: Option<String>,
    pub project_root: Option<String>,
    pub default_branch: Option<String>,
}

#[derive(Debug, Clone)]
pub struct EnvironmentCreateRequest {
    pub current_dir: PathBuf,
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

#[derive(Debug, Clone)]
pub struct EnvironmentUpdateRequest {
    pub current_dir: PathBuf,
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

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct InferredProjectInput {
    pub(crate) project_name: String,
    pub(crate) git_repo_url: String,
    pub(crate) default_branch: Option<String>,
    pub(crate) project_root: String,
}

pub struct ProjectService<'a> {
    db: &'a Db,
}

impl<'a> ProjectService<'a> {
    pub fn new(db: &'a Db) -> Self {
        Self { db }
    }

    pub async fn init(&self, request: ProjectInitRequest) -> AppResult<ProjectRecord> {
        self.db.require_current_schema().await?;
        let inferred = infer_project_defaults(
            &request.current_dir,
            request.git_repo_url.as_deref(),
            request.project_root.as_deref(),
            request.default_branch.as_deref(),
        )?;
        let existing_project_id = read_dbtx_project_id(&request.current_dir)?;
        if let Some(existing_project_id) = existing_project_id.as_deref()
            && !request.force
        {
            return Err(AppError::ProjectIdAlreadyConfigured(
                existing_project_id.to_string(),
            ));
        }

        let project_id = format!("prj_{}", Uuid::new_v4().simple());
        let input = CreateProjectInput {
            project_id: project_id.clone(),
            project_name: inferred.project_name,
            git_repo_url: inferred.git_repo_url,
            default_branch: inferred.default_branch,
            project_root: inferred.project_root,
        };
        let project = if let Some(existing_project_id) = existing_project_id.as_deref() {
            self.db.reinitialize_project_id(existing_project_id, input).await?
        } else {
            self.db.create_project(input).await?
        };
        write_dbtx_toml(
            &request.current_dir,
            Some(&project_id),
            Some(&request.database_url),
            request.force,
        )?;
        Ok(project)
    }

    pub async fn update(&self, request: ProjectUpdateRequest) -> AppResult<ProjectRecord> {
        self.db.require_current_schema().await?;
        let project_id =
            read_dbtx_project_id(&request.current_dir)?.ok_or(AppError::ProjectIdMissing)?;
        let inferred = infer_project_defaults(
            &request.current_dir,
            request.git_repo_url.as_deref(),
            request.project_root.as_deref(),
            request.default_branch.as_deref(),
        )?;
        self.db
            .update_project(CreateProjectInput {
                project_id,
                project_name: inferred.project_name,
                git_repo_url: inferred.git_repo_url,
                default_branch: inferred.default_branch,
                project_root: inferred.project_root,
            })
            .await
    }

    pub async fn list(&self) -> AppResult<Vec<ProjectRecord>> {
        self.db.require_current_schema().await?;
        self.db.list_projects().await
    }

    pub async fn show(&self, current_dir: &Path, project: Option<String>) -> AppResult<ProjectRecord> {
        self.db.require_current_schema().await?;
        let project_id = project
            .or_else(|| read_dbtx_project_id(current_dir).ok().flatten())
            .ok_or(AppError::ProjectIdMissing)?;
        self.db.get_project_by_project_id(&project_id).await
    }
}

pub struct EnvironmentService<'a> {
    db: &'a Db,
}

impl<'a> EnvironmentService<'a> {
    pub fn new(db: &'a Db) -> Self {
        Self { db }
    }

    pub async fn create(&self, request: EnvironmentCreateRequest) -> AppResult<EnvironmentRecord> {
        self.db.require_current_schema().await?;
        let project = resolve_project_identifier(request.project, &request.current_dir)?;
        let local_profile =
            LocalTargetProfile::from_local_project(&request.current_dir, request.target.as_deref())?;
        let profile_secrets = local_profile.encrypted_secrets()?;
        let slug = request
            .slug
            .unwrap_or_else(|| local_profile.target_name.clone());
        self.db
            .create_environment(CreateEnvironmentInput {
                project,
                slug,
                target_name: local_profile.target_name,
                kind: request.kind,
                baseline_slug: request.baseline,
                git_branch: request.git_branch,
                git_commit_sha: request.git_commit_sha,
                pr_number: request.pr_number,
                immutable: request.immutable,
                status: request.status,
                adapter_type: local_profile.adapter_type,
                schema_name: request.schema_name.or(Some(local_profile.schema_name)),
                threads: local_profile.threads,
                profile_config: local_profile.profile_config,
                profile_secrets,
            })
            .await
    }

    pub async fn update(&self, request: EnvironmentUpdateRequest) -> AppResult<EnvironmentRecord> {
        self.db.require_current_schema().await?;
        let project = resolve_project_identifier(Some(request.project), &request.current_dir)?;
        self.db
            .update_environment(UpdateEnvironmentInput {
                project,
                slug: request.slug,
                kind: request.kind,
                baseline_slug: request.baseline,
                git_branch: request.git_branch,
                git_commit_sha: request.git_commit_sha,
                pr_number: request.pr_number,
                immutable: request.immutable,
                status: request.status,
                adapter_type: request.adapter_type,
                target_name: None,
                schema_name: request.schema_name,
                threads: request.threads,
                profile_config: None,
                profile_secrets: None,
            })
            .await
    }

    pub async fn list(&self, current_dir: &Path, project: String) -> AppResult<Vec<EnvironmentRecord>> {
        self.db.require_current_schema().await?;
        let project = resolve_project_identifier(Some(project), current_dir)?;
        self.db.list_environments(&project).await
    }

    pub async fn show(
        &self,
        current_dir: &Path,
        project: String,
        slug: String,
    ) -> AppResult<EnvironmentRecord> {
        self.db.require_current_schema().await?;
        let project = resolve_project_identifier(Some(project), current_dir)?;
        self.db.get_environment(&project, &slug).await
    }
}

pub struct InvocationService<'a> {
    db: &'a Db,
}

struct InvocationScope<'a> {
    ctx: InvocationContext,
    project: &'a ProjectRecord,
    environment: &'a EnvironmentRecord,
    git_state: &'a GitState,
}

impl<'a> InvocationService<'a> {
    pub fn new(db: &'a Db) -> Self {
        Self { db }
    }

    pub async fn invoke<O: InvocationObserver>(
        &self,
        request: InvocationRequest,
        observer: &mut O,
    ) -> AppResult<InvocationResult> {
        self.db.require_current_schema().await?;
        let inject_json_logging = request.command.persists_state();
        let current_dir = request
            .current_dir
            .clone()
            .unwrap_or(std::env::current_dir()?);
        let ctx =
            InvocationContext::from_args_in_dir(&request.args, inject_json_logging, &current_dir)?;
        let project = self.db.resolve_local_project(&ctx.project_dir).await?;
        let git_state = read_git_state(&ctx.project_dir);
        let environment = self
            .db
            .get_environment(&project.project_id, &ctx.environment_slug)
            .await?;
        validate_environment_git_state(&project, &environment, &git_state)?;

        match request.command {
            InvocationCommand::Ls => {
                self.invoke_ls(
                    &request,
                    InvocationScope {
                        ctx,
                        project: &project,
                        environment: &environment,
                        git_state: &git_state,
                    },
                    observer,
                )
                .await
            }
            _ => {
                self.invoke_persisting(
                    request.command.as_str(),
                    &request.config,
                    InvocationScope {
                        ctx,
                        project: &project,
                        environment: &environment,
                        git_state: &git_state,
                    },
                    observer,
                )
                .await
            }
        }
    }

    async fn invoke_persisting<O: InvocationObserver>(
        &self,
        subcommand: &str,
        config: &RuntimeConfig,
        scope: InvocationScope<'_>,
        observer: &mut O,
    ) -> AppResult<InvocationResult> {
        let InvocationScope {
            ctx,
            project,
            environment,
            git_state,
        } = scope;
        let run_id = Uuid::new_v4();
        let reconstructed_manifest = self
            .db
            .load_reconstructed_manifest(project.id, environment.id)
            .await?
            .or(if ctx.wants_state_modified {
                Some(
                    ReconstructedManifest::write_empty_state(
                        &read_dbt_project_name(&ctx.project_dir),
                        &environment.adapter_type,
                    )
                    .await?,
                )
            } else {
                None
            });
        let generated_profiles = build_generated_profiles(&ctx.project_dir, environment)?;
        let dbt_args = append_invocation_id(
            append_profiles_dir(
                append_state_dir(ctx.dbt_args.clone(), reconstructed_manifest.as_ref()),
                &generated_profiles,
            ),
            run_id,
        );
        let args_json = Value::Array(
            dbt_args
                .iter()
                .map(|value| Value::String(value.to_string_lossy().into_owned()))
                .collect(),
        );
        self.db
            .insert_run_started(RunStart {
                run_id,
                project,
                environment,
                subcommand,
                args_json,
                is_full_graph_run: ctx.is_full_graph_run,
                git_state,
            })
            .await?;

        let mut child = match spawn_dbt_child(&config.dbt_path, subcommand, &dbt_args, &ctx.project_dir)
        {
            Ok(child) => child,
            Err(err) => {
                self.db.mark_run_finished(run_id, None, 1, "wrapper_failed").await?;
                return Err(err);
            }
        };

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| AppError::Io(std::io::Error::other("missing child stdout")))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| AppError::Io(std::io::Error::other("missing child stderr")))?;

        let stderr_handle = tokio::spawn(async move {
            let mut reader = BufReader::new(stderr).lines();
            let mut lines = Vec::new();
            while let Some(line) = reader.next_line().await? {
                lines.push(line);
            }
            Result::<Vec<String>, std::io::Error>::Ok(lines)
        });

        let mut reader = BufReader::new(stdout).lines();
        let mut sequence_no: i64 = 0;
        let mut dbt_version: Option<String> = None;
        while let Some(line) = reader.next_line().await? {
            sequence_no += 1;
            if let Some(event) = LogEvent::parse(&line) {
                let rendered = event.render_text_line();
                observer.dbt_log(&event, rendered.as_deref());
                if let Some(rendered) = rendered {
                    observer.stdout_line(&rendered);
                }
                if dbt_version.is_none() && event.info.name == "MainReportVersion" {
                    dbt_version = event
                        .data
                        .get("version")
                        .and_then(Value::as_str)
                        .map(ToString::to_string);
                }
                self.db
                    .persist_log_event(run_id, project.id, environment.id, sequence_no, &event)
                    .await?;
            } else {
                observer.stdout_line(&line);
                self.db.persist_raw_line(run_id, sequence_no, &line).await?;
            }
        }

        let status = child.wait().await?;
        for line in stderr_handle.await.map_err(|err| {
            AppError::Io(std::io::Error::other(format!("stderr task failed: {err}")))
        })?? {
            observer.stderr_line(&line);
        }

        let manifest_path = ctx.target_path.join("manifest.json");
        let manifest_result = ManifestSnapshot::from_path(&manifest_path).await;
        let terminal_status = if status.success() { "success" } else { "failed" };
        let exit_code = status.code().unwrap_or(1);

        self.db
            .finalize_run(RunFinalization {
                run_id,
                project_id: project.id,
                environment_id: environment.id,
                subcommand,
                dbt_version: dbt_version.as_deref(),
                exit_code,
                terminal_status,
                manifest: manifest_result.ok().as_ref(),
                promote_base_manifest: ctx.is_full_graph_run && status.success(),
            })
            .await?;

        let result = InvocationResult {
            exit_code,
        };
        if status.success() {
            Ok(result)
        } else {
            Err(AppError::DbtFailed(exit_code))
        }
    }

    async fn invoke_ls<O: InvocationObserver>(
        &self,
        request: &InvocationRequest,
        scope: InvocationScope<'_>,
        observer: &mut O,
    ) -> AppResult<InvocationResult> {
        let InvocationScope {
            ctx,
            project,
            environment,
            ..
        } = scope;
        let reconstructed_manifest = self
            .db
            .load_reconstructed_manifest(project.id, environment.id)
            .await?
            .or(if ctx.wants_state_modified {
                Some(
                    ReconstructedManifest::write_empty_state(
                        &read_dbt_project_name(&ctx.project_dir),
                        &environment.adapter_type,
                    )
                    .await?,
                )
            } else {
                None
            });
        let generated_profiles = build_generated_profiles(&ctx.project_dir, environment)?;
        let dbt_args = append_profiles_dir(
            append_state_dir(ctx.dbt_args.clone(), reconstructed_manifest.as_ref()),
            &generated_profiles,
        );

        let mut child = spawn_dbt_child(
            &request.config.dbt_path,
            InvocationCommand::Ls.as_str(),
            &dbt_args,
            &ctx.project_dir,
        )?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| AppError::Io(std::io::Error::other("missing child stdout")))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| AppError::Io(std::io::Error::other("missing child stderr")))?;

        let stdout_handle = tokio::spawn(async move {
            let mut reader = BufReader::new(stdout).lines();
            let mut lines = Vec::new();
            while let Some(line) = reader.next_line().await? {
                lines.push(line);
            }
            Result::<Vec<String>, std::io::Error>::Ok(lines)
        });
        let stderr_handle = tokio::spawn(async move {
            let mut reader = BufReader::new(stderr).lines();
            let mut lines = Vec::new();
            while let Some(line) = reader.next_line().await? {
                lines.push(line);
            }
            Result::<Vec<String>, std::io::Error>::Ok(lines)
        });

        let status = child.wait().await?;
        for line in stdout_handle.await.map_err(|err| {
            AppError::Io(std::io::Error::other(format!("stdout task failed: {err}")))
        })?? {
            observer.stdout_line(&line);
        }
        for line in stderr_handle.await.map_err(|err| {
            AppError::Io(std::io::Error::other(format!("stderr task failed: {err}")))
        })?? {
            observer.stderr_line(&line);
        }

        let result = InvocationResult {
            exit_code: status.code().unwrap_or(1),
        };
        if status.success() {
            Ok(result)
        } else {
            Err(AppError::DbtFailed(result.exit_code))
        }
    }
}

fn resolve_project_identifier(project: Option<String>, current_dir: &Path) -> AppResult<String> {
    project
        .or_else(|| std::env::var("DBTX_PROJECT_ID").ok())
        .or_else(|| read_dbtx_project_id(current_dir).ok().flatten())
        .ok_or(AppError::ProjectIdMissing)
}

pub(crate) fn infer_project_defaults(
    current_dir: &Path,
    git_repo_url: Option<&str>,
    project_root: Option<&str>,
    default_branch: Option<&str>,
) -> AppResult<InferredProjectInput> {
    let project_name = read_dbt_project_name_from_root(current_dir)?;
    let repo_root = git_repo_root(current_dir)?;

    Ok(InferredProjectInput {
        project_name,
        git_repo_url: git_repo_url
            .map(ToString::to_string)
            .or_else(|| git_remote_origin_url(&repo_root).ok())
            .ok_or(AppError::GitRemoteNotFound)?,
        default_branch: default_branch.map(ToString::to_string),
        project_root: project_root
            .map(ToString::to_string)
            .unwrap_or(relative_project_root(&repo_root, current_dir)),
    })
}

pub(crate) fn read_dbt_project_name_from_root(project_root: &Path) -> AppResult<String> {
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

fn git_repo_root(current_dir: &Path) -> AppResult<PathBuf> {
    let output = run_git(["rev-parse", "--show-toplevel"], current_dir)?;
    Ok(PathBuf::from(output))
}

fn git_remote_origin_url(repo_root: &Path) -> AppResult<String> {
    run_git(["config", "--get", "remote.origin.url"], repo_root)
        .map_err(|_| AppError::GitRemoteNotFound)
}

pub(crate) fn relative_project_root(repo_root: &Path, project_root: &Path) -> String {
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

fn run_git<const N: usize>(args: [&str; N], cwd: &Path) -> AppResult<String> {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()?;
    if !output.status.success() {
        return Err(AppError::GitRepoNotFound);
    }
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if stdout.is_empty() {
        return Err(AppError::GitRepoNotFound);
    }
    Ok(stdout)
}
