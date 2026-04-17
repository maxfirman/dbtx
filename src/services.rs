use crate::config::{InvocationContext, RuntimeConfig};
use crate::db::{
    CreateEnvironmentDraftInput, CreateProjectDraftInput, CreateProjectInput, Db,
    EnvironmentDraftRecord, EnvironmentRecord, EnvironmentReleaseInput, EnvironmentVersionRecord,
    GitState, LocalEnvironmentUpsertInput, ProjectDraftRecord, ProjectRecord, RunFinalization,
    RunStart, UpdateEnvironmentDraftInput,
    append_invocation_id, append_profiles_dir, append_state_dir, build_generated_profiles,
    read_dbt_project_name, read_git_state, spawn_dbt_child,
};
use crate::error::{AppError, AppResult};
use crate::event::LogEvent;
use crate::execution::ExecutionMode;
use crate::manifest::{ManifestSnapshot, ReconstructedManifest};
use crate::profile::{EnvironmentProfileRecord, LocalTargetProfile, resolve_runtime_profile, validate_environment_profile};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::ffi::OsString;
use std::path::Component;
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
    Release,
    ProjectValidate,
    EnvironmentPrepare,
    EnvironmentValidate,
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
        }
    }

    pub fn persists_state(self) -> bool {
        !matches!(self, Self::Ls | Self::Release | Self::ProjectValidate | Self::EnvironmentPrepare | Self::EnvironmentValidate)
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
    pub auto_deploy: bool,
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

pub struct ProjectService<'a> {
    db: &'a Db,
}

impl<'a> ProjectService<'a> {
    pub fn new(db: &'a Db) -> Self {
        Self { db }
    }

    pub async fn create_draft(
        &self,
        request: ProjectCreateRequest,
    ) -> AppResult<ProjectDraftRecord> {
        self.db.require_current_schema().await?;
        self.db
            .create_project_draft(CreateProjectDraftInput {
                git_repo_url: request.git_repo_url,
                project_root: request.project_root,
            })
            .await
    }

    pub async fn prepare_draft_validation(
        &self,
        draft_id: Uuid,
    ) -> AppResult<ProjectDraftValidationPrepared> {
        self.db.require_current_schema().await?;
        let invocation_id = Uuid::new_v4();
        let draft = self.db.mark_project_draft_validating(draft_id).await?;
        Ok(ProjectDraftValidationPrepared {
            spec: ProjectValidationSpec {
                repo_url: draft.git_repo_url.clone(),
                project_root: draft.project_root.clone(),
            },
            worker_queue: validation_worker_queue(),
            draft,
            invocation_id,
        })
    }

    pub async fn get_draft(&self, draft_id: Uuid) -> AppResult<ProjectDraftRecord> {
        self.db.require_current_schema().await?;
        self.db.get_project_draft(draft_id).await
    }

    pub async fn confirm_draft(&self, draft_id: Uuid) -> AppResult<ProjectRecord> {
        self.db.require_current_schema().await?;
        self.db.confirm_project_draft(draft_id).await
    }

    pub async fn update(&self, request: ProjectUpdateRequest) -> AppResult<ProjectRecord> {
        self.db.require_current_schema().await?;
        let existing = self.db.get_project_by_project_id(&request.project).await?;
        if existing.mode != "remote" {
            return Err(AppError::RemoteExecutionRequiresRemoteProject(
                existing.project_id.clone(),
                existing.mode,
            ));
        }
        let git_repo_url = request.git_repo_url.or(existing.git_repo_url.clone());
        let project_root = request.project_root.or(existing.project_root.clone());
        validate_remote_project_root(
            project_root
                .as_deref()
                .ok_or_else(|| AppError::InvalidRemoteProjectRoot(String::new()))?,
        )?;
        self.db
            .update_project(CreateProjectInput {
                project_id: existing.project_id.clone(),
                project_name: existing.project_name,
                mode: "remote".to_string(),
                git_repo_url,
                default_branch: existing.default_branch,
                project_root,
            })
            .await
    }

    pub async fn list(&self) -> AppResult<Vec<ProjectRecord>> {
        self.db.require_current_schema().await?;
        self.db.list_projects().await
    }

    pub async fn show(&self, project: String) -> AppResult<ProjectRecord> {
        self.db.require_current_schema().await?;
        self.db.get_project_by_project_id(&project).await
    }

    pub async fn delete(&self, project: String) -> AppResult<()> {
        self.db.require_current_schema().await?;
        self.db.delete_project(&project).await
    }
}

pub struct EnvironmentService<'a> {
    db: &'a Db,
}

impl<'a> EnvironmentService<'a> {
    pub fn new(db: &'a Db) -> Self {
        Self { db }
    }

    pub async fn create_draft(&self, project: String) -> AppResult<EnvironmentDraftRecord> {
        self.db.require_current_schema().await?;
        let project = self.db.get_project_by_project_id(&project).await?;
        if project.mode != "remote" {
            return Err(AppError::RemoteExecutionRequiresRemoteProject(
                project.project_id,
                project.mode,
            ));
        }
        self.db
            .create_environment_draft(CreateEnvironmentDraftInput {
                project_id: project.id,
                default_branch: project.default_branch,
            })
            .await
    }

    pub async fn get_draft(&self, draft_id: Uuid) -> AppResult<EnvironmentDraftRecord> {
        self.db.require_current_schema().await?;
        self.db.get_environment_draft(draft_id).await
    }

    pub async fn update_draft(
        &self,
        draft_id: Uuid,
        request: EnvironmentDraftUpdateRequest,
    ) -> AppResult<EnvironmentDraftRecord> {
        self.db.require_current_schema().await?;
        self.db
            .update_environment_draft(
                draft_id,
                UpdateEnvironmentDraftInput {
                    slug: request.slug,
                    git_branch: request.git_branch,
                    git_commit_sha: request.git_commit_sha,
                    use_latest_commit: request.use_latest_commit,
                    auto_deploy: request.auto_deploy,
                    immutable: request.immutable,
                    adapter_type: Some(request.adapter_type),
                    schema_name: Some(request.schema_name),
                    threads: request.threads,
                    profile_config: request.profile_config,
                    profile_secrets: request.profile_secrets,
                },
            )
            .await
    }

    pub async fn prepare_draft_git_metadata(
        &self,
        draft_id: Uuid,
    ) -> AppResult<EnvironmentDraftCreatePrepared> {
        self.db.require_current_schema().await?;
        let invocation_id = Uuid::new_v4();
        let draft = self.db.mark_environment_draft_loading_git(draft_id).await?;
        let project = self.db.get_project_by_id(draft.project_id).await?;
        let repo_url = project
            .git_repo_url
            .ok_or_else(|| AppError::RemoteExecutionRequiresGitRepoUrl(project.project_id.clone()))?;
        Ok(EnvironmentDraftCreatePrepared {
            draft,
            invocation_id,
            spec: EnvironmentPrepareSpec {
                repo_url,
                selected_branch: None,
            },
            worker_queue: validation_worker_queue(),
        })
    }

    pub async fn refresh_draft_branch(
        &self,
        draft_id: Uuid,
        request: EnvironmentDraftUpdateRequest,
    ) -> AppResult<EnvironmentDraftCreatePrepared> {
        let draft = self.update_draft(draft_id, request).await?;
        let invocation_id = Uuid::new_v4();
        let draft = self.db.mark_environment_draft_loading_git(draft.id).await?;
        let project = self.db.get_project_by_id(draft.project_id).await?;
        let repo_url = project
            .git_repo_url
            .ok_or_else(|| AppError::RemoteExecutionRequiresGitRepoUrl(project.project_id.clone()))?;
        Ok(EnvironmentDraftCreatePrepared {
            draft: draft.clone(),
            invocation_id,
            spec: EnvironmentPrepareSpec {
                repo_url,
                selected_branch: draft.git_branch.clone(),
            },
            worker_queue: validation_worker_queue(),
        })
    }

    pub async fn prepare_draft_validation(
        &self,
        draft_id: Uuid,
        request: EnvironmentDraftUpdateRequest,
    ) -> AppResult<EnvironmentDraftValidationPrepared> {
        let draft = self.update_draft(draft_id, request).await?;
        validate_environment_profile(
            draft.adapter_type.as_deref().unwrap_or_default(),
            draft.schema_name.as_deref().unwrap_or_default(),
            draft.threads,
            &draft.profile_config,
            &crate::profile::decrypt_json(&draft.profile_secrets)?,
            false,
        )?;
        let invocation_id = Uuid::new_v4();
        let draft = self.db.mark_environment_draft_validating(draft.id).await?;
        let project = self.db.get_project_by_id(draft.project_id).await?;
        let repo_url = project
            .git_repo_url
            .ok_or_else(|| AppError::RemoteExecutionRequiresGitRepoUrl(project.project_id.clone()))?;
        let project_root = project
            .project_root
            .ok_or_else(|| AppError::RemoteExecutionRequiresProjectRoot(project.project_id.clone()))?;
        let profile_record = EnvironmentProfileRecord {
            adapter_type: draft
                .adapter_type
                .clone()
                .ok_or_else(|| AppError::InvalidProfileConfig("adapter type is required".to_string()))?,
            schema_name: draft
                .schema_name
                .clone()
                .ok_or_else(|| AppError::InvalidProfileConfig("schema is required".to_string()))?,
            threads: draft.threads,
            profile_config: draft.profile_config.clone(),
            profile_secrets: draft.profile_secrets.clone(),
        };
        let profile_name = project.project_name.clone();
        let resolved = resolve_runtime_profile(&profile_name, &draft.slug, &profile_record)?;
        let generated = resolved.generate()?;
        let profiles_yml = std::fs::read_to_string(generated.temp_dir.path().join("profiles.yml"))?;
        let commit_sha = draft
            .git_commit_sha
            .clone()
            .ok_or_else(|| AppError::RemoteExecutionRequiresCommitSha(project.project_id.clone(), draft.slug.clone()))?;
        let selected_branch = draft.git_branch.clone();
        Ok(EnvironmentDraftValidationPrepared {
            draft,
            invocation_id,
            spec: EnvironmentValidationSpec {
                repo_url,
                commit_sha,
                project_root,
                selected_branch,
                profiles_yml,
            },
            worker_queue: validation_worker_queue(),
        })
    }

    pub async fn confirm_draft(&self, draft_id: Uuid) -> AppResult<EnvironmentRecord> {
        self.db.require_current_schema().await?;
        self.db.confirm_environment_draft(draft_id).await
    }

    pub async fn release(
        &self,
        request: EnvironmentReleaseRequest,
    ) -> AppResult<EnvironmentRecord> {
        self.db.require_current_schema().await?;
        let project = self.db.get_project_by_project_id(&request.project).await?;
        if project.mode != "remote" {
            return Err(AppError::RemoteExecutionRequiresRemoteProject(
                project.project_id,
                project.mode,
            ));
        }
        let git_commit_sha = request.git_commit_sha.ok_or_else(|| {
            AppError::InvalidReleaseTarget(
                "public release API requires git_commit_sha; use worker-validated release flow to resolve refs"
                    .to_string(),
            )
        })?;
        self.db
            .release_environment(EnvironmentReleaseInput {
                project: project.project_id,
                slug: request.slug,
                git_branch: request.git_branch,
                git_commit_sha,
            })
            .await
    }

    pub async fn history(
        &self,
        project: String,
        slug: String,
    ) -> AppResult<Vec<EnvironmentVersionRecord>> {
        self.db.require_current_schema().await?;
        self.db.list_environment_versions(&project, &slug).await
    }

    pub async fn rollback(
        &self,
        request: EnvironmentRollbackRequest,
    ) -> AppResult<EnvironmentRecord> {
        self.db.require_current_schema().await?;
        let project = self.db.get_project_by_project_id(&request.project).await?;
        if project.mode != "remote" {
            return Err(AppError::RemoteExecutionRequiresRemoteProject(
                project.project_id,
                project.mode,
            ));
        }
        self.db
            .rollback_environment_to_version(&project.project_id, &request.slug, request.version_id)
            .await
    }

    pub async fn list(
        &self,
        project: String,
    ) -> AppResult<Vec<EnvironmentRecord>> {
        self.db.require_current_schema().await?;
        self.db.list_environments(&project).await
    }

    pub async fn show(
        &self,
        project: String,
        slug: String,
    ) -> AppResult<EnvironmentRecord> {
        self.db.require_current_schema().await?;
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
        let git_state = read_git_state(&ctx.project_dir);
        let (project, environment) = self
            .resolve_local_project_and_environment(&ctx.project_dir, ctx.target_name.as_deref())
            .await?;
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
                    request.execution_mode,
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

    pub async fn prepare_local_execution(
        &self,
        run_id: Uuid,
        request: InvocationRequest,
    ) -> AppResult<LocalExecutionPrepared> {
        self.db.require_current_schema().await?;
        let inject_json_logging = request.command.persists_state();
        let current_dir = request
            .current_dir
            .clone()
            .unwrap_or(std::env::current_dir()?);
        let ctx =
            InvocationContext::from_args_in_dir(&request.args, inject_json_logging, &current_dir)?;
        let git_state = read_git_state(&ctx.project_dir);
        let (project, environment) = self
            .resolve_local_project_and_environment(&ctx.project_dir, ctx.target_name.as_deref())
            .await?;
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
        let state_manifest = if let Some(reconstructed_manifest) = reconstructed_manifest.as_ref() {
            let path = reconstructed_manifest.temp_dir.path().join("manifest.json");
            let content = tokio::fs::read_to_string(path).await?;
            Some(serde_json::from_str(&content)?)
        } else {
            None
        };
        let generated_profiles = build_generated_profiles(&ctx.project_dir, &environment)?;
        let profiles_yml =
            tokio::fs::read_to_string(generated_profiles.temp_dir.path().join("profiles.yml"))
                .await?;
        let dbt_args = if request.command.persists_state() {
            append_invocation_id(ctx.dbt_args, run_id)
        } else {
            ctx.dbt_args
        };
        let persistence = if request.command.persists_state() {
            let args_json = Value::Array(
                dbt_args
                    .iter()
                    .map(|value| Value::String(value.to_string_lossy().into_owned()))
                    .collect(),
            );
            self.db
                .insert_run_started(RunStart {
                    run_id,
                    project: &project,
                    environment: &environment,
                    subcommand: request.command.as_str(),
                    args_json,
                    is_full_graph_run: ctx.is_full_graph_run,
                    execution_mode: request.execution_mode,
                    git_state: &git_state,
                })
                .await?;
            Some(LocalExecutionPersistence {
                run_id,
                project_id: project.id,
                environment_id: environment.id,
                subcommand: request.command.as_str().to_string(),
                promote_base_manifest: ctx.is_full_graph_run,
            })
        } else {
            None
        };

        Ok(LocalExecutionPrepared {
            spec: PreparedExecutionSpec::Local(LocalExecutionSpec {
                command: request.command,
                args: dbt_args,
                project_dir: ctx.project_dir.clone(),
                profiles_yml,
                state_manifest,
            }),
            persistence,
            worker_queue: environment.worker_queue.clone(),
            project_id: Some(project.id),
            environment_id: Some(environment.id),
            project_draft_id: None,
            environment_draft_id: None,
        })
    }

    pub async fn prepare_remote_execution(
        &self,
        run_id: Uuid,
        command: InvocationCommand,
        args: Vec<OsString>,
        project_id: &str,
        environment_slug: &str,
    ) -> AppResult<LocalExecutionPrepared> {
        self.db.require_current_schema().await?;
        let project = self.db.get_project_by_project_id(project_id).await?;
        let environment = self
            .db
            .get_environment(project_id, environment_slug)
            .await?;

        if project.mode != "remote" {
            return Err(AppError::RemoteExecutionRequiresRemoteProject(
                project.project_id.clone(),
                project.mode.clone(),
            ));
        }
        let repo_url = project.git_repo_url.clone().ok_or_else(|| {
            AppError::RemoteExecutionRequiresGitRepoUrl(project.project_id.clone())
        })?;
        let project_root = project.project_root.clone().ok_or_else(|| {
            AppError::RemoteExecutionRequiresProjectRoot(project.project_id.clone())
        })?;
        let commit_sha = environment.git_commit_sha.clone().ok_or_else(|| {
            AppError::RemoteExecutionRequiresCommitSha(
                project.project_id.clone(),
                environment.slug.clone(),
            )
        })?;

        let inject_json_logging = command.persists_state();
        let fake_project_dir = PathBuf::from("/");
        let ctx =
            InvocationContext::from_args_in_dir(&args, inject_json_logging, &fake_project_dir)?;
        let project_name = project.project_name.clone();

        let reconstructed_manifest = self
            .db
            .load_reconstructed_manifest(project.id, environment.id)
            .await?
            .or(if ctx.wants_state_modified {
                Some(
                    ReconstructedManifest::write_empty_state(
                        &project_name,
                        &environment.adapter_type,
                    )
                    .await?,
                )
            } else {
                None
            });
        let state_manifest = if let Some(reconstructed_manifest) = reconstructed_manifest.as_ref() {
            let path = reconstructed_manifest.temp_dir.path().join("manifest.json");
            let content = tokio::fs::read_to_string(path).await?;
            Some(serde_json::from_str(&content)?)
        } else {
            None
        };
        let generated_profiles = build_generated_profiles(Path::new("."), &environment)?;
        let profiles_yml =
            tokio::fs::read_to_string(generated_profiles.temp_dir.path().join("profiles.yml"))
                .await?;
        let dbt_args = if command.persists_state() {
            append_invocation_id(ctx.dbt_args, run_id)
        } else {
            ctx.dbt_args
        };
        let persistence = if command.persists_state() {
            let args_json = Value::Array(
                dbt_args
                    .iter()
                    .map(|value| Value::String(value.to_string_lossy().into_owned()))
                    .collect(),
            );
            let git_state = GitState {
                branch: environment.git_branch.clone(),
                commit_sha: Some(commit_sha.clone()),
                repo_url: Some(repo_url.clone()),
            };
            self.db
                .insert_run_started(RunStart {
                    run_id,
                    project: &project,
                    environment: &environment,
                    subcommand: command.as_str(),
                    args_json,
                    is_full_graph_run: ctx.is_full_graph_run,
                    execution_mode: ExecutionMode::Server,
                    git_state: &git_state,
                })
                .await?;
            Some(LocalExecutionPersistence {
                run_id,
                project_id: project.id,
                environment_id: environment.id,
                subcommand: command.as_str().to_string(),
                promote_base_manifest: ctx.is_full_graph_run,
            })
        } else {
            None
        };

        Ok(LocalExecutionPrepared {
            spec: PreparedExecutionSpec::Remote(RemoteExecutionSpec {
                command,
                args: dbt_args,
                repo_url,
                commit_sha,
                project_root,
                profiles_yml,
                state_manifest,
            }),
            persistence,
            worker_queue: environment.worker_queue.clone(),
            project_id: Some(project.id),
            environment_id: Some(environment.id),
            project_draft_id: None,
            environment_draft_id: None,
        })
    }

    pub async fn prepare_release_validation(
        &self,
        args: Vec<OsString>,
        project_id: &str,
        environment_slug: &str,
    ) -> AppResult<LocalExecutionPrepared> {
        self.db.require_current_schema().await?;
        let project = self.db.get_project_by_project_id(project_id).await?;
        let environment = self
            .db
            .get_environment(project_id, environment_slug)
            .await?;

        if project.mode != "remote" {
            return Err(AppError::RemoteExecutionRequiresRemoteProject(
                project.project_id.clone(),
                project.mode.clone(),
            ));
        }
        let repo_url = project.git_repo_url.clone().ok_or_else(|| {
            AppError::RemoteExecutionRequiresGitRepoUrl(project.project_id.clone())
        })?;
        let target = parse_release_target_args(&args)?;

        Ok(LocalExecutionPrepared {
            spec: PreparedExecutionSpec::ReleaseValidation(ReleaseValidationSpec {
                repo_url,
                git_ref: target.git_ref,
                git_commit_sha: target.git_commit_sha,
                git_branch: target.git_branch,
            }),
            persistence: None,
            worker_queue: environment.worker_queue.clone(),
            project_id: Some(project.id),
            environment_id: Some(environment.id),
            project_draft_id: None,
            environment_draft_id: None,
        })
    }

    async fn resolve_local_project_and_environment(
        &self,
        project_dir: &Path,
        target_override: Option<&str>,
    ) -> AppResult<(ProjectRecord, EnvironmentRecord)> {
        let project = self.load_or_create_inferred_project(project_dir).await?;
        let local_profile = LocalTargetProfile::from_local_project(project_dir, target_override)?;
        let profile_secrets = local_profile.encrypted_secrets()?;
        let worker_queue = infer_local_worker_queue(project_dir)?;
        let environment = self
            .db
            .upsert_local_environment(LocalEnvironmentUpsertInput {
                project: &project,
                profile_name: &local_profile.profile_name,
                target_name: &local_profile.target_name,
                adapter_type: &local_profile.adapter_type,
                worker_queue: &worker_queue,
                schema_name: &local_profile.schema_name,
                threads: local_profile.threads,
                profile_config: &local_profile.profile_config,
                profile_secrets: &profile_secrets,
            })
            .await?;
        Ok((project, environment))
    }

    async fn load_or_create_inferred_project(
        &self,
        project_dir: &Path,
    ) -> AppResult<ProjectRecord> {
        let project_input = infer_local_project_defaults(project_dir, None, None, None)?;
        match self
            .db
            .get_project_by_project_id(&project_input.project_id)
            .await
        {
            Ok(project) => Ok(project),
            Err(AppError::ProjectIdNotFound(_)) => {
                self.db
                    .upsert_project(CreateProjectInput {
                        project_id: project_input.project_id,
                        project_name: project_input.project_name,
                        mode: project_input.mode,
                        git_repo_url: project_input.git_repo_url,
                        default_branch: project_input.default_branch,
                        project_root: project_input.project_root,
                    })
                    .await
            }
            Err(err) => Err(err),
        }
    }

    async fn invoke_persisting<O: InvocationObserver>(
        &self,
        subcommand: &str,
        execution_mode: ExecutionMode,
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
                execution_mode,
                git_state,
            })
            .await?;

        let mut child =
            match spawn_dbt_child(&config.dbt_path, subcommand, &dbt_args, &ctx.project_dir) {
                Ok(child) => child,
                Err(err) => {
                    self.db
                        .mark_run_finished(run_id, None, 1, "wrapper_failed")
                        .await?;
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
                if dbt_version.is_none() && event.info.name == "MainReportVersion" {
                    dbt_version = event
                        .data
                        .get("version")
                        .and_then(Value::as_str)
                        .map(ToString::to_string);
                }
                self.db
                    .persist_log_event(
                        None,
                        run_id,
                        project.id,
                        environment.id,
                        sequence_no,
                        &event,
                    )
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
        let terminal_status = if status.success() {
            "success"
        } else {
            "failed"
        };
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

        let result = InvocationResult { exit_code };
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
    let repo_root = crate::db::git_repo_root(current_dir)
        .map_err(|_| AppError::RemoteProjectRequiresGitRepo)?;
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
        && !is_valid_release_commit_sha(git_commit_sha)
    {
        return Err(AppError::InvalidReleaseTarget(format!(
            "invalid git commit sha '{git_commit_sha}': expected 7 to 64 hexadecimal characters"
        )));
    }
    Ok(request)
}

fn is_valid_release_commit_sha(value: &str) -> bool {
    let trimmed = value.trim();
    (7..=64).contains(&trimmed.len()) && trimmed.bytes().all(|byte| byte.is_ascii_hexdigit())
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

    Err(AppError::Io(std::io::Error::other(
        "failed to determine local machine scope",
    )))
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
    use super::{
        is_valid_release_commit_sha, parse_release_target_args, validation_worker_queue_from_env,
    };
    use std::ffi::OsString;

    #[test]
    fn release_commit_sha_requires_hex_shape() {
        assert!(is_valid_release_commit_sha("deadbeef"));
        assert!(is_valid_release_commit_sha(
            "0123456789abcdef0123456789abcdef01234567"
        ));
        assert!(!is_valid_release_commit_sha("abc123"));
        assert!(!is_valid_release_commit_sha("main"));
        assert!(!is_valid_release_commit_sha("dead beef"));
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
}
