use crate::api::{
    EnvironmentCreateApiRequest, EnvironmentResponse, EnvironmentUpdateApiRequest,
    EnvironmentsResponse, InvocationCancelApiRequest, InvocationClaimApiRequest,
    InvocationClaimNextApiRequest, InvocationClaimResponse, InvocationCommandApi,
    InvocationCompleteApiRequest, InvocationCreateApiRequest, InvocationCreateResponse,
    InvocationEvent, InvocationEventBatchApiRequest, InvocationExecutionSpecApi,
    InvocationHeartbeatApiRequest, InvocationHeartbeatResponse, InvocationLifecycleStatus,
    InvocationStatusResponse, InvocationWorkerHealthApi, InvocationsResponse, MigrateResponse,
    ProjectInitApiRequest, ProjectResponse, ProjectShowApiRequest, ProjectUpdateApiRequest,
    ProjectsResponse,
};
use crate::config::RuntimeConfig;
use crate::db::Db;
use crate::error::{AppError, AppResult};
use crate::event::LogEvent;
use crate::execution::ExecutionMode;
use crate::execution::{ExecutionCompletion, ExecutionEvent, ExecutionEventKind};
use crate::manifest::ManifestSnapshot;
use crate::services::{
    EnvironmentCreateRequest, EnvironmentService, EnvironmentUpdateRequest, InvocationCommand,
    InvocationRequest, InvocationService, ProjectInitRequest, ProjectService, ProjectUpdateRequest,
};
use async_stream::stream;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post};
use axum::{Json, Router};
use chrono::Utc;
use futures_util::Stream;
use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, broadcast};
use tokio::time::{Duration, sleep};
use tower_http::trace::TraceLayer;
use tracing::{error, info, info_span};
use uuid::Uuid;

#[derive(Clone)]
pub struct AppState {
    db: Db,
    runtime_config: RuntimeConfig,
    invocations: InvocationManager,
}

impl AppState {
    pub fn new(db: Db, runtime_config: RuntimeConfig) -> Self {
        Self {
            db,
            runtime_config,
            invocations: InvocationManager::default(),
        }
    }
}

#[derive(Clone, Default)]
struct InvocationManager {
    inner: Arc<Mutex<HashMap<Uuid, Arc<InvocationRuntime>>>>,
}

#[derive(Debug, Clone)]
struct SequencedInvocationEvent {
    sequence: u64,
    event: InvocationEvent,
}

#[derive(Default)]
struct InvocationHistory {
    next_sequence: u64,
    items: Vec<SequencedInvocationEvent>,
}

struct InvocationRuntime {
    status: Mutex<InvocationStatusResponse>,
    history: Mutex<InvocationHistory>,
    tx: broadcast::Sender<SequencedInvocationEvent>,
    persistence: Option<InvocationPersistence>,
    execution_mode: crate::api::InvocationExecutionModeApi,
    execution_spec: Mutex<Option<InvocationExecutionSpecApi>>,
    lease: Mutex<Option<InvocationLease>>,
}

#[derive(Clone)]
struct InvocationRecorder {
    runtime: Arc<InvocationRuntime>,
}

#[derive(Clone)]
struct InvocationPersistence {
    db: Db,
    run_id: Uuid,
    project_id: i64,
    environment_id: i64,
    subcommand: String,
    promote_base_manifest: bool,
}

#[derive(Debug, Clone)]
struct InvocationLease {
    worker_id: String,
    claimed_at: chrono::DateTime<Utc>,
}

const INVOCATION_STALE_AFTER: Duration = Duration::from_secs(15);

impl InvocationManager {
    async fn create(
        &self,
        invocation_id: Uuid,
        persistence: Option<InvocationPersistence>,
        execution_mode: crate::api::InvocationExecutionModeApi,
        execution_spec: Option<InvocationExecutionSpecApi>,
    ) -> (Uuid, Arc<InvocationRuntime>) {
        let status = InvocationStatusResponse {
            invocation_id,
            execution_mode,
            worker_health: InvocationWorkerHealthApi::Unclaimed,
            status: InvocationLifecycleStatus::Running,
            exit_code: None,
            error: None,
            started_at: Utc::now(),
            claimed_at: None,
            last_heartbeat_at: None,
            completed_at: None,
            cancel_requested: false,
            claimed_by: None,
        };
        let (tx, _) = broadcast::channel(1024);
        let runtime = Arc::new(InvocationRuntime {
            status: Mutex::new(status),
            history: Mutex::new(InvocationHistory::default()),
            tx,
            persistence,
            execution_mode,
            execution_spec: Mutex::new(execution_spec),
            lease: Mutex::new(None),
        });
        self.inner
            .lock()
            .await
            .insert(invocation_id, runtime.clone());
        (invocation_id, runtime)
    }

    async fn get(&self, invocation_id: Uuid) -> Option<Arc<InvocationRuntime>> {
        self.inner.lock().await.get(&invocation_id).cloned()
    }

    async fn list(&self) -> Vec<InvocationStatusResponse> {
        let runtimes = self
            .inner
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut statuses = Vec::with_capacity(runtimes.len());
        for runtime in runtimes {
            statuses.push(runtime.status().await);
        }
        statuses.sort_by_key(|status| std::cmp::Reverse(status.started_at));
        statuses
    }

    async fn stale_count(&self) -> usize {
        self.list()
            .await
            .into_iter()
            .filter(|status| status.worker_health == InvocationWorkerHealthApi::Stale)
            .count()
    }

    async fn claim_next(
        &self,
        worker_id: &str,
        execution_mode: Option<crate::api::InvocationExecutionModeApi>,
    ) -> AppResult<Option<InvocationClaimResponse>> {
        let runtimes = self
            .inner
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for runtime in runtimes {
            if !matches!(
                runtime.status().await.status,
                InvocationLifecycleStatus::Running
            ) {
                continue;
            }
            if let Some(mode) = execution_mode
                && runtime.execution_mode != mode
            {
                continue;
            }
            if runtime.execution_spec.lock().await.is_none() {
                continue;
            }
            let invocation_id = runtime.status().await.invocation_id;
            if !runtime.can_be_claimed().await {
                continue;
            }
            return runtime.claim_execution(invocation_id, worker_id).await.map(Some);
        }
        Ok(None)
    }

    fn schedule_cleanup(&self, invocation_id: Uuid) {
        let inner = self.inner.clone();
        tokio::spawn(async move {
            sleep(Duration::from_secs(300)).await;
            inner.lock().await.remove(&invocation_id);
        });
    }
}

impl InvocationRuntime {
    async fn can_be_claimed(&self) -> bool {
        if self.execution_spec.lock().await.is_none() {
            return false;
        }
        let lease = self.lease.lock().await;
        match lease.as_ref() {
            None => true,
            Some(lease) => Utc::now() - lease.claimed_at > chrono::Duration::from_std(INVOCATION_STALE_AFTER).unwrap_or_else(|_| chrono::Duration::seconds(15)),
        }
    }

    async fn push_event(&self, event: InvocationEvent) -> u64 {
        let sequenced = {
            let mut history = self.history.lock().await;
            history.next_sequence += 1;
            let sequenced = SequencedInvocationEvent {
                sequence: history.next_sequence,
                event,
            };
            history.items.push(sequenced.clone());
            sequenced
        };
        let sequence = sequenced.sequence;
        let _ = self.tx.send(sequenced);
        sequence
    }

    async fn status(&self) -> InvocationStatusResponse {
        let mut status = self.status.lock().await.clone();
        status.worker_health = self.worker_health(&status);
        status
    }

    async fn finish(
        &self,
        status: InvocationLifecycleStatus,
        exit_code: i32,
        error: Option<String>,
    ) {
        let mut current = self.status.lock().await;
        current.status = status;
        current.exit_code = Some(exit_code);
        current.error = error.clone();
        current.completed_at = Some(Utc::now());
        let completed = InvocationEvent {
            event_type: "invocation.completed".to_string(),
            timestamp: Utc::now(),
            text: None,
            stream: None,
            dbt_event_name: None,
            node_unique_id: None,
            level: None,
            exit_code: Some(exit_code),
            error,
        };
        drop(current);
        *self.lease.lock().await = None;
        let _ = self.push_event(completed).await;
    }

    async fn heartbeat(&self, worker_id: &str) -> AppResult<bool> {
        self.ensure_owned_by(worker_id).await?;
        let mut current = self.status.lock().await;
        current.last_heartbeat_at = Some(Utc::now());
        Ok(current.cancel_requested)
    }

    async fn request_cancel(&self) {
        let mut current = self.status.lock().await;
        current.cancel_requested = true;
    }

    async fn claim_execution(
        &self,
        invocation_id: Uuid,
        worker_id: &str,
    ) -> AppResult<InvocationClaimResponse> {
        let mut lease = self.lease.lock().await;
        if let Some(existing) = lease.as_ref()
            && Utc::now() - existing.claimed_at
                <= chrono::Duration::from_std(INVOCATION_STALE_AFTER)
                    .unwrap_or_else(|_| chrono::Duration::seconds(15))
        {
            return Err(AppError::InvocationAlreadyClaimed(
                invocation_id.to_string(),
            ));
        }
        let spec = self
            .execution_spec
            .lock()
            .await
            .clone()
            .ok_or_else(|| AppError::InvocationNotClaimable(invocation_id.to_string()))?;
        *lease = Some(InvocationLease {
            worker_id: worker_id.to_string(),
            claimed_at: Utc::now(),
        });
        let mut status = self.status.lock().await;
        status.claimed_by = Some(worker_id.to_string());
        status.claimed_at = lease.as_ref().map(|value| value.claimed_at);
        status.worker_health = InvocationWorkerHealthApi::Claimed;
        drop(status);
        Ok(InvocationClaimResponse {
            invocation_id,
            worker_id: worker_id.to_string(),
            execution_mode: self.execution_mode,
            execution_spec: spec,
        })
    }

    async fn ensure_owned_by(&self, worker_id: &str) -> AppResult<()> {
        let lease = self.lease.lock().await;
        match lease.as_ref() {
            Some(lease) if lease.worker_id == worker_id => Ok(()),
            Some(_) => Err(AppError::Io(std::io::Error::other(
                "invocation is owned by a different worker",
            ))),
            None => Err(AppError::Io(std::io::Error::other(
                "invocation has not been claimed",
            ))),
        }
    }

    fn worker_health(&self, status: &InvocationStatusResponse) -> InvocationWorkerHealthApi {
        if !matches!(status.status, InvocationLifecycleStatus::Running) {
            return InvocationWorkerHealthApi::Completed;
        }
        match (status.claimed_at, status.last_heartbeat_at.as_ref(), status.claimed_by.as_ref()) {
            (_, _, None) => InvocationWorkerHealthApi::Unclaimed,
            (_, Some(last_heartbeat), Some(_))
                if Utc::now() - *last_heartbeat
                    > chrono::Duration::from_std(INVOCATION_STALE_AFTER)
                        .unwrap_or_else(|_| chrono::Duration::seconds(15)) =>
            {
                InvocationWorkerHealthApi::Stale
            }
            (Some(claimed_at), None, Some(_))
                if Utc::now() - claimed_at
                    > chrono::Duration::from_std(INVOCATION_STALE_AFTER)
                        .unwrap_or_else(|_| chrono::Duration::seconds(15)) =>
            {
                InvocationWorkerHealthApi::Stale
            }
            (_, _, Some(_)) => InvocationWorkerHealthApi::Claimed,
        }
    }
}

impl InvocationRecorder {
    async fn record(&self, event: ExecutionEvent) -> AppResult<()> {
        let sse_event = InvocationEvent {
            event_type: match event.kind {
                ExecutionEventKind::StdoutLine => "stdout.line".to_string(),
                ExecutionEventKind::StderrLine => "stderr.line".to_string(),
                ExecutionEventKind::DbtLog => "dbt.log".to_string(),
            },
            timestamp: event.occurred_at,
            text: event.text.clone(),
            stream: match event.kind {
                ExecutionEventKind::StdoutLine | ExecutionEventKind::DbtLog => {
                    Some("stdout".to_string())
                }
                ExecutionEventKind::StderrLine => Some("stderr".to_string()),
            },
            dbt_event_name: event.dbt_event_name.clone(),
            node_unique_id: event.node_unique_id.clone(),
            level: event.level.clone(),
            exit_code: None,
            error: event.error.clone(),
        };
        let sequence = self.runtime.push_event(sse_event).await as i64;
        if let Some(persistence) = self.runtime.persistence.as_ref() {
            match event.kind {
                ExecutionEventKind::DbtLog => {
                    if let Some(raw_line) = event.raw_line.as_deref()
                        && let Some(log_event) = LogEvent::parse(raw_line)
                    {
                        persistence
                            .db
                            .persist_log_event(
                                persistence.run_id,
                                persistence.project_id,
                                persistence.environment_id,
                                sequence,
                                &log_event,
                            )
                            .await?;
                    }
                }
                ExecutionEventKind::StdoutLine => {
                    if let Some(raw_line) = event.raw_line.as_deref().or(event.text.as_deref()) {
                        persistence
                            .db
                            .persist_raw_line(persistence.run_id, sequence, raw_line)
                            .await?;
                    }
                }
                ExecutionEventKind::StderrLine => {}
            }
        }
        Ok(())
    }

    async fn complete(&self, completion: ExecutionCompletion) -> AppResult<()> {
        if let Some(persistence) = self.runtime.persistence.as_ref() {
            let manifest = completion.manifest.clone().map(ManifestSnapshot::from_raw);
            persistence
                .db
                .finalize_run(crate::db::RunFinalization {
                    run_id: persistence.run_id,
                    project_id: persistence.project_id,
                    environment_id: persistence.environment_id,
                    subcommand: &persistence.subcommand,
                    dbt_version: completion.dbt_version.as_deref(),
                    exit_code: completion.exit_code,
                    terminal_status: if matches!(
                        completion.status,
                        InvocationLifecycleStatus::Succeeded
                    ) {
                        "success"
                    } else {
                        "failed"
                    },
                    manifest: manifest.as_ref(),
                    promote_base_manifest: persistence.promote_base_manifest
                        && matches!(completion.status, InvocationLifecycleStatus::Succeeded),
                })
                .await?;
        }
        self.runtime
            .finish(completion.status, completion.exit_code, completion.error)
            .await;
        Ok(())
    }

    async fn is_running(&self) -> bool {
        matches!(
            self.runtime.status().await.status,
            InvocationLifecycleStatus::Running
        )
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/v1/state/migrate", post(migrate))
        .route("/v1/projects:init", post(project_init))
        .route("/v1/projects", get(projects_list))
        .route("/v1/projects/show", post(project_show))
        .route(
            "/v1/projects/{project_id}",
            patch(project_update).get(project_get),
        )
        .route("/v1/environments", post(environment_create))
        .route(
            "/v1/projects/{project_id}/environments",
            get(environment_list),
        )
        .route(
            "/v1/projects/{project_id}/environments/{slug}",
            get(environment_get).patch(environment_update),
        )
        .route("/v1/invocations", get(invocation_list).post(invocation_create))
        .route("/v1/invocations/claim-next", post(invocation_claim_next))
        .route("/v1/invocations/{id}/claim", post(invocation_claim))
        .route("/v1/invocations/{id}", get(invocation_status))
        .route("/v1/invocations/{id}/heartbeat", post(invocation_heartbeat))
        .route("/v1/invocations/{id}/cancel", post(invocation_cancel))
        .route("/v1/invocations/{id}/complete", post(invocation_complete))
        .route(
            "/v1/invocations/{id}/events",
            post(invocation_append_events),
        )
        .route("/v1/invocations/{id}/events", get(invocation_events))
        .layer(
            TraceLayer::new_for_http().make_span_with(|request: &axum::http::Request<_>| {
                info_span!(
                    "http_request",
                    method = %request.method(),
                    uri = %request.uri(),
                )
            }),
        )
        .with_state(state)
}

pub async fn serve(listen: &str, state: AppState) -> AppResult<()> {
    let addr: SocketAddr = listen.parse().map_err(|err| {
        AppError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid listen address '{listen}': {err}"),
        ))
    })?;
    info!(listen = %addr, "starting dbtx daemon");
    let reclaimable_stale_invocations = state.invocations.stale_count().await;
    info!(
        listen = %addr,
        reclaimable_stale_invocations,
        "dbtx daemon execution state initialized"
    );
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(listen = %addr, "dbtx daemon listening");
    axum::serve(listener, router(state)).await.map_err(|err| {
        error!(error = %err, "dbtx daemon stopped with error");
        AppError::Io(err)
    })
}

async fn migrate(State(state): State<AppState>) -> Result<Json<MigrateResponse>, ApiError> {
    let applied = state.db.migrate().await?;
    info!(applied = applied.len(), "applied database migrations");
    Ok(Json(MigrateResponse { applied }))
}

async fn project_init(
    State(state): State<AppState>,
    Json(request): Json<ProjectInitApiRequest>,
) -> Result<Json<ProjectResponse>, ApiError> {
    let service = ProjectService::new(&state.db);
    let project = service
        .init(ProjectInitRequest {
            current_dir: PathBuf::from(request.current_dir),
            git_repo_url: request.git_repo_url,
            project_root: request.project_root,
            default_branch: request.default_branch,
            force: request.force,
        })
        .await?;
    info!(project_id = %project.project_id, project_name = %project.project_name, "initialized project");
    Ok(Json(ProjectResponse { project }))
}

async fn project_update(
    State(state): State<AppState>,
    Path(project_id): Path<String>,
    Json(request): Json<ProjectUpdateApiRequest>,
) -> Result<Json<ProjectResponse>, ApiError> {
    let _ = project_id;
    let service = ProjectService::new(&state.db);
    let project = service
        .update(ProjectUpdateRequest {
            current_dir: PathBuf::from(request.current_dir),
            git_repo_url: request.git_repo_url,
            project_root: request.project_root,
            default_branch: request.default_branch,
        })
        .await?;
    info!(project_id = %project.project_id, project_name = %project.project_name, "updated project");
    Ok(Json(ProjectResponse { project }))
}

async fn projects_list(State(state): State<AppState>) -> Result<Json<ProjectsResponse>, ApiError> {
    let service = ProjectService::new(&state.db);
    let projects = service.list().await?;
    info!(count = projects.len(), "listed projects");
    Ok(Json(ProjectsResponse { projects }))
}

async fn project_get(
    State(state): State<AppState>,
    Path(project_id): Path<String>,
) -> Result<Json<ProjectResponse>, ApiError> {
    let project = state.db.get_project_by_project_id(&project_id).await?;
    info!(project_id = %project.project_id, "loaded project");
    Ok(Json(ProjectResponse { project }))
}

async fn project_show(
    State(state): State<AppState>,
    Json(request): Json<ProjectShowApiRequest>,
) -> Result<Json<ProjectResponse>, ApiError> {
    let service = ProjectService::new(&state.db);
    let project = service
        .show(&PathBuf::from(request.current_dir), request.project)
        .await?;
    info!(project_id = %project.project_id, "resolved project via context");
    Ok(Json(ProjectResponse { project }))
}

async fn environment_create(
    State(state): State<AppState>,
    Json(request): Json<EnvironmentCreateApiRequest>,
) -> Result<Json<EnvironmentResponse>, ApiError> {
    let service = EnvironmentService::new(&state.db);
    let environment = service
        .create(EnvironmentCreateRequest {
            current_dir: PathBuf::from(request.current_dir),
            project: request.project,
            slug: request.slug,
            target: request.target,
            kind: request.kind,
            baseline: request.baseline,
            git_branch: request.git_branch,
            git_commit_sha: request.git_commit_sha,
            pr_number: request.pr_number,
            immutable: request.immutable,
            status: request.status,
            schema_name: request.schema_name,
        })
        .await?;
    info!(
        project_id = %environment.project_ref,
        environment = %environment.slug,
        target_name = %environment.target_name,
        "created environment"
    );
    Ok(Json(EnvironmentResponse { environment }))
}

async fn environment_update(
    State(state): State<AppState>,
    Path((_project_id, _slug)): Path<(String, String)>,
    Json(request): Json<EnvironmentUpdateApiRequest>,
) -> Result<Json<EnvironmentResponse>, ApiError> {
    let service = EnvironmentService::new(&state.db);
    let environment = service
        .update(EnvironmentUpdateRequest {
            current_dir: PathBuf::from(request.current_dir),
            project: request.project,
            slug: request.slug,
            kind: request.kind,
            baseline: request.baseline,
            git_branch: request.git_branch,
            git_commit_sha: request.git_commit_sha,
            pr_number: request.pr_number,
            immutable: request.immutable,
            status: request.status,
            adapter_type: request.adapter_type,
            schema_name: request.schema_name,
            threads: request.threads,
        })
        .await?;
    info!(
        project_id = %environment.project_ref,
        environment = %environment.slug,
        target_name = %environment.target_name,
        "updated environment"
    );
    Ok(Json(EnvironmentResponse { environment }))
}

async fn environment_list(
    State(state): State<AppState>,
    Path(project_id): Path<String>,
) -> Result<Json<EnvironmentsResponse>, ApiError> {
    let service = EnvironmentService::new(&state.db);
    let environments = service.list(std::path::Path::new("."), project_id).await?;
    info!(count = environments.len(), "listed environments");
    Ok(Json(EnvironmentsResponse { environments }))
}

async fn environment_get(
    State(state): State<AppState>,
    Path((project_id, slug)): Path<(String, String)>,
) -> Result<Json<EnvironmentResponse>, ApiError> {
    let service = EnvironmentService::new(&state.db);
    let environment = service
        .show(std::path::Path::new("."), project_id, slug)
        .await?;
    info!(
        project_id = %environment.project_ref,
        environment = %environment.slug,
        "loaded environment"
    );
    Ok(Json(EnvironmentResponse { environment }))
}

async fn invocation_create(
    State(state): State<AppState>,
    Json(request): Json<InvocationCreateApiRequest>,
) -> Result<Json<InvocationCreateResponse>, ApiError> {
    let invocation_id = Uuid::new_v4();
    info!(
        invocation_id = %invocation_id,
        command = ?request.command,
        current_dir = %request.current_dir,
        "starting invocation"
    );

    let runtime_config = state.runtime_config.clone();
    let db = state.db.clone();
    let service = InvocationService::new(&db);
    let execution_mode = match request.execution_mode {
        crate::api::InvocationExecutionModeApi::Server => ExecutionMode::Server,
        crate::api::InvocationExecutionModeApi::Local => ExecutionMode::Local,
    };
    let prepared = service
        .prepare_local_execution(
            invocation_id,
            InvocationRequest {
                command: map_invocation_command(request.command),
                args: request.args.iter().cloned().map(Into::into).collect(),
                config: runtime_config,
                current_dir: Some(PathBuf::from(&request.current_dir)),
                environment_slug: request.environment_slug.clone(),
                execution_mode,
            },
        )
        .await?;
    let execution_spec = InvocationExecutionSpecApi {
        command: request.command,
        args: prepared
            .spec
            .args
            .into_iter()
            .map(|value| value.to_string_lossy().into_owned())
            .collect(),
        project_dir: request.current_dir.clone(),
        profiles_yml: prepared.spec.profiles_yml,
        state_manifest: prepared.spec.state_manifest,
    };
    let (invocation_id, runtime) = state
        .invocations
        .create(
            invocation_id,
            prepared.persistence.map(|p| InvocationPersistence {
                db: db.clone(),
                run_id: p.run_id,
                project_id: p.project_id,
                environment_id: p.environment_id,
                subcommand: p.subcommand,
                promote_base_manifest: p.promote_base_manifest,
            }),
            request.execution_mode,
            Some(execution_spec),
        )
        .await;
    let _ = runtime
        .push_event(InvocationEvent {
            event_type: "invocation.started".to_string(),
            timestamp: Utc::now(),
            text: None,
            stream: None,
            dbt_event_name: None,
            node_unique_id: None,
            level: None,
            exit_code: None,
            error: None,
        })
        .await;
    info!(
        invocation_id = %invocation_id,
        execution_mode = ?request.execution_mode,
        "created worker-claimable invocation"
    );
    Ok(Json(InvocationCreateResponse {
        invocation_id,
        execution_mode: request.execution_mode,
    }))
}

async fn invocation_claim(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(request): Json<InvocationClaimApiRequest>,
) -> Result<Json<InvocationClaimResponse>, ApiError> {
    let runtime = state.invocations.get(id).await.ok_or_else(|| {
        AppError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "invocation not found",
        ))
    })?;
    let claimed = runtime.claim_execution(id, &request.worker_id).await?;
    info!(invocation_id = %id, "claimed invocation execution");
    Ok(Json(claimed))
}

async fn invocation_claim_next(
    State(state): State<AppState>,
    Json(request): Json<InvocationClaimNextApiRequest>,
) -> Result<Response, ApiError> {
    let Some(claimed) = state
        .invocations
        .claim_next(&request.worker_id, request.execution_mode)
        .await?
    else {
        return Ok(StatusCode::NO_CONTENT.into_response());
    };
    info!(invocation_id = %claimed.invocation_id, "claimed next invocation execution");
    Ok(Json(claimed).into_response())
}

async fn invocation_heartbeat(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(request): Json<InvocationHeartbeatApiRequest>,
) -> Result<Json<InvocationHeartbeatResponse>, ApiError> {
    let runtime = state.invocations.get(id).await.ok_or_else(|| {
        AppError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "invocation not found",
        ))
    })?;
    let cancel_requested = runtime.heartbeat(&request.worker_id).await?;
    Ok(Json(InvocationHeartbeatResponse { cancel_requested }))
}

async fn invocation_cancel(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(_request): Json<InvocationCancelApiRequest>,
) -> Result<StatusCode, ApiError> {
    let runtime = state.invocations.get(id).await.ok_or_else(|| {
        AppError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "invocation not found",
        ))
    })?;
    runtime.request_cancel().await;
    info!(invocation_id = %id, "requested invocation cancel");
    Ok(StatusCode::NO_CONTENT)
}

async fn invocation_status(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<InvocationStatusResponse>, ApiError> {
    let runtime = state.invocations.get(id).await.ok_or_else(|| {
        AppError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "invocation not found",
        ))
    })?;
    info!(invocation_id = %id, "loaded invocation status");
    Ok(Json(runtime.status().await))
}

async fn invocation_list(State(state): State<AppState>) -> Result<Json<InvocationsResponse>, ApiError> {
    let invocations = state.invocations.list().await;
    info!(count = invocations.len(), "listed invocations");
    Ok(Json(InvocationsResponse { invocations }))
}

async fn invocation_append_events(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(request): Json<InvocationEventBatchApiRequest>,
) -> Result<StatusCode, ApiError> {
    let runtime = state.invocations.get(id).await.ok_or_else(|| {
        AppError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "invocation not found",
        ))
    })?;
    let recorder = InvocationRecorder { runtime };
    if !recorder.is_running().await {
        return Err(ApiError(AppError::Io(std::io::Error::other(
            "invocation is already completed",
        ))));
    }
    recorder.runtime.ensure_owned_by(&request.worker_id).await?;
    for event in request.events {
        recorder.record(event).await?;
    }
    info!(invocation_id = %id, "appended invocation events");
    Ok(StatusCode::NO_CONTENT)
}

async fn invocation_complete(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(request): Json<InvocationCompleteApiRequest>,
) -> Result<StatusCode, ApiError> {
    let runtime = state.invocations.get(id).await.ok_or_else(|| {
        AppError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "invocation not found",
        ))
    })?;
    let recorder = InvocationRecorder { runtime };
    recorder.runtime.ensure_owned_by(&request.worker_id).await?;
    recorder.complete(request.completion).await?;
    state.invocations.schedule_cleanup(id);
    info!(invocation_id = %id, "completed invocation via api");
    Ok(StatusCode::NO_CONTENT)
}

async fn invocation_events(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let runtime = state.invocations.get(id).await.ok_or_else(|| {
        AppError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "invocation not found",
        ))
    })?;
    let rx = runtime.tx.subscribe();
    let history = runtime.history.lock().await;
    let buffered_events = history.items.len();
    let last_sequence = history.items.last().map(|item| item.sequence).unwrap_or(0);
    let stream = event_stream(history.items.clone(), last_sequence, rx);
    info!(invocation_id = %id, buffered_events, "subscribed to invocation event stream");
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

fn event_stream(
    history: Vec<SequencedInvocationEvent>,
    last_history_sequence: u64,
    mut rx: broadcast::Receiver<SequencedInvocationEvent>,
) -> impl Stream<Item = Result<Event, Infallible>> {
    stream! {
        for item in history {
            yield Ok(to_sse_event(&item.event));
        }
        loop {
            match rx.recv().await {
                Ok(item) if item.sequence > last_history_sequence => yield Ok(to_sse_event(&item.event)),
                Ok(_) => continue,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    }
}

fn to_sse_event(event: &InvocationEvent) -> Event {
    Event::default()
        .event(event.event_type.clone())
        .data(serde_json::to_string(event).unwrap_or_else(|_| "{}".to_string()))
}

fn map_invocation_command(command: InvocationCommandApi) -> InvocationCommand {
    match command {
        InvocationCommandApi::Build => InvocationCommand::Build,
        InvocationCommandApi::Run => InvocationCommand::Run,
        InvocationCommandApi::Ls => InvocationCommand::Ls,
        InvocationCommandApi::Test => InvocationCommand::Test,
        InvocationCommandApi::Seed => InvocationCommand::Seed,
    }
}

struct ApiError(AppError);

impl From<AppError> for ApiError {
    fn from(value: AppError) -> Self {
        Self(value)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self.0 {
            AppError::ProjectIdMissing
            | AppError::MissingDatabaseUrl
            | AppError::UserStateNotAllowed
            | AppError::UserTargetNotAllowed
            | AppError::UserProfilesDirNotAllowed
            | AppError::InvalidEnvironmentKind(_)
            | AppError::InvalidEnvironmentStatus(_)
            | AppError::CommitEnvironmentRequiresSha
            | AppError::InvalidProfileConfig(_)
            | AppError::InvalidProfileSecret(_)
            | AppError::MissingSecretKey
            | AppError::UnsupportedLocalExecution(_) => StatusCode::BAD_REQUEST,
            AppError::ProjectIdNotFound(_) | AppError::EnvironmentNotFound(_, _) => {
                StatusCode::NOT_FOUND
            }
            AppError::EnvironmentAlreadyExists(_, _) | AppError::ProjectIdAlreadyConfigured(_) => {
                StatusCode::CONFLICT
            }
            AppError::InvocationAlreadyClaimed(_) => StatusCode::CONFLICT,
            AppError::InvocationNotClaimable(_) => StatusCode::BAD_REQUEST,
            AppError::SchemaOutOfDate => StatusCode::PRECONDITION_FAILED,
            AppError::ImmutableEnvironment(_, _)
            | AppError::ImmutableEnvironmentGitMismatch(_, _) => StatusCode::CONFLICT,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let body = serde_json::json!({ "error": self.0.to_string() });
        (status, Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        INVOCATION_STALE_AFTER, InvocationEvent, InvocationExecutionSpecApi, InvocationManager,
        InvocationRecorder, SequencedInvocationEvent,
        event_stream,
    };
    use crate::api::{InvocationCommandApi, InvocationExecutionModeApi};
    use crate::execution::{ExecutionCompletion, ExecutionEvent, ExecutionEventKind};
    use chrono::Utc;
    use futures_util::StreamExt;
    use tokio::sync::broadcast;
    use uuid::Uuid;

    fn sample_event(text: &str) -> InvocationEvent {
        InvocationEvent {
            event_type: "stdout.line".to_string(),
            timestamp: Utc::now(),
            text: Some(text.to_string()),
            stream: Some("stdout".to_string()),
            dbt_event_name: None,
            node_unique_id: None,
            level: None,
            exit_code: None,
            error: None,
        }
    }

    #[tokio::test]
    async fn event_stream_replays_history_then_live_events() {
        let (tx, rx) = broadcast::channel(16);
        let history = vec![SequencedInvocationEvent {
            sequence: 1,
            event: sample_event("one"),
        }];
        let mut stream = Box::pin(event_stream(history, 1, rx));

        let first = stream.next().await.expect("history item").expect("event");
        let _first = first;

        tx.send(SequencedInvocationEvent {
            sequence: 2,
            event: sample_event("two"),
        })
        .expect("send live event");
        let second = stream.next().await.expect("live item").expect("event");
        let _second = second;
    }

    #[tokio::test]
    async fn event_stream_skips_duplicate_live_events_already_in_history() {
        let (tx, rx) = broadcast::channel(16);
        let history = vec![SequencedInvocationEvent {
            sequence: 1,
            event: sample_event("one"),
        }];
        let mut stream = Box::pin(event_stream(history, 1, rx));

        let _first = stream.next().await.expect("history item").expect("event");

        tx.send(SequencedInvocationEvent {
            sequence: 1,
            event: sample_event("one"),
        })
        .expect("send duplicate live event");
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), stream.next())
                .await
                .is_err(),
            "duplicate live event should not be emitted"
        );
        tx.send(SequencedInvocationEvent {
            sequence: 2,
            event: sample_event("two"),
        })
        .expect("send next live event");

        let _second = stream.next().await.expect("live item").expect("event");
    }

    #[tokio::test]
    async fn recorder_appends_execution_events_into_runtime_history() {
        let manager = InvocationManager::default();
        let (_id, runtime) = manager
            .create(
                Uuid::new_v4(),
                None,
                crate::api::InvocationExecutionModeApi::Server,
                None,
            )
            .await;
        let recorder = InvocationRecorder {
            runtime: runtime.clone(),
        };

        recorder
            .record(ExecutionEvent {
                kind: ExecutionEventKind::StdoutLine,
                occurred_at: Utc::now(),
                text: Some("hello".to_string()),
                raw_line: None,
                dbt_event_name: None,
                node_unique_id: None,
                level: None,
                error: None,
            })
            .await
            .expect("record event");

        let history = runtime.history.lock().await;
        assert_eq!(history.items.len(), 1);
        assert_eq!(history.items[0].event.event_type, "stdout.line");
        assert_eq!(history.items[0].event.text.as_deref(), Some("hello"));
    }

    #[tokio::test]
    async fn recorder_marks_invocation_complete() {
        let manager = InvocationManager::default();
        let (_id, runtime) = manager
            .create(
                Uuid::new_v4(),
                None,
                crate::api::InvocationExecutionModeApi::Server,
                None,
            )
            .await;
        let recorder = InvocationRecorder { runtime };

        recorder
            .complete(ExecutionCompletion {
                status: crate::api::InvocationLifecycleStatus::Succeeded,
                exit_code: 0,
                error: None,
                dbt_version: None,
                manifest: None,
            })
            .await
            .expect("complete invocation");

        let status = recorder.runtime.status().await;
        assert!(matches!(
            status.status,
            crate::api::InvocationLifecycleStatus::Succeeded
        ));
        assert_eq!(status.exit_code, Some(0));
        assert!(status.completed_at.is_some());
    }

    #[tokio::test]
    async fn recorder_rejects_appends_after_completion() {
        let manager = InvocationManager::default();
        let (_id, runtime) = manager
            .create(
                Uuid::new_v4(),
                None,
                crate::api::InvocationExecutionModeApi::Server,
                None,
            )
            .await;
        let recorder = InvocationRecorder {
            runtime: runtime.clone(),
        };

        assert!(recorder.is_running().await);

        recorder
            .complete(ExecutionCompletion {
                status: crate::api::InvocationLifecycleStatus::Succeeded,
                exit_code: 0,
                error: None,
                dbt_version: None,
                manifest: None,
            })
            .await
            .expect("complete invocation");

        assert!(!recorder.is_running().await);
    }

    #[tokio::test]
    async fn uploaded_events_are_visible_via_sse_history() {
        let manager = InvocationManager::default();
        let (_id, runtime) = manager
            .create(
                Uuid::new_v4(),
                None,
                crate::api::InvocationExecutionModeApi::Server,
                None,
            )
            .await;
        let recorder = InvocationRecorder {
            runtime: runtime.clone(),
        };

        recorder
            .record(ExecutionEvent {
                kind: ExecutionEventKind::StdoutLine,
                occurred_at: Utc::now(),
                text: Some("one".to_string()),
                raw_line: None,
                dbt_event_name: None,
                node_unique_id: None,
                level: None,
                error: None,
            })
            .await
            .expect("record event");
        recorder
            .record(ExecutionEvent {
                kind: ExecutionEventKind::StdoutLine,
                occurred_at: Utc::now(),
                text: Some("two".to_string()),
                raw_line: None,
                dbt_event_name: None,
                node_unique_id: None,
                level: None,
                error: None,
            })
            .await
            .expect("record event");

        let history = runtime.history.lock().await;
        assert_eq!(history.items.len(), 2);
        assert_eq!(history.items[0].event.text.as_deref(), Some("one"));
        assert_eq!(history.items[1].event.text.as_deref(), Some("two"));
    }

    #[tokio::test]
    async fn stale_claim_can_be_reclaimed_by_another_worker() {
        let manager = InvocationManager::default();
        let (invocation_id, runtime) = manager
            .create(
                Uuid::new_v4(),
                None,
                InvocationExecutionModeApi::Server,
                Some(InvocationExecutionSpecApi {
                    command: InvocationCommandApi::Run,
                    args: vec![],
                    project_dir: ".".to_string(),
                    profiles_yml: "profiles".to_string(),
                    state_manifest: None,
                }),
            )
            .await;

        let claim = runtime
            .claim_execution(invocation_id, "worker-a")
            .await
            .expect("initial claim");
        assert_eq!(claim.worker_id, "worker-a");

        {
            let mut lease = runtime.lease.lock().await;
            let lease = lease.as_mut().expect("lease");
            lease.claimed_at = Utc::now()
                - chrono::Duration::from_std(INVOCATION_STALE_AFTER).expect("duration")
                - chrono::Duration::seconds(1);
        }

        let reclaimed = runtime
            .claim_execution(invocation_id, "worker-b")
            .await
            .expect("reclaimed");
        assert_eq!(reclaimed.worker_id, "worker-b");
        assert_eq!(runtime.status().await.claimed_by.as_deref(), Some("worker-b"));
    }
}
