use crate::api::{
    EnvironmentCreateApiRequest, EnvironmentResponse, EnvironmentUpdateApiRequest,
    EnvironmentsResponse, HealthResponse, InvocationCancelApiRequest,
    InvocationClaimNextApiRequest, InvocationCleanupApiRequest, InvocationCleanupResponse,
    InvocationCommandApi, InvocationCompleteApiRequest, InvocationCreateApiRequest,
    InvocationCreateResponse, InvocationEvent, InvocationEventBatchApiRequest,
    InvocationExecutionSpecApi, InvocationHeartbeatApiRequest, InvocationHeartbeatResponse,
    InvocationLifecycleStatus, InvocationListApiRequest, InvocationStatusResponse,
    InvocationsResponse, MigrateResponse, ProjectInitApiRequest, ProjectResponse,
    ProjectShowApiRequest, ProjectUpdateApiRequest, ProjectsResponse, QueuesResponse,
    ReadyResponse, WorkersResponse,
};
use crate::config::RuntimeConfig;
use crate::db::{
    CreateInvocationInput, Db, InvocationCancellationRecord, InvocationPersistenceRecord,
    TimedOutInvocationRecord,
};
use crate::error::{AppError, AppResult};
use crate::event::LogEvent;
use crate::execution::ExecutionMode;
use crate::execution::{
    ExecutionCompletion, ExecutionEvent, ExecutionEventKind, claim_startup_timeout,
    heartbeat_stale_timeout,
};
use crate::services::{
    EnvironmentCreateRequest, EnvironmentService, EnvironmentUpdateRequest, InvocationCommand,
    InvocationRequest, InvocationService, PreparedExecutionSpec, ProjectInitRequest,
    ProjectService, ProjectUpdateRequest,
};
use async_stream::stream;
use axum::extract::{Path, Query, State};
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
    items: Vec<SequencedInvocationEvent>,
}

struct InvocationRuntime {
    history: Mutex<InvocationHistory>,
    tx: broadcast::Sender<SequencedInvocationEvent>,
    persistence: Mutex<Option<InvocationPersistence>>,
}

#[derive(Clone)]
struct InvocationRecorder {
    db: Db,
    invocation_id: Uuid,
    runtime: Arc<InvocationRuntime>,
}

#[derive(Clone)]
struct InvocationPersistence {
    run_id: Uuid,
    project_id: i64,
    environment_id: i64,
    promote_base_manifest: bool,
}

impl InvocationPersistence {
    fn from_record(record: InvocationPersistenceRecord) -> Option<Self> {
        Some(Self {
            run_id: record.run_id?,
            project_id: record.project_id,
            environment_id: record.environment_id,
            promote_base_manifest: record.promote_base_manifest,
        })
    }

    async fn persist_log_event(
        &self,
        db: &Db,
        run_id: Uuid,
        project_id: i64,
        environment_id: i64,
        sequence: i64,
        log_event: &LogEvent,
    ) -> AppResult<()> {
        db.persist_log_event(run_id, project_id, environment_id, sequence, log_event)
            .await
    }

    async fn persist_raw_line(
        &self,
        db: &Db,
        run_id: Uuid,
        sequence: i64,
        raw_line: &str,
    ) -> AppResult<()> {
        db.persist_raw_line(run_id, sequence, raw_line).await
    }
}

impl InvocationManager {
    async fn get_or_create(
        &self,
        invocation_id: Uuid,
        persistence: Option<InvocationPersistence>,
    ) -> Arc<InvocationRuntime> {
        let mut guard = self.inner.lock().await;
        if let Some(runtime) = guard.get(&invocation_id) {
            if persistence.is_some() {
                *runtime.persistence.lock().await = persistence;
            }
            return runtime.clone();
        }
        let (tx, _) = broadcast::channel(1024);
        let runtime = Arc::new(InvocationRuntime {
            history: Mutex::new(InvocationHistory::default()),
            tx,
            persistence: Mutex::new(persistence),
        });
        guard.insert(invocation_id, runtime.clone());
        runtime
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
    async fn push_event(&self, sequence: u64, event: InvocationEvent) {
        let sequenced = {
            let mut history = self.history.lock().await;
            let sequenced = SequencedInvocationEvent { sequence, event };
            history.items.push(sequenced.clone());
            sequenced
        };
        let _ = self.tx.send(sequenced);
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
        let sequence = self
            .db
            .append_invocation_event(self.invocation_id, &sse_event)
            .await? as i64;
        self.runtime.push_event(sequence as u64, sse_event).await;
        if let Some(persistence) = self.persistence().await? {
            match event.kind {
                ExecutionEventKind::DbtLog => {
                    if let Some(raw_line) = event.raw_line.as_deref()
                        && let Some(log_event) = LogEvent::parse(raw_line)
                    {
                        persistence
                            .persist_log_event(
                                &self.db,
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
                            .persist_raw_line(&self.db, persistence.run_id, sequence, raw_line)
                            .await?;
                    }
                }
                ExecutionEventKind::StderrLine => {}
            }
        }
        Ok(())
    }

    async fn complete(
        &self,
        worker_id: &str,
        lease_token: Uuid,
        completion: ExecutionCompletion,
    ) -> AppResult<()> {
        self.db
            .complete_invocation(self.invocation_id, worker_id, lease_token, &completion)
            .await?;
        let completed_event = InvocationEvent {
            event_type: "invocation.completed".to_string(),
            timestamp: Utc::now(),
            text: None,
            stream: None,
            dbt_event_name: None,
            node_unique_id: None,
            level: None,
            exit_code: Some(completion.exit_code),
            error: completion.error.clone(),
        };
        let sequence = self
            .db
            .append_invocation_event(self.invocation_id, &completed_event)
            .await;
        if let Ok(sequence) = sequence {
            self.runtime.push_event(sequence, completed_event).await;
        }
        Ok(())
    }

    async fn is_running(&self) -> bool {
        matches!(
            self.db.get_invocation_status(self.invocation_id).await,
            Ok(InvocationStatusResponse {
                status: InvocationLifecycleStatus::Running,
                ..
            })
        )
    }

    async fn persistence(&self) -> AppResult<Option<InvocationPersistence>> {
        let mut guard = self.runtime.persistence.lock().await;
        if guard.is_none() {
            let loaded = self
                .db
                .get_invocation_persistence(self.invocation_id, None, None)
                .await?;
            *guard = InvocationPersistence::from_record(loaded);
        }
        Ok(guard.clone())
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
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
        .route(
            "/v1/invocations",
            get(invocation_list).post(invocation_create),
        )
        .route("/v1/workers", get(worker_list))
        .route("/v1/queues", get(queue_list))
        .route("/v1/invocations/cleanup", post(invocation_cleanup))
        .route("/v1/invocations/claim-next", post(invocation_claim_next))
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

#[derive(Debug, Default, serde::Deserialize)]
struct InvocationEventsQuery {
    after_sequence: Option<u64>,
}

pub async fn serve(listen: &str, state: AppState) -> AppResult<()> {
    let addr: SocketAddr = listen.parse().map_err(|err| {
        AppError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid listen address '{listen}': {err}"),
        ))
    })?;
    info!(listen = %addr, "starting dbtx server");
    let timed_out_invocations = reconcile_timed_out_invocations(&state).await.unwrap_or(0);
    info!(
        listen = %addr,
        timed_out_invocations,
        "dbtx server execution state initialized"
    );
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(listen = %addr, "dbtx server listening");
    axum::serve(listener, router(state)).await.map_err(|err| {
        error!(error = %err, "dbtx server stopped with error");
        AppError::Io(err)
    })
}

async fn reconcile_timed_out_invocations(state: &AppState) -> AppResult<usize> {
    let timed_out = state
        .db
        .reconcile_timed_out_invocations(
            heartbeat_stale_timeout(crate::api::InvocationExecutionModeApi::Local),
            heartbeat_stale_timeout(crate::api::InvocationExecutionModeApi::Server),
        )
        .await?;
    publish_timed_out_invocations(state, &timed_out).await?;
    Ok(timed_out.len())
}

async fn publish_timed_out_invocations(
    state: &AppState,
    timed_out: &[TimedOutInvocationRecord],
) -> AppResult<()> {
    for timed_out_invocation in timed_out {
        publish_terminal_invocation(
            state,
            timed_out_invocation.invocation_id,
            timed_out_invocation.exit_code,
            timed_out_invocation.error.clone(),
        )
        .await?;
        info!(
            invocation_id = %timed_out_invocation.invocation_id,
            status = ?timed_out_invocation.status,
            error = %timed_out_invocation.error,
            "failed timed out invocation"
        );
    }
    Ok(())
}

async fn publish_terminal_invocation(
    state: &AppState,
    invocation_id: Uuid,
    exit_code: i32,
    error: String,
) -> AppResult<()> {
    let runtime = state.invocations.get_or_create(invocation_id, None).await;
    let completed_event = InvocationEvent {
        event_type: "invocation.completed".to_string(),
        timestamp: Utc::now(),
        text: None,
        stream: None,
        dbt_event_name: None,
        node_unique_id: None,
        level: None,
        exit_code: Some(exit_code),
        error: Some(error),
    };
    let sequence = state
        .db
        .append_invocation_event(invocation_id, &completed_event)
        .await?;
    runtime.push_event(sequence, completed_event).await;
    state.invocations.schedule_cleanup(invocation_id);
    Ok(())
}

async fn healthz() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
    })
}

async fn readyz(
    State(state): State<AppState>,
) -> Result<(StatusCode, Json<ReadyResponse>), ApiError> {
    let database = match state.db.ping().await {
        Ok(()) => "ok",
        Err(err) => {
            error!(error = %err, "readiness database check failed");
            return Ok((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ReadyResponse {
                    status: "not_ready".to_string(),
                    database: "error".to_string(),
                    schema: "unknown".to_string(),
                }),
            ));
        }
    };

    let (status_code, schema, status) = match state.db.require_current_schema().await {
        Ok(()) => (StatusCode::OK, "ok", "ready"),
        Err(AppError::SchemaOutOfDate) => {
            (StatusCode::SERVICE_UNAVAILABLE, "out_of_date", "not_ready")
        }
        Err(err) => {
            error!(error = %err, "readiness schema check failed");
            (StatusCode::SERVICE_UNAVAILABLE, "error", "not_ready")
        }
    };

    Ok((
        status_code,
        Json(ReadyResponse {
            status: status.to_string(),
            database: database.to_string(),
            schema: schema.to_string(),
        }),
    ))
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
            mode: request.mode,
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
            mode: request.mode,
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
            baseline: request.baseline,
            git_branch: request.git_branch,
            git_commit_sha: request.git_commit_sha,
            pr_number: request.pr_number,
            status: request.status,
            worker_queue: request.worker_queue,
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
            baseline: request.baseline,
            git_branch: request.git_branch,
            git_commit_sha: request.git_commit_sha,
            pr_number: request.pr_number,
            status: request.status,
            adapter_type: request.adapter_type,
            worker_queue: request.worker_queue,
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
        current_dir = request.current_dir.as_deref().unwrap_or(""),
        project_id = request.project_id.as_deref().unwrap_or(""),
        environment_slug = request.environment_slug.as_deref().unwrap_or(""),
        "starting invocation"
    );

    let runtime_config = state.runtime_config.clone();
    let db = state.db.clone();
    let service = InvocationService::new(&db);
    let execution_mode = match request.execution_mode {
        crate::api::InvocationExecutionModeApi::Server => ExecutionMode::Server,
        crate::api::InvocationExecutionModeApi::Local => ExecutionMode::Local,
    };
    let prepared = match request.execution_mode {
        crate::api::InvocationExecutionModeApi::Local => {
            let current_dir =
                request
                    .current_dir
                    .as_deref()
                    .ok_or(AppError::UnsupportedLocalExecution(
                        "local invocation requires current_dir".to_string(),
                    ))?;
            service
                .prepare_local_execution(
                    invocation_id,
                    InvocationRequest {
                        command: map_invocation_command(request.command),
                        args: request.args.iter().cloned().map(Into::into).collect(),
                        config: runtime_config,
                        current_dir: Some(PathBuf::from(current_dir)),
                        environment_slug: request.environment_slug.clone().unwrap_or_default(),
                        execution_mode,
                    },
                )
                .await?
        }
        crate::api::InvocationExecutionModeApi::Server => {
            let project_id = request
                .project_id
                .as_deref()
                .ok_or(AppError::RemoteExecutionRequiresProjectId)?;
            let environment_slug = request
                .environment_slug
                .as_deref()
                .ok_or(AppError::RemoteExecutionRequiresEnvironmentSlug)?;
            service
                .prepare_remote_execution(
                    invocation_id,
                    map_invocation_command(request.command),
                    request.args.iter().cloned().map(Into::into).collect(),
                    project_id,
                    environment_slug,
                )
                .await?
        }
    };
    let execution_spec = match prepared.spec {
        PreparedExecutionSpec::Local(spec) => InvocationExecutionSpecApi::Local {
            command: request.command,
            args: spec
                .args
                .into_iter()
                .map(|value| value.to_string_lossy().into_owned())
                .collect(),
            project_dir: request.current_dir.clone().unwrap_or_default(),
            profiles_yml: spec.profiles_yml,
            state_manifest: spec.state_manifest,
        },
        PreparedExecutionSpec::Remote(spec) => InvocationExecutionSpecApi::Remote {
            command: request.command,
            args: spec
                .args
                .into_iter()
                .map(|value| value.to_string_lossy().into_owned())
                .collect(),
            repo_url: spec.repo_url,
            commit_sha: spec.commit_sha,
            project_root: spec.project_root,
            profiles_yml: spec.profiles_yml,
            state_manifest: spec.state_manifest,
        },
    };
    let worker_queue = request
        .worker_queue
        .clone()
        .unwrap_or_else(|| prepared.worker_queue.clone());
    let persistence = prepared.persistence.map(|p| InvocationPersistence {
        run_id: p.run_id,
        project_id: p.project_id,
        environment_id: p.environment_id,
        promote_base_manifest: p.promote_base_manifest,
    });
    state
        .db
        .create_invocation(CreateInvocationInput {
            invocation_id,
            run_id: persistence.as_ref().map(|p| p.run_id),
            project_id: prepared.project_id,
            environment_id: prepared.environment_id,
            command: map_invocation_command(request.command).as_str().to_string(),
            execution_mode: request.execution_mode,
            worker_queue: worker_queue.clone(),
            execution_spec: Some(execution_spec),
            promote_base_manifest: persistence
                .as_ref()
                .map(|p| p.promote_base_manifest)
                .unwrap_or(false),
            claim_deadline_at: Some(
                Utc::now()
                    + chrono::Duration::from_std(claim_startup_timeout(request.execution_mode))
                        .expect("duration"),
            ),
        })
        .await?;
    let runtime = state
        .invocations
        .get_or_create(invocation_id, persistence)
        .await;
    let started_event = InvocationEvent {
        event_type: "invocation.started".to_string(),
        timestamp: Utc::now(),
        text: None,
        stream: None,
        dbt_event_name: None,
        node_unique_id: None,
        level: None,
        exit_code: None,
        error: None,
    };
    let started_sequence = state
        .db
        .append_invocation_event(invocation_id, &started_event)
        .await;
    if let Ok(sequence) = started_sequence {
        runtime.push_event(sequence, started_event).await;
    }
    info!(
        invocation_id = %invocation_id,
        execution_mode = ?request.execution_mode,
        "created worker-claimable invocation"
    );
    Ok(Json(InvocationCreateResponse {
        invocation_id,
        execution_mode: request.execution_mode,
        worker_queue,
    }))
}

async fn invocation_claim_next(
    State(state): State<AppState>,
    Json(request): Json<InvocationClaimNextApiRequest>,
) -> Result<Response, ApiError> {
    reconcile_timed_out_invocations(&state).await?;
    let Some(claimed) = state
        .db
        .claim_next_invocation(
            &request.worker_id,
            request.execution_mode,
            request.worker_queue.as_deref(),
        )
        .await?
    else {
        return Ok(StatusCode::NO_CONTENT.into_response());
    };
    state
        .invocations
        .get_or_create(claimed.invocation_id, None)
        .await;
    info!(invocation_id = %claimed.invocation_id, "claimed next invocation execution");
    Ok(Json(claimed).into_response())
}

async fn invocation_heartbeat(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(request): Json<InvocationHeartbeatApiRequest>,
) -> Result<Json<InvocationHeartbeatResponse>, ApiError> {
    reconcile_timed_out_invocations(&state).await?;
    let cancel_requested = state
        .db
        .heartbeat_invocation(id, &request.worker_id, request.lease_token)
        .await?;
    Ok(Json(InvocationHeartbeatResponse { cancel_requested }))
}

async fn invocation_cancel(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(_request): Json<InvocationCancelApiRequest>,
) -> Result<StatusCode, ApiError> {
    reconcile_timed_out_invocations(&state).await?;
    if let Some(InvocationCancellationRecord {
        invocation_id,
        status,
        exit_code,
        error,
    }) = state.db.request_cancel_invocation(id).await?
    {
        publish_terminal_invocation(&state, invocation_id, exit_code, error.clone()).await?;
        info!(invocation_id = %id, status = ?status, error = %error, "canceled unclaimed invocation immediately");
        return Ok(StatusCode::NO_CONTENT);
    }
    info!(invocation_id = %id, "requested invocation cancel");
    Ok(StatusCode::NO_CONTENT)
}

async fn invocation_status(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<InvocationStatusResponse>, ApiError> {
    reconcile_timed_out_invocations(&state).await?;
    info!(invocation_id = %id, "loaded invocation status");
    Ok(Json(state.db.get_invocation_status(id).await?))
}

async fn invocation_list(
    State(state): State<AppState>,
    Query(filter): Query<InvocationListApiRequest>,
) -> Result<Json<InvocationsResponse>, ApiError> {
    reconcile_timed_out_invocations(&state).await?;
    let invocations = state.db.list_invocations(filter).await?;
    info!(count = invocations.len(), "listed invocations");
    Ok(Json(InvocationsResponse { invocations }))
}

async fn worker_list(State(state): State<AppState>) -> Result<Json<WorkersResponse>, ApiError> {
    let workers = state.db.list_workers().await?;
    info!(count = workers.len(), "listed workers");
    Ok(Json(WorkersResponse { workers }))
}

async fn queue_list(State(state): State<AppState>) -> Result<Json<QueuesResponse>, ApiError> {
    let queues = state.db.list_queues().await?;
    info!(count = queues.len(), "listed queues");
    Ok(Json(QueuesResponse { queues }))
}

async fn invocation_cleanup(
    State(state): State<AppState>,
    Json(request): Json<InvocationCleanupApiRequest>,
) -> Result<Json<InvocationCleanupResponse>, ApiError> {
    if request.older_than_seconds <= 0 {
        return Err(ApiError(AppError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "older_than_seconds must be greater than 0",
        ))));
    }
    let cutoff = Utc::now() - chrono::Duration::seconds(request.older_than_seconds);
    let deleted = state
        .db
        .cleanup_terminal_invocations_older_than(cutoff)
        .await?;
    info!(
        older_than_seconds = request.older_than_seconds,
        deleted, "cleaned up terminal invocations"
    );
    Ok(Json(InvocationCleanupResponse { deleted }))
}

async fn invocation_append_events(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(request): Json<InvocationEventBatchApiRequest>,
) -> Result<StatusCode, ApiError> {
    reconcile_timed_out_invocations(&state).await?;
    let runtime = state.invocations.get_or_create(id, None).await;
    let recorder = InvocationRecorder {
        db: state.db.clone(),
        invocation_id: id,
        runtime,
    };
    if !recorder.is_running().await {
        return Err(ApiError(AppError::Io(std::io::Error::other(
            "invocation is already completed",
        ))));
    }
    state
        .db
        .get_invocation_persistence(id, Some(&request.worker_id), Some(request.lease_token))
        .await?;
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
    reconcile_timed_out_invocations(&state).await?;
    let runtime = state.invocations.get_or_create(id, None).await;
    let recorder = InvocationRecorder {
        db: state.db.clone(),
        invocation_id: id,
        runtime,
    };
    state
        .db
        .get_invocation_persistence(id, Some(&request.worker_id), Some(request.lease_token))
        .await?;
    recorder
        .complete(&request.worker_id, request.lease_token, request.completion)
        .await?;
    state.invocations.schedule_cleanup(id);
    info!(invocation_id = %id, "completed invocation via api");
    Ok(StatusCode::NO_CONTENT)
}

async fn invocation_events(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Query(query): Query<InvocationEventsQuery>,
    headers: axum::http::HeaderMap,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let runtime = state.invocations.get_or_create(id, None).await;
    let rx = runtime.tx.subscribe();
    let header_resume = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    let after_sequence = query.after_sequence.or(header_resume).unwrap_or(0);
    let history = state
        .db
        .load_invocation_events_since(id, after_sequence)
        .await?;
    let buffered_events = history.len();
    let last_sequence = history.last().map(|item| item.0).unwrap_or(after_sequence);
    let stream = event_stream(
        history
            .into_iter()
            .map(|(sequence, event)| SequencedInvocationEvent { sequence, event })
            .collect(),
        last_sequence,
        rx,
    );
    info!(invocation_id = %id, buffered_events, "subscribed to invocation event stream");
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

fn event_stream(
    history: Vec<SequencedInvocationEvent>,
    last_history_sequence: u64,
    mut rx: broadcast::Receiver<SequencedInvocationEvent>,
) -> impl Stream<Item = Result<Event, Infallible>> {
    stream! {
        let mut last_seen_sequence = last_history_sequence;
        for item in history {
            last_seen_sequence = item.sequence;
            yield Ok(to_sse_event(&item));
        }
        loop {
            match rx.recv().await {
                Ok(item) if item.sequence > last_seen_sequence => {
                    last_seen_sequence = item.sequence;
                    yield Ok(to_sse_event(&item))
                }
                Ok(_) => continue,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    }
}

fn to_sse_event(item: &SequencedInvocationEvent) -> Event {
    Event::default()
        .event(item.event.event_type.clone())
        .id(item.sequence.to_string())
        .data(serde_json::to_string(&item.event).unwrap_or_else(|_| "{}".to_string()))
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
            | AppError::RemoteExecutionRequiresProjectId
            | AppError::RemoteExecutionRequiresEnvironmentSlug
            | AppError::RemoteExecutionRequiresRemoteProject(_, _)
            | AppError::RemoteExecutionRequiresGitRepoUrl(_)
            | AppError::RemoteExecutionRequiresProjectRoot(_)
            | AppError::RemoteExecutionRequiresCommitSha(_, _)
            | AppError::MissingDatabaseUrl
            | AppError::UserStateNotAllowed
            | AppError::UserTargetNotAllowed
            | AppError::UserProfilesDirNotAllowed
            | AppError::InvalidProjectMode(_)
            | AppError::InvalidEnvironmentStatus(_)
            | AppError::RemoteProjectEnvironmentRequiresSha(_, _)
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
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let body = serde_json::json!({ "error": self.0.to_string() });
        (status, Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::{InvocationEvent, SequencedInvocationEvent, event_stream};
    use chrono::Utc;
    use futures_util::StreamExt;
    use tokio::sync::broadcast;

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
}
