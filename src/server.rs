use crate::api::{
    ApiErrorResponse, EnvironmentReleaseApiRequest, EnvironmentResponse, EnvironmentRollbackApiRequest,
    EnvironmentVersionsResponse, EnvironmentsResponse, HealthResponse, InvocationCancelApiRequest,
    InvocationCancelStateApi, InvocationClaimNextApiRequest, InvocationClaimResponse,
    InvocationCleanupApiRequest, InvocationCleanupResponse, InvocationCommandApi,
    InvocationCompleteApiRequest, InvocationCreateApiRequest, InvocationCreateResponse,
    InvocationEvent, InvocationEventBatchApiRequest, InvocationExecutionModeApi,
    InvocationExecutionSpecApi, InvocationHeartbeatApiRequest, InvocationHeartbeatResponse,
    InvocationLifecycleStatus, InvocationListApiRequest, InvocationStatusResponse,
    InvocationWorkerHealthApi, InvocationsResponse, MigrateResponse, ProjectDeleteResponse,
    ProjectDraftCreateApiRequest, ProjectDraftResponse, ProjectDraftValidateResponse,
    ProjectResponse, ProjectUpdateApiRequest, ProjectsResponse, QueueStatusResponse,
    QueuesResponse, ReadyResponse, WorkerStatusResponse, WorkersResponse,
};
use crate::config::RuntimeConfig;
use crate::db::{
    AppliedMigration, CreateInvocationInput, Db, EnvironmentRecord, EnvironmentVersionRecord,
    InvocationCancellationRecord, InvocationPersistenceRecord, ProjectRecord,
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
    EnvironmentReleaseRequest, EnvironmentRollbackRequest, EnvironmentService, InvocationCommand,
    InvocationRequest, InvocationService, PreparedExecutionSpec, ProjectCreateRequest,
    ProjectService, ProjectUpdateRequest,
};
use async_stream::stream;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse, Response};
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
use utoipa::OpenApi;
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

    pub(crate) fn db(&self) -> &Db {
        &self.db
    }

    pub(crate) async fn bootstrap_invocation_started(
        &self,
        invocation_id: Uuid,
        persistence: Option<InvocationPersistence>,
    ) -> AppResult<()> {
        let runtime = self.invocations.get_or_create(invocation_id, persistence).await;
        let started_event = started_invocation_event();
        let sequence = self
            .db
            .append_invocation_event(invocation_id, &started_event)
            .await?;
        runtime.push_event(sequence, started_event).await;
        Ok(())
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
pub(crate) struct InvocationPersistence {
    run_id: Uuid,
    project_id: i64,
    environment_id: i64,
    promote_base_manifest: bool,
}

impl InvocationPersistence {
    fn from_record(record: InvocationPersistenceRecord) -> Option<Self> {
        Some(Self {
            run_id: record.run_id?,
            project_id: record.project_id?,
            environment_id: record.environment_id?,
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
    pub(crate) async fn push_event(&self, sequence: u64, event: InvocationEvent) {
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

pub(crate) fn invocation_claim_deadline_at(
    execution_mode: InvocationExecutionModeApi,
) -> chrono::DateTime<Utc> {
    Utc::now()
        + chrono::Duration::from_std(claim_startup_timeout(execution_mode)).expect("duration")
}

fn started_invocation_event() -> InvocationEvent {
    InvocationEvent {
        event_type: "invocation.started".to_string(),
        timestamp: Utc::now(),
        text: None,
        stream: None,
        dbt_event_name: None,
        node_unique_id: None,
        level: None,
        exit_code: None,
        error: None,
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .merge(crate::ui::router())
        .route("/openapi.json", get(openapi_json))
        .route("/docs", get(swagger_docs))
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/v1/state/migrate", post(migrate))
        .route("/v1/projects", get(projects_list))
        .route(
            "/v1/projects/{project_id}",
            patch(project_update).get(project_get).delete(project_delete),
        )
        .route("/v1/project-drafts", post(project_draft_create))
        .route("/v1/project-drafts/{draft_id}", get(project_draft_get))
        .route(
            "/v1/project-drafts/{draft_id}/validate",
            post(project_draft_validate),
        )
        .route(
            "/v1/project-drafts/{draft_id}/confirm",
            post(project_draft_confirm),
        )
        .route(
            "/v1/projects/{project_id}/environments",
            get(environment_list),
        )
        .route(
            "/v1/projects/{project_id}/environments/{slug}",
            get(environment_get),
        )
        .route(
            "/v1/projects/{project_id}/environments/{slug}/release",
            post(environment_release),
        )
        .route(
            "/v1/projects/{project_id}/environments/{slug}/history",
            get(environment_history),
        )
        .route(
            "/v1/projects/{project_id}/environments/{slug}/rollback",
            post(environment_rollback),
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

async fn openapi_json() -> Json<utoipa::openapi::OpenApi> {
    Json(ApiDoc::openapi())
}

async fn swagger_docs() -> Html<&'static str> {
    Html(
        r#"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>dbtx API Docs</title>
    <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
  </head>
  <body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
    <script>
      window.ui = SwaggerUIBundle({
        url: '/openapi.json',
        dom_id: '#swagger-ui',
        deepLinking: true,
        presets: [SwaggerUIBundle.presets.apis],
      });
    </script>
  </body>
</html>"#,
    )
}

#[derive(Debug, Default, serde::Deserialize)]
struct InvocationEventsQuery {
    after_sequence: Option<u64>,
}

#[derive(OpenApi)]
#[openapi(
    paths(
        migrate,
        project_draft_create,
        project_draft_get,
        project_draft_validate,
        project_draft_confirm,
        projects_list,
        project_get,
        project_update,
        project_delete,
        environment_list,
        environment_get,
        environment_release,
        environment_history,
        environment_rollback,
        invocation_create,
        invocation_list,
        worker_list,
        queue_list,
        invocation_cleanup,
        invocation_claim_next,
        invocation_status,
        invocation_heartbeat,
        invocation_cancel,
        invocation_append_events,
        invocation_complete,
        invocation_events
    ),
    components(
        schemas(
            ApiErrorResponse,
            MigrateResponse,
            ProjectResponse,
            ProjectsResponse,
            ProjectDraftResponse,
            ProjectDraftValidateResponse,
            ProjectDraftCreateApiRequest,
            ProjectUpdateApiRequest,
            EnvironmentResponse,
            EnvironmentsResponse,
            EnvironmentVersionsResponse,
            EnvironmentReleaseApiRequest,
            EnvironmentRollbackApiRequest,
            InvocationCreateApiRequest,
            InvocationCreateResponse,
            InvocationsResponse,
            InvocationListApiRequest,
            InvocationCleanupApiRequest,
            InvocationCleanupResponse,
            InvocationClaimNextApiRequest,
            InvocationClaimResponse,
            InvocationHeartbeatApiRequest,
            InvocationHeartbeatResponse,
            InvocationCancelApiRequest,
            InvocationStatusResponse,
            InvocationEventBatchApiRequest,
            InvocationCompleteApiRequest,
            InvocationEvent,
            InvocationCommandApi,
            InvocationExecutionModeApi,
            InvocationExecutionSpecApi,
            InvocationLifecycleStatus,
            InvocationWorkerHealthApi,
            InvocationCancelStateApi,
            WorkersResponse,
            WorkerStatusResponse,
            QueuesResponse,
            QueueStatusResponse,
            AppliedMigration,
            ProjectRecord,
            EnvironmentRecord,
            EnvironmentVersionRecord,
            ExecutionEvent,
            ExecutionEventKind,
            ExecutionCompletion
        )
    ),
    tags(
        (name = "state", description = "Database and schema operations"),
        (name = "projects", description = "Project management"),
        (name = "environments", description = "Environment management and releases"),
        (name = "invocations", description = "Invocation lifecycle and event streaming"),
        (name = "workers", description = "Worker and queue operational views")
    )
)]
struct ApiDoc;

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

#[utoipa::path(
    post,
    path = "/v1/state/migrate",
    tag = "state",
    responses(
        (status = 200, description = "Applied migrations", body = MigrateResponse),
        (status = 500, description = "Migration failed", body = ApiErrorResponse)
    )
)]
async fn migrate(State(state): State<AppState>) -> Result<Json<MigrateResponse>, ApiError> {
    let applied = state.db.migrate().await?;
    info!(applied = applied.len(), "applied database migrations");
    Ok(Json(MigrateResponse { applied }))
}

#[utoipa::path(
    patch,
    path = "/v1/projects/{project_id}",
    tag = "projects",
    params(
        ("project_id" = String, Path, description = "Project identifier")
    ),
    request_body = ProjectUpdateApiRequest,
    responses(
        (status = 200, description = "Updated project", body = ProjectResponse),
        (status = 400, description = "Invalid request", body = ApiErrorResponse),
        (status = 404, description = "Project not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
async fn project_update(
    State(state): State<AppState>,
    Path(project_id): Path<String>,
    Json(request): Json<ProjectUpdateApiRequest>,
) -> Result<Json<ProjectResponse>, ApiError> {
    let service = ProjectService::new(&state.db);
    let project = service
        .update(ProjectUpdateRequest {
            project: project_id,
            git_repo_url: request.git_repo_url,
            project_root: request.project_root,
        })
        .await?;
    info!(project_id = %project.project_id, project_name = %project.project_name, "updated project");
    Ok(Json(ProjectResponse { project }))
}

#[utoipa::path(
    delete,
    path = "/v1/projects/{project_id}",
    tag = "projects",
    params(
        ("project_id" = String, Path, description = "Project identifier")
    ),
    responses(
        (status = 200, description = "Deleted project", body = ProjectDeleteResponse),
        (status = 404, description = "Project not found", body = ApiErrorResponse),
        (status = 409, description = "Project deletion blocked", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
async fn project_delete(
    State(state): State<AppState>,
    Path(project_id): Path<String>,
) -> Result<Json<ProjectDeleteResponse>, ApiError> {
    ProjectService::new(&state.db)
        .delete(project_id.clone())
        .await?;
    info!(project_id = %project_id, "deleted project");
    Ok(Json(ProjectDeleteResponse { deleted_project_id: project_id }))
}

#[utoipa::path(
    post,
    path = "/v1/project-drafts",
    tag = "projects",
    request_body = ProjectDraftCreateApiRequest,
    responses(
        (status = 200, description = "Created project draft", body = ProjectDraftResponse),
        (status = 400, description = "Invalid request", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
async fn project_draft_create(
    State(state): State<AppState>,
    Json(request): Json<ProjectDraftCreateApiRequest>,
) -> Result<Json<ProjectDraftResponse>, ApiError> {
    let service = ProjectService::new(&state.db);
    let draft = service
        .create_draft(ProjectCreateRequest {
            git_repo_url: request.git_repo_url,
            project_root: request.project_root,
        })
        .await?;
    Ok(Json(ProjectDraftResponse { draft }))
}

#[utoipa::path(
    get,
    path = "/v1/project-drafts/{draft_id}",
    tag = "projects",
    params(
        ("draft_id" = Uuid, Path, description = "Draft identifier")
    ),
    responses(
        (status = 200, description = "Project draft", body = ProjectDraftResponse),
        (status = 404, description = "Draft not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
async fn project_draft_get(
    State(state): State<AppState>,
    Path(draft_id): Path<Uuid>,
) -> Result<Json<ProjectDraftResponse>, ApiError> {
    let draft = ProjectService::new(&state.db).get_draft(draft_id).await?;
    Ok(Json(ProjectDraftResponse { draft }))
}

#[utoipa::path(
    post,
    path = "/v1/project-drafts/{draft_id}/validate",
    tag = "projects",
    params(
        ("draft_id" = Uuid, Path, description = "Draft identifier")
    ),
    responses(
        (status = 200, description = "Started draft validation", body = ProjectDraftValidateResponse),
        (status = 404, description = "Draft not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
async fn project_draft_validate(
    State(state): State<AppState>,
    Path(draft_id): Path<Uuid>,
) -> Result<Json<ProjectDraftValidateResponse>, ApiError> {
    let prepared = ProjectService::new(&state.db)
        .prepare_draft_validation(draft_id)
        .await?;
    let invocation_id = prepared.invocation_id;
    state
        .db
        .create_invocation(CreateInvocationInput {
            invocation_id,
            run_id: None,
            project_id: None,
            environment_id: None,
            project_draft_id: Some(prepared.draft.id),
            environment_draft_id: None,
            command: InvocationCommand::ProjectValidate.as_str().to_string(),
            execution_mode: InvocationExecutionModeApi::Server,
            worker_queue: prepared.worker_queue,
            execution_spec: Some(InvocationExecutionSpecApi::ProjectValidation {
                repo_url: prepared.spec.repo_url,
                project_root: prepared.spec.project_root,
            }),
            promote_base_manifest: false,
            claim_deadline_at: Some(invocation_claim_deadline_at(
                InvocationExecutionModeApi::Server,
            )),
        })
        .await?;
    state.bootstrap_invocation_started(invocation_id, None).await?;
    Ok(Json(ProjectDraftValidateResponse {
        draft: prepared.draft,
        invocation_id,
    }))
}

#[utoipa::path(
    post,
    path = "/v1/project-drafts/{draft_id}/confirm",
    tag = "projects",
    params(
        ("draft_id" = Uuid, Path, description = "Draft identifier")
    ),
    responses(
        (status = 200, description = "Confirmed project", body = ProjectResponse),
        (status = 400, description = "Draft not validated", body = ApiErrorResponse),
        (status = 404, description = "Draft not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
async fn project_draft_confirm(
    State(state): State<AppState>,
    Path(draft_id): Path<Uuid>,
) -> Result<Json<ProjectResponse>, ApiError> {
    let project = ProjectService::new(&state.db)
        .confirm_draft(draft_id)
        .await?;
    Ok(Json(ProjectResponse { project }))
}

#[utoipa::path(
    get,
    path = "/v1/projects",
    tag = "projects",
    responses(
        (status = 200, description = "Projects", body = ProjectsResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
async fn projects_list(State(state): State<AppState>) -> Result<Json<ProjectsResponse>, ApiError> {
    let service = ProjectService::new(&state.db);
    let projects = service.list().await?;
    info!(count = projects.len(), "listed projects");
    Ok(Json(ProjectsResponse { projects }))
}

#[utoipa::path(
    get,
    path = "/v1/projects/{project_id}",
    tag = "projects",
    params(
        ("project_id" = String, Path, description = "Project identifier")
    ),
    responses(
        (status = 200, description = "Project", body = ProjectResponse),
        (status = 404, description = "Project not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
async fn project_get(
    State(state): State<AppState>,
    Path(project_id): Path<String>,
) -> Result<Json<ProjectResponse>, ApiError> {
    let project = state.db.get_project_by_project_id(&project_id).await?;
    info!(project_id = %project.project_id, "loaded project");
    Ok(Json(ProjectResponse { project }))
}

#[utoipa::path(
    get,
    path = "/v1/projects/{project_id}/environments",
    tag = "environments",
    params(
        ("project_id" = String, Path, description = "Project identifier")
    ),
    responses(
        (status = 200, description = "Environments", body = EnvironmentsResponse),
        (status = 404, description = "Project not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
async fn environment_list(
    State(state): State<AppState>,
    Path(project_id): Path<String>,
) -> Result<Json<EnvironmentsResponse>, ApiError> {
    let service = EnvironmentService::new(&state.db);
    let environments = service.list(project_id).await?;
    info!(count = environments.len(), "listed environments");
    Ok(Json(EnvironmentsResponse { environments }))
}

#[utoipa::path(
    get,
    path = "/v1/projects/{project_id}/environments/{slug}",
    tag = "environments",
    params(
        ("project_id" = String, Path, description = "Project identifier"),
        ("slug" = String, Path, description = "Environment slug")
    ),
    responses(
        (status = 200, description = "Environment", body = EnvironmentResponse),
        (status = 404, description = "Environment not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
async fn environment_get(
    State(state): State<AppState>,
    Path((project_id, slug)): Path<(String, String)>,
) -> Result<Json<EnvironmentResponse>, ApiError> {
    let service = EnvironmentService::new(&state.db);
    let environment = service.show(project_id, slug).await?;
    info!(
        project_id = %environment.project_ref,
        environment = %environment.slug,
        "loaded environment"
    );
    Ok(Json(EnvironmentResponse { environment }))
}

#[utoipa::path(
    post,
    path = "/v1/projects/{project_id}/environments/{slug}/release",
    tag = "environments",
    params(
        ("project_id" = String, Path, description = "Project identifier"),
        ("slug" = String, Path, description = "Environment slug")
    ),
    request_body = EnvironmentReleaseApiRequest,
    responses(
        (status = 200, description = "Released environment", body = EnvironmentResponse),
        (status = 400, description = "Invalid request", body = ApiErrorResponse),
        (status = 404, description = "Environment not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
async fn environment_release(
    State(state): State<AppState>,
    Path((project_id, slug)): Path<(String, String)>,
    Json(request): Json<EnvironmentReleaseApiRequest>,
) -> Result<Json<EnvironmentResponse>, ApiError> {
    let service = EnvironmentService::new(&state.db);
    let environment = service
        .release(EnvironmentReleaseRequest {
            project: project_id,
            slug,
            git_branch: request.git_branch,
            git_commit_sha: request.git_commit_sha,
            git_ref: request.git_ref,
        })
        .await?;
    info!(
        project_id = %environment.project_ref,
        environment = %environment.slug,
        git_commit_sha = %environment.git_commit_sha.as_deref().unwrap_or(""),
        "released environment"
    );
    Ok(Json(EnvironmentResponse { environment }))
}

#[utoipa::path(
    get,
    path = "/v1/projects/{project_id}/environments/{slug}/history",
    tag = "environments",
    params(
        ("project_id" = String, Path, description = "Project identifier"),
        ("slug" = String, Path, description = "Environment slug")
    ),
    responses(
        (status = 200, description = "Environment version history", body = EnvironmentVersionsResponse),
        (status = 404, description = "Environment not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
async fn environment_history(
    State(state): State<AppState>,
    Path((project_id, slug)): Path<(String, String)>,
) -> Result<Json<EnvironmentVersionsResponse>, ApiError> {
    let service = EnvironmentService::new(&state.db);
    let versions = service.history(project_id, slug).await?;
    Ok(Json(EnvironmentVersionsResponse { versions }))
}

#[utoipa::path(
    post,
    path = "/v1/projects/{project_id}/environments/{slug}/rollback",
    tag = "environments",
    params(
        ("project_id" = String, Path, description = "Project identifier"),
        ("slug" = String, Path, description = "Environment slug")
    ),
    request_body = EnvironmentRollbackApiRequest,
    responses(
        (status = 200, description = "Rolled back environment", body = EnvironmentResponse),
        (status = 400, description = "Invalid request", body = ApiErrorResponse),
        (status = 404, description = "Environment or version not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
async fn environment_rollback(
    State(state): State<AppState>,
    Path((project_id, slug)): Path<(String, String)>,
    Json(request): Json<EnvironmentRollbackApiRequest>,
) -> Result<Json<EnvironmentResponse>, ApiError> {
    let service = EnvironmentService::new(&state.db);
    let environment = service
        .rollback(EnvironmentRollbackRequest {
            project: project_id,
            slug,
            version_id: request.version_id,
        })
        .await?;
    info!(
        project_id = %environment.project_ref,
        environment = %environment.slug,
        git_commit_sha = %environment.git_commit_sha.as_deref().unwrap_or(""),
        "rolled back environment"
    );
    Ok(Json(EnvironmentResponse { environment }))
}

#[utoipa::path(
    post,
    path = "/v1/invocations",
    tag = "invocations",
    request_body = InvocationCreateApiRequest,
    responses(
        (status = 200, description = "Created invocation", body = InvocationCreateResponse),
        (status = 400, description = "Invalid request", body = ApiErrorResponse),
        (status = 404, description = "Project or environment not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
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
    let derived_execution_mode = if let Some(project_id) = request.project_id.as_deref() {
        let environment_slug = request
            .environment_slug
            .as_deref()
            .ok_or(AppError::RemoteExecutionRequiresEnvironmentSlug)?;
        let environment = db.get_environment(project_id, environment_slug).await?;
        let project = db.get_project_by_project_id(&environment.project_ref).await?;
        match project.mode.as_str() {
            "remote" => InvocationExecutionModeApi::Server,
            "local" => InvocationExecutionModeApi::Local,
            other => return Err(ApiError(AppError::InvalidProjectMode(other.to_string()))),
        }
    } else {
        InvocationExecutionModeApi::Local
    };
    let execution_mode = match derived_execution_mode {
        crate::api::InvocationExecutionModeApi::Server => ExecutionMode::Server,
        crate::api::InvocationExecutionModeApi::Local => ExecutionMode::Local,
    };
    let prepared = match derived_execution_mode {
        crate::api::InvocationExecutionModeApi::Local => {
            if matches!(request.command, InvocationCommandApi::Release) {
                return Err(ApiError(AppError::UnsupportedLocalExecution(
                    "release".to_string(),
                )));
            }
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
            match request.command {
                InvocationCommandApi::Release => {
                    service
                        .prepare_release_validation(
                            request.args.iter().cloned().map(Into::into).collect(),
                            project_id,
                            environment_slug,
                        )
                        .await?
                }
                _ => {
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
            }
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
            project_dir: spec.project_dir.display().to_string(),
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
        PreparedExecutionSpec::ReleaseValidation(spec) => {
            InvocationExecutionSpecApi::ReleaseValidation {
                repo_url: spec.repo_url,
                git_ref: spec.git_ref,
                git_commit_sha: spec.git_commit_sha,
                git_branch: spec.git_branch,
            }
        }
        PreparedExecutionSpec::ProjectValidation(spec) => {
            InvocationExecutionSpecApi::ProjectValidation {
                repo_url: spec.repo_url,
                project_root: spec.project_root,
            }
        }
        PreparedExecutionSpec::EnvironmentPrepare(spec) => {
            InvocationExecutionSpecApi::EnvironmentPrepare {
                repo_url: spec.repo_url,
                selected_branch: spec.selected_branch,
            }
        }
        PreparedExecutionSpec::EnvironmentValidate(spec) => {
            InvocationExecutionSpecApi::EnvironmentValidate {
                repo_url: spec.repo_url,
                commit_sha: spec.commit_sha,
                project_root: spec.project_root,
                selected_branch: spec.selected_branch,
                profiles_yml: spec.profiles_yml,
            }
        }
    };
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
            project_draft_id: prepared.project_draft_id,
            environment_draft_id: prepared.environment_draft_id,
            command: map_invocation_command(request.command).as_str().to_string(),
            execution_mode: derived_execution_mode,
            worker_queue: prepared.worker_queue.clone(),
            execution_spec: Some(execution_spec),
            promote_base_manifest: persistence
                .as_ref()
                .map(|p| p.promote_base_manifest)
                .unwrap_or(false),
            claim_deadline_at: Some(invocation_claim_deadline_at(derived_execution_mode)),
        })
        .await?;
    state
        .bootstrap_invocation_started(invocation_id, persistence)
        .await?;
    info!(
        invocation_id = %invocation_id,
        execution_mode = ?derived_execution_mode,
        "created worker-claimable invocation"
    );
    Ok(Json(InvocationCreateResponse {
        invocation_id,
        execution_mode: derived_execution_mode,
        worker_queue: prepared.worker_queue,
    }))
}

#[utoipa::path(
    post,
    path = "/v1/invocations/claim-next",
    tag = "invocations",
    request_body = InvocationClaimNextApiRequest,
    responses(
        (status = 200, description = "Claimed invocation", body = InvocationClaimResponse),
        (status = 204, description = "No work available"),
        (status = 400, description = "Invalid request", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
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

#[utoipa::path(
    post,
    path = "/v1/invocations/{id}/heartbeat",
    tag = "invocations",
    params(
        ("id" = Uuid, Path, description = "Invocation identifier")
    ),
    request_body = InvocationHeartbeatApiRequest,
    responses(
        (status = 200, description = "Heartbeat accepted", body = InvocationHeartbeatResponse),
        (status = 400, description = "Invalid request", body = ApiErrorResponse),
        (status = 404, description = "Invocation not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
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

#[utoipa::path(
    post,
    path = "/v1/invocations/{id}/cancel",
    tag = "invocations",
    params(
        ("id" = Uuid, Path, description = "Invocation identifier")
    ),
    request_body = InvocationCancelApiRequest,
    responses(
        (status = 204, description = "Cancel requested"),
        (status = 404, description = "Invocation not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
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

#[utoipa::path(
    get,
    path = "/v1/invocations/{id}",
    tag = "invocations",
    params(
        ("id" = Uuid, Path, description = "Invocation identifier")
    ),
    responses(
        (status = 200, description = "Invocation status", body = InvocationStatusResponse),
        (status = 404, description = "Invocation not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
async fn invocation_status(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<InvocationStatusResponse>, ApiError> {
    reconcile_timed_out_invocations(&state).await?;
    info!(invocation_id = %id, "loaded invocation status");
    Ok(Json(state.db.get_invocation_status(id).await?))
}

#[utoipa::path(
    get,
    path = "/v1/invocations",
    tag = "invocations",
    params(
        ("status" = Option<InvocationLifecycleStatus>, Query, description = "Filter by lifecycle status"),
        ("execution_mode" = Option<InvocationExecutionModeApi>, Query, description = "Filter by execution mode"),
        ("worker_queue" = Option<String>, Query, description = "Filter by worker queue"),
        ("claimed_by" = Option<String>, Query, description = "Filter by worker id"),
        ("cancel_state" = Option<InvocationCancelStateApi>, Query, description = "Filter by cancel state"),
        ("limit" = Option<i64>, Query, description = "Limit result count")
    ),
    responses(
        (status = 200, description = "Invocations", body = InvocationsResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
async fn invocation_list(
    State(state): State<AppState>,
    Query(filter): Query<InvocationListApiRequest>,
) -> Result<Json<InvocationsResponse>, ApiError> {
    reconcile_timed_out_invocations(&state).await?;
    let invocations = state.db.list_invocations(filter).await?;
    info!(count = invocations.len(), "listed invocations");
    Ok(Json(InvocationsResponse { invocations }))
}

#[utoipa::path(
    get,
    path = "/v1/workers",
    tag = "workers",
    responses(
        (status = 200, description = "Worker operational view", body = WorkersResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
async fn worker_list(State(state): State<AppState>) -> Result<Json<WorkersResponse>, ApiError> {
    let workers = state.db.list_workers().await?;
    info!(count = workers.len(), "listed workers");
    Ok(Json(WorkersResponse { workers }))
}

#[utoipa::path(
    get,
    path = "/v1/queues",
    tag = "workers",
    responses(
        (status = 200, description = "Queue operational view", body = QueuesResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
async fn queue_list(State(state): State<AppState>) -> Result<Json<QueuesResponse>, ApiError> {
    let queues = state.db.list_queues().await?;
    info!(count = queues.len(), "listed queues");
    Ok(Json(QueuesResponse { queues }))
}

#[utoipa::path(
    post,
    path = "/v1/invocations/cleanup",
    tag = "invocations",
    request_body = InvocationCleanupApiRequest,
    responses(
        (status = 200, description = "Deleted old terminal invocations", body = InvocationCleanupResponse),
        (status = 400, description = "Invalid request", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
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

#[utoipa::path(
    post,
    path = "/v1/invocations/{id}/events",
    tag = "invocations",
    params(
        ("id" = Uuid, Path, description = "Invocation identifier")
    ),
    request_body = InvocationEventBatchApiRequest,
    responses(
        (status = 204, description = "Events appended"),
        (status = 400, description = "Invalid request", body = ApiErrorResponse),
        (status = 404, description = "Invocation not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
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

#[utoipa::path(
    post,
    path = "/v1/invocations/{id}/complete",
    tag = "invocations",
    params(
        ("id" = Uuid, Path, description = "Invocation identifier")
    ),
    request_body = InvocationCompleteApiRequest,
    responses(
        (status = 204, description = "Invocation completed"),
        (status = 400, description = "Invalid request", body = ApiErrorResponse),
        (status = 404, description = "Invocation not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
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

#[utoipa::path(
    get,
    path = "/v1/invocations/{id}/events",
    tag = "invocations",
    params(
        ("id" = Uuid, Path, description = "Invocation identifier"),
        ("after_sequence" = Option<u64>, Query, description = "Replay events strictly after this sequence number")
    ),
    responses(
        (status = 200, description = "Invocation event stream", content_type = "text/event-stream", body = String),
        (status = 404, description = "Invocation not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
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
        InvocationCommandApi::Release => InvocationCommand::Release,
        InvocationCommandApi::ProjectValidate => InvocationCommand::ProjectValidate,
        InvocationCommandApi::EnvironmentPrepare => InvocationCommand::EnvironmentPrepare,
        InvocationCommandApi::EnvironmentValidate => InvocationCommand::EnvironmentValidate,
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
            | AppError::InvalidReleaseTarget(_)
            | AppError::RemoteProjectEnvironmentRequiresSha(_, _)
            | AppError::InvalidRemoteProjectCommitSha(_, _, _)
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
            AppError::ProjectDeleteBlocked(_) => StatusCode::CONFLICT,
            AppError::InvocationAlreadyClaimed(_) => StatusCode::CONFLICT,
            AppError::InvocationNotClaimable(_) => StatusCode::BAD_REQUEST,
            AppError::SchemaOutOfDate => StatusCode::PRECONDITION_FAILED,
            AppError::Io(ref err) if err.kind() == std::io::ErrorKind::NotFound => {
                StatusCode::NOT_FOUND
            }
            AppError::Io(ref err) if err.kind() == std::io::ErrorKind::InvalidInput => {
                StatusCode::BAD_REQUEST
            }
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let body = ApiErrorResponse {
            error: self.0.to_string(),
        };
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
