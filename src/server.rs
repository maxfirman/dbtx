//! HTTP API server: routes, handlers, and OpenAPI documentation.
use crate::api::{
    ApiErrorResponse, EnvironmentActiveResourcesApiRequest, EnvironmentActiveResourcesResponse,
    EnvironmentActualStateResponse, EnvironmentDraftResponse, EnvironmentDraftStartResponse,
    EnvironmentDraftUpdateApiRequest, EnvironmentReconcileApiRequest,
    EnvironmentReconcilePreparationResponse, EnvironmentReleaseApiRequest, EnvironmentResponse,
    EnvironmentRollbackApiRequest, EnvironmentRunPlanResponse, EnvironmentRunPlansResponse,
    EnvironmentVersionsResponse, EnvironmentsResponse, HealthResponse, InvocationCancelApiRequest,
    InvocationCancelStateApi, InvocationClaimNextApiRequest, InvocationClaimResponse,
    InvocationCleanupApiRequest, InvocationCleanupResponse, InvocationCommandApi,
    InvocationCompleteApiRequest, InvocationCreateApiRequest, InvocationCreateResponse,
    InvocationEvent, InvocationEventBatchApiRequest, InvocationExecutionModeApi,
    InvocationExecutionSpecApi, InvocationHeartbeatApiRequest, InvocationHeartbeatResponse,
    InvocationLifecycleStatus, InvocationListApiRequest, InvocationStatusResponse,
    InvocationWorkerHealthApi, InvocationsResponse, MigrateResponse, ProjectDeleteResponse,
    ProjectDraftCreateApiRequest, ProjectDraftResponse, ProjectDraftValidateResponse,
    ProjectResponse, ProjectResolveQuery, ProjectResolveResponse, ProjectUpdateApiRequest,
    ProjectsResponse, LocalEnvironmentUpsertApiRequest, LocalEnvironmentUpsertApiResponse,
    QueueStatusResponse, QueuesResponse, ReadyResponse, SourceStateEventCreateApiRequest,
    SourceStateEventResponse, WorkerStatusResponse, WorkersResponse,
};
use crate::config::RuntimeConfig;
use crate::db::{
    AppliedMigration, CreateInvocationInput, Db, EnvironmentRecord, EnvironmentVersionRecord,
    InvocationCancellationRecord, ProjectRecord, TimedOutInvocationRecord,
};
use crate::error::{AppError, AppResult};
use crate::execution::ExecutionMode;
use crate::execution::{
    ExecutionCompletion, ExecutionEvent, ExecutionEventKind, heartbeat_stale_timeout,
};
use crate::invocation_bootstrap::invocation_claim_deadline_at;
use crate::invocation_bootstrap::{
    ensure_target_manifest_for_reconcile, start_environment_draft_prepare_invocation,
    start_environment_draft_validation_invocation,
    start_project_draft_validation_invocation,
};
use crate::invocation_runtime::{
    InvocationManager, InvocationPersistence, InvocationRecorder, event_stream,
    started_invocation_event,
};
use crate::reconciler::auto_admit_blocked_plans_for_environment;
use crate::services::{
    EnvironmentReleaseRequest, EnvironmentRollbackRequest, EnvironmentService, InvocationCommand,
    InvocationService, PreparedExecutionSpec, ProjectCreateRequest,
    ProjectService, ProjectUpdateRequest, SourceStateEventCreateRequest,
};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, patch, post};
use axum::{Json, Router};
use chrono::Utc;
use futures_util::Stream;
use std::convert::Infallible;
use std::net::SocketAddr;
use tower_http::trace::TraceLayer;
use tracing::{error, info, info_span};
use utoipa::OpenApi;
use uuid::Uuid;

#[derive(Clone)]
pub struct AppState {
    db: Db,
    #[allow(dead_code)]
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
        let runtime = self
            .invocations
            .get_or_create(invocation_id, persistence)
            .await;
        let started_event = started_invocation_event();
        let sequence = self
            .db
            .append_invocation_event(invocation_id, &started_event)
            .await?;
        runtime.push_event(sequence, started_event).await;
        Ok(())
    }

    /// Complete an invocation and apply all post-terminal reactions:
    /// persist completion, auto-admit blocked plans, schedule cleanup.
    pub(crate) async fn complete_invocation(
        &self,
        invocation_id: Uuid,
        worker_id: &str,
        lease_token: Uuid,
        completion: ExecutionCompletion,
    ) -> AppResult<()> {
        let persistence = self
            .db
            .get_invocation_persistence(invocation_id, Some(worker_id), Some(lease_token))
            .await?;
        let runtime = self.invocations.get_or_create(invocation_id, None).await;
        let recorder = InvocationRecorder::new(self.db.clone(), invocation_id, runtime);
        recorder.complete(worker_id, lease_token, completion).await?;
        if let (Some(project_id), Some(environment_id)) =
            (persistence.project_id, persistence.environment_id)
        {
            let admitted =
                auto_admit_blocked_plans_for_environment(self, project_id, environment_id).await?;
            if admitted > 0 {
                info!(
                    invocation_id = %invocation_id,
                    project_id,
                    environment_id,
                    admitted,
                    "auto-admitted blocked reconciliation plans"
                );
            }
        }
        self.invocations.schedule_cleanup(invocation_id);
        Ok(())
    }
}

impl crate::services::InvocationStarter for AppState {
    async fn start_prepared_invocation(
        &self,
        invocation_id: Uuid,
        command: crate::api::InvocationCommandApi,
        plan_id: Option<Uuid>,
        prepared: crate::services::LocalExecutionPrepared,
    ) -> AppResult<Uuid> {
        crate::invocation_bootstrap::start_prepared_invocation(
            self,
            invocation_id,
            command,
            plan_id,
            prepared,
        )
        .await
    }
}

pub fn router(state: AppState) -> Router {
    let schema_checked_routes = Router::new()
        .merge(crate::ui::router())
        .merge(project_routes())
        .merge(environment_routes())
        .merge(invocation_routes())
        .merge(operator_routes())
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            require_current_schema_middleware,
        ));

    Router::new()
        .merge(schema_checked_routes)
        .merge(system_routes())
        .layer(
            TraceLayer::new_for_http().make_span_with(|request: &axum::http::Request<_>| {
                let request_id = request
                    .headers()
                    .get("x-request-id")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("-");
                info_span!(
                    "http_request",
                    method = %request.method(),
                    uri = %request.uri(),
                    request_id = %request_id,
                )
            }),
        )
        // The request-id middleware must run before the trace layer so spans
        // for requests without an incoming X-Request-Id still get the generated id.
        .layer(axum::middleware::from_fn(request_id_middleware))
        .with_state(state)
}

async fn request_id_middleware(
    mut request: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let request_id = request
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(ToString::to_string)
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    request.headers_mut().insert(
        "x-request-id",
        axum::http::HeaderValue::from_str(&request_id)
            .unwrap_or_else(|_| axum::http::HeaderValue::from_static("-")),
    );
    let mut response = next.run(request).await;
    if let Ok(value) = axum::http::HeaderValue::from_str(&request_id) {
        response.headers_mut().insert("x-request-id", value);
    }
    response
}

async fn require_current_schema_middleware(
    State(state): State<AppState>,
    request: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> axum::response::Response {
    if let Err(_err) = state.db.require_current_schema().await {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiErrorResponse {
                error: "schema out of date — run migrations".to_string(),
            }),
        )
            .into_response();
    }
    next.run(request).await
}

fn system_routes() -> Router<AppState> {
    Router::new()
        .route("/openapi.json", get(openapi_json))
        .route("/docs", get(swagger_docs))
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/v1/state/migrate", post(migrate))
}

fn project_routes() -> Router<AppState> {
    Router::new()
        .route("/v1/projects", get(projects_list))
        .route("/v1/projects/resolve", get(project_resolve))
        .route(
            "/v1/projects/{project_id}",
            patch(project_update)
                .get(project_get)
                .delete(project_delete),
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
}

fn environment_routes() -> Router<AppState> {
    Router::new()
        .route(
            "/v1/projects/{project_id}/environments/local",
            post(environment_local_upsert),
        )
        .route(
            "/v1/projects/{project_id}/environment-drafts",
            post(environment_draft_create),
        )
        .route(
            "/v1/environment-drafts/{draft_id}",
            get(environment_draft_get),
        )
        .route(
            "/v1/environment-drafts/{draft_id}/branch",
            post(environment_draft_branch_refresh),
        )
        .route(
            "/v1/environment-drafts/{draft_id}/validate",
            post(environment_draft_validate),
        )
        .route(
            "/v1/environment-drafts/{draft_id}/confirm",
            post(environment_draft_confirm),
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
            "/v1/projects/{project_id}/environments/{slug}/actual-state",
            get(environment_actual_state),
        )
        .route(
            "/v1/projects/{project_id}/environments/{slug}/reconcile-preparation",
            get(environment_reconcile_preparation),
        )
        .route(
            "/v1/projects/{project_id}/environments/{slug}/release",
            post(environment_release),
        )
        .route(
            "/v1/projects/{project_id}/environments/{slug}/pause",
            post(environment_pause),
        )
        .route(
            "/v1/projects/{project_id}/environments/{slug}/resume",
            post(environment_resume),
        )
        .route(
            "/v1/projects/{project_id}/environments/{slug}/history",
            get(environment_history),
        )
        .route(
            "/v1/projects/{project_id}/environments/{slug}/active-resources",
            get(environment_active_resources),
        )
        .route(
            "/v1/projects/{project_id}/environments/{slug}/source-state-events",
            post(environment_source_state_event_create),
        )
        .route(
            "/v1/projects/{project_id}/environments/{slug}/plans",
            get(environment_plan_list),
        )
        .route(
            "/v1/projects/{project_id}/environments/{slug}/reconcile",
            post(environment_reconcile),
        )
        .route(
            "/v1/projects/{project_id}/environments/{slug}/rollback",
            post(environment_rollback),
        )
        .route("/v1/plans/{plan_id}", get(environment_plan_get))
        .route("/v1/plans/{plan_id}/admit", post(environment_plan_admit))
}

fn invocation_routes() -> Router<AppState> {
    Router::new()
        .route(
            "/v1/invocations",
            get(invocation_list).post(invocation_create),
        )
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
}

fn operator_routes() -> Router<AppState> {
    Router::new()
        .route("/v1/workers", get(worker_list))
        .route("/v1/queues", get(queue_list))
        .route("/v1/reconcile/tick", post(reconcile_tick))
        .route("/v1/reconcile/sweep", post(reconcile_sweep))
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
        environment_draft_create,
        environment_draft_get,
        environment_draft_branch_refresh,
        environment_draft_validate,
        environment_draft_confirm,
        projects_list,
        project_get,
        project_update,
        project_delete,
        environment_list,
        environment_get,
        environment_actual_state,
        environment_release,
        environment_history,
        environment_active_resources,
        environment_source_state_event_create,
        environment_plan_list,
        environment_plan_get,
        environment_reconcile,
        environment_plan_admit,
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
            EnvironmentDraftResponse,
            EnvironmentDraftStartResponse,
            EnvironmentDraftUpdateApiRequest,
            EnvironmentResponse,
            EnvironmentActualStateResponse,
            EnvironmentsResponse,
            EnvironmentActiveResourcesResponse,
            EnvironmentRunPlanResponse,
            EnvironmentRunPlansResponse,
            EnvironmentActiveResourcesApiRequest,
            EnvironmentVersionsResponse,
            EnvironmentReleaseApiRequest,
            EnvironmentRollbackApiRequest,
            EnvironmentReconcileApiRequest,
            SourceStateEventCreateApiRequest,
            SourceStateEventResponse,
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
        AppError::InvalidInput(format!("invalid listen address '{listen}': {err}"))
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
    axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|err| {
            error!(error = %err, "dbtx server stopped with error");
            AppError::Io(err)
        })
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("received shutdown signal, draining connections");
}

async fn reconcile_timed_out_invocations(state: &AppState) -> AppResult<usize> {
    let timed_out = state
        .db
        .reconcile_timed_out_invocations(
            heartbeat_stale_timeout(crate::api::InvocationExecutionModeApi::Local),
            heartbeat_stale_timeout(crate::api::InvocationExecutionModeApi::Server),
        )
        .await?;
    for timed_out_invocation in &timed_out {
        if let Some((project_id, environment_id)) = state
            .db
            .force_complete_invocation(
                timed_out_invocation.invocation_id,
                &crate::execution::ExecutionCompletion {
                    status: timed_out_invocation.status,
                    exit_code: timed_out_invocation.exit_code,
                    error: Some(timed_out_invocation.error.clone()),
                    dbt_version: None,
                    result: None,
                    manifest: None,
                },
            )
            .await?
        {
            auto_admit_blocked_plans_for_environment(state, project_id, environment_id).await?;
        }
    }
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
    Ok(Json(ProjectDeleteResponse {
        deleted_project_id: project_id,
    }))
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
    let invocation_id = start_project_draft_validation_invocation(&state, prepared).await?;
    Ok(Json(ProjectDraftValidateResponse {
        draft: ProjectService::new(&state.db).get_draft(draft_id).await?,
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

async fn environment_local_upsert(
    State(state): State<AppState>,
    Path(project_id): Path<String>,
    Json(request): Json<LocalEnvironmentUpsertApiRequest>,
) -> Result<Json<LocalEnvironmentUpsertApiResponse>, ApiError> {
    let project = state.db.get_project_by_project_id(&project_id).await?;
    let slug = format!("local-{}-{}", request.machine_id, request.target_name);
    let worker_queue = format!("local-{}", request.machine_id);
    let environment = state
        .db
        .upsert_local_environment_lightweight(
            project.id,
            &slug,
            &request.target_name,
            &request.adapter_type,
            &worker_queue,
            &request.schema_name,
        )
        .await?;
    info!(
        project_id = %project_id,
        environment_slug = %environment.slug,
        machine_id = %request.machine_id,
        "upserted local environment"
    );
    Ok(Json(LocalEnvironmentUpsertApiResponse {
        environment_slug: environment.slug,
        worker_queue,
    }))
}

#[utoipa::path(
    post,
    path = "/v1/projects/{project_id}/environment-drafts",
    tag = "environments",
    params(
        ("project_id" = String, Path, description = "Project identifier")
    ),
    responses(
        (status = 200, description = "Started environment draft git metadata load", body = EnvironmentDraftStartResponse),
        (status = 404, description = "Project not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
async fn environment_draft_create(
    State(state): State<AppState>,
    Path(project_id): Path<String>,
) -> Result<Json<EnvironmentDraftStartResponse>, ApiError> {
    let service = EnvironmentService::new(&state.db);
    let draft = service.create_draft(project_id).await?;
    let prepared = service.prepare_draft_git_metadata(draft.id).await?;
    let invocation_id = start_environment_draft_prepare_invocation(&state, prepared).await?;
    let draft = service.get_draft(draft.id).await?;
    Ok(Json(EnvironmentDraftStartResponse {
        draft,
        invocation_id,
    }))
}

#[utoipa::path(
    get,
    path = "/v1/environment-drafts/{draft_id}",
    tag = "environments",
    params(
        ("draft_id" = Uuid, Path, description = "Draft identifier")
    ),
    responses(
        (status = 200, description = "Environment draft", body = EnvironmentDraftResponse),
        (status = 404, description = "Draft not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
async fn environment_draft_get(
    State(state): State<AppState>,
    Path(draft_id): Path<Uuid>,
) -> Result<Json<EnvironmentDraftResponse>, ApiError> {
    let draft = EnvironmentService::new(&state.db)
        .get_draft(draft_id)
        .await?;
    Ok(Json(EnvironmentDraftResponse { draft }))
}

#[utoipa::path(
    post,
    path = "/v1/environment-drafts/{draft_id}/branch",
    tag = "environments",
    params(
        ("draft_id" = Uuid, Path, description = "Draft identifier")
    ),
    request_body = EnvironmentDraftUpdateApiRequest,
    responses(
        (status = 200, description = "Started branch metadata refresh", body = EnvironmentDraftStartResponse),
        (status = 404, description = "Draft not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
async fn environment_draft_branch_refresh(
    State(state): State<AppState>,
    Path(draft_id): Path<Uuid>,
    Json(request): Json<EnvironmentDraftUpdateApiRequest>,
) -> Result<Json<EnvironmentDraftStartResponse>, ApiError> {
    let service = EnvironmentService::new(&state.db);
    let prepared = service
        .refresh_draft_branch(draft_id, environment_draft_update_request(request))
        .await?;
    let invocation_id = start_environment_draft_prepare_invocation(&state, prepared).await?;
    let draft = service.get_draft(draft_id).await?;
    Ok(Json(EnvironmentDraftStartResponse {
        draft,
        invocation_id,
    }))
}

#[utoipa::path(
    post,
    path = "/v1/environment-drafts/{draft_id}/validate",
    tag = "environments",
    params(
        ("draft_id" = Uuid, Path, description = "Draft identifier")
    ),
    request_body = EnvironmentDraftUpdateApiRequest,
    responses(
        (status = 200, description = "Started environment draft validation", body = EnvironmentDraftStartResponse),
        (status = 404, description = "Draft not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
async fn environment_draft_validate(
    State(state): State<AppState>,
    Path(draft_id): Path<Uuid>,
    Json(request): Json<EnvironmentDraftUpdateApiRequest>,
) -> Result<Json<EnvironmentDraftStartResponse>, ApiError> {
    let service = EnvironmentService::new(&state.db);
    let prepared = service
        .prepare_draft_validation(draft_id, environment_draft_update_request(request))
        .await?;
    let invocation_id = start_environment_draft_validation_invocation(&state, prepared).await?;
    let draft = service.get_draft(draft_id).await?;
    Ok(Json(EnvironmentDraftStartResponse {
        draft,
        invocation_id,
    }))
}

#[utoipa::path(
    post,
    path = "/v1/environment-drafts/{draft_id}/confirm",
    tag = "environments",
    params(
        ("draft_id" = Uuid, Path, description = "Draft identifier")
    ),
    responses(
        (status = 200, description = "Confirmed environment", body = EnvironmentResponse),
        (status = 404, description = "Draft not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
async fn environment_draft_confirm(
    State(state): State<AppState>,
    Path(draft_id): Path<Uuid>,
) -> Result<Json<EnvironmentResponse>, ApiError> {
    let environment = EnvironmentService::new(&state.db)
        .confirm_draft(draft_id)
        .await?;
    Ok(Json(EnvironmentResponse { environment }))
}

fn environment_draft_update_request(
    request: EnvironmentDraftUpdateApiRequest,
) -> crate::services::EnvironmentDraftUpdateRequest {
    crate::services::EnvironmentDraftUpdateRequest {
        project: String::new(),
        slug: request.slug,
        git_branch: request.git_branch,
        git_commit_sha: request.git_commit_sha,
        use_latest_commit: request.use_latest_commit,
        auto_reconcile: request.auto_reconcile,
        immutable: request.immutable,
        adapter_type: request.adapter_type,
        schema_name: request.schema_name,
        threads: request.threads,
        profile_config: request.profile_config,
        profile_secrets: request.profile_secrets,
    }
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

async fn project_resolve(
    State(state): State<AppState>,
    Query(query): Query<ProjectResolveQuery>,
) -> Result<Json<ProjectResolveResponse>, ApiError> {
    let project = state
        .db
        .get_project_by_repo(&query.git_repo_url, &query.project_root)
        .await?;
    Ok(Json(ProjectResolveResponse {
        project: ProjectResponse { project },
    }))
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
    get,
    path = "/v1/projects/{project_id}/environments/{slug}/actual-state",
    tag = "environments",
    params(
        ("project_id" = String, Path, description = "Project identifier"),
        ("slug" = String, Path, description = "Environment slug")
    ),
    responses(
        (status = 200, description = "Environment actual state", body = EnvironmentActualStateResponse),
        (status = 404, description = "Environment not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
async fn environment_actual_state(
    State(state): State<AppState>,
    Path((project_id, slug)): Path<(String, String)>,
) -> Result<Json<EnvironmentActualStateResponse>, ApiError> {
    let service = EnvironmentService::new(&state.db);
    let actual_state = service.actual_state(project_id, slug).await?;
    Ok(Json(EnvironmentActualStateResponse { actual_state }))
}

#[utoipa::path(
    get,
    path = "/v1/projects/{project_id}/environments/{slug}/reconcile-preparation",
    tag = "environments",
    params(
        ("project_id" = String, Path, description = "Project identifier"),
        ("slug" = String, Path, description = "Environment slug")
    ),
    responses(
        (status = 200, description = "Environment reconciliation preparation state", body = EnvironmentReconcilePreparationResponse),
        (status = 404, description = "Environment not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
async fn environment_reconcile_preparation(
    State(state): State<AppState>,
    Path((project_id, slug)): Path<(String, String)>,
) -> Result<Json<EnvironmentReconcilePreparationResponse>, ApiError> {
    let preparation = state
        .db()
        .get_environment_reconcile_preparation(&project_id, &slug)
        .await?;
    Ok(Json(EnvironmentReconcilePreparationResponse {
        preparation,
    }))
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

async fn environment_pause(
    State(state): State<AppState>,
    Path((project_id, slug)): Path<(String, String)>,
) -> Result<Json<EnvironmentResponse>, ApiError> {
    let environment = state
        .db
        .set_environment_auto_reconcile(&project_id, &slug, false)
        .await?;
    info!(
        project_id = %environment.project_ref,
        environment = %environment.slug,
        "paused automatic reconciliation"
    );
    Ok(Json(EnvironmentResponse { environment }))
}

async fn environment_resume(
    State(state): State<AppState>,
    Path((project_id, slug)): Path<(String, String)>,
) -> Result<Json<EnvironmentResponse>, ApiError> {
    let environment = state
        .db
        .set_environment_auto_reconcile(&project_id, &slug, true)
        .await?;
    info!(
        project_id = %environment.project_ref,
        environment = %environment.slug,
        "resumed automatic reconciliation"
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
    get,
    path = "/v1/projects/{project_id}/environments/{slug}/active-resources",
    tag = "environments",
    params(
        ("project_id" = String, Path, description = "Project identifier"),
        ("slug" = String, Path, description = "Environment slug"),
        ("resource_type" = Option<String>, Query, description = "Optional dbt resource type filter, e.g. model")
    ),
    responses(
        (status = 200, description = "Active selected resources for the environment", body = EnvironmentActiveResourcesResponse),
        (status = 404, description = "Environment not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
async fn environment_active_resources(
    State(state): State<AppState>,
    Path((project_id, slug)): Path<(String, String)>,
    Query(request): Query<EnvironmentActiveResourcesApiRequest>,
) -> Result<Json<EnvironmentActiveResourcesResponse>, ApiError> {
    let resources = state
        .db
        .list_active_environment_resources(&project_id, &slug, request.resource_type.as_deref())
        .await?;
    Ok(Json(EnvironmentActiveResourcesResponse { resources }))
}

#[utoipa::path(
    post,
    path = "/v1/projects/{project_id}/environments/{slug}/source-state-events",
    tag = "environments",
    params(
        ("project_id" = String, Path, description = "Project identifier"),
        ("slug" = String, Path, description = "Environment slug")
    ),
    request_body = SourceStateEventCreateApiRequest,
    responses(
        (status = 200, description = "Created source state event", body = SourceStateEventResponse),
        (status = 404, description = "Environment not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
async fn environment_source_state_event_create(
    State(state): State<AppState>,
    Path((project_id, slug)): Path<(String, String)>,
    Json(request): Json<SourceStateEventCreateApiRequest>,
) -> Result<Json<SourceStateEventResponse>, ApiError> {
    let service = EnvironmentService::new(&state.db);
    let event = service
        .create_source_state_event(SourceStateEventCreateRequest {
            project: project_id,
            slug,
            source_key: request.source_key,
            provider: request.provider,
            state_version: request.state_version,
            observed_at: request.observed_at,
            payload: request.payload,
        })
        .await?;
    Ok(Json(SourceStateEventResponse { event }))
}

#[utoipa::path(
    get,
    path = "/v1/projects/{project_id}/environments/{slug}/plans",
    tag = "environments",
    params(
        ("project_id" = String, Path, description = "Project identifier"),
        ("slug" = String, Path, description = "Environment slug")
    ),
    responses(
        (status = 200, description = "Environment run plans", body = EnvironmentRunPlansResponse),
        (status = 404, description = "Environment not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
async fn environment_plan_list(
    State(state): State<AppState>,
    Path((project_id, slug)): Path<(String, String)>,
) -> Result<Json<EnvironmentRunPlansResponse>, ApiError> {
    let service = EnvironmentService::new(&state.db);
    let plans = service.list_plans(project_id, slug).await?;
    Ok(Json(EnvironmentRunPlansResponse { plans }))
}

#[utoipa::path(
    get,
    path = "/v1/plans/{plan_id}",
    tag = "environments",
    params(
        ("plan_id" = Uuid, Path, description = "Plan identifier")
    ),
    responses(
        (status = 200, description = "Environment run plan", body = EnvironmentRunPlanResponse),
        (status = 404, description = "Plan not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
async fn environment_plan_get(
    State(state): State<AppState>,
    Path(plan_id): Path<Uuid>,
) -> Result<Json<EnvironmentRunPlanResponse>, ApiError> {
    let service = EnvironmentService::new(&state.db);
    let plan = service.get_plan(plan_id).await?;
    Ok(Json(EnvironmentRunPlanResponse { plan }))
}

#[utoipa::path(
    post,
    path = "/v1/projects/{project_id}/environments/{slug}/reconcile",
    tag = "environments",
    params(
        ("project_id" = String, Path, description = "Project identifier"),
        ("slug" = String, Path, description = "Environment slug")
    ),
    request_body = EnvironmentReconcileApiRequest,
    responses(
        (status = 200, description = "Created reconciliation plan", body = EnvironmentRunPlanResponse),
        (status = 400, description = "No reconciliation work available", body = ApiErrorResponse),
        (status = 404, description = "Environment not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
async fn environment_reconcile(
    State(state): State<AppState>,
    Path((project_id, slug)): Path<(String, String)>,
    Json(_request): Json<EnvironmentReconcileApiRequest>,
) -> Result<Json<EnvironmentRunPlanResponse>, ApiError> {
    ensure_target_manifest_for_reconcile(&state, &project_id, &slug).await?;
    let service = EnvironmentService::new(&state.db);
    let plan = service.reconcile(project_id, slug).await?;
    Ok(Json(EnvironmentRunPlanResponse { plan }))
}

#[utoipa::path(
    post,
    path = "/v1/plans/{plan_id}/admit",
    tag = "environments",
    params(
        ("plan_id" = Uuid, Path, description = "Plan identifier")
    ),
    responses(
        (status = 200, description = "Admitted or blocked plan", body = EnvironmentRunPlanResponse),
        (status = 404, description = "Plan not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
async fn environment_plan_admit(
    State(state): State<AppState>,
    Path(plan_id): Path<Uuid>,
) -> Result<Json<EnvironmentRunPlanResponse>, ApiError> {
    let service = EnvironmentService::new(&state.db);
    let admission = service.admit_and_start_plan(&state, plan_id).await?;
    Ok(Json(EnvironmentRunPlanResponse {
        plan: admission.plan,
    }))
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

struct PreparedInvocation {
    execution_spec: InvocationExecutionSpecApi,
    persistence: Option<InvocationPersistence>,
    worker_queue: String,
    project_id: Option<i64>,
    environment_id: Option<i64>,
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
        project_id = request.project_id.as_deref().unwrap_or(""),
        environment_slug = request.environment_slug.as_deref().unwrap_or(""),
        "starting invocation"
    );

    let db = state.db.clone();
    let service = InvocationService::new(&db);
    let project_id = request
        .project_id
        .as_deref()
        .ok_or(AppError::RemoteExecutionRequiresProjectId)?;
    let environment_slug = request
        .environment_slug
        .as_deref()
        .ok_or(AppError::RemoteExecutionRequiresEnvironmentSlug)?;
    let environment = db.get_environment(project_id, environment_slug).await?;
    let project = db
        .get_project_by_project_id(&environment.project_ref)
        .await?;
    let derived_execution_mode = if environment.git_commit_sha.is_some() {
        InvocationExecutionModeApi::Server
    } else {
        InvocationExecutionModeApi::Local
    };
    let prepared = match derived_execution_mode {
        crate::api::InvocationExecutionModeApi::Local => {
            if matches!(request.command, InvocationCommandApi::Release) {
                return Err(ApiError(AppError::UnsupportedLocalExecution(
                    "release".to_string(),
                )));
            }
            let command = map_invocation_command(request.command);
            let inject_json_logging = command.persists_state();
            let ctx = crate::config::InvocationContext::from_args(
                &request
                    .args
                    .iter()
                    .cloned()
                    .map(Into::into)
                    .collect::<Vec<std::ffi::OsString>>(),
                inject_json_logging,
            )?;
            let run_id = invocation_id;
            let reconstructed_manifest = db
                .load_reconstructed_manifest(project.id, environment.id)
                .await?
                .or(if ctx.wants_state_modified {
                    Some(
                        crate::manifest::ReconstructedManifest::write_empty_state(
                            &project.project_name,
                            &environment.adapter_type,
                        )
                        .await?,
                    )
                } else {
                    None
                });
            let state_manifest =
                if let Some(reconstructed_manifest) = reconstructed_manifest.as_ref() {
                    let path = reconstructed_manifest.temp_dir.path().join("manifest.json");
                    let content = tokio::fs::read_to_string(path)
                        .await
                        .map_err(|e| AppError::Internal(e.to_string()))?;
                    Some(
                        serde_json::from_str(&content)
                            .map_err(|e| AppError::Internal(e.to_string()))?,
                    )
                } else {
                    None
                };
            let mut dbt_args: Vec<std::ffi::OsString> =
                request.args.iter().cloned().map(Into::into).collect();
            if command.persists_state() {
                dbt_args = crate::dbt_utils::append_invocation_id(dbt_args, run_id);
            }
            let persistence = if command.persists_state() {
                let args_json = serde_json::Value::Array(
                    dbt_args
                        .iter()
                        .map(|v| serde_json::Value::String(v.to_string_lossy().into_owned()))
                        .collect(),
                );
                let git_state = crate::dbt_utils::read_git_state(std::path::Path::new("."));
                db.insert_run_started(crate::db::RunStart {
                    run_id,
                    project: &project,
                    environment: &environment,
                    subcommand: command.as_str(),
                    args_json,
                    is_full_graph_run: ctx.is_full_graph_run,
                    execution_mode: ExecutionMode::Local,
                    git_state: &git_state,
                })
                .await?;
                Some(InvocationPersistence {
                    run_id,
                    project_id: project.id,
                    environment_id: environment.id,
                    promote_base_manifest: ctx.is_full_graph_run,
                    updates_actual_state: true,
                })
            } else {
                None
            };
            let execution_spec = InvocationExecutionSpecApi::Local {
                command: request.command,
                args: dbt_args
                    .into_iter()
                    .map(|v| v.to_string_lossy().into_owned())
                    .collect(),
                state_manifest,
            };
            PreparedInvocation {
                execution_spec,
                persistence,
                worker_queue: environment.worker_queue.clone(),
                project_id: Some(project.id),
                environment_id: Some(environment.id),
            }
        }
        crate::api::InvocationExecutionModeApi::Server => {
            let prepared = match request.command {
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
            };
            let execution_spec = match prepared.spec {
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
                _ => {
                    return Err(ApiError(AppError::Internal(
                        "unexpected execution spec for server mode".to_string(),
                    )));
                }
            };
            let persistence = prepared.persistence.map(|p| InvocationPersistence {
                run_id: p.run_id,
                project_id: p.project_id,
                environment_id: p.environment_id,
                promote_base_manifest: p.promote_base_manifest,
                updates_actual_state: p.updates_actual_state,
            });
            PreparedInvocation {
                execution_spec,
                persistence,
                worker_queue: prepared.worker_queue,
                project_id: prepared.project_id,
                environment_id: prepared.environment_id,
            }
        }
    };
    state
        .db
        .create_invocation(CreateInvocationInput {
            invocation_id,
            plan_id: None,
            run_id: prepared.persistence.as_ref().map(|p| p.run_id),
            project_id: prepared.project_id,
            environment_id: prepared.environment_id,
            project_draft_id: None,
            environment_draft_id: None,
            command: map_invocation_command(request.command).as_str().to_string(),
            execution_mode: derived_execution_mode,
            worker_queue: prepared.worker_queue.clone(),
            execution_spec: Some(prepared.execution_spec.clone()),
            promote_base_manifest: prepared
                .persistence
                .as_ref()
                .map(|p| p.promote_base_manifest)
                .unwrap_or(false),
            updates_actual_state: prepared
                .persistence
                .as_ref()
                .map(|p| p.updates_actual_state)
                .unwrap_or(false),
            claim_deadline_at: Some(invocation_claim_deadline_at(derived_execution_mode)),
        })
        .await?;
    state
        .bootstrap_invocation_started(invocation_id, prepared.persistence)
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
    let worker_queues = normalize_worker_queues(&request.worker_queues)?;
    let Some(claimed) = state
        .db
        .claim_next_invocation(&request.worker_id, request.execution_mode, &worker_queues)
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

fn normalize_worker_queues(worker_queues: &[String]) -> Result<Vec<String>, ApiError> {
    let mut normalized = worker_queues
        .iter()
        .map(|queue| queue.trim())
        .filter(|queue| !queue.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    normalized.sort();
    normalized.dedup();
    if normalized.is_empty() {
        return Err(ApiError(AppError::InvalidInput(
            "worker_queues must not be empty".to_string(),
        )));
    }
    Ok(normalized)
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
        if let Some((project_id, environment_id)) = state
            .db
            .force_complete_invocation(
                invocation_id,
                &crate::execution::ExecutionCompletion {
                    status,
                    exit_code,
                    error: Some(error.clone()),
                    dbt_version: None,
                    result: None,
                    manifest: None,
                },
            )
            .await?
        {
            auto_admit_blocked_plans_for_environment(&state, project_id, environment_id).await?;
        }
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

async fn reconcile_tick(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let planned = crate::reconciler::reconcile_environments_once(&state).await?;
    Ok(Json(serde_json::json!({ "planned": planned })))
}

async fn reconcile_sweep(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let admitted = crate::reconciler::sweep_blocked_plans_once(&state).await?;
    Ok(Json(serde_json::json!({ "admitted": admitted })))
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
        return Err(ApiError(AppError::InvalidInput(
            "older_than_seconds must be greater than 0".to_string(),
        )));
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
    let recorder = InvocationRecorder::new(state.db.clone(), id, runtime);
    if !recorder.is_running().await {
        return Err(ApiError(AppError::Internal(
            "invocation is already completed".to_string(),
        )));
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
    state
        .complete_invocation(id, &request.worker_id, request.lease_token, request.completion)
        .await?;
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
    let rx = runtime.subscribe();
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
            .map(
                |(sequence, event)| crate::invocation_runtime::SequencedInvocationEvent {
                    sequence,
                    event,
                },
            )
            .collect(),
        last_sequence,
        rx,
    );
    info!(invocation_id = %id, buffered_events, "subscribed to invocation event stream");
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

fn map_invocation_command(command: InvocationCommandApi) -> InvocationCommand {
    command.into()
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
            | AppError::InvalidInput(_)
            | AppError::UnsupportedLocalExecution(_) => StatusCode::BAD_REQUEST,
            AppError::ProjectIdNotFound(_)
            | AppError::ProjectNotFoundByRepo(_, _)
            | AppError::EnvironmentNotFound(_, _)
            | AppError::PlanNotFound(_)
            | AppError::InvocationNotFound(_)
            | AppError::ProjectDraftNotFound(_)
            | AppError::EnvironmentDraftNotFound(_) => StatusCode::NOT_FOUND,
            AppError::EnvironmentAlreadyExists(_, _) | AppError::ProjectIdAlreadyConfigured(_) => {
                StatusCode::CONFLICT
            }
            AppError::ProjectDeleteBlocked(_) => StatusCode::CONFLICT,
            AppError::ImmutableEnvironment(_) => StatusCode::CONFLICT,
            AppError::InvocationAlreadyClaimed(_) => StatusCode::CONFLICT,
            AppError::InvocationOwnershipMismatch => StatusCode::CONFLICT,
            AppError::InvocationNotClaimable(_) => StatusCode::BAD_REQUEST,
            AppError::EnvironmentAlreadyReconciled
            | AppError::ReconciliationInProgress
            | AppError::PlanNotAdmissible(_, _) => StatusCode::CONFLICT,
            AppError::ReconciliationRequiresBaseline
            | AppError::ReconciliationRequiresCommitSha
            | AppError::ReconciliationEmptyPlan => StatusCode::UNPROCESSABLE_ENTITY,
            AppError::SchemaOutOfDate => StatusCode::PRECONDITION_FAILED,
            AppError::InvalidDatabaseValue(_, _) => StatusCode::INTERNAL_SERVER_ERROR,
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
    use super::ApiDoc;
    use utoipa::OpenApi;

    #[test]
    fn openapi_includes_environment_draft_endpoints() {
        let json = serde_json::to_value(ApiDoc::openapi()).expect("openapi json");
        let paths = json
            .get("paths")
            .and_then(|value| value.as_object())
            .expect("paths object");

        assert!(paths.contains_key("/v1/projects/{project_id}/environment-drafts"));
        assert!(paths.contains_key("/v1/environment-drafts/{draft_id}"));
        assert!(paths.contains_key("/v1/environment-drafts/{draft_id}/branch"));
        assert!(paths.contains_key("/v1/environment-drafts/{draft_id}/validate"));
        assert!(paths.contains_key("/v1/environment-drafts/{draft_id}/confirm"));
    }

    #[test]
    fn normalize_worker_queues_trims_and_deduplicates() {
        let queues = super::normalize_worker_queues(&[
            " generic ".to_string(),
            "validation".to_string(),
            "generic".to_string(),
        ]);
        assert!(queues.is_ok());
        let queues = queues.unwrap_or_default();
        assert_eq!(
            queues,
            vec!["generic".to_string(), "validation".to_string()]
        );
    }

    #[test]
    fn normalize_worker_queues_rejects_empty_input() {
        assert!(super::normalize_worker_queues(&[]).is_err());
        assert!(super::normalize_worker_queues(&["   ".to_string()]).is_err());
    }

    #[test]
    fn error_response_maps_not_found_errors() {
        use super::ApiError;
        use crate::error::AppError;
        use axum::http::StatusCode;
        use axum::response::IntoResponse;

        let resp = ApiError(AppError::ProjectIdNotFound("prj_1".to_string())).into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let resp = ApiError(AppError::EnvironmentNotFound(
            "prj_1".to_string(),
            "dev".to_string(),
        ))
        .into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let resp = ApiError(AppError::PlanNotFound(
            "550e8400-e29b-41d4-a716-446655440000".to_string(),
        ))
        .into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn error_response_maps_conflict_errors() {
        use super::ApiError;
        use crate::error::AppError;
        use axum::http::StatusCode;
        use axum::response::IntoResponse;

        let resp = ApiError(AppError::EnvironmentAlreadyReconciled).into_response();
        assert_eq!(resp.status(), StatusCode::CONFLICT);

        let resp = ApiError(AppError::ReconciliationInProgress).into_response();
        assert_eq!(resp.status(), StatusCode::CONFLICT);

        let resp = ApiError(AppError::PlanNotAdmissible(
            "id".to_string(),
            "completed".to_string(),
        ))
        .into_response();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }

    #[test]
    fn error_response_maps_unprocessable_entity() {
        use super::ApiError;
        use crate::error::AppError;
        use axum::http::StatusCode;
        use axum::response::IntoResponse;

        let resp = ApiError(AppError::ReconciliationRequiresBaseline).into_response();
        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);

        let resp = ApiError(AppError::ReconciliationEmptyPlan).into_response();
        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[test]
    fn error_response_maps_bad_request_errors() {
        use super::ApiError;
        use crate::error::AppError;
        use axum::http::StatusCode;
        use axum::response::IntoResponse;

        let resp = ApiError(AppError::UserStateNotAllowed).into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        let resp = ApiError(AppError::InvalidProjectMode("bad".to_string())).into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn error_response_maps_internal_to_500() {
        use super::ApiError;
        use crate::error::AppError;
        use axum::http::StatusCode;
        use axum::response::IntoResponse;

        let resp = ApiError(AppError::Internal("something broke".to_string())).into_response();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn schema_out_of_date_maps_to_precondition_failed() {
        use super::ApiError;
        use crate::error::AppError;
        use axum::http::StatusCode;
        use axum::response::IntoResponse;

        let resp = ApiError(AppError::SchemaOutOfDate).into_response();
        assert_eq!(resp.status(), StatusCode::PRECONDITION_FAILED);
    }
}
