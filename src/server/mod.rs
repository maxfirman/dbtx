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
    InvocationWorkerHealthApi, InvocationsResponse, LocalEnvironmentUpsertApiRequest,
    LocalEnvironmentUpsertApiResponse, MigrateResponse, ProjectDeleteResponse,
    ProjectDraftCreateApiRequest, ProjectDraftResponse, ProjectDraftValidateResponse,
    ProjectResolveQuery, ProjectResolveResponse, ProjectResponse, ProjectUpdateApiRequest,
    ProjectsResponse, QueueStatusResponse, QueuesResponse, ReadyResponse,
    SourceStateEventCreateApiRequest, SourceStateEventResponse, WorkerStatusResponse,
    WorkersResponse,
};
use crate::db::{
    AppliedMigration, CreateInvocationInput, EnvironmentRecord, EnvironmentVersionRecord,
    InvocationCancellationRecord, ProjectRecord, TimedOutInvocationRecord,
};
use crate::error::{AppError, AppResult};
use crate::execution::{
    ExecutionCompletion, ExecutionEvent, ExecutionEventKind, heartbeat_stale_timeout,
};
use crate::invocation_bootstrap::invocation_claim_deadline_at;
use crate::invocation_bootstrap::{
    ensure_target_manifest_for_reconcile, start_environment_draft_prepare_invocation,
    start_environment_draft_validation_invocation, start_project_draft_validation_invocation,
};
use crate::invocation_runtime::{InvocationPersistence, InvocationRecorder, event_stream};
use crate::reconciler::auto_admit_blocked_plans_for_environment;
use crate::services::{
    EnvironmentReleaseRequest, EnvironmentRollbackRequest, EnvironmentService, InvocationCommand,
    InvocationService, PreparedExecutionSpec, ProjectCreateRequest, ProjectService,
    ProjectUpdateRequest, SourceStateEventCreateRequest,
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

mod environments;
mod invocations;
mod operators;
mod projects;
mod system;

/// AppState is a type alias for ProcessState.
pub type AppState = crate::process_state::ProcessState;

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
        .route("/healthz", get(system::healthz))
        .route("/readyz", get(system::readyz))
        .route("/v1/state/migrate", post(system::migrate))
}

fn project_routes() -> Router<AppState> {
    Router::new()
        .route("/v1/projects", get(projects::projects_list))
        .route("/v1/projects/resolve", get(projects::project_resolve))
        .route(
            "/v1/projects/{project_id}",
            patch(projects::project_update)
                .get(projects::project_get)
                .delete(projects::project_delete),
        )
        .route("/v1/project-drafts", post(projects::project_draft_create))
        .route(
            "/v1/project-drafts/{draft_id}",
            get(projects::project_draft_get),
        )
        .route(
            "/v1/project-drafts/{draft_id}/validate",
            post(projects::project_draft_validate),
        )
        .route(
            "/v1/project-drafts/{draft_id}/confirm",
            post(projects::project_draft_confirm),
        )
}

fn environment_routes() -> Router<AppState> {
    Router::new()
        .route(
            "/v1/projects/{project_id}/environments/local",
            post(environments::environment_local_upsert),
        )
        .route(
            "/v1/projects/{project_id}/environment-drafts",
            post(environments::environment_draft_create),
        )
        .route(
            "/v1/environment-drafts/{draft_id}",
            get(environments::environment_draft_get),
        )
        .route(
            "/v1/environment-drafts/{draft_id}/branch",
            post(environments::environment_draft_branch_refresh),
        )
        .route(
            "/v1/environment-drafts/{draft_id}/validate",
            post(environments::environment_draft_validate),
        )
        .route(
            "/v1/environment-drafts/{draft_id}/confirm",
            post(environments::environment_draft_confirm),
        )
        .route(
            "/v1/projects/{project_id}/environments",
            get(environments::environment_list),
        )
        .route(
            "/v1/projects/{project_id}/environments/{slug}",
            get(environments::environment_get),
        )
        .route(
            "/v1/projects/{project_id}/environments/{slug}/actual-state",
            get(environments::environment_actual_state),
        )
        .route(
            "/v1/projects/{project_id}/environments/{slug}/reconcile-preparation",
            get(environments::environment_reconcile_preparation),
        )
        .route(
            "/v1/projects/{project_id}/environments/{slug}/release",
            post(environments::environment_release),
        )
        .route(
            "/v1/projects/{project_id}/environments/{slug}/pause",
            post(environments::environment_pause),
        )
        .route(
            "/v1/projects/{project_id}/environments/{slug}/resume",
            post(environments::environment_resume),
        )
        .route(
            "/v1/projects/{project_id}/environments/{slug}/history",
            get(environments::environment_history),
        )
        .route(
            "/v1/projects/{project_id}/environments/{slug}/active-resources",
            get(environments::environment_active_resources),
        )
        .route(
            "/v1/projects/{project_id}/environments/{slug}/source-state-events",
            post(environments::environment_source_state_event_create),
        )
        .route(
            "/v1/projects/{project_id}/environments/{slug}/plans",
            get(environments::environment_plan_list),
        )
        .route(
            "/v1/projects/{project_id}/environments/{slug}/reconcile",
            post(environments::environment_reconcile),
        )
        .route(
            "/v1/projects/{project_id}/environments/{slug}/rollback",
            post(environments::environment_rollback),
        )
        .route(
            "/v1/plans/{plan_id}",
            get(environments::environment_plan_get),
        )
        .route(
            "/v1/plans/{plan_id}/admit",
            post(environments::environment_plan_admit),
        )
}

fn invocation_routes() -> Router<AppState> {
    Router::new()
        .route(
            "/v1/invocations",
            get(invocations::invocation_list).post(invocations::invocation_create),
        )
        .route(
            "/v1/invocations/cleanup",
            post(invocations::invocation_cleanup),
        )
        .route(
            "/v1/invocations/claim-next",
            post(invocations::invocation_claim_next),
        )
        .route("/v1/invocations/{id}", get(invocations::invocation_status))
        .route(
            "/v1/invocations/{id}/heartbeat",
            post(invocations::invocation_heartbeat),
        )
        .route(
            "/v1/invocations/{id}/cancel",
            post(invocations::invocation_cancel),
        )
        .route(
            "/v1/invocations/{id}/complete",
            post(invocations::invocation_complete),
        )
        .route(
            "/v1/invocations/{id}/events",
            post(invocations::invocation_append_events),
        )
        .route(
            "/v1/invocations/{id}/events",
            get(invocations::invocation_events),
        )
}

fn operator_routes() -> Router<AppState> {
    Router::new()
        .route("/v1/workers", get(operators::worker_list))
        .route("/v1/queues", get(operators::queue_list))
        .route("/v1/reconcile/tick", post(operators::reconcile_tick))
        .route("/v1/reconcile/sweep", post(operators::reconcile_sweep))
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
        system::migrate,
        projects::project_draft_create,
        projects::project_draft_get,
        projects::project_draft_validate,
        projects::project_draft_confirm,
        environments::environment_draft_create,
        environments::environment_draft_get,
        environments::environment_draft_branch_refresh,
        environments::environment_draft_validate,
        environments::environment_draft_confirm,
        projects::projects_list,
        projects::project_get,
        projects::project_update,
        projects::project_delete,
        environments::environment_list,
        environments::environment_get,
        environments::environment_actual_state,
        environments::environment_release,
        environments::environment_history,
        environments::environment_active_resources,
        environments::environment_source_state_event_create,
        environments::environment_plan_list,
        environments::environment_plan_get,
        environments::environment_reconcile,
        environments::environment_plan_admit,
        environments::environment_rollback,
        invocations::invocation_create,
        invocations::invocation_list,
        operators::worker_list,
        operators::queue_list,
        invocations::invocation_cleanup,
        invocations::invocation_claim_next,
        invocations::invocation_status,
        invocations::invocation_heartbeat,
        invocations::invocation_cancel,
        invocations::invocation_append_events,
        invocations::invocation_complete,
        invocations::invocation_events
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

struct PreparedInvocation {
    execution_spec: InvocationExecutionSpecApi,
    persistence: Option<InvocationPersistence>,
    worker_queue: String,
    project_id: Option<i64>,
    environment_id: Option<i64>,
}

fn map_invocation_command(command: InvocationCommandApi) -> InvocationCommand {
    command.into()
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
            | AppError::RemoteExecutionRequiresGitRepoUrl(_)
            | AppError::RemoteExecutionRequiresProjectRoot(_)
            | AppError::RemoteExecutionRequiresCommitSha(_, _)
            | AppError::MissingDatabaseUrl
            | AppError::UserStateNotAllowed
            | AppError::UserTargetNotAllowed
            | AppError::UserProfilesDirNotAllowed
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
            AppError::NotFound(_) => StatusCode::NOT_FOUND,
            AppError::Conflict(_) => StatusCode::CONFLICT,
            AppError::BadRequest(_) => StatusCode::BAD_REQUEST,
            AppError::ServiceUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            AppError::RequestTimeout(_) => StatusCode::GATEWAY_TIMEOUT,
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
