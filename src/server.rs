use crate::api::{
    EnvironmentCreateApiRequest, EnvironmentResponse, EnvironmentUpdateApiRequest,
    EnvironmentsResponse, InvocationCommandApi, InvocationCreateApiRequest,
    InvocationCreateResponse, InvocationEvent, InvocationLifecycleStatus, InvocationStatusResponse,
    MigrateResponse, ProjectInitApiRequest, ProjectResponse, ProjectShowApiRequest,
    ProjectUpdateApiRequest, ProjectsResponse,
};
use crate::config::RuntimeConfig;
use crate::db::Db;
use crate::error::{AppError, AppResult};
use crate::event::LogEvent;
use crate::services::{
    EnvironmentCreateRequest, EnvironmentService, EnvironmentUpdateRequest, InvocationCommand,
    InvocationObserver, InvocationRequest, InvocationService, ProjectInitRequest, ProjectService,
    ProjectUpdateRequest,
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
use tower_http::trace::TraceLayer;
use tracing::{error, info, info_span, warn};
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

struct InvocationRuntime {
    status: Mutex<InvocationStatusResponse>,
    history: Mutex<Vec<InvocationEvent>>,
    tx: broadcast::Sender<InvocationEvent>,
}

impl InvocationManager {
    async fn create(&self) -> (Uuid, Arc<InvocationRuntime>) {
        let invocation_id = Uuid::new_v4();
        let status = InvocationStatusResponse {
            invocation_id,
            status: InvocationLifecycleStatus::Running,
            exit_code: None,
            error: None,
            started_at: Utc::now(),
            completed_at: None,
        };
        let (tx, _) = broadcast::channel(1024);
        let runtime = Arc::new(InvocationRuntime {
            status: Mutex::new(status),
            history: Mutex::new(Vec::new()),
            tx,
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
}

impl InvocationRuntime {
    async fn push_event(&self, event: InvocationEvent) {
        self.history.lock().await.push(event.clone());
        let _ = self.tx.send(event);
    }

    async fn status(&self) -> InvocationStatusResponse {
        self.status.lock().await.clone()
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
        self.push_event(completed).await;
    }
}

struct StreamingInvocationObserver {
    runtime: Arc<InvocationRuntime>,
}

impl InvocationObserver for StreamingInvocationObserver {
    fn stdout_line(&mut self, line: &str) {
        let runtime = self.runtime.clone();
        let text = line.to_string();
        tokio::spawn(async move {
            runtime
                .push_event(InvocationEvent {
                    event_type: "stdout.line".to_string(),
                    timestamp: Utc::now(),
                    text: Some(text),
                    stream: Some("stdout".to_string()),
                    dbt_event_name: None,
                    node_unique_id: None,
                    level: None,
                    exit_code: None,
                    error: None,
                })
                .await;
        });
    }

    fn stderr_line(&mut self, line: &str) {
        let runtime = self.runtime.clone();
        let text = line.to_string();
        tokio::spawn(async move {
            runtime
                .push_event(InvocationEvent {
                    event_type: "stderr.line".to_string(),
                    timestamp: Utc::now(),
                    text: Some(text),
                    stream: Some("stderr".to_string()),
                    dbt_event_name: None,
                    node_unique_id: None,
                    level: None,
                    exit_code: None,
                    error: None,
                })
                .await;
        });
    }

    fn dbt_log(&mut self, event: &LogEvent, rendered: Option<&str>) {
        let runtime = self.runtime.clone();
        let payload = InvocationEvent {
            event_type: "dbt.log".to_string(),
            timestamp: Utc::now(),
            text: rendered.map(ToString::to_string),
            stream: Some("stdout".to_string()),
            dbt_event_name: Some(event.info.name.clone()),
            node_unique_id: event
                .data
                .get("node_info")
                .and_then(|value| value.get("unique_id"))
                .and_then(|value| value.as_str())
                .map(ToString::to_string),
            level: Some(event.info.level.clone()),
            exit_code: None,
            error: None,
        };
        tokio::spawn(async move {
            runtime.push_event(payload).await;
        });
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
        .route("/v1/invocations", post(invocation_create))
        .route("/v1/invocations/{id}", get(invocation_status))
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
    let (invocation_id, runtime) = state.invocations.create().await;
    info!(
        invocation_id = %invocation_id,
        command = ?request.command,
        current_dir = %request.current_dir,
        "starting invocation"
    );
    runtime
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

    let db = state.db.clone();
    let runtime_config = state.runtime_config.clone();
    tokio::spawn(async move {
        let service = InvocationService::new(&db);
        let mut observer = StreamingInvocationObserver {
            runtime: runtime.clone(),
        };
        let result = service
            .invoke(
                InvocationRequest {
                    command: map_invocation_command(request.command),
                    args: request.args.into_iter().map(Into::into).collect(),
                    config: runtime_config,
                    current_dir: Some(PathBuf::from(request.current_dir)),
                    environment_slug: request.environment_slug,
                },
                &mut observer,
            )
            .await;
        match result {
            Ok(result) => {
                info!(
                    invocation_id = %invocation_id,
                    exit_code = result.exit_code,
                    "invocation completed successfully"
                );
                runtime
                    .finish(InvocationLifecycleStatus::Succeeded, result.exit_code, None)
                    .await
            }
            Err(err) => {
                warn!(
                    invocation_id = %invocation_id,
                    exit_code = err.exit_code(),
                    error = %err,
                    "invocation failed"
                );
                runtime
                    .finish(
                        InvocationLifecycleStatus::Failed,
                        err.exit_code(),
                        Some(err.to_string()),
                    )
                    .await
            }
        }
    });
    Ok(Json(InvocationCreateResponse { invocation_id }))
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
    let history = runtime.history.lock().await.clone();
    let rx = runtime.tx.subscribe();
    info!(invocation_id = %id, buffered_events = history.len(), "subscribed to invocation event stream");
    let stream = event_stream(history, rx);
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

fn event_stream(
    history: Vec<InvocationEvent>,
    mut rx: broadcast::Receiver<InvocationEvent>,
) -> impl Stream<Item = Result<Event, Infallible>> {
    stream! {
        for item in history {
            yield Ok(to_sse_event(&item));
        }
        loop {
            match rx.recv().await {
                Ok(item) => yield Ok(to_sse_event(&item)),
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
            | AppError::MissingSecretKey => StatusCode::BAD_REQUEST,
            AppError::ProjectIdNotFound(_) | AppError::EnvironmentNotFound(_, _) => {
                StatusCode::NOT_FOUND
            }
            AppError::EnvironmentAlreadyExists(_, _) | AppError::ProjectIdAlreadyConfigured(_) => {
                StatusCode::CONFLICT
            }
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
    use super::{InvocationEvent, event_stream};
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
        let history = vec![sample_event("one")];
        let mut stream = Box::pin(event_stream(history, rx));

        let first = stream.next().await.expect("history item").expect("event");
        let _first = first;

        tx.send(sample_event("two")).expect("send live event");
        let second = stream.next().await.expect("live item").expect("event");
        let _second = second;
    }
}
