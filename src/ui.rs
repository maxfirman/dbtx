use crate::api::{
    InvocationCancelStateApi, InvocationExecutionModeApi, InvocationLifecycleStatus,
    InvocationListApiRequest, InvocationStatusResponse, QueueStatusResponse, WorkerStatusResponse,
};
use crate::db::{EnvironmentRecord, EnvironmentVersionRecord, ProjectRecord};
use crate::error::{AppError, AppResult};
use crate::server::AppState;
use askama::Template;
use axum::extract::{Form, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::Router;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use uuid::Uuid;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(dashboard))
        .route("/ui/projects", get(projects_index))
        .route("/ui/invocations", get(invocations_index))
        .route("/ui/invocations/{id}", get(invocation_detail))
        .route("/ui/invocations/{id}/cancel", post(invocation_cancel))
        .route("/ui/workers", get(workers_index))
        .route("/ui/queues", get(queues_index))
        .route(
            "/ui/projects/{project_id}/environments/{slug}",
            get(environment_detail),
        )
        .route(
            "/ui/projects/{project_id}/environments/{slug}/release",
            post(environment_release),
        )
        .route(
            "/ui/projects/{project_id}/environments/{slug}/rollback",
            post(environment_rollback),
        )
}

#[derive(Debug)]
struct UiError(AppError);

impl From<AppError> for UiError {
    fn from(value: AppError) -> Self {
        Self(value)
    }
}

impl IntoResponse for UiError {
    fn into_response(self) -> Response {
        let (status, title) = match &self.0 {
            AppError::ProjectIdNotFound(_)
            | AppError::EnvironmentNotFound(_, _)
            | AppError::EnvironmentVersionNotFound(_, _, _) => {
                (StatusCode::NOT_FOUND, "Not Found")
            }
            _ => (StatusCode::INTERNAL_SERVER_ERROR, "Server Error"),
        };
        let body = render_template(&ErrorTemplate {
            title,
            message: &self.0.to_string(),
        })
        .unwrap_or_else(|_| Html(format!("<h1>{title}</h1><p>{}</p>", self.0)));
        (status, body).into_response()
    }
}

fn render_template<T: Template>(template: &T) -> Result<Html<String>, UiError> {
    template
        .render()
        .map(Html)
        .map_err(|err| UiError(AppError::Io(std::io::Error::other(err.to_string()))))
}

fn is_htmx(headers: &HeaderMap) -> bool {
    headers
        .get("HX-Request")
        .and_then(|value| value.to_str().ok())
        == Some("true")
}

async fn dashboard(State(state): State<AppState>) -> Result<Html<String>, UiError> {
    let db = state.db();
    db.require_current_schema().await?;
    let projects = db.list_projects().await?;
    let invocations = db
        .list_invocations(InvocationListApiRequest {
            limit: Some(10),
            ..Default::default()
        })
        .await?;
    let workers = db.list_workers().await?;
    let queues = db.list_queues().await?;

    let page = DashboardTemplate {
        title: "Dashboard",
        project_count: projects.len() as i64,
        active_invocation_count: invocations
            .iter()
            .filter(|item| matches!(item.status, InvocationLifecycleStatus::Running))
            .count() as i64,
        worker_count: workers.len() as i64,
        queued_work_count: queues.iter().map(|item| item.pending_count).sum(),
        invocations: invocations.iter().map(invocation_summary_view).collect(),
        projects: projects.iter().map(project_summary_view).collect(),
        workers: workers.iter().map(worker_summary_view).collect(),
        queues: queues.iter().map(queue_summary_view).collect(),
    };
    render_template(&page)
}

async fn projects_index(State(state): State<AppState>) -> Result<Html<String>, UiError> {
    let db = state.db();
    db.require_current_schema().await?;
    let projects = db.list_projects().await?;
    let mut views = Vec::with_capacity(projects.len());
    for project in projects {
        let environments = db.list_environments(&project.project_id).await?;
        views.push(ProjectWithEnvironmentsView {
            project: project_summary_view(&project),
            environments: environments.iter().map(environment_link_view).collect(),
        });
    }

    render_template(&ProjectsTemplate {
        title: "Projects",
        projects: views,
    })
}

#[derive(Debug, Default, Deserialize)]
struct InvocationFilterQuery {
    status: Option<InvocationLifecycleStatus>,
    execution_mode: Option<InvocationExecutionModeApi>,
    worker_queue: Option<String>,
    claimed_by: Option<String>,
    cancel_state: Option<InvocationCancelStateApi>,
    limit: Option<i64>,
}

async fn invocations_index(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<InvocationFilterQuery>,
) -> Result<Html<String>, UiError> {
    let db = state.db();
    db.require_current_schema().await?;
    let invocations = db
        .list_invocations(InvocationListApiRequest {
            status: query.status.clone(),
            execution_mode: query.execution_mode,
            worker_queue: query.worker_queue.clone(),
            claimed_by: query.claimed_by.clone(),
            cancel_state: query.cancel_state,
            limit: query.limit.or(Some(100)),
        })
        .await?;
    let rows = invocations.iter().map(invocation_summary_view).collect::<Vec<_>>();
    if is_htmx(&headers) {
        return render_template(&InvocationTableTemplate { invocations: rows });
    }

    render_template(&InvocationsPageTemplate {
        title: "Invocations",
        filters: InvocationFilterView {
            status: query
                .status
                .as_ref()
                .map(invocation_status_value)
                .unwrap_or_default()
                .to_string(),
            execution_mode: query
                .execution_mode
                .as_ref()
                .map(invocation_mode_value)
                .unwrap_or_default()
                .to_string(),
            worker_queue: query.worker_queue.unwrap_or_default(),
            claimed_by: query.claimed_by.unwrap_or_default(),
            cancel_state: query
                .cancel_state
                .as_ref()
                .map(cancel_state_value)
                .unwrap_or_default()
                .to_string(),
            limit: query.limit.map(|value| value.to_string()).unwrap_or_default(),
        },
        invocations: rows,
    })
}

async fn invocation_detail(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Html<String>, UiError> {
    let db = state.db();
    db.require_current_schema().await?;
    let invocation_id = parse_uuid(&id)?;
    let invocation = db.get_invocation_status(invocation_id).await?;
    let events = db.load_invocation_events_since(invocation_id, 0).await?;
    let lines = events
        .into_iter()
        .filter_map(|(_, event)| {
            event.text.as_deref().and_then(|text| {
                let cleaned = strip_ansi(text);
                if cleaned.trim().is_empty() {
                    None
                } else {
                    Some(cleaned)
                }
            })
        })
        .collect();

    render_template(&InvocationDetailTemplate {
        title: "Invocation",
        invocation: invocation_detail_view(&invocation),
        initial_log_lines: lines,
        sse_url: format!("/v1/invocations/{invocation_id}/events"),
    })
}

async fn invocation_cancel(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, UiError> {
    let db = state.db();
    db.require_current_schema().await?;
    let invocation_id = parse_uuid(&id)?;
    db.request_cancel_invocation(invocation_id).await?;
    Ok(Redirect::to(&format!("/ui/invocations/{invocation_id}")))
}

async fn workers_index(State(state): State<AppState>) -> Result<Html<String>, UiError> {
    let db = state.db();
    db.require_current_schema().await?;
    let workers = db.list_workers().await?;
    render_template(&WorkersTemplate {
        title: "Workers",
        workers: workers.iter().map(worker_summary_view).collect(),
    })
}

async fn queues_index(State(state): State<AppState>) -> Result<Html<String>, UiError> {
    let db = state.db();
    db.require_current_schema().await?;
    let queues = db.list_queues().await?;
    render_template(&QueuesTemplate {
        title: "Queues",
        queues: queues.iter().map(queue_summary_view).collect(),
    })
}

async fn environment_detail(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((project_id, slug)): Path<(String, String)>,
) -> Result<Html<String>, UiError> {
    let db = state.db();
    db.require_current_schema().await?;
    let project = db.get_project_by_project_id(&project_id).await?;
    let environment = db
        .list_environments(&project.project_id)
        .await?
        .into_iter()
        .find(|environment| environment.slug == slug)
        .ok_or_else(|| UiError(AppError::EnvironmentNotFound(project.project_id.clone(), slug.clone())))?;
    let history = db.list_environment_versions(&project.project_id, &slug).await?;
    let panel = EnvironmentPanelTemplate {
        project: project_summary_view(&project),
        environment: environment_detail_view(&environment),
        versions: history.iter().map(environment_version_view).collect(),
        is_remote: project.mode == "remote",
    };

    if is_htmx(&headers) {
        return render_template(&panel);
    }

    render_template(&EnvironmentPageTemplate {
        title: "Environment",
        project: project_summary_view(&project),
        environment_slug: environment.slug.clone(),
        panel_html: panel
            .render()
            .map_err(|err| UiError(AppError::Io(std::io::Error::other(err.to_string()))))?,
    })
}

#[derive(Debug, Deserialize)]
struct EnvironmentReleaseForm {
    git_branch: Option<String>,
    git_commit_sha: String,
}

async fn environment_release(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((project_id, slug)): Path<(String, String)>,
    Form(form): Form<EnvironmentReleaseForm>,
) -> Result<Response, UiError> {
    let db = state.db();
    db.require_current_schema().await?;
    let project = db.get_project_by_project_id(&project_id).await?;
    if project.mode != "remote" {
        return Err(UiError(AppError::RemoteExecutionRequiresRemoteProject(
            project.project_id,
            project.mode,
        )));
    }
    db.release_environment(crate::db::EnvironmentReleaseInput {
        project: project.project_id.clone(),
        slug: slug.clone(),
        git_branch: form.git_branch,
        git_commit_sha: form.git_commit_sha,
    })
    .await?;

    if is_htmx(&headers) {
        return environment_detail(
            State(state),
            htmx_headers(),
            Path((project_id, slug)),
        )
        .await
        .map(IntoResponse::into_response);
    }

    Ok(Redirect::to(&format!("/ui/projects/{project_id}/environments/{slug}")).into_response())
}

#[derive(Debug, Deserialize)]
struct EnvironmentRollbackForm {
    version_id: i64,
}

async fn environment_rollback(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((project_id, slug)): Path<(String, String)>,
    Form(form): Form<EnvironmentRollbackForm>,
) -> Result<Response, UiError> {
    let db = state.db();
    db.require_current_schema().await?;
    let project = db.get_project_by_project_id(&project_id).await?;
    if project.mode != "remote" {
        return Err(UiError(AppError::RemoteExecutionRequiresRemoteProject(
            project.project_id,
            project.mode,
        )));
    }
    db.rollback_environment_to_version(&project.project_id, &slug, form.version_id)
        .await?;

    if is_htmx(&headers) {
        return environment_detail(
            State(state),
            htmx_headers(),
            Path((project_id, slug)),
        )
        .await
        .map(IntoResponse::into_response);
    }

    Ok(Redirect::to(&format!("/ui/projects/{project_id}/environments/{slug}")).into_response())
}

fn htmx_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("HX-Request", "true".parse().expect("valid header"));
    headers
}

fn parse_uuid(value: &str) -> AppResult<Uuid> {
    Uuid::parse_str(value).map_err(|err| {
        AppError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid uuid: {err}"),
        ))
    })
}

fn strip_ansi(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' && chars.peek() == Some(&'[') {
            let _ = chars.next();
            for next in chars.by_ref() {
                if next.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            output.push(ch);
        }
    }
    output
}

fn fmt_ts(value: DateTime<Utc>) -> String {
    value.format("%Y-%m-%d %H:%M:%S UTC").to_string()
}

fn fmt_optional_ts(value: Option<DateTime<Utc>>) -> String {
    value.map(fmt_ts).unwrap_or_default()
}

fn invocation_status_value(value: &InvocationLifecycleStatus) -> &'static str {
    match value {
        InvocationLifecycleStatus::Running => "running",
        InvocationLifecycleStatus::Succeeded => "succeeded",
        InvocationLifecycleStatus::Failed => "failed",
        InvocationLifecycleStatus::Canceled => "canceled",
    }
}

fn invocation_mode_value(value: &InvocationExecutionModeApi) -> &'static str {
    match value {
        InvocationExecutionModeApi::Server => "server",
        InvocationExecutionModeApi::Local => "local",
    }
}

fn cancel_state_value(value: &InvocationCancelStateApi) -> &'static str {
    match value {
        InvocationCancelStateApi::None => "none",
        InvocationCancelStateApi::Requested => "requested",
        InvocationCancelStateApi::Completed => "completed",
    }
}

fn status_badge_class(status: &str) -> &'static str {
    match status {
        "running" => "bg-amber-100 text-amber-800",
        "succeeded" => "bg-emerald-100 text-emerald-800",
        "failed" => "bg-rose-100 text-rose-800",
        "canceled" => "bg-slate-200 text-slate-700",
        "claimed" => "bg-sky-100 text-sky-800",
        "stale" => "bg-orange-100 text-orange-800",
        _ => "bg-slate-100 text-slate-700",
    }
}

#[derive(Clone)]
struct ProjectSummaryView {
    project_id: String,
    project_name: String,
    mode: String,
    git_repo_url: String,
    project_root: String,
}

#[derive(Clone)]
struct ProjectWithEnvironmentsView {
    project: ProjectSummaryView,
    environments: Vec<EnvironmentLinkView>,
}

#[derive(Clone)]
struct EnvironmentLinkView {
    slug: String,
    target_name: String,
    adapter_type: String,
    status: String,
    status_class: &'static str,
    detail_url: String,
}

#[derive(Clone)]
struct InvocationSummaryView {
    invocation_id: String,
    status: String,
    status_class: &'static str,
    execution_mode: String,
    worker_queue: String,
    claimed_by: String,
    started_at: String,
    completed_at: String,
    detail_url: String,
}

#[derive(Clone)]
struct InvocationDetailView {
    invocation_id: String,
    status: String,
    status_class: &'static str,
    execution_mode: String,
    worker_queue: String,
    worker_health: String,
    cancel_state: String,
    claimed_by: String,
    claimed_at: String,
    last_heartbeat_at: String,
    started_at: String,
    completed_at: String,
    error: String,
}

#[derive(Clone)]
struct WorkerSummaryView {
    worker_id: String,
    execution_mode: String,
    worker_queue: String,
    claimed_invocation_count: i64,
    last_heartbeat_at: String,
    health: String,
    health_class: &'static str,
}

#[derive(Clone)]
struct QueueSummaryView {
    worker_queue: String,
    execution_mode: String,
    pending_count: i64,
    claimed_count: i64,
    stale_claim_count: i64,
    oldest_pending_at: String,
}

#[derive(Clone)]
struct EnvironmentDetailView {
    slug: String,
    profile_name: String,
    target_name: String,
    adapter_type: String,
    worker_queue: String,
    schema_name: String,
    status: String,
    status_class: &'static str,
    git_branch: String,
    git_commit_sha: String,
}

#[derive(Clone)]
struct EnvironmentVersionView {
    id: i64,
    recorded_at: String,
    reason: String,
    git_branch: String,
    git_commit_sha: String,
}

#[derive(Clone)]
struct InvocationFilterView {
    status: String,
    execution_mode: String,
    worker_queue: String,
    claimed_by: String,
    cancel_state: String,
    limit: String,
}

fn project_summary_view(project: &ProjectRecord) -> ProjectSummaryView {
    ProjectSummaryView {
        project_id: project.project_id.clone(),
        project_name: project.project_name.clone(),
        mode: project.mode.clone(),
        git_repo_url: project.git_repo_url.clone().unwrap_or_default(),
        project_root: project.project_root.clone().unwrap_or_default(),
    }
}

fn environment_link_view(environment: &EnvironmentRecord) -> EnvironmentLinkView {
    EnvironmentLinkView {
        slug: environment.slug.clone(),
        target_name: environment.target_name.clone(),
        adapter_type: environment.adapter_type.clone(),
        status: environment.status.clone(),
        status_class: status_badge_class(&environment.status),
        detail_url: format!(
            "/ui/projects/{}/environments/{}",
            environment.project_ref, environment.slug
        ),
    }
}

fn invocation_summary_view(invocation: &InvocationStatusResponse) -> InvocationSummaryView {
    let status = invocation_status_value(&invocation.status).to_string();
    InvocationSummaryView {
        invocation_id: invocation.invocation_id.to_string(),
        status_class: status_badge_class(&status),
        status,
        execution_mode: invocation_mode_value(&invocation.execution_mode).to_string(),
        worker_queue: invocation.worker_queue.clone(),
        claimed_by: invocation.claimed_by.clone().unwrap_or_default(),
        started_at: fmt_ts(invocation.started_at),
        completed_at: fmt_optional_ts(invocation.completed_at),
        detail_url: format!("/ui/invocations/{}", invocation.invocation_id),
    }
}

fn invocation_detail_view(invocation: &InvocationStatusResponse) -> InvocationDetailView {
    let status = invocation_status_value(&invocation.status).to_string();
    InvocationDetailView {
        invocation_id: invocation.invocation_id.to_string(),
        status_class: status_badge_class(&status),
        status,
        execution_mode: invocation_mode_value(&invocation.execution_mode).to_string(),
        worker_queue: invocation.worker_queue.clone(),
        worker_health: format!("{:?}", invocation.worker_health).to_lowercase(),
        cancel_state: format!("{:?}", invocation.cancel_state).to_lowercase(),
        claimed_by: invocation.claimed_by.clone().unwrap_or_default(),
        claimed_at: fmt_optional_ts(invocation.claimed_at),
        last_heartbeat_at: fmt_optional_ts(invocation.last_heartbeat_at),
        started_at: fmt_ts(invocation.started_at),
        completed_at: fmt_optional_ts(invocation.completed_at),
        error: invocation.error.clone().unwrap_or_default(),
    }
}

fn worker_summary_view(worker: &WorkerStatusResponse) -> WorkerSummaryView {
    let health = format!("{:?}", worker.health).to_lowercase();
    WorkerSummaryView {
        worker_id: worker.worker_id.clone(),
        execution_mode: invocation_mode_value(&worker.execution_mode).to_string(),
        worker_queue: worker.worker_queue.clone(),
        claimed_invocation_count: worker.claimed_invocation_count,
        last_heartbeat_at: fmt_optional_ts(worker.last_heartbeat_at),
        health_class: status_badge_class(&health),
        health,
    }
}

fn queue_summary_view(queue: &QueueStatusResponse) -> QueueSummaryView {
    QueueSummaryView {
        worker_queue: queue.worker_queue.clone(),
        execution_mode: invocation_mode_value(&queue.execution_mode).to_string(),
        pending_count: queue.pending_count,
        claimed_count: queue.claimed_count,
        stale_claim_count: queue.stale_claim_count,
        oldest_pending_at: fmt_optional_ts(queue.oldest_pending_at),
    }
}

fn environment_detail_view(environment: &EnvironmentRecord) -> EnvironmentDetailView {
    EnvironmentDetailView {
        slug: environment.slug.clone(),
        profile_name: environment.profile_name.clone(),
        target_name: environment.target_name.clone(),
        adapter_type: environment.adapter_type.clone(),
        worker_queue: environment.worker_queue.clone(),
        schema_name: environment.schema_name.clone(),
        status_class: status_badge_class(&environment.status),
        status: environment.status.clone(),
        git_branch: environment.git_branch.clone().unwrap_or_default(),
        git_commit_sha: environment.git_commit_sha.clone().unwrap_or_default(),
    }
}

fn environment_version_view(version: &EnvironmentVersionRecord) -> EnvironmentVersionView {
    EnvironmentVersionView {
        id: version.id,
        recorded_at: fmt_ts(version.recorded_at),
        reason: version.reason.clone(),
        git_branch: version.git_branch.clone().unwrap_or_default(),
        git_commit_sha: version.git_commit_sha.clone().unwrap_or_default(),
    }
}

#[derive(Template)]
#[template(path = "dashboard.html")]
struct DashboardTemplate {
    title: &'static str,
    project_count: i64,
    active_invocation_count: i64,
    worker_count: i64,
    queued_work_count: i64,
    invocations: Vec<InvocationSummaryView>,
    projects: Vec<ProjectSummaryView>,
    workers: Vec<WorkerSummaryView>,
    queues: Vec<QueueSummaryView>,
}

#[derive(Template)]
#[template(path = "projects/index.html")]
struct ProjectsTemplate {
    title: &'static str,
    projects: Vec<ProjectWithEnvironmentsView>,
}

#[derive(Template)]
#[template(path = "invocations/index.html")]
struct InvocationsPageTemplate {
    title: &'static str,
    filters: InvocationFilterView,
    invocations: Vec<InvocationSummaryView>,
}

#[derive(Template)]
#[template(path = "invocations/_table.html")]
struct InvocationTableTemplate {
    invocations: Vec<InvocationSummaryView>,
}

#[derive(Template)]
#[template(path = "invocations/show.html")]
struct InvocationDetailTemplate {
    title: &'static str,
    invocation: InvocationDetailView,
    initial_log_lines: Vec<String>,
    sse_url: String,
}

#[derive(Template)]
#[template(path = "workers/index.html")]
struct WorkersTemplate {
    title: &'static str,
    workers: Vec<WorkerSummaryView>,
}

#[derive(Template)]
#[template(path = "queues/index.html")]
struct QueuesTemplate {
    title: &'static str,
    queues: Vec<QueueSummaryView>,
}

#[derive(Template)]
#[template(path = "environments/show.html")]
struct EnvironmentPageTemplate {
    title: &'static str,
    project: ProjectSummaryView,
    environment_slug: String,
    panel_html: String,
}

#[derive(Template)]
#[template(path = "environments/_panel.html")]
struct EnvironmentPanelTemplate {
    project: ProjectSummaryView,
    environment: EnvironmentDetailView,
    versions: Vec<EnvironmentVersionView>,
    is_remote: bool,
}

#[derive(Template)]
#[template(path = "error.html")]
struct ErrorTemplate<'a> {
    title: &'a str,
    message: &'a str,
}
