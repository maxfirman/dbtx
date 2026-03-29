use crate::api::{
    InvocationCancelStateApi, InvocationExecutionModeApi, InvocationLifecycleStatus,
    InvocationListApiRequest, InvocationStatusResponse, QueueStatusResponse, WorkerStatusResponse,
};
use crate::db::{EnvironmentRecord, EnvironmentVersionRecord, ProjectRecord};
use crate::error::{AppError, AppResult};
use crate::invocation_bootstrap::{
    start_environment_draft_prepare_invocation, start_environment_draft_validation_invocation,
    start_project_draft_validation_invocation,
};
use crate::server::AppState;
use crate::services::{
    EnvironmentDraftUpdateRequest, EnvironmentService, ProjectCreateRequest, ProjectService,
};
use askama::Template;
use axum::Router;
use axum::extract::{Form, Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(dashboard))
        .route("/ui/projects", get(projects_index))
        .route("/ui/projects/new", get(project_create_modal))
        .route(
            "/ui/projects/{project_id}/environments/new",
            get(environment_create_modal),
        )
        .route(
            "/ui/projects/{project_id}/delete",
            get(project_delete_modal).post(project_delete),
        )
        .route("/ui/project-drafts", post(project_draft_create))
        .route("/ui/project-drafts/{draft_id}", get(project_draft_status))
        .route(
            "/ui/project-drafts/{draft_id}/confirm",
            post(project_draft_confirm),
        )
        .route(
            "/ui/environment-drafts/{draft_id}",
            get(environment_draft_status),
        )
        .route(
            "/ui/environment-drafts/{draft_id}/branch",
            post(environment_draft_branch_refresh),
        )
        .route(
            "/ui/environment-drafts/{draft_id}/validate",
            post(environment_draft_validate),
        )
        .route(
            "/ui/environment-drafts/{draft_id}/confirm",
            post(environment_draft_confirm),
        )
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
            | AppError::EnvironmentVersionNotFound(_, _, _) => (StatusCode::NOT_FOUND, "Not Found"),
            AppError::ProjectDeleteBlocked(_) => (StatusCode::CONFLICT, "Conflict"),
            AppError::Io(err) if err.kind() == std::io::ErrorKind::NotFound => {
                (StatusCode::NOT_FOUND, "Not Found")
            }
            AppError::Io(err) if err.kind() == std::io::ErrorKind::InvalidInput => {
                (StatusCode::BAD_REQUEST, "Invalid Request")
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

#[derive(Debug, Default, Deserialize, Clone)]
struct ProjectDraftForm {
    git_repo_url: String,
    project_root: String,
}

#[derive(Debug, Default, Deserialize, Clone)]
struct EnvironmentDraftForm {
    slug: String,
    git_branch: String,
    git_commit_sha: String,
    use_latest_commit: Option<String>,
    auto_deploy: Option<String>,
    immutable: Option<String>,
    adapter_type: String,
    schema_name: String,
    #[serde(default, deserialize_with = "deserialize_optional_i32_form_field")]
    threads: Option<i32>,
    profile_config_json: Option<String>,
    profile_secrets_json: Option<String>,
}

fn deserialize_optional_i32_form_field<'de, D>(
    deserializer: D,
) -> Result<Option<i32>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;
    match value.as_deref().map(str::trim) {
        None | Some("") => Ok(None),
        Some(raw) => raw
            .parse::<i32>()
            .map(Some)
            .map_err(serde::de::Error::custom),
    }
}

async fn project_create_modal() -> Result<Html<String>, UiError> {
    render_template(&ProjectCreateModalTemplate {
        draft: ProjectDraftForm::default(),
        error: None,
    })
}

async fn environment_create_modal(
    State(state): State<AppState>,
    Path(project_id): Path<String>,
) -> Result<Html<String>, UiError> {
    let service = EnvironmentService::new(state.db());
    let draft = service.create_draft(project_id).await?;
    start_environment_draft_prepare(&state, draft.id).await?;
    render_environment_draft_modal(state.db(), &service.get_draft(draft.id).await?).await
}

async fn project_delete_modal(
    State(state): State<AppState>,
    Path(project_id): Path<String>,
) -> Result<Html<String>, UiError> {
    let project = ProjectService::new(state.db()).show(project_id).await?;
    render_template(&ProjectDeleteModalTemplate {
        project: project_summary_view(&project),
        error: None,
    })
}

async fn environment_draft_status(
    State(state): State<AppState>,
    Path(draft_id): Path<Uuid>,
) -> Result<Html<String>, UiError> {
    render_environment_draft_modal(state.db(), &EnvironmentService::new(state.db()).get_draft(draft_id).await?).await
}

async fn environment_draft_branch_refresh(
    State(state): State<AppState>,
    Path(draft_id): Path<Uuid>,
    Form(form): Form<EnvironmentDraftForm>,
) -> Result<Html<String>, UiError> {
    let service = EnvironmentService::new(state.db());
    let request = environment_draft_update_request(form)?;
    let prepared = match service.refresh_draft_branch(draft_id, request).await {
        Ok(prepared) => prepared,
        Err(error) => {
            let draft = state
                .db()
                .fail_environment_draft(draft_id, &error.to_string())
                .await?;
            return render_environment_draft_modal(state.db(), &draft).await;
        }
    };
    start_environment_draft_prepared(&state, prepared).await?;
    render_environment_draft_modal(state.db(), &service.get_draft(draft_id).await?).await
}

async fn environment_draft_validate(
    State(state): State<AppState>,
    Path(draft_id): Path<Uuid>,
    Form(form): Form<EnvironmentDraftForm>,
) -> Result<Html<String>, UiError> {
    let service = EnvironmentService::new(state.db());
    let request = environment_draft_update_request(form)?;
    let prepared = match service.prepare_draft_validation(draft_id, request).await {
        Ok(prepared) => prepared,
        Err(error) => {
            let draft = state
                .db()
                .fail_environment_draft(draft_id, &error.to_string())
                .await?;
            return render_environment_draft_modal(state.db(), &draft).await;
        }
    };
    start_environment_draft_validation(&state, prepared).await?;
    render_environment_draft_modal(state.db(), &service.get_draft(draft_id).await?).await
}

async fn environment_draft_confirm(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(draft_id): Path<Uuid>,
) -> Result<impl IntoResponse, UiError> {
    let environment = EnvironmentService::new(state.db()).confirm_draft(draft_id).await?;
    let redirect = format!(
        "/ui/projects/{}/environments/{}",
        environment.project_ref, environment.slug
    );
    if is_htmx(&headers) {
        let mut response = Html(String::new()).into_response();
        response
            .headers_mut()
            .insert("HX-Redirect", HeaderValue::from_str(&redirect).unwrap());
        Ok(response)
    } else {
        Ok(Redirect::to(&redirect).into_response())
    }
}

async fn project_draft_create(
    State(state): State<AppState>,
    Form(form): Form<ProjectDraftForm>,
) -> Result<Html<String>, UiError> {
    let service = ProjectService::new(state.db());
    let draft = match service
        .create_draft(ProjectCreateRequest {
            git_repo_url: form.git_repo_url.clone(),
            project_root: form.project_root.clone(),
        })
        .await
    {
        Ok(draft) => draft,
        Err(err) => {
            return render_template(&ProjectDraftFailedTemplate {
                error: err.to_string(),
            });
        }
    };
    if let Err(err) = start_project_draft_validation(&state, draft.id).await {
        return render_template(&ProjectDraftFailedTemplate {
            error: err.0.to_string(),
        });
    }
    let draft = service.get_draft(draft.id).await?;
    render_project_draft_fragment(&draft, None, true)
}

async fn project_draft_status(
    State(state): State<AppState>,
    Path(draft_id): Path<Uuid>,
) -> Result<Html<String>, UiError> {
    let draft = ProjectService::new(state.db()).get_draft(draft_id).await?;
    render_project_draft_fragment(
        &draft,
        None,
        !is_terminal_project_draft_status(&draft.status),
    )
}

async fn project_draft_confirm(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(draft_id): Path<Uuid>,
) -> Result<impl IntoResponse, UiError> {
    ProjectService::new(state.db())
        .confirm_draft(draft_id)
        .await?;
    if is_htmx(&headers) {
        let mut response = Html(String::new()).into_response();
        response
            .headers_mut()
            .insert("HX-Redirect", HeaderValue::from_static("/ui/projects"));
        Ok(response)
    } else {
        Ok(Redirect::to("/ui/projects").into_response())
    }
}

async fn project_delete(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(project_id): Path<String>,
) -> Result<impl IntoResponse, UiError> {
    let service = ProjectService::new(state.db());
    if let Err(err) = service.delete(project_id.clone()).await {
        let project = service.show(project_id).await?;
        return Ok(render_template(&ProjectDeleteModalTemplate {
            project: project_summary_view(&project),
            error: Some(err.to_string()),
        })?
        .into_response());
    }
    if is_htmx(&headers) {
        let mut response = Html(String::new()).into_response();
        response
            .headers_mut()
            .insert("HX-Redirect", HeaderValue::from_static("/ui/projects"));
        Ok(response)
    } else {
        Ok(Redirect::to("/ui/projects").into_response())
    }
}

async fn start_project_draft_validation(state: &AppState, draft_id: Uuid) -> Result<(), UiError> {
    let prepared = ProjectService::new(state.db())
        .prepare_draft_validation(draft_id)
        .await?;
    start_project_draft_validation_invocation(state, prepared)
        .await
        .map_err(UiError::from)?;
    Ok(())
}

async fn start_environment_draft_prepare(state: &AppState, draft_id: Uuid) -> Result<(), UiError> {
    let prepared = EnvironmentService::new(state.db())
        .prepare_draft_git_metadata(draft_id)
        .await?;
    start_environment_draft_prepared(state, prepared).await
}

async fn start_environment_draft_prepared(
    state: &AppState,
    prepared: crate::services::EnvironmentDraftCreatePrepared,
) -> Result<(), UiError> {
    start_environment_draft_prepare_invocation(state, prepared)
        .await
        .map_err(UiError::from)?;
    Ok(())
}

async fn start_environment_draft_validation(
    state: &AppState,
    prepared: crate::services::EnvironmentDraftValidationPrepared,
) -> Result<(), UiError> {
    start_environment_draft_validation_invocation(state, prepared)
        .await
        .map_err(UiError::from)?;
    Ok(())
}

fn render_project_draft_fragment(
    draft: &crate::db::ProjectDraftRecord,
    error: Option<String>,
    should_poll: bool,
) -> Result<Html<String>, UiError> {
    match draft.status.as_str() {
        "validated" => render_template(&ProjectDraftValidatedTemplate {
            draft: project_draft_view(draft),
        }),
        "failed" => render_template(&ProjectDraftFailedTemplate {
            error: error
                .or_else(|| draft.validation_error.clone())
                .unwrap_or_default(),
        }),
        _ => render_template(&ProjectDraftPendingTemplate {
            draft: project_draft_view(draft),
            should_poll,
        }),
    }
}

fn environment_draft_update_request(form: EnvironmentDraftForm) -> Result<EnvironmentDraftUpdateRequest, UiError> {
    let profile_config = parse_json_object(form.profile_config_json.as_deref().unwrap_or("{}"))?;
    let profile_secrets = parse_json_object(form.profile_secrets_json.as_deref().unwrap_or("{}"))?;
    Ok(EnvironmentDraftUpdateRequest {
        project: String::new(),
        slug: form.slug,
        git_branch: if form.git_branch.trim().is_empty() { None } else { Some(form.git_branch) },
        git_commit_sha: if form.git_commit_sha.trim().is_empty() { None } else { Some(form.git_commit_sha) },
        use_latest_commit: form.use_latest_commit.is_some(),
        auto_deploy: form.auto_deploy.is_some(),
        immutable: form.immutable.is_some(),
        adapter_type: form.adapter_type,
        schema_name: form.schema_name,
        threads: form.threads,
        profile_config,
        profile_secrets,
    })
}

fn parse_json_object(input: &str) -> Result<Value, UiError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Ok(serde_json::json!({}));
    }
    let value: Value = serde_json::from_str(trimmed)
        .map_err(AppError::from)
        .map_err(UiError::from)?;
    if value.is_object() {
        Ok(value)
    } else {
        Err(UiError(AppError::InvalidProfileConfig("expected object".to_string())))
    }
}

async fn render_environment_draft_modal(
    db: &crate::db::Db,
    draft: &crate::db::EnvironmentDraftRecord,
) -> Result<Html<String>, UiError> {
    let project = db.get_project_by_id(draft.project_id).await.map_err(UiError)?;
    render_template(&EnvironmentCreateModalTemplate {
        project: project_summary_view(&project),
        draft: environment_draft_view(&project, draft)?,
    })
}

fn is_terminal_project_draft_status(status: &str) -> bool {
    matches!(status, "validated" | "failed")
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
    let rows = invocations
        .iter()
        .map(invocation_summary_view)
        .collect::<Vec<_>>();
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
            limit: query
                .limit
                .map(|value| value.to_string())
                .unwrap_or_default(),
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
    let initial_log_sequence = events.last().map(|(sequence, _)| *sequence).unwrap_or(0);
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
        initial_log_sequence,
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
        .ok_or_else(|| {
            UiError(AppError::EnvironmentNotFound(
                project.project_id.clone(),
                slug.clone(),
            ))
        })?;
    let history = db
        .list_environment_versions(&project.project_id, &slug)
        .await?;
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
    let service = EnvironmentService::new(state.db());
    service
        .release(crate::services::EnvironmentReleaseRequest {
            project: project_id.clone(),
            slug: slug.clone(),
            git_branch: form.git_branch,
            git_commit_sha: Some(form.git_commit_sha),
            git_ref: None,
        })
        .await?;

    if is_htmx(&headers) {
        return environment_detail(State(state), htmx_headers(), Path((project_id, slug)))
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
    let service = EnvironmentService::new(state.db());
    service
        .rollback(crate::services::EnvironmentRollbackRequest {
            project: project_id.clone(),
            slug: slug.clone(),
            version_id: form.version_id,
        })
        .await?;

    if is_htmx(&headers) {
        return environment_detail(State(state), htmx_headers(), Path((project_id, slug)))
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
    delete_url: String,
    create_environment_url: String,
}

#[derive(Clone)]
struct ProjectWithEnvironmentsView {
    project: ProjectSummaryView,
    environments: Vec<EnvironmentLinkView>,
}

#[derive(Clone)]
struct ProjectDraftView {
    id: String,
    git_repo_url: String,
    project_root: String,
    status: String,
    status_class: &'static str,
    project_name: String,
    default_branch: String,
    has_validation_stream: bool,
    validation_sse_url: String,
    status_url: String,
}

#[derive(Clone, Serialize)]
struct EnvironmentDraftPairView {
    key: String,
    value: String,
}

#[derive(Clone)]
struct EnvironmentDraftBranchOptionView {
    name: String,
    selected: bool,
}

#[derive(Clone)]
struct EnvironmentDraftCommitOptionView {
    sha: String,
    short_sha: String,
    summary: String,
    committed_at: String,
    selected: bool,
}

#[derive(Clone)]
struct EnvironmentDraftView {
    status: String,
    slug: String,
    git_branch: String,
    git_commit_sha: String,
    latest_commit_sha: String,
    use_latest_commit: bool,
    auto_deploy: bool,
    immutable: bool,
    adapter_type: String,
    schema_name: String,
    threads: String,
    branch_options: Vec<EnvironmentDraftBranchOptionView>,
    commit_options: Vec<EnvironmentDraftCommitOptionView>,
    status_url: String,
    branch_refresh_url: String,
    validate_url: String,
    confirm_url: String,
    validation_error: String,
    validation_sse_url: String,
    profile_config_pairs_json: String,
    profile_secret_pairs_json: String,
    is_loading: bool,
    is_validating: bool,
    is_validated: bool,
    is_failed: bool,
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
        delete_url: format!("/ui/projects/{}/delete", project.project_id),
        create_environment_url: format!("/ui/projects/{}/environments/new", project.project_id),
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

fn project_draft_view(draft: &crate::db::ProjectDraftRecord) -> ProjectDraftView {
    let validation_sse_url = draft
        .validation_invocation_id
        .map(|id| format!("/v1/invocations/{id}/events"))
        .unwrap_or_default();
    ProjectDraftView {
        id: draft.id.to_string(),
        git_repo_url: draft.git_repo_url.clone(),
        project_root: draft.project_root.clone(),
        status_class: status_badge_class(&draft.status),
        status: draft.status.clone(),
        project_name: draft.project_name.clone().unwrap_or_default(),
        default_branch: draft.default_branch.clone().unwrap_or_default(),
        has_validation_stream: !validation_sse_url.is_empty(),
        validation_sse_url,
        status_url: format!("/ui/project-drafts/{}", draft.id),
    }
}

fn environment_draft_view(
    _project: &ProjectRecord,
    draft: &crate::db::EnvironmentDraftRecord,
) -> Result<EnvironmentDraftView, UiError> {
    let config_pairs = json_object_pairs(&draft.profile_config);
    let decrypted_secrets =
        crate::profile::decrypt_json(&draft.profile_secrets).map_err(UiError)?;
    let secret_pairs = json_object_pairs(&decrypted_secrets);
    let branch_options = draft
        .branch_options
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|value| {
            value.as_str().map(|name| EnvironmentDraftBranchOptionView {
                name: name.to_string(),
                selected: draft.git_branch.as_deref() == Some(name),
            })
        })
        .collect::<Vec<_>>();
    let commit_options = draft
        .commit_options
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|value| EnvironmentDraftCommitOptionView {
            sha: value
                .get("sha")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            short_sha: value
                .get("short_sha")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            summary: value
                .get("summary")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            committed_at: value
                .get("committed_at")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            selected: value
                .get("sha")
                .and_then(Value::as_str)
                == draft.git_commit_sha.as_deref(),
        })
        .collect::<Vec<_>>();
    let commit_options = if commit_options.is_empty() && !draft.git_commit_sha.clone().unwrap_or_default().is_empty() {
        let sha = draft.git_commit_sha.clone().unwrap_or_default();
        vec![EnvironmentDraftCommitOptionView {
            sha: sha.clone(),
            short_sha: sha.chars().take(8).collect(),
            summary: "Resolved latest commit".to_string(),
            committed_at: String::new(),
            selected: true,
        }]
    } else {
        commit_options
    };
    Ok(EnvironmentDraftView {
        status: draft.status.clone(),
        slug: draft.slug.clone(),
        git_branch: draft.git_branch.clone().unwrap_or_default(),
        git_commit_sha: draft.git_commit_sha.clone().unwrap_or_default(),
        latest_commit_sha: draft.git_commit_sha.clone().unwrap_or_default(),
        use_latest_commit: draft.use_latest_commit,
        auto_deploy: draft.auto_deploy,
        immutable: draft.immutable,
        adapter_type: draft.adapter_type.clone().unwrap_or_else(|| "postgres".to_string()),
        schema_name: draft.schema_name.clone().unwrap_or_default(),
        threads: draft.threads.map(|v| v.to_string()).unwrap_or_default(),
        branch_options,
        commit_options,
        status_url: format!("/ui/environment-drafts/{}", draft.id),
        branch_refresh_url: format!("/ui/environment-drafts/{}/branch", draft.id),
        validate_url: format!("/ui/environment-drafts/{}/validate", draft.id),
        confirm_url: format!("/ui/environment-drafts/{}/confirm", draft.id),
        validation_error: draft.validation_error.clone().unwrap_or_default(),
        validation_sse_url: draft
            .validation_invocation_id
            .map(|id| format!("/v1/invocations/{id}/events"))
            .unwrap_or_default(),
        profile_config_pairs_json: serde_json::to_string(&config_pairs)
            .map_err(AppError::from)
            .map_err(UiError::from)?,
        profile_secret_pairs_json: serde_json::to_string(&secret_pairs)
            .map_err(AppError::from)
            .map_err(UiError::from)?,
        is_loading: draft.status == "loading_git",
        is_validating: draft.status == "validating",
        is_validated: draft.status == "validated",
        is_failed: draft.status == "failed",
    })
}

fn json_object_pairs(value: &Value) -> Vec<EnvironmentDraftPairView> {
    value
        .as_object()
        .map(|object| {
            object
                .iter()
                .map(|(key, value)| EnvironmentDraftPairView {
                    key: key.clone(),
                    value: value
                        .as_str()
                        .map(ToString::to_string)
                        .unwrap_or_else(|| value.to_string()),
                })
                .collect()
        })
        .unwrap_or_default()
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
#[template(path = "projects/_create_modal.html")]
struct ProjectCreateModalTemplate {
    draft: ProjectDraftForm,
    error: Option<String>,
}

#[derive(Template)]
#[template(path = "projects/_delete_modal.html")]
struct ProjectDeleteModalTemplate {
    project: ProjectSummaryView,
    error: Option<String>,
}

#[derive(Template)]
#[template(path = "environments/_create_modal.html")]
struct EnvironmentCreateModalTemplate {
    project: ProjectSummaryView,
    draft: EnvironmentDraftView,
}

#[derive(Template)]
#[template(path = "projects/_draft_pending.html")]
struct ProjectDraftPendingTemplate {
    draft: ProjectDraftView,
    should_poll: bool,
}

#[derive(Template)]
#[template(path = "projects/_draft_failed.html")]
struct ProjectDraftFailedTemplate {
    error: String,
}

#[derive(Template)]
#[template(path = "projects/_draft_validated.html")]
struct ProjectDraftValidatedTemplate {
    draft: ProjectDraftView,
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
    initial_log_sequence: u64,
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn draft(
        status: &str,
        validation_invocation_id: Option<Uuid>,
    ) -> crate::db::ProjectDraftRecord {
        crate::db::ProjectDraftRecord {
            id: Uuid::new_v4(),
            git_repo_url: "git@github.com:org/repo.git".to_string(),
            project_root: "analytics/jaffle_shop".to_string(),
            status: status.to_string(),
            validation_error: None,
            project_name: Some("jaffle_shop".to_string()),
            default_branch: Some("main".to_string()),
            validation_invocation_id,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            validated_at: None,
        }
    }

    #[test]
    fn pending_draft_fragment_includes_polling_and_sse_progress() {
        let draft = draft("validating", Some(Uuid::nil()));
        let rendered = render_project_draft_fragment(&draft, None, true)
            .expect("render pending draft")
            .0;

        assert!(rendered.contains("hx-trigger=\"load delay:2s\""));
        assert!(rendered.contains("/ui/project-drafts/"));
        assert!(rendered.contains("/v1/invocations/00000000-0000-0000-0000-000000000000/events"));
        assert!(rendered.contains("Validation Progress"));
    }

    #[test]
    fn terminal_project_draft_statuses_are_detected() {
        assert!(is_terminal_project_draft_status("validated"));
        assert!(is_terminal_project_draft_status("failed"));
        assert!(!is_terminal_project_draft_status("draft"));
        assert!(!is_terminal_project_draft_status("validating"));
    }

    #[test]
    fn invocation_detail_resumes_stream_after_initial_history() {
        let rendered = InvocationDetailTemplate {
            title: "Invocation",
            invocation: InvocationDetailView {
                invocation_id: Uuid::nil().to_string(),
                status: "running".to_string(),
                status_class: "",
                execution_mode: "server".to_string(),
                worker_queue: "default".to_string(),
                worker_health: "healthy".to_string(),
                cancel_state: "none".to_string(),
                claimed_by: "worker-1".to_string(),
                claimed_at: "2026-03-28T12:00:00Z".to_string(),
                last_heartbeat_at: "2026-03-28T12:00:01Z".to_string(),
                started_at: "2026-03-28T12:00:00Z".to_string(),
                completed_at: "".to_string(),
                error: "".to_string(),
            },
            initial_log_lines: vec!["line 1".to_string()],
            initial_log_sequence: 7,
            sse_url: "/v1/invocations/00000000-0000-0000-0000-000000000000/events".to_string(),
        }
        .render()
        .expect("render invocation detail");

        assert!(rendered.contains(
            "x-data=\"invocationLogs('/v1/invocations/00000000-0000-0000-0000-000000000000/events', 7)\""
        ));
    }
}
