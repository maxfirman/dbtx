use crate::api::{
    InvocationExecutionModeApi, InvocationLifecycleStatus, InvocationListApiRequest,
    InvocationStatusResponse, QueueStatusResponse, WorkerStatusResponse,
};
use crate::db::{EnvironmentRecord, EnvironmentVersionRecord, InvocationListFilters, ProjectRecord};
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
use axum::extract::{Form, Path, Query, RawQuery, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use uuid::Uuid;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(dashboard))
        .route(
            "/ui/dashboard/recent-invocations",
            get(dashboard_recent_invocations),
        )
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
        .route("/ui/invocations/table", get(invocations_table))
        .route("/ui/invocations/{id}", get(invocation_detail))
        .route("/ui/invocations/{id}/panel", get(invocation_detail_panel))
        .route("/ui/invocations/{id}/cancel", post(invocation_cancel))
        .route("/ui/workers", get(workers_index))
        .route("/ui/workers/table", get(workers_table))
        .route("/ui/queues", get(queues_index))
        .route("/ui/queues/table", get(queues_table))
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
    let raw_workers = db.list_workers().await?;
    let workers = filter_workers(
        raw_workers
            .iter()
            .map(worker_summary_view)
            .collect::<Vec<_>>(),
        false,
    );
    let configured_queues = configured_queue_keys(db).await?;
    let (non_stale_worker_queues, stale_worker_queues) = worker_queue_health_sets(&raw_workers);
    let queues = filter_queues(
        db.list_queues()
            .await?
            .iter()
            .map(queue_summary_view)
            .collect::<Vec<_>>(),
        false,
        &configured_queues,
        &non_stale_worker_queues,
        &stale_worker_queues,
    );

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
        workers,
        queues,
    };
    render_template(&page)
}

async fn dashboard_recent_invocations(State(state): State<AppState>) -> Result<Html<String>, UiError> {
    let db = state.db();
    db.require_current_schema().await?;
    let invocations = db
        .list_invocations(InvocationListApiRequest {
            limit: Some(10),
            ..Default::default()
        })
        .await?;
    render_template(&InvocationTableTemplate {
        invocations: invocations.iter().map(invocation_summary_view).collect(),
    })
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

fn deserialize_multi_value_form_field<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany {
        One(String),
        Many(Vec<String>),
    }

    match Option::<OneOrMany>::deserialize(deserializer)? {
        None => Ok(Vec::new()),
        Some(OneOrMany::One(value)) => Ok(vec![value]),
        Some(OneOrMany::Many(values)) => Ok(values),
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
            should_poll: should_poll && draft.validation_invocation_id.is_none(),
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
    #[serde(default, deserialize_with = "deserialize_multi_value_form_field")]
    status: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_multi_value_form_field")]
    execution_mode: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_multi_value_form_field")]
    worker_queue: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_multi_value_form_field")]
    claimed_by: Vec<String>,
    page: Option<usize>,
}

const INVOCATIONS_PAGE_SIZE: usize = 50;

#[derive(Debug, Clone)]
struct NormalizedInvocationFilters {
    display_statuses: Vec<String>,
    execution_modes: Vec<String>,
    worker_queues: Vec<String>,
    claimed_bys: Vec<String>,
}

#[derive(Debug, Clone)]
struct InvocationFilterOptions {
    worker_queues: Vec<String>,
    claimed_bys: Vec<String>,
}

fn normalize_filter_values(values: &[String]) -> Vec<String> {
    let mut normalized = values
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    normalized.sort();
    normalized.dedup();
    normalized
}

fn parse_display_status_filters(values: &[String]) -> AppResult<Vec<String>> {
    let mut display_statuses = Vec::new();
    for value in normalize_filter_values(values) {
        match value.as_str() {
            "queued" | "running" | "cancelling" | "succeeded" | "failed" | "canceled" => {
                display_statuses.push(value)
            }
            other => {
                return Err(AppError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("invalid invocation status filter: {other}"),
                )));
            }
        }
    }
    Ok(display_statuses)
}

fn parse_execution_mode_filters(values: &[String]) -> AppResult<Vec<String>> {
    normalize_filter_values(values)
        .into_iter()
        .map(|value| match value.as_str() {
            "local" | "server" => Ok(value),
            other => Err(AppError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("invalid invocation execution mode filter: {other}"),
            ))),
        })
        .collect()
}

fn normalized_invocation_filters(query: &InvocationFilterQuery) -> AppResult<NormalizedInvocationFilters> {
    let display_statuses = parse_display_status_filters(&query.status)?;
    let execution_modes = parse_execution_mode_filters(&query.execution_mode)?;
    Ok(NormalizedInvocationFilters {
        display_statuses,
        execution_modes,
        worker_queues: normalize_filter_values(&query.worker_queue),
        claimed_bys: normalize_filter_values(&query.claimed_by),
    })
}

fn parse_page_number(value: Option<usize>) -> AppResult<usize> {
    match value {
        None => Ok(1),
        Some(0) => Err(AppError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "invalid invocation page: must be >= 1",
        ))),
        Some(page) => Ok(page),
    }
}

fn parse_invocation_filter_query(raw_query: Option<&str>) -> AppResult<InvocationFilterQuery> {
    let mut query = InvocationFilterQuery::default();
    let Some(raw_query) = raw_query else {
        return Ok(query);
    };
    let url = reqwest::Url::parse(&format!("http://localhost/ui/invocations?{raw_query}"))
        .map_err(|err| AppError::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, err)))?;
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "status" => query.status.push(value.into_owned()),
            "execution_mode" => query.execution_mode.push(value.into_owned()),
            "worker_queue" => query.worker_queue.push(value.into_owned()),
            "claimed_by" => query.claimed_by.push(value.into_owned()),
            "page" => {
                let page = value.parse::<usize>().map_err(|err| {
                    AppError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("invalid invocation page filter: {err}"),
                    ))
                })?;
                query.page = Some(page);
            }
            _ => {}
        }
    }
    Ok(query)
}

fn invocations_page_url(query: &InvocationFilterQuery, page: usize) -> String {
    let mut url = reqwest::Url::parse("http://localhost/ui/invocations").expect("valid invocations url");
    {
        let mut pairs = url.query_pairs_mut();
        for value in normalize_filter_values(&query.status) {
            pairs.append_pair("status", &value);
        }
        for value in normalize_filter_values(&query.execution_mode) {
            pairs.append_pair("execution_mode", &value);
        }
        for value in normalize_filter_values(&query.worker_queue) {
            pairs.append_pair("worker_queue", &value);
        }
        for value in normalize_filter_values(&query.claimed_by) {
            pairs.append_pair("claimed_by", &value);
        }
        if page > 1 {
            pairs.append_pair("page", &page.to_string());
        }
    }
    match url.query() {
        Some(query) => format!("{}?{query}", url.path()),
        None => url.path().to_string(),
    }
}

fn invocations_table_url(query: &InvocationFilterQuery, page: usize) -> String {
    invocations_page_url(query, page).replacen("/ui/invocations", "/ui/invocations/table", 1)
}

fn invocation_filter_option_views(
    selected: &[String],
    options: &[(&str, &str)],
) -> Vec<SelectOptionView> {
    options
        .iter()
        .map(|(value, label)| SelectOptionView {
            value: (*value).to_string(),
            label: (*label).to_string(),
            selected: selected.iter().any(|selected| selected == value),
        })
        .collect()
}

fn invocation_dynamic_option_views(selected: &[String], options: &[String]) -> Vec<SelectOptionView> {
    options
        .iter()
        .map(|value| SelectOptionView {
            value: value.clone(),
            label: value.clone(),
            selected: selected.iter().any(|selected| selected == value),
        })
        .collect()
}

async fn invocation_filter_options(db: &crate::db::Db) -> Result<InvocationFilterOptions, UiError> {
    let worker_queues = db
        .list_queues()
        .await?
        .into_iter()
        .map(|queue| queue.worker_queue)
        .collect::<Vec<_>>();
    let claimed_bys = db.list_worker_filter_options().await?;
    Ok(InvocationFilterOptions {
        worker_queues,
        claimed_bys,
    })
}

fn invocation_rows_summary(current_page: usize, total_count: i64, row_count: usize) -> String {
    if total_count == 0 {
        return "No invocations found".to_string();
    }
    let start = ((current_page - 1) * INVOCATIONS_PAGE_SIZE) + 1;
    let end = start + row_count.saturating_sub(1);
    format!("Showing {start}-{end} of {total_count}")
}

fn invocation_page_window(current_page: usize, total_pages: usize) -> std::ops::RangeInclusive<usize> {
    if total_pages <= 5 {
        return 1..=total_pages.max(1);
    }
    let start = current_page.saturating_sub(2).max(1);
    let end = (start + 4).min(total_pages);
    let adjusted_start = end.saturating_sub(4).max(1);
    adjusted_start..=end
}

async fn invocations_index(
    State(state): State<AppState>,
    RawQuery(raw_query): RawQuery,
) -> Result<Html<String>, UiError> {
    let query = parse_invocation_filter_query(raw_query.as_deref()).map_err(UiError::from)?;
    let db = state.db();
    let (rows, pagination, options) = load_invocation_rows(db, &query).await?;
    render_template(&InvocationsPageTemplate {
        title: "Invocations",
        filters: InvocationFilterView {
            status: invocation_filter_option_views(
                &normalize_filter_values(&query.status),
                &[
                    ("queued", "Queued"),
                    ("running", "Running"),
                    ("cancelling", "Cancelling"),
                    ("succeeded", "Succeeded"),
                    ("failed", "Failed"),
                    ("canceled", "Canceled"),
                ],
            ),
            execution_mode: invocation_filter_option_views(
                &normalize_filter_values(&query.execution_mode),
                &[("local", "Local"), ("server", "Server")],
            ),
            worker_queue: invocation_dynamic_option_views(
                &normalize_filter_values(&query.worker_queue),
                &options.worker_queues,
            ),
            claimed_by: invocation_dynamic_option_views(
                &normalize_filter_values(&query.claimed_by),
                &options.claimed_bys,
            ),
        },
        invocations: rows.clone(),
        results: InvocationResultsView {
            table_url: invocations_table_url(&query, pagination.current_page),
            summary: invocation_rows_summary(pagination.current_page, pagination.total_count, rows.len()),
            pagination,
        },
    })
}

async fn invocations_table(
    State(state): State<AppState>,
    RawQuery(raw_query): RawQuery,
) -> Result<Html<String>, UiError> {
    let query = parse_invocation_filter_query(raw_query.as_deref()).map_err(UiError::from)?;
    let (rows, pagination, _) = load_invocation_rows(state.db(), &query).await?;
    render_template(&InvocationResultsTemplate {
        invocations: rows.clone(),
        results: InvocationResultsView {
            table_url: invocations_table_url(&query, pagination.current_page),
            summary: invocation_rows_summary(pagination.current_page, pagination.total_count, rows.len()),
            pagination,
        },
    })
}

async fn load_invocation_rows(
    db: &crate::db::Db,
    query: &InvocationFilterQuery,
) -> Result<(Vec<InvocationSummaryView>, InvocationPaginationView, InvocationFilterOptions), UiError> {
    db.require_current_schema().await?;
    let normalized = normalized_invocation_filters(query).map_err(UiError::from)?;
    let requested_page = parse_page_number(query.page).map_err(UiError::from)?;
    let total_count = db
        .count_invocations_filtered(InvocationListFilters {
            display_statuses: &normalized.display_statuses,
            execution_modes: &normalized.execution_modes,
            worker_queues: &normalized.worker_queues,
            claimed_bys: &normalized.claimed_bys,
        })
        .await?;
    let total_pages = ((total_count.max(1) as usize - 1) / INVOCATIONS_PAGE_SIZE) + 1;
    let current_page = requested_page.min(total_pages.max(1));
    let invocations = db
        .list_invocations_filtered(
            InvocationListFilters {
                display_statuses: &normalized.display_statuses,
                execution_modes: &normalized.execution_modes,
                worker_queues: &normalized.worker_queues,
                claimed_bys: &normalized.claimed_bys,
            },
            INVOCATIONS_PAGE_SIZE as i64,
            ((current_page - 1) * INVOCATIONS_PAGE_SIZE) as i64,
        )
        .await?;
    let rows = invocations
        .iter()
        .map(invocation_summary_view)
        .collect::<Vec<_>>();
    let options = invocation_filter_options(db).await?;
    let page_links = invocation_page_window(current_page, total_pages)
        .map(|page| PaginationLinkView {
            label: page.to_string(),
            page_url: invocations_page_url(query, page),
            current: page == current_page,
        })
        .collect::<Vec<_>>();
    Ok((
        rows,
        InvocationPaginationView {
            current_page,
            total_pages,
            total_count,
            previous_page_url: (current_page > 1)
                .then(|| invocations_page_url(query, current_page - 1)),
            next_page_url: (current_page < total_pages)
                .then(|| invocations_page_url(query, current_page + 1)),
            page_links,
        },
        options,
    ))
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
        .filter_map(|(_, event)| render_invocation_log_html(&event))
        .collect();

    render_template(&InvocationDetailTemplate {
        title: "Invocation",
        invocation: invocation_detail_view(&invocation),
        initial_log_lines: lines,
        initial_log_sequence,
        sse_url: format!("/v1/invocations/{invocation_id}/events"),
        panel_url: format!("/ui/invocations/{invocation_id}/panel"),
    })
}

async fn invocation_detail_panel(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Html<String>, UiError> {
    let db = state.db();
    db.require_current_schema().await?;
    let invocation_id = parse_uuid(&id)?;
    let invocation = db.get_invocation_status(invocation_id).await?;
    render_template(&InvocationDetailPanelTemplate {
        invocation: invocation_detail_view(&invocation),
        panel_url: format!("/ui/invocations/{invocation_id}/panel"),
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

#[derive(Debug, Default, Deserialize, Clone, Copy)]
struct StaleVisibilityQuery {
    show_stale: Option<bool>,
}

fn show_stale_enabled(query: &StaleVisibilityQuery) -> bool {
    query.show_stale.unwrap_or(false)
}

fn workers_page_url(show_stale: bool) -> &'static str {
    if show_stale {
        "/ui/workers?show_stale=true"
    } else {
        "/ui/workers"
    }
}

fn workers_table_url(show_stale: bool) -> &'static str {
    if show_stale {
        "/ui/workers/table?show_stale=true"
    } else {
        "/ui/workers/table"
    }
}

fn queues_page_url(show_stale: bool) -> &'static str {
    if show_stale {
        "/ui/queues?show_stale=true"
    } else {
        "/ui/queues"
    }
}

fn queues_table_url(show_stale: bool) -> &'static str {
    if show_stale {
        "/ui/queues/table?show_stale=true"
    } else {
        "/ui/queues/table"
    }
}

fn filter_workers(workers: Vec<WorkerSummaryView>, show_stale: bool) -> Vec<WorkerSummaryView> {
    if show_stale {
        workers
    } else {
        workers
            .into_iter()
            .filter(|worker| worker.health != "stale")
            .collect()
    }
}

fn queue_is_stale_only(queue: &QueueSummaryView) -> bool {
    queue.pending_count == 0 && queue.claimed_count > 0 && queue.claimed_count == queue.stale_claim_count
}

fn queue_key(execution_mode: &str, worker_queue: &str) -> String {
    format!("{execution_mode}:{worker_queue}")
}

fn worker_queue_health_sets(
    workers: &[WorkerStatusResponse],
) -> (HashSet<String>, HashSet<String>) {
    let mut non_stale = HashSet::new();
    let mut stale = HashSet::new();
    for worker in workers {
        let mode = invocation_mode_value(&worker.execution_mode);
        for worker_queue in &worker.worker_queues {
            let key = queue_key(mode, worker_queue);
            if matches!(worker.health, crate::api::InvocationWorkerHealthApi::Stale) {
                stale.insert(key);
            } else {
                non_stale.insert(key);
            }
        }
    }
    (non_stale, stale)
}

async fn configured_queue_keys(db: &crate::db::Db) -> Result<HashSet<String>, UiError> {
    let projects = db.list_projects().await?;
    let mut keys = HashSet::new();
    for project in projects {
        let execution_mode = if project.mode == "remote" { "server" } else { "local" };
        for environment in db.list_environments(&project.project_id).await? {
            keys.insert(queue_key(execution_mode, &environment.worker_queue));
        }
    }
    Ok(keys)
}

fn filter_queues(
    queues: Vec<QueueSummaryView>,
    show_stale: bool,
    configured_queues: &HashSet<String>,
    non_stale_worker_queues: &HashSet<String>,
    stale_worker_queues: &HashSet<String>,
) -> Vec<QueueSummaryView> {
    if show_stale {
        queues
    } else {
        queues
            .into_iter()
            .filter(|queue| {
                if queue_is_stale_only(queue) {
                    return false;
                }
                let key = queue_key(&queue.execution_mode, &queue.worker_queue);
                let stale_worker_only_zero_work = queue.pending_count == 0
                    && queue.claimed_count == 0
                    && !configured_queues.contains(&key)
                    && !non_stale_worker_queues.contains(&key)
                    && stale_worker_queues.contains(&key);
                !stale_worker_only_zero_work
            })
            .collect()
    }
}

async fn workers_index_inner(
    state: AppState,
    show_stale: bool,
) -> Result<Html<String>, UiError> {
    let db = state.db();
    db.require_current_schema().await?;
    let workers = filter_workers(
        db.list_workers()
            .await?
            .iter()
            .map(worker_summary_view)
            .collect(),
        show_stale,
    );
    render_template(&WorkersTemplate {
        title: "Workers",
        workers,
        show_stale,
        page_url: workers_page_url(show_stale),
        table_url: workers_table_url(show_stale),
    })
}

async fn workers_index(
    State(state): State<AppState>,
    Query(query): Query<StaleVisibilityQuery>,
) -> Result<Html<String>, UiError> {
    workers_index_inner(state, show_stale_enabled(&query)).await
}

async fn workers_table(
    State(state): State<AppState>,
    Query(query): Query<StaleVisibilityQuery>,
) -> Result<Html<String>, UiError> {
    let db = state.db();
    db.require_current_schema().await?;
    let show_stale = show_stale_enabled(&query);
    let workers = filter_workers(
        db.list_workers()
            .await?
            .iter()
            .map(worker_summary_view)
            .collect(),
        show_stale,
    );
    render_template(&WorkersTableTemplate {
        workers,
        show_stale,
        table_url: workers_table_url(show_stale),
    })
}

async fn queues_index_inner(
    state: AppState,
    show_stale: bool,
) -> Result<Html<String>, UiError> {
    let db = state.db();
    db.require_current_schema().await?;
    let raw_workers = db.list_workers().await?;
    let configured_queues = configured_queue_keys(db).await?;
    let (non_stale_worker_queues, stale_worker_queues) = worker_queue_health_sets(&raw_workers);
    let queues = filter_queues(
        db.list_queues()
            .await?
            .iter()
            .map(queue_summary_view)
            .collect(),
        show_stale,
        &configured_queues,
        &non_stale_worker_queues,
        &stale_worker_queues,
    );
    render_template(&QueuesTemplate {
        title: "Queues",
        queues,
        show_stale,
        page_url: queues_page_url(show_stale),
        table_url: queues_table_url(show_stale),
    })
}

async fn queues_index(
    State(state): State<AppState>,
    Query(query): Query<StaleVisibilityQuery>,
) -> Result<Html<String>, UiError> {
    queues_index_inner(state, show_stale_enabled(&query)).await
}

async fn queues_table(
    State(state): State<AppState>,
    Query(query): Query<StaleVisibilityQuery>,
) -> Result<Html<String>, UiError> {
    let db = state.db();
    db.require_current_schema().await?;
    let show_stale = show_stale_enabled(&query);
    let raw_workers = db.list_workers().await?;
    let configured_queues = configured_queue_keys(db).await?;
    let (non_stale_worker_queues, stale_worker_queues) = worker_queue_health_sets(&raw_workers);
    let queues = filter_queues(
        db.list_queues()
            .await?
            .iter()
            .map(queue_summary_view)
            .collect(),
        show_stale,
        &configured_queues,
        &non_stale_worker_queues,
        &stale_worker_queues,
    );
    render_template(&QueuesTableTemplate {
        queues,
        show_stale,
        table_url: queues_table_url(show_stale),
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

fn escape_html(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '&' => output.push_str("&amp;"),
            '<' => output.push_str("&lt;"),
            '>' => output.push_str("&gt;"),
            '"' => output.push_str("&quot;"),
            '\'' => output.push_str("&#39;"),
            _ => output.push(ch),
        }
    }
    output
}

fn consume_identifier(bytes: &[u8], start: usize) -> usize {
    let mut index = start;
    while index < bytes.len() {
        let byte = bytes[index];
        let is_ident = byte.is_ascii_alphanumeric() || byte == b'_';
        if !is_ident {
            break;
        }
        index += 1;
    }
    index
}

fn style_relation_tokens(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut output = String::new();
    let mut index = 0;
    while index < bytes.len() {
        let start = index;
        let left_end = consume_identifier(bytes, index);
        if left_end > start
            && left_end < bytes.len()
            && bytes[left_end] == b'.'
        {
            let right_start = left_end + 1;
            let right_end = consume_identifier(bytes, right_start);
            if right_end > right_start {
                output.push_str("<span class=\"font-semibold text-cyan-700\">");
                output.push_str(&escape_html(&input[start..left_end]));
                output.push_str(".</span><span class=\"font-semibold text-blue-700\">");
                output.push_str(&escape_html(&input[right_start..right_end]));
                output.push_str("</span>");
                index = right_end;
                continue;
            }
        }
        let ch = input[index..].chars().next().expect("char boundary");
        output.push_str(&escape_html(&ch.to_string()));
        index += ch.len_utf8();
    }
    output
}

fn style_bracket_segments(input: &str) -> String {
    let mut output = String::new();
    let mut remaining = input;
    while let Some(start) = remaining.find('[') {
        let (before, tail) = remaining.split_at(start);
        output.push_str(&style_relation_tokens(before));
        if let Some(end) = tail.find(']') {
            let (segment, rest) = tail.split_at(end + 1);
            output.push_str("<span class=\"text-slate-400\">");
            output.push_str(&escape_html(segment));
            output.push_str("</span>");
            remaining = rest;
        } else {
            output.push_str(&style_relation_tokens(tail));
            remaining = "";
        }
    }
    output.push_str(&style_relation_tokens(remaining));
    output
}

fn render_cli_like_log_html(text: &str) -> String {
    let text = strip_ansi(text);
    let trimmed = text.trim_end();
    if trimmed.is_empty() {
        return String::new();
    }

    if let Some(rest) = trimmed.strip_prefix("dbt-fusion ") {
        return format!(
            "<span class=\"font-semibold text-emerald-700\">dbt-fusion</span> {}",
            style_bracket_segments(rest)
        );
    }

    for (prefix, class_name) in [
        ("Succeeded", "font-semibold text-emerald-700"),
        ("Failed", "font-semibold text-rose-700"),
        ("Warned", "font-semibold text-amber-700"),
        ("Skipped", "font-semibold text-amber-700"),
        ("PASS", "font-semibold text-emerald-700"),
        ("ERROR", "font-semibold text-rose-700"),
    ] {
        if let Some(rest) = trimmed.strip_prefix(prefix) {
            return format!(
                "<span class=\"{class_name}\">{}</span>{}",
                escape_html(prefix),
                style_bracket_segments(rest)
            );
        }
    }

    style_bracket_segments(trimmed)
}

fn render_invocation_log_html(event: &crate::api::InvocationEvent) -> Option<String> {
    let text = event.text.as_deref()?;
    if text.trim().is_empty() {
        return None;
    }
    let rendered = match event.event_type.as_str() {
        "dbt.log" => render_cli_like_log_html(text),
        _ => escape_html(&strip_ansi(text)),
    };
    if rendered.is_empty() {
        None
    } else {
        Some(rendered)
    }
}

fn fmt_ts(value: DateTime<Utc>) -> String {
    value.format("%Y-%m-%d %H:%M:%S UTC").to_string()
}

fn fmt_optional_ts(value: Option<DateTime<Utc>>) -> String {
    value.map(fmt_ts).unwrap_or_default()
}

fn invocation_display_status(invocation: &InvocationStatusResponse) -> &'static str {
    match invocation.status {
        InvocationLifecycleStatus::Running if invocation.claimed_by.is_none() => "queued",
        InvocationLifecycleStatus::Running
            if !matches!(invocation.cancel_state, crate::api::InvocationCancelStateApi::None) =>
        {
            "cancelling"
        }
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

fn status_badge_class(status: &str) -> &'static str {
    match status {
        "queued" => "bg-amber-100 text-amber-800",
        "running" => "bg-sky-100 text-sky-800",
        "cancelling" => "bg-orange-100 text-orange-800",
        "succeeded" => "bg-emerald-100 text-emerald-800",
        "failed" => "bg-rose-100 text-rose-800",
        "canceled" => "bg-slate-200 text-slate-700",
        "claimed" => "bg-sky-100 text-sky-800",
        "idle" => "bg-slate-100 text-slate-700",
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
    is_terminal: bool,
    can_cancel: bool,
    execution_mode: String,
    worker_queue: String,
    worker_health: String,
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
    worker_queues: String,
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
struct SelectOptionView {
    value: String,
    label: String,
    selected: bool,
}

#[derive(Clone)]
struct PaginationLinkView {
    label: String,
    page_url: String,
    current: bool,
}

#[derive(Clone)]
struct InvocationPaginationView {
    current_page: usize,
    total_pages: usize,
    total_count: i64,
    previous_page_url: Option<String>,
    next_page_url: Option<String>,
    page_links: Vec<PaginationLinkView>,
}

#[derive(Clone)]
struct InvocationResultsView {
    table_url: String,
    summary: String,
    pagination: InvocationPaginationView,
}

#[derive(Clone)]
struct InvocationFilterView {
    status: Vec<SelectOptionView>,
    execution_mode: Vec<SelectOptionView>,
    worker_queue: Vec<SelectOptionView>,
    claimed_by: Vec<SelectOptionView>,
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
    let status = invocation_display_status(invocation).to_string();
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
    let status = invocation_display_status(invocation).to_string();
    InvocationDetailView {
        invocation_id: invocation.invocation_id.to_string(),
        status_class: status_badge_class(&status),
        status,
        is_terminal: !matches!(invocation.status, InvocationLifecycleStatus::Running),
        can_cancel: matches!(invocation.status, InvocationLifecycleStatus::Running),
        execution_mode: invocation_mode_value(&invocation.execution_mode).to_string(),
        worker_queue: invocation.worker_queue.clone(),
        worker_health: format!("{:?}", invocation.worker_health).to_lowercase(),
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
        worker_queues: worker.worker_queues.join(", "),
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
    results: InvocationResultsView,
}

#[derive(Template)]
#[template(path = "invocations/_results.html")]
struct InvocationResultsTemplate {
    invocations: Vec<InvocationSummaryView>,
    results: InvocationResultsView,
}

#[derive(Template)]
#[template(path = "invocations/_table.html")]
struct InvocationTableTemplate {
    invocations: Vec<InvocationSummaryView>,
}

#[derive(Template)]
#[template(path = "invocations/_detail_panel.html")]
struct InvocationDetailPanelTemplate {
    invocation: InvocationDetailView,
    panel_url: String,
}

#[derive(Template)]
#[template(path = "invocations/show.html")]
struct InvocationDetailTemplate {
    title: &'static str,
    invocation: InvocationDetailView,
    initial_log_lines: Vec<String>,
    initial_log_sequence: u64,
    sse_url: String,
    panel_url: String,
}

#[derive(Template)]
#[template(path = "workers/index.html")]
struct WorkersTemplate {
    title: &'static str,
    workers: Vec<WorkerSummaryView>,
    show_stale: bool,
    page_url: &'static str,
    table_url: &'static str,
}

#[derive(Template)]
#[template(path = "workers/_table.html")]
struct WorkersTableTemplate {
    workers: Vec<WorkerSummaryView>,
    show_stale: bool,
    table_url: &'static str,
}

#[derive(Template)]
#[template(path = "queues/index.html")]
struct QueuesTemplate {
    title: &'static str,
    queues: Vec<QueueSummaryView>,
    show_stale: bool,
    page_url: &'static str,
    table_url: &'static str,
}

#[derive(Template)]
#[template(path = "queues/_table.html")]
struct QueuesTableTemplate {
    queues: Vec<QueueSummaryView>,
    show_stale: bool,
    table_url: &'static str,
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
    use crate::api::InvocationCancelStateApi;
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
    fn pending_draft_fragment_prefers_sse_over_polling() {
        let draft = draft("validating", Some(Uuid::nil()));
        let rendered = render_project_draft_fragment(&draft, None, true)
            .expect("render pending draft")
            .0;

        assert!(!rendered.contains("hx-trigger=\"load delay:2s\""));
        assert!(rendered.contains("/ui/project-drafts/"));
        assert!(rendered.contains("/v1/invocations/00000000-0000-0000-0000-000000000000/events"));
        assert!(rendered.contains("Validation Progress"));
    }

    #[test]
    fn pending_draft_fragment_polls_when_no_sse_stream_is_available() {
        let draft = draft("validating", None);
        let rendered = render_project_draft_fragment(&draft, None, true)
            .expect("render pending draft")
            .0;

        assert!(rendered.contains("hx-trigger=\"load delay:2s\""));
        assert!(rendered.contains("/ui/project-drafts/"));
    }

    #[test]
    fn terminal_project_draft_statuses_are_detected() {
        assert!(is_terminal_project_draft_status("validated"));
        assert!(is_terminal_project_draft_status("failed"));
        assert!(!is_terminal_project_draft_status("draft"));
        assert!(!is_terminal_project_draft_status("validating"));
    }

    #[test]
    fn running_unclaimed_invocations_render_as_queued() {
        let invocation = InvocationStatusResponse {
            invocation_id: Uuid::nil(),
            status: InvocationLifecycleStatus::Running,
            execution_mode: InvocationExecutionModeApi::Server,
            worker_queue: "generic".to_string(),
            worker_health: crate::api::InvocationWorkerHealthApi::Unclaimed,
            cancel_state: InvocationCancelStateApi::None,
            claimed_at: None,
            claimed_by: None,
            last_heartbeat_at: None,
            cancel_requested_at: None,
            started_at: Utc::now(),
            completed_at: None,
            cancel_requested: false,
            exit_code: None,
            error: None,
        };

        let view = invocation_summary_view(&invocation);
        assert_eq!(view.status, "queued");
        assert_eq!(view.status_class, "bg-amber-100 text-amber-800");
    }

    #[test]
    fn running_claimed_invocations_render_as_running() {
        let invocation = InvocationStatusResponse {
            invocation_id: Uuid::nil(),
            status: InvocationLifecycleStatus::Running,
            execution_mode: InvocationExecutionModeApi::Server,
            worker_queue: "generic".to_string(),
            worker_health: crate::api::InvocationWorkerHealthApi::Claimed,
            cancel_state: InvocationCancelStateApi::None,
            claimed_at: Some(Utc::now()),
            claimed_by: Some("worker-1".to_string()),
            last_heartbeat_at: Some(Utc::now()),
            cancel_requested_at: None,
            started_at: Utc::now(),
            completed_at: None,
            cancel_requested: false,
            exit_code: None,
            error: None,
        };

        let view = invocation_detail_view(&invocation);
        assert_eq!(view.status, "running");
        assert_eq!(view.status_class, "bg-sky-100 text-sky-800");
        assert!(!view.is_terminal);
        assert!(view.can_cancel);
    }

    #[test]
    fn running_cancel_requested_invocations_render_as_cancelling() {
        let invocation = InvocationStatusResponse {
            invocation_id: Uuid::nil(),
            status: InvocationLifecycleStatus::Running,
            execution_mode: InvocationExecutionModeApi::Server,
            worker_queue: "generic".to_string(),
            worker_health: crate::api::InvocationWorkerHealthApi::Claimed,
            cancel_state: InvocationCancelStateApi::Requested,
            claimed_at: Some(Utc::now()),
            claimed_by: Some("worker-1".to_string()),
            last_heartbeat_at: Some(Utc::now()),
            cancel_requested_at: Some(Utc::now()),
            started_at: Utc::now(),
            completed_at: None,
            cancel_requested: true,
            exit_code: None,
            error: None,
        };

        let view = invocation_detail_view(&invocation);
        assert_eq!(view.status, "cancelling");
        assert_eq!(view.status_class, "bg-orange-100 text-orange-800");
    }

    #[test]
    fn invocation_detail_panel_keeps_polling_until_terminal() {
        let rendered = InvocationDetailPanelTemplate {
            invocation: InvocationDetailView {
                invocation_id: Uuid::nil().to_string(),
                status: "queued".to_string(),
                status_class: "bg-amber-100 text-amber-800",
                is_terminal: false,
                can_cancel: true,
                execution_mode: "server".to_string(),
                worker_queue: "default".to_string(),
                worker_health: "unknown".to_string(),
                claimed_by: "".to_string(),
                claimed_at: "".to_string(),
                last_heartbeat_at: "".to_string(),
                started_at: "2026-03-28 12:00:00 UTC".to_string(),
                completed_at: "".to_string(),
                error: "".to_string(),
            },
            panel_url: "/ui/invocations/00000000-0000-0000-0000-000000000000/panel".to_string(),
        }
        .render()
        .expect("render invocation detail panel");

        assert!(rendered.contains("hx-get=\"/ui/invocations/00000000-0000-0000-0000-000000000000/panel\""));
        assert!(rendered.contains("hx-trigger=\"every 2s\""));
    }

    #[test]
    fn invocation_detail_resumes_stream_after_initial_history() {
        let rendered = InvocationDetailTemplate {
            title: "Invocation",
            invocation: InvocationDetailView {
                invocation_id: Uuid::nil().to_string(),
                status: "running".to_string(),
                status_class: "",
                is_terminal: false,
                can_cancel: true,
                execution_mode: "server".to_string(),
                worker_queue: "default".to_string(),
                worker_health: "healthy".to_string(),
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
            panel_url: "/ui/invocations/00000000-0000-0000-0000-000000000000/panel".to_string(),
        }
        .render()
        .expect("render invocation detail");

        assert!(rendered.contains(
            "x-data=\"invocationLogs('/v1/invocations/00000000-0000-0000-0000-000000000000/events', 7)\""
        ));
    }

    #[test]
    fn idle_workers_render_with_neutral_badge() {
        let worker = WorkerStatusResponse {
            worker_id: "worker-1".to_string(),
            execution_mode: InvocationExecutionModeApi::Server,
            worker_queues: vec!["generic".to_string(), "validation".to_string()],
            claimed_invocation_count: 0,
            last_heartbeat_at: Some(Utc::now()),
            health: crate::api::InvocationWorkerHealthApi::Idle,
        };

        let view = worker_summary_view(&worker);
        assert_eq!(view.health, "idle");
        assert_eq!(view.health_class, "bg-slate-100 text-slate-700");
        assert_eq!(view.worker_queues, "generic, validation");
    }

    #[test]
    fn workers_table_renders_empty_state() {
        let rendered = WorkersTableTemplate {
            workers: vec![],
            show_stale: false,
            table_url: "/ui/workers/table",
        }
            .render()
            .expect("render workers table");
        assert!(rendered.contains("No active or idle workers."));
        assert!(rendered.contains("hx-get=\"/ui/workers/table\""));
    }

    #[test]
    fn queues_table_renders_empty_state() {
        let rendered = QueuesTableTemplate {
            queues: vec![],
            show_stale: false,
            table_url: "/ui/queues/table",
        }
            .render()
            .expect("render queues table");
        assert!(rendered.contains("No active or idle queues."));
        assert!(rendered.contains("hx-get=\"/ui/queues/table\""));
    }

    #[test]
    fn filter_workers_hides_stale_by_default() {
        let filtered = filter_workers(
            vec![
                WorkerSummaryView {
                    worker_id: "worker-idle".to_string(),
                    execution_mode: "server".to_string(),
                    worker_queues: "generic".to_string(),
                    claimed_invocation_count: 0,
                    last_heartbeat_at: "2026-03-30 12:00:00 UTC".to_string(),
                    health: "idle".to_string(),
                    health_class: "bg-slate-100 text-slate-700",
                },
                WorkerSummaryView {
                    worker_id: "worker-stale".to_string(),
                    execution_mode: "server".to_string(),
                    worker_queues: "generic".to_string(),
                    claimed_invocation_count: 0,
                    last_heartbeat_at: "2026-03-30 11:00:00 UTC".to_string(),
                    health: "stale".to_string(),
                    health_class: "bg-orange-100 text-orange-800",
                },
            ],
            false,
        );
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].worker_id, "worker-idle");
    }

    #[test]
    fn filter_queues_hides_stale_only_rows_by_default() {
        let configured: HashSet<String> = HashSet::new();
        let non_stale: HashSet<String> = HashSet::new();
        let stale: HashSet<String> = HashSet::new();
        let filtered = filter_queues(
            vec![
                QueueSummaryView {
                    worker_queue: "stale-only".to_string(),
                    execution_mode: "server".to_string(),
                    pending_count: 0,
                    claimed_count: 2,
                    stale_claim_count: 2,
                    oldest_pending_at: String::new(),
                },
                QueueSummaryView {
                    worker_queue: "active".to_string(),
                    execution_mode: "server".to_string(),
                    pending_count: 0,
                    claimed_count: 2,
                    stale_claim_count: 1,
                    oldest_pending_at: String::new(),
                },
            ],
            false,
            &configured,
            &non_stale,
            &stale,
        );
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].worker_queue, "active");
    }

    #[test]
    fn filter_queues_hides_stale_worker_only_zero_work_rows_by_default() {
        let non_stale: HashSet<String> = HashSet::new();
        let stale: HashSet<String> = HashSet::from([queue_key("server", "stale-worker-only")]);
        let filtered = filter_queues(
            vec![
                QueueSummaryView {
                    worker_queue: "stale-worker-only".to_string(),
                    execution_mode: "server".to_string(),
                    pending_count: 0,
                    claimed_count: 0,
                    stale_claim_count: 0,
                    oldest_pending_at: String::new(),
                },
                QueueSummaryView {
                    worker_queue: "configured-idle".to_string(),
                    execution_mode: "server".to_string(),
                    pending_count: 0,
                    claimed_count: 0,
                    stale_claim_count: 0,
                    oldest_pending_at: String::new(),
                },
            ],
            false,
            &HashSet::from([queue_key("server", "configured-idle")]),
            &non_stale,
            &stale,
        );
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].worker_queue, "configured-idle");
    }

    #[test]
    fn workers_index_renders_show_stale_toggle_checked_from_query_state() {
        let rendered = WorkersTemplate {
            title: "Workers",
            workers: vec![],
            show_stale: true,
            page_url: "/ui/workers?show_stale=true",
            table_url: "/ui/workers/table?show_stale=true",
        }
        .render()
        .expect("render workers page");
        assert!(rendered.contains("show_stale"));
        assert!(rendered.contains("checked"));
    }

    #[test]
    fn queues_table_preserves_show_stale_polling_url() {
        let rendered = QueuesTableTemplate {
            queues: vec![],
            show_stale: true,
            table_url: "/ui/queues/table?show_stale=true",
        }
        .render()
        .expect("render queues table");
        assert!(rendered.contains("hx-get=\"/ui/queues/table?show_stale=true\""));
    }

    #[test]
    fn invocation_page_url_repeats_multi_select_params() {
        let query = InvocationFilterQuery {
            status: vec!["queued".to_string(), "cancelling".to_string()],
            execution_mode: vec!["server".to_string()],
            worker_queue: vec!["generic".to_string(), "validation".to_string()],
            claimed_by: vec!["worker-a".to_string()],
            page: Some(3),
        };

        let url = invocations_page_url(&query, 3);
        assert!(url.contains("status=queued"));
        assert!(url.contains("status=cancelling"));
        assert!(url.contains("worker_queue=generic"));
        assert!(url.contains("worker_queue=validation"));
        assert!(url.contains("page=3"));
    }

    #[test]
    fn invocation_results_render_pagination_links() {
        let rendered = InvocationResultsTemplate {
            invocations: vec![],
            results: InvocationResultsView {
                table_url: "/ui/invocations/table?status=queued&page=2".to_string(),
                summary: "Showing 51-60 of 120".to_string(),
                pagination: InvocationPaginationView {
                    current_page: 2,
                    total_pages: 3,
                    total_count: 120,
                    previous_page_url: Some("/ui/invocations?status=queued".to_string()),
                    next_page_url: Some("/ui/invocations?status=queued&page=3".to_string()),
                    page_links: vec![
                        PaginationLinkView {
                            label: "1".to_string(),
                            page_url: "/ui/invocations?status=queued".to_string(),
                            current: false,
                        },
                        PaginationLinkView {
                            label: "2".to_string(),
                            page_url: "/ui/invocations?status=queued&page=2".to_string(),
                            current: true,
                        },
                    ],
                },
            },
        }
        .render()
        .expect("render invocation results");

        assert!(rendered.contains("Page 2 of 3"));
        assert!(rendered.contains("Previous"));
        assert!(rendered.contains("Next"));
    }

    #[test]
    fn invocations_filter_form_uses_multi_selects_and_no_limit() {
        let rendered = InvocationsPageTemplate {
            title: "Invocations",
            filters: InvocationFilterView {
                status: invocation_filter_option_views(
                    &["queued".to_string()],
                    &[("queued", "Queued"), ("running", "Running"), ("cancelling", "Cancelling")],
                ),
                execution_mode: invocation_filter_option_views(
                    &["server".to_string()],
                    &[("server", "Server")],
                ),
                worker_queue: invocation_dynamic_option_views(&[], &["generic".to_string()]),
                claimed_by: invocation_dynamic_option_views(&[], &["worker-a".to_string()]),
            },
            invocations: vec![],
            results: InvocationResultsView {
                table_url: "/ui/invocations/table".to_string(),
                summary: "No invocations found".to_string(),
                pagination: InvocationPaginationView {
                    current_page: 1,
                    total_pages: 1,
                    total_count: 0,
                    previous_page_url: None,
                    next_page_url: None,
                    page_links: vec![PaginationLinkView {
                        label: "1".to_string(),
                        page_url: "/ui/invocations".to_string(),
                        current: true,
                    }],
                },
            },
        }
        .render()
        .expect("render invocations page");

        assert!(rendered.contains("x-data=\"multiSelectDropdown('Status')\""));
        assert!(rendered.contains("type=\"checkbox\" name=\"status\""));
        assert!(rendered.contains("type=\"checkbox\" name=\"worker_queue\""));
        assert!(!rendered.contains("Cancel State"));
        assert!(!rendered.contains("Limit"));
        assert!(rendered.contains("hx-get=\"/ui/invocations\""));
    }

    #[test]
    fn parse_invocation_filter_query_accepts_single_and_repeated_values() {
        let parsed = parse_invocation_filter_query(Some(
            "status=canceled&status=failed&worker_queue=generic&page=2",
        ))
        .expect("parse invocation filter query");
        assert_eq!(
            parsed.status,
            vec!["canceled".to_string(), "failed".to_string()]
        );
        assert_eq!(parsed.worker_queue, vec!["generic".to_string()]);
        assert_eq!(parsed.page, Some(2));
    }
}
