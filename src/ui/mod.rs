//! Server-rendered operator UI: HTMX handlers, Askama templates, and view models.
mod read_models;

use crate::api::{
    EnvironmentActiveResourcePhaseApi, InvocationCommandApi, InvocationExecutionModeApi,
    InvocationLifecycleStatus, InvocationListApiRequest, InvocationStatusResponse,
    QueueStatusResponse, WorkerStatusResponse,
};
use crate::db::{
    DraftStatus, EnvironmentActiveResourceRecord, EnvironmentActualStateRecord,
    EnvironmentReconcilePreparationRecord, EnvironmentRecord, EnvironmentRunPlanRecord,
    EnvironmentVersionRecord, InvocationListFilters, NodeExecutionStatus, NodeReconcileState,
    PlanStatus, PreparationStatus, ProjectRecord,
};
use crate::error::{AppError, AppResult};
use crate::invocation_bootstrap::{
    ensure_target_manifest_for_reconcile, start_environment_draft_prepare_invocation,
    start_environment_draft_validation_invocation, start_prepared_invocation,
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
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(dashboard))
        .route(
            "/ui/dashboard/recent-invocations",
            get(dashboard_recent_invocations),
        )
        .route("/ui/dashboard/summary", get(dashboard_summary))
        .route("/ui/dashboard/workers", get(dashboard_workers))
        .route("/ui/dashboard/queues", get(dashboard_queues))
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
        .route("/ui/invocations/{id}/tab", get(invocation_tab))
        .route("/ui/invocations/{id}/panel", get(invocation_detail_panel))
        .route("/ui/invocations/{id}/cancel", post(invocation_cancel))
        .route("/v1/invocations/{id}/timeline", get(invocation_timeline))
        .route("/ui/workers", get(workers_index))
        .route("/ui/workers/table", get(workers_table))
        .route("/ui/queues", get(queues_index))
        .route("/ui/queues/table", get(queues_table))
        .route(
            "/ui/projects/{project_id}/environments/{slug}",
            get(environment_detail),
        )
        .route(
            "/ui/projects/{project_id}/environments/{slug}/panel",
            get(environment_detail_panel),
        )
        .route(
            "/ui/projects/{project_id}/environments/{slug}/release",
            post(environment_release),
        )
        .route(
            "/ui/projects/{project_id}/environments/{slug}/reconcile",
            post(environment_reconcile),
        )
        .route(
            "/ui/projects/{project_id}/environments/{slug}/pause",
            post(ui_environment_pause),
        )
        .route(
            "/ui/projects/{project_id}/environments/{slug}/resume",
            post(ui_environment_resume),
        )
        .route(
            "/ui/projects/{project_id}/environments/{slug}/plans/{plan_id}/admit",
            post(environment_plan_admit),
        )
        .route(
            "/ui/projects/{project_id}/environments/{slug}/rollback",
            post(environment_rollback),
        )
        .route("/ui/catalog", get(models_index))
        .route(
            "/ui/catalog/{project_id}/{env_slug}/{unique_id}",
            get(model_detail),
        )
        .route(
            "/ui/catalog/{project_id}/{env_slug}/{unique_id}/tab",
            get(model_tab),
        )
        .route(
            "/ui/catalog/{project_id}/{env_slug}/{unique_id}/test-history/{test_unique_id}",
            get(model_test_history),
        )
        .route(
            "/ui/catalog/{project_id}/{env_slug}/{unique_id}/history-diff",
            get(model_history_diff),
        )
        .route("/ui/assets/lineage.js", get(lineage_js_asset))
        .route("/ui/assets/lineage.css", get(lineage_css_asset))
        .route("/ui/assets/timeline.js", get(timeline_js_asset))
        .route("/ui/assets/timeline.css", get(timeline_css_asset))
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
        .map_err(|err| UiError(AppError::Internal(err.to_string())))
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
    let summary = load_dashboard_summary(db, projects.len() as i64, workers.len() as i64).await?;

    let page = DashboardTemplate {
        title: "Dashboard",
        summary,
        invocations: invocations.iter().map(invocation_summary_view).collect(),
        projects: projects.iter().map(project_summary_view).collect(),
        workers,
        queues,
    };
    render_template(&page)
}

async fn dashboard_summary(State(state): State<AppState>) -> Result<Html<String>, UiError> {
    let db = state.db();
    db.require_current_schema().await?;
    let project_count = db.list_projects().await?.len() as i64;
    let raw_workers = db.list_workers().await?;
    let workers = filter_workers(
        raw_workers
            .iter()
            .map(worker_summary_view)
            .collect::<Vec<_>>(),
        false,
    );
    render_template(&DashboardSummaryTemplate {
        summary: load_dashboard_summary(db, project_count, workers.len() as i64).await?,
    })
}

async fn load_dashboard_summary(
    db: &crate::db::Db,
    project_count: i64,
    worker_count: i64,
) -> Result<DashboardSummaryView, UiError> {
    let running_filters = vec!["running".to_string()];
    let queued_filters = vec!["queued".to_string()];
    let no_filters: Vec<String> = Vec::new();
    let running_invocation_count = db
        .count_invocations_filtered(InvocationListFilters {
            display_statuses: &running_filters,
            execution_modes: &no_filters,
            worker_queues: &no_filters,
            claimed_bys: &no_filters,
        })
        .await?;
    let queued_invocation_count = db
        .count_invocations_filtered(InvocationListFilters {
            display_statuses: &queued_filters,
            execution_modes: &no_filters,
            worker_queues: &no_filters,
            claimed_bys: &no_filters,
        })
        .await?;
    Ok(DashboardSummaryView {
        project_count,
        running_invocation_count,
        queued_invocation_count,
        worker_count,
    })
}

async fn dashboard_recent_invocations(
    State(state): State<AppState>,
) -> Result<Html<String>, UiError> {
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

async fn dashboard_workers(State(state): State<AppState>) -> Result<Html<String>, UiError> {
    let db = state.db();
    db.require_current_schema().await?;
    let workers = filter_workers(
        db.list_workers()
            .await?
            .iter()
            .map(worker_summary_view)
            .collect(),
        false,
    );
    render_template(&DashboardWorkersTemplate { workers })
}

async fn dashboard_queues(State(state): State<AppState>) -> Result<Html<String>, UiError> {
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
        false,
        &configured_queues,
        &non_stale_worker_queues,
        &stale_worker_queues,
    );
    render_template(&DashboardQueuesTemplate { queues })
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
    auto_reconcile: Option<String>,
    immutable: Option<String>,
    adapter_type: String,
    schema_name: String,
    #[serde(default, deserialize_with = "deserialize_optional_i32_form_field")]
    threads: Option<i32>,
    profile_config_json: Option<String>,
    profile_secrets_json: Option<String>,
}

fn deserialize_optional_i32_form_field<'de, D>(deserializer: D) -> Result<Option<i32>, D::Error>
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
    render_environment_draft_modal(
        state.db(),
        &EnvironmentService::new(state.db())
            .get_draft(draft_id)
            .await?,
    )
    .await
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
    let environment = EnvironmentService::new(state.db())
        .confirm_draft(draft_id)
        .await?;
    let redirect = format!(
        "/ui/projects/{}/environments/{}",
        environment.project_ref, environment.slug
    );
    if is_htmx(&headers) {
        let mut response = Html(String::new()).into_response();
        response.headers_mut().insert(
            "HX-Redirect",
            HeaderValue::from_str(&redirect).unwrap_or_else(|_| HeaderValue::from_static("/")),
        );
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
        !is_terminal_project_draft_status(draft.status),
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

fn environment_draft_update_request(
    form: EnvironmentDraftForm,
) -> Result<EnvironmentDraftUpdateRequest, UiError> {
    let profile_config = parse_json_object(form.profile_config_json.as_deref().unwrap_or("{}"))?;
    let profile_secrets = parse_json_object(form.profile_secrets_json.as_deref().unwrap_or("{}"))?;
    Ok(EnvironmentDraftUpdateRequest {
        project: String::new(),
        slug: form.slug,
        git_branch: if form.git_branch.trim().is_empty() {
            None
        } else {
            Some(form.git_branch)
        },
        git_commit_sha: if form.git_commit_sha.trim().is_empty() {
            None
        } else {
            Some(form.git_commit_sha)
        },
        use_latest_commit: form.use_latest_commit.is_some(),
        auto_reconcile: form.auto_reconcile.is_some(),
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
        Err(UiError(AppError::InvalidProfileConfig(
            "expected object".to_string(),
        )))
    }
}

async fn render_environment_draft_modal(
    db: &crate::db::Db,
    draft: &crate::db::EnvironmentDraftRecord,
) -> Result<Html<String>, UiError> {
    let project = db
        .get_project_by_id(draft.project_id)
        .await
        .map_err(UiError)?;
    render_template(&EnvironmentCreateModalTemplate {
        project: project_summary_view(&project),
        draft: environment_draft_view(&project, draft)?,
    })
}

fn is_terminal_project_draft_status(status: DraftStatus) -> bool {
    status.is_terminal()
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
                return Err(AppError::InvalidInput(format!(
                    "invalid invocation status filter: {other}"
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
            other => Err(AppError::InvalidInput(format!(
                "invalid invocation execution mode filter: {other}"
            ))),
        })
        .collect()
}

fn normalized_invocation_filters(
    query: &InvocationFilterQuery,
) -> AppResult<NormalizedInvocationFilters> {
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
        Some(0) => Err(AppError::InvalidInput(
            "invalid invocation page: must be >= 1".to_string(),
        )),
        Some(page) => Ok(page),
    }
}

fn parse_invocation_filter_query(raw_query: Option<&str>) -> AppResult<InvocationFilterQuery> {
    let mut query = InvocationFilterQuery::default();
    let Some(raw_query) = raw_query else {
        return Ok(query);
    };
    let url = reqwest::Url::parse(&format!("http://localhost/ui/invocations?{raw_query}"))
        .map_err(|err| AppError::InvalidInput(format!("invalid query string: {err}")))?;
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "status" => query.status.push(value.into_owned()),
            "execution_mode" => query.execution_mode.push(value.into_owned()),
            "worker_queue" => query.worker_queue.push(value.into_owned()),
            "claimed_by" => query.claimed_by.push(value.into_owned()),
            "page" => {
                let page = value.parse::<usize>().map_err(|err| {
                    AppError::InvalidInput(format!("invalid invocation page filter: {err}"))
                })?;
                query.page = Some(page);
            }
            _ => {}
        }
    }
    Ok(query)
}

fn invocations_page_url(query: &InvocationFilterQuery, page: usize) -> String {
    let mut url =
        reqwest::Url::parse("http://localhost/ui/invocations").expect("valid invocations url");
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

fn invocation_dynamic_option_views(
    selected: &[String],
    options: &[String],
) -> Vec<SelectOptionView> {
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

fn invocation_page_window(
    current_page: usize,
    total_pages: usize,
) -> std::ops::RangeInclusive<usize> {
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
            summary: invocation_rows_summary(
                pagination.current_page,
                pagination.total_count,
                rows.len(),
            ),
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
            summary: invocation_rows_summary(
                pagination.current_page,
                pagination.total_count,
                rows.len(),
            ),
            pagination,
        },
    })
}

async fn load_invocation_rows(
    db: &crate::db::Db,
    query: &InvocationFilterQuery,
) -> Result<
    (
        Vec<InvocationSummaryView>,
        InvocationPaginationView,
        InvocationFilterOptions,
    ),
    UiError,
> {
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

#[derive(Debug, Default, Deserialize)]
struct InvocationTabQuery {
    tab: Option<String>,
}

async fn invocation_detail(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<InvocationTabQuery>,
) -> Result<Html<String>, UiError> {
    let db = state.db();
    db.require_current_schema().await?;
    let invocation_id = parse_uuid(&id)?;
    let invocation = db.get_invocation_status(invocation_id).await?;

    let tab = query.tab.as_deref().unwrap_or("timeline");
    let tab = if matches!(tab, "timeline" | "lineage" | "logs") {
        tab
    } else {
        "timeline"
    };
    let base = format!("/ui/invocations/{invocation_id}");

    let tab_content_html = render_invocation_tab_content(db, invocation_id, tab).await?;

    let tabs = ["timeline", "lineage", "logs"]
        .iter()
        .map(|&t| InvocationTabView {
            label: match t {
                "timeline" => "Timeline",
                "lineage" => "Lineage",
                "logs" => "Logs",
                _ => t,
            },
            url: format!("{base}?tab={t}"),
            partial_url: format!("{base}/tab?tab={t}"),
            active: t == tab,
        })
        .collect();

    render_template(&InvocationDetailTemplate {
        title: "Invocation",
        invocation: invocation_detail_view(&invocation),
        panel_url: format!("{base}/panel"),
        tabs,
        tab_content_html,
    })
}

async fn invocation_tab(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<InvocationTabQuery>,
) -> Result<Html<String>, UiError> {
    let db = state.db();
    db.require_current_schema().await?;
    let invocation_id = parse_uuid(&id)?;
    let tab = query.tab.as_deref().unwrap_or("timeline");
    let html = render_invocation_tab_content(db, invocation_id, tab).await?;
    Ok(Html(html))
}

async fn render_invocation_tab_content(
    db: &crate::db::Db,
    invocation_id: Uuid,
    tab: &str,
) -> Result<String, UiError> {
    let sse_url = format!("/v1/invocations/{invocation_id}/events");
    match tab {
        "lineage" => {
            let lineage = db.get_invocation_lineage(invocation_id).await?;

            let mut base_url = String::new();
            if let Ok(p) = db
                .get_invocation_persistence(invocation_id, None, None)
                .await
                && let Some(env_id) = p.environment_id
                && let Ok(env) = db.get_environment_by_id(env_id).await
            {
                base_url = format!("/ui/catalog/{}/{}", env.project_ref, env.slug);
            }

            let test_ids: HashSet<&str> = lineage
                .nodes
                .iter()
                .filter(|n| n.resource_type.as_deref() == Some("test"))
                .map(|n| n.unique_id.as_str())
                .collect();
            let mut test_counts: HashMap<&str, (u32, u32)> = HashMap::new();
            for (parent, child) in &lineage.edges {
                if test_ids.contains(child.as_str())
                    && let Some(tn) = lineage.nodes.iter().find(|n| n.unique_id == *child)
                {
                    let e = test_counts.entry(parent.as_str()).or_insert((0, 0));
                    match tn.status.as_deref().and_then(NodeExecutionStatus::parse) {
                        Some(NodeExecutionStatus::Pass | NodeExecutionStatus::Success) => e.0 += 1,
                        Some(NodeExecutionStatus::Fail | NodeExecutionStatus::Error) => e.1 += 1,
                        _ => {}
                    }
                }
            }

            let nodes_json: Vec<Value> = lineage
                .nodes
                .iter()
                .filter(|n| !test_ids.contains(n.unique_id.as_str()))
                .map(|n| {
                    let (pass, fail) = test_counts
                        .get(n.unique_id.as_str())
                        .copied()
                        .unwrap_or((0, 0));
                    serde_json::json!({
                        "id": n.unique_id,
                        "data": {
                            "label": n.name.as_deref().unwrap_or(&n.unique_id),
                            "name": n.name,
                            "resource_type": n.resource_type,
                            "status": n.status,
                            "materialized": n.materialized,
                            "package_name": n.package_name,
                            "testsPassing": pass,
                            "testsFailing": fail,
                        }
                    })
                })
                .collect();

            let edges_json: Vec<Value> = lineage
                .edges
                .iter()
                .filter(|(s, t)| !test_ids.contains(s.as_str()) && !test_ids.contains(t.as_str()))
                .map(|(src, tgt)| {
                    serde_json::json!({
                        "id": format!("{src}->{tgt}"),
                        "source": src,
                        "target": tgt,
                    })
                })
                .collect();

            let lineage_data = serde_json::json!({
                "nodes": nodes_json,
                "edges": edges_json,
                "currentNodeId": "",
                "baseUrl": base_url,
                "depth": 1,
                "direction": "both",
            });

            InvocationLineageTabTemplate {
                lineage_json: lineage_data.to_string(),
            }
            .render()
            .map_err(|e| UiError(AppError::Internal(e.to_string())))
        }
        "logs" => {
            let events = db.load_invocation_events_since(invocation_id, 0).await?;
            let initial_log_sequence = events.last().map(|(seq, _)| *seq).unwrap_or(0);
            let lines = events
                .into_iter()
                .filter_map(|(_, event)| render_invocation_log_html(&event))
                .collect();
            InvocationLogsTabTemplate {
                sse_url,
                initial_log_lines: lines,
                initial_log_sequence,
            }
            .render()
            .map_err(|e| UiError(AppError::Internal(e.to_string())))
        }
        _ => {
            // timeline (default)
            InvocationTimelineTabTemplate {
                sse_url,
                timeline_api_url: format!("/v1/invocations/{invocation_id}/timeline"),
            }
            .render()
            .map_err(|e| UiError(AppError::Internal(e.to_string())))
        }
    }
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

async fn invocation_timeline(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<axum::Json<crate::api::InvocationTimelineResponse>, UiError> {
    let db = state.db();
    let invocation_id = parse_uuid(&id)?;
    let invocation = db.get_invocation_status(invocation_id).await?;
    let rows = db.get_invocation_timeline_resources(invocation_id).await?;

    let mut edges = Vec::new();
    let mut model_base_url: Option<String> = None;
    if let Ok(p) = db
        .get_invocation_persistence(invocation_id, None, None)
        .await
    {
        if let Some(run_id) = p.run_id
            && let Ok(e) = db.load_manifest_edges(run_id).await
        {
            edges = e;
        }
        if let Some(env_id) = p.environment_id
            && let Ok(env) = db.get_environment_by_id(env_id).await
        {
            model_base_url = Some(format!("/ui/catalog/{}/{}", env.project_ref, env.slug));
        }
    }

    let unique_ids: Vec<&str> = rows.iter().map(|r| r.0.as_str()).collect();
    let sorted = topo_sort_resources(&unique_ids, &edges);

    let resource_map: std::collections::HashMap<_, _> =
        rows.iter().map(|r| (r.0.as_str(), r)).collect();

    let resources = sorted
        .iter()
        .filter_map(|uid| {
            let r = resource_map.get(uid.as_str())?;
            let status = match (&r.3, r.4.as_deref()) {
                (Some(_), Some("completed")) => "success",
                (Some(_), _) => "error",
                (None, _) if r.2.is_some() => "running",
                _ => "pending",
            };
            Some(crate::api::TimelineResource {
                unique_id: r.0.clone(),
                resource_type: r.1.clone(),
                status: status.to_string(),
                started_at: r.2,
                finished_at: r.3,
            })
        })
        .collect();

    Ok(axum::Json(crate::api::InvocationTimelineResponse {
        resources,
        invocation_started_at: Some(invocation.started_at),
        is_terminal: !matches!(invocation.status, InvocationLifecycleStatus::Running),
        model_base_url,
    }))
}

fn topo_sort_resources(resource_ids: &[&str], edges: &[(String, String)]) -> Vec<String> {
    use std::collections::{HashMap, HashSet, VecDeque};

    let id_set: HashSet<&str> = resource_ids.iter().copied().collect();
    let mut in_degree: HashMap<&str, usize> = HashMap::new();
    let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();

    for uid in &id_set {
        in_degree.entry(uid).or_insert(0);
    }

    for (parent, child) in edges {
        if id_set.contains(parent.as_str()) && id_set.contains(child.as_str()) {
            adj.entry(parent.as_str()).or_default().push(child.as_str());
            *in_degree.entry(child.as_str()).or_insert(0) += 1;
        }
    }

    let mut queue: VecDeque<&str> = in_degree
        .iter()
        .filter(|(_, deg)| **deg == 0)
        .map(|(&id, _)| id)
        .collect();
    // Sort the initial queue for deterministic output
    let mut initial: Vec<&str> = queue.drain(..).collect();
    initial.sort();
    queue.extend(initial);

    let mut result = Vec::new();
    while let Some(node) = queue.pop_front() {
        result.push(node.to_string());
        if let Some(children) = adj.get(node) {
            let mut next = Vec::new();
            for &child in children {
                if let Some(deg) = in_degree.get_mut(child) {
                    *deg -= 1;
                    if *deg == 0 {
                        next.push(child);
                    }
                }
            }
            next.sort();
            queue.extend(next);
        }
    }

    // Append any resources not in the edge graph
    for uid in resource_ids {
        if !result.iter().any(|r| r == uid) {
            result.push(uid.to_string());
        }
    }

    result
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
    queue.pending_count == 0
        && queue.claimed_count > 0
        && queue.claimed_count == queue.stale_claim_count
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
        let execution_mode = if project.mode == "remote" {
            "server"
        } else {
            "local"
        };
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

async fn workers_index_inner(state: AppState, show_stale: bool) -> Result<Html<String>, UiError> {
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

async fn queues_index_inner(state: AppState, show_stale: bool) -> Result<Html<String>, UiError> {
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
    let panel = read_models::load_environment_panel(&state, &project_id, &slug).await?;

    if is_htmx(&headers) {
        return render_template(&panel);
    }

    render_template(&EnvironmentPageTemplate {
        title: "Environment",
        project: panel.project.clone(),
        environment_slug: slug,
        panel_html: panel
            .render()
            .map_err(|err| UiError(AppError::Internal(err.to_string())))?,
    })
}

async fn environment_detail_panel(
    State(state): State<AppState>,
    Path((project_id, slug)): Path<(String, String)>,
) -> Result<Html<String>, UiError> {
    let panel = read_models::load_environment_panel(&state, &project_id, &slug).await?;
    render_template(&panel)
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

async fn environment_reconcile(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((project_id, slug)): Path<(String, String)>,
) -> Result<Response, UiError> {
    ensure_target_manifest_for_reconcile(&state, &project_id, &slug).await?;
    let service = EnvironmentService::new(state.db());
    service.reconcile(project_id.clone(), slug.clone()).await?;

    if is_htmx(&headers) {
        return environment_detail_panel(State(state), Path((project_id, slug)))
            .await
            .map(IntoResponse::into_response);
    }

    Ok(Redirect::to(&format!("/ui/projects/{project_id}/environments/{slug}")).into_response())
}

async fn ui_environment_pause(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((project_id, slug)): Path<(String, String)>,
) -> Result<Response, UiError> {
    state
        .db()
        .set_environment_auto_reconcile(&project_id, &slug, false)
        .await?;
    if is_htmx(&headers) {
        return environment_detail_panel(State(state), Path((project_id, slug)))
            .await
            .map(IntoResponse::into_response);
    }
    Ok(Redirect::to(&format!("/ui/projects/{project_id}/environments/{slug}")).into_response())
}

async fn ui_environment_resume(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((project_id, slug)): Path<(String, String)>,
) -> Result<Response, UiError> {
    state
        .db()
        .set_environment_auto_reconcile(&project_id, &slug, true)
        .await?;
    if is_htmx(&headers) {
        return environment_detail_panel(State(state), Path((project_id, slug)))
            .await
            .map(IntoResponse::into_response);
    }
    Ok(Redirect::to(&format!("/ui/projects/{project_id}/environments/{slug}")).into_response())
}

async fn environment_plan_admit(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((project_id, slug, plan_id)): Path<(String, String, Uuid)>,
) -> Result<Response, UiError> {
    let service = EnvironmentService::new(state.db());
    let prepared = service.admit_plan(Uuid::new_v4(), plan_id).await?;
    if let (Some(invocation_id), Some(prepared_invocation)) =
        (prepared.invocation_id, prepared.prepared)
    {
        start_prepared_invocation(
            &state,
            invocation_id,
            InvocationCommandApi::Build,
            Some(plan_id),
            prepared_invocation,
        )
        .await?;
        state
            .db()
            .mark_environment_run_plan_admitted(plan_id, invocation_id)
            .await?;
    }

    if is_htmx(&headers) {
        return environment_detail_panel(State(state), Path((project_id, slug)))
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

async fn build_environment_run_plan_views(
    db: &crate::db::Db,
    project_id: &str,
    slug: &str,
    plans: &[EnvironmentRunPlanRecord],
) -> Result<Vec<EnvironmentRunPlanView>, UiError> {
    let mut invocation_cache: HashMap<Uuid, Option<InvocationStatusResponse>> = HashMap::new();
    let mut views = Vec::with_capacity(plans.len());
    for plan in plans {
        let blocker = match plan.blocked_by_invocation_id {
            Some(invocation_id) if plan.status == PlanStatus::Blocked => {
                let invocation = if let Some(cached) = invocation_cache.get(&invocation_id) {
                    cached.clone()
                } else {
                    let loaded = match db.get_invocation_status(invocation_id).await {
                        Ok(status) => Some(status),
                        Err(AppError::Io(err)) if err.kind() == std::io::ErrorKind::NotFound => {
                            None
                        }
                        Err(err) => return Err(UiError(err)),
                    };
                    invocation_cache.insert(invocation_id, loaded.clone());
                    loaded
                };
                let (overlap_count, overlapping_resources) = db
                    .conflicting_resources_for_plan_and_invocation(plan.plan_id, invocation_id, 8)
                    .await?;
                Some(environment_plan_blocker_view(
                    invocation_id,
                    invocation.as_ref(),
                    overlap_count,
                    overlapping_resources,
                ))
            }
            _ => None,
        };
        views.push(environment_run_plan_view(project_id, slug, plan, blocker));
    }
    Ok(views)
}

fn htmx_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("HX-Request", "true".parse().expect("valid header"));
    headers
}

fn parse_uuid(value: &str) -> AppResult<Uuid> {
    Uuid::parse_str(value).map_err(|err| AppError::InvalidInput(format!("invalid uuid: {err}")))
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
        if left_end > start && left_end < bytes.len() && bytes[left_end] == b'.' {
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

fn plan_status_class(status: PlanStatus) -> &'static str {
    match status {
        PlanStatus::Planned => "bg-amber-100 text-amber-800",
        PlanStatus::Blocked => "bg-orange-100 text-orange-800",
        PlanStatus::Admitted => "bg-sky-100 text-sky-800",
        PlanStatus::Completed => "bg-emerald-100 text-emerald-800",
        PlanStatus::Failed | PlanStatus::Canceled => "bg-rose-100 text-rose-800",
        PlanStatus::Superseded => "bg-slate-100 text-slate-700",
    }
}

fn invocation_display_status(invocation: &InvocationStatusResponse) -> &'static str {
    match invocation.status {
        InvocationLifecycleStatus::Running if invocation.claimed_by.is_none() => "queued",
        InvocationLifecycleStatus::Running
            if !matches!(
                invocation.cancel_state,
                crate::api::InvocationCancelStateApi::None
            ) =>
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
    auto_reconcile: bool,
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
    auto_reconcile: bool,
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
struct EnvironmentActualStateView {
    last_attempted_run_id: String,
    last_attempted_commit_sha: String,
    last_attempted_at: String,
    last_successful_run_id: String,
    last_successful_commit_sha: String,
    last_successful_at: String,
    last_admitted_plan_id: String,
    last_completed_plan_id: String,
    updated_at: String,
}

#[derive(Clone)]
struct EnvironmentReconcilePreparationView {
    kind: String,
    status: String,
    status_class: &'static str,
    input_summary: String,
    input_fingerprint: String,
    target_git_commit_sha: String,
    invocation_id: String,
    invocation_url: String,
    error: String,
    failure_count: i32,
    next_attempt_at: String,
    started_at: String,
    completed_at: String,
    updated_at: String,
}

#[derive(Clone)]
struct EnvironmentReconciliationSummaryView {
    state: String,
    state_class: &'static str,
    desired_commit_sha: String,
    actual_commit_sha: String,
    latest_plan_status: String,
    latest_plan_status_class: &'static str,
    latest_plan_reason: String,
    latest_plan_input_summary: String,
    latest_plan_retry_at: String,
    preparation_status: String,
    preparation_status_class: &'static str,
    preparation_input_summary: String,
    preparation_retry_at: String,
    active_resource_count: usize,
    blocked_plan_count: usize,
}

#[derive(Clone)]
struct EnvironmentRunPlanView {
    plan_id: String,
    status: String,
    status_class: &'static str,
    reason: String,
    input_summary: String,
    input_fingerprint: String,
    target_git_branch: String,
    target_git_commit_sha: String,
    resource_count: i32,
    selection_spec: String,
    blocked_by_invocation_id: String,
    admitted_invocation_id: String,
    admitted_invocation_url: String,
    error: String,
    failure_count: i32,
    next_attempt_at: String,
    created_at: String,
    admitted_at: String,
    completed_at: String,
    selected_resources: Vec<String>,
    blocker: Option<EnvironmentPlanBlockerView>,
    admit_url: String,
    can_admit: bool,
}

#[derive(Clone)]
struct EnvironmentPlanBlockerView {
    invocation_id: String,
    invocation_url: String,
    status: String,
    status_class: &'static str,
    worker_queue: String,
    claimed_by: String,
    overlap_count: i64,
    overlapping_resources: Vec<String>,
    remaining_overlap_count: i64,
}

#[derive(Clone)]
struct EnvironmentActiveResourceView {
    invocation_id: String,
    invocation_url: String,
    unique_id: String,
    resource_type: String,
    phase: String,
    phase_class: &'static str,
    selected_at: String,
    node_started_at: String,
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
        status: environment.status.to_string(),
        status_class: status_badge_class(environment.status.as_str()),
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
        status_class: status_badge_class(draft.status.as_str()),
        status: draft.status.to_string(),
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
            selected: value.get("sha").and_then(Value::as_str) == draft.git_commit_sha.as_deref(),
        })
        .collect::<Vec<_>>();
    let commit_options = if commit_options.is_empty()
        && !draft.git_commit_sha.clone().unwrap_or_default().is_empty()
    {
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
        status: draft.status.to_string(),
        slug: draft.slug.clone(),
        git_branch: draft.git_branch.clone().unwrap_or_default(),
        git_commit_sha: draft.git_commit_sha.clone().unwrap_or_default(),
        latest_commit_sha: draft.git_commit_sha.clone().unwrap_or_default(),
        use_latest_commit: draft.use_latest_commit,
        auto_reconcile: draft.auto_reconcile,
        immutable: draft.immutable,
        adapter_type: draft
            .adapter_type
            .clone()
            .unwrap_or_else(|| "postgres".to_string()),
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
        is_loading: draft.status == DraftStatus::LoadingGit,
        is_validating: draft.status == DraftStatus::Validating,
        is_validated: draft.status == DraftStatus::Validated,
        is_failed: draft.status == DraftStatus::Failed,
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
        status_class: status_badge_class(environment.status.as_str()),
        status: environment.status.to_string(),
        git_branch: environment.git_branch.clone().unwrap_or_default(),
        git_commit_sha: environment.git_commit_sha.clone().unwrap_or_default(),
        auto_reconcile: environment.auto_reconcile,
    }
}

fn environment_actual_state_view(
    actual_state: &EnvironmentActualStateRecord,
) -> EnvironmentActualStateView {
    EnvironmentActualStateView {
        last_attempted_run_id: actual_state
            .last_attempted_run_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "—".to_string()),
        last_attempted_commit_sha: actual_state
            .last_attempted_commit_sha
            .clone()
            .unwrap_or_else(|| "—".to_string()),
        last_attempted_at: fmt_optional_ts(actual_state.last_attempted_at),
        last_successful_run_id: actual_state
            .last_successful_run_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "—".to_string()),
        last_successful_commit_sha: actual_state
            .last_successful_commit_sha
            .clone()
            .unwrap_or_else(|| "—".to_string()),
        last_successful_at: fmt_optional_ts(actual_state.last_successful_at),
        last_admitted_plan_id: actual_state
            .last_admitted_plan_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "—".to_string()),
        last_completed_plan_id: actual_state
            .last_completed_plan_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "—".to_string()),
        updated_at: fmt_ts(actual_state.updated_at),
    }
}

fn environment_reconcile_preparation_view(
    preparation: &EnvironmentReconcilePreparationRecord,
) -> EnvironmentReconcilePreparationView {
    let status_class = match preparation.status {
        PreparationStatus::Running => "bg-sky-100 text-sky-800",
        PreparationStatus::Succeeded => "bg-emerald-100 text-emerald-800",
        PreparationStatus::Failed => "bg-rose-100 text-rose-800",
    };
    let invocation_id = preparation
        .invocation_id
        .map(|id| id.to_string())
        .unwrap_or_else(|| "—".to_string());
    let invocation_url = preparation
        .invocation_id
        .map(|id| format!("/ui/invocations/{id}"))
        .unwrap_or_default();
    EnvironmentReconcilePreparationView {
        kind: preparation.kind.replace('_', " "),
        status: preparation.status.to_string(),
        status_class,
        input_summary: reconcile_preparation_input_summary(preparation),
        input_fingerprint: preparation
            .input_fingerprint
            .clone()
            .unwrap_or_else(|| "—".to_string()),
        target_git_commit_sha: preparation
            .target_git_commit_sha
            .clone()
            .unwrap_or_else(|| "—".to_string()),
        invocation_id,
        invocation_url,
        error: preparation.error.clone().unwrap_or_default(),
        failure_count: preparation.failure_count,
        next_attempt_at: fmt_optional_ts(preparation.next_attempt_at),
        started_at: fmt_optional_ts(preparation.started_at),
        completed_at: fmt_optional_ts(preparation.completed_at),
        updated_at: fmt_ts(preparation.updated_at),
    }
}

fn environment_reconciliation_summary_view(
    environment: &EnvironmentRecord,
    actual_state: &EnvironmentActualStateRecord,
    preparation: Option<&EnvironmentReconcilePreparationRecord>,
    plans: &[EnvironmentRunPlanRecord],
    active_resource_count: usize,
) -> EnvironmentReconciliationSummaryView {
    let desired_commit_sha = environment
        .git_commit_sha
        .clone()
        .unwrap_or_else(|| "—".to_string());
    let actual_commit_sha = actual_state
        .last_successful_commit_sha
        .clone()
        .unwrap_or_else(|| "—".to_string());
    let (state, state_class) = if desired_commit_sha == "—" {
        (
            "missing desired commit".to_string(),
            "bg-slate-100 text-slate-700",
        )
    } else if desired_commit_sha == actual_commit_sha && active_resource_count == 0 {
        ("reconciled".to_string(), "bg-emerald-100 text-emerald-800")
    } else if active_resource_count > 0 {
        ("reconciling".to_string(), "bg-sky-100 text-sky-800")
    } else {
        ("drift detected".to_string(), "bg-amber-100 text-amber-800")
    };
    let latest_plan = plans.first();
    let (latest_plan_status, latest_plan_status_class, latest_plan_reason) = latest_plan
        .map(|plan| {
            (
                plan.status.to_string(),
                plan_status_class(plan.status),
                plan.reason.clone(),
            )
        })
        .unwrap_or_else(|| {
            (
                "none".to_string(),
                "bg-slate-100 text-slate-700",
                "—".to_string(),
            )
        });
    let latest_plan_input_summary = latest_plan
        .map(reconcile_plan_input_summary)
        .unwrap_or_else(|| "—".to_string());
    let latest_plan_retry_at = latest_plan
        .and_then(|plan| plan.next_attempt_at)
        .map(fmt_ts)
        .unwrap_or_else(|| "—".to_string());
    let (preparation_status, preparation_status_class) = preparation
        .map(|preparation| {
            (
                preparation.status.to_string(),
                match preparation.status {
                    PreparationStatus::Running => "bg-sky-100 text-sky-800",
                    PreparationStatus::Succeeded => "bg-emerald-100 text-emerald-800",
                    PreparationStatus::Failed => "bg-rose-100 text-rose-800",
                },
            )
        })
        .unwrap_or_else(|| ("idle".to_string(), "bg-slate-100 text-slate-700"));
    let preparation_input_summary = preparation
        .map(reconcile_preparation_input_summary)
        .unwrap_or_else(|| "—".to_string());
    let preparation_retry_at = preparation
        .and_then(|preparation| preparation.next_attempt_at)
        .map(fmt_ts)
        .unwrap_or_else(|| "—".to_string());
    let blocked_plan_count = plans
        .iter()
        .filter(|plan| plan.status == PlanStatus::Blocked)
        .count();
    EnvironmentReconciliationSummaryView {
        state,
        state_class,
        desired_commit_sha,
        actual_commit_sha,
        latest_plan_status,
        latest_plan_status_class,
        latest_plan_reason,
        latest_plan_input_summary,
        latest_plan_retry_at,
        preparation_status,
        preparation_status_class,
        preparation_input_summary,
        preparation_retry_at,
        active_resource_count,
        blocked_plan_count,
    }
}

fn environment_run_plan_view(
    project_id: &str,
    slug: &str,
    plan: &EnvironmentRunPlanRecord,
    blocker: Option<EnvironmentPlanBlockerView>,
) -> EnvironmentRunPlanView {
    let status_class = plan_status_class(plan.status);
    EnvironmentRunPlanView {
        plan_id: plan.plan_id.to_string(),
        status: plan.status.to_string(),
        status_class,
        reason: plan.reason.replace('_', " "),
        input_summary: reconcile_plan_input_summary(plan),
        input_fingerprint: plan
            .input_fingerprint
            .clone()
            .unwrap_or_else(|| "—".to_string()),
        target_git_branch: plan
            .target_git_branch
            .clone()
            .unwrap_or_else(|| "—".to_string()),
        target_git_commit_sha: plan
            .target_git_commit_sha
            .clone()
            .unwrap_or_else(|| "—".to_string()),
        resource_count: plan.resource_count,
        selection_spec: plan
            .selection_spec
            .clone()
            .unwrap_or_else(|| "—".to_string()),
        blocked_by_invocation_id: plan
            .blocked_by_invocation_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "—".to_string()),
        admitted_invocation_id: plan
            .admitted_invocation_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "—".to_string()),
        admitted_invocation_url: plan
            .admitted_invocation_id
            .map(|id| format!("/ui/invocations/{id}"))
            .unwrap_or_default(),
        error: plan.error.clone().unwrap_or_default(),
        failure_count: plan.failure_count,
        next_attempt_at: fmt_optional_ts(plan.next_attempt_at),
        created_at: fmt_ts(plan.created_at),
        admitted_at: fmt_optional_ts(plan.admitted_at),
        completed_at: fmt_optional_ts(plan.completed_at),
        selected_resources: plan.selected_resources.clone(),
        blocker,
        admit_url: format!(
            "/ui/projects/{project_id}/environments/{slug}/plans/{}/admit",
            plan.plan_id
        ),
        can_admit: plan.status.is_admissible(),
    }
}

fn environment_plan_blocker_view(
    invocation_id: Uuid,
    invocation: Option<&InvocationStatusResponse>,
    overlap_count: i64,
    overlapping_resources: Vec<String>,
) -> EnvironmentPlanBlockerView {
    let remaining_overlap_count = overlap_count.saturating_sub(overlapping_resources.len() as i64);
    if let Some(invocation) = invocation {
        let status = invocation_display_status(invocation).to_string();
        EnvironmentPlanBlockerView {
            invocation_id: invocation_id.to_string(),
            invocation_url: format!("/ui/invocations/{invocation_id}"),
            status_class: status_badge_class(&status),
            status,
            worker_queue: invocation.worker_queue.clone(),
            claimed_by: invocation
                .claimed_by
                .clone()
                .unwrap_or_else(|| "—".to_string()),
            overlap_count,
            overlapping_resources,
            remaining_overlap_count,
        }
    } else {
        EnvironmentPlanBlockerView {
            invocation_id: invocation_id.to_string(),
            invocation_url: format!("/ui/invocations/{invocation_id}"),
            status: "missing".to_string(),
            status_class: "bg-slate-100 text-slate-700",
            worker_queue: "—".to_string(),
            claimed_by: "—".to_string(),
            overlap_count,
            overlapping_resources,
            remaining_overlap_count,
        }
    }
}

fn reconcile_preparation_input_summary(
    preparation: &EnvironmentReconcilePreparationRecord,
) -> String {
    match preparation.kind.as_str() {
        "target_manifest" => preparation
            .target_git_commit_sha
            .as_deref()
            .map(|sha| format!("Prepare target manifest for {}", short_commit_sha(sha)))
            .unwrap_or_else(|| "Prepare target manifest".to_string()),
        _ => preparation.kind.replace('_', " "),
    }
}

fn reconcile_plan_input_summary(plan: &EnvironmentRunPlanRecord) -> String {
    match plan.reason.as_str() {
        "code_change" => {
            let target = plan
                .target_git_commit_sha
                .as_deref()
                .map(short_commit_sha)
                .unwrap_or("—");
            let baseline = plan
                .baseline_run_id
                .map(|run_id| run_id.to_string())
                .map(|run_id| run_id.chars().take(8).collect::<String>())
                .unwrap_or_else(|| "—".to_string());
            format!("Code drift to {target} from baseline run {baseline}")
        }
        "source_state_change" => {
            let source_keys = plan
                .metadata
                .get("source_keys")
                .and_then(Value::as_array)
                .map(|values| values.iter().filter_map(Value::as_str).collect::<Vec<_>>())
                .unwrap_or_default();
            if source_keys.is_empty() {
                "Unsatisfied source state change".to_string()
            } else if source_keys.len() == 1 {
                format!("Source update from {}", source_keys[0])
            } else {
                format!(
                    "Source updates from {} and {} more",
                    source_keys[0],
                    source_keys.len() - 1
                )
            }
        }
        other => other.replace('_', " "),
    }
}

fn short_commit_sha(sha: &str) -> &str {
    let end = sha.len().min(8);
    &sha[..end]
}

fn environment_active_resource_view(
    resource: &EnvironmentActiveResourceRecord,
) -> EnvironmentActiveResourceView {
    let (phase, phase_class) = match resource.phase {
        EnvironmentActiveResourcePhaseApi::Selected => {
            ("selected".to_string(), "bg-amber-100 text-amber-800")
        }
        EnvironmentActiveResourcePhaseApi::Running => {
            ("running".to_string(), "bg-sky-100 text-sky-800")
        }
    };
    EnvironmentActiveResourceView {
        invocation_id: resource.invocation_id.to_string(),
        invocation_url: format!("/ui/invocations/{}", resource.invocation_id),
        unique_id: resource.unique_id.clone(),
        resource_type: resource.resource_type.clone(),
        phase,
        phase_class,
        selected_at: fmt_ts(resource.selected_at),
        node_started_at: fmt_optional_ts(resource.node_started_at),
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
    summary: DashboardSummaryView,
    invocations: Vec<InvocationSummaryView>,
    projects: Vec<ProjectSummaryView>,
    workers: Vec<WorkerSummaryView>,
    queues: Vec<QueueSummaryView>,
}

#[derive(Clone)]
struct DashboardSummaryView {
    project_count: i64,
    running_invocation_count: i64,
    queued_invocation_count: i64,
    worker_count: i64,
}

#[derive(Template)]
#[template(path = "dashboard/_summary.html")]
struct DashboardSummaryTemplate {
    summary: DashboardSummaryView,
}

#[derive(Template)]
#[template(path = "dashboard/_workers.html")]
struct DashboardWorkersTemplate {
    workers: Vec<WorkerSummaryView>,
}

#[derive(Template)]
#[template(path = "dashboard/_queues.html")]
struct DashboardQueuesTemplate {
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

struct InvocationTabView {
    label: &'static str,
    url: String,
    partial_url: String,
    active: bool,
}

#[derive(Template)]
#[template(path = "invocations/show.html")]
struct InvocationDetailTemplate {
    title: &'static str,
    invocation: InvocationDetailView,
    panel_url: String,
    tabs: Vec<InvocationTabView>,
    tab_content_html: String,
}

#[derive(Template)]
#[template(path = "invocations/_tab_timeline.html")]
struct InvocationTimelineTabTemplate {
    sse_url: String,
    timeline_api_url: String,
}

#[derive(Template)]
#[template(path = "invocations/_tab_logs.html")]
struct InvocationLogsTabTemplate {
    sse_url: String,
    initial_log_lines: Vec<String>,
    initial_log_sequence: u64,
}

#[derive(Template)]
#[template(path = "invocations/_lineage.html")]
struct InvocationLineageTabTemplate {
    lineage_json: String,
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
    summary: EnvironmentReconciliationSummaryView,
    actual_state: EnvironmentActualStateView,
    preparation: Option<EnvironmentReconcilePreparationView>,
    active_resources: Vec<EnvironmentActiveResourceView>,
    plans: Vec<EnvironmentRunPlanView>,
    versions: Vec<EnvironmentVersionView>,
    is_remote: bool,
    panel_url: String,
    reconcile_url: String,
    pause_url: String,
    resume_url: String,
}

#[derive(Template)]
#[template(path = "error.html")]
struct ErrorTemplate<'a> {
    title: &'a str,
    message: &'a str,
}

// --- Model UI view structs ---

struct ModelFilterSelectView {
    value: String,
    label: String,
    selected: bool,
}

struct ModelFiltersView {
    projects: Vec<ModelFilterSelectView>,
    environments: Vec<ModelFilterSelectView>,
    resource_types: Vec<ModelFilterSelectView>,
}

struct ModelSummaryViewItem {
    name: String,
    node_path: String,
    resource_type: String,
    package_name: String,
    materialized: String,
    status: String,
    status_class: String,
    schema: String,
    finished_at: String,
    last_success_at: String,
    detail_url: String,
    code_state: &'static str,
    code_tooltip: String,
    source_state: &'static str,
    source_tooltip: String,
}

struct ModelTabView {
    label: &'static str,
    url: String,
    partial_url: String,
    active: bool,
}

struct ModelColumnView {
    name: String,
    data_type: String,
    description: String,
}

struct ModelExecView {
    invocation_id: String,
    invocation_url: String,
    command: String,
    status: String,
    status_class: String,
    started_at: String,
    duration: String,
    git_commit_sha: String,
}

struct ModelTestView {
    index: usize,
    unique_id: String,
    name: String,
    test_type: String,
    status: String,
    status_class: String,
    finished_at: String,
    history_url: String,
    detail_url: String,
}

struct ModelHistoryEntryView {
    index: usize,
    git_commit_sha: String,
    git_url: String,
    started_at: String,
    checksum_short: String,
    prev_checksum: String,
    prev_checksum_short: String,
    diff_url: String,
}

struct DiffLineView {
    kind: String,
    text: String,
}

// --- Model UI template structs ---

#[derive(Template)]
#[template(path = "models/index.html")]
struct ModelsPageTemplate {
    title: &'static str,
    filters: ModelFiltersView,
    models: Vec<ModelSummaryViewItem>,
    needs_selection: bool,
}

#[derive(Template)]
#[template(path = "models/show.html")]
struct ModelDetailTemplate {
    title: &'static str,
    project_id: String,
    project_name: String,
    environment_slug: String,
    model_name: String,
    unique_id: String,
    resource_type: String,
    project_mode: String,
    tabs: Vec<ModelTabView>,
    tab_content_html: String,
}

#[derive(Template)]
#[template(path = "models/_overview.html")]
struct ModelOverviewTemplate {
    description: String,
    materialized: String,
    database: String,
    schema: String,
    alias: String,
    file_path: String,
    package_name: String,
    tags: Vec<String>,
    status: String,
    status_class: String,
    columns: Vec<ModelColumnView>,
    promoted_raw_code: String,
    is_stale: bool,
    poll_url: String,
    lineage: OverviewLineageView,
}

#[derive(Template)]
#[template(path = "models/_code.html")]
struct ModelCodeTemplate {
    raw_code: String,
    compiled_code: String,
    raw_code_html: String,
    compiled_code_html: String,
}

#[derive(Template)]
#[template(path = "models/_invocations.html")]
struct ModelInvocationsTemplate {
    executions: Vec<ModelExecView>,
}

#[derive(Template)]
#[template(path = "models/_lineage.html")]
struct ModelLineageTemplate {
    lineage_json: String,
    depth_options: Vec<(i32, bool)>,
    direction: String,
    partial_url: String,
    node_count: usize,
    project_id: String,
    environment_slug: String,
    model_selector: String,
    project_mode: String,
}

#[derive(Template)]
#[template(path = "models/_tests.html")]
struct ModelTestsTemplate {
    tests: Vec<ModelTestView>,
    project_id: String,
    environment_slug: String,
    project_mode: String,
    all_test_selector: String,
    test_count: usize,
}

#[derive(Template)]
#[template(path = "models/_test_history.html")]
struct ModelTestHistoryTemplate {
    executions: Vec<ModelExecView>,
}

#[derive(Template)]
#[template(path = "models/_history.html")]
struct ModelHistoryTemplate {
    entries: Vec<ModelHistoryEntryView>,
}

#[derive(Template)]
#[template(path = "models/_history_diff.html")]
struct ModelHistoryDiffTemplate {
    diff_lines: Vec<DiffLineView>,
}

// --- Catalog overview structs ---

#[allow(dead_code)]
struct OverviewLineageNodeView {
    name: String,
    resource_type: String,
    status: String,
    status_class: String,
    detail_url: String,
}

struct OverviewLineageView {
    parents: Vec<OverviewLineageNodeView>,
    current: OverviewLineageNodeView,
    children: Vec<OverviewLineageNodeView>,
    has_lineage: bool,
}

impl Default for OverviewLineageView {
    fn default() -> Self {
        Self {
            parents: Vec::new(),
            current: OverviewLineageNodeView {
                name: String::new(),
                resource_type: String::new(),
                status: String::new(),
                status_class: String::new(),
                detail_url: String::new(),
            },
            children: Vec::new(),
            has_lineage: false,
        }
    }
}

struct TestDependsOnView {
    name: String,
    unique_id: String,
    detail_url: String,
}

#[derive(Template)]
#[template(path = "models/_overview_source.html")]
struct SourceOverviewTemplate {
    description: String,
    database: String,
    schema: String,
    loader: String,
    identifier: String,
    freshness: String,
    columns: Vec<ModelColumnView>,
    status: String,
    status_class: String,
    lineage: OverviewLineageView,
    poll_url: String,
}

#[derive(Template)]
#[template(path = "models/_overview_seed.html")]
struct SeedOverviewTemplate {
    description: String,
    file_path: String,
    package_name: String,
    database: String,
    schema: String,
    alias: String,
    columns: Vec<ModelColumnView>,
    status: String,
    status_class: String,
    lineage: OverviewLineageView,
    poll_url: String,
}

#[derive(Template)]
#[template(path = "models/_overview_test.html")]
struct TestOverviewTemplate {
    description: String,
    test_type: String,
    severity: String,
    depends_on: Vec<TestDependsOnView>,
    status: String,
    status_class: String,
    poll_url: String,
}

#[derive(Template)]
#[template(path = "models/_overview_snapshot.html")]
struct SnapshotOverviewTemplate {
    description: String,
    strategy: String,
    unique_key: String,
    updated_at_col: String,
    database: String,
    schema: String,
    alias: String,
    file_path: String,
    package_name: String,
    columns: Vec<ModelColumnView>,
    raw_code: String,
    status: String,
    status_class: String,
    lineage: OverviewLineageView,
    poll_url: String,
}

// --- Model UI helpers ---

const LINEAGE_JS: &str = include_str!("../../lineage-ui/dist/lineage.js");
const LINEAGE_CSS: &str = include_str!("../../lineage-ui/dist/lineage.css");
const TIMELINE_JS: &str = include_str!("../../timeline-ui/dist/timeline.js");
const TIMELINE_CSS: &str = include_str!("../../timeline-ui/dist/timeline.css");

#[derive(Debug, Default)]
struct ModelListQuery {
    project_id: Option<String>,
    environment_slug: Option<String>,
    resource_type: Vec<String>,
}

fn parse_catalog_filter_query(raw_query: Option<&str>) -> ModelListQuery {
    let mut query = ModelListQuery::default();
    let Some(raw) = raw_query else { return query };
    let Ok(url) = reqwest::Url::parse(&format!("http://localhost/ui/catalog?{raw}")) else {
        return query;
    };
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "project_id" => query.project_id = Some(value.into_owned()),
            "environment_slug" => query.environment_slug = Some(value.into_owned()),
            "resource_type" => query.resource_type.push(value.into_owned()),
            _ => {}
        }
    }
    query
}

async fn models_index(
    State(state): State<AppState>,
    RawQuery(raw_query): RawQuery,
) -> Result<impl IntoResponse, UiError> {
    let query = parse_catalog_filter_query(raw_query.as_deref());
    let page = read_models::load_catalog_page(&state, query).await?;
    render_template(&page).map(|html| html.into_response())

    // Note: HTMX requests also get the full page; the template uses
    // hx-select to extract the section content for cascading filter updates.
}

async fn lineage_js_asset() -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "application/javascript")],
        LINEAGE_JS,
    )
}

async fn lineage_css_asset() -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "text/css")],
        LINEAGE_CSS,
    )
}

async fn timeline_js_asset() -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "application/javascript")],
        TIMELINE_JS,
    )
}

async fn timeline_css_asset() -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "text/css")],
        TIMELINE_CSS,
    )
}

#[derive(Debug, Default, Deserialize)]
struct ModelTabQuery {
    tab: Option<String>,
    depth: Option<i32>,
    direction: Option<String>,
}

async fn model_detail(
    State(state): State<AppState>,
    Path((project_id, env_slug, unique_id)): Path<(String, String, String)>,
    Query(query): Query<ModelTabQuery>,
) -> Result<Html<String>, UiError> {
    let db = state.db();
    db.require_current_schema().await?;
    let project = db.get_project_by_project_id(&project_id).await?;
    let env = db.get_environment(&project.project_id, &env_slug).await?;
    let unique_id = urlencoding::decode(&unique_id)
        .unwrap_or_default()
        .to_string();
    let resource_type = resource_type_from_unique_id(&unique_id).to_string();

    let tab = query.tab.as_deref().unwrap_or("overview");
    let base = format!(
        "/ui/catalog/{}/{}/{}",
        project_id,
        env_slug,
        urlencoding::encode(&unique_id)
    );

    let valid_tabs: &[&str] = match resource_type.as_str() {
        "source" => &["overview", "lineage"],
        "test" => &["overview", "invocations"],
        "seed" => &["overview", "invocations", "lineage", "history"],
        "snapshot" => &["overview", "code", "invocations", "lineage", "history"],
        _ => &[
            "overview",
            "code",
            "invocations",
            "lineage",
            "tests",
            "history",
        ],
    };
    let tab = if valid_tabs.contains(&tab) {
        tab
    } else {
        "overview"
    };

    let tab_content_html = render_tab(db, &project, &env, &unique_id, tab, &base, &query).await?;

    let detail = db.get_model_detail(project.id, env.id, &unique_id).await?;
    let model_name = detail
        .latest_manifest_node
        .as_ref()
        .and_then(|n| n.get("name").and_then(Value::as_str))
        .unwrap_or(&unique_id)
        .to_string();

    let tabs = valid_tabs
        .iter()
        .map(|&t| {
            let extra = if t == "lineage" {
                let d = query.depth.unwrap_or(2);
                let dir = query.direction.as_deref().unwrap_or("both");
                format!("&depth={d}&direction={dir}")
            } else {
                String::new()
            };
            ModelTabView {
                label: match t {
                    "overview" => "Overview",
                    "code" => "Code",
                    "invocations" => "Invocations",
                    "lineage" => "Lineage",
                    "tests" => "Tests",
                    "history" => "History",
                    _ => t,
                },
                url: format!("{base}?tab={t}{extra}"),
                partial_url: format!("{base}/tab?tab={t}{extra}"),
                active: t == tab,
            }
        })
        .collect();

    let title: &'static str = match resource_type.as_str() {
        "source" => "Source",
        "seed" => "Seed",
        "test" => "Test",
        "snapshot" => "Snapshot",
        _ => "Model",
    };

    render_template(&ModelDetailTemplate {
        title,
        project_id: project_id.clone(),
        project_name: project.project_name.clone(),
        environment_slug: env_slug.clone(),
        model_name,
        unique_id,
        resource_type: resource_type.to_string(),
        project_mode: project.mode.clone(),
        tabs,
        tab_content_html,
    })
}

async fn model_tab(
    State(state): State<AppState>,
    Path((project_id, env_slug, unique_id)): Path<(String, String, String)>,
    Query(query): Query<ModelTabQuery>,
) -> Result<Html<String>, UiError> {
    let db = state.db();
    db.require_current_schema().await?;
    let project = db.get_project_by_project_id(&project_id).await?;
    let env = db.get_environment(&project.project_id, &env_slug).await?;
    let unique_id = urlencoding::decode(&unique_id)
        .unwrap_or_default()
        .to_string();
    let tab = query.tab.as_deref().unwrap_or("overview");
    let base = format!(
        "/ui/catalog/{}/{}/{}",
        project_id,
        env_slug,
        urlencoding::encode(&unique_id)
    );
    let html = render_tab(db, &project, &env, &unique_id, tab, &base, &query).await?;
    Ok(Html(html))
}

async fn render_tab(
    db: &crate::db::Db,
    project: &crate::db::ProjectRecord,
    env: &crate::db::EnvironmentRecord,
    unique_id: &str,
    tab: &str,
    base: &str,
    query: &ModelTabQuery,
) -> Result<String, UiError> {
    match tab {
        "overview" => render_overview_tab(db, project, env, unique_id, base).await,
        "code" => render_code_tab(db, project, env, unique_id).await,
        "invocations" => render_invocations_tab(db, project, env, unique_id).await,
        "lineage" => render_lineage_tab(db, project, env, unique_id, base, query).await,
        "tests" => render_tests_tab(db, project, env, unique_id, base).await,
        "history" => render_history_tab(db, project, env, unique_id, base).await,
        _ => render_overview_tab(db, project, env, unique_id, base).await,
    }
}

fn model_status_class(status: &str) -> &'static str {
    match NodeExecutionStatus::parse(status) {
        Some(NodeExecutionStatus::Success | NodeExecutionStatus::Pass) => {
            "bg-emerald-100 text-emerald-800"
        }
        Some(NodeExecutionStatus::Error | NodeExecutionStatus::Fail) => "bg-rose-100 text-rose-800",
        _ => "bg-slate-100 text-slate-600",
    }
}

async fn render_overview_tab(
    db: &crate::db::Db,
    project: &crate::db::ProjectRecord,
    env: &crate::db::EnvironmentRecord,
    unique_id: &str,
    base: &str,
) -> Result<String, UiError> {
    let detail = db.get_model_detail(project.id, env.id, unique_id).await?;
    let node = detail.latest_manifest_node.as_ref();
    let promoted = detail.promoted_manifest_node.as_ref();

    let empty = Value::Object(Default::default());
    let n = node.unwrap_or(&empty);
    let status = detail.status.as_deref().unwrap_or("unknown");
    let poll_url = format!("{base}/tab?tab=overview");
    let resource_type = resource_type_from_unique_id(unique_id);
    let node_name = extract_str(n, "name");

    let columns_obj = n.get("columns").and_then(Value::as_object);
    let columns: Vec<ModelColumnView> = columns_obj
        .into_iter()
        .flat_map(|cols| cols.values())
        .map(|col| ModelColumnView {
            name: extract_str(col, "name"),
            data_type: extract_str(col, "data_type"),
            description: extract_str(col, "description"),
        })
        .collect();

    match resource_type {
        "source" => {
            let lineage = build_overview_lineage(
                db,
                project,
                env,
                unique_id,
                &node_name,
                resource_type,
                status,
            )
            .await?;
            let freshness = n
                .get("freshness")
                .map(|v| v.to_string())
                .unwrap_or_default();
            SourceOverviewTemplate {
                description: extract_str(n, "description"),
                database: extract_str(n, "database"),
                schema: extract_str(n, "schema"),
                loader: extract_str(n, "loader"),
                identifier: extract_str(n, "identifier"),
                freshness,
                columns,
                status: status.to_string(),
                status_class: model_status_class(status).to_string(),
                lineage,
                poll_url,
            }
            .render()
            .map_err(|e| UiError(AppError::Internal(e.to_string())))
        }
        "seed" => {
            let lineage = build_overview_lineage(
                db,
                project,
                env,
                unique_id,
                &node_name,
                resource_type,
                status,
            )
            .await?;
            SeedOverviewTemplate {
                description: extract_str(n, "description"),
                file_path: extract_str(n, "original_file_path"),
                package_name: extract_str(n, "package_name"),
                database: extract_str(n, "database"),
                schema: extract_str(n, "schema"),
                alias: extract_str(n, "alias"),
                columns,
                status: status.to_string(),
                status_class: model_status_class(status).to_string(),
                lineage,
                poll_url,
            }
            .render()
            .map_err(|e| UiError(AppError::Internal(e.to_string())))
        }
        "test" => {
            let config = n
                .get("config")
                .cloned()
                .unwrap_or(Value::Object(Default::default()));
            let test_type = config
                .get("test_metadata")
                .and_then(|tm| tm.get("name"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let severity = config
                .get("severity")
                .and_then(Value::as_str)
                .unwrap_or("ERROR")
                .to_string();
            let depends_on_nodes = n
                .get("depends_on")
                .and_then(|d| d.get("nodes"))
                .and_then(Value::as_array);
            let base_url = format!("/ui/catalog/{}/{}", project.project_id, env.slug);
            let depends_on: Vec<TestDependsOnView> = depends_on_nodes
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(|uid| {
                    let name = uid.rsplit('.').next().unwrap_or(uid).to_string();
                    TestDependsOnView {
                        name,
                        unique_id: uid.to_string(),
                        detail_url: format!("{}/{}", base_url, urlencoding::encode(uid)),
                    }
                })
                .collect();
            TestOverviewTemplate {
                description: extract_str(n, "description"),
                test_type,
                severity,
                depends_on,
                status: status.to_string(),
                status_class: model_status_class(status).to_string(),
                poll_url,
            }
            .render()
            .map_err(|e| UiError(AppError::Internal(e.to_string())))
        }
        "snapshot" => {
            let lineage = build_overview_lineage(
                db,
                project,
                env,
                unique_id,
                &node_name,
                resource_type,
                status,
            )
            .await?;
            let config = n
                .get("config")
                .cloned()
                .unwrap_or(Value::Object(Default::default()));
            SnapshotOverviewTemplate {
                description: extract_str(n, "description"),
                strategy: config
                    .get("strategy")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                unique_key: config
                    .get("unique_key")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                updated_at_col: config
                    .get("updated_at")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                database: extract_str(n, "database"),
                schema: extract_str(n, "schema"),
                alias: extract_str(n, "alias"),
                file_path: extract_str(n, "original_file_path"),
                package_name: extract_str(n, "package_name"),
                columns,
                raw_code: n
                    .get("raw_code")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                status: status.to_string(),
                status_class: model_status_class(status).to_string(),
                lineage,
                poll_url,
            }
            .render()
            .map_err(|e| UiError(AppError::Internal(e.to_string())))
        }
        _ => {
            // model (default)
            let latest_checksum = node.and_then(|n| {
                n.get("checksum")
                    .and_then(|c| c.get("checksum"))
                    .and_then(Value::as_str)
            });
            let promoted_checksum = promoted.and_then(|n| {
                n.get("checksum")
                    .and_then(|c| c.get("checksum"))
                    .and_then(Value::as_str)
            });
            let is_stale = match (latest_checksum, promoted_checksum) {
                (Some(l), Some(p)) => l != p,
                (Some(_), None) => true,
                _ => false,
            };
            let tags: Vec<String> = n
                .get("tags")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(String::from)
                .collect();
            let materialized = n
                .get("config")
                .and_then(|c| c.get("materialized"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let promoted_raw_code = if is_stale {
                promoted
                    .and_then(|p| p.get("raw_code").and_then(Value::as_str))
                    .unwrap_or("")
                    .to_string()
            } else {
                String::new()
            };
            let lineage =
                build_overview_lineage(db, project, env, unique_id, &node_name, "model", status)
                    .await?;
            ModelOverviewTemplate {
                description: extract_str(n, "description"),
                materialized,
                database: extract_str(n, "database"),
                schema: extract_str(n, "schema"),
                alias: extract_str(n, "alias"),
                file_path: extract_str(n, "original_file_path"),
                package_name: extract_str(n, "package_name"),
                tags,
                status: status.to_string(),
                status_class: model_status_class(status).to_string(),
                columns,
                promoted_raw_code,
                is_stale,
                poll_url,
                lineage,
            }
            .render()
            .map_err(|e| UiError(AppError::Internal(e.to_string())))
        }
    }
}

async fn render_code_tab(
    db: &crate::db::Db,
    project: &crate::db::ProjectRecord,
    env: &crate::db::EnvironmentRecord,
    unique_id: &str,
) -> Result<String, UiError> {
    let detail = db.get_model_detail(project.id, env.id, unique_id).await?;
    let node = detail.latest_manifest_node.as_ref();
    let empty = Value::Object(Default::default());
    let n = node.unwrap_or(&empty);
    let raw_code = n
        .get("raw_code")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let compiled_code = n
        .get("compiled_code")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let raw_code_html = if raw_code.is_empty() {
        String::new()
    } else {
        highlight_sql(&raw_code)
    };
    let compiled_code_html = if compiled_code.is_empty() {
        String::new()
    } else {
        highlight_sql(&compiled_code)
    };
    ModelCodeTemplate {
        raw_code,
        compiled_code,
        raw_code_html,
        compiled_code_html,
    }
    .render()
    .map_err(|e| UiError(AppError::Internal(e.to_string())))
}

async fn render_invocations_tab(
    db: &crate::db::Db,
    project: &crate::db::ProjectRecord,
    env: &crate::db::EnvironmentRecord,
    unique_id: &str,
) -> Result<String, UiError> {
    let execs = db
        .get_model_node_executions(project.id, env.id, unique_id, 50)
        .await?;
    let executions = execs.iter().map(build_exec_view).collect();
    ModelInvocationsTemplate { executions }
        .render()
        .map_err(|e| UiError(AppError::Internal(e.to_string())))
}

fn build_exec_view(e: &crate::db::ModelNodeExecutionRecord) -> ModelExecView {
    let status = e.status.as_deref().unwrap_or("unknown");
    ModelExecView {
        invocation_id: e.invocation_id.map(|id| id.to_string()).unwrap_or_default(),
        invocation_url: e
            .invocation_id
            .map(|id| format!("/ui/invocations/{id}"))
            .unwrap_or_default(),
        command: e.command.clone(),
        status: status.to_string(),
        status_class: model_status_class(status).to_string(),
        started_at: fmt_opt_time(e.started_at),
        duration: fmt_duration(e.execution_time_seconds),
        git_commit_sha: e
            .git_commit_sha
            .as_deref()
            .map(short_hash)
            .unwrap_or_default(),
    }
}

fn fmt_opt_time(t: Option<chrono::DateTime<Utc>>) -> String {
    t.map(|t| t.format("%Y-%m-%d %H:%M").to_string())
        .unwrap_or_default()
}

async fn render_lineage_tab(
    db: &crate::db::Db,
    project: &crate::db::ProjectRecord,
    env: &crate::db::EnvironmentRecord,
    unique_id: &str,
    base: &str,
    query: &ModelTabQuery,
) -> Result<String, UiError> {
    let depth = query.depth.unwrap_or(2).clamp(1, 5);
    let direction = query.direction.as_deref().unwrap_or("both");
    let lineage = db
        .get_model_lineage(project.id, env.id, unique_id, depth, direction)
        .await?;

    let base_url = format!("/ui/catalog/{}/{}", project.project_id, env.slug);

    let test_ids: std::collections::HashSet<&str> = lineage
        .nodes
        .iter()
        .filter(|n| n.resource_type.as_deref() == Some("test"))
        .map(|n| n.unique_id.as_str())
        .collect();
    let mut test_counts: std::collections::HashMap<&str, (u32, u32)> =
        std::collections::HashMap::new();
    for (parent, child) in &lineage.edges {
        if test_ids.contains(child.as_str())
            && let Some(tn) = lineage.nodes.iter().find(|n| n.unique_id == *child)
        {
            let e = test_counts.entry(parent.as_str()).or_insert((0, 0));
            match tn.status.as_deref().and_then(NodeExecutionStatus::parse) {
                Some(NodeExecutionStatus::Pass | NodeExecutionStatus::Success) => e.0 += 1,
                Some(NodeExecutionStatus::Fail | NodeExecutionStatus::Error) => e.1 += 1,
                _ => {}
            }
        }
    }

    // Load reconciliation state for all visible nodes
    let visible_ids: Vec<String> = lineage
        .nodes
        .iter()
        .filter(|n| !test_ids.contains(n.unique_id.as_str()))
        .map(|n| n.unique_id.clone())
        .collect();
    let reconcile_states = db
        .load_node_reconciliation_state(project.id, env.id, &visible_ids)
        .await?;
    let reconcile_map: std::collections::HashMap<&str, &crate::db::NodeReconcileState> =
        reconcile_states
            .iter()
            .map(|s| (s.unique_id.as_str(), s))
            .collect();

    let nodes_json: Vec<Value> = lineage
        .nodes
        .iter()
        .filter(|n| !test_ids.contains(n.unique_id.as_str()))
        .map(|n| {
            let (pass, fail) = test_counts
                .get(n.unique_id.as_str())
                .copied()
                .unwrap_or((0, 0));
            let reconcile = reconcile_map.get(n.unique_id.as_str());
            serde_json::json!({
                "id": n.unique_id,
                "data": {
                    "label": n.name.as_deref().unwrap_or(&n.unique_id),
                    "name": n.name,
                    "resource_type": n.resource_type,
                    "status": n.status,
                    "materialized": n.materialized,
                    "package_name": n.package_name,
                    "testsPassing": pass,
                    "testsFailing": fail,
                    "reconcileState": {
                        "code": reconcile.map(|r| r.code_state.as_str()).unwrap_or("unknown"),
                        "codeTooltip": reconcile.map(|r| r.code_tooltip.as_str()).unwrap_or(""),
                        "source": reconcile.map(|r| r.source_state.as_str()).unwrap_or("no_sources"),
                        "sourceTooltip": reconcile.map(|r| r.source_tooltip.as_str()).unwrap_or(""),
                    }
                }
            })
        })
        .collect();

    let edges_json: Vec<Value> = lineage
        .edges
        .iter()
        .filter(|(s, t)| !test_ids.contains(s.as_str()) && !test_ids.contains(t.as_str()))
        .map(|(src, tgt)| {
            serde_json::json!({
                "id": format!("{src}->{tgt}"),
                "source": src,
                "target": tgt,
            })
        })
        .collect();

    let lineage_data = serde_json::json!({
        "nodes": nodes_json,
        "edges": edges_json,
        "currentNodeId": unique_id,
        "baseUrl": base_url,
        "depth": depth,
        "direction": direction,
    });

    ModelLineageTemplate {
        lineage_json: lineage_data.to_string(),
        depth_options: (1..=5).map(|d| (d, d == depth)).collect(),
        direction: direction.to_string(),
        partial_url: format!("{base}/tab"),
        node_count: nodes_json.len(),
        project_id: project.project_id.clone(),
        environment_slug: env.slug.clone(),
        model_selector: nodes_json
            .iter()
            .filter_map(|n| n.get("id").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join(" "),
        project_mode: project.mode.clone(),
    }
    .render()
    .map_err(|e| UiError(AppError::Internal(e.to_string())))
}

fn fmt_duration(seconds: Option<f64>) -> String {
    match seconds {
        Some(s) if s >= 60.0 => format!("{:.0}m {:.0}s", s / 60.0, s % 60.0),
        Some(s) => format!("{:.1}s", s),
        None => String::new(),
    }
}

fn short_hash(s: &str) -> String {
    if s.len() > 8 {
        s[..8].to_string()
    } else {
        s.to_string()
    }
}

fn highlight_sql(code: &str) -> String {
    use syntect::highlighting::ThemeSet;
    use syntect::html::highlighted_html_for_string;
    use syntect::parsing::SyntaxSet;

    let ss = SyntaxSet::load_defaults_newlines();
    let ts = ThemeSet::load_defaults();
    let syntax = ss
        .find_syntax_by_extension("sql")
        .unwrap_or_else(|| ss.find_syntax_plain_text());
    let theme = &ts.themes["InspiredGitHub"];
    let inner = match highlighted_html_for_string(code, &ss, syntax, theme) {
        Ok(html) => html
            .strip_prefix("<pre style=\"")
            .and_then(|s| s.find("\">").map(|i| &s[i + 2..]))
            .and_then(|s| {
                s.strip_suffix("</pre>\n")
                    .or_else(|| s.strip_suffix("</pre>"))
            })
            .unwrap_or(&html)
            .to_string(),
        Err(_) => code
            .replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;"),
    };
    let lines: Vec<&str> = inner.split('\n').collect();
    let count = if lines.last() == Some(&"") {
        lines.len() - 1
    } else {
        lines.len()
    };
    let mut out = String::from("<table class=\"w-full border-collapse\"><tbody>");
    for (i, line) in lines.iter().enumerate().take(count) {
        let num = i + 1;
        out.push_str(&format!(
            "<tr><td class=\"select-none pr-4 text-right align-top text-slate-300\" style=\"width:1%;white-space:nowrap;\">{num}</td><td><pre class=\"m-0 p-0\" style=\"background:transparent;\">{line}</pre></td></tr>"
        ));
    }
    out.push_str("</tbody></table>");
    out
}

async fn render_tests_tab(
    db: &crate::db::Db,
    project: &crate::db::ProjectRecord,
    env: &crate::db::EnvironmentRecord,
    unique_id: &str,
    base: &str,
) -> Result<String, UiError> {
    let raw_tests = db.get_model_tests(project.id, env.id, unique_id).await?;
    let tests: Vec<ModelTestView> = raw_tests
        .iter()
        .enumerate()
        .map(|(i, t)| {
            let status = t.status.as_deref().unwrap_or("unknown");
            ModelTestView {
                index: i,
                unique_id: t.unique_id.clone(),
                name: t.name.clone().unwrap_or_else(|| t.unique_id.clone()),
                test_type: t.test_type.clone().unwrap_or_default(),
                status: status.to_string(),
                status_class: model_status_class(status).to_string(),
                finished_at: fmt_opt_time(t.finished_at),
                history_url: format!("{base}/test-history/{}", urlencoding::encode(&t.unique_id)),
                detail_url: format!(
                    "/ui/catalog/{}/{}/{}",
                    project.project_id,
                    env.slug,
                    urlencoding::encode(&t.unique_id)
                ),
            }
        })
        .collect();
    let all_test_selector = raw_tests
        .iter()
        .map(|t| t.unique_id.as_str())
        .collect::<Vec<_>>()
        .join(" ");
    let test_count = tests.len();
    ModelTestsTemplate {
        tests,
        project_id: project.project_id.clone(),
        environment_slug: env.slug.clone(),
        project_mode: project.mode.clone(),
        all_test_selector,
        test_count,
    }
    .render()
    .map_err(|e| UiError(AppError::Internal(e.to_string())))
}

async fn render_history_tab(
    db: &crate::db::Db,
    project: &crate::db::ProjectRecord,
    env: &crate::db::EnvironmentRecord,
    unique_id: &str,
    base: &str,
) -> Result<String, UiError> {
    let history = db.get_model_history(project.id, env.id, unique_id).await?;
    let entries: Vec<ModelHistoryEntryView> = history
        .iter()
        .enumerate()
        .map(|(i, h)| {
            let sha = h.git_commit_sha.as_deref().unwrap_or("");
            let prev_run_id = if i + 1 < history.len() {
                Some(history[i + 1].run_id)
            } else {
                None
            };
            ModelHistoryEntryView {
                index: i,
                git_commit_sha: short_hash(sha),
                git_url: git_commit_url(h.git_repo_url.as_deref(), h.git_commit_sha.as_deref()),
                started_at: h.started_at.format("%Y-%m-%d %H:%M").to_string(),
                checksum_short: h.checksum.as_deref().map(short_hash).unwrap_or_default(),
                prev_checksum: h.prev_checksum.clone().unwrap_or_default(),
                prev_checksum_short: h
                    .prev_checksum
                    .as_deref()
                    .map(short_hash)
                    .unwrap_or_default(),
                diff_url: format!(
                    "{base}/history-diff?run_id={}&prev_run_id={}",
                    h.run_id,
                    prev_run_id.map(|id| id.to_string()).unwrap_or_default()
                ),
            }
        })
        .collect();
    ModelHistoryTemplate { entries }
        .render()
        .map_err(|e| UiError(AppError::Internal(e.to_string())))
}

async fn model_test_history(
    State(state): State<AppState>,
    Path((project_id, env_slug, _unique_id, test_unique_id)): Path<(
        String,
        String,
        String,
        String,
    )>,
) -> Result<Html<String>, UiError> {
    let db = state.db();
    db.require_current_schema().await?;
    let project = db.get_project_by_project_id(&project_id).await?;
    let env = db.get_environment(&project.project_id, &env_slug).await?;
    let test_uid = urlencoding::decode(&test_unique_id)
        .unwrap_or_default()
        .to_string();
    let execs = db
        .get_model_node_executions(project.id, env.id, &test_uid, 20)
        .await?;
    let executions = execs.iter().map(build_exec_view).collect();
    render_template(&ModelTestHistoryTemplate { executions })
}

#[derive(Debug, Deserialize)]
struct HistoryDiffQuery {
    run_id: Uuid,
    prev_run_id: Option<Uuid>,
}

async fn model_history_diff(
    State(state): State<AppState>,
    Path((_project_id, _env_slug, unique_id)): Path<(String, String, String)>,
    Query(query): Query<HistoryDiffQuery>,
) -> Result<Html<String>, UiError> {
    let db = state.db();
    let unique_id = urlencoding::decode(&unique_id)
        .unwrap_or_default()
        .to_string();
    let new_code = db
        .get_model_history_raw_code(query.run_id, &unique_id)
        .await?
        .unwrap_or_default();
    let old_code = match query.prev_run_id {
        Some(prev) if prev != Uuid::nil() => db
            .get_model_history_raw_code(prev, &unique_id)
            .await?
            .unwrap_or_default(),
        _ => String::new(),
    };
    let diff_lines = compute_diff(&old_code, &new_code);
    render_template(&ModelHistoryDiffTemplate { diff_lines })
}

fn git_commit_url(repo_url: Option<&str>, sha: Option<&str>) -> String {
    match (repo_url, sha) {
        (Some(url), Some(sha)) if !url.is_empty() && !sha.is_empty() => {
            let base = url.trim_end_matches(".git");
            format!("{base}/commit/{sha}")
        }
        _ => String::new(),
    }
}

fn extract_str(v: &Value, key: &str) -> String {
    v.get(key).and_then(Value::as_str).unwrap_or("").to_string()
}

fn resource_type_from_unique_id(unique_id: &str) -> &str {
    if unique_id.starts_with("source.") {
        "source"
    } else if unique_id.starts_with("seed.") {
        "seed"
    } else if unique_id.starts_with("test.") {
        "test"
    } else if unique_id.starts_with("snapshot.") {
        "snapshot"
    } else {
        "model"
    }
}

async fn build_overview_lineage(
    db: &crate::db::Db,
    project: &crate::db::ProjectRecord,
    env: &crate::db::EnvironmentRecord,
    unique_id: &str,
    current_name: &str,
    current_resource_type: &str,
    current_status: &str,
) -> Result<OverviewLineageView, UiError> {
    let lineage = db
        .get_model_lineage(project.id, env.id, unique_id, 1, "both")
        .await?;
    let base_url = format!("/ui/catalog/{}/{}", project.project_id, env.slug);

    let make_node = |n: &crate::db::LineageNodeRecord| {
        let status = n.status.as_deref().unwrap_or("unknown");
        OverviewLineageNodeView {
            name: n.name.clone().unwrap_or_else(|| n.unique_id.clone()),
            resource_type: n.resource_type.clone().unwrap_or_default(),
            status: status.to_string(),
            status_class: model_status_class(status).to_string(),
            detail_url: format!("{}/{}", base_url, urlencoding::encode(&n.unique_id)),
        }
    };

    let parents: Vec<OverviewLineageNodeView> = lineage
        .edges
        .iter()
        .filter(|(_, child)| child == unique_id)
        .filter_map(|(parent, _)| lineage.nodes.iter().find(|n| n.unique_id == *parent))
        .map(make_node)
        .collect();

    let children: Vec<OverviewLineageNodeView> = lineage
        .edges
        .iter()
        .filter(|(parent, _)| parent == unique_id)
        .filter_map(|(_, child)| lineage.nodes.iter().find(|n| n.unique_id == *child))
        .map(make_node)
        .collect();

    let has_lineage = !parents.is_empty() || !children.is_empty();

    Ok(OverviewLineageView {
        parents,
        current: OverviewLineageNodeView {
            name: current_name.to_string(),
            resource_type: current_resource_type.to_string(),
            status: current_status.to_string(),
            status_class: model_status_class(current_status).to_string(),
            detail_url: String::new(),
        },
        children,
        has_lineage,
    })
}

fn compute_diff(old: &str, new: &str) -> Vec<DiffLineView> {
    let old_lines: Vec<&str> = old.lines().collect();
    let new_lines: Vec<&str> = new.lines().collect();
    let mut result = Vec::new();
    let max = old_lines.len().max(new_lines.len());
    let mut oi = 0;
    let mut ni = 0;
    while oi < old_lines.len() || ni < new_lines.len() {
        if oi < old_lines.len() && ni < new_lines.len() && old_lines[oi] == new_lines[ni] {
            result.push(DiffLineView {
                kind: "same".into(),
                text: old_lines[oi].to_string(),
            });
            oi += 1;
            ni += 1;
        } else if oi < old_lines.len()
            && (ni >= new_lines.len()
                || (ni + 1 < new_lines.len() && new_lines[ni + 1..].contains(&old_lines[oi])))
        {
            // Check if old line appears later in new — if so, new lines were added
            if ni < new_lines.len() && !old_lines[oi..].contains(&new_lines[ni]) {
                result.push(DiffLineView {
                    kind: "add".into(),
                    text: new_lines[ni].to_string(),
                });
                ni += 1;
            } else {
                result.push(DiffLineView {
                    kind: "remove".into(),
                    text: old_lines[oi].to_string(),
                });
                oi += 1;
            }
        } else if ni < new_lines.len() {
            result.push(DiffLineView {
                kind: "add".into(),
                text: new_lines[ni].to_string(),
            });
            ni += 1;
        } else {
            result.push(DiffLineView {
                kind: "remove".into(),
                text: old_lines[oi].to_string(),
            });
            oi += 1;
        }
        if result.len() > max + 100 {
            break;
        } // safety
    }
    let _ = max; // suppress unused
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::InvocationCancelStateApi;
    use chrono::Utc;

    fn draft(
        status: DraftStatus,
        validation_invocation_id: Option<Uuid>,
    ) -> crate::db::ProjectDraftRecord {
        crate::db::ProjectDraftRecord {
            id: Uuid::new_v4(),
            git_repo_url: "git@github.com:org/repo.git".to_string(),
            project_root: "analytics/jaffle_shop".to_string(),
            status,
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
        let draft = draft(DraftStatus::Validating, Some(Uuid::nil()));
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
        let draft = draft(DraftStatus::Validating, None);
        let rendered = render_project_draft_fragment(&draft, None, true)
            .expect("render pending draft")
            .0;

        assert!(rendered.contains("hx-trigger=\"load delay:2s\""));
        assert!(rendered.contains("/ui/project-drafts/"));
    }

    #[test]
    fn terminal_project_draft_statuses_are_detected() {
        assert!(is_terminal_project_draft_status(DraftStatus::Validated));
        assert!(is_terminal_project_draft_status(DraftStatus::Failed));
        assert!(!is_terminal_project_draft_status(DraftStatus::Draft));
        assert!(!is_terminal_project_draft_status(DraftStatus::Validating));
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

        assert!(
            rendered
                .contains("hx-get=\"/ui/invocations/00000000-0000-0000-0000-000000000000/panel\"")
        );
        assert!(rendered.contains("hx-trigger=\"every 2s\""));
    }

    #[test]
    fn invocation_detail_resumes_stream_after_initial_history() {
        let rendered = InvocationLogsTabTemplate {
            sse_url: "/v1/invocations/00000000-0000-0000-0000-000000000000/events".to_string(),
            initial_log_lines: vec!["line 1".to_string()],
            initial_log_sequence: 7,
        }
        .render()
        .expect("render invocation logs tab");

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
    fn dashboard_summary_uses_running_and_queued_invocation_links() {
        let rendered = DashboardSummaryTemplate {
            summary: DashboardSummaryView {
                project_count: 4,
                running_invocation_count: 2,
                queued_invocation_count: 3,
                worker_count: 1,
            },
        }
        .render()
        .expect("render dashboard summary");
        assert!(rendered.contains("Running Invocations"));
        assert!(rendered.contains("Queued Invocations"));
        assert!(rendered.contains("href=\"/ui/invocations?status=running\""));
        assert!(rendered.contains("href=\"/ui/invocations?status=queued\""));
        assert!(rendered.contains("hx-get=\"/ui/dashboard/summary\""));
    }

    #[test]
    fn dashboard_workers_and_queues_poll_for_updates() {
        let workers_rendered = DashboardWorkersTemplate { workers: vec![] }
            .render()
            .expect("render dashboard workers");
        assert!(workers_rendered.contains("hx-get=\"/ui/dashboard/workers\""));
        assert!(workers_rendered.contains("hx-trigger=\"every 2s\""));

        let queues_rendered = DashboardQueuesTemplate { queues: vec![] }
            .render()
            .expect("render dashboard queues");
        assert!(queues_rendered.contains("hx-get=\"/ui/dashboard/queues\""));
        assert!(queues_rendered.contains("hx-trigger=\"every 2s\""));
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
                    &[
                        ("queued", "Queued"),
                        ("running", "Running"),
                        ("cancelling", "Cancelling"),
                    ],
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

    #[test]
    fn environment_panel_renders_reconciliation_sections_and_actions() {
        let rendered = EnvironmentPanelTemplate {
            project: ProjectSummaryView {
                project_id: "prj_123".to_string(),
                project_name: "analytics".to_string(),
                mode: "remote".to_string(),
                git_repo_url: "https://example.com/repo.git".to_string(),
                project_root: ".".to_string(),
                delete_url: "/ui/projects/prj_123/delete".to_string(),
                create_environment_url: "/ui/projects/prj_123/environments/new".to_string(),
            },
            environment: EnvironmentDetailView {
                slug: "prod".to_string(),
                profile_name: "analytics".to_string(),
                target_name: "prod".to_string(),
                adapter_type: "duckdb".to_string(),
                worker_queue: "generic".to_string(),
                schema_name: "main".to_string(),
                status: "active".to_string(),
                status_class: "bg-emerald-100 text-emerald-800",
                git_branch: "main".to_string(),
                git_commit_sha: "aaaaaaaa".to_string(),
                auto_reconcile: true,
            },
            summary: EnvironmentReconciliationSummaryView {
                state: "drift detected".to_string(),
                state_class: "bg-amber-100 text-amber-800",
                desired_commit_sha: "bbbbbbbb".to_string(),
                actual_commit_sha: "aaaaaaaa".to_string(),
                latest_plan_status: "blocked".to_string(),
                latest_plan_status_class: "bg-orange-100 text-orange-800",
                latest_plan_reason: "source_state_change".to_string(),
                latest_plan_input_summary: "Source update from source.pkg.raw_orders".to_string(),
                latest_plan_retry_at: "2026-01-01 00:05:00".to_string(),
                preparation_status: "running".to_string(),
                preparation_status_class: "bg-sky-100 text-sky-800",
                preparation_input_summary: "Prepare target manifest for bbbbbbbb".to_string(),
                preparation_retry_at: "—".to_string(),
                active_resource_count: 1,
                blocked_plan_count: 1,
            },
            actual_state: EnvironmentActualStateView {
                last_attempted_run_id: "run-a".to_string(),
                last_attempted_commit_sha: "aaaaaaaa".to_string(),
                last_attempted_at: "2026-01-01 00:00:00".to_string(),
                last_successful_run_id: "run-a".to_string(),
                last_successful_commit_sha: "aaaaaaaa".to_string(),
                last_successful_at: "2026-01-01 00:00:00".to_string(),
                last_admitted_plan_id: "plan-a".to_string(),
                last_completed_plan_id: "plan-b".to_string(),
                updated_at: "2026-01-01 00:00:00".to_string(),
            },
            preparation: Some(EnvironmentReconcilePreparationView {
                kind: "target manifest".to_string(),
                status: "running".to_string(),
                status_class: "bg-sky-100 text-sky-800",
                input_summary: "Prepare target manifest for bbbbbbbb".to_string(),
                input_fingerprint: "target_manifest:code_change:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:run-a".to_string(),
                target_git_commit_sha: "bbbbbbbb".to_string(),
                invocation_id: Uuid::nil().to_string(),
                invocation_url: format!("/ui/invocations/{}", Uuid::nil()),
                error: String::new(),
                failure_count: 0,
                next_attempt_at: "—".to_string(),
                started_at: "2026-01-01 00:00:00".to_string(),
                completed_at: "—".to_string(),
                updated_at: "2026-01-01 00:00:00".to_string(),
            }),
            active_resources: vec![EnvironmentActiveResourceView {
                invocation_id: Uuid::nil().to_string(),
                invocation_url: format!("/ui/invocations/{}", Uuid::nil()),
                unique_id: "model.pkg.orders".to_string(),
                resource_type: "model".to_string(),
                phase: "running".to_string(),
                phase_class: "bg-sky-100 text-sky-800",
                selected_at: "2026-01-01 00:00:00".to_string(),
                node_started_at: "2026-01-01 00:00:01".to_string(),
            }],
            plans: vec![EnvironmentRunPlanView {
                plan_id: Uuid::nil().to_string(),
                status: "blocked".to_string(),
                status_class: "bg-orange-100 text-orange-800",
                reason: "source state change".to_string(),
                input_summary: "Source update from source.pkg.raw_orders".to_string(),
                input_fingerprint: "source_state_change:123".to_string(),
                target_git_branch: "main".to_string(),
                target_git_commit_sha: "aaaaaaaa".to_string(),
                resource_count: 2,
                selection_spec: "source_downstream".to_string(),
                blocked_by_invocation_id: Uuid::nil().to_string(),
                admitted_invocation_id: "—".to_string(),
                admitted_invocation_url: String::new(),
                error: "plan is blocked".to_string(),
                failure_count: 1,
                next_attempt_at: "2026-01-01 00:05:00".to_string(),
                created_at: "2026-01-01 00:00:00".to_string(),
                admitted_at: "—".to_string(),
                completed_at: "—".to_string(),
                selected_resources: vec![
                    "source.pkg.raw_orders".to_string(),
                    "model.pkg.orders".to_string(),
                ],
                blocker: Some(EnvironmentPlanBlockerView {
                    invocation_id: Uuid::nil().to_string(),
                    invocation_url: format!("/ui/invocations/{}", Uuid::nil()),
                    status: "running".to_string(),
                    status_class: "bg-sky-100 text-sky-800",
                    worker_queue: "generic".to_string(),
                    claimed_by: "worker-1".to_string(),
                    overlap_count: 2,
                    overlapping_resources: vec![
                        "source.pkg.raw_orders".to_string(),
                        "model.pkg.orders".to_string(),
                    ],
                    remaining_overlap_count: 0,
                }),
                admit_url: "/ui/projects/prj_123/environments/prod/plans/00000000-0000-0000-0000-000000000000/admit".to_string(),
                can_admit: true,
            }],
            versions: vec![],
            is_remote: true,
            panel_url: "/ui/projects/prj_123/environments/prod/panel".to_string(),
            reconcile_url: "/ui/projects/prj_123/environments/prod/reconcile".to_string(),
            pause_url: "/ui/projects/prj_123/environments/prod/pause".to_string(),
            resume_url: "/ui/projects/prj_123/environments/prod/resume".to_string(),
        }
        .render()
        .expect("render environment panel");

        assert!(rendered.contains("Actual State"));
        assert!(rendered.contains("Active Resources"));
        assert!(rendered.contains("Reconciliation Plans"));
        assert!(rendered.contains("/ui/projects/prj_123/environments/prod/reconcile"));
        assert!(rendered.contains("/ui/projects/prj_123/environments/prod/panel"));
        assert!(rendered.contains("source.pkg.raw_orders"));
        assert!(rendered.contains("Input Fingerprint"));
        assert!(rendered.contains("Prepare target manifest for bbbbbbbb"));
        assert!(rendered.contains("Blocking Invocation"));
        assert!(rendered.contains("Overlap 2"));
    }

    #[test]
    fn topo_sort_orders_parents_before_children() {
        let ids = vec!["model.pkg.a", "model.pkg.b", "model.pkg.c"];
        let edges = vec![
            ("model.pkg.a".to_string(), "model.pkg.b".to_string()),
            ("model.pkg.b".to_string(), "model.pkg.c".to_string()),
        ];
        let sorted = super::topo_sort_resources(&ids, &edges);
        assert_eq!(sorted, vec!["model.pkg.a", "model.pkg.b", "model.pkg.c"]);
    }

    #[test]
    fn topo_sort_handles_no_edges() {
        let ids = vec!["model.pkg.b", "model.pkg.a"];
        let edges: Vec<(String, String)> = vec![];
        let sorted = super::topo_sort_resources(&ids, &edges);
        // Alphabetical when no edges
        assert_eq!(sorted, vec!["model.pkg.a", "model.pkg.b"]);
    }

    #[test]
    fn topo_sort_ignores_edges_outside_resource_set() {
        let ids = vec!["model.pkg.b", "model.pkg.c"];
        let edges = vec![
            ("model.pkg.a".to_string(), "model.pkg.b".to_string()),
            ("model.pkg.b".to_string(), "model.pkg.c".to_string()),
        ];
        let sorted = super::topo_sort_resources(&ids, &edges);
        assert_eq!(sorted, vec!["model.pkg.b", "model.pkg.c"]);
    }

    #[test]
    fn resource_type_from_unique_id_maps_all_types() {
        assert_eq!(
            super::resource_type_from_unique_id("model.pkg.orders"),
            "model"
        );
        assert_eq!(
            super::resource_type_from_unique_id("source.pkg.raw"),
            "source"
        );
        assert_eq!(super::resource_type_from_unique_id("seed.pkg.data"), "seed");
        assert_eq!(
            super::resource_type_from_unique_id("test.pkg.not_null"),
            "test"
        );
        assert_eq!(
            super::resource_type_from_unique_id("snapshot.pkg.snap"),
            "snapshot"
        );
        assert_eq!(
            super::resource_type_from_unique_id("unknown.pkg.x"),
            "model"
        );
        assert_eq!(super::resource_type_from_unique_id(""), "model");
    }

    #[test]
    fn parse_catalog_filter_query_handles_repeated_resource_type() {
        let query = super::parse_catalog_filter_query(Some(
            "project_id=prj1&environment_slug=dev&resource_type=model&resource_type=source",
        ));
        assert_eq!(query.project_id.as_deref(), Some("prj1"));
        assert_eq!(query.environment_slug.as_deref(), Some("dev"));
        assert_eq!(query.resource_type, vec!["model", "source"]);
    }

    #[test]
    fn parse_catalog_filter_query_defaults_empty_when_no_resource_type() {
        let query = super::parse_catalog_filter_query(Some("project_id=prj1"));
        assert!(query.resource_type.is_empty());
    }

    #[test]
    fn parse_catalog_filter_query_handles_none() {
        let query = super::parse_catalog_filter_query(None);
        assert!(query.project_id.is_none());
        assert!(query.resource_type.is_empty());
    }
}
