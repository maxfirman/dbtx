//! Server-rendered operator UI: HTMX handlers, Askama templates, and view models.
mod assets;
mod catalog;
mod dashboard;
mod environments;
mod formatting;
mod invocations;
mod operators;
mod projects;
mod read_models;

use formatting::{
    DiffLineView, compute_diff, fmt_duration, fmt_opt_time, fmt_optional_ts, fmt_ts,
    git_commit_url, highlight_sql, invocation_display_status, invocation_mode_value,
    model_status_class, plan_status_class, render_invocation_log_html,
    resource_type_from_unique_id, short_commit_sha, short_hash, status_badge_class,
    topo_sort_resources,
};

use crate::api::{
    EnvironmentActiveResourcePhaseApi, InvocationLifecycleStatus, InvocationListApiRequest,
    InvocationStatusResponse, QueueStatusResponse, WorkerStatusResponse,
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
    start_environment_draft_validation_invocation, start_project_draft_validation_invocation,
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
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(dashboard::dashboard))
        .route(
            "/ui/dashboard/recent-invocations",
            get(dashboard::dashboard_recent_invocations),
        )
        .route("/ui/dashboard/summary", get(dashboard::dashboard_summary))
        .route("/ui/dashboard/workers", get(dashboard::dashboard_workers))
        .route("/ui/dashboard/queues", get(dashboard::dashboard_queues))
        .route("/ui/projects", get(projects::projects_index))
        .route("/ui/projects/new", get(projects::project_create_modal))
        .route(
            "/ui/projects/{project_id}/environments/new",
            get(projects::environment_create_modal),
        )
        .route(
            "/ui/projects/{project_id}/delete",
            get(projects::project_delete_modal).post(projects::project_delete),
        )
        .route("/ui/project-drafts", post(projects::project_draft_create))
        .route(
            "/ui/project-drafts/{draft_id}",
            get(projects::project_draft_status),
        )
        .route(
            "/ui/project-drafts/{draft_id}/confirm",
            post(projects::project_draft_confirm),
        )
        .route(
            "/ui/environment-drafts/{draft_id}",
            get(projects::environment_draft_status),
        )
        .route(
            "/ui/environment-drafts/{draft_id}/branch",
            post(projects::environment_draft_branch_refresh),
        )
        .route(
            "/ui/environment-drafts/{draft_id}/validate",
            post(projects::environment_draft_validate),
        )
        .route(
            "/ui/environment-drafts/{draft_id}/confirm",
            post(projects::environment_draft_confirm),
        )
        .route("/ui/invocations", get(invocations::invocations_index))
        .route("/ui/invocations/table", get(invocations::invocations_table))
        .route("/ui/invocations/{id}", get(invocations::invocation_detail))
        .route("/ui/invocations/{id}/tab", get(invocations::invocation_tab))
        .route(
            "/ui/invocations/{id}/panel",
            get(invocations::invocation_detail_panel),
        )
        .route(
            "/ui/invocations/{id}/cancel",
            post(invocations::invocation_cancel),
        )
        .route(
            "/v1/invocations/{id}/timeline",
            get(invocations::invocation_timeline),
        )
        .route("/ui/workers", get(operators::workers_index))
        .route("/ui/workers/table", get(operators::workers_table))
        .route("/ui/queues", get(operators::queues_index))
        .route("/ui/queues/table", get(operators::queues_table))
        .route(
            "/ui/projects/{project_id}/environments/{slug}",
            get(environments::environment_detail),
        )
        .route(
            "/ui/projects/{project_id}/environments/{slug}/panel",
            get(environments::environment_detail_panel),
        )
        .route(
            "/ui/projects/{project_id}/environments/{slug}/release",
            post(environments::environment_release),
        )
        .route(
            "/ui/projects/{project_id}/environments/{slug}/reconcile",
            post(environments::environment_reconcile),
        )
        .route(
            "/ui/projects/{project_id}/environments/{slug}/pause",
            post(environments::ui_environment_pause),
        )
        .route(
            "/ui/projects/{project_id}/environments/{slug}/resume",
            post(environments::ui_environment_resume),
        )
        .route(
            "/ui/projects/{project_id}/environments/{slug}/plans/{plan_id}/admit",
            post(environments::environment_plan_admit),
        )
        .route(
            "/ui/projects/{project_id}/environments/{slug}/rollback",
            post(environments::environment_rollback),
        )
        .route("/ui/catalog", get(catalog::models_index))
        .route(
            "/ui/catalog/{project_id}/{env_slug}/{unique_id}",
            get(catalog::model_detail),
        )
        .route(
            "/ui/catalog/{project_id}/{env_slug}/{unique_id}/tab",
            get(catalog::model_tab),
        )
        .route(
            "/ui/catalog/{project_id}/{env_slug}/{unique_id}/test-history/{test_unique_id}",
            get(catalog::model_test_history),
        )
        .route(
            "/ui/catalog/{project_id}/{env_slug}/{unique_id}/history-diff",
            get(catalog::model_history_diff),
        )
        .route("/ui/assets/lineage.js", get(assets::lineage_js_asset))
        .route("/ui/assets/lineage.css", get(assets::lineage_css_asset))
        .route("/ui/assets/timeline.js", get(assets::timeline_js_asset))
        .route("/ui/assets/timeline.css", get(assets::timeline_css_asset))
}

// --- Shared helpers ---

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

fn htmx_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("HX-Request", "true".parse().expect("valid header"));
    headers
}

fn parse_uuid(value: &str) -> AppResult<Uuid> {
    Uuid::parse_str(value).map_err(|err| AppError::InvalidInput(format!("invalid uuid: {err}")))
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

// --- Shared form types ---

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

// --- View structs ---

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

#[derive(Clone)]
struct DashboardSummaryView {
    project_count: i64,
    running_invocation_count: i64,
    queued_invocation_count: i64,
    worker_count: i64,
}

// --- Model/Catalog view structs ---

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

// --- Template structs ---

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

// --- View builder functions ---

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
#[cfg(test)]
mod tests {
    use super::*;
    use super::catalog::parse_catalog_filter_query;
    use super::invocations::{
        InvocationFilterQuery, invocation_dynamic_option_views, invocation_filter_option_views,
        invocations_page_url, parse_invocation_filter_query,
    };
    use super::operators::{filter_queues, filter_workers, queue_key};
    use super::projects::{is_terminal_project_draft_status, render_project_draft_fragment};
    use crate::api::{InvocationCancelStateApi, InvocationExecutionModeApi};
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
    fn parse_catalog_filter_query_handles_repeated_resource_type() {
        let query = parse_catalog_filter_query(Some(
            "project_id=prj1&environment_slug=dev&resource_type=model&resource_type=source",
        ));
        assert_eq!(query.project_id.as_deref(), Some("prj1"));
        assert_eq!(query.environment_slug.as_deref(), Some("dev"));
        assert_eq!(query.resource_type, vec!["model", "source"]);
    }

    #[test]
    fn parse_catalog_filter_query_defaults_empty_when_no_resource_type() {
        let query = parse_catalog_filter_query(Some("project_id=prj1"));
        assert!(query.resource_type.is_empty());
    }

    #[test]
    fn parse_catalog_filter_query_handles_none() {
        let query = parse_catalog_filter_query(None);
        assert!(query.project_id.is_none());
        assert!(query.resource_type.is_empty());
    }
}
