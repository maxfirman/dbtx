use super::*;

pub(super) async fn dashboard(State(state): State<AppState>) -> Result<Html<String>, UiError> {
    let db = state.db();
    let projects = db.list_projects().await?;
    let invocations = db
        .list_invocations(InvocationListApiRequest {
            limit: Some(10),
            ..Default::default()
        })
        .await?;
    let raw_workers = db.list_workers().await?;
    let workers = operators::filter_workers(
        raw_workers
            .iter()
            .map(worker_summary_view)
            .collect::<Vec<_>>(),
        false,
    );
    let configured_queues = operators::configured_queue_keys(db).await?;
    let (non_stale_worker_queues, stale_worker_queues) =
        operators::worker_queue_health_sets(&raw_workers);
    let queues = operators::filter_queues(
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

pub(super) async fn dashboard_summary(
    State(state): State<AppState>,
) -> Result<Html<String>, UiError> {
    let db = state.db();
    let project_count = db.list_projects().await?.len() as i64;
    let raw_workers = db.list_workers().await?;
    let workers = operators::filter_workers(
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

pub(super) async fn load_dashboard_summary(
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

pub(super) async fn dashboard_recent_invocations(
    State(state): State<AppState>,
) -> Result<Html<String>, UiError> {
    let db = state.db();
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

pub(super) async fn dashboard_workers(
    State(state): State<AppState>,
) -> Result<Html<String>, UiError> {
    let db = state.db();
    let workers = operators::filter_workers(
        db.list_workers()
            .await?
            .iter()
            .map(worker_summary_view)
            .collect(),
        false,
    );
    render_template(&DashboardWorkersTemplate { workers })
}

pub(super) async fn dashboard_queues(
    State(state): State<AppState>,
) -> Result<Html<String>, UiError> {
    let db = state.db();
    let raw_workers = db.list_workers().await?;
    let configured_queues = operators::configured_queue_keys(db).await?;
    let (non_stale_worker_queues, stale_worker_queues) =
        operators::worker_queue_health_sets(&raw_workers);
    let queues = operators::filter_queues(
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
