use super::*;

#[derive(Debug, Default, Deserialize)]
pub(super) struct StaleVisibilityQuery {
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

pub(super) fn filter_workers(
    workers: Vec<WorkerSummaryView>,
    show_stale: bool,
) -> Vec<WorkerSummaryView> {
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

pub(super) fn queue_key(execution_mode: &str, worker_queue: &str) -> String {
    format!("{execution_mode}:{worker_queue}")
}

pub(super) fn worker_queue_health_sets(
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

pub(super) async fn configured_queue_keys(db: &crate::db::Db) -> Result<HashSet<String>, UiError> {
    let projects = db.list_projects().await?;
    let mut keys = HashSet::new();
    for project in projects {
        for environment in db.list_environments(&project.project_id).await? {
            let execution_mode = if environment.git_commit_sha.is_some() {
                "server"
            } else {
                "local"
            };
            keys.insert(queue_key(execution_mode, &environment.worker_queue));
        }
    }
    Ok(keys)
}

pub(super) fn filter_queues(
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

pub(super) async fn workers_index(
    State(state): State<AppState>,
    Query(query): Query<StaleVisibilityQuery>,
) -> Result<Html<String>, UiError> {
    workers_index_inner(state, show_stale_enabled(&query)).await
}

pub(super) async fn workers_table(
    State(state): State<AppState>,
    Query(query): Query<StaleVisibilityQuery>,
) -> Result<Html<String>, UiError> {
    let db = state.db();
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

pub(super) async fn queues_index(
    State(state): State<AppState>,
    Query(query): Query<StaleVisibilityQuery>,
) -> Result<Html<String>, UiError> {
    queues_index_inner(state, show_stale_enabled(&query)).await
}

pub(super) async fn queues_table(
    State(state): State<AppState>,
    Query(query): Query<StaleVisibilityQuery>,
) -> Result<Html<String>, UiError> {
    let db = state.db();
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
