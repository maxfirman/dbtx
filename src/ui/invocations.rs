use super::*;

const INVOCATIONS_PAGE_SIZE: usize = 50;

#[derive(Debug, Default, Deserialize)]
pub(super) struct InvocationFilterQuery {
    #[serde(default, deserialize_with = "deserialize_multi_value_form_field")]
    pub(super) status: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_multi_value_form_field")]
    pub(super) execution_mode: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_multi_value_form_field")]
    pub(super) worker_queue: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_multi_value_form_field")]
    pub(super) claimed_by: Vec<String>,
    pub(super) page: Option<usize>,
}

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

pub(super) fn parse_invocation_filter_query(
    raw_query: Option<&str>,
) -> AppResult<InvocationFilterQuery> {
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

pub(super) fn invocations_page_url(query: &InvocationFilterQuery, page: usize) -> String {
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

pub(super) fn invocation_filter_option_views(
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

pub(super) fn invocation_dynamic_option_views(
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

pub(super) async fn invocations_index(
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

pub(super) async fn invocations_table(
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
pub(super) struct InvocationTabQuery {
    pub(super) tab: Option<String>,
}

pub(super) async fn invocation_detail(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<InvocationTabQuery>,
) -> Result<Html<String>, UiError> {
    let invocation_id = parse_uuid(&id)?;
    let page =
        read_models::load_invocation_detail_page(&state, invocation_id, query.tab.as_deref())
            .await?;
    render_template(&page)
}

pub(super) async fn invocation_tab(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<InvocationTabQuery>,
) -> Result<Html<String>, UiError> {
    let invocation_id = parse_uuid(&id)?;
    let html =
        read_models::render_invocation_tab_content(&state, invocation_id, query.tab.as_deref())
            .await?;
    Ok(Html(html))
}

pub(super) async fn invocation_detail_panel(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Html<String>, UiError> {
    let invocation_id = parse_uuid(&id)?;
    let panel = read_models::load_invocation_detail_panel(&state, invocation_id).await?;
    render_template(&panel)
}

pub(super) async fn invocation_cancel(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, UiError> {
    let db = state.db();
    let invocation_id = parse_uuid(&id)?;
    db.request_cancel_invocation(invocation_id).await?;
    Ok(Redirect::to(&format!("/ui/invocations/{invocation_id}")))
}

pub(super) async fn invocation_timeline(
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
