use super::*;

#[utoipa::path(
    get,
    path = "/v1/workers",
    tag = "workers",
    responses(
        (status = 200, description = "Worker operational view", body = WorkersResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn worker_list(
    State(state): State<AppState>,
) -> Result<Json<WorkersResponse>, ApiError> {
    let workers = state.db.list_workers().await?;
    info!(count = workers.len(), "listed workers");
    Ok(Json(WorkersResponse { workers }))
}

#[utoipa::path(
    get,
    path = "/v1/queues",
    tag = "workers",
    responses(
        (status = 200, description = "Queue operational view", body = QueuesResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn queue_list(
    State(state): State<AppState>,
) -> Result<Json<QueuesResponse>, ApiError> {
    let queues = state.db.list_queues().await?;
    info!(count = queues.len(), "listed queues");
    Ok(Json(QueuesResponse { queues }))
}

pub(super) async fn reconcile_tick(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let planned = crate::reconciler::reconcile_environments_once(&state).await?;
    Ok(Json(serde_json::json!({ "planned": planned })))
}

pub(super) async fn reconcile_sweep(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let admitted = crate::reconciler::sweep_blocked_plans_once(&state).await?;
    Ok(Json(serde_json::json!({ "admitted": admitted })))
}
