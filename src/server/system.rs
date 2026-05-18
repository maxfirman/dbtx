use super::*;

pub(super) async fn healthz() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
    })
}

pub(super) async fn readyz(
    State(state): State<AppState>,
) -> Result<(StatusCode, Json<ReadyResponse>), ApiError> {
    let database = match state.db.ping().await {
        Ok(()) => "ok",
        Err(err) => {
            error!(error = %err, "readiness database check failed");
            return Ok((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ReadyResponse {
                    status: "not_ready".to_string(),
                    database: "error".to_string(),
                    schema: "unknown".to_string(),
                }),
            ));
        }
    };

    let (status_code, schema, status) = match state.db.require_current_schema().await {
        Ok(()) => (StatusCode::OK, "ok", "ready"),
        Err(AppError::SchemaOutOfDate) => {
            (StatusCode::SERVICE_UNAVAILABLE, "out_of_date", "not_ready")
        }
        Err(err) => {
            error!(error = %err, "readiness schema check failed");
            (StatusCode::SERVICE_UNAVAILABLE, "error", "not_ready")
        }
    };

    Ok((
        status_code,
        Json(ReadyResponse {
            status: status.to_string(),
            database: database.to_string(),
            schema: schema.to_string(),
        }),
    ))
}

#[utoipa::path(
    post,
    path = "/v1/state/migrate",
    tag = "state",
    responses(
        (status = 200, description = "Applied migrations", body = MigrateResponse),
        (status = 500, description = "Migration failed", body = ApiErrorResponse)
    )
)]
pub(super) async fn migrate(
    State(state): State<AppState>,
) -> Result<Json<MigrateResponse>, ApiError> {
    let applied = state.db.migrate().await?;
    info!(applied = applied.len(), "applied database migrations");
    Ok(Json(MigrateResponse { applied }))
}
