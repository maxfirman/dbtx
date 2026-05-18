use super::*;

pub(super) async fn environment_local_upsert(
    State(state): State<AppState>,
    Path(project_id): Path<String>,
    Json(request): Json<LocalEnvironmentUpsertApiRequest>,
) -> Result<Json<LocalEnvironmentUpsertApiResponse>, ApiError> {
    let project = state.db.get_project_by_project_id(&project_id).await?;
    let slug = format!("local-{}-{}", request.machine_id, request.target_name);
    let worker_queue = format!("local-{}", request.machine_id);
    let environment = state
        .db
        .upsert_local_environment_lightweight(
            project.id,
            &slug,
            &request.target_name,
            &request.adapter_type,
            &worker_queue,
            &request.schema_name,
        )
        .await?;
    info!(
        project_id = %project_id,
        environment_slug = %environment.slug,
        machine_id = %request.machine_id,
        "upserted local environment"
    );
    Ok(Json(LocalEnvironmentUpsertApiResponse {
        environment_slug: environment.slug,
        worker_queue,
    }))
}

#[utoipa::path(
    post,
    path = "/v1/projects/{project_id}/environment-drafts",
    tag = "environments",
    params(
        ("project_id" = String, Path, description = "Project identifier")
    ),
    responses(
        (status = 200, description = "Started environment draft git metadata load", body = EnvironmentDraftStartResponse),
        (status = 404, description = "Project not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn environment_draft_create(
    State(state): State<AppState>,
    Path(project_id): Path<String>,
) -> Result<Json<EnvironmentDraftStartResponse>, ApiError> {
    let service = EnvironmentService::new(&state.db);
    let draft = service.create_draft(project_id).await?;
    let prepared = service.prepare_draft_git_metadata(draft.id).await?;
    let invocation_id = state
        .start_environment_draft_prepare_invocation(prepared)
        .await?;
    let draft = state.db.get_environment_draft(draft.id).await?;
    Ok(Json(EnvironmentDraftStartResponse {
        draft,
        invocation_id,
    }))
}

#[utoipa::path(
    get,
    path = "/v1/environment-drafts/{draft_id}",
    tag = "environments",
    params(
        ("draft_id" = Uuid, Path, description = "Draft identifier")
    ),
    responses(
        (status = 200, description = "Environment draft", body = EnvironmentDraftResponse),
        (status = 404, description = "Draft not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn environment_draft_get(
    State(state): State<AppState>,
    Path(draft_id): Path<Uuid>,
) -> Result<Json<EnvironmentDraftResponse>, ApiError> {
    let draft = state.db.get_environment_draft(draft_id).await?;
    Ok(Json(EnvironmentDraftResponse { draft }))
}

#[utoipa::path(
    post,
    path = "/v1/environment-drafts/{draft_id}/branch",
    tag = "environments",
    params(
        ("draft_id" = Uuid, Path, description = "Draft identifier")
    ),
    request_body = EnvironmentDraftUpdateApiRequest,
    responses(
        (status = 200, description = "Started branch metadata refresh", body = EnvironmentDraftStartResponse),
        (status = 404, description = "Draft not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn environment_draft_branch_refresh(
    State(state): State<AppState>,
    Path(draft_id): Path<Uuid>,
    Json(request): Json<EnvironmentDraftUpdateApiRequest>,
) -> Result<Json<EnvironmentDraftStartResponse>, ApiError> {
    let service = EnvironmentService::new(&state.db);
    let prepared = service
        .refresh_draft_branch(draft_id, environment_draft_update_request(request))
        .await?;
    let invocation_id = state
        .start_environment_draft_prepare_invocation(prepared)
        .await?;
    let draft = state.db.get_environment_draft(draft_id).await?;
    Ok(Json(EnvironmentDraftStartResponse {
        draft,
        invocation_id,
    }))
}

#[utoipa::path(
    post,
    path = "/v1/environment-drafts/{draft_id}/validate",
    tag = "environments",
    params(
        ("draft_id" = Uuid, Path, description = "Draft identifier")
    ),
    request_body = EnvironmentDraftUpdateApiRequest,
    responses(
        (status = 200, description = "Started environment draft validation", body = EnvironmentDraftStartResponse),
        (status = 404, description = "Draft not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn environment_draft_validate(
    State(state): State<AppState>,
    Path(draft_id): Path<Uuid>,
    Json(request): Json<EnvironmentDraftUpdateApiRequest>,
) -> Result<Json<EnvironmentDraftStartResponse>, ApiError> {
    let service = EnvironmentService::new(&state.db);
    let prepared = service
        .prepare_draft_validation(draft_id, environment_draft_update_request(request))
        .await?;
    let invocation_id = state
        .start_environment_draft_validation_invocation(prepared)
        .await?;
    let draft = state.db.get_environment_draft(draft_id).await?;
    Ok(Json(EnvironmentDraftStartResponse {
        draft,
        invocation_id,
    }))
}

#[utoipa::path(
    post,
    path = "/v1/environment-drafts/{draft_id}/confirm",
    tag = "environments",
    params(
        ("draft_id" = Uuid, Path, description = "Draft identifier")
    ),
    responses(
        (status = 200, description = "Confirmed environment", body = EnvironmentResponse),
        (status = 404, description = "Draft not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn environment_draft_confirm(
    State(state): State<AppState>,
    Path(draft_id): Path<Uuid>,
) -> Result<Json<EnvironmentResponse>, ApiError> {
    let environment = state.db.confirm_environment_draft(draft_id).await?;
    Ok(Json(EnvironmentResponse { environment }))
}

fn environment_draft_update_request(
    request: EnvironmentDraftUpdateApiRequest,
) -> crate::services::EnvironmentDraftUpdateRequest {
    crate::services::EnvironmentDraftUpdateRequest {
        project: String::new(),
        slug: request.slug,
        git_branch: request.git_branch,
        git_commit_sha: request.git_commit_sha,
        use_latest_commit: request.use_latest_commit,
        auto_reconcile: request.auto_reconcile,
        immutable: request.immutable,
        adapter_type: request.adapter_type,
        schema_name: request.schema_name,
        threads: request.threads,
        profile_config: request.profile_config,
        profile_secrets: request.profile_secrets,
    }
}

#[utoipa::path(
    get,
    path = "/v1/projects/{project_id}/environments",
    tag = "environments",
    params(
        ("project_id" = String, Path, description = "Project identifier")
    ),
    responses(
        (status = 200, description = "Environments", body = EnvironmentsResponse),
        (status = 404, description = "Project not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn environment_list(
    State(state): State<AppState>,
    Path(project_id): Path<String>,
) -> Result<Json<EnvironmentsResponse>, ApiError> {
    let environments = state.db.list_environments(&project_id).await?;
    info!(count = environments.len(), "listed environments");
    Ok(Json(EnvironmentsResponse { environments }))
}

#[utoipa::path(
    get,
    path = "/v1/projects/{project_id}/environments/{slug}",
    tag = "environments",
    params(
        ("project_id" = String, Path, description = "Project identifier"),
        ("slug" = String, Path, description = "Environment slug")
    ),
    responses(
        (status = 200, description = "Environment", body = EnvironmentResponse),
        (status = 404, description = "Environment not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn environment_get(
    State(state): State<AppState>,
    Path((project_id, slug)): Path<(String, String)>,
) -> Result<Json<EnvironmentResponse>, ApiError> {
    let environment = state.db.get_environment(&project_id, &slug).await?;
    info!(
        project_id = %environment.project_ref,
        environment = %environment.slug,
        "loaded environment"
    );
    Ok(Json(EnvironmentResponse { environment }))
}

#[utoipa::path(
    get,
    path = "/v1/projects/{project_id}/environments/{slug}/actual-state",
    tag = "environments",
    params(
        ("project_id" = String, Path, description = "Project identifier"),
        ("slug" = String, Path, description = "Environment slug")
    ),
    responses(
        (status = 200, description = "Environment actual state", body = EnvironmentActualStateResponse),
        (status = 404, description = "Environment not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn environment_actual_state(
    State(state): State<AppState>,
    Path((project_id, slug)): Path<(String, String)>,
) -> Result<Json<EnvironmentActualStateResponse>, ApiError> {
    let actual_state = state
        .db
        .get_environment_actual_state(&project_id, &slug)
        .await?;
    Ok(Json(EnvironmentActualStateResponse { actual_state }))
}

#[utoipa::path(
    get,
    path = "/v1/projects/{project_id}/environments/{slug}/reconcile-preparation",
    tag = "environments",
    params(
        ("project_id" = String, Path, description = "Project identifier"),
        ("slug" = String, Path, description = "Environment slug")
    ),
    responses(
        (status = 200, description = "Environment reconciliation preparation state", body = EnvironmentReconcilePreparationResponse),
        (status = 404, description = "Environment not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn environment_reconcile_preparation(
    State(state): State<AppState>,
    Path((project_id, slug)): Path<(String, String)>,
) -> Result<Json<EnvironmentReconcilePreparationResponse>, ApiError> {
    let preparation = state
        .db()
        .get_environment_reconcile_preparation(&project_id, &slug)
        .await?;
    Ok(Json(EnvironmentReconcilePreparationResponse {
        preparation,
    }))
}

#[utoipa::path(
    post,
    path = "/v1/projects/{project_id}/environments/{slug}/release",
    tag = "environments",
    params(
        ("project_id" = String, Path, description = "Project identifier"),
        ("slug" = String, Path, description = "Environment slug")
    ),
    request_body = EnvironmentReleaseApiRequest,
    responses(
        (status = 200, description = "Released environment", body = EnvironmentResponse),
        (status = 400, description = "Invalid request", body = ApiErrorResponse),
        (status = 404, description = "Environment not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn environment_release(
    State(state): State<AppState>,
    Path((project_id, slug)): Path<(String, String)>,
    Json(request): Json<EnvironmentReleaseApiRequest>,
) -> Result<Json<EnvironmentResponse>, ApiError> {
    let service = EnvironmentService::new(&state.db);
    let environment = service
        .release(EnvironmentReleaseRequest {
            project: project_id,
            slug,
            git_branch: request.git_branch,
            git_commit_sha: request.git_commit_sha,
        })
        .await?;
    info!(
        project_id = %environment.project_ref,
        environment = %environment.slug,
        git_commit_sha = %environment.git_commit_sha.as_deref().unwrap_or(""),
        "released environment"
    );
    Ok(Json(EnvironmentResponse { environment }))
}

pub(super) async fn environment_pause(
    State(state): State<AppState>,
    Path((project_id, slug)): Path<(String, String)>,
) -> Result<Json<EnvironmentResponse>, ApiError> {
    let environment = state
        .db
        .set_environment_auto_reconcile(&project_id, &slug, false)
        .await?;
    info!(
        project_id = %environment.project_ref,
        environment = %environment.slug,
        "paused automatic reconciliation"
    );
    Ok(Json(EnvironmentResponse { environment }))
}

pub(super) async fn environment_resume(
    State(state): State<AppState>,
    Path((project_id, slug)): Path<(String, String)>,
) -> Result<Json<EnvironmentResponse>, ApiError> {
    let environment = state
        .db
        .set_environment_auto_reconcile(&project_id, &slug, true)
        .await?;
    info!(
        project_id = %environment.project_ref,
        environment = %environment.slug,
        "resumed automatic reconciliation"
    );
    Ok(Json(EnvironmentResponse { environment }))
}

#[utoipa::path(
    get,
    path = "/v1/projects/{project_id}/environments/{slug}/history",
    tag = "environments",
    params(
        ("project_id" = String, Path, description = "Project identifier"),
        ("slug" = String, Path, description = "Environment slug")
    ),
    responses(
        (status = 200, description = "Environment version history", body = EnvironmentVersionsResponse),
        (status = 404, description = "Environment not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn environment_history(
    State(state): State<AppState>,
    Path((project_id, slug)): Path<(String, String)>,
) -> Result<Json<EnvironmentVersionsResponse>, ApiError> {
    let versions = state
        .db
        .list_environment_versions(&project_id, &slug)
        .await?;
    Ok(Json(EnvironmentVersionsResponse { versions }))
}

#[utoipa::path(
    get,
    path = "/v1/projects/{project_id}/environments/{slug}/active-resources",
    tag = "environments",
    params(
        ("project_id" = String, Path, description = "Project identifier"),
        ("slug" = String, Path, description = "Environment slug"),
        ("resource_type" = Option<String>, Query, description = "Optional dbt resource type filter, e.g. model")
    ),
    responses(
        (status = 200, description = "Active selected resources for the environment", body = EnvironmentActiveResourcesResponse),
        (status = 404, description = "Environment not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn environment_active_resources(
    State(state): State<AppState>,
    Path((project_id, slug)): Path<(String, String)>,
    Query(request): Query<EnvironmentActiveResourcesApiRequest>,
) -> Result<Json<EnvironmentActiveResourcesResponse>, ApiError> {
    let resources = state
        .db
        .list_active_environment_resources(&project_id, &slug, request.resource_type.as_deref())
        .await?;
    Ok(Json(EnvironmentActiveResourcesResponse { resources }))
}

#[utoipa::path(
    post,
    path = "/v1/projects/{project_id}/environments/{slug}/source-state-events",
    tag = "environments",
    params(
        ("project_id" = String, Path, description = "Project identifier"),
        ("slug" = String, Path, description = "Environment slug")
    ),
    request_body = SourceStateEventCreateApiRequest,
    responses(
        (status = 200, description = "Created source state event", body = SourceStateEventResponse),
        (status = 404, description = "Environment not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn environment_source_state_event_create(
    State(state): State<AppState>,
    Path((project_id, slug)): Path<(String, String)>,
    Json(request): Json<SourceStateEventCreateApiRequest>,
) -> Result<Json<SourceStateEventResponse>, ApiError> {
    let service = EnvironmentService::new(&state.db);
    let event = service
        .create_source_state_event(SourceStateEventCreateRequest {
            project: project_id,
            slug,
            source_key: request.source_key,
            provider: request.provider,
            state_version: request.state_version,
            observed_at: request.observed_at,
            payload: request.payload,
        })
        .await?;
    Ok(Json(SourceStateEventResponse { event }))
}

#[utoipa::path(
    get,
    path = "/v1/projects/{project_id}/environments/{slug}/plans",
    tag = "environments",
    params(
        ("project_id" = String, Path, description = "Project identifier"),
        ("slug" = String, Path, description = "Environment slug")
    ),
    responses(
        (status = 200, description = "Environment run plans", body = EnvironmentRunPlansResponse),
        (status = 404, description = "Environment not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn environment_plan_list(
    State(state): State<AppState>,
    Path((project_id, slug)): Path<(String, String)>,
) -> Result<Json<EnvironmentRunPlansResponse>, ApiError> {
    let plans = state
        .db
        .list_environment_run_plans(&project_id, &slug)
        .await?;
    Ok(Json(EnvironmentRunPlansResponse { plans }))
}

#[utoipa::path(
    get,
    path = "/v1/plans/{plan_id}",
    tag = "environments",
    params(
        ("plan_id" = Uuid, Path, description = "Plan identifier")
    ),
    responses(
        (status = 200, description = "Environment run plan", body = EnvironmentRunPlanResponse),
        (status = 404, description = "Plan not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn environment_plan_get(
    State(state): State<AppState>,
    Path(plan_id): Path<Uuid>,
) -> Result<Json<EnvironmentRunPlanResponse>, ApiError> {
    let plan = state.db.get_environment_run_plan(plan_id).await?;
    Ok(Json(EnvironmentRunPlanResponse { plan }))
}

#[utoipa::path(
    post,
    path = "/v1/projects/{project_id}/environments/{slug}/reconcile",
    tag = "environments",
    params(
        ("project_id" = String, Path, description = "Project identifier"),
        ("slug" = String, Path, description = "Environment slug")
    ),
    request_body = EnvironmentReconcileApiRequest,
    responses(
        (status = 200, description = "Created reconciliation plan", body = EnvironmentRunPlanResponse),
        (status = 400, description = "No reconciliation work available", body = ApiErrorResponse),
        (status = 404, description = "Environment not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn environment_reconcile(
    State(state): State<AppState>,
    Path((project_id, slug)): Path<(String, String)>,
    Json(_request): Json<EnvironmentReconcileApiRequest>,
) -> Result<Json<EnvironmentRunPlanResponse>, ApiError> {
    state
        .ensure_target_manifest_for_reconcile(&project_id, &slug)
        .await?;
    let service = EnvironmentService::new(&state.db);
    let plan = service.reconcile(project_id, slug).await?;
    Ok(Json(EnvironmentRunPlanResponse { plan }))
}

#[utoipa::path(
    post,
    path = "/v1/plans/{plan_id}/admit",
    tag = "environments",
    params(
        ("plan_id" = Uuid, Path, description = "Plan identifier")
    ),
    responses(
        (status = 200, description = "Admitted or blocked plan", body = EnvironmentRunPlanResponse),
        (status = 404, description = "Plan not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn environment_plan_admit(
    State(state): State<AppState>,
    Path(plan_id): Path<Uuid>,
) -> Result<Json<EnvironmentRunPlanResponse>, ApiError> {
    let service = EnvironmentService::new(&state.db);
    let admission = service.admit_and_start_plan(&state, plan_id).await?;
    Ok(Json(EnvironmentRunPlanResponse {
        plan: admission.plan,
    }))
}

#[utoipa::path(
    post,
    path = "/v1/projects/{project_id}/environments/{slug}/rollback",
    tag = "environments",
    params(
        ("project_id" = String, Path, description = "Project identifier"),
        ("slug" = String, Path, description = "Environment slug")
    ),
    request_body = EnvironmentRollbackApiRequest,
    responses(
        (status = 200, description = "Rolled back environment", body = EnvironmentResponse),
        (status = 400, description = "Invalid request", body = ApiErrorResponse),
        (status = 404, description = "Environment or version not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn environment_rollback(
    State(state): State<AppState>,
    Path((project_id, slug)): Path<(String, String)>,
    Json(request): Json<EnvironmentRollbackApiRequest>,
) -> Result<Json<EnvironmentResponse>, ApiError> {
    let service = EnvironmentService::new(&state.db);
    let environment = service
        .rollback(EnvironmentRollbackRequest {
            project: project_id,
            slug,
            version_id: request.version_id,
        })
        .await?;
    info!(
        project_id = %environment.project_ref,
        environment = %environment.slug,
        git_commit_sha = %environment.git_commit_sha.as_deref().unwrap_or(""),
        "rolled back environment"
    );
    Ok(Json(EnvironmentResponse { environment }))
}
