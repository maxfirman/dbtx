use super::*;

#[utoipa::path(
    patch,
    path = "/v1/projects/{project_id}",
    tag = "projects",
    params(
        ("project_id" = String, Path, description = "Project identifier")
    ),
    request_body = ProjectUpdateApiRequest,
    responses(
        (status = 200, description = "Updated project", body = ProjectResponse),
        (status = 400, description = "Invalid request", body = ApiErrorResponse),
        (status = 404, description = "Project not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn project_update(
    State(state): State<AppState>,
    Path(project_id): Path<String>,
    Json(request): Json<ProjectUpdateApiRequest>,
) -> Result<Json<ProjectResponse>, ApiError> {
    let service = ProjectService::new(&state.db);
    let project = service
        .update(ProjectUpdateRequest {
            project: project_id,
            git_repo_url: request.git_repo_url,
            project_root: request.project_root,
        })
        .await?;
    info!(project_id = %project.project_id, project_name = %project.project_name, "updated project");
    Ok(Json(ProjectResponse { project }))
}

#[utoipa::path(
    delete,
    path = "/v1/projects/{project_id}",
    tag = "projects",
    params(
        ("project_id" = String, Path, description = "Project identifier")
    ),
    responses(
        (status = 200, description = "Deleted project", body = ProjectDeleteResponse),
        (status = 404, description = "Project not found", body = ApiErrorResponse),
        (status = 409, description = "Project deletion blocked", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn project_delete(
    State(state): State<AppState>,
    Path(project_id): Path<String>,
) -> Result<Json<ProjectDeleteResponse>, ApiError> {
    state.db.delete_project(&project_id).await?;
    info!(project_id = %project_id, "deleted project");
    Ok(Json(ProjectDeleteResponse {
        deleted_project_id: project_id,
    }))
}

#[utoipa::path(
    post,
    path = "/v1/project-drafts",
    tag = "projects",
    request_body = ProjectDraftCreateApiRequest,
    responses(
        (status = 200, description = "Created project draft", body = ProjectDraftResponse),
        (status = 400, description = "Invalid request", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn project_draft_create(
    State(state): State<AppState>,
    Json(request): Json<ProjectDraftCreateApiRequest>,
) -> Result<Json<ProjectDraftResponse>, ApiError> {
    let service = ProjectService::new(&state.db);
    let draft = service
        .create_draft(ProjectCreateRequest {
            git_repo_url: request.git_repo_url,
            project_root: request.project_root,
        })
        .await?;
    Ok(Json(ProjectDraftResponse { draft }))
}

#[utoipa::path(
    get,
    path = "/v1/project-drafts/{draft_id}",
    tag = "projects",
    params(
        ("draft_id" = Uuid, Path, description = "Draft identifier")
    ),
    responses(
        (status = 200, description = "Project draft", body = ProjectDraftResponse),
        (status = 404, description = "Draft not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn project_draft_get(
    State(state): State<AppState>,
    Path(draft_id): Path<Uuid>,
) -> Result<Json<ProjectDraftResponse>, ApiError> {
    let draft = state.db.get_project_draft(draft_id).await?;
    Ok(Json(ProjectDraftResponse { draft }))
}

#[utoipa::path(
    post,
    path = "/v1/project-drafts/{draft_id}/validate",
    tag = "projects",
    params(
        ("draft_id" = Uuid, Path, description = "Draft identifier")
    ),
    responses(
        (status = 200, description = "Started draft validation", body = ProjectDraftValidateResponse),
        (status = 404, description = "Draft not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn project_draft_validate(
    State(state): State<AppState>,
    Path(draft_id): Path<Uuid>,
) -> Result<Json<ProjectDraftValidateResponse>, ApiError> {
    let prepared = ProjectService::new(&state.db)
        .prepare_draft_validation(draft_id)
        .await?;
    let invocation_id = state.start_project_draft_validation_invocation(prepared).await?;
    Ok(Json(ProjectDraftValidateResponse {
        draft: state.db.get_project_draft(draft_id).await?,
        invocation_id,
    }))
}

#[utoipa::path(
    post,
    path = "/v1/project-drafts/{draft_id}/confirm",
    tag = "projects",
    params(
        ("draft_id" = Uuid, Path, description = "Draft identifier")
    ),
    responses(
        (status = 200, description = "Confirmed project", body = ProjectResponse),
        (status = 400, description = "Draft not validated", body = ApiErrorResponse),
        (status = 404, description = "Draft not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn project_draft_confirm(
    State(state): State<AppState>,
    Path(draft_id): Path<Uuid>,
) -> Result<Json<ProjectResponse>, ApiError> {
    let project = state.db.confirm_project_draft(draft_id).await?;
    Ok(Json(ProjectResponse { project }))
}

#[utoipa::path(
    get,
    path = "/v1/projects",
    tag = "projects",
    responses(
        (status = 200, description = "Projects", body = ProjectsResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn projects_list(
    State(state): State<AppState>,
) -> Result<Json<ProjectsResponse>, ApiError> {
    let projects = state.db.list_projects().await?;
    info!(count = projects.len(), "listed projects");
    Ok(Json(ProjectsResponse { projects }))
}

pub(super) async fn project_resolve(
    State(state): State<AppState>,
    Query(query): Query<ProjectResolveQuery>,
) -> Result<Json<ProjectResolveResponse>, ApiError> {
    let project = state
        .db
        .get_project_by_repo(&query.git_repo_url, &query.project_root)
        .await?;
    Ok(Json(ProjectResolveResponse {
        project: ProjectResponse { project },
    }))
}

#[utoipa::path(
    get,
    path = "/v1/projects/{project_id}",
    tag = "projects",
    params(
        ("project_id" = String, Path, description = "Project identifier")
    ),
    responses(
        (status = 200, description = "Project", body = ProjectResponse),
        (status = 404, description = "Project not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn project_get(
    State(state): State<AppState>,
    Path(project_id): Path<String>,
) -> Result<Json<ProjectResponse>, ApiError> {
    let project = state.db.get_project_by_project_id(&project_id).await?;
    info!(project_id = %project.project_id, "loaded project");
    Ok(Json(ProjectResponse { project }))
}
