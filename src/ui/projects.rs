use super::*;

pub(super) async fn projects_index(State(state): State<AppState>) -> Result<Html<String>, UiError> {
    let db = state.db();
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

pub(super) async fn project_create_modal() -> Result<Html<String>, UiError> {
    render_template(&ProjectCreateModalTemplate {
        draft: ProjectDraftForm::default(),
        error: None,
    })
}

pub(super) async fn environment_create_modal(
    State(state): State<AppState>,
    Path(project_id): Path<String>,
) -> Result<Html<String>, UiError> {
    let service = EnvironmentService::new(state.db());
    let draft = service.create_draft(project_id).await?;
    start_environment_draft_prepare(&state, draft.id).await?;
    render_environment_draft_modal(
        state.db(),
        &state.db().get_environment_draft(draft.id).await?,
    )
    .await
}

pub(super) async fn project_delete_modal(
    State(state): State<AppState>,
    Path(project_id): Path<String>,
) -> Result<Html<String>, UiError> {
    let project = state.db().get_project_by_project_id(&project_id).await?;
    render_template(&ProjectDeleteModalTemplate {
        project: project_summary_view(&project),
        error: None,
    })
}

pub(super) async fn environment_draft_status(
    State(state): State<AppState>,
    Path(draft_id): Path<Uuid>,
) -> Result<Html<String>, UiError> {
    render_environment_draft_modal(
        state.db(),
        &state.db().get_environment_draft(draft_id).await?,
    )
    .await
}

pub(super) async fn environment_draft_branch_refresh(
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
    render_environment_draft_modal(
        state.db(),
        &state.db().get_environment_draft(draft_id).await?,
    )
    .await
}

pub(super) async fn environment_draft_validate(
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
    render_environment_draft_modal(
        state.db(),
        &state.db().get_environment_draft(draft_id).await?,
    )
    .await
}

pub(super) async fn environment_draft_confirm(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(draft_id): Path<Uuid>,
) -> Result<impl IntoResponse, UiError> {
    let environment = state.db().confirm_environment_draft(draft_id).await?;
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

pub(super) async fn project_draft_create(
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
    let draft = state.db().get_project_draft(draft.id).await?;
    render_project_draft_fragment(&draft, None, true)
}

pub(super) async fn project_draft_status(
    State(state): State<AppState>,
    Path(draft_id): Path<Uuid>,
) -> Result<Html<String>, UiError> {
    let draft = state.db().get_project_draft(draft_id).await?;
    render_project_draft_fragment(
        &draft,
        None,
        !is_terminal_project_draft_status(draft.status),
    )
}

pub(super) async fn project_draft_confirm(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(draft_id): Path<Uuid>,
) -> Result<impl IntoResponse, UiError> {
    state.db().confirm_project_draft(draft_id).await?;
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

pub(super) async fn project_delete(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(project_id): Path<String>,
) -> Result<impl IntoResponse, UiError> {
    if let Err(err) = state.db().delete_project(&project_id).await {
        let project = state.db().get_project_by_project_id(&project_id).await?;
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
    state
        .start_project_draft_validation_invocation(prepared)
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
    state
        .start_environment_draft_prepare_invocation(prepared)
        .await
        .map_err(UiError::from)?;
    Ok(())
}

async fn start_environment_draft_validation(
    state: &AppState,
    prepared: crate::services::EnvironmentDraftValidationPrepared,
) -> Result<(), UiError> {
    state
        .start_environment_draft_validation_invocation(prepared)
        .await
        .map_err(UiError::from)?;
    Ok(())
}

pub(super) fn render_project_draft_fragment(
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

pub(super) async fn render_environment_draft_modal(
    db: &crate::db::Db,
    draft: &crate::db::EnvironmentDraftRecord,
) -> Result<Html<String>, UiError> {
    let project = db.get_project_by_id(draft.project_id).await?;
    let draft_view = environment_draft_view(&project, draft)?;
    render_template(&EnvironmentCreateModalTemplate {
        project: project_summary_view(&project),
        draft: draft_view,
    })
}

pub(super) fn is_terminal_project_draft_status(status: DraftStatus) -> bool {
    matches!(status, DraftStatus::Validated | DraftStatus::Failed)
}
