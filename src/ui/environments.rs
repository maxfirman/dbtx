use super::*;

pub(super) async fn environment_detail(
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

pub(super) async fn environment_detail_panel(
    State(state): State<AppState>,
    Path((project_id, slug)): Path<(String, String)>,
) -> Result<Html<String>, UiError> {
    let panel = read_models::load_environment_panel(&state, &project_id, &slug).await?;
    render_template(&panel)
}

#[derive(Debug, Deserialize)]
pub(super) struct EnvironmentReleaseForm {
    git_branch: Option<String>,
    git_commit_sha: String,
}

pub(super) async fn environment_release(
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

pub(super) async fn environment_reconcile(
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

pub(super) async fn ui_environment_pause(
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

pub(super) async fn ui_environment_resume(
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

pub(super) async fn environment_plan_admit(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((project_id, slug, plan_id)): Path<(String, String, Uuid)>,
) -> Result<Response, UiError> {
    let service = EnvironmentService::new(state.db());
    service.admit_and_start_plan(&state, plan_id).await?;

    if is_htmx(&headers) {
        return environment_detail_panel(State(state), Path((project_id, slug)))
            .await
            .map(IntoResponse::into_response);
    }

    Ok(Redirect::to(&format!("/ui/projects/{project_id}/environments/{slug}")).into_response())
}

#[derive(Debug, Deserialize)]
pub(super) struct EnvironmentRollbackForm {
    version_id: i64,
}

pub(super) async fn environment_rollback(
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

pub(super) async fn build_environment_run_plan_views(
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
