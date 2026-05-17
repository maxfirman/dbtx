//! Render-ready UI read models.

use super::*;

pub(super) async fn load_environment_panel(
    state: &AppState,
    project_id: &str,
    slug: &str,
) -> Result<EnvironmentPanelTemplate, UiError> {
    let db = state.db();
    db.require_current_schema().await?;
    let project = db.get_project_by_project_id(project_id).await?;
    let environment = db
        .list_environments(&project.project_id)
        .await?
        .into_iter()
        .find(|environment| environment.slug == slug)
        .ok_or_else(|| {
            UiError(AppError::EnvironmentNotFound(
                project.project_id.clone(),
                slug.to_string(),
            ))
        })?;
    let history = db
        .list_environment_versions(&project.project_id, slug)
        .await?;
    let actual_state = db
        .get_environment_actual_state(&project.project_id, slug)
        .await?;
    let preparation = db
        .get_environment_reconcile_preparation(&project.project_id, slug)
        .await?;
    let active_resources = db
        .list_active_environment_resources(&project.project_id, slug, None)
        .await?;
    let plans = db
        .list_environment_run_plans(&project.project_id, slug)
        .await?;
    let plan_views =
        build_environment_run_plan_views(db, &project.project_id, slug, &plans).await?;

    Ok(EnvironmentPanelTemplate {
        project: project_summary_view(&project),
        environment: environment_detail_view(&environment),
        summary: environment_reconciliation_summary_view(
            &environment,
            &actual_state,
            preparation.as_ref(),
            &plans,
            active_resources.len(),
        ),
        actual_state: environment_actual_state_view(&actual_state),
        preparation: preparation
            .as_ref()
            .map(environment_reconcile_preparation_view),
        active_resources: active_resources
            .iter()
            .map(environment_active_resource_view)
            .collect(),
        plans: plan_views,
        versions: history.iter().map(environment_version_view).collect(),
        is_remote: project.mode == "remote",
        panel_url: format!("/ui/projects/{project_id}/environments/{slug}/panel"),
        reconcile_url: format!("/ui/projects/{project_id}/environments/{slug}/reconcile"),
        pause_url: format!("/ui/projects/{project_id}/environments/{slug}/pause"),
        resume_url: format!("/ui/projects/{project_id}/environments/{slug}/resume"),
    })
}

pub(super) async fn load_catalog_page(
    state: &AppState,
    query: ModelListQuery,
) -> Result<ModelsPageTemplate, UiError> {
    let db = state.db();
    db.require_current_schema().await?;
    let projects = db.list_projects().await?;

    let resolved_project_id = query
        .project_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .or_else(|| projects.first().map(|project| project.project_id.as_str()))
        .map(String::from);

    let mut environments = Vec::new();
    let mut models = Vec::new();
    let mut resolved_environment_slug: Option<String> = None;

    if let Some(ref project_id) = resolved_project_id {
        environments = db.list_environments(project_id).await?;

        resolved_environment_slug = query
            .environment_slug
            .as_deref()
            .filter(|value| {
                !value.is_empty()
                    && environments
                        .iter()
                        .any(|environment| environment.slug == *value)
            })
            .or_else(|| {
                environments
                    .first()
                    .map(|environment| environment.slug.as_str())
            })
            .map(String::from);

        if let Some(ref slug) = resolved_environment_slug
            && let Some(environment) = environments
                .iter()
                .find(|environment| &environment.slug == slug)
            && let Some(project) = projects
                .iter()
                .find(|project| &project.project_id == project_id)
        {
            models = load_catalog_models(
                db,
                project,
                environment,
                &query.resource_type,
                project_id,
                slug,
            )
            .await?;
        }
    }

    let needs_selection = projects.is_empty();
    let filters = catalog_filters(
        &projects,
        &environments,
        resolved_project_id.as_deref(),
        resolved_environment_slug.as_deref(),
        &query.resource_type,
    );

    Ok(ModelsPageTemplate {
        title: "Catalog",
        filters,
        models,
        needs_selection,
    })
}

pub(super) async fn load_invocation_detail_page(
    state: &AppState,
    invocation_id: Uuid,
    tab: Option<&str>,
) -> Result<InvocationDetailTemplate, UiError> {
    let db = state.db();
    db.require_current_schema().await?;
    let invocation = db.get_invocation_status(invocation_id).await?;
    let tab = normalized_invocation_tab(tab);
    let base = format!("/ui/invocations/{invocation_id}");
    let tab_content_html = render_invocation_tab_content_for_db(db, invocation_id, tab).await?;

    let tabs = ["timeline", "lineage", "logs"]
        .iter()
        .map(|&candidate| InvocationTabView {
            label: match candidate {
                "timeline" => "Timeline",
                "lineage" => "Lineage",
                "logs" => "Logs",
                _ => candidate,
            },
            url: format!("{base}?tab={candidate}"),
            partial_url: format!("{base}/tab?tab={candidate}"),
            active: candidate == tab,
        })
        .collect();

    Ok(InvocationDetailTemplate {
        title: "Invocation",
        invocation: invocation_detail_view(&invocation),
        panel_url: format!("{base}/panel"),
        tabs,
        tab_content_html,
    })
}

pub(super) async fn load_invocation_detail_panel(
    state: &AppState,
    invocation_id: Uuid,
) -> Result<InvocationDetailPanelTemplate, UiError> {
    let db = state.db();
    db.require_current_schema().await?;
    let invocation = db.get_invocation_status(invocation_id).await?;
    Ok(InvocationDetailPanelTemplate {
        invocation: invocation_detail_view(&invocation),
        panel_url: format!("/ui/invocations/{invocation_id}/panel"),
    })
}

pub(super) async fn render_invocation_tab_content(
    state: &AppState,
    invocation_id: Uuid,
    tab: Option<&str>,
) -> Result<String, UiError> {
    let db = state.db();
    db.require_current_schema().await?;
    render_invocation_tab_content_for_db(db, invocation_id, normalized_invocation_tab(tab)).await
}

async fn render_invocation_tab_content_for_db(
    db: &crate::db::Db,
    invocation_id: Uuid,
    tab: &str,
) -> Result<String, UiError> {
    let sse_url = format!("/v1/invocations/{invocation_id}/events");
    match tab {
        "lineage" => render_invocation_lineage_tab(db, invocation_id).await,
        "logs" => {
            let events = db.load_invocation_events_since(invocation_id, 0).await?;
            let initial_log_sequence = events.last().map(|(seq, _)| *seq).unwrap_or(0);
            let lines = events
                .into_iter()
                .filter_map(|(_, event)| render_invocation_log_html(&event))
                .collect();
            InvocationLogsTabTemplate {
                sse_url,
                initial_log_lines: lines,
                initial_log_sequence,
            }
            .render()
            .map_err(|err| UiError(AppError::Internal(err.to_string())))
        }
        _ => InvocationTimelineTabTemplate {
            sse_url,
            timeline_api_url: format!("/v1/invocations/{invocation_id}/timeline"),
        }
        .render()
        .map_err(|err| UiError(AppError::Internal(err.to_string()))),
    }
}

async fn render_invocation_lineage_tab(
    db: &crate::db::Db,
    invocation_id: Uuid,
) -> Result<String, UiError> {
    let lineage = db.get_invocation_lineage(invocation_id).await?;

    let mut base_url = String::new();
    if let Ok(persistence) = db
        .get_invocation_persistence(invocation_id, None, None)
        .await
        && let Some(environment_id) = persistence.environment_id
        && let Ok(environment) = db.get_environment_by_id(environment_id).await
    {
        base_url = format!(
            "/ui/catalog/{}/{}",
            environment.project_ref, environment.slug
        );
    }

    let test_ids: HashSet<&str> = lineage
        .nodes
        .iter()
        .filter(|node| node.resource_type.as_deref() == Some("test"))
        .map(|node| node.unique_id.as_str())
        .collect();
    let mut test_counts: HashMap<&str, (u32, u32)> = HashMap::new();
    for (parent, child) in &lineage.edges {
        if test_ids.contains(child.as_str())
            && let Some(test_node) = lineage.nodes.iter().find(|node| node.unique_id == *child)
        {
            let counts = test_counts.entry(parent.as_str()).or_insert((0, 0));
            match test_node
                .status
                .as_deref()
                .and_then(NodeExecutionStatus::parse)
            {
                Some(NodeExecutionStatus::Pass | NodeExecutionStatus::Success) => counts.0 += 1,
                Some(NodeExecutionStatus::Fail | NodeExecutionStatus::Error) => counts.1 += 1,
                _ => {}
            }
        }
    }

    let nodes_json: Vec<Value> = lineage
        .nodes
        .iter()
        .filter(|node| !test_ids.contains(node.unique_id.as_str()))
        .map(|node| {
            let (pass, fail) = test_counts
                .get(node.unique_id.as_str())
                .copied()
                .unwrap_or((0, 0));
            serde_json::json!({
                "id": node.unique_id,
                "data": {
                    "label": node.name.as_deref().unwrap_or(&node.unique_id),
                    "name": node.name,
                    "resource_type": node.resource_type,
                    "status": node.status,
                    "materialized": node.materialized,
                    "package_name": node.package_name,
                    "testsPassing": pass,
                    "testsFailing": fail,
                }
            })
        })
        .collect();

    let edges_json: Vec<Value> = lineage
        .edges
        .iter()
        .filter(|(source, target)| {
            !test_ids.contains(source.as_str()) && !test_ids.contains(target.as_str())
        })
        .map(|(source, target)| {
            serde_json::json!({
                "id": format!("{source}->{target}"),
                "source": source,
                "target": target,
            })
        })
        .collect();

    let lineage_data = serde_json::json!({
        "nodes": nodes_json,
        "edges": edges_json,
        "currentNodeId": "",
        "baseUrl": base_url,
        "depth": 1,
        "direction": "both",
    });

    InvocationLineageTabTemplate {
        lineage_json: lineage_data.to_string(),
    }
    .render()
    .map_err(|err| UiError(AppError::Internal(err.to_string())))
}

fn normalized_invocation_tab(tab: Option<&str>) -> &str {
    match tab {
        Some(tab @ ("timeline" | "lineage" | "logs")) => tab,
        _ => "timeline",
    }
}

async fn load_catalog_models(
    db: &crate::db::Db,
    project: &ProjectRecord,
    environment: &EnvironmentRecord,
    resource_types: &[String],
    project_id: &str,
    slug: &str,
) -> Result<Vec<ModelSummaryViewItem>, UiError> {
    let raw = db
        .list_models_for_environment(project.id, environment.id, resource_types)
        .await?;
    let model_ids: Vec<String> = raw.iter().map(|model| model.unique_id.clone()).collect();
    let reconcile_states = db
        .load_node_reconciliation_state(project.id, environment.id, &model_ids)
        .await?;
    let reconcile_map: HashMap<&str, &NodeReconcileState> = reconcile_states
        .iter()
        .map(|state| (state.unique_id.as_str(), state))
        .collect();

    Ok(raw
        .iter()
        .map(|model| {
            let reconcile_state = reconcile_map.get(model.unique_id.as_str());
            ModelSummaryViewItem {
                name: model
                    .node_name
                    .clone()
                    .unwrap_or_else(|| model.unique_id.clone()),
                node_path: model.node_path.clone().unwrap_or_default(),
                resource_type: model.resource_type.clone().unwrap_or_default(),
                package_name: model.package_name.clone().unwrap_or_default(),
                materialized: model.materialized.clone().unwrap_or_default(),
                status: model.status.clone().unwrap_or("unknown".into()),
                status_class: model_status_class(model.status.as_deref().unwrap_or("")).to_string(),
                schema: model.relation_schema.clone().unwrap_or_default(),
                finished_at: fmt_opt_time(model.finished_at),
                last_success_at: fmt_opt_time(model.last_success_at),
                detail_url: format!(
                    "/ui/catalog/{}/{}/{}",
                    project_id,
                    slug,
                    urlencoding::encode(&model.unique_id)
                ),
                code_state: reconcile_state
                    .map(|state| state.code_state.as_str())
                    .unwrap_or("unknown"),
                code_tooltip: reconcile_state
                    .map(|state| state.code_tooltip.clone())
                    .unwrap_or_default(),
                source_state: reconcile_state
                    .map(|state| state.source_state.as_str())
                    .unwrap_or("no_sources"),
                source_tooltip: reconcile_state
                    .map(|state| state.source_tooltip.clone())
                    .unwrap_or_default(),
            }
        })
        .collect())
}

fn catalog_filters(
    projects: &[ProjectRecord],
    environments: &[EnvironmentRecord],
    resolved_project_id: Option<&str>,
    resolved_environment_slug: Option<&str>,
    selected_resource_types: &[String],
) -> ModelFiltersView {
    const CATALOG_RESOURCE_TYPES: &[&str] = &["model", "source", "seed", "test", "snapshot"];

    ModelFiltersView {
        projects: projects
            .iter()
            .map(|project| ModelFilterSelectView {
                selected: resolved_project_id == Some(&project.project_id),
                value: project.project_id.clone(),
                label: project.project_name.clone(),
            })
            .collect(),
        environments: environments
            .iter()
            .map(|environment| ModelFilterSelectView {
                selected: resolved_environment_slug == Some(&environment.slug),
                value: environment.slug.clone(),
                label: environment.slug.clone(),
            })
            .collect(),
        resource_types: CATALOG_RESOURCE_TYPES
            .iter()
            .map(|&resource_type| ModelFilterSelectView {
                selected: selected_resource_types.contains(&resource_type.to_string()),
                value: resource_type.to_string(),
                label: resource_type.to_string(),
            })
            .collect(),
    }
}
