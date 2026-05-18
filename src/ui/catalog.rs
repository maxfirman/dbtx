use super::*;

#[derive(Debug, Default)]
pub(super) struct ModelListQuery {
    pub(super) project_id: Option<String>,
    pub(super) environment_slug: Option<String>,
    pub(super) resource_type: Vec<String>,
}

pub(super) fn parse_catalog_filter_query(raw_query: Option<&str>) -> ModelListQuery {
    let mut query = ModelListQuery::default();
    let Some(raw) = raw_query else { return query };
    let Ok(url) = reqwest::Url::parse(&format!("http://localhost/ui/catalog?{raw}")) else {
        return query;
    };
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "project_id" => query.project_id = Some(value.into_owned()),
            "environment_slug" => query.environment_slug = Some(value.into_owned()),
            "resource_type" => query.resource_type.push(value.into_owned()),
            _ => {}
        }
    }
    query
}

pub(super) async fn models_index(
    State(state): State<AppState>,
    RawQuery(raw_query): RawQuery,
) -> Result<impl IntoResponse, UiError> {
    let query = parse_catalog_filter_query(raw_query.as_deref());
    let page = read_models::load_catalog_page(&state, query).await?;
    render_template(&page).map(|html| html.into_response())
}

#[derive(Debug, Default, Deserialize)]
pub(super) struct ModelTabQuery {
    tab: Option<String>,
    depth: Option<i32>,
    direction: Option<String>,
}

pub(super) async fn model_detail(
    State(state): State<AppState>,
    Path((project_id, env_slug, unique_id)): Path<(String, String, String)>,
    Query(query): Query<ModelTabQuery>,
) -> Result<Html<String>, UiError> {
    let db = state.db();
    let project = db.get_project_by_project_id(&project_id).await?;
    let env = db.get_environment(&project.project_id, &env_slug).await?;
    let unique_id = urlencoding::decode(&unique_id)
        .unwrap_or_default()
        .to_string();
    let resource_type = resource_type_from_unique_id(&unique_id).to_string();

    let tab = query.tab.as_deref().unwrap_or("overview");
    let base = format!(
        "/ui/catalog/{}/{}/{}",
        project_id,
        env_slug,
        urlencoding::encode(&unique_id)
    );

    let valid_tabs: &[&str] = match resource_type.as_str() {
        "source" => &["overview", "lineage"],
        "test" => &["overview", "invocations"],
        "seed" => &["overview", "invocations", "lineage", "history"],
        "snapshot" => &["overview", "code", "invocations", "lineage", "history"],
        _ => &[
            "overview",
            "code",
            "invocations",
            "lineage",
            "tests",
            "history",
        ],
    };
    let tab = if valid_tabs.contains(&tab) {
        tab
    } else {
        "overview"
    };

    let tab_content_html = render_tab(db, &project, &env, &unique_id, tab, &base, &query).await?;

    let detail = db.get_model_detail(project.id, env.id, &unique_id).await?;
    let model_name = detail
        .latest_manifest_node
        .as_ref()
        .and_then(|n| n.get("name").and_then(Value::as_str))
        .unwrap_or(&unique_id)
        .to_string();

    let tabs = valid_tabs
        .iter()
        .map(|&t| {
            let extra = if t == "lineage" {
                let d = query.depth.unwrap_or(2);
                let dir = query.direction.as_deref().unwrap_or("both");
                format!("&depth={d}&direction={dir}")
            } else {
                String::new()
            };
            ModelTabView {
                label: match t {
                    "overview" => "Overview",
                    "code" => "Code",
                    "invocations" => "Invocations",
                    "lineage" => "Lineage",
                    "tests" => "Tests",
                    "history" => "History",
                    _ => t,
                },
                url: format!("{base}?tab={t}{extra}"),
                partial_url: format!("{base}/tab?tab={t}{extra}"),
                active: t == tab,
            }
        })
        .collect();

    let title: &'static str = match resource_type.as_str() {
        "source" => "Source",
        "seed" => "Seed",
        "test" => "Test",
        "snapshot" => "Snapshot",
        _ => "Model",
    };

    render_template(&ModelDetailTemplate {
        title,
        project_id: project_id.clone(),
        project_name: project.project_name.clone(),
        environment_slug: env_slug.clone(),
        model_name,
        unique_id,
        resource_type: resource_type.to_string(),
        project_mode: project.mode.clone(),
        tabs,
        tab_content_html,
    })
}

pub(super) async fn model_tab(
    State(state): State<AppState>,
    Path((project_id, env_slug, unique_id)): Path<(String, String, String)>,
    Query(query): Query<ModelTabQuery>,
) -> Result<Html<String>, UiError> {
    let db = state.db();
    let project = db.get_project_by_project_id(&project_id).await?;
    let env = db.get_environment(&project.project_id, &env_slug).await?;
    let unique_id = urlencoding::decode(&unique_id)
        .unwrap_or_default()
        .to_string();
    let tab = query.tab.as_deref().unwrap_or("overview");
    let base = format!(
        "/ui/catalog/{}/{}/{}",
        project_id,
        env_slug,
        urlencoding::encode(&unique_id)
    );
    let html = render_tab(db, &project, &env, &unique_id, tab, &base, &query).await?;
    Ok(Html(html))
}

async fn render_tab(
    db: &crate::db::Db,
    project: &crate::db::ProjectRecord,
    env: &crate::db::EnvironmentRecord,
    unique_id: &str,
    tab: &str,
    base: &str,
    query: &ModelTabQuery,
) -> Result<String, UiError> {
    match tab {
        "overview" => render_overview_tab(db, project, env, unique_id, base).await,
        "code" => render_code_tab(db, project, env, unique_id).await,
        "invocations" => render_invocations_tab(db, project, env, unique_id).await,
        "lineage" => render_lineage_tab(db, project, env, unique_id, base, query).await,
        "tests" => render_tests_tab(db, project, env, unique_id, base).await,
        "history" => render_history_tab(db, project, env, unique_id, base).await,
        _ => render_overview_tab(db, project, env, unique_id, base).await,
    }
}

async fn render_overview_tab(
    db: &crate::db::Db,
    project: &crate::db::ProjectRecord,
    env: &crate::db::EnvironmentRecord,
    unique_id: &str,
    base: &str,
) -> Result<String, UiError> {
    let detail = db.get_model_detail(project.id, env.id, unique_id).await?;
    let node = detail.latest_manifest_node.as_ref();
    let promoted = detail.promoted_manifest_node.as_ref();

    let empty = Value::Object(Default::default());
    let n = node.unwrap_or(&empty);
    let status = detail.status.as_deref().unwrap_or("unknown");
    let poll_url = format!("{base}/tab?tab=overview");
    let resource_type = resource_type_from_unique_id(unique_id);
    let node_name = extract_str(n, "name");

    let columns_obj = n.get("columns").and_then(Value::as_object);
    let columns: Vec<ModelColumnView> = columns_obj
        .into_iter()
        .flat_map(|cols| cols.values())
        .map(|col| ModelColumnView {
            name: extract_str(col, "name"),
            data_type: extract_str(col, "data_type"),
            description: extract_str(col, "description"),
        })
        .collect();

    match resource_type {
        "source" => {
            let lineage = build_overview_lineage(
                db,
                project,
                env,
                unique_id,
                &node_name,
                resource_type,
                status,
            )
            .await?;
            let freshness = n
                .get("freshness")
                .map(|v| v.to_string())
                .unwrap_or_default();
            SourceOverviewTemplate {
                description: extract_str(n, "description"),
                database: extract_str(n, "database"),
                schema: extract_str(n, "schema"),
                loader: extract_str(n, "loader"),
                identifier: extract_str(n, "identifier"),
                freshness,
                columns,
                status: status.to_string(),
                status_class: model_status_class(status).to_string(),
                lineage,
                poll_url,
            }
            .render()
            .map_err(|e| UiError(AppError::Internal(e.to_string())))
        }
        "seed" => {
            let lineage = build_overview_lineage(
                db,
                project,
                env,
                unique_id,
                &node_name,
                resource_type,
                status,
            )
            .await?;
            SeedOverviewTemplate {
                description: extract_str(n, "description"),
                file_path: extract_str(n, "original_file_path"),
                package_name: extract_str(n, "package_name"),
                database: extract_str(n, "database"),
                schema: extract_str(n, "schema"),
                alias: extract_str(n, "alias"),
                columns,
                status: status.to_string(),
                status_class: model_status_class(status).to_string(),
                lineage,
                poll_url,
            }
            .render()
            .map_err(|e| UiError(AppError::Internal(e.to_string())))
        }
        "test" => {
            let config = n
                .get("config")
                .cloned()
                .unwrap_or(Value::Object(Default::default()));
            let test_type = config
                .get("test_metadata")
                .and_then(|tm| tm.get("name"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let severity = config
                .get("severity")
                .and_then(Value::as_str)
                .unwrap_or("ERROR")
                .to_string();
            let depends_on_nodes = n
                .get("depends_on")
                .and_then(|d| d.get("nodes"))
                .and_then(Value::as_array);
            let base_url = format!("/ui/catalog/{}/{}", project.project_id, env.slug);
            let depends_on: Vec<TestDependsOnView> = depends_on_nodes
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(|uid| {
                    let name = uid.rsplit('.').next().unwrap_or(uid).to_string();
                    TestDependsOnView {
                        name,
                        unique_id: uid.to_string(),
                        detail_url: format!("{}/{}", base_url, urlencoding::encode(uid)),
                    }
                })
                .collect();
            TestOverviewTemplate {
                description: extract_str(n, "description"),
                test_type,
                severity,
                depends_on,
                status: status.to_string(),
                status_class: model_status_class(status).to_string(),
                poll_url,
            }
            .render()
            .map_err(|e| UiError(AppError::Internal(e.to_string())))
        }
        "snapshot" => {
            let lineage = build_overview_lineage(
                db,
                project,
                env,
                unique_id,
                &node_name,
                resource_type,
                status,
            )
            .await?;
            let config = n
                .get("config")
                .cloned()
                .unwrap_or(Value::Object(Default::default()));
            SnapshotOverviewTemplate {
                description: extract_str(n, "description"),
                strategy: config
                    .get("strategy")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                unique_key: config
                    .get("unique_key")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                updated_at_col: config
                    .get("updated_at")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                database: extract_str(n, "database"),
                schema: extract_str(n, "schema"),
                alias: extract_str(n, "alias"),
                file_path: extract_str(n, "original_file_path"),
                package_name: extract_str(n, "package_name"),
                columns,
                raw_code: n
                    .get("raw_code")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                status: status.to_string(),
                status_class: model_status_class(status).to_string(),
                lineage,
                poll_url,
            }
            .render()
            .map_err(|e| UiError(AppError::Internal(e.to_string())))
        }
        _ => {
            // model (default)
            let latest_checksum = node.and_then(|n| {
                n.get("checksum")
                    .and_then(|c| c.get("checksum"))
                    .and_then(Value::as_str)
            });
            let promoted_checksum = promoted.and_then(|n| {
                n.get("checksum")
                    .and_then(|c| c.get("checksum"))
                    .and_then(Value::as_str)
            });
            let is_stale = match (latest_checksum, promoted_checksum) {
                (Some(l), Some(p)) => l != p,
                (Some(_), None) => true,
                _ => false,
            };
            let tags: Vec<String> = n
                .get("tags")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(String::from)
                .collect();
            let materialized = n
                .get("config")
                .and_then(|c| c.get("materialized"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let promoted_raw_code = if is_stale {
                promoted
                    .and_then(|p| p.get("raw_code").and_then(Value::as_str))
                    .unwrap_or("")
                    .to_string()
            } else {
                String::new()
            };
            let lineage =
                build_overview_lineage(db, project, env, unique_id, &node_name, "model", status)
                    .await?;
            ModelOverviewTemplate {
                description: extract_str(n, "description"),
                materialized,
                database: extract_str(n, "database"),
                schema: extract_str(n, "schema"),
                alias: extract_str(n, "alias"),
                file_path: extract_str(n, "original_file_path"),
                package_name: extract_str(n, "package_name"),
                tags,
                status: status.to_string(),
                status_class: model_status_class(status).to_string(),
                columns,
                promoted_raw_code,
                is_stale,
                poll_url,
                lineage,
            }
            .render()
            .map_err(|e| UiError(AppError::Internal(e.to_string())))
        }
    }
}

async fn render_code_tab(
    db: &crate::db::Db,
    project: &crate::db::ProjectRecord,
    env: &crate::db::EnvironmentRecord,
    unique_id: &str,
) -> Result<String, UiError> {
    let detail = db.get_model_detail(project.id, env.id, unique_id).await?;
    let node = detail.latest_manifest_node.as_ref();
    let empty = Value::Object(Default::default());
    let n = node.unwrap_or(&empty);
    let raw_code = n
        .get("raw_code")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let compiled_code = n
        .get("compiled_code")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let raw_code_html = if raw_code.is_empty() {
        String::new()
    } else {
        highlight_sql(&raw_code)
    };
    let compiled_code_html = if compiled_code.is_empty() {
        String::new()
    } else {
        highlight_sql(&compiled_code)
    };
    ModelCodeTemplate {
        raw_code,
        compiled_code,
        raw_code_html,
        compiled_code_html,
    }
    .render()
    .map_err(|e| UiError(AppError::Internal(e.to_string())))
}

async fn render_invocations_tab(
    db: &crate::db::Db,
    project: &crate::db::ProjectRecord,
    env: &crate::db::EnvironmentRecord,
    unique_id: &str,
) -> Result<String, UiError> {
    let execs = db
        .get_model_node_executions(project.id, env.id, unique_id, 50)
        .await?;
    let executions = execs.iter().map(build_exec_view).collect();
    ModelInvocationsTemplate { executions }
        .render()
        .map_err(|e| UiError(AppError::Internal(e.to_string())))
}

fn build_exec_view(e: &crate::db::ModelNodeExecutionRecord) -> ModelExecView {
    let status = e.status.as_deref().unwrap_or("unknown");
    ModelExecView {
        invocation_id: e.invocation_id.map(|id| id.to_string()).unwrap_or_default(),
        invocation_url: e
            .invocation_id
            .map(|id| format!("/ui/invocations/{id}"))
            .unwrap_or_default(),
        command: e.command.clone(),
        status: status.to_string(),
        status_class: model_status_class(status).to_string(),
        started_at: fmt_opt_time(e.started_at),
        duration: fmt_duration(e.execution_time_seconds),
        git_commit_sha: e
            .git_commit_sha
            .as_deref()
            .map(short_hash)
            .unwrap_or_default(),
    }
}

async fn render_lineage_tab(
    db: &crate::db::Db,
    project: &crate::db::ProjectRecord,
    env: &crate::db::EnvironmentRecord,
    unique_id: &str,
    base: &str,
    query: &ModelTabQuery,
) -> Result<String, UiError> {
    let depth = query.depth.unwrap_or(2).clamp(1, 5);
    let direction = query.direction.as_deref().unwrap_or("both");
    let lineage = db
        .get_model_lineage(project.id, env.id, unique_id, depth, direction)
        .await?;

    let base_url = format!("/ui/catalog/{}/{}", project.project_id, env.slug);

    let test_ids: std::collections::HashSet<&str> = lineage
        .nodes
        .iter()
        .filter(|n| n.resource_type.as_deref() == Some("test"))
        .map(|n| n.unique_id.as_str())
        .collect();
    let mut test_counts: std::collections::HashMap<&str, (u32, u32)> =
        std::collections::HashMap::new();
    for (parent, child) in &lineage.edges {
        if test_ids.contains(child.as_str())
            && let Some(tn) = lineage.nodes.iter().find(|n| n.unique_id == *child)
        {
            let e = test_counts.entry(parent.as_str()).or_insert((0, 0));
            match tn.status.as_deref().and_then(NodeExecutionStatus::parse) {
                Some(NodeExecutionStatus::Pass | NodeExecutionStatus::Success) => e.0 += 1,
                Some(NodeExecutionStatus::Fail | NodeExecutionStatus::Error) => e.1 += 1,
                _ => {}
            }
        }
    }

    let visible_ids: Vec<String> = lineage
        .nodes
        .iter()
        .filter(|n| !test_ids.contains(n.unique_id.as_str()))
        .map(|n| n.unique_id.clone())
        .collect();
    let reconcile_states = db
        .load_node_reconciliation_state(project.id, env.id, &visible_ids)
        .await?;
    let reconcile_map: std::collections::HashMap<&str, &crate::db::NodeReconcileState> =
        reconcile_states
            .iter()
            .map(|s| (s.unique_id.as_str(), s))
            .collect();

    let nodes_json: Vec<Value> = lineage
        .nodes
        .iter()
        .filter(|n| !test_ids.contains(n.unique_id.as_str()))
        .map(|n| {
            let (pass, fail) = test_counts
                .get(n.unique_id.as_str())
                .copied()
                .unwrap_or((0, 0));
            let reconcile = reconcile_map.get(n.unique_id.as_str());
            serde_json::json!({
                "id": n.unique_id,
                "data": {
                    "label": n.name.as_deref().unwrap_or(&n.unique_id),
                    "name": n.name,
                    "resource_type": n.resource_type,
                    "status": n.status,
                    "materialized": n.materialized,
                    "package_name": n.package_name,
                    "testsPassing": pass,
                    "testsFailing": fail,
                    "reconcileState": {
                        "code": reconcile.map(|r| r.code_state.as_str()).unwrap_or("unknown"),
                        "codeTooltip": reconcile.map(|r| r.code_tooltip.as_str()).unwrap_or(""),
                        "source": reconcile.map(|r| r.source_state.as_str()).unwrap_or("no_sources"),
                        "sourceTooltip": reconcile.map(|r| r.source_tooltip.as_str()).unwrap_or(""),
                    }
                }
            })
        })
        .collect();

    let edges_json: Vec<Value> = lineage
        .edges
        .iter()
        .filter(|(s, t)| !test_ids.contains(s.as_str()) && !test_ids.contains(t.as_str()))
        .map(|(src, tgt)| {
            serde_json::json!({
                "id": format!("{src}->{tgt}"),
                "source": src,
                "target": tgt,
            })
        })
        .collect();

    let lineage_data = serde_json::json!({
        "nodes": nodes_json,
        "edges": edges_json,
        "currentNodeId": unique_id,
        "baseUrl": base_url,
        "depth": depth,
        "direction": direction,
    });

    ModelLineageTemplate {
        lineage_json: lineage_data.to_string(),
        depth_options: (1..=5).map(|d| (d, d == depth)).collect(),
        direction: direction.to_string(),
        partial_url: format!("{base}/tab"),
        node_count: nodes_json.len(),
        project_id: project.project_id.clone(),
        environment_slug: env.slug.clone(),
        model_selector: nodes_json
            .iter()
            .filter_map(|n| n.get("id").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join(" "),
        project_mode: project.mode.clone(),
    }
    .render()
    .map_err(|e| UiError(AppError::Internal(e.to_string())))
}

async fn render_tests_tab(
    db: &crate::db::Db,
    project: &crate::db::ProjectRecord,
    env: &crate::db::EnvironmentRecord,
    unique_id: &str,
    base: &str,
) -> Result<String, UiError> {
    let raw_tests = db.get_model_tests(project.id, env.id, unique_id).await?;
    let tests: Vec<ModelTestView> = raw_tests
        .iter()
        .enumerate()
        .map(|(i, t)| {
            let status = t.status.as_deref().unwrap_or("unknown");
            ModelTestView {
                index: i,
                unique_id: t.unique_id.clone(),
                name: t.name.clone().unwrap_or_else(|| t.unique_id.clone()),
                test_type: t.test_type.clone().unwrap_or_default(),
                status: status.to_string(),
                status_class: model_status_class(status).to_string(),
                finished_at: fmt_opt_time(t.finished_at),
                history_url: format!("{base}/test-history/{}", urlencoding::encode(&t.unique_id)),
                detail_url: format!(
                    "/ui/catalog/{}/{}/{}",
                    project.project_id,
                    env.slug,
                    urlencoding::encode(&t.unique_id)
                ),
            }
        })
        .collect();
    let all_test_selector = raw_tests
        .iter()
        .map(|t| t.unique_id.as_str())
        .collect::<Vec<_>>()
        .join(" ");
    let test_count = tests.len();
    ModelTestsTemplate {
        tests,
        project_id: project.project_id.clone(),
        environment_slug: env.slug.clone(),
        project_mode: project.mode.clone(),
        all_test_selector,
        test_count,
    }
    .render()
    .map_err(|e| UiError(AppError::Internal(e.to_string())))
}

async fn render_history_tab(
    db: &crate::db::Db,
    project: &crate::db::ProjectRecord,
    env: &crate::db::EnvironmentRecord,
    unique_id: &str,
    base: &str,
) -> Result<String, UiError> {
    let history = db.get_model_history(project.id, env.id, unique_id).await?;
    let entries: Vec<ModelHistoryEntryView> = history
        .iter()
        .enumerate()
        .map(|(i, h)| {
            let sha = h.git_commit_sha.as_deref().unwrap_or("");
            let prev_run_id = if i + 1 < history.len() {
                Some(history[i + 1].run_id)
            } else {
                None
            };
            ModelHistoryEntryView {
                index: i,
                git_commit_sha: short_hash(sha),
                git_url: git_commit_url(h.git_repo_url.as_deref(), h.git_commit_sha.as_deref()),
                started_at: h.started_at.format("%Y-%m-%d %H:%M").to_string(),
                checksum_short: h.checksum.as_deref().map(short_hash).unwrap_or_default(),
                prev_checksum: h.prev_checksum.clone().unwrap_or_default(),
                prev_checksum_short: h
                    .prev_checksum
                    .as_deref()
                    .map(short_hash)
                    .unwrap_or_default(),
                diff_url: format!(
                    "{base}/history-diff?run_id={}&prev_run_id={}",
                    h.run_id,
                    prev_run_id.map(|id| id.to_string()).unwrap_or_default()
                ),
            }
        })
        .collect();
    ModelHistoryTemplate { entries }
        .render()
        .map_err(|e| UiError(AppError::Internal(e.to_string())))
}

pub(super) async fn model_test_history(
    State(state): State<AppState>,
    Path((project_id, env_slug, _unique_id, test_unique_id)): Path<(
        String,
        String,
        String,
        String,
    )>,
) -> Result<Html<String>, UiError> {
    let db = state.db();
    let project = db.get_project_by_project_id(&project_id).await?;
    let env = db.get_environment(&project.project_id, &env_slug).await?;
    let test_uid = urlencoding::decode(&test_unique_id)
        .unwrap_or_default()
        .to_string();
    let execs = db
        .get_model_node_executions(project.id, env.id, &test_uid, 20)
        .await?;
    let executions = execs.iter().map(build_exec_view).collect();
    render_template(&ModelTestHistoryTemplate { executions })
}

#[derive(Debug, Deserialize)]
pub(super) struct HistoryDiffQuery {
    run_id: Uuid,
    prev_run_id: Option<Uuid>,
}

pub(super) async fn model_history_diff(
    State(state): State<AppState>,
    Path((_project_id, _env_slug, unique_id)): Path<(String, String, String)>,
    Query(query): Query<HistoryDiffQuery>,
) -> Result<Html<String>, UiError> {
    let db = state.db();
    let unique_id = urlencoding::decode(&unique_id)
        .unwrap_or_default()
        .to_string();
    let new_code = db
        .get_model_history_raw_code(query.run_id, &unique_id)
        .await?
        .unwrap_or_default();
    let old_code = match query.prev_run_id {
        Some(prev) if prev != Uuid::nil() => db
            .get_model_history_raw_code(prev, &unique_id)
            .await?
            .unwrap_or_default(),
        _ => String::new(),
    };
    let diff_lines = compute_diff(&old_code, &new_code);
    render_template(&ModelHistoryDiffTemplate { diff_lines })
}

fn extract_str(v: &Value, key: &str) -> String {
    v.get(key).and_then(Value::as_str).unwrap_or("").to_string()
}

async fn build_overview_lineage(
    db: &crate::db::Db,
    project: &crate::db::ProjectRecord,
    env: &crate::db::EnvironmentRecord,
    unique_id: &str,
    current_name: &str,
    current_resource_type: &str,
    current_status: &str,
) -> Result<OverviewLineageView, UiError> {
    let lineage = db
        .get_model_lineage(project.id, env.id, unique_id, 1, "both")
        .await?;
    let base_url = format!("/ui/catalog/{}/{}", project.project_id, env.slug);

    let make_node = |n: &crate::db::LineageNodeRecord| {
        let status = n.status.as_deref().unwrap_or("unknown");
        OverviewLineageNodeView {
            name: n.name.clone().unwrap_or_else(|| n.unique_id.clone()),
            resource_type: n.resource_type.clone().unwrap_or_default(),
            status: status.to_string(),
            status_class: model_status_class(status).to_string(),
            detail_url: format!("{}/{}", base_url, urlencoding::encode(&n.unique_id)),
        }
    };

    let parents: Vec<OverviewLineageNodeView> = lineage
        .edges
        .iter()
        .filter(|(_, child)| child == unique_id)
        .filter_map(|(parent, _)| lineage.nodes.iter().find(|n| n.unique_id == *parent))
        .map(make_node)
        .collect();

    let children: Vec<OverviewLineageNodeView> = lineage
        .edges
        .iter()
        .filter(|(parent, _)| parent == unique_id)
        .filter_map(|(_, child)| lineage.nodes.iter().find(|n| n.unique_id == *child))
        .map(make_node)
        .collect();

    let has_lineage = !parents.is_empty() || !children.is_empty();

    Ok(OverviewLineageView {
        parents,
        current: OverviewLineageNodeView {
            name: current_name.to_string(),
            resource_type: current_resource_type.to_string(),
            status: current_status.to_string(),
            status_class: model_status_class(current_status).to_string(),
            detail_url: String::new(),
        },
        children,
        has_lineage,
    })
}
