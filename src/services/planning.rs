//! Environment run-plan derivation and blocked-plan replanning.

use super::*;

pub(super) struct EnvironmentPlanDraft {
    pub reason: &'static str,
    pub input_fingerprint: String,
    pub baseline_run_id: Option<Uuid>,
    pub selection_spec: Option<String>,
    pub selected_resources: Vec<String>,
    pub source_event_id: Option<i64>,
    pub metadata: Value,
    pub code_drift: bool,
}

pub(super) async fn derive_environment_plan(
    db: &Db,
    environment: &EnvironmentRecord,
    actual_state: &EnvironmentActualStateRecord,
    source_events: &[SourceStateEventRecord],
) -> AppResult<EnvironmentPlanDraft> {
    let baseline_run_id = actual_state.last_successful_run_id;
    let code_drift = environment.git_commit_sha != actual_state.last_successful_commit_sha;

    if !code_drift && source_events.is_empty() {
        return Err(AppError::EnvironmentAlreadyReconciled);
    }

    if code_drift {
        derive_code_change_plan(
            db,
            environment,
            actual_state,
            source_events,
            baseline_run_id,
        )
        .await
    } else {
        derive_source_state_change_plan(db, environment, source_events, baseline_run_id).await
    }
}

async fn derive_code_change_plan(
    db: &Db,
    environment: &EnvironmentRecord,
    actual_state: &EnvironmentActualStateRecord,
    source_events: &[SourceStateEventRecord],
    baseline_run_id: Option<Uuid>,
) -> AppResult<EnvironmentPlanDraft> {
    let desired_commit_sha = environment
        .git_commit_sha
        .clone()
        .ok_or(AppError::ReconciliationRequiresCommitSha)?;
    let input_fingerprint =
        code_change_input_fingerprint_for_baseline(&desired_commit_sha, baseline_run_id);

    if let Some(target_manifest_run_id) = db
        .latest_manifest_run_id_for_commit(
            environment.project_id,
            environment.id,
            &desired_commit_sha,
        )
        .await?
    {
        if let Some(baseline_run_id) = baseline_run_id {
            let target_nodes = db
                .load_planning_manifest_nodes(target_manifest_run_id)
                .await?;
            let baseline_nodes = db.load_planning_manifest_nodes(baseline_run_id).await?;
            let target_edges = db.load_manifest_edges(target_manifest_run_id).await?;
            let current_nodes = db
                .load_current_node_state_for_planning(environment.project_id, environment.id)
                .await?;
            let selected_resources = plan_code_change_selected_resources(
                &baseline_nodes,
                &target_nodes,
                &target_edges,
                &current_nodes,
            );
            let selection_spec = if selected_resources.is_empty() {
                "state_modified_live"
            } else {
                "state_modified_live_plus"
            };
            return Ok(EnvironmentPlanDraft {
                reason: "code_change",
                input_fingerprint,
                baseline_run_id: Some(baseline_run_id),
                selection_spec: Some(selection_spec.to_string()),
                selected_resources,
                source_event_id: None,
                metadata: serde_json::json!({
                    "code_drift": true,
                    "actual_commit_sha": actual_state.last_successful_commit_sha,
                    "desired_commit_sha": desired_commit_sha,
                    "source_event_count": source_events.len(),
                    "target_manifest_run_id": target_manifest_run_id,
                    "planning_mode": "live_state_diff",
                }),
                code_drift: true,
            });
        }

        return Ok(EnvironmentPlanDraft {
            reason: "code_change",
            input_fingerprint,
            baseline_run_id,
            selection_spec: Some("full_graph".to_string()),
            selected_resources: db
                .list_manifest_node_unique_ids(target_manifest_run_id)
                .await?,
            source_event_id: None,
            metadata: serde_json::json!({
                "code_drift": true,
                "actual_commit_sha": actual_state.last_successful_commit_sha,
                "desired_commit_sha": desired_commit_sha,
                "source_event_count": source_events.len(),
                "target_manifest_run_id": target_manifest_run_id,
                "planning_mode": "initial_full_graph_no_baseline",
            }),
            code_drift: true,
        });
    }

    let Some(baseline_run_id) = baseline_run_id else {
        return Err(AppError::ReconciliationRequiresBaseline);
    };
    Ok(EnvironmentPlanDraft {
        reason: "code_change",
        input_fingerprint,
        baseline_run_id: Some(baseline_run_id),
        selection_spec: Some("full_graph".to_string()),
        selected_resources: db.list_manifest_node_unique_ids(baseline_run_id).await?,
        source_event_id: None,
        metadata: serde_json::json!({
            "code_drift": true,
            "actual_commit_sha": actual_state.last_successful_commit_sha,
            "desired_commit_sha": desired_commit_sha,
            "source_event_count": source_events.len(),
            "planning_mode": "full_graph_fallback_no_target_manifest",
        }),
        code_drift: true,
    })
}

async fn derive_source_state_change_plan(
    db: &Db,
    environment: &EnvironmentRecord,
    source_events: &[SourceStateEventRecord],
    baseline_run_id: Option<Uuid>,
) -> AppResult<EnvironmentPlanDraft> {
    let source_baseline_run_id = baseline_run_id.ok_or(AppError::ReconciliationRequiresBaseline)?;
    let source_keys: Vec<String> = source_events
        .iter()
        .map(|event| event.source_key.clone())
        .collect();
    let source_event_ids: Vec<i64> = source_events.iter().map(|event| event.id).collect();
    let input_fingerprint = source_state_change_input_fingerprint(&source_event_ids);

    Ok(EnvironmentPlanDraft {
        reason: "source_state_change",
        input_fingerprint,
        baseline_run_id,
        selection_spec: Some("source_downstream_stale".to_string()),
        selected_resources: db
            .list_stale_downstream_nodes(
                environment.project_id,
                environment.id,
                &source_keys,
                &source_event_ids,
                source_baseline_run_id,
            )
            .await?,
        source_event_id: source_events.first().map(|event| event.id),
        metadata: serde_json::json!({
            "source_keys": source_keys,
            "source_event_ids": source_event_ids,
            "source_event_count": source_events.len(),
            "planning_mode": "watermark_stale",
        }),
        code_drift: false,
    })
}

pub(super) async fn replan_pending_plan(
    db: &Db,
    plan: EnvironmentRunPlanRecord,
) -> AppResult<EnvironmentRunPlanRecord> {
    let Some(baseline_run_id) = plan.baseline_run_id else {
        return Ok(plan);
    };

    match plan.reason.as_str() {
        "code_change" => replan_code_change_plan(db, plan, baseline_run_id).await,
        "source_state_change" => replan_source_state_change_plan(db, plan, baseline_run_id).await,
        _ => Ok(plan),
    }
}

async fn replan_code_change_plan(
    db: &Db,
    plan: EnvironmentRunPlanRecord,
    baseline_run_id: Uuid,
) -> AppResult<EnvironmentRunPlanRecord> {
    let Some(target_git_commit_sha) = plan.target_git_commit_sha.clone() else {
        return Ok(plan);
    };
    let Some(target_manifest_run_id) = db
        .latest_manifest_run_id_for_commit(
            plan.project_id,
            plan.environment_id,
            &target_git_commit_sha,
        )
        .await?
    else {
        return Ok(plan);
    };

    let target_nodes = db
        .load_planning_manifest_nodes(target_manifest_run_id)
        .await?;
    let baseline_nodes = db.load_planning_manifest_nodes(baseline_run_id).await?;
    let target_edges = db.load_manifest_edges(target_manifest_run_id).await?;
    let current_nodes = db
        .load_current_node_state_for_planning(plan.project_id, plan.environment_id)
        .await?;
    let selected_resources = plan_code_change_selected_resources(
        &baseline_nodes,
        &target_nodes,
        &target_edges,
        &current_nodes,
    );
    let selection_spec = if selected_resources.is_empty() {
        Some("state_modified_live".to_string())
    } else {
        Some("state_modified_live_plus".to_string())
    };
    let mut metadata = plan.metadata.clone();
    metadata["last_replanned_at"] = Value::String(chrono::Utc::now().to_rfc3339());
    metadata["replanning_mode"] = Value::String("live_state_diff".to_string());
    if selected_resources.is_empty() {
        return db
            .mark_environment_run_plan_completed_noop(
                plan.plan_id,
                "plan already reconciled by prior run progress",
                metadata,
            )
            .await;
    }
    if selected_resources != plan.selected_resources
        || selection_spec.as_deref() != plan.selection_spec.as_deref()
    {
        return db
            .update_environment_run_plan_selection(
                plan.plan_id,
                selection_spec.as_deref(),
                &selected_resources,
                metadata,
            )
            .await;
    }
    db.update_environment_run_plan_selection(
        plan.plan_id,
        plan.selection_spec.as_deref(),
        &plan.selected_resources,
        metadata,
    )
    .await
}

async fn replan_source_state_change_plan(
    db: &Db,
    plan: EnvironmentRunPlanRecord,
    baseline_run_id: Uuid,
) -> AppResult<EnvironmentRunPlanRecord> {
    let source_event_ids = plan_source_event_ids(plan.source_event_id, &plan.metadata);
    if source_event_ids.is_empty() {
        return Ok(plan);
    }
    let source_keys: Vec<String> = plan
        .metadata
        .get("source_keys")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
        .filter_map(|v| v.as_str().map(ToString::to_string))
        .collect();
    let mut metadata = plan.metadata.clone();
    metadata["last_replanned_at"] = Value::String(chrono::Utc::now().to_rfc3339());
    metadata["replanning_mode"] = Value::String("watermark_stale_replan".to_string());

    let already_satisfied = db
        .are_source_state_events_satisfied(plan.project_id, plan.environment_id, &source_event_ids)
        .await?;
    if already_satisfied {
        return db
            .mark_environment_run_plan_completed_noop(
                plan.plan_id,
                "source-triggered plan already satisfied by a successful plan",
                metadata,
            )
            .await;
    }

    let stale_nodes = if !source_keys.is_empty() {
        db.list_stale_downstream_nodes(
            plan.project_id,
            plan.environment_id,
            &source_keys,
            &source_event_ids,
            baseline_run_id,
        )
        .await?
    } else {
        plan.selected_resources.clone()
    };

    if stale_nodes.is_empty() {
        return db
            .mark_environment_run_plan_completed_noop(
                plan.plan_id,
                "all downstream nodes already satisfy source watermarks",
                metadata,
            )
            .await;
    }
    if stale_nodes != plan.selected_resources {
        return db
            .update_environment_run_plan_selection(
                plan.plan_id,
                Some("source_downstream_stale"),
                &stale_nodes,
                metadata,
            )
            .await;
    }
    db.update_environment_run_plan_selection(
        plan.plan_id,
        plan.selection_spec.as_deref(),
        &plan.selected_resources,
        metadata,
    )
    .await
}
