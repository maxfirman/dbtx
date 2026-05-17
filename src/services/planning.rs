//! Planning algorithm: resource selection, plan derivation, and blocked-plan replanning.

use super::*;

pub(super) fn plan_code_change_selected_resources(
    baseline_nodes: &[PlanningManifestNodeRecord],
    target_nodes: &[PlanningManifestNodeRecord],
    target_edges: &[(String, String)],
    current_nodes: &[CurrentNodeStatePlanningRecord],
) -> Vec<String> {
    let baseline_checksums = baseline_nodes
        .iter()
        .map(|node| (node.unique_id.clone(), node.checksum.clone()))
        .collect::<BTreeMap<_, _>>();
    let target_by_id = target_nodes
        .iter()
        .map(|node| (node.unique_id.clone(), node))
        .collect::<BTreeMap<_, _>>();
    let current_by_id = current_nodes
        .iter()
        .map(|node| (node.unique_id.clone(), node))
        .collect::<BTreeMap<_, _>>();

    let directly_modified = target_nodes
        .iter()
        .filter(|node| is_build_plannable_resource_type(node.resource_type.as_deref()))
        .filter(|node| baseline_checksums.get(&node.unique_id).cloned().flatten() != node.checksum)
        .map(|node| node.unique_id.clone())
        .collect::<BTreeSet<_>>();

    if directly_modified.is_empty() {
        return Vec::new();
    }

    let mut child_map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut parent_map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (parent, child) in target_edges {
        child_map
            .entry(parent.clone())
            .or_default()
            .push(child.clone());
        parent_map
            .entry(child.clone())
            .or_default()
            .push(parent.clone());
    }

    let mut candidate = directly_modified.clone();
    let mut stack = directly_modified.iter().cloned().collect::<Vec<_>>();
    while let Some(parent) = stack.pop() {
        for child in child_map.get(&parent).into_iter().flatten() {
            if target_by_id.contains_key(child) && candidate.insert(child.clone()) {
                stack.push(child.clone());
            }
        }
    }

    let mut memo: BTreeMap<String, AncestorRequirement> = BTreeMap::new();
    let mut selected = candidate
        .iter()
        .filter_map(|unique_id| {
            let target = target_by_id.get(unique_id)?;
            let current = current_by_id.get(unique_id).copied();
            let requirement = compute_ancestor_requirement(
                unique_id,
                &candidate,
                &directly_modified,
                &parent_map,
                &target_by_id,
                &current_by_id,
                &mut memo,
            );
            let current_checksum = current.and_then(|node| node.checksum.clone());
            let current_success_at = current.and_then(|node| node.last_success_at);
            let matches_target = current_checksum == target.checksum;
            let is_stale = !matches_target
                || requirement.has_unreconciled_ancestor
                || requirement
                    .latest_reconciled_ancestor_success_at
                    .is_some_and(|ancestor_time| {
                        current_success_at
                            .map(|node_time| node_time < ancestor_time)
                            .unwrap_or(true)
                    });
            is_stale.then(|| unique_id.clone())
        })
        .collect::<Vec<_>>();
    selected.sort();
    selected
}

#[derive(Clone, Copy, Default)]
struct AncestorRequirement {
    has_unreconciled_ancestor: bool,
    latest_reconciled_ancestor_success_at: Option<chrono::DateTime<chrono::Utc>>,
}

fn compute_ancestor_requirement(
    unique_id: &str,
    candidate: &BTreeSet<String>,
    directly_modified: &BTreeSet<String>,
    parent_map: &BTreeMap<String, Vec<String>>,
    target_by_id: &BTreeMap<String, &PlanningManifestNodeRecord>,
    current_by_id: &BTreeMap<String, &CurrentNodeStatePlanningRecord>,
    memo: &mut BTreeMap<String, AncestorRequirement>,
) -> AncestorRequirement {
    if let Some(existing) = memo.get(unique_id).copied() {
        return existing;
    }

    let mut requirement = AncestorRequirement::default();
    if directly_modified.contains(unique_id) {
        let target_checksum = target_by_id
            .get(unique_id)
            .and_then(|node| node.checksum.clone());
        let current = current_by_id.get(unique_id).copied();
        let current_checksum = current.and_then(|node| node.checksum.clone());
        let current_success_at = current.and_then(|node| node.last_success_at);
        let root_reconciled = current_checksum == target_checksum && current_success_at.is_some();
        if root_reconciled {
            requirement.latest_reconciled_ancestor_success_at = current_success_at;
        } else {
            requirement.has_unreconciled_ancestor = true;
        }
    }

    for parent in parent_map.get(unique_id).into_iter().flatten() {
        if !candidate.contains(parent) {
            continue;
        }
        let parent_requirement = compute_ancestor_requirement(
            parent,
            candidate,
            directly_modified,
            parent_map,
            target_by_id,
            current_by_id,
            memo,
        );
        requirement.has_unreconciled_ancestor |= parent_requirement.has_unreconciled_ancestor;
        requirement.latest_reconciled_ancestor_success_at = match (
            requirement.latest_reconciled_ancestor_success_at,
            parent_requirement.latest_reconciled_ancestor_success_at,
        ) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (Some(left), None) => Some(left),
            (None, Some(right)) => Some(right),
            (None, None) => None,
        };
    }

    memo.insert(unique_id.to_string(), requirement);
    requirement
}

fn is_build_plannable_resource_type(resource_type: Option<&str>) -> bool {
    matches!(
        resource_type,
        Some("model" | "seed" | "snapshot" | "test" | "unit_test")
    )
}

pub(super) fn plan_source_event_ids(source_event_id: Option<i64>, metadata: &Value) -> Vec<i64> {
    let mut event_ids = metadata
        .get("source_event_ids")
        .and_then(|value| value.as_array())
        .into_iter()
        .flatten()
        .filter_map(|value| value.as_i64())
        .collect::<Vec<_>>();
    if event_ids.is_empty()
        && let Some(source_event_id) = source_event_id
    {
        event_ids.push(source_event_id);
    }
    event_ids.sort_unstable();
    event_ids.dedup();
    event_ids
}

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
    staleness: &(impl super::StalenessOracle + ?Sized),
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
        selected_resources: staleness
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
        "source_state_change" => replan_source_state_change_plan(db, db, plan, baseline_run_id).await,
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
    staleness: &(impl super::StalenessOracle + ?Sized),
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
        staleness
            .list_stale_downstream_nodes(
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

#[cfg(test)]
mod tests {
    use super::{plan_code_change_selected_resources, plan_source_event_ids};
    use crate::db::{CurrentNodeStatePlanningRecord, PlanningManifestNodeRecord};

    #[test]
    fn code_change_planning_uses_live_current_state_for_completed_roots() {
        let baseline = vec![
            PlanningManifestNodeRecord {
                unique_id: "model.pkg.orders".to_string(),
                resource_type: Some("model".to_string()),
                checksum: Some("old-orders".to_string()),
            },
            PlanningManifestNodeRecord {
                unique_id: "model.pkg.customers".to_string(),
                resource_type: Some("model".to_string()),
                checksum: Some("same-customers".to_string()),
            },
        ];
        let target = vec![
            PlanningManifestNodeRecord {
                unique_id: "model.pkg.orders".to_string(),
                resource_type: Some("model".to_string()),
                checksum: Some("new-orders".to_string()),
            },
            PlanningManifestNodeRecord {
                unique_id: "model.pkg.customers".to_string(),
                resource_type: Some("model".to_string()),
                checksum: Some("same-customers".to_string()),
            },
        ];
        let edges = vec![(
            "model.pkg.orders".to_string(),
            "model.pkg.customers".to_string(),
        )];
        let current = vec![
            CurrentNodeStatePlanningRecord {
                unique_id: "model.pkg.orders".to_string(),
                checksum: Some("new-orders".to_string()),
                last_success_at: Some(chrono::Utc::now()),
            },
            CurrentNodeStatePlanningRecord {
                unique_id: "model.pkg.customers".to_string(),
                checksum: Some("same-customers".to_string()),
                last_success_at: Some(chrono::Utc::now() - chrono::Duration::minutes(5)),
            },
        ];

        let selected = plan_code_change_selected_resources(&baseline, &target, &edges, &current);
        assert_eq!(selected, vec!["model.pkg.customers".to_string()]);
    }

    #[test]
    fn code_change_planning_selects_modified_node_and_downstream() {
        let baseline = vec![
            PlanningManifestNodeRecord {
                unique_id: "model.pkg.stg_orders".to_string(),
                resource_type: Some("model".to_string()),
                checksum: Some("old-stg".to_string()),
            },
            PlanningManifestNodeRecord {
                unique_id: "model.pkg.orders".to_string(),
                resource_type: Some("model".to_string()),
                checksum: Some("orders-v1".to_string()),
            },
            PlanningManifestNodeRecord {
                unique_id: "model.pkg.revenue".to_string(),
                resource_type: Some("model".to_string()),
                checksum: Some("revenue-v1".to_string()),
            },
        ];
        let target = vec![
            PlanningManifestNodeRecord {
                unique_id: "model.pkg.stg_orders".to_string(),
                resource_type: Some("model".to_string()),
                checksum: Some("new-stg".to_string()),
            },
            PlanningManifestNodeRecord {
                unique_id: "model.pkg.orders".to_string(),
                resource_type: Some("model".to_string()),
                checksum: Some("orders-v1".to_string()),
            },
            PlanningManifestNodeRecord {
                unique_id: "model.pkg.revenue".to_string(),
                resource_type: Some("model".to_string()),
                checksum: Some("revenue-v1".to_string()),
            },
        ];
        let edges = vec![
            (
                "model.pkg.stg_orders".to_string(),
                "model.pkg.orders".to_string(),
            ),
            (
                "model.pkg.orders".to_string(),
                "model.pkg.revenue".to_string(),
            ),
        ];
        let current = vec![];

        let selected = plan_code_change_selected_resources(&baseline, &target, &edges, &current);
        assert_eq!(
            selected,
            vec![
                "model.pkg.orders".to_string(),
                "model.pkg.revenue".to_string(),
                "model.pkg.stg_orders".to_string(),
            ]
        );
    }

    #[test]
    fn code_change_planning_returns_empty_when_no_changes() {
        let nodes = vec![PlanningManifestNodeRecord {
            unique_id: "model.pkg.orders".to_string(),
            resource_type: Some("model".to_string()),
            checksum: Some("same".to_string()),
        }];
        let selected = plan_code_change_selected_resources(&nodes, &nodes, &[], &[]);
        assert!(selected.is_empty());
    }

    #[test]
    fn code_change_planning_skips_non_plannable_resource_types() {
        let baseline = vec![PlanningManifestNodeRecord {
            unique_id: "source.pkg.raw_orders".to_string(),
            resource_type: Some("source".to_string()),
            checksum: Some("old".to_string()),
        }];
        let target = vec![PlanningManifestNodeRecord {
            unique_id: "source.pkg.raw_orders".to_string(),
            resource_type: Some("source".to_string()),
            checksum: Some("new".to_string()),
        }];
        let selected = plan_code_change_selected_resources(&baseline, &target, &[], &[]);
        assert!(selected.is_empty());
    }

    #[test]
    fn code_change_planning_includes_new_nodes_not_in_baseline() {
        let baseline = vec![];
        let target = vec![PlanningManifestNodeRecord {
            unique_id: "model.pkg.new_model".to_string(),
            resource_type: Some("model".to_string()),
            checksum: Some("abc".to_string()),
        }];
        let selected = plan_code_change_selected_resources(&baseline, &target, &[], &[]);
        assert_eq!(selected, vec!["model.pkg.new_model".to_string()]);
    }

    #[test]
    fn plan_source_event_ids_reads_from_metadata_array() {
        let metadata = serde_json::json!({"source_event_ids": [5, 3, 1]});
        let ids = plan_source_event_ids(None, &metadata);
        assert_eq!(ids, vec![1, 3, 5]);
    }

    #[test]
    fn plan_source_event_ids_falls_back_to_source_event_id() {
        let metadata = serde_json::json!({});
        let ids = plan_source_event_ids(Some(42), &metadata);
        assert_eq!(ids, vec![42]);
    }

    #[test]
    fn plan_source_event_ids_empty_when_no_source() {
        let metadata = serde_json::json!({});
        let ids = plan_source_event_ids(None, &metadata);
        assert!(ids.is_empty());
    }

    #[test]
    fn plan_source_event_ids_deduplicates() {
        let metadata = serde_json::json!({"source_event_ids": [3, 3, 1, 1]});
        let ids = plan_source_event_ids(None, &metadata);
        assert_eq!(ids, vec![1, 3]);
    }

    #[test]
    fn plan_source_event_ids_ignores_fallback_when_metadata_present() {
        let metadata = serde_json::json!({"source_event_ids": [10]});
        let ids = plan_source_event_ids(Some(99), &metadata);
        assert_eq!(ids, vec![10]);
    }
}

#[cfg(test)]
mod proptests {
    use super::{is_build_plannable_resource_type, plan_code_change_selected_resources};
    use crate::db::{CurrentNodeStatePlanningRecord, PlanningManifestNodeRecord};
    use proptest::prelude::*;
    use std::collections::BTreeSet;

    fn node_id(i: usize) -> String {
        format!("model.pkg.node_{i}")
    }

    fn arb_dag(
        max_nodes: usize,
    ) -> impl Strategy<Value = (Vec<PlanningManifestNodeRecord>, Vec<(String, String)>)> {
        (1..=max_nodes)
            .prop_flat_map(|n| {
                let checksums = proptest::collection::vec(proptest::option::of("[a-f0-9]{8}"), n);
                let edge_bits = proptest::collection::vec(proptest::bool::ANY, n * n);
                (Just(n), checksums, edge_bits)
            })
            .prop_map(|(n, checksums, edge_bits)| {
                let nodes: Vec<PlanningManifestNodeRecord> = (0..n)
                    .map(|i| PlanningManifestNodeRecord {
                        unique_id: node_id(i),
                        resource_type: Some("model".to_string()),
                        checksum: checksums[i].clone(),
                    })
                    .collect();
                let mut edges = Vec::new();
                for i in 0..n {
                    for j in (i + 1)..n {
                        if edge_bits[i * n + j] {
                            edges.push((node_id(i), node_id(j)));
                        }
                    }
                }
                (nodes, edges)
            })
    }

    proptest! {
        #[test]
        fn empty_when_no_checksums_changed(
            (nodes, edges) in arb_dag(8)
        ) {
            let result = plan_code_change_selected_resources(&nodes, &nodes, &edges, &[]);
            prop_assert!(result.is_empty(), "expected empty but got {:?}", result);
        }

        #[test]
        fn output_is_sorted_and_deduplicated(
            (baseline, edges) in arb_dag(8),
            modifications in proptest::collection::vec(0..8usize, 0..4)
        ) {
            let mut target = baseline.clone();
            for &idx in &modifications {
                if idx < target.len() {
                    target[idx].checksum = Some("modified".to_string());
                }
            }
            let result = plan_code_change_selected_resources(&baseline, &target, &edges, &[]);
            let mut sorted = result.clone();
            sorted.sort();
            sorted.dedup();
            prop_assert_eq!(&result, &sorted);
        }

        #[test]
        fn directly_modified_nodes_are_included_when_not_reconciled(
            (baseline, edges) in arb_dag(8),
            modifications in proptest::collection::vec(0..8usize, 1..4)
        ) {
            let mut target = baseline.clone();
            let mut modified_ids = BTreeSet::new();
            for &idx in &modifications {
                if idx < target.len() {
                    target[idx].checksum = Some("modified".to_string());
                    modified_ids.insert(target[idx].unique_id.clone());
                }
            }
            let result = plan_code_change_selected_resources(&baseline, &target, &edges, &[]);
            let result_set: BTreeSet<_> = result.into_iter().collect();
            for id in &modified_ids {
                if is_build_plannable_resource_type(Some("model")) {
                    prop_assert!(
                        result_set.contains(id),
                        "modified node {id} missing from result"
                    );
                }
            }
        }

        #[test]
        fn output_nodes_are_reachable_from_modified(
            (baseline, edges) in arb_dag(8),
            modifications in proptest::collection::vec(0..8usize, 1..3)
        ) {
            let mut target = baseline.clone();
            let mut modified_ids = BTreeSet::new();
            for &idx in &modifications {
                if idx < target.len() {
                    target[idx].checksum = Some("modified".to_string());
                    modified_ids.insert(target[idx].unique_id.clone());
                }
            }
            let result = plan_code_change_selected_resources(&baseline, &target, &edges, &[]);

            let mut reachable = modified_ids.clone();
            let mut stack: Vec<_> = modified_ids.iter().cloned().collect();
            let target_ids: BTreeSet<_> = target.iter().map(|n| n.unique_id.clone()).collect();
            while let Some(parent) = stack.pop() {
                for (p, c) in &edges {
                    if *p == parent && target_ids.contains(c) && reachable.insert(c.clone()) {
                        stack.push(c.clone());
                    }
                }
            }

            for id in &result {
                prop_assert!(
                    reachable.contains(id),
                    "output node {id} is not reachable from any modified node"
                );
            }
        }

        #[test]
        fn fully_reconciled_nodes_are_excluded(
            (baseline, edges) in arb_dag(6),
            mod_idx in 0..6usize
        ) {
            if mod_idx >= baseline.len() {
                return Ok(());
            }
            let mut target = baseline.clone();
            target[mod_idx].checksum = Some("modified".to_string());

            let current = vec![CurrentNodeStatePlanningRecord {
                unique_id: target[mod_idx].unique_id.clone(),
                checksum: Some("modified".to_string()),
                last_success_at: Some(chrono::Utc::now()),
            }];

            let result = plan_code_change_selected_resources(&baseline, &target, &edges, &current);
            prop_assert!(
                !result.contains(&target[mod_idx].unique_id),
                "reconciled node should be excluded"
            );
        }
    }
}