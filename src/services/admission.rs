//! Plan admission: replan, conflict check, and invocation preparation.

use super::*;

#[derive(Debug, Clone)]
pub struct EnvironmentPlanAdmitPrepared {
    pub plan: EnvironmentRunPlanRecord,
    pub invocation_id: Option<Uuid>,
    pub prepared: Option<LocalExecutionPrepared>,
}

pub(super) async fn admit_plan(
    db: &Db,
    invocation_id: Uuid,
    plan_id: Uuid,
) -> AppResult<EnvironmentPlanAdmitPrepared> {
    let plan = db.get_environment_run_plan(plan_id).await?;
    let environment_id = plan.environment_id;
    let lease_owner = format!("admit:{invocation_id}");
    acquire_reconcile_lease(db, environment_id, &lease_owner).await?;
    let result = admit_plan_with_lease(db, invocation_id, plan).await;
    let _ = db
        .release_environment_reconcile_lease(environment_id, &lease_owner)
        .await;
    result
}

async fn acquire_reconcile_lease(db: &Db, environment_id: i64, owner: &str) -> AppResult<()> {
    if db
        .acquire_environment_reconcile_lease(environment_id, owner, RECONCILE_LEASE_DURATION)
        .await?
    {
        Ok(())
    } else {
        Err(AppError::ReconciliationInProgress)
    }
}

async fn admit_plan_with_lease(
    db: &Db,
    invocation_id: Uuid,
    plan: EnvironmentRunPlanRecord,
) -> AppResult<EnvironmentPlanAdmitPrepared> {
    if !plan.status.is_admissible() {
        return Err(AppError::PlanNotAdmissible(
            plan.plan_id.to_string(),
            plan.status.to_string(),
        ));
    }

    let plan = crate::services::planning::replan_pending_plan(db, plan).await?;
    if plan.status == PlanStatus::Completed {
        return Ok(EnvironmentPlanAdmitPrepared {
            plan,
            invocation_id: None,
            prepared: None,
        });
    }

    let blockers = db.list_active_conflicting_invocations(plan.plan_id).await?;
    if let Some(blocking_invocation_id) = blockers.first().copied() {
        let blocked = db
            .mark_environment_run_plan_blocked(
                plan.plan_id,
                Some(blocking_invocation_id),
                "plan is blocked by active resource overlap",
            )
            .await?;
        return Ok(EnvironmentPlanAdmitPrepared {
            plan: blocked,
            invocation_id: None,
            prepared: None,
        });
    }

    let project = db.get_project_by_id(plan.project_id).await?;
    let environment = db.get_environment_by_id(plan.environment_id).await?;
    let prepared = InvocationService::new(db)
        .prepare_remote_execution(
            invocation_id,
            InvocationCommand::Build,
            build_args(&plan),
            &project.project_id,
            &environment.slug,
        )
        .await?;
    Ok(EnvironmentPlanAdmitPrepared {
        plan,
        invocation_id: Some(invocation_id),
        prepared: Some(prepared),
    })
}

fn build_args(plan: &EnvironmentRunPlanRecord) -> Vec<OsString> {
    let mut args = Vec::new();
    if !plan.selected_resources.is_empty() {
        args.push("--select".into());
        for resource in &plan.selected_resources {
            args.push(resource.into());
        }
    }
    args
}

#[cfg(test)]
mod tests {
    use super::build_args;
    use crate::db::{EnvironmentRunPlanRecord, PlanStatus};
    use serde_json::Value;
    use std::ffi::OsString;
    use uuid::Uuid;

    fn plan(selected_resources: Vec<String>) -> EnvironmentRunPlanRecord {
        EnvironmentRunPlanRecord {
            plan_id: Uuid::new_v4(),
            project_id: 1,
            environment_id: 1,
            status: PlanStatus::Planned,
            reason: "code_change".to_string(),
            input_fingerprint: None,
            target_git_branch: None,
            target_git_commit_sha: None,
            baseline_run_id: None,
            selection_spec: None,
            selected_resources,
            resource_count: 0,
            superseded_by_plan_id: None,
            retry_count: 0,
            blocked_by_invocation_id: None,
            admitted_invocation_id: None,
            source_event_id: None,
            error: None,
            failure_count: 0,
            next_attempt_at: None,
            first_blocked_at: None,
            last_blocked_at: None,
            last_checked_at: None,
            admitted_at: None,
            completed_at: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            metadata: Value::Null,
        }
    }

    #[test]
    fn build_args_include_selected_resources() {
        let args = build_args(&plan(vec![
            "model.pkg.orders".to_string(),
            "seed.pkg.customers".to_string(),
        ]));
        assert_eq!(
            args,
            vec![
                OsString::from("--select"),
                OsString::from("model.pkg.orders"),
                OsString::from("seed.pkg.customers")
            ]
        );
    }

    #[test]
    fn build_args_empty_for_full_graph_plan() {
        assert!(build_args(&plan(Vec::new())).is_empty());
    }
}
