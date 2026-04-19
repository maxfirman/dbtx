use crate::api::InvocationCommandApi;
use crate::error::{AppError, AppResult};
use crate::invocation_bootstrap::start_prepared_invocation;
use crate::server::AppState;
use crate::services::EnvironmentService;
use std::time::Duration;
use tracing::{error, info};
use uuid::Uuid;

fn reconcile_interval() -> Duration {
    std::env::var("DBTX_RECONCILE_INTERVAL_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .map(Duration::from_millis)
        .unwrap_or_else(|| Duration::from_secs(5))
}

fn blocked_plan_sweep_interval() -> Duration {
    std::env::var("DBTX_BLOCKED_PLAN_SWEEP_INTERVAL_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .map(Duration::from_millis)
        .unwrap_or_else(|| Duration::from_secs(2))
}

pub async fn run(state: AppState) -> AppResult<()> {
    let reconcile_interval_duration = reconcile_interval();
    let blocked_interval_duration = blocked_plan_sweep_interval();
    info!(
        reconcile_interval_ms = reconcile_interval_duration.as_millis() as u64,
        blocked_plan_sweep_interval_ms = blocked_interval_duration.as_millis() as u64,
        "starting dbtx reconciler"
    );
    let mut reconcile_interval = tokio::time::interval(reconcile_interval_duration);
    reconcile_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut blocked_interval = tokio::time::interval(blocked_interval_duration);
    blocked_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = reconcile_interval.tick() => {
                if let Err(err) = reconcile_environments_once(&state).await {
                    error!(error = %err, "environment reconcile sweep failed");
                }
            }
            _ = blocked_interval.tick() => {
                if let Err(err) = sweep_blocked_plans_once(&state).await {
                    error!(error = %err, "blocked plan sweep failed");
                }
            }
        }
    }
}

pub async fn reconcile_environments_once(state: &AppState) -> AppResult<usize> {
    let environments = state.db().list_auto_deploy_remote_environments().await?;
    let mut planned = 0usize;
    for environment in environments {
        let service = EnvironmentService::new(state.db());
        let plan = match service
            .reconcile(environment.project_ref.clone(), environment.slug.clone())
            .await
        {
            Ok(plan) => plan,
            Err(err) if should_ignore_reconcile_error(&err) => continue,
            Err(err) => {
                error!(
                    error = %err,
                    project_id = %environment.project_ref,
                    environment_slug = %environment.slug,
                    "automatic reconcile failed"
                );
                continue;
            }
        };
        planned += 1;
        let invocation_id = Uuid::new_v4();
        let prepared = match service.admit_plan(invocation_id, plan.plan_id).await {
            Ok(prepared) => prepared,
            Err(err) if should_ignore_reconcile_error(&err) => continue,
            Err(err) => {
                error!(
                    error = %err,
                    project_id = %environment.project_ref,
                    environment_slug = %environment.slug,
                    plan_id = %plan.plan_id,
                    "automatic admit failed"
                );
                continue;
            }
        };
        let Some(prepared_invocation) = prepared.prepared else {
            continue;
        };
        start_prepared_invocation(
            state,
            invocation_id,
            InvocationCommandApi::Build,
            Some(plan.plan_id),
            prepared_invocation,
        )
        .await?;
        state
            .db()
            .mark_environment_run_plan_admitted(plan.plan_id, invocation_id)
            .await?;
    }
    Ok(planned)
}

pub async fn sweep_blocked_plans_once(state: &AppState) -> AppResult<usize> {
    let scopes = state.db().list_blocked_environment_scopes().await?;
    let mut admitted = 0usize;
    for (project_id, environment_id) in scopes {
        admitted += auto_admit_blocked_plans_for_environment(state, project_id, environment_id)
            .await?;
    }
    Ok(admitted)
}

pub async fn auto_admit_blocked_plans_for_environment(
    state: &AppState,
    project_id: i64,
    environment_id: i64,
) -> AppResult<usize> {
    let blocked_plan_ids = state
        .db()
        .list_blocked_environment_run_plan_ids(project_id, environment_id)
        .await?;
    let service = EnvironmentService::new(state.db());
    let mut admitted = 0usize;

    for plan_id in blocked_plan_ids {
        let invocation_id = Uuid::new_v4();
        let prepared = service.admit_plan(invocation_id, plan_id).await?;
        let Some(prepared_invocation) = prepared.prepared else {
            continue;
        };
        start_prepared_invocation(
            state,
            invocation_id,
            InvocationCommandApi::Build,
            Some(plan_id),
            prepared_invocation,
        )
        .await?;
        state
            .db()
            .mark_environment_run_plan_admitted(plan_id, invocation_id)
            .await?;
        admitted += 1;
    }

    Ok(admitted)
}

fn should_ignore_reconcile_error(err: &AppError) -> bool {
    match err {
        AppError::Io(io_err) => {
            let message = io_err.to_string();
            message.contains("environment is already reconciled to known desired state")
                || message.contains("environment reconciliation is already in progress")
                || message.contains("plan ")
                    && message.contains(" is not admissible from status ")
        }
        _ => false,
    }
}
