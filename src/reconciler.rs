use crate::api::InvocationCommandApi;
use crate::error::AppResult;
use crate::invocation_bootstrap::start_prepared_invocation;
use crate::server::AppState;
use crate::services::EnvironmentService;
use std::time::Duration;
use tracing::{error, info};
use uuid::Uuid;

fn blocked_plan_sweep_interval() -> Duration {
    std::env::var("DBTX_BLOCKED_PLAN_SWEEP_INTERVAL_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .map(Duration::from_millis)
        .unwrap_or_else(|| Duration::from_secs(2))
}

pub async fn run(state: AppState) -> AppResult<()> {
    let interval_duration = blocked_plan_sweep_interval();
    info!(
        interval_ms = interval_duration.as_millis() as u64,
        "starting dbtx reconciler"
    );
    let mut interval = tokio::time::interval(interval_duration);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        if let Err(err) = sweep_blocked_plans_once(&state).await {
            error!(error = %err, "blocked plan sweep failed");
        }
    }
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
