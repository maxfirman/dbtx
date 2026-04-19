use crate::api::InvocationCommandApi;
use crate::db::{EnvironmentRecord, EnvironmentRunPlanRecord, SourceStateEventRecord};
use crate::error::{AppError, AppResult};
use crate::invocation_bootstrap::start_prepared_invocation;
use crate::server::AppState;
use crate::services::{EnvironmentService, InvocationService};
use chrono::Utc;
use serde_json::Value;
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
        let actual_state = state
            .db()
            .get_environment_actual_state(&environment.project_ref, &environment.slug)
            .await?;
        let source_events = state
            .db()
            .list_unsatisfied_source_state_events(environment.project_id, environment.id)
            .await?;
        if let Some(next_attempt_at) = automatic_reconcile_backoff_until(
            state,
            &environment,
            actual_state.last_successful_commit_sha.as_deref(),
            &source_events,
        )
        .await?
        .filter(|next_attempt_at| *next_attempt_at > Utc::now())
        {
            info!(
                project_id = %environment.project_ref,
                environment_slug = %environment.slug,
                next_attempt_at = %next_attempt_at,
                "skipping automatic reconcile until retry backoff expires"
            );
            continue;
        }
        if environment.git_commit_sha != actual_state.last_successful_commit_sha
            && ensure_target_manifest_for_reconcile_async(state, &environment).await?
        {
            continue;
        }
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

async fn automatic_reconcile_backoff_until(
    state: &AppState,
    environment: &EnvironmentRecord,
    last_successful_commit_sha: Option<&str>,
    source_events: &[SourceStateEventRecord],
) -> AppResult<Option<chrono::DateTime<Utc>>> {
    if let Some(preparation) = state
        .db()
        .get_environment_reconcile_preparation_by_scope(environment.project_id, environment.id)
        .await?
    {
        let code_drift = environment.git_commit_sha.as_deref() != last_successful_commit_sha;
        if preparation.kind == "target_manifest"
            && preparation.status == "failed"
            && code_drift
            && preparation.target_git_commit_sha == environment.git_commit_sha
        {
            return Ok(preparation.next_attempt_at);
        }
    }

    let latest_failed_plan = state
        .db()
        .list_environment_run_plans_by_scope(environment.project_id, environment.id)
        .await?
        .into_iter()
        .find(|plan| matches!(plan.status.as_str(), "failed" | "canceled"));
    let Some(plan) = latest_failed_plan else {
        return Ok(None);
    };
    let should_apply = match plan.reason.as_str() {
        "code_change" => {
            environment.git_commit_sha.as_deref() != last_successful_commit_sha
                && plan.target_git_commit_sha == environment.git_commit_sha
        }
        "source_state_change" => {
            let current_event_ids = source_events.iter().map(|event| event.id).collect::<Vec<_>>();
            !current_event_ids.is_empty() && current_event_ids == plan_source_event_ids(&plan)
        }
        _ => false,
    };
    Ok(if should_apply { plan.next_attempt_at } else { None })
}

async fn ensure_target_manifest_for_reconcile_async(
    state: &AppState,
    environment: &crate::db::EnvironmentRecord,
) -> AppResult<bool> {
    let Some(desired_commit_sha) = environment.git_commit_sha.clone() else {
        return Ok(false);
    };
    if state
        .db()
        .latest_manifest_run_id_for_commit(environment.project_id, environment.id, &desired_commit_sha)
        .await?
        .is_some()
    {
        return Ok(false);
    }
    if state
        .db()
        .has_active_manifest_prepare_for_commit(
            environment.project_id,
            environment.id,
            &desired_commit_sha,
        )
        .await?
    {
        return Ok(true);
    }
    if state
        .db()
        .get_environment_reconcile_preparation_by_scope(environment.project_id, environment.id)
        .await?
        .filter(|preparation| {
            preparation.kind == "target_manifest"
                && preparation.target_git_commit_sha.as_deref() == Some(desired_commit_sha.as_str())
                && preparation.status == "failed"
                && preparation
                    .next_attempt_at
                    .map(|next_attempt_at| next_attempt_at > Utc::now())
                    .unwrap_or(false)
        })
        .is_some()
    {
        return Ok(true);
    }

    let invocation_id = Uuid::new_v4();
    let prepared = InvocationService::new(state.db())
        .prepare_remote_manifest_capture(
            invocation_id,
            &environment.project_ref,
            &environment.slug,
        )
        .await?;
    start_prepared_invocation(
        state,
        invocation_id,
        InvocationCommandApi::ManifestPrepare,
        None,
        prepared,
    )
    .await?;
    state
        .db()
        .mark_manifest_prepare_running(
            environment.project_id,
            environment.id,
            &desired_commit_sha,
            invocation_id,
        )
        .await?;
    Ok(true)
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

fn plan_source_event_ids(plan: &EnvironmentRunPlanRecord) -> Vec<i64> {
    let mut event_ids = plan
        .metadata
        .get("source_event_ids")
        .and_then(Value::as_array)
        .into_iter()
        .flat_map(|values| values.iter())
        .filter_map(Value::as_i64)
        .collect::<Vec<_>>();
    if event_ids.is_empty() && let Some(source_event_id) = plan.source_event_id {
        event_ids.push(source_event_id);
    }
    event_ids.sort_unstable();
    event_ids.dedup();
    event_ids
}
