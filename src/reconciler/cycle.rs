//! Per-environment automatic reconciliation cycle.

use super::should_ignore_reconcile_error;
use crate::db::{EnvironmentRecord, PlanStatus, PreparationStatus, SourceStateEventRecord};
use crate::error::AppResult;
use crate::process_state::ProcessState;
use crate::services::{EnvironmentService, PreparationKind, ReconcileInputIdentity};
use chrono::Utc;
use tracing::{error, info};
use uuid::Uuid;

pub async fn reconcile_environments_once(state: &ProcessState) -> AppResult<usize> {
    let environments = state.db().list_auto_reconcile_remote_environments().await?;
    let mut planned = 0usize;
    for environment in environments {
        let outcome = reconcile_environment_once(state, environment).await?;
        if outcome.plan_created {
            planned += 1;
        }
    }
    Ok(planned)
}

#[derive(Debug, Clone, Copy)]
struct ReconcileEnvironmentOutcome {
    plan_created: bool,
}

async fn reconcile_environment_once(
    state: &ProcessState,
    environment: EnvironmentRecord,
) -> AppResult<ReconcileEnvironmentOutcome> {
    let actual_state = state
        .db()
        .get_environment_actual_state(&environment.project_ref, &environment.slug)
        .await?;
    let source_events = crate::services::source_state::advance_and_load_unsatisfied_source_events(
        state.db(),
        &environment,
        actual_state.last_successful_run_id,
    )
    .await?;

    if let Some(next_attempt_at) = automatic_reconcile_backoff_until(
        state,
        &environment,
        actual_state.last_successful_run_id,
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
        return Ok(ReconcileEnvironmentOutcome {
            plan_created: false,
        });
    }

    if environment.git_commit_sha != actual_state.last_successful_commit_sha
        && ensure_target_manifest_for_reconcile_async(state, &environment).await?
    {
        return Ok(ReconcileEnvironmentOutcome {
            plan_created: false,
        });
    }

    let service = EnvironmentService::new(state.db());
    let plan = match service
        .reconcile(environment.project_ref.clone(), environment.slug.clone())
        .await
    {
        Ok(plan) => plan,
        Err(err) if should_ignore_reconcile_error(&err) => {
            return Ok(ReconcileEnvironmentOutcome {
                plan_created: false,
            });
        }
        Err(err) => {
            error!(
                error = %err,
                project_id = %environment.project_ref,
                environment_slug = %environment.slug,
                "automatic reconcile failed"
            );
            return Ok(ReconcileEnvironmentOutcome {
                plan_created: false,
            });
        }
    };
    info!(
        project_id = %environment.project_ref,
        environment_slug = %environment.slug,
        plan_id = %plan.plan_id,
        plan_reason = %plan.reason,
        resource_count = plan.resource_count,
        "created reconciliation plan"
    );

    match EnvironmentService::new(state.db())
        .admit_and_start_plan(state, plan.plan_id)
        .await
    {
        Ok(_) => {}
        Err(err) if should_ignore_reconcile_error(&err) => {}
        Err(err) => {
            error!(
                error = %err,
                project_id = %environment.project_ref,
                environment_slug = %environment.slug,
                plan_id = %plan.plan_id,
                "automatic admit failed"
            );
        }
    }

    Ok(ReconcileEnvironmentOutcome { plan_created: true })
}

async fn automatic_reconcile_backoff_until(
    state: &ProcessState,
    environment: &EnvironmentRecord,
    baseline_run_id: Option<Uuid>,
    last_successful_commit_sha: Option<&str>,
    source_events: &[SourceStateEventRecord],
) -> AppResult<Option<chrono::DateTime<Utc>>> {
    let current_code_change_identity =
        if environment.git_commit_sha.as_deref() != last_successful_commit_sha {
            environment
                .git_commit_sha
                .as_deref()
                .map(|desired_commit_sha| {
                    ReconcileInputIdentity::code_change(desired_commit_sha, baseline_run_id)
                })
        } else {
            None
        };
    if let Some(preparation) = state
        .db()
        .get_environment_reconcile_preparation_by_scope(environment.project_id, environment.id)
        .await?
        && preparation.kind == PreparationKind::TargetManifest.as_str()
        && preparation.status == PreparationStatus::Failed
        && preparation.input_fingerprint
            == current_code_change_identity
                .as_ref()
                .map(ReconcileInputIdentity::target_manifest_preparation_fingerprint)
    {
        return Ok(preparation.next_attempt_at);
    }

    let latest_failed_plan = state
        .db()
        .list_environment_run_plans_by_scope(environment.project_id, environment.id)
        .await?
        .into_iter()
        .find(|plan| matches!(plan.status, PlanStatus::Failed | PlanStatus::Canceled));
    let Some(plan) = latest_failed_plan else {
        return Ok(None);
    };
    let should_apply = match current_code_change_identity.as_ref() {
        Some(identity) if identity.matches_plan(&plan) => true,
        _ => {
            let current_event_ids = source_events
                .iter()
                .map(|event| event.id)
                .collect::<Vec<_>>();
            !current_event_ids.is_empty()
                && ReconcileInputIdentity::source_state_change(&current_event_ids)
                    .matches_plan(&plan)
        }
    };
    Ok(if should_apply {
        plan.next_attempt_at
    } else {
        None
    })
}

async fn ensure_target_manifest_for_reconcile_async(
    state: &ProcessState,
    environment: &EnvironmentRecord,
) -> AppResult<bool> {
    if environment.git_commit_sha.is_none() {
        return Ok(false);
    }
    let outcome = crate::manifest_preparation::ensure_manifest_preparation(
        state.db(),
        environment,
        crate::manifest_preparation::ProcessStateStarter(state),
    )
    .await?;
    match outcome {
        crate::manifest_preparation::ManifestPreparationOutcome::AlreadyAvailable => Ok(false),
        crate::manifest_preparation::ManifestPreparationOutcome::InProgress
        | crate::manifest_preparation::ManifestPreparationOutcome::Started(_) => Ok(true),
    }
}
