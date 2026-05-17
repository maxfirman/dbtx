//! Source-state progress used before reconciliation planning.

use super::*;

pub(crate) async fn advance_and_load_unsatisfied_source_events(
    db: &Db,
    environment: &EnvironmentRecord,
    baseline_run_id: Option<Uuid>,
) -> AppResult<Vec<SourceStateEventRecord>> {
    let source_events = db
        .list_unsatisfied_source_state_events(environment.project_id, environment.id)
        .await?;
    let Some(manifest_run_id) = baseline_run_id else {
        return Ok(source_events);
    };
    if source_events.is_empty() {
        return Ok(source_events);
    }

    let source_keys = source_events
        .iter()
        .map(|event| event.source_key.clone())
        .collect::<Vec<_>>();
    db.advance_satisfied_source_events_from_watermarks(
        environment.project_id,
        environment.id,
        &source_keys,
        manifest_run_id,
    )
    .await?;
    db.list_unsatisfied_source_state_events(environment.project_id, environment.id)
        .await
}
