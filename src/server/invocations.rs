use super::*;

#[utoipa::path(
    post,
    path = "/v1/invocations",
    tag = "invocations",
    request_body = InvocationCreateApiRequest,
    responses(
        (status = 200, description = "Created invocation", body = InvocationCreateResponse),
        (status = 400, description = "Invalid request", body = ApiErrorResponse),
        (status = 404, description = "Project or environment not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn invocation_create(
    State(state): State<AppState>,
    Json(request): Json<InvocationCreateApiRequest>,
) -> Result<Json<InvocationCreateResponse>, ApiError> {
    let invocation_id = Uuid::new_v4();
    info!(
        invocation_id = %invocation_id,
        command = ?request.command,
        project_id = request.project_id.as_deref().unwrap_or(""),
        environment_slug = request.environment_slug.as_deref().unwrap_or(""),
        "starting invocation"
    );

    let db = state.db.clone();
    let service = InvocationService::new(&db);
    let project_id = request
        .project_id
        .as_deref()
        .ok_or(AppError::RemoteExecutionRequiresProjectId)?;
    let environment_slug = request
        .environment_slug
        .as_deref()
        .ok_or(AppError::RemoteExecutionRequiresEnvironmentSlug)?;
    let environment = db.get_environment(project_id, environment_slug).await?;
    let project = db
        .get_project_by_project_id(&environment.project_ref)
        .await?;
    let derived_execution_mode = if environment.git_commit_sha.is_some() {
        InvocationExecutionModeApi::Server
    } else {
        InvocationExecutionModeApi::Local
    };
    let prepared = match derived_execution_mode {
        crate::api::InvocationExecutionModeApi::Local => {
            let prepared = service
                .prepare_local_execution(
                    invocation_id,
                    map_invocation_command(request.command),
                    request.args.iter().cloned().map(Into::into).collect(),
                    &project,
                    &environment,
                )
                .await?;
            let execution_spec = match prepared.spec {
                PreparedExecutionSpec::Local(spec) => InvocationExecutionSpecApi::Local {
                    command: request.command,
                    args: spec
                        .args
                        .into_iter()
                        .map(|v| v.to_string_lossy().into_owned())
                        .collect(),
                    state_manifest: spec.state_manifest,
                },
                _ => {
                    return Err(ApiError(AppError::Internal(
                        "unexpected execution spec for local mode".to_string(),
                    )));
                }
            };
            let persistence = prepared.persistence.map(|p| InvocationPersistence {
                run_id: p.run_id,
                project_id: p.project_id,
                environment_id: p.environment_id,
                promote_base_manifest: p.promote_base_manifest,
                updates_actual_state: p.updates_actual_state,
            });
            PreparedInvocation {
                execution_spec,
                persistence,
                worker_queue: prepared.worker_queue,
                project_id: prepared.project_id,
                environment_id: prepared.environment_id,
            }
        }
        crate::api::InvocationExecutionModeApi::Server => {
            let prepared = match request.command {
                InvocationCommandApi::Release => {
                    service
                        .prepare_release_validation(
                            request.args.iter().cloned().map(Into::into).collect(),
                            project_id,
                            environment_slug,
                        )
                        .await?
                }
                _ => {
                    service
                        .prepare_remote_execution(
                            invocation_id,
                            map_invocation_command(request.command),
                            request.args.iter().cloned().map(Into::into).collect(),
                            project_id,
                            environment_slug,
                        )
                        .await?
                }
            };
            let execution_spec = match prepared.spec {
                PreparedExecutionSpec::Remote(spec) => InvocationExecutionSpecApi::Remote {
                    command: request.command,
                    args: spec
                        .args
                        .into_iter()
                        .map(|value| value.to_string_lossy().into_owned())
                        .collect(),
                    repo_url: spec.repo_url,
                    commit_sha: spec.commit_sha,
                    project_root: spec.project_root,
                    profiles_yml: spec.profiles_yml,
                    state_manifest: spec.state_manifest,
                },
                PreparedExecutionSpec::ReleaseValidation(spec) => {
                    InvocationExecutionSpecApi::ReleaseValidation {
                        repo_url: spec.repo_url,
                        git_ref: spec.git_ref,
                        git_commit_sha: spec.git_commit_sha,
                        git_branch: spec.git_branch,
                    }
                }
                _ => {
                    return Err(ApiError(AppError::Internal(
                        "unexpected execution spec for server mode".to_string(),
                    )));
                }
            };
            let persistence = prepared.persistence.map(|p| InvocationPersistence {
                run_id: p.run_id,
                project_id: p.project_id,
                environment_id: p.environment_id,
                promote_base_manifest: p.promote_base_manifest,
                updates_actual_state: p.updates_actual_state,
            });
            PreparedInvocation {
                execution_spec,
                persistence,
                worker_queue: prepared.worker_queue,
                project_id: prepared.project_id,
                environment_id: prepared.environment_id,
            }
        }
    };
    state
        .db
        .create_invocation(CreateInvocationInput {
            invocation_id,
            plan_id: None,
            run_id: prepared.persistence.as_ref().map(|p| p.run_id),
            project_id: prepared.project_id,
            environment_id: prepared.environment_id,
            project_draft_id: None,
            environment_draft_id: None,
            command: map_invocation_command(request.command).as_str().to_string(),
            execution_mode: derived_execution_mode,
            worker_queue: prepared.worker_queue.clone(),
            execution_spec: Some(prepared.execution_spec.clone()),
            promote_base_manifest: prepared
                .persistence
                .as_ref()
                .map(|p| p.promote_base_manifest)
                .unwrap_or(false),
            updates_actual_state: prepared
                .persistence
                .as_ref()
                .map(|p| p.updates_actual_state)
                .unwrap_or(false),
            claim_deadline_at: Some(invocation_claim_deadline_at(derived_execution_mode)),
        })
        .await?;
    state
        .bootstrap_invocation_started(invocation_id, prepared.persistence)
        .await?;
    info!(
        invocation_id = %invocation_id,
        execution_mode = ?derived_execution_mode,
        "created worker-claimable invocation"
    );
    Ok(Json(InvocationCreateResponse {
        invocation_id,
        execution_mode: derived_execution_mode,
        worker_queue: prepared.worker_queue,
    }))
}

#[utoipa::path(
    post,
    path = "/v1/invocations/claim-next",
    tag = "invocations",
    request_body = InvocationClaimNextApiRequest,
    responses(
        (status = 200, description = "Claimed invocation", body = InvocationClaimResponse),
        (status = 204, description = "No work available"),
        (status = 400, description = "Invalid request", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn invocation_claim_next(
    State(state): State<AppState>,
    Json(request): Json<InvocationClaimNextApiRequest>,
) -> Result<Response, ApiError> {
    reconcile_timed_out_invocations(&state).await?;
    let worker_queues = normalize_worker_queues(&request.worker_queues)?;
    let Some(claimed) = state
        .db
        .claim_next_invocation(&request.worker_id, request.execution_mode, &worker_queues)
        .await?
    else {
        return Ok(StatusCode::NO_CONTENT.into_response());
    };
    state
        .invocations
        .get_or_create(claimed.invocation_id, None)
        .await;
    info!(invocation_id = %claimed.invocation_id, "claimed next invocation execution");
    Ok(Json(claimed).into_response())
}

#[utoipa::path(
    post,
    path = "/v1/invocations/{id}/heartbeat",
    tag = "invocations",
    params(
        ("id" = Uuid, Path, description = "Invocation identifier")
    ),
    request_body = InvocationHeartbeatApiRequest,
    responses(
        (status = 200, description = "Heartbeat accepted", body = InvocationHeartbeatResponse),
        (status = 400, description = "Invalid request", body = ApiErrorResponse),
        (status = 404, description = "Invocation not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn invocation_heartbeat(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(request): Json<InvocationHeartbeatApiRequest>,
) -> Result<Json<InvocationHeartbeatResponse>, ApiError> {
    reconcile_timed_out_invocations(&state).await?;
    let cancel_requested = state
        .db
        .heartbeat_invocation(id, &request.worker_id, request.lease_token)
        .await?;
    Ok(Json(InvocationHeartbeatResponse { cancel_requested }))
}

#[utoipa::path(
    post,
    path = "/v1/invocations/{id}/cancel",
    tag = "invocations",
    params(
        ("id" = Uuid, Path, description = "Invocation identifier")
    ),
    request_body = InvocationCancelApiRequest,
    responses(
        (status = 204, description = "Cancel requested"),
        (status = 404, description = "Invocation not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn invocation_cancel(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(_request): Json<InvocationCancelApiRequest>,
) -> Result<StatusCode, ApiError> {
    reconcile_timed_out_invocations(&state).await?;
    if let Some(InvocationCancellationRecord {
        invocation_id,
        status,
        exit_code,
        error,
    }) = state.db.request_cancel_invocation(id).await?
    {
        if let Some((project_id, environment_id)) = state
            .db
            .force_complete_invocation(
                invocation_id,
                &crate::execution::ExecutionCompletion {
                    status,
                    exit_code,
                    error: Some(error.clone()),
                    dbt_version: None,
                    result: None,
                    manifest: None,
                },
            )
            .await?
        {
            auto_admit_blocked_plans_for_environment(&state, project_id, environment_id).await?;
        }
        publish_terminal_invocation(&state, invocation_id, exit_code, error.clone()).await?;
        info!(invocation_id = %id, status = ?status, error = %error, "canceled unclaimed invocation immediately");
        return Ok(StatusCode::NO_CONTENT);
    }
    info!(invocation_id = %id, "requested invocation cancel");
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    get,
    path = "/v1/invocations/{id}",
    tag = "invocations",
    params(
        ("id" = Uuid, Path, description = "Invocation identifier")
    ),
    responses(
        (status = 200, description = "Invocation status", body = InvocationStatusResponse),
        (status = 404, description = "Invocation not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn invocation_status(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<InvocationStatusResponse>, ApiError> {
    reconcile_timed_out_invocations(&state).await?;
    info!(invocation_id = %id, "loaded invocation status");
    Ok(Json(state.db.get_invocation_status(id).await?))
}

#[utoipa::path(
    get,
    path = "/v1/invocations",
    tag = "invocations",
    params(
        ("status" = Option<InvocationLifecycleStatus>, Query, description = "Filter by lifecycle status"),
        ("execution_mode" = Option<InvocationExecutionModeApi>, Query, description = "Filter by execution mode"),
        ("worker_queue" = Option<String>, Query, description = "Filter by worker queue"),
        ("claimed_by" = Option<String>, Query, description = "Filter by worker id"),
        ("cancel_state" = Option<InvocationCancelStateApi>, Query, description = "Filter by cancel state"),
        ("limit" = Option<i64>, Query, description = "Limit result count")
    ),
    responses(
        (status = 200, description = "Invocations", body = InvocationsResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn invocation_list(
    State(state): State<AppState>,
    Query(filter): Query<InvocationListApiRequest>,
) -> Result<Json<InvocationsResponse>, ApiError> {
    reconcile_timed_out_invocations(&state).await?;
    let invocations = state.db.list_invocations(filter).await?;
    info!(count = invocations.len(), "listed invocations");
    Ok(Json(InvocationsResponse { invocations }))
}

#[utoipa::path(
    post,
    path = "/v1/invocations/cleanup",
    tag = "invocations",
    request_body = InvocationCleanupApiRequest,
    responses(
        (status = 200, description = "Deleted old terminal invocations", body = InvocationCleanupResponse),
        (status = 400, description = "Invalid request", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn invocation_cleanup(
    State(state): State<AppState>,
    Json(request): Json<InvocationCleanupApiRequest>,
) -> Result<Json<InvocationCleanupResponse>, ApiError> {
    if request.older_than_seconds <= 0 {
        return Err(ApiError(AppError::InvalidInput(
            "older_than_seconds must be greater than 0".to_string(),
        )));
    }
    let cutoff = Utc::now() - chrono::Duration::seconds(request.older_than_seconds);
    let deleted = state
        .db
        .cleanup_terminal_invocations_older_than(cutoff)
        .await?;
    info!(
        older_than_seconds = request.older_than_seconds,
        deleted, "cleaned up terminal invocations"
    );
    Ok(Json(InvocationCleanupResponse { deleted }))
}

#[utoipa::path(
    post,
    path = "/v1/invocations/{id}/events",
    tag = "invocations",
    params(
        ("id" = Uuid, Path, description = "Invocation identifier")
    ),
    request_body = InvocationEventBatchApiRequest,
    responses(
        (status = 204, description = "Events appended"),
        (status = 400, description = "Invalid request", body = ApiErrorResponse),
        (status = 404, description = "Invocation not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn invocation_append_events(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(request): Json<InvocationEventBatchApiRequest>,
) -> Result<StatusCode, ApiError> {
    reconcile_timed_out_invocations(&state).await?;
    let runtime = state.invocations.get_or_create(id, None).await;
    let recorder = InvocationRecorder::new(state.db.clone(), id, runtime);
    if !recorder.is_running().await {
        return Err(ApiError(AppError::Internal(
            "invocation is already completed".to_string(),
        )));
    }
    state
        .db
        .get_invocation_persistence(id, Some(&request.worker_id), Some(request.lease_token))
        .await?;
    for event in request.events {
        recorder.record(event).await?;
    }
    info!(invocation_id = %id, "appended invocation events");
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/v1/invocations/{id}/complete",
    tag = "invocations",
    params(
        ("id" = Uuid, Path, description = "Invocation identifier")
    ),
    request_body = InvocationCompleteApiRequest,
    responses(
        (status = 204, description = "Invocation completed"),
        (status = 400, description = "Invalid request", body = ApiErrorResponse),
        (status = 404, description = "Invocation not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn invocation_complete(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(request): Json<InvocationCompleteApiRequest>,
) -> Result<StatusCode, ApiError> {
    reconcile_timed_out_invocations(&state).await?;
    state
        .complete_invocation(
            id,
            &request.worker_id,
            request.lease_token,
            request.completion,
        )
        .await?;
    info!(invocation_id = %id, "completed invocation via api");
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    get,
    path = "/v1/invocations/{id}/events",
    tag = "invocations",
    params(
        ("id" = Uuid, Path, description = "Invocation identifier"),
        ("after_sequence" = Option<u64>, Query, description = "Replay events strictly after this sequence number")
    ),
    responses(
        (status = 200, description = "Invocation event stream", content_type = "text/event-stream", body = String),
        (status = 404, description = "Invocation not found", body = ApiErrorResponse),
        (status = 500, description = "Server error", body = ApiErrorResponse)
    )
)]
pub(super) async fn invocation_events(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Query(query): Query<InvocationEventsQuery>,
    headers: axum::http::HeaderMap,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let runtime = state.invocations.get_or_create(id, None).await;
    let rx = runtime.subscribe();
    let header_resume = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    let after_sequence = query.after_sequence.or(header_resume).unwrap_or(0);
    let history = state
        .db
        .load_invocation_events_since(id, after_sequence)
        .await?;
    let buffered_events = history.len();
    let last_sequence = history.last().map(|item| item.0).unwrap_or(after_sequence);
    let stream = event_stream(
        history
            .into_iter()
            .map(
                |(sequence, event)| crate::invocation_runtime::SequencedInvocationEvent {
                    sequence,
                    event,
                },
            )
            .collect(),
        last_sequence,
        rx,
    );
    info!(invocation_id = %id, buffered_events, "subscribed to invocation event stream");
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}
