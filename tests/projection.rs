use dbtx::api::{
    EnvironmentActiveResourcesApiRequest, EnvironmentReconcileApiRequest,
    EnvironmentDraftUpdateApiRequest, SourceStateEventCreateApiRequest,
    EnvironmentReleaseApiRequest, EnvironmentRollbackApiRequest, InvocationCancelStateApi,
    InvocationClaimNextApiRequest, InvocationCommandApi, InvocationCompleteApiRequest,
    InvocationCreateApiRequest, InvocationEventBatchApiRequest, InvocationExecutionModeApi,
    InvocationHeartbeatApiRequest, InvocationLifecycleStatus, InvocationListApiRequest,
    ProjectDraftCreateApiRequest,
};
use dbtx::client::DaemonClient;
use dbtx::execution::{ExecutionCompletion, ExecutionEvent, ExecutionEventKind};
use dbtx::services::{infer_local_project_defaults, infer_remote_project_defaults};
use sqlx::{PgPool, Row};
use std::fs;
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use testcontainers_modules::{
    postgres::Postgres,
    testcontainers::{ContainerAsync, runners::AsyncRunner},
};
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn remote_invocation_requires_remote_project_mode() {
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;
    let repo = TempProjectRepo::new("proj");
    let client = DaemonClient::new(db.service_url().to_string());

    client
        .invocation_create(local_invocation_request(
            repo.project_dir(),
            InvocationCommandApi::Ls,
            Some("remote"),
        ))
        .await
        .expect("create local invocation");
    let project_id = read_project_id_from_dbt_project(repo.project_dir(), false);

    assert!(
        client
            .invocation_create(remote_invocation_request(
                &project_id,
                "remote",
                InvocationCommandApi::Ls,
            ))
            .await
            .is_err()
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn remote_invocation_requires_commit_pinned_immutable_environment() {
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;
    let repo = TempProjectRepo::new("proj");
    let client = DaemonClient::new(db.service_url().to_string());

    let project_id = read_project_id_from_dbt_project(repo.project_dir(), true);
    bootstrap_remote_project_and_env_direct(
        db.pool(),
        repo.project_dir(),
        &project_id,
        "mutable",
        None,
    )
    .await;

    assert!(
        client
            .invocation_create(remote_invocation_request(
                &project_id,
                "mutable",
                InvocationCommandApi::Ls,
            ))
            .await
            .is_err()
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn lease_tokens_enforce_invocation_ownership() {
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;
    let repo = TempProjectRepo::new("proj");
    let client = DaemonClient::new(db.service_url().to_string());

    bootstrap_project_and_env(db.pool(), &repo, "dev").await;
    let created = client
        .invocation_create(local_invocation_request(
            repo.project_dir(),
            InvocationCommandApi::Ls,
            Some("dev"),
        ))
        .await
        .expect("create invocation");
    let local_queue = created.worker_queue.clone();
    let claim = client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Local),
            worker_id: "worker-a".to_string(),
            worker_queues: vec![local_queue],
        })
        .await
        .expect("claim next")
        .expect("claimed invocation");
    let wrong_lease_token = Uuid::new_v4();

    assert!(
        client
            .invocation_heartbeat(
                created.invocation_id,
                InvocationHeartbeatApiRequest {
                    worker_id: "worker-a".to_string(),
                    lease_token: wrong_lease_token,
                },
            )
            .await
            .is_err()
    );
    assert!(
        client
            .invocation_append_events(
                created.invocation_id,
                InvocationEventBatchApiRequest {
                    worker_id: "worker-a".to_string(),
                    lease_token: wrong_lease_token,
                    events: vec![sample_execution_event("hello")],
                },
            )
            .await
            .is_err()
    );
    let heartbeat = client
        .invocation_heartbeat(
            created.invocation_id,
            InvocationHeartbeatApiRequest {
                worker_id: "worker-a".to_string(),
                lease_token: claim.lease_token,
            },
        )
        .await
        .expect("heartbeat with owned lease succeeds");
    assert!(!heartbeat.cancel_requested);
    assert!(
        client
            .invocation_complete(
                created.invocation_id,
                InvocationCompleteApiRequest {
                    worker_id: "worker-a".to_string(),
                    lease_token: wrong_lease_token,
                    completion: sample_execution_completion(
                        InvocationLifecycleStatus::Succeeded,
                        0
                    ),
                },
            )
            .await
            .is_err()
    );

    client
        .invocation_heartbeat(
            created.invocation_id,
            InvocationHeartbeatApiRequest {
                worker_id: "worker-a".to_string(),
                lease_token: claim.lease_token,
            },
        )
        .await
        .expect("heartbeat with valid lease token");
    client
        .invocation_append_events(
            created.invocation_id,
            InvocationEventBatchApiRequest {
                worker_id: "worker-a".to_string(),
                lease_token: claim.lease_token,
                events: vec![sample_execution_event("valid line")],
            },
        )
        .await
        .expect("append events with valid lease token");
    client
        .invocation_complete(
            created.invocation_id,
            InvocationCompleteApiRequest {
                worker_id: "worker-a".to_string(),
                lease_token: claim.lease_token,
                completion: sample_execution_completion(InvocationLifecycleStatus::Succeeded, 0),
            },
        )
        .await
        .expect("complete with valid lease token");

    let status = client
        .invocation_status(created.invocation_id)
        .await
        .expect("load invocation status");
    assert!(matches!(
        status.status,
        InvocationLifecycleStatus::Succeeded
    ));
    assert_eq!(status.claimed_by.as_deref(), Some("worker-a"));
    assert!(status.claimed_at.is_some());
    assert!(status.last_heartbeat_at.is_some());
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn selected_resources_are_tracked_until_node_finish_or_invocation_completion() {
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;
    let repo = TempProjectRepo::new("proj");
    let client = DaemonClient::new(db.service_url().to_string());
    let project_id = read_project_id_from_dbt_project(repo.project_dir(), true);

    bootstrap_remote_project_and_env_direct(
        db.pool(),
        repo.project_dir(),
        &project_id,
        "remote",
        Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
    )
    .await;

    let created = client
        .invocation_create(remote_invocation_request(
            &project_id,
            "remote",
            InvocationCommandApi::Run,
        ))
        .await
        .expect("create remote invocation");
    let claim = client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Server),
            worker_id: "worker-a".to_string(),
            worker_queues: vec![created.worker_queue.clone()],
        })
        .await
        .expect("claim invocation")
        .expect("claimed invocation");

    client
        .invocation_append_events(
            created.invocation_id,
            InvocationEventBatchApiRequest {
                worker_id: "worker-a".to_string(),
                lease_token: claim.lease_token,
                events: vec![sample_dbt_log_event(
                    r#"{"info":{"name":"Generic","msg":"DBTX_SELECTED_RESOURCES::{\"selected_resources\":[\"model.pkg.orders\",\"seed.pkg.customers\"]}"},"data":{}}"#,
                )],
            },
        )
        .await
        .expect("append dbt events");

    let active_resources = client
        .environment_active_resources(
            &project_id,
            "remote",
            EnvironmentActiveResourcesApiRequest {
                resource_type: Some("model".to_string()),
            },
        )
        .await
        .expect("active environment resources");
    assert_eq!(active_resources.resources.len(), 1);
    assert_eq!(active_resources.resources[0].unique_id, "model.pkg.orders");
    assert!(matches!(
        active_resources.resources[0].phase,
        dbtx::api::EnvironmentActiveResourcePhaseApi::Selected
    ));

    client
        .invocation_append_events(
            created.invocation_id,
            InvocationEventBatchApiRequest {
                worker_id: "worker-a".to_string(),
                lease_token: claim.lease_token,
                events: vec![sample_dbt_log_event(
                    r#"{"info":{"name":"NodeFinished","code":"Q025","invocation_id":"abc"},"data":{"node_info":{"unique_id":"model.pkg.orders","resource_type":"model","node_name":"orders","node_status":"success","node_started_at":"2025-01-01T00:00:00Z","node_finished_at":"2025-01-01T00:00:01Z"},"run_result":{"status":"success","execution_time":1.0}}}"#,
                )],
            },
        )
        .await
        .expect("append node finished event");

    client
        .invocation_complete(
            created.invocation_id,
            InvocationCompleteApiRequest {
                worker_id: "worker-a".to_string(),
                lease_token: claim.lease_token,
                completion: sample_execution_completion(InvocationLifecycleStatus::Failed, 1),
            },
        )
        .await
        .expect("complete invocation");

    let rows = sqlx::query(
        r#"
        SELECT unique_id, resource_type, finished_at, close_reason
        FROM invocation_selected_resources
        WHERE invocation_id = $1
        ORDER BY unique_id
        "#,
    )
    .bind(created.invocation_id)
    .fetch_all(db.pool())
    .await
    .expect("selected resource rows");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("unique_id"), "model.pkg.orders");
    assert_eq!(
        rows[0].get::<Option<String>, _>("close_reason").as_deref(),
        Some("completed")
    );
    assert!(rows[0].get::<Option<chrono::DateTime<chrono::Utc>>, _>("finished_at").is_some());
    assert_eq!(rows[1].get::<String, _>("unique_id"), "seed.pkg.customers");
    assert_eq!(
        rows[1].get::<Option<String>, _>("resource_type").as_deref(),
        Some("seed")
    );
    assert_eq!(
        rows[1].get::<Option<String>, _>("close_reason").as_deref(),
        Some("invocation_failed")
    );
    assert!(rows[1].get::<Option<chrono::DateTime<chrono::Utc>>, _>("finished_at").is_some());
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn source_state_reconcile_creates_and_admits_plan() {
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;
    let repo = TempProjectRepo::new("proj");
    let client = DaemonClient::new(db.service_url().to_string());
    let project_id = read_project_id_from_dbt_project(repo.project_dir(), true);
    let commit_sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    bootstrap_remote_project_and_env_direct(
        db.pool(),
        repo.project_dir(),
        &project_id,
        "remote",
        Some(commit_sha),
    )
    .await;
    seed_environment_actual_state_with_manifest(
        db.pool(),
        &project_id,
        "remote",
        commit_sha,
        &[
            ("source.pkg.raw_orders", "source"),
            ("model.pkg.orders", "model"),
            ("model.pkg.customers", "model"),
        ],
        &[
            ("source.pkg.raw_orders", "model.pkg.orders"),
            ("model.pkg.orders", "model.pkg.customers"),
        ],
    )
    .await;

    let actual_state = client
        .environment_actual_state(&project_id, "remote")
        .await
        .expect("environment actual state");
    assert_eq!(
        actual_state.actual_state.last_successful_commit_sha.as_deref(),
        Some(commit_sha)
    );

    client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_orders".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v2".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({
                    "reason": "new upstream data"
                }),
            },
        )
        .await
        .expect("create source state event");

    let plan = client
        .environment_reconcile(&project_id, "remote", EnvironmentReconcileApiRequest::default())
        .await
        .expect("create reconciliation plan")
        .plan;
    assert_eq!(plan.status, "planned");
    assert_eq!(plan.reason, "source_state_change");
    assert_eq!(plan.selection_spec.as_deref(), Some("source_downstream"));
    assert_eq!(
        plan.selected_resources,
        vec![
            "model.pkg.customers".to_string(),
            "model.pkg.orders".to_string(),
            "source.pkg.raw_orders".to_string(),
        ]
    );
    assert_eq!(plan.resource_count, 3);
    assert!(plan.source_event_id.is_some());

    let admitted = client
        .environment_plan_admit(plan.plan_id)
        .await
        .expect("admit reconciliation plan")
        .plan;
    assert_eq!(admitted.status, "admitted");
    assert!(admitted.admitted_invocation_id.is_some());

    let reloaded = client
        .environment_plan_get(plan.plan_id)
        .await
        .expect("reload admitted plan")
        .plan;
    assert_eq!(reloaded.status, "admitted");
    assert_eq!(reloaded.admitted_invocation_id, admitted.admitted_invocation_id);

    let linked_plan_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT plan_id FROM invocations WHERE invocation_id = $1",
    )
    .bind(admitted.admitted_invocation_id.expect("admitted invocation id"))
    .fetch_one(db.pool())
    .await
    .expect("load linked invocation plan id");
    assert_eq!(linked_plan_id, Some(plan.plan_id));
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn code_change_reconcile_uses_target_manifest_and_live_current_state() {
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;
    let repo = TempProjectRepo::new("proj");
    let client = DaemonClient::new(db.service_url().to_string());
    let project_id = read_project_id_from_dbt_project(repo.project_dir(), true);
    let baseline_commit = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let desired_commit = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    bootstrap_remote_project_and_env_direct(
        db.pool(),
        repo.project_dir(),
        &project_id,
        "remote",
        Some(desired_commit),
    )
    .await;
    seed_environment_actual_state_with_manifest(
        db.pool(),
        &project_id,
        "remote",
        baseline_commit,
        &[
            ("model.pkg.orders", "model"),
            ("model.pkg.customers", "model"),
        ],
        &[("model.pkg.orders", "model.pkg.customers")],
    )
    .await;
    seed_manifest_run_only(
        db.pool(),
        &project_id,
        "remote",
        desired_commit,
        &[
            ("model.pkg.orders", "model", Some("new-orders")),
            ("model.pkg.customers", "model", Some("same-customers")),
        ],
        &[("model.pkg.orders", "model.pkg.customers")],
    )
    .await;
    mark_current_node_state_reconciled(
        db.pool(),
        &project_id,
        "remote",
        "model.pkg.orders",
        Some("new-orders"),
    )
    .await;
    mark_current_node_state_reconciled(
        db.pool(),
        &project_id,
        "remote",
        "model.pkg.customers",
        Some("same-customers"),
    )
    .await;
    age_current_node_success(
        db.pool(),
        &project_id,
        "remote",
        "model.pkg.customers",
        chrono::Duration::minutes(5),
    )
    .await;

    let plan = client
        .environment_reconcile(&project_id, "remote", EnvironmentReconcileApiRequest::default())
        .await
        .expect("create code-change reconciliation plan")
        .plan;

    assert_eq!(plan.status, "planned");
    assert_eq!(plan.reason, "code_change");
    assert_eq!(plan.selection_spec.as_deref(), Some("state_modified_live_plus"));
    assert_eq!(plan.selected_resources, vec!["model.pkg.customers".to_string()]);
    assert_eq!(plan.resource_count, 1);
    assert_eq!(
        plan.metadata
            .get("planning_mode")
            .and_then(serde_json::Value::as_str),
        Some("live_state_diff")
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn claimed_invocation_timeout_fails_without_reclaim() {
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;
    let repo = TempProjectRepo::new("proj");
    let client = DaemonClient::new(db.service_url().to_string());

    bootstrap_project_and_env(db.pool(), &repo, "dev").await;
    let project_id = read_project_id_from_dbt_project(repo.project_dir(), true);

    let created = client
        .invocation_create(remote_invocation_request(
            &project_id,
            "dev",
            InvocationCommandApi::Ls,
        ))
        .await
        .expect("create invocation");
    let claim = client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Server),
            worker_id: "worker-a".to_string(),
            worker_queues: vec!["generic".to_string()],
        })
        .await
        .expect("claim next")
        .expect("claimed invocation");

    sqlx::query(
        "UPDATE invocations SET claimed_at = NOW() - INTERVAL '2 minutes', last_heartbeat_at = NOW() - INTERVAL '2 minutes' WHERE invocation_id = $1",
    )
    .bind(created.invocation_id)
    .execute(db.pool())
    .await
    .expect("age heartbeat");

    let status = client
        .invocation_status(created.invocation_id)
        .await
        .expect("load invocation status");
    assert!(matches!(status.status, InvocationLifecycleStatus::Failed));
    assert_eq!(status.error.as_deref(), Some("worker heartbeat timed out"));
    assert_eq!(status.claimed_by.as_deref(), Some("worker-a"));
    assert!(status.claimed_at.is_some());
    assert!(status.last_heartbeat_at.is_some());

    let reclaimed = client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Server),
            worker_id: "worker-b".to_string(),
            worker_queues: vec!["generic".to_string()],
        })
        .await
        .expect("claim next after timeout");
    assert!(
        reclaimed.is_none(),
        "timed out invocation should not be reclaimed"
    );

    assert!(
        client
            .invocation_append_events(
                created.invocation_id,
                InvocationEventBatchApiRequest {
                    worker_id: claim.worker_id.clone(),
                    lease_token: claim.lease_token,
                    events: vec![sample_execution_event("late line")],
                },
            )
            .await
            .is_err()
    );
    assert!(
        client
            .invocation_complete(
                created.invocation_id,
                InvocationCompleteApiRequest {
                    worker_id: claim.worker_id,
                    lease_token: claim.lease_token,
                    completion: sample_execution_completion(
                        InvocationLifecycleStatus::Succeeded,
                        0
                    ),
                },
            )
            .await
            .is_err()
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn local_invocations_use_shorter_claim_deadlines_than_server_invocations() {
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;
    let repo = TempProjectRepo::new("proj");
    let client = DaemonClient::new(db.service_url().to_string());

    bootstrap_project_and_env(db.pool(), &repo, "dev").await;
    let project_id = read_project_id_from_dbt_project(repo.project_dir(), true);

    let local = client
        .invocation_create(local_invocation_request(
            repo.project_dir(),
            InvocationCommandApi::Ls,
            Some("dev"),
        ))
        .await
        .expect("create local invocation");
    let server = client
        .invocation_create(remote_invocation_request(
            &project_id,
            "dev",
            InvocationCommandApi::Ls,
        ))
        .await
        .expect("create server invocation");

    let deadline_row = sqlx::query(
        "SELECT invocation_id, claim_deadline_at FROM invocations WHERE invocation_id IN ($1, $2)",
    )
    .bind(local.invocation_id)
    .bind(server.invocation_id)
    .fetch_all(db.pool())
    .await
    .expect("fetch deadlines");
    let mut local_deadline = None;
    let mut server_deadline = None;
    for row in deadline_row {
        let invocation_id: Uuid = row.get("invocation_id");
        let deadline = row.get::<Option<chrono::DateTime<chrono::Utc>>, _>("claim_deadline_at");
        if invocation_id == local.invocation_id {
            local_deadline = deadline;
        } else if invocation_id == server.invocation_id {
            server_deadline = deadline;
        }
    }
    let local_deadline = local_deadline.expect("local deadline");
    let server_deadline = server_deadline.expect("server deadline");
    assert!(
        server_deadline - local_deadline >= chrono::Duration::seconds(40),
        "expected server deadline to be materially later than local deadline"
    );

    sqlx::query("UPDATE invocations SET claim_deadline_at = NOW() - INTERVAL '1 second' WHERE invocation_id = $1")
        .bind(local.invocation_id)
        .execute(db.pool())
        .await
        .expect("expire local deadline");

    let invocations = client
        .invocation_list(InvocationListApiRequest::default())
        .await
        .expect("list invocations")
        .invocations;
    let local_status = invocations
        .iter()
        .find(|status| status.invocation_id == local.invocation_id)
        .expect("local invocation status");
    let server_status = invocations
        .iter()
        .find(|status| status.invocation_id == server.invocation_id)
        .expect("server invocation status");
    assert!(matches!(
        local_status.status,
        InvocationLifecycleStatus::Failed
    ));
    assert!(matches!(
        server_status.status,
        InvocationLifecycleStatus::Running
    ));
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn local_heartbeat_timeout_is_shorter_than_server_timeout() {
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;
    let repo = TempProjectRepo::new("proj");
    let client = DaemonClient::new(db.service_url().to_string());

    bootstrap_project_and_env(db.pool(), &repo, "dev").await;
    let project_id = read_project_id_from_dbt_project(repo.project_dir(), true);

    let local = client
        .invocation_create(local_invocation_request(
            repo.project_dir(),
            InvocationCommandApi::Ls,
            Some("dev"),
        ))
        .await
        .expect("create local invocation");
    let local_queue = local.worker_queue.clone();
    let server = client
        .invocation_create(remote_invocation_request(
            &project_id,
            "dev",
            InvocationCommandApi::Ls,
        ))
        .await
        .expect("create server invocation");

    client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Local),
            worker_id: "worker-local".to_string(),
            worker_queues: vec![local_queue],
        })
        .await
        .expect("claim local invocation")
        .expect("local claimed");
    client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Server),
            worker_id: "worker-server".to_string(),
            worker_queues: vec!["generic".to_string()],
        })
        .await
        .expect("claim server invocation")
        .expect("server claimed");

    sqlx::query(
        "UPDATE invocations SET claimed_at = NOW() - INTERVAL '30 seconds', last_heartbeat_at = NOW() - INTERVAL '30 seconds' WHERE invocation_id IN ($1, $2)",
    )
    .bind(local.invocation_id)
    .bind(server.invocation_id)
    .execute(db.pool())
    .await
    .expect("age heartbeats");

    let local_status = client
        .invocation_status(local.invocation_id)
        .await
        .expect("load local status");
    let server_status = client
        .invocation_status(server.invocation_id)
        .await
        .expect("load server status");
    assert!(matches!(
        local_status.status,
        InvocationLifecycleStatus::Failed
    ));
    assert!(matches!(
        server_status.status,
        InvocationLifecycleStatus::Running
    ));

    sqlx::query(
        "UPDATE invocations SET claimed_at = NOW() - INTERVAL '2 minutes', last_heartbeat_at = NOW() - INTERVAL '2 minutes' WHERE invocation_id = $1",
    )
    .bind(server.invocation_id)
    .execute(db.pool())
    .await
    .expect("age server heartbeat further");

    let server_status = client
        .invocation_status(server.invocation_id)
        .await
        .expect("reload server status");
    assert!(matches!(
        server_status.status,
        InvocationLifecycleStatus::Failed
    ));
    assert_eq!(
        server_status.error.as_deref(),
        Some("worker heartbeat timed out")
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn canceling_unclaimed_invocation_finishes_it_immediately() {
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;
    let repo = TempProjectRepo::new("proj");
    let client = DaemonClient::new(db.service_url().to_string());

    bootstrap_project_and_env(db.pool(), &repo, "dev").await;

    let created = client
        .invocation_create(local_invocation_request(
            repo.project_dir(),
            InvocationCommandApi::Ls,
            Some("dev"),
        ))
        .await
        .expect("create invocation");

    client
        .invocation_cancel(created.invocation_id, Default::default())
        .await
        .expect("cancel invocation");

    let status = client
        .invocation_status(created.invocation_id)
        .await
        .expect("load invocation status");
    assert!(matches!(status.status, InvocationLifecycleStatus::Canceled));
    assert!(matches!(
        status.cancel_state,
        InvocationCancelStateApi::Completed
    ));
    assert_eq!(status.error.as_deref(), Some("invocation canceled"));
    assert!(status.cancel_requested_at.is_some());

    let event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM invocation_events WHERE invocation_id = $1 AND event_type = 'invocation.completed'",
    )
    .bind(created.invocation_id)
    .fetch_one(db.pool())
    .await
    .expect("invocation completion event count");
    assert_eq!(event_count, 1);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn canceling_claimed_invocation_marks_cancel_requested_until_worker_finishes() {
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;
    let repo = TempProjectRepo::new("proj");
    let client = DaemonClient::new(db.service_url().to_string());

    bootstrap_project_and_env(db.pool(), &repo, "dev").await;
    let project_id = read_project_id_from_dbt_project(repo.project_dir(), true);

    let created = client
        .invocation_create(remote_invocation_request(
            &project_id,
            "dev",
            InvocationCommandApi::Ls,
        ))
        .await
        .expect("create invocation");
    let _claim = client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Server),
            worker_id: "worker-a".to_string(),
            worker_queues: vec!["generic".to_string()],
        })
        .await
        .expect("claim invocation")
        .expect("claimed");

    client
        .invocation_cancel(created.invocation_id, Default::default())
        .await
        .expect("cancel claimed invocation");

    let status = client
        .invocation_status(created.invocation_id)
        .await
        .expect("load invocation status");
    assert!(matches!(status.status, InvocationLifecycleStatus::Running));
    assert!(status.cancel_requested);
    assert!(matches!(
        status.cancel_state,
        InvocationCancelStateApi::Requested
    ));
    assert!(status.cancel_requested_at.is_some());
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn worker_and_queue_views_aggregate_running_invocations() {
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;
    let repo = TempProjectRepo::new("proj");
    let client = DaemonClient::new(db.service_url().to_string());

    bootstrap_project_and_env(db.pool(), &repo, "dev").await;
    let project_id = read_project_id_from_dbt_project(repo.project_dir(), true);

    let _server_generic_1 = client
        .invocation_create(remote_invocation_request(
            &project_id,
            "dev",
            InvocationCommandApi::Ls,
        ))
        .await
        .expect("create server invocation 1");
    let _server_generic_2 = client
        .invocation_create(remote_invocation_request(
            &project_id,
            "dev",
            InvocationCommandApi::Ls,
        ))
        .await
        .expect("create server invocation 2");
    let local_isolated = client
        .invocation_create(local_invocation_request(
            repo.project_dir(),
            InvocationCommandApi::Ls,
            Some("dev"),
        ))
        .await
        .expect("create local invocation");
    let local_queue = local_isolated.worker_queue.clone();

    let _claim_a = client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Server),
            worker_id: "worker-a".to_string(),
            worker_queues: vec!["generic".to_string()],
        })
        .await
        .expect("claim generic server work")
        .expect("claimed server work");
    let _claim_b = client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Local),
            worker_id: "worker-b".to_string(),
            worker_queues: vec![local_queue.clone()],
        })
        .await
        .expect("claim isolated local work")
        .expect("claimed local work");

    sqlx::query(
        "UPDATE invocations SET claimed_at = NOW() - INTERVAL '30 seconds', last_heartbeat_at = NOW() - INTERVAL '30 seconds' WHERE invocation_id = $1",
    )
    .bind(local_isolated.invocation_id)
    .execute(db.pool())
    .await
    .expect("age local claim to stale");

    let workers = client.worker_list().await.expect("worker list").workers;
    assert_eq!(workers.len(), 2);
    let worker_a = workers
        .iter()
        .find(|worker| worker.worker_id == "worker-a")
        .expect("worker-a");
    assert_eq!(worker_a.claimed_invocation_count, 1);
    assert_eq!(worker_a.worker_queues, vec!["generic".to_string()]);
    let worker_b = workers
        .iter()
        .find(|worker| worker.worker_id == "worker-b")
        .expect("worker-b");
    assert_eq!(worker_b.claimed_invocation_count, 1);
    assert_eq!(worker_b.worker_queues, vec![local_queue.clone()]);
    assert_eq!(format!("{:?}", worker_b.health), "Stale");

    let queues = client.queue_list().await.expect("queue list").queues;
    let generic = queues
        .iter()
        .find(|queue| queue.worker_queue == "generic")
        .expect("generic queue");
    assert_eq!(generic.pending_count, 1);
    assert_eq!(generic.claimed_count, 1);
    assert_eq!(generic.stale_claim_count, 0);
    assert!(generic.oldest_pending_at.is_some());

    let isolated = queues
        .iter()
        .find(|queue| queue.worker_queue == local_isolated.worker_queue)
        .expect("local queue");
    assert_eq!(isolated.pending_count, 0);
    assert_eq!(isolated.claimed_count, 1);
    assert_eq!(isolated.stale_claim_count, 1);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn environment_draft_api_round_trip_and_confirms_validated_draft() {
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;
    let repo = TempProjectRepo::new("proj");
    let client = DaemonClient::new(db.service_url().to_string());

    let project_id = read_project_id_from_dbt_project(repo.project_dir(), true);
    bootstrap_remote_project_only(db.pool(), repo.project_dir(), &project_id).await;

    let created = client
        .environment_draft_create(&project_id)
        .await
        .expect("create environment draft");
    assert_eq!(created.draft.status, "loading_git");

    let head_sha = git_rev_parse(repo.project_dir(), "HEAD");
    let request = environment_draft_update_request("api-env", "main", Some(&head_sha), false);

    let refreshed = client
        .environment_draft_refresh_branch(created.draft.id, request.clone())
        .await
        .expect("refresh branch");
    assert_eq!(refreshed.draft.status, "loading_git");
    assert_eq!(refreshed.draft.slug, "api-env");
    assert_eq!(refreshed.draft.git_branch.as_deref(), Some("main"));

    let validating = client
        .environment_draft_validate(created.draft.id, request)
        .await
        .expect("validate draft");
    assert_eq!(validating.draft.status, "validating");
    assert_eq!(validating.draft.git_commit_sha.as_deref(), Some(head_sha.as_str()));

    mark_environment_draft_validated(
        db.pool(),
        created.draft.id,
        "main",
        &head_sha,
        &["main", "preview"],
    )
    .await;

    let confirmed = client
        .environment_draft_confirm(created.draft.id)
        .await
        .expect("confirm validated draft");
    assert_eq!(confirmed.environment.slug, "api-env");
    assert_eq!(confirmed.environment.target_name, "api-env");
    assert_eq!(confirmed.environment.git_branch.as_deref(), Some("main"));
    assert_eq!(
        confirmed.environment.git_commit_sha.as_deref(),
        Some(head_sha.as_str())
    );
    assert!(!confirmed.environment.use_latest_commit);
    assert!(confirmed.environment.auto_deploy);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn environment_release_is_idempotent_and_rollback_records_forward_fix() {
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;
    let repo = TempProjectRepo::new("proj");
    let client = DaemonClient::new(db.service_url().to_string());

    fs::write(repo.project_dir().join("README.md"), "second commit\n").expect("write second commit file");
    git(&["add", "."], repo.project_dir().parent().expect("repo root"));
    git(&["commit", "-m", "second"], repo.project_dir().parent().expect("repo root"));

    let head_sha = git_rev_parse(repo.project_dir(), "HEAD");
    let previous_sha = git_rev_parse(repo.project_dir(), "HEAD~1");
    let project_id = read_project_id_from_dbt_project(repo.project_dir(), true);
    bootstrap_remote_project_and_env_direct(
        db.pool(),
        repo.project_dir(),
        &project_id,
        "release-api",
        Some(&head_sha),
    )
    .await;

    let initial_history = client
        .environment_history(&project_id, "release-api")
        .await
        .expect("load initial history");
    assert_eq!(initial_history.versions.len(), 1);

    let unchanged = client
        .environment_release(
            &project_id,
            "release-api",
            EnvironmentReleaseApiRequest {
                git_branch: Some("main".to_string()),
                git_commit_sha: Some(head_sha.clone()),
            },
        )
        .await
        .expect("idempotent release");
    assert_eq!(
        unchanged.environment.git_commit_sha.as_deref(),
        Some(head_sha.as_str())
    );
    let history_after_noop = client
        .environment_history(&project_id, "release-api")
        .await
        .expect("history after noop release");
    assert_eq!(history_after_noop.versions.len(), 1);

    let released = client
        .environment_release(
            &project_id,
            "release-api",
            EnvironmentReleaseApiRequest {
                git_branch: Some("main".to_string()),
                git_commit_sha: Some(previous_sha.clone()),
            },
        )
        .await
        .expect("release previous commit");
    assert_eq!(
        released.environment.git_commit_sha.as_deref(),
        Some(previous_sha.as_str())
    );
    let history_after_release = client
        .environment_history(&project_id, "release-api")
        .await
        .expect("history after release");
    assert_eq!(history_after_release.versions.len(), 2);
    assert_eq!(history_after_release.versions[0].reason, "released");

    let original_version = history_after_release
        .versions
        .iter()
        .find(|version| version.git_commit_sha.as_deref() == Some(head_sha.as_str()))
        .expect("original version present");

    let rolled_back = client
        .environment_rollback(
            &project_id,
            "release-api",
            EnvironmentRollbackApiRequest {
                version_id: original_version.id,
            },
        )
        .await
        .expect("rollback to original version");
    assert_eq!(
        rolled_back.environment.git_commit_sha.as_deref(),
        Some(head_sha.as_str())
    );
    let history_after_rollback = client
        .environment_history(&project_id, "release-api")
        .await
        .expect("history after rollback");
    assert_eq!(history_after_rollback.versions.len(), 3);
    assert_eq!(history_after_rollback.versions[0].reason, "rolled_back");
    assert_eq!(
        history_after_rollback.versions[0].git_commit_sha.as_deref(),
        Some(head_sha.as_str())
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn project_draft_api_round_trip_and_confirms_validated_draft() {
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;
    let repo = TempProjectRepo::new("proj");
    let client = DaemonClient::new(db.service_url().to_string());

    let repo_root = repo.project_dir().parent().expect("repo root");
    let project_root = dbtx::services::relative_project_root(repo_root, repo.project_dir());

    let created = client
        .project_draft_create(ProjectDraftCreateApiRequest {
            git_repo_url: "https://example.com/repo.git".to_string(),
            project_root: project_root.clone(),
        })
        .await
        .expect("create project draft");
    assert_eq!(created.draft.status, "draft");
    assert_eq!(created.draft.project_root, project_root);

    let validating = client
        .project_draft_validate(created.draft.id)
        .await
        .expect("start project draft validation");
    assert_eq!(validating.draft.status, "validating");

    let project_name = read_dbt_project_name(repo.project_dir());
    mark_project_draft_validated(
        db.pool(),
        created.draft.id,
        &project_name,
        "main",
    )
    .await;

    let reloaded = client
        .project_draft_get(created.draft.id)
        .await
        .expect("reload validated draft");
    assert_eq!(reloaded.draft.status, "validated");
    assert_eq!(reloaded.draft.project_name.as_deref(), Some(project_name.as_str()));
    assert_eq!(reloaded.draft.default_branch.as_deref(), Some("main"));

    let confirmed = client
        .project_draft_confirm(created.draft.id)
        .await
        .expect("confirm validated project draft");
    assert_eq!(confirmed.project.mode, "remote");
    assert_eq!(
        confirmed.project.git_repo_url.as_deref(),
        Some("https://example.com/repo.git")
    );
    assert_eq!(confirmed.project.project_root.as_deref(), Some(project_root.as_str()));
    assert_eq!(confirmed.project.project_name, project_name);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn validation_queue_routes_onboarding_but_not_normal_remote_invocations() {
    let db = TestDatabase::new_with_validation_queue("validation-only").await;
    reset_db(db.pool()).await;
    let repo = TempProjectRepo::new("proj");
    let client = DaemonClient::new(db.service_url().to_string());

    let repo_root = repo.project_dir().parent().expect("repo root");
    let project_root = dbtx::services::relative_project_root(repo_root, repo.project_dir());

    let project_draft = client
        .project_draft_create(ProjectDraftCreateApiRequest {
            git_repo_url: "https://example.com/repo.git".to_string(),
            project_root: project_root.clone(),
        })
        .await
        .expect("create project draft");
    let project_validation = client
        .project_draft_validate(project_draft.draft.id)
        .await
        .expect("validate project draft");

    let project_id = read_project_id_from_dbt_project(repo.project_dir(), true);
    bootstrap_remote_project_only(db.pool(), repo.project_dir(), &project_id).await;
    let env_draft = client
        .environment_draft_create(&project_id)
        .await
        .expect("create environment draft");
    let head_sha = git_rev_parse(repo.project_dir(), "HEAD");
    let env_request = environment_draft_update_request("queue-env", "main", Some(&head_sha), false);
    let env_prepare = client
        .environment_draft_refresh_branch(env_draft.draft.id, env_request.clone())
        .await
        .expect("refresh env branch");
    let env_validation = client
        .environment_draft_validate(env_draft.draft.id, env_request)
        .await
        .expect("validate env draft");

    bootstrap_remote_project_and_env_direct(
        db.pool(),
        repo.project_dir(),
        &project_id,
        "queue-env",
        Some(&head_sha),
    )
    .await;
    let normal_invocation = client
        .invocation_create(remote_invocation_request(
            &project_id,
            "queue-env",
            InvocationCommandApi::Ls,
        ))
        .await
        .expect("create normal remote invocation");

    let rows = sqlx::query(
        "SELECT invocation_id, worker_queue FROM invocations WHERE invocation_id IN ($1, $2, $3, $4)",
    )
    .bind(project_validation.invocation_id)
    .bind(env_prepare.invocation_id)
    .bind(env_validation.invocation_id)
    .bind(normal_invocation.invocation_id)
    .fetch_all(db.pool())
    .await
    .expect("load invocation queues");

    let mut queues = std::collections::HashMap::new();
    for row in rows {
        let invocation_id: Uuid = row.get("invocation_id");
        let worker_queue: String = row.get("worker_queue");
        queues.insert(invocation_id, worker_queue);
    }

    assert_eq!(
        queues.get(&project_validation.invocation_id).map(String::as_str),
        Some("validation-only")
    );
    assert_eq!(
        queues.get(&env_prepare.invocation_id).map(String::as_str),
        Some("validation-only")
    );
    assert_eq!(
        queues.get(&env_validation.invocation_id).map(String::as_str),
        Some("validation-only")
    );
    assert_eq!(
        queues.get(&normal_invocation.invocation_id).map(String::as_str),
        Some("generic")
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn invocation_list_filters_apply_to_operator_views() {
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;
    let repo = TempProjectRepo::new("proj");
    let client = DaemonClient::new(db.service_url().to_string());

    bootstrap_project_and_env(db.pool(), &repo, "dev").await;
    let project_id = read_project_id_from_dbt_project(repo.project_dir(), true);

    let running = client
        .invocation_create(remote_invocation_request(
            &project_id,
            "dev",
            InvocationCommandApi::Ls,
        ))
        .await
        .expect("create running invocation");
    let canceled = client
        .invocation_create(local_invocation_request(
            repo.project_dir(),
            InvocationCommandApi::Ls,
            Some("dev"),
        ))
        .await
        .expect("create canceled invocation");

    client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Server),
            worker_id: "worker-filter".to_string(),
            worker_queues: vec!["generic".to_string()],
        })
        .await
        .expect("claim running invocation")
        .expect("claimed");
    client
        .invocation_cancel(canceled.invocation_id, Default::default())
        .await
        .expect("cancel unclaimed invocation");

    let running_only = client
        .invocation_list(InvocationListApiRequest {
            status: Some(InvocationLifecycleStatus::Running),
            execution_mode: Some(InvocationExecutionModeApi::Server),
            worker_queue: Some("generic".to_string()),
            claimed_by: Some("worker-filter".to_string()),
            cancel_state: None,
            limit: Some(10),
        })
        .await
        .expect("filtered running invocation list")
        .invocations;
    assert_eq!(running_only.len(), 1);
    assert_eq!(running_only[0].invocation_id, running.invocation_id);

    let canceled_only = client
        .invocation_list(InvocationListApiRequest {
            status: None,
            execution_mode: None,
            worker_queue: None,
            claimed_by: None,
            cancel_state: Some(InvocationCancelStateApi::Completed),
            limit: Some(10),
        })
        .await
        .expect("filtered canceled invocation list")
        .invocations;
    assert_eq!(canceled_only.len(), 1);
    assert_eq!(canceled_only[0].invocation_id, canceled.invocation_id);
    assert!(matches!(
        canceled_only[0].status,
        InvocationLifecycleStatus::Canceled
    ));
}

struct TestDatabase {
    daemon: TestDaemon,
    pool: PgPool,
    _container: Option<ContainerAsync<Postgres>>,
}

impl TestDatabase {
    async fn new() -> Self {
        Self::new_inner(None).await
    }

    async fn new_with_validation_queue(queue: &str) -> Self {
        Self::new_inner(Some(queue)).await
    }

    async fn new_inner(validation_queue: Option<&str>) -> Self {
        if let Ok(url) = std::env::var("DBTX_TEST_DATABASE_URL") {
            let daemon = TestDaemon::start(&url, validation_queue);
            init_dbtx_schema(daemon.service_url());
            let pool = PgPool::connect(&url)
                .await
                .expect("connect external test db");
            return Self {
                daemon,
                pool,
                _container: None,
            };
        }

        let container = Postgres::default()
            .with_db_name("dbtx")
            .with_user("dbtx")
            .with_password("dbtx")
            .start()
            .await
            .expect("start postgres container");

        let host = container.get_host().await.expect("postgres host");
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .expect("postgres port");
        let url = format!("postgres://dbtx:dbtx@{host}:{port}/dbtx");
        let daemon = TestDaemon::start(&url, validation_queue);
        init_dbtx_schema(daemon.service_url());
        let pool = PgPool::connect(&url)
            .await
            .expect("connect testcontainer db");

        Self {
            daemon,
            pool,
            _container: Some(container),
        }
    }
    fn service_url(&self) -> &str {
        self.daemon.service_url()
    }

    fn pool(&self) -> &PgPool {
        &self.pool
    }
}

struct TestDaemon {
    service_url: String,
    child: Child,
}

impl TestDaemon {
    fn start(database_url: &str, validation_queue: Option<&str>) -> Self {
        let listen = next_listen_addr();
        let mut command = Command::new(env!("CARGO_BIN_EXE_dbtx-server"));
        command
            .args(["--listen", &listen])
            .env("DBTX_DATABASE_URL", database_url)
            .env("DBTX_SECRET_KEY", "test-secret-key")
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if let Some(validation_queue) = validation_queue {
            command.env("DBTX_VALIDATION_QUEUE", validation_queue);
        }
        let mut child = command.spawn().expect("start dbtx-server");

        let service_url = format!("http://{listen}");
        wait_for_server(&service_url, &mut child);
        Self { service_url, child }
    }

    fn service_url(&self) -> &str {
        &self.service_url
    }
}

impl Drop for TestDaemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn next_listen_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    drop(listener);
    addr.to_string()
}

fn wait_for_server(service_url: &str, child: &mut Child) {
    let deadline = Instant::now() + Duration::from_secs(10);
    let addr = service_url.trim_start_matches("http://");
    loop {
        if TcpStream::connect(addr).is_ok() {
            return;
        }
        if let Some(status) = child.try_wait().expect("poll dbtx-server") {
            panic!("dbtx-server exited early with status {status}");
        }
        if Instant::now() >= deadline {
            panic!("timed out waiting for dbtx-server at {service_url}");
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn init_dbtx_schema(service_url: &str) {
    let output = Command::new(env!("CARGO_BIN_EXE_dbtx"))
        .args(["state", "migrate"])
        .env("DBTX_SERVICE_URL", service_url)
        .output()
        .expect("run dbtx migrate");
    assert!(
        output.status.success(),
        "dbtx init failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

struct TempProjectRepo {
    _temp_dir: TempDir,
    project_dir: PathBuf,
}

impl TempProjectRepo {
    fn new(project_name: &str) -> Self {
        let temp_dir = TempDir::new().expect("temp dir");
        let project_dir = temp_dir.path().join("analytics");
        fs::create_dir_all(&project_dir).expect("create project dir");
        fs::write(
            project_dir.join("dbt_project.yml"),
            format!("name: {project_name}\nprofile: {project_name}\nversion: '1.0'\n"),
        )
        .expect("write dbt project");
        fs::write(
            project_dir.join("profiles.yml"),
            format!(
                "{project_name}:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      path: warehouse.duckdb\n      schema: main\n      threads: 4\n"
            ),
        )
        .expect("write profiles");
        git(&["init", "-b", "main"], temp_dir.path());
        git(
            &["config", "user.email", "dbtx@example.com"],
            temp_dir.path(),
        );
        git(&["config", "user.name", "dbtx"], temp_dir.path());
        git(
            &["remote", "add", "origin", "https://example.com/repo.git"],
            temp_dir.path(),
        );
        git(&["add", "."], temp_dir.path());
        git(&["commit", "-m", "initial"], temp_dir.path());
        Self {
            _temp_dir: temp_dir,
            project_dir,
        }
    }

    fn project_dir(&self) -> &Path {
        &self.project_dir
    }
}

fn read_project_id_from_dbt_project(project_dir: &Path, remote: bool) -> String {
    if remote {
        infer_remote_project_defaults(project_dir, None, None, None)
            .expect("infer remote project")
            .project_id
    } else {
        infer_local_project_defaults(project_dir, None, None, None)
            .expect("infer local project")
            .project_id
    }
}

fn git(args: &[&str], cwd: &Path) {
    let output = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .expect("run git");
    assert!(
        output.status.success(),
        "git failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

async fn reset_db(pool: &PgPool) {
    sqlx::query(
        "TRUNCATE invocation_events, invocations, environment_seeds, promoted_manifest_nodes, promoted_manifest_meta, current_node_state, manifest_edges, manifest_nodes, manifest_snapshots, node_executions, run_events, runs, environments, projects CASCADE",
    )
    .execute(pool)
    .await
    .expect("truncate db");
}

async fn bootstrap_project_and_env(pool: &PgPool, repo: &TempProjectRepo, slug: &str) {
    let commit_sha = git_rev_parse(repo.project_dir(), "HEAD");
    let project_id = read_project_id_from_dbt_project(repo.project_dir(), true);
    bootstrap_remote_project_and_env_direct(
        pool,
        repo.project_dir(),
        &project_id,
        slug,
        Some(&commit_sha),
    )
    .await;
}

async fn bootstrap_remote_project_only(pool: &PgPool, project_dir: &Path, project_id: &str) {
    let project_name = read_dbt_project_name(project_dir);
    let project_root = dbtx::services::relative_project_root(
        project_dir.parent().expect("repo root"),
        project_dir,
    );

    sqlx::query(
        r#"
        INSERT INTO projects (project_id, project_name, mode, git_repo_url, default_branch, project_root, metadata)
        VALUES ($1, $2, 'remote', 'https://example.com/repo.git', 'main', $3, '{}'::jsonb)
        ON CONFLICT (project_id) DO UPDATE
        SET project_name = EXCLUDED.project_name,
            mode = EXCLUDED.mode,
            git_repo_url = EXCLUDED.git_repo_url,
            default_branch = EXCLUDED.default_branch,
            project_root = EXCLUDED.project_root
        "#,
    )
    .bind(project_id)
    .bind(&project_name)
    .bind(&project_root)
    .execute(pool)
    .await
    .expect("upsert remote project");
}

async fn seed_environment_actual_state_with_manifest(
    pool: &PgPool,
    project_id: &str,
    slug: &str,
    commit_sha: &str,
    nodes: &[(&str, &str)],
    edges: &[(&str, &str)],
) {
    let scope = sqlx::query(
        r#"
        SELECT p.id AS project_pk, p.project_name, p.project_root, p.git_repo_url, e.id AS environment_pk
        FROM projects p
        JOIN environments e ON e.project_id = p.id
        WHERE p.project_id = $1 AND e.slug = $2
        "#,
    )
    .bind(project_id)
    .bind(slug)
    .fetch_one(pool)
    .await
    .expect("load project/environment scope");
    let project_pk: i64 = scope.get("project_pk");
    let environment_pk: i64 = scope.get("environment_pk");
    let project_name: String = scope.get("project_name");
    let project_root: Option<String> = scope.get("project_root");
    let git_repo_url: Option<String> = scope.get("git_repo_url");
    let run_id = Uuid::new_v4();
    let successful_at = chrono::Utc::now() - chrono::Duration::hours(1);

    sqlx::query(
        r#"
        INSERT INTO runs (
            run_id, project_id, environment_id, command, args, is_full_graph_run, execution_mode,
            git_branch, git_commit_sha, git_repo_url, project_root, project_name, project_ref,
            started_at, finished_at, exit_code, terminal_status
        )
        VALUES (
            $1, $2, $3, 'build', '[]'::jsonb, true, 'server',
            'main', $4, $5, $6, $7, $8,
            $9, $10, 0, 'succeeded'
        )
        "#,
    )
    .bind(run_id)
    .bind(project_pk)
    .bind(environment_pk)
    .bind(commit_sha)
    .bind(git_repo_url)
    .bind(project_root)
    .bind(&project_name)
    .bind(project_id)
    .bind(successful_at)
    .bind(successful_at)
    .execute(pool)
    .await
    .expect("insert baseline run");

    sqlx::query(
        r#"
        INSERT INTO manifest_snapshots (run_id, manifest, manifest_size_bytes, checksum)
        VALUES ($1, '{}'::jsonb, 2, 'fixture-checksum')
        "#,
    )
    .bind(run_id)
    .execute(pool)
    .await
    .expect("insert manifest snapshot");

    for (unique_id, resource_type) in nodes {
        let name = unique_id.rsplit('.').next().expect("node name");
        sqlx::query(
            r#"
            INSERT INTO manifest_nodes (
                run_id, unique_id, resource_type, name, package_name, original_file_path,
                tags, fqn, config, checksum, database_name, schema_name, alias, relation_name
            )
            VALUES (
                $1, $2, $3, $4, 'pkg', '', '[]'::jsonb, '[]'::jsonb, '{}'::jsonb,
                NULL, NULL, NULL, NULL, NULL
            )
            "#,
        )
        .bind(run_id)
        .bind(unique_id)
        .bind(resource_type)
        .bind(name)
        .execute(pool)
        .await
        .expect("insert manifest node");

        sqlx::query(
            r#"
            INSERT INTO current_node_state (
                project_id, environment_id, unique_id, last_run_id, status, resource_type,
                node_name, checksum, finished_at, last_success_at, updated_at
            )
            VALUES (
                $1, $2, $3, $4, 'succeeded', $5,
                $6, NULL, $7, $7, NOW()
            )
            ON CONFLICT (project_id, environment_id, unique_id) DO UPDATE
            SET last_run_id = EXCLUDED.last_run_id,
                status = EXCLUDED.status,
                resource_type = EXCLUDED.resource_type,
                node_name = EXCLUDED.node_name,
                finished_at = EXCLUDED.finished_at,
                last_success_at = EXCLUDED.last_success_at,
                updated_at = NOW()
            "#,
        )
        .bind(project_pk)
        .bind(environment_pk)
        .bind(unique_id)
        .bind(run_id)
        .bind(resource_type)
        .bind(name)
        .bind(successful_at)
        .execute(pool)
        .await
        .expect("seed current node state");
    }

    for (parent_unique_id, child_unique_id) in edges {
        sqlx::query(
            r#"
            INSERT INTO manifest_edges (run_id, parent_unique_id, child_unique_id)
            VALUES ($1, $2, $3)
            "#,
        )
        .bind(run_id)
        .bind(parent_unique_id)
        .bind(child_unique_id)
        .execute(pool)
        .await
        .expect("insert manifest edge");
    }

    sqlx::query(
        r#"
        INSERT INTO environment_actual_state (
            project_id, environment_id,
            last_attempted_run_id, last_attempted_commit_sha, last_attempted_at,
            last_successful_run_id, last_successful_commit_sha, last_successful_at,
            updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $3, $4, $5, $5)
        ON CONFLICT (project_id, environment_id) DO UPDATE
        SET last_attempted_run_id = EXCLUDED.last_attempted_run_id,
            last_attempted_commit_sha = EXCLUDED.last_attempted_commit_sha,
            last_attempted_at = EXCLUDED.last_attempted_at,
            last_successful_run_id = EXCLUDED.last_successful_run_id,
            last_successful_commit_sha = EXCLUDED.last_successful_commit_sha,
            last_successful_at = EXCLUDED.last_successful_at,
            updated_at = EXCLUDED.updated_at
        "#,
    )
    .bind(project_pk)
    .bind(environment_pk)
    .bind(run_id)
    .bind(commit_sha)
    .bind(successful_at)
    .execute(pool)
    .await
    .expect("seed environment actual state");
}

async fn seed_manifest_run_only(
    pool: &PgPool,
    project_id: &str,
    slug: &str,
    commit_sha: &str,
    nodes: &[(&str, &str, Option<&str>)],
    edges: &[(&str, &str)],
) {
    let scope = sqlx::query(
        r#"
        SELECT p.id AS project_pk, p.project_name, p.project_root, p.git_repo_url, e.id AS environment_pk
        FROM projects p
        JOIN environments e ON e.project_id = p.id
        WHERE p.project_id = $1 AND e.slug = $2
        "#,
    )
    .bind(project_id)
    .bind(slug)
    .fetch_one(pool)
    .await
    .expect("load scope for manifest-only run");
    let project_pk: i64 = scope.get("project_pk");
    let environment_pk: i64 = scope.get("environment_pk");
    let project_name: String = scope.get("project_name");
    let project_root: Option<String> = scope.get("project_root");
    let git_repo_url: Option<String> = scope.get("git_repo_url");
    let run_id = Uuid::new_v4();
    let finished_at = chrono::Utc::now();

    sqlx::query(
        r#"
        INSERT INTO runs (
            run_id, project_id, environment_id, command, args, is_full_graph_run, execution_mode,
            git_branch, git_commit_sha, git_repo_url, project_root, project_name, project_ref,
            started_at, finished_at, exit_code, terminal_status
        )
        VALUES (
            $1, $2, $3, 'manifest_prepare', '[]'::jsonb, false, 'server',
            'main', $4, $5, $6, $7, $8,
            $9, $9, 0, 'success'
        )
        "#,
    )
    .bind(run_id)
    .bind(project_pk)
    .bind(environment_pk)
    .bind(commit_sha)
    .bind(git_repo_url)
    .bind(project_root)
    .bind(&project_name)
    .bind(project_id)
    .bind(finished_at)
    .execute(pool)
    .await
    .expect("insert manifest-only run");

    let manifest = serde_json::json!({
        "metadata": {
            "project_name": project_name,
            "adapter_type": "duckdb"
        },
        "nodes": nodes.iter().map(|(unique_id, resource_type, checksum)| {
            (
                (*unique_id).to_string(),
                serde_json::json!({
                    "resource_type": resource_type,
                    "name": unique_id.rsplit('.').next().unwrap_or(*unique_id),
                    "package_name": "pkg",
                    "original_file_path": "",
                    "tags": [],
                    "fqn": [],
                    "config": {},
                    "checksum": { "checksum": checksum },
                })
            )
        }).collect::<serde_json::Map<String, serde_json::Value>>(),
        "parent_map": edges.iter().fold(serde_json::Map::new(), |mut map, (parent, child)| {
            let entry = map.entry((*child).to_string()).or_insert_with(|| serde_json::Value::Array(vec![]));
            entry.as_array_mut().expect("parent map array").push(serde_json::Value::String((*parent).to_string()));
            map
        }),
        "child_map": edges.iter().fold(serde_json::Map::new(), |mut map, (parent, child)| {
            let entry = map.entry((*parent).to_string()).or_insert_with(|| serde_json::Value::Array(vec![]));
            entry.as_array_mut().expect("child map array").push(serde_json::Value::String((*child).to_string()));
            map
        }),
        "sources": {},
        "macros": {},
        "docs": {},
        "exposures": {},
        "groups": {},
        "group_map": {},
        "metrics": {},
        "selectors": {},
        "semantic_models": {},
        "saved_queries": {},
        "unit_tests": {},
        "disabled": {},
        "functions": {}
    });

    sqlx::query(
        r#"
        INSERT INTO manifest_snapshots (run_id, manifest, manifest_size_bytes, checksum)
        VALUES ($1, $2, $3, 'target-checksum')
        "#,
    )
    .bind(run_id)
    .bind(sqlx::types::Json(manifest.clone()))
    .bind(serde_json::to_vec(&manifest).expect("serialize manifest").len() as i64)
    .execute(pool)
    .await
    .expect("insert target manifest snapshot");

    for (unique_id, resource_type, checksum) in nodes {
        let name = unique_id.rsplit('.').next().expect("node name");
        sqlx::query(
            r#"
            INSERT INTO manifest_nodes (
                run_id, unique_id, resource_type, name, package_name, original_file_path,
                tags, fqn, config, checksum, database_name, schema_name, alias, relation_name
            )
            VALUES (
                $1, $2, $3, $4, 'pkg', '', '[]'::jsonb, '[]'::jsonb, '{}'::jsonb,
                $5, NULL, NULL, NULL, NULL
            )
            "#,
        )
        .bind(run_id)
        .bind(unique_id)
        .bind(resource_type)
        .bind(name)
        .bind(checksum.map(ToString::to_string))
        .execute(pool)
        .await
        .expect("insert target manifest node");
    }

    for (parent_unique_id, child_unique_id) in edges {
        sqlx::query(
            r#"
            INSERT INTO manifest_edges (run_id, parent_unique_id, child_unique_id)
            VALUES ($1, $2, $3)
            "#,
        )
        .bind(run_id)
        .bind(parent_unique_id)
        .bind(child_unique_id)
        .execute(pool)
        .await
        .expect("insert target manifest edge");
    }
}

async fn mark_current_node_state_reconciled(
    pool: &PgPool,
    project_id: &str,
    slug: &str,
    unique_id: &str,
    checksum: Option<&str>,
) {
    let scope = sqlx::query(
        r#"
        SELECT p.id AS project_pk, e.id AS environment_pk
        FROM projects p
        JOIN environments e ON e.project_id = p.id
        WHERE p.project_id = $1 AND e.slug = $2
        "#,
    )
    .bind(project_id)
    .bind(slug)
    .fetch_one(pool)
    .await
    .expect("load current state scope");
    let project_pk: i64 = scope.get("project_pk");
    let environment_pk: i64 = scope.get("environment_pk");
    let now = chrono::Utc::now();

    sqlx::query(
        r#"
        UPDATE current_node_state
        SET checksum = $4,
            last_success_at = $5,
            updated_at = NOW()
        WHERE project_id = $1
          AND environment_id = $2
          AND unique_id = $3
        "#,
    )
    .bind(project_pk)
    .bind(environment_pk)
    .bind(unique_id)
    .bind(checksum)
    .bind(now)
    .execute(pool)
    .await
    .expect("update current node state");
}

async fn age_current_node_success(
    pool: &PgPool,
    project_id: &str,
    slug: &str,
    unique_id: &str,
    age: chrono::Duration,
) {
    let scope = sqlx::query(
        r#"
        SELECT p.id AS project_pk, e.id AS environment_pk
        FROM projects p
        JOIN environments e ON e.project_id = p.id
        WHERE p.project_id = $1 AND e.slug = $2
        "#,
    )
    .bind(project_id)
    .bind(slug)
    .fetch_one(pool)
    .await
    .expect("load scope for aging current state");
    let project_pk: i64 = scope.get("project_pk");
    let environment_pk: i64 = scope.get("environment_pk");
    let aged = chrono::Utc::now() - age;
    sqlx::query(
        r#"
        UPDATE current_node_state
        SET last_success_at = $4,
            updated_at = NOW()
        WHERE project_id = $1
          AND environment_id = $2
          AND unique_id = $3
        "#,
    )
    .bind(project_pk)
    .bind(environment_pk)
    .bind(unique_id)
    .bind(aged)
    .execute(pool)
    .await
    .expect("age current node success");
}

async fn mark_project_draft_validated(
    pool: &PgPool,
    draft_id: Uuid,
    project_name: &str,
    default_branch: &str,
) {
    sqlx::query(
        r#"
        UPDATE project_onboarding_drafts
        SET status = 'validated',
            validation_error = NULL,
            project_name = $2,
            default_branch = $3,
            validated_at = NOW(),
            updated_at = NOW()
        WHERE id = $1
        "#,
    )
    .bind(draft_id)
    .bind(project_name)
    .bind(default_branch)
    .execute(pool)
    .await
    .expect("mark project draft validated");
}

async fn bootstrap_remote_project_and_env_direct(
    pool: &PgPool,
    project_dir: &Path,
    project_id: &str,
    slug: &str,
    commit_sha: Option<&str>,
) {
    let project_name = read_dbt_project_name(project_dir);
    let project_root = dbtx::services::relative_project_root(
        project_dir.parent().expect("repo root"),
        project_dir,
    );

    let project_row = sqlx::query(
        r#"
        INSERT INTO projects (project_id, project_name, mode, git_repo_url, default_branch, project_root, metadata)
        VALUES ($1, $2, 'remote', 'https://example.com/repo.git', 'main', $3, '{}'::jsonb)
        ON CONFLICT (project_id) DO UPDATE
        SET project_name = EXCLUDED.project_name,
            mode = EXCLUDED.mode,
            git_repo_url = EXCLUDED.git_repo_url,
            default_branch = EXCLUDED.default_branch,
            project_root = EXCLUDED.project_root
        RETURNING id
        "#,
    )
    .bind(project_id)
    .bind(&project_name)
    .bind(&project_root)
    .fetch_one(pool)
    .await
    .expect("upsert project");
    let project_pk: i64 = project_row.get("id");

    let env_row = sqlx::query(
        r#"
        INSERT INTO environments (
            project_id, slug, profile_name, target_name, git_branch, git_commit_sha,
            use_latest_commit, auto_deploy, immutable, status, adapter_type, worker_queue,
            schema_name, threads, profile_config, profile_secrets, metadata
        )
        VALUES ($1, $2, $3, 'dev', 'main', $4, false, true, false, 'active', 'duckdb', 'generic',
                'main', 4, '{"path":"warehouse.duckdb"}'::jsonb, '{}'::jsonb, '{}'::jsonb)
        ON CONFLICT (project_id, slug) DO UPDATE
        SET git_branch = EXCLUDED.git_branch,
            git_commit_sha = EXCLUDED.git_commit_sha
        RETURNING id
        "#,
    )
    .bind(project_pk)
    .bind(slug)
    .bind(&project_name)
    .bind(commit_sha)
    .fetch_one(pool)
    .await
    .expect("upsert environment");
    let environment_pk: i64 = env_row.get("id");

    sqlx::query(
        r#"
        INSERT INTO environment_versions (
            environment_id, project_id, reason, git_branch, git_commit_sha,
            use_latest_commit, auto_deploy, immutable, baseline_environment_id, metadata
        )
        VALUES ($1, $2, 'created', 'main', $3, false, true, false, NULL, '{}'::jsonb)
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(environment_pk)
    .bind(project_pk)
    .bind(commit_sha)
    .execute(pool)
    .await
    .expect("insert environment version");
}

fn read_dbt_project_name(project_dir: &Path) -> String {
    let content = fs::read_to_string(project_dir.join("dbt_project.yml")).expect("read dbt_project");
    content
        .lines()
        .find_map(|line| line.strip_prefix("name: ").map(str::to_string))
        .expect("project name")
}

fn git_rev_parse(cwd: &Path, rev: &str) -> String {
    let output = Command::new("git")
        .args(["rev-parse", rev])
        .current_dir(cwd)
        .output()
        .expect("git rev-parse");
    assert!(
        output.status.success(),
        "git rev-parse failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

fn local_invocation_request(
    project_dir: &Path,
    command: InvocationCommandApi,
    environment_slug: Option<&str>,
) -> InvocationCreateApiRequest {
    InvocationCreateApiRequest {
        command,
        args: vec![],
        current_dir: Some(project_dir.display().to_string()),
        project_id: None,
        environment_slug: environment_slug.map(ToString::to_string),
    }
}

fn environment_draft_update_request(
    slug: &str,
    git_branch: &str,
    git_commit_sha: Option<&str>,
    use_latest_commit: bool,
) -> EnvironmentDraftUpdateApiRequest {
    EnvironmentDraftUpdateApiRequest {
        slug: slug.to_string(),
        git_branch: Some(git_branch.to_string()),
        git_commit_sha: git_commit_sha.map(ToString::to_string),
        use_latest_commit,
        auto_deploy: true,
        immutable: false,
        adapter_type: "duckdb".to_string(),
        schema_name: "main".to_string(),
        threads: Some(4),
        profile_config: serde_json::json!({ "path": "warehouse.duckdb" }),
        profile_secrets: serde_json::json!({}),
    }
}

async fn mark_environment_draft_validated(
    pool: &PgPool,
    draft_id: Uuid,
    git_branch: &str,
    git_commit_sha: &str,
    branch_options: &[&str],
) {
    let branch_options = serde_json::Value::Array(
        branch_options
            .iter()
            .map(|branch| serde_json::Value::String((*branch).to_string()))
            .collect(),
    );
    let commit_options = serde_json::json!([
        {
            "sha": git_commit_sha,
            "short_sha": &git_commit_sha[..8],
            "summary": "fixture commit",
            "committed_at": "",
        }
    ]);
    sqlx::query(
        r#"
        UPDATE environment_onboarding_drafts
        SET status = 'validated',
            git_branch = $2,
            git_commit_sha = $3,
            branch_options = $4,
            commit_options = $5,
            validated_at = NOW(),
            updated_at = NOW()
        WHERE id = $1
        "#,
    )
    .bind(draft_id)
    .bind(git_branch)
    .bind(git_commit_sha)
    .bind(sqlx::types::Json(branch_options))
    .bind(sqlx::types::Json(commit_options))
    .execute(pool)
    .await
    .expect("mark environment draft validated");
}

fn remote_invocation_request(
    project_id: &str,
    environment_slug: &str,
    command: InvocationCommandApi,
) -> InvocationCreateApiRequest {
    InvocationCreateApiRequest {
        command,
        args: vec![],
        current_dir: None,
        project_id: Some(project_id.to_string()),
        environment_slug: Some(environment_slug.to_string()),
    }
}

fn sample_execution_event(text: &str) -> ExecutionEvent {
    ExecutionEvent {
        kind: ExecutionEventKind::StdoutLine,
        occurred_at: chrono::Utc::now(),
        text: Some(text.to_string()),
        raw_line: Some(text.to_string()),
        dbt_event_name: None,
        node_unique_id: None,
        level: None,
        error: None,
    }
}

fn sample_dbt_log_event(raw_line: &str) -> ExecutionEvent {
    ExecutionEvent {
        kind: ExecutionEventKind::DbtLog,
        occurred_at: chrono::Utc::now(),
        text: None,
        raw_line: Some(raw_line.to_string()),
        dbt_event_name: None,
        node_unique_id: None,
        level: None,
        error: None,
    }
}

fn sample_execution_completion(
    status: InvocationLifecycleStatus,
    exit_code: i32,
) -> ExecutionCompletion {
    ExecutionCompletion {
        status,
        exit_code,
        error: None,
        dbt_version: None,
        manifest: None,
        result: None,
    }
}
