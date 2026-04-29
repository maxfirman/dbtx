mod common;

use common::{InProcessClient, TEMPLATE_CLONE_LOCK, connect_db_with_retry, connect_test_pool};
use dbtx::api::{
    EnvironmentActiveResourcesApiRequest, EnvironmentDraftUpdateApiRequest,
    EnvironmentReconcileApiRequest, EnvironmentReleaseApiRequest, EnvironmentRollbackApiRequest,
    InvocationCancelStateApi, InvocationClaimNextApiRequest, InvocationCommandApi,
    InvocationCompleteApiRequest, InvocationCreateApiRequest, InvocationEventBatchApiRequest,
    InvocationExecutionModeApi, InvocationHeartbeatApiRequest, InvocationLifecycleStatus,
    InvocationListApiRequest, ProjectDraftCreateApiRequest, SourceStateEventCreateApiRequest,
};
use dbtx::config::RuntimeConfig;
use dbtx::db::{DraftStatus, PlanStatus};
use dbtx::execution::{ExecutionCompletion, ExecutionEvent, ExecutionEventKind};
use dbtx::server::{AppState, router};
use dbtx::services::{
    code_change_input_fingerprint, infer_local_project_defaults, infer_remote_project_defaults,
    source_state_change_input_fingerprint, target_manifest_input_fingerprint,
};
use sqlx::{PgPool, Row};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
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
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();

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
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();

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
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();

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
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
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
    assert!(
        rows[0]
            .get::<Option<chrono::DateTime<chrono::Utc>>, _>("finished_at")
            .is_some()
    );
    assert_eq!(rows[1].get::<String, _>("unique_id"), "seed.pkg.customers");
    assert_eq!(
        rows[1].get::<Option<String>, _>("resource_type").as_deref(),
        Some("seed")
    );
    assert_eq!(
        rows[1].get::<Option<String>, _>("close_reason").as_deref(),
        Some("invocation_failed")
    );
    assert!(
        rows[1]
            .get::<Option<chrono::DateTime<chrono::Utc>>, _>("finished_at")
            .is_some()
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn source_state_reconcile_creates_and_admits_plan() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
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
        actual_state
            .actual_state
            .last_successful_commit_sha
            .as_deref(),
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
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create reconciliation plan")
        .plan;
    assert_eq!(plan.status, PlanStatus::Planned);
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
    assert_eq!(admitted.status, PlanStatus::Admitted);
    assert!(admitted.admitted_invocation_id.is_some());

    let reloaded = client
        .environment_plan_get(plan.plan_id)
        .await
        .expect("reload admitted plan")
        .plan;
    assert_eq!(reloaded.status, PlanStatus::Admitted);
    assert_eq!(
        reloaded.admitted_invocation_id,
        admitted.admitted_invocation_id
    );

    let linked_plan_id: Option<Uuid> =
        sqlx::query_scalar("SELECT plan_id FROM invocations WHERE invocation_id = $1")
            .bind(
                admitted
                    .admitted_invocation_id
                    .expect("admitted invocation id"),
            )
            .fetch_one(db.pool())
            .await
            .expect("load linked invocation plan id");
    assert_eq!(linked_plan_id, Some(plan.plan_id));
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn successful_source_state_plan_records_satisfaction_and_suppresses_reconcile() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
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
        ],
        &[("source.pkg.raw_orders", "model.pkg.orders")],
    )
    .await;

    let source_event = client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_orders".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v2".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({ "reason": "new upstream data" }),
            },
        )
        .await
        .expect("create source state event")
        .event;

    let plan = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create reconciliation plan")
        .plan;
    let admitted = client
        .environment_plan_admit(plan.plan_id)
        .await
        .expect("admit reconciliation plan")
        .plan;
    let invocation_id = admitted
        .admitted_invocation_id
        .expect("admitted invocation id");

    let claim = client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Server),
            worker_id: "worker-a".to_string(),
            worker_queues: vec!["generic".to_string()],
        })
        .await
        .expect("claim admitted invocation")
        .expect("invocation claimed");
    assert_eq!(claim.invocation_id, invocation_id);

    client
        .invocation_complete(
            invocation_id,
            InvocationCompleteApiRequest {
                worker_id: claim.worker_id,
                lease_token: claim.lease_token,
                completion: sample_execution_completion(InvocationLifecycleStatus::Succeeded, 0),
            },
        )
        .await
        .expect("complete admitted invocation");

    let satisfied_event_id: i64 = sqlx::query_scalar(
        r#"
        SELECT latest_satisfied_event_id
        FROM environment_source_state_status
        WHERE project_id = (SELECT id FROM projects WHERE project_id = $1)
          AND environment_id = (
              SELECT e.id
              FROM environments e
              JOIN projects p ON p.id = e.project_id
              WHERE p.project_id = $1 AND e.slug = $2
          )
          AND source_key = $3
        "#,
    )
    .bind(&project_id)
    .bind("remote")
    .bind("source.pkg.raw_orders")
    .fetch_one(db.pool())
    .await
    .expect("load satisfied source event");
    assert_eq!(satisfied_event_id, source_event.id);

    let err = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect_err("already satisfied source state should not create a new plan");
    assert!(
        err.to_string()
            .contains("environment is already reconciled to known desired state"),
        "{err}"
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn newer_source_state_event_after_satisfaction_creates_a_new_plan() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
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
        ],
        &[("source.pkg.raw_orders", "model.pkg.orders")],
    )
    .await;

    let first = client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_orders".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v1".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({ "reason": "first" }),
            },
        )
        .await
        .expect("create first source state event")
        .event;
    insert_source_state_satisfaction(
        db.pool(),
        &project_id,
        "remote",
        "source.pkg.raw_orders",
        first.id,
        first.state_version.as_deref(),
        first.observed_at,
    )
    .await;

    let second = client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_orders".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v2".to_string()),
                observed_at: Some(chrono::Utc::now() + chrono::Duration::seconds(10)),
                payload: serde_json::json!({ "reason": "second" }),
            },
        )
        .await
        .expect("create second source state event")
        .event;

    let plan = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create new reconciliation plan")
        .plan;
    assert_eq!(plan.reason, "source_state_change");
    assert_eq!(plan.source_event_id, Some(second.id));
    let source_event_ids = plan
        .metadata
        .get("source_event_ids")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert_eq!(source_event_ids.len(), 1);
    assert_eq!(source_event_ids[0].as_i64(), Some(second.id));
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn periodic_reconciler_creates_and_admits_source_state_plan() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
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

    let source_event = client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_orders".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v2".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({ "reason": "new upstream data" }),
            },
        )
        .await
        .expect("create source state event")
        .event;

    client.reconcile_tick().await.expect("reconcile tick");

    let plans = client
        .environment_plan_list(&project_id, "remote")
        .await
        .expect("list plans")
        .plans;
    let created_plan = plans
        .into_iter()
        .find(|p| p.reason == "source_state_change")
        .expect("source_state_change plan should exist");
    assert_eq!(created_plan.source_event_id, Some(source_event.id));
    assert_eq!(created_plan.status, PlanStatus::Admitted);
    assert!(created_plan.admitted_invocation_id.is_some());
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn source_state_event_with_unmatched_source_key_returns_empty_plan() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
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
        ],
        &[("source.pkg.raw_orders", "model.pkg.orders")],
    )
    .await;

    // Post a source event for a key that does NOT exist in the manifest
    client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.nonexistent".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v1".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({}),
            },
        )
        .await
        .expect("create source state event for unmatched key");

    // Reconcile should fail with empty plan — no downstream nodes to run
    let err = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect_err("unmatched source key should not create a plan");
    assert!(
        err.to_string().contains("no selected resources"),
        "expected empty plan error, got: {err}"
    );

    // No plans should have been created
    let plans = client
        .environment_plan_list(&project_id, "remote")
        .await
        .expect("list plans")
        .plans;
    assert!(
        plans.is_empty(),
        "no plans should exist for unmatched source key"
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn two_source_events_for_different_keys_produce_single_plan() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
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
            ("source.pkg.raw_customers", "source"),
            ("model.pkg.orders", "model"),
            ("model.pkg.customers", "model"),
            ("model.pkg.summary", "model"),
        ],
        &[
            ("source.pkg.raw_orders", "model.pkg.orders"),
            ("source.pkg.raw_customers", "model.pkg.customers"),
            ("model.pkg.orders", "model.pkg.summary"),
            ("model.pkg.customers", "model.pkg.summary"),
        ],
    )
    .await;

    // Post events for two different source keys
    let event_a = client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_orders".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v2".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({}),
            },
        )
        .await
        .expect("create source event for raw_orders")
        .event;

    let event_b = client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_customers".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v2".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({}),
            },
        )
        .await
        .expect("create source event for raw_customers")
        .event;

    let plan = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create reconciliation plan")
        .plan;

    assert_eq!(plan.reason, "source_state_change");
    assert_eq!(plan.selection_spec.as_deref(), Some("source_downstream"));

    // Both source trees should be included — all 5 nodes
    let mut expected = vec![
        "model.pkg.customers",
        "model.pkg.orders",
        "model.pkg.summary",
        "source.pkg.raw_customers",
        "source.pkg.raw_orders",
    ];
    expected.sort();
    let mut actual = plan.selected_resources.clone();
    actual.sort();
    assert_eq!(
        actual, expected,
        "plan should select downstream of both sources"
    );
    assert_eq!(plan.resource_count, 5);

    // metadata should contain both source event IDs
    let source_event_ids = plan
        .metadata
        .get("source_event_ids")
        .and_then(|v| v.as_array())
        .expect("source_event_ids in metadata");
    let mut ids: Vec<i64> = source_event_ids.iter().filter_map(|v| v.as_i64()).collect();
    ids.sort();
    let mut expected_ids = vec![event_a.id, event_b.id];
    expected_ids.sort();
    assert_eq!(
        ids, expected_ids,
        "metadata should reference both source events"
    );

    // Only one plan should exist
    let plans = client
        .environment_plan_list(&project_id, "remote")
        .await
        .expect("list plans")
        .plans;
    assert_eq!(
        plans.len(),
        1,
        "both source events should produce a single plan"
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn simultaneous_code_drift_and_source_state_creates_code_plan_then_source_plan() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
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
    // Seed a target manifest for the desired commit (with a changed checksum on orders)
    seed_manifest_run_only(
        db.pool(),
        &project_id,
        "remote",
        desired_commit,
        &[
            ("source.pkg.raw_orders", "source", None),
            ("model.pkg.orders", "model", Some("new-orders-checksum")),
            ("model.pkg.customers", "model", Some("same-customers")),
        ],
        &[
            ("source.pkg.raw_orders", "model.pkg.orders"),
            ("model.pkg.orders", "model.pkg.customers"),
        ],
    )
    .await;

    // Create a source state event while code drift also exists
    let source_event = client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_orders".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v2".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({}),
            },
        )
        .await
        .expect("create source state event")
        .event;

    // First reconcile should create a code_change plan (code drift takes priority)
    let code_plan = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create code_change plan")
        .plan;
    assert_eq!(code_plan.reason, "code_change");
    // metadata should note the pending source events
    assert_eq!(
        code_plan
            .metadata
            .get("source_event_count")
            .and_then(|v| v.as_i64()),
        Some(1),
        "code_change plan metadata should record pending source event count"
    );

    // Admit and complete the code change plan
    let admitted = client
        .environment_plan_admit(code_plan.plan_id)
        .await
        .expect("admit code_change plan")
        .plan;
    let invocation_id = admitted
        .admitted_invocation_id
        .expect("admitted invocation id");

    let claim = client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Server),
            worker_id: "worker-a".to_string(),
            worker_queues: vec!["generic".to_string()],
        })
        .await
        .expect("claim invocation")
        .expect("invocation claimed");
    assert_eq!(claim.invocation_id, invocation_id);

    client
        .invocation_complete(
            invocation_id,
            InvocationCompleteApiRequest {
                worker_id: claim.worker_id,
                lease_token: claim.lease_token,
                completion: sample_execution_completion(InvocationLifecycleStatus::Succeeded, 0),
            },
        )
        .await
        .expect("complete code_change invocation");

    // The completed invocation created a new baseline run. Seed manifest nodes for it
    // so the source downstream lookup can find the source node.
    let new_actual = client
        .environment_actual_state(&project_id, "remote")
        .await
        .expect("load actual state after code change")
        .actual_state;
    let new_run_id = new_actual
        .last_successful_run_id
        .expect("new baseline run id");
    sqlx::query(
        "INSERT INTO manifest_snapshots (run_id, manifest, manifest_size_bytes, checksum) VALUES ($1, '{}'::jsonb, 2, 'post-code-change')",
    )
    .bind(new_run_id)
    .execute(db.pool())
    .await
    .expect("seed manifest snapshot for new baseline");
    for (unique_id, resource_type) in [
        ("source.pkg.raw_orders", "source"),
        ("model.pkg.orders", "model"),
        ("model.pkg.customers", "model"),
    ] {
        let name = unique_id.rsplit('.').next().unwrap();
        sqlx::query(
            r#"
            INSERT INTO manifest_nodes (
                run_id, unique_id, resource_type, name, package_name, original_file_path,
                tags, fqn, config, checksum, database_name, schema_name, alias, relation_name
            )
            VALUES ($1, $2, $3, $4, 'pkg', '', '[]'::jsonb, '[]'::jsonb, '{}'::jsonb,
                    NULL, NULL, NULL, NULL, NULL)
            "#,
        )
        .bind(new_run_id)
        .bind(unique_id)
        .bind(resource_type)
        .bind(name)
        .execute(db.pool())
        .await
        .expect("seed manifest node for new baseline");
    }
    for (parent, child) in [
        ("source.pkg.raw_orders", "model.pkg.orders"),
        ("model.pkg.orders", "model.pkg.customers"),
    ] {
        sqlx::query(
            "INSERT INTO manifest_edges (run_id, parent_unique_id, child_unique_id) VALUES ($1, $2, $3)",
        )
        .bind(new_run_id)
        .bind(parent)
        .bind(child)
        .execute(db.pool())
        .await
        .expect("seed manifest edge for new baseline");
    }

    // Source event should still be unsatisfied (code_change doesn't mark source satisfaction)
    // Second reconcile should now create a source_state_change plan
    let source_plan = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create source_state_change plan after code change completes")
        .plan;
    assert_eq!(source_plan.reason, "source_state_change");
    assert_eq!(source_plan.source_event_id, Some(source_event.id));
    assert_eq!(
        source_plan.selection_spec.as_deref(),
        Some("source_downstream")
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn code_drift_with_empty_diff_returns_empty_plan() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
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
    // Seed target manifest with identical checksums — no actual changes
    seed_manifest_run_only(
        db.pool(),
        &project_id,
        "remote",
        desired_commit,
        &[
            ("model.pkg.orders", "model", None),
            ("model.pkg.customers", "model", None),
        ],
        &[("model.pkg.orders", "model.pkg.customers")],
    )
    .await;
    // Mark current node state as reconciled with matching checksums
    mark_current_node_state_reconciled(db.pool(), &project_id, "remote", "model.pkg.orders", None)
        .await;
    mark_current_node_state_reconciled(
        db.pool(),
        &project_id,
        "remote",
        "model.pkg.customers",
        None,
    )
    .await;

    // Reconcile should detect code drift but find no changed nodes → empty plan
    let err = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect_err("empty diff should not create a plan");
    assert!(
        err.to_string().contains("no selected resources"),
        "expected empty plan error, got: {err}"
    );

    // Actual state should be advanced to the desired commit (noop advance)
    let actual = client
        .environment_actual_state(&project_id, "remote")
        .await
        .expect("load actual state after empty diff")
        .actual_state;
    assert_eq!(
        actual.last_successful_commit_sha.as_deref(),
        Some(desired_commit),
        "actual state commit should be advanced for noop code drift"
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn admitted_source_plan_produces_invocation_with_select_args() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
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

    client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_orders".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v2".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({}),
            },
        )
        .await
        .expect("create source state event");

    let plan = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create plan")
        .plan;
    let admitted = client
        .environment_plan_admit(plan.plan_id)
        .await
        .expect("admit plan")
        .plan;
    let invocation_id = admitted
        .admitted_invocation_id
        .expect("admitted invocation id");

    // Check the run's args contain --select with the expected resources
    let row = sqlx::query(
        "SELECT r.args, r.is_full_graph_run FROM runs r JOIN invocations i ON i.run_id = r.run_id WHERE i.invocation_id = $1",
    )
    .bind(invocation_id)
    .fetch_one(db.pool())
    .await
    .expect("load run for admitted invocation");

    let args: serde_json::Value = row.get("args");
    let is_full_graph_run: bool = row.get("is_full_graph_run");
    let args_vec: Vec<String> = args
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();

    assert!(
        args_vec.contains(&"--select".to_string()),
        "args should contain --select, got: {args_vec:?}"
    );

    // All selected resources should appear after --select
    let select_idx = args_vec.iter().position(|a| a == "--select").unwrap();
    let select_args: Vec<&str> = args_vec[select_idx + 1..]
        .iter()
        .take_while(|a| !a.starts_with("--"))
        .map(|s| s.as_str())
        .collect();
    let mut expected = vec![
        "model.pkg.customers",
        "model.pkg.orders",
        "source.pkg.raw_orders",
    ];
    expected.sort();
    let mut actual: Vec<&str> = select_args.clone();
    actual.sort();
    assert_eq!(
        actual, expected,
        "select args should match plan selected_resources"
    );

    assert!(
        !is_full_graph_run,
        "selective plan should set is_full_graph_run=false"
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn selective_source_plan_does_not_promote_manifest() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
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
        ],
        &[("source.pkg.raw_orders", "model.pkg.orders")],
    )
    .await;

    client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_orders".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v2".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({}),
            },
        )
        .await
        .expect("create source state event");

    let plan = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create plan")
        .plan;
    let admitted = client
        .environment_plan_admit(plan.plan_id)
        .await
        .expect("admit plan")
        .plan;
    let invocation_id = admitted
        .admitted_invocation_id
        .expect("admitted invocation id");

    // Verify promote_base_manifest is false on the invocation
    let promote: bool = sqlx::query_scalar(
        "SELECT promote_base_manifest FROM invocations WHERE invocation_id = $1",
    )
    .bind(invocation_id)
    .fetch_one(db.pool())
    .await
    .expect("load promote_base_manifest");
    assert!(
        !promote,
        "selective source plan should not promote manifest"
    );

    // Complete the invocation and verify no promoted manifest was written
    let claim = client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Server),
            worker_id: "worker-a".to_string(),
            worker_queues: vec!["generic".to_string()],
        })
        .await
        .expect("claim invocation")
        .expect("invocation claimed");

    client
        .invocation_complete(
            invocation_id,
            InvocationCompleteApiRequest {
                worker_id: claim.worker_id,
                lease_token: claim.lease_token,
                completion: sample_execution_completion(InvocationLifecycleStatus::Succeeded, 0),
            },
        )
        .await
        .expect("complete invocation");

    let scope = sqlx::query(
        "SELECT p.id AS ppk, e.id AS epk FROM projects p JOIN environments e ON e.project_id = p.id WHERE p.project_id = $1 AND e.slug = $2",
    )
    .bind(&project_id)
    .bind("remote")
    .fetch_one(db.pool())
    .await
    .expect("load scope");
    let ppk: i64 = scope.get("ppk");
    let epk: i64 = scope.get("epk");

    let promoted_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM promoted_manifest_nodes WHERE project_id = $1 AND environment_id = $2",
    )
    .bind(ppk)
    .bind(epk)
    .fetch_one(db.pool())
    .await
    .expect("count promoted manifest nodes");
    assert_eq!(
        promoted_count, 0,
        "selective source plan should not promote manifest nodes"
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn partial_satisfaction_targets_only_unsatisfied_source_downstream() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
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
    // Two independent source trees with a shared downstream model
    seed_environment_actual_state_with_manifest(
        db.pool(),
        &project_id,
        "remote",
        commit_sha,
        &[
            ("source.pkg.raw_orders", "source"),
            ("source.pkg.raw_customers", "source"),
            ("model.pkg.orders", "model"),
            ("model.pkg.customers", "model"),
            ("model.pkg.summary", "model"),
        ],
        &[
            ("source.pkg.raw_orders", "model.pkg.orders"),
            ("source.pkg.raw_customers", "model.pkg.customers"),
            ("model.pkg.orders", "model.pkg.summary"),
            ("model.pkg.customers", "model.pkg.summary"),
        ],
    )
    .await;

    // Create events for both sources
    let event_orders = client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_orders".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v1".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({}),
            },
        )
        .await
        .expect("create orders source event")
        .event;
    let _event_customers = client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_customers".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v1".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({}),
            },
        )
        .await
        .expect("create customers source event")
        .event;

    // Satisfy only the orders source event
    insert_source_state_satisfaction(
        db.pool(),
        &project_id,
        "remote",
        "source.pkg.raw_orders",
        event_orders.id,
        Some("v1"),
        event_orders.observed_at,
    )
    .await;

    // Reconcile should only target the unsatisfied source (raw_customers) downstream
    let plan = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create plan for unsatisfied source")
        .plan;
    assert_eq!(plan.reason, "source_state_change");

    let source_keys = plan
        .metadata
        .get("source_keys")
        .and_then(|v| v.as_array())
        .expect("source_keys in metadata");
    let keys: Vec<&str> = source_keys.iter().filter_map(|v| v.as_str()).collect();
    assert_eq!(
        keys,
        vec!["source.pkg.raw_customers"],
        "only unsatisfied source should be in plan"
    );

    // selected_resources should include raw_customers downstream but NOT raw_orders downstream
    let mut resources = plan.selected_resources.clone();
    resources.sort();
    assert!(
        resources.contains(&"source.pkg.raw_customers".to_string()),
        "should include raw_customers"
    );
    assert!(
        resources.contains(&"model.pkg.customers".to_string()),
        "should include customers model"
    );
    assert!(
        resources.contains(&"model.pkg.summary".to_string()),
        "should include summary (downstream of customers)"
    );
    assert!(
        !resources.contains(&"source.pkg.raw_orders".to_string()),
        "should NOT include raw_orders (already satisfied)"
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn blocked_source_plan_completed_as_noop_when_events_already_satisfied() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
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
        ],
        &[("source.pkg.raw_orders", "model.pkg.orders")],
    )
    .await;

    // Create a source event
    let _source_event = client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_orders".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v2".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({}),
            },
        )
        .await
        .expect("create source event")
        .event;

    // Create and admit a first plan, then claim it to make it "running"
    let first_plan = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create first plan")
        .plan;
    let first_admitted = client
        .environment_plan_admit(first_plan.plan_id)
        .await
        .expect("admit first plan")
        .plan;
    let first_invocation_id = first_admitted
        .admitted_invocation_id
        .expect("first invocation id");

    let claim = client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Server),
            worker_id: "worker-a".to_string(),
            worker_queues: vec!["generic".to_string()],
        })
        .await
        .expect("claim first invocation")
        .expect("first invocation claimed");

    // Register active resource overlap so the second plan will be blocked
    insert_active_selected_resource(
        db.pool(),
        first_invocation_id,
        &project_id,
        "remote",
        "model.pkg.orders",
        Some("model"),
    )
    .await;

    // Create a second source event and plan — it should be blocked
    client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_orders".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v3".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({}),
            },
        )
        .await
        .expect("create second source event");

    let second_plan = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create second plan")
        .plan;
    let blocked = client
        .environment_plan_admit(second_plan.plan_id)
        .await
        .expect("attempt second plan admit")
        .plan;
    assert_eq!(blocked.status, PlanStatus::Blocked);

    // Satisfy the source events BEFORE completing the first invocation,
    // because completion triggers immediate blocked-plan re-admission.
    let latest_event_id: i64 = sqlx::query_scalar(
        r#"
        SELECT MAX(id) FROM source_state_events
        WHERE project_id = (SELECT id FROM projects WHERE project_id = $1)
          AND source_key = 'source.pkg.raw_orders'
        "#,
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .expect("find latest source event id");

    insert_source_state_satisfaction(
        db.pool(),
        &project_id,
        "remote",
        "source.pkg.raw_orders",
        latest_event_id,
        Some("v3"),
        chrono::Utc::now(),
    )
    .await;

    // Complete the first invocation — this triggers immediate blocked-plan sweep
    client
        .invocation_complete(
            first_invocation_id,
            InvocationCompleteApiRequest {
                worker_id: claim.worker_id,
                lease_token: claim.lease_token,
                completion: sample_execution_completion(InvocationLifecycleStatus::Succeeded, 0),
            },
        )
        .await
        .expect("complete first invocation");

    let reloaded = client
        .environment_plan_get(second_plan.plan_id)
        .await
        .expect("reload second plan after sweep")
        .plan;
    assert_eq!(
        reloaded.status,
        PlanStatus::Completed,
        "blocked plan should be completed as no-op when source events are already satisfied"
    );
    assert!(
        reloaded.admitted_invocation_id.is_none(),
        "no-op plan should not have an admitted invocation"
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn failed_source_plan_with_same_fingerprint_respects_backoff() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
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
        ],
        &[("source.pkg.raw_orders", "model.pkg.orders")],
    )
    .await;

    let source_event = client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_orders".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v1".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({}),
            },
        )
        .await
        .expect("create source event")
        .event;

    // Insert a failed plan with next_attempt_at 5 minutes in the future
    let fingerprint = source_state_change_input_fingerprint(&[source_event.id]);
    sqlx::query(
        r#"
        INSERT INTO environment_run_plans (
            plan_id, project_id, environment_id, status, reason, input_fingerprint,
            target_git_branch, target_git_commit_sha, baseline_run_id,
            selection_spec, selected_resources, resource_count,
            source_event_id, error, failure_count, next_attempt_at,
            created_at, updated_at, metadata
        )
        SELECT
            $3, p.id, e.id, 'failed', 'source_state_change', $4,
            'main', $5,
            (SELECT run_id FROM runs r WHERE r.project_id = p.id AND r.environment_id = e.id ORDER BY r.id DESC LIMIT 1),
            'source_downstream', '["model.pkg.orders","source.pkg.raw_orders"]'::jsonb, 2,
            $6, 'source rebuild failed', 1, NOW() + INTERVAL '5 minutes',
            NOW(), NOW(),
            jsonb_build_object('source_keys', '["source.pkg.raw_orders"]'::jsonb, 'source_event_ids', jsonb_build_array($6))
        FROM projects p
        JOIN environments e ON e.project_id = p.id
        WHERE p.project_id = $1 AND e.slug = $2
        "#,
    )
    .bind(&project_id)
    .bind("remote")
    .bind(Uuid::new_v4())
    .bind(&fingerprint)
    .bind(commit_sha)
    .bind(source_event.id)
    .execute(db.pool())
    .await
    .expect("insert failed source plan with backoff");

    // Reconcile tick should skip this environment due to backoff
    client.reconcile_tick().await.expect("reconcile tick");

    // No new plans should have been created (only the manually inserted failed one)
    let plans = client
        .environment_plan_list(&project_id, "remote")
        .await
        .expect("list plans")
        .plans;
    let source_plans: Vec<_> = plans
        .iter()
        .filter(|p| p.reason == "source_state_change")
        .collect();
    assert_eq!(
        source_plans.len(),
        1,
        "backoff should prevent creating a new plan"
    );
    assert_eq!(
        source_plans[0].status,
        PlanStatus::Failed,
        "only the original failed plan should exist"
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn periodic_reconciler_creates_and_admits_code_change_plan() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
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

    client.reconcile_tick().await.expect("reconcile tick");

    let plans = client
        .environment_plan_list(&project_id, "remote")
        .await
        .expect("list plans")
        .plans;
    let plan = plans
        .into_iter()
        .find(|p| p.reason == "code_change")
        .expect("code_change plan should exist");
    assert_eq!(plan.status, PlanStatus::Admitted);
    assert!(plan.admitted_invocation_id.is_some());
    assert_eq!(
        plan.selected_resources,
        vec![
            "model.pkg.customers".to_string(),
            "model.pkg.orders".to_string()
        ]
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn periodic_reconciler_starts_manifest_prepare_for_unseen_code_commit_before_planning() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let project_id = read_project_id_from_dbt_project(repo.project_dir(), true);
    let desired_commit = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let baseline_commit = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

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
        &[("model.pkg.orders", "model")],
        &[],
    )
    .await;

    db.client()
        .reconcile_tick()
        .await
        .expect("reconcile tick for manifest prepare");
    wait_for_manifest_prepare_invocation(db.pool(), &project_id, "remote", desired_commit).await;

    let row = sqlx::query(
        r#"
        SELECT status, target_git_commit_sha, invocation_id
        FROM environment_reconcile_preparations
        WHERE project_id = (SELECT id FROM projects WHERE project_id = $1)
          AND environment_id = (
              SELECT e.id
              FROM environments e
              JOIN projects p ON p.id = e.project_id
              WHERE p.project_id = $1 AND e.slug = $2
          )
          AND kind = 'target_manifest'
        "#,
    )
    .bind(&project_id)
    .bind("remote")
    .fetch_one(db.pool())
    .await
    .expect("load reconcile preparation record");
    assert_eq!(row.get::<String, _>("status"), "running");
    assert_eq!(
        row.get::<Option<String>, _>("target_git_commit_sha")
            .as_deref(),
        Some(desired_commit)
    );
    assert!(row.get::<Option<Uuid>, _>("invocation_id").is_some());

    let code_change_plan_count: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM environment_run_plans erp
        JOIN projects p ON p.id = erp.project_id
        JOIN environments e ON e.id = erp.environment_id
        WHERE p.project_id = $1
          AND e.slug = $2
          AND erp.reason = 'code_change'
        "#,
    )
    .bind(&project_id)
    .bind("remote")
    .fetch_one(db.pool())
    .await
    .expect("count code-change plans");
    assert_eq!(code_change_plan_count, 0);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn periodic_reconciler_bootstraps_fresh_environment_without_baseline() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
    let project_id = read_project_id_from_dbt_project(repo.project_dir(), true);
    let desired_commit = "dddddddddddddddddddddddddddddddddddddddd";

    bootstrap_remote_project_and_env_direct(
        db.pool(),
        repo.project_dir(),
        &project_id,
        "remote",
        Some(desired_commit),
    )
    .await;

    client
        .reconcile_tick()
        .await
        .expect("reconcile tick for manifest prepare");
    wait_for_manifest_prepare_invocation(db.pool(), &project_id, "remote", desired_commit).await;

    seed_manifest_run_only(
        db.pool(),
        &project_id,
        "remote",
        desired_commit,
        &[
            ("model.pkg.orders", "model", Some("orders-checksum")),
            ("model.pkg.customers", "model", Some("customers-checksum")),
        ],
        &[("model.pkg.orders", "model.pkg.customers")],
    )
    .await;

    client
        .reconcile_tick()
        .await
        .expect("reconcile tick for plan creation");
    let created_plan = wait_for_plan_reason(&client, &project_id, "remote", "code_change").await;
    let plan = wait_for_plan_status(&client, created_plan.plan_id, PlanStatus::Admitted).await;
    assert!(plan.admitted_invocation_id.is_some());
    assert_eq!(
        plan.selection_spec.as_deref(),
        Some("full_graph"),
        "fresh environments should bootstrap with a full-graph initial plan"
    );
    assert_eq!(
        plan.selected_resources,
        vec![
            "model.pkg.customers".to_string(),
            "model.pkg.orders".to_string(),
        ]
    );
    assert_eq!(plan.baseline_run_id, None);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn periodic_reconciler_respects_manifest_prepare_retry_backoff() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let project_id = read_project_id_from_dbt_project(repo.project_dir(), true);
    let desired_commit = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let baseline_commit = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

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
        &[("model.pkg.orders", "model")],
        &[],
    )
    .await;

    sqlx::query(
        r#"
        INSERT INTO environment_reconcile_preparations (
            project_id,
            environment_id,
            kind,
            input_fingerprint,
            target_git_commit_sha,
            status,
            invocation_id,
            error,
            failure_count,
            next_attempt_at,
            started_at,
            completed_at,
            updated_at
        )
        SELECT
            p.id,
            e.id,
            'target_manifest',
            $3,
            $3,
            'failed',
            NULL,
            'manifest prepare failed',
            2,
            NOW() + INTERVAL '5 minutes',
            NOW() - INTERVAL '1 minute',
            NOW() - INTERVAL '1 minute',
            NOW()
        FROM projects p
        JOIN environments e ON e.project_id = p.id
        WHERE p.project_id = $1
          AND e.slug = $2
        "#,
    )
    .bind(&project_id)
    .bind("remote")
    .bind(target_manifest_input_fingerprint(
        &code_change_input_fingerprint(
            desired_commit,
            latest_run_id_for_commit(db.pool(), &project_id, "remote", baseline_commit).await,
        ),
    ))
    .bind(desired_commit)
    .execute(db.pool())
    .await
    .expect("insert failed reconcile preparation");

    tokio::time::sleep(Duration::from_millis(900)).await;

    let manifest_prepare_count: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM invocations i
        JOIN runs r ON r.run_id = i.run_id
        JOIN projects p ON p.id = i.project_id
        JOIN environments e ON e.id = i.environment_id
        WHERE p.project_id = $1
          AND e.slug = $2
          AND i.command = 'manifest_prepare'
          AND r.git_commit_sha = $3
        "#,
    )
    .bind(&project_id)
    .bind("remote")
    .bind(desired_commit)
    .fetch_one(db.pool())
    .await
    .expect("count manifest prepare invocations");
    assert_eq!(manifest_prepare_count, 0);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn manual_reconcile_respects_manifest_prepare_retry_backoff() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
    let project_id = read_project_id_from_dbt_project(repo.project_dir(), true);
    let desired_commit = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let baseline_commit = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

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
        &[("model.pkg.orders", "model")],
        &[],
    )
    .await;
    let baseline_run_id =
        latest_run_id_for_commit(db.pool(), &project_id, "remote", baseline_commit).await;
    let input_fingerprint = target_manifest_input_fingerprint(&code_change_input_fingerprint(
        desired_commit,
        baseline_run_id,
    ));

    sqlx::query(
        r#"
        INSERT INTO environment_reconcile_preparations (
            project_id,
            environment_id,
            kind,
            input_fingerprint,
            target_git_commit_sha,
            status,
            invocation_id,
            error,
            failure_count,
            next_attempt_at,
            started_at,
            completed_at,
            updated_at
        )
        SELECT
            p.id,
            e.id,
            'target_manifest',
            $3,
            $4,
            'failed',
            NULL,
            'manifest prepare failed',
            2,
            NOW() + INTERVAL '5 minutes',
            NOW() - INTERVAL '1 minute',
            NOW() - INTERVAL '1 minute',
            NOW()
        FROM projects p
        JOIN environments e ON e.project_id = p.id
        WHERE p.project_id = $1
          AND e.slug = $2
        "#,
    )
    .bind(&project_id)
    .bind("remote")
    .bind(&input_fingerprint)
    .bind(desired_commit)
    .execute(db.pool())
    .await
    .expect("insert failed reconcile preparation");

    assert!(
        client
            .environment_reconcile(
                &project_id,
                "remote",
                EnvironmentReconcileApiRequest::default()
            )
            .await
            .is_err(),
        "manual reconcile should respect manifest prepare retry backoff"
    );

    let manifest_prepare_count: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM invocations i
        JOIN runs r ON r.run_id = i.run_id
        JOIN projects p ON p.id = i.project_id
        JOIN environments e ON e.id = i.environment_id
        WHERE p.project_id = $1
          AND e.slug = $2
          AND i.command = 'manifest_prepare'
          AND r.git_commit_sha = $3
        "#,
    )
    .bind(&project_id)
    .bind("remote")
    .bind(desired_commit)
    .fetch_one(db.pool())
    .await
    .expect("count manifest prepare invocations");
    assert_eq!(manifest_prepare_count, 0);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn periodic_reconciler_respects_failed_plan_retry_backoff() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
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

    sqlx::query(
        r#"
        INSERT INTO environment_run_plans (
            plan_id,
            project_id,
            environment_id,
            status,
            reason,
            input_fingerprint,
            target_git_branch,
            target_git_commit_sha,
            baseline_run_id,
            selection_spec,
            selected_resources,
            resource_count,
            error,
            failure_count,
            next_attempt_at,
            created_at,
            updated_at,
            metadata
        )
        SELECT
            $3,
            p.id,
            e.id,
            'failed',
            'code_change',
            $4,
            'main',
            $5,
            (
                SELECT run_id
                FROM runs r
                WHERE r.project_id = p.id
                  AND r.environment_id = e.id
                  AND r.git_commit_sha = $6
                ORDER BY r.id DESC
                LIMIT 1
            ),
            'state_modified_live_plus',
            '["model.pkg.orders","model.pkg.customers"]'::jsonb,
            2,
            'build failed',
            1,
            NOW() + INTERVAL '5 minutes',
            NOW(),
            NOW(),
            '{"planning_mode":"live_state_diff"}'::jsonb
        FROM projects p
        JOIN environments e ON e.project_id = p.id
        WHERE p.project_id = $1
          AND e.slug = $2
        "#,
    )
    .bind(&project_id)
    .bind("remote")
    .bind(Uuid::new_v4())
    .bind(code_change_input_fingerprint(
        desired_commit,
        latest_run_id_for_commit(db.pool(), &project_id, "remote", baseline_commit).await,
    ))
    .bind(desired_commit)
    .bind(baseline_commit)
    .execute(db.pool())
    .await
    .expect("insert failed code-change plan");

    tokio::time::sleep(Duration::from_millis(900)).await;

    let code_change_plan_count: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM environment_run_plans erp
        JOIN projects p ON p.id = erp.project_id
        JOIN environments e ON e.id = erp.environment_id
        WHERE p.project_id = $1
          AND e.slug = $2
          AND erp.reason = 'code_change'
        "#,
    )
    .bind(&project_id)
    .bind("remote")
    .fetch_one(db.pool())
    .await
    .expect("count code-change plans");
    assert_eq!(code_change_plan_count, 1);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn periodic_reconciler_bypasses_old_manifest_prepare_backoff_for_new_desired_commit() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let project_id = read_project_id_from_dbt_project(repo.project_dir(), true);
    let baseline_commit = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let old_desired_commit = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let new_desired_commit = "cccccccccccccccccccccccccccccccccccccccc";

    bootstrap_remote_project_and_env_direct(
        db.pool(),
        repo.project_dir(),
        &project_id,
        "remote",
        Some(new_desired_commit),
    )
    .await;
    seed_environment_actual_state_with_manifest(
        db.pool(),
        &project_id,
        "remote",
        baseline_commit,
        &[("model.pkg.orders", "model")],
        &[],
    )
    .await;

    sqlx::query(
        r#"
        INSERT INTO environment_reconcile_preparations (
            project_id,
            environment_id,
            kind,
            input_fingerprint,
            target_git_commit_sha,
            status,
            invocation_id,
            error,
            failure_count,
            next_attempt_at,
            started_at,
            completed_at,
            updated_at
        )
        SELECT
            p.id,
            e.id,
            'target_manifest',
            $3,
            $3,
            'failed',
            NULL,
            'manifest prepare failed',
            2,
            NOW() + INTERVAL '5 minutes',
            NOW() - INTERVAL '1 minute',
            NOW() - INTERVAL '1 minute',
            NOW()
        FROM projects p
        JOIN environments e ON e.project_id = p.id
        WHERE p.project_id = $1
          AND e.slug = $2
        "#,
    )
    .bind(&project_id)
    .bind("remote")
    .bind(target_manifest_input_fingerprint(
        &code_change_input_fingerprint(
            old_desired_commit,
            latest_run_id_for_commit(db.pool(), &project_id, "remote", baseline_commit).await,
        ),
    ))
    .bind(old_desired_commit)
    .execute(db.pool())
    .await
    .expect("insert failed old reconcile preparation");

    db.client()
        .reconcile_tick()
        .await
        .expect("reconcile tick for manifest prepare bypass");
    wait_for_manifest_prepare_invocation(db.pool(), &project_id, "remote", new_desired_commit)
        .await;
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn periodic_reconciler_bypasses_old_source_backoff_for_newer_source_event() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
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

    let first = client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_orders".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v1".to_string()),
                observed_at: Some(chrono::Utc::now() - chrono::Duration::minutes(2)),
                payload: serde_json::json!({ "reason": "first upstream data change" }),
            },
        )
        .await
        .expect("create first source event")
        .event;

    sqlx::query(
        r#"
        INSERT INTO environment_run_plans (
            plan_id,
            project_id,
            environment_id,
            status,
            reason,
            input_fingerprint,
            target_git_branch,
            target_git_commit_sha,
            baseline_run_id,
            selection_spec,
            selected_resources,
            resource_count,
            source_event_id,
            error,
            failure_count,
            next_attempt_at,
            created_at,
            updated_at,
            metadata
        )
        SELECT
            $3,
            p.id,
            e.id,
            'failed',
            'source_state_change',
            $4,
            'main',
            $5,
            (
                SELECT run_id
                FROM runs r
                WHERE r.project_id = p.id
                  AND r.environment_id = e.id
                  AND r.git_commit_sha = $5
                ORDER BY r.id DESC
                LIMIT 1
            ),
            'source_downstream',
            '["model.pkg.orders","model.pkg.customers"]'::jsonb,
            2,
            $6,
            'source rebuild failed',
            1,
            NOW() + INTERVAL '5 minutes',
            NOW(),
            NOW(),
            jsonb_build_object('source_keys', '["source.pkg.raw_orders"]'::jsonb, 'source_event_ids', jsonb_build_array($6))
        FROM projects p
        JOIN environments e ON e.project_id = p.id
        WHERE p.project_id = $1
          AND e.slug = $2
        "#,
    )
    .bind(&project_id)
    .bind("remote")
    .bind(Uuid::new_v4())
    .bind(source_state_change_input_fingerprint(&[first.id]))
    .bind(commit_sha)
    .bind(first.id)
    .execute(db.pool())
    .await
    .expect("insert failed source-state plan");

    let second = client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_orders".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v2".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({ "reason": "newer upstream data change" }),
            },
        )
        .await
        .expect("create second source event")
        .event;

    client
        .reconcile_tick()
        .await
        .expect("reconcile tick for source backoff bypass");
    let new_plan =
        wait_for_plan_reason(&client, &project_id, "remote", "source_state_change").await;
    assert_eq!(new_plan.source_event_id, Some(second.id));
    let admitted = wait_for_plan_status(&client, new_plan.plan_id, PlanStatus::Admitted).await;
    assert_eq!(admitted.source_event_id, Some(second.id));
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn blocked_plan_auto_admits_when_conflicting_invocation_completes() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
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

    client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_orders".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v1".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({ "reason": "first" }),
            },
        )
        .await
        .expect("create first source state event");
    let first_plan = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create first reconciliation plan")
        .plan;
    let first_admitted = client
        .environment_plan_admit(first_plan.plan_id)
        .await
        .expect("admit first reconciliation plan")
        .plan;
    let first_invocation_id = first_admitted
        .admitted_invocation_id
        .expect("first admitted invocation id");

    let claim = client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Server),
            worker_id: "worker-a".to_string(),
            worker_queues: vec!["generic".to_string()],
        })
        .await
        .expect("claim first invocation")
        .expect("first invocation claimed");

    insert_active_selected_resource(
        db.pool(),
        first_invocation_id,
        &project_id,
        "remote",
        "model.pkg.orders",
        Some("model"),
    )
    .await;

    client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_orders".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v2".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({ "reason": "second" }),
            },
        )
        .await
        .expect("create second source state event");
    let second_plan = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create second reconciliation plan")
        .plan;
    let blocked = client
        .environment_plan_admit(second_plan.plan_id)
        .await
        .expect("attempt second plan admit")
        .plan;
    assert_eq!(blocked.status, PlanStatus::Blocked);
    assert_eq!(blocked.blocked_by_invocation_id, Some(first_invocation_id));
    assert!(blocked.admitted_invocation_id.is_none());

    client
        .invocation_complete(
            first_invocation_id,
            InvocationCompleteApiRequest {
                worker_id: claim.worker_id,
                lease_token: claim.lease_token,
                completion: sample_execution_completion(InvocationLifecycleStatus::Succeeded, 0),
            },
        )
        .await
        .expect("complete blocking invocation");

    let reloaded = client
        .environment_plan_get(second_plan.plan_id)
        .await
        .expect("reload second plan after blocker completion")
        .plan;
    assert_eq!(reloaded.status, PlanStatus::Admitted);
    let auto_admitted_invocation_id = reloaded
        .admitted_invocation_id
        .expect("auto-admitted invocation id");
    assert_ne!(auto_admitted_invocation_id, first_invocation_id);

    let linked_plan_id: Option<Uuid> =
        sqlx::query_scalar("SELECT plan_id FROM invocations WHERE invocation_id = $1")
            .bind(auto_admitted_invocation_id)
            .fetch_one(db.pool())
            .await
            .expect("load auto-admitted invocation plan id");
    assert_eq!(linked_plan_id, Some(second_plan.plan_id));
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn blocked_plan_auto_admits_when_conflicting_invocation_cancels() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
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

    client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_orders".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v1".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({ "reason": "first" }),
            },
        )
        .await
        .expect("create first source state event");
    let first_plan = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create first reconciliation plan")
        .plan;
    let first_admitted = client
        .environment_plan_admit(first_plan.plan_id)
        .await
        .expect("admit first reconciliation plan")
        .plan;
    let first_invocation_id = first_admitted
        .admitted_invocation_id
        .expect("first admitted invocation id");

    let claim = client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Server),
            worker_id: "worker-a".to_string(),
            worker_queues: vec!["generic".to_string()],
        })
        .await
        .expect("claim first invocation")
        .expect("first invocation claimed");

    insert_active_selected_resource(
        db.pool(),
        first_invocation_id,
        &project_id,
        "remote",
        "model.pkg.orders",
        Some("model"),
    )
    .await;

    client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_orders".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v2".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({ "reason": "second" }),
            },
        )
        .await
        .expect("create second source state event");
    let second_plan = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create second reconciliation plan")
        .plan;
    let blocked = client
        .environment_plan_admit(second_plan.plan_id)
        .await
        .expect("attempt second plan admit")
        .plan;
    assert_eq!(blocked.status, PlanStatus::Blocked);

    client
        .invocation_cancel(first_invocation_id, Default::default())
        .await
        .expect("request cancel");
    client
        .invocation_complete(
            first_invocation_id,
            InvocationCompleteApiRequest {
                worker_id: claim.worker_id,
                lease_token: claim.lease_token,
                completion: sample_execution_completion(InvocationLifecycleStatus::Canceled, 130),
            },
        )
        .await
        .expect("complete canceled blocking invocation");

    let reloaded = client
        .environment_plan_get(second_plan.plan_id)
        .await
        .expect("reload second plan after cancel completion")
        .plan;
    assert_eq!(reloaded.status, PlanStatus::Admitted);
    assert!(reloaded.admitted_invocation_id.is_some());
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn blocked_plan_auto_admits_when_conflicting_invocation_times_out() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
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

    client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_orders".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v1".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({ "reason": "first" }),
            },
        )
        .await
        .expect("create first source state event");
    let first_plan = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create first reconciliation plan")
        .plan;
    let first_admitted = client
        .environment_plan_admit(first_plan.plan_id)
        .await
        .expect("admit first reconciliation plan")
        .plan;
    let first_invocation_id = first_admitted
        .admitted_invocation_id
        .expect("first admitted invocation id");

    let claim = client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Server),
            worker_id: "worker-a".to_string(),
            worker_queues: vec!["generic".to_string()],
        })
        .await
        .expect("claim first invocation")
        .expect("first invocation claimed");

    insert_active_selected_resource(
        db.pool(),
        first_invocation_id,
        &project_id,
        "remote",
        "model.pkg.orders",
        Some("model"),
    )
    .await;

    client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_orders".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v2".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({ "reason": "second" }),
            },
        )
        .await
        .expect("create second source state event");
    let second_plan = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create second reconciliation plan")
        .plan;
    let blocked = client
        .environment_plan_admit(second_plan.plan_id)
        .await
        .expect("attempt second plan admit")
        .plan;
    assert_eq!(blocked.status, PlanStatus::Blocked);

    sqlx::query(
        "UPDATE invocations SET claimed_at = NOW() - INTERVAL '2 minutes', last_heartbeat_at = NOW() - INTERVAL '2 minutes' WHERE invocation_id = $1",
    )
    .bind(first_invocation_id)
    .execute(db.pool())
    .await
    .expect("age heartbeat");

    let status = client
        .invocation_status(first_invocation_id)
        .await
        .expect("load timed out invocation status");
    assert!(matches!(status.status, InvocationLifecycleStatus::Failed));
    assert_eq!(status.claimed_by.as_deref(), Some(claim.worker_id.as_str()));

    let reloaded = client
        .environment_plan_get(second_plan.plan_id)
        .await
        .expect("reload second plan after timeout")
        .plan;
    assert_eq!(reloaded.status, PlanStatus::Admitted);
    assert!(reloaded.admitted_invocation_id.is_some());
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn blocked_plan_auto_admits_when_conflicting_invocation_fails() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
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

    client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_orders".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v1".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({ "reason": "first" }),
            },
        )
        .await
        .expect("create first source state event");
    let first_plan = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create first reconciliation plan")
        .plan;
    let first_admitted = client
        .environment_plan_admit(first_plan.plan_id)
        .await
        .expect("admit first reconciliation plan")
        .plan;
    let first_invocation_id = first_admitted
        .admitted_invocation_id
        .expect("first admitted invocation id");

    let claim = client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Server),
            worker_id: "worker-a".to_string(),
            worker_queues: vec!["generic".to_string()],
        })
        .await
        .expect("claim first invocation")
        .expect("first invocation claimed");

    insert_active_selected_resource(
        db.pool(),
        first_invocation_id,
        &project_id,
        "remote",
        "model.pkg.orders",
        Some("model"),
    )
    .await;

    client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_orders".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v2".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({ "reason": "second" }),
            },
        )
        .await
        .expect("create second source state event");
    let second_plan = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create second reconciliation plan")
        .plan;
    let blocked = client
        .environment_plan_admit(second_plan.plan_id)
        .await
        .expect("attempt second plan admit")
        .plan;
    assert_eq!(blocked.status, PlanStatus::Blocked);

    client
        .invocation_complete(
            first_invocation_id,
            InvocationCompleteApiRequest {
                worker_id: claim.worker_id,
                lease_token: claim.lease_token,
                completion: sample_execution_completion(InvocationLifecycleStatus::Failed, 1),
            },
        )
        .await
        .expect("complete failed blocking invocation");

    let reloaded = client
        .environment_plan_get(second_plan.plan_id)
        .await
        .expect("reload second plan after failure")
        .plan;
    assert_eq!(reloaded.status, PlanStatus::Admitted);
    assert!(reloaded.admitted_invocation_id.is_some());

    let close_reason: Option<String> = sqlx::query_scalar(
        "SELECT close_reason FROM invocation_selected_resources WHERE invocation_id = $1 AND unique_id = 'model.pkg.orders'",
    )
    .bind(first_invocation_id)
    .fetch_one(db.pool())
    .await
    .expect("load selected resource close reason");
    assert_eq!(close_reason.as_deref(), Some("invocation_failed"));
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn only_one_of_multiple_blocked_plans_for_same_resource_auto_admits() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
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

    for version in ["v1", "v2", "v3"] {
        client
            .environment_source_state_event_create(
                &project_id,
                "remote",
                SourceStateEventCreateApiRequest {
                    source_key: "source.pkg.raw_orders".to_string(),
                    provider: "manual".to_string(),
                    state_version: Some(version.to_string()),
                    observed_at: Some(chrono::Utc::now()),
                    payload: serde_json::json!({ "reason": version }),
                },
            )
            .await
            .expect("create source state event");
    }

    let first_plan = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create first plan")
        .plan;
    let first_admitted = client
        .environment_plan_admit(first_plan.plan_id)
        .await
        .expect("admit first plan")
        .plan;
    let first_invocation_id = first_admitted
        .admitted_invocation_id
        .expect("first admitted invocation id");
    let claim = client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Server),
            worker_id: "worker-a".to_string(),
            worker_queues: vec!["generic".to_string()],
        })
        .await
        .expect("claim first invocation")
        .expect("first invocation claimed");
    insert_active_selected_resource(
        db.pool(),
        first_invocation_id,
        &project_id,
        "remote",
        "model.pkg.orders",
        Some("model"),
    )
    .await;

    let second_plan = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create second plan")
        .plan;
    let third_plan = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create third plan")
        .plan;
    assert_eq!(
        client
            .environment_plan_admit(second_plan.plan_id)
            .await
            .expect("block second plan")
            .plan
            .status,
        PlanStatus::Blocked
    );
    assert_eq!(
        client
            .environment_plan_admit(third_plan.plan_id)
            .await
            .expect("block third plan")
            .plan
            .status,
        PlanStatus::Blocked
    );

    client
        .invocation_complete(
            first_invocation_id,
            InvocationCompleteApiRequest {
                worker_id: claim.worker_id,
                lease_token: claim.lease_token,
                completion: sample_execution_completion(InvocationLifecycleStatus::Succeeded, 0),
            },
        )
        .await
        .expect("complete first blocking invocation");

    let second_reloaded = client
        .environment_plan_get(second_plan.plan_id)
        .await
        .expect("reload second plan")
        .plan;
    let third_reloaded = client
        .environment_plan_get(third_plan.plan_id)
        .await
        .expect("reload third plan")
        .plan;
    // After the first plan succeeds and satisfies the source state,
    // both remaining plans should be completed as noops — there is no
    // remaining drift to reconcile.
    assert_eq!(second_reloaded.status, PlanStatus::Completed);
    assert_eq!(third_reloaded.status, PlanStatus::Completed);
    assert!(second_reloaded.admitted_invocation_id.is_none());
    assert!(third_reloaded.admitted_invocation_id.is_none());
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn admitted_plan_is_not_auto_admitted_again_on_later_completion() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
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

    client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_orders".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v1".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({ "reason": "first" }),
            },
        )
        .await
        .expect("create first source state event");
    let first_plan = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create first plan")
        .plan;
    let first_admitted = client
        .environment_plan_admit(first_plan.plan_id)
        .await
        .expect("admit first plan")
        .plan;
    let first_invocation_id = first_admitted
        .admitted_invocation_id
        .expect("first admitted invocation id");
    let first_claim = client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Server),
            worker_id: "worker-a".to_string(),
            worker_queues: vec!["generic".to_string()],
        })
        .await
        .expect("claim first invocation")
        .expect("first invocation claimed");
    insert_active_selected_resource(
        db.pool(),
        first_invocation_id,
        &project_id,
        "remote",
        "model.pkg.orders",
        Some("model"),
    )
    .await;

    client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_orders".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v2".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({ "reason": "second" }),
            },
        )
        .await
        .expect("create second source state event");
    let second_plan = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create second plan")
        .plan;
    assert_eq!(
        client
            .environment_plan_admit(second_plan.plan_id)
            .await
            .expect("block second plan")
            .plan
            .status,
        PlanStatus::Blocked
    );

    client
        .invocation_complete(
            first_invocation_id,
            InvocationCompleteApiRequest {
                worker_id: first_claim.worker_id,
                lease_token: first_claim.lease_token,
                completion: sample_execution_completion(InvocationLifecycleStatus::Succeeded, 0),
            },
        )
        .await
        .expect("complete first invocation");

    let admitted_invocation_id = client
        .environment_plan_get(second_plan.plan_id)
        .await
        .expect("reload second plan")
        .plan
        .admitted_invocation_id
        .expect("second plan admitted");

    let before_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM invocations WHERE plan_id = $1")
            .bind(second_plan.plan_id)
            .fetch_one(db.pool())
            .await
            .expect("count plan invocations before unrelated completion");
    assert_eq!(before_count, 1);

    let unrelated = client
        .invocation_create(remote_invocation_request(
            &project_id,
            "remote",
            InvocationCommandApi::Ls,
        ))
        .await
        .expect("create unrelated invocation");
    client
        .invocation_cancel(unrelated.invocation_id, Default::default())
        .await
        .expect("cancel unrelated invocation");

    let after_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM invocations WHERE plan_id = $1")
            .bind(second_plan.plan_id)
            .fetch_one(db.pool())
            .await
            .expect("count plan invocations after unrelated completion");
    assert_eq!(after_count, 1);
    let reloaded = client
        .environment_plan_get(second_plan.plan_id)
        .await
        .expect("reload second plan after unrelated completion")
        .plan;
    assert_eq!(reloaded.status, PlanStatus::Admitted);
    assert_eq!(
        reloaded.admitted_invocation_id,
        Some(admitted_invocation_id)
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn blocked_plan_auto_admits_when_unclaimed_conflicting_invocation_is_canceled() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
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

    client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_orders".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v1".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({ "reason": "first" }),
            },
        )
        .await
        .expect("create first source state event");
    let first_plan = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create first plan")
        .plan;
    let first_admitted = client
        .environment_plan_admit(first_plan.plan_id)
        .await
        .expect("admit first plan")
        .plan;
    let first_invocation_id = first_admitted
        .admitted_invocation_id
        .expect("first admitted invocation id");
    insert_active_selected_resource(
        db.pool(),
        first_invocation_id,
        &project_id,
        "remote",
        "model.pkg.orders",
        Some("model"),
    )
    .await;

    client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_orders".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v2".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({ "reason": "second" }),
            },
        )
        .await
        .expect("create second source state event");
    let second_plan = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create second plan")
        .plan;
    assert_eq!(
        client
            .environment_plan_admit(second_plan.plan_id)
            .await
            .expect("block second plan")
            .plan
            .status,
        PlanStatus::Blocked
    );

    client
        .invocation_cancel(first_invocation_id, Default::default())
        .await
        .expect("cancel unclaimed first invocation");

    let reloaded = client
        .environment_plan_get(second_plan.plan_id)
        .await
        .expect("reload second plan after unclaimed cancel")
        .plan;
    assert_eq!(reloaded.status, PlanStatus::Admitted);
    assert!(reloaded.admitted_invocation_id.is_some());

    let close_reason: Option<String> = sqlx::query_scalar(
        "SELECT close_reason FROM invocation_selected_resources WHERE invocation_id = $1 AND unique_id = 'model.pkg.orders'",
    )
    .bind(first_invocation_id)
    .fetch_one(db.pool())
    .await
    .expect("load selected resource close reason");
    assert_eq!(close_reason.as_deref(), Some("invocation_canceled"));
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn manual_admit_after_auto_admit_fails_cleanly() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
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

    client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_orders".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v1".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({ "reason": "first" }),
            },
        )
        .await
        .expect("create first source state event");
    let first_plan = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create first plan")
        .plan;
    let first_admitted = client
        .environment_plan_admit(first_plan.plan_id)
        .await
        .expect("admit first plan")
        .plan;
    let first_invocation_id = first_admitted
        .admitted_invocation_id
        .expect("first admitted invocation id");
    let first_claim = client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Server),
            worker_id: "worker-a".to_string(),
            worker_queues: vec!["generic".to_string()],
        })
        .await
        .expect("claim first invocation")
        .expect("first invocation claimed");
    insert_active_selected_resource(
        db.pool(),
        first_invocation_id,
        &project_id,
        "remote",
        "model.pkg.orders",
        Some("model"),
    )
    .await;

    client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_orders".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v2".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({ "reason": "second" }),
            },
        )
        .await
        .expect("create second source state event");
    let second_plan = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create second plan")
        .plan;
    assert_eq!(
        client
            .environment_plan_admit(second_plan.plan_id)
            .await
            .expect("block second plan")
            .plan
            .status,
        PlanStatus::Blocked
    );

    client
        .invocation_complete(
            first_invocation_id,
            InvocationCompleteApiRequest {
                worker_id: first_claim.worker_id,
                lease_token: first_claim.lease_token,
                completion: sample_execution_completion(InvocationLifecycleStatus::Succeeded, 0),
            },
        )
        .await
        .expect("complete first invocation");

    let reloaded = client
        .environment_plan_get(second_plan.plan_id)
        .await
        .expect("reload second plan after auto admit")
        .plan;
    assert_eq!(reloaded.status, PlanStatus::Admitted);
    assert!(
        client
            .environment_plan_admit(second_plan.plan_id)
            .await
            .is_err(),
        "manual admit after auto-admit should fail"
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn code_change_reconcile_uses_target_manifest_and_live_current_state() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
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
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create code-change reconciliation plan")
        .plan;

    assert_eq!(plan.status, PlanStatus::Planned);
    assert_eq!(plan.reason, "code_change");
    assert_eq!(
        plan.selection_spec.as_deref(),
        Some("state_modified_live_plus")
    );
    assert_eq!(
        plan.selected_resources,
        vec!["model.pkg.customers".to_string()]
    );
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
async fn reconcile_reuses_equivalent_pending_plan() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
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

    client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_orders".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v1".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({ "reason": "same" }),
            },
        )
        .await
        .expect("create source state event");

    let first = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create first reconcile plan")
        .plan;
    let second = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("reuse equivalent reconcile plan")
        .plan;

    assert_eq!(second.plan_id, first.plan_id);
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM environment_run_plans WHERE project_id = (SELECT id FROM projects WHERE project_id = $1)",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .expect("count plans");
    assert_eq!(count, 1);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn reconcile_supersedes_older_pending_plan_when_target_changes() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
    let project_id = read_project_id_from_dbt_project(repo.project_dir(), true);
    let baseline_commit = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let desired_commit_a = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let desired_commit_b = "cccccccccccccccccccccccccccccccccccccccc";

    bootstrap_remote_project_and_env_direct(
        db.pool(),
        repo.project_dir(),
        &project_id,
        "remote",
        Some(desired_commit_a),
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
        desired_commit_a,
        &[
            ("model.pkg.orders", "model", Some("new-orders-a")),
            ("model.pkg.customers", "model", Some("same-customers")),
        ],
        &[("model.pkg.orders", "model.pkg.customers")],
    )
    .await;

    let first = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create first plan")
        .plan;

    sqlx::query(
        "UPDATE environments SET git_commit_sha = $2 WHERE slug = $1 AND project_id = (SELECT id FROM projects WHERE project_id = $3)",
    )
    .bind("remote")
    .bind(desired_commit_b)
    .bind(&project_id)
    .execute(db.pool())
    .await
    .expect("update desired commit");
    seed_manifest_run_only(
        db.pool(),
        &project_id,
        "remote",
        desired_commit_b,
        &[
            ("model.pkg.orders", "model", Some("new-orders-b")),
            ("model.pkg.customers", "model", Some("same-customers")),
        ],
        &[("model.pkg.orders", "model.pkg.customers")],
    )
    .await;

    let second = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create superseding plan")
        .plan;

    assert_ne!(second.plan_id, first.plan_id);
    let first_reloaded = client
        .environment_plan_get(first.plan_id)
        .await
        .expect("reload first plan")
        .plan;
    assert_eq!(first_reloaded.status, PlanStatus::Superseded);
    assert_eq!(first_reloaded.superseded_by_plan_id, Some(second.plan_id));
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn reconcile_respects_active_environment_lease() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
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

    client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_orders".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v1".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({ "reason": "lease" }),
            },
        )
        .await
        .expect("create source state event");

    sqlx::query(
        r#"
        INSERT INTO environment_reconcile_leases (environment_id, owner, leased_until, updated_at)
        SELECT e.id, 'test-owner', NOW() + INTERVAL '30 seconds', NOW()
        FROM environments e
        JOIN projects p ON p.id = e.project_id
        WHERE p.project_id = $1
          AND e.slug = $2
        "#,
    )
    .bind(&project_id)
    .bind("remote")
    .execute(db.pool())
    .await
    .expect("insert active lease");

    assert!(
        client
            .environment_reconcile(
                &project_id,
                "remote",
                EnvironmentReconcileApiRequest::default()
            )
            .await
            .is_err(),
        "active reconcile lease should block a concurrent reconcile request"
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn blocked_code_change_plan_replans_to_latest_live_state_before_admit() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
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

    let created = client
        .invocation_create(remote_invocation_request(
            &project_id,
            "remote",
            InvocationCommandApi::Build,
        ))
        .await
        .expect("create blocking invocation");
    let _claim = client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Server),
            worker_id: "worker-a".to_string(),
            worker_queues: vec!["generic".to_string()],
        })
        .await
        .expect("claim blocking invocation")
        .expect("blocking invocation claimed");
    insert_active_selected_resource(
        db.pool(),
        created.invocation_id,
        &project_id,
        "remote",
        "model.pkg.orders",
        Some("model"),
    )
    .await;
    insert_active_selected_resource(
        db.pool(),
        created.invocation_id,
        &project_id,
        "remote",
        "model.pkg.customers",
        Some("model"),
    )
    .await;

    let plan = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create code-change plan")
        .plan;
    let blocked = client
        .environment_plan_admit(plan.plan_id)
        .await
        .expect("initial admit attempt")
        .plan;
    assert_eq!(blocked.status, PlanStatus::Blocked);
    assert_eq!(
        blocked.selected_resources,
        vec![
            "model.pkg.customers".to_string(),
            "model.pkg.orders".to_string()
        ]
    );

    mark_current_node_state_reconciled(
        db.pool(),
        &project_id,
        "remote",
        "model.pkg.orders",
        Some("new-orders"),
    )
    .await;

    let replanned = client
        .environment_plan_admit(plan.plan_id)
        .await
        .expect("re-admit blocked plan")
        .plan;
    assert_eq!(replanned.status, PlanStatus::Blocked);
    assert_eq!(
        replanned.selected_resources,
        vec!["model.pkg.customers".to_string()]
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn blocked_source_state_plan_completes_noop_when_source_event_is_already_satisfied() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
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

    let created = client
        .invocation_create(remote_invocation_request(
            &project_id,
            "remote",
            InvocationCommandApi::Build,
        ))
        .await
        .expect("create blocking invocation");
    let _claim = client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Server),
            worker_id: "worker-a".to_string(),
            worker_queues: vec!["generic".to_string()],
        })
        .await
        .expect("claim blocking invocation")
        .expect("blocking invocation claimed");
    insert_active_selected_resource(
        db.pool(),
        created.invocation_id,
        &project_id,
        "remote",
        "model.pkg.orders",
        Some("model"),
    )
    .await;

    let source_event = client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_orders".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v1".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({ "reason": "new data" }),
            },
        )
        .await
        .expect("create source state event")
        .event;
    let plan = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create source-state plan")
        .plan;
    let blocked = client
        .environment_plan_admit(plan.plan_id)
        .await
        .expect("initial admit attempt")
        .plan;
    assert_eq!(blocked.status, PlanStatus::Blocked);

    insert_source_state_satisfaction(
        db.pool(),
        &project_id,
        "remote",
        "source.pkg.raw_orders",
        source_event.id,
        source_event.state_version.as_deref(),
        source_event.observed_at,
    )
    .await;

    let no_op = client
        .environment_plan_admit(plan.plan_id)
        .await
        .expect("re-admit source-state plan after satisfaction")
        .plan;
    assert_eq!(no_op.status, PlanStatus::Completed);
    assert!(no_op.admitted_invocation_id.is_none());
    assert_eq!(no_op.selected_resources, Vec::<String>::new());
    assert_eq!(
        no_op.error.as_deref(),
        Some("source-triggered plan already satisfied by a successful plan")
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn periodic_blocked_plan_sweep_replans_and_auto_admits_code_change_work() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
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

    let created = client
        .invocation_create(remote_invocation_request(
            &project_id,
            "remote",
            InvocationCommandApi::Build,
        ))
        .await
        .expect("create blocking invocation");
    let _claim = client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Server),
            worker_id: "worker-a".to_string(),
            worker_queues: vec!["generic".to_string()],
        })
        .await
        .expect("claim blocking invocation")
        .expect("blocking invocation claimed");
    insert_active_selected_resource(
        db.pool(),
        created.invocation_id,
        &project_id,
        "remote",
        "model.pkg.orders",
        Some("model"),
    )
    .await;

    let plan = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create code-change plan")
        .plan;
    let blocked = client
        .environment_plan_admit(plan.plan_id)
        .await
        .expect("initial admit attempt")
        .plan;
    assert_eq!(blocked.status, PlanStatus::Blocked);
    assert_eq!(
        blocked.selected_resources,
        vec![
            "model.pkg.customers".to_string(),
            "model.pkg.orders".to_string()
        ]
    );

    mark_current_node_state_reconciled(
        db.pool(),
        &project_id,
        "remote",
        "model.pkg.orders",
        Some("new-orders"),
    )
    .await;

    client.sweep_tick().await.expect("sweep tick");

    let admitted = client
        .environment_plan_get(plan.plan_id)
        .await
        .expect("reload plan after sweep")
        .plan;
    assert_eq!(admitted.status, PlanStatus::Admitted);
    assert_eq!(
        admitted.selected_resources,
        vec!["model.pkg.customers".to_string()]
    );
    assert!(admitted.admitted_invocation_id.is_some());
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn periodic_blocked_plan_sweep_completes_satisfied_source_plan_noop() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
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

    let created = client
        .invocation_create(remote_invocation_request(
            &project_id,
            "remote",
            InvocationCommandApi::Build,
        ))
        .await
        .expect("create blocking invocation");
    let _claim = client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Server),
            worker_id: "worker-a".to_string(),
            worker_queues: vec!["generic".to_string()],
        })
        .await
        .expect("claim blocking invocation")
        .expect("blocking invocation claimed");
    insert_active_selected_resource(
        db.pool(),
        created.invocation_id,
        &project_id,
        "remote",
        "model.pkg.orders",
        Some("model"),
    )
    .await;

    let source_event = client
        .environment_source_state_event_create(
            &project_id,
            "remote",
            SourceStateEventCreateApiRequest {
                source_key: "source.pkg.raw_orders".to_string(),
                provider: "manual".to_string(),
                state_version: Some("v1".to_string()),
                observed_at: Some(chrono::Utc::now()),
                payload: serde_json::json!({ "reason": "new data" }),
            },
        )
        .await
        .expect("create source state event")
        .event;
    let plan = client
        .environment_reconcile(
            &project_id,
            "remote",
            EnvironmentReconcileApiRequest::default(),
        )
        .await
        .expect("create source-state plan")
        .plan;
    let blocked = client
        .environment_plan_admit(plan.plan_id)
        .await
        .expect("initial admit attempt")
        .plan;
    assert_eq!(blocked.status, PlanStatus::Blocked);

    insert_source_state_satisfaction(
        db.pool(),
        &project_id,
        "remote",
        "source.pkg.raw_orders",
        source_event.id,
        source_event.state_version.as_deref(),
        source_event.observed_at,
    )
    .await;

    client.sweep_tick().await.expect("sweep tick");

    let completed = client
        .environment_plan_get(plan.plan_id)
        .await
        .expect("reload plan after sweep")
        .plan;
    assert_eq!(completed.status, PlanStatus::Completed);
    assert!(completed.admitted_invocation_id.is_none());
    assert_eq!(completed.selected_resources, Vec::<String>::new());
    assert_eq!(
        completed.error.as_deref(),
        Some("source-triggered plan already satisfied by a successful plan")
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn claimed_invocation_timeout_fails_without_reclaim() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();

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
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();

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
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();

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
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();

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
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();

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
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();

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
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();

    let project_id = read_project_id_from_dbt_project(repo.project_dir(), true);
    bootstrap_remote_project_only(db.pool(), repo.project_dir(), &project_id).await;

    let created = client
        .environment_draft_create(&project_id)
        .await
        .expect("create environment draft");
    assert_eq!(created.draft.status, DraftStatus::LoadingGit);

    let head_sha = git_rev_parse(repo.project_dir(), "HEAD");
    let request = environment_draft_update_request("api-env", "main", Some(&head_sha), false);

    let refreshed = client
        .environment_draft_refresh_branch(created.draft.id, request.clone())
        .await
        .expect("refresh branch");
    assert_eq!(refreshed.draft.status, DraftStatus::LoadingGit);
    assert_eq!(refreshed.draft.slug, "api-env");
    assert_eq!(refreshed.draft.git_branch.as_deref(), Some("main"));

    let validating = client
        .environment_draft_validate(created.draft.id, request)
        .await
        .expect("validate draft");
    assert_eq!(validating.draft.status, DraftStatus::Validating);
    assert_eq!(
        validating.draft.git_commit_sha.as_deref(),
        Some(head_sha.as_str())
    );

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
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();

    fs::write(repo.project_dir().join("README.md"), "second commit\n")
        .expect("write second commit file");
    git(
        &["add", "."],
        repo.project_dir().parent().expect("repo root"),
    );
    git(
        &["commit", "-m", "second"],
        repo.project_dir().parent().expect("repo root"),
    );

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
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();

    let repo_root = repo.project_dir().parent().expect("repo root");
    let project_root = dbtx::services::relative_project_root(repo_root, repo.project_dir());

    let created = client
        .project_draft_create(ProjectDraftCreateApiRequest {
            git_repo_url: "https://example.com/repo.git".to_string(),
            project_root: project_root.clone(),
        })
        .await
        .expect("create project draft");
    assert_eq!(created.draft.status, DraftStatus::Draft);
    assert_eq!(created.draft.project_root, project_root);

    let validating = client
        .project_draft_validate(created.draft.id)
        .await
        .expect("start project draft validation");
    assert_eq!(validating.draft.status, DraftStatus::Validating);

    let project_name = read_dbt_project_name(repo.project_dir());
    mark_project_draft_validated(db.pool(), created.draft.id, &project_name, "main").await;

    let reloaded = client
        .project_draft_get(created.draft.id)
        .await
        .expect("reload validated draft");
    assert_eq!(reloaded.draft.status, DraftStatus::Validated);
    assert_eq!(
        reloaded.draft.project_name.as_deref(),
        Some(project_name.as_str())
    );
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
    assert_eq!(
        confirmed.project.project_root.as_deref(),
        Some(project_root.as_str())
    );
    assert_eq!(confirmed.project.project_name, project_name);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn validation_queue_routes_onboarding_but_not_normal_remote_invocations() {
    let db = TestDatabase::new_with_validation_queue("validation-only").await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();

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
        queues
            .get(&project_validation.invocation_id)
            .map(String::as_str),
        Some("validation-only")
    );
    assert_eq!(
        queues.get(&env_prepare.invocation_id).map(String::as_str),
        Some("validation-only")
    );
    assert_eq!(
        queues
            .get(&env_validation.invocation_id)
            .map(String::as_str),
        Some("validation-only")
    );
    assert_eq!(
        queues
            .get(&normal_invocation.invocation_id)
            .map(String::as_str),
        Some("generic")
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn invocation_list_filters_apply_to_operator_views() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();

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
    client: InProcessClient,
    pool: PgPool,
    db_name: String,
    admin_pool: PgPool,
}

struct SharedTestInfra {
    admin_url: String,
    _container: Option<ContainerAsync<Postgres>>,
}

static SHARED_INFRA: tokio::sync::OnceCell<SharedTestInfra> = tokio::sync::OnceCell::const_new();

async fn get_shared_infra() -> &'static SharedTestInfra {
    SHARED_INFRA
        .get_or_init(|| async {
            if let Ok(url) = std::env::var("DBTX_TEST_DATABASE_URL") {
                let admin_url = url
                    .rsplit_once('/')
                    .map(|(base, _)| format!("{base}/postgres"))
                    .unwrap_or_else(|| url.clone());
                let admin_pool = connect_test_pool(&admin_url, "connect admin").await;
                let _ = sqlx::query("CREATE DATABASE dbtx_template")
                    .execute(&admin_pool)
                    .await;
                admin_pool.close().await;
                let template_url = url
                    .rsplit_once('/')
                    .map(|(base, _)| format!("{base}/dbtx_template"))
                    .unwrap_or_else(|| url.clone());
                let db = connect_db_with_retry(&template_url, "connect template").await;
                db.migrate().await.expect("migrate template");
                return SharedTestInfra {
                    admin_url,
                    _container: None,
                };
            }

            let container = Postgres::default()
                .with_db_name("dbtx_template")
                .with_user("dbtx")
                .with_password("dbtx")
                .start()
                .await
                .expect("start shared postgres container");

            let host = container.get_host().await.expect("postgres host");
            let port = container
                .get_host_port_ipv4(5432)
                .await
                .expect("postgres port");
            let template_url = format!("postgres://dbtx:dbtx@{host}:{port}/dbtx_template");
            let admin_url = format!("postgres://dbtx:dbtx@{host}:{port}/postgres");

            let db = connect_db_with_retry(&template_url, "connect template").await;
            db.migrate().await.expect("migrate template");

            SharedTestInfra {
                admin_url,
                _container: Some(container),
            }
        })
        .await
}

impl TestDatabase {
    async fn new_without_reconciler() -> Self {
        Self::new_inner().await
    }

    async fn new_with_validation_queue(queue: &str) -> Self {
        if !queue.is_empty() {
            unsafe { std::env::set_var("DBTX_VALIDATION_QUEUE", queue) };
        }
        Self::new_inner().await
    }

    async fn new_inner() -> Self {
        unsafe { std::env::set_var("DBTX_SECRET_KEY", "test-secret-key") };
        let infra = get_shared_infra().await;
        let db_name = format!("test_{}", Uuid::new_v4().simple());

        let admin_pool = connect_test_pool(&infra.admin_url, "connect admin db").await;
        let _clone_permit = TEMPLATE_CLONE_LOCK
            .acquire()
            .await
            .expect("template clone lock");
        sqlx::query(&format!("CREATE DATABASE {db_name} TEMPLATE dbtx_template"))
            .execute(&admin_pool)
            .await
            .expect("create test database from template");

        let test_url = infra
            .admin_url
            .rsplit_once('/')
            .map(|(base, _)| format!("{base}/{db_name}"))
            .unwrap_or_else(|| format!("{}/{db_name}", infra.admin_url));

        let db = connect_db_with_retry(&test_url, "connect app db").await;
        let config = RuntimeConfig::from_database_url(test_url.clone());
        let state = AppState::new(db, config);
        let client = InProcessClient::new(router(state));
        let pool = connect_test_pool(&test_url, "connect test db").await;

        Self {
            client,
            pool,
            db_name,
            admin_pool,
        }
    }

    fn client(&self) -> &InProcessClient {
        &self.client
    }

    fn pool(&self) -> &PgPool {
        &self.pool
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        self.pool.close_event();
        let db_name = self.db_name.clone();
        let admin_pool = self.admin_pool.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("drop runtime");
            rt.block_on(async {
                let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS {db_name} WITH (FORCE)"))
                    .execute(&admin_pool)
                    .await;
            });
        });
    }
}

async fn wait_for_plan_status(
    client: &InProcessClient,
    plan_id: Uuid,
    expected_status: PlanStatus,
) -> dbtx::db::EnvironmentRunPlanRecord {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let plan = client
            .environment_plan_get(plan_id)
            .await
            .expect("reload plan while waiting");
        if plan.plan.status == expected_status {
            return plan.plan;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for plan {plan_id} to reach status {expected_status}, last status was {}",
            plan.plan.status
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_plan_reason(
    client: &InProcessClient,
    project_id: &str,
    slug: &str,
    expected_reason: &str,
) -> dbtx::db::EnvironmentRunPlanRecord {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let plans = client
            .environment_plan_list(project_id, slug)
            .await
            .expect("list plans while waiting")
            .plans;
        if let Some(plan) = plans
            .into_iter()
            .find(|plan| plan.reason == expected_reason)
            && matches!(
                plan.status,
                PlanStatus::Planned
                    | PlanStatus::Blocked
                    | PlanStatus::Admitted
                    | PlanStatus::Completed
            )
        {
            return plan;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for plan with reason {expected_reason} in {project_id}/{slug}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_manifest_prepare_invocation(
    pool: &PgPool,
    project_id: &str,
    slug: &str,
    commit_sha: &str,
) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let count: i64 = sqlx::query_scalar(
            r#"SELECT COUNT(*) FROM invocations i JOIN runs r ON r.run_id = i.run_id JOIN projects p ON p.id = i.project_id JOIN environments e ON e.id = i.environment_id WHERE p.project_id = $1 AND e.slug = $2 AND i.command = 'manifest_prepare' AND i.status = 'running' AND i.completed_at IS NULL AND r.git_commit_sha = $3"#,
        ).bind(project_id).bind(slug).bind(commit_sha).fetch_one(pool).await.expect("count manifest prepare invocations");
        if count > 0 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for manifest prepare invocation for {commit_sha}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn latest_run_id_for_commit(
    pool: &PgPool,
    project_id: &str,
    slug: &str,
    commit_sha: &str,
) -> Uuid {
    sqlx::query_scalar(
        r#"SELECT r.run_id FROM runs r JOIN projects p ON p.id = r.project_id JOIN environments e ON e.id = r.environment_id WHERE p.project_id = $1 AND e.slug = $2 AND r.git_commit_sha = $3 ORDER BY r.id DESC LIMIT 1"#,
    ).bind(project_id).bind(slug).bind(commit_sha).fetch_one(pool).await.expect("load latest run id for commit")
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
    .bind(
        serde_json::to_vec(&manifest)
            .expect("serialize manifest")
            .len() as i64,
    )
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

async fn insert_source_state_satisfaction(
    pool: &PgPool,
    project_id: &str,
    slug: &str,
    source_key: &str,
    latest_satisfied_event_id: i64,
    latest_satisfied_state_version: Option<&str>,
    latest_satisfied_observed_at: chrono::DateTime<chrono::Utc>,
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
    .expect("load source satisfaction scope");
    let project_pk: i64 = scope.get("project_pk");
    let environment_pk: i64 = scope.get("environment_pk");

    sqlx::query(
        r#"
        INSERT INTO environment_source_state_status (
            project_id,
            environment_id,
            source_key,
            latest_satisfied_event_id,
            latest_satisfied_state_version,
            latest_satisfied_observed_at,
            last_satisfied_run_id,
            last_satisfied_plan_id,
            updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, NULL, NULL, NOW())
        ON CONFLICT (project_id, environment_id, source_key) DO UPDATE SET
            latest_satisfied_event_id = EXCLUDED.latest_satisfied_event_id,
            latest_satisfied_state_version = EXCLUDED.latest_satisfied_state_version,
            latest_satisfied_observed_at = EXCLUDED.latest_satisfied_observed_at,
            updated_at = NOW()
        "#,
    )
    .bind(project_pk)
    .bind(environment_pk)
    .bind(source_key)
    .bind(latest_satisfied_event_id)
    .bind(latest_satisfied_state_version)
    .bind(latest_satisfied_observed_at)
    .execute(pool)
    .await
    .expect("insert source state satisfaction");
}

async fn insert_active_selected_resource(
    pool: &PgPool,
    invocation_id: Uuid,
    project_id: &str,
    slug: &str,
    unique_id: &str,
    resource_type: Option<&str>,
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
    .expect("load selected resource scope");
    let project_pk: i64 = scope.get("project_pk");
    let environment_pk: i64 = scope.get("environment_pk");

    sqlx::query(
        r#"
        INSERT INTO invocation_selected_resources (
            invocation_id, run_id, project_id, environment_id, unique_id, resource_type,
            selected_at, created_at, updated_at
        )
        SELECT invocation_id, run_id, project_id, environment_id, $2, $3, NOW(), NOW(), NOW()
        FROM invocations
        WHERE invocation_id = $1
          AND project_id = $4
          AND environment_id = $5
        "#,
    )
    .bind(invocation_id)
    .bind(unique_id)
    .bind(resource_type)
    .bind(project_pk)
    .bind(environment_pk)
    .execute(pool)
    .await
    .expect("insert active selected resource");
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
    let content =
        fs::read_to_string(project_dir.join("dbt_project.yml")).expect("read dbt_project");
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

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn reconcile_already_reconciled_environment_returns_conflict() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
    let project_id = read_project_id_from_dbt_project(repo.project_dir(), true);
    let commit_sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    bootstrap_remote_project_and_env_direct(
        db.pool(),
        repo.project_dir(),
        &project_id,
        "prod",
        Some(commit_sha),
    )
    .await;
    seed_environment_actual_state_with_manifest(
        db.pool(),
        &project_id,
        "prod",
        commit_sha,
        &[("model.pkg.orders", "model")],
        &[],
    )
    .await;

    // Environment is already reconciled (desired == actual commit)
    let err = client
        .environment_reconcile(
            &project_id,
            "prod",
            dbtx::api::EnvironmentReconcileApiRequest {},
        )
        .await
        .expect_err("should fail with conflict");
    assert!(
        err.to_string().contains("already reconciled"),
        "expected 'already reconciled' error, got: {err}"
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn admit_completed_plan_returns_conflict() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
    let project_id = read_project_id_from_dbt_project(repo.project_dir(), true);
    let baseline_sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let desired_sha = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    bootstrap_remote_project_and_env_direct(
        db.pool(),
        repo.project_dir(),
        &project_id,
        "prod",
        Some(desired_sha),
    )
    .await;
    seed_environment_actual_state_with_manifest(
        db.pool(),
        &project_id,
        "prod",
        baseline_sha,
        &[("model.pkg.orders", "model")],
        &[],
    )
    .await;
    seed_manifest_run_only(
        db.pool(),
        &project_id,
        "prod",
        desired_sha,
        &[("model.pkg.orders", "model", Some("new-checksum"))],
        &[],
    )
    .await;

    let plan = client
        .environment_reconcile(
            &project_id,
            "prod",
            dbtx::api::EnvironmentReconcileApiRequest {},
        )
        .await
        .expect("reconcile should create plan")
        .plan;
    assert_eq!(plan.status, PlanStatus::Planned);

    // Admit the plan
    let admitted = client
        .environment_plan_admit(plan.plan_id)
        .await
        .expect("admit should succeed");
    assert_eq!(admitted.plan.status, PlanStatus::Admitted);

    // Complete the plan's invocation
    let invocation_id = admitted
        .plan
        .admitted_invocation_id
        .expect("has invocation");
    let claim = client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Server),
            worker_id: "test-worker".to_string(),
            worker_queues: vec!["generic".to_string()],
        })
        .await
        .expect("claim")
        .expect("has claim");
    client
        .invocation_complete(
            invocation_id,
            dbtx::api::InvocationCompleteApiRequest {
                worker_id: "test-worker".to_string(),
                lease_token: claim.lease_token,
                completion: sample_execution_completion(InvocationLifecycleStatus::Succeeded, 0),
            },
        )
        .await
        .expect("complete");

    // Now try to admit the same plan again — should fail
    let err = client
        .environment_plan_admit(plan.plan_id)
        .await
        .expect_err("should fail with conflict");
    let msg = err.to_string();
    assert!(
        msg.contains("not admissible")
            || msg.contains("already in progress")
            || msg.contains("already reconciled"),
        "expected conflict error, got: {msg}"
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn reconcile_without_baseline_returns_unprocessable() {
    let db = TestDatabase::new_without_reconciler().await;
    let repo = TempProjectRepo::new("proj");
    let client = db.client().clone();
    let project_id = read_project_id_from_dbt_project(repo.project_dir(), true);
    let desired_sha = "cccccccccccccccccccccccccccccccccccccccc";

    bootstrap_remote_project_and_env_direct(
        db.pool(),
        repo.project_dir(),
        &project_id,
        "prod",
        Some(desired_sha),
    )
    .await;
    // No actual state seeded — no baseline run exists

    let err = client
        .environment_reconcile(
            &project_id,
            "prod",
            dbtx::api::EnvironmentReconcileApiRequest {},
        )
        .await
        .expect_err("should fail without baseline");
    let msg = err.to_string();
    assert!(
        msg.contains("baseline") || msg.contains("reconcil"),
        "expected baseline/reconciliation error, got: {msg}"
    );
}
