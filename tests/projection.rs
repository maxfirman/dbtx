use dbtx::api::{
    InvocationCancelStateApi, InvocationClaimNextApiRequest, InvocationCommandApi,
    InvocationCompleteApiRequest, InvocationCreateApiRequest, InvocationEventBatchApiRequest,
    InvocationExecutionModeApi, InvocationHeartbeatApiRequest, InvocationLifecycleStatus,
    InvocationListApiRequest,
};
use dbtx::client::DaemonClient;
use dbtx::execution::{ExecutionCompletion, ExecutionEvent, ExecutionEventKind};
use dbtx::services::{infer_local_project_defaults, infer_remote_project_defaults};
use serde_json::json;
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
async fn commands_require_explicit_migration() {
    let db = TestDatabase::new_unmigrated().await;
    let repo = TempProjectRepo::new("proj");

    let output = run_dbtx_in_dir(db.service_url(), repo.project_dir(), &["project", "init"]);
    assert_failure(&output);
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("dbtx state migrate"),
        "unexpected stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    init_dbtx_schema(db.service_url());

    let output = run_dbtx_in_dir(db.service_url(), repo.project_dir(), &["project", "init"]);
    assert_success(&output);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn project_and_environment_cli_round_trip() {
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;
    let repo = TempProjectRepo::new("proj");

    let output = run_dbtx_in_dir(db.service_url(), repo.project_dir(), &["project", "init"]);
    assert_success(&output);
    let project_id = read_project_id_from_dbt_project(repo.project_dir(), false);

    let output = run_dbtx_in_dir(
        db.service_url(),
        repo.project_dir(),
        &["environment", "create", "--slug", "staging", "--target", "dev"],
    );
    assert_success(&output);

    let list_output = run_dbtx_in_dir(
        db.service_url(),
        repo.project_dir(),
        &["environment", "list", "--project", &project_id],
    );
    assert_success(&list_output);
    let stdout = String::from_utf8_lossy(&list_output.stdout);
    assert!(
        stdout.contains("slug=staging"),
        "unexpected stdout: {stdout}"
    );
    assert!(
        stdout.contains("target_name=dev"),
        "unexpected stdout: {stdout}"
    );
    assert!(
        stdout.contains(&format!("project_id={project_id}")),
        "expected project id in stdout: {stdout}"
    );

    let project_row = sqlx::query(
        "SELECT project_id, project_name, git_repo_url, default_branch, project_root FROM projects WHERE project_id = $1",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .expect("project row");
    assert_eq!(project_row.get::<String, _>("project_id"), project_id);
    assert_eq!(project_row.get::<String, _>("project_name"), "proj");
    assert_eq!(
        project_row
            .get::<Option<String>, _>("git_repo_url")
            .as_deref(),
        Some("https://example.com/repo.git")
    );

    let environment_row =
        sqlx::query("SELECT slug, target_name, status FROM environments WHERE slug = 'staging'")
    .fetch_one(db.pool())
    .await
    .expect("environment row");
    assert_eq!(environment_row.get::<String, _>("slug"), "staging");
    assert_eq!(environment_row.get::<String, _>("target_name"), "dev");
    assert_eq!(environment_row.get::<String, _>("status"), "active");

    let duplicate_output = run_dbtx_in_dir(
        db.service_url(),
        repo.project_dir(),
        &[
            "environment",
            "create",
            "--project",
            &project_id,
            "--slug",
            "staging",
        ],
    );
    assert_failure(&duplicate_output);
    assert!(
        String::from_utf8_lossy(&duplicate_output.stderr).contains("already exists"),
        "unexpected stderr: {}",
        String::from_utf8_lossy(&duplicate_output.stderr)
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn environment_seed_from_copies_active_state_without_runs() {
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;
    let repo = TempProjectRepo::new("proj");

    assert_success(&run_dbtx_in_dir(
        db.service_url(),
        repo.project_dir(),
        &["project", "init"],
    ));
    let project_id = read_project_id_from_dbt_project(repo.project_dir(), false);
    assert_success(&run_dbtx_in_dir(
        db.service_url(),
        repo.project_dir(),
        &[
            "environment",
            "create",
            "--project",
            &project_id,
            "--slug",
            "source",
        ],
    ));
    let ids = project_environment_ids(db.pool(), &project_id, "source").await;
    let run_id = Uuid::new_v4();
    insert_run(
        db.pool(),
        RunInsert {
            id: 1,
            run_id,
            project_id: ids.project_id,
            environment_id: ids.environment_id,
            command: "run",
            is_full_graph_run: true,
            terminal_status: "success",
        },
    )
    .await;
    insert_manifest(
        db.pool(),
        run_id,
        &manifest_with_nodes([node("model.pkg.a", "seeded")]),
    )
    .await;
    insert_node_execution(
        db.pool(),
        run_id,
        "model.pkg.a",
        "model",
        "success",
        Some("seeded"),
    )
    .await;

    sqlx::query(
        "INSERT INTO promoted_manifest_meta (project_id, environment_id, source_run_id, base_manifest) SELECT $1, $2, $3, manifest FROM manifest_snapshots WHERE run_id = $3",
    )
    .bind(ids.project_id)
    .bind(ids.environment_id)
    .bind(run_id)
    .execute(db.pool())
    .await
    .expect("insert promoted meta");
    sqlx::query(
        "INSERT INTO promoted_manifest_nodes (project_id, environment_id, unique_id, source_run_id, checksum, raw_node) SELECT $1, $2, 'model.pkg.a', $3, 'seeded', manifest -> 'nodes' -> 'model.pkg.a' FROM manifest_snapshots WHERE run_id = $3",
    )
    .bind(ids.project_id)
    .bind(ids.environment_id)
    .bind(run_id)
    .execute(db.pool())
    .await
    .expect("insert promoted node");
    sqlx::query(
        r#"
        INSERT INTO current_node_state (
            project_id, environment_id, unique_id, last_run_id, status, resource_type, node_name,
            node_path, materialized, relation_database, relation_schema, relation_alias,
            relation_name, checksum, started_at, finished_at, execution_time_seconds,
            last_success_at, updated_at
        )
        VALUES ($1, $2, 'model.pkg.a', $3, 'success', 'model', 'a', 'models/a.sql', 'table',
                'warehouse', 'main', 'a', 'warehouse.main.a', 'seeded', NOW(), NOW(), 0.1, NOW(), NOW())
        "#,
    )
    .bind(ids.project_id)
    .bind(ids.environment_id)
    .bind(run_id)
    .execute(db.pool())
    .await
    .expect("insert current state");

    assert_success(&run_dbtx_in_dir(
        db.service_url(),
        repo.project_dir(),
        &[
            "environment",
            "create",
            "--project",
            &project_id,
            "--slug",
            "target",
            "--baseline",
            "source",
            "--git-branch",
            "main",
            "--pr-number",
            "123",
        ],
    ));

    let target_ids = project_environment_ids(db.pool(), &project_id, "target").await;
    let promoted_node_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM promoted_manifest_nodes WHERE project_id = $1 AND environment_id = $2",
    )
    .bind(target_ids.project_id)
    .bind(target_ids.environment_id)
    .fetch_one(db.pool())
    .await
    .expect("promoted node count");
    let current_state_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM current_node_state WHERE project_id = $1 AND environment_id = $2",
    )
    .bind(target_ids.project_id)
    .bind(target_ids.environment_id)
    .fetch_one(db.pool())
    .await
    .expect("current state count");
    let seed_record_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM environment_seeds WHERE target_environment_id = $1",
    )
    .bind(target_ids.environment_id)
    .fetch_one(db.pool())
    .await
    .expect("seed records");
    let runs_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM runs")
        .fetch_one(db.pool())
        .await
        .expect("runs count");

    assert_eq!(promoted_node_count, 1);
    assert_eq!(current_state_count, 1);
    assert_eq!(seed_record_count, 1);
    assert_eq!(runs_count, 1);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn remote_project_environment_requires_commit_sha_and_records_version_history() {
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;
    let repo = TempProjectRepo::new("proj");

    assert_success(&run_dbtx_in_dir(
        db.service_url(),
        repo.project_dir(),
        &["project", "init", "--mode", "remote"],
    ));
    let project_id = read_project_id_from_dbt_project(repo.project_dir(), true);

    let missing_sha = run_dbtx_in_dir(
        db.service_url(),
        repo.project_dir(),
        &[
            "environment",
            "create",
            "--project",
            &project_id,
            "--slug",
            "ci-main",
        ],
    );
    assert_failure(&missing_sha);
    assert!(
        String::from_utf8_lossy(&missing_sha.stderr).contains("requires --git-commit-sha"),
        "unexpected stderr: {}",
        String::from_utf8_lossy(&missing_sha.stderr)
    );

    assert_success(&run_dbtx_in_dir(
        db.service_url(),
        repo.project_dir(),
        &[
            "environment",
            "create",
            "--project",
            &project_id,
            "--slug",
            "ci-main",
            "--git-branch",
            "main",
            "--git-commit-sha",
            "abc123",
        ],
    ));

    let versions: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM environment_versions ev JOIN environments e ON e.id = ev.environment_id WHERE e.slug = 'ci-main'",
    )
    .fetch_one(db.pool())
    .await
    .expect("environment versions");
    assert_eq!(versions, 1);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn remote_project_environment_allows_commit_updates() {
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;
    let repo = TempProjectRepo::new("proj");

    assert_success(&run_dbtx_in_dir(
        db.service_url(),
        repo.project_dir(),
        &["project", "init", "--mode", "remote"],
    ));
    let project_id = read_project_id_from_dbt_project(repo.project_dir(), true);

    assert_success(&run_dbtx_in_dir(
        db.service_url(),
        repo.project_dir(),
        &[
            "environment",
            "create",
            "--project",
            &project_id,
            "--slug",
            "ci-main",
            "--git-branch",
            "main",
            "--git-commit-sha",
            "abc123",
        ],
    ));

    let update = run_dbtx_in_dir(
        db.service_url(),
        repo.project_dir(),
        &[
            "environment",
            "update",
            "--project",
            &project_id,
            "--slug",
            "ci-main",
            "--git-commit-sha",
            "def456",
        ],
    );
    assert_success(&update);

    let commit_sha: Option<String> = sqlx::query_scalar(
        "SELECT git_commit_sha FROM environments WHERE slug = 'ci-main'",
    )
    .fetch_one(db.pool())
    .await
    .expect("environment commit sha");
    assert_eq!(commit_sha.as_deref(), Some("def456"));
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn project_update_preserves_existing_remote_mode_without_flag() {
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;
    let repo = TempProjectRepo::new("proj");

    assert_success(&run_dbtx_in_dir(
        db.service_url(),
        repo.project_dir(),
        &["project", "init", "--mode", "remote"],
    ));

    assert_success(&run_dbtx_in_dir(
        db.service_url(),
        repo.project_dir(),
        &["project", "update"],
    ));

    let project_id = read_project_id_from_dbt_project(repo.project_dir(), true);
    let mode: String = sqlx::query_scalar("SELECT mode FROM projects WHERE project_id = $1")
        .bind(&project_id)
        .fetch_one(db.pool())
        .await
        .expect("project mode");
    assert_eq!(mode, "remote");
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn remote_invocation_requires_remote_project_mode() {
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;
    let repo = TempProjectRepo::new("proj");
    let client = DaemonClient::new(db.service_url().to_string());

    assert_success(&run_dbtx_in_dir(
        db.service_url(),
        repo.project_dir(),
        &["project", "init"],
    ));
    let project_id = read_project_id_from_dbt_project(repo.project_dir(), false);
    assert_success(&run_dbtx_in_dir(
        db.service_url(),
        repo.project_dir(),
        &["environment", "create", "--slug", "remote", "--target", "dev"],
    ));

    assert!(
        client
            .invocation_create(remote_invocation_request(
                &project_id,
                "remote",
                InvocationCommandApi::Ls,
                None,
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

    assert_success(&run_dbtx_in_dir(
        db.service_url(),
        repo.project_dir(),
        &["project", "init"],
    ));
    let project_id = read_project_id_from_dbt_project(repo.project_dir(), false);
    assert_success(&run_dbtx_in_dir(
        db.service_url(),
        repo.project_dir(),
        &[
            "environment",
            "create",
            "--slug",
            "mutable",
            "--target",
            "dev",
        ],
    ));

    assert!(
        client
            .invocation_create(remote_invocation_request(
                &project_id,
                "mutable",
                InvocationCommandApi::Ls,
                None,
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

    bootstrap_project_and_env(&repo, db.service_url(), "dev").await;
    let created = client
        .invocation_create(local_invocation_request(
            repo.project_dir(),
            InvocationCommandApi::Ls,
            Some("dev"),
            Some("lease-test"),
        ))
        .await
        .expect("create invocation");
    let claim = client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Local),
            worker_id: "worker-a".to_string(),
            worker_queue: Some("lease-test".to_string()),
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
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn claimed_invocation_timeout_fails_without_reclaim() {
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;
    let repo = TempProjectRepo::new("proj");
    let client = DaemonClient::new(db.service_url().to_string());

    bootstrap_project_and_env(&repo, db.service_url(), "dev").await;
    let project_id = read_project_id_from_dbt_project(repo.project_dir(), true);

    let created = client
        .invocation_create(remote_invocation_request(
            &project_id,
            "dev",
            InvocationCommandApi::Ls,
            None,
        ))
        .await
        .expect("create invocation");
    let claim = client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Server),
            worker_id: "worker-a".to_string(),
            worker_queue: None,
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

    let reclaimed = client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Server),
            worker_id: "worker-b".to_string(),
            worker_queue: None,
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

    bootstrap_project_and_env(&repo, db.service_url(), "dev").await;
    let project_id = read_project_id_from_dbt_project(repo.project_dir(), true);

    let local = client
        .invocation_create(local_invocation_request(
            repo.project_dir(),
            InvocationCommandApi::Ls,
            Some("dev"),
            Some("local-deadline-test"),
        ))
        .await
        .expect("create local invocation");
    let server = client
        .invocation_create(remote_invocation_request(
            &project_id,
            "dev",
            InvocationCommandApi::Ls,
            None,
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

    bootstrap_project_and_env(&repo, db.service_url(), "dev").await;
    let project_id = read_project_id_from_dbt_project(repo.project_dir(), true);

    let local = client
        .invocation_create(local_invocation_request(
            repo.project_dir(),
            InvocationCommandApi::Ls,
            Some("dev"),
            Some("local-heartbeat-test"),
        ))
        .await
        .expect("create local invocation");
    let server = client
        .invocation_create(remote_invocation_request(
            &project_id,
            "dev",
            InvocationCommandApi::Ls,
            None,
        ))
        .await
        .expect("create server invocation");

    client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Local),
            worker_id: "worker-local".to_string(),
            worker_queue: Some("local-heartbeat-test".to_string()),
        })
        .await
        .expect("claim local invocation")
        .expect("local claimed");
    client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Server),
            worker_id: "worker-server".to_string(),
            worker_queue: None,
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

    bootstrap_project_and_env(&repo, db.service_url(), "dev").await;

    let created = client
        .invocation_create(local_invocation_request(
            repo.project_dir(),
            InvocationCommandApi::Ls,
            Some("dev"),
            Some("cancel-immediate"),
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

    bootstrap_project_and_env(&repo, db.service_url(), "dev").await;
    let project_id = read_project_id_from_dbt_project(repo.project_dir(), true);

    let created = client
        .invocation_create(remote_invocation_request(
            &project_id,
            "dev",
            InvocationCommandApi::Ls,
            None,
        ))
        .await
        .expect("create invocation");
    let _claim = client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Server),
            worker_id: "worker-a".to_string(),
            worker_queue: None,
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

    bootstrap_project_and_env(&repo, db.service_url(), "dev").await;
    let project_id = read_project_id_from_dbt_project(repo.project_dir(), true);

    let _server_generic_1 = client
        .invocation_create(remote_invocation_request(
            &project_id,
            "dev",
            InvocationCommandApi::Ls,
            Some("generic"),
        ))
        .await
        .expect("create server invocation 1");
    let _server_generic_2 = client
        .invocation_create(remote_invocation_request(
            &project_id,
            "dev",
            InvocationCommandApi::Ls,
            Some("generic"),
        ))
        .await
        .expect("create server invocation 2");
    let local_isolated = client
        .invocation_create(local_invocation_request(
            repo.project_dir(),
            InvocationCommandApi::Ls,
            Some("dev"),
            Some("isolated"),
        ))
        .await
        .expect("create local invocation");

    let _claim_a = client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Server),
            worker_id: "worker-a".to_string(),
            worker_queue: Some("generic".to_string()),
        })
        .await
        .expect("claim generic server work")
        .expect("claimed server work");
    let _claim_b = client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Local),
            worker_id: "worker-b".to_string(),
            worker_queue: Some("isolated".to_string()),
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
    assert_eq!(worker_a.worker_queue, "generic");
    let worker_b = workers
        .iter()
        .find(|worker| worker.worker_id == "worker-b")
        .expect("worker-b");
    assert_eq!(worker_b.claimed_invocation_count, 1);
    assert_eq!(worker_b.worker_queue, "isolated");
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
        .find(|queue| queue.worker_queue == "isolated")
        .expect("isolated queue");
    assert_eq!(isolated.pending_count, 0);
    assert_eq!(isolated.claimed_count, 1);
    assert_eq!(isolated.stale_claim_count, 1);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn invocation_list_filters_apply_to_operator_views() {
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;
    let repo = TempProjectRepo::new("proj");
    let client = DaemonClient::new(db.service_url().to_string());

    bootstrap_project_and_env(&repo, db.service_url(), "dev").await;
    let project_id = read_project_id_from_dbt_project(repo.project_dir(), true);

    let running = client
        .invocation_create(remote_invocation_request(
            &project_id,
            "dev",
            InvocationCommandApi::Ls,
            Some("generic"),
        ))
        .await
        .expect("create running invocation");
    let canceled = client
        .invocation_create(local_invocation_request(
            repo.project_dir(),
            InvocationCommandApi::Ls,
            Some("dev"),
            Some("special"),
        ))
        .await
        .expect("create canceled invocation");

    client
        .invocation_claim_next(InvocationClaimNextApiRequest {
            execution_mode: Some(InvocationExecutionModeApi::Server),
            worker_id: "worker-filter".to_string(),
            worker_queue: Some("generic".to_string()),
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

struct ScopeIds {
    project_id: i64,
    environment_id: i64,
}

struct RunInsert<'a> {
    id: i64,
    run_id: Uuid,
    project_id: i64,
    environment_id: i64,
    command: &'a str,
    is_full_graph_run: bool,
    terminal_status: &'a str,
}

struct TestDatabase {
    daemon: TestDaemon,
    pool: PgPool,
    _container: Option<ContainerAsync<Postgres>>,
}

impl TestDatabase {
    async fn new() -> Self {
        if let Ok(url) = std::env::var("DBTX_TEST_DATABASE_URL") {
            let daemon = TestDaemon::start(&url);
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
        let daemon = TestDaemon::start(&url);
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

    async fn new_unmigrated() -> Self {
        if let Ok(url) = std::env::var("DBTX_TEST_DATABASE_URL") {
            let daemon = TestDaemon::start(&url);
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
        let daemon = TestDaemon::start(&url);
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
    fn start(database_url: &str) -> Self {
        let listen = next_listen_addr();
        let mut child = Command::new(env!("CARGO_BIN_EXE_dbtx-server"))
            .args(["--listen", &listen])
            .env("DBTX_DATABASE_URL", database_url)
            .env("DBTX_SECRET_KEY", "test-secret-key")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start dbtx-server");

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

async fn project_environment_ids(
    pool: &PgPool,
    project_ref: &str,
    environment_slug: &str,
) -> ScopeIds {
    let row = sqlx::query(
        r#"
        SELECT p.id AS project_id, e.id AS environment_id
        FROM projects p
        JOIN environments e ON e.project_id = p.id
        WHERE p.project_id = $1 AND e.slug = $2
        "#,
    )
    .bind(project_ref)
    .bind(environment_slug)
    .fetch_one(pool)
    .await
    .expect("project/environment row");
    ScopeIds {
        project_id: row.get("project_id"),
        environment_id: row.get("environment_id"),
    }
}

async fn insert_run(pool: &PgPool, run: RunInsert<'_>) {
    sqlx::query(
        r#"
        INSERT INTO runs (
            id, run_id, project_id, environment_id, command, args,
            is_full_graph_run, started_at, finished_at, exit_code, terminal_status
        )
        VALUES ($1, $2, $3, $4, $5, '[]'::jsonb, $6, NOW(), NOW(), 0, $7)
        "#,
    )
    .bind(run.id)
    .bind(run.run_id)
    .bind(run.project_id)
    .bind(run.environment_id)
    .bind(run.command)
    .bind(run.is_full_graph_run)
    .bind(run.terminal_status)
    .execute(pool)
    .await
    .expect("insert run");
}

async fn insert_manifest(pool: &PgPool, run_id: Uuid, manifest: &serde_json::Value) {
    sqlx::query(
        r#"
        INSERT INTO manifest_snapshots (run_id, manifest, manifest_size_bytes, checksum)
        VALUES ($1, $2, $3, $4)
        "#,
    )
    .bind(run_id)
    .bind(sqlx::types::Json(manifest))
    .bind(serde_json::to_vec(manifest).expect("manifest json").len() as i64)
    .bind(format!(
        "{:x}",
        md5::compute(serde_json::to_vec(manifest).expect("manifest bytes"))
    ))
    .execute(pool)
    .await
    .expect("insert manifest");
}

async fn insert_node_execution(
    pool: &PgPool,
    run_id: Uuid,
    unique_id: &str,
    resource_type: &str,
    status: &str,
    checksum: Option<&str>,
) {
    sqlx::query(
        r#"
        INSERT INTO node_executions (
            run_id, unique_id, resource_type, node_name, node_path, materialized, status,
            checksum, started_at, finished_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, 'table', $6, $7, NOW(), NOW(), NOW())
        "#,
    )
    .bind(run_id)
    .bind(unique_id)
    .bind(resource_type)
    .bind(unique_id)
    .bind(format!("{unique_id}.sql"))
    .bind(status)
    .bind(checksum)
    .execute(pool)
    .await
    .expect("insert node execution");
}

fn node(unique_id: &str, checksum: &str) -> serde_json::Value {
    json!({
        "unique_id": unique_id,
        "resource_type": "model",
        "name": unique_id,
        "database": "warehouse",
        "schema": "main",
        "alias": unique_id,
        "relation_name": unique_id,
        "config": {"materialized": "table"},
        "checksum": {"checksum": checksum},
        "depends_on": {"nodes": []}
    })
}

fn manifest_with_nodes<const N: usize>(nodes: [serde_json::Value; N]) -> serde_json::Value {
    let nodes = nodes
        .into_iter()
        .map(|node| {
            let unique_id = node["unique_id"]
                .as_str()
                .expect("node unique_id")
                .to_string();
            (unique_id, node)
        })
        .collect::<serde_json::Map<_, _>>();
    json!({
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
            "dbt_version": "2.0.0",
            "generated_at": "2026-01-01T00:00:00Z",
            "invocation_id": Uuid::nil(),
            "project_name": "proj",
            "project_id": "proj",
            "adapter_type": "duckdb",
            "env": {}
        },
        "nodes": nodes,
        "sources": {},
        "parent_map": {},
        "child_map": {},
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
    })
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

fn run_dbtx_in_dir(service_url: &str, cwd: &Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_dbtx"))
        .args(args)
        .env("DBTX_SERVICE_URL", service_url)
        .current_dir(cwd)
        .output()
        .expect("run dbtx in dir")
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

fn assert_success(output: &std::process::Output) {
    assert!(
        output.status.success(),
        "command failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

fn assert_failure(output: &std::process::Output) {
    assert!(
        !output.status.success(),
        "expected failure but command succeeded\nstdout:\n{}\nstderr:\n{}",
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

async fn bootstrap_project_and_env(repo: &TempProjectRepo, service_url: &str, slug: &str) {
    let commit_sha = git_rev_parse(repo.project_dir(), "HEAD");
    assert_success(&run_dbtx_in_dir(
        service_url,
        repo.project_dir(),
        &["project", "init", "--mode", "remote"],
    ));
    assert_success(&run_dbtx_in_dir(
        service_url,
        repo.project_dir(),
        &[
            "environment",
            "create",
            "--slug",
            slug,
            "--target",
            "dev",
            "--git-branch",
            "main",
            "--git-commit-sha",
            &commit_sha,
        ],
    ));
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
    worker_queue: Option<&str>,
) -> InvocationCreateApiRequest {
    InvocationCreateApiRequest {
        command,
        args: vec![],
        current_dir: Some(project_dir.display().to_string()),
        project_id: None,
        environment_slug: environment_slug.map(ToString::to_string),
        execution_mode: InvocationExecutionModeApi::Local,
        worker_queue: worker_queue.map(ToString::to_string),
    }
}

fn remote_invocation_request(
    project_id: &str,
    environment_slug: &str,
    command: InvocationCommandApi,
    worker_queue: Option<&str>,
) -> InvocationCreateApiRequest {
    InvocationCreateApiRequest {
        command,
        args: vec![],
        current_dir: None,
        project_id: Some(project_id.to_string()),
        environment_slug: Some(environment_slug.to_string()),
        execution_mode: InvocationExecutionModeApi::Server,
        worker_queue: worker_queue.map(ToString::to_string),
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
    }
}
