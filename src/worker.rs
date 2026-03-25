use crate::api::{
    InvocationClaimResponse, InvocationCommandApi, InvocationCompleteApiRequest,
    InvocationEventBatchApiRequest, InvocationHeartbeatApiRequest, InvocationLifecycleStatus,
};
use crate::client::DaemonClient;
use crate::error::{AppError, AppResult};
use crate::event::LogEvent;
use std::ffi::OsString;
use std::path::PathBuf;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command as TokioCommand;
use tracing::{debug, warn};

pub async fn execute_claimed_invocation(
    client: &DaemonClient,
    claim: InvocationClaimResponse,
    expected_invocation_id: Option<uuid::Uuid>,
) -> AppResult<()> {
    if let Some(expected) = expected_invocation_id
        && claim.invocation_id != expected
    {
        return Err(AppError::Io(std::io::Error::other(format!(
            "claimed unexpected invocation {}, expected {}",
            claim.invocation_id, expected
        ))));
    }

    let spec = claim.execution_spec;
    debug!(
        invocation_id = %claim.invocation_id,
        worker_id = %claim.worker_id,
        command = ?spec.command,
        project_dir = %spec.project_dir,
        "starting claimed invocation execution"
    );
    let project_dir = PathBuf::from(&spec.project_dir);
    let profiles_dir = write_profiles_dir(&spec.profiles_yml)?;
    let state_dir = write_state_dir(spec.state_manifest.as_ref())?;

    let mut dbt_args: Vec<OsString> = spec.args.iter().cloned().map(Into::into).collect();
    if let Some(state_dir) = state_dir.as_ref() {
        dbt_args.push("--state".into());
        dbt_args.push(state_dir.path().as_os_str().to_os_string());
    }
    dbt_args.push("--profiles-dir".into());
    dbt_args.push(profiles_dir.path().as_os_str().to_os_string());

    let command = map_command(spec.command);
    let mut child =
        TokioCommand::new(std::env::var("DBTX_DBT_PATH").unwrap_or_else(|_| "dbt".to_string()))
            .arg(command)
            .args(&dbt_args)
            .current_dir(&project_dir)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| AppError::Io(std::io::Error::other("missing child stdout")))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| AppError::Io(std::io::Error::other("missing child stderr")))?;

    let mut stdout_reader = BufReader::new(stdout).lines();
    let stderr_handle = tokio::spawn(async move {
        let mut reader = BufReader::new(stderr).lines();
        let mut lines = Vec::new();
        while let Some(line) = reader.next_line().await? {
            lines.push(line);
        }
        Result::<Vec<String>, std::io::Error>::Ok(lines)
    });

    let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(2));
    let mut dbt_version: Option<String> = None;
    let mut cancel_requested = false;

    loop {
        tokio::select! {
            line = stdout_reader.next_line() => {
                let Some(line) = line? else { break; };
                if persists_state(spec.command)
                    && let Some(event) = LogEvent::parse(&line)
                {
                    if dbt_version.is_none() && event.info.name == "MainReportVersion" {
                        dbt_version = event
                            .data
                            .get("version")
                            .and_then(serde_json::Value::as_str)
                            .map(ToString::to_string);
                    }
                    if let Some(rendered) = event.render_text_line() {
                        println!("{rendered}");
                    }
                    client
                        .invocation_append_events(
                            claim.invocation_id,
                            InvocationEventBatchApiRequest {
                                worker_id: claim.worker_id.clone(),
                                events: vec![crate::execution::ExecutionEvent {
                                    kind: crate::execution::ExecutionEventKind::DbtLog,
                                    occurred_at: chrono::Utc::now(),
                                    text: event.render_text_line(),
                                    raw_line: Some(line),
                                    dbt_event_name: Some(event.info.name.clone()),
                                    node_unique_id: event
                                        .data
                                        .get("node_info")
                                        .and_then(|value| value.get("unique_id"))
                                        .and_then(|value| value.as_str())
                                        .map(ToString::to_string),
                                    level: Some(event.info.level.clone()),
                                    error: None,
                                }],
                            },
                        )
                        .await?;
                } else {
                    println!("{line}");
                    client
                        .invocation_append_events(
                            claim.invocation_id,
                            InvocationEventBatchApiRequest {
                                worker_id: claim.worker_id.clone(),
                                events: vec![crate::execution::ExecutionEvent {
                                    kind: crate::execution::ExecutionEventKind::StdoutLine,
                                    occurred_at: chrono::Utc::now(),
                                    text: Some(line.clone()),
                                    raw_line: Some(line),
                                    dbt_event_name: None,
                                    node_unique_id: None,
                                    level: None,
                                    error: None,
                                }],
                            },
                        )
                        .await?;
                }
            }
            _ = heartbeat.tick() => {
                let hb = client
                    .invocation_heartbeat(
                        claim.invocation_id,
                        InvocationHeartbeatApiRequest {
                            worker_id: claim.worker_id.clone(),
                        },
                    )
                    .await?;
                if hb.cancel_requested {
                    warn!(
                        invocation_id = %claim.invocation_id,
                        worker_id = %claim.worker_id,
                        "cancel requested by control plane"
                    );
                    cancel_requested = true;
                    let _ = child.start_kill();
                }
            }
        }
    }

    let status = child.wait().await?;
    for line in stderr_handle.await.map_err(|err| {
        AppError::Io(std::io::Error::other(format!("stderr task failed: {err}")))
    })?? {
        eprintln!("{line}");
        client
            .invocation_append_events(
                claim.invocation_id,
                InvocationEventBatchApiRequest {
                    worker_id: claim.worker_id.clone(),
                    events: vec![crate::execution::ExecutionEvent {
                        kind: crate::execution::ExecutionEventKind::StderrLine,
                        occurred_at: chrono::Utc::now(),
                        text: Some(line.clone()),
                        raw_line: Some(line),
                        dbt_event_name: None,
                        node_unique_id: None,
                        level: None,
                        error: None,
                    }],
                },
            )
            .await?;
    }

    let exit_code = if cancel_requested {
        130
    } else {
        status.code().unwrap_or(1)
    };
    let manifest = if persists_state(spec.command) {
        let manifest_path = project_dir.join("target").join("manifest.json");
        std::fs::read_to_string(&manifest_path)
            .ok()
            .and_then(|content| serde_json::from_str(&content).ok())
    } else {
        None
    };

    client
        .invocation_complete(
            claim.invocation_id,
            InvocationCompleteApiRequest {
                worker_id: claim.worker_id.clone(),
                completion: crate::execution::ExecutionCompletion {
                    status: if cancel_requested {
                        InvocationLifecycleStatus::Canceled
                    } else if !status.success() {
                        InvocationLifecycleStatus::Failed
                    } else {
                        InvocationLifecycleStatus::Succeeded
                    },
                    exit_code,
                    error: if cancel_requested {
                        Some("invocation canceled".to_string())
                    } else if status.success() {
                        None
                    } else {
                        Some(format!("dbt invocation failed with exit code {exit_code}"))
                    },
                    dbt_version,
                    manifest,
                },
            },
        )
        .await?;

    debug!(
        invocation_id = %claim.invocation_id,
        worker_id = %claim.worker_id,
        exit_code,
        canceled = cancel_requested,
        "finished claimed invocation execution"
    );

    if cancel_requested {
        Err(AppError::Io(std::io::Error::other("invocation canceled")))
    } else if status.success() {
        Ok(())
    } else {
        Err(AppError::DbtFailed(exit_code))
    }
}

fn persists_state(command: InvocationCommandApi) -> bool {
    !matches!(command, InvocationCommandApi::Ls)
}

fn map_command(command: InvocationCommandApi) -> &'static str {
    match command {
        InvocationCommandApi::Build => "build",
        InvocationCommandApi::Run => "run",
        InvocationCommandApi::Ls => "ls",
        InvocationCommandApi::Test => "test",
        InvocationCommandApi::Seed => "seed",
    }
}

fn write_profiles_dir(profiles_yml: &str) -> AppResult<TempDir> {
    let temp_dir = TempDir::new()?;
    std::fs::write(temp_dir.path().join("profiles.yml"), profiles_yml)?;
    Ok(temp_dir)
}

fn write_state_dir(state_manifest: Option<&serde_json::Value>) -> AppResult<Option<TempDir>> {
    let Some(state_manifest) = state_manifest else {
        return Ok(None);
    };
    let temp_dir = TempDir::new()?;
    std::fs::write(
        temp_dir.path().join("manifest.json"),
        serde_json::to_vec(state_manifest)?,
    )?;
    Ok(Some(temp_dir))
}
