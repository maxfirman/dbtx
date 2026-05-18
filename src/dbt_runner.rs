//! Shared dbt child process spawning, output capture, and execution loop.
//!
//! Provides `DbtChild` for spawning and `run_dbt_execution` for the full
//! heartbeat/cancel/stream lifecycle. Callers provide a `DbtExecutionSession`
//! to handle control-plane communication.

use crate::error::{AppError, AppResult};
use crate::event::LogEvent;
use crate::execution::{ExecutionEvent, ExecutionEventKind};
use std::ffi::OsString;
use std::path::Path;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader, Lines};
use tokio::process::{Child, ChildStdout, Command};
use tracing::warn;

pub(crate) fn dbt_path_from_env() -> String {
    std::env::var("DBTX_DBT_PATH").unwrap_or_else(|_| "dbt".to_string())
}

/// A spawned dbt child process with captured stdout and a background stderr task.
pub(crate) struct DbtChild {
    child: Child,
    pub stdout_lines: Lines<BufReader<ChildStdout>>,
    stderr_handle: tokio::task::JoinHandle<Result<Vec<String>, std::io::Error>>,
}

/// Result of waiting for a dbt child process to complete.
#[must_use]
pub(crate) struct DbtChildResult {
    pub exit_code: i32,
    pub stderr_lines: Vec<String>,
}

impl DbtChild {
    /// Spawn a dbt child process with the given subcommand and arguments.
    pub fn spawn(
        dbt_path: &str,
        subcommand: &str,
        args: &[OsString],
        project_dir: &Path,
    ) -> AppResult<Self> {
        let mut child = Command::new(dbt_path)
            .arg(subcommand)
            .args(args)
            .current_dir(project_dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| AppError::Internal("missing child stdout".to_string()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| AppError::Internal("missing child stderr".to_string()))?;

        let stdout_lines = BufReader::new(stdout).lines();
        let stderr_handle = tokio::spawn(async move {
            let mut reader = BufReader::new(stderr).lines();
            let mut lines = Vec::new();
            while let Some(line) = reader.next_line().await? {
                lines.push(line);
            }
            Ok(lines)
        });

        Ok(Self {
            child,
            stdout_lines,
            stderr_handle,
        })
    }

    /// Wait for the child process to exit and collect stderr.
    pub async fn wait(mut self) -> AppResult<DbtChildResult> {
        let status = self.child.wait().await?;
        let stderr_lines = self
            .stderr_handle
            .await
            .map_err(|err| AppError::Internal(format!("stderr task failed: {err}")))??;
        Ok(DbtChildResult {
            exit_code: status.code().unwrap_or(1),
            stderr_lines,
        })
    }

    /// Send a kill signal to the child process.
    pub fn start_kill(&mut self) {
        let _ = self.child.start_kill();
    }
}

// --- Execution loop ---

/// Session trait for the dbt execution loop.
/// Provides heartbeat (cancel detection) and event streaming to the control plane.
pub(crate) trait DbtExecutionSession {
    /// Send a heartbeat and return whether cancellation was requested.
    fn heartbeat(
        &self,
    ) -> impl std::future::Future<Output = AppResult<bool>> + Send;

    /// Send an execution event to the control plane.
    fn send_event(
        &self,
        event: ExecutionEvent,
    ) -> impl std::future::Future<Output = AppResult<()>> + Send;
}

/// Configuration for the dbt execution loop.
pub(crate) struct DbtExecutionConfig {
    /// Whether to parse dbt JSON log events from stdout.
    pub parse_dbt_logs: bool,
    /// Whether to emit pretty terminal output (local mode).
    pub pretty_terminal_output: bool,
    /// Identifiers for logging.
    pub invocation_id: uuid::Uuid,
    pub worker_id: String,
}

/// Result of a completed dbt execution loop.
pub(crate) struct DbtExecutionResult {
    pub child_result: DbtChildResult,
    pub cancel_requested: bool,
    pub dbt_version: Option<String>,
}

/// Run the full dbt execution loop: stream stdout, heartbeat, handle cancellation.
///
/// This absorbs the duplicated `tokio::select!` pattern from both normal execution
/// and validation into a single implementation.
pub(crate) async fn run_dbt_execution(
    mut dbt_child: DbtChild,
    session: &impl DbtExecutionSession,
    config: &DbtExecutionConfig,
) -> AppResult<DbtExecutionResult> {
    let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(2));
    let mut dbt_version: Option<String> = None;
    let mut cancel_requested = false;

    loop {
        tokio::select! {
            line = dbt_child.stdout_lines.next_line() => {
                let Some(line) = line? else { break; };
                if config.parse_dbt_logs {
                    if let Some(event) = LogEvent::parse(&line) {
                        if dbt_version.is_none() && event.info.name == "MainReportVersion" {
                            dbt_version = event.data.get("version")
                                .and_then(serde_json::Value::as_str)
                                .map(ToString::to_string);
                        }
                        emit_dbt_log_output(config, &event);
                        session.send_event(ExecutionEvent {
                            kind: ExecutionEventKind::DbtLog,
                            occurred_at: chrono::Utc::now(),
                            text: event.render_text_line(),
                            raw_line: Some(line),
                            dbt_event_name: Some(event.info.name.clone()),
                            node_unique_id: event.data.get("node_info")
                                .and_then(|v| v.get("unique_id"))
                                .and_then(|v| v.as_str())
                                .map(ToString::to_string),
                            level: Some(event.info.level.clone()),
                            error: None,
                        }).await?;
                    } else {
                        emit_stream_output(config, "stdout", &line);
                        session.send_event(ExecutionEvent {
                            kind: ExecutionEventKind::StdoutLine,
                            occurred_at: chrono::Utc::now(),
                            text: Some(line.clone()),
                            raw_line: Some(line),
                            dbt_event_name: None,
                            node_unique_id: None,
                            level: None,
                            error: None,
                        }).await?;
                    }
                } else {
                    emit_stream_output(config, "stdout", &line);
                    session.send_event(ExecutionEvent {
                        kind: ExecutionEventKind::StdoutLine,
                        occurred_at: chrono::Utc::now(),
                        text: Some(line.clone()),
                        raw_line: Some(line),
                        dbt_event_name: None,
                        node_unique_id: None,
                        level: None,
                        error: None,
                    }).await?;
                }
            }
            _ = heartbeat.tick() => {
                let should_cancel = session.heartbeat().await?;
                if should_cancel {
                    warn!(
                        invocation_id = %config.invocation_id,
                        worker_id = %config.worker_id,
                        "cancel requested by control plane"
                    );
                    cancel_requested = true;
                    dbt_child.start_kill();
                }
            }
        }
    }

    let result = dbt_child.wait().await?;
    for line in &result.stderr_lines {
        emit_stream_output(config, "stderr", line);
        session
            .send_event(ExecutionEvent {
                kind: ExecutionEventKind::StderrLine,
                occurred_at: chrono::Utc::now(),
                text: Some(line.clone()),
                raw_line: Some(line.clone()),
                dbt_event_name: None,
                node_unique_id: None,
                level: None,
                error: None,
            })
            .await?;
    }

    Ok(DbtExecutionResult {
        child_result: result,
        cancel_requested,
        dbt_version,
    })
}

fn emit_dbt_log_output(config: &DbtExecutionConfig, event: &LogEvent) {
    let rendered = event.render_text_line();
    if config.pretty_terminal_output {
        if let Some(rendered) = rendered {
            println!("{rendered}");
        }
        return;
    }
    tracing::info!(
        invocation_id = %config.invocation_id,
        worker_id = %config.worker_id,
        event_type = "dbt.log",
        dbt_event_name = %event.info.name,
        level = %event.info.level,
        node_unique_id = event.data.get("node_info")
            .and_then(|v| v.get("unique_id"))
            .and_then(|v| v.as_str())
            .unwrap_or(""),
        text = rendered.as_deref().unwrap_or(""),
        "worker invocation event"
    );
}

fn emit_stream_output(config: &DbtExecutionConfig, stream: &str, line: &str) {
    if config.pretty_terminal_output {
        match stream {
            "stderr" => eprintln!("{line}"),
            _ => println!("{line}"),
        }
        return;
    }
    tracing::info!(
        invocation_id = %config.invocation_id,
        worker_id = %config.worker_id,
        event_type = if stream == "stderr" { "stderr.line" } else { "stdout.line" },
        stream = %stream,
        text = %line,
        "worker invocation event"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Mock session for testing the execution loop without HTTP.
    struct MockSession {
        events: Arc<tokio::sync::Mutex<Vec<ExecutionEvent>>>,
        heartbeat_count: Arc<AtomicUsize>,
        cancel_after: Option<usize>,
    }

    impl MockSession {
        fn new() -> Self {
            Self {
                events: Arc::new(tokio::sync::Mutex::new(Vec::new())),
                heartbeat_count: Arc::new(AtomicUsize::new(0)),
                cancel_after: None,
            }
        }

        fn with_cancel_after(mut self, n: usize) -> Self {
            self.cancel_after = Some(n);
            self
        }
    }

    impl DbtExecutionSession for MockSession {
        async fn heartbeat(&self) -> AppResult<bool> {
            let count = self.heartbeat_count.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(self.cancel_after.is_some_and(|n| count >= n))
        }

        async fn send_event(&self, event: ExecutionEvent) -> AppResult<()> {
            self.events.lock().await.push(event);
            Ok(())
        }
    }

    #[tokio::test]
    async fn execution_loop_captures_stdout_events() {
        let child = DbtChild::spawn("echo", "hello", &[], Path::new(".")).unwrap();
        let session = MockSession::new();
        let config = DbtExecutionConfig {
            parse_dbt_logs: false,
            pretty_terminal_output: false,
            invocation_id: uuid::Uuid::nil(),
            worker_id: "test".to_string(),
        };
        let result = run_dbt_execution(child, &session, &config).await.unwrap();
        assert_eq!(result.child_result.exit_code, 0);
        assert!(!result.cancel_requested);
        let events = session.events.lock().await;
        assert!(events.iter().any(|e| matches!(e.kind, ExecutionEventKind::StdoutLine)));
    }

    #[tokio::test]
    async fn execution_loop_captures_stderr_events() {
        let child = DbtChild::spawn(
            "sh",
            "-c",
            &[OsString::from("echo err >&2")],
            Path::new("."),
        )
        .unwrap();
        let session = MockSession::new();
        let config = DbtExecutionConfig {
            parse_dbt_logs: false,
            pretty_terminal_output: false,
            invocation_id: uuid::Uuid::nil(),
            worker_id: "test".to_string(),
        };
        let result = run_dbt_execution(child, &session, &config).await.unwrap();
        assert_eq!(result.child_result.exit_code, 0);
        let events = session.events.lock().await;
        assert!(events.iter().any(|e| matches!(e.kind, ExecutionEventKind::StderrLine)));
    }

    #[tokio::test]
    async fn execution_loop_cancels_on_heartbeat() {
        let child = DbtChild::spawn("sleep", "60", &[], Path::new(".")).unwrap();
        let session = MockSession::new().with_cancel_after(1);
        let config = DbtExecutionConfig {
            parse_dbt_logs: false,
            pretty_terminal_output: false,
            invocation_id: uuid::Uuid::nil(),
            worker_id: "test".to_string(),
        };
        let result = run_dbt_execution(child, &session, &config).await.unwrap();
        assert!(result.cancel_requested);
        assert_ne!(result.child_result.exit_code, 0);
    }

    #[tokio::test]
    async fn execution_loop_reports_nonzero_exit() {
        let child =
            DbtChild::spawn("sh", "-c", &[OsString::from("exit 7")], Path::new(".")).unwrap();
        let session = MockSession::new();
        let config = DbtExecutionConfig {
            parse_dbt_logs: false,
            pretty_terminal_output: false,
            invocation_id: uuid::Uuid::nil(),
            worker_id: "test".to_string(),
        };
        let result = run_dbt_execution(child, &session, &config).await.unwrap();
        assert_eq!(result.child_result.exit_code, 7);
        assert!(!result.cancel_requested);
    }

    #[tokio::test]
    async fn spawn_captures_stdout() {
        let mut child =
            DbtChild::spawn("echo", "hello world", &[], Path::new(".")).expect("spawn echo");
        let line = child.stdout_lines.next_line().await.unwrap();
        assert_eq!(line.as_deref(), Some("hello world"));
        let result = child.wait().await.unwrap();
        assert_eq!(result.exit_code, 0);
        assert!(result.stderr_lines.is_empty());
    }

    #[tokio::test]
    async fn spawn_captures_stderr() {
        let mut child = DbtChild::spawn(
            "sh",
            "-c",
            &[OsString::from("echo err >&2")],
            Path::new("."),
        )
        .expect("spawn sh");
        while child.stdout_lines.next_line().await.unwrap().is_some() {}
        let result = child.wait().await.unwrap();
        assert_eq!(result.exit_code, 0);
        assert_eq!(result.stderr_lines, vec!["err"]);
    }

    #[tokio::test]
    async fn spawn_reports_nonzero_exit_code() {
        let mut child = DbtChild::spawn("sh", "-c", &[OsString::from("exit 42")], Path::new("."))
            .expect("spawn sh");
        while child.stdout_lines.next_line().await.unwrap().is_some() {}
        let result = child.wait().await.unwrap();
        assert_eq!(result.exit_code, 42);
    }

    #[tokio::test]
    async fn spawn_fails_for_missing_executable() {
        let err = DbtChild::spawn("/nonexistent/binary", "arg", &[], Path::new("."));
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn start_kill_terminates_process() {
        let mut child = DbtChild::spawn("sleep", "60", &[], Path::new(".")).expect("spawn sleep");
        child.start_kill();
        // After kill, stdout closes and wait completes
        while child.stdout_lines.next_line().await.unwrap().is_some() {}
        let result = child.wait().await.unwrap();
        assert_ne!(result.exit_code, 0);
    }
}
