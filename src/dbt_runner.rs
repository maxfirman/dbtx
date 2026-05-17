//! Shared dbt child process spawning and output capture.
//!
//! Provides `DbtChild` which wraps the common pattern of spawning a dbt process,
//! capturing stdout/stderr, and waiting for completion.

use crate::error::{AppError, AppResult};
use std::ffi::OsString;
use std::path::Path;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader, Lines};
use tokio::process::{Child, ChildStdout, Command};

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

#[cfg(test)]
mod tests {
    use super::*;

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
