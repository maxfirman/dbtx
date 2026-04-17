use crate::api::{InvocationEvent, InvocationLifecycleStatus, InvocationStatusResponse};
use crate::db::{Db, InvocationPersistenceRecord};
use crate::error::AppResult;
use crate::event::LogEvent;
use crate::execution::{ExecutionCompletion, ExecutionEvent, ExecutionEventKind};
use async_stream::stream;
use axum::response::sse::Event;
use chrono::Utc;
use futures_util::Stream;
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::{Mutex, broadcast};
use tokio::time::{Duration, sleep};
use uuid::Uuid;

#[derive(Clone, Default)]
pub(crate) struct InvocationManager {
    inner: Arc<Mutex<HashMap<Uuid, Arc<InvocationRuntime>>>>,
}

#[derive(Debug, Clone)]
pub(crate) struct SequencedInvocationEvent {
    pub(crate) sequence: u64,
    pub(crate) event: InvocationEvent,
}

#[derive(Default)]
struct InvocationHistory {
    items: Vec<SequencedInvocationEvent>,
}

pub(crate) struct InvocationRuntime {
    history: Mutex<InvocationHistory>,
    tx: broadcast::Sender<SequencedInvocationEvent>,
    persistence: Mutex<Option<InvocationPersistence>>,
}

#[derive(Clone)]
pub(crate) struct InvocationRecorder {
    db: Db,
    invocation_id: Uuid,
    runtime: Arc<InvocationRuntime>,
}

#[derive(Clone)]
pub(crate) struct InvocationPersistence {
    pub(crate) run_id: Uuid,
    pub(crate) project_id: i64,
    pub(crate) environment_id: i64,
    pub(crate) promote_base_manifest: bool,
}

impl InvocationPersistence {
    pub(crate) fn from_record(record: InvocationPersistenceRecord) -> Option<Self> {
        Some(Self {
            run_id: record.run_id?,
            project_id: record.project_id?,
            environment_id: record.environment_id?,
            promote_base_manifest: record.promote_base_manifest,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn persist_log_event(
        &self,
        db: &Db,
        invocation_id: Uuid,
        run_id: Uuid,
        project_id: i64,
        environment_id: i64,
        sequence: i64,
        log_event: &LogEvent,
    ) -> AppResult<()> {
        db.persist_log_event(
            Some(invocation_id),
            run_id,
            project_id,
            environment_id,
            sequence,
            log_event,
        )
        .await
    }

    async fn persist_raw_line(
        &self,
        db: &Db,
        run_id: Uuid,
        sequence: i64,
        raw_line: &str,
    ) -> AppResult<()> {
        db.persist_raw_line(run_id, sequence, raw_line).await
    }
}

impl InvocationManager {
    pub(crate) async fn get_or_create(
        &self,
        invocation_id: Uuid,
        persistence: Option<InvocationPersistence>,
    ) -> Arc<InvocationRuntime> {
        let mut guard = self.inner.lock().await;
        if let Some(runtime) = guard.get(&invocation_id) {
            if persistence.is_some() {
                *runtime.persistence.lock().await = persistence;
            }
            return runtime.clone();
        }
        let (tx, _) = broadcast::channel(1024);
        let runtime = Arc::new(InvocationRuntime {
            history: Mutex::new(InvocationHistory::default()),
            tx,
            persistence: Mutex::new(persistence),
        });
        guard.insert(invocation_id, runtime.clone());
        runtime
    }

    pub(crate) fn schedule_cleanup(&self, invocation_id: Uuid) {
        let inner = self.inner.clone();
        tokio::spawn(async move {
            sleep(Duration::from_secs(300)).await;
            inner.lock().await.remove(&invocation_id);
        });
    }
}

impl InvocationRuntime {
    pub(crate) async fn push_event(&self, sequence: u64, event: InvocationEvent) {
        let sequenced = {
            let mut history = self.history.lock().await;
            let sequenced = SequencedInvocationEvent { sequence, event };
            history.items.push(sequenced.clone());
            sequenced
        };
        let _ = self.tx.send(sequenced);
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<SequencedInvocationEvent> {
        self.tx.subscribe()
    }
}

impl InvocationRecorder {
    pub(crate) fn new(db: Db, invocation_id: Uuid, runtime: Arc<InvocationRuntime>) -> Self {
        Self {
            db,
            invocation_id,
            runtime,
        }
    }

    pub(crate) async fn record(&self, event: ExecutionEvent) -> AppResult<()> {
        let sse_event = InvocationEvent {
            event_type: match event.kind {
                ExecutionEventKind::StdoutLine => "stdout.line".to_string(),
                ExecutionEventKind::StderrLine => "stderr.line".to_string(),
                ExecutionEventKind::DbtLog => "dbt.log".to_string(),
            },
            timestamp: event.occurred_at,
            text: event.text.clone(),
            stream: match event.kind {
                ExecutionEventKind::StdoutLine | ExecutionEventKind::DbtLog => {
                    Some("stdout".to_string())
                }
                ExecutionEventKind::StderrLine => Some("stderr".to_string()),
            },
            dbt_event_name: event.dbt_event_name.clone(),
            node_unique_id: event.node_unique_id.clone(),
            level: event.level.clone(),
            exit_code: None,
            error: event.error.clone(),
        };
        let sequence = self
            .db
            .append_invocation_event(self.invocation_id, &sse_event)
            .await? as i64;
        self.runtime.push_event(sequence as u64, sse_event).await;
        if let Some(persistence) = self.persistence().await? {
            match event.kind {
                ExecutionEventKind::DbtLog => {
                    if let Some(raw_line) = event.raw_line.as_deref()
                        && let Some(log_event) = LogEvent::parse(raw_line)
                    {
                        persistence
                            .persist_log_event(
                                &self.db,
                                self.invocation_id,
                                persistence.run_id,
                                persistence.project_id,
                                persistence.environment_id,
                                sequence,
                                &log_event,
                            )
                            .await?;
                    }
                }
                ExecutionEventKind::StdoutLine => {
                    if let Some(raw_line) = event.raw_line.as_deref().or(event.text.as_deref()) {
                        persistence
                            .persist_raw_line(&self.db, persistence.run_id, sequence, raw_line)
                            .await?;
                    }
                }
                ExecutionEventKind::StderrLine => {}
            }
        }
        Ok(())
    }

    pub(crate) async fn complete(
        &self,
        worker_id: &str,
        lease_token: Uuid,
        completion: ExecutionCompletion,
    ) -> AppResult<()> {
        self.db
            .complete_invocation(self.invocation_id, worker_id, lease_token, &completion)
            .await?;
        let completed_event = InvocationEvent {
            event_type: "invocation.completed".to_string(),
            timestamp: Utc::now(),
            text: None,
            stream: None,
            dbt_event_name: None,
            node_unique_id: None,
            level: None,
            exit_code: Some(completion.exit_code),
            error: completion.error.clone(),
        };
        let sequence = self
            .db
            .append_invocation_event(self.invocation_id, &completed_event)
            .await;
        if let Ok(sequence) = sequence {
            self.runtime.push_event(sequence, completed_event).await;
        }
        Ok(())
    }

    pub(crate) async fn is_running(&self) -> bool {
        matches!(
            self.db.get_invocation_status(self.invocation_id).await,
            Ok(InvocationStatusResponse {
                status: InvocationLifecycleStatus::Running,
                ..
            })
        )
    }

    async fn persistence(&self) -> AppResult<Option<InvocationPersistence>> {
        let mut guard = self.runtime.persistence.lock().await;
        if guard.is_none() {
            let loaded = self
                .db
                .get_invocation_persistence(self.invocation_id, None, None)
                .await?;
            *guard = InvocationPersistence::from_record(loaded);
        }
        Ok(guard.clone())
    }
}

pub(crate) fn started_invocation_event() -> InvocationEvent {
    InvocationEvent {
        event_type: "invocation.started".to_string(),
        timestamp: Utc::now(),
        text: None,
        stream: None,
        dbt_event_name: None,
        node_unique_id: None,
        level: None,
        exit_code: None,
        error: None,
    }
}

pub(crate) fn event_stream(
    history: Vec<SequencedInvocationEvent>,
    last_history_sequence: u64,
    mut rx: broadcast::Receiver<SequencedInvocationEvent>,
) -> impl Stream<Item = Result<Event, Infallible>> {
    stream! {
        let mut last_seen_sequence = last_history_sequence;
        for item in history {
            last_seen_sequence = item.sequence;
            yield Ok(to_sse_event(&item));
        }
        loop {
            match rx.recv().await {
                Ok(item) if item.sequence > last_seen_sequence => {
                    last_seen_sequence = item.sequence;
                    yield Ok(to_sse_event(&item))
                }
                Ok(_) => continue,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    }
}

fn to_sse_event(item: &SequencedInvocationEvent) -> Event {
    Event::default()
        .event(item.event.event_type.clone())
        .id(item.sequence.to_string())
        .data(serde_json::to_string(&item.event).unwrap_or_else(|_| "{}".to_string()))
}

#[cfg(test)]
mod tests {
    use super::{SequencedInvocationEvent, event_stream};
    use crate::api::InvocationEvent;
    use chrono::Utc;
    use futures_util::StreamExt;
    use tokio::sync::broadcast;

    fn sample_event(text: &str) -> InvocationEvent {
        InvocationEvent {
            event_type: "stdout.line".to_string(),
            timestamp: Utc::now(),
            text: Some(text.to_string()),
            stream: Some("stdout".to_string()),
            dbt_event_name: None,
            node_unique_id: None,
            level: None,
            exit_code: None,
            error: None,
        }
    }

    #[tokio::test]
    async fn event_stream_replays_history_then_live_events() {
        let (tx, rx) = broadcast::channel(16);
        let history = vec![SequencedInvocationEvent {
            sequence: 1,
            event: sample_event("one"),
        }];
        let mut stream = Box::pin(event_stream(history, 1, rx));

        let first = stream.next().await.expect("history item").expect("event");
        let _first = first;

        tx.send(SequencedInvocationEvent {
            sequence: 2,
            event: sample_event("two"),
        })
        .expect("send live event");
        let second = stream.next().await.expect("live item").expect("event");
        let _second = second;
    }

    #[tokio::test]
    async fn event_stream_skips_duplicate_live_events_already_in_history() {
        let (tx, rx) = broadcast::channel(16);
        let history = vec![SequencedInvocationEvent {
            sequence: 1,
            event: sample_event("one"),
        }];
        let mut stream = Box::pin(event_stream(history, 1, rx));

        let _first = stream.next().await.expect("history item").expect("event");

        tx.send(SequencedInvocationEvent {
            sequence: 1,
            event: sample_event("one"),
        })
        .expect("send duplicate live event");
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), stream.next())
                .await
                .is_err(),
            "duplicate live event should not be emitted"
        );
        tx.send(SequencedInvocationEvent {
            sequence: 2,
            event: sample_event("two"),
        })
        .expect("send next live event");

        let _second = stream.next().await.expect("live item").expect("event");
    }
}
