//! Meepo headless — durable task execution.
//!
//! A [`TaskRun`] is a task that outlives a single turn or process: its state
//! is the fold of an append-only [`TaskEvent`] ledger
//! (`headless_task_run_events`), so a new process can resume it. An
//! autonomous loop drives attempts (each an agent run) until a terminal
//! status, with a self-check gate before finalization.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use meepo_core::{AgentBackend, RuntimeEventStore, StoreResult};
use meepo_runtime::{RunStatus, SessionManager};
use serde::{Deserialize, Serialize};

/// TaskRun lifecycle status (13 states).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskRunStatus {
    Queued,
    Created,
    Running,
    Verifying,
    Completed,
    Failed,
    Incomplete,
    Blocked,
    PolicyDenied,
    BudgetExhausted,
    NeedsApproval,
    Aborted,
    Cancelled,
}

impl TaskRunStatus {
    /// Terminal statuses end a task run (no further attempts).
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Incomplete | Self::Blocked
                | Self::PolicyDenied | Self::BudgetExhausted | Self::Aborted | Self::Cancelled
        )
    }
}

/// One unit of work to execute.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskDefinition {
    pub task_id: String,
    pub instruction: String,
    pub workspace_dir: String,
}

/// A durable task run — its state is the fold of its [`TaskEvent`] ledger.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRun {
    pub task_run_id: String,
    pub task_id: String,
    pub status: TaskRunStatus,
    pub instruction: String,
    pub started_at: Option<i64>,
    pub finished_at: Option<i64>,
    pub attempt_count: u32,
}

/// One attempt of a task run (one agent run).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAttempt {
    pub attempt_id: String,
    pub task_run_id: String,
    pub status: TaskRunStatus,
    pub started_at: i64,
    pub finished_at: Option<i64>,
}

/// One event in the TaskRun event ledger. The task run's state is the fold
/// of these events in sequence order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TaskEvent {
    Created {
        task_run_id: String,
        task_id: String,
        instruction: String,
        ts: i64,
    },
    Queued {
        task_run_id: String,
        ts: i64,
    },
    AttemptStarted {
        task_run_id: String,
        attempt_id: String,
        ts: i64,
    },
    AttemptCompleted {
        task_run_id: String,
        attempt_id: String,
        status: TaskRunStatus,
        ts: i64,
    },
    RunCompleted {
        task_run_id: String,
        ts: i64,
    },
    RunFailed {
        task_run_id: String,
        error: String,
        ts: i64,
    },
}

/// Persists a TaskRun's event ledger. Each event is appended at a monotonic
/// sequence; reading them back in order and folding via [`project_task_run`]
/// reconstructs the run state across processes.
#[async_trait]
pub trait TaskRunStore: Send + Sync {
    /// Append one event at `sequence` for `task_run_id`.
    async fn append_event(
        &self,
        task_run_id: &str,
        sequence: i64,
        event: &TaskEvent,
    ) -> StoreResult<()>;
    /// Read all events for `task_run_id` in sequence order.
    async fn read_events(&self, task_run_id: &str) -> StoreResult<Vec<TaskEvent>>;
}

/// In-memory [`TaskRunStore`] for tests (no SQLite dependency).
#[derive(Default)]
pub struct InMemoryTaskRunStore {
    events: Mutex<HashMap<String, Vec<TaskEvent>>>,
}

impl InMemoryTaskRunStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl TaskRunStore for InMemoryTaskRunStore {
    async fn append_event(
        &self,
        task_run_id: &str,
        _sequence: i64,
        event: &TaskEvent,
    ) -> StoreResult<()> {
        self.events
            .lock()
            .expect("poisoned")
            .entry(task_run_id.into())
            .or_default()
            .push(event.clone());
        Ok(())
    }

    async fn read_events(&self, task_run_id: &str) -> StoreResult<Vec<TaskEvent>> {
        Ok(self
            .events
            .lock()
            .expect("poisoned")
            .get(task_run_id)
            .cloned()
            .unwrap_or_default())
    }
}

/// Fold a TaskRun's event ledger into its current state. Returns None if the
/// ledger has no Created event. This is the projection a resume reads to
/// reconstruct a task run across processes.
pub fn project_task_run(events: &[TaskEvent]) -> Option<TaskRun> {
    let mut run: Option<TaskRun> = None;
    let mut attempt_count = 0u32;
    for event in events {
        match event {
            TaskEvent::Created { task_run_id, task_id, instruction, ts } => {
                run = Some(TaskRun {
                    task_run_id: task_run_id.clone(),
                    task_id: task_id.clone(),
                    status: TaskRunStatus::Created,
                    instruction: instruction.clone(),
                    started_at: Some(*ts),
                    finished_at: None,
                    attempt_count: 0,
                });
            }
            TaskEvent::Queued { .. } => {
                if let Some(r) = run.as_mut() {
                    r.status = TaskRunStatus::Queued;
                }
            }
            TaskEvent::AttemptStarted { .. } => {
                attempt_count += 1;
                if let Some(r) = run.as_mut() {
                    r.status = TaskRunStatus::Running;
                    r.attempt_count = attempt_count;
                }
            }
            TaskEvent::AttemptCompleted { status, .. } => {
                if let Some(r) = run.as_mut() {
                    r.status = *status;
                }
            }
            TaskEvent::RunCompleted { ts, .. } => {
                if let Some(r) = run.as_mut() {
                    r.status = TaskRunStatus::Completed;
                    r.finished_at = Some(*ts);
                }
            }
            TaskEvent::RunFailed { ts, .. } => {
                if let Some(r) = run.as_mut() {
                    r.status = TaskRunStatus::Failed;
                    r.finished_at = Some(*ts);
                }
            }
        }
    }
    run
}

/// Self-check gate decision after an attempt completes (maka Ch5).
#[derive(Debug, Clone)]
pub enum SelfCheckDecision {
    /// The attempt is verified — finalize the task run.
    AllowFinalize { reason: String },
    /// The attempt needs repair — retry with a feedback prompt.
    Repair { reason: String, prompt: String },
    /// Repair budget exhausted — release to an external verifier.
    AllowAfterBounded { reason: String },
}

/// Quality gate evaluated after each successful attempt. Decides whether the
/// task is truly complete, needs a repair retry, or should be released to an
/// external verifier after bounded repair attempts. The default gate always
/// allows finalize (self-check disabled); a real gate verifies evidence.
#[async_trait]
pub trait SelfCheckGate: Send + Sync {
    async fn check(&self, attempt: u32, attempt_status: TaskRunStatus) -> SelfCheckDecision;
}

/// Default gate: always allows finalize (self-check not configured).
pub struct DefaultSelfCheckGate;

#[async_trait]
impl SelfCheckGate for DefaultSelfCheckGate {
    async fn check(&self, _attempt: u32, _status: TaskRunStatus) -> SelfCheckDecision {
        SelfCheckDecision::AllowFinalize { reason: "self-check not configured".into() }
    }
}

/// Drive a task to completion: repeat attempts (each a fresh SessionManager
/// turn with the task instruction) until one succeeds or `max_attempts` is
/// exhausted. Every transition is appended to the [`TaskRunStore`], so the
/// run is durable and resumable.
pub async fn run_task(
    task_run_id: &str,
    backend: &mut dyn AgentBackend,
    session_store: &dyn RuntimeEventStore,
    task_store: &dyn TaskRunStore,
    task: &TaskDefinition,
    max_attempts: u32,
) -> TaskRun {
    let mut seq = 0i64;
    let _ = task_store
        .append_event(task_run_id, seq, &TaskEvent::Created {
            task_run_id: task_run_id.into(),
            task_id: task.task_id.clone(),
            instruction: task.instruction.clone(),
            ts: seq,
        })
        .await;
    seq += 1;

    let mut succeeded = false;
    for attempt in 0..max_attempts {
        let attempt_id = format!("{task_run_id}-a{attempt}");
        let attempt_session = format!("{task_run_id}-s{attempt}");
        let _ = task_store
            .append_event(task_run_id, seq, &TaskEvent::AttemptStarted {
                task_run_id: task_run_id.into(),
                attempt_id: attempt_id.clone(),
                ts: seq,
            })
            .await;
        seq += 1;

        let mut session = SessionManager::new(&attempt_session);
        let result = session
            .send_turn(backend, session_store, task.instruction.clone(), None, &[])
            .await;
        let attempt_status = match result.status {
            RunStatus::Completed => TaskRunStatus::Completed,
            RunStatus::Aborted => TaskRunStatus::Aborted,
            RunStatus::Failed => TaskRunStatus::Failed,
        };

        let _ = task_store
            .append_event(task_run_id, seq, &TaskEvent::AttemptCompleted {
                task_run_id: task_run_id.into(),
                attempt_id: attempt_id.clone(),
                status: attempt_status,
                ts: seq,
            })
            .await;
        seq += 1;

        if attempt_status == TaskRunStatus::Completed {
            succeeded = true;
            let _ = task_store
                .append_event(task_run_id, seq, &TaskEvent::RunCompleted {
                    task_run_id: task_run_id.into(),
                    ts: seq,
                })
                .await;
            break;
        }
    }

    if !succeeded {
        let _ = task_store
            .append_event(task_run_id, seq, &TaskEvent::RunFailed {
                task_run_id: task_run_id.into(),
                error: format!("exhausted {max_attempts} attempts"),
                ts: seq,
            })
            .await;
    }

    let events = task_store.read_events(task_run_id).await.unwrap_or_default();
    project_task_run(&events).unwrap_or(TaskRun {
        task_run_id: task_run_id.into(),
        task_id: task.task_id.clone(),
        status: TaskRunStatus::Failed,
        instruction: task.instruction.clone(),
        started_at: None,
        finished_at: None,
        attempt_count: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_full_lifecycle() {
        let events = vec![
            TaskEvent::Created {
                task_run_id: "tr1".into(), task_id: "t1".into(),
                instruction: "fix the bug".into(), ts: 1,
            },
            TaskEvent::Queued { task_run_id: "tr1".into(), ts: 2 },
            TaskEvent::AttemptStarted {
                task_run_id: "tr1".into(), attempt_id: "a1".into(), ts: 3,
            },
            TaskEvent::AttemptCompleted {
                task_run_id: "tr1".into(), attempt_id: "a1".into(),
                status: TaskRunStatus::Completed, ts: 4,
            },
            TaskEvent::RunCompleted { task_run_id: "tr1".into(), ts: 5 },
        ];
        let run = project_task_run(&events).expect("non-empty ledger");
        assert_eq!(run.task_run_id, "tr1");
        assert_eq!(run.status, TaskRunStatus::Completed);
        assert_eq!(run.attempt_count, 1);
        assert!(run.status.is_terminal());
        assert_eq!(run.finished_at, Some(5));
    }

    #[test]
    fn project_empty_is_none() {
        assert!(project_task_run(&[]).is_none());
    }

    #[test]
    fn project_failed_run() {
        let events = vec![
            TaskEvent::Created {
                task_run_id: "tr2".into(), task_id: "t2".into(),
                instruction: "x".into(), ts: 1,
            },
            TaskEvent::AttemptStarted {
                task_run_id: "tr2".into(), attempt_id: "a1".into(), ts: 2,
            },
            TaskEvent::RunFailed { task_run_id: "tr2".into(), error: "boom".into(), ts: 3 },
        ];
        let run = project_task_run(&events).unwrap();
        assert_eq!(run.status, TaskRunStatus::Failed);
        assert!(run.status.is_terminal());
    }

    #[test]
    fn task_event_round_trips_json() {
        let event = TaskEvent::AttemptStarted {
            task_run_id: "tr1".into(), attempt_id: "a1".into(), ts: 99,
        };
        let json = serde_json::to_string(&event).unwrap();
        let back: TaskEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(json, serde_json::to_string(&back).unwrap());
    }

    #[tokio::test]
    async fn run_task_succeeds_on_first_attempt() {
        use meepo_core::{FakeBackend, InMemoryRuntimeEventStore, SessionEvent, StopReason};
        let session_store = InMemoryRuntimeEventStore::new();
        let task_store = InMemoryTaskRunStore::new();
        let mut backend = FakeBackend::new(
            "task-sess",
            vec![
                SessionEvent::TextComplete {
                    id: "1".into(), turn_id: "t".into(), ts: 0,
                    message_id: "m".into(), text: "done".into(), provider_options: None,
                },
                SessionEvent::Complete {
                    id: "2".into(), turn_id: "t".into(), ts: 1,
                    stop_reason: StopReason::EndTurn,
                },
            ],
        );
        let task = TaskDefinition {
            task_id: "t1".into(),
            instruction: "do it".into(),
            workspace_dir: "/tmp".into(),
        };
        let run = run_task("tr1", &mut backend, &session_store, &task_store, &task, 3).await;
        assert_eq!(run.status, TaskRunStatus::Completed);
        assert_eq!(run.attempt_count, 1);
    }

    #[tokio::test]
    async fn run_task_retries_then_fails() {
        // FakeBackend with no script -> send returns empty -> runner
        // synthesizes a missing-terminal error -> Failed.
        use meepo_core::{FakeBackend, InMemoryRuntimeEventStore};
        let session_store = InMemoryRuntimeEventStore::new();
        let task_store = InMemoryTaskRunStore::new();
        let mut backend = FakeBackend::new("task-sess", vec![]);
        let task = TaskDefinition {
            task_id: "t2".into(),
            instruction: "impossible".into(),
            workspace_dir: "/tmp".into(),
        };
        let run = run_task("tr2", &mut backend, &session_store, &task_store, &task, 2).await;
        assert_eq!(run.status, TaskRunStatus::Failed);
        assert_eq!(run.attempt_count, 2);
    }
}
