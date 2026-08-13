//! Meepo headless — durable task execution.
//!
//! A [`TaskRun`] is a task that outlives a single turn or process: its state
//! is the fold of an append-only [`TaskEvent`] ledger
//! (`headless_task_run_events`), so a new process can resume it. An
//! autonomous loop drives attempts (each an agent run) until a terminal
//! status, with a self-check gate before finalization.

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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
}
