// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

//! Bounded runtime observations. These values never authorize lifecycle changes or prove retirement.

use super::{GLOBAL_MRF_LIFECYCLE_SENDER, MrfLegacyResponsibility, MrfLifecycleCommand, MrfLifecycleControlError, MrfRuntime};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, LazyLock, RwLock};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use uuid::Uuid;

const READ_CAPACITY: usize = 8;
static READ_BUDGET: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(READ_CAPACITY)));
static SUMMARY: RwLock<Option<MrfLifecycleSummary>> = RwLock::new(None);

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MrfLifecycleSummary {
    pub contract_version: u32,
    pub sampled_at_ms: u64,
    pub checkpoint_readable: bool,
    pub checkpoint_owner: Option<Uuid>,
    pub checkpoint_sequence: Option<u64>,
    pub active_responsibility_count: usize,
    pub held_count: usize,
    pub legacy_generation_unknown_count: usize,
    pub bucket_incarnation_changed_count: usize,
    pub operator_accepted_count: usize,
    pub best_effort_queue_depth: usize,
    pub responsibility_bytes: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MrfResponsibilityObservation {
    pub contract_version: u32,
    pub observed_at_ms: u64,
    /// Absence means not observed in this runtime, never verified or discharged.
    pub responsibility: Option<MrfLegacyResponsibility>,
}

/// Returns the last published sample without scanning or queueing work.
///
/// # Errors
/// Returns unavailable until the consumer publishes a sample, or after it stops.
pub fn lifecycle_summary() -> Result<MrfLifecycleSummary, MrfLifecycleControlError> {
    if GLOBAL_MRF_LIFECYCLE_SENDER.get().is_none_or(mpsc::Sender::is_closed) {
        return Err(MrfLifecycleControlError::Unavailable);
    }
    SUMMARY
        .read()
        .map_err(|_| MrfLifecycleControlError::Unavailable)?
        .clone()
        .ok_or(MrfLifecycleControlError::Unavailable)
}

/// Observes one current responsibility, including Active entries excluded from legacy lists.
/// Cancellation skips work that has not started; observations are not durable success receipts.
///
/// # Errors
/// Rejects nil IDs, an exhausted read budget, or an unavailable/unreadable controller.
pub async fn responsibility(id: Uuid) -> Result<MrfResponsibilityObservation, MrfLifecycleControlError> {
    if id.is_nil() {
        return Err(MrfLifecycleControlError::InvalidAction("responsibility ID must be non-nil".into()));
    }
    let permit = read_permit()?;
    let sender = GLOBAL_MRF_LIFECYCLE_SENDER
        .get()
        .ok_or(MrfLifecycleControlError::Unavailable)?;
    let (response, receiver) = oneshot::channel();
    sender
        .try_send(MrfLifecycleCommand::Get {
            id,
            response,
            _permit: permit,
        })
        .map_err(queue_error)?;
    receiver.await.map_err(|_| MrfLifecycleControlError::Unavailable)?
}

pub(super) fn read_permit() -> Result<OwnedSemaphorePermit, MrfLifecycleControlError> {
    READ_BUDGET
        .clone()
        .try_acquire_owned()
        .map_err(|_| MrfLifecycleControlError::Busy)
}

pub(super) fn queue_error<T>(error: mpsc::error::TrySendError<T>) -> MrfLifecycleControlError {
    match error {
        mpsc::error::TrySendError::Full(_) => MrfLifecycleControlError::Busy,
        mpsc::error::TrySendError::Closed(_) => MrfLifecycleControlError::Unavailable,
    }
}

pub(super) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |time| u64::try_from(time.as_millis()).unwrap_or(u64::MAX))
}

pub(super) fn publish_summary(runtime: &MrfRuntime, counts: &super::partial_write::PartialWriteLifecycleSummary) {
    let unverified = counts.held_count
        + counts.legacy_generation_unknown_count
        + counts.bucket_incarnation_changed_count
        + counts.operator_accepted_count;
    let summary = MrfLifecycleSummary {
        contract_version: 1,
        sampled_at_ms: now_ms(),
        checkpoint_readable: runtime.replay_readable,
        checkpoint_owner: runtime.runtime_checkpoint.map(|(owner, _)| owner),
        checkpoint_sequence: runtime.runtime_checkpoint.map(|(_, sequence)| sequence),
        active_responsibility_count: runtime.partial_writes.depth().saturating_sub(unverified),
        held_count: counts.held_count,
        legacy_generation_unknown_count: counts.legacy_generation_unknown_count,
        bucket_incarnation_changed_count: counts.bucket_incarnation_changed_count,
        operator_accepted_count: counts.operator_accepted_count,
        best_effort_queue_depth: runtime.queue.depth(),
        responsibility_bytes: runtime.partial_writes.bytes(),
    };
    if let Ok(mut published) = SUMMARY.write() {
        *published = Some(summary);
    }
}

#[cfg(test)]
mod tests {
    use super::super::{MRF_LIFECYCLE_FORMAT_VERSION, MrfConsumerConfig, MrfQueue, PartialWrites, intent_digest};
    use super::*;
    use crate::heal::manager::HealManager;
    use crate::heal::storage::ECStoreHealStorage;
    use rustfs_common::mrf_channel::{MrfIntent, MrfKind, MrfScope};

    #[test]
    fn cancelled_read_holds_budget_until_dequeued() {
        let budget = Arc::new(Semaphore::new(1));
        let (response, receiver) = oneshot::channel();
        let command = MrfLifecycleCommand::Get {
            id: Uuid::new_v4(),
            response,
            _permit: budget.clone().try_acquire_owned().unwrap(),
        };
        drop(receiver);
        assert!(budget.clone().try_acquire_owned().is_err());
        drop(command);
        assert!(budget.try_acquire_owned().is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn management_queries_preserve_active_records_and_unreadable_checkpoint() {
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .prefix("mrf_management_queries")
            .build()
            .await;
        let manager =
            HealManager::new_without_root_recovery_for_test(Arc::new(ECStoreHealStorage::new(env.ecstore.clone())), None);
        let mut runtime = MrfRuntime {
            queue: MrfQueue::new(4, 8192),
            partial_writes: PartialWrites::default(),
            config: MrfConsumerConfig::default(),
            checkpoint_owner: Uuid::new_v4(),
            next_checkpoint_sequence: 2,
            lifecycle_format_version: MRF_LIFECYCLE_FORMAT_VERSION,
            new_since_flush: 0,
            dirty: false,
            journal_on_disk: true,
            retain_replay_journal: true,
            replay_readable: true,
            durable_replay_anchors: Vec::new(),
            replay_cleanup: None,
            runtime_checkpoint: Some((Uuid::new_v4(), 1)),
            backoff_until: None,
        };
        for (key, source) in [("active", Some(Uuid::new_v4())), ("legacy", None)] {
            runtime
                .admit_partial_write(
                    MrfIntent {
                        bucket: Arc::from("management"),
                        object: Arc::from(key),
                        version_id: None,
                        kind: MrfKind::PartialWrite,
                        delete_marker_purge: None,
                        scope: Some(MrfScope {
                            pool_index: 0,
                            set_index: 0,
                        }),
                        lease: None,
                        enqueued_at_ms: 1,
                        attempts: 0,
                    },
                    source,
                )
                .expect("retain responsibility fixture");
        }
        let active = runtime
            .partial_writes
            .checkpoint_records(intent_digest)
            .into_iter()
            .find(|row| row.source_bucket_incarnation_id.is_some())
            .expect("source-bound responsibility")
            .responsibility_id;
        let before = runtime.snapshot();
        let (sender, mut receiver) = mpsc::channel(16);
        assert!(GLOBAL_MRF_LIFECYCLE_SENDER.set(sender).is_ok(), "query test requires an isolated process");
        assert!(matches!(lifecycle_summary(), Err(MrfLifecycleControlError::Unavailable)));
        publish_summary(&runtime, &runtime.partial_writes.lifecycle_summary());
        let mut sample = lifecycle_summary().expect("published summary");
        assert_eq!(sample.active_responsibility_count, 1);
        assert_eq!(sample.legacy_generation_unknown_count, 1);
        assert!(sample.checkpoint_readable);
        sample.sampled_at_ms = 0;
        *SUMMARY.write().expect("summary fixture") = Some(sample.clone());

        let permits: Vec<_> = (0..READ_CAPACITY).map(|_| read_permit().expect("read slot")).collect();
        assert!(matches!(responsibility(active).await, Err(MrfLifecycleControlError::Busy)));
        assert!(matches!(
            super::super::list_legacy_responsibilities(None, 1).await,
            Err(MrfLifecycleControlError::Busy)
        ));
        drop(permits);

        let (observed, ()) = tokio::join!(responsibility(active), async {
            let command = receiver.recv().await.expect("queued Get");
            runtime.handle_lifecycle_command(&manager, command).await;
        });
        let row = observed
            .expect("Get succeeds")
            .responsibility
            .expect("Active responsibility remains observable");
        assert_eq!(row.responsibility_id, active);
        assert_eq!(row.status, "active");
        let (listed, ()) = tokio::join!(super::super::list_legacy_responsibilities(None, 2), async {
            let command = receiver.recv().await.expect("queued List");
            runtime.handle_lifecycle_command(&manager, command).await;
        });
        let listed = listed.expect("legacy list succeeds");
        assert_eq!(listed.responsibilities.len(), 1);
        assert_ne!(listed.responsibilities[0].responsibility_id, active);
        assert_eq!(runtime.snapshot(), before, "observations must preserve the retained payloads");
        assert_eq!(
            lifecycle_summary().expect("cached summary").sampled_at_ms,
            sample.sampled_at_ms,
            "read commands must not republish summary metrics"
        );

        for list in [false, true] {
            let cancelled = tokio::spawn(async move {
                if list {
                    super::super::list_legacy_responsibilities(None, 1).await.map(|_| ())
                } else {
                    responsibility(active).await.map(|_| ())
                }
            });
            let command = tokio::time::timeout(std::time::Duration::from_secs(5), receiver.recv())
                .await
                .expect("query is enqueued promptly")
                .expect("queued cancellable query");
            cancelled.abort();
            assert!(cancelled.await.expect_err("query task cancelled").is_cancelled());
            assert_eq!(
                READ_BUDGET.available_permits(),
                READ_CAPACITY - 1,
                "queued work retains its slot after client cancellation"
            );
            runtime.handle_lifecycle_command(&manager, command).await;
            assert_eq!(READ_BUDGET.available_permits(), READ_CAPACITY);
            assert_eq!(runtime.snapshot(), before);
        }

        runtime.replay_readable = false;
        publish_summary(&runtime, &runtime.partial_writes.lifecycle_summary());
        assert!(
            !lifecycle_summary()
                .expect("unreadable summary remains observable")
                .checkpoint_readable
        );
        let (observed, ()) = tokio::join!(responsibility(active), async {
            let command = receiver.recv().await.expect("Get against unreadable checkpoint");
            runtime.handle_lifecycle_command(&manager, command).await;
        });
        assert!(matches!(observed, Err(MrfLifecycleControlError::CheckpointUnreadable)));
        let (listed, ()) = tokio::join!(super::super::list_legacy_responsibilities(None, 1), async {
            let command = receiver.recv().await.expect("List against unreadable checkpoint");
            runtime.handle_lifecycle_command(&manager, command).await;
        });
        assert!(matches!(listed, Err(MrfLifecycleControlError::CheckpointUnreadable)));
        assert_eq!(runtime.snapshot(), before);
        receiver.close();
        assert!(matches!(responsibility(active).await, Err(MrfLifecycleControlError::Unavailable)));
        assert!(
            matches!(lifecycle_summary(), Err(MrfLifecycleControlError::Unavailable)),
            "a stale sample cannot hide shutdown"
        );
        assert_eq!(READ_BUDGET.available_permits(), READ_CAPACITY);
    }

    #[tokio::test]
    async fn management_query_bounds_and_queue_errors_are_explicit() {
        assert!(matches!(
            responsibility(Uuid::nil()).await,
            Err(MrfLifecycleControlError::InvalidAction(_))
        ));
        let (sender, receiver) = mpsc::channel(1);
        sender.try_send(()).expect("fill queue");
        assert!(matches!(
            queue_error(sender.try_send(()).expect_err("full queue")),
            MrfLifecycleControlError::Busy
        ));
        drop(receiver);
        assert!(matches!(
            queue_error(sender.try_send(()).expect_err("closed queue")),
            MrfLifecycleControlError::Unavailable
        ));
        assert_eq!(READ_BUDGET.available_permits(), READ_CAPACITY);
    }
}
