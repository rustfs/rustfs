// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::{PhysicalDiskBinding, ResourceIoUnit};
use rustfs_concurrency::adaptive::{
    NodeAdmission,
    permit_set::{NodeResourceCompletion, NodeResourcePermit},
};
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::io;
use std::sync::Arc;

tokio::task_local! {
    static REMOTE_RESOURCES: RemoteBackgroundIo;
}

/// Receiver-owned admission for an authenticated background RPC. The owner must
/// be the same node controller used by local Scanner and Heal work.
pub struct RemoteBackgroundIo {
    node: Arc<NodeAdmission>,
    admitted: RefCell<Option<NodeResourcePermit>>,
    progressed: Cell<bool>,
    finished: Cell<bool>,
}

impl RemoteBackgroundIo {
    pub fn new(node: Arc<NodeAdmission>) -> Self {
        Self {
            node,
            admitted: RefCell::new(None),
            progressed: Cell::new(false),
            finished: Cell::new(false),
        }
    }

    /// Move the request's reservations into its existing stream producer.
    /// Only physical subsets may be cloned into native I/O; never its node unit.
    pub fn take_current() -> Option<Self> {
        REMOTE_RESOURCES
            .try_with(|context| Self {
                node: context.node.clone(),
                admitted: RefCell::new(context.admitted.take()),
                progressed: Cell::new(context.progressed.replace(false)),
                finished: Cell::new(false),
            })
            .ok()
    }

    /// Defer a response's progress without keeping async admission alive while
    /// its body streams. The reader retains its own native resource ownership.
    pub fn take_current_completion() -> Option<NodeResourceCompletion> {
        REMOTE_RESOURCES
            .try_with(|context| {
                let admitted = context.admitted.take();
                if context.progressed.replace(false) {
                    admitted.map(NodeResourcePermit::into_completion)
                } else {
                    None
                }
            })
            .ok()
            .flatten()
    }

    /// A failed response or cancelled producer must not credit earlier phases.
    pub fn discard_current_progress() {
        let _ = REMOTE_RESOURCES.try_with(|context| context.progressed.set(false));
    }
}

impl Drop for RemoteBackgroundIo {
    fn drop(&mut self) {
        if self.finished.get()
            && self.progressed.get()
            && let Some(permit) = self.admitted.get_mut().take()
        {
            permit.complete();
        }
    }
}

/// Scope a verified request or its stream producer. An interrupted request
/// returns node capacity while native jobs retain their selected physical units.
pub async fn with_remote_background_io<F: Future>(context: Option<RemoteBackgroundIo>, operation: F) -> F::Output {
    match context {
        Some(context) => {
            REMOTE_RESOURCES
                .scope(context, async {
                    let result = operation.await;
                    REMOTE_RESOURCES.with(|context| context.finished.set(true));
                    result
                })
                .await
        }
        None => operation.await,
    }
}

pub(crate) fn active() -> bool {
    REMOTE_RESOURCES.try_with(|_| ()).is_ok()
}

pub(crate) fn resources() -> Option<ResourceIoUnit> {
    REMOTE_RESOURCES
        .try_with(|context| {
            context
                .admitted
                .borrow()
                .as_ref()
                .map(|permit| ResourceIoUnit::from(permit.resources().clone()))
        })
        .ok()
        .flatten()
}

pub(crate) fn record_progress() {
    let _ = REMOTE_RESOURCES.try_with(|context| context.progressed.set(true));
}

pub(crate) fn admit(binding: &PhysicalDiskBinding) -> io::Result<()> {
    REMOTE_RESOURCES
        .try_with(|context| {
            let deferred = || io::Error::from(io::ErrorKind::WouldBlock);
            let plan = binding.plan().ok_or_else(deferred)?;
            let mut admitted = context.admitted.borrow_mut();
            if let Some(permit) = admitted.as_ref() {
                // Reuse the request's receipt for append/read/rename phases at
                // capacity one. Additional attachments never wait under locks.
                let native = plan.capture_or_acquire_native(permit.resources()).map_err(|_| deferred())?;
                drop(native);
                if !plan.is_current() {
                    return Err(deferred());
                }
            } else {
                *admitted = Some(plan.try_acquire(&context.node).ok_or_else(deferred)?);
            }
            Ok(())
        })
        .unwrap_or(Ok(()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_concurrency::adaptive::{
        AdaptiveAdmission, FeedbackController, FeedbackObservation, ResourcePressure, permit_set::AdaptivePermitSetOwner,
    };
    use std::num::NonZeroU16;

    fn node() -> Arc<NodeAdmission> {
        let node = NodeAdmission::new(NonZeroU16::MIN);
        node.update(
            &mut FeedbackController::new(),
            Some(FeedbackObservation {
                generation: 1,
                max_concurrency: 1,
                pressure: ResourcePressure::Unknown,
                completed_units: None,
            }),
        );
        node
    }

    #[tokio::test]
    async fn receiver_reuses_one_receipt_across_phases_and_releases_only_native_subsets() {
        let node = node();
        let a = AdaptiveAdmission::new(NonZeroU16::MIN, 1);
        let b = AdaptiveAdmission::new(NonZeroU16::MIN, 1);
        let shared = AdaptivePermitSetOwner::new(NonZeroU16::MIN, vec![a.clone(), b.clone()]).expect("device catalog");
        let first = PhysicalDiskBinding::default();
        let second = PhysicalDiskBinding::default();
        first
            .set_test_shared_capacity(shared.clone(), vec![a.clone()])
            .expect("device A");
        second.set_test_shared_capacity(shared, vec![b.clone()]).expect("device B");

        let native = with_remote_background_io(Some(RemoteBackgroundIo::new(node.clone())), async {
            assert!(resources().is_none(), "no physical work before target selection");
            admit(&first).expect("first phase");
            let admitted = resources().expect("admitted resources");
            let native = first
                .plan()
                .expect("current plan")
                .capture_native(admitted.as_set().expect("resource set"))
                .expect("escaped native read");
            admit(&first).expect("second phase reuses capacity while the first reader lives");
            admit(&second).expect("additional selected device");
            assert_eq!([node.snapshot().active, a.snapshot().active, b.snapshot().active], [1, 1, 1]);
            record_progress();
            native
        })
        .await;
        assert_eq!([node.snapshot().active, a.snapshot().active, b.snapshot().active], [0, 1, 0]);
        assert_eq!([node.completed(), a.completed(), b.completed()], [Some(1), Some(1), Some(1)]);
        drop(native);
        assert_eq!(a.snapshot().active, 0);
    }

    #[tokio::test]
    async fn receiver_cancellation_returns_node_but_not_escaped_native_capacity() {
        let node = node();
        let gate = AdaptiveAdmission::new(NonZeroU16::MIN, 1);
        let binding = PhysicalDiskBinding::default();
        binding.set_test_capacity(vec![gate.clone()]).expect("device plan");
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(with_remote_background_io(Some(RemoteBackgroundIo::new(node.clone())), async move {
            admit(&binding).expect("receiver admission");
            let admitted = resources().expect("receiver resources");
            let native = binding
                .plan()
                .expect("plan")
                .capture_native(admitted.as_set().expect("set"))
                .expect("native subset");
            sender.send(native).expect("native owner receiver");
            record_progress();
            std::future::pending::<()>().await;
        }));
        let native = receiver.await.expect("native read ownership");
        task.abort();
        assert!(task.await.expect_err("cancel request").is_cancelled());
        assert_eq!([node.snapshot().active, gate.snapshot().active], [0, 1]);
        assert_eq!([node.completed(), gate.completed()], [Some(0), Some(0)]);
        drop(native);
        assert_eq!(gate.snapshot().active, 0);
    }

    #[tokio::test]
    async fn receiver_defers_stream_progress_without_retaining_node_capacity() {
        for (progressed, succeeds) in [(false, true), (true, false), (true, true)] {
            let node = node();
            let gate = AdaptiveAdmission::new(NonZeroU16::MIN, 1);
            let binding = PhysicalDiskBinding::default();
            binding.set_test_capacity(vec![gate.clone()]).expect("device plan");
            let completion = with_remote_background_io(Some(RemoteBackgroundIo::new(node.clone())), async {
                admit(&binding).expect("receiver admission");
                if progressed {
                    record_progress();
                }
                let completion = RemoteBackgroundIo::take_current_completion();
                assert!(RemoteBackgroundIo::take_current_completion().is_none(), "single progress owner");
                assert!(resources().is_none());
                completion
            })
            .await;
            assert_eq!([node.snapshot().active, gate.snapshot().active], [0, 0]);
            assert_eq!([node.completed(), gate.completed()], [Some(0), Some(0)]);
            assert_eq!(completion.is_some(), progressed);
            if succeeds && let Some(completion) = completion {
                completion.complete();
            }
            let expected = u64::from(progressed && succeeds);
            assert_eq!([node.completed(), gate.completed()], [Some(expected), Some(expected)]);
        }
        assert!(
            RemoteBackgroundIo::take_current_completion().is_none(),
            "foreground has no progress owner"
        );
    }

    #[tokio::test]
    async fn receiver_failed_completion_does_not_credit_successful_preparation() {
        let node = node();
        let gate = AdaptiveAdmission::new(NonZeroU16::MIN, 1);
        let binding = PhysicalDiskBinding::default();
        binding.set_test_capacity(vec![gate.clone()]).expect("device plan");
        with_remote_background_io(Some(RemoteBackgroundIo::new(node.clone())), async {
            admit(&binding).expect("receiver admission");
            record_progress();
            RemoteBackgroundIo::discard_current_progress();
        })
        .await;
        assert_eq!([node.snapshot().active, gate.snapshot().active], [0, 0]);
        assert_eq!([node.completed(), gate.completed()], [Some(0), Some(0)]);
    }

    #[tokio::test]
    async fn receiver_stream_transfer_moves_node_ownership_and_defers_unavailable_devices() {
        let node = node();
        let gate = AdaptiveAdmission::new(NonZeroU16::MIN, 0);
        let binding = PhysicalDiskBinding::default();
        binding.set_test_capacity(vec![gate.clone()]).expect("device plan");
        let context = with_remote_background_io(Some(RemoteBackgroundIo::new(node.clone())), async {
            assert_eq!(admit(&binding).expect_err("closed device").kind(), io::ErrorKind::WouldBlock);
            assert_eq!([node.snapshot().active, gate.snapshot().active], [0, 0]);
            gate.set_limit(1);
            admit(&binding).expect("opened device");
            let context = RemoteBackgroundIo::take_current().expect("stream context");
            assert!(resources().is_none(), "request cannot keep a second node owner");
            context
        })
        .await;
        assert_eq!([node.snapshot().active, gate.snapshot().active], [1, 1]);
        assert_eq!(node.completed(), Some(0));
        with_remote_background_io(Some(context), async {
            admit(&binding).expect("stream reuses the request's receipt");
            binding
                .set_test_capacity(vec![AdaptiveAdmission::new(NonZeroU16::MIN, 1)])
                .expect("replace physical device");
            assert_eq!(
                admit(&binding).expect_err("old receipt cannot cross topology").kind(),
                io::ErrorKind::WouldBlock
            );
        })
        .await;
        assert_eq!([node.snapshot().active, gate.snapshot().active], [0, 0]);
        assert_eq!(node.completed(), Some(0), "admission alone is not completed work");
    }
}
