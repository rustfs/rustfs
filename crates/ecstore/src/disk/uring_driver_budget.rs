// Copyright 2024 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use std::ffi::OsStr;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

/// Optional process budget for the dedicated driver threads (one per shard).
/// It does not account for io-wq workers, blocking tasks, or buffer allocation.
pub(super) enum DriverThreadBudget {
    Unlimited,
    Limited(Arc<Semaphore>),
}

#[derive(Debug)]
pub(super) struct InvalidBudget;

impl std::fmt::Display for InvalidBudget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("expected an unsigned decimal integer within Semaphore::MAX_PERMITS")
    }
}

impl std::error::Error for InvalidBudget {}

impl DriverThreadBudget {
    /// Parse a supplied value so tests do not mutate process-wide environment.
    /// Missing or zero is the legacy unlimited policy; malformed input fails.
    pub(super) fn from_env_value(value: Option<&OsStr>) -> Result<Self, InvalidBudget> {
        let Some(value) = value else { return Ok(Self::Unlimited) };
        let text = value.to_str().ok_or(InvalidBudget)?;
        if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(InvalidBudget);
        }
        let limit = text.parse::<usize>().map_err(|_| InvalidBudget)?;
        if limit > Semaphore::MAX_PERMITS {
            return Err(InvalidBudget);
        }
        Ok(if limit == 0 {
            Self::Unlimited
        } else {
            Self::Limited(Arc::new(Semaphore::new(limit)))
        })
    }

    /// Reserve the full configured shard count without waiting for a live
    /// driver to retire. Insufficient capacity selects the caller's std backend.
    pub(super) fn try_reserve(&self, shards: usize) -> Result<Option<OwnedSemaphorePermit>, TryAcquireError> {
        // Production shards are clamped to 1..=16 before this call. Retain a
        // checked conversion at the semaphore's u32 API boundary.
        let count = u32::try_from(shards).map_err(|_| TryAcquireError::NoPermits)?;
        if count == 0 {
            return Err(TryAcquireError::NoPermits);
        }
        match self {
            Self::Unlimited => Ok(None),
            Self::Limited(permits) => Arc::clone(permits).try_acquire_many_owned(count).map(Some),
        }
    }
}

/// Driver and its lifetime reservation must share the same Arc, including
/// transient references held by the stats exporter.
pub(super) struct BudgetedDriver<T> {
    // Field drop order is intentional: joining all driver threads finishes
    // before the reservation is returned. Do not move the permit above driver.
    driver: T,
    _threads: Option<OwnedSemaphorePermit>,
}

impl<T> BudgetedDriver<T> {
    pub(super) fn new(driver: T, threads: Option<OwnedSemaphorePermit>) -> Self {
        Self {
            driver,
            _threads: threads,
        }
    }
}

impl<T> std::ops::Deref for BudgetedDriver<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.driver
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Poll, Waker};
    use tokio::sync::oneshot;

    fn budget(limit: usize) -> DriverThreadBudget {
        DriverThreadBudget::from_env_value(Some(OsStr::new(&limit.to_string()))).expect("valid budget")
    }

    fn poll_once<T>(future: std::pin::Pin<&mut impl Future<Output = T>>) -> Poll<T> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }

    #[test]
    fn configuration_accepts_only_supported_decimal_limits() {
        for value in [None, Some(OsStr::new("0")), Some(OsStr::new("000"))] {
            assert!(matches!(DriverThreadBudget::from_env_value(value), Ok(DriverThreadBudget::Unlimited)));
        }
        for limit in [1, 16, Semaphore::MAX_PERMITS] {
            let DriverThreadBudget::Limited(permits) = budget(limit) else {
                panic!("nonzero budget must be limited")
            };
            assert_eq!(permits.available_permits(), limit);
        }
        for value in ["", " ", " 4", "4 ", "+4", "-1", "bad", "1.5", "184467440737095516160"] {
            assert!(
                DriverThreadBudget::from_env_value(Some(OsStr::new(value))).is_err(),
                "invalid config accepted: {value}"
            );
        }
        for limit in [Semaphore::MAX_PERMITS + 1, usize::MAX] {
            assert!(DriverThreadBudget::from_env_value(Some(OsStr::new(&limit.to_string()))).is_err());
        }
    }

    #[cfg(unix)]
    #[test]
    fn non_utf8_configuration_fails_closed() {
        use std::os::unix::ffi::OsStrExt;
        assert!(DriverThreadBudget::from_env_value(Some(OsStr::from_bytes(&[0xff]))).is_err());
    }

    #[test]
    fn weighted_reservations_reject_excess_without_partial_acquisition() {
        let budget = budget(5);
        let first = budget.try_reserve(3).expect("first driver fits");
        assert!(matches!(budget.try_reserve(3), Err(TryAcquireError::NoPermits)));
        let second = budget.try_reserve(2).expect("failed reserve must not consume capacity");
        assert!(matches!(budget.try_reserve(1), Err(TryAcquireError::NoPermits)));
        drop(first);
        let replacement = budget.try_reserve(3).expect("retirement returns exact weight");
        drop(second);
        drop(replacement);
        assert!(budget.try_reserve(5).is_ok());
    }

    #[test]
    fn unlimited_policy_preserves_legacy_and_invalid_shard_counts_do_not_panic() {
        let budget = DriverThreadBudget::Unlimited;
        assert!(budget.try_reserve(16).expect("legacy admission").is_none());
        assert!(budget.try_reserve(0).is_err());
        if usize::BITS > u32::BITS {
            assert!(budget.try_reserve(usize::MAX).is_err());
        }
    }

    #[test]
    fn stats_arc_keeps_budget_until_final_driver_drop() {
        let budget = budget(2);
        let driver = Arc::new(BudgetedDriver::new((), budget.try_reserve(2).expect("reserve driver")));
        let stats = Arc::downgrade(&driver);
        let exporter_reference = stats.upgrade().expect("stats borrows a live driver");
        drop(driver);
        assert!(budget.try_reserve(1).is_err(), "backend drop must not bypass the exporter Arc");
        drop(exporter_reference);
        assert!(stats.upgrade().is_none());
        assert!(budget.try_reserve(2).is_ok(), "a Weak alone must not hold the reservation");
    }

    #[tokio::test]
    async fn blocking_driver_drop_retains_budget_until_join_finishes() {
        struct JoiningDriver {
            started: Option<oneshot::Sender<()>>,
            finish: std::sync::mpsc::Receiver<()>,
        }
        impl Drop for JoiningDriver {
            fn drop(&mut self) {
                self.started
                    .take()
                    .expect("single driver drop")
                    .send(())
                    .expect("observe drop entry");
                self.finish
                    .recv_timeout(std::time::Duration::from_secs(10))
                    .expect("release simulated join");
            }
        }
        let budget = budget(2);
        let (started_tx, started_rx) = oneshot::channel();
        let (finish_tx, finish_rx) = std::sync::mpsc::channel();
        let driver = BudgetedDriver::new(
            JoiningDriver {
                started: Some(started_tx),
                finish: finish_rx,
            },
            budget.try_reserve(2).expect("reserve driver"),
        );
        let cleanup = tokio::task::spawn_blocking(move || drop(driver));
        started_rx.await.expect("driver drop started");
        let unavailable_during_join = budget.try_reserve(1).is_err();
        finish_tx.send(()).expect("finish driver join");
        cleanup.await.expect("cleanup completes");
        assert!(unavailable_during_join);
        assert!(budget.try_reserve(2).is_ok());
    }

    #[tokio::test]
    async fn failed_and_panicking_probes_return_their_thread_reservation() {
        let budget = budget(2);
        let threads = budget.try_reserve(2).expect("reserve failed probe");
        let failure = super::super::uring_probe::run(move || {
            let _threads = threads;
            Err::<(), _>(std::io::Error::from_raw_os_error(5))
        })
        .await
        .expect("probe work returns normally");
        assert_eq!(failure.expect_err("probe error preserved").raw_os_error(), Some(5));
        let threads = budget.try_reserve(2).expect("failed probe returned reservation");
        assert!(
            super::super::uring_probe::run(move || {
                let _threads = threads;
                panic!("injected probe panic");
            })
            .await
            .is_err()
        );
        assert!(budget.try_reserve(2).is_ok());
    }

    #[tokio::test]
    async fn started_probe_cancellation_retains_thread_reservation_until_completion() {
        let budget = Arc::new(budget(2));
        let threads = budget.try_reserve(2).expect("reserve initializing driver");
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (dropped_tx, dropped_rx) = oneshot::channel();
        struct Driver(Option<oneshot::Sender<()>>);
        impl Drop for Driver {
            fn drop(&mut self) {
                self.0.take().expect("single drop").send(()).expect("observe cleanup");
            }
        }
        let mut probe = Box::pin(super::super::uring_probe::run(move || {
            entered_tx.send(()).expect("probe entered");
            release_rx
                .recv_timeout(std::time::Duration::from_secs(10))
                .expect("finish probe");
            BudgetedDriver::new(Driver(Some(dropped_tx)), threads)
        }));
        assert!(poll_once(probe.as_mut()).is_pending());
        entered_rx.await.expect("probe started");
        drop(probe);
        let still_reserved = budget.try_reserve(1).is_err();
        release_tx.send(()).expect("complete detached probe");
        dropped_rx.await.expect("abandoned driver is cleaned up");
        // The Drop notification precedes the permit field's destructor. Wait on
        // the test semaphore only to observe final cleanup, never in production.
        let DriverThreadBudget::Limited(permits) = budget.as_ref() else { panic!("limited budget") };
        let returned = permits
            .clone()
            .acquire_many_owned(2)
            .await
            .expect("cleanup returns all thread slots");
        assert!(still_reserved);
        drop(returned);
    }

    #[tokio::test]
    async fn canceling_initialization_waiter_releases_reserved_driver_slots() {
        let budget = budget(2);
        let threads = budget.try_reserve(2).expect("reserve pending initialization");
        let initialization = Arc::new(Semaphore::new(0));
        let started = Arc::new(AtomicBool::new(false));
        let mark_started = started.clone();
        let mut probe = Box::pin(super::super::uring_probe::run_with_permits(initialization, move || {
            mark_started.store(true, Ordering::SeqCst);
            BudgetedDriver::new((), threads)
        }));
        assert!(poll_once(probe.as_mut()).is_pending());
        assert!(budget.try_reserve(1).is_err());
        drop(probe);
        assert!(!started.load(Ordering::SeqCst));
        assert!(budget.try_reserve(2).is_ok());
    }

    #[tokio::test]
    async fn completed_unconsumed_driver_keeps_slots_through_offloaded_cleanup() {
        struct WakeFinished(Arc<tokio::sync::Notify>);
        impl std::task::Wake for WakeFinished {
            fn wake(self: Arc<Self>) {
                self.0.notify_one();
            }
        }
        struct Driver {
            budget: Arc<DriverThreadBudget>,
            dropped: Option<oneshot::Sender<(bool, std::thread::ThreadId)>>,
        }
        impl Drop for Driver {
            fn drop(&mut self) {
                let reservation_retained = self.budget.try_reserve(1).is_err();
                self.dropped
                    .take()
                    .expect("single driver drop")
                    .send((reservation_retained, std::thread::current().id()))
                    .expect("observe cleanup");
            }
        }
        let budget = Arc::new(budget(2));
        let threads = budget.try_reserve(2).expect("reserve driver");
        let (drop_tx, drop_rx) = oneshot::channel();
        let driver = Driver {
            budget: budget.clone(),
            dropped: Some(drop_tx),
        };
        let (finish_tx, finish_rx) = std::sync::mpsc::channel();
        let initialization = Arc::new(Semaphore::new(1));
        let mut probe = Box::pin(super::super::uring_probe::run_with_permits(initialization, move || {
            finish_rx
                .recv_timeout(std::time::Duration::from_secs(10))
                .expect("allow result publication");
            BudgetedDriver::new(driver, threads)
        }));
        let finished = Arc::new(tokio::sync::Notify::new());
        let waker = Waker::from(Arc::new(WakeFinished(finished.clone())));
        assert!(probe.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
        finish_tx.send(()).expect("complete driver initialization");
        finished.notified().await;
        assert!(budget.try_reserve(1).is_err(), "ready but unconsumed driver remains charged");
        drop(probe);
        let (retained_during_drop, dropped_on) = drop_rx.await.expect("detached result cleanup");
        let DriverThreadBudget::Limited(permits) = budget.as_ref() else { panic!("limited budget") };
        let returned = permits
            .clone()
            .acquire_many_owned(2)
            .await
            .expect("completed cleanup returns thread slots");
        assert!(retained_during_drop);
        assert_ne!(dropped_on, std::thread::current().id());
        drop(returned);
    }
}
