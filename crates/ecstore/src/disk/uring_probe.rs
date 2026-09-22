// Copyright 2024 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use std::io;
use std::sync::{Arc, LazyLock};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

// This caps concurrent initialization, not live drivers, io-wq threads or bytes.
const MAX_CONCURRENT_PROBES: usize = 4;
static PROBE_PERMITS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(MAX_CONCURRENT_PROBES)));

/// Run only the root-independent driver probe here. Backend construction may
/// depend on a mount-lease fd owned by the async caller and must stay with it.
pub(super) async fn run<T: Send + 'static>(probe: impl FnOnce() -> T + Send + 'static) -> io::Result<T> {
    run_with_permits(Arc::clone(&PROBE_PERMITS), probe).await
}

async fn run_with_permits<T: Send + 'static>(
    permits: Arc<Semaphore>,
    probe: impl FnOnce() -> T + Send + 'static,
) -> io::Result<T> {
    // Acquisition precedes spawn: canceling a waiter cannot enqueue probe work.
    // The closure owns its permit even after its async JoinHandle is abandoned.
    let permit = permits.acquire_owned().await.map_err(io::Error::other)?;
    let mut result = tokio::task::spawn_blocking(move || ProbeResult {
        value: Some(probe()),
        permit: Some(permit),
    })
    .await
    .map_err(io::Error::other)?;
    // Consuming the result ends initialization admission. Abandoned results
    // retain the permit until their blocking cleanup finishes instead.
    result
        .value
        .take()
        .ok_or_else(|| io::Error::other("uring probe result was already consumed"))
}

struct ProbeResult<T: Send + 'static> {
    value: Option<T>,
    permit: Option<OwnedSemaphorePermit>,
}

impl<T: Send + 'static> Drop for ProbeResult<T> {
    fn drop(&mut self) {
        if let Some(value) = self.value.take() {
            let permit = self.permit.take();
            let cleanup = move || {
                // Driver destruction can join threads. In particular, dropping
                // a completed-but-unconsumed JoinHandle may run here on a Tokio
                // worker, so move that destruction back to blocking work.
                drop(value);
                drop(permit);
            };
            match tokio::runtime::Handle::try_current() {
                Ok(runtime) => {
                    runtime.spawn_blocking(cleanup);
                }
                Err(_) => cleanup(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::{Context, Poll, Wake, Waker};
    use tokio::sync::oneshot;

    fn poll_once<T>(future: std::pin::Pin<&mut impl Future<Output = T>>) -> Poll<T> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }

    #[tokio::test]
    async fn probe_runs_outside_async_caller_thread() {
        let caller = std::thread::current().id();
        let worker = run(std::thread::current).await.expect("probe should finish").id();
        assert_ne!(worker, caller);
    }

    #[tokio::test]
    async fn concurrent_probes_never_exceed_the_initialization_limit() {
        let permits = Arc::new(Semaphore::new(MAX_CONCURRENT_PROBES));
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let (entered_tx, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut releases = Vec::new();
        let mut probes = Vec::new();
        for _ in 0..MAX_CONCURRENT_PROBES {
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            releases.push(release_tx);
            let active = active.clone();
            let peak = peak.clone();
            let entered = entered_tx.clone();
            probes.push(tokio::spawn(run_with_permits(permits.clone(), move || {
                let running = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(running, Ordering::SeqCst);
                entered.send(()).expect("observe started probe");
                release_rx
                    .recv_timeout(std::time::Duration::from_secs(10))
                    .expect("release blocked probe");
                active.fetch_sub(1, Ordering::SeqCst);
            })));
        }
        for _ in 0..MAX_CONCURRENT_PROBES {
            entered_rx.recv().await.expect("each admitted probe should start");
        }
        let extra_started = Arc::new(AtomicBool::new(false));
        let mark_started = extra_started.clone();
        let mut extra = Box::pin(run_with_permits(permits.clone(), move || mark_started.store(true, Ordering::SeqCst)));
        let was_pending = poll_once(extra.as_mut()).is_pending();
        let started_early = extra_started.load(Ordering::SeqCst);
        for release in releases {
            release.send(()).expect("release active probes");
        }
        for probe in probes {
            probe.await.expect("probe task join").expect("probe execution");
        }
        extra.await.expect("queued probe runs after release");
        assert!(was_pending && !started_early);
        assert_eq!(peak.load(Ordering::SeqCst), MAX_CONCURRENT_PROBES);
        assert_eq!(permits.available_permits(), MAX_CONCURRENT_PROBES);
    }

    #[tokio::test]
    async fn canceling_before_admission_never_starts_probe() {
        let permits = Arc::new(Semaphore::new(1));
        let held = permits.clone().acquire_owned().await.expect("occupy admission");
        let started = Arc::new(AtomicBool::new(false));
        let mark_started = started.clone();
        let mut waiting = Box::pin(run_with_permits(permits.clone(), move || mark_started.store(true, Ordering::SeqCst)));
        assert!(poll_once(waiting.as_mut()).is_pending());
        drop(waiting);
        drop(held);
        run_with_permits(permits.clone(), || ()).await.expect("next probe should run");
        assert!(!started.load(Ordering::SeqCst));
        assert_eq!(permits.available_permits(), 1);
    }

    #[tokio::test]
    async fn canceling_started_probe_holds_capacity_until_worker_finishes() {
        let permits = Arc::new(Semaphore::new(1));
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let mut first = Box::pin(run_with_permits(permits.clone(), move || {
            entered_tx.send(()).expect("notify probe entry");
            release_rx
                .recv_timeout(std::time::Duration::from_secs(10))
                .expect("release running probe");
        }));
        assert!(poll_once(first.as_mut()).is_pending());
        entered_rx.await.expect("probe started");
        drop(first);
        let held_after_cancel = permits.available_permits();
        let mut second = Box::pin(run_with_permits(permits.clone(), || ()));
        let second_pending = poll_once(second.as_mut()).is_pending();
        release_tx.send(()).expect("finish detached probe");
        second.await.expect("capacity returns after real completion");
        assert_eq!(held_after_cancel, 0);
        assert!(second_pending);
        assert_eq!(permits.available_permits(), 1);
    }

    #[tokio::test]
    async fn ordinary_probe_errors_remain_unwrapped_and_release_capacity() {
        let permits = Arc::new(Semaphore::new(1));
        // Model both restriction and unexpected probe results. The offload layer
        // must pass them through; only its own Acquire/Join errors are wrapped.
        for errno in [1, 5] {
            let result = run_with_permits(permits.clone(), move || Err::<(), _>(io::Error::from_raw_os_error(errno)))
                .await
                .expect("blocking work itself should complete");
            assert_eq!(result.expect_err("probe returned an error").raw_os_error(), Some(errno));
            assert_eq!(permits.available_permits(), 1);
        }
    }

    #[tokio::test]
    async fn panic_and_closed_admission_are_not_os_restriction_errors() {
        let permits = Arc::new(Semaphore::new(1));
        let panic_error = run_with_permits(permits.clone(), || panic!("injected probe panic"))
            .await
            .expect_err("worker panic must be returned");
        assert!(
            panic_error
                .get_ref()
                .and_then(|error| error.downcast_ref::<tokio::task::JoinError>())
                .is_some()
        );
        assert_eq!(panic_error.raw_os_error(), None);
        assert_eq!(permits.available_permits(), 1);
        permits.close();
        let closed_error = run_with_permits(permits, || panic!("closed admission must not start"))
            .await
            .expect_err("closed admission must return an error");
        assert_eq!(closed_error.raw_os_error(), None);
        assert!(
            closed_error
                .get_ref()
                .and_then(|error| error.downcast_ref::<tokio::sync::AcquireError>())
                .is_some()
        );
    }

    #[tokio::test]
    async fn completed_unconsumed_result_drops_outside_async_caller_with_capacity_held() {
        struct WakeFinished(Arc<tokio::sync::Notify>);
        impl Wake for WakeFinished {
            fn wake(self: Arc<Self>) {
                self.0.notify_one();
            }
        }
        struct BlockingDrop {
            entered: Option<oneshot::Sender<std::thread::ThreadId>>,
            release: std::sync::mpsc::Receiver<()>,
        }
        impl Drop for BlockingDrop {
            fn drop(&mut self) {
                self.entered
                    .take()
                    .expect("single drop")
                    .send(std::thread::current().id())
                    .expect("observe cleanup");
                self.release
                    .recv_timeout(std::time::Duration::from_secs(10))
                    .expect("release cleanup");
            }
        }
        let permits = Arc::new(Semaphore::new(1));
        let (drop_tx, drop_rx) = oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (produce_tx, produce_rx) = std::sync::mpsc::channel();
        let result = BlockingDrop {
            entered: Some(drop_tx),
            release: release_rx,
        };
        let mut probe = Box::pin(run_with_permits(permits.clone(), move || {
            produce_rx
                .recv_timeout(std::time::Duration::from_secs(10))
                .expect("allow probe completion after waiter registration");
            result
        }));
        let finished = Arc::new(tokio::sync::Notify::new());
        let waker = Waker::from(Arc::new(WakeFinished(finished.clone())));
        assert!(probe.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
        produce_tx.send(()).expect("complete probe");
        // JoinHandle wakes only after its result is ready. Do not poll the
        // outer future again: cancel precisely before it consumes that result.
        finished.notified().await;
        drop(probe);
        let dropped_on = drop_rx.await.expect("abandoned result cleanup should run");
        let capacity_during_drop = permits.available_permits();
        release_tx.send(()).expect("allow cleanup to finish");
        let released = permits.clone().acquire_owned().await.expect("cleanup returns permit");
        assert_ne!(dropped_on, std::thread::current().id());
        assert_eq!(capacity_during_drop, 0);
        drop(released);
    }
}
