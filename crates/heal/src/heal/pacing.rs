// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use crate::{Error, Result};
use rustfs_concurrency::{
    WorkloadAdmissionSnapshotProvider,
    workload::{ForegroundPressure, foreground_pressure},
};
use std::{sync::Arc, time::Duration};
use tokio::{sync::Mutex, time::Instant};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct PacingState {
    throttled: bool,
    low_since: Option<Instant>,
}

pub(crate) enum PacingDecision {
    Ready,
    Wait(Option<ForegroundPressure>),
}

/// Cooperative pacing for one admin execution, not a storage admission permit.
pub(crate) struct MainlinePacer {
    provider: Arc<dyn WorkloadAdmissionSnapshotProvider + Send + Sync>,
    read_high: usize,
    write_high: usize,
    pause: Duration,
    state: Mutex<PacingState>,
}

impl MainlinePacer {
    pub(crate) fn new(
        provider: Arc<dyn WorkloadAdmissionSnapshotProvider + Send + Sync>,
        read_high: usize,
        write_high: usize,
        pause: Duration,
    ) -> Option<Self> {
        if (read_high == 0 && write_high == 0) || pause.is_zero() {
            return None;
        }
        Some(Self {
            provider,
            read_high: read_high.min(100),
            write_high: write_high.min(100),
            pause: pause.min(Duration::from_secs(1)),
            state: Mutex::new(PacingState::default()),
        })
    }

    /// Fresh, nonblocking decision while the caller owns actual page capacity.
    /// A contended pacing latch is conservative, but never awaited here.
    pub(crate) fn admission_decision(&self) -> PacingDecision {
        let snapshot = self.provider.workload_admission_snapshot();
        let pressure = foreground_pressure(&snapshot, self.read_high, self.write_high);
        if pressure.is_none() && self.state.try_lock().is_ok_and(|state| !state.throttled) {
            PacingDecision::Ready
        } else {
            PacingDecision::Wait(pressure)
        }
    }

    /// Call only between storage operations, with no namespace lock or I/O
    /// permit held. The pacing-only mutex serializes starts within this task;
    /// each holder waits at most one pause so persistent pressure cannot stop
    /// all maintenance progress. Cancellation also interrupts queued waiters.
    pub(crate) async fn wait(&self, cancel: &CancellationToken) -> Result<()> {
        self.wait_after_admission(cancel, None).await.map(|_| ())
    }

    /// Returns whether this unit paid a bounded pause. That grant permits one
    /// unit even if pressure persists when page capacity becomes available.
    pub(crate) async fn wait_after_admission(
        &self,
        cancel: &CancellationToken,
        observed: Option<ForegroundPressure>,
    ) -> Result<bool> {
        let mut state = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(Error::TaskCancelled),
            state = self.state.lock() => state,
        };
        if observed.is_some() {
            state.throttled = true;
            state.low_since = None;
        }
        let snapshot = self.provider.workload_admission_snapshot();
        let pressure = foreground_pressure(&snapshot, self.read_high, self.write_high);
        if pressure.is_some() {
            state.throttled = true;
            state.low_since = None;
        } else if state.throttled {
            let low = |high: usize| if high == 0 { 0 } else { (high * 3 / 4).max(1) };
            if foreground_pressure(&snapshot, low(self.read_high), low(self.write_high)).is_none() {
                let now = Instant::now();
                let since = state.low_since.get_or_insert(now);
                if now.duration_since(*since) >= self.pause.saturating_mul(4) {
                    state.throttled = false;
                    state.low_since = None;
                }
            } else {
                state.low_since = None;
            }
        }
        if !state.throttled {
            return Ok(false);
        }
        metrics::counter!(
            "rustfs_heal_mainline_throttle_total",
            "source" => "admin",
            "result" => "delayed",
            "reason" => pressure.or(observed).map_or("recovery_window", |pressure| pressure.reason())
        )
        .increment(1);
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(Error::TaskCancelled),
            _ = tokio::time::sleep(self.pause) => Ok(true),
        }
    }
}

#[cfg(test)]
pub(crate) struct TestPressure {
    pub(crate) active: std::sync::atomic::AtomicUsize,
    pub(crate) sampled: tokio::sync::Notify,
    class: rustfs_concurrency::WorkloadClass,
}

#[cfg(test)]
impl TestPressure {
    pub(crate) fn new(class: rustfs_concurrency::WorkloadClass, active: usize) -> Self {
        Self {
            active: std::sync::atomic::AtomicUsize::new(active),
            sampled: tokio::sync::Notify::new(),
            class,
        }
    }
}

#[cfg(test)]
impl WorkloadAdmissionSnapshotProvider for TestPressure {
    fn workload_admission_snapshot(&self) -> rustfs_concurrency::WorkloadAdmissionRegistrySnapshot {
        let active = self.active.load(std::sync::atomic::Ordering::SeqCst);
        self.sampled.notify_one();
        rustfs_concurrency::WorkloadAdmissionRegistrySnapshot::new(vec![
            rustfs_concurrency::WorkloadAdmissionSnapshot::new(self.class, rustfs_concurrency::AdmissionState::Open).with_counts(
                Some(active),
                None,
                Some(100),
            ),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_concurrency::WorkloadClass;
    use std::sync::atomic::Ordering;

    #[tokio::test(start_paused = true)]
    async fn running_mainline_hysteresis_uses_configured_watermarks_and_stable_low_window() {
        for class in [WorkloadClass::ForegroundRead, WorkloadClass::ForegroundWrite] {
            let provider = Arc::new(TestPressure::new(class, 0));
            let pause = Duration::from_millis(250);
            let pacer = MainlinePacer::new(
                provider.clone(),
                if class == WorkloadClass::ForegroundRead { 40 } else { 0 },
                if class == WorkloadClass::ForegroundWrite { 40 } else { 0 },
                pause,
            )
            .expect("enabled pacer");
            let cancel = CancellationToken::new();
            let now = Instant::now();
            pacer.wait(&cancel).await.expect("quiet work");
            assert_eq!(Instant::now(), now);
            // Low watermark is 30 for the configured high watermark 40.
            for utilization in [40, 29, 35, 29, 29, 29, 29] {
                provider.active.store(utilization, Ordering::SeqCst);
                let before = Instant::now();
                pacer.wait(&cancel).await.expect("bounded maintenance progress");
                assert_eq!(Instant::now() - before, pause);
            }
            let before = Instant::now();
            pacer.wait(&cancel).await.expect("stable low pressure restores unpaced work");
            assert_eq!(Instant::now(), before);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn running_mainline_huge_pause_is_capped_and_disabled_classes_do_not_sleep() {
        let provider = Arc::new(TestPressure::new(WorkloadClass::ForegroundRead, 100));
        assert!(MainlinePacer::new(provider.clone(), 0, 0, Duration::from_secs(1)).is_none());
        assert!(MainlinePacer::new(provider.clone(), 80, 80, Duration::ZERO).is_none());
        let pacer = MainlinePacer::new(provider, 80, 80, Duration::from_secs(3600)).expect("pacer");
        let before = Instant::now();
        pacer.wait(&CancellationToken::new()).await.expect("hard-capped pause");
        assert_eq!(Instant::now() - before, Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn running_mainline_waiters_cancel_and_task_latches_are_isolated() {
        let provider = Arc::new(TestPressure::new(WorkloadClass::ForegroundRead, 100));
        let paced = Arc::new(MainlinePacer::new(provider.clone(), 80, 80, Duration::from_secs(1)).expect("pacer"));
        let cancel_first = CancellationToken::new();
        let first = tokio::spawn({
            let paced = paced.clone();
            let cancel = cancel_first.clone();
            async move { paced.wait(&cancel).await }
        });
        provider.sampled.notified().await;
        let cancel_second = CancellationToken::new();
        let second = tokio::spawn({
            let paced = paced.clone();
            let cancel = cancel_second.clone();
            async move { paced.wait(&cancel).await }
        });
        tokio::task::yield_now().await;
        cancel_second.cancel();
        assert!(matches!(second.await.expect("queued waiter"), Err(Error::TaskCancelled)));
        provider.active.store(0, Ordering::SeqCst);
        let other_task = MainlinePacer::new(provider.clone(), 80, 80, Duration::from_secs(1)).expect("independent task");
        let before = Instant::now();
        other_task
            .wait(&CancellationToken::new())
            .await
            .expect("another task has no inherited latch");
        assert_eq!(Instant::now(), before, "task/set pacing state must not be global");
        cancel_first.cancel();
        assert!(matches!(first.await.expect("sleeping waiter"), Err(Error::TaskCancelled)));
        tokio::time::timeout(Duration::from_secs(2), paced.wait(&CancellationToken::new()))
            .await
            .expect("pacing lock released")
            .expect("bounded work after cancellation");
    }
}
