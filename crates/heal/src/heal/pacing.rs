// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use crate::{Error, Result};
use rustfs_concurrency::{
    WorkloadAdmissionSnapshotProvider,
    workload::{ForegroundPressure, foreground_pressure},
};
use rustfs_heal_contracts::heal_channel::HealRequestSource;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};
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

/// S3 `SlowDown` ("Please reduce your request rate") is a rate signal, not a
/// per-object failure. One governor owns a heal page loop's own concurrency so
/// heal does not re-saturate the same admission at full speed after rejection.
#[derive(Debug)]
pub(crate) struct SlowDownRateGovernor {
    base_concurrency: usize,
    /// 0 = full base concurrency; each SlowDown increases the reduction.
    step: AtomicU32,
    next_ready: std::sync::Mutex<Option<Instant>>,
}

impl SlowDownRateGovernor {
    pub(crate) fn new(base_concurrency: usize) -> Self {
        Self {
            base_concurrency: base_concurrency.max(1),
            step: AtomicU32::new(0),
            next_ready: std::sync::Mutex::new(None),
        }
    }

    /// Concurrency after applying the current SlowDown reduction. Always ≥ 1
    /// so maintenance can still make progress under sustained rate pressure.
    pub(crate) fn effective_concurrency(&self) -> usize {
        let step = self.step.load(Ordering::Relaxed).min(8);
        (self.base_concurrency >> step).max(1)
    }

    /// Cut concurrency and arm the next start delay. Repeated observations
    /// compound the delay up to a bounded ceiling.
    pub(crate) fn observe_slow_down(&self) {
        let step = self.step.fetch_add(1, Ordering::Relaxed).min(8);
        let delay = Self::backoff(step);
        let ready = Instant::now() + delay;
        if let Ok(mut next_ready) = self.next_ready.lock()
            && next_ready.is_none_or(|current| current < ready)
        {
            *next_ready = Some(ready);
        }
    }

    /// Restore one concurrency step after a quiet window.
    pub(crate) fn observe_success(&self) {
        self.step
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |step| Some(step.saturating_sub(1)))
            .ok();
    }

    /// Wait until the SlowDown cool-down expires. Cancellation interrupts.
    pub(crate) async fn wait_ready(&self, cancel: &CancellationToken) -> Result<()> {
        let delay = self
            .next_ready
            .lock()
            .ok()
            .and_then(|next_ready| *next_ready)
            .map(|ready| ready.saturating_duration_since(Instant::now()));
        let Some(delay) = delay else {
            return Ok(());
        };
        if delay.is_zero() {
            return Ok(());
        }
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(Error::TaskCancelled),
            _ = tokio::time::sleep(delay) => Ok(()),
        }
    }

    fn backoff(step: u32) -> Duration {
        // 2s, 4s, 8s, … capped at 30s — S3 clients are told to reduce rate with
        // exponential backoff, and heal must do the same to itself.
        let exponent = step.min(5);
        Duration::from_secs(2_u64.saturating_pow(exponent)).min(Duration::from_secs(30))
    }
}

/// Cooperative pacing for one heal execution, not a storage admission permit.
pub(crate) struct MainlinePacer {
    provider: Arc<dyn WorkloadAdmissionSnapshotProvider + Send + Sync>,
    read_high: usize,
    write_high: usize,
    pause: Duration,
    source: HealRequestSource,
    state: Mutex<PacingState>,
}

impl MainlinePacer {
    pub(crate) fn new(
        provider: Arc<dyn WorkloadAdmissionSnapshotProvider + Send + Sync>,
        read_high: usize,
        write_high: usize,
        pause: Duration,
        source: HealRequestSource,
    ) -> Option<Self> {
        if (read_high == 0 && write_high == 0) || pause.is_zero() {
            return None;
        }
        Some(Self {
            provider,
            read_high: read_high.min(100),
            write_high: write_high.min(100),
            pause: pause.min(Duration::from_secs(1)),
            source,
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
            "source" => self.source.as_str(),
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
    async fn slow_down_governor_reduces_concurrency_and_backs_off() {
        let governor = SlowDownRateGovernor::new(8);
        assert_eq!(governor.effective_concurrency(), 8);
        governor.observe_slow_down();
        assert_eq!(governor.effective_concurrency(), 4);
        governor.observe_slow_down();
        assert_eq!(governor.effective_concurrency(), 2);
        // Floor is one so maintenance never stops entirely.
        for _ in 0..8 {
            governor.observe_slow_down();
        }
        assert_eq!(governor.effective_concurrency(), 1);

        let cancel = CancellationToken::new();
        let before = Instant::now();
        // The latest observation armed a bounded cool-down; wait_ready honors it.
        assert!(governor.wait_ready(&cancel).await.is_ok());
        assert!(Instant::now() >= before);

        // Successes restore concurrency one step at a time.
        for _ in 0..12 {
            governor.observe_success();
        }
        assert!(governor.effective_concurrency() > 1);
    }

    #[tokio::test(start_paused = true)]
    async fn slow_down_governor_wait_is_cancellable() {
        let governor = SlowDownRateGovernor::new(4);
        governor.observe_slow_down();
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert!(matches!(governor.wait_ready(&cancel).await, Err(Error::TaskCancelled)));
    }

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
                HealRequestSource::Admin,
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
        assert!(MainlinePacer::new(provider.clone(), 0, 0, Duration::from_secs(1), HealRequestSource::Admin).is_none());
        assert!(MainlinePacer::new(provider.clone(), 80, 80, Duration::ZERO, HealRequestSource::Admin).is_none());
        let pacer = MainlinePacer::new(provider, 80, 80, Duration::from_secs(3600), HealRequestSource::Admin).expect("pacer");
        let before = Instant::now();
        pacer.wait(&CancellationToken::new()).await.expect("hard-capped pause");
        assert_eq!(Instant::now() - before, Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn running_mainline_waiters_cancel_and_task_latches_are_isolated() {
        let provider = Arc::new(TestPressure::new(WorkloadClass::ForegroundRead, 100));
        let paced = Arc::new(
            MainlinePacer::new(provider.clone(), 80, 80, Duration::from_secs(1), HealRequestSource::Admin).expect("pacer"),
        );
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
        let other_task = MainlinePacer::new(provider.clone(), 80, 80, Duration::from_secs(1), HealRequestSource::Admin)
            .expect("independent task");
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
