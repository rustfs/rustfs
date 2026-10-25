// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

//! Opt-in node scheduling, physical-device feedback and native stream-buffer caps.

use rustfs_concurrency::adaptive::{FeedbackController, FeedbackObservation, NodeAdmission, ResourcePressure};
use rustfs_concurrency::foreground::ForegroundWindow;
use rustfs_concurrency::{AdmissionState, WorkloadAdmissionRegistrySnapshot, WorkloadAdmissionSnapshotProvider, WorkloadClass};
use std::num::NonZeroU16;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

use crate::cgroup_resources::live::ResourceSample;
use crate::storage_api::startup::background::{BackgroundMemoryBudget, ECStore, PhysicalDiskSample, PhysicalDiskSampler};

const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);
pub(crate) const MAXIMUM: NonZeroU16 = NonZeroU16::new(256).expect("fixed nonzero scheduling bound");

#[derive(Default)]
struct Policy {
    previous: Option<TimedResources>,
    reference: Option<ForegroundWindow>,
    last_window: Option<Instant>,
}

#[derive(Clone, Copy)]
struct TimedResources {
    sample: ResourceSample,
    began: Instant,
    ended: Instant,
}

impl Policy {
    fn observe(
        &mut self,
        now: Instant,
        sample: Option<TimedResources>,
        foreground: Option<ForegroundWindow>,
        registry: &WorkloadAdmissionRegistrySnapshot,
    ) -> Option<FeedbackObservation> {
        let Some(observation) = sample.filter(|sample| {
            now.checked_duration_since(sample.began)
                .is_some_and(|age| age <= SAMPLE_INTERVAL)
                && sample
                    .ended
                    .checked_duration_since(sample.began)
                    .is_some_and(|width| width <= Duration::from_millis(20))
                && sample.ended <= now
        }) else {
            *self = Self::default();
            return None;
        };
        let sample = observation.sample;
        let old = self.previous.replace(observation);
        if old.is_none_or(|old| {
            old.sample.identity != sample.identity
                || old.sample.cpu_millis != sample.cpu_millis
                || old.sample.memory_limit != sample.memory_limit
        }) {
            self.reference = None;
            self.last_window = None;
        }
        // These caps constrain async scheduling only. Memory pressure can stop
        // new work; it does not establish a bytes-per-operation reservation.
        let max_concurrency = if sample.cpu_millis == 0 || sample.memory_headroom <= sample.memory_limit / 100 {
            0
        } else {
            u32::try_from(sample.cpu_millis.div_ceil(1000).min(u64::from(MAXIMUM.get()))).ok()?
        };
        let resources = resource_pressure(observation, old);
        let foreground = self.foreground_pressure(now, foreground, registry);
        let pressure = match (resources, foreground) {
            (ResourcePressure::Busy, _) | (_, ResourcePressure::Busy) => ResourcePressure::Busy,
            (ResourcePressure::Stable, ResourcePressure::Stable) => ResourcePressure::Stable,
            _ => ResourcePressure::Unknown,
        };
        Some(FeedbackObservation {
            generation: sample.identity,
            max_concurrency,
            pressure,
            completed_units: None,
        })
    }

    fn foreground_pressure(
        &mut self,
        now: Instant,
        window: Option<ForegroundWindow>,
        registry: &WorkloadAdmissionRegistrySnapshot,
    ) -> ResourcePressure {
        let mut queues_known = true;
        for class in [WorkloadClass::ForegroundRead, WorkloadClass::ForegroundWrite] {
            match registry.get(class) {
                Some(entry) if entry.state == AdmissionState::Saturated || entry.queued.is_some_and(|n| n > 0) => {
                    return ResourcePressure::Busy;
                }
                Some(entry)
                    if matches!(entry.state, AdmissionState::Open | AdmissionState::Disabled) && entry.queued == Some(0) => {}
                _ => queues_known = false,
            }
        }
        let Some(window) = window.filter(|window| {
            window.valid
                && now
                    .checked_duration_since(window.end.latest)
                    .is_some_and(|age| age <= Duration::from_millis(1500))
                && window.end.latest.duration_since(window.end.earliest) <= Duration::from_millis(20)
                && window.start.latest.duration_since(window.start.earliest) <= Duration::from_millis(20)
                && window
                    .end
                    .earliest
                    .checked_duration_since(window.start.latest)
                    .is_some_and(|duration| (Duration::from_millis(500)..=Duration::from_millis(1500)).contains(&duration))
                && self.last_window.is_none_or(|last| window.end.latest > last)
        }) else {
            return ResourcePressure::Unknown;
        };
        self.last_window = Some(window.end.latest);
        if window
            .classes
            .iter()
            .flatten()
            .any(|class| class.failed > 0 || class.cancelled > 0)
        {
            return ResourcePressure::Busy;
        }
        if !queues_known || window.classes.iter().any(Option::is_none) || window.started.is_none() || window.in_flight.is_none() {
            return ResourcePressure::Unknown;
        }
        if window.started == Some(0) && window.in_flight == Some(0) && window.classes.iter().flatten().all(|c| c.finished == 0) {
            self.reference = None;
            return ResourcePressure::Stable;
        }
        // Never interpret low sample count or outstanding-only work as idle.
        if window
            .classes
            .iter()
            .flatten()
            .any(|c| c.finished > 0 && (c.finished < 100 || c.p99.and_then(|p| p.upper_nanos).is_none()))
            || window.classes.iter().flatten().all(|c| c.finished == 0)
        {
            return ResourcePressure::Unknown;
        }
        let Some(reference) = self.reference else {
            self.reference = Some(window);
            return ResourcePressure::Unknown;
        };
        let current_duration = window.end.latest.duration_since(window.start.earliest).as_nanos();
        let reference_duration = reference.end.latest.duration_since(reference.start.earliest).as_nanos();
        let mut comparable = true;
        let mut stable = true;
        for (current, prior) in window.classes.into_iter().zip(reference.classes) {
            let (Some(current), Some(prior)) = (current, prior) else {
                return ResourcePressure::Unknown;
            };
            let current_rate = u128::from(current.finished) * reference_duration;
            let prior_rate = u128::from(prior.finished) * current_duration;
            if current_rate * 100 < prior_rate * 80 || current_rate * 100 > prior_rate * 125 {
                comparable = false;
                continue;
            }
            if current.finished == 0 && prior.finished == 0 {
                continue;
            }
            let (Some(current), Some(prior)) = (current.p99, prior.p99) else {
                return ResourcePressure::Unknown;
            };
            let (Some(current_upper), Some(prior_upper)) = (current.upper_nanos, prior.upper_nanos) else {
                return ResourcePressure::Unknown;
            };
            if u128::from(current.lower_nanos) * 100 > u128::from(prior_upper) * 105 {
                return ResourcePressure::Busy;
            }
            stable &= u128::from(current_upper) * 100 <= u128::from(prior.lower_nanos) * 105;
        }
        if !comparable {
            // A changed workload needs a new conservative baseline. This is
            // an observed regime, never a claimed background-off baseline.
            self.reference = Some(window);
            ResourcePressure::Unknown
        } else if stable {
            ResourcePressure::Stable
        } else {
            ResourcePressure::Unknown
        }
    }
}

fn resource_pressure(current: TimedResources, old: Option<TimedResources>) -> ResourcePressure {
    let sample = current.sample;
    if sample.memory_headroom <= sample.memory_limit / 10 {
        return ResourcePressure::Busy;
    }
    let Some(previous) = old.filter(|old| old.sample.identity == sample.identity && old.sample.cpu_millis == sample.cpu_millis)
    else {
        return ResourcePressure::Unknown;
    };
    let Some(min_elapsed) = current.began.checked_duration_since(previous.ended) else {
        return ResourcePressure::Unknown;
    };
    let Some(max_elapsed) = current.ended.checked_duration_since(previous.began) else {
        return ResourcePressure::Unknown;
    };
    if min_elapsed < Duration::from_millis(500) || max_elapsed > Duration::from_secs(2) {
        return ResourcePressure::Unknown;
    }
    let min_elapsed = min_elapsed.as_micros();
    let max_elapsed = max_elapsed.as_micros();
    let previous = previous.sample;
    let mut stable = sample.memory_headroom >= sample.memory_limit / 5;
    let pairs = [
        (sample.cpu_usage_usec, previous.cpu_usage_usec, u128::from(sample.cpu_millis), 900, 700),
        (sample.cpu_throttled_usec, previous.cpu_throttled_usec, 1000, 10, 1),
        (sample.cpu_stall_usec, previous.cpu_stall_usec, 1000, 10, 1),
        (sample.memory_stall_usec, previous.memory_stall_usec, 1000, 10, 1),
        (sample.io_stall_usec, previous.io_stall_usec, 1000, 10, 1),
    ];
    let mut known = true;
    for (current, old, capacity, high, low) in pairs {
        let Some(delta) = current.zip(old).and_then(|(current, old)| current.checked_sub(old)) else {
            known = false;
            continue;
        };
        // Counter reads lie somewhere inside their original brackets. Use
        // the slowest possible rate for pressure, fastest for safe recovery.
        let usage = u128::from(delta) * 1_000_000;
        if usage > capacity * max_elapsed * high {
            return ResourcePressure::Busy;
        }
        stable &= usage <= capacity * min_elapsed * low;
    }
    if known && stable {
        ResourcePressure::Stable
    } else {
        ResourcePressure::Unknown
    }
}

// Match the resource policy's low-pressure headroom, and cap this staging
// storage at ten percent of the effective memory limit. Headroom is already net
// of retained allocations; using it directly conservatively includes other work.
fn memory_capacity(sample: ResourceSample) -> u64 {
    (sample.memory_limit / 10).min(sample.memory_headroom.saturating_sub(sample.memory_limit / 5))
}

struct Lifetime(Arc<NodeAdmission>, Arc<BackgroundMemoryBudget>);
impl Drop for Lifetime {
    fn drop(&mut self) {
        self.1.set_sampled_limit(0, Duration::ZERO);
        self.0.update(&mut FeedbackController::new(), None);
    }
}

pub(crate) fn start(
    owner: Arc<NodeAdmission>,
    memory: Arc<BackgroundMemoryBudget>,
    store: Arc<ECStore>,
    cancel: CancellationToken,
    provider: Arc<dyn WorkloadAdmissionSnapshotProvider + Send + Sync>,
) {
    let lifetime = Lifetime(owner, memory);
    tokio::spawn(async move {
        let lifetime = lifetime;
        let mut policy = Policy::default();
        let mut controller = FeedbackController::new();
        let mut physical = PhysicalDiskSampler::default();
        let mut tick = tokio::time::interval(SAMPLE_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! { biased; _ = cancel.cancelled() => break, _ = tick.tick() => {} }
            // One blocking sample at a time. A stuck reader cannot create an
            // unbounded worker queue; the previous admission lease expires.
            let disks = PhysicalDiskSample::collect(&store);
            let samples = tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                result = tokio::task::spawn_blocking(move || {
                    let began = Instant::now();
                    let sample = crate::cgroup_resources::live::sample().ok()
                        .map(|sample| TimedResources { sample, began, ended: Instant::now() });
                    (sample, disks.read())
                }) => result.ok(),
            };
            let foreground = crate::foreground_observation::latest_window();
            let registry = provider.workload_admission_snapshot();
            // Recheck age after every input read, including a delayed provider.
            let observation =
                policy.observe(Instant::now(), samples.as_ref().and_then(|(sample, _)| *sample), foreground, &registry);
            let memory_limit = observation
                .and_then(|_| samples.as_ref().and_then(|(sample, _)| *sample))
                .map_or(0, |sample| memory_capacity(sample.sample));
            lifetime.1.set_sampled_limit(memory_limit, Duration::from_secs(3));
            let decision = lifetime
                .0
                .update_with_probe(&mut controller, observation, physical.node_may_probe());
            if let Some((_, disks)) = samples {
                physical.publish(disks, observation.map_or(ResourcePressure::Unknown, |sample| sample.pressure));
            }
            if rustfs_io_metrics::metrics_enabled() {
                metrics::gauge!("rustfs_background_node_admission_limit").set(f64::from(decision.limit));
                metrics::gauge!("rustfs_background_node_admission_active").set(f64::from(lifetime.0.snapshot().active));
                let memory = lifetime.1.snapshot();
                metrics::gauge!("rustfs_background_buffer_memory_limit_bytes").set(memory.limit as f64);
                metrics::gauge!("rustfs_background_buffer_memory_retained_bytes").set(memory.used as f64);
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_concurrency::WorkloadAdmissionSnapshot;
    use rustfs_concurrency::foreground::{ClassCompletions, LatencyBounds, WindowBoundary};

    fn resources() -> ResourceSample {
        ResourceSample {
            cpu_millis: 4000,
            memory_limit: 1000,
            memory_headroom: 500,
            cpu_usage_usec: Some(0),
            cpu_throttled_usec: Some(0),
            cpu_stall_usec: Some(0),
            memory_stall_usec: Some(0),
            io_stall_usec: Some(0),
            identity: 1,
        }
    }

    #[test]
    fn buffer_capacity_respects_effective_limit_and_headroom() {
        let mut sample = resources();
        for (headroom, expected) in [(0, 0), (199, 0), (200, 0), (201, 1), (250, 50), (300, 100), (1000, 100)] {
            sample.memory_headroom = headroom;
            assert_eq!(memory_capacity(sample), expected, "headroom={headroom}");
        }
        sample.memory_limit = 500;
        assert_eq!(memory_capacity(sample), 50, "a hot shrink changes the absolute cap");
        sample.memory_limit = 0;
        assert_eq!(memory_capacity(sample), 0, "missing effective capacity cannot grant memory");
    }

    fn timed(at: Instant, sample: ResourceSample) -> TimedResources {
        TimedResources {
            sample,
            began: at,
            ended: at,
        }
    }

    fn queues(queued: Option<usize>) -> WorkloadAdmissionRegistrySnapshot {
        WorkloadAdmissionRegistrySnapshot::new(
            [WorkloadClass::ForegroundRead, WorkloadClass::ForegroundWrite]
                .into_iter()
                .map(|class| WorkloadAdmissionSnapshot::new(class, AdmissionState::Open).with_counts(Some(0), queued, Some(4)))
                .collect(),
        )
    }

    fn window(end: Instant, count: u64, latency: u64) -> ForegroundWindow {
        let mut classes = [Some(ClassCompletions::default()); 4];
        classes[0] = Some(ClassCompletions {
            finished: count,
            p99: (count >= 100).then_some(LatencyBounds {
                lower_nanos: latency,
                upper_nanos: Some(latency + 1),
            }),
            ..Default::default()
        });
        ForegroundWindow {
            start: WindowBoundary {
                earliest: end - SAMPLE_INTERVAL,
                latest: end - SAMPLE_INTERVAL,
            },
            end: WindowBoundary {
                earliest: end,
                latest: end,
            },
            started: Some(count),
            in_flight: Some(0),
            classes,
            valid: true,
        }
    }

    #[test]
    fn resource_caps_follow_fractional_cpu_and_memory_headroom() {
        let now = Instant::now();
        let mut policy = Policy::default();
        let mut sample = resources();
        for (cpu, memory, expected) in [
            (4000, 500, 4),
            (500, 500, 1),
            (0, 500, 0),
            (4000, 10, 0),
            (u64::MAX, 500, 256),
        ] {
            sample.cpu_millis = cpu;
            sample.memory_headroom = memory;
            assert_eq!(
                policy
                    .observe(now, Some(timed(now, sample)), None, &queues(Some(0)))
                    .expect("known caps")
                    .max_concurrency,
                expected
            );
        }
        assert!(policy.observe(now, None, None, &queues(Some(0))).is_none());
    }

    #[test]
    fn resource_deltas_reject_missing_reset_and_delayed_samples() {
        let at = Instant::now();
        let now = at + SAMPLE_INTERVAL;
        let sample = resources();
        assert_eq!(resource_pressure(timed(now, sample), Some(timed(at, sample))), ResourcePressure::Stable);
        let mut unknown = sample;
        unknown.io_stall_usec = None;
        assert_eq!(resource_pressure(timed(now, unknown), Some(timed(at, sample))), ResourcePressure::Unknown);
        let mut previous = sample;
        previous.cpu_usage_usec = Some(1);
        assert_eq!(
            resource_pressure(timed(now, sample), Some(timed(at, previous))),
            ResourcePressure::Unknown
        );
        previous.identity += 1;
        assert_eq!(
            resource_pressure(timed(now, sample), Some(timed(at, previous))),
            ResourcePressure::Unknown
        );
        assert_eq!(
            resource_pressure(timed(now + Duration::from_secs(2), sample), Some(timed(at, sample))),
            ResourcePressure::Unknown
        );
    }

    #[test]
    fn delayed_input_reads_and_wide_sampling_brackets_cannot_renew_admission() {
        let at = Instant::now();
        let sample = timed(at, resources());
        let mut policy = Policy::default();
        let later = at + SAMPLE_INTERVAL + Duration::from_nanos(1);
        assert!(
            policy
                .observe(later, Some(sample), Some(window(later, 0, 0)), &queues(Some(0)))
                .is_none(),
            "a slow registry read must not refresh an old resource lease"
        );
        assert!(policy.previous.is_none());
        let wide = TimedResources {
            ended: at + Duration::from_millis(21),
            ..sample
        };
        assert!(
            policy
                .observe(wide.ended, Some(wide), Some(window(wide.ended, 0, 0)), &queues(Some(0)))
                .is_none()
        );
        let future = timed(later, resources());
        assert!(policy.observe(at, Some(future), None, &queues(Some(0))).is_none());
    }

    #[test]
    fn resource_hysteresis_uses_actual_counter_read_brackets() {
        let at = Instant::now();
        let prior = timed(at, resources());
        for (stall, expected) in [
            (1000, ResourcePressure::Stable),
            (1001, ResourcePressure::Unknown),
            (10_000, ResourcePressure::Unknown),
            (10_001, ResourcePressure::Busy),
        ] {
            let mut current = timed(at + SAMPLE_INTERVAL, resources());
            current.sample.io_stall_usec = Some(stall);
            assert_eq!(resource_pressure(current, Some(prior)), expected);
        }
        let mut prior = prior;
        prior.ended += Duration::from_millis(20);
        let mut current = timed(at + SAMPLE_INTERVAL, resources());
        current.ended += Duration::from_millis(20);
        current.sample.io_stall_usec = Some(1000);
        assert_eq!(
            resource_pressure(current, Some(prior)),
            ResourcePressure::Unknown,
            "uncertainty around the low watermark cannot justify growth"
        );
        current.sample.io_stall_usec = Some(10_001);
        assert_eq!(
            resource_pressure(current, Some(prior)),
            ResourcePressure::Unknown,
            "the upper elapsed bound is not yet definitely busy"
        );
        current.sample.io_stall_usec = Some(10_201);
        assert_eq!(resource_pressure(current, Some(prior)), ResourcePressure::Busy);
    }

    #[test]
    fn pressure_from_any_resource_wins_over_missing_telemetry() {
        let at = Instant::now();
        let old = resources();
        for field in 0..5 {
            let mut sample = old;
            sample.cpu_usage_usec = None;
            match field {
                0 => sample.cpu_usage_usec = Some(3_600_001),
                1 => sample.cpu_throttled_usec = Some(10_001),
                2 => sample.cpu_stall_usec = Some(10_001),
                3 => sample.memory_stall_usec = Some(10_001),
                _ => sample.io_stall_usec = Some(10_001),
            }
            assert_eq!(
                resource_pressure(timed(at + SAMPLE_INTERVAL, sample), Some(timed(at, old))),
                ResourcePressure::Busy
            );
        }
    }

    #[test]
    fn foreground_requires_fresh_complete_distinct_windows_and_known_queues() {
        let now = Instant::now();
        let valid = window(now, 0, 0);
        let mut policy = Policy::default();
        assert_eq!(policy.foreground_pressure(now, Some(valid), &queues(None)), ResourcePressure::Unknown);
        assert_eq!(
            policy.foreground_pressure(now, Some(valid), &queues(Some(0))),
            ResourcePressure::Unknown,
            "duplicate cut"
        );
        let fresh = window(now + SAMPLE_INTERVAL, 0, 0);
        assert_eq!(
            policy.foreground_pressure(now + SAMPLE_INTERVAL, Some(fresh), &queues(Some(0))),
            ResourcePressure::Stable
        );
        for invalid in [
            None,
            Some(window(now - Duration::from_secs(4), 0, 0)),
            Some(window(now + Duration::from_secs(8), 0, 0)),
        ] {
            assert_eq!(
                Policy::default().foreground_pressure(now, invalid, &queues(Some(0))),
                ResourcePressure::Unknown
            );
        }
        let mut ambiguous = valid;
        ambiguous.end.earliest -= Duration::from_millis(21);
        assert_eq!(
            Policy::default().foreground_pressure(now, Some(ambiguous), &queues(Some(0))),
            ResourcePressure::Unknown
        );
    }

    #[test]
    fn disabled_admission_is_known_only_when_the_provider_reports_no_queue() {
        let now = Instant::now();
        for (queued, expected) in [
            (Some(0), ResourcePressure::Stable),
            (Some(1), ResourcePressure::Busy),
            (None, ResourcePressure::Unknown),
        ] {
            let registry = WorkloadAdmissionRegistrySnapshot::new(
                [WorkloadClass::ForegroundRead, WorkloadClass::ForegroundWrite]
                    .into_iter()
                    .map(|class| {
                        WorkloadAdmissionSnapshot::new(class, AdmissionState::Disabled).with_counts(Some(0), queued, Some(0))
                    })
                    .collect(),
            );
            assert_eq!(Policy::default().foreground_pressure(now, Some(window(now, 0, 0)), &registry), expected);
        }
    }

    #[tokio::test]
    async fn foreground_policy_uses_real_storage_admission_observations() {
        use crate::storage_api::workload::concurrency::ConcurrencyManager;
        let manager = ConcurrencyManager::with_put_admission_for_test(true, 1, Duration::from_secs(30));
        let now = Instant::now();
        let mut policy = Policy::default();
        assert_eq!(
            policy.foreground_pressure(now, Some(window(now, 0, 0)), &manager.workload_admission_registry_snapshot()),
            ResourcePressure::Stable,
            "actual storage providers must expose known empty queues"
        );
        let held = manager.admit_put_object(1024).await.unwrap();
        let mut waiting = Box::pin(manager.admit_put_object(1024));
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        assert_eq!(manager.put_object_admission_snapshot().queued, Some(1));
        let next = now + SAMPLE_INTERVAL;
        assert_eq!(
            policy.foreground_pressure(next, Some(window(next, 0, 0)), &manager.workload_admission_registry_snapshot()),
            ResourcePressure::Busy
        );
        drop(waiting);
        drop(held);
        let next = next + SAMPLE_INTERVAL;
        assert_eq!(
            policy.foreground_pressure(next, Some(window(next, 0, 0)), &manager.workload_admission_registry_snapshot()),
            ResourcePressure::Stable
        );
    }

    #[test]
    fn foreground_errors_queues_and_tail_regression_reduce_admission() {
        let now = Instant::now();
        let mut policy = Policy::default();
        assert_eq!(
            policy.foreground_pressure(now, Some(window(now, 1000, 1000)), &queues(Some(0))),
            ResourcePressure::Unknown
        );
        let next = now + SAMPLE_INTERVAL;
        assert_eq!(
            policy.foreground_pressure(next, Some(window(next, 1000, 1000)), &queues(Some(0))),
            ResourcePressure::Stable
        );
        let next = next + SAMPLE_INTERVAL;
        assert_eq!(
            policy.foreground_pressure(next, Some(window(next, 1000, 1100)), &queues(Some(0))),
            ResourcePressure::Busy
        );
        assert_eq!(policy.foreground_pressure(next, None, &queues(Some(1))), ResourcePressure::Busy);
        let next = next + SAMPLE_INTERVAL;
        let mut failed = window(next, 1, 0);
        failed.classes[0].as_mut().expect("read class").cancelled = 1;
        assert_eq!(policy.foreground_pressure(next, Some(failed), &queues(Some(0))), ResourcePressure::Busy);
    }

    #[test]
    fn foreground_low_counts_and_changed_workloads_do_not_justify_growth() {
        let now = Instant::now();
        let mut policy = Policy::default();
        assert_eq!(
            policy.foreground_pressure(now, Some(window(now, 50, 1000)), &queues(Some(0))),
            ResourcePressure::Unknown
        );
        let next = now + SAMPLE_INTERVAL;
        assert_eq!(
            policy.foreground_pressure(next, Some(window(next, 1000, 1000)), &queues(Some(0))),
            ResourcePressure::Unknown
        );
        let next = next + SAMPLE_INTERVAL;
        assert_eq!(
            policy.foreground_pressure(next, Some(window(next, 2000, 1000)), &queues(Some(0))),
            ResourcePressure::Unknown
        );
        let next = next + SAMPLE_INTERVAL;
        let mut pending = window(next, 0, 0);
        pending.in_flight = Some(1);
        assert_eq!(
            policy.foreground_pressure(next, Some(pending), &queues(Some(0))),
            ResourcePressure::Unknown
        );
    }

    #[test]
    fn observations_require_both_resource_and_foreground_evidence() {
        let now = Instant::now();
        let mut policy = Policy::default();
        assert_eq!(
            policy
                .observe(now, Some(timed(now, resources())), Some(window(now, 0, 0)), &queues(Some(0)))
                .expect("caps")
                .pressure,
            ResourcePressure::Unknown
        );
        let next = now + SAMPLE_INTERVAL;
        assert_eq!(
            policy
                .observe(next, Some(timed(next, resources())), Some(window(next, 0, 0)), &queues(Some(0)))
                .expect("caps")
                .pressure,
            ResourcePressure::Stable
        );
        let next = next + SAMPLE_INTERVAL;
        assert_eq!(
            policy
                .observe(next, Some(timed(next, resources())), None, &queues(Some(0)))
                .expect("caps")
                .pressure,
            ResourcePressure::Unknown
        );
        assert_eq!(policy.observe(next, None, None, &queues(Some(0))).map(|o| o.pressure), None);
        assert!(policy.previous.is_none() && policy.reference.is_none());
    }

    #[tokio::test]
    async fn sampler_lifetime_blocks_new_work_without_revoking_active_work() {
        let owner = NodeAdmission::new(MAXIMUM);
        owner.update(
            &mut FeedbackController::new(),
            Some(FeedbackObservation {
                generation: 1,
                max_concurrency: 1,
                pressure: ResourcePressure::Unknown,
                completed_units: None,
            }),
        );
        let active = owner.acquire().await;
        let memory = BackgroundMemoryBudget::new();
        memory.set_sampled_limit(1024, Duration::from_secs(3));
        drop(Lifetime(owner.clone(), memory.clone()));
        assert_eq!(memory.snapshot().limit, 0, "sampler shutdown closes new buffer admission");
        assert_eq!(owner.snapshot().limit, 0);
        assert_eq!(owner.snapshot().active, 1);
        assert!(owner.try_acquire().is_none());
        active.complete();
        assert_eq!(owner.completed(), Some(1));
        assert_eq!(owner.snapshot().active, 0);
    }
}
