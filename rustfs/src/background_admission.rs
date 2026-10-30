// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

//! Opt-in node scheduling feedback. This is not a physical-disk reservation or
//! a byte budget: native work and escaping buffers require their own owners.

use rustfs_concurrency::adaptive::{FeedbackController, FeedbackObservation, NodeAdmission, ResourcePressure};
use rustfs_concurrency::foreground::ForegroundWindow;
use rustfs_concurrency::{AdmissionState, WorkloadAdmissionRegistrySnapshot, WorkloadAdmissionSnapshotProvider, WorkloadClass};
use std::num::NonZeroU16;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

use crate::cgroup_resources::live::{CpuCoreObservation, CpuQuotaSample, ResourceSample};

const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);
pub(crate) const MAXIMUM: NonZeroU16 = NonZeroU16::new(256).expect("fixed nonzero scheduling bound");

#[derive(Default)]
struct Policy {
    previous: Option<TimedResources>,
    reference: Option<ForegroundWindow>,
    last_window: Option<Instant>,
}

#[derive(Clone)]
struct TimedResources {
    sample: ResourceSample,
    quota_ancestors: Vec<CpuQuotaSample>,
    cpu_cores: Option<CpuCoreObservation>,
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
        let old = self.previous.replace(observation.clone());
        if old.as_ref().is_none_or(|old| {
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
    let previous_sample = previous.sample;
    let mut stable = sample.memory_headroom >= sample.memory_limit / 5;
    let pairs = [
        (
            sample.cpu_usage_usec,
            previous_sample.cpu_usage_usec,
            u128::from(sample.cpu_millis),
            900,
            700,
        ),
        (sample.cpu_throttled_usec, previous_sample.cpu_throttled_usec, 1000, 10, 1),
        (sample.cpu_stall_usec, previous_sample.cpu_stall_usec, 1000, 10, 1),
        (sample.memory_stall_usec, previous_sample.memory_stall_usec, 1000, 10, 1),
        (sample.io_stall_usec, previous_sample.io_stall_usec, 1000, 10, 1),
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
    if current.quota_ancestors.len() != previous.quota_ancestors.len() {
        known = false;
    }
    for ancestor in &current.quota_ancestors {
        let Some(prior) = previous
            .quota_ancestors
            .iter()
            .find(|prior| prior.identity == ancestor.identity)
        else {
            known = false;
            continue;
        };
        if (ancestor.quota, ancestor.period, ancestor.cpu_millis) != (prior.quota, prior.period, prior.cpu_millis) {
            known = false;
            continue;
        }
        for (current, old, capacity, high, low) in [
            (ancestor.usage_usec, prior.usage_usec, u128::from(ancestor.cpu_millis), 900, 700),
            (ancestor.throttled_usec, prior.throttled_usec, 1000, 10, 1),
        ] {
            let Some(delta) = current.zip(old).and_then(|(current, old)| current.checked_sub(old)) else {
                known = false;
                continue;
            };
            let usage = u128::from(delta) * 1_000_000;
            if usage > capacity * max_elapsed * high {
                return ResourcePressure::Busy;
            }
            stable &= usage <= capacity * min_elapsed * low;
        }
    }
    match (&current.cpu_cores, &previous.cpu_cores) {
        (Some(current), Some(previous)) => match cpu_core_pressure(current, previous, min_elapsed, max_elapsed) {
            ResourcePressure::Busy => return ResourcePressure::Busy,
            ResourcePressure::Unknown => known = false,
            ResourcePressure::Stable => {}
        },
        _ => known = false,
    }
    if known && stable {
        ResourcePressure::Stable
    } else {
        ResourcePressure::Unknown
    }
}

fn cpu_core_pressure(
    current: &CpuCoreObservation,
    previous: &CpuCoreObservation,
    min_elapsed_us: u128,
    max_elapsed_us: u128,
) -> ResourcePressure {
    if current.ticks_per_second == 0
        || current.ticks_per_second != previous.ticks_per_second
        || current.cores.is_empty()
        || current.sysfs_identity != previous.sysfs_identity
        || current.cpu_root_identity != previous.cpu_root_identity
        || current.topology != previous.topology
        || current.threads.is_empty()
        || current.threads != previous.threads
        || current.topology.len() != current.cores.len()
        || previous.topology.len() != previous.cores.len()
        || current
            .cores
            .iter()
            .zip(&current.topology)
            .any(|(core, (id, _))| core.id != *id)
        || previous
            .cores
            .iter()
            .zip(&previous.topology)
            .any(|(core, (id, _))| core.id != *id)
    {
        return ResourcePressure::Unknown;
    }
    let ticks = u128::from(current.ticks_per_second);
    let min_ticks = min_elapsed_us * ticks / 1_000_000;
    let max_ticks = (max_elapsed_us * ticks).div_ceil(1_000_000);
    // With fewer ticks, rounding the independent fields cannot establish a
    // useful low-watermark bound for the whole sampling interval.
    if min_ticks < 16 {
        return ResourcePressure::Unknown;
    }
    let mut known = current.cores.len() == previous.cores.len();
    let mut core_busy = std::collections::BTreeMap::<u32, u128>::new();
    for ((core, prior), (_, topology)) in current.cores.iter().zip(&previous.cores).zip(&current.topology) {
        if core.id != prior.id {
            known = false;
            continue;
        }
        let mut total = 0_u128;
        let mut busy = 0_u128;
        let mut complete = true;
        for (index, (&now, &old)) in core.counters.iter().zip(&prior.counters).enumerate() {
            let Some(delta) = now.checked_sub(old) else {
                complete = false;
                break;
            };
            total += u128::from(delta);
            if index != 3 {
                // iowait is treated as busy; guest and guest_nice are absent
                // because Linux already includes them in user and nice.
                busy += u128::from(delta);
            }
        }
        if !complete || total < min_ticks.saturating_sub(8) || total > max_ticks + 8 {
            known = false;
            continue;
        }
        // Each exported field is rounded to USER_HZ independently at both
        // cuts; seven non-idle fields give at most seven ticks of uncertainty.
        if busy.saturating_sub(7) * 100 > max_ticks * 90 {
            return ResourcePressure::Busy;
        }
        let Some(group) = topology.siblings.first() else {
            known = false;
            continue;
        };
        *core_busy.entry(*group).or_default() += busy + 7;
    }
    // The sum of the logical threads' busy upper bounds bounds how long
    // any sibling could have occupied the shared physical core. Individual
    // idle fractions cannot safely be averaged across SMT siblings.
    if core_busy.values().any(|busy| busy * 100 > min_ticks * 70) {
        known = false;
    }
    if known {
        ResourcePressure::Stable
    } else {
        ResourcePressure::Unknown
    }
}

struct Lifetime(Arc<NodeAdmission>);
impl Drop for Lifetime {
    fn drop(&mut self) {
        self.0.update(&mut FeedbackController::new(), None);
    }
}

pub(crate) fn start(
    owner: Arc<NodeAdmission>,
    cancel: CancellationToken,
    provider: Arc<dyn WorkloadAdmissionSnapshotProvider + Send + Sync>,
) {
    let lifetime = Lifetime(owner);
    tokio::spawn(async move {
        let lifetime = lifetime;
        let mut policy = Policy::default();
        let mut controller = FeedbackController::new();
        let mut tick = tokio::time::interval(SAMPLE_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! { biased; _ = cancel.cancelled() => break, _ = tick.tick() => {} }
            // One blocking sample at a time. A stuck reader cannot create an
            // unbounded worker queue; the previous admission lease expires.
            let sample = tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                result = tokio::task::spawn_blocking(|| {
                    let began = Instant::now();
                    let (sample, quota_ancestors, cpu_cores) = crate::cgroup_resources::live::sample_for_admission()?;
                    Ok::<_, std::io::Error>(TimedResources { sample, quota_ancestors, cpu_cores, began, ended: Instant::now() })
                }) => result.ok().and_then(Result::ok),
            };
            let foreground = crate::foreground_observation::latest_window();
            let registry = provider.workload_admission_snapshot();
            // Recheck age after every input read, including a delayed provider.
            let observation = policy.observe(Instant::now(), sample, foreground, &registry);
            let decision = lifetime.0.update(&mut controller, observation);
            if rustfs_io_metrics::metrics_enabled() {
                metrics::gauge!("rustfs_background_node_admission_limit").set(f64::from(decision.limit));
                metrics::gauge!("rustfs_background_node_admission_active").set(f64::from(lifetime.0.snapshot().active));
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

    fn timed(at: Instant, sample: ResourceSample) -> TimedResources {
        static ORIGIN: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
        let origin = *ORIGIN.get_or_init(|| Instant::now() - Duration::from_secs(3600));
        let ticks = u64::try_from(at.duration_since(origin).as_millis() / 10).expect("fixture tick count");
        TimedResources {
            sample,
            quota_ancestors: Vec::new(),
            cpu_cores: Some(CpuCoreObservation {
                ticks_per_second: 100,
                cores: (0..4)
                    .map(|id| {
                        let mut counters = [0; 8];
                        counters[3] = ticks;
                        crate::cgroup_resources::live::CpuCoreSample { id, counters }
                    })
                    .collect(),
                sysfs_identity: (1, 1),
                cpu_root_identity: (1, 2),
                topology: (0_u32..4)
                    .map(|id| {
                        (
                            id,
                            crate::cgroup_resources::live::SiblingObservation {
                                cpu_identity: (1, u64::from(id) + 3),
                                topology_identity: (1, u64::from(id) + 7),
                                siblings: vec![id],
                            },
                        )
                    })
                    .collect(),
                threads: vec![crate::cgroup_resources::live::ThreadObservation {
                    id: 42,
                    identity: (2, 42),
                    allowed: vec![0, 1, 2, 3],
                }],
            }),
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
                .observe(later, Some(sample.clone()), Some(window(later, 0, 0)), &queues(Some(0)))
                .is_none(),
            "a slow registry read must not refresh an old resource lease"
        );
        assert!(policy.previous.is_none());
        let wide = TimedResources {
            ended: at + Duration::from_millis(21),
            ..sample
        };
        let wide_end = wide.ended;
        assert!(
            policy
                .observe(wide_end, Some(wide), Some(window(wide_end, 0, 0)), &queues(Some(0)))
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
            assert_eq!(resource_pressure(current, Some(prior.clone())), expected);
        }
        let mut prior = prior;
        prior.ended += Duration::from_millis(20);
        let mut current = timed(at + SAMPLE_INTERVAL, resources());
        current.ended += Duration::from_millis(20);
        current.sample.io_stall_usec = Some(1000);
        assert_eq!(
            resource_pressure(current.clone(), Some(prior.clone())),
            ResourcePressure::Unknown,
            "uncertainty around the low watermark cannot justify growth"
        );
        current.sample.io_stall_usec = Some(10_001);
        assert_eq!(
            resource_pressure(current.clone(), Some(prior.clone())),
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
    fn busy_shared_parent_is_not_hidden_by_an_idle_leaf() {
        let at = Instant::now();
        let mut prior = timed(at, resources());
        prior.quota_ancestors.push(CpuQuotaSample {
            identity: (1, 2),
            quota: 100_000,
            period: 100_000,
            cpu_millis: 1000,
            usage_usec: Some(1_000_000),
            throttled_usec: Some(0),
        });
        let mut current = timed(at + SAMPLE_INTERVAL, resources());
        current.quota_ancestors = prior.quota_ancestors.clone();
        current.quota_ancestors[0].usage_usec = Some(1_500_000);
        assert_eq!(resource_pressure(current.clone(), Some(prior.clone())), ResourcePressure::Stable);

        // The leaf itself stays idle while a sibling consumes the parent's quota.
        current.quota_ancestors[0].usage_usec = Some(2_000_000);
        assert_eq!(resource_pressure(current.clone(), Some(prior.clone())), ResourcePressure::Busy);
        current.quota_ancestors[0].usage_usec = Some(1_100_000);
        current.quota_ancestors[0].throttled_usec = Some(20_000);
        assert_eq!(resource_pressure(current.clone(), Some(prior.clone())), ResourcePressure::Busy);

        current.quota_ancestors[0].throttled_usec = Some(0);
        for usage in [None, Some(999_999)] {
            current.quota_ancestors[0].usage_usec = usage;
            assert_eq!(resource_pressure(current.clone(), Some(prior.clone())), ResourcePressure::Unknown);
        }
        current.quota_ancestors[0].usage_usec = Some(1_100_000);
        current.quota_ancestors[0].period = 200_000;
        assert_eq!(resource_pressure(current.clone(), Some(prior.clone())), ResourcePressure::Unknown);
        current.quota_ancestors[0].period = 100_000;
        current.quota_ancestors[0].identity = (1, 3);
        assert_eq!(resource_pressure(current.clone(), Some(prior.clone())), ResourcePressure::Unknown);
        current.quota_ancestors.clear();
        assert_eq!(resource_pressure(current.clone(), Some(prior.clone())), ResourcePressure::Unknown);
        current.sample.cpu_usage_usec = Some(3_600_001);
        assert_eq!(resource_pressure(current, Some(prior)), ResourcePressure::Busy);
    }

    #[test]
    fn busy_shared_cpu_is_not_hidden_by_an_idle_leaf_or_other_idle_cpus() {
        let at = Instant::now();
        let prior = timed(at, resources());
        let mut current = timed(at + SAMPLE_INTERVAL, resources());
        assert_eq!(resource_pressure(current.clone(), Some(prior.clone())), ResourcePressure::Stable);

        let core = &mut current.cpu_cores.as_mut().expect("CPU fixture").cores[1];
        core.counters[3] -= 100;
        core.counters[0] += 100;
        assert_eq!(
            resource_pressure(current.clone(), Some(prior.clone())),
            ResourcePressure::Busy,
            "one saturated shared CPU cannot be averaged away by other idle CPUs"
        );
        {
            let core = &mut current.cpu_cores.as_mut().expect("CPU fixture").cores[1];
            core.counters[0] -= 60;
            core.counters[3] += 60;
        }
        assert_eq!(resource_pressure(current.clone(), Some(prior.clone())), ResourcePressure::Stable);
        {
            let core = &mut current.cpu_cores.as_mut().expect("CPU fixture").cores[1];
            core.counters[0] += 30;
            core.counters[3] -= 30;
        }
        assert_eq!(
            resource_pressure(current.clone(), Some(prior.clone())),
            ResourcePressure::Unknown,
            "tick rounding around the low watermark cannot authorize growth"
        );

        let mut missing = timed(at + SAMPLE_INTERVAL, resources());
        missing.cpu_cores.as_mut().expect("CPU fixture").cores.pop();
        assert_eq!(resource_pressure(missing, Some(prior.clone())), ResourcePressure::Unknown);
        let mut missing = timed(at + SAMPLE_INTERVAL, resources());
        missing.cpu_cores = None;
        assert_eq!(resource_pressure(missing, Some(prior.clone())), ResourcePressure::Unknown);
        let mut hotplug = timed(at + SAMPLE_INTERVAL, resources());
        hotplug.cpu_cores.as_mut().expect("CPU fixture").cores[0].id = 9;
        assert_eq!(resource_pressure(hotplug, Some(prior.clone())), ResourcePressure::Unknown);
        let mut replaced_topology = timed(at + SAMPLE_INTERVAL, resources());
        replaced_topology.cpu_cores.as_mut().expect("CPU fixture").topology[0]
            .1
            .topology_identity = (1, 100);
        assert_eq!(
            resource_pressure(replaced_topology, Some(prior.clone())),
            ResourcePressure::Unknown,
            "unchanged CPU IDs and monotonic counters cannot hide a replaced topology"
        );
        let mut replaced_thread = timed(at + SAMPLE_INTERVAL, resources());
        replaced_thread.cpu_cores.as_mut().expect("CPU fixture").threads[0].identity = (2, 43);
        assert_eq!(resource_pressure(replaced_thread, Some(prior.clone())), ResourcePressure::Unknown);
        let mut reset = timed(at + SAMPLE_INTERVAL, resources());
        reset.cpu_cores.as_mut().expect("CPU fixture").cores[0].counters[3] = 0;
        assert_eq!(resource_pressure(reset, Some(prior.clone())), ResourcePressure::Unknown);
        let mut old_iowait = prior.clone();
        old_iowait.cpu_cores.as_mut().expect("CPU fixture").cores[0].counters[4] = 2;
        assert_eq!(
            resource_pressure(timed(at + SAMPLE_INTERVAL, resources()), Some(old_iowait)),
            ResourcePressure::Unknown,
            "iowait can decrease and must not be treated as idle"
        );
        let next = timed(at + SAMPLE_INTERVAL, resources());
        assert_eq!(
            cpu_core_pressure(
                next.cpu_cores.as_ref().expect("CPU fixture"),
                prior.cpu_cores.as_ref().expect("CPU fixture"),
                100_000,
                100_000,
            ),
            ResourcePressure::Unknown,
            "too few USER_HZ ticks cannot establish a stable window"
        );
    }

    #[test]
    fn busy_smt_sibling_outside_affinity_prevents_stable_admission() {
        let at = Instant::now();
        let mut prior = timed(at, resources());
        let mut current = timed(at + SAMPLE_INTERVAL, resources());
        for observation in [
            prior.cpu_cores.as_mut().expect("CPU fixture"),
            current.cpu_cores.as_mut().expect("CPU fixture"),
        ] {
            observation.cores.truncate(2);
            observation.topology.truncate(2);
            observation.threads[0].allowed = vec![0];
            for (_, topology) in &mut observation.topology {
                topology.siblings = vec![0, 1];
            }
        }
        let sibling = &mut current.cpu_cores.as_mut().expect("CPU fixture").cores[1];
        sibling.counters[3] -= 100;
        sibling.counters[0] += 100;
        assert_eq!(
            resource_pressure(current, Some(prior)),
            ResourcePressure::Busy,
            "the monitored SMT sibling cannot be omitted merely because it is outside the affinity mask"
        );
    }

    #[test]
    fn smt_siblings_cannot_each_claim_the_same_idle_time() {
        let at = Instant::now();
        let mut prior = timed(at, resources());
        let mut current = timed(at + SAMPLE_INTERVAL, resources());
        for observation in [
            prior.cpu_cores.as_mut().expect("CPU fixture"),
            current.cpu_cores.as_mut().expect("CPU fixture"),
        ] {
            observation.cores.truncate(2);
            observation.topology.truncate(2);
            observation.threads[0].allowed = vec![0];
            for (_, topology) in &mut observation.topology {
                topology.siblings = vec![0, 1];
            }
        }
        for core in &mut current.cpu_cores.as_mut().expect("CPU fixture").cores {
            core.counters[3] -= 20;
            core.counters[0] += 20;
        }
        assert_eq!(resource_pressure(current.clone(), Some(prior.clone())), ResourcePressure::Stable);
        for core in &mut current.cpu_cores.as_mut().expect("CPU fixture").cores {
            core.counters[3] -= 20;
            core.counters[0] += 20;
        }
        assert_eq!(
            resource_pressure(current, Some(prior)),
            ResourcePressure::Unknown,
            "two 40% siblings do not establish a low-pressure core window"
        );
    }

    #[test]
    fn swapped_thread_affinities_do_not_reuse_a_stable_resource_window() {
        let at = Instant::now();
        let mut prior = timed(at, resources());
        let mut current = timed(at + SAMPLE_INTERVAL, resources());
        let threads = &mut prior.cpu_cores.as_mut().expect("CPU fixture").threads;
        threads[0].allowed = vec![0, 1];
        threads.push(crate::cgroup_resources::live::ThreadObservation {
            id: 43,
            identity: (2, 43),
            allowed: vec![2, 3],
        });
        current.cpu_cores.as_mut().expect("CPU fixture").threads = threads.clone();
        assert_eq!(resource_pressure(current.clone(), Some(prior.clone())), ResourcePressure::Stable);
        {
            let later = &mut current.cpu_cores.as_mut().expect("CPU fixture").threads;
            later[0].allowed = vec![2, 3];
            later[1].allowed = vec![0, 1];
        }
        assert_eq!(
            resource_pressure(current, Some(prior)),
            ResourcePressure::Unknown,
            "equal CPU unions and minimum sizes do not prove the same thread affinities"
        );
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
        drop(Lifetime(owner.clone()));
        assert_eq!(owner.snapshot().limit, 0);
        assert_eq!(owner.snapshot().active, 1);
        assert!(owner.try_acquire().is_none());
        active.complete();
        assert_eq!(owner.completed(), Some(1));
        assert_eq!(owner.snapshot().active, 0);
    }
}
