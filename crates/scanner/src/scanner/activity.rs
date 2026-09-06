// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
/// Cycle wake/backoff policy and scanner activity observation (probing, generations, topology digest).
use super::*;
use crate::storage_api::ScannerStorage;
use crate::storage_api::scan::SCANNER_ACTIVITY_V6_PROTOCOL_VERSION;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ScannerCycleWakeReason {
    Timer,
    DirtyUsage,
    MovementGeneration,
    ClusterActivity,
    ClusterMaintenance,
    ClusterActivityUnavailable,
    RuntimeConfig,
    MaintenanceConfig,
    LeaderLockLost,
    Cancelled,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScannerCycleOutcome {
    Completed,
    CompletedWithPendingMaintenance,
    Partial,
    Superseded,
    Deferred(ScannerCycleDeferReason),
    Failed,
}

pub(crate) fn scanner_cycle_outcome_with_pending_maintenance(
    outcome: ScannerCycleOutcome,
    pending_maintenance_work: bool,
) -> ScannerCycleOutcome {
    if outcome == ScannerCycleOutcome::Completed && pending_maintenance_work {
        ScannerCycleOutcome::CompletedWithPendingMaintenance
    } else {
        outcome
    }
}

pub(super) async fn remote_dirty_usage_acknowledgement_pending<F, E>(
    cycle: u64,
    acknowledgement_count: usize,
    acknowledgement: F,
) -> bool
where
    F: Future<Output = Result<bool, E>>,
    E: std::fmt::Display,
{
    match acknowledgement.await {
        Ok(dirty_usage_pending) => dirty_usage_pending,
        Err(err) => {
            warn!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                cycle,
                acknowledgement_count,
                error = %err,
                state = "remote_dirty_usage_acknowledgement_pending",
                "Scanner cycle left remote dirty usage acknowledgements pending"
            );
            true
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ScannerCleanIdleBackoff {
    pub(super) interval_multiplier: u32,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct ScannerRetryBackoff {
    pub(super) consecutive_cycles: u32,
}

impl ScannerRetryBackoff {
    pub(super) fn record_retryable_cycle(&mut self, retryable: bool) {
        if retryable {
            self.consecutive_cycles = self.consecutive_cycles.saturating_add(1);
        } else {
            self.consecutive_cycles = 0;
        }
    }

    pub(super) fn retry_interval(self, configured_interval: Duration) -> Option<Duration> {
        let exponent = self.consecutive_cycles.checked_sub(1)?.min(31);
        let multiplier = 1u32.checked_shl(exponent).unwrap_or(u32::MAX);
        let base_interval = configured_interval
            .max(Duration::from_secs(1))
            .min(SCANNER_RETRY_BASE_INTERVAL);
        let cap = SCANNER_RETRY_MAX_INTERVAL.max(configured_interval.max(Duration::from_secs(1)));
        Some(base_interval.saturating_mul(multiplier).min(cap))
    }
}

pub(super) fn scanner_superseded_retry_interval(
    backoff: ScannerRetryBackoff,
    runtime_config: &ScannerRuntimeConfig,
) -> Option<Duration> {
    let retry_interval = backoff.retry_interval(runtime_config.cycle_interval)?;
    if runtime_config.cycle_interval_source == ScannerRuntimeConfigSource::Default {
        return Some(retry_interval);
    }

    // An explicit cycle is an operator-selected duty-cycle floor. A
    // superseded snapshot keeps its dirty work pending, but retrying that work
    // sooner than the configured cadence would turn continuous writes into a
    // repeated full-walk loop despite the override.
    Some(retry_interval.max(runtime_config.cycle_interval))
}

const SCANNER_PUBLICATION_PROOF_RETRY_MAX_INTERVAL: Duration = Duration::from_secs(30);

pub(crate) fn scanner_publication_proof_retry_delay(consecutive_failures: u32) -> Duration {
    let exponent = consecutive_failures.saturating_sub(1).min(31);
    let multiplier = 1u32.checked_shl(exponent).unwrap_or(u32::MAX);
    SCANNER_RETRY_BASE_INTERVAL
        .saturating_mul(multiplier)
        .min(SCANNER_PUBLICATION_PROOF_RETRY_MAX_INTERVAL)
}

pub(crate) fn scanner_publication_activity_error_is_retryable(error: &str) -> bool {
    crate::storage_api::scanner_peer_transport_error_message_is_retryable(error)
}

pub(crate) fn scanner_publication_lease_error_is_retryable(error: &str) -> bool {
    scanner_publication_activity_error_is_retryable(error)
        || error.ends_with("scanner publication lease capacity is exhausted")
        || error.ends_with("scanner publication lease response arrived after its safety window")
}

pub(crate) enum ScannerPublicationProofWait<T, E> {
    Ready(T),
    Rejected(E),
    Cancelled,
}

pub(crate) async fn await_scanner_publication_proof<T, E, F, Fut, Retryable>(
    ctx: &CancellationToken,
    cycle: u64,
    stage: &'static str,
    mut proof: F,
    retryable: Retryable,
) -> ScannerPublicationProofWait<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
    Retryable: Fn(&E) -> bool,
{
    let started_at = Instant::now();
    let mut consecutive_failures = 0u32;

    loop {
        if ctx.is_cancelled() {
            return ScannerPublicationProofWait::Cancelled;
        }

        match proof().await {
            Ok(value) => {
                if consecutive_failures > 0 {
                    info!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_CYCLE_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        state = "publication_proof_recovered",
                        cycle,
                        stage,
                        attempts = consecutive_failures.saturating_add(1),
                        pending_duration = ?started_at.elapsed(),
                        "Scanner publication proof recovered"
                    );
                }
                return ScannerPublicationProofWait::Ready(value);
            }
            Err(err) if !retryable(&err) => {
                warn!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_CYCLE_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    state = "publication_proof_rejected",
                    cycle,
                    stage,
                    error = %err,
                    "Scanner publication proof failed with a non-retryable cluster state"
                );
                return ScannerPublicationProofWait::Rejected(err);
            }
            Err(err) => {
                consecutive_failures = consecutive_failures.saturating_add(1);
                let retry_delay = scanner_publication_proof_retry_delay(consecutive_failures);
                if consecutive_failures == 1 || consecutive_failures.is_multiple_of(20) {
                    warn!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_CYCLE_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        state = "publication_proof_pending",
                        cycle,
                        stage,
                        attempt = consecutive_failures,
                        retry_delay = ?retry_delay,
                        error = %err,
                        "Scanner retained a completed scan while publication proof is unavailable"
                    );
                } else {
                    debug!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_CYCLE_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        state = "publication_proof_retry",
                        cycle,
                        stage,
                        attempt = consecutive_failures,
                        retry_delay = ?retry_delay,
                        error = %err,
                        "Scanner publication activity proof retry scheduled"
                    );
                }

                tokio::select! {
                    _ = ctx.cancelled() => return ScannerPublicationProofWait::Cancelled,
                    _ = tokio::time::sleep(retry_delay) => {}
                }
            }
        }
    }
}

#[cfg(test)]
pub(crate) async fn await_scanner_publication_activity<F, Fut>(
    ctx: &CancellationToken,
    cycle: u64,
    stage: &'static str,
    probe: F,
) -> Result<ScannerActivitySnapshot, String>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<ScannerActivitySnapshot, String>>,
{
    match await_scanner_publication_proof(ctx, cycle, stage, probe, |err: &String| {
        scanner_publication_activity_error_is_retryable(err)
    })
    .await
    {
        ScannerPublicationProofWait::Ready(snapshot) => Ok(snapshot),
        ScannerPublicationProofWait::Rejected(err) => Err(err),
        ScannerPublicationProofWait::Cancelled => Err(format!("scanner publication activity proof was cancelled during {stage}")),
    }
}

impl Default for ScannerCleanIdleBackoff {
    fn default() -> Self {
        Self { interval_multiplier: 1 }
    }
}

impl ScannerCleanIdleBackoff {
    pub(super) fn reset(&mut self) {
        self.interval_multiplier = 1;
    }

    pub(super) fn effective_interval(self, base_interval: Duration, max_interval: Duration, enabled: bool) -> Duration {
        let base_interval = base_interval.max(Duration::from_secs(1));
        if !enabled {
            return base_interval;
        }

        let max_interval = max_interval.max(base_interval);
        base_interval.saturating_mul(self.interval_multiplier).min(max_interval)
    }

    pub(super) fn record_cycle(
        &mut self,
        base_interval: Duration,
        max_interval: Duration,
        enabled: bool,
        wake_reason: ScannerCycleWakeReason,
        outcome: ScannerCycleOutcome,
        dirty_work_observed: bool,
    ) {
        if !enabled
            || wake_reason != ScannerCycleWakeReason::Timer
            || outcome != ScannerCycleOutcome::Completed
            || dirty_work_observed
        {
            self.reset();
            return;
        }

        let max_interval = max_interval.max(base_interval.max(Duration::from_secs(1)));
        if self.effective_interval(base_interval, max_interval, true) < max_interval {
            self.interval_multiplier = self.interval_multiplier.saturating_mul(CLEAN_IDLE_BACKOFF_FACTOR);
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct ScannerMaintenanceInspectionRetry {
    pub(super) consecutive_failures: u32,
    pub(super) retry_at: Option<Instant>,
}

impl ScannerMaintenanceInspectionRetry {
    pub(super) fn from_features(features: ScannerMaintenanceFeatures, now: Instant) -> Self {
        let mut retry = Self::default();
        retry.record_inspection(features, now);
        retry
    }

    pub(super) fn reset(&mut self) {
        self.consecutive_failures = 0;
        self.retry_at = None;
    }

    pub(super) fn retry_interval(self) -> Option<Duration> {
        if self.consecutive_failures == 0 {
            return None;
        }

        let exponent = self.consecutive_failures.saturating_sub(1).min(31);
        let multiplier = 1u32.checked_shl(exponent).unwrap_or(u32::MAX);
        Some(
            MAINTENANCE_FEATURE_INSPECTION_RETRY_BASE_INTERVAL
                .saturating_mul(multiplier)
                .min(MAINTENANCE_FEATURE_INSPECTION_RETRY_MAX_INTERVAL),
        )
    }

    pub(super) fn record_inspection(&mut self, features: ScannerMaintenanceFeatures, now: Instant) {
        if !features.inspection_failed {
            self.reset();
            return;
        }

        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.retry_at = self.retry_interval().map(|interval| now + interval);
    }

    pub(super) fn retry_due(
        self,
        features: ScannerMaintenanceFeatures,
        wake_reason: ScannerCycleWakeReason,
        now: Instant,
    ) -> bool {
        features.inspection_failed
            && wake_reason == ScannerCycleWakeReason::Timer
            && self.retry_at.is_some_and(|retry_at| now >= retry_at)
    }
}

pub(super) fn scanner_cycle_observed_dirty_work(
    pending_before_wait: bool,
    generation_before_wait: u64,
    generation_after_cycle: u64,
) -> bool {
    pending_before_wait || generation_before_wait != generation_after_cycle
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ScannerCycleWaitPlan {
    pub(super) effective_interval: Duration,
    pub(super) clean_idle_max_interval: Duration,
    pub(super) delay: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ScannerCycleObservedGenerations {
    pub(super) dirty_usage: Option<u64>,
    pub(super) runtime_config: u64,
    pub(super) maintenance: u64,
    pub(super) defer_cluster_activity: bool,
}

impl ScannerCycleObservedGenerations {
    pub(super) fn for_wait(
        runtime_config: &ScannerRuntimeConfig,
        convergence_retry_interval: Option<Duration>,
        dirty_usage_generation_seen: u64,
        runtime_config_generation: u64,
        maintenance_generation: u64,
    ) -> Self {
        Self {
            // An explicit cycle override is a duty-cycle policy; dirty usage
            // wakes stay on the default adaptive path so the interval holds.
            dirty_usage: (convergence_retry_interval.is_none()
                && runtime_config.cycle_interval_source == ScannerRuntimeConfigSource::Default)
                .then_some(dirty_usage_generation_seen),
            runtime_config: runtime_config_generation,
            maintenance: maintenance_generation,
            defer_cluster_activity: convergence_retry_interval.is_some(),
        }
    }
}

/// Movement state observed while a scanner waits for the next cycle.
///
/// Keeping the movement inputs together makes it harder for callers to pair a
/// generation with the wrong notification or lock predicate.
pub(super) struct ScannerMovementWaitContext<G, F> {
    pub(super) movement_generation_seen: Option<u64>,
    pub(super) movement_changed: Arc<Notify>,
    pub(super) current_movement_generation: G,
    pub(super) is_lock_lost: F,
}

pub(super) const LOCAL_SCANNER_ACTIVITY_NODE: &str = "<local>";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ScannerNodeActivity {
    pub(super) instance_id: String,
    pub(super) namespace_generation: u64,
    pub(super) maintenance_generation: u64,
    pub(super) protocol_version: u32,
    pub(super) topology_digest: [u8; 32],
    pub(super) data_movement_active: bool,
    pub(super) dirty_usage_generation: u64,
    pub(super) dirty_usage_pending: bool,
    pub(super) movement_generation: u64,
    pub(super) publication_blocked: bool,
}

pub(crate) type ScannerActivitySnapshot = BTreeMap<String, ScannerNodeActivity>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ScannerDirtyUsageAcknowledgement {
    pub(crate) host: String,
    pub(crate) instance_id: String,
    pub(crate) kind: ScannerDirtyUsageAcknowledgementKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ScannerDirtyUsageAcknowledgementKind {
    Generation(u64),
    Scoped {
        owner_id: String,
        entries: Vec<crate::storage_api::EcstoreScannerScopedDirtyUsageAckEntry>,
    },
}

impl From<ScannerDirtyUsageAcknowledgement> for crate::storage_api::EcstoreScannerDirtyUsageAcknowledgement {
    fn from(acknowledgement: ScannerDirtyUsageAcknowledgement) -> Self {
        match acknowledgement.kind {
            ScannerDirtyUsageAcknowledgementKind::Generation(generation) => Self::Generation {
                host: acknowledgement.host,
                instance_id: acknowledgement.instance_id,
                generation,
            },
            ScannerDirtyUsageAcknowledgementKind::Scoped { owner_id, entries } => Self::Scoped {
                host: acknowledgement.host,
                owner_id,
                instance_id: acknowledgement.instance_id,
                entries,
            },
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ScannerActivityObservation {
    NotRequired,
    Unchanged,
    Changed,
    /// A storage-owned movement generation changed. This wake must bypass the
    /// ordinary deferred cluster-activity backoff so publication can retry
    /// after a transition reaches its terminal state.
    MovementChanged,
    /// A remote scanner process restarted. Publication leases are bound to the
    /// process instance, so this must bypass deferred cluster-activity backoff
    /// even when the restarted peer reports otherwise ordinary activity.
    RemoteRestarted,
    MaintenanceChanged,
    Unverified,
}

pub(super) fn scanner_cycle_wait_plan(
    runtime_config: &ScannerRuntimeConfig,
    clean_idle_backoff: ScannerCleanIdleBackoff,
    clean_idle_backoff_enabled: bool,
    jitter: impl FnOnce(Duration) -> Duration,
) -> ScannerCycleWaitPlan {
    let clean_idle_max_interval = scanner_clean_idle_max_interval(runtime_config.cycle_interval, runtime_config);
    let effective_interval =
        clean_idle_backoff.effective_interval(runtime_config.cycle_interval, clean_idle_max_interval, clean_idle_backoff_enabled);
    let delay = cap_clean_idle_cycle_delay(jitter(effective_interval), clean_idle_max_interval, clean_idle_backoff_enabled);

    ScannerCycleWaitPlan {
        effective_interval,
        clean_idle_max_interval,
        delay,
    }
}

pub(super) fn record_scanner_cycle_result(
    clean_idle_backoff: &mut ScannerCleanIdleBackoff,
    runtime_config: &ScannerRuntimeConfig,
    clean_idle_backoff_enabled: bool,
    wake_reason: ScannerCycleWakeReason,
    outcome: ScannerCycleOutcome,
    dirty_work_observed: bool,
) {
    clean_idle_backoff.record_cycle(
        runtime_config.cycle_interval,
        scanner_clean_idle_max_interval(runtime_config.cycle_interval, runtime_config),
        clean_idle_backoff_enabled,
        wake_reason,
        outcome,
        dirty_work_observed,
    );
}

pub(super) fn scanner_clean_idle_backoff_configured(runtime_config: &ScannerRuntimeConfig) -> bool {
    let bitrot_cycle_allows_backoff =
        runtime_config.bitrot_cycle.is_none() || runtime_config.bitrot_cycle_source == ScannerRuntimeConfigSource::Default;
    runtime_config.cycle_interval_source == ScannerRuntimeConfigSource::Default && bitrot_cycle_allows_backoff
}

pub(super) fn scanner_clean_idle_max_interval(base_interval: Duration, runtime_config: &ScannerRuntimeConfig) -> Duration {
    let policy_max = CLEAN_IDLE_MAX_INTERVAL.max(base_interval);
    let Some(bitrot_cycle) = runtime_config.bitrot_cycle else {
        return policy_max;
    };
    if runtime_config.bitrot_cycle_source != ScannerRuntimeConfigSource::Default {
        return policy_max;
    }

    let selection_window = heal_object_select_prob();
    if selection_window == 0 {
        return policy_max;
    }

    bitrot_cycle
        .checked_div(selection_window)
        .unwrap_or(base_interval)
        .max(base_interval)
        .min(policy_max)
}

pub(super) fn scanner_clean_idle_backoff_enabled(
    topology_supported: bool,
    cluster_activity_ready: bool,
    features: ScannerMaintenanceFeatures,
    runtime_config: &ScannerRuntimeConfig,
) -> bool {
    topology_supported
        && cluster_activity_ready
        && !features.needs_regular_cycle()
        && scanner_clean_idle_backoff_configured(runtime_config)
}

pub(super) fn scanner_activity_probe_required(
    topology_supported: bool,
    backoff_blocked: bool,
    features: ScannerMaintenanceFeatures,
    runtime_config: &ScannerRuntimeConfig,
) -> bool {
    topology_supported
        && !backoff_blocked
        && !features.needs_regular_cycle()
        && scanner_clean_idle_backoff_configured(runtime_config)
}

pub(super) fn scanner_activity_observed_work(observation: ScannerActivityObservation) -> bool {
    matches!(
        observation,
        ScannerActivityObservation::Changed
            | ScannerActivityObservation::MovementChanged
            | ScannerActivityObservation::RemoteRestarted
            | ScannerActivityObservation::MaintenanceChanged
            | ScannerActivityObservation::Unverified
    )
}

pub(super) fn scanner_activity_backoff_blocked_after_wake(currently_blocked: bool, wake_reason: ScannerCycleWakeReason) -> bool {
    match wake_reason {
        ScannerCycleWakeReason::ClusterMaintenance => true,
        ScannerCycleWakeReason::MaintenanceConfig => false,
        _ => currently_blocked,
    }
}

#[cfg(test)]
pub(super) async fn wait_for_next_scanner_cycle<F>(
    ctx: &CancellationToken,
    delay: Duration,
    dirty_usage_generation_seen: Option<u64>,
    runtime_config_generation: u64,
    maintenance_generation: u64,
    is_lock_lost: F,
) -> ScannerCycleWakeReason
where
    F: Fn() -> bool,
{
    let movement = ScannerMovementWaitContext {
        movement_generation_seen: None,
        movement_changed: Arc::new(Notify::new()),
        current_movement_generation: || 0,
        is_lock_lost,
    };
    wait_for_next_scanner_cycle_with_movement(
        ctx,
        delay,
        ScannerCycleObservedGenerations {
            dirty_usage: dirty_usage_generation_seen,
            runtime_config: runtime_config_generation,
            maintenance: maintenance_generation,
            defer_cluster_activity: false,
        },
        &movement,
    )
    .await
}

pub(super) async fn wait_for_next_scanner_cycle_with_movement<G, F>(
    ctx: &CancellationToken,
    delay: Duration,
    generations: ScannerCycleObservedGenerations,
    movement: &ScannerMovementWaitContext<G, F>,
) -> ScannerCycleWakeReason
where
    F: Fn() -> bool,
    G: Fn() -> u64,
{
    let sleep = tokio::time::sleep(delay);
    tokio::pin!(sleep);
    let lock_poll = tokio::time::sleep(SCANNER_LEADER_LOCK_POLL_INTERVAL);
    tokio::pin!(lock_poll);

    loop {
        if (movement.is_lock_lost)() {
            return ScannerCycleWakeReason::LeaderLockLost;
        }
        if scanner_runtime_config_generation() != generations.runtime_config {
            return ScannerCycleWakeReason::RuntimeConfig;
        }
        if scanner_maintenance_generation() != generations.maintenance {
            return ScannerCycleWakeReason::MaintenanceConfig;
        }
        if generations
            .dirty_usage
            .is_some_and(|seen| dirty_usage_buckets_pending() && dirty_usage_generation() != seen)
        {
            return ScannerCycleWakeReason::DirtyUsage;
        }
        if movement
            .movement_generation_seen
            .is_some_and(|seen| (movement.current_movement_generation)() != seen)
        {
            return ScannerCycleWakeReason::MovementGeneration;
        }

        let movement_notification = movement.movement_changed.notified();
        tokio::pin!(movement_notification);
        movement_notification.as_mut().enable();
        // A transition may finish between the initial generation read and
        // registration with Notify. Re-check after `enable()` so that such a
        // transition cannot be lost when it used `notify_waiters()`.
        if movement
            .movement_generation_seen
            .is_some_and(|seen| (movement.current_movement_generation)() != seen)
        {
            return ScannerCycleWakeReason::MovementGeneration;
        }
        tokio::select! {
            _ = ctx.cancelled() => return ScannerCycleWakeReason::Cancelled,
            _ = &mut sleep => return ScannerCycleWakeReason::Timer,
            _ = &mut lock_poll => {
                if (movement.is_lock_lost)() {
                    return ScannerCycleWakeReason::LeaderLockLost;
                }
                lock_poll.as_mut().reset(Instant::now() + SCANNER_LEADER_LOCK_POLL_INTERVAL);
            }
            _ = dirty_usage_bucket_notified() => {
                if scanner_runtime_config_generation() != generations.runtime_config {
                    return ScannerCycleWakeReason::RuntimeConfig;
                }
                if scanner_maintenance_generation() != generations.maintenance {
                    return ScannerCycleWakeReason::MaintenanceConfig;
                }
                if generations
                    .dirty_usage
                    .is_some_and(|seen| dirty_usage_buckets_pending() && dirty_usage_generation() != seen)
                {
                    return ScannerCycleWakeReason::DirtyUsage;
                }
            }
            _ = scanner_runtime_config_changed() => {
                if scanner_runtime_config_generation() != generations.runtime_config {
                    return ScannerCycleWakeReason::RuntimeConfig;
                }
            }
            _ = scanner_maintenance_changed() => {
                if scanner_maintenance_generation() != generations.maintenance {
                    return ScannerCycleWakeReason::MaintenanceConfig;
                }
            }
            _ = &mut movement_notification => {
                if movement
                    .movement_generation_seen
                    .is_some_and(|seen| (movement.current_movement_generation)() != seen)
                {
                    return ScannerCycleWakeReason::MovementGeneration;
                }
            }
        }
    }
}

#[cfg(test)]
pub(super) async fn wait_for_next_scanner_cycle_with_activity<F, Probe, ProbeFuture>(
    ctx: &CancellationToken,
    delay: Duration,
    activity_poll_interval: Option<Duration>,
    activity_seen: &mut Option<ScannerActivitySnapshot>,
    generations: ScannerCycleObservedGenerations,
    is_lock_lost: F,
    probe_activity: Probe,
) -> ScannerCycleWakeReason
where
    F: Fn() -> bool,
    Probe: FnMut() -> ProbeFuture,
    ProbeFuture: Future<Output = Result<ScannerActivitySnapshot, String>>,
{
    let movement = ScannerMovementWaitContext {
        movement_generation_seen: None,
        movement_changed: Arc::new(Notify::new()),
        current_movement_generation: || 0,
        is_lock_lost,
    };
    wait_for_next_scanner_cycle_with_activity_and_movement(
        ctx,
        delay,
        activity_poll_interval,
        activity_seen,
        generations,
        movement,
        probe_activity,
    )
    .await
}

pub(super) async fn wait_for_next_scanner_cycle_with_activity_and_movement<F, G, Probe, ProbeFuture>(
    ctx: &CancellationToken,
    delay: Duration,
    activity_poll_interval: Option<Duration>,
    activity_seen: &mut Option<ScannerActivitySnapshot>,
    generations: ScannerCycleObservedGenerations,
    movement: ScannerMovementWaitContext<G, F>,
    mut probe_activity: Probe,
) -> ScannerCycleWakeReason
where
    F: Fn() -> bool,
    G: Fn() -> u64,
    Probe: FnMut() -> ProbeFuture,
    ProbeFuture: Future<Output = Result<ScannerActivitySnapshot, String>>,
{
    let deadline = Instant::now() + delay;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return ScannerCycleWakeReason::Timer;
        }
        let wait_slice = activity_poll_interval
            .map(|interval| interval.max(Duration::from_secs(1)).min(remaining))
            .unwrap_or(remaining);
        let wake_reason = wait_for_next_scanner_cycle_with_movement(ctx, wait_slice, generations, &movement).await;
        if wake_reason != ScannerCycleWakeReason::Timer || Instant::now() >= deadline {
            return wake_reason;
        }

        let Some(_) = activity_poll_interval else {
            return ScannerCycleWakeReason::Timer;
        };
        if (movement.is_lock_lost)() {
            return ScannerCycleWakeReason::LeaderLockLost;
        }

        let probe = probe_activity();
        tokio::pin!(probe);
        let lock_lost = async {
            loop {
                tokio::time::sleep(SCANNER_LEADER_LOCK_POLL_INTERVAL).await;
                if (movement.is_lock_lost)() {
                    break;
                }
            }
        };
        tokio::pin!(lock_lost);
        let probe_result = tokio::select! {
            result = &mut probe => result,
            _ = ctx.cancelled() => return ScannerCycleWakeReason::Cancelled,
            _ = &mut lock_lost => return ScannerCycleWakeReason::LeaderLockLost,
        };

        let had_baseline = activity_seen.is_some();
        let (observation, probe_error) = apply_scanner_activity_probe_result(activity_seen, probe_result);
        if let Some(err) = probe_error {
            log_scanner_activity_probe_error(had_baseline, &err);
        }
        match observation {
            ScannerActivityObservation::Unchanged | ScannerActivityObservation::NotRequired => {}
            ScannerActivityObservation::MovementChanged | ScannerActivityObservation::RemoteRestarted => {
                return ScannerCycleWakeReason::ClusterActivity;
            }
            ScannerActivityObservation::Changed if !generations.defer_cluster_activity => {
                return ScannerCycleWakeReason::ClusterActivity;
            }
            ScannerActivityObservation::Changed => {}
            ScannerActivityObservation::MaintenanceChanged => return ScannerCycleWakeReason::ClusterMaintenance,
            ScannerActivityObservation::Unverified if !generations.defer_cluster_activity => {
                return ScannerCycleWakeReason::ClusterActivityUnavailable;
            }
            ScannerActivityObservation::Unverified => {}
        }
    }
}

pub(super) fn log_scanner_activity_probe_error(had_baseline: bool, err: &str) {
    if had_baseline {
        warn!(
            target: "rustfs::scanner",
            event = EVENT_SCANNER_CYCLE_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            state = "cluster_activity_probe_failed",
            error = %err,
            "Scanner cluster activity probe failed; preserving the base cycle"
        );
    } else {
        debug!(
            target: "rustfs::scanner",
            event = EVENT_SCANNER_CYCLE_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            state = "cluster_activity_probe_unavailable",
            error = %err,
            "Scanner cluster activity probe remains unavailable"
        );
    }
}

pub(super) fn compare_scanner_activity(
    previous: &ScannerActivitySnapshot,
    current: &ScannerActivitySnapshot,
) -> ScannerActivityObservation {
    if previous == current {
        return ScannerActivityObservation::Unchanged;
    }

    for (host, current_activity) in current {
        let Some(previous_activity) = previous.get(host) else {
            continue;
        };
        if host != LOCAL_SCANNER_ACTIVITY_NODE && previous_activity.instance_id != current_activity.instance_id {
            return ScannerActivityObservation::RemoteRestarted;
        }
        if host != LOCAL_SCANNER_ACTIVITY_NODE
            && previous_activity.instance_id == current_activity.instance_id
            && previous_activity.maintenance_generation != current_activity.maintenance_generation
        {
            return ScannerActivityObservation::MaintenanceChanged;
        }

        if previous_activity.instance_id == current_activity.instance_id
            && (previous_activity.data_movement_active != current_activity.data_movement_active
                || previous_activity.movement_generation != current_activity.movement_generation
                || previous_activity.publication_blocked != current_activity.publication_blocked)
        {
            return ScannerActivityObservation::MovementChanged;
        }
    }

    ScannerActivityObservation::Changed
}

pub(super) fn apply_scanner_activity_probe_result(
    activity_seen: &mut Option<ScannerActivitySnapshot>,
    result: Result<ScannerActivitySnapshot, String>,
) -> (ScannerActivityObservation, Option<String>) {
    match result {
        Ok(current) => {
            let observation = match activity_seen.as_ref() {
                Some(previous) => compare_scanner_activity(previous, &current),
                None => ScannerActivityObservation::Unverified,
            };
            *activity_seen = Some(current);
            (observation, None)
        }
        Err(err) => {
            *activity_seen = None;
            (ScannerActivityObservation::Unverified, Some(err))
        }
    }
}

pub(super) async fn observe_scanner_activity<S>(
    storeapi: &Arc<S>,
    distributed: bool,
    activity_seen: &mut Option<ScannerActivitySnapshot>,
) -> ScannerActivityObservation
where
    S: ScannerStorage,
{
    let had_baseline = activity_seen.is_some();
    let (observation, probe_error) =
        apply_scanner_activity_probe_result(activity_seen, probe_scanner_activity(storeapi.as_ref(), distributed).await);
    if let Some(err) = probe_error {
        log_scanner_activity_probe_error(had_baseline, &err);
    }
    observation
}

pub(crate) fn scanner_activity_snapshot_digest(snapshot: &ScannerActivitySnapshot) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(u64::try_from(snapshot.len()).unwrap_or(u64::MAX).to_be_bytes());
    for (host, activity) in snapshot {
        let host = host.as_bytes();
        let instance_id = activity.instance_id.as_bytes();
        hasher.update(u64::try_from(host.len()).unwrap_or(u64::MAX).to_be_bytes());
        hasher.update(host);
        hasher.update(u64::try_from(instance_id.len()).unwrap_or(u64::MAX).to_be_bytes());
        hasher.update(instance_id);
        hasher.update(activity.namespace_generation.to_be_bytes());
        hasher.update(activity.maintenance_generation.to_be_bytes());
        hasher.update(activity.protocol_version.to_be_bytes());
        hasher.update(activity.topology_digest);
        hasher.update([u8::from(activity.data_movement_active)]);
        hasher.update(activity.dirty_usage_generation.to_be_bytes());
        hasher.update([u8::from(activity.dirty_usage_pending)]);
        hasher.update(activity.movement_generation.to_be_bytes());
        hasher.update([u8::from(activity.publication_blocked)]);
    }
    hasher.finalize().into()
}

/// Hash the activity inputs that make an existing scanner cache unsafe to
/// reuse. Regular namespace writes and dirty-usage generations are omitted:
/// their affected buckets are tracked separately and may be refreshed from a
/// complete authoritative cache baseline.
pub(crate) fn scanner_activity_structural_digest(snapshot: &ScannerActivitySnapshot) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(u64::try_from(snapshot.len()).unwrap_or(u64::MAX).to_be_bytes());
    for (host, activity) in snapshot {
        let host = host.as_bytes();
        let instance_id = activity.instance_id.as_bytes();
        hasher.update(u64::try_from(host.len()).unwrap_or(u64::MAX).to_be_bytes());
        hasher.update(host);
        hasher.update(u64::try_from(instance_id.len()).unwrap_or(u64::MAX).to_be_bytes());
        hasher.update(instance_id);
        hasher.update(activity.maintenance_generation.to_be_bytes());
        hasher.update(activity.protocol_version.to_be_bytes());
        hasher.update(activity.topology_digest);
        hasher.update([u8::from(activity.data_movement_active)]);
        hasher.update(activity.movement_generation.to_be_bytes());
        hasher.update([u8::from(activity.publication_blocked)]);
    }
    hasher.finalize().into()
}

pub(crate) fn scanner_activity_allows_usage_publication(snapshot: &ScannerActivitySnapshot) -> bool {
    !snapshot.is_empty()
        && snapshot.values().all(|activity| {
            activity.protocol_version == SCANNER_ACTIVITY_PROTOCOL_VERSION
                && activity.movement_generation != u64::MAX
                && !activity.data_movement_active
                && !activity.publication_blocked
        })
}

pub(crate) fn scanner_activity_publication_lease_targets(snapshot: &ScannerActivitySnapshot) -> Vec<(String, String, u64)> {
    snapshot
        .iter()
        .filter(|(host, _)| host.as_str() != LOCAL_SCANNER_ACTIVITY_NODE)
        .map(|(host, activity)| (host.clone(), activity.instance_id.clone(), activity.movement_generation))
        .collect()
}

pub(crate) fn scanner_dirty_usage_acknowledgements(snapshot: &ScannerActivitySnapshot) -> Vec<ScannerDirtyUsageAcknowledgement> {
    snapshot
        .iter()
        .filter(|(host, activity)| host.as_str() != LOCAL_SCANNER_ACTIVITY_NODE && activity.dirty_usage_pending)
        .map(|(host, activity)| ScannerDirtyUsageAcknowledgement {
            host: host.clone(),
            instance_id: activity.instance_id.clone(),
            kind: ScannerDirtyUsageAcknowledgementKind::Generation(activity.dirty_usage_generation),
        })
        .collect()
}

pub(crate) fn scanner_activity_dirty_usage_state_for_host<'a>(
    snapshot: &'a ScannerActivitySnapshot,
    host: &str,
) -> Option<(&'a str, u64, bool)> {
    snapshot
        .get(host)
        .filter(|_| host != LOCAL_SCANNER_ACTIVITY_NODE)
        .map(|activity| {
            (
                activity.instance_id.as_str(),
                activity.dirty_usage_generation,
                activity.dirty_usage_pending,
            )
        })
}

pub fn scanner_topology_digest(storeapi: &ECStore) -> [u8; 32] {
    let endpoint_pools = storeapi.endpoints();
    let mut hasher = Sha256::new();
    hasher.update(u64::try_from(endpoint_pools.0.len()).unwrap_or(u64::MAX).to_be_bytes());
    for (pool_index, pool) in endpoint_pools.0.iter().enumerate() {
        hasher.update(u64::try_from(pool_index).unwrap_or(u64::MAX).to_be_bytes());
        hasher.update(u64::try_from(pool.set_count).unwrap_or(u64::MAX).to_be_bytes());
        hasher.update(u64::try_from(pool.drives_per_set).unwrap_or(u64::MAX).to_be_bytes());
        let mut endpoints = pool.endpoints.as_ref().iter().collect::<Vec<_>>();
        endpoints.sort_unstable_by(|left, right| {
            (left.pool_idx, left.set_idx, left.disk_idx, left.url.as_str()).cmp(&(
                right.pool_idx,
                right.set_idx,
                right.disk_idx,
                right.url.as_str(),
            ))
        });
        hasher.update(u64::try_from(endpoints.len()).unwrap_or(u64::MAX).to_be_bytes());
        for endpoint in endpoints {
            hasher.update(endpoint.pool_idx.to_be_bytes());
            hasher.update(endpoint.set_idx.to_be_bytes());
            hasher.update(endpoint.disk_idx.to_be_bytes());
            let url = endpoint.url.as_str().as_bytes();
            hasher.update(u64::try_from(url.len()).unwrap_or(u64::MAX).to_be_bytes());
            hasher.update(url);
        }
    }
    hasher.finalize().into()
}

pub(super) fn record_scanner_activity_instance(
    instance_hosts: &mut BTreeMap<String, String>,
    host: &str,
    instance_id: &str,
) -> Result<(), String> {
    if let Some(existing_host) = instance_hosts.insert(instance_id.to_string(), host.to_string()) {
        return Err(format!(
            "scanner activity peers {existing_host} and {host} report the same process instance"
        ));
    }
    Ok(())
}

pub(crate) async fn probe_scanner_activity<S>(storeapi: &S, distributed: bool) -> Result<ScannerActivitySnapshot, String>
where
    S: ScannerStorage,
{
    let topology_digest = storeapi.scanner_topology_digest();
    let (data_movement_active, publication_blocked, movement_generation) = storeapi.scanner_data_movement_activity().await;
    let namespace_generation = storeapi.scanner_namespace_mutation_generation();
    let maintenance_generation = scanner_maintenance_generation();
    let dirty_usage = scanner_dirty_usage_state();
    if namespace_generation == u64::MAX
        || maintenance_generation == u64::MAX
        || dirty_usage.generation == u64::MAX
        || movement_generation == u64::MAX
    {
        return Err("local scanner activity generation is exhausted".to_string());
    }
    let local_instance_id = crate::scanner_io::scanner_activity_epoch().to_string();
    let mut instance_hosts = BTreeMap::from([(local_instance_id.clone(), LOCAL_SCANNER_ACTIVITY_NODE.to_string())]);
    let mut snapshot = ScannerActivitySnapshot::from([(
        LOCAL_SCANNER_ACTIVITY_NODE.to_string(),
        ScannerNodeActivity {
            instance_id: local_instance_id,
            namespace_generation,
            maintenance_generation,
            protocol_version: SCANNER_ACTIVITY_PROTOCOL_VERSION,
            topology_digest,
            data_movement_active,
            dirty_usage_generation: dirty_usage.generation,
            dirty_usage_pending: dirty_usage.pending,
            movement_generation,
            publication_blocked,
        },
    )]);
    if !distributed {
        return Ok(snapshot);
    }

    let notification_system = storeapi
        .scanner_notification_system()
        .ok_or_else(|| "notification system is not initialized".to_string())?;
    let peers = notification_system
        .scanner_activity_snapshots()
        .await
        .map_err(|err| err.to_string())?;
    for (host, activity) in peers {
        if activity.namespace_generation == u64::MAX || activity.maintenance_generation == u64::MAX {
            return Err(format!("scanner activity peer {host} exhausted its activity generation"));
        }
        let (
            peer_topology_digest,
            peer_data_movement_active,
            peer_dirty_usage_generation,
            peer_dirty_usage_pending,
            peer_movement_generation,
            peer_publication_blocked,
        ) = match activity.protocol_version {
            SCANNER_ACTIVITY_LEGACY_PROTOCOL_VERSION => {
                return Err(format!("scanner activity peer {host} cannot verify data movement publication fencing"));
            }
            SCANNER_ACTIVITY_PREVIOUS_PROTOCOL_VERSION => {
                return Err(format!(
                    "scanner activity peer {host} cannot safely share scanner cache locks with protocol {}",
                    SCANNER_ACTIVITY_PREVIOUS_PROTOCOL_VERSION
                ));
            }
            SCANNER_ACTIVITY_V6_PROTOCOL_VERSION => {
                return Err(format!(
                    "scanner activity peer {host} cannot verify terminal movement state with protocol {}",
                    SCANNER_ACTIVITY_V6_PROTOCOL_VERSION
                ));
            }
            SCANNER_ACTIVITY_PROTOCOL_VERSION => (
                activity
                    .topology_digest
                    .ok_or_else(|| format!("scanner activity peer {host} omitted its storage topology"))?,
                activity
                    .data_movement_active
                    .ok_or_else(|| format!("scanner activity peer {host} omitted its data movement state"))?,
                activity
                    .dirty_usage_generation
                    .ok_or_else(|| format!("scanner activity peer {host} omitted its dirty usage generation"))?,
                activity
                    .dirty_usage_pending
                    .ok_or_else(|| format!("scanner activity peer {host} omitted its dirty usage state"))?,
                activity
                    .movement_generation
                    .ok_or_else(|| format!("scanner activity peer {host} omitted its movement generation"))?,
                activity
                    .publication_blocked
                    .ok_or_else(|| format!("scanner activity peer {host} omitted its publication blocked state"))?,
            ),
            version => {
                return Err(format!(
                    "scanner activity peer {host} uses protocol {version}, expected {}",
                    SCANNER_ACTIVITY_PROTOCOL_VERSION
                ));
            }
        };
        if peer_dirty_usage_generation == u64::MAX || peer_movement_generation == u64::MAX {
            return Err(format!("scanner activity peer {host} exhausted its dirty usage generation"));
        }
        if peer_topology_digest != topology_digest {
            return Err(format!("scanner activity peer {host} has a different storage topology"));
        }
        record_scanner_activity_instance(&mut instance_hosts, &host, &activity.instance_id)?;
        if snapshot
            .insert(
                host.clone(),
                ScannerNodeActivity {
                    instance_id: activity.instance_id,
                    namespace_generation: activity.namespace_generation,
                    maintenance_generation: activity.maintenance_generation,
                    protocol_version: activity.protocol_version,
                    topology_digest: peer_topology_digest,
                    data_movement_active: peer_data_movement_active,
                    dirty_usage_generation: peer_dirty_usage_generation,
                    dirty_usage_pending: peer_dirty_usage_pending,
                    movement_generation: peer_movement_generation,
                    publication_blocked: peer_publication_blocked,
                },
            )
            .is_some()
        {
            return Err(format!("duplicate scanner activity peer: {host}"));
        }
    }
    Ok(snapshot)
}
