// Copyright 2026 RustFS Team
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

//! Live candidate diagnostics, deliberately separate from persisted retry hints
//! and traversal coverage. Missing content/leader identities remain unverified.

use metrics::{counter, gauge};
use rustfs_heal_contracts::{
    completion::{MAX_COMPLETION_IDENTITY_BYTES, ObservedHealCompletion, subscribe_observed_heal_completions},
    heal_channel::{HealChannelRequest, HealObservationChannelError, HealScanMode, ObservedHealAdmissionReceipt},
    outcome::{HealAbortReason, HealExecutionOutcome, HealObjectDisposition, HealObjectIdentity, HealObjectKind},
};
use serde::Serialize;
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};
use tokio::sync::{broadcast, mpsc};

const MAX_PENDING: usize = 1024;
const MAX_PENDING_GROUPS: usize = MAX_PENDING;
const MAX_RECENT: usize = 1024;
const PENDING_TTL: Duration = Duration::from_secs(60 * 60);
const RECENT_TTL: Duration = Duration::from_secs(10 * 60);

#[derive(Clone, Debug, Serialize)]
pub struct ScannerHealTerminalObservation {
    pub execution: &'static str,
    pub disposition: &'static str,
    pub scope: &'static str,
    pub scan_mode: &'static str,
    pub count: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ScannerHealUnknownObservation {
    pub reason: &'static str,
    pub count: u64,
}

/// A bounded local observation, not a cluster backlog or a repair receipt.
#[derive(Clone, Debug, Serialize)]
pub struct ScannerHealObservationStatus {
    pub state: &'static str,
    pub generation: Option<u64>,
    pub started_at_unix_secs: Option<u64>,
    pub sampled_at_unix_secs: Option<u64>,
    pub retained_pending_tasks: Option<usize>,
    pub retained_pending_candidates: Option<u64>,
    pub oldest_pending_seconds: Option<f64>,
    pub queued_admissions: Option<usize>,
    pub terminal_task_observations_total: Option<u64>,
    pub terminal_candidates: Vec<ScannerHealTerminalObservation>,
    pub unknown_candidates: Vec<ScannerHealUnknownObservation>,
    pub lost_completion_notifications: Option<u64>,
    pub integrity_state: &'static str,
}

impl Default for ScannerHealObservationStatus {
    fn default() -> Self {
        Self {
            state: "not_started",
            generation: None,
            started_at_unix_secs: None,
            sampled_at_unix_secs: None,
            retained_pending_tasks: None,
            retained_pending_candidates: None,
            oldest_pending_seconds: None,
            queued_admissions: None,
            terminal_task_observations_total: None,
            terminal_candidates: Vec::new(),
            unknown_candidates: Vec::new(),
            lost_completion_notifications: None,
            integrity_state: "unknown",
        }
    }
}

#[derive(Debug)]
pub(crate) struct Candidate {
    identity: Option<HealObjectIdentity>,
    mode: HealScanMode,
    dry_run: bool,
    first_seen: Instant,
    count: u64,
}

impl Candidate {
    fn from_request(request: &HealChannelRequest) -> Option<Self> {
        let bytes = request
            .bucket
            .len()
            .saturating_add(request.object_prefix.as_ref().map_or(0, String::len))
            .saturating_add(request.object_version_id.as_ref().map_or(0, String::len));
        if bytes > MAX_COMPLETION_IDENTITY_BYTES {
            return None;
        }
        Some(Self {
            identity: request.object_prefix.as_ref().map(|object| HealObjectIdentity {
                kind: HealObjectKind::Object,
                bucket: request.bucket.clone(),
                object: object.clone(),
                version_id: request.object_version_id.clone(),
                bucket_incarnation_id: request.expected_bucket_incarnation_id,
                pool_index: request.pool_index,
                set_index: request.set_index,
            }),
            mode: request.scan_mode.unwrap_or_default(),
            dry_run: request.dry_run.unwrap_or(false),
            first_seen: Instant::now(),
            count: 1,
        })
    }

    fn has_same_semantics(&self, other: &Self) -> bool {
        self.identity == other.identity && self.mode == other.mode && self.dry_run == other.dry_run
    }
}

struct Pending {
    // Keep the common single-group case inline.
    first: Candidate,
    others: Vec<Candidate>,
}

impl Pending {
    fn iter(&self) -> impl Iterator<Item = &Candidate> {
        std::iter::once(&self.first).chain(self.others.iter())
    }

    fn into_candidates(self) -> impl Iterator<Item = Candidate> {
        std::iter::once(self.first).chain(self.others)
    }
}

struct Recent {
    completion: Arc<ObservedHealCompletion>,
    counted: bool,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct ObservationKey {
    task_id: String,
    observation_id: rustfs_heal_contracts::completion::HealObservationId,
}

#[derive(Default)]
struct Observations {
    pending: HashMap<ObservationKey, Pending>,
    // The group bound applies across all observation keys.
    pending_groups: usize,
    recent: HashMap<ObservationKey, Recent>,
    recent_order: VecDeque<(Instant, ObservationKey)>,
    // All keys come from fixed execution/disposition/scope/mode labels, never task or object identities.
    terminal_counts: BTreeMap<(&'static str, &'static str, &'static str, &'static str), u64>,
    terminal_tasks: u64,
    unknown_counts: BTreeMap<&'static str, u64>,
    lost_notifications: u64,
}

#[derive(Debug)]
struct Settlement {
    candidates: u64,
    first_task_observation: bool,
    execution: &'static str,
    disposition: &'static str,
    scope: &'static str,
    mode: HealScanMode,
}

impl Settlement {
    fn new(candidate: Candidate, recent: &mut Recent) -> Self {
        let completion = &recent.completion;
        let execution = match completion.execution {
            Some(HealExecutionOutcome::Pending) => "pending",
            Some(HealExecutionOutcome::Running) => "running",
            Some(HealExecutionOutcome::Completed) => "completed",
            Some(HealExecutionOutcome::CompletedWithErrors) => "completed_with_errors",
            Some(HealExecutionOutcome::Aborted(HealAbortReason::Cancelled)) => "aborted_cancelled",
            Some(HealExecutionOutcome::Aborted(HealAbortReason::Deadline)) => "aborted_deadline",
            Some(HealExecutionOutcome::Aborted(HealAbortReason::Untraversable)) => "aborted_untraversable",
            None => "unknown",
        };
        let disposition = match completion.object.as_ref().map(|object| &object.disposition) {
            Some(HealObjectDisposition::VerifiedHealthy) => "verified_healthy",
            Some(HealObjectDisposition::MetadataHealthy) => "metadata_healthy",
            Some(HealObjectDisposition::Repaired) => "repaired",
            Some(HealObjectDisposition::AuthoritativelyAbsent) => "absent",
            Some(HealObjectDisposition::Deferred { .. }) => "deferred",
            Some(HealObjectDisposition::Failed(_)) => "failed",
            Some(HealObjectDisposition::Cancelled) => "cancelled",
            Some(HealObjectDisposition::DryRunObserved) => "dry_run",
            Some(HealObjectDisposition::Unknown) | None => "unknown",
        };
        let scope = if candidate.dry_run || completion.dry_run {
            "dry_run"
        } else if completion.execution != Some(HealExecutionOutcome::Completed) {
            "execution_incomplete"
        } else if candidate.mode == HealScanMode::Deep && completion.scan_mode != HealScanMode::Deep {
            "insufficient_scan"
        } else {
            match (candidate.identity.as_ref(), completion.object.as_ref()) {
                (Some(expected), Some(actual)) => Self::identity_scope(expected, &actual.identity),
                _ => "identity_unknown",
            }
        };
        let first_task_observation = !recent.counted;
        recent.counted = true;
        Self {
            candidates: candidate.count,
            first_task_observation,
            execution,
            disposition,
            scope,
            mode: candidate.mode,
        }
    }

    fn identity_scope(expected: &HealObjectIdentity, actual: &HealObjectIdentity) -> &'static str {
        fn known_version(version: Option<&str>) -> Option<uuid::Uuid> {
            version
                .and_then(|id| uuid::Uuid::parse_str(id).ok())
                .filter(|id| !id.is_nil())
        }
        let expected_bucket = (!expected.bucket.is_empty()).then_some(expected.bucket.as_str());
        let actual_bucket = (!actual.bucket.is_empty()).then_some(actual.bucket.as_str());
        let expected_object = (!expected.object.is_empty()).then_some(expected.object.as_str());
        let actual_object = (!actual.object.is_empty()).then_some(actual.object.as_str());
        let expected_version = known_version(expected.version_id.as_deref());
        let actual_version = known_version(actual.version_id.as_deref());
        let expected_incarnation = expected.bucket_incarnation_id.filter(|id| !id.is_nil());
        let actual_incarnation = actual.bucket_incarnation_id.filter(|id| !id.is_nil());

        if expected.kind != actual.kind
            || expected_bucket.zip(actual_bucket).is_some_and(|(left, right)| left != right)
            || expected_object.zip(actual_object).is_some_and(|(left, right)| left != right)
            || expected_version
                .zip(actual_version)
                .is_some_and(|(left, right)| left != right)
            || expected_incarnation
                .zip(actual_incarnation)
                .is_some_and(|(left, right)| left != right)
            || expected
                .pool_index
                .zip(actual.pool_index)
                .is_some_and(|(left, right)| left != right)
            || expected
                .set_index
                .zip(actual.set_index)
                .is_some_and(|(left, right)| left != right)
        {
            "identity_mismatch"
        } else if expected_bucket.is_none()
            || actual_bucket.is_none()
            || expected_object.is_none()
            || actual_object.is_none()
            || expected_version.is_none()
            || actual_version.is_none()
            || expected_incarnation.is_none()
            || actual_incarnation.is_none()
            || expected.pool_index.is_none()
            || actual.pool_index.is_none()
            || expected.set_index.is_none()
            || actual.set_index.is_none()
        {
            "identity_unknown"
        } else {
            // Matching identities still do not bind content or leader epoch.
            "content_and_leader_unknown"
        }
    }

    fn record(&self) {
        counter!("rustfs_scanner_heal_candidate_terminal_total",
            "disposition" => self.disposition, "scope" => self.scope, "scan_mode" => self.mode.as_str())
        .increment(self.candidates);
        if self.first_task_observation {
            counter!("rustfs_scanner_heal_task_terminal_observations_total").increment(1);
        }
    }
}

impl Observations {
    fn record_unknown(&mut self, reason: &'static str, count: u64) {
        let total = self.unknown_counts.entry(reason).or_default();
        *total = total.saturating_add(count);
        counter!("rustfs_scanner_heal_candidate_unknown_total", "reason" => reason).increment(count);
    }

    fn record_settlement(&mut self, settlement: &Settlement) {
        let total = self
            .terminal_counts
            .entry((settlement.execution, settlement.disposition, settlement.scope, settlement.mode.as_str()))
            .or_default();
        *total = total.saturating_add(settlement.candidates);
        if settlement.first_task_observation {
            self.terminal_tasks = self.terminal_tasks.saturating_add(1);
        }
        settlement.record();
        // Even a typed terminal lacks the candidate's content and leader proof.
        self.record_unknown(settlement.scope, settlement.candidates);
    }

    fn admit(&mut self, key: ObservationKey, candidate: Candidate) -> Option<Settlement> {
        if Instant::now().saturating_duration_since(candidate.first_seen) >= PENDING_TTL {
            self.record_unknown("expired", candidate.count);
            return None;
        }
        if let Some(recent) = self.recent.get_mut(&key) {
            let settlement = Settlement::new(candidate, recent);
            self.record_settlement(&settlement);
            return Some(settlement);
        }
        if let Some(pending) = self.pending.get_mut(&key) {
            if pending.first.has_same_semantics(&candidate) {
                pending.first.count = pending.first.count.saturating_add(candidate.count);
                pending.first.first_seen = pending.first.first_seen.min(candidate.first_seen);
            } else if let Some(group) = pending.others.iter_mut().find(|group| group.has_same_semantics(&candidate)) {
                group.count = group.count.saturating_add(candidate.count);
                group.first_seen = group.first_seen.min(candidate.first_seen);
            } else if self.pending_groups < MAX_PENDING_GROUPS {
                pending.others.push(candidate);
                self.pending_groups += 1;
            } else {
                self.record_unknown("pending_groups_full", candidate.count);
            }
        } else if self.pending.len() >= MAX_PENDING {
            self.record_unknown("pending_full", candidate.count);
        } else if self.pending_groups >= MAX_PENDING_GROUPS {
            self.record_unknown("pending_groups_full", candidate.count);
        } else {
            self.pending.insert(
                key,
                Pending {
                    first: candidate,
                    others: Vec::new(),
                },
            );
            self.pending_groups += 1;
        }
        None
    }

    fn complete(&mut self, completion: Arc<ObservedHealCompletion>, now: Instant) -> Option<Settlement> {
        let key = ObservationKey {
            task_id: completion.task_id.clone(),
            observation_id: completion.observation_id?,
        };
        if self.recent.contains_key(&key) {
            return None;
        }
        let mut recent = Recent {
            completion,
            counted: false,
        };
        let mut settled = None;
        if let Some(pending) = self.pending.remove(&key) {
            self.pending_groups -= 1 + pending.others.len();
            for candidate in pending.into_candidates() {
                if now.saturating_duration_since(candidate.first_seen) >= PENDING_TTL {
                    self.record_unknown("expired", candidate.count);
                    continue;
                }
                let settlement = Settlement::new(candidate, &mut recent);
                self.record_settlement(&settlement);
                if settled.is_none() {
                    settled = Some(settlement);
                }
            }
        }
        if self.recent.len() == MAX_RECENT {
            self.evict_recent();
        }
        self.recent_order.push_back((now, key.clone()));
        self.recent.insert(key, recent);
        settled
    }

    fn evict_recent(&mut self) {
        if let Some((_, id)) = self.recent_order.pop_front() {
            self.recent.remove(&id);
        }
    }

    fn lost(&mut self) {
        let candidates = self
            .pending
            .drain()
            .flat_map(|(_, pending)| pending.into_candidates())
            .fold(0u64, |count, candidate| count.saturating_add(candidate.count));
        self.pending_groups = 0;
        if candidates > 0 {
            self.record_unknown("completion_lost", candidates);
        }
        self.recent.clear();
        self.recent_order.clear();
    }

    fn lagged(&mut self, skipped: u64) {
        counter!("rustfs_scanner_heal_completion_lost_total").increment(skipped);
        self.lost_notifications = self.lost_notifications.saturating_add(skipped);
        self.lost();
    }

    fn maintain(&mut self, now: Instant) {
        let mut expired = 0u64;
        let mut expired_groups = 0usize;
        self.pending.retain(|_, pending| {
            let previous_len = pending.others.len();
            pending.others.retain(|candidate| {
                if now.saturating_duration_since(candidate.first_seen) >= PENDING_TTL {
                    expired = expired.saturating_add(candidate.count);
                    expired_groups += 1;
                    false
                } else {
                    true
                }
            });
            if now.saturating_duration_since(pending.first.first_seen) >= PENDING_TTL {
                expired = expired.saturating_add(pending.first.count);
                expired_groups += 1;
                if pending.others.is_empty() {
                    return false;
                }
                pending.first = pending.others.remove(0);
            }
            if pending.others.len() < previous_len {
                pending.others.shrink_to_fit();
            }
            true
        });
        self.pending_groups -= expired_groups;
        if expired > 0 {
            self.record_unknown("expired", expired);
        }
        while self
            .recent_order
            .front()
            .is_some_and(|(at, _)| now.saturating_duration_since(*at) >= RECENT_TTL)
        {
            self.evict_recent();
        }
        let oldest = self
            .pending
            .values()
            .flat_map(Pending::iter)
            .map(|candidate| now.saturating_duration_since(candidate.first_seen))
            .max();
        gauge!("rustfs_scanner_heal_pending_tasks").set(self.pending.len() as f64);
        gauge!("rustfs_scanner_heal_pending_oldest_seconds").set(oldest.unwrap_or_default().as_secs_f64());
    }
}

type AdmissionSender = mpsc::Sender<(ObservationKey, Candidate)>;

#[derive(Clone)]
struct ObserverHandle {
    sender: AdmissionSender,
    state: Arc<Mutex<Observations>>,
    generation: u64,
    started_at_unix_secs: Option<u64>,
}

impl ObserverHandle {
    fn record_unknown(&self, reason: &'static str, count: u64) {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .record_unknown(reason, count);
    }
}

static OBSERVER: Mutex<Option<ObserverHandle>> = Mutex::new(None);

fn observer_from(registry: &Mutex<Option<ObserverHandle>>) -> ObserverHandle {
    // Installation takes only the registry lock. Drop records under registry then state;
    // other readers release the registry before taking state, and workers take only state.
    let mut installed = registry.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(handle) = installed.as_ref().filter(|handle| !handle.sender.is_closed()) {
        return handle.clone();
    }
    let state = Arc::new(Mutex::new(Observations::default()));
    let handle = {
        let (sender, mut admissions) = mpsc::channel(MAX_PENDING);
        let mut completions = subscribe_observed_heal_completions();
        let observations = state.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    candidate = admissions.recv() => match candidate {
                        Some((id, candidate)) => {
                            let _ = observations.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).admit(id, candidate);
                        },
                        None => break,
                    },
                    completion = completions.recv() => match completion {
                        Ok(completion) => {
                            let _ = observations.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
                                .complete(completion, Instant::now());
                        },
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            observations.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).lagged(skipped);
                        },
                        Err(broadcast::error::RecvError::Closed) => {
                            observations.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).lost();
                            break;
                        },
                    },
                    _ = tick.tick() => {
                        observations.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).maintain(Instant::now());
                    },
                }
            }
        });
        ObserverHandle {
            sender,
            state,
            generation: installed.as_ref().map_or(1, |handle| handle.generation.saturating_add(1)),
            started_at_unix_secs: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .ok()
                .map(|age| age.as_secs()),
        }
    };
    *installed = Some(handle.clone());
    handle
}

fn observer() -> ObserverHandle {
    observer_from(&OBSERVER)
}

fn observation_status_from(registry: &Mutex<Option<ObserverHandle>>) -> ScannerHealObservationStatus {
    let handle = registry.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).clone();
    let mut status = ScannerHealObservationStatus {
        sampled_at_unix_secs: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .ok()
            .map(|age| age.as_secs()),
        ..Default::default()
    };
    let Some(handle) = handle else {
        return status;
    };
    status.generation = Some(handle.generation);
    status.started_at_unix_secs = handle.started_at_unix_secs;
    let observations = handle.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if handle.sender.is_closed() {
        status.state = "stopped";
    } else {
        status.state = "active";
        status.retained_pending_tasks = Some(observations.pending.len());
        status.retained_pending_candidates = Some(
            observations
                .pending
                .values()
                .flat_map(Pending::iter)
                .fold(0u64, |count, candidate| count.saturating_add(candidate.count)),
        );
        let now = Instant::now();
        status.oldest_pending_seconds = observations
            .pending
            .values()
            .flat_map(Pending::iter)
            .map(|candidate| now.saturating_duration_since(candidate.first_seen))
            .max()
            .map(|age| age.as_secs_f64());
        // A local channel sample, not an atomic cluster backlog or a completion test.
        status.queued_admissions = Some(handle.sender.max_capacity().saturating_sub(handle.sender.capacity()));
    }
    status.terminal_task_observations_total = Some(observations.terminal_tasks);
    status.terminal_candidates = observations
        .terminal_counts
        .iter()
        .map(|(&(execution, disposition, scope, scan_mode), &count)| ScannerHealTerminalObservation {
            execution,
            disposition,
            scope,
            scan_mode,
            count,
        })
        .collect();
    status.unknown_candidates = observations
        .unknown_counts
        .iter()
        .map(|(&reason, &count)| ScannerHealUnknownObservation { reason, count })
        .collect();
    status.lost_completion_notifications = Some(observations.lost_notifications);
    status
}

/// Read existing diagnostics without starting an observer or admitting work.
pub fn scanner_heal_observation_status() -> ScannerHealObservationStatus {
    observation_status_from(&OBSERVER)
}

/// Keeps an unconfirmed admission visible if its caller is dropped while waiting.
pub(crate) struct AdmissionObservation {
    observer: ObserverHandle,
    candidate: Option<Candidate>,
    enqueued: bool,
    finished: bool,
}

impl AdmissionObservation {
    pub(crate) fn mark_enqueued(&mut self) {
        self.enqueued = true;
    }

    pub(crate) fn observe(mut self, receipt: Result<&ObservedHealAdmissionReceipt, &HealObservationChannelError>) {
        self.finished = true;
        let observer = observer();
        if receipt.is_err() {
            let reason = if self.enqueued {
                "admission_unknown"
            } else {
                "admission_not_sent"
            };
            observer.record_unknown(reason, 1);
        } else {
            observe_on(&observer, self.candidate.take(), receipt);
        }
    }
}

impl Drop for AdmissionObservation {
    fn drop(&mut self) {
        if self.enqueued && !self.finished {
            record_dropped_admission(&OBSERVER, &self.observer);
        }
    }
}

fn record_dropped_admission(registry: &Mutex<Option<ObserverHandle>>, original: &ObserverHandle) {
    let installed = registry.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    installed.as_ref().unwrap_or(original).record_unknown("admission_unknown", 1);
}

/// Capture only the identity actually available at discovery, before dispatch.
pub(crate) fn prepare(request: &HealChannelRequest) -> AdmissionObservation {
    AdmissionObservation {
        observer: observer(),
        candidate: Candidate::from_request(request),
        enqueued: false,
        finished: false,
    }
}

fn observe_on(
    observer: &ObserverHandle,
    candidate: Option<Candidate>,
    receipt: Result<&ObservedHealAdmissionReceipt, &HealObservationChannelError>,
) {
    match receipt {
        Ok(receipt) if receipt.result.is_admitted() => {
            let Some(candidate) = candidate else {
                observer.record_unknown("identity_too_large", 1);
                return;
            };
            let task_id = &receipt.task_id;
            if task_id.is_empty() || task_id.len() > 128 {
                observer.record_unknown("missing_task_id", 1);
                return;
            }
            let Some(observation_id) = receipt.observation_id else {
                observer.record_unknown("owner_generation_unknown", candidate.count);
                return;
            };
            let key = ObservationKey {
                task_id: task_id.clone(),
                observation_id,
            };
            if observer.sender.try_send((key, candidate)).is_err() {
                observer.record_unknown("observer_full_or_closed", 1);
            }
        }
        Ok(_) => observer.record_unknown("not_admitted", 1),
        Err(HealObservationChannelError::NotInitialized | HealObservationChannelError::LegacyCommandClosed) => {
            observer.record_unknown("admission_not_sent", 1);
        }
        Err(_) => observer.record_unknown("admission_unknown", 1),
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod admission_drop_tests {
    use super::*;

    #[test]
    fn dropped_admission_is_recorded_in_the_installed_generation() {
        let (sender, _original_receiver) = mpsc::channel(1);
        let original = ObserverHandle {
            sender,
            state: Arc::new(Mutex::new(Observations::default())),
            generation: 1,
            started_at_unix_secs: None,
        };
        let (sender, _replacement_receiver) = mpsc::channel(1);
        let replacement = ObserverHandle {
            sender,
            state: Arc::new(Mutex::new(Observations::default())),
            generation: 2,
            started_at_unix_secs: None,
        };
        let registry = Mutex::new(Some(replacement));
        record_dropped_admission(&registry, &original);
        let original_state = original.state.lock().expect("original observation state");
        assert!(!original_state.unknown_counts.contains_key("admission_unknown"));
        drop(original_state);
        let status = observation_status_from(&registry);
        assert_eq!(status.generation, Some(2));
        assert!(
            status
                .unknown_candidates
                .iter()
                .any(|row| row.reason == "admission_unknown" && row.count == 1)
        );
    }
}
