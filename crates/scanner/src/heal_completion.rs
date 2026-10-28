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
    completion::{HealCompletion, MAX_COMPLETION_IDENTITY_BYTES, subscribe_heal_completions},
    heal_channel::{HealAdmissionReceipt, HealChannelRequest, HealScanMode},
    outcome::{HealAbortReason, HealExecutionOutcome, HealObjectDisposition, HealObjectIdentity, HealObjectKind},
};
use serde::Serialize;
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};
use tokio::sync::{broadcast, mpsc};
use uuid::Uuid;

const MAX_PENDING: usize = 1024;
const MAX_PENDING_GROUPS: usize = 1024;
const MAX_RECENT: usize = 1024;
const PENDING_TTL: Duration = Duration::from_secs(60 * 60);
const RECENT_TTL: Duration = Duration::from_secs(10 * 60);

#[cfg(feature = "test-util")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ScannerHealTestAdmission {
    Pending,
    Unknown,
    CachedTerminal { data_dir: Option<Uuid> },
    Unaccounted,
}

#[cfg(feature = "test-util")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ScannerHealTestEvent {
    Admission {
        task_id: String,
        candidate_data_dir: Option<Uuid>,
        outcome: ScannerHealTestAdmission,
    },
    Terminal {
        task_id: String,
        terminal_data_dir: Option<Uuid>,
        duplicate: bool,
        settled_candidate_data_dirs: Vec<Option<Uuid>>,
        remaining_candidate_data_dirs: Vec<Option<Uuid>>,
    },
}

#[cfg(feature = "test-util")]
fn test_events() -> &'static broadcast::Sender<ScannerHealTestEvent> {
    static EVENTS: std::sync::OnceLock<broadcast::Sender<ScannerHealTestEvent>> = std::sync::OnceLock::new();
    EVENTS.get_or_init(|| broadcast::channel(64).0)
}

/// Subscribe to bounded, process-local observer events in test builds.
#[cfg(feature = "test-util")]
#[doc(hidden)]
pub fn subscribe_scanner_heal_test_events() -> broadcast::Receiver<ScannerHealTestEvent> {
    test_events().subscribe()
}

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
    data_dir: Option<Uuid>,
    mode: HealScanMode,
    dry_run: bool,
    first_seen: Instant,
    count: u64,
}

impl Candidate {
    fn from_request(request: &HealChannelRequest, data_dir: Option<Uuid>) -> Option<Self> {
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
            data_dir: data_dir.filter(|id| !id.is_nil()),
            mode: request.scan_mode.unwrap_or_default(),
            dry_run: request.dry_run.unwrap_or(false),
            first_seen: Instant::now(),
            count: 1,
        })
    }

    fn matches(&self, other: &Self) -> bool {
        self.identity == other.identity
            && self.data_dir == other.data_dir
            && self.mode == other.mode
            && self.dry_run == other.dry_run
    }
}

struct Pending {
    first: Candidate,
    // The common single-candidate case does not allocate a vector.
    others: Vec<Candidate>,
}

impl Pending {
    fn iter(&self) -> impl Iterator<Item = &Candidate> {
        std::iter::once(&self.first).chain(self.others.iter())
    }

    fn into_iter(self) -> impl Iterator<Item = Candidate> {
        std::iter::once(self.first).chain(self.others)
    }
}

struct Recent {
    completion: Arc<HealCompletion>,
    counted: bool,
}

#[derive(Default)]
struct Observations {
    pending: HashMap<String, Pending>,
    pending_groups: usize,
    recent: HashMap<String, Recent>,
    recent_order: VecDeque<(Instant, String)>,
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
    generation_mismatch: bool,
    deep_payload_verified_task: bool,
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
        let generation_comparison = match (candidate.identity.as_ref(), completion.object.as_ref()) {
            (Some(expected), Some(actual))
                if expected.kind == actual.identity.kind
                    && expected.bucket == actual.identity.bucket
                    && expected.object == actual.identity.object =>
            {
                candidate
                    .data_dir
                    .filter(|id| !id.is_nil())
                    .zip(actual.resolved_data_dir.filter(|id| !id.is_nil()))
                    .map(|(left, right)| left == right)
            }
            _ => None,
        };
        let generation_mismatch = generation_comparison == Some(false);
        let payload_verified = completion
            .object
            .as_ref()
            .is_some_and(|object| object.payload_integrity_verified);
        let scope = if candidate.dry_run || completion.dry_run {
            "dry_run"
        } else if completion.execution != Some(HealExecutionOutcome::Completed) {
            "execution_incomplete"
        } else if candidate.mode == HealScanMode::Deep
            && completion.scan_mode != HealScanMode::Deep
            && !(payload_verified && generation_comparison == Some(true))
        {
            "insufficient_scan"
        } else {
            match (candidate.identity.as_ref(), completion.object.as_ref()) {
                (Some(expected), Some(actual)) => {
                    Self::identity_scope(expected, &actual.identity, candidate.data_dir, actual.resolved_data_dir)
                }
                _ => "identity_unknown",
            }
        };
        let first_task_observation = !recent.counted;
        let deep_payload_verified_task = first_task_observation
            && completion.execution == Some(HealExecutionOutcome::Completed)
            && !completion.dry_run
            && payload_verified;
        recent.counted = true;
        Self {
            candidates: candidate.count,
            first_task_observation,
            generation_mismatch,
            deep_payload_verified_task,
            execution,
            disposition,
            scope,
            mode: candidate.mode,
        }
    }

    fn identity_scope(
        expected: &HealObjectIdentity,
        actual: &HealObjectIdentity,
        expected_data_dir: Option<Uuid>,
        resolved_data_dir: Option<Uuid>,
    ) -> &'static str {
        let expected_version = expected.version_id.as_deref().filter(|version| !version.is_empty());
        let actual_version = actual.version_id.as_deref().filter(|version| !version.is_empty());
        let expected_incarnation = expected.bucket_incarnation_id.filter(|id| !id.is_nil());
        let actual_incarnation = actual.bucket_incarnation_id.filter(|id| !id.is_nil());
        let expected_data_dir = expected_data_dir.filter(|id| !id.is_nil());
        let resolved_data_dir = resolved_data_dir.filter(|id| !id.is_nil());
        if expected.kind != actual.kind
            || expected.bucket != actual.bucket
            || expected.object != actual.object
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
            || expected_data_dir
                .zip(resolved_data_dir)
                .is_some_and(|(left, right)| left != right)
        {
            "identity_mismatch"
        } else if expected_version.is_none()
            || actual_version.is_none()
            || expected_incarnation.is_none()
            || actual_incarnation.is_none()
            || expected.pool_index.is_none()
            || actual.pool_index.is_none()
            || expected.set_index.is_none()
            || actual.set_index.is_none()
            || expected_data_dir.is_none()
            || resolved_data_dir.is_none()
        {
            "identity_unknown"
        } else {
            // Matching identities still do not bind content and leader epoch.
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
        // This is independent of scope: an insufficient scan can also have
        // resolved a different object generation.
        if self.generation_mismatch {
            counter!("rustfs_scanner_heal_candidate_generation_mismatch_total").increment(self.candidates);
        }
        if self.deep_payload_verified_task {
            counter!("rustfs_scanner_heal_task_deep_payload_verified_observations_total").increment(1);
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

    fn admit(&mut self, task_id: String, candidate: Candidate) -> Option<Settlement> {
        if let Some(recent) = self.recent.get_mut(&task_id) {
            let settlement = Settlement::new(candidate, recent);
            self.record_settlement(&settlement);
            return Some(settlement);
        }
        if let Some(pending) = self.pending.get_mut(&task_id) {
            if pending.first.matches(&candidate) {
                pending.first.count = pending.first.count.saturating_add(candidate.count);
            } else if let Some(index) = pending.others.iter().position(|group| group.matches(&candidate)) {
                let group = &mut pending.others[index];
                group.count = group.count.saturating_add(candidate.count);
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
                task_id,
                Pending {
                    first: candidate,
                    others: Vec::new(),
                },
            );
            self.pending_groups += 1;
        }
        None
    }

    fn complete(&mut self, completion: Arc<HealCompletion>, now: Instant) -> Option<Settlement> {
        if self.recent.contains_key(&completion.task_id) {
            return None;
        }
        let task_id = completion.task_id.clone();
        let mut recent = Recent {
            completion,
            counted: false,
        };
        let mut settled = None;
        if let Some(pending) = self.pending.remove(&task_id) {
            self.pending_groups -= 1 + pending.others.len();
            for candidate in pending.into_iter() {
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
        self.recent_order.push_back((now, task_id.clone()));
        self.recent.insert(task_id, recent);
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
            .flat_map(|(_, pending)| pending.into_iter())
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
            if pending.others.len() != previous_len {
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

type AdmissionSender = mpsc::Sender<(String, Candidate)>;

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
    // Installation takes only the registry lock. Readers release it before taking state; workers take only state.
    let mut installed = registry.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(handle) = installed.as_ref().filter(|handle| !handle.sender.is_closed()) {
        return handle.clone();
    }
    let state = Arc::new(Mutex::new(Observations::default()));
    let handle = {
        let (sender, mut admissions) = mpsc::channel(MAX_PENDING);
        let mut completions = subscribe_heal_completions();
        let observations = state.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    candidate = admissions.recv() => match candidate {
                        Some((id, candidate)) => {
                            let mut state = observations.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                            #[cfg(feature = "test-util")]
                            let probe_id = id.clone();
                            #[cfg(feature = "test-util")]
                            let probe_identity = candidate.identity.clone();
                            #[cfg(feature = "test-util")]
                            let probe_data_dir = candidate.data_dir;
                            #[cfg(feature = "test-util")]
                            let probe_mode = candidate.mode;
                            #[cfg(feature = "test-util")]
                            let probe_dry_run = candidate.dry_run;
                            #[cfg(feature = "test-util")]
                            let unknown_before: u64 = state.unknown_counts.values().copied().sum();
                            let _settled = state.admit(id, candidate);
                            #[cfg(feature = "test-util")]
                            if test_events().receiver_count() != 0 {
                                let pending = state.pending.get(&probe_id).is_some_and(|pending| {
                                    pending.iter().any(|candidate| {
                                        candidate.identity == probe_identity
                                            && candidate.data_dir == probe_data_dir
                                            && candidate.mode == probe_mode
                                            && candidate.dry_run == probe_dry_run
                                    })
                                });
                                let outcome = if _settled.is_some() {
                                    ScannerHealTestAdmission::CachedTerminal {
                                        data_dir: state.recent.get(&probe_id)
                                            .and_then(|recent| recent.completion.object.as_ref())
                                            .and_then(|object| object.resolved_data_dir),
                                    }
                                } else if pending {
                                    ScannerHealTestAdmission::Pending
                                } else if state.unknown_counts.values().copied().sum::<u64>() > unknown_before {
                                    ScannerHealTestAdmission::Unknown
                                } else {
                                    ScannerHealTestAdmission::Unaccounted
                                };
                                let _ = test_events().send(ScannerHealTestEvent::Admission {
                                    task_id: probe_id,
                                    candidate_data_dir: probe_data_dir,
                                    outcome,
                                });
                            }
                        },
                        None => break,
                    },
                    completion = completions.recv() => match completion {
                        Ok(completion) => {
                            let mut state = observations.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                            #[cfg(feature = "test-util")]
                            let probe_id = completion.task_id.clone();
                            #[cfg(feature = "test-util")]
                            let probe_data_dir = completion.object.as_ref().and_then(|object| object.resolved_data_dir);
                            #[cfg(feature = "test-util")]
                            let duplicate = state.recent.contains_key(&probe_id);
                            #[cfg(feature = "test-util")]
                            let pending_before: Vec<_> = state.pending.get(&probe_id)
                                .map(|pending| pending.iter().map(|candidate| candidate.data_dir).collect())
                                .unwrap_or_default();
                            let _settled = state.complete(completion, Instant::now());
                            #[cfg(feature = "test-util")]
                            if test_events().receiver_count() != 0 {
                                let remaining_candidate_data_dirs = state.pending.get(&probe_id)
                                    .map(|pending| pending.iter().map(|candidate| candidate.data_dir).collect())
                                    .unwrap_or_default();
                                let _ = test_events().send(ScannerHealTestEvent::Terminal {
                                    task_id: probe_id,
                                    terminal_data_dir: probe_data_dir,
                                    duplicate,
                                    settled_candidate_data_dirs: if _settled.is_some() { pending_before } else { Vec::new() },
                                    remaining_candidate_data_dirs,
                                });
                            }
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

/// Capture only the identity actually available at discovery, before dispatch.
pub(crate) fn prepare(request: &HealChannelRequest, data_dir: Option<Uuid>) -> Option<Candidate> {
    let _ = observer();
    Candidate::from_request(request, data_dir)
}

pub(crate) fn observe(candidate: Option<Candidate>, receipt: Result<&HealAdmissionReceipt, ()>) {
    observe_on(&observer(), candidate, receipt);
}

fn observe_on(observer: &ObserverHandle, candidate: Option<Candidate>, receipt: Result<&HealAdmissionReceipt, ()>) {
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
            if observer.sender.try_send((task_id.clone(), candidate)).is_err() {
                observer.record_unknown("observer_full_or_closed", 1);
            }
        }
        _ => observer.record_unknown("not_admitted", 1),
    }
}

#[cfg(test)]
mod tests;
