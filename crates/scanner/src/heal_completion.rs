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
    outcome::{HealExecutionOutcome, HealObjectDisposition, HealObjectIdentity, HealObjectKind},
};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::{broadcast, mpsc};

const MAX_PENDING: usize = 1024;
const MAX_RECENT: usize = 1024;
const PENDING_TTL: Duration = Duration::from_secs(60 * 60);
const RECENT_TTL: Duration = Duration::from_secs(10 * 60);

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
}

struct Recent {
    completion: Arc<HealCompletion>,
    counted: bool,
}

#[derive(Default)]
struct Observations {
    pending: HashMap<String, Candidate>,
    recent: HashMap<String, Recent>,
    recent_order: VecDeque<(Instant, String)>,
}

#[derive(Debug)]
struct Settlement {
    candidates: u64,
    first_task_observation: bool,
    disposition: &'static str,
    scope: &'static str,
    mode: HealScanMode,
}

impl Settlement {
    fn new(candidate: Candidate, recent: &mut Recent) -> Self {
        let completion = &recent.completion;
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
                (Some(expected), Some(actual)) if expected != &actual.identity => "identity_mismatch",
                (Some(expected), Some(_))
                    if expected.bucket_incarnation_id.is_some_and(|id| !id.is_nil())
                        && expected.pool_index.is_some()
                        && expected.set_index.is_some()
                        && expected.version_id.as_ref().is_some_and(|version| !version.is_empty()) =>
                {
                    // Even an exact version and VerifiedHealthy do not bind the
                    // candidate's content and leader epoch with today's contract.
                    "content_and_leader_unknown"
                }
                _ => "identity_unknown",
            }
        };
        let first_task_observation = !recent.counted;
        recent.counted = true;
        Self {
            candidates: candidate.count,
            first_task_observation,
            disposition,
            scope,
            mode: candidate.mode,
        }
    }

    fn record(self) {
        counter!("rustfs_scanner_heal_candidate_terminal_total",
            "disposition" => self.disposition, "scope" => self.scope, "scan_mode" => self.mode.as_str())
        .increment(self.candidates);
        if self.first_task_observation {
            counter!("rustfs_scanner_heal_task_terminal_observations_total").increment(1);
        }
        // No terminal here can certify payload coverage. In particular repaired,
        // metadata-healthy and authoritative absence never imply verified bytes.
        record_unknown(self.scope, self.candidates);
    }
}

fn record_unknown(reason: &'static str, count: u64) {
    counter!("rustfs_scanner_heal_candidate_unknown_total", "reason" => reason).increment(count);
}

impl Observations {
    fn admit(&mut self, task_id: String, candidate: Candidate) -> Option<Settlement> {
        if let Some(recent) = self.recent.get_mut(&task_id) {
            return Some(Settlement::new(candidate, recent));
        }
        if let Some(pending) = self.pending.get_mut(&task_id) {
            pending.count = pending.count.saturating_add(candidate.count);
            if pending.identity != candidate.identity {
                pending.identity = None;
            }
            if candidate.mode == HealScanMode::Deep {
                pending.mode = HealScanMode::Deep;
            }
            pending.dry_run |= candidate.dry_run;
        } else if self.pending.len() < MAX_PENDING {
            self.pending.insert(task_id, candidate);
        } else {
            record_unknown("pending_full", candidate.count);
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
        let settled = self
            .pending
            .remove(&task_id)
            .map(|candidate| Settlement::new(candidate, &mut recent));
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
        for (_, candidate) in self.pending.drain() {
            record_unknown("completion_lost", candidate.count);
        }
        self.recent.clear();
        self.recent_order.clear();
    }

    fn maintain(&mut self, now: Instant) {
        self.pending.retain(|_, candidate| {
            if now.saturating_duration_since(candidate.first_seen) >= PENDING_TTL {
                record_unknown("expired", candidate.count);
                false
            } else {
                true
            }
        });
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
            .map(|candidate| now.saturating_duration_since(candidate.first_seen))
            .max();
        gauge!("rustfs_scanner_heal_pending_tasks").set(self.pending.len() as f64);
        gauge!("rustfs_scanner_heal_pending_oldest_seconds").set(oldest.unwrap_or_default().as_secs_f64());
    }
}

type AdmissionSender = mpsc::Sender<(String, Candidate)>;

fn observer_from(registry: &Mutex<Option<AdmissionSender>>) -> AdmissionSender {
    // This lock protects installation only; no I/O or await occurs while held.
    let mut installed = registry.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(sender) = installed.as_ref().filter(|sender| !sender.is_closed()) {
        return sender.clone();
    }
    let sender = {
        let (sender, mut admissions) = mpsc::channel(MAX_PENDING);
        let mut completions = subscribe_heal_completions();
        tokio::spawn(async move {
            let mut observations = Observations::default();
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                let settlement = tokio::select! {
                    candidate = admissions.recv() => match candidate {
                        Some((id, candidate)) => observations.admit(id, candidate),
                        None => break,
                    },
                    completion = completions.recv() => match completion {
                        Ok(completion) => observations.complete(completion, Instant::now()),
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            counter!("rustfs_scanner_heal_completion_lost_total").increment(skipped);
                            observations.lost();
                            None
                        },
                        Err(broadcast::error::RecvError::Closed) => { observations.lost(); break },
                    },
                    _ = tick.tick() => { observations.maintain(Instant::now()); None },
                };
                if let Some(settlement) = settlement {
                    settlement.record();
                }
            }
        });
        sender
    };
    *installed = Some(sender.clone());
    sender
}

fn observer() -> AdmissionSender {
    static OBSERVER: Mutex<Option<AdmissionSender>> = Mutex::new(None);
    observer_from(&OBSERVER)
}

/// Capture only the identity actually available at discovery, before dispatch.
pub(crate) fn prepare(request: &HealChannelRequest) -> Option<Candidate> {
    let _ = observer();
    Candidate::from_request(request)
}

pub(crate) fn observe(candidate: Option<Candidate>, receipt: Result<&HealAdmissionReceipt, ()>) {
    observe_on(&observer(), candidate, receipt);
}

fn observe_on(sender: &AdmissionSender, candidate: Option<Candidate>, receipt: Result<&HealAdmissionReceipt, ()>) {
    match receipt {
        Ok(receipt) if receipt.result.is_admitted() => {
            let Some(candidate) = candidate else {
                record_unknown("identity_too_large", 1);
                return;
            };
            let task_id = &receipt.task_id;
            if task_id.is_empty() || task_id.len() > 128 {
                record_unknown("missing_task_id", 1);
                return;
            }
            if sender.try_send((task_id.clone(), candidate)).is_err() {
                record_unknown("observer_full_or_closed", 1);
            }
        }
        _ => record_unknown("not_admitted", 1),
    }
}

#[cfg(test)]
mod tests;
