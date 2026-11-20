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

//! Read-only coordinator observations from the configured members of this deployment.
use crate::admin::runtime_sources::AppContext;
use crate::admin::storage_api::metrics::{CollectMetricsOpts, MetricType, collect_local_metrics};
use futures::{StreamExt, stream};
use jiff::Timestamp;
use rustfs_madmin::metrics::{ScannerAttemptSample, ScannerNodeSample};
use serde::Serialize;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::sync::OnceCell;
use tokio::time::{Instant, timeout_at};

const PROBE_WINDOW: Duration = Duration::from_secs(2);
const FENCE_WINDOW: Duration = Duration::from_secs(1);
const REUSE_WINDOW: Duration = Duration::from_secs(2);
const PROBE_CONCURRENCY: usize = 8;

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum State {
    Scanning,
    Idle,
    Unknown,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct Summary {
    state: State,
    nodes_total: usize,
    nodes_responded: usize,
    sampled_at: Timestamp,
    #[serde(skip_serializing_if = "Option::is_none")]
    current: Option<Current>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_attempt: Option<LastAttempt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_success_at: Option<Timestamp>,
}

#[derive(Clone, Debug, Serialize)]
struct Current {
    cycle: u64,
    started_at: Timestamp,
    objects_scanned: u64,
    leader: String,
}

#[derive(Clone, Debug, Serialize)]
struct LastAttempt {
    cycle: u64,
    result: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    partial_reason: Option<String>,
    duration_seconds: f64,
}

struct DurableCycle {
    epoch: u64,
    cycle: u64,
    last_success_at: Option<Timestamp>,
}

fn summarize(total: usize, samples: &[(String, ScannerNodeSample)], durable: Option<DurableCycle>) -> Summary {
    let fence = durable.as_ref().map(|record| (record.epoch, record.cycle));
    let covered = total > 0 && total == samples.len();
    let active_count = samples.iter().filter(|(_, node)| node.active).count();
    let mut holders = samples.iter().filter(|(_, node)| node.leader);
    let holder = holders.next();
    let conflict = holders.next().is_some();
    let mut attempts = samples
        .iter()
        .filter_map(|(_, node)| node.last_attempt.as_ref())
        .collect::<Vec<_>>();
    attempts.sort_unstable_by_key(|a| (a.epoch, a.cycle));
    let mut attempts = attempts.into_iter().rev();
    let latest = attempts.next();
    let tied = latest
        .zip(attempts.next())
        .is_some_and(|(a, b)| (a.epoch, a.cycle) == (b.epoch, b.cycle));
    let fenced_leader = holder.filter(|(_, node)| node.epoch.is_some() && node.epoch == fence.map(|(epoch, _)| epoch));
    let current = fenced_leader.and_then(|(host, node)| {
        let epoch = node.epoch?;
        let started_at = node.started_at?;
        if fence != Some((epoch, node.cycle))
            || conflict
            || active_count != 1
            || !node.active
            || latest.is_some_and(|a| (a.epoch, a.cycle) > (epoch, node.cycle))
        {
            return None;
        }
        Some(Current {
            cycle: node.cycle,
            started_at,
            objects_scanned: node.objects_scanned,
            leader: host.clone(),
        })
    });
    let state = if current.is_some() {
        State::Scanning
    } else if covered && active_count == 0 && !conflict && fenced_leader.is_some() {
        State::Idle
    } else {
        State::Unknown
    };
    let last_attempt = latest
        .filter(|a| covered && !tied && fence.is_some_and(|head| (a.epoch, a.cycle) <= head))
        .map(|a: &ScannerAttemptSample| LastAttempt {
            cycle: a.cycle,
            result: a.result.clone(),
            partial_reason: a.partial_reason.clone(),
            duration_seconds: a.duration_seconds,
        });
    Summary {
        state,
        nodes_total: total,
        nodes_responded: samples.len(),
        sampled_at: Timestamp::now(),
        current,
        last_attempt,
        last_success_at: durable.and_then(|record| record.last_success_at),
    }
}

async fn probe(context: &AppContext) -> Summary {
    let Some(endpoints) = context.endpoints().handle() else { return summarize(0, &[], None) };
    let nodes = endpoints.get_nodes();
    let total = nodes.len();
    let notification = context.notification_system().handle();
    let deadline = Instant::now() + PROBE_WINDOW;
    let mut samples = stream::iter(nodes)
        .map(|node| {
            let notification = notification.as_deref();
            async move {
                let opts = CollectMetricsOpts::default();
                let host = if node.grid_host.is_empty() && node.is_local {
                    "local".to_owned()
                } else {
                    node.grid_host
                };
                let fetch = async {
                    if node.is_local {
                        collect_local_metrics(MetricType::SCANNER_SUMMARY, &opts).await.scanner_node
                    } else {
                        notification?
                            .peer_client_for_grid_host(&host)?
                            .get_metrics(MetricType::SCANNER_SUMMARY, &opts)
                            .await
                            .ok()?
                            .scanner_node
                    }
                };
                let sample = timeout_at(deadline, fetch).await.ok().flatten()?;
                Some((host, sample))
            }
        })
        .buffer_unordered(PROBE_CONCURRENCY)
        .filter_map(|sample| async { sample })
        .collect::<Vec<_>>()
        .await;
    // Require distinct configured identities, even if a malformed endpoint table
    // contains overlapping aliases. Missing coverage cannot establish idleness.
    samples.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    let responses = samples.len();
    samples.dedup_by(|a, b| a.0 == b.0);
    if responses != samples.len() {
        return summarize(total, &samples, None);
    }
    let fence = if let Some(store) = crate::admin::runtime_sources::current_object_store_handle_for_context(Some(context)) {
        timeout_at(deadline + FENCE_WINDOW, rustfs_scanner::scanner::read_scanner_status_fence(store))
            .await
            .ok()
            .and_then(Result::ok)
    } else {
        None
    };
    let durable = fence.map(|(epoch, record)| DurableCycle {
        epoch,
        cycle: record.next,
        last_success_at: record.last_success_at(),
    });
    summarize(total, &samples, durable)
}

struct Cache {
    owner: Weak<AppContext>,
    observation: Arc<OnceCell<(Instant, Summary)>>,
}

/// Concurrent Console refreshes share a cancellable, bounded fan-out. Never reuse a
/// sample from another server context or refresh its observation timestamp on a hit.
pub(super) async fn read(context: Option<Arc<AppContext>>) -> Summary {
    static CACHE: Mutex<Option<Cache>> = Mutex::new(None);
    let Some(context) = context else { return summarize(0, &[], None) };
    let owner = Arc::downgrade(&context);
    let observation = {
        let mut cache = CACHE.lock().unwrap_or_else(|e| e.into_inner());
        let cached = cache.as_ref().filter(|entry| {
            entry.owner.ptr_eq(&owner) && entry.observation.get().is_none_or(|(time, _)| time.elapsed() < REUSE_WINDOW)
        });
        if let Some(entry) = cached {
            Arc::clone(&entry.observation)
        } else {
            let observation = Arc::new(OnceCell::new());
            *cache = Some(Cache {
                owner,
                observation: Arc::clone(&observation),
            });
            observation
        }
    };
    observation
        .get_or_init(|| async {
            let summary = probe(&context).await;
            (Instant::now(), summary)
        })
        .await
        .1
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn durable(epoch: u64, cycle: u64) -> Option<DurableCycle> {
        Some(DurableCycle {
            epoch,
            cycle,
            last_success_at: None,
        })
    }

    fn with_fence(total: usize, samples: &[(String, ScannerNodeSample)]) -> Summary {
        let cycle = samples.iter().find(|(_, node)| node.leader).map_or(0, |(_, node)| node.cycle);
        summarize(total, samples, durable(9, cycle))
    }

    fn node(name: &str, sample: ScannerNodeSample) -> (String, ScannerNodeSample) {
        (name.to_owned(), sample)
    }

    fn leader(cycle: u64) -> ScannerNodeSample {
        ScannerNodeSample {
            active: true,
            leader: true,
            epoch: Some(9),
            cycle,
            started_at: Some(Timestamp::UNIX_EPOCH),
            objects_scanned: 301_363,
            ..Default::default()
        }
    }

    fn partial(epoch: u64, cycle: u64) -> ScannerAttemptSample {
        ScannerAttemptSample {
            epoch,
            cycle,
            result: "partial".into(),
            partial_reason: Some("runtime".into()),
            duration_seconds: 1801.225,
        }
    }

    #[test]
    fn cluster_scanner_only_reports_success_from_durable_history() {
        let when = Timestamp::constant(1_700_000_000, 0);
        // An older peer may send its speculative timestamp even if persistence failed.
        let mut peer = serde_json::to_value(leader(40)).expect("peer sample");
        peer["last_success_at"] = serde_json::json!(when);
        let samples = [node("owner", serde_json::from_value(peer).expect("compatible peer"))];
        assert!(summarize(1, &samples, None).last_success_at.is_none());
        assert!(summarize(1, &samples, durable(9, 40)).last_success_at.is_none());
        let confirmed = Timestamp::constant(1_699_000_000, 0);
        let record = DurableCycle {
            epoch: 9,
            cycle: 40,
            last_success_at: Some(confirmed),
        };
        assert_eq!(summarize(1, &samples, Some(record)).last_success_at, Some(confirmed));
    }

    #[test]
    fn cluster_scanner_requires_a_current_persisted_fence_to_trust_a_leader() {
        let samples = [node("former", leader(40))];
        assert_eq!(summarize(4, &samples, None).state, State::Unknown);
        assert_eq!(summarize(4, &samples, durable(10, 41)).state, State::Unknown);
        assert_eq!(summarize(4, &samples, durable(9, 41)).state, State::Unknown);
        assert_eq!(summarize(4, &samples, durable(9, 40)).state, State::Scanning);
        let idle = [node(
            "owner",
            ScannerNodeSample {
                leader: true,
                epoch: Some(9),
                ..Default::default()
            },
        )];
        assert_eq!(summarize(1, &idle, durable(9, 41)).state, State::Idle);
        assert_eq!(summarize(2, &idle, durable(9, 41)).state, State::Unknown);
        assert_eq!(summarize(1, &idle, None).state, State::Unknown);
    }

    #[test]
    fn cluster_scanner_returns_leader_progress_and_previous_attempt_on_a_follower_request() {
        let mut old_leader = ScannerNodeSample {
            last_attempt: Some(partial(8, 39)),
            ..Default::default()
        };
        old_leader.objects_scanned = 999;
        let status = with_fence(2, &[node("receiving-node", old_leader), node("scan-leader", leader(40))]);
        assert_eq!(status.state, State::Scanning);
        assert_eq!((status.nodes_responded, status.nodes_total), (2, 2));
        let current = status.current.expect("leader is known");
        assert_eq!(
            (current.leader.as_str(), current.cycle, current.objects_scanned),
            ("scan-leader", 40, 301_363)
        );
        let attempt = status.last_attempt.expect("completed partial");
        assert_eq!(
            (attempt.cycle, attempt.result.as_str(), attempt.partial_reason.as_deref()),
            (39, "partial", Some("runtime"))
        );
        assert!(status.last_success_at.is_none());
    }

    #[test]
    fn cluster_scanner_missing_peers_allow_positive_activity_but_cannot_establish_idle() {
        let active = with_fence(4, &[node("leader", leader(0))]);
        assert_eq!(active.state, State::Scanning);
        assert_eq!(active.current.expect("first cycle").cycle, 0);
        assert_eq!(with_fence(4, &[node("follower", Default::default())]).state, State::Unknown);
        assert_eq!(with_fence(1, &[node("local", Default::default())]).state, State::Unknown);
        assert_eq!(with_fence(0, &[]).state, State::Unknown);
    }

    #[test]
    fn cluster_scanner_withholds_conflicting_or_unidentified_coordinators() {
        let ambiguous = with_fence(2, &[node("former", leader(39)), node("new", leader(40))]);
        assert_eq!(ambiguous.state, State::Unknown);
        assert!(ambiguous.current.is_none());
        let unverified = ScannerNodeSample {
            epoch: None,
            ..leader(40)
        };
        assert_eq!(with_fence(1, &[node("unknown", unverified)]).state, State::Unknown);
        let later = ScannerNodeSample {
            last_attempt: Some(partial(10, 41)),
            ..Default::default()
        };
        assert_eq!(with_fence(2, &[node("stale", leader(40)), node("new", later)]).state, State::Unknown);
    }

    #[test]
    fn cluster_scanner_selects_latest_attempt_by_generation_without_fabricating_success() {
        let a = ScannerNodeSample {
            last_attempt: Some(partial(9, 500)),
            ..Default::default()
        };
        let b = ScannerNodeSample {
            last_attempt: Some(partial(10, 2)),
            ..Default::default()
        };
        let members = [node("new", b), node("old", a)];
        assert!(summarize(2, &members, durable(9, 501)).last_attempt.is_none());
        let status = summarize(2, &members, durable(10, 3));
        assert_eq!(status.last_attempt.expect("latest epoch").cycle, 2);
        let json = serde_json::to_value(status.state).expect("state");
        assert_eq!(json, "unknown");
        let json = serde_json::to_value(with_fence(2, &[node("one", Default::default())])).expect("frame");
        assert!(json.get("last_success_at").is_none() && json.get("last_attempt").is_none() && json.get("current").is_none());
    }
}
