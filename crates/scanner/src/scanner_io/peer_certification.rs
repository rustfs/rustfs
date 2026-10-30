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

use super::*;
use crate::scanner::{
    ScannerActivitySnapshot, ScannerDirtyUsageAcknowledgement, ScannerDirtyUsageAcknowledgementKind,
    scanner_activity_allows_usage_publication, scanner_activity_dirty_usage_state_for_host, scanner_activity_guardrail_veto,
    scanner_activity_publication_lease_targets,
};
use crate::storage_api::EcstoreScannerPublicationLeaseGrant;

/// Only worker results received in this cycle enter this map. A carried value
/// or a cold bucket copied from the old set root is not a completion.
pub(super) type ScannerBucketCompletions = HashMap<DataUsageCacheSource, HashMap<String, uuid::Uuid>>;

#[derive(Debug)]
pub(super) struct ScannerRemoteDirtyUsageWindow {
    pub(super) before: ScannerActivitySnapshot,
    pub(super) after: ScannerActivitySnapshot,
    pub(super) peers: BTreeMap<String, EcstoreScannerPeerDirtyUsageSnapshot>,
}

fn remote_generations_advance(before: &ScannerActivitySnapshot, after: &ScannerActivitySnapshot) -> bool {
    scanner_activity_allows_usage_publication(before)
        && scanner_activity_allows_usage_publication(after)
        && scanner_activity_guardrail_veto(before, after).is_none()
        && scanner_activity_publication_lease_targets(before).iter().all(|(host, _, _)| {
            let Some((_, before_generation, _)) = scanner_activity_dirty_usage_state_for_host(before, host) else {
                return false;
            };
            scanner_activity_dirty_usage_state_for_host(after, host)
                .is_some_and(|(_, after_generation, _)| before_generation <= after_generation && after_generation < u64::MAX)
        })
}

impl ScannerRemoteDirtyUsageWindow {
    /// The caller obtains these in order: before activity, dirty snapshot,
    /// after activity. The generation can advance without waiting for silence.
    pub(super) fn verify(
        before: ScannerActivitySnapshot,
        snapshots: Vec<(String, EcstoreScannerPeerDirtyUsageSnapshot)>,
        after: ScannerActivitySnapshot,
    ) -> Option<Self> {
        let expected = scanner_activity_publication_lease_targets(&before);
        if expected.is_empty() || expected.len() != snapshots.len() || !remote_generations_advance(&before, &after) {
            return None;
        }
        let mut peers = BTreeMap::new();
        for (host, snapshot) in snapshots {
            let (instance_id, lower, lower_pending) = scanner_activity_dirty_usage_state_for_host(&before, &host)?;
            let (_, upper, upper_pending) = scanner_activity_dirty_usage_state_for_host(&after, &host)?;
            let pending = !snapshot.buckets.is_empty();
            if host.is_empty()
                || snapshot.instance_id != instance_id
                || uuid::Uuid::parse_str(&snapshot.owner_id)
                    .ok()
                    .is_none_or(|owner| owner.is_nil())
                || uuid::Uuid::parse_str(&snapshot.instance_id)
                    .ok()
                    .is_none_or(|instance| instance.is_nil())
                || snapshot.protocol_version != crate::SCANNER_DIRTY_USAGE_SNAPSHOT_PROTOCOL_VERSION
                || !snapshot.complete
                || snapshot.buckets.len() > crate::SCANNER_DIRTY_USAGE_SNAPSHOT_MAX_ENTRIES
                || snapshot.pending_bucket_count != u64::try_from(snapshot.buckets.len()).ok()?
                || !(lower <= snapshot.generation && snapshot.generation <= upper && upper < u64::MAX)
                || (lower == snapshot.generation && lower_pending != pending)
                || (upper == snapshot.generation && upper_pending != pending)
                || snapshot.buckets.iter().any(|(bucket, entry)| {
                    bucket.is_empty()
                        || bucket == DATA_USAGE_ROOT
                        || entry.bucket_incarnation.is_nil()
                        || entry.generation == 0
                        || entry.generation > snapshot.generation
                })
                || peers.insert(host, snapshot).is_some()
            {
                return None;
            }
        }
        (peers.len() == expected.len() && expected.iter().all(|(host, _, _)| peers.contains_key(host))).then_some(Self {
            before,
            after,
            peers,
        })
    }

    pub(super) fn activity_for_walk(&self) -> Arc<ScannerActivitySnapshot> {
        Arc::new(self.after.clone())
    }

    pub(super) fn strict_remote_usage(&self) -> Option<VerifiedRemoteDirtyUsage> {
        if crate::scanner::scanner_peer_dirty_usage_moved(&self.before, &self.after).is_some() {
            return None;
        }
        let expected = self
            .peers
            .keys()
            .map(|host| {
                let (instance_id, generation, pending) = scanner_activity_dirty_usage_state_for_host(&self.before, host)?;
                Some((
                    host.clone(),
                    ScannerPeerDirtyUsageExpectation {
                        instance_id: instance_id.to_string(),
                        generation,
                        pending,
                    },
                ))
            })
            .collect::<Option<HashMap<_, _>>>()?;
        verified_remote_dirty_usage(&expected, self.peers.iter().map(|(host, peer)| (host.clone(), peer.clone())).collect())
    }

    #[cfg(feature = "e2e-test-hooks")]
    pub(super) fn observation(&self, epoch: u64, cycle: u64) -> serde_json::Value {
        serde_json::json!({
            "epoch": epoch,
            "cycle": cycle,
            "verified": true,
            "activity_before": crate::e2e_barrier::activity_observations(&self.before),
            "activity_after": crate::e2e_barrier::activity_observations(&self.after),
            "peers": self.peers.iter().map(|(host, peer)| serde_json::json!({
                "host": host,
                "owner_id": peer.owner_id,
                "instance_id": peer.instance_id,
                "generation": peer.generation,
                "protocol_version": peer.protocol_version,
                "complete": peer.complete,
                "pending_bucket_count": peer.pending_bucket_count,
                "buckets": peer.buckets.iter().map(|(bucket, entry)| serde_json::json!({
                    "bucket": bucket,
                    "bucket_incarnation": entry.bucket_incarnation,
                    "generation": entry.generation,
                })).collect::<Vec<_>>(),
            })).collect::<Vec<_>>(),
        })
    }

    pub(super) fn dirty_buckets(&self) -> HashSet<String> {
        self.peers.values().flat_map(|peer| peer.buckets.keys().cloned()).collect()
    }

    pub(super) fn acknowledgements(&self) -> Vec<ScannerDirtyUsageAcknowledgement> {
        self.peers
            .iter()
            .filter(|(_, peer)| !peer.buckets.is_empty())
            .map(|(host, peer)| ScannerDirtyUsageAcknowledgement {
                host: host.clone(),
                instance_id: peer.instance_id.clone(),
                kind: ScannerDirtyUsageAcknowledgementKind::Scoped {
                    owner_id: peer.owner_id.clone(),
                    entries: peer
                        .buckets
                        .iter()
                        .map(|(bucket, entry)| crate::storage_api::EcstoreScannerScopedDirtyUsageAckEntry {
                            bucket: bucket.clone(),
                            bucket_incarnation: entry.bucket_incarnation,
                            generation: entry.generation,
                        })
                        .collect(),
                },
            })
            .collect()
    }
}

/// A complete producer proof includes clean peers even when it yields no ACK.
/// The start and post-walk maps remain sealed until publication and lease
/// validation; post-lease writes are retained by exact receiver comparison.
#[derive(Debug)]
pub(crate) struct ScannerRemoteDirtyUsageCertification {
    pub(super) start: ScannerRemoteDirtyUsageWindow,
    pub(super) post_walk: ScannerRemoteDirtyUsageWindow,
}

impl ScannerRemoteDirtyUsageCertification {
    pub(super) fn new(start: ScannerRemoteDirtyUsageWindow, post_walk: ScannerRemoteDirtyUsageWindow) -> Option<Self> {
        if !remote_generations_advance(&start.after, &post_walk.before)
            || start.peers.len() != post_walk.peers.len()
            || start.peers.iter().any(|(host, before)| {
                post_walk.peers.get(host).is_none_or(|after| {
                    before.owner_id != after.owner_id
                        || before.instance_id != after.instance_id
                        || before.protocol_version != after.protocol_version
                        || before.generation > after.generation
                })
            })
        {
            return None;
        }
        Some(Self { start, post_walk })
    }

    #[cfg(test)]
    pub(crate) fn for_tests(
        activity: ScannerActivitySnapshot,
        peers: Vec<(String, EcstoreScannerPeerDirtyUsageSnapshot)>,
    ) -> Option<Self> {
        let start = ScannerRemoteDirtyUsageWindow::verify(activity.clone(), peers.clone(), activity.clone())?;
        let post = ScannerRemoteDirtyUsageWindow::verify(activity.clone(), peers, activity)?;
        Self::new(start, post)
    }

    pub(super) fn classify_buckets(
        &self,
        expected_sources: &HashSet<DataUsageCacheSource>,
        completions: &ScannerBucketCompletions,
        uncertified: &HashSet<String>,
    ) -> (Vec<ScannerDirtyUsageAcknowledgement>, ScannerCyclePendingBuckets) {
        let mut buckets = self.start.dirty_buckets();
        buckets.extend(self.post_walk.dirty_buckets());
        let sources_complete = !expected_sources.is_empty()
            && completions.len() == expected_sources.len()
            && expected_sources.iter().all(|source| completions.contains_key(source));
        let pending = buckets
            .iter()
            .filter(|bucket| {
                !sources_complete
                    || uncertified.contains(*bucket)
                    || self.start.peers.iter().any(|(host, before)| {
                        let start = before.buckets.get(*bucket);
                        let post_walk = self.post_walk.peers[host].buckets.get(*bucket);
                        start != post_walk
                            || start.is_some_and(|entry| {
                                expected_sources.iter().any(|source| {
                                    completions.get(source).and_then(|set| set.get(*bucket)) != Some(&entry.bucket_incarnation)
                                })
                            })
                    })
            })
            .cloned()
            .collect::<ScannerCyclePendingBuckets>();
        let acknowledgements = self
            .start
            .acknowledgements()
            .into_iter()
            .filter_map(|mut acknowledgement| {
                let ScannerDirtyUsageAcknowledgementKind::Scoped { entries, .. } = &mut acknowledgement.kind else {
                    return None;
                };
                entries.retain(|entry| !pending.contains(&entry.bucket));
                (!entries.is_empty()).then_some(acknowledgement)
            })
            .collect();
        (acknowledgements, pending)
    }

    pub(crate) fn matches_activity(&self, activity: &ScannerActivitySnapshot) -> bool {
        remote_generations_advance(&self.post_walk.after, activity)
    }

    pub(crate) fn matches_publication_leases(&self, grants: &[EcstoreScannerPublicationLeaseGrant]) -> bool {
        if grants.len() != self.post_walk.peers.len() {
            return false;
        }
        let targets = scanner_activity_publication_lease_targets(&self.post_walk.after)
            .into_iter()
            .map(|(host, instance, movement)| (host, (instance, movement)))
            .collect::<BTreeMap<_, _>>();
        let mut hosts = HashSet::new();
        grants.iter().all(|grant| {
            let Some(peer) = self.post_walk.peers.get(&grant.host) else {
                return false;
            };
            hosts.insert(grant.host.as_str())
                && grant.lease.is_valid()
                && !grant.lease.observational_only
                && !grant.lease.token.is_nil()
                && grant.lease.owner_id == peer.owner_id
                && grant.lease.session_id == peer.instance_id
                && targets.get(&grant.host).is_some_and(|(instance, movement)| {
                    instance == &grant.lease.session_id && *movement == grant.lease.movement_generation
                })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOT: uuid::Uuid = uuid::Uuid::from_u128(71);
    const COLD: uuid::Uuid = uuid::Uuid::from_u128(72);

    fn peers(generation: u64, hot_generation: u64) -> Vec<(String, EcstoreScannerPeerDirtyUsageSnapshot)> {
        ["peer-a", "peer-b"]
            .into_iter()
            .enumerate()
            .map(|(index, host)| {
                let buckets = if index == 0 {
                    BTreeMap::from([
                        (
                            "cold-bucket".to_string(),
                            crate::storage_api::EcstoreScannerPeerDirtyUsageBucket {
                                bucket_incarnation: COLD,
                                generation: 8,
                            },
                        ),
                        (
                            "hot-bucket".to_string(),
                            crate::storage_api::EcstoreScannerPeerDirtyUsageBucket {
                                bucket_incarnation: HOT,
                                generation: hot_generation,
                            },
                        ),
                    ])
                } else {
                    BTreeMap::new()
                };
                (
                    host.to_string(),
                    EcstoreScannerPeerDirtyUsageSnapshot {
                        owner_id: uuid::Uuid::from_u128(101 + index as u128).simple().to_string(),
                        instance_id: uuid::Uuid::from_u128(201 + index as u128).simple().to_string(),
                        generation: if index == 0 { generation } else { 7 },
                        pending_bucket_count: buckets.len() as u64,
                        protocol_version: crate::SCANNER_DIRTY_USAGE_SNAPSHOT_PROTOCOL_VERSION,
                        complete: true,
                        buckets,
                    },
                )
            })
            .collect()
    }

    fn activity(generation: u64) -> ScannerActivitySnapshot {
        peers(generation, generation)
            .into_iter()
            .map(|(host, peer)| {
                (
                    host,
                    crate::scanner::scanner_node_activity_for_tests(
                        &peer.instance_id,
                        peer.generation,
                        peer.generation,
                        !peer.buckets.is_empty(),
                    ),
                )
            })
            .collect()
    }

    fn window(lower: u64, generation: u64, upper: u64) -> ScannerRemoteDirtyUsageWindow {
        ScannerRemoteDirtyUsageWindow::verify(activity(lower), peers(generation, generation), activity(upper))
            .expect("ordered complete producer window")
    }

    fn completions() -> (HashSet<DataUsageCacheSource>, ScannerBucketCompletions) {
        let sources = HashSet::from([DataUsageCacheSource::new(0, 0), DataUsageCacheSource::new(0, 1)]);
        let completed = sources
            .iter()
            .map(|source| {
                (
                    *source,
                    HashMap::from([("cold-bucket".to_string(), COLD), ("hot-bucket".to_string(), HOT)]),
                )
            })
            .collect();
        (sources, completed)
    }

    #[test]
    fn hot_writes_cross_both_bounded_windows_while_the_clean_peer_stays_in_the_proof() {
        let start = window(9, 10, 11);
        assert!(start.strict_remote_usage().is_none(), "the relaxed window grants no segment reuse proof");
        let proof = ScannerRemoteDirtyUsageCertification::new(start, window(12, 13, 14)).expect("same producers");
        let (sources, completed) = completions();
        let (acks, pending) = proof.classify_buckets(&sources, &completed, &HashSet::new());
        assert_eq!(pending, BTreeSet::from(["hot-bucket".to_string()]));
        assert_eq!(acks.len(), 1);
        assert_eq!(proof.post_walk.peers.len(), 2);
        assert!(proof.post_walk.peers["peer-b"].buckets.is_empty());
        let ScannerDirtyUsageAcknowledgementKind::Scoped { entries, .. } = &acks[0].kind else {
            panic!("scoped ACK")
        };
        assert_eq!(entries.len(), 1);
        assert_eq!(
            (entries[0].bucket.as_str(), entries[0].bucket_incarnation, entries[0].generation),
            ("cold-bucket", COLD, 8)
        );
    }

    #[test]
    fn producer_windows_reject_missing_duplicate_stale_future_overflow_and_incomplete_evidence() {
        for case in [
            "missing_peer",
            "duplicate_peer",
            "old_snapshot",
            "future_snapshot",
            "overflow",
            "owner",
            "instance",
            "protocol",
            "incomplete",
            "count",
            "future_entry",
            "nil_incarnation",
            "empty_bucket",
            "entry_limit",
            "pending",
        ] {
            let mut snapshots = peers(10, 10);
            let before = activity(9);
            let mut after = activity(11);
            match case {
                "missing_peer" => {
                    snapshots.pop();
                }
                "duplicate_peer" => {
                    snapshots[1] = snapshots[0].clone();
                }
                "old_snapshot" => snapshots[0].1.generation = 8,
                "future_snapshot" => snapshots[0].1.generation = 12,
                "overflow" => after = activity(u64::MAX),
                "owner" => snapshots[0].1.owner_id = uuid::Uuid::nil().to_string(),
                "instance" => snapshots[0].1.instance_id = uuid::Uuid::from_u128(999).to_string(),
                "protocol" => snapshots[0].1.protocol_version += 1,
                "incomplete" => snapshots[0].1.complete = false,
                "count" => snapshots[0].1.pending_bucket_count += 1,
                "future_entry" => snapshots[0].1.buckets.get_mut("cold-bucket").expect("cold").generation = 11,
                "nil_incarnation" => {
                    snapshots[0]
                        .1
                        .buckets
                        .get_mut("cold-bucket")
                        .expect("cold")
                        .bucket_incarnation = uuid::Uuid::nil()
                }
                "empty_bucket" => {
                    let entry = snapshots[0].1.buckets.remove("cold-bucket").expect("cold");
                    snapshots[0].1.buckets.insert(String::new(), entry);
                }
                "entry_limit" => {
                    let peer = &mut snapshots[0].1;
                    peer.buckets = (0..=crate::SCANNER_DIRTY_USAGE_SNAPSHOT_MAX_ENTRIES)
                        .map(|index| {
                            (
                                format!("bucket-{index}"),
                                crate::storage_api::EcstoreScannerPeerDirtyUsageBucket {
                                    bucket_incarnation: COLD,
                                    generation: 8,
                                },
                            )
                        })
                        .collect();
                    peer.pending_bucket_count = peer.buckets.len() as u64;
                }
                "pending" => {
                    snapshots[0].1.generation = 11;
                    snapshots[0].1.buckets.clear();
                    snapshots[0].1.pending_bucket_count = 0;
                }
                _ => unreachable!(),
            }
            assert!(ScannerRemoteDirtyUsageWindow::verify(before, snapshots, after).is_none(), "{case}");
        }
    }

    #[test]
    fn certification_rejects_owner_replacement_and_a_generation_rollback_between_windows() {
        let mut changed = window(12, 13, 14);
        changed.peers.get_mut("peer-b").expect("clean peer").owner_id = uuid::Uuid::from_u128(999).to_string();
        assert!(ScannerRemoteDirtyUsageCertification::new(window(9, 10, 11), changed).is_none());
        assert!(ScannerRemoteDirtyUsageCertification::new(window(9, 10, 11), window(8, 9, 10)).is_none());
    }

    #[test]
    fn certification_requires_actual_coverage_and_the_same_incarnation_on_every_set() {
        let proof = ScannerRemoteDirtyUsageCertification::new(window(10, 10, 10), window(10, 10, 10)).expect("stable peers");
        for case in ["missing_set", "missing_bucket", "wrong_incarnation", "partial"] {
            let (sources, mut completed) = completions();
            let mut uncertified = HashSet::new();
            match case {
                "missing_set" => {
                    completed.remove(&DataUsageCacheSource::new(0, 1));
                }
                "missing_bucket" => {
                    completed
                        .get_mut(&DataUsageCacheSource::new(0, 1))
                        .expect("set")
                        .remove("cold-bucket");
                }
                "wrong_incarnation" => {
                    completed
                        .get_mut(&DataUsageCacheSource::new(0, 1))
                        .expect("set")
                        .insert("cold-bucket".to_string(), uuid::Uuid::from_u128(999));
                }
                "partial" => {
                    uncertified.insert("cold-bucket".to_string());
                }
                _ => unreachable!(),
            }
            let (acks, pending) = proof.classify_buckets(&sources, &completed, &uncertified);
            assert!(pending.contains("cold-bucket"), "{case}");
            assert!(acks.iter().all(|ack| matches!(&ack.kind, ScannerDirtyUsageAcknowledgementKind::Scoped { entries, .. } if entries.iter().all(|entry| entry.bucket != "cold-bucket"))), "{case}");
        }
    }

    #[test]
    fn a_bucket_changed_on_one_peer_cannot_be_acknowledged_on_another() {
        let start = window(10, 10, 10);
        let mut post_peers = peers(10, 10);
        post_peers[1].1.generation = 8;
        post_peers[1].1.pending_bucket_count = 1;
        post_peers[1].1.buckets.insert(
            "cold-bucket".to_string(),
            crate::storage_api::EcstoreScannerPeerDirtyUsageBucket {
                bucket_incarnation: COLD,
                generation: 8,
            },
        );
        let mut after = activity(10);
        after.insert(
            "peer-b".to_string(),
            crate::scanner::scanner_node_activity_for_tests(&post_peers[1].1.instance_id, 8, 8, true),
        );
        let post = ScannerRemoteDirtyUsageWindow::verify(activity(10), post_peers, after).expect("clean peer became dirty");
        let proof = ScannerRemoteDirtyUsageCertification::new(start, post).expect("same producers");
        let (sources, completed) = completions();
        let (acks, pending) = proof.classify_buckets(&sources, &completed, &HashSet::new());
        assert!(pending.contains("cold-bucket"));
        assert!(acks.iter().all(|ack| matches!(&ack.kind, ScannerDirtyUsageAcknowledgementKind::Scoped { entries, .. } if entries.iter().all(|entry| entry.bucket != "cold-bucket"))));
    }

    #[test]
    fn start_window_trailing_activity_invalidates_same_cycle_current_cache_without_segment_reuse() {
        let window = window(9, 10, 11);
        let old_plan = scanner_bucket_cache_digest(
            DataUsageScanPlanDigest(crate::scanner::scanner_activity_snapshot_digest(&window.before)),
            None,
        );
        let new_plan = scanner_bucket_cache_digest(
            DataUsageScanPlanDigest(crate::scanner::scanner_activity_snapshot_digest(&window.activity_for_walk())),
            None,
        );
        let source = DataUsageCacheSource::new(0, 0);
        let mut cache = DataUsageCache {
            info: DataUsageCacheInfo {
                name: "cold-bucket".to_string(),
                next_cycle: 7,
                leader_epoch: 11,
                source: Some(source),
                last_update: Some(SystemTime::UNIX_EPOCH),
                snapshot_complete: true,
                scan_plan_digest: Some(old_plan),
                cache_key_format: DATA_USAGE_CACHE_KEY_FORMAT,
                ..Default::default()
            },
            ..Default::default()
        };
        cache.replace("cold-bucket", "", DataUsageEntry::default());
        let options = || DataUsageCacheReuseOptions {
            require_source: true,
            tier_registry_generation: None,
            checkpoint_identity: None,
            checkpoint_plan_digest: None,
        };
        assert!(matches!(
            current_cache_root_or_prepare_with_generation(&mut cache.clone(), "cold-bucket", source, 7, 11, old_plan, options()),
            DataUsageCacheScanState::Current(_)
        ));
        assert!(matches!(
            current_cache_root_or_prepare_with_generation(&mut cache, "cold-bucket", source, 7, 11, new_plan, options()),
            DataUsageCacheScanState::Prepared {
                outcome: DataUsageCachePrepareOutcome::Reset,
                ..
            }
        ));
        assert!(window.strict_remote_usage().is_none());
    }
}
