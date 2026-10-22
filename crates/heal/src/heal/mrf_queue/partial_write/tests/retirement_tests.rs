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
use crate::heal::DiskStore;
use crate::heal::storage::{HealListItem, HealObjectInfo};
use crate::heal::storage_api::storage::{BucketInfo, DiskSetSelector};
use crate::{Error, Result};
use rustfs_common::mrf_channel::{MrfDeleteMarkerPurge, take_mrf_verified_repair_events_for};
use rustfs_heal_contracts::heal_channel::HealOpts;
use rustfs_madmin::heal_commands::HealResultItem;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU8, Ordering};

#[derive(Clone, Debug)]
struct RetirementCall {
    bucket: String,
    incarnation: Uuid,
    at: Instant,
    object_refs: Vec<usize>,
}

// Controlled callback provider for producer cost/cache behavior. The real
// ECStore retirement-authority regression remains a separate required gate.
struct RetirementStorage {
    watched: Vec<Arc<str>>,
    calls: Mutex<Vec<RetirementCall>>,
    result: AtomicU8,
    delay: Duration,
}

impl RetirementStorage {
    fn new(watched: Vec<Arc<str>>, delay: Duration) -> Self {
        Self {
            watched,
            calls: Mutex::new(Vec::new()),
            result: AtomicU8::new(0),
            delay,
        }
    }
    fn calls(&self) -> Vec<RetirementCall> {
        self.calls.lock().expect("retirement calls").clone()
    }
}

#[async_trait::async_trait]
impl HealStorageAPI for RetirementStorage {
    async fn mrf_bucket_incarnation_retired(&self, bucket: &str, expected: Uuid) -> Result<bool> {
        self.calls.lock().expect("retirement calls").push(RetirementCall {
            bucket: bucket.to_string(),
            incarnation: expected,
            at: Instant::now(),
            object_refs: self.watched.iter().map(Arc::strong_count).collect(),
        });
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        match self.result.load(Ordering::Relaxed) {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Error::other("retirement storage unavailable")),
        }
    }
    async fn get_object_meta(&self, _: &str, _: &str) -> Result<Option<HealObjectInfo>> {
        panic!("retirement must not read objects")
    }
    async fn ec_decode_rebuild(&self, _: &str, _: &str) -> Result<Vec<u8>> {
        panic!("retirement must not rebuild objects")
    }
    async fn get_bucket_info(&self, _: &str) -> Result<Option<BucketInfo>> {
        panic!("retirement uses exact generation checks")
    }
    async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        panic!("retirement must not list buckets")
    }
    async fn list_buckets_for_admin_heal(&self, _: Option<DiskSetSelector>) -> Result<Vec<BucketInfo>> {
        panic!("retirement must not start admin healing")
    }
    async fn list_objects_for_heal_page(
        &self,
        _: &str,
        _: &str,
        _: Option<&str>,
        _: bool,
    ) -> Result<(Vec<HealListItem>, Option<String>, bool)> {
        panic!("retirement must not enumerate objects")
    }
    async fn object_exists(&self, _: &str, _: &str) -> Result<bool> {
        panic!("retirement must not probe objects")
    }
    async fn heal_object(&self, _: &str, _: &str, _: Option<&str>, _: &HealOpts) -> Result<(HealResultItem, Option<Error>)> {
        panic!("retirement must not heal objects")
    }
    async fn heal_bucket(&self, _: &str, _: &HealOpts) -> Result<HealResultItem> {
        panic!("retirement must not heal buckets")
    }
    async fn heal_format(&self, _: bool) -> Result<(HealResultItem, Option<Error>)> {
        panic!("retirement must not heal formats")
    }
    async fn get_disk_for_resume(&self, _: &str) -> Result<DiskStore> {
        panic!("retirement must not open resume disks")
    }
}

fn admit(writes: &mut PartialWrites, bucket: &str, object: &str, source: Option<Uuid>, persisted: bool) -> PartialWriteKey {
    let item = MrfIntent {
        bucket: Arc::from(bucket),
        object: Arc::from(object),
        version_id: None,
        kind: MrfKind::PartialWrite,
        delete_marker_purge: None,
        scope: Some(MrfScope {
            pool_index: 0,
            set_index: 0,
        }),
        lease: None,
        enqueued_at_ms: 17,
        attempts: 0,
    };
    let key = PartialWriteKey::new(&item, source);
    writes
        .admit_with_source_incarnation(item, source, 256, usize::MAX)
        .expect("admit retirement fixture");
    writes.entries.get_mut(&key).expect("retained fixture").persisted = persisted;
    key
}

fn expected_event(entry: &Responsibility, source: Uuid) -> MrfVerifiedRepairEvent {
    let anchor = MrfDurableRepairAnchor::from_intent(&entry.intent, source).expect("exact eligible source");
    MrfVerifiedRepairEvent {
        kind: anchor.kind,
        bucket: anchor.bucket,
        object: anchor.object,
        version_id: anchor.version_id,
        scope: anchor.scope,
        delete_marker_purge: anchor.delete_marker_purge,
        lease: Some(anchor.lease),
        bucket_incarnation_id: anchor.bucket_incarnation_id,
        disposition: MrfVerifiedRepairDisposition::AuthoritativelyAbsent,
    }
}

#[tokio::test(start_paused = true)]
async fn retirement_avoids_prepared_object_handles_and_preserves_false_error_true_cache() {
    let bucket = format!("retirement-cost-{}", Uuid::new_v4());
    let source = Uuid::new_v4();
    let mut writes = PartialWrites::default();
    for index in 0..32 {
        admit(&mut writes, &bucket, &format!("object-{index}"), Some(source), true);
    }
    let mut purge = intent("purge-proof");
    purge.bucket = Arc::from(bucket.as_str());
    purge.kind = MrfKind::DeleteMarkerPurge;
    purge.version_id = Some([7; 16]);
    purge.scope = Some(MrfScope {
        pool_index: 7,
        set_index: 12,
    });
    purge.delete_marker_purge = MrfDeleteMarkerPurge::new(source, source, [9; 32], vec![7]);
    writes
        .admit_with_source_incarnation(purge, Some(source), 256, usize::MAX)
        .expect("admit exact marker purge");
    writes.mark_persisted();
    assert!(
        writes.entries.values().all(|entry| entry.anchor.is_none()),
        "undispatched obligations still require retirement checks"
    );
    let storage = Arc::new(RetirementStorage::new(
        writes.entries.values().map(|entry| entry.intent.object.clone()).collect(),
        Duration::ZERO,
    ));
    let baseline = storage.watched.iter().map(Arc::strong_count).collect::<Vec<_>>();
    let manager = HealManager::new_without_root_recovery_for_test(storage.clone(), None);
    let original = writes.checkpoint_records(super::super::super::intent_digest);
    let started = Instant::now();
    writes.note_retired_generation_proofs(&manager).await;
    assert_eq!(storage.calls().len(), 1);
    assert_eq!(
        storage.calls()[0].object_refs,
        baseline,
        "the actual retirement storage callback must not see N eagerly owned anchor object handles"
    );
    assert!(take_mrf_verified_repair_events_for(&bucket).is_empty());
    assert_eq!(writes.retirement_checks[&(Arc::from(bucket.as_str()), source)], (started, false));
    tokio::time::advance(Duration::from_millis(4999)).await;
    writes.note_retired_generation_proofs(&manager).await;
    assert_eq!(storage.calls().len(), 1, "cached false does not query or refresh its timestamp");
    storage.result.store(2, Ordering::Relaxed);
    tokio::time::advance(Duration::from_millis(1)).await;
    let failed_at = Instant::now();
    writes.note_retired_generation_proofs(&manager).await;
    assert_eq!(storage.calls().len(), 2, "the exact TTL boundary must recheck");
    assert_eq!(
        writes.retirement_checks[&(Arc::from(bucket.as_str()), source)],
        (failed_at, false),
        "I/O error is cached false"
    );
    tokio::time::advance(Duration::from_millis(4999)).await;
    writes.note_retired_generation_proofs(&manager).await;
    assert_eq!(storage.calls().len(), 2);
    storage.result.store(1, Ordering::Relaxed);
    tokio::time::advance(Duration::from_millis(1)).await;
    writes.note_retired_generation_proofs(&manager).await;
    assert_eq!(storage.calls().len(), 3);
    assert!(storage.calls().iter().all(|call| call.object_refs == baseline));
    let expected = writes
        .entries
        .values()
        .map(|entry| expected_event(entry, source))
        .collect::<Vec<_>>();
    assert_eq!(
        take_mrf_verified_repair_events_for(&bucket),
        expected,
        "proofs preserve exact fields and entry order"
    );
    tokio::time::advance(Duration::from_secs(100)).await;
    writes.note_retired_generation_proofs(&manager).await;
    assert_eq!(storage.calls().len(), 3, "true is sticky while the eligible generation remains");
    assert_eq!(
        take_mrf_verified_repair_events_for(&bucket),
        expected,
        "cached true still republishes the original exact proofs"
    );
    assert_eq!(
        writes.checkpoint_records(super::super::super::intent_digest),
        original,
        "proof production does not alter responsibility state"
    );
}

#[tokio::test(start_paused = true)]
async fn retirement_keeps_one_preawait_now_and_one_query_per_generation() {
    let bucket = format!("retirement-clock-{}", Uuid::new_v4());
    let sources = [Uuid::new_v4(), Uuid::new_v4()];
    let mut writes = PartialWrites::default();
    for (index, source) in sources.iter().enumerate() {
        admit(&mut writes, &bucket, &format!("object-{index}-a"), Some(*source), true);
        admit(&mut writes, &bucket, &format!("object-{index}-b"), Some(*source), true);
    }
    let storage = Arc::new(RetirementStorage::new(Vec::new(), Duration::from_secs(3)));
    let manager = HealManager::new_without_root_recovery_for_test(storage.clone(), None);
    let started = Instant::now();
    writes.note_retired_generation_proofs(&manager).await;
    assert_eq!(storage.calls().len(), 2);
    assert!(
        writes.retirement_checks.values().all(|value| *value == (started, false)),
        "every group stores the single time captured before either await"
    );
    assert_eq!(
        storage
            .calls()
            .iter()
            .map(|call| (&call.bucket, call.incarnation))
            .collect::<HashSet<_>>()
            .len(),
        2
    );
    assert!(
        storage.calls()[0].at < storage.calls()[1].at,
        "the callback really spans different instants"
    );
    let next = Instant::now();
    assert!(next.duration_since(started) >= Duration::from_secs(6));
    writes.note_retired_generation_proofs(&manager).await;
    assert_eq!(storage.calls().len(), 4, "both false groups expire from the same pre-await timestamp");
    assert!(writes.retirement_checks.values().all(|value| *value == (next, false)));
    assert!(take_mrf_verified_repair_events_for(&bucket).is_empty());
    storage.result.store(1, Ordering::Relaxed);
    // The preceding callbacks took six seconds, so both false entries expire.
    writes.note_retired_generation_proofs(&manager).await;
    assert_eq!(storage.calls().len(), 6);
    let events = take_mrf_verified_repair_events_for(&bucket);
    assert_eq!(events.len(), 4);
    assert_ne!(events[0].bucket_incarnation_id, events[2].bucket_incarnation_id);
    assert_eq!(events[0].bucket_incarnation_id, events[1].bucket_incarnation_id);
    assert_eq!(events[2].bucket_incarnation_id, events[3].bucket_incarnation_id);
    for source in sources {
        let actual = events
            .iter()
            .filter(|event| event.bucket_incarnation_id == source)
            .cloned()
            .collect::<Vec<_>>();
        let expected = writes
            .entries
            .values()
            .filter(|entry| entry.source_bucket_incarnation_id == Some(source))
            .map(|entry| expected_event(entry, source))
            .collect::<Vec<_>>();
        assert_eq!(
            actual, expected,
            "each original generation group remains contiguous and keeps its entry order"
        );
        assert_eq!(storage.calls().iter().filter(|call| call.incarnation == source).count(), 3);
    }
}

#[tokio::test]
async fn retirement_cache_membership_uses_only_exact_eligible_persisted_sources() {
    let bucket = format!("retirement-membership-{}", Uuid::new_v4());
    let valid = Uuid::new_v4();
    let unpersisted = Uuid::new_v4();
    let unleased = Uuid::new_v4();
    let mismatched = Uuid::new_v4();
    let purge_source = Uuid::new_v4();
    let mut writes = PartialWrites::default();
    let valid_key = admit(&mut writes, &bucket, "valid", Some(valid), true);
    let unknown_key = admit(&mut writes, &bucket, "unknown", None, true);
    let unpersisted_key = admit(&mut writes, &bucket, "not-persisted", Some(unpersisted), false);
    let unleased_key = admit(&mut writes, &bucket, "no-lease", Some(unleased), true);
    writes.entries.get_mut(&unleased_key).expect("lease fixture").intent.lease = None;
    let mut purge = intent("mismatched-purge");
    purge.bucket = Arc::from(bucket.as_str());
    purge.kind = MrfKind::DeleteMarkerPurge;
    purge.version_id = Some([7; 16]);
    purge.delete_marker_purge = MrfDeleteMarkerPurge::new(purge_source, purge_source, [9; 32], vec![7]);
    writes
        .admit_with_source_incarnation(purge, Some(mismatched), 256, usize::MAX)
        .expect("retain a mismatched source without rewriting it");
    writes.mark_persisted();
    writes
        .entries
        .get_mut(&unpersisted_key)
        .expect("unpersisted fixture")
        .persisted = false;
    let original_unknown = writes.entries.get(&unknown_key).expect("old None").responsibility_id;
    let unknown_before = writes
        .checkpoint_records(super::super::super::intent_digest)
        .into_iter()
        .find(|record| record.responsibility_id == original_unknown)
        .expect("original None checkpoint");
    let now = Instant::now();
    for source in [valid, unpersisted, unleased, mismatched, purge_source] {
        writes
            .retirement_checks
            .insert((Arc::from(bucket.as_str()), source), (now, true));
    }
    let storage = Arc::new(RetirementStorage::new(Vec::new(), Duration::ZERO));
    let manager = HealManager::new_without_root_recovery_for_test(storage.clone(), None);
    writes.note_retired_generation_proofs(&manager).await;
    assert_eq!(writes.retirement_checks.len(), 1);
    assert!(writes.retirement_checks.contains_key(&(Arc::from(bucket.as_str()), valid)));
    assert!(storage.calls().is_empty(), "the only eligible generation uses its sticky true cache");
    assert_eq!(
        take_mrf_verified_repair_events_for(&bucket),
        vec![expected_event(&writes.entries[&valid_key], valid)]
    );
    writes.entries.remove(&valid_key);
    writes.note_retired_generation_proofs(&manager).await;
    assert!(
        writes.retirement_checks.is_empty(),
        "None and ineligible entries cannot keep an old cache generation alive"
    );
    writes
        .entries
        .get_mut(&unpersisted_key)
        .expect("activate persisted source")
        .persisted = true;
    writes.note_retired_generation_proofs(&manager).await;
    assert_eq!(storage.calls().len(), 1, "a newly eligible source needs a real retirement lookup");
    assert_eq!(storage.calls()[0].incarnation, unpersisted);
    assert_eq!(
        writes
            .checkpoint_records(super::super::super::intent_digest)
            .into_iter()
            .find(|record| record.responsibility_id == original_unknown),
        Some(unknown_before)
    );
}

#[tokio::test(start_paused = true)]
async fn retirement_cancellation_cannot_publish_an_unfinished_retirement() {
    let bucket = format!("retirement-cancel-{}", Uuid::new_v4());
    let mut writes = PartialWrites::default();
    admit(&mut writes, &bucket, "object", Some(Uuid::new_v4()), true);
    let original = writes.checkpoint_records(super::super::super::intent_digest);
    let storage = Arc::new(RetirementStorage::new(Vec::new(), Duration::from_secs(3)));
    storage.result.store(1, Ordering::Relaxed);
    let manager = HealManager::new_without_root_recovery_for_test(storage.clone(), None);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), writes.note_retired_generation_proofs(&manager))
            .await
            .is_err()
    );
    assert_eq!(storage.calls().len(), 1);
    assert!(writes.retirement_checks.is_empty());
    assert!(take_mrf_verified_repair_events_for(&bucket).is_empty());
    assert_eq!(writes.checkpoint_records(super::super::super::intent_digest), original);
}
