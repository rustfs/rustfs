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
use crate::heal::mrf_queue::{MrfLifecycleControlError, MrfQueue, MrfRuntime};
use crate::heal::storage::{HealListItem, HealObjectInfo};
use crate::heal::storage_api::storage::{BucketInfo, DiskSetSelector};
use crate::heal::{DiskStore, EcstoreError};
use crate::{Error, Result};
use rustfs_heal_contracts::heal_channel::HealOpts;
use rustfs_madmin::heal_commands::HealResultItem;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Copy)]
enum Lookup {
    Identity(Option<Uuid>),
    PermissionDenied,
    Quorum,
    Transport,
    Pending,
}

struct ProbeStorage {
    lookup: Mutex<Lookup>,
    calls: AtomicUsize,
}

#[async_trait::async_trait]
impl HealStorageAPI for ProbeStorage {
    async fn mrf_bucket_incarnation_id(&self, _: &str) -> Result<Option<Uuid>> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let lookup = *self.lookup.lock().expect("lookup outcome");
        match lookup {
            Lookup::Identity(value) => Ok(value),
            Lookup::PermissionDenied => Err(Error::Io(std::io::ErrorKind::PermissionDenied.into())),
            Lookup::Quorum => Err(Error::Storage(EcstoreError::ErasureReadQuorum)),
            Lookup::Transport => Err(Error::Io(std::io::ErrorKind::ConnectionReset.into())),
            Lookup::Pending => std::future::pending().await,
        }
    }
    async fn get_object_meta(&self, _: &str, _: &str) -> Result<Option<HealObjectInfo>> {
        panic!("identity probing must not read objects")
    }
    async fn ec_decode_rebuild(&self, _: &str, _: &str) -> Result<Vec<u8>> {
        panic!("identity probing must not rebuild objects")
    }
    async fn get_bucket_info(&self, _: &str) -> Result<Option<BucketInfo>> {
        panic!("identity probing must use the incarnation lookup")
    }
    async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        panic!("identity probing must not enumerate buckets")
    }
    async fn list_buckets_for_admin_heal(&self, _: Option<DiskSetSelector>) -> Result<Vec<BucketInfo>> {
        panic!("identity probing must not start admin healing")
    }
    async fn list_objects_for_heal_page(
        &self,
        _: &str,
        _: &str,
        _: Option<&str>,
        _: bool,
    ) -> Result<(Vec<HealListItem>, Option<String>, bool)> {
        panic!("identity probing must not enumerate objects")
    }
    async fn object_exists(&self, _: &str, _: &str) -> Result<bool> {
        panic!("identity probing must not probe objects")
    }
    async fn heal_object(&self, _: &str, _: &str, _: Option<&str>, _: &HealOpts) -> Result<(HealResultItem, Option<Error>)> {
        panic!("identity probing must not heal objects")
    }
    async fn heal_bucket(&self, _: &str, _: &HealOpts) -> Result<HealResultItem> {
        panic!("identity probing must not heal buckets")
    }
    async fn heal_format(&self, _: bool) -> Result<(HealResultItem, Option<Error>)> {
        panic!("identity probing must not heal formats")
    }
    async fn get_disk_for_resume(&self, _: &str) -> Result<DiskStore> {
        panic!("identity probing must not open resume disks")
    }
}

fn runtime() -> MrfRuntime {
    let config = MrfConsumerConfig::default();
    MrfRuntime {
        partial_writes: PartialWrites::default(),
        queue: MrfQueue::new(config.queue_capacity, config.journal_max_bytes),
        config,
        checkpoint_owner: Uuid::new_v4(),
        next_checkpoint_sequence: 1,
        new_since_flush: 0,
        dirty: false,
        journal_on_disk: false,
        retain_replay_journal: false,
        replay_readable: true,
        durable_replay_anchors: Vec::new(),
        replay_cleanup: None,
        runtime_checkpoint: None,
        backoff_until: None,
    }
}

#[tokio::test(start_paused = true)]
async fn unknown_source_deleted_bucket_probes_preserve_errors_and_bound_retries() {
    let storage = Arc::new(ProbeStorage {
        lookup: Mutex::new(Lookup::Identity(None)),
        calls: AtomicUsize::new(0),
    });
    let manager = HealManager::new_without_root_recovery_for_test(storage.clone(), None);
    let mut runtime = runtime();
    let original = intent("identity-probe");
    let key = PartialWriteKey::new(&original, None);
    runtime
        .admit_partial_write(original, None)
        .expect("admit a retained obligation");
    runtime.partial_writes.mark_persisted();
    let checkpoint = runtime.partial_writes.checkpoint_records(super::super::super::intent_digest);
    let id = checkpoint[0].responsibility_id;
    let bytes = runtime.partial_writes.bytes();
    for outcome in [
        Lookup::Identity(None),
        Lookup::PermissionDenied,
        Lookup::Quorum,
        Lookup::Transport,
        Lookup::Identity(Some(Uuid::nil())),
        Lookup::Pending,
    ] {
        *storage.lookup.lock().expect("set lookup outcome") = outcome;
        runtime
            .partial_writes
            .entries
            .get_mut(&key)
            .expect("retained obligation")
            .next_attempt = Instant::now();
        let calls = storage.calls.load(Ordering::Relaxed);
        let start = Instant::now();
        assert!(
            !runtime
                .partial_writes
                .dispatch_collecting_lifecycle_change(&manager, &runtime.config)
                .await
        );
        assert_eq!(storage.calls.load(Ordering::Relaxed), calls + 1);
        assert!(start.elapsed() <= MRF_IDENTITY_PROBE_TIMEOUT);
        let entry = runtime.partial_writes.entries.get(&key).expect("probe must retain its owner");
        assert!(entry.next_attempt > Instant::now());
        assert!(entry.next_attempt <= Instant::now() + MAX_UNVERIFIED_RETRY_BACKOFF);
        assert!(entry.anchor.is_none(), "lookup failures cannot create a repair proof");
        assert_eq!(runtime.partial_writes.bytes(), bytes);
        assert_eq!(runtime.partial_writes.checkpoint_records(super::super::super::intent_digest), checkpoint);
        assert!(
            !runtime
                .partial_writes
                .dispatch_collecting_lifecycle_change(&manager, &runtime.config)
                .await
        );
        assert_eq!(
            storage.calls.load(Ordering::Relaxed),
            calls + 1,
            "a consumer tick cannot bypass probe backoff"
        );
        let row = runtime
            .list_unverified_legacy(None, 1)
            .expect("list retained obligation")
            .responsibilities
            .remove(0);
        assert_eq!(row.bucket_incarnation_id, None);
        assert_eq!(row.source_bucket_incarnation_id, None);
        assert_eq!(
            row.last_identity_probe_result,
            Some(if matches!(outcome, Lookup::Identity(None)) {
                "unresolved"
            } else {
                "unavailable"
            })
        );
        assert!(row.next_identity_probe_at_ms.is_some());

        if !matches!(outcome, Lookup::Pending) {
            let error = runtime
                .refresh_legacy_generation(&manager, id, None)
                .await
                .expect_err("refresh needs a known identity");
            match (outcome, error) {
                (Lookup::Identity(None), MrfLifecycleControlError::BucketIdentityUnresolved) => {}
                (Lookup::PermissionDenied, MrfLifecycleControlError::BucketIdentityUnavailable(source)) => {
                    assert!(matches!(*source, Error::Io(ref error) if error.kind() == std::io::ErrorKind::PermissionDenied));
                }
                (Lookup::Quorum, MrfLifecycleControlError::BucketIdentityUnavailable(source)) => {
                    assert!(matches!(*source, Error::Storage(EcstoreError::ErasureReadQuorum)));
                }
                (Lookup::Transport, MrfLifecycleControlError::BucketIdentityUnavailable(source)) => {
                    assert!(matches!(*source, Error::Io(ref error) if error.kind() == std::io::ErrorKind::ConnectionReset));
                }
                (Lookup::Identity(Some(_)), MrfLifecycleControlError::BucketIdentityUnavailable(source)) => {
                    assert!(matches!(*source, Error::InvalidBucketIncarnation));
                }
                (_, error) => panic!("lookup error lost its type: {error:?}"),
            }
        }
    }
    assert_eq!(manager.get_queue_length().await, 0);
}

#[tokio::test(start_paused = true)]
async fn unknown_source_deleted_bucket_probe_rotation_keeps_count_and_byte_budgets() {
    let current = Uuid::new_v4();
    let storage = Arc::new(ProbeStorage {
        lookup: Mutex::new(Lookup::Identity(Some(current))),
        calls: AtomicUsize::new(0),
    });
    let manager = HealManager::new_without_root_recovery_for_test(storage.clone(), None);
    let mut writes = PartialWrites::default();
    let config = MrfConsumerConfig {
        replay_batch: 1,
        ..Default::default()
    };
    for object in ["rotation-a", "rotation-b", "rotation-c"] {
        let item = intent(object);
        let budget = writes.bytes() + PartialWrites::cost(&item);
        writes.admit(item, 3, budget).expect("admit at the exact byte boundary");
    }
    let bytes = writes.bytes();
    assert!(writes.admit(intent("overflow"), 3, bytes).is_err());
    writes.mark_persisted();
    for entry in writes.entries.values_mut() {
        entry.next_attempt = Instant::now();
    }
    for expected_calls in 1..=3 {
        assert!(writes.dispatch_collecting_lifecycle_change(&manager, &config).await);
        assert_eq!(storage.calls.load(Ordering::Relaxed), expected_calls);
        assert_eq!(writes.bytes(), bytes, "observation must fit the budget reserved at admission");
    }
    let (rows, _) = writes.list_unverified_legacy(None, 10);
    assert_eq!(rows.len(), 3);
    assert!(
        rows.iter()
            .all(|row| row.source_bucket_incarnation_id.is_none() && row.bucket_incarnation_id == Some(current))
    );
    for _ in 0..3 {
        assert!(!writes.dispatch_collecting_lifecycle_change(&manager, &config).await);
    }
    assert_eq!(storage.calls.load(Ordering::Relaxed), 3, "observed unknown sources stay parked");
    assert_eq!(manager.get_queue_length().await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn unknown_source_deleted_bucket_replay_probes_recreation_without_rebinding() {
    let env = rustfs_test_utils::TestECStoreEnv::builder()
        .prefix("rustfs_mrf_unknown_deleted_bucket")
        .build()
        .await;
    let bucket = "partial-write-retention";
    env.make_bucket(bucket, false).await;
    let storage: Arc<dyn HealStorageAPI> = Arc::new(ECStoreHealStorage::new(env.ecstore.clone()));
    let manager = Arc::new(HealManager::new_without_root_recovery_for_test(storage, None));
    let mut original = runtime();
    for index in 0..3 {
        original
            .admit_partial_write(intent(&format!("deleted-bucket-{index}")), None)
            .expect("admit each unknown-source responsibility");
    }
    assert!(original.flush().await, "commit responsibilities before deleting the bucket");
    let before = original.partial_writes.checkpoint_records(super::super::super::intent_digest);
    env.ecstore
        .delete_bucket(bucket, &DeleteBucketOptions::default())
        .await
        .expect("delete the bucket through its supported API");
    let mut restarted = runtime();
    let replay = super::super::super::replay_into(&manager, &mut restarted.queue, &mut None).await;
    assert_eq!(replay.partial_writes.len(), before.len());
    restarted.next_checkpoint_sequence = replay.next_checkpoint_sequence;
    restarted.replay_cleanup = replay.cleanup;
    restarted.retain_replay_journal = replay.retain_journal_for_replay;
    restarted.durable_replay_anchors = replay.durable_replay_anchors;
    restarted.adopt_replayed_partial_writes(replay.partial_writes);
    restarted.dirty = true;
    assert!(restarted.flush().await, "publish the retained owner after replay");
    restarted.dispatch(&manager).await;

    let mut visible = Vec::new();
    let mut cursor = None;
    loop {
        let page = restarted
            .list_unverified_legacy(cursor, 1)
            .expect("list readable responsibilities");
        assert_eq!(page.legacy_generation_unknown_count, before.len());
        visible.extend(page.responsibilities.into_iter().map(|row| {
            assert_eq!(row.source_bucket_incarnation_id, None);
            assert_eq!(row.bucket_incarnation_id, None);
            row.responsibility_id
        }));
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(
        visible.into_iter().collect::<HashSet<_>>(),
        before.iter().map(|row| row.responsibility_id).collect()
    );

    env.make_bucket(bucket, false).await;
    let observed = env
        .ecstore
        .bucket_incarnation_id(bucket)
        .await
        .expect("recreated incarnation");
    for entry in restarted.partial_writes.entries.values_mut() {
        entry.next_attempt = Instant::now();
    }
    for _ in 0..before.len() {
        restarted.dispatch(&manager).await;
    }
    let rows = restarted
        .list_unverified_legacy(None, 10)
        .expect("list observed responsibilities")
        .responsibilities;
    assert_eq!(rows.len(), before.len(), "identity probing cannot discharge a responsibility");
    for row in rows {
        assert_eq!(
            row.status, "legacy_generation_unknown",
            "recreated buckets must be observed automatically"
        );
        assert_eq!(row.bucket_incarnation_id, Some(observed));
        assert_eq!(
            row.source_bucket_incarnation_id, None,
            "current identity is never evidence of the old source"
        );
        assert!(
            restarted
                .recheck_legacy_generation(&manager, row.responsibility_id, observed)
                .await
                .is_err()
        );
    }
    assert_eq!(manager.get_queue_length().await, 0, "identity probes must not enqueue object healing");
}
