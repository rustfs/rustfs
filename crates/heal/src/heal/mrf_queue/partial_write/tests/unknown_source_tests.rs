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
        lifecycle_format_version: crate::heal::mrf_queue::MRF_LIFECYCLE_FORMAT_VERSION,
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

#[test]
#[serial]
fn unknown_source_deleted_bucket_replay_probes_recreation_without_rebinding() {
    // Match the bounded stack used by other deep real-storage heal fixtures.
    const STACK_SIZE: usize = 8 * 1024 * 1024;
    std::thread::Builder::new()
        .name("mrf-unknown-source-replay".to_owned())
        .stack_size(STACK_SIZE)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_stack_size(STACK_SIZE)
                .enable_all()
                .build()
                .expect("MRF replay test runtime");
            runtime.block_on(check_unknown_source_deleted_bucket_replay());
        })
        .expect("MRF replay test thread")
        .join()
        .expect("MRF replay test must finish");
}

async fn check_unknown_source_deleted_bucket_replay() {
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

fn restored_v2_runtime(next_probe_at_ms: u64, probe_attempts: u32) -> (MrfRuntime, PartialWriteKey) {
    let mut runtime = runtime();
    runtime.lifecycle_format_version = 2;
    let original = intent("v2-unknown-generation");
    let key = PartialWriteKey::new(&original, None);
    runtime
        .admit_partial_write(original.clone(), None)
        .expect("admit retained owner");
    let record = ResponsibilityCheckpoint {
        intent_digest: super::super::super::intent_digest(&original).expect("intent identity"),
        responsibility_id: Uuid::new_v4(),
        source_bucket_incarnation_id: None,
        last_operator_acceptance: None,
        state: ResponsibilityState::SourceAndCurrentGenerationUnknown {
            since_ms: 7,
            next_probe_at_ms,
            probe_attempts,
        },
    };
    assert!(runtime.partial_writes.restore_state(&original, &record));
    runtime.partial_writes.mark_persisted();
    (runtime, key)
}

#[tokio::test(start_paused = true)]
async fn lifecycle_v2_reader_restores_bounded_probe_schedule_and_attempt_count() {
    for (deadline, attempts, expected_delay) in [
        (0, 0, MRF_RETIREMENT_PROOF_RECHECK_INTERVAL),
        (u64::MAX, u32::MAX, MAX_UNVERIFIED_RETRY_BACKOFF),
    ] {
        let now = Instant::now();
        let (runtime, key) = restored_v2_runtime(deadline, attempts);
        let entry = runtime.partial_writes.entries.get(&key).expect("retained owner");
        assert_eq!(entry.next_attempt, now + expected_delay);
        assert_eq!(entry.consecutive_unverified_attempts, attempts);
        assert!(entry.anchor.is_none());
        assert!(!entry.retry_queued);
        assert!(entry.is_parked());
        assert_eq!(entry.source_bucket_incarnation_id, None);
        assert!(
            matches!(entry.state, ResponsibilityState::SourceAndCurrentGenerationUnknown { since_ms: 7, next_probe_at_ms, probe_attempts } if next_probe_at_ms == deadline && probe_attempts == attempts)
        );
        assert_eq!(runtime.partial_writes.bytes(), PartialWrites::entry_cost(entry));
        assert_eq!(runtime.partial_writes.lifecycle_summary().legacy_generation_unknown_count, 1);
    }
}

#[tokio::test(start_paused = true)]
async fn lifecycle_v2_reader_persists_failed_probe_pacing_without_object_dispatch() {
    let storage = Arc::new(ProbeStorage {
        lookup: Mutex::new(Lookup::Identity(None)),
        calls: AtomicUsize::new(0),
    });
    let manager = HealManager::new_without_root_recovery_for_test(storage.clone(), None);
    let (mut runtime, key) = restored_v2_runtime(0, u32::MAX);
    let id = runtime.partial_writes.entries[&key].responsibility_id;
    let bytes = runtime.partial_writes.bytes();
    for outcome in [
        Lookup::Identity(None),
        Lookup::PermissionDenied,
        Lookup::Quorum,
        Lookup::Transport,
        Lookup::Identity(Some(Uuid::nil())),
        Lookup::Pending,
    ] {
        *storage.lookup.lock().expect("set outcome") = outcome;
        runtime.partial_writes.entries.get_mut(&key).expect("owner").next_attempt = Instant::now();
        let calls = storage.calls.load(Ordering::Relaxed);
        runtime.dirty = false;
        runtime.dispatch(&manager).await;
        assert!(runtime.dirty, "failed V2 probes still change the durable retry schedule");
        assert_eq!(storage.calls.load(Ordering::Relaxed), calls + 1);
        let entry = &runtime.partial_writes.entries[&key];
        assert_eq!(entry.responsibility_id, id);
        assert_eq!(entry.source_bucket_incarnation_id, None);
        assert!(entry.anchor.is_none());
        assert!(!entry.retry_queued);
        assert!(entry.next_attempt > Instant::now());
        assert!(entry.next_attempt <= Instant::now() + MAX_UNVERIFIED_RETRY_BACKOFF);
        assert!(
            matches!(entry.state, ResponsibilityState::SourceAndCurrentGenerationUnknown { since_ms: 7, probe_attempts: u32::MAX, next_probe_at_ms } if next_probe_at_ms > unix_now_ms() && next_probe_at_ms <= unix_now_ms() + 60_000)
        );
        assert_eq!(runtime.partial_writes.bytes(), bytes);
        runtime.dirty = false;
        runtime.dispatch(&manager).await;
        assert!(!runtime.dirty);
        assert_eq!(storage.calls.load(Ordering::Relaxed), calls + 1);
    }
    let observed = Uuid::new_v4();
    *storage.lookup.lock().expect("known outcome") = Lookup::Identity(Some(observed));
    runtime.partial_writes.entries.get_mut(&key).expect("owner").next_attempt = Instant::now();
    runtime.dispatch(&manager).await;
    assert!(runtime.dirty);
    assert_eq!(runtime.partial_writes.entries[&key].source_bucket_incarnation_id, None);
    assert!(
        matches!(runtime.partial_writes.entries[&key].state, ResponsibilityState::LegacyGenerationUnknown { observed_bucket_incarnation_id, detected_at_ms: 7 } if observed_bucket_incarnation_id == observed)
    );
    assert_eq!(runtime.partial_writes.bytes(), bytes);
    assert_eq!(manager.get_queue_length().await, 0);
}

#[tokio::test(start_paused = true)]
async fn lifecycle_v2_reader_rejects_risk_acceptance_and_recheck_without_current_identity() {
    let storage = Arc::new(ProbeStorage {
        lookup: Mutex::new(Lookup::Identity(Some(Uuid::new_v4()))),
        calls: AtomicUsize::new(0),
    });
    let manager = HealManager::new_without_root_recovery_for_test(storage.clone(), None);
    let (mut runtime, key) = restored_v2_runtime(0, 3);
    let id = runtime.partial_writes.entries[&key].responsibility_id;
    let before = runtime.partial_writes.checkpoint_records(super::super::super::intent_digest);
    let expected = Uuid::new_v4();
    assert!(matches!(
        runtime.recheck_legacy_generation(&manager, id, expected).await,
        Err(MrfLifecycleControlError::BucketIdentityUnresolved)
    ));
    assert!(matches!(
        runtime
            .accept_legacy_risk(
                &manager,
                risk_acceptance(id, expected, true, true, "Reviewed the retained owner", "INC-1234")
            )
            .await,
        Err(MrfLifecycleControlError::BucketIdentityUnresolved)
    ));
    assert_eq!(runtime.partial_writes.checkpoint_records(super::super::super::intent_digest), before);
    assert_eq!(
        storage.calls.load(Ordering::Relaxed),
        0,
        "a guessed identity cannot bypass explicit refresh"
    );
}

#[test]
fn lifecycle_v2_reader_keeps_first_hold_across_repeated_incarnation_refresh() {
    let (mut runtime, key) = restored_v2_runtime(0, 3);
    let id = runtime.partial_writes.entries[&key].responsibility_id;
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    runtime
        .partial_writes
        .replace_observed_incarnation(id, None, first, None)
        .expect("first observation");
    assert!(matches!(
        runtime.partial_writes.entries[&key].state,
        ResponsibilityState::LegacyGenerationUnknown { detected_at_ms: 7, .. }
    ));
    runtime
        .partial_writes
        .replace_observed_incarnation(id, Some(first), second, None)
        .expect("second observation of the same retained responsibility");
    let row = runtime
        .list_unverified_legacy(None, 1)
        .expect("list the retained responsibility")
        .responsibilities
        .remove(0);
    assert_eq!(row.responsibility_id, id);
    assert_eq!(row.source_bucket_incarnation_id, None);
    assert_eq!(row.bucket_incarnation_id, Some(second));
    assert_eq!(
        row.held_since_ms,
        Some(7),
        "changing the observed bucket cannot reset when this responsibility was first retained"
    );
}

fn with_writer_fixture<F, Fut>(test: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()>,
{
    temp_env::with_var("RUSTFS_MRF_LIFECYCLE_V2_ENABLE", Some("true"), || {
        const STACK_SIZE: usize = 8 * 1024 * 1024;
        std::thread::Builder::new()
            .name("mrf-lifecycle-writer".to_owned())
            .stack_size(STACK_SIZE)
            .spawn(move || {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .thread_stack_size(STACK_SIZE)
                    .enable_all()
                    .build()
                    .expect("writer fixture runtime")
                    .block_on(async {
                        let env = rustfs_test_utils::TestECStoreEnv::builder()
                            .prefix("rustfs_mrf_lifecycle_writer")
                            .build()
                            .await;
                        rustfs_common::set_global_local_node_name("mrf-writer-test:9000").await;
                        crate::heal::storage_api::ecstore_new_global_notification_sys(env.endpoint_pools.clone())
                            .await
                            .expect("publish the single-node notification system");
                        crate::heal::storage_api::ecstore_start_remote_version_state_fleet_probe(
                            "mrf-writer-test-topology".to_owned(),
                        );
                        test().await;
                    });
            })
            .expect("writer fixture thread")
            .join()
            .expect("writer fixture must finish");
    });
}

#[test]
#[serial]
fn unknown_source_deleted_bucket_writer_commits_only_the_retained_identity() {
    with_writer_fixture(|| async {
        let storage = Arc::new(ProbeStorage {
            lookup: Mutex::new(Lookup::Identity(None)),
            calls: AtomicUsize::new(0),
        });
        let manager = HealManager::new_without_root_recovery_for_test(storage.clone(), None);
        let mut runtime = runtime();
        let mut original = intent("writer-unresolved");
        original.scope = Some(rustfs_common::mrf_channel::MrfScope {
            pool_index: 0,
            set_index: 0,
        });
        let key = PartialWriteKey::new(&original, None);
        runtime
            .admit_partial_write(original.clone(), None)
            .expect("retain the legacy responsibility");
        assert!(runtime.flush().await, "commit the old-format responsibility first");
        let before = runtime.partial_writes.checkpoint_records(super::super::super::intent_digest);
        assert_eq!(before.len(), 1);
        assert!(matches!(before[0].state, ResponsibilityState::Active {}));
        runtime
            .partial_writes
            .entries
            .get_mut(&key)
            .expect("retained entry")
            .next_attempt = Instant::now();
        runtime.dispatch_partial_writes(&manager).await;
        let checkpoint = super::super::super::read_mrf_lifecycle_checkpoint(
            runtime.checkpoint_owner,
            runtime.runtime_checkpoint.expect("committed owner").1,
            runtime.config.journal_max_bytes,
            runtime.config.journal_max_bytes * 4,
        )
        .await
        .expect("read the committed lifecycle")
        .expect("lifecycle companion");
        assert_eq!(checkpoint.format_version, 2, "an authorized unresolved observation must persist V2");
        assert_eq!(checkpoint.records.len(), 1);
        let retained = &checkpoint.records[0];
        assert_eq!(retained.responsibility_id, before[0].responsibility_id);
        assert_eq!(retained.intent_digest, before[0].intent_digest);
        assert_eq!(retained.source_bucket_incarnation_id, None);
        assert!(matches!(retained.state, ResponsibilityState::SourceAndCurrentGenerationUnknown { .. }));
        assert_eq!(runtime.partial_writes.depth(), 1);
        assert_eq!(runtime.partial_writes.intents().next().expect("retained intent").scope, original.scope);
        assert_eq!(storage.calls.load(Ordering::Relaxed), 1);
        let (listed, _) = runtime.partial_writes.list_unverified_legacy(None, 1);
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].responsibility_id, before[0].responsibility_id);
        assert_eq!(listed[0].bucket_incarnation_id, None);
        assert_eq!(listed[0].source_bucket_incarnation_id, None);
        assert!(listed[0].held_since_ms.is_some());
    });
}

#[tokio::test(start_paused = true)]
async fn unknown_source_deleted_bucket_writer_does_not_promote_errors_or_disabled_observations() {
    for (enabled, lookup) in [
        (false, Lookup::Identity(None)),
        (true, Lookup::PermissionDenied),
        (true, Lookup::Quorum),
        (true, Lookup::Transport),
        (true, Lookup::Identity(Some(Uuid::nil()))),
        (true, Lookup::Pending),
    ] {
        let storage = Arc::new(ProbeStorage {
            lookup: Mutex::new(lookup),
            calls: AtomicUsize::new(0),
        });
        let manager = HealManager::new_without_root_recovery_for_test(storage.clone(), None);
        let mut runtime = runtime();
        runtime.config.lifecycle_v2_writer_enabled = enabled;
        let original = intent("writer-ineligible");
        let key = PartialWriteKey::new(&original, None);
        runtime
            .admit_partial_write(original, None)
            .expect("retain legacy responsibility");
        runtime.partial_writes.mark_persisted();
        runtime
            .partial_writes
            .entries
            .get_mut(&key)
            .expect("legacy entry")
            .next_attempt = Instant::now();
        let before = runtime.partial_writes.checkpoint_records(super::super::super::intent_digest);
        assert!(
            !runtime
                .partial_writes
                .dispatch_collecting_lifecycle_change(&manager, &runtime.config)
                .await
        );
        assert!(runtime.partial_writes.take_unresolved_generation_candidate().is_none());
        assert_eq!(runtime.partial_writes.checkpoint_records(super::super::super::intent_digest), before);
        assert!(runtime.partial_writes.entries[&key].next_attempt > Instant::now());
        assert_eq!(storage.calls.load(Ordering::Relaxed), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn unknown_source_deleted_bucket_writer_without_fleet_proof_keeps_v1_visible_and_backed_off() {
    let storage = Arc::new(ProbeStorage {
        lookup: Mutex::new(Lookup::Identity(None)),
        calls: AtomicUsize::new(0),
    });
    let manager = HealManager::new_without_root_recovery_for_test(storage.clone(), None);
    let mut runtime = runtime();
    runtime.config.lifecycle_v2_writer_enabled = true;
    let original = intent("writer-without-fleet");
    let key = PartialWriteKey::new(&original, None);
    runtime
        .admit_partial_write(original, None)
        .expect("retain legacy responsibility");
    runtime.partial_writes.mark_persisted();
    runtime
        .partial_writes
        .entries
        .get_mut(&key)
        .expect("legacy entry")
        .next_attempt = Instant::now();
    let before = runtime.partial_writes.checkpoint_records(super::super::super::intent_digest);
    for _ in 0..20 {
        runtime.dispatch_partial_writes(&manager).await;
    }
    assert_eq!(storage.calls.load(Ordering::Relaxed), 1);
    assert_eq!(runtime.lifecycle_format_version, 1);
    assert!(runtime.partial_writes.pending_lifecycle_transition.is_none());
    assert_eq!(runtime.partial_writes.checkpoint_records(super::super::super::intent_digest), before);
    let page = runtime
        .list_unverified_legacy(None, 1)
        .expect("legacy responsibility remains visible");
    assert_eq!(page.responsibilities.len(), 1);
    assert_eq!(page.responsibilities[0].responsibility_id, before[0].responsibility_id);
    assert_eq!(page.responsibilities[0].bucket_incarnation_id, None);

    // Earlier unresolved observations remain the retention origin even when
    // fleet unavailability forces another identity lookup before promotion.
    runtime
        .partial_writes
        .entries
        .get_mut(&key)
        .expect("legacy entry")
        .first_unresolved_at_ms = Some(7);
    tokio::time::advance(Duration::from_secs(61)).await;
    runtime.dispatch_partial_writes(&manager).await;
    assert_eq!(runtime.partial_writes.entries[&key].first_unresolved_at_ms, Some(7));
    tokio::time::advance(Duration::from_secs(61)).await;
    runtime
        .partial_writes
        .dispatch_collecting_lifecycle_change(&manager, &runtime.config)
        .await;
    let candidate = runtime
        .partial_writes
        .take_unresolved_generation_candidate()
        .expect("a later unresolved observation");
    assert!(matches!(
        candidate.record.state,
        ResponsibilityState::SourceAndCurrentGenerationUnknown { since_ms: 7, .. }
    ));
    assert_eq!(storage.calls.load(Ordering::Relaxed), 3);
}

#[test]
#[serial]
fn unknown_source_deleted_bucket_writer_failed_companion_keeps_owner_until_bounded_retry() {
    with_writer_fixture(|| async {
        use crate::heal::mrf_queue::{MRF_LIFECYCLE_PATHS, journal_disks, read_mrf_lifecycle_checkpoint};
        use crate::heal::storage_api::EcstoreDiskAPI;

        let storage = Arc::new(ProbeStorage {
            lookup: Mutex::new(Lookup::Identity(None)),
            calls: AtomicUsize::new(0),
        });
        let manager = HealManager::new_without_root_recovery_for_test(storage.clone(), None);
        let mut runtime = runtime();
        let original = intent("writer-failed-companion");
        let key = PartialWriteKey::new(&original, None);
        runtime
            .admit_partial_write(original, None)
            .expect("retain legacy responsibility");
        assert!(runtime.flush().await);
        let owner = runtime.runtime_checkpoint.expect("old checkpoint");
        let before = runtime.partial_writes.checkpoint_records(super::super::super::intent_digest);
        let mut obstacles = Vec::new();
        for disk in journal_disks().await {
            let path = EcstoreDiskAPI::path(disk.as_ref())
                .join(crate::heal::RUSTFS_META_BUCKET)
                .join(MRF_LIFECYCLE_PATHS[1]);
            std::fs::create_dir(&path).expect("inject an unwritable successor companion");
            obstacles.push(path);
        }
        runtime
            .partial_writes
            .entries
            .get_mut(&key)
            .expect("legacy entry")
            .next_attempt = Instant::now();
        runtime.dispatch_partial_writes(&manager).await;
        assert!(runtime.partial_writes.pending_lifecycle_transition.is_some());
        assert_eq!(runtime.runtime_checkpoint, Some(owner));
        assert_eq!(runtime.lifecycle_format_version, 1);
        assert_eq!(runtime.partial_writes.checkpoint_records(super::super::super::intent_digest), before);
        let old = read_mrf_lifecycle_checkpoint(
            owner.0,
            owner.1,
            runtime.config.journal_max_bytes,
            runtime.config.journal_max_bytes * 4,
        )
        .await
        .expect("read predecessor")
        .expect("predecessor companion");
        assert_eq!(old.format_version, 1);
        assert_eq!(old.records, before);
        assert_eq!(
            runtime
                .list_unverified_legacy(None, 1)
                .expect("pending owner remains queryable")
                .responsibilities[0]
                .responsibility_id,
            before[0].responsibility_id
        );
        assert!(matches!(
            runtime.admit_partial_write(intent("later"), None),
            Err(MrfDurableAdmissionError::Persistence)
        ));
        assert!(matches!(
            runtime
                .recheck_legacy_generation(&manager, before[0].responsibility_id, Uuid::new_v4())
                .await,
            Err(MrfLifecycleControlError::Persistence)
        ));
        runtime.dispatch_partial_writes(&manager).await;
        assert_eq!(storage.calls.load(Ordering::Relaxed), 1);
        assert!(!runtime.flush().await, "a consumer tick cannot bypass the retry deadline");
        for path in obstacles {
            std::fs::remove_dir(path).expect("remove only the empty injected obstacle");
        }
        runtime
            .partial_writes
            .pending_lifecycle_transition
            .as_mut()
            .expect("same pending transition")
            .next_attempt = Instant::now();
        assert!(runtime.flush().await, "the same owner retries after storage recovery");
        assert!(runtime.partial_writes.pending_lifecycle_transition.is_none());
        assert_eq!(runtime.runtime_checkpoint, Some((owner.0, owner.1 + 1)));
        let committed = read_mrf_lifecycle_checkpoint(
            owner.0,
            owner.1 + 1,
            runtime.config.journal_max_bytes,
            runtime.config.journal_max_bytes * 4,
        )
        .await
        .expect("read retry commit")
        .expect("retry companion");
        assert_eq!(committed.format_version, 2);
        assert_eq!(committed.records[0].responsibility_id, before[0].responsibility_id);
        assert_eq!(committed.records[0].source_bucket_incarnation_id, None);
    });
}

#[test]
#[serial]
fn unknown_source_deleted_bucket_writer_lost_result_is_resolved_by_exact_commit_readback() {
    with_writer_fixture(|| async {
        use crate::heal::mrf_queue::{
            MRF_LIFECYCLE_PATHS, encode_mrf_lifecycle_checkpoint, intent_digest, journal_disks, snapshot,
        };

        let storage = Arc::new(ProbeStorage {
            lookup: Mutex::new(Lookup::Identity(None)),
            calls: AtomicUsize::new(0),
        });
        let manager = HealManager::new_without_root_recovery_for_test(storage, None);
        let mut runtime = runtime();
        let original = intent("writer-lost-result");
        let key = PartialWriteKey::new(&original, None);
        runtime
            .admit_partial_write(original, None)
            .expect("retain legacy responsibility");
        for object in ["writer-lost-result-other-a", "writer-lost-result-other-b"] {
            runtime
                .admit_partial_write(intent(object), None)
                .expect("retain another responsibility");
        }
        assert!(runtime.flush().await);
        runtime
            .partial_writes
            .entries
            .get_mut(&key)
            .expect("legacy entry")
            .next_attempt = Instant::now();
        runtime
            .partial_writes
            .dispatch_collecting_lifecycle_change(&manager, &runtime.config)
            .await;
        let pending = runtime
            .partial_writes
            .take_unresolved_generation_candidate()
            .expect("unresolved proposal");
        let record = pending.record.clone();
        runtime.partial_writes.pending_lifecycle_transition = Some(pending);
        let sequence = runtime.next_checkpoint_sequence;
        let (payload, _) = runtime.snapshot();
        let lifecycle = encode_mrf_lifecycle_checkpoint(
            2,
            runtime.checkpoint_owner,
            sequence,
            {
                let mut records = runtime.partial_writes.checkpoint_records(intent_digest);
                *records
                    .iter_mut()
                    .find(|item| item.responsibility_id == record.responsibility_id)
                    .expect("pending responsibility") = record.clone();
                records
            },
            runtime.config.journal_max_bytes * 4,
        )
        .expect("exact candidate body");
        // Commit the proposal without delivering the result to its runtime.
        snapshot::publish_committed_snapshot_with_companion(
            &journal_disks().await,
            runtime.checkpoint_owner,
            sequence,
            &payload,
            runtime.config.journal_max_bytes,
            Some((&MRF_LIFECYCLE_PATHS, &lifecycle, runtime.config.journal_max_bytes * 4)),
        )
        .await
        .expect("commit before the simulated lost acknowledgement");
        crate::heal::storage_api::ecstore_start_remote_version_state_fleet_probe("changed-test-topology".to_owned());
        assert!(
            crate::heal::storage_api::ecstore_acquire_mrf_lifecycle_writer_fleet_proof(2)
                .await
                .is_none()
        );
        assert!(runtime.flush().await, "an exact committed result needs no new writer authority");
        assert_eq!(runtime.lifecycle_format_version, 2);
        assert_eq!(runtime.next_checkpoint_sequence, sequence + 1);
        let restored = runtime.partial_writes.checkpoint_records(intent_digest);
        assert_eq!(restored.len(), 3);
        assert_eq!(
            restored
                .iter()
                .find(|item| item.responsibility_id == record.responsibility_id),
            Some(&record)
        );
        assert_eq!(
            restored
                .iter()
                .filter(|item| matches!(item.state, ResponsibilityState::Active {}))
                .count(),
            2
        );
        assert!(runtime.partial_writes.pending_lifecycle_transition.is_none());
        let committed = snapshot::inspect_local_committed_snapshot(runtime.config.journal_max_bytes)
            .await
            .expect("inspect readback")
            .expect("committed snapshot");
        assert_eq!(committed.sequence(), sequence, "resolving a lost result must not publish a new sequence");
    });
}

#[test]
#[serial]
fn unknown_source_deleted_bucket_writer_failed_mirror_cannot_downgrade_a_committed_owner() {
    with_writer_fixture(|| async {
        use crate::heal::mrf_queue::{MRF_SCOPED_JOURNAL_PATH, journal_disks, read_mrf_lifecycle_checkpoint};
        use crate::heal::storage_api::EcstoreDiskAPI;

        let storage = Arc::new(ProbeStorage {
            lookup: Mutex::new(Lookup::Identity(None)),
            calls: AtomicUsize::new(0),
        });
        let manager = HealManager::new_without_root_recovery_for_test(storage, None);
        let mut runtime = runtime();
        let original = intent("writer-failed-mirror");
        let key = PartialWriteKey::new(&original, None);
        runtime.admit_partial_write(original, None).expect("retain responsibility");
        assert!(runtime.flush().await);
        let owner = runtime.runtime_checkpoint.expect("old checkpoint");
        let mut obstacles = Vec::new();
        for disk in journal_disks().await {
            let path = EcstoreDiskAPI::path(disk.as_ref())
                .join(crate::heal::RUSTFS_META_BUCKET)
                .join(MRF_SCOPED_JOURNAL_PATH);
            std::fs::remove_file(&path).expect("remove the isolated fixture mirror");
            std::fs::create_dir(&path).expect("prevent only compatibility-mirror publication");
            obstacles.push(path);
        }
        runtime
            .partial_writes
            .entries
            .get_mut(&key)
            .expect("legacy entry")
            .next_attempt = Instant::now();
        runtime.dispatch_partial_writes(&manager).await;
        assert_eq!(runtime.runtime_checkpoint, Some((owner.0, owner.1 + 1)));
        assert_eq!(runtime.lifecycle_format_version, 2);
        assert!(runtime.partial_writes.pending_lifecycle_transition.is_none());
        assert!(runtime.dirty, "the failed mirror must be retried");
        let committed = read_mrf_lifecycle_checkpoint(
            owner.0,
            owner.1 + 1,
            runtime.config.journal_max_bytes,
            runtime.config.journal_max_bytes * 4,
        )
        .await
        .expect("read committed owner")
        .expect("committed lifecycle");
        assert_eq!(committed.format_version, 2);
        assert!(matches!(
            committed.records[0].state,
            ResponsibilityState::SourceAndCurrentGenerationUnknown { .. }
        ));
        for path in obstacles {
            std::fs::remove_dir(path).expect("remove the empty mirror obstacle");
        }
        assert!(runtime.flush().await);
        assert!(!runtime.dirty);
        let retried = read_mrf_lifecycle_checkpoint(
            owner.0,
            owner.1 + 2,
            runtime.config.journal_max_bytes,
            runtime.config.journal_max_bytes * 4,
        )
        .await
        .expect("read repaired mirror owner")
        .expect("repaired lifecycle");
        assert_eq!(retried.format_version, 2);
        assert_eq!(retried.records, committed.records);
    });
}
