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
use crate::disk::error::DiskError;
use crate::storage_api_contracts::namespace::NamespaceLocking as _;
use codec::{fixture_inline_pool_meta_replicas as replicas, fixture_split_inline_pool_meta_replicas as split};
use std::path::PathBuf;
use tokio::fs;
use uuid::Uuid;

struct PhysicalFixture {
    _directories: Vec<tempfile::TempDir>,
    store: Arc<ECStore>,
    canonical: Vec<u8>,
    pending: Vec<u8>,
    identity: Vec<u8>,
    revision: PoolMetaRevision,
}

fn object_path(disk: &DiskStore) -> PathBuf {
    disk.get_object_path_if_local(RUSTFS_META_BUCKET, POOL_META_NAME)
        .expect("physical fixture must use local disks")
        .expect("pool metadata path should resolve")
}

async fn disks(store: &ECStore) -> Vec<Vec<DiskStore>> {
    let mut all = Vec::with_capacity(store.pools.len());
    for pool in &store.pools {
        let set = pool.get_disks_by_key(POOL_META_NAME);
        let disks = set
            .disks
            .read()
            .await
            .iter()
            .cloned()
            .map(|disk| disk.expect("physical fixture must keep every disk online"))
            .collect::<Vec<_>>();
        assert_eq!(disks.len(), 4, "fixture requires an EC 2+2 set");
        all.push(disks);
    }
    all
}

async fn owner(store: &ECStore) -> Arc<rustfs_lock::NamespaceLockGuard> {
    let lock = store.pools[0]
        .new_ns_lock(RUSTFS_META_BUCKET, POOL_META_NAME)
        .await
        .expect("real namespace lock should be created");
    Arc::new(
        lock.get_write_lock(get_lock_acquire_timeout())
            .await
            .expect("physical recovery must hold the namespace writer"),
    )
}

fn clean_state(fixture: &PhysicalFixture) -> PoolMetaWriteState {
    PoolMetaWriteState::for_startup(fixture.revision.cluster_id.expect("fixture V3 revision requires identity"), false)
}

async fn snapshots(store: &ECStore) -> Vec<Vec<PoolMetaNativeInspection>> {
    let mut all = Vec::new();
    for pool in disks(store).await {
        let mut current = Vec::new();
        for disk in pool {
            match disk
                .pool_meta_native_recovery(Request::Inspect {
                    object: PoolMetaNativeObject::PoolBin,
                })
                .await
                .expect("physical native inspection should succeed")
            {
                Response::Inspected(inspection) => current.push(inspection),
                _ => panic!("inspection must return a disk snapshot"),
            }
        }
        all.push(current);
    }
    all
}

async fn fixture(split_pools: usize) -> PhysicalFixture {
    let (directories, store, _other_store) =
        crate::services::rebalance::test_three_pool_stores_with_four_disk_sets_with_isolated_node_contexts(None).await;
    persist_v3_pool_meta_for_test(&store).await;
    let previous = load_pool_meta_replicas(store.pools.clone(), true)
        .await
        .expect("baseline V3 pool metadata must be ordinary-readable");
    let revision = PoolMetaRevision {
        generation: previous
            .revision
            .generation
            .checked_add(1)
            .expect("test generation should increment"),
        transaction_id: Some(uuid::Uuid::new_v4()),
        ..previous.revision
    };
    let candidate = PoolMetaCommittedCandidate {
        canonical: previous.canonical.expect("baseline must have a canonical V3 COMMIT"),
        meta: previous.meta.clone(),
        revision: previous.revision,
    };
    let canonical = encode_pool_meta_v3_envelope(&previous.meta, revision, true, None).expect("next COMMIT should encode");
    let pending =
        encode_pool_meta_v3_envelope(&previous.meta, revision, false, Some(&candidate)).expect("next PREPARE should encode");
    let identity = load_pool_meta_identity(
        store.pools.clone(),
        revision
            .cluster_id
            .expect("baseline V3 metadata should have a cluster identity"),
    )
    .await
    .expect("real initialized identities must agree")
    .identity
    .expect("real fixture identity must be present");
    let identity = encode_pool_meta_identity(identity).expect("real fixture identity should encode canonically");
    let modified = OffsetDateTime::now_utc();
    let split = split(&pending, &canonical, modified).await;
    let commit = replicas(&canonical, modified + Duration::seconds(1)).await;
    let prepared = replicas(&pending, modified).await;
    let directory = uuid::Uuid::new_v4();
    for (pool_index, pool) in disks(&store).await.into_iter().enumerate() {
        let heads = if pool_index < split_pools { &split } else { &commit };
        for (disk_index, disk) in pool.into_iter().enumerate() {
            let object = object_path(&disk);
            fs::remove_dir_all(&object)
                .await
                .expect("fault injection should remove only its own pool.bin fixture");
            fs::create_dir_all(object.join(directory.to_string()))
                .await
                .expect("fixture rollback directory should be created");
            fs::write(object.join("xl.meta"), &heads[disk_index])
                .await
                .expect("physical split head should be installed");
            fs::write(object.join(directory.to_string()).join("xl.meta.bkp"), &prepared[disk_index])
                .await
                .expect("physical PREPARE backup should be installed");
        }
    }
    PhysicalFixture {
        _directories: directories,
        store,
        canonical,
        pending,
        identity,
        revision,
    }
}

async fn assert_committed(fixture: &PhysicalFixture) {
    let confirmed = load_pool_meta_replicas(fixture.store.pools.clone(), true)
        .await
        .expect("ordinary full-pool read must succeed after physical recovery");
    assert_eq!(confirmed.canonical.as_deref(), Some(fixture.canonical.as_slice()));
    assert_eq!(confirmed.revision, fixture.revision);
    assert!(!confirmed.replica_state.needs_repair);
    assert!(confirmed.replica_state.repair_write_safe);
    for pool in snapshots(&fixture.store).await {
        let raw = pool
            .iter()
            .map(|disk| disk.current.clone().expect("every recovered physical head must exist"))
            .collect::<Vec<_>>();
        assert_eq!(
            codec::decode_inline_pool_meta_replicas(&raw)
                .await
                .expect("every recovered shard must verify"),
            vec![Bytes::copy_from_slice(&fixture.canonical)]
        );
    }
}

#[tokio::test]
#[serial_test::serial(pool_meta_version_env)]
async fn pool_meta_native_physical_split_repair_completes_normal_cas_and_quorum_readback() {
    let fixture = fixture(1).await;
    let before = read_pool_meta_replicas(fixture.store.pools.clone(), true).await;
    assert!(matches!(before[0].replica, PoolMetaReplica::Unreadable(_)));
    assert!(matches!(before[1].replica, PoolMetaReplica::Valid { committed: true, .. }));
    let mut state = clean_state(&fixture);
    let fence = PoolMetaPersistenceFence::Owned(owner(&fixture.store).await);
    assert!(
        repair(&fixture.store, &mut state, &fence)
            .await
            .expect("healthy ordinary COMMIT must authorize physical repair")
    );
    state
        .ensure_write_safe("physical recovery readback")
        .expect("completed recovery must disarm its transaction");
    assert_committed(&fixture).await;
}

#[tokio::test]
#[serial_test::serial(pool_meta_version_env)]
async fn pool_meta_native_physical_split_without_ordinary_commit_performs_no_mutation() {
    let fixture = fixture(3).await;
    let before = snapshots(&fixture.store).await;
    let mut state = clean_state(&fixture);
    let fence = PoolMetaPersistenceFence::Owned(owner(&fixture.store).await);
    assert!(
        repair(&fixture.store, &mut state, &fence).await.is_err(),
        "reconstruction alone must never authorize a COMMIT"
    );
    assert_eq!(
        snapshots(&fixture.store).await,
        before,
        "all current heads, identities and complete backups must remain unchanged"
    );
    assert_eq!(state.active_transactions.load(Ordering::SeqCst), 0);
}

struct IdentityEpochFork {
    path: PathBuf,
    original: Bytes,
    replacement: Bytes,
    original_identity: PersistedPoolMetaIdentity,
}

async fn identity_epoch_fork(fixture: &PhysicalFixture) -> IdentityEpochFork {
    let identity_set = fixture.store.pools[0].get_disks_by_key(POOL_META_IDENTITY_NAME);
    assert!(
        !Arc::ptr_eq(&identity_set, &fixture.store.pools[0].get_disks_by_key(POOL_META_NAME)),
        "the identity evidence must come from its independently placed physical set"
    );
    let identity_disks = identity_set
        .disks
        .read()
        .await
        .iter()
        .cloned()
        .map(|disk| disk.expect("actual identity set must retain all four disks"))
        .collect::<Vec<_>>();
    let mut raw = Vec::with_capacity(identity_disks.len());
    for disk in &identity_disks {
        match disk
            .pool_meta_native_recovery(Request::InspectIdentity {})
            .await
            .expect("actual identity set must support native inspection")
        {
            Response::IdentityInspected { disk_id, raw: current } => {
                assert!(!disk_id.is_nil());
                raw.push(current);
            }
            _ => panic!("identity inspection must return its physical raw shard"),
        }
    }
    assert_eq!(raw.len(), 4);
    let original_identity = match decode_pool_meta_identity(&fixture.identity) {
        PoolMetaIdentityReplica::Valid(identity) => identity,
        _ => panic!("fixture must provide a valid initialized identity"),
    };
    let forked_identity = PersistedPoolMetaIdentity {
        epoch: original_identity
            .epoch
            .checked_add(1)
            .expect("test identity epoch should increment"),
        ..original_identity
    };
    let forked = encode_pool_meta_identity(forked_identity).expect("foreign epoch identity should remain validly encoded");
    assert!(matches!(decode_pool_meta_identity(&forked), PoolMetaIdentityReplica::Valid(_)));
    let modified = raw
        .iter()
        .map(|raw| {
            codec::parse_inline_info(raw)
                .expect("actual identity shard layout should parse")
                .mod_time
        })
        .collect::<Option<Vec<_>>>()
        .expect("every actual identity shard should have a modification time")
        .into_iter()
        .max()
        .expect("actual identity set should contain four shards")
        + Duration::nanoseconds(1);
    let replacements = codec::encode_inline_pool_meta_replicas(&raw, &forked, modified)
        .await
        .expect("epoch fork must retain the actual set's valid erasure layout");
    assert_eq!(
        codec::parse_inline_info(&raw[0]).expect("original layout").erasure,
        codec::parse_inline_info(&replacements[0])
            .expect("replacement layout")
            .erasure
    );
    codec::validate_inline_pool_meta_shard(&replacements[0], 0, &forked)
        .await
        .expect("forked shard must pass the physical integrity checks");
    let identity_path = identity_disks[0]
        .get_object_path_if_local(RUSTFS_META_BUCKET, POOL_META_IDENTITY_NAME)
        .expect("identity fixture must use local storage")
        .expect("actual identity metadata path should resolve");
    IdentityEpochFork {
        path: identity_path.join("xl.meta"),
        original: raw[0].clone(),
        replacement: replacements[0].clone(),
        original_identity,
    }
}

#[tokio::test]
#[serial_test::serial(pool_meta_version_env)]
async fn pool_meta_native_physical_identity_actual_set_minority_epoch_fork_performs_no_apply() {
    let fixture = fixture(1).await;
    let fork = identity_epoch_fork(&fixture).await;
    let heads = |pools: Vec<Vec<PoolMetaNativeInspection>>| {
        pools
            .into_iter()
            .map(|pool| pool.into_iter().map(|disk| (disk.current, disk.backups)).collect::<Vec<_>>())
            .collect::<Vec<_>>()
    };
    let before = heads(snapshots(&fixture.store).await);
    fs::write(&fork.path, &fork.replacement)
        .await
        .expect("only one actual identity-set disk should receive the epoch fork");
    let selected = load_pool_meta_identity(fixture.store.pools.clone(), fork.original_identity.cluster_id)
        .await
        .expect("three matching identity shards must remain ordinary-readable");
    assert_eq!(selected.identity, Some(fork.original_identity));
    let reads = read_pool_meta_replicas(fixture.store.pools.clone(), true).await;
    assert!(matches!(reads[0].replica, PoolMetaReplica::Unreadable(_)));
    assert!(matches!(reads[1].replica, PoolMetaReplica::Valid { committed: true, .. }));
    let mut state = clean_state(&fixture);
    let fence = PoolMetaPersistenceFence::Owned(owner(&fixture.store).await);
    let error = repair(&fixture.store, &mut state, &fence)
        .await
        .expect_err("ordinary identity quorum must not hide a physical minority epoch fork");
    assert_eq!(
        error
            .pool_metadata_failure()
            .expect("known identity fork must retain typed failure")
            .kind,
        crate::error::PoolMetadataFailure::RecoveryRequired
    );
    assert!(state.write_blocked, "known minority fork must latch the write gate");
    assert_eq!(
        heads(snapshots(&fixture.store).await),
        before,
        "identity rejection must precede every pool.bin Apply"
    );
    assert_eq!(state.active_transactions.load(Ordering::SeqCst), 0);
    fs::write(&fork.path, &fork.original)
        .await
        .expect("restoring the input identity must not clear the observed fork latch");
    let error = repair(&fixture.store, &mut state, &fence)
        .await
        .expect_err("the same state must retain operator recovery after identity becomes consistent");
    assert_eq!(
        error
            .pool_metadata_failure()
            .expect("sticky failure must retain its classification")
            .kind,
        crate::error::PoolMetadataFailure::RecoveryRequired
    );
    assert!(state.write_blocked);
    assert_eq!(heads(snapshots(&fixture.store).await), before);
}

#[tokio::test]
#[serial_test::serial(pool_meta_version_env)]
async fn pool_meta_native_physical_runtime_fork_after_apply_latches_original_gate() {
    let fixture = fixture(1).await;
    let fork = identity_epoch_fork(&fixture).await;
    let since = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("fixed original failure timestamp should be valid");
    {
        let mut state = fixture.store.pool_meta_save_gate.lock().await;
        assert_eq!(state.active_transactions.load(Ordering::SeqCst), 0);
        state.write_blocked = false;
        state.block_context = None;
        state.aborted_transaction.store(true, Ordering::SeqCst);
        let mut failure = pool_metadata_error(crate::error::PoolMetadataFailure::TransactionUnknown, "commit_cas", None);
        failure.since = since;
        *state
            .transaction_failure
            .lock()
            .expect("original transaction failure mutex should lock") = Some(failure);
        *state
            .block_started_at
            .lock()
            .expect("original block timestamp mutex should lock") = Some(since);
    }
    let pause = Arc::new(NativeApplyPause {
        reached: tokio::sync::Notify::new(),
        resume: tokio::sync::Notify::new(),
    });
    let reached = pause.reached.notified();
    tokio::pin!(reached);
    reached.as_mut().enable();
    let recovery = tokio::spawn({
        let store = Arc::clone(&fixture.store);
        let pause = Arc::clone(&pause);
        async move {
            AFTER_NATIVE_APPLY
                .scope(pause, async move { store.recover_pool_meta_transaction().await })
                .await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), reached)
        .await
        .expect("real runtime recovery should pause after completing physical Apply");
    let readable = load_pool_meta_replicas(fixture.store.pools.clone(), true)
        .await
        .expect("physical Apply must already make every pool ordinary-readable");
    assert_eq!(readable.canonical.as_deref(), Some(fixture.canonical.as_slice()));
    fs::write(&fork.path, &fork.replacement)
        .await
        .expect("a valid minority identity fork should arrive after physical repair");
    pause.resume.notify_one();
    let error = tokio::time::timeout(std::time::Duration::from_secs(10), recovery)
        .await
        .expect("resumed runtime recovery should finish its validation")
        .expect("runtime recovery task should not panic")
        .expect_err("post-Apply identity fork must reject runtime recovery");
    assert_eq!(
        error
            .pool_metadata_failure()
            .expect("runtime fork failure must remain typed")
            .kind,
        crate::error::PoolMetadataFailure::RecoveryRequired
    );
    assert_committed(&fixture).await;
    {
        let state = fixture.store.pool_meta_save_gate.lock().await;
        assert!(
            state.write_blocked,
            "candidate validation must propagate its permanent latch to the original state"
        );
        let context = state
            .block_context
            .as_ref()
            .expect("original state must retain the known fork context");
        assert_eq!(context.kind, crate::error::PoolMetadataFailure::RecoveryRequired);
        assert_eq!(context.since, since, "candidate failure must preserve the original outage timestamp");
        assert_eq!(*state.block_started_at.lock().expect("original block timestamp should lock"), Some(since));
    }
    assert!(
        !fixture
            .store
            .recover_pool_meta_transaction()
            .await
            .expect("permanently blocked recovery should return without mutation"),
        "ordinary-readable repaired heads must not clear the original known-fork latch"
    );
    let state = fixture.store.pool_meta_save_gate.lock().await;
    assert!(state.write_blocked);
    assert_eq!(
        state
            .block_context
            .as_ref()
            .expect("sticky fork context must remain present")
            .since,
        since
    );
}

#[tokio::test]
#[serial_test::serial(pool_meta_version_env)]
async fn pool_meta_native_physical_partial_apply_resumes_after_conditional_rejection() {
    let fixture = fixture(1).await;
    let fence = PoolMetaPersistenceFence::Owned(owner(&fixture.store).await);
    let inspected = inspect(&fixture.store.pools, &fence)
        .await
        .expect("real native snapshots should be obtained");
    let replacements = plan(&inspected, &fixture.canonical, fixture.revision, &fixture.identity)
        .await
        .expect("physical recovery plan should validate");
    let snapshot = &inspected[0].snapshots[0];
    assert_eq!(
        inspected[0].disks[0]
            .pool_meta_native_recovery(Request::Apply {
                object: PoolMetaNativeObject::PoolBin,
                expected_disk_id: snapshot.disk_id,
                expected_current_sha256: snapshot.current.as_ref().map(|raw| Sha256::digest(raw).into()),
                expected_backup_inventory_sha256: snapshot.inventory_sha256().expect("snapshot inventory should hash"),
                replacement: replacements[0][0].clone(),
            })
            .await
            .expect("first real Apply must atomically publish and sync"),
        Response::Applied
    );
    let before_rejected = snapshots(&fixture.store).await;
    let snapshot = &inspected[0].snapshots[1];
    let mut stale_inventory = snapshot.inventory_sha256().expect("snapshot inventory should hash");
    stale_inventory[0] ^= 1;
    assert!(matches!(
        inspected[0].disks[1]
            .pool_meta_native_recovery(Request::Apply {
                object: PoolMetaNativeObject::PoolBin,
                expected_disk_id: snapshot.disk_id,
                expected_current_sha256: snapshot.current.as_ref().map(|raw| Sha256::digest(raw).into()),
                expected_backup_inventory_sha256: stale_inventory,
                replacement: replacements[0][1].clone(),
            })
            .await,
        Err(DiskError::ConditionalDeleteConflict)
    ));
    assert_eq!(
        snapshots(&fixture.store).await,
        before_rejected,
        "rejected Apply must preserve the partial repair evidence"
    );
    drop(inspected);
    drop(fence);
    let fence = PoolMetaPersistenceFence::Owned(owner(&fixture.store).await);
    let mut state = clean_state(&fixture);
    assert!(
        repair(&fixture.store, &mut state, &fence)
            .await
            .expect("a fresh owner should resume the partial physical repair")
    );
    assert_committed(&fixture).await;
}

#[tokio::test]
#[serial_test::serial(pool_meta_version_env)]
async fn pool_meta_native_physical_noninline_conditional_io_round_trip() {
    let fixture = fixture(0).await;
    let pool = Arc::clone(&fixture.store.pools[0]);
    let fence = PoolMetaPersistenceFence::Owned(owner(&fixture.store).await);
    let (original, info) = read_config_no_lock_preserve_empty_with_metadata(Arc::clone(&pool), POOL_META_NAME)
        .await
        .expect("baseline conditional object must be readable");
    let token = pool_meta_cas_token_from_info(info);
    // This smoke test targets the physical conditional I/O boundary. The large
    // opaque body deliberately exceeds the inline writer's one-block limit.
    let payload = vec![0x5a; rustfs_filemeta::BLOCK_SIZE_V2 + 137];
    let state = clean_state(&fixture);
    let mut arm = state.arm_transaction();
    save_pool_meta_object_cas(
        Arc::clone(&pool),
        POOL_META_NAME,
        payload.clone(),
        &token,
        &fence,
        "noninline_smoke",
        &mut arm,
    )
    .await
    .expect("normal native CAS must support noninline pool metadata storage");
    let (readback, info) = read_config_no_lock_preserve_empty_with_metadata(Arc::clone(&pool), POOL_META_NAME)
        .await
        .expect("noninline conditional write must be ordinary-readable");
    assert_eq!(readback, payload);
    for disk in &snapshots(&fixture.store).await[0] {
        let raw = disk
            .current
            .as_ref()
            .expect("noninline head should be installed on every disk");
        let info = rustfs_filemeta::get_file_info(
            raw,
            RUSTFS_META_BUCKET,
            POOL_META_NAME,
            "",
            rustfs_filemeta::FileInfoOpts {
                data: true,
                include_free_versions: false,
                include_part_checksums: true,
            },
        )
        .expect("physical noninline metadata should decode");
        assert!(!info.inline_data());
        assert!(info.data.is_none());
    }
    save_pool_meta_object_cas(
        pool,
        POOL_META_NAME,
        original.clone(),
        &pool_meta_cas_token_from_info(info),
        &fence,
        "noninline_to_inline_smoke",
        &mut arm,
    )
    .await
    .expect("normal CAS must also replace a noninline parent with inline metadata");
    arm.disarm();
    assert_committed(&fixture).await;
}

#[tokio::test]
#[serial_test::serial(pool_meta_version_env)]
async fn pool_meta_native_physical_three_plus_one_checkpoint_converges_all_shards() {
    let fixture = fixture(0).await;
    let pending = replicas(&fixture.pending, OffsetDateTime::now_utc() - Duration::seconds(1)).await;
    let all_disks = disks(&fixture.store).await;
    fs::write(object_path(&all_disks[0][0]).join("xl.meta"), &pending[0])
        .await
        .expect("one stale physical PREPARE head should be installed");
    let selection = load_pool_meta_replicas(fixture.store.pools.clone(), true)
        .await
        .expect("three matching COMMIT shards must remain ordinary-readable");
    assert_eq!(selection.canonical.as_deref(), Some(fixture.canonical.as_slice()));
    let guard = owner(&fixture.store).await;
    let fence = PoolMetaPersistenceFence::Owned(Arc::clone(&guard));
    let mut state = clean_state(&fixture);
    assert!(
        !repair(&fixture.store, &mut state, &fence)
            .await
            .expect("ordinary-readable pools should use normal checkpoint recovery")
    );
    selection
        .meta
        .save_for_startup_guarded(fixture.store.pools.clone(), &mut state, guard)
        .await
        .expect("normal checkpoint must converge a readable 3+1 pool without an unsafe native overwrite");
    let checkpoint = load_pool_meta_replicas(fixture.store.pools.clone(), true)
        .await
        .expect("checkpoint must remain ordinary-readable across all pools");
    assert!(!checkpoint.replica_state.needs_repair);
    assert!(checkpoint.replica_state.repair_write_safe);
    assert!(checkpoint.revision.generation > fixture.revision.generation);
    let canonical = checkpoint.canonical.expect("checkpoint should expose a canonical COMMIT");
    for pool in snapshots(&fixture.store).await {
        let raw = pool
            .into_iter()
            .map(|disk| disk.current.expect("checkpoint head must exist"))
            .collect::<Vec<_>>();
        assert_eq!(
            codec::decode_inline_pool_meta_replicas(&raw)
                .await
                .expect("all four checkpoint shards must verify"),
            vec![Bytes::copy_from_slice(&canonical)]
        );
    }
}

async fn owner_repair_no_backups_fixture() -> PhysicalFixture {
    let fixture = fixture(0).await;
    for pool in disks(&fixture.store).await {
        for disk in pool {
            let object = object_path(&disk);
            let mut entries = fs::read_dir(&object).await.expect("fixture metadata directory");
            while let Some(entry) = entries.next_entry().await.expect("fixture directory entry") {
                if entry.file_type().await.expect("fixture entry type").is_dir() {
                    fs::remove_dir_all(entry.path())
                        .await
                        .expect("remove task-owned fixture rollback directory");
                }
            }
        }
    }
    fixture
}

async fn owner_repair_identity_heads(store: &ECStore) -> Vec<Vec<(Uuid, Bytes)>> {
    let mut all = Vec::new();
    for pool in &store.pools {
        let set = pool.get_disks_by_key(POOL_META_IDENTITY_NAME);
        let members = set
            .disks
            .read()
            .await
            .iter()
            .cloned()
            .map(|disk| disk.expect("identity fixture disk must be online"))
            .collect::<Vec<_>>();
        let mut heads = Vec::new();
        for disk in members {
            match disk
                .pool_meta_native_recovery(Request::InspectIdentity {})
                .await
                .expect("physical identity inspection")
            {
                Response::IdentityInspected { disk_id, raw } => heads.push((disk_id, raw)),
                _ => panic!("identity inspection must return its physical head"),
            }
        }
        all.push(heads);
    }
    all
}

async fn legacy_native_owner_fixture(version: u16, stale: bool) -> (PhysicalFixture, Vec<u8>) {
    let fixture = owner_repair_no_backups_fixture().await;
    let mut meta = match decode_pool_meta_replica(fixture.canonical.clone()) {
        PoolMetaReplica::Valid {
            meta, committed: true, ..
        } => meta,
        _ => panic!("fixture must provide valid committed pool metadata"),
    };
    meta.version = version;
    let raw = meta
        .encode_config_data_for_v2_gate(version == POOL_META_VERSION)
        .expect("legacy fixture should encode in its original version");
    assert_eq!(u16::from_le_bytes([raw[2], raw[3]]), version);
    let all = disks(&fixture.store).await;
    let now = OffsetDateTime::now_utc();
    for pool in &all {
        let heads = replicas(&raw, now).await;
        for (index, disk) in pool.iter().enumerate() {
            fs::write(object_path(disk).join("xl.meta"), &heads[index])
                .await
                .expect("install exact legacy head");
        }
    }
    if stale {
        let old = replicas(&raw, now - Duration::seconds(10)).await;
        fs::write(object_path(&all[0][0]).join("xl.meta"), &old[0])
            .await
            .expect("install stale same-raw legacy head");
    }
    (fixture, raw)
}

#[tokio::test]
#[serial_test::serial(pool_meta_version_env)]
async fn pool_meta_native_heal_v1_with_all_upgrade_gates_disabled_keeps_exact_raw() {
    temp_env::async_with_vars(
        [
            (rustfs_config::ENV_POOL_META_V2_WRITE, Some("false")),
            (rustfs_config::ENV_POOL_META_V2_FLEET_CONFIRMED, Some("false")),
            (rustfs_config::ENV_POOL_META_V3_WRITE, Some("false")),
            (rustfs_config::ENV_POOL_META_V3_FLEET_CONFIRMED, Some("false")),
        ],
        async {
            let (fixture, raw) = legacy_native_owner_fixture(POOL_META_V1_VERSION, false).await;
            let guard = owner(&fixture.store).await;
            let mut state = fixture.store.pool_meta_save_gate.lock().await;
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                fixture.store.heal_pool_meta_replica_under_fence(0, guard, &mut state),
            )
            .await
            .expect("legacy native heal must terminate");
            assert!(result.is_ok(), "fully online legacy native heal should succeed: {result:?}");
            drop(state);
            let reads = read_pool_meta_replicas(fixture.store.pools.clone(), true).await;
            for read in &reads {
                assert!(
                    matches!(&read.replica, PoolMetaReplica::Valid { raw: bytes, revision, .. }
                    if bytes == &raw && revision.version == POOL_META_V1_VERSION),
                    "native heal must not upgrade a V1 raw payload when both upgrade gates are disabled"
                );
            }
        },
    )
    .await;
}

#[tokio::test]
#[serial_test::serial(pool_meta_version_env)]
async fn pool_meta_native_heal_rejects_legacy_source_with_trailing_bytes_without_writes() {
    temp_env::async_with_vars(
        [
            (rustfs_config::ENV_POOL_META_V2_WRITE, Some("false")),
            (rustfs_config::ENV_POOL_META_V2_FLEET_CONFIRMED, Some("false")),
            (rustfs_config::ENV_POOL_META_V3_WRITE, Some("false")),
            (rustfs_config::ENV_POOL_META_V3_FLEET_CONFIRMED, Some("false")),
        ],
        async {
            for version in [POOL_META_V1_VERSION, POOL_META_VERSION] {
                let (fixture, raw) = legacy_native_owner_fixture(version, false).await;
                let mut source_raw = raw.clone();
                // A second MessagePack value is readable by legacy decoding but
                // must not be copied as part of an unverified repair payload.
                source_raw.push(0xc0);
                assert!(
                    matches!(decode_pool_meta_replica(source_raw.clone()), PoolMetaReplica::Valid { raw, .. } if raw == source_raw),
                    "ordinary legacy reads must remain compatible with trailing bytes"
                );
                let all_disks = disks(&fixture.store).await;
                let source_heads = replicas(&source_raw, OffsetDateTime::now_utc() + Duration::seconds(1)).await;
                for (index, disk) in all_disks[0].iter().enumerate() {
                    fs::write(object_path(disk).join("xl.meta"), &source_heads[index])
                        .await
                        .expect("install readable legacy source with trailing bytes");
                }
                let selection = load_pool_meta_replicas(fixture.store.pools.clone(), true)
                    .await
                    .expect("legacy semantic selection must remain readable");
                assert_eq!(selection.legacy_raw.as_deref(), Some(source_raw.as_slice()));
                let before = snapshots(&fixture.store).await;
                let identities = owner_repair_identity_heads(&fixture.store).await;
                let guard = owner(&fixture.store).await;
                let mut state = fixture.store.pool_meta_save_gate.lock().await;
                let result = tokio::time::timeout(
                    std::time::Duration::from_secs(30),
                    fixture.store.heal_pool_meta_replica_under_fence(1, guard, &mut state),
                )
                .await
                .expect("legacy native heal must terminate");
                drop(state);
                assert!(result.is_err(), "version {version}: unparsed trailing source bytes must not be republished");
                assert_eq!(snapshots(&fixture.store).await, before, "version {version}: no pool.bin head or backup may change");
                assert_eq!(
                    owner_repair_identity_heads(&fixture.store).await,
                    identities,
                    "version {version}: no identity shard may change"
                );
                let reads = read_pool_meta_replicas(fixture.store.pools.clone(), true).await;
                assert!(matches!(&reads[0].replica, PoolMetaReplica::Valid { raw, .. } if raw == &source_raw));
                assert!(matches!(&reads[1].replica, PoolMetaReplica::Valid { raw: target, .. } if target == &raw));
            }
        },
    )
    .await;
}

#[tokio::test]
#[serial_test::serial(pool_meta_version_env)]
async fn pool_meta_native_heal_mixed_v1_v2_preserves_observed_format_floor() {
    temp_env::async_with_vars(
        [
            (rustfs_config::ENV_POOL_META_V2_WRITE, Some("false")),
            (rustfs_config::ENV_POOL_META_V2_FLEET_CONFIRMED, Some("false")),
            (rustfs_config::ENV_POOL_META_V3_WRITE, Some("false")),
            (rustfs_config::ENV_POOL_META_V3_FLEET_CONFIRMED, Some("false")),
        ],
        async {
            for first_version in [POOL_META_V1_VERSION, POOL_META_VERSION] {
                for target_pool in [0, 1] {
                    let (fixture, raw_v1) = legacy_native_owner_fixture(POOL_META_V1_VERSION, false).await;
                    let mut meta = match decode_pool_meta_replica(raw_v1.clone()) {
                        PoolMetaReplica::Valid { meta, committed: true, .. } => meta,
                        _ => panic!("initial V1 metadata must decode"),
                    };
                    meta.version = POOL_META_VERSION;
                    let raw_v2 = meta.encode_config_data_for_v2_gate(true).expect("equivalent V2 payload must encode");
                    assert_eq!(u16::from_le_bytes([raw_v1[2], raw_v1[3]]), POOL_META_V1_VERSION);
                    assert_eq!(u16::from_le_bytes([raw_v2[2], raw_v2[3]]), POOL_META_VERSION);
                    let semantic_v1 = match decode_pool_meta_replica(raw_v1.clone()) {
                        PoolMetaReplica::Valid { canonical, .. } => canonical,
                        _ => panic!("V1 payload must remain valid"),
                    };
                    let semantic_v2 = match decode_pool_meta_replica(raw_v2.clone()) {
                        PoolMetaReplica::Valid { canonical, .. } => canonical,
                        _ => panic!("V2 payload must remain valid"),
                    };
                    assert_eq!(semantic_v1, semantic_v2, "the two formats must carry equivalent metadata");
                    let other_version = if first_version == POOL_META_V1_VERSION { POOL_META_VERSION } else { POOL_META_V1_VERSION };
                    // Pool two follows pool zero; pools zero and one cover both
                    // orderings and both target formats in separate runs.
                    let versions = [first_version, other_version, first_version];
                    let physical = disks(&fixture.store).await;
                    let timestamp = OffsetDateTime::now_utc();
                    for (pool_index, pool) in physical.iter().enumerate() {
                        let raw = if versions[pool_index] == POOL_META_V1_VERSION { &raw_v1 } else { &raw_v2 };
                        let heads = replicas(raw, timestamp).await;
                        for (index, disk) in pool.iter().enumerate() {
                            fs::write(object_path(disk).join("xl.meta"), &heads[index]).await.expect("install mixed legacy pool head");
                        }
                    }
                    let before = snapshots(&fixture.store).await;
                    let selection = load_pool_meta_replicas(fixture.store.pools.clone(), true)
                        .await.expect("equivalent legacy pools should be selectable");
                    assert_eq!(selection.revision.version, POOL_META_VERSION, "the observed V2 format floor must be retained");
                    assert_eq!(selection.canonical.as_deref(), Some(semantic_v2.as_slice()));
                    let guard = owner(&fixture.store).await;
                    let mut state = fixture.store.pool_meta_save_gate.lock().await;
                    tokio::time::timeout(std::time::Duration::from_secs(30),
                        fixture.store.heal_pool_meta_replica_under_fence(target_pool, guard, &mut state))
                        .await.expect("mixed legacy heal must terminate")
                        .expect("equivalent legacy metadata should heal without downgrading the format floor");
                    drop(state);
                    let after = snapshots(&fixture.store).await;
                    let reads = read_pool_meta_replicas(fixture.store.pools.clone(), true).await;
                    for (pool_index, read) in reads.iter().enumerate() {
                        let expected_version = if pool_index == target_pool { POOL_META_VERSION } else { versions[pool_index] };
                        let expected_raw = if expected_version == POOL_META_V1_VERSION { &raw_v1 } else { &raw_v2 };
                        assert!(matches!(&read.replica,
                            PoolMetaReplica::Valid { raw, canonical, revision, committed: true, .. }
                                if raw == expected_raw && canonical == &semantic_v2 && revision.version == expected_version
                        ), "pool {pool_index}, first={first_version}, target={target_pool}: preserve equivalent payload and expected legacy version");
                        for (index, snapshot) in after[pool_index].iter().enumerate() {
                            codec::validate_inline_pool_meta_shard(snapshot.current.as_ref().expect("physical head"), index, expected_raw)
                                .await.expect("every physical shard must match its selected legacy payload");
                        }
                        if pool_index != target_pool {
                            assert_eq!(after[pool_index], before[pool_index], "healing one pool must not modify other pools");
                        }
                    }
                }
            }
        },
    ).await;
}

#[tokio::test]
#[serial_test::serial(pool_meta_version_env)]
async fn pool_meta_native_heal_rejects_v1_and_v3_prepare_in_either_pool_order() {
    temp_env::async_with_vars(
        [
            (rustfs_config::ENV_POOL_META_V2_WRITE, Some("false")),
            (rustfs_config::ENV_POOL_META_V2_FLEET_CONFIRMED, Some("false")),
            (rustfs_config::ENV_POOL_META_V3_WRITE, Some("false")),
            (rustfs_config::ENV_POOL_META_V3_FLEET_CONFIRMED, Some("false")),
        ],
        async {
            for prepare_pool in [0, 1] {
                let (fixture, raw_v1) = legacy_native_owner_fixture(POOL_META_V1_VERSION, false).await;
                let (legacy_meta, legacy_canonical) = match decode_pool_meta_replica(raw_v1.clone()) {
                    PoolMetaReplica::Valid {
                        meta,
                        canonical,
                        revision,
                        committed: true,
                        ..
                    } if revision.version == POOL_META_V1_VERSION => (meta, canonical),
                    _ => panic!("baseline V1 metadata must be committed and readable"),
                };
                let previous = PoolMetaCommittedCandidate {
                    canonical: legacy_canonical,
                    meta: legacy_meta.clone(),
                    revision: PoolMetaRevision::legacy(POOL_META_V1_VERSION),
                };
                let mut next = legacy_meta;
                next.version = POOL_META_GENERATION_VERSION;
                next.pools[0].last_update += Duration::seconds(1);
                let revision = PoolMetaRevision {
                    version: POOL_META_GENERATION_VERSION,
                    cluster_id: fixture.revision.cluster_id,
                    epoch: fixture.revision.epoch,
                    generation: 1,
                    transaction_id: Some(Uuid::new_v4()),
                };
                let pending = encode_pool_meta_v3_envelope(&next, revision, false, Some(&previous))
                    .expect("V3 PREPARE with V1 predecessor must encode");
                assert!(matches!(decode_pool_meta_replica(pending.clone()),
                    PoolMetaReplica::Valid { committed: false, revision: actual, .. } if actual == revision));
                let heads = replicas(&pending, OffsetDateTime::now_utc() + Duration::seconds(1)).await;
                let physical = disks(&fixture.store).await;
                for (index, disk) in physical[prepare_pool].iter().enumerate() {
                    fs::write(object_path(disk).join("xl.meta"), &heads[index])
                        .await
                        .expect("install task-owned V3 PREPARE evidence");
                }
                let reads = read_pool_meta_replicas(fixture.store.pools.clone(), true).await;
                assert!(matches!(&reads[prepare_pool].replica,
                    PoolMetaReplica::Valid { raw, committed: false, revision: actual, .. }
                        if raw == &pending && *actual == revision));
                let other_pool = 1 - prepare_pool;
                assert!(matches!(&reads[other_pool].replica,
                    PoolMetaReplica::Valid { raw, committed: true, revision: actual, .. }
                        if raw == &raw_v1 && actual.version == POOL_META_V1_VERSION));
                let before = snapshots(&fixture.store).await;
                let identities = owner_repair_identity_heads(&fixture.store).await;
                let guard = owner(&fixture.store).await;
                let mut state = fixture.store.pool_meta_save_gate.lock().await;
                let result = tokio::time::timeout(
                    std::time::Duration::from_secs(30),
                    fixture
                        .store
                        .heal_pool_meta_replica_under_fence(prepare_pool, guard, &mut state),
                )
                .await
                .expect("mixed V1/V3 PREPARE heal must terminate");
                drop(state);
                assert!(
                    result.is_err(),
                    "pool {prepare_pool}: native heal must not overwrite a V3 PREPARE with legacy data"
                );
                assert_eq!(
                    snapshots(&fixture.store).await,
                    before,
                    "pool {prepare_pool}: rejection must retain all physical head and backup evidence"
                );
                assert_eq!(
                    owner_repair_identity_heads(&fixture.store).await,
                    identities,
                    "pool {prepare_pool}: rejection must not mutate identity evidence"
                );
            }
        },
    )
    .await;
}

#[tokio::test]
#[serial_test::serial(pool_meta_version_env)]
async fn pool_meta_native_heal_rejects_v1_legacy_dont_save_true_without_writes() {
    temp_env::async_with_vars(
        [
            (rustfs_config::ENV_POOL_META_V2_WRITE, Some("false")),
            (rustfs_config::ENV_POOL_META_V2_FLEET_CONFIRMED, Some("false")),
            (rustfs_config::ENV_POOL_META_V3_WRITE, Some("false")),
            (rustfs_config::ENV_POOL_META_V3_FLEET_CONFIRMED, Some("false")),
        ],
        async {
            let (fixture, safe_v1) = legacy_native_owner_fixture(POOL_META_V1_VERSION, false).await;
            let meta = match decode_pool_meta_replica(safe_v1.clone()) {
                PoolMetaReplica::Valid {
                    meta, committed: true, ..
                } => meta,
                _ => panic!("baseline V1 metadata must decode"),
            };
            let statuses = meta
                .pools
                .iter()
                .map(|pool| {
                    assert!(pool.decommission.is_none(), "this regression fixture does not inject decommission state");
                    LegacyPoolStatus {
                        id: pool.id,
                        cmd_line: pool.cmd_line.clone(),
                        last_update: pool.last_update,
                        decommission: None,
                    }
                })
                .collect();
            let legacy = LegacyPoolMeta {
                version: POOL_META_V1_VERSION,
                pools: statuses,
                dont_save: true,
            };
            let mut dangerous_raw = Vec::new();
            dangerous_raw.extend_from_slice(&POOL_META_FORMAT.to_le_bytes());
            dangerous_raw.extend_from_slice(&POOL_META_V1_VERSION.to_le_bytes());
            dangerous_raw.extend_from_slice(&rmp_serde::to_vec(&legacy).expect("legacy three-field tuple must serialize"));
            let old_reader: LegacyPoolMeta =
                rmp_serde::from_slice(&dangerous_raw[4..]).expect("legacy reader must decode source");
            assert!(old_reader.dont_save, "the old reader would treat this source as permanently do-not-save");
            assert_eq!(rmp::decode::read_array_len(&mut &dangerous_raw[4..]).expect("tuple length"), 3);
            let source = match decode_pool_meta_replica(dangerous_raw.clone()) {
                PoolMetaReplica::Valid {
                    canonical,
                    revision,
                    committed: true,
                    ..
                } => {
                    assert_eq!(revision.version, POOL_META_V1_VERSION);
                    canonical
                }
                _ => panic!("current reader must recognize the legacy source"),
            };
            assert!(matches!(decode_pool_meta_replica(safe_v1.clone()),
                PoolMetaReplica::Valid { canonical, revision, committed: true, .. }
                    if canonical == source && revision.version == POOL_META_V1_VERSION));
            let physical = disks(&fixture.store).await;
            let source_heads = replicas(&dangerous_raw, OffsetDateTime::now_utc() + Duration::seconds(1)).await;
            for (index, disk) in physical[0].iter().enumerate() {
                fs::write(object_path(disk).join("xl.meta"), &source_heads[index])
                    .await
                    .expect("install legacy source on pool zero");
            }
            let reads = read_pool_meta_replicas(fixture.store.pools.clone(), true).await;
            assert!(matches!(&reads[0].replica, PoolMetaReplica::Valid { raw, committed: true, .. } if raw == &dangerous_raw));
            assert!(matches!(&reads[1].replica, PoolMetaReplica::Valid { raw, committed: true, .. } if raw == &safe_v1));
            let before = snapshots(&fixture.store).await;
            let identities = owner_repair_identity_heads(&fixture.store).await;
            let guard = owner(&fixture.store).await;
            let mut state = fixture.store.pool_meta_save_gate.lock().await;
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                fixture.store.heal_pool_meta_replica_under_fence(1, guard, &mut state),
            )
            .await
            .expect("unsafe legacy heal must terminate");
            drop(state);
            let error = result.expect_err("native heal must reject a source whose old-reader dont_save is true");
            assert!(
                error.to_string().contains("dont_save"),
                "rejection must identify the unsafe legacy field: {error}"
            );
            assert_eq!(
                snapshots(&fixture.store).await,
                before,
                "rejection must preserve every original head and backup"
            );
            assert_eq!(
                owner_repair_identity_heads(&fixture.store).await,
                identities,
                "rejection must not write identity shards"
            );
            let after = read_pool_meta_replicas(fixture.store.pools.clone(), true).await;
            assert!(matches!(&after[0].replica, PoolMetaReplica::Valid { raw, .. } if raw == &dangerous_raw));
            assert!(
                matches!(&after[1].replica, PoolMetaReplica::Valid { raw, .. } if raw == &safe_v1),
                "the dangerous source bytes must not be copied into the target pool"
            );
        },
    )
    .await;
}

#[tokio::test]
#[serial_test::serial(pool_meta_version_env)]
async fn pool_meta_native_heal_rejects_target_v1_dont_save_true_without_writes() {
    temp_env::async_with_vars(
        [
            (rustfs_config::ENV_POOL_META_V2_WRITE, Some("false")),
            (rustfs_config::ENV_POOL_META_V2_FLEET_CONFIRMED, Some("false")),
            (rustfs_config::ENV_POOL_META_V3_WRITE, Some("false")),
            (rustfs_config::ENV_POOL_META_V3_FLEET_CONFIRMED, Some("false")),
        ],
        async {
            let (fixture, safe_v1) = legacy_native_owner_fixture(POOL_META_V1_VERSION, false).await;
            let meta = match decode_pool_meta_replica(safe_v1.clone()) {
                PoolMetaReplica::Valid {
                    meta, committed: true, ..
                } => meta,
                _ => panic!("baseline V1 metadata must decode"),
            };
            let statuses = meta
                .pools
                .iter()
                .map(|pool| {
                    assert!(pool.decommission.is_none(), "this regression fixture does not inject decommission state");
                    LegacyPoolStatus {
                        id: pool.id,
                        cmd_line: pool.cmd_line.clone(),
                        last_update: pool.last_update,
                        decommission: None,
                    }
                })
                .collect();
            let legacy = LegacyPoolMeta {
                version: POOL_META_V1_VERSION,
                pools: statuses,
                dont_save: true,
            };
            let mut dangerous_raw = Vec::new();
            dangerous_raw.extend_from_slice(&POOL_META_FORMAT.to_le_bytes());
            dangerous_raw.extend_from_slice(&POOL_META_V1_VERSION.to_le_bytes());
            dangerous_raw.extend_from_slice(&rmp_serde::to_vec(&legacy).expect("legacy three-field tuple must serialize"));
            let old_reader: LegacyPoolMeta =
                rmp_serde::from_slice(&dangerous_raw[4..]).expect("legacy reader must decode target");
            assert!(old_reader.dont_save, "the old reader interprets the target as do-not-save");
            assert_eq!(rmp::decode::read_array_len(&mut &dangerous_raw[4..]).expect("tuple length"), 3);
            let source_canonical = match decode_pool_meta_replica(safe_v1.clone()) {
                PoolMetaReplica::Valid {
                    canonical,
                    revision,
                    committed: true,
                    ..
                } => {
                    assert_eq!(revision.version, POOL_META_V1_VERSION);
                    canonical
                }
                _ => panic!("safe pool-zero source must decode"),
            };
            assert!(
                matches!(decode_pool_meta_replica(dangerous_raw.clone()),
                PoolMetaReplica::Valid { canonical, revision, committed: true, .. }
                    if canonical == source_canonical && revision.version == POOL_META_V1_VERSION),
                "the dangerous target must remain semantically equivalent to the safe source"
            );
            let physical = disks(&fixture.store).await;
            let target_heads = replicas(&dangerous_raw, OffsetDateTime::now_utc() + Duration::seconds(1)).await;
            for (index, disk) in physical[1].iter().enumerate() {
                fs::write(object_path(disk).join("xl.meta"), &target_heads[index])
                    .await
                    .expect("install legacy dangerous target head");
            }
            let reads = read_pool_meta_replicas(fixture.store.pools.clone(), true).await;
            assert!(matches!(&reads[0].replica, PoolMetaReplica::Valid { raw, committed: true, .. } if raw == &safe_v1));
            assert!(matches!(&reads[1].replica, PoolMetaReplica::Valid { raw, committed: true, .. } if raw == &dangerous_raw));
            let before = snapshots(&fixture.store).await;
            let identities = owner_repair_identity_heads(&fixture.store).await;
            let guard = owner(&fixture.store).await;
            let mut state = fixture.store.pool_meta_save_gate.lock().await;
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                fixture.store.heal_pool_meta_replica_under_fence(1, guard, &mut state),
            )
            .await
            .expect("unsafe legacy target heal must terminate");
            drop(state);
            let error = result.expect_err("native heal must reject a target whose old-reader dont_save is true");
            assert!(
                error.to_string().contains("dont_save"),
                "rejection must identify the unsafe legacy field: {error}"
            );
            assert_eq!(
                snapshots(&fixture.store).await,
                before,
                "rejection must preserve all physical heads and backups"
            );
            assert_eq!(
                owner_repair_identity_heads(&fixture.store).await,
                identities,
                "rejection must not write identity shards"
            );
            let after = read_pool_meta_replicas(fixture.store.pools.clone(), true).await;
            assert!(matches!(&after[0].replica, PoolMetaReplica::Valid { raw, .. } if raw == &safe_v1));
            assert!(
                matches!(&after[1].replica, PoolMetaReplica::Valid { raw, .. } if raw == &dangerous_raw),
                "unsafe target evidence must not be overwritten by the safe source"
            );
        },
    )
    .await;
}
