use super::ECStore;
use crate::{
    bucket::metadata_sys::{init_bucket_metadata_sys, test_support::isolated_store_over_temp_disks},
    disk::endpoint::Endpoint,
    layout::endpoints::{EndpointServerPools, Endpoints, PoolEndpoints},
    object_api::{ObjectOptions, PutObjReader},
    runtime::instance::InstanceContext,
    storage_api_contracts::{
        bucket::{BucketOperations as _, MakeBucketOptions},
        object::ObjectOperations as _,
    },
    store::init_local_disks_with_instance_ctx,
};
use sha2::{Digest, Sha256};
use std::{collections::HashSet, sync::Arc};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const BUCKET: &str = "native-quorum-preparation";
const KEY: &str = "nested/object.bin";
const CONTENT: &[u8] = b"native preparation content\0with an immutable identity";

async fn fixture() -> (Vec<TempDir>, Arc<ECStore>) {
    let (dirs, store) = isolated_store_over_temp_disks().await;
    init_bucket_metadata_sys(store.clone(), Vec::new()).await;
    store
        .make_bucket(BUCKET, &MakeBucketOptions::default())
        .await
        .expect("create actual four-disk bucket");
    (dirs, store)
}

async fn assert_not_published(store: &ECStore) {
    let result = store.get_object_info(BUCKET, KEY, &ObjectOptions::default()).await;
    assert!(
        matches!(result, Err(ref error) if crate::error::is_err_object_not_found(error)),
        "NP02_PREPARATION_NOT_AUTHORITY: native preparation must remain invisible to ordinary object HEAD"
    );
}

fn fail_preparation_disk(dir: &TempDir) {
    // Keep the physical disk and its format present; only native preparation I/O fails.
    std::fs::write(dir.path().join(".rustfs.sys/native-preparations"), b"not a directory")
        .expect("install real per-disk namespace obstruction");
}

async fn reopen(dirs: &[TempDir]) -> Arc<ECStore> {
    let endpoints = dirs
        .iter()
        .enumerate()
        .map(|(index, dir)| {
            let mut endpoint = Endpoint::try_from(dir.path().to_str().expect("disk path")).expect("existing local endpoint");
            endpoint.set_pool_index(0);
            endpoint.set_set_index(index / 4);
            endpoint.set_disk_index(index % 4);
            endpoint
        })
        .collect::<Vec<_>>();
    let pools = EndpointServerPools(vec![PoolEndpoints {
        legacy: false,
        set_count: dirs.len() / 4,
        drives_per_set: 4,
        endpoints: Endpoints::from(endpoints),
        cmd_line: "native-quorum-reopen".into(),
        platform: "test".into(),
    }]);
    let context = Arc::new(InstanceContext::new());
    init_local_disks_with_instance_ctx(&context, pools.clone())
        .await
        .expect("reopen existing disk formats");
    let store =
        ECStore::new_with_instance_ctx("127.0.0.1:0".parse().expect("local address"), pools, CancellationToken::new(), context)
            .await
            .expect("restart store over the same physical disks");
    init_bucket_metadata_sys(store.clone(), Vec::new()).await;
    store
}

#[tokio::test]
#[serial_test::serial]
async fn native_object_prepare_one_disk_failure_retains_write_quorum() {
    let (dirs, store) = fixture().await;
    fail_preparation_disk(&dirs[0]);
    let preparation = Uuid::new_v4();
    let owner = store
        .prepare_native_object(BUCKET, KEY, preparation, &mut PutObjReader::from_vec(CONTENT.to_vec()))
        .await;
    assert!(
        owner.is_ok(),
        "NP02_WDATA_BOUND: N4/W3 must tolerate one actual disk I/O failure: {:?}",
        owner.as_ref().err()
    );
    let owner = owner.expect("checked preparation result");
    assert_eq!(owner.preparation_id(), preparation, "NP02_WDATA_BOUND");
    assert_eq!(
        owner.object_digest(),
        <[u8; 32]>::from(Sha256::digest(CONTENT)),
        "NP02_OPERATION_IDENTITY_BOUND: digest computed from actual reader bytes"
    );
    assert_eq!(owner.receipt_count(), 3, "NP02_WDATA_BOUND: three real durable receipts");
    assert_eq!(
        owner.disk_ids().collect::<HashSet<_>>().len(),
        3,
        "NP02_WDATA_BOUND: distinct physical disks"
    );
    assert_eq!(owner.object_length(), u64::try_from(CONTENT.len()).expect("small content"));
    assert!(!owner.version_id().is_nil(), "NP02_WDATA_BOUND: real immutable version");
    drop(owner);
    assert_not_published(&store).await;
}

#[tokio::test]
#[serial_test::serial]
async fn native_object_prepare_below_write_quorum_has_no_authority() {
    let (dirs, store) = fixture().await;
    fail_preparation_disk(&dirs[0]);
    fail_preparation_disk(&dirs[1]);
    let preparation = Uuid::new_v4();
    let prepared = store
        .prepare_native_object(BUCKET, KEY, preparation, &mut PutObjReader::from_vec(CONTENT.to_vec()))
        .await;
    assert!(
        prepared.is_err(),
        "NP02_PREPARATION_NOT_AUTHORITY: W-1 must not construct preparation authority"
    );
    drop(prepared);
    let recovered = store.recover_native_object(BUCKET, KEY, preparation).await;
    assert!(
        recovered.is_err(),
        "NP02_PREPARATION_NOT_AUTHORITY: partial durable facts are not quorum authority"
    );
    drop(recovered);
    assert_not_published(&store).await;
    // Recovery after a new store context must apply the same threshold to those facts.
    drop(store);
    let restarted = reopen(&dirs).await;
    assert!(
        restarted.recover_native_object(BUCKET, KEY, preparation).await.is_err(),
        "NP02_PREPARATION_NOT_AUTHORITY: restart cannot promote W-1 facts"
    );
}

#[tokio::test]
#[serial_test::serial]
async fn native_object_prepare_restart_preserves_operation_and_content_identity() {
    let (dirs, store) = fixture().await;
    let preparation = Uuid::new_v4();
    let owner = store
        .prepare_native_object(BUCKET, KEY, preparation, &mut PutObjReader::from_vec(CONTENT.to_vec()))
        .await;
    assert!(
        owner.is_ok(),
        "NP02_OPERATION_IDENTITY_BOUND: prepare actual native object: {:?}",
        owner.as_ref().err()
    );
    let owner = owner.expect("checked preparation result");
    let expected = (owner.preparation_id(), owner.version_id(), owner.object_digest(), owner.object_length());
    assert!(owner.receipt_count() >= 3, "NP02_WDATA_BOUND: durable write quorum");
    assert_eq!(
        owner.disk_ids().collect::<HashSet<_>>().len(),
        owner.receipt_count(),
        "NP02_WDATA_BOUND: independent disk facts"
    );
    drop(owner);
    drop(store);
    let restarted = reopen(&dirs).await;
    let recovered = restarted.recover_native_object(BUCKET, KEY, preparation).await;
    assert!(
        recovered.is_ok(),
        "NP02_OPERATION_IDENTITY_BOUND: recover real disk records in new store context"
    );
    let recovered = recovered.expect("checked recovery result");
    assert_eq!(
        (
            recovered.preparation_id(),
            recovered.version_id(),
            recovered.object_digest(),
            recovered.object_length()
        ),
        expected,
        "NP02_OPERATION_IDENTITY_BOUND: recovery retains exact operation, version and content"
    );
    assert!(recovered.receipt_count() >= 3, "NP02_WDATA_BOUND: recovered write quorum");
    assert_eq!(
        recovered.disk_ids().collect::<HashSet<_>>().len(),
        recovered.receipt_count(),
        "NP02_WDATA_BOUND"
    );
    drop(recovered);
    let replay = restarted
        .prepare_native_object(BUCKET, KEY, preparation, &mut PutObjReader::from_vec(CONTENT.to_vec()))
        .await;
    assert!(replay.is_ok(), "NP02_OPERATION_IDENTITY_BOUND: retry original preparation and content");
    let replay = replay.expect("checked replay result");
    assert_eq!(
        (
            replay.preparation_id(),
            replay.version_id(),
            replay.object_digest(),
            replay.object_length()
        ),
        expected,
        "NP02_OPERATION_IDENTITY_BOUND: retry of the local preparation must not invent a new version"
    );
    drop(replay);
    let mut changed_content = CONTENT.to_vec();
    changed_content[0] ^= 1;
    assert!(
        restarted
            .prepare_native_object(BUCKET, KEY, preparation, &mut PutObjReader::from_vec(changed_content))
            .await
            .is_err(),
        "NP02_OPERATION_IDENTITY_BOUND: a local preparation cannot be rebound to different content"
    );
    assert!(
        restarted
            .recover_native_object(BUCKET, "another-key", preparation)
            .await
            .is_err(),
        "NP02_OPERATION_IDENTITY_BOUND: recovery is bound to the original key"
    );
}

#[tokio::test]
#[serial_test::serial]
async fn native_object_prepare_cancelled_writer_retains_pending_disk_owner() {
    let (dirs, store) = fixture().await;
    fail_preparation_disk(&dirs[0]);
    let preparation = Uuid::new_v4();
    let (entered, release) = crate::disk::local::native_prepare::pause_native_preparation(preparation, "PREPARED:file");
    // LocalDisk canonicalizes its root before constructing metadata lock keys.
    let owner_paths = dirs[1..]
        .iter()
        .map(|dir| {
            std::fs::canonicalize(dir.path())
                .expect("actual canonical disk root")
                .join(BUCKET)
                .join(KEY)
        })
        .collect::<Vec<_>>();
    assert_eq!(owner_paths.len(), 3, "three healthy actual local disk owners");
    let writing_store = store.clone();
    let writer = tokio::spawn(async move {
        writing_store
            .prepare_native_object(BUCKET, KEY, preparation, &mut PutObjReader::from_vec(CONTENT.to_vec()))
            .await
    });
    let reached_io = tokio::task::spawn_blocking(move || entered.recv_timeout(std::time::Duration::from_secs(30)))
        .await
        .expect("observe actual disk barrier");
    // Release before asserting so a failed test never strands the real blocking I/O.
    if reached_io.is_err() {
        let _ = release.send(());
        writer.abort();
        let _ = writer.await;
        crate::disk::local::native_prepare::clear_native_preparation_probe(preparation);
        panic!("NP02_PENDING_OWNER_BOUND: preparation must reach actual disk I/O");
    }
    writer.abort();
    let cancelled = matches!(writer.await, Err(error) if error.is_cancelled());
    let mut held_owners = 0;
    for path in &owner_paths {
        if tokio::time::timeout(
            std::time::Duration::from_millis(50),
            crate::disk::os::acquire_metadata_mutation_lease(path, None),
        )
        .await
        .is_err()
        {
            held_owners += 1;
        }
    }
    release.send(()).expect("release actual disk I/O after cancelled waiter");
    for path in &owner_paths {
        let owner = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            crate::disk::os::acquire_metadata_mutation_lease(path, None),
        )
        .await
        .expect("completed actual I/O releases metadata owner");
        drop(owner);
    }
    crate::disk::local::native_prepare::clear_native_preparation_probe(preparation);
    assert!(
        cancelled,
        "NP02_PENDING_OWNER_BOUND: writer remains pending below W3 until the actual disk barrier"
    );
    assert!(
        held_owners >= 1,
        "NP02_PENDING_OWNER_BOUND: cancellation cannot release an in-flight real disk owner"
    );
    assert_not_published(&store).await;
    drop(store);
    let restarted = reopen(&dirs).await;
    let recovered = restarted.recover_native_object(BUCKET, KEY, preparation).await;
    assert!(
        recovered.is_ok(),
        "NP02_PENDING_OWNER_BOUND: retain actual durable facts after cancelled waiter"
    );
    let recovered = recovered.expect("checked recovery result");
    let version = recovered.version_id();
    assert_eq!(recovered.preparation_id(), preparation, "NP02_OPERATION_IDENTITY_BOUND");
    assert_eq!(
        recovered.object_digest(),
        <[u8; 32]>::from(Sha256::digest(CONTENT)),
        "NP02_OPERATION_IDENTITY_BOUND"
    );
    assert_eq!(recovered.object_length(), u64::try_from(CONTENT.len()).expect("small content"));
    assert_eq!(recovered.receipt_count(), 3, "NP02_WDATA_BOUND");
    drop(recovered);
    let replay = restarted
        .prepare_native_object(BUCKET, KEY, preparation, &mut PutObjReader::from_vec(CONTENT.to_vec()))
        .await;
    assert!(
        replay.is_ok(),
        "NP02_OPERATION_IDENTITY_BOUND: retry cancelled operation from persisted facts"
    );
    assert_eq!(replay.expect("checked replay").version_id(), version, "NP02_OPERATION_IDENTITY_BOUND");
    assert_not_published(&restarted).await;
}

#[tokio::test]
#[serial_test::serial]
async fn native_object_prepare_nonzero_set_reopens_the_routed_identity() {
    let dirs = (0..8)
        .map(|_| tempfile::tempdir().expect("real local disk directory"))
        .collect::<Vec<_>>();
    let store = reopen(&dirs).await;
    store
        .make_bucket(BUCKET, &MakeBucketOptions::default())
        .await
        .expect("real two-set bucket");
    let selected_key = (0..1024)
        .map(|index| format!("routed/{index}.bin"))
        .find(|key| store.pools[0].get_disks_by_key(key).set_index == 1)
        .expect("find a key routed by the actual algorithm to nonzero set");
    let other_key = (0..1024)
        .map(|index| format!("separate/{index}.bin"))
        .find(|key| store.pools[0].get_disks_by_key(key).set_index == 0)
        .expect("find another key routed to the other real set");
    let target_disks = store.pools[0].format.erasure.sets[1].iter().copied().collect::<HashSet<_>>();
    let preparation = Uuid::new_v4();
    let prepared = store
        .prepare_native_object(BUCKET, &selected_key, preparation, &mut PutObjReader::from_vec(CONTENT.to_vec()))
        .await;
    assert!(
        prepared.is_ok(),
        "NP02_OPERATION_IDENTITY_BOUND: nonzero routed set prepares real data: {:?}",
        prepared.as_ref().err()
    );
    let prepared = prepared.expect("checked nonzero set preparation");
    let expected = (
        prepared.preparation_id(),
        prepared.version_id(),
        prepared.object_digest(),
        prepared.object_length(),
    );
    assert!(
        prepared.receipt_count() >= 3,
        "NP02_WDATA_BOUND: quorum is scoped to the selected physical set"
    );
    let actual_disks = prepared.disk_ids().collect::<HashSet<_>>();
    assert_eq!(actual_disks.len(), prepared.receipt_count(), "NP02_WDATA_BOUND");
    assert!(
        actual_disks.is_subset(&target_disks),
        "NP02_OPERATION_IDENTITY_BOUND: receipts must come from routed set one"
    );
    drop(prepared);
    assert!(
        dirs[..4].iter().all(|dir| !dir
            .path()
            .join(".rustfs.sys/native-preparations")
            .join(preparation.to_string())
            .exists()),
        "NP02_OPERATION_IDENTITY_BOUND: preparation must not write unrelated set zero"
    );
    let mut other_content = CONTENT.to_vec();
    other_content[0] ^= 1;
    let other = store
        .prepare_native_object(BUCKET, &other_key, preparation, &mut PutObjReader::from_vec(other_content.clone()))
        .await;
    assert!(
        other.is_ok(),
        "NP02_OPERATION_IDENTITY_BOUND: preparation UUID is local set/key identity, not global operation authority"
    );
    drop(other);
    for key in [&selected_key, &other_key] {
        let head = store.get_object_info(BUCKET, key, &ObjectOptions::default()).await;
        assert!(
            matches!(head, Err(ref error) if crate::error::is_err_object_not_found(error)),
            "NP02_PREPARATION_NOT_AUTHORITY"
        );
    }
    drop(store);
    let restarted = reopen(&dirs).await;
    assert_eq!(
        restarted.pools[0].get_disks_by_key(&selected_key).set_index,
        1,
        "NP02_OPERATION_IDENTITY_BOUND: durable routing after reopen"
    );
    let recovered = restarted.recover_native_object(BUCKET, &selected_key, preparation).await;
    assert!(recovered.is_ok(), "NP02_OPERATION_IDENTITY_BOUND: reopen routed set preparation");
    let recovered = recovered.expect("checked routed recovery");
    assert_eq!(
        (
            recovered.preparation_id(),
            recovered.version_id(),
            recovered.object_digest(),
            recovered.object_length()
        ),
        expected,
        "NP02_OPERATION_IDENTITY_BOUND: other set facts cannot replace selected key content"
    );
    assert!(
        recovered.disk_ids().collect::<HashSet<_>>().is_subset(&target_disks),
        "NP02_OPERATION_IDENTITY_BOUND"
    );
    drop(recovered);
    let other = restarted.recover_native_object(BUCKET, &other_key, preparation).await;
    assert!(
        other.is_ok(),
        "NP02_OPERATION_IDENTITY_BOUND: recover independent preparation in other set"
    );
    assert_eq!(
        other.expect("checked other set recovery").object_digest(),
        <[u8; 32]>::from(Sha256::digest(&other_content)),
        "NP02_OPERATION_IDENTITY_BOUND: select actual key/set facts"
    );
}
