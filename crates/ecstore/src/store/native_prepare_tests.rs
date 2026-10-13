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

#[tokio::test]
#[serial_test::serial]
async fn native_stream_prepare_large_restart() {
    let (dirs, store) = fixture().await;
    let body = (0..(5 * 1024 * 1024 + 37))
        .map(|index| u8::try_from(index % 251).expect("bounded byte"))
        .collect::<Vec<_>>();
    let expected: [u8; 32] = Sha256::digest(&body).into();
    let preparation = Uuid::new_v4();
    let prepared = store
        .prepare_native_object(BUCKET, KEY, preparation, &mut PutObjReader::from_vec(body.clone()))
        .await
        .expect("actual multi-block streaming preparation");
    assert_eq!(prepared.object_digest(), expected);
    assert_eq!(prepared.object_length(), u64::try_from(body.len()).expect("bounded content length"));
    assert!(prepared.receipt_count() >= 3);
    assert_not_published(&store).await;
    let mut records = Vec::new();
    for disk in &dirs {
        let path = disk
            .path()
            .join(".rustfs.sys/native-preparations")
            .join(preparation.to_string());
        let metadata = std::fs::read(path.join("xl.meta")).expect("exact streaming metadata");
        let record: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path.join("intent.json")).expect("durable locator"))
                .expect("strict streaming intent JSON");
        assert_eq!(record["format"], 3);
        assert_eq!(record["set_disks"].as_array().expect("persistent disk set").len(), 4);
        assert_eq!(record["data_dir"], preparation.to_string());
        assert!(
            std::fs::metadata(path.join(preparation.to_string()).join("part.1"))
                .expect("real non-inline shard")
                .len()
                > 0
        );
        records.push((path, metadata));
    }
    drop(prepared);
    drop(store);
    let restarted = reopen(&dirs).await;
    let recovered = restarted
        .recover_native_object(BUCKET, KEY, preparation)
        .await
        .expect("actual streaming reopen recovery");
    assert_eq!(recovered.object_digest(), expected);
    assert_eq!(recovered.object_length(), u64::try_from(body.len()).expect("length"));
    assert_eq!(recovered.version_id(), preparation);
    for (path, bytes) in records {
        assert_eq!(std::fs::read(path.join("xl.meta")).expect("preserved metadata"), bytes);
    }
    assert_not_published(&restarted).await;
}

#[tokio::test]
#[serial_test::serial]
async fn native_stream_prepare_locator_conflict() {
    let (dirs, store) = fixture().await;
    let preparation = Uuid::new_v4();
    let body = vec![17; 2 * 1024 * 1024 + 1];
    drop(
        store
            .prepare_native_object(BUCKET, KEY, preparation, &mut PutObjReader::from_vec(body))
            .await
            .expect("real durable streaming preparation"),
    );
    for disk in &dirs {
        let path = disk
            .path()
            .join(".rustfs.sys/native-preparations")
            .join(preparation.to_string());
        let mut record: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path.join("intent.json")).expect("record")).expect("JSON");
        record["set_disks"]
            .as_array_mut()
            .expect("saved physical disk order")
            .swap(0, 1);
        // Preserve the actual record serialization order; receipt marker signs exact canonical record bytes.
        let bytes = crate::disk::local::native_prepare::canonical_test_record(&record);
        std::fs::write(path.join("intent.json"), &bytes).expect("install conflicting durable locator");
        std::fs::write(path.join("PREPARED"), Sha256::digest(&bytes)).expect("matching record marker");
    }
    assert!(
        store.recover_native_object(BUCKET, KEY, preparation).await.is_err(),
        "NATIVE_STREAM_REQUIRES_SAVED_PHYSICAL_SET"
    );
    assert_not_published(&store).await;
}

#[tokio::test]
#[serial_test::serial]
async fn native_stream_prepare_crash_barriers() {
    use crate::disk::local::native_prepare::{clear_native_preparation_probe, fail_native_preparation};
    for (stage, after, recoverable) in [
        ("part.1:file", false, false),
        ("intent.json:file", true, false),
        ("PREPARED:directory", true, true),
    ] {
        let (dirs, store) = fixture().await;
        let preparation = Uuid::new_v4();
        fail_native_preparation(preparation, stage, after);
        let result = store
            .prepare_native_object(BUCKET, KEY, preparation, &mut PutObjReader::from_vec(vec![23; 2 * 1024 * 1024 + 3]))
            .await;
        assert!(result.is_err(), "injected disk barrier must not return a receipt: {stage}");
        clear_native_preparation_probe(preparation);
        assert_not_published(&store).await;
        drop(store);
        let restarted = reopen(&dirs).await;
        let recovered = restarted.recover_native_object(BUCKET, KEY, preparation).await;
        assert_eq!(recovered.is_ok(), recoverable, "durable boundary: {stage}");
        assert_not_published(&restarted).await;
    }
}

#[tokio::test]
#[serial_test::serial]
async fn native_stream_prepare_multi_part_restart() {
    use crate::disk::Disk;
    use rustfs_filemeta::FileInfoOpts;
    let (dirs, store) = fixture().await;
    let body = vec![29; 3 * 1024 * 1024 + 13];
    let encoded_preparation = Uuid::new_v4();
    drop(
        store
            .prepare_native_object(BUCKET, KEY, encoded_preparation, &mut PutObjReader::from_vec(body.clone()))
            .await
            .expect("existing ordinary streaming encoder fixture"),
    );
    let preparation = Uuid::new_v4();
    let incarnation = store.bucket_incarnation_id(BUCKET).await.expect("actual incarnation");
    let fence = store.object_lock_config_snapshot(BUCKET).await.expect("actual bucket fence");
    let set = store.pools[0].get_disks_by_key(KEY);
    let disks = set.disks.read().await.clone();
    let guard = store
        .handle_new_ns_lock(BUCKET, KEY)
        .await
        .expect("actual commit namespace")
        .get_write_lock(crate::set_disk::get_lock_acquire_timeout())
        .await
        .expect("owned commit lock");
    let mut candidates = Vec::new();
    for (dir, disk) in dirs.iter().zip(disks) {
        let original = dir
            .path()
            .join(".rustfs.sys/native-preparations")
            .join(encoded_preparation.to_string());
        let metadata = std::fs::read(original.join("xl.meta")).expect("encoder metadata");
        let mut info = rustfs_filemeta::get_file_info(
            &metadata,
            BUCKET,
            KEY,
            &encoded_preparation.to_string(),
            FileInfoOpts {
                data: false,
                include_free_versions: false,
                include_part_checksums: true,
            },
        )
        .expect("exact FileInfo");
        let first = info.erasure.block_size;
        let codec = crate::erasure::coding::Erasure::try_new_with_options(
            info.erasure.data_blocks,
            info.erasure.parity_blocks,
            first,
            info.uses_legacy_checksum,
        )
        .expect("existing erasure geometry");
        let split = crate::erasure::coding::bitrot_shard_file_size(
            usize::try_from(codec.shard_file_size(i64::try_from(first).expect("part size"))).expect("shard size"),
            codec.shard_size(),
            info.erasure.get_checksum_info(1).algorithm,
        );
        let shard = std::fs::read(original.join(encoded_preparation.to_string()).join("part.1")).expect("actual encoded shard");
        let disk = disk.expect("actual disk");
        let Disk::Local(local) = disk.as_ref() else {
            panic!("local fixture");
        };
        local
            .get_disk()
            .begin_native_stream(preparation, store.id, BUCKET, incarnation, fence.clone())
            .await
            .expect("actual private owner");
        let destination = dir
            .path()
            .join(".rustfs.sys/native-preparations")
            .join(preparation.to_string())
            .join(preparation.to_string());
        std::fs::create_dir(&destination).expect("private part fixture directory");
        std::fs::write(destination.join("part.1"), &shard[..split]).expect("first actual encoded blocks");
        std::fs::write(destination.join("part.2"), &shard[split..]).expect("remaining actual encoded blocks");
        info.version_id = Some(preparation);
        info.data_dir = Some(preparation);
        info.parts.clear();
        info.add_object_part(1, String::new(), first, None, i64::try_from(first).expect("size"), None, None);
        info.add_object_part(
            2,
            String::new(),
            body.len() - first,
            None,
            i64::try_from(body.len() - first).expect("size"),
            None,
            None,
        );
        candidates.push((disk.clone(), info));
    }
    let owner = set
        .attest_native_stream(
            store.id,
            BUCKET,
            KEY,
            preparation,
            incarnation,
            candidates,
            Sha256::digest(&body).into(),
            u64::try_from(body.len()).expect("length"),
            fence,
        )
        .await
        .expect("actual adoption verifies/fsyncs both parts without rereading upload or taking namespace");
    assert!(owner.receipt_count() >= 3);
    drop(owner);
    drop(guard);
    drop(set);
    drop(store);
    let restarted = reopen(&dirs).await;
    let recovered = restarted
        .recover_native_object(BUCKET, KEY, preparation)
        .await
        .expect("actual multi-part recovery decode");
    assert_eq!(recovered.object_digest(), <[u8; 32]>::from(Sha256::digest(&body)));
    assert_eq!(recovered.object_length(), u64::try_from(body.len()).expect("length"));
    assert_not_published(&restarted).await;
}

#[tokio::test]
#[serial_test::serial]
async fn native_stream_prepare_rejects_overwrite() {
    use crate::disk::{DiskAPI, RUSTFS_META_BUCKET};
    let (dirs, store) = fixture().await;
    let preparation = Uuid::new_v4();
    drop(
        store
            .prepare_native_object(BUCKET, KEY, preparation, &mut PutObjReader::from_vec(vec![31; 2 * 1024 * 1024 + 11]))
            .await
            .expect("attested private stream"),
    );
    let disk = store.pools[0].get_disks_by_key(KEY).disks.read().await[0]
        .clone()
        .expect("actual disk");
    let path = format!("native-preparations/{preparation}/{preparation}/part.1");
    let full = dirs[0].path().join(RUSTFS_META_BUCKET).join(&path);
    let before = std::fs::read(&full).expect("attested shard bytes");
    assert!(
        disk.create_file("", RUSTFS_META_BUCKET, &path, 1).await.is_err(),
        "NATIVE_STREAM_ATTESTED_PART_MUST_NOT_BE_TRUNCATED"
    );
    assert_eq!(std::fs::read(&full).expect("retained shard"), before);
    assert!(store.recover_native_object(BUCKET, KEY, preparation).await.is_ok());
}

#[tokio::test]
#[serial_test::serial]
async fn native_stream_prepare_record_locator() {
    use crate::disk::Disk;
    let (dirs, store) = fixture().await;
    let preparation = Uuid::new_v4();
    drop(
        store
            .prepare_native_object(BUCKET, KEY, preparation, &mut PutObjReader::from_vec(vec![37; 2 * 1024 * 1024 + 9]))
            .await
            .expect("actual prepared locator"),
    );
    let disk = store.pools[0].get_disks_by_key(KEY).disks.read().await[0]
        .clone()
        .expect("actual disk");
    let Disk::Local(local) = disk.as_ref() else {
        panic!("local fixture");
    };
    let listed = local
        .get_disk()
        .list_native_preparations(store.id, None, 1)
        .await
        .expect("descriptor-relative list");
    assert_eq!(listed.records.len(), 1);
    assert!(listed.unfinished.is_empty());
    assert_eq!(listed.next, None);
    let record = &listed.records[0];
    assert_eq!(record.preparation, preparation);
    assert_eq!(record.bucket, BUCKET);
    assert_eq!(record.key, KEY);
    assert_eq!(record.version, preparation);
    assert!(record.prepared);
    let path = dirs[0]
        .path()
        .join(".rustfs.sys/native-preparations")
        .join(preparation.to_string());
    assert_eq!(
        record.metadata.as_ref(),
        std::fs::read(path.join("xl.meta")).expect("retained exact metadata")
    );
    let direct = local
        .get_disk()
        .locate_native_preparation(store.id, preparation)
        .await
        .expect("direct locator")
        .expect("present record");
    assert_eq!(direct.metadata, record.metadata);
    assert!(
        local
            .get_disk()
            .locate_native_preparation(store.id, Uuid::new_v4())
            .await
            .expect("healthy absence")
            .is_none()
    );
    assert!(local.get_disk().list_native_preparations(store.id, None, 65).await.is_err());
    // Sender-owned namespace guard must not be acquired again by the recovery receiver.
    let guard = store
        .handle_new_ns_lock(BUCKET, KEY)
        .await
        .expect("namespace")
        .get_write_lock(crate::set_disk::get_lock_acquire_timeout())
        .await
        .expect("actual sender lock");
    let (owner, metadata) = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        store.recover_native_object_unlocked(BUCKET, KEY, preparation),
    )
    .await
    .expect("no recursive namespace lock")
    .expect("actual disk recovery under sender lock");
    assert!(metadata.iter().any(|(id, bytes)| {
        owner
            .receipts()
            .iter()
            .any(|receipt| receipt.disk_id() == *id && receipt.metadata_digest() == &<[u8; 32]>::from(Sha256::digest(bytes)))
    }));
    drop(guard);
    // Moving a published part must not erase its durable location facts; locate grants no readiness.
    std::fs::rename(
        path.join(preparation.to_string()).join("part.1"),
        dirs[0].path().join("published-part-test"),
    )
    .expect("publication progress fixture");
    let located = local
        .get_disk()
        .locate_native_preparation(store.id, preparation)
        .await
        .expect("retained locator after part move")
        .expect("retained record");
    assert_eq!(located.metadata, record.metadata);
    assert!(
        store.recover_native_object(BUCKET, KEY, preparation).await.is_err(),
        "prepared marker is not publication authority"
    );
}
