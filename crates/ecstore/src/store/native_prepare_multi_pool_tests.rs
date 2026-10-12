//! Real private preparation placement across two physical local pools.
use super::ECStore;
use crate::{
    bucket::metadata_sys::init_bucket_metadata_sys,
    disk::endpoint::Endpoint,
    layout::endpoints::{EndpointServerPools, Endpoints, PoolEndpoints},
    object_api::PutObjReader,
    runtime::instance::InstanceContext,
    storage_api_contracts::bucket::{BucketOperations as _, MakeBucketOptions},
    store::init_local_disks_with_instance_ctx,
};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const BUCKET: &str = "native-multi-pool-preparation";
const KEY: &str = "private/placement.bin";
const CONTENT: &[u8] = b"actual two-pool reader bytes\0remain bound to durable placement";
const DRIVES: usize = 4;

async fn reopen(dirs: &[TempDir]) -> Arc<ECStore> {
    assert_eq!(dirs.len(), 2 * DRIVES);
    let pools = (0..2)
        .map(|pool| {
            let endpoints = (0..DRIVES)
                .map(|slot| {
                    let mut endpoint = Endpoint::try_from(dirs[pool * DRIVES + slot].path().to_str().expect("local path"))
                        .expect("actual local endpoint");
                    endpoint.set_pool_index(pool);
                    endpoint.set_set_index(0);
                    endpoint.set_disk_index(slot);
                    endpoint
                })
                .collect::<Vec<_>>();
            PoolEndpoints {
                legacy: false,
                set_count: 1,
                drives_per_set: DRIVES,
                endpoints: Endpoints::from(endpoints),
                cmd_line: format!("native-multi-pool-{pool}"),
                platform: "test".into(),
            }
        })
        .collect();
    let pools = EndpointServerPools(pools);
    let context = Arc::new(InstanceContext::new());
    init_local_disks_with_instance_ctx(&context, pools.clone())
        .await
        .expect("initialize real formats for both pools");
    let store =
        ECStore::new_with_instance_ctx("127.0.0.1:0".parse().expect("local address"), pools, CancellationToken::new(), context)
            .await
            .expect("open actual two-pool store");
    init_bucket_metadata_sys(store.clone(), Vec::new()).await;
    store
}

fn private_path(dir: &TempDir, preparation: Uuid) -> std::path::PathBuf {
    dir.path()
        .join(".rustfs.sys/native-preparations")
        .join(preparation.to_string())
}

async fn create_bucket(store: &Arc<ECStore>) {
    let store = store.clone();
    tokio::spawn(async move { store.make_bucket(BUCKET, &MakeBucketOptions::default()).await })
        .await
        .expect("join actual bucket creation")
        .expect("create actual bucket");
}

fn assert_only_pool(dirs: &[TempDir], preparation: Uuid, pool: usize) {
    let other = 1 - pool;
    for dir in &dirs[other * DRIVES..(other + 1) * DRIVES] {
        assert!(
            !private_path(dir, preparation).exists(),
            "NP02_POOL_PLACEMENT_BOUND: retry must not create private facts in another pool"
        );
    }
}

async fn await_pending_owners(store: &ECStore) {
    // The preparation's parent owner outlives its early Wdata return. Acquire the
    // same real object lock before changing files in this test's fixture.
    let lock = store.handle_new_ns_lock(BUCKET, KEY).await.expect("actual object lock");
    let owner = lock
        .get_write_lock(std::time::Duration::from_secs(30))
        .await
        .expect("pending disk owners finish before fixture mutation");
    drop(owner);
}

#[tokio::test]
#[serial_test::serial]
async fn native_object_prepare_multi_pool_restart_pending_and_unknown_preserve_placement() {
    let dirs = (0..2 * DRIVES)
        .map(|_| tempfile::tempdir().expect("physical local disk"))
        .collect::<Vec<_>>();
    let store = reopen(&dirs).await;
    create_bucket(&store).await;
    let preparation = Uuid::new_v4();
    let prepared = store
        .prepare_native_object(BUCKET, KEY, preparation, &mut PutObjReader::from_vec(CONTENT.to_vec()))
        .await
        .expect("NP02_POOL_PLACEMENT_BOUND: two-pool native preparation is supported");
    let version = prepared.version_id();
    assert_eq!(prepared.object_digest(), <[u8; 32]>::from(Sha256::digest(CONTENT)));
    let present = (0..2)
        .filter(|pool| {
            dirs[pool * DRIVES..(pool + 1) * DRIVES]
                .iter()
                .any(|dir| private_path(dir, preparation).exists())
        })
        .collect::<Vec<_>>();
    assert_eq!(present.len(), 1, "NP02_POOL_PLACEMENT_BOUND: one actual durable pool");
    let pool = present[0];
    assert_only_pool(&dirs, preparation, pool);
    drop(prepared);
    await_pending_owners(&store).await;
    // Recovery has to rediscover private placement from physical files in a fresh context.
    let reopened = reopen(&dirs).await;
    let recovered = reopened
        .recover_native_object(BUCKET, KEY, preparation)
        .await
        .expect("NP02_POOL_PLACEMENT_BOUND: restart recovers original private pool");
    assert_eq!(recovered.version_id(), version, "NP02_POOL_PLACEMENT_BOUND");
    assert_eq!(recovered.object_digest(), <[u8; 32]>::from(Sha256::digest(CONTENT)));
    drop(recovered);
    await_pending_owners(&reopened).await;
    let marker = dirs[pool * DRIVES..(pool + 1) * DRIVES]
        .iter()
        .map(|dir| private_path(dir, preparation).join("PREPARED"))
        .find(|path| path.exists())
        .expect("actual persisted acknowledgment");
    let original_marker = std::fs::read(&marker).expect("retain original marker bytes");
    std::fs::write(&marker, b"corrupt existing acknowledgment").expect("corrupt own actual marker");
    let corrupt = reopened
        .prepare_native_object(BUCKET, KEY, preparation, &mut PutObjReader::from_vec(CONTENT.to_vec()))
        .await;
    assert!(
        corrupt.is_err(),
        "NP02_POOL_PLACEMENT_BOUND: a corrupted existing marker is not absent placement"
    );
    assert_only_pool(&dirs, preparation, pool);
    await_pending_owners(&reopened).await;
    std::fs::write(&marker, original_marker).expect("restore original actual marker");
    // Keep durable intents while removing acknowledgments and xl.meta.
    // Retry must restore each writer's original raw bytes in the same pool.
    let mut original_metadata = Vec::new();
    for dir in &dirs[pool * DRIVES..(pool + 1) * DRIVES] {
        let marker = private_path(dir, preparation).join("PREPARED");
        if marker.exists() {
            std::fs::remove_file(marker).expect("remove own fixture acknowledgment");
        }
        let metadata = private_path(dir, preparation).join("xl.meta");
        if metadata.exists() {
            let bytes = std::fs::read(&metadata).expect("retain original raw metadata");
            std::fs::remove_file(&metadata).expect("leave only original durable intent");
            original_metadata.push((metadata, bytes));
        }
    }
    let repaired = reopened
        .prepare_native_object(BUCKET, KEY, preparation, &mut PutObjReader::from_vec(CONTENT.to_vec()))
        .await
        .expect("NP02_POOL_PLACEMENT_BOUND: pending intents repair in their existing pool");
    assert_eq!(repaired.version_id(), version, "NP02_POOL_PLACEMENT_BOUND");
    drop(repaired);
    await_pending_owners(&reopened).await;
    assert!(
        original_metadata.len() >= 3,
        "NP02_POOL_PLACEMENT_BOUND: retain actual write quorum metadata"
    );
    for (path, bytes) in original_metadata {
        assert_eq!(
            std::fs::read(path).expect("repaired actual metadata file"),
            bytes,
            "NP02_POOL_PLACEMENT_BOUND: top-level retry restores original raw bytes from intent"
        );
    }
    assert_only_pool(&dirs, preparation, pool);
    // Preserve each original directory while making actual filesystem inspection fail.
    // Public object absence must not turn this unknown private outcome into a new placement.
    for dir in &dirs[pool * DRIVES..(pool + 1) * DRIVES] {
        let root = dir.path().join(".rustfs.sys/native-preparations");
        std::fs::rename(&root, dir.path().join(".rustfs.sys/native-preparations-preserved"))
            .expect("retain original private facts");
        std::fs::write(root, b"actual namespace obstruction").expect("install real I/O failure");
    }
    let retry = reopened
        .prepare_native_object(BUCKET, KEY, preparation, &mut PutObjReader::from_vec(CONTENT.to_vec()))
        .await;
    assert!(
        retry.is_err(),
        "NP02_POOL_PLACEMENT_BOUND: unknown original pool cannot migrate to healthy pool"
    );
    assert_only_pool(&dirs, preparation, pool);
}
#[tokio::test]
#[serial_test::serial]
async fn native_object_prepare_coherent_replay_rejects_changed_block_size() {
    let (dirs, store) = Box::pin(crate::bucket::metadata_sys::test_support::isolated_store_over_temp_disks()).await;
    init_bucket_metadata_sys(store.clone(), Vec::new()).await;
    create_bucket(&store).await;
    let preparation = Uuid::new_v4();
    let prepared = store
        .prepare_native_object(BUCKET, KEY, preparation, &mut PutObjReader::from_vec(CONTENT.to_vec()))
        .await
        .expect("original real native preparation");
    drop(prepared);
    await_pending_owners(&store).await;
    let private = private_path(&dirs[0], preparation);
    let original = std::fs::read(private.join("xl.meta")).expect("original actual writer metadata");
    let mut info = rustfs_filemeta::get_file_info(
        &original,
        BUCKET,
        KEY,
        &preparation.to_string(),
        rustfs_filemeta::FileInfoOpts {
            data: true,
            include_free_versions: false,
            include_part_checksums: true,
        },
    )
    .expect("strictly decode real writer metadata");
    let original_block_size = info.erasure.block_size;
    info.erasure.block_size /= 2;
    assert!(info.erasure.block_size > CONTENT.len());
    assert_ne!(info.erasure.block_size, original_block_size);
    let mut meta = rustfs_filemeta::FileMeta::new();
    meta.add_version(info).expect("real metadata writer for changed geometry");
    let changed = meta.marshal_msg().expect("encode actual changed raw metadata");
    let mut encoded = std::fs::read_to_string(private.join("intent.json")).expect("actual canonical durable intent");
    let original_intent: serde_json::Value = serde_json::from_str(&encoded).expect("decode actual intent");
    // Preserve the original canonical field order. Change only the three raw-byte
    // bindings, so this is a coherent candidate rather than a checksum mismatch.
    for (field, value) in [
        ("metadata_digest", serde_json::json!(Sha256::digest(&changed).to_vec())),
        ("metadata_length", serde_json::json!(changed.len())),
        ("recovery_metadata", serde_json::json!(base64_simd::STANDARD.encode_to_string(&changed))),
    ] {
        let old = format!("\"{field}\":{}", serde_json::to_string(&original_intent[field]).expect("original field"));
        let new = format!("\"{field}\":{}", serde_json::to_string(&value).expect("changed field"));
        assert_eq!(encoded.matches(&old).count(), 1, "only one original canonical binding");
        encoded = encoded.replacen(&old, &new, 1);
    }
    std::fs::write(private.join("xl.meta"), &changed).expect("replace own actual shard metadata");
    std::fs::write(private.join("intent.json"), encoded.as_bytes()).expect("retain coherent actual intent");
    std::fs::write(private.join("PREPARED"), Sha256::digest(encoded.as_bytes())).expect("coherent existing marker");
    let set = store.pools[0].get_disks_by_key(KEY);
    let disk = set.disks.read().await[0].clone().expect("actual first physical disk");
    let crate::disk::Disk::Local(local) = disk.as_ref() else {
        panic!("local fixture disk");
    };
    let fence = store
        .object_lock_config_snapshot(BUCKET)
        .await
        .expect("fresh real lifecycle fence");
    let incarnation = store.bucket_incarnation_id(BUCKET).await.expect("actual bucket incarnation");
    local
        .get_disk()
        .recover_native(preparation, store.id, BUCKET, incarnation, fence)
        .await
        .expect("NP02_GEOMETRY_IDENTITY_BOUND: changed short shard remains a valid real NP01 candidate");
    assert!(
        store
            .prepare_native_object(BUCKET, KEY, preparation, &mut PutObjReader::from_vec(CONTENT.to_vec()))
            .await
            .is_err(),
        "NP02_GEOMETRY_IDENTITY_BOUND: top-level retry rejects a different actual block geometry"
    );
    await_pending_owners(&store).await;
    assert!(
        store.recover_native_object(BUCKET, KEY, preparation).await.is_err(),
        "NP02_GEOMETRY_IDENTITY_BOUND: top-level recovery rejects mixed actual block geometry"
    );
}
