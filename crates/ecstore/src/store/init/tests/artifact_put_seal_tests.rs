use super::{build_isolated_test_store, without_storage_class_env};
use crate::{
    bucket::{artifact::artifact_put_seal_record_path, metadata_sys},
    config::com,
    disk::DiskAPI as _,
    object_api::{ObjectInfo, ObjectOptions, PutObjReader, WriteCompletion},
    runtime::instance::InstanceContext,
    set_disk::{PutObjectCommitBarrier, PutObjectCommitPause, rename_fanout_barrier},
    storage_api_contracts::{
        bucket::{BucketOperations as _, DeleteBucketOptions, MakeBucketOptions},
        object::ObjectIO as _,
    },
    store::ECStore,
};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const DEADLINE: Duration = Duration::from_secs(30);

fn attempt_prefix() -> String {
    format!("outputs/74656e616e74/{}/{}/", Uuid::new_v4(), Uuid::new_v4())
}

async fn open_store(root: &std::path::Path) -> (Arc<InstanceContext>, Arc<ECStore>, CancellationToken) {
    let (ctx, store, shutdown) = without_storage_class_env(build_isolated_test_store(root, "artifact-put-seal", &[4])).await;
    metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
    (ctx, store, shutdown)
}

async fn create_bucket(store: &ECStore, bucket: &str) -> Uuid {
    store
        .make_bucket(
            bucket,
            &MakeBucketOptions {
                artifact_base_prefix: Some("outputs/".to_owned()),
                ..Default::default()
            },
        )
        .await
        .expect("create a real versioned artifact bucket");
    store
        .bucket_incarnation_id(bucket)
        .await
        .expect("read actual bucket incarnation")
}

async fn put(store: &ECStore, bucket: &str, object: &str) -> ObjectInfo {
    store
        .put_object(
            bucket,
            object,
            &mut PutObjReader::from_vec(vec![0xA5; 273]),
            &ObjectOptions {
                versioned: true,
                write_completion: WriteCompletion::TailDrained,
                ..Default::default()
            },
        )
        .await
        .expect("unsealed PUT must commit on all four disks")
}

async fn disk_metadata(store: &ECStore, bucket: &str, object: &str) -> Vec<Vec<u8>> {
    let disks = store.pools[0].disk_set[0].disk_inventory().await;
    assert_eq!(disks.len(), 4);
    let mut metadata = Vec::new();
    for disk in disks {
        metadata.push(
            disk.expect("real fixture disk is online")
                .read_xl(bucket, object, false)
                .await
                .expect("read actual per-disk versions")
                .buf,
        );
    }
    metadata
}

async fn assert_rejected_without_new_version(store: &ECStore, bucket: &str, object: &str, before: &[Vec<u8>]) {
    let result = store
        .put_object(
            bucket,
            object,
            &mut PutObjReader::from_vec(vec![0xBB; 379]),
            &ObjectOptions {
                versioned: true,
                write_completion: WriteCompletion::TailDrained,
                ..Default::default()
            },
        )
        .await;
    assert!(result.is_err(), "ARTIFACT_PUT_SEALED: later PUT must be rejected");
    assert_eq!(
        disk_metadata(store, bucket, object).await,
        before,
        "ARTIFACT_PUT_SEALED: no disk gains a version"
    );
}

async fn tail_barrier_case(cancel_put_waiter: bool) {
    let root = tempfile::tempdir().expect("artifact tail fixture directory");
    let (ctx, store, shutdown) = open_store(root.path()).await;
    let bucket = "artifact-put-tail";
    let incarnation = create_bucket(&store, bucket).await;
    let prefix = attempt_prefix();
    let object = format!("{prefix}image.png");
    temp_env::async_with_vars([(crate::set_disk::ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("true"))], async {
        let tracker = rename_fanout_barrier::observe_tasks(&object);
        let tail = rename_fanout_barrier::arm(&object, 0, rename_fanout_barrier::PHASE_RENAME);
        let quorum = PutObjectCommitBarrier::install(bucket, &object, PutObjectCommitPause::AfterRenameQuorum);
        let handoff = PutObjectCommitBarrier::install(bucket, &object, PutObjectCommitPause::AfterRenameHandoff);
        let put_store = store.clone();
        let put_object = object.clone();
        let put = tokio::spawn(async move {
            put_store
                .put_object(
                    bucket,
                    &put_object,
                    &mut PutObjReader::from_vec(vec![0xA5; 273]),
                    &ObjectOptions {
                        versioned: true,
                        ..Default::default()
                    },
                )
                .await
        });
        tokio::time::timeout(DEADLINE, tail.wait_until_paused())
            .await
            .expect("one real disk pauses before rename");
        quorum.wait_until_paused().await;
        if cancel_put_waiter {
            put.abort();
        }
        quorum.release();
        handoff.wait_until_paused().await;
        handoff.release();
        let result = tokio::time::timeout(DEADLINE, put)
            .await
            .expect("PUT waiter returns while its tail remains paused");
        if cancel_put_waiter {
            assert!(result.expect_err("cancelled PUT waiter").is_cancelled());
        } else {
            result
                .expect("PUT task joins")
                .expect("PUT quorum ACK succeeds before tail completion");
        }
        assert!(tracker.running() > 0, "real rename tail must still be live");
        let seal_store = store.clone();
        let seal_prefix = prefix.clone();
        let mut seal =
            tokio::spawn(async move { seal_store.seal_artifact_attempt_puts(bucket, incarnation, &seal_prefix).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(200), &mut seal).await.is_err(),
            "ARTIFACT_PUT_TAIL_BARRIER: seal cannot finish while a real rename tail is paused"
        );
        assert!(tracker.running() > 0, "tail remains paused throughout the seal attempt");
        tail.release();
        tokio::time::timeout(DEADLINE, seal)
            .await
            .expect("seal resumes after tail release")
            .expect("seal task joins")
            .expect("seal persists");
        tokio::time::timeout(DEADLINE, async {
            while tracker.running() != 0 || ctx.namespace_commits_pending() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("actual detached write owners drain");
        let before = disk_metadata(&store, bucket, &object).await;
        assert_rejected_without_new_version(&store, bucket, &object, &before).await;
    })
    .await;
    shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(storage_class_env)]
async fn artifact_put_seal_waits_for_quorum_ack_rename_tail() {
    tail_barrier_case(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(storage_class_env)]
async fn artifact_put_seal_waits_after_put_waiter_cancellation() {
    tail_barrier_case(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(storage_class_env)]
async fn artifact_put_seal_survives_reopen_and_is_attempt_scoped() {
    let root = tempfile::tempdir().expect("artifact durable seal directory");
    let (ctx, store, shutdown) = open_store(root.path()).await;
    let bucket = "artifact-put-reopen";
    let incarnation = create_bucket(&store, bucket).await;
    let prefix = attempt_prefix();
    let object = format!("{prefix}image.png");
    put(&store, bucket, &object).await;
    let before = disk_metadata(&store, bucket, &object).await;
    store
        .clone()
        .seal_artifact_attempt_puts(bucket, incarnation, &prefix)
        .await
        .expect("persist first seal");
    store
        .clone()
        .seal_artifact_attempt_puts(bucket, incarnation, &prefix)
        .await
        .expect("same seal is idempotent");
    assert_rejected_without_new_version(&store, bucket, &object, &before).await;
    put(&store, bucket, &format!("{}other.png", attempt_prefix())).await;
    shutdown.cancel();
    assert!(
        ctx.wait_for_detached_mutations(DEADLINE).await,
        "old instance mutations drain before reopen"
    );
    drop(store);
    drop(ctx);
    let (_ctx, store, shutdown) = open_store(root.path()).await;
    metadata_sys::reload_bucket_metadata(store.clone(), bucket)
        .await
        .expect("reload dedicated bucket after restart");
    assert_eq!(store.bucket_incarnation_id(bucket).await.expect("reopened incarnation"), incarnation);
    assert_rejected_without_new_version(&store, bucket, &object, &before).await;
    store
        .clone()
        .seal_artifact_attempt_puts(bucket, incarnation, &prefix)
        .await
        .expect("persisted seal remains idempotent");
    shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(storage_class_env)]
async fn artifact_put_seal_rejects_corrupt_and_wrong_incarnation_records() {
    let root = tempfile::tempdir().expect("artifact invalid seal record directory");
    let (_ctx, store, shutdown) = open_store(root.path()).await;
    let bucket = "artifact-put-corrupt";
    let prefix = attempt_prefix();
    let old_incarnation = create_bucket(&store, bucket).await;
    store
        .clone()
        .seal_artifact_attempt_puts(bucket, old_incarnation, &prefix)
        .await
        .expect("produce a real previous-generation seal");
    let old_record = com::read_config(store.clone(), &artifact_put_seal_record_path(bucket, old_incarnation, &prefix))
        .await
        .expect("read actual seal bytes");
    store
        .delete_bucket(bucket, &DeleteBucketOptions::default())
        .await
        .expect("delete empty old-generation bucket");
    let incarnation = create_bucket(&store, bucket).await;
    assert_ne!(incarnation, old_incarnation);
    assert!(
        store
            .clone()
            .seal_artifact_attempt_puts(bucket, old_incarnation, &prefix)
            .await
            .is_err(),
        "wrong expected incarnation cannot seal current bucket"
    );
    let object = format!("{prefix}image.png");
    put(&store, bucket, &object).await;
    let before = disk_metadata(&store, bucket, &object).await;
    let record = artifact_put_seal_record_path(bucket, incarnation, &prefix);
    for invalid_record in [old_record, b"not-json".to_vec()] {
        com::save_config_with_opts(
            store.clone(),
            &record,
            invalid_record,
            &ObjectOptions {
                max_parity: true,
                write_completion: WriteCompletion::TailDrained,
                ..Default::default()
            },
        )
        .await
        .expect("persist actual invalid seal record on all disks");
        assert_rejected_without_new_version(&store, bucket, &object, &before).await;
        assert!(
            store
                .clone()
                .seal_artifact_attempt_puts(bucket, incarnation, &prefix)
                .await
                .is_err(),
            "invalid seal cannot be treated as absent or idempotent success"
        );
    }
    shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(storage_class_env)]
async fn artifact_put_seal_does_not_change_ordinary_bucket_puts() {
    let root = tempfile::tempdir().expect("ordinary bucket seal regression directory");
    let (_ctx, store, shutdown) = open_store(root.path()).await;
    let bucket = "artifact-put-ordinary";
    store
        .make_bucket(
            bucket,
            &MakeBucketOptions {
                versioning_enabled: true,
                ..Default::default()
            },
        )
        .await
        .expect("create ordinary versioned bucket");
    let incarnation = store.bucket_incarnation_id(bucket).await.expect("ordinary incarnation");
    let prefix = attempt_prefix();
    let object = format!("{prefix}image.png");
    let first = put(&store, bucket, &object).await;
    assert!(
        store
            .clone()
            .seal_artifact_attempt_puts(bucket, incarnation, &prefix)
            .await
            .is_err(),
        "ordinary bucket cannot acquire artifact seal semantics"
    );
    let second = put(&store, bucket, &object).await;
    assert_ne!(first.version_id, second.version_id, "ordinary versioned PUT creates another version");
    shutdown.cancel();
}
