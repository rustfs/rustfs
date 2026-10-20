#![cfg(unix)]

use super::{build_isolated_test_store, run_large_stack_async_test, without_storage_class_env};
use crate::{
    bucket::{
        artifact::{artifact_put_seal_record_path, artifact_repair_seal_record_path},
        metadata_sys,
    },
    config::com,
    disk::{Disk, DiskAPI as _, DiskStore},
    object_api::{ObjectOptions, PutObjReader, WriteCompletion},
    storage_api_contracts::{
        bucket::{BucketOperations as _, MakeBucketOptions},
        namespace::NamespaceLocking as _,
        object::{ObjectIO as _, ObjectOperations as _},
    },
    store::ECStore,
};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::io::AsyncReadExt as _;
use uuid::Uuid;

const DEADLINE: Duration = Duration::from_secs(30);
const BUCKET: &str = "artifact-writes-seal";

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_exact_delete_preserves_other_versions_and_retries_absence() {
    run_large_stack_async_test("artifact-exact-delete", || async {
        let root = tempfile::tempdir().expect("exact delete root");
        let (ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(root.path(), "artifact-exact-delete", &[4])).await;
        assert!(store.supports_artifact_exact_delete().await, "local fixture supports typed deletion");
        metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let incarnation = create_bucket(&store, BUCKET, true).await;
        let attempt = prefix();
        let object = format!("{attempt}image.png");
        let retained = put(&store, BUCKET, &object)
            .await
            .expect("retained version")
            .version_id
            .expect("versioned PUT");
        let deleted = put(&store, BUCKET, &object)
            .await
            .expect("orphan version")
            .version_id
            .expect("versioned PUT");
        store
            .clone()
            .seal_artifact_attempt_writes(BUCKET, incarnation, &attempt)
            .await
            .expect("combined drain");
        crate::data_usage::seed_bucket_usage_memory_for_test(BUCKET, 2 * 1024 * 1024).await;
        store
            .clone()
            .delete_artifact_object_version(BUCKET, incarnation, &attempt, &object, deleted)
            .await
            .expect("ARTIFACT_EXACT_DELETE: real managed deletion");
        let mut displayed = rustfs_data_usage::DataUsageInfo::default();
        crate::data_usage::apply_bucket_usage_memory_overlay(&mut displayed).await;
        assert_eq!(
            displayed.bucket_sizes.get(BUCKET),
            Some(&(1024 * 1024)),
            "ARTIFACT_EXACT_DELETE: committed delete applies one usage delta"
        );
        assert_eq!(
            crate::data_usage::get_bucket_usage_memory(BUCKET).await,
            Some(2 * 1024 * 1024),
            "ARTIFACT_EXACT_DELETE: quota keeps its pending-negative reservation until scanner confirmation"
        );
        let exact = |version: Uuid| ObjectOptions {
            version_id: Some(version.to_string()),
            suppress_read_repair: true,
            ..Default::default()
        };
        assert!(
            store.get_object_info(BUCKET, &object, &exact(deleted)).await.is_err(),
            "ARTIFACT_EXACT_DELETE: deleted exact version is absent"
        );
        assert!(
            store.get_object_info(BUCKET, &object, &exact(retained)).await.is_ok(),
            "ARTIFACT_EXACT_DELETE: another version remains readable"
        );
        let mut reader = store
            .get_object_reader(BUCKET, &object, None, Default::default(), &exact(retained))
            .await
            .expect("retained exact-version GET after deletion");
        let mut body = Vec::new();
        reader
            .stream
            .read_to_end(&mut body)
            .await
            .expect("retained bytes after deletion");
        drop(reader);
        assert_eq!(body, vec![0xA5; 1024 * 1024], "ARTIFACT_EXACT_DELETE: retained exact bytes are unchanged");
        store
            .clone()
            .delete_artifact_object_version(BUCKET, incarnation, &attempt, &object, deleted)
            .await
            .expect("ARTIFACT_EXACT_DELETE: same-identity absence is retryable");
        let mut displayed = rustfs_data_usage::DataUsageInfo::default();
        crate::data_usage::apply_bucket_usage_memory_overlay(&mut displayed).await;
        assert_eq!(
            displayed.bucket_sizes.get(BUCKET),
            Some(&(1024 * 1024)),
            "ARTIFACT_EXACT_DELETE: strict absence retry cannot subtract twice"
        );
        assert!(
            store.delete_object(BUCKET, &object, exact(retained)).await.is_err(),
            "ARTIFACT_EXACT_DELETE: ordinary S3 delete remains rejected"
        );
        ctx.update_erasure_type(crate::layout::endpoints::SetupType::DistErasure)
            .await;
        assert!(!store.supports_artifact_exact_delete().await, "distributed capability is unavailable");
        assert!(
            store
                .clone()
                .delete_artifact_object_version(BUCKET, incarnation, &attempt, &object, retained)
                .await
                .is_err(),
            "ARTIFACT_EXACT_DELETE: distributed mutation rejects before deletion"
        );
        ctx.update_erasure_type(crate::layout::endpoints::SetupType::Erasure).await;
        assert!(store.get_object_info(BUCKET, &object, &exact(retained)).await.is_ok());
        store
            .clone()
            .delete_artifact_object_version(BUCKET, incarnation, &attempt, &object, retained)
            .await
            .expect("ARTIFACT_EXACT_DELETE: last-version deletion");
        assert!(
            store.get_object_info(BUCKET, &object, &exact(retained)).await.is_err(),
            "ARTIFACT_EXACT_DELETE: the final exact version is absent"
        );
        shutdown.cancel();
    });
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_exact_delete_requires_complete_seal_and_exact_identity() {
    run_large_stack_async_test("artifact-exact-delete-fences", || async {
        let root = tempfile::tempdir().expect("exact delete fences root");
        let (_ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(root.path(), "artifact-exact-delete-fences", &[4])).await;
        metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let incarnation = create_bucket(&store, BUCKET, true).await;
        let attempt = prefix();
        let object = format!("{attempt}image.png");
        let version = put(&store, BUCKET, &object)
            .await
            .expect("real object")
            .version_id
            .expect("versioned PUT");
        assert!(
            store
                .clone()
                .delete_artifact_object_version(BUCKET, incarnation, &attempt, &object, version)
                .await
                .is_err(),
            "ARTIFACT_EXACT_DELETE_SEAL: unsealed attempt is protected"
        );
        store
            .clone()
            .seal_artifact_attempt_puts(BUCKET, incarnation, &attempt)
            .await
            .expect("PUT seal only");
        assert!(
            store
                .clone()
                .delete_artifact_object_version(BUCKET, incarnation, &attempt, &object, version)
                .await
                .is_err(),
            "ARTIFACT_EXACT_DELETE_SEAL: repair domain is still open"
        );
        store
            .clone()
            .seal_artifact_attempt_writes(BUCKET, incarnation, &attempt)
            .await
            .expect("combined seal");
        for (expected, path, key, id) in [
            (Uuid::new_v4(), attempt.clone(), object.clone(), version),
            (incarnation, attempt.clone(), object.clone(), Uuid::nil()),
            (incarnation, prefix(), object.clone(), version),
            (incarnation, attempt.clone(), attempt.clone(), version),
        ] {
            assert!(
                store
                    .clone()
                    .delete_artifact_object_version(BUCKET, expected, &path, &key, id)
                    .await
                    .is_err(),
                "ARTIFACT_EXACT_DELETE_IDENTITY: malformed or changed identity rejects"
            );
        }
        assert!(
            store
                .clone()
                .delete_artifact_object_version("missing-managed-bucket", incarnation, &attempt, &object, version)
                .await
                .is_err(),
            "ARTIFACT_EXACT_DELETE_IDENTITY: missing bucket is not confirmed absence"
        );
        assert!(
            store
                .get_object_info(
                    BUCKET,
                    &object,
                    &ObjectOptions {
                        version_id: Some(version.to_string()),
                        ..Default::default()
                    }
                )
                .await
                .is_ok(),
            "ARTIFACT_EXACT_DELETE_IDENTITY: rejected requests preserve data"
        );
        shutdown.cancel();
    });
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_exact_delete_directory_marker_retries_exact_absence() {
    run_large_stack_async_test("artifact-exact-delete-directory", || async {
        let root = tempfile::tempdir().expect("directory delete root");
        let (_ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(root.path(), "artifact-exact-delete-directory", &[4])).await;
        metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let incarnation = create_bucket(&store, BUCKET, true).await;
        let attempt = prefix();
        let object = format!("{attempt}directory/");
        let version = store
            .put_object(
                BUCKET,
                &object,
                &mut PutObjReader::from_vec(Vec::new()),
                &ObjectOptions {
                    versioned: true,
                    write_completion: WriteCompletion::TailDrained,
                    ..Default::default()
                },
            )
            .await
            .expect("real directory marker")
            .version_id
            .expect("versioned directory marker");
        store
            .clone()
            .seal_artifact_attempt_writes(BUCKET, incarnation, &attempt)
            .await
            .expect("combined directory marker seal");
        store
            .clone()
            .delete_artifact_object_version(BUCKET, incarnation, &attempt, &object, version)
            .await
            .expect("ARTIFACT_EXACT_DELETE_DIRECTORY: first exact deletion");
        let retry = store
            .clone()
            .delete_artifact_object_version(BUCKET, incarnation, &attempt, &object, version)
            .await;
        assert!(
            retry.is_ok(),
            "ARTIFACT_EXACT_DELETE_DIRECTORY: same directory identity retries absence: {retry:?}"
        );
        shutdown.cancel();
    });
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_exact_delete_waits_for_its_exact_object_namespace_owner() {
    use crate::store::DeleteAfterObjectLockSnapshotBarrier;
    run_large_stack_async_test("artifact-exact-delete-object-lock", || async {
        let root = tempfile::tempdir().expect("object lock delete root");
        let (_ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(root.path(), "artifact-exact-delete-object-lock", &[4])).await;
        metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let incarnation = create_bucket(&store, BUCKET, true).await;
        let attempt = prefix();
        let object = format!("{attempt}image.png");
        put(&store, BUCKET, &object).await.expect("other exact version");
        let version = put(&store, BUCKET, &object)
            .await
            .expect("target exact version")
            .version_id
            .expect("versioned target");
        store
            .clone()
            .seal_artifact_attempt_writes(BUCKET, incarnation, &attempt)
            .await
            .expect("combined seal");
        let namespace = store.new_ns_lock(BUCKET, &object).await.expect("exact object namespace");
        let reader = namespace.get_read_lock(DEADLINE).await.expect("real exact-object read owner");
        let barrier = DeleteAfterObjectLockSnapshotBarrier::install(BUCKET);
        let worker_store = store.clone();
        let worker_object = object.clone();
        let mut worker = tokio::spawn(async move {
            worker_store
                .delete_artifact_object_version(BUCKET, incarnation, &attempt, &worker_object, version)
                .await
        });
        tokio::time::timeout(DEADLINE, barrier.wait_until_paused())
            .await
            .expect("delete owns fresh policy snapshot before object lock");
        barrier.release();
        let premature = tokio::time::timeout(Duration::from_secs(1), &mut worker).await;
        assert!(
            premature.is_err(),
            "ARTIFACT_EXACT_DELETE_OBJECT_LOCK: seal owners cannot replace the exact object owner: {premature:?}"
        );
        drop(reader);
        tokio::time::timeout(DEADLINE, worker)
            .await
            .expect("delete resumes after exact reader releases")
            .expect("delete joins")
            .expect("delete with real exact-object owner succeeds");
        assert!(
            barrier.namespace_acquired(),
            "ARTIFACT_EXACT_DELETE_OBJECT_LOCK: normal DELETE acquired its exact owner"
        );
        shutdown.cancel();
    });
}

fn prefix() -> String {
    format!("outputs/74656e616e74/{}/{}/", Uuid::new_v4(), Uuid::new_v4())
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_exact_delete_keeps_object_lock_retention() {
    run_large_stack_async_test("artifact-exact-delete-retention", || async {
        let root = tempfile::tempdir().expect("retained artifact root");
        let (_ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(root.path(), "artifact-exact-delete-retention", &[4])).await;
        metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        store
            .make_bucket(
                BUCKET,
                &MakeBucketOptions {
                    artifact_base_prefix: Some("outputs/".to_owned()),
                    versioning_enabled: true,
                    lock_enabled: true,
                    ..Default::default()
                },
            )
            .await
            .expect("managed Object Lock bucket");
        let incarnation = store.bucket_incarnation_id(BUCKET).await.expect("retained bucket identity");
        let attempt = prefix();
        let object = format!("{attempt}retained.png");
        let version = store
            .put_object(
                BUCKET,
                &object,
                &mut PutObjReader::from_vec(vec![1; 4096]),
                &ObjectOptions {
                    versioned: true,
                    user_defined: std::collections::HashMap::from([
                        ("x-amz-object-lock-mode".to_owned(), "COMPLIANCE".to_owned()),
                        ("x-amz-object-lock-retain-until-date".to_owned(), "2999-01-01T00:00:00Z".to_owned()),
                    ]),
                    write_completion: WriteCompletion::TailDrained,
                    ..Default::default()
                },
            )
            .await
            .expect("retained exact version")
            .version_id
            .expect("versioned PUT");
        store
            .clone()
            .seal_artifact_attempt_writes(BUCKET, incarnation, &attempt)
            .await
            .expect("combined seal");
        assert!(
            store
                .clone()
                .delete_artifact_object_version(BUCKET, incarnation, &attempt, &object, version)
                .await
                .is_err(),
            "ARTIFACT_EXACT_DELETE_RETENTION: typed cleanup cannot bypass compliance"
        );
        assert!(
            store
                .get_object_info(
                    BUCKET,
                    &object,
                    &ObjectOptions {
                        version_id: Some(version.to_string()),
                        ..Default::default()
                    }
                )
                .await
                .is_ok(),
            "ARTIFACT_EXACT_DELETE_RETENTION: retained version stays readable"
        );
        shutdown.cancel();
    });
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_exact_delete_cancel_keeps_incarnation_owner_until_completion() {
    use crate::{storage_api_contracts::bucket::DeleteBucketOptions, store::DeleteAfterObjectLockSnapshotBarrier};
    run_large_stack_async_test("artifact-exact-delete-cancel", || async {
        let root = tempfile::tempdir().expect("cancelled delete root");
        let (_ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(root.path(), "artifact-exact-delete-cancel", &[4])).await;
        metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let incarnation = create_bucket(&store, BUCKET, true).await;
        let attempt = prefix();
        let object = format!("{attempt}image.png");
        let version = put(&store, BUCKET, &object)
            .await
            .expect("real exact object")
            .version_id
            .expect("versioned PUT");
        store
            .clone()
            .seal_artifact_attempt_writes(BUCKET, incarnation, &attempt)
            .await
            .expect("combined seal");
        let barrier = DeleteAfterObjectLockSnapshotBarrier::install(BUCKET);
        let worker_store = store.clone();
        let worker_attempt = attempt.clone();
        let worker_object = object.clone();
        let worker = tokio::spawn(async move {
            worker_store
                .delete_artifact_object_version(BUCKET, incarnation, &worker_attempt, &worker_object, version)
                .await
        });
        tokio::time::timeout(DEADLINE, barrier.wait_until_paused())
            .await
            .expect("real delete owns its metadata snapshot");
        worker.abort();
        assert!(worker.await.expect_err("HTTP waiter cancelled").is_cancelled());
        let deleting_store = store.clone();
        let mut delete_bucket = tokio::spawn(async move {
            deleting_store
                .delete_bucket(
                    BUCKET,
                    &DeleteBucketOptions {
                        force: true,
                        ..Default::default()
                    },
                )
                .await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut delete_bucket)
                .await
                .is_err(),
            "ARTIFACT_EXACT_DELETE_CANCEL: cancellation must not release the lifecycle owner"
        );
        barrier.release();
        tokio::time::timeout(DEADLINE, delete_bucket)
            .await
            .expect("delete owner eventually drains")
            .expect("bucket worker joins")
            .expect("bucket deletion after owned operation");
        let replacement = create_bucket(&store, BUCKET, true).await;
        assert_ne!(replacement, incarnation, "recreated bucket gets a new incarnation");
        assert!(
            store
                .clone()
                .delete_artifact_object_version(BUCKET, incarnation, &attempt, &object, version)
                .await
                .is_err(),
            "ARTIFACT_EXACT_DELETE_CANCEL: old identity cannot acknowledge a replacement bucket"
        );
        shutdown.cancel();
    });
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_exact_delete_reconfirms_real_physical_writer_drain() {
    run_large_stack_async_test("artifact-exact-delete-drain", || async {
        let root = tempfile::tempdir().expect("delete drain root");
        let (_ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(root.path(), "artifact-exact-delete-drain", &[4])).await;
        metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let incarnation = create_bucket(&store, BUCKET, true).await;
        let attempt = prefix();
        let object = format!("{attempt}image.png");
        let version = put(&store, BUCKET, &object)
            .await
            .expect("real object")
            .version_id
            .expect("versioned PUT");
        let receiver_key = key(&store, incarnation, &attempt).await;
        let members = disks(&store).await;
        let Disk::Local(local) = members[0].as_ref() else { panic!("local fixture") };
        let tail = local
            .acquire_artifact_put_lease(receiver_key)
            .await
            .expect("real earlier writer owner");
        // Persist the two logical records while an earlier receiver still owns
        // its tail: records alone must not authorize physical deletion.
        for repair in [false, true] {
            let mut fact = serde_json::json!({"bucket": BUCKET,"incarnation_id":incarnation,"base_prefix":"outputs/","attempt_prefix":attempt});
            fact[if repair { "format_version" } else { "version" }] = 1.into();
            let path = if repair {
                artifact_repair_seal_record_path(BUCKET, incarnation, &attempt)
            } else {
                artifact_put_seal_record_path(BUCKET, incarnation, &attempt)
            };
            com::save_config(store.clone(), &path, serde_json::to_vec(&fact).expect("valid logical seal"))
                .await
                .expect("actual persisted seal");
        }
        let worker_store = store.clone();
        let worker_attempt = attempt.clone();
        let worker_object = object.clone();
        let worker = tokio::spawn(async move {
            worker_store
                .delete_artifact_object_version(BUCKET, incarnation, &worker_attempt, &worker_object, version)
                .await
        });
        wait_marker(&marker(&members[0], receiver_key, false)).await;
        assert!(!worker.is_finished(), "ARTIFACT_EXACT_DELETE_DRAIN: physical tail prevents confirmation");
        drop(tail);
        tokio::time::timeout(DEADLINE, worker)
            .await
            .expect("physical owner drains")
            .expect("delete joins")
            .expect("delete follows physical closure");
        assert!(
            store
                .get_object_info(
                    BUCKET,
                    &object,
                    &ObjectOptions {
                        version_id: Some(version.to_string()),
                        ..Default::default()
                    }
                )
                .await
                .is_err(),
            "ARTIFACT_EXACT_DELETE_DRAIN: exact deletion completes after drain"
        );
        shutdown.cancel();
    });
}

async fn create_bucket(store: &ECStore, name: &str, managed: bool) -> Uuid {
    store
        .make_bucket(
            name,
            &MakeBucketOptions {
                artifact_base_prefix: managed.then(|| "outputs/".to_owned()),
                versioning_enabled: true,
                ..Default::default()
            },
        )
        .await
        .expect("real versioned bucket");
    store.bucket_incarnation_id(name).await.expect("current incarnation")
}

async fn put(store: &ECStore, bucket: &str, object: &str) -> crate::error::Result<crate::object_api::ObjectInfo> {
    store
        .put_object(
            bucket,
            object,
            &mut PutObjReader::from_vec(vec![0xA5; 1024 * 1024]),
            &ObjectOptions {
                versioned: true,
                write_completion: WriteCompletion::TailDrained,
                ..Default::default()
            },
        )
        .await
}

async fn disks(store: &ECStore) -> Vec<DiskStore> {
    store.pools[0].disk_set[0]
        .disk_inventory()
        .await
        .into_iter()
        .map(|disk| disk.expect("online fixture member"))
        .collect()
}

async fn key(store: &ECStore, incarnation: Uuid, attempt: &str) -> [u8; 32] {
    store
        .artifact_receiver_identity(BUCKET, incarnation, attempt, true)
        .await
        .expect("actual receiver identity")
        .receiver_key
}

fn marker(disk: &DiskStore, key: [u8; 32], repair: bool) -> PathBuf {
    disk.path()
        .join(if repair {
            ".rustfs-artifact-repair-seals"
        } else {
            ".rustfs-artifact-put-seals"
        })
        .join(hex_simd::encode_to_string(key, hex_simd::AsciiCase::Lower))
        .join("closed")
}

async fn wait_marker(path: &std::path::Path) {
    tokio::time::timeout(DEADLINE, async {
        while !path.is_file() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("real disk admission reaches its monotonic close point");
}

async fn assert_domains_closed(disks: &[DiskStore], key: [u8; 32]) {
    for disk in disks {
        let Disk::Local(local) = disk.as_ref() else {
            panic!("real local fixture");
        };
        assert!(
            local.acquire_artifact_put_lease(key).await.is_err(),
            "ARTIFACT_WRITES_SEAL_PUT_DOMAIN_CLOSED"
        );
        assert!(
            local.acquire_artifact_repair_lease(key).await.is_err(),
            "ARTIFACT_WRITES_SEAL_REPAIR_DOMAIN_CLOSED"
        );
    }
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_writes_seal_first_retry_and_reopen_close_both_domains() {
    run_large_stack_async_test("artifact-writes-seal-reopen", || async {
        let root = tempfile::tempdir().expect("writes seal root");
        let (ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(root.path(), "artifact-writes", &[4])).await;
        metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let incarnation = create_bucket(&store, BUCKET, true).await;
        let attempt = prefix();
        let object = format!("{attempt}payload.bin");
        let version = put(&store, BUCKET, &object)
            .await
            .expect("seed real shards")
            .version_id
            .expect("exact version");
        let members = disks(&store).await;
        let before = futures::future::join_all(members.iter().map(|disk| disk.read_xl(BUCKET, &object, false)))
            .await
            .into_iter()
            .map(|result| result.expect("original physical metadata").buf)
            .collect::<Vec<_>>();
        for _ in 0..2 {
            store
                .clone()
                .seal_artifact_attempt_writes(BUCKET, incarnation, &attempt)
                .await
                .expect("joint confirmation on every receiver");
        }
        let receiver_key = key(&store, incarnation, &attempt).await;
        assert_domains_closed(&members, receiver_key).await;
        for (member, metadata) in members.iter().zip(&before) {
            assert_eq!(
                &member
                    .read_xl(BUCKET, &object, false)
                    .await
                    .expect("read sealed metadata")
                    .buf,
                metadata
            );
            assert_eq!(
                member
                    .read_version(BUCKET, BUCKET, &object, &version.to_string(), &crate::disk::ReadOptions::default())
                    .await
                    .expect("sealed version remains readable")
                    .version_id,
                Some(version)
            );
        }
        assert!(put(&store, BUCKET, &object).await.is_err(), "joint seal rejects logical PUT");
        let healed = store
            .heal_object_with_proof(
                BUCKET,
                &object,
                &version.to_string(),
                &rustfs_heal_contracts::heal_channel::HealOpts::default(),
            )
            .await;
        assert!(healed.is_err(), "joint seal rejects repair even when no shard is missing");
        for path in [
            artifact_put_seal_record_path(BUCKET, incarnation, &attempt),
            artifact_repair_seal_record_path(BUCKET, incarnation, &attempt),
        ] {
            com::read_config(store.clone(), &path)
                .await
                .expect("both original immutable records exist");
        }
        put(&store, BUCKET, &format!("{}other.bin", prefix()))
            .await
            .expect("another attempt remains writable");
        create_bucket(&store, "artifact-writes-control", false).await;
        put(&store, "artifact-writes-control", "object")
            .await
            .expect("ordinary bucket still writes");
        drop(members);
        shutdown.cancel();
        assert!(ctx.wait_for_detached_mutations(DEADLINE).await);
        drop(store);
        drop(ctx);
        let (_ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(root.path(), "artifact-writes-reopen", &[4])).await;
        metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        metadata_sys::reload_bucket_metadata(store.clone(), BUCKET)
            .await
            .expect("reload persisted managed identity");
        store
            .clone()
            .seal_artifact_attempt_writes(BUCKET, incarnation, &attempt)
            .await
            .expect("reopened joint seal rechecks both domains");
        assert_domains_closed(&disks(&store).await, receiver_key).await;
        assert!(put(&store, BUCKET, &object).await.is_err());
        shutdown.cancel();
    });
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_writes_seal_cancelled_waiter_keeps_partial_close_and_drains() {
    run_large_stack_async_test("artifact-writes-seal-cancel", || async {
        let root = tempfile::tempdir().expect("partial close root");
        let (ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(root.path(), "artifact-writes-cancel", &[4])).await;
        metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let incarnation = create_bucket(&store, BUCKET, true).await;
        let attempt = prefix();
        let members = disks(&store).await;
        let receiver_key = key(&store, incarnation, &attempt).await;
        let Disk::Local(local) = members[0].as_ref() else {
            panic!("local fixture");
        };
        let owner = local
            .acquire_artifact_repair_lease(receiver_key)
            .await
            .expect("actual repair owner before close");
        let worker_store = Arc::clone(&store);
        let worker_attempt = attempt.clone();
        let waiter = tokio::spawn(async move {
            worker_store
                .seal_artifact_attempt_writes(BUCKET, incarnation, &worker_attempt)
                .await
        });
        for member in &members {
            wait_marker(&marker(member, receiver_key, false)).await;
            wait_marker(&marker(member, receiver_key, true)).await;
        }
        assert!(!waiter.is_finished(), "physical repair owner must prevent joint confirmation");
        waiter.abort();
        assert!(waiter.await.expect_err("cancel only seal request waiter").is_cancelled());
        assert_domains_closed(&members, receiver_key).await;
        drop(owner);
        tokio::time::timeout(DEADLINE, store.clone().seal_artifact_attempt_writes(BUCKET, incarnation, &attempt))
            .await
            .expect("owned first transaction and retry drain")
            .expect("identical retry confirms both permanent closures");
        shutdown.cancel();
        assert!(ctx.wait_for_detached_mutations(DEADLINE).await);
    });
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_writes_seal_rejects_corrupt_and_mismatched_records() {
    run_large_stack_async_test("artifact-writes-seal-records", || async {
        let root = tempfile::tempdir().expect("record failure root");
        let (_ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(root.path(), "artifact-writes-records", &[4])).await;
        metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let incarnation = create_bucket(&store, BUCKET, true).await;
        for repair in [false, true] {
            for kind in 0..3 {
                let attempt = prefix();
                let mut mismatch = serde_json::json!({
                    "bucket": BUCKET,
                    "incarnation_id": Uuid::new_v4(),
                    "base_prefix": "outputs/",
                    "attempt_prefix": attempt,
                });
                mismatch[if repair { "format_version" } else { "version" }] = 1.into();
                let bytes = match kind {
                    0 => Vec::new(),
                    1 => b"not-json".to_vec(),
                    _ => serde_json::to_vec(&mismatch).expect("valid record shape with another incarnation"),
                };
                let path = if repair {
                    artifact_repair_seal_record_path(BUCKET, incarnation, &attempt)
                } else {
                    artifact_put_seal_record_path(BUCKET, incarnation, &attempt)
                };
                com::save_config_with_opts(
                    store.clone(),
                    &path,
                    bytes.clone(),
                    &ObjectOptions {
                        max_parity: true,
                        write_completion: WriteCompletion::TailDrained,
                        ..Default::default()
                    },
                )
                .await
                .expect("persist actual bad record on every disk");
                assert!(
                    store
                        .clone()
                        .seal_artifact_attempt_writes(BUCKET, incarnation, &attempt)
                        .await
                        .is_err(),
                    "bad record cannot become joint confirmation"
                );
                let (preserved, info) = com::read_config_observation(store.clone(), &path, 16 * 1024)
                    .await
                    .expect("failed seal preserves the real config object, including an empty payload");
                assert_eq!(
                    info.size,
                    i64::try_from(bytes.len()).expect("small record length"),
                    "persisted object metadata retains the original payload length"
                );
                assert_eq!(preserved, bytes, "failed seal preserves actual record bytes");
                if repair {
                    let receiver_key = key(&store, incarnation, &attempt).await;
                    for disk in disks(&store).await {
                        assert!(
                            marker(&disk, receiver_key, false).is_file(),
                            "earlier PUT closure is monotonic when repair record fails"
                        );
                        let Disk::Local(local) = disk.as_ref() else {
                            panic!("local fixture");
                        };
                        drop(
                            local
                                .acquire_artifact_repair_lease(receiver_key)
                                .await
                                .expect("failed repair-record validation never claimed physical repair closure"),
                        );
                    }
                }
            }
        }
        shutdown.cancel();
    });
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_writes_seal_rejects_incomplete_duplicate_and_empty_identity_members() {
    run_large_stack_async_test("artifact-writes-seal-members", || async {
        let root = tempfile::tempdir().expect("membership root");
        let (_ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(root.path(), "artifact-writes-members", &[4])).await;
        metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let incarnation = create_bucket(&store, BUCKET, true).await;
        let set = &store.pools[0].disk_set[0];
        let original = set.disk_inventory().await;
        let attempt = prefix();
        for replacement in [
            {
                let mut disks = original.clone();
                disks[0] = None;
                disks
            },
            {
                let mut disks = original.clone();
                disks.pop();
                disks
            },
            {
                let mut disks = original.clone();
                disks[0] = disks[1].clone();
                disks
            },
        ] {
            *set.disks.write().await = replacement;
            let result = store
                .clone()
                .seal_artifact_attempt_writes(BUCKET, incarnation, &attempt)
                .await;
            *set.disks.write().await = original.clone();
            assert!(result.is_err(), "every unique configured slot is required");
        }
        let member = original[0].as_ref().expect("original member");
        let id = member.cached_disk_id().await.expect("registered identity");
        for invalid in [None, Some(Uuid::nil())] {
            member
                .set_disk_id_state(invalid)
                .await
                .expect("replace only registered ID state");
            let result = store
                .clone()
                .seal_artifact_attempt_writes(BUCKET, incarnation, &attempt)
                .await;
            member.set_disk_id_state(Some(id)).await.expect("restore registered identity");
            assert!(result.is_err(), "missing and nil IDs cannot establish a receiver snapshot");
        }
        store
            .clone()
            .seal_artifact_attempt_writes(BUCKET, incarnation, &attempt)
            .await
            .expect("restored complete topology confirms");
        shutdown.cancel();
    });
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_writes_seal_common_snapshot_rejects_arc_replacement_between_domains() {
    run_large_stack_async_test("artifact-writes-seal-common-snapshot", || async {
        let root = tempfile::tempdir().expect("shared snapshot root");
        let (_ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(root.path(), "artifact-writes-snapshot", &[4])).await;
        metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let incarnation = create_bucket(&store, BUCKET, true).await;
        let attempt = prefix();
        let members = disks(&store).await;
        let receiver_key = key(&store, incarnation, &attempt).await;
        let Disk::Local(local) = members[0].as_ref() else {
            panic!("local fixture");
        };
        let owner = local
            .acquire_artifact_put_lease(receiver_key)
            .await
            .expect("actual PUT admission blocks first domain");
        let replacement = Arc::new(Disk::Local(local.clone()));
        let worker_store = Arc::clone(&store);
        let worker_attempt = attempt.clone();
        let worker = tokio::spawn(async move {
            worker_store
                .seal_artifact_attempt_writes(BUCKET, incarnation, &worker_attempt)
                .await
        });
        wait_marker(&marker(&members[0], receiver_key, false)).await;
        store.pools[0].disk_set[0].disks.write().await[0] = Some(replacement);
        drop(owner);
        let result = tokio::time::timeout(DEADLINE, worker)
            .await
            .expect("captured domains drain")
            .expect("seal worker");
        store.pools[0].disk_set[0].disks.write().await[0] = Some(Arc::clone(&members[0]));
        let error = result.expect_err("same endpoint and UUID on another Arc cannot replace the captured member");
        assert!(error.to_string().contains("receiver membership changed"), "{error}");
        assert_domains_closed(&members, receiver_key).await;
        store
            .clone()
            .seal_artifact_attempt_writes(BUCKET, incarnation, &attempt)
            .await
            .expect("restored member exact retry confirms");
        shutdown.cancel();
    });
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_writes_seal_fresh_format_rejects_cache_nil_and_wrong_layout() {
    run_large_stack_async_test("artifact-writes-seal-fresh-format", || async {
        let root = tempfile::tempdir().expect("fresh format root");
        let (_ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(root.path(), "artifact-writes-format", &[4])).await;
        metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let incarnation = create_bucket(&store, BUCKET, true).await;
        let members = disks(&store).await;
        let disk = &members[0];
        let expected = disk.cached_disk_id().await.expect("registered disk identity");
        assert_eq!(disk.get_disk_id().await.expect("warm ordinary format cache"), Some(expected));
        let path = disk.path().join(".rustfs.sys/format.json");
        let original = tokio::fs::read(&path).await.expect("real physical disk format");
        let format: crate::disk::format::FormatV3 = serde_json::from_slice(&original).expect("original format");
        let attempt = prefix();
        let mut replaced = format.clone();
        replaced.erasure.this = Uuid::new_v4();
        replaced.erasure.sets[0][0] = replaced.erasure.this;
        let mut nil = format.clone();
        nil.erasure.this = Uuid::nil();
        let mut wrong_slot = format.clone();
        wrong_slot.erasure.this = wrong_slot.erasure.sets[0][1];
        for invalid in [replaced, nil, wrong_slot] {
            tokio::fs::write(&path, serde_json::to_vec(&invalid).expect("replacement format"))
                .await
                .expect("replace actual format without waiting for cache expiry");
            assert_eq!(
                disk.cached_disk_id().await,
                Some(expected),
                "registered cache intentionally stays unchanged"
            );
            let Disk::Local(local) = disk.as_ref() else {
                panic!("local fixture");
            };
            let physical = local.artifact_disk_id().await;
            let result = store
                .clone()
                .seal_artifact_attempt_writes(BUCKET, incarnation, &attempt)
                .await;
            let receiver = store
                .seal_local_artifact_puts(&disk.endpoint().to_string(), expected, BUCKET, incarnation, &attempt)
                .await;
            tokio::fs::write(&path, &original)
                .await
                .expect("restore original physical format");
            assert!(result.is_err(), "cached UUID is not a physical confirmation");
            assert!(physical.is_err() || physical.expect("fresh replacement identity") != expected);
            assert!(receiver.is_err(), "signed receiver path must independently read the real format");
        }
        store
            .clone()
            .seal_artifact_attempt_writes(BUCKET, incarnation, &attempt)
            .await
            .expect("original format retry confirms");
        shutdown.cancel();
    });
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_writes_seal_configured_root_replacement_cannot_confirm_old_descriptor() {
    run_large_stack_async_test("artifact-writes-seal-root", || async {
        let root = tempfile::tempdir().expect("configured root fixture");
        let (_ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(root.path(), "artifact-writes-root", &[4])).await;
        metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let incarnation = create_bucket(&store, BUCKET, true).await;
        let members = disks(&store).await;
        let disk = &members[0];
        let configured = disk.path();
        let parked = configured.with_extension("artifact-parked");
        let expected = disk.cached_disk_id().await.expect("registered disk identity");
        let attempt = prefix();
        tokio::fs::rename(&configured, &parked)
            .await
            .expect("detach original configured directory");
        tokio::fs::create_dir(&configured).await.expect("replacement configured root");
        let Disk::Local(local) = disk.as_ref() else {
            panic!("local fixture");
        };
        let physical = local.artifact_disk_id().await;
        let result = store
            .clone()
            .seal_artifact_attempt_writes(BUCKET, incarnation, &attempt)
            .await;
        let receiver = store
            .seal_local_artifact_puts(&disk.endpoint().to_string(), expected, BUCKET, incarnation, &attempt)
            .await;
        tokio::fs::remove_dir(&configured)
            .await
            .expect("remove only empty task-owned replacement");
        tokio::fs::rename(&parked, &configured)
            .await
            .expect("restore configured root before assertions");
        assert!(physical.is_err(), "old pinned descriptor cannot authenticate a replacement root");
        assert!(result.is_err(), "old configured Arc cannot establish joint confirmation");
        assert!(
            receiver.is_err(),
            "receiver cannot acknowledge the old descriptor as the current endpoint"
        );
        store
            .clone()
            .seal_artifact_attempt_writes(BUCKET, incarnation, &attempt)
            .await
            .expect("restored root retry confirms");
        shutdown.cancel();
    });
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_writes_seal_root_switch_drains_the_original_physical_owner() {
    run_large_stack_async_test("artifact-writes-seal-root-owner", || async {
        let root = tempfile::tempdir().expect("root switch fixture");
        let (_ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(root.path(), "artifact-writes-root-owner", &[4])).await;
        metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let incarnation = create_bucket(&store, BUCKET, true).await;
        let attempt = prefix();
        let members = disks(&store).await;
        let disk = &members[0];
        let Disk::Local(local) = disk.as_ref() else {
            panic!("local fixture");
        };
        let receiver_key = key(&store, incarnation, &attempt).await;
        let owner = local
            .acquire_artifact_put_lease(receiver_key)
            .await
            .expect("real original-root physical owner");
        let expected = disk.cached_disk_id().await.expect("registered original disk");
        assert_eq!(local.artifact_disk_id().await.expect("fresh original-root identity"), expected);
        let configured = disk.path();
        let parked = configured.with_extension("artifact-owner-parked");
        let original_marker = parked.join(
            marker(disk, receiver_key, false)
                .strip_prefix(&configured)
                .expect("marker under root"),
        );
        let replacement_marker = marker(disk, receiver_key, false);
        tokio::fs::rename(&configured, &parked).await.expect("park original root A");
        tokio::fs::create_dir(&configured)
            .await
            .expect("create task-owned replacement root B");
        let receiver = local.clone();
        let sealer = tokio::spawn(async move { receiver.seal_artifact_puts(receiver_key).await });
        tokio::time::timeout(DEADLINE, async {
            while !original_marker.is_file() && !replacement_marker.is_file() && !sealer.is_finished() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("sealer reaches either actual root, or returns");
        let original_closed = original_marker.is_file();
        let replacement_closed = replacement_marker.is_file();
        let finished_with_owner = sealer.is_finished();
        // Restore the endpoint before releasing A's owner and before assertions.
        tokio::fs::remove_dir_all(&configured)
            .await
            .expect("remove only task-owned replacement B");
        tokio::fs::rename(&parked, &configured)
            .await
            .expect("restore original root A");
        let restored_identity = local.artifact_disk_id().await;
        drop(owner);
        let result = tokio::time::timeout(DEADLINE, sealer)
            .await
            .expect("original physical owner drains")
            .expect("sealer task joins");
        assert!(original_closed, "ARTIFACT_WRITES_SEAL_ORIGINAL_ROOT_CLOSED");
        assert!(!replacement_closed, "replacement pathname must not redirect closure to B");
        assert!(!finished_with_owner, "A's physical owner must block seal confirmation");
        assert_eq!(restored_identity.expect("fresh restored identity"), expected);
        result.expect("seal completes after the original owner drops");
        assert!(
            local.acquire_artifact_put_lease(receiver_key).await.is_err(),
            "A stays permanently closed"
        );
        shutdown.cancel();
    });
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_backup_retention_preserves_exact_versions_and_reopen() {
    run_large_stack_async_test("artifact-backup-retention", || async {
        let root = tempfile::tempdir().expect("retention root");
        let (ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(root.path(), "artifact-backup-retention", &[4])).await;
        metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let incarnation = create_bucket(&store, BUCKET, true).await;
        let attempt = prefix();
        let object = format!("{attempt}image.png");
        let version = put(&store, BUCKET, &object)
            .await
            .expect("actual version")
            .version_id
            .expect("versioned");
        assert!(
            !store
                .inspect_artifact_backup_retention(BUCKET, incarnation, "outputs/")
                .await
                .expect("default off")
        );
        for (expected, base) in [(Uuid::new_v4(), "outputs/"), (incarnation, "wrong/")] {
            assert!(
                store.clone().retain_artifact_backups(BUCKET, expected, base).await.is_err(),
                "O02P3_IDENTITY_BOUND"
            );
        }
        store
            .clone()
            .retain_artifact_backups(BUCKET, incarnation, "outputs/")
            .await
            .expect("durable retention");
        store
            .clone()
            .retain_artifact_backups(BUCKET, incarnation, "outputs/")
            .await
            .expect("idempotent retry");
        store
            .clone()
            .seal_artifact_attempt_writes(BUCKET, incarnation, &attempt)
            .await
            .expect("seal actual version");
        assert!(
            store
                .clone()
                .delete_artifact_object_version(BUCKET, incarnation, &attempt, &object, version)
                .await
                .is_err(),
            "O02P3_RETAINED_DELETE_DENIED"
        );
        let exact = ObjectOptions {
            version_id: Some(version.to_string()),
            suppress_read_repair: true,
            ..Default::default()
        };
        let mut reader = store
            .get_object_reader(BUCKET, &object, None, Default::default(), &exact)
            .await
            .expect("O02P3_EXACT_VERSION_SURVIVES");
        let mut bytes = Vec::new();
        reader.stream.read_to_end(&mut bytes).await.expect("actual bytes");
        assert_eq!(bytes, vec![0xA5; 1024 * 1024], "O02P3_EXACT_BYTES_SURVIVE");
        let (mut metadata, _) = crate::bucket::metadata::load_bucket_metadata_parse_with_presence(store.clone(), BUCKET, true)
            .await
            .expect("persisted marker");
        metadata.artifact_backup_retained = false;
        assert!(metadata.save_with_store(store.clone()).await.is_err(), "O02P3_CANNOT_REMOVE_PROTECTION");
        for force in [false, true] {
            assert!(
                store
                    .delete_bucket(
                        BUCKET,
                        &crate::storage_api_contracts::bucket::DeleteBucketOptions {
                            force,
                            ..Default::default()
                        }
                    )
                    .await
                    .is_err(),
                "O02P3_BUCKET_DELETE_DENIED"
            );
        }
        shutdown.cancel();
        assert!(ctx.wait_for_detached_mutations(DEADLINE).await);
        drop(reader);
        drop(store);
        drop(ctx);
        let (_ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(root.path(), "artifact-backup-retention-reopen", &[4])).await;
        metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        assert!(
            store
                .inspect_artifact_backup_retention(BUCKET, incarnation, "outputs/")
                .await
                .expect("O02P3_REOPEN_PROTECTED")
        );
        assert!(
            store
                .clone()
                .delete_artifact_object_version(BUCKET, incarnation, &attempt, &object, version)
                .await
                .is_err(),
            "O02P3_REOPEN_DELETE_DENIED"
        );
        assert!(
            store.get_object_info(BUCKET, &object, &exact).await.is_ok(),
            "O02P3_REOPEN_VERSION_SURVIVES"
        );
        shutdown.cancel();
    });
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_backup_retention_cancelled_waiter_keeps_owned_fence() {
    run_large_stack_async_test("artifact-backup-retention-owner", || async {
        let root = tempfile::tempdir().expect("retention owner root");
        let (ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(root.path(), "artifact-backup-retention-owner", &[4])).await;
        metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let incarnation = create_bucket(&store, BUCKET, true).await;
        let earlier_delete = store
            .acquire_bucket_lifecycle_read_lock(BUCKET)
            .await
            .expect("prior delete lifecycle owner");
        let worker_store = store.clone();
        let mut waiter = tokio::spawn(async move { worker_store.retain_artifact_backups(BUCKET, incarnation, "outputs/").await });
        tokio::time::timeout(DEADLINE, async {
            while ctx.detached_mutation_count() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("retention owner registered");
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut waiter).await.is_err(),
            "O02P3_PRIOR_DELETE_DRAINED"
        );
        waiter.abort();
        assert!(waiter.await.expect_err("caller cancelled").is_cancelled());
        drop(earlier_delete);
        assert!(ctx.wait_for_detached_mutations(DEADLINE).await, "O02P3_CANCELLED_OWNER_DRAINS");
        assert!(
            store
                .inspect_artifact_backup_retention(BUCKET, incarnation, "outputs/")
                .await
                .expect("protection published after cancellation"),
            "O02P3_CANCELLED_RETENTION_PERSISTS"
        );
        assert!(
            store
                .delete_bucket(
                    BUCKET,
                    &crate::storage_api_contracts::bucket::DeleteBucketOptions {
                        force: true,
                        ..Default::default()
                    }
                )
                .await
                .is_err(),
            "O02P3_EMPTY_BUCKET_RETAINED"
        );
        shutdown.cancel();
    });
}
