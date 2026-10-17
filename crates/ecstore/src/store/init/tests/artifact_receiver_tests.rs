use super::{build_isolated_test_store, run_large_stack_async_test, without_storage_class_env};
use crate::{
    bucket::metadata_sys,
    disk::{
        DiskAPI as _, DiskStore, RUSTFS_META_TMP_BUCKET, ReadOptions,
        os::{LocalPublicationPause, LocalPublicationStage, prepared_publication_test_hooks},
    },
    storage_api_contracts::bucket::{BucketOperations as _, MakeBucketOptions},
    store::ECStore,
};
use bytes::Bytes;
use futures::FutureExt;
use rustfs_filemeta::{FileInfo, ObjectPartInfo};
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

const DEADLINE: Duration = Duration::from_secs(30);
const BUCKET: &str = "artifact-receiver-tests";
const BODY: &[u8] = b"real receiver inline publication";

fn prefix() -> String {
    format!("outputs/74656e616e74/{}/{}/", Uuid::new_v4(), Uuid::new_v4())
}

async fn bucket(store: &ECStore, name: &str, managed: bool) -> Uuid {
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
        .expect("persist real bucket admission metadata");
    store.bucket_incarnation_id(name).await.expect("current bucket incarnation")
}

async fn disk(store: &ECStore) -> (DiskStore, String, Uuid) {
    let disk = store.pools[0].disk_set[0]
        .disk_inventory()
        .await
        .into_iter()
        .next()
        .flatten()
        .expect("online physical fixture disk");
    let reference = disk.endpoint().to_string();
    let id = disk
        .get_disk_id()
        .await
        .expect("real disk format")
        .expect("formatted fixture disk");
    (disk, reference, id)
}

async fn stage(disk: &DiskStore, source: &str, object: &str) -> FileInfo {
    let mut fi = FileInfo::new(object, 1, 0);
    fi.erasure.index = 1;
    fi.version_id = Some(Uuid::new_v4());
    fi.mod_time = Some(time::OffsetDateTime::now_utc());
    fi.size = i64::try_from(BODY.len()).expect("fixture body size");
    fi.parts = vec![ObjectPartInfo {
        number: 1,
        size: BODY.len(),
        actual_size: fi.size,
        ..Default::default()
    }];
    fi.data = Some(Bytes::from_static(BODY));
    fi.set_inline_data();
    fi.validate_for_metadata_read().expect("real valid inline staging metadata");
    disk.write_metadata("", RUSTFS_META_TMP_BUCKET, source, fi.clone())
        .await
        .expect("stage real receiver metadata");
    fi
}

async fn assert_version(disk: &DiskStore, object: &str, fi: &FileInfo) {
    let version = fi.version_id.expect("versioned fixture").to_string();
    let actual = disk
        .read_version(
            BUCKET,
            BUCKET,
            object,
            &version,
            &ReadOptions {
                read_data: true,
                ..Default::default()
            },
        )
        .await
        .expect("read actual receiver publication");
    assert_eq!(actual.version_id, fi.version_id, "ARTIFACT_RECEIVER_EXACT_VERSION");
    assert_eq!(actual.data, Some(Bytes::from_static(BODY)), "ARTIFACT_RECEIVER_PHYSICAL_BODY");
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_receiver_binds_managed_incarnation_and_disk() {
    run_large_stack_async_test("artifact-receiver-admission", || async {
        let root = tempfile::tempdir().expect("receiver admission root");
        let (_ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(root.path(), "artifact-receiver-admission", &[4])).await;
        metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let incarnation = bucket(&store, BUCKET, true).await;
        bucket(&store, "ordinary-receiver-tests", false).await;
        assert!(
            store.reject_legacy_artifact_mutation(BUCKET).await.is_err(),
            "ARTIFACT_RECEIVER_LEGACY_REJECTED"
        );
        store
            .reject_legacy_artifact_mutation("ordinary-receiver-tests")
            .await
            .expect("ordinary bucket keeps legacy classification");
        let (disk, reference, id) = disk(&store).await;
        let attempt = prefix();
        let object = format!("{attempt}image.png");
        let fi = stage(&disk, "receiver-admission", &object).await;
        assert!(
            store
                .rename_local_artifact_data(
                    &reference,
                    (RUSTFS_META_TMP_BUCKET, "receiver-admission"),
                    &fi,
                    (BUCKET, &object),
                    Uuid::new_v4()
                )
                .await
                .is_err(),
            "ARTIFACT_RECEIVER_INCARNATION_BOUND"
        );
        assert!(
            store
                .rename_local_artifact_data(
                    &Uuid::new_v4().to_string(),
                    (RUSTFS_META_TMP_BUCKET, "receiver-admission"),
                    &fi,
                    (BUCKET, &object),
                    incarnation
                )
                .await
                .is_err(),
            "ARTIFACT_RECEIVER_DISK_BOUND"
        );
        assert!(
            store
                .seal_local_artifact_puts(&reference, Uuid::new_v4(), BUCKET, incarnation, &attempt)
                .await
                .is_err(),
            "ARTIFACT_RECEIVER_SEAL_DISK_BOUND"
        );
        assert!(
            store
                .seal_local_artifact_puts(&reference, id, BUCKET, Uuid::new_v4(), &attempt)
                .await
                .is_err(),
            "ARTIFACT_RECEIVER_SEAL_INCARNATION_BOUND"
        );
        assert!(
            disk.read_version(BUCKET, BUCKET, &object, "", &ReadOptions::default())
                .await
                .is_err(),
            "ARTIFACT_RECEIVER_REJECTIONS_DO_NOT_PUBLISH"
        );
        store
            .rename_local_artifact_data(
                &reference,
                (RUSTFS_META_TMP_BUCKET, "receiver-admission"),
                &fi,
                (BUCKET, &object),
                incarnation,
            )
            .await
            .expect("unsealed real managed receiver publication");
        assert_version(&disk, &object, &fi).await;
        shutdown.cancel();
    });
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_receiver_durable_seal_rejects_late_publication() {
    run_large_stack_async_test("artifact-receiver-closed", || async {
        let root = tempfile::tempdir().expect("receiver close root");
        let (_ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(root.path(), "artifact-receiver-closed", &[4])).await;
        metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let incarnation = bucket(&store, BUCKET, true).await;
        let (disk, reference, id) = disk(&store).await;
        let attempt = prefix();
        let object = format!("{attempt}image.png");
        let fi = stage(&disk, "receiver-first", &object).await;
        store
            .rename_local_artifact_data(
                &reference,
                (RUSTFS_META_TMP_BUCKET, "receiver-first"),
                &fi,
                (BUCKET, &object),
                incarnation,
            )
            .await
            .expect("initial real receiver publication");
        let before = disk
            .read_xl(BUCKET, &object, false)
            .await
            .expect("published physical metadata")
            .buf;
        assert_eq!(
            store
                .seal_local_artifact_puts(&reference, id, BUCKET, incarnation, &attempt)
                .await
                .expect("durable receiver seal"),
            id
        );
        assert_eq!(
            store
                .seal_local_artifact_puts(&reference, id, BUCKET, incarnation, &attempt)
                .await
                .expect("independent seal reopen"),
            id
        );
        let late = stage(&disk, "receiver-late", &object).await;
        assert!(
            store
                .rename_local_artifact_data(
                    &reference,
                    (RUSTFS_META_TMP_BUCKET, "receiver-late"),
                    &late,
                    (BUCKET, &object),
                    incarnation
                )
                .await
                .is_err(),
            "ARTIFACT_RECEIVER_LATE_RENAME_REJECTED"
        );
        assert_eq!(
            disk.read_xl(BUCKET, &object, false)
                .await
                .expect("original physical metadata retained")
                .buf,
            before,
            "ARTIFACT_RECEIVER_SEAL_PRESERVES_VERSIONS"
        );
        assert_version(&disk, &object, &fi).await;
        shutdown.cancel();
    });
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_receiver_cancelled_waiter_cannot_confirm_physical_tail() {
    run_large_stack_async_test("artifact-receiver-physical-tail", physical_tail_case);
}

async fn physical_tail_case() {
    let root = tempfile::tempdir().expect("receiver physical-tail root");
    let (ctx, store, shutdown) =
        without_storage_class_env(build_isolated_test_store(root.path(), "artifact-receiver-physical-tail", &[4])).await;
    metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
    let incarnation = bucket(&store, BUCKET, true).await;
    let (disk, reference, id) = disk(&store).await;
    let attempt = prefix();
    let object = format!("{attempt}image.png");
    let late_object = format!("{attempt}late.png");
    let fi = stage(&disk, "receiver-paused", &object).await;
    let late = stage(&disk, "receiver-paused-late", &late_object).await;
    let physical_object = disk
        .get_object_path_for_io_if_local(BUCKET, &object)
        .expect("local receiver disk")
        .expect("physical object path");
    let mut pause =
        LocalPublicationPause::install(&disk, BUCKET, &format!("{object}/xl.meta"), LocalPublicationStage::PreparedRename)
            .expect("pause actual receiver physical publication");
    let owner = Arc::clone(&store);
    let rename_reference = reference.clone();
    let rename_object = object.clone();
    let rename_fi = fi.clone();
    let mut rename = tokio::spawn(async move {
        owner
            .rename_local_artifact_data(
                &rename_reference,
                (RUSTFS_META_TMP_BUCKET, "receiver-paused"),
                &rename_fi,
                (BUCKET, &rename_object),
                incarnation,
            )
            .await
    });
    let mut rename_joined = false;
    let mut sealer = None;
    let mut early_seal = None;
    let observed = std::panic::AssertUnwindSafe(async {
        tokio::time::timeout(DEADLINE, async {
            tokio::select! {
                result = &mut rename => { rename_joined = true; panic!("receiver returned before physical entry: {result:?}"); }
                entered = pause.entered() => entered.expect("physical receiver rename entered"),
            }
        })
        .await
        .expect("bounded receiver physical entry");
        rename.abort();
        let cancelled = (&mut rename).await.expect_err("cancel only the receiver waiter");
        rename_joined = true;
        assert!(cancelled.is_cancelled());
        let owner = Arc::clone(&store);
        let seal_reference = reference.clone();
        let seal_attempt = attempt.clone();
        sealer = Some(tokio::spawn(async move {
            owner
                .seal_local_artifact_puts(&seal_reference, id, BUCKET, incarnation, &seal_attempt)
                .await
        }));
        let identity = store
            .artifact_receiver_identity(BUCKET, incarnation, &attempt, true)
            .await
            .expect("authoritative receiver key");
        let marker = disk
            .path()
            .join(".rustfs-artifact-put-seals")
            .join(hex_simd::encode_to_string(identity.receiver_key, hex_simd::AsciiCase::Lower))
            .join("closed");
        tokio::time::timeout(DEADLINE, async {
            while !marker.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("real receiver close admission point");
        if let Ok(result) = tokio::time::timeout(Duration::from_millis(100), sealer.as_mut().expect("sealer launched")).await {
            early_seal = Some(result);
            panic!("ARTIFACT_RECEIVER_CANCELLED_TAIL_BARRIER: close returned before the physical owner drained");
        }
        assert!(
            store
                .rename_local_artifact_data(
                    &reference,
                    (RUSTFS_META_TMP_BUCKET, "receiver-paused-late"),
                    &late,
                    (BUCKET, &late_object),
                    incarnation
                )
                .await
                .is_err(),
            "ARTIFACT_RECEIVER_CLOSE_REJECTS_NEW_WRITER"
        );
    })
    .catch_unwind()
    .await;
    drop(pause);
    if !rename_joined {
        rename.abort();
        let _ = (&mut rename).await;
    }
    let physical_drained =
        tokio::time::timeout(DEADLINE, prepared_publication_test_hooks::drain_namespace_key(&physical_object)).await;
    let seal_result = match (early_seal, sealer) {
        (Some(result), _) => Some(Ok(result)),
        (None, Some(sealer)) => Some(tokio::time::timeout(DEADLINE, sealer).await),
        (None, None) => None,
    };
    let owners_drained = tokio::time::timeout(DEADLINE, async {
        while ctx.namespace_commits_pending() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    shutdown.cancel();
    if physical_drained.is_err() || owners_drained.is_err() || seal_result.as_ref().is_some_and(Result::is_err) {
        let retained = root.keep();
        panic!("ARTIFACT_RECEIVER_PHYSICAL_DRAIN: retained={retained:?}");
    }
    if let Err(panic) = observed {
        std::panic::resume_unwind(panic);
    }
    assert_eq!(
        seal_result
            .expect("sealer ran")
            .expect("physical seal completion bounded")
            .expect("sealer task")
            .expect("physical tail drained"),
        id,
        "ARTIFACT_RECEIVER_PHYSICAL_COMPLETION"
    );
    assert_version(&disk, &object, &fi).await;
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_legacy_classification_does_not_migrate_or_reenter_namespace() {
    run_large_stack_async_test("artifact-legacy-classification", || async {
        use crate::bucket::metadata::{BucketMetadata, load_bucket_incarnation};
        use crate::storage_api_contracts::namespace::NamespaceLocking as _;
        let root = tempfile::tempdir().expect("legacy receiver root");
        let (ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(root.path(), "artifact-legacy-classification", &[4])).await;
        metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let bucket = "legacy-receiver-classification";
        for disk in store.pools[0].disk_set[0].disk_inventory().await.into_iter().flatten() {
            disk.make_volume(bucket).await.expect("legacy volume");
        }
        let sys = metadata_sys::require_bucket_metadata_sys_in(&ctx).expect("metadata authority");
        let mut metadata = BucketMetadata::new(bucket);
        metadata.bucket_incarnation_id = Uuid::nil();
        sys.read()
            .await
            .persist_and_set(metadata)
            .await
            .expect("persist old metadata without identity");
        let lock = store.new_ns_lock(bucket, bucket).await.expect("deletion namespace lock");
        let _deleting = lock.get_write_lock(DEADLINE).await.expect("caller owns deletion lock");
        tokio::time::timeout(Duration::from_secs(2), store.reject_legacy_artifact_mutation(bucket))
            .await
            .expect("ARTIFACT_LEGACY_CLASSIFICATION_NO_REENTRANT_LOCK")
            .expect("persisted ordinary legacy metadata permits classification");
        assert!(
            load_bucket_incarnation(store.clone(), bucket)
                .await
                .expect("sidecar read")
                .is_none(),
            "ARTIFACT_LEGACY_CLASSIFICATION_NO_MIGRATION"
        );
        shutdown.cancel();
    });
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_receiver_disk_lookup_stays_on_captured_instance() {
    run_large_stack_async_test("artifact-receiver-instance", || async {
        let first_root = tempfile::tempdir().expect("first instance root");
        let second_root = tempfile::tempdir().expect("second instance root");
        let (first_ctx, first, first_shutdown) =
            without_storage_class_env(build_isolated_test_store(first_root.path(), "artifact-first-instance", &[4])).await;
        let (_, second, second_shutdown) =
            without_storage_class_env(build_isolated_test_store(second_root.path(), "artifact-second-instance", &[4])).await;
        let (first_disk, first_ref, _) = disk(&first).await;
        let (_, second_ref, _) = disk(&second).await;
        assert!(Arc::ptr_eq(&first.local_mutation_disk(&first_ref).await.expect("own disk"), &first_disk));
        assert!(first.local_mutation_disk(&second_ref).await.is_err(), "ARTIFACT_RECEIVER_NO_AMBIENT_DISK");
        let bootstrap = crate::store::BootstrapLocalTarget::new(first_ctx);
        assert!(Arc::ptr_eq(
            &bootstrap.local_mutation_disk(&first_ref).await.expect("own bootstrap disk"),
            &first_disk
        ));
        assert!(
            bootstrap.local_mutation_disk(&second_ref).await.is_err(),
            "ARTIFACT_BOOTSTRAP_NO_AMBIENT_DISK"
        );
        first_shutdown.cancel();
        second_shutdown.cancel();
    });
}
