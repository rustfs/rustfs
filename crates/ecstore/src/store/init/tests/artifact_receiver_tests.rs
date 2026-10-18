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
    fi.name = object.to_owned();
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

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_repair_receiver_rejects_wrong_disk_incarnation_version_and_legacy_domain() {
    run_large_stack_async_test("artifact-repair-receiver-identity", || async {
        let root = tempfile::tempdir().expect("repair receiver identity root");
        let (_ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(root.path(), "artifact-repair-receiver-identity", &[4])).await;
        metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let incarnation = bucket(&store, BUCKET, true).await;
        let ordinary = bucket(&store, "ordinary-repair-receiver", false).await;
        let (disk, reference, id) = disk(&store).await;
        let attempt = prefix();
        let object = format!("{attempt}image.png");
        let fi = stage(&disk, "repair-identity", &object).await;
        let mut invalid = Vec::new();
        invalid.push((fi.clone(), BUCKET, object.clone(), Uuid::nil(), incarnation));
        invalid.push((fi.clone(), BUCKET, object.clone(), Uuid::new_v4(), incarnation));
        invalid.push((fi.clone(), BUCKET, object.clone(), id, Uuid::nil()));
        invalid.push((fi.clone(), BUCKET, object.clone(), id, Uuid::new_v4()));
        invalid.push((fi.clone(), "ordinary-repair-receiver", object.clone(), id, ordinary));
        invalid.push((fi.clone(), BUCKET, format!("{attempt}../alias.png"), id, incarnation));
        invalid.push((fi.clone(), BUCKET, format!("/{object}"), id, incarnation));
        for version in [None, Some(Uuid::nil())] {
            let mut candidate = fi.clone();
            candidate.version_id = version;
            invalid.push((candidate, BUCKET, object.clone(), id, incarnation));
        }
        let mut candidate = fi.clone();
        candidate.name = format!("{attempt}different.png");
        invalid.push((candidate, BUCKET, object.clone(), id, incarnation));
        let mut candidate = fi.clone();
        candidate.volume = "different-bucket".to_owned();
        invalid.push((candidate, BUCKET, object.clone(), id, incarnation));
        for (candidate, volume, path, expected_disk, expected_incarnation) in invalid {
            assert!(
                store
                    .rename_local_artifact_repair_data(
                        &reference,
                        expected_disk,
                        (RUSTFS_META_TMP_BUCKET, "repair-identity"),
                        &candidate,
                        (volume, &path),
                        expected_incarnation
                    )
                    .await
                    .is_err(),
                "ARTIFACT_REPAIR_RECEIVER_RENAME_IDENTITY"
            );
            assert!(
                store
                    .write_local_artifact_repair_metadata(
                        &reference,
                        expected_disk,
                        (volume, &path),
                        candidate,
                        expected_incarnation
                    )
                    .await
                    .is_err(),
                "ARTIFACT_REPAIR_RECEIVER_METADATA_IDENTITY"
            );
        }
        assert!(
            store.reject_legacy_artifact_mutation(BUCKET).await.is_err(),
            "ARTIFACT_REPAIR_RECEIVER_NO_LEGACY_BYPASS"
        );
        assert!(
            store
                .rename_local_artifact_repair_data(
                    &reference,
                    id,
                    (BUCKET, "repair-identity"),
                    &fi,
                    (BUCKET, &object),
                    incarnation
                )
                .await
                .is_err(),
            "ARTIFACT_REPAIR_RECEIVER_INTERNAL_STAGING_ONLY"
        );
        assert!(
            disk.read_version(BUCKET, BUCKET, &object, "", &ReadOptions::default())
                .await
                .is_err(),
            "ARTIFACT_REPAIR_RECEIVER_REJECTION_HAS_NO_PUBLICATION"
        );
        store
            .seal_local_artifact_puts(&reference, id, BUCKET, incarnation, &attempt)
            .await
            .expect("PUT domain closes");
        store
            .rename_local_artifact_repair_data(
                &reference,
                id,
                (RUSTFS_META_TMP_BUCKET, "repair-identity"),
                &fi,
                (BUCKET, &object),
                incarnation,
            )
            .await
            .expect("existing-version repair permits PUT closure");
        store
            .write_local_artifact_repair_metadata(&reference, id, (BUCKET, &object), fi.clone(), incarnation)
            .await
            .expect("metadata repair permits PUT closure");
        assert_version(&disk, &object, &fi).await;
        assert_eq!(
            store
                .seal_local_artifact_repairs(&reference, id, BUCKET, incarnation, &attempt)
                .await
                .expect("receiver repair close"),
            id
        );
        assert_eq!(
            store
                .seal_local_artifact_repairs(&reference, id, BUCKET, incarnation, &attempt)
                .await
                .expect("repair close survives independent reopen"),
            id
        );
        assert!(
            store
                .write_local_artifact_repair_metadata(&reference, id, (BUCKET, &object), fi.clone(), incarnation)
                .await
                .is_err(),
            "ARTIFACT_REPAIR_RECEIVER_METADATA_CLOSED"
        );
        assert!(
            store
                .rename_local_artifact_repair_data(
                    &reference,
                    id,
                    (RUSTFS_META_TMP_BUCKET, "repair-identity"),
                    &fi,
                    (BUCKET, &object),
                    incarnation
                )
                .await
                .is_err(),
            "ARTIFACT_REPAIR_RECEIVER_RENAME_CLOSED"
        );
        let other_object = format!("{}other.png", prefix());
        let other = stage(&disk, "repair-other-attempt", &other_object).await;
        store
            .rename_local_artifact_repair_data(
                &reference,
                id,
                (RUSTFS_META_TMP_BUCKET, "repair-other-attempt"),
                &other,
                (BUCKET, &other_object),
                incarnation,
            )
            .await
            .expect("other attempt remains repairable");
        shutdown.cancel();
    });
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_repair_receiver_rejects_authoritative_retirement_and_repair_seal() {
    run_large_stack_async_test("artifact-repair-receiver-records", || async {
        let root = tempfile::tempdir().expect("repair receiver records root");
        let (_ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(root.path(), "artifact-repair-receiver-records", &[4])).await;
        metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let incarnation = bucket(&store, BUCKET, true).await;
        let (disk, reference, id) = disk(&store).await;
        let attempt = prefix();
        let object = format!("{attempt}image.png");
        let fi = stage(&disk, "repair-retired", &object).await;
        store
            .clone()
            .seal_artifact_attempt_puts(BUCKET, incarnation, &attempt)
            .await
            .expect("seal logical PUT attempt");
        store
            .clone()
            .retire_artifact_object_version(
                BUCKET,
                incarnation,
                &attempt,
                &object,
                fi.version_id.expect("exact retired fixture version"),
            )
            .await
            .expect("persist exact retirement");
        assert!(
            store
                .rename_local_artifact_repair_data(
                    &reference,
                    id,
                    (RUSTFS_META_TMP_BUCKET, "repair-retired"),
                    &fi,
                    (BUCKET, &object),
                    incarnation
                )
                .await
                .is_err(),
            "ARTIFACT_REPAIR_RECEIVER_RETIRED_RENAME"
        );
        assert!(
            store
                .write_local_artifact_repair_metadata(&reference, id, (BUCKET, &object), fi.clone(), incarnation)
                .await
                .is_err(),
            "ARTIFACT_REPAIR_RECEIVER_RETIRED_METADATA"
        );
        let another = stage(&disk, "repair-non-retired", &object).await;
        store
            .write_local_artifact_repair_metadata(&reference, id, (BUCKET, &object), another.clone(), incarnation)
            .await
            .expect("another exact version is not retired");
        store
            .clone()
            .seal_artifact_attempt_repairs(BUCKET, incarnation, &attempt)
            .await
            .expect("persist repair seal");
        assert!(
            store
                .write_local_artifact_repair_metadata(&reference, id, (BUCKET, &object), another, incarnation)
                .await
                .is_err(),
            "ARTIFACT_REPAIR_RECEIVER_AUTHORITATIVE_REPAIR_SEAL"
        );
        shutdown.cancel();
    });
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_repair_receiver_rename_owner_survives_rpc_cancellation() {
    run_repair_physical_tail_case(false, false);
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_repair_receiver_metadata_owner_survives_rpc_timeout() {
    run_repair_physical_tail_case(true, false);
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_repair_receiver_retirement_waits_for_cancelled_rename() {
    run_repair_physical_tail_case(false, true);
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_repair_receiver_retirement_waits_for_metadata_timeout() {
    run_repair_physical_tail_case(true, true);
}

fn run_repair_physical_tail_case(metadata: bool, retirement: bool) {
    // Pausing deadlines requires a current-thread runtime; blocking disk workers
    // still execute independently and retain their physical owners.
    std::thread::Builder::new()
        .name(format!("repair-receiver-{metadata}-{retirement}"))
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("receiver timeout runtime")
                .block_on(temp_env::async_with_vars(
                    [(rustfs_config::ENV_DRIVE_MAX_TIMEOUT_DURATION, Some("60"))],
                    repair_physical_tail_case(metadata, retirement),
                ));
        })
        .expect("receiver physical-tail thread")
        .join()
        .expect("receiver physical-tail test completes");
}

async fn repair_physical_tail_case(metadata: bool, retirement: bool) {
    let root = tempfile::tempdir().expect("repair receiver physical-tail root");
    let (ctx, store, shutdown) =
        without_storage_class_env(build_isolated_test_store(root.path(), "artifact-repair-receiver-tail", &[4])).await;
    metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
    let incarnation = bucket(&store, BUCKET, true).await;
    let (disk, reference, id) = disk(&store).await;
    let attempt = prefix();
    let object = format!("{attempt}image.png");
    let fi = stage(&disk, "repair-receiver-paused", &object).await;
    store
        .seal_local_artifact_puts(&reference, id, BUCKET, incarnation, &attempt)
        .await
        .expect("close PUT domain first");
    if retirement {
        store
            .clone()
            .seal_artifact_attempt_puts(BUCKET, incarnation, &attempt)
            .await
            .expect("retirement requires global PUT seal");
    }
    let physical_object = disk
        .get_object_path_for_io_if_local(BUCKET, &object)
        .expect("local receiver")
        .expect("physical object path");
    let stage = if metadata {
        LocalPublicationStage::Rename
    } else {
        LocalPublicationStage::PreparedRename
    };
    let mut pause = LocalPublicationPause::install(&disk, BUCKET, &format!("{object}/xl.meta"), stage)
        .expect("pause actual repair publication");
    let owner = Arc::clone(&store);
    let mutation_reference = reference.clone();
    let mutation_object = object.clone();
    let mutation_fi = fi.clone();
    let mut mutation = tokio::spawn(async move {
        if metadata {
            owner
                .write_local_artifact_repair_metadata(
                    &mutation_reference,
                    id,
                    (BUCKET, &mutation_object),
                    mutation_fi,
                    incarnation,
                )
                .await
        } else {
            owner
                .rename_local_artifact_repair_data(
                    &mutation_reference,
                    id,
                    (RUSTFS_META_TMP_BUCKET, "repair-receiver-paused"),
                    &mutation_fi,
                    (BUCKET, &mutation_object),
                    incarnation,
                )
                .await
                .map(|_| ())
        }
    });
    let mut joined = false;
    let mut sealer = None;
    let mut early_seal = None;
    let observed = std::panic::AssertUnwindSafe(async {
        tokio::time::timeout(DEADLINE, async {
            tokio::select! {
                result = &mut mutation => { joined = true; panic!("repair returned before physical entry: {result:?}"); }
                entered = pause.entered() => entered.expect("physical repair worker entered"),
            }
        })
        .await
        .expect("bounded physical repair entry");
        if metadata {
            let before = disk.metrics_snapshot().expect("disk metrics").total_errors_timeout;
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(61)).await;
            tokio::time::resume();
            let result = tokio::time::timeout(DEADLINE, &mut mutation)
                .await
                .expect("real wrapper timeout returns")
                .expect("receiver task");
            joined = true;
            assert!(
                matches!(result, Err(crate::disk::error::DiskError::Timeout)),
                "real metadata wrapper must time out"
            );
            assert!(
                disk.metrics_snapshot().expect("timeout metrics").total_errors_timeout > before,
                "ARTIFACT_REPAIR_RECEIVER_REAL_METADATA_TIMEOUT"
            );
        } else {
            mutation.abort();
            assert!((&mut mutation).await.expect_err("cancel only receiver waiter").is_cancelled());
            joined = true;
        }
        let owner = Arc::clone(&store);
        let seal_reference = reference.clone();
        let seal_attempt = attempt.clone();
        let retire_object = object.clone();
        let version = fi.version_id.expect("exact repair version");
        sealer = Some(tokio::spawn(async move {
            if retirement {
                return owner
                    .retire_artifact_object_version(BUCKET, incarnation, &seal_attempt, &retire_object, version)
                    .await
                    .map(|()| id)
                    .map_err(crate::disk::error::DiskError::other);
            }
            owner
                .seal_local_artifact_repairs(&seal_reference, id, BUCKET, incarnation, &seal_attempt)
                .await
        }));
        if !retirement {
            let identity = store
                .artifact_receiver_identity(BUCKET, incarnation, &attempt, true)
                .await
                .expect("repair receiver identity");
            let marker = disk
                .path()
                .join(".rustfs-artifact-repair-seals")
                .join(hex_simd::encode_to_string(identity.receiver_key, hex_simd::AsciiCase::Lower))
                .join("closed");
            tokio::time::timeout(DEADLINE, async {
                while !marker.exists() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("receiver repair admission durably closes");
        }
        if let Ok(result) = tokio::time::timeout(Duration::from_millis(500), sealer.as_mut().expect("sealer launched")).await {
            early_seal = Some(result);
            panic!("ARTIFACT_REPAIR_RECEIVER_PHYSICAL_TAIL_BARRIER: seal returned before physical completion");
        }
    })
    .catch_unwind()
    .await;
    drop(pause);
    if !joined {
        mutation.abort();
        let _ = (&mut mutation).await;
    }
    let drained = tokio::time::timeout(DEADLINE, prepared_publication_test_hooks::drain_namespace_key(&physical_object)).await;
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
    if drained.is_err() || owners_drained.is_err() || seal_result.as_ref().is_some_and(Result::is_err) {
        let retained = root.keep();
        panic!("ARTIFACT_REPAIR_RECEIVER_DRAIN_FAILED: retained={retained:?}");
    }
    if let Err(panic) = observed {
        std::panic::resume_unwind(panic);
    }
    assert_eq!(
        seal_result
            .expect("sealer ran")
            .expect("bounded seal completion")
            .expect("sealer task")
            .expect("physical repair drained"),
        id
    );
    assert_version(&disk, &object, &fi).await;
    assert!(
        store
            .write_local_artifact_repair_metadata(&reference, id, (BUCKET, &object), fi.clone(), incarnation)
            .await
            .is_err(),
        "ARTIFACT_REPAIR_RECEIVER_CLOSING_REJECTS_ADMISSION"
    );
    shutdown.cancel();
}
