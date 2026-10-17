#![cfg(unix)]

use super::{build_isolated_test_store, run_large_stack_async_test, without_storage_class_env};
use crate::{
    bucket::{artifact::artifact_version_retirement_record_path, metadata_sys},
    config::com,
    disk::{DiskAPI as _, DiskStore, RUSTFS_META_BUCKET, ReadOptions, STORAGE_FORMAT_FILE, os},
    error::Error,
    object_api::{ObjectOptions, PutObjReader, ShardIntegrityWriteMode, WriteCompletion},
    runtime::instance::InstanceContext,
    storage_api_contracts::{
        bucket::{BucketOperations as _, MakeBucketOptions},
        heal::HealOperations,
        object::ObjectIO as _,
    },
    store::ECStore,
};
use rustfs_heal_contracts::heal_channel::{HealOpts, HealScanMode};
use rustfs_utils::path::encode_dir_object;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::io::AsyncReadExt as _;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const DEADLINE: Duration = Duration::from_secs(30);
const PAYLOAD_SIZE: usize = 1024 * 1024;

fn attempt_prefix() -> String {
    format!("outputs/74656e616e74/{}/{}/", Uuid::new_v4(), Uuid::new_v4())
}

fn put_options() -> ObjectOptions {
    ObjectOptions {
        versioned: true,
        write_completion: WriteCompletion::TailDrained,
        ..Default::default()
    }
}

fn heal_options() -> HealOpts {
    HealOpts {
        scan_mode: HealScanMode::Deep,
        ..Default::default()
    }
}

async fn open_store(root: &std::path::Path) -> (Arc<InstanceContext>, Arc<ECStore>, CancellationToken) {
    let (ctx, store, shutdown) = without_storage_class_env(build_isolated_test_store(root, "artifact-retirement", &[4])).await;
    metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
    (ctx, store, shutdown)
}

async fn create_bucket(store: &ECStore, bucket: &str, artifact: bool) -> Uuid {
    store
        .make_bucket(
            bucket,
            &MakeBucketOptions {
                artifact_base_prefix: artifact.then(|| "outputs/".to_owned()),
                versioning_enabled: true,
                ..Default::default()
            },
        )
        .await
        .expect("create real four-disk versioned bucket");
    store
        .bucket_incarnation_id(bucket)
        .await
        .expect("read actual bucket incarnation")
}

async fn disks(store: &ECStore) -> Vec<DiskStore> {
    let disks = store.pools[0].disk_set[0]
        .disk_inventory()
        .await
        .into_iter()
        .map(|disk| disk.expect("fixture disk remains online"))
        .collect::<Vec<_>>();
    assert_eq!(disks.len(), 4);
    disks
}

fn object_path(disk: &DiskStore, bucket: &str, object: &str) -> PathBuf {
    disk.get_object_path_for_io_if_local(bucket, &encode_dir_object(object))
        .expect("fixture uses local disks")
        .expect("resolve object through held mount")
}

async fn file_bytes(path: &Path) -> Option<Vec<u8>> {
    match tokio::fs::read(path).await {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => panic!("read actual fixture file: {error}"),
    }
}

async fn metadata(store: &ECStore, bucket: &str, object: &str) -> Vec<Option<Vec<u8>>> {
    let mut result = Vec::new();
    for disk in disks(store).await {
        let path = object_path(&disk, bucket, object).join(STORAGE_FORMAT_FILE);
        result.push(file_bytes(&path).await);
    }
    result
}

async fn put_payload(store: &ECStore, bucket: &str, object: &str, byte: u8, legacy: bool) -> Uuid {
    let mut options = put_options();
    if legacy {
        // Exercise genuine supported minority metadata recovery, without fabricated proofs.
        options.shard_integrity_write_mode = Some(ShardIntegrityWriteMode::Legacy);
    }
    let body = if object.ends_with('/') {
        Vec::new()
    } else {
        vec![byte; PAYLOAD_SIZE]
    };
    store
        .put_object(bucket, object, &mut PutObjReader::from_vec(body), &options)
        .await
        .expect("commit exact payload and version to all four disks")
        .version_id
        .expect("artifact PUT returns a non-null version")
}

async fn assert_payload(store: &ECStore, bucket: &str, object: &str, version: Uuid, byte: u8) {
    let mut reader = store
        .get_object_reader(
            bucket,
            object,
            None,
            Default::default(),
            &ObjectOptions {
                version_id: Some(version.to_string()),
                ..put_options()
            },
        )
        .await
        .expect("retirement leaves the exact committed version readable");
    assert_eq!(reader.object_info.version_id, Some(version), "GET preserves exact version identity");
    let mut body = Vec::new();
    reader
        .stream
        .read_to_end(&mut body)
        .await
        .expect("read exact committed bytes");
    let expected = if object.ends_with('/') {
        Vec::new()
    } else {
        vec![byte; PAYLOAD_SIZE]
    };
    assert_eq!(body, expected, "retirement does not delete or replace committed bytes");
}

async fn remove_shard(store: &ECStore, bucket: &str, object: &str, version: Uuid) -> (DiskStore, PathBuf) {
    let disk = disks(store).await.remove(0);
    let info = disk
        .read_version("", bucket, &encode_dir_object(object), &version.to_string(), &ReadOptions::default())
        .await
        .expect("read exact version before removing a real shard");
    assert_eq!(info.version_id, Some(version));
    let part = object_path(&disk, bucket, object)
        .join(info.data_dir.expect("non-inline object has a data directory").to_string())
        .join("part.1");
    tokio::fs::remove_file(&part).await.expect("remove one real payload shard");
    assert!(!part.exists());
    (disk, part)
}

async fn assert_healthy_version(store: &ECStore, bucket: &str, object: &str, version: Uuid) {
    for disk in disks(store).await {
        let info = disk
            .read_version("", bucket, &encode_dir_object(object), &version.to_string(), &ReadOptions::default())
            .await
            .expect("repaired disk retains exact committed metadata");
        assert_eq!(info.version_id, Some(version));
        assert_eq!(info.size, i64::try_from(PAYLOAD_SIZE).expect("payload size fits i64"));
        let checked = disk
            .check_parts(bucket, &encode_dir_object(object), &info)
            .await
            .expect("verify genuine repaired shard");
        assert!(!checked.results.is_empty());
        assert!(
            checked
                .results
                .iter()
                .all(|result| *result == crate::disk::CHECK_PART_SUCCESS),
            "unretired version's real repaired shard passes integrity verification"
        );
    }
}

fn assert_policy_error(error: &Error) {
    assert!(
        !matches!(
            error,
            Error::FileNotFound | Error::FileVersionNotFound | Error::ObjectNotFound(_, _) | Error::VersionNotFound(_, _, _)
        ),
        "retirement policy refusal must not claim physical absence: {error:?}"
    );
}

async fn assert_repair_rejected(store: &ECStore, bucket: &str, object: &str, version: &str) -> Error {
    let result = HealOperations::heal_object(store, bucket, object, version, &heal_options()).await;
    let error = match result {
        Err(error) | Ok((_, Some(error))) => error,
        Ok((item, None)) => {
            panic!("ARTIFACT_RETIREMENT_NO_RESURRECTION: retired exact version was accepted for real repair: {item:?}")
        }
    };
    assert_policy_error(&error);
    // The proof-returning API must preserve the same refusal without converting it
    // into authoritative absence that a later destructive caller could consume.
    match store.heal_object_with_proof(bucket, object, version, &heal_options()).await {
        Err(error) => assert_policy_error(&error),
        Ok(result) => {
            assert!(result.absence.is_none(), "retirement never produces a physical absence proof");
            assert_policy_error(result.error.as_ref().expect("retired repair returns a policy error"));
        }
    }
    error
}

async fn assert_retired_repair_rejected(store: &ECStore, bucket: &str, object: &str, version: &str) {
    let error = assert_repair_rejected(store, bucket, object, version).await;
    assert!(
        error.to_string().contains("artifact object version is retired"),
        "ARTIFACT_RETIREMENT_NO_RESURRECTION: expected retirement refusal, received {error:?}"
    );
}

async fn seal(store: &Arc<ECStore>, bucket: &str, incarnation: Uuid, prefix: &str) {
    store
        .clone()
        .seal_artifact_attempt_puts(bucket, incarnation, prefix)
        .await
        .expect("persist the attempt PUT seal before retirement");
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_retirement_requires_seal_exact_identity_and_canonical_directory() {
    run_large_stack_async_test("artifact-retirement-identity", || async {
        let root = tempfile::tempdir().expect("retirement identity directory");
        let (ctx, store, shutdown) = open_store(root.path()).await;
        let bucket = "artifact-retirement-identity";
        let incarnation = create_bucket(&store, bucket, true).await;
        let prefix = attempt_prefix();
        let object = format!("{prefix}image.png");
        let directory = format!("{prefix}folder/");
        let version = put_payload(&store, bucket, &object, 0xA5, false).await;
        let directory_version = put_payload(&store, bucket, &directory, 0, false).await;
        let before = metadata(&store, bucket, &object).await;
        assert!(
            store
                .clone()
                .retire_artifact_object_version(bucket, incarnation, &prefix, &object, version)
                .await
                .is_err(),
            "unsealed attempt cannot retire a version"
        );
        seal(&store, bucket, incarnation, &prefix).await;
        let wrong_prefix = attempt_prefix();
        seal(&store, bucket, incarnation, &wrong_prefix).await;
        for (expected, attempt, target, selected) in [
            (Uuid::new_v4(), prefix.clone(), object.clone(), version),
            (incarnation, wrong_prefix, object.clone(), version),
            (incarnation, prefix.clone(), object.clone(), Uuid::nil()),
            (incarnation, prefix.clone(), prefix.clone(), version),
        ] {
            assert!(
                store
                    .clone()
                    .retire_artifact_object_version(bucket, expected, &attempt, &target, selected)
                    .await
                    .is_err(),
                "retirement requires the exact incarnation, attempt, inner object, and non-null version"
            );
        }
        assert_eq!(metadata(&store, bucket, &object).await, before);
        let ordinary = "artifact-retirement-ordinary";
        let ordinary_incarnation = create_bucket(&store, ordinary, false).await;
        assert!(
            store
                .clone()
                .retire_artifact_object_version(ordinary, ordinary_incarnation, &prefix, &object, version)
                .await
                .is_err(),
            "retirement is limited to dedicated artifact buckets"
        );

        store
            .clone()
            .retire_artifact_object_version(bucket, incarnation, &prefix, &directory, directory_version)
            .await
            .expect("retire logical directory identity");
        let record = artifact_version_retirement_record_path(bucket, incarnation, &prefix, &directory, directory_version);
        assert_eq!(
            record,
            artifact_version_retirement_record_path(
                bucket,
                incarnation,
                &prefix,
                &encode_dir_object(&directory),
                directory_version
            )
        );
        let bytes = com::read_config(store.clone(), &record)
            .await
            .expect("read durable directory retirement");
        store
            .clone()
            .retire_artifact_object_version(bucket, incarnation, &prefix, &encode_dir_object(&directory), directory_version)
            .await
            .expect("encoded directory retirement retry is idempotent");
        assert_eq!(com::read_config(store.clone(), &record).await.expect("read unchanged record"), bytes);
        assert_retired_repair_rejected(&store, bucket, &directory, &directory_version.to_string()).await;
        assert_retired_repair_rejected(&store, bucket, &encode_dir_object(&directory), &directory_version.to_string()).await;
        assert_payload(&store, bucket, &directory, directory_version, 0).await;
        // Rejected identity requests must not retire the valid regular object.
        let (_, missing) = remove_shard(&store, bucket, &object, version).await;
        let (_, error) = HealOperations::heal_object(store.as_ref(), bucket, &object, &version.to_string(), &heal_options())
            .await
            .expect("unretired version remains repairable after rejected retirement");
        assert!(error.is_none(), "unretired repair failed: {error:?}");
        assert!(missing.exists(), "unretired version's real shard is restored");
        assert_healthy_version(&store, bucket, &object, version).await;
        assert_payload(&store, bucket, &object, version, 0xA5).await;
        shutdown.cancel();
        assert!(ctx.wait_for_detached_mutations(DEADLINE).await);
    });
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_retirement_rejects_explicit_and_latest_repair_but_preserves_other_version_after_reopen() {
    run_large_stack_async_test("artifact-retirement-versions", || async {
        let root = tempfile::tempdir().expect("retired version fixture directory");
        let (ctx, store, shutdown) = open_store(root.path()).await;
        let bucket = "artifact-retirement-versions";
        let incarnation = create_bucket(&store, bucket, true).await;
        let prefix = attempt_prefix();
        let object = format!("{prefix}image.png");
        let retained = put_payload(&store, bucket, &object, 0xA5, false).await;
        let retired = put_payload(&store, bucket, &object, 0xB6, false).await;
        assert_ne!(retained, retired);
        seal(&store, bucket, incarnation, &prefix).await;
        let before = metadata(&store, bucket, &object).await;
        let mut physical_parts = Vec::new();
        for disk in disks(&store).await {
            for version in [retained, retired] {
                let info = disk
                    .read_version("", bucket, &object, &version.to_string(), &ReadOptions::default())
                    .await
                    .expect("snapshot exact version before retirement");
                assert_eq!(info.version_id, Some(version));
                let part = object_path(&disk, bucket, &object)
                    .join(info.data_dir.expect("payload data directory").to_string())
                    .join("part.1");
                physical_parts.push((part.clone(), tokio::fs::read(part).await.expect("snapshot original shard bytes")));
            }
        }
        store
            .clone()
            .retire_artifact_object_version(bucket, incarnation, &prefix, &object, retired)
            .await
            .expect("persist exact latest-version retirement");
        assert_eq!(
            metadata(&store, bucket, &object).await,
            before,
            "retirement itself does not edit object metadata"
        );
        for (part, bytes) in physical_parts {
            assert_eq!(
                tokio::fs::read(part).await.expect("retirement leaves every physical shard"),
                bytes,
                "retirement changes neither version's physical data"
            );
        }
        assert_payload(&store, bucket, &object, retired, 0xB6).await;
        assert_payload(&store, bucket, &object, retained, 0xA5).await;
        let (_, retired_part) = remove_shard(&store, bucket, &object, retired).await;
        let before = metadata(&store, bucket, &object).await;
        for selected in [retired.to_string(), String::new()] {
            assert_retired_repair_rejected(&store, bucket, &object, &selected).await;
            assert!(
                !retired_part.exists(),
                "ARTIFACT_RETIREMENT_NO_RESURRECTION: retired shard must stay absent"
            );
            assert_eq!(metadata(&store, bucket, &object).await, before, "retired repair publishes no metadata");
        }
        let (_, retained_part) = remove_shard(&store, bucket, &object, retained).await;
        let (_, error) = HealOperations::heal_object(store.as_ref(), bucket, &object, &retained.to_string(), &heal_options())
            .await
            .expect("another version on the same object remains repairable");
        assert!(error.is_none(), "other-version repair failed: {error:?}");
        assert!(retained_part.exists(), "other version's actual shard is restored");
        assert_healthy_version(&store, bucket, &object, retained).await;
        assert!(!retired_part.exists(), "other-version repair must not restore the retired shard");
        assert_payload(&store, bucket, &object, retained, 0xA5).await;
        assert_payload(&store, bucket, &object, retired, 0xB6).await;
        shutdown.cancel();
        assert!(ctx.wait_for_detached_mutations(DEADLINE).await);
        drop(store);
        drop(ctx);

        let (ctx, store, shutdown) = open_store(root.path()).await;
        assert_eq!(store.bucket_incarnation_id(bucket).await.expect("reopened incarnation"), incarnation);
        assert_retired_repair_rejected(&store, bucket, &object, &retired.to_string()).await;
        assert!(!retired_part.exists(), "reopen cannot forget exact-version retirement");
        assert_payload(&store, bucket, &object, retired, 0xB6).await;
        assert_payload(&store, bucket, &object, retained, 0xA5).await;
        shutdown.cancel();
        assert!(ctx.wait_for_detached_mutations(DEADLINE).await);
    });
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_retirement_blocks_minority_metadata_regeneration() {
    run_large_stack_async_test("artifact-retirement-regeneration", || async {
        let root = tempfile::tempdir().expect("minority metadata fixture directory");
        let (ctx, store, shutdown) = open_store(root.path()).await;
        let bucket = "artifact-retirement-regeneration";
        let incarnation = create_bucket(&store, bucket, true).await;
        let prefix = attempt_prefix();
        let object = format!("{prefix}image.png");
        let version = put_payload(&store, bucket, &object, 0xA5, true).await;
        seal(&store, bucket, incarnation, &prefix).await;
        store
            .clone()
            .retire_artifact_object_version(bucket, incarnation, &prefix, &object, version)
            .await
            .expect("persist version retirement before minority metadata damage");
        assert_payload(&store, bucket, &object, version, 0xA5).await;
        let mut parts = Vec::new();
        for (index, disk) in disks(&store).await.iter().enumerate() {
            let info = disk
                .read_version("", bucket, &object, &version.to_string(), &ReadOptions::default())
                .await
                .expect("read genuine legacy version before damage");
            assert_eq!(info.version_id, Some(version));
            let part = object_path(disk, bucket, &object)
                .join(info.data_dir.expect("legacy payload has data directory").to_string())
                .join("part.1");
            parts.push((part.clone(), tokio::fs::read(&part).await.expect("snapshot real shard bytes")));
            if index > 0 {
                tokio::fs::remove_file(object_path(disk, bucket, &object).join(STORAGE_FORMAT_FILE))
                    .await
                    .expect("leave exactly one genuine metadata copy");
            }
        }
        let before = metadata(&store, bucket, &object).await;
        assert_eq!(before.iter().filter(|entry| entry.is_some()).count(), 1);
        assert_retired_repair_rejected(&store, bucket, &object, &version.to_string()).await;
        assert_eq!(
            metadata(&store, bucket, &object).await,
            before,
            "ARTIFACT_RETIREMENT_NO_RESURRECTION: minority regeneration must not publish retired metadata"
        );
        for (path, bytes) in parts {
            assert_eq!(tokio::fs::read(path).await.expect("retired shard remains physically present"), bytes);
        }
        shutdown.cancel();
        assert!(ctx.wait_for_detached_mutations(DEADLINE).await);
    });
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_retirement_corrupt_or_wrong_identity_records_fail_closed() {
    run_large_stack_async_test("artifact-retirement-records", || async {
        let root = tempfile::tempdir().expect("corrupt retirement fixture directory");
        let (ctx, store, shutdown) = open_store(root.path()).await;
        let bucket = "artifact-retirement-records";
        let incarnation = create_bucket(&store, bucket, true).await;
        let prefix = attempt_prefix();
        let object = format!("{prefix}image.png");
        let version = put_payload(&store, bucket, &object, 0xA5, false).await;
        seal(&store, bucket, incarnation, &prefix).await;
        store
            .clone()
            .retire_artifact_object_version(bucket, incarnation, &prefix, &object, version)
            .await
            .expect("create genuine retirement record");
        let record = artifact_version_retirement_record_path(bucket, incarnation, &prefix, &object, version);
        let original = com::read_config(store.clone(), &record)
            .await
            .expect("read genuine retirement JSON");
        let decoded: serde_json::Value = serde_json::from_slice(&original).expect("retirement record is JSON");
        let mut invalid = vec![Vec::new(), b"not-json".to_vec()];
        for (field, value) in [
            ("format_version", serde_json::json!(2)),
            ("bucket", serde_json::json!("different-bucket")),
            ("base_prefix", serde_json::json!("other/")),
            ("incarnation_id", serde_json::json!(Uuid::new_v4())),
            ("attempt_prefix", serde_json::json!(attempt_prefix())),
            ("object", serde_json::json!(format!("{prefix}other.png"))),
            ("version_id", serde_json::json!(Uuid::new_v4())),
            ("unexpected_field", serde_json::json!(true)),
        ] {
            let mut changed = decoded.clone();
            changed
                .as_object_mut()
                .expect("strict identity record is an object")
                .insert(field.to_owned(), value);
            invalid.push(serde_json::to_vec(&changed).expect("encode invalid persisted identity"));
        }
        let (_, missing) = remove_shard(&store, bucket, &object, version).await;
        let before = metadata(&store, bucket, &object).await;
        for bytes in invalid {
            // Inject corrupt persisted bytes through real guarded config I/O; the
            // fixture does not emulate the production retirement state machine.
            com::with_config_object_write_lock(store.clone(), record.clone(), {
                let store = store.clone();
                let record = record.clone();
                move |guard| async move { com::save_config_no_lock(store, &record, bytes, &guard).await }
            })
            .await
            .expect("hold matching record guard")
            .expect("persist invalid record on real disks");
            assert!(
                store
                    .clone()
                    .retire_artifact_object_version(bucket, incarnation, &prefix, &object, version)
                    .await
                    .is_err(),
                "corrupt or wrong-identity record cannot confirm idempotent retirement"
            );
            assert_repair_rejected(&store, bucket, &object, &version.to_string()).await;
            assert!(!missing.exists(), "invalid record must not authorize real repair publication");
            assert_eq!(metadata(&store, bucket, &object).await, before);
            assert_payload(&store, bucket, &object, version, 0xA5).await;
        }
        shutdown.cancel();
        assert!(ctx.wait_for_detached_mutations(DEADLINE).await);
    });
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_retirement_waits_for_cancelled_physical_repair_and_retries_cancelled_retirement() {
    run_large_stack_async_test("artifact-retirement-physical-tail", || async {
        let root = tempfile::tempdir().expect("physical retirement fixture directory");
        let (ctx, store, shutdown) = open_store(root.path()).await;
        let bucket = "artifact-retirement-physical-tail";
        let incarnation = create_bucket(&store, bucket, true).await;
        let prefix = attempt_prefix();
        let object = format!("{prefix}image.png");
        let version = put_payload(&store, bucket, &object, 0xA5, false).await;
        seal(&store, bucket, incarnation, &prefix).await;
        let (disk, part) = remove_shard(&store, bucket, &object, version).await;
        let destination = object_path(&disk, bucket, &object);
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let repair_hook = os::prepared_publication_test_hooks::install_at(
            os::prepared_publication_test_hooks::Stage::PreparedRename,
            &destination.join(STORAGE_FORMAT_FILE),
            move || {
                let _ = entered_tx.send(());
                // Sender drop also releases physical work when an assertion unwinds.
                let _ = release_rx.recv();
            },
        );
        let repair_store = store.clone();
        let repair_object = object.clone();
        let mut repair = tokio::spawn(async move {
            HealOperations::heal_object(repair_store.as_ref(), bucket, &repair_object, &version.to_string(), &heal_options())
                .await
        });
        tokio::time::timeout(DEADLINE, async {
            tokio::select! {
                entered = entered_rx => entered.expect("repair reaches real physical rename syscall"),
                result = &mut repair => panic!("repair returned before syscall pause: {result:?}"),
            }
        })
        .await
        .expect("real repair worker is paused");
        repair.abort();
        assert!(repair.await.expect_err("cancel repair caller").is_cancelled());

        let retire_store = store.clone();
        let retire_prefix = prefix.clone();
        let retire_object = object.clone();
        let mut retirement = tokio::spawn(async move {
            retire_store
                .retire_artifact_object_version(bucket, incarnation, &retire_prefix, &retire_object, version)
                .await
        });
        let blocked = tokio::time::timeout(Duration::from_millis(200), &mut retirement)
            .await
            .is_err();
        drop(release_tx);
        drop(
            tokio::time::timeout(DEADLINE, os::acquire_rename_data_mutation_lease(&disk.path(), bucket, &destination))
                .await
                .expect("cancelled physical repair drains"),
        );
        drop(repair_hook);
        if blocked {
            tokio::time::timeout(DEADLINE, retirement)
                .await
                .expect("retirement resumes after real repair tail")
                .expect("retirement task joins")
                .expect("retirement persists");
        }
        assert!(
            blocked,
            "ARTIFACT_RETIREMENT_NO_RESURRECTION: retirement must wait for cancelled repair's physical publication tail"
        );
        assert_payload(&store, bucket, &object, version, 0xA5).await;
        // A cancelled repair may roll back its destination; retirement must stop
        // any later repair regardless of whether that first publication committed.
        let before = metadata(&store, bucket, &object).await;
        let part_before = file_bytes(&part).await;
        assert_retired_repair_rejected(&store, bucket, &object, &version.to_string()).await;
        assert_eq!(metadata(&store, bucket, &object).await, before);
        assert_eq!(file_bytes(&part).await, part_before);

        let second_prefix = attempt_prefix();
        let second_object = format!("{second_prefix}cancelled.bin");
        let second_version = put_payload(&store, bucket, &second_object, 0xB6, false).await;
        seal(&store, bucket, incarnation, &second_prefix).await;
        let record = artifact_version_retirement_record_path(bucket, incarnation, &second_prefix, &second_object, second_version);
        let record_disk = disks(&store).await.remove(0);
        let record_path = object_path(&record_disk, RUSTFS_META_BUCKET, &record).join(STORAGE_FORMAT_FILE);
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let _retirement_hook = os::prepared_publication_test_hooks::install_at(
            os::prepared_publication_test_hooks::Stage::PreparedRename,
            &record_path,
            move || {
                let _ = entered_tx.send(());
                let _ = release_rx.recv();
            },
        );
        let retire_store = store.clone();
        let retire_prefix = second_prefix.clone();
        let retire_object = second_object.clone();
        let mut retirement = tokio::spawn(async move {
            retire_store
                .retire_artifact_object_version(bucket, incarnation, &retire_prefix, &retire_object, second_version)
                .await
        });
        tokio::time::timeout(DEADLINE, async {
            tokio::select! {
                entered = entered_rx => entered.expect("retirement reaches real record rename syscall"),
                result = &mut retirement => panic!("retirement returned before durable record publication: {result:?}"),
            }
        })
        .await
        .expect("retirement's real config publication is paused");
        retirement.abort();
        assert!(retirement.await.expect_err("cancel retirement caller").is_cancelled());
        let retry_store = store.clone();
        let retry_prefix = second_prefix.clone();
        let retry_object = second_object.clone();
        let mut retry = tokio::spawn(async move {
            retry_store
                .retire_artifact_object_version(bucket, incarnation, &retry_prefix, &retry_object, second_version)
                .await
        });
        let retry_blocked = tokio::time::timeout(Duration::from_millis(200), &mut retry).await.is_err();
        drop(release_tx);
        if retry_blocked {
            tokio::time::timeout(DEADLINE, retry)
                .await
                .expect("retry waits for original publication tail")
                .expect("retry task joins")
                .expect("cancelled retirement is confirmed idempotently");
        }
        assert!(retry_blocked, "retirement retry cannot pass a physically incomplete original publication");
        let persisted = com::read_config(store.clone(), &record)
            .await
            .expect("cancelled retirement left durable record");
        store
            .clone()
            .retire_artifact_object_version(bucket, incarnation, &second_prefix, &second_object, second_version)
            .await
            .expect("idempotently confirm cancelled retirement");
        assert_eq!(com::read_config(store.clone(), &record).await.expect("read confirmed record"), persisted);
        assert_retired_repair_rejected(&store, bucket, &second_object, &second_version.to_string()).await;
        assert_payload(&store, bucket, &second_object, second_version, 0xB6).await;
        shutdown.cancel();
        assert!(ctx.wait_for_detached_mutations(DEADLINE).await);
    });
}
