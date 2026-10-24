#![cfg(unix)]

use super::{build_isolated_test_store, run_large_stack_async_test, without_storage_class_env};
use crate::{
    bucket::{
        artifact::{ArtifactPutTarget, artifact_put_seal_record_path, artifact_repair_seal_record_path},
        metadata_sys,
    },
    config::com,
    disk::{DiskAPI as _, DiskStore, RUSTFS_META_BUCKET, ReadOptions, STORAGE_FORMAT_FILE, os},
    object_api::{ObjectOptions, PutObjReader, ShardIntegrityWriteMode, WriteCompletion},
    runtime::instance::InstanceContext,
    storage_api_contracts::{
        bucket::{BUCKET_LIFECYCLE_LOCK_OBJECT, BucketOperations as _, DeleteBucketOptions, MakeBucketOptions},
        namespace::NamespaceLocking as _,
        object::ObjectIO as _,
    },
    store::ECStore,
};
use rustfs_heal_contracts::heal_channel::{HealOpts, HealScanMode};
use std::{path::PathBuf, sync::Arc, time::Duration};
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
    let (ctx, store, shutdown) = without_storage_class_env(build_isolated_test_store(root, "artifact-repair", &[4])).await;
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
        .expect("create real four-disk bucket");
    store.bucket_incarnation_id(bucket).await.expect("read bucket incarnation")
}

async fn disks(store: &ECStore) -> Vec<DiskStore> {
    let disks = store.pools[0].disk_set[0]
        .disk_inventory()
        .await
        .into_iter()
        .map(|disk| disk.expect("fixture disk stays online"))
        .collect::<Vec<_>>();
    assert_eq!(disks.len(), 4);
    disks
}

fn object_path(disk: &DiskStore, bucket: &str, object: &str) -> PathBuf {
    disk.get_object_path_for_io_if_local(bucket, object)
        .expect("fixture uses local disks")
        .expect("resolve object through the held mount")
}

async fn metadata(store: &ECStore, bucket: &str, object: &str) -> Vec<Vec<u8>> {
    let mut result = Vec::new();
    for disk in disks(store).await {
        result.push(disk.read_xl(bucket, object, false).await.expect("read real xl.meta").buf);
    }
    result
}

#[derive(Clone, Copy, Debug)]
enum RepairKind {
    Rename,
    Metadata,
}

async fn put_payload(store: &ECStore, bucket: &str, object: &str, kind: RepairKind) -> Uuid {
    let mut opts = put_options();
    if matches!(kind, RepairKind::Metadata) {
        // Minority metadata recovery is supported for the legacy on-disk format.
        // Write genuine legacy shards; do not fabricate a protected integrity proof.
        opts.shard_integrity_write_mode = Some(ShardIntegrityWriteMode::Legacy);
    }
    store
        .put_object(bucket, object, &mut PutObjReader::from_vec(vec![0xA5; PAYLOAD_SIZE]), &opts)
        .await
        .expect("commit real payload and version on every disk")
        .version_id
        .expect("artifact PUT is versioned")
}

async fn damage_for_repair(store: &ECStore, bucket: &str, object: &str, version: Uuid, kind: RepairKind) -> DiskStore {
    let disks = disks(store).await;
    match kind {
        RepairKind::Rename => {
            let disk = &disks[0];
            let info = disk
                .read_version("", bucket, object, &version.to_string(), &ReadOptions::default())
                .await
                .expect("read the exact version before removing its shard");
            let part = object_path(disk, bucket, object)
                .join(info.data_dir.expect("non-inline payload has a data directory").to_string())
                .join("part.1");
            tokio::fs::remove_file(&part).await.expect("remove one actual payload shard");
            assert!(!part.exists());
            disk.clone()
        }
        RepairKind::Metadata => {
            for disk in &disks[1..] {
                tokio::fs::remove_file(object_path(disk, bucket, object).join(STORAGE_FORMAT_FILE))
                    .await
                    .expect("leave one metadata copy and all four actual payload shards");
            }
            disks[1].clone()
        }
    }
}

async fn assert_exact_payload(store: &ECStore, bucket: &str, object: &str, version: Uuid) {
    for disk in disks(store).await {
        let info = disk
            .read_version("", bucket, object, &version.to_string(), &ReadOptions::default())
            .await
            .expect("repaired disk retains the exact committed version");
        assert_eq!(info.version_id, Some(version));
        assert_eq!(info.size, i64::try_from(PAYLOAD_SIZE).expect("fixture size fits i64"));
        let checked = disk
            .check_parts(bucket, object, &info)
            .await
            .expect("check actual repaired shard");
        assert!(!checked.results.is_empty());
        assert!(
            checked
                .results
                .iter()
                .all(|result| *result == crate::disk::CHECK_PART_SUCCESS)
        );
    }
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
        .expect("open exact repaired version");
    let mut body = Vec::new();
    reader.stream.read_to_end(&mut body).await.expect("read repaired payload");
    assert_eq!(
        body,
        vec![0xA5; PAYLOAD_SIZE],
        "ARTIFACT_REPAIR_ADMISSION: repair preserves committed bytes"
    );
}

async fn metadata_files(store: &ECStore, bucket: &str, object: &str) -> Vec<Option<Vec<u8>>> {
    let mut result = Vec::new();
    for disk in disks(store).await {
        match tokio::fs::read(object_path(&disk, bucket, object).join(STORAGE_FORMAT_FILE)).await {
            Ok(bytes) => result.push(Some(bytes)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => result.push(None),
            Err(error) => panic!("read physical metadata snapshot: {error}"),
        }
    }
    result
}

async fn payload_part(disk: &DiskStore, bucket: &str, object: &str, version: Uuid) -> PathBuf {
    let info = disk
        .read_version("", bucket, object, &version.to_string(), &ReadOptions::default())
        .await
        .expect("read exact metadata to locate its physical shard");
    object_path(disk, bucket, object)
        .join(info.data_dir.expect("non-inline payload has a data directory").to_string())
        .join("part.1")
}

async fn assert_repair_seal_denied(store: &ECStore, bucket: &str, object: &str, version: &str) {
    use crate::{error::Error, storage_api_contracts::heal::HealOperations};

    for result in [
        HealOperations::heal_object(store, bucket, object, version, &heal_options()).await,
        HealOperations::heal_object(store.pools[0].disk_set[0].as_ref(), bucket, object, version, &heal_options()).await,
    ] {
        let error = match result {
            Err(error) | Ok((_, Some(error))) => error,
            Ok((item, None)) => panic!("ARTIFACT_ATTEMPT_REPAIR_SEAL: closed attempt admitted repair: {item:?}"),
        };
        assert!(
            !matches!(
                error,
                Error::FileNotFound | Error::FileVersionNotFound | Error::ObjectNotFound(_, _) | Error::VersionNotFound(_, _, _)
            ),
            "ARTIFACT_ATTEMPT_REPAIR_SEAL: repair refusal must not claim physical absence: {error:?}"
        );
    }
    if let Ok(result) = store.heal_object_with_proof(bucket, object, version, &heal_options()).await {
        assert!(result.error.is_some(), "ARTIFACT_ATTEMPT_REPAIR_SEAL: proof API rejects repair");
        assert!(result.absence.is_none(), "repair seal does not produce an absence proof");
    }
}

async fn assert_payload_readable(store: &ECStore, bucket: &str, object: &str, version: Uuid) {
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
        .expect("sealed version remains readable with data quorum");
    assert_eq!(reader.object_info.version_id, Some(version));
    let mut body = Vec::new();
    reader.stream.read_to_end(&mut body).await.expect("read sealed version bytes");
    assert_eq!(
        body,
        vec![0xA5; PAYLOAD_SIZE],
        "ARTIFACT_ATTEMPT_REPAIR_SEAL: seal preserves committed bytes"
    );
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_repair_low_level_put_requires_matching_target() {
    run_large_stack_async_test("artifact-repair-admission", || async {
        let root = tempfile::tempdir().expect("admission fixture directory");
        let (ctx, store, shutdown) = open_store(root.path()).await;
        let bucket = "artifact-repair-admission";
        let incarnation = create_bucket(&store, bucket, true).await;
        let object = format!("{}image.png", attempt_prefix());
        put_payload(&store, bucket, &object, RepairKind::Rename).await;
        let before = metadata(&store, bucket, &object).await;
        let snapshot = metadata_sys::get_bucket_incarnation_metadata_in(&ctx, bucket)
            .await
            .expect("read authoritative artifact classification");
        let wrong_target = ArtifactPutTarget::from_metadata(&snapshot, &format!("{}other.png", attempt_prefix()))
            .expect("derive a real but different target");
        for target in [None, wrong_target] {
            let result = store.pools[0].disk_set[0]
                .put_object(
                    bucket,
                    &object,
                    &mut PutObjReader::from_vec(vec![0xBB; 379]),
                    &ObjectOptions {
                        expected_bucket_incarnation_id: Some(incarnation),
                        artifact_put_target: target,
                        ..put_options()
                    },
                )
                .await;
            assert!(
                result.is_err(),
                "ARTIFACT_REPAIR_ADMISSION: low-level artifact PUT requires its matching target"
            );
            assert_eq!(metadata(&store, bucket, &object).await, before, "rejected PUT does not publish a version");
        }
        let ordinary = "artifact-repair-ordinary";
        let ordinary_incarnation = create_bucket(&store, ordinary, false).await;
        store.pools[0].disk_set[0]
            .put_object(
                ordinary,
                "ordinary.bin",
                &mut PutObjReader::from_vec(vec![0xCC; 379]),
                &ObjectOptions {
                    expected_bucket_incarnation_id: Some(ordinary_incarnation),
                    ..put_options()
                },
            )
            .await
            .expect("ARTIFACT_REPAIR_ADMISSION: ordinary low-level PUT needs no artifact target");
        assert_eq!(metadata(&store, ordinary, "ordinary.bin").await.len(), 4);
        shutdown.cancel();
        assert!(ctx.wait_for_detached_mutations(DEADLINE).await);
    });
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_repair_preserves_existing_versions_after_seal() {
    run_large_stack_async_test("artifact-repair-sealed", || async {
        let root = tempfile::tempdir().expect("sealed repair fixture directory");
        let (ctx, store, shutdown) = open_store(root.path()).await;
        let bucket = "artifact-repair-sealed";
        let incarnation = create_bucket(&store, bucket, true).await;
        for kind in [RepairKind::Rename, RepairKind::Metadata] {
            let prefix = attempt_prefix();
            let object = format!("{prefix}image.png");
            let version = put_payload(&store, bucket, &object, kind).await;
            store
                .clone()
                .seal_artifact_attempt_puts(bucket, incarnation, &prefix)
                .await
                .expect("persist seal");
            damage_for_repair(&store, bucket, &object, version, kind).await;
            let repaired = store
                .heal_object_with_proof(bucket, &object, &version.to_string(), &heal_options())
                .await
                .expect("ARTIFACT_REPAIR_ADMISSION: sealed existing version remains repairable");
            assert!(repaired.error.is_none(), "repair failed: {:?}", repaired.error);
            assert_exact_payload(&store, bucket, &object, version).await;
            let before = metadata(&store, bucket, &object).await;
            assert!(
                store
                    .put_object(bucket, &object, &mut PutObjReader::from_vec(vec![0xBB; 379]), &put_options())
                    .await
                    .is_err()
            );
            assert_eq!(
                metadata(&store, bucket, &object).await,
                before,
                "repair does not reopen the attempt for PUT"
            );
        }
        shutdown.cancel();
        assert!(ctx.wait_for_detached_mutations(DEADLINE).await);
    });
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_attempt_repair_seal_covers_all_versions_and_survives_reopen() {
    run_large_stack_async_test("artifact-attempt-repair-seal", || async {
        let root = tempfile::tempdir().expect("attempt repair seal fixture directory");
        let (ctx, store, shutdown) = open_store(root.path()).await;
        let bucket = "artifact-attempt-repair-seal";
        let incarnation = create_bucket(&store, bucket, true).await;
        let prefix = attempt_prefix();
        let object = format!("{prefix}image.png");
        let old = put_payload(&store, bucket, &object, RepairKind::Rename).await;
        let latest = put_payload(&store, bucket, &object, RepairKind::Rename).await;
        assert_ne!(old, latest);
        let minority_object = format!("{prefix}legacy.png");
        let minority = put_payload(&store, bucket, &minority_object, RepairKind::Metadata).await;
        let other_prefix = attempt_prefix();
        let other_object = format!("{other_prefix}image.png");
        let other = put_payload(&store, bucket, &other_object, RepairKind::Rename).await;
        for prefix in [&prefix, &other_prefix] {
            store
                .clone()
                .seal_artifact_attempt_puts(bucket, incarnation, prefix)
                .await
                .expect("seal ordinary PUTs");
        }
        let intact_metadata = metadata_files(&store, bucket, &object).await;
        store
            .clone()
            .seal_artifact_attempt_repairs(bucket, incarnation, &prefix)
            .await
            .expect("close attempt repairs");
        assert_eq!(
            metadata_files(&store, bucket, &object).await,
            intact_metadata,
            "ARTIFACT_ATTEMPT_REPAIR_SEAL: closing repairs does not edit object metadata"
        );
        let record = artifact_repair_seal_record_path(bucket, incarnation, &prefix);
        let persisted = com::read_config(store.clone(), &record)
            .await
            .expect("read durable repair seal");
        store
            .clone()
            .seal_artifact_attempt_repairs(bucket, incarnation, &prefix)
            .await
            .expect("repair seal is idempotent");
        assert_eq!(
            com::read_config(store.clone(), &record).await.expect("read repeated seal"),
            persisted,
            "ARTIFACT_ATTEMPT_REPAIR_SEAL: retry preserves the exact durable fact"
        );

        let disk = disks(&store).await.remove(0);
        let mut missing = Vec::new();
        for version in [old, latest] {
            let part = payload_part(&disk, bucket, &object, version).await;
            damage_for_repair(&store, bucket, &object, version, RepairKind::Rename).await;
            missing.push(part);
        }
        damage_for_repair(&store, bucket, &minority_object, minority, RepairKind::Metadata).await;
        let before = metadata_files(&store, bucket, &object).await;
        let minority_before = metadata_files(&store, bucket, &minority_object).await;
        for selected in [old.to_string(), latest.to_string(), String::new()] {
            assert_repair_seal_denied(&store, bucket, &object, &selected).await;
            assert_eq!(
                missing.iter().map(|part| part.exists()).collect::<Vec<_>>(),
                vec![false, false],
                "ARTIFACT_ATTEMPT_REPAIR_SEAL: every version's missing shard stays absent"
            );
            assert_eq!(
                metadata_files(&store, bucket, &object).await,
                before,
                "ARTIFACT_ATTEMPT_REPAIR_SEAL: rejected repair publishes no metadata"
            );
        }
        for selected in [minority.to_string(), String::new()] {
            assert_repair_seal_denied(&store, bucket, &minority_object, &selected).await;
            assert_eq!(
                metadata_files(&store, bucket, &minority_object).await,
                minority_before,
                "ARTIFACT_ATTEMPT_REPAIR_SEAL: minority recovery cannot regenerate missing metadata"
            );
        }
        for version in [old, latest] {
            assert_payload_readable(&store, bucket, &object, version).await;
        }
        shutdown.cancel();
        assert!(ctx.wait_for_detached_mutations(DEADLINE).await);
        drop(store);
        drop(ctx);

        let (ctx, store, shutdown) = open_store(root.path()).await;
        assert_eq!(store.bucket_incarnation_id(bucket).await.expect("reopened incarnation"), incarnation);
        store
            .clone()
            .seal_artifact_attempt_repairs(bucket, incarnation, &prefix)
            .await
            .expect("confirm seal after reopen");
        for selected in [old.to_string(), latest.to_string(), String::new()] {
            assert_repair_seal_denied(&store, bucket, &object, &selected).await;
        }
        assert_repair_seal_denied(&store, bucket, &minority_object, &minority.to_string()).await;
        assert_eq!(
            missing.iter().map(|part| part.exists()).collect::<Vec<_>>(),
            vec![false, false],
            "ARTIFACT_ATTEMPT_REPAIR_SEAL: reopen preserves whole-attempt shard refusal"
        );
        assert_eq!(
            metadata_files(&store, bucket, &object).await,
            before,
            "ARTIFACT_ATTEMPT_REPAIR_SEAL: reopen preserves object metadata"
        );
        assert_eq!(
            metadata_files(&store, bucket, &minority_object).await,
            minority_before,
            "ARTIFACT_ATTEMPT_REPAIR_SEAL: reopen preserves minority metadata refusal"
        );
        for version in [old, latest] {
            assert_payload_readable(&store, bucket, &object, version).await;
        }
        let other_disk = damage_for_repair(&store, bucket, &other_object, other, RepairKind::Rename).await;
        let other_part = payload_part(&other_disk, bucket, &other_object, other).await;
        let repaired = store
            .heal_object_with_proof(bucket, &other_object, &other.to_string(), &heal_options())
            .await
            .expect("another ordinary-sealed attempt remains repairable");
        assert!(repaired.error.is_none(), "other-attempt repair failed: {:?}", repaired.error);
        assert!(
            other_part.exists(),
            "ARTIFACT_ATTEMPT_REPAIR_SEAL: another attempt restores its physical shard"
        );
        assert_exact_payload(&store, bucket, &other_object, other).await;
        assert_eq!(
            missing.iter().map(|part| part.exists()).collect::<Vec<_>>(),
            vec![false, false],
            "ARTIFACT_ATTEMPT_REPAIR_SEAL: another attempt cannot restore closed versions"
        );
        shutdown.cancel();
        assert!(ctx.wait_for_detached_mutations(DEADLINE).await);
    });
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_attempt_repair_seal_requires_ordinary_seal_and_matching_identity() {
    run_large_stack_async_test("artifact-repair-seal-prerequisite", || async {
        let root = tempfile::tempdir().expect("repair seal prerequisite fixture directory");
        let (ctx, store, shutdown) = open_store(root.path()).await;
        let bucket = "artifact-repair-seal-prerequisite";
        let incarnation = create_bucket(&store, bucket, true).await;
        let prefix = attempt_prefix();
        let object = format!("{prefix}image.png");
        let version = put_payload(&store, bucket, &object, RepairKind::Rename).await;
        let record = artifact_repair_seal_record_path(bucket, incarnation, &prefix);
        assert!(
            store
                .clone()
                .seal_artifact_attempt_repairs(bucket, incarnation, &prefix)
                .await
                .is_err(),
            "repair close requires the separate ordinary PUT seal"
        );
        assert!(matches!(
            com::read_config(store.clone(), &record).await,
            Err(crate::error::Error::ConfigNotFound)
        ));
        damage_for_repair(&store, bucket, &object, version, RepairKind::Rename).await;
        let repaired = store
            .heal_object_with_proof(bucket, &object, &version.to_string(), &heal_options())
            .await
            .expect("a rejected close leaves repair admission open");
        assert!(repaired.error.is_none(), "repair after rejected close failed: {:?}", repaired.error);
        assert_exact_payload(&store, bucket, &object, version).await;
        store
            .clone()
            .seal_artifact_attempt_puts(bucket, incarnation, &prefix)
            .await
            .expect("seal ordinary PUTs");
        for (identity, prefix) in [
            (Uuid::nil(), prefix.clone()),
            (Uuid::new_v4(), prefix.clone()),
            (incarnation, object),
        ] {
            assert!(
                store
                    .clone()
                    .seal_artifact_attempt_repairs(bucket, identity, &prefix)
                    .await
                    .is_err(),
                "repair close requires exact bucket incarnation and attempt prefix"
            );
        }
        assert!(matches!(
            com::read_config(store.clone(), &record).await,
            Err(crate::error::Error::ConfigNotFound)
        ));
        store
            .clone()
            .seal_artifact_attempt_repairs(bucket, incarnation, &prefix)
            .await
            .expect("matching sealed attempt closes");
        shutdown.cancel();
        assert!(ctx.wait_for_detached_mutations(DEADLINE).await);
    });
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_attempt_repair_seal_corrupt_or_wrong_identity_records_fail_closed() {
    run_large_stack_async_test("artifact-repair-seal-records", || async {
        let root = tempfile::tempdir().expect("repair seal record fixture directory");
        let (ctx, store, shutdown) = open_store(root.path()).await;
        let bucket = "artifact-repair-seal-records";
        let incarnation = create_bucket(&store, bucket, true).await;
        let prefix = attempt_prefix();
        let object = format!("{prefix}image.png");
        let version = put_payload(&store, bucket, &object, RepairKind::Rename).await;
        store
            .clone()
            .seal_artifact_attempt_puts(bucket, incarnation, &prefix)
            .await
            .expect("seal ordinary PUTs");
        store
            .clone()
            .seal_artifact_attempt_repairs(bucket, incarnation, &prefix)
            .await
            .expect("persist genuine repair seal");
        let record = artifact_repair_seal_record_path(bucket, incarnation, &prefix);
        let original = com::read_config(store.clone(), &record).await.expect("read repair seal JSON");
        let decoded: serde_json::Value = serde_json::from_slice(&original).expect("repair seal is JSON");
        let mut invalid = vec![Vec::new(), b"not-json".to_vec()];
        for (field, value) in [
            ("format_version", serde_json::json!(2)),
            ("bucket", serde_json::json!("different-bucket")),
            ("base_prefix", serde_json::json!("other/")),
            ("incarnation_id", serde_json::json!(Uuid::new_v4())),
            ("attempt_prefix", serde_json::json!(attempt_prefix())),
            ("unexpected_field", serde_json::json!(true)),
        ] {
            let mut changed = decoded.clone();
            changed
                .as_object_mut()
                .expect("repair seal is an identity object")
                .insert(field.to_owned(), value);
            invalid.push(serde_json::to_vec(&changed).expect("encode invalid repair seal identity"));
        }
        let disk = damage_for_repair(&store, bucket, &object, version, RepairKind::Rename).await;
        let missing = payload_part(&disk, bucket, &object, version).await;
        let before = metadata_files(&store, bucket, &object).await;
        for bytes in invalid {
            com::with_config_object_write_lock(store.clone(), record.clone(), {
                let store = store.clone();
                let record = record.clone();
                move |guard| async move { com::save_config_no_lock(store, &record, bytes, &guard).await }
            })
            .await
            .expect("acquire matching repair record guard")
            .expect("persist invalid repair seal on real disks");
            assert!(
                store
                    .clone()
                    .seal_artifact_attempt_repairs(bucket, incarnation, &prefix)
                    .await
                    .is_err(),
                "invalid persisted seal cannot confirm an idempotent close"
            );
            assert_repair_seal_denied(&store, bucket, &object, &version.to_string()).await;
            assert_repair_seal_denied(&store, bucket, &object, "").await;
            assert!(
                !missing.exists(),
                "ARTIFACT_ATTEMPT_REPAIR_SEAL: invalid seal never admits shard publication"
            );
            assert_eq!(
                metadata_files(&store, bucket, &object).await,
                before,
                "ARTIFACT_ATTEMPT_REPAIR_SEAL: invalid seal never admits metadata publication"
            );
            assert_payload_readable(&store, bucket, &object, version).await;
        }
        shutdown.cancel();
        assert!(ctx.wait_for_detached_mutations(DEADLINE).await);
    });
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_repair_rejects_previous_bucket_incarnation() {
    run_large_stack_async_test("artifact-repair-incarnation", || async {
        let root = tempfile::tempdir().expect("incarnation fixture directory");
        let (ctx, store, shutdown) = open_store(root.path()).await;
        let bucket = "artifact-repair-incarnation";
        let old = create_bucket(&store, bucket, true).await;
        store
            .delete_bucket(bucket, &DeleteBucketOptions::default())
            .await
            .expect("delete empty old incarnation");
        let new = create_bucket(&store, bucket, true).await;
        assert_ne!(old, new);
        let object = format!("{}image.png", attempt_prefix());
        let version = put_payload(&store, bucket, &object, RepairKind::Rename).await;
        damage_for_repair(&store, bucket, &object, version, RepairKind::Rename).await;
        let before = metadata(&store, bucket, &object).await;
        let disk = disks(&store).await.remove(0);
        let result = store
            .heal_object_at_incarnation(bucket, &object, &version.to_string(), old, &heal_options())
            .await;
        assert!(
            result.is_err(),
            "ARTIFACT_REPAIR_ADMISSION: stale incarnation cannot admit a repair into the new bucket"
        );
        assert_eq!(metadata(&store, bucket, &object).await, before);
        let info = disk
            .read_version("", bucket, &object, &version.to_string(), &ReadOptions::default())
            .await
            .expect("new metadata is intact");
        let checked = disk
            .check_parts(bucket, &object, &info)
            .await
            .expect("inspect unrepaired new shard");
        assert!(
            checked
                .results
                .iter()
                .any(|result| *result != crate::disk::CHECK_PART_SUCCESS),
            "stale repair must not recreate the new bucket's shard"
        );
        shutdown.cancel();
        assert!(ctx.wait_for_detached_mutations(DEADLINE).await);
    });
}

fn run_physical_repair_case(kind: RepairKind, cancel: bool, close_repairs: bool) {
    // Retain the large fixture stack and the current-thread clock needed to expire deadlines.
    std::thread::Builder::new()
        .name(format!("artifact-repair-{kind:?}-{cancel}"))
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("physical repair runtime")
                .block_on(physical_repair_case(kind, cancel, close_repairs));
        })
        .expect("physical repair test thread")
        .join()
        .expect("physical repair test completes");
}

async fn physical_repair_case(kind: RepairKind, cancel: bool, close_repairs: bool) {
    temp_env::async_with_vars(
        [
            (rustfs_config::ENV_DRIVE_MAX_TIMEOUT_DURATION, Some("60")),
            (crate::set_disk::ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("false")),
        ],
        async {
            if matches!(kind, RepairKind::Metadata) && !cancel {
                metadata_repair_disk_timeout_case(close_repairs).await;
                return;
            }
            let root = tempfile::tempdir().expect("physical repair fixture directory");
            let (ctx, store, shutdown) = open_store(root.path()).await;
            let bucket = "artifact-repair-physical";
            let incarnation = create_bucket(&store, bucket, true).await;
            let prefix = attempt_prefix();
            let object = format!("{prefix}image.png");
            let version = put_payload(&store, bucket, &object, kind).await;
            if close_repairs {
                store
                    .clone()
                    .seal_artifact_attempt_puts(bucket, incarnation, &prefix)
                    .await
                    .expect("ordinary seal precedes active repair");
            }
            let disk = damage_for_repair(&store, bucket, &object, version, kind).await;
            let destination = object_path(&disk, bucket, &object);
            let metadata_path = destination.join(STORAGE_FORMAT_FILE);
            let stage = match kind {
                RepairKind::Rename => os::prepared_publication_test_hooks::Stage::PreparedRename,
                RepairKind::Metadata => os::prepared_publication_test_hooks::Stage::Rename,
            };
            let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
            let _hook = os::prepared_publication_test_hooks::install_at(stage, &metadata_path, move || {
                let _ = entered_tx.send(());
                // Sender drop releases the actual syscall even when the test unwinds.
                let _ = release_rx.recv();
            });
            let heal_store = store.clone();
            let heal_object = object.clone();
            let mut repair = tokio::spawn(async move {
                // Timeout the heal future itself, so Elapsed drops its repair scope.
                // Timing out only a JoinHandle would leave the entire coordinator alive.
                tokio::time::timeout(
                    Duration::from_secs(60),
                    heal_store.heal_object_with_proof(bucket, &heal_object, &version.to_string(), &heal_options()),
                )
                .await
            });
            tokio::time::timeout(DEADLINE, async {
                tokio::select! {
                    entered = entered_rx => entered.expect("real repair reaches physical publication"),
                    result = &mut repair => panic!("repair returned before its physical publication pause: {result:?}"),
                }
            })
            .await
            .expect("physical repair pause is reached");

            if cancel {
                repair.abort();
                assert!(repair.await.expect_err("repair waiter is cancelled").is_cancelled());
            } else {
                tokio::time::pause();
                tokio::time::advance(Duration::from_secs(61)).await;
                tokio::time::resume();
                // Guarded rename deliberately has no disk-wrapper deadline. Expire the
                // caller future while its owned physical publication is still blocked.
                let result = tokio::time::timeout(DEADLINE, repair)
                    .await
                    .expect("repair waiter returns after its caller deadline")
                    .expect("repair task joins");
                assert!(result.is_err(), "the caller deadline must drop the pending heal future");
            }
            let record = artifact_put_seal_record_path(bucket, incarnation, &prefix);
            let lock = store
                .new_ns_lock(RUSTFS_META_BUCKET, &record)
                .await
                .expect("actual repair/seal coordination lock");
            let retained = lock.get_write_lock(Duration::from_secs(1)).await.is_err();
            let seal_store = store.clone();
            let seal_prefix = prefix.clone();
            let mut seal = tokio::spawn(async move {
                if close_repairs {
                    seal_store
                        .seal_artifact_attempt_repairs(bucket, incarnation, &seal_prefix)
                        .await
                } else {
                    seal_store.seal_artifact_attempt_puts(bucket, incarnation, &seal_prefix).await
                }
            });
            let blocked = tokio::time::timeout(Duration::from_millis(200), &mut seal).await.is_err();

            // Release and drain before regression assertions, so a RED run also cleans up.
            drop(release_tx);
            match kind {
                RepairKind::Rename => {
                    drop(
                        tokio::time::timeout(
                            DEADLINE,
                            os::acquire_rename_data_mutation_lease(&disk.path(), bucket, &destination),
                        )
                        .await
                        .expect("physical rename drains"),
                    );
                }
                RepairKind::Metadata => {
                    tokio::time::timeout(DEADLINE, os::prepared_publication_test_hooks::drain_namespace_key(&metadata_path))
                        .await
                        .expect("physical metadata replacement drains");
                }
            }
            if blocked {
                tokio::time::timeout(DEADLINE, seal)
                    .await
                    .expect("seal resumes after physical repair")
                    .expect("seal task joins")
                    .expect("seal persists");
            }
            assert!(
                retained,
                "ARTIFACT_REPAIR_TAIL_BARRIER: actual repair worker retains attempt ownership after cancellation or timeout"
            );
            assert!(
                blocked,
                "ARTIFACT_REPAIR_TAIL_BARRIER: seal cannot finish while physical repair publication remains paused"
            );
            if close_repairs {
                assert_closed_attempt_cannot_repair(&store, bucket, &object, version, kind).await;
            } else {
                let repaired = store
                    .heal_object_with_proof(bucket, &object, &version.to_string(), &heal_options())
                    .await
                    .expect("finish remaining repair after seal");
                assert!(repaired.error.is_none(), "remaining repair failed: {:?}", repaired.error);
                assert_exact_payload(&store, bucket, &object, version).await;
            }
            shutdown.cancel();
            assert!(ctx.wait_for_detached_mutations(DEADLINE).await);
        },
    )
    .await;
}

async fn metadata_repair_disk_timeout_case(close_repairs: bool) {
    use crate::storage_api_contracts::heal::HealOperations;

    let root = tempfile::tempdir().expect("metadata timeout fixture directory");
    let (ctx, store, shutdown) = open_store(root.path()).await;
    let bucket = "artifact-repair-meta-timeout";
    let incarnation = create_bucket(&store, bucket, true).await;
    let prefix = attempt_prefix();
    let object = format!("{prefix}image.png");
    let version = put_payload(&store, bucket, &object, RepairKind::Metadata).await;
    if close_repairs {
        store
            .clone()
            .seal_artifact_attempt_puts(bucket, incarnation, &prefix)
            .await
            .expect("ordinary seal precedes metadata repair");
    }
    damage_for_repair(&store, bucket, &object, version, RepairKind::Metadata).await;
    let disks = disks(&store).await;
    let mut hooks = Vec::new();
    let mut entered = Vec::new();
    let mut releases = Vec::new();
    let mut timeout_counts = Vec::new();
    for disk in &disks[1..] {
        let destination = object_path(disk, bucket, &object).join(STORAGE_FORMAT_FILE);
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        hooks.push(os::prepared_publication_test_hooks::install_at(
            os::prepared_publication_test_hooks::Stage::Rename,
            &destination,
            move || {
                let _ = entered_tx.send(());
                let _ = release_rx.recv();
            },
        ));
        entered.push(entered_rx);
        releases.push(release_tx);
        timeout_counts.push(disk.metrics_snapshot().expect("local disk metrics").total_errors_timeout);
    }
    let set = store.pools[0].disk_set[0].clone();
    let heal_object = object.clone();
    // The public set entrypoint owns a real repair scope, without the Store's
    // BucketHealScope that deliberately disables write_metadata wrapper deadlines.
    let mut repair = tokio::spawn(async move {
        HealOperations::heal_object(set.as_ref(), bucket, &heal_object, &version.to_string(), &heal_options()).await
    });
    // Regeneration visits disks serially. Timeout every missing metadata writer;
    // allowing a later writer to succeed would retry repair against the paused disk.
    for signal in entered {
        tokio::time::timeout(DEADLINE, async {
            tokio::select! {
                entered = signal => entered.expect("metadata worker reaches the real rename syscall"),
                result = &mut repair => panic!("repair returned before every metadata worker was paused: {result:?}"),
            }
        })
        .await
        .expect("next physical metadata publication starts");
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(61)).await;
        tokio::time::resume();
    }
    let result = tokio::time::timeout(DEADLINE, repair)
        .await
        .expect("repair waiter returns after real metadata wrapper timeouts")
        .expect("repair task joins");
    assert!(
        result.is_err() || result.as_ref().is_ok_and(|(_, error)| error.is_some()),
        "three timed-out metadata writers cannot restore metadata quorum"
    );
    let timed_out = disks[1..]
        .iter()
        .zip(&timeout_counts)
        .all(|(disk, before)| disk.metrics_snapshot().expect("local timeout metrics").total_errors_timeout > *before);
    let record = artifact_put_seal_record_path(bucket, incarnation, &prefix);
    let lock = store
        .new_ns_lock(RUSTFS_META_BUCKET, &record)
        .await
        .expect("actual repair/seal lock");
    let retained = lock.get_write_lock(Duration::from_secs(1)).await.is_err();
    let seal_store = store.clone();
    let mut seal = tokio::spawn(async move {
        if close_repairs {
            seal_store.seal_artifact_attempt_repairs(bucket, incarnation, &prefix).await
        } else {
            seal_store.seal_artifact_attempt_puts(bucket, incarnation, &prefix).await
        }
    });
    let blocked = tokio::time::timeout(Duration::from_millis(200), &mut seal).await.is_err();

    drop(releases);
    for disk in &disks[1..] {
        let path = object_path(disk, bucket, &object).join(STORAGE_FORMAT_FILE);
        tokio::time::timeout(DEADLINE, os::prepared_publication_test_hooks::drain_namespace_key(&path))
            .await
            .expect("timed-out physical metadata worker drains");
    }
    if blocked {
        tokio::time::timeout(DEADLINE, seal)
            .await
            .expect("seal resumes after physical metadata writers drain")
            .expect("seal task joins")
            .expect("seal persists");
    }
    assert!(
        timed_out,
        "actual disk wrapper timeout metrics increase for all three paused metadata writers"
    );
    assert!(
        retained,
        "ARTIFACT_REPAIR_TAIL_BARRIER: physical metadata workers retain the attempt after real disk timeouts"
    );
    assert!(
        blocked,
        "ARTIFACT_REPAIR_TAIL_BARRIER: seal waits for the timed-out physical metadata workers"
    );
    if close_repairs {
        assert_closed_attempt_cannot_repair(&store, bucket, &object, version, RepairKind::Metadata).await;
    } else {
        assert_exact_payload(&store, bucket, &object, version).await;
    }
    shutdown.cancel();
    assert!(ctx.wait_for_detached_mutations(DEADLINE).await);
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_repair_rename_owner_survives_cancellation() {
    run_physical_repair_case(RepairKind::Rename, true, false);
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_repair_rename_owner_survives_caller_deadline() {
    run_physical_repair_case(RepairKind::Rename, false, false);
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_repair_metadata_owner_survives_cancellation() {
    run_physical_repair_case(RepairKind::Metadata, true, false);
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_repair_metadata_owner_survives_disk_timeout() {
    run_physical_repair_case(RepairKind::Metadata, false, false);
}

async fn assert_closed_attempt_cannot_repair(store: &ECStore, bucket: &str, object: &str, version: Uuid, kind: RepairKind) {
    let disks = disks(store).await;
    let missing = match kind {
        RepairKind::Rename => Some(payload_part(&disks[0], bucket, object, version).await),
        RepairKind::Metadata => None,
    };
    let paths = match &missing {
        Some(part) => vec![part.clone()],
        None => disks[1..]
            .iter()
            .map(|disk| object_path(disk, bucket, object).join(STORAGE_FORMAT_FILE))
            .collect(),
    };
    for path in paths {
        match tokio::fs::remove_file(path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!("damage the closed attempt after its physical repair drains: {error}"),
        }
    }
    let before = metadata_files(store, bucket, object).await;
    for selected in [version.to_string(), String::new()] {
        assert_repair_seal_denied(store, bucket, object, &selected).await;
        assert_eq!(
            metadata_files(store, bucket, object).await,
            before,
            "ARTIFACT_ATTEMPT_REPAIR_SEAL: closed repair cannot publish after the old physical tail drains"
        );
        if let Some(part) = &missing {
            assert!(
                !part.exists(),
                "ARTIFACT_ATTEMPT_REPAIR_SEAL: closed repair cannot restore a shard after the old physical tail drains"
            );
        }
    }
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_attempt_repair_seal_waits_for_cancelled_rename() {
    run_physical_repair_case(RepairKind::Rename, true, true);
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_attempt_repair_seal_waits_for_rename_caller_deadline() {
    run_physical_repair_case(RepairKind::Rename, false, true);
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_attempt_repair_seal_waits_for_cancelled_metadata() {
    run_physical_repair_case(RepairKind::Metadata, true, true);
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_attempt_repair_seal_waits_for_metadata_disk_timeout() {
    run_physical_repair_case(RepairKind::Metadata, false, true);
}

#[test]
#[serial_test::serial(storage_class_env)]
fn ordinary_repair_rename_owner_retains_lifecycle_after_cancellation() {
    run_large_stack_async_test("ordinary-repair-lifecycle-tail", || async {
        let root = tempfile::tempdir().expect("ordinary repair fixture directory");
        let (ctx, store, shutdown) = open_store(root.path()).await;
        let bucket = "ordinary-repair-lifecycle-tail";
        let incarnation = create_bucket(&store, bucket, false).await;
        let object = "repair.bin";
        let version = put_payload(&store, bucket, object, RepairKind::Rename).await;
        let disk = damage_for_repair(&store, bucket, object, version, RepairKind::Rename).await;
        let destination = object_path(&disk, bucket, object);
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let _hook = os::prepared_publication_test_hooks::install(&destination.join(STORAGE_FORMAT_FILE), move || {
            let _ = entered_tx.send(());
            // Sender drop also releases the physical worker on assertion failure.
            let _ = release_rx.recv();
        });
        let heal_store = store.clone();
        let mut repair = tokio::spawn(async move {
            heal_store
                .heal_object_with_proof(bucket, object, &version.to_string(), &heal_options())
                .await
        });
        tokio::time::timeout(DEADLINE, async {
            tokio::select! {
                entered = entered_rx => entered.expect("ordinary repair reaches physical metadata publication"),
                result = &mut repair => panic!("ordinary repair returned before physical publication pause: {result:?}"),
            }
        })
        .await
        .expect("ordinary repair pauses inside the real rename worker");
        repair.abort();
        assert!(repair.await.expect_err("ordinary repair waiter is cancelled").is_cancelled());

        let lifecycle = store
            .new_ns_lock(bucket, BUCKET_LIFECYCLE_LOCK_OBJECT)
            .await
            .expect("resolve the same lifecycle namespace used by bucket deletion");
        let retained = lifecycle.get_write_lock(Duration::from_secs(1)).await.is_err();

        // Drain before the regression assertion so a failing owner-retention run cannot hang.
        drop(release_tx);
        drop(
            tokio::time::timeout(DEADLINE, os::acquire_rename_data_mutation_lease(&disk.path(), bucket, &destination))
                .await
                .expect("ordinary repair physical publication and rollback drain"),
        );
        let lifecycle_writer = lifecycle
            .get_write_lock(DEADLINE)
            .await
            .expect("bucket lifecycle writer proceeds after the real repair worker exits");
        drop(lifecycle_writer);
        assert!(
            retained,
            "ARTIFACT_REPAIR_TAIL_BARRIER: ordinary repair retains its lifecycle owner after waiter cancellation until physical publication drains"
        );
        assert_eq!(
            store
                .bucket_incarnation_id(bucket)
                .await
                .expect("read unchanged ordinary incarnation"),
            incarnation
        );
        // Cancellation may leave cleanup to a later repair; it must retain the original
        // committed version and bytes rather than authorizing a replacement PUT.
        let repaired = store
            .heal_object_with_proof(bucket, object, &version.to_string(), &heal_options())
            .await
            .expect("finish any remaining repair of the original ordinary version");
        assert!(repaired.error.is_none(), "remaining ordinary repair failed: {:?}", repaired.error);
        assert_exact_payload(&store, bucket, object, version).await;
        shutdown.cancel();
        assert!(ctx.wait_for_detached_mutations(DEADLINE).await);
    });
}
