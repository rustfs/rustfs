#![cfg(unix)]

use super::{build_isolated_test_store, run_large_stack_async_test, without_storage_class_env};
use crate::{
    bucket::{
        artifact::{ArtifactPutTarget, artifact_put_seal_record_path},
        metadata_sys,
    },
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

fn run_physical_repair_case(kind: RepairKind, cancel: bool) {
    // Retain the large fixture stack and the current-thread clock needed to expire disk wrappers.
    std::thread::Builder::new()
        .name(format!("artifact-repair-{kind:?}-{cancel}"))
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("physical repair runtime")
                .block_on(physical_repair_case(kind, cancel));
        })
        .expect("physical repair test thread")
        .join()
        .expect("physical repair test completes");
}

async fn physical_repair_case(kind: RepairKind, cancel: bool) {
    temp_env::async_with_vars(
        [
            (rustfs_config::ENV_DRIVE_MAX_TIMEOUT_DURATION, Some("60")),
            (crate::set_disk::ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("false")),
        ],
        async {
            if matches!(kind, RepairKind::Metadata) && !cancel {
                metadata_repair_disk_timeout_case().await;
                return;
            }
            let root = tempfile::tempdir().expect("physical repair fixture directory");
            let (ctx, store, shutdown) = open_store(root.path()).await;
            let bucket = "artifact-repair-physical";
            let incarnation = create_bucket(&store, bucket, true).await;
            let prefix = attempt_prefix();
            let object = format!("{prefix}image.png");
            let version = put_payload(&store, bucket, &object, kind).await;
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
                heal_store
                    .heal_object_with_proof(bucket, &heal_object, &version.to_string(), &heal_options())
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
                // The disk wrapper may fail while another disk restores quorum. Either result
                // must retain the physical owner's lock until this publisher actually exits.
                let _ = tokio::time::timeout(DEADLINE, repair)
                    .await
                    .expect("repair waiter returns after disk timeout")
                    .expect("repair task joins");
            }
            let record = artifact_put_seal_record_path(bucket, incarnation, &prefix);
            let lock = store
                .new_ns_lock(RUSTFS_META_BUCKET, &record)
                .await
                .expect("actual repair/seal coordination lock");
            let retained = lock.get_write_lock(Duration::from_secs(1)).await.is_err();
            let seal_store = store.clone();
            let seal_prefix = prefix.clone();
            let mut seal =
                tokio::spawn(async move { seal_store.seal_artifact_attempt_puts(bucket, incarnation, &seal_prefix).await });
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
            let repaired = store
                .heal_object_with_proof(bucket, &object, &version.to_string(), &heal_options())
                .await
                .expect("finish remaining repair after seal");
            assert!(repaired.error.is_none(), "remaining repair failed: {:?}", repaired.error);
            assert_exact_payload(&store, bucket, &object, version).await;
            shutdown.cancel();
            assert!(ctx.wait_for_detached_mutations(DEADLINE).await);
        },
    )
    .await;
}

async fn metadata_repair_disk_timeout_case() {
    use crate::storage_api_contracts::heal::HealOperations;

    let root = tempfile::tempdir().expect("metadata timeout fixture directory");
    let (ctx, store, shutdown) = open_store(root.path()).await;
    let bucket = "artifact-repair-meta-timeout";
    let incarnation = create_bucket(&store, bucket, true).await;
    let prefix = attempt_prefix();
    let object = format!("{prefix}image.png");
    let version = put_payload(&store, bucket, &object, RepairKind::Metadata).await;
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
    let mut seal = tokio::spawn(async move { seal_store.seal_artifact_attempt_puts(bucket, incarnation, &prefix).await });
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
    assert_exact_payload(&store, bucket, &object, version).await;
    shutdown.cancel();
    assert!(ctx.wait_for_detached_mutations(DEADLINE).await);
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_repair_rename_owner_survives_cancellation() {
    run_physical_repair_case(RepairKind::Rename, true);
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_repair_rename_owner_survives_disk_timeout() {
    run_physical_repair_case(RepairKind::Rename, false);
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_repair_metadata_owner_survives_cancellation() {
    run_physical_repair_case(RepairKind::Metadata, true);
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_repair_metadata_owner_survives_disk_timeout() {
    run_physical_repair_case(RepairKind::Metadata, false);
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
