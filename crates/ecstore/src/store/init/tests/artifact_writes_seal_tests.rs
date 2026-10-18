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
        object::ObjectIO as _,
    },
    store::ECStore,
};
use std::{path::PathBuf, sync::Arc, time::Duration};
use uuid::Uuid;

const DEADLINE: Duration = Duration::from_secs(30);
const BUCKET: &str = "artifact-writes-seal";

fn prefix() -> String {
    format!("outputs/74656e616e74/{}/{}/", Uuid::new_v4(), Uuid::new_v4())
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
