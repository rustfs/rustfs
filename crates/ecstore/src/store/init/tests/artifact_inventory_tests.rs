#![cfg(unix)]

use super::{build_isolated_test_store, run_large_stack_async_test, without_storage_class_env};
use crate::{
    bucket::metadata_sys,
    disk::{Disk, local::artifact_inventory::ArtifactInventoryFrame},
    object_api::{ObjectOptions, PutObjReader, ShardIntegrityWriteMode, WriteCompletion},
    storage_api_contracts::{
        bucket::{BucketOperations as _, MakeBucketOptions},
        object::ObjectIO as _,
    },
};
use std::time::Duration;
use uuid::Uuid;

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_inventory_managed_put_zero_and_versions_are_complete() {
    assert_managed_put_inventory_is_complete(ShardIntegrityWriteMode::Legacy);
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_inventory_protected_managed_put_versions_are_complete() {
    assert_managed_put_inventory_is_complete(ShardIntegrityWriteMode::Protected);
}

fn assert_managed_put_inventory_is_complete(mode: ShardIntegrityWriteMode) {
    run_large_stack_async_test("artifact-inventory-managed", move || async move {
        let root = tempfile::tempdir().expect("managed inventory disk roots");
        let (_ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(root.path(), "inventory-managed", &[4])).await;
        metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let bucket = "artifact-inventory-managed";
        store
            .make_bucket(
                bucket,
                &MakeBucketOptions {
                    artifact_base_prefix: Some("outputs/".to_owned()),
                    versioning_enabled: true,
                    ..Default::default()
                },
            )
            .await
            .expect("actual managed versioned bucket");
        let incarnation = store.bucket_incarnation_id(bucket).await.expect("actual incarnation");
        let prefix = format!("outputs/74656e616e74/{}/{}/", Uuid::new_v4(), Uuid::new_v4());
        let keys = vec![
            (format!("{prefix}zero"), Vec::new()),
            (format!("{prefix}inline"), vec![0xA5; 1024]),
            (format!("{prefix}payload"), vec![0xA5; 1024 * 1024]),
        ];
        let mut expected = std::collections::BTreeSet::new();
        for (key, data) in &keys {
            let object = store
                .put_object(
                    bucket,
                    key,
                    &mut PutObjReader::from_vec(data.clone()),
                    &ObjectOptions {
                        versioned: true,
                        shard_integrity_write_mode: Some(mode),
                        write_completion: WriteCompletion::TailDrained,
                        ..Default::default()
                    },
                )
                .await
                .expect("real managed PUT including empty, inline and sharded payloads");
            expected.insert((key.clone(), object.version_id.expect("exact managed version")));
        }
        let identity = store
            .artifact_receiver_identity(bucket, incarnation, &prefix, true)
            .await
            .expect("metadata-derived identity");
        store
            .clone()
            .seal_artifact_attempt_writes(bucket, incarnation, &prefix)
            .await
            .expect("actual all-writes seal");
        for member in store.pools[0].disk_set[0].disk_inventory().await {
            let member = member.expect("configured local member");
            let Disk::Local(local) = member.as_ref() else {
                panic!("local fixture");
            };
            let local = local.get_disk();
            let id = local.artifact_disk_id().await.expect("fresh actual disk identity");
            let (output, mut input) = tokio::sync::mpsc::channel(2);
            let (result, frames) = tokio::join!(
                local.inventory_artifact_attempt(
                    id,
                    bucket,
                    &prefix,
                    identity,
                    tokio::time::Instant::now() + Duration::from_secs(30),
                    output
                ),
                async {
                    let mut frames = Vec::new();
                    while let Some(frame) = input.recv().await {
                        frames.push(frame);
                    }
                    frames
                }
            );
            assert!(result.is_ok(), "A03I1_REAL_MANAGED_PUT_SCAN: {result:?}");
            let actual = frames
                .iter()
                .filter_map(|frame| match frame {
                    ArtifactInventoryFrame::Entries { versions, .. } => Some(versions),
                    _ => None,
                })
                .flatten()
                .map(|version| (version.key.clone(), version.version_id))
                .collect::<std::collections::BTreeSet<_>>();
            assert_eq!(actual, expected, "A03I1_REAL_ZERO_INLINE_AND_SHARDED_VERSIONS");
            assert!(matches!(frames.last(), Some(ArtifactInventoryFrame::Complete { entries: 3, .. })));
        }
        shutdown.cancel();
    });
}

async fn inventory_fixture() -> (
    tempfile::TempDir,
    std::sync::Arc<crate::store::ECStore>,
    tokio_util::sync::CancellationToken,
    Uuid,
    String,
) {
    let root = tempfile::tempdir().expect("inventory aggregate roots");
    let (_ctx, store, shutdown) =
        without_storage_class_env(build_isolated_test_store(root.path(), "inventory-aggregate", &[4])).await;
    metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
    store
        .make_bucket(
            "inventory-aggregate",
            &MakeBucketOptions {
                artifact_base_prefix: Some("outputs/".into()),
                versioning_enabled: true,
                ..Default::default()
            },
        )
        .await
        .expect("managed bucket");
    let incarnation = store
        .bucket_incarnation_id("inventory-aggregate")
        .await
        .expect("actual incarnation");
    let prefix = format!("outputs/74656e616e74/{}/{}/", Uuid::new_v4(), Uuid::new_v4());
    store
        .put_object(
            "inventory-aggregate",
            &format!("{prefix}object"),
            &mut PutObjReader::from_vec(vec![1; 1024]),
            &ObjectOptions {
                versioned: true,
                write_completion: WriteCompletion::TailDrained,
                ..Default::default()
            },
        )
        .await
        .expect("actual managed object");
    store
        .clone()
        .seal_artifact_attempt_writes("inventory-aggregate", incarnation, &prefix)
        .await
        .expect("actual two-domain seal");
    (root, store, shutdown, incarnation, prefix)
}

async fn aggregate(
    store: std::sync::Arc<crate::store::ECStore>,
    incarnation: Uuid,
    prefix: &str,
) -> (
    crate::error::Result<()>,
    Vec<crate::bucket::artifact::inventory::ArtifactAttemptInventoryFrame>,
) {
    let (output, mut receiver) = tokio::sync::mpsc::channel(1);
    let drain = async {
        let mut frames = Vec::new();
        while let Some(frame) = receiver.recv().await {
            frames.push(frame);
        }
        frames
    };
    tokio::join!(
        store.inventory_artifact_attempt(
            "inventory-aggregate",
            incarnation,
            prefix,
            tokio::time::Instant::now() + Duration::from_secs(30),
            output
        ),
        drain
    )
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_inventory_all_members_required() {
    run_large_stack_async_test("artifact-inventory-all-members", || async {
        use crate::bucket::artifact::inventory::ArtifactAttemptInventoryFrame;
        let (_root, store, shutdown, incarnation, prefix) = inventory_fixture().await;
        let (result, frames) = aggregate(store.clone(), incarnation, &prefix).await;
        result.expect("all four real sealed disks scanned");
        let observed: std::collections::BTreeSet<_> = frames
            .iter()
            .filter_map(|frame| match frame {
                ArtifactAttemptInventoryFrame::Observations {
                    endpoint,
                    disk_id,
                    versions,
                } => {
                    assert_eq!(versions.len(), 1);
                    assert_eq!(versions[0].key, format!("{prefix}object"));
                    Some((endpoint.clone(), *disk_id))
                }
                _ => None,
            })
            .collect();
        assert_eq!(observed.len(), 4, "A03I2_ALL_MEMBERS");
        assert!(
            matches!(
                frames.last(),
                Some(ArtifactAttemptInventoryFrame::Complete {
                    members: 4,
                    observations: 4
                })
            ),
            "A03I2_ALL_MEMBERS"
        );
        let original = store.pools[0].disk_set[0].disks.write().await[3].take();
        let (denied, frames) = aggregate(store.clone(), incarnation, &prefix).await;
        assert!(denied.is_err(), "one missing configured member is incomplete");
        assert!(
            !frames
                .iter()
                .any(|frame| matches!(frame, ArtifactAttemptInventoryFrame::Complete { .. }))
        );
        store.pools[0].disk_set[0].disks.write().await[3] = original;
        shutdown.cancel();
    });
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_inventory_membership_change_is_incomplete() {
    run_large_stack_async_test("artifact-inventory-membership", || async {
        use crate::bucket::artifact::inventory::ArtifactAttemptInventoryFrame;
        let (_root, store, shutdown, incarnation, prefix) = inventory_fixture().await;
        let original = store.pools[0].disk_set[0].disk_inventory().await[0]
            .clone()
            .expect("first member");
        let Disk::Local(local) = original.as_ref() else {
            panic!("local fixture");
        };
        let replacement = std::sync::Arc::new(Disk::Local(local.clone()));
        let (output, mut receiver) = tokio::sync::mpsc::channel(1);
        let owned = store.clone();
        let worker = tokio::spawn(async move {
            owned
                .inventory_artifact_attempt(
                    "inventory-aggregate",
                    incarnation,
                    &prefix,
                    tokio::time::Instant::now() + Duration::from_secs(30),
                    output,
                )
                .await
        });
        assert!(matches!(receiver.recv().await, Some(ArtifactAttemptInventoryFrame::Observations { .. })));
        store.pools[0].disk_set[0].disks.write().await[0] = Some(replacement);
        let mut complete = false;
        while let Some(frame) = receiver.recv().await {
            complete |= matches!(frame, ArtifactAttemptInventoryFrame::Complete { .. });
        }
        let error = worker
            .await
            .expect("aggregate worker")
            .expect_err("same endpoint/UUID but changed captured Arc is incomplete");
        assert!(error.to_string().contains("membership changed"), "{error}");
        assert!(!complete);
        store.pools[0].disk_set[0].disks.write().await[0] = Some(original);
        shutdown.cancel();
    });
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_inventory_readonly_authority_cold_cache_and_queued_writer() {
    run_large_stack_async_test("artifact-inventory-readonly-authority", || async {
        use crate::{
            disk::{BUCKET_META_PREFIX, DiskAPI as _, RUSTFS_META_BUCKET},
            storage_api_contracts::namespace::NamespaceLocking as _,
        };
        let (_root, store, shutdown, incarnation, prefix) = inventory_fixture().await;
        let member = store.pools[0].disk_set[0].disk_inventory().await[0]
            .clone()
            .expect("real receiver");
        let Disk::Local(local) = member.as_ref() else {
            panic!("local");
        };
        let expected = local.artifact_disk_id().await.expect("actual disk");
        let _lifecycle = store
            .acquire_bucket_lifecycle_read_lock("inventory-aggregate")
            .await
            .expect("coordinator lifecycle owner");
        metadata_sys::remove_bucket_metadata_in(&store.ctx, "inventory-aggregate")
            .await
            .expect("cold metadata cache");
        let lock = store
            .new_ns_lock("inventory-aggregate", "inventory-aggregate")
            .await
            .expect("business bucket namespace");
        let reader = lock
            .get_read_lock(crate::set_disk::get_lock_acquire_timeout())
            .await
            .expect("existing namespace reader");
        let mut writer = Box::pin(lock.get_write_lock(crate::set_disk::get_lock_acquire_timeout()));
        assert!(
            matches!(futures::poll!(writer.as_mut()), std::task::Poll::Pending),
            "real queued business bucket WRITE"
        );
        let (output, mut frames) = tokio::sync::mpsc::channel(1);
        let drain = async { while frames.recv().await.is_some() {} };
        let endpoint = member.endpoint().to_string();
        let receive = store.inventory_local_artifact_attempt(
            (&endpoint, expected),
            ("inventory-aggregate", incarnation),
            &prefix,
            rustfs_protos::proto_gen::node_service::ArtifactInventoryPhase::ArtifactInventoryScan,
            tokio::time::Instant::now() + Duration::from_secs(2),
            output,
        );
        let (result, ()) = tokio::join!(receive, drain);
        result.expect("A03I2_READONLY_AUTHORITY_NO_REENTRY");
        drop(writer);
        drop(reader);
        drop(_lifecycle);
        // A legacy physical bucket lacking both authoritative records must
        // stay legacy after rejected inventory; no implicit getter migration.
        store
            .make_bucket(
                "inventory-legacy",
                &MakeBucketOptions {
                    versioning_enabled: true,
                    ..Default::default()
                },
            )
            .await
            .expect("ordinary legacy fixture");
        let members = store.pools[0].disk_set[0].disk_inventory().await;
        for member in members.iter().flatten() {
            for file in [".metadata.bin", ".bucket-incarnation"] {
                let path = member
                    .path()
                    .join(RUSTFS_META_BUCKET)
                    .join(BUCKET_META_PREFIX)
                    .join("inventory-legacy")
                    .join(file);
                if path.exists() {
                    std::fs::remove_dir_all(&path).expect("remove fixture-only creation facts");
                }
            }
        }
        metadata_sys::remove_bucket_metadata_in(&store.ctx, "inventory-legacy")
            .await
            .expect("cold legacy cache");
        let (output, mut frames) = tokio::sync::mpsc::channel(1);
        let drain = async { while frames.recv().await.is_some() {} };
        let (denied, ()) = tokio::join!(
            store.clone().inventory_artifact_attempt(
                "inventory-legacy",
                Uuid::new_v4(),
                &prefix,
                tokio::time::Instant::now() + Duration::from_secs(2),
                output
            ),
            drain
        );
        assert!(denied.is_err());
        for member in members.iter().flatten() {
            for file in [".metadata.bin", ".bucket-incarnation"] {
                assert!(
                    !member
                        .path()
                        .join(RUSTFS_META_BUCKET)
                        .join(BUCKET_META_PREFIX)
                        .join("inventory-legacy")
                        .join(file)
                        .exists(),
                    "A03I2_READONLY_NO_MIGRATION"
                );
            }
        }
        shutdown.cancel();
    });
}
