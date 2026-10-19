#![cfg(unix)]

use super::{build_isolated_test_store, run_large_stack_async_test, without_storage_class_env};
use crate::{
    bucket::metadata_sys,
    disk::{Disk, local::artifact_inventory::ArtifactInventoryFrame},
    object_api::{ObjectOptions, PutObjReader, WriteCompletion},
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
    run_large_stack_async_test("artifact-inventory-managed", || async {
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
