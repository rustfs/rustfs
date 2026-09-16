// Copyright 2026 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![recursion_limit = "256"]

use rustfs_heal::heal::{
    outcome::HealObjectDisposition,
    storage::{ECStoreHealStorage, HealStorageAPI},
};
use rustfs_heal_contracts::heal_channel::{HealOpts, HealScanMode};
use rustfs_test_utils::TestECStoreEnv;
use serial_test::serial;
use tokio::io::AsyncReadExt as _;

mod storage_api;
use storage_api::integration::{
    DiskAPI, ObjectIO, ObjectOptions, PutObjReader, ReadOptions, ShardIntegrityWriteMode, WriteCompletion,
};

#[tokio::test]
#[serial]
async fn legacy_repair_reports_execution_without_strong_receipt() {
    let root = tempfile::tempdir().expect("legacy receipt fixture");
    let env = TestECStoreEnv::builder()
        .base_dir(root.path())
        .prefix("legacy_receipt")
        .build()
        .await;
    let bucket = "legacy-receipt";
    let object = "missing-shard";
    env.make_bucket(bucket, false).await;
    let expected = vec![0x5c; 1024 * 1024 + 37];
    env.ecstore
        .put_object(
            bucket,
            object,
            &mut PutObjReader::from_vec(expected.clone()),
            &ObjectOptions {
                shard_integrity_write_mode: Some(ShardIntegrityWriteMode::Legacy),
                // Disk inspection and fault injection require every shard rename to finish.
                write_completion: WriteCompletion::TailDrained,
                ..Default::default()
            },
        )
        .await
        .expect("legacy object");
    let set = env.ecstore.pools[0].get_disks(0);
    let disks = set.disks.read().await.iter().flatten().cloned().collect::<Vec<_>>();
    let meta = disks[0]
        .read_version("", bucket, object, "", &ReadOptions::default())
        .await
        .expect("metadata");
    tokio::fs::remove_file(
        env.disk_paths[0]
            .join(bucket)
            .join(object)
            .join(meta.data_dir.expect("external directory").to_string())
            .join("part.1"),
    )
    .await
    .expect("remove one shard");
    let result = ECStoreHealStorage::new(env.ecstore.clone())
        .heal_object_with_receipt(
            bucket,
            object,
            None,
            &HealOpts {
                scan_mode: HealScanMode::Deep,
                ..Default::default()
            },
        )
        .await
        .expect("legacy repair");
    assert!(result.error.is_none(), "{:?}", result.error);
    assert_eq!(result.item.drives_healed(), Some(1));
    assert!(!result.item.integrity_verified);
    assert!(result.receipt.is_none(), "physical repair does not prove original identity");
    let storage = ECStoreHealStorage::new(env.ecstore.clone());
    let incarnation = storage.admit_bucket_incarnation(bucket).await.expect("bucket admission");
    let scoped = storage
        .heal_object_at_incarnation(
            bucket,
            object,
            None,
            incarnation,
            &HealOpts {
                scan_mode: HealScanMode::Deep,
                ..Default::default()
            },
        )
        .await
        .expect("incarnation-bound legacy scan");
    assert!(scoped.error.is_none(), "{:?}", scoped.error);
    assert!(!scoped.item.integrity_verified);
    assert!(scoped.receipt.is_none(), "bucket admission cannot certify legacy shard integrity");
    let mut reader = env
        .ecstore
        .get_object_reader(bucket, object, None, Default::default(), &ObjectOptions::default())
        .await
        .expect("read repaired object");
    let mut actual = Vec::new();
    reader.stream.read_to_end(&mut actual).await.expect("exact recovered body");
    assert_eq!(actual, expected);
}

#[tokio::test]
#[serial]
async fn receipt_requires_independent_deep_verification() {
    let root = tempfile::tempdir().expect("receipt fixture");
    let env = TestECStoreEnv::builder()
        .base_dir(root.path())
        .prefix("shard_identity_receipt")
        .build()
        .await;
    let bucket = "shard-identity-receipt";
    env.make_bucket(bucket, false).await;
    let set = env.ecstore.pools[0].get_disks(0);
    let disks = set.disks.read().await.iter().flatten().cloned().collect::<Vec<_>>();
    let storage = ECStoreHealStorage::new(env.ecstore.clone());
    for corrupt_count in [1usize, 3] {
        let object = format!("target-{corrupt_count}");
        let donor = format!("donor-{corrupt_count}");
        for (name, byte) in [(&object, 0x3c), (&donor, 0xa9)] {
            env.ecstore
                .put_object(
                    bucket,
                    name,
                    &mut PutObjReader::from_vec(vec![byte; 1024 * 1024 + 123]),
                    &ObjectOptions {
                        shard_integrity_write_mode: Some(ShardIntegrityWriteMode::Protected),
                        no_lock: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("commit all fixture shards");
        }
        let mut target_meta = Vec::new();
        let mut donor_meta = Vec::new();
        for disk in &disks {
            target_meta.push(
                disk.read_version("", bucket, &object, "", &ReadOptions::default())
                    .await
                    .expect("target metadata"),
            );
            donor_meta.push(
                disk.read_version("", bucket, &donor, "", &ReadOptions::default())
                    .await
                    .expect("donor metadata"),
            );
        }
        let mut retained = Vec::new();
        for (slot, target) in target_meta.iter().enumerate() {
            let target_path = env.disk_paths[slot]
                .join(bucket)
                .join(&object)
                .join(target.data_dir.expect("target directory").to_string())
                .join("part.1");
            let original = tokio::fs::read(&target_path).await.expect("original shard");
            if target.erasure.index <= corrupt_count {
                let donor_slot = donor_meta
                    .iter()
                    .position(|part| part.erasure.index == target.erasure.index)
                    .expect("same donor coding index");
                let donor_path = env.disk_paths[donor_slot]
                    .join(bucket)
                    .join(&donor)
                    .join(donor_meta[donor_slot].data_dir.expect("donor directory").to_string())
                    .join("part.1");
                let replacement = tokio::fs::read(donor_path).await.expect("complete donor shard");
                assert_eq!(replacement.len(), original.len());
                tokio::fs::write(&target_path, replacement)
                    .await
                    .expect("replace intact shard");
            } else {
                retained.push((target_path, original));
            }
        }
        let normal = storage
            .heal_object_with_receipt(
                bucket,
                &object,
                None,
                &HealOpts {
                    no_lock: true,
                    scan_mode: HealScanMode::Normal,
                    ..Default::default()
                },
            )
            .await
            .expect("normal presence scan");
        let normal_receipt = normal.receipt.expect("a normal presence scan should certify metadata health");
        assert_eq!(
            normal_receipt.disposition,
            HealObjectDisposition::MetadataHealthy,
            "a presence scan must not be promoted to payload VerifiedHealthy"
        );
        let result = storage
            .heal_object_with_receipt(
                bucket,
                &object,
                None,
                &HealOpts {
                    no_lock: true,
                    scan_mode: HealScanMode::Deep,
                    ..Default::default()
                },
            )
            .await;
        if corrupt_count == 1 {
            let result = result.expect("recoverable repair");
            assert!(result.error.is_none());
            assert_eq!(
                result.receipt.expect("verified repair receipt").disposition,
                HealObjectDisposition::Repaired,
                "presence alone must not produce VerifiedHealthy"
            );
        } else if let Ok(result) = result {
            assert!(result.error.is_some(), "below quorum cannot be a success");
            assert!(result.receipt.is_none(), "no completion receipt without authoritative sources");
        }
        for (path, original) in retained {
            assert_eq!(tokio::fs::read(path).await.expect("retained correct shard"), original);
        }
    }
}

#[test]
#[serial]
fn admin_pool_set_repairs_five_shards_and_retains_exact_terminal_after_restart() {
    std::thread::Builder::new()
        .stack_size(8 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("heal fixture runtime")
                .block_on(async {
                    use rustfs_heal::heal::{
                        manager::{HealConfig, HealManager},
                        task::{HealOptions, HealPriority, HealRequest, HealTaskStatus, HealType},
                    };
                    use rustfs_heal_contracts::heal_channel::HealRequestSource;
                    use std::sync::Arc;
                    use std::time::Duration;
                    use storage_api::integration::WriteCompletion;

                    let root = tempfile::tempdir().expect("administrator pool/set fixture");
                    let env = TestECStoreEnv::builder()
                        .base_dir(root.path())
                        .prefix("admin_pool_set_receipts")
                        .build()
                        .await;
                    let storage = Arc::new(ECStoreHealStorage::new(env.ecstore.clone()));
                    let set = env.ecstore.pools[0].get_disks(0);
                    let disks = set.disks.read().await.iter().flatten().cloned().collect::<Vec<_>>();
                    let mut faults = Vec::new();
                    for (bucket, count) in [("admin-first", 2), ("admin-second", 3)] {
                        env.make_bucket(bucket, false).await;
                        for index in 0..count {
                            let object = format!("object-{index}");
                            let body = vec![u8::try_from(index + 1).expect("fixture byte"); 1024 * 1024 + 37];
                            env.ecstore
                                .put_object(
                                    bucket,
                                    &object,
                                    &mut PutObjReader::from_vec(body.clone()),
                                    &ObjectOptions {
                                        write_completion: WriteCompletion::TailDrained,
                                        shard_integrity_write_mode: Some(ShardIntegrityWriteMode::Protected),
                                        ..Default::default()
                                    },
                                )
                                .await
                                .expect("commit every fixture shard before fault injection");
                            let meta = disks[0]
                                .read_version("", bucket, &object, "", &ReadOptions::default())
                                .await
                                .expect("fixture metadata");
                            let path = env.disk_paths[0]
                                .join(bucket)
                                .join(&object)
                                .join(meta.data_dir.expect("external shard").to_string())
                                .join("part.1");
                            let bytes = tokio::fs::read(&path).await.expect("original shard bytes");
                            tokio::fs::remove_file(&path).await.expect("inject missing shard");
                            faults.push((bucket.to_owned(), object, path, bytes, body));
                        }
                    }
                    let config = HealConfig {
                        enable_auto_heal: false,
                        ..Default::default()
                    };
                    let manager = HealManager::new(storage.clone(), Some(config.clone()));
                    manager.start().await.expect("start scoped heal manager");
                    let mut request = HealRequest::new(
                        HealType::ErasureSet {
                            buckets: Vec::new(),
                            set_disk_id: "pool_0_set_0".to_owned(),
                        },
                        HealOptions {
                            recursive: true,
                            scan_mode: HealScanMode::Deep,
                            pool_index: Some(0),
                            set_index: Some(0),
                            timeout: Some(Duration::from_secs(60)),
                            ..Default::default()
                        },
                        HealPriority::High,
                    );
                    request.source = HealRequestSource::Admin;
                    let token = request.id.clone();
                    manager
                        .submit_heal_request(request)
                        .await
                        .expect("durably admit actual all-buckets scope");
                    let terminal = tokio::time::timeout(Duration::from_secs(60), async {
                        loop {
                            let report = manager.get_task_report(&token).await.expect("same-token report");
                            if matches!(
                                report.status,
                                HealTaskStatus::Completed | HealTaskStatus::Failed { .. } | HealTaskStatus::Timeout
                            ) {
                                break report;
                            }
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                    })
                    .await
                    .expect("terminal deadline");
                    assert_eq!(terminal.status, HealTaskStatus::Completed);
                    let outcome = terminal.outcome.as_deref().expect("canonical terminal");
                    assert_eq!((outcome.counters.processed, outcome.counters.healed), (5, 5), "{outcome:?}");
                    assert_eq!(outcome.objects.len(), 5);
                    assert!(!outcome.objects_truncated);
                    let progress = terminal.progress.as_ref().expect("terminal progress");
                    assert_eq!((progress.objects_scanned, progress.objects_healed), (5, 5));
                    for (bucket, object, path, original, body) in &faults {
                        assert_eq!(&tokio::fs::read(path).await.expect("rebuilt physical shard"), original);
                        let receipt = outcome
                            .objects
                            .iter()
                            .find(|receipt| receipt.identity.bucket == *bucket && receipt.identity.object == *object)
                            .expect("one receipt per exact fault");
                        assert_eq!(receipt.disposition, HealObjectDisposition::Repaired);
                        assert_eq!((receipt.identity.pool_index, receipt.identity.set_index), (Some(0), Some(0)));
                        assert!(receipt.identity.version_id.is_some());
                        assert_eq!(
                            receipt.identity.bucket_incarnation_id,
                            Some(storage.admit_bucket_incarnation(bucket).await.expect("original bucket"))
                        );
                        let mut reader = env
                            .ecstore
                            .get_object_reader(bucket, object, None, Default::default(), &ObjectOptions::default())
                            .await
                            .expect("repaired S3 contents");
                        let mut actual = Vec::new();
                        reader.stream.read_to_end(&mut actual).await.expect("read repaired object");
                        assert_eq!(&actual, body);
                    }
                    manager.stop().await.expect("stop completed manager");
                    drop(manager);
                    let restarted = HealManager::new(storage, Some(config));
                    restarted.start().await.expect("recover retained terminal");
                    let restored = restarted.get_task_report(&token).await.expect("same token after restart");
                    assert_eq!(restored.status, terminal.status);
                    assert_eq!(restored.outcome, terminal.outcome);
                    assert_eq!(
                        serde_json::to_value(&restored.progress).expect("restored progress"),
                        serde_json::to_value(&terminal.progress).expect("original progress")
                    );
                    assert_eq!(restarted.get_queue_length().await, 0);
                    restarted.stop().await.expect("stop restarted manager");
                });
        })
        .expect("heal fixture thread")
        .join()
        .expect("heal fixture result");
}

#[test]
#[serial]
fn admin_normal_protected_repairs_report_repaired_for_data_and_parity() {
    std::thread::Builder::new()
        .stack_size(8 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("heal fixture runtime")
                .block_on(async {
                    use rustfs_heal::heal::{
                        manager::{HealConfig, HealManager},
                        task::{HealOptions, HealPriority, HealRequest, HealTaskStatus, HealType},
                    };
                    use rustfs_heal_contracts::heal_channel::HealRequestSource;
                    use std::sync::Arc;
                    use std::time::Duration;
                    use storage_api::integration::WriteCompletion;

                    let root = tempfile::tempdir().expect("normal repair fixture");
                    let env = TestECStoreEnv::builder()
                        .base_dir(root.path())
                        .prefix("admin_normal_repair_receipts")
                        .build()
                        .await;
                    let bucket = "admin-normal-repair";
                    env.make_bucket(bucket, false).await;
                    let set = env.ecstore.pools[0].get_disks(0);
                    let disks = set.disks.read().await.iter().flatten().cloned().collect::<Vec<_>>();
                    let mut faults = Vec::new();
                    for (object, want_data, byte) in [("missing-data", true, 0x31_u8), ("missing-parity", false, 0x72_u8)] {
                        let body = vec![byte; 1024 * 1024 + 37];
                        env.ecstore
                            .put_object(
                                bucket,
                                object,
                                &mut PutObjReader::from_vec(body.clone()),
                                &ObjectOptions {
                                    write_completion: WriteCompletion::TailDrained,
                                    shard_integrity_write_mode: Some(ShardIntegrityWriteMode::Protected),
                                    ..Default::default()
                                },
                            )
                            .await
                            .expect("commit protected fixture");
                        let mut metadata = Vec::new();
                        for disk in &disks {
                            metadata.push(
                                disk.read_version("", bucket, object, "", &ReadOptions::default())
                                    .await
                                    .expect("fixture metadata"),
                            );
                        }
                        let target_slot = metadata
                            .iter()
                            .position(|part| (part.erasure.index <= part.erasure.data_blocks) == want_data)
                            .expect("fixture must expose both data and parity shards");
                        let target = &metadata[target_slot];
                        let path = env.disk_paths[target_slot]
                            .join(bucket)
                            .join(object)
                            .join(target.data_dir.expect("external shard").to_string())
                            .join("part.1");
                        let original = tokio::fs::read(&path).await.expect("original shard bytes");
                        tokio::fs::remove_file(&path).await.expect("inject missing shard");
                        faults.push((object.to_owned(), path, original, body));
                    }

                    let storage = Arc::new(ECStoreHealStorage::new(env.ecstore.clone()));
                    let manager = HealManager::new(
                        storage.clone(),
                        Some(HealConfig {
                            enable_auto_heal: false,
                            ..Default::default()
                        }),
                    );
                    manager.start().await.expect("start heal manager");
                    let mut request = HealRequest::new(
                        HealType::ErasureSet {
                            buckets: Vec::new(),
                            set_disk_id: "pool_0_set_0".to_owned(),
                        },
                        HealOptions {
                            recursive: true,
                            scan_mode: HealScanMode::Normal,
                            pool_index: Some(0),
                            set_index: Some(0),
                            timeout: Some(Duration::from_secs(60)),
                            ..Default::default()
                        },
                        HealPriority::High,
                    );
                    request.source = HealRequestSource::Admin;
                    let token = request.id.clone();
                    manager
                        .submit_heal_request(request)
                        .await
                        .expect("admit normal all-buckets heal");
                    let terminal = tokio::time::timeout(Duration::from_secs(60), async {
                        loop {
                            let report = manager.get_task_report(&token).await.expect("same-token report");
                            if matches!(
                                report.status,
                                HealTaskStatus::Completed | HealTaskStatus::Failed { .. } | HealTaskStatus::Timeout
                            ) {
                                break report;
                            }
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                    })
                    .await
                    .expect("terminal deadline");
                    assert_eq!(terminal.status, HealTaskStatus::Completed);
                    let outcome = terminal.outcome.as_deref().expect("canonical terminal");
                    assert_eq!(
                        (
                            outcome.counters.processed,
                            outcome.counters.healed,
                            outcome.counters.unchanged,
                            outcome.counters.skipped,
                            outcome.counters.failed,
                            outcome.counters.unknown,
                        ),
                        (2, 2, 0, 0, 0, 0),
                        "normal protected repairs must be canonical: {outcome:?}"
                    );
                    assert_eq!(outcome.objects.len(), 2);
                    for (object, path, original, body) in &faults {
                        assert_eq!(&tokio::fs::read(path).await.expect("rebuilt physical shard"), original);
                        let receipt = outcome
                            .objects
                            .iter()
                            .find(|receipt| receipt.identity.bucket == bucket && receipt.identity.object == *object)
                            .expect("one receipt per repaired object");
                        assert_eq!(receipt.disposition, HealObjectDisposition::Repaired);
                        assert_eq!((receipt.identity.pool_index, receipt.identity.set_index), (Some(0), Some(0)));
                        assert!(receipt.identity.version_id.is_some());
                        assert_eq!(
                            receipt.identity.bucket_incarnation_id,
                            Some(storage.admit_bucket_incarnation(bucket).await.expect("original bucket"))
                        );
                        let mut reader = env
                            .ecstore
                            .get_object_reader(bucket, object, None, Default::default(), &ObjectOptions::default())
                            .await
                            .expect("read repaired object");
                        let mut actual = Vec::new();
                        reader.stream.read_to_end(&mut actual).await.expect("read repaired body");
                        assert_eq!(&actual, body);
                    }
                    manager.stop().await.expect("stop heal manager");
                });
        })
        .expect("heal fixture thread")
        .join()
        .expect("heal fixture result");
}
