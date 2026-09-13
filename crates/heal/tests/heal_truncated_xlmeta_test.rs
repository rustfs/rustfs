// Copyright 2024 RustFS Team
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

use http::HeaderMap;
use rustfs_heal::heal::{
    outcome::{HealObjectDisposition, HealTraversalCoverage},
    storage::{ECStoreHealStorage, HealObjectOptions as ObjectOptions, HealPutObjReader as PutObjReader},
    task::{HealOptions, HealPriority, HealRequest, HealTask, HealType},
};
use rustfs_heal_contracts::heal_channel::{DriveState, HealScanMode};
use std::{sync::Arc, time::Duration};
use tokio::io::AsyncReadExt as _;

mod storage_api;
use storage_api::integration::{
    DiskAPI as _, DiskError, DiskOption, Endpoint, NamespaceLocking as _, ObjectIO as _, ReadOptions, new_disk,
};

fn deep_heal_task(storage: &Arc<ECStoreHealStorage>, bucket: &str, object: &str, dry_run: bool) -> HealTask {
    HealTask::from_request(
        HealRequest::new(
            HealType::Object {
                bucket: bucket.to_owned(),
                object: object.to_owned(),
                version_id: None,
            },
            HealOptions {
                scan_mode: HealScanMode::Deep,
                dry_run,
                ..Default::default()
            },
            HealPriority::Normal,
        ),
        storage.clone(),
    )
}

#[tokio::test]
async fn deep_heal_rebuilds_truncated_xlmeta_with_authoritative_outcomes() {
    let temp = tempfile::tempdir().expect("create caller-owned disks");
    let env = rustfs_test_utils::TestECStoreEnv::builder()
        .disk_count(16)
        .base_dir(temp.path())
        .build()
        .await;
    let storage = Arc::new(ECStoreHealStorage::new(env.ecstore.clone()));
    let bucket = "truncated-xlmeta";
    env.make_bucket(bucket, false).await;
    let payload = vec![0x7b; 4 * 1024 * 1024];
    let mut endpoint = Endpoint::try_from(env.disk_paths[0].to_str().expect("UTF-8 disk path")).expect("target endpoint");
    endpoint.set_pool_index(0);
    endpoint.set_set_index(0);
    endpoint.set_disk_index(0);
    let disk = new_disk(&endpoint, &DiskOption::default())
        .await
        .expect("open target disk inspector");
    let read_options = ReadOptions {
        read_data: true,
        ..Default::default()
    };

    for damage in ["length-prefix", "metadata-body", "crc-tail", "versioned"] {
        let versioned = damage == "versioned";
        let bucket = if versioned { "truncated-xlmeta-versioned" } else { bucket };
        if versioned {
            env.make_bucket(bucket, true).await;
        }
        let object = format!("{damage}/object.bin");
        let mut reader = PutObjReader::from_vec(payload.clone());
        env.ecstore
            .put_object(
                bucket,
                &object,
                &mut reader,
                &ObjectOptions {
                    versioned,
                    ..Default::default()
                },
            )
            .await
            .expect("write source object");
        // The namespace fence waits for the detached PUT publication owner;
        // no GET/HEAD can repair the target before the explicit heal.
        let lock = env
            .ecstore
            .new_ns_lock(bucket, &object)
            .await
            .expect("fixture namespace lock");
        let settled = lock
            .get_write_lock(Duration::from_secs(30))
            .await
            .expect("PUT publication must finish");
        let original_info = disk
            .read_version("", bucket, &object, "", &read_options)
            .await
            .expect("read original metadata");
        assert_eq!(original_info.version_id.is_some(), versioned);
        assert_eq!((original_info.erasure.data_blocks, original_info.erasure.parity_blocks), (12, 4));
        let data_dir = original_info.data_dir.expect("non-inline object has a data directory");
        let target_dir = env.disk_paths[0].join(bucket).join(&object);
        let target_meta = target_dir.join("xl.meta");
        let original_part = tokio::fs::read(target_dir.join(data_dir.to_string()).join("part.1"))
            .await
            .expect("read original shard");
        let mut original_metadata = Vec::new();
        for path in &env.disk_paths {
            original_metadata.push(
                tokio::fs::read(path.join(bucket).join(&object).join("xl.meta"))
                    .await
                    .expect("snapshot every member"),
            );
        }
        let original = &original_metadata[0];
        let metadata_len = usize::try_from(u32::from_be_bytes(original[9..13].try_into().expect("bin32 length")))
            .expect("metadata length fits usize");
        assert_eq!(original.len(), 13 + metadata_len + 5, "fixture must exclude inline data");
        let cut = match damage {
            "length-prefix" => 12,
            "metadata-body" | "versioned" => 13 + metadata_len / 2,
            "crc-tail" => original.len() - 1,
            _ => unreachable!("fixed damage matrix"),
        };
        tokio::fs::write(&target_meta, &original[..cut])
            .await
            .expect("inject exact truncation");
        drop(settled);
        let read_error = disk
            .read_version("", bucket, &object, "", &read_options)
            .await
            .expect_err("target must be unreadable before heal");

        let dry_run = deep_heal_task(&storage, bucket, &object, true);
        dry_run.execute().await.expect("dry-run traversal completes");
        assert_eq!(tokio::fs::read(&target_meta).await.expect("read dry-run target"), original[..cut]);
        assert_eq!(dry_run.get_outcome().await.objects[0].disposition, HealObjectDisposition::DryRunObserved);

        let task = deep_heal_task(&storage, bucket, &object, false);
        task.execute().await.expect("deep heal traversal completes");
        let outcome = task.get_outcome().await;
        assert_eq!(outcome.coverage, HealTraversalCoverage::Complete);
        assert_eq!(outcome.counters.processed, 1);
        assert_eq!(outcome.counters.healed, 1, "{damage}: {outcome:?}");
        assert_eq!(outcome.counters.unknown, 0);
        assert_eq!(outcome.counters.skipped, 0);
        assert_eq!(outcome.counters.failed, 0);
        assert_eq!(outcome.counters.attempt_failures, 0);
        assert_eq!(outcome.objects[0].disposition, HealObjectDisposition::Repaired);
        assert_eq!(read_error, DiskError::FileCorrupt);
        let results = task.get_result_items().await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].before.drives.len(), 16);
        assert_eq!(results[0].after.drives.len(), 16);
        assert_eq!(results[0].before.drives[0].state, DriveState::Corrupt.to_string());
        assert!(
            results[0]
                .after
                .drives
                .iter()
                .all(|drive| drive.state == DriveState::Ok.to_string())
        );

        let repaired = disk
            .read_version("", bucket, &object, "", &read_options)
            .await
            .expect("physical metadata must decode after heal");
        assert_eq!(repaired.version_id, original_info.version_id);
        assert_eq!(repaired.mod_time, original_info.mod_time);
        assert_eq!(repaired.data_dir, original_info.data_dir);
        assert_eq!(repaired.parts, original_info.parts);
        assert_eq!(repaired.erasure, original_info.erasure);
        assert_eq!(
            tokio::fs::read(target_dir.join(data_dir.to_string()).join("part.1"))
                .await
                .expect("read rebuilt shard"),
            original_part
        );
        for (index, path) in env.disk_paths.iter().enumerate().skip(1) {
            assert_eq!(
                tokio::fs::read(path.join(bucket).join(&object).join("xl.meta"))
                    .await
                    .expect("read healthy member"),
                original_metadata[index]
            );
        }

        let healed_bytes = tokio::fs::read(&target_meta).await.expect("read committed metadata");
        let repeat = deep_heal_task(&storage, bucket, &object, false);
        repeat.execute().await.expect("repeated heal completes");
        let repeat_outcome = repeat.get_outcome().await;
        assert_eq!(repeat_outcome.counters.healed, 0);
        assert_eq!(repeat_outcome.counters.unchanged, 1);
        assert_eq!(repeat_outcome.counters.unknown, 0);
        assert_eq!(tokio::fs::read(&target_meta).await.expect("read repeated-heal target"), healed_bytes);
        let mut reader = env
            .ecstore
            .get_object_reader(bucket, &object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("read healed object");
        let mut actual = Vec::new();
        reader.read_to_end(&mut actual).await.expect("read complete healed body");
        drop(reader);
        assert_eq!(actual, payload);

        // A future format is not evidence of corruption. Even with healthy
        // quorum, the target must remain unknown and must not be overwritten.
        let mut future = healed_bytes;
        future[4..6].copy_from_slice(&2_u16.to_le_bytes());
        tokio::fs::write(&target_meta, &future)
            .await
            .expect("install unsupported major version");
        let unsupported = deep_heal_task(&storage, bucket, &object, false);
        unsupported
            .execute()
            .await
            .expect("unsupported member permits traversal completion");
        let unknown = unsupported.get_outcome().await;
        assert_eq!(
            (
                unknown.counters.processed,
                unknown.counters.healed,
                unknown.counters.skipped,
                unknown.counters.unknown,
                unknown.counters.failed
            ),
            (1, 0, 1, 1, 0)
        );
        assert_eq!(unknown.objects[0].disposition, HealObjectDisposition::Unknown);
        assert_eq!(tokio::fs::read(&target_meta).await.expect("read unsupported metadata"), future);

        if damage == "metadata-body" {
            for damaged in [4, 5] {
                for (index, path) in env.disk_paths.iter().enumerate() {
                    let bytes = if index < damaged {
                        &original_metadata[index][..cut]
                    } else {
                        &original_metadata[index]
                    };
                    tokio::fs::write(path.join(bucket).join(&object).join("xl.meta"), bytes)
                        .await
                        .expect("install exact-quorum fixture");
                }
                let quorum_task = deep_heal_task(&storage, bucket, &object, false);
                let result = quorum_task.execute().await;
                let outcome = quorum_task.get_outcome().await;
                if damaged == 4 {
                    result.expect("twelve authoritative members must repair four damaged copies");
                    assert_eq!(outcome.counters.healed, 1);
                    assert_eq!(outcome.counters.unknown, 0);
                    assert!(
                        quorum_task.get_result_items().await[0]
                            .after
                            .drives
                            .iter()
                            .all(|drive| drive.state == DriveState::Ok.to_string())
                    );
                } else {
                    assert_eq!(outcome.counters.healed, 0, "eleven healthy members must not authorize repair");
                }
                for (index, path) in env.disk_paths.iter().enumerate() {
                    let actual = tokio::fs::read(path.join(bucket).join(&object).join("xl.meta"))
                        .await
                        .expect("inspect quorum fixture");
                    if index < damaged && damaged == 4 {
                        let mut endpoint =
                            Endpoint::try_from(path.to_str().expect("UTF-8 member path")).expect("member endpoint");
                        endpoint.set_pool_index(0);
                        endpoint.set_set_index(0);
                        endpoint.set_disk_index(index);
                        let member = new_disk(&endpoint, &DiskOption::default())
                            .await
                            .expect("open repaired member");
                        let info = member
                            .read_version("", bucket, &object, "", &read_options)
                            .await
                            .expect("every repaired member must decode");
                        assert_eq!(info.data_dir, original_info.data_dir);
                        assert_eq!(info.version_id, original_info.version_id);
                        assert_eq!(info.parts, original_info.parts);
                        assert_eq!(info.erasure.index, original_info.erasure.distribution[index]);
                        assert_ne!(actual, original_metadata[index][..cut]);
                    } else {
                        let expected = if index < damaged {
                            &original_metadata[index][..cut]
                        } else {
                            &original_metadata[index]
                        };
                        assert_eq!(actual, expected, "quorum-minus-one must preserve every copy");
                    }
                }
            }
        }
    }
}
