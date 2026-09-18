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

//! B5-2 (rustfs/backlog#919): real-disk-wipe e2e regression suite proving the
//! B5-1 version-aware heal enumeration physically repairs OLD non-latest
//! versions and DELETE-MARKER-latest objects.
//!
//! These drive the REAL `ECStoreHealStorage` (not a mock) against a real 4-disk
//! `ECStore`, mirroring `heal_integration_test.rs`. Every test is `#[serial]`
//! and re-runs the full `heal_env` init (which sets the process-global
//! bucket-metadata-sys OnceCell) — under `cargo nextest` each test runs
//! in its own process so the OnceCell never collides.

#![recursion_limit = "256"]

use http::HeaderMap;
use rustfs_filemeta::{FileInfo, FileMeta};
use rustfs_heal::heal::{
    manager::{HealConfig, HealManager},
    outcome::HealObjectDisposition,
    storage::{
        ECStoreHealStorage, HealListItem, HealObjectOptions as ObjectOptions, HealPutObjReader as PutObjReader, HealStorageAPI,
    },
    task::{HealOptions, HealPriority, HealRequest, HealTaskStatus, HealType},
};
use rustfs_heal_contracts::heal_channel::{HealOpts, HealScanMode};
use serial_test::serial;
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tracing::info;
use walkdir::WalkDir;

mod storage_api;

use storage_api::integration::{
    BucketOperations, ECStore, MakeBucketOptions, NamespaceLocking as _, ObjectIO as _, ObjectOperations as _,
    ShardIntegrityWriteMode,
};

/// 256 KiB + change: large enough to be stored as non-inline erasure shards
/// (so each data version materializes as an on-disk `part.*` file we can assert
/// was physically restored on the wiped disk).
const NON_INLINE_TEST_DATA_SIZE: usize = 256 * 1024 + 137;

const SET_DISK_ID: &str = "pool_0_set_0";

fn versioned_test_data(seed: u8) -> Vec<u8> {
    (0..NON_INLINE_TEST_DATA_SIZE)
        .map(|idx| ((idx + seed as usize) % 251) as u8)
        .collect()
}

/// Build a real 4-disk `ECStore` + `ECStoreHealStorage` via the shared
/// rustfs-test-utils environment (backlog#1153 infra-1). Mirrors
/// `heal_integration_test::heal_env`.
async fn heal_env() -> (Vec<PathBuf>, Arc<ECStore>, Arc<ECStoreHealStorage>) {
    let env = rustfs_test_utils::TestECStoreEnv::builder()
        .prefix("rustfs_heal_b5_test")
        .build()
        .await;
    let heal_storage = Arc::new(ECStoreHealStorage::new(env.ecstore.clone()));
    (env.disk_paths, env.ecstore, heal_storage)
}

/// Create a bucket with S3 versioning ENABLED at creation time. Without this the
/// second PUT overwrites in place and DELETE removes the object outright — no
/// old versions and no delete-marker-latest would ever exist (empty-pass trap).
async fn create_versioned_bucket(ecstore: &Arc<ECStore>, bucket: &str) {
    (**ecstore)
        .make_bucket(
            bucket,
            &MakeBucketOptions {
                versioning_enabled: true,
                ..Default::default()
            },
        )
        .await
        .expect("failed to create versioned bucket");
}

async fn create_unversioned_bucket(ecstore: &Arc<ECStore>, bucket: &str) {
    (**ecstore)
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("failed to create unversioned bucket");
}

/// PUT a new version (versioned:true forces a fresh version id per write) and
/// return the created version id as a String.
async fn put_versioned(ecstore: &Arc<ECStore>, bucket: &str, object: &str, data: &[u8]) -> String {
    let mut reader = PutObjReader::from_vec(data.to_vec());
    let opts = ObjectOptions {
        versioned: true,
        shard_integrity_write_mode: Some(ShardIntegrityWriteMode::Protected),
        ..Default::default()
    };
    let info = (**ecstore)
        .put_object(bucket, object, &mut reader, &opts)
        .await
        .expect("versioned put_object failed");
    wait_for_put_tail(ecstore, bucket, object).await;
    info.version_id
        .map(|u| u.to_string())
        .expect("versioned put must return a version id")
}

async fn put_unversioned(ecstore: &Arc<ECStore>, bucket: &str, object: &str, data: &[u8]) {
    let mut reader = PutObjReader::from_vec(data.to_vec());
    (**ecstore)
        .put_object(
            bucket,
            object,
            &mut reader,
            &ObjectOptions {
                shard_integrity_write_mode: Some(ShardIntegrityWriteMode::Protected),
                ..Default::default()
            },
        )
        .await
        .expect("unversioned put_object failed");
    wait_for_put_tail(ecstore, bucket, object).await;
}

/// Create a delete-marker as the latest version (versioned:true, no version_id)
/// and return the delete-marker's version id.
async fn put_delete_marker(ecstore: &Arc<ECStore>, bucket: &str, object: &str) -> String {
    let opts = ObjectOptions {
        versioned: true,
        ..Default::default()
    };
    let info = (**ecstore)
        .delete_object(bucket, object, opts)
        .await
        .expect("versioned delete (delete-marker) failed");
    assert!(info.delete_marker, "delete on a versioned bucket must yield a delete marker");
    info.version_id
        .map(|u| u.to_string())
        .expect("delete marker must carry a version id")
}

/// On-disk object directory for a given disk: `<disk>/<bucket>/<object>/`.
fn object_dir(disk: &Path, bucket: &str, object: &str) -> PathBuf {
    disk.join(bucket).join(object)
}

/// Count `part.N` data-shard files two levels below the object dir, excluding
/// integrity indexes. One data dir per non-delete-marker version.
fn count_part_files(obj_dir: &Path) -> usize {
    if !obj_dir.exists() {
        return 0;
    }
    WalkDir::new(obj_dir)
        .min_depth(2)
        .max_depth(2)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| {
            entry.file_type().is_file()
                && entry
                    .file_name()
                    .to_str()
                    .and_then(|name| name.strip_prefix("part."))
                    .is_some_and(|number| number.parse::<usize>().is_ok())
        })
        .count()
}

fn xl_meta_path(obj_dir: &Path) -> PathBuf {
    obj_dir.join("xl.meta")
}

async fn wait_for_put_tail(ecstore: &Arc<ECStore>, bucket: &str, object: &str) {
    // Shards and xl.meta can exist before the detached PUT owner finishes.
    let lock = ecstore
        .new_ns_lock(bucket, object)
        .await
        .expect("fixture namespace lock should be created");
    let _settled = lock
        .get_write_lock(Duration::from_secs(30))
        .await
        .expect("PUT rename tail must finish before inspecting or wiping the fixture");
}

fn recreate_heal_opts() -> HealOpts {
    // Mirrors the proven-working object heal opts in
    // `heal_integration_test::test_heal_format_with_data`.
    HealOpts {
        recreate: true,
        remove: false,
        ..Default::default()
    }
}

/// Enumerate every version via the REAL B5-1 paged listing, walking all pages.
async fn enumerate_all_versions(heal_storage: &Arc<ECStoreHealStorage>, bucket: &str) -> Vec<HealListItem> {
    let mut items = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let (page, next, truncated) = heal_storage
            .list_objects_for_heal_page(bucket, "", token.as_deref(), false)
            .await
            .expect("list_objects_for_heal_page failed");
        items.extend(page);
        if !truncated {
            break;
        }
        token = next;
        if token.is_none() {
            break;
        }
    }
    items
}

/// Heal every enumerated version through the REAL `ECStoreHealStorage`, mirroring
/// the production per-version loop. Returns (healed_ok, failed).
async fn heal_all_versions(heal_storage: &Arc<ECStoreHealStorage>, bucket: &str) -> (usize, usize) {
    let items = enumerate_all_versions(heal_storage, bucket).await;
    let opts = recreate_heal_opts();
    let mut healed = 0usize;
    let mut failed = 0usize;
    for item in items {
        let (result, error) = heal_storage
            .heal_object(bucket, &item.name, item.version_id.as_deref(), &opts)
            .await
            .expect("heal_object call itself must not error out");
        if error.is_some() {
            failed += 1;
            info!(
                "heal_object reported error for {}/{} v={:?} dm={}: {:?}",
                bucket, item.name, item.version_id, item.is_delete_marker, error
            );
        } else {
            healed += 1;
        }
        let _ = result;
    }
    (healed, failed)
}

async fn read_version(ecstore: &Arc<ECStore>, bucket: &str, object: &str, version_id: &str) -> Vec<u8> {
    let opts = ObjectOptions {
        version_id: Some(version_id.to_string()),
        ..Default::default()
    };
    let mut reader = ecstore
        .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
        .await
        .expect("failed to open version reader");
    let mut buf = Vec::new();
    tokio::io::copy(&mut reader, &mut buf)
        .await
        .expect("failed to read version data");
    buf
}

mod serial_tests {
    use super::*;

    fn physical_version(disk: &Path, bucket: &str, object: &str, version: &str) -> FileInfo {
        let bytes = std::fs::read(xl_meta_path(&object_dir(disk, bucket, object))).expect("physical xl.meta must exist");
        FileMeta::load_or_convert(&bytes)
            .expect("physical metadata must decode")
            .into_fileinfo(bucket, object, version, true, false, true)
            .expect("the exact physical version must exist")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_exact_null_heal_with_current_and_historical_markers() {
        let (disks, ecstore, storage) = heal_env().await;
        let null = uuid::Uuid::nil().to_string();
        let object = "object.bin";
        for (null_marker, newer, latest_marker) in [
            (false, false, false),
            (true, false, false),
            (true, true, false),
            (false, true, true),
            (true, true, true),
        ] {
            let bucket = format!("null-marker-{null_marker}-{newer}-{latest_marker}");
            create_versioned_bucket(&ecstore, &bucket).await;
            let old = put_versioned(&ecstore, &bucket, object, &versioned_test_data(10)).await;
            put_unversioned(&ecstore, &bucket, object, &versioned_test_data(11)).await;
            if null_marker {
                let info = ecstore
                    .delete_object(
                        &bucket,
                        object,
                        ObjectOptions {
                            version_suspended: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("suspended delete must create a null marker");
                assert!(info.delete_marker);
                assert_eq!(info.version_id.unwrap_or_default(), uuid::Uuid::nil());
            }
            let latest = if !newer {
                null.clone()
            } else if latest_marker {
                put_delete_marker(&ecstore, &bucket, object).await
            } else {
                put_versioned(&ecstore, &bucket, object, &versioned_test_data(12)).await
            };
            let mut expected = vec![old, null.clone()];
            if newer {
                expected.push(latest.clone());
            }
            let originals: Vec<_> = expected
                .iter()
                .map(|version| physical_version(&disks[0], &bucket, object, version))
                .collect();
            assert_eq!(originals[1].deleted, null_marker);
            assert_eq!(originals[1].is_latest, !newer);
            let listed = enumerate_all_versions(&storage, &bucket).await;
            let (walked, _, truncated) = storage
                .list_versions_for_heal_page_disk_walk(SET_DISK_ID, &bucket, "", None, false)
                .await
                .expect("disk walk must enumerate null and UUID versions");
            assert!(!truncated);
            for items in [&listed, &walked] {
                assert_eq!(items.len(), expected.len());
                for version in &expected {
                    assert_eq!(
                        items
                            .iter()
                            .filter(|item| item.version_id.as_deref() == Some(version.as_str()))
                            .count(),
                        1
                    );
                }
                let item = items
                    .iter()
                    .find(|item| item.version_id.as_deref() == Some(null.as_str()))
                    .expect("exact null entry");
                assert_eq!(item.is_delete_marker, null_marker);
            }
            std::fs::remove_file(xl_meta_path(&object_dir(&disks[0], &bucket, object))).expect("remove one member's metadata");
            let options = HealOpts {
                scan_mode: HealScanMode::Deep,
                pool: Some(0),
                set: Some(0),
                ..Default::default()
            };
            for item in listed {
                let healed = storage
                    .heal_object_with_receipt(&bucket, object, item.version_id.as_deref(), &options)
                    .await
                    .expect("heal exact version");
                assert!(healed.error.is_none(), "exact version repair failed: {:?}", healed.error);
                if item.is_delete_marker {
                    assert!(!healed.item.integrity_verified);
                    let receipt = healed.receipt.expect("delete markers require an exact metadata receipt");
                    assert_eq!(receipt.identity.version_id, item.version_id);
                    assert!(matches!(
                        receipt.disposition,
                        HealObjectDisposition::Repaired | HealObjectDisposition::MetadataHealthy
                    ));
                } else {
                    assert!(healed.item.integrity_verified);
                    let receipt = healed
                        .receipt
                        .expect("verified exact data version repair must produce a receipt");
                    assert_eq!(receipt.identity.version_id, item.version_id);
                }
                assert_eq!(
                    healed.item.resolved_version_id,
                    Some(
                        *uuid::Uuid::parse_str(item.version_id.as_deref().expect("exact selector"))
                            .expect("UUID selector")
                            .as_bytes()
                    )
                );
            }
            for (version, original) in expected.iter().zip(originals) {
                let repaired = physical_version(&disks[0], &bucket, object, version);
                assert_eq!(repaired.version_id, original.version_id);
                assert_eq!(repaired.deleted, original.deleted);
                assert_eq!(repaired.data_dir, original.data_dir);
                assert_eq!(repaired.size, original.size);
            }
            let latest_result = storage
                .heal_object_with_receipt(&bucket, object, None, &options)
                .await
                .expect("heal latest");
            assert!(latest_result.error.is_none());
            assert_eq!(
                latest_result.item.resolved_version_id,
                Some(*uuid::Uuid::parse_str(&latest).expect("latest UUID").as_bytes())
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_historical_null_recursive_heal_restores_physical_versions() {
        use rustfs_heal::heal::outcome::HealObjectDisposition;
        let (disk_paths, ecstore, storage) = heal_env().await;
        let manager = HealManager::new(
            storage.clone(),
            Some(HealConfig {
                heal_interval: Duration::from_millis(1),
                ..Default::default()
            }),
        );
        manager.start().await.expect("heal manager should start");
        let null = uuid::Uuid::nil().to_string();
        let object = "versions/object.bin";

        for (parity, missing_metadata) in [(false, false), (true, false), (false, true), (true, true)] {
            let bucket = format!("historical-null-{parity}-{missing_metadata}");
            create_versioned_bucket(&ecstore, &bucket).await;
            let mut versions = Vec::new();
            for seed in 1..=3 {
                let data = versioned_test_data(seed);
                let version = put_versioned(&ecstore, &bucket, object, &data).await;
                versions.push((version, data));
            }
            // Exercise the suspended null slot, including an overwrite, before
            // a new versioned write makes that slot historical.
            put_unversioned(&ecstore, &bucket, object, &versioned_test_data(4)).await;
            let null_data = versioned_test_data(5);
            put_unversioned(&ecstore, &bucket, object, &null_data).await;
            versions.push((null.clone(), null_data.clone()));
            let mut latest_data = versioned_test_data(6);
            latest_data.extend_from_slice(b"new-uuid");
            let latest = put_versioned(&ecstore, &bucket, object, &latest_data).await;
            versions.push((latest.clone(), latest_data.clone()));

            let target = disk_paths
                .iter()
                .find(|disk| {
                    let info = physical_version(disk, &bucket, object, &null);
                    assert!(!info.is_latest, "the damaged null must be historical");
                    info.erasure.index == if parity { info.erasure.data_blocks + 1 } else { 1 }
                })
                .expect("the requested data/parity member must exist");
            let originals: Vec<_> = versions
                .iter()
                .map(|(version, _)| {
                    let info = physical_version(target, &bucket, object, version);
                    let part = object_dir(target, &bucket, object)
                        .join(info.data_dir.expect("data version must have a data directory").to_string())
                        .join("part.1");
                    let bytes = std::fs::read(&part).expect("original physical shard must exist");
                    (info, part, bytes)
                })
                .collect();
            if missing_metadata {
                std::fs::remove_file(xl_meta_path(&object_dir(target, &bucket, object))).expect("remove one xl.meta");
            } else {
                std::fs::remove_file(&originals[3].1).expect("remove only the historical null shard");
            }

            let request = HealRequest::new(
                HealType::Bucket { bucket: bucket.clone() },
                HealOptions {
                    recursive: true,
                    scan_mode: HealScanMode::Deep,
                    pool_index: Some(0),
                    set_index: Some(0),
                    ..Default::default()
                },
                HealPriority::Normal,
            );
            let task_id = request.id.clone();
            assert!(
                manager
                    .submit_heal_request(request)
                    .await
                    .expect("submit recursive heal")
                    .is_admitted()
            );
            wait_for_task(&manager, &task_id, Duration::from_secs(60)).await;

            // Inspect physical repair before any GET can trigger read repair.
            for ((version, _), (original, part, bytes)) in versions.iter().zip(&originals) {
                assert_eq!(std::fs::read(part).expect("heal must restore every physical shard"), *bytes);
                let repaired = physical_version(target, &bucket, object, version);
                assert_eq!(repaired.version_id, original.version_id);
                assert_eq!(repaired.data_dir, original.data_dir);
                assert_eq!(repaired.size, original.size);
            }
            let report = manager.get_task_report(&task_id).await.expect("completed task report");
            let outcome = report.outcome.expect("recursive task must have an outcome");
            assert_eq!(outcome.counters.processed, 5);
            assert_eq!(outcome.counters.failed, 0);
            assert_eq!(outcome.counters.unknown, 0);
            assert_eq!(outcome.counters.healed, if missing_metadata { 5 } else { 1 });
            let null_outcome = outcome
                .objects
                .iter()
                .find(|item| item.identity.version_id.as_deref() == Some(null.as_str()))
                .expect("historical null must have its own exact receipt");
            assert_eq!(null_outcome.disposition, HealObjectDisposition::Repaired);
            let null_item = report
                .result_items
                .iter()
                .find(|item| item.version_id == null)
                .expect("historical null must have its own result item");
            assert_eq!(null_item.object_size, null_data.len(), "null must not report the latest UUID's size");

            let listed = enumerate_all_versions(&storage, &bucket).await;
            assert_eq!(listed.len(), 5);
            assert!(listed.iter().any(|item| item.version_id.as_deref() == Some(null.as_str())));
            let latest_result = storage
                .heal_object_with_receipt(&bucket, object, None, &HealOpts::default())
                .await
                .expect("an omitted selector must still heal latest");
            assert!(latest_result.error.is_none());
            assert_eq!(latest_result.item.object_size, latest_data.len());
            assert!(
                latest_result.receipt.is_none(),
                "an omitted selector certifies metadata health only for the null identity"
            );
            let latest_receipt = storage
                .heal_object_with_receipt(&bucket, object, Some(latest.as_str()), &HealOpts::default())
                .await
                .expect("a normal scan of the exact latest version")
                .receipt
                .expect("a normal scan should certify metadata health");
            assert_eq!(latest_receipt.disposition, HealObjectDisposition::MetadataHealthy);
            let verified_latest = storage
                .heal_object_with_receipt(
                    &bucket,
                    object,
                    None,
                    &HealOpts {
                        scan_mode: HealScanMode::Deep,
                        ..Default::default()
                    },
                )
                .await
                .expect("deep heal of latest");
            assert!(verified_latest.error.is_none());
            assert_eq!(verified_latest.item.object_size, latest_data.len());
            assert!(verified_latest.receipt.is_some(), "a deep scan can certify the exact latest version");
            for (version, data) in &versions {
                assert_eq!(&read_version(&ecstore, &bucket, object, version).await, data);
            }
        }
        manager.stop().await.expect("heal manager should stop");
    }

    /// Directly exercises `ECStoreHealStorage::list_objects_for_heal_page` on a
    /// real versioned fixture: two data versions + a delete-marker-latest. Proves
    /// enumeration returns one `HealListItem` per version, flags the delete marker
    /// via `is_delete_marker`, and carries the correct version ids.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_enumeration_includes_all_versions_and_delete_marker_real_fixture() {
        let (_disk_paths, ecstore, heal_storage) = heal_env().await;
        let bucket = "b5-enum-versions";
        let object = "obj.bin";
        create_versioned_bucket(&ecstore, bucket).await;

        let v1 = put_versioned(&ecstore, bucket, object, &versioned_test_data(1)).await;
        let v2 = put_versioned(&ecstore, bucket, object, &versioned_test_data(2)).await;
        let dm = put_delete_marker(&ecstore, bucket, object).await;
        assert_ne!(v1, v2);
        assert_ne!(v2, dm);

        let items = enumerate_all_versions(&heal_storage, bucket).await;

        // FIXTURE-NON-EMPTY GUARD: exactly the three versions we created.
        assert_eq!(items.len(), 3, "must enumerate every version + the delete marker, got {items:?}");
        assert!(items.iter().all(|it| it.name == object), "all entries belong to the same object");

        let dm_items: Vec<&HealListItem> = items.iter().filter(|it| it.is_delete_marker).collect();
        assert_eq!(dm_items.len(), 1, "exactly one entry must be flagged as a delete marker");
        assert_eq!(
            dm_items[0].version_id.as_deref(),
            Some(dm.as_str()),
            "the delete-marker entry must carry the delete marker's version id"
        );

        let ids: std::collections::HashSet<Option<String>> = items.iter().map(|it| it.version_id.clone()).collect();
        assert!(ids.contains(&Some(v1.clone())), "old version v1 must be enumerated");
        assert!(ids.contains(&Some(v2.clone())), "version v2 must be enumerated");
        assert!(ids.contains(&Some(dm.clone())), "delete marker version must be enumerated");
        // Every version id is a real (non-nil) id, so none normalized to None.
        assert!(items.iter().all(|it| it.version_id.is_some()), "versioned entries must keep their ids");
    }

    /// Physical repair of an OLD non-latest version after wiping one disk.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_heal_old_nonlatest_version_after_disk_wipe() {
        let (disk_paths, ecstore, heal_storage) = heal_env().await;
        let bucket = "b5-old-version-heal";
        let object = "obj.bin";
        create_versioned_bucket(&ecstore, bucket).await;

        let data_v1 = versioned_test_data(10);
        let data_v2 = versioned_test_data(20);
        let v1 = put_versioned(&ecstore, bucket, object, &data_v1).await; // OLD, non-latest
        let v2 = put_versioned(&ecstore, bucket, object, &data_v2).await; // latest
        assert!(
            disk_paths.iter().all(|disk| {
                let dir = object_dir(disk, bucket, object);
                xl_meta_path(&dir).exists() && count_part_files(&dir) >= 2
            }),
            "both versions must exist on every disk before wiping the fixture"
        );

        // ── Pre-wipe: prove the fixture actually has 2 versions on disk[0] ──
        let obj_dir0 = object_dir(&disk_paths[0], bucket, object);
        assert!(xl_meta_path(&obj_dir0).exists(), "xl.meta must exist before wipe");
        assert_eq!(
            count_part_files(&obj_dir0),
            2,
            "two data versions must each have an on-disk shard before wipe"
        );
        let pre_items = enumerate_all_versions(&heal_storage, bucket).await;
        assert_eq!(pre_items.len(), 2, "fixture must expose both versions");

        // ── Wipe disk[0]'s object dir (xl.meta + both data dirs) ──
        std::fs::remove_dir_all(&obj_dir0).expect("failed to wipe object dir on disk[0]");
        assert!(!obj_dir0.exists(), "object dir must be gone after wipe");
        assert_eq!(count_part_files(&obj_dir0), 0, "no shards on disk[0] after wipe");

        // ── Heal every enumerated version through the real heal storage ──
        let (healed, failed) = heal_all_versions(&heal_storage, bucket).await;
        assert_eq!(failed, 0, "no version may be recorded as failed");
        assert!(healed >= 2, "both versions must be healed, healed={healed}");

        // ── Post-heal: disk[0] physically restored, incl. the OLD version ──
        assert!(xl_meta_path(&obj_dir0).exists(), "xl.meta must be restored on the wiped disk");
        assert_eq!(
            count_part_files(&obj_dir0),
            2,
            "both versions' data shards must be physically restored on disk[0]"
        );

        // Old non-latest version data must be intact and readable end-to-end.
        assert_eq!(read_version(&ecstore, bucket, object, &v1).await, data_v1, "old version data corrupted");
        assert_eq!(
            read_version(&ecstore, bucket, object, &v2).await,
            data_v2,
            "latest version data corrupted"
        );
    }

    /// Delete-marker-latest objects must be enumerated AND healed after a wipe.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_heal_delete_marker_latest_enumerated_and_healed() {
        let (disk_paths, ecstore, heal_storage) = heal_env().await;
        let bucket = "b5-dm-latest-heal";
        let object = "obj.bin";
        create_versioned_bucket(&ecstore, bucket).await;

        let data_v1 = versioned_test_data(30);
        let v1 = put_versioned(&ecstore, bucket, object, &data_v1).await; // data version
        let dm = put_delete_marker(&ecstore, bucket, object).await; // delete-marker latest

        // ── Pre-wipe fixture proof: one data shard + a delete-marker latest ──
        let obj_dir0 = object_dir(&disk_paths[0], bucket, object);
        assert!(xl_meta_path(&obj_dir0).exists(), "xl.meta must exist before wipe");
        assert_eq!(count_part_files(&obj_dir0), 1, "one data version => one shard before wipe");
        let pre_items = enumerate_all_versions(&heal_storage, bucket).await;
        assert_eq!(pre_items.len(), 2, "must enumerate the data version and the delete marker");
        assert!(
            pre_items
                .iter()
                .any(|it| it.is_delete_marker && it.version_id.as_deref() == Some(dm.as_str())),
            "the delete-marker-latest must be enumerated as a heal unit"
        );
        // The latest, per get_object_info, is a delete marker (object reads as deleted).
        let latest = ecstore.get_object_info(bucket, object, &ObjectOptions::default()).await;
        assert!(
            matches!(&latest, Ok(info) if info.delete_marker) || latest.is_err(),
            "latest must resolve to a delete marker before heal"
        );

        // ── Wipe disk[0]'s object dir ──
        std::fs::remove_dir_all(&obj_dir0).expect("failed to wipe object dir on disk[0]");
        assert_eq!(count_part_files(&obj_dir0), 0, "no shards on disk[0] after wipe");

        // ── Heal all enumerated versions (data version + delete marker) ──
        let (healed, failed) = heal_all_versions(&heal_storage, bucket).await;
        assert_eq!(failed, 0, "delete marker + data version must not be recorded as failed");
        assert!(healed >= 2, "both the delete marker and the data version must heal, healed={healed}");

        // ── Post-heal: xl.meta (with the tombstone) + the data shard restored ──
        assert!(
            xl_meta_path(&obj_dir0).exists(),
            "xl.meta (carrying the delete-marker tombstone) must be restored on disk[0]"
        );
        assert_eq!(
            count_part_files(&obj_dir0),
            1,
            "the underlying data version's shard must be physically restored on disk[0]"
        );

        // The old data version is still readable by id; the latest is still a DM.
        assert_eq!(
            read_version(&ecstore, bucket, object, &v1).await,
            data_v1,
            "healed data version corrupted"
        );
        let latest_after = ecstore.get_object_info(bucket, object, &ObjectOptions::default()).await;
        assert!(
            matches!(&latest_after, Ok(info) if info.delete_marker) || latest_after.is_err(),
            "latest must remain a delete marker after heal"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_recursive_deep_heal_reports_stale_delete_marker_repair() {
        let (disk_paths, ecstore, storage) = heal_env().await;
        let manager = HealManager::new(
            storage.clone(),
            Some(HealConfig {
                heal_interval: Duration::from_millis(1),
                ..Default::default()
            }),
        );
        manager.start().await.expect("heal manager should start");

        let bucket = "b5-stale-delete-marker-recursive";
        let object = "obj.bin";
        create_versioned_bucket(&ecstore, bucket).await;
        put_unversioned(&ecstore, bucket, "control/object.bin", &versioned_test_data(42)).await;
        let historical_data = versioned_test_data(43);
        let historical = put_versioned(&ecstore, bucket, object, &historical_data).await;
        let target = &disk_paths[1];
        let target_meta = xl_meta_path(&object_dir(target, bucket, object));
        let stale_meta = std::fs::read(&target_meta).expect("target xl.meta must exist before marker creation");
        let stale_digest = Sha256::digest(&stale_meta);
        let marker = put_delete_marker(&ecstore, bucket, object).await;
        std::fs::write(&target_meta, &stale_meta).expect("restore stale target xl.meta");

        let request = HealRequest::new(
            HealType::Bucket {
                bucket: bucket.to_string(),
            },
            HealOptions {
                recursive: true,
                scan_mode: HealScanMode::Deep,
                pool_index: Some(0),
                set_index: Some(0),
                ..Default::default()
            },
            HealPriority::Normal,
        );
        let task_id = request.id.clone();
        assert!(
            manager
                .submit_heal_request(request)
                .await
                .expect("submit recursive heal")
                .is_admitted()
        );
        wait_for_task(&manager, &task_id, Duration::from_secs(60)).await;

        let report = manager.get_task_report(&task_id).await.expect("completed task report");
        let outcome = report.outcome.expect("recursive task must have an outcome");
        assert_eq!(outcome.counters.failed, 0);
        assert_eq!(outcome.counters.unknown, 0);
        assert_eq!(outcome.counters.processed, 3);
        assert_eq!(outcome.counters.healed, 1, "delete marker repair must be counted as healed");
        assert_eq!(outcome.counters.unchanged, 2);

        let repaired_meta = std::fs::read(&target_meta).expect("repaired target xl.meta must exist");
        assert_ne!(
            Sha256::digest(&repaired_meta),
            stale_digest,
            "deep heal must replace stale rejoined metadata"
        );

        let marker_info = physical_version(target, bucket, object, &marker);
        assert!(
            marker_info.deleted && marker_info.is_latest,
            "target metadata must converge to delete-marker latest"
        );
        let historical_info = physical_version(target, bucket, object, &historical);
        assert!(!historical_info.is_latest, "historical version must no longer be marked latest");
        assert_eq!(
            read_version(&ecstore, bucket, object, &historical).await,
            historical_data,
            "historical version must remain readable after marker convergence"
        );
        let marker_receipt = outcome
            .objects
            .iter()
            .find(|item| item.identity.version_id.as_deref() == Some(marker.as_str()))
            .expect("deep heal must emit a receipt for the repaired delete marker");
        assert_eq!(marker_receipt.disposition, HealObjectDisposition::Repaired);
        manager.stop().await.expect("heal manager should stop");
    }

    /// A missing xl.meta must produce an exact marker repair receipt, while a
    /// healthy replay must prove metadata health without claiming payload integrity.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_delete_marker_receipt_repair_then_metadata_healthy() {
        let (disk_paths, ecstore, heal_storage) = heal_env().await;
        let bucket = "b5-dm-receipt";
        let object = "marker.bin";
        create_versioned_bucket(&ecstore, bucket).await;

        let _data = put_versioned(&ecstore, bucket, object, &versioned_test_data(41)).await;
        let marker = put_delete_marker(&ecstore, bucket, object).await;
        let obj_dir = object_dir(&disk_paths[0], bucket, object);
        tokio::fs::remove_file(xl_meta_path(&obj_dir))
            .await
            .expect("remove one xl.meta replica");

        let repaired = heal_storage
            .heal_object_with_receipt(bucket, object, Some(&marker), &recreate_heal_opts())
            .await
            .expect("marker repair request");
        assert!(repaired.error.is_none(), "{:?}", repaired.error);
        assert!(repaired.item.metadata_repair_verified);
        let receipt = repaired.receipt.expect("committed marker repair receipt");
        assert_eq!(receipt.disposition, HealObjectDisposition::Repaired);
        assert_eq!(receipt.identity.version_id.as_deref(), Some(marker.as_str()));

        let healthy = heal_storage
            .heal_object_with_receipt(bucket, object, Some(&marker), &recreate_heal_opts())
            .await
            .expect("healthy marker replay");
        assert!(healthy.error.is_none(), "{:?}", healthy.error);
        assert_eq!(healthy.item.drives_healed(), Some(0));
        assert!(healthy.item.metadata_verified);
        assert!(!healthy.item.integrity_verified);
        let receipt = healthy.receipt.expect("metadata health receipt");
        assert_eq!(receipt.disposition, HealObjectDisposition::MetadataHealthy);
        assert_eq!(receipt.identity.version_id.as_deref(), Some(marker.as_str()));
    }

    /// Enumerated unversioned objects select the exact null slot once each.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_version_id_normalization_null_and_unversioned_real_fixture() {
        let (_disk_paths, ecstore, heal_storage) = heal_env().await;
        let bucket = "b5-unversioned-normalize";
        create_unversioned_bucket(&ecstore, bucket).await;

        put_unversioned(&ecstore, bucket, "a.bin", &versioned_test_data(1)).await;
        put_unversioned(&ecstore, bucket, "b.bin", &versioned_test_data(2)).await;
        // Overwrite b.bin: on an unversioned bucket this replaces in place, so
        // there is still exactly one enumerable unit for it.
        put_unversioned(&ecstore, bucket, "b.bin", &versioned_test_data(3)).await;

        let items = enumerate_all_versions(&heal_storage, bucket).await;
        assert_eq!(items.len(), 2, "unversioned bucket => exactly one heal unit per object, got {items:?}");
        assert!(
            items
                .iter()
                .all(|it| it.version_id.as_deref() == Some(uuid::Uuid::nil().to_string().as_str())),
            "enumerated null versions must retain an exact selector: {items:?}"
        );
        assert!(items.iter().all(|it| !it.is_delete_marker), "no delete markers expected");
        let names: std::collections::HashSet<&str> = items.iter().map(|it| it.name.as_str()).collect();
        assert!(names.contains("a.bin") && names.contains("b.bin"), "both objects enumerated once");
    }

    /// Unversioned bucket, full heal path via `HealManager` (recursive bucket
    /// heal), after wiping one disk. Every object is healed exactly once and
    /// remains readable; nothing is dropped or double-processed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_heal_unversioned_bucket_e2e() {
        let (disk_paths, ecstore, heal_storage) = heal_env().await;
        let bucket = "b5-unversioned-e2e";
        create_unversioned_bucket(&ecstore, bucket).await;

        let objects = ["o1.bin", "o2.bin", "o3.bin"];
        let mut datas = Vec::new();
        for (i, obj) in objects.iter().enumerate() {
            let data = versioned_test_data(i as u8 + 40);
            put_unversioned(&ecstore, bucket, obj, &data).await;
            datas.push(data);
        }

        // Wipe every object dir on disk[0].
        for obj in &objects {
            let dir = object_dir(&disk_paths[0], bucket, obj);
            std::fs::remove_dir_all(&dir).expect("failed to wipe object dir");
            assert_eq!(count_part_files(&dir), 0);
        }

        // Enumeration returns exactly one unit per object (no duplicates).
        let items = enumerate_all_versions(&heal_storage, bucket).await;
        assert_eq!(items.len(), objects.len(), "one heal unit per object");
        assert!(
            items
                .iter()
                .all(|it| it.version_id.as_deref() == Some(uuid::Uuid::nil().to_string().as_str()))
        );

        // Drive the real recursive bucket heal through the HealManager task loop.
        let cfg = HealConfig {
            heal_interval: Duration::from_millis(1),
            ..Default::default()
        };
        let heal_manager = HealManager::new(heal_storage.clone(), Some(cfg));
        heal_manager.start().await.unwrap();
        let request = HealRequest::new(
            HealType::Bucket {
                bucket: bucket.to_string(),
            },
            HealOptions {
                recursive: true,
                recreate_missing: true,
                scan_mode: HealScanMode::Normal,
                timeout: Some(Duration::from_secs(300)),
                ..Default::default()
            },
            HealPriority::Normal,
        );
        let task_id = request.id.clone();
        let admission = heal_manager
            .submit_heal_request(request)
            .await
            .expect("failed to submit unversioned bucket heal");
        assert!(admission.is_admitted());

        wait_for_task(&heal_manager, &task_id, Duration::from_secs(60)).await;

        // Every object is physically restored on disk[0] and reads back intact.
        for (obj, data) in objects.iter().zip(datas.iter()) {
            let dir = object_dir(&disk_paths[0], bucket, obj);
            assert!(xl_meta_path(&dir).exists(), "{obj}: xl.meta not restored on disk[0]");
            assert_eq!(count_part_files(&dir), 1, "{obj}: data shard not restored on disk[0]");
            let mut reader = ecstore
                .get_object_reader(bucket, obj, None, HeaderMap::new(), &ObjectOptions::default())
                .await
                .expect("healed object must be readable");
            let mut buf = Vec::new();
            tokio::io::copy(&mut reader, &mut buf).await.unwrap();
            assert_eq!(&buf, data, "{obj}: healed data mismatch");
        }
    }

    /// End-to-end drive of the REAL resume/checkpoint-backed erasure-set healer
    /// (`ErasureSet` heal type -> `ErasureSetHealer::heal_bucket_with_resume`)
    /// against real disks with a multi-version fixture across a wiped disk.
    ///
    /// SCOPE NOTE: literally crossing the 1000-key `list_object_versions` page
    /// boundary at e2e scale (1000+ real versions) is impractical for a unit-speed
    /// test, so that exact boundary + mid-page cancel/resume behavior is covered
    /// by the B5-1 in-crate loop tests
    /// (`erasure_healer::resume_loop_tests::test_resume_across_page_boundary_no_drop_no_double`,
    /// `test_object_with_versions_spanning_pages_advances`,
    /// `test_non_advancing_cursor_aborts`,
    /// `test_schedule_retry_resets_both_managers_and_reheals`). Here we prove the
    /// same resume machinery drives a real ECStore heal to completion and repairs
    /// every version, with the resume state cleaned up afterward.
    #[test]
    #[serial]
    fn test_heal_resume_across_page_boundary_e2e() {
        // Resume runs on Tokio workers as well as the test thread; both need the debug server's stack budget.
        const STACK_SIZE: usize = 8 * 1024 * 1024;
        std::thread::Builder::new()
            .name("heal-resume-page-boundary".to_owned())
            .stack_size(STACK_SIZE)
            .spawn(|| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(4)
                    .thread_stack_size(STACK_SIZE)
                    .enable_all()
                    .build()
                    .expect("resume test runtime should build");
                runtime.block_on(test_heal_resume_across_page_boundary_e2e_inner());
            })
            .expect("resume test thread should spawn")
            .join()
            .expect("resume test thread should finish");
    }

    async fn test_heal_resume_across_page_boundary_e2e_inner() {
        let (disk_paths, ecstore, heal_storage) = heal_env().await;
        let bucket = "b5-resume-e2e";
        create_versioned_bucket(&ecstore, bucket).await;

        // A small multi-object, multi-version fixture: 3 objects, 2 versions each,
        // plus a delete-marker-latest on one of them.
        let mut versions: Vec<(String, String, Vec<u8>)> = Vec::new(); // (object, version_id, data)
        for obj_idx in 0..3u8 {
            let object = format!("obj-{obj_idx}.bin");
            let d1 = versioned_test_data(obj_idx + 50);
            let d2 = versioned_test_data(obj_idx + 80);
            let v1 = put_versioned(&ecstore, bucket, &object, &d1).await;
            let v2 = put_versioned(&ecstore, bucket, &object, &d2).await;
            versions.push((object.clone(), v1, d1));
            versions.push((object.clone(), v2, d2));
        }
        // Delete-marker latest on obj-0.
        let _dm = put_delete_marker(&ecstore, bucket, "obj-0.bin").await;

        // Wipe disk[0]'s bucket dir entirely (all objects' shards + metadata).
        let bucket_dir0 = disk_paths[0].join(bucket);
        assert!(bucket_dir0.exists());
        std::fs::remove_dir_all(&bucket_dir0).expect("failed to wipe bucket dir on disk[0]");
        assert!(!bucket_dir0.exists());

        // Drive the resume-backed erasure-set heal through the HealManager, which
        // constructs the ErasureSetHealer and runs heal_bucket_with_resume.
        let cfg = HealConfig {
            heal_interval: Duration::from_millis(1),
            ..Default::default()
        };
        let heal_manager = HealManager::new(heal_storage.clone(), Some(cfg));
        heal_manager.start().await.unwrap();

        // The erasure-set healer defers any per-version heal that hits a
        // transient error (unmet quorum, DiskNotFound, a slow-disk read under
        // load) to a later heal cycle: it persists its resume/checkpoint state
        // and returns a terminal `Failed { .. "retry scheduled" }` so a *fresh*
        // heal run re-drives the still-unhealed versions (see the finalize step
        // in `ErasureSetHealer::heal_bucket_with_resume`). In production the
        // background scanner is that next run; here we supply it ourselves by
        // re-submitting the idempotent heal. This keeps the e2e faithful to the
        // resume design without making it hostage to a rare single-version
        // transient hiccup — the strict data-restoration assertions below still
        // run only after a genuine `Completed`.
        let build_request = |force_start: bool| {
            let mut request = HealRequest::new(
                HealType::ErasureSet {
                    buckets: vec![bucket.to_string()],
                    set_disk_id: SET_DISK_ID.to_string(),
                },
                HealOptions {
                    recursive: true,
                    recreate_missing: true,
                    scan_mode: HealScanMode::Normal,
                    timeout: Some(Duration::from_secs(300)),
                    ..Default::default()
                },
                HealPriority::Normal,
            );
            request.force_start = force_start;
            request
        };
        drive_heal_to_completion(&heal_manager, build_request, Duration::from_secs(120), 3).await;

        // Every data version is readable end-to-end and physically restored on the
        // wiped disk. (Resume machinery drove the full per-version heal.)
        for (object, version_id, data) in &versions {
            assert_eq!(
                &read_version(&ecstore, bucket, object, version_id).await,
                data,
                "{object} v={version_id}: data not restored after resume-backed heal"
            );
            let dir = object_dir(&disk_paths[0], bucket, object);
            assert!(xl_meta_path(&dir).exists(), "{object}: xl.meta not restored on disk[0]");
        }
        // Each object should have both of its data versions' shards back on disk[0].
        for obj_idx in 0..3u8 {
            let dir = object_dir(&disk_paths[0], bucket, &format!("obj-{obj_idx}.bin"));
            assert_eq!(
                count_part_files(&dir),
                2,
                "obj-{obj_idx}: both data versions' shards must be restored on disk[0]"
            );
        }
    }

    /// Poll a heal task to a terminal state, panicking on failure/timeout.
    async fn wait_for_task(heal_manager: &HealManager, task_id: &str, timeout: Duration) {
        match await_terminal_status(heal_manager, task_id, timeout).await {
            HealTaskStatus::Completed => {}
            HealTaskStatus::Failed { error } => panic!("heal task failed: {error}"),
            HealTaskStatus::Cancelled => panic!("heal task was cancelled"),
            other => panic!("heal task reached unexpected terminal state: {other:?}"),
        }
    }

    /// Poll a heal task until it reaches a terminal state and return that state.
    /// Panics only on timeout — callers decide how to treat each terminal state.
    async fn await_terminal_status(heal_manager: &HealManager, task_id: &str, timeout: Duration) -> HealTaskStatus {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Ok(status) = heal_manager.get_task_status(task_id).await
                && matches!(
                    status,
                    HealTaskStatus::Completed
                        | HealTaskStatus::Failed { .. }
                        | HealTaskStatus::Cancelled
                        | HealTaskStatus::Timeout
                )
            {
                return status;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("heal task {task_id} did not reach a terminal state within {timeout:?}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Submit an erasure-set heal and drive it to a genuine `Completed`,
    /// tolerating the healer's by-design "retry scheduled" deferral.
    ///
    /// When a per-version heal hits a transient error the erasure-set healer
    /// persists its resume state and returns a terminal `Failed` carrying
    /// `retry scheduled` (retry budget still remaining) — expecting a later heal
    /// run to finish the job. We re-submit ourselves (an idempotent re-heal),
    /// mirroring the production background scanner. A `Failed` WITHOUT that
    /// marker (e.g. `exhausted retries`) or any other non-`Completed` terminal
    /// state is a real failure and panics.
    async fn drive_heal_to_completion(
        heal_manager: &HealManager,
        build_request: impl Fn(bool) -> HealRequest,
        per_attempt_timeout: Duration,
        max_redrives: usize,
    ) {
        for attempt in 0..=max_redrives {
            let request = build_request(attempt > 0);
            let task_id = request.id.clone();
            let admission = heal_manager
                .submit_heal_request(request)
                .await
                .expect("failed to submit erasure-set heal");
            assert!(admission.is_admitted(), "erasure-set heal must be admitted");

            match await_terminal_status(heal_manager, &task_id, per_attempt_timeout).await {
                HealTaskStatus::Completed => return,
                HealTaskStatus::Failed { error } if error.contains("retry scheduled") => {
                    info!(attempt, error, "erasure-set heal deferred a transient version; re-driving to completion");
                    // Brief settle before the next idempotent re-heal.
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                HealTaskStatus::Failed { error } => panic!("heal task failed (non-retryable): {error}"),
                HealTaskStatus::Cancelled => panic!("heal task was cancelled"),
                other => panic!("heal task reached unexpected terminal state: {other:?}"),
            }
        }
        panic!("erasure-set heal did not reach Completed within {} attempt(s)", max_redrives + 1);
    }
}
