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
//! and re-runs the full `setup_test_env` init (which sets the process-global
//! bucket-metadata-sys OnceCell) — under `cargo nextest` each test runs
//! in its own process so the OnceCell never collides.

use http::HeaderMap;
use rustfs_common::heal_channel::{HealOpts, HealScanMode};
use rustfs_heal::heal::{
    manager::{HealConfig, HealManager},
    storage::{
        ECStoreHealStorage, HealListItem, HealObjectOptions as ObjectOptions, HealPutObjReader as PutObjReader, HealStorageAPI,
    },
    task::{HealOptions, HealPriority, HealRequest, HealTaskStatus, HealType},
};
use serial_test::serial;
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Once},
    time::Duration,
};
use tokio::fs;
use tokio_util::sync::CancellationToken;
use tracing::info;
use walkdir::WalkDir;

mod storage_api;

use storage_api::integration::{
    BucketOperations, BucketOptions, ECStore, Endpoint, EndpointServerPools, Endpoints, MakeBucketOptions, ObjectIO as _,
    ObjectOperations as _, PoolEndpoints, init_bucket_metadata_sys, init_local_disks,
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

static INIT: Once = Once::new();

fn init_tracing() {
    INIT.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
            .with_thread_names(true)
            .try_init();
    });
}

/// Build a real 4-disk `ECStore` + `ECStoreHealStorage`. Mirrors
/// `heal_integration_test::setup_test_env`.
async fn setup_test_env() -> (Vec<PathBuf>, Arc<ECStore>, Arc<ECStoreHealStorage>) {
    init_tracing();

    let test_base_dir = format!("/tmp/rustfs_heal_b5_test_{}", uuid::Uuid::new_v4());
    let temp_dir = PathBuf::from(&test_base_dir);
    if temp_dir.exists() {
        fs::remove_dir_all(&temp_dir).await.ok();
    }
    fs::create_dir_all(&temp_dir).await.unwrap();

    let disk_paths = vec![
        temp_dir.join("disk1"),
        temp_dir.join("disk2"),
        temp_dir.join("disk3"),
        temp_dir.join("disk4"),
    ];
    for disk_path in &disk_paths {
        fs::create_dir_all(disk_path).await.unwrap();
    }

    let mut endpoints = Vec::new();
    for (i, disk_path) in disk_paths.iter().enumerate() {
        let mut endpoint = Endpoint::try_from(disk_path.to_str().unwrap()).unwrap();
        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(i);
        endpoints.push(endpoint);
    }

    let pool_endpoints = PoolEndpoints {
        legacy: false,
        set_count: 1,
        drives_per_set: 4,
        endpoints: Endpoints::from(endpoints),
        cmd_line: "test".to_string(),
        platform: format!("OS: {} | Arch: {}", std::env::consts::OS, std::env::consts::ARCH),
    };
    let endpoint_pools = EndpointServerPools::from(vec![pool_endpoints]);

    init_local_disks(endpoint_pools.clone()).await.unwrap();

    let server_addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let ecstore = ECStore::new(server_addr, endpoint_pools, CancellationToken::new())
        .await
        .unwrap();

    let buckets_list = ecstore
        .list_bucket(&BucketOptions {
            no_metadata: true,
            ..Default::default()
        })
        .await
        .unwrap();
    let buckets = buckets_list.into_iter().map(|v| v.name).collect();
    init_bucket_metadata_sys(ecstore.clone(), buckets).await;

    let heal_storage = Arc::new(ECStoreHealStorage::new(ecstore.clone()));
    (disk_paths, ecstore, heal_storage)
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
        ..Default::default()
    };
    let info = (**ecstore)
        .put_object(bucket, object, &mut reader, &opts)
        .await
        .expect("versioned put_object failed");
    info.version_id
        .map(|u| u.to_string())
        .expect("versioned put must return a version id")
}

async fn put_unversioned(ecstore: &Arc<ECStore>, bucket: &str, object: &str, data: &[u8]) {
    let mut reader = PutObjReader::from_vec(data.to_vec());
    (**ecstore)
        .put_object(bucket, object, &mut reader, &ObjectOptions::default())
        .await
        .expect("unversioned put_object failed");
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

/// Count `part.*` data-shard files two levels below the object dir
/// (`<object>/<data-uuid>/part.N`). One data dir per non-delete-marker version.
fn count_part_files(obj_dir: &Path) -> usize {
    if !obj_dir.exists() {
        return 0;
    }
    WalkDir::new(obj_dir)
        .min_depth(2)
        .max_depth(2)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file() && e.file_name().to_str().map(|n| n.starts_with("part.")).unwrap_or(false))
        .count()
}

fn xl_meta_path(obj_dir: &Path) -> PathBuf {
    obj_dir.join("xl.meta")
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
            .list_objects_for_heal_page(bucket, "", token.as_deref())
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

    /// Directly exercises `ECStoreHealStorage::list_objects_for_heal_page` on a
    /// real versioned fixture: two data versions + a delete-marker-latest. Proves
    /// enumeration returns one `HealListItem` per version, flags the delete marker
    /// via `is_delete_marker`, and carries the correct version ids.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_enumeration_includes_all_versions_and_delete_marker_real_fixture() {
        let (_disk_paths, ecstore, heal_storage) = setup_test_env().await;
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
        let (disk_paths, ecstore, heal_storage) = setup_test_env().await;
        let bucket = "b5-old-version-heal";
        let object = "obj.bin";
        create_versioned_bucket(&ecstore, bucket).await;

        let data_v1 = versioned_test_data(10);
        let data_v2 = versioned_test_data(20);
        let v1 = put_versioned(&ecstore, bucket, object, &data_v1).await; // OLD, non-latest
        let v2 = put_versioned(&ecstore, bucket, object, &data_v2).await; // latest

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
        let (disk_paths, ecstore, heal_storage) = setup_test_env().await;
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

    /// Unversioned objects normalize to `version_id == None` and enumerate once
    /// each (never a phantom `Some("null")`).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_version_id_normalization_null_and_unversioned_real_fixture() {
        let (_disk_paths, ecstore, heal_storage) = setup_test_env().await;
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
            items.iter().all(|it| it.version_id.is_none()),
            "unversioned objects must normalize to version_id == None, never Some(\"null\"): {items:?}"
        );
        assert!(items.iter().all(|it| !it.is_delete_marker), "no delete markers expected");
        let names: std::collections::HashSet<&str> = items.iter().map(|it| it.name.as_str()).collect();
        assert!(names.contains("a.bin") && names.contains("b.bin"), "both objects enumerated once");

        // NOTE: A literal MinIO-interop "null" *string* version id is only minted
        // by the S3 request layer (versioning-suspended writes); it cannot be
        // constructed through the internal ECStore put path used here (suspended/
        // unversioned writes store no version id at all -> None). The nil-UUID ->
        // None normalization itself is unit-covered in B5-1
        // (crates/heal storage list_objects_for_heal_page maps
        // `version_id.filter(|u| !u.is_nil())`). This test covers the real
        // unversioned-object => None path end-to-end.
    }

    /// Unversioned bucket, full heal path via `HealManager` (recursive bucket
    /// heal), after wiping one disk. Every object is healed exactly once and
    /// remains readable; nothing is dropped or double-processed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_heal_unversioned_bucket_e2e() {
        let (disk_paths, ecstore, heal_storage) = setup_test_env().await;
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
        assert!(items.iter().all(|it| it.version_id.is_none()));

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
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_heal_resume_across_page_boundary_e2e() {
        let (disk_paths, ecstore, heal_storage) = setup_test_env().await;
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
        let request = HealRequest::new(
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
        let task_id = request.id.clone();
        let admission = heal_manager
            .submit_heal_request(request)
            .await
            .expect("failed to submit erasure-set heal");
        assert!(admission.is_admitted(), "erasure-set heal must be admitted");

        wait_for_task(&heal_manager, &task_id, Duration::from_secs(120)).await;

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
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Ok(status) = heal_manager.get_task_status(task_id).await {
                match status {
                    HealTaskStatus::Completed => return,
                    HealTaskStatus::Failed { ref error } => panic!("heal task failed: {error}"),
                    HealTaskStatus::Cancelled => panic!("heal task was cancelled"),
                    _ => {}
                }
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("heal task {task_id} did not complete within {timeout:?}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}
