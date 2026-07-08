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

//! backlog#920: real-disk e2e suite proving the per-erasure-set DISK-WALK UNION
//! heal enumerator surfaces (and heals) sub-quorum versions that the B5
//! read-quorum enumeration (`list_object_versions`) omits, AND that the
//! data-safety guard (decision 1) regenerates a lost xl.meta for a
//! reconstructable version instead of dangling-DELETING it.
//!
//! These drive the REAL `ECStoreHealStorage` + `ECStore` against real disks.
//! Every test is `#[serial]`; under `cargo nextest` each runs in its own process.

use http::HeaderMap;
use rustfs_common::heal_channel::{HealOpts, HealScanMode};
use rustfs_heal::heal::storage::{
    ECStoreHealStorage, HealListItem, HealObjectOptions as ObjectOptions, HealPutObjReader as PutObjReader, HealStorageAPI,
};
use serial_test::serial;
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Once},
};
use tokio::fs;
use tokio_util::sync::CancellationToken;
use walkdir::WalkDir;

mod storage_api;

use storage_api::integration::{
    BucketOperations, BucketOptions, ECStore, Endpoint, EndpointServerPools, Endpoints, MakeBucketOptions, ObjectIO as _,
    PoolEndpoints, init_bucket_metadata_sys, init_local_disks,
};

/// 256 KiB + change: large enough to be stored as non-inline erasure shards, so
/// deleting the `xl.meta` file does NOT delete the data (the `part.*` shards live
/// in a sibling data-dir). This is required to exercise the "meta lost, data
/// present" rescue path — an inline object would lose its data with its xl.meta.
const NON_INLINE_TEST_DATA_SIZE: usize = 256 * 1024 + 137;

const SET_DISK_ID: &str = "pool_0_set_0";
const GRACE_ENV: &str = "RUSTFS_HEAL_DANGLING_DELETE_GRACE_SECS";

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
            .try_init();
    });
}

/// Disable the dangling-delete grace window so the destructive path is genuinely
/// LIVE in these tests: without the decision-1 guard, a recoverable version WOULD
/// be dangling-deleted here. With grace at its 1h default the delete path would be
/// masked and the test would prove nothing.
fn disable_dangling_grace() {
    // Safe under nextest: each test runs in its own process and is `#[serial]`.
    unsafe {
        std::env::set_var(GRACE_ENV, "0");
    }
}

/// Build a real N-disk single-set `ECStore` + `ECStoreHealStorage`.
async fn setup_test_env_n(n_disks: usize) -> (Vec<PathBuf>, Arc<ECStore>, Arc<ECStoreHealStorage>) {
    init_tracing();

    let test_base_dir = format!("/tmp/rustfs_heal_b920_test_{}", uuid::Uuid::new_v4());
    let temp_dir = PathBuf::from(&test_base_dir);
    if temp_dir.exists() {
        fs::remove_dir_all(&temp_dir).await.ok();
    }
    fs::create_dir_all(&temp_dir).await.unwrap();

    let disk_paths: Vec<PathBuf> = (0..n_disks).map(|i| temp_dir.join(format!("disk{}", i + 1))).collect();
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
        drives_per_set: n_disks,
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

fn object_dir(disk: &Path, bucket: &str, object: &str) -> PathBuf {
    disk.join(bucket).join(object)
}

/// Count `part.*` data-shard files two levels below the object dir.
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

/// Delete ONLY the `xl.meta` file for an object on one disk, leaving its `part.*`
/// data shards intact (models a lost-metadata-but-present-data disk).
fn remove_xl_meta_only(disk: &Path, bucket: &str, object: &str) {
    let meta = xl_meta_path(&object_dir(disk, bucket, object));
    assert!(meta.exists(), "xl.meta must exist before removal: {meta:?}");
    std::fs::remove_file(&meta).expect("failed to remove xl.meta");
    // Data dir + part files remain.
    assert!(
        count_part_files(&object_dir(disk, bucket, object)) >= 1,
        "data shards must remain after removing only xl.meta"
    );
}

fn deep_heal_opts() -> HealOpts {
    HealOpts {
        recreate: true,
        remove: false,
        scan_mode: HealScanMode::Deep,
        ..Default::default()
    }
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

/// Enumerate every version via the B5 read-quorum listing (walks all pages).
async fn enumerate_b5(heal_storage: &Arc<ECStoreHealStorage>, bucket: &str) -> Vec<HealListItem> {
    let mut items = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let (page, next, truncated) = heal_storage
            .list_objects_for_heal_page(bucket, "", token.as_deref())
            .await
            .expect("b5 list page failed");
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

/// Enumerate every version via the disk-walk UNION enumerator (walks all pages).
async fn enumerate_disk_walk(heal_storage: &Arc<ECStoreHealStorage>, bucket: &str) -> Vec<HealListItem> {
    let mut items = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let (page, next, truncated) = heal_storage
            .list_versions_for_heal_page_disk_walk(SET_DISK_ID, bucket, "", token.as_deref())
            .await
            .expect("disk-walk list page failed");
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

mod serial_tests {
    use super::*;

    /// The enumeration gap: a version surviving on only 1/4 disks (below the
    /// read-quorum of 2) is OMITTED by the B5 `list_object_versions` enumeration
    /// but INCLUDED by the disk-walk union enumerator.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn disk_walk_page_enumerates_subquorum_version_omitted_by_list_object_versions() {
        let (disk_paths, ecstore, heal_storage) = setup_test_env_n(4).await;
        let bucket = "b920-enum-gap";
        let object = "obj.bin";
        create_versioned_bucket(&ecstore, bucket).await;

        let v1 = put_versioned(&ecstore, bucket, object, &versioned_test_data(1)).await;

        // Wipe the object entirely on disks 1..4, leaving it on ONLY disk[0]
        // (1/4 disks < read-quorum 2). effective listing_quorum for 4 drives is
        // drives/2 = 2, so B5 cannot surface a 1-of-4 version.
        for disk in &disk_paths[1..] {
            let dir = object_dir(disk, bucket, object);
            std::fs::remove_dir_all(&dir).expect("wipe object dir");
        }
        assert!(xl_meta_path(&object_dir(&disk_paths[0], bucket, object)).exists());

        let b5 = enumerate_b5(&heal_storage, bucket).await;
        assert!(
            !b5.iter().any(|it| it.version_id.as_deref() == Some(v1.as_str())),
            "B5 read-quorum enumeration MUST omit the 1-of-4 sub-quorum version, got {b5:?}"
        );

        let walk = enumerate_disk_walk(&heal_storage, bucket).await;
        assert!(
            walk.iter()
                .any(|it| it.name == object && it.version_id.as_deref() == Some(v1.as_str())),
            "disk-walk union enumeration MUST include the 1-of-4 sub-quorum version, got {walk:?}"
        );
    }

    /// DECISION 1 PIN (highest risk): xl.meta deleted on > parity disks while the
    /// data shards remain. A Deep heal (grace disabled) must REGENERATE the lost
    /// xl.meta and read back byte-identical — NOT dangling-delete the version.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn union_meta_lost_data_present_is_repaired_not_destroyed() {
        disable_dangling_grace();
        let (disk_paths, ecstore, heal_storage) = setup_test_env_n(8).await;
        let bucket = "b920-meta-lost";
        let object = "obj.bin";
        create_versioned_bucket(&ecstore, bucket).await;

        let data_v1 = versioned_test_data(7);
        let v1 = put_versioned(&ecstore, bucket, object, &data_v1).await;

        // EC4+4: parity = 4. Delete ONLY xl.meta on 5 disks (> parity), leaving the
        // data shards on all 8. Meta quorum (4) is now unreachable (3 metas), which
        // WITHOUT the guard drives delete_if_dangling -> destruction (grace=0).
        for disk in &disk_paths[0..5] {
            remove_xl_meta_only(disk, bucket, object);
            assert!(!xl_meta_path(&object_dir(disk, bucket, object)).exists());
        }

        // Heal the version through the real heal storage (Deep).
        let (_result, error) = heal_storage
            .heal_object(bucket, object, Some(&v1), &deep_heal_opts())
            .await
            .expect("heal_object call must not itself error");
        assert!(
            error.is_none(),
            "recoverable version must heal without error (must NOT be dangling-deleted): {error:?}"
        );

        // xl.meta physically regenerated on the 5 meta-wiped disks.
        for disk in &disk_paths[0..5] {
            assert!(
                xl_meta_path(&object_dir(disk, bucket, object)).exists(),
                "xl.meta must be regenerated on the meta-wiped disk {disk:?}"
            );
        }

        // The version is still present (NOT deleted) and reads back byte-identical.
        assert_eq!(
            read_version(&ecstore, bucket, object, &v1).await,
            data_v1,
            "rescued version must read back byte-identical"
        );
    }

    /// Torn minority: a version whose DATA physically survives on FEWER than
    /// data_blocks disks is genuinely unrecoverable. With grace disabled it must
    /// still be dangling-deleted (the delete path is LIVE and the guard is
    /// discriminating — it does not resurrect torn writes).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn deep_heal_torn_minority_is_dangling_deleted_with_grace_zero() {
        disable_dangling_grace();
        let (disk_paths, ecstore, heal_storage) = setup_test_env_n(4).await;
        let bucket = "b920-torn";
        let object = "obj.bin";
        create_versioned_bucket(&ecstore, bucket).await;

        let v1 = put_versioned(&ecstore, bucket, object, &versioned_test_data(3)).await;

        // EC2+2 (4 drives, parity 2, data_blocks 2). Wipe the object ENTIRELY
        // (meta + data) on 3 of 4 disks, leaving it on only 1 (< data_blocks 2):
        // genuinely unrecoverable => dangling delete after (zero) grace.
        for disk in &disk_paths[1..] {
            std::fs::remove_dir_all(object_dir(disk, bucket, object)).expect("wipe object dir");
        }
        // The surviving minority copy IS present before heal (so the delete below
        // is a real destructive action, not a no-op).
        assert_eq!(count_part_files(&object_dir(&disk_paths[0], bucket, object)), 1);

        let (_result, error) = heal_storage
            .heal_object(bucket, object, Some(&v1), &deep_heal_opts())
            .await
            .expect("heal_object call must not itself error");
        // A dangling delete reports the version as gone (FileVersionNotFound),
        // proving the destructive path fired for a genuinely torn write.
        assert!(error.is_some(), "a torn (< data_blocks) version must NOT be silently treated as healed");
        // Destructive-path PROOF: the stale minority copy on disk0 was purged by
        // the dangling delete (the guard correctly did NOT rescue a torn write).
        assert_eq!(
            count_part_files(&object_dir(&disk_paths[0], bucket, object)),
            0,
            "torn-write dangling delete must purge the surviving minority shard on disk0"
        );
        // And the wiped disks are not resurrected.
        for disk in &disk_paths[1..] {
            assert_eq!(
                count_part_files(&object_dir(disk, bucket, object)),
                0,
                "torn-write heal must not resurrect data on the wiped disk {disk:?}"
            );
        }
    }

    /// A version present on exactly data_blocks=4 disks (< listing_quorum) yet
    /// EC-reconstructable: after Deep heal the part.* + xl.meta are physically
    /// restored on the 4 wiped disks and the data reads back byte-identical. B5
    /// omits it; the disk-walk enumerates it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn deep_heal_restores_subquorum_but_reconstructable_version_wider_set() {
        disable_dangling_grace();
        let (disk_paths, ecstore, heal_storage) = setup_test_env_n(8).await;
        let bucket = "b920-reconstruct";
        let object = "obj.bin";
        create_versioned_bucket(&ecstore, bucket).await;

        let data_v1 = versioned_test_data(9);
        let v1 = put_versioned(&ecstore, bucket, object, &data_v1).await;

        // EC4+4: wipe the object ENTIRELY on 4 disks, leaving full copies on the
        // other 4 (== data_blocks). Meta quorum (4) still holds, so this heals via
        // the normal reconstruction path once enumerated by the disk walk.
        for disk in &disk_paths[0..4] {
            std::fs::remove_dir_all(object_dir(disk, bucket, object)).expect("wipe object dir");
            assert_eq!(count_part_files(&object_dir(disk, bucket, object)), 0);
        }

        // The disk-walk enumerates it (union view).
        let walk = enumerate_disk_walk(&heal_storage, bucket).await;
        assert!(
            walk.iter().any(|it| it.version_id.as_deref() == Some(v1.as_str())),
            "disk-walk must enumerate the reconstructable sub-quorum version"
        );

        let (_result, error) = heal_storage
            .heal_object(bucket, object, Some(&v1), &deep_heal_opts())
            .await
            .expect("heal_object call must not itself error");
        assert!(error.is_none(), "reconstructable version must heal cleanly: {error:?}");

        // part.* + xl.meta physically restored on the 4 wiped disks.
        for disk in &disk_paths[0..4] {
            assert!(
                xl_meta_path(&object_dir(disk, bucket, object)).exists(),
                "xl.meta restored on wiped disk {disk:?}"
            );
            assert_eq!(
                count_part_files(&object_dir(disk, bucket, object)),
                1,
                "data shard restored on wiped disk {disk:?}"
            );
        }
        assert_eq!(
            read_version(&ecstore, bucket, object, &v1).await,
            data_v1,
            "reconstructed version must read back byte-identical"
        );
    }

    /// The bounded disk-walk paginates across a multi-object bucket with
    /// batch_objects=2, healing every version exactly once with a monotonic,
    /// `dw1:`-tagged resume cursor and no anti-loop trip on the de-overlapped tail.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn disk_walk_multipage_resume_heals_each_version_once() {
        let (_disk_paths, ecstore, _heal_storage) = setup_test_env_n(4).await;
        let bucket = "b920-multipage";
        create_versioned_bucket(&ecstore, bucket).await;

        // 5 objects, 1 version each.
        let mut expected: Vec<(String, String)> = Vec::new();
        for i in 0..5u8 {
            let object = format!("obj-{i}.bin");
            let v = put_versioned(&ecstore, bucket, &object, &versioned_test_data(i + 20)).await;
            expected.push((object, v));
        }

        // Page the raw ecstore disk-walk directly with an explicit small bound so
        // pagination is actually exercised (the storage-API wrapper uses a large
        // batch that would not split this fixture).
        let mut seen: Vec<(String, Option<String>)> = Vec::new();
        let mut forward: Option<String> = None;
        let mut pages = 0usize;
        loop {
            let (versions, next_forward, truncated) = ecstore
                .heal_walk_versions_page(0, 0, bucket, "", forward.as_deref(), 2, 100_000)
                .await
                .expect("heal_walk_versions_page failed");
            pages += 1;
            for v in &versions {
                seen.push((v.name.clone(), v.version_id.clone()));
            }
            if !truncated {
                break;
            }
            let nf = next_forward.expect("truncated page must carry a next_forward");
            // Cursor must strictly advance (monotonic) to avoid loops.
            if let Some(prev) = &forward {
                assert!(&nf > prev, "resume cursor must advance monotonically: {prev} -> {nf}");
            }
            forward = Some(nf);
            assert!(pages < 20, "pagination must terminate");
        }
        assert!(pages >= 2, "batch_objects=2 over 5 objects must span multiple pages, pages={pages}");

        // Every version enumerated EXACTLY once (no drops, no duplicates from the
        // inclusive-forward de-overlap).
        assert_eq!(seen.len(), expected.len(), "each version must appear exactly once: {seen:?}");
        for (object, v) in &expected {
            let hits = seen
                .iter()
                .filter(|(n, vid)| n == object && vid.as_deref() == Some(v.as_str()))
                .count();
            assert_eq!(hits, 1, "version {object}/{v} must be enumerated exactly once, got {hits}");
        }
    }

    /// A disk that lost this object (its shard is gone) during the walk must not
    /// cause a dangling delete: the union still enumerates the version from the
    /// remaining disks (min_disks=1), and the still-quorum-present object heals
    /// cleanly and reads back byte-identical (grace disabled).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn offline_disk_during_walk_does_not_dangling_delete() {
        disable_dangling_grace();
        let (disk_paths, ecstore, heal_storage) = setup_test_env_n(4).await;
        let bucket = "b920-offline";
        let object = "obj.bin";
        create_versioned_bucket(&ecstore, bucket).await;

        let data_v1 = versioned_test_data(11);
        let v1 = put_versioned(&ecstore, bucket, object, &data_v1).await;

        // Drop the object entirely on ONE disk (its shard + meta gone), leaving it
        // on 3/4 (>= data_blocks 2). The union must still enumerate it.
        std::fs::remove_dir_all(object_dir(&disk_paths[3], bucket, object)).expect("wipe object on disk3");

        let walk = enumerate_disk_walk(&heal_storage, bucket).await;
        assert!(
            walk.iter().any(|it| it.version_id.as_deref() == Some(v1.as_str())),
            "a disk missing this object must not drop the version from the union, got {walk:?}"
        );

        // Healing must NOT dangling-delete: the object is present on 3/4 (quorum).
        // (Normal scan: the disk-walk enumerator gates on Deep OR AutoHeal, but the
        // per-version repair itself is scan-mode agnostic — this pins that a
        // missing-shard disk is reconstructed, not dangling-deleted.)
        let normal_opts = HealOpts {
            recreate: true,
            remove: false,
            ..Default::default()
        };
        let (_result, error) = heal_storage
            .heal_object(bucket, object, Some(&v1), &normal_opts)
            .await
            .expect("heal_object call must not itself error");
        assert!(error.is_none(), "quorum-present object must not be destroyed: {error:?}");
        assert!(
            xl_meta_path(&object_dir(&disk_paths[3], bucket, object)).exists(),
            "the missing shard's xl.meta must be restored on disk3"
        );
        assert_eq!(
            read_version(&ecstore, bucket, object, &v1).await,
            data_v1,
            "version must remain byte-identical after heal"
        );
    }
}
