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

#![recursion_limit = "256"]

use http::HeaderMap;
use rustfs_heal::heal::{
    manager::{HealConfig, HealManager},
    storage::{ECStoreHealStorage, HealObjectOptions as ObjectOptions, HealPutObjReader as PutObjReader, HealStorageAPI},
    task::{HealOptions, HealPriority, HealRequest, HealTaskStatus, HealType},
};
use rustfs_heal_contracts::heal_channel::{HealOpts, HealScanMode};
use serial_test::serial;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::fs;
use tracing::info;
use walkdir::WalkDir;

mod storage_api;

use storage_api::integration::{BucketOperations, ECStore, ObjectIO as _, ObjectOperations as _, WriteCompletion};

const HEAL_FORMAT_WAIT_TIMEOUT: Duration = Duration::from_secs(25);
const HEAL_FORMAT_WAIT_INTERVAL: Duration = Duration::from_millis(250);
const NON_INLINE_TEST_DATA_SIZE: usize = 256 * 1024 + 137;

fn non_inline_test_data() -> Vec<u8> {
    (0..NON_INLINE_TEST_DATA_SIZE).map(|idx| (idx % 251) as u8).collect()
}

async fn wait_for_path_exists(path: &Path, timeout: Duration, interval: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if path.exists() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(interval).await;
    }
}

fn find_part_file(obj_dir: &Path) -> Option<PathBuf> {
    WalkDir::new(obj_dir)
        .min_depth(2)
        .max_depth(2)
        .into_iter()
        .filter_map(Result::ok)
        .find(|entry| entry.file_type().is_file() && entry.file_name().to_str().is_some_and(|name| name.starts_with("part.")))
        .map(|entry| entry.into_path())
}

async fn wait_for_object_copies(disks: &[PathBuf], bucket: &str, object: &str) {
    tokio::time::timeout(HEAL_FORMAT_WAIT_TIMEOUT, async {
        loop {
            if disks.iter().all(|disk| {
                let obj_dir = disk.join(bucket).join(object);
                obj_dir.join("xl.meta").exists() && find_part_file(&obj_dir).is_some()
            }) {
                break;
            }
            tokio::time::sleep(HEAL_FORMAT_WAIT_INTERVAL).await;
        }
    })
    .await
    .expect("PUT rename tails must converge before corrupting the disk fixture");
}

/// Test helper: build the shared 4-disk temp-dir ECStore environment
/// (rustfs-test-utils, backlog#1153 infra-1) and wrap it in the heal storage
/// layer. Port 0 + uuid temp dirs keep this parallel-safe under nextest.
async fn heal_env() -> (Vec<PathBuf>, Arc<ECStore>, Arc<ECStoreHealStorage>) {
    let env = rustfs_test_utils::TestECStoreEnv::builder()
        .prefix("rustfs_heal_heal_test")
        .build()
        .await;
    let heal_storage = Arc::new(ECStoreHealStorage::new(env.ecstore.clone()));
    (env.disk_paths, env.ecstore, heal_storage)
}

/// Test helper: Create a test bucket
async fn create_test_bucket(ecstore: &Arc<ECStore>, bucket_name: &str) {
    (**ecstore)
        .make_bucket(bucket_name, &Default::default())
        .await
        .expect("Failed to create test bucket");
    info!("Created test bucket: {}", bucket_name);
}

/// Test helper: Upload test object
async fn upload_test_object(ecstore: &Arc<ECStore>, bucket: &str, object: &str, data: &[u8]) {
    let mut reader = PutObjReader::from_vec(data.to_vec());
    let object_info = (**ecstore)
        .put_object(
            bucket,
            object,
            &mut reader,
            &ObjectOptions {
                write_completion: WriteCompletion::TailDrained,
                ..Default::default()
            },
        )
        .await
        .expect("Failed to upload test object");

    info!("Uploaded test object: {}/{} ({} bytes)", bucket, object, object_info.size);
}

async fn empty_replacement_with_committed_delete(
    ecstore: &Arc<ECStore>,
    disk_paths: &[PathBuf],
    bucket: &str,
    object: &str,
) -> (uuid::Uuid, uuid::Uuid) {
    use rustfs_filemeta::FileInfo;
    use storage_api::integration::{
        DeleteOptions, DeleteRecoveryAction, DeleteRecoveryState, DiskAPI as _, DiskSetSelector, StorageAdminApi as _,
    };
    assert_eq!(disk_paths.len(), 4, "replacement fixture needs four paths");
    let incarnation = ecstore
        .bucket_incarnation_id_from_disk(bucket)
        .await
        .expect("admit the survivor bucket generation");
    let disks = ecstore
        .disk_set_inventory(DiskSetSelector::new(0, 0))
        .await
        .expect("read the live fixture set");
    assert_eq!(disks.len(), 4);
    let transaction = uuid::Uuid::new_v4();
    // Retain a certified survivor decision without the normal delete's
    // immediate recovery/cleanup consuming the fault input.
    for disk in &disks[1..] {
        let disk = disk.as_ref().expect("all three preparation survivors are online");
        disk.delete_version_recoverable(
            bucket,
            object,
            FileInfo {
                name: object.to_owned(),
                ..Default::default()
            },
            false,
            DeleteOptions {
                old_data_dir: Some(transaction),
                delete_recovery_bucket_incarnation: Some(incarnation),
                ..Default::default()
            },
        )
        .await
        .expect("prepare the same delete on every survivor");
    }
    for disk in &disks[1..] {
        let disk = disk.as_ref().expect("all three survivors are online");
        let prepared = disk
            .inspect_delete_transaction(bucket, object, transaction)
            .await
            .expect("inspect prepared survivor decision");
        assert_eq!(prepared.state, DeleteRecoveryState::NewPrepared);
        let committed = disk
            .settle_delete_transaction(bucket, object, transaction, prepared.fingerprint, DeleteRecoveryAction::PublishCommit)
            .await
            .expect("publish the retained survivor decision");
        assert_eq!(committed.state, DeleteRecoveryState::QuorumCommitted);
    }
    // Linux disk I/O pins the root inode. Clearing its contents models the
    // empty target on every platform; renaming it leaves Linux handles on
    // the retired data and does not simulate a replacement for this test.
    for entry in std::fs::read_dir(&disk_paths[0]).expect("read target root") {
        let entry = entry.expect("read target entry");
        if entry.file_type().expect("target entry type").is_dir() {
            std::fs::remove_dir_all(entry.path()).expect("clear target directory");
        } else {
            std::fs::remove_file(entry.path()).expect("clear target file");
        }
    }
    (incarnation, transaction)
}

mod serial_tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_heal_object_basic() {
        let (disk_paths, ecstore, heal_storage) = heal_env().await;

        // Create test bucket and object
        let bucket_name = "test-heal-object-basic";
        let object_name = "test-object.txt";
        let test_data = non_inline_test_data();

        create_test_bucket(&ecstore, bucket_name).await;
        upload_test_object(&ecstore, bucket_name, object_name, &test_data).await;
        wait_for_object_copies(&disk_paths, bucket_name, object_name).await;
        // ─── 1️⃣ delete single data shard file ─────────────────────────────────────
        let obj_dir = disk_paths[0].join(bucket_name).join(object_name);
        let target_part = find_part_file(&obj_dir).expect("converged fixture must contain a part file");

        std::fs::remove_file(&target_part).expect("failed to delete part file");
        assert!(!target_part.exists());
        println!("✅ Deleted shard part file: {target_part:?}");

        let heal_opts = HealOpts {
            recreate: true,
            remove: false,
            update_parity: true,
            ..Default::default()
        };
        let (object_result, object_error) = heal_storage
            .heal_object(bucket_name, object_name, None, &heal_opts)
            .await
            .expect("failed to heal object");
        info!("heal_object result: {:?}, error: {:?}", object_result, object_error);
        assert!(object_error.is_none(), "heal_object returned error: {object_error:?}");

        // `test_heal_format_with_data` covers on-disk shard restoration. Here we
        // focus on the object-level healing contract: the object must remain
        // readable with intact contents after healing.
        let mut reader = ecstore
            .get_object_reader(bucket_name, object_name, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("Failed to get object reader after heal");

        let mut downloaded_data = Vec::new();
        tokio::io::copy(&mut reader, &mut downloaded_data)
            .await
            .expect("Failed to read healed object data");

        assert_eq!(downloaded_data, test_data, "Healed object data does not match original");

        info!("Heal object basic test passed");
    }

    // Regression for PR #4356 review (issues #3231, #3191): healing an object that
    // needs no shard repair must still reclaim leaked pre-#3510 data dirs.
    //
    // The reclaim was originally wired only into `heal_object`'s post-heal tail,
    // which is unreachable when `disks_to_heal_count == 0` — exactly the state of
    // the objects the sweep targets (valid `xl.meta`, all shards present, plus one
    // orphaned UUID data dir). A healthy heal took the early return and reclaimed
    // nothing. This drives the full heal path on an untouched object and asserts
    // the stray dir is swept, while a dry-run heal leaves it in place.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_heal_object_reclaims_orphan_data_dir_when_healthy() {
        let (disk_paths, ecstore, heal_storage) = heal_env().await;

        let bucket_name = "test-heal-reclaim-orphan";
        let object_name = "healthy-object.bin";
        let test_data = non_inline_test_data();

        create_test_bucket(&ecstore, bucket_name).await;
        upload_test_object(&ecstore, bucket_name, object_name, &test_data).await;
        wait_for_object_copies(&disk_paths, bucket_name, object_name).await;

        // Plant a leaked, unreferenced UUID data dir under the object on every disk
        // that actually holds the object (i.e. has an `xl.meta`). Planting on a disk
        // without `xl.meta` would (correctly) trip the fail-closed guard and abort
        // the whole reclaim, so we only seed disks that carry the object.
        let orphan_dir = uuid::Uuid::new_v4().to_string();
        let mut seeded_orphans: Vec<PathBuf> = Vec::new();
        for disk_path in &disk_paths {
            let obj_dir = disk_path.join(bucket_name).join(object_name);
            if !obj_dir.join("xl.meta").exists() {
                continue;
            }
            let stray = obj_dir.join(&orphan_dir);
            fs::create_dir_all(&stray).await.expect("orphan data dir should be created");
            fs::write(stray.join("part.1"), b"leaked pre-#3510 data")
                .await
                .expect("orphan part should be written");
            seeded_orphans.push(stray);
        }
        assert!(
            !seeded_orphans.is_empty(),
            "test setup failed: no disk carried the object to seed an orphan under"
        );

        let dry_run_opts = HealOpts {
            dry_run: true,
            recreate: true,
            remove: false,
            update_parity: true,
            ..Default::default()
        };
        let (_res, err) = heal_storage
            .heal_object(bucket_name, object_name, None, &dry_run_opts)
            .await
            .expect("dry-run heal should succeed");
        assert!(err.is_none(), "dry-run heal returned error: {err:?}");
        for stray in &seeded_orphans {
            assert!(stray.exists(), "dry-run heal must NOT remove the orphan data dir: {stray:?}");
        }

        // Snapshot the live (referenced) data dirs so we can assert they survive.
        let mut live_dirs: Vec<PathBuf> = Vec::new();
        for disk_path in &disk_paths {
            let obj_dir = disk_path.join(bucket_name).join(object_name);
            if !obj_dir.join("xl.meta").exists() {
                continue;
            }
            for entry in WalkDir::new(&obj_dir)
                .min_depth(1)
                .max_depth(1)
                .into_iter()
                .filter_map(Result::ok)
            {
                let name = entry.file_name().to_string_lossy().to_string();
                if entry.file_type().is_dir() && name != orphan_dir {
                    live_dirs.push(entry.into_path());
                }
            }
        }
        assert!(!live_dirs.is_empty(), "expected at least one live data dir to remain");

        let heal_opts = HealOpts {
            dry_run: false,
            recreate: true,
            remove: false,
            update_parity: true,
            ..Default::default()
        };
        let (_res, err) = heal_storage
            .heal_object(bucket_name, object_name, None, &heal_opts)
            .await
            .expect("heal should succeed");
        assert!(err.is_none(), "heal returned error: {err:?}");

        for stray in &seeded_orphans {
            assert!(!stray.exists(), "healthy heal must reclaim the orphan data dir: {stray:?}");
        }
        for live in &live_dirs {
            assert!(live.exists(), "referenced data dir must be preserved: {live:?}");
        }
        for disk_path in &disk_paths {
            let obj_dir = disk_path.join(bucket_name).join(object_name);
            if obj_dir.exists() {
                assert!(obj_dir.join("xl.meta").exists(), "xl.meta must be preserved: {obj_dir:?}");
            }
        }

        // The object must remain intact and readable after the reclaim.
        let mut reader = ecstore
            .get_object_reader(bucket_name, object_name, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("Failed to get object reader after reclaim");
        let mut downloaded_data = Vec::new();
        tokio::io::copy(&mut reader, &mut downloaded_data)
            .await
            .expect("Failed to read object after reclaim");
        assert_eq!(downloaded_data, test_data, "object contents must survive orphan reclaim");

        info!("Heal object orphan-reclaim test passed");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_heal_bucket_basic() {
        let (disk_paths, ecstore, heal_storage) = heal_env().await;

        // Create test bucket
        let bucket_name = "test-heal-bucket-basic";
        create_test_bucket(&ecstore, bucket_name).await;

        // ─── 1️⃣ delete bucket dir on disk ──────────────
        let broken_bucket_path = disk_paths[0].join(bucket_name);
        assert!(broken_bucket_path.exists(), "bucket dir does not exist on disk");
        std::fs::remove_dir_all(&broken_bucket_path).expect("failed to delete bucket dir on disk");
        assert!(!broken_bucket_path.exists(), "bucket dir still exists after deletion");
        println!("✅ Deleted bucket directory on disk: {broken_bucket_path:?}");

        // Create heal manager with faster interval
        let cfg = HealConfig {
            heal_interval: Duration::from_millis(1),
            ..Default::default()
        };
        let heal_manager = HealManager::new(heal_storage.clone(), Some(cfg));
        heal_manager.start().await.unwrap();

        // Submit heal request for the bucket
        let heal_request = HealRequest::new(
            HealType::Bucket {
                bucket: bucket_name.to_string(),
            },
            HealOptions {
                dry_run: false,
                recursive: true,
                remove_corrupted: false,
                recreate_missing: false,
                scan_mode: HealScanMode::Normal,
                update_parity: false,
                no_lock: false,
                timeout: Some(Duration::from_secs(300)),
                pool_index: None,
                set_index: None,
            },
            HealPriority::Normal,
        );

        let task_id = heal_request.id.clone();
        let admission = heal_manager
            .submit_heal_request(heal_request)
            .await
            .expect("Failed to submit bucket heal request");
        assert!(admission.is_admitted(), "bucket heal request should be admitted");

        info!("Submitted bucket heal request with task ID: {}", task_id);

        // Wait for task completion
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        // Attempt to fetch task status (optional)
        if let Ok(status) = heal_manager.get_task_status(&task_id).await {
            if status == HealTaskStatus::Completed {
                info!("Bucket heal task status: {:?}", status);
            } else {
                panic!("Bucket heal task status: {status:?}");
            }
        }

        // ─── 3️⃣ Verify bucket directory is restored on every disk ───────
        assert!(broken_bucket_path.exists(), "bucket dir does not exist on disk");

        info!("Heal bucket basic test passed");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_heal_format_basic() {
        let (disk_paths, _ecstore, heal_storage) = heal_env().await;

        // ─── 1️⃣ delete format.json on one disk ──────────────
        let format_path = disk_paths[0].join(".rustfs.sys").join("format.json");
        assert!(format_path.exists(), "format.json does not exist on disk");
        std::fs::remove_file(&format_path).expect("failed to delete format.json on disk");
        assert!(!format_path.exists(), "format.json still exists after deletion");
        println!("✅ Deleted format.json on disk: {format_path:?}");

        let (_format_result, format_error) = heal_storage.heal_format(false).await.expect("failed to run heal_format");
        if let Some(err) = format_error {
            info!("heal_format returned error: {:?}", err);
        }

        let restored = wait_for_path_exists(&format_path, HEAL_FORMAT_WAIT_TIMEOUT, HEAL_FORMAT_WAIT_INTERVAL).await;
        assert!(restored, "format.json does not exist on disk after heal");

        // ─── 2️⃣ verify format.json is restored ───────
        assert!(format_path.exists(), "format.json does not exist on disk after heal");

        info!("Heal format basic test passed");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_heal_format_with_data() {
        let (disk_paths, ecstore, heal_storage) = heal_env().await;

        // Create test bucket and object
        let bucket_name = "test-heal-format-with-data";
        let object_name = "test-object.txt";
        let test_data = non_inline_test_data();

        create_test_bucket(&ecstore, bucket_name).await;
        upload_test_object(&ecstore, bucket_name, object_name, &test_data).await;
        wait_for_object_copies(&disk_paths, bucket_name, object_name).await;
        let obj_dir = disk_paths[0].join(bucket_name).join(object_name);
        let target_part = find_part_file(&obj_dir).expect("converged fixture must contain a part file");

        // ─── 1️⃣ delete format.json on one disk ──────────────
        let format_path = disk_paths[0].join(".rustfs.sys").join("format.json");
        let failed_disk_path = disk_paths[0].with_extension("failed");
        std::fs::rename(&disk_paths[0], &failed_disk_path).expect("failed to detach disk_paths[0]");
        std::fs::create_dir_all(&disk_paths[0]).expect("failed to recreate disk_paths[0] directory");
        assert!(!format_path.exists(), "replacement disk already contains format.json before heal");
        println!("✅ Deleted format.json on disk: {:?}", disk_paths[0]);

        let (_format_result, format_error) = heal_storage.heal_format(false).await.expect("failed to run heal_format");
        if let Some(err) = format_error {
            info!("heal_format returned warning/error: {:?}", err);
        }

        let bucket_heal_opts = HealOpts {
            recursive: true,
            recreate: true,
            ..Default::default()
        };
        heal_storage
            .heal_bucket(bucket_name, &bucket_heal_opts)
            .await
            .expect("failed to heal bucket");

        let heal_opts = HealOpts {
            recreate: true,
            remove: false,
            ..Default::default()
        };
        let (object_result, object_error) = heal_storage
            .heal_object(bucket_name, object_name, None, &heal_opts)
            .await
            .expect("failed to heal object");
        info!("heal_object result: {:?}, error: {:?}", object_result, object_error);
        assert!(object_error.is_none(), "heal_object returned error: {object_error:?}");

        let format_restored = wait_for_path_exists(&format_path, HEAL_FORMAT_WAIT_TIMEOUT, HEAL_FORMAT_WAIT_INTERVAL).await;
        assert!(format_restored, "format.json does not exist on disk after heal");
        let target_restored = wait_for_path_exists(&target_part, HEAL_FORMAT_WAIT_TIMEOUT, HEAL_FORMAT_WAIT_INTERVAL).await;

        // ─── 3️⃣ verify format.json is restored ───────
        assert!(format_path.exists(), "format.json does not exist on disk after heal");
        assert!(target_restored, "part file was not restored after heal");

        // ─── 3️⃣ verify each part file is restored ───────
        assert!(target_part.exists());

        // Verify object metadata is accessible
        let obj_info = ecstore
            .get_object_info(bucket_name, object_name, &ObjectOptions::default())
            .await
            .expect("Expected object to be readable after heal");
        assert_eq!(obj_info.size as usize, test_data.len());

        // Actually read the object data to verify integrity
        let mut reader = ecstore
            .get_object_reader(bucket_name, object_name, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("Failed to get object reader");

        let mut downloaded_data = Vec::new();
        tokio::io::copy(&mut reader, &mut downloaded_data)
            .await
            .expect("Failed to read object data");

        assert_eq!(downloaded_data, test_data, "Healed object data does not match original");

        info!("Heal format with data test passed");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_heal_storage_api_direct() {
        let (_disk_paths, ecstore, heal_storage) = heal_env().await;

        // Test direct heal storage API calls

        // Test heal_format
        let format_result = heal_storage.heal_format(true).await; // dry run
        assert!(format_result.is_ok());
        info!("Direct heal_format test passed");

        // Test heal_bucket
        let bucket_name = "test-bucket-direct";
        create_test_bucket(&ecstore, bucket_name).await;

        let heal_opts = HealOpts {
            recursive: true,
            dry_run: true,
            remove: false,
            recreate: false,
            scan_mode: HealScanMode::Normal,
            update_parity: false,
            no_lock: false,
            read_repair: false,
            pool: None,
            set: None,
        };

        let bucket_result = heal_storage.heal_bucket(bucket_name, &heal_opts).await;
        assert!(bucket_result.is_ok());
        info!("Direct heal_bucket test passed");

        // Test heal_object
        let object_name = "test-object-direct.txt";
        let test_data = b"Test data for direct heal API";
        upload_test_object(&ecstore, bucket_name, object_name, test_data).await;

        let object_heal_opts = HealOpts {
            recursive: false,
            dry_run: true,
            remove: false,
            recreate: false,
            scan_mode: HealScanMode::Normal,
            update_parity: false,
            no_lock: false,
            read_repair: false,
            pool: None,
            set: None,
        };

        let object_result = heal_storage
            .heal_object(bucket_name, object_name, None, &object_heal_opts)
            .await;
        assert!(object_result.is_ok());
        info!("Direct heal_object test passed");

        info!("Direct heal storage API test passed");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn replacement_delete_recovery_on_fresh_target() {
    use rustfs_heal::Error;
    use rustfs_heal::heal::task::HealTask;
    use rustfs_heal_contracts::heal_channel::HealRequestSource;
    use storage_api::integration::{
        DeleteRecoveryState, DiskAPI as _, DiskError, DiskSetSelector, EcstoreStorageError, StorageAdminApi as _,
    };

    for (source, erasure_set) in [
        (HealRequestSource::AutoHeal, true),
        (HealRequestSource::Admin, true),
        (HealRequestSource::Admin, false),
    ] {
        let (disk_paths, ecstore, storage) = heal_env().await;
        let bucket = "replacement-delete-prepass";
        create_test_bucket(&ecstore, bucket).await;
        let original = non_inline_test_data();
        for key in ["live", "deleted"] {
            upload_test_object(&ecstore, bucket, key, &original).await;
            wait_for_object_copies(&disk_paths, bucket, key).await;
        }
        let (incarnation, transaction) = empty_replacement_with_committed_delete(&ecstore, &disk_paths, bucket, "deleted").await;
        let updated = vec![17; NON_INLINE_TEST_DATA_SIZE];
        upload_test_object(&ecstore, bucket, "live", &updated).await;
        upload_test_object(&ecstore, bucket, "outage-created", &original).await;
        let (_, error) = storage.heal_format(false).await.expect("format target");
        assert!(error.is_none(), "{error:?}");
        assert_eq!(
            ecstore
                .bucket_incarnation_id_from_disk(bucket)
                .await
                .expect("survivor incarnation remains authoritative"),
            incarnation
        );
        let current_disks = ecstore
            .disk_set_inventory(DiskSetSelector::new(0, 0))
            .await
            .expect("read the formatted live set");
        assert!(
            matches!(
                current_disks[0]
                    .as_ref()
                    .expect("formatted target is attached")
                    .stat_volume(bucket)
                    .await,
                Err(DiskError::VolumeNotFound)
            ),
            "the formatted target must still lack its publication bucket"
        );
        for disk in &current_disks[1..] {
            let retained = disk
                .as_ref()
                .expect("survivor remains attached")
                .inspect_delete_transaction(bucket, "deleted", transaction)
                .await
                .expect("retained committed survivor proof");
            assert_eq!(retained.state, DeleteRecoveryState::QuorumCommitted);
        }
        // A committed survivor journal must be adopted, but publishing its
        // witness cannot recreate a missing bucket outside lifecycle ownership.
        let premature = storage
            .recover_delete_transactions(bucket, None, None, &HealOpts::default())
            .await;
        assert!(
            matches!(&premature, Err(Error::Storage(EcstoreStorageError::FileNotFound))),
            "{source:?} erasure_set={erasure_set}: {premature:?}"
        );
        let heal_type = if erasure_set {
            HealType::ErasureSet {
                buckets: vec![bucket.to_owned()],
                set_disk_id: "pool_0_set_0".to_owned(),
            }
        } else {
            HealType::Bucket {
                bucket: bucket.to_owned(),
            }
        };
        let mut request = HealRequest::new(
            heal_type,
            HealOptions {
                recursive: true,
                pool_index: Some(0),
                set_index: Some(0),
                scan_mode: HealScanMode::Deep,
                ..Default::default()
            },
            HealPriority::Normal,
        );
        request.source = source;
        if !erasure_set {
            request.bucket_incarnation_id = Some(incarnation);
        }
        HealTask::from_request(request, storage.clone())
            .execute()
            .await
            .unwrap_or_else(|err| panic!("{source:?} erasure_set={erasure_set}: {err}"));
        // Check physical repair before GET can hide missing shards via read repair.
        for key in ["live", "outage-created"] {
            assert!(disk_paths[0].join(bucket).join(key).join("xl.meta").exists());
            assert!(find_part_file(&disk_paths[0].join(bucket).join(key)).is_some());
        }
        for (key, expected) in [("live", updated.as_slice()), ("outage-created", original.as_slice())] {
            let mut reader = ecstore
                .get_object_reader(bucket, key, None, HeaderMap::new(), &ObjectOptions::default())
                .await
                .expect("open rebuilt object");
            assert_eq!(reader.read_all().await.expect("read rebuilt bytes"), expected);
        }
        assert!(matches!(
            ecstore.get_object_info(bucket, "deleted", &ObjectOptions::default()).await,
            Err(EcstoreStorageError::ObjectNotFound(_, _))
        ));
        assert_eq!(
            ecstore
                .bucket_incarnation_id_from_disk(bucket)
                .await
                .expect("retained incarnation"),
            incarnation
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn bucket_removal_recovers_delete_proofs_before_removing_empty_volumes() {
    use rustfs_heal::heal::task::HealTask;
    use rustfs_heal_contracts::heal_channel::HealRequestSource;
    use storage_api::integration::{DeleteRecoveryState, DiskAPI as _, DiskSetSelector, StorageAdminApi as _};
    let (disk_paths, ecstore, storage) = heal_env().await;
    let bucket = "delete-proof-before-volume-removal";
    create_test_bucket(&ecstore, bucket).await;
    upload_test_object(&ecstore, bucket, "deleted", &non_inline_test_data()).await;
    wait_for_object_copies(&disk_paths, bucket, "deleted").await;
    let (incarnation, transaction) = empty_replacement_with_committed_delete(&ecstore, &disk_paths, bucket, "deleted").await;
    let (_, error) = storage.heal_format(false).await.expect("format target");
    assert!(error.is_none());
    storage
        .heal_bucket(
            bucket,
            &HealOpts {
                recreate: true,
                ..Default::default()
            },
        )
        .await
        .expect("empty target volume");
    assert!(disk_paths[0].join(bucket).exists());
    let disks = ecstore
        .disk_set_inventory(DiskSetSelector::new(0, 0))
        .await
        .expect("live removal fixture set");
    for disk in &disks[1..] {
        let retained = disk
            .as_ref()
            .expect("removal survivor remains attached")
            .inspect_delete_transaction(bucket, "deleted", transaction)
            .await
            .expect("removal must still adopt the retained delete proof");
        assert_eq!(retained.state, DeleteRecoveryState::QuorumCommitted);
    }
    let mut request = HealRequest::new(
        HealType::Bucket {
            bucket: bucket.to_owned(),
        },
        HealOptions {
            recursive: false,
            remove_corrupted: true,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    request.source = HealRequestSource::Admin;
    request.bucket_incarnation_id = Some(incarnation);
    HealTask::from_request(request, storage)
        .execute()
        .await
        .expect("settle journals before deleting empty volumes");
    assert!(
        disk_paths.iter().all(|path| !path.join(bucket).exists()),
        "all empty replicas should be removed after proof settlement"
    );
}
