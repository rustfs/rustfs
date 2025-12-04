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

use async_trait::async_trait;
use rustfs_ahm::{
    heal::{
        manager::{HealConfig, HealManager},
        storage::{ECStoreHealStorage, HealStorageAPI},
        task::{HealOptions, HealPriority, HealRequest, HealTaskStatus, HealType},
    },
    scanner::{ScanMode, Scanner},
};
use rustfs_common::heal_channel::{HealOpts, HealScanMode};
use rustfs_ecstore::bucket::metadata_sys::{self, set_bucket_metadata};
use rustfs_ecstore::bucket::replication::{
    DeletedObjectReplicationInfo, DynReplicationPool, GLOBAL_REPLICATION_POOL, ReplicationPoolTrait, ReplicationPriority,
};
use rustfs_ecstore::bucket::target::{BucketTarget, BucketTargetType, BucketTargets};
use rustfs_ecstore::bucket::utils::serialize;
use rustfs_ecstore::error::Error as EcstoreError;
use rustfs_ecstore::{
    disk::endpoint::Endpoint,
    endpoints::{EndpointServerPools, Endpoints, PoolEndpoints},
    store::ECStore,
    store_api::{ObjectIO, ObjectOptions, PutObjReader, StorageAPI},
};
use rustfs_filemeta::{ReplicateObjectInfo, ReplicationStatusType};
use rustfs_utils::http::headers::{AMZ_BUCKET_REPLICATION_STATUS, RESERVED_METADATA_PREFIX_LOWER};
use s3s::dto::{
    BucketVersioningStatus, Destination, ExistingObjectReplication, ExistingObjectReplicationStatus, ReplicationConfiguration,
    ReplicationRule, ReplicationRuleStatus, VersioningConfiguration,
};
use serial_test::serial;
use std::{
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::{Arc, Once, OnceLock},
    time::Duration,
};
use time::OffsetDateTime;
use tokio::fs;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::info;
use walkdir::WalkDir;

static GLOBAL_ENV: OnceLock<(Vec<PathBuf>, Arc<ECStore>, Arc<ECStoreHealStorage>)> = OnceLock::new();
static INIT: Once = Once::new();
const TEST_REPLICATION_TARGET_ARN: &str = "arn:aws:s3:::rustfs-replication-heal-target";

fn init_tracing() {
    INIT.call_once(|| {
        let _ = tracing_subscriber::fmt::try_init();
    });
}

/// Test helper: Create test environment with ECStore
async fn setup_test_env() -> (Vec<PathBuf>, Arc<ECStore>, Arc<ECStoreHealStorage>) {
    init_tracing();

    // Fast path: already initialized, just clone and return
    if let Some((paths, ecstore, heal_storage)) = GLOBAL_ENV.get() {
        return (paths.clone(), ecstore.clone(), heal_storage.clone());
    }

    // create temp dir as 4 disks with unique base dir
    let test_base_dir = format!("/tmp/rustfs_ahm_heal_test_{}", uuid::Uuid::new_v4());
    let temp_dir = std::path::PathBuf::from(&test_base_dir);
    if temp_dir.exists() {
        fs::remove_dir_all(&temp_dir).await.ok();
    }
    fs::create_dir_all(&temp_dir).await.unwrap();

    // create 4 disk dirs
    let disk_paths = vec![
        temp_dir.join("disk1"),
        temp_dir.join("disk2"),
        temp_dir.join("disk3"),
        temp_dir.join("disk4"),
    ];

    for disk_path in &disk_paths {
        fs::create_dir_all(disk_path).await.unwrap();
    }

    // create EndpointServerPools
    let mut endpoints = Vec::new();
    for (i, disk_path) in disk_paths.iter().enumerate() {
        let mut endpoint = Endpoint::try_from(disk_path.to_str().unwrap()).unwrap();
        // set correct index
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

    let endpoint_pools = EndpointServerPools(vec![pool_endpoints]);

    // format disks (only first time)
    rustfs_ecstore::store::init_local_disks(endpoint_pools.clone()).await.unwrap();

    // create ECStore with dynamic port 0 (let OS assign) or fixed 9001 if free
    let port = 9001; // for simplicity
    let server_addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let ecstore = ECStore::new(server_addr, endpoint_pools, CancellationToken::new())
        .await
        .unwrap();

    // init bucket metadata system
    let buckets_list = ecstore
        .list_bucket(&rustfs_ecstore::store_api::BucketOptions {
            no_metadata: true,
            ..Default::default()
        })
        .await
        .unwrap();
    let buckets = buckets_list.into_iter().map(|v| v.name).collect();
    rustfs_ecstore::bucket::metadata_sys::init_bucket_metadata_sys(ecstore.clone(), buckets).await;

    // Create heal storage layer
    let heal_storage = Arc::new(ECStoreHealStorage::new(ecstore.clone()));

    // Store in global once lock
    let _ = GLOBAL_ENV.set((disk_paths.clone(), ecstore.clone(), heal_storage.clone()));

    (disk_paths, ecstore, heal_storage)
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
        .put_object(bucket, object, &mut reader, &ObjectOptions::default())
        .await
        .expect("Failed to upload test object");

    info!("Uploaded test object: {}/{} ({} bytes)", bucket, object, object_info.size);
}

fn delete_first_part_file(disk_paths: &[PathBuf], bucket: &str, object: &str) -> PathBuf {
    for disk_path in disk_paths {
        let obj_dir = disk_path.join(bucket).join(object);
        if !obj_dir.exists() {
            continue;
        }

        if let Some(part_path) = WalkDir::new(&obj_dir)
            .min_depth(2)
            .max_depth(2)
            .into_iter()
            .filter_map(Result::ok)
            .find(|entry| {
                entry.file_type().is_file()
                    && entry
                        .file_name()
                        .to_str()
                        .map(|name| name.starts_with("part."))
                        .unwrap_or(false)
            })
            .map(|entry| entry.into_path())
        {
            std::fs::remove_file(&part_path).expect("Failed to delete part file");
            return part_path;
        }
    }

    panic!("Failed to locate part file for {}/{}", bucket, object);
}

fn delete_xl_meta_file(disk_paths: &[PathBuf], bucket: &str, object: &str) -> PathBuf {
    for disk_path in disk_paths {
        let xl_meta_path = disk_path.join(bucket).join(object).join("xl.meta");
        if xl_meta_path.exists() {
            std::fs::remove_file(&xl_meta_path).expect("Failed to delete xl.meta file");
            return xl_meta_path;
        }
    }

    panic!("Failed to locate xl.meta for {}/{}", bucket, object);
}

struct FormatPathGuard {
    original: PathBuf,
    backup: PathBuf,
}

impl FormatPathGuard {
    fn new(original: PathBuf) -> std::io::Result<Self> {
        let backup = original.with_extension("bak");
        if backup.exists() {
            std::fs::remove_file(&backup)?;
        }
        std::fs::rename(&original, &backup)?;
        Ok(Self { original, backup })
    }
}

impl Drop for FormatPathGuard {
    fn drop(&mut self) {
        if self.backup.exists() {
            let _ = std::fs::rename(&self.backup, &self.original);
        }
    }
}

struct PermissionGuard {
    path: PathBuf,
    original_mode: u32,
}

impl PermissionGuard {
    fn new(path: PathBuf, new_mode: u32) -> std::io::Result<Self> {
        let metadata = std::fs::metadata(&path)?;
        let original_mode = metadata.permissions().mode();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(new_mode))?;
        Ok(Self { path, original_mode })
    }
}

impl Drop for PermissionGuard {
    fn drop(&mut self) {
        if self.path.exists() {
            let _ = std::fs::set_permissions(&self.path, std::fs::Permissions::from_mode(self.original_mode));
        }
    }
}

#[derive(Debug, Default)]
struct RecordingReplicationPool {
    replica_tasks: Mutex<Vec<ReplicateObjectInfo>>,
    delete_tasks: Mutex<Vec<DeletedObjectReplicationInfo>>,
}

impl RecordingReplicationPool {
    async fn take_replica_tasks(&self) -> Vec<ReplicateObjectInfo> {
        let mut guard = self.replica_tasks.lock().await;
        guard.drain(..).collect()
    }

    async fn clear(&self) {
        self.replica_tasks.lock().await.clear();
        self.delete_tasks.lock().await.clear();
    }
}

#[async_trait]
impl ReplicationPoolTrait for RecordingReplicationPool {
    async fn queue_replica_task(&self, ri: ReplicateObjectInfo) {
        self.replica_tasks.lock().await.push(ri);
    }

    async fn queue_replica_delete_task(&self, ri: DeletedObjectReplicationInfo) {
        self.delete_tasks.lock().await.push(ri);
    }

    async fn resize(&self, _priority: ReplicationPriority, _max_workers: usize, _max_l_workers: usize) {}

    async fn init_resync(
        self: Arc<Self>,
        _cancellation_token: CancellationToken,
        _buckets: Vec<String>,
    ) -> Result<(), EcstoreError> {
        Ok(())
    }
}

async fn ensure_test_replication_pool() -> Arc<RecordingReplicationPool> {
    static TEST_POOL: OnceLock<Arc<RecordingReplicationPool>> = OnceLock::new();

    if let Some(pool) = TEST_POOL.get() {
        pool.clear().await;
        return pool.clone();
    }

    let pool = Arc::new(RecordingReplicationPool::default());
    let dyn_pool: Arc<DynReplicationPool> = pool.clone();
    let global_pool = GLOBAL_REPLICATION_POOL
        .get_or_init(|| {
            let pool_clone = dyn_pool.clone();
            async move { pool_clone }
        })
        .await
        .clone();

    assert!(
        Arc::ptr_eq(&dyn_pool, &global_pool),
        "GLOBAL_REPLICATION_POOL initialized before test replication pool"
    );

    let _ = TEST_POOL.set(pool.clone());
    pool.clear().await;
    pool
}

async fn configure_bucket_replication(bucket: &str, target_arn: &str) {
    let meta = metadata_sys::get(bucket)
        .await
        .expect("bucket metadata should exist for replication configuration");
    let mut metadata = (*meta).clone();

    let replication_rule = ReplicationRule {
        delete_marker_replication: None,
        delete_replication: None,
        destination: Destination {
            access_control_translation: None,
            account: None,
            bucket: target_arn.to_string(),
            encryption_configuration: None,
            metrics: None,
            replication_time: None,
            storage_class: None,
        },
        existing_object_replication: Some(ExistingObjectReplication {
            status: ExistingObjectReplicationStatus::from_static(ExistingObjectReplicationStatus::ENABLED),
        }),
        filter: None,
        id: Some("heal-replication-rule".to_string()),
        prefix: Some(String::new()),
        priority: Some(1),
        source_selection_criteria: None,
        status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
    };

    let replication_cfg = ReplicationConfiguration {
        role: target_arn.to_string(),
        rules: vec![replication_rule],
    };

    let bucket_targets = BucketTargets {
        targets: vec![BucketTarget {
            source_bucket: bucket.to_string(),
            endpoint: "replication.invalid".to_string(),
            target_bucket: "replication-target".to_string(),
            arn: target_arn.to_string(),
            target_type: BucketTargetType::ReplicationService,
            ..Default::default()
        }],
    };

    metadata.replication_config = Some(replication_cfg.clone());
    metadata.replication_config_xml = serialize(&replication_cfg).expect("serialize replication config");
    metadata.replication_config_updated_at = OffsetDateTime::now_utc();
    metadata.bucket_target_config = Some(bucket_targets.clone());
    metadata.bucket_targets_config_json = serde_json::to_vec(&bucket_targets).expect("serialize bucket targets");
    metadata.bucket_targets_config_updated_at = OffsetDateTime::now_utc();
    let versioning_cfg = VersioningConfiguration {
        status: Some(BucketVersioningStatus::from_static(BucketVersioningStatus::ENABLED)),
        ..Default::default()
    };
    metadata.versioning_config = Some(versioning_cfg.clone());
    metadata.versioning_config_xml = serialize(&versioning_cfg).expect("serialize versioning config");
    metadata.versioning_config_updated_at = OffsetDateTime::now_utc();

    set_bucket_metadata(bucket.to_string(), metadata)
        .await
        .expect("failed to update bucket metadata for replication");
}

mod serial_tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_heal_object_basic() {
        let (disk_paths, ecstore, heal_storage) = setup_test_env().await;

        // Create test bucket and object
        let bucket_name = "test-heal-object-basic";
        let object_name = "test-object.txt";
        let test_data = b"Hello, this is test data for healing!";

        create_test_bucket(&ecstore, bucket_name).await;
        upload_test_object(&ecstore, bucket_name, object_name, test_data).await;

        // ─── 1️⃣ delete single data shard file ─────────────────────────────────────
        let obj_dir = disk_paths[0].join(bucket_name).join(object_name);
        // find part file at depth 2, e.g. .../<uuid>/part.1
        let target_part = WalkDir::new(&obj_dir)
            .min_depth(2)
            .max_depth(2)
            .into_iter()
            .filter_map(Result::ok)
            .find(|e| e.file_type().is_file() && e.file_name().to_str().map(|n| n.starts_with("part.")).unwrap_or(false))
            .map(|e| e.into_path())
            .expect("Failed to locate part file to delete");

        std::fs::remove_file(&target_part).expect("failed to delete part file");
        assert!(!target_part.exists());
        println!("✅ Deleted shard part file: {target_part:?}");

        // Create heal manager with faster interval
        let cfg = HealConfig {
            heal_interval: Duration::from_millis(1),
            ..Default::default()
        };
        let heal_manager = HealManager::new(heal_storage.clone(), Some(cfg));
        heal_manager.start().await.unwrap();

        // Submit heal request for the object
        let heal_request = HealRequest::new(
            HealType::Object {
                bucket: bucket_name.to_string(),
                object: object_name.to_string(),
                version_id: None,
            },
            HealOptions {
                dry_run: false,
                recursive: false,
                remove_corrupted: false,
                recreate_missing: true,
                scan_mode: HealScanMode::Normal,
                update_parity: true,
                timeout: Some(Duration::from_secs(300)),
                pool_index: None,
                set_index: None,
            },
            HealPriority::Normal,
        );

        let task_id = heal_manager
            .submit_heal_request(heal_request)
            .await
            .expect("Failed to submit heal request");

        info!("Submitted heal request with task ID: {}", task_id);

        // Wait for task completion
        tokio::time::sleep(tokio::time::Duration::from_secs(8)).await;

        // Attempt to fetch task status (might be removed if finished)
        match heal_manager.get_task_status(&task_id).await {
            Ok(status) => info!("Task status: {:?}", status),
            Err(e) => info!("Task status not found (likely completed): {}", e),
        }

        // ─── 2️⃣ verify each part file is restored ───────
        assert!(target_part.exists());

        info!("Heal object basic test passed");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_heal_bucket_basic() {
        let (disk_paths, ecstore, heal_storage) = setup_test_env().await;

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
                timeout: Some(Duration::from_secs(300)),
                pool_index: None,
                set_index: None,
            },
            HealPriority::Normal,
        );

        let task_id = heal_manager
            .submit_heal_request(heal_request)
            .await
            .expect("Failed to submit bucket heal request");

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
        let (disk_paths, _ecstore, heal_storage) = setup_test_env().await;

        // ─── 1️⃣ delete format.json on one disk ──────────────
        let format_path = disk_paths[0].join(".rustfs.sys").join("format.json");
        assert!(format_path.exists(), "format.json does not exist on disk");
        std::fs::remove_file(&format_path).expect("failed to delete format.json on disk");
        assert!(!format_path.exists(), "format.json still exists after deletion");
        println!("✅ Deleted format.json on disk: {format_path:?}");

        // Create heal manager with faster interval
        let cfg = HealConfig {
            heal_interval: Duration::from_secs(2),
            ..Default::default()
        };
        let heal_manager = HealManager::new(heal_storage.clone(), Some(cfg));
        heal_manager.start().await.unwrap();

        // Wait for task completion
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        // ─── 2️⃣ verify format.json is restored ───────
        assert!(format_path.exists(), "format.json does not exist on disk after heal");

        info!("Heal format basic test passed");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_heal_format_with_data() {
        let (disk_paths, ecstore, heal_storage) = setup_test_env().await;

        // Create test bucket and object
        let bucket_name = "test-heal-format-with-data";
        let object_name = "test-object.txt";
        let test_data = b"Hello, this is test data for healing!";

        create_test_bucket(&ecstore, bucket_name).await;
        upload_test_object(&ecstore, bucket_name, object_name, test_data).await;

        let obj_dir = disk_paths[0].join(bucket_name).join(object_name);
        let target_part = WalkDir::new(&obj_dir)
            .min_depth(2)
            .max_depth(2)
            .into_iter()
            .filter_map(Result::ok)
            .find(|e| e.file_type().is_file() && e.file_name().to_str().map(|n| n.starts_with("part.")).unwrap_or(false))
            .map(|e| e.into_path())
            .expect("Failed to locate part file to delete");

        // ─── 1️⃣ delete format.json on one disk ──────────────
        let format_path = disk_paths[0].join(".rustfs.sys").join("format.json");
        std::fs::remove_dir_all(&disk_paths[0]).expect("failed to delete all contents under disk_paths[0]");
        std::fs::create_dir_all(&disk_paths[0]).expect("failed to recreate disk_paths[0] directory");
        println!("✅ Deleted format.json on disk: {:?}", disk_paths[0]);

        // Create heal manager with faster interval
        let cfg = HealConfig {
            heal_interval: Duration::from_secs(2),
            ..Default::default()
        };
        let heal_manager = HealManager::new(heal_storage.clone(), Some(cfg));
        heal_manager.start().await.unwrap();

        // Wait for task completion
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        // ─── 2️⃣ verify format.json is restored ───────
        assert!(format_path.exists(), "format.json does not exist on disk after heal");
        // ─── 3 verify each part file is restored ───────
        assert!(target_part.exists());

        info!("Heal format basic test passed");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_heal_storage_api_direct() {
        let (_disk_paths, ecstore, heal_storage) = setup_test_env().await;

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_scanner_submits_heal_task_when_part_missing() {
        let (disk_paths, ecstore, heal_storage) = setup_test_env().await;

        let bucket_name = format!("scanner-heal-bucket-{}", uuid::Uuid::new_v4().simple());
        let object_name = "scanner-heal-object.txt";
        create_test_bucket(&ecstore, &bucket_name).await;
        upload_test_object(&ecstore, &bucket_name, object_name, b"Scanner auto-heal data").await;

        let heal_cfg = HealConfig {
            enable_auto_heal: true,
            heal_interval: Duration::from_millis(20),
            max_concurrent_heals: 4,
            ..Default::default()
        };
        let heal_manager = Arc::new(HealManager::new(heal_storage.clone(), Some(heal_cfg)));
        heal_manager.start().await.unwrap();

        let scanner = Scanner::new(None, Some(heal_manager.clone()));
        scanner.initialize_with_ecstore().await;
        scanner.set_config_enable_healing(true).await;
        scanner.set_config_scan_mode(ScanMode::Deep).await;

        scanner
            .scan_cycle()
            .await
            .expect("Initial scan should succeed before simulating failures");
        let baseline_stats = heal_manager.get_statistics().await;

        let deleted_part_path = delete_first_part_file(&disk_paths, &bucket_name, object_name);
        assert!(!deleted_part_path.exists(), "Deleted part file should not exist before healing");

        scanner
            .scan_cycle()
            .await
            .expect("Scan after part deletion should finish and enqueue heal task");
        tokio::time::sleep(Duration::from_millis(500)).await;

        let updated_stats = heal_manager.get_statistics().await;
        assert!(
            updated_stats.total_tasks > baseline_stats.total_tasks,
            "Scanner should submit heal tasks when data parts go missing"
        );

        // Allow heal manager to restore the missing part
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(
            deleted_part_path.exists(),
            "Missing part should be restored after heal: {:?}",
            deleted_part_path
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_scanner_submits_metadata_heal_when_xl_meta_missing() {
        let (disk_paths, ecstore, heal_storage) = setup_test_env().await;

        let bucket_name = format!("scanner-meta-bucket-{}", uuid::Uuid::new_v4().simple());
        let object_name = "scanner-meta-object.txt";
        create_test_bucket(&ecstore, &bucket_name).await;
        upload_test_object(&ecstore, &bucket_name, object_name, b"Scanner metadata heal data").await;

        let heal_cfg = HealConfig {
            enable_auto_heal: true,
            heal_interval: Duration::from_millis(20),
            max_concurrent_heals: 4,
            ..Default::default()
        };
        let heal_manager = Arc::new(HealManager::new(heal_storage.clone(), Some(heal_cfg)));
        heal_manager.start().await.unwrap();

        let scanner = Scanner::new(None, Some(heal_manager.clone()));
        scanner.initialize_with_ecstore().await;
        scanner.set_config_enable_healing(true).await;
        scanner.set_config_scan_mode(ScanMode::Deep).await;

        scanner
            .scan_cycle()
            .await
            .expect("Initial scan should succeed before metadata deletion");
        let baseline_stats = heal_manager.get_statistics().await;

        let deleted_meta_path = delete_xl_meta_file(&disk_paths, &bucket_name, object_name);
        assert!(!deleted_meta_path.exists(), "Deleted xl.meta should not exist before healing");

        scanner
            .scan_cycle()
            .await
            .expect("Scan after metadata deletion should finish and enqueue heal task");
        tokio::time::sleep(Duration::from_millis(800)).await;

        let updated_stats = heal_manager.get_statistics().await;
        assert!(
            updated_stats.total_tasks > baseline_stats.total_tasks,
            "Scanner should submit metadata heal tasks when xl.meta is missing"
        );

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(
            deleted_meta_path.exists(),
            "xl.meta should be restored after heal: {:?}",
            deleted_meta_path
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_scanner_triggers_replication_heal_when_status_failed() {
        let (_disk_paths, ecstore, heal_storage) = setup_test_env().await;

        let bucket_name = format!("scanner-replication-bucket-{}", uuid::Uuid::new_v4().simple());
        let object_name = "scanner-replication-heal-object";
        create_test_bucket(&ecstore, &bucket_name).await;
        configure_bucket_replication(&bucket_name, TEST_REPLICATION_TARGET_ARN).await;

        let replication_pool = ensure_test_replication_pool().await;
        replication_pool.clear().await;

        let mut opts = ObjectOptions::default();
        opts.user_defined.insert(
            AMZ_BUCKET_REPLICATION_STATUS.to_string(),
            ReplicationStatusType::Failed.as_str().to_string(),
        );
        let replication_status_key = format!("{}replication-status", RESERVED_METADATA_PREFIX_LOWER);
        opts.user_defined.insert(
            replication_status_key.clone(),
            format!("{}={};", TEST_REPLICATION_TARGET_ARN, ReplicationStatusType::Failed.as_str()),
        );
        let mut reader = PutObjReader::from_vec(b"replication heal data".to_vec());
        ecstore
            .put_object(&bucket_name, object_name, &mut reader, &opts)
            .await
            .expect("Failed to upload replication test object");

        let object_info = ecstore
            .get_object_info(&bucket_name, object_name, &ObjectOptions::default())
            .await
            .expect("Failed to read object info for replication test");
        assert_eq!(
            object_info
                .user_defined
                .get(AMZ_BUCKET_REPLICATION_STATUS)
                .map(|s| s.as_str()),
            Some(ReplicationStatusType::Failed.as_str()),
            "Uploaded object should contain replication status metadata"
        );
        assert!(
            object_info
                .user_defined
                .get(&replication_status_key)
                .map(|s| s.contains(ReplicationStatusType::Failed.as_str()))
                .unwrap_or(false),
            "Uploaded object should preserve internal replication status metadata"
        );

        let heal_cfg = HealConfig {
            enable_auto_heal: true,
            heal_interval: Duration::from_millis(20),
            max_concurrent_heals: 4,
            ..Default::default()
        };
        let heal_manager = Arc::new(HealManager::new(heal_storage.clone(), Some(heal_cfg)));
        heal_manager.start().await.unwrap();

        let scanner = Scanner::new(None, Some(heal_manager.clone()));
        scanner.initialize_with_ecstore().await;
        scanner.set_config_enable_healing(true).await;
        scanner.set_config_scan_mode(ScanMode::Deep).await;

        scanner
            .scan_cycle()
            .await
            .expect("Scan cycle should succeed and evaluate replication state");

        let replica_tasks = replication_pool.take_replica_tasks().await;
        assert!(
            replica_tasks
                .iter()
                .any(|info| info.bucket == bucket_name && info.name == object_name),
            "Scanner should enqueue replication heal task when replication status is FAILED (recorded tasks: {:?})",
            replica_tasks
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_scanner_submits_erasure_set_heal_when_disk_offline() {
        let (disk_paths, _ecstore, heal_storage) = setup_test_env().await;

        let format_path = disk_paths[0].join(".rustfs.sys").join("format.json");
        assert!(format_path.exists(), "format.json should exist before simulating offline disk");
        let _format_guard = FormatPathGuard::new(format_path.clone()).expect("failed to move format.json");

        let heal_cfg = HealConfig {
            enable_auto_heal: true,
            heal_interval: Duration::from_millis(20),
            max_concurrent_heals: 2,
            ..Default::default()
        };
        let heal_manager = Arc::new(HealManager::new(heal_storage.clone(), Some(heal_cfg)));
        heal_manager.start().await.unwrap();

        let scanner = Scanner::new(None, Some(heal_manager.clone()));
        scanner.initialize_with_ecstore().await;
        scanner.set_config_enable_healing(true).await;
        scanner.set_config_scan_mode(ScanMode::Normal).await;

        let baseline_stats = heal_manager.get_statistics().await;
        scanner
            .scan_cycle()
            .await
            .expect("Scan cycle should complete even when a disk is offline");
        tokio::time::sleep(Duration::from_millis(200)).await;
        let updated_stats = heal_manager.get_statistics().await;

        assert!(
            updated_stats.total_tasks > baseline_stats.total_tasks,
            "Scanner should enqueue erasure set heal when disk is offline (before {}, after {})",
            baseline_stats.total_tasks,
            updated_stats.total_tasks
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_scanner_submits_erasure_set_heal_when_listing_volumes_fails() {
        let (disk_paths, ecstore, heal_storage) = setup_test_env().await;

        let bucket_name = format!("scanner-list-volumes-{}", uuid::Uuid::new_v4().simple());
        let object_name = "scanner-list-volumes-object";
        create_test_bucket(&ecstore, &bucket_name).await;
        upload_test_object(&ecstore, &bucket_name, object_name, b"disk list volumes failure").await;

        let heal_cfg = HealConfig {
            enable_auto_heal: true,
            heal_interval: Duration::from_millis(20),
            max_concurrent_heals: 2,
            ..Default::default()
        };
        let heal_manager = Arc::new(HealManager::new(heal_storage.clone(), Some(heal_cfg)));
        heal_manager.start().await.unwrap();

        let scanner = Scanner::new(None, Some(heal_manager.clone()));
        scanner.initialize_with_ecstore().await;
        scanner.set_config_enable_healing(true).await;
        scanner.set_config_scan_mode(ScanMode::Deep).await;

        scanner
            .scan_cycle()
            .await
            .expect("Initial scan should succeed before simulating disk permission issues");
        let baseline_stats = heal_manager.get_statistics().await;

        let disk_root = disk_paths[0].clone();
        assert!(disk_root.exists(), "Disk root should exist so we can simulate permission failures");

        {
            let _root_perm_guard =
                PermissionGuard::new(disk_root.clone(), 0o000).expect("Failed to change disk root permissions");

            let scan_result = scanner.scan_cycle().await;
            assert!(
                scan_result.is_ok(),
                "Scan cycle should continue even if disk volumes cannot be listed: {:?}",
                scan_result
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
            let updated_stats = heal_manager.get_statistics().await;

            assert!(
                updated_stats.total_tasks > baseline_stats.total_tasks,
                "Scanner should enqueue erasure set heal when listing volumes fails (before {}, after {})",
                baseline_stats.total_tasks,
                updated_stats.total_tasks
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_scanner_submits_erasure_set_heal_when_disk_access_fails() {
        let (disk_paths, ecstore, heal_storage) = setup_test_env().await;

        let bucket_name = format!("scanner-access-error-{}", uuid::Uuid::new_v4().simple());
        let object_name = "scanner-access-error-object.txt";
        create_test_bucket(&ecstore, &bucket_name).await;
        upload_test_object(&ecstore, &bucket_name, object_name, b"disk access failure").await;

        let bucket_path = disk_paths[0].join(&bucket_name);
        assert!(bucket_path.exists(), "Bucket path should exist on disk for access test");
        let _perm_guard = PermissionGuard::new(bucket_path.clone(), 0o000).expect("Failed to change permissions");

        let heal_cfg = HealConfig {
            enable_auto_heal: true,
            heal_interval: Duration::from_millis(20),
            max_concurrent_heals: 2,
            ..Default::default()
        };
        let heal_manager = Arc::new(HealManager::new(heal_storage.clone(), Some(heal_cfg)));
        heal_manager.start().await.unwrap();

        let scanner = Scanner::new(None, Some(heal_manager.clone()));
        scanner.initialize_with_ecstore().await;
        scanner.set_config_enable_healing(true).await;
        scanner.set_config_scan_mode(ScanMode::Deep).await;

        let baseline_stats = heal_manager.get_statistics().await;
        let scan_result = scanner.scan_cycle().await;
        assert!(
            scan_result.is_ok(),
            "Scan cycle should complete even if a disk volume has access errors: {:?}",
            scan_result
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        let updated_stats = heal_manager.get_statistics().await;

        assert!(
            updated_stats.total_tasks > baseline_stats.total_tasks,
            "Scanner should enqueue erasure set heal when disk access fails (before {}, after {})",
            baseline_stats.total_tasks,
            updated_stats.total_tasks
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_scanner_detects_missing_bucket_directory_and_queues_bucket_heal() {
        let (disk_paths, ecstore, heal_storage) = setup_test_env().await;

        let bucket_name = format!("scanner-missing-bucket-{}", uuid::Uuid::new_v4().simple());
        create_test_bucket(&ecstore, &bucket_name).await;
        upload_test_object(&ecstore, &bucket_name, "seed-object", b"bucket heal data").await;

        let scanner_heal_cfg = HealConfig {
            enable_auto_heal: true,
            heal_interval: Duration::from_millis(20),
            max_concurrent_heals: 4,
            ..Default::default()
        };
        let scanner_heal_manager = Arc::new(HealManager::new(heal_storage.clone(), Some(scanner_heal_cfg)));
        scanner_heal_manager.start().await.unwrap();

        let scanner = Scanner::new(None, Some(scanner_heal_manager.clone()));
        scanner.initialize_with_ecstore().await;
        scanner.set_config_enable_healing(true).await;
        scanner.set_config_scan_mode(ScanMode::Normal).await;

        scanner
            .scan_cycle()
            .await
            .expect("Initial scan should succeed before deleting bucket directory");
        let baseline_stats = scanner_heal_manager.get_statistics().await;

        let missing_dir = disk_paths[0].join(&bucket_name);
        assert!(missing_dir.exists());
        std::fs::remove_dir_all(&missing_dir).expect("Failed to remove bucket directory for heal simulation");
        assert!(!missing_dir.exists(), "Bucket directory should be removed on disk to trigger heal");

        scanner
            .run_volume_consistency_check()
            .await
            .expect("Volume consistency check should run after bucket removal");
        tokio::time::sleep(Duration::from_millis(800)).await;

        let updated_stats = scanner_heal_manager.get_statistics().await;
        assert!(
            updated_stats.total_tasks > baseline_stats.total_tasks,
            "Scanner should submit bucket heal tasks when a bucket directory is missing"
        );

        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(missing_dir.exists(), "Bucket directory should be restored after heal");
    }
}
