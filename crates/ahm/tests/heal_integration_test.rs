use rustfs_ahm::heal::{
    manager::{HealConfig, HealManager},
    storage::{ECStoreHealStorage, HealStorageAPI},
    task::{HealOptions, HealPriority, HealRequest, HealTaskStatus, HealType},
};
use rustfs_common::heal_channel::{HealOpts, HealScanMode};
use rustfs_ecstore::{
    disk::endpoint::Endpoint,
    endpoints::{EndpointServerPools, Endpoints, PoolEndpoints},
    store::ECStore,
    store_api::{ObjectIO, ObjectOptions, PutObjReader, StorageAPI},
};
use serial_test::serial;
use std::sync::Once;
use std::sync::OnceLock;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::fs;
use tracing::info;
use walkdir::WalkDir;

static GLOBAL_ENV: OnceLock<(Vec<PathBuf>, Arc<ECStore>, Arc<ECStoreHealStorage>)> = OnceLock::new();
static INIT: Once = Once::new();

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
    let ecstore = ECStore::new(server_addr, endpoint_pools).await.unwrap();

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
    match (**ecstore).make_bucket(bucket_name, &Default::default()).await {
        Ok(_) => info!("Created test bucket: {}", bucket_name),
        Err(e) => {
            // If the bucket already exists from a previous test run in the shared env, ignore.
            if matches!(e, rustfs_ecstore::error::StorageError::BucketExists(_)) {
                info!("Bucket already exists, continuing: {}", bucket_name);
            } else {
                panic!("Failed to create test bucket: {e:?}");
            }
        }
    }
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_heal_object_basic() {
    let (disk_paths, ecstore, heal_storage) = setup_test_env().await;

    // Create test bucket and object
    let bucket_name = "test-bucket";
    let object_name = "test-object.txt";
    let test_data = b"Hello, this is test data for healing!";

    create_test_bucket(&ecstore, bucket_name).await;
    upload_test_object(&ecstore, bucket_name, object_name, test_data).await;

    // ─── 1️⃣ delete xl.meta file to simulate corruption ───────────────────────
    let obj_dir = disk_paths[0].join(bucket_name).join(object_name);
    let xl_meta_path = obj_dir.join("xl.meta");

    // Check if xl.meta exists (for inline data) or look for part files (for distributed data)
    let target_file = if xl_meta_path.exists() {
        println!("Found inline data in xl.meta file");
        xl_meta_path
    } else {
        // Try to find part file at depth 2, e.g. .../<uuid>/part.1
        WalkDir::new(&obj_dir)
            .min_depth(2)
            .max_depth(2)
            .into_iter()
            .filter_map(Result::ok)
            .find(|e| e.file_type().is_file() && e.file_name().to_str().map(|n| n.starts_with("part.")).unwrap_or(false))
            .map(|e| e.into_path())
            .expect("Failed to locate part file or xl.meta to delete")
    };

    std::fs::remove_file(&target_file).expect("failed to delete file");
    assert!(!target_file.exists());
    println!("✅ Deleted file: {target_file:?}");

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

    // ─── 2️⃣ verify file is restored ───────
    assert!(target_file.exists());

    info!("Heal object basic test passed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_heal_bucket_basic() {
    let (disk_paths, ecstore, heal_storage) = setup_test_env().await;

    // Create test bucket
    let bucket_name = "test-bucket-heal";
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
    let bucket_name = "test-bucket";
    let object_name = "test-object.txt";
    let test_data = b"Hello, this is test data for healing!";

    create_test_bucket(&ecstore, bucket_name).await;
    upload_test_object(&ecstore, bucket_name, object_name, test_data).await;

    let obj_dir = disk_paths[0].join(bucket_name).join(object_name);
    let xl_meta_path = obj_dir.join("xl.meta");

    // Check if xl.meta exists (for inline data) or look for part files (for distributed data)
    let target_file = if xl_meta_path.exists() {
        println!("Found inline data in xl.meta file");
        xl_meta_path
    } else {
        // Try to find part file at depth 2, e.g. .../<uuid>/part.1
        WalkDir::new(&obj_dir)
            .min_depth(2)
            .max_depth(2)
            .into_iter()
            .filter_map(Result::ok)
            .find(|e| e.file_type().is_file() && e.file_name().to_str().map(|n| n.starts_with("part.")).unwrap_or(false))
            .map(|e| e.into_path())
            .expect("Failed to locate part file or xl.meta to delete")
    };

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
    // ─── 3 verify file is restored ───────
    assert!(target_file.exists());

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
