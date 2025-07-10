use rustfs_ahm::heal::{
    manager::HealManager,
    storage::{ECStoreHealStorage, HealStorageAPI},
    task::{HealOptions, HealPriority, HealRequest, HealType, HEAL_NORMAL_SCAN},
};
use rustfs_ecstore::{
    disk::endpoint::Endpoint,
    endpoints::{EndpointServerPools, Endpoints, PoolEndpoints},
    store::ECStore,
    store_api::{PutObjReader, ObjectOptions, StorageAPI, ObjectIO},
};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::fs;
use tracing::info;

/// Test helper: Create test environment with ECStore
async fn setup_test_env() -> (Vec<PathBuf>, Arc<ECStore>, Arc<ECStoreHealStorage>) {
    // create temp dir as 4 disks
    let test_base_dir = "/tmp/rustfs_ahm_heal_test";
    let temp_dir = std::path::PathBuf::from(test_base_dir);
    if temp_dir.exists() {
        fs::remove_dir_all(&temp_dir).await.unwrap();
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

    // format disks
    rustfs_ecstore::store::init_local_disks(endpoint_pools.clone()).await.unwrap();

    // create ECStore with dynamic port
    let port = 9001;
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
    
    (disk_paths, ecstore, heal_storage)
}

/// Test helper: Create a test bucket
async fn create_test_bucket(ecstore: &Arc<ECStore>, bucket_name: &str) {
    (&**ecstore)
        .make_bucket(bucket_name, &Default::default())
        .await
        .expect("Failed to create test bucket");
    info!("Created test bucket: {}", bucket_name);
}

/// Test helper: Upload test object
async fn upload_test_object(
    ecstore: &Arc<ECStore>,
    bucket: &str,
    object: &str,
    data: &[u8],
) {
    let mut reader = PutObjReader::from_vec(data.to_vec());
    let object_info = (&**ecstore)
        .put_object(bucket, object, &mut reader, &ObjectOptions::default())
        .await
        .expect("Failed to upload test object");
    
    info!(
        "Uploaded test object: {}/{} ({} bytes)",
        bucket, object, object_info.size
    );
}

/// Test helper: Cleanup test environment
async fn cleanup_test_env(disk_paths: &[PathBuf]) {
    for disk_path in disk_paths {
        if disk_path.exists() {
            fs::remove_dir_all(disk_path).await.expect("Failed to cleanup disk path");
        }
    }
    
    // Clean up test base directory
    let test_base_dir = PathBuf::from("/tmp/rustfs_ahm_heal_test");
    if test_base_dir.exists() {
        fs::remove_dir_all(&test_base_dir).await.expect("Failed to cleanup test base directory");
    }
    
    info!("Test environment cleaned up");
}

#[tokio::test]
async fn test_heal_object_basic() {
    let (disk_paths, ecstore, heal_storage) = setup_test_env().await;
    
    // Create test bucket and object
    let bucket_name = "test-bucket";
    let object_name = "test-object.txt";
    let test_data = b"Hello, this is test data for healing!";
    
    create_test_bucket(&ecstore, bucket_name).await;
    upload_test_object(&ecstore, bucket_name, object_name, test_data).await;
    
    // Create heal manager
    let heal_manager = HealManager::new(heal_storage.clone(), Default::default());
    
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
            scan_mode: HEAL_NORMAL_SCAN,
            update_parity: true,
            timeout: Some(Duration::from_secs(300)),
        },
        HealPriority::Normal,
    );
    
    let task_id = heal_manager
        .submit_heal_request(heal_request)
        .await
        .expect("Failed to submit heal request");
    
    info!("Submitted heal request with task ID: {}", task_id);
    
    // Wait for task completion
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    
    // Check task status
    let task_status = heal_manager.get_task_status(&task_id).await;
    assert!(task_status.is_ok());
    
    let status = task_status.unwrap();
    info!("Task status: {:?}", status);
    
    // Verify object still exists and is accessible
    let object_exists = heal_storage.object_exists(bucket_name, object_name).await;
    assert!(object_exists.is_ok());
    assert!(object_exists.unwrap());
    
    // Cleanup
    cleanup_test_env(&disk_paths).await;
    
    info!("Heal object basic test passed");
}

#[tokio::test]
async fn test_heal_bucket_basic() {
    let (disk_paths, ecstore, heal_storage) = setup_test_env().await;
    
    // Create test bucket
    let bucket_name = "test-bucket-heal";
    create_test_bucket(&ecstore, bucket_name).await;
    
    // Create heal manager
    let heal_manager = HealManager::new(heal_storage.clone(), Default::default());
    
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
            scan_mode: HEAL_NORMAL_SCAN,
            update_parity: false,
            timeout: Some(Duration::from_secs(300)),
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
    
    // Check task status
    let task_status = heal_manager.get_task_status(&task_id).await;
    assert!(task_status.is_ok());
    
    let status = task_status.unwrap();
    info!("Bucket heal task status: {:?}", status);
    
    // Verify bucket still exists
    let bucket_exists = heal_storage.get_bucket_info(bucket_name).await;
    assert!(bucket_exists.is_ok());
    assert!(bucket_exists.unwrap().is_some());
    
    // Cleanup
    cleanup_test_env(&disk_paths).await;
    
    info!("Heal bucket basic test passed");
}

#[tokio::test]
async fn test_heal_format_basic() {
    let (disk_paths, _ecstore, heal_storage) = setup_test_env().await;
    
    // Create heal manager
    let heal_manager = HealManager::new(heal_storage.clone(), Default::default());
    
    // Get disk endpoint for testing
    let disk_endpoint = Endpoint::try_from(disk_paths[0].to_str().unwrap())
        .expect("Failed to create disk endpoint");
    
    // Submit disk heal request (format heal)
    let heal_request = HealRequest::new(
        HealType::Disk { endpoint: disk_endpoint },
        HealOptions {
            dry_run: true, // Use dry run for format heal test
            recursive: false,
            remove_corrupted: false,
            recreate_missing: false,
            scan_mode: HEAL_NORMAL_SCAN,
            update_parity: false,
            timeout: Some(Duration::from_secs(300)),
        },
        HealPriority::Normal,
    );
    
    let task_id = heal_manager
        .submit_heal_request(heal_request)
        .await
        .expect("Failed to submit disk heal request");
    
    info!("Submitted disk heal request with task ID: {}", task_id);
    
    // Wait for task completion
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
    
    // Check task status
    let task_status = heal_manager.get_task_status(&task_id).await;
    assert!(task_status.is_ok());
    
    let status = task_status.unwrap();
    info!("Disk heal task status: {:?}", status);
    
    // Cleanup
    cleanup_test_env(&disk_paths).await;
    
    info!("Heal format basic test passed");
}

#[tokio::test]
async fn test_heal_storage_api_direct() {
    let (disk_paths, ecstore, heal_storage) = setup_test_env().await;
    
    // Test direct heal storage API calls
    
    // Test heal_format
    let format_result = heal_storage.heal_format(true).await; // dry run
    assert!(format_result.is_ok());
    info!("Direct heal_format test passed");
    
    // Test heal_bucket
    let bucket_name = "test-bucket-direct";
    create_test_bucket(&ecstore, bucket_name).await;
    
    let heal_opts = rustfs_ecstore::heal::heal_commands::HealOpts {
        recursive: true,
        dry_run: true,
        remove: false,
        recreate: false,
        scan_mode: rustfs_ecstore::heal::heal_commands::HEAL_NORMAL_SCAN,
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
    
    let object_heal_opts = rustfs_ecstore::heal::heal_commands::HealOpts {
        recursive: false,
        dry_run: true,
        remove: false,
        recreate: false,
        scan_mode: rustfs_ecstore::heal::heal_commands::HEAL_NORMAL_SCAN,
        update_parity: false,
        no_lock: false,
        pool: None,
        set: None,
    };
    
    let object_result = heal_storage.heal_object(bucket_name, object_name, None, &object_heal_opts).await;
    assert!(object_result.is_ok());
    info!("Direct heal_object test passed");
    
    // Cleanup
    cleanup_test_env(&disk_paths).await;
    
    info!("Direct heal storage API test passed");
} 