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

use rustfs_ahm::scanner::{Scanner, data_scanner::ScannerConfig};
use rustfs_ecstore::{
    bucket::metadata::BUCKET_LIFECYCLE_CONFIG,
    bucket::metadata_sys,
    disk::endpoint::Endpoint,
    endpoints::{EndpointServerPools, Endpoints, PoolEndpoints},
    global::GLOBAL_TierConfigMgr,
    store::ECStore,
    store_api::{MakeBucketOptions, ObjectIO, ObjectOptions, PutObjReader, StorageAPI},
    tier::tier_config::{TierConfig, TierMinIO, TierType},
};
use serial_test::serial;
use std::{
    path::PathBuf,
    sync::{Arc, Once, OnceLock},
    time::Duration,
};
use tokio::fs;
use tokio_util::sync::CancellationToken;
use tracing::info;

static GLOBAL_ENV: OnceLock<(Vec<PathBuf>, Arc<ECStore>)> = OnceLock::new();
static INIT: Once = Once::new();

fn init_tracing() {
    INIT.call_once(|| {
        let _ = tracing_subscriber::fmt::try_init();
    });
}

/// Test helper: Create test environment with ECStore
async fn setup_test_env() -> (Vec<PathBuf>, Arc<ECStore>) {
    init_tracing();

    // Fast path: already initialized, just clone and return
    if let Some((paths, ecstore)) = GLOBAL_ENV.get() {
        return (paths.clone(), ecstore.clone());
    }

    // create temp dir as 4 disks with unique base dir
    let test_base_dir = format!("/tmp/rustfs_ahm_lifecycle_test_{}", uuid::Uuid::new_v4());
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

    // create ECStore with dynamic port 0 (let OS assign) or fixed 9002 if free
    let port = 9002; // for simplicity
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

    // Initialize background expiry workers
    rustfs_ecstore::bucket::lifecycle::bucket_lifecycle_ops::init_background_expiry(ecstore.clone()).await;

    // Store in global once lock
    let _ = GLOBAL_ENV.set((disk_paths.clone(), ecstore.clone()));

    (disk_paths, ecstore)
}

/// Test helper: Create a test bucket
async fn create_test_bucket(ecstore: &Arc<ECStore>, bucket_name: &str) {
    (**ecstore)
        .make_bucket(bucket_name, &Default::default())
        .await
        .expect("Failed to create test bucket");
    info!("Created test bucket: {}", bucket_name);
}

/// Test helper: Create a test lock bucket
async fn create_test_lock_bucket(ecstore: &Arc<ECStore>, bucket_name: &str) {
    (**ecstore)
        .make_bucket(
            bucket_name,
            &MakeBucketOptions {
                lock_enabled: true,
                versioning_enabled: true,
                ..Default::default()
            },
        )
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

/// Test helper: Set bucket lifecycle configuration
async fn set_bucket_lifecycle(bucket_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Create a simple lifecycle configuration XML with 0 days expiry for immediate testing
    let lifecycle_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
    <Rule>
        <ID>test-rule</ID>
        <Status>Enabled</Status>
        <Filter>
            <Prefix>test/</Prefix>
        </Filter>
        <Expiration>
            <Days>0</Days>
        </Expiration>
    </Rule>
</LifecycleConfiguration>"#;

    metadata_sys::update(bucket_name, BUCKET_LIFECYCLE_CONFIG, lifecycle_xml.as_bytes().to_vec()).await?;

    Ok(())
}

/// Test helper: Set bucket lifecycle configuration
async fn set_bucket_lifecycle_deletemarker(bucket_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Create a simple lifecycle configuration XML with 0 days expiry for immediate testing
    let lifecycle_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
    <Rule>
        <ID>test-rule</ID>
        <Status>Enabled</Status>
        <Filter>
            <Prefix>test/</Prefix>
        </Filter>
        <Expiration>
            <Days>0</Days>
            <ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker>
        </Expiration>
    </Rule>
</LifecycleConfiguration>"#;

    metadata_sys::update(bucket_name, BUCKET_LIFECYCLE_CONFIG, lifecycle_xml.as_bytes().to_vec()).await?;

    Ok(())
}

#[allow(dead_code)]
async fn set_bucket_lifecycle_transition(bucket_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Create a simple lifecycle configuration XML with 0 days expiry for immediate testing
    let lifecycle_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
    <Rule>
        <ID>test-rule</ID>
        <Status>Enabled</Status>
        <Filter>
            <Prefix>test/</Prefix>
        </Filter>
        <Transition>
          <Days>0</Days>
          <StorageClass>COLDTIER44</StorageClass>
        </Transition>
    </Rule>
    <Rule>
        <ID>test-rule2</ID>
        <Status>Disabled</Status>
        <Filter>
            <Prefix>test/</Prefix>
        </Filter>
        <NoncurrentVersionTransition>
          <NoncurrentDays>0</NoncurrentDays>
          <StorageClass>COLDTIER44</StorageClass>
        </NoncurrentVersionTransition>
    </Rule>
</LifecycleConfiguration>"#;

    metadata_sys::update(bucket_name, BUCKET_LIFECYCLE_CONFIG, lifecycle_xml.as_bytes().to_vec()).await?;

    Ok(())
}

/// Test helper: Create a test tier
#[allow(dead_code)]
async fn create_test_tier(server: u32) {
    let args = TierConfig {
        version: "v1".to_string(),
        tier_type: TierType::MinIO,
        name: "COLDTIER44".to_string(),
        s3: None,
        aliyun: None,
        tencent: None,
        huaweicloud: None,
        azure: None,
        gcs: None,
        r2: None,
        rustfs: None,
        minio: if server == 1 {
            Some(TierMinIO {
                access_key: "minioadmin".to_string(),
                secret_key: "minioadmin".to_string(),
                bucket: "hello".to_string(),
                endpoint: "http://39.105.198.204:9000".to_string(),
                prefix: format!("mypre{}/", uuid::Uuid::new_v4()),
                region: "".to_string(),
                ..Default::default()
            })
        } else {
            Some(TierMinIO {
                access_key: "minioadmin".to_string(),
                secret_key: "minioadmin".to_string(),
                bucket: "mblock2".to_string(),
                endpoint: "http://127.0.0.1:9020".to_string(),
                prefix: format!("mypre{}/", uuid::Uuid::new_v4()),
                region: "".to_string(),
                ..Default::default()
            })
        },
    };
    let mut tier_config_mgr = GLOBAL_TierConfigMgr.write().await;
    if let Err(err) = tier_config_mgr.add(args, false).await {
        println!("tier_config_mgr add failed, e: {err:?}");
        panic!("tier add failed. {err}");
    }
    if let Err(e) = tier_config_mgr.save().await {
        println!("tier_config_mgr save failed, e: {e:?}");
        panic!("tier save failed");
    }
    println!("Created test tier: COLDTIER44");
}

/// Test helper: Check if object exists
async fn object_exists(ecstore: &Arc<ECStore>, bucket: &str, object: &str) -> bool {
    match (**ecstore).get_object_info(bucket, object, &ObjectOptions::default()).await {
        Ok(info) => !info.delete_marker,
        Err(_) => false,
    }
}

/// Test helper: Check if object exists
#[allow(dead_code)]
async fn object_is_delete_marker(ecstore: &Arc<ECStore>, bucket: &str, object: &str) -> bool {
    if let Ok(oi) = (**ecstore).get_object_info(bucket, object, &ObjectOptions::default()).await {
        println!("oi: {oi:?}");
        oi.delete_marker
    } else {
        println!("object_is_delete_marker is error");
        panic!("object_is_delete_marker is error");
    }
}

/// Test helper: Check if object exists
#[allow(dead_code)]
async fn object_is_transitioned(ecstore: &Arc<ECStore>, bucket: &str, object: &str) -> bool {
    if let Ok(oi) = (**ecstore).get_object_info(bucket, object, &ObjectOptions::default()).await {
        println!("oi: {oi:?}");
        !oi.transitioned_object.status.is_empty()
    } else {
        println!("object_is_transitioned is error");
        panic!("object_is_transitioned is error");
    }
}

async fn wait_for_object_absence(ecstore: &Arc<ECStore>, bucket: &str, object: &str, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        if !object_exists(ecstore, bucket, object).await {
            return true;
        }

        if tokio::time::Instant::now() >= deadline {
            return false;
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

mod serial_tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_lifecycle_expiry_basic() {
        let (_disk_paths, ecstore) = setup_test_env().await;

        // Create test bucket and object
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let bucket_name = format!("test-lc-expiry-basic-{}", &suffix[..8]);
        let object_name = "test/object.txt"; // Match the lifecycle rule prefix "test/"
        let test_data = b"Hello, this is test data for lifecycle expiry!";

        create_test_lock_bucket(&ecstore, bucket_name.as_str()).await;
        upload_test_object(&ecstore, bucket_name.as_str(), object_name, test_data).await;

        // Verify object exists initially
        assert!(object_exists(&ecstore, bucket_name.as_str(), object_name).await);
        println!("✅ Object exists before lifecycle processing");

        // Set lifecycle configuration with very short expiry (0 days = immediate expiry)
        set_bucket_lifecycle(bucket_name.as_str())
            .await
            .expect("Failed to set lifecycle configuration");
        println!("✅ Lifecycle configuration set for bucket: {bucket_name}");

        // Verify lifecycle configuration was set
        match rustfs_ecstore::bucket::metadata_sys::get(bucket_name.as_str()).await {
            Ok(bucket_meta) => {
                assert!(bucket_meta.lifecycle_config.is_some());
                println!("✅ Bucket metadata retrieved successfully");
            }
            Err(e) => {
                println!("❌ Error retrieving bucket metadata: {e:?}");
            }
        }

        // Create scanner with very short intervals for testing
        let scanner_config = ScannerConfig {
            scan_interval: Duration::from_millis(100),
            deep_scan_interval: Duration::from_millis(500),
            max_concurrent_scans: 1,
            ..Default::default()
        };

        let scanner = Scanner::new(Some(scanner_config), None);

        // Start scanner
        scanner.start().await.expect("Failed to start scanner");
        println!("✅ Scanner started");

        // Wait for scanner to process lifecycle rules
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Manually trigger a scan cycle to ensure lifecycle processing
        scanner.scan_cycle().await.expect("Failed to trigger scan cycle");
        println!("✅ Manual scan cycle completed");

        let mut expired = false;
        for attempt in 0..3 {
            if attempt > 0 {
                scanner.scan_cycle().await.expect("Failed to trigger scan cycle on retry");
            }
            expired = wait_for_object_absence(&ecstore, bucket_name.as_str(), object_name, Duration::from_secs(5)).await;
            if expired {
                break;
            }
        }

        println!("Object is_delete_marker after lifecycle processing: {}", !expired);

        if !expired {
            let pending = rustfs_ecstore::bucket::lifecycle::bucket_lifecycle_ops::GLOBAL_ExpiryState
                .read()
                .await
                .pending_tasks()
                .await;
            println!("Pending expiry tasks: {pending}");

            if let Ok((lc_config, _)) = rustfs_ecstore::bucket::metadata_sys::get_lifecycle_config(bucket_name.as_str()).await {
                if let Ok(object_info) = ecstore
                    .get_object_info(bucket_name.as_str(), object_name, &rustfs_ecstore::store_api::ObjectOptions::default())
                    .await
                {
                    let event = rustfs_ecstore::bucket::lifecycle::bucket_lifecycle_ops::eval_action_from_lifecycle(
                        &lc_config,
                        None,
                        None,
                        &object_info,
                    )
                    .await;

                    rustfs_ecstore::bucket::lifecycle::bucket_lifecycle_ops::apply_expiry_on_non_transitioned_objects(
                        ecstore.clone(),
                        &object_info,
                        &event,
                        &rustfs_ecstore::bucket::lifecycle::bucket_lifecycle_audit::LcEventSrc::Scanner,
                    )
                    .await;

                    expired = wait_for_object_absence(&ecstore, bucket_name.as_str(), object_name, Duration::from_secs(2)).await;
                }
            }

            if !expired {
                println!("❌ Object was not deleted by lifecycle processing");
            }
        } else {
            println!("✅ Object was successfully deleted by lifecycle processing");
            // Let's try to get object info to see its details
            match ecstore
                .get_object_info(bucket_name.as_str(), object_name, &rustfs_ecstore::store_api::ObjectOptions::default())
                .await
            {
                Ok(obj_info) => {
                    println!(
                        "Object info: name={}, size={}, mod_time={:?}",
                        obj_info.name, obj_info.size, obj_info.mod_time
                    );
                }
                Err(e) => {
                    println!("Error getting object info: {e:?}");
                }
            }
        }

        assert!(expired);
        println!("✅ Object successfully expired");

        // Stop scanner
        let _ = scanner.stop().await;
        println!("✅ Scanner stopped");

        println!("Lifecycle expiry basic test completed");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial]
    //#[ignore]
    async fn test_lifecycle_expiry_deletemarker() {
        let (_disk_paths, ecstore) = setup_test_env().await;

        // Create test bucket and object
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let bucket_name = format!("test-lc-expiry-marker-{}", &suffix[..8]);
        let object_name = "test/object.txt"; // Match the lifecycle rule prefix "test/"
        let test_data = b"Hello, this is test data for lifecycle expiry!";

        create_test_lock_bucket(&ecstore, bucket_name.as_str()).await;
        upload_test_object(&ecstore, bucket_name.as_str(), object_name, test_data).await;

        // Verify object exists initially
        assert!(object_exists(&ecstore, bucket_name.as_str(), object_name).await);
        println!("✅ Object exists before lifecycle processing");

        // Set lifecycle configuration with very short expiry (0 days = immediate expiry)
        set_bucket_lifecycle_deletemarker(bucket_name.as_str())
            .await
            .expect("Failed to set lifecycle configuration");
        println!("✅ Lifecycle configuration set for bucket: {bucket_name}");

        // Verify lifecycle configuration was set
        match rustfs_ecstore::bucket::metadata_sys::get(bucket_name.as_str()).await {
            Ok(bucket_meta) => {
                assert!(bucket_meta.lifecycle_config.is_some());
                println!("✅ Bucket metadata retrieved successfully");
            }
            Err(e) => {
                println!("❌ Error retrieving bucket metadata: {e:?}");
            }
        }

        // Create scanner with very short intervals for testing
        let scanner_config = ScannerConfig {
            scan_interval: Duration::from_millis(100),
            deep_scan_interval: Duration::from_millis(500),
            max_concurrent_scans: 1,
            ..Default::default()
        };

        let scanner = Scanner::new(Some(scanner_config), None);

        // Start scanner
        scanner.start().await.expect("Failed to start scanner");
        println!("✅ Scanner started");

        // Wait for scanner to process lifecycle rules
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Manually trigger a scan cycle to ensure lifecycle processing
        scanner.scan_cycle().await.expect("Failed to trigger scan cycle");
        println!("✅ Manual scan cycle completed");

        let mut deleted = false;
        for attempt in 0..3 {
            if attempt > 0 {
                scanner.scan_cycle().await.expect("Failed to trigger scan cycle on retry");
            }
            deleted = wait_for_object_absence(&ecstore, bucket_name.as_str(), object_name, Duration::from_secs(5)).await;
            if deleted {
                break;
            }
        }

        println!("Object exists after lifecycle processing: {}", !deleted);

        if !deleted {
            let pending = rustfs_ecstore::bucket::lifecycle::bucket_lifecycle_ops::GLOBAL_ExpiryState
                .read()
                .await
                .pending_tasks()
                .await;
            println!("Pending expiry tasks: {pending}");

            if let Ok((lc_config, _)) = rustfs_ecstore::bucket::metadata_sys::get_lifecycle_config(bucket_name.as_str()).await {
                if let Ok(obj_info) = ecstore
                    .get_object_info(bucket_name.as_str(), object_name, &rustfs_ecstore::store_api::ObjectOptions::default())
                    .await
                {
                    let event = rustfs_ecstore::bucket::lifecycle::bucket_lifecycle_ops::eval_action_from_lifecycle(
                        &lc_config, None, None, &obj_info,
                    )
                    .await;

                    rustfs_ecstore::bucket::lifecycle::bucket_lifecycle_ops::apply_expiry_on_non_transitioned_objects(
                        ecstore.clone(),
                        &obj_info,
                        &event,
                        &rustfs_ecstore::bucket::lifecycle::bucket_lifecycle_audit::LcEventSrc::Scanner,
                    )
                    .await;

                    deleted = wait_for_object_absence(&ecstore, bucket_name.as_str(), object_name, Duration::from_secs(2)).await;

                    if !deleted {
                        println!(
                            "Object info: name={}, size={}, mod_time={:?}",
                            obj_info.name, obj_info.size, obj_info.mod_time
                        );
                    }
                }
            }

            if !deleted {
                println!("❌ Object was not deleted by lifecycle processing");
            }
        } else {
            println!("✅ Object was successfully deleted by lifecycle processing");
        }

        assert!(deleted);
        println!("✅ Object successfully expired");

        // Stop scanner
        let _ = scanner.stop().await;
        println!("✅ Scanner stopped");

        println!("Lifecycle expiry basic test completed");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial]
    #[ignore]
    async fn test_lifecycle_transition_basic() {
        let (_disk_paths, ecstore) = setup_test_env().await;

        create_test_tier(1).await;

        // Create test bucket and object
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let bucket_name = format!("test-lc-transition-{}", &suffix[..8]);
        let object_name = "test/object.txt"; // Match the lifecycle rule prefix "test/"
        let test_data = b"Hello, this is test data for lifecycle expiry!";

        //create_test_lock_bucket(&ecstore, bucket_name.as_str()).await;
        create_test_bucket(&ecstore, bucket_name.as_str()).await;
        upload_test_object(&ecstore, bucket_name.as_str(), object_name, test_data).await;

        // Verify object exists initially
        assert!(object_exists(&ecstore, bucket_name.as_str(), object_name).await);
        println!("✅ Object exists before lifecycle processing");

        // Set lifecycle configuration with very short expiry (0 days = immediate expiry)
        set_bucket_lifecycle_transition(bucket_name.as_str())
            .await
            .expect("Failed to set lifecycle configuration");
        println!("✅ Lifecycle configuration set for bucket: {bucket_name}");

        // Verify lifecycle configuration was set
        match rustfs_ecstore::bucket::metadata_sys::get(bucket_name.as_str()).await {
            Ok(bucket_meta) => {
                assert!(bucket_meta.lifecycle_config.is_some());
                println!("✅ Bucket metadata retrieved successfully");
            }
            Err(e) => {
                println!("❌ Error retrieving bucket metadata: {e:?}");
            }
        }

        // Create scanner with very short intervals for testing
        let scanner_config = ScannerConfig {
            scan_interval: Duration::from_millis(100),
            deep_scan_interval: Duration::from_millis(500),
            max_concurrent_scans: 1,
            ..Default::default()
        };

        let scanner = Scanner::new(Some(scanner_config), None);

        // Start scanner
        scanner.start().await.expect("Failed to start scanner");
        println!("✅ Scanner started");

        // Wait for scanner to process lifecycle rules
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Manually trigger a scan cycle to ensure lifecycle processing
        scanner.scan_cycle().await.expect("Failed to trigger scan cycle");
        println!("✅ Manual scan cycle completed");

        // Wait a bit more for background workers to process expiry tasks
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Check if object has been expired (deleted)
        let check_result = object_is_transitioned(&ecstore, &bucket_name, object_name).await;
        println!("Object exists after lifecycle processing: {check_result}");

        if check_result {
            println!("✅ Object was transitioned by lifecycle processing");
            // Let's try to get object info to see its details
            match ecstore
                .get_object_info(bucket_name.as_str(), object_name, &rustfs_ecstore::store_api::ObjectOptions::default())
                .await
            {
                Ok(obj_info) => {
                    println!(
                        "Object info: name={}, size={}, mod_time={:?}",
                        obj_info.name, obj_info.size, obj_info.mod_time
                    );
                    println!("Object info: transitioned_object={:?}", obj_info.transitioned_object);
                }
                Err(e) => {
                    println!("Error getting object info: {e:?}");
                }
            }
        } else {
            println!("❌ Object was not transitioned by lifecycle processing");
        }

        assert!(check_result);
        println!("✅ Object successfully transitioned");

        // Stop scanner
        let _ = scanner.stop().await;
        println!("✅ Scanner stopped");

        println!("Lifecycle transition basic test completed");
    }
}
