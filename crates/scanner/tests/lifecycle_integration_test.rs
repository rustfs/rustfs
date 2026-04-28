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

use rustfs_ecstore::{
    bucket::lifecycle::lifecycle::TransitionOptions,
    bucket::metadata::BUCKET_LIFECYCLE_CONFIG,
    bucket::{lifecycle::bucket_lifecycle_ops::enqueue_transition_for_existing_objects, metadata_sys},
    client::transition_api::{ReadCloser, ReaderImpl},
    disk::endpoint::Endpoint,
    disk::{DiskAPI, DiskOption, STORAGE_FORMAT_FILE, new_disk},
    endpoints::{EndpointServerPools, Endpoints, PoolEndpoints},
    global::GLOBAL_TierConfigMgr,
    pools::path2_bucket_object_with_base_path,
    store::ECStore,
    store_api::{
        BucketOperations, ListOperations, MakeBucketOptions, MultipartOperations, ObjectIO, ObjectOperations, ObjectOptions,
        PutObjReader,
    },
    tier::{
        tier_config::{TierConfig, TierMinIO, TierType},
        warm_backend::{WarmBackend, WarmBackendGetOpts, build_transition_put_options},
    },
};
use rustfs_filemeta::FileMeta;
use rustfs_scanner::scanner::init_data_scanner;
use rustfs_scanner::scanner_folder::ScannerItem;
use rustfs_scanner::scanner_io::ScannerIODisk;
use rustfs_utils::path::path_join_buf;
use s3s::dto::RestoreRequest;
use serial_test::serial;
use std::{
    collections::HashMap,
    io::Cursor,
    path::{Path, PathBuf},
    sync::{Arc, Once, OnceLock},
    time::Duration,
};
use tokio::fs;
use tokio::io::AsyncReadExt;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::info;
use uuid::Uuid;

static GLOBAL_ENV: OnceLock<(Vec<PathBuf>, Arc<ECStore>)> = OnceLock::new();
static INIT: Once = Once::new();
const TRANSITION_WAIT_TIMEOUT: Duration = Duration::from_secs(15);

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
    let test_base_dir = format!("/tmp/rustfs_scanner_lifecycle_test_{}", uuid::Uuid::new_v4());
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

async fn setup_isolated_test_env(init_expiry: bool) -> (Vec<PathBuf>, Arc<ECStore>) {
    init_tracing();

    let test_base_dir = format!("/tmp/rustfs_scanner_lifecycle_test_{}", uuid::Uuid::new_v4());
    let temp_dir = std::path::PathBuf::from(&test_base_dir);
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

    let endpoint_pools = EndpointServerPools(vec![pool_endpoints]);
    rustfs_ecstore::store::init_local_disks(endpoint_pools.clone()).await.unwrap();

    let server_addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let ecstore = ECStore::new(server_addr, endpoint_pools, CancellationToken::new())
        .await
        .unwrap();

    let buckets_list = ecstore
        .list_bucket(&rustfs_ecstore::store_api::BucketOptions {
            no_metadata: true,
            ..Default::default()
        })
        .await
        .unwrap();
    let buckets = buckets_list.into_iter().map(|v| v.name).collect();
    rustfs_ecstore::bucket::metadata_sys::init_bucket_metadata_sys(ecstore.clone(), buckets).await;

    if init_expiry {
        rustfs_ecstore::bucket::lifecycle::bucket_lifecycle_ops::init_background_expiry(ecstore.clone()).await;
    }

    (disk_paths, ecstore)
}

/// Test helper: Create a test bucket
#[allow(dead_code)]
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
#[allow(dead_code)]
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
#[allow(dead_code)]
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
    set_bucket_lifecycle_transition_with_tier(bucket_name, "COLDTIER44").await
}

#[allow(dead_code)]
async fn set_bucket_lifecycle_transition_with_tier(
    bucket_name: &str,
    storage_class: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Create a simple lifecycle configuration XML with 0 days expiry for immediate testing
    let lifecycle_xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
    <Rule>
        <ID>test-rule</ID>
        <Status>Enabled</Status>
        <Filter>
            <Prefix>test/</Prefix>
        </Filter>
        <Transition>
          <Days>0</Days>
          <StorageClass>{storage_class}</StorageClass>
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
          <StorageClass>{storage_class}</StorageClass>
        </NoncurrentVersionTransition>
    </Rule>
</LifecycleConfiguration>"#
    );

    metadata_sys::update(bucket_name, BUCKET_LIFECYCLE_CONFIG, lifecycle_xml.into_bytes()).await?;

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
                endpoint: "http://127.0.0.1:9000".to_string(),
                prefix: format!("mypre{}/", uuid::Uuid::new_v4()),
                region: "".to_string(),
                ..Default::default()
            })
        } else if server == 2 {
            let test_compatible_server = std::env::var("TEST_MINIO_SERVER").unwrap_or_else(|_| "localhost:9000".to_string());
            Some(TierMinIO {
                access_key: "minioadmin".to_string(),
                secret_key: "minioadmin".to_string(),
                bucket: "mblock2".to_string(),
                endpoint: format!("http://{}", test_compatible_server),
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

#[allow(dead_code)]
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

async fn wait_for_remote_absence(backend: &MockWarmBackend, object: &str, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        if !backend.objects.lock().await.contains_key(object) {
            return true;
        }

        if tokio::time::Instant::now() >= deadline {
            return false;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn free_version_count(disk_path: &Path, bucket: &str, object: &str) -> usize {
    let mut endpoint = Endpoint::try_from(disk_path.to_str().unwrap()).unwrap();
    endpoint.set_pool_index(0);
    endpoint.set_set_index(0);
    endpoint.set_disk_index(0);
    let disk = new_disk(
        &endpoint,
        &DiskOption {
            cleanup: false,
            health_check: false,
        },
    )
    .await
    .expect("failed to open local disk");
    let data = disk
        .read_metadata(bucket, &path_join_buf(&[object, STORAGE_FORMAT_FILE]))
        .await;
    let Ok(data) = data else {
        return 0;
    };
    let meta = FileMeta::load(&data).expect("failed to load file metadata");
    meta.get_file_info_versions(bucket, object, false)
        .expect("failed to decode file info versions")
        .free_versions
        .len()
}

async fn object_version_count(ecstore: &Arc<ECStore>, bucket: &str, object: &str) -> usize {
    let mut marker = None;
    let mut version_marker = None;
    let mut count = 0;

    loop {
        let Ok(page) = ecstore
            .clone()
            .list_object_versions(bucket, object, marker.clone(), version_marker.clone(), None, 1000)
            .await
        else {
            return 0;
        };

        count += page.objects.iter().filter(|version| version.name == object).count();

        if !page.is_truncated {
            return count;
        }

        marker = page.next_marker;
        version_marker = page.next_version_idmarker;
    }
}

async fn wait_for_version_count(ecstore: &Arc<ECStore>, bucket: &str, object: &str, expected: usize, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        if object_version_count(ecstore, bucket, object).await == expected {
            return true;
        }

        if tokio::time::Instant::now() >= deadline {
            return false;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn scan_object_with_lifecycle(disk_path: &Path, bucket: &str, object: &str) {
    let mut endpoint = Endpoint::try_from(disk_path.to_str().unwrap()).unwrap();
    endpoint.set_pool_index(0);
    endpoint.set_set_index(0);
    endpoint.set_disk_index(0);
    let disk = new_disk(
        &endpoint,
        &DiskOption {
            cleanup: false,
            health_check: false,
        },
    )
    .await
    .expect("failed to open local disk");
    let metadata_path = disk_path.join(bucket).join(object).join(STORAGE_FORMAT_FILE);
    let relative_path = metadata_path.to_string_lossy().to_string();
    let (_, scanner_path) = path2_bucket_object_with_base_path(disk_path.to_string_lossy().as_ref(), relative_path.as_str());
    let file_type = fs::metadata(&metadata_path)
        .await
        .expect("failed to stat object metadata")
        .file_type();
    let lifecycle = metadata_sys::get(bucket)
        .await
        .expect("failed to load bucket metadata")
        .lifecycle_config
        .clone()
        .map(Arc::new);
    let item = ScannerItem {
        path: scanner_path.clone(),
        bucket: bucket.to_string(),
        prefix: object.to_string(),
        object_name: STORAGE_FORMAT_FILE.to_string(),
        file_type,
        lifecycle,
        replication: None,
        heal_enabled: false,
        heal_bitrot: false,
        debug: false,
    };
    disk.get_size(item).await.expect("scanner get_size should succeed");
}

async fn scan_object_metadata(disk_path: &Path, bucket: &str, object: &str) {
    let mut endpoint = Endpoint::try_from(disk_path.to_str().unwrap()).unwrap();
    endpoint.set_pool_index(0);
    endpoint.set_set_index(0);
    endpoint.set_disk_index(0);
    let disk = new_disk(
        &endpoint,
        &DiskOption {
            cleanup: false,
            health_check: false,
        },
    )
    .await
    .expect("failed to open local disk");
    let metadata_path = disk_path.join(bucket).join(object).join(STORAGE_FORMAT_FILE);
    let relative_path = metadata_path.to_string_lossy().to_string();
    let (_, scanner_path) = path2_bucket_object_with_base_path(disk_path.to_string_lossy().as_ref(), relative_path.as_str());
    let file_type = fs::metadata(&metadata_path)
        .await
        .expect("failed to stat object metadata")
        .file_type();
    let item = ScannerItem {
        path: scanner_path.clone(),
        bucket: bucket.to_string(),
        prefix: object.to_string(),
        object_name: STORAGE_FORMAT_FILE.to_string(),
        file_type,
        lifecycle: None,
        replication: None,
        heal_enabled: false,
        heal_bitrot: false,
        debug: false,
    };
    disk.get_size(item).await.expect("scanner get_size should succeed");
}

#[derive(Clone, Default)]
struct MockStoredObject {
    bytes: Vec<u8>,
    metadata: HashMap<String, String>,
}

#[derive(Clone, Default)]
struct MockWarmBackend {
    objects: Arc<Mutex<HashMap<String, MockStoredObject>>>,
}

impl MockWarmBackend {
    async fn put_bytes(&self, object: &str, bytes: Vec<u8>, metadata: HashMap<String, String>) -> String {
        self.objects
            .lock()
            .await
            .insert(object.to_string(), MockStoredObject { bytes, metadata });
        Uuid::new_v4().to_string()
    }

    async fn read_bytes(&self, reader: ReaderImpl) -> Result<Vec<u8>, std::io::Error> {
        match reader {
            ReaderImpl::Body(bytes) => Ok(bytes.to_vec()),
            ReaderImpl::ObjectBody(mut reader) => {
                let mut buf = Vec::new();
                reader.stream.read_to_end(&mut buf).await?;
                Ok(buf)
            }
        }
    }
}

#[async_trait::async_trait]
impl WarmBackend for MockWarmBackend {
    async fn put(&self, object: &str, r: ReaderImpl, _length: i64) -> Result<String, std::io::Error> {
        let bytes = self.read_bytes(r).await?;
        Ok(self.put_bytes(object, bytes, HashMap::new()).await)
    }

    async fn put_with_meta(
        &self,
        object: &str,
        r: ReaderImpl,
        _length: i64,
        meta: HashMap<String, String>,
    ) -> Result<String, std::io::Error> {
        let bytes = self.read_bytes(r).await?;
        let opts = build_transition_put_options(String::new(), meta);
        let mut metadata = opts.user_metadata.clone();
        if !opts.content_type.is_empty() {
            metadata.insert("content-type".to_string(), opts.content_type.clone());
        }
        if !opts.content_encoding.is_empty() {
            metadata.insert("content-encoding".to_string(), opts.content_encoding.clone());
        }
        if !opts.cache_control.is_empty() {
            metadata.insert("cache-control".to_string(), opts.cache_control.clone());
        }
        if !opts.internal.replication_status.as_str().is_empty() {
            metadata.insert(
                "x-amz-replication-status".to_string(),
                opts.internal.replication_status.as_str().to_string(),
            );
        }
        if !opts.legalhold.as_str().is_empty() {
            metadata.insert("x-amz-object-lock-legal-hold".to_string(), opts.legalhold.as_str().to_string());
        }
        Ok(self.put_bytes(object, bytes, metadata).await)
    }

    async fn get(&self, object: &str, _rv: &str, opts: WarmBackendGetOpts) -> Result<ReadCloser, std::io::Error> {
        let objects = self.objects.lock().await;
        let Some(stored) = objects.get(object) else {
            return Err(std::io::Error::new(std::io::ErrorKind::NotFound, "mock object not found"));
        };
        let bytes = &stored.bytes;

        let start = opts.start_offset.max(0) as usize;
        let end = if opts.length > 0 {
            start.saturating_add(opts.length as usize).min(bytes.len())
        } else {
            bytes.len()
        };

        Ok(tokio::io::BufReader::new(Cursor::new(bytes[start.min(bytes.len())..end].to_vec())))
    }

    async fn remove(&self, object: &str, _rv: &str) -> Result<(), std::io::Error> {
        self.objects.lock().await.remove(object);
        Ok(())
    }

    async fn in_use(&self) -> Result<bool, std::io::Error> {
        Ok(false)
    }
}

async fn register_mock_tier(tier_name: &str) -> MockWarmBackend {
    let backend = MockWarmBackend::default();
    let mut tier_config_mgr = GLOBAL_TierConfigMgr.write().await;
    tier_config_mgr.tiers.insert(
        tier_name.to_string(),
        TierConfig {
            version: "v1".to_string(),
            tier_type: TierType::MinIO,
            name: tier_name.to_string(),
            minio: Some(TierMinIO {
                access_key: "minioadmin".to_string(),
                secret_key: "minioadmin".to_string(),
                bucket: "mock-tier".to_string(),
                endpoint: "http://127.0.0.1:0".to_string(),
                prefix: format!("mock/{}/", Uuid::new_v4()),
                region: String::new(),
                ..Default::default()
            }),
            ..Default::default()
        },
    );
    tier_config_mgr
        .driver_cache
        .insert(tier_name.to_string(), Box::new(backend.clone()));
    backend
}

async fn wait_for_transition(
    ecstore: &Arc<ECStore>,
    bucket: &str,
    object: &str,
    timeout: Duration,
) -> Option<rustfs_ecstore::store_api::ObjectInfo> {
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        if let Ok(info) = (**ecstore).get_object_info(bucket, object, &ObjectOptions::default()).await
            && info.transitioned_object.status == "complete"
        {
            return Some(info);
        }

        if tokio::time::Instant::now() >= deadline {
            return None;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

mod serial_tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial]
    #[ignore]
    async fn test_lifecycle_transition_basic() {
        let (_disk_paths, ecstore) = setup_test_env().await;

        create_test_tier(2).await;

        // Create test bucket and object
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let bucket_name = format!("test-lc-transition-{}", &suffix[..8]);
        let object_name = "test/object.txt"; // Match the lifecycle rule prefix "test/"
        let test_data = b"Hello, this is test data for lifecycle expiry!";

        create_test_lock_bucket(&ecstore, bucket_name.as_str()).await;
        upload_test_object(
            &ecstore,
            bucket_name.as_str(),
            object_name,
            b"Hello, this is test data for lifecycle expiry 1111-11111111-1111 !",
        )
        .await;
        //create_test_bucket(&ecstore, bucket_name.as_str()).await;
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

        let ctx = CancellationToken::new();

        // Start scanner
        init_data_scanner(ctx.clone(), ecstore.clone()).await;
        println!("✅ Scanner started");

        // Wait for scanner to process lifecycle rules
        tokio::time::sleep(Duration::from_secs(1200)).await;

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
        ctx.cancel();
        println!("✅ Scanner stopped");

        println!("Lifecycle transition basic test completed");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial]
    #[ignore = "requires isolated global object layer state"]
    async fn test_transition_and_restore_flows() {
        let (_disk_paths, ecstore) = setup_test_env().await;

        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&tier_name).await;

        let put_bucket = format!("test-immediate-put-{}", &Uuid::new_v4().simple().to_string()[..8]);
        let put_object = "test/object.txt";
        let put_payload = b"Hello, immediate transition!";

        create_test_bucket(&ecstore, put_bucket.as_str()).await;
        set_bucket_lifecycle_transition_with_tier(put_bucket.as_str(), &tier_name)
            .await
            .expect("Failed to set lifecycle configuration");

        let mut reader = PutObjReader::from_vec(put_payload.to_vec());
        let mut metadata = HashMap::new();
        metadata.insert("content-type".to_string(), "text/plain".to_string());
        ecstore
            .put_object(
                put_bucket.as_str(),
                put_object,
                &mut reader,
                &ObjectOptions {
                    user_defined: metadata,
                    ..Default::default()
                },
            )
            .await
            .expect("Failed to upload transition metadata test object");

        enqueue_transition_for_existing_objects(ecstore.clone(), put_bucket.as_str())
            .await
            .expect("Failed to enqueue transitioned put object");

        let put_info = wait_for_transition(&ecstore, put_bucket.as_str(), put_object, TRANSITION_WAIT_TIMEOUT)
            .await
            .expect("object should transition after enqueueing existing objects");

        assert_eq!(put_info.transitioned_object.status, "complete");
        assert_eq!(put_info.transitioned_object.tier, tier_name);
        assert!(backend.objects.lock().await.contains_key(&put_info.transitioned_object.name));
        {
            let stored = backend.objects.lock().await;
            let transitioned = stored
                .get(&put_info.transitioned_object.name)
                .expect("transitioned object should be present in mock backend");
            assert_eq!(transitioned.metadata.get("content-type"), Some(&"text/plain".to_string()));
            assert!(
                !transitioned.metadata.contains_key("x-amz-replication-status"),
                "transitioned objects must not inherit replication status defaults"
            );
            assert!(
                !transitioned.metadata.contains_key("x-amz-object-lock-legal-hold"),
                "transitioned objects must not invent object lock headers"
            );
        }

        let multipart_bucket = format!("test-immediate-mpu-{}", &Uuid::new_v4().simple().to_string()[..8]);
        let multipart_object = "test/multipart.txt";

        create_test_bucket(&ecstore, multipart_bucket.as_str()).await;
        set_bucket_lifecycle_transition_with_tier(multipart_bucket.as_str(), &tier_name)
            .await
            .expect("Failed to set lifecycle configuration");

        let upload = ecstore
            .new_multipart_upload(multipart_bucket.as_str(), multipart_object, &ObjectOptions::default())
            .await
            .expect("Failed to create multipart upload");

        let part_data = b"multipart immediate transition";
        let mut reader = PutObjReader::from_vec(part_data.to_vec());
        let part = ecstore
            .put_object_part(
                multipart_bucket.as_str(),
                multipart_object,
                &upload.upload_id,
                1,
                &mut reader,
                &ObjectOptions::default(),
            )
            .await
            .expect("Failed to upload multipart part");

        ecstore
            .clone()
            .complete_multipart_upload(
                multipart_bucket.as_str(),
                multipart_object,
                &upload.upload_id,
                vec![rustfs_ecstore::store_api::CompletePart {
                    part_num: 1,
                    etag: part.etag.clone(),
                    ..Default::default()
                }],
                &ObjectOptions::default(),
            )
            .await
            .expect("Failed to complete multipart upload");

        enqueue_transition_for_existing_objects(ecstore.clone(), multipart_bucket.as_str())
            .await
            .expect("Failed to enqueue transitioned multipart object");

        let multipart_info = wait_for_transition(&ecstore, multipart_bucket.as_str(), multipart_object, TRANSITION_WAIT_TIMEOUT)
            .await
            .expect("object should transition after enqueueing existing objects");

        assert_eq!(multipart_info.transitioned_object.status, "complete");
        assert_eq!(multipart_info.transitioned_object.tier, tier_name);
        assert!(
            backend
                .objects
                .lock()
                .await
                .contains_key(&multipart_info.transitioned_object.name)
        );

        let src_bucket = format!("test-immediate-copy-src-{}", &Uuid::new_v4().simple().to_string()[..8]);
        let dst_bucket = format!("test-immediate-copy-dst-{}", &Uuid::new_v4().simple().to_string()[..8]);
        let src_object = "test/source.txt";
        let dst_object = "test/copied.txt";
        let payload = b"copy object immediate transition";

        create_test_bucket(&ecstore, src_bucket.as_str()).await;
        create_test_bucket(&ecstore, dst_bucket.as_str()).await;
        set_bucket_lifecycle_transition_with_tier(dst_bucket.as_str(), &tier_name)
            .await
            .expect("Failed to set destination lifecycle configuration");

        upload_test_object(&ecstore, src_bucket.as_str(), src_object, payload).await;

        let mut src_info = ecstore
            .get_object_info(src_bucket.as_str(), src_object, &ObjectOptions::default())
            .await
            .expect("Failed to load source object info");
        src_info.put_object_reader = Some(PutObjReader::from_vec(payload.to_vec()));

        ecstore
            .copy_object(
                src_bucket.as_str(),
                src_object,
                dst_bucket.as_str(),
                dst_object,
                &mut src_info,
                &ObjectOptions::default(),
                &ObjectOptions::default(),
            )
            .await
            .expect("Failed to copy object");

        enqueue_transition_for_existing_objects(ecstore.clone(), dst_bucket.as_str())
            .await
            .expect("Failed to enqueue transitioned copied object");

        let copy_info = wait_for_transition(&ecstore, dst_bucket.as_str(), dst_object, TRANSITION_WAIT_TIMEOUT)
            .await
            .expect("copied object should transition after enqueueing existing objects");

        assert_eq!(copy_info.transitioned_object.status, "complete");
        assert_eq!(copy_info.transitioned_object.tier, tier_name);
        assert!(backend.objects.lock().await.contains_key(&copy_info.transitioned_object.name));

        let bucket_name = format!("test-lifecycle-update-{}", &Uuid::new_v4().simple().to_string()[..8]);
        let object_name = "test/existing.txt";
        let payload = b"existing object before lifecycle";

        create_test_bucket(&ecstore, bucket_name.as_str()).await;
        upload_test_object(&ecstore, bucket_name.as_str(), object_name, payload).await;

        set_bucket_lifecycle_transition_with_tier(bucket_name.as_str(), &tier_name)
            .await
            .expect("Failed to set lifecycle configuration");

        enqueue_transition_for_existing_objects(ecstore.clone(), bucket_name.as_str())
            .await
            .expect("Failed to enqueue transition for existing objects");

        let info = wait_for_transition(&ecstore, bucket_name.as_str(), object_name, TRANSITION_WAIT_TIMEOUT)
            .await
            .expect("existing object should transition after lifecycle update");

        assert_eq!(info.transitioned_object.status, "complete");
        assert_eq!(info.transitioned_object.tier, tier_name);
        assert!(backend.objects.lock().await.contains_key(&info.transitioned_object.name));

        let bucket_name = format!("test-restore-mpu-{}", &Uuid::new_v4().simple().to_string()[..8]);
        let object_name = "test/restore.txt";
        let part1 = vec![b'a'; 5 * 1024 * 1024];
        let part2 = b"restored-tail".to_vec();
        let expected = [part1.clone(), part2.clone()].concat();

        create_test_bucket(&ecstore, bucket_name.as_str()).await;
        set_bucket_lifecycle_transition_with_tier(bucket_name.as_str(), &tier_name)
            .await
            .expect("Failed to set lifecycle configuration");

        let upload = ecstore
            .new_multipart_upload(bucket_name.as_str(), object_name, &ObjectOptions::default())
            .await
            .expect("Failed to create multipart upload");

        let mut part1_reader = PutObjReader::from_vec(part1);
        let uploaded_part1 = ecstore
            .put_object_part(
                bucket_name.as_str(),
                object_name,
                &upload.upload_id,
                1,
                &mut part1_reader,
                &ObjectOptions::default(),
            )
            .await
            .expect("Failed to upload first multipart part");

        let mut part2_reader = PutObjReader::from_vec(part2);
        let uploaded_part2 = ecstore
            .put_object_part(
                bucket_name.as_str(),
                object_name,
                &upload.upload_id,
                2,
                &mut part2_reader,
                &ObjectOptions::default(),
            )
            .await
            .expect("Failed to upload second multipart part");

        ecstore
            .clone()
            .complete_multipart_upload(
                bucket_name.as_str(),
                object_name,
                &upload.upload_id,
                vec![
                    rustfs_ecstore::store_api::CompletePart {
                        part_num: 1,
                        etag: uploaded_part1.etag.clone(),
                        ..Default::default()
                    },
                    rustfs_ecstore::store_api::CompletePart {
                        part_num: 2,
                        etag: uploaded_part2.etag.clone(),
                        ..Default::default()
                    },
                ],
                &ObjectOptions::default(),
            )
            .await
            .expect("Failed to complete multipart upload");

        enqueue_transition_for_existing_objects(ecstore.clone(), bucket_name.as_str())
            .await
            .expect("Failed to enqueue transitioned restore object");

        let transitioned = wait_for_transition(&ecstore, bucket_name.as_str(), object_name, TRANSITION_WAIT_TIMEOUT)
            .await
            .expect("multipart object should transition after enqueueing existing objects");
        assert_eq!(transitioned.parts.len(), 2);

        ecstore
            .clone()
            .restore_transitioned_object(
                bucket_name.as_str(),
                object_name,
                &ObjectOptions {
                    transition: TransitionOptions {
                        restore_request: RestoreRequest {
                            days: Some(1),
                            description: None,
                            glacier_job_parameters: None,
                            output_location: None,
                            select_parameters: None,
                            tier: None,
                            type_: None,
                        },
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("Failed to restore transitioned multipart object");

        let restored = ecstore
            .get_object_info(bucket_name.as_str(), object_name, &ObjectOptions::default())
            .await
            .expect("Failed to load restored object info");
        assert_eq!(restored.parts.len(), 2);
        assert!(restored.restore_expires.is_some());
        assert!(!restored.restore_ongoing);

        let mut reader = ecstore
            .get_object_reader(bucket_name.as_str(), object_name, None, http::HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("Failed to read restored object");
        let mut data = Vec::new();
        reader
            .stream
            .read_to_end(&mut data)
            .await
            .expect("Failed to consume restored object stream");
        assert_eq!(data, expected);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial]
    #[ignore = "requires isolated global object layer state"]
    async fn test_scanner_enqueues_free_version_cleanup_for_stale_transitioned_object() {
        let (disk_paths, ecstore) = setup_isolated_test_env(false).await;

        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&tier_name).await;

        let bucket_name = format!("test-scanner-free-version-{}", &Uuid::new_v4().simple().to_string()[..8]);
        let object_name = "test/object.txt";
        let initial_payload = b"scanner should clean stale transitioned null version";
        create_test_bucket(&ecstore, bucket_name.as_str()).await;
        set_bucket_lifecycle_transition_with_tier(bucket_name.as_str(), &tier_name)
            .await
            .expect("Failed to set lifecycle configuration");

        upload_test_object(&ecstore, bucket_name.as_str(), object_name, initial_payload).await;
        enqueue_transition_for_existing_objects(ecstore.clone(), bucket_name.as_str())
            .await
            .expect("Failed to enqueue transitioned object");

        let transitioned = wait_for_transition(&ecstore, bucket_name.as_str(), object_name, TRANSITION_WAIT_TIMEOUT)
            .await
            .expect("object should transition before overwrite");
        let stale_remote_object = transitioned.transitioned_object.name.clone();
        assert!(backend.objects.lock().await.contains_key(&stale_remote_object));

        ecstore
            .delete_object(bucket_name.as_str(), object_name, ObjectOptions::default())
            .await
            .expect("Failed to delete transitioned object without expiry workers");

        assert!(
            free_version_count(&disk_paths[0], bucket_name.as_str(), object_name).await > 0,
            "deleting a transitioned null version should leave a free version for async cleanup"
        );
        assert!(
            backend.objects.lock().await.contains_key(&stale_remote_object),
            "stale transitioned remote object should still exist before scanner fallback runs"
        );

        rustfs_ecstore::bucket::lifecycle::bucket_lifecycle_ops::init_background_expiry(ecstore.clone()).await;
        scan_object_metadata(&disk_paths[0], bucket_name.as_str(), object_name).await;

        assert!(
            wait_for_remote_absence(&backend, &stale_remote_object, TRANSITION_WAIT_TIMEOUT).await,
            "scanner should enqueue stale free-version cleanup for the transitioned remote object"
        );
        assert_eq!(
            free_version_count(&disk_paths[0], bucket_name.as_str(), object_name).await,
            0,
            "free-version metadata should be removed after scanner-triggered cleanup"
        );
        assert!(
            wait_for_object_absence(&ecstore, bucket_name.as_str(), object_name, Duration::from_secs(1)).await,
            "deleted object should remain absent after scanner cleanup"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial]
    #[ignore = "requires isolated global object layer state"]
    async fn test_scanner_expires_zero_day_current_version() {
        let (disk_paths, ecstore) = setup_isolated_test_env(false).await;

        let bucket_name = format!("test-zero-day-expire-{}", &Uuid::new_v4().simple().to_string()[..8]);
        let object_name = "test/object.txt";

        create_test_bucket(&ecstore, bucket_name.as_str()).await;
        upload_test_object(&ecstore, bucket_name.as_str(), object_name, b"expire immediately").await;

        set_bucket_lifecycle(bucket_name.as_str())
            .await
            .expect("Failed to set lifecycle configuration");

        assert!(object_exists(&ecstore, bucket_name.as_str(), object_name).await);

        rustfs_ecstore::bucket::lifecycle::bucket_lifecycle_ops::init_background_expiry(ecstore.clone()).await;
        scan_object_with_lifecycle(&disk_paths[0], bucket_name.as_str(), object_name).await;

        assert!(
            wait_for_object_absence(&ecstore, bucket_name.as_str(), object_name, Duration::from_secs(3)).await,
            "scanner should delete zero-day current version after enqueueing expiry"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial]
    #[ignore = "requires isolated global object layer state"]
    async fn test_put_object_immediately_enqueues_zero_day_current_expiry() {
        let (_disk_paths, ecstore) = setup_isolated_test_env(true).await;

        let bucket_name = format!("test-put-zero-day-expire-{}", &Uuid::new_v4().simple().to_string()[..8]);
        let object_name = "expire-now.txt";

        create_test_bucket(&ecstore, bucket_name.as_str()).await;

        let lifecycle_xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
    <Rule>
        <ID>test-rule</ID>
        <Status>Enabled</Status>
        <Filter>
            <Prefix>{object_name}</Prefix>
        </Filter>
        <Expiration>
            <Days>0</Days>
        </Expiration>
    </Rule>
</LifecycleConfiguration>"#
        );
        metadata_sys::update(bucket_name.as_str(), BUCKET_LIFECYCLE_CONFIG, lifecycle_xml.into_bytes())
            .await
            .expect("Failed to set lifecycle configuration");

        upload_test_object(&ecstore, bucket_name.as_str(), object_name, b"expire immediately").await;

        assert!(
            wait_for_object_absence(&ecstore, bucket_name.as_str(), object_name, Duration::from_secs(2)).await,
            "put_object should enqueue zero-day current expiry without waiting for scanner"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial]
    #[ignore = "requires isolated global object layer state"]
    async fn test_scanner_expires_zero_day_noncurrent_version() {
        let (disk_paths, ecstore) = setup_isolated_test_env(false).await;

        let bucket_name = format!("test-zero-day-noncurrent-{}", &Uuid::new_v4().simple().to_string()[..8]);
        let object_name = "test/object.txt";

        create_test_lock_bucket(&ecstore, bucket_name.as_str()).await;

        let mut reader = PutObjReader::from_vec(b"v1".to_vec());
        ecstore
            .put_object(
                bucket_name.as_str(),
                object_name,
                &mut reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("failed to upload v1");
        let mut reader = PutObjReader::from_vec(b"v2".to_vec());
        ecstore
            .put_object(
                bucket_name.as_str(),
                object_name,
                &mut reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("failed to upload v2");

        assert_eq!(object_version_count(&ecstore, bucket_name.as_str(), object_name).await, 2);

        let lifecycle_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
    <Rule>
        <ID>test-rule</ID>
        <Status>Enabled</Status>
        <Filter>
            <Prefix>test/</Prefix>
        </Filter>
        <NoncurrentVersionExpiration>
            <NoncurrentDays>0</NoncurrentDays>
        </NoncurrentVersionExpiration>
    </Rule>
</LifecycleConfiguration>"#;
        metadata_sys::update(bucket_name.as_str(), BUCKET_LIFECYCLE_CONFIG, lifecycle_xml.as_bytes().to_vec())
            .await
            .expect("Failed to set noncurrent lifecycle configuration");

        rustfs_ecstore::bucket::lifecycle::bucket_lifecycle_ops::init_background_expiry(ecstore.clone()).await;

        scan_object_with_lifecycle(&disk_paths[0], bucket_name.as_str(), object_name).await;

        assert!(
            wait_for_version_count(&ecstore, bucket_name.as_str(), object_name, 1, Duration::from_secs(3)).await,
            "scanner should delete zero-day noncurrent versions after enqueueing expiry"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial]
    #[ignore = "requires isolated global object layer state"]
    async fn test_put_object_immediately_enqueues_zero_day_noncurrent_expiry() {
        let (_disk_paths, ecstore) = setup_isolated_test_env(true).await;

        let bucket_name = format!("test-put-zero-day-noncurrent-{}", &Uuid::new_v4().simple().to_string()[..8]);
        let object_name = "test/object.txt";

        create_test_lock_bucket(&ecstore, bucket_name.as_str()).await;

        let lifecycle_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
    <Rule>
        <ID>test-rule</ID>
        <Status>Enabled</Status>
        <Filter>
            <Prefix>test/</Prefix>
        </Filter>
        <NoncurrentVersionExpiration>
            <NoncurrentDays>0</NoncurrentDays>
        </NoncurrentVersionExpiration>
    </Rule>
</LifecycleConfiguration>"#;
        metadata_sys::update(bucket_name.as_str(), BUCKET_LIFECYCLE_CONFIG, lifecycle_xml.as_bytes().to_vec())
            .await
            .expect("Failed to set noncurrent lifecycle configuration");

        let mut reader = PutObjReader::from_vec(b"v1".to_vec());
        ecstore
            .put_object(
                bucket_name.as_str(),
                object_name,
                &mut reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("failed to upload v1");
        let mut reader = PutObjReader::from_vec(b"v2".to_vec());
        ecstore
            .put_object(
                bucket_name.as_str(),
                object_name,
                &mut reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("failed to upload v2");

        assert!(
            wait_for_version_count(&ecstore, bucket_name.as_str(), object_name, 1, Duration::from_secs(2)).await,
            "put_object should enqueue zero-day noncurrent expiry without waiting for scanner"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial]
    async fn test_background_scanner_expires_zero_day_current_version() {
        let (_disk_paths, ecstore) = setup_isolated_test_env(true).await;

        let bucket_name = format!("test-bg-zero-day-expire-{}", &Uuid::new_v4().simple().to_string()[..8]);
        let object_name = "test/object.txt";

        create_test_bucket(&ecstore, bucket_name.as_str()).await;
        set_bucket_lifecycle(bucket_name.as_str())
            .await
            .expect("Failed to set lifecycle configuration");
        upload_test_object(&ecstore, bucket_name.as_str(), object_name, b"expire immediately").await;

        let ctx = CancellationToken::new();
        init_data_scanner(ctx.clone(), ecstore.clone()).await;

        let deleted = wait_for_object_absence(&ecstore, bucket_name.as_str(), object_name, Duration::from_secs(12)).await;

        ctx.cancel();

        assert!(deleted, "background scanner should delete zero-day current version after startup delay");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial]
    #[ignore = "requires isolated global object layer state"]
    async fn test_background_scanner_expires_zero_day_current_version_for_exact_key_prefix() {
        let (_disk_paths, ecstore) = setup_isolated_test_env(true).await;

        let bucket_name = format!("test-bg-zero-day-exact-{}", &Uuid::new_v4().simple().to_string()[..8]);
        let object_name = "expire-now.txt";

        create_test_bucket(&ecstore, bucket_name.as_str()).await;

        let lifecycle_xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
    <Rule>
        <ID>test-rule</ID>
        <Status>Enabled</Status>
        <Filter>
            <Prefix>{object_name}</Prefix>
        </Filter>
        <Expiration>
            <Days>0</Days>
        </Expiration>
    </Rule>
</LifecycleConfiguration>"#
        );
        metadata_sys::update(bucket_name.as_str(), BUCKET_LIFECYCLE_CONFIG, lifecycle_xml.into_bytes())
            .await
            .expect("Failed to set lifecycle configuration");
        upload_test_object(&ecstore, bucket_name.as_str(), object_name, b"expire immediately").await;

        let ctx = CancellationToken::new();
        init_data_scanner(ctx.clone(), ecstore.clone()).await;

        let deleted = wait_for_object_absence(&ecstore, bucket_name.as_str(), object_name, Duration::from_secs(12)).await;

        ctx.cancel();

        assert!(deleted, "background scanner should delete zero-day exact-key lifecycle targets");
    }
}
