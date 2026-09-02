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

use futures::FutureExt;
use rustfs_config::ENV_TEST_FORCE_IMMEDIATE_TRANSITION_ENQUEUE_TIMEOUT;
use rustfs_scanner::scanner_folder::ScannerItem;
use rustfs_scanner::scanner_io::ScannerIODisk;
use rustfs_scanner::{
    ScannerObjectInfo as ObjectInfo, ScannerObjectOptions as ObjectOptions, ScannerPutObjReader as PutObjReader,
    scanner::init_data_scanner,
};
use s3s::dto::RestoreRequest;
use serial_test::serial;
use std::{
    collections::HashMap,
    env,
    path::{Path, PathBuf},
    sync::{Arc, Once, OnceLock},
    time::Duration,
};
use tokio::fs;
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;
use tracing::info;
use uuid::Uuid;

mod storage_api;

use storage_api::lifecycle::{
    BUCKET_LIFECYCLE_CONFIG, BucketOperations, BucketOptions, BucketVersioningSys, CompletePart, DiskOption, ECStore,
    EcstoreError, Endpoint, EndpointServerPools, Endpoints, ExpiryState, IlmAction, LcEvent, LcEventSrc, ListOperations as _,
    MakeBucketOptions, MockWarmBackend, MultipartOperations as _, ObjectIO as _, ObjectOperations as _, PoolEndpoints,
    STORAGE_FORMAT_FILE, TRANSITION_PENDING, TransitionCleanupStoreBarrier, TransitionOptions, assert_transition_meta_consistent,
    enqueue_transition_for_existing_objects, expire_transitioned_object, free_version_count, get_bucket_metadata,
    get_global_tier_config_mgr, init_background_expiry, init_bucket_metadata_sys, init_local_disks, is_err_object_not_found,
    is_err_version_not_found, new_disk, path2_bucket_object_with_base_path, recover_transition_transaction_records,
    register_mock_tier_util, update_bucket_metadata, wait_for_free_version_absence,
};

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

    let endpoint_pools = EndpointServerPools::from(vec![pool_endpoints]);

    // format disks (only first time)
    init_local_disks(endpoint_pools.clone()).await.unwrap();

    // create ECStore with dynamic port 0 (let OS assign) or fixed 9002 if free
    let port = 9002; // for simplicity
    let server_addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let ecstore = ECStore::new(server_addr, endpoint_pools, CancellationToken::new())
        .await
        .unwrap();

    // init bucket metadata system
    let buckets_list = ecstore
        .list_bucket(&BucketOptions {
            no_metadata: true,
            ..Default::default()
        })
        .await
        .unwrap();
    let buckets = buckets_list.into_iter().map(|v| v.name).collect();
    init_bucket_metadata_sys(ecstore.clone(), buckets).await;

    // Initialize background expiry workers
    init_background_expiry(ecstore.clone()).await;

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

    if init_expiry {
        init_background_expiry(ecstore.clone()).await;
    }

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

async fn modeled_versioned_delete_opts(bucket: &str, object: &str) -> ObjectOptions {
    ObjectOptions {
        versioned: BucketVersioningSys::prefix_enabled(bucket, object).await,
        version_suspended: BucketVersioningSys::prefix_suspended(bucket, object).await,
        ..Default::default()
    }
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

    update_bucket_metadata(bucket_name, BUCKET_LIFECYCLE_CONFIG, lifecycle_xml.as_bytes().to_vec()).await?;

    Ok(())
}

/// Test helper: Set bucket lifecycle configuration
async fn set_bucket_lifecycle_deletemarker(bucket_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Create lifecycle rule that targets delete-marker cleanup only.
    // Keep Expiration.Days unset to avoid expiring live transitioned object versions.
    let lifecycle_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
    <Rule>
        <ID>test-rule</ID>
        <Status>Enabled</Status>
        <Filter>
            <Prefix>test/</Prefix>
        </Filter>
        <Expiration>
            <ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker>
        </Expiration>
    </Rule>
</LifecycleConfiguration>"#;

    update_bucket_metadata(bucket_name, BUCKET_LIFECYCLE_CONFIG, lifecycle_xml.as_bytes().to_vec()).await?;

    Ok(())
}

async fn set_bucket_lifecycle_delmarker_expiration(bucket_name: &str, days: i64) -> Result<(), Box<dyn std::error::Error>> {
    let lifecycle_xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
    <Rule>
        <ID>test-rule</ID>
        <Status>Enabled</Status>
        <Filter>
            <Prefix>test/</Prefix>
        </Filter>
        <DelMarkerExpiration>
            <Days>{days}</Days>
        </DelMarkerExpiration>
    </Rule>
</LifecycleConfiguration>"#
    );

    update_bucket_metadata(bucket_name, BUCKET_LIFECYCLE_CONFIG, lifecycle_xml.into_bytes()).await?;

    Ok(())
}

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

    update_bucket_metadata(bucket_name, BUCKET_LIFECYCLE_CONFIG, lifecycle_xml.into_bytes()).await?;

    Ok(())
}

/// Test helper: Check if object exists
async fn object_exists(ecstore: &Arc<ECStore>, bucket: &str, object: &str) -> bool {
    match (**ecstore).get_object_info(bucket, object, &ObjectOptions::default()).await {
        Ok(info) => !info.delete_marker,
        Err(_) => false,
    }
}

/// Test helper: Check if object exists
async fn object_is_delete_marker(ecstore: &Arc<ECStore>, bucket: &str, object: &str) -> bool {
    if let Ok(oi) = (**ecstore).get_object_info(bucket, object, &ObjectOptions::default()).await {
        println!("oi: {oi:?}");
        oi.delete_marker
    } else {
        println!("object_is_delete_marker is error");
        panic!("object_is_delete_marker is error");
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
    let lifecycle = get_bucket_metadata(bucket)
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
        object_lock: None,
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
        object_lock: None,
        replication: None,
        heal_enabled: false,
        heal_bitrot: false,
        debug: false,
    };
    disk.get_size(item).await.expect("scanner get_size should succeed");
}

/// Register the shared [`MockWarmBackend`] into the global tier config manager
/// used by the scanner integration tests. Thin wrapper over the shared
/// `register_mock_tier` helper (rustfs/backlog#1148 ilm-6).
async fn register_mock_tier(tier_name: &str) -> MockWarmBackend {
    register_mock_tier_util(&get_global_tier_config_mgr(), tier_name).await
}

async fn wait_for_transition(ecstore: &Arc<ECStore>, bucket: &str, object: &str, timeout: Duration) -> Option<ObjectInfo> {
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

// SAFETY: this helper is used only by `#[serial]` tests and runs under the single-threaded Tokio
// runtime (`worker_threads = 1`), so no concurrent test can mutate process environment during the
// `env::set_var` / `env::remove_var` window.
#[allow(unsafe_code)]
async fn with_forced_immediate_enqueue_timeout<F, Fut>(test_fn: F)
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let original = env::var_os(ENV_TEST_FORCE_IMMEDIATE_TRANSITION_ENQUEUE_TIMEOUT);
    unsafe {
        env::set_var(ENV_TEST_FORCE_IMMEDIATE_TRANSITION_ENQUEUE_TIMEOUT, "1");
    }
    let result = std::panic::AssertUnwindSafe(test_fn()).catch_unwind().await;
    match original {
        Some(value) => unsafe {
            env::set_var(ENV_TEST_FORCE_IMMEDIATE_TRANSITION_ENQUEUE_TIMEOUT, value);
        },
        None => unsafe {
            env::remove_var(ENV_TEST_FORCE_IMMEDIATE_TRANSITION_ENQUEUE_TIMEOUT);
        },
    }
    if let Err(err) = result {
        std::panic::resume_unwind(err);
    }
}

// Deep transition futures can overflow libtest's default stack before their
// first assertion, so the serial ILM cases use one dedicated test thread.
fn run_large_stack_async_test<F, Fut>(thread_name: &'static str, test_fn: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + 'static,
{
    let handle = std::thread::Builder::new()
        .name(thread_name.to_string())
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("large-stack scanner test runtime should build");
            runtime.block_on(test_fn());
        })
        .expect("large-stack scanner test thread should spawn");
    if let Err(payload) = handle.join() {
        std::panic::resume_unwind(payload);
    }
}

mod serial_tests {
    use super::*;

    /// Regression for rustfs#3491 (backlog#1148 ilm-2): the expire/GET race on a
    /// transitioned (tiered) object.
    ///
    /// Before #3491, `expire_transitioned_object` removed the remote tier
    /// version **before** deleting local metadata. A GET that raced into the
    /// window between those two steps read local metadata still pointing at a
    /// remote version that was already gone, so the tier fetch failed and the
    /// client saw a spurious, user-visible error (`NoSuchVersion`). #3491
    /// flipped the ordering: local metadata is deleted **first** (the object
    /// becomes atomically unreachable) and remote-tier cleanup is deferred to
    /// persisted free-version recovery -- so no live local metadata ever points
    /// at an already-removed remote version.
    ///
    /// This test pins the fixed contract with deterministic GET and DELETE
    /// barriers (reverting to remote-first ordering turns it red):
    ///
    ///  1. A GET that already resolved the transitioned metadata keeps its read
    ///     lock and returns the complete remote body while expiry waits.
    ///  2. Expiry returns after committing the local free-version without
    ///     waiting for the post-commit worker's remote DELETE. While that DELETE
    ///     is paused, the durable marker and remote body must both still exist.
    ///  3. A later GET observes a clean object/version-not-found, never a tier
    ///     fetch or read-quorum failure.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial]
    #[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-2)"]
    async fn test_expire_transitioned_object_never_races_concurrent_get() {
        let (disk_paths, ecstore) = setup_isolated_test_env(false).await;

        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&tier_name).await;
        backend.set_put_remote_version(Some(String::new())).await;

        let bucket_name = format!("test-expire-get-race-{}", &Uuid::new_v4().simple().to_string()[..8]);
        let object_name = "test/race-object.bin";
        // Multi-block payload so the transitioned GET streams from the remote
        // tier rather than serving an inline fast-path.
        let payload: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();

        create_test_bucket(&ecstore, bucket_name.as_str()).await;
        set_bucket_lifecycle_transition_with_tier(bucket_name.as_str(), &tier_name)
            .await
            .expect("Failed to set lifecycle configuration");

        upload_test_object(&ecstore, bucket_name.as_str(), object_name, &payload).await;
        enqueue_transition_for_existing_objects(ecstore.clone(), bucket_name.as_str())
            .await
            .expect("Failed to enqueue transition for existing objects");

        let transitioned = wait_for_transition(&ecstore, bucket_name.as_str(), object_name, TRANSITION_WAIT_TIMEOUT)
            .await
            .expect("object should transition before expiry");
        let remote_object = transitioned.transitioned_object.name.clone();
        assert!(
            backend.contains(&remote_object).await,
            "transitioned remote object should exist in the mock tier before expiry"
        );
        assert_eq!(
            backend.remove_count().await,
            0,
            "no remote-tier removal should have happened before expiry"
        );

        // The ObjectInfo the scanner would hand to the expiry action.
        let oi = ecstore
            .get_object_info(bucket_name.as_str(), object_name, &ObjectOptions::default())
            .await
            .expect("Failed to load transitioned object info");
        assert!(
            oi.transitioned_object.version_id.is_empty(),
            "the regression must exercise an unversioned remote tier"
        );

        ExpiryState::resize_workers(1, ecstore.clone()).await;

        // Pause one real tier GET after it has resolved local transition
        // metadata. The reader still owns the object read lock, so local expiry
        // cannot commit until this GET finishes.
        let get_barrier = backend.arm_get_barrier().await;
        let get_store = ecstore.clone();
        let get_bucket = bucket_name.clone();
        let get_object = object_name.to_string();
        let in_flight_get = tokio::spawn(async move {
            let mut reader = get_store
                .get_object_reader(
                    get_bucket.as_str(),
                    get_object.as_str(),
                    None,
                    http::HeaderMap::new(),
                    &ObjectOptions::default(),
                )
                .await
                .map_err(|err| format!("in-flight GET failed before streaming: {err:?}"))?;
            let mut data = Vec::new();
            reader
                .stream
                .read_to_end(&mut data)
                .await
                .map_err(|err| format!("in-flight GET returned a failed or truncated stream: {err:?}"))?;
            Ok::<_, String>(data)
        });
        get_barrier.wait_until_paused().await;

        // The next remote DELETE pauses and then fails. A correct local-first
        // expiry returns while this barrier is still held; synchronous cleanup
        // (remote-first or local-first) instead times out here.
        let remove_barrier = backend.arm_failing_remove_barrier().await;
        get_barrier.release();

        // Run the exact expiry action the scanner drives for a transitioned
        // current version.
        let lc_event = LcEvent {
            action: IlmAction::DeleteAction,
            ..Default::default()
        };
        let bucket_incarnation_id = ecstore
            .bucket_incarnation_id(bucket_name.as_str())
            .await
            .expect("read bucket incarnation");
        let expiry_outcome = tokio::time::timeout(
            TRANSITION_WAIT_TIMEOUT,
            expire_transitioned_object(ecstore.clone(), &oi, &lc_event, &LcEventSrc::Scanner, bucket_incarnation_id),
        )
        .await;
        let remove_arrival = tokio::time::timeout(TRANSITION_WAIT_TIMEOUT, remove_barrier.wait_until_paused()).await;

        // Snapshot only lock-free observables while the cleanup worker holds
        // the object write lock. Store API reads wait until the barrier is
        // released below.
        let free_version_persisted = free_version_count(&disk_paths[0], bucket_name.as_str(), object_name).await > 0;
        let remote_present_at_cleanup = backend.contains(&remote_object).await;

        remove_barrier.release();
        let remove_operation_dropped = if remove_arrival.is_ok() {
            Some(tokio::time::timeout(TRANSITION_WAIT_TIMEOUT, remove_barrier.wait_until_operation_dropped()).await)
        } else {
            None
        };
        let in_flight_get_outcome = tokio::time::timeout(TRANSITION_WAIT_TIMEOUT, in_flight_get).await;
        let post_expiry_get = tokio::time::timeout(
            TRANSITION_WAIT_TIMEOUT,
            ecstore.get_object_reader(bucket_name.as_str(), object_name, None, http::HeaderMap::new(), &ObjectOptions::default()),
        )
        .await;

        expiry_outcome
            .expect("expire_transitioned_object must not wait for asynchronous remote-tier cleanup")
            .expect("expire_transitioned_object should succeed");
        remove_arrival.expect("the post-commit free-version worker should reach the remote DELETE barrier");
        remove_operation_dropped
            .expect("the remote DELETE should have reached the barrier")
            .expect("the injected remote DELETE should finish after release");
        assert!(
            free_version_persisted,
            "the durable free-version marker must exist before asynchronous remote cleanup"
        );
        assert!(
            remote_present_at_cleanup,
            "the remote object must remain readable until the paused cleanup DELETE is released"
        );

        let in_flight_body = in_flight_get_outcome
            .expect("the in-flight GET should finish within the test deadline")
            .expect("the in-flight GET task should not panic")
            .expect("a GET that wins the expiry race must return a complete body");
        assert_eq!(
            in_flight_body, payload,
            "a GET that resolved transitioned metadata before expiry must return the complete, correct body"
        );

        match post_expiry_get.expect("the post-expiry GET should finish within the test deadline") {
            Ok(_) => panic!("the locally expired transitioned object must no longer be readable"),
            Err(err) => {
                let ec: &EcstoreError = &err;
                assert!(
                    is_err_object_not_found(ec) || is_err_version_not_found(ec),
                    "a GET after expiry may only fail with a clean object/version-not-found; \
                     a tier-fetch or read-quorum failure is the #3491 regression: {err:?}"
                );
            }
        }
    }

    #[test]
    #[serial]
    #[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial"]
    fn rejected_transition_candidate_is_recovered_from_persisted_transaction() {
        run_large_stack_async_test(
            "scanner-rejected-transition-transaction",
            rejected_transition_candidate_is_recovered_from_persisted_transaction_case,
        );
    }

    async fn rejected_transition_candidate_is_recovered_from_persisted_transaction_case() {
        let (_disk_paths, ecstore) = setup_isolated_test_env(false).await;

        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&tier_name).await;
        backend.set_put_read_limit(Some(4096)).await;
        backend.set_put_remote_version(Some(String::new())).await;
        backend.set_remove_failure(true);

        let bucket_name = format!("test-transition-cleanup-journal-{}", &Uuid::new_v4().simple().to_string()[..8]);
        let object_name = "test/rejected-candidate.bin";
        let payload = b"rejected remote candidate must not replace the local source".repeat(1024);

        create_test_bucket(&ecstore, bucket_name.as_str()).await;
        upload_test_object(&ecstore, bucket_name.as_str(), object_name, &payload).await;
        let original = ecstore
            .get_object_info(bucket_name.as_str(), object_name, &ObjectOptions::default())
            .await
            .expect("source object metadata should resolve before transition");
        let opts = ObjectOptions {
            no_lock: true,
            transition: TransitionOptions {
                status: TRANSITION_PENDING.to_string(),
                tier: tier_name,
                etag: original.etag.clone().expect("uploaded source object should have an ETag"),
                ..Default::default()
            },
            version_id: original.version_id.map(|version| version.to_string()),
            mod_time: original.mod_time,
            ..Default::default()
        };

        ecstore
            .transition_object(bucket_name.as_str(), object_name, &opts)
            .await
            .expect_err("a remotely accepted partial upload must not commit transition metadata");

        let put_versions = backend.put_versions().await;
        assert_eq!(put_versions.len(), 1, "transition should create exactly one remote candidate");
        assert!(put_versions[0].1.is_empty());
        assert_eq!(
            backend.object_count().await,
            1,
            "failed cleanup must retain the remote candidate for recovery"
        );
        assert!(
            backend.remove_versions().await.is_empty(),
            "no cleanup path may delete the candidate while remove failures are enabled"
        );

        let retained = recover_transition_transaction_records(ecstore.clone(), 100, None)
            .await
            .expect("transition transaction recovery should scan the persisted candidate");
        assert_eq!((retained.scanned, retained.recovered, retained.retained, retained.failed), (1, 0, 0, 1));
        assert_eq!(backend.object_count().await, 1, "failed recovery must retain the remote candidate");

        backend.set_remove_failure(false);
        let recovered = recover_transition_transaction_records(ecstore.clone(), 100, None)
            .await
            .expect("transition transaction recovery should delete the retained candidate");
        assert_eq!(
            (recovered.scanned, recovered.recovered, recovered.retained, recovered.failed),
            (1, 1, 0, 0)
        );
        let removed_versions = backend.remove_versions().await;
        assert!(!removed_versions.is_empty(), "recovery must issue at least one successful delete");
        assert!(
            removed_versions.iter().all(|removed| removed == &put_versions[0]),
            "every idempotent cleanup must delete the exact PUT object and version"
        );
        assert_eq!(backend.object_count().await, 0, "recovery should remove the rejected remote candidate");

        let empty = recover_transition_transaction_records(ecstore.clone(), 100, None)
            .await
            .expect("a removed transition transaction should no longer be listed");
        assert_eq!(empty.scanned, 0, "successful recovery must remove the persisted transaction");
        assert_eq!(
            read_object_fully(&ecstore, bucket_name.as_str(), object_name).await,
            payload,
            "rejected transition cleanup must leave the source object readable"
        );
    }

    #[test]
    #[serial]
    #[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial"]
    fn cancelled_before_cleanup_store_resolution_persists_transaction() {
        run_large_stack_async_test(
            "scanner-cancelled-transition-transaction",
            cancelled_before_cleanup_store_resolution_persists_transaction_case,
        );
    }

    async fn cancelled_before_cleanup_store_resolution_persists_transaction_case() {
        let (_disk_paths, ecstore) = setup_isolated_test_env(false).await;
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&tier_name).await;
        backend.set_put_remote_version(Some(Uuid::new_v4().to_string())).await;
        backend.reject_next_non_empty_remote_version_validation();
        backend.set_remove_failure(true);
        let cleanup_store_barrier = TransitionCleanupStoreBarrier::install();

        let bucket_name = format!("test-transition-cancel-cleanup-{}", &Uuid::new_v4().simple().to_string()[..8]);
        let object_name = "test/rejected-candidate.bin";
        let payload = b"cancelled rejected cleanup must retain durable recovery evidence".repeat(1024);
        create_test_bucket(&ecstore, bucket_name.as_str()).await;
        upload_test_object(&ecstore, bucket_name.as_str(), object_name, &payload).await;
        let original = ecstore
            .get_object_info(bucket_name.as_str(), object_name, &ObjectOptions::default())
            .await
            .expect("source object metadata should resolve before transition");
        let opts = ObjectOptions {
            no_lock: true,
            transition: TransitionOptions {
                status: TRANSITION_PENDING.to_string(),
                tier: tier_name,
                etag: original.etag.clone().expect("uploaded source object should have an ETag"),
                ..Default::default()
            },
            version_id: original.version_id.map(|version| version.to_string()),
            mod_time: original.mod_time,
            ..Default::default()
        };

        let transition_store = ecstore.clone();
        let transition_bucket = bucket_name.clone();
        let transition = tokio::spawn(async move {
            transition_store
                .transition_object(transition_bucket.as_str(), object_name, &opts)
                .await
        });
        cleanup_store_barrier.wait_until_paused().await;
        transition.abort();
        assert!(
            transition
                .await
                .expect_err("aborted transition task should be cancelled")
                .is_cancelled()
        );

        let retained = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let recovery = recover_transition_transaction_records(ecstore.clone(), 100, None)
                    .await
                    .expect("the cancelled transition transaction should be readable");
                if recovery.scanned > 0 {
                    break recovery;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Drop should persist the rejected candidate through the saved instance context");
        assert_eq!((retained.scanned, retained.recovered, retained.retained, retained.failed), (1, 0, 0, 1));
        tokio::time::timeout(Duration::from_secs(5), async {
            while backend.exact_remove_count() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Drop cleanup and failed transaction recovery must both preserve the exact version constraint");
        let failed_exact_attempts = backend.exact_remove_count();
        assert_eq!(
            failed_exact_attempts, 2,
            "the cancelled task and the first failed recovery must each preserve the exact delete constraint"
        );
        assert_eq!(backend.object_count().await, 1);
        assert!(backend.remove_versions().await.is_empty());

        backend.set_remove_failure(false);
        let recovered = recover_transition_transaction_records(ecstore.clone(), 100, None)
            .await
            .expect("recovery should delete the candidate retained by the cancelled transition");
        assert_eq!(
            (recovered.scanned, recovered.recovered, recovered.retained, recovered.failed),
            (1, 1, 0, 0)
        );
        assert_eq!(backend.remove_versions().await, backend.put_versions().await);
        assert_eq!(backend.exact_remove_count(), failed_exact_attempts + 1);
        assert_eq!(backend.object_count().await, 0);
        let empty = recover_transition_transaction_records(ecstore.clone(), 100, None)
            .await
            .expect("successful recovery should remove the cancellation transaction");
        assert_eq!(empty.scanned, 0);
        assert_eq!(
            read_object_fully(&ecstore, bucket_name.as_str(), object_name).await,
            payload,
            "cancelled transition cleanup must preserve the local source"
        );
    }

    #[test]
    #[serial]
    #[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial"]
    fn rejected_transition_cleanup_durability_matrix() {
        run_large_stack_async_test("scanner-transition-cleanup-matrix", rejected_transition_cleanup_durability_matrix_case);
    }

    async fn rejected_transition_cleanup_durability_matrix_case() {
        #[derive(Clone, Copy)]
        enum CleanupCase {
            Persisted,
            DeleteFallback,
            RetryPersisted,
            FullyFailed,
        }

        let (_disk_paths, ecstore) = setup_isolated_test_env(false).await;

        for case in [
            CleanupCase::Persisted,
            CleanupCase::DeleteFallback,
            CleanupCase::RetryPersisted,
            CleanupCase::FullyFailed,
        ] {
            let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
            let backend = register_mock_tier(&tier_name).await;
            let remote_version = Uuid::new_v4().to_string();
            backend.set_put_remote_version(Some(remote_version.clone())).await;
            backend.reject_next_non_empty_remote_version_validation();
            backend.set_remove_failure(matches!(case, CleanupCase::FullyFailed));
            let put_barrier = backend.arm_put_barrier().await;
            let remove_barrier = if matches!(case, CleanupCase::RetryPersisted) {
                Some(backend.arm_failing_remove_barrier().await)
            } else {
                None
            };

            let bucket_name = format!("test-transition-journal-failure-{}", &Uuid::new_v4().simple().to_string()[..8]);
            let object_name = "test/rejected-candidate.bin";
            let payload = b"journal failure must either delete the exact candidate or report both failures".repeat(1024);
            create_test_bucket(&ecstore, bucket_name.as_str()).await;
            upload_test_object(&ecstore, bucket_name.as_str(), object_name, &payload).await;
            let original = ecstore
                .get_object_info(bucket_name.as_str(), object_name, &ObjectOptions::default())
                .await
                .expect("source object metadata should resolve before transition");
            let opts = ObjectOptions {
                no_lock: true,
                transition: TransitionOptions {
                    status: TRANSITION_PENDING.to_string(),
                    tier: tier_name,
                    etag: original.etag.clone().expect("uploaded source object should have an ETag"),
                    ..Default::default()
                },
                version_id: original.version_id.map(|version| version.to_string()),
                mod_time: original.mod_time,
                ..Default::default()
            };

            let transition_store = ecstore.clone();
            let transition_bucket = bucket_name.clone();
            let transition = tokio::spawn(async move {
                transition_store
                    .transition_object(transition_bucket.as_str(), object_name, &opts)
                    .await
            });
            put_barrier.wait_until_paused().await;
            let set = ecstore.pools[0].get_disks(0);
            let mut saved_disks = if matches!(case, CleanupCase::Persisted) {
                None
            } else {
                let mut disks = set.disks.write().await;
                let saved = std::mem::take(&mut *disks);
                *disks = vec![None; saved.len()];
                Some(saved)
            };
            put_barrier.release();
            if let Some(remove_barrier) = remove_barrier.as_ref() {
                remove_barrier.wait_until_paused().await;
                *set.disks.write().await = saved_disks.take().expect("offline disks should be restorable");
                backend.set_remove_failure(true);
                remove_barrier.release();
            }
            let transition_result = tokio::time::timeout(Duration::from_secs(30), transition).await;
            if let Some(saved_disks) = saved_disks {
                *set.disks.write().await = saved_disks;
            }
            let err = transition_result
                .expect("transition should finish while validating cleanup durability")
                .expect("transition task should not panic")
                .expect_err("a versioned candidate must not commit to an unversioned tier");

            match case {
                CleanupCase::Persisted | CleanupCase::DeleteFallback => {
                    assert_eq!(backend.remove_versions().await, backend.put_versions().await);
                    assert_eq!(backend.object_count().await, 0, "cleanup must remove the exact candidate");
                    let recovery = recover_transition_transaction_records(ecstore.clone(), 100, None)
                        .await
                        .expect("successful cleanup must leave no failed transition transaction");
                    assert_eq!(recovery.failed, 0);
                    assert_eq!(recovery.retained, 0);
                    assert_eq!(recovery.recovered, recovery.scanned);
                    let empty = recover_transition_transaction_records(ecstore.clone(), 100, None)
                        .await
                        .expect("successful reconciliation must remove every transition transaction");
                    assert_eq!(empty.scanned, 0);
                }
                CleanupCase::RetryPersisted => {
                    assert!(
                        !err.to_string().contains("journal retry error"),
                        "the transaction path must not surface the removed journal fallback error"
                    );
                    assert_eq!(backend.object_count().await, 1);
                    let retained = recover_transition_transaction_records(ecstore.clone(), 100, None)
                        .await
                        .expect("the retained transaction should be recoverable");
                    assert_eq!((retained.scanned, retained.recovered, retained.retained, retained.failed), (1, 0, 0, 1));
                    backend.set_remove_failure(false);
                    let recovered = recover_transition_transaction_records(ecstore.clone(), 100, None)
                        .await
                        .expect("recovery should delete the exact transaction candidate");
                    assert_eq!(
                        (recovered.scanned, recovered.recovered, recovered.retained, recovered.failed),
                        (1, 1, 0, 0)
                    );
                    assert_eq!(backend.remove_versions().await, backend.put_versions().await);
                    assert_eq!(backend.object_count().await, 0);
                    let empty = recover_transition_transaction_records(ecstore.clone(), 100, None)
                        .await
                        .expect("successful recovery must remove the retained transaction");
                    assert_eq!(empty.scanned, 0);
                }
                CleanupCase::FullyFailed => {
                    let message = err.to_string();
                    assert!(message.contains("cleanup error"), "{message}");
                    assert_eq!(backend.object_count().await, 1, "both failed safeguards must leave the candidate visible");
                    assert!(backend.remove_versions().await.is_empty());
                    let retained = recover_transition_transaction_records(ecstore.clone(), 100, None)
                        .await
                        .expect("the pre-upload transaction must retain ownership after cleanup failure");
                    assert_eq!(retained.scanned, 1, "the failed cleanup must keep one durable transaction owner");
                    assert_eq!(retained.recovered, 0);
                    assert_eq!(retained.retained + retained.failed, 1);
                    backend.set_remove_failure(false);
                    let recovered = recover_transition_transaction_records(ecstore.clone(), 100, None)
                        .await
                        .expect("recovery should delete the candidate after the backend becomes available");
                    assert_eq!(
                        (recovered.scanned, recovered.recovered, recovered.retained, recovered.failed),
                        (1, 1, 0, 0)
                    );
                    assert_eq!(backend.remove_versions().await, backend.put_versions().await);
                    assert_eq!(backend.object_count().await, 0);
                    let empty = recover_transition_transaction_records(ecstore.clone(), 100, None)
                        .await
                        .expect("successful recovery must remove the failed cleanup transaction");
                    assert_eq!(empty.scanned, 0);
                }
            }
            assert_eq!(
                read_object_fully(&ecstore, bucket_name.as_str(), object_name).await,
                payload,
                "rejected transition cleanup must preserve the local source"
            );
        }
    }

    #[test]
    #[serial]
    #[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
    fn test_transition_and_restore_flows() {
        run_large_stack_async_test("scanner-transition-restore-flows", test_transition_and_restore_flows_inner);
    }

    async fn test_transition_and_restore_flows_inner() {
        let (disk_paths, ecstore) = setup_test_env().await;

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
        assert!(backend.contains(&put_info.transitioned_object.name).await);
        {
            let transitioned = backend
                .stored(&put_info.transitioned_object.name)
                .await
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

        // Cross-shard xl.meta transition assertion helper (rustfs/backlog#1148 ilm-6):
        // every disk must agree on the transition tuple for the object.
        let put_meta = assert_transition_meta_consistent(&disk_paths, put_bucket.as_str(), put_object).await;
        assert_eq!(put_meta.status, "complete");
        assert_eq!(put_meta.tier, tier_name);

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
                vec![CompletePart {
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
        assert!(backend.contains(&multipart_info.transitioned_object.name).await);

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
        assert!(backend.contains(&copy_info.transitioned_object.name).await);

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
        assert!(backend.contains(&info.transitioned_object.name).await);

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
                    CompletePart {
                        part_num: 1,
                        etag: uploaded_part1.etag.clone(),
                        ..Default::default()
                    },
                    CompletePart {
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
    #[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
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
        assert!(backend.contains(&stale_remote_object).await);

        ExpiryState::resize_workers(1, ecstore.clone()).await;
        let remove_barrier = backend.arm_failing_remove_barrier().await;
        tokio::time::timeout(
            Duration::from_secs(5),
            ecstore.delete_object(bucket_name.as_str(), object_name, ObjectOptions::default()),
        )
        .await
        .expect("DeleteObject must not wait for asynchronous remote-tier cleanup")
        .expect("Failed to delete transitioned object before scanner fallback");
        tokio::time::timeout(Duration::from_secs(5), remove_barrier.wait_until_paused())
            .await
            .expect("the immediate free-version worker should reach the injected remote DELETE barrier");

        assert!(
            free_version_count(&disk_paths[0], bucket_name.as_str(), object_name).await > 0,
            "deleting a transitioned null version should leave a free version for async cleanup"
        );
        assert!(
            backend.contains(&stale_remote_object).await,
            "stale transitioned remote object should still exist before scanner fallback runs"
        );

        // Queue the scanner fallback while the causal task is still blocked.
        // Releasing the barrier fails only that first task, so the queued
        // scanner task can prove durable-marker recovery on a healthy backend.
        scan_object_metadata(&disk_paths[0], bucket_name.as_str(), object_name).await;
        remove_barrier.release();
        remove_barrier.wait_until_operation_dropped().await;

        assert!(
            backend
                .wait_for_remote_absence(&stale_remote_object, TRANSITION_WAIT_TIMEOUT)
                .await,
            "scanner should enqueue stale free-version cleanup for the transitioned remote object"
        );
        assert!(
            wait_for_free_version_absence(&disk_paths[0], bucket_name.as_str(), object_name, TRANSITION_WAIT_TIMEOUT).await,
            "free-version metadata should be removed after scanner-triggered cleanup"
        );
        assert!(
            wait_for_object_absence(&ecstore, bucket_name.as_str(), object_name, Duration::from_secs(1)).await,
            "deleted object should remain absent after scanner cleanup"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial]
    #[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
    async fn test_scanner_cleanup_still_works_after_immediate_compensation_transition() {
        let (disk_paths, ecstore) = setup_isolated_test_env(false).await;

        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&tier_name).await;

        let bucket_name = format!("test-scanner-after-compensation-{}", &Uuid::new_v4().simple().to_string()[..8]);
        let object_name = "test/object.txt";
        let payload = b"scanner cleanup should still work after immediate compensation";

        create_test_bucket(&ecstore, bucket_name.as_str()).await;
        set_bucket_lifecycle_transition_with_tier(bucket_name.as_str(), &tier_name)
            .await
            .expect("Failed to set lifecycle configuration");

        with_forced_immediate_enqueue_timeout(|| async {
            upload_test_object(&ecstore, bucket_name.as_str(), object_name, payload).await;
        })
        .await;

        let transitioned = wait_for_transition(&ecstore, bucket_name.as_str(), object_name, TRANSITION_WAIT_TIMEOUT)
            .await
            .expect("object should transition after compensation backfill");
        let stale_remote_object = transitioned.transitioned_object.name.clone();
        assert!(backend.contains(&stale_remote_object).await);

        ExpiryState::resize_workers(1, ecstore.clone()).await;
        let remove_barrier = backend.arm_failing_remove_barrier().await;
        tokio::time::timeout(
            Duration::from_secs(5),
            ecstore.delete_object(bucket_name.as_str(), object_name, ObjectOptions::default()),
        )
        .await
        .expect("DeleteObject must not wait for asynchronous remote-tier cleanup")
        .expect("Failed to delete transitioned object after compensation-driven transition");
        tokio::time::timeout(Duration::from_secs(5), remove_barrier.wait_until_paused())
            .await
            .expect("the immediate free-version worker should reach the injected remote DELETE barrier");

        assert!(
            free_version_count(&disk_paths[0], bucket_name.as_str(), object_name).await > 0,
            "deleting a compensation-transitioned null version should leave a free version for async cleanup"
        );
        assert!(
            backend.contains(&stale_remote_object).await,
            "stale transitioned remote object should still exist before scanner cleanup runs"
        );

        // Enqueue the scanner fallback before the first, causal cleanup task is
        // released into its injected failure. This keeps attribution
        // deterministic and proves the durable marker drives convergence.
        scan_object_metadata(&disk_paths[0], bucket_name.as_str(), object_name).await;
        remove_barrier.release();
        remove_barrier.wait_until_operation_dropped().await;

        assert!(
            backend
                .wait_for_remote_absence(&stale_remote_object, TRANSITION_WAIT_TIMEOUT)
                .await,
            "scanner should clean stale remote object even after immediate compensation transitioned it"
        );
        assert!(
            wait_for_free_version_absence(&disk_paths[0], bucket_name.as_str(), object_name, TRANSITION_WAIT_TIMEOUT).await,
            "free-version metadata should be removed after scanner cleanup"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial]
    #[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
    async fn test_existing_object_backfill_is_idempotent_after_immediate_compensation_transition() {
        let (_disk_paths, ecstore) = setup_isolated_test_env(false).await;

        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&tier_name).await;

        let bucket_name = format!("test-backfill-after-compensation-{}", &Uuid::new_v4().simple().to_string()[..8]);
        let object_name = "test/object.txt";
        let payload = b"existing-object backfill should be idempotent after compensation transition";

        create_test_bucket(&ecstore, bucket_name.as_str()).await;
        set_bucket_lifecycle_transition_with_tier(bucket_name.as_str(), &tier_name)
            .await
            .expect("Failed to set lifecycle configuration");

        with_forced_immediate_enqueue_timeout(|| async {
            upload_test_object(&ecstore, bucket_name.as_str(), object_name, payload).await;
        })
        .await;

        let transitioned = wait_for_transition(&ecstore, bucket_name.as_str(), object_name, TRANSITION_WAIT_TIMEOUT)
            .await
            .expect("object should transition after immediate compensation backfill");
        let remote_object = transitioned.transitioned_object.name.clone();
        assert!(backend.contains(&remote_object).await);

        enqueue_transition_for_existing_objects(ecstore.clone(), bucket_name.as_str())
            .await
            .expect("existing-object backfill should succeed after compensation transition");

        let info = wait_for_transition(&ecstore, bucket_name.as_str(), object_name, TRANSITION_WAIT_TIMEOUT)
            .await
            .expect("object should remain transitioned after existing-object backfill rerun");

        assert_eq!(info.transitioned_object.status, "complete");
        assert_eq!(info.transitioned_object.tier, tier_name);
        assert_eq!(info.transitioned_object.name, remote_object);
        assert!(backend.contains(&remote_object).await);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial]
    #[ignore = "FAILING on main: excluded from the serial ILM lane pending a fix, see rustfs/backlog#1148 (ilm-1 partial)"]
    async fn test_noncurrent_expiry_still_works_after_immediate_compensation_transition() {
        let (disk_paths, ecstore) = setup_isolated_test_env(true).await;

        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&tier_name).await;

        let bucket_name = format!("test-versioned-compensation-{}", &Uuid::new_v4().simple().to_string()[..8]);
        let object_name = "test/object.txt";

        create_test_lock_bucket(&ecstore, bucket_name.as_str()).await;

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
          <StorageClass>{tier_name}</StorageClass>
        </Transition>
        <NoncurrentVersionExpiration>
            <NoncurrentDays>0</NoncurrentDays>
        </NoncurrentVersionExpiration>
    </Rule>
</LifecycleConfiguration>"#
        );
        update_bucket_metadata(bucket_name.as_str(), BUCKET_LIFECYCLE_CONFIG, lifecycle_xml.into_bytes())
            .await
            .expect("Failed to set lifecycle configuration");

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

        with_forced_immediate_enqueue_timeout(|| async {
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
        })
        .await;

        let info = wait_for_transition(&ecstore, bucket_name.as_str(), object_name, TRANSITION_WAIT_TIMEOUT)
            .await
            .expect("current version should transition after compensation backfill");

        assert_eq!(info.transitioned_object.status, "complete");
        assert_eq!(info.transitioned_object.tier, tier_name);
        assert!(backend.contains(&info.transitioned_object.name).await);

        scan_object_with_lifecycle(&disk_paths[0], bucket_name.as_str(), object_name).await;

        assert!(
            wait_for_version_count(&ecstore, bucket_name.as_str(), object_name, 1, Duration::from_secs(3)).await,
            "noncurrent expiry should still remove the previous version after compensation transition"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial]
    #[ignore = "FAILING on main: excluded from the serial ILM lane pending a fix, see rustfs/backlog#1148 (ilm-1 partial)"]
    async fn test_noncurrent_transition_still_works_after_immediate_compensation_transition() {
        let (disk_paths, ecstore) = setup_isolated_test_env(true).await;

        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&tier_name).await;

        let bucket_name = format!("test-noncurrent-transition-comp-{}", &Uuid::new_v4().simple().to_string()[..8]);
        let object_name = "test/object.txt";

        create_test_lock_bucket(&ecstore, bucket_name.as_str()).await;

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
          <StorageClass>{tier_name}</StorageClass>
        </Transition>
        <NoncurrentVersionTransition>
            <NoncurrentDays>0</NoncurrentDays>
            <StorageClass>{tier_name}</StorageClass>
        </NoncurrentVersionTransition>
    </Rule>
</LifecycleConfiguration>"#
        );
        update_bucket_metadata(bucket_name.as_str(), BUCKET_LIFECYCLE_CONFIG, lifecycle_xml.into_bytes())
            .await
            .expect("Failed to set lifecycle configuration");

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

        with_forced_immediate_enqueue_timeout(|| async {
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
        })
        .await;

        let info = wait_for_transition(&ecstore, bucket_name.as_str(), object_name, TRANSITION_WAIT_TIMEOUT)
            .await
            .expect("current version should transition after compensation backfill");
        assert_eq!(info.transitioned_object.status, "complete");
        assert_eq!(info.transitioned_object.tier, tier_name);

        scan_object_with_lifecycle(&disk_paths[0], bucket_name.as_str(), object_name).await;

        assert!(
            backend.wait_for_object_count(2, TRANSITION_WAIT_TIMEOUT).await,
            "noncurrent transition should still move the previous version into the remote tier"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial]
    #[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
    async fn test_modeled_versioned_delete_creates_delete_marker_after_immediate_compensation_transition() {
        let (_disk_paths, ecstore) = setup_isolated_test_env(true).await;

        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&tier_name).await;

        let bucket_name = format!("test-modeled-versioned-delete-{}", &Uuid::new_v4().simple().to_string()[..8]);
        let object_name = "test/object.txt";
        let payload = b"modeled versioned delete should create delete marker after compensation";

        create_test_lock_bucket(&ecstore, bucket_name.as_str()).await;
        set_bucket_lifecycle_transition_with_tier(bucket_name.as_str(), &tier_name)
            .await
            .expect("Failed to set transition lifecycle configuration");

        with_forced_immediate_enqueue_timeout(|| async {
            upload_test_object(&ecstore, bucket_name.as_str(), object_name, payload).await;
        })
        .await;

        let transitioned = wait_for_transition(&ecstore, bucket_name.as_str(), object_name, TRANSITION_WAIT_TIMEOUT)
            .await
            .expect("current version should transition after compensation backfill");
        let remote_object = transitioned.transitioned_object.name.clone();
        assert!(backend.contains(&remote_object).await);

        ecstore
            .delete_object(
                bucket_name.as_str(),
                object_name,
                modeled_versioned_delete_opts(bucket_name.as_str(), object_name).await,
            )
            .await
            .expect("modeled versioned delete should succeed");

        assert!(
            object_is_delete_marker(&ecstore, bucket_name.as_str(), object_name).await,
            "versioned delete modeled with versioned flags should create a delete marker"
        );
        assert!(
            backend.contains(&remote_object).await,
            "creating a delete marker should not remove the transitioned remote object version"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial]
    #[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
    async fn test_modeled_delete_marker_cleanup_after_immediate_compensation_transition() {
        let (disk_paths, ecstore) = setup_isolated_test_env(true).await;

        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&tier_name).await;

        let bucket_name = format!("test-modeled-del-marker-cleanup-{}", &Uuid::new_v4().simple().to_string()[..8]);
        let object_name = "test/object.txt";
        let payload = b"modeled delete-marker cleanup should converge after compensation transition";

        create_test_lock_bucket(&ecstore, bucket_name.as_str()).await;
        set_bucket_lifecycle_transition_with_tier(bucket_name.as_str(), &tier_name)
            .await
            .expect("Failed to set transition lifecycle configuration");

        with_forced_immediate_enqueue_timeout(|| async {
            upload_test_object(&ecstore, bucket_name.as_str(), object_name, payload).await;
        })
        .await;

        let transitioned = wait_for_transition(&ecstore, bucket_name.as_str(), object_name, TRANSITION_WAIT_TIMEOUT)
            .await
            .expect("current version should transition after compensation backfill");
        let remote_object = transitioned.transitioned_object.name.clone();
        assert!(backend.contains(&remote_object).await);

        ecstore
            .delete_object(
                bucket_name.as_str(),
                object_name,
                modeled_versioned_delete_opts(bucket_name.as_str(), object_name).await,
            )
            .await
            .expect("modeled versioned delete should succeed");

        assert!(
            object_is_delete_marker(&ecstore, bucket_name.as_str(), object_name).await,
            "modeled versioned delete should create delete marker before cleanup"
        );
        assert!(
            backend.contains(&remote_object).await,
            "delete marker creation should not remove transitioned remote object"
        );

        set_bucket_lifecycle_delmarker_expiration(bucket_name.as_str(), 1)
            .await
            .expect("Failed to set delete marker expiration lifecycle configuration");

        scan_object_with_lifecycle(&disk_paths[0], bucket_name.as_str(), object_name).await;

        assert!(
            object_is_delete_marker(&ecstore, bucket_name.as_str(), object_name).await,
            "delete marker should remain before DelMarkerExpiration due time"
        );
        assert!(
            backend.contains(&remote_object).await,
            "pre-due delete marker lifecycle scan should not remove transitioned remote object"
        );

        set_bucket_lifecycle_deletemarker(bucket_name.as_str())
            .await
            .expect("Failed to set expired object delete marker lifecycle configuration");
        scan_object_with_lifecycle(&disk_paths[0], bucket_name.as_str(), object_name).await;

        assert!(
            wait_for_object_absence(&ecstore, bucket_name.as_str(), object_name, Duration::from_secs(5)).await,
            "expired object delete marker lifecycle should eventually clean up the delete marker"
        );
        assert!(
            backend.contains(&remote_object).await,
            "delete marker lifecycle cleanup should not remove transitioned remote object"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial]
    #[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
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

        init_background_expiry(ecstore.clone()).await;
        scan_object_with_lifecycle(&disk_paths[0], bucket_name.as_str(), object_name).await;

        assert!(
            wait_for_object_absence(&ecstore, bucket_name.as_str(), object_name, Duration::from_secs(3)).await,
            "scanner should delete zero-day current version after enqueueing expiry"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial]
    #[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
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
        update_bucket_metadata(bucket_name.as_str(), BUCKET_LIFECYCLE_CONFIG, lifecycle_xml.into_bytes())
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
    #[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
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
        update_bucket_metadata(bucket_name.as_str(), BUCKET_LIFECYCLE_CONFIG, lifecycle_xml.as_bytes().to_vec())
            .await
            .expect("Failed to set noncurrent lifecycle configuration");

        init_background_expiry(ecstore.clone()).await;

        scan_object_with_lifecycle(&disk_paths[0], bucket_name.as_str(), object_name).await;

        assert!(
            wait_for_version_count(&ecstore, bucket_name.as_str(), object_name, 1, Duration::from_secs(3)).await,
            "scanner should delete zero-day noncurrent versions after enqueueing expiry"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial]
    #[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
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
        update_bucket_metadata(bucket_name.as_str(), BUCKET_LIFECYCLE_CONFIG, lifecycle_xml.as_bytes().to_vec())
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
    #[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
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
        update_bucket_metadata(bucket_name.as_str(), BUCKET_LIFECYCLE_CONFIG, lifecycle_xml.into_bytes())
            .await
            .expect("Failed to set lifecycle configuration");
        upload_test_object(&ecstore, bucket_name.as_str(), object_name, b"expire immediately").await;

        let ctx = CancellationToken::new();
        init_data_scanner(ctx.clone(), ecstore.clone()).await;

        let deleted = wait_for_object_absence(&ecstore, bucket_name.as_str(), object_name, Duration::from_secs(12)).await;

        ctx.cancel();

        assert!(deleted, "background scanner should delete zero-day exact-key lifecycle targets");
    }

    /// Read the full object body through the regular GET path.
    async fn read_object_fully(ecstore: &Arc<ECStore>, bucket: &str, object: &str) -> Vec<u8> {
        let mut reader = ecstore
            .get_object_reader(bucket, object, None, http::HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("Failed to open object reader");
        let mut data = Vec::new();
        reader
            .stream
            .read_to_end(&mut data)
            .await
            .expect("Failed to consume object stream");
        data
    }

    /// backlog#1148 ilm-8: the full restore chain on a transitioned object.
    ///
    /// transition -> restore(days=1) -> the restored copy serves GET locally
    /// (the mock tier records zero additional `get` calls) -> the scanner's
    /// restore-expiry action (`DeleteRestoredAction`, driven directly like the
    /// ilm-2 expiry test; the evaluator mapping from a past `restore_expires`
    /// to this action is pinned by unit tests in crates/lifecycle) removes ONLY
    /// the local restored copy -> the object is still transitioned, the remote
    /// tier object is untouched (zero `remove` calls) -> GET streams from the
    /// tier again -> a second restore succeeds.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial]
    #[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-8)"]
    async fn test_restore_chain_local_read_expiry_keeps_remote_and_allows_re_restore() {
        let (_disk_paths, ecstore) = setup_test_env().await;

        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&tier_name).await;

        let bucket_name = format!("test-restore-chain-{}", &Uuid::new_v4().simple().to_string()[..8]);
        // Must live under the `test/` prefix: `set_bucket_lifecycle_transition_with_tier`
        // installs a transition rule filtered on `test/`, so any other key never
        // transitions and `wait_for_transition` below times out.
        let object_name = "test/restore/report.bin";
        // Position-dependent payload so a misaligned read is caught.
        let payload: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();

        create_test_bucket(&ecstore, bucket_name.as_str()).await;
        set_bucket_lifecycle_transition_with_tier(bucket_name.as_str(), &tier_name)
            .await
            .expect("Failed to set lifecycle configuration");
        upload_test_object(&ecstore, bucket_name.as_str(), object_name, &payload).await;

        enqueue_transition_for_existing_objects(ecstore.clone(), bucket_name.as_str())
            .await
            .expect("Failed to enqueue transition for existing objects");
        let transitioned = wait_for_transition(&ecstore, bucket_name.as_str(), object_name, TRANSITION_WAIT_TIMEOUT)
            .await
            .expect("object should transition before restore");
        let remote_object = transitioned.transitioned_object.name.clone();

        // Restore for one day.
        let restore_opts = || ObjectOptions {
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
        };
        ecstore
            .clone()
            .restore_transitioned_object(bucket_name.as_str(), object_name, &restore_opts())
            .await
            .expect("Failed to restore transitioned object");

        let restored = ecstore
            .get_object_info(bucket_name.as_str(), object_name, &ObjectOptions::default())
            .await
            .expect("Failed to load restored object info");
        assert!(restored.restore_expires.is_some(), "restore must record an expiry date");
        assert!(!restored.restore_ongoing, "restore must be complete");
        assert_eq!(
            restored.transitioned_object.status, "complete",
            "restore must not clear the transitioned state"
        );

        // GET is served from the LOCAL restored copy: the mock tier records no
        // further `get` calls.
        let tier_gets_after_restore = backend.get_count().await;
        let data = read_object_fully(&ecstore, bucket_name.as_str(), object_name).await;
        assert_eq!(data, payload, "restored GET must return the original bytes");
        assert_eq!(
            backend.get_count().await,
            tier_gets_after_restore,
            "GET of a restored object must be served locally, not from the tier"
        );

        // The scanner's restore-expiry action removes only the local restored
        // copy (same direct-drive pattern as the ilm-2 expiry test).
        let lc_event = LcEvent {
            action: IlmAction::DeleteRestoredAction,
            ..Default::default()
        };
        let bucket_incarnation_id = ecstore
            .bucket_incarnation_id(bucket_name.as_str())
            .await
            .expect("read bucket incarnation");
        expire_transitioned_object(ecstore.clone(), &restored, &lc_event, &LcEventSrc::Scanner, bucket_incarnation_id)
            .await
            .expect("restore-expiry cleanup should succeed");

        let after = ecstore
            .get_object_info(bucket_name.as_str(), object_name, &ObjectOptions::default())
            .await
            .expect("object must still exist after restored-copy cleanup");
        assert!(
            after.restore_expires.is_none(),
            "restore headers must be stripped by DeleteRestoredAction"
        );
        assert_eq!(
            after.transitioned_object.status, "complete",
            "the object must remain transitioned after restored-copy cleanup"
        );
        assert_eq!(backend.remove_count().await, 0, "restore-expiry must never remove the remote tier object");
        assert!(
            backend.contains(&remote_object).await,
            "remote tier object must survive restored-copy cleanup"
        );

        // GET now streams from the tier again...
        let tier_gets_before_remote_read = backend.get_count().await;
        let data = read_object_fully(&ecstore, bucket_name.as_str(), object_name).await;
        assert_eq!(data, payload, "post-cleanup GET must stream the original bytes from the tier");
        assert!(
            backend.get_count().await > tier_gets_before_remote_read,
            "post-cleanup GET must hit the remote tier"
        );

        // ...and the object is restorable again.
        ecstore
            .clone()
            .restore_transitioned_object(bucket_name.as_str(), object_name, &restore_opts())
            .await
            .expect("object must be restorable again after restored-copy cleanup");
        let re_restored = ecstore
            .get_object_info(bucket_name.as_str(), object_name, &ObjectOptions::default())
            .await
            .expect("Failed to load re-restored object info");
        assert!(re_restored.restore_expires.is_some(), "second restore must record an expiry");
    }

    /// backlog#1148 ilm-8: restoring a transitioned MULTIPART object (>= 3
    /// parts) must reassemble the exact part layout: part count and sizes,
    /// the multipart ETag, and byte-identical content across part boundaries.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial]
    #[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-8)"]
    async fn test_multipart_restore_preserves_parts_and_etag() {
        let (_disk_paths, ecstore) = setup_test_env().await;

        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let _backend = register_mock_tier(&tier_name).await;

        let bucket_name = format!("test-restore-mpu3-{}", &Uuid::new_v4().simple().to_string()[..8]);
        // Must live under the `test/` prefix (see the transition rule filter in
        // `set_bucket_lifecycle_transition_with_tier`) or the object never
        // transitions and `wait_for_transition` below times out.
        let object_name = "test/restore/multipart.bin";
        // Three parts: 5 MiB + 5 MiB + small tail, with position-dependent
        // bytes so any part-boundary mixup is caught.
        let part_sizes = [5 * 1024 * 1024usize, 5 * 1024 * 1024, 4096];
        let total: usize = part_sizes.iter().sum();
        let expected: Vec<u8> = (0..total).map(|i| (i % 249) as u8).collect();

        create_test_bucket(&ecstore, bucket_name.as_str()).await;
        set_bucket_lifecycle_transition_with_tier(bucket_name.as_str(), &tier_name)
            .await
            .expect("Failed to set lifecycle configuration");

        let upload = ecstore
            .new_multipart_upload(bucket_name.as_str(), object_name, &ObjectOptions::default())
            .await
            .expect("Failed to create multipart upload");
        let mut completed = Vec::new();
        let mut offset = 0usize;
        for (idx, part_size) in part_sizes.iter().enumerate() {
            let mut reader = PutObjReader::from_vec(expected[offset..offset + part_size].to_vec());
            let part = ecstore
                .put_object_part(
                    bucket_name.as_str(),
                    object_name,
                    &upload.upload_id,
                    idx + 1,
                    &mut reader,
                    &ObjectOptions::default(),
                )
                .await
                .expect("Failed to upload multipart part");
            completed.push(CompletePart {
                part_num: idx + 1,
                etag: part.etag.clone(),
                ..Default::default()
            });
            offset += part_size;
        }
        ecstore
            .clone()
            .complete_multipart_upload(bucket_name.as_str(), object_name, &upload.upload_id, completed, &ObjectOptions::default())
            .await
            .expect("Failed to complete multipart upload");

        enqueue_transition_for_existing_objects(ecstore.clone(), bucket_name.as_str())
            .await
            .expect("Failed to enqueue transition for existing objects");
        let transitioned = wait_for_transition(&ecstore, bucket_name.as_str(), object_name, TRANSITION_WAIT_TIMEOUT)
            .await
            .expect("multipart object should transition before restore");
        assert_eq!(transitioned.parts.len(), part_sizes.len());
        let etag_before = transitioned.etag.clone();
        assert!(
            etag_before.as_deref().is_some_and(|etag| etag.ends_with("-3")),
            "expected a 3-part multipart etag, got {etag_before:?}"
        );

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
            .expect("Failed to load restored multipart object info");
        assert!(restored.restore_expires.is_some());
        assert!(!restored.restore_ongoing);
        assert_eq!(restored.parts.len(), part_sizes.len(), "restore must preserve the part count");
        for (idx, part_size) in part_sizes.iter().enumerate() {
            assert_eq!(restored.parts[idx].size, *part_size, "restore must preserve the size of part {}", idx + 1);
        }
        assert_eq!(restored.etag, etag_before, "restore must preserve the multipart ETag");

        let data = read_object_fully(&ecstore, bucket_name.as_str(), object_name).await;
        assert_eq!(data.len(), expected.len(), "restored multipart read-back length mismatch");
        assert_eq!(data, expected, "restored multipart read-back must be byte-identical");
    }
}
