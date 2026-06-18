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

use super::{multipart_usecase::DefaultMultipartUsecase, object_usecase::DefaultObjectUsecase};
use crate::app::bucket_usecase::DefaultBucketUsecase;
use crate::storage::ecfs::FS;
use crate::storage::{
    StorageObjectInfo as ObjectInfo, StorageObjectOptions as ObjectOptions, StoragePutObjReader as PutObjReader,
};
use bytes::Bytes;
use futures::FutureExt;
use futures::stream;
use http::{Extensions, HeaderMap, HeaderValue, Method, Uri, header::IF_NONE_MATCH};
use rustfs_config::{ENV_OBJECT_LOCK_OPTIMIZATION_ENABLE, ENV_TEST_FORCE_IMMEDIATE_TRANSITION_ENQUEUE_TIMEOUT};
use rustfs_ecstore::{
    bucket::metadata::{BUCKET_LIFECYCLE_CONFIG, OBJECT_LOCK_CONFIG},
    bucket::metadata_sys,
    client::object_api_utils::to_s3s_etag,
    client::transition_api::{ReadCloser, ReaderImpl},
    disk::endpoint::Endpoint,
    endpoints::{EndpointServerPools, Endpoints, PoolEndpoints},
    global::GLOBAL_TierConfigMgr,
    store::ECStore,
    tier::{
        tier_config::{TierConfig, TierType},
        warm_backend::{WarmBackend, WarmBackendGetOpts},
    },
};
use rustfs_object_capacity::capacity_manager::{HybridStrategyConfig, create_isolated_manager};
use rustfs_storage_api::{
    BucketOperations, BucketOptions, ListOperations as _, MakeBucketOptions, MultipartOperations as _, ObjectIO as _,
    ObjectOperations as _,
};
use rustfs_utils::http::{SUFFIX_FORCE_DELETE, insert_header};
use s3s::{S3Request, dto::*};
use serial_test::serial;
use std::{
    collections::HashMap,
    convert::Infallible,
    env, fs as stdfs,
    io::Cursor,
    path::PathBuf,
    sync::{Arc, Once, OnceLock},
    time::Duration,
};
use tokio::fs;
use tokio::io::AsyncReadExt;
use tokio::sync::{Barrier, Mutex};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

static GLOBAL_ENV: OnceLock<(Vec<PathBuf>, Arc<ECStore>)> = OnceLock::new();
static INIT: Once = Once::new();
const TRANSITION_WAIT_TIMEOUT: Duration = Duration::from_secs(15);

fn init_tracing() {
    INIT.call_once(|| {});
}

async fn setup_test_env() -> (Vec<PathBuf>, Arc<ECStore>) {
    init_tracing();

    if let Some((paths, ecstore)) = GLOBAL_ENV.get() {
        return (paths.clone(), ecstore.clone());
    }

    let test_base_dir = format!("/tmp/rustfs_app_lifecycle_test_{}", Uuid::new_v4());
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

    let endpoint_pools = EndpointServerPools(vec![pool_endpoints]);

    rustfs_ecstore::store::init_local_disks(endpoint_pools.clone()).await.unwrap();

    let server_addr: std::net::SocketAddr = "127.0.0.1:9003".parse().unwrap();
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
    metadata_sys::init_bucket_metadata_sys(ecstore.clone(), buckets).await;

    rustfs_ecstore::bucket::lifecycle::bucket_lifecycle_ops::init_background_expiry(ecstore.clone()).await;

    let _ = GLOBAL_ENV.set((disk_paths.clone(), ecstore.clone()));

    (disk_paths, ecstore)
}

async fn create_test_bucket(ecstore: &Arc<ECStore>, bucket_name: &str) {
    (**ecstore)
        .make_bucket(
            bucket_name,
            &MakeBucketOptions {
                versioning_enabled: true,
                ..Default::default()
            },
        )
        .await
        .expect("Failed to create test bucket");
}

async fn upload_test_object(ecstore: &Arc<ECStore>, bucket: &str, object: &str, data: &[u8]) -> ObjectInfo {
    let mut reader = PutObjReader::from_vec(data.to_vec());
    (**ecstore)
        .put_object(bucket, object, &mut reader, &ObjectOptions::default())
        .await
        .expect("Failed to upload test object")
}

async fn set_bucket_lifecycle_transition_with_tier(
    bucket_name: &str,
    storage_class: &str,
) -> Result<(), Box<dyn std::error::Error>> {
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
</LifecycleConfiguration>"#
    );

    metadata_sys::update(bucket_name, BUCKET_LIFECYCLE_CONFIG, lifecycle_xml.into_bytes()).await?;
    Ok(())
}

async fn set_bucket_object_lock_enabled(bucket_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let object_lock_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ObjectLockConfiguration>
    <ObjectLockEnabled>Enabled</ObjectLockEnabled>
</ObjectLockConfiguration>"#;
    metadata_sys::update(bucket_name, OBJECT_LOCK_CONFIG, object_lock_xml.as_bytes().to_vec()).await?;
    Ok(())
}

fn expiration_lifecycle_configuration(prefix: &str) -> BucketLifecycleConfiguration {
    BucketLifecycleConfiguration {
        expiry_updated_at: None,
        rules: vec![LifecycleRule {
            status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
            abort_incomplete_multipart_upload: None,
            del_marker_expiration: None,
            expiration: Some(LifecycleExpiration {
                date: Some(Timestamp::from(
                    time::OffsetDateTime::now_utc()
                        .replace_time(time::Time::MIDNIGHT)
                        .saturating_sub(time::Duration::days(1)),
                )),
                days: None,
                expired_object_delete_marker: None,
                ..Default::default()
            }),
            filter: Some(LifecycleRuleFilter {
                and: None,
                object_size_greater_than: None,
                object_size_less_than: None,
                prefix: Some(prefix.to_string()),
                tag: None,
                ..Default::default()
            }),
            id: Some("expire-existing".to_string()),
            noncurrent_version_expiration: None,
            noncurrent_version_transitions: None,
            prefix: None,
            transitions: None,
        }],
    }
}

fn expired_object_delete_marker_lifecycle_configuration() -> BucketLifecycleConfiguration {
    BucketLifecycleConfiguration {
        expiry_updated_at: None,
        rules: vec![LifecycleRule {
            status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
            abort_incomplete_multipart_upload: None,
            del_marker_expiration: None,
            expiration: Some(LifecycleExpiration {
                expired_object_delete_marker: Some(true),
                ..Default::default()
            }),
            filter: Some(LifecycleRuleFilter {
                prefix: Some("test/".to_string()),
                ..Default::default()
            }),
            id: Some("expired-delete-marker".to_string()),
            noncurrent_version_expiration: None,
            noncurrent_version_transitions: None,
            prefix: None,
            transitions: None,
        }],
    }
}

fn del_marker_expiration_lifecycle_configuration(days: i32) -> BucketLifecycleConfiguration {
    BucketLifecycleConfiguration {
        expiry_updated_at: None,
        rules: vec![LifecycleRule {
            status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
            abort_incomplete_multipart_upload: None,
            del_marker_expiration: Some(DelMarkerExpiration { days: Some(days) }),
            expiration: Some(LifecycleExpiration {
                days: Some(30),
                ..Default::default()
            }),
            filter: Some(LifecycleRuleFilter {
                prefix: Some("test/".to_string()),
                ..Default::default()
            }),
            id: Some("del-marker-expiration".to_string()),
            noncurrent_version_expiration: None,
            noncurrent_version_transitions: None,
            prefix: None,
            transitions: None,
        }],
    }
}

#[derive(Clone, Default)]
struct MockWarmBackend {
    objects: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl MockWarmBackend {
    async fn put_bytes(&self, object: &str, bytes: Vec<u8>) -> String {
        self.objects.lock().await.insert(object.to_string(), bytes);
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
        Ok(self.put_bytes(object, bytes).await)
    }

    async fn put_with_meta(
        &self,
        object: &str,
        r: ReaderImpl,
        _length: i64,
        _meta: HashMap<String, String>,
    ) -> Result<String, std::io::Error> {
        let bytes = self.read_bytes(r).await?;
        Ok(self.put_bytes(object, bytes).await)
    }

    async fn get(&self, object: &str, _rv: &str, opts: WarmBackendGetOpts) -> Result<ReadCloser, std::io::Error> {
        let objects = self.objects.lock().await;
        let Some(bytes) = objects.get(object) else {
            return Err(std::io::Error::new(std::io::ErrorKind::NotFound, "mock object not found"));
        };

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
            ..Default::default()
        },
    );
    tier_config_mgr
        .driver_cache
        .insert(tier_name.to_string(), Box::new(backend.clone()));
    backend
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

async fn wait_for_object_absence(ecstore: &Arc<ECStore>, bucket: &str, object: &str, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        if ecstore
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .is_err()
        {
            return true;
        }

        if tokio::time::Instant::now() >= deadline {
            return false;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_delete_marker(ecstore: &Arc<ECStore>, bucket: &str, object: &str, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        if let Ok(info) = ecstore.get_object_info(bucket, object, &ObjectOptions::default()).await
            && info.delete_marker
        {
            return true;
        }

        if tokio::time::Instant::now() >= deadline {
            return false;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn build_request<T>(input: T, method: Method) -> S3Request<T> {
    S3Request {
        input,
        method,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions: Extensions::new(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

fn streaming_blob_from_bytes(data: &[u8]) -> StreamingBlob {
    let body = Bytes::copy_from_slice(data);
    StreamingBlob::wrap::<_, Infallible>(stream::once(async move { Ok(body) }))
}

async fn read_object_bytes(ecstore: &Arc<ECStore>, bucket: &str, object: &str) -> Vec<u8> {
    let mut reader = (**ecstore)
        .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
        .await
        .expect("Failed to read object");
    let mut buf = Vec::new();
    reader
        .stream
        .read_to_end(&mut buf)
        .await
        .expect("Failed to drain object reader");
    buf
}

async fn live_object_version_count(ecstore: &Arc<ECStore>, bucket: &str, object: &str) -> usize {
    let versions = ecstore
        .clone()
        .list_object_versions(bucket, object, None, None, None, 1000)
        .await
        .expect("Failed to list object versions");

    versions
        .objects
        .iter()
        .filter(|info| info.name == object && !info.delete_marker)
        .count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "requires isolated global object layer state"]
async fn put_object_if_none_match_existing_object_returns_precondition_failed() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let fs = FS::new();
    let usecase = DefaultObjectUsecase::without_context();

    let bucket = format!("test-put-if-none-match-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "test/object.txt";
    let initial_payload = b"initial conditional put payload";
    let replacement_payload = b"replacement conditional put payload";

    create_test_bucket(&ecstore, bucket.as_str()).await;

    let initial_input = PutObjectInput::builder()
        .bucket(bucket.clone())
        .key(object.to_string())
        .body(Some(streaming_blob_from_bytes(initial_payload)))
        .content_length(Some(initial_payload.len() as i64))
        .build()
        .unwrap();
    Box::pin(usecase.execute_put_object(&fs, build_request(initial_input, Method::PUT)))
        .await
        .expect("Failed to upload initial object through usecase");

    let existing_info = ecstore
        .get_object_info(bucket.as_str(), object, &ObjectOptions::default())
        .await
        .expect("Failed to fetch existing object info");
    let existing_etag = existing_info.etag.expect("existing object should have an ETag");

    let replacement_input = PutObjectInput::builder()
        .bucket(bucket.clone())
        .key(object.to_string())
        .body(Some(streaming_blob_from_bytes(replacement_payload)))
        .content_length(Some(replacement_payload.len() as i64))
        .build()
        .unwrap();
    let mut req = build_request(replacement_input, Method::PUT);
    req.headers
        .insert(IF_NONE_MATCH, HeaderValue::from_str(existing_etag.as_str()).unwrap());

    let err = Box::pin(usecase.execute_put_object(&fs, req)).await.unwrap_err();

    assert_eq!(err.code(), &s3s::S3ErrorCode::PreconditionFailed);
    assert_eq!(
        read_object_bytes(&ecstore, bucket.as_str(), object).await,
        initial_payload,
        "failed conditional PutObject must not overwrite the current object"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "requires isolated global object layer state"]
async fn copy_object_if_none_match_existing_destination_returns_precondition_failed() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultObjectUsecase::without_context();

    let src_bucket = format!("test-copy-if-none-match-src-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let dst_bucket = format!("test-copy-if-none-match-dst-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let src_object = "test/source.txt";
    let dst_object = "test/destination.txt";
    let src_payload = b"conditional copy source payload";
    let dst_payload = b"conditional copy destination payload";

    create_test_bucket(&ecstore, src_bucket.as_str()).await;
    create_test_bucket(&ecstore, dst_bucket.as_str()).await;
    let _ = upload_test_object(&ecstore, src_bucket.as_str(), src_object, src_payload).await;
    let _ = upload_test_object(&ecstore, dst_bucket.as_str(), dst_object, dst_payload).await;

    let dst_info = ecstore
        .get_object_info(dst_bucket.as_str(), dst_object, &ObjectOptions::default())
        .await
        .expect("Failed to fetch destination object info");
    let dst_etag = dst_info.etag.expect("destination object should have an ETag");

    let copy_input = CopyObjectInput::builder()
        .copy_source(CopySource::Bucket {
            bucket: src_bucket.clone().into(),
            key: src_object.to_string().into(),
            version_id: None,
        })
        .bucket(dst_bucket.clone())
        .key(dst_object.to_string())
        .build()
        .unwrap();
    let mut req = build_request(copy_input, Method::PUT);
    req.headers
        .insert(IF_NONE_MATCH, HeaderValue::from_str(dst_etag.as_str()).unwrap());

    let err = Box::pin(usecase.execute_copy_object(req)).await.unwrap_err();

    assert_eq!(err.code(), &s3s::S3ErrorCode::PreconditionFailed);
    assert_eq!(
        read_object_bytes(&ecstore, dst_bucket.as_str(), dst_object).await,
        dst_payload,
        "failed conditional CopyObject must not overwrite the current destination object"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "requires isolated global object layer state"]
async fn copy_object_allows_new_version_for_locked_destination_but_blocks_explicit_overwrite() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let fs = FS::new();
    let usecase = DefaultObjectUsecase::without_context();

    let bucket = format!("test-copy-object-lock-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let src_object = "test/source.txt";
    let dst_object = "test/destination.txt";
    let locked_payload = b"locked destination payload";
    let source_payload = b"copy source payload";

    create_test_bucket(&ecstore, bucket.as_str()).await;
    set_bucket_object_lock_enabled(bucket.as_str())
        .await
        .expect("Failed to enable object lock for bucket");
    let _ = upload_test_object(&ecstore, bucket.as_str(), src_object, source_payload).await;

    let retain_until = Timestamp::from(time::OffsetDateTime::now_utc().saturating_add(time::Duration::days(1)));
    let locked_input = PutObjectInput::builder()
        .bucket(bucket.clone())
        .key(dst_object.to_string())
        .body(Some(streaming_blob_from_bytes(locked_payload)))
        .content_length(Some(locked_payload.len() as i64))
        .object_lock_mode(Some(ObjectLockMode::from_static(ObjectLockMode::COMPLIANCE)))
        .object_lock_retain_until_date(Some(retain_until))
        .build()
        .unwrap();

    Box::pin(usecase.execute_put_object(&fs, build_request(locked_input, Method::PUT)))
        .await
        .expect("Failed to upload locked destination object");

    let locked_info = ecstore
        .get_object_info(bucket.as_str(), dst_object, &ObjectOptions::default())
        .await
        .expect("Failed to fetch locked destination object info");
    let locked_version_id = locked_info
        .version_id
        .expect("locked destination should have a version ID")
        .to_string();

    let copy_input = CopyObjectInput::builder()
        .copy_source(CopySource::Bucket {
            bucket: bucket.clone().into(),
            key: src_object.to_string().into(),
            version_id: None,
        })
        .bucket(bucket.clone())
        .key(dst_object.to_string())
        .build()
        .unwrap();

    let copy_output = Box::pin(usecase.execute_copy_object(build_request(copy_input, Method::PUT)))
        .await
        .expect("CopyObject should create a new version over a locked current version");
    let copied_version_id = copy_output
        .output
        .version_id
        .expect("versioned CopyObject should return the created version ID");

    assert_ne!(copied_version_id, locked_version_id);
    assert_eq!(read_object_bytes(&ecstore, bucket.as_str(), dst_object).await, source_payload);
    assert_eq!(live_object_version_count(&ecstore, bucket.as_str(), dst_object).await, 2);

    let explicit_overwrite_input = CopyObjectInput::builder()
        .copy_source(CopySource::Bucket {
            bucket: bucket.clone().into(),
            key: src_object.to_string().into(),
            version_id: None,
        })
        .bucket(bucket.clone())
        .key(dst_object.to_string())
        .version_id(Some(locked_version_id))
        .build()
        .unwrap();

    let err = Box::pin(usecase.execute_copy_object(build_request(explicit_overwrite_input, Method::PUT)))
        .await
        .expect_err("explicit CopyObject overwrite of a locked version should be blocked");

    assert_eq!(err.code(), &s3s::S3ErrorCode::AccessDenied);
    assert_eq!(read_object_bytes(&ecstore, bucket.as_str(), dst_object).await, source_payload);
    assert_eq!(live_object_version_count(&ecstore, bucket.as_str(), dst_object).await, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "requires isolated global object layer state"]
async fn concurrent_reverse_copy_object_does_not_deadlock_with_reader_locks() {
    temp_env::async_with_vars([(ENV_OBJECT_LOCK_OPTIMIZATION_ENABLE, Some("false"))], async {
        let (_disk_paths, ecstore) = setup_test_env().await;

        let bucket = format!("test-reverse-copy-{}", &Uuid::new_v4().simple().to_string()[..8]);
        let object_a = "test/a.bin";
        let object_b = "test/b.bin";
        let payload_a = vec![b'a'; 4 * 1024 * 1024];
        let payload_b = vec![b'b'; 4 * 1024 * 1024];

        create_test_bucket(&ecstore, bucket.as_str()).await;
        let _ = upload_test_object(&ecstore, bucket.as_str(), object_a, &payload_a).await;
        let _ = upload_test_object(&ecstore, bucket.as_str(), object_b, &payload_b).await;

        let a_to_b_input = CopyObjectInput::builder()
            .copy_source(CopySource::Bucket {
                bucket: bucket.clone().into(),
                key: object_a.to_string().into(),
                version_id: None,
            })
            .bucket(bucket.clone())
            .key(object_b.to_string())
            .build()
            .unwrap();
        let b_to_a_input = CopyObjectInput::builder()
            .copy_source(CopySource::Bucket {
                bucket: bucket.clone().into(),
                key: object_b.to_string().into(),
                version_id: None,
            })
            .bucket(bucket.clone())
            .key(object_a.to_string())
            .build()
            .unwrap();

        let start = Arc::new(Barrier::new(3));
        let start_a = start.clone();
        let a_to_b = tokio::spawn(async move {
            start_a.wait().await;
            let usecase = DefaultObjectUsecase::without_context();
            Box::pin(usecase.execute_copy_object(build_request(a_to_b_input, Method::PUT))).await
        });
        let start_b = start.clone();
        let b_to_a = tokio::spawn(async move {
            start_b.wait().await;
            let usecase = DefaultObjectUsecase::without_context();
            Box::pin(usecase.execute_copy_object(build_request(b_to_a_input, Method::PUT))).await
        });

        start.wait().await;

        let (a_to_b_result, b_to_a_result) = tokio::time::timeout(Duration::from_secs(10), async {
            let (a_to_b_result, b_to_a_result) = tokio::join!(a_to_b, b_to_a);
            (
                a_to_b_result.expect("A-to-B CopyObject task should not panic"),
                b_to_a_result.expect("B-to-A CopyObject task should not panic"),
            )
        })
        .await
        .expect("reverse CopyObject operations should not deadlock");

        a_to_b_result.expect("A-to-B CopyObject should succeed");
        b_to_a_result.expect("B-to-A CopyObject should succeed");

        let final_a = read_object_bytes(&ecstore, bucket.as_str(), object_a).await;
        let final_b = read_object_bytes(&ecstore, bucket.as_str(), object_b).await;
        assert!(
            final_a == payload_a || final_a == payload_b,
            "object A must contain a complete copied payload"
        );
        assert!(
            final_b == payload_a || final_b == payload_b,
            "object B must contain a complete copied payload"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "requires isolated global object layer state"]
async fn put_and_copy_object_transition_immediately_via_usecases() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let fs = FS::new();
    let usecase = DefaultObjectUsecase::without_context();

    let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
    let backend = register_mock_tier(&tier_name).await;

    let put_bucket = format!("test-api-put-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let put_object = "test/object.txt";
    let put_payload = b"Hello, immediate transition through put API!";

    create_test_bucket(&ecstore, put_bucket.as_str()).await;
    set_bucket_lifecycle_transition_with_tier(put_bucket.as_str(), &tier_name)
        .await
        .expect("Failed to set lifecycle configuration");

    let put_input = PutObjectInput::builder()
        .bucket(put_bucket.clone())
        .key(put_object.to_string())
        .body(Some(streaming_blob_from_bytes(put_payload)))
        .content_length(Some(put_payload.len() as i64))
        .build()
        .unwrap();

    Box::pin(usecase.execute_put_object(&fs, build_request(put_input, Method::PUT)))
        .await
        .expect("Failed to put object through usecase");

    let put_info = wait_for_transition(&ecstore, put_bucket.as_str(), put_object, TRANSITION_WAIT_TIMEOUT)
        .await
        .expect("object should transition immediately after put usecase");

    assert_eq!(put_info.transitioned_object.status, "complete");
    assert_eq!(put_info.transitioned_object.tier, tier_name);
    assert!(backend.objects.lock().await.contains_key(&put_info.transitioned_object.name));

    let src_bucket = format!("test-api-copy-src-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let dst_bucket = format!("test-api-copy-dst-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let src_object = "test/source.txt";
    let dst_object = "test/copied.txt";
    let copy_payload = b"copy object immediate transition through copy API";

    create_test_bucket(&ecstore, src_bucket.as_str()).await;
    create_test_bucket(&ecstore, dst_bucket.as_str()).await;
    set_bucket_lifecycle_transition_with_tier(dst_bucket.as_str(), &tier_name)
        .await
        .expect("Failed to set destination lifecycle configuration");
    let _ = upload_test_object(&ecstore, src_bucket.as_str(), src_object, copy_payload).await;

    let copy_input = CopyObjectInput::builder()
        .copy_source(CopySource::Bucket {
            bucket: src_bucket.clone().into(),
            key: src_object.to_string().into(),
            version_id: None,
        })
        .bucket(dst_bucket.clone())
        .key(dst_object.to_string())
        .build()
        .unwrap();

    Box::pin(usecase.execute_copy_object(build_request(copy_input, Method::PUT)))
        .await
        .expect("Failed to copy object through usecase");

    let copy_info = wait_for_transition(&ecstore, dst_bucket.as_str(), dst_object, TRANSITION_WAIT_TIMEOUT)
        .await
        .expect("copied object should transition immediately after copy usecase");

    assert_eq!(copy_info.transitioned_object.status, "complete");
    assert_eq!(copy_info.transitioned_object.tier, tier_name);
    assert!(backend.objects.lock().await.contains_key(&copy_info.transitioned_object.name));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "requires isolated global object layer state"]
async fn complete_multipart_upload_transitions_immediately_via_usecase() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultMultipartUsecase::without_context();

    let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
    let backend = register_mock_tier(&tier_name).await;

    let bucket = format!("test-api-mpu-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "test/multipart.txt";
    let payload = b"multipart immediate transition through complete API";

    create_test_bucket(&ecstore, bucket.as_str()).await;
    set_bucket_lifecycle_transition_with_tier(bucket.as_str(), &tier_name)
        .await
        .expect("Failed to set lifecycle configuration");

    let upload = ecstore
        .new_multipart_upload(bucket.as_str(), object, &ObjectOptions::default())
        .await
        .expect("Failed to create multipart upload");

    let mut reader = PutObjReader::from_vec(payload.to_vec());
    let uploaded_part = ecstore
        .put_object_part(bucket.as_str(), object, &upload.upload_id, 1, &mut reader, &ObjectOptions::default())
        .await
        .expect("Failed to upload multipart part");

    let complete_input = CompleteMultipartUploadInput::builder()
        .bucket(bucket.clone())
        .key(object.to_string())
        .upload_id(upload.upload_id.clone())
        .multipart_upload(Some(CompletedMultipartUpload {
            parts: Some(vec![CompletedPart {
                part_number: Some(1),
                e_tag: uploaded_part.etag.clone().map(|etag| to_s3s_etag(&etag)),
                ..Default::default()
            }]),
        }))
        .build()
        .unwrap();

    Box::pin(usecase.execute_complete_multipart_upload(build_request(complete_input, Method::POST)))
        .await
        .expect("Failed to complete multipart upload through usecase");

    let info = wait_for_transition(&ecstore, bucket.as_str(), object, TRANSITION_WAIT_TIMEOUT)
        .await
        .expect("multipart object should transition immediately after complete usecase");

    assert_eq!(info.transitioned_object.status, "complete");
    assert_eq!(info.transitioned_object.tier, tier_name);
    assert!(backend.objects.lock().await.contains_key(&info.transitioned_object.name));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "requires isolated global object layer state"]
async fn delete_transitioned_object_removes_remote_tier_copy_via_usecase() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultObjectUsecase::without_context();

    let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
    let backend = register_mock_tier(&tier_name).await;

    let bucket = format!("test-api-delete-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "test/object.txt";
    let payload = b"delete transitioned object through delete API";

    create_test_bucket(&ecstore, bucket.as_str()).await;
    set_bucket_lifecycle_transition_with_tier(bucket.as_str(), &tier_name)
        .await
        .expect("Failed to set lifecycle configuration");
    let _ = upload_test_object(&ecstore, bucket.as_str(), object, payload).await;

    rustfs_ecstore::bucket::lifecycle::bucket_lifecycle_ops::enqueue_transition_for_existing_objects(
        ecstore.clone(),
        bucket.as_str(),
    )
    .await
    .expect("Failed to enqueue transitioned object");

    let transitioned = wait_for_transition(&ecstore, bucket.as_str(), object, TRANSITION_WAIT_TIMEOUT)
        .await
        .expect("object should transition before delete usecase runs");
    let remote_object = transitioned.transitioned_object.name.clone();

    assert!(backend.objects.lock().await.contains_key(&remote_object));

    let mut req = build_request(
        DeleteObjectInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .build()
            .unwrap(),
        Method::DELETE,
    );
    insert_header(&mut req.headers, SUFFIX_FORCE_DELETE, "true");

    Box::pin(usecase.execute_delete_object(req))
        .await
        .expect("Failed to delete object through usecase");

    assert!(
        wait_for_object_absence(&ecstore, bucket.as_str(), object, TRANSITION_WAIT_TIMEOUT).await,
        "object should be removed from hot tier after delete usecase"
    );

    assert!(
        wait_for_remote_absence(&backend, &remote_object, TRANSITION_WAIT_TIMEOUT).await,
        "transitioned object should be removed from remote tier after delete usecase"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "requires isolated global object layer state"]
async fn lifecycle_transition_marks_dirty_disks_for_capacity_manager() {
    let (disk_paths, ecstore) = setup_test_env().await;
    let manager = create_isolated_manager(HybridStrategyConfig::default());
    let _ = manager.get_dirty_disks().await;

    let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
    let _backend = register_mock_tier(&tier_name).await;

    let bucket = format!("test-capacity-transition-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "test/object.txt";
    let payload = b"transition should mark dirty scope";

    create_test_bucket(&ecstore, bucket.as_str()).await;
    set_bucket_lifecycle_transition_with_tier(bucket.as_str(), &tier_name)
        .await
        .expect("Failed to set lifecycle configuration");
    let _ = upload_test_object(&ecstore, bucket.as_str(), object, payload).await;

    rustfs_ecstore::bucket::lifecycle::bucket_lifecycle_ops::enqueue_transition_for_existing_objects(
        ecstore.clone(),
        bucket.as_str(),
    )
    .await
    .expect("Failed to enqueue transitioned object");

    let _ = wait_for_transition(&ecstore, bucket.as_str(), object, TRANSITION_WAIT_TIMEOUT)
        .await
        .expect("object should transition before dirty scope assertion");

    let dirty_disks = manager.get_dirty_disks().await;
    assert_eq!(dirty_disks.len(), disk_paths.len());

    let actual_paths: std::collections::HashSet<_> = dirty_disks
        .into_iter()
        .map(|disk| stdfs::canonicalize(&disk.drive_path).unwrap().to_string_lossy().into_owned())
        .collect();
    let expected_paths: std::collections::HashSet<_> = disk_paths
        .iter()
        .map(|path| stdfs::canonicalize(path).unwrap().to_string_lossy().into_owned())
        .collect();
    assert_eq!(actual_paths, expected_paths);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "requires isolated global object layer state"]
async fn immediate_transition_timeout_eventually_completes_via_compensation() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
    let backend = register_mock_tier(&tier_name).await;

    let bucket = format!("test-compensation-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "test/object.txt";
    let payload = b"transition compensation should eventually complete";

    create_test_bucket(&ecstore, bucket.as_str()).await;
    set_bucket_lifecycle_transition_with_tier(bucket.as_str(), &tier_name)
        .await
        .expect("Failed to set lifecycle configuration");

    with_forced_immediate_enqueue_timeout(|| async {
        let _ = upload_test_object(&ecstore, bucket.as_str(), object, payload).await;
    })
    .await;

    let info = wait_for_transition(&ecstore, bucket.as_str(), object, TRANSITION_WAIT_TIMEOUT)
        .await
        .expect("object should eventually transition after compensation backfill");

    assert_eq!(info.transitioned_object.status, "complete");
    assert_eq!(info.transitioned_object.tier, tier_name);
    assert!(backend.objects.lock().await.contains_key(&info.transitioned_object.name));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "requires isolated global object layer state"]
async fn compensation_driven_copy_still_completes_transition() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultObjectUsecase::without_context();

    let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
    let backend = register_mock_tier(&tier_name).await;

    let src_bucket = format!("test-comp-copy-src-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let dst_bucket = format!("test-comp-copy-dst-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let src_object = "test/source.txt";
    let dst_object = "test/copied.txt";
    let payload = b"copy object should still transition after compensation";

    create_test_bucket(&ecstore, src_bucket.as_str()).await;
    create_test_bucket(&ecstore, dst_bucket.as_str()).await;
    set_bucket_lifecycle_transition_with_tier(dst_bucket.as_str(), &tier_name)
        .await
        .expect("Failed to set destination lifecycle configuration");
    let _ = upload_test_object(&ecstore, src_bucket.as_str(), src_object, payload).await;

    let copy_input = CopyObjectInput::builder()
        .copy_source(CopySource::Bucket {
            bucket: src_bucket.clone().into(),
            key: src_object.to_string().into(),
            version_id: None,
        })
        .bucket(dst_bucket.clone())
        .key(dst_object.to_string())
        .build()
        .unwrap();

    with_forced_immediate_enqueue_timeout(|| async {
        Box::pin(usecase.execute_copy_object(build_request(copy_input, Method::PUT)))
            .await
            .expect("Failed to copy object through usecase");
    })
    .await;

    let info = wait_for_transition(&ecstore, dst_bucket.as_str(), dst_object, TRANSITION_WAIT_TIMEOUT)
        .await
        .expect("copied object should eventually transition after compensation backfill");

    assert_eq!(info.transitioned_object.status, "complete");
    assert_eq!(info.transitioned_object.tier, tier_name);
    assert!(backend.objects.lock().await.contains_key(&info.transitioned_object.name));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "requires isolated global object layer state"]
async fn compensation_driven_complete_multipart_upload_still_transitions() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultMultipartUsecase::without_context();

    let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
    let backend = register_mock_tier(&tier_name).await;

    let bucket = format!("test-comp-mpu-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "test/multipart.txt";
    let payload = b"multipart should still transition after compensation";

    create_test_bucket(&ecstore, bucket.as_str()).await;
    set_bucket_lifecycle_transition_with_tier(bucket.as_str(), &tier_name)
        .await
        .expect("Failed to set lifecycle configuration");

    let upload = ecstore
        .new_multipart_upload(bucket.as_str(), object, &ObjectOptions::default())
        .await
        .expect("Failed to create multipart upload");

    let mut reader = PutObjReader::from_vec(payload.to_vec());
    let uploaded_part = ecstore
        .put_object_part(bucket.as_str(), object, &upload.upload_id, 1, &mut reader, &ObjectOptions::default())
        .await
        .expect("Failed to upload multipart part");

    let complete_input = CompleteMultipartUploadInput::builder()
        .bucket(bucket.clone())
        .key(object.to_string())
        .upload_id(upload.upload_id.clone())
        .multipart_upload(Some(CompletedMultipartUpload {
            parts: Some(vec![CompletedPart {
                part_number: Some(1),
                e_tag: uploaded_part.etag.clone().map(|etag| to_s3s_etag(&etag)),
                ..Default::default()
            }]),
        }))
        .build()
        .unwrap();

    with_forced_immediate_enqueue_timeout(|| async {
        Box::pin(usecase.execute_complete_multipart_upload(build_request(complete_input, Method::POST)))
            .await
            .expect("Failed to complete multipart upload through usecase");
    })
    .await;

    let info = wait_for_transition(&ecstore, bucket.as_str(), object, TRANSITION_WAIT_TIMEOUT)
        .await
        .expect("multipart object should eventually transition after compensation backfill");

    assert_eq!(info.transitioned_object.status, "complete");
    assert_eq!(info.transitioned_object.tier, tier_name);
    assert!(backend.objects.lock().await.contains_key(&info.transitioned_object.name));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "requires isolated global object layer state"]
async fn compensation_driven_transition_still_cleans_remote_tier_on_delete() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultObjectUsecase::without_context();

    let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
    let backend = register_mock_tier(&tier_name).await;

    let bucket = format!("test-compensation-delete-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "test/object.txt";
    let payload = b"compensation should still preserve delete cleanup";

    create_test_bucket(&ecstore, bucket.as_str()).await;
    set_bucket_lifecycle_transition_with_tier(bucket.as_str(), &tier_name)
        .await
        .expect("Failed to set lifecycle configuration");

    with_forced_immediate_enqueue_timeout(|| async {
        let _ = upload_test_object(&ecstore, bucket.as_str(), object, payload).await;
    })
    .await;

    let transitioned = wait_for_transition(&ecstore, bucket.as_str(), object, TRANSITION_WAIT_TIMEOUT)
        .await
        .expect("object should eventually transition after compensation backfill");
    let remote_object = transitioned.transitioned_object.name.clone();

    assert!(backend.objects.lock().await.contains_key(&remote_object));

    let mut req = build_request(
        DeleteObjectInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .build()
            .unwrap(),
        Method::DELETE,
    );
    insert_header(&mut req.headers, SUFFIX_FORCE_DELETE, "true");

    Box::pin(usecase.execute_delete_object(req))
        .await
        .expect("Failed to delete object through usecase after compensation-driven transition");

    assert!(
        wait_for_object_absence(&ecstore, bucket.as_str(), object, TRANSITION_WAIT_TIMEOUT).await,
        "object should be removed from hot tier after delete usecase"
    );

    assert!(
        wait_for_remote_absence(&backend, &remote_object, TRANSITION_WAIT_TIMEOUT).await,
        "transitioned object should be removed from remote tier after delete usecase"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "requires isolated global object layer state"]
async fn compensation_driven_versioned_delete_still_creates_delete_marker() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultObjectUsecase::without_context();

    let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
    let backend = register_mock_tier(&tier_name).await;

    let bucket = format!("test-comp-versioned-delete-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "test/object.txt";
    let payload = b"versioned delete should preserve transitioned remote version behind delete marker";

    create_test_bucket(&ecstore, bucket.as_str()).await;
    set_bucket_lifecycle_transition_with_tier(bucket.as_str(), &tier_name)
        .await
        .expect("Failed to set lifecycle configuration");

    with_forced_immediate_enqueue_timeout(|| async {
        let _ = upload_test_object(&ecstore, bucket.as_str(), object, payload).await;
    })
    .await;

    let transitioned = wait_for_transition(&ecstore, bucket.as_str(), object, TRANSITION_WAIT_TIMEOUT)
        .await
        .expect("object should eventually transition after compensation backfill");
    let remote_object = transitioned.transitioned_object.name.clone();

    assert!(backend.objects.lock().await.contains_key(&remote_object));

    let req = build_request(
        DeleteObjectInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .build()
            .unwrap(),
        Method::DELETE,
    );

    Box::pin(usecase.execute_delete_object(req))
        .await
        .expect("Failed to issue versioned delete after compensation-driven transition");

    assert!(
        wait_for_delete_marker(&ecstore, bucket.as_str(), object, TRANSITION_WAIT_TIMEOUT).await,
        "versioned delete should create a delete marker after compensation-driven transition"
    );
    assert!(
        backend.objects.lock().await.contains_key(&remote_object),
        "creating a delete marker should not remove the transitioned remote object version"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "requires isolated global object layer state"]
async fn compensation_driven_delete_marker_still_honors_lifecycle_cleanup() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultObjectUsecase::without_context();
    let bucket_usecase = DefaultBucketUsecase::without_context();

    let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
    let backend = register_mock_tier(&tier_name).await;

    let bucket = format!("test-comp-del-marker-cleanup-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "test/object.txt";
    let payload = b"delete marker lifecycle should still clean up after compensation-driven transition";

    create_test_bucket(&ecstore, bucket.as_str()).await;
    set_bucket_lifecycle_transition_with_tier(bucket.as_str(), &tier_name)
        .await
        .expect("Failed to set transition lifecycle configuration");

    with_forced_immediate_enqueue_timeout(|| async {
        let _ = upload_test_object(&ecstore, bucket.as_str(), object, payload).await;
    })
    .await;

    let transitioned = wait_for_transition(&ecstore, bucket.as_str(), object, TRANSITION_WAIT_TIMEOUT)
        .await
        .expect("object should eventually transition after compensation backfill");
    let remote_object = transitioned.transitioned_object.name.clone();

    assert!(backend.objects.lock().await.contains_key(&remote_object));

    let req = build_request(
        DeleteObjectInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .build()
            .unwrap(),
        Method::DELETE,
    );

    Box::pin(usecase.execute_delete_object(req))
        .await
        .expect("Failed to issue versioned delete after compensation-driven transition");

    assert!(
        wait_for_delete_marker(&ecstore, bucket.as_str(), object, TRANSITION_WAIT_TIMEOUT).await,
        "versioned delete should create a delete marker before lifecycle cleanup"
    );
    assert!(
        backend.objects.lock().await.contains_key(&remote_object),
        "delete marker creation should keep the transitioned remote object version"
    );

    let req = build_request(
        PutBucketLifecycleConfigurationInput::builder()
            .bucket(bucket.clone())
            .lifecycle_configuration(Some(expiration_lifecycle_configuration("test/")))
            .build()
            .unwrap(),
        Method::PUT,
    );
    bucket_usecase
        .execute_put_bucket_lifecycle_configuration(req)
        .await
        .expect("Failed to update lifecycle configuration for delete marker cleanup");

    assert!(
        wait_for_delete_marker(&ecstore, bucket.as_str(), object, TRANSITION_WAIT_TIMEOUT).await,
        "delete marker should remain visible after lifecycle update until cleanup completes"
    );
    assert!(
        backend.objects.lock().await.contains_key(&remote_object),
        "delete marker lifecycle cleanup should not remove the transitioned remote object version"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "requires isolated global object layer state"]
async fn put_bucket_lifecycle_configuration_expires_existing_objects() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultBucketUsecase::without_context();

    let bucket = format!("test-api-expire-existing-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "test/existing.txt";
    let payload = b"expire existing object after lifecycle update";

    create_test_bucket(&ecstore, bucket.as_str()).await;
    let _ = upload_test_object(&ecstore, bucket.as_str(), object, payload).await;

    let req = build_request(
        PutBucketLifecycleConfigurationInput::builder()
            .bucket(bucket.clone())
            .lifecycle_configuration(Some(expiration_lifecycle_configuration("test/")))
            .build()
            .unwrap(),
        Method::PUT,
    );

    usecase
        .execute_put_bucket_lifecycle_configuration(req)
        .await
        .expect("Failed to update lifecycle configuration");

    assert!(
        wait_for_delete_marker(&ecstore, bucket.as_str(), object, TRANSITION_WAIT_TIMEOUT).await,
        "existing object should be lifecycle-deleted after lifecycle update"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "requires isolated global object layer state"]
async fn put_bucket_lifecycle_configuration_allows_expired_delete_marker_on_object_lock_bucket() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultBucketUsecase::without_context();

    let bucket = format!("test-lock-expired-del-marker-{}", &Uuid::new_v4().simple().to_string()[..8]);
    create_test_bucket(&ecstore, bucket.as_str()).await;
    set_bucket_object_lock_enabled(bucket.as_str())
        .await
        .expect("Failed to enable object lock for bucket");

    let req = build_request(
        PutBucketLifecycleConfigurationInput::builder()
            .bucket(bucket.clone())
            .lifecycle_configuration(Some(expired_object_delete_marker_lifecycle_configuration()))
            .build()
            .unwrap(),
        Method::PUT,
    );

    usecase
        .execute_put_bucket_lifecycle_configuration(req)
        .await
        .expect("ExpiredObjectDeleteMarker should be accepted on object-lock bucket");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "requires isolated global object layer state"]
async fn put_bucket_lifecycle_configuration_rejects_del_marker_expiration_on_object_lock_bucket() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultBucketUsecase::without_context();

    let bucket = format!("test-lock-del-marker-exp-{}", &Uuid::new_v4().simple().to_string()[..8]);
    create_test_bucket(&ecstore, bucket.as_str()).await;
    set_bucket_object_lock_enabled(bucket.as_str())
        .await
        .expect("Failed to enable object lock for bucket");

    let req = build_request(
        PutBucketLifecycleConfigurationInput::builder()
            .bucket(bucket.clone())
            .lifecycle_configuration(Some(del_marker_expiration_lifecycle_configuration(1)))
            .build()
            .unwrap(),
        Method::PUT,
    );

    let err = usecase
        .execute_put_bucket_lifecycle_configuration(req)
        .await
        .expect_err("DelMarkerExpiration must be rejected on object-lock bucket");
    assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidArgument);
    let message = err.message().unwrap_or_default();
    assert!(
        message.contains(
            "ExpiredObjectAllVersions element and DelMarkerExpiration action cannot be used on an object locked bucket"
        ),
        "unexpected error message: {message}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "requires isolated global object layer state"]
async fn put_bucket_lifecycle_configuration_rejects_zero_day_del_marker_expiration_on_object_lock_bucket() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultBucketUsecase::without_context();

    let bucket = format!("test-lock-del-marker-exp0-{}", &Uuid::new_v4().simple().to_string()[..8]);
    create_test_bucket(&ecstore, bucket.as_str()).await;
    set_bucket_object_lock_enabled(bucket.as_str())
        .await
        .expect("Failed to enable object lock for bucket");

    let req = build_request(
        PutBucketLifecycleConfigurationInput::builder()
            .bucket(bucket.clone())
            .lifecycle_configuration(Some(del_marker_expiration_lifecycle_configuration(0)))
            .build()
            .unwrap(),
        Method::PUT,
    );

    let err = usecase
        .execute_put_bucket_lifecycle_configuration(req)
        .await
        .expect_err("DelMarkerExpiration with zero days must be rejected on object-lock bucket");
    assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidArgument);
    let message = err.message().unwrap_or_default();
    assert!(
        message.contains(
            "ExpiredObjectAllVersions element and DelMarkerExpiration action cannot be used on an object locked bucket"
        ),
        "unexpected error message: {message}"
    );
}
