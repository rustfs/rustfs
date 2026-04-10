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

use super::*;
use crate::storage::concurrency::ConcurrencyManager;
use futures::StreamExt;
use http::{Extensions, HeaderMap, Method, Uri};
use rustfs_ecstore::store_api::GetObjectChunkPath;
use rustfs_ecstore::{
    bucket::metadata_sys,
    disk::endpoint::Endpoint,
    endpoints::{EndpointServerPools, Endpoints, PoolEndpoints},
    store::ECStore,
    store_api::{
        BucketOperations, ChunkNativePutData, CompletePart, MakeBucketOptions, MultipartOperations, ObjectIO, ObjectOptions,
    },
};
use rustfs_object_io::get::{GetObjectBodySource, GetObjectReadSetup};
use serial_test::serial;
use std::{
    path::PathBuf,
    sync::{Arc, Once, OnceLock},
};
use tokio::fs;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

static DIRECT_CHUNK_TEST_ENV: OnceLock<(Vec<PathBuf>, Arc<ECStore>)> = OnceLock::new();
static DIRECT_CHUNK_MULTI_DISK_TEST_ENV: OnceLock<(Vec<PathBuf>, Arc<ECStore>)> = OnceLock::new();
static DIRECT_CHUNK_TEST_INIT: Once = Once::new();

fn init_direct_chunk_test_tracing() {
    DIRECT_CHUNK_TEST_INIT.call_once(|| {});
}

async fn setup_direct_chunk_test_env() -> (Vec<PathBuf>, Arc<ECStore>) {
    init_direct_chunk_test_tracing();

    if let Some((paths, store)) = DIRECT_CHUNK_TEST_ENV.get() {
        return (paths.clone(), store.clone());
    }

    let test_base_dir = format!("/tmp/rustfs_app_chunk_direct_test_{}", Uuid::new_v4());
    let temp_dir = PathBuf::from(&test_base_dir);
    if temp_dir.exists() {
        fs::remove_dir_all(&temp_dir).await.ok();
    }
    fs::create_dir_all(&temp_dir).await.unwrap();

    let disk_path = temp_dir.join("disk1");
    fs::create_dir_all(&disk_path).await.unwrap();

    let mut endpoint = Endpoint::try_from(disk_path.to_str().unwrap()).unwrap();
    endpoint.set_pool_index(0);
    endpoint.set_set_index(0);
    endpoint.set_disk_index(0);

    let pool_endpoints = PoolEndpoints {
        legacy: false,
        set_count: 1,
        drives_per_set: 1,
        endpoints: Endpoints::from(vec![endpoint]),
        cmd_line: "test".to_string(),
        platform: format!("OS: {} | Arch: {}", std::env::consts::OS, std::env::consts::ARCH),
    };

    let endpoint_pools = EndpointServerPools(vec![pool_endpoints]);

    rustfs_ecstore::store::init_local_disks(endpoint_pools.clone()).await.unwrap();

    let server_addr: std::net::SocketAddr = "127.0.0.1:9013".parse().unwrap();
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

    let _ = DIRECT_CHUNK_TEST_ENV.set((vec![disk_path], ecstore.clone()));

    (DIRECT_CHUNK_TEST_ENV.get().unwrap().0.clone(), ecstore)
}

async fn setup_direct_chunk_multi_disk_test_env() -> (Vec<PathBuf>, Arc<ECStore>) {
    init_direct_chunk_test_tracing();

    if let Some((paths, store)) = DIRECT_CHUNK_MULTI_DISK_TEST_ENV.get() {
        return (paths.clone(), store.clone());
    }

    let test_base_dir = format!("/tmp/rustfs_app_chunk_direct_multi_test_{}", Uuid::new_v4());
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

    let server_addr: std::net::SocketAddr = "127.0.0.1:9014".parse().unwrap();
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

    let _ = DIRECT_CHUNK_MULTI_DISK_TEST_ENV.set((disk_paths.clone(), ecstore.clone()));

    (DIRECT_CHUNK_MULTI_DISK_TEST_ENV.get().unwrap().0.clone(), ecstore)
}

async fn create_direct_chunk_test_bucket(ecstore: &Arc<ECStore>, bucket_name: &str) {
    (**ecstore)
        .make_bucket(
            bucket_name,
            &MakeBucketOptions {
                versioning_enabled: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
}

async fn create_direct_chunk_test_multipart_object(
    ecstore: &Arc<ECStore>,
    bucket: &str,
    key: &str,
    parts: Vec<Vec<u8>>,
) -> Vec<Vec<u8>> {
    let upload = ecstore
        .new_multipart_upload(bucket, key, &ObjectOptions::default())
        .await
        .unwrap();

    let mut completed_parts = Vec::new();
    for (idx, part) in parts.iter().enumerate() {
        let mut reader = ChunkNativePutData::from_vec(part.clone());
        let part_info = ecstore
            .put_object_part(bucket, key, &upload.upload_id, idx + 1, &mut reader, &ObjectOptions::default())
            .await
            .unwrap();
        completed_parts.push(CompletePart {
            part_num: idx + 1,
            etag: part_info.etag,
            ..Default::default()
        });
    }

    ecstore
        .clone()
        .complete_multipart_upload(bucket, key, &upload.upload_id, completed_parts, &ObjectOptions::default())
        .await
        .unwrap();

    parts
}

fn find_part_file(root: &Path, part_name: &str) -> Option<PathBuf> {
    let entries = std::fs::read_dir(root).ok()?;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if let Some(found) = find_part_file(&path, part_name) {
                return Some(found);
            }
            continue;
        }

        if path.file_name().and_then(|name| name.to_str()) == Some(part_name) {
            return Some(path);
        }
    }

    None
}

fn find_part_files(root: &Path, part_name: &str, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            find_part_files(&path, part_name, out);
            continue;
        }

        if path.file_name().and_then(|name| name.to_str()) == Some(part_name) {
            out.push(path);
        }
    }
}

async fn remove_part_files(root: &Path, part_name: &str) -> Vec<(PathBuf, Vec<u8>)> {
    let mut paths = Vec::new();
    find_part_files(root, part_name, &mut paths);

    let mut removed = Vec::with_capacity(paths.len());
    for path in paths {
        let content = fs::read(&path).await.expect("read part file before removal");
        fs::remove_file(&path).await.expect("remove part file");
        removed.push((path, content));
    }

    removed
}

async fn restore_part_files(files: Vec<(PathBuf, Vec<u8>)>) {
    for (path, content) in files {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.expect("ensure parent for part restore");
        }
        fs::write(&path, content).await.expect("restore part file");
    }
}

async fn select_reconstructed_chunk_read(
    disk_paths: &[PathBuf],
    bucket: &str,
    key: &str,
    missing_part_name: &str,
    ecstore: &Arc<ECStore>,
    manager: &ConcurrencyManager,
    request_context: &GetObjectRequestContext,
) -> GetObjectReadSetup {
    for disk_path in disk_paths {
        let object_root = disk_path.join(bucket).join(key);
        let removed_parts = remove_part_files(&object_root, missing_part_name).await;
        if removed_parts.is_empty() {
            continue;
        }

        let candidate =
            get_object_zero_copy::prepare_get_object_chunk_read(request_context, ecstore, manager, std::time::Instant::now())
                .await
                .unwrap()
                .expect("expected chunk fast path");

        let is_reconstructed = matches!(
            &candidate.body_source,
            GetObjectBodySource::Chunk {
                path: GetObjectChunkPath::Direct,
                copy_mode: rustfs_io_metrics::CopyMode::Reconstructed,
                ..
            }
        );
        if is_reconstructed {
            return candidate;
        }

        restore_part_files(removed_parts).await;
    }

    panic!("expected reconstructed chunk path after removing one disk shard copy of {missing_part_name}");
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

async fn execute_get_object_with_fast_path_enabled(
    usecase: &DefaultObjectUsecase,
    req: S3Request<GetObjectInput>,
) -> S3Result<S3Response<GetObjectOutput>> {
    temp_env::with_var(rustfs_config::ENV_OBJECT_GET_CHUNK_FAST_PATH_ENABLE, Some("true"), || async move {
        usecase.execute_get_object(req).await
    })
    .await
}

#[tokio::test]
async fn execute_get_object_rejects_zero_part_number() {
    let input = GetObjectInput::builder()
        .bucket("test-bucket".to_string())
        .key("test-key".to_string())
        .part_number(Some(0))
        .build()
        .unwrap();

    let req = build_request(input, Method::GET);
    let usecase = DefaultObjectUsecase::without_context();

    let err = usecase.execute_get_object(req).await.unwrap_err();
    assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "requires isolated global object layer state"]
async fn prepare_get_object_chunk_read_marks_direct_path_for_single_disk_store() {
    let (_disk_paths, ecstore) = setup_direct_chunk_test_env().await;
    let bucket = format!("direct-chunk-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let key = "test/object.bin";
    let payload = vec![1u8; 128 * 1024];

    create_direct_chunk_test_bucket(&ecstore, &bucket).await;

    let mut reader = ChunkNativePutData::from_vec(payload.clone());
    ecstore
        .put_object(&bucket, key, &mut reader, &ObjectOptions::default())
        .await
        .unwrap();

    let input = GetObjectInput::builder()
        .bucket(bucket.clone())
        .key(key.to_string())
        .build()
        .unwrap();
    let req = build_request(input, Method::GET);

    let request_context = prepare_get_object_request_context(&req).await.unwrap();
    let manager = get_concurrency_manager();

    let read_setup =
        get_object_zero_copy::prepare_get_object_chunk_read(&request_context, &ecstore, manager, std::time::Instant::now())
            .await
            .unwrap()
            .expect("expected chunk fast path");

    match read_setup.body_source {
        GetObjectBodySource::Chunk { path, copy_mode, .. } => {
            assert!(matches!(path, GetObjectChunkPath::Direct), "expected direct chunk path");
            assert_eq!(copy_mode, rustfs_io_metrics::CopyMode::TrueZeroCopy);
        }
        GetObjectBodySource::Reader(_) => panic!("expected chunk body source"),
    }

    let usecase = DefaultObjectUsecase::without_context();
    let response = execute_get_object_with_fast_path_enabled(&usecase, req).await.unwrap();
    assert_eq!(response.output.content_length, Some(payload.len() as i64));
    let mut body = response.output.body.expect("expected body");
    let mut collected = Vec::new();
    while let Some(chunk) = body.next().await {
        collected.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(collected, payload);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "requires isolated global object layer state"]
async fn prepare_get_object_chunk_read_falls_back_to_legacy_when_chunk_bridge_fails() {
    let (disk_paths, ecstore) = setup_direct_chunk_test_env().await;
    let bucket = format!("direct-chunk-fallback-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let key = "test/fallback.bin";
    let payload = vec![3u8; 128 * 1024];

    create_direct_chunk_test_bucket(&ecstore, &bucket).await;

    let mut reader = ChunkNativePutData::from_vec(payload);
    ecstore
        .put_object(&bucket, key, &mut reader, &ObjectOptions::default())
        .await
        .unwrap();

    let object_root = disk_paths[0].join(&bucket).join("test").join("fallback.bin");
    let missing_part = find_part_file(&object_root, "part.1").expect("part file on disk");
    fs::remove_file(&missing_part).await.expect("remove single-disk part file");

    let input = GetObjectInput::builder()
        .bucket(bucket.clone())
        .key(key.to_string())
        .build()
        .unwrap();
    let req = build_request(input, Method::GET);
    let request_context = prepare_get_object_request_context(&req).await.unwrap();
    let manager = get_concurrency_manager();

    let read_setup =
        get_object_zero_copy::prepare_get_object_chunk_read(&request_context, &ecstore, manager, std::time::Instant::now())
            .await
            .unwrap();

    assert!(read_setup.is_none(), "chunk bridge failure should fall back to legacy reader");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "requires isolated global object layer state"]
async fn execute_get_object_range_marks_direct_path_for_single_disk_store() {
    let (_disk_paths, ecstore) = setup_direct_chunk_test_env().await;
    let bucket = format!("direct-range-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let key = "test/range.bin";
    let payload = vec![2u8; 128 * 1024];

    create_direct_chunk_test_bucket(&ecstore, &bucket).await;

    let mut reader = ChunkNativePutData::from_vec(payload.clone());
    ecstore
        .put_object(&bucket, key, &mut reader, &ObjectOptions::default())
        .await
        .unwrap();

    let input = GetObjectInput::builder()
        .bucket(bucket.clone())
        .key(key.to_string())
        .range(Some(Range::Int {
            first: 0,
            last: Some(63 * 1024),
        }))
        .build()
        .unwrap();
    let req = build_request(input, Method::GET);

    let request_context = prepare_get_object_request_context(&req).await.unwrap();
    let manager = get_concurrency_manager();

    let read_setup =
        get_object_zero_copy::prepare_get_object_chunk_read(&request_context, &ecstore, manager, std::time::Instant::now())
            .await
            .unwrap()
            .expect("expected chunk fast path");

    match read_setup.body_source {
        GetObjectBodySource::Chunk { path, .. } => {
            assert!(matches!(path, GetObjectChunkPath::Direct), "expected direct chunk path")
        }
        GetObjectBodySource::Reader(_) => panic!("expected chunk body source"),
    }

    let usecase = DefaultObjectUsecase::without_context();
    let response = execute_get_object_with_fast_path_enabled(&usecase, req).await.unwrap();
    assert_eq!(response.output.content_length, Some(63 * 1024 + 1));
    let mut body = response.output.body.expect("expected body");
    let mut collected = Vec::new();
    while let Some(chunk) = body.next().await {
        collected.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(collected, payload[..(63 * 1024 + 1)].to_vec());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "requires isolated global object layer state"]
async fn execute_get_object_range_marks_direct_path_for_multi_disk_store_without_rebuild() {
    let (_disk_paths, ecstore) = setup_direct_chunk_multi_disk_test_env().await;
    let bucket = format!("direct-multi-range-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let key = "test/multi-range.bin";
    let payload_len = 3 * 1024 * 1024 + 137;
    let payload: Vec<u8> = (0..payload_len).map(|idx| (idx % 251) as u8).collect();

    create_direct_chunk_test_bucket(&ecstore, &bucket).await;

    let mut reader = ChunkNativePutData::from_vec(payload.clone());
    let put_info = ecstore
        .put_object(&bucket, key, &mut reader, &ObjectOptions::default())
        .await
        .unwrap();
    assert!(
        put_info.data_blocks > 1,
        "expected multi-data-shard object, got {}+{}",
        put_info.data_blocks,
        put_info.parity_blocks
    );

    let range_start = 123_457_u64;
    let range_end = 2 * 1024 * 1024 + 33_333_u64;
    let input = GetObjectInput::builder()
        .bucket(bucket.clone())
        .key(key.to_string())
        .range(Some(Range::Int {
            first: range_start,
            last: Some(range_end),
        }))
        .build()
        .unwrap();
    let req = build_request(input, Method::GET);

    let request_context = prepare_get_object_request_context(&req).await.unwrap();
    let manager = get_concurrency_manager();

    let read_setup =
        get_object_zero_copy::prepare_get_object_chunk_read(&request_context, &ecstore, manager, std::time::Instant::now())
            .await
            .unwrap()
            .expect("expected chunk fast path");

    match read_setup.body_source {
        GetObjectBodySource::Chunk { path, copy_mode, .. } => {
            assert!(matches!(path, GetObjectChunkPath::Direct), "expected direct chunk path");
            #[cfg(unix)]
            assert_eq!(
                copy_mode,
                rustfs_io_metrics::CopyMode::TrueZeroCopy,
                "multi-data-shard direct path should preserve shard mmap slices without assembly copies"
            );
            #[cfg(not(unix))]
            assert_eq!(
                copy_mode,
                rustfs_io_metrics::CopyMode::SharedBytes,
                "multi-data-shard direct path should avoid assembly copies even without mmap"
            );
        }
        GetObjectBodySource::Reader(_) => panic!("expected chunk body source"),
    }

    let usecase = DefaultObjectUsecase::without_context();
    let response = execute_get_object_with_fast_path_enabled(&usecase, req).await.unwrap();
    assert_eq!(response.output.content_length, Some((range_end - range_start + 1) as i64));
    let mut body = response.output.body.expect("expected body");
    let mut collected = Vec::new();
    while let Some(chunk) = body.next().await {
        collected.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(collected, payload[range_start as usize..=range_end as usize].to_vec());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "requires isolated global object layer state"]
async fn execute_get_object_range_marks_reconstructed_path_for_multi_disk_store_with_missing_shard() {
    let (disk_paths, ecstore) = setup_direct_chunk_multi_disk_test_env().await;
    let bucket = format!("direct-multi-reconstructed-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let key = "test/multi-reconstructed.bin";
    let payload_len = 3 * 1024 * 1024 + 137;
    let payload: Vec<u8> = (0..payload_len).map(|idx| (idx % 251) as u8).collect();

    create_direct_chunk_test_bucket(&ecstore, &bucket).await;

    let mut reader = ChunkNativePutData::from_vec(payload.clone());
    let put_info = ecstore
        .put_object(&bucket, key, &mut reader, &ObjectOptions::default())
        .await
        .unwrap();
    assert!(put_info.data_blocks > 1, "expected multi-data-shard object");

    let object_root = disk_paths[0].join(&bucket).join("test").join("multi-reconstructed.bin");
    let missing_part = find_part_file(&object_root, "part.1").expect("part file on first disk");
    fs::remove_file(&missing_part).await.expect("remove first shard part");

    let range_start = 123_457_u64;
    let range_end = 2 * 1024 * 1024 + 33_333_u64;
    let input = GetObjectInput::builder()
        .bucket(bucket.clone())
        .key(key.to_string())
        .range(Some(Range::Int {
            first: range_start,
            last: Some(range_end),
        }))
        .build()
        .unwrap();
    let req = build_request(input, Method::GET);

    let request_context = prepare_get_object_request_context(&req).await.unwrap();
    let manager = get_concurrency_manager();

    let read_setup =
        get_object_zero_copy::prepare_get_object_chunk_read(&request_context, &ecstore, manager, std::time::Instant::now())
            .await
            .unwrap()
            .expect("expected chunk fast path");

    match read_setup.body_source {
        GetObjectBodySource::Chunk { path, copy_mode, .. } => {
            assert!(matches!(path, GetObjectChunkPath::Direct), "expected direct chunk path");
            assert_eq!(
                copy_mode,
                rustfs_io_metrics::CopyMode::Reconstructed,
                "missing data shard should trigger reconstructed chunk path"
            );
        }
        GetObjectBodySource::Reader(_) => panic!("expected chunk body source"),
    }

    let usecase = DefaultObjectUsecase::without_context();
    let response = execute_get_object_with_fast_path_enabled(&usecase, req).await.unwrap();
    assert_eq!(response.output.content_length, Some((range_end - range_start + 1) as i64));
    let mut body = response.output.body.expect("expected body");
    let mut collected = Vec::new();
    while let Some(chunk) = body.next().await {
        collected.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(collected, payload[range_start as usize..=range_end as usize].to_vec());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "requires isolated global object layer state"]
async fn execute_get_object_part_number_marks_direct_path_for_single_disk_store() {
    let (_disk_paths, ecstore) = setup_direct_chunk_test_env().await;
    let bucket = format!("direct-part-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let key = "test/multipart.bin";
    let part_one = vec![11u8; 5 * 1024 * 1024];
    let part_two = vec![22u8; 5 * 1024 * 1024];

    create_direct_chunk_test_bucket(&ecstore, &bucket).await;
    let parts = create_direct_chunk_test_multipart_object(&ecstore, &bucket, key, vec![part_one.clone(), part_two.clone()]).await;

    let input = GetObjectInput::builder()
        .bucket(bucket.clone())
        .key(key.to_string())
        .part_number(Some(1))
        .build()
        .unwrap();
    let req = build_request(input, Method::GET);

    let request_context = prepare_get_object_request_context(&req).await.unwrap();
    let manager = get_concurrency_manager();

    let read_setup =
        get_object_zero_copy::prepare_get_object_chunk_read(&request_context, &ecstore, manager, std::time::Instant::now())
            .await
            .unwrap()
            .expect("expected chunk fast path");

    match read_setup.body_source {
        GetObjectBodySource::Chunk { path, copy_mode, .. } => {
            assert!(matches!(path, GetObjectChunkPath::Direct), "expected direct chunk path");
            assert_eq!(copy_mode, rustfs_io_metrics::CopyMode::TrueZeroCopy);
        }
        GetObjectBodySource::Reader(_) => panic!("expected chunk body source"),
    }

    let usecase = DefaultObjectUsecase::without_context();
    let response = execute_get_object_with_fast_path_enabled(&usecase, req).await.unwrap();
    assert_eq!(response.output.content_length, Some(parts[0].len() as i64));
    let mut body = response.output.body.expect("expected body");
    let mut collected = Vec::new();
    while let Some(chunk) = body.next().await {
        collected.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(collected, parts[0]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "requires isolated global object layer state"]
async fn execute_get_object_whole_multipart_marks_direct_path_for_single_disk_store() {
    let (_disk_paths, ecstore) = setup_direct_chunk_test_env().await;
    let bucket = format!("direct-whole-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let key = "test/multipart-whole.bin";
    let part_one = vec![11u8; 5 * 1024 * 1024];
    let part_two = vec![22u8; 5 * 1024 * 1024 + 321];

    create_direct_chunk_test_bucket(&ecstore, &bucket).await;
    let parts = create_direct_chunk_test_multipart_object(&ecstore, &bucket, key, vec![part_one.clone(), part_two.clone()]).await;

    let input = GetObjectInput::builder()
        .bucket(bucket.clone())
        .key(key.to_string())
        .build()
        .unwrap();
    let req = build_request(input, Method::GET);

    let request_context = prepare_get_object_request_context(&req).await.unwrap();
    let manager = get_concurrency_manager();

    let read_setup =
        get_object_zero_copy::prepare_get_object_chunk_read(&request_context, &ecstore, manager, std::time::Instant::now())
            .await
            .unwrap()
            .expect("expected chunk fast path");

    match read_setup.body_source {
        GetObjectBodySource::Chunk { path, copy_mode, .. } => {
            assert!(matches!(path, GetObjectChunkPath::Direct), "expected direct chunk path");
            assert_eq!(copy_mode, rustfs_io_metrics::CopyMode::TrueZeroCopy);
        }
        GetObjectBodySource::Reader(_) => panic!("expected chunk body source"),
    }

    let usecase = DefaultObjectUsecase::without_context();
    let response = execute_get_object_with_fast_path_enabled(&usecase, req).await.unwrap();
    assert_eq!(response.output.content_length, Some(parts.iter().map(|part| part.len() as i64).sum()));
    let mut body = response.output.body.expect("expected body");
    let mut collected = Vec::new();
    while let Some(chunk) = body.next().await {
        collected.extend_from_slice(&chunk.unwrap());
    }

    let mut expected = Vec::with_capacity(parts.iter().map(Vec::len).sum());
    for part in &parts {
        expected.extend_from_slice(part);
    }
    assert_eq!(collected, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "requires isolated global object layer state"]
async fn execute_get_object_part_number_marks_reconstructed_path_for_multi_disk_store_with_missing_shard() {
    let (disk_paths, ecstore) = setup_direct_chunk_multi_disk_test_env().await;
    let bucket = format!("direct-part-reconstructed-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let key = "test/multipart-reconstructed.bin";
    let part_one: Vec<u8> = (0..(5 * 1024 * 1024)).map(|idx| (idx % 251) as u8).collect();
    let part_two: Vec<u8> = (0..(5 * 1024 * 1024 + 137)).map(|idx| ((idx + 11) % 251) as u8).collect();
    let part_three: Vec<u8> = (0..(1024 * 1024 + 77)).map(|idx| ((idx + 29) % 251) as u8).collect();

    create_direct_chunk_test_bucket(&ecstore, &bucket).await;
    let parts =
        create_direct_chunk_test_multipart_object(&ecstore, &bucket, key, vec![part_one.clone(), part_two.clone(), part_three])
            .await;

    let input = GetObjectInput::builder()
        .bucket(bucket.clone())
        .key(key.to_string())
        .part_number(Some(1))
        .build()
        .unwrap();
    let req = build_request(input, Method::GET);

    let request_context = prepare_get_object_request_context(&req).await.unwrap();
    let manager = get_concurrency_manager();
    let read_setup =
        select_reconstructed_chunk_read(&disk_paths, &bucket, key, "part.1", &ecstore, manager, &request_context).await;

    match read_setup.body_source {
        GetObjectBodySource::Chunk { path, copy_mode, .. } => {
            assert!(matches!(path, GetObjectChunkPath::Direct), "expected direct chunk path");
            assert_eq!(
                copy_mode,
                rustfs_io_metrics::CopyMode::Reconstructed,
                "missing data shard in multipart part should trigger reconstructed chunk path"
            );
        }
        GetObjectBodySource::Reader(_) => panic!("expected chunk body source"),
    }

    let usecase = DefaultObjectUsecase::without_context();
    let response = execute_get_object_with_fast_path_enabled(&usecase, req).await.unwrap();
    assert_eq!(response.output.content_length, Some(parts[0].len() as i64));
    let mut body = response.output.body.expect("expected body");
    let mut collected = Vec::new();
    while let Some(chunk) = body.next().await {
        collected.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(collected, parts[0]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "requires isolated global object layer state"]
async fn execute_get_object_part_number_marks_reconstructed_path_for_second_multipart_part_with_missing_shard() {
    let (disk_paths, ecstore) = setup_direct_chunk_multi_disk_test_env().await;
    let bucket = format!("direct-part2-reconstructed-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let key = "test/multipart-reconstructed-second-part.bin";
    let part_one: Vec<u8> = (0..(5 * 1024 * 1024)).map(|idx| (idx % 251) as u8).collect();
    let part_two: Vec<u8> = (0..(5 * 1024 * 1024 + 137)).map(|idx| ((idx + 11) % 251) as u8).collect();
    let part_three: Vec<u8> = (0..(1024 * 1024 + 77)).map(|idx| ((idx + 29) % 251) as u8).collect();

    create_direct_chunk_test_bucket(&ecstore, &bucket).await;
    let parts =
        create_direct_chunk_test_multipart_object(&ecstore, &bucket, key, vec![part_one, part_two.clone(), part_three]).await;

    let input = GetObjectInput::builder()
        .bucket(bucket.clone())
        .key(key.to_string())
        .part_number(Some(2))
        .build()
        .unwrap();
    let req = build_request(input, Method::GET);

    let request_context = prepare_get_object_request_context(&req).await.unwrap();
    let manager = get_concurrency_manager();
    let read_setup =
        select_reconstructed_chunk_read(&disk_paths, &bucket, key, "part.2", &ecstore, manager, &request_context).await;

    match read_setup.body_source {
        GetObjectBodySource::Chunk { path, copy_mode, .. } => {
            assert!(matches!(path, GetObjectChunkPath::Direct), "expected direct chunk path");
            assert_eq!(
                copy_mode,
                rustfs_io_metrics::CopyMode::Reconstructed,
                "missing data shard in multipart part 2 should trigger reconstructed chunk path"
            );
        }
        GetObjectBodySource::Reader(_) => panic!("expected chunk body source"),
    }

    let usecase = DefaultObjectUsecase::without_context();
    let response = execute_get_object_with_fast_path_enabled(&usecase, req).await.unwrap();
    assert_eq!(response.output.content_length, Some(parts[1].len() as i64));
    let mut body = response.output.body.expect("expected body");
    let mut collected = Vec::new();
    while let Some(chunk) = body.next().await {
        collected.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(collected, parts[1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "requires isolated global object layer state"]
async fn execute_get_object_part_number_marks_reconstructed_path_for_final_multipart_part_with_missing_shard() {
    let (disk_paths, ecstore) = setup_direct_chunk_multi_disk_test_env().await;
    let bucket = format!("direct-part3-reconstructed-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let key = "test/multipart-reconstructed-final-part.bin";
    let part_one: Vec<u8> = (0..(5 * 1024 * 1024)).map(|idx| (idx % 251) as u8).collect();
    let part_two: Vec<u8> = (0..(5 * 1024 * 1024 + 137)).map(|idx| ((idx + 11) % 251) as u8).collect();
    let part_three: Vec<u8> = (0..(1024 * 1024 + 77)).map(|idx| ((idx + 29) % 251) as u8).collect();

    create_direct_chunk_test_bucket(&ecstore, &bucket).await;
    let parts =
        create_direct_chunk_test_multipart_object(&ecstore, &bucket, key, vec![part_one, part_two, part_three.clone()]).await;

    let input = GetObjectInput::builder()
        .bucket(bucket.clone())
        .key(key.to_string())
        .part_number(Some(3))
        .build()
        .unwrap();
    let req = build_request(input, Method::GET);

    let request_context = prepare_get_object_request_context(&req).await.unwrap();
    let manager = get_concurrency_manager();
    let read_setup =
        select_reconstructed_chunk_read(&disk_paths, &bucket, key, "part.3", &ecstore, manager, &request_context).await;

    match read_setup.body_source {
        GetObjectBodySource::Chunk { path, copy_mode, .. } => {
            assert!(matches!(path, GetObjectChunkPath::Direct), "expected direct chunk path");
            assert_eq!(
                copy_mode,
                rustfs_io_metrics::CopyMode::Reconstructed,
                "missing data shard in the final multipart part should trigger reconstructed chunk path"
            );
        }
        GetObjectBodySource::Reader(_) => panic!("expected chunk body source"),
    }

    let usecase = DefaultObjectUsecase::without_context();
    let response = execute_get_object_with_fast_path_enabled(&usecase, req).await.unwrap();
    assert_eq!(response.output.content_length, Some(parts[2].len() as i64));
    let mut body = response.output.body.expect("expected body");
    let mut collected = Vec::new();
    while let Some(chunk) = body.next().await {
        collected.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(collected, part_three);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "requires isolated global object layer state"]
async fn execute_get_object_whole_multipart_marks_reconstructed_path_for_missing_middle_part_shard() {
    let (disk_paths, ecstore) = setup_direct_chunk_multi_disk_test_env().await;
    let bucket = format!("direct-whole-multipart-reconstructed-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let key = "test/multipart-whole-reconstructed.bin";
    let part_one: Vec<u8> = (0..(5 * 1024 * 1024)).map(|idx| (idx % 251) as u8).collect();
    let part_two: Vec<u8> = (0..(5 * 1024 * 1024 + 257)).map(|idx| ((idx + 17) % 251) as u8).collect();
    let part_three: Vec<u8> = (0..(1024 * 1024 + 211)).map(|idx| ((idx + 33) % 251) as u8).collect();

    create_direct_chunk_test_bucket(&ecstore, &bucket).await;
    let parts = create_direct_chunk_test_multipart_object(&ecstore, &bucket, key, vec![part_one, part_two, part_three]).await;
    let input = GetObjectInput::builder()
        .bucket(bucket.clone())
        .key(key.to_string())
        .build()
        .unwrap();
    let req = build_request(input, Method::GET);

    let request_context = prepare_get_object_request_context(&req).await.unwrap();
    let manager = get_concurrency_manager();
    let read_setup =
        select_reconstructed_chunk_read(&disk_paths, &bucket, key, "part.2", &ecstore, manager, &request_context).await;

    let mut direct_stream = match read_setup.body_source {
        GetObjectBodySource::Chunk { path, copy_mode, stream } => {
            assert!(matches!(path, GetObjectChunkPath::Direct), "expected direct chunk path");
            assert_eq!(
                copy_mode,
                rustfs_io_metrics::CopyMode::Reconstructed,
                "whole multipart GET should keep the reconstructed chunk fast path when a middle part is missing a shard"
            );
            stream
        }
        GetObjectBodySource::Reader(_) => panic!("expected chunk body source"),
    };

    let mut expected = Vec::with_capacity(parts.iter().map(Vec::len).sum());
    for part in &parts {
        expected.extend_from_slice(part);
    }

    let mut direct_collected = Vec::new();
    while let Some(chunk) = direct_stream.next().await {
        direct_collected.extend_from_slice(chunk.unwrap().as_bytes().as_ref());
    }
    assert_eq!(
        direct_collected.len(),
        expected.len(),
        "prepared chunk stream reconstructed whole multipart length mismatch"
    );
    let direct_first_diff = direct_collected
        .iter()
        .zip(expected.iter())
        .position(|(left, right)| left != right);
    assert_eq!(
        direct_first_diff, None,
        "prepared chunk stream reconstructed whole multipart first diff at {:?}",
        direct_first_diff
    );
    assert_eq!(
        direct_collected, expected,
        "prepared chunk stream should cover the whole reconstructed multipart object"
    );

    let usecase = DefaultObjectUsecase::without_context();
    let response = execute_get_object_with_fast_path_enabled(&usecase, req).await.unwrap();
    assert_eq!(response.output.content_length, Some(expected.len() as i64));
    let mut body = response.output.body.expect("expected body");
    let mut collected = Vec::new();
    while let Some(chunk) = body.next().await {
        collected.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(collected.len(), expected.len(), "whole multipart reconstructed GET length mismatch");
    let first_diff = collected.iter().zip(expected.iter()).position(|(left, right)| left != right);
    assert_eq!(first_diff, None, "whole multipart reconstructed GET first diff at {:?}", first_diff);
    assert_eq!(collected, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "requires isolated global object layer state"]
async fn execute_get_object_multipart_range_marks_reconstructed_path_for_missing_shard_part() {
    let (disk_paths, ecstore) = setup_direct_chunk_multi_disk_test_env().await;
    let bucket = format!("direct-multipart-range-reconstructed-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let key = "test/multipart-range-reconstructed.bin";
    let part_one: Vec<u8> = (0..(5 * 1024 * 1024)).map(|idx| (idx % 251) as u8).collect();
    let part_two: Vec<u8> = (0..(5 * 1024 * 1024 + 257)).map(|idx| ((idx + 17) % 251) as u8).collect();
    let part_three: Vec<u8> = (0..(1024 * 1024 + 211)).map(|idx| ((idx + 33) % 251) as u8).collect();

    create_direct_chunk_test_bucket(&ecstore, &bucket).await;
    let parts =
        create_direct_chunk_test_multipart_object(&ecstore, &bucket, key, vec![part_one.clone(), part_two.clone(), part_three])
            .await;

    let range_start = 1024_u64;
    let range_end = 64 * 1024 + 2048;
    let input = GetObjectInput::builder()
        .bucket(bucket.clone())
        .key(key.to_string())
        .range(Some(Range::Int {
            first: range_start,
            last: Some(range_end),
        }))
        .build()
        .unwrap();
    let req = build_request(input, Method::GET);

    let request_context = prepare_get_object_request_context(&req).await.unwrap();
    let manager = get_concurrency_manager();
    let read_setup =
        select_reconstructed_chunk_read(&disk_paths, &bucket, key, "part.1", &ecstore, manager, &request_context).await;

    match read_setup.body_source {
        GetObjectBodySource::Chunk { path, copy_mode, .. } => {
            assert!(matches!(path, GetObjectChunkPath::Direct), "expected direct chunk path");
            assert_eq!(
                copy_mode,
                rustfs_io_metrics::CopyMode::Reconstructed,
                "multipart partial range should stay on reconstructed fast path when the covered part has a missing shard"
            );
        }
        GetObjectBodySource::Reader(_) => panic!("expected chunk body source"),
    }

    let mut expected = Vec::with_capacity(parts.iter().map(Vec::len).sum());
    for part in &parts {
        expected.extend_from_slice(part);
    }

    let usecase = DefaultObjectUsecase::without_context();
    let response = execute_get_object_with_fast_path_enabled(&usecase, req).await.unwrap();
    assert_eq!(response.output.content_length, Some((range_end - range_start + 1) as i64));
    let mut body = response.output.body.expect("expected body");
    let mut collected = Vec::new();
    while let Some(chunk) = body.next().await {
        collected.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(collected, expected[range_start as usize..=range_end as usize].to_vec());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "requires isolated global object layer state"]
async fn execute_get_object_cross_part_multipart_range_marks_reconstructed_path_for_missing_next_part_shard() {
    let (disk_paths, ecstore) = setup_direct_chunk_multi_disk_test_env().await;
    let bucket = format!("direct-multipart-cross-range-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let key = "test/multipart-cross-range-reconstructed.bin";
    let part_one: Vec<u8> = (0..(5 * 1024 * 1024)).map(|idx| (idx % 251) as u8).collect();
    let part_two: Vec<u8> = (0..(5 * 1024 * 1024 + 257)).map(|idx| ((idx + 17) % 251) as u8).collect();
    let part_three: Vec<u8> = (0..(1024 * 1024 + 211)).map(|idx| ((idx + 33) % 251) as u8).collect();

    create_direct_chunk_test_bucket(&ecstore, &bucket).await;
    let parts = create_direct_chunk_test_multipart_object(&ecstore, &bucket, key, vec![part_one, part_two, part_three]).await;

    let part_one_len = parts[0].len() as u64;
    let range_start = part_one_len - 32 * 1024;
    let range_end = part_one_len + 96 * 1024;
    let input = GetObjectInput::builder()
        .bucket(bucket.clone())
        .key(key.to_string())
        .range(Some(Range::Int {
            first: range_start,
            last: Some(range_end),
        }))
        .build()
        .unwrap();
    let req = build_request(input, Method::GET);

    let request_context = prepare_get_object_request_context(&req).await.unwrap();
    let manager = get_concurrency_manager();
    let read_setup =
        select_reconstructed_chunk_read(&disk_paths, &bucket, key, "part.2", &ecstore, manager, &request_context).await;

    match read_setup.body_source {
        GetObjectBodySource::Chunk { path, copy_mode, .. } => {
            assert!(matches!(path, GetObjectChunkPath::Direct), "expected direct chunk path");
            assert_eq!(
                copy_mode,
                rustfs_io_metrics::CopyMode::Reconstructed,
                "cross-part multipart range should keep the reconstructed chunk fast path when the next part has a missing shard"
            );
        }
        GetObjectBodySource::Reader(_) => panic!("expected chunk body source"),
    }

    let mut expected = Vec::with_capacity(parts.iter().map(Vec::len).sum());
    for part in &parts {
        expected.extend_from_slice(part);
    }

    let usecase = DefaultObjectUsecase::without_context();
    let response = usecase.execute_get_object(req).await.unwrap();
    assert_eq!(response.output.content_length, Some((range_end - range_start + 1) as i64));
    let mut body = response.output.body.expect("expected body");
    let mut collected = Vec::new();
    while let Some(chunk) = body.next().await {
        collected.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(collected, expected[range_start as usize..=range_end as usize].to_vec());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "requires isolated global object layer state"]
async fn execute_get_object_cross_part_multipart_range_marks_reconstructed_path_for_missing_final_part_shard() {
    let (disk_paths, ecstore) = setup_direct_chunk_multi_disk_test_env().await;
    let bucket = format!("direct-multipart-final-cross-range-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let key = "test/multipart-final-cross-range-reconstructed.bin";
    let part_one: Vec<u8> = (0..(5 * 1024 * 1024)).map(|idx| (idx % 251) as u8).collect();
    let part_two: Vec<u8> = (0..(5 * 1024 * 1024 + 257)).map(|idx| ((idx + 17) % 251) as u8).collect();
    let part_three: Vec<u8> = (0..(1024 * 1024 + 211)).map(|idx| ((idx + 33) % 251) as u8).collect();

    create_direct_chunk_test_bucket(&ecstore, &bucket).await;
    let parts = create_direct_chunk_test_multipart_object(&ecstore, &bucket, key, vec![part_one, part_two, part_three]).await;

    let part_two_end = (parts[0].len() + parts[1].len()) as u64;
    let range_start = part_two_end - 32 * 1024;
    let range_end = part_two_end + 96 * 1024;
    let input = GetObjectInput::builder()
        .bucket(bucket.clone())
        .key(key.to_string())
        .range(Some(Range::Int {
            first: range_start,
            last: Some(range_end),
        }))
        .build()
        .unwrap();
    let req = build_request(input, Method::GET);

    let request_context = prepare_get_object_request_context(&req).await.unwrap();
    let manager = get_concurrency_manager();
    let read_setup =
        select_reconstructed_chunk_read(&disk_paths, &bucket, key, "part.3", &ecstore, manager, &request_context).await;

    match read_setup.body_source {
        GetObjectBodySource::Chunk { path, copy_mode, .. } => {
            assert!(matches!(path, GetObjectChunkPath::Direct), "expected direct chunk path");
            assert_eq!(
                copy_mode,
                rustfs_io_metrics::CopyMode::Reconstructed,
                "cross-part multipart range into the final part should keep the reconstructed chunk fast path"
            );
        }
        GetObjectBodySource::Reader(_) => panic!("expected chunk body source"),
    }

    let mut expected = Vec::with_capacity(parts.iter().map(Vec::len).sum());
    for part in &parts {
        expected.extend_from_slice(part);
    }

    let usecase = DefaultObjectUsecase::without_context();
    let response = usecase.execute_get_object(req).await.unwrap();
    assert_eq!(response.output.content_length, Some((range_end - range_start + 1) as i64));
    let mut body = response.output.body.expect("expected body");
    let mut collected = Vec::new();
    while let Some(chunk) = body.next().await {
        collected.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(collected, expected[range_start as usize..=range_end as usize].to_vec());
}
