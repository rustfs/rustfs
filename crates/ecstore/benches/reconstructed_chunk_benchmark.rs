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

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures_util::StreamExt;
use http::HeaderMap;
use rustfs_ecstore::bucket::metadata_sys;
use rustfs_ecstore::disk::endpoint::Endpoint;
use rustfs_ecstore::endpoints::{EndpointServerPools, Endpoints, PoolEndpoints};
use rustfs_ecstore::global::{GLOBAL_LOCAL_DISK_ID_MAP, GLOBAL_LOCAL_DISK_MAP, GLOBAL_LOCAL_DISK_SET_DRIVES};
use rustfs_ecstore::store::{ECStore, init_local_disks};
use rustfs_ecstore::store_api::{
    BucketOperations, BucketOptions, ChunkNativePutData, CompletePart, GetObjectChunkCopyMode, HTTPRangeSpec, MakeBucketOptions,
    MultipartOperations, ObjectOperations, ObjectOptions,
};
use std::hint::black_box;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use tempfile::TempDir;
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
enum ReconstructedReadSpec {
    PartNumber {
        part_number: usize,
        missing_part_name: &'static str,
    },
    Range {
        start: u64,
        end: u64,
        missing_part_name: &'static str,
    },
}

#[derive(Clone)]
struct ReconstructedBenchCase {
    object_id: &'static str,
    name: &'static str,
    read_spec: ReconstructedReadSpec,
}

struct ReconstructedBenchEnv {
    store: Arc<ECStore>,
    bucket: String,
    key: String,
    range: HTTPRangeSpec,
    opts: ObjectOptions,
    expected_len: usize,
}

struct ReconstructedBenchSuite {
    _temp_dir: TempDir,
    store: Arc<ECStore>,
    disk_paths: Vec<PathBuf>,
}

const MULTIPART_PART_ONE_LEN: usize = 5 * 1024 * 1024;
const MULTIPART_PART_TWO_LEN: usize = 5 * 1024 * 1024 + 137;
const MULTIPART_PART_THREE_LEN: usize = 1024 * 1024 + 77;

fn reconstructed_bench_cases() -> [ReconstructedBenchCase; 4] {
    [
        ReconstructedBenchCase {
            object_id: "mp-part2",
            name: "multi_disk_missing_shard_multipart_part2",
            read_spec: ReconstructedReadSpec::PartNumber {
                part_number: 2,
                missing_part_name: "part.2",
            },
        },
        ReconstructedBenchCase {
            object_id: "mp-cross-range",
            name: "multi_disk_missing_shard_multipart_cross_part_range",
            read_spec: ReconstructedReadSpec::Range {
                start: (MULTIPART_PART_ONE_LEN - 32 * 1024) as u64,
                end: (MULTIPART_PART_ONE_LEN + 96 * 1024) as u64,
                missing_part_name: "part.2",
            },
        },
        ReconstructedBenchCase {
            object_id: "mp-part3",
            name: "multi_disk_missing_shard_multipart_part3",
            read_spec: ReconstructedReadSpec::PartNumber {
                part_number: 3,
                missing_part_name: "part.3",
            },
        },
        ReconstructedBenchCase {
            object_id: "mp-cross-final-range",
            name: "multi_disk_missing_shard_multipart_cross_final_part_range",
            read_spec: ReconstructedReadSpec::Range {
                start: (MULTIPART_PART_ONE_LEN + MULTIPART_PART_TWO_LEN - 32 * 1024) as u64,
                end: (MULTIPART_PART_ONE_LEN + MULTIPART_PART_TWO_LEN + 96 * 1024) as u64,
                missing_part_name: "part.3",
            },
        },
    ]
}

fn next_loopback_addr() -> SocketAddr {
    static NEXT_PORT: AtomicU16 = AtomicU16::new(39113);
    let port = NEXT_PORT.fetch_add(1, Ordering::Relaxed);
    SocketAddr::from(([127, 0, 0, 1], port))
}

fn clone_range_spec(range: &HTTPRangeSpec) -> HTTPRangeSpec {
    HTTPRangeSpec {
        is_suffix_length: range.is_suffix_length,
        start: range.start,
        end: range.end,
    }
}

fn build_endpoint_pools(paths: &[PathBuf]) -> EndpointServerPools {
    let mut endpoints = Vec::with_capacity(paths.len());
    for (idx, disk_path) in paths.iter().enumerate() {
        let mut endpoint = Endpoint::try_from(disk_path.to_str().expect("utf8 path")).expect("endpoint");
        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(idx);
        endpoints.push(endpoint);
    }

    EndpointServerPools(vec![PoolEndpoints {
        legacy: false,
        set_count: 1,
        drives_per_set: paths.len(),
        endpoints: Endpoints::from(endpoints),
        cmd_line: "bench".to_string(),
        platform: format!("OS: {} | Arch: {}", std::env::consts::OS, std::env::consts::ARCH),
    }])
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
        let content = tokio::fs::read(&path).await.expect("read part file before removal");
        tokio::fs::remove_file(&path).await.expect("remove part file");
        removed.push((path, content));
    }

    removed
}

async fn restore_part_files(files: Vec<(PathBuf, Vec<u8>)>) {
    for (path, content) in files {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .expect("ensure parent for part restore");
        }
        tokio::fs::write(&path, content).await.expect("restore part file");
    }
}

async fn run_reconstructed_get_object_chunks(
    store: &Arc<ECStore>,
    bucket: &str,
    key: &str,
    range: &HTTPRangeSpec,
    opts: &ObjectOptions,
) -> (GetObjectChunkCopyMode, usize, usize) {
    let mut result = store
        .get_object_chunks(bucket, key, Some(clone_range_spec(range)), HeaderMap::new(), opts)
        .await
        .expect("get object chunks");

    let copy_mode = result.copy_mode;
    let mut total_len = 0usize;
    let mut chunk_count = 0usize;
    while let Some(chunk) = result.stream.next().await {
        let chunk = chunk.expect("chunk");
        total_len += chunk.len();
        chunk_count += 1;
    }

    (copy_mode, total_len, chunk_count)
}

async fn create_multipart_object(store: &Arc<ECStore>, bucket: &str, key: &str, parts: &[Vec<u8>]) -> usize {
    let upload = store
        .new_multipart_upload(bucket, key, &ObjectOptions::default())
        .await
        .expect("new multipart upload");

    let mut completed_parts = Vec::with_capacity(parts.len());
    for (idx, part) in parts.iter().enumerate() {
        let mut reader = ChunkNativePutData::from_vec(part.clone());
        let part_info = store
            .put_object_part(bucket, key, &upload.upload_id, idx + 1, &mut reader, &ObjectOptions::default())
            .await
            .expect("put object part");
        completed_parts.push(CompletePart {
            part_num: idx + 1,
            etag: part_info.etag,
            ..Default::default()
        });
    }

    store
        .clone()
        .complete_multipart_upload(bucket, key, &upload.upload_id, completed_parts, &ObjectOptions::default())
        .await
        .expect("complete multipart upload");

    parts.iter().map(Vec::len).sum()
}

async fn build_reconstructed_bench_suite() -> ReconstructedBenchSuite {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let disk_paths: Vec<_> = (1..=4).map(|idx| temp_dir.path().join(format!("disk{idx}"))).collect();
    for disk_path in &disk_paths {
        tokio::fs::create_dir_all(disk_path).await.expect("create disk dir");
    }

    let endpoint_pools = build_endpoint_pools(&disk_paths);
    GLOBAL_LOCAL_DISK_MAP.write().await.clear();
    GLOBAL_LOCAL_DISK_ID_MAP.write().await.clear();
    GLOBAL_LOCAL_DISK_SET_DRIVES.write().await.clear();
    init_local_disks(endpoint_pools.clone()).await.expect("init local disks");

    let store = ECStore::new(next_loopback_addr(), endpoint_pools, CancellationToken::new())
        .await
        .expect("create ecstore");

    let buckets = store
        .list_bucket(&BucketOptions {
            no_metadata: true,
            ..Default::default()
        })
        .await
        .expect("list buckets")
        .into_iter()
        .map(|bucket| bucket.name)
        .collect();
    metadata_sys::init_bucket_metadata_sys(store.clone(), buckets).await;

    ReconstructedBenchSuite {
        _temp_dir: temp_dir,
        store,
        disk_paths,
    }
}

async fn build_reconstructed_bench_env(suite: &ReconstructedBenchSuite, case: &ReconstructedBenchCase) -> ReconstructedBenchEnv {
    let bucket = format!("bench-r-{}", case.object_id);
    let key = format!("objects/{}.bin", case.object_id);
    suite
        .store
        .make_bucket(
            &bucket,
            &MakeBucketOptions {
                versioning_enabled: true,
                ..Default::default()
            },
        )
        .await
        .expect("make bucket");

    let part_one: Vec<u8> = (0..MULTIPART_PART_ONE_LEN).map(|idx| (idx % 251) as u8).collect();
    let part_two: Vec<u8> = (0..MULTIPART_PART_TWO_LEN).map(|idx| ((idx + 11) % 251) as u8).collect();
    let part_three: Vec<u8> = (0..MULTIPART_PART_THREE_LEN).map(|idx| ((idx + 29) % 251) as u8).collect();
    let payload_len = create_multipart_object(&suite.store, &bucket, &key, &[part_one, part_two, part_three]).await;
    let info = suite
        .store
        .get_object_info(&bucket, &key, &ObjectOptions::default())
        .await
        .expect("get object info");
    let (range, opts, missing_part_name) = match case.read_spec.clone() {
        ReconstructedReadSpec::PartNumber {
            part_number,
            missing_part_name,
        } => {
            let range = HTTPRangeSpec::from_object_info(&info, part_number).expect("part_number range");
            let opts = ObjectOptions {
                part_number: Some(part_number),
                ..Default::default()
            };
            (range, opts, missing_part_name)
        }
        ReconstructedReadSpec::Range {
            start,
            end,
            missing_part_name,
        } => (
            HTTPRangeSpec {
                is_suffix_length: false,
                start: start as i64,
                end: end as i64,
            },
            ObjectOptions::default(),
            missing_part_name,
        ),
    };

    let (_, expected_len) = range.get_offset_length(payload_len as i64).expect("range length");
    let mut selected_reconstructed = false;
    for disk_path in &suite.disk_paths {
        let object_root = disk_path
            .join(&bucket)
            .join("objects")
            .join(format!("{}.bin", case.object_id));
        let removed_parts = remove_part_files(&object_root, missing_part_name).await;
        if removed_parts.is_empty() {
            continue;
        }

        let (copy_mode, total_len, _) = run_reconstructed_get_object_chunks(&suite.store, &bucket, &key, &range, &opts).await;
        if copy_mode == GetObjectChunkCopyMode::Reconstructed && total_len == expected_len as usize {
            selected_reconstructed = true;
            break;
        }

        restore_part_files(removed_parts).await;
    }
    assert!(
        selected_reconstructed,
        "failed to select a missing shard placement that triggers reconstructed copy mode for benchmark case {}",
        case.name
    );

    ReconstructedBenchEnv {
        store: suite.store.clone(),
        bucket,
        key,
        range: clone_range_spec(&range),
        opts,
        expected_len: expected_len as usize,
    }
}

async fn run_reconstructed_get_object_chunks_bench(env: &ReconstructedBenchEnv) -> (GetObjectChunkCopyMode, usize, usize) {
    run_reconstructed_get_object_chunks(&env.store, &env.bucket, &env.key, &env.range, &env.opts).await
}

fn bench_reconstructed_chunk_path(c: &mut Criterion) {
    let runtime = Runtime::new().expect("tokio runtime");
    let suite = runtime.block_on(build_reconstructed_bench_suite());
    let mut group = c.benchmark_group("reconstructed_chunk_path");
    group.sample_size(10);
    for case in reconstructed_bench_cases() {
        let env = runtime.block_on(build_reconstructed_bench_env(&suite, &case));
        let (copy_mode, total_len, _) = runtime.block_on(run_reconstructed_get_object_chunks_bench(&env));
        assert_eq!(copy_mode, GetObjectChunkCopyMode::Reconstructed);
        assert_eq!(total_len, env.expected_len);

        group.throughput(Throughput::Bytes(env.expected_len as u64));
        group.bench_with_input(BenchmarkId::new("drain", case.name), &env, |b, env| {
            b.iter(|| {
                let result = runtime.block_on(run_reconstructed_get_object_chunks_bench(env));
                black_box(result);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_reconstructed_chunk_path);
criterion_main!(benches);
