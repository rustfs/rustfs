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
    BucketOperations, BucketOptions, GetObjectChunkCopyMode, HTTPRangeSpec, MakeBucketOptions, ObjectIO, ObjectOptions,
    PutObjReader,
};
use std::hint::black_box;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use tempfile::TempDir;
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

struct ReconstructedBenchEnv {
    _temp_dir: TempDir,
    store: Arc<ECStore>,
    bucket: String,
    key: String,
    range: HTTPRangeSpec,
    opts: ObjectOptions,
    expected_len: usize,
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

async fn build_reconstructed_bench_env() -> ReconstructedBenchEnv {
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

    let bucket = "bench-reconstructed-range".to_string();
    let key = "objects/reconstructed-range.bin".to_string();
    store
        .make_bucket(
            &bucket,
            &MakeBucketOptions {
                versioning_enabled: true,
                ..Default::default()
            },
        )
        .await
        .expect("make bucket");

    let payload_len = 3 * 1024 * 1024 + 137;
    let payload: Vec<u8> = (0..payload_len).map(|idx| (idx % 251) as u8).collect();
    let mut reader = PutObjReader::from_vec(payload);
    let put_info = store
        .put_object(&bucket, &key, &mut reader, &ObjectOptions::default())
        .await
        .expect("put object");
    assert!(put_info.data_blocks > 1, "expected multi-data-shard object");

    let object_root = disk_paths[0].join(&bucket).join("objects").join("reconstructed-range.bin");
    let missing_part = find_part_file(&object_root, "part.1").expect("part file on first disk");
    tokio::fs::remove_file(&missing_part).await.expect("remove first shard part");

    let range = HTTPRangeSpec {
        is_suffix_length: false,
        start: 123_457,
        end: 2 * 1024 * 1024 + 33_333,
    };
    let (_, expected_len) = range.get_offset_length(payload_len as i64).expect("range length");

    ReconstructedBenchEnv {
        _temp_dir: temp_dir,
        store,
        bucket,
        key,
        range,
        opts: ObjectOptions::default(),
        expected_len: expected_len as usize,
    }
}

async fn run_reconstructed_get_object_chunks_bench(env: &ReconstructedBenchEnv) -> (GetObjectChunkCopyMode, usize, usize) {
    let mut result = env
        .store
        .get_object_chunks(&env.bucket, &env.key, Some(clone_range_spec(&env.range)), HeaderMap::new(), &env.opts)
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

fn bench_reconstructed_chunk_path(c: &mut Criterion) {
    let runtime = Runtime::new().expect("tokio runtime");
    let env = runtime.block_on(build_reconstructed_bench_env());
    let (copy_mode, total_len, _) = runtime.block_on(run_reconstructed_get_object_chunks_bench(&env));
    assert_eq!(copy_mode, GetObjectChunkCopyMode::Reconstructed);
    assert_eq!(total_len, env.expected_len);

    let mut group = c.benchmark_group("reconstructed_chunk_path");
    group.sample_size(10);
    group.throughput(Throughput::Bytes(env.expected_len as u64));
    group.bench_with_input(BenchmarkId::new("drain", "multi_disk_missing_shard"), &env, |b, env| {
        b.iter(|| {
            let result = runtime.block_on(run_reconstructed_get_object_chunks_bench(env));
            black_box(result);
        });
    });
    group.finish();
}

criterion_group!(benches, bench_reconstructed_chunk_path);
criterion_main!(benches);
