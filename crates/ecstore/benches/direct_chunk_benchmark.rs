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

use bytes::{Bytes, BytesMut};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures_util::StreamExt;
use futures_util::stream;
use http::HeaderMap;
use rustfs_ecstore::bucket::metadata_sys;
use rustfs_ecstore::disk::endpoint::Endpoint;
use rustfs_ecstore::endpoints::{EndpointServerPools, Endpoints, PoolEndpoints};
use rustfs_ecstore::global::{GLOBAL_LOCAL_DISK_ID_MAP, GLOBAL_LOCAL_DISK_MAP, GLOBAL_LOCAL_DISK_SET_DRIVES};
use rustfs_ecstore::set_disk::collect_direct_data_shard_chunks_for_benchmark;
use rustfs_ecstore::store::{ECStore, init_local_disks};
use rustfs_ecstore::store_api::{
    BucketOperations, BucketOptions, ChunkNativePutData, GetObjectChunkCopyMode, HTTPRangeSpec, MakeBucketOptions, ObjectIO,
    ObjectOptions,
};
use rustfs_io_core::{BoxChunkStream, IoChunk, MappedChunk};
use std::hint::black_box;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use tempfile::TempDir;
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct BenchCase {
    name: &'static str,
    data_shards: usize,
    block_size: usize,
    blocks: usize,
    offset: usize,
    length: usize,
}

fn bench_cases() -> [BenchCase; 2] {
    [
        BenchCase {
            name: "single_block_aligned",
            data_shards: 4,
            block_size: 256 * 1024,
            blocks: 1,
            offset: 0,
            length: 256 * 1024,
        },
        BenchCase {
            name: "multi_block_unaligned",
            data_shards: 4,
            block_size: 256 * 1024,
            blocks: 8,
            offset: 123_457,
            length: 2 * 256 * 1024 + 33_333,
        },
    ]
}

#[derive(Clone)]
struct EcstoreBenchCase {
    name: &'static str,
    disk_count: usize,
    payload_len: usize,
    range: HTTPRangeSpec,
    expected_copy_mode: GetObjectChunkCopyMode,
}

struct EcstoreBenchEnv {
    _temp_dir: TempDir,
    store: Arc<ECStore>,
    bucket: String,
    key: String,
    range: HTTPRangeSpec,
    opts: ObjectOptions,
    expected_len: usize,
    expected_copy_mode: GetObjectChunkCopyMode,
}

fn ecstore_bench_cases() -> [EcstoreBenchCase; 1] {
    [EcstoreBenchCase {
        name: "multi_disk_range",
        disk_count: 4,
        payload_len: 3 * 1024 * 1024 + 137,
        range: HTTPRangeSpec {
            is_suffix_length: false,
            start: 123_457,
            end: 2 * 1024 * 1024 + 33_333,
        },
        expected_copy_mode: expected_direct_copy_mode(),
    }]
}

#[cfg(unix)]
const fn expected_direct_copy_mode() -> GetObjectChunkCopyMode {
    GetObjectChunkCopyMode::TrueZeroCopy
}

#[cfg(not(unix))]
const fn expected_direct_copy_mode() -> GetObjectChunkCopyMode {
    GetObjectChunkCopyMode::SharedBytes
}

fn clone_range_spec(range: &HTTPRangeSpec) -> HTTPRangeSpec {
    HTTPRangeSpec {
        is_suffix_length: range.is_suffix_length,
        start: range.start,
        end: range.end,
    }
}

fn next_loopback_addr() -> SocketAddr {
    static NEXT_PORT: AtomicU16 = AtomicU16::new(39013);
    let port = NEXT_PORT.fetch_add(1, Ordering::Relaxed);
    SocketAddr::from(([127, 0, 0, 1], port))
}

fn build_endpoint_pools(paths: &[std::path::PathBuf]) -> EndpointServerPools {
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

async fn build_ecstore_bench_env(case: &EcstoreBenchCase) -> EcstoreBenchEnv {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let mut disk_paths = Vec::with_capacity(case.disk_count);
    for idx in 0..case.disk_count {
        let path = temp_dir.path().join(format!("disk{}", idx + 1));
        tokio::fs::create_dir_all(&path).await.expect("create disk dir");
        disk_paths.push(path);
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

    let object_id = case.name.replace('_', "-");
    let bucket = format!("bench-direct-{object_id}");
    let key = format!("objects/{object_id}.bin");
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

    let payload: Vec<u8> = (0..case.payload_len).map(|idx| (idx % 251) as u8).collect();
    let mut reader = ChunkNativePutData::from_vec(payload);
    let put_info = store
        .put_object(&bucket, &key, &mut reader, &ObjectOptions::default())
        .await
        .expect("put object");
    if case.disk_count > 1 {
        assert!(put_info.data_blocks > 1, "expected multi-data-shard object");
    }

    let (_, expected_len) = case.range.get_offset_length(case.payload_len as i64).expect("range length");

    EcstoreBenchEnv {
        _temp_dir: temp_dir,
        store,
        bucket,
        key,
        range: clone_range_spec(&case.range),
        opts: ObjectOptions::default(),
        expected_len: expected_len as usize,
        expected_copy_mode: case.expected_copy_mode,
    }
}

async fn run_ecstore_get_object_chunks_bench(env: &EcstoreBenchEnv) -> (GetObjectChunkCopyMode, usize, usize) {
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

fn build_shard_bytes(case: &BenchCase) -> Vec<Vec<Bytes>> {
    let total_len = case.block_size * case.blocks;
    let payload: Vec<u8> = (0..total_len).map(|idx| (idx % 251) as u8).collect();
    let mut shards = vec![Vec::with_capacity(case.blocks); case.data_shards];

    for block in 0..case.blocks {
        let block_start = block * case.block_size;
        let block_slice = &payload[block_start..block_start + case.block_size];
        let shard_width = case.block_size / case.data_shards;
        for (shard_idx, shard) in shards.iter_mut().enumerate().take(case.data_shards) {
            let shard_start = shard_idx * shard_width;
            let shard_end = shard_start + shard_width;
            shard.push(Bytes::copy_from_slice(&block_slice[shard_start..shard_end]));
        }
    }

    shards
}

fn build_mapped_streams(shards: &[Vec<Bytes>]) -> Vec<BoxChunkStream> {
    shards
        .iter()
        .map(|shard_chunks| {
            let chunks: Vec<_> = shard_chunks
                .iter()
                .map(|chunk| {
                    let mapped = MappedChunk::new(chunk.clone(), 0, chunk.len()).expect("mapped chunk");
                    Ok::<IoChunk, std::io::Error>(IoChunk::Mapped(mapped))
                })
                .collect();
            Box::pin(stream::iter(chunks)) as BoxChunkStream
        })
        .collect()
}

fn collect_old_assembly(
    shards: &[Vec<Bytes>],
    data_shards: usize,
    block_size: usize,
    offset: usize,
    length: usize,
) -> Vec<IoChunk> {
    if length == 0 {
        return Vec::new();
    }

    let start_block = offset / block_size;
    let end_block = offset.saturating_add(length - 1) / block_size;
    let mut result = Vec::with_capacity(end_block - start_block + 1);

    for block_index in start_block..=end_block {
        let block_offset = if block_index == start_block { offset % block_size } else { 0 };
        let block_length = if start_block == end_block {
            length
        } else if block_index == start_block {
            block_size - (offset % block_size)
        } else if block_index == end_block {
            (offset + length) % block_size
        } else {
            block_size
        };

        if block_length == 0 {
            break;
        }

        let mut block = BytesMut::with_capacity(block_length);
        let mut write_left = block_length;
        let mut skip = block_offset;

        for shard in shards.iter().take(data_shards) {
            let shard_chunk = &shard[block_index];
            if skip >= shard_chunk.len() {
                skip -= shard_chunk.len();
                continue;
            }

            let available = &shard_chunk[skip..];
            skip = 0;
            let take = available.len().min(write_left);
            block.extend_from_slice(&available[..take]);
            write_left -= take;

            if write_left == 0 {
                break;
            }
        }

        result.push(IoChunk::Shared(block.freeze()));
    }

    result
}

fn bench_direct_chunk_path(c: &mut Criterion) {
    let runtime = Runtime::new().expect("tokio runtime");
    let mut group = c.benchmark_group("direct_chunk_path");

    for case in bench_cases() {
        let shard_bytes = build_shard_bytes(&case);
        group.throughput(Throughput::Bytes(case.length as u64));

        group.bench_with_input(BenchmarkId::new("slice_forwarding", case.name), &case, |b, case| {
            b.iter(|| {
                let streams = build_mapped_streams(&shard_bytes);
                let chunks = runtime
                    .block_on(collect_direct_data_shard_chunks_for_benchmark(
                        streams,
                        case.data_shards,
                        case.block_size,
                        case.blocks * case.block_size,
                        false,
                        case.offset,
                        case.length,
                    ))
                    .expect("collect direct chunks");
                black_box(chunks);
            });
        });

        group.bench_with_input(BenchmarkId::new("assembled_copy", case.name), &case, |b, case| {
            b.iter(|| {
                let chunks = collect_old_assembly(&shard_bytes, case.data_shards, case.block_size, case.offset, case.length);
                black_box(chunks);
            });
        });
    }

    group.finish();
}

fn bench_ecstore_get_object_chunks(c: &mut Criterion) {
    let runtime = Runtime::new().expect("tokio runtime");
    let mut group = c.benchmark_group("ecstore_get_object_chunks");
    group.sample_size(10);

    for case in ecstore_bench_cases() {
        let env = runtime.block_on(build_ecstore_bench_env(&case));
        let (copy_mode, total_len, _) = runtime.block_on(run_ecstore_get_object_chunks_bench(&env));
        assert_eq!(copy_mode, env.expected_copy_mode);
        assert_eq!(total_len, env.expected_len);

        group.throughput(Throughput::Bytes(env.expected_len as u64));
        group.bench_with_input(BenchmarkId::new("drain", case.name), &env, |b, env| {
            b.iter(|| {
                let result = runtime.block_on(run_ecstore_get_object_chunks_bench(env));
                black_box(result);
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_direct_chunk_path, bench_ecstore_get_object_chunks);
criterion_main!(benches);
