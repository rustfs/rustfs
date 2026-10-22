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
use rustfs_utils::HashAlgorithm;
use rustfs_utils::http::{
    SUFFIX_ACTUAL_SIZE, SUFFIX_COMPRESSION, SUFFIX_CRC, SUFFIX_INLINE_DATA, SUFFIX_PART_CHECKSUMS, SUFFIX_TRANSITION_STATUS,
    SUFFIX_TRANSITIONED_VERSION_ID, get_consistent_str, has_internal_suffix, insert_str, is_internal_key,
    strip_internal_prefix_preserving_case,
};
use rustfs_utils::path::{clean, path_join_buf};
use std::collections::HashMap;
use std::hint::black_box;

fn generate_payload(size: usize) -> Vec<u8> {
    (0..size)
        .map(|i| u8::try_from(i % 251).expect("modulo output fits in u8"))
        .collect()
}

fn bench_hash_hotpaths(c: &mut Criterion) {
    let payloads = [
        ("64KiB", generate_payload(64 * 1024)),
        ("1MiB", generate_payload(1024 * 1024)),
    ];
    let algorithms = [
        ("md5", HashAlgorithm::Md5),
        ("sha256", HashAlgorithm::SHA256),
        ("highwayhash256s", HashAlgorithm::HighwayHash256S),
        ("highwayhash256s_legacy", HashAlgorithm::HighwayHash256SLegacy),
    ];

    let mut group = c.benchmark_group("hash_hotpath");
    for (payload_name, payload) in &payloads {
        let payload_len = u64::try_from(payload.len()).expect("benchmark payload length fits in u64");
        group.throughput(Throughput::Bytes(payload_len));
        for (algo_name, algorithm) in &algorithms {
            let algorithm = algorithm.clone();
            group.bench_with_input(BenchmarkId::new(*algo_name, payload_name), payload.as_slice(), move |b, payload| {
                b.iter(|| {
                    let hash = algorithm.hash_encode(black_box(payload));
                    black_box(hash.as_ref()[0]);
                });
            });
        }
    }
    group.finish();
}

fn bench_metadata_compat_hotpaths(c: &mut Criterion) {
    let keys = [
        "content-type",
        "x-amz-meta-author",
        "x-amz-meta-project",
        "x-amz-meta-env",
        "x-rustfs-internal-compression",
        "x-minio-internal-compression",
        "x-rustfs-internal-compression-size",
        "x-minio-internal-compression-size",
        "x-rustfs-internal-actual-size",
        "x-minio-internal-actual-size",
        "x-rustfs-internal-crc",
        "x-minio-internal-crc",
        "x-rustfs-internal-inline-data",
        "x-minio-internal-inline-data",
        "X-Minio-Internal-Transition-Status",
        "X-RustFS-Internal-Transitioned-VersionID",
    ];

    let mut map = HashMap::new();
    map.insert("content-type".to_string(), "application/octet-stream".to_string());
    map.insert("x-amz-meta-author".to_string(), "test-user".to_string());
    map.insert("x-amz-meta-project".to_string(), "rustfs".to_string());
    map.insert("x-amz-meta-env".to_string(), "production".to_string());
    insert_str(&mut map, SUFFIX_COMPRESSION, "klauspost/compress/s2".to_string());
    insert_str(&mut map, SUFFIX_ACTUAL_SIZE, "2097152".to_string());
    insert_str(&mut map, SUFFIX_CRC, "crc32c:abcdef01".to_string());
    insert_str(&mut map, SUFFIX_INLINE_DATA, "true".to_string());
    insert_str(&mut map, SUFFIX_TRANSITION_STATUS, "COMPLETE".to_string());

    let mut group = c.benchmark_group("metadata_compat_hotpath");
    group.throughput(Throughput::Elements(u64::try_from(keys.len()).expect("benchmark key count fits in u64")));
    group.bench_function("classify_and_strip_batch", |b| {
        b.iter(|| {
            let mut matched = 0usize;
            for &key in black_box(&keys) {
                if is_internal_key(key) {
                    matched += 1;
                }
                if let Some(suffix) = strip_internal_prefix_preserving_case(key) {
                    matched += suffix.len();
                }
                if has_internal_suffix(key, SUFFIX_CRC)
                    || has_internal_suffix(key, SUFFIX_PART_CHECKSUMS)
                    || has_internal_suffix(key, SUFFIX_TRANSITIONED_VERSION_ID)
                {
                    matched += 1;
                }
            }
            black_box(matched)
        });
    });

    group.throughput(Throughput::Elements(3));
    group.bench_function("get_consistent_str_lookup", |b| {
        b.iter(|| {
            let a = get_consistent_str(black_box(&map), black_box(SUFFIX_CRC));
            let b_val = get_consistent_str(black_box(&map), black_box(SUFFIX_TRANSITION_STATUS));
            let c_val = get_consistent_str(black_box(&map), black_box(SUFFIX_PART_CHECKSUMS));
            black_box((a, b_val, c_val))
        });
    });
    group.finish();
}

fn bench_path_hotpaths(c: &mut Criterion) {
    let mut group = c.benchmark_group("path_hotpath");
    let object_elems = [
        "/data/rustfs0",
        "bucket-production",
        "2026/10/22/prefix",
        "object-00001234",
        "xl.meta",
    ];
    let dir_elems = [
        "/data/rustfs0/",
        "bucket-production",
        "2026/10/22/prefix/",
        "subdir-00001234/",
    ];
    let clean_input = "/data/rustfs0/bucket-production/2026/10/22/prefix/object-00001234/xl.meta";
    let dirty_input = "/data/rustfs0//bucket-production/./2026/10/22/prefix/sub/../object-00001234/xl.meta/";

    group.bench_function("path_join_buf_object", |b| {
        b.iter(|| black_box(path_join_buf(black_box(&object_elems))));
    });
    group.bench_function("path_join_buf_dir_trailing_slash", |b| {
        b.iter(|| black_box(path_join_buf(black_box(&dir_elems))));
    });
    group.bench_function("clean_already_clean", |b| {
        b.iter(|| black_box(clean(black_box(clean_input))));
    });
    group.bench_function("clean_needs_clean", |b| {
        b.iter(|| black_box(clean(black_box(dirty_input))));
    });
    group.finish();
}

criterion_group!(benches, bench_hash_hotpaths, bench_metadata_compat_hotpaths, bench_path_hotpaths);
criterion_main!(benches);
