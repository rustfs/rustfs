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

use bytes::Bytes;
use criterion::{BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main};
use rustfs_filemeta::{ErasureAlgo, FileInfo, FileMeta, ObjectPartInfo};
use serde::Serialize;
use std::hint::black_box;
use std::time::Duration;
use time::OffsetDateTime;
use uuid::Uuid;

const VERSION_COUNT_CASES: &[usize] = &[1, 8, 32, 64];
const BENCH_BASE_TIME_UNIX_SECS: i64 = 1_700_000_000;

fn make_file_info(version_id: Uuid, data_dir: Uuid, size: i64, mod_time: OffsetDateTime) -> FileInfo {
    FileInfo {
        version_id: Some(version_id),
        data_dir: Some(data_dir),
        size,
        mod_time: Some(mod_time),
        metadata: [("etag".to_string(), format!("etag-{version_id}"))].into_iter().collect(),
        erasure: rustfs_filemeta::ErasureInfo {
            algorithm: ErasureAlgo::ReedSolomon.to_string(),
            data_blocks: 4,
            parity_blocks: 2,
            block_size: 1024 * 1024,
            index: 1,
            distribution: vec![1, 2, 3, 4, 5, 6],
            ..Default::default()
        },
        ..Default::default()
    }
}

fn build_meta_with_versions(version_count: usize) -> FileMeta {
    let mut meta = FileMeta::new();
    let base_time = OffsetDateTime::from_unix_timestamp(BENCH_BASE_TIME_UNIX_SECS).expect("valid bench base timestamp");
    for i in 0..version_count {
        let fi = make_file_info(Uuid::new_v4(), Uuid::new_v4(), 64 * 1024, base_time - Duration::from_secs(i as u64));
        meta.add_version(fi).expect("seed add_version should succeed");
    }
    meta
}

fn bench_rename_data_meta_path(c: &mut Criterion) {
    let mut group = c.benchmark_group("rename_data_meta");

    for &version_count in VERSION_COUNT_CASES {
        let seeded = build_meta_with_versions(version_count);
        let dst_buf = seeded.marshal_msg().expect("marshal seeded meta");
        let base_time = OffsetDateTime::from_unix_timestamp(BENCH_BASE_TIME_UNIX_SECS).expect("valid bench base timestamp");
        let replace_version_id = seeded
            .versions
            .first()
            .and_then(|v| v.header.version_id)
            .unwrap_or(Uuid::nil());

        group.bench_with_input(BenchmarkId::new("find_unshared_data_dir", version_count), &version_count, |b, _| {
            b.iter(|| black_box(seeded.find_unshared_data_dir_for_version(black_box(Some(replace_version_id)))));
        });

        group.bench_with_input(BenchmarkId::new("read_modify_write", version_count), &version_count, |b, _| {
            b.iter(|| {
                let mut xlmeta = FileMeta::load(black_box(&dst_buf)).expect("load dst meta");
                let search_version_id = Some(replace_version_id);
                let has_old_data_dir = xlmeta.find_unshared_data_dir_for_version(search_version_id);
                if let Some(old_data_dir) = has_old_data_dir {
                    let _ = xlmeta.data.remove_two(replace_version_id, old_data_dir);
                }
                let fi = make_file_info(replace_version_id, Uuid::new_v4(), 64 * 1024, base_time + Duration::from_millis(1));
                xlmeta.add_version(fi).expect("add new version");
                let out = xlmeta.marshal_msg().expect("marshal updated meta");
                black_box(out);
            });
        });

        let mut prepared = FileMeta::load(&dst_buf).expect("load prepared meta");
        if let Some(old_data_dir) = prepared.find_unshared_data_dir_for_version(Some(replace_version_id)) {
            let _ = prepared.data.remove_two(replace_version_id, old_data_dir);
        }
        group.bench_with_input(BenchmarkId::new("add_version_marshal_only", version_count), &version_count, |b, _| {
            b.iter_batched(
                || prepared.clone(),
                |mut xlmeta| {
                    let fi = make_file_info(replace_version_id, Uuid::new_v4(), 64 * 1024, base_time + Duration::from_millis(1));
                    xlmeta.add_version(fi).expect("add new version");
                    let out = xlmeta.marshal_msg().expect("marshal updated meta");
                    black_box(out);
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_with_input(BenchmarkId::new("remove_two_only", version_count), &version_count, |b, _| {
            b.iter(|| {
                let mut xlmeta = FileMeta::load(black_box(&dst_buf)).expect("load dst meta");
                let removed = if let Some(old_data_dir) = xlmeta.find_unshared_data_dir_for_version(Some(replace_version_id)) {
                    xlmeta.data.remove_two(replace_version_id, old_data_dir).expect("remove two")
                } else {
                    false
                };
                black_box(removed);
                black_box(xlmeta);
            });
        });
    }
}

fn bench_delete_version_batches(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete_version_batch");
    // Keep iteration counts and fixture batches independent of sample position.
    group.sampling_mode(SamplingMode::Flat);
    for (kind, inline_size) in [
        ("physical", None),
        ("inline-64", Some(64_usize)),
        ("inline-9k", Some(9 * 1024)),
        ("markers", None),
        ("mixed-markers-physical", None),
        ("mixed-markers-inline", Some(64)),
    ] {
        for (versions, requested) in [(1_usize, 1), (8, 8), (32, 32), (128, 128), (1000, 1000), (1000, 32)] {
            let mut meta = FileMeta::new();
            let base_time = OffsetDateTime::from_unix_timestamp(BENCH_BASE_TIME_UNIX_SECS).expect("fixture timestamp");
            for id in 1..=versions {
                let size = inline_size.unwrap_or(64 * 1024);
                let mut info = make_file_info(
                    Uuid::from_u128(u128::try_from(id).expect("version id")),
                    Uuid::from_u128(u128::try_from(versions + id).expect("directory id")),
                    i64::try_from(size).expect("object size"),
                    base_time + time::Duration::seconds(i64::try_from(id).expect("version time")),
                );
                if kind == "markers" || (kind.starts_with("mixed-markers-") && id % 2 == 0) {
                    info = FileInfo {
                        deleted: true,
                        version_id: info.version_id,
                        mod_time: info.mod_time,
                        ..Default::default()
                    };
                } else {
                    info.parts.push(ObjectPartInfo {
                        number: 1,
                        size,
                        actual_size: info.size,
                        ..Default::default()
                    });
                    if inline_size.is_some() {
                        info.set_inline_data();
                        info.data = Some(Bytes::from(vec![42; size]));
                    }
                }
                meta.add_version(info).expect("seed batch metadata");
            }
            let requests: Vec<_> = meta
                .versions
                .iter()
                .take(requested)
                .map(|version| FileInfo {
                    version_id: version.header.version_id,
                    ..Default::default()
                })
                .collect();
            // Large histories use one fixture per timing interval in both implementations.
            // Tiny histories amortize the timer over a fixed batch instead.
            let batch_size = if versions < 128 {
                BatchSize::NumIterations(32)
            } else {
                BatchSize::PerIteration
            };
            group.bench_with_input(BenchmarkId::new(format!("{kind}-r{requested}"), versions), &versions, |b, _| {
                b.iter_batched(
                    || meta.clone(),
                    |mut metadata| black_box(metadata.delete_versions(black_box(&requests)).expect("delete batch")),
                    batch_size,
                )
            });
        }
    }
    group.finish();
}

fn encode_file_info_msgpack(file_info: &FileInfo) -> Vec<u8> {
    let mut serializer = rmp_serde::Serializer::new(Vec::with_capacity(1024));
    file_info.serialize(&mut serializer).expect("serialize FileInfo as msgpack");
    serializer.into_inner()
}

fn inline_rpc_file_info(data_size: usize) -> FileInfo {
    let mut info = make_file_info(
        Uuid::from_u128(1),
        Uuid::from_u128(2),
        100 * 1024,
        OffsetDateTime::from_unix_timestamp(BENCH_BASE_TIME_UNIX_SECS).expect("fixture timestamp"),
    );
    info.erasure.data_blocks = 12;
    info.erasure.parity_blocks = 4;
    info.erasure.index = 1;
    info.erasure.distribution = (1..=16).collect();
    info.parts.push(ObjectPartInfo {
        number: 1,
        size: 100 * 1024,
        actual_size: 100 * 1024,
        ..Default::default()
    });
    if data_size > 0 {
        info.set_inline_data();
        info.data = Some(Bytes::from(vec![42; data_size]));
    }
    info
}

fn bench_rpc_file_info_encoding(c: &mut Criterion) {
    for (kind, data_size) in [("metadata-only", 0_usize), ("inline-9k", 9 * 1024)] {
        let file_info = inline_rpc_file_info(data_size);
        let json_bytes = serde_json::to_vec(&file_info).expect("fixture JSON").len();
        let msgpack_bytes = encode_file_info_msgpack(&file_info).len();
        let mut group = c.benchmark_group(format!("rpc_file_info_encode/{kind}"));
        group.throughput(Throughput::Bytes(u64::try_from(data_size.max(1)).expect("fixture size")));
        group.bench_function(BenchmarkId::new("json", json_bytes), |b| {
            b.iter(|| black_box(serde_json::to_string(black_box(&file_info)).expect("encode JSON")))
        });
        group.bench_function(BenchmarkId::new("msgpack", msgpack_bytes), |b| {
            b.iter(|| black_box(encode_file_info_msgpack(black_box(&file_info))))
        });
        group.bench_function(BenchmarkId::new("dual", json_bytes + msgpack_bytes), |b| {
            b.iter(|| {
                let json = serde_json::to_string(black_box(&file_info)).expect("encode JSON");
                let msgpack = encode_file_info_msgpack(black_box(&file_info));
                black_box((json, msgpack))
            })
        });
        group.finish();
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(20)
        .warm_up_time(Duration::from_millis(300))
        .measurement_time(Duration::from_secs(1));
    targets = bench_rename_data_meta_path, bench_delete_version_batches, bench_rpc_file_info_encoding
}
criterion_main!(benches);
