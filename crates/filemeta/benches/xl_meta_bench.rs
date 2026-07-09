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

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use rustfs_filemeta::{FileMeta, MetaCacheEntry, test_data::*};
use std::hint::black_box;

fn bench_create_real_xlmeta(c: &mut Criterion) {
    c.bench_function("create_real_xlmeta", |b| b.iter(|| black_box(create_real_xlmeta().unwrap())));
}

fn bench_create_complex_xlmeta(c: &mut Criterion) {
    c.bench_function("create_complex_xlmeta", |b| b.iter(|| black_box(create_complex_xlmeta().unwrap())));
}

fn bench_parse_real_xlmeta(c: &mut Criterion) {
    let data = create_real_xlmeta().unwrap();

    c.bench_function("parse_real_xlmeta", |b| b.iter(|| black_box(FileMeta::load(&data).unwrap())));
}

fn bench_parse_complex_xlmeta(c: &mut Criterion) {
    let data = create_complex_xlmeta().unwrap();

    c.bench_function("parse_complex_xlmeta", |b| b.iter(|| black_box(FileMeta::load(&data).unwrap())));
}

fn bench_serialize_real_xlmeta(c: &mut Criterion) {
    let data = create_real_xlmeta().unwrap();
    let fm = FileMeta::load(&data).unwrap();

    c.bench_function("serialize_real_xlmeta", |b| b.iter(|| black_box(fm.marshal_msg().unwrap())));
}

fn bench_serialize_complex_xlmeta(c: &mut Criterion) {
    let data = create_complex_xlmeta().unwrap();
    let fm = FileMeta::load(&data).unwrap();

    c.bench_function("serialize_complex_xlmeta", |b| b.iter(|| black_box(fm.marshal_msg().unwrap())));
}

fn bench_round_trip_real_xlmeta(c: &mut Criterion) {
    let original_data = create_real_xlmeta().unwrap();

    c.bench_function("round_trip_real_xlmeta", |b| {
        b.iter(|| {
            let fm = FileMeta::load(&original_data).unwrap();
            let serialized = fm.marshal_msg().unwrap();
            black_box(FileMeta::load(&serialized).unwrap())
        })
    });
}

fn bench_round_trip_complex_xlmeta(c: &mut Criterion) {
    let original_data = create_complex_xlmeta().unwrap();

    c.bench_function("round_trip_complex_xlmeta", |b| {
        b.iter(|| {
            let fm = FileMeta::load(&original_data).unwrap();
            let serialized = fm.marshal_msg().unwrap();
            black_box(FileMeta::load(&serialized).unwrap())
        })
    });
}

fn bench_version_stats(c: &mut Criterion) {
    let data = create_complex_xlmeta().unwrap();
    let fm = FileMeta::load(&data).unwrap();

    c.bench_function("version_stats", |b| b.iter(|| black_box(fm.get_version_stats())));
}

fn bench_into_fileinfo_realistic(c: &mut Criterion) {
    let data = create_realistic_object_xlmeta().unwrap();
    let fm = FileMeta::load(&data).unwrap();

    c.bench_function("into_fileinfo_realistic", |b| {
        b.iter(|| {
            black_box(
                fm.into_fileinfo(black_box("bucket"), black_box("object"), black_box(""), false, false, true)
                    .unwrap(),
            )
        })
    });
}

fn bench_validate_integrity(c: &mut Criterion) {
    let data = create_real_xlmeta().unwrap();
    let fm = FileMeta::load(&data).unwrap();

    c.bench_function("validate_integrity", |b| {
        b.iter(|| {
            fm.validate_integrity().unwrap();
            black_box(())
        })
    });
}

// Simulates the per-object CPU cost of a LIST page: each entry carries raw xl.meta bytes
// (cached = None), so to_fileinfo runs the full FileMeta::load + into_fileinfo path that a
// listing performs for every returned object. Reports objects/sec via Throughput::Elements.
fn bench_list_page_to_fileinfo(c: &mut Criterion) {
    const N: u64 = 10_000;
    let data = create_realistic_object_xlmeta().unwrap();
    let entries: Vec<MetaCacheEntry> = (0..N)
        .map(|i| MetaCacheEntry {
            name: format!("prefix/object-{i:08}"),
            metadata: data.clone(),
            cached: None,
            reusable: false,
        })
        .collect();

    let mut group = c.benchmark_group("list_page_to_fileinfo");
    group.throughput(Throughput::Elements(N));
    group.bench_function("realistic_10k", |b| {
        b.iter(|| {
            for entry in &entries {
                black_box(entry.to_fileinfo(black_box("bucket")).unwrap());
            }
        })
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_create_real_xlmeta,
    bench_create_complex_xlmeta,
    bench_parse_real_xlmeta,
    bench_parse_complex_xlmeta,
    bench_serialize_real_xlmeta,
    bench_serialize_complex_xlmeta,
    bench_round_trip_real_xlmeta,
    bench_round_trip_complex_xlmeta,
    bench_version_stats,
    bench_validate_integrity,
    bench_into_fileinfo_realistic,
    bench_list_page_to_fileinfo
);

criterion_main!(benches);
