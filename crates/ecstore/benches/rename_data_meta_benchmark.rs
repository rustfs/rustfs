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

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rustfs_filemeta::{ErasureAlgo, FileInfo, FileMeta};
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
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));

    for &version_count in VERSION_COUNT_CASES {
        let seeded = build_meta_with_versions(version_count);
        let dst_buf = seeded.marshal_msg().expect("marshal seeded meta");
        let base_time = OffsetDateTime::from_unix_timestamp(BENCH_BASE_TIME_UNIX_SECS).expect("valid bench base timestamp");
        let replace_version_id = seeded
            .versions
            .first()
            .and_then(|v| v.header.version_id)
            .unwrap_or(Uuid::nil());

        group.bench_with_input(BenchmarkId::new("read_modify_write", version_count), &version_count, |b, _| {
            b.iter(|| {
                let mut xlmeta = FileMeta::load(black_box(&dst_buf)).expect("load dst meta");
                let search_version_id = Some(replace_version_id);
                let has_old_data_dir = xlmeta.find_unshared_data_dir_for_version(search_version_id);
                if let Some(old_data_dir) = has_old_data_dir {
                    let _ = xlmeta.data.remove(vec![search_version_id.unwrap_or_default(), old_data_dir]);
                }
                let fi = make_file_info(replace_version_id, Uuid::new_v4(), 64 * 1024, base_time + Duration::from_millis(1));
                xlmeta.add_version(fi).expect("add new version");
                let out = xlmeta.marshal_msg().expect("marshal updated meta");
                black_box(out);
            });
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

criterion_group!(benches, bench_rename_data_meta_path);
criterion_main!(benches);
