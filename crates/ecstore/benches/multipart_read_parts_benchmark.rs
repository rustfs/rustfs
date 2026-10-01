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
use rustfs_filemeta::ObjectPartInfo;
use std::hint::black_box;
use std::time::Duration;

mod storage_api;
use storage_api::multipart::{DiskAPI, DiskOption, Endpoint, new_disk};

fn bench_multipart_read_parts(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("benchmark runtime");
    let root = tempfile::tempdir().expect("benchmark disk");
    let mut endpoint = Endpoint::try_from(root.path().to_str().expect("UTF-8 path")).expect("endpoint");
    endpoint.set_pool_index(0);
    endpoint.set_set_index(0);
    endpoint.set_disk_index(0);
    let disk = runtime
        .block_on(new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        ))
        .expect("local disk");
    let bucket = "multipart-bench";
    runtime.block_on(disk.make_volume(bucket)).expect("benchmark volume");
    let upload = root.path().join(bucket).join("upload");
    std::fs::create_dir_all(&upload).expect("upload directory");
    let mut paths = Vec::with_capacity(1024);
    for number in 1..=1024 {
        let part = ObjectPartInfo {
            number,
            etag: format!("{number:032x}"),
            size: 1024,
            actual_size: 1024,
            ..Default::default()
        };
        std::fs::write(upload.join(format!("part.{number}")), b"data").expect("part data");
        std::fs::write(upload.join(format!("part.{number}.meta")), part.marshal_msg().expect("metadata")).expect("part metadata");
        paths.push(format!("upload/part.{number}.meta"));
    }

    // Measures the real DiskAPI path against warm local files. Fixture creation
    // and correctness checks stay outside the timed region; this is not a cold-disk benchmark.
    let mut group = c.benchmark_group("multipart_read_parts");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));
    for count in [1, 32, 256, 1024] {
        let paths = &paths[..count];
        let parts = runtime.block_on(disk.read_parts(bucket, paths)).expect("read parts");
        assert_eq!(parts.len(), count);
        assert!(
            parts
                .iter()
                .enumerate()
                .all(|(i, part)| part.number == i + 1 && part.error.is_none())
        );
        group.throughput(Throughput::Elements(u64::try_from(count).expect("part count")));
        group.bench_with_input(BenchmarkId::from_parameter(count), &paths, |b, paths| {
            b.iter(|| {
                black_box(
                    runtime
                        .block_on(disk.read_parts(bucket, black_box(paths)))
                        .expect("read parts"),
                )
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_multipart_read_parts);
criterion_main!(benches);
