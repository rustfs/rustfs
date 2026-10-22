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
use rustfs_ecstore::api::disk::{Disk, DiskAPI, DiskOption, Endpoint, WalkDirOptions, new_disk};
use rustfs_filemeta::{MetaCacheEntry, MetacacheReader, test_data::create_real_xlmeta};
use std::hint::black_box;

const OBJECTS: usize = 1000;

async fn walk_page(disk: &Disk, recursive: bool) -> Vec<MetaCacheEntry> {
    let opts = WalkDirOptions {
        bucket: "bench-bucket".to_string(),
        base_dir: "prefix".to_string(),
        recursive,
        limit: i32::try_from(OBJECTS).expect("page limit fits"),
        ..Default::default()
    };
    let (reader, mut writer) = tokio::io::duplex(4096);
    let produce = async move { disk.walk_dir(opts, &mut writer).await };
    let consume = async { MetacacheReader::new(reader).read_all().await.expect("decode listing stream") };
    let (result, entries) = tokio::join!(produce, consume);
    result.expect("production walk_dir must succeed");
    entries
}

fn bench_local_walk_dir(c: &mut Criterion) {
    let root = tempfile::tempdir().expect("create benchmark disk");
    let metadata = create_real_xlmeta().expect("create object metadata");
    let decoded = rustfs_filemeta::FileMeta::load(&metadata).expect("decode fixture");
    let data_dirs = decoded.get_data_dirs().expect("fixture data directories");
    for i in 0..OBJECTS {
        let object_dir = root.path().join("bench-bucket/prefix").join(format!("object-{i:04}"));
        std::fs::create_dir_all(&object_dir).expect("create object directory");
        for data_dir in data_dirs.iter().flatten() {
            std::fs::create_dir_all(object_dir.join(data_dir.to_string())).expect("create data directory");
        }
        std::fs::write(object_dir.join("xl.meta"), &metadata).expect("write object metadata");
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("build benchmark runtime");
    let endpoint = Endpoint::try_from(root.path().to_str().expect("UTF-8 disk path")).expect("disk endpoint");
    let disk = runtime
        .block_on(new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        ))
        .expect("open local disk");
    let mut group = c.benchmark_group("local_walk_dir");
    group.throughput(Throughput::Elements(u64::try_from(OBJECTS).expect("object count fits")));
    for (name, recursive) in [("recursive_1000_objects", true), ("non_recursive_1000_objects", false)] {
        let entries = runtime.block_on(walk_page(&disk, recursive));
        let objects: Vec<_> = entries.iter().filter(|entry| !entry.metadata.is_empty()).collect();
        assert_eq!(objects.len(), OBJECTS);
        for (i, entry) in objects.iter().enumerate() {
            assert_eq!(entry.name, format!("prefix/object-{i:04}"));
        }
        group.bench_function(name, |b| {
            b.iter(|| black_box(runtime.block_on(walk_page(&disk, recursive))));
        });
    }
    group.finish();
}

criterion_group!(benches, bench_local_walk_dir);
criterion_main!(benches);
