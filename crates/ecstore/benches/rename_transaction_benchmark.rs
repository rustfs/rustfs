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
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use rustfs_filemeta::{FileInfo, FileMeta};
use std::{fs, hint::black_box, sync::Arc, time::Duration};
use tempfile::TempDir;
use time::OffsetDateTime;
use tokio::runtime::Runtime;
use uuid::Uuid;

mod storage_api;

use storage_api::rename_transaction::{DiskAPI, Endpoint, LocalDisk, RUSTFS_META_TMP_BUCKET, STORAGE_FORMAT_FILE};

struct RenameCase {
    _root: TempDir,
    disk: Arc<LocalDisk>,
    incoming: Option<FileInfo>,
}

fn metadata(file_info: FileInfo) -> Vec<u8> {
    let mut metadata = FileMeta::new();
    metadata
        .add_version(file_info)
        .expect("benchmark metadata should accept version");
    metadata.marshal_msg().expect("benchmark metadata should encode")
}

fn file_info(name: &str, contents: &'static [u8]) -> FileInfo {
    FileInfo {
        name: name.to_string(),
        version_id: Some(Uuid::new_v4()),
        data: Some(Bytes::from_static(contents)),
        size: contents.len() as i64,
        mod_time: Some(OffsetDateTime::now_utc()),
        ..Default::default()
    }
}

fn setup_case(runtime: &Runtime, overwrite: bool) -> RenameCase {
    let root = tempfile::tempdir().expect("benchmark temp dir should be created");
    let endpoint = Endpoint::try_from(root.path().to_str().expect("benchmark path should be utf8"))
        .expect("benchmark endpoint should parse");
    let disk = Arc::new(
        runtime
            .block_on(LocalDisk::new(&endpoint, false))
            .expect("benchmark disk should initialize"),
    );
    fs::create_dir_all(root.path().join(RUSTFS_META_TMP_BUCKET).join("source")).expect("benchmark source should be created");
    fs::create_dir_all(root.path().join("bucket").join("object")).expect("benchmark destination should be created");

    let incoming = file_info("object", b"new");
    fs::write(
        root.path()
            .join(RUSTFS_META_TMP_BUCKET)
            .join("source")
            .join(STORAGE_FORMAT_FILE),
        metadata(incoming.clone()),
    )
    .expect("benchmark source metadata should be written");
    if overwrite {
        fs::write(
            root.path().join("bucket").join("object").join(STORAGE_FORMAT_FILE),
            metadata(file_info("object", b"old")),
        )
        .expect("benchmark destination metadata should be written");
    }

    RenameCase {
        _root: root,
        disk,
        incoming: Some(incoming),
    }
}

fn bench_rename_transaction(c: &mut Criterion) {
    let runtime = Runtime::new().expect("benchmark runtime should initialize");
    let mut group = c.benchmark_group("rename_transaction");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(5));

    for overwrite in [false, true] {
        let case_name = if overwrite { "overwrite" } else { "new_object" };
        group.bench_with_input(BenchmarkId::new("legacy", case_name), &overwrite, |b, overwrite| {
            b.iter_batched_ref(
                || setup_case(&runtime, *overwrite),
                |case| {
                    runtime.block_on(async {
                        black_box(
                            case.disk
                                .rename_data(
                                    RUSTFS_META_TMP_BUCKET,
                                    "source",
                                    case.incoming.take().expect("benchmark input should be present"),
                                    "bucket",
                                    "object",
                                )
                                .await
                                .expect("legacy rename should succeed"),
                        );
                    });
                },
                BatchSize::PerIteration,
            );
        });
        group.bench_with_input(BenchmarkId::new("transaction", case_name), &overwrite, |b, overwrite| {
            b.iter_batched_ref(
                || setup_case(&runtime, *overwrite),
                |case| {
                    runtime.block_on(async {
                        black_box(
                            case.disk
                                .rename_data_with_rollback(
                                    RUSTFS_META_TMP_BUCKET,
                                    "source",
                                    case.incoming.take().expect("benchmark input should be present"),
                                    "bucket",
                                    "object",
                                    Uuid::new_v4(),
                                )
                                .await
                                .expect("transaction rename should succeed"),
                        );
                    });
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_rename_transaction);
criterion_main!(benches);
