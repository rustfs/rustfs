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
use rustfs_zip::{
    ArchiveLimits, CompressionFormat, CompressionLevel, ZipWriteOptions, create_zip_with_options, extract_tar_entries,
    extract_zip_to_path_with_limits, extract_zip_with_limits,
};
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::tempdir;
use tokio::runtime::Builder;
use tokio_tar::{Builder as TarBuilder, Header};
use zip::ZipArchive;

fn build_runtime() -> tokio::runtime::Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime for rustfs-zip benchmarks")
}

async fn build_tar_payload(entry_count: usize, payload_size: usize) -> Vec<u8> {
    let sink = tokio::io::duplex(64 * 1024);
    let (writer, mut reader) = sink;
    let write_task = tokio::spawn(async move {
        let mut builder = TarBuilder::new(writer);
        let payload = vec![b'a'; payload_size];
        for index in 0..entry_count {
            let mut header = Header::new_gnu();
            header.set_size(payload.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder
                .append_data(&mut header, format!("entry-{index}.txt"), &payload[..])
                .await
                .expect("append tar benchmark entry");
        }
        builder.finish().await.expect("finish tar benchmark archive");
    });

    let mut output = Vec::new();
    tokio::io::copy(&mut reader, &mut output)
        .await
        .expect("read tar benchmark archive");
    write_task.await.expect("join tar writer task");
    output
}

async fn build_compressed_tar_payload(format: CompressionFormat, entry_count: usize, payload_size: usize) -> Vec<u8> {
    let tar_payload = build_tar_payload(entry_count, payload_size).await;
    rustfs_zip::Compressor::new(format)
        .compress(&tar_payload)
        .await
        .expect("compress tar benchmark payload")
}

fn bench_tar_family_extract(c: &mut Criterion) {
    let runtime = build_runtime();
    let mut group = c.benchmark_group("zip_tar_family_extract");

    for (name, format, entry_count, payload_size) in [
        ("tar_gzip_small_many", CompressionFormat::Gzip, 64usize, 256usize),
        ("tar_zstd_medium", CompressionFormat::Zstd, 16usize, 16 * 1024usize),
    ] {
        let payload = runtime.block_on(build_compressed_tar_payload(format, entry_count, payload_size));
        group.throughput(Throughput::Bytes(payload.len() as u64));
        group.bench_with_input(BenchmarkId::new(name, payload.len()), &payload, |b, payload| {
            b.iter(|| {
                runtime.block_on(async {
                    let seen = Arc::new(AtomicUsize::new(0));
                    let seen_ref = Arc::clone(&seen);
                    extract_tar_entries(std::io::Cursor::new(payload.clone()), format, move |_entry| {
                        let seen_ref = Arc::clone(&seen_ref);
                        async move {
                            seen_ref.fetch_add(1, Ordering::Relaxed);
                            Ok(())
                        }
                    })
                    .await
                    .expect("extract tar benchmark payload");
                    black_box(seen.load(Ordering::Relaxed));
                });
            });
        });
    }

    group.finish();
}

fn bench_zip_helper_round_trip(c: &mut Criterion) {
    let runtime = build_runtime();
    let mut group = c.benchmark_group("zip_helper_round_trip");

    let zip_matrix = [
        ("stored_flat_32x128", CompressionLevel::Fastest, 32usize, 128usize, "flat"),
        ("stored_nested_32x256", CompressionLevel::Fastest, 32usize, 256usize, "nested"),
        ("stored_flat_256x128", CompressionLevel::Fastest, 256usize, 128usize, "flat"),
        ("deflated_flat_32x1k", CompressionLevel::Best, 32usize, 1024usize, "flat"),
        ("deflated_nested_256x1k", CompressionLevel::Best, 256usize, 1024usize, "nested"),
        ("deflated_deep_1024x4k", CompressionLevel::Best, 1024usize, 4 * 1024usize, "deep"),
    ];

    for (name, compression_level, file_count, payload_size, layout) in zip_matrix {
        let files = (0..file_count)
            .map(|index| {
                let path = match layout {
                    "flat" => format!("file-{index}.txt"),
                    "nested" => format!("batch-{}/file-{index}.txt", index % 8),
                    "deep" => format!("lvl1/lvl2-{}/lvl3-{}/file-{index}.txt", index % 16, index % 32),
                    _ => format!("file-{index}.txt"),
                };
                (path, vec![b'b'; payload_size])
            })
            .collect::<Vec<_>>();
        let total_bytes = (file_count * payload_size) as u64;
        group.throughput(Throughput::Bytes(total_bytes));

        group.bench_with_input(BenchmarkId::new(name, total_bytes), &files, |b, files| {
            b.iter(|| {
                let temp = tempdir().expect("create benchmark tempdir");
                let zip_path = temp.path().join("archive.zip");
                let extract_path = temp.path().join("extract");
                runtime.block_on(async {
                    create_zip_with_options(
                        &zip_path,
                        files.clone(),
                        ZipWriteOptions {
                            compression_level,
                            create_directory_entries: true,
                        },
                    )
                    .await
                    .expect("create zip benchmark archive");

                    let entries = extract_zip_with_limits(&zip_path, &extract_path, ArchiveLimits::default())
                        .await
                        .expect("extract zip benchmark archive");
                    black_box(entries.len());
                });
            });
        });
    }

    group.finish();
}

fn bench_zip_helper_hotspot_breakdown(c: &mut Criterion) {
    let runtime = build_runtime();
    let mut group = c.benchmark_group("zip_helper_hotspot_breakdown");
    let files = (0..32)
        .map(|index| (format!("batch/file-{index}.txt"), vec![b'c'; 256]))
        .collect::<Vec<_>>();
    let total_bytes = (32 * 256) as u64;
    group.throughput(Throughput::Bytes(total_bytes));

    group.bench_function("fs_setup_cleanup_only", |b| {
        b.iter(|| {
            let temp = tempdir().expect("create benchmark tempdir");
            let zip_path = temp.path().join("archive.zip");
            let extract_path = temp.path().join("extract");
            black_box((zip_path, extract_path));
        });
    });

    group.bench_function("zip_create_only_stored_small", |b| {
        b.iter(|| {
            let temp = tempdir().expect("create benchmark tempdir");
            let zip_path = temp.path().join("archive.zip");
            runtime.block_on(async {
                create_zip_with_options(
                    &zip_path,
                    files.clone(),
                    ZipWriteOptions {
                        compression_level: CompressionLevel::Fastest,
                        create_directory_entries: true,
                    },
                )
                .await
                .expect("create zip benchmark archive");
            });
        });
    });

    let payload_for_extract = {
        let temp = tempdir().expect("create benchmark tempdir");
        let zip_path = temp.path().join("archive.zip");
        runtime.block_on(async {
            create_zip_with_options(
                &zip_path,
                files.clone(),
                ZipWriteOptions {
                    compression_level: CompressionLevel::Fastest,
                    create_directory_entries: true,
                },
            )
            .await
            .expect("prepare zip benchmark extract payload");
        });
        std::fs::read(&zip_path).expect("read benchmark zip payload")
    };

    group.bench_function("zip_extract_only_stored_small", |b| {
        b.iter(|| {
            let temp = tempdir().expect("create benchmark tempdir");
            let zip_path = temp.path().join("archive.zip");
            let extract_path = temp.path().join("extract");
            std::fs::write(&zip_path, &payload_for_extract).expect("write benchmark zip payload");
            runtime.block_on(async {
                let entries = extract_zip_with_limits(&zip_path, &extract_path, ArchiveLimits::default())
                    .await
                    .expect("extract zip benchmark archive");
                black_box(entries.len());
            });
        });
    });

    group.bench_function("zip_extract_only_stored_small_summary_only", |b| {
        b.iter(|| {
            let temp = tempdir().expect("create benchmark tempdir");
            let zip_path = temp.path().join("archive.zip");
            let extract_path = temp.path().join("extract");
            std::fs::write(&zip_path, &payload_for_extract).expect("write benchmark zip payload");
            runtime.block_on(async {
                let summary = extract_zip_to_path_with_limits(&zip_path, &extract_path, ArchiveLimits::default())
                    .await
                    .expect("extract zip benchmark summary path");
                black_box(summary.entry_count);
            });
        });
    });

    group.bench_function("zip_reader_only_stored_small", |b| {
        b.iter(|| {
            let cursor = std::io::Cursor::new(payload_for_extract.clone());
            let mut archive = ZipArchive::new(cursor).expect("open zip archive for reader-only benchmark");
            let mut total_bytes = 0usize;
            for index in 0..archive.len() {
                let mut zip_file = archive.by_index(index).expect("access zip entry by index");
                let enclosed_name = zip_file
                    .enclosed_name()
                    .expect("resolve enclosed zip entry name")
                    .to_string_lossy()
                    .replace('\\', "/");
                let size = zip_file.size();
                assert!(!enclosed_name.is_empty(), "zip reader-only benchmark expects non-empty names");
                assert!(
                    size <= ArchiveLimits::default().max_entry_size,
                    "zip reader-only benchmark expects small entries"
                );
                if !zip_file.is_dir() {
                    let mut sink = [0_u8; 256];
                    let bytes_read =
                        std::io::Read::read(&mut zip_file, &mut sink).expect("read zip entry payload for reader-only benchmark");
                    total_bytes += bytes_read;
                }
            }
            black_box(total_bytes);
        });
    });

    group.bench_function("file_write_only_stored_small", |b| {
        b.iter(|| {
            let temp = tempdir().expect("create benchmark tempdir");
            let extract_path = temp.path().join("extract");
            std::fs::create_dir_all(&extract_path).expect("create extract dir for file-write-only benchmark");
            let mut total_bytes = 0usize;
            for index in 0..32 {
                let path = extract_path.join(format!("file-{index}.txt"));
                std::fs::write(&path, [b'c'; 256]).expect("write small file for file-write-only benchmark");
                total_bytes += 256;
            }
            black_box(total_bytes);
        });
    });

    group.finish();
}

fn build_object_archive_files(
    metadata_count: usize,
    metadata_size: usize,
    payload_count: usize,
    payload_size: usize,
) -> Vec<(String, Vec<u8>)> {
    let mut files = Vec::with_capacity(metadata_count * 2 + payload_count);

    for index in 0..metadata_count {
        let key_prefix = format!(
            "bucket-a/shard-{}/tenant-{}/dataset-{}/object-{index:04}",
            index % 8,
            index % 16,
            index % 32
        );
        files.push((
            format!("{key_prefix}/meta.json"),
            format!(
                "{{\"key\":\"object-{index:04}\",\"etag\":\"{:032x}\",\"size\":{},\"content_type\":\"application/octet-stream\"}}",
                index,
                payload_size
            )
            .into_bytes(),
        ));
        files.push((format!("{key_prefix}/tags.txt"), vec![b'm'; metadata_size]));
    }

    for index in 0..payload_count {
        let payload_prefix = format!(
            "bucket-a/shard-{}/tenant-{}/dataset-{}/object-{index:04}",
            index % 8,
            index % 16,
            index % 32
        );
        files.push((format!("{payload_prefix}/part-00000.bin"), vec![b'p'; payload_size]));
    }

    files
}

fn bench_zip_object_archive_extract(c: &mut Criterion) {
    let runtime = build_runtime();
    let mut group = c.benchmark_group("zip_object_archive_extract");

    for (name, compression_level, metadata_count, metadata_size, payload_count, payload_size) in [
        (
            "stored_metadata_heavy_384m_24p",
            CompressionLevel::Fastest,
            384usize,
            192usize,
            24usize,
            32 * 1024usize,
        ),
        (
            "deflated_mixed_192m_32p",
            CompressionLevel::Best,
            192usize,
            256usize,
            32usize,
            64 * 1024usize,
        ),
    ] {
        let files = build_object_archive_files(metadata_count, metadata_size, payload_count, payload_size);
        let total_bytes = files.iter().map(|(_, payload)| payload.len() as u64).sum::<u64>();
        let payload = {
            let temp = tempdir().expect("create benchmark tempdir");
            let zip_path = temp.path().join("object-archive.zip");
            runtime.block_on(async {
                create_zip_with_options(
                    &zip_path,
                    files.clone(),
                    ZipWriteOptions {
                        compression_level,
                        create_directory_entries: true,
                    },
                )
                .await
                .expect("create object archive benchmark payload");
            });
            std::fs::read(&zip_path).expect("read object archive benchmark payload")
        };

        group.throughput(Throughput::Bytes(total_bytes));
        group.bench_function(BenchmarkId::new("extract_full", name), |b| {
            b.iter(|| {
                let temp = tempdir().expect("create benchmark tempdir");
                let zip_path = temp.path().join("archive.zip");
                let extract_path = temp.path().join("extract");
                std::fs::write(&zip_path, &payload).expect("write object archive benchmark payload");
                runtime.block_on(async {
                    let entries = extract_zip_with_limits(&zip_path, &extract_path, ArchiveLimits::default())
                        .await
                        .expect("extract object archive benchmark payload");
                    black_box(entries.len());
                });
            });
        });

        group.bench_function(BenchmarkId::new("extract_summary_only", name), |b| {
            b.iter(|| {
                let temp = tempdir().expect("create benchmark tempdir");
                let zip_path = temp.path().join("archive.zip");
                let extract_path = temp.path().join("extract");
                std::fs::write(&zip_path, &payload).expect("write object archive benchmark payload");
                runtime.block_on(async {
                    let summary = extract_zip_to_path_with_limits(&zip_path, &extract_path, ArchiveLimits::default())
                        .await
                        .expect("extract object archive benchmark summary");
                    black_box(summary.file_count);
                });
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_tar_family_extract,
    bench_zip_helper_round_trip,
    bench_zip_helper_hotspot_breakdown,
    bench_zip_object_archive_extract
);
criterion_main!(benches);
