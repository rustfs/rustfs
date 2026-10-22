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

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rustfs_filemeta::{FileInfo, FileMeta, MetaCacheEntry, ObjectPartInfo, test_data::*};
use std::hint::black_box;
use std::time::Duration;
use time::OffsetDateTime;
use uuid::Uuid;

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

fn bench_delete_inline_versions(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete_inline_versions");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(2));
    for count in [1000_u32, 2000, 10000] {
        let mut meta = FileMeta::new();
        let mut inline = vec![1];
        rmp::encode::write_map_len(&mut inline, count).expect("inline map header");
        let payload = vec![42; 9 * 1024];
        let mut requests = Vec::new();
        for id in 1..=count {
            let version = Uuid::from_u128(u128::from(id));
            let mut fi = FileInfo::new("object", 1, 0);
            fi.version_id = Some(version);
            fi.data_dir = Some(Uuid::from_u128(u128::from(count) + u128::from(id)));
            fi.mod_time = Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1_700_000_000 + i64::from(id)));
            fi.erasure.index = 1;
            fi.size = i64::try_from(payload.len()).expect("payload length");
            fi.parts.push(ObjectPartInfo {
                number: 1,
                size: payload.len(),
                actual_size: fi.size,
                ..Default::default()
            });
            fi.set_inline_data();
            meta.add_version(fi).expect("seed version metadata");
            rmp::encode::write_str(&mut inline, &version.to_string()).expect("inline key");
            rmp::encode::write_bin(&mut inline, &payload).expect("inline body");
            if id <= 1000 {
                requests.push(FileInfo {
                    version_id: Some(version),
                    ..Default::default()
                });
            }
        }
        meta.data.update(&inline);
        group.throughput(Throughput::Elements(u64::try_from(requests.len()).expect("batch size")));
        group.bench_with_input(BenchmarkId::new("batch", count), &count, |b, _| {
            b.iter_batched(
                || meta.clone(),
                |mut meta| black_box(meta.delete_versions(black_box(&requests)).expect("batch delete")),
                BatchSize::LargeInput,
            );
        });
        if count <= 2000 {
            group.bench_with_input(BenchmarkId::new("sequential", count), &count, |b, _| {
                b.iter_batched(
                    || meta.clone(),
                    |mut meta| {
                        for request in black_box(&requests) {
                            if let Some(dir) = meta.delete_version(request).expect("sequential delete") {
                                meta.data
                                    .remove(vec![request.version_id.expect("explicit version"), dir])
                                    .expect("remove inline body");
                            }
                        }
                        black_box(meta);
                    },
                    BatchSize::LargeInput,
                );
            });
        }
    }
    group.finish();
}

fn bench_metacache_stream_page(c: &mut Criterion) {
    let xl_bytes = create_real_xlmeta().expect("create_real_xlmeta failed");
    let mut entries = Vec::with_capacity(2000);
    for i in 0..1000 {
        entries.push(MetaCacheEntry {
            name: format!("prefix/subprefix/object-{i:04}"),
            metadata: xl_bytes.clone(),
            cached: None,
            reusable: false,
        });
        entries.push(MetaCacheEntry {
            name: format!("prefix/subprefix/object-{i:04}/"),
            metadata: Vec::new(),
            cached: None,
            reusable: false,
        });
    }

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build current_thread runtime");

    let encoded_stream = rt.block_on(async {
        let mut cursor = std::io::Cursor::new(Vec::with_capacity(512 * 1024));
        let mut writer = rustfs_filemeta::MetacacheWriter::new(&mut cursor);
        writer.write(&entries).await.expect("encode entries");
        writer.close().await.expect("close stream");
        cursor.into_inner()
    });

    let mut group = c.benchmark_group("metacache_stream_page");

    group.bench_function("reader_read_all/1000_objects", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut reader =
                    rustfs_filemeta::MetacacheReader::new(std::io::Cursor::new(black_box(encoded_stream.as_slice())));
                let out = reader.read_all().await.expect("read_all");
                black_box(out.len());
            });
        });
    });

    group.bench_function("duplex_4k_write_and_read/1000_objects", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (rd, mut wr) = tokio::io::duplex(4096);
                let entries_ref = &entries;
                let writer_fut = async move {
                    let mut out = rustfs_filemeta::MetacacheWriter::new(&mut wr);
                    for chunk in entries_ref.chunks(64) {
                        out.write(chunk).await.expect("write");
                    }
                    out.close().await.expect("close");
                };
                let reader_fut = async move {
                    let mut reader = rustfs_filemeta::MetacacheReader::new(rd);
                    let out = reader.read_all().await.expect("read_all");
                    black_box(out.len());
                };
                tokio::join!(writer_fut, reader_fut);
            });
        });
    });

    group.bench_function("duplex_4k_write_obj_and_read/1000_objects", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (rd, mut wr) = tokio::io::duplex(4096);
                let entries_ref = &entries;
                let writer_fut = async move {
                    let mut out = rustfs_filemeta::MetacacheWriter::new(&mut wr);
                    for entry in entries_ref {
                        out.write_obj(entry).await.expect("write_obj");
                    }
                    out.close().await.expect("close");
                };
                let reader_fut = async move {
                    let mut reader = rustfs_filemeta::MetacacheReader::new(rd);
                    let out = reader.read_all().await.expect("read_all");
                    black_box(out.len());
                };
                tokio::join!(writer_fut, reader_fut);
            });
        });
    });

    let temp_dir = std::env::temp_dir().join(format!("rustfs_walk_bench_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(&temp_dir).expect("create bench dir");
    let mut obj_dirs = Vec::with_capacity(1000);
    let mut obj_names = Vec::with_capacity(1000);
    let sample_meta = FileMeta::load(&xl_bytes).expect("load sample meta");
    let sample_data_dir = sample_meta
        .get_data_dirs()
        .expect("get data dirs")
        .into_iter()
        .flatten()
        .next()
        .unwrap_or_else(Uuid::new_v4);
    for i in 0..1000 {
        let name = format!("prefix/subprefix/object-{i:04}");
        let obj_dir = temp_dir.join(format!("object-{i:04}"));
        std::fs::create_dir_all(obj_dir.join(sample_data_dir.to_string())).expect("create data_dir");
        std::fs::write(obj_dir.join("xl.meta"), &xl_bytes).expect("write xl.meta");
        obj_dirs.push(obj_dir);
        obj_names.push(name);
    }

    let mt_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("build multi_thread runtime");

    group.bench_function("walk_dir_1000_objects", |b| {
        b.iter(|| {
            mt_rt.block_on(async {
                let (rd, mut wr) = tokio::io::duplex(4096);
                let dirs = &obj_dirs;
                let names = &obj_names;
                let writer_fut = async move {
                    let mut out = rustfs_filemeta::MetacacheWriter::new(&mut wr);
                    for (dir_chunk, name_chunk) in dirs.chunks(32).zip(names.chunks(32)) {
                        let dir_chunk = dir_chunk.to_vec();
                        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
                        drop(tokio::task::spawn_blocking(move || {
                            let mut chunk = Vec::with_capacity(dir_chunk.len());
                            let mut chunk_started = std::time::Instant::now();
                            for obj_dir in &dir_chunk {
                                if tx.is_closed() {
                                    break;
                                }
                                let mut file = std::fs::File::open(obj_dir.join("xl.meta")).expect("open");
                                let mut initial_buf = [0u8; 4096];
                                let n = std::io::Read::read(&mut file, &mut initial_buf).expect("read");
                                let bytes = initial_buf[..n].to_vec();
                                let file_meta = FileMeta::load(&bytes).ok();
                                let mut data_dirs = Vec::new();
                                if let Some(meta) = file_meta.as_ref()
                                    && let Ok(dirs) = meta.get_data_dirs()
                                {
                                    data_dirs.extend(dirs.into_iter().flatten());
                                    data_dirs.sort_unstable();
                                    data_dirs.dedup();
                                }
                                let counts = file_meta.as_ref().is_none_or(|m| {
                                    m.into_fileinfo_without_part_checksums("", "", "", false, true).map_or_else(
                                        |_| !m.all_hidden(true),
                                        |latest| !latest.deleted && !latest.tier_free_version(),
                                    )
                                });
                                let mut dir = std::fs::read_dir(obj_dir).expect("read_dir");
                                let mut has_child = false;
                                for _ in 0..256 {
                                    let Some(entry) = dir.next() else { break };
                                    let entry = entry.expect("entry");
                                    let fname = entry.file_name();
                                    let ft = entry.file_type().expect("ft");
                                    if ft.is_dir() {
                                        let is_dd = fname.len() == uuid::fmt::Hyphenated::LENGTH
                                            && fname
                                                .to_str()
                                                .and_then(|s| {
                                                    let parsed = Uuid::try_parse(s).ok()?;
                                                    let mut buf = [0u8; uuid::fmt::Hyphenated::LENGTH];
                                                    (parsed.hyphenated().encode_lower(&mut buf) == s
                                                        && data_dirs.binary_search(&parsed).is_ok())
                                                    .then_some(())
                                                })
                                                .is_some();
                                        if !is_dd {
                                            has_child = true;
                                            break;
                                        }
                                    }
                                }
                                chunk.push((bytes, counts, has_child));
                                if has_child {
                                    let _ = tx.send(chunk);
                                    return;
                                }
                                if chunk_started.elapsed() >= std::time::Duration::from_millis(20) {
                                    let flushed = std::mem::replace(&mut chunk, Vec::with_capacity(dir_chunk.len()));
                                    if tx.send(flushed).is_err() {
                                        return;
                                    }
                                    chunk_started = std::time::Instant::now();
                                }
                            }
                            if !chunk.is_empty() {
                                let _ = tx.send(chunk);
                            }
                        }));
                        let mut name_iter = name_chunk.iter();
                        while let Some(subchunk) = rx.recv().await {
                            for (bytes, _counts, _has_child) in subchunk {
                                let name = name_iter.next().expect("name");
                                out.write_obj(&MetaCacheEntry {
                                    name: name.clone(),
                                    metadata: bytes,
                                    cached: None,
                                    reusable: false,
                                })
                                .await
                                .expect("write_obj");
                                out.write_obj(&MetaCacheEntry {
                                    name: format!("{name}/"),
                                    metadata: Vec::new(),
                                    cached: None,
                                    reusable: false,
                                })
                                .await
                                .expect("write_dir");
                            }
                        }
                    }
                    out.close().await.expect("close");
                };
                let reader_fut = async move {
                    let mut reader = rustfs_filemeta::MetacacheReader::new(rd);
                    let out = reader.read_all().await.expect("read_all");
                    black_box(out.len());
                };
                tokio::join!(writer_fut, reader_fut);
            });
        });
    });

    let _ = std::fs::remove_dir_all(&temp_dir);

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
    bench_list_page_to_fileinfo,
    bench_delete_inline_versions,
    bench_metacache_stream_page
);

criterion_main!(benches);
