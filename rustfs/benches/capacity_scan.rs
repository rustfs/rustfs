use criterion::{Criterion, criterion_group, criterion_main};
use rustfs::capacity::{CapacityDiskRef, scan_used_capacity_disks};
use std::fs;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tempfile::TempDir;

const EXACT_FILE_SIZE: usize = 4 * 1024;
const SAMPLED_FILE_SIZE: usize = 1;
const DEFAULT_SAMPLE_TRIGGER_FILE_COUNT: usize = 202_048;

#[derive(Clone, Copy)]
struct DiskSpec {
    file_count: usize,
    file_size: usize,
}

struct CapacityScanFixture {
    _dirs: Vec<TempDir>,
    disks: Vec<CapacityDiskRef>,
}

impl CapacityScanFixture {
    fn new(specs: &[DiskSpec]) -> Self {
        let mut dirs = Vec::with_capacity(specs.len());
        let mut disks = Vec::with_capacity(specs.len());

        for (idx, spec) in specs.iter().enumerate() {
            let dir = TempDir::new().expect("create temp dir");
            populate_files(dir.path(), spec.file_count, spec.file_size).expect("populate files");
            disks.push(CapacityDiskRef {
                endpoint: format!("bench-disk-{idx}"),
                drive_path: dir.path().to_string_lossy().into_owned(),
            });
            dirs.push(dir);
        }

        Self { _dirs: dirs, disks }
    }
}

fn populate_files(root: &Path, file_count: usize, file_size: usize) -> std::io::Result<()> {
    let payload = vec![b'x'; file_size];
    let shard_count = (file_count / 512).clamp(1, 256);

    for shard_idx in 0..shard_count {
        fs::create_dir_all(root.join(format!("bucket-{shard_idx:03}")))?;
    }

    for file_idx in 0..file_count {
        let subdir = root.join(format!("bucket-{:03}", file_idx % shard_count));
        let file_path: PathBuf = subdir.join(format!("object-{file_idx:08}.bin"));
        fs::write(file_path, &payload)?;
    }

    Ok(())
}

fn bench_capacity_scan(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create runtime");

    let exact_fixture = CapacityScanFixture::new(&[DiskSpec {
        file_count: 10_000,
        file_size: EXACT_FILE_SIZE,
    }]);

    let sampled_fixture = CapacityScanFixture::new(&[DiskSpec {
        file_count: DEFAULT_SAMPLE_TRIGGER_FILE_COUNT,
        file_size: SAMPLED_FILE_SIZE,
    }]);

    let multi_disk_fixture = CapacityScanFixture::new(&[
        DiskSpec {
            file_count: 4_000,
            file_size: 1024,
        },
        DiskSpec {
            file_count: 6_000,
            file_size: 2048,
        },
        DiskSpec {
            file_count: 8_000,
            file_size: 4096,
        },
        DiskSpec {
            file_count: 10_000,
            file_size: 1024,
        },
    ]);

    let mut exact_group = c.benchmark_group("capacity_scan_exact");
    exact_group.sample_size(10);
    exact_group.measurement_time(Duration::from_secs(10));
    exact_group.bench_function("single_disk_10k_4k", |b| {
        b.iter(|| {
            let summary = runtime
                .block_on(scan_used_capacity_disks(black_box(&exact_fixture.disks)))
                .expect("exact scan");
            black_box(summary);
        });
    });
    exact_group.finish();

    let mut sampled_group = c.benchmark_group("capacity_scan_sampled");
    sampled_group.sample_size(10);
    sampled_group.measurement_time(Duration::from_secs(10));
    sampled_group.bench_function("single_disk_202k_1b", |b| {
        b.iter(|| {
            let summary = runtime
                .block_on(scan_used_capacity_disks(black_box(&sampled_fixture.disks)))
                .expect("sampled scan");
            black_box(summary);
        });
    });
    sampled_group.finish();

    let mut multi_disk_group = c.benchmark_group("capacity_scan_multi_disk");
    multi_disk_group.sample_size(10);
    multi_disk_group.measurement_time(Duration::from_secs(10));
    multi_disk_group.bench_function("four_disks_mixed_exact", |b| {
        b.iter(|| {
            let summary = runtime
                .block_on(scan_used_capacity_disks(black_box(&multi_disk_fixture.disks)))
                .expect("multi-disk scan");
            black_box(summary);
        });
    });
    multi_disk_group.finish();
}

criterion_group!(benches, bench_capacity_scan);
criterion_main!(benches);
