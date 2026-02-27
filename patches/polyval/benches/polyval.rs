//! POLYVAL benchmarks.

#![allow(missing_docs)]

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use polyval::{Polyval, universal_hash::UniversalHash};

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("polyval");

    for size in &[10, 100, 1000, 10000] {
        let buf = vec![0u8; *size];

        group.throughput(Throughput::Bytes(*size as u64));

        group.bench_function(BenchmarkId::new("update_padded", size), |b| {
            let mut polyval = Polyval::new(&Default::default());
            b.iter(|| polyval.update_padded(&buf));
        });
    }

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default();
    targets = bench
);

criterion_main!(benches);
