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

//! Reed-Solomon SIMD performance analysis benchmarks
//!
//! This benchmark analyzes the performance characteristics of the SIMD Reed-Solomon implementation
//! across different data sizes, shard configurations, and usage patterns.
//!
//! ## Running Performance Analysis
//!
//! ```bash
//! # Run all SIMD performance tests
//! cargo bench --bench comparison_benchmark
//!
//! # Generate detailed performance report
//! cargo bench --bench comparison_benchmark -- --save-baseline simd_analysis
//!
//! # Run specific test categories
//! cargo bench --bench comparison_benchmark encode_analysis
//! cargo bench --bench comparison_benchmark decode_analysis
//! cargo bench --bench comparison_benchmark shard_analysis
//! ```

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rustfs_ecstore::erasure_coding::Erasure;
use std::hint::black_box;
use std::time::Duration;

/// Performance test data configuration
struct TestData {
    data: Vec<u8>,
    size_name: &'static str,
}

impl TestData {
    fn new(size: usize, size_name: &'static str) -> Self {
        let data = (0..size).map(|i| (i % 256) as u8).collect();
        Self { data, size_name }
    }
}

/// Generate different sized test datasets for performance analysis
fn generate_test_datasets() -> Vec<TestData> {
    vec![
        TestData::new(1024, "1KB"),            // Small data
        TestData::new(8 * 1024, "8KB"),        // Medium-small data
        TestData::new(64 * 1024, "64KB"),      // Medium data
        TestData::new(256 * 1024, "256KB"),    // Medium-large data
        TestData::new(1024 * 1024, "1MB"),     // Large data
        TestData::new(4 * 1024 * 1024, "4MB"), // Extra large data
    ]
}

/// SIMD encoding performance analysis
fn bench_encode_analysis(c: &mut Criterion) {
    let datasets = generate_test_datasets();
    let configs = vec![
        (4, 2, "4+2"), // Common configuration
        (6, 3, "6+3"), // 50% redundancy
        (8, 4, "8+4"), // 50% redundancy, more shards
    ];

    for dataset in &datasets {
        for (data_shards, parity_shards, config_name) in &configs {
            let test_name = format!("{}_{}_{}", dataset.size_name, config_name, "simd");

            let mut group = c.benchmark_group("encode_analysis");
            group.throughput(Throughput::Bytes(dataset.data.len() as u64));
            group.sample_size(20);
            group.measurement_time(Duration::from_secs(10));

            // Test SIMD encoding performance
            match Erasure::new(*data_shards, *parity_shards, dataset.data.len()).encode_data(&dataset.data) {
                Ok(_) => {
                    group.bench_with_input(
                        BenchmarkId::new("simd_encode", &test_name),
                        &(&dataset.data, *data_shards, *parity_shards),
                        |b, (data, data_shards, parity_shards)| {
                            let erasure = Erasure::new(*data_shards, *parity_shards, data.len());
                            b.iter(|| {
                                let shards = erasure.encode_data(black_box(data)).unwrap();
                                black_box(shards);
                            });
                        },
                    );
                }
                Err(e) => {
                    println!("⚠️  Skipping test {test_name} - configuration not supported: {e}");
                }
            }
            group.finish();
        }
    }
}

/// SIMD decoding performance analysis
fn bench_decode_analysis(c: &mut Criterion) {
    let datasets = generate_test_datasets();
    let configs = vec![(4, 2, "4+2"), (6, 3, "6+3"), (8, 4, "8+4")];

    for dataset in &datasets {
        for (data_shards, parity_shards, config_name) in &configs {
            let test_name = format!("{}_{}_{}", dataset.size_name, config_name, "simd");
            let erasure = Erasure::new(*data_shards, *parity_shards, dataset.data.len());

            // Pre-encode data - check if this configuration is supported
            match erasure.encode_data(&dataset.data) {
                Ok(encoded_shards) => {
                    let mut group = c.benchmark_group("decode_analysis");
                    group.throughput(Throughput::Bytes(dataset.data.len() as u64));
                    group.sample_size(20);
                    group.measurement_time(Duration::from_secs(10));

                    group.bench_with_input(
                        BenchmarkId::new("simd_decode", &test_name),
                        &(&encoded_shards, *data_shards, *parity_shards),
                        |b, (shards, data_shards, parity_shards)| {
                            let erasure = Erasure::new(*data_shards, *parity_shards, dataset.data.len());
                            b.iter(|| {
                                // Simulate maximum recoverable data loss
                                let mut shards_opt: Vec<Option<Vec<u8>>> =
                                    shards.iter().map(|shard| Some(shard.to_vec())).collect();

                                // Lose up to parity_shards number of shards
                                for item in shards_opt.iter_mut().take(*parity_shards) {
                                    *item = None;
                                }

                                erasure.decode_data(black_box(&mut shards_opt)).unwrap();
                                black_box(&shards_opt);
                            });
                        },
                    );
                    group.finish();
                }
                Err(e) => {
                    println!("⚠️  Skipping decode test {test_name} - configuration not supported: {e}");
                }
            }
        }
    }
}

/// Shard size sensitivity analysis for SIMD optimization
fn bench_shard_size_analysis(c: &mut Criterion) {
    let data_shards = 4;
    let parity_shards = 2;

    // Test different shard sizes, focusing on SIMD optimization thresholds
    let shard_sizes = vec![32, 64, 128, 256, 512, 1024, 2048, 4096, 8192];

    let mut group = c.benchmark_group("shard_size_analysis");
    group.sample_size(15);
    group.measurement_time(Duration::from_secs(8));

    for shard_size in shard_sizes {
        let total_size = shard_size * data_shards;
        let data = (0..total_size).map(|i| (i % 256) as u8).collect::<Vec<u8>>();
        let test_name = format!("{shard_size}B_shard_simd");

        group.throughput(Throughput::Bytes(total_size as u64));

        // Check if this shard size is supported
        let erasure = Erasure::new(data_shards, parity_shards, data.len());
        match erasure.encode_data(&data) {
            Ok(_) => {
                group.bench_with_input(BenchmarkId::new("shard_size", &test_name), &data, |b, data| {
                    let erasure = Erasure::new(data_shards, parity_shards, data.len());
                    b.iter(|| {
                        let shards = erasure.encode_data(black_box(data)).unwrap();
                        black_box(shards);
                    });
                });
            }
            Err(e) => {
                println!("⚠️  Skipping shard size test {test_name} - not supported: {e}");
            }
        }
    }
    group.finish();
}

/// High-load concurrent performance analysis
fn bench_concurrent_analysis(c: &mut Criterion) {
    use std::sync::Arc;
    use std::thread;

    let data_size = 1024 * 1024; // 1MB
    let data = Arc::new((0..data_size).map(|i| (i % 256) as u8).collect::<Vec<u8>>());
    let erasure = Arc::new(Erasure::new(4, 2, data_size));

    let mut group = c.benchmark_group("concurrent_analysis");
    group.throughput(Throughput::Bytes(data_size as u64));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    let test_name = "1MB_concurrent_simd";

    group.bench_function(test_name, |b| {
        b.iter(|| {
            let handles: Vec<_> = (0..4)
                .map(|_| {
                    let data_clone = data.clone();
                    let erasure_clone = erasure.clone();
                    thread::spawn(move || {
                        let shards = erasure_clone.encode_data(&data_clone).unwrap();
                        black_box(shards);
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }
        });
    });
    group.finish();
}

/// Error recovery performance analysis
fn bench_error_recovery_analysis(c: &mut Criterion) {
    let data_size = 512 * 1024; // 512KB
    let data = (0..data_size).map(|i| (i % 256) as u8).collect::<Vec<u8>>();

    // Test different error recovery scenarios
    let scenarios = vec![
        (4, 2, 1, "single_loss"),     // Lose 1 shard
        (4, 2, 2, "double_loss"),     // Lose 2 shards (maximum)
        (6, 3, 1, "single_loss_6_3"), // Lose 1 shard with 6+3
        (6, 3, 3, "triple_loss_6_3"), // Lose 3 shards (maximum)
        (8, 4, 2, "double_loss_8_4"), // Lose 2 shards with 8+4
        (8, 4, 4, "quad_loss_8_4"),   // Lose 4 shards (maximum)
    ];

    let mut group = c.benchmark_group("error_recovery_analysis");
    group.throughput(Throughput::Bytes(data_size as u64));
    group.sample_size(15);
    group.measurement_time(Duration::from_secs(10));

    for (data_shards, parity_shards, loss_count, scenario_name) in scenarios {
        let erasure = Erasure::new(data_shards, parity_shards, data_size);

        match erasure.encode_data(&data) {
            Ok(encoded_shards) => {
                let test_name = format!("{data_shards}+{parity_shards}_{scenario_name}");

                group.bench_with_input(
                    BenchmarkId::new("recovery", &test_name),
                    &(&encoded_shards, data_shards, parity_shards, loss_count),
                    |b, (shards, data_shards, parity_shards, loss_count)| {
                        let erasure = Erasure::new(*data_shards, *parity_shards, data_size);
                        b.iter(|| {
                            // Simulate specific number of shard losses
                            let mut shards_opt: Vec<Option<Vec<u8>>> = shards.iter().map(|shard| Some(shard.to_vec())).collect();

                            // Lose the specified number of shards
                            for item in shards_opt.iter_mut().take(*loss_count) {
                                *item = None;
                            }

                            erasure.decode_data(black_box(&mut shards_opt)).unwrap();
                            black_box(&shards_opt);
                        });
                    },
                );
            }
            Err(e) => {
                println!("⚠️  Skipping recovery test {scenario_name}: {e}");
            }
        }
    }
    group.finish();
}

/// Memory efficiency analysis
fn bench_memory_analysis(c: &mut Criterion) {
    let data_sizes = vec![64 * 1024, 256 * 1024, 1024 * 1024]; // 64KB, 256KB, 1MB
    let config = (4, 2); // 4+2 configuration

    let mut group = c.benchmark_group("memory_analysis");
    group.sample_size(15);
    group.measurement_time(Duration::from_secs(8));

    for data_size in data_sizes {
        let data = (0..data_size).map(|i| (i % 256) as u8).collect::<Vec<u8>>();
        let size_name = format!("{}KB", data_size / 1024);

        group.throughput(Throughput::Bytes(data_size as u64));

        // Test instance reuse vs new instance creation
        group.bench_with_input(BenchmarkId::new("reuse_instance", &size_name), &data, |b, data| {
            let erasure = Erasure::new(config.0, config.1, data.len());
            b.iter(|| {
                let shards = erasure.encode_data(black_box(data)).unwrap();
                black_box(shards);
            });
        });

        group.bench_with_input(BenchmarkId::new("new_instance", &size_name), &data, |b, data| {
            b.iter(|| {
                let erasure = Erasure::new(config.0, config.1, data.len());
                let shards = erasure.encode_data(black_box(data)).unwrap();
                black_box(shards);
            });
        });
    }
    group.finish();
}

// Benchmark group configuration
criterion_group!(
    benches,
    bench_encode_analysis,
    bench_decode_analysis,
    bench_shard_size_analysis,
    bench_concurrent_analysis,
    bench_error_recovery_analysis,
    bench_memory_analysis
);

criterion_main!(benches);
