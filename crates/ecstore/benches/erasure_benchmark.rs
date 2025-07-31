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

//! Reed-Solomon SIMD erasure coding performance benchmarks.
//!
//! This benchmark tests the performance of the high-performance SIMD Reed-Solomon implementation.
//!
//! ## Running Benchmarks
//!
//! ```bash
//! # Run all benchmarks
//! cargo bench
//!
//! # Run specific benchmark
//! cargo bench --bench erasure_benchmark
//!
//! # Generate HTML report
//! cargo bench --bench erasure_benchmark -- --output-format html
//!
//! # Test encoding performance only
//! cargo bench encode
//!
//! # Test decoding performance only
//! cargo bench decode
//! ```
//!
//! ## Test Configurations
//!
//! The benchmarks test various scenarios:
//! - Different data sizes: 1KB, 64KB, 1MB, 16MB
//! - Different erasure coding configurations: (4,2), (6,3), (8,4)
//! - Both encoding and decoding operations
//! - SIMD optimization for different shard sizes

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rustfs_ecstore::erasure_coding::{Erasure, calc_shard_size};
use std::hint::black_box;
use std::time::Duration;

/// Benchmark configuration structure
#[derive(Clone, Debug)]
struct BenchConfig {
    /// Number of data shards
    data_shards: usize,
    /// Number of parity shards
    parity_shards: usize,
    /// Test data size (bytes)
    data_size: usize,
    /// Block size (bytes)
    block_size: usize,
    /// Configuration name
    name: String,
}

impl BenchConfig {
    fn new(data_shards: usize, parity_shards: usize, data_size: usize, block_size: usize) -> Self {
        Self {
            data_shards,
            parity_shards,
            data_size,
            block_size,
            name: format!("{}+{}_{}KB_{}KB-block", data_shards, parity_shards, data_size / 1024, block_size / 1024),
        }
    }
}

/// Generate test data
fn generate_test_data(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 256) as u8).collect()
}

/// Benchmark: Encoding performance
fn bench_encode_performance(c: &mut Criterion) {
    let configs = vec![
        // Small data tests - 1KB
        BenchConfig::new(4, 2, 1024, 1024),
        BenchConfig::new(6, 3, 1024, 1024),
        BenchConfig::new(8, 4, 1024, 1024),
        // Medium data tests - 64KB
        BenchConfig::new(4, 2, 64 * 1024, 64 * 1024),
        BenchConfig::new(6, 3, 64 * 1024, 64 * 1024),
        BenchConfig::new(8, 4, 64 * 1024, 64 * 1024),
        // Large data tests - 1MB
        BenchConfig::new(4, 2, 1024 * 1024, 1024 * 1024),
        BenchConfig::new(6, 3, 1024 * 1024, 1024 * 1024),
        BenchConfig::new(8, 4, 1024 * 1024, 1024 * 1024),
        // Extra large data tests - 16MB
        BenchConfig::new(4, 2, 16 * 1024 * 1024, 16 * 1024 * 1024),
        BenchConfig::new(6, 3, 16 * 1024 * 1024, 16 * 1024 * 1024),
    ];

    for config in configs {
        let data = generate_test_data(config.data_size);

        // Test SIMD encoding performance
        let mut group = c.benchmark_group("encode_simd");
        group.throughput(Throughput::Bytes(config.data_size as u64));
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(5));

        group.bench_with_input(BenchmarkId::new("simd_impl", &config.name), &(&data, &config), |b, (data, config)| {
            let erasure = Erasure::new(config.data_shards, config.parity_shards, config.block_size);
            b.iter(|| {
                let shards = erasure.encode_data(black_box(data)).unwrap();
                black_box(shards);
            });
        });
        group.finish();

        // Test direct SIMD implementation for large shards (>= 512 bytes)
        let shard_size = calc_shard_size(config.data_size, config.data_shards);
        if shard_size >= 512 {
            let mut simd_group = c.benchmark_group("encode_simd_direct");
            simd_group.throughput(Throughput::Bytes(config.data_size as u64));
            simd_group.sample_size(10);
            simd_group.measurement_time(Duration::from_secs(5));

            simd_group.bench_with_input(BenchmarkId::new("simd_direct", &config.name), &(&data, &config), |b, (data, config)| {
                b.iter(|| {
                    // Direct SIMD implementation
                    let per_shard_size = calc_shard_size(data.len(), config.data_shards);
                    match reed_solomon_simd::ReedSolomonEncoder::new(config.data_shards, config.parity_shards, per_shard_size) {
                        Ok(mut encoder) => {
                            // Create properly sized buffer and fill with data
                            let mut buffer = vec![0u8; per_shard_size * config.data_shards];
                            let copy_len = data.len().min(buffer.len());
                            buffer[..copy_len].copy_from_slice(&data[..copy_len]);

                            // Add data shards with correct shard size
                            for chunk in buffer.chunks_exact(per_shard_size) {
                                encoder.add_original_shard(black_box(chunk)).unwrap();
                            }

                            let result = encoder.encode().unwrap();
                            black_box(result);
                        }
                        Err(_) => {
                            // SIMD doesn't support this configuration, skip
                            black_box(());
                        }
                    }
                });
            });
            simd_group.finish();
        }
    }
}

/// Benchmark: Decoding performance
fn bench_decode_performance(c: &mut Criterion) {
    let configs = vec![
        // Medium data tests - 64KB
        BenchConfig::new(4, 2, 64 * 1024, 64 * 1024),
        BenchConfig::new(6, 3, 64 * 1024, 64 * 1024),
        // Large data tests - 1MB
        BenchConfig::new(4, 2, 1024 * 1024, 1024 * 1024),
        BenchConfig::new(6, 3, 1024 * 1024, 1024 * 1024),
        // Extra large data tests - 16MB
        BenchConfig::new(4, 2, 16 * 1024 * 1024, 16 * 1024 * 1024),
    ];

    for config in configs {
        let data = generate_test_data(config.data_size);
        let erasure = Erasure::new(config.data_shards, config.parity_shards, config.block_size);

        // Pre-encode data
        let encoded_shards = erasure.encode_data(&data).unwrap();

        // Test SIMD decoding performance
        let mut group = c.benchmark_group("decode_simd");
        group.throughput(Throughput::Bytes(config.data_size as u64));
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(5));

        group.bench_with_input(
            BenchmarkId::new("simd_impl", &config.name),
            &(&encoded_shards, &config),
            |b, (shards, config)| {
                let erasure = Erasure::new(config.data_shards, config.parity_shards, config.block_size);
                b.iter(|| {
                    // Simulate data loss - lose one data shard and one parity shard
                    let mut shards_opt: Vec<Option<Vec<u8>>> = shards.iter().map(|shard| Some(shard.to_vec())).collect();

                    // Lose last data shard and first parity shard
                    shards_opt[config.data_shards - 1] = None;
                    shards_opt[config.data_shards] = None;

                    erasure.decode_data(black_box(&mut shards_opt)).unwrap();
                    black_box(&shards_opt);
                });
            },
        );
        group.finish();

        // Test direct SIMD decoding for large shards
        let shard_size = calc_shard_size(config.data_size, config.data_shards);
        if shard_size >= 512 {
            let mut simd_group = c.benchmark_group("decode_simd_direct");
            simd_group.throughput(Throughput::Bytes(config.data_size as u64));
            simd_group.sample_size(10);
            simd_group.measurement_time(Duration::from_secs(5));

            simd_group.bench_with_input(
                BenchmarkId::new("simd_direct", &config.name),
                &(&encoded_shards, &config),
                |b, (shards, config)| {
                    b.iter(|| {
                        let per_shard_size = calc_shard_size(config.data_size, config.data_shards);
                        match reed_solomon_simd::ReedSolomonDecoder::new(config.data_shards, config.parity_shards, per_shard_size)
                        {
                            Ok(mut decoder) => {
                                // Add available shards (except lost ones)
                                for (i, shard) in shards.iter().enumerate() {
                                    if i != config.data_shards - 1 && i != config.data_shards {
                                        if i < config.data_shards {
                                            decoder.add_original_shard(i, black_box(shard)).unwrap();
                                        } else {
                                            let recovery_idx = i - config.data_shards;
                                            decoder.add_recovery_shard(recovery_idx, black_box(shard)).unwrap();
                                        }
                                    }
                                }

                                let result = decoder.decode().unwrap();
                                black_box(result);
                            }
                            Err(_) => {
                                // SIMD doesn't support this configuration, skip
                                black_box(());
                            }
                        }
                    });
                },
            );
            simd_group.finish();
        }
    }
}

/// Benchmark: Impact of different shard sizes on performance
fn bench_shard_size_impact(c: &mut Criterion) {
    let shard_sizes = vec![64, 128, 256, 512, 1024, 2048, 4096, 8192];
    let data_shards = 4;
    let parity_shards = 2;

    let mut group = c.benchmark_group("shard_size_impact");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(3));

    for shard_size in shard_sizes {
        let total_data_size = shard_size * data_shards;
        let data = generate_test_data(total_data_size);

        group.throughput(Throughput::Bytes(total_data_size as u64));

        // Test SIMD implementation
        group.bench_with_input(BenchmarkId::new("simd", format!("shard_{shard_size}B")), &data, |b, data| {
            let erasure = Erasure::new(data_shards, parity_shards, total_data_size);
            b.iter(|| {
                let shards = erasure.encode_data(black_box(data)).unwrap();
                black_box(shards);
            });
        });
    }
    group.finish();
}

/// Benchmark: Impact of coding configurations on performance
fn bench_coding_configurations(c: &mut Criterion) {
    let configs = vec![
        (2, 1),  // Minimal redundancy
        (3, 2),  // Medium redundancy
        (4, 2),  // Common configuration
        (6, 3),  // 50% redundancy
        (8, 4),  // 50% redundancy, more shards
        (10, 5), // 50% redundancy, many shards
        (12, 6), // 50% redundancy, very many shards
    ];

    let data_size = 1024 * 1024; // 1MB test data
    let data = generate_test_data(data_size);

    let mut group = c.benchmark_group("coding_configurations");
    group.throughput(Throughput::Bytes(data_size as u64));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));

    for (data_shards, parity_shards) in configs {
        let config_name = format!("{data_shards}+{parity_shards}");

        group.bench_with_input(BenchmarkId::new("encode", &config_name), &data, |b, data| {
            let erasure = Erasure::new(data_shards, parity_shards, data_size);
            b.iter(|| {
                let shards = erasure.encode_data(black_box(data)).unwrap();
                black_box(shards);
            });
        });
    }
    group.finish();
}

/// Benchmark: Memory usage patterns
fn bench_memory_patterns(c: &mut Criterion) {
    let data_shards = 4;
    let parity_shards = 2;
    let block_size = 1024 * 1024; // 1MB block

    let mut group = c.benchmark_group("memory_patterns");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));

    // Test reusing the same Erasure instance
    group.bench_function("reuse_erasure_instance", |b| {
        let erasure = Erasure::new(data_shards, parity_shards, block_size);
        let data = generate_test_data(block_size);

        b.iter(|| {
            let shards = erasure.encode_data(black_box(&data)).unwrap();
            black_box(shards);
        });
    });

    // Test creating new Erasure instance each time
    group.bench_function("new_erasure_instance", |b| {
        let data = generate_test_data(block_size);

        b.iter(|| {
            let erasure = Erasure::new(data_shards, parity_shards, block_size);
            let shards = erasure.encode_data(black_box(&data)).unwrap();
            black_box(shards);
        });
    });

    group.finish();
}

// Benchmark group configuration
criterion_group!(
    benches,
    bench_encode_performance,
    bench_decode_performance,
    bench_shard_size_impact,
    bench_coding_configurations,
    bench_memory_patterns
);

criterion_main!(benches);
