//! Reed-Solomon erasure coding performance benchmarks.
//!
//! This benchmark compares the performance of different Reed-Solomon implementations:
//! - SIMD mode: High-performance reed-solomon-simd implementation
//! - `reed-solomon-simd` feature: SIMD mode with optimized performance
//!
//! ## Running Benchmarks
//!
//! ```bash
//! # 运行所有基准测试
//! cargo bench
//!
//! # 运行特定的基准测试
//! cargo bench --bench erasure_benchmark
//!
//! # 生成HTML报告
//! cargo bench --bench erasure_benchmark -- --output-format html
//!
//! # 只测试编码性能
//! cargo bench encode
//!
//! # 只测试解码性能
//! cargo bench decode
//! ```
//!
//! ## Test Configurations
//!
//! The benchmarks test various scenarios:
//! - Different data sizes: 1KB, 64KB, 1MB, 16MB
//! - Different erasure coding configurations: (4,2), (6,3), (8,4)
//! - Both encoding and decoding operations
//! - Small vs large shard scenarios for SIMD optimization

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use ecstore::erasure_coding::{Erasure, calc_shard_size};
use std::time::Duration;

/// 基准测试配置结构体
#[derive(Clone, Debug)]
struct BenchConfig {
    /// 数据分片数量
    data_shards: usize,
    /// 奇偶校验分片数量
    parity_shards: usize,
    /// 测试数据大小（字节）
    data_size: usize,
    /// 块大小（字节）
    block_size: usize,
    /// 配置名称
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

/// 生成测试数据
fn generate_test_data(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 256) as u8).collect()
}

/// 基准测试: 编码性能对比
fn bench_encode_performance(c: &mut Criterion) {
    let configs = vec![
        // 小数据量测试 - 1KB
        BenchConfig::new(4, 2, 1024, 1024),
        BenchConfig::new(6, 3, 1024, 1024),
        BenchConfig::new(8, 4, 1024, 1024),
        // 中等数据量测试 - 64KB
        BenchConfig::new(4, 2, 64 * 1024, 64 * 1024),
        BenchConfig::new(6, 3, 64 * 1024, 64 * 1024),
        BenchConfig::new(8, 4, 64 * 1024, 64 * 1024),
        // 大数据量测试 - 1MB
        BenchConfig::new(4, 2, 1024 * 1024, 1024 * 1024),
        BenchConfig::new(6, 3, 1024 * 1024, 1024 * 1024),
        BenchConfig::new(8, 4, 1024 * 1024, 1024 * 1024),
        // 超大数据量测试 - 16MB
        BenchConfig::new(4, 2, 16 * 1024 * 1024, 16 * 1024 * 1024),
        BenchConfig::new(6, 3, 16 * 1024 * 1024, 16 * 1024 * 1024),
    ];

    for config in configs {
        let data = generate_test_data(config.data_size);

        // 测试当前默认实现（通常是SIMD）
        let mut group = c.benchmark_group("encode_current");
        group.throughput(Throughput::Bytes(config.data_size as u64));
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(5));

        group.bench_with_input(BenchmarkId::new("current_impl", &config.name), &(&data, &config), |b, (data, config)| {
            let erasure = Erasure::new(config.data_shards, config.parity_shards, config.block_size);
            b.iter(|| {
                let shards = erasure.encode_data(black_box(data)).unwrap();
                black_box(shards);
            });
        });
        group.finish();

        // 如果SIMD feature启用，测试专用的erasure实现对比
        #[cfg(feature = "reed-solomon-simd")]
        {
            use ecstore::erasure_coding::ReedSolomonEncoder;

            let mut erasure_group = c.benchmark_group("encode_erasure_only");
            erasure_group.throughput(Throughput::Bytes(config.data_size as u64));
            erasure_group.sample_size(10);
            erasure_group.measurement_time(Duration::from_secs(5));

            erasure_group.bench_with_input(
                BenchmarkId::new("erasure_impl", &config.name),
                &(&data, &config),
                |b, (data, config)| {
                    let encoder = ReedSolomonEncoder::new(config.data_shards, config.parity_shards).unwrap();
                    b.iter(|| {
                        // 创建编码所需的数据结构
                        let per_shard_size = calc_shard_size(data.len(), config.data_shards);
                        let total_size = per_shard_size * (config.data_shards + config.parity_shards);
                        let mut buffer = vec![0u8; total_size];
                        buffer[..data.len()].copy_from_slice(data);

                        let slices: smallvec::SmallVec<[&mut [u8]; 16]> = buffer.chunks_exact_mut(per_shard_size).collect();

                        encoder.encode(black_box(slices)).unwrap();
                        black_box(&buffer);
                    });
                },
            );
            erasure_group.finish();
        }

        // 如果使用SIMD feature，测试直接SIMD实现对比
        #[cfg(feature = "reed-solomon-simd")]
        {
            // 只对大shard测试SIMD（小于512字节的shard SIMD性能不佳）
            let shard_size = calc_shard_size(config.data_size, config.data_shards);
            if shard_size >= 512 {
                let mut simd_group = c.benchmark_group("encode_simd_direct");
                simd_group.throughput(Throughput::Bytes(config.data_size as u64));
                simd_group.sample_size(10);
                simd_group.measurement_time(Duration::from_secs(5));

                simd_group.bench_with_input(
                    BenchmarkId::new("simd_impl", &config.name),
                    &(&data, &config),
                    |b, (data, config)| {
                        b.iter(|| {
                            // 直接使用SIMD实现
                            let per_shard_size = calc_shard_size(data.len(), config.data_shards);
                            match reed_solomon_simd::ReedSolomonEncoder::new(
                                config.data_shards,
                                config.parity_shards,
                                per_shard_size,
                            ) {
                                Ok(mut encoder) => {
                                    // 创建正确大小的缓冲区，并填充数据
                                    let mut buffer = vec![0u8; per_shard_size * config.data_shards];
                                    let copy_len = data.len().min(buffer.len());
                                    buffer[..copy_len].copy_from_slice(&data[..copy_len]);

                                    // 按正确的分片大小添加数据分片
                                    for chunk in buffer.chunks_exact(per_shard_size) {
                                        encoder.add_original_shard(black_box(chunk)).unwrap();
                                    }

                                    let result = encoder.encode().unwrap();
                                    black_box(result);
                                }
                                Err(_) => {
                                    // SIMD不支持此配置，跳过
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
}

/// 基准测试: 解码性能对比
fn bench_decode_performance(c: &mut Criterion) {
    let configs = vec![
        // 中等数据量测试 - 64KB
        BenchConfig::new(4, 2, 64 * 1024, 64 * 1024),
        BenchConfig::new(6, 3, 64 * 1024, 64 * 1024),
        // 大数据量测试 - 1MB
        BenchConfig::new(4, 2, 1024 * 1024, 1024 * 1024),
        BenchConfig::new(6, 3, 1024 * 1024, 1024 * 1024),
        // 超大数据量测试 - 16MB
        BenchConfig::new(4, 2, 16 * 1024 * 1024, 16 * 1024 * 1024),
    ];

    for config in configs {
        let data = generate_test_data(config.data_size);
        let erasure = Erasure::new(config.data_shards, config.parity_shards, config.block_size);

        // 预先编码数据
        let encoded_shards = erasure.encode_data(&data).unwrap();

        // 测试当前默认实现的解码性能
        let mut group = c.benchmark_group("decode_current");
        group.throughput(Throughput::Bytes(config.data_size as u64));
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(5));

        group.bench_with_input(
            BenchmarkId::new("current_impl", &config.name),
            &(&encoded_shards, &config),
            |b, (shards, config)| {
                let erasure = Erasure::new(config.data_shards, config.parity_shards, config.block_size);
                b.iter(|| {
                    // 模拟数据丢失 - 丢失一个数据分片和一个奇偶分片
                    let mut shards_opt: Vec<Option<Vec<u8>>> = shards.iter().map(|shard| Some(shard.to_vec())).collect();

                    // 丢失最后一个数据分片和第一个奇偶分片
                    shards_opt[config.data_shards - 1] = None;
                    shards_opt[config.data_shards] = None;

                    erasure.decode_data(black_box(&mut shards_opt)).unwrap();
                    black_box(&shards_opt);
                });
            },
        );
        group.finish();

        // 如果使用混合模式（默认），测试SIMD解码性能

        {
            let shard_size = calc_shard_size(config.data_size, config.data_shards);
            if shard_size >= 512 {
                let mut simd_group = c.benchmark_group("decode_simd_direct");
                simd_group.throughput(Throughput::Bytes(config.data_size as u64));
                simd_group.sample_size(10);
                simd_group.measurement_time(Duration::from_secs(5));

                simd_group.bench_with_input(
                    BenchmarkId::new("simd_impl", &config.name),
                    &(&encoded_shards, &config),
                    |b, (shards, config)| {
                        b.iter(|| {
                            let per_shard_size = calc_shard_size(config.data_size, config.data_shards);
                            match reed_solomon_simd::ReedSolomonDecoder::new(
                                config.data_shards,
                                config.parity_shards,
                                per_shard_size,
                            ) {
                                Ok(mut decoder) => {
                                    // 添加可用的分片（除了丢失的）
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
                                    // SIMD不支持此配置，跳过
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
}

/// 基准测试: 不同分片大小对性能的影响
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

        // 测试当前实现
        group.bench_with_input(BenchmarkId::new("current", format!("shard_{}B", shard_size)), &data, |b, data| {
            let erasure = Erasure::new(data_shards, parity_shards, total_data_size);
            b.iter(|| {
                let shards = erasure.encode_data(black_box(data)).unwrap();
                black_box(shards);
            });
        });
    }
    group.finish();
}

/// 基准测试: 编码配置对性能的影响
fn bench_coding_configurations(c: &mut Criterion) {
    let configs = vec![
        (2, 1),  // 最小冗余
        (3, 2),  // 中等冗余
        (4, 2),  // 常用配置
        (6, 3),  // 50%冗余
        (8, 4),  // 50%冗余，更多分片
        (10, 5), // 50%冗余，大量分片
        (12, 6), // 50%冗余，更大量分片
    ];

    let data_size = 1024 * 1024; // 1MB测试数据
    let data = generate_test_data(data_size);

    let mut group = c.benchmark_group("coding_configurations");
    group.throughput(Throughput::Bytes(data_size as u64));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));

    for (data_shards, parity_shards) in configs {
        let config_name = format!("{}+{}", data_shards, parity_shards);

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

/// 基准测试: 内存使用模式
fn bench_memory_patterns(c: &mut Criterion) {
    let data_shards = 4;
    let parity_shards = 2;
    let block_size = 1024 * 1024; // 1MB块

    let mut group = c.benchmark_group("memory_patterns");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));

    // 测试重复使用同一个Erasure实例
    group.bench_function("reuse_erasure_instance", |b| {
        let erasure = Erasure::new(data_shards, parity_shards, block_size);
        let data = generate_test_data(block_size);

        b.iter(|| {
            let shards = erasure.encode_data(black_box(&data)).unwrap();
            black_box(shards);
        });
    });

    // 测试每次创建新的Erasure实例
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

// 基准测试组配置
criterion_group!(
    benches,
    bench_encode_performance,
    bench_decode_performance,
    bench_shard_size_impact,
    bench_coding_configurations,
    bench_memory_patterns
);

criterion_main!(benches);
