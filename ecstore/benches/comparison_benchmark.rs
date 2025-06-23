//! 专门比较 Pure Erasure 和 Hybrid (SIMD) 模式性能的基准测试
//!
//! 这个基准测试使用不同的feature编译配置来直接对比两种实现的性能。
//!
//! ## 运行比较测试
//!
//! ```bash
//! # 测试 Pure Erasure 实现 (默认)
//! cargo bench --bench comparison_benchmark
//!
//! # 测试 Hybrid (SIMD) 实现  
//! cargo bench --bench comparison_benchmark --features reed-solomon-simd
//!
//! # 测试强制 erasure-only 模式
//! cargo bench --bench comparison_benchmark
//!
//! # 生成对比报告
//! cargo bench --bench comparison_benchmark -- --save-baseline erasure
//! cargo bench --bench comparison_benchmark --features reed-solomon-simd -- --save-baseline hybrid
//! ```

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use ecstore::erasure_coding::Erasure;
use std::time::Duration;

/// 基准测试数据配置
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

/// 生成不同大小的测试数据集
fn generate_test_datasets() -> Vec<TestData> {
    vec![
        TestData::new(1024, "1KB"),            // 小数据
        TestData::new(8 * 1024, "8KB"),        // 中小数据
        TestData::new(64 * 1024, "64KB"),      // 中等数据
        TestData::new(256 * 1024, "256KB"),    // 中大数据
        TestData::new(1024 * 1024, "1MB"),     // 大数据
        TestData::new(4 * 1024 * 1024, "4MB"), // 超大数据
    ]
}

/// 编码性能比较基准测试
fn bench_encode_comparison(c: &mut Criterion) {
    let datasets = generate_test_datasets();
    let configs = vec![
        (4, 2, "4+2"), // 常用配置
        (6, 3, "6+3"), // 50%冗余
        (8, 4, "8+4"), // 50%冗余，更多分片
    ];

    for dataset in &datasets {
        for (data_shards, parity_shards, config_name) in &configs {
            let test_name = format!("{}_{}_{}", dataset.size_name, config_name, get_implementation_name());

            let mut group = c.benchmark_group("encode_comparison");
            group.throughput(Throughput::Bytes(dataset.data.len() as u64));
            group.sample_size(20);
            group.measurement_time(Duration::from_secs(10));

            // 检查是否能够创建erasure实例（某些配置在纯SIMD模式下可能失败）
            match Erasure::new(*data_shards, *parity_shards, dataset.data.len()).encode_data(&dataset.data) {
                Ok(_) => {
                    group.bench_with_input(
                        BenchmarkId::new("implementation", &test_name),
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
                    println!("⚠️  跳过测试 {} - 配置不支持: {}", test_name, e);
                }
            }
            group.finish();
        }
    }
}

/// 解码性能比较基准测试
fn bench_decode_comparison(c: &mut Criterion) {
    let datasets = generate_test_datasets();
    let configs = vec![(4, 2, "4+2"), (6, 3, "6+3"), (8, 4, "8+4")];

    for dataset in &datasets {
        for (data_shards, parity_shards, config_name) in &configs {
            let test_name = format!("{}_{}_{}", dataset.size_name, config_name, get_implementation_name());
            let erasure = Erasure::new(*data_shards, *parity_shards, dataset.data.len());

            // 预先编码数据 - 检查是否支持此配置
            match erasure.encode_data(&dataset.data) {
                Ok(encoded_shards) => {
                    let mut group = c.benchmark_group("decode_comparison");
                    group.throughput(Throughput::Bytes(dataset.data.len() as u64));
                    group.sample_size(20);
                    group.measurement_time(Duration::from_secs(10));

                    group.bench_with_input(
                        BenchmarkId::new("implementation", &test_name),
                        &(&encoded_shards, *data_shards, *parity_shards),
                        |b, (shards, data_shards, parity_shards)| {
                            let erasure = Erasure::new(*data_shards, *parity_shards, dataset.data.len());
                            b.iter(|| {
                                // 模拟最大可恢复的数据丢失
                                let mut shards_opt: Vec<Option<Vec<u8>>> =
                                    shards.iter().map(|shard| Some(shard.to_vec())).collect();

                                // 丢失等于奇偶校验分片数量的分片
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
                    println!("⚠️  跳过解码测试 {} - 配置不支持: {}", test_name, e);
                }
            }
        }
    }
}

/// 分片大小敏感性测试
fn bench_shard_size_sensitivity(c: &mut Criterion) {
    let data_shards = 4;
    let parity_shards = 2;

    // 测试不同的分片大小，特别关注SIMD的临界点
    let shard_sizes = vec![32, 64, 128, 256, 512, 1024, 2048, 4096, 8192];

    let mut group = c.benchmark_group("shard_size_sensitivity");
    group.sample_size(15);
    group.measurement_time(Duration::from_secs(8));

    for shard_size in shard_sizes {
        let total_size = shard_size * data_shards;
        let data = (0..total_size).map(|i| (i % 256) as u8).collect::<Vec<u8>>();
        let test_name = format!("{}B_shard_{}", shard_size, get_implementation_name());

        group.throughput(Throughput::Bytes(total_size as u64));

        // 检查此分片大小是否支持
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
                println!("⚠️  跳过分片大小测试 {} - 不支持: {}", test_name, e);
            }
        }
    }
    group.finish();
}

/// 高负载并发测试
fn bench_concurrent_load(c: &mut Criterion) {
    use std::sync::Arc;
    use std::thread;

    let data_size = 1024 * 1024; // 1MB
    let data = Arc::new((0..data_size).map(|i| (i % 256) as u8).collect::<Vec<u8>>());
    let erasure = Arc::new(Erasure::new(4, 2, data_size));

    let mut group = c.benchmark_group("concurrent_load");
    group.throughput(Throughput::Bytes(data_size as u64));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    let test_name = format!("1MB_concurrent_{}", get_implementation_name());

    group.bench_function(&test_name, |b| {
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

/// 错误恢复能力测试
fn bench_error_recovery_performance(c: &mut Criterion) {
    let data_size = 256 * 1024; // 256KB
    let data = (0..data_size).map(|i| (i % 256) as u8).collect::<Vec<u8>>();

    let configs = vec![
        (4, 2, 1), // 丢失1个分片
        (4, 2, 2), // 丢失2个分片（最大可恢复）
        (6, 3, 2), // 丢失2个分片
        (6, 3, 3), // 丢失3个分片（最大可恢复）
        (8, 4, 3), // 丢失3个分片
        (8, 4, 4), // 丢失4个分片（最大可恢复）
    ];

    let mut group = c.benchmark_group("error_recovery");
    group.throughput(Throughput::Bytes(data_size as u64));
    group.sample_size(15);
    group.measurement_time(Duration::from_secs(8));

    for (data_shards, parity_shards, lost_shards) in configs {
        let erasure = Erasure::new(data_shards, parity_shards, data_size);
        let test_name = format!("{}+{}_lost{}_{}", data_shards, parity_shards, lost_shards, get_implementation_name());

        // 检查此配置是否支持
        match erasure.encode_data(&data) {
            Ok(encoded_shards) => {
                group.bench_with_input(
                    BenchmarkId::new("recovery", &test_name),
                    &(&encoded_shards, data_shards, parity_shards, lost_shards),
                    |b, (shards, data_shards, parity_shards, lost_shards)| {
                        let erasure = Erasure::new(*data_shards, *parity_shards, data_size);
                        b.iter(|| {
                            let mut shards_opt: Vec<Option<Vec<u8>>> = shards.iter().map(|shard| Some(shard.to_vec())).collect();

                            // 丢失指定数量的分片
                            for item in shards_opt.iter_mut().take(*lost_shards) {
                                *item = None;
                            }

                            erasure.decode_data(black_box(&mut shards_opt)).unwrap();
                            black_box(&shards_opt);
                        });
                    },
                );
            }
            Err(e) => {
                println!("⚠️  跳过错误恢复测试 {} - 配置不支持: {}", test_name, e);
            }
        }
    }
    group.finish();
}

/// 内存效率测试
fn bench_memory_efficiency(c: &mut Criterion) {
    let data_shards = 4;
    let parity_shards = 2;
    let data_size = 1024 * 1024; // 1MB

    let mut group = c.benchmark_group("memory_efficiency");
    group.throughput(Throughput::Bytes(data_size as u64));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(8));

    let test_name = format!("memory_pattern_{}", get_implementation_name());

    // 测试连续多次编码对内存的影响
    group.bench_function(format!("{}_continuous", test_name), |b| {
        let erasure = Erasure::new(data_shards, parity_shards, data_size);
        b.iter(|| {
            for i in 0..10 {
                let data = vec![(i % 256) as u8; data_size];
                let shards = erasure.encode_data(black_box(&data)).unwrap();
                black_box(shards);
            }
        });
    });

    // 测试大量小编码任务
    group.bench_function(format!("{}_small_chunks", test_name), |b| {
        let chunk_size = 1024; // 1KB chunks
        let erasure = Erasure::new(data_shards, parity_shards, chunk_size);
        b.iter(|| {
            for i in 0..1024 {
                let data = vec![(i % 256) as u8; chunk_size];
                let shards = erasure.encode_data(black_box(&data)).unwrap();
                black_box(shards);
            }
        });
    });

    group.finish();
}

/// 获取当前实现的名称
fn get_implementation_name() -> &'static str {
    #[cfg(feature = "reed-solomon-simd")]
    return "hybrid";

    #[cfg(not(feature = "reed-solomon-simd"))]
    return "erasure";
}

criterion_group!(
    benches,
    bench_encode_comparison,
    bench_decode_comparison,
    bench_shard_size_sensitivity,
    bench_concurrent_load,
    bench_error_recovery_performance,
    bench_memory_efficiency
);

criterion_main!(benches);
