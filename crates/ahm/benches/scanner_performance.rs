use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use rustfs_ahm::scanner::data_scanner::{Scanner, ScannerConfig};
use rustfs_ecstore::{self as ecstore, StorageAPI, store::ECStore, endpoints::{EndpointServerPools, PoolEndpoints, Endpoints}};
use rustfs_ecstore::store_api::{PutObjReader, ObjectOptions, MakeBucketOptions, ObjectIO};
use rustfs_ecstore::disk::endpoint::Endpoint;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::net::SocketAddr;
use tempfile::TempDir;
use tokio::runtime::Runtime;
use sysinfo::System;
use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::HashMap;

/// Scanner性能基准测试配置
struct ScannerBenchmarkConfig {
    /// 测试数据量（对象数量）
    pub object_counts: Vec<usize>,
    /// 对象大小
    pub object_sizes: Vec<usize>,
    /// Scanner配置变体
    pub scanner_configs: Vec<ScannerTestConfig>,
    /// 测试持续时间
    pub test_duration: Duration,
}

#[derive(Clone, Debug)]
struct ScannerTestConfig {
    name: String,
    config: ScannerConfig,
}

impl Default for ScannerBenchmarkConfig {
    fn default() -> Self {
        Self {
            object_counts: vec![100, 500, 1000, 5000],
            object_sizes: vec![1024, 10_240, 102_400, 1_048_576], // 1KB to 1MB
            scanner_configs: vec![
                ScannerTestConfig {
                    name: "default".to_string(),
                    config: ScannerConfig::default(),
                },
                ScannerTestConfig {
                    name: "fast_scan".to_string(),
                    config: ScannerConfig {
                        scan_interval: Duration::from_secs(30),
                        enable_healing: true,
                        enable_metrics: true,
                        enable_data_usage_stats: true,
                        ..Default::default()
                    },
                },
                ScannerTestConfig {
                    name: "efficient_scan".to_string(),
                    config: ScannerConfig {
                        scan_interval: Duration::from_secs(60),
                        enable_healing: true,
                        enable_metrics: true,
                        enable_data_usage_stats: true,
                        ..Default::default()
                    },
                },
            ],
            test_duration: Duration::from_secs(60),
        }
    }
}

/// Scanner性能监控器
struct ScannerPerformanceMonitor {
    start_time: Instant,
    system: System,
    objects_scanned: Arc<AtomicU64>,
    bytes_scanned: Arc<AtomicU64>,
    scan_operations: Arc<AtomicU64>,
}

impl ScannerPerformanceMonitor {
    fn new() -> Self {
        let mut system = System::new_all();
        system.refresh_all();
        
        Self {
            start_time: Instant::now(),
            system,
            objects_scanned: Arc::new(AtomicU64::new(0)),
            bytes_scanned: Arc::new(AtomicU64::new(0)),
            scan_operations: Arc::new(AtomicU64::new(0)),
        }
    }
    
    fn record_object_scanned(&self, size: u64) {
        self.objects_scanned.fetch_add(1, Ordering::Relaxed);
        self.bytes_scanned.fetch_add(size, Ordering::Relaxed);
    }
    
    fn record_scan_operation(&self) {
        self.scan_operations.fetch_add(1, Ordering::Relaxed);
    }
    
    fn get_scan_metrics(&mut self) -> ScannerPerformanceMetrics {
        self.system.refresh_all();
        let duration = self.start_time.elapsed();
        let objects = self.objects_scanned.load(Ordering::Relaxed);
        let bytes = self.bytes_scanned.load(Ordering::Relaxed);
        let operations = self.scan_operations.load(Ordering::Relaxed);
        
        ScannerPerformanceMetrics {
            duration,
            objects_scanned: objects,
            bytes_scanned: bytes,
            scan_operations: operations,
            objects_per_second: (objects as f64) / duration.as_secs_f64(),
            bytes_per_second: (bytes as f64) / duration.as_secs_f64(),
            operations_per_second: (operations as f64) / duration.as_secs_f64(),
            cpu_usage: self.system.global_cpu_info().cpu_usage(),
            memory_usage: self.system.used_memory(),
        }
    }
}

#[derive(Debug, Clone)]
struct ScannerPerformanceMetrics {
    duration: Duration,
    objects_scanned: u64,
    bytes_scanned: u64,
    scan_operations: u64,
    objects_per_second: f64,
    bytes_per_second: f64,
    operations_per_second: f64,
    cpu_usage: f32,
    memory_usage: u64,
}

/// Scanner测试环境
struct ScannerTestEnvironment {
    temp_dir: TempDir,
    ecstore: Arc<ECStore>,
    scanner: Arc<Scanner>,
    runtime: Runtime,
}

impl ScannerTestEnvironment {
    async fn new(scanner_config: ScannerTestConfig) -> anyhow::Result<Self> {
        let temp_dir = TempDir::new()?;
        let runtime = Runtime::new()?;
        
        // 创建ECStore实例
        let ecstore = Self::create_test_ecstore(&temp_dir).await?;
        
        // 创建Scanner
        let scanner = Arc::new(Scanner::new(Some(scanner_config.config), None));
        
        Ok(ScannerTestEnvironment {
            temp_dir,
            ecstore,
            scanner,
            runtime,
        })
    }
    
    async fn create_test_ecstore(temp_dir: &TempDir) -> anyhow::Result<Arc<ECStore>> {
        // 创建测试磁盘路径
        let disk_paths = (0..4)
            .map(|i| temp_dir.path().join(format!("disk{}", i)))
            .collect::<Vec<_>>();

        // 创建磁盘目录
        for path in &disk_paths {
            tokio::fs::create_dir_all(path).await?;
        }

        // 构建endpoints
        let mut endpoints: Vec<Endpoint> = disk_paths
            .iter()
            .map(|p| Endpoint::try_from(p.to_string_lossy().as_ref()).unwrap())
            .collect();
        
        // 正确设置Endpoint索引
        for (i, endpoint) in endpoints.iter_mut().enumerate() {
            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(i);
        }

        let pool_endpoints = PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: endpoints.len(),  // 使用实际endpoint数量
            endpoints: Endpoints::from(endpoints),
            cmd_line: "scanner_bench".to_string(),
            platform: format!("OS: {} | Arch: {}", std::env::consts::OS, std::env::consts::ARCH),
        };

        let endpoint_pools = EndpointServerPools(vec![pool_endpoints]);
        rustfs_ecstore::store::init_local_disks(endpoint_pools.clone()).await?;

        let server_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        
        // 检查是否已经存在全局ECStore实例，如果存在则复用
        if let Some(existing_store) = ecstore::new_object_layer_fn() {
            return Ok(existing_store);
        }
        
        let ecstore = ECStore::new(server_addr, endpoint_pools).await?;

        // 初始化bucket元数据系统
        let buckets_list = ecstore
            .list_bucket(&rustfs_ecstore::store_api::BucketOptions {
                no_metadata: true,
                ..Default::default()
            })
            .await?;
        let buckets = buckets_list.into_iter().map(|v| v.name).collect();
        rustfs_ecstore::bucket::metadata_sys::init_bucket_metadata_sys(ecstore.clone(), buckets).await;

        Ok(ecstore)
    }
    
    /// 创建测试数据
    async fn create_test_data(&self, bucket: &str, object_count: usize, object_size: usize) -> anyhow::Result<()> {
        // 创建bucket
        self.ecstore.make_bucket(bucket, &Default::default()).await?;
        
        // 创建测试对象
        let test_data = vec![b'T'; object_size];
        
        for i in 0..object_count {
            let object_name = format!("test-object-{:06}", i);
            let mut put_reader = PutObjReader::from_vec(test_data.clone());
            
            self.ecstore
                .put_object(bucket, &object_name, &mut put_reader, &Default::default())
                .await?;
        }
        
        Ok(())
    }
    
    async fn start_scanner(&self) -> anyhow::Result<()> {
        self.scanner.start().await?;
        Ok(())
    }
    
    async fn stop_scanner(&self) -> anyhow::Result<()> {
        self.scanner.stop().await?;
        Ok(())
    }
    
    async fn run_scan_cycle(&self) -> anyhow::Result<()> {
        self.scanner.scan_cycle().await?;
        Ok(())
    }
    
    async fn get_scanner_metrics(&self) -> anyhow::Result<()> {
        let _ = self.scanner.get_metrics().await;
        Ok(())
    }
}

/// 扫描速度基准测试
async fn benchmark_scan_speed(
    env: &ScannerTestEnvironment,
    object_count: usize,
    object_size: usize,
    monitor: &ScannerPerformanceMonitor,
) -> anyhow::Result<ScannerPerformanceMetrics> {
    let bucket_name = "scan-speed-bucket";
    
    // 创建测试数据
    env.create_test_data(bucket_name, object_count, object_size).await?;
    
    // 开始扫描
    let start_time = Instant::now();
    env.run_scan_cycle().await?;
    let scan_duration = start_time.elapsed();
    
    // 记录扫描指标
    monitor.record_scan_operation();
    let total_bytes = (object_count * object_size) as u64;
    monitor.bytes_scanned.store(total_bytes, Ordering::Relaxed);
    monitor.objects_scanned.store(object_count as u64, Ordering::Relaxed);
    
    let mut monitor_clone = monitor.clone();
    let mut metrics = monitor_clone.get_scan_metrics();
    metrics.duration = scan_duration;
    
    Ok(metrics)
}

/// 扫描吞吐量基准测试
async fn benchmark_scan_throughput(
    env: &ScannerTestEnvironment,
    object_count: usize,
    object_size: usize,
    monitor: &ScannerPerformanceMonitor,
) -> anyhow::Result<ScannerPerformanceMetrics> {
    let bucket_name = "scan-throughput-bucket";
    
    // 创建测试数据
    env.create_test_data(bucket_name, object_count, object_size).await?;
    
    // 持续扫描一段时间
    let test_duration = Duration::from_secs(30);
    let start_time = Instant::now();
    let mut scan_cycles = 0;
    
    while start_time.elapsed() < test_duration {
        env.run_scan_cycle().await?;
        monitor.record_scan_operation();
        scan_cycles += 1;
        
        // 短暂延迟避免过度压力
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    
    // 记录总体指标
    let total_bytes = (object_count * object_size * scan_cycles) as u64;
    let total_objects = (object_count * scan_cycles) as u64;
    monitor.bytes_scanned.store(total_bytes, Ordering::Relaxed);
    monitor.objects_scanned.store(total_objects, Ordering::Relaxed);
    
    let mut monitor_clone = monitor.clone();
    let metrics = monitor_clone.get_scan_metrics();
    
    Ok(metrics)
}

impl Clone for ScannerPerformanceMonitor {
    fn clone(&self) -> Self {
        Self {
            start_time: self.start_time,
            system: System::new(),
            objects_scanned: self.objects_scanned.clone(),
            bytes_scanned: self.bytes_scanned.clone(),
            scan_operations: self.scan_operations.clone(),
        }
    }
}

/// Scanner性能基准测试主函数
fn benchmark_scanner_performance(c: &mut Criterion) {
    let config = ScannerBenchmarkConfig::default();
    let rt = Runtime::new().unwrap();
    
    // 为每种Scanner配置创建测试组
    for scanner_config in &config.scanner_configs {
        let group_name = format!("scanner_performance_{}", scanner_config.name);
        let mut group = c.benchmark_group(&group_name);
        
        for &object_count in &config.object_counts {
            for &object_size in &config.object_sizes {
                group.throughput(Throughput::Bytes(object_count as u64 * object_size as u64));
                
                // 扫描速度测试
                group.bench_with_input(
                    BenchmarkId::new("scan_speed", format!("{}objs_{}bytes", object_count, object_size)),
                    &(object_count, object_size),
                    |b, &(obj_count, obj_size)| {
                        b.iter(|| {
                        rt.block_on(async {
                            let env = ScannerTestEnvironment::new(scanner_config.clone()).await.unwrap();
                            let monitor = ScannerPerformanceMonitor::new();
                            
                            let metrics = benchmark_scan_speed(&env, obj_count, obj_size, &monitor).await.unwrap();
                            
                            metrics
                            })
                    });
                    }
                );
                
                // 扫描吞吐量测试
                group.bench_with_input(
                    BenchmarkId::new("scan_throughput", format!("{}objs_{}bytes", object_count, object_size)),
                    &(object_count, object_size),
                    |b, &(obj_count, obj_size)| {
                        b.iter(|| {
                        rt.block_on(async {
                            let env = ScannerTestEnvironment::new(scanner_config.clone()).await.unwrap();
                            let monitor = ScannerPerformanceMonitor::new();
                            
                            let metrics = benchmark_scan_throughput(&env, obj_count, obj_size, &monitor).await.unwrap();
                            
                            metrics
                            })
                    });
                    }
                );
            }
        }
        
        group.finish();
    }
}

/// Scanner内存使用基准测试
fn benchmark_scanner_memory_usage(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("scanner_memory_usage");
    
    // 测试不同数据量下的内存使用
    for object_count in [1000, 5000, 10000, 50000] {
        group.bench_function(format!("memory_usage_{}objects", object_count), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let config = ScannerTestConfig {
                        name: "memory_test".to_string(),
                        config: ScannerConfig::default(),
                    };
                    
                    let env = ScannerTestEnvironment::new(config).await.unwrap();
                    let monitor = ScannerPerformanceMonitor::new();
                    
                    // 创建大量测试数据
                    env.create_test_data("memory-test-bucket", object_count, 1024).await.unwrap();
                    
                    // 执行扫描并监控内存使用
                    env.start_scanner().await.unwrap();
                    tokio::time::sleep(Duration::from_secs(10)).await; // 让Scanner运行一段时间
                    env.stop_scanner().await.unwrap();
                    
                    let mut monitor_clone = monitor.clone();
                    let metrics = monitor_clone.get_scan_metrics();
                    
                    metrics.memory_usage
                })
            });
        });
    }
    
    group.finish();
}

/// Scanner CPU使用基准测试
fn benchmark_scanner_cpu_usage(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("scanner_cpu_usage");
    
    // 测试不同扫描模式下的CPU使用
    let configs = vec![
        ScannerTestConfig {
            name: "light_scan".to_string(),
            config: ScannerConfig {
                scan_interval: Duration::from_secs(300), // 5分钟间隔
                enable_healing: false,
                enable_metrics: true,
                enable_data_usage_stats: true,
                ..Default::default()
            },
        },
        ScannerTestConfig {
            name: "intensive_scan".to_string(),
            config: ScannerConfig {
                scan_interval: Duration::from_secs(30), // 30秒间隔
                enable_healing: true,
                enable_metrics: true,
                enable_data_usage_stats: true,
                ..Default::default()
            },
        },
    ];
    
    for config in configs {
        group.bench_function(format!("cpu_usage_{}", config.name), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let env = ScannerTestEnvironment::new(config.clone()).await.unwrap();
                    let monitor = ScannerPerformanceMonitor::new();
                    
                    // 创建测试数据
                    env.create_test_data("cpu-test-bucket", 1000, 10240).await.unwrap();
                    
                    // 运行Scanner并监控CPU使用
                    env.start_scanner().await.unwrap();
                    tokio::time::sleep(Duration::from_secs(15)).await;
                    env.stop_scanner().await.unwrap();
                    
                    let mut monitor_clone = monitor.clone();
                    let metrics = monitor_clone.get_scan_metrics();
                    
                    metrics.cpu_usage
                })
            });
        });
    }
    
    group.finish();
}

criterion_group!(
    benches,
    benchmark_scanner_performance,
    benchmark_scanner_memory_usage,
    benchmark_scanner_cpu_usage,
);
criterion_main!(benches);