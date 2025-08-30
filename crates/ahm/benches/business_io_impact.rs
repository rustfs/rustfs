use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use rustfs_ahm::scanner::data_scanner::{Scanner, ScannerConfig};
use rustfs_ecstore::{self as ecstore, StorageAPI, endpoints::{EndpointServerPools, PoolEndpoints, Endpoints}};
use rustfs_ecstore::store_api::ObjectIO;
use rustfs_ecstore::disk::endpoint::Endpoint;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::net::SocketAddr;
use tempfile::TempDir;
use tokio::runtime::Runtime;
use sysinfo::System;
use std::sync::atomic::{AtomicU64, Ordering};

/// 基准测试配置
struct BenchmarkConfig {
    /// 测试数据大小（字节）
    pub data_sizes: Vec<usize>,
    /// 并发请求数
    pub concurrency_levels: Vec<usize>,
    /// Scanner配置选项
    pub scanner_modes: Vec<ScannerMode>,
    /// 测试持续时间
    pub test_duration: Duration,
}

#[derive(Clone, Debug)]
enum ScannerMode {
    Disabled,           // Scanner关闭
    LegacyEnabled,      // 传统Scanner开启（高并发模式）
    OptimizedEnabled,   // 优化后Scanner开启（串行模式）
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            data_sizes: vec![1024, 10_240, 102_400, 1_048_576], // 1KB to 1MB
            concurrency_levels: vec![1, 4, 8, 16, 32],
            scanner_modes: vec![
                ScannerMode::Disabled,
                ScannerMode::LegacyEnabled,
                ScannerMode::OptimizedEnabled,
            ],
            test_duration: Duration::from_secs(30),
        }
    }
}

/// 性能监控器
#[derive(Debug)]
struct PerformanceMonitor {
    start_time: Instant,
    start_cpu: f32,
    start_memory: u64,
    iops_counter: Arc<AtomicU64>,
    system: System,
}

impl PerformanceMonitor {
    fn new() -> Self {
        let mut system = System::new_all();
        system.refresh_all();
        
        Self {
            start_time: Instant::now(),
            start_cpu: system.global_cpu_info().cpu_usage(),
            start_memory: system.used_memory(),
            iops_counter: Arc::new(AtomicU64::new(0)),
            system,
        }
    }
    
    fn record_io_operation(&self) {
        self.iops_counter.fetch_add(1, Ordering::Relaxed);
    }
    
    fn get_metrics(&mut self) -> PerformanceMetrics {
        self.system.refresh_all();
        let duration = self.start_time.elapsed();
        let iops = self.iops_counter.load(Ordering::Relaxed);
        
        PerformanceMetrics {
            duration,
            cpu_usage: self.system.global_cpu_info().cpu_usage() - self.start_cpu,
            memory_usage: self.system.used_memory() - self.start_memory,
            total_iops: iops,
            avg_iops: (iops as f64) / duration.as_secs_f64(),
        }
    }
}

#[derive(Debug, Clone)]
struct PerformanceMetrics {
    duration: Duration,
    cpu_usage: f32,
    memory_usage: u64,
    total_iops: u64,
    avg_iops: f64,
}

/// 测试环境设置
struct TestEnvironment {
    temp_dir: TempDir,
    ecstore: Arc<ecstore::store::ECStore>,
    scanner: Option<Arc<Scanner>>,
}

impl TestEnvironment {
    async fn new(scanner_mode: ScannerMode) -> anyhow::Result<Self> {
        let temp_dir = TempDir::new()?;
        
        // 创建ECStore实例（简化实现）
        let ecstore = Self::create_test_ecstore(&temp_dir).await?;
        
        // 根据模式创建Scanner
        let scanner = match scanner_mode {
            ScannerMode::Disabled => None,
            ScannerMode::LegacyEnabled => {
                let config = ScannerConfig {
                    scan_interval: Duration::from_secs(60),
                    enable_healing: true,
                    // 传统高并发模式 - 模拟原有的高并发扫描
                    ..Default::default()
                };
                Some(Arc::new(Scanner::new(Some(config), None)))
            },
            ScannerMode::OptimizedEnabled => {
                let config = ScannerConfig {
                    scan_interval: Duration::from_secs(30),
                    enable_healing: true,
                    // 优化后串行模式 - 使用新的串行扫描策略
                    ..Default::default()
                };
                Some(Arc::new(Scanner::new(Some(config), None)))
            },
        };
        
        Ok(TestEnvironment {
            temp_dir,
            ecstore,
            scanner,
        })
    }
    
    async fn create_test_ecstore(temp_dir: &TempDir) -> anyhow::Result<Arc<ecstore::store::ECStore>> {
        // 创建测试磁盘路径
        let disk_paths = (0..4)
            .map(|i| temp_dir.path().join(format!("disk{}", i)))
            .collect::<Vec<_>>();

        // 创建磁盘目录
        for path in &disk_paths {
            tokio::fs::create_dir_all(path).await?;
            
            // 创建.rustfs.sys目录结构
            let sys_dir = path.join(".rustfs.sys");
            tokio::fs::create_dir_all(&sys_dir).await?;
            
            // 创建必要的子目录
            tokio::fs::create_dir_all(sys_dir.join("multipart")).await?;
            tokio::fs::create_dir_all(sys_dir.join("tmp")).await?;
            tokio::fs::create_dir_all(sys_dir.join("scanner")).await?;
        }

        // 构建 endpoints
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
            cmd_line: "benchmark".to_string(),
            platform: format!("OS: {} | Arch: {}", std::env::consts::OS, std::env::consts::ARCH),
        };

        let endpoint_pools = EndpointServerPools(vec![pool_endpoints]);
        rustfs_ecstore::store::init_local_disks(endpoint_pools.clone()).await?;

        let server_addr: SocketAddr = "127.0.0.1:0".parse().unwrap(); // 使用动态端口
        
        // 检查是否已经存在全局ECStore实例，如果存在则复用
        if let Some(existing_store) = ecstore::new_object_layer_fn() {
            return Ok(existing_store);
        }
        
        let ecstore = ecstore::store::ECStore::new(server_addr, endpoint_pools).await?;

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
    
    async fn start_scanner(&self) -> anyhow::Result<()> {
        if let Some(scanner) = &self.scanner {
            scanner.start().await?;
        }
        Ok(())
    }
    
    async fn stop_scanner(&self) -> anyhow::Result<()> {
        if let Some(scanner) = &self.scanner {
            scanner.stop().await?;
        }
        Ok(())
    }
}

/// PUT操作基准测试
async fn benchmark_put_operations(
    env: &TestEnvironment,
    data_size: usize,
    concurrency: usize,
    monitor: Arc<PerformanceMonitor>,
) -> anyhow::Result<PerformanceMetrics> {
    let test_data = vec![b'A'; data_size];
    let bucket_name = "benchmark-bucket";
    
    // 创建bucket
    env.ecstore.as_ref().make_bucket(bucket_name, &Default::default()).await?;
    
    // 并发PUT操作
    let mut handles = Vec::new();
    for i in 0..concurrency {
        let ecstore = env.ecstore.clone();
        let data = test_data.clone();
        let monitor_clone = monitor.clone();
        let object_name = format!("test-object-{}", i);
        
        let handle = tokio::spawn(async move {
            let mut put_reader = rustfs_ecstore::store_api::PutObjReader::from_vec(data);
            let start = Instant::now();
            
            match ecstore.as_ref().put_object(bucket_name, &object_name, &mut put_reader, &Default::default()).await {
                Ok(_) => {
                    monitor_clone.record_io_operation();
                    Ok(start.elapsed())
                },
                Err(e) => Err(e.into()),
            }
        });
        handles.push(handle);
    }
    
    // 等待所有操作完成
    let mut total_latency = Duration::ZERO;
    for handle in handles {
        match handle.await? {
            Ok(latency) => total_latency += latency,
            Err(e) => return Err(e),
        }
    }
    
    let mut monitor_mut = Arc::try_unwrap(monitor).unwrap();
    let mut metrics = monitor_mut.get_metrics();
    metrics.duration = total_latency / concurrency as u32;
    
    Ok(metrics)
}

/// GET操作基准测试
async fn benchmark_get_operations(
    env: &TestEnvironment,
    object_count: usize,
    concurrency: usize,
    monitor: Arc<PerformanceMonitor>,
) -> anyhow::Result<PerformanceMetrics> {
    let bucket_name = "benchmark-bucket";
    
    // 并发GET操作
    let mut handles = Vec::new();
    for i in 0..concurrency {
        let ecstore = env.ecstore.clone();
        let monitor_clone = monitor.clone();
        let object_name = format!("test-object-{}", i % object_count);
        
        let handle = tokio::spawn(async move {
            let start = Instant::now();
            
            match ecstore.as_ref().get_object_info(bucket_name, &object_name, &Default::default()).await {
                Ok(_) => {
                    monitor_clone.record_io_operation();
                    Ok(start.elapsed())
                },
                Err(e) => Err(e.into()),
            }
        });
        handles.push(handle);
    }
    
    // 等待所有操作完成
    let mut total_latency = Duration::ZERO;
    for handle in handles {
        match handle.await? {
            Ok(latency) => total_latency += latency,
            Err(e) => return Err(e),
        }
    }
    
    let mut monitor_mut = Arc::try_unwrap(monitor).unwrap();
    let mut metrics = monitor_mut.get_metrics();
    metrics.duration = total_latency / concurrency as u32;
    
    Ok(metrics)
}

/// 基准测试主函数
fn benchmark_business_io_impact(c: &mut Criterion) {
    let config = BenchmarkConfig::default();
    
    // 为每种Scanner模式创建测试组
    for scanner_mode in &config.scanner_modes {
        let group_name = match scanner_mode {
            ScannerMode::Disabled => "business_io_scanner_disabled",
            ScannerMode::LegacyEnabled => "business_io_legacy_scanner",
            ScannerMode::OptimizedEnabled => "business_io_optimized_scanner",
        };
        
        let mut group = c.benchmark_group(group_name);
        
        for &data_size in &config.data_sizes {
            for &concurrency in &config.concurrency_levels {
                group.throughput(Throughput::Bytes(data_size as u64 * concurrency as u64));
                
                // PUT操作基准测试
                group.bench_with_input(
                    BenchmarkId::new("put", format!("{}b_{}threads", data_size, concurrency)),
                    &(data_size, concurrency),
                    |b, &(size, conc)| {
                        b.iter(|| {
                            let rt = Runtime::new().unwrap();
                            let result = rt.block_on(async {
                                let env = TestEnvironment::new(scanner_mode.clone()).await.unwrap();
                                env.start_scanner().await.unwrap();
                                
                                let monitor = Arc::new(PerformanceMonitor::new());
                                let metrics = benchmark_put_operations(&env, size, conc, monitor).await.unwrap();
                                
                                env.stop_scanner().await.unwrap();
                                metrics
                            });
                            // 在运行时外部清理
                            drop(rt);
                            result
                        });
                    }
                );
                
                // GET操作基准测试
                group.bench_with_input(
                    BenchmarkId::new("get", format!("{}objects_{}threads", data_size / 1024, concurrency)),
                    &(data_size, concurrency),
                    |b, &(obj_count, conc)| {
                        b.iter(|| {
                            let rt = Runtime::new().unwrap();
                            let result = rt.block_on(async {
                                let env = TestEnvironment::new(scanner_mode.clone()).await.unwrap();
                                env.start_scanner().await.unwrap();
                                
                                let monitor = Arc::new(PerformanceMonitor::new());
                                let metrics = benchmark_get_operations(&env, obj_count / 1024, conc, monitor).await.unwrap();
                                
                                env.stop_scanner().await.unwrap();
                                metrics
                            });
                            // 在运行时外部清理
                            drop(rt);
                            result
                        });
                    }
                );
            }
        }
        
        group.finish();
    }
}

/// 延迟分布基准测试
fn benchmark_latency_distribution(c: &mut Criterion) {
    let mut group = c.benchmark_group("latency_distribution");
    
    // 测试不同Scanner模式下的延迟分布
    for scanner_mode in [ScannerMode::Disabled, ScannerMode::OptimizedEnabled] {
        let mode_name = match scanner_mode {
            ScannerMode::Disabled => "no_scanner",
            ScannerMode::OptimizedEnabled => "optimized_scanner",
            _ => continue,
        };
        
        group.bench_function(format!("p99_latency_{}", mode_name), |b| {
            b.iter(|| {
                let rt = Runtime::new().unwrap();
                let result = rt.block_on(async {
                    let env = TestEnvironment::new(scanner_mode.clone()).await.unwrap();
                    env.start_scanner().await.unwrap();
                    
                    // 执行大量操作并收集延迟数据
                    let mut latencies = Vec::new();
                    for _ in 0..1000 {
                        let start = Instant::now();
                        // 执行一个简单的操作
                        let _ = env.ecstore.as_ref().list_bucket(&Default::default()).await;
                        latencies.push(start.elapsed());
                    }
                    
                    env.stop_scanner().await.unwrap();
                    
                    // 计算P99延迟
                    latencies.sort();
                    latencies[(latencies.len() * 99) / 100]
                });
                // 在运行时外部清理
                drop(rt);
                result
            });
        });
    }
    
    group.finish();
}

criterion_group!(
    benches,
    benchmark_business_io_impact,
    benchmark_latency_distribution,
);
criterion_main!(benches);