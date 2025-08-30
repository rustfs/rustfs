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
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::collections::HashMap;
use tokio::sync::Semaphore;
use rand::Rng;

/// 资源竞争基准测试配置
struct ResourceContentionConfig {
    /// 业务IO负载级别
    pub business_load_levels: Vec<BusinessLoadLevel>,
    /// 并发业务请求数
    pub concurrent_requests: Vec<usize>,
    /// Scanner配置
    pub scanner_configs: Vec<ScannerContentionConfig>,
    /// 测试持续时间
    pub test_duration: Duration,
}

#[derive(Clone, Debug)]
enum BusinessLoadLevel {
    Low,     // 低负载（10% 磁盘使用率）
    Medium,  // 中负载（50% 磁盘使用率）
    High,    // 高负载（80% 磁盘使用率）
    Peak,    // 峰值负载（95% 磁盘使用率）
}

#[derive(Clone, Debug)]
struct ScannerContentionConfig {
    name: String,
    config: ScannerConfig,
    description: String,
}

impl Default for ResourceContentionConfig {
    fn default() -> Self {
        Self {
            business_load_levels: vec![
                BusinessLoadLevel::Low,
                BusinessLoadLevel::Medium,
                BusinessLoadLevel::High,
                BusinessLoadLevel::Peak,
            ],
            concurrent_requests: vec![1, 5, 10, 20, 50],
            scanner_configs: vec![
                ScannerContentionConfig {
                    name: "disabled".to_string(),
                    config: ScannerConfig {
                        scan_interval: Duration::from_secs(0), // 禁用
                        enable_healing: false,
                        enable_metrics: false,
                        enable_data_usage_stats: false,
                        ..Default::default()
                    },
                    description: "Scanner完全禁用，作为基准对照".to_string(),
                },
                ScannerContentionConfig {
                    name: "legacy_aggressive".to_string(),
                    config: ScannerConfig {
                        scan_interval: Duration::from_secs(30), // 激进扫描
                        enable_healing: true,
                        enable_metrics: true,
                        enable_data_usage_stats: true,
                        ..Default::default()
                    },
                    description: "传统激进扫描模式，模拟现有问题".to_string(),
                },
                ScannerContentionConfig {
                    name: "optimized_gentle".to_string(),
                    config: ScannerConfig {
                        scan_interval: Duration::from_secs(300), // 温和扫描
                        enable_healing: true,
                        enable_metrics: true,
                        enable_data_usage_stats: true,
                        ..Default::default()
                    },
                    description: "优化后温和扫描模式，减少竞争".to_string(),
                },
            ],
            test_duration: Duration::from_secs(60),
        }
    }
}

/// 资源竞争监控器
#[derive(Debug)]
struct ResourceContentionMonitor {
    start_time: Instant,
    system: System,
    business_operations: Arc<AtomicU64>,
    business_total_latency: Arc<AtomicU64>, // 纳秒
    business_max_latency: Arc<AtomicU64>,   // 纳秒
    business_errors: Arc<AtomicU64>,
    
    // Scanner指标
    scanner_operations: Arc<AtomicU64>,
    scanner_total_latency: Arc<AtomicU64>,
    scanner_cpu_usage: Arc<AtomicU64>,
    
    // 系统资源指标
    peak_cpu_usage: Arc<AtomicU64>,
    peak_memory_usage: Arc<AtomicU64>,
    disk_queue_depth: Arc<AtomicU64>,
    
    // 竞争指标
    io_wait_time: Arc<AtomicU64>,
    contention_count: Arc<AtomicU64>,
}

impl ResourceContentionMonitor {
    fn new() -> Self {
        let mut system = System::new_all();
        system.refresh_all();
        
        Self {
            start_time: Instant::now(),
            system,
            business_operations: Arc::new(AtomicU64::new(0)),
            business_total_latency: Arc::new(AtomicU64::new(0)),
            business_max_latency: Arc::new(AtomicU64::new(0)),
            business_errors: Arc::new(AtomicU64::new(0)),
            scanner_operations: Arc::new(AtomicU64::new(0)),
            scanner_total_latency: Arc::new(AtomicU64::new(0)),
            scanner_cpu_usage: Arc::new(AtomicU64::new(0)),
            peak_cpu_usage: Arc::new(AtomicU64::new(0)),
            peak_memory_usage: Arc::new(AtomicU64::new(0)),
            disk_queue_depth: Arc::new(AtomicU64::new(0)),
            io_wait_time: Arc::new(AtomicU64::new(0)),
            contention_count: Arc::new(AtomicU64::new(0)),
        }
    }
    
    fn record_business_operation(&self, latency: Duration, is_error: bool) {
        self.business_operations.fetch_add(1, Ordering::Relaxed);
        
        let latency_nanos = latency.as_nanos() as u64;
        self.business_total_latency.fetch_add(latency_nanos, Ordering::Relaxed);
        
        // 更新最大延迟
        let mut current_max = self.business_max_latency.load(Ordering::Relaxed);
        while latency_nanos > current_max {
            match self.business_max_latency.compare_exchange_weak(
                current_max, 
                latency_nanos, 
                Ordering::Relaxed, 
                Ordering::Relaxed
            ) {
                Ok(_) => break,
                Err(new_max) => current_max = new_max,
            }
        }
        
        if is_error {
            self.business_errors.fetch_add(1, Ordering::Relaxed);
        }
        
        // 检测潜在的资源竞争
        if latency > Duration::from_millis(500) {
            self.contention_count.fetch_add(1, Ordering::Relaxed);
        }
    }
    
    fn record_scanner_operation(&self, latency: Duration) {
        self.scanner_operations.fetch_add(1, Ordering::Relaxed);
        self.scanner_total_latency.fetch_add(latency.as_nanos() as u64, Ordering::Relaxed);
    }
    
    fn update_system_metrics(&mut self) {
        self.system.refresh_all();
        
        let cpu_usage = (self.system.global_cpu_info().cpu_usage() * 100.0) as u64;
        let memory_usage = self.system.used_memory() / 1024 / 1024; // MB
        
        // 更新峰值CPU使用率
        let mut current_cpu = self.peak_cpu_usage.load(Ordering::Relaxed);
        while cpu_usage > current_cpu {
            match self.peak_cpu_usage.compare_exchange_weak(
                current_cpu, 
                cpu_usage, 
                Ordering::Relaxed, 
                Ordering::Relaxed
            ) {
                Ok(_) => break,
                Err(new_cpu) => current_cpu = new_cpu,
            }
        }
        
        // 更新峰值内存使用
        let mut current_memory = self.peak_memory_usage.load(Ordering::Relaxed);
        while memory_usage > current_memory {
            match self.peak_memory_usage.compare_exchange_weak(
                current_memory, 
                memory_usage, 
                Ordering::Relaxed, 
                Ordering::Relaxed
            ) {
                Ok(_) => break,
                Err(new_memory) => current_memory = new_memory,
            }
        }
    }
    
    fn get_contention_metrics(&mut self) -> ResourceContentionMetrics {
        self.update_system_metrics();
        
        let duration = self.start_time.elapsed();
        let business_ops = self.business_operations.load(Ordering::Relaxed);
        let business_total_latency = self.business_total_latency.load(Ordering::Relaxed);
        let scanner_ops = self.scanner_operations.load(Ordering::Relaxed);
        
        ResourceContentionMetrics {
            duration,
            business_operations: business_ops,
            business_avg_latency: if business_ops > 0 {
                Duration::from_nanos(business_total_latency / business_ops)
            } else {
                Duration::ZERO
            },
            business_max_latency: Duration::from_nanos(self.business_max_latency.load(Ordering::Relaxed)),
            business_error_rate: if business_ops > 0 {
                (self.business_errors.load(Ordering::Relaxed) as f64) / (business_ops as f64)
            } else {
                0.0
            },
            business_throughput: (business_ops as f64) / duration.as_secs_f64(),
            scanner_operations: scanner_ops,
            scanner_avg_latency: if scanner_ops > 0 {
                Duration::from_nanos(self.scanner_total_latency.load(Ordering::Relaxed) / scanner_ops)
            } else {
                Duration::ZERO
            },
            peak_cpu_usage: self.peak_cpu_usage.load(Ordering::Relaxed) as f32 / 100.0,
            peak_memory_usage: self.peak_memory_usage.load(Ordering::Relaxed),
            contention_events: self.contention_count.load(Ordering::Relaxed),
            contention_rate: (self.contention_count.load(Ordering::Relaxed) as f64) / (business_ops as f64),
        }
    }
}

#[derive(Debug, Clone)]
struct ResourceContentionMetrics {
    duration: Duration,
    business_operations: u64,
    business_avg_latency: Duration,
    business_max_latency: Duration,
    business_error_rate: f64,
    business_throughput: f64,
    scanner_operations: u64,
    scanner_avg_latency: Duration,
    peak_cpu_usage: f32,
    peak_memory_usage: u64,
    contention_events: u64,
    contention_rate: f64,
}

/// 资源竞争测试环境
struct ResourceContentionTestEnvironment {
    temp_dir: TempDir,
    ecstore: Arc<ECStore>,
    scanner: Option<Arc<Scanner>>,
    runtime: Runtime,
    stop_signal: Arc<AtomicBool>,
}

impl ResourceContentionTestEnvironment {
    async fn new(scanner_config: ScannerContentionConfig) -> anyhow::Result<Self> {
        let temp_dir = TempDir::new()?;
        let runtime = Runtime::new()?;
        
        // 创建ECStore实例
        let ecstore = Self::create_test_ecstore(&temp_dir).await?;
        
        // 创建Scanner（如果配置需要）
        let scanner = if scanner_config.config.scan_interval.as_secs() > 0 {
            Some(Arc::new(Scanner::new(Some(scanner_config.config), None)))
        } else {
            None
        };
        
        Ok(ResourceContentionTestEnvironment {
            temp_dir,
            ecstore,
            scanner,
            runtime,
            stop_signal: Arc::new(AtomicBool::new(false)),
        })
    }
    
    async fn create_test_ecstore(temp_dir: &TempDir) -> anyhow::Result<Arc<ECStore>> {
        let disk_paths: Vec<_> = (0..4)
            .map(|i| temp_dir.path().join(format!("disk{}", i)))
            .collect();

        for path in &disk_paths {
            tokio::fs::create_dir_all(path).await?;
        }

        let mut endpoints: Vec<Endpoint> = disk_paths.iter()
            .map(|p| Endpoint::try_from(p.to_string_lossy().as_ref()).unwrap())
            .collect();
        
        // 正确设置Endpoint索引
        for (i, endpoint) in endpoints.iter_mut().enumerate() {
            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(i);
        }

        let pool_endpoints = PoolEndpoints {
            legacy: false, set_count: 1, drives_per_set: endpoints.len(),
            endpoints: Endpoints::from(endpoints),
            cmd_line: "contention".to_string(),
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

        let buckets_list = ecstore.list_bucket(&rustfs_ecstore::store_api::BucketOptions {
            no_metadata: true, ..Default::default()
        }).await?;
        let buckets = buckets_list.into_iter().map(|v| v.name).collect();
        rustfs_ecstore::bucket::metadata_sys::init_bucket_metadata_sys(ecstore.clone(), buckets).await;

        Ok(ecstore)
    }
    
    /// 启动Scanner（如果存在）
    async fn start_scanner(&self) -> anyhow::Result<()> {
        if let Some(scanner) = &self.scanner {
            scanner.start().await?;
        }
        Ok(())
    }
    
    /// 停止Scanner（如果存在）
    async fn stop_scanner(&self) -> anyhow::Result<()> {
        if let Some(scanner) = &self.scanner {
            scanner.stop().await?;
        }
        Ok(())
    }
    
    /// 创建测试数据
    async fn prepare_test_data(&self, bucket: &str, object_count: usize) -> anyhow::Result<()> {
        // 创建bucket
        self.ecstore.make_bucket(bucket, &Default::default()).await?;
        
        // 创建一些基础测试对象
        let test_data = vec![b'C'; 10240]; // 10KB对象
        
        for i in 0..object_count {
            let object_name = format!("base-object-{:06}", i);
            let mut put_reader = PutObjReader::from_vec(test_data.clone());
            
            self.ecstore
                .put_object(bucket, &object_name, &mut put_reader, &Default::default())
                .await?;
        }
        
        Ok(())
    }
    
    /// 模拟业务IO负载
    async fn simulate_business_load(
        &self,
        load_level: BusinessLoadLevel,
        concurrent_requests: usize,
        duration: Duration,
        monitor: Arc<ResourceContentionMonitor>,
    ) -> anyhow::Result<()> {
        let bucket_name = "business-load-bucket";
        self.prepare_test_data(bucket_name, 100).await?;
        
        let semaphore = Arc::new(Semaphore::new(concurrent_requests));
        let mut handles = Vec::new();
        
        let start_time = Instant::now();
        let stop_signal = self.stop_signal.clone();
        
        // 根据负载级别确定操作频率
        let operation_interval = match load_level {
            BusinessLoadLevel::Low => Duration::from_millis(100),
            BusinessLoadLevel::Medium => Duration::from_millis(50),
            BusinessLoadLevel::High => Duration::from_millis(20),
            BusinessLoadLevel::Peak => Duration::from_millis(10),
        };
        
        // 启动并发业务IO
        for i in 0..concurrent_requests {
            let ecstore = self.ecstore.clone();
            let monitor = monitor.clone();
            let semaphore = semaphore.clone();
            let stop_signal = stop_signal.clone();
            let interval = operation_interval;
            
            let handle = tokio::spawn(async move {
                let mut operation_count = 0;
                
                while start_time.elapsed() < duration && !stop_signal.load(Ordering::Relaxed) {
                    let _permit = semaphore.acquire().await.unwrap();
                    
                    let op_start = Instant::now();
                    let object_name = format!("business-object-{}-{:06}", i, operation_count);
                    
                    // 随机选择PUT或GET操作
                    let is_put = operation_count % 3 == 0; // 33% PUT, 67% GET
                    let mut is_error = false;
                    
                    if is_put {
                        // PUT操作
                        let data = vec![b'B'; 5120]; // 5KB
                        let mut put_reader = PutObjReader::from_vec(data);
                        
                        if let Err(_) = ecstore.put_object(bucket_name, &object_name, &mut put_reader, &Default::default()).await {
                            is_error = true;
                        }
                    } else {
                        // GET操作
                        let existing_object = format!("base-object-{:06}", operation_count % 100);
                        if let Err(_) = ecstore.get_object_info(bucket_name, &existing_object, &Default::default()).await {
                            is_error = true;
                        }
                    }
                    
                    let latency = op_start.elapsed();
                    monitor.record_business_operation(latency, is_error);
                    
                    operation_count += 1;
                    
                    // 根据负载级别控制操作频率
                    tokio::time::sleep(interval).await;
                }
            });
            
            handles.push(handle);
        }
        
        // 等待所有业务IO完成
        for handle in handles {
            let _ = handle.await;
        }
        
        Ok(())
    }
    
    /// 设置停止信号
    fn signal_stop(&self) {
        self.stop_signal.store(true, Ordering::Relaxed);
    }
}

/// 业务IO与Scanner竞争基准测试
async fn benchmark_business_scanner_contention(
    env: &ResourceContentionTestEnvironment,
    load_level: BusinessLoadLevel,
    concurrent_requests: usize,
    monitor: Arc<ResourceContentionMonitor>,
) -> anyhow::Result<ResourceContentionMetrics> {
    let test_duration = Duration::from_secs(30);
    
    // 启动Scanner
    env.start_scanner().await?;
    
    // 启动系统指标监控
    let monitor_clone = monitor.clone();
    let monitor_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        loop {
            interval.tick().await;
            // 这里可以添加更详细的系统指标监控
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
    
    // 运行业务负载
    env.simulate_business_load(load_level, concurrent_requests, test_duration, monitor.clone()).await?;
    
    // 停止监控
    monitor_handle.abort();
    
    // 停止Scanner
    env.stop_scanner().await?;
    
    let mut monitor_mut = Arc::try_unwrap(monitor).unwrap();
    Ok(monitor_mut.get_contention_metrics())
}

/// 磁盘队列深度基准测试
async fn benchmark_disk_queue_depth(
    env: &ResourceContentionTestEnvironment,
    concurrent_requests: usize,
    monitor: Arc<ResourceContentionMonitor>,
) -> anyhow::Result<ResourceContentionMetrics> {
    let bucket_name = "queue-depth-bucket";
    env.prepare_test_data(bucket_name, 50).await?;
    
    // 启动Scanner
    env.start_scanner().await?;
    
    // 创建高IO负载以测试队列深度
    let mut handles = Vec::new();
    
    for i in 0..concurrent_requests {
        let ecstore = env.ecstore.clone();
        let monitor = monitor.clone();
        
        let handle = tokio::spawn(async move {
            for j in 0..100 {
                let op_start = Instant::now();
                
                // 大量连续IO操作
                let object_name = format!("queue-test-{}-{}", i, j);
                let data = vec![b'Q'; 20480]; // 20KB
                let mut put_reader = PutObjReader::from_vec(data);
                
                let is_error = ecstore.put_object(bucket_name, &object_name, &mut put_reader, &Default::default()).await.is_err();
                
                let latency = op_start.elapsed();
                monitor.record_business_operation(latency, is_error);
                
                // 模拟队列压力
                if j % 10 == 0 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        });
        
        handles.push(handle);
    }
    
    // 等待所有操作完成
    for handle in handles {
        let _ = handle.await;
    }
    
    env.stop_scanner().await?;
    
    let mut monitor_mut = Arc::try_unwrap(monitor).unwrap();
    Ok(monitor_mut.get_contention_metrics())
}

/// 资源竞争基准测试主函数
fn benchmark_resource_contention(c: &mut Criterion) {
    let config = ResourceContentionConfig::default();
    let rt = Runtime::new().unwrap();
    
    // 为每种Scanner配置创建测试组
    for scanner_config in &config.scanner_configs {
        let group_name = format!("resource_contention_{}", scanner_config.name);
        let mut group = c.benchmark_group(&group_name);
        
        for load_level in &config.business_load_levels {
            for &concurrent_requests in &config.concurrent_requests {
                let load_name = match load_level {
                    BusinessLoadLevel::Low => "low",
                    BusinessLoadLevel::Medium => "medium", 
                    BusinessLoadLevel::High => "high",
                    BusinessLoadLevel::Peak => "peak",
                };
                
                group.throughput(Throughput::Elements(concurrent_requests as u64));
                
                // 业务IO与Scanner竞争测试
                group.bench_with_input(
                    BenchmarkId::new("business_scanner_contention", format!("{}_load_{}conc", load_name, concurrent_requests)),
                    &(load_level.clone(), concurrent_requests),
                    |b, (load, conc)| {
                        b.iter(|| {
                            rt.block_on(async {
                                let env = ResourceContentionTestEnvironment::new(scanner_config.clone()).await.unwrap();
                                let monitor = Arc::new(ResourceContentionMonitor::new());
                                
                                let metrics = benchmark_business_scanner_contention(&env, load.clone(), *conc, monitor).await.unwrap();
                                
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

/// 磁盘IO竞争基准测试
fn benchmark_disk_io_contention(c: &mut Criterion) {
    let config = ResourceContentionConfig::default();
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("disk_io_contention");
    
    for scanner_config in &config.scanner_configs {
        for concurrent_requests in [5, 10, 20, 50] {
            group.bench_with_input(
                BenchmarkId::new(&scanner_config.name, format!("{}concurrent", concurrent_requests)),
                &concurrent_requests,
                |b, &conc| {
                    b.iter(|| {
                        rt.block_on(async {
                            let env = ResourceContentionTestEnvironment::new(scanner_config.clone()).await.unwrap();
                            let monitor = Arc::new(ResourceContentionMonitor::new());
                            
                            let metrics = benchmark_disk_queue_depth(&env, conc, monitor).await.unwrap();
                            
                            metrics
                        })
                    });
                }
            );
        }
    }
    
    group.finish();
}

/// 延迟分布对比基准测试
fn benchmark_latency_distribution_impact(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("latency_distribution_impact");
    
    // 对比Scanner开启前后的延迟分布
    let configs = [
        ScannerContentionConfig {
            name: "baseline".to_string(),
            config: ScannerConfig {
                scan_interval: Duration::from_secs(0), // 禁用
                enable_healing: false,
                enable_metrics: false,
                enable_data_usage_stats: false,
                ..Default::default()
            },
            description: "基准测试，无Scanner".to_string(),
        },
        ScannerContentionConfig {
            name: "scanner_enabled".to_string(),
            config: ScannerConfig {
                scan_interval: Duration::from_secs(60),
                enable_healing: true,
                enable_metrics: true,
                enable_data_usage_stats: true,
                ..Default::default()
            },
            description: "Scanner开启".to_string(),
        },
    ];
    
    for config in configs {
        group.bench_function(format!("p95_latency_{}", config.name), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let env = ResourceContentionTestEnvironment::new(config.clone()).await.unwrap();
                    let monitor = Arc::new(ResourceContentionMonitor::new());
                    
                    // 运行中等负载测试
                    let metrics = benchmark_business_scanner_contention(
                        &env, 
                        BusinessLoadLevel::Medium, 
                        10, 
                        monitor
                    ).await.unwrap();
                    
                    metrics.business_avg_latency
                })
            });
        });
    }
    
    group.finish();
}

criterion_group!(
    benches,
    benchmark_resource_contention,
    benchmark_disk_io_contention,
    benchmark_latency_distribution_impact,
);
criterion_main!(benches);