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
use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::VecDeque;
use rand::Rng;

/// 智能调度基准测试配置
#[derive(Clone, Debug)]
struct AdaptiveSchedulingConfig {
    name: String,
    base_interval: Duration,
    adaptive_enabled: bool,
    load_thresholds: LoadThresholds,
}

#[derive(Clone, Debug)]
struct LoadThresholds {
    low: f64,     // < 30%
    medium: f64,  // 30-60%
    high: f64,    // 60-80%
    critical: f64, // > 80%
}

impl Default for LoadThresholds {
    fn default() -> Self {
        Self { low: 0.3, medium: 0.6, high: 0.8, critical: 0.9 }
    }
}

/// 负载场景定义
#[derive(Clone, Debug)]
struct LoadScenario {
    name: String,
    phases: Vec<LoadPhase>,
}

#[derive(Clone, Debug)]
struct LoadPhase {
    load_level: f64,
    duration: Duration,
    requests_per_second: u32,
}

/// 智能调度监控器
struct AdaptiveMonitor {
    start_time: Instant,
    business_operations: Arc<AtomicU64>,
    business_latency_sum: Arc<AtomicU64>,
    scanner_adjustments: Arc<AtomicU64>,
    current_load: Arc<AtomicU64>, // load * 1000
}

impl AdaptiveMonitor {
    fn new() -> Self {
        Self {
            start_time: Instant::now(),
            business_operations: Arc::new(AtomicU64::new(0)),
            business_latency_sum: Arc::new(AtomicU64::new(0)),
            scanner_adjustments: Arc::new(AtomicU64::new(0)),
            current_load: Arc::new(AtomicU64::new(0)),
        }
    }
    
    fn record_business_operation(&self, latency: Duration) {
        self.business_operations.fetch_add(1, Ordering::Relaxed);
        self.business_latency_sum.fetch_add(latency.as_millis() as u64, Ordering::Relaxed);
    }
    
    fn record_load_change(&self, load: f64) {
        self.current_load.store((load * 1000.0) as u64, Ordering::Relaxed);
    }
    
    fn record_scanner_adjustment(&self) {
        self.scanner_adjustments.fetch_add(1, Ordering::Relaxed);
    }
    
    fn get_metrics(&self) -> AdaptiveMetrics {
        let duration = self.start_time.elapsed();
        let ops = self.business_operations.load(Ordering::Relaxed);
        let latency_sum = self.business_latency_sum.load(Ordering::Relaxed);
        
        AdaptiveMetrics {
            duration,
            total_operations: ops,
            avg_latency: if ops > 0 { Duration::from_millis(latency_sum / ops) } else { Duration::ZERO },
            throughput: ops as f64 / duration.as_secs_f64(),
            scanner_adjustments: self.scanner_adjustments.load(Ordering::Relaxed),
            final_load: self.current_load.load(Ordering::Relaxed) as f64 / 1000.0,
        }
    }
}

#[derive(Debug, Clone)]
struct AdaptiveMetrics {
    duration: Duration,
    total_operations: u64,
    avg_latency: Duration,
    throughput: f64,
    scanner_adjustments: u64,
    final_load: f64,
}

/// 测试环境
struct AdaptiveTestEnv {
    temp_dir: TempDir,
    ecstore: Arc<ECStore>,
    scanner: Arc<Scanner>,
    config: AdaptiveSchedulingConfig,
}

impl AdaptiveTestEnv {
    async fn new(config: AdaptiveSchedulingConfig) -> anyhow::Result<Self> {
        let temp_dir = TempDir::new()?;
        let ecstore = Self::create_ecstore(&temp_dir).await?;
        
        let scanner_config = ScannerConfig {
            scan_interval: config.base_interval,
            enable_healing: true,
            enable_metrics: true,
            enable_data_usage_stats: true,
            ..Default::default()
        };
        
        let scanner = Arc::new(Scanner::new(Some(scanner_config), None));
        
        Ok(Self { temp_dir, ecstore, scanner, config })
    }
    
    async fn create_ecstore(temp_dir: &TempDir) -> anyhow::Result<Arc<ECStore>> {
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
            cmd_line: "adaptive".to_string(),
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
    
    async fn prepare_data(&self, bucket: &str, count: usize) -> anyhow::Result<()> {
        self.ecstore.make_bucket(bucket, &Default::default()).await?;
        let data = vec![b'A'; 4096];
        
        for i in 0..count {
            let object_name = format!("test-{:06}", i);
            let mut reader = PutObjReader::from_vec(data.clone());
            self.ecstore.put_object(bucket, &object_name, &mut reader, &Default::default()).await?;
        }
        Ok(())
    }
    
    async fn run_load_scenario(&self, scenario: LoadScenario, monitor: Arc<AdaptiveMonitor>) -> anyhow::Result<()> {
        let bucket = "adaptive-test";
        self.prepare_data(bucket, 100).await?;
        self.scanner.start().await?;
        
        // 模拟自适应调度器
        let adaptive_handle = if self.config.adaptive_enabled {
            let monitor_clone = monitor.clone();
            let thresholds = self.config.load_thresholds.clone();
            Some(tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    let load = monitor_clone.current_load.load(Ordering::Relaxed) as f64 / 1000.0;
                    
                    // 简化的自适应逻辑
                    if load > thresholds.high {
                        monitor_clone.record_scanner_adjustment();
                    }
                }
            }))
        } else { None };
        
        // 执行负载场景
        for phase in scenario.phases {
            let start = Instant::now();
            while start.elapsed() < phase.duration {
                monitor.record_load_change(phase.load_level);
                
                let op_start = Instant::now();
                let obj_id = rand::thread_rng().gen_range(0..100);
                let _result = self.ecstore.get_object_info(bucket, &format!("test-{:06}", obj_id), &Default::default()).await;
                monitor.record_business_operation(op_start.elapsed());
                
                tokio::time::sleep(Duration::from_millis(1000 / phase.requests_per_second as u64)).await;
            }
        }
        
        if let Some(handle) = adaptive_handle { handle.abort(); }
        self.scanner.stop().await?;
        Ok(())
    }
}

async fn benchmark_adaptive_scenario(env: &AdaptiveTestEnv, scenario: LoadScenario, monitor: Arc<AdaptiveMonitor>) -> anyhow::Result<AdaptiveMetrics> {
    env.run_load_scenario(scenario, monitor.clone()).await?;
    Ok(monitor.get_metrics())
}

fn benchmark_adaptive_scheduling(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("adaptive_scheduling");
    
    let configs = vec![
        AdaptiveSchedulingConfig {
            name: "static_conservative".to_string(),
            base_interval: Duration::from_secs(300),
            adaptive_enabled: false,
            load_thresholds: LoadThresholds::default(),
        },
        AdaptiveSchedulingConfig {
            name: "adaptive_smart".to_string(),
            base_interval: Duration::from_secs(60),
            adaptive_enabled: true,
            load_thresholds: LoadThresholds::default(),
        },
    ];
    
    let scenarios = vec![
        LoadScenario {
            name: "gradual_increase".to_string(),
            phases: vec![
                LoadPhase { load_level: 0.2, duration: Duration::from_secs(30), requests_per_second: 10 },
                LoadPhase { load_level: 0.6, duration: Duration::from_secs(30), requests_per_second: 30 },
                LoadPhase { load_level: 0.9, duration: Duration::from_secs(30), requests_per_second: 45 },
            ],
        },
        LoadScenario {
            name: "burst_traffic".to_string(),
            phases: vec![
                LoadPhase { load_level: 0.1, duration: Duration::from_secs(20), requests_per_second: 5 },
                LoadPhase { load_level: 0.9, duration: Duration::from_secs(40), requests_per_second: 50 },
                LoadPhase { load_level: 0.1, duration: Duration::from_secs(20), requests_per_second: 5 },
            ],
        },
    ];
    
    for config in &configs {
        for scenario in &scenarios {
            group.bench_with_input(
                BenchmarkId::new(&config.name, &scenario.name),
                &(config.clone(), scenario.clone()),
                |b, (cfg, scn)| {
                    b.iter(|| {
                        rt.block_on(async {
                            let env = AdaptiveTestEnv::new(cfg.clone()).await.unwrap();
                            let monitor = Arc::new(AdaptiveMonitor::new());
                            benchmark_adaptive_scenario(&env, scn.clone(), monitor).await.unwrap()
                        })
                    });
                }
            );
        }
    }
    
    group.finish();
}

criterion_group!(benches, benchmark_adaptive_scheduling);
criterion_main!(benches);