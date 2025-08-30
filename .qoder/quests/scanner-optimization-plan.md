# Scanner 性能优化设计方案

## 概述

当前 AHM crate 中的 scanner 组件对业务 IO 性能造成严重影响，在压测环境下导致业务性能下降 6 倍。本方案旨在通过智能调度、资源隔离和分布式架构优化，将 scanner 对业务 IO 的影响降至最低，同时保持所有现有功能。

### 优化目标
- 将业务性能影响从 6 倍下降减少到 10% 以内
- Scanner 保持低速率持续运行
- 实现基于业务 IO 负载的智能调节
- 保留所有对外功能（生命周期、数据统计、heal 触发等）

## 技术架构

### 当前架构问题分析

``mermaid
graph TD
    A[Scanner] --> B[全集群扫描]
    B --> C[高并发磁盘访问]
    C --> D[与业务 IO 竞争]
    D --> E[性能下降 6 倍]
    
    F[业务请求] --> G[磁盘 IO]
    C --> G
    G --> H[IO 瓶颈]
```

**问题根因：**
1. **全集群扫描模式**：当前 scanner 扫描所有 EC sets，造成全局 IO 压力
2. **高并发访问**：`max_concurrent_scans: 20` 导致过度并发，应改为串行扫描
3. **无业务感知**：缺乏业务 IO 负载感知和动态调节机制
4. **资源竞争**：scanner 与业务 IO 直接竞争磁盘资源
5. **扫描密度过高**：连续扫描操作之间缺乏合理间隔

### 优化后架构设计

``mermaid
graph TD
    A[节点 A Scanner] --> B[本地磁盘串行扫描]
    C[节点 B Scanner] --> D[本地磁盘串行扫描]
    E[节点 C Scanner] --> F[本地磁盘串行扫描]
    
    G[业务 IO 监控] --> A
    H[业务 IO 监控] --> C
    I[业务 IO 监控] --> E
    
    J[本地数据统计] --> A
    K[本地数据统计] --> C
    L[本地数据统计] --> E
    
    M[客户端访问] --> N[实时聚合各节点数据]
    N --> J
    N --> K  
    N --> L
```

## 核心优化策略

### 1. 去中心化串行扫描架构

**设计原理：**
- 每个节点的 scanner 只负责扫描当前节点的磁盘
- 节点内磁盘采用严格串行扫描，避免 IO 竞争
- 无中心节点，各节点独立运行，数据聚合在访问时实时进行

``mermaid
sequenceDiagram
    participant N1 as 节点1 Scanner
    participant N2 as 节点2 Scanner
    participant N3 as 节点3 Scanner
    participant C as 客户端访问
    
    par 各节点独立串行扫描
        N1->>N1: 串行扫描磁盘1
        N1->>N1: 延迟10秒
        N1->>N1: 串行扫描磁盘2
        N1->>N1: 延迟10秒
        N1->>N1: 串行扫描磁盘3
        N1->>N1: 更新本地统计
    and
        N2->>N2: 串行扫描磁盘1
        N2->>N2: 延迟10秒
        N2->>N2: 串行扫描磁盘2
        N2->>N2: 更新本地统计
    and 
        N3->>N3: 串行扫描磁盘1
        N3->>N3: 延迟10秒
        N3->>N3: 串行扫描磁盘2
        N3->>N3: 更新本地统计
    end
    
    C->>N1: 请求数据统计
    C->>N2: 请求数据统计
    C->>N3: 请求数据统计
    N1->>C: 返回本地数据
    N2->>C: 返回本地数据
    N3->>C: 返回本地数据
    C->>C: 实时聚合全局数据
```

### 2. 业务 IO 感知的智能调度

**负载监控指标：**
- 磁盘 IOPS 使用率
- 磁盘队列深度
- 业务请求延迟
- 系统 CPU/内存使用率

**动态调节策略：**

| 业务负载级别 | Scanner 模式 | 扫描间隔 | 资源限制 |
|------------|-------------|----------|----------|
| 低负载 (<30%) | 串行扫描 | 30秒 | 正常速度 |
| 中负载 (30-60%) | 串行扫描 | 60秒 | 降速50% |
| 高负载 (60-80%) | 串行扫描 | 120秒 | 降速75% |
| 超高负载 (>80%) | 暂停 | 600秒 | 暂停扫描 |

### 3. 资源隔离与限流机制

**IO 资源隔离：**
```rust
pub struct IOThrottler {
    max_iops: Arc<AtomicU64>,           // 最大 IOPS 限制
    current_iops: Arc<AtomicU64>,       // 当前 IOPS 使用量  
    business_priority: Arc<AtomicU8>,   // 业务优先级权重
    scan_delay: Arc<AtomicU64>,         // 扫描操作间延迟
}
```

**串行扫描资源策略：**
- L0: 业务 IO 优先级 95%，Scanner 5%（串行无竞争）
- L1: 业务 IO 优先级 90%，Scanner 10%
- L2: 业务 IO 优先级 85%，Scanner 15%

## 详细实现方案

### 1. Scanner 架构重构

#### 1.1 去中心化 Scanner 组件

```rust
// 去掉中心化协调器，每个节点独立运行
pub struct IndependentNodeScanner {
    node_id: String,
    local_disks: Vec<Arc<DiskStore>>,
    io_monitor: Arc<IOMonitor>,
    throttler: Arc<IOThrottler>,
    config: Arc<RwLock<NodeScannerConfig>>,
    local_stats: Arc<RwLock<LocalScanStats>>,     // 本地统计数据
    current_disk_index: Arc<AtomicUsize>,
}

// 本地统计数据结构
#[derive(Debug, Clone, Default)]
pub struct LocalScanStats {
    pub objects_scanned: u64,
    pub healthy_objects: u64,
    pub corrupted_objects: u64,
    pub data_usage: DataUsageInfo,
    pub last_update: SystemTime,
    pub buckets_stats: HashMap<String, BucketStats>,
    pub disks_stats: HashMap<String, DiskStats>,
    
    // 新增：断点续传状态
    pub scan_progress: ScanProgress,
    pub checkpoint_timestamp: SystemTime,
}

// 扫描进度状态
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ScanProgress {
    pub current_cycle: u64,
    pub current_disk_index: usize,
    pub current_bucket: Option<String>,
    pub current_object_prefix: Option<String>,
    pub completed_disks: HashSet<String>,
    pub completed_buckets: HashMap<String, BucketScanState>,
    pub last_scan_key: Option<String>,  // 最后扫描的对象key
    pub scan_start_time: SystemTime,
    pub estimated_completion: Option<SystemTime>,
}

// Bucket 扫描状态
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketScanState {
    pub completed: bool,
    pub last_object_key: Option<String>,
    pub objects_scanned: u64,
    pub scan_timestamp: SystemTime,
}

// 断点续传管理器
pub struct CheckpointManager {
    checkpoint_file: PathBuf,
    backup_file: PathBuf,
    save_interval: Duration,
    last_save: Arc<RwLock<SystemTime>>,
}

// 去中心化数据聚合服务
pub struct DecentralizedStatsAggregator {
    node_clients: HashMap<String, Arc<NodeClient>>,
    cache: Arc<RwLock<AggregatedStats>>,
    cache_ttl: Duration,  // 3秒缓存
    last_refresh: Arc<RwLock<SystemTime>>,
}

impl CheckpointManager {
    pub fn new(node_id: &str, data_dir: &Path) -> Self {
        let checkpoint_file = data_dir.join(format!("scanner_checkpoint_{}.json", node_id));
        let backup_file = data_dir.join(format!("scanner_checkpoint_{}.backup", node_id));
        
        Self {
            checkpoint_file,
            backup_file,
            save_interval: Duration::from_secs(30), // 30秒保存一次
            last_save: Arc::new(RwLock::new(SystemTime::UNIX_EPOCH)),
        }
    }
    
    // 保存扫描进度到磁盘
    pub async fn save_checkpoint(&self, progress: &ScanProgress) -> Result<()> {
        let now = SystemTime::now();
        let last_save = *self.last_save.read().await;
        
        // 频率控制，避免过于频繁的 IO
        if now.duration_since(last_save).unwrap_or(Duration::ZERO) < self.save_interval {
            return Ok(());
        }
        
        let checkpoint_data = CheckpointData {
            version: 1,
            timestamp: now,
            progress: progress.clone(),
            node_id: std::env::var("NODE_ID").unwrap_or_else(|_| "unknown".to_string()),
        };
        
        // 先写入临时文件，再原子性替换
        let temp_file = self.checkpoint_file.with_extension("tmp");
        let json_data = serde_json::to_string_pretty(&checkpoint_data)?;
        
        tokio::fs::write(&temp_file, json_data).await?;
        
        // 原子性替换，保证数据一致性
        if self.checkpoint_file.exists() {
            tokio::fs::copy(&self.checkpoint_file, &self.backup_file).await?;
        }
        tokio::fs::rename(&temp_file, &self.checkpoint_file).await?;
        
        *self.last_save.write().await = now;
        
        debug!("已保存扫描进度到 {:?}", self.checkpoint_file);
        Ok(())
    }
    
    // 从磁盘恢复扫描进度
    pub async fn load_checkpoint(&self) -> Result<Option<ScanProgress>> {
        // 先尝试主文件
        let checkpoint_result = self.load_checkpoint_from_file(&self.checkpoint_file).await;
        
        match checkpoint_result {
            Ok(checkpoint) => {
                info!("从主文件恢复扫描进度: cycle={}, disk_index={}", 
                      checkpoint.current_cycle, checkpoint.current_disk_index);
                Ok(Some(checkpoint))
            },
            Err(e) => {
                warn!("主检查点文件损坏: {}", e);
                
                // 尝试备份文件
                let backup_result = self.load_checkpoint_from_file(&self.backup_file).await;
                match backup_result {
                    Ok(checkpoint) => {
                        warn!("从备份文件恢复扫描进度");
                        Ok(Some(checkpoint))
                    },
                    Err(backup_e) => {
                        warn!("备份文件也损坏: {}", backup_e);
                        info!("无法恢复扫描进度，将从头开始扫描");
                        Ok(None)
                    }
                }
            }
        }
    }
    
    async fn load_checkpoint_from_file(&self, file_path: &Path) -> Result<ScanProgress> {
        if !file_path.exists() {
            return Err(Error::NotFound("检查点文件不存在".to_string()));
        }
        
        let content = tokio::fs::read_to_string(file_path).await?;
        let checkpoint_data: CheckpointData = serde_json::from_str(&content)?;
        
        // 验证检查点数据的有效性
        self.validate_checkpoint(&checkpoint_data)?;
        
        Ok(checkpoint_data.progress)
    }
    
    fn validate_checkpoint(&self, checkpoint: &CheckpointData) -> Result<()> {
        let now = SystemTime::now();
        let checkpoint_age = now.duration_since(checkpoint.timestamp)
            .unwrap_or(Duration::MAX);
        
        // 检查点太旧（超过 24 小时），可能数据已经过期
        if checkpoint_age > Duration::from_secs(24 * 3600) {
            return Err(Error::InvalidCheckpoint(
                format!("检查点数据过旧: {:?}", checkpoint_age)
            ));
        }
        
        // 验证版本兼容性
        if checkpoint.version > 1 {
            return Err(Error::InvalidCheckpoint(
                format!("不支持的检查点版本: {}", checkpoint.version)
            ));
        }
        
        Ok(())
    }
    
    // 清理检查点文件
    pub async fn cleanup_checkpoint(&self) -> Result<()> {
        if self.checkpoint_file.exists() {
            tokio::fs::remove_file(&self.checkpoint_file).await?;
        }
        if self.backup_file.exists() {
            tokio::fs::remove_file(&self.backup_file).await?;
        }
        info!("已清理扫描检查点文件");
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct CheckpointData {
    version: u32,
    timestamp: SystemTime,
    progress: ScanProgress,
    node_id: String,
}

impl SerialDiskScanner {
    // 串行扫描所有本地磁盘，更新本地统计
    pub async fn scan_all_disks_serially(&self) -> Result<Vec<ScanResult>> {
        let mut results = Vec::new();
        
        for (index, disk) in self.disks.iter().enumerate() {
            // 检查业务负载，决定是否继续扫描
            let load_level = self.io_monitor.get_business_load_level().await;
            if matches!(load_level, LoadLevel::Critical) {
                info!("业务负载过高，暂停磁盘 {} 扫描", index);
                break;
            }
            
            info!("开始串行扫描磁盘 {}", index);
            
            // 串行扫描单个磁盘
            let result = self.scan_single_disk_with_throttle(disk).await?;
            results.push(result.clone());
            
            // 实时更新本地统计数据
            self.update_local_stats(&result).await;
            
            // 磁盘间延迟，避免连续 IO 压力
            if index < self.disks.len() - 1 {
                info!("磁盘 {} 扫描完成，延迟 {:?} 后继续", index, self.scan_delay);
                tokio::time::sleep(self.scan_delay).await;
            }
        }
        
        Ok(results)
    }
    
    async fn update_local_stats(&self, result: &ScanResult) {
        // 更新本地统计数据，不上报到中心节点
        // ...
    }
}

```rust
// 新增：节点级 Scanner（串行扫描模式 + 断点续传）
pub struct NodeScanner {
    node_id: String,
    local_disks: Vec<Arc<DiskStore>>,
    io_monitor: Arc<IOMonitor>,
    throttler: Arc<IOThrottler>,
    config: Arc<RwLock<NodeScannerConfig>>,
    current_disk_index: Arc<AtomicUsize>,  // 当前扫描的磁盘索引
    local_stats: Arc<RwLock<LocalScanStats>>, // 本地统计数据
    checkpoint_manager: Arc<CheckpointManager>, // 断点续传管理
    scan_progress: Arc<RwLock<ScanProgress>>,   // 扫描进度
}

impl NodeScanner {
    pub fn new(node_id: String, config: NodeScannerConfig, data_dir: &Path) -> Self {
        let checkpoint_manager = Arc::new(CheckpointManager::new(&node_id, data_dir));
        
        Self {
            node_id,
            local_disks: Vec::new(),
            io_monitor: Arc::new(IOMonitor::new()),
            throttler: Arc::new(IOThrottler::new()),
            config: Arc::new(RwLock::new(config)),
            current_disk_index: Arc::new(AtomicUsize::new(0)),
            local_stats: Arc::new(RwLock::new(LocalScanStats::default())),
            checkpoint_manager,
            scan_progress: Arc::new(RwLock::new(ScanProgress::default())),
        }
    }
    
    // 节点启动时先尝试恢复断点
    pub async fn start_with_resume(&self) -> Result<()> {
        info!("节点 {} 启动，尝试恢复断点续传", self.node_id);
        
        // 尝试恢复扫描进度
        match self.checkpoint_manager.load_checkpoint().await {
            Ok(Some(progress)) => {
                info!("成功恢复扫描进度: cycle={}, disk={}, last_key={:?}", 
                      progress.current_cycle, 
                      progress.current_disk_index,
                      progress.last_scan_key);
                
                *self.scan_progress.write().await = progress;
                
                // 使用恢复的进度开始扫描
                self.resume_scanning_from_checkpoint().await
            },
            Ok(None) => {
                info!("无有有效检查点，从头开始扫描");
                self.start_fresh_scanning().await
            },
            Err(e) => {
                warn!("恢复扫描进度失败: {}，从头开始", e);
                self.start_fresh_scanning().await
            }
        }
    }
    
    // 从检查点恢复扫描
    async fn resume_scanning_from_checkpoint(&self) -> Result<()> {
        let progress = self.scan_progress.read().await;
        let disk_index = progress.current_disk_index;
        let last_scan_key = progress.last_scan_key.clone();
        drop(progress);
        
        info!("从磁盘 {} 位置恢复扫描，上次扫描到: {:?}", disk_index, last_scan_key);
        
        // 更新当前磁盘索引
        self.current_disk_index.store(disk_index, std::sync::atomic::Ordering::Relaxed);
        
        // 开始扫描循环
        self.scan_loop_with_resume(last_scan_key).await
    }
    
    // 全新开始扫描
    async fn start_fresh_scanning(&self) -> Result<()> {
        info!("开始全新扫描循环");
        
        // 初始化扫描进度
        {
            let mut progress = self.scan_progress.write().await;
            progress.current_cycle += 1;
            progress.current_disk_index = 0;
            progress.scan_start_time = SystemTime::now();
            progress.last_scan_key = None;
            progress.completed_disks.clear();
            progress.completed_buckets.clear();
        }
        
        self.current_disk_index.store(0, std::sync::atomic::Ordering::Relaxed);
        
        // 开始扫描循环
        self.scan_loop_with_resume(None).await
    }

impl NodeScanner {
    // 节点启动后独立运行，不依赖中心协调器
    pub async fn start_independent_scanning(&self) -> Result<()> {
        info!("节点 {} 开始独立串行扫描模式", self.node_id);
        
        loop {
            // 串行扫描所有本地磁盘
            if let Err(e) = self.scan_local_disks_serially().await {
                error!("扫描失败: {}", e);
            }
            
            // 根据业务负载动态调整下次扫描间隔
            let scan_interval = self.calculate_next_scan_interval().await;
            tokio::time::sleep(scan_interval).await;
        }
    }
    
    
    // 带断点续传的扫描循环
    async fn scan_loop_with_resume(&self, resume_key: Option<String>) -> Result<()> {
        let config = self.config.read().await;
        let mut interval = tokio::time::interval(config.scan_interval);
        let deep_scan_interval = config.deep_scan_interval;
        drop(config);
        
        let cancel_token = if let Some(global_token) = get_ahm_services_cancel_token() {
            global_token.clone()
        } else {
            CancellationToken::new()
        };
        
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if !self.state.read().await.is_running {
                        break;
                    }
                    
                    if cancel_token.is_cancelled() {
                        info!("取消信号触发，保存检查点并退出");
                        self.save_checkpoint_and_exit().await?;
                        break;
                    }
                    
                    // 执行带断点续传的扫描周期
                    if let Err(e) = self.scan_cycle_with_checkpoint().await {
                        error!("扫描周期失败: {}", e);
                    }
                }
                _ = cancel_token.cancelled() => {
                    info!("接收到取消信号，停止 scanner 循环");
                    self.save_checkpoint_and_exit().await?;
                    break;
                }
            }
        }
        
        info!("Scanner 循环停止");
        Ok(())
    }
    
    // 带检查点保存的扫描周期
    async fn scan_cycle_with_checkpoint(&self) -> Result<()> {
        let start_time = SystemTime::now();
        
        info!("开始带断点续传的扫描周期");
        
        // 更新扫描进度
        {
            let mut progress = self.scan_progress.write().await;
            progress.scan_start_time = start_time;
        }
        
        // 从当前磁盘位置继续扫描
        let start_disk_index = self.current_disk_index.load(std::sync::atomic::Ordering::Relaxed);
        
        for disk_index in start_disk_index..self.local_disks.len() {
            // 检查业务负载
            let load_level = self.io_monitor.get_business_load_level().await;
            if matches!(load_level, LoadLevel::Critical) {
                info!("业务负载过高，暂停磁盘 {} 扫描并保存检查点", disk_index);
                
                // 更新进度并保存检查点
                {
                    let mut progress = self.scan_progress.write().await;
                    progress.current_disk_index = disk_index;
                }
                
                self.checkpoint_manager.save_checkpoint(&*self.scan_progress.read().await).await?;
                break;
            }
            
            info!("开始扫描磁盘 {}", disk_index);
            
            // 扫描单个磁盘
            let disk = &self.local_disks[disk_index];
            let scan_result = self.scan_single_disk_with_resume(disk, disk_index).await?;
            
            // 更新进度
            {
                let mut progress = self.scan_progress.write().await;
                progress.current_disk_index = disk_index + 1;
                progress.completed_disks.insert(disk.path().to_string_lossy().to_string());
                progress.last_scan_key = scan_result.last_processed_key;
            }
            
            // 定期保存检查点
            self.checkpoint_manager.save_checkpoint(&*self.scan_progress.read().await).await?;
            
            // 磁盘间延迟
            let config = self.config.read().await;
            let disk_scan_interval = config.disk_scan_interval;
            drop(config);
            
            if disk_index < self.local_disks.len() - 1 {
                info!("磁盘 {} 扫描完成，延迟 {:?} 后继续", disk_index, disk_scan_interval);
                tokio::time::sleep(disk_scan_interval).await;
            }
        }
        
        // 一个完整周期结束，重置进度
        {
            let mut progress = self.scan_progress.write().await;
            progress.current_cycle += 1;
            progress.current_disk_index = 0;
            progress.completed_disks.clear();
            progress.completed_buckets.clear();
            progress.last_scan_key = None;
        }
        
        self.current_disk_index.store(0, std::sync::atomic::Ordering::Relaxed);
        
        // 清理检查点（一个完整周期结束）
        self.checkpoint_manager.cleanup_checkpoint().await?;
        
        let scan_duration = SystemTime::now().duration_since(start_time).unwrap_or(Duration::ZERO);
        info!("扫描周期完成，耗时: {:?}", scan_duration);
        
        Ok(())
    }
    
    // 带断点续传的单磁盘扫描
    async fn scan_single_disk_with_resume(&self, disk: &Arc<DiskStore>, disk_index: usize) -> Result<DiskScanResult> {
        let mut result = DiskScanResult {
            disk_path: disk.path().to_string_lossy().to_string(),
            objects_scanned: 0,
            last_processed_key: None,
        };
        
        // 获取上次扫描的位置
        let resume_key = {
            let progress = self.scan_progress.read().await;
            if progress.current_disk_index == disk_index {
                progress.last_scan_key.clone()
            } else {
                None
            }
        };
        
        info!("扫描磁盘 {}，从位置 {:?} 继续", disk_index, resume_key);
        
        // 实现具体的磁盘扫描逻辑，支持从指定 key 继续
        // 这里需要与 ECStore API 集成，实现分页扫描
        
        Ok(result)
    }
    
    // 保存检查点并退出
    async fn save_checkpoint_and_exit(&self) -> Result<()> {
        info!("正在保存检查点并退出...");
        
        // 保存当前进度
        self.checkpoint_manager.save_checkpoint(&*self.scan_progress.read().await).await?;
        
        // 更新状态
        {
            let mut state = self.state.write().await;
            state.is_running = false;
            state.last_scan_end = Some(SystemTime::now());
        }
        
        info!("已保存检查点，安全退出");
        Ok(())
    }
    
    // 获取扫描进度信息
    pub async fn get_scan_progress(&self) -> ScanProgress {
        self.scan_progress.read().await.clone()
    }
    
    // 手动清理检查点（用于管理员操作）
    pub async fn clear_checkpoint(&self) -> Result<()> {
        self.checkpoint_manager.cleanup_checkpoint().await?;
        
        // 重置进度
        {
            let mut progress = self.scan_progress.write().await;
            *progress = ScanProgress::default();
        }
        
        self.current_disk_index.store(0, std::sync::atomic::Ordering::Relaxed);
        
        Ok(())
    }
}

#[derive(Debug)]
struct DiskScanResult {
    disk_path: String,
    objects_scanned: u64,
    last_processed_key: Option<String>,
}

### 5. 断点续传使用示例

```rust
// 启动 Scanner 并自动恢复断点
#[tokio::main]
async fn main() -> Result<()> {
    let node_id = std::env::var("NODE_ID").unwrap_or_else(|_| "node1".to_string());
    let data_dir = Path::new("/data/scanner");
    
    // 创建 Scanner 实例
    let scanner = NodeScanner::new(node_id, NodeScannerConfig::default(), data_dir);
    
    // 设置信号处理器，保证优雅关闭
    let scanner_clone = scanner.clone();
    tokio::spawn(async move {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
        sigterm.recv().await;
        
        info!("接收到 SIGTERM，保存检查点并退出");
        if let Err(e) = scanner_clone.save_checkpoint_and_exit().await {
            error!("保存检查点失败: {}", e);
        }
        
        std::process::exit(0);
    });
    
    // 启动 Scanner（自动断点续传）
    scanner.start_with_resume().await?;
    
    Ok(())
}

// 管理员命令行工具
struct ScannerCLI;

impl ScannerCLI {
    // 查看扫描进度
    pub async fn show_progress(node_id: &str) -> Result<()> {
        let data_dir = Path::new("/data/scanner");
        let checkpoint_manager = CheckpointManager::new(node_id, data_dir);
        
        match checkpoint_manager.load_checkpoint().await? {
            Some(progress) => {
                println!("节点 {} 扫描进度:", node_id);
                println!("  当前周期: {}", progress.current_cycle);
                println!("  当前磁盘: {}", progress.current_disk_index);
                println!("  最后扫描 key: {:?}", progress.last_scan_key);
                println!("  已完成磁盘: {:?}", progress.completed_disks);
                
                let elapsed = SystemTime::now().duration_since(progress.scan_start_time)
                    .unwrap_or(Duration::ZERO);
                println!("  扫描耗时: {:?}", elapsed);
            },
            None => {
                println!("节点 {} 无有扫描进度信息", node_id);
            }
        }
        
        Ok(())
    }
    
    // 清理检查点（重新开始）
    pub async fn clear_checkpoint(node_id: &str) -> Result<()> {
        let data_dir = Path::new("/data/scanner");
        let checkpoint_manager = CheckpointManager::new(node_id, data_dir);
        
        checkpoint_manager.cleanup_checkpoint().await?;
        println!("已清理节点 {} 的扫描检查点", node_id);
        
        Ok(())
    }
}
```

// 去除集群协调器，改为客户端聚合服务
// pub struct ScannerCoordinator { ... } // 删除
```

#### 1.2 配置参数优化

```rust
pub struct OptimizedScannerConfig {
    // 基础配置
    pub scan_interval: Duration,
    pub disk_scan_interval: Duration,       // 磁盘间扫描间隔
    
    // 新增：业务感知配置
    pub business_io_threshold: f64,        // 业务 IO 阈值
    pub adaptive_interval_range: (Duration, Duration), // 动态间隔范围
    pub io_throttle_ratio: f64,            // IO 限流比例
    pub scan_operation_delay: Duration,    // 扫描操作间延迟
    
    // 新增：串行扫描配置
    pub enable_serial_scanning: bool,      // 启用串行扫描模式
    pub enable_local_scanning: bool,       // 启用本地扫描模式
    pub cross_node_verification: bool,     // 跨节点验证
    
    // 保留原有功能配置
    pub enable_healing: bool,
    pub enable_metrics: bool,
    pub enable_data_usage_stats: bool,
}

impl Default for OptimizedScannerConfig {
    fn default() -> Self {
        Self {
            scan_interval: Duration::from_secs(300),    // 提高到5分钟
            disk_scan_interval: Duration::from_secs(10), // 磁盘间间隔10秒
            business_io_threshold: 0.6,                 // 60% 阈值
            adaptive_interval_range: (
                Duration::from_secs(60),                // 最小1分钟
                Duration::from_secs(1200)               // 最大20分钟
            ),
            io_throttle_ratio: 0.05,                    // Scanner 最多占用5% IO
            scan_operation_delay: Duration::from_millis(100), // 操作间延迟100ms
            enable_serial_scanning: true,
            enable_local_scanning: true,
            cross_node_verification: false,
            enable_healing: true,
            enable_metrics: true,
            enable_data_usage_stats: true,
        }
    }
}
```

### 2. 业务 IO 监控系统

#### 2.1 IO 负载监控

```rust
pub struct IOMonitor {
    disk_metrics: HashMap<String, DiskIOMetrics>,
    business_latency: MovingAverage,
    scanner_latency: MovingAverage,
    update_interval: Duration,
}

#[derive(Debug, Clone)]
pub struct DiskIOMetrics {
    pub current_iops: u64,
    pub max_iops: u64,
    pub queue_depth: u32,
    pub avg_latency: Duration,
    pub utilization: f64,
}

impl IOMonitor {
    // 获取当前业务 IO 负载级别
    pub async fn get_business_load_level(&self) -> LoadLevel {
        let avg_utilization = self.calculate_average_utilization().await;
        let avg_latency = self.business_latency.average();
        
        match (avg_utilization, avg_latency.as_millis()) {
            (u, _) if u < 0.3 => LoadLevel::Low,
            (u, l) if u < 0.6 && l < 100 => LoadLevel::Medium,
            (u, l) if u < 0.8 && l < 200 => LoadLevel::High,
            _ => LoadLevel::Critical,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum LoadLevel {
    Low,      // < 30%
    Medium,   // 30-60%
    High,     // 60-80%
    Critical, // > 80%
}
```

#### 2.2 智能调度器

```rust
pub struct IntelligentScheduler {
    io_monitor: Arc<IOMonitor>,
    current_policy: Arc<RwLock<SchedulingPolicy>>,
    adjustment_history: Vec<SchedulingAdjustment>,
}

#[derive(Debug, Clone)]
pub struct SchedulingPolicy {
    pub scan_interval: Duration,
    pub disk_scan_interval: Duration,
    pub scan_operation_delay: Duration,
    pub io_throttle_ratio: f64,
    pub enable_deep_scan: bool,
    pub enable_scanning: bool,
}

impl IntelligentScheduler {
    // 基于业务负载动态调整串行扫描策略
    pub async fn adjust_scanning_policy(&self) -> SchedulingPolicy {
        let load_level = self.io_monitor.get_business_load_level().await;
        
        match load_level {
            LoadLevel::Low => SchedulingPolicy {
                scan_interval: Duration::from_secs(180),        // 3分钟
                disk_scan_interval: Duration::from_secs(5),     // 磁盘间间隔5秒
                scan_operation_delay: Duration::from_millis(50), // 操作间延迟50ms
                io_throttle_ratio: 0.15,
                enable_deep_scan: true,
                enable_scanning: true,
            },
            LoadLevel::Medium => SchedulingPolicy {
                scan_interval: Duration::from_secs(300),        // 5分钟
                disk_scan_interval: Duration::from_secs(10),    // 磁盘间间隔10秒
                scan_operation_delay: Duration::from_millis(100), // 操作间延迟100ms
                io_throttle_ratio: 0.1,
                enable_deep_scan: true,
                enable_scanning: true,
            },
            LoadLevel::High => SchedulingPolicy {
                scan_interval: Duration::from_secs(600),        // 10分钟
                disk_scan_interval: Duration::from_secs(30),    // 磁盘间间隔30秒
                scan_operation_delay: Duration::from_millis(200), // 操作间延迟200ms
                io_throttle_ratio: 0.05,
                enable_deep_scan: false,
                enable_scanning: true,
            },
            LoadLevel::Critical => SchedulingPolicy {
                scan_interval: Duration::from_secs(1200),       // 20分钟
                disk_scan_interval: Duration::from_secs(0),     // 暂停扫描
                scan_operation_delay: Duration::from_millis(0),
                io_throttle_ratio: 0.02,
                enable_deep_scan: false,
                enable_scanning: false,                         // 完全暂停
            },
        }
    }
}
```

### 3. 去中心化功能完整性设计

#### 3.1 数据统计功能

```rust
pub struct DecentralizedDataUsageCollector {
    node_clients: HashMap<String, Arc<NodeClient>>,
    cache: Arc<RwLock<Option<AggregatedDataUsage>>>,
    cache_ttl: Duration,  // 3秒缓存时间
    last_refresh: Arc<RwLock<SystemTime>>,
}

impl DecentralizedDataUsageCollector {
    // 实时从各节点获取并聚合数据使用统计
    pub async fn get_global_data_usage(&self) -> Result<DataUsageInfo> {
        // 检查缓存是否过期
        let now = SystemTime::now();
        let last_refresh = *self.last_refresh.read().await;
        
        if now.duration_since(last_refresh).unwrap_or(Duration::MAX) < self.cache_ttl {
            // 使用缓存数据，减少系统压力
            if let Some(cached) = self.cache.read().await.as_ref() {
                return Ok(cached.clone().into());
            }
        }
        
        // 并行从各节点获取数据
        let mut node_futures = Vec::new();
        for (node_id, client) in &self.node_clients {
            let client = client.clone();
            let node_id = node_id.clone();
            
            node_futures.push(async move {
                client.get_local_data_usage().await
                    .map(|data| (node_id, data))
            });
        }
        
        // 等待所有节点响应（设置超时）
        let timeout = Duration::from_millis(500); // 500ms 超时
        let results = tokio::time::timeout(
            timeout,
            futures::future::join_all(node_futures)
        ).await;
        
        match results {
            Ok(node_results) => {
                // 聚合所有节点数据
                let aggregated = self.aggregate_node_data(node_results).await?;
                
                // 更新缓存
                *self.cache.write().await = Some(aggregated.clone());
                *self.last_refresh.write().await = now;
                
                Ok(aggregated.into())
            },
            Err(_) => {
                // 超时时使用缓存数据或返回错误
                if let Some(cached) = self.cache.read().await.as_ref() {
                    warn!("获取节点数据超时，使用缓存数据");
                    Ok(cached.clone().into())
                } else {
                    Err(Error::Timeout("获取全局数据使用统计超时".to_string()))
                }
            }
        }
    }
    
    async fn aggregate_node_data(&self, node_results: Vec<Result<(String, LocalDataUsage)>>) -> Result<AggregatedDataUsage> {
        let mut total_objects = 0u64;
        let mut total_size = 0u64;
        let mut buckets_usage = HashMap::new();
        
        for result in node_results {
            match result {
                Ok((node_id, local_data)) => {
                    info!("从节点 {} 获取数据: {} 对象", node_id, local_data.objects_count);
                    total_objects += local_data.objects_count;
                    total_size += local_data.total_size;
                    
                    // 聚合bucket级数据
                    for (bucket, stats) in local_data.bucket_stats {
                        let entry = buckets_usage.entry(bucket).or_insert_with(BucketUsage::default);
                        entry.objects_count += stats.objects_count;
                        entry.total_size += stats.total_size;
                    }
                },
                Err(e) => {
                    warn!("获取节点数据失败: {}", e);
                    // 继续处理其他节点数据
                }
            }
        }
        
        Ok(AggregatedDataUsage {
            objects_total_count: total_objects,
            total_size,
            buckets_usage,
            last_update: SystemTime::now(),
        })
    }
}
```

#### 3.2 生命周期管理

```rust
pub struct DecentralizedLifecycleManager {
    node_clients: HashMap<String, Arc<NodeClient>>,
    local_lifecycle_processor: Arc<LocalLifecycleProcessor>,
}

impl DecentralizedLifecycleManager {
    // 各节点独立处理本地对象生命周期，无需中心协调
    pub async fn process_lifecycle_rules(&self) -> Result<()> {
        // 每个节点独立处理本地对象的生命周期规则
        self.local_lifecycle_processor.process_local_objects().await?;
        
        // 无需与其他节点协调，因为每个对象只在一个节点上处理
        Ok(())
    }
    
    // 全局生命周期统计（可选）
    pub async fn get_global_lifecycle_stats(&self) -> Result<LifecycleStats> {
        let mut node_futures = Vec::new();
        
        for (node_id, client) in &self.node_clients {
            let client = client.clone();
            let node_id = node_id.clone();
            
            node_futures.push(async move {
                client.get_lifecycle_stats().await
                    .map(|stats| (node_id, stats))
            });
        }
        
        let results = futures::future::join_all(node_futures).await;
        self.aggregate_lifecycle_stats(results).await
    }
}
```

#### 3.3 Heal 触发机制

```rust
pub struct DecentralizedHealManager {
    local_heal_manager: Arc<LocalHealManager>,
    node_clients: HashMap<String, Arc<NodeClient>>,  // 用于获取全局统计
}

impl DecentralizedHealManager {
    // 本地 heal 检测和处理，无需中心协调
    pub async fn process_local_heal_tasks(&self) -> Result<()> {
        // 每个节点只处理本地发现的 heal 需求
        let local_heal_requests = self.detect_local_heal_needs().await?;
        
        // 根据业务负载智能调度 heal 任务
        for heal_request in local_heal_requests {
            self.local_heal_manager.schedule_heal_task(heal_request).await?;
        }
        
        Ok(())
    }
    
    // 全局 heal 统计（仅用于监控）
    pub async fn get_global_heal_stats(&self) -> Result<GlobalHealStats> {
        let mut node_futures = Vec::new();
        
        for (node_id, client) in &self.node_clients {
            let client = client.clone();
            let node_id = node_id.clone();
            
            node_futures.push(async move {
                client.get_heal_stats().await
                    .map(|stats| (node_id, stats))
            });
        }
        
        let timeout = Duration::from_millis(300);
        let results = tokio::time::timeout(
            timeout,
            futures::future::join_all(node_futures)
        ).await.unwrap_or_default();
        
        self.aggregate_heal_stats(results).await
    }
    
    async fn detect_local_heal_needs(&self) -> Result<Vec<HealRequest>> {
        // 本地检测需要 heal 的对象
        // 基于本地扫描结果
        // ...
        Ok(vec![])
    }
}

### 4. 实时数据聚合 API 设计

```rust
// 客户端访问接口
pub struct ScannerStatsAPI {
    aggregator: Arc<DecentralizedStatsAggregator>,
}

impl ScannerStatsAPI {
    // 获取全局数据使用统计（带缓存）
    pub async fn get_data_usage_stats(&self) -> Result<DataUsageResponse> {
        let stats = self.aggregator.get_global_data_usage().await?;
        Ok(DataUsageResponse {
            data: stats,
            cache_age: self.aggregator.get_cache_age().await,
            source: "实时聚合".to_string(),
        })
    }
    
    // 获取单个节点统计
    pub async fn get_node_stats(&self, node_id: &str) -> Result<NodeStatsResponse> {
        let stats = self.aggregator.get_node_stats(node_id).await?;
        Ok(NodeStatsResponse {
            node_id: node_id.to_string(),
            stats,
            timestamp: SystemTime::now(),
        })
    }
    
    // 强制刷新缓存（仅在需要时使用）
    pub async fn force_refresh_cache(&self) -> Result<()> {
        self.aggregator.force_refresh().await
    }
}

// API 响应结构
#[derive(Debug, Serialize, Deserialize)]
pub struct DataUsageResponse {
    pub data: DataUsageInfo,
    pub cache_age: Duration,  // 缓存年龄
    pub source: String,       // 数据来源说明
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeStatsResponse {
    pub node_id: String,
    pub stats: LocalScanStats,
    pub timestamp: SystemTime,
}
```

### 6. 渐进式迁移策略

#### 6.1 阶段1：IO 监控和串行限流 + 断点续传 (3周)

``mermaid
gantt
    title 阶段1实施计划
    dateFormat  YYYY-MM-DD
    section IO监控
    IO监控模块开发    :a1, 2024-01-01, 1w
    串行限流机制实现  :a2, after a1, 1w
    section 断点续传
    CheckpointManager开发  :a3, after a2, 1w
    section 测试验证
    功能测试         :a4, after a3, 3d
    性能测试         :a5, after a4, 4d
```

- 实现 IOMonitor 和 IOThrottler
- 在现有 Scanner 中集成串行扫描和限流机制
- 实现断点续传功能和 CheckpointManager
- 验证业务性能改善效果和断点续传可靠性

#### 6.2 阶段2：智能调度和去中心化 (3周)

- 实现 IntelligentScheduler 和去中心化架构
- 集成动态调节策略
- 优化扫描间隔和操作延迟
- 实现本地数据聚合和缓存机制

#### 6.3 阶段3：功能完整性验证 (2周)

- 验证所有现有功能保持完整
- 实现实时数据聚合API
- 全面性能测试和调优
- 断点续传在各种异常场景下的测试

## 性能改善预期

### 量化目标

| 指标 | 当前状态 | 目标状态 | 改善幅度 |
|------|----------|----------|----------|
| 业务性能下降 | 6倍 | <5% | 95%改善 |
| Scanner 吞吐量 | 100% | 10-20% | 低速持续运行 |
| 资源利用率 | 无控制 | <5% IO占用 | 精确控制 |
| 响应延迟 | +600% | <+3% | 大幅改善 |
| IO 冲突 | 频繁 | 无冲突 | 完全消除 |
| 断点续传准确性 | 无 | >99.9% | 新增功能 |

### 断点续传可靠性验证

``mermaid
graph LR
    A[正常扫描] --> B[强制中断]
    B --> C[保存检查点]
    C --> D[重启Scanner]
    D --> E[加载检查点]
    E --> F[继续扫描]
    F --> G[验证完整性]
```

**测试场景：**
- 进程异常终止（kill -9）
- 系统重启
- 网络中断
- 磁盘IO异常
- 高业务负载中断

### 性能测试验证

``mermaid
graph LR
    A[压测环境] --> B[业务负载生成]
    B --> C[Scanner 串行运行]
    C --> D[性能指标监控]
    D --> E[效果验证]
    
    F[监控指标] --> G[IOPS]
    F --> H[延迟]
    F --> I[吞吐量]
    F --> J[CPU/内存]
    F --> K[无IO冲突]
    F --> L[断点续传成功率]
```

## 风险评估与缓解

### 技术风险

| 风险 | 概率 | 影响 | 缓解措施 |
|------|------|------|----------|
| 去中心化一致性问题 | 低 | 中 | 实现最终一致性，数据本地化处理 |
| 性能监控开销 | 低 | 中 | 使用轻量级监控，异步处理 |
| 功能回归风险 | 中 | 高 | 充分的回归测试 |
| 数据聚合延迟 | 中 | 低 | 3秒缓存机制，超时处理 |
| 检查点文件损坏 | 低 | 中 | 双文件备份，版本验证机制 |
| 断点续传失败 | 低 | 中 | 自动降级到全新扫描 |

### 业务风险

| 风险 | 概率 | 影响 | 缓解措施 |
|------|------|------|----------|
| 扫描覆盖不完整 | 低 | 中 | 定期验证，本地状态监控 |
| Heal 延迟增加 | 中 | 中 | 紧急情况下提升优先级 |
| 数据统计不一致 | 低 | 低 | 实现容错聚合机制 |
| 检查点过期导致重复扫描 | 低 | 低 | 24小时过期机制，渐进式恢复 |

## 测试策略

### 单元测试

``rust
#[tokio::test]
async fn test_checkpoint_save_and_load() {
    let temp_dir = tempfile::tempdir().unwrap();
    let checkpoint_manager = CheckpointManager::new("test_node", temp_dir.path());
    
    // 创建测试进度
    let progress = ScanProgress {
        current_cycle: 5,
        current_disk_index: 2,
        last_scan_key: Some("test_key_123".to_string()),
        completed_disks: HashSet::from(["disk1".to_string(), "disk2".to_string()]),
        scan_start_time: SystemTime::now(),
        ..Default::default()
    };
    
    // 保存检查点
    checkpoint_manager.save_checkpoint(&progress).await.unwrap();
    
    // 加载检查点
    let loaded_progress = checkpoint_manager.load_checkpoint().await.unwrap().unwrap();
    
    // 验证数据一致性
    assert_eq!(loaded_progress.current_cycle, 5);
    assert_eq!(loaded_progress.current_disk_index, 2);
    assert_eq!(loaded_progress.last_scan_key, Some("test_key_123".to_string()));
    assert_eq!(loaded_progress.completed_disks.len(), 2);
}

#[tokio::test]
async fn test_checkpoint_corruption_recovery() {
    let temp_dir = tempfile::tempdir().unwrap();
    let checkpoint_manager = CheckpointManager::new("test_node", temp_dir.path());
    
    // 保存有效检查点
    let progress = ScanProgress::default();
    checkpoint_manager.save_checkpoint(&progress).await.unwrap();
    
    // 损坏主文件
    let checkpoint_file = temp_dir.path().join("scanner_checkpoint_test_node.json");
    tokio::fs::write(&checkpoint_file, "invalid json").await.unwrap();
    
    // 验证能从备份文件恢复
    let loaded_progress = checkpoint_manager.load_checkpoint().await.unwrap();
    assert!(loaded_progress.is_some());
}

#[tokio::test]
async fn test_serial_disk_scanning() {
    let scanner = SerialDiskScanner::new();
    
    // 模拟不同负载级别的串行扫描
    for load in [LoadLevel::Low, LoadLevel::High] {
        scanner.io_monitor.set_load_level(load).await;
        let results = scanner.scan_all_disks_serially().await;
        
        match load {
            LoadLevel::Low => {
                assert!(results.is_ok());
                assert!(results.unwrap().len() > 0);
            },
            LoadLevel::Critical => {
                // 高负载时可能提前停止
                assert!(results.is_ok());
            }
        }
    }
}

#[tokio::test] 
async fn test_decentralized_data_aggregation() {
    let aggregator = DecentralizedStatsAggregator::new();
    
    // 模拟从3个节点获取数据
    let global_stats = aggregator.get_global_data_usage().await;
    assert!(global_stats.is_ok());
    
    // 验证缓存机制
    let cache_age = aggregator.get_cache_age().await;
    assert!(cache_age < Duration::from_secs(3));
}
```

### 集成测试

``rust
#[tokio::test]
#[ignore = "integration test"]
async fn test_scanner_resume_after_interruption() {
    // 1. 建立测试环境
    let temp_dir = tempfile::tempdir().unwrap();
    let node_id = "test_node_resume";
    
    // 2. 启动Scanner并运行一段时间
    let scanner = NodeScanner::new(node_id.to_string(), NodeScannerConfig::default(), temp_dir.path());
    let scanner_clone = scanner.clone();
    
    // 3. 模拟扫描中断
    let handle = tokio::spawn(async move {
        scanner_clone.start_with_resume().await
    });
    
    // 等待一段时间后中断
    tokio::time::sleep(Duration::from_secs(5)).await;
    handle.abort();
    
    // 4. 重新启动Scanner
    let scanner2 = NodeScanner::new(node_id.to_string(), NodeScannerConfig::default(), temp_dir.path());
    let progress = scanner2.get_scan_progress().await;
    
    // 5. 验证能够成功恢复
    assert!(progress.current_cycle > 0 || progress.current_disk_index > 0);
}

#[tokio::test]
#[ignore = "integration test"]
async fn test_serial_scanning_performance() {
    // 1. 建立3节点测试集群
    let cluster = TestCluster::new(3).await;
    
    // 2. 生成业务负载
    let business_load = BusinessLoadGenerator::new();
    business_load.start_continuous_load().await;
    
    // 3. 启动串行优化后的 Scanner
    let scanner = OptimizedScanner::new_with_serial_mode(&cluster).await;
    scanner.start().await;
    
    // 4. 运行性能测试
    let metrics = PerformanceMonitor::run_test(Duration::from_minutes(10)).await;
    
    // 5. 验证串行扫描性能改善
    assert!(metrics.business_performance_impact < 0.05); // <5% 影响
    assert!(metrics.scanner_coverage > 0.95); // >95% 覆盖率
    assert!(metrics.disk_io_conflicts == 0); // 无 IO 冲突
    assert!(metrics.checkpoint_success_rate > 0.999); // >99.9% 断点成功率
}
```

### 压力测试

# 业务压力测试脚本（串行扫描模式 + 断点续传）
#!/bin/bash

# 启动业务负载生成器
./business_load_generator --qps=1000 --duration=30m &

# 启动串行优化后的 Scanner
./optimized_scanner --config=production.yaml --mode=serial --enable-checkpoint &
SCANNER_PID=$!

# 监控性能指标
./performance_monitor --interval=1s --output=serial_results.json --focus=io_conflicts &

# 随机中断测试
sleep 600  # 运行10分钟
echo "模拟Scanner中断..."
kill -TERM $SCANNER_PID

sleep 10

# 重启Scanner测试断点续传
echo "重启Scanner测试断点续传..."
./optimized_scanner --config=production.yaml --mode=serial --enable-checkpoint &

# 继续监控
wait

# 分析结果
./analyze_performance --baseline=baseline.json --current=serial_results.json --mode=serial --check-resume=true