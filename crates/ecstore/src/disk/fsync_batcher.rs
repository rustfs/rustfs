// fsync_batcher.rs - 批量fsync优化模块
// 
// 该模块实现了智能的fsync批处理策略，通过批量执行fsync操作来减少系统调用开销
// 支持多种模式：立即同步、批量同步、定时同步和无同步
// 对于高低配置的机器都能自适应调整批处理参数

use std::sync::Arc;
use std::time::Duration;
use tokio::fs::File;
use tokio::sync::Mutex;
use tokio::time::{interval, Instant};
use futures::future::join_all;
use tracing::{debug, warn};

/// Fsync同步模式
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FsyncMode {
    /// 每次写入立即同步（适用于对数据安全性要求极高的场景）
    Immediate,
    /// 批量同步（默认模式，平衡性能和安全性）
    Batch,
    /// 定时同步（适用于可以容忍一定数据丢失的场景）
    Periodic,
    /// 不主动同步，依赖操作系统缓存（最高性能，最低安全性）
    None,
}

impl Default for FsyncMode {
    fn default() -> Self {
        // 默认使用批量模式，平衡性能和数据安全
        FsyncMode::Batch
    }
}

/// Fsync批处理器配置
#[derive(Debug, Clone)]
pub struct FsyncBatcherConfig {
    /// 同步模式
    pub mode: FsyncMode,
    /// 批量大小阈值（文件数量）
    /// 对于低配机器可以设置较小值（如8-16），高配机器可以设置较大值（如32-64）
    pub batch_size: usize,
    /// 批量超时时间
    /// 即使未达到批量大小，超过此时间也会触发同步
    pub batch_timeout: Duration,
    /// 是否启用自适应调整
    /// 根据系统负载动态调整批处理参数
    pub adaptive: bool,
    /// 最大等待文件数
    /// 防止内存无限增长
    pub max_pending: usize,
}

impl Default for FsyncBatcherConfig {
    fn default() -> Self {
        Self {
            mode: FsyncMode::default(),
            batch_size: 32,  // 默认批量大小，适合中等配置机器
            batch_timeout: Duration::from_millis(100),  // 100ms超时
            adaptive: true,  // 默认启用自适应
            max_pending: 1000,  // 最多缓存1000个文件
        }
    }
}

/// 待同步的文件信息
struct PendingFile {
    file: File,
    path: String,
    added_at: Instant,
}

/// Fsync批处理器
pub struct FsyncBatcher {
    config: Arc<Mutex<FsyncBatcherConfig>>,
    pending_files: Arc<Mutex<Vec<PendingFile>>>,
    // 性能统计，用于自适应调整
    stats: Arc<Mutex<FsyncStats>>,
}

/// 性能统计信息
#[derive(Debug, Default, Clone)]
struct FsyncStats {
    total_syncs: u64,
    batch_syncs: u64,
    total_files: u64,
    avg_batch_size: f64,
    last_sync_duration: Duration,
}

impl FsyncBatcher {
    /// 创建新的Fsync批处理器
    pub fn new(config: FsyncBatcherConfig) -> Self {
        let batcher = Self {
            config: Arc::new(Mutex::new(config.clone())),
            pending_files: Arc::new(Mutex::new(Vec::new())),
            stats: Arc::new(Mutex::new(FsyncStats::default())),
        };

        // 如果是Batch或Periodic模式，启动后台处理任务
        if matches!(config.mode, FsyncMode::Batch | FsyncMode::Periodic) {
            let batcher_clone = batcher.clone();
            tokio::spawn(async move {
                batcher_clone.background_worker().await;
            });
        }

        batcher
    }

    /// 添加文件到批处理队列
    pub async fn add_file(&self, file: File, path: String) -> Result<(), std::io::Error> {
        let config = self.config.lock().await;
        
        match config.mode {
            FsyncMode::Immediate => {
                // 立即同步模式：直接执行fsync
                debug!("Immediate fsync for file: {}", path);
                file.sync_all().await?;
                
                // 更新统计
                let mut stats = self.stats.lock().await;
                stats.total_syncs += 1;
                stats.total_files += 1;
            }
            FsyncMode::Batch => {
                // 批量模式：添加到待处理队列
                let mut pending = self.pending_files.lock().await;
                
                // 检查是否超过最大等待数
                if pending.len() >= config.max_pending {
                    warn!("Fsync queue full, forcing immediate sync for file: {}", path);
                    file.sync_all().await?;
                    return Ok(());
                }
                
                debug!("Adding file to batch queue: {}", path);
                pending.push(PendingFile {
                    file,
                    path,
                    added_at: Instant::now(),
                });
                
                // 如果达到批量大小，立即触发同步
                if pending.len() >= config.batch_size {
                    drop(config);  // 释放config锁
                    drop(pending); // 释放pending锁
                    self.flush_all().await?;
                }
            }
            FsyncMode::Periodic => {
                // 定时模式：只添加到队列，等待定时器触发
                let mut pending = self.pending_files.lock().await;
                
                if pending.len() >= config.max_pending {
                    warn!("Fsync queue full in periodic mode, dropping oldest files");
                    // 移除最老的10%文件
                    let remove_count = pending.len() / 10;
                    pending.drain(0..remove_count);
                }
                
                pending.push(PendingFile {
                    file,
                    path,
                    added_at: Instant::now(),
                });
            }
            FsyncMode::None => {
                // 不同步模式：什么都不做
                debug!("Skipping fsync for file: {} (mode=None)", path);
            }
        }
        
        Ok(())
    }

    /// 立即同步所有待处理的文件
    pub async fn flush_all(&self) -> Result<(), std::io::Error> {
        let files = {
            let mut pending = self.pending_files.lock().await;
            std::mem::take(&mut *pending)
        };

        if files.is_empty() {
            return Ok(());
        }

        let start = Instant::now();
        let file_count = files.len();
        debug!("Flushing {} files to disk", file_count);

        // 并行执行所有fsync操作
        // 这里使用join_all并行执行，充分利用多核CPU和I/O并行能力
        let futures: Vec<_> = files
            .into_iter()
            .map(|pf| async move {
                if let Err(e) = pf.file.sync_all().await {
                    warn!("Failed to sync file {}: {}", pf.path, e);
                    Err(e)
                } else {
                    Ok(())
                }
            })
            .collect();

        let results = join_all(futures).await;
        
        // 统计成功和失败的数量
        let success_count = results.iter().filter(|r| r.is_ok()).count();
        let duration = start.elapsed();
        
        debug!(
            "Batch fsync completed: {}/{} files synced in {:?}",
            success_count, file_count, duration
        );

        // 更新统计信息
        let mut stats = self.stats.lock().await;
        stats.batch_syncs += 1;
        stats.total_syncs += success_count as u64;
        stats.total_files += file_count as u64;
        stats.avg_batch_size = (stats.avg_batch_size * (stats.batch_syncs - 1) as f64 
            + file_count as f64) / stats.batch_syncs as f64;
        stats.last_sync_duration = duration;

        // 自适应调整（如果启用）
        if self.config.lock().await.adaptive {
            self.adaptive_adjust(&stats).await;
        }

        // 如果有任何失败，返回第一个错误
        for result in results {
            if let Err(e) = result {
                return Err(e);
            }
        }

        Ok(())
    }

    /// 后台工作线程
    async fn background_worker(&self) {
        let config = self.config.lock().await.clone();
        let mut ticker = interval(config.batch_timeout);
        
        loop {
            ticker.tick().await;
            
            // 检查是否有超时的文件需要同步
            let should_flush = {
                let pending = self.pending_files.lock().await;
                if pending.is_empty() {
                    false
                } else {
                    // 检查最老的文件是否超时
                    let oldest = &pending[0];
                    oldest.added_at.elapsed() >= config.batch_timeout
                }
            };

            if should_flush {
                if let Err(e) = self.flush_all().await {
                    warn!("Background flush failed: {}", e);
                }
            }
        }
    }

    /// 自适应调整批处理参数
    async fn adaptive_adjust(&self, stats: &FsyncStats) {
        let mut config = self.config.lock().await;
        
        // 根据平均批量大小调整batch_size
        if stats.avg_batch_size < config.batch_size as f64 * 0.5 {
            // 如果平均批量大小小于配置的50%，减小batch_size
            config.batch_size = (config.batch_size as f64 * 0.8).max(4.0) as usize;
            debug!("Decreasing batch_size to {}", config.batch_size);
        } else if stats.avg_batch_size > config.batch_size as f64 * 0.9 {
            // 如果经常达到批量大小，增加batch_size
            config.batch_size = (config.batch_size as f64 * 1.2).min(128.0) as usize;
            debug!("Increasing batch_size to {}", config.batch_size);
        }

        // 根据同步耗时调整超时时间
        if stats.last_sync_duration > Duration::from_millis(500) {
            // 如果同步耗时过长，增加超时时间以聚合更多文件
            let new_timeout_ms = (config.batch_timeout.as_millis() as f64 * 1.1)
                .min(1000.0) as u64;
            config.batch_timeout = Duration::from_millis(new_timeout_ms);
            debug!("Increasing batch_timeout to {:?}", config.batch_timeout);
        } else if stats.last_sync_duration < Duration::from_millis(50) {
            // 如果同步很快，可以减少超时时间以降低延迟
            let new_timeout_ms = (config.batch_timeout.as_millis() as f64 * 0.9)
                .max(10.0) as u64;
            config.batch_timeout = Duration::from_millis(new_timeout_ms);
            debug!("Decreasing batch_timeout to {:?}", config.batch_timeout);
        }
    }

    /// 获取当前统计信息（用于监控）
    pub async fn get_stats(&self) -> FsyncStats {
        self.stats.lock().await.clone()
    }

    /// 动态更新配置
    pub async fn update_config(&self, config: FsyncBatcherConfig) {
        *self.config.lock().await = config;
    }
}

impl Clone for FsyncBatcher {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            pending_files: self.pending_files.clone(),
            stats: self.stats.clone(),
        }
    }
}

impl FsyncStats {
    /// 获取平均每次批量同步的文件数
    pub fn avg_files_per_batch(&self) -> f64 {
        if self.batch_syncs == 0 {
            0.0
        } else {
            self.total_files as f64 / self.batch_syncs as f64
        }
    }

    /// 获取fsync效率（批量同步占比）
    pub fn batch_efficiency(&self) -> f64 {
        if self.total_syncs == 0 {
            0.0
        } else {
            self.batch_syncs as f64 / self.total_syncs as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use tokio::fs::OpenOptions;

    #[tokio::test]
    async fn test_immediate_mode() {
        let config = FsyncBatcherConfig {
            mode: FsyncMode::Immediate,
            ..Default::default()
        };
        
        let batcher = FsyncBatcher::new(config);
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.txt");
        
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&path)
            .await
            .unwrap();
        
        batcher.add_file(file, path.to_string_lossy().to_string()).await.unwrap();
        
        let stats = batcher.get_stats().await;
        assert_eq!(stats.total_files, 1);
        assert_eq!(stats.total_syncs, 1);
    }

    #[tokio::test]
    async fn test_batch_mode() {
        let config = FsyncBatcherConfig {
            mode: FsyncMode::Batch,
            batch_size: 3,
            ..Default::default()
        };
        
        let batcher = FsyncBatcher::new(config);
        let dir = tempdir().unwrap();
        
        // 添加3个文件，应该触发批量同步
        for i in 0..3 {
            let path = dir.path().join(format!("test{}.txt", i));
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .open(&path)
                .await
                .unwrap();
            
            batcher.add_file(file, path.to_string_lossy().to_string()).await.unwrap();
        }
        
        // 等待一下让批量同步完成
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        let stats = batcher.get_stats().await;
        assert_eq!(stats.total_files, 3);
        assert_eq!(stats.batch_syncs, 1);
    }
}