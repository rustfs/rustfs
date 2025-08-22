// write_buffer_pool.rs - 写入缓冲池模块
// 
// 该模块实现了写入缓冲池，用于优化小文件写入和写入合并
// 通过缓冲和合并相邻的小写入，减少系统调用开销
// 支持自适应调整缓冲区大小，适配不同配置的机器

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use bytes::{Bytes, BytesMut};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{interval, Instant};
use tracing::{debug, warn};

/// 写入缓冲池配置
#[derive(Debug, Clone)]
pub struct WriteBufferPoolConfig {
    /// 单个缓冲区最大大小
    /// 对于低配机器可以设置为16MB，高配机器可以设置为64MB或更大
    pub max_buffer_size: usize,
    /// 总容量限制
    /// 防止内存占用过大
    pub total_capacity: usize,
    /// 刷新间隔
    /// 定期将缓冲区数据写入磁盘
    pub flush_interval: Duration,
    /// 合并窗口
    /// 在此时间窗口内的写入请求会尝试合并
    pub combine_window: Duration,
    /// 是否启用写入合并
    pub enable_combining: bool,
    /// 小文件阈值
    /// 小于此大小的文件会使用内存缓冲
    pub small_file_threshold: usize,
}

impl Default for WriteBufferPoolConfig {
    fn default() -> Self {
        Self {
            max_buffer_size: 64 * 1024 * 1024,  // 64MB
            total_capacity: 256 * 1024 * 1024,  // 256MB总容量
            flush_interval: Duration::from_millis(100),  // 100ms刷新间隔
            combine_window: Duration::from_millis(10),  // 10ms合并窗口
            enable_combining: true,
            small_file_threshold: 1024 * 1024,  // 1MB为小文件阈值
        }
    }
}

/// 写入请求
#[derive(Debug)]
pub struct WriteRequest {
    pub path: PathBuf,
    pub data: Bytes,
    pub offset: Option<u64>,
    pub timestamp: Instant,
}

/// 缓冲区状态
#[derive(Debug)]
struct BufferState {
    data: BytesMut,
    last_write: Instant,
    write_count: usize,
    dirty: bool,
}

/// 写入合并器
struct WriteCombiner {
    pending_writes: HashMap<PathBuf, Vec<WriteRequest>>,
    combine_window: Duration,
}

impl WriteCombiner {
    fn new(combine_window: Duration) -> Self {
        Self {
            pending_writes: HashMap::new(),
            combine_window,
        }
    }

    /// 尝试合并写入请求
    fn try_combine(&mut self, path: &Path, data: Bytes) -> Option<Vec<WriteRequest>> {
        let now = Instant::now();
        
        // 检查是否有待合并的写入
        if let Some(pending) = self.pending_writes.get_mut(path) {
            // 检查时间窗口
            if let Some(first) = pending.first() {
                if now.duration_since(first.timestamp) < self.combine_window {
                    // 在合并窗口内，添加到待合并列表
                    pending.push(WriteRequest {
                        path: path.to_path_buf(),
                        data,
                        offset: None,
                        timestamp: now,
                    });
                    
                    // 如果累积了足够多的写入，返回合并后的结果
                    if pending.len() >= 4 {
                        return Some(std::mem::take(pending));
                    }
                    
                    return None;
                }
            }
        }
        
        // 创建新的待合并列表
        self.pending_writes.insert(
            path.to_path_buf(),
            vec![WriteRequest {
                path: path.to_path_buf(),
                data,
                offset: None,
                timestamp: now,
            }],
        );
        
        None
    }

    /// 获取所有超时的待合并写入
    fn get_expired_writes(&mut self) -> Vec<Vec<WriteRequest>> {
        let now = Instant::now();
        let mut expired = Vec::new();
        
        self.pending_writes.retain(|_, writes| {
            if let Some(first) = writes.first() {
                if now.duration_since(first.timestamp) >= self.combine_window {
                    expired.push(std::mem::take(writes));
                    return false;
                }
            }
            true
        });
        
        expired
    }
}

/// 写入缓冲池
pub struct WriteBufferPool {
    config: Arc<RwLock<WriteBufferPoolConfig>>,
    buffers: Arc<RwLock<HashMap<PathBuf, BufferState>>>,
    write_combiner: Arc<Mutex<WriteCombiner>>,
    current_usage: Arc<Mutex<usize>>,
}

impl WriteBufferPool {
    /// 创建新的写入缓冲池
    pub fn new(config: WriteBufferPoolConfig) -> Self {
        let pool = Self {
            config: Arc::new(RwLock::new(config.clone())),
            buffers: Arc::new(RwLock::new(HashMap::new())),
            write_combiner: Arc::new(Mutex::new(WriteCombiner::new(config.combine_window))),
            current_usage: Arc::new(Mutex::new(0)),
        };

        // 启动定期刷新任务
        let pool_clone = pool.clone();
        tokio::spawn(async move {
            pool_clone.periodic_flush().await;
        });

        // 启动合并检查任务
        if config.enable_combining {
            let pool_clone = pool.clone();
            tokio::spawn(async move {
                pool_clone.combine_checker().await;
            });
        }

        pool
    }

    /// 写入数据
    pub async fn write(&self, path: PathBuf, data: Bytes) -> Result<(), std::io::Error> {
        let config = self.config.read().await;
        
        // 检查是否是小文件，决定是否使用缓冲
        if data.len() > config.small_file_threshold {
            // 大文件直接写入，不使用缓冲
            debug!("Large file write, bypassing buffer: {} bytes to {:?}", data.len(), path);
            return self.write_direct(&path, data).await;
        }

        // 尝试合并写入
        if config.enable_combining {
            let mut combiner = self.write_combiner.lock().await;
            if let Some(combined) = combiner.try_combine(&path, data.clone()) {
                drop(combiner);
                debug!("Combined {} writes for {:?}", combined.len(), path);
                return self.flush_combined(combined).await;
            }
        }

        // 检查总容量限制
        let mut current_usage = self.current_usage.lock().await;
        if *current_usage + data.len() > config.total_capacity {
            warn!("Buffer pool capacity exceeded, forcing flush");
            drop(current_usage);
            self.flush_all().await?;
            current_usage = self.current_usage.lock().await;
        }

        // 获取或创建缓冲区
        let mut buffers = self.buffers.write().await;
        let buffer = buffers.entry(path.clone()).or_insert_with(|| BufferState {
            data: BytesMut::new(),
            last_write: Instant::now(),
            write_count: 0,
            dirty: false,
        });

        // 添加数据到缓冲区
        buffer.data.extend_from_slice(&data);
        buffer.last_write = Instant::now();
        buffer.write_count += 1;
        buffer.dirty = true;
        
        *current_usage += data.len();

        // 检查是否需要刷新
        if buffer.data.len() >= config.max_buffer_size {
            debug!("Buffer size exceeded for {:?}, flushing", path);
            let data = buffer.data.split().freeze();
            buffer.dirty = false;
            drop(buffers);
            
            *current_usage -= data.len();
            drop(current_usage);
            
            self.write_direct(&path, data).await?;
        }

        Ok(())
    }

    /// 直接写入文件（不使用缓冲）
    async fn write_direct(&self, path: &Path, data: Bytes) -> Result<(), std::io::Error> {
        // 确保父目录存在
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .await?;

        file.write_all(&data).await?;
        file.flush().await?;

        Ok(())
    }

    /// 刷新合并的写入
    async fn flush_combined(&self, writes: Vec<WriteRequest>) -> Result<(), std::io::Error> {
        if writes.is_empty() {
            return Ok(());
        }

        // 按路径分组
        let mut grouped: HashMap<PathBuf, Vec<Bytes>> = HashMap::new();
        for write in writes {
            grouped.entry(write.path).or_insert_with(Vec::new).push(write.data);
        }

        // 并行写入每个文件
        let futures: Vec<_> = grouped
            .into_iter()
            .map(|(path, data_vec)| {
                let combined_data = data_vec.into_iter().fold(BytesMut::new(), |mut acc, data| {
                    acc.extend_from_slice(&data);
                    acc
                }).freeze();
                
                let pool = self.clone();
                async move {
                    pool.write_direct(&path, combined_data).await
                }
            })
            .collect();

        futures::future::try_join_all(futures).await?;

        Ok(())
    }

    /// 刷新所有缓冲区
    pub async fn flush_all(&self) -> Result<(), std::io::Error> {
        let mut buffers = self.buffers.write().await;
        let mut current_usage = self.current_usage.lock().await;
        
        let dirty_buffers: Vec<_> = buffers
            .iter_mut()
            .filter(|(_, state)| state.dirty)
            .map(|(path, state)| {
                state.dirty = false;
                let data = state.data.split().freeze();
                *current_usage -= data.len();
                (path.clone(), data)
            })
            .collect();

        drop(buffers);
        drop(current_usage);

        debug!("Flushing {} buffers", dirty_buffers.len());

        // 并行写入所有脏缓冲区
        let futures: Vec<_> = dirty_buffers
            .into_iter()
            .map(|(path, data)| {
                let pool = self.clone();
                async move {
                    pool.write_direct(&path, data).await
                }
            })
            .collect();

        futures::future::try_join_all(futures).await?;

        Ok(())
    }

    /// 定期刷新任务
    async fn periodic_flush(&self) {
        let config = self.config.read().await;
        let mut ticker = interval(config.flush_interval);
        drop(config);

        loop {
            ticker.tick().await;
            
            if let Err(e) = self.flush_all().await {
                warn!("Periodic flush failed: {}", e);
            }
        }
    }

    /// 合并检查任务
    async fn combine_checker(&self) {
        let mut ticker = interval(Duration::from_millis(5));

        loop {
            ticker.tick().await;

            let mut combiner = self.write_combiner.lock().await;
            let expired = combiner.get_expired_writes();
            drop(combiner);

            for writes in expired {
                if let Err(e) = self.flush_combined(writes).await {
                    warn!("Failed to flush combined writes: {}", e);
                }
            }
        }
    }

    /// 获取当前使用的内存大小
    pub async fn get_usage(&self) -> usize {
        *self.current_usage.lock().await
    }

    /// 更新配置
    pub async fn update_config(&self, config: WriteBufferPoolConfig) {
        *self.config.write().await = config;
    }
}

impl Clone for WriteBufferPool {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            buffers: self.buffers.clone(),
            write_combiner: self.write_combiner.clone(),
            current_usage: self.current_usage.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_small_file_buffering() {
        let config = WriteBufferPoolConfig {
            small_file_threshold: 1024,  // 1KB
            flush_interval: Duration::from_secs(1),
            ..Default::default()
        };

        let pool = WriteBufferPool::new(config);
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.txt");

        // 写入小文件
        let data = Bytes::from(vec![0u8; 512]);
        pool.write(path.clone(), data).await.unwrap();

        // 检查缓冲区使用量
        assert!(pool.get_usage().await > 0);

        // 刷新缓冲区
        pool.flush_all().await.unwrap();

        // 验证文件已写入
        assert!(path.exists());
    }

    #[tokio::test]
    async fn test_write_combining() {
        let config = WriteBufferPoolConfig {
            enable_combining: true,
            combine_window: Duration::from_millis(100),
            ..Default::default()
        };

        let pool = WriteBufferPool::new(config);
        let dir = tempdir().unwrap();
        let path = dir.path().join("combined.txt");

        // 快速连续写入多个小数据
        for i in 0..4 {
            let data = Bytes::from(format!("data{}", i));
            pool.write(path.clone(), data).await.unwrap();
        }

        // 等待合并和写入
        tokio::time::sleep(Duration::from_millis(200)).await;

        // 验证文件已写入
        assert!(path.exists());
    }
}