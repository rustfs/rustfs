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

use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    sync::Arc,
    time::{Duration, SystemTime},
};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use rustfs_common::data_usage::DataUsageInfo;

use crate::{error::Result, Error};
use super::node_scanner::{DiskStats, LocalScanStats, BucketStats};

/// 统计数据持久化管理器
/// 
/// 负责本地统计数据的收集、存储和持久化：
/// - 实时更新扫描统计数据
/// - 定期持久化到磁盘
/// - 提供统计数据查询接口
/// - 支持统计数据的增量更新
pub struct LocalStatsManager {
    /// 节点ID
    node_id: String,
    /// 统计数据文件路径
    stats_file: PathBuf,
    /// 备份文件路径
    backup_file: PathBuf,
    /// 临时文件路径
    temp_file: PathBuf,
    /// 本地统计数据
    stats: Arc<RwLock<LocalScanStats>>,
    /// 保存间隔
    save_interval: Duration,
    /// 最后保存时间
    last_save: Arc<RwLock<SystemTime>>,
    /// 统计计数器
    counters: Arc<StatsCounters>,
}

/// 统计计数器
pub struct StatsCounters {
    /// 总扫描对象数
    pub total_objects_scanned: AtomicU64,
    /// 健康对象数
    pub total_healthy_objects: AtomicU64,
    /// 损坏对象数  
    pub total_corrupted_objects: AtomicU64,
    /// 总扫描字节数
    pub total_bytes_scanned: AtomicU64,
    /// 扫描错误数
    pub total_scan_errors: AtomicU64,
    /// heal 触发次数
    pub total_heal_triggered: AtomicU64,
}

impl Default for StatsCounters {
    fn default() -> Self {
        Self {
            total_objects_scanned: AtomicU64::new(0),
            total_healthy_objects: AtomicU64::new(0),
            total_corrupted_objects: AtomicU64::new(0),
            total_bytes_scanned: AtomicU64::new(0),
            total_scan_errors: AtomicU64::new(0),
            total_heal_triggered: AtomicU64::new(0),
        }
    }
}

/// 扫描结果条目
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanResultEntry {
    /// 对象路径
    pub object_path: String,
    /// 存储桶名称
    pub bucket_name: String,
    /// 对象大小
    pub object_size: u64,
    /// 是否健康
    pub is_healthy: bool,
    /// 错误信息（如果有）
    pub error_message: Option<String>,
    /// 扫描时间
    pub scan_time: SystemTime,
    /// 磁盘ID
    pub disk_id: String,
}

/// 批量扫描结果
#[derive(Debug, Clone)]
pub struct BatchScanResult {
    /// 磁盘ID
    pub disk_id: String,
    /// 扫描结果条目
    pub entries: Vec<ScanResultEntry>,
    /// 扫描开始时间
    pub scan_start: SystemTime,
    /// 扫描结束时间
    pub scan_end: SystemTime,
    /// 扫描耗时
    pub scan_duration: Duration,
}

impl LocalStatsManager {
    /// 创建新的本地统计管理器
    pub fn new(node_id: &str, data_dir: &Path) -> Self {
        // 确保数据目录存在
        if !data_dir.exists() {
            if let Err(e) = std::fs::create_dir_all(data_dir) {
                error!("创建统计数据目录失败 {:?}: {}", data_dir, e);
            }
        }

        let stats_file = data_dir.join(format!("scanner_stats_{}.json", node_id));
        let backup_file = data_dir.join(format!("scanner_stats_{}.backup", node_id));
        let temp_file = data_dir.join(format!("scanner_stats_{}.tmp", node_id));

        Self {
            node_id: node_id.to_string(),
            stats_file,
            backup_file,
            temp_file,
            stats: Arc::new(RwLock::new(LocalScanStats::default())),
            save_interval: Duration::from_secs(60), // 60秒保存一次
            last_save: Arc::new(RwLock::new(SystemTime::UNIX_EPOCH)),
            counters: Arc::new(StatsCounters::default()),
        }
    }

    /// 加载本地统计数据
    pub async fn load_stats(&self) -> Result<()> {
        if !self.stats_file.exists() {
            info!("统计数据文件不存在，将创建新的统计数据");
            return Ok(());
        }

        match self.load_stats_from_file(&self.stats_file).await {
            Ok(stats) => {
                *self.stats.write().await = stats;
                info!("成功加载本地统计数据");
                Ok(())
            }
            Err(e) => {
                warn!("加载主统计文件失败: {}，尝试备份文件", e);
                
                match self.load_stats_from_file(&self.backup_file).await {
                    Ok(stats) => {
                        *self.stats.write().await = stats;
                        warn!("从备份文件恢复统计数据");
                        Ok(())
                    }
                    Err(backup_e) => {
                        warn!("备份文件也无法加载: {}，将使用默认统计数据", backup_e);
                        Ok(())
                    }
                }
            }
        }
    }

    /// 从文件加载统计数据
    async fn load_stats_from_file(&self, file_path: &Path) -> Result<LocalScanStats> {
        let content = tokio::fs::read_to_string(file_path).await
            .map_err(|e| Error::IO(format!("读取统计文件失败: {}", e)))?;

        let stats: LocalScanStats = serde_json::from_str(&content)
            .map_err(|e| Error::Serialization(format!("反序列化统计数据失败: {}", e)))?;

        Ok(stats)
    }

    /// 保存统计数据到磁盘
    pub async fn save_stats(&self) -> Result<()> {
        let now = SystemTime::now();
        let last_save = *self.last_save.read().await;

        // 频率控制
        if now.duration_since(last_save).unwrap_or(Duration::ZERO) < self.save_interval {
            return Ok(());
        }

        let stats = self.stats.read().await.clone();
        
        // 序列化
        let json_data = serde_json::to_string_pretty(&stats)
            .map_err(|e| Error::Serialization(format!("序列化统计数据失败: {}", e)))?;

        // 原子性写入
        tokio::fs::write(&self.temp_file, json_data).await
            .map_err(|e| Error::IO(format!("写入临时统计文件失败: {}", e)))?;

        // 备份现有文件
        if self.stats_file.exists() {
            tokio::fs::copy(&self.stats_file, &self.backup_file).await
                .map_err(|e| Error::IO(format!("备份统计文件失败: {}", e)))?;
        }

        // 原子性替换
        tokio::fs::rename(&self.temp_file, &self.stats_file).await
            .map_err(|e| Error::IO(format!("替换统计文件失败: {}", e)))?;

        *self.last_save.write().await = now;

        debug!("已保存本地统计数据到 {:?}", self.stats_file);
        Ok(())
    }

    /// 强制保存统计数据
    pub async fn force_save_stats(&self) -> Result<()> {
        *self.last_save.write().await = SystemTime::UNIX_EPOCH;
        self.save_stats().await
    }

    /// 更新磁盘扫描结果
    pub async fn update_disk_scan_result(&self, result: &BatchScanResult) -> Result<()> {
        let mut stats = self.stats.write().await;

        // 更新磁盘统计
        let disk_stat = stats.disks_stats.entry(result.disk_id.clone())
            .or_insert_with(|| DiskStats {
                disk_id: result.disk_id.clone(),
                ..Default::default()
            });

        let healthy_count = result.entries.iter().filter(|e| e.is_healthy).count() as u64;
        let error_count = result.entries.iter().filter(|e| !e.is_healthy).count() as u64;

        disk_stat.objects_scanned += result.entries.len() as u64;
        disk_stat.errors_count += error_count;
        disk_stat.last_scan_time = result.scan_end;
        disk_stat.scan_duration = result.scan_duration;
        disk_stat.scan_completed = true;

        // 更新总体统计
        stats.objects_scanned += result.entries.len() as u64;
        stats.healthy_objects += healthy_count;
        stats.corrupted_objects += error_count;
        stats.last_update = SystemTime::now();

        // 按存储桶更新统计
        for entry in &result.entries {
            let _bucket_stat = stats.buckets_stats.entry(entry.bucket_name.clone())
                .or_insert_with(BucketStats::default);

            // TODO: 更新 BucketStats 的具体字段
            // 这里需要根据 BucketStats 的实际结构来更新
        }

        // 更新原子计数器
        self.counters.total_objects_scanned.fetch_add(result.entries.len() as u64, Ordering::Relaxed);
        self.counters.total_healthy_objects.fetch_add(healthy_count, Ordering::Relaxed);
        self.counters.total_corrupted_objects.fetch_add(error_count, Ordering::Relaxed);
        
        let total_bytes: u64 = result.entries.iter().map(|e| e.object_size).sum();
        self.counters.total_bytes_scanned.fetch_add(total_bytes, Ordering::Relaxed);

        if error_count > 0 {
            self.counters.total_scan_errors.fetch_add(error_count, Ordering::Relaxed);
        }

        drop(stats);

        debug!("更新磁盘 {} 扫描结果：对象数 {}, 健康 {}, 错误 {}", 
               result.disk_id, result.entries.len(), healthy_count, error_count);

        Ok(())
    }

    /// 记录单个对象扫描结果
    pub async fn record_object_scan(&self, entry: ScanResultEntry) -> Result<()> {
        let result = BatchScanResult {
            disk_id: entry.disk_id.clone(),
            entries: vec![entry],
            scan_start: SystemTime::now(),
            scan_end: SystemTime::now(),
            scan_duration: Duration::from_millis(0),
        };

        self.update_disk_scan_result(&result).await
    }

    /// 获取本地统计数据的副本
    pub async fn get_stats(&self) -> LocalScanStats {
        self.stats.read().await.clone()
    }

    /// 获取实时计数器
    pub fn get_counters(&self) -> Arc<StatsCounters> {
        self.counters.clone()
    }

    /// 重置统计数据
    pub async fn reset_stats(&self) -> Result<()> {
        {
            let mut stats = self.stats.write().await;
            *stats = LocalScanStats::default();
        }

        // 重置计数器
        self.counters.total_objects_scanned.store(0, Ordering::Relaxed);
        self.counters.total_healthy_objects.store(0, Ordering::Relaxed);
        self.counters.total_corrupted_objects.store(0, Ordering::Relaxed);
        self.counters.total_bytes_scanned.store(0, Ordering::Relaxed);
        self.counters.total_scan_errors.store(0, Ordering::Relaxed);
        self.counters.total_heal_triggered.store(0, Ordering::Relaxed);

        info!("已重置本地统计数据");
        Ok(())
    }

    /// 获取统计摘要
    pub async fn get_stats_summary(&self) -> StatsSummary {
        let stats = self.stats.read().await;
        
        StatsSummary {
            node_id: self.node_id.clone(),
            total_objects_scanned: self.counters.total_objects_scanned.load(Ordering::Relaxed),
            total_healthy_objects: self.counters.total_healthy_objects.load(Ordering::Relaxed),
            total_corrupted_objects: self.counters.total_corrupted_objects.load(Ordering::Relaxed),
            total_bytes_scanned: self.counters.total_bytes_scanned.load(Ordering::Relaxed),
            total_scan_errors: self.counters.total_scan_errors.load(Ordering::Relaxed),
            total_heal_triggered: self.counters.total_heal_triggered.load(Ordering::Relaxed),
            total_disks: stats.disks_stats.len(),
            total_buckets: stats.buckets_stats.len(),
            last_update: stats.last_update,
            scan_progress: stats.scan_progress.clone(),
        }
    }

    /// 记录 heal 触发
    pub async fn record_heal_triggered(&self, object_path: &str, error_message: &str) {
        self.counters.total_heal_triggered.fetch_add(1, Ordering::Relaxed);
        
        info!("记录 heal 触发: 对象={}, 错误={}", object_path, error_message);
    }

    /// 更新数据使用统计
    pub async fn update_data_usage(&self, data_usage: DataUsageInfo) {
        let mut stats = self.stats.write().await;
        stats.data_usage = data_usage;
        stats.last_update = SystemTime::now();
        
        debug!("更新数据使用统计");
    }

    /// 清理统计文件
    pub async fn cleanup_stats_files(&self) -> Result<()> {
        // 删除主文件
        if self.stats_file.exists() {
            tokio::fs::remove_file(&self.stats_file).await
                .map_err(|e| Error::IO(format!("删除统计文件失败: {}", e)))?;
        }

        // 删除备份文件
        if self.backup_file.exists() {
            tokio::fs::remove_file(&self.backup_file).await
                .map_err(|e| Error::IO(format!("删除备份统计文件失败: {}", e)))?;
        }

        // 删除临时文件
        if self.temp_file.exists() {
            tokio::fs::remove_file(&self.temp_file).await
                .map_err(|e| Error::IO(format!("删除临时统计文件失败: {}", e)))?;
        }

        info!("已清理所有统计文件");
        Ok(())
    }

    /// 设置保存间隔
    pub fn set_save_interval(&mut self, interval: Duration) {
        self.save_interval = interval;
        info!("统计数据保存间隔设置为: {:?}", interval);
    }
}

/// 统计摘要
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsSummary {
    /// 节点ID
    pub node_id: String,
    /// 总扫描对象数
    pub total_objects_scanned: u64,
    /// 总健康对象数
    pub total_healthy_objects: u64,
    /// 总损坏对象数
    pub total_corrupted_objects: u64,
    /// 总扫描字节数
    pub total_bytes_scanned: u64,
    /// 总扫描错误数
    pub total_scan_errors: u64,
    /// 总 heal 触发次数
    pub total_heal_triggered: u64,
    /// 磁盘总数
    pub total_disks: usize,
    /// 存储桶总数
    pub total_buckets: usize,
    /// 最后更新时间
    pub last_update: SystemTime,
    /// 扫描进度
    pub scan_progress: super::node_scanner::ScanProgress,
}