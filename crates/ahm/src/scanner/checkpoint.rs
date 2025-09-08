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
    time::{Duration, SystemTime},
};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use super::node_scanner::ScanProgress;
use crate::{Error, error::Result};

/// 检查点数据结构
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CheckpointData {
    /// 检查点版本，用于兼容性检查
    pub version: u32,
    /// 检查点创建时间
    pub timestamp: SystemTime,
    /// 扫描进度
    pub progress: ScanProgress,
    /// 节点ID
    pub node_id: String,
    /// 校验和，确保数据完整性
    pub checksum: u64,
}

impl CheckpointData {
    /// 创建新的检查点数据
    pub fn new(progress: ScanProgress, node_id: String) -> Self {
        let mut checkpoint = Self {
            version: 1,
            timestamp: SystemTime::now(),
            progress,
            node_id,
            checksum: 0,
        };

        // 计算校验和
        checkpoint.checksum = checkpoint.calculate_checksum();
        checkpoint
    }

    /// 计算检查点数据的校验和
    fn calculate_checksum(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        self.version.hash(&mut hasher);
        self.node_id.hash(&mut hasher);
        self.progress.current_cycle.hash(&mut hasher);
        self.progress.current_disk_index.hash(&mut hasher);

        if let Some(ref bucket) = self.progress.current_bucket {
            bucket.hash(&mut hasher);
        }

        if let Some(ref key) = self.progress.last_scan_key {
            key.hash(&mut hasher);
        }

        hasher.finish()
    }

    /// 验证检查点数据完整性
    pub fn verify_integrity(&self) -> bool {
        let calculated_checksum = self.calculate_checksum();
        self.checksum == calculated_checksum
    }
}

/// 断点续传管理器
///
/// 负责管理扫描进度的持久化存储和恢复，支持：
/// - 定期保存扫描进度到磁盘
/// - 重启后从检查点恢复扫描状态
/// - 数据完整性验证
/// - 备份和恢复机制
pub struct CheckpointManager {
    /// 主检查点文件路径
    checkpoint_file: PathBuf,
    /// 备份检查点文件路径
    backup_file: PathBuf,
    /// 临时文件路径
    temp_file: PathBuf,
    /// 保存间隔
    save_interval: Duration,
    /// 最后保存时间
    last_save: RwLock<SystemTime>,
    /// 节点ID
    node_id: String,
}

impl CheckpointManager {
    /// 创建新的检查点管理器
    pub fn new(node_id: &str, data_dir: &Path) -> Self {
        // 确保数据目录存在
        if !data_dir.exists() {
            if let Err(e) = std::fs::create_dir_all(data_dir) {
                error!("创建数据目录失败 {:?}: {}", data_dir, e);
            }
        }

        let checkpoint_file = data_dir.join(format!("scanner_checkpoint_{}.json", node_id));
        let backup_file = data_dir.join(format!("scanner_checkpoint_{}.backup", node_id));
        let temp_file = data_dir.join(format!("scanner_checkpoint_{}.tmp", node_id));

        Self {
            checkpoint_file,
            backup_file,
            temp_file,
            save_interval: Duration::from_secs(30), // 30秒保存一次
            last_save: RwLock::new(SystemTime::UNIX_EPOCH),
            node_id: node_id.to_string(),
        }
    }

    /// 保存扫描进度到磁盘
    ///
    /// 使用原子性写入确保数据一致性：
    /// 1. 先写入临时文件
    /// 2. 备份旧的检查点文件
    /// 3. 原子性替换主文件
    pub async fn save_checkpoint(&self, progress: &ScanProgress) -> Result<()> {
        let now = SystemTime::now();
        let last_save = *self.last_save.read().await;

        // 频率控制，避免过于频繁的 IO
        if now.duration_since(last_save).unwrap_or(Duration::ZERO) < self.save_interval {
            return Ok(());
        }

        // 创建检查点数据
        let checkpoint_data = CheckpointData::new(progress.clone(), self.node_id.clone());

        // 序列化为 JSON
        let json_data = serde_json::to_string_pretty(&checkpoint_data)
            .map_err(|e| Error::Serialization(format!("序列化检查点失败: {}", e)))?;

        // 写入临时文件
        tokio::fs::write(&self.temp_file, json_data)
            .await
            .map_err(|e| Error::IO(format!("写入临时检查点文件失败: {}", e)))?;

        // 备份现有检查点文件
        if self.checkpoint_file.exists() {
            tokio::fs::copy(&self.checkpoint_file, &self.backup_file)
                .await
                .map_err(|e| Error::IO(format!("备份检查点文件失败: {}", e)))?;
        }

        // 原子性替换主文件
        tokio::fs::rename(&self.temp_file, &self.checkpoint_file)
            .await
            .map_err(|e| Error::IO(format!("替换检查点文件失败: {}", e)))?;

        // 更新最后保存时间
        *self.last_save.write().await = now;

        debug!(
            "已保存扫描进度到 {:?}, 周期: {}, 磁盘索引: {}",
            self.checkpoint_file, checkpoint_data.progress.current_cycle, checkpoint_data.progress.current_disk_index
        );

        Ok(())
    }

    /// 从磁盘恢复扫描进度
    ///
    /// 恢复策略：
    /// 1. 先尝试主检查点文件
    /// 2. 如果主文件损坏，尝试备份文件
    /// 3. 如果都失败，返回 None（从头开始扫描）
    pub async fn load_checkpoint(&self) -> Result<Option<ScanProgress>> {
        // 先尝试主文件
        match self.load_checkpoint_from_file(&self.checkpoint_file).await {
            Ok(checkpoint) => {
                info!(
                    "从主文件恢复扫描进度: 周期={}, 磁盘索引={}, 最后扫描key={:?}",
                    checkpoint.current_cycle, checkpoint.current_disk_index, checkpoint.last_scan_key
                );
                Ok(Some(checkpoint))
            }
            Err(e) => {
                warn!("主检查点文件损坏或不存在: {}", e);

                // 尝试备份文件
                match self.load_checkpoint_from_file(&self.backup_file).await {
                    Ok(checkpoint) => {
                        warn!(
                            "从备份文件成功恢复扫描进度: 周期={}, 磁盘索引={}",
                            checkpoint.current_cycle, checkpoint.current_disk_index
                        );

                        // 将备份文件复制为新的主文件
                        if let Err(copy_err) = tokio::fs::copy(&self.backup_file, &self.checkpoint_file).await {
                            warn!("恢复主检查点文件失败: {}", copy_err);
                        }

                        Ok(Some(checkpoint))
                    }
                    Err(backup_e) => {
                        warn!("备份文件也损坏或不存在: {}", backup_e);
                        info!("无法恢复扫描进度，将从头开始扫描");
                        Ok(None)
                    }
                }
            }
        }
    }

    /// 从指定文件加载检查点
    async fn load_checkpoint_from_file(&self, file_path: &Path) -> Result<ScanProgress> {
        if !file_path.exists() {
            return Err(Error::NotFound(format!("检查点文件不存在: {:?}", file_path)));
        }

        // 读取文件内容
        let content = tokio::fs::read_to_string(file_path)
            .await
            .map_err(|e| Error::IO(format!("读取检查点文件失败: {}", e)))?;

        // 反序列化
        let checkpoint_data: CheckpointData =
            serde_json::from_str(&content).map_err(|e| Error::Serialization(format!("反序列化检查点失败: {}", e)))?;

        // 验证检查点数据的有效性
        self.validate_checkpoint(&checkpoint_data)?;

        Ok(checkpoint_data.progress)
    }

    /// 验证检查点数据的有效性
    fn validate_checkpoint(&self, checkpoint: &CheckpointData) -> Result<()> {
        // 验证数据完整性
        if !checkpoint.verify_integrity() {
            return Err(Error::InvalidCheckpoint("检查点数据校验失败，可能已损坏".to_string()));
        }

        // 验证节点ID匹配
        if checkpoint.node_id != self.node_id {
            return Err(Error::InvalidCheckpoint(format!(
                "检查点节点ID不匹配: 期望 {}, 实际 {}",
                self.node_id, checkpoint.node_id
            )));
        }

        let now = SystemTime::now();
        let checkpoint_age = now.duration_since(checkpoint.timestamp).unwrap_or(Duration::MAX);

        // 检查点太旧（超过 24 小时），可能数据已经过期
        if checkpoint_age > Duration::from_secs(24 * 3600) {
            return Err(Error::InvalidCheckpoint(format!("检查点数据过旧: {:?}", checkpoint_age)));
        }

        // 验证版本兼容性
        if checkpoint.version > 1 {
            return Err(Error::InvalidCheckpoint(format!("不支持的检查点版本: {}", checkpoint.version)));
        }

        Ok(())
    }

    /// 清理检查点文件
    ///
    /// 在扫描器停止或重置时调用
    pub async fn cleanup_checkpoint(&self) -> Result<()> {
        // 删除主文件
        if self.checkpoint_file.exists() {
            tokio::fs::remove_file(&self.checkpoint_file)
                .await
                .map_err(|e| Error::IO(format!("删除主检查点文件失败: {}", e)))?;
        }

        // 删除备份文件
        if self.backup_file.exists() {
            tokio::fs::remove_file(&self.backup_file)
                .await
                .map_err(|e| Error::IO(format!("删除备份检查点文件失败: {}", e)))?;
        }

        // 删除临时文件
        if self.temp_file.exists() {
            tokio::fs::remove_file(&self.temp_file)
                .await
                .map_err(|e| Error::IO(format!("删除临时检查点文件失败: {}", e)))?;
        }

        info!("已清理所有检查点文件");
        Ok(())
    }

    /// 获取检查点文件信息
    pub async fn get_checkpoint_info(&self) -> Result<Option<CheckpointInfo>> {
        if !self.checkpoint_file.exists() {
            return Ok(None);
        }

        let metadata = tokio::fs::metadata(&self.checkpoint_file)
            .await
            .map_err(|e| Error::IO(format!("获取检查点文件元数据失败: {}", e)))?;

        let content = tokio::fs::read_to_string(&self.checkpoint_file)
            .await
            .map_err(|e| Error::IO(format!("读取检查点文件失败: {}", e)))?;

        let checkpoint_data: CheckpointData =
            serde_json::from_str(&content).map_err(|e| Error::Serialization(format!("解析检查点失败: {}", e)))?;

        Ok(Some(CheckpointInfo {
            file_size: metadata.len(),
            last_modified: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
            checkpoint_timestamp: checkpoint_data.timestamp,
            current_cycle: checkpoint_data.progress.current_cycle,
            current_disk_index: checkpoint_data.progress.current_disk_index,
            completed_disks_count: checkpoint_data.progress.completed_disks.len(),
            is_valid: checkpoint_data.verify_integrity(),
        }))
    }

    /// 强制保存检查点（忽略时间间隔限制）
    pub async fn force_save_checkpoint(&self, progress: &ScanProgress) -> Result<()> {
        // 临时重置最后保存时间，强制保存
        *self.last_save.write().await = SystemTime::UNIX_EPOCH;
        self.save_checkpoint(progress).await
    }

    /// 设置保存间隔
    pub async fn set_save_interval(&mut self, interval: Duration) {
        self.save_interval = interval;
        info!("检查点保存间隔设置为: {:?}", interval);
    }
}

/// 检查点信息
#[derive(Debug, Clone)]
pub struct CheckpointInfo {
    /// 文件大小
    pub file_size: u64,
    /// 文件最后修改时间
    pub last_modified: SystemTime,
    /// 检查点创建时间
    pub checkpoint_timestamp: SystemTime,
    /// 当前扫描周期
    pub current_cycle: u64,
    /// 当前磁盘索引
    pub current_disk_index: usize,
    /// 已完成磁盘数量
    pub completed_disks_count: usize,
    /// 检查点是否有效
    pub is_valid: bool,
}
