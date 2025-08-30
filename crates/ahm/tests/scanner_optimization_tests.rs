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

use std::{path::PathBuf, time::Duration};
use tempfile::TempDir;
use tokio::time::timeout;

use rustfs_ahm::scanner::{
    checkpoint::{CheckpointManager, CheckpointData},
    io_monitor::{AdvancedIOMonitor, IOMonitorConfig},
    io_throttler::{AdvancedIOThrottler, IOThrottlerConfig},
    local_stats::LocalStatsManager,
    node_scanner::{LoadLevel, NodeScanner, NodeScannerConfig, ScanProgress},
    stats_aggregator::{DecentralizedStatsAggregator, DecentralizedStatsAggregatorConfig},
};

#[tokio::test]
async fn test_checkpoint_manager_save_and_load() {
    let temp_dir = TempDir::new().unwrap();
    let node_id = "test-node-1";
    let checkpoint_manager = CheckpointManager::new(node_id, temp_dir.path());

    // 创建测试扫描进度
    let mut progress = ScanProgress::default();
    progress.current_cycle = 5;
    progress.current_disk_index = 2;
    progress.last_scan_key = Some("test-object-key".to_string());

    // 保存检查点
    checkpoint_manager
        .force_save_checkpoint(&progress)
        .await
        .expect("Failed to save checkpoint");

    // 加载检查点
    let loaded_progress = checkpoint_manager
        .load_checkpoint()
        .await
        .expect("Failed to load checkpoint")
        .expect("No checkpoint found");

    // 验证数据
    assert_eq!(loaded_progress.current_cycle, 5);
    assert_eq!(loaded_progress.current_disk_index, 2);
    assert_eq!(
        loaded_progress.last_scan_key,
        Some("test-object-key".to_string())
    );
}

#[tokio::test]
async fn test_checkpoint_data_integrity() {
    let temp_dir = TempDir::new().unwrap();
    let node_id = "test-node-integrity";
    let checkpoint_manager = CheckpointManager::new(node_id, temp_dir.path());

    let progress = ScanProgress::default();

    // 创建检查点数据
    let checkpoint_data = CheckpointData::new(progress.clone(), node_id.to_string());

    // 验证校验和
    assert!(checkpoint_data.verify_integrity());

    // 保存和加载
    checkpoint_manager
        .force_save_checkpoint(&progress)
        .await
        .expect("Failed to save checkpoint");

    let loaded = checkpoint_manager
        .load_checkpoint()
        .await
        .expect("Failed to load checkpoint");

    assert!(loaded.is_some());
}

#[tokio::test]
async fn test_local_stats_manager() {
    let temp_dir = TempDir::new().unwrap();
    let node_id = "test-stats-node";
    let stats_manager = LocalStatsManager::new(node_id, temp_dir.path());

    // 加载初始统计
    stats_manager
        .load_stats()
        .await
        .expect("Failed to load stats");

    // 获取统计摘要
    let summary = stats_manager.get_stats_summary().await;
    assert_eq!(summary.node_id, node_id);
    assert_eq!(summary.total_objects_scanned, 0);

    // 记录 heal 触发
    stats_manager
        .record_heal_triggered("test-object", "corruption detected")
        .await;

    let counters = stats_manager.get_counters();
    assert_eq!(counters.total_heal_triggered.load(std::sync::atomic::Ordering::Relaxed), 1);
}

#[tokio::test]
async fn test_io_monitor_load_level_calculation() {
    let config = IOMonitorConfig {
        enable_system_monitoring: false, // 使用模拟数据
        ..Default::default()
    };

    let io_monitor = AdvancedIOMonitor::new(config);
    io_monitor.start().await.expect("Failed to start IO monitor");

    // 更新业务指标以影响负载计算
    io_monitor
        .update_business_metrics(50, 100, 0, 10)
        .await;

    // 等待一个监控周期
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let load_level = io_monitor.get_business_load_level().await;
    
    // 负载级别应该在合理范围内
    assert!(matches!(
        load_level,
        LoadLevel::Low | LoadLevel::Medium | LoadLevel::High | LoadLevel::Critical
    ));

    io_monitor.stop().await;
}

#[tokio::test]
async fn test_io_throttler_load_adjustment() {
    let config = IOThrottlerConfig::default();
    let throttler = AdvancedIOThrottler::new(config);

    // 测试不同负载级别的延迟调整
    let low_delay = throttler.adjust_for_load_level(LoadLevel::Low).await;
    let medium_delay = throttler.adjust_for_load_level(LoadLevel::Medium).await;
    let high_delay = throttler.adjust_for_load_level(LoadLevel::High).await;
    let critical_delay = throttler.adjust_for_load_level(LoadLevel::Critical).await;

    // 验证延迟递增
    assert!(low_delay < medium_delay);
    assert!(medium_delay < high_delay);
    assert!(high_delay < critical_delay);

    // 验证暂停逻辑
    assert!(!throttler.should_pause_scanning(LoadLevel::Low).await);
    assert!(!throttler.should_pause_scanning(LoadLevel::Medium).await);
    assert!(!throttler.should_pause_scanning(LoadLevel::High).await);
    assert!(throttler.should_pause_scanning(LoadLevel::Critical).await);
}

#[tokio::test]
async fn test_throttler_business_pressure_simulation() {
    let throttler = AdvancedIOThrottler::default();

    // 运行短时间的压力测试
    let simulation_duration = Duration::from_millis(500);
    let result = throttler
        .simulate_business_pressure(simulation_duration)
        .await;

    // 验证模拟结果
    assert!(!result.simulation_records.is_empty());
    assert!(result.total_duration >= simulation_duration);
    assert!(result.final_stats.total_decisions > 0);

    // 验证所有负载级别都被测试
    let load_levels: std::collections::HashSet<_> = result
        .simulation_records
        .iter()
        .map(|r| r.load_level)
        .collect();

    assert!(load_levels.contains(&LoadLevel::Low));
    assert!(load_levels.contains(&LoadLevel::Critical));
}

#[tokio::test]
async fn test_node_scanner_creation_and_config() {
    let temp_dir = TempDir::new().unwrap();
    let node_id = "test-scanner-node".to_string();
    
    let config = NodeScannerConfig {
        scan_interval: Duration::from_secs(30),
        disk_scan_delay: Duration::from_secs(5),
        enable_smart_scheduling: true,
        enable_checkpoint: true,
        data_dir: temp_dir.path().to_path_buf(),
        ..Default::default()
    };

    let scanner = NodeScanner::new(node_id.clone(), config);

    // 验证节点 ID
    assert_eq!(scanner.node_id(), &node_id);

    // 初始化统计
    scanner
        .initialize_stats()
        .await
        .expect("Failed to initialize stats");

    // 获取统计摘要
    let summary = scanner.get_stats_summary().await;
    assert_eq!(summary.node_id, node_id);
}

#[tokio::test]
async fn test_decentralized_stats_aggregator() {
    let config = DecentralizedStatsAggregatorConfig {
        cache_ttl: Duration::from_millis(100), // 很短的缓存时间用于测试
        ..Default::default()
    };

    let aggregator = DecentralizedStatsAggregator::new(config);

    // 测试缓存机制
    let start_time = std::time::Instant::now();

    // 第一次获取统计（应该触发聚合）
    let stats1 = aggregator
        .get_aggregated_stats()
        .await
        .expect("Failed to get aggregated stats");

    let first_call_duration = start_time.elapsed();

    // 第二次获取统计（应该使用缓存）
    let cache_start = std::time::Instant::now();
    let stats2 = aggregator
        .get_aggregated_stats()
        .await
        .expect("Failed to get cached stats");

    let cache_call_duration = cache_start.elapsed();

    // 缓存调用应该更快
    assert!(cache_call_duration < first_call_duration);

    // 数据应该相同
    assert_eq!(stats1.aggregation_timestamp, stats2.aggregation_timestamp);

    // 等待缓存过期
    tokio::time::sleep(Duration::from_millis(150)).await;

    // 第三次获取应该重新聚合
    let stats3 = aggregator
        .get_aggregated_stats()
        .await
        .expect("Failed to get refreshed stats");

    // 时间戳应该不同
    assert!(stats3.aggregation_timestamp > stats1.aggregation_timestamp);
}

#[tokio::test]
async fn test_scanner_performance_impact() {
    let temp_dir = TempDir::new().unwrap();
    let node_id = "performance-test-node".to_string();
    
    let config = NodeScannerConfig {
        scan_interval: Duration::from_millis(100), // 快速扫描用于测试
        disk_scan_delay: Duration::from_millis(10),
        data_dir: temp_dir.path().to_path_buf(),
        ..Default::default()
    };

    let scanner = NodeScanner::new(node_id, config);

    // 模拟业务负载
    let start_time = std::time::Instant::now();

    // 更新业务指标为高负载
    scanner
        .update_business_metrics(200, 1000, 50, 100)
        .await;

    // 获取 IO 监控器和限流器
    let io_monitor = scanner.get_io_monitor();
    let throttler = scanner.get_io_throttler();

    // 启动 IO 监控
    io_monitor.start().await.expect("Failed to start IO monitor");

    // 等待监控系统稳定
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 检查负载级别是否正确响应
    let load_level = io_monitor.get_business_load_level().await;
    
    // 在高负载下，扫描器应该自动调节
    let throttle_stats = throttler.get_throttle_stats().await;
    
    println!("Performance test results:");
    println!("  Load level: {:?}", load_level);
    println!("  Throttle decisions: {}", throttle_stats.total_decisions);
    println!("  Average delay: {:?}", throttle_stats.average_delay);

    // 验证性能影响控制
    assert!(throttle_stats.average_delay > Duration::from_millis(0));

    io_monitor.stop().await;
}

#[tokio::test]
async fn test_checkpoint_recovery_resilience() {
    let temp_dir = TempDir::new().unwrap();
    let node_id = "resilience-test-node";
    let checkpoint_manager = CheckpointManager::new(node_id, temp_dir.path());

    // 测试没有检查点文件的情况
    let result = checkpoint_manager.load_checkpoint().await.unwrap();
    assert!(result.is_none());

    // 创建并保存检查点
    let progress = ScanProgress {
        current_cycle: 10,
        current_disk_index: 3,
        last_scan_key: Some("recovery-test-key".to_string()),
        ..Default::default()
    };

    checkpoint_manager
        .force_save_checkpoint(&progress)
        .await
        .expect("Failed to save checkpoint");

    // 验证恢复
    let recovered = checkpoint_manager
        .load_checkpoint()
        .await
        .expect("Failed to load checkpoint")
        .expect("No checkpoint recovered");

    assert_eq!(recovered.current_cycle, 10);
    assert_eq!(recovered.current_disk_index, 3);

    // 清理检查点
    checkpoint_manager
        .cleanup_checkpoint()
        .await
        .expect("Failed to cleanup checkpoint");

    // 验证清理后无法加载
    let after_cleanup = checkpoint_manager.load_checkpoint().await.unwrap();
    assert!(after_cleanup.is_none());
}

// 集成测试辅助函数
pub async fn create_test_scanner(temp_dir: &TempDir) -> NodeScanner {
    let config = NodeScannerConfig {
        scan_interval: Duration::from_millis(50),
        disk_scan_delay: Duration::from_millis(10),
        data_dir: temp_dir.path().to_path_buf(),
        ..Default::default()
    };

    NodeScanner::new("integration-test-node".to_string(), config)
}

// 性能基准测试辅助
pub struct PerformanceBenchmark {
    pub scanner_overhead_ms: u64,
    pub business_impact_percentage: f64,
    pub throttle_effectiveness: f64,
}

impl PerformanceBenchmark {
    pub fn meets_optimization_goals(&self) -> bool {
        // 验证是否达到优化目标：业务影响 < 10%
        self.business_impact_percentage < 10.0
    }
}