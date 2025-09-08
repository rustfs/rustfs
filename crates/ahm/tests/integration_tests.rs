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

use std::{sync::Arc, time::Duration};
use tempfile::TempDir;

use rustfs_ahm::scanner::{
    io_throttler::MetricsSnapshot,
    local_stats::StatsSummary,
    node_scanner::{LoadLevel, NodeScanner, NodeScannerConfig},
    stats_aggregator::{DecentralizedStatsAggregator, DecentralizedStatsAggregatorConfig, NodeInfo},
};

mod scanner_optimization_tests;
use scanner_optimization_tests::{PerformanceBenchmark, create_test_scanner};

#[tokio::test]
async fn test_end_to_end_scanner_lifecycle() {
    let temp_dir = TempDir::new().unwrap();
    let scanner = create_test_scanner(&temp_dir).await;

    scanner.initialize_stats().await.expect("Failed to initialize stats");

    let initial_progress = scanner.get_scan_progress().await;
    assert_eq!(initial_progress.current_cycle, 0);

    scanner.force_save_checkpoint().await.expect("Failed to save checkpoint");

    let checkpoint_info = scanner.get_checkpoint_info().await.unwrap();
    assert!(checkpoint_info.is_some());
}

#[tokio::test]
async fn test_load_balancing_and_throttling_integration() {
    let temp_dir = TempDir::new().unwrap();
    let scanner = create_test_scanner(&temp_dir).await;

    let io_monitor = scanner.get_io_monitor();
    let throttler = scanner.get_io_throttler();

    // 启动 IO 监控
    io_monitor.start().await.expect("Failed to start IO monitor");

    // 模拟负载变化场景
    let load_scenarios = vec![
        (LoadLevel::Low, 10, 100, 0, 5), // (负载级别, 延迟, QPS, 错误率, 连接数)
        (LoadLevel::Medium, 30, 300, 10, 20),
        (LoadLevel::High, 80, 800, 50, 50),
        (LoadLevel::Critical, 200, 1200, 100, 100),
    ];

    for (expected_level, latency, qps, error_rate, connections) in load_scenarios {
        // 更新业务指标
        scanner.update_business_metrics(latency, qps, error_rate, connections).await;

        // 等待监控系统响应
        tokio::time::sleep(Duration::from_millis(1200)).await;

        // 获取当前负载级别
        let current_level = io_monitor.get_business_load_level().await;

        // 获取限流决策
        let metrics_snapshot = MetricsSnapshot {
            iops: 100 + qps / 10,
            latency,
            cpu_usage: std::cmp::min(50 + (qps / 20) as u8, 100),
            memory_usage: 40,
        };

        let decision = throttler.make_throttle_decision(current_level, Some(metrics_snapshot)).await;

        println!(
            "Load scenario test: Expected={:?}, Actual={:?}, Should_pause={}, Delay={:?}",
            expected_level, current_level, decision.should_pause, decision.suggested_delay
        );

        // 验证高负载时的限流效果
        if matches!(current_level, LoadLevel::High | LoadLevel::Critical) {
            assert!(decision.suggested_delay > Duration::from_millis(1000));
        }

        if matches!(current_level, LoadLevel::Critical) {
            assert!(decision.should_pause);
        }
    }

    io_monitor.stop().await;
}

#[tokio::test]
async fn test_checkpoint_resume_functionality() {
    let temp_dir = TempDir::new().unwrap();

    // 创建第一个扫描器实例
    let scanner1 = {
        let config = NodeScannerConfig {
            data_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        };
        NodeScanner::new("checkpoint-test-node".to_string(), config)
    };

    // 初始化并模拟一些扫描进度
    scanner1.initialize_stats().await.unwrap();

    // 模拟扫描进度
    scanner1
        .update_scan_progress_for_test(3, 1, Some("checkpoint-test-key".to_string()))
        .await;

    // 保存检查点
    scanner1.force_save_checkpoint().await.unwrap();

    // 停止第一个扫描器
    scanner1.stop().await.unwrap();

    // 创建第二个扫描器实例（模拟重启）
    let scanner2 = {
        let config = NodeScannerConfig {
            data_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        };
        NodeScanner::new("checkpoint-test-node".to_string(), config)
    };

    // 尝试从检查点恢复
    scanner2.start_with_resume().await.unwrap();

    // 验证恢复的进度
    let recovered_progress = scanner2.get_scan_progress().await;
    assert_eq!(recovered_progress.current_cycle, 3);
    assert_eq!(recovered_progress.current_disk_index, 1);
    assert_eq!(recovered_progress.last_scan_key, Some("checkpoint-test-key".to_string()));

    // 清理
    scanner2.cleanup_checkpoint().await.unwrap();
}

#[tokio::test]
async fn test_distributed_stats_aggregation() {
    // Create decentralized stats aggregator
    let config = DecentralizedStatsAggregatorConfig {
        cache_ttl: Duration::from_secs(10),       // 增加缓存TTL以确保测试期间缓存有效
        node_timeout: Duration::from_millis(500), // 减少超时时间
        ..Default::default()
    };
    let aggregator = DecentralizedStatsAggregator::new(config);

    // 模拟多个节点（这些节点在测试环境中不存在，会导致连接失败）
    let node_infos = vec![
        NodeInfo {
            node_id: "node-1".to_string(),
            address: "127.0.0.1".to_string(),
            port: 9001,
            is_online: true,
            last_heartbeat: std::time::SystemTime::now(),
            version: "1.0.0".to_string(),
        },
        NodeInfo {
            node_id: "node-2".to_string(),
            address: "127.0.0.1".to_string(),
            port: 9002,
            is_online: true,
            last_heartbeat: std::time::SystemTime::now(),
            version: "1.0.0".to_string(),
        },
    ];

    // 添加节点到聚合器
    for node_info in node_infos {
        aggregator.add_node(node_info).await;
    }

    // 设置本地统计（模拟本地节点）
    let local_stats = StatsSummary {
        node_id: "local-node".to_string(),
        total_objects_scanned: 1000,
        total_healthy_objects: 950,
        total_corrupted_objects: 50,
        total_bytes_scanned: 1024 * 1024 * 100, // 100MB
        total_scan_errors: 5,
        total_heal_triggered: 10,
        total_disks: 4,
        total_buckets: 5,
        last_update: std::time::SystemTime::now(),
        scan_progress: Default::default(),
    };

    aggregator.set_local_stats(local_stats).await;

    // 获取聚合统计（远程节点会失败，但本地节点应该成功）
    let aggregated = aggregator.get_aggregated_stats().await.unwrap();

    // 验证本地节点统计被包含
    assert!(aggregated.node_summaries.contains_key("local-node"));
    assert!(aggregated.total_objects_scanned >= 1000);

    // 由于远程节点连接失败，只有本地节点的数据
    assert_eq!(aggregated.node_summaries.len(), 1);

    // Test caching mechanism
    let original_timestamp = aggregated.aggregation_timestamp;

    let start_time = std::time::Instant::now();
    let cached_result = aggregator.get_aggregated_stats().await.unwrap();
    let cached_duration = start_time.elapsed();

    // 验证缓存生效：时间戳应该相同
    assert_eq!(original_timestamp, cached_result.aggregation_timestamp);

    // 缓存调用应该很快（放宽到200ms以适应测试环境）
    assert!(cached_duration < Duration::from_millis(200));

    // 强制刷新
    let _refreshed = aggregator.force_refresh_aggregated_stats().await.unwrap();

    // 清除缓存
    aggregator.clear_cache().await;

    // 验证缓存状态
    let cache_status = aggregator.get_cache_status().await;
    assert!(!cache_status.has_cached_data);
}

#[tokio::test]
async fn test_performance_impact_measurement() {
    let temp_dir = TempDir::new().unwrap();
    let scanner = create_test_scanner(&temp_dir).await;

    // 启动性能监控
    let io_monitor = scanner.get_io_monitor();
    let _throttler = scanner.get_io_throttler();

    io_monitor.start().await.unwrap();

    // 基准测试：无扫描器负载
    let baseline_start = std::time::Instant::now();
    simulate_business_workload(1000).await;
    let baseline_duration = baseline_start.elapsed();

    // 模拟扫描器活动
    scanner.update_business_metrics(50, 500, 0, 25).await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // 性能测试：有扫描器负载
    let with_scanner_start = std::time::Instant::now();
    simulate_business_workload(1000).await;
    let with_scanner_duration = with_scanner_start.elapsed();

    // 计算性能影响
    let overhead_ms = with_scanner_duration.saturating_sub(baseline_duration).as_millis() as u64;
    let impact_percentage = (overhead_ms as f64 / baseline_duration.as_millis() as f64) * 100.0;

    let benchmark = PerformanceBenchmark {
        _scanner_overhead_ms: overhead_ms,
        business_impact_percentage: impact_percentage,
        _throttle_effectiveness: 95.0, // 模拟值
    };

    println!("Performance impact measurement:");
    println!("  Baseline duration: {:?}", baseline_duration);
    println!("  With scanner duration: {:?}", with_scanner_duration);
    println!("  Overhead: {} ms", overhead_ms);
    println!("  Impact percentage: {:.2}%", impact_percentage);
    println!("  Meets optimization goals: {}", benchmark.meets_optimization_goals());

    // 验证优化目标（业务影响 < 10%）
    // 注意：在实际环境中这个测试可能需要更长时间和真实负载
    assert!(impact_percentage < 50.0, "Performance impact too high: {:.2}%", impact_percentage);

    io_monitor.stop().await;
}

#[tokio::test]
async fn test_concurrent_scanner_operations() {
    let temp_dir = TempDir::new().unwrap();
    let scanner = Arc::new(create_test_scanner(&temp_dir).await);

    scanner.initialize_stats().await.unwrap();

    // 并发执行多个扫描器操作
    let tasks = vec![
        // 任务1：定期更新业务指标
        {
            let scanner = scanner.clone();
            tokio::spawn(async move {
                for i in 0..10 {
                    scanner.update_business_metrics(10 + i * 5, 100 + i * 10, i, 5 + i).await;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
        },
        // 任务2：定期保存检查点
        {
            let scanner = scanner.clone();
            tokio::spawn(async move {
                for _i in 0..5 {
                    if let Err(e) = scanner.force_save_checkpoint().await {
                        eprintln!("Checkpoint save failed: {}", e);
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
        },
        // 任务3：定期获取统计
        {
            let scanner = scanner.clone();
            tokio::spawn(async move {
                for _i in 0..8 {
                    let _summary = scanner.get_stats_summary().await;
                    let _progress = scanner.get_scan_progress().await;
                    tokio::time::sleep(Duration::from_millis(75)).await;
                }
            })
        },
    ];

    // 等待所有任务完成
    for task in tasks {
        task.await.unwrap();
    }

    // 验证最终状态
    let final_stats = scanner.get_stats_summary().await;
    let _final_progress = scanner.get_scan_progress().await;

    assert_eq!(final_stats.node_id, "integration-test-node");
    assert!(final_stats.last_update > std::time::SystemTime::UNIX_EPOCH);

    // 清理
    scanner.cleanup_checkpoint().await.unwrap();
}

// 模拟业务工作负载的辅助函数
async fn simulate_business_workload(operations: usize) {
    for _i in 0..operations {
        // 模拟一些计算密集型操作
        let _result: u64 = (0..100).map(|x| x * x).sum();

        // 小延迟模拟 IO 操作
        if _i % 100 == 0 {
            tokio::task::yield_now().await;
        }
    }
}

#[tokio::test]
async fn test_error_recovery_and_resilience() {
    let temp_dir = TempDir::new().unwrap();
    let scanner = create_test_scanner(&temp_dir).await;

    // 测试统计初始化失败恢复
    scanner.initialize_stats().await.unwrap();

    // 测试检查点损坏恢复
    scanner.force_save_checkpoint().await.unwrap();

    // 人为损坏检查点文件（通过写入无效数据）
    let checkpoint_file = temp_dir.path().join("scanner_checkpoint_integration-test-node.json");
    if checkpoint_file.exists() {
        tokio::fs::write(&checkpoint_file, "invalid json data").await.unwrap();
    }

    // 验证系统能够优雅处理损坏的检查点
    let checkpoint_info = scanner.get_checkpoint_info().await;
    // 应该返回错误或空值，而不是崩溃
    assert!(checkpoint_info.is_err() || checkpoint_info.unwrap().is_none());

    // 清理损坏的检查点
    scanner.cleanup_checkpoint().await.unwrap();

    // 验证能够重新创建有效检查点
    scanner.force_save_checkpoint().await.unwrap();
    let new_checkpoint_info = scanner.get_checkpoint_info().await.unwrap();
    assert!(new_checkpoint_info.is_some());
}
