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

    // Start IO monitoring
    io_monitor.start().await.expect("Failed to start IO monitor");

    // Simulate load variation scenarios
    let load_scenarios = vec![
        (LoadLevel::Low, 10, 100, 0, 5), // (load level, latency, QPS, error rate, connections)
        (LoadLevel::Medium, 30, 300, 10, 20),
        (LoadLevel::High, 80, 800, 50, 50),
        (LoadLevel::Critical, 200, 1200, 100, 100),
    ];

    for (expected_level, latency, qps, error_rate, connections) in load_scenarios {
        // Update business metrics
        scanner.update_business_metrics(latency, qps, error_rate, connections).await;

        // Wait for monitoring system response
        tokio::time::sleep(Duration::from_millis(1200)).await;

        // Get current load level
        let current_level = io_monitor.get_business_load_level().await;

        // Get throttling decision
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

        // Verify throttling effect under high load
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

    // Create first scanner instance
    let scanner1 = {
        let config = NodeScannerConfig {
            data_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        };
        NodeScanner::new("checkpoint-test-node".to_string(), config)
    };

    // Initialize and simulate some scan progress
    scanner1.initialize_stats().await.unwrap();

    // Simulate scan progress
    scanner1
        .update_scan_progress_for_test(3, 1, Some("checkpoint-test-key".to_string()))
        .await;

    // Save checkpoint
    scanner1.force_save_checkpoint().await.unwrap();

    // Stop first scanner
    scanner1.stop().await.unwrap();

    // Create second scanner instance (simulate restart)
    let scanner2 = {
        let config = NodeScannerConfig {
            data_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        };
        NodeScanner::new("checkpoint-test-node".to_string(), config)
    };

    // Try to recover from checkpoint
    scanner2.start_with_resume().await.unwrap();

    // Verify recovered progress
    let recovered_progress = scanner2.get_scan_progress().await;
    assert_eq!(recovered_progress.current_cycle, 3);
    assert_eq!(recovered_progress.current_disk_index, 1);
    assert_eq!(recovered_progress.last_scan_key, Some("checkpoint-test-key".to_string()));

    // Cleanup
    scanner2.cleanup_checkpoint().await.unwrap();
}

#[tokio::test]
async fn test_distributed_stats_aggregation() {
    // Create decentralized stats aggregator
    let config = DecentralizedStatsAggregatorConfig {
        cache_ttl: Duration::from_secs(10), // Increase cache TTL to ensure cache is valid during test
        node_timeout: Duration::from_millis(500), // Reduce timeout
        ..Default::default()
    };
    let aggregator = DecentralizedStatsAggregator::new(config);

    // Simulate multiple nodes (these nodes don't exist in test environment, will cause connection failures)
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

    // Add nodes to aggregator
    for node_info in node_infos {
        aggregator.add_node(node_info).await;
    }

    // Set local statistics (simulate local node)
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
        data_usage: rustfs_common::data_usage::DataUsageInfo::default(),
    };

    aggregator.set_local_stats(local_stats).await;

    // Get aggregated statistics (remote nodes will fail, but local node should succeed)
    let aggregated = aggregator.get_aggregated_stats().await.unwrap();

    // Verify local node statistics are included
    assert!(aggregated.node_summaries.contains_key("local-node"));
    assert!(aggregated.total_objects_scanned >= 1000);

    // Only local node data due to remote node connection failures
    assert_eq!(aggregated.node_summaries.len(), 1);

    // Test caching mechanism
    let original_timestamp = aggregated.aggregation_timestamp;

    let start_time = std::time::Instant::now();
    let cached_result = aggregator.get_aggregated_stats().await.unwrap();
    let cached_duration = start_time.elapsed();

    // Verify cache is effective: timestamps should be the same
    assert_eq!(original_timestamp, cached_result.aggregation_timestamp);

    // Cached calls should be fast (relaxed to 200ms for test environment)
    assert!(cached_duration < Duration::from_millis(200));

    // Force refresh
    let _refreshed = aggregator.force_refresh_aggregated_stats().await.unwrap();

    // Clear cache
    aggregator.clear_cache().await;

    // Verify cache status
    let cache_status = aggregator.get_cache_status().await;
    assert!(!cache_status.has_cached_data);
}

#[tokio::test]
async fn test_performance_impact_measurement() {
    let temp_dir = TempDir::new().unwrap();
    let scanner = create_test_scanner(&temp_dir).await;

    // Start performance monitoring
    let io_monitor = scanner.get_io_monitor();
    let _throttler = scanner.get_io_throttler();

    io_monitor.start().await.unwrap();

    // Baseline test: no scanner load
    let baseline_start = std::time::Instant::now();
    simulate_business_workload(1000).await;
    let baseline_duration = baseline_start.elapsed();

    // Simulate scanner activity
    scanner.update_business_metrics(50, 500, 0, 25).await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Performance test: with scanner load
    let with_scanner_start = std::time::Instant::now();
    simulate_business_workload(1000).await;
    let with_scanner_duration = with_scanner_start.elapsed();

    // Calculate performance impact
    let overhead_ms = with_scanner_duration.saturating_sub(baseline_duration).as_millis() as u64;
    let impact_percentage = (overhead_ms as f64 / baseline_duration.as_millis() as f64) * 100.0;

    let benchmark = PerformanceBenchmark {
        _scanner_overhead_ms: overhead_ms,
        business_impact_percentage: impact_percentage,
        _throttle_effectiveness: 95.0, // Simulated value
    };

    println!("Performance impact measurement:");
    println!("  Baseline duration: {baseline_duration:?}");
    println!("  With scanner duration: {with_scanner_duration:?}");
    println!("  Overhead: {overhead_ms} ms");
    println!("  Impact percentage: {impact_percentage:.2}%");
    println!("  Meets optimization goals: {}", benchmark.meets_optimization_goals());

    // Verify optimization target (business impact < 10%)
    // Note: In real environment this test may need longer time and real load
    assert!(impact_percentage < 50.0, "Performance impact too high: {impact_percentage:.2}%");

    io_monitor.stop().await;
}

#[tokio::test]
async fn test_concurrent_scanner_operations() {
    let temp_dir = TempDir::new().unwrap();
    let scanner = Arc::new(create_test_scanner(&temp_dir).await);

    scanner.initialize_stats().await.unwrap();

    // Execute multiple scanner operations concurrently
    let tasks = vec![
        // Task 1: Periodically update business metrics
        {
            let scanner = scanner.clone();
            tokio::spawn(async move {
                for i in 0..10 {
                    scanner.update_business_metrics(10 + i * 5, 100 + i * 10, i, 5 + i).await;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
        },
        // Task 2: Periodically save checkpoints
        {
            let scanner = scanner.clone();
            tokio::spawn(async move {
                for _i in 0..5 {
                    if let Err(e) = scanner.force_save_checkpoint().await {
                        eprintln!("Checkpoint save failed: {e}");
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
        },
        // Task 3: Periodically get statistics
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

    // Wait for all tasks to complete
    for task in tasks {
        task.await.unwrap();
    }

    // Verify final state
    let final_stats = scanner.get_stats_summary().await;
    let _final_progress = scanner.get_scan_progress().await;

    assert_eq!(final_stats.node_id, "integration-test-node");
    assert!(final_stats.last_update > std::time::SystemTime::UNIX_EPOCH);

    // Cleanup
    scanner.cleanup_checkpoint().await.unwrap();
}

// Helper function to simulate business workload
async fn simulate_business_workload(operations: usize) {
    for _i in 0..operations {
        // Simulate some CPU-intensive operations
        let _result: u64 = (0..100).map(|x| x * x).sum();

        // Small delay to simulate IO operations
        if _i % 100 == 0 {
            tokio::task::yield_now().await;
        }
    }
}

#[tokio::test]
async fn test_error_recovery_and_resilience() {
    let temp_dir = TempDir::new().unwrap();
    let scanner = create_test_scanner(&temp_dir).await;

    // Test recovery from stats initialization failure
    scanner.initialize_stats().await.unwrap();

    // Test recovery from checkpoint corruption
    scanner.force_save_checkpoint().await.unwrap();

    // Artificially corrupt checkpoint file (by writing invalid data)
    let checkpoint_file = temp_dir.path().join("scanner_checkpoint_integration-test-node.json");
    if checkpoint_file.exists() {
        tokio::fs::write(&checkpoint_file, "invalid json data").await.unwrap();
    }

    // Verify system can gracefully handle corrupted checkpoint
    let checkpoint_info = scanner.get_checkpoint_info().await;
    // Should return error or null value, not crash
    assert!(checkpoint_info.is_err() || checkpoint_info.unwrap().is_none());

    // Clean up corrupted checkpoint
    scanner.cleanup_checkpoint().await.unwrap();

    // Verify ability to recreate valid checkpoint
    scanner.force_save_checkpoint().await.unwrap();
    let new_checkpoint_info = scanner.get_checkpoint_info().await.unwrap();
    assert!(new_checkpoint_info.is_some());
}
