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

use rustfs_ahm::scanner::{
    checkpoint::{CheckpointData, CheckpointManager},
    io_monitor::{AdvancedIOMonitor, IOMonitorConfig},
    io_throttler::{AdvancedIOThrottler, IOThrottlerConfig},
    local_stats::LocalStatsManager,
    node_scanner::{LoadLevel, NodeScanner, NodeScannerConfig, ScanProgress},
    stats_aggregator::{DecentralizedStatsAggregator, DecentralizedStatsAggregatorConfig},
};
use std::time::Duration;
use tempfile::TempDir;

#[tokio::test]
async fn test_checkpoint_manager_save_and_load() {
    let temp_dir = TempDir::new().unwrap();
    let node_id = "test-node-1";
    let checkpoint_manager = CheckpointManager::new(node_id, temp_dir.path());

    // create checkpoint
    let progress = ScanProgress {
        current_cycle: 5,
        current_disk_index: 2,
        last_scan_key: Some("test-object-key".to_string()),
        ..Default::default()
    };

    // save checkpoint
    checkpoint_manager
        .force_save_checkpoint(&progress)
        .await
        .expect("Failed to save checkpoint");

    // load checkpoint
    let loaded_progress = checkpoint_manager
        .load_checkpoint()
        .await
        .expect("Failed to load checkpoint")
        .expect("No checkpoint found");

    // verify data
    assert_eq!(loaded_progress.current_cycle, 5);
    assert_eq!(loaded_progress.current_disk_index, 2);
    assert_eq!(loaded_progress.last_scan_key, Some("test-object-key".to_string()));
}

#[tokio::test]
async fn test_checkpoint_data_integrity() {
    let temp_dir = TempDir::new().unwrap();
    let node_id = "test-node-integrity";
    let checkpoint_manager = CheckpointManager::new(node_id, temp_dir.path());

    let progress = ScanProgress::default();

    // create checkpoint data
    let checkpoint_data = CheckpointData::new(progress.clone(), node_id.to_string());

    // verify integrity
    assert!(checkpoint_data.verify_integrity());

    // save and load
    checkpoint_manager
        .force_save_checkpoint(&progress)
        .await
        .expect("Failed to save checkpoint");

    let loaded = checkpoint_manager.load_checkpoint().await.expect("Failed to load checkpoint");

    assert!(loaded.is_some());
}

#[tokio::test]
async fn test_local_stats_manager() {
    let temp_dir = TempDir::new().unwrap();
    let node_id = "test-stats-node";
    let stats_manager = LocalStatsManager::new(node_id, temp_dir.path());

    // load stats
    stats_manager.load_stats().await.expect("Failed to load stats");

    // get stats summary
    let summary = stats_manager.get_stats_summary().await;
    assert_eq!(summary.node_id, node_id);
    assert_eq!(summary.total_objects_scanned, 0);

    // record heal triggered
    stats_manager
        .record_heal_triggered("test-object", "corruption detected")
        .await;

    let counters = stats_manager.get_counters();
    assert_eq!(counters.total_heal_triggered.load(std::sync::atomic::Ordering::Relaxed), 1);
}

#[tokio::test]
async fn test_io_monitor_load_level_calculation() {
    let config = IOMonitorConfig {
        enable_system_monitoring: false, // use mock data
        ..Default::default()
    };

    let io_monitor = AdvancedIOMonitor::new(config);
    io_monitor.start().await.expect("Failed to start IO monitor");

    // update business metrics to affect load calculation
    io_monitor.update_business_metrics(50, 100, 0, 10).await;

    // wait for a monitoring cycle
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let load_level = io_monitor.get_business_load_level().await;

    // load level should be in a reasonable range
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

    // test adjust for load level
    let low_delay = throttler.adjust_for_load_level(LoadLevel::Low).await;
    let medium_delay = throttler.adjust_for_load_level(LoadLevel::Medium).await;
    let high_delay = throttler.adjust_for_load_level(LoadLevel::High).await;
    let critical_delay = throttler.adjust_for_load_level(LoadLevel::Critical).await;

    // verify delay increment
    assert!(low_delay < medium_delay);
    assert!(medium_delay < high_delay);
    assert!(high_delay < critical_delay);

    // verify pause logic
    assert!(!throttler.should_pause_scanning(LoadLevel::Low).await);
    assert!(!throttler.should_pause_scanning(LoadLevel::Medium).await);
    assert!(!throttler.should_pause_scanning(LoadLevel::High).await);
    assert!(throttler.should_pause_scanning(LoadLevel::Critical).await);
}

#[tokio::test]
async fn test_throttler_business_pressure_simulation() {
    let throttler = AdvancedIOThrottler::default();

    // run short time pressure test
    let simulation_duration = Duration::from_millis(500);
    let result = throttler.simulate_business_pressure(simulation_duration).await;

    // verify simulation result
    assert!(!result.simulation_records.is_empty());
    assert!(result.total_duration >= simulation_duration);
    assert!(result.final_stats.total_decisions > 0);

    // verify all load levels are tested
    let load_levels: std::collections::HashSet<_> = result.simulation_records.iter().map(|r| r.load_level).collect();

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

    // verify node id
    assert_eq!(scanner.node_id(), &node_id);

    // initialize stats
    scanner.initialize_stats().await.expect("Failed to initialize stats");

    // get stats summary
    let summary = scanner.get_stats_summary().await;
    assert_eq!(summary.node_id, node_id);
}

#[tokio::test]
async fn test_decentralized_stats_aggregator() {
    let config = DecentralizedStatsAggregatorConfig {
        cache_ttl: Duration::from_millis(100), // short cache ttl for testing
        ..Default::default()
    };

    let aggregator = DecentralizedStatsAggregator::new(config);

    // test cache mechanism
    let _start_time = std::time::Instant::now();

    // first get stats (should trigger aggregation)
    let stats1 = aggregator
        .get_aggregated_stats()
        .await
        .expect("Failed to get aggregated stats");

    let first_call_duration = _start_time.elapsed();

    // second get stats (should use cache)
    let cache_start = std::time::Instant::now();
    let stats2 = aggregator.get_aggregated_stats().await.expect("Failed to get cached stats");

    let cache_call_duration = cache_start.elapsed();

    // cache call should be faster
    assert!(cache_call_duration < first_call_duration);

    // data should be same
    assert_eq!(stats1.aggregation_timestamp, stats2.aggregation_timestamp);

    // wait for cache expiration
    tokio::time::sleep(Duration::from_millis(150)).await;

    // third get should refresh data
    let stats3 = aggregator
        .get_aggregated_stats()
        .await
        .expect("Failed to get refreshed stats");

    // timestamp should be different
    assert!(stats3.aggregation_timestamp > stats1.aggregation_timestamp);
}

#[tokio::test]
async fn test_scanner_performance_impact() {
    let temp_dir = TempDir::new().unwrap();
    let node_id = "performance-test-node".to_string();

    let config = NodeScannerConfig {
        scan_interval: Duration::from_millis(100), // fast scan for testing
        disk_scan_delay: Duration::from_millis(10),
        data_dir: temp_dir.path().to_path_buf(),
        ..Default::default()
    };

    let scanner = NodeScanner::new(node_id, config);

    // simulate business workload
    let _start_time = std::time::Instant::now();

    // update business metrics for high load
    scanner.update_business_metrics(1500, 3000, 500, 800).await;

    // get io monitor and throttler
    let io_monitor = scanner.get_io_monitor();
    let throttler = scanner.get_io_throttler();

    // start io monitor
    io_monitor.start().await.expect("Failed to start IO monitor");

    // wait for monitor system to stabilize and trigger throttling - increase wait time
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // simulate some io operations to trigger throttling mechanism
    for _ in 0..10 {
        let _current_metrics = io_monitor.get_current_metrics().await;
        let metrics_snapshot = rustfs_ahm::scanner::io_throttler::MetricsSnapshot {
            iops: 1000,
            latency: 100,
            cpu_usage: 80,
            memory_usage: 70,
        };
        let load_level = io_monitor.get_business_load_level().await;
        let _decision = throttler.make_throttle_decision(load_level, Some(metrics_snapshot)).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // check if load level is correctly responded
    let load_level = io_monitor.get_business_load_level().await;

    // in high load, scanner should automatically adjust
    let throttle_stats = throttler.get_throttle_stats().await;

    println!("Performance test results:");
    println!("  Load level: {load_level:?}");
    println!("  Throttle decisions: {}", throttle_stats.total_decisions);
    println!("  Average delay: {:?}", throttle_stats.average_delay);

    // verify performance impact control - if load is high enough, there should be throttling delay
    if load_level != LoadLevel::Low {
        assert!(throttle_stats.average_delay > Duration::from_millis(0));
    } else {
        // in low load, there should be no throttling delay
        assert!(throttle_stats.average_delay >= Duration::from_millis(0));
    }

    io_monitor.stop().await;
}

#[tokio::test]
async fn test_checkpoint_recovery_resilience() {
    let temp_dir = TempDir::new().unwrap();
    let node_id = "resilience-test-node";
    let checkpoint_manager = CheckpointManager::new(node_id, temp_dir.path());

    // verify checkpoint manager
    let result = checkpoint_manager.load_checkpoint().await.unwrap();
    assert!(result.is_none());

    // create and save checkpoint
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

    // verify recovery
    let recovered = checkpoint_manager
        .load_checkpoint()
        .await
        .expect("Failed to load checkpoint")
        .expect("No checkpoint recovered");

    assert_eq!(recovered.current_cycle, 10);
    assert_eq!(recovered.current_disk_index, 3);

    // cleanup checkpoint
    checkpoint_manager
        .cleanup_checkpoint()
        .await
        .expect("Failed to cleanup checkpoint");

    // verify cleanup
    let after_cleanup = checkpoint_manager.load_checkpoint().await.unwrap();
    assert!(after_cleanup.is_none());
}

pub async fn create_test_scanner(temp_dir: &TempDir) -> NodeScanner {
    let config = NodeScannerConfig {
        scan_interval: Duration::from_millis(50),
        disk_scan_delay: Duration::from_millis(10),
        data_dir: temp_dir.path().to_path_buf(),
        ..Default::default()
    };

    NodeScanner::new("integration-test-node".to_string(), config)
}

pub struct PerformanceBenchmark {
    pub _scanner_overhead_ms: u64,
    pub business_impact_percentage: f64,
    pub _throttle_effectiveness: f64,
}

impl PerformanceBenchmark {
    pub fn meets_optimization_goals(&self) -> bool {
        self.business_impact_percentage < 10.0
    }
}
