//  Copyright 2024 RustFS Team
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

//! Tests for audit system observability and metrics

use rustfs_audit::observability::*;
use std::time::Duration;

#[tokio::test]
async fn test_metrics_collection() {
    let metrics = AuditMetrics::new();

    // Initially all metrics should be zero
    let report = metrics.generate_report().await;
    assert_eq!(report.total_events_processed, 0);
    assert_eq!(report.total_events_failed, 0);
    assert_eq!(report.events_per_second, 0.0);

    // Record some events
    metrics.record_event_success(Duration::from_millis(10));
    metrics.record_event_success(Duration::from_millis(20));
    metrics.record_event_failure(Duration::from_millis(30));

    // Check updated metrics
    let report = metrics.generate_report().await;
    assert_eq!(report.total_events_processed, 2);
    assert_eq!(report.total_events_failed, 1);
    assert_eq!(report.error_rate_percent, 33.33333333333333); // 1/3 * 100
    assert_eq!(report.average_latency_ms, 20.0); // (10+20+30)/3
}

#[tokio::test]
async fn test_target_metrics() {
    let metrics = AuditMetrics::new();

    // Record target operations
    metrics.record_target_success();
    metrics.record_target_success();
    metrics.record_target_failure();

    let success_rate = metrics.get_target_success_rate();
    assert_eq!(success_rate, 66.66666666666666); // 2/3 * 100
}

#[tokio::test]
async fn test_performance_validation_pass() {
    let metrics = AuditMetrics::new();

    // Simulate high EPS with low latency
    for _ in 0..5000 {
        metrics.record_event_success(Duration::from_millis(5));
    }

    // Small delay to make EPS calculation meaningful
    tokio::time::sleep(Duration::from_millis(1)).await;

    let validation = metrics.validate_performance_requirements().await;

    // Should meet latency requirement
    assert!(validation.meets_latency_requirement, "Latency requirement should be met");
    assert!(validation.current_latency_ms <= 30.0);

    // Should meet error rate requirement (no failures)
    assert!(validation.meets_error_rate_requirement, "Error rate requirement should be met");
    assert_eq!(validation.current_error_rate, 0.0);
}

#[tokio::test]
async fn test_performance_validation_fail() {
    let metrics = AuditMetrics::new();

    // Simulate high latency
    metrics.record_event_success(Duration::from_millis(50)); // Above 30ms requirement
    metrics.record_event_failure(Duration::from_millis(60));

    let validation = metrics.validate_performance_requirements().await;

    // Should fail latency requirement
    assert!(!validation.meets_latency_requirement, "Latency requirement should fail");
    assert!(validation.current_latency_ms > 30.0);

    // Should fail error rate requirement
    assert!(!validation.meets_error_rate_requirement, "Error rate requirement should fail");
    assert!(validation.current_error_rate > 1.0);

    // Should have recommendations
    assert!(!validation.recommendations.is_empty());
}

#[tokio::test]
async fn test_global_metrics() {
    // Test global metrics functions
    record_audit_success(Duration::from_millis(10));
    record_audit_failure(Duration::from_millis(20));
    record_target_success();
    record_target_failure();
    record_config_reload();
    record_system_start();

    let report = get_metrics_report().await;
    assert!(report.total_events_processed > 0);
    assert!(report.total_events_failed > 0);
    assert!(report.config_reload_count > 0);
    assert!(report.system_start_count > 0);

    // Reset metrics
    reset_metrics().await;

    let report_after_reset = get_metrics_report().await;
    assert_eq!(report_after_reset.total_events_processed, 0);
    assert_eq!(report_after_reset.total_events_failed, 0);
}

#[test]
fn test_metrics_report_formatting() {
    let report = AuditMetricsReport {
        events_per_second: 1500.5,
        average_latency_ms: 25.75,
        error_rate_percent: 0.5,
        target_success_rate_percent: 99.5,
        total_events_processed: 10000,
        total_events_failed: 50,
        config_reload_count: 3,
        system_start_count: 1,
    };

    let formatted = report.format();
    assert!(formatted.contains("1500.50")); // EPS
    assert!(formatted.contains("25.75")); // Latency
    assert!(formatted.contains("0.50")); // Error rate
    assert!(formatted.contains("99.50")); // Success rate
    assert!(formatted.contains("10000")); // Events processed
    assert!(formatted.contains("50")); // Events failed
}

#[test]
fn test_performance_validation_formatting() {
    let validation = PerformanceValidation {
        meets_eps_requirement: false,
        meets_latency_requirement: true,
        meets_error_rate_requirement: true,
        current_eps: 2500.0,
        current_latency_ms: 15.0,
        current_error_rate: 0.1,
        recommendations: vec![
            "EPS too low, consider optimization".to_string(),
            "Latency is good".to_string(),
        ],
    };

    let formatted = validation.format();
    assert!(formatted.contains("❌ FAIL")); // Should show fail
    assert!(formatted.contains("2500.00")); // Current EPS
    assert!(formatted.contains("15.00")); // Current latency  
    assert!(formatted.contains("0.10")); // Current error rate
    assert!(formatted.contains("EPS too low")); // Recommendation
    assert!(formatted.contains("Latency is good")); // Recommendation
}

#[test]
fn test_performance_validation_all_pass() {
    let validation = PerformanceValidation {
        meets_eps_requirement: true,
        meets_latency_requirement: true,
        meets_error_rate_requirement: true,
        current_eps: 5000.0,
        current_latency_ms: 10.0,
        current_error_rate: 0.01,
        recommendations: vec!["All requirements met".to_string()],
    };

    assert!(validation.all_requirements_met());

    let formatted = validation.format();
    assert!(formatted.contains("✅ PASS")); // Should show pass
    assert!(formatted.contains("All requirements met"));
}

#[tokio::test]
async fn test_eps_calculation() {
    let metrics = AuditMetrics::new();

    // Record events
    for _ in 0..100 {
        metrics.record_event_success(Duration::from_millis(1));
    }

    // Small delay to allow EPS calculation
    tokio::time::sleep(Duration::from_millis(10)).await;

    let eps = metrics.get_events_per_second().await;

    // Should have some EPS value > 0
    assert!(eps > 0.0, "EPS should be greater than 0");

    // EPS should be reasonable (events / time)
    // With 100 events in ~10ms, should be very high
    assert!(eps > 1000.0, "EPS should be high for short time period");
}

#[test]
fn test_error_rate_calculation() {
    let metrics = AuditMetrics::new();

    // No events - should be 0% error rate
    assert_eq!(metrics.get_error_rate(), 0.0);

    // Record 7 successes, 3 failures = 30% error rate
    for _ in 0..7 {
        metrics.record_event_success(Duration::from_millis(1));
    }
    for _ in 0..3 {
        metrics.record_event_failure(Duration::from_millis(1));
    }

    let error_rate = metrics.get_error_rate();
    assert_eq!(error_rate, 30.0);
}

#[test]
fn test_target_success_rate_calculation() {
    let metrics = AuditMetrics::new();

    // No operations - should be 100% success rate
    assert_eq!(metrics.get_target_success_rate(), 100.0);

    // Record 8 successes, 2 failures = 80% success rate
    for _ in 0..8 {
        metrics.record_target_success();
    }
    for _ in 0..2 {
        metrics.record_target_failure();
    }

    let success_rate = metrics.get_target_success_rate();
    assert_eq!(success_rate, 80.0);
}

#[tokio::test]
async fn test_metrics_reset() {
    let metrics = AuditMetrics::new();

    // Record some data
    metrics.record_event_success(Duration::from_millis(10));
    metrics.record_target_success();
    metrics.record_config_reload();
    metrics.record_system_start();

    // Verify data exists
    let report_before = metrics.generate_report().await;
    assert!(report_before.total_events_processed > 0);
    assert!(report_before.config_reload_count > 0);
    assert!(report_before.system_start_count > 0);

    // Reset
    metrics.reset().await;

    // Verify data is reset
    let report_after = metrics.generate_report().await;
    assert_eq!(report_after.total_events_processed, 0);
    assert_eq!(report_after.total_events_failed, 0);
    // Note: config_reload_count and system_start_count are reset to 0 as well
    assert_eq!(report_after.config_reload_count, 0);
    assert_eq!(report_after.system_start_count, 0);
}
