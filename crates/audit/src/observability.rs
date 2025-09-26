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

//! Observability and metrics for the audit system
//!
//! This module provides comprehensive observability features including:
//! - Performance metrics (EPS, latency)
//! - Target health monitoring  
//! - Configuration change tracking
//! - Error rate monitoring
//! - Queue depth monitoring

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::info;

/// Metrics collector for audit system observability
#[derive(Debug)]
pub struct AuditMetrics {
    // Performance metrics
    total_events_processed: AtomicU64,
    total_events_failed: AtomicU64,
    total_dispatch_time_ns: AtomicU64,

    // Target metrics
    target_success_count: AtomicU64,
    target_failure_count: AtomicU64,

    // System metrics
    config_reload_count: AtomicU64,
    system_start_count: AtomicU64,

    // Performance tracking
    last_reset_time: Arc<RwLock<Instant>>,
}

impl Default for AuditMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl AuditMetrics {
    /// Creates a new metrics collector
    pub fn new() -> Self {
        Self {
            total_events_processed: AtomicU64::new(0),
            total_events_failed: AtomicU64::new(0),
            total_dispatch_time_ns: AtomicU64::new(0),
            target_success_count: AtomicU64::new(0),
            target_failure_count: AtomicU64::new(0),
            config_reload_count: AtomicU64::new(0),
            system_start_count: AtomicU64::new(0),
            last_reset_time: Arc::new(RwLock::new(Instant::now())),
        }
    }

    /// Records a successful event dispatch
    pub fn record_event_success(&self, dispatch_time: Duration) {
        self.total_events_processed.fetch_add(1, Ordering::Relaxed);
        self.total_dispatch_time_ns
            .fetch_add(dispatch_time.as_nanos() as u64, Ordering::Relaxed);
    }

    /// Records a failed event dispatch
    pub fn record_event_failure(&self, dispatch_time: Duration) {
        self.total_events_failed.fetch_add(1, Ordering::Relaxed);
        self.total_dispatch_time_ns
            .fetch_add(dispatch_time.as_nanos() as u64, Ordering::Relaxed);
    }

    /// Records a successful target operation
    pub fn record_target_success(&self) {
        self.target_success_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a failed target operation
    pub fn record_target_failure(&self) {
        self.target_failure_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a configuration reload
    pub fn record_config_reload(&self) {
        self.config_reload_count.fetch_add(1, Ordering::Relaxed);
        info!("Audit configuration reloaded");
    }

    /// Records a system start
    pub fn record_system_start(&self) {
        self.system_start_count.fetch_add(1, Ordering::Relaxed);
        info!("Audit system started");
    }

    /// Gets the current events per second (EPS)
    pub async fn get_events_per_second(&self) -> f64 {
        let reset_time = *self.last_reset_time.read().await;
        let elapsed = reset_time.elapsed();
        let total_events = self.total_events_processed.load(Ordering::Relaxed) + self.total_events_failed.load(Ordering::Relaxed);

        if elapsed.as_secs_f64() > 0.0 {
            total_events as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        }
    }

    /// Gets the average dispatch latency in milliseconds
    pub fn get_average_latency_ms(&self) -> f64 {
        let total_events = self.total_events_processed.load(Ordering::Relaxed) + self.total_events_failed.load(Ordering::Relaxed);
        let total_time_ns = self.total_dispatch_time_ns.load(Ordering::Relaxed);

        if total_events > 0 {
            (total_time_ns as f64 / total_events as f64) / 1_000_000.0 // Convert ns to ms
        } else {
            0.0
        }
    }

    /// Gets the error rate as a percentage
    pub fn get_error_rate(&self) -> f64 {
        let total_events = self.total_events_processed.load(Ordering::Relaxed) + self.total_events_failed.load(Ordering::Relaxed);
        let failed_events = self.total_events_failed.load(Ordering::Relaxed);

        if total_events > 0 {
            (failed_events as f64 / total_events as f64) * 100.0
        } else {
            0.0
        }
    }

    /// Gets target success rate as a percentage
    pub fn get_target_success_rate(&self) -> f64 {
        let total_ops = self.target_success_count.load(Ordering::Relaxed) + self.target_failure_count.load(Ordering::Relaxed);
        let success_ops = self.target_success_count.load(Ordering::Relaxed);

        if total_ops > 0 {
            (success_ops as f64 / total_ops as f64) * 100.0
        } else {
            100.0 // No operations = 100% success rate
        }
    }

    /// Resets all metrics and timing
    pub async fn reset(&self) {
        self.total_events_processed.store(0, Ordering::Relaxed);
        self.total_events_failed.store(0, Ordering::Relaxed);
        self.total_dispatch_time_ns.store(0, Ordering::Relaxed);
        self.target_success_count.store(0, Ordering::Relaxed);
        self.target_failure_count.store(0, Ordering::Relaxed);
        self.config_reload_count.store(0, Ordering::Relaxed);
        self.system_start_count.store(0, Ordering::Relaxed);

        let mut reset_time = self.last_reset_time.write().await;
        *reset_time = Instant::now();

        info!("Audit metrics reset");
    }

    /// Generates a comprehensive metrics report
    pub async fn generate_report(&self) -> AuditMetricsReport {
        AuditMetricsReport {
            events_per_second: self.get_events_per_second().await,
            average_latency_ms: self.get_average_latency_ms(),
            error_rate_percent: self.get_error_rate(),
            target_success_rate_percent: self.get_target_success_rate(),
            total_events_processed: self.total_events_processed.load(Ordering::Relaxed),
            total_events_failed: self.total_events_failed.load(Ordering::Relaxed),
            config_reload_count: self.config_reload_count.load(Ordering::Relaxed),
            system_start_count: self.system_start_count.load(Ordering::Relaxed),
        }
    }

    /// Validates performance requirements
    pub async fn validate_performance_requirements(&self) -> PerformanceValidation {
        let eps = self.get_events_per_second().await;
        let avg_latency_ms = self.get_average_latency_ms();
        let error_rate = self.get_error_rate();

        let mut validation = PerformanceValidation {
            meets_eps_requirement: eps >= 3000.0,
            meets_latency_requirement: avg_latency_ms <= 30.0,
            meets_error_rate_requirement: error_rate <= 1.0, // Less than 1% error rate
            current_eps: eps,
            current_latency_ms: avg_latency_ms,
            current_error_rate: error_rate,
            recommendations: Vec::new(),
        };

        // Generate recommendations
        if !validation.meets_eps_requirement {
            validation.recommendations.push(format!(
                "EPS ({eps:.0}) is below requirement (3000). Consider optimizing target dispatch or adding more target instances."
            ));
        }

        if !validation.meets_latency_requirement {
            validation.recommendations.push(format!(
                "Average latency ({avg_latency_ms:.2}ms) exceeds requirement (30ms). Consider optimizing target responses or increasing timeout values."
            ));
        }

        if !validation.meets_error_rate_requirement {
            validation.recommendations.push(format!(
                "Error rate ({error_rate:.2}%) exceeds recommendation (1%). Check target connectivity and configuration."
            ));
        }

        if validation.meets_eps_requirement && validation.meets_latency_requirement && validation.meets_error_rate_requirement {
            validation
                .recommendations
                .push("All performance requirements are met.".to_string());
        }

        validation
    }
}

/// Comprehensive metrics report
#[derive(Debug, Clone)]
pub struct AuditMetricsReport {
    pub events_per_second: f64,
    pub average_latency_ms: f64,
    pub error_rate_percent: f64,
    pub target_success_rate_percent: f64,
    pub total_events_processed: u64,
    pub total_events_failed: u64,
    pub config_reload_count: u64,
    pub system_start_count: u64,
}

impl AuditMetricsReport {
    /// Formats the report as a human-readable string
    pub fn format(&self) -> String {
        format!(
            "Audit System Metrics Report:\n\
             Events per Second: {:.2}\n\
             Average Latency: {:.2}ms\n\
             Error Rate: {:.2}%\n\
             Target Success Rate: {:.2}%\n\
             Total Events Processed: {}\n\
             Total Events Failed: {}\n\
             Configuration Reloads: {}\n\
             System Starts: {}",
            self.events_per_second,
            self.average_latency_ms,
            self.error_rate_percent,
            self.target_success_rate_percent,
            self.total_events_processed,
            self.total_events_failed,
            self.config_reload_count,
            self.system_start_count
        )
    }
}

/// Performance validation results
#[derive(Debug, Clone)]
pub struct PerformanceValidation {
    pub meets_eps_requirement: bool,
    pub meets_latency_requirement: bool,
    pub meets_error_rate_requirement: bool,
    pub current_eps: f64,
    pub current_latency_ms: f64,
    pub current_error_rate: f64,
    pub recommendations: Vec<String>,
}

impl PerformanceValidation {
    /// Checks if all performance requirements are met
    pub fn all_requirements_met(&self) -> bool {
        self.meets_eps_requirement && self.meets_latency_requirement && self.meets_error_rate_requirement
    }

    /// Formats the validation as a human-readable string
    pub fn format(&self) -> String {
        let status = if self.all_requirements_met() { "✅ PASS" } else { "❌ FAIL" };

        let mut result = format!(
            "Performance Requirements Validation: {}\n\
             EPS Requirement (≥3000): {} ({:.2})\n\
             Latency Requirement (≤30ms): {} ({:.2}ms)\n\
             Error Rate Requirement (≤1%): {} ({:.2}%)\n\
             \nRecommendations:",
            status,
            if self.meets_eps_requirement { "✅" } else { "❌" },
            self.current_eps,
            if self.meets_latency_requirement { "✅" } else { "❌" },
            self.current_latency_ms,
            if self.meets_error_rate_requirement { "✅" } else { "❌" },
            self.current_error_rate
        );

        for rec in &self.recommendations {
            result.push_str(&format!("\n• {rec}"));
        }

        result
    }
}

/// Global metrics instance
static GLOBAL_METRICS: once_cell::sync::OnceCell<Arc<AuditMetrics>> = once_cell::sync::OnceCell::new();

/// Get or initialize the global metrics instance
pub fn global_metrics() -> Arc<AuditMetrics> {
    GLOBAL_METRICS.get_or_init(|| Arc::new(AuditMetrics::new())).clone()
}

/// Record a successful audit event dispatch
pub fn record_audit_success(dispatch_time: Duration) {
    global_metrics().record_event_success(dispatch_time);
}

/// Record a failed audit event dispatch
pub fn record_audit_failure(dispatch_time: Duration) {
    global_metrics().record_event_failure(dispatch_time);
}

/// Record a successful target operation
pub fn record_target_success() {
    global_metrics().record_target_success();
}

/// Record a failed target operation
pub fn record_target_failure() {
    global_metrics().record_target_failure();
}

/// Record a configuration reload
pub fn record_config_reload() {
    global_metrics().record_config_reload();
}

/// Record a system start
pub fn record_system_start() {
    global_metrics().record_system_start();
}

/// Get the current metrics report
pub async fn get_metrics_report() -> AuditMetricsReport {
    global_metrics().generate_report().await
}

/// Validate performance requirements
pub async fn validate_performance() -> PerformanceValidation {
    global_metrics().validate_performance_requirements().await
}

/// Reset all metrics
pub async fn reset_metrics() {
    global_metrics().reset().await;
}
