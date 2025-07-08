// Copyright 2024 RustFS Team

use std::{
    collections::HashMap,
    fmt,
    sync::Arc,
    time::{Duration, SystemTime},
};

use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use serde::{Serialize, Deserialize};

use crate::error::Result;

use super::{
    AggregatedMetrics, MetricsQuery, MetricsSummary, SystemMetrics,
};

/// Configuration for the metrics reporter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReporterConfig {
    /// Whether to enable reporting
    pub enabled: bool,
    /// Report generation interval
    pub report_interval: Duration,
    /// Maximum number of reports to keep in memory
    pub max_reports_in_memory: usize,
    /// Alert thresholds
    pub alert_thresholds: AlertThresholds,
    /// Report output format
    pub default_format: ReportFormat,
    /// Whether to enable alerting
    pub enable_alerts: bool,
    /// Maximum number of alerts to keep in memory
    pub max_alerts_in_memory: usize,
    /// Report output directory
    pub output_directory: Option<String>,
    /// Whether to enable HTTP reporting
    pub enable_http_reporting: bool,
    /// HTTP reporting endpoint
    pub http_endpoint: Option<String>,
}

impl Default for ReporterConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            report_interval: Duration::from_secs(60), // 1 minute
            max_reports_in_memory: 1000,
            alert_thresholds: AlertThresholds::default(),
            default_format: ReportFormat::Json,
            enable_alerts: true,
            max_alerts_in_memory: 1000,
            output_directory: None,
            enable_http_reporting: false,
            http_endpoint: None,
        }
    }
}

/// Alert thresholds for metrics reporting
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertThresholds {
    /// CPU usage threshold (percentage)
    pub cpu_usage_threshold: f64,
    /// Memory usage threshold (percentage)
    pub memory_usage_threshold: f64,
    /// Disk usage threshold (percentage)
    pub disk_usage_threshold: f64,
    /// System load threshold
    pub system_load_threshold: f64,
    /// Repair failure rate threshold (percentage)
    pub repair_failure_rate_threshold: f64,
    /// Health issues threshold (count)
    pub health_issues_threshold: u64,
}

impl Default for AlertThresholds {
    fn default() -> Self {
        Self {
            cpu_usage_threshold: 80.0,
            memory_usage_threshold: 85.0,
            disk_usage_threshold: 90.0,
            system_load_threshold: 5.0,
            repair_failure_rate_threshold: 20.0,
            health_issues_threshold: 10,
        }
    }
}

/// Metrics reporter that generates and outputs metrics reports
pub struct Reporter {
    config: ReporterConfig,
    reports: Arc<RwLock<Vec<MetricsReport>>>,
    alerts: Arc<RwLock<Vec<Alert>>>,
    last_report_time: Arc<RwLock<SystemTime>>,
    report_count: Arc<RwLock<u64>>,
    alert_count: Arc<RwLock<u64>>,
}

impl Reporter {
    /// Create a new metrics reporter
    pub async fn new(config: ReporterConfig) -> Result<Self> {
        Ok(Self {
            config,
            reports: Arc::new(RwLock::new(Vec::new())),
            alerts: Arc::new(RwLock::new(Vec::new())),
            last_report_time: Arc::new(RwLock::new(SystemTime::now())),
            report_count: Arc::new(RwLock::new(0)),
            alert_count: Arc::new(RwLock::new(0)),
        })
    }

    /// Get the configuration
    pub fn config(&self) -> &ReporterConfig {
        &self.config
    }

    /// Generate a metrics report
    pub async fn generate_report(&self, metrics: &SystemMetrics) -> Result<MetricsReport> {
        let start_time = SystemTime::now();
        
        let report = MetricsReport {
            timestamp: start_time,
            metrics: metrics.clone(),
            alerts: self.check_alerts(metrics).await?,
            summary: self.generate_summary(metrics).await?,
            format: self.config.default_format,
        };

        // Store report
        {
            let mut reports = self.reports.write().await;
            reports.push(report.clone());
            
            // Trim old reports if we exceed the limit
            if reports.len() > self.config.max_reports_in_memory {
                let excess = reports.len() - self.config.max_reports_in_memory;
                reports.drain(0..excess);
            }
        }

        // Update statistics
        {
            let mut last_time = self.last_report_time.write().await;
            *last_time = start_time;
            
            let mut count = self.report_count.write().await;
            *count += 1;
        }

        info!("Generated metrics report #{}", *self.report_count.read().await);
        Ok(report)
    }

    /// Generate a comprehensive report from aggregated metrics
    pub async fn generate_comprehensive_report(&self, aggregated: &AggregatedMetrics) -> Result<ComprehensiveReport> {
        let start_time = SystemTime::now();
        
        let report = ComprehensiveReport {
            timestamp: start_time,
            query: aggregated.query.clone(),
            data_points: aggregated.data_points.len(),
            summary: aggregated.summary.clone(),
            alerts: self.check_aggregated_alerts(aggregated).await?,
            trends: self.analyze_trends(aggregated).await?,
            recommendations: self.generate_recommendations(aggregated).await?,
        };

        info!("Generated comprehensive report with {} data points", report.data_points);
        Ok(report)
    }

    /// Output a report in the specified format
    pub async fn output_report(&self, report: &MetricsReport, format: ReportFormat) -> Result<()> {
        match format {
            ReportFormat::Console => self.output_to_console(report).await?,
            ReportFormat::File => self.output_to_file(report).await?,
            ReportFormat::Http => self.output_to_http(report).await?,
            ReportFormat::Prometheus => self.output_prometheus(report).await?,
            ReportFormat::Json => self.output_json(report).await?,
            ReportFormat::Csv => self.output_csv(report).await?,
        }

        Ok(())
    }

    /// Check for alerts based on metrics
    async fn check_alerts(&self, metrics: &SystemMetrics) -> Result<Vec<Alert>> {
        let mut alerts = Vec::new();

        // Check CPU usage
        if metrics.cpu_usage > self.config.alert_thresholds.cpu_usage_threshold {
            alerts.push(Alert {
                timestamp: SystemTime::now(),
                severity: AlertSeverity::Warning,
                category: AlertCategory::System,
                message: format!("High CPU usage: {:.1}%", metrics.cpu_usage),
                metric_value: metrics.cpu_usage,
                threshold: self.config.alert_thresholds.cpu_usage_threshold,
            });
        }

        // Check memory usage
        if metrics.memory_usage > self.config.alert_thresholds.memory_usage_threshold {
            alerts.push(Alert {
                timestamp: SystemTime::now(),
                severity: AlertSeverity::Warning,
                category: AlertCategory::System,
                message: format!("High memory usage: {:.1}%", metrics.memory_usage),
                metric_value: metrics.memory_usage,
                threshold: self.config.alert_thresholds.memory_usage_threshold,
            });
        }

        // Check disk usage
        if metrics.disk_usage > self.config.alert_thresholds.disk_usage_threshold {
            alerts.push(Alert {
                timestamp: SystemTime::now(),
                severity: AlertSeverity::Critical,
                category: AlertCategory::System,
                message: format!("High disk usage: {:.1}%", metrics.disk_usage),
                metric_value: metrics.disk_usage,
                threshold: self.config.alert_thresholds.disk_usage_threshold,
            });
        }

        // Check system load
        if metrics.system_load > self.config.alert_thresholds.system_load_threshold {
            alerts.push(Alert {
                timestamp: SystemTime::now(),
                severity: AlertSeverity::Warning,
                category: AlertCategory::System,
                message: format!("High system load: {:.2}", metrics.system_load),
                metric_value: metrics.system_load,
                threshold: self.config.alert_thresholds.system_load_threshold,
            });
        }

        // Check repair failure rate
        if metrics.heal_metrics.total_repairs > 0 {
            let failure_rate = (metrics.heal_metrics.failed_repairs as f64 / metrics.heal_metrics.total_repairs as f64) * 100.0;
            if failure_rate > self.config.alert_thresholds.repair_failure_rate_threshold {
                alerts.push(Alert {
                    timestamp: SystemTime::now(),
                    severity: AlertSeverity::Critical,
                    category: AlertCategory::Heal,
                    message: format!("High repair failure rate: {:.1}%", failure_rate),
                    metric_value: failure_rate,
                    threshold: self.config.alert_thresholds.repair_failure_rate_threshold,
                });
            }
        }

        // Check health issues
        let total_health_issues: u64 = metrics.health_issues.values().sum();
        if total_health_issues > self.config.alert_thresholds.health_issues_threshold {
            alerts.push(Alert {
                timestamp: SystemTime::now(),
                severity: AlertSeverity::Warning,
                category: AlertCategory::Health,
                message: format!("High number of health issues: {}", total_health_issues),
                metric_value: total_health_issues as f64,
                threshold: self.config.alert_thresholds.health_issues_threshold as f64,
            });
        }

        // Store alerts
        if !alerts.is_empty() {
            let mut alert_store = self.alerts.write().await;
            alert_store.extend(alerts.clone());
            
            let mut count = self.alert_count.write().await;
            *count += alerts.len() as u64;
        }

        Ok(alerts)
    }

    /// Check for alerts based on aggregated metrics
    async fn check_aggregated_alerts(&self, aggregated: &AggregatedMetrics) -> Result<Vec<Alert>> {
        let mut alerts = Vec::new();

        // Check summary statistics
        if aggregated.summary.avg_cpu_usage > self.config.alert_thresholds.cpu_usage_threshold {
            alerts.push(Alert {
                timestamp: SystemTime::now(),
                severity: AlertSeverity::Warning,
                category: AlertCategory::System,
                message: format!("High average CPU usage: {:.1}%", aggregated.summary.avg_cpu_usage),
                metric_value: aggregated.summary.avg_cpu_usage,
                threshold: self.config.alert_thresholds.cpu_usage_threshold,
            });
        }

        if aggregated.summary.repair_success_rate < (100.0 - self.config.alert_thresholds.repair_failure_rate_threshold) {
            alerts.push(Alert {
                timestamp: SystemTime::now(),
                severity: AlertSeverity::Critical,
                category: AlertCategory::Heal,
                message: format!("Low repair success rate: {:.1}%", aggregated.summary.repair_success_rate * 100.0),
                metric_value: aggregated.summary.repair_success_rate * 100.0,
                threshold: 100.0 - self.config.alert_thresholds.repair_failure_rate_threshold,
            });
        }

        Ok(alerts)
    }

    /// Generate summary for metrics
    async fn generate_summary(&self, metrics: &SystemMetrics) -> Result<ReportSummary> {
        Ok(ReportSummary {
            system_health: self.calculate_system_health(metrics),
            performance_score: self.calculate_performance_score(metrics),
            resource_utilization: self.calculate_resource_utilization(metrics),
            operational_status: self.determine_operational_status(metrics),
            key_metrics: self.extract_key_metrics(metrics),
        })
    }

    /// Analyze trends in aggregated data
    async fn analyze_trends(&self, aggregated: &AggregatedMetrics) -> Result<Vec<TrendAnalysis>> {
        let mut trends = Vec::new();

        if aggregated.data_points.len() < 2 {
            return Ok(trends);
        }

        // Analyze CPU usage trend
        let cpu_values: Vec<f64> = aggregated
            .data_points
            .iter()
            .filter_map(|p| p.system.as_ref().map(|s| s.cpu_usage))
            .collect();

        if cpu_values.len() >= 2 {
            let trend = self.calculate_trend(&cpu_values, "CPU Usage");
            trends.push(trend);
        }

        // Analyze memory usage trend
        let memory_values: Vec<f64> = aggregated
            .data_points
            .iter()
            .filter_map(|p| p.system.as_ref().map(|s| s.memory_usage))
            .collect();

        if memory_values.len() >= 2 {
            let trend = self.calculate_trend(&memory_values, "Memory Usage");
            trends.push(trend);
        }

        Ok(trends)
    }

    /// Generate recommendations based on metrics
    async fn generate_recommendations(&self, aggregated: &AggregatedMetrics) -> Result<Vec<Recommendation>> {
        let mut recommendations = Vec::new();

        // Check for high resource usage
        if aggregated.summary.avg_cpu_usage > 70.0 {
            recommendations.push(Recommendation {
                priority: RecommendationPriority::High,
                category: RecommendationCategory::Performance,
                title: "High CPU Usage".to_string(),
                description: "Consider scaling up CPU resources or optimizing workload distribution".to_string(),
                action: "Monitor CPU usage patterns and consider resource allocation adjustments".to_string(),
            });
        }

        if aggregated.summary.avg_memory_usage > 80.0 {
            recommendations.push(Recommendation {
                priority: RecommendationPriority::High,
                category: RecommendationCategory::Performance,
                title: "High Memory Usage".to_string(),
                description: "Memory usage is approaching critical levels".to_string(),
                action: "Consider increasing memory allocation or optimizing memory usage".to_string(),
            });
        }

        // Check for repair issues
        if aggregated.summary.repair_success_rate < 0.8 {
            recommendations.push(Recommendation {
                priority: RecommendationPriority::Critical,
                category: RecommendationCategory::Reliability,
                title: "Low Repair Success Rate".to_string(),
                description: "Data repair operations are failing frequently".to_string(),
                action: "Investigate repair failures and check system health".to_string(),
            });
        }

        Ok(recommendations)
    }

    /// Calculate trend for a series of values
    fn calculate_trend(&self, values: &[f64], metric_name: &str) -> TrendAnalysis {
        if values.len() < 2 {
            return TrendAnalysis {
                metric_name: metric_name.to_string(),
                trend_direction: TrendDirection::Stable,
                change_rate: 0.0,
                confidence: 0.0,
            };
        }

        let first = values[0];
        let last = values[values.len() - 1];
        let change_rate = ((last - first) / first) * 100.0;

        let trend_direction = if change_rate > 5.0 {
            TrendDirection::Increasing
        } else if change_rate < -5.0 {
            TrendDirection::Decreasing
        } else {
            TrendDirection::Stable
        };

        // Simple confidence calculation based on data points
        let confidence = (values.len() as f64 / 10.0).min(1.0);

        TrendAnalysis {
            metric_name: metric_name.to_string(),
            trend_direction,
            change_rate,
            confidence,
        }
    }

    /// Calculate system health score
    fn calculate_system_health(&self, metrics: &SystemMetrics) -> f64 {
        let mut score = 100.0;

        // Deduct points for high resource usage
        if metrics.cpu_usage > 80.0 {
            score -= (metrics.cpu_usage - 80.0) * 0.5;
        }
        if metrics.memory_usage > 85.0 {
            score -= (metrics.memory_usage - 85.0) * 0.5;
        }
        if metrics.disk_usage > 90.0 {
            score -= (metrics.disk_usage - 90.0) * 1.0;
        }

        // Deduct points for health issues
        let total_health_issues: u64 = metrics.health_issues.values().sum();
        score -= total_health_issues as f64 * 5.0;

        // Deduct points for repair failures
        if metrics.heal_metrics.total_repairs > 0 {
            let failure_rate = metrics.heal_metrics.failed_repairs as f64 / metrics.heal_metrics.total_repairs as f64;
            score -= failure_rate * 20.0;
        }

        score.max(0.0)
    }

    /// Calculate performance score
    fn calculate_performance_score(&self, metrics: &SystemMetrics) -> f64 {
        let mut score = 100.0;

        // Base score on resource efficiency
        score -= metrics.cpu_usage * 0.3;
        score -= metrics.memory_usage * 0.3;
        score -= metrics.disk_usage * 0.2;
        score -= metrics.system_load * 10.0;

        score.max(0.0)
    }

    /// Calculate resource utilization
    fn calculate_resource_utilization(&self, metrics: &SystemMetrics) -> f64 {
        (metrics.cpu_usage + metrics.memory_usage + metrics.disk_usage) / 3.0
    }

    /// Determine operational status
    fn determine_operational_status(&self, metrics: &SystemMetrics) -> OperationalStatus {
        let health_score = self.calculate_system_health(metrics);
        
        if health_score >= 90.0 {
            OperationalStatus::Excellent
        } else if health_score >= 75.0 {
            OperationalStatus::Good
        } else if health_score >= 50.0 {
            OperationalStatus::Fair
        } else {
            OperationalStatus::Poor
        }
    }

    /// Extract key metrics
    fn extract_key_metrics(&self, metrics: &SystemMetrics) -> HashMap<String, f64> {
        let mut key_metrics = HashMap::new();
        key_metrics.insert("cpu_usage".to_string(), metrics.cpu_usage);
        key_metrics.insert("memory_usage".to_string(), metrics.memory_usage);
        key_metrics.insert("disk_usage".to_string(), metrics.disk_usage);
        key_metrics.insert("system_load".to_string(), metrics.system_load);
        key_metrics.insert("active_operations".to_string(), metrics.active_operations as f64);
        key_metrics.insert("objects_scanned".to_string(), metrics.scan_metrics.objects_scanned as f64);
        key_metrics.insert("total_repairs".to_string(), metrics.heal_metrics.total_repairs as f64);
        key_metrics.insert("successful_repairs".to_string(), metrics.heal_metrics.successful_repairs as f64);
        
        key_metrics
    }

    /// Output methods (simulated)
    async fn output_to_console(&self, report: &MetricsReport) -> Result<()> {
        if self.config.enabled {
            info!("=== Metrics Report ===");
            info!("Timestamp: {:?}", report.timestamp);
            info!("System Health: {:.1}%", report.summary.system_health);
            info!("Performance Score: {:.1}%", report.summary.performance_score);
            info!("Operational Status: {:?}", report.summary.operational_status);
            
            if !report.alerts.is_empty() {
                info!("=== Alerts ===");
                for alert in &report.alerts {
                    info!("[{}] {}: {}", alert.severity, alert.category, alert.message);
                }
            }
        }
        Ok(())
    }

    async fn output_to_file(&self, _report: &MetricsReport) -> Result<()> {
        if self.config.enabled {
            // In a real implementation, this would write to a file
            debug!("Would write report to file: {}", self.config.output_directory.as_ref().unwrap_or(&String::new()));
        }
        Ok(())
    }

    async fn output_to_http(&self, _report: &MetricsReport) -> Result<()> {
        if self.config.enable_http_reporting {
            // In a real implementation, this would serve via HTTP
            debug!("Would serve report via HTTP on endpoint: {}", self.config.http_endpoint.as_ref().unwrap_or(&String::new()));
        }
        Ok(())
    }

    async fn output_prometheus(&self, _report: &MetricsReport) -> Result<()> {
        if self.config.enabled {
            // In a real implementation, this would output Prometheus format
            debug!("Would output Prometheus format");
        }
        Ok(())
    }

    async fn output_json(&self, _report: &MetricsReport) -> Result<()> {
        if self.config.enabled {
            // In a real implementation, this would output JSON format
            debug!("Would output JSON format");
        }
        Ok(())
    }

    async fn output_csv(&self, _report: &MetricsReport) -> Result<()> {
        if self.config.enabled {
            // In a real implementation, this would output CSV format
            debug!("Would output CSV format");
        }
        Ok(())
    }

    /// Get reporting statistics
    pub async fn get_statistics(&self) -> ReporterStatistics {
        let report_count = *self.report_count.read().await;
        let alert_count = *self.alert_count.read().await;
        let last_report_time = *self.last_report_time.read().await;
        let reports_count = self.reports.read().await.len();
        let alerts_count = self.alerts.read().await.len();

        ReporterStatistics {
            total_reports: report_count,
            total_alerts: alert_count,
            reports_in_memory: reports_count,
            alerts_in_memory: alerts_count,
            last_report_time,
            config: self.config.clone(),
        }
    }

    /// Get recent alerts
    pub async fn get_recent_alerts(&self, hours: u64) -> Result<Vec<Alert>> {
        let cutoff_time = SystemTime::now() - Duration::from_secs(hours * 3600);
        let alerts = self.alerts.read().await;
        
        let recent_alerts: Vec<Alert> = alerts
            .iter()
            .filter(|alert| alert.timestamp >= cutoff_time)
            .cloned()
            .collect();

        Ok(recent_alerts)
    }

    /// Clear old alerts
    pub async fn clear_old_alerts(&self, hours: u64) -> Result<()> {
        let cutoff_time = SystemTime::now() - Duration::from_secs(hours * 3600);
        let mut alerts = self.alerts.write().await;
        alerts.retain(|alert| alert.timestamp >= cutoff_time);
        
        info!("Cleared alerts older than {} hours", hours);
        Ok(())
    }
}

/// Metrics report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsReport {
    pub timestamp: SystemTime,
    pub metrics: SystemMetrics,
    pub alerts: Vec<Alert>,
    pub summary: ReportSummary,
    pub format: ReportFormat,
}

/// Comprehensive report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComprehensiveReport {
    pub timestamp: SystemTime,
    pub query: MetricsQuery,
    pub data_points: usize,
    pub summary: MetricsSummary,
    pub alerts: Vec<Alert>,
    pub trends: Vec<TrendAnalysis>,
    pub recommendations: Vec<Recommendation>,
}

/// Report summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportSummary {
    pub system_health: f64,
    pub performance_score: f64,
    pub resource_utilization: f64,
    pub operational_status: OperationalStatus,
    pub key_metrics: HashMap<String, f64>,
}

/// Alert
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub timestamp: SystemTime,
    pub severity: AlertSeverity,
    pub category: AlertCategory,
    pub message: String,
    pub metric_value: f64,
    pub threshold: f64,
}

/// Alert severity
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
}

impl fmt::Display for AlertSeverity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlertSeverity::Info => write!(f, "INFO"),
            AlertSeverity::Warning => write!(f, "WARNING"),
            AlertSeverity::Critical => write!(f, "CRITICAL"),
        }
    }
}

/// Alert category
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlertCategory {
    System,
    Performance,
    Health,
    Heal,
    Security,
}

impl fmt::Display for AlertCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlertCategory::System => write!(f, "SYSTEM"),
            AlertCategory::Performance => write!(f, "PERFORMANCE"),
            AlertCategory::Health => write!(f, "HEALTH"),
            AlertCategory::Heal => write!(f, "HEAL"),
            AlertCategory::Security => write!(f, "SECURITY"),
        }
    }
}

/// Report format
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReportFormat {
    Console,
    File,
    Http,
    Prometheus,
    Json,
    Csv,
}

/// Operational status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationalStatus {
    Excellent,
    Good,
    Fair,
    Poor,
}

/// Trend analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendAnalysis {
    pub metric_name: String,
    pub trend_direction: TrendDirection,
    pub change_rate: f64,
    pub confidence: f64,
}

/// Trend direction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TrendDirection {
    Increasing,
    Decreasing,
    Stable,
}

/// Recommendation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Recommendation {
    pub priority: RecommendationPriority,
    pub category: RecommendationCategory,
    pub title: String,
    pub description: String,
    pub action: String,
}

/// Recommendation priority
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecommendationPriority {
    Low,
    Medium,
    High,
    Critical,
}

/// Recommendation category
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecommendationCategory {
    Performance,
    Reliability,
    Security,
    Maintenance,
}

/// Reporter statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReporterStatistics {
    pub total_reports: u64,
    pub total_alerts: u64,
    pub reports_in_memory: usize,
    pub alerts_in_memory: usize,
    pub last_report_time: SystemTime,
    pub config: ReporterConfig,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_reporter_creation() {
        let config = ReporterConfig::default();
        let reporter = Reporter::new(config).await.unwrap();
        
        assert_eq!(reporter.config().report_interval, Duration::from_secs(60));
        assert!(reporter.config().enabled);
    }

    #[tokio::test]
    async fn test_report_generation() {
        let config = ReporterConfig::default();
        let reporter = Reporter::new(config).await.unwrap();
        
        let metrics = SystemMetrics::default();
        let report = reporter.generate_report(&metrics).await.unwrap();
        
        assert_eq!(report.metrics.cpu_usage, 0.0);
        assert_eq!(report.alerts.len(), 0);
    }

    #[tokio::test]
    async fn test_alert_generation() {
        let config = ReporterConfig {
            alert_thresholds: AlertThresholds {
                cpu_usage_threshold: 50.0,
                ..Default::default()
            },
            ..Default::default()
        };
        let reporter = Reporter::new(config).await.unwrap();
        
        let mut metrics = SystemMetrics::default();
        metrics.cpu_usage = 75.0; // Above threshold
        
        let report = reporter.generate_report(&metrics).await.unwrap();
        assert!(!report.alerts.is_empty());
        assert_eq!(report.alerts[0].severity, AlertSeverity::Warning);
    }

    #[tokio::test]
    async fn test_comprehensive_report() {
        let config = ReporterConfig::default();
        let reporter = Reporter::new(config).await.unwrap();
        
        let aggregated = AggregatedMetrics {
            query: MetricsQuery {
                start_time: SystemTime::now(),
                end_time: SystemTime::now() + Duration::from_secs(3600),
                interval: Duration::from_secs(60),
                metrics: vec![],
                severity_filter: None,
                limit: None,
            },
            data_points: vec![],
            summary: MetricsSummary::default(),
        };
        
        let report = reporter.generate_comprehensive_report(&aggregated).await.unwrap();
        assert_eq!(report.data_points, 0);
        assert!(report.recommendations.is_empty());
    }

    #[tokio::test]
    async fn test_reporter_statistics() {
        let config = ReporterConfig::default();
        let reporter = Reporter::new(config).await.unwrap();
        
        let stats = reporter.get_statistics().await;
        assert_eq!(stats.total_reports, 0);
        assert_eq!(stats.total_alerts, 0);
    }

    #[tokio::test]
    async fn test_alert_clearing() {
        let config = ReporterConfig::default();
        let reporter = Reporter::new(config).await.unwrap();
        
        // Generate some alerts
        let mut metrics = SystemMetrics::default();
        metrics.cpu_usage = 90.0; // Above threshold
        
        let _report = reporter.generate_report(&metrics).await.unwrap();
        
        // Clear old alerts
        reporter.clear_old_alerts(1).await.unwrap();
        
        let stats = reporter.get_statistics().await;
        assert_eq!(stats.alerts_in_memory, 0);
    }
} 