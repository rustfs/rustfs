// Copyright 2024 RustFS Team

use std::sync::Arc;
use std::time::{SystemTime, Duration};

use tracing::{debug, error, info, warn};

use crate::{
    error::Result,
    metrics::{Collector, Reporter, Storage, MetricsQuery, MetricType},
};

use super::{HttpRequest, HttpResponse};

/// Configuration for the metrics API
#[derive(Debug, Clone)]
pub struct MetricsApiConfig {
    /// Whether to enable metrics API
    pub enabled: bool,
    /// Metrics API prefix
    pub prefix: String,
    /// Authentication required
    pub require_auth: bool,
    /// Metrics token
    pub metrics_token: Option<String>,
    /// Rate limiting for metrics endpoints
    pub rate_limit_requests_per_minute: u32,
    /// Maximum request body size
    pub max_request_size: usize,
    /// Enable metrics caching
    pub enable_caching: bool,
    /// Cache TTL in seconds
    pub cache_ttl_seconds: u64,
    /// Enable metrics compression
    pub enable_compression: bool,
    /// Default metrics format
    pub default_format: MetricsFormat,
}

impl Default for MetricsApiConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            prefix: "/metrics".to_string(),
            require_auth: false,
            metrics_token: None,
            rate_limit_requests_per_minute: 1000,
            max_request_size: 1024 * 1024, // 1 MB
            enable_caching: true,
            cache_ttl_seconds: 300, // 5 minutes
            enable_compression: true,
            default_format: MetricsFormat::Json,
        }
    }
}

/// Metrics format
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricsFormat {
    Json,
    Prometheus,
    Csv,
    Xml,
}

/// Backup report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupReport {
    pub timestamp: SystemTime,
    pub backup_id: String,
    pub status: BackupStatus,
    pub objects_backed_up: u64,
    pub total_size: u64,
    pub duration: Duration,
    pub errors: Vec<String>,
}

/// Restore report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreReport {
    pub timestamp: SystemTime,
    pub restore_id: String,
    pub status: RestoreStatus,
    pub objects_restored: u64,
    pub total_size: u64,
    pub duration: Duration,
    pub errors: Vec<String>,
}

/// Data integrity report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataIntegrityReport {
    pub timestamp: SystemTime,
    pub validation_id: String,
    pub status: ValidationStatus,
    pub objects_validated: u64,
    pub corrupted_objects: u64,
    pub duration: Duration,
    pub details: Vec<ValidationDetail>,
}

/// Backup status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackupStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
}

/// Restore status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RestoreStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
}

/// Validation status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValidationStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
}

/// Validation detail
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationDetail {
    pub object_path: String,
    pub status: ValidationResult,
    pub error_message: Option<String>,
}

/// Validation result
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValidationResult {
    Valid,
    Corrupted,
    Missing,
    AccessDenied,
}

/// Metrics API that provides metrics data and operations
pub struct MetricsApi {
    config: MetricsApiConfig,
    collector: Arc<Collector>,
    reporter: Arc<Reporter>,
    storage: Arc<Storage>,
}

impl MetricsApi {
    /// Create a new metrics API
    pub async fn new(
        config: MetricsApiConfig,
        collector: Arc<Collector>,
        reporter: Arc<Reporter>,
        storage: Arc<Storage>,
    ) -> Result<Self> {
        Ok(Self {
            config,
            collector,
            reporter,
            storage,
        })
    }

    /// Get the configuration
    pub fn config(&self) -> &MetricsApiConfig {
        &self.config
    }

    /// Handle HTTP request
    pub async fn handle_request(&self, request: HttpRequest) -> Result<HttpResponse> {
        // Check authentication if required
        if self.config.require_auth {
            if !self.authenticate_request(&request).await? {
                return Ok(HttpResponse {
                    status_code: 401,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "error": "Unauthorized",
                        "message": "Authentication required"
                    }).to_string(),
                })
            }
        }

        match request.path.as_str() {
            // Current metrics
            "/metrics/current" => self.get_current_metrics(request).await,
            "/metrics/latest" => self.get_latest_metrics(request).await,
            
            // Historical metrics
            "/metrics/history" => self.get_metrics_history(request).await,
            "/metrics/range" => self.get_metrics_range(request).await,
            
            // Aggregated metrics
            "/metrics/aggregated" => self.get_aggregated_metrics(request).await,
            "/metrics/summary" => self.get_metrics_summary(request).await,
            
            // Specific metric types
            "/metrics/system" => self.get_system_metrics(request).await,
            "/metrics/scan" => self.get_scan_metrics(request).await,
            "/metrics/heal" => self.get_heal_metrics(request).await,
            "/metrics/policy" => self.get_policy_metrics(request).await,
            "/metrics/network" => self.get_network_metrics(request).await,
            "/metrics/disk" => self.get_disk_metrics(request).await,
            
            // Health issues
            "/metrics/health-issues" => self.get_health_issues(request).await,
            "/metrics/alerts" => self.get_alerts(request).await,
            
            // Reports
            "/metrics/reports" => self.get_reports(request).await,
            "/metrics/reports/comprehensive" => self.get_comprehensive_report(request).await,
            
            // Prometheus format
            "/metrics/prometheus" => self.get_prometheus_metrics(request).await,
            
            // Storage operations
            "/metrics/storage/backup" => self.backup_metrics(request).await,
            "/metrics/storage/restore" => self.restore_metrics(request).await,
            "/metrics/storage/validate" => self.validate_metrics(request).await,
            
            // Default 404
            _ => Ok(HttpResponse {
                status_code: 404,
                headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                body: serde_json::json!({
                    "error": "Not Found",
                    "message": "Metrics endpoint not found"
                }).to_string(),
            }),
        }
    }

    /// Authenticate request
    async fn authenticate_request(&self, request: &HttpRequest) -> Result<bool> {
        if let Some(token) = &self.config.metrics_token {
            // Check for Authorization header
            if let Some(auth_header) = request.headers.iter().find(|(k, _)| k.to_lowercase() == "authorization") {
                if auth_header.1 == format!("Bearer {}", token) {
                    return Ok(true);
                }
            }

            // Check for token in query parameters
            if let Some(token_param) = request.query_params.iter().find(|(k, _)| k == "token") {
                if token_param.1 == *token {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    /// Get current metrics
    async fn get_current_metrics(&self, _request: HttpRequest) -> Result<HttpResponse> {
        match self.collector.collect_metrics().await {
            Ok(metrics) => {
                let format = self.get_request_format(&_request);
                let body = self.format_metrics(&metrics, format.clone()).await?;
                
                Ok(HttpResponse {
                    status_code: 200,
                    headers: vec![("Content-Type".to_string(), self.get_content_type(format))],
                    body,
                })
            }
            Err(e) => {
                error!("Failed to collect current metrics: {}", e);
                Ok(HttpResponse {
                    status_code: 500,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "error": "Internal Server Error",
                        "message": format!("Failed to collect metrics: {}", e)
                    }).to_string(),
                })
            }
        }
    }

    /// Get latest metrics
    async fn get_latest_metrics(&self, _request: HttpRequest) -> Result<HttpResponse> {
        match self.collector.get_latest_metrics().await {
            Ok(Some(metrics)) => {
                let format = self.get_request_format(&_request);
                let body = self.format_metrics(&metrics, format.clone()).await?;
                
                Ok(HttpResponse {
                    status_code: 200,
                    headers: vec![("Content-Type".to_string(), self.get_content_type(format))],
                    body,
                })
            }
            Ok(None) => {
                Ok(HttpResponse {
                    status_code: 404,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "error": "Not Found",
                        "message": "No metrics available"
                    }).to_string(),
                })
            }
            Err(e) => {
                error!("Failed to get latest metrics: {}", e);
                Ok(HttpResponse {
                    status_code: 500,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "error": "Internal Server Error",
                        "message": format!("Failed to get latest metrics: {}", e)
                    }).to_string(),
                })
            }
        }
    }

    /// Get metrics history
    async fn get_metrics_history(&self, request: HttpRequest) -> Result<HttpResponse> {
        let hours = request.query_params
            .iter()
            .find(|(k, _)| k == "hours")
            .and_then(|(_, v)| v.parse::<u64>().ok())
            .unwrap_or(24);

        let end_time = std::time::SystemTime::now();
        let start_time = end_time - std::time::Duration::from_secs(hours * 3600);

        let query = MetricsQuery {
            start_time,
            end_time,
            interval: std::time::Duration::from_secs(300), // 5 minutes
            metrics: vec![],
            severity_filter: None,
            limit: None,
        };

        match self.collector.query_metrics(query).await {
            Ok(aggregated) => {
                let format = self.get_request_format(&request);
                let body = self.format_aggregated_metrics(&aggregated, format.clone()).await?;
                
                Ok(HttpResponse {
                    status_code: 200,
                    headers: vec![("Content-Type".to_string(), self.get_content_type(format))],
                    body,
                })
            }
            Err(e) => {
                error!("Failed to get metrics history: {}", e);
                Ok(HttpResponse {
                    status_code: 500,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "error": "Internal Server Error",
                        "message": format!("Failed to get metrics history: {}", e)
                    }).to_string(),
                })
            }
        }
    }

    /// Get metrics range
    async fn get_metrics_range(&self, request: HttpRequest) -> Result<HttpResponse> {
        let start_time = request.query_params
            .iter()
            .find(|(k, _)| k == "start")
            .and_then(|(_, v)| v.parse::<u64>().ok())
            .map(|ts| std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(ts))
            .unwrap_or_else(|| std::time::SystemTime::now() - std::time::Duration::from_secs(3600));

        let end_time = request.query_params
            .iter()
            .find(|(k, _)| k == "end")
            .and_then(|(_, v)| v.parse::<u64>().ok())
            .map(|ts| std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(ts))
            .unwrap_or_else(std::time::SystemTime::now);

        let query = MetricsQuery {
            start_time,
            end_time,
            interval: std::time::Duration::from_secs(300), // 5 minutes
            metrics: vec![],
            severity_filter: None,
            limit: None,
        };

        match self.collector.query_metrics(query).await {
            Ok(aggregated) => {
                let format = self.get_request_format(&request);
                let body = self.format_aggregated_metrics(&aggregated, format.clone()).await?;
                
                Ok(HttpResponse {
                    status_code: 200,
                    headers: vec![("Content-Type".to_string(), self.get_content_type(format))],
                    body,
                })
            }
            Err(e) => {
                error!("Failed to get metrics range: {}", e);
                Ok(HttpResponse {
                    status_code: 500,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "error": "Internal Server Error",
                        "message": format!("Failed to get metrics range: {}", e)
                    }).to_string(),
                })
            }
        }
    }

    /// Get aggregated metrics
    async fn get_aggregated_metrics(&self, request: HttpRequest) -> Result<HttpResponse> {
        let query = self.parse_metrics_query(&request)?;

        match self.collector.query_metrics(query).await {
            Ok(aggregated) => {
                let format = self.get_request_format(&request);
                let body = self.format_aggregated_metrics(&aggregated, format.clone()).await?;
                
                Ok(HttpResponse {
                    status_code: 200,
                    headers: vec![("Content-Type".to_string(), self.get_content_type(format))],
                    body,
                })
            }
            Err(e) => {
                error!("Failed to get aggregated metrics: {}", e);
                Ok(HttpResponse {
                    status_code: 500,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "error": "Internal Server Error",
                        "message": format!("Failed to get aggregated metrics: {}", e)
                    }).to_string(),
                })
            }
        }
    }

    /// Get metrics summary
    async fn get_metrics_summary(&self, request: HttpRequest) -> Result<HttpResponse> {
        let hours = request.query_params
            .iter()
            .find(|(k, _)| k == "hours")
            .and_then(|(_, v)| v.parse::<u64>().ok())
            .unwrap_or(24);

        let end_time = std::time::SystemTime::now();
        let start_time = end_time - std::time::Duration::from_secs(hours * 3600);

        let query = MetricsQuery {
            start_time,
            end_time,
            interval: std::time::Duration::from_secs(3600), // 1 hour
            metrics: vec![],
            severity_filter: None,
            limit: None,
        };

        match self.collector.query_metrics(query).await {
            Ok(aggregated) => {
                Ok(HttpResponse {
                    status_code: 200,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "status": "success",
                        "summary": aggregated.summary,
                        "timestamp": chrono::Utc::now().to_rfc3339()
                    }).to_string(),
                })
            }
            Err(e) => {
                error!("Failed to get metrics summary: {}", e);
                Ok(HttpResponse {
                    status_code: 500,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "error": "Internal Server Error",
                        "message": format!("Failed to get metrics summary: {}", e)
                    }).to_string(),
                })
            }
        }
    }

    /// Get system metrics
    async fn get_system_metrics(&self, request: HttpRequest) -> Result<HttpResponse> {
        match self.collector.collect_metrics().await {
            Ok(metrics) => {
                let system_data = serde_json::json!({
                    "cpu_usage": metrics.cpu_usage,
                    "memory_usage": metrics.memory_usage,
                    "disk_usage": metrics.disk_usage,
                    "system_load": metrics.system_load,
                    "active_operations": metrics.active_operations,
                    "network_io": metrics.network_io,
                    "disk_io": metrics.disk_io,
                });

                let format = self.get_request_format(&request);
                let body = self.format_json_data(&system_data, format.clone()).await?;
                
                Ok(HttpResponse {
                    status_code: 200,
                    headers: vec![("Content-Type".to_string(), self.get_content_type(format))],
                    body,
                })
            }
            Err(e) => {
                error!("Failed to get system metrics: {}", e);
                Ok(HttpResponse {
                    status_code: 500,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "error": "Internal Server Error",
                        "message": format!("Failed to get system metrics: {}", e)
                    }).to_string(),
                })
            }
        }
    }

    /// Get scan metrics
    async fn get_scan_metrics(&self, request: HttpRequest) -> Result<HttpResponse> {
        match self.collector.collect_metrics().await {
            Ok(metrics) => {
                let scan_data = serde_json::json!({
                    "scan_metrics": metrics.scan_metrics,
                });

                let format = self.get_request_format(&request);
                let body = self.format_json_data(&scan_data, format.clone()).await?;
                
                Ok(HttpResponse {
                    status_code: 200,
                    headers: vec![("Content-Type".to_string(), self.get_content_type(format))],
                    body,
                })
            }
            Err(e) => {
                error!("Failed to get scan metrics: {}", e);
                Ok(HttpResponse {
                    status_code: 500,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "error": "Internal Server Error",
                        "message": format!("Failed to get scan metrics: {}", e)
                    }).to_string(),
                })
            }
        }
    }

    /// Get heal metrics
    async fn get_heal_metrics(&self, request: HttpRequest) -> Result<HttpResponse> {
        match self.collector.collect_metrics().await {
            Ok(metrics) => {
                let heal_data = serde_json::json!({
                    "heal_metrics": metrics.heal_metrics,
                });

                let format = self.get_request_format(&request);
                let body = self.format_json_data(&heal_data, format.clone()).await?;
                
                Ok(HttpResponse {
                    status_code: 200,
                    headers: vec![("Content-Type".to_string(), self.get_content_type(format))],
                    body,
                })
            }
            Err(e) => {
                error!("Failed to get heal metrics: {}", e);
                Ok(HttpResponse {
                    status_code: 500,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "error": "Internal Server Error",
                        "message": format!("Failed to get heal metrics: {}", e)
                    }).to_string(),
                })
            }
        }
    }

    /// Get policy metrics
    async fn get_policy_metrics(&self, request: HttpRequest) -> Result<HttpResponse> {
        match self.collector.collect_metrics().await {
            Ok(metrics) => {
                let policy_data = serde_json::json!({
                    "policy_metrics": metrics.policy_metrics,
                });

                let format = self.get_request_format(&request);
                let body = self.format_json_data(&policy_data, format.clone()).await?;
                
                Ok(HttpResponse {
                    status_code: 200,
                    headers: vec![("Content-Type".to_string(), self.get_content_type(format))],
                    body,
                })
            }
            Err(e) => {
                error!("Failed to get policy metrics: {}", e);
                Ok(HttpResponse {
                    status_code: 500,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "error": "Internal Server Error",
                        "message": format!("Failed to get policy metrics: {}", e)
                    }).to_string(),
                })
            }
        }
    }

    /// Get network metrics
    async fn get_network_metrics(&self, request: HttpRequest) -> Result<HttpResponse> {
        match self.collector.collect_metrics().await {
            Ok(metrics) => {
                let network_data = serde_json::json!({
                    "network_io": metrics.network_io,
                });

                let format = self.get_request_format(&request);
                let body = self.format_json_data(&network_data, format.clone()).await?;
                
                Ok(HttpResponse {
                    status_code: 200,
                    headers: vec![("Content-Type".to_string(), self.get_content_type(format))],
                    body,
                })
            }
            Err(e) => {
                error!("Failed to get network metrics: {}", e);
                Ok(HttpResponse {
                    status_code: 500,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "error": "Internal Server Error",
                        "message": format!("Failed to get network metrics: {}", e)
                    }).to_string(),
                })
            }
        }
    }

    /// Get disk metrics
    async fn get_disk_metrics(&self, request: HttpRequest) -> Result<HttpResponse> {
        match self.collector.collect_metrics().await {
            Ok(metrics) => {
                let disk_data = serde_json::json!({
                    "disk_io": metrics.disk_io,
                    "disk_usage": metrics.disk_usage,
                });

                let format = self.get_request_format(&request);
                let body = self.format_json_data(&disk_data, format.clone()).await?;
                
                Ok(HttpResponse {
                    status_code: 200,
                    headers: vec![("Content-Type".to_string(), self.get_content_type(format))],
                    body,
                })
            }
            Err(e) => {
                error!("Failed to get disk metrics: {}", e);
                Ok(HttpResponse {
                    status_code: 500,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "error": "Internal Server Error",
                        "message": format!("Failed to get disk metrics: {}", e)
                    }).to_string(),
                })
            }
        }
    }

    /// Get health issues
    async fn get_health_issues(&self, _request: HttpRequest) -> Result<HttpResponse> {
        match self.collector.collect_metrics().await {
            Ok(metrics) => {
                Ok(HttpResponse {
                    status_code: 200,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "status": "success",
                        "health_issues": metrics.health_issues,
                        "timestamp": chrono::Utc::now().to_rfc3339()
                    }).to_string(),
                })
            }
            Err(e) => {
                error!("Failed to get health issues: {}", e);
                Ok(HttpResponse {
                    status_code: 500,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "error": "Internal Server Error",
                        "message": format!("Failed to get health issues: {}", e)
                    }).to_string(),
                })
            }
        }
    }

    /// Get alerts
    async fn get_alerts(&self, request: HttpRequest) -> Result<HttpResponse> {
        let hours = request.query_params
            .iter()
            .find(|(k, _)| k == "hours")
            .and_then(|(_, v)| v.parse::<u64>().ok())
            .unwrap_or(24);

        match self.reporter.get_recent_alerts(hours).await {
            Ok(alerts) => {
                Ok(HttpResponse {
                    status_code: 200,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "status": "success",
                        "alerts": alerts,
                        "timestamp": chrono::Utc::now().to_rfc3339()
                    }).to_string(),
                })
            }
            Err(e) => {
                error!("Failed to get alerts: {}", e);
                Ok(HttpResponse {
                    status_code: 500,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "error": "Internal Server Error",
                        "message": format!("Failed to get alerts: {}", e)
                    }).to_string(),
                })
            }
        }
    }

    /// Get reports
    async fn get_reports(&self, _request: HttpRequest) -> Result<HttpResponse> {
        let stats = self.reporter.get_statistics().await;
        
        Ok(HttpResponse {
            status_code: 200,
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            body: serde_json::json!({
                "status": "success",
                "reports": stats,
                "timestamp": chrono::Utc::now().to_rfc3339()
            }).to_string(),
        })
    }

    /// Get comprehensive report
    async fn get_comprehensive_report(&self, request: HttpRequest) -> Result<HttpResponse> {
        let query = self.parse_metrics_query(&request)?;

        match self.collector.query_metrics(query).await {
            Ok(aggregated) => {
                match self.reporter.generate_comprehensive_report(&aggregated).await {
                    Ok(report) => {
                        Ok(HttpResponse {
                            status_code: 200,
                            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                            body: serde_json::json!({
                                "status": "success",
                                "report": report,
                                "timestamp": chrono::Utc::now().to_rfc3339()
                            }).to_string(),
                        })
                    }
                    Err(e) => {
                        error!("Failed to generate comprehensive report: {}", e);
                        Ok(HttpResponse {
                            status_code: 500,
                            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                            body: serde_json::json!({
                                "error": "Internal Server Error",
                                "message": format!("Failed to generate report: {}", e)
                            }).to_string(),
                        })
                    }
                }
            }
            Err(e) => {
                error!("Failed to get metrics for comprehensive report: {}", e);
                Ok(HttpResponse {
                    status_code: 500,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "error": "Internal Server Error",
                        "message": format!("Failed to get metrics: {}", e)
                    }).to_string(),
                })
            }
        }
    }

    /// Get Prometheus metrics
    async fn get_prometheus_metrics(&self, _request: HttpRequest) -> Result<HttpResponse> {
        match self.collector.collect_metrics().await {
            Ok(metrics) => {
                let prometheus_data = self.format_prometheus_metrics(&metrics).await?;
                
                Ok(HttpResponse {
                    status_code: 200,
                    headers: vec![("Content-Type".to_string(), "text/plain; version=0.0.4; charset=utf-8".to_string())],
                    body: prometheus_data,
                })
            }
            Err(e) => {
                error!("Failed to get Prometheus metrics: {}", e);
                Ok(HttpResponse {
                    status_code: 500,
                    headers: vec![("Content-Type".to_string(), "text/plain".to_string())],
                    body: format!("# ERROR: Failed to get metrics: {}", e),
                })
            }
        }
    }

    /// Backup metrics
    async fn backup_metrics(&self, request: HttpRequest) -> Result<HttpResponse> {
        let backup_path = request.query_params
            .iter()
            .find(|(k, _)| k == "path")
            .map(|(_, v)| std::path::PathBuf::from(v))
            .unwrap_or_else(|| std::path::PathBuf::from("/tmp/rustfs/metrics-backup"));

        match self.storage.backup_data(&backup_path).await {
            Ok(report) => {
                Ok(HttpResponse {
                    status_code: 200,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "status": "success",
                        "backup_report": report,
                        "timestamp": chrono::Utc::now().to_rfc3339()
                    }).to_string(),
                })
            }
            Err(e) => {
                error!("Failed to backup metrics: {}", e);
                Ok(HttpResponse {
                    status_code: 500,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "error": "Internal Server Error",
                        "message": format!("Failed to backup metrics: {}", e)
                    }).to_string(),
                })
            }
        }
    }

    /// Restore metrics
    async fn restore_metrics(&self, request: HttpRequest) -> Result<HttpResponse> {
        let backup_path = request.query_params
            .iter()
            .find(|(k, _)| k == "path")
            .map(|(_, v)| std::path::PathBuf::from(v))
            .unwrap_or_else(|| std::path::PathBuf::from("/tmp/rustfs/metrics-backup"));

        match self.storage.restore_data(&backup_path).await {
            Ok(report) => {
                Ok(HttpResponse {
                    status_code: 200,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "status": "success",
                        "restore_report": report,
                        "timestamp": chrono::Utc::now().to_rfc3339()
                    }).to_string(),
                })
            }
            Err(e) => {
                error!("Failed to restore metrics: {}", e);
                Ok(HttpResponse {
                    status_code: 500,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "error": "Internal Server Error",
                        "message": format!("Failed to restore metrics: {}", e)
                    }).to_string(),
                })
            }
        }
    }

    /// Validate metrics
    async fn validate_metrics(&self, _request: HttpRequest) -> Result<HttpResponse> {
        match self.storage.validate_data_integrity().await {
            Ok(report) => {
                Ok(HttpResponse {
                    status_code: 200,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "status": "success",
                        "validation_report": report,
                        "timestamp": chrono::Utc::now().to_rfc3339()
                    }).to_string(),
                })
            }
            Err(e) => {
                error!("Failed to validate metrics: {}", e);
                Ok(HttpResponse {
                    status_code: 500,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "error": "Internal Server Error",
                        "message": format!("Failed to validate metrics: {}", e)
                    }).to_string(),
                })
            }
        }
    }

    /// Helper methods
    fn get_request_format(&self, request: &HttpRequest) -> MetricsFormat {
        request.query_params
            .iter()
            .find(|(k, _)| k == "format")
            .and_then(|(_, v)| match v.as_str() {
                "prometheus" => Some(MetricsFormat::Prometheus),
                "csv" => Some(MetricsFormat::Csv),
                "xml" => Some(MetricsFormat::Xml),
                _ => Some(MetricsFormat::Json),
            })
            .unwrap_or(self.config.default_format.clone())
    }

    fn get_content_type(&self, format: MetricsFormat) -> String {
        match format {
            MetricsFormat::Json => "application/json".to_string(),
            MetricsFormat::Prometheus => "text/plain; version=0.0.4; charset=utf-8".to_string(),
            MetricsFormat::Csv => "text/csv".to_string(),
            MetricsFormat::Xml => "application/xml".to_string(),
        }
    }

    async fn format_metrics(&self, metrics: &crate::metrics::SystemMetrics, format: MetricsFormat) -> Result<String> {
        match format {
            MetricsFormat::Json => Ok(serde_json::json!({
                "status": "success",
                "metrics": metrics,
                "timestamp": chrono::Utc::now().to_rfc3339()
            }).to_string()),
            MetricsFormat::Prometheus => self.format_prometheus_metrics(metrics).await,
            MetricsFormat::Csv => self.format_csv_metrics(metrics).await,
            MetricsFormat::Xml => self.format_xml_metrics(metrics).await,
        }
    }

    async fn format_aggregated_metrics(&self, aggregated: &crate::metrics::AggregatedMetrics, format: MetricsFormat) -> Result<String> {
        match format {
            MetricsFormat::Json => Ok(serde_json::json!({
                "status": "success",
                "aggregated_metrics": aggregated,
                "timestamp": chrono::Utc::now().to_rfc3339()
            }).to_string()),
            MetricsFormat::Prometheus => self.format_prometheus_aggregated(aggregated).await,
            MetricsFormat::Csv => self.format_csv_aggregated(aggregated).await,
            MetricsFormat::Xml => self.format_xml_aggregated(aggregated).await,
        }
    }

    async fn format_json_data(&self, data: &serde_json::Value, format: MetricsFormat) -> Result<String> {
        match format {
            MetricsFormat::Json => Ok(serde_json::json!({
                "status": "success",
                "data": data,
                "timestamp": chrono::Utc::now().to_rfc3339()
            }).to_string()),
            _ => Ok(serde_json::json!(data).to_string()),
        }
    }

    async fn format_prometheus_metrics(&self, metrics: &crate::metrics::SystemMetrics) -> Result<String> {
        let mut prometheus_lines = Vec::new();
        
        // System metrics
        prometheus_lines.push(format!("rustfs_cpu_usage_percent {}", metrics.cpu_usage));
        prometheus_lines.push(format!("rustfs_memory_usage_percent {}", metrics.memory_usage));
        prometheus_lines.push(format!("rustfs_disk_usage_percent {}", metrics.disk_usage));
        prometheus_lines.push(format!("rustfs_system_load {}", metrics.system_load));
        prometheus_lines.push(format!("rustfs_active_operations {}", metrics.active_operations));
        
        // Network metrics
        prometheus_lines.push(format!("rustfs_network_bytes_received_per_sec {}", metrics.network_io.bytes_received_per_sec));
        prometheus_lines.push(format!("rustfs_network_bytes_sent_per_sec {}", metrics.network_io.bytes_sent_per_sec));
        
        // Disk metrics
        prometheus_lines.push(format!("rustfs_disk_bytes_read_per_sec {}", metrics.disk_io.bytes_read_per_sec));
        prometheus_lines.push(format!("rustfs_disk_bytes_written_per_sec {}", metrics.disk_io.bytes_written_per_sec));
        
        // Scan metrics
        prometheus_lines.push(format!("rustfs_scan_objects_scanned_total {}", metrics.scan_metrics.objects_scanned));
        prometheus_lines.push(format!("rustfs_scan_bytes_scanned_total {}", metrics.scan_metrics.bytes_scanned));
        
        // Heal metrics
        prometheus_lines.push(format!("rustfs_heal_total_repairs {}", metrics.heal_metrics.total_repairs));
        prometheus_lines.push(format!("rustfs_heal_successful_repairs {}", metrics.heal_metrics.successful_repairs));
        prometheus_lines.push(format!("rustfs_heal_failed_repairs {}", metrics.heal_metrics.failed_repairs));
        
        Ok(prometheus_lines.join("\n"))
    }

    async fn format_prometheus_aggregated(&self, _aggregated: &crate::metrics::AggregatedMetrics) -> Result<String> {
        // In a real implementation, this would format aggregated metrics for Prometheus
        Ok("# Aggregated metrics not yet implemented for Prometheus format".to_string())
    }

    async fn format_csv_metrics(&self, _metrics: &crate::metrics::SystemMetrics) -> Result<String> {
        // In a real implementation, this would format metrics as CSV
        Ok("timestamp,cpu_usage,memory_usage,disk_usage\n".to_string())
    }

    async fn format_csv_aggregated(&self, _aggregated: &crate::metrics::AggregatedMetrics) -> Result<String> {
        // In a real implementation, this would format aggregated metrics as CSV
        Ok("timestamp,avg_cpu_usage,avg_memory_usage,avg_disk_usage\n".to_string())
    }

    async fn format_xml_metrics(&self, _metrics: &crate::metrics::SystemMetrics) -> Result<String> {
        // In a real implementation, this would format metrics as XML
        Ok("<metrics><status>success</status></metrics>".to_string())
    }

    async fn format_xml_aggregated(&self, _aggregated: &crate::metrics::AggregatedMetrics) -> Result<String> {
        // In a real implementation, this would format aggregated metrics as XML
        Ok("<aggregated_metrics><status>success</status></aggregated_metrics>".to_string())
    }

    fn parse_metrics_query(&self, request: &HttpRequest) -> Result<MetricsQuery> {
        let start_time = request.query_params
            .iter()
            .find(|(k, _)| k == "start")
            .and_then(|(_, v)| v.parse::<u64>().ok())
            .map(|ts| std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(ts))
            .unwrap_or_else(|| std::time::SystemTime::now() - std::time::Duration::from_secs(3600));

        let end_time = request.query_params
            .iter()
            .find(|(k, _)| k == "end")
            .and_then(|(_, v)| v.parse::<u64>().ok())
            .map(|ts| std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(ts))
            .unwrap_or_else(std::time::SystemTime::now);

        let interval = request.query_params
            .iter()
            .find(|(k, _)| k == "interval")
            .and_then(|(_, v)| v.parse::<u64>().ok())
            .map(|secs| std::time::Duration::from_secs(secs))
            .unwrap_or(std::time::Duration::from_secs(300));

        let metrics = request.query_params
            .iter()
            .filter(|(k, _)| k == "metric")
            .map(|(_, v)| match v.as_str() {
                "system" => MetricType::System,
                "network" => MetricType::Network,
                "disk" => MetricType::DiskIo,
                "scan" => MetricType::Scan,
                "heal" => MetricType::Heal,
                "policy" => MetricType::Policy,
                "health" => MetricType::HealthIssues,
                _ => MetricType::System,
            })
            .collect();

        let limit = request.query_params
            .iter()
            .find(|(k, _)| k == "limit")
            .and_then(|(_, v)| v.parse::<u64>().ok());

        Ok(MetricsQuery {
            start_time,
            end_time,
            interval,
            metrics,
            severity_filter: None,
            limit,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{CollectorConfig, ReporterConfig, StorageConfig};

    #[tokio::test]
    async fn test_metrics_api_creation() {
        let config = MetricsApiConfig::default();
        let collector = Arc::new(Collector::new(CollectorConfig::default()).await.unwrap());
        let reporter = Arc::new(Reporter::new(ReporterConfig::default()).await.unwrap());
        let storage = Arc::new(Storage::new(StorageConfig::default()).await.unwrap());

        let metrics_api = MetricsApi::new(config, collector, reporter, storage).await.unwrap();
        
        assert!(metrics_api.config().enabled);
        assert_eq!(metrics_api.config().prefix, "/metrics");
    }

    #[tokio::test]
    async fn test_current_metrics() {
        let config = MetricsApiConfig::default();
        let collector = Arc::new(Collector::new(CollectorConfig::default()).await.unwrap());
        let reporter = Arc::new(Reporter::new(ReporterConfig::default()).await.unwrap());
        let storage = Arc::new(Storage::new(StorageConfig::default()).await.unwrap());

        let metrics_api = MetricsApi::new(config, collector, reporter, storage).await.unwrap();

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/metrics/current".to_string(),
            headers: vec![],
            body: None,
            query_params: vec![],
        };

        let response = metrics_api.handle_request(request).await.unwrap();
        assert_eq!(response.status_code, 200);
        assert!(response.body.contains("status"));
    }

    #[tokio::test]
    async fn test_prometheus_metrics() {
        let config = MetricsApiConfig::default();
        let collector = Arc::new(Collector::new(CollectorConfig::default()).await.unwrap());
        let reporter = Arc::new(Reporter::new(ReporterConfig::default()).await.unwrap());
        let storage = Arc::new(Storage::new(StorageConfig::default()).await.unwrap());

        let metrics_api = MetricsApi::new(config, collector, reporter, storage).await.unwrap();

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/metrics/prometheus".to_string(),
            headers: vec![],
            body: None,
            query_params: vec![],
        };

        let response = metrics_api.handle_request(request).await.unwrap();
        assert_eq!(response.status_code, 200);
        assert!(response.body.contains("rustfs_cpu_usage_percent"));
    }

    #[tokio::test]
    async fn test_system_metrics() {
        let config = MetricsApiConfig::default();
        let collector = Arc::new(Collector::new(CollectorConfig::default()).await.unwrap());
        let reporter = Arc::new(Reporter::new(ReporterConfig::default()).await.unwrap());
        let storage = Arc::new(Storage::new(StorageConfig::default()).await.unwrap());

        let metrics_api = MetricsApi::new(config, collector, reporter, storage).await.unwrap();

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/metrics/system".to_string(),
            headers: vec![],
            body: None,
            query_params: vec![],
        };

        let response = metrics_api.handle_request(request).await.unwrap();
        assert_eq!(response.status_code, 200);
        assert!(response.body.contains("cpu_usage"));
    }
} 