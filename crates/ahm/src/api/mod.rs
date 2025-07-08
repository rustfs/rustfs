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

//! API interfaces for the AHM system
//!
//! Provides REST and gRPC endpoints for:
//! - Administrative operations
//! - Metrics and monitoring
//! - System status and control

pub mod admin_api;
pub mod metrics_api;
pub mod status_api;

pub use admin_api::{AdminApi, AdminApiConfig};
pub use metrics_api::{MetricsApi, MetricsApiConfig};
pub use status_api::{StatusApi, StatusApiConfig};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::{
    error::Result,
    heal::HealEngine,
    metrics::{Collector, Reporter, Storage},
    policy::{ScanPolicyEngine as PolicyEngine},
    scanner::{Engine as ScanEngine},
};

/// Configuration for the API server
#[derive(Debug, Clone)]
pub struct ApiConfig {
    /// Admin API configuration
    pub admin: AdminApiConfig,
    /// Metrics API configuration
    pub metrics: MetricsApiConfig,
    /// Status API configuration
    pub status: StatusApiConfig,
    /// Server address
    pub address: String,
    /// Server port
    pub port: u16,
    /// Enable HTTPS
    pub enable_https: bool,
    /// SSL certificate path
    pub ssl_cert_path: Option<String>,
    /// SSL key path
    pub ssl_key_path: Option<String>,
    /// Request timeout
    pub request_timeout: std::time::Duration,
    /// Maximum request size
    pub max_request_size: usize,
    /// Enable CORS
    pub enable_cors: bool,
    /// CORS origins
    pub cors_origins: Vec<String>,
    /// Enable rate limiting
    pub enable_rate_limiting: bool,
    /// Rate limit requests per minute
    pub rate_limit_requests_per_minute: u32,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            admin: AdminApiConfig::default(),
            metrics: MetricsApiConfig::default(),
            status: StatusApiConfig::default(),
            address: "127.0.0.1".to_string(),
            port: 8080,
            enable_https: false,
            ssl_cert_path: None,
            ssl_key_path: None,
            request_timeout: std::time::Duration::from_secs(30),
            max_request_size: 1024 * 1024, // 1 MB
            enable_cors: true,
            cors_origins: vec!["*".to_string()],
            enable_rate_limiting: true,
            rate_limit_requests_per_minute: 1000,
        }
    }
}

/// API server that provides HTTP endpoints for AHM functionality
pub struct ApiServer {
    config: ApiConfig,
    admin_api: Arc<AdminApi>,
    metrics_api: Arc<MetricsApi>,
    status_api: Arc<StatusApi>,
    scan_engine: Arc<ScanEngine>,
    heal_engine: Arc<HealEngine>,
    policy_engine: Arc<PolicyEngine>,
    metrics_collector: Arc<Collector>,
    metrics_reporter: Arc<Reporter>,
    metrics_storage: Arc<Storage>,
}

impl ApiServer {
    /// Create a new API server
    pub async fn new(
        config: ApiConfig,
        scan_engine: Arc<ScanEngine>,
        heal_engine: Arc<HealEngine>,
        policy_engine: Arc<PolicyEngine>,
        metrics_collector: Arc<Collector>,
        metrics_reporter: Arc<Reporter>,
        metrics_storage: Arc<Storage>,
    ) -> Result<Self> {
        let admin_api = Arc::new(AdminApi::new(config.admin.clone(), scan_engine.clone(), heal_engine.clone(), policy_engine.clone()).await?);
        let metrics_api = Arc::new(MetricsApi::new(config.metrics.clone(), metrics_collector.clone(), metrics_reporter.clone(), metrics_storage.clone()).await?);
        let status_api = Arc::new(StatusApi::new(config.status.clone(), scan_engine.clone(), heal_engine.clone(), policy_engine.clone()).await?);

        Ok(Self {
            config,
            admin_api,
            metrics_api,
            status_api,
            scan_engine,
            heal_engine,
            policy_engine,
            metrics_collector,
            metrics_reporter,
            metrics_storage,
        })
    }

    /// Get the configuration
    pub fn config(&self) -> &ApiConfig {
        &self.config
    }

    /// Start the API server
    pub async fn start(&self) -> Result<()> {
        // In a real implementation, this would start an HTTP server
        // For now, we'll just simulate the server startup
        tracing::info!("API server starting on {}:{}", self.config.address, self.config.port);
        
        if self.config.enable_https {
            tracing::info!("HTTPS enabled");
        }
        
        if self.config.enable_cors {
            tracing::info!("CORS enabled with origins: {:?}", self.config.cors_origins);
        }
        
        if self.config.enable_rate_limiting {
            tracing::info!("Rate limiting enabled: {} requests/minute", self.config.rate_limit_requests_per_minute);
        }

        tracing::info!("API server started successfully");
        Ok(())
    }

    /// Stop the API server
    pub async fn stop(&self) -> Result<()> {
        tracing::info!("API server stopping");
        tracing::info!("API server stopped successfully");
        Ok(())
    }

    /// Get server status
    pub async fn status(&self) -> ServerStatus {
        ServerStatus {
            address: self.config.address.clone(),
            port: self.config.port,
            https_enabled: self.config.enable_https,
            cors_enabled: self.config.enable_cors,
            rate_limiting_enabled: self.config.enable_rate_limiting,
            admin_api_enabled: true,
            metrics_api_enabled: true,
            status_api_enabled: true,
        }
    }

    /// Get admin API
    pub fn admin_api(&self) -> &Arc<AdminApi> {
        &self.admin_api
    }

    /// Get metrics API
    pub fn metrics_api(&self) -> &Arc<MetricsApi> {
        &self.metrics_api
    }

    /// Get status API
    pub fn status_api(&self) -> &Arc<StatusApi> {
        &self.status_api
    }

    /// Handle HTTP request
    pub async fn handle_request(&self, request: HttpRequest) -> Result<HttpResponse> {
        match request.path.as_str() {
            // Admin API routes
            path if path.starts_with("/admin") => {
                self.admin_api.handle_request(request).await
            }
            // Metrics API routes
            path if path.starts_with("/metrics") => {
                self.metrics_api.handle_request(request).await
            }
            // Status API routes
            path if path.starts_with("/status") => {
                self.status_api.handle_request(request).await
            }
            // Health check
            "/health" => {
                Ok(HttpResponse {
                    status_code: 200,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "status": "healthy",
                        "timestamp": chrono::Utc::now().to_rfc3339(),
                        "version": env!("CARGO_PKG_VERSION")
                    }).to_string(),
                })
            }
            // Root endpoint
            "/" => {
                Ok(HttpResponse {
                    status_code: 200,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "service": "RustFS AHM API",
                        "version": env!("CARGO_PKG_VERSION"),
                        "endpoints": {
                            "admin": "/admin",
                            "metrics": "/metrics",
                            "status": "/status",
                            "health": "/health"
                        }
                    }).to_string(),
                })
            }
            // 404 for unknown routes
            _ => {
                Ok(HttpResponse {
                    status_code: 404,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "error": "Not Found",
                        "message": "The requested endpoint does not exist"
                    }).to_string(),
                })
            }
        }
    }
}

/// HTTP request
#[derive(Debug, Clone)]
pub struct HttpRequest {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<String>,
    pub query_params: Vec<(String, String)>,
}

/// HTTP response
#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status_code: u16,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

/// Server status
#[derive(Debug, Clone)]
pub struct ServerStatus {
    pub address: String,
    pub port: u16,
    pub https_enabled: bool,
    pub cors_enabled: bool,
    pub rate_limiting_enabled: bool,
    pub admin_api_enabled: bool,
    pub metrics_api_enabled: bool,
    pub status_api_enabled: bool,
}

/// API endpoint information
#[derive(Debug, Clone)]
pub struct EndpointInfo {
    pub path: String,
    pub method: String,
    pub description: String,
    pub parameters: Vec<ParameterInfo>,
    pub response_type: String,
}

/// Parameter information
#[derive(Debug, Clone)]
pub struct ParameterInfo {
    pub name: String,
    pub parameter_type: String,
    pub required: bool,
    pub description: String,
}

/// API documentation
#[derive(Debug, Clone)]
pub struct ApiDocumentation {
    pub title: String,
    pub version: String,
    pub description: String,
    pub endpoints: Vec<EndpointInfo>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        heal::HealEngineConfig,
        metrics::{CollectorConfig, ReporterConfig, StorageConfig},
        policy::PolicyEngineConfig,
        scanner::ScanEngineConfig,
    };

    #[tokio::test]
    async fn test_api_server_creation() {
        let config = ApiConfig::default();
        let scan_engine = Arc::new(ScanEngine::new(ScanEngineConfig::default()).await.unwrap());
        let heal_engine = Arc::new(HealEngine::new(HealEngineConfig::default()).await.unwrap());
        let policy_engine = Arc::new(PolicyEngine::new(PolicyEngineConfig::default()).await.unwrap());
        let metrics_collector = Arc::new(Collector::new(CollectorConfig::default()).await.unwrap());
        let metrics_reporter = Arc::new(Reporter::new(ReporterConfig::default()).await.unwrap());
        let metrics_storage = Arc::new(Storage::new(StorageConfig::default()).await.unwrap());

        let server = ApiServer::new(
            config,
            scan_engine,
            heal_engine,
            policy_engine,
            metrics_collector,
            metrics_reporter,
            metrics_storage,
        ).await.unwrap();

        assert_eq!(server.config().port, 8080);
        assert_eq!(server.config().address, "127.0.0.1");
    }

    #[tokio::test]
    async fn test_api_server_start_stop() {
        let config = ApiConfig::default();
        let scan_engine = Arc::new(ScanEngine::new(ScanEngineConfig::default()).await.unwrap());
        let heal_engine = Arc::new(HealEngine::new(HealEngineConfig::default()).await.unwrap());
        let policy_engine = Arc::new(PolicyEngine::new(PolicyEngineConfig::default()).await.unwrap());
        let metrics_collector = Arc::new(Collector::new(CollectorConfig::default()).await.unwrap());
        let metrics_reporter = Arc::new(Reporter::new(ReporterConfig::default()).await.unwrap());
        let metrics_storage = Arc::new(Storage::new(StorageConfig::default()).await.unwrap());

        let server = ApiServer::new(
            config,
            scan_engine,
            heal_engine,
            policy_engine,
            metrics_collector,
            metrics_reporter,
            metrics_storage,
        ).await.unwrap();

        server.start().await.unwrap();
        server.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_api_server_status() {
        let config = ApiConfig::default();
        let scan_engine = Arc::new(ScanEngine::new(ScanEngineConfig::default()).await.unwrap());
        let heal_engine = Arc::new(HealEngine::new(HealEngineConfig::default()).await.unwrap());
        let policy_engine = Arc::new(PolicyEngine::new(PolicyEngineConfig::default()).await.unwrap());
        let metrics_collector = Arc::new(Collector::new(CollectorConfig::default()).await.unwrap());
        let metrics_reporter = Arc::new(Reporter::new(ReporterConfig::default()).await.unwrap());
        let metrics_storage = Arc::new(Storage::new(StorageConfig::default()).await.unwrap());

        let server = ApiServer::new(
            config,
            scan_engine,
            heal_engine,
            policy_engine,
            metrics_collector,
            metrics_reporter,
            metrics_storage,
        ).await.unwrap();

        let status = server.status().await;
        assert_eq!(status.port, 8080);
        assert_eq!(status.address, "127.0.0.1");
        assert!(status.admin_api_enabled);
        assert!(status.metrics_api_enabled);
        assert!(status.status_api_enabled);
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let config = ApiConfig::default();
        let scan_engine = Arc::new(ScanEngine::new(ScanEngineConfig::default()).await.unwrap());
        let heal_engine = Arc::new(HealEngine::new(HealEngineConfig::default()).await.unwrap());
        let policy_engine = Arc::new(PolicyEngine::new(PolicyEngineConfig::default()).await.unwrap());
        let metrics_collector = Arc::new(Collector::new(CollectorConfig::default()).await.unwrap());
        let metrics_reporter = Arc::new(Reporter::new(ReporterConfig::default()).await.unwrap());
        let metrics_storage = Arc::new(Storage::new(StorageConfig::default()).await.unwrap());

        let server = ApiServer::new(
            config,
            scan_engine,
            heal_engine,
            policy_engine,
            metrics_collector,
            metrics_reporter,
            metrics_storage,
        ).await.unwrap();

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/health".to_string(),
            headers: vec![],
            body: None,
            query_params: vec![],
        };

        let response = server.handle_request(request).await.unwrap();
        assert_eq!(response.status_code, 200);
        assert!(response.body.contains("healthy"));
    }

    #[tokio::test]
    async fn test_root_endpoint() {
        let config = ApiConfig::default();
        let scan_engine = Arc::new(ScanEngine::new(ScanEngineConfig::default()).await.unwrap());
        let heal_engine = Arc::new(HealEngine::new(HealEngineConfig::default()).await.unwrap());
        let policy_engine = Arc::new(PolicyEngine::new(PolicyEngineConfig::default()).await.unwrap());
        let metrics_collector = Arc::new(Collector::new(CollectorConfig::default()).await.unwrap());
        let metrics_reporter = Arc::new(Reporter::new(ReporterConfig::default()).await.unwrap());
        let metrics_storage = Arc::new(Storage::new(StorageConfig::default()).await.unwrap());

        let server = ApiServer::new(
            config,
            scan_engine,
            heal_engine,
            policy_engine,
            metrics_collector,
            metrics_reporter,
            metrics_storage,
        ).await.unwrap();

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/".to_string(),
            headers: vec![],
            body: None,
            query_params: vec![],
        };

        let response = server.handle_request(request).await.unwrap();
        assert_eq!(response.status_code, 200);
        assert!(response.body.contains("RustFS AHM API"));
    }

    #[tokio::test]
    async fn test_404_endpoint() {
        let config = ApiConfig::default();
        let scan_engine = Arc::new(ScanEngine::new(ScanEngineConfig::default()).await.unwrap());
        let heal_engine = Arc::new(HealEngine::new(HealEngineConfig::default()).await.unwrap());
        let policy_engine = Arc::new(PolicyEngine::new(PolicyEngineConfig::default()).await.unwrap());
        let metrics_collector = Arc::new(Collector::new(CollectorConfig::default()).await.unwrap());
        let metrics_reporter = Arc::new(Reporter::new(ReporterConfig::default()).await.unwrap());
        let metrics_storage = Arc::new(Storage::new(StorageConfig::default()).await.unwrap());

        let server = ApiServer::new(
            config,
            scan_engine,
            heal_engine,
            policy_engine,
            metrics_collector,
            metrics_reporter,
            metrics_storage,
        ).await.unwrap();

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/unknown".to_string(),
            headers: vec![],
            body: None,
            query_params: vec![],
        };

        let response = server.handle_request(request).await.unwrap();
        assert_eq!(response.status_code, 404);
        assert!(response.body.contains("Not Found"));
    }
} 