// Copyright 2024 RustFS Team

use std::sync::Arc;

use tracing::{debug, error, info, warn};

use crate::{
    error::Result,
    heal::HealEngine,
    policy::{ScanPolicyEngine as PolicyEngine},
    scanner::{Engine as ScanEngine},
};

use super::{HttpRequest, HttpResponse};

use serde::{Deserialize, Serialize};

/// Configuration for the status API
#[derive(Debug, Clone)]
pub struct StatusApiConfig {
    /// Whether to enable status API
    pub enabled: bool,
    /// Status API prefix
    pub prefix: String,
    /// Authentication required
    pub require_auth: bool,
    /// Status token
    pub status_token: Option<String>,
    /// Rate limiting for status endpoints
    pub rate_limit_requests_per_minute: u32,
    /// Maximum request body size
    pub max_request_size: usize,
    /// Enable detailed status information
    pub enable_detailed_status: bool,
    /// Status cache TTL in seconds
    pub status_cache_ttl_seconds: u64,
    /// Enable health checks
    pub enable_health_checks: bool,
    /// Health check timeout
    pub health_check_timeout: std::time::Duration,
}

impl Default for StatusApiConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            prefix: "/status".to_string(),
            require_auth: false,
            status_token: None,
            rate_limit_requests_per_minute: 1000,
            max_request_size: 1024 * 1024, // 1 MB
            enable_detailed_status: true,
            status_cache_ttl_seconds: 30, // 30 seconds
            enable_health_checks: true,
            health_check_timeout: std::time::Duration::from_secs(5),
        }
    }
}

/// Status API that provides system status and health information
pub struct StatusApi {
    config: StatusApiConfig,
    scan_engine: Arc<ScanEngine>,
    heal_engine: Arc<HealEngine>,
    policy_engine: Arc<PolicyEngine>,
}

impl StatusApi {
    /// Create a new status API
    pub async fn new(
        config: StatusApiConfig,
        scan_engine: Arc<ScanEngine>,
        heal_engine: Arc<HealEngine>,
        policy_engine: Arc<PolicyEngine>,
    ) -> Result<Self> {
        Ok(Self {
            config,
            scan_engine,
            heal_engine,
            policy_engine,
        })
    }

    /// Get the configuration
    pub fn config(&self) -> &StatusApiConfig {
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
            // Basic status
            "/status" => self.get_status(request).await,
            "/status/health" => self.get_health_status(request).await,
            "/status/overview" => self.get_overview_status(request).await,
            
            // Component status
            "/status/scan" => self.get_scan_status(request).await,
            "/status/heal" => self.get_heal_status(request).await,
            "/status/policy" => self.get_policy_status(request).await,
            
            // Detailed status
            "/status/detailed" => self.get_detailed_status(request).await,
            "/status/components" => self.get_components_status(request).await,
            "/status/resources" => self.get_resources_status(request).await,
            
            // Health checks
            "/status/health/check" => self.perform_health_check(request).await,
            "/status/health/readiness" => self.get_readiness_status(request).await,
            "/status/health/liveness" => self.get_liveness_status(request).await,
            
            // System information
            "/status/info" => self.get_system_info(request).await,
            "/status/version" => self.get_version_info(request).await,
            "/status/uptime" => self.get_uptime_info(request).await,
            
            // Default 404
            _ => Ok(HttpResponse {
                status_code: 404,
                headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                body: serde_json::json!({
                    "error": "Not Found",
                    "message": "Status endpoint not found"
                }).to_string(),
            }),
        }
    }

    /// Authenticate request
    async fn authenticate_request(&self, request: &HttpRequest) -> Result<bool> {
        if let Some(token) = &self.config.status_token {
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

    /// Get basic status
    async fn get_status(&self, _request: HttpRequest) -> Result<HttpResponse> {
        let scan_status = self.scan_engine.status().await;
        let heal_status = self.heal_engine.get_status().await;
        
        let overall_status = if scan_status == crate::scanner::Status::Running && heal_status == crate::heal::Status::Running {
            "healthy"
        } else if scan_status == crate::scanner::Status::Stopped && heal_status == crate::heal::Status::Stopped {
            "stopped"
        } else {
            "degraded"
        };

        Ok(HttpResponse {
            status_code: 200,
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            body: serde_json::json!({
                "status": "success",
                "overall_status": overall_status,
                "components": {
                    "scan": scan_status,
                    "heal": heal_status
                },
                "timestamp": chrono::Utc::now().to_rfc3339()
            }).to_string(),
        })
    }

    /// Get health status
    async fn get_health_status(&self, _request: HttpRequest) -> Result<HttpResponse> {
        let scan_status = self.scan_engine.status().await;
        let heal_status = self.heal_engine.get_status().await;
        
        let is_healthy = scan_status == crate::scanner::Status::Running && heal_status == crate::heal::Status::Running;
        let status_code = if is_healthy { 200 } else { 503 };

        Ok(HttpResponse {
            status_code,
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            body: serde_json::json!({
                "status": if is_healthy { "healthy" } else { "unhealthy" },
                "components": {
                    "scan": {
                        "status": scan_status,
                        "healthy": scan_status == crate::scanner::Status::Running
                    },
                    "heal": {
                        "status": heal_status,
                        "healthy": heal_status == crate::heal::Status::Running
                    }
                },
                "timestamp": chrono::Utc::now().to_rfc3339()
            }).to_string(),
        })
    }

    /// Get overview status
    async fn get_overview_status(&self, _request: HttpRequest) -> Result<HttpResponse> {
        let scan_status = self.scan_engine.status().await;
        let heal_status = self.heal_engine.get_status().await;
        
        let scan_config = self.scan_engine.get_config().await;
        let heal_config = self.heal_engine.get_config().await;

        Ok(HttpResponse {
            status_code: 200,
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            body: serde_json::json!({
                "status": "success",
                "overview": {
                    "scan": {
                        "status": scan_status,
                        "enabled": scan_config.enabled,
                        "scan_interval": scan_config.scan_interval.as_secs()
                    },
                    "heal": {
                        "status": heal_status,
                        "enabled": heal_config.auto_heal_enabled,
                        "max_workers": heal_config.max_workers
                    }
                },
                "timestamp": chrono::Utc::now().to_rfc3339()
            }).to_string(),
        })
    }

    /// Get scan status
    async fn get_scan_status(&self, _request: HttpRequest) -> Result<HttpResponse> {
        let status = self.scan_engine.status().await;
        let config = self.scan_engine.get_config().await;
        
        Ok(HttpResponse {
            status_code: 200,
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            body: serde_json::json!({
                "status": "success",
                "scan": {
                    "status": status,
                    "enabled": config.enabled,
                    "scan_interval": config.scan_interval.as_secs(),
                    "max_concurrent_scans": config.max_concurrent_scans,
                    "scan_paths": config.scan_paths,
                    "bandwidth_limit": config.bandwidth_limit
                },
                "timestamp": chrono::Utc::now().to_rfc3339()
            }).to_string(),
        })
    }

    /// Get heal status
    async fn get_heal_status(&self, _request: HttpRequest) -> Result<HttpResponse> {
        let status = self.heal_engine.get_status().await;
        let config = self.heal_engine.get_config().await;
        
        Ok(HttpResponse {
            status_code: 200,
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            body: serde_json::json!({
                "status": "success",
                "heal": {
                    "status": status,
                    "enabled": config.auto_heal_enabled,
                    "max_workers": config.max_workers,
                    "repair_timeout": config.repair_timeout.as_secs(),
                    "retry_attempts": config.max_retry_attempts,
                    "priority_queue_size": config.max_queue_size
                },
                "timestamp": chrono::Utc::now().to_rfc3339()
            }).to_string(),
        })
    }

    /// Get policy status
    async fn get_policy_status(&self, _request: HttpRequest) -> Result<HttpResponse> {
        let policies = self.policy_engine.list_policies().await?;
        let config = self.policy_engine.get_config().await;
        
        Ok(HttpResponse {
            status_code: 200,
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            body: serde_json::json!({
                "status": "success",
                "policy": {
                    "enabled": config.enabled,
                    "total_policies": policies.len(),
                    "policies": policies,
                    "evaluation_timeout": config.evaluation_timeout.as_secs(),
                    "cache_enabled": config.cache_enabled
                },
                "timestamp": chrono::Utc::now().to_rfc3339()
            }).to_string(),
        })
    }

    /// Get detailed status
    async fn get_detailed_status(&self, _request: HttpRequest) -> Result<HttpResponse> {
        if !self.config.enable_detailed_status {
            return Ok(HttpResponse {
                status_code: 403,
                headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                body: serde_json::json!({
                    "error": "Forbidden",
                    "message": "Detailed status is disabled"
                }).to_string(),
            });
        }

        let scan_status = self.scan_engine.status().await;
        let heal_status = self.heal_engine.get_status().await;
        let scan_config = self.scan_engine.get_config().await;
        let heal_config = self.heal_engine.get_config().await;
        let policy_config = self.policy_engine.get_config().await;

        Ok(HttpResponse {
            status_code: 200,
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            body: serde_json::json!({
                "status": "success",
                "detailed_status": {
                    "scan": {
                        "status": scan_status,
                        "config": scan_config
                    },
                    "heal": {
                        "status": heal_status,
                        "config": heal_config
                    },
                    "policy": {
                        "config": policy_config
                    }
                },
                "timestamp": chrono::Utc::now().to_rfc3339()
            }).to_string(),
        })
    }

    /// Get components status
    async fn get_components_status(&self, _request: HttpRequest) -> Result<HttpResponse> {
        let scan_status = self.scan_engine.status().await;
        let heal_status = self.heal_engine.get_status().await;
        
        let components = vec![
            serde_json::json!({
                "name": "scan_engine",
                "status": scan_status,
                "healthy": scan_status == crate::scanner::Status::Running,
                "type": "scanner"
            }),
            serde_json::json!({
                "name": "heal_engine",
                "status": heal_status,
                "healthy": heal_status == crate::heal::Status::Running,
                "type": "healer"
            }),
            serde_json::json!({
                "name": "policy_engine",
                "status": "running",
                "healthy": true,
                "type": "policy"
            })
        ];

        Ok(HttpResponse {
            status_code: 200,
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            body: serde_json::json!({
                "status": "success",
                "components": components,
                "timestamp": chrono::Utc::now().to_rfc3339()
            }).to_string(),
        })
    }

    /// Get resources status
    async fn get_resources_status(&self, _request: HttpRequest) -> Result<HttpResponse> {
        // In a real implementation, this would collect actual resource usage
        // For now, we'll return simulated data
        let resources = serde_json::json!({
            "cpu": {
                "usage_percent": 25.5,
                "cores": 8,
                "load_average": 0.75
            },
            "memory": {
                "usage_percent": 60.2,
                "total_bytes": 8589934592, // 8 GB
                "available_bytes": 3422552064 // ~3.2 GB
            },
            "disk": {
                "usage_percent": 45.8,
                "total_bytes": 107374182400, // 100 GB
                "available_bytes": 58133032960 // ~54 GB
            },
            "network": {
                "bytes_received_per_sec": 1048576, // 1 MB/s
                "bytes_sent_per_sec": 524288 // 512 KB/s
            }
        });

        Ok(HttpResponse {
            status_code: 200,
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            body: serde_json::json!({
                "status": "success",
                "resources": resources,
                "timestamp": chrono::Utc::now().to_rfc3339()
            }).to_string(),
        })
    }

    /// Perform health checks
    async fn perform_health_checks(&self) -> Result<Vec<HealthCheckResult>> {
        let mut checks = Vec::new();
        let start_time = std::time::Instant::now();

        // Check scan engine
        let scan_start = std::time::Instant::now();
        let scan_status = self.scan_engine.status().await;
        let scan_duration = scan_start.elapsed();
        checks.push(HealthCheckResult {
            name: "scan_engine".to_string(),
            healthy: scan_status == crate::scanner::Status::Running,
            message: format!("Scan engine status: {:?}", scan_status),
            duration_ms: scan_duration.as_millis() as u64,
        });

        // Check heal engine
        let heal_start = std::time::Instant::now();
        let heal_status = self.heal_engine.get_status().await;
        let heal_duration = heal_start.elapsed();
        checks.push(HealthCheckResult {
            name: "heal_engine".to_string(),
            healthy: heal_status == crate::heal::Status::Running,
            message: format!("Heal engine status: {:?}", heal_status),
            duration_ms: heal_duration.as_millis() as u64,
        });

        // Check policy engine
        let policy_start = std::time::Instant::now();
        let policy_result = self.policy_engine.list_policies().await;
        let policy_duration = policy_start.elapsed();
        checks.push(HealthCheckResult {
            name: "policy_engine".to_string(),
            healthy: policy_result.is_ok(),
            message: if policy_result.is_ok() {
                "Policy engine is responding".to_string()
            } else {
                format!("Policy engine error: {:?}", policy_result.unwrap_err())
            },
            duration_ms: policy_duration.as_millis() as u64,
        });

        let total_duration = start_time.elapsed();
        info!("Health checks completed in {:?}", total_duration);

        Ok(checks)
    }

    /// Perform health check (alias for perform_health_checks)
    async fn perform_health_check(&self, _request: HttpRequest) -> Result<HttpResponse> {
        let checks = self.perform_health_checks().await?;
        let all_healthy = checks.iter().all(|check| check.healthy);
        let check_time = std::time::Instant::now().elapsed();

        Ok(HttpResponse {
            status_code: if all_healthy { 200 } else { 503 },
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            body: serde_json::json!({
                "status": if all_healthy { "healthy" } else { "unhealthy" },
                "checks": checks,
                "check_time_ms": check_time.as_millis(),
                "timestamp": chrono::Utc::now().to_rfc3339()
            }).to_string(),
        })
    }

    /// Get readiness status
    async fn get_readiness_status(&self, _request: HttpRequest) -> Result<HttpResponse> {
        let scan_status = self.scan_engine.status().await;
        let heal_status = self.heal_engine.get_status().await;
        
        let is_ready = scan_status == crate::scanner::Status::Running && heal_status == crate::heal::Status::Running;
        let status_code = if is_ready { 200 } else { 503 };

        Ok(HttpResponse {
            status_code,
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            body: serde_json::json!({
                "status": if is_ready { "ready" } else { "not_ready" },
                "components": {
                    "scan_engine": scan_status == crate::scanner::Status::Running,
                    "heal_engine": heal_status == crate::heal::Status::Running
                },
                "timestamp": chrono::Utc::now().to_rfc3339()
            }).to_string(),
        })
    }

    /// Get liveness status
    async fn get_liveness_status(&self, _request: HttpRequest) -> Result<HttpResponse> {
        // Liveness check is simple - if we can respond, we're alive
        Ok(HttpResponse {
            status_code: 200,
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            body: serde_json::json!({
                "status": "alive",
                "timestamp": chrono::Utc::now().to_rfc3339()
            }).to_string(),
        })
    }

    /// Get system information
    async fn get_system_info(&self, _request: HttpRequest) -> Result<HttpResponse> {
        let system_info = serde_json::json!({
            "service": "RustFS AHM",
            "version": env!("CARGO_PKG_VERSION"),
            "system_info": {
                "rust_version": option_env!("RUST_VERSION").unwrap_or("unknown"),
                "target_arch": option_env!("TARGET_ARCH").unwrap_or("unknown"),
                "target_os": option_env!("TARGET_OS").unwrap_or("unknown"),
                "build_time": option_env!("VERGEN_BUILD_TIMESTAMP").unwrap_or("unknown"),
                "git_commit": option_env!("VERGEN_GIT_SHA").unwrap_or("unknown"),
                "git_branch": option_env!("VERGEN_GIT_BRANCH").unwrap_or("unknown"),
            },
        });

        Ok(HttpResponse {
            status_code: 200,
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            body: serde_json::json!({
                "status": "success",
                "system_info": system_info,
                "timestamp": chrono::Utc::now().to_rfc3339()
            }).to_string(),
        })
    }

    /// Get version information
    async fn get_version_info(&self, _request: HttpRequest) -> Result<HttpResponse> {
        Ok(HttpResponse {
            status_code: 200,
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            body: serde_json::json!({
                "status": "success",
                "version": env!("CARGO_PKG_VERSION"),
                "build_time": option_env!("VERGEN_BUILD_TIMESTAMP").unwrap_or("unknown"),
                "git_commit": option_env!("VERGEN_GIT_SHA").unwrap_or("unknown"),
                "timestamp": chrono::Utc::now().to_rfc3339()
            }).to_string(),
        })
    }

    /// Get uptime information
    async fn get_uptime_info(&self, _request: HttpRequest) -> Result<HttpResponse> {
        // In a real implementation, this would track actual uptime
        // For now, we'll return simulated data
        let uptime_seconds = 3600; // 1 hour
        let uptime_duration = std::time::Duration::from_secs(uptime_seconds);

        Ok(HttpResponse {
            status_code: 200,
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            body: serde_json::json!({
                "status": "success",
                "uptime": {
                    "seconds": uptime_seconds,
                    "duration": format!("{:?}", uptime_duration),
                    "start_time": chrono::Utc::now() - chrono::Duration::seconds(uptime_seconds as i64)
                },
                "timestamp": chrono::Utc::now().to_rfc3339()
            }).to_string(),
        })
    }
}

/// Health check result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckResult {
    pub name: String,
    pub healthy: bool,
    pub message: String,
    pub duration_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        heal::HealEngineConfig,
        policy::PolicyEngineConfig,
        scanner::ScanEngineConfig,
    };

    #[tokio::test]
    async fn test_status_api_creation() {
        let config = StatusApiConfig::default();
        let scan_engine = Arc::new(ScanEngine::new(ScanEngineConfig::default()).await.unwrap());
        let heal_engine = Arc::new(HealEngine::new(HealEngineConfig::default()).await.unwrap());
        let policy_engine = Arc::new(PolicyEngine::new(PolicyEngineConfig::default()).await.unwrap());

        let status_api = StatusApi::new(config, scan_engine, heal_engine, policy_engine).await.unwrap();
        
        assert!(status_api.config().enabled);
        assert_eq!(status_api.config().prefix, "/status");
    }

    #[tokio::test]
    async fn test_basic_status() {
        let config = StatusApiConfig::default();
        let scan_engine = Arc::new(ScanEngine::new(ScanEngineConfig::default()).await.unwrap());
        let heal_engine = Arc::new(HealEngine::new(HealEngineConfig::default()).await.unwrap());
        let policy_engine = Arc::new(PolicyEngine::new(PolicyEngineConfig::default()).await.unwrap());

        let status_api = StatusApi::new(config, scan_engine, heal_engine, policy_engine).await.unwrap();

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/status".to_string(),
            headers: vec![],
            body: None,
            query_params: vec![],
        };

        let response = status_api.handle_request(request).await.unwrap();
        assert_eq!(response.status_code, 200);
        assert!(response.body.contains("overall_status"));
    }

    #[tokio::test]
    async fn test_health_status() {
        let config = StatusApiConfig::default();
        let scan_engine = Arc::new(ScanEngine::new(ScanEngineConfig::default()).await.unwrap());
        let heal_engine = Arc::new(HealEngine::new(HealEngineConfig::default()).await.unwrap());
        let policy_engine = Arc::new(PolicyEngine::new(PolicyEngineConfig::default()).await.unwrap());

        let status_api = StatusApi::new(config, scan_engine, heal_engine, policy_engine).await.unwrap();

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/status/health".to_string(),
            headers: vec![],
            body: None,
            query_params: vec![],
        };

        let response = status_api.handle_request(request).await.unwrap();
        assert_eq!(response.status_code, 200);
        assert!(response.body.contains("status"));
    }

    #[tokio::test]
    async fn test_scan_status() {
        let config = StatusApiConfig::default();
        let scan_engine = Arc::new(ScanEngine::new(ScanEngineConfig::default()).await.unwrap());
        let heal_engine = Arc::new(HealEngine::new(HealEngineConfig::default()).await.unwrap());
        let policy_engine = Arc::new(PolicyEngine::new(PolicyEngineConfig::default()).await.unwrap());

        let status_api = StatusApi::new(config, scan_engine, heal_engine, policy_engine).await.unwrap();

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/status/scan".to_string(),
            headers: vec![],
            body: None,
            query_params: vec![],
        };

        let response = status_api.handle_request(request).await.unwrap();
        assert_eq!(response.status_code, 200);
        assert!(response.body.contains("scan"));
    }

    #[tokio::test]
    async fn test_heal_status() {
        let config = StatusApiConfig::default();
        let scan_engine = Arc::new(ScanEngine::new(ScanEngineConfig::default()).await.unwrap());
        let heal_engine = Arc::new(HealEngine::new(HealEngineConfig::default()).await.unwrap());
        let policy_engine = Arc::new(PolicyEngine::new(PolicyEngineConfig::default()).await.unwrap());

        let status_api = StatusApi::new(config, scan_engine, heal_engine, policy_engine).await.unwrap();

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/status/heal".to_string(),
            headers: vec![],
            body: None,
            query_params: vec![],
        };

        let response = status_api.handle_request(request).await.unwrap();
        assert_eq!(response.status_code, 200);
        assert!(response.body.contains("heal"));
    }

    #[tokio::test]
    async fn test_version_info() {
        let config = StatusApiConfig::default();
        let scan_engine = Arc::new(ScanEngine::new(ScanEngineConfig::default()).await.unwrap());
        let heal_engine = Arc::new(HealEngine::new(HealEngineConfig::default()).await.unwrap());
        let policy_engine = Arc::new(PolicyEngine::new(PolicyEngineConfig::default()).await.unwrap());

        let status_api = StatusApi::new(config, scan_engine, heal_engine, policy_engine).await.unwrap();

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/status/version".to_string(),
            headers: vec![],
            body: None,
            query_params: vec![],
        };

        let response = status_api.handle_request(request).await.unwrap();
        assert_eq!(response.status_code, 200);
        assert!(response.body.contains("version"));
    }

    #[tokio::test]
    async fn test_liveness_status() {
        let config = StatusApiConfig::default();
        let scan_engine = Arc::new(ScanEngine::new(ScanEngineConfig::default()).await.unwrap());
        let heal_engine = Arc::new(HealEngine::new(HealEngineConfig::default()).await.unwrap());
        let policy_engine = Arc::new(PolicyEngine::new(PolicyEngineConfig::default()).await.unwrap());

        let status_api = StatusApi::new(config, scan_engine, heal_engine, policy_engine).await.unwrap();

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/status/health/liveness".to_string(),
            headers: vec![],
            body: None,
            query_params: vec![],
        };

        let response = status_api.handle_request(request).await.unwrap();
        assert_eq!(response.status_code, 200);
        assert!(response.body.contains("alive"));
    }
} 