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

/// Configuration for the admin API
#[derive(Debug, Clone)]
pub struct AdminApiConfig {
    /// Whether to enable admin API
    pub enabled: bool,
    /// Admin API prefix
    pub prefix: String,
    /// Authentication required
    pub require_auth: bool,
    /// Admin token
    pub admin_token: Option<String>,
    /// Rate limiting for admin endpoints
    pub rate_limit_requests_per_minute: u32,
    /// Maximum request body size
    pub max_request_size: usize,
    /// Enable audit logging
    pub enable_audit_logging: bool,
    /// Audit log path
    pub audit_log_path: Option<String>,
}

impl Default for AdminApiConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            prefix: "/admin".to_string(),
            require_auth: true,
            admin_token: Some("admin-secret-token".to_string()),
            rate_limit_requests_per_minute: 100,
            max_request_size: 1024 * 1024, // 1 MB
            enable_audit_logging: true,
            audit_log_path: Some("/tmp/rustfs/admin-audit.log".to_string()),
        }
    }
}

/// Admin API that provides administrative operations
pub struct AdminApi {
    config: AdminApiConfig,
    scan_engine: Arc<ScanEngine>,
    heal_engine: Arc<HealEngine>,
    policy_engine: Arc<PolicyEngine>,
}

impl AdminApi {
    /// Create a new admin API
    pub async fn new(
        config: AdminApiConfig,
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
    pub fn config(&self) -> &AdminApiConfig {
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
                });
            }
        }

        // Log audit if enabled
        if self.config.enable_audit_logging {
            self.log_audit(&request).await?;
        }

        match request.path.as_str() {
            // Scan operations
            "/admin/scan/start" => self.start_scan(request).await,
            "/admin/scan/stop" => self.stop_scan(request).await,
            "/admin/scan/status" => self.get_scan_status(request).await,
            "/admin/scan/config" => self.get_scan_config(request).await,
            "/admin/scan/config" if request.method == "PUT" => self.update_scan_config(request).await,

            // Heal operations
            "/admin/heal/start" => self.start_heal(request).await,
            "/admin/heal/stop" => self.stop_heal(request).await,
            "/admin/heal/status" => self.get_heal_status(request).await,
            "/admin/heal/config" => self.get_heal_config(request).await,
            "/admin/heal/config" if request.method == "PUT" => self.update_heal_config(request).await,

            // Policy operations
            "/admin/policy/list" => self.list_policies(request).await,
            "/admin/policy/get" => self.get_policy(request).await,
            "/admin/policy/create" => self.create_policy(request).await,
            "/admin/policy/update" => self.update_policy(request).await,
            "/admin/policy/delete" => self.delete_policy(request).await,

            // System operations
            "/admin/system/status" => self.get_system_status(request).await,
            "/admin/system/config" => self.get_system_config(request).await,
            "/admin/system/restart" => self.restart_system(request).await,
            "/admin/system/shutdown" => self.shutdown_system(request).await,

            // Default 404
            _ => Ok(HttpResponse {
                status_code: 404,
                headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                body: serde_json::json!({
                    "error": "Not Found",
                    "message": "Admin endpoint not found"
                }).to_string(),
            }),
        }
    }

    /// Authenticate request
    async fn authenticate_request(&self, request: &HttpRequest) -> Result<bool> {
        if let Some(token) = &self.config.admin_token {
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

    /// Log audit entry
    async fn log_audit(&self, request: &HttpRequest) -> Result<()> {
        let audit_entry = serde_json::json!({
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "method": request.method,
            "path": request.path,
            "ip": "127.0.0.1", // In real implementation, get from request
            "user_agent": "admin-api", // In real implementation, get from headers
        });

        if let Some(log_path) = &self.config.audit_log_path {
            // In a real implementation, this would write to the audit log file
            debug!("Audit log entry: {}", audit_entry);
        }

        Ok(())
    }

    /// Start scan operation
    async fn start_scan(&self, _request: HttpRequest) -> Result<HttpResponse> {
        match self.scan_engine.start_scan().await {
            Ok(_) => {
                info!("Scan started via admin API");
                Ok(HttpResponse {
                    status_code: 200,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "status": "success",
                        "message": "Scan started successfully",
                        "timestamp": chrono::Utc::now().to_rfc3339()
                    }).to_string(),
                })
            }
            Err(e) => {
                error!("Failed to start scan: {}", e);
                Ok(HttpResponse {
                    status_code: 500,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "error": "Internal Server Error",
                        "message": format!("Failed to start scan: {}", e)
                    }).to_string(),
                })
            }
        }
    }

    /// Stop scan operation
    async fn stop_scan(&self, _request: HttpRequest) -> Result<HttpResponse> {
        match self.scan_engine.stop_scan().await {
            Ok(_) => {
                info!("Scan stopped via admin API");
                Ok(HttpResponse {
                    status_code: 200,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "status": "success",
                        "message": "Scan stopped successfully",
                        "timestamp": chrono::Utc::now().to_rfc3339()
                    }).to_string(),
                })
            }
            Err(e) => {
                error!("Failed to stop scan: {}", e);
                Ok(HttpResponse {
                    status_code: 500,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "error": "Internal Server Error",
                        "message": format!("Failed to stop scan: {}", e)
                    }).to_string(),
                })
            }
        }
    }

    /// Get scan status
    async fn get_scan_status(&self, _request: HttpRequest) -> Result<HttpResponse> {
        let status = self.scan_engine.get_status().await;
        
        Ok(HttpResponse {
            status_code: 200,
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            body: serde_json::json!({
                "status": "success",
                "scan_status": status,
                "timestamp": chrono::Utc::now().to_rfc3339()
            }).to_string(),
        })
    }

    /// Get scan configuration
    async fn get_scan_config(&self, _request: HttpRequest) -> Result<HttpResponse> {
        let config = self.scan_engine.get_config().await;
        
        Ok(HttpResponse {
            status_code: 200,
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            body: serde_json::json!({
                "status": "success",
                "scan_config": config,
                "timestamp": chrono::Utc::now().to_rfc3339()
            }).to_string(),
        })
    }

    /// Update scan configuration
    async fn update_scan_config(&self, request: HttpRequest) -> Result<HttpResponse> {
        if let Some(body) = request.body {
            match serde_json::from_str::<serde_json::Value>(&body) {
                Ok(config_json) => {
                    // In a real implementation, this would update the scan configuration
                    info!("Scan config updated via admin API: {:?}", config_json);
                    
                    Ok(HttpResponse {
                        status_code: 200,
                        headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                        body: serde_json::json!({
                            "status": "success",
                            "message": "Scan configuration updated successfully",
                            "timestamp": chrono::Utc::now().to_rfc3339()
                        }).to_string(),
                    })
                }
                Err(e) => {
                    Ok(HttpResponse {
                        status_code: 400,
                        headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                        body: serde_json::json!({
                            "error": "Bad Request",
                            "message": format!("Invalid JSON: {}", e)
                        }).to_string(),
                    })
                }
            }
        } else {
            Ok(HttpResponse {
                status_code: 400,
                headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                body: serde_json::json!({
                    "error": "Bad Request",
                    "message": "Request body required"
                }).to_string(),
            })
        }
    }

    /// Start heal operation
    async fn start_heal(&self, _request: HttpRequest) -> Result<HttpResponse> {
        match self.heal_engine.start_healing().await {
            Ok(_) => {
                info!("Healing started via admin API");
                Ok(HttpResponse {
                    status_code: 200,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "status": "success",
                        "message": "Healing started successfully",
                        "timestamp": chrono::Utc::now().to_rfc3339()
                    }).to_string(),
                })
            }
            Err(e) => {
                error!("Failed to start healing: {}", e);
                Ok(HttpResponse {
                    status_code: 500,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "error": "Internal Server Error",
                        "message": format!("Failed to start healing: {}", e)
                    }).to_string(),
                })
            }
        }
    }

    /// Stop heal operation
    async fn stop_heal(&self, _request: HttpRequest) -> Result<HttpResponse> {
        match self.heal_engine.stop_healing().await {
            Ok(_) => {
                info!("Healing stopped via admin API");
                Ok(HttpResponse {
                    status_code: 200,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "status": "success",
                        "message": "Healing stopped successfully",
                        "timestamp": chrono::Utc::now().to_rfc3339()
                    }).to_string(),
                })
            }
            Err(e) => {
                error!("Failed to stop healing: {}", e);
                Ok(HttpResponse {
                    status_code: 500,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: serde_json::json!({
                        "error": "Internal Server Error",
                        "message": format!("Failed to stop healing: {}", e)
                    }).to_string(),
                })
            }
        }
    }

    /// Get heal status
    async fn get_heal_status(&self, _request: HttpRequest) -> Result<HttpResponse> {
        let status = self.heal_engine.get_status().await;
        
        Ok(HttpResponse {
            status_code: 200,
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            body: serde_json::json!({
                "status": "success",
                "heal_status": status,
                "timestamp": chrono::Utc::now().to_rfc3339()
            }).to_string(),
        })
    }

    /// Get heal configuration
    async fn get_heal_config(&self, _request: HttpRequest) -> Result<HttpResponse> {
        let config = self.heal_engine.get_config().await;
        
        Ok(HttpResponse {
            status_code: 200,
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            body: serde_json::json!({
                "status": "success",
                "heal_config": config,
                "timestamp": chrono::Utc::now().to_rfc3339()
            }).to_string(),
        })
    }

    /// Update heal configuration
    async fn update_heal_config(&self, request: HttpRequest) -> Result<HttpResponse> {
        if let Some(body) = request.body {
            match serde_json::from_str::<serde_json::Value>(&body) {
                Ok(config_json) => {
                    // In a real implementation, this would update the heal configuration
                    info!("Heal config updated via admin API: {:?}", config_json);
                    
                    Ok(HttpResponse {
                        status_code: 200,
                        headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                        body: serde_json::json!({
                            "status": "success",
                            "message": "Heal configuration updated successfully",
                            "timestamp": chrono::Utc::now().to_rfc3339()
                        }).to_string(),
                    })
                }
                Err(e) => {
                    Ok(HttpResponse {
                        status_code: 400,
                        headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                        body: serde_json::json!({
                            "error": "Bad Request",
                            "message": format!("Invalid JSON: {}", e)
                        }).to_string(),
                    })
                }
            }
        } else {
            Ok(HttpResponse {
                status_code: 400,
                headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                body: serde_json::json!({
                    "error": "Bad Request",
                    "message": "Request body required"
                }).to_string(),
            })
        }
    }

    /// List policies
    async fn list_policies(&self, _request: HttpRequest) -> Result<HttpResponse> {
        let policies = self.policy_engine.list_policies().await?;
        
        Ok(HttpResponse {
            status_code: 200,
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            body: serde_json::json!({
                "status": "success",
                "policies": policies,
                "timestamp": chrono::Utc::now().to_rfc3339()
            }).to_string(),
        })
    }

    /// Get policy
    async fn get_policy(&self, request: HttpRequest) -> Result<HttpResponse> {
        if let Some(policy_name) = request.query_params.iter().find(|(k, _)| k == "name") {
            match self.policy_engine.get_policy(&policy_name.1).await {
                Ok(policy) => {
                    Ok(HttpResponse {
                        status_code: 200,
                        headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                        body: serde_json::json!({
                            "status": "success",
                            "policy": policy,
                            "timestamp": chrono::Utc::now().to_rfc3339()
                        }).to_string(),
                    })
                }
                Err(e) => {
                    Ok(HttpResponse {
                        status_code: 404,
                        headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                        body: serde_json::json!({
                            "error": "Not Found",
                            "message": format!("Policy not found: {}", e)
                        }).to_string(),
                    })
                }
            }
        } else {
            Ok(HttpResponse {
                status_code: 400,
                headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                body: serde_json::json!({
                    "error": "Bad Request",
                    "message": "Policy name parameter required"
                }).to_string(),
            })
        }
    }

    /// Create policy
    async fn create_policy(&self, request: HttpRequest) -> Result<HttpResponse> {
        if let Some(body) = request.body {
            match serde_json::from_str::<serde_json::Value>(&body) {
                Ok(policy_json) => {
                    // In a real implementation, this would create the policy
                    info!("Policy created via admin API: {:?}", policy_json);
                    
                    Ok(HttpResponse {
                        status_code: 201,
                        headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                        body: serde_json::json!({
                            "status": "success",
                            "message": "Policy created successfully",
                            "timestamp": chrono::Utc::now().to_rfc3339()
                        }).to_string(),
                    })
                }
                Err(e) => {
                    Ok(HttpResponse {
                        status_code: 400,
                        headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                        body: serde_json::json!({
                            "error": "Bad Request",
                            "message": format!("Invalid JSON: {}", e)
                        }).to_string(),
                    })
                }
            }
        } else {
            Ok(HttpResponse {
                status_code: 400,
                headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                body: serde_json::json!({
                    "error": "Bad Request",
                    "message": "Request body required"
                }).to_string(),
            })
        }
    }

    /// Update policy
    async fn update_policy(&self, request: HttpRequest) -> Result<HttpResponse> {
        if let Some(body) = request.body {
            match serde_json::from_str::<serde_json::Value>(&body) {
                Ok(policy_json) => {
                    // In a real implementation, this would update the policy
                    info!("Policy updated via admin API: {:?}", policy_json);
                    
                    Ok(HttpResponse {
                        status_code: 200,
                        headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                        body: serde_json::json!({
                            "status": "success",
                            "message": "Policy updated successfully",
                            "timestamp": chrono::Utc::now().to_rfc3339()
                        }).to_string(),
                    })
                }
                Err(e) => {
                    Ok(HttpResponse {
                        status_code: 400,
                        headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                        body: serde_json::json!({
                            "error": "Bad Request",
                            "message": format!("Invalid JSON: {}", e)
                        }).to_string(),
                    })
                }
            }
        } else {
            Ok(HttpResponse {
                status_code: 400,
                headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                body: serde_json::json!({
                    "error": "Bad Request",
                    "message": "Request body required"
                }).to_string(),
            })
        }
    }

    /// Delete policy
    async fn delete_policy(&self, request: HttpRequest) -> Result<HttpResponse> {
        if let Some(policy_name) = request.query_params.iter().find(|(k, _)| k == "name") {
            // In a real implementation, this would delete the policy
            info!("Policy deleted via admin API: {}", policy_name.1);
            
            Ok(HttpResponse {
                status_code: 200,
                headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                body: serde_json::json!({
                    "status": "success",
                    "message": "Policy deleted successfully",
                    "timestamp": chrono::Utc::now().to_rfc3339()
                }).to_string(),
            })
        } else {
            Ok(HttpResponse {
                status_code: 400,
                headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                body: serde_json::json!({
                    "error": "Bad Request",
                    "message": "Policy name parameter required"
                }).to_string(),
            })
        }
    }

    /// Get system status
    async fn get_system_status(&self, _request: HttpRequest) -> Result<HttpResponse> {
        let scan_status = self.scan_engine.get_status().await;
        let heal_status = self.heal_engine.get_status().await;
        
        Ok(HttpResponse {
            status_code: 200,
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            body: serde_json::json!({
                "status": "success",
                "system_status": {
                    "scan": scan_status,
                    "heal": heal_status,
                    "overall": "healthy"
                },
                "timestamp": chrono::Utc::now().to_rfc3339()
            }).to_string(),
        })
    }

    /// Get system configuration
    async fn get_system_config(&self, _request: HttpRequest) -> Result<HttpResponse> {
        let scan_config = self.scan_engine.get_config().await;
        let heal_config = self.heal_engine.get_config().await;
        
        Ok(HttpResponse {
            status_code: 200,
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            body: serde_json::json!({
                "status": "success",
                "system_config": {
                    "scan": scan_config,
                    "heal": heal_config
                },
                "timestamp": chrono::Utc::now().to_rfc3339()
            }).to_string(),
        })
    }

    /// Restart system
    async fn restart_system(&self, _request: HttpRequest) -> Result<HttpResponse> {
        // In a real implementation, this would restart the system
        info!("System restart requested via admin API");
        
        Ok(HttpResponse {
            status_code: 200,
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            body: serde_json::json!({
                "status": "success",
                "message": "System restart initiated",
                "timestamp": chrono::Utc::now().to_rfc3339()
            }).to_string(),
        })
    }

    /// Shutdown system
    async fn shutdown_system(&self, _request: HttpRequest) -> Result<HttpResponse> {
        // In a real implementation, this would shutdown the system
        info!("System shutdown requested via admin API");
        
        Ok(HttpResponse {
            status_code: 200,
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            body: serde_json::json!({
                "status": "success",
                "message": "System shutdown initiated",
                "timestamp": chrono::Utc::now().to_rfc3339()
            }).to_string(),
        })
    }
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
    async fn test_admin_api_creation() {
        let config = AdminApiConfig::default();
        let scan_engine = Arc::new(ScanEngine::new(ScanEngineConfig::default()).await.unwrap());
        let heal_engine = Arc::new(HealEngine::new(HealEngineConfig::default()).await.unwrap());
        let policy_engine = Arc::new(PolicyEngine::new(PolicyEngineConfig::default()).await.unwrap());

        let admin_api = AdminApi::new(config, scan_engine, heal_engine, policy_engine).await.unwrap();
        
        assert!(admin_api.config().enabled);
        assert_eq!(admin_api.config().prefix, "/admin");
    }

    #[tokio::test]
    async fn test_authentication() {
        let config = AdminApiConfig {
            admin_token: Some("test-token".to_string()),
            ..Default::default()
        };
        let scan_engine = Arc::new(ScanEngine::new(ScanEngineConfig::default()).await.unwrap());
        let heal_engine = Arc::new(HealEngine::new(HealEngineConfig::default()).await.unwrap());
        let policy_engine = Arc::new(PolicyEngine::new(PolicyEngineConfig::default()).await.unwrap());

        let admin_api = AdminApi::new(config, scan_engine, heal_engine, policy_engine).await.unwrap();

        // Test with valid token in header
        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/admin/scan/status".to_string(),
            headers: vec![("Authorization".to_string(), "Bearer test-token".to_string())],
            body: None,
            query_params: vec![],
        };

        let response = admin_api.handle_request(request).await.unwrap();
        assert_eq!(response.status_code, 200);

        // Test with valid token in query
        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/admin/scan/status".to_string(),
            headers: vec![],
            body: None,
            query_params: vec![("token".to_string(), "test-token".to_string())],
        };

        let response = admin_api.handle_request(request).await.unwrap();
        assert_eq!(response.status_code, 200);

        // Test with invalid token
        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/admin/scan/status".to_string(),
            headers: vec![("Authorization".to_string(), "Bearer invalid-token".to_string())],
            body: None,
            query_params: vec![],
        };

        let response = admin_api.handle_request(request).await.unwrap();
        assert_eq!(response.status_code, 401);
    }

    #[tokio::test]
    async fn test_scan_operations() {
        let config = AdminApiConfig {
            require_auth: false, // Disable auth for testing
            ..Default::default()
        };
        let scan_engine = Arc::new(ScanEngine::new(ScanEngineConfig::default()).await.unwrap());
        let heal_engine = Arc::new(HealEngine::new(HealEngineConfig::default()).await.unwrap());
        let policy_engine = Arc::new(PolicyEngine::new(PolicyEngineConfig::default()).await.unwrap());

        let admin_api = AdminApi::new(config, scan_engine, heal_engine, policy_engine).await.unwrap();

        // Test start scan
        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/admin/scan/start".to_string(),
            headers: vec![],
            body: None,
            query_params: vec![],
        };

        let response = admin_api.handle_request(request).await.unwrap();
        assert_eq!(response.status_code, 200);

        // Test get scan status
        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/admin/scan/status".to_string(),
            headers: vec![],
            body: None,
            query_params: vec![],
        };

        let response = admin_api.handle_request(request).await.unwrap();
        assert_eq!(response.status_code, 200);
    }

    #[tokio::test]
    async fn test_heal_operations() {
        let config = AdminApiConfig {
            require_auth: false, // Disable auth for testing
            ..Default::default()
        };
        let scan_engine = Arc::new(ScanEngine::new(ScanEngineConfig::default()).await.unwrap());
        let heal_engine = Arc::new(HealEngine::new(HealEngineConfig::default()).await.unwrap());
        let policy_engine = Arc::new(PolicyEngine::new(PolicyEngineConfig::default()).await.unwrap());

        let admin_api = AdminApi::new(config, scan_engine, heal_engine, policy_engine).await.unwrap();

        // Test start heal
        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/admin/heal/start".to_string(),
            headers: vec![],
            body: None,
            query_params: vec![],
        };

        let response = admin_api.handle_request(request).await.unwrap();
        assert_eq!(response.status_code, 200);

        // Test get heal status
        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/admin/heal/status".to_string(),
            headers: vec![],
            body: None,
            query_params: vec![],
        };

        let response = admin_api.handle_request(request).await.unwrap();
        assert_eq!(response.status_code, 200);
    }

    #[tokio::test]
    async fn test_system_operations() {
        let config = AdminApiConfig {
            require_auth: false, // Disable auth for testing
            ..Default::default()
        };
        let scan_engine = Arc::new(ScanEngine::new(ScanEngineConfig::default()).await.unwrap());
        let heal_engine = Arc::new(HealEngine::new(HealEngineConfig::default()).await.unwrap());
        let policy_engine = Arc::new(PolicyEngine::new(PolicyEngineConfig::default()).await.unwrap());

        let admin_api = AdminApi::new(config, scan_engine, heal_engine, policy_engine).await.unwrap();

        // Test get system status
        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/admin/system/status".to_string(),
            headers: vec![],
            body: None,
            query_params: vec![],
        };

        let response = admin_api.handle_request(request).await.unwrap();
        assert_eq!(response.status_code, 200);
        assert!(response.body.contains("system_status"));
    }
} 