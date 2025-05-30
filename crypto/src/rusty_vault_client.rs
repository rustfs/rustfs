// rusty_vault_client.rs - Simple RustyVault API client implementation
// Based on https://github.com/Tongsuo-Project/RustyVault/blob/main/src/api/client.rs

use serde::{Deserialize, Serialize};
use serde_json::Map;
use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

// 定义 API 响应结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VaultResponse {
    pub request_id: Option<String>,
    pub lease_id: Option<String>,
    pub renewable: Option<bool>,
    pub lease_duration: Option<i64>,
    pub data: Option<serde_json::Value>,
    pub response_data: Option<serde_json::Map<String, serde_json::Value>>,
    pub warnings: Option<Vec<String>>,
    pub auth: Option<serde_json::Value>,
}

// 客户端错误类型
#[derive(Debug)]
pub enum ClientError {
    RequestError(reqwest::Error),
    StatusError { status: u16, message: String },
    ParseError(String),
    ConfigError(String),
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::RequestError(e) => write!(f, "Request error: {}", e),
            ClientError::StatusError { status, message } => {
                write!(f, "Status error: {} - {}", status, message)
            }
            ClientError::ParseError(s) => write!(f, "Parse error: {}", s),
            ClientError::ConfigError(s) => write!(f, "Configuration error: {}", s),
        }
    }
}

impl std::error::Error for ClientError {}

impl From<reqwest::Error> for ClientError {
    fn from(err: reqwest::Error) -> Self {
        ClientError::RequestError(err)
    }
}

// RustyVault 客户端
pub struct Client {
    addr: String,
    token: String,
    client: reqwest::Client,
    namespace: Option<String>,
}

impl Client {

    // 执行 GET 请求
    pub async fn read(&self, path: &str, query: Option<HashMap<String, String>>) -> Result<VaultResponse, ClientError> {
        let url = format!("{}/v1/{}", self.addr.trim_end_matches('/'), path.trim_start_matches('/'));
        
        let mut req = self.client.get(&url)
            .header("X-Vault-Token", &self.token);
            
        if let Some(ns) = &self.namespace {
            req = req.header("X-Vault-Namespace", ns);
        }
        
        if let Some(q) = query {
            req = req.query(&q);
        }
        
        self.send_request(req).await
    }
    
    // 执行 POST 请求
    pub async fn write(&self, path: &str, body: Option<serde_json::Map<String, serde_json::Value>>) -> Result<VaultResponse, ClientError> {
        let url = format!("{}/v1/{}", self.addr.trim_end_matches('/'), path.trim_start_matches('/'));
        
        let mut req = self.client.post(&url)
            .header("X-Vault-Token", &self.token);
            
        if let Some(ns) = &self.namespace {
            req = req.header("X-Vault-Namespace", ns);
        }
        
        if let Some(b) = body {
            req = req.json(&b);
        }
        
        self.send_request(req).await
    }
    
    // 执行 DELETE 请求
    pub async fn delete(&self, path: &str) -> Result<VaultResponse, ClientError> {
        let url = format!("{}/v1/{}", self.addr.trim_end_matches('/'), path.trim_start_matches('/'));
        
        let mut req = self.client.delete(&url)
            .header("X-Vault-Token", &self.token);
            
        if let Some(ns) = &self.namespace {
            req = req.header("X-Vault-Namespace", ns);
        }
        
        self.send_request(req).await
    }
    
    // 发送请求并处理响应
    async fn send_request(&self, req: reqwest::RequestBuilder) -> Result<VaultResponse, ClientError> {
        let response = req.send().await?;
        
        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
            return Err(ClientError::StatusError {
                status: status.as_u16(),
                message: error_text,
            });
        }
        
        let json: serde_json::Value = response.json().await?;
        
        // 构建响应对象
        let mut vault_response = VaultResponse {
            request_id: json.get("request_id").and_then(|v| v.as_str()).map(String::from),
            lease_id: json.get("lease_id").and_then(|v| v.as_str()).map(String::from),
            renewable: json.get("renewable").and_then(|v| v.as_bool()),
            lease_duration: json.get("lease_duration").and_then(|v| v.as_i64()),
            data: json.get("data").cloned(),
            response_data: None,
            warnings: json.get("warnings").and_then(|v| {
                let mut warnings = Vec::new();
                if let Some(array) = v.as_array() {
                    for item in array {
                        if let Some(s) = item.as_str() {
                            warnings.push(s.to_string());
                        }
                    }
                    Some(warnings)
                } else {
                    None
                }
            }),
            auth: json.get("auth").cloned(),
        };
        
        // 解析 response_data
        if let Some(data) = json.get("data").and_then(|v| v.as_object()) {
            vault_response.response_data = Some(data.clone());
        } else {
            vault_response.response_data = Some(Map::new());
        }
        
        Ok(vault_response)
    }
}

// 客户端构建器
#[derive(Default)]
pub struct ClientBuilder {
    addr: Option<String>,
    token: Option<String>,
    namespace: Option<String>,
    timeout: Option<Duration>,
    skip_tls_verify: bool,
    ca_cert: Option<String>,
    client_cert: Option<String>,
    client_key: Option<String>,
}

impl ClientBuilder {
    // 设置 Vault 服务器地址
    pub fn with_addr(mut self, addr: &str) -> Self {
        self.addr = Some(addr.to_string());
        self
    }
    
    // 设置认证令牌
    pub fn with_token(mut self, token: &str) -> Self {
        self.token = Some(token.to_string());
        self
    }
    
    // 设置命名空间
    pub fn with_namespace(mut self, namespace: &str) -> Self {
        self.namespace = Some(namespace.to_string());
        self
    }
    
    // 设置请求超时时间
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
    
    // 跳过 TLS 证书验证
    pub fn skip_tls_verify(mut self) -> Self {
        self.skip_tls_verify = true;
        self
    }
    
    // 设置 CA 证书路径
    pub fn with_ca_cert(mut self, ca_path: &str) -> Self {
        self.ca_cert = Some(ca_path.to_string());
        self
    }
    
    // 设置客户端证书和密钥
    pub fn with_client_cert(mut self, cert_path: &str, key_path: &str) -> Self {
        self.client_cert = Some(cert_path.to_string());
        self.client_key = Some(key_path.to_string());
        self
    }
    
    // 构建客户端实例
    pub fn build(self) -> Result<Client, ClientError> {
        let addr = self.addr.ok_or_else(|| {
            ClientError::ConfigError("Vault address is required".to_string())
        })?;
        
        let token = self.token.unwrap_or_default();
        
        // 构建 reqwest 客户端
        let mut client_builder = reqwest::Client::builder();
        
        // 设置超时
        if let Some(timeout) = self.timeout {
            client_builder = client_builder.timeout(timeout);
        }
        
        // TLS 配置
        if self.skip_tls_verify {
            client_builder = client_builder.danger_accept_invalid_certs(true);
        }
        
        // 构建客户端
        let client = client_builder.build().map_err(|e| {
            ClientError::ConfigError(format!("Failed to build HTTP client: {}", e))
        })?;
        
        Ok(Client {
            addr,
            token,
            client,
            namespace: self.namespace,
        })
    }
}