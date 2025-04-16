use crate::event::Event;
use crate::target::{Target, TargetID};
use async_trait::async_trait;
use reqwest::{header, Client};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

#[derive(Error, Debug)]
pub enum WebhookError {
    #[error("HTTP client error: {0}")]
    HttpClient(#[from] reqwest::Error),
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Store error: {0}")]
    Store(#[from] Box<dyn std::error::Error + Send + Sync>),
    #[error("Webhook request failed after retries")]
    RequestFailed,
}

type Result<T> = std::result::Result<T, WebhookError>;

/// Webhook 目标配置

/// Webhook 目标配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookConfig {
    /// Webhook endpoint URL
    pub endpoint: String,
    /// 认证令牌
    pub auth_token: Option<String>,
    /// 自定义请求头
    pub custom_headers: Option<HashMap<String, String>>,
    /// 重试次数
    pub max_retries: u32,
    /// 连接超时时间 (秒)
    pub timeout: u64,
}

/// Webhook Target 实现
pub struct Webhook {
    /// 目标 ID
    id: TargetID,
    /// HTTP 客户端
    client: Client,
    /// 配置信息
    config: WebhookConfig,
    /// 事件存储
    store: Arc<Mutex<dyn Store>>,
}

#[async_trait]
impl Target for Webhook {
    /// 返回目标 ID
    fn id(&self) -> &TargetID {
        &self.id
    }

    /// 检查 Webhook 是否可用
    async fn is_active(&self) -> Result<bool> {
        // 发送测试请求验证连接
        let resp = self.client.get(&self.config.endpoint).send().await?;

        Ok(resp.status().is_success())
    }

    /// 保存事件到存储并异步发送
    async fn save(&self, event: Event) -> Result<()> {
        // 序列化事件
        let event_json = serde_json::to_string(&event)?;

        // 存储事件
        self.store.lock().await.save(event_json)?;

        // 异步发送
        tokio::spawn(self.send_event(event));

        Ok(())
    }

    /// 从存储发送事件
    async fn send(&self, key: StoreKey) -> Result<()> {
        // 从存储获取事件
        let event_json = self.store.lock().await.get(&key)?;
        let event: Event = serde_json::from_str(&event_json)?;

        // 发送事件到 webhook
        self.send_event(event).await?;

        // 发送成功后删除
        self.store.lock().await.delete(&key)?;

        Ok(())
    }

    /// 返回事件存储
    fn store(&self) -> Arc<Mutex<dyn Store>> {
        self.store.clone()
    }

    /// 关闭 Target
    async fn close(&self) -> Result<()> {
        // 等待所有事件发送完成
        self.store.lock().await.flush()?;
        Ok(())
    }
}

impl Webhook {
    /// 创建新的 Webhook Target
    pub fn new(id: TargetID, config: WebhookConfig, store: Arc<Mutex<dyn Store>>) -> Result<Self> {
        // 构建 HTTP 客户端
        let client = Client::builder().timeout(Duration::from_secs(config.timeout)).build()?;

        Ok(Self {
            id,
            client,
            config,
            store,
        })
    }

    /// 发送事件到 Webhook endpoint
    async fn send_event(&self, event: Event) -> Result<()> {
        let mut retries = 0;
        loop {
            // 构建请求
            let mut req = self.client.post(&self.config.endpoint).json(&event);

            // 添加认证头
            if let Some(token) = &self.config.auth_token {
                req = req.header(header::AUTHORIZATION, format!("Bearer {}", token));
            }

            // 添加自定义头
            if let Some(headers) = &self.config.custom_headers {
                for (key, value) in headers {
                    req = req.header(key, value);
                }
            }

            // 发送请求
            match req.send().await {
                Ok(resp) if resp.status().is_success() => {
                    return Ok(());
                }
                _ if retries < self.config.max_retries => {
                    retries += 1;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
                Err(e) => return Err(WebhookError::HttpClient(e)),
                _ => return Err(WebhookError::RequestFailed),
            }
        }
    }
}
