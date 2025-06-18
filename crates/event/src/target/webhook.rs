use super::{Logger, Target, TargetID, TargetStore, STORE_PREFIX};
use async_trait::async_trait;
use once_cell::sync::OnceCell;
use reqwest::{header, Client, StatusCode};
use rustfs_config::notify::webhook::WebhookArgs;
use rustfs_notify::{
    store::{self, Key, Store, StoreError, StoreResult},
    Event,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::{net::TcpStream, sync::mpsc};
use url::Url;

pub struct WebhookTarget {
    init: OnceCell<()>,
    id: TargetID,
    args: WebhookArgs,
    client: Client,
    store: Option<Arc<dyn Store<Event>>>,
    logger: Logger,
    cancel_tx: mpsc::Sender<()>,
    addr: String, // 完整地址，包含 IP/DNS 和端口号
}

impl WebhookTarget {
    pub async fn new(id: &str, args: WebhookArgs, logger: Logger) -> Result<Self, StoreError> {
        // 创建取消通道
        let (cancel_tx, cancel_rx) = mpsc::channel(1);

        // 配置客户端
        let mut client_builder = Client::builder().timeout(Duration::from_secs(10));

        // 添加客户端证书如果配置了
        if !args.client_cert.is_empty() && !args.client_key.is_empty() {
            let cert =
                std::fs::read(&args.client_cert).map_err(|e| StoreError::Other(format!("Failed to read client cert: {}", e)))?;
            let key =
                std::fs::read(&args.client_key).map_err(|e| StoreError::Other(format!("Failed to read client key: {}", e)))?;

            let identity = reqwest::Identity::from_pem(&[cert, key].concat())
                .map_err(|e| StoreError::Other(format!("Failed to create identity: {}", e)))?;

            client_builder = client_builder.identity(identity);
        }

        let client = client_builder
            .build()
            .map_err(|e| StoreError::Other(format!("Failed to create HTTP client: {}", e)))?;

        // 计算目标地址
        let endpoint = Url::parse(&args.endpoint).map_err(|e| StoreError::Other(format!("Invalid URL: {}", e)))?;

        let mut addr = endpoint
            .host_str()
            .ok_or_else(|| StoreError::Other("Missing host in endpoint".into()))?
            .to_string();

        // 如果没有端口，根据协议添加默认端口
        if endpoint.port().is_none() {
            match endpoint.scheme() {
                "http" => addr.push_str(":80"),
                "https" => addr.push_str(":443"),
                _ => return Err(StoreError::Other("Unsupported scheme".into())),
            }
        } else if let Some(port) = endpoint.port() {
            addr = format!("{}:{}", addr, port);
        }

        // 创建队列存储（如果配置了）
        let mut store = None;
        if !args.queue_dir.is_empty() {
            let queue_dir = PathBuf::from(&args.queue_dir).join(format!("{}-webhook-{}", STORE_PREFIX, id));
            let queue_store = Arc::new(store::queue::QueueStore::<Event>::new(queue_dir, args.queue_limit, Some(".event")));

            queue_store.open().await?;
            store = Some(queue_store.clone() as Arc<dyn Store<Event>>);

            // 设置事件流
            let target_store = Arc::new(queue_store);
            let target = Arc::new(WebhookTargetWrapper::new(
                id,
                args.clone(),
                client.clone(),
                addr.clone(),
                logger,
                cancel_tx.clone(),
            ));

            tokio::spawn(async move {
                store::stream_items(target_store.clone(), target.clone(), cancel_rx, logger).await;
            });
        }

        Ok(Self {
            init: OnceCell::new(),
            id: TargetID::new(id, "webhook"),
            args,
            client,
            store,
            logger,
            cancel_tx,
            addr,
        })
    }

    async fn initialize(&self) -> StoreResult<()> {
        if self.init.get().is_some() {
            return Ok(());
        }

        let is_active = self.is_active().await?;
        if !is_active {
            return Err(StoreError::NotConnected);
        }

        self.init
            .set(())
            .map_err(|_| StoreError::Other("Failed to initialize".into()))?;
        Ok(())
    }

    async fn send(&self, event_data: &Event) -> StoreResult<()> {
        // 构建请求数据
        let object_key = match urlencoding::decode(&event_data.s3.object.key) {
            Ok(key) => key.to_string(),
            Err(e) => return Err(StoreError::Other(format!("Failed to decode object key: {}", e))),
        };

        let key = format!("{}/{}", event_data.s3.bucket.name, object_key);
        let log_data = json!({
            "EventName": event_data.event_name,
            "Key": key,
            "Records": [event_data]
        });

        // 创建请求
        let mut request_builder = self
            .client
            .post(&self.args.endpoint)
            .header(header::CONTENT_TYPE, "application/json");

        // 添加认证头
        if !self.args.auth_token.is_empty() {
            let tokens: Vec<&str> = self.args.auth_token.split_whitespace().collect();
            match tokens.len() {
                2 => request_builder = request_builder.header(header::AUTHORIZATION, &self.args.auth_token),
                1 => request_builder = request_builder.header(header::AUTHORIZATION, format!("Bearer {}", &self.args.auth_token)),
                _ => {}
            }
        }

        // 发送请求
        let response = request_builder.json(&log_data).send().await.map_err(|e| {
            if e.is_timeout() || e.is_connect() {
                StoreError::NotConnected
            } else {
                StoreError::Other(format!("Request failed: {}", e))
            }
        })?;

        // 检查响应状态
        let status = response.status();
        if status.is_success() {
            Ok(())
        } else if status == StatusCode::FORBIDDEN {
            Err(StoreError::Other(format!(
                "{} returned '{}', please check if your auth token is correctly set",
                self.args.endpoint, status
            )))
        } else {
            Err(StoreError::Other(format!(
                "{} returned '{}', please check your endpoint configuration",
                self.args.endpoint, status
            )))
        }
    }
}

struct WebhookTargetWrapper {
    id: TargetID,
    args: WebhookArgs,
    client: Client,
    addr: String,
    logger: Logger,
    cancel_tx: mpsc::Sender<()>,
}

impl WebhookTargetWrapper {
    fn new(id: &str, args: WebhookArgs, client: Client, addr: String, logger: Logger, cancel_tx: mpsc::Sender<()>) -> Self {
        Self {
            id: TargetID::new(id, "webhook"),
            args,
            client,
            addr,
            logger,
            cancel_tx,
        }
    }
}

#[async_trait]
impl Target for WebhookTargetWrapper {
    fn name(&self) -> String {
        self.id.to_string()
    }

    async fn send_from_store(&self, key: Key) -> StoreResult<()> {
        // 这个方法在 Target trait 实现中需要，但我们不会直接使用它
        // 实际上，它将由上面创建的 WebhookTarget 的 SendFromStore 方法处理
        Ok(())
    }

    async fn is_active(&self) -> StoreResult<bool> {
        // 尝试连接到目标地址
        match tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(&self.addr)).await {
            Ok(Ok(_)) => Ok(true),
            Ok(Err(e)) => {
                if e.kind() == std::io::ErrorKind::ConnectionRefused
                    || e.kind() == std::io::ErrorKind::ConnectionAborted
                    || e.kind() == std::io::ErrorKind::ConnectionReset
                {
                    Err(StoreError::NotConnected)
                } else {
                    Err(StoreError::Other(format!("Connection error: {}", e)))
                }
            }
            Err(_) => Err(StoreError::NotConnected),
        }
    }

    async fn close(&self) -> StoreResult<()> {
        // 发送取消信号
        let _ = self.cancel_tx.send(()).await;
        Ok(())
    }
}

#[async_trait]
impl Target for WebhookTarget {
    fn name(&self) -> String {
        self.id.to_string()
    }

    async fn send_from_store(&self, key: Key) -> StoreResult<()> {
        self.initialize().await?;

        // 如果有存储，获取事件并发送
        if let Some(store) = &self.store {
            match store.get(key.clone()).await {
                Ok(event_data) => match self.send(&event_data).await {
                    Ok(_) => store.del(key).await?,
                    Err(e) => {
                        if matches!(e, StoreError::NotConnected) {
                            return Err(StoreError::NotConnected);
                        }
                        return Err(e);
                    }
                },
                Err(e) => {
                    // 如果键不存在，可能已经被发送，忽略错误
                    if let StoreError::IoError(io_err) = &e {
                        if io_err.kind() == std::io::ErrorKind::NotFound {
                            return Ok(());
                        }
                    }
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    async fn is_active(&self) -> StoreResult<bool> {
        // 尝试连接到目标地址
        match tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(&self.addr)).await {
            Ok(Ok(_)) => Ok(true),
            Ok(Err(_)) => Err(StoreError::NotConnected),
            Err(_) => Err(StoreError::NotConnected),
        }
    }

    async fn close(&self) -> StoreResult<()> {
        // 发送取消信号
        let _ = self.cancel_tx.send(()).await;
        Ok(())
    }
}

impl TargetStore for WebhookTarget {
    fn store<T>(&self) -> Option<Arc<dyn Store<T>>>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        if let Some(store) = &self.store {
            // 注意：这里假设 T 是 Event 类型，需要类型转换（如果不是，将返回 None）
            if std::any::TypeId::of::<T>() == std::any::TypeId::of::<Event>() {
                // 安全：因为我们检查了类型 ID
                let store_ptr = Arc::as_ptr(store);
                let store_t = unsafe { Arc::from_raw(store_ptr as *const dyn Store<T>) };
                // 增加引用计数，避免释放原始指针
                std::mem::forget(store_t.clone());
                return Some(store_t);
            }
        }
        None
    }
}

impl WebhookTarget {
    pub async fn save(&self, event_data: Event) -> StoreResult<()> {
        // 如果配置了存储，则存储事件
        if let Some(store) = &self.store {
            return store.put(event_data).await.map(|_| ());
        }

        // 否则，初始化并直接发送
        self.initialize().await?;
        self.send(&event_data).await
    }

    pub fn id(&self) -> &TargetID {
        &self.id
    }
}
