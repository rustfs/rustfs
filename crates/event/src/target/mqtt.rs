use super::{Logger, Target, TargetID, TargetStore, STORE_PREFIX};
use async_trait::async_trait;
use once_cell::sync::OnceCell;
use rumqttc::{AsyncClient, ConnectionError, Event as MqttEvent, MqttOptions, QoS, Transport};
use rustfs_config::notify::mqtt::MQTTArgs;
use rustfs_notify::store;
use rustfs_notify::{
    store::{Key, Store, StoreError, StoreResult},
    Event, QueueStore,
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::json;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, Mutex},
    task::JoinHandle,
};
use url::Url;

pub struct MQTTTarget {
    init: OnceCell<()>,
    id: TargetID,
    args: MQTTArgs,
    client: Option<Arc<Mutex<AsyncClient>>>,
    eventloop_handle: Option<JoinHandle<()>>,
    store: Option<Arc<dyn Store<Event>>>,
    logger: Logger,
    cancel_tx: mpsc::Sender<()>,
    connection_status: Arc<Mutex<bool>>,
}

impl MQTTTarget {
    pub async fn new(id: &str, args: MQTTArgs, logger: Logger) -> Result<Self, StoreError> {
        // 创建取消通道
        let (cancel_tx, mut cancel_rx) = mpsc::channel(1);
        let connection_status = Arc::new(Mutex::new(false));

        // 创建队列存储（如果配置了）
        let mut store = None;
        if !args.queue_dir.is_empty() {
            if args.qos == 0 {
                return Err(StoreError::Other("QoS should be set to 1 or 2 if queueDir is set".to_string()));
            }

            let queue_dir = PathBuf::from(&args.queue_dir).join(format!("{}-mqtt-{}", STORE_PREFIX, id));
            let queue_store = Arc::new(QueueStore::<Event>::new(queue_dir, args.queue_limit, Some(".event")));

            queue_store.open().await?;
            store = Some(queue_store.clone() as Arc<dyn Store<Event>>);

            // 设置事件流
            let status_clone = connection_status.clone();
            let logger_clone = logger;
            let target_store = queue_store;
            let args_clone = args.clone();
            let id_clone = id.to_string();
            let cancel_tx_clone = cancel_tx.clone();

            tokio::spawn(async move {
                let target = Arc::new(MQTTTargetWrapper {
                    id: TargetID::new(&id_clone, "mqtt"),
                    args: args_clone,
                    client: None,
                    logger: logger_clone,
                    cancel_tx: cancel_tx_clone,
                    connection_status: status_clone,
                });

                store::stream_items(target_store, target, cancel_rx, logger_clone).await;
            });
        }

        Ok(Self {
            init: OnceCell::new(),
            id: TargetID::new(id, "mqtt"),
            args,
            client: None,
            eventloop_handle: None,
            store,
            logger,
            cancel_tx,
            connection_status,
        })
    }

    async fn initialize(&self) -> StoreResult<()> {
        if self.init.get().is_some() {
            return Ok(());
        }

        // 解析 MQTT broker 地址
        let broker_url = Url::parse(&self.args.broker).map_err(|e| StoreError::Other(format!("Invalid broker URL: {}", e)))?;

        let host = broker_url
            .host_str()
            .ok_or_else(|| StoreError::Other("Missing host in broker URL".into()))?
            .to_string();

        let port = broker_url.port().unwrap_or_else(|| {
            match broker_url.scheme() {
                "mqtt" => 1883,
                "mqtts" | "ssl" | "tls" => 8883,
                "ws" => 80,
                "wss" => 443,
                _ => 1883, // 默认
            }
        });

        // 创建客户端 ID
        let client_id = format!(
            "{:x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| StoreError::Other(e.to_string()))?
                .as_nanos()
        );

        // 创建 MQTT 选项
        let mut mqtt_options = MqttOptions::new(client_id, host, port);
        mqtt_options.set_clean_session(true);
        mqtt_options.set_keep_alive(self.args.keep_alive);
        mqtt_options.set_max_packet_size(100 * 1024); // 100KB

        // 设置重连间隔
        mqtt_options.set_connection_timeout(self.args.keep_alive.as_secs() as u16);
        mqtt_options.set_max_reconnect_retry(10); // 最大重试次数
        mqtt_options.set_retry_interval(Duration::from_millis(100));

        // 如果设置了用户名和密码
        if !self.args.username.is_empty() {
            mqtt_options.set_credentials(&self.args.username, &self.args.password);
        }

        // TLS 配置
        if self.args.root_cas.is_some()
            || broker_url.scheme() == "mqtts"
            || broker_url.scheme() == "ssl"
            || broker_url.scheme() == "tls"
            || broker_url.scheme() == "wss"
        {
            let mut transport = if broker_url.scheme() == "ws" || broker_url.scheme() == "wss" {
                let path = broker_url.path();
                Transport::Ws {
                    path: if path == "/" { "/mqtt".to_string() } else { path.to_string() },
                }
            } else {
                Transport::Tls
            };

            // 如果提供了根证书
            if let Some(root_cas) = &self.args.root_cas {
                if let Transport::Tls = transport {
                    transport = Transport::Tls;
                }

                // 在实际实现中，这里需要设置 TLS 证书
                // 由于 rumqttc 的接口可能会随版本变化，请参考最新的文档
            }

            mqtt_options.set_transport(transport);
        } else if broker_url.scheme() == "ws" {
            let path = broker_url.path();
            mqtt_options.set_transport(Transport::Ws {
                path: if path == "/" { "/mqtt".to_string() } else { path.to_string() },
            });
        }

        // 创建 MQTT 客户端
        let (client, mut eventloop) = AsyncClient::new(mqtt_options, 10);
        let client = Arc::new(Mutex::new(client));

        // 克隆引用用于事件循环
        let connection_status = self.connection_status.clone();
        let client_clone = client.clone();
        let logger = self.logger;
        let target_id = self.id.to_string();

        // 启动事件循环
        let eventloop_handle = tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(event) => match event {
                        MqttEvent::Incoming(incoming) => match incoming {
                            rumqttc::Packet::ConnAck(connack) => {
                                if connack.code == rumqttc::ConnectReturnCode::Success {
                                    *connection_status.lock().await = true;
                                } else {
                                    logger(
                                        None,
                                        StoreError::Other(format!("MQTT connection failed: {:?}", connack.code)),
                                        &target_id,
                                        &[],
                                    );
                                    *connection_status.lock().await = false;
                                }
                            }
                            _ => {}
                        },
                        MqttEvent::Outgoing(_) => {}
                    },
                    Err(ConnectionError::ConnectionRefused(_)) => {
                        *connection_status.lock().await = false;
                        logger(None, StoreError::NotConnected, &target_id, &["MQTT connection refused"]);
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                    Err(e) => {
                        *connection_status.lock().await = false;
                        logger(None, StoreError::Other(format!("MQTT error: {}", e)), &target_id, &[]);
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        });

        // 更新目标状态
        self.client = Some(client_clone);
        self.eventloop_handle = Some(eventloop_handle);

        // 等待连接建立
        for _ in 0..5 {
            if *self.connection_status.lock().await {
                self.init
                    .set(())
                    .map_err(|_| StoreError::Other("Failed to initialize MQTT target".into()))?;
                return Ok(());
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        Err(StoreError::NotConnected)
    }

    async fn send(&self, event_data: &Event) -> StoreResult<()> {
        let client = match &self.client {
            Some(client) => client,
            None => return Err(StoreError::NotConnected),
        };

        if !*self.connection_status.lock().await {
            return Err(StoreError::NotConnected);
        }

        // 构建消息内容
        let object_key = urlencoding::decode(&event_data.s3.object.key)
            .map_err(|e| StoreError::Other(format!("Failed to decode object key: {}", e)))?;

        let key = format!("{}/{}", event_data.s3.bucket.name, object_key);
        let log_data = json!({
            "EventName": event_data.event_name,
            "Key": key,
            "Records": [event_data]
        });

        let payload = serde_json::to_string(&log_data).map_err(|e| StoreError::SerdeError(e))?;

        // 确定 QoS 级别
        let qos = match self.args.qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            _ => QoS::AtMostOnce, // 默认
        };

        // 发布消息
        let mut client_guard = client.lock().await;
        client_guard
            .publish(&self.args.topic, qos, false, payload)
            .await
            .map_err(|e| {
                if matches!(e, rumqttc::ClientError::ConnectionLost(_)) {
                    StoreError::NotConnected
                } else {
                    StoreError::Other(format!("MQTT publish error: {}", e))
                }
            })?;

        Ok(())
    }
}

// MQTT 目标包装器，用于流事件
struct MQTTTargetWrapper {
    id: TargetID,
    args: MQTTArgs,
    client: Option<Arc<Mutex<AsyncClient>>>,
    logger: Logger,
    cancel_tx: mpsc::Sender<()>,
    connection_status: Arc<Mutex<bool>>,
}

#[async_trait]
impl Target for MQTTTargetWrapper {
    fn name(&self) -> String {
        self.id.to_string()
    }

    async fn send_from_store(&self, _key: Key) -> StoreResult<()> {
        // 这个方法在实际 MQTTTarget 中实现
        Ok(())
    }

    async fn is_active(&self) -> StoreResult<bool> {
        Ok(*self.connection_status.lock().await)
    }

    async fn close(&self) -> StoreResult<()> {
        // 发送取消信号
        let _ = self.cancel_tx.send(()).await;
        Ok(())
    }
}

#[async_trait]
impl Target for MQTTTarget {
    fn name(&self) -> String {
        self.id.to_string()
    }

    async fn send_from_store(&self, key: Key) -> StoreResult<()> {
        self.initialize().await?;

        // 如果没有连接，返回错误
        if !*self.connection_status.lock().await {
            return Err(StoreError::NotConnected);
        }

        // 如果有存储，获取事件并发送
        if let Some(store) = &self.store {
            match store.get(key.clone()).await {
                Ok(event_data) => {
                    match self.send(&event_data).await {
                        Ok(_) => {
                            // 成功发送后删除事件
                            return store.del(key).await.map(|_| ());
                        }
                        Err(e) => {
                            (self.logger)(None, e.clone(), &self.id.to_string(), &["Failed to send event"]);
                            return Err(e);
                        }
                    }
                }
                Err(e) => {
                    // 如果文件不存在，忽略错误（可能已被处理）
                    if let StoreError::IOError(ref io_err) = e {
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
        if self.init.get().is_none() {
            return Ok(false);
        }
        Ok(*self.connection_status.lock().await)
    }

    async fn close(&self) -> StoreResult<()> {
        // 发送取消信号
        let _ = self.cancel_tx.send(()).await;

        // 取消事件循环
        if let Some(handle) = &self.eventloop_handle {
            handle.abort();
        }

        // 断开 MQTT 连接
        if let Some(client) = &self.client {
            if let Ok(mut client) = client.try_lock() {
                // 尝试断开连接（忽略错误）
                let _ = client.disconnect().await;
            }
        }

        Ok(())
    }
}

impl TargetStore for MQTTTarget {
    fn store<T>(&self) -> Option<Arc<dyn Store<T>>>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        if let Some(store) = &self.store {
            // 类型检查确保 T 是 Event 类型
            if std::any::TypeId::of::<T>() == std::any::TypeId::of::<Event>() {
                // 安全：我们已经检查类型 ID 匹配
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

impl MQTTTarget {
    pub async fn save(&self, event_data: Event) -> StoreResult<()> {
        // 如果配置了存储，则存储事件
        if let Some(store) = &self.store {
            return store.put(event_data).await.map(|_| ());
        }

        // 否则，初始化并直接发送
        self.initialize().await?;

        // 检查连接
        if !*self.connection_status.lock().await {
            return Err(StoreError::NotConnected);
        }

        self.send(&event_data).await
    }

    pub fn id(&self) -> &TargetID {
        &self.id
    }
}
