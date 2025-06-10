use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::error::Error;
use std::fmt;
use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time;

pub(crate) mod manager;
pub(crate) mod queue;

// 常量定义
pub const RETRY_INTERVAL: Duration = Duration::from_secs(3);
pub const DEFAULT_LIMIT: u64 = 100000; // 默认存储限制
pub const DEFAULT_EXT: &str = ".unknown";
pub const COMPRESS_EXT: &str = ".snappy";

// 错误类型
#[derive(Debug)]
pub enum StoreError {
    NotConnected,
    LimitExceeded,
    IoError(std::io::Error),
    Utf8(std::str::Utf8Error),
    SerdeError(serde_json::Error),
    Deserialize(serde_json::Error),
    UuidError(uuid::Error),
    Other(String),
}

impl Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::NotConnected => write!(f, "not connected to target server/service"),
            StoreError::LimitExceeded => write!(f, "the maximum store limit reached"),
            StoreError::IoError(e) => write!(f, "IO error: {}", e),
            StoreError::Utf8(e) => write!(f, "UTF-8 conversion error: {}", e),
            StoreError::SerdeError(e) => write!(f, "serialization error: {}", e),
            StoreError::Deserialize(e) => write!(f, "deserialization error: {}", e),
            StoreError::UuidError(e) => write!(f, "UUID generation error: {}", e),
            StoreError::Other(s) => write!(f, "{}", s),
        }
    }
}

impl Error for StoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            StoreError::IoError(e) => Some(e),
            StoreError::SerdeError(e) => Some(e),
            StoreError::UuidError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for StoreError {
    fn from(e: std::io::Error) -> Self {
        StoreError::IoError(e)
    }
}

impl From<serde_json::Error> for StoreError {
    fn from(e: serde_json::Error) -> Self {
        StoreError::SerdeError(e)
    }
}

impl From<uuid::Error> for StoreError {
    fn from(e: uuid::Error) -> Self {
        StoreError::UuidError(e)
    }
}

pub type StoreResult<T> = Result<T, StoreError>;

// 日志记录器类型
pub type Logger = fn(ctx: Option<&str>, err: StoreError, id: &str, err_kind: &[&dyn Display]);

// Key 结构体定义
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Key {
    pub name: String,
    pub compress: bool,
    pub extension: String,
    pub item_count: usize,
}

impl Key {
    pub fn new(name: String, extension: String) -> Self {
        Self {
            name,
            extension,
            compress: false,
            item_count: 1,
        }
    }

    pub fn with_compression(mut self, compress: bool) -> Self {
        self.compress = compress;
        self
    }

    pub fn with_item_count(mut self, count: usize) -> Self {
        self.item_count = count;
        self
    }

    pub fn to_string(&self) -> String {
        let mut key_str = self.name.clone();

        if self.item_count > 1 {
            key_str = format!("{}:{}", self.item_count, self.name);
        }

        let ext = if self.compress {
            format!("{}{}", self.extension, COMPRESS_EXT)
        } else {
            self.extension.clone()
        };

        format!("{}{}", key_str, ext)
    }
}

impl Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

pub fn parse_key(k: &str) -> Key {
    let mut key = Key {
        name: k.to_string(),
        compress: false,
        extension: String::new(),
        item_count: 1,
    };

    // 检查压缩扩展名
    if k.ends_with(COMPRESS_EXT) {
        key.compress = true;
        key.name = key.name[..key.name.len() - COMPRESS_EXT.len()].to_string();
    }

    // 解析项目数量
    if let Some(colon_pos) = key.name.find(':') {
        if let Ok(count) = key.name[..colon_pos].parse::<usize>() {
            key.item_count = count;
            key.name = key.name[colon_pos + 1..].to_string();
        }
    }

    // 解析扩展名
    if let Some(dot_pos) = key.name.rfind('.') {
        key.extension = key.name[dot_pos..].to_string();
        key.name = key.name[..dot_pos].to_string();
    }

    key
}

// Target trait 定义
#[async_trait]
pub trait Target: Send + Sync {
    fn name(&self) -> String;
    async fn send_from_store(&self, key: Key) -> StoreResult<()>;
}

// Store trait 定义
#[async_trait]
pub trait Store<T>: Send + Sync
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    async fn put(&self, item: T) -> StoreResult<Key>;
    async fn put_multiple(&self, items: Vec<T>) -> StoreResult<Key>;
    async fn get(&self, key: Key) -> StoreResult<T>;
    async fn get_multiple(&self, key: Key) -> StoreResult<Vec<T>>;
    async fn get_raw(&self, key: Key) -> StoreResult<Vec<u8>>;
    async fn put_raw(&self, b: Vec<u8>) -> StoreResult<Key>;
    async fn len(&self) -> usize;
    async fn list(&self) -> Vec<Key>;
    async fn del(&self, key: Key) -> StoreResult<()>;
    async fn open(&self) -> StoreResult<()>;
    async fn delete(&self) -> StoreResult<()>;
}

// 重播项目辅助函数
pub async fn replay_items<T>(store: Arc<dyn Store<T>>, done_ch: mpsc::Receiver<()>, log: Logger, id: &str) -> mpsc::Receiver<Key>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    let (tx, rx) = mpsc::channel(100); // 合理的缓冲区大小
    let id = id.to_string();

    tokio::spawn(async move {
        let mut done_ch = done_ch;
        let mut retry_interval = time::interval(RETRY_INTERVAL);
        let mut retry_interval = time::interval_at(retry_interval.tick().await, RETRY_INTERVAL);

        loop {
            let keys = store.list().await;

            for key in keys {
                let tx = tx.clone();
                tokio::select! {
                    _ = tx.send(key) => {
                        // 成功发送下一个键
                    }
                    _ = done_ch.recv() => {
                        return;
                    }
                }
            }

            tokio::select! {
                _ = retry_interval.tick() => {
                    // 重试定时器触发，继续循环
                }
                _ = done_ch.recv() => {
                    return;
                }
            }
        }
    });

    rx
}

// 发送项目辅助函数
pub async fn send_items(target: &dyn Target, mut key_ch: mpsc::Receiver<Key>, mut done_ch: mpsc::Receiver<()>, logger: Logger) {
    let mut retry_interval = time::interval(RETRY_INTERVAL);

    async fn try_send(
        target: &dyn Target,
        key: Key,
        retry_interval: &mut time::Interval,
        done_ch: &mut mpsc::Receiver<()>,
        logger: Logger,
    ) -> bool {
        loop {
            match target.send_from_store(key.clone()).await {
                Ok(_) => return true,
                Err(err) => {
                    logger(None, err, &target.name(), &[&format!("unable to send log entry to '{}'", target.name())]);

                    tokio::select! {
                        _ = retry_interval.tick() => {
                            // 重试
                        }
                        _ = done_ch.recv() => {
                            return false;
                        }
                    }
                }
            }
        }
    }

    loop {
        tokio::select! {
            maybe_key = key_ch.recv() => {
                match maybe_key {
                    Some(key) => {
                        if !try_send(target, key, &mut retry_interval, &mut done_ch, logger).await {
                            return;
                        }
                    }
                    None => return,
                }
            }
            _ = done_ch.recv() => {
                return;
            }
        }
    }
}

// 流式传输项目
pub async fn stream_items<T>(store: Arc<dyn Store<T>>, target: &dyn Target, done_ch: mpsc::Receiver<()>, logger: Logger)
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    // 创建一个 done_ch 的克隆，以便可以将其传递给 replay_items
    // let (tx, rx) = mpsc::channel::<()>(1);

    let (tx_replay, rx_replay) = mpsc::channel::<()>(1);
    let (tx_send, rx_send) = mpsc::channel::<()>(1);

    let mut done_ch = done_ch;

    let key_ch = replay_items(store, rx_replay, logger, &target.name()).await;
    // let key_ch = replay_items(store, rx, logger, &target.name()).await;

    let tx_replay_clone = tx_replay.clone();
    let tx_send_clone = tx_send.clone();

    // 监听原始 done_ch，如果收到信号，则关闭我们创建的通道
    tokio::spawn(async move {
        // if done_ch.recv().await.is_some() {
        //     let _ = tx.send(()).await;
        // }
        if done_ch.recv().await.is_some() {
            let _ = tx_replay_clone.send(()).await;
            let _ = tx_send_clone.send(()).await;
        }
    });

    // send_items(target, key_ch, rx, logger).await;
    send_items(target, key_ch, rx_send, logger).await;
}
