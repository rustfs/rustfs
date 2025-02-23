#[cfg(feature = "audit-kafka")]
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};

#[cfg(feature = "audit-webhook")]
use reqwest::Client;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

/// AuditEntry is a struct that represents an audit entry
/// that can be logged
/// # Fields
/// * `version` - The version of the audit entry
/// * `event_type` - The type of event that occurred
/// * `bucket` - The bucket that was accessed
/// * `object` - The object that was accessed
/// * `user` - The user that accessed the object
/// * `time` - The time the event occurred
/// * `user_agent` - The user agent that accessed the object
/// * `span_id` - The span ID of the event
/// # Example
/// ```
/// use rustfs_logging::AuditEntry;
/// let entry = AuditEntry {
///     version: "1.0".to_string(),
///     event_type: "read".to_string(),
///     bucket: "bucket".to_string(),
///     object: "object".to_string(),
///     user: "user".to_string(),
///     time: "time".to_string(),
///     user_agent: "user_agent".to_string(),
///     span_id: "span_id".to_string(),
/// };
/// ```
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AuditEntry {
    pub version: String,
    pub event_type: String,
    pub bucket: String,
    pub object: String,
    pub user: String,
    pub time: String,
    pub user_agent: String,
    pub span_id: String,
}

/// AuditTarget is a trait that defines the interface for audit targets
/// that can receive audit entries
pub trait AuditTarget: Send + Sync {
    fn send(&self, entry: AuditEntry);
}

/// FileAuditTarget is an audit target that logs audit entries to a file
pub struct FileAuditTarget;

impl AuditTarget for FileAuditTarget {
    /// Send an audit entry to a file
    /// # Arguments
    /// *   `entry` - The audit entry to send
    ///
    /// # Example
    /// ```
    /// use rustfs_logging::{AuditEntry, AuditTarget, FileAuditTarget};
    /// let entry = AuditEntry {
    ///     version: "1.0".to_string(),
    ///     event_type: "read".to_string(),
    ///     bucket: "bucket".to_string(),
    ///     object: "object".to_string(),
    ///     user: "user".to_string(),
    ///     time: "time".to_string(),
    ///     user_agent: "user_agent".to_string(),
    ///     span_id: "span_id".to_string(),
    /// };
    /// FileAuditTarget.send(entry);
    /// ```
    fn send(&self, entry: AuditEntry) {
        println!("File audit: {:?}", entry);
    }
}

#[cfg(feature = "audit-webhook")]
/// Webhook audit objectives
/// #Arguments
/// * `client` - The reqwest client
/// * `url` - The URL of the webhook
/// # Example 
/// ```
/// use rustfs_logging::WebhookAuditTarget;
/// let target = WebhookAuditTarget::new("http://localhost:8080");
/// ```
pub struct WebhookAuditTarget {
    client: Client,
    url: String,
}

#[cfg(feature = "audit-webhook")]
impl WebhookAuditTarget {
    pub fn new(url: &str) -> Self {
        Self {
            client: Client::new(),
            url: url.to_string(),
        }
    }
}

#[cfg(feature = "audit-webhook")]
impl AuditTarget for WebhookAuditTarget {
    fn send(&self, entry: AuditEntry) {
        let client = self.client.clone();
        let url = self.url.clone();
        tokio::spawn(async move {
            if let Err(e) = client.post(&url).json(&entry).send().await {
                eprintln!("Failed to send to Webhook: {:?}", e);
            }
        });
    }
}

#[cfg(feature = "audit-kafka")]
/// Kafka audit objectives
/// # Arguments
/// * `producer` - The Kafka producer
/// * `topic` - The Kafka topic
/// # Example
/// ```
/// use rustfs_logging::KafkaAuditTarget;
/// let target = KafkaAuditTarget::new("localhost:9092", "minio-audit");
/// ```
/// # Note
/// This feature requires the `rdkafka` crate
/// # Example
/// ```toml
/// [dependencies]
/// rdkafka = "0.26.0"
/// rustfs_logging = { version = "0.1.0", features = ["audit-kafka"] }
/// ```
/// # Note
/// The `rdkafka` crate requires the `librdkafka` library to be installed
/// # Example
/// ```sh
/// sudo apt-get install librdkafka-dev
/// ```
/// # Note
/// The `rdkafka` crate requires the `libssl-dev` and `pkg-config` packages to be installed
/// # Example
/// ```sh
/// sudo apt-get install libssl-dev pkg-config
/// ```
/// # Note
/// The `rdkafka` crate requires the `zlib1g-dev` package to be installed
/// # Example
/// ```sh
/// sudo apt-get install zlib1g-dev
/// ```
/// # Note
/// The `rdkafka` crate requires the `zstd` package to be installed
/// # Example
/// ```sh
/// sudo apt-get install zstd
/// ```
/// # Note
/// The `rdkafka` crate requires the `lz4` package to be installed
/// # Example
/// ```sh
/// sudo apt-get install lz4
/// ```
pub struct KafkaAuditTarget {
    producer: FutureProducer,
    topic: String,
}

#[cfg(feature = "audit-kafka")]
impl KafkaAuditTarget {
    pub fn new(brokers: &str, topic: &str) -> Self {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create()
            .expect("Kafka producer creation failed");
        Self {
            producer,
            topic: topic.to_string(),
        }
    }
}

#[cfg(feature = "audit-kafka")]
impl AuditTarget for KafkaAuditTarget {
    fn send(&self, entry: AuditEntry) {
        let topic = self.topic.clone();
        let span_id = entry.span_id.clone();
        let payload = serde_json::to_string(&entry).unwrap();
        // let record = FutureRecord::to(&topic).payload(&payload).key(&span_id);
        tokio::spawn({
            // 在异步闭包内部创建 record
            let topic = topic;
            let payload = payload;
            let span_id = span_id;
            let producer = self.producer.clone();
            async move {
                let record = FutureRecord::to(&topic).payload(&payload).key(&span_id);
                if let Err(e) = producer.send(record, std::time::Duration::from_secs(0)).await {
                    eprintln!("Failed to send to Kafka: {:?}", e);
                }
            }
        });
    }
}
/// AuditLogger is a logger that logs audit entries
/// to multiple targets
///
/// # Example
/// ```
/// use rustfs_logging::{AuditEntry, AuditLogger, FileAuditTarget};
///
/// #[tokio::main]
/// async fn main() {
///     let logger = AuditLogger::new(vec![Box::new(FileAuditTarget)]);
///     let entry = AuditEntry {
///         version: "1.0".to_string(),
///         event_type: "read".to_string(),
///         bucket: "bucket".to_string(),
///         object: "object".to_string(),
///         user: "user".to_string(),
///         time: "time".to_string(),
///         user_agent: "user_agent".to_string(),
///         span_id: "span_id".to_string(),
///     };
///     logger.log(entry).await;
/// }
/// ```

#[derive(Debug)]
/// AuditLogger is a logger that logs audit entries
/// to multiple targets
/// # Example
/// ```
/// use rustfs_logging::{AuditEntry, AuditLogger, FileAuditTarget};
/// let logger = AuditLogger::new(vec![Box::new(FileAuditTarget)]);
/// ```
/// # Note
/// This feature requires the `tokio` crate
/// # Example
/// ```toml
/// [dependencies]
/// tokio = { version = "1", features = ["full"] }
/// rustfs_logging = { version = "0.1.0"}
/// ```
/// # Note
/// This feature requires the `serde` crate
/// # Example
/// ```toml
/// [dependencies]
/// serde = { version = "1", features = ["derive"] }
/// rustfs_logging = { version = "0.1.0"}
/// ```
pub struct AuditLogger {
    tx: mpsc::Sender<AuditEntry>,
}

impl AuditLogger {
    /// Create a new AuditLogger with the given targets
    /// that will receive audit entries
    /// # Arguments
    /// * `targets` - A vector of audit targets
    /// # Returns
    /// * An AuditLogger
    /// # Example
    /// ```
    /// use rustfs_logging::{AuditLogger, AuditEntry, FileAuditTarget};
    ///
    /// let logger = AuditLogger::new(vec![Box::new(FileAuditTarget)]);
    /// ```
    pub fn new(targets: Vec<Box<dyn AuditTarget>>) -> Self {
        let (tx, mut rx) = mpsc::channel::<AuditEntry>(1000);
        tokio::spawn(async move {
            while let Some(entry) = rx.recv().await {
                for target in &targets {
                    target.send(entry.clone());
                }
            }
        });
        Self { tx }
    }

    /// Log an audit entry
    /// # Arguments
    /// * `entry` - The audit entry to log
    /// # Example
    /// ```
    /// use rustfs_logging::{AuditEntry, AuditLogger, FileAuditTarget};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let logger = AuditLogger::new(vec![Box::new(FileAuditTarget)]);
    ///     let entry = AuditEntry {
    ///         version: "1.0".to_string(),
    ///         event_type: "read".to_string(),
    ///         bucket: "bucket".to_string(),
    ///         object: "object".to_string(),
    ///         user: "user".to_string(),
    ///         time: "time".to_string(),
    ///         user_agent: "user_agent".to_string(),
    ///         span_id: "span_id".to_string(),
    ///     };
    ///     logger.log(entry).await;
    /// }
    /// ```
    pub async fn log(&self, entry: AuditEntry) {
        let _ = self.tx.send(entry).await;
    }
}
