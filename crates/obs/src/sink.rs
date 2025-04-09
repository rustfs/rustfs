use crate::{AppConfig, LogRecord, UnifiedLogEntry};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio::io;
use tokio::io::AsyncWriteExt;
use tracing::debug;

/// Sink Trait definition, asynchronously write logs
#[async_trait]
pub trait Sink: Send + Sync {
    async fn write(&self, entry: &UnifiedLogEntry);
}

#[cfg(feature = "kafka")]
/// Kafka Sink Implementation
pub struct KafkaSink {
    producer: rdkafka::producer::FutureProducer,
    topic: String,
    batch_size: usize,
    batch_timeout_ms: u64,
    entries: Arc<tokio::sync::Mutex<Vec<UnifiedLogEntry>>>,
    last_flush: Arc<std::sync::atomic::AtomicU64>,
}

#[cfg(feature = "kafka")]
impl KafkaSink {
    /// Create a new KafkaSink instance
    pub fn new(producer: rdkafka::producer::FutureProducer, topic: String, batch_size: usize, batch_timeout_ms: u64) -> Self {
        // Create Arc-wrapped values first
        let entries = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(batch_size)));
        let last_flush = Arc::new(std::sync::atomic::AtomicU64::new(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        ));
        let sink = KafkaSink {
            producer: producer.clone(),
            topic: topic.clone(),
            batch_size,
            batch_timeout_ms,
            entries: entries.clone(),
            last_flush: last_flush.clone(),
        };

        // Start background flusher
        tokio::spawn(Self::periodic_flush(producer, topic, entries, last_flush, batch_timeout_ms));

        sink
    }

    /// Add a getter method to read the batch_timeout_ms field
    #[allow(dead_code)]
    pub fn batch_timeout(&self) -> u64 {
        self.batch_timeout_ms
    }

    /// Add a method to dynamically adjust the timeout if needed
    #[allow(dead_code)]
    pub fn set_batch_timeout(&mut self, new_timeout_ms: u64) {
        self.batch_timeout_ms = new_timeout_ms;
    }

    async fn periodic_flush(
        producer: rdkafka::producer::FutureProducer,
        topic: String,
        entries: Arc<tokio::sync::Mutex<Vec<UnifiedLogEntry>>>,
        last_flush: Arc<std::sync::atomic::AtomicU64>,
        timeout_ms: u64,
    ) {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_millis(timeout_ms / 2)).await;

            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            let last = last_flush.load(std::sync::atomic::Ordering::Relaxed);

            if now - last >= timeout_ms {
                let mut batch = entries.lock().await;
                if !batch.is_empty() {
                    Self::send_batch(&producer, &topic, batch.drain(..).collect()).await;
                    last_flush.store(now, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
    }

    async fn send_batch(producer: &rdkafka::producer::FutureProducer, topic: &str, entries: Vec<UnifiedLogEntry>) {
        for entry in entries {
            let payload = match serde_json::to_string(&entry) {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("Failed to serialize log entry: {}", e);
                    continue;
                }
            };

            let span_id = entry.get_timestamp().to_rfc3339();

            let _ = producer
                .send(
                    rdkafka::producer::FutureRecord::to(topic).payload(&payload).key(&span_id),
                    std::time::Duration::from_secs(5),
                )
                .await;
        }
    }
}

#[cfg(feature = "kafka")]
#[async_trait]
impl Sink for KafkaSink {
    async fn write(&self, entry: &UnifiedLogEntry) {
        let mut batch = self.entries.lock().await;
        batch.push(entry.clone());

        let should_flush_by_size = batch.len() >= self.batch_size;
        let should_flush_by_time = {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let last = self.last_flush.load(std::sync::atomic::Ordering::Relaxed);
            now - last >= self.batch_timeout_ms
        };

        if should_flush_by_size || should_flush_by_time {
            // Existing flush logic
            let entries_to_send: Vec<UnifiedLogEntry> = batch.drain(..).collect();
            let producer = self.producer.clone();
            let topic = self.topic.clone();

            self.last_flush.store(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );

            tokio::spawn(async move {
                KafkaSink::send_batch(&producer, &topic, entries_to_send).await;
            });
        }
    }
}

#[cfg(feature = "kafka")]
impl Drop for KafkaSink {
    fn drop(&mut self) {
        // Perform any necessary cleanup here
        // For example, you might want to flush any remaining entries
        let producer = self.producer.clone();
        let topic = self.topic.clone();
        let entries = self.entries.clone();
        let last_flush = self.last_flush.clone();

        tokio::spawn(async move {
            let mut batch = entries.lock().await;
            if !batch.is_empty() {
                KafkaSink::send_batch(&producer, &topic, batch.drain(..).collect()).await;
                last_flush.store(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
        });

        eprintln!("Dropping KafkaSink with topic: {}", self.topic);
    }
}

#[cfg(feature = "webhook")]
/// Webhook Sink Implementation
pub struct WebhookSink {
    endpoint: String,
    auth_token: String,
    client: reqwest::Client,
    max_retries: usize,
    retry_delay_ms: u64,
}

#[cfg(feature = "webhook")]
impl WebhookSink {
    pub fn new(endpoint: String, auth_token: String, max_retries: usize, retry_delay_ms: u64) -> Self {
        WebhookSink {
            endpoint,
            auth_token,
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
            max_retries,
            retry_delay_ms,
        }
    }
}

#[cfg(feature = "webhook")]
#[async_trait]
impl Sink for WebhookSink {
    async fn write(&self, entry: &UnifiedLogEntry) {
        let mut retries = 0;
        let url = self.endpoint.clone();
        let entry_clone = entry.clone();
        let auth_value = reqwest::header::HeaderValue::from_str(format!("Bearer {}", self.auth_token.clone()).as_str()).unwrap();
        while retries < self.max_retries {
            match self
                .client
                .post(&url)
                .header(reqwest::header::AUTHORIZATION, auth_value.clone())
                .json(&entry_clone)
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => {
                    return;
                }
                _ => {
                    retries += 1;
                    if retries < self.max_retries {
                        tokio::time::sleep(tokio::time::Duration::from_millis(
                            self.retry_delay_ms * (1 << retries), // Exponential backoff
                        ))
                        .await;
                    }
                }
            }
        }

        eprintln!("Failed to send log to webhook after {} retries", self.max_retries);
    }
}

#[cfg(feature = "webhook")]
impl Drop for WebhookSink {
    fn drop(&mut self) {
        // Perform any necessary cleanup here
        // For example, you might want to log that the sink is being dropped
        eprintln!("Dropping WebhookSink with URL: {}", self.endpoint);
    }
}

#[cfg(feature = "file")]
/// File Sink Implementation
pub struct FileSink {
    path: String,
    buffer_size: usize,
    writer: Arc<tokio::sync::Mutex<io::BufWriter<tokio::fs::File>>>,
    entry_count: std::sync::atomic::AtomicUsize,
    last_flush: std::sync::atomic::AtomicU64,
    flush_interval_ms: u64, // Time between flushes
    flush_threshold: usize, // Number of entries before flush
}

#[cfg(feature = "file")]
impl FileSink {
    /// Create a new FileSink instance
    pub async fn new(
        path: String,
        buffer_size: usize,
        flush_interval_ms: u64,
        flush_threshold: usize,
    ) -> Result<Self, io::Error> {
        // check if the file exists
        let file_exists = tokio::fs::metadata(&path).await.is_ok();
        // if the file not exists, create it
        if !file_exists {
            tokio::fs::create_dir_all(std::path::Path::new(&path).parent().unwrap()).await?;
            debug!("the file not exists,create if. path: {:?}", path)
        }
        let file = if file_exists {
            // If the file exists, open it in append mode
            debug!("FileSink: File exists, opening in append mode.");
            OpenOptions::new().append(true).create(true).open(&path).await?
        } else {
            // If the file does not exist, create it
            debug!("FileSink: File does not exist, creating a new file.");
            // Create the file and write a header or initial content if needed
            OpenOptions::new().create(true).truncate(true).write(true).open(&path).await?
        };
        let writer = io::BufWriter::with_capacity(buffer_size, file);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        Ok(FileSink {
            path,
            buffer_size,
            writer: Arc::new(tokio::sync::Mutex::new(writer)),
            entry_count: std::sync::atomic::AtomicUsize::new(0),
            last_flush: std::sync::atomic::AtomicU64::new(now),
            flush_interval_ms,
            flush_threshold,
        })
    }

    #[allow(dead_code)]
    async fn initialize_writer(&mut self) -> io::Result<()> {
        let file = tokio::fs::File::create(&self.path).await?;

        // Use buffer_size to create a buffer writer with a specified capacity
        let buf_writer = io::BufWriter::with_capacity(self.buffer_size, file);

        // Replace the original writer with the new Mutex
        self.writer = Arc::new(tokio::sync::Mutex::new(buf_writer));
        Ok(())
    }

    // Get the current buffer size
    #[allow(dead_code)]
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    // How to dynamically adjust the buffer size
    #[allow(dead_code)]
    pub async fn set_buffer_size(&mut self, new_size: usize) -> io::Result<()> {
        if self.buffer_size != new_size {
            self.buffer_size = new_size;
            // Reinitialize the writer directly, without checking is_some()
            self.initialize_writer().await?;
        }
        Ok(())
    }

    // Check if flushing is needed based on count or time
    fn should_flush(&self) -> bool {
        // Check entry count threshold
        if self.entry_count.load(std::sync::atomic::Ordering::Relaxed) >= self.flush_threshold {
            return true;
        }

        // Check time threshold
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let last = self.last_flush.load(std::sync::atomic::Ordering::Relaxed);
        now - last >= self.flush_interval_ms
    }
}

#[cfg(feature = "file")]
#[async_trait]
impl Sink for FileSink {
    async fn write(&self, entry: &UnifiedLogEntry) {
        let line = format!("{:?}\n", entry);
        let mut writer = self.writer.lock().await;

        if let Err(e) = writer.write_all(line.as_bytes()).await {
            eprintln!(
                "Failed to write log to file {}: {},entry timestamp:{:?}",
                self.path,
                e,
                entry.get_timestamp()
            );
            return;
        }

        // Only flush periodically to improve performance
        // Logic to determine when to flush could be added here
        // Increment the entry count
        self.entry_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Check if we should flush
        if self.should_flush() {
            if let Err(e) = writer.flush().await {
                eprintln!("Failed to flush log file {}: {}", self.path, e);
                return;
            }

            // Reset counters
            self.entry_count.store(0, std::sync::atomic::Ordering::Relaxed);

            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            self.last_flush.store(now, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

#[cfg(feature = "file")]
impl Drop for FileSink {
    fn drop(&mut self) {
        let writer = self.writer.clone();
        let path = self.path.clone();

        tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let mut writer = writer.lock().await;
                if let Err(e) = writer.flush().await {
                    eprintln!("Failed to flush log file {}: {}", path, e);
                }
            });
        });
    }
}

/// Create a list of Sink instances
pub async fn create_sinks(config: &AppConfig) -> Vec<Arc<dyn Sink>> {
    let mut sinks: Vec<Arc<dyn Sink>> = Vec::new();

    #[cfg(feature = "kafka")]
    if config.sinks.kafka.enabled {
        match rdkafka::config::ClientConfig::new()
            .set("bootstrap.servers", &config.sinks.kafka.bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .create()
        {
            Ok(producer) => {
                sinks.push(Arc::new(KafkaSink::new(
                    producer,
                    config.sinks.kafka.topic.clone(),
                    config.sinks.kafka.batch_size.unwrap_or(100),
                    config.sinks.kafka.batch_timeout_ms.unwrap_or(1000),
                )));
            }
            Err(e) => eprintln!("Failed to create Kafka producer: {}", e),
        }
    }

    #[cfg(feature = "webhook")]
    if config.sinks.webhook.enabled {
        sinks.push(Arc::new(WebhookSink::new(
            config.sinks.webhook.endpoint.clone(),
            config.sinks.webhook.auth_token.clone(),
            config.sinks.webhook.max_retries.unwrap_or(3),
            config.sinks.webhook.retry_delay_ms.unwrap_or(100),
        )));
    }

    #[cfg(feature = "file")]
    {
        let path = if config.sinks.file.enabled {
            config.sinks.file.path.clone()
        } else {
            "default.log".to_string()
        };
        debug!("FileSink: Using path: {}", path);
        sinks.push(Arc::new(
            FileSink::new(
                path.clone(),
                config.sinks.file.buffer_size.unwrap_or(8192),
                config.sinks.file.flush_interval_ms.unwrap_or(1000),
                config.sinks.file.flush_threshold.unwrap_or(100),
            )
            .await
            .unwrap(),
        ));
    }

    sinks
}
