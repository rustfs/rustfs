use crate::config::kafka::KafkaConfig;
use crate::config::{default_queue_limit, DEFAULT_RETRY_INTERVAL, STORE_PREFIX};
use crate::{ChannelAdapter, ChannelAdapterType};
use crate::{Error, Event, QueueStore};
use async_trait::async_trait;
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::util::Timeout;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use ChannelAdapterType::Kafka;

/// Kafka adapter for sending events to a Kafka topic.
pub struct KafkaAdapter {
    producer: FutureProducer,
    store: Option<Arc<QueueStore<Event>>>,
    config: KafkaConfig,
}

impl KafkaAdapter {
    /// Creates a new Kafka adapter.
    pub fn new(config: &KafkaConfig) -> Result<Self, Error> {
        // Create a Kafka producer with the provided configuration.
        let producer = rdkafka::config::ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("message.timeout.ms", config.timeout.to_string())
            .create()
            .map_err(|e| Error::msg(format!("Failed to create a Kafka producer: {}", e)))?;

        // create a queue store if enabled
        let store = if !config.common.queue_dir.is_empty() {
            let store_path = PathBuf::from(&config.common.queue_dir).join(format!(
                "{}-{}-{}",
                STORE_PREFIX,
                Kafka.as_str(),
                config.common.identifier
            ));

            let queue_limit = if config.queue_limit > 0 {
                config.queue_limit
            } else {
                default_queue_limit()
            };
            let store = QueueStore::new(store_path, config.queue_limit, Some(".event".to_string()));
            if let Err(e) = store.open() {
                tracing::error!("Unable to open queue storage: {}", e);
                None
            } else {
                Some(Arc::new(store))
            }
        } else {
            None
        };

        Ok(Self { config, producer, store })
    }

    /// Handle backlog events in storage
    pub async fn process_backlog(&self) -> Result<(), Error> {
        if let Some(store) = &self.store {
            let keys = store.list();

            for key in keys {
                match store.get_multiple(&key) {
                    Ok(events) => {
                        for event in events {
                            // Use the retry interval to send events
                            if let Err(e) = self.send_with_retry(&event).await {
                                tracing::error!("Processing of backlog events failed: {}", e);
                                // If it still fails, we remain in the queue
                                break;
                            }
                        }

                        // The event is deleted after it has been successfully processed
                        if let Err(e) = store.del(&key) {
                            tracing::error!("Failed to delete a handled event: {}", e);
                        }
                    }
                    Err(e) => {
                        tracing::error!("Fetch events from the queue failed: {}", e);

                        // If the event cannot be read, it may be corrupted, delete it
                        if let Err(del_err) = store.del(&key) {
                            tracing::error!("Failed to delete a corrupted event: {}", del_err);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Sends an event to the Kafka topic with retry logic.
    async fn send_with_retry(&self, event: &Event) -> Result<(), Error> {
        let retry_interval = match self.config.retry_interval {
            Some(t) => Duration::from_secs(t),
            None => Duration::from_secs(DEFAULT_RETRY_INTERVAL), // Default to 3 seconds if not set
        };

        for attempt in 0..self.max_retries {
            match self.send_request(event).await {
                Ok(_) => return Ok(()),
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _)) => {
                    tracing::warn!("Kafka attempt {} failed: Queue full. Retrying...", attempt + 1);
                    // sleep(Duration::from_secs(2u64.pow(attempt))).await;
                    sleep(retry_interval).await;
                }
                Err((e, _)) => {
                    tracing::error!("Kafka send error: {}", e);
                    return Err(Error::Kafka(e));
                }
            }
        }

        Err(Error::Custom("Exceeded maximum retry attempts for Kafka message".to_string()))
    }

    /// Send a single Kafka message
    async fn send_request(&self, event: &Event) -> Result<(), Error> {
        // Serialize events
        let payload = serde_json::to_string(event).map_err(|e| Error::Custom(format!("Serialization event failed: {}", e)))?;

        // Create a Kafka record
        let record = FutureRecord::to(&self.config.topic).payload(&payload).key(&event.id); // Use the event ID as the key

        // Send to Kafka
        let delivery_status = self
            .producer
            .send(record, Duration::from_millis(self.config.timeout))
            .await
            .map_err(|(e, _)| Error::Custom(format!("Failed to send to Kafka: {}", e)))?;
        // Check delivery status
        if let Some((err, _)) = delivery_status {
            return Err(Error::Kafka(err));
        }

        Ok(())
    }

    /// Save the event to the queue
    async fn save_to_queue(&self, event: &Event) -> Result<(), Error> {
        if let Some(store) = &self.store {
            store
                .put(event.clone())
                .map_err(|e| Error::Custom(format!("Saving events to queue failed: {}", e)))?;
        }
        Ok(())
    }
}

#[async_trait]
impl ChannelAdapter for KafkaAdapter {
    fn name(&self) -> String {
        ChannelAdapterType::Kafka.to_string()
    }

    async fn send(&self, event: &Event) -> Result<(), Error> {
        // Try to deal with the backlog of events first
        let _ = self.process_backlog().await;

        // An attempt was made to send the current event
        match self.send_with_retry(event).await {
            Ok(_) => Ok(()),
            Err(e) => {
                // If the send fails and the queue is enabled, save to the queue
                if let Some(_) = &self.store {
                    tracing::warn!("Failed to send events to Kafka and saved to a queue: {}", e);
                    self.save_to_queue(event).await?;
                    return Ok(());
                }
                Err(e)
            }
        }
    }
}
