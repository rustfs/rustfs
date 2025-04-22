use crate::ChannelAdapter;
use crate::Error;
use crate::Event;
use crate::KafkaConfig;
use async_trait::async_trait;
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::util::Timeout;
use std::time::Duration;
use tokio::time::sleep;

/// Kafka adapter for sending events to a Kafka topic.
pub struct KafkaAdapter {
    producer: FutureProducer,
    topic: String,
    max_retries: u32,
}

impl KafkaAdapter {
    /// Creates a new Kafka adapter.
    pub fn new(config: &KafkaConfig) -> Result<Self, Error> {
        // Create a Kafka producer with the provided configuration.
        let producer = rdkafka::config::ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("message.timeout.ms", config.timeout.to_string())
            .create()?;

        Ok(Self {
            producer,
            topic: config.topic.clone(),
            max_retries: config.max_retries,
        })
    }
    /// Sends an event to the Kafka topic with retry logic.
    async fn send_with_retry(&self, event: &Event) -> Result<(), Error> {
        let event_id = event.id.to_string();
        let payload = serde_json::to_string(&event)?;

        for attempt in 0..self.max_retries {
            let record = FutureRecord::to(&self.topic).key(&event_id).payload(&payload);

            match self.producer.send(record, Timeout::Never).await {
                Ok(_) => return Ok(()),
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _)) => {
                    tracing::warn!("Kafka attempt {} failed: Queue full. Retrying...", attempt + 1);
                    sleep(Duration::from_secs(2u64.pow(attempt))).await;
                }
                Err((e, _)) => {
                    tracing::error!("Kafka send error: {}", e);
                    return Err(Error::Kafka(e));
                }
            }
        }

        Err(Error::Custom("Exceeded maximum retry attempts for Kafka message".to_string()))
    }
}

#[async_trait]
impl ChannelAdapter for KafkaAdapter {
    fn name(&self) -> String {
        "kafka".to_string()
    }

    async fn send(&self, event: &Event) -> Result<(), Error> {
        self.send_with_retry(event).await
    }
}
