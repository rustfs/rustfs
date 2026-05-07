// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::{
    StoreError, Target, TargetLog,
    arn::TargetID,
    error::TargetError,
    store::{Key, QueueStore, Store},
    target::{
        ChannelTargetType, EntityTarget, QueuedPayload, QueuedPayloadMeta, TargetDeliveryCounters, TargetDeliverySnapshot,
        TargetType, queue_store_subdir_name,
    },
};
use async_trait::async_trait;
use rustfs_config::audit::AUDIT_STORE_EXTENSION;
use rustfs_config::notify::NOTIFY_STORE_EXTENSION;
use rustfs_kafka_async::error::{ConnectionError, Error as KafkaError};
use rustfs_kafka_async::{AsyncProducer, AsyncProducerConfig, Record, RequiredAcks, SecurityConfig};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::{marker::PhantomData, path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tracing::{debug, error, info, instrument, warn};

/// Arguments for configuring a Kafka target
#[derive(Debug, Clone)]
pub struct KafkaArgs {
    /// Whether the target is enabled
    pub enable: bool,
    /// Comma-separated list of broker addresses (e.g. "localhost:9092,broker2:9092")
    pub brokers: Vec<String>,
    /// The topic to publish events to
    pub topic: String,
    /// Required acks: 0 = none, 1 = leader, -1 = all
    pub acks: i16,
    /// Whether to enable TLS for Kafka transport
    pub tls_enable: bool,
    /// Optional path to CA cert used for broker verification
    pub tls_ca: String,
    /// Optional path to client certificate for mTLS
    pub tls_client_cert: String,
    /// Optional path to client private key for mTLS
    pub tls_client_key: String,
    /// The directory to store events in case of failure
    pub queue_dir: String,
    /// The maximum number of events to store
    pub queue_limit: u64,
    /// The target type (audit or notify)
    pub target_type: TargetType,
}

impl KafkaArgs {
    /// Validates the KafkaArgs configuration
    pub fn validate(&self) -> Result<(), TargetError> {
        if !self.enable {
            return Ok(());
        }

        if self.brokers.is_empty() {
            return Err(TargetError::Configuration("kafka brokers cannot be empty".to_string()));
        }

        if self.topic.is_empty() {
            return Err(TargetError::Configuration("kafka topic cannot be empty".to_string()));
        }

        if !matches!(self.acks, -1..=1) {
            return Err(TargetError::Configuration("kafka acks must be one of: 0, 1, -1".to_string()));
        }

        if self.tls_client_cert.is_empty() != self.tls_client_key.is_empty() {
            return Err(TargetError::Configuration(
                "kafka tls_client_cert and tls_client_key must be specified together".to_string(),
            ));
        }

        if !self.queue_dir.is_empty() {
            let path = std::path::Path::new(&self.queue_dir);
            if !path.is_absolute() {
                return Err(TargetError::Configuration("kafka queueDir path should be absolute".to_string()));
            }
        }

        Ok(())
    }
}

/// A target that sends events to an Apache Kafka topic
pub struct KafkaTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    id: TargetID,
    args: KafkaArgs,
    store: Option<Box<dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync>>,
    producer: Arc<Mutex<Option<Arc<AsyncProducer>>>>,
    delivery_counters: Arc<TargetDeliveryCounters>,
    _phantom: PhantomData<E>,
}

impl<E> KafkaTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    fn map_kafka_error(err: KafkaError, context: &str) -> TargetError {
        match err {
            KafkaError::Connection(ConnectionError::NoHostReachable) => TargetError::NotConnected,
            KafkaError::Connection(ConnectionError::Timeout(_)) => TargetError::Timeout(format!("{context}: {err}")),
            KafkaError::Connection(_) => TargetError::Network(format!("{context}: {err}")),
            KafkaError::Config(_) => TargetError::Configuration(format!("{context}: {err}")),
            _ => TargetError::Request(format!("{context}: {err}")),
        }
    }

    fn is_connection_error(err: &TargetError) -> bool {
        matches!(err, TargetError::NotConnected | TargetError::Timeout(_) | TargetError::Network(_))
    }

    /// Creates a new KafkaTarget
    #[instrument(skip(args), fields(target_id = %id))]
    pub fn new(id: String, args: KafkaArgs) -> Result<Self, TargetError> {
        args.validate()?;

        let target_id = TargetID::new(id, ChannelTargetType::Kafka.as_str().to_string());

        let queue_store = if !args.queue_dir.is_empty() {
            let queue_dir =
                PathBuf::from(&args.queue_dir).join(queue_store_subdir_name(ChannelTargetType::Kafka.as_str(), &target_id.id));

            let extension = match args.target_type {
                TargetType::AuditLog => AUDIT_STORE_EXTENSION,
                TargetType::NotifyEvent => NOTIFY_STORE_EXTENSION,
            };

            let store = QueueStore::<QueuedPayload>::new(queue_dir, args.queue_limit, extension);
            if let Err(e) = store.open() {
                error!("Failed to open store for Kafka target {}: {}", target_id.id, e);
                return Err(TargetError::Storage(format!("{e}")));
            }

            Some(Box::new(store) as Box<dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync>)
        } else {
            None
        };

        info!(target_id = %target_id.id, "Kafka target created");
        Ok(KafkaTarget {
            id: target_id,
            args,
            store: queue_store,
            producer: Arc::new(Mutex::new(None)),
            delivery_counters: Arc::new(TargetDeliveryCounters::default()),
            _phantom: PhantomData,
        })
    }

    /// Builds a Kafka producer from the current args
    async fn build_producer(&self) -> Result<AsyncProducer, TargetError> {
        let acks = match self.args.acks {
            0 => RequiredAcks::None,
            1 => RequiredAcks::One,
            _ => RequiredAcks::All,
        };

        let mut config = AsyncProducerConfig::new()
            .with_ack_timeout(Duration::from_secs(30))
            .with_required_acks(acks);

        if self.args.tls_enable {
            let mut security = SecurityConfig::new();
            if !self.args.tls_ca.is_empty() {
                security = security.with_ca_cert(self.args.tls_ca.clone());
            }
            if !self.args.tls_client_cert.is_empty() && !self.args.tls_client_key.is_empty() {
                security = security.with_client_cert(self.args.tls_client_cert.clone(), self.args.tls_client_key.clone());
            }
            config = config.with_security(security);
        }

        AsyncProducer::from_hosts_with_config(self.args.brokers.clone(), config)
            .await
            .map_err(|e| Self::map_kafka_error(e, "Failed to create Kafka producer"))
    }

    async fn get_or_build_producer(&self) -> Result<Arc<AsyncProducer>, TargetError> {
        let mut cached = self.producer.lock().await;
        if let Some(producer) = cached.as_ref() {
            return Ok(Arc::clone(producer));
        }

        let producer = Arc::new(self.build_producer().await?);
        *cached = Some(Arc::clone(&producer));
        Ok(producer)
    }

    async fn invalidate_cached_producer(&self) {
        let mut cached = self.producer.lock().await;
        *cached = None;
    }

    /// Serializes the event and builds a QueuedPayload
    fn build_queued_payload(&self, event: &EntityTarget<E>) -> Result<QueuedPayload, TargetError> {
        let object_name = crate::target::decode_object_name(&event.object_name)?;
        let key = format!("{}/{}", event.bucket_name, object_name);

        let log = TargetLog {
            event_name: event.event_name,
            key,
            records: vec![event.data.clone()],
        };

        let body = serde_json::to_vec(&log).map_err(|e| TargetError::Serialization(format!("Failed to serialize event: {e}")))?;

        let meta = QueuedPayloadMeta::new(
            event.event_name,
            event.bucket_name.clone(),
            event.object_name.clone(),
            "application/json",
            body.len(),
        );

        Ok(QueuedPayload::new(meta, body))
    }

    /// Sends the raw body to Kafka
    #[instrument(skip(self, body, meta), fields(target_id = %self.id))]
    async fn send_body(&self, body: Vec<u8>, meta: &QueuedPayloadMeta) -> Result<(), TargetError> {
        debug!(
            target = %self.id,
            bucket = %meta.bucket_name,
            object = %meta.object_name,
            event = %meta.event_name,
            payload_len = body.len(),
            "Sending Kafka payload"
        );

        let producer = self.get_or_build_producer().await?;

        if let Err(err) = producer.send(&Record::from_value(&self.args.topic, body.as_slice())).await {
            let mapped = Self::map_kafka_error(err, "Failed to send message to Kafka");
            if Self::is_connection_error(&mapped) {
                self.invalidate_cached_producer().await;
            }
            return Err(mapped);
        }

        debug!(target_id = %self.id, topic = %self.args.topic, "Event published to Kafka topic");
        self.delivery_counters.record_success();
        Ok(())
    }

    /// Clones this target into a boxed trait object
    pub fn clone_box(&self) -> Box<dyn Target<E> + Send + Sync> {
        Box::new(KafkaTarget::<E> {
            id: self.id.clone(),
            args: self.args.clone(),
            store: self.store.as_ref().map(|s| s.boxed_clone()),
            producer: Arc::clone(&self.producer),
            delivery_counters: Arc::clone(&self.delivery_counters),
            _phantom: PhantomData,
        })
    }
}

#[async_trait]
impl<E> Target<E> for KafkaTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    fn id(&self) -> TargetID {
        self.id.clone()
    }

    async fn is_active(&self) -> Result<bool, TargetError> {
        let _ = self.get_or_build_producer().await?;
        Ok(true)
    }

    async fn save(&self, event: Arc<EntityTarget<E>>) -> Result<(), TargetError> {
        let queued = match self.build_queued_payload(&event) {
            Ok(queued) => queued,
            Err(err) => {
                self.delivery_counters.record_final_failure();
                return Err(err);
            }
        };

        if let Some(store) = &self.store {
            let encoded = match queued.encode() {
                Ok(encoded) => encoded,
                Err(err) => {
                    self.delivery_counters.record_final_failure();
                    return Err(TargetError::Storage(format!("Failed to encode queued payload: {err}")));
                }
            };
            if let Err(e) = store.put_raw(&encoded) {
                self.delivery_counters.record_final_failure();
                return Err(TargetError::Storage(format!("Failed to save event to store: {e}")));
            }
            debug!("Event saved to store for Kafka target: {}", self.id);
            Ok(())
        } else {
            if let Err(err) = self.send_body(queued.body, &queued.meta).await {
                self.delivery_counters.record_final_failure();
                return Err(err);
            }
            Ok(())
        }
    }

    async fn send_raw_from_store(&self, key: Key, body: Vec<u8>, meta: QueuedPayloadMeta) -> Result<(), TargetError> {
        debug!("Sending queued payload from store for Kafka target: {}, key: {}", self.id, key);

        if let Err(e) = self.send_body(body, &meta).await {
            if matches!(e, TargetError::NotConnected) {
                warn!(target_id = %self.id, "Kafka not reachable, event remains in store.");
                return Err(TargetError::NotConnected);
            }
            error!(target_id = %self.id, error = %e, "Failed to send event from store.");
            return Err(e);
        }

        debug!("Event sent from store for Kafka target: {}", self.id);
        Ok(())
    }

    async fn close(&self) -> Result<(), TargetError> {
        info!("Kafka target closed: {}", self.id);
        Ok(())
    }

    fn store(&self) -> Option<&(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync)> {
        self.store.as_deref()
    }

    fn clone_dyn(&self) -> Box<dyn Target<E> + Send + Sync> {
        self.clone_box()
    }

    fn is_enabled(&self) -> bool {
        self.args.enable
    }

    fn delivery_snapshot(&self) -> TargetDeliverySnapshot {
        self.delivery_counters
            .snapshot(self.store.as_deref().map_or(0, |store| store.len() as u64))
    }

    fn record_final_failure(&self) {
        self.delivery_counters.record_final_failure();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_args() -> KafkaArgs {
        KafkaArgs {
            enable: true,
            brokers: vec!["localhost:9092".to_string()],
            topic: "rustfs-events".to_string(),
            acks: 1,
            tls_enable: false,
            tls_ca: String::new(),
            tls_client_cert: String::new(),
            tls_client_key: String::new(),
            queue_dir: String::new(),
            queue_limit: 0,
            target_type: TargetType::NotifyEvent,
        }
    }

    #[test]
    fn test_validate_empty_brokers() {
        let args = KafkaArgs {
            brokers: vec![],
            ..base_args()
        };
        assert!(args.validate().is_err());
    }

    #[test]
    fn test_validate_empty_topic() {
        let args = KafkaArgs {
            topic: String::new(),
            ..base_args()
        };
        assert!(args.validate().is_err());
    }

    #[test]
    fn test_validate_relative_queue_dir() {
        let args = KafkaArgs {
            queue_dir: "relative/path".to_string(),
            ..base_args()
        };
        assert!(args.validate().is_err());
    }

    #[test]
    fn test_validate_valid_args() {
        assert!(base_args().validate().is_ok());
    }

    #[test]
    fn test_validate_disabled_target_skips_validation() {
        let args = KafkaArgs {
            enable: false,
            brokers: vec![],
            topic: String::new(),
            ..base_args()
        };
        assert!(args.validate().is_ok());
    }

    #[test]
    fn test_validate_tls_client_cert_and_key_must_be_paired() {
        let args = KafkaArgs {
            tls_client_cert: "/tmp/client.crt".to_string(),
            tls_client_key: String::new(),
            ..base_args()
        };
        assert!(args.validate().is_err());
    }
}
