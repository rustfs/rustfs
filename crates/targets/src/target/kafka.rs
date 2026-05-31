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
    StoreError, Target,
    arn::TargetID,
    error::TargetError,
    runtime::tls::{
        ReloadableTargetTls, TargetTlsInputSet, TlsReloadAdapter, config::ReloadApplyMode, fingerprint::TargetTlsGeneration,
        validate::validate_tls_material,
    },
    store::{Key, Store},
    target::{
        ChannelTargetType, EntityTarget, QueuedPayload, QueuedPayloadMeta, TargetDeliveryCounters, TargetDeliverySnapshot,
        TargetTlsState, TargetType, build_queued_payload, build_target_tls_fingerprint, invalidate_cache_on_connectivity_error,
        open_target_queue_store, persist_queued_payload_to_store,
    },
};
use async_trait::async_trait;
use rustfs_kafka_async::error::{ConnectionError, Error as KafkaError};
use rustfs_kafka_async::{AsyncProducer, AsyncProducerConfig, Record, RequiredAcks, SaslConfig, SecurityConfig};
use rustfs_tls_runtime::{load_cert_bundle_der_bytes, load_private_key};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::{fmt, marker::PhantomData, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tracing::{debug, error, info, instrument, warn};

pub(crate) const KAFKA_SASL_PLAIN: &str = "PLAIN";
pub(crate) const KAFKA_SASL_SCRAM_SHA_256: &str = "SCRAM-SHA-256";
pub(crate) const KAFKA_SASL_SCRAM_SHA_512: &str = "SCRAM-SHA-512";

/// Arguments for configuring a Kafka target
#[derive(Clone)]
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
    /// Whether to enable SASL authentication over the TLS transport
    pub sasl_enable: bool,
    /// SASL mechanism (PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512)
    pub sasl_mechanism: String,
    /// SASL username
    pub sasl_username: String,
    /// SASL password
    pub sasl_password: String,
    /// The directory to store events in case of failure
    pub queue_dir: String,
    /// The maximum number of events to store
    pub queue_limit: u64,
    /// The target type (audit or notify)
    pub target_type: TargetType,
}

impl fmt::Debug for KafkaArgs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaArgs")
            .field("enable", &self.enable)
            .field("brokers", &self.brokers)
            .field("topic", &self.topic)
            .field("acks", &self.acks)
            .field("tls_enable", &self.tls_enable)
            .field("tls_ca", &self.tls_ca)
            .field("tls_client_cert", &self.tls_client_cert)
            .field(
                "tls_client_key",
                if self.tls_client_key.is_empty() {
                    &""
                } else {
                    &"***REDACTED***"
                },
            )
            .field("sasl_enable", &self.sasl_enable)
            .field("sasl_mechanism", &self.sasl_mechanism)
            .field("sasl_username", &self.sasl_username)
            .field(
                "sasl_password",
                if self.sasl_password.is_empty() {
                    &""
                } else {
                    &"***REDACTED***"
                },
            )
            .field("queue_dir", &self.queue_dir)
            .field("queue_limit", &self.queue_limit)
            .field("target_type", &self.target_type)
            .finish()
    }
}

fn normalize_kafka_sasl_mechanism(mechanism: &str) -> Result<&'static str, TargetError> {
    let mechanism = mechanism.trim();
    if mechanism.is_empty() || mechanism.eq_ignore_ascii_case(KAFKA_SASL_PLAIN) {
        return Ok(KAFKA_SASL_PLAIN);
    }
    if mechanism.eq_ignore_ascii_case(KAFKA_SASL_SCRAM_SHA_256) {
        return Ok(KAFKA_SASL_SCRAM_SHA_256);
    }
    if mechanism.eq_ignore_ascii_case(KAFKA_SASL_SCRAM_SHA_512) {
        return Ok(KAFKA_SASL_SCRAM_SHA_512);
    }
    Err(TargetError::Configuration(
        "kafka sasl_mechanism must be one of: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512".to_string(),
    ))
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

        if self.sasl_enable {
            if !self.tls_enable {
                return Err(TargetError::Configuration(
                    "kafka sasl_enable requires tls_enable for SASL_SSL".to_string(),
                ));
            }
            normalize_kafka_sasl_mechanism(&self.sasl_mechanism)?;
            if self.sasl_username.is_empty() || self.sasl_password.is_empty() {
                return Err(TargetError::Configuration(
                    "kafka sasl_username and sasl_password must be specified when sasl_enable is true".to_string(),
                ));
            }
        } else if !self.sasl_mechanism.is_empty() || !self.sasl_username.is_empty() || !self.sasl_password.is_empty() {
            return Err(TargetError::Configuration(
                "kafka sasl_enable must be true when SASL fields are specified".to_string(),
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

    pub(crate) fn security_config(&self, validate_tls_files: bool) -> Result<Option<SecurityConfig>, TargetError> {
        if !self.tls_enable && !self.sasl_enable {
            return Ok(None);
        }

        let mut security = SecurityConfig::new();
        if !self.tls_ca.is_empty() {
            if validate_tls_files {
                let certs = load_cert_bundle_der_bytes(&self.tls_ca)
                    .map_err(|e| TargetError::Configuration(format!("Failed to parse Kafka tls_ca: {e}")))?;
                if certs.is_empty() {
                    return Err(TargetError::Configuration(
                        "Kafka tls_ca did not contain any parsable certificates".to_string(),
                    ));
                }
            }
            security = security.with_ca_cert(self.tls_ca.clone());
        }
        if !self.tls_client_cert.is_empty() && !self.tls_client_key.is_empty() {
            if validate_tls_files {
                let certs = load_cert_bundle_der_bytes(&self.tls_client_cert)
                    .map_err(|e| TargetError::Configuration(format!("Failed to parse Kafka tls_client_cert: {e}")))?;
                if certs.is_empty() {
                    return Err(TargetError::Configuration(
                        "Kafka tls_client_cert did not contain any parsable certificates".to_string(),
                    ));
                }
                let _ = load_private_key(&self.tls_client_key)
                    .map_err(|e| TargetError::Configuration(format!("Failed to parse Kafka tls_client_key: {e}")))?;
            }
            security = security.with_client_cert(self.tls_client_cert.clone(), self.tls_client_key.clone());
        }
        if self.sasl_enable {
            security = security.with_sasl(SaslConfig::new(
                normalize_kafka_sasl_mechanism(&self.sasl_mechanism)?.to_string(),
                self.sasl_username.clone(),
                self.sasl_password.clone(),
            ));
        }

        Ok(Some(security))
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
    tls_state: Arc<Mutex<TargetTlsState>>,
    /// Adapter that bridges this target to the TLS reload coordinator.
    /// When `Some`, the target uses coordinator-managed material; when `None`,
    /// it falls back to inline fingerprint-based change detection.
    tls_adapter: Option<TlsReloadAdapter<Arc<AsyncProducer>>>,
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

    /// Creates a new KafkaTarget
    #[instrument(skip(args), fields(target_id = %id))]
    pub fn new(id: String, args: KafkaArgs) -> Result<Self, TargetError> {
        args.validate()?;

        let target_id = TargetID::new(id, ChannelTargetType::Kafka.as_str().to_string());

        let queue_store = open_target_queue_store(
            &args.queue_dir,
            args.queue_limit,
            args.target_type,
            ChannelTargetType::Kafka.as_str(),
            &target_id,
            "Failed to open store for Kafka target",
        )?;

        info!(target_id = %target_id.id, "Kafka target created");
        Ok(KafkaTarget {
            id: target_id,
            args,
            store: queue_store,
            producer: Arc::new(Mutex::new(None)),
            tls_state: Arc::new(Mutex::new(TargetTlsState::default())),
            tls_adapter: None,
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

        if let Some(security) = self.args.security_config(true)? {
            config = config.with_security(security);
        }

        AsyncProducer::from_hosts_with_config(self.args.brokers.clone(), config)
            .await
            .map_err(|e| Self::map_kafka_error(e, "Failed to create Kafka producer"))
    }

    async fn get_or_build_producer(&self) -> Result<Arc<AsyncProducer>, TargetError> {
        // Adapter-managed path: use the material directly from the TLS reload adapter.
        if let Some(adapter) = &self.tls_adapter {
            let producer: Arc<AsyncProducer> = (*adapter.current_material()).clone();

            // Ensure the producer is also stored locally so that close() can drain it.
            {
                let mut guard = self.producer.lock().await;
                *guard = Some(Arc::clone(&producer));
            }
            return Ok(producer);
        }

        // Inline fingerprint fallback path (no coordinator).
        let next_fingerprint =
            build_target_tls_fingerprint(&self.args.tls_ca, &self.args.tls_client_cert, &self.args.tls_client_key).await?;
        let tls_changed = {
            let tls_state_guard = self.tls_state.lock().await;
            tls_state_guard.needs_update(&next_fingerprint)
        };
        if tls_changed {
            let mut cached = self.producer.lock().await;
            *cached = None;
            self.tls_state.lock().await.refresh(next_fingerprint);
        }

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
        self.tls_state.lock().await.reset();
    }

    /// Serializes the event and builds a QueuedPayload
    fn build_queued_payload(&self, event: &EntityTarget<E>) -> Result<QueuedPayload, TargetError> {
        build_queued_payload(event)
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
            invalidate_cache_on_connectivity_error(&mapped, || self.invalidate_cached_producer()).await;
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
            tls_state: Arc::clone(&self.tls_state),
            tls_adapter: self.tls_adapter.clone(),
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
            if let Err(e) = persist_queued_payload_to_store(store.as_ref(), &queued) {
                self.delivery_counters.record_final_failure();
                return Err(e);
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
        {
            let mut guard = self.producer.lock().await;
            *guard = None;
        }

        self.tls_state.lock().await.reset();

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

/// Coordinated TLS hot-reload implementation for Kafka targets.
///
/// The coordinator calls these methods on a background poll loop to detect
/// TLS file changes and rebuild the producer without restarting.
#[async_trait]
impl<E> ReloadableTargetTls for KafkaTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    type Material = Arc<AsyncProducer>;

    fn tls_input_set(&self) -> TargetTlsInputSet {
        TargetTlsInputSet {
            ca_path: self.args.tls_ca.clone(),
            client_cert_path: self.args.tls_client_cert.clone(),
            client_key_path: self.args.tls_client_key.clone(),
            target_label: format!("kafka:{}", self.id.id),
        }
    }

    async fn build_tls_material(&self) -> Result<Self::Material, TargetError> {
        let producer = self.build_producer().await?;
        Ok(Arc::new(producer))
    }

    async fn apply_tls_material(
        &self,
        _generation: TargetTlsGeneration,
        material: Arc<Self::Material>,
        _mode: ReloadApplyMode,
    ) -> Result<(), TargetError> {
        let mut guard = self.producer.lock().await;
        *guard = Some((*material).clone());
        Ok(())
    }

    async fn validate_tls_files(&self) -> Result<(), TargetError> {
        validate_tls_material(&self.args.tls_ca, &self.args.tls_client_cert, &self.args.tls_client_key)
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
            sasl_enable: false,
            sasl_mechanism: String::new(),
            sasl_username: String::new(),
            sasl_password: String::new(),
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

    #[test]
    fn test_validate_sasl_requires_tls() {
        let args = KafkaArgs {
            sasl_enable: true,
            sasl_mechanism: KAFKA_SASL_SCRAM_SHA_512.to_string(),
            sasl_username: "user".to_string(),
            sasl_password: "secret".to_string(),
            ..base_args()
        };
        let err = args.validate().expect_err("SASL without TLS should fail");
        assert!(err.to_string().contains("requires tls_enable"));
    }

    #[test]
    fn test_validate_sasl_requires_username_and_password() {
        let args = KafkaArgs {
            tls_enable: true,
            sasl_enable: true,
            sasl_mechanism: KAFKA_SASL_PLAIN.to_string(),
            sasl_username: "user".to_string(),
            sasl_password: String::new(),
            ..base_args()
        };
        let err = args.validate().expect_err("SASL credentials should be paired");
        assert!(err.to_string().contains("sasl_username and sasl_password"));
    }

    #[test]
    fn test_validate_sasl_rejects_unsupported_mechanism() {
        let args = KafkaArgs {
            tls_enable: true,
            sasl_enable: true,
            sasl_mechanism: "OAUTHBEARER".to_string(),
            sasl_username: "user".to_string(),
            sasl_password: "secret".to_string(),
            ..base_args()
        };
        let err = args.validate().expect_err("unsupported SASL mechanism should fail");
        assert!(err.to_string().contains("sasl_mechanism must be one of"));
    }

    #[test]
    fn test_security_config_includes_sasl() {
        let args = KafkaArgs {
            tls_enable: true,
            sasl_enable: true,
            sasl_mechanism: "scram-sha-512".to_string(),
            sasl_username: "user".to_string(),
            sasl_password: "secret".to_string(),
            ..base_args()
        };

        let security = args
            .security_config(false)
            .expect("valid security config")
            .expect("security should be configured");
        let sasl = security.sasl_config().expect("SASL should be configured");

        assert_eq!(sasl.mechanism(), KAFKA_SASL_SCRAM_SHA_512);
        assert_eq!(sasl.username(), "user");
        assert_eq!(sasl.password(), "secret");
    }

    #[test]
    fn test_debug_redacts_sasl_password_and_tls_key() {
        let rendered = format!(
            "{:?}",
            KafkaArgs {
                tls_client_key: "/tmp/client.key".to_string(),
                sasl_enable: true,
                sasl_password: "super-secret".to_string(),
                ..base_args()
            }
        );

        assert!(!rendered.contains("super-secret"));
        assert!(!rendered.contains("/tmp/client.key"));
        assert!(rendered.contains("***REDACTED***"));
    }
}
