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
use pulsar::{Authentication, Producer, Pulsar, TokioExecutor};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Mutex as AsyncMutex;
use tracing::{error, info, instrument};
use url::Url;

#[derive(Debug, Clone)]
pub struct PulsarArgs {
    pub enable: bool,
    pub broker: String,
    pub topic: String,
    pub auth_token: String,
    pub username: String,
    pub password: String,
    pub tls_ca: String,
    pub tls_allow_insecure: bool,
    pub tls_hostname_verification: bool,
    pub queue_dir: String,
    pub queue_limit: u64,
    pub target_type: TargetType,
}

impl PulsarArgs {
    pub fn validate(&self) -> Result<(), TargetError> {
        if !self.enable {
            return Ok(());
        }

        validate_pulsar_broker(&self.broker)?;

        if self.topic.trim().is_empty() {
            return Err(TargetError::Configuration("Pulsar topic cannot be empty".to_string()));
        }

        if !self.auth_token.is_empty() && (!self.username.is_empty() || !self.password.is_empty()) {
            return Err(TargetError::Configuration(
                "Pulsar supports either auth_token or username/password auth, not both".to_string(),
            ));
        }

        if self.username.is_empty() != self.password.is_empty() {
            return Err(TargetError::Configuration(
                "Pulsar username and password must be specified together".to_string(),
            ));
        }

        if !self.tls_ca.is_empty() && !Path::new(&self.tls_ca).is_absolute() {
            return Err(TargetError::Configuration("Pulsar tls_ca must be an absolute path".to_string()));
        }

        if !self.queue_dir.is_empty() && !Path::new(&self.queue_dir).is_absolute() {
            return Err(TargetError::Configuration("Pulsar queue directory must be an absolute path".to_string()));
        }

        let parsed = Url::parse(&self.broker)
            .map_err(|e| TargetError::Configuration(format!("Invalid Pulsar broker URL: {e} (value: '{}')", self.broker)))?;
        let tls_enabled = parsed.scheme() == "pulsar+ssl";
        if !tls_enabled && (!self.tls_ca.is_empty() || self.tls_allow_insecure || !self.tls_hostname_verification) {
            return Err(TargetError::Configuration(
                "Pulsar TLS settings are only allowed with pulsar+ssl brokers".to_string(),
            ));
        }

        Ok(())
    }
}

pub fn validate_pulsar_broker(broker: &str) -> Result<Url, TargetError> {
    let url = Url::parse(broker)
        .map_err(|e| TargetError::Configuration(format!("Invalid Pulsar broker URL: {e} (value: '{broker}')")))?;

    match url.scheme() {
        "pulsar" | "pulsar+ssl" => {}
        _ => {
            return Err(TargetError::Configuration(
                "Pulsar broker must use pulsar:// or pulsar+ssl://".to_string(),
            ));
        }
    }

    if !url.username().is_empty() || url.password().is_some() {
        return Err(TargetError::Configuration(
            "Pulsar broker URL must not embed username or password".to_string(),
        ));
    }

    if url.host_str().is_none() {
        return Err(TargetError::Configuration("Pulsar broker is missing host".to_string()));
    }

    Ok(url)
}

pub async fn connect_pulsar(args: &PulsarArgs) -> Result<Pulsar<TokioExecutor>, TargetError> {
    args.validate()?;

    let mut builder = Pulsar::builder(args.broker.clone(), TokioExecutor);

    if !args.auth_token.is_empty() {
        builder = builder.with_auth(Authentication {
            name: "token".to_string(),
            data: args.auth_token.clone().into_bytes(),
        });
    } else if !args.username.is_empty() {
        builder =
            builder.with_auth_provider(pulsar::authentication::basic::BasicAuthentication::new(&args.username, &args.password));
    }

    if !args.tls_ca.is_empty() {
        builder = builder
            .with_certificate_chain_file(&args.tls_ca)
            .map_err(|e| TargetError::Configuration(format!("Failed to load Pulsar tls_ca: {e}")))?;
    }

    builder = builder
        .with_allow_insecure_connection(args.tls_allow_insecure)
        .with_tls_hostname_verification_enabled(args.tls_hostname_verification);

    builder
        .build()
        .await
        .map_err(|e| TargetError::Network(format!("Failed to connect to Pulsar broker: {e}")))
}

pub struct PulsarTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    id: TargetID,
    args: PulsarArgs,
    client: Mutex<Option<Pulsar<TokioExecutor>>>,
    producer: AsyncMutex<Option<Producer<TokioExecutor>>>,
    store: Option<Box<dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync>>,
    connected: AtomicBool,
    delivery_counters: Arc<TargetDeliveryCounters>,
    _phantom: std::marker::PhantomData<E>,
}

impl<E> PulsarTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    pub fn clone_box(&self) -> Box<dyn Target<E> + Send + Sync> {
        Box::new(PulsarTarget::<E> {
            id: self.id.clone(),
            args: self.args.clone(),
            client: Mutex::new(self.client.lock().unwrap().clone()),
            producer: AsyncMutex::new(None),
            store: self.store.as_ref().map(|s| s.boxed_clone()),
            connected: AtomicBool::new(self.connected.load(Ordering::SeqCst)),
            delivery_counters: Arc::clone(&self.delivery_counters),
            _phantom: std::marker::PhantomData,
        })
    }

    #[instrument(skip(args), fields(target_id_as_string = %id))]
    pub fn new(id: String, args: PulsarArgs) -> Result<Self, TargetError> {
        args.validate()?;
        let target_id = TargetID::new(id, ChannelTargetType::Pulsar.as_str().to_string());
        let queue_store = if !args.queue_dir.is_empty() {
            let base_path = PathBuf::from(&args.queue_dir);
            let specific_queue_path = base_path.join(queue_store_subdir_name(ChannelTargetType::Pulsar.as_str(), &target_id.id));
            let extension = match args.target_type {
                TargetType::AuditLog => rustfs_config::audit::AUDIT_STORE_EXTENSION,
                TargetType::NotifyEvent => rustfs_config::notify::NOTIFY_STORE_EXTENSION,
            };
            let store = QueueStore::<QueuedPayload>::new(specific_queue_path, args.queue_limit, extension);
            if let Err(e) = store.open() {
                error!(target_id = %target_id, error = %e, "Failed to open store for Pulsar target");
                return Err(TargetError::Storage(format!("{e}")));
            }
            Some(Box::new(store) as Box<dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync>)
        } else {
            None
        };

        Ok(Self {
            id: target_id,
            args,
            client: Mutex::new(None),
            producer: AsyncMutex::new(None),
            store: queue_store,
            connected: AtomicBool::new(false),
            delivery_counters: Arc::new(TargetDeliveryCounters::default()),
            _phantom: std::marker::PhantomData,
        })
    }

    async fn get_or_connect_client(&self) -> Result<Pulsar<TokioExecutor>, TargetError> {
        if let Some(client) = self.client.lock().unwrap().clone() {
            return Ok(client);
        }

        let client = connect_pulsar(&self.args).await?;
        self.connected.store(true, Ordering::SeqCst);
        let mut guard = self.client.lock().unwrap();
        let shared = guard.get_or_insert_with(|| client.clone()).clone();
        Ok(shared)
    }

    async fn init_producer(&self) -> Result<(), TargetError> {
        if self.producer.lock().await.is_some() {
            return Ok(());
        }

        let client = self.get_or_connect_client().await?;
        let producer = client
            .producer()
            .with_topic(self.args.topic.clone())
            .with_name(self.id.id.clone())
            .build()
            .await
            .map_err(|e| TargetError::Network(format!("Failed to create Pulsar producer: {e}")))?;

        let mut guard = self.producer.lock().await;
        if guard.is_none() {
            *guard = Some(producer);
        }
        Ok(())
    }

    fn build_queued_payload(&self, event: &EntityTarget<E>) -> Result<QueuedPayload, TargetError> {
        let object_name = crate::target::decode_object_name(&event.object_name)?;
        let key = format!("{}/{}", event.bucket_name, object_name);
        let log = TargetLog {
            event_name: event.event_name,
            key,
            records: vec![event.clone()],
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

    async fn send_body(&self, body: Vec<u8>) -> Result<(), TargetError> {
        self.init_producer().await?;
        let mut guard = self.producer.lock().await;
        let producer = guard
            .as_mut()
            .ok_or_else(|| TargetError::Configuration("Pulsar producer not initialized".to_string()))?;
        let receipt = producer
            .send_non_blocking(body)
            .await
            .map_err(|e| TargetError::Request(format!("Failed to send Pulsar message: {e}")))?;
        receipt
            .await
            .map_err(|e| TargetError::Request(format!("Failed to receive Pulsar receipt: {e}")))?;
        self.delivery_counters.record_success();
        Ok(())
    }
}

#[async_trait]
impl<E> Target<E> for PulsarTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    fn id(&self) -> TargetID {
        self.id.clone()
    }

    async fn is_active(&self) -> Result<bool, TargetError> {
        self.init_producer().await?;
        let guard = self.producer.lock().await;
        let producer = guard
            .as_ref()
            .ok_or_else(|| TargetError::Configuration("Pulsar producer not initialized".to_string()))?;
        producer
            .check_connection()
            .await
            .map_err(|e| TargetError::Network(format!("Pulsar health check failed: {e}")))?;
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
            Ok(())
        } else {
            if let Err(err) = self.send_body(queued.body).await {
                self.delivery_counters.record_final_failure();
                return Err(err);
            }
            Ok(())
        }
    }

    async fn send_raw_from_store(&self, _key: Key, body: Vec<u8>, _meta: QueuedPayloadMeta) -> Result<(), TargetError> {
        self.send_body(body).await
    }

    async fn close(&self) -> Result<(), TargetError> {
        let mut producer = self.producer.lock().await;
        if let Some(producer) = producer.as_mut() {
            producer
                .close()
                .await
                .map_err(|e| TargetError::Network(format!("Failed to close Pulsar producer: {e}")))?;
        }
        *producer = None;
        self.client.lock().unwrap().take();
        self.connected.store(false, Ordering::SeqCst);
        info!(target_id = %self.id, "Pulsar target closed");
        Ok(())
    }

    fn store(&self) -> Option<&(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync)> {
        self.store.as_deref()
    }

    fn clone_dyn(&self) -> Box<dyn Target<E> + Send + Sync> {
        self.clone_box()
    }

    async fn init(&self) -> Result<(), TargetError> {
        if !self.is_enabled() {
            return Ok(());
        }
        self.init_producer().await
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
