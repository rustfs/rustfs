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

//! AMQP 0-9-1 event notification target.
//!
//! Publishes S3 events to RabbitMQ-compatible AMQP 0-9-1 brokers via `lapin`.
//! Queue-store mode uses the shared target store and replays the same raw JSON
//! body through `send_raw_from_store`.

use crate::{
    StoreError, Target, TargetLog,
    arn::TargetID,
    error::TargetError,
    store::{Key, QueueStore, Store},
    target::{
        ChannelTargetType, EntityTarget, QueuedPayload, QueuedPayloadMeta, TargetDeliveryCounters, TargetDeliverySnapshot,
        TargetType,
    },
};
use async_trait::async_trait;
use lapin::{
    BasicProperties, Channel, Confirmation, Connection, ConnectionProperties,
    options::{BasicPublishOptions, ConfirmSelectOptions},
    tcp::{OwnedIdentity, OwnedTLSConfig},
};
use rustfs_config::{AMQP_TLS_CA, AMQP_TLS_CLIENT_CERT, AMQP_TLS_CLIENT_KEY};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::sync::Mutex as AsyncMutex;
use tracing::{error, info, instrument, warn};
use url::Url;

#[derive(Clone)]
pub struct AMQPArgs {
    pub enable: bool,
    pub url: Url,
    pub exchange: String,
    pub routing_key: String,
    pub mandatory: bool,
    pub persistent: bool,
    pub username: String,
    pub password: String,
    pub tls_ca: String,
    pub tls_client_cert: String,
    pub tls_client_key: String,
    pub queue_dir: String,
    pub queue_limit: u64,
    pub target_type: TargetType,
}

impl fmt::Debug for AMQPArgs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AMQPArgs")
            .field("enable", &self.enable)
            .field("url", &redacted_amqp_url(&self.url))
            .field("exchange", &self.exchange)
            .field("routing_key", &self.routing_key)
            .field("mandatory", &self.mandatory)
            .field("persistent", &self.persistent)
            .field("username", &self.username)
            .field("password", if self.password.is_empty() { &"" } else { &"***REDACTED***" })
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
            .field("queue_dir", &self.queue_dir)
            .field("queue_limit", &self.queue_limit)
            .field("target_type", &self.target_type)
            .finish()
    }
}

impl AMQPArgs {
    pub fn validate(&self) -> Result<(), TargetError> {
        if !self.enable {
            return Ok(());
        }

        validate_amqp_url(&self.url)?;

        if self.exchange.trim().is_empty() {
            return Err(TargetError::Configuration("AMQP exchange cannot be empty".to_string()));
        }
        if self.routing_key.trim().is_empty() {
            return Err(TargetError::Configuration("AMQP routing_key cannot be empty".to_string()));
        }

        let url_has_credentials = !self.url.username().is_empty() || self.url.password().is_some();
        let config_has_credentials = !self.username.is_empty() || !self.password.is_empty();
        if self.username.is_empty() != self.password.is_empty() {
            return Err(TargetError::Configuration(
                "AMQP username and password must be specified together".to_string(),
            ));
        }
        if url_has_credentials && config_has_credentials {
            return Err(TargetError::Configuration(
                "AMQP credentials must be specified either in url or username/password, not both".to_string(),
            ));
        }

        validate_amqp_tls_paths(self)?;

        if !self.queue_dir.is_empty() && !Path::new(&self.queue_dir).is_absolute() {
            return Err(TargetError::Configuration("AMQP queue directory must be an absolute path".to_string()));
        }

        Ok(())
    }
}

fn redacted_amqp_url(url: &Url) -> String {
    if url.password().is_none() {
        return url.to_string();
    }
    let mut redacted = url.clone();
    let _ = redacted.set_password(Some("***REDACTED***"));
    redacted.to_string()
}

pub fn validate_amqp_url(url: &Url) -> Result<(), TargetError> {
    match url.scheme() {
        "amqp" | "amqps" => {
            if url.host_str().is_none() {
                return Err(TargetError::Configuration("AMQP URL is missing host".to_string()));
            }
            Ok(())
        }
        scheme => Err(TargetError::Configuration(format!(
            "Unsupported AMQP URL scheme: {scheme} (only amqp and amqps are allowed)"
        ))),
    }
}

fn validate_amqp_tls_paths(args: &AMQPArgs) -> Result<(), TargetError> {
    let has_tls_settings = !args.tls_ca.is_empty() || !args.tls_client_cert.is_empty() || !args.tls_client_key.is_empty();
    if has_tls_settings && args.url.scheme() != "amqps" {
        return Err(TargetError::Configuration(
            "AMQP TLS settings are only allowed with amqps URLs".to_string(),
        ));
    }

    if args.tls_client_cert.is_empty() != args.tls_client_key.is_empty() {
        return Err(TargetError::Configuration(
            "AMQP tls_client_cert and tls_client_key must be specified together".to_string(),
        ));
    }

    if !args.tls_ca.is_empty() && !Path::new(&args.tls_ca).is_absolute() {
        return Err(TargetError::Configuration(format!("{AMQP_TLS_CA} must be an absolute path")));
    }
    if !args.tls_client_cert.is_empty() && !Path::new(&args.tls_client_cert).is_absolute() {
        return Err(TargetError::Configuration(format!("{AMQP_TLS_CLIENT_CERT} must be an absolute path")));
    }
    if !args.tls_client_key.is_empty() && !Path::new(&args.tls_client_key).is_absolute() {
        return Err(TargetError::Configuration(format!("{AMQP_TLS_CLIENT_KEY} must be an absolute path")));
    }

    Ok(())
}

fn connection_url(args: &AMQPArgs) -> Result<String, TargetError> {
    let mut url = args.url.clone();
    if !args.username.is_empty() {
        url.set_username(&args.username)
            .map_err(|_| TargetError::Configuration("AMQP username cannot be set on URL".to_string()))?;
        url.set_password(Some(&args.password))
            .map_err(|_| TargetError::Configuration("AMQP password cannot be set on URL".to_string()))?;
    }
    Ok(url.to_string())
}

async fn build_tls_config(args: &AMQPArgs) -> Result<OwnedTLSConfig, TargetError> {
    let cert_chain = if args.tls_ca.is_empty() {
        None
    } else {
        Some(
            tokio::fs::read_to_string(&args.tls_ca)
                .await
                .map_err(|e| TargetError::Configuration(format!("Failed to read {AMQP_TLS_CA}: {e}")))?,
        )
    };

    let identity = if args.tls_client_cert.is_empty() {
        None
    } else {
        let pem = tokio::fs::read(&args.tls_client_cert)
            .await
            .map_err(|e| TargetError::Configuration(format!("Failed to read {AMQP_TLS_CLIENT_CERT}: {e}")))?;
        let key = tokio::fs::read(&args.tls_client_key)
            .await
            .map_err(|e| TargetError::Configuration(format!("Failed to read {AMQP_TLS_CLIENT_KEY}: {e}")))?;
        Some(OwnedIdentity::PKCS8 { pem, key })
    };

    Ok(OwnedTLSConfig { identity, cert_chain })
}

fn build_publish_properties(args: &AMQPArgs) -> BasicProperties {
    let mut properties = BasicProperties::default().with_content_type("application/json".into());
    if args.persistent {
        properties = properties.with_delivery_mode(2);
    }
    properties
}

fn map_lapin_error(err: lapin::Error, context: &str) -> TargetError {
    TargetError::Network(format!("{context}: {err}"))
}

pub async fn connect_amqp(args: &AMQPArgs) -> Result<AMQPConnection, TargetError> {
    args.validate()?;
    match tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let url = connection_url(args)?;
        // Reconnect explicitly so every new channel enables publisher confirms below.
        let properties = ConnectionProperties::default();
        let connection = if args.url.scheme() == "amqps" && (!args.tls_ca.is_empty() || !args.tls_client_cert.is_empty()) {
            Connection::connect_with_config(
                &url,
                properties,
                build_tls_config(args).await?,
                lapin::runtime::default_runtime()
                    .map_err(|e| TargetError::Initialization(format!("Failed to create AMQP runtime: {e}")))?,
            )
            .await
        } else {
            Connection::connect(&url, properties).await
        }
        .map_err(|e| map_lapin_error(e, "Failed to connect to AMQP broker"))?;

        let channel = connection
            .create_channel()
            .await
            .map_err(|e| map_lapin_error(e, "Failed to create AMQP channel"))?;
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .map_err(|e| map_lapin_error(e, "Failed to enable AMQP publisher confirms"))?;

        Ok(AMQPConnection { connection, channel })
    })
    .await
    {
        Ok(result) => result,
        Err(_) => Err(TargetError::Timeout("AMQP connection timed out".to_string())),
    }
}

pub struct AMQPConnection {
    pub(crate) connection: Connection,
    pub(crate) channel: Channel,
}

pub struct AMQPTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    id: TargetID,
    args: AMQPArgs,
    connection: Mutex<Option<Arc<AMQPConnection>>>,
    connect_lock: AsyncMutex<()>,
    store: Option<Box<dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync>>,
    delivery_counters: Arc<TargetDeliveryCounters>,
    _phantom: std::marker::PhantomData<E>,
}

impl<E> AMQPTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    pub fn clone_box(&self) -> Box<dyn Target<E> + Send + Sync> {
        Box::new(AMQPTarget::<E> {
            id: self.id.clone(),
            args: self.args.clone(),
            connection: Mutex::new(self.connection.lock().unwrap().clone()),
            connect_lock: AsyncMutex::new(()),
            store: self.store.as_ref().map(|s| s.boxed_clone()),
            delivery_counters: Arc::clone(&self.delivery_counters),
            _phantom: std::marker::PhantomData,
        })
    }

    #[instrument(skip(args), fields(target_id_as_string = %id))]
    pub fn new(id: String, args: AMQPArgs) -> Result<Self, TargetError> {
        args.validate()?;
        let target_id = TargetID::new(id, ChannelTargetType::Amqp.as_str().to_string());
        let queue_store = if !args.queue_dir.is_empty() {
            let base_path = PathBuf::from(&args.queue_dir);
            let specific_queue_path = base_path.join(format!("rustfs-{}-{}", ChannelTargetType::Amqp.as_str(), target_id.id));
            let extension = match args.target_type {
                TargetType::AuditLog => rustfs_config::audit::AUDIT_STORE_EXTENSION,
                TargetType::NotifyEvent => rustfs_config::notify::NOTIFY_STORE_EXTENSION,
            };
            let store = QueueStore::<QueuedPayload>::new(specific_queue_path, args.queue_limit, extension);
            if let Err(e) = store.open() {
                error!(target_id = %target_id, error = %e, "Failed to open store for AMQP target");
                return Err(TargetError::Storage(format!("{e}")));
            }
            Some(Box::new(store) as Box<dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync>)
        } else {
            None
        };

        Ok(Self {
            id: target_id,
            args,
            connection: Mutex::new(None),
            connect_lock: AsyncMutex::new(()),
            store: queue_store,
            delivery_counters: Arc::new(TargetDeliveryCounters::default()),
            _phantom: std::marker::PhantomData,
        })
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

    async fn get_or_connect(&self) -> Result<Arc<AMQPConnection>, TargetError> {
        if let Some(connection) = self.connection.lock().unwrap().clone()
            && connection.connection.status().connected()
            && connection.channel.status().connected()
        {
            return Ok(connection);
        }

        let _guard = self.connect_lock.lock().await;
        if let Some(connection) = self.connection.lock().unwrap().clone()
            && connection.connection.status().connected()
            && connection.channel.status().connected()
        {
            return Ok(connection);
        }

        let connection = Arc::new(connect_amqp(&self.args).await?);
        let mut guard = self.connection.lock().unwrap();
        *guard = Some(Arc::clone(&connection));
        Ok(connection)
    }

    fn clear_connection(&self) {
        *self.connection.lock().unwrap() = None;
    }

    async fn send_body(&self, body: &[u8]) -> Result<(), TargetError> {
        let connection = self.get_or_connect().await?;
        let publish = connection
            .channel
            .basic_publish(
                self.args.exchange.clone().into(),
                self.args.routing_key.clone().into(),
                BasicPublishOptions {
                    mandatory: self.args.mandatory,
                    ..BasicPublishOptions::default()
                },
                body,
                build_publish_properties(&self.args),
            )
            .await;

        let confirm = match publish {
            Ok(confirm) => confirm.await,
            Err(err) => {
                self.clear_connection();
                return Err(map_lapin_error(err, "Failed to publish AMQP message"));
            }
        };

        match confirm {
            Ok(Confirmation::Ack(None) | Confirmation::NotRequested) => {
                self.delivery_counters.record_success();
                Ok(())
            }
            Ok(Confirmation::Ack(Some(returned)) | Confirmation::Nack(Some(returned))) => {
                Err(TargetError::Request(format!("AMQP broker returned message: {}", returned.reply_text)))
            }
            Ok(Confirmation::Nack(None)) => Err(TargetError::Request("AMQP broker negatively acknowledged message".to_string())),
            Err(err) => {
                self.clear_connection();
                Err(map_lapin_error(err, "Failed to confirm AMQP publish"))
            }
        }
    }
}

#[async_trait]
impl<E> Target<E> for AMQPTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    fn id(&self) -> TargetID {
        self.id.clone()
    }

    async fn is_active(&self) -> Result<bool, TargetError> {
        let connection = self.get_or_connect().await?;
        Ok(connection.connection.status().connected() && connection.channel.status().connected())
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
            if let Err(err) = self.send_body(&queued.body).await {
                self.delivery_counters.record_final_failure();
                return Err(err);
            }
            Ok(())
        }
    }

    async fn send_raw_from_store(&self, _key: Key, body: Vec<u8>, _meta: QueuedPayloadMeta) -> Result<(), TargetError> {
        self.send_body(&body).await
    }

    async fn close(&self) -> Result<(), TargetError> {
        let connection = self.connection.lock().unwrap().take();
        if let Some(connection) = connection {
            connection
                .connection
                .close(200, "OK".into())
                .await
                .map_err(|e| map_lapin_error(e, "Failed to close AMQP connection"))?;
        }
        info!(target_id = %self.id, "AMQP target closed");
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
        match self.get_or_connect().await {
            Ok(_) => Ok(()),
            Err(err) if self.store.is_some() => {
                warn!(target_id = %self.id, error = %err, "AMQP init failed; events will buffer in store");
                Ok(())
            }
            Err(err) => Err(err),
        }
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
    use rustfs_s3_common::EventName;
    use serde_json::json;
    use std::sync::Arc;
    use uuid::Uuid;

    fn valid_args() -> AMQPArgs {
        AMQPArgs {
            enable: true,
            url: Url::parse("amqp://127.0.0.1:5672/%2f").unwrap(),
            exchange: "rustfs.events".to_string(),
            routing_key: "objects".to_string(),
            mandatory: false,
            persistent: true,
            username: String::new(),
            password: String::new(),
            tls_ca: String::new(),
            tls_client_cert: String::new(),
            tls_client_key: String::new(),
            queue_dir: String::new(),
            queue_limit: 10,
            target_type: TargetType::NotifyEvent,
        }
    }

    fn unreachable_args() -> AMQPArgs {
        AMQPArgs {
            url: Url::parse("amqp://127.0.0.1:1/%2f").unwrap(),
            ..valid_args()
        }
    }

    fn test_event() -> Arc<EntityTarget<serde_json::Value>> {
        Arc::new(EntityTarget {
            object_name: "object.txt".to_string(),
            bucket_name: "bucket".to_string(),
            event_name: EventName::ObjectCreatedPut,
            data: json!({"ok": true}),
        })
    }

    fn temp_store_dir(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("rustfs-amqp-target-{name}-{}", Uuid::new_v4()))
    }

    fn assert_connect_failure(err: &TargetError) {
        let rendered = err.to_string();
        assert!(
            rendered.contains("Failed to connect to AMQP broker") || rendered.contains("AMQP connection timed out"),
            "unexpected error: {rendered}"
        );
    }

    #[test]
    fn new_rejects_invalid_args() {
        let mut args = valid_args();
        args.exchange.clear();

        let err = match AMQPTarget::<serde_json::Value>::new("primary".to_string(), args) {
            Ok(_) => panic!("invalid args should fail"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("exchange cannot be empty"));
    }

    #[test]
    fn new_accepts_queue_mode() {
        let mut args = valid_args();
        args.queue_dir = temp_store_dir("queue-mode").to_string_lossy().to_string();

        let target =
            AMQPTarget::<serde_json::Value>::new("primary".to_string(), args.clone()).expect("queue mode should be supported");

        assert!(target.store().is_some());
        let _ = std::fs::remove_dir_all(args.queue_dir);
    }

    #[tokio::test]
    async fn save_with_store_queues_event_without_broker() {
        let mut args = unreachable_args();
        args.queue_dir = temp_store_dir("save-store").to_string_lossy().to_string();
        let target = AMQPTarget::<serde_json::Value>::new("primary".to_string(), args.clone()).expect("target should build");

        target
            .save(test_event())
            .await
            .expect("store-backed save should queue without broker");

        assert_eq!(target.delivery_snapshot().queue_length, 1);
        assert_eq!(target.delivery_snapshot().failed_messages, 0);
        let _ = std::fs::remove_dir_all(args.queue_dir);
    }

    #[tokio::test]
    async fn save_without_store_returns_connection_error() {
        let target =
            AMQPTarget::<serde_json::Value>::new("primary".to_string(), unreachable_args()).expect("target should build");

        let err = target
            .save(test_event())
            .await
            .expect_err("direct publish should fail without broker");

        assert_connect_failure(&err);
        assert_eq!(target.delivery_snapshot().failed_messages, 1);
    }

    #[tokio::test]
    async fn init_with_store_allows_broker_to_recover_later() {
        let mut args = unreachable_args();
        args.queue_dir = temp_store_dir("init-store").to_string_lossy().to_string();
        let target = AMQPTarget::<serde_json::Value>::new("primary".to_string(), args.clone()).expect("target should build");

        target.init().await.expect("store-backed init should tolerate broker failure");
        let _ = std::fs::remove_dir_all(args.queue_dir);
    }

    #[tokio::test]
    async fn init_without_store_returns_connection_error() {
        let target =
            AMQPTarget::<serde_json::Value>::new("primary".to_string(), unreachable_args()).expect("target should build");

        let err = target
            .init()
            .await
            .expect_err("init should fail without broker when no store exists");

        assert_connect_failure(&err);
    }

    #[tokio::test]
    async fn send_raw_from_store_returns_connection_error() {
        let target =
            AMQPTarget::<serde_json::Value>::new("primary".to_string(), unreachable_args()).expect("target should build");
        let key = Key {
            name: "queued".to_string(),
            extension: ".event".to_string(),
            item_count: 1,
            compress: false,
        };
        let meta = QueuedPayloadMeta::new(
            EventName::ObjectCreatedPut,
            "bucket".to_string(),
            "object.txt".to_string(),
            "application/json",
            2,
        );

        let err = target
            .send_raw_from_store(key, b"{}".to_vec(), meta)
            .await
            .expect_err("queue replay should fail without broker");

        assert_connect_failure(&err);
    }

    #[test]
    fn debug_masks_secret_values() {
        let args = AMQPArgs {
            url: Url::parse("amqp://guest:secret@127.0.0.1:5672/%2f").unwrap(),
            password: "secret".to_string(),
            tls_client_key: "/tmp/client.key".to_string(),
            ..valid_args()
        };
        let rendered = format!("{args:?}");

        assert!(!rendered.contains("guest:secret"));
        assert!(!rendered.contains("password: \"secret\""));
        assert!(!rendered.contains("tls_client_key: \"/tmp/client.key\""));
        assert!(rendered.contains("***REDACTED***"));
    }
}
