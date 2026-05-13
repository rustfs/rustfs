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

use crate::arn::TargetID;
use crate::store::{Key, QueueStore, Store};
use crate::{StoreError, TargetError, TargetLog};
use async_trait::async_trait;
use rustfs_s3_common::EventName;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Formatter;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, warn};

pub mod amqp;
pub mod kafka;
pub mod mqtt;
pub mod mysql;
pub mod nats;
pub mod postgres;
pub mod pulsar;
pub mod redis;
pub mod webhook;

/// A read-only snapshot of delivery counters for a target.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TargetDeliverySnapshot {
    pub failed_messages: u64,
    pub queue_length: u64,
    pub total_messages: u64,
}

/// Shared target delivery counters.
#[derive(Debug, Default)]
pub struct TargetDeliveryCounters {
    failed_messages: AtomicU64,
    total_messages: AtomicU64,
}

pub(crate) type BoxedQueuedStore = Box<dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync>;

impl TargetDeliveryCounters {
    #[inline]
    pub fn record_success(&self) {
        self.total_messages.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_final_failure(&self) {
        self.failed_messages.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn snapshot(&self, queue_length: u64) -> TargetDeliverySnapshot {
        TargetDeliverySnapshot {
            failed_messages: self.failed_messages.load(Ordering::Relaxed),
            queue_length,
            total_messages: self.total_messages.load(Ordering::Relaxed),
        }
    }
}

/// Trait for notification targets
#[async_trait]
pub trait Target<E>: Send + Sync + 'static
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    /// Returns the ID of the target
    fn id(&self) -> TargetID;

    /// Returns the name of the target
    fn name(&self) -> String {
        self.id().to_string()
    }

    /// Checks if the target is active and reachable
    async fn is_active(&self) -> Result<bool, TargetError>;

    /// Saves an event (either sends it immediately or stores it for later)
    async fn save(&self, event: Arc<EntityTarget<E>>) -> Result<(), TargetError>;

    /// Sends an event from the store using the queued raw body and metadata.
    async fn send_raw_from_store(&self, key: Key, body: Vec<u8>, meta: QueuedPayloadMeta) -> Result<(), TargetError>;

    /// Sends an event from the store.
    async fn send_from_store(&self, key: Key) -> Result<(), TargetError> {
        let store = self
            .store()
            .ok_or_else(|| TargetError::Configuration("No store configured".to_string()))?;

        let raw = match store.get_raw(&key) {
            Ok(raw) => raw,
            Err(StoreError::NotFound) => return Ok(()),
            Err(err) => return Err(TargetError::Storage(format!("Failed to read queued payload from store: {err}"))),
        };

        let queued = match QueuedPayload::decode(&raw) {
            Ok(queued) => queued,
            Err(err) => {
                delete_stored_payload(store, &key).map_err(|delete_err| {
                    TargetError::Storage(format!(
                        "Failed to delete invalid queued payload {key} after decode error '{err}': {delete_err}"
                    ))
                })?;
                self.record_final_failure();
                warn!("Dropped invalid queued payload {key}: {err}");
                return Err(TargetError::Dropped(format!("Dropped invalid queued payload {key}: {err}")));
            }
        };

        self.send_raw_from_store(key.clone(), queued.body, queued.meta).await?;
        delete_stored_payload(store, &key)
    }

    /// Closes the target and releases resources
    async fn close(&self) -> Result<(), TargetError>;

    /// Returns the store associated with the target (if any)
    fn store(&self) -> Option<&(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync)>;

    /// Returns the type of the target
    fn clone_dyn(&self) -> Box<dyn Target<E> + Send + Sync>;

    /// Initialize the target, such as establishing a connection, etc.
    async fn init(&self) -> Result<(), TargetError> {
        // The default implementation is empty
        Ok(())
    }

    /// Check if the target is enabled
    fn is_enabled(&self) -> bool;

    /// Returns a read-only delivery snapshot for metrics collection.
    fn delivery_snapshot(&self) -> TargetDeliverySnapshot {
        TargetDeliverySnapshot {
            queue_length: self.store().map_or(0, |store| store.len() as u64),
            ..TargetDeliverySnapshot::default()
        }
    }

    /// Records a final, non-retryable delivery failure for metrics collection.
    fn record_final_failure(&self) {}
}

#[derive(Debug, Serialize, Clone, Deserialize)]
pub struct EntityTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize,
{
    pub object_name: String,
    pub bucket_name: String,
    pub event_name: EventName,
    pub data: E,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedPayloadMeta {
    pub event_name: EventName,
    pub bucket_name: String,
    pub object_name: String,
    pub content_type: String,
    pub queued_at_unix_ms: u64,
    pub payload_len: usize,
}

impl QueuedPayloadMeta {
    pub fn new(
        event_name: EventName,
        bucket_name: String,
        object_name: String,
        content_type: impl Into<String>,
        payload_len: usize,
    ) -> Self {
        Self {
            event_name,
            bucket_name,
            object_name,
            content_type: content_type.into(),
            queued_at_unix_ms: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64,
            payload_len,
        }
    }

    pub fn best_effort_preview(&self, body: &[u8], limit: usize) -> String {
        if limit == 0 || body.is_empty() {
            return String::new();
        }

        let slice = &body[..body.len().min(limit)];
        match std::str::from_utf8(slice) {
            Ok(text) => {
                if body.len() > limit {
                    format!("{text}...")
                } else {
                    text.to_string()
                }
            }
            Err(_) => format!("<{} bytes binary>", body.len()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedPayload {
    pub meta: QueuedPayloadMeta,
    pub body: Vec<u8>,
}

impl QueuedPayload {
    const MAGIC: [u8; 4] = *b"RQP1";

    pub fn new(meta: QueuedPayloadMeta, body: Vec<u8>) -> Self {
        Self { meta, body }
    }

    pub fn encode(&self) -> Result<Vec<u8>, TargetError> {
        let meta = serde_json::to_vec(&self.meta)
            .map_err(|err| TargetError::Serialization(format!("Failed to serialize queued payload metadata: {err}")))?;
        let meta_len = u32::try_from(meta.len())
            .map_err(|_| TargetError::Serialization("Queued payload metadata is too large".to_string()))?;

        let mut out = Vec::with_capacity(Self::MAGIC.len() + 4 + meta.len() + self.body.len());
        out.extend_from_slice(&Self::MAGIC);
        out.extend_from_slice(&meta_len.to_le_bytes());
        out.extend_from_slice(&meta);
        out.extend_from_slice(&self.body);
        Ok(out)
    }

    pub fn decode(raw: &[u8]) -> Result<Self, TargetError> {
        if raw.len() < Self::MAGIC.len() + 4 {
            return Err(TargetError::Serialization("Queued payload is too short".to_string()));
        }
        if raw[..Self::MAGIC.len()] != Self::MAGIC {
            return Err(TargetError::Serialization("Queued payload magic mismatch".to_string()));
        }

        let mut meta_len_bytes = [0u8; 4];
        meta_len_bytes.copy_from_slice(&raw[Self::MAGIC.len()..Self::MAGIC.len() + 4]);
        let meta_len = u32::from_le_bytes(meta_len_bytes) as usize;
        let meta_start = Self::MAGIC.len() + 4;
        let meta_end = meta_start + meta_len;

        if meta_end > raw.len() {
            return Err(TargetError::Serialization("Queued payload metadata length exceeds input".to_string()));
        }

        let meta = serde_json::from_slice(&raw[meta_start..meta_end])
            .map_err(|err| TargetError::Serialization(format!("Failed to deserialize queued payload metadata: {err}")))?;
        let body = raw[meta_end..].to_vec();

        Ok(Self { meta, body })
    }
}

/// The `ChannelTargetType` enum represents the different types of channel Target
/// used in the notification system.
///
/// It includes:
/// - `Amqp`: Represents an AMQP 0-9-1 target for sending notifications to a broker.
/// - `Webhook`: Sends notifications via HTTP POST requests.
/// - `Kafka`: Publishes notifications to a Kafka topic.
/// - `Mqtt`: Publishes notifications via MQTT protocol.
/// - `MySql`: Writes notifications to a MySQL/TiDB table.
/// - `Nats`: Publishes notifications to a NATS subject.
/// - `Postgres`: Writes notifications to a PostgreSQL table (namespace or access format).
/// - `Pulsar`: Publishes notifications to a Pulsar topic.
/// - `Redis`: Publishes notifications to a Redis channel (pub/sub).
///
/// Each variant has an associated string representation that can be used for serialization
/// or logging purposes.
/// The `as_str` method returns the string representation of the target type,
/// and the `Display` implementation allows for easy formatting of the target type as a string.
///
/// Example usage:
/// ```rust
/// use rustfs_targets::target::ChannelTargetType;
///
/// let target_type = ChannelTargetType::Webhook;
/// assert_eq!(target_type.as_str(), "webhook");
/// println!("Target type: {}", target_type);
/// ```
pub enum ChannelTargetType {
    Amqp,
    Webhook,
    Kafka,
    Mqtt,
    MySql,
    Nats,
    Postgres,
    Pulsar,
    Redis,
}

impl ChannelTargetType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ChannelTargetType::Amqp => "amqp",
            ChannelTargetType::Webhook => "webhook",
            ChannelTargetType::Kafka => "kafka",
            ChannelTargetType::Mqtt => "mqtt",
            ChannelTargetType::MySql => "mysql",
            ChannelTargetType::Nats => "nats",
            ChannelTargetType::Postgres => "postgres",
            ChannelTargetType::Pulsar => "pulsar",
            ChannelTargetType::Redis => "redis",
        }
    }
}

impl std::fmt::Display for ChannelTargetType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ChannelTargetType::Amqp => write!(f, "amqp"),
            ChannelTargetType::Webhook => write!(f, "webhook"),
            ChannelTargetType::Kafka => write!(f, "kafka"),
            ChannelTargetType::Mqtt => write!(f, "mqtt"),
            ChannelTargetType::MySql => write!(f, "mysql"),
            ChannelTargetType::Nats => write!(f, "nats"),
            ChannelTargetType::Postgres => write!(f, "postgres"),
            ChannelTargetType::Pulsar => write!(f, "pulsar"),
            ChannelTargetType::Redis => write!(f, "redis"),
        }
    }
}

/// `TargetType` enum represents the type of target in the notification system.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetType {
    AuditLog,
    NotifyEvent,
}

impl TargetType {
    pub fn as_str(&self) -> &'static str {
        match self {
            TargetType::AuditLog => "audit_log",
            TargetType::NotifyEvent => "notify_event",
        }
    }
}

impl std::fmt::Display for TargetType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TargetType::AuditLog => write!(f, "audit_log"),
            TargetType::NotifyEvent => write!(f, "notify_event"),
        }
    }
}

pub(crate) fn sanitize_queue_dir_component(component: &str) -> String {
    let mut sanitized = String::with_capacity(component.len());
    for ch in component.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            sanitized.push(ch);
        } else {
            sanitized.push('_');
        }
    }

    if sanitized.is_empty() { "_".to_string() } else { sanitized }
}

pub(crate) fn queue_store_subdir_name(target_type: &str, target_id: &str) -> String {
    format!("rustfs-{target_type}-{}", sanitize_queue_dir_component(target_id))
}

/// Decodes a form-urlencoded object name to its original form.
///
/// This function properly handles form-urlencoded strings where spaces are
/// represented as `+` symbols. It first replaces `+` with spaces, then
/// performs standard percent-decoding.
///
/// # Arguments
/// * `encoded` - The form-urlencoded string to decode
///
/// # Returns
/// The decoded string, or an error if decoding fails
///
/// # Example
/// ```
/// use rustfs_targets::target::decode_object_name;
///
/// let encoded = "greeting+file+%282%29.csv";
/// let decoded = decode_object_name(encoded).unwrap();
/// assert_eq!(decoded, "greeting file (2).csv");
/// ```
pub fn decode_object_name(encoded: &str) -> Result<String, TargetError> {
    let replaced = encoded.replace("+", " ");
    urlencoding::decode(&replaced)
        .map(|s| s.into_owned())
        .map_err(|e| TargetError::Encoding(format!("Failed to decode object key: {e}")))
}

pub(crate) fn build_queued_payload<E>(event: &EntityTarget<E>) -> Result<QueuedPayload, TargetError>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    build_queued_payload_with_records(event, vec![event.data.clone()])
}

pub(crate) fn build_queued_payload_with_records<E, R>(
    event: &EntityTarget<E>,
    records: Vec<R>,
) -> Result<QueuedPayload, TargetError>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
    R: Serialize,
{
    let object_name = decode_object_name(&event.object_name)?;
    let key = format!("{}/{}", event.bucket_name, object_name);

    let log = TargetLog {
        event_name: event.event_name,
        key,
        records,
    };

    let body = serde_json::to_vec(&log).map_err(|err| TargetError::Serialization(format!("Failed to serialize event: {err}")))?;
    let meta = QueuedPayloadMeta::new(
        event.event_name,
        event.bucket_name.clone(),
        event.object_name.clone(),
        "application/json",
        body.len(),
    );

    Ok(QueuedPayload::new(meta, body))
}

pub(crate) fn open_target_queue_store(
    queue_dir: &str,
    queue_limit: u64,
    target_type: TargetType,
    target_type_label: &str,
    target_id: &TargetID,
    open_context: &str,
) -> Result<Option<BoxedQueuedStore>, TargetError> {
    fn boxed_queue_store(store: QueueStore<QueuedPayload>) -> BoxedQueuedStore {
        Box::new(store)
    }

    if queue_dir.is_empty() {
        return Ok(None);
    }

    let queue_dir = PathBuf::from(queue_dir).join(queue_store_subdir_name(target_type_label, &target_id.id));
    let extension = match target_type {
        TargetType::AuditLog => rustfs_config::audit::AUDIT_STORE_EXTENSION,
        TargetType::NotifyEvent => rustfs_config::notify::NOTIFY_STORE_EXTENSION,
    };
    let store = QueueStore::<QueuedPayload>::new(queue_dir, queue_limit, extension);
    store
        .open()
        .map_err(|err| TargetError::Storage(format!("{open_context}: {err}")))?;

    Ok(Some(boxed_queue_store(store)))
}

pub(crate) fn persist_queued_payload_to_store(
    store: &(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync),
    queued: &QueuedPayload,
) -> Result<(), TargetError> {
    let encoded = queued
        .encode()
        .map_err(|err| TargetError::Storage(format!("Failed to encode queued payload: {err}")))?;
    store
        .put_raw(&encoded)
        .map(|_| ())
        .map_err(|err| TargetError::Storage(format!("Failed to save event to store: {err}")))
}

pub(crate) fn is_connectivity_error(err: &TargetError) -> bool {
    matches!(err, TargetError::NotConnected | TargetError::Timeout(_) | TargetError::Network(_))
}

pub(crate) fn delete_stored_payload(
    store: &(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync),
    key: &Key,
) -> Result<(), TargetError> {
    match store.del(key) {
        Ok(()) | Err(StoreError::NotFound) => Ok(()),
        Err(err) => Err(TargetError::Storage(format!("Failed to delete event from store: {err}"))),
    }
}

/// Ensures a rustls crypto provider is installed before any TLS operation.
///
/// Multiple target modules (MySQL, Redis, Postgres, MQTT) need this because
/// each may be the first to perform a TLS handshake. Idempotent: if a
/// provider is already registered, returns immediately.
pub(crate) fn ensure_rustls_provider_installed() {
    if rustls::crypto::CryptoProvider::get_default().is_some() {
        return;
    }
    if let Err(err) = rustls::crypto::aws_lc_rs::default_provider().install_default() {
        debug!("rustls provider already installed or unavailable: {err:?}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::Mutex;
    use uuid::Uuid;

    #[derive(Clone)]
    struct MockQueuedStore {
        fail_put_raw: bool,
        writes: Arc<Mutex<Vec<Vec<u8>>>>,
    }

    impl MockQueuedStore {
        fn new(fail_put_raw: bool) -> Self {
            Self {
                fail_put_raw,
                writes: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl Store<QueuedPayload> for MockQueuedStore {
        type Error = StoreError;
        type Key = Key;

        fn open(&self) -> Result<(), Self::Error> {
            Ok(())
        }

        fn put(&self, _item: Arc<QueuedPayload>) -> Result<Self::Key, Self::Error> {
            Err(StoreError::Internal("not implemented in mock".to_string()))
        }

        fn put_multiple(&self, _items: Vec<QueuedPayload>) -> Result<Self::Key, Self::Error> {
            Err(StoreError::Internal("not implemented in mock".to_string()))
        }

        fn put_raw(&self, data: &[u8]) -> Result<Self::Key, Self::Error> {
            if self.fail_put_raw {
                return Err(StoreError::Internal("mock put_raw failed".to_string()));
            }
            self.writes.lock().expect("mock writes lock poisoned").push(data.to_vec());
            Ok(Key {
                name: "mock".to_string(),
                extension: ".json".to_string(),
                item_count: 1,
                compress: false,
            })
        }

        fn get(&self, _key: &Self::Key) -> Result<QueuedPayload, Self::Error> {
            Err(StoreError::Internal("not implemented in mock".to_string()))
        }

        fn get_multiple(&self, _key: &Self::Key) -> Result<Vec<QueuedPayload>, Self::Error> {
            Err(StoreError::Internal("not implemented in mock".to_string()))
        }

        fn get_raw(&self, _key: &Self::Key) -> Result<Vec<u8>, Self::Error> {
            Err(StoreError::Internal("not implemented in mock".to_string()))
        }

        fn del(&self, _key: &Self::Key) -> Result<(), Self::Error> {
            Err(StoreError::Internal("not implemented in mock".to_string()))
        }

        fn delete(&self) -> Result<(), Self::Error> {
            Err(StoreError::Internal("not implemented in mock".to_string()))
        }

        fn list(&self) -> Vec<Self::Key> {
            Vec::new()
        }

        fn len(&self) -> usize {
            0
        }

        fn is_empty(&self) -> bool {
            true
        }

        fn boxed_clone(&self) -> Box<dyn Store<QueuedPayload, Error = Self::Error, Key = Self::Key> + Send + Sync> {
            Box::new(self.clone())
        }
    }

    #[test]
    fn channel_target_type_amqp_uses_runtime_name() {
        assert_eq!(ChannelTargetType::Amqp.as_str(), "amqp");
        assert_eq!(ChannelTargetType::Amqp.to_string(), "amqp");
    }

    #[test]
    fn queued_payload_round_trips_meta_and_body() {
        let meta = QueuedPayloadMeta::new(
            EventName::ObjectCreatedPut,
            "bucket-a".to_string(),
            "folder/object.txt".to_string(),
            "application/json",
            12,
        );
        let payload = QueuedPayload::new(meta.clone(), br#"{"ok":true}"#.to_vec());

        let encoded = payload.encode().unwrap();
        let decoded = QueuedPayload::decode(&encoded).unwrap();

        assert_eq!(decoded.meta.event_name, meta.event_name);
        assert_eq!(decoded.meta.bucket_name, meta.bucket_name);
        assert_eq!(decoded.meta.object_name, meta.object_name);
        assert_eq!(decoded.meta.content_type, meta.content_type);
        assert_eq!(decoded.body, br#"{"ok":true}"#);
    }

    #[test]
    fn build_queued_payload_uses_event_data_shape() {
        let event = EntityTarget {
            object_name: "greeting+file+%282%29.csv".to_string(),
            bucket_name: "bucket-a".to_string(),
            event_name: EventName::ObjectCreatedPut,
            data: "payload-data".to_string(),
        };

        let payload = build_queued_payload(&event).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&payload.body).unwrap();

        assert_eq!(value["Key"], "bucket-a/greeting file (2).csv");
        assert_eq!(value["Records"][0], "payload-data");
    }

    #[test]
    fn build_queued_payload_with_records_preserves_custom_record_shape() {
        let event = EntityTarget {
            object_name: "object.txt".to_string(),
            bucket_name: "bucket-a".to_string(),
            event_name: EventName::ObjectCreatedPut,
            data: "ignored".to_string(),
        };

        let payload = build_queued_payload_with_records(&event, vec![event.clone()]).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&payload.body).unwrap();

        assert_eq!(value["Records"][0]["bucket_name"], "bucket-a");
        assert_eq!(value["Records"][0]["object_name"], "object.txt");
        assert_eq!(value["Records"][0]["data"], "ignored");
    }

    #[test]
    fn open_target_queue_store_returns_none_when_queue_dir_empty() {
        let target_id = TargetID::new("target-a".to_string(), ChannelTargetType::Webhook.as_str().to_string());
        let store = open_target_queue_store(
            "",
            100,
            TargetType::NotifyEvent,
            ChannelTargetType::Webhook.as_str(),
            &target_id,
            "open failed",
        )
        .unwrap();
        assert!(store.is_none());
    }

    #[test]
    fn open_target_queue_store_adds_context_on_open_error() {
        let base = std::env::temp_dir().join(format!("rustfs-target-store-file-{}", Uuid::new_v4()));
        fs::write(&base, b"not-a-directory").expect("failed to create file base");
        let target_id = TargetID::new("target-a".to_string(), ChannelTargetType::Kafka.as_str().to_string());

        let result = open_target_queue_store(
            base.to_str().unwrap(),
            100,
            TargetType::NotifyEvent,
            ChannelTargetType::Kafka.as_str(),
            &target_id,
            "custom open context",
        );

        match result {
            Ok(_) => panic!("expected open_target_queue_store to fail on file base path"),
            Err(err) => assert!(err.to_string().contains("custom open context")),
        }
        let _ = fs::remove_file(base);
    }

    #[test]
    fn persist_queued_payload_to_store_writes_encoded_payload() {
        let store = MockQueuedStore::new(false);
        let meta = QueuedPayloadMeta::new(
            EventName::ObjectCreatedPut,
            "bucket-a".to_string(),
            "obj.txt".to_string(),
            "application/json",
            7,
        );
        let queued = QueuedPayload::new(meta, br#"{"x":1}"#.to_vec());

        persist_queued_payload_to_store(&store, &queued).unwrap();

        let writes = store.writes.lock().expect("mock writes lock poisoned");
        assert_eq!(writes.len(), 1);
        let decoded = QueuedPayload::decode(&writes[0]).unwrap();
        assert_eq!(decoded.body, br#"{"x":1}"#);
    }

    #[test]
    fn persist_queued_payload_to_store_maps_store_error() {
        let store = MockQueuedStore::new(true);
        let meta = QueuedPayloadMeta::new(
            EventName::ObjectCreatedPut,
            "bucket-a".to_string(),
            "obj.txt".to_string(),
            "application/json",
            7,
        );
        let queued = QueuedPayload::new(meta, br#"{"x":1}"#.to_vec());

        let err = persist_queued_payload_to_store(&store, &queued).expect_err("expected put_raw failure");
        assert!(err.to_string().contains("Failed to save event to store"));
    }

    #[test]
    fn is_connectivity_error_classifies_target_errors() {
        assert!(is_connectivity_error(&TargetError::NotConnected));
        assert!(is_connectivity_error(&TargetError::Timeout("timeout".to_string())));
        assert!(is_connectivity_error(&TargetError::Network("network".to_string())));
        assert!(!is_connectivity_error(&TargetError::Storage("storage".to_string())));
        assert!(!is_connectivity_error(&TargetError::Serialization("serialization".to_string())));
    }

    #[test]
    fn queued_payload_decode_rejects_invalid_magic() {
        let err = QueuedPayload::decode(b"bad-payload").unwrap_err();
        assert!(err.to_string().contains("magic") || err.to_string().contains("short"));
    }

    #[test]
    fn sanitize_queue_dir_component_replaces_non_path_safe_characters() {
        let sanitized = sanitize_queue_dir_component("tenant:alpha/beta\\gamma?*");
        assert_eq!(sanitized, "tenant_alpha_beta_gamma__");
    }

    #[test]
    fn queue_store_subdir_name_sanitizes_target_id() {
        let dir = queue_store_subdir_name("redis", "tenant:alpha");
        assert_eq!(dir, "rustfs-redis-tenant_alpha");
    }
}
