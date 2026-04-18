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
use crate::store::{Key, Store};
use crate::{StoreError, TargetError};
use async_trait::async_trait;
use rustfs_s3_common::EventName;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::warn;

pub mod mqtt;
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
/// - `Webhook`: Represents a webhook target for sending notifications via HTTP requests.
/// - `Kafka`: Represents a Kafka target for sending notifications to a Kafka topic.
/// - `Mqtt`: Represents an MQTT target for sending notifications via MQTT protocol.
///
/// Each variant has an associated string representation that can be used for serialization
/// or logging purposes.
/// The `as_str` method returns the string representation of the target type,
/// and the `Display` implementation allows for easy formatting of the target type as a string.
///
/// example usage:
/// ```rust
/// use rustfs_targets::target::ChannelTargetType;
///
/// let target_type = ChannelTargetType::Webhook;
/// assert_eq!(target_type.as_str(), "webhook");
/// println!("Target type: {}", target_type);
/// ```
///
/// example output:
/// Target type: webhook
pub enum ChannelTargetType {
    Webhook,
    Kafka,
    Mqtt,
}

impl ChannelTargetType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ChannelTargetType::Webhook => "webhook",
            ChannelTargetType::Kafka => "kafka",
            ChannelTargetType::Mqtt => "mqtt",
        }
    }
}

impl std::fmt::Display for ChannelTargetType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ChannelTargetType::Webhook => write!(f, "webhook"),
            ChannelTargetType::Kafka => write!(f, "kafka"),
            ChannelTargetType::Mqtt => write!(f, "mqtt"),
        }
    }
}

pub fn parse_bool(value: &str) -> Result<bool, TargetError> {
    match value.to_lowercase().as_str() {
        "true" | "on" | "yes" | "1" => Ok(true),
        "false" | "off" | "no" | "0" => Ok(false),
        _ => Err(TargetError::ParseError(format!("Unable to parse boolean: {value}"))),
    }
}

/// `TargetType` enum represents the type of target in the notification system.
#[derive(Debug, Clone)]
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

pub(crate) fn delete_stored_payload(
    store: &(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync),
    key: &Key,
) -> Result<(), TargetError> {
    match store.del(key) {
        Ok(()) | Err(StoreError::NotFound) => Ok(()),
        Err(err) => Err(TargetError::Storage(format!("Failed to delete event from store: {err}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn queued_payload_decode_rejects_invalid_magic() {
        let err = QueuedPayload::decode(b"bad-payload").unwrap_err();
        assert!(err.to_string().contains("magic") || err.to_string().contains("short"));
    }
}
