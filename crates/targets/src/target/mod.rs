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
use crate::plugin::PluginEvent;
use crate::store::{FailedEventStore, Key, QueueStore, Store};
use crate::{StoreError, TargetError, TargetLog};
use async_trait::async_trait;
use rustfs_s3_types::EventName;
use serde::{Deserialize, Serialize};
use std::fmt::Formatter;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

#[cfg(test)]
pub(crate) use crate::runtime::tls::fingerprint::TargetTlsFingerprint as TargetTlsFingerprintState;
#[cfg(test)]
pub(crate) use crate::runtime::tls::fingerprint::TargetTlsGeneration;
pub(crate) use crate::runtime::tls::fingerprint::TargetTlsState;
pub(crate) use crate::runtime::tls::fingerprint::build_target_tls_fingerprint;

pub(crate) const REDACTED_SECRET: &str = "***redacted***";

pub(crate) fn redacted_secret(value: &str) -> &'static str {
    if value.is_empty() { "" } else { REDACTED_SECRET }
}

pub(crate) fn redacted_optional_secret(value: Option<&str>) -> &'static str {
    value.filter(|secret| !secret.is_empty()).map_or("", |_| REDACTED_SECRET)
}

/// A read-only snapshot of delivery counters for a target.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TargetDeliverySnapshot {
    pub failed_messages: u64,
    pub failed_store_length: u64,
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
    pub fn snapshot(&self, queue_length: u64, failed_store_length: u64) -> TargetDeliverySnapshot {
        TargetDeliverySnapshot {
            failed_messages: self.failed_messages.load(Ordering::Relaxed),
            failed_store_length,
            queue_length,
            total_messages: self.total_messages.load(Ordering::Relaxed),
        }
    }
}

/// Trait for notification targets
#[async_trait]
pub trait Target<E>: Send + Sync + 'static
where
    E: PluginEvent,
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
            Err(StoreError::NotFound) => {
                // The backing file is missing or empty (a zero-byte file reads as
                // NotFound). Left in the index it would be "replayed" forever and
                // permanently occupy a queue slot, eventually rejecting new events
                // with LimitExceeded. Purge the stale index entry (and any residual
                // file) before returning.
                delete_stored_payload(store, &key)?;
                return Ok(());
            }
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

    /// Returns the failed-events store capability when the target records terminal failures.
    ///
    /// The default is no capability, so a target that never parks a terminal entry runs no failed-store
    /// maintenance and reports zero failed-store depth.
    fn failed_store(&self) -> Option<&dyn FailedEventStore> {
        None
    }

    /// Moves a terminally failed entry to the target's failed-events store, returning true when the entry was handled. A target without a terminal-failure store declines the move and the entry stays live.
    async fn handle_terminal_failure(
        &self,
        _store: &(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send),
        _key: &Key,
        _error: &TargetError,
        _retry_count: u32,
    ) -> bool {
        false
    }

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
            failed_store_length: self.failed_store().map_or(0, |failed_store| failed_store.failed_len() as u64),
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
    /// Stable per-entry deduplication identifier sent as the NATS JetStream Nats-Msg-Id header.
    /// Empty for every non-JetStream target and for entries queued before JetStream was enabled, so
    /// it is skipped on serialization and absent stored entries decode to an empty value. This keeps
    /// the stored bytes identical to entries written without the field.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub dedup_id: String,

    /// Set only on an entry written to the failed-events store. None on every live-queue entry, so it
    /// is skipped on serialization and a live entry decodes with no failure metadata, keeping live
    /// stored bytes identical to entries written without the field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<FailedEntryMeta>,
}

/// The error class recorded on a failed-store entry. Only a non-retryable publish error reaches the
/// failed store, so the class confirms a terminal cause for an operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FailedErrorClass {
    /// A non-retryable publish error. Replaying it without fixing the underlying configuration repeats
    /// the failure.
    Terminal,
}

impl FailedErrorClass {
    /// Stable lowercase tag used in structured logs and operator tooling.
    pub fn as_str(&self) -> &'static str {
        match self {
            FailedErrorClass::Terminal => "terminal",
        }
    }
}

/// Failure metadata added to a queued payload when it is moved to the failed-events store, carried
/// inside the existing QueuedPayload meta so the failed entry decodes through the same reader.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailedEntryMeta {
    /// The failure class recorded for operator triage. A failed-store entry is always terminal.
    pub error_class: FailedErrorClass,
    /// An allowlisted, credential-free summary of the failure for operator diagnosis. Never the raw
    /// error rendering.
    pub error_detail: String,
    /// The stored dedup identifier, recording the id the publish attempted. A failed record is never
    /// republished.
    pub nats_msg_id: String,
    /// The instant the entry entered the failed store, a diagnostic field for operator triage. Expiry
    /// uses the filesystem modification time, not this value.
    pub failed_at_unix_ms: u64,
    /// The replay attempt count recorded at the terminal failure.
    pub retry_count: u32,
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
            dedup_id: String::new(),
            failure: None,
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

        let meta: QueuedPayloadMeta = serde_json::from_slice(&raw[meta_start..meta_end])
            .map_err(|err| TargetError::Serialization(format!("Failed to deserialize queued payload metadata: {err}")))?;
        let body = raw[meta_end..].to_vec();

        // Reject torn/truncated writes: the body length recorded at encode time
        // must match the bytes actually present. Without this, a partially
        // written file (e.g. a crash mid-write) would decode into a silently
        // truncated payload and be delivered as if complete.
        if body.len() != meta.payload_len {
            return Err(TargetError::Serialization(format!(
                "Queued payload body length mismatch: header declares {} bytes but {} were present",
                meta.payload_len,
                body.len()
            )));
        }

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

/// Stable, deterministic 64-bit FNV-1a hash used only to disambiguate queue
/// directory names. It must stay identical across restarts and releases so a
/// target keeps resolving to the same on-disk queue directory, hence a fixed
/// inline implementation rather than `DefaultHasher` (whose algorithm is not
/// contractually stable).
fn fnv1a_hash(bytes: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = FNV_OFFSET;
    for &byte in bytes {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Maps a target-id component to a filesystem-safe queue directory name.
///
/// Path-unsafe characters are replaced with an underscore. That replacement is lossy, so two
/// distinct ids (for example a/b and a_b) could otherwise collapse to the same directory and
/// interleave their persisted events. A short hash of the original component is appended whenever any
/// character was replaced, so distinct ids map to distinct directories.
///
/// Ids that are already path-safe are returned unchanged, preserving the on-disk directory layout for
/// existing deployments.
pub(crate) fn sanitize_queue_dir_component(component: &str) -> String {
    let mut sanitized = String::with_capacity(component.len());
    let mut lossy = false;
    for ch in component.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            sanitized.push(ch);
        } else {
            sanitized.push('_');
            lossy = true;
        }
    }

    if sanitized.is_empty() {
        // An entirely non-safe id would otherwise collapse to a single underscore. Key it by
        // the original bytes so distinct ids stay distinct.
        return format!("_{:016x}", fnv1a_hash(component.as_bytes()));
    }

    if lossy {
        // Disambiguate the lossy replacement so different originals cannot alias.
        return format!("{sanitized}-{:016x}", fnv1a_hash(component.as_bytes()));
    }

    sanitized
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
    E: PluginEvent,
{
    build_queued_payload_with_records(event, vec![event.data.clone()])
}

pub(crate) fn build_queued_payload_with_records<E, R>(
    event: &EntityTarget<E>,
    records: Vec<R>,
) -> Result<QueuedPayload, TargetError>
where
    E: PluginEvent,
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
    let store = open_target_queue_store_typed(queue_dir, queue_limit, target_type, target_type_label, target_id, open_context)?;
    Ok(store.map(|store| Box::new(store) as BoxedQueuedStore))
}

/// Opens the queue store and returns the concrete QueueStore, so a target that needs its typed
/// failed-store capability holds it directly rather than through the type-erased Store handle.
pub(crate) fn open_target_queue_store_typed(
    queue_dir: &str,
    queue_limit: u64,
    target_type: TargetType,
    target_type_label: &str,
    target_id: &TargetID,
    open_context: &str,
) -> Result<Option<QueueStore<QueuedPayload>>, TargetError> {
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

    Ok(Some(store))
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

pub(crate) async fn invalidate_cache_on_connectivity_error<F, Fut>(err: &TargetError, invalidate: F)
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = ()>,
{
    if is_connectivity_error(err) {
        invalidate().await;
    }
}

pub(crate) fn mark_target_disconnected_on_connectivity_error(connected: &AtomicBool, err: &TargetError) {
    if is_connectivity_error(err) {
        connected.store(false, Ordering::SeqCst);
    }
}

pub(crate) fn delete_stored_payload(
    store: &(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send),
    key: &Key,
) -> Result<(), TargetError> {
    match store.del(key) {
        Ok(()) | Err(StoreError::NotFound) => Ok(()),
        Err(err) => Err(TargetError::Storage(format!("Failed to delete event from store: {err}"))),
    }
}

/// Upper bound on the characters retained from a classified error so a failed-store entry and its
/// alarm carry a diagnosable summary without an unbounded message.
const FAILED_ERROR_DETAIL_MAX_LEN: usize = 256;

/// Fallback label substituted for a JetStreamPublish detail that falls outside the fixed vocabulary.
const UNRECOGNIZED_DETAIL_LABEL: &str = "unrecognized detail";

/// Restricts a JetStreamPublish detail to the fixed vocabulary at the persistence boundary. A detail
/// of lowercase letters, digits, space, underscore, and colon passes verbatim, any other content is
/// replaced with a fixed fallback label.
fn sanitize_failed_detail(detail: &str) -> &str {
    let in_vocabulary = !detail.is_empty()
        && detail.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || matches!(character, ' ' | '_' | ':')
        });
    if in_vocabulary { detail } else { UNRECOGNIZED_DETAIL_LABEL }
}

/// Builds a credential-free diagnostic string for a failed entry from a classified error. The
/// publish-error detail passes through the persistence-boundary sanitizer, a non-publish error
/// contributes only its variant category, and any embedded value is redacted. The result is
/// length-bounded at a character boundary.
pub(crate) fn build_failed_error_detail(error: &TargetError) -> String {
    let summary = match error {
        // The publish detail is sanitized to the fixed vocabulary at the persistence boundary.
        TargetError::JetStreamPublish { detail, .. } => format!("jetstream_publish: {}", sanitize_failed_detail(detail)),
        TargetError::Dropped(reason) => format!("dropped: {}", redacted_secret(reason)),
        // Every other variant carries a free-form message that may name a host, path, or credential.
        // Only the variant category is recorded, with any embedded value redacted.
        TargetError::Network(value) => format!("network: {}", redacted_secret(value)),
        TargetError::Request(value) => format!("request: {}", redacted_secret(value)),
        TargetError::Timeout(value) => format!("timeout: {}", redacted_secret(value)),
        TargetError::Storage(value) => format!("storage: {}", redacted_secret(value)),
        TargetError::Authentication(_) => "authentication".to_string(),
        TargetError::Configuration(_) => "configuration".to_string(),
        other => format!("error: {}", redacted_secret(&other_error_category(other))),
    };

    let mut detail = summary;
    truncate_to_char_boundary(&mut detail, FAILED_ERROR_DETAIL_MAX_LEN);
    detail
}

/// Truncates the string to at most max_len bytes, stepping the cut down to the nearest character
/// boundary so a multi-byte character straddling the cap is dropped whole rather than split.
fn truncate_to_char_boundary(value: &mut String, max_len: usize) {
    if value.len() <= max_len {
        return;
    }
    let mut cut = max_len;
    while !value.is_char_boundary(cut) {
        cut -= 1;
    }
    value.truncate(cut);
}

/// Names the category of an otherwise free-form error without revealing its message.
fn other_error_category(error: &TargetError) -> String {
    match error {
        TargetError::Encoding(_) => "encoding".to_string(),
        TargetError::Serialization(_) => "serialization".to_string(),
        TargetError::Initialization(_) => "initialization".to_string(),
        TargetError::Unknown(_) => "unknown".to_string(),
        _ => "other".to_string(),
    }
}

/// Encodes a queued payload as a failed-store entry, extending its meta with the failure fields.
///
/// Reuses the QueuedPayload format so the failed entry decodes through the same reader, preserving the
/// routing meta. The error_detail is the credential-free summary. The nats_msg_id is the resolved
/// dedup id, so an operator sees the id the server saw even for a pre-enable entry.
pub(crate) fn encode_failed_entry(
    mut queued: QueuedPayload,
    error_class: FailedErrorClass,
    error: &TargetError,
    retry_count: u32,
    resolved_dedup_id: &str,
) -> Result<Vec<u8>, TargetError> {
    let failed_at_unix_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
    queued.meta.failure = Some(FailedEntryMeta {
        error_class,
        error_detail: build_failed_error_detail(error),
        nats_msg_id: resolved_dedup_id.to_string(),
        failed_at_unix_ms,
        retry_count,
    });
    queued.encode()
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
pub(crate) mod test_support {
    use super::{EntityTarget, QueuedPayload, QueuedPayloadMeta};
    use crate::arn::TargetID;
    use crate::store::{FailedEventStore, Key, QueueStore, Store};
    use crate::{StoreError, Target, TargetError};
    use async_trait::async_trait;
    use rustfs_s3_types::EventName;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use uuid::Uuid;

    /// A minimal target for failed-store move tests: every delivery method succeeds, the optional
    /// store backs the store and failed-store accessors, and final failures land on a shared counter.
    #[derive(Clone)]
    pub(crate) struct MoveTestTarget {
        pub(crate) id: TargetID,
        pub(crate) store: Option<Arc<QueueStore<QueuedPayload>>>,
        pub(crate) failed: Arc<AtomicU64>,
    }

    #[async_trait]
    impl Target<String> for MoveTestTarget {
        fn id(&self) -> TargetID {
            self.id.clone()
        }
        async fn is_active(&self) -> Result<bool, TargetError> {
            Ok(true)
        }
        async fn save(&self, _event: Arc<EntityTarget<String>>) -> Result<(), TargetError> {
            Ok(())
        }
        async fn send_raw_from_store(&self, _key: Key, _body: Vec<u8>, _meta: QueuedPayloadMeta) -> Result<(), TargetError> {
            Ok(())
        }
        async fn close(&self) -> Result<(), TargetError> {
            Ok(())
        }
        fn store(&self) -> Option<&(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync)> {
            self.store
                .as_deref()
                .map(|store| store as &(dyn Store<_, Error = StoreError, Key = Key> + Send + Sync))
        }
        fn failed_store(&self) -> Option<&dyn FailedEventStore> {
            self.store.as_deref().map(|store| store as &dyn FailedEventStore)
        }
        fn clone_dyn(&self) -> Box<dyn Target<String> + Send + Sync> {
            Box::new(self.clone())
        }
        fn is_enabled(&self) -> bool {
            true
        }
        fn record_final_failure(&self) {
            self.failed.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn move_test_target() -> Arc<dyn Target<String> + Send + Sync> {
        Arc::new(MoveTestTarget {
            id: TargetID::new("target-a".to_string(), "nats".to_string()),
            store: None,
            failed: Arc::new(AtomicU64::new(0)),
        })
    }

    pub(crate) fn move_test_target_with_store(store: Arc<QueueStore<QueuedPayload>>) -> Arc<dyn Target<String> + Send + Sync> {
        Arc::new(MoveTestTarget {
            id: TargetID::new("target-a".to_string(), "nats".to_string()),
            store: Some(store),
            failed: Arc::new(AtomicU64::new(0)),
        })
    }

    pub(crate) fn failed_store_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("rustfs-failed-{name}-{}", Uuid::new_v4()))
    }

    pub(crate) fn sample_queued(dedup_id: &str) -> QueuedPayload {
        let mut meta = QueuedPayloadMeta::new(
            EventName::ObjectCreatedPut,
            "bucket-a".to_string(),
            "obj.txt".to_string(),
            "application/json",
            7,
        );
        meta.dedup_id = dedup_id.to_string();
        QueuedPayload::new(meta, br#"{"x":1}"#.to_vec())
    }
}

#[cfg(test)]
mod tls_state_tests {
    use super::{TargetTlsFingerprintState, TargetTlsGeneration, TargetTlsState};

    #[test]
    fn refresh_increments_generation_only_when_fingerprint_changes() {
        let mut state = TargetTlsState::default();
        let first = TargetTlsFingerprintState {
            ca_sha256: Some([1; 32]),
            client_cert_sha256: None,
            client_key_sha256: None,
        };
        let second = TargetTlsFingerprintState {
            ca_sha256: Some([2; 32]),
            client_cert_sha256: None,
            client_key_sha256: None,
        };

        assert!(state.refresh(first.clone()));
        assert_eq!(state.generation, TargetTlsGeneration(1));
        assert!(!state.refresh(first));
        assert_eq!(state.generation, TargetTlsGeneration(1));
        assert!(state.refresh(second));
        assert_eq!(state.generation, TargetTlsGeneration(2));
    }

    #[test]
    fn reset_clears_generation_and_fingerprint() {
        let mut state = TargetTlsState {
            generation: TargetTlsGeneration(5),
            fingerprint: Some(TargetTlsFingerprintState {
                ca_sha256: Some([9; 32]),
                client_cert_sha256: None,
                client_key_sha256: None,
            }),
        };

        state.reset();
        assert_eq!(state, TargetTlsState::default());
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
    fn queued_payload_meta_omits_empty_dedup_id_on_serialization() {
        let meta = QueuedPayloadMeta::new(
            EventName::ObjectCreatedPut,
            "bucket-a".to_string(),
            "obj.txt".to_string(),
            "application/json",
            7,
        );
        assert!(meta.dedup_id.is_empty());

        let json = serde_json::to_string(&meta).unwrap();
        assert!(
            !json.contains("dedup_id"),
            "an empty dedup id is skipped so stored bytes match the pre-feature format"
        );

        // An entry written without the field decodes to an empty dedup id.
        let decoded: QueuedPayloadMeta = serde_json::from_str(&json).unwrap();
        assert!(decoded.dedup_id.is_empty());
    }

    #[test]
    fn queued_payload_meta_round_trips_a_populated_dedup_id() {
        let mut meta = QueuedPayloadMeta::new(
            EventName::ObjectCreatedPut,
            "bucket-a".to_string(),
            "obj.txt".to_string(),
            "application/json",
            7,
        );
        meta.dedup_id = "minted-id".to_string();

        let json = serde_json::to_string(&meta).unwrap();
        assert!(json.contains("dedup_id"));
        let decoded: QueuedPayloadMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.dedup_id, "minted-id");
    }

    #[test]
    fn queued_payload_round_trips_meta_and_body() {
        let body = br#"{"ok":true}"#.to_vec();
        let meta = QueuedPayloadMeta::new(
            EventName::ObjectCreatedPut,
            "bucket-a".to_string(),
            "folder/object.txt".to_string(),
            "application/json",
            body.len(),
        );
        let payload = QueuedPayload::new(meta.clone(), body);

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

    #[tokio::test]
    async fn invalidate_cache_on_connectivity_error_only_runs_for_connectivity_failures() {
        let marker = Arc::new(AtomicBool::new(false));
        invalidate_cache_on_connectivity_error(&TargetError::NotConnected, {
            let marker = Arc::clone(&marker);
            move || async move {
                marker.store(true, Ordering::SeqCst);
            }
        })
        .await;
        assert!(marker.load(Ordering::SeqCst));

        marker.store(false, Ordering::SeqCst);
        invalidate_cache_on_connectivity_error(&TargetError::Request("request failed".to_string()), {
            let marker = Arc::clone(&marker);
            move || async move {
                marker.store(true, Ordering::SeqCst);
            }
        })
        .await;
        assert!(!marker.load(Ordering::SeqCst));
    }

    #[test]
    fn mark_target_disconnected_on_connectivity_error_only_marks_connectivity_failures() {
        let connected = AtomicBool::new(true);
        mark_target_disconnected_on_connectivity_error(&connected, &TargetError::Timeout("timeout".to_string()));
        assert!(!connected.load(Ordering::SeqCst));

        connected.store(true, Ordering::SeqCst);
        mark_target_disconnected_on_connectivity_error(&connected, &TargetError::Request("request failed".to_string()));
        assert!(connected.load(Ordering::SeqCst));
    }

    #[test]
    fn queued_payload_decode_rejects_invalid_magic() {
        let err = QueuedPayload::decode(b"bad-payload").unwrap_err();
        assert!(err.to_string().contains("magic") || err.to_string().contains("short"));
    }

    #[test]
    fn sanitize_queue_dir_component_replaces_non_path_safe_characters() {
        let sanitized = sanitize_queue_dir_component("tenant:alpha/beta\\gamma?*");
        // The readable, path-safe prefix is preserved, followed by a disambiguating
        // hash suffix because the replacement was lossy.
        assert!(
            sanitized.starts_with("tenant_alpha_beta_gamma__-"),
            "unexpected sanitized value: {sanitized}"
        );
        // Deterministic across calls (must be stable across restarts).
        assert_eq!(sanitized, sanitize_queue_dir_component("tenant:alpha/beta\\gamma?*"));
    }

    #[test]
    fn sanitize_queue_dir_component_preserves_path_safe_ids() {
        // Path-safe ids are returned unchanged so existing on-disk queue
        // directories keep resolving (no migration on upgrade).
        assert_eq!(sanitize_queue_dir_component("plain-id_1.2"), "plain-id_1.2");
    }

    #[test]
    fn sanitize_queue_dir_component_disambiguates_colliding_ids() {
        // Two distinct ids that used to collapse onto the same directory must now
        // map to different directories.
        let a = sanitize_queue_dir_component("a/b");
        let b = sanitize_queue_dir_component("a_b");
        assert_ne!(a, b, "distinct ids must not share a queue directory");
    }

    #[test]
    fn queue_store_subdir_name_sanitizes_target_id() {
        let dir = queue_store_subdir_name("redis", "tenant:alpha");
        assert!(dir.starts_with("rustfs-redis-tenant_alpha-"), "unexpected subdir: {dir}");
    }

    #[derive(Clone)]
    struct StoreBackedTarget {
        id: TargetID,
        store: QueueStore<QueuedPayload>,
    }

    #[async_trait]
    impl Target<String> for StoreBackedTarget {
        fn id(&self) -> TargetID {
            self.id.clone()
        }

        async fn is_active(&self) -> Result<bool, TargetError> {
            Ok(true)
        }

        async fn save(&self, _event: Arc<EntityTarget<String>>) -> Result<(), TargetError> {
            Ok(())
        }

        async fn send_raw_from_store(&self, _key: Key, _body: Vec<u8>, _meta: QueuedPayloadMeta) -> Result<(), TargetError> {
            Ok(())
        }

        async fn close(&self) -> Result<(), TargetError> {
            Ok(())
        }

        fn store(&self) -> Option<&(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync)> {
            Some(&self.store)
        }

        fn clone_dyn(&self) -> Box<dyn Target<String> + Send + Sync> {
            Box::new(self.clone())
        }

        fn is_enabled(&self) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn send_from_store_purges_missing_or_empty_entry() {
        let dir = std::env::temp_dir().join(format!("rustfs-send-from-store-{}", Uuid::new_v4()));
        let store = QueueStore::<QueuedPayload>::new_with_compression(&dir, 8, ".event", false);
        store.open().unwrap();

        // Enqueue a valid payload, then truncate its backing file to zero bytes to
        // simulate a torn write: read_file now reports NotFound while the index
        // still counts the entry.
        let meta = QueuedPayloadMeta::new(
            EventName::ObjectCreatedPut,
            "bucket-a".to_string(),
            "obj.txt".to_string(),
            "application/json",
            7,
        );
        let encoded = QueuedPayload::new(meta, br#"{"x":1}"#.to_vec()).encode().unwrap();
        let key = store.put_raw(&encoded).unwrap();
        assert_eq!(store.len(), 1);

        let event_file = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| p.is_file())
            .expect("event file should exist");
        std::fs::write(&event_file, b"").unwrap();

        let target = StoreBackedTarget {
            id: TargetID::new("primary".to_string(), "webhook".to_string()),
            store: store.clone(),
        };

        // A NotFound/empty entry must be purged (index + file) rather than
        // silently skipped and replayed forever.
        target.send_from_store(key).await.unwrap();
        assert_eq!(store.len(), 0, "stale entry must be removed from the index");

        let _ = store.delete();
    }

    #[test]
    fn queued_payload_decode_rejects_body_length_mismatch() {
        let meta = QueuedPayloadMeta::new(
            EventName::ObjectCreatedPut,
            "bucket-a".to_string(),
            "obj.txt".to_string(),
            "application/json",
            11,
        );
        let payload = QueuedPayload::new(meta, br#"{"ok":true}"#.to_vec());
        let mut encoded = payload.encode().unwrap();

        // Drop the final body byte, simulating a torn/truncated write. The header
        // still declares the original payload_len, so decode must reject it rather
        // than hand back a silently truncated body.
        encoded.pop();
        let err = QueuedPayload::decode(&encoded).unwrap_err();
        assert!(err.to_string().contains("body length mismatch"), "unexpected error: {err}");
    }

    use super::test_support::sample_queued;

    // The error_detail recorded on a failed entry is built from an allowlist of error categories and
    // the fixed publish-error vocabulary, never the raw error rendering, so a credential embedded in
    // a free-form error message is absent from the detail.
    #[test]
    fn build_failed_error_detail_redacts_credential_bearing_messages() {
        let secret = "nats://user:supersecret@broker:4222";
        let cases = [
            TargetError::Network(secret.to_string()),
            TargetError::Request(secret.to_string()),
            TargetError::Timeout(secret.to_string()),
            TargetError::Storage(secret.to_string()),
            TargetError::Authentication(secret.to_string()),
            TargetError::Configuration(secret.to_string()),
            TargetError::Dropped(secret.to_string()),
            TargetError::Unknown(secret.to_string()),
        ];
        for error in cases {
            let detail = build_failed_error_detail(&error);
            assert!(!detail.contains("supersecret"), "detail leaked a credential: {detail}");
            assert!(!detail.contains("broker:4222"), "detail leaked a connection string: {detail}");
        }
    }

    // The publish-error classifier sets the detail from its fixed vocabulary of kind labels and
    // numeric codes, so the failed-entry detail names the cause without leaking a server-side
    // message.
    #[test]
    fn build_failed_error_detail_uses_the_classified_publish_kind() {
        let error = TargetError::JetStreamPublish {
            retryable: false,
            detail: "max payload exceeded".to_string(),
        };
        let detail = build_failed_error_detail(&error);
        assert_eq!(detail, "jetstream_publish: max payload exceeded");
    }

    // The persistence-boundary sanitizer replaces a JetStreamPublish detail that carries anything
    // outside the fixed vocabulary with the fallback label, while a vocabulary detail passes through
    // verbatim.
    #[test]
    fn build_failed_error_detail_sanitizes_a_hostile_jetstream_detail() {
        let hostile = TargetError::JetStreamPublish {
            retryable: false,
            detail: "Boom! nats://user:pass@host/DROP".to_string(),
        };
        assert_eq!(build_failed_error_detail(&hostile), "jetstream_publish: unrecognized detail");

        let vocabulary = TargetError::JetStreamPublish {
            retryable: false,
            detail: "wrong last sequence".to_string(),
        };
        assert_eq!(build_failed_error_detail(&vocabulary), "jetstream_publish: wrong last sequence");
    }

    // A summary whose cap lands inside a multi-byte character truncates at the preceding character
    // boundary instead of panicking on a split character. A three-byte character does not divide
    // the 256-byte cap, so one character straddles the cap by construction.
    #[test]
    fn truncate_to_char_boundary_handles_multi_byte_characters_at_the_cap() {
        let mut value = "\u{4e2d}".repeat(FAILED_ERROR_DETAIL_MAX_LEN);
        assert!(!value.is_char_boundary(FAILED_ERROR_DETAIL_MAX_LEN), "a character straddles the cap");
        truncate_to_char_boundary(&mut value, FAILED_ERROR_DETAIL_MAX_LEN);
        assert!(value.len() <= FAILED_ERROR_DETAIL_MAX_LEN, "the result stays within the cap");
        assert!(value.is_char_boundary(value.len()), "the result ends on a character boundary");
    }

    // A terminal move writes a failed entry carrying the full failure meta extension and preserves the
    // original routing metadata and dedup id through the reused QueuedPayload format.
    #[test]
    fn encode_failed_entry_carries_full_failure_meta() {
        let queued = sample_queued("minted-id");
        let error = TargetError::JetStreamPublish {
            retryable: false,
            detail: "wrong last sequence".to_string(),
        };
        let encoded = encode_failed_entry(queued, FailedErrorClass::Terminal, &error, 0, "minted-id").unwrap();
        let decoded = QueuedPayload::decode(&encoded).unwrap();

        let failure = decoded.meta.failure.expect("a failed entry carries failure meta");
        assert_eq!(failure.error_class, FailedErrorClass::Terminal);
        assert_eq!(failure.error_detail, "jetstream_publish: wrong last sequence");
        assert_eq!(failure.nats_msg_id, "minted-id");
        assert_eq!(failure.retry_count, 0);
        assert!(failure.failed_at_unix_ms > 0);
        // The original routing metadata survives the move.
        assert_eq!(decoded.meta.bucket_name, "bucket-a");
        assert_eq!(decoded.meta.object_name, "obj.txt");
        assert_eq!(decoded.body, br#"{"x":1}"#);
    }
}
