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

use s3s::dto::{BucketLifecycleConfiguration, ExpirationStatus, LifecycleRule, ReplicationConfiguration, ReplicationRuleStatus};
use serde::{Deserialize, Serialize};
use std::{
    fmt::{self, Display},
    sync::OnceLock,
};
use tokio::sync::{broadcast, mpsc};
use uuid::Uuid;

pub const HEAL_DELETE_DANGLING: bool = true;
pub const RUSTFS_RESERVED_BUCKET: &str = "rustfs";
pub const RUSTFS_RESERVED_BUCKET_PATH: &str = "/rustfs";

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum HealItemType {
    Metadata,
    Bucket,
    BucketMetadata,
    Object,
}

impl HealItemType {
    pub fn to_str(&self) -> &str {
        match self {
            HealItemType::Metadata => "metadata",
            HealItemType::Bucket => "bucket",
            HealItemType::BucketMetadata => "bucket-metadata",
            HealItemType::Object => "object",
        }
    }
}

impl Display for HealItemType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_str())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum DriveState {
    Ok,
    Offline,
    Corrupt,
    Missing,
    PermissionDenied,
    Faulty,
    RootMount,
    Unknown,
    Unformatted, // only returned by disk
}

impl DriveState {
    pub fn to_str(&self) -> &str {
        match self {
            DriveState::Ok => "ok",
            DriveState::Offline => "offline",
            DriveState::Corrupt => "corrupt",
            DriveState::Missing => "missing",
            DriveState::PermissionDenied => "permission-denied",
            DriveState::Faulty => "faulty",
            DriveState::RootMount => "root-mount",
            DriveState::Unknown => "unknown",
            DriveState::Unformatted => "unformatted",
        }
    }
}

impl Display for DriveState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_str())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum HealScanMode {
    Unknown = 0,
    #[default]
    Normal = 1,
    Deep = 2,
}

impl Serialize for HealScanMode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u8(*self as u8)
    }
}

impl<'de> Deserialize<'de> for HealScanMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct HealScanModeVisitor;

        impl<'de> serde::de::Visitor<'de> for HealScanModeVisitor {
            type Value = HealScanMode;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("an integer between 0 and 2")
            }

            fn visit_u8<E>(self, value: u8) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    0 => Ok(HealScanMode::Unknown),
                    1 => Ok(HealScanMode::Normal),
                    2 => Ok(HealScanMode::Deep),
                    _ => Err(E::custom(format!("invalid HealScanMode value: {value}"))),
                }
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if value > u8::MAX as u64 {
                    return Err(E::custom(format!("HealScanMode value too large: {value}")));
                }
                self.visit_u8(value as u8)
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if value < 0 || value > u8::MAX as i64 {
                    return Err(E::custom(format!("invalid HealScanMode value: {value}")));
                }
                self.visit_u8(value as u8)
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                // Try parsing as number string first (for URL-encoded values)
                if let Ok(num) = value.parse::<u8>() {
                    return self.visit_u8(num);
                }
                // Try parsing as named string
                match value {
                    "Unknown" | "unknown" => Ok(HealScanMode::Unknown),
                    "Normal" | "normal" => Ok(HealScanMode::Normal),
                    "Deep" | "deep" => Ok(HealScanMode::Deep),
                    _ => Err(E::custom(format!("invalid HealScanMode string: {value}"))),
                }
            }
        }

        deserializer.deserialize_any(HealScanModeVisitor)
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct HealOpts {
    pub recursive: bool,
    #[serde(rename = "dryRun")]
    pub dry_run: bool,
    pub remove: bool,
    pub recreate: bool,
    #[serde(rename = "scanMode")]
    pub scan_mode: HealScanMode,
    #[serde(rename = "updateParity")]
    pub update_parity: bool,
    #[serde(rename = "nolock")]
    pub no_lock: bool,
    #[serde(rename = "pool", default)]
    pub pool: Option<usize>,
    #[serde(rename = "set", default)]
    pub set: Option<usize>,
}

/// Heal channel command type
#[derive(Debug, Clone)]
pub enum HealChannelCommand {
    /// Start a new heal task
    Start(HealChannelRequest),
    /// Query heal task status
    Query { heal_path: String, client_token: String },
    /// Cancel heal task
    Cancel { heal_path: String },
}

/// Heal request from admin to ahm
#[derive(Debug, Clone, Default)]
pub struct HealChannelRequest {
    /// Unique request ID
    pub id: String,
    /// Disk ID for heal disk/erasure set task
    pub disk: Option<String>,
    /// Bucket name
    pub bucket: String,
    /// Object prefix (optional)
    pub object_prefix: Option<String>,
    /// Force start heal
    pub force_start: bool,
    /// Priority
    pub priority: HealChannelPriority,
    /// Pool index (optional)
    pub pool_index: Option<usize>,
    /// Set index (optional)
    pub set_index: Option<usize>,
    /// Scan mode (optional)
    pub scan_mode: Option<HealScanMode>,
    /// Whether to remove corrupted data
    pub remove_corrupted: Option<bool>,
    /// Whether to recreate missing data
    pub recreate_missing: Option<bool>,
    /// Whether to update parity
    pub update_parity: Option<bool>,
    /// Whether to recursively process
    pub recursive: Option<bool>,
    /// Whether to dry run
    pub dry_run: Option<bool>,
    /// Timeout in seconds (optional)
    pub timeout_seconds: Option<u64>,
}

/// Heal response from ahm to admin
#[derive(Debug, Clone)]
pub struct HealChannelResponse {
    /// Request ID
    pub request_id: String,
    /// Success status
    pub success: bool,
    /// Response data (if successful)
    pub data: Option<Vec<u8>>,
    /// Error message (if failed)
    pub error: Option<String>,
}

/// Heal priority
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum HealChannelPriority {
    /// Low priority
    Low,
    /// Normal priority
    #[default]
    Normal,
    /// High priority
    High,
    /// Critical priority
    Critical,
}

/// Heal channel sender
pub type HealChannelSender = mpsc::UnboundedSender<HealChannelCommand>;

/// Heal channel receiver
pub type HealChannelReceiver = mpsc::UnboundedReceiver<HealChannelCommand>;

/// Global heal channel sender
static GLOBAL_HEAL_CHANNEL_SENDER: OnceLock<HealChannelSender> = OnceLock::new();

type HealResponseSender = broadcast::Sender<HealChannelResponse>;

/// Global heal response broadcaster
static GLOBAL_HEAL_RESPONSE_SENDER: OnceLock<HealResponseSender> = OnceLock::new();

/// Initialize global heal channel
pub fn init_heal_channel() -> HealChannelReceiver {
    let (tx, rx) = mpsc::unbounded_channel();
    GLOBAL_HEAL_CHANNEL_SENDER
        .set(tx)
        .expect("Heal channel sender already initialized");
    rx
}

/// Get global heal channel sender
pub fn get_heal_channel_sender() -> Option<&'static HealChannelSender> {
    GLOBAL_HEAL_CHANNEL_SENDER.get()
}

/// Send heal command through global channel
pub async fn send_heal_command(command: HealChannelCommand) -> Result<(), String> {
    if let Some(sender) = get_heal_channel_sender() {
        sender
            .send(command)
            .map_err(|e| format!("Failed to send heal command: {e}"))?;
        Ok(())
    } else {
        Err("Heal channel not initialized".to_string())
    }
}

fn heal_response_sender() -> &'static HealResponseSender {
    GLOBAL_HEAL_RESPONSE_SENDER.get_or_init(|| {
        let (tx, _rx) = broadcast::channel(1024);
        tx
    })
}

/// Publish a heal response to subscribers.
pub fn publish_heal_response(response: HealChannelResponse) -> Result<(), broadcast::error::SendError<HealChannelResponse>> {
    heal_response_sender().send(response).map(|_| ())
}

/// Subscribe to heal responses.
pub fn subscribe_heal_responses() -> broadcast::Receiver<HealChannelResponse> {
    heal_response_sender().subscribe()
}

/// Send heal start request
pub async fn send_heal_request(request: HealChannelRequest) -> Result<(), String> {
    send_heal_command(HealChannelCommand::Start(request)).await
}

/// Send heal query request
pub async fn query_heal_status(heal_path: String, client_token: String) -> Result<(), String> {
    send_heal_command(HealChannelCommand::Query { heal_path, client_token }).await
}

/// Send heal cancel request
pub async fn cancel_heal_task(heal_path: String) -> Result<(), String> {
    send_heal_command(HealChannelCommand::Cancel { heal_path }).await
}

/// Create a new heal request
pub fn create_heal_request(
    bucket: String,
    object_prefix: Option<String>,
    force_start: bool,
    priority: Option<HealChannelPriority>,
) -> HealChannelRequest {
    HealChannelRequest {
        id: Uuid::new_v4().to_string(),
        bucket,
        object_prefix,
        force_start,
        priority: priority.unwrap_or_default(),
        pool_index: None,
        set_index: None,
        scan_mode: None,
        remove_corrupted: None,
        recreate_missing: None,
        update_parity: None,
        recursive: None,
        dry_run: None,
        timeout_seconds: None,
        disk: None,
    }
}

/// Create a new heal request with advanced options
pub fn create_heal_request_with_options(
    bucket: String,
    object_prefix: Option<String>,
    force_start: bool,
    priority: Option<HealChannelPriority>,
    pool_index: Option<usize>,
    set_index: Option<usize>,
) -> HealChannelRequest {
    HealChannelRequest {
        id: Uuid::new_v4().to_string(),
        bucket,
        object_prefix,
        force_start,
        priority: priority.unwrap_or_default(),
        pool_index,
        set_index,
        ..Default::default()
    }
}

/// Create a heal response
pub fn create_heal_response(
    request_id: String,
    success: bool,
    data: Option<Vec<u8>>,
    error: Option<String>,
) -> HealChannelResponse {
    HealChannelResponse {
        request_id,
        success,
        data,
        error,
    }
}

fn lc_get_prefix(rule: &LifecycleRule) -> String {
    if let Some(p) = &rule.prefix {
        return p.to_string();
    } else if let Some(filter) = &rule.filter {
        if let Some(p) = &filter.prefix {
            return p.to_string();
        } else if let Some(and) = &filter.and {
            if let Some(p) = &and.prefix {
                return p.to_string();
            }
        }
    }

    "".into()
}

pub fn lc_has_active_rules(config: &BucketLifecycleConfiguration, prefix: &str) -> bool {
    if config.rules.is_empty() {
        return false;
    }

    for rule in config.rules.iter() {
        if rule.status == ExpirationStatus::from_static(ExpirationStatus::DISABLED) {
            continue;
        }
        let rule_prefix = lc_get_prefix(rule);
        if !prefix.is_empty() && !rule_prefix.is_empty() && !prefix.starts_with(&rule_prefix) && !rule_prefix.starts_with(prefix)
        {
            continue;
        }

        if let Some(e) = &rule.noncurrent_version_expiration {
            if let Some(true) = e.noncurrent_days.map(|d| d > 0) {
                return true;
            }
            if let Some(true) = e.newer_noncurrent_versions.map(|d| d > 0) {
                return true;
            }
        }

        if rule.noncurrent_version_transitions.is_some() {
            return true;
        }
        if let Some(true) = rule.expiration.as_ref().map(|e| e.date.is_some()) {
            return true;
        }

        if let Some(true) = rule.expiration.as_ref().map(|e| e.days.is_some()) {
            return true;
        }

        if let Some(Some(true)) = rule.expiration.as_ref().map(|e| e.expired_object_delete_marker) {
            return true;
        }

        if let Some(true) = rule.transitions.as_ref().map(|t| !t.is_empty()) {
            return true;
        }

        if rule.transitions.is_some() {
            return true;
        }
    }
    false
}

pub fn rep_has_active_rules(config: &ReplicationConfiguration, prefix: &str, recursive: bool) -> bool {
    if config.rules.is_empty() {
        return false;
    }

    for rule in config.rules.iter() {
        if rule
            .status
            .eq(&ReplicationRuleStatus::from_static(ReplicationRuleStatus::DISABLED))
        {
            continue;
        }
        if !prefix.is_empty() {
            if let Some(filter) = &rule.filter {
                if let Some(r_prefix) = &filter.prefix {
                    if !r_prefix.is_empty() {
                        // incoming prefix must be in rule prefix
                        if !recursive && !prefix.starts_with(r_prefix) {
                            continue;
                        }
                        // If recursive, we can skip this rule if it doesn't match the tested prefix or level below prefix
                        // does not match
                        if recursive && !r_prefix.starts_with(prefix) && !prefix.starts_with(r_prefix) {
                            continue;
                        }
                    }
                }
            }
        }
        return true;
    }
    false
}

pub async fn send_heal_disk(set_disk_id: String, priority: Option<HealChannelPriority>) -> Result<(), String> {
    let req = HealChannelRequest {
        id: Uuid::new_v4().to_string(),
        bucket: "".to_string(),
        object_prefix: None,
        disk: Some(set_disk_id),
        force_start: false,
        priority: priority.unwrap_or_default(),
        pool_index: None,
        set_index: None,
        scan_mode: None,
        remove_corrupted: None,
        recreate_missing: None,
        update_parity: None,
        recursive: None,
        dry_run: None,
        timeout_seconds: None,
    };
    send_heal_request(req).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn heal_response_broadcast_reaches_subscriber() {
        let mut receiver = subscribe_heal_responses();
        let response = create_heal_response("req-1".to_string(), true, None, None);

        publish_heal_response(response.clone()).expect("publish should succeed");

        let received = receiver.recv().await.expect("should receive heal response");
        assert_eq!(received.request_id, response.request_id);
        assert!(received.success);
    }
}
