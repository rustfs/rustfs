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

use std::sync::OnceLock;
use tokio::sync::mpsc;
use uuid::Uuid;

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
#[derive(Debug, Clone)]
pub struct HealChannelRequest {
    /// Unique request ID
    pub id: String,
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
    pub scan_mode: Option<HealChannelScanMode>,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealChannelPriority {
    /// Low priority
    Low,
    /// Normal priority
    Normal,
    /// High priority
    High,
    /// Critical priority
    Critical,
}

impl Default for HealChannelPriority {
    fn default() -> Self {
        Self::Normal
    }
}

/// Heal channel sender
pub type HealChannelSender = mpsc::UnboundedSender<HealChannelCommand>;

/// Heal channel receiver
pub type HealChannelReceiver = mpsc::UnboundedReceiver<HealChannelCommand>;

/// Global heal channel sender
static GLOBAL_HEAL_CHANNEL_SENDER: OnceLock<HealChannelSender> = OnceLock::new();

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
            .map_err(|e| format!("Failed to send heal command: {}", e))?;
        Ok(())
    } else {
        Err("Heal channel not initialized".to_string())
    }
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
    scan_mode: Option<HealChannelScanMode>,
    remove_corrupted: Option<bool>,
    recreate_missing: Option<bool>,
    update_parity: Option<bool>,
    recursive: Option<bool>,
    dry_run: Option<bool>,
    timeout_seconds: Option<u64>,
) -> HealChannelRequest {
    HealChannelRequest {
        id: Uuid::new_v4().to_string(),
        bucket,
        object_prefix,
        force_start,
        priority: priority.unwrap_or_default(),
        pool_index,
        set_index,
        scan_mode,
        remove_corrupted,
        recreate_missing,
        update_parity,
        recursive,
        dry_run,
        timeout_seconds,
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

/// Heal scan mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealChannelScanMode {
    /// Normal scan
    Normal,
    /// Deep scan
    Deep,
}
