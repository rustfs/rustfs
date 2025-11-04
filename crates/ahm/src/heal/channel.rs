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

use crate::error::{Error, Result};
use crate::heal::{
    manager::HealManager,
    task::{HealOptions, HealPriority, HealRequest, HealType},
    utils,
};
use rustfs_common::heal_channel::{
    HealChannelCommand, HealChannelPriority, HealChannelReceiver, HealChannelRequest, HealChannelResponse, HealScanMode,
    publish_heal_response,
};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

/// Heal channel processor
pub struct HealChannelProcessor {
    /// Heal manager
    heal_manager: Arc<HealManager>,
    /// Response sender
    response_sender: mpsc::UnboundedSender<HealChannelResponse>,
    /// Response receiver
    response_receiver: mpsc::UnboundedReceiver<HealChannelResponse>,
}

impl HealChannelProcessor {
    /// Create new HealChannelProcessor
    pub fn new(heal_manager: Arc<HealManager>) -> Self {
        let (response_tx, response_rx) = mpsc::unbounded_channel();
        Self {
            heal_manager,
            response_sender: response_tx,
            response_receiver: response_rx,
        }
    }

    /// Start processing heal channel requests
    pub async fn start(&mut self, mut receiver: HealChannelReceiver) -> Result<()> {
        info!("Starting heal channel processor");

        loop {
            tokio::select! {
                command = receiver.recv() => {
                    match command {
                        Some(command) => {
                            if let Err(e) = self.process_command(command).await {
                                error!("Failed to process heal command: {}", e);
                            }
                        }
                        None => {
                            debug!("Heal channel receiver closed, stopping processor");
                            break;
                        }
                    }
                }
                response = self.response_receiver.recv() => {
                    if let Some(response) = response {
                        // Handle response if needed
                        info!("Received heal response for request: {}", response.request_id);
                    }
                }
            }
        }

        info!("Heal channel processor stopped");
        Ok(())
    }

    /// Process heal command
    async fn process_command(&self, command: HealChannelCommand) -> Result<()> {
        match command {
            HealChannelCommand::Start(request) => self.process_start_request(request).await,
            HealChannelCommand::Query { heal_path, client_token } => self.process_query_request(heal_path, client_token).await,
            HealChannelCommand::Cancel { heal_path } => self.process_cancel_request(heal_path).await,
        }
    }

    /// Process start request
    async fn process_start_request(&self, request: HealChannelRequest) -> Result<()> {
        info!("Processing heal start request: {} for bucket: {}", request.id, request.bucket);

        // Convert channel request to heal request
        let heal_request = self.convert_to_heal_request(request.clone())?;

        // Submit to heal manager
        match self.heal_manager.submit_heal_request(heal_request).await {
            Ok(task_id) => {
                info!("Successfully submitted heal request: {} as task: {}", request.id, task_id);

                let response = HealChannelResponse {
                    request_id: request.id,
                    success: true,
                    data: Some(format!("Task ID: {task_id}").into_bytes()),
                    error: None,
                };

                self.publish_response(response);
            }
            Err(e) => {
                error!("Failed to submit heal request: {} - {}", request.id, e);

                // Send error response
                let response = HealChannelResponse {
                    request_id: request.id,
                    success: false,
                    data: None,
                    error: Some(e.to_string()),
                };

                self.publish_response(response);
            }
        }

        Ok(())
    }

    /// Process query request
    async fn process_query_request(&self, heal_path: String, client_token: String) -> Result<()> {
        info!("Processing heal query request for path: {}", heal_path);

        // TODO: Implement query logic based on heal_path and client_token
        // For now, return a placeholder response
        let response = HealChannelResponse {
            request_id: client_token,
            success: true,
            data: Some(format!("Query result for path: {heal_path}").into_bytes()),
            error: None,
        };

        self.publish_response(response);

        Ok(())
    }

    /// Process cancel request
    async fn process_cancel_request(&self, heal_path: String) -> Result<()> {
        info!("Processing heal cancel request for path: {}", heal_path);

        // TODO: Implement cancel logic based on heal_path
        // For now, return a placeholder response
        let response = HealChannelResponse {
            request_id: heal_path.clone(),
            success: true,
            data: Some(format!("Cancel request for path: {heal_path}").into_bytes()),
            error: None,
        };

        self.publish_response(response);

        Ok(())
    }

    /// Convert channel request to heal request
    fn convert_to_heal_request(&self, request: HealChannelRequest) -> Result<HealRequest> {
        let heal_type = if let Some(disk_id) = &request.disk {
            let set_disk_id = utils::normalize_set_disk_id(disk_id).ok_or_else(|| Error::InvalidHealType {
                heal_type: format!("erasure-set({disk_id})"),
            })?;
            HealType::ErasureSet {
                buckets: vec![],
                set_disk_id,
            }
        } else if let Some(prefix) = &request.object_prefix {
            if !prefix.is_empty() {
                HealType::Object {
                    bucket: request.bucket.clone(),
                    object: prefix.clone(),
                    version_id: None,
                }
            } else {
                HealType::Bucket {
                    bucket: request.bucket.clone(),
                }
            }
        } else {
            HealType::Bucket {
                bucket: request.bucket.clone(),
            }
        };

        let priority = match request.priority {
            HealChannelPriority::Low => HealPriority::Low,
            HealChannelPriority::Normal => HealPriority::Normal,
            HealChannelPriority::High => HealPriority::High,
            HealChannelPriority::Critical => HealPriority::Urgent,
        };

        // Build HealOptions with all available fields
        let mut options = HealOptions {
            scan_mode: request.scan_mode.unwrap_or(HealScanMode::Normal),
            remove_corrupted: request.remove_corrupted.unwrap_or(false),
            recreate_missing: request.recreate_missing.unwrap_or(true),
            update_parity: request.update_parity.unwrap_or(true),
            recursive: request.recursive.unwrap_or(false),
            dry_run: request.dry_run.unwrap_or(false),
            timeout: request.timeout_seconds.map(std::time::Duration::from_secs),
            pool_index: request.pool_index,
            set_index: request.set_index,
        };

        // Apply force_start overrides
        if request.force_start {
            options.remove_corrupted = true;
            options.recreate_missing = true;
            options.update_parity = true;
        }

        Ok(HealRequest::new(heal_type, options, priority))
    }

    fn publish_response(&self, response: HealChannelResponse) {
        // Try to send to local channel first, but don't block broadcast on failure
        if let Err(e) = self.response_sender.send(response.clone()) {
            error!("Failed to enqueue heal response locally: {}", e);
        }
        // Always attempt to broadcast, even if local send failed
        // Use the original response to avoid unnecessary clone
        if let Err(e) = publish_heal_response(response) {
            error!("Failed to broadcast heal response: {}", e);
        }
    }

    /// Get response sender for external use
    pub fn get_response_sender(&self) -> mpsc::UnboundedSender<HealChannelResponse> {
        self.response_sender.clone()
    }
}
