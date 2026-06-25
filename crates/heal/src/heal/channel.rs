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

use crate::heal::{
    manager::{HealManager, HealTaskReport},
    progress::HealProgress,
    task::{HealOptions, HealPriority, HealRequest, HealTaskStatus, HealType},
    utils,
};
use crate::{Error, Result};
use rustfs_common::heal_channel::{
    HealAdmissionResult, HealChannelCommand, HealChannelPriority, HealChannelReceiver, HealChannelRequest, HealChannelResponse,
    HealScanMode, publish_heal_response,
};
use rustfs_madmin::heal_commands::HealResultItem;
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info};

const LOG_COMPONENT_HEAL: &str = "heal";
const LOG_SUBSYSTEM_CHANNEL: &str = "channel";
const EVENT_HEAL_CHANNEL_STATE: &str = "heal_channel_state";
const EVENT_HEAL_CHANNEL_REQUEST: &str = "heal_channel_request";
const EVENT_HEAL_CHANNEL_RESPONSE: &str = "heal_channel_response";

/// Heal channel processor
pub struct HealChannelProcessor {
    /// Heal manager
    heal_manager: Arc<HealManager>,
    /// Response sender
    response_sender: mpsc::UnboundedSender<HealChannelResponse>,
    /// Response receiver
    response_receiver: mpsc::UnboundedReceiver<HealChannelResponse>,
}

#[derive(Serialize)]
struct HealTaskStatusPayload {
    summary: String,
    items: Vec<HealResultItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    progress: Option<HealProgress>,
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
        info!(
            target: "rustfs::heal::channel",
            event = EVENT_HEAL_CHANNEL_STATE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_CHANNEL,
            state = "started",
            "Heal channel started"
        );

        loop {
            tokio::select! {
                command = receiver.recv() => {
                    match command {
                        Some(command) => {
                            if let Err(e) = self.process_command(command).await {
                                error!(
                                    target: "rustfs::heal::channel",
                                    event = EVENT_HEAL_CHANNEL_REQUEST,
                                    component = LOG_COMPONENT_HEAL,
                                    subsystem = LOG_SUBSYSTEM_CHANNEL,
                                    state = "process_failed",
                                    error = %e,
                                    "Heal channel processing failed"
                                );
                            }
                        }
                        None => {
                            debug!(
                                target: "rustfs::heal::channel",
                                event = EVENT_HEAL_CHANNEL_STATE,
                                component = LOG_COMPONENT_HEAL,
                                subsystem = LOG_SUBSYSTEM_CHANNEL,
                                state = "receiver_closed",
                                "Heal channel receiver closed"
                            );
                            break;
                        }
                    }
                }
                response = self.response_receiver.recv() => {
                    if let Some(response) = response {
                        // Handle response if needed
                        debug!(
                            target: "rustfs::heal::channel",
                            event = EVENT_HEAL_CHANNEL_RESPONSE,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_CHANNEL,
                            request_id = %response.request_id,
                            success = response.success,
                            state = "received_local",
                            "Heal response observed"
                        );
                    }
                }
            }
        }

        info!(
            target: "rustfs::heal::channel",
            event = EVENT_HEAL_CHANNEL_STATE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_CHANNEL,
            state = "stopped",
            "Heal channel stopped"
        );
        Ok(())
    }

    /// Process heal command
    async fn process_command(&self, command: HealChannelCommand) -> Result<()> {
        match command {
            HealChannelCommand::Start { request, response_tx } => self.process_start_request(request, response_tx).await,
            HealChannelCommand::Query {
                heal_path,
                client_token,
                response_tx,
            } => self.process_query_request(heal_path, client_token, response_tx).await,
            HealChannelCommand::Cancel {
                heal_path,
                client_token,
                response_tx,
            } => self.process_cancel_request(heal_path, client_token, response_tx).await,
        }
    }

    /// Process start request
    async fn process_start_request(
        &self,
        request: HealChannelRequest,
        response_tx: oneshot::Sender<std::result::Result<HealAdmissionResult, String>>,
    ) -> Result<()> {
        debug!(
            target: "rustfs::heal::channel",
            event = EVENT_HEAL_CHANNEL_REQUEST,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_CHANNEL,
            request_id = %request.id,
            bucket = %request.bucket,
            object_prefix = %request.object_prefix.as_deref().unwrap_or(""),
            state = "start_received",
            "Heal start received"
        );

        // Convert channel request to heal request
        let heal_request = match self.convert_to_heal_request(request.clone()) {
            Ok(heal_request) => heal_request,
            Err(err) => {
                let error_text = err.to_string();
                let _ = response_tx.send(Err(error_text.clone()));
                self.publish_response(HealChannelResponse {
                    request_id: request.id,
                    success: false,
                    data: None,
                    error: Some(error_text),
                });
                return Ok(());
            }
        };

        // Submit to heal manager
        match self.heal_manager.submit_heal_request(heal_request).await {
            Ok(admission) => {
                debug!(
                    target: "rustfs::heal::channel",
                    event = EVENT_HEAL_CHANNEL_REQUEST,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_CHANNEL,
                    request_id = %request.id,
                    admission = admission.result_label(),
                    state = "admission_decided",
                    "Heal admission decided"
                );

                let _ = response_tx.send(Ok(admission));

                let (success, error) = match admission {
                    HealAdmissionResult::Accepted | HealAdmissionResult::Merged => (true, None),
                    HealAdmissionResult::Full => (false, Some("Heal request queue is full".to_string())),
                    HealAdmissionResult::Dropped(reason) => (false, Some(format!("Heal request dropped: {}", reason.as_str()))),
                };

                let response = HealChannelResponse {
                    request_id: request.id,
                    success,
                    data: Some(
                        format!("admission={},reason={}", admission.result_label(), admission.reason_label()).into_bytes(),
                    ),
                    error,
                };

                self.publish_response(response);
            }
            Err(e) => {
                let error_text = e.to_string();
                error!(
                    target: "rustfs::heal::channel",
                    event = EVENT_HEAL_CHANNEL_REQUEST,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_CHANNEL,
                    request_id = %request.id,
                    state = "submit_failed",
                    error = %error_text,
                    "Heal start submission failed"
                );
                let _ = response_tx.send(Err(error_text.clone()));

                // Send error response
                let response = HealChannelResponse {
                    request_id: request.id,
                    success: false,
                    data: None,
                    error: Some(error_text),
                };

                self.publish_response(response);
            }
        }

        Ok(())
    }

    /// Process query request
    async fn process_query_request(
        &self,
        heal_path: String,
        client_token: String,
        response_tx: oneshot::Sender<std::result::Result<HealChannelResponse, String>>,
    ) -> Result<()> {
        debug!(
            target: "rustfs::heal::channel",
            event = EVENT_HEAL_CHANNEL_REQUEST,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_CHANNEL,
            request_id = %client_token,
            heal_path = %heal_path,
            state = "query_received",
            "Heal query received"
        );

        let report = if heal_path.trim_matches('/').is_empty() {
            self.heal_manager.get_task_report(&client_token).await
        } else {
            self.heal_manager.get_task_report_for_path(&heal_path, &client_token).await
        };

        let (summary, detail, items, progress) = match report {
            Ok(HealTaskReport {
                status: HealTaskStatus::Pending | HealTaskStatus::Running,
                result_items,
                progress,
            }) => ("running".to_string(), None, result_items, progress),
            Ok(HealTaskReport {
                status: HealTaskStatus::Retrying { error, retry_attempt },
                result_items,
                progress,
            }) => (
                "running".to_string(),
                Some(format!("heal task retrying after recoverable failure, attempt {retry_attempt}: {error}")),
                result_items,
                progress,
            ),
            Ok(HealTaskReport {
                status: HealTaskStatus::Completed,
                result_items,
                progress,
            }) => ("finished".to_string(), None, result_items, progress),
            Ok(HealTaskReport {
                status: HealTaskStatus::Cancelled,
                result_items,
                progress,
            }) => ("stopped".to_string(), Some("heal task cancelled".to_string()), result_items, progress),
            Ok(HealTaskReport {
                status: HealTaskStatus::Timeout,
                result_items,
                progress,
            }) => ("stopped".to_string(), Some("heal task timed out".to_string()), result_items, progress),
            Ok(HealTaskReport {
                status: HealTaskStatus::Failed { error },
                result_items,
                progress,
            }) => ("stopped".to_string(), Some(error), result_items, progress),
            Err(crate::Error::TaskNotFound { .. }) => (
                "notFound".to_string(),
                Some("heal task not found or expired".to_string()),
                Vec::new(),
                None,
            ),
            Err(crate::Error::InvalidClientToken) => {
                let response = HealChannelResponse {
                    request_id: client_token,
                    success: false,
                    data: None,
                    error: Some("invalid heal client token".to_string()),
                };
                let _ = response_tx.send(Ok(response.clone()));
                self.publish_response(response);
                return Ok(());
            }
            Err(err) => {
                let error_text = err.to_string();
                let response = HealChannelResponse {
                    request_id: client_token,
                    success: false,
                    data: None,
                    error: Some(error_text.clone()),
                };
                let _ = response_tx.send(Ok(response.clone()));
                self.publish_response(response);
                return Ok(());
            }
        };

        let data = serde_json::to_vec(&HealTaskStatusPayload {
            summary,
            items,
            progress,
        })
        .map_err(|e| crate::Error::Serialization(format!("failed to serialize heal task status: {e}")))?;

        let response = HealChannelResponse {
            request_id: client_token,
            success: true,
            data: Some(data),
            error: detail,
        };

        let _ = response_tx.send(Ok(response.clone()));
        self.publish_response(response);

        Ok(())
    }

    /// Process cancel request
    async fn process_cancel_request(
        &self,
        heal_path: String,
        client_token: String,
        response_tx: oneshot::Sender<std::result::Result<HealChannelResponse, String>>,
    ) -> Result<()> {
        debug!(
            target: "rustfs::heal::channel",
            event = EVENT_HEAL_CHANNEL_REQUEST,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_CHANNEL,
            request_id = %client_token,
            heal_path = %heal_path,
            state = "cancel_received",
            "Heal cancel received"
        );

        let request_id = if client_token.is_empty() {
            heal_path.clone()
        } else {
            client_token.clone()
        };

        let cancel_result = if client_token.is_empty() {
            self.heal_manager.cancel_tasks_for_path(&heal_path).await.map(|_| ())
        } else {
            self.heal_manager.cancel_task(&client_token).await
        };

        let response = match cancel_result {
            Ok(()) => HealChannelResponse {
                request_id,
                success: true,
                data: Some("stopped".as_bytes().to_vec()),
                error: None,
            },
            Err(Error::TaskNotFound { .. }) if client_token.is_empty() => HealChannelResponse {
                request_id,
                success: true,
                data: Some("stopped".as_bytes().to_vec()),
                error: None,
            },
            Err(err) => HealChannelResponse {
                request_id,
                success: false,
                data: None,
                error: Some(err.to_string()),
            },
        };

        let _ = response_tx.send(Ok(response.clone()));
        self.publish_response(response);

        Ok(())
    }

    /// Convert channel request to heal request
    fn convert_to_heal_request(&self, request: HealChannelRequest) -> Result<HealRequest> {
        let recursive = request.recursive.unwrap_or(false);
        let heal_type = if let Some(disk_id) = &request.disk {
            let set_disk_id = utils::normalize_set_disk_id(disk_id).ok_or_else(|| Error::InvalidHealType {
                heal_type: format!("erasure-set({disk_id})"),
            })?;
            HealType::ErasureSet {
                buckets: vec![],
                set_disk_id,
            }
        } else if request.bucket.is_empty() {
            HealType::Cluster
        } else if let Some(prefix) = &request.object_prefix {
            if !prefix.is_empty() {
                if recursive {
                    HealType::Prefix {
                        bucket: request.bucket.clone(),
                        prefix: prefix.clone(),
                    }
                } else {
                    HealType::Object {
                        bucket: request.bucket.clone(),
                        object: prefix.clone(),
                        version_id: request.object_version_id.clone(),
                    }
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
        let options = HealOptions {
            scan_mode: request.scan_mode.unwrap_or(HealScanMode::Normal),
            remove_corrupted: request.remove_corrupted.unwrap_or(false),
            recreate_missing: request.recreate_missing.unwrap_or(true),
            update_parity: request.update_parity.unwrap_or(true),
            recursive,
            dry_run: request.dry_run.unwrap_or(false),
            timeout: request.timeout_seconds.map(std::time::Duration::from_secs),
            pool_index: request.pool_index,
            set_index: request.set_index,
        };

        let mut heal_request = HealRequest::new(heal_type, options, priority);
        heal_request.id = request.id;
        heal_request.source = request.source;
        // force_start controls admission/queue semantics only. Do not reinterpret it as
        // destructive heal options: admin clients commonly pass forceStart=true together
        // with remove=false, and turning that into remove_corrupted=true can delete the
        // remaining healthy bucket volumes before object shards are rebuilt.
        heal_request.force_start = request.force_start;
        Ok(heal_request)
    }

    fn publish_response(&self, response: HealChannelResponse) {
        // Try to send to local channel first, but don't block broadcast on failure
        if let Err(e) = self.response_sender.send(response.clone()) {
            error!(
                target: "rustfs::heal::channel",
                event = EVENT_HEAL_CHANNEL_RESPONSE,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_CHANNEL,
                request_id = %response.request_id,
                state = "enqueue_local_failed",
                error = %e,
                "Heal response local enqueue failed"
            );
        }
        // Always attempt to broadcast, even if local send failed
        // Use the original response for broadcast; local send uses a clone
        if let Err(e) = publish_heal_response(response) {
            error!(
                target: "rustfs::heal::channel",
                event = EVENT_HEAL_CHANNEL_RESPONSE,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_CHANNEL,
                state = "broadcast_failed",
                error = %e,
                "Heal response broadcast failed"
            );
        }
    }

    /// Get response sender for external use
    pub fn get_response_sender(&self) -> mpsc::UnboundedSender<HealChannelResponse> {
        self.response_sender.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::super::{DiskStore, Endpoint};
    use super::*;
    use crate::heal::manager::HealConfig;
    use crate::heal::storage::{HealObjectInfo, HealStorageAPI};
    use rustfs_common::heal_channel::{
        HealAdmissionResult, HealChannelPriority, HealChannelRequest, HealRequestSource, HealScanMode,
    };
    use std::sync::Arc;

    // Mock storage for testing
    struct MockStorage;
    #[async_trait::async_trait]
    impl HealStorageAPI for MockStorage {
        async fn get_object_meta(&self, _bucket: &str, _object: &str) -> crate::Result<Option<HealObjectInfo>> {
            Ok(None)
        }
        async fn get_object_data(&self, _bucket: &str, _object: &str) -> crate::Result<Option<Vec<u8>>> {
            Ok(None)
        }
        async fn put_object_data(&self, _bucket: &str, _object: &str, _data: &[u8]) -> crate::Result<()> {
            Ok(())
        }
        async fn delete_object(&self, _bucket: &str, _object: &str) -> crate::Result<()> {
            Ok(())
        }
        async fn verify_object_integrity(&self, _bucket: &str, _object: &str) -> crate::Result<bool> {
            Ok(true)
        }
        async fn ec_decode_rebuild(&self, _bucket: &str, _object: &str) -> crate::Result<Vec<u8>> {
            Ok(vec![])
        }
        async fn get_disk_status(&self, _endpoint: &Endpoint) -> crate::Result<crate::heal::storage::DiskStatus> {
            Ok(crate::heal::storage::DiskStatus::Ok)
        }
        async fn format_disk(&self, _endpoint: &Endpoint) -> crate::Result<()> {
            Ok(())
        }
        async fn get_bucket_info(&self, _bucket: &str) -> crate::Result<Option<crate::heal::storage_api::BucketInfo>> {
            Ok(None)
        }
        async fn heal_bucket_metadata(&self, _bucket: &str) -> crate::Result<()> {
            Ok(())
        }
        async fn list_buckets(&self) -> crate::Result<Vec<crate::heal::storage_api::BucketInfo>> {
            Ok(vec![])
        }
        async fn object_exists(&self, _bucket: &str, _object: &str) -> crate::Result<bool> {
            Ok(false)
        }
        async fn get_object_size(&self, _bucket: &str, _object: &str) -> crate::Result<Option<u64>> {
            Ok(None)
        }
        async fn get_object_checksum(&self, _bucket: &str, _object: &str) -> crate::Result<Option<String>> {
            Ok(None)
        }
        async fn heal_object(
            &self,
            _bucket: &str,
            _object: &str,
            _version_id: Option<&str>,
            _opts: &rustfs_common::heal_channel::HealOpts,
        ) -> crate::Result<(rustfs_madmin::heal_commands::HealResultItem, Option<crate::Error>)> {
            Ok((rustfs_madmin::heal_commands::HealResultItem::default(), None))
        }
        async fn heal_bucket(
            &self,
            _bucket: &str,
            _opts: &rustfs_common::heal_channel::HealOpts,
        ) -> crate::Result<rustfs_madmin::heal_commands::HealResultItem> {
            Ok(rustfs_madmin::heal_commands::HealResultItem::default())
        }
        async fn heal_format(
            &self,
            _dry_run: bool,
        ) -> crate::Result<(rustfs_madmin::heal_commands::HealResultItem, Option<crate::Error>)> {
            Ok((rustfs_madmin::heal_commands::HealResultItem::default(), None))
        }
        async fn list_objects_for_heal(&self, _bucket: &str, _prefix: &str) -> crate::Result<Vec<String>> {
            Ok(vec![])
        }
        async fn list_objects_for_heal_page(
            &self,
            _bucket: &str,
            _prefix: &str,
            _continuation_token: Option<&str>,
        ) -> crate::Result<(Vec<String>, Option<String>, bool)> {
            Ok((vec![], None, false))
        }
        async fn get_disk_for_resume(&self, _set_disk_id: &str) -> crate::Result<DiskStore> {
            Err(crate::Error::other("Not implemented in mock"))
        }
    }

    fn create_test_heal_manager() -> Arc<HealManager> {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        Arc::new(HealManager::new(storage, None))
    }

    #[test]
    fn test_heal_channel_processor_new() {
        let heal_manager = create_test_heal_manager();
        let processor = HealChannelProcessor::new(heal_manager);

        // Verify processor is created successfully
        let _sender = processor.get_response_sender();
        // If we can get the sender, processor was created correctly
    }

    #[tokio::test]
    async fn test_convert_to_heal_request_bucket() {
        let heal_manager = create_test_heal_manager();
        let processor = HealChannelProcessor::new(heal_manager);

        let channel_request = HealChannelRequest {
            id: "test-id".to_string(),
            bucket: "test-bucket".to_string(),
            object_prefix: None,
            object_version_id: None,
            disk: None,
            priority: HealChannelPriority::Normal,
            scan_mode: None,
            remove_corrupted: None,
            recreate_missing: None,
            update_parity: None,
            recursive: None,
            dry_run: None,
            timeout_seconds: None,
            pool_index: None,
            set_index: None,
            force_start: false,
            source: HealRequestSource::Internal,
        };

        let heal_request = processor.convert_to_heal_request(channel_request).unwrap();
        assert_eq!(heal_request.id, "test-id");
        assert!(matches!(heal_request.heal_type, HealType::Bucket { .. }));
        assert_eq!(heal_request.priority, HealPriority::Normal);
    }

    #[tokio::test]
    async fn test_convert_to_heal_request_cluster() {
        let heal_manager = create_test_heal_manager();
        let processor = HealChannelProcessor::new(heal_manager);

        let channel_request = HealChannelRequest {
            id: "test-id".to_string(),
            bucket: String::new(),
            object_prefix: None,
            object_version_id: None,
            disk: None,
            priority: HealChannelPriority::High,
            scan_mode: Some(HealScanMode::Normal),
            remove_corrupted: Some(false),
            recreate_missing: Some(true),
            update_parity: Some(true),
            recursive: Some(true),
            dry_run: Some(false),
            timeout_seconds: None,
            pool_index: None,
            set_index: None,
            force_start: false,
            source: HealRequestSource::Admin,
        };

        let heal_request = processor.convert_to_heal_request(channel_request).unwrap();

        assert!(matches!(heal_request.heal_type, HealType::Cluster));
        assert!(heal_request.options.recursive);
    }

    #[tokio::test]
    async fn test_convert_to_heal_request_object() {
        let heal_manager = create_test_heal_manager();
        let processor = HealChannelProcessor::new(heal_manager);

        let channel_request = HealChannelRequest {
            id: "test-id".to_string(),
            bucket: "test-bucket".to_string(),
            object_prefix: Some("test-object".to_string()),
            object_version_id: None,
            disk: None,
            priority: HealChannelPriority::High,
            scan_mode: Some(HealScanMode::Deep),
            remove_corrupted: Some(true),
            recreate_missing: Some(true),
            update_parity: Some(true),
            recursive: Some(false),
            dry_run: Some(false),
            timeout_seconds: Some(300),
            pool_index: Some(0),
            set_index: Some(1),
            force_start: false,
            source: HealRequestSource::Scanner,
        };

        let heal_request = processor.convert_to_heal_request(channel_request).unwrap();
        assert!(matches!(heal_request.heal_type, HealType::Object { .. }));
        assert_eq!(heal_request.priority, HealPriority::High);
        assert_eq!(heal_request.source, HealRequestSource::Scanner);
        assert_eq!(heal_request.options.scan_mode, HealScanMode::Deep);
        assert!(heal_request.options.remove_corrupted);
        assert!(heal_request.options.recreate_missing);
    }

    #[tokio::test]
    async fn test_convert_to_heal_request_prefix_when_recursive() {
        let heal_manager = create_test_heal_manager();
        let processor = HealChannelProcessor::new(heal_manager);

        let channel_request = HealChannelRequest {
            id: "test-id".to_string(),
            bucket: "test-bucket".to_string(),
            object_prefix: Some("logs/".to_string()),
            object_version_id: None,
            disk: None,
            priority: HealChannelPriority::High,
            scan_mode: Some(HealScanMode::Normal),
            remove_corrupted: Some(false),
            recreate_missing: Some(true),
            update_parity: Some(true),
            recursive: Some(true),
            dry_run: Some(false),
            timeout_seconds: None,
            pool_index: None,
            set_index: None,
            force_start: false,
            source: HealRequestSource::Admin,
        };

        let heal_request = processor.convert_to_heal_request(channel_request).unwrap();

        assert!(matches!(
            heal_request.heal_type,
            HealType::Prefix { ref bucket, ref prefix } if bucket == "test-bucket" && prefix == "logs/"
        ));
        assert!(heal_request.options.recursive);
    }

    #[tokio::test]
    async fn test_convert_to_heal_request_erasure_set() {
        let heal_manager = create_test_heal_manager();
        let processor = HealChannelProcessor::new(heal_manager);

        let channel_request = HealChannelRequest {
            id: "test-id".to_string(),
            bucket: "test-bucket".to_string(),
            object_prefix: None,
            object_version_id: None,
            disk: Some("pool_0_set_1".to_string()),
            priority: HealChannelPriority::Critical,
            scan_mode: None,
            remove_corrupted: None,
            recreate_missing: None,
            update_parity: None,
            recursive: None,
            dry_run: None,
            timeout_seconds: None,
            pool_index: None,
            set_index: None,
            force_start: false,
            source: HealRequestSource::Internal,
        };

        let heal_request = processor.convert_to_heal_request(channel_request).unwrap();
        assert!(matches!(heal_request.heal_type, HealType::ErasureSet { .. }));
        assert_eq!(heal_request.priority, HealPriority::Urgent);
    }

    #[tokio::test]
    async fn test_convert_to_heal_request_invalid_disk_id() {
        let heal_manager = create_test_heal_manager();
        let processor = HealChannelProcessor::new(heal_manager);

        let channel_request = HealChannelRequest {
            id: "test-id".to_string(),
            bucket: "test-bucket".to_string(),
            object_prefix: None,
            object_version_id: None,
            disk: Some("invalid-disk-id".to_string()),
            priority: HealChannelPriority::Normal,
            scan_mode: None,
            remove_corrupted: None,
            recreate_missing: None,
            update_parity: None,
            recursive: None,
            dry_run: None,
            timeout_seconds: None,
            pool_index: None,
            set_index: None,
            force_start: false,
            source: HealRequestSource::Internal,
        };

        let result = processor.convert_to_heal_request(channel_request);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_convert_to_heal_request_priority_mapping() {
        let heal_manager = create_test_heal_manager();
        let processor = HealChannelProcessor::new(heal_manager);

        let priorities = vec![
            (HealChannelPriority::Low, HealPriority::Low),
            (HealChannelPriority::Normal, HealPriority::Normal),
            (HealChannelPriority::High, HealPriority::High),
            (HealChannelPriority::Critical, HealPriority::Urgent),
        ];

        for (channel_priority, expected_heal_priority) in priorities {
            let channel_request = HealChannelRequest {
                id: "test-id".to_string(),
                bucket: "test-bucket".to_string(),
                object_prefix: None,
                object_version_id: None,
                disk: None,
                priority: channel_priority,
                scan_mode: None,
                remove_corrupted: None,
                recreate_missing: None,
                update_parity: None,
                recursive: None,
                dry_run: None,
                timeout_seconds: None,
                pool_index: None,
                set_index: None,
                force_start: false,
                source: HealRequestSource::Internal,
            };

            let heal_request = processor.convert_to_heal_request(channel_request).unwrap();
            assert_eq!(heal_request.priority, expected_heal_priority);
        }
    }

    #[tokio::test]
    async fn test_convert_to_heal_request_force_start() {
        let heal_manager = create_test_heal_manager();
        let processor = HealChannelProcessor::new(heal_manager);

        let channel_request = HealChannelRequest {
            id: "test-id".to_string(),
            bucket: "test-bucket".to_string(),
            object_prefix: None,
            object_version_id: None,
            disk: None,
            priority: HealChannelPriority::Normal,
            scan_mode: None,
            remove_corrupted: Some(false),
            recreate_missing: Some(false),
            update_parity: Some(false),
            recursive: None,
            dry_run: None,
            timeout_seconds: None,
            pool_index: None,
            set_index: None,
            force_start: true, // Admission force only; must not override explicit heal options.
            source: HealRequestSource::Internal,
        };

        let heal_request = processor.convert_to_heal_request(channel_request).unwrap();
        assert!(heal_request.force_start);
        assert!(!heal_request.options.remove_corrupted);
        assert!(!heal_request.options.recreate_missing);
        assert!(!heal_request.options.update_parity);
    }

    #[tokio::test]
    async fn test_convert_to_heal_request_empty_object_prefix() {
        let heal_manager = create_test_heal_manager();
        let processor = HealChannelProcessor::new(heal_manager);

        let channel_request = HealChannelRequest {
            id: "test-id".to_string(),
            bucket: "test-bucket".to_string(),
            object_prefix: Some("".to_string()), // Empty prefix should be treated as bucket heal
            object_version_id: None,
            disk: None,
            priority: HealChannelPriority::Normal,
            scan_mode: None,
            remove_corrupted: None,
            recreate_missing: None,
            update_parity: None,
            recursive: None,
            dry_run: None,
            timeout_seconds: None,
            pool_index: None,
            set_index: None,
            force_start: false,
            source: HealRequestSource::Internal,
        };

        let heal_request = processor.convert_to_heal_request(channel_request).unwrap();
        assert!(matches!(heal_request.heal_type, HealType::Bucket { .. }));
    }

    #[tokio::test]
    async fn test_process_start_request_returns_admission_result() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = Arc::new(HealManager::new(
            storage,
            Some(HealConfig {
                queue_size: 1,
                ..HealConfig::default()
            }),
        ));
        let processor = HealChannelProcessor::new(manager);

        let request = HealChannelRequest {
            id: "admission-id".to_string(),
            bucket: "bucket".to_string(),
            object_prefix: Some("object".to_string()),
            object_version_id: None,
            disk: None,
            priority: HealChannelPriority::Low,
            scan_mode: Some(HealScanMode::Normal),
            remove_corrupted: None,
            recreate_missing: None,
            update_parity: None,
            recursive: None,
            dry_run: None,
            timeout_seconds: None,
            pool_index: None,
            set_index: None,
            force_start: false,
            source: HealRequestSource::Internal,
        };

        let (tx, rx) = oneshot::channel();
        processor
            .process_start_request(request.clone(), tx)
            .await
            .expect("first admission should succeed");
        assert_eq!(
            rx.await
                .expect("oneshot should resolve")
                .expect("admission should be returned"),
            HealAdmissionResult::Accepted
        );

        let (tx, rx) = oneshot::channel();
        processor
            .process_start_request(request, tx)
            .await
            .expect("duplicate admission should succeed");
        assert_eq!(
            rx.await
                .expect("oneshot should resolve")
                .expect("admission should be returned"),
            HealAdmissionResult::Merged
        );
    }

    #[tokio::test]
    async fn test_process_start_request_returns_error_on_invalid_request() {
        let heal_manager = create_test_heal_manager();
        let processor = HealChannelProcessor::new(heal_manager);

        let request = HealChannelRequest {
            id: "invalid-id".to_string(),
            bucket: "bucket".to_string(),
            object_prefix: None,
            object_version_id: None,
            disk: Some("invalid".to_string()),
            priority: HealChannelPriority::Normal,
            scan_mode: None,
            remove_corrupted: None,
            recreate_missing: None,
            update_parity: None,
            recursive: None,
            dry_run: None,
            timeout_seconds: None,
            pool_index: None,
            set_index: None,
            force_start: false,
            source: HealRequestSource::Internal,
        };

        let (tx, rx) = oneshot::channel();
        processor
            .process_start_request(request, tx)
            .await
            .expect("processor should surface invalid request through response channel");
        assert!(rx.await.expect("oneshot should resolve").is_err());
    }

    #[tokio::test]
    async fn test_process_query_request_reports_not_found_when_task_is_unknown() {
        let heal_manager = create_test_heal_manager();
        let processor = HealChannelProcessor::new(heal_manager);
        let (tx, rx) = oneshot::channel();

        processor
            .process_query_request("bucket".to_string(), "completed-token".to_string(), tx)
            .await
            .expect("query should process");

        let response = rx
            .await
            .expect("oneshot should resolve")
            .expect("query response should be returned");
        assert!(response.success);
        assert_eq!(response.request_id, "completed-token");
        assert_eq!(response.error.as_deref(), Some("heal task not found or expired"));
        let payload: serde_json::Value =
            serde_json::from_slice(response.data.as_deref().expect("status payload should be present"))
                .expect("status payload should be json");
        assert_eq!(payload["summary"], "notFound");
        assert_eq!(payload["items"].as_array().expect("items should be an array").len(), 0);
    }

    #[tokio::test]
    async fn test_process_query_request_reports_running_for_queued_task() {
        let heal_manager = create_test_heal_manager();
        let request = HealRequest::bucket("bucket".to_string());
        let task_id = request.id.clone();
        assert_eq!(
            heal_manager
                .submit_heal_request(request)
                .await
                .expect("request should be accepted"),
            HealAdmissionResult::Accepted
        );

        let processor = HealChannelProcessor::new(heal_manager);
        let (tx, rx) = oneshot::channel();

        processor
            .process_query_request("bucket".to_string(), task_id.clone(), tx)
            .await
            .expect("query should process");

        let response = rx
            .await
            .expect("oneshot should resolve")
            .expect("query response should be returned");
        assert!(response.success);
        assert_eq!(response.request_id, task_id);
        let payload: serde_json::Value =
            serde_json::from_slice(response.data.as_deref().expect("status payload should be present"))
                .expect("status payload should be json");
        assert_eq!(payload["summary"], "running");
        assert_eq!(payload["items"].as_array().expect("items should be an array").len(), 0);
    }

    #[tokio::test]
    async fn test_process_query_request_rejects_wrong_token_for_active_path() {
        let heal_manager = create_test_heal_manager();
        let request = HealRequest::bucket("bucket".to_string());
        assert_eq!(
            heal_manager
                .submit_heal_request(request)
                .await
                .expect("request should be accepted"),
            HealAdmissionResult::Accepted
        );

        let processor = HealChannelProcessor::new(heal_manager);
        let (tx, rx) = oneshot::channel();

        processor
            .process_query_request("bucket".to_string(), "wrong-token".to_string(), tx)
            .await
            .expect("query should process");

        let response = rx
            .await
            .expect("oneshot should resolve")
            .expect("query response should be returned");
        assert!(!response.success);
        assert_eq!(response.request_id, "wrong-token");
        assert_eq!(response.error.as_deref(), Some("invalid heal client token"));
    }

    #[tokio::test]
    async fn test_process_query_request_empty_path_ignores_unrelated_tasks() {
        let heal_manager = create_test_heal_manager();
        heal_manager
            .submit_heal_request(HealRequest::bucket("bucket".to_string()))
            .await
            .expect("request should be accepted");

        let processor = HealChannelProcessor::new(heal_manager);
        let (tx, rx) = oneshot::channel();

        processor
            .process_query_request(String::new(), "wrong-token".to_string(), tx)
            .await
            .expect("query should process");

        let response = rx
            .await
            .expect("oneshot should resolve")
            .expect("query response should be returned");
        assert!(response.success);
        let payload: serde_json::Value =
            serde_json::from_slice(response.data.as_deref().expect("status payload should be present"))
                .expect("status payload should be json");
        assert_eq!(payload["summary"], "notFound");
        assert_eq!(payload["items"].as_array().expect("items should be an array").len(), 0);
    }

    #[tokio::test]
    async fn test_process_query_request_empty_path_uses_client_token_directly() {
        let heal_manager = create_test_heal_manager();
        let request = HealRequest::new(
            HealType::ErasureSet {
                buckets: vec![],
                set_disk_id: "pool_0_set_1".to_string(),
            },
            HealOptions::default(),
            HealPriority::High,
        );
        let task_id = request.id.clone();
        heal_manager
            .submit_heal_request(request)
            .await
            .expect("request should be accepted");

        let processor = HealChannelProcessor::new(heal_manager);
        let (tx, rx) = oneshot::channel();

        processor
            .process_query_request(String::new(), task_id.clone(), tx)
            .await
            .expect("query should process");

        let response = rx
            .await
            .expect("oneshot should resolve")
            .expect("query response should be returned");
        assert!(response.success);
        assert_eq!(response.request_id, task_id);
        assert!(response.error.is_none());
        let payload: serde_json::Value =
            serde_json::from_slice(response.data.as_deref().expect("status payload should be present"))
                .expect("status payload should be json");
        assert_eq!(payload["summary"], "running");
        assert_eq!(payload["items"].as_array().expect("items should be an array").len(), 0);
    }

    #[tokio::test]
    async fn test_process_cancel_request_cancels_queued_task_by_token() {
        let heal_manager = create_test_heal_manager();
        let request = HealRequest::bucket("bucket".to_string());
        let task_id = request.id.clone();
        heal_manager
            .submit_heal_request(request)
            .await
            .expect("request should be accepted");

        let processor = HealChannelProcessor::new(heal_manager.clone());
        let (tx, rx) = oneshot::channel();

        processor
            .process_cancel_request("bucket".to_string(), task_id.clone(), tx)
            .await
            .expect("cancel should process");

        let response = rx
            .await
            .expect("oneshot should resolve")
            .expect("cancel response should be returned");
        assert!(response.success);
        assert_eq!(response.request_id, task_id);
        assert_eq!(response.data.as_deref(), Some("stopped".as_bytes()));
        assert!(matches!(
            heal_manager.get_task_status(&response.request_id).await,
            Err(crate::Error::TaskNotFound { .. })
        ));
    }

    #[tokio::test]
    async fn test_process_cancel_request_cancels_queued_task_by_path() {
        let heal_manager = create_test_heal_manager();
        let request = HealRequest::bucket("bucket".to_string());
        let task_id = request.id.clone();
        heal_manager
            .submit_heal_request(request)
            .await
            .expect("request should be accepted");

        let processor = HealChannelProcessor::new(heal_manager.clone());
        let (tx, rx) = oneshot::channel();

        processor
            .process_cancel_request("bucket".to_string(), String::new(), tx)
            .await
            .expect("cancel should process");

        let response = rx
            .await
            .expect("oneshot should resolve")
            .expect("cancel response should be returned");
        assert!(response.success);
        assert_eq!(response.request_id, "bucket");
        assert_eq!(response.data.as_deref(), Some("stopped".as_bytes()));
        assert!(matches!(
            heal_manager.get_task_status(&task_id).await,
            Err(crate::Error::TaskNotFound { .. })
        ));
    }

    #[tokio::test]
    async fn test_process_cancel_request_treats_unknown_path_as_stopped() {
        let heal_manager = create_test_heal_manager();
        let processor = HealChannelProcessor::new(heal_manager);
        let (tx, rx) = oneshot::channel();

        processor
            .process_cancel_request(".".to_string(), String::new(), tx)
            .await
            .expect("cancel should process");

        let response = rx
            .await
            .expect("oneshot should resolve")
            .expect("cancel response should be returned");
        assert!(response.success);
        assert_eq!(response.request_id, ".");
        assert_eq!(response.data.as_deref(), Some("stopped".as_bytes()));
        assert!(response.error.is_none());
    }

    #[tokio::test]
    async fn test_process_cancel_request_reports_unknown_task() {
        let heal_manager = create_test_heal_manager();
        let processor = HealChannelProcessor::new(heal_manager);
        let (tx, rx) = oneshot::channel();

        processor
            .process_cancel_request("missing".to_string(), "missing-token".to_string(), tx)
            .await
            .expect("cancel should process");

        let response = rx
            .await
            .expect("oneshot should resolve")
            .expect("cancel response should be returned");
        assert!(!response.success);
        assert_eq!(response.request_id, "missing-token");
        assert!(response.error.unwrap_or_default().contains("Heal task not found"));
    }
}
