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
    manager::HealManager,
    task::{HealOptions, HealPriority, HealRequest, HealType},
    utils,
};
use crate::{Error, Result};
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
        // Use the original response for broadcast; local send uses a clone
        if let Err(e) = publish_heal_response(response) {
            error!("Failed to broadcast heal response: {}", e);
        }
    }

    /// Get response sender for external use
    pub fn get_response_sender(&self) -> mpsc::UnboundedSender<HealChannelResponse> {
        self.response_sender.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::heal::storage::HealStorageAPI;
    use rustfs_common::heal_channel::{HealChannelPriority, HealChannelRequest, HealScanMode};
    use std::sync::Arc;

    // Mock storage for testing
    struct MockStorage;
    #[async_trait::async_trait]
    impl HealStorageAPI for MockStorage {
        async fn get_object_meta(
            &self,
            _bucket: &str,
            _object: &str,
        ) -> crate::Result<Option<rustfs_ecstore::store_api::ObjectInfo>> {
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
        async fn get_disk_status(
            &self,
            _endpoint: &rustfs_ecstore::disk::endpoint::Endpoint,
        ) -> crate::Result<crate::heal::storage::DiskStatus> {
            Ok(crate::heal::storage::DiskStatus::Ok)
        }
        async fn format_disk(&self, _endpoint: &rustfs_ecstore::disk::endpoint::Endpoint) -> crate::Result<()> {
            Ok(())
        }
        async fn get_bucket_info(&self, _bucket: &str) -> crate::Result<Option<rustfs_ecstore::store_api::BucketInfo>> {
            Ok(None)
        }
        async fn heal_bucket_metadata(&self, _bucket: &str) -> crate::Result<()> {
            Ok(())
        }
        async fn list_buckets(&self) -> crate::Result<Vec<rustfs_ecstore::store_api::BucketInfo>> {
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
        async fn get_disk_for_resume(&self, _set_disk_id: &str) -> crate::Result<rustfs_ecstore::disk::DiskStore> {
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
        };

        let heal_request = processor.convert_to_heal_request(channel_request).unwrap();
        assert!(matches!(heal_request.heal_type, HealType::Bucket { .. }));
        assert_eq!(heal_request.priority, HealPriority::Normal);
    }

    #[tokio::test]
    async fn test_convert_to_heal_request_object() {
        let heal_manager = create_test_heal_manager();
        let processor = HealChannelProcessor::new(heal_manager);

        let channel_request = HealChannelRequest {
            id: "test-id".to_string(),
            bucket: "test-bucket".to_string(),
            object_prefix: Some("test-object".to_string()),
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
        };

        let heal_request = processor.convert_to_heal_request(channel_request).unwrap();
        assert!(matches!(heal_request.heal_type, HealType::Object { .. }));
        assert_eq!(heal_request.priority, HealPriority::High);
        assert_eq!(heal_request.options.scan_mode, HealScanMode::Deep);
        assert!(heal_request.options.remove_corrupted);
        assert!(heal_request.options.recreate_missing);
    }

    #[tokio::test]
    async fn test_convert_to_heal_request_erasure_set() {
        let heal_manager = create_test_heal_manager();
        let processor = HealChannelProcessor::new(heal_manager);

        let channel_request = HealChannelRequest {
            id: "test-id".to_string(),
            bucket: "test-bucket".to_string(),
            object_prefix: None,
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
            force_start: true, // Should override the above false values
        };

        let heal_request = processor.convert_to_heal_request(channel_request).unwrap();
        assert!(heal_request.options.remove_corrupted);
        assert!(heal_request.options.recreate_missing);
        assert!(heal_request.options.update_parity);
    }

    #[tokio::test]
    async fn test_convert_to_heal_request_empty_object_prefix() {
        let heal_manager = create_test_heal_manager();
        let processor = HealChannelProcessor::new(heal_manager);

        let channel_request = HealChannelRequest {
            id: "test-id".to_string(),
            bucket: "test-bucket".to_string(),
            object_prefix: Some("".to_string()), // Empty prefix should be treated as bucket heal
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
        };

        let heal_request = processor.convert_to_heal_request(channel_request).unwrap();
        assert!(matches!(heal_request.heal_type, HealType::Bucket { .. }));
    }
}
