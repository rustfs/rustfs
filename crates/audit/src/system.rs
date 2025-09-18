//  Copyright 2024 RustFS Team
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

//! High-performance audit system with batching and concurrent processing

use crate::config::{AuditConfig, PerformanceConfig};
use crate::entity::AuditEntry;
use crate::error::{AuditError, AuditResult};
use crate::registry::TargetRegistry;
use rustfs_targets::target::EntityTarget;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{interval, timeout};
use tracing::{debug, error, info, warn};

/// High-performance audit system with batching and concurrent processing
pub struct AuditSystem {
    registry: Arc<TargetRegistry>,
    config: PerformanceConfig,
    sender: Option<mpsc::UnboundedSender<Arc<EntityTarget<AuditEntry>>>>,
    _handle: Option<tokio::task::JoinHandle<()>>,
}

impl AuditSystem {
    /// Create a new audit system
    pub fn new(registry: Arc<TargetRegistry>, config: PerformanceConfig) -> Self {
        Self {
            registry,
            config,
            sender: None,
            _handle: None,
        }
    }

    /// Start the audit system with background processing
    pub async fn start(&mut self) -> AuditResult<()> {
        info!("Starting audit system");

        let (sender, mut receiver) = mpsc::unbounded_channel::<Arc<EntityTarget<AuditEntry>>>();
        let registry = self.registry.clone();
        let config = self.config.clone();

        // Start the registry
        registry.start().await?;

        // Spawn background processor
        let handle = tokio::spawn(async move {
            let mut batch = Vec::with_capacity(config.batch_size);
            let mut batch_timer = interval(Duration::from_millis(config.batch_timeout_ms));
            batch_timer.tick().await; // Skip first immediate tick

            loop {
                tokio::select! {
                    // Receive new audit entries
                    event = receiver.recv() => {
                        match event {
                            Some(event) => {
                                batch.push(event);
                                
                                // Process batch if it's full
                                if batch.len() >= config.batch_size {
                                    Self::process_batch(&registry, &mut batch, &config).await;
                                }
                            }
                            None => {
                                // Channel closed, process remaining batch and exit
                                if !batch.is_empty() {
                                    Self::process_batch(&registry, &mut batch, &config).await;
                                }
                                break;
                            }
                        }
                    }
                    
                    // Process batch on timeout
                    _ = batch_timer.tick() => {
                        if !batch.is_empty() {
                            Self::process_batch(&registry, &mut batch, &config).await;
                        }
                    }
                }
            }

            info!("Audit system background processor stopped");
        });

        self.sender = Some(sender);
        self._handle = Some(handle);

        info!("Audit system started successfully");
        Ok(())
    }

    /// Stop the audit system
    pub async fn stop(&mut self) -> AuditResult<()> {
        info!("Stopping audit system");

        // Close the sender to signal shutdown
        if let Some(sender) = self.sender.take() {
            drop(sender);
        }

        // Wait for background processor to finish
        if let Some(handle) = self._handle.take() {
            if let Err(e) = handle.await {
                error!(error = %e, "Error waiting for audit system shutdown");
            }
        }

        // Stop the registry
        self.registry.close().await?;

        info!("Audit system stopped");
        Ok(())
    }

    /// Log an audit entry (async, non-blocking)
    pub async fn log(&self, event: Arc<EntityTarget<AuditEntry>>) -> AuditResult<()> {
        if let Some(ref sender) = self.sender {
            sender.send(event).map_err(|_| {
                AuditError::RuntimeError("Audit system is not running".to_string())
            })?;
            debug!("Audit entry queued for processing");
            Ok(())
        } else {
            Err(AuditError::SystemNotInitialized)
        }
    }

    /// Process a batch of audit entries
    async fn process_batch(
        registry: &Arc<TargetRegistry>,
        batch: &mut Vec<Arc<EntityTarget<AuditEntry>>>,
        config: &PerformanceConfig,
    ) {
        if batch.is_empty() {
            return;
        }

        debug!(batch_size = batch.len(), "Processing audit batch");

        // Process each event in the batch concurrently with semaphore control
        let semaphore = Arc::new(tokio::sync::Semaphore::new(config.max_concurrent));
        let mut tasks = Vec::new();

        for event in batch.drain(..) {
            let registry_clone = registry.clone();
            let semaphore_clone = semaphore.clone();
            
            let task = tokio::spawn(async move {
                let _permit = semaphore_clone.acquire().await.unwrap();
                
                // Apply timeout to dispatch operation
                let dispatch_result = timeout(
                    Duration::from_millis(5000), // 5 second timeout
                    registry_clone.dispatch(event)
                ).await;

                match dispatch_result {
                    Ok(Ok(())) => {
                        debug!("Successfully dispatched audit event");
                    }
                    Ok(Err(e)) => {
                        warn!(error = %e, "Failed to dispatch audit event");
                    }
                    Err(_) => {
                        error!("Audit event dispatch timed out");
                    }
                }
            });
            
            tasks.push(task);
        }

        // Wait for all tasks to complete
        for task in tasks {
            if let Err(e) = task.await {
                error!(error = %e, "Task execution error in audit batch processing");
            }
        }

        debug!("Completed processing audit batch");
    }

    /// Get the target registry
    pub fn registry(&self) -> &Arc<TargetRegistry> {
        &self.registry
    }

    /// Check if the system is running
    pub fn is_running(&self) -> bool {
        self.sender.is_some()
    }

    /// Pause audit processing
    pub fn pause(&self) -> AuditResult<()> {
        self.registry.pause()
    }

    /// Resume audit processing
    pub fn resume(&self) -> AuditResult<()> {
        self.registry.resume()
    }
}

impl Drop for AuditSystem {
    fn drop(&mut self) {
        // Ensure cleanup on drop
        if let Some(sender) = self.sender.take() {
            drop(sender);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PerformanceConfig;
    use crate::registry::{DefaultAuditTargetFactory, TargetRegistry};
    use crate::entity::s3_events;
    use rustfs_targets::EventName;
    use serde_json::json;

    #[tokio::test]
    async fn test_audit_system_lifecycle() {
        let factory = Arc::new(DefaultAuditTargetFactory::new());
        let registry = Arc::new(TargetRegistry::new(factory));
        let config = PerformanceConfig::default();
        
        let mut system = AuditSystem::new(registry, config);
        
        // Start system
        system.start().await.unwrap();
        assert!(system.is_running());
        
        // Stop system
        system.stop().await.unwrap();
        assert!(!system.is_running());
    }

    #[tokio::test]
    async fn test_audit_system_logging() {
        let factory = Arc::new(DefaultAuditTargetFactory::new());
        let registry = Arc::new(TargetRegistry::new(factory));
        let config = PerformanceConfig {
            batch_size: 2,
            batch_timeout_ms: 50,
            max_concurrent: 5,
            enable_metrics: true,
        };
        
        let mut system = AuditSystem::new(registry, config);
        system.start().await.unwrap();

        // Log some audit entries
        let audit_entry = s3_events::get_object("test-bucket", "test-key");
        let entity_target = Arc::new(EntityTarget {
            object_name: "test-key".to_string(),
            bucket_name: "test-bucket".to_string(),
            event_name: EventName::ObjectAccessedGet,
            data: audit_entry,
        });

        // Should not fail even with no targets
        system.log(entity_target).await.unwrap();

        // Give some time for processing
        tokio::time::sleep(Duration::from_millis(100)).await;

        system.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_audit_system_pause_resume() {
        let factory = Arc::new(DefaultAuditTargetFactory::new());
        let registry = Arc::new(TargetRegistry::new(factory));
        let config = PerformanceConfig::default();
        
        let mut system = AuditSystem::new(registry, config);
        system.start().await.unwrap();

        // Pause and resume
        system.pause().unwrap();
        assert!(system.registry().is_paused());
        
        system.resume().unwrap();
        assert!(!system.registry().is_paused());

        system.stop().await.unwrap();
    }
}