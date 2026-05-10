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

use crate::{AuditEntry, AuditResult, observability, system::AuditTargetMetricSnapshot};
use rustfs_targets::{Target, target::EntityTarget};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

#[derive(Clone)]
pub struct AuditPipeline {
    registry: Arc<Mutex<crate::AuditRegistry>>,
}

impl AuditPipeline {
    pub fn new(registry: Arc<Mutex<crate::AuditRegistry>>) -> Self {
        Self { registry }
    }

    pub async fn dispatch(&self, entry: Arc<AuditEntry>) -> AuditResult<()> {
        let start_time = std::time::Instant::now();

        let targets: Vec<(String, Box<dyn Target<AuditEntry> + Send + Sync>)> = {
            let registry = self.registry.lock().await;
            let target_keys = registry.list_targets();

            if target_keys.is_empty() {
                warn!("No audit targets configured for dispatch");
                return Ok(());
            }

            target_keys
                .into_iter()
                .filter_map(|key| registry.get_target(&key).map(|t| (key, t.clone_dyn())))
                .collect()
        };

        let mut tasks = Vec::new();

        for (target_key, target) in targets {
            let entity_target = EntityTarget {
                object_name: entry.api.name.clone().unwrap_or_default(),
                bucket_name: entry.api.bucket.clone().unwrap_or_default(),
                event_name: entry.event,
                data: (*entry).clone(),
            };

            let task = async move {
                let result = target.save(Arc::new(entity_target)).await;
                (target_key, result)
            };

            tasks.push(task);
        }

        let results = futures::future::join_all(tasks).await;

        let mut errors = Vec::new();
        let mut success_count = 0;

        for (target_key, result) in results {
            match result {
                Ok(_) => {
                    success_count += 1;
                    observability::record_target_success();
                }
                Err(e) => {
                    error!(target_id = %target_key, error = %e, "Failed to dispatch audit log to target");
                    errors.push(e);
                    observability::record_target_failure();
                }
            }
        }

        let dispatch_time = start_time.elapsed();

        if errors.is_empty() {
            observability::record_audit_success(dispatch_time);
        } else {
            observability::record_audit_failure(dispatch_time);
            warn!(
                error_count = errors.len(),
                success_count = success_count,
                "Some audit targets failed to receive log entry"
            );
        }

        Ok(())
    }

    pub async fn dispatch_batch(&self, entries: Vec<Arc<AuditEntry>>) -> AuditResult<()> {
        let start_time = std::time::Instant::now();

        let targets: Vec<(String, Box<dyn Target<AuditEntry> + Send + Sync>)> = {
            let registry = self.registry.lock().await;
            let target_keys = registry.list_targets();

            if target_keys.is_empty() {
                warn!("No audit targets configured for batch dispatch");
                return Ok(());
            }

            target_keys
                .into_iter()
                .filter_map(|key| registry.get_target(&key).map(|t| (key, t.clone_dyn())))
                .collect()
        };

        let mut tasks = Vec::new();
        for (target_key, target) in targets {
            let entries_clone: Vec<_> = entries.iter().map(Arc::clone).collect();
            let target_key_clone = target_key.clone();

            let task = async move {
                let mut success_count = 0;
                let mut errors = Vec::new();
                for entry in entries_clone {
                    let entity_target = EntityTarget {
                        object_name: entry.api.name.clone().unwrap_or_default(),
                        bucket_name: entry.api.bucket.clone().unwrap_or_default(),
                        event_name: entry.event,
                        data: (*entry).clone(),
                    };
                    match target.save(Arc::new(entity_target)).await {
                        Ok(_) => success_count += 1,
                        Err(e) => errors.push(e),
                    }
                }
                (target_key_clone, success_count, errors)
            };
            tasks.push(task);
        }

        let results = futures::future::join_all(tasks).await;
        let mut total_success = 0;
        let mut total_errors = 0;
        for (_target_id, success_count, errors) in results {
            total_success += success_count;
            total_errors += errors.len();
            for e in errors {
                error!("Batch dispatch error: {:?}", e);
            }
        }

        let dispatch_time = start_time.elapsed();
        info!(
            "Batch dispatched {} entries, success: {}, errors: {}, time: {:?}",
            entries.len(),
            total_success,
            total_errors,
            dispatch_time
        );

        Ok(())
    }

    pub async fn snapshot_target_metrics(&self) -> Vec<AuditTargetMetricSnapshot> {
        let registry = self.registry.lock().await;
        registry
            .list_target_values()
            .into_iter()
            .map(|target| {
                let delivery = target.delivery_snapshot();
                AuditTargetMetricSnapshot {
                    failed_messages: delivery.failed_messages,
                    queue_length: delivery.queue_length,
                    target_id: target.id().to_string(),
                    total_messages: delivery.total_messages,
                }
            })
            .collect()
    }

    pub async fn snapshot_target_health(&self) -> Vec<rustfs_targets::RuntimeTargetHealthSnapshot> {
        let registry = self.registry.lock().await;
        registry.runtime_manager().health_snapshots().await
    }
}

#[derive(Clone)]
pub struct AuditRuntimeView {
    registry: Arc<Mutex<crate::AuditRegistry>>,
}

impl AuditRuntimeView {
    pub fn new(registry: Arc<Mutex<crate::AuditRegistry>>) -> Self {
        Self { registry }
    }

    pub async fn list_targets(&self) -> Vec<String> {
        let registry = self.registry.lock().await;
        registry.list_targets()
    }

    pub async fn get_target_values(&self) -> Vec<rustfs_targets::SharedTarget<AuditEntry>> {
        let registry = self.registry.lock().await;
        registry.list_target_values()
    }

    pub async fn get_target(&self, target_id: &str) -> Option<String> {
        let registry = self.registry.lock().await;
        registry.get_target(target_id).map(|target| target.id().to_string())
    }

    pub async fn enable_target(&self, target_id: &str) -> AuditResult<()> {
        let registry = self.registry.lock().await;
        if registry.get_target(target_id).is_some() {
            info!(target_id = %target_id, "Target enabled");
            Ok(())
        } else {
            Err(crate::AuditError::Configuration(format!("Target not found: {target_id}"), None))
        }
    }

    pub async fn disable_target(&self, target_id: &str) -> AuditResult<()> {
        let registry = self.registry.lock().await;
        if registry.get_target(target_id).is_some() {
            info!(target_id = %target_id, "Target disabled");
            Ok(())
        } else {
            Err(crate::AuditError::Configuration(format!("Target not found: {target_id}"), None))
        }
    }

    pub async fn remove_target(&self, target_id: &str) -> AuditResult<()> {
        let mut registry = self.registry.lock().await;
        if registry.remove_target(target_id).await.is_some() {
            info!(target_id = %target_id, "Target removed");
            Ok(())
        } else {
            Err(crate::AuditError::Configuration(format!("Target not found: {target_id}"), None))
        }
    }

    pub async fn upsert_target(&self, target_id: String, target: Box<dyn Target<AuditEntry> + Send + Sync>) -> AuditResult<()> {
        let mut registry = self.registry.lock().await;

        if let Err(err) = target.init().await {
            return Err(crate::AuditError::Target(err));
        }

        let _ = registry.remove_target(&target_id).await;
        registry.add_target(target_id.clone(), target);
        info!(target_id = %target_id, "Target upserted");
        Ok(())
    }
}
