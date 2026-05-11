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

use crate::{
    AuditEntry, AuditResult, observability,
    system::{AuditSystemState, AuditTargetMetricSnapshot},
};
use rustfs_targets::{SharedTarget, Target, target::EntityTarget};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
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

        let targets: Vec<SharedTarget<AuditEntry>> = {
            let registry = self.registry.lock().await;
            let targets = registry.list_target_values();

            if targets.is_empty() {
                warn!("No audit targets configured for dispatch");
                return Ok(());
            }

            targets
        };

        let mut tasks = Vec::new();

        for target in targets {
            let entity_target = EntityTarget {
                object_name: entry.api.name.clone().unwrap_or_default(),
                bucket_name: entry.api.bucket.clone().unwrap_or_default(),
                event_name: entry.event,
                data: (*entry).clone(),
            };

            let task = async move {
                let result = target.save(Arc::new(entity_target)).await;
                (target.id().to_string(), result)
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

        let targets: Vec<SharedTarget<AuditEntry>> = {
            let registry = self.registry.lock().await;
            let targets = registry.list_target_values();

            if targets.is_empty() {
                warn!("No audit targets configured for batch dispatch");
                return Ok(());
            }

            targets
        };

        let mut tasks = Vec::new();
        for target in targets {
            let entries_clone: Vec<_> = entries.iter().map(Arc::clone).collect();

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
                (target.id().to_string(), success_count, errors)
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
        if let Err(err) = target.init().await {
            return Err(crate::AuditError::Target(err));
        }

        let shared_target: SharedTarget<AuditEntry> = Arc::from(target);
        let mut registry = self.registry.lock().await;
        let _ = registry.remove_target(&target_id).await;
        registry.add_shared_target(target_id.clone(), shared_target);
        info!(target_id = %target_id, "Target upserted");
        Ok(())
    }
}

#[derive(Clone)]
pub struct AuditRuntimeFacade {
    registry: Arc<Mutex<crate::AuditRegistry>>,
    replay_workers: Arc<RwLock<rustfs_targets::ReplayWorkerManager>>,
}

impl AuditRuntimeFacade {
    pub fn new(
        registry: Arc<Mutex<crate::AuditRegistry>>,
        replay_workers: Arc<RwLock<rustfs_targets::ReplayWorkerManager>>,
    ) -> Self {
        Self {
            registry,
            replay_workers,
        }
    }

    pub async fn replace_targets(&self, activation: rustfs_targets::RuntimeActivation<AuditEntry>) -> AuditResult<()> {
        self.stop_replay_workers().await;

        let mut registry = self.registry.lock().await;
        registry.close_all().await?;

        for target in activation.targets {
            let target_id = target.id().to_string();
            info!(target_id = %target_id, "Target initialized");
            registry.add_shared_target(target_id, target);
        }

        drop(registry);
        *self.replay_workers.write().await = activation.replay_workers;
        Ok(())
    }

    pub async fn activate_targets_with_replay(
        &self,
        state: Arc<RwLock<AuditSystemState>>,
        targets: Vec<Box<dyn Target<AuditEntry> + Send + Sync>>,
    ) -> rustfs_targets::RuntimeActivation<AuditEntry> {
        rustfs_targets::activate_targets_with_replay(targets, |target| {
            let state = state.clone();
            async move {
                rustfs_targets::init_target_and_optionally_start_replay(
                    target,
                    |target_id, has_replay| {
                        if has_replay {
                            info!(target_id = %target_id, "Audit stream processing started");
                        } else {
                            info!(target_id = %target_id, "No store configured, skip audit stream processing");
                        }
                    },
                    move |store, target| {
                        rustfs_targets::start_replay_worker(
                            store,
                            target,
                            Arc::new(move |event| {
                                let state = state.clone();
                                Box::pin(async move {
                                    if !matches!(
                                        *state.read().await,
                                        AuditSystemState::Running | AuditSystemState::Paused | AuditSystemState::Starting
                                    ) {
                                        return;
                                    }
                                    match event {
                                        rustfs_targets::ReplayEvent::Delivered { key, target } => {
                                            info!(
                                                "Successfully sent audit entry, target: {}, key: {}",
                                                target.id(),
                                                key.to_string()
                                            );
                                            observability::record_target_success();
                                        }
                                        rustfs_targets::ReplayEvent::RetryableError { error, target, .. } => match error {
                                            rustfs_targets::TargetError::NotConnected => {
                                                warn!("Target {} not connected, retrying...", target.id());
                                            }
                                            rustfs_targets::TargetError::Timeout(_) => {
                                                warn!("Timeout sending to target {}, retrying...", target.id());
                                            }
                                            _ => {}
                                        },
                                        rustfs_targets::ReplayEvent::Dropped { reason, target, .. } => {
                                            warn!("Dropped queued payload for target {}: {}", target.id(), reason);
                                            observability::record_target_failure();
                                        }
                                        rustfs_targets::ReplayEvent::PermanentFailure { error, target, .. } => {
                                            error!("Permanent error for target {}: {}", target.id(), error);
                                            target.record_final_failure();
                                            observability::record_target_failure();
                                        }
                                        rustfs_targets::ReplayEvent::RetryExhausted { key, target } => {
                                            warn!(
                                                "Max retries exceeded for key {}, target: {}, skipping",
                                                key.to_string(),
                                                target.id()
                                            );
                                            target.record_final_failure();
                                            observability::record_target_failure();
                                        }
                                        rustfs_targets::ReplayEvent::UnreadableEntry { key, error, target } => {
                                            warn!(
                                                "Skipping unreadable audit store entry {} for target {}: {}",
                                                key,
                                                target.id(),
                                                error
                                            );
                                        }
                                    }
                                })
                            }),
                            None,
                            Duration::from_millis(500),
                            Duration::from_millis(500),
                        )
                    },
                )
                .await
            }
        })
        .await
    }

    pub async fn stop_replay_workers(&self) {
        let mut replay_workers = self.replay_workers.write().await;
        replay_workers.stop_all("Stopping audit stream").await;
    }
}
