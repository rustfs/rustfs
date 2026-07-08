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
use rustfs_targets::{
    BuiltinPluginRuntimeAdapter, PluginRuntimeAdapter, ReplayEvent, ReplayWorkerManager, RuntimeActivation, SharedTarget, Target,
    target::EntityTarget,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info, warn};

const LOG_COMPONENT_AUDIT: &str = "audit";
const LOG_SUBSYSTEM_PIPELINE: &str = "pipeline";
const EVENT_AUDIT_DISPATCH_SKIPPED: &str = "audit_dispatch_skipped";
const EVENT_AUDIT_DISPATCH_FAILED: &str = "audit_dispatch_failed";
const EVENT_AUDIT_BATCH_DISPATCH_SKIPPED: &str = "audit_batch_dispatch_skipped";
const EVENT_AUDIT_BATCH_DISPATCH_FAILED: &str = "audit_batch_dispatch_failed";
const EVENT_AUDIT_BATCH_DISPATCH_COMPLETED: &str = "audit_batch_dispatch_completed";
const EVENT_AUDIT_TARGET_STATE_CHANGED: &str = "audit_target_state_changed";
const EVENT_AUDIT_REPLAY_DELIVERED: &str = "audit_replay_delivered";
const EVENT_AUDIT_REPLAY_RETRY_SCHEDULED: &str = "audit_replay_retry_scheduled";
const EVENT_AUDIT_REPLAY_DROPPED: &str = "audit_replay_dropped";
const EVENT_AUDIT_REPLAY_STREAM_STATUS: &str = "audit_replay_stream_status";

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
                debug!(
                    event = EVENT_AUDIT_DISPATCH_SKIPPED,
                    component = LOG_COMPONENT_AUDIT,
                    subsystem = LOG_SUBSYSTEM_PIPELINE,
                    reason = "no_targets_configured",
                    "Skipped audit dispatch"
                );
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
                    error!(
                        event = EVENT_AUDIT_DISPATCH_FAILED,
                        component = LOG_COMPONENT_AUDIT,
                        subsystem = LOG_SUBSYSTEM_PIPELINE,
                        target_id = %target_key,
                        error = %e,
                        "Failed to dispatch audit event"
                    );
                    errors.push(e);
                    observability::record_target_failure();
                }
            }
        }

        let dispatch_time = start_time.elapsed();

        if errors.is_empty() {
            observability::record_audit_success(dispatch_time);
            return Ok(());
        }

        observability::record_audit_failure(dispatch_time);
        let error_count = errors.len();

        if success_count == 0 {
            // Every configured target rejected the event. For store-backed targets a
            // failed save() means the entry was neither delivered nor persisted for
            // replay, so it is lost outright. Propagate the failure instead of
            // returning Ok so the caller can react (alert, degrade, or reject the
            // request) rather than assume the audit trail is intact.
            error!(
                event = EVENT_AUDIT_DISPATCH_FAILED,
                component = LOG_COMPONENT_AUDIT,
                subsystem = LOG_SUBSYSTEM_PIPELINE,
                error_count = error_count,
                duration_ms = dispatch_time.as_millis() as u64,
                "All audit targets failed to receive audit event"
            );
            // `errors` is non-empty here, so `remove(0)` cannot panic.
            return Err(crate::AuditError::Target(errors.remove(0)));
        }

        // Partial failure: at least one target accepted the event, so the entry is
        // not lost. Surface the degradation but let the dispatch succeed.
        warn!(
            event = EVENT_AUDIT_DISPATCH_FAILED,
            component = LOG_COMPONENT_AUDIT,
            subsystem = LOG_SUBSYSTEM_PIPELINE,
            error_count = error_count,
            success_count = success_count,
            duration_ms = dispatch_time.as_millis() as u64,
            "Some audit targets failed to receive audit event"
        );

        Ok(())
    }

    pub async fn dispatch_batch(&self, entries: Vec<Arc<AuditEntry>>) -> AuditResult<()> {
        let start_time = std::time::Instant::now();

        let targets: Vec<SharedTarget<AuditEntry>> = {
            let registry = self.registry.lock().await;
            let targets = registry.list_target_values();

            if targets.is_empty() {
                debug!(
                    event = EVENT_AUDIT_BATCH_DISPATCH_SKIPPED,
                    component = LOG_COMPONENT_AUDIT,
                    subsystem = LOG_SUBSYSTEM_PIPELINE,
                    entry_count = entries.len(),
                    reason = "no_targets_configured",
                    "Skipped audit batch dispatch"
                );
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
        let mut first_error: Option<rustfs_targets::TargetError> = None;
        for (target_id, success_count, errors) in results {
            total_success += success_count;
            total_errors += errors.len();
            for e in errors {
                error!(
                    event = EVENT_AUDIT_BATCH_DISPATCH_FAILED,
                    component = LOG_COMPONENT_AUDIT,
                    subsystem = LOG_SUBSYSTEM_PIPELINE,
                    target_id = %target_id,
                    error = ?e,
                    "Audit batch dispatch failed"
                );
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }

        let dispatch_time = start_time.elapsed();
        debug!(
            event = EVENT_AUDIT_BATCH_DISPATCH_COMPLETED,
            component = LOG_COMPONENT_AUDIT,
            subsystem = LOG_SUBSYSTEM_PIPELINE,
            entry_count = entries.len(),
            success_count = total_success,
            error_count = total_errors,
            duration_ms = dispatch_time.as_millis() as u64,
            "Completed audit batch dispatch"
        );

        // No save() across any target/entry succeeded while errors were recorded:
        // the batch was lost entirely. Propagate rather than silently returning Ok.
        if total_errors > 0 && total_success == 0 {
            observability::record_audit_failure(dispatch_time);
            error!(
                event = EVENT_AUDIT_BATCH_DISPATCH_FAILED,
                component = LOG_COMPONENT_AUDIT,
                subsystem = LOG_SUBSYSTEM_PIPELINE,
                entry_count = entries.len(),
                error_count = total_errors,
                duration_ms = dispatch_time.as_millis() as u64,
                "All audit targets failed to receive audit batch"
            );
            return Err(crate::AuditError::Target(
                first_error.expect("total_errors > 0 guarantees a captured target error"),
            ));
        }

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

    pub async fn get_target_values(&self) -> Vec<SharedTarget<AuditEntry>> {
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
            info!(
                event = EVENT_AUDIT_TARGET_STATE_CHANGED,
                component = LOG_COMPONENT_AUDIT,
                subsystem = LOG_SUBSYSTEM_PIPELINE,
                target_id = %target_id,
                state = "enabled",
                "audit target state"
            );
            Ok(())
        } else {
            Err(crate::AuditError::Configuration(format!("Target not found: {target_id}"), None))
        }
    }

    pub async fn disable_target(&self, target_id: &str) -> AuditResult<()> {
        let registry = self.registry.lock().await;
        if registry.get_target(target_id).is_some() {
            info!(
                event = EVENT_AUDIT_TARGET_STATE_CHANGED,
                component = LOG_COMPONENT_AUDIT,
                subsystem = LOG_SUBSYSTEM_PIPELINE,
                target_id = %target_id,
                state = "disabled",
                "audit target state"
            );
            Ok(())
        } else {
            Err(crate::AuditError::Configuration(format!("Target not found: {target_id}"), None))
        }
    }

    pub async fn remove_target(&self, target_id: &str) -> AuditResult<()> {
        let mut registry = self.registry.lock().await;
        if registry.remove_target(target_id).await.is_some() {
            info!(
                event = EVENT_AUDIT_TARGET_STATE_CHANGED,
                component = LOG_COMPONENT_AUDIT,
                subsystem = LOG_SUBSYSTEM_PIPELINE,
                target_id = %target_id,
                state = "removed",
                "audit target state"
            );
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
        info!(
            event = EVENT_AUDIT_TARGET_STATE_CHANGED,
            component = LOG_COMPONENT_AUDIT,
            subsystem = LOG_SUBSYSTEM_PIPELINE,
            target_id = %target_id,
            state = "upserted",
            "audit target state"
        );
        Ok(())
    }
}

#[derive(Clone)]
pub struct AuditRuntimeFacade {
    registry: Arc<Mutex<crate::AuditRegistry>>,
    replay_workers: Arc<RwLock<ReplayWorkerManager>>,
    runtime_adapter: Arc<dyn PluginRuntimeAdapter<AuditEntry>>,
}

impl AuditRuntimeFacade {
    pub fn new(registry: Arc<Mutex<crate::AuditRegistry>>, replay_workers: Arc<RwLock<ReplayWorkerManager>>) -> Self {
        let runtime_adapter = BuiltinPluginRuntimeAdapter::new(
            Arc::new(move |event: ReplayEvent<AuditEntry>| {
                Box::pin(async move {
                    match event {
                        ReplayEvent::Delivered { key, target } => {
                            debug!(
                                event = EVENT_AUDIT_REPLAY_DELIVERED,
                                component = LOG_COMPONENT_AUDIT,
                                subsystem = LOG_SUBSYSTEM_PIPELINE,
                                target_id = %target.id(),
                                replay_key = %key,
                                "audit replay delivery"
                            );
                            observability::record_target_success();
                        }
                        ReplayEvent::RetryableError { error, target, .. } => match error {
                            rustfs_targets::TargetError::NotConnected => {
                                debug!(
                                    event = EVENT_AUDIT_REPLAY_RETRY_SCHEDULED,
                                    component = LOG_COMPONENT_AUDIT,
                                    subsystem = LOG_SUBSYSTEM_PIPELINE,
                                    target_id = %target.id(),
                                    reason = "not_connected",
                                    "audit replay delivery"
                                );
                            }
                            rustfs_targets::TargetError::Timeout(_) => {
                                debug!(
                                    event = EVENT_AUDIT_REPLAY_RETRY_SCHEDULED,
                                    component = LOG_COMPONENT_AUDIT,
                                    subsystem = LOG_SUBSYSTEM_PIPELINE,
                                    target_id = %target.id(),
                                    reason = "timeout",
                                    "audit replay delivery"
                                );
                            }
                            _ => {}
                        },
                        ReplayEvent::Dropped { reason, target, .. } => {
                            warn!(
                                event = EVENT_AUDIT_REPLAY_DROPPED,
                                component = LOG_COMPONENT_AUDIT,
                                subsystem = LOG_SUBSYSTEM_PIPELINE,
                                target_id = %target.id(),
                                reason = %reason,
                                "audit replay delivery"
                            );
                            observability::record_target_failure();
                        }
                        ReplayEvent::PermanentFailure { error, target, .. } => {
                            error!(
                                event = EVENT_AUDIT_REPLAY_DROPPED,
                                component = LOG_COMPONENT_AUDIT,
                                subsystem = LOG_SUBSYSTEM_PIPELINE,
                                target_id = %target.id(),
                                error = %error,
                                reason = "permanent_failure",
                                "audit replay delivery"
                            );
                            target.record_final_failure();
                            observability::record_target_failure();
                        }
                        ReplayEvent::RetryExhausted { key, target } => {
                            warn!(
                                event = EVENT_AUDIT_REPLAY_DROPPED,
                                component = LOG_COMPONENT_AUDIT,
                                subsystem = LOG_SUBSYSTEM_PIPELINE,
                                target_id = %target.id(),
                                replay_key = %key,
                                reason = "retry_exhausted",
                                "audit replay delivery"
                            );
                            target.record_final_failure();
                            observability::record_target_failure();
                        }
                        ReplayEvent::UnreadableEntry { key, error, target } => {
                            warn!(
                                event = EVENT_AUDIT_REPLAY_DROPPED,
                                component = LOG_COMPONENT_AUDIT,
                                subsystem = LOG_SUBSYSTEM_PIPELINE,
                                target_id = %target.id(),
                                replay_key = %key,
                                error = %error,
                                reason = "unreadable_entry",
                                "audit replay delivery"
                            );
                        }
                    }
                })
            }),
            Arc::new(|target_id, has_replay| {
                if has_replay {
                    info!(
                        event = EVENT_AUDIT_REPLAY_STREAM_STATUS,
                        component = LOG_COMPONENT_AUDIT,
                        subsystem = LOG_SUBSYSTEM_PIPELINE,
                        target_id = %target_id,
                        replay_enabled = true,
                        "audit replay stream"
                    );
                } else {
                    debug!(
                        event = EVENT_AUDIT_REPLAY_STREAM_STATUS,
                        component = LOG_COMPONENT_AUDIT,
                        subsystem = LOG_SUBSYSTEM_PIPELINE,
                        target_id = %target_id,
                        replay_enabled = false,
                        reason = "no_store_configured",
                        "audit replay stream"
                    );
                }
            }),
            None,
            Duration::from_millis(500),
            Duration::from_millis(500),
            "Stopping audit stream",
        );

        Self {
            registry,
            replay_workers,
            runtime_adapter: Arc::new(runtime_adapter),
        }
    }

    pub async fn replace_targets(&self, activation: RuntimeActivation<AuditEntry>) -> AuditResult<()> {
        let mut registry = self.registry.lock().await;
        let mut replay_workers = self.replay_workers.write().await;
        self.runtime_adapter
            .replace_runtime_targets(registry.runtime_manager_mut(), &mut replay_workers, activation)
            .await
            .map_err(crate::AuditError::Target)?;
        Ok(())
    }

    pub async fn shutdown_runtime(
        &self,
        registry: &mut crate::AuditRegistry,
        replay_workers: &mut ReplayWorkerManager,
    ) -> AuditResult<()> {
        self.runtime_adapter
            .shutdown(registry.runtime_manager_mut(), replay_workers)
            .await
            .map_err(crate::AuditError::Target)
    }

    pub async fn activate_targets_with_replay(
        &self,
        targets: Vec<Box<dyn Target<AuditEntry> + Send + Sync>>,
    ) -> RuntimeActivation<AuditEntry> {
        self.runtime_adapter.activate_with_replay(targets).await
    }

    pub async fn stop_replay_workers(&self) {
        let mut replay_workers = self.replay_workers.write().await;
        self.runtime_adapter.stop_replay_workers(&mut replay_workers).await;
    }
}
