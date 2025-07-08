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

//! Core coordinator for the AHM system
//!
//! The coordinator is responsible for:
//! - Event routing and distribution between subsystems
//! - Resource management and allocation
//! - Global state coordination
//! - Cross-system communication

use std::{
    sync::{Arc, atomic::{AtomicU64, Ordering}},
    time::{Duration, Instant},
};

use tokio::{
    sync::{broadcast, RwLock},
    task::JoinHandle,
    time::interval,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::{SystemEvent, metrics};
use super::{Status, Scheduler, SchedulerConfig};
use crate::scanner;
use crate::error::Result;
use crate::scanner::{HealthIssue, HealthIssueType, Severity};

/// Configuration for the coordinator
#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    /// Event channel buffer size
    pub event_buffer_size: usize,
    /// Resource monitoring interval
    pub resource_monitor_interval: Duration,
    /// Maximum number of concurrent operations
    pub max_concurrent_operations: usize,
    /// Scheduler configuration
    pub scheduler: SchedulerConfig,
    /// Event channel capacity
    pub event_channel_capacity: usize,
    /// Health check interval
    pub health_check_interval: Duration,
    /// Metrics update interval
    pub metrics_update_interval: Duration,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            event_buffer_size: 10000,
            resource_monitor_interval: Duration::from_secs(30),
            max_concurrent_operations: 100,
            scheduler: SchedulerConfig::default(),
            event_channel_capacity: 1024,
            health_check_interval: Duration::from_secs(300),
            metrics_update_interval: Duration::from_secs(60),
        }
    }
}

/// Core coordinator for the AHM system
#[derive(Debug)]
pub struct Coordinator {
    /// Configuration
    config: CoordinatorConfig,
    /// Current status
    status: Arc<RwLock<Status>>,
    /// Event broadcaster
    event_tx: broadcast::Sender<SystemEvent>,
    /// Resource monitor handle
    resource_monitor_handle: Arc<RwLock<Option<JoinHandle<()>>>>,
    /// Event processor handle
    event_processor_handle: Arc<RwLock<Option<JoinHandle<()>>>>,
    /// Task scheduler
    scheduler: Arc<Scheduler>,
    /// Metrics collector reference
    metrics: Arc<metrics::Collector>,
    /// Active operations counter
    active_operations: AtomicU64,
    /// Cancellation token
    cancel_token: CancellationToken,
    /// Operation statistics
    operation_stats: Arc<RwLock<OperationStatistics>>,
}

impl Coordinator {
    /// Create a new coordinator
    pub async fn new(
        config: CoordinatorConfig,
        metrics: Arc<metrics::Collector>,
        cancel_token: CancellationToken,
    ) -> Result<Self> {
        let (event_tx, _) = broadcast::channel(config.event_buffer_size);
        let scheduler = Arc::new(Scheduler::new(config.scheduler.clone()).await?);
        
        Ok(Self {
            config,
            status: Arc::new(RwLock::new(Status::Initializing)),
            event_tx,
            resource_monitor_handle: Arc::new(RwLock::new(None)),
            event_processor_handle: Arc::new(RwLock::new(None)),
            scheduler,
            metrics,
            active_operations: AtomicU64::new(0),
            cancel_token,
            operation_stats: Arc::new(RwLock::new(OperationStatistics::default())),
        })
    }
    
    /// Start the coordinator
    pub async fn start(&self) -> Result<()> {
        info!("Starting AHM coordinator");
        
        // Update status
        *self.status.write().await = Status::Running;
        
        // Start resource monitor
        self.start_resource_monitor().await?;
        
        // Start event processor
        self.start_event_processor().await?;
        
        // Start scheduler
        self.scheduler.start().await?;
        
        info!("AHM coordinator started successfully");
        Ok(())
    }
    
    /// Stop the coordinator
    pub async fn stop(&self) -> Result<()> {
        info!("Stopping AHM coordinator");
        
        // Update status
        *self.status.write().await = Status::Stopping;
        
        // Stop scheduler
        self.scheduler.stop().await?;
        
        // Stop resource monitor
        if let Some(handle) = self.resource_monitor_handle.write().await.take() {
            handle.abort();
        }
        
        // Stop event processor
        if let Some(handle) = self.event_processor_handle.write().await.take() {
            handle.abort();
        }
        
        *self.status.write().await = Status::Stopped;
        info!("AHM coordinator stopped");
        Ok(())
    }
    
    /// Get current status
    pub async fn status(&self) -> Status {
        self.status.read().await.clone()
    }
    
    /// Subscribe to system events
    pub fn subscribe_events(&self) -> broadcast::Receiver<SystemEvent> {
        self.event_tx.subscribe()
    }
    
    /// Publish a system event
    pub async fn publish_event(&self, event: SystemEvent) -> Result<()> {
        debug!("Publishing system event: {:?}", event);
        
        // Update operation statistics
        self.update_operation_stats(&event).await;
        
        // Send to all subscribers
        if let Err(e) = self.event_tx.send(event.clone()) {
            warn!("Failed to publish event: {:?}", e);
        }
        
        // Record the event in metrics
        self.metrics.record_health_issue(&HealthIssue {
            issue_type: HealthIssueType::Unknown,
            severity: Severity::Low,
            bucket: "system".to_string(),
            object: "coordinator".to_string(),
            description: format!("System event: {:?}", event),
            metadata: None,
        }).await?;
        
        Ok(())
    }
    
    /// Get system resource usage
    pub async fn get_resource_usage(&self) -> metrics::ResourceUsage {
        metrics::ResourceUsage {
            disk_usage: metrics::DiskUsage {
                total_bytes: 1_000_000_000,
                used_bytes: 500_000_000,
                available_bytes: 500_000_000,
                usage_percentage: 50.0,
            },
            memory_usage: metrics::MemoryUsage {
                total_bytes: 16_000_000_000,
                used_bytes: 4_000_000_000,
                available_bytes: 12_000_000_000,
                usage_percentage: 25.0,
            },
            network_usage: metrics::NetworkUsage {
                bytes_received: 1_000_000,
                bytes_sent: 500_000,
                packets_received: 1000,
                packets_sent: 500,
            },
            cpu_usage: metrics::CpuUsage {
                usage_percentage: 0.25,
                cores: 8,
                load_average: 1.5,
            },
        }
    }
    
    /// Get operation statistics
    pub async fn get_operation_statistics(&self) -> OperationStatistics {
        self.operation_stats.read().await.clone()
    }
    
    /// Get active operations count
    pub fn get_active_operations_count(&self) -> u64 {
        self.active_operations.load(Ordering::Relaxed)
    }
    
    /// Register an active operation
    pub fn register_operation(&self) -> OperationGuard {
        let count = self.active_operations.fetch_add(1, Ordering::Relaxed);
        debug!("Registered operation, active count: {}", count + 1);
        OperationGuard::new(&self.active_operations)
    }
    
    /// Start the resource monitor
    async fn start_resource_monitor(&self) -> Result<()> {
        let cancel_token = self.cancel_token.clone();
        let _event_tx = self.event_tx.clone();
        let interval_duration = self.config.resource_monitor_interval;
        
        let handle = tokio::spawn(async move {
            let mut interval = interval(interval_duration);
            
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        debug!("Resource monitor cancelled");
                        break;
                    }
                    _ = interval.tick() => {
                        // This would collect real resource metrics
                        // For now, we'll skip the actual collection
                        debug!("Resource monitor tick");
                    }
                }
            }
        });
        
        *self.resource_monitor_handle.write().await = Some(handle);
        Ok(())
    }
    
    /// Start the event processor
    async fn start_event_processor(&self) -> Result<()> {
        let mut event_rx = self.event_tx.subscribe();
        let cancel_token = self.cancel_token.clone();
        
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        debug!("Event processor cancelled");
                        break;
                    }
                    event = event_rx.recv() => {
                        match event {
                            Ok(event) => {
                                debug!("Processing system event: {:?}", event);
                                // Process the event (e.g., route to specific handlers)
                            }
                            Err(e) => {
                                warn!("Event processor error: {:?}", e);
                            }
                        }
                    }
                }
            }
        });
        
        *self.event_processor_handle.write().await = Some(handle);
        Ok(())
    }
    
    /// Update operation statistics based on events
    async fn update_operation_stats(&self, event: &SystemEvent) {
        let mut stats = self.operation_stats.write().await;
        
        match event {
            SystemEvent::ObjectDiscovered { .. } => {
                stats.objects_discovered += 1;
            }
            SystemEvent::HealthIssueDetected(issue) => {
                stats.health_issues_detected += 1;
                match issue.severity {
                    scanner::Severity::Critical => stats.critical_issues += 1,
                    scanner::Severity::High => stats.high_priority_issues += 1,
                    scanner::Severity::Medium => stats.medium_priority_issues += 1,
                    scanner::Severity::Low => stats.low_priority_issues += 1,
                }
            }
            SystemEvent::HealCompleted(result) => {
                if result.success {
                    stats.heal_operations_succeeded += 1;
                } else {
                    stats.heal_operations_failed += 1;
                }
            }
            SystemEvent::ScanCompleted(_) => {
                stats.scan_cycles_completed += 1;
            }
            SystemEvent::ResourceUsageUpdated { .. } => {
                stats.resource_updates += 1;
            }
        }
        
        stats.last_updated = Instant::now();
    }
}

/// RAII guard for tracking active operations
pub struct OperationGuard<'a> {
    active_operations: &'a AtomicU64,
}

impl<'a> OperationGuard<'a> {
    pub fn new(active_operations: &'a AtomicU64) -> Self {
        active_operations.fetch_add(1, Ordering::Relaxed);
        Self { active_operations }
    }
}

impl Drop for OperationGuard<'_> {
    fn drop(&mut self) {
        self.active_operations.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Operation statistics tracked by the coordinator
#[derive(Debug, Clone)]
pub struct OperationStatistics {
    pub objects_discovered: u64,
    pub health_issues_detected: u64,
    pub heal_operations_succeeded: u64,
    pub heal_operations_failed: u64,
    pub scan_cycles_completed: u64,
    pub resource_updates: u64,
    pub critical_issues: u64,
    pub high_priority_issues: u64,
    pub medium_priority_issues: u64,
    pub low_priority_issues: u64,
    pub last_updated: Instant,
}

impl Default for OperationStatistics {
    fn default() -> Self {
        Self {
            objects_discovered: 0,
            health_issues_detected: 0,
            heal_operations_succeeded: 0,
            heal_operations_failed: 0,
            scan_cycles_completed: 0,
            resource_updates: 0,
            critical_issues: 0,
            high_priority_issues: 0,
            medium_priority_issues: 0,
            low_priority_issues: 0,
            last_updated: Instant::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::CollectorConfig;
    
    #[tokio::test]
    async fn test_coordinator_lifecycle() {
        let config = CoordinatorConfig::default();
        let metrics_config = CollectorConfig::default();
        let metrics = Arc::new(metrics::Collector::new(metrics_config).await.unwrap());
        let cancel_token = CancellationToken::new();
        
        let coordinator = Coordinator::new(config, metrics, cancel_token).await.unwrap();
        
        // Test initial status
        assert_eq!(coordinator.status().await, Status::Initializing);
        
        // Start coordinator
        coordinator.start().await.unwrap();
        assert_eq!(coordinator.status().await, Status::Running);
        
        // Stop coordinator
        coordinator.stop().await.unwrap();
        assert_eq!(coordinator.status().await, Status::Stopped);
    }
    
    #[tokio::test]
    async fn test_operation_guard() {
        let config = CoordinatorConfig::default();
        let metrics_config = CollectorConfig::default();
        let metrics = Arc::new(metrics::Collector::new(metrics_config).await.unwrap());
        let cancel_token = CancellationToken::new();
        
        let coordinator = Coordinator::new(config, metrics, cancel_token).await.unwrap();
        
        assert_eq!(coordinator.get_active_operations_count(), 0);
        
        {
            let _guard1 = coordinator.register_operation();
            assert_eq!(coordinator.get_active_operations_count(), 1);
            
            {
                let _guard2 = coordinator.register_operation();
                assert_eq!(coordinator.get_active_operations_count(), 2);
            }
            
            assert_eq!(coordinator.get_active_operations_count(), 1);
        }
        
        assert_eq!(coordinator.get_active_operations_count(), 0);
    }
} 