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

//! Core audit system for high-performance log dispatch
use crate::entity::AuditEntry;
use crate::error::{AuditError, AuditResult};
use crate::registry::TargetRegistry;
use crate::{AuditConfig, AuditTargetConfig};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock as AsyncRwLock, mpsc, oneshot};
use tracing::{debug, error, info, warn};

/// Configuration for the audit system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditConfig {
    /// Whether audit logging is enabled
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// List of audit targets
    #[serde(default)]
    pub targets: Vec<AuditTargetConfig>,
    /// Header redaction blacklist
    #[serde(default)]
    pub redaction: RedactionConfig,
    /// Performance settings
    #[serde(default)]
    pub performance: PerformanceConfig,
}

/// Header redaction configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedactionConfig {
    /// Headers to redact in audit logs
    #[serde(default = "default_blacklist")]
    pub headers_blacklist: Vec<String>,
}

/// Performance configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    /// Channel buffer size for audit entries
    #[serde(default = "default_channel_buffer")]
    pub channel_buffer_size: usize,
    /// Maximum concurrent dispatches
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent_dispatches: usize,
    /// Batch size for processing entries
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Batch timeout in milliseconds
    #[serde(default = "default_batch_timeout_ms")]
    pub batch_timeout_ms: u64,
    /// Maximum queue size before dropping entries
    #[serde(default = "default_max_queue_size")]
    pub max_queue_size: usize,
}

/// Statistics for the audit system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditStats {
    /// Total entries processed
    pub entries_processed: u64,
    /// Total entries dropped due to errors
    pub entries_dropped: u64,
    /// Current queue size
    pub current_queue_size: usize,
    /// Average processing time in nanoseconds
    pub avg_processing_time_ns: u64,
    /// Number of active targets
    pub active_targets: usize,
    /// System start time
    pub start_time: chrono::DateTime<chrono::Utc>,
    /// Last activity time
    pub last_activity: Option<chrono::DateTime<chrono::Utc>>,
}

/// Command for the audit system worker
enum AuditCommand {
    /// Log an audit entry
    LogEntry(Arc<AuditEntry>),
    /// Get system statistics
    GetStats(oneshot::Sender<AuditStats>),
    /// Shutdown the system
    Shutdown,
}

/// High-performance audit system
pub struct AuditSystem {
    /// Command sender
    cmd_tx: mpsc::UnboundedSender<AuditCommand>,
    /// Target registry
    registry: Arc<TargetRegistry>,
    /// System configuration
    config: Arc<AsyncRwLock<AuditConfig>>,
    /// System statistics
    #[allow(dead_code)]
    stats: Arc<RwLock<AuditStats>>,
    /// Shutdown signal
    shutdown_tx: Arc<AsyncRwLock<Option<oneshot::Sender<()>>>>,
}

impl AuditSystem {
    /// Create a new audit system
    pub fn new(registry: Arc<TargetRegistry>, config: AuditConfig) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let stats = Arc::new(RwLock::new(AuditStats {
            entries_processed: 0,
            entries_dropped: 0,
            current_queue_size: 0,
            avg_processing_time_ns: 0,
            active_targets: 0,
            start_time: chrono::Utc::now(),
            last_activity: None,
        }));

        let config_arc = Arc::new(AsyncRwLock::new(config.clone()));

        // Start worker task
        let worker_registry = registry.clone();
        let worker_config = config_arc.clone();
        let worker_stats = stats.clone();

        tokio::spawn(async move {
            Self::worker_task(worker_registry, worker_config, worker_stats, cmd_rx, shutdown_rx).await;
        });

        Self {
            cmd_tx,
            registry,
            config: config_arc,
            stats,
            shutdown_tx: Arc::new(AsyncRwLock::new(Some(shutdown_tx))),
        }
    }

    /// Log an audit entry
    pub async fn log(&self, entry: Arc<AuditEntry>) -> AuditResult<()> {
        // Check if system is enabled
        if !self.config.read().await.enabled {
            debug!("Audit system disabled, dropping entry");
            return Ok(());
        }

        // Apply header redaction
        let mut entry = (*entry).clone();
        let blacklist = &self.config.read().await.redaction.headers_blacklist;
        entry.redact_headers(blacklist);

        // Send to worker
        self.cmd_tx
            .send(AuditCommand::LogEntry(Arc::new(entry)))
            .map_err(|_| AuditError::channel("Failed to send audit entry to worker"))?;

        Ok(())
    }

    /// Get system statistics
    pub async fn get_stats(&self) -> AuditResult<AuditStats> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(AuditCommand::GetStats(tx))
            .map_err(|_| AuditError::channel("Failed to request stats"))?;

        rx.await.map_err(|_| AuditError::channel("Failed to receive stats"))
    }

    /// Update system configuration
    pub async fn update_config(&self, new_config: AuditConfig) -> AuditResult<()> {
        *self.config.write().await = new_config;
        info!("Audit system configuration updated");
        Ok(())
    }

    /// Get current configuration
    pub async fn get_config(&self) -> AuditConfig {
        self.config.read().await.clone()
    }

    /// Start the audit system
    pub async fn start(&self) -> AuditResult<()> {
        info!("Starting audit system");
        // System is automatically started when created
        Ok(())
    }

    /// Pause audit logging
    pub async fn pause(&self) -> AuditResult<()> {
        let mut config = self.config.write().await;
        config.enabled = false;
        info!("Audit system paused");
        Ok(())
    }

    /// Resume audit logging
    pub async fn resume(&self) -> AuditResult<()> {
        let mut config = self.config.write().await;
        config.enabled = true;
        info!("Audit system resumed");
        Ok(())
    }

    /// Shutdown the audit system gracefully
    pub async fn close(&self) -> AuditResult<()> {
        info!("Shutting down audit system");

        // Send shutdown command
        if let Err(_) = self.cmd_tx.send(AuditCommand::Shutdown) {
            warn!("Failed to send shutdown command to worker");
        }

        // Trigger shutdown signal
        if let Some(shutdown_tx) = self.shutdown_tx.write().await.take() {
            let _ = shutdown_tx.send(());
        }

        // Close all targets
        self.registry.close_all().await?;

        info!("Audit system shutdown complete");
        Ok(())
    }

    /// Get target registry
    pub fn registry(&self) -> &Arc<TargetRegistry> {
        &self.registry
    }

    /// Worker task for processing audit entries
    async fn worker_task(
        registry: Arc<TargetRegistry>,
        config: Arc<AsyncRwLock<AuditConfig>>,
        stats: Arc<RwLock<AuditStats>>,
        mut cmd_rx: mpsc::UnboundedReceiver<AuditCommand>,
        mut shutdown_rx: oneshot::Receiver<()>,
    ) {
        info!("Audit system worker started");

        let mut batch = Vec::new();
        let batch_timeout = Duration::from_millis(config.read().await.performance.batch_timeout_ms);
        let mut last_flush = Instant::now();

        loop {
            tokio::select! {
                // Process commands
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(AuditCommand::LogEntry(entry)) => {
                            batch.push(entry);

                            // Update queue size
                            {
                                let mut s = stats.write();
                                s.current_queue_size = batch.len();
                                s.last_activity = Some(chrono::Utc::now());
                            }

                            // Check if we should flush the batch
                            let batch_size = config.read().await.performance.batch_size;
                            if batch.len() >= batch_size || last_flush.elapsed() >= batch_timeout {
                                Self::flush_batch(&registry, &stats, &mut batch).await;
                                last_flush = Instant::now();
                            }
                        }
                        Some(AuditCommand::GetStats(tx)) => {
                            let current_stats = {
                                let mut s = stats.write();
                                s.active_targets = registry.enabled_target_count();
                                s.clone()
                            };
                            let _ = tx.send(current_stats);
                        }
                        Some(AuditCommand::Shutdown) => {
                            info!("Worker received shutdown command");
                            break;
                        }
                        None => {
                            warn!("Command channel closed");
                            break;
                        }
                    }
                }

                // Periodic batch flush
                _ = tokio::time::sleep(batch_timeout) => {
                    if !batch.is_empty() && last_flush.elapsed() >= batch_timeout {
                        Self::flush_batch(&registry, &stats, &mut batch).await;
                        last_flush = Instant::now();
                    }
                }

                // Shutdown signal
                _ = &mut shutdown_rx => {
                    info!("Worker received shutdown signal");
                    break;
                }
            }
        }

        // Flush remaining entries
        if !batch.is_empty() {
            Self::flush_batch(&registry, &stats, &mut batch).await;
        }

        info!("Audit system worker stopped");
    }

    /// Flush a batch of audit entries
    async fn flush_batch(registry: &Arc<TargetRegistry>, stats: &Arc<RwLock<AuditStats>>, batch: &mut Vec<Arc<AuditEntry>>) {
        if batch.is_empty() {
            return;
        }

        let start_time = Instant::now();
        let batch_size = batch.len();

        // Dispatch to all targets concurrently
        let mut tasks = Vec::new();
        for entry in batch.drain(..) {
            let registry_clone = registry.clone();
            tasks.push(tokio::spawn(async move { registry_clone.dispatch(entry).await }));
        }

        // Wait for all dispatches to complete
        let mut success_count = 0;
        let mut error_count = 0;

        for task in tasks {
            match task.await {
                Ok(results) => {
                    for result in results {
                        match result {
                            Ok(()) => success_count += 1,
                            Err(e) => {
                                error_count += 1;
                                debug!("Target dispatch failed: {}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    error_count += 1;
                    error!("Task join error: {}", e);
                }
            }
        }

        // Update statistics
        let processing_time = start_time.elapsed();
        {
            let mut s = stats.write();
            s.entries_processed += success_count;
            s.entries_dropped += error_count;
            s.current_queue_size = 0;

            // Update average processing time (simple moving average)
            let new_time_ns = processing_time.as_nanos() as u64 / batch_size as u64;
            if s.avg_processing_time_ns == 0 {
                s.avg_processing_time_ns = new_time_ns;
            } else {
                s.avg_processing_time_ns = (s.avg_processing_time_ns * 9 + new_time_ns) / 10;
            }
        }

        debug!(
            "Processed batch of {} entries in {:?} (success: {}, errors: {})",
            batch_size, processing_time, success_count, error_count
        );
    }
}

// Default values
fn default_enabled() -> bool {
    true
}

fn default_blacklist() -> Vec<String> {
    vec!["Authorization".to_string(), "Cookie".to_string(), "X-Api-Key".to_string()]
}

fn default_channel_buffer() -> usize {
    10000
}

fn default_max_concurrent() -> usize {
    100
}

fn default_batch_size() -> usize {
    50
}

fn default_batch_timeout_ms() -> u64 {
    100
}

fn default_max_queue_size() -> usize {
    50000
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            targets: Vec::new(),
            redaction: RedactionConfig::default(),
            performance: PerformanceConfig::default(),
        }
    }
}

impl Default for RedactionConfig {
    fn default() -> Self {
        Self {
            headers_blacklist: default_blacklist(),
        }
    }
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            channel_buffer_size: default_channel_buffer(),
            max_concurrent_dispatches: default_max_concurrent(),
            batch_size: default_batch_size(),
            batch_timeout_ms: default_batch_timeout_ms(),
            max_queue_size: default_max_queue_size(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TargetStatus;
    use crate::registry::{AuditTarget, AuditTargetFactory};
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    // Mock target for testing
    struct MockTarget {
        id: String,
        counter: Arc<AtomicUsize>,
        should_fail: Arc<AtomicBool>,
    }

    #[async_trait]
    impl AuditTarget for MockTarget {
        async fn send(&self, _entry: Arc<AuditEntry>) -> crate::error::TargetResult<()> {
            if self.should_fail.load(Ordering::Relaxed) {
                Err(crate::error::TargetError::operation_failed("Mock failure"))
            } else {
                self.counter.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        }

        async fn start(&self) -> crate::error::TargetResult<()> {
            Ok(())
        }
        async fn pause(&self) -> crate::error::TargetResult<()> {
            Ok(())
        }
        async fn resume(&self) -> crate::error::TargetResult<()> {
            Ok(())
        }
        async fn close(&self) -> crate::error::TargetResult<()> {
            Ok(())
        }

        fn id(&self) -> &str {
            &self.id
        }
        fn target_type(&self) -> &str {
            "mock"
        }
        fn is_enabled(&self) -> bool {
            true
        }

        fn status(&self) -> TargetStatus {
            TargetStatus {
                id: self.id.clone(),
                target_type: "mock".to_string(),
                enabled: true,
                running: true,
                last_error: None,
                last_error_time: None,
                success_count: self.counter.load(Ordering::Relaxed) as u64,
                error_count: 0,
                last_success_time: None,
                queue_size: None,
            }
        }
    }

    struct MockFactory {
        counter: Arc<AtomicUsize>,
        should_fail: Arc<AtomicBool>,
    }

    #[async_trait]
    impl AuditTargetFactory for MockFactory {
        async fn create_target(&self, config: &AuditTargetConfig) -> crate::error::TargetResult<Box<dyn AuditTarget>> {
            Ok(Box::new(MockTarget {
                id: config.id.clone(),
                counter: self.counter.clone(),
                should_fail: self.should_fail.clone(),
            }))
        }

        fn validate_config(&self, _config: &AuditTargetConfig) -> crate::error::TargetResult<()> {
            Ok(())
        }

        fn supported_types(&self) -> Vec<&'static str> {
            vec!["mock"]
        }
    }

    #[tokio::test]
    async fn test_audit_system_basic_operations() {
        let counter = Arc::new(AtomicUsize::new(0));
        let should_fail = Arc::new(AtomicBool::new(false));

        let factory = Arc::new(MockFactory {
            counter: counter.clone(),
            should_fail: should_fail.clone(),
        });

        let registry = Arc::new(TargetRegistry::new(factory));

        // Add a mock target
        let config = AuditTargetConfig {
            id: "test-target".to_string(),
            target_type: "mock".to_string(),
            enabled: true,
            args: serde_json::Value::Object(serde_json::Map::new()),
        };
        registry.add_target(config).await.unwrap();

        let audit_config = AuditConfig::default();
        let system = AuditSystem::new(registry, audit_config);

        // Log some entries
        for i in 0..5 {
            let entry = Arc::new(AuditEntry::for_s3_operation(
                "s3:GetObject",
                "GetObject",
                Some("test-bucket"),
                Some(&format!("test-object-{}", i)),
            ));
            system.log(entry).await.unwrap();
        }

        // Wait a bit for processing
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Check that entries were processed
        let stats = system.get_stats().await.unwrap();
        assert!(stats.entries_processed > 0);

        // Verify target received entries
        assert!(counter.load(Ordering::Relaxed) > 0);

        // Shutdown system
        system.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_audit_system_pause_resume() {
        let counter = Arc::new(AtomicUsize::new(0));
        let should_fail = Arc::new(AtomicBool::new(false));

        let factory = Arc::new(MockFactory {
            counter: counter.clone(),
            should_fail: should_fail.clone(),
        });

        let registry = Arc::new(TargetRegistry::new(factory));
        let audit_config = AuditConfig::default();
        let system = AuditSystem::new(registry, audit_config);

        // Pause system
        system.pause().await.unwrap();

        // Try to log entry (should be dropped)
        let entry = Arc::new(AuditEntry::for_s3_operation(
            "s3:GetObject",
            "GetObject",
            Some("test-bucket"),
            Some("test-object"),
        ));
        system.log(entry).await.unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Should not have processed any entries
        let stats = system.get_stats().await.unwrap();
        assert_eq!(stats.entries_processed, 0);

        // Resume and try again
        system.resume().await.unwrap();

        let entry = Arc::new(AuditEntry::for_s3_operation(
            "s3:PutObject",
            "PutObject",
            Some("test-bucket"),
            Some("test-object-2"),
        ));
        system.log(entry).await.unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        system.close().await.unwrap();
    }
}
