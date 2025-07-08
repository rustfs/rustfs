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

//! # Advanced Health & Metrics (AHM) System
//!
//! The AHM system provides comprehensive health monitoring, scanning, and healing
//! capabilities for RustFS distributed storage. It features:
//!
//! - **Smart Scanning**: Efficient object and disk scanning with bandwidth control
//! - **Intelligent Healing**: Priority-based repair system with real-time and background modes
//! - **Rich Metrics**: Comprehensive statistics collection and aggregation
//! - **Policy-Driven**: Configurable policies for scanning, healing, and retention
//! - **High Observability**: Detailed logging, tracing, and monitoring capabilities
//!
//! ## Architecture Overview
//!
//! ```text
//! ┌─────────────────────────────────────┐
//! │          API Layer (REST/gRPC)      │
//! ├─────────────────────────────────────┤
//! │       Policy & Configuration       │
//! ├─────────────────────────────────────┤
//! │     Core Coordination Engine       │
//! ├─────────────────────────────────────┤
//! │   Scanner Engine │  Heal Engine    │
//! ├─────────────────────────────────────┤
//! │        Metrics & Observability     │
//! ├─────────────────────────────────────┤
//! │         Storage Abstraction        │
//! └─────────────────────────────────────┘
//! ```
//!
//! ## Key Features
//!
//! ### Scanner System
//! - **Multi-level scanning**: Object, directory, and disk level scanning
//! - **Bandwidth throttling**: Configurable I/O bandwidth limits
//! - **Incremental scanning**: Smart detection of changes since last scan
//! - **Parallel processing**: Multi-threaded scanning with work-stealing queues
//!
//! ### Heal System
//! - **Priority-based healing**: Business-critical objects get priority
//! - **Real-time healing**: Immediate repair during GET/PUT operations
//! - **Background healing**: Scheduled full-system health checks
//! - **Validation system**: Comprehensive repair verification
//!
//! ### Metrics System
//! - **Real-time statistics**: Live collection of usage and performance metrics
//! - **Historical data**: Time-series storage for trend analysis
//! - **Aggregation layers**: Bucket, node, and cluster-level aggregations
//! - **Export capabilities**: Prometheus, InfluxDB, and custom backends

use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::info;

pub mod api;
pub mod core;
pub mod error;
pub mod heal;
pub mod metrics;
pub mod policy;
pub mod scanner;

use crate::error::Result;

/// Main AHM system coordinator
#[allow(dead_code)]
#[derive(Debug)]
pub struct AhmSystem {
    config: Arc<AhmConfig>,
    coordinator: Arc<core::Coordinator>,
    scheduler: Arc<core::scheduler::Scheduler>,
    health: Arc<RwLock<SystemHealth>>,
    metrics: Arc<metrics::Collector>,
    cancellation_token: CancellationToken,
}

impl AhmSystem {
    pub async fn new(
        config: AhmConfig,
    ) -> Result<Arc<AhmSystem>> {
        let config = Arc::new(config);
        let metrics = Arc::new(metrics::Collector::new(metrics::CollectorConfig::default()).await?);
        let coordinator = Arc::new(core::Coordinator::new(
            config.coordinator.clone(),
            metrics.clone(),
            CancellationToken::new(),
        ).await?);
        let scheduler = Arc::new(core::scheduler::Scheduler::new(config.scheduler.clone()).await?);

        let system = Arc::new(AhmSystem {
            config,
            coordinator,
            scheduler,
            health: Arc::new(RwLock::new(SystemHealth::default())),
            metrics,
            cancellation_token: CancellationToken::new(),
        });

        Ok(system)
    }

    /// Starts the AHM system and all its components.
    pub async fn start(&self) -> Result<()> {
        info!("Starting AHM system...");
        let coordinator_handle = self.coordinator.start();
        let scheduler_handle = self.scheduler.start();

        self.cancellation_token.cancelled().await;

        info!("AHM system shutting down.");
        let _ = coordinator_handle.await;
        let _ = scheduler_handle.await;
        Ok(())
    }
}

/// Represents the overall health status of the system.
#[derive(Debug, Clone, Default)]
pub struct SystemHealth {
    pub status: String,
    pub last_check: Option<u64>,
    pub issues: Vec<String>,
}

/// System-wide events that flow between subsystems
#[derive(Debug, Clone)]
pub enum SystemEvent {
    /// Object discovered during scanning
    ObjectDiscovered {
        bucket: String,
        object: String,
        version_id: Option<String>,
        metadata: ObjectMetadata,
    },
    /// Object health issue detected
    HealthIssueDetected(scanner::HealthIssue),
    /// Healing operation completed
    HealCompleted(heal::HealResult),
    /// Scanner cycle completed
    ScanCompleted(scanner::ScanReport),
    /// System resource usage updated
    ResourceUsageUpdated {
        usage: ResourceUsage,
    },
}

#[derive(Debug, Clone, Default)]
pub struct AhmConfig {
    pub enabled: bool,
    pub coordinator: core::CoordinatorConfig,
    pub scheduler: core::SchedulerConfig,
    pub scanner: scanner::ScannerConfig,
    pub heal: heal::HealConfig,
    pub metrics: metrics::CollectorConfig,
    pub api: api::ApiConfig,
    pub policy: policy::PolicyManagerConfig,
}

#[derive(Debug, Clone)]
pub struct ObjectMetadata {
    pub size: u64,
    pub mod_time: i64,
    pub content_type: String,
    pub etag: String,
}

/// Overall system health status
#[derive(Debug, Clone)]
pub struct HealthStatus {
    pub scanner_status: scanner::ScannerStatus,
    pub heal_status: heal::Status,
    pub metrics_status: metrics::Status,
    pub coordinator_status: core::Status,
}

/// Types of health issues that can be detected
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HealthIssueType {
    /// Missing object shards
    MissingShards,
    /// Corrupted data (checksum mismatch)
    DataCorruption,
    /// Insufficient redundancy
    InsufficientRedundancy,
    /// Disk hardware issues
    DiskFailure,
    /// Network connectivity issues
    NetworkIssues,
    /// Metadata inconsistency
    MetadataInconsistency,
    /// System-level issue
    System,
}

/// Severity levels for health issues
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    Low,
    Medium,
    High,
    Critical,
}

/// Result of a healing operation
#[derive(Debug, Clone)]
pub struct HealResult {
    pub success: bool,
    pub shards_repaired: u32,
    pub error_message: Option<String>,
    pub duration_ms: u64,
}

/// Statistics from a scan cycle
#[derive(Debug, Clone)]
pub struct ScanStatistics {
    pub objects_scanned: u64,
    pub bytes_scanned: u64,
    pub issues_found: u64,
    pub duration_ms: u64,
    pub scan_rate_objects_per_sec: f64,
    pub scan_rate_bytes_per_sec: f64,
}

/// System resource usage information
#[derive(Debug, Clone)]
pub struct ResourceUsage {
    pub disk_usage: DiskUsage,
    pub memory_usage: MemoryUsage,
    pub network_usage: NetworkUsage,
    pub cpu_usage: CpuUsage,
}

#[derive(Debug, Clone)]
pub struct DiskUsage {
    pub total_capacity: u64,
    pub used_capacity: u64,
    pub available_capacity: u64,
    pub iops_read: f64,
    pub iops_write: f64,
}

#[derive(Debug, Clone)]
pub struct MemoryUsage {
    pub total_memory: u64,
    pub used_memory: u64,
    pub available_memory: u64,
    pub cache_usage: u64,
}

#[derive(Debug, Clone)]
pub struct NetworkUsage {
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub packets_sent: u64,
    pub packets_received: u64,
    pub bandwidth_utilization: f64,
}

#[derive(Debug, Clone)]
pub struct CpuUsage {
    pub total_cores: u32,
    pub usage_percentage: f64,
    pub load_average: [f64; 3], // 1min, 5min, 15min
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncryptionStatus {
    None,
    ServerSide,
    ClientSide,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationStatus {
    None,
    Pending,
    InProgress,
    Completed,
    Failed,
} 