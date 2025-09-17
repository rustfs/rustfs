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

//! # RustFS Audit System
//!
//! Enterprise-grade audit logging system for RustFS distributed object storage with S3/MinIO compatibility.
//!
//! ## Features
//!
//! - **Multi-Target Dispatch**: Integrates with `rustfs-targets` for MQTT, Webhook, and extensible targets
//! - **High Performance**: Async processing with batching and concurrent dispatch (3k EPS/node, P99 < 30ms)
//! - **S3 Compatible**: Audit log format matches AWS S3 and MinIO audit logs exactly
//! - **Configuration Integration**: Uses `rustfs-config` constants and `rustfs-ecstore` for persistence
//! - **Environment Priority**: ENV > file instance > file default configuration precedence
//! - **Hot Reload**: Runtime configuration updates from `.rustfs.sys` without restart
//! - **Error Isolation**: Individual target failures don't affect other targets or main S3 operations
//! - **Header Redaction**: Configurable sensitive header masking for security compliance
//! - **Global Integration**: OnceCell-based singleton for easy system-wide integration
//!
//! ## Basic Usage
//!
//! ```rust
//! use rustfs_audit::{
//!     initialize_audit_system, log_audit_entry, s3_events,
//!     AuditConfig, load_config_from_env_and_ecstore
//! };
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // 1. Load configuration with ENV precedence
//!     let config = load_config_from_env_and_ecstore().await?;
//!     
//!     // 2. Initialize global audit system
//!     let factory = Arc::new(DefaultAuditTargetFactory::new());
//!     initialize_audit_logger(factory, config).await?;
//!     
//!     // 3. Log S3 operations
//!     let audit_entry = s3_events::get_object("my-bucket", "my-file.txt")
//!         .with_request_context(
//!             Some("192.168.1.100".to_string()),
//!             Some("aws-cli/2.0".to_string()),
//!             Some("/my-bucket/my-file.txt".to_string()),
//!             Some("s3.amazonaws.com".to_string()),
//!         )
//!         .with_response_status(
//!             Some("OK".to_string()),
//!             Some(200),
//!             None,
//!             Some(150_000_000), // 150ms
//!         );
//!     
//!     log_audit_entry(audit_entry).await?;
//!     
//!     Ok(())
//! }
//! ```
//!
//! ## Configuration
//!
//! The audit system supports JSON configuration with multiple targets:
//!
//! ```json
//! {
//!   "audit": {
//!     "enabled": true,
//!     "targets": [
//!       {
//!         "id": "audit-webhook-1",
//!         "kind": "webhook",
//!         "enabled": true,
//!         "args": {
//!           "target_type": "AuditLog",
//!           "url": "https://audit.company.com/webhook",
//!           "method": "POST",
//!           "headers": {"Authorization": "Bearer xxx"},
//!           "timeout_ms": 2000,
//!           "retries": 3
//!         }
//!       },
//!       {
//!         "id": "audit-mqtt-1",
//!         "kind": "mqtt",
//!         "enabled": true,
//!         "args": {
//!           "target_type": "AuditLog",
//!           "broker_url": "mqtts://broker:8883",
//!           "topic": "rustfs/audit",
//!           "qos": 1,
//!           "auth": {"username": "...", "password": "..."},
//!           "timeout_ms": 2000,
//!           "retries": 3
//!         }
//!       }
//!     ],
//!     "redaction": {
//!       "headers_blacklist": ["Authorization","Cookie","X-Api-Key"]
//!     }
//!   }
//! }
//! ```

pub mod config;
pub mod entity;
pub mod error;
pub mod global;
pub mod registry;
pub mod system;
pub mod targets;

// Re-export main types for easy access
pub use config::{AuditConfig, AuditTargetConfig, load_config_from_env_and_ecstore, load_config_with_prefix, PerformanceConfig, RedactionConfig};
pub use entity::{ApiDetails, AuditEntry, ObjectVersion, s3_events};
pub use error::{AuditError, AuditResult, TargetError, TargetResult};
pub use global::{
    AuditLogger, audit_logger, initialize_audit_system, log_audit_entry,
};
pub use registry::{DefaultAuditTargetFactory, TargetRegistry, TargetState, TargetStats, AuditTargetFactory};
pub use system::{AuditStats, AuditSystem};

// Re-export rustfs-targets types for convenience
pub use rustfs_targets::{EventName, Target, TargetLog};
