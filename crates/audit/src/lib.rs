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
//! - **Multi-Target Dispatch**: Send audit logs to MQTT, Webhook, and other configurable targets
//! - **High Performance**: Async processing with batching and concurrent dispatch (3k EPS/node, P99 < 30ms)
//! - **S3 Compatible**: Audit log format matches AWS S3 and MinIO audit logs
//! - **Hot Reload**: Runtime configuration updates without restart
//! - **Error Isolation**: Individual target failures don't affect other targets
//! - **Header Redaction**: Configurable sensitive header masking for security
//! - **Global Integration**: OnceCell-based singleton for easy system-wide integration
//!
//! ## Basic Usage
//!
//! ```rust
//! use rustfs_audit::{
//!     initialize_audit_logger, log_audit_entry, s3_events,
//!     AuditConfig, AuditTargetConfig, DefaultAuditTargetFactory
//! };
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // 1. Create configuration
//!     let mut config = AuditConfig::default();
//!     config.targets.push(AuditTargetConfig {
//!         id: "webhook-audit".to_string(),
//!         target_type: "webhook".to_string(),
//!         enabled: true,
//!         args: serde_json::json!({
//!             "url": "http://localhost:8080/audit",
//!             "timeout_ms": 5000
//!         }),
//!     });
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

pub mod entity;
pub mod error;
pub mod global;
pub mod registry;
pub mod system;
pub mod targets;

// Re-export main types for easy access
pub use entity::{ApiDetails, AuditEntry, ObjectVersion};
pub use error::{AuditError, AuditResult, TargetError, TargetResult};
pub use global::{
    AuditLogger, audit_logger, close_audit_system, create_s3_audit_entry, get_audit_stats, initialize_audit_logger,
    is_audit_system_initialized, list_audit_targets, log_audit, log_audit_entry, pause_audit_system, resume_audit_system,
    s3_events, start_audit_system,
};
pub use registry::{AuditTarget, AuditTargetConfig, AuditTargetFactory, TargetRegistry, TargetStatus};
pub use system::{AuditConfig, AuditStats, AuditSystem, PerformanceConfig, RedactionConfig};
pub use targets::{DefaultAuditTargetFactory, MqttAuditTarget, WebhookAuditTarget};

// Re-export rustfs-targets types for convenience
pub use rustfs_targets::{EventName, Target, TargetLog};
