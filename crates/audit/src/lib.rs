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

//! # RustFS Audit Module
//!
//! Enterprise-grade audit logging system for RustFS with multi-target dispatch,
//! comprehensive observability, and scalable architecture.
//!
//! ## Features
//!
//! - **Multi-Target Dispatch**: Support for MQTT, Webhook, and extensible target system
//! - **Environment-First Configuration**: ENV > file instance > file default precedence
//! - **Unified Lifecycle Management**: start/pause/resume/close operations
//! - **Hot Reload**: Runtime configuration updates without service restart
//! - **Enterprise Observability**: Thread-safe statistics and performance monitoring
//! - **S3/MinIO Compatibility**: Full audit log format compatibility
//!
//! ## Usage
//!
//! ```rust
//! use rustfs_audit::{initialize_audit_logger, audit_logger, s3_events};
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Initialize the audit system
//!     initialize_audit_logger().await?;
//!     
//!     // Get the global logger
//!     let logger = audit_logger().expect("Audit logger should be initialized");
//!     
//!     // Log an S3 operation
//!     let audit_entry = s3_events::get_object("my-bucket", "my-key");
//!     logger.log(Arc::new(audit_entry)).await?;
//!     
//!     Ok(())
//! }
//! ```

pub mod config;
pub mod entity;
pub mod error;
pub mod global;
pub mod registry;
pub mod system;
pub mod targets;

// Re-export main types for convenience
pub use config::{AuditConfig, AuditTargetConfig, load_config_from_env};
pub use entity::{AuditEntry, s3_events};
pub use error::{AuditError, AuditResult};
pub use global::{initialize_audit_logger, audit_logger, log_audit_entry};
pub use registry::{AuditTargetFactory, TargetRegistry, TargetStats, RegistryStats};
pub use system::AuditSystem;

/// Version information
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Current API version for compatibility
pub const API_VERSION: &str = "v1";