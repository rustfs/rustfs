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

//! # RustFS Observability
//!
//! provides tools for system and service monitoring
//!
//! ## Features
//!
//! This crate provides observability tools for RustFS:
//! - Logging with tracing
//! - Metrics collection
//! - Distributed tracing
//! - Tokio runtime telemetry (via dial9)
//!
//! ## Usage
//!
//! ```no_run
//! use rustfs_obs::init_obs;
//!
//! # #[tokio::main]
//! # async fn main() {
//! #   let _guard = match init_obs(None).await {
//! #         Ok(g) => g,
//! #         Err(e) => {
//! #             panic!("Failed to initialize observability: {:?}", e);
//! #         }
//! #     };
//! #   // Application logic here
//! #   {
//! #       // Simulate some work
//! #       tokio::time::sleep(std::time::Duration::from_secs(2)).await;
//! #       println!("Application is running...");
//! #   }
//! #   // Guard will be dropped here, flushing telemetry data
//! # }
//! ```
//!
//! ## System Monitoring Migration
//!
//! The system monitoring functionality has been migrated to `rustfs-metrics`.
//! Use `rustfs_metrics::init_metrics_collectors()` for system metrics collection.
//!
//! ```ignore
//! use tokio_util::sync::CancellationToken;
//! use rustfs_metrics::init_metrics_collectors;
//!
//! let token = CancellationToken::new();
//! init_metrics_collectors(token.clone());
//! ```
mod cleaner;
mod config;
mod error;
mod global;
mod telemetry;

pub use cleaner::*;
pub use config::*;
pub use error::*;
pub use global::*;
pub use telemetry::{OtelGuard, Recorder};

// Dial9 Tokio runtime telemetry
// Re-export dial9 types at crate root level for easier access
pub use telemetry::dial9;
