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

//! # Capacity Management Module
//!
//! This module provides hybrid capacity management for RustFS with:
//! - Scheduled background updates (configurable interval)
//! - Write-triggered updates for high-frequency write scenarios
//! - Configurable caching thresholds and smart update strategies
//! - Capacity metrics emitted through `rustfs-io-metrics`
//!
//! ## Configuration
//!
//! All configuration is via environment variables (see `rustfs_config`):
//! - `RUSTFS_CAPACITY_SCHEDULED_INTERVAL` - Update interval in seconds (default: 120)
//! - `RUSTFS_CAPACITY_WRITE_TRIGGER_DELAY` - Write trigger delay (default: 5s)
//! - `RUSTFS_CAPACITY_WRITE_FREQUENCY_THRESHOLD` - Write frequency threshold (default: 5 writes/min)
//! - `RUSTFS_CAPACITY_FAST_UPDATE_THRESHOLD` - Fast update threshold (default: 30s)
//! - `RUSTFS_CAPACITY_MAX_FILES_THRESHOLD` - Max files before sampling (default: 200,000)
//! - `RUSTFS_CAPACITY_STAT_TIMEOUT` - Stat operation timeout (default: 3s)
//! - `RUSTFS_CAPACITY_SAMPLE_RATE` - Sampling rate for metrics (default: 200)
//! - `RUSTFS_CAPACITY_FOLLOW_SYMLINKS` - Follow symlinks during traversal (default: false)
//! - `RUSTFS_CAPACITY_MAX_SYMLINK_DEPTH` - Max symlink depth (default: 3)
//! - `RUSTFS_CAPACITY_ENABLE_DYNAMIC_TIMEOUT` - Enable dynamic timeout (default: true)
//! - `RUSTFS_CAPACITY_MIN_TIMEOUT` - Minimum timeout (default: 2s)
//! - `RUSTFS_CAPACITY_MAX_TIMEOUT` - Maximum timeout (default: 15s)
//! - `RUSTFS_CAPACITY_STALL_TIMEOUT` - Stall detection timeout (default: 20s)
//!
//! ## Architecture
//!
//! The capacity management system uses a hybrid strategy:
//! 1. **Real-time updates**: Triggered by write operations above threshold
//! 2. **Scheduled updates**: Periodic background updates
//! 3. **Cached responses**: Returns cached data when fresh
//! 4. **Timeout protection**: Dynamic timeouts prevent hangs on large directories
//!
//! Capacity metrics flow through the existing observability pipeline via the `metrics`
//! crate and `rustfs-io-metrics`; this module does not expose a Prometheus HTTP endpoint.
//!
//! ## Testing
//!
//! For isolated tests, use `create_isolated_manager()` to create independent
//! instances instead of the global singleton:
//!
//! ```ignore
//! use crate::capacity::create_isolated_manager;
//!
//! let manager = create_isolated_manager(HybridStrategyConfig::default());
//! // Test without affecting global state
//! ```
//!

pub mod capacity_integration;
pub mod capacity_manager;
#[cfg(test)]
mod capacity_manager_test;
#[cfg(test)]
mod write_trigger_test;
