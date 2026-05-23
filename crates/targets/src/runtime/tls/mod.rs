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

//! Unified TLS hot-reload infrastructure for notification targets.
//!
//! This module provides:
//! - Fingerprint-based change detection (`fingerprint`)
//! - Per-target reload configuration (`config`)
//! - Runtime state tracking with atomic timestamps (`state`)
//! - The `ReloadableTargetTls` trait protocol (`trait`)
//! - TLS material validation helpers (`validate`)
//! - The reload coordinator with background poll loops (`coordinator`)
//! - Target-level reload metrics (`metrics`)

pub mod config;
pub mod coordinator;
pub mod fingerprint;
pub mod metrics;
pub mod state;
pub mod r#trait;
pub mod validate;

pub use coordinator::TargetTlsReloadCoordinator;
pub use fingerprint::{
    TargetTlsFingerprint, TargetTlsGeneration, TargetTlsState, build_target_tls_fingerprint, refresh_tls_fingerprint_state,
};
pub use metrics::init_target_tls_metrics;
pub use state::{TargetTlsInputSet, TargetTlsPublishedState, TargetTlsRuntimeState, TargetTlsStatusSnapshot};
pub use r#trait::ReloadableTargetTls;
pub use validate::validate_tls_material;
