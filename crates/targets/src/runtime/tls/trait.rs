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

//! The `ReloadableTargetTls` trait — the public protocol each TLS-capable
//! target implements to participate in coordinated hot-reload.

use crate::error::TargetError;
use async_trait::async_trait;
use std::sync::Arc;

use super::config::ReloadApplyMode;
use super::fingerprint::TargetTlsGeneration;
use super::state::TargetTlsInputSet;

/// Protocol that each TLS-capable target implements so the reload coordinator
/// can drive certificate hot-reload without knowing the target's internals.
///
/// The target is responsible for:
/// - Declaring which TLS files it reads (`tls_input_set`)
/// - Building a new client/pool/connector from current files (`build_tls_material`)
/// - Atomically swapping the active connection state (`apply_tls_material`)
///
/// The coordinator is responsible for:
/// - Deciding *when* to check
/// - Detecting *whether* material changed
/// - Ensuring *safety* (validate, build-then-apply, fallback on failure)
#[async_trait]
pub trait ReloadableTargetTls: Send + Sync + 'static {
    /// The rebuilt connection/client/pool object this target uses.
    type Material: Send + Sync + 'static;

    /// Returns the TLS file paths this target reads.
    fn tls_input_set(&self) -> TargetTlsInputSet;

    /// Build a fresh TLS material object from current files on disk.
    ///
    /// Called by the coordinator on the reload path only — never on the send hot path.
    async fn build_tls_material(&self) -> Result<Self::Material, TargetError>;

    /// Atomically apply new TLS material, replacing the current active connection state.
    ///
    /// On success, the target's internal state must point to the new material.
    /// On failure, the target must keep its current state unchanged.
    async fn apply_tls_material(
        &self,
        generation: TargetTlsGeneration,
        material: Arc<Self::Material>,
        mode: ReloadApplyMode,
    ) -> Result<(), TargetError>;

    /// Optional pre-check: validate that TLS files on disk are self-consistent
    /// (cert/key pair parseable, CA loadable) before attempting `build_tls_material`.
    /// Default implementation returns `Ok(())`.
    async fn validate_tls_files(&self) -> Result<(), TargetError> {
        Ok(())
    }
}
