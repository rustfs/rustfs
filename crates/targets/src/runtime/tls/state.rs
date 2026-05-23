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

//! Per-target TLS reload runtime state with atomic timestamps and error tracking.

use super::fingerprint::{TargetTlsFingerprint, TargetTlsGeneration};
use ::arc_swap::ArcSwap;
use serde::Serialize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Describes which TLS files a target reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetTlsInputSet {
    pub ca_path: String,
    pub client_cert_path: String,
    pub client_key_path: String,
    /// Human-readable label for logging and metrics (e.g. "webhook:primary").
    pub target_label: String,
}

impl TargetTlsInputSet {
    /// Returns `true` when no TLS paths are configured (no CA, cert, or key).
    pub fn is_empty(&self) -> bool {
        self.ca_path.is_empty() && self.client_cert_path.is_empty() && self.client_key_path.is_empty()
    }
}

/// Immutable snapshot of a successfully published TLS material generation.
pub struct TargetTlsPublishedState<M> {
    pub generation: TargetTlsGeneration,
    pub fingerprint: TargetTlsFingerprint,
    pub material: Arc<M>,
    pub loaded_at_unix_ms: u64,
}

/// Per-target TLS reload runtime state. Owns the current published material
/// and tracks timestamps and the last error for observability.
pub struct TargetTlsRuntimeState<M> {
    /// The currently active TLS material generation.
    pub current: ArcSwap<TargetTlsPublishedState<M>>,
    /// The last known-good generation (never overwritten by a failed reload).
    pub last_good: ArcSwap<TargetTlsPublishedState<M>>,
    /// Unix-millis timestamp of the last reload *attempt* (success or failure).
    pub last_attempt_unix_ms: AtomicU64,
    /// Unix-millis timestamp of the last *successful* reload.
    pub last_success_unix_ms: AtomicU64,
    /// Last reload error message, if any.
    pub last_error: parking_lot::RwLock<Option<String>>,
    /// The TLS file paths this state watches.
    pub inputs: TargetTlsInputSet,
}

impl<M> TargetTlsRuntimeState<M> {
    /// Creates a new runtime state with the given initial published state.
    pub fn new(initial: Arc<TargetTlsPublishedState<M>>, inputs: TargetTlsInputSet) -> Self {
        Self {
            current: ArcSwap::from(initial.clone()),
            last_good: ArcSwap::from(initial),
            last_attempt_unix_ms: AtomicU64::new(0),
            last_success_unix_ms: AtomicU64::new(0),
            last_error: parking_lot::RwLock::new(None),
            inputs,
        }
    }

    /// Returns the generation of the currently active material.
    pub fn current_generation(&self) -> TargetTlsGeneration {
        self.current.load().generation
    }

    /// Atomically bumps and returns the next generation.
    pub fn bump_generation(&self) -> TargetTlsGeneration {
        // Load the current generation from the arc-swap, compute next,
        // and return it. The caller is responsible for publishing the new state.
        let current = self.current.load();
        TargetTlsGeneration(current.generation.0.saturating_add(1))
    }

    /// Records the timestamp of a reload attempt.
    pub fn mark_attempt(&self, unix_ms: u64) {
        self.last_attempt_unix_ms.store(unix_ms, Ordering::Release);
    }

    /// Records the timestamp of a successful reload.
    pub fn mark_success(&self, unix_ms: u64) {
        self.last_success_unix_ms.store(unix_ms, Ordering::Release);
    }

    /// Returns the last attempt timestamp.
    pub fn last_attempt_unix_ms(&self) -> u64 {
        self.last_attempt_unix_ms.load(Ordering::Acquire)
    }

    /// Returns the last success timestamp.
    pub fn last_success_unix_ms(&self) -> u64 {
        self.last_success_unix_ms.load(Ordering::Acquire)
    }
}

/// Read-only status snapshot for admin/debug visibility.
#[derive(Debug, Clone, Serialize)]
pub struct TargetTlsStatusSnapshot {
    pub target_label: String,
    pub generation: u64,
    pub reload_enabled: bool,
    pub detect_mode: &'static str,
    pub apply_mode: &'static str,
    pub last_attempt_time: Option<u64>,
    pub last_success_time: Option<u64>,
    pub last_error: Option<String>,
    /// TLS file paths this target watches (for admin diagnostics).
    pub ca_path: String,
    pub client_cert_path: String,
    pub client_key_path: String,
}
