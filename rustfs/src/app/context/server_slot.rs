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

//! Per-server application-context slot (backlog#1052 S2).
//!
//! The S3 service starts serving before storage and IAM finish initializing,
//! so a server cannot hold its `AppContext` at construction time. This slot is
//! the late-bound carrier: startup creates one per server, hands it to the
//! request-path owners (the `FS` service), and installs the `AppContext` into
//! it once IAM bootstrap completes.
//!
//! Until every app subsystem is per-server (backlog#1052 S3), resolution falls
//! back to the process-global `AppContext` singleton when the slot has not
//! been installed — byte-for-byte the ambient resolution the request path used
//! before this seam existed. The fallback is removed when multi-instance flips
//! on (backlog#1052 S5).

use super::global::{AppContext, get_global_app_context};
use crate::app::storage_api::context::ECStore;
use std::sync::{Arc, OnceLock};

/// Late-bound, per-server handle to the application context.
#[derive(Default)]
pub struct ServerContextSlot {
    app_context: OnceLock<Arc<AppContext>>,
    heal_topology_fingerprint: Arc<tokio::sync::OnceCell<String>>,
}

// Manual Debug: AppContext is not Debug, so summarize installation state.
impl std::fmt::Debug for ServerContextSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerContextSlot")
            .field("app_context_installed", &self.app_context.get().is_some())
            .field("heal_topology_fingerprint_initialized", &self.heal_topology_fingerprint.get().is_some())
            .finish()
    }
}

impl ServerContextSlot {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            app_context: OnceLock::new(),
            heal_topology_fingerprint: Arc::new(tokio::sync::OnceCell::new()),
        })
    }

    /// Install this server's application context (once). Returns `false` if
    /// the slot was already installed; the first installation wins, matching
    /// the process-global singleton's `get_or_init` semantics.
    pub fn install(&self, context: Arc<AppContext>) -> bool {
        self.app_context.set(context).is_ok()
    }

    /// This server's application context: the installed one, or the
    /// process-global singleton as the single-instance legacy default.
    pub fn app_context(&self) -> Option<Arc<AppContext>> {
        self.app_context.get().cloned().or_else(get_global_app_context)
    }

    /// This server's object store, resolved through [`Self::app_context`].
    pub fn object_store(&self) -> Option<Arc<ECStore>> {
        self.app_context().map(|context| context.object_store())
    }

    pub fn heal_topology_fingerprint(&self) -> Arc<tokio::sync::OnceCell<String>> {
        Arc::clone(&self.heal_topology_fingerprint)
    }
}

#[cfg(test)]
mod tests {
    use super::{ServerContextSlot, get_global_app_context};
    use std::sync::Arc;

    // Before anything is installed — and with no global AppContext in this
    // process (nextest runs each test in its own process) — resolution yields
    // nothing: the same "not ready yet" answer the ambient path gives before
    // IAM bootstrap completes.
    #[test]
    fn empty_slot_resolves_like_the_ambient_path() {
        let slot = ServerContextSlot::new();
        assert_eq!(slot.app_context().is_some(), get_global_app_context().is_some());
        assert_eq!(slot.object_store().is_some(), get_global_app_context().is_some());
    }

    #[test]
    fn heal_topology_cache_is_owned_by_each_server_slot() {
        let first = ServerContextSlot::new();
        let second = ServerContextSlot::new();

        assert!(!Arc::ptr_eq(&first.heal_topology_fingerprint(), &second.heal_topology_fingerprint()));
    }
}
