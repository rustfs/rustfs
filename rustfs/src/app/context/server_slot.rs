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
//! Some legacy, non-request call sites still use the ambient compatibility
//! resolver. Request dispatch must instead use the strict installed-context
//! accessors: once a request carries a server slot, an empty slot means that
//! server is not ready rather than that another server's global context applies.

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

    /// This server's installed application context, if startup has completed.
    ///
    /// Request-scoped resolution must use this accessor so an empty slot never
    /// resolves through another server's ambient context.
    pub fn installed_app_context(&self) -> Option<Arc<AppContext>> {
        self.app_context.get().cloned()
    }

    /// This server's installed object store, if startup has completed.
    pub fn installed_object_store(&self) -> Option<Arc<ECStore>> {
        self.installed_app_context().map(|context| context.object_store())
    }

    /// This server's application context: the installed one, or the
    /// process-global singleton as the legacy compatibility default.
    ///
    /// This accessor is for callers without request-slot semantics. Request
    /// dispatch must use [`Self::installed_app_context`] instead.
    pub fn app_context(&self) -> Option<Arc<AppContext>> {
        self.installed_app_context().or_else(get_global_app_context)
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
    fn empty_slot_retains_ambient_compatibility() {
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
