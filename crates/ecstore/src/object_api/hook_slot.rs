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

//! A process-wide, re-registrable slot for a single `Arc` hook.
//!
//! The GET body cache hook and the object mutation hook both need the same
//! shape: one global slot that starts empty, is (re-)registered from the app
//! layer at startup, is read on the hot/delete paths, and warns when a
//! re-registration swaps in a *different* instance (an unexpected re-init that
//! orphans the previous adapter's cache). This type holds that logic once so
//! the two callers cannot drift apart.

use std::sync::{Arc, RwLock};

/// A global slot holding at most one `Arc<T>` hook.
///
/// Registration atomically swaps the stored hook. Poisoned locks recover in
/// place: a panicked writer cannot leave an `Option<Arc<_>>` in an unsound
/// state, so there is nothing to salvage.
pub(crate) struct HookSlot<T: ?Sized> {
    inner: RwLock<Option<Arc<T>>>,
}

impl<T: ?Sized> HookSlot<T> {
    /// Creates an empty slot. `const` so it can initialize a `static`.
    pub(crate) const fn new() -> Self {
        Self {
            inner: RwLock::new(None),
        }
    }

    /// Registers (or re-registers) the hook.
    ///
    /// Replacing a *different* instance logs `warn_on_replace` at WARN: in
    /// production a hook is installed exactly once per process, so a swap to a
    /// distinct instance signals an unexpected re-init that leaves the previous
    /// adapter's cache unreachable.
    pub(crate) fn register(&self, hook: Arc<T>, warn_on_replace: &str) {
        let mut slot = self.inner.write().unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(previous) = slot.as_ref()
            && !Arc::ptr_eq(previous, &hook)
        {
            tracing::warn!("{warn_on_replace}");
        }
        *slot = Some(hook);
    }

    /// Returns the registered hook, if any.
    pub(crate) fn get(&self) -> Option<Arc<T>> {
        self.inner.read().unwrap_or_else(|poisoned| poisoned.into_inner()).clone()
    }

    /// Clears the slot during feature disable or test cleanup.
    pub(crate) fn clear(&self) {
        *self.inner.write().unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }
}

#[cfg(test)]
mod tests {
    use super::HookSlot;
    use std::sync::Arc;

    trait Marker: Send + Sync {
        fn id(&self) -> u32;
    }
    struct Impl(u32);
    impl Marker for Impl {
        fn id(&self) -> u32 {
            self.0
        }
    }

    #[test]
    fn empty_slot_reads_none() {
        let slot: HookSlot<dyn Marker> = HookSlot::new();
        assert!(slot.get().is_none());
    }

    #[test]
    fn register_then_get_returns_the_hook() {
        let slot: HookSlot<dyn Marker> = HookSlot::new();
        slot.register(Arc::new(Impl(7)), "unused");
        assert_eq!(slot.get().map(|h| h.id()), Some(7));
    }

    #[test]
    fn re_registration_swaps_to_the_latest_instance() {
        // The load-bearing #1126 guarantee: the newest registration wins, so a
        // rebuilt AppContext leaves ecstore pointed at the current adapter
        // rather than a stranded first-wins one.
        let slot: HookSlot<dyn Marker> = HookSlot::new();
        slot.register(Arc::new(Impl(1)), "unused");
        slot.register(Arc::new(Impl(2)), "unused");
        assert_eq!(slot.get().map(|h| h.id()), Some(2));
    }

    #[test]
    fn re_registering_the_same_arc_is_idempotent() {
        let slot: HookSlot<dyn Marker> = HookSlot::new();
        let hook: Arc<dyn Marker> = Arc::new(Impl(9));
        slot.register(Arc::clone(&hook), "unused");
        slot.register(hook, "unused");
        assert_eq!(slot.get().map(|h| h.id()), Some(9));
    }

    #[test]
    fn clear_empties_the_slot() {
        let slot: HookSlot<dyn Marker> = HookSlot::new();
        slot.register(Arc::new(Impl(3)), "unused");
        slot.clear();
        assert!(slot.get().is_none());
    }
}
