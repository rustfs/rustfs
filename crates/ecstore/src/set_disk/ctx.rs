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

//! Borrow context for the `SetDisks` split (tracking #815, phase P0 #816).
//!
//! `SetDisks` stays the single owner of the shared set state — topology/config
//! (immutable after construction), `disks`, the locker trio, and the moka
//! caches. Operation-family service units introduced in later phases borrow
//! that state through [`SetDisksCtx`] instead of copying it, so no state is
//! duplicated as trait impls move out.
//!
//! This module only establishes the borrow handle. It moves no trait impl and
//! changes no runtime behavior.

use super::*;

/// Lightweight, `Copy` handle borrowing the shared [`SetDisks`] core state.
///
/// Accessors return references tied to the borrowed core's lifetime, so an
/// operation-family unit can read topology, disks, and lockers without holding
/// its own copy. Anything not yet exposed through a typed accessor is reachable
/// via [`SetDisksCtx::core`].
#[derive(Clone, Copy)]
pub(crate) struct SetDisksCtx<'a> {
    core: &'a SetDisks,
}

impl<'a> SetDisksCtx<'a> {
    pub(crate) fn new(core: &'a SetDisks) -> Self {
        Self { core }
    }

    /// The borrowed core, for state not yet fronted by a typed accessor.
    pub(crate) fn core(&self) -> &'a SetDisks {
        self.core
    }

    // --- Immutable topology / config (fixed after construction) ---

    pub(crate) fn set_index(&self) -> usize {
        self.core.set_index
    }

    pub(crate) fn pool_index(&self) -> usize {
        self.core.pool_index
    }

    pub(crate) fn set_drive_count(&self) -> usize {
        self.core.set_drive_count
    }

    pub(crate) fn default_parity_count(&self) -> usize {
        self.core.default_parity_count
    }

    pub(crate) fn set_endpoints(&self) -> &'a [Endpoint] {
        &self.core.set_endpoints
    }

    pub(crate) fn format(&self) -> &'a FormatV3 {
        &self.core.format
    }

    pub(crate) fn locker_owner(&self) -> &'a str {
        &self.core.locker_owner
    }

    // --- Shared mutable state (behind their own synchronization) ---

    pub(crate) fn disks(&self) -> &'a Arc<RwLock<Vec<Option<DiskStore>>>> {
        &self.core.disks
    }

    // --- Locker trio ---

    pub(crate) fn lockers(&self) -> &'a [Arc<dyn LockClient>] {
        &self.core.lockers
    }
}

impl SetDisks {
    /// Borrow this set's shared core state through a lightweight handle.
    ///
    /// Foundation for the operation-family split (#815 / #816): service units
    /// take a [`SetDisksCtx`] rather than owning duplicated state.
    pub(crate) fn ctx(&self) -> SetDisksCtx<'_> {
        SetDisksCtx::new(self)
    }
}
