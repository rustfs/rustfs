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

//! On-disk home of the offline enrollment key.
//!
//! An air-gapped device enrols with a key that is not its online device
//! identity: the online key is minted during a registration exchange this
//! device cannot perform, and an operator who carries an enrolment response out
//! on removable media is enrolling exactly one key that Connect will pin. Losing
//! it means asking for a fresh challenge, so it is written durably and published
//! exactly once.
//!
//! The durability protocol is not reimplemented here. [`IdentityStore`] already
//! seals a P-256 key at mode 0600, fsyncs it, and publishes it through a
//! no-clobber link so a retry or a concurrent start converges on one key; it is
//! pointed at a directory of this key's own rather than generalised into a
//! key-store abstraction that would have to describe both lifecycles.

use std::path::{Path, PathBuf};

use super::super::identity::DeviceIdentity;
use super::super::identity_store::{IdentityStore, StoreError};

/// Subdirectory holding the offline enrolment key, kept apart from the online
/// device identity so neither can be read in place of the other.
const OFFLINE_DIRECTORY: &str = "offline";

/// The offline enrolment key of one deployment.
#[derive(Clone, Debug)]
pub struct OfflineKeyStore {
    inner: IdentityStore,
}

impl OfflineKeyStore {
    pub fn new(directory: impl AsRef<Path>) -> Self {
        Self {
            inner: IdentityStore::new(directory.as_ref().join(OFFLINE_DIRECTORY)),
        }
    }

    pub fn key_path(&self) -> PathBuf {
        self.inner.key_path()
    }

    /// Return the stored key, or `None` when this deployment has never enrolled
    /// offline. Reading never creates one, so a deployment that only ever
    /// registers online holds no offline key.
    pub fn load(&self) -> Result<Option<DeviceIdentity>, StoreError> {
        self.inner.load()
    }

    /// Return the stored key, generating and publishing one the first time.
    ///
    /// A second enrolment attempt returns the original key rather than minting a
    /// replacement: the operator may already be carrying a response for it, and
    /// two keys would mean the response and the device disagree about which one
    /// Connect pinned.
    pub fn load_or_create(&self) -> Result<DeviceIdentity, StoreError> {
        self.inner.load_or_create()
    }
}
