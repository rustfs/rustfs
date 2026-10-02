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
#[cfg(unix)]
use std::{
    fs, io,
    os::unix::fs::{DirBuilderExt as _, MetadataExt as _, PermissionsExt as _},
};

use super::super::identity::DeviceIdentity;
use super::super::identity_store::{IdentityStore, StoreError};

/// Subdirectory holding the offline enrolment key, kept apart from the online
/// device identity so neither can be read in place of the other.
const OFFLINE_DIRECTORY: &str = "offline";

/// The offline enrolment key of one deployment.
#[derive(Clone, Debug)]
pub struct OfflineKeyStore {
    #[cfg(unix)]
    state_root: PathBuf,
    inner: IdentityStore,
}

impl OfflineKeyStore {
    pub fn new(directory: impl AsRef<Path>) -> Self {
        let state_root = directory.as_ref().to_path_buf();
        Self {
            inner: IdentityStore::new(state_root.join(OFFLINE_DIRECTORY)),
            #[cfg(unix)]
            state_root,
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
    /// Connect pinned. The state root must also satisfy the service IPC's
    /// owner-only 0700 boundary; an existing unsafe root is never repaired here.
    pub fn load_or_create(&self) -> Result<DeviceIdentity, StoreError> {
        #[cfg(unix)]
        ensure_private_state_root(&self.state_root)?;
        self.inner.load_or_create()
    }
}

#[cfg(unix)]
fn ensure_private_state_root(path: &Path) -> Result<(), StoreError> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true).mode(0o700);
    builder.create(path).map_err(|source| StoreError::Io {
        path: path.to_path_buf(),
        source,
    })?;

    let metadata = fs::symlink_metadata(path).map_err(|source| StoreError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.uid() != process_uid()
        || metadata.permissions().mode() & 0o7777 != 0o700
    {
        return Err(StoreError::Io {
            path: path.to_path_buf(),
            source: io::Error::new(
                io::ErrorKind::PermissionDenied,
                "offline state root must be a real owner-owned directory with mode 0700",
            ),
        });
    }
    Ok(())
}

#[cfg(unix)]
#[allow(unsafe_code)]
fn process_uid() -> u32 {
    // SAFETY: geteuid has no pointer arguments or caller preconditions.
    unsafe { libc::geteuid() }
}

#[cfg(all(test, unix))]
mod tests {
    use std::{
        fs,
        os::unix::fs::{PermissionsExt as _, symlink},
    };

    use super::OfflineKeyStore;

    #[test]
    fn first_enrollment_creates_private_state_root() {
        let temporary = tempfile::tempdir().unwrap();
        let state = temporary.path().join("connect");
        let store = OfflineKeyStore::new(&state);

        store.load_or_create().unwrap();

        assert_eq!(fs::metadata(&state).unwrap().permissions().mode() & 0o7777, 0o700);
        assert_eq!(fs::metadata(store.key_path()).unwrap().permissions().mode() & 0o7777, 0o600);
    }

    #[test]
    fn existing_public_state_root_is_rejected_without_creating_or_replacing_a_key() {
        let temporary = tempfile::tempdir().unwrap();
        let state = temporary.path().join("connect");
        fs::create_dir(&state).unwrap();
        fs::set_permissions(&state, fs::Permissions::from_mode(0o755)).unwrap();
        let store = OfflineKeyStore::new(&state);

        let error = store.load_or_create().expect_err("public state root must fail closed");
        assert!(error.to_string().contains("mode 0700"));
        assert!(!store.key_path().exists());
        assert_eq!(fs::metadata(&state).unwrap().permissions().mode() & 0o7777, 0o755);

        fs::set_permissions(&state, fs::Permissions::from_mode(0o700)).unwrap();
        store.load_or_create().unwrap();
        let original = fs::read(store.key_path()).unwrap();
        fs::set_permissions(&state, fs::Permissions::from_mode(0o755)).unwrap();

        assert!(store.load_or_create().is_err());
        assert_eq!(fs::read(store.key_path()).unwrap(), original);
        assert_eq!(fs::metadata(&state).unwrap().permissions().mode() & 0o7777, 0o755);
    }

    #[test]
    fn symbolic_state_root_is_rejected_before_key_generation() {
        let temporary = tempfile::tempdir().unwrap();
        let target = temporary.path().join("target");
        fs::create_dir(&target).unwrap();
        fs::set_permissions(&target, fs::Permissions::from_mode(0o700)).unwrap();
        let state = temporary.path().join("connect");
        symlink(&target, &state).unwrap();
        let store = OfflineKeyStore::new(&state);

        assert!(store.load_or_create().is_err());
        assert!(!target.join("offline/device.key").exists());
    }
}
