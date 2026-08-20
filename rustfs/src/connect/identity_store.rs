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

//! On-disk home of the device key.
//!
//! A device that loses its key loses the certificate issued for it and has to
//! spend a fresh registration token to get back, so the store is written
//! durably and published exactly once. It deliberately does not reuse
//! `rustfs_kms`'s `durable_file`, which implements the same commit protocol
//! for envelope keys but is `pub(crate)` to that crate and carries KMS error
//! and failpoint types this path has no use for.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use zeroize::Zeroizing;

use super::identity::{DeviceIdentity, IdentityError};

/// Name of the key file inside the store directory.
const KEY_FILE: &str = "device.key";

/// Owner read/write only. The key is the device's whole identity.
#[cfg(unix)]
const KEY_MODE: u32 = 0o600;

/// Distinguishes the staging file of concurrent publishers. The process id
/// alone is not enough: several threads of one process may initialise the same
/// store, and a shared staging name would let them truncate each other's
/// half-written key and then link the result into place.
static STAGING_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("connect identity store I/O failed at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    /// The key file exists but does not decode. Fail closed: regenerating here
    /// would silently abandon a device certificate that is still valid and
    /// still trusted by the control plane.
    #[error("connect device key at {path} is unreadable and was left untouched: {source}")]
    Corrupt {
        path: PathBuf,
        #[source]
        source: IdentityError,
    },

    /// The key file is present with permissions that expose it. Refused rather
    /// than repaired, because a key that has been world-readable has to be
    /// treated as disclosed and rotated, not quietly re-sealed.
    #[cfg(unix)]
    #[error("connect device key at {path} has mode {mode:o}, expected {expected:o}")]
    Permissions { path: PathBuf, mode: u32, expected: u32 },

    #[error(transparent)]
    Identity(#[from] IdentityError),
}

/// A directory holding one device identity.
#[derive(Clone, Debug)]
pub struct IdentityStore {
    directory: PathBuf,
}

impl IdentityStore {
    pub fn new(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
        }
    }

    pub fn key_path(&self) -> PathBuf {
        self.directory.join(KEY_FILE)
    }

    /// Return the stored identity, or `None` when this deployment has never
    /// been enrolled. Reading never creates anything, so an unconfigured
    /// server can ask without acquiring an identity as a side effect.
    pub fn load(&self) -> Result<Option<DeviceIdentity>, StoreError> {
        let path = self.key_path();

        let der = match fs::read(&path) {
            Ok(der) => Zeroizing::new(der),
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(StoreError::Io { path, source }),
        };

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;

            let metadata = fs::metadata(&path).map_err(|source| StoreError::Io {
                path: path.clone(),
                source,
            })?;
            let mode = metadata.permissions().mode() & 0o7777;
            if mode != KEY_MODE {
                return Err(StoreError::Permissions {
                    path,
                    mode,
                    expected: KEY_MODE,
                });
            }
        }

        DeviceIdentity::from_pkcs8_der(&der)
            .map(Some)
            .map_err(|source| StoreError::Corrupt { path, source })
    }

    /// Return the stored identity, generating and publishing one the first
    /// time. Concurrent callers converge on a single identity: publication is
    /// a no-clobber link, and whoever loses the race discards its candidate
    /// and reads the winner's.
    pub fn load_or_create(&self) -> Result<DeviceIdentity, StoreError> {
        if let Some(identity) = self.load()? {
            return Ok(identity);
        }

        fs::create_dir_all(&self.directory).map_err(|source| StoreError::Io {
            path: self.directory.clone(),
            source,
        })?;

        let candidate = DeviceIdentity::generate();
        let der = candidate.to_pkcs8_der()?;

        match self.publish(&der) {
            Ok(()) => Ok(candidate),
            // Another process published first. Its key is the identity; ours
            // was never written anywhere and simply goes out of scope.
            Err(StoreError::Io { source, .. }) if source.kind() == io::ErrorKind::AlreadyExists => {
                self.load()?.ok_or_else(|| StoreError::Io {
                    path: self.key_path(),
                    source: io::Error::new(
                        io::ErrorKind::NotFound,
                        "device key vanished immediately after another writer published it",
                    ),
                })
            }
            Err(error) => Err(error),
        }
    }

    /// Write, seal, fsync, then link into place and fsync the directory. The
    /// key is durable before it is reachable, and it is reachable only once.
    fn publish(&self, der: &[u8]) -> Result<(), StoreError> {
        use std::io::Write as _;

        let final_path = self.key_path();
        let temp_path = self.directory.join(format!(
            "{KEY_FILE}.{}.{}.tmp",
            std::process::id(),
            STAGING_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));

        let io_at = |path: &Path| {
            let path = path.to_path_buf();
            move |source| StoreError::Io { path, source }
        };

        let mut options = fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(KEY_MODE);
        }

        let mut file = options.open(&temp_path).map_err(io_at(&temp_path))?;

        let result = (|| -> Result<(), StoreError> {
            file.write_all(der).map_err(io_at(&temp_path))?;

            // The umask can only narrow the creation mode, so set and verify
            // the exact mode before the bytes become durable.
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;

                file.set_permissions(fs::Permissions::from_mode(KEY_MODE))
                    .map_err(io_at(&temp_path))?;
                let mode = file.metadata().map_err(io_at(&temp_path))?.permissions().mode() & 0o7777;
                if mode != KEY_MODE {
                    return Err(StoreError::Permissions {
                        path: temp_path.clone(),
                        mode,
                        expected: KEY_MODE,
                    });
                }
            }

            file.sync_all().map_err(io_at(&temp_path))?;
            Ok(())
        })();

        drop(file);

        if let Err(error) = result {
            let _ = fs::remove_file(&temp_path);
            return Err(error);
        }

        // `hard_link` fails rather than replacing an existing key, which is
        // what makes a retry return the original identity instead of minting
        // a second one.
        let published = fs::hard_link(&temp_path, &final_path);
        let _ = fs::remove_file(&temp_path);
        published.map_err(io_at(&final_path))?;

        fsync_dir(&self.directory).map_err(io_at(&self.directory))?;

        Ok(())
    }
}

/// Fsync a directory so a freshly linked entry survives power loss. Directories
/// cannot be opened for syncing on Windows, where this is a no-op.
fn fsync_dir(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        fs::File::open(dir)?.sync_all()?;
    }
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}
