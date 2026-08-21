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

use std::fs;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

const CREDENTIAL_FILE: &str = "device.crt.json";
const REGISTRATION_COMPLETED_FILE: &str = "registration.completed.json";
const REGISTRATION_PENDING_FILE: &str = "registration.pending.json";
const ROTATION_PENDING_FILE: &str = "rotation.pending.json";
const LOCK_FILE: &str = ".state.lock";

#[cfg(unix)]
const FILE_MODE: u32 = 0o600;

static STAGING_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceCredential {
    pub name: String,
    pub uid: String,
    pub protocol_version: String,
    pub key_id: String,
    pub certificate_serial: String,
    pub certificate: String,
    pub certificate_chain: String,
    pub not_before_unix: i64,
    pub not_after_unix: i64,
}

impl std::fmt::Debug for DeviceCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeviceCredential")
            .field("name", &self.name)
            .field("key_id", &self.key_id)
            .field("certificate_serial", &self.certificate_serial)
            .field("not_after_unix", &self.not_after_unix)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PendingRegistration {
    pub token_uid: String,
    pub request_id: String,
    pub certificate_request: String,
    #[serde(default)]
    pub previous_credential_fingerprint: Option<String>,
    #[serde(default)]
    pub next_public_key_sha256: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CompletedRegistration {
    pub token_uid: String,
    pub credential_fingerprint: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PendingRotation {
    pub credential_fingerprint: String,
    pub device_name: String,
    pub request_id: String,
    pub certificate_request: String,
    pub next_public_key_sha256: String,
}

#[derive(Debug, thiserror::Error)]
pub enum CredentialStoreError {
    #[error("connect credential store I/O failed at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error("connect credential data at {path} is invalid: {source}")]
    Invalid {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },

    #[cfg(unix)]
    #[error("connect credential file at {path} has mode {mode:o}, expected {expected:o}")]
    Permissions { path: PathBuf, mode: u32, expected: u32 },
}

#[derive(Clone, Debug)]
pub struct CredentialStore {
    directory: PathBuf,
}

pub(crate) struct CredentialLock {
    _file: fs::File,
}

impl CredentialStore {
    pub fn new(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
        }
    }

    pub(crate) fn load(&self) -> Result<Option<DeviceCredential>, CredentialStoreError> {
        self.read(CREDENTIAL_FILE)
    }

    pub(crate) async fn lock(&self) -> Result<CredentialLock, CredentialStoreError> {
        let directory = self.directory.clone();
        tokio::task::spawn_blocking(move || {
            fs::create_dir_all(&directory).map_err(|source| CredentialStoreError::Io {
                path: directory.clone(),
                source,
            })?;
            let path = directory.join(LOCK_FILE);
            let mut options = fs::OpenOptions::new();
            options.create(true).truncate(false).read(true).write(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt as _;
                options.mode(FILE_MODE);
            }
            let file = options.open(&path).map_err(|source| CredentialStoreError::Io {
                path: path.clone(),
                source,
            })?;
            check_mode(&path)?;
            file.lock().map_err(|source| CredentialStoreError::Io { path, source })?;
            Ok(CredentialLock { _file: file })
        })
        .await
        .map_err(|source| CredentialStoreError::Io {
            path: self.directory.join(LOCK_FILE),
            source: io::Error::other(source),
        })?
    }

    pub(crate) fn save(&self, credential: &DeviceCredential) -> Result<(), CredentialStoreError> {
        self.write(CREDENTIAL_FILE, credential)
    }

    pub(crate) fn claim_pending_registration(
        &self,
        pending: &PendingRegistration,
    ) -> Result<PendingRegistration, CredentialStoreError> {
        self.claim(REGISTRATION_PENDING_FILE, pending)
    }

    pub(crate) fn load_pending_registration(&self) -> Result<Option<PendingRegistration>, CredentialStoreError> {
        self.read(REGISTRATION_PENDING_FILE)
    }

    pub(crate) fn clear_pending_registration(&self) -> Result<(), CredentialStoreError> {
        self.remove(REGISTRATION_PENDING_FILE)
    }

    pub(crate) fn load_completed_registration(&self) -> Result<Option<CompletedRegistration>, CredentialStoreError> {
        self.read(REGISTRATION_COMPLETED_FILE)
    }

    pub(crate) fn save_completed_registration(&self, completed: &CompletedRegistration) -> Result<(), CredentialStoreError> {
        self.write(REGISTRATION_COMPLETED_FILE, completed)
    }

    pub(crate) fn load_pending_rotation(&self) -> Result<Option<PendingRotation>, CredentialStoreError> {
        self.read(ROTATION_PENDING_FILE)
    }

    pub(crate) fn claim_pending_rotation(&self, pending: &PendingRotation) -> Result<PendingRotation, CredentialStoreError> {
        self.claim(ROTATION_PENDING_FILE, pending)
    }

    pub(crate) fn clear_pending_rotation(&self) -> Result<(), CredentialStoreError> {
        self.remove(ROTATION_PENDING_FILE)
    }

    fn read<T: for<'de> Deserialize<'de>>(&self, file: &str) -> Result<Option<T>, CredentialStoreError> {
        let path = self.directory.join(file);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(CredentialStoreError::Io { path, source }),
        };

        check_mode(&path)?;
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|source| CredentialStoreError::Invalid { path, source })
    }

    fn write<T: Serialize>(&self, file: &str, value: &T) -> Result<(), CredentialStoreError> {
        let bytes = serde_json::to_vec(value).map_err(|source| CredentialStoreError::Invalid {
            path: self.directory.join(file),
            source,
        })?;

        fs::create_dir_all(&self.directory).map_err(|source| CredentialStoreError::Io {
            path: self.directory.clone(),
            source,
        })?;

        let final_path = self.directory.join(file);
        let temp_path = self.stage(file, &bytes)?;
        let result = fs::rename(&temp_path, &final_path)
            .map_err(|source| CredentialStoreError::Io {
                path: final_path,
                source,
            })
            .and_then(|()| {
                fsync_dir(&self.directory).map_err(|source| CredentialStoreError::Io {
                    path: self.directory.clone(),
                    source,
                })
            });

        if result.is_err() {
            let _ = fs::remove_file(&temp_path);
        }
        result
    }

    fn claim<T>(&self, file: &str, value: &T) -> Result<T, CredentialStoreError>
    where
        T: Clone + Serialize + for<'de> Deserialize<'de>,
    {
        if let Some(existing) = self.read(file)? {
            return Ok(existing);
        }
        let bytes = serde_json::to_vec(value).map_err(|source| CredentialStoreError::Invalid {
            path: self.directory.join(file),
            source,
        })?;
        fs::create_dir_all(&self.directory).map_err(|source| CredentialStoreError::Io {
            path: self.directory.clone(),
            source,
        })?;
        let final_path = self.directory.join(file);
        let temp_path = self.stage(file, &bytes)?;
        let published = fs::hard_link(&temp_path, &final_path);
        let _ = fs::remove_file(&temp_path);
        match published {
            Ok(()) => {
                fsync_dir(&self.directory).map_err(|source| CredentialStoreError::Io {
                    path: self.directory.clone(),
                    source,
                })?;
                Ok(value.clone())
            }
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
                fsync_dir(&self.directory).map_err(|source| CredentialStoreError::Io {
                    path: self.directory.clone(),
                    source,
                })?;
                self.read(file)?.ok_or_else(|| CredentialStoreError::Io {
                    path: final_path,
                    source: io::Error::new(io::ErrorKind::NotFound, "pending state vanished after publication"),
                })
            }
            Err(source) => Err(CredentialStoreError::Io {
                path: final_path,
                source,
            }),
        }
    }

    fn stage(&self, file: &str, bytes: &[u8]) -> Result<PathBuf, CredentialStoreError> {
        loop {
            let path = self.directory.join(format!(
                ".{file}.{}.{}.tmp",
                std::process::id(),
                STAGING_SEQUENCE.fetch_add(1, Ordering::Relaxed)
            ));
            let mut options = fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt as _;
                options.mode(FILE_MODE);
            }
            let mut staging = match options.open(&path) {
                Ok(staging) => staging,
                Err(source) if source.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(source) => return Err(CredentialStoreError::Io { path, source }),
            };
            let result = staging
                .write_all(bytes)
                .and_then(|()| staging.sync_all())
                .map_err(|source| CredentialStoreError::Io {
                    path: path.clone(),
                    source,
                })
                .and_then(|()| check_mode(&path));
            if let Err(error) = result {
                let _ = fs::remove_file(&path);
                return Err(error);
            }
            return Ok(path);
        }
    }

    fn remove(&self, file: &str) -> Result<(), CredentialStoreError> {
        let path = self.directory.join(file);
        match fs::remove_file(&path) {
            Ok(()) => fsync_dir(&self.directory).map_err(|source| CredentialStoreError::Io {
                path: self.directory.clone(),
                source,
            }),
            Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(CredentialStoreError::Io { path, source }),
        }
    }
}

#[cfg(unix)]
fn check_mode(path: &Path) -> Result<(), CredentialStoreError> {
    use std::os::unix::fs::PermissionsExt as _;

    let mode = fs::metadata(path)
        .map_err(|source| CredentialStoreError::Io {
            path: path.to_path_buf(),
            source,
        })?
        .permissions()
        .mode()
        & 0o7777;
    if mode != FILE_MODE {
        return Err(CredentialStoreError::Permissions {
            path: path.to_path_buf(),
            mode,
            expected: FILE_MODE,
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_mode(_path: &Path) -> Result<(), CredentialStoreError> {
    Ok(())
}

fn fsync_dir(directory: &Path) -> io::Result<()> {
    #[cfg(unix)]
    fs::File::open(directory)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = directory;
    Ok(())
}
