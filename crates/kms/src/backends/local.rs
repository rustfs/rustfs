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

//! Local file-based KMS backend implementation

use crate::backends::{BackendCapabilities, ExpiredKeyRemoval, KmsBackend, StateGatedOperation, ensure_key_status_permits};
use crate::config::KmsConfig;
use crate::config::LocalConfig;
use crate::encryption::{AesDekCrypto, DataKeyEnvelope, DekCrypto, generate_key_material};
use crate::error::{KmsError, Result};
use crate::types::*;
use aes_gcm::{
    Aes256Gcm, Key, Nonce,
    aead::{Aead, KeyInit},
};
use argon2::{Algorithm, Argon2, Params, Version};
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use jiff::Zoned;
use rand::RngExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::fs;
use tracing::{debug, warn};
use zeroize::Zeroizing;

/// Reject key identifiers that would not name a single file directly inside the key
/// directory.
///
/// The rule is containment, not a character allowlist: anything that stays inside
/// `key_dir` is accepted, so identifiers already in use by existing deployments keep
/// resolving. Only separators, traversal and the degenerate cases are refused, which is
/// what stops `key_dir.join(...)` from escaping.
fn validate_key_id(key_id: &str) -> Result<()> {
    if key_id.is_empty() {
        return Err(KmsError::invalid_key("key identifier must not be empty"));
    }
    if key_id.contains('/') || key_id.contains('\\') || key_id.contains('\0') {
        return Err(KmsError::invalid_key(format!(
            "key identifier must not contain path separators or NUL: {key_id:?}"
        )));
    }

    // Catches `.`, `..`, absolute paths, and platform-specific forms such as Windows
    // drive prefixes, all of which would move the join outside key_dir.
    let file_name = format!("{key_id}.key");
    let mut components = Path::new(&file_name).components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(_)), None) => Ok(()),
        _ => Err(KmsError::invalid_key(format!(
            "key identifier must name a single file inside the key directory: {key_id:?}"
        ))),
    }
}

const LOCAL_KMS_MASTER_KEY_SALT_FILE: &str = ".master-key.salt";
// The KDF parameters are pub(crate) so the backup manifest contract
// (`crate::backup`) records the exact compiled-in derivation instead of a
// copy that could drift.
pub(crate) const LOCAL_KMS_MASTER_KEY_SALT_LEN: usize = 16;
pub(crate) const LOCAL_KMS_MASTER_KEY_LEN: usize = 32;
pub(crate) const LOCAL_KMS_ARGON2_M_COST_KIB: u32 = 19 * 1024;
pub(crate) const LOCAL_KMS_ARGON2_T_COST: u32 = 2;
pub(crate) const LOCAL_KMS_ARGON2_P_COST: u32 = 1;

/// Strict matcher for leftover commit temp files (`<prefix>.tmp-<uuid>`).
///
/// Both temp shapes ever produced by this backend are covered: key temps
/// `<stem>.tmp-<uuid>` (`with_extension` replaced the `.key` suffix) and salt
/// temps `.master-key.salt.tmp-<uuid>` (suffix appended to the full name).
/// Published key files always end in `.key` — even a key literally named
/// `foo.tmp-<uuid>` is stored as `foo.tmp-<uuid>.key` — so the `.key` guard
/// plus the exact hyphenated-UUID check makes it impossible to match an
/// authoritative file.
fn is_orphan_commit_temp_name(file_name: &str) -> bool {
    if file_name.ends_with(".key") {
        return false;
    }
    let Some((prefix, suffix)) = file_name.rsplit_once(".tmp-") else {
        return false;
    };
    !prefix.is_empty() && suffix.len() == 36 && uuid::Uuid::try_parse(suffix).is_ok()
}

/// Durable single-file commit protocol for the key directory.
///
/// Key material and metadata are unrecoverable state, so every mutation of the
/// key directory must survive a crash or power loss at any point. All writers
/// share one protocol: exclusively create a temp file in the destination
/// directory, write and fsync the content, publish it atomically (`rename` to
/// replace, `hard_link` to create without clobbering) and fsync the parent
/// directory so the new directory entry itself is durable. Deletion mirrors
/// the tail of the protocol (`remove_file` + parent directory fsync) so a
/// removed key cannot resurface after power loss.
///
/// This intentionally mirrors ecstore's fsync helpers without depending on the
/// ecstore crate: the KMS backend stays decoupled from storage internals.
mod durable_file {
    use std::io::{self, Write};
    use std::path::{Path, PathBuf};

    /// How the fully written temp file becomes visible under its final name.
    pub(super) enum Publish {
        /// Atomically replace whatever is at the destination via `rename`.
        Replace,
        /// Publish via `hard_link`, failing with [`CommitError::AlreadyExists`]
        /// when the destination exists so concurrent creates stay linearized.
        NoClobber,
    }

    #[derive(Debug)]
    pub(super) enum CommitError {
        AlreadyExists,
        Io(io::Error),
        /// Test-only simulated crash: the protocol stops after the given step
        /// with no cleanup, exactly as a power loss would.
        #[cfg(test)]
        InjectedCrash(CommitStep),
    }

    impl From<io::Error> for CommitError {
        fn from(error: io::Error) -> Self {
            CommitError::Io(error)
        }
    }

    impl From<CommitError> for crate::error::KmsError {
        fn from(error: CommitError) -> Self {
            match error {
                // Callers publishing with `NoClobber` are expected to map
                // `AlreadyExists` to their own domain error before this.
                CommitError::AlreadyExists => crate::error::KmsError::internal_error("durable commit destination already exists"),
                CommitError::Io(error) => error.into(),
                #[cfg(test)]
                CommitError::InjectedCrash(step) => {
                    crate::error::KmsError::internal_error(format!("injected crash after {step:?}"))
                }
            }
        }
    }

    /// Protocol steps in execution order. Tests arm a failpoint after any step
    /// to prove that every interrupted prefix recovers to either the complete
    /// old state or the complete new state.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum CommitStep {
        TempWritten,
        FileSynced,
        Published,
        DirSynced,
    }

    pub(super) async fn commit(
        temp_path: PathBuf,
        final_path: PathBuf,
        content: Vec<u8>,
        permissions: Option<u32>,
        publish: Publish,
    ) -> Result<(), CommitError> {
        tokio::task::spawn_blocking(move || commit_blocking(&temp_path, &final_path, &content, permissions, &publish))
            .await
            .map_err(|join_error| CommitError::Io(io::Error::other(join_error)))?
    }

    /// Remove a published file durably: without the parent directory fsync a
    /// deleted key could resurface after power loss.
    pub(super) async fn remove_durably(path: PathBuf) -> io::Result<()> {
        tokio::task::spawn_blocking(move || {
            std::fs::remove_file(&path)?;
            let parent = path
                .parent()
                .ok_or_else(|| io::Error::other("path has no parent directory"))?;
            fsync_dir(parent)
        })
        .await
        .map_err(io::Error::other)?
    }

    fn commit_blocking(
        temp_path: &Path,
        final_path: &Path,
        content: &[u8],
        permissions: Option<u32>,
        publish: &Publish,
    ) -> Result<(), CommitError> {
        let file = open_temp_exclusive(temp_path, permissions)?;
        match run_protocol(file, temp_path, final_path, content, permissions, publish) {
            // A simulated crash must leave the directory exactly as a real one
            // would: no cleanup.
            #[cfg(test)]
            Err(CommitError::InjectedCrash(step)) => Err(CommitError::InjectedCrash(step)),
            Err(error) => {
                let _ = std::fs::remove_file(temp_path);
                Err(error)
            }
            Ok(()) => Ok(()),
        }
    }

    fn open_temp_exclusive(temp_path: &Path, permissions: Option<u32>) -> io::Result<std::fs::File> {
        let mut options = std::fs::OpenOptions::new();
        // `create_new` refuses to follow anything already at the temp path, so
        // the temp file is always a fresh regular file owned by this process.
        options.write(true).create_new(true);
        #[cfg(unix)]
        if let Some(mode) = permissions {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(mode & 0o7777);
        }
        #[cfg(not(unix))]
        let _ = permissions;
        options.open(temp_path)
    }

    fn run_protocol(
        mut file: std::fs::File,
        temp_path: &Path,
        final_path: &Path,
        content: &[u8],
        permissions: Option<u32>,
        publish: &Publish,
    ) -> Result<(), CommitError> {
        file.write_all(content)?;
        crash_if_armed(final_path, CommitStep::TempWritten)?;

        // The umask can only narrow the creation mode, so apply and verify the
        // exact requested permissions before the content becomes durable.
        #[cfg(unix)]
        if let Some(mode) = permissions {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(std::fs::Permissions::from_mode(mode))?;
            let actual = file.metadata()?.permissions().mode() & 0o7777;
            if actual != mode & 0o7777 {
                return Err(CommitError::Io(io::Error::other(format!(
                    "temp file permissions {actual:o} do not match requested {mode:o}"
                ))));
            }
        }
        #[cfg(not(unix))]
        let _ = permissions;

        file.sync_all()?;
        #[cfg(test)]
        fsync_recorder::record_file(final_path);
        crash_if_armed(final_path, CommitStep::FileSynced)?;
        drop(file);

        match publish {
            Publish::Replace => std::fs::rename(temp_path, final_path)?,
            Publish::NoClobber => {
                if let Err(error) = std::fs::hard_link(temp_path, final_path) {
                    if error.kind() == io::ErrorKind::AlreadyExists {
                        return Err(CommitError::AlreadyExists);
                    }
                    return Err(error.into());
                }
            }
        }
        crash_if_armed(final_path, CommitStep::Published)?;

        let parent = final_path
            .parent()
            .ok_or_else(|| io::Error::other("destination has no parent directory"))?;
        fsync_dir(parent)?;
        crash_if_armed(final_path, CommitStep::DirSynced)?;

        // The published name is durable at this point; the extra temp link left
        // by `hard_link` is only cleanup. A crash here leaves an orphan that
        // startup recovery removes.
        if matches!(publish, Publish::NoClobber) {
            let _ = std::fs::remove_file(temp_path);
        }
        Ok(())
    }

    /// Fsync a directory so recently created, renamed or removed entries
    /// survive power loss. No-op on non-Unix platforms where directories
    /// cannot be opened for syncing.
    fn fsync_dir(dir: &Path) -> io::Result<()> {
        #[cfg(test)]
        fsync_recorder::record_dir(dir);
        #[cfg(unix)]
        {
            std::fs::File::open(dir)?.sync_all()?;
        }
        #[cfg(not(unix))]
        let _ = dir;
        Ok(())
    }

    fn crash_if_armed(_final_path: &Path, _step: CommitStep) -> Result<(), CommitError> {
        #[cfg(test)]
        if failpoint::is_armed(_final_path, _step) {
            return Err(CommitError::InjectedCrash(_step));
        }
        Ok(())
    }

    /// Test-only recorder mirroring ecstore's `fsync_dir_recorder`: durability
    /// regressions are invisible to ordinary behavior tests (the data is on
    /// disk either way), so tests assert directly on which paths were synced.
    /// File syncs are recorded under the commit's destination path because the
    /// temp name is randomized. Records are global; tests must match paths
    /// under their own unique tempdir to stay robust against parallel tests.
    #[cfg(test)]
    pub(super) mod fsync_recorder {
        use std::path::{Path, PathBuf};
        use std::sync::Mutex;

        static FILE_SYNCS: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());
        static DIR_SYNCS: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

        pub(super) fn record_file(path: &Path) {
            FILE_SYNCS.lock().expect("fsync recorder poisoned").push(path.to_path_buf());
        }

        pub(super) fn record_dir(dir: &Path) {
            DIR_SYNCS.lock().expect("fsync recorder poisoned").push(dir.to_path_buf());
        }

        pub(crate) fn file_sync_count(path: &Path) -> usize {
            FILE_SYNCS
                .lock()
                .expect("fsync recorder poisoned")
                .iter()
                .filter(|recorded| recorded.as_path() == path)
                .count()
        }

        pub(crate) fn dir_sync_count(dir: &Path) -> usize {
            DIR_SYNCS
                .lock()
                .expect("fsync recorder poisoned")
                .iter()
                .filter(|recorded| recorded.as_path() == dir)
                .count()
        }
    }

    /// Test-only failpoints simulating a crash after a given commit step.
    /// Armed per directory so parallel tests never affect each other.
    #[cfg(test)]
    pub(super) mod failpoint {
        use super::CommitStep;
        use std::path::{Path, PathBuf};
        use std::sync::Mutex;

        static ARMED: Mutex<Vec<(PathBuf, CommitStep)>> = Mutex::new(Vec::new());

        pub(crate) fn arm(dir: &Path, step: CommitStep) {
            let mut armed = ARMED.lock().expect("commit failpoint poisoned");
            armed.retain(|(armed_dir, _)| armed_dir != dir);
            armed.push((dir.to_path_buf(), step));
        }

        pub(crate) fn disarm(dir: &Path) {
            ARMED
                .lock()
                .expect("commit failpoint poisoned")
                .retain(|(armed_dir, _)| armed_dir != dir);
        }

        pub(super) fn is_armed(final_path: &Path, step: CommitStep) -> bool {
            ARMED
                .lock()
                .expect("commit failpoint poisoned")
                .iter()
                .any(|(dir, armed_step)| *armed_step == step && final_path.starts_with(dir))
        }
    }
}

/// Local KMS client that stores keys in local files
pub struct LocalKmsClient {
    config: LocalConfig,
    /// Master encryption key for encrypting stored keys
    master_cipher: Option<Aes256Gcm>,
    /// Legacy pre-beta.9 master cipher for reading pre-Argon2 key files
    legacy_master_cipher: Option<Aes256Gcm>,
    /// DEK encryption implementation
    dek_crypto: AesDekCrypto,
    /// Per-key write locks serializing read-modify-write updates within this
    /// process (see [`Self::lock_key_for_write`]).
    key_write_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

// pub(crate) so the backup contract tests can anchor the manifest's
// protection-state wire names against the marker values written to disk.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum StoredKeyProtection {
    #[default]
    LegacyUnspecified,
    EncryptedMasterKey,
    PlaintextDevOnly,
}

/// Serializable representation of a master key stored on disk
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredMasterKey {
    key_id: String,
    version: u32,
    algorithm: String,
    usage: KeyUsage,
    status: KeyStatus,
    description: Option<String>,
    metadata: HashMap<String, String>,
    #[serde(with = "crate::time_serde::zoned")]
    created_at: Zoned,
    #[serde(with = "crate::time_serde::option_zoned")]
    rotated_at: Option<Zoned>,
    created_by: Option<String>,
    /// Scheduled deletion deadline; absent on records written before deadline
    /// persistence landed, so it must stay optional for backward compatibility.
    #[serde(default, with = "crate::time_serde::option_zoned")]
    deletion_date: Option<Zoned>,
    /// Encrypted key material (32 bytes encoded in base64 for AES-256)
    encrypted_key_material: String,
    /// Nonce used for encryption
    nonce: Vec<u8>,
    #[serde(default)]
    at_rest_protection: StoredKeyProtection,
}

impl LocalKmsClient {
    /// Create a new local KMS client
    pub async fn new(config: LocalConfig) -> Result<Self> {
        // Create key directory if it doesn't exist
        if !fs::try_exists(&config.key_dir).await? {
            fs::create_dir_all(&config.key_dir).await?;
            debug!(path = ?config.key_dir, "KMS key directory created");
        }

        // Initialize master cipher if master key is provided
        let (master_cipher, legacy_master_cipher) = if let Some(ref master_key) = config.master_key {
            let salt = Self::load_or_create_master_key_salt(&config).await?;
            let key = Self::derive_master_key(master_key, &salt)?;
            let legacy_key = Self::derive_legacy_master_key(master_key)?;
            (Some(Aes256Gcm::new(&key)), Some(Aes256Gcm::new(&legacy_key)))
        } else {
            warn!("No master key provided - local KMS key material will use explicit plaintext-dev-only storage");
            (None, None)
        };

        let client = Self {
            config,
            master_cipher,
            legacy_master_cipher,
            dek_crypto: AesDekCrypto::new(),
            key_write_locks: Mutex::new(HashMap::new()),
        };
        client.validate_existing_keys().await?;
        Ok(client)
    }

    /// Open a Local KMS key directory without creating or modifying any files.
    ///
    /// This constructor is restricted to explicit key-export tooling. Normal
    /// backend operation must use [`Self::new`].
    pub async fn new_for_key_export(config: LocalConfig) -> Result<Self> {
        if !fs::try_exists(&config.key_dir).await? {
            return Err(KmsError::configuration_error("Local KMS key directory does not exist"));
        }

        let (master_cipher, legacy_master_cipher) = if let Some(ref master_key) = config.master_key {
            let legacy_key = Self::derive_legacy_master_key(master_key)?;
            let legacy_master_cipher = Aes256Gcm::new(&legacy_key);
            let salt_path = Self::master_key_salt_path(&config);
            let master_cipher = if fs::try_exists(&salt_path).await? {
                let salt = fs::read(&salt_path).await?;
                let salt: [u8; LOCAL_KMS_MASTER_KEY_SALT_LEN] = salt.try_into().map_err(|_| {
                    KmsError::configuration_error(format!(
                        "Local KMS master key salt at {} must be exactly {} bytes",
                        salt_path.display(),
                        LOCAL_KMS_MASTER_KEY_SALT_LEN
                    ))
                })?;
                Aes256Gcm::new(&Self::derive_master_key(master_key, &salt)?)
            } else {
                Aes256Gcm::new(&legacy_key)
            };
            (Some(master_cipher), Some(legacy_master_cipher))
        } else {
            (None, None)
        };

        Ok(Self {
            config,
            master_cipher,
            legacy_master_cipher,
            dek_crypto: AesDekCrypto::new(),
            key_write_locks: Mutex::new(HashMap::new()),
        })
    }

    /// Serialize writers of one key within this process.
    ///
    /// Status updates are read-modify-write cycles over the key file, so two
    /// concurrent writers would silently drop one update or interleave a
    /// delete with a rewrite. Cross-process writers sharing a key directory
    /// remain unsupported. Entries live for the client's lifetime; the table
    /// is bounded by the number of distinct key ids this process touches.
    async fn lock_key_for_write(&self, key_id: &str) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = {
            let mut locks = self.key_write_locks.lock().expect("Local KMS key write lock table poisoned");
            Arc::clone(locks.entry(key_id.to_string()).or_default())
        };
        lock.lock_owned().await
    }

    /// Derive a 256-bit key from the master key string using a persistent Argon2id salt.
    fn derive_master_key(master_key: &str, salt: &[u8]) -> Result<Key<Aes256Gcm>> {
        let params = Params::new(
            LOCAL_KMS_ARGON2_M_COST_KIB,
            LOCAL_KMS_ARGON2_T_COST,
            LOCAL_KMS_ARGON2_P_COST,
            Some(LOCAL_KMS_MASTER_KEY_LEN),
        )
        .map_err(|err| KmsError::configuration_error(format!("invalid local KMS Argon2 params: {err}")))?;
        let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
        let mut derived = [0u8; LOCAL_KMS_MASTER_KEY_LEN];
        argon2
            .hash_password_into(master_key.as_bytes(), salt, &mut derived)
            .map_err(|err| KmsError::cryptographic_error("argon2id_kdf", err.to_string()))?;
        let key = Key::<Aes256Gcm>::from(derived);
        Ok(key)
    }

    fn derive_legacy_master_key(master_key: &str) -> Result<Key<Aes256Gcm>> {
        let mut hasher = Sha256::new();
        hasher.update(master_key.as_bytes());
        hasher.update(b"rustfs-kms-local");
        Key::<Aes256Gcm>::try_from(hasher.finalize().as_slice())
            .map_err(|_| KmsError::cryptographic_error("legacy_key", "Invalid key length"))
    }

    fn master_key_salt_path(config: &LocalConfig) -> PathBuf {
        config.key_dir.join(LOCAL_KMS_MASTER_KEY_SALT_FILE)
    }

    async fn load_or_create_master_key_salt(config: &LocalConfig) -> Result<[u8; LOCAL_KMS_MASTER_KEY_SALT_LEN]> {
        let salt_path = Self::master_key_salt_path(config);
        if fs::try_exists(&salt_path).await? {
            let bytes = fs::read(&salt_path).await?;
            return bytes.try_into().map_err(|_| {
                KmsError::configuration_error(format!(
                    "Local KMS master key salt at {} must be exactly {} bytes",
                    salt_path.display(),
                    LOCAL_KMS_MASTER_KEY_SALT_LEN
                ))
            });
        }

        Self::ensure_missing_salt_can_be_generated(config).await?;

        let mut salt = [0u8; LOCAL_KMS_MASTER_KEY_SALT_LEN];
        rand::rng().fill(&mut salt[..]);
        let temp_path = config
            .key_dir
            .join(format!("{LOCAL_KMS_MASTER_KEY_SALT_FILE}.tmp-{}", uuid::Uuid::new_v4()));
        match durable_file::commit(
            temp_path,
            salt_path.clone(),
            salt.to_vec(),
            config.file_permissions,
            durable_file::Publish::NoClobber,
        )
        .await
        {
            Ok(()) => {
                debug!(path = ?salt_path, "Local KMS master key salt created");
                Ok(salt)
            }
            Err(durable_file::CommitError::AlreadyExists) => {
                let bytes = fs::read(&salt_path).await?;
                bytes.try_into().map_err(|_| {
                    KmsError::configuration_error(format!(
                        "Local KMS master key salt at {} must be exactly {} bytes",
                        salt_path.display(),
                        LOCAL_KMS_MASTER_KEY_SALT_LEN
                    ))
                })
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Refuse to generate a fresh salt when the directory already holds keys
    /// explicitly marked `encrypted-master-key`: their KDF output depends on
    /// the missing salt, so a replacement salt could never decrypt them.
    /// Failing closed with a salt-specific error points the operator at the
    /// real problem (restore the salt file or the whole directory) instead of
    /// a generic decrypt failure.
    ///
    /// Files that do not parse are ignored here — startup key validation
    /// reports them with their own errors right after. Legacy pre-marker files
    /// are also ignored: pre-beta.9 directories legitimately have no salt file
    /// yet, and an empty directory must keep initializing as before.
    async fn ensure_missing_salt_can_be_generated(config: &LocalConfig) -> Result<()> {
        #[derive(Deserialize)]
        struct ProtectionProbe {
            #[serde(default)]
            at_rest_protection: StoredKeyProtection,
        }

        let mut entries = fs::read_dir(&config.key_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().is_none_or(|extension| extension != "key") {
                continue;
            }
            let Ok(content) = fs::read(&path).await else {
                continue;
            };
            let Ok(probe) = serde_json::from_slice::<ProtectionProbe>(&content) else {
                continue;
            };
            if probe.at_rest_protection == StoredKeyProtection::EncryptedMasterKey {
                return Err(KmsError::configuration_error(format!(
                    "Local KMS master key salt at {} is missing but {} is marked encrypted-master-key; \
                     restore the salt file from backup instead of generating a new one",
                    Self::master_key_salt_path(config).display(),
                    path.display()
                )));
            }
        }
        Ok(())
    }

    /// Get the file path for a master key.
    ///
    /// Key identifiers reach this from request input (the `name` tag on CreateKey, the
    /// `keyId` body field or query parameter on DeleteKey), so they are joined onto
    /// `key_dir` only after being confirmed to name a single file inside it. Without that
    /// check an identifier such as `../../tmp/evil` escapes the configured key directory,
    /// turning key creation into a constrained arbitrary-file write and key deletion into
    /// a cross-directory delete.
    ///
    /// Every filesystem path in this backend is derived here, so validating at this one
    /// point covers `decode_stored_key`, `load_master_key`, `save_master_key`, `create_key`
    /// and `delete_key`.
    fn master_key_path(&self, key_id: &str) -> Result<PathBuf> {
        validate_key_id(key_id)?;
        Ok(self.config.key_dir.join(format!("{key_id}.key")))
    }

    /// Decode and decrypt a stored key file, returning both the metadata and decrypted key material
    async fn decode_stored_key(&self, key_id: &str) -> Result<(StoredMasterKey, Vec<u8>)> {
        let key_path = self.master_key_path(key_id)?;
        if !fs::try_exists(&key_path).await? {
            return Err(KmsError::key_not_found(key_id));
        }

        let content = fs::read(&key_path).await?;

        // Two-stage parse so an unrecognised protection marker is reported as an
        // unsupported format (a newer build may still read the key) instead of being
        // folded into generic corruption with every other malformed record.
        let raw: serde_json::Value = serde_json::from_slice(&content)
            .map_err(|e| KmsError::material_corrupt(key_id, format!("stored key record is not valid JSON: {e}")))?;
        if let Some(marker) = raw.get("at_rest_protection")
            && serde_json::from_value::<StoredKeyProtection>(marker.clone()).is_err()
        {
            let version = marker.as_str().map(str::to_owned).unwrap_or_else(|| marker.to_string());
            return Err(KmsError::unsupported_format_version(key_id, version));
        }
        let stored_key: StoredMasterKey = serde_json::from_value(raw)
            .map_err(|e| KmsError::material_corrupt(key_id, format!("stored key record does not deserialize: {e}")))?;
        if stored_key.key_id != key_id {
            return Err(KmsError::invalid_key(format!(
                "Local KMS key file identity mismatch: expected {key_id:?}, found {:?}",
                stored_key.key_id
            )));
        }

        // An empty material field is a damaged record, whatever the protection marker
        // says. Fail closed: reads must never backfill or regenerate master key material.
        if stored_key.encrypted_key_material.is_empty() {
            return Err(KmsError::material_missing(key_id));
        }

        let encrypted_bytes = BASE64
            .decode(&stored_key.encrypted_key_material)
            .map_err(|e| KmsError::material_corrupt(key_id, format!("stored key material is not valid base64: {e}")))?;

        let effective_protection = if stored_key.at_rest_protection == StoredKeyProtection::LegacyUnspecified {
            if stored_key.nonce.is_empty() {
                StoredKeyProtection::PlaintextDevOnly
            } else {
                StoredKeyProtection::EncryptedMasterKey
            }
        } else {
            stored_key.at_rest_protection
        };

        // Decrypt key material if master cipher is available.
        let key_material = match effective_protection {
            StoredKeyProtection::EncryptedMasterKey => {
                // RUSTFS_COMPAT_TODO(rustfs-5063): Remove after upgrades rewrite all pre-beta.9 key files.
                // Pre-beta.9 files have no protection marker and use the legacy
                // SHA-256 KDF, while later pre-marker files use Argon2.
                let cipher = self.master_cipher.as_ref().ok_or_else(|| {
                    KmsError::configuration_error(format!(
                        "Local KMS key {key_id} is encrypted at rest and requires a configured master key"
                    ))
                })?;
                if stored_key.nonce.len() != 12 {
                    return Err(KmsError::material_corrupt(
                        key_id,
                        format!("stored nonce has invalid length ({} bytes, expected 12)", stored_key.nonce.len()),
                    ));
                }

                let mut nonce_array = [0u8; 12];
                nonce_array.copy_from_slice(&stored_key.nonce);
                let nonce = Nonce::from(nonce_array);

                match cipher.decrypt(&nonce, encrypted_bytes.as_ref()) {
                    Ok(key_material) => key_material,
                    Err(_) if stored_key.at_rest_protection == StoredKeyProtection::LegacyUnspecified => {
                        let legacy_cipher = self.legacy_master_cipher.as_ref().ok_or_else(|| {
                            KmsError::configuration_error(format!(
                                "Local KMS key {key_id} is encrypted at rest and requires a configured master key"
                            ))
                        })?;
                        legacy_cipher
                            .decrypt(&nonce, encrypted_bytes.as_ref())
                            .map_err(|_| KmsError::material_authentication_failed(key_id))?
                    }
                    Err(_) => return Err(KmsError::material_authentication_failed(key_id)),
                }
            }
            StoredKeyProtection::PlaintextDevOnly | StoredKeyProtection::LegacyUnspecified => {
                if self.master_cipher.is_some() && stored_key.at_rest_protection == StoredKeyProtection::PlaintextDevOnly {
                    warn!(
                        key_id,
                        "Local KMS loaded plaintext-dev-only key material while a master key is configured"
                    );
                }
                encrypted_bytes
            }
        };

        Ok((stored_key, key_material))
    }

    /// Load a master key from disk
    async fn load_master_key(&self, key_id: &str) -> Result<MasterKeyInfo> {
        let (stored_key, _key_material) = self.decode_stored_key(key_id).await?;

        Ok(MasterKeyInfo {
            key_id: stored_key.key_id,
            version: stored_key.version,
            algorithm: stored_key.algorithm,
            usage: stored_key.usage,
            status: stored_key.status,
            description: stored_key.description,
            metadata: stored_key.metadata,
            created_at: stored_key.created_at,
            rotated_at: stored_key.rotated_at,
            created_by: stored_key.created_by,
            deletion_date: stored_key.deletion_date,
        })
    }

    /// Save a master key to disk, durably replacing any existing file
    async fn save_master_key(&self, master_key: &MasterKeyInfo, key_material: &[u8]) -> Result<()> {
        let key_path = self.master_key_path(&master_key.key_id)?;
        let content = self.encode_master_key(master_key, key_material)?;
        let temp_path = key_path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
        durable_file::commit(
            temp_path,
            key_path.clone(),
            content,
            self.config.file_permissions,
            durable_file::Publish::Replace,
        )
        .await?;

        debug!(key_id = %master_key.key_id, path = ?key_path, "Local KMS master key saved");
        Ok(())
    }

    async fn save_new_master_key(&self, master_key: &MasterKeyInfo, key_material: &[u8]) -> Result<()> {
        let key_path = self.master_key_path(&master_key.key_id)?;
        let content = self.encode_master_key(master_key, key_material)?;
        let temp_path = key_path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
        match durable_file::commit(
            temp_path,
            key_path.clone(),
            content,
            self.config.file_permissions,
            durable_file::Publish::NoClobber,
        )
        .await
        {
            Ok(()) => {
                debug!(key_id = %master_key.key_id, path = ?key_path, "Local KMS master key created");
                Ok(())
            }
            Err(durable_file::CommitError::AlreadyExists) => Err(KmsError::key_already_exists(&master_key.key_id)),
            Err(error) => Err(error.into()),
        }
    }

    fn encode_master_key(&self, master_key: &MasterKeyInfo, key_material: &[u8]) -> Result<Vec<u8>> {
        let (encrypted_key_material, nonce, at_rest_protection) = if let Some(ref cipher) = self.master_cipher {
            let mut nonce_bytes = [0u8; 12];
            rand::rng().fill(&mut nonce_bytes[..]);
            let nonce = Nonce::from(nonce_bytes);

            let encrypted = cipher
                .encrypt(&nonce, key_material)
                .map_err(|e| KmsError::cryptographic_error("encrypt", e.to_string()))?;
            // Encode encrypted bytes to base64 string
            (BASE64.encode(&encrypted), nonce.to_vec(), StoredKeyProtection::EncryptedMasterKey)
        } else {
            warn!(
                key_id = %master_key.key_id,
                "Local KMS is storing key material as plaintext-dev-only because no master key is configured"
            );
            (BASE64.encode(key_material), Vec::new(), StoredKeyProtection::PlaintextDevOnly)
        };

        let stored_key = StoredMasterKey {
            key_id: master_key.key_id.clone(),
            version: master_key.version,
            algorithm: master_key.algorithm.clone(),
            usage: master_key.usage.clone(),
            status: master_key.status.clone(),
            description: master_key.description.clone(),
            metadata: master_key.metadata.clone(),
            created_at: master_key.created_at.clone(),
            rotated_at: master_key.rotated_at.clone(),
            created_by: master_key.created_by.clone(),
            deletion_date: master_key.deletion_date.clone(),
            encrypted_key_material,
            nonce,
            at_rest_protection,
        };

        serde_json::to_vec_pretty(&stored_key).map_err(Into::into)
    }

    /// Get the actual key material for a master key
    async fn get_key_material(&self, key_id: &str) -> Result<Vec<u8>> {
        let (_stored_key, key_material) = self.decode_stored_key(key_id).await?;
        Ok(key_material)
    }

    /// Decrypt an AES-256 Local KMS key for explicit migration tooling.
    ///
    /// The returned buffer is zeroized on drop. Callers must treat the value as
    /// plaintext key material and avoid logging or persisting it.
    pub async fn decrypt_key_material_for_export(&self, key_id: &str) -> Result<Zeroizing<[u8; 32]>> {
        let (stored_key, key_material) = self.decode_stored_key(key_id).await?;
        if stored_key.algorithm != "AES_256" {
            return Err(KmsError::unsupported_algorithm(stored_key.algorithm));
        }
        let actual = key_material.len();
        let key_material = key_material.try_into().map_err(|_| KmsError::invalid_key_size(32, actual))?;
        Ok(Zeroizing::new(key_material))
    }

    /// Startup recovery and validation for the key directory.
    ///
    /// Leftover commit temp files are removed first: publishing is atomic
    /// (`rename`/`hard_link`), so a strictly matching temp name can only be an
    /// unpublished remnant of an interrupted commit, never the authoritative
    /// copy. Every published `.key` file must then decode.
    async fn validate_existing_keys(&self) -> Result<()> {
        let mut key_ids = Vec::new();
        let mut orphan_temps = Vec::new();
        let mut entries = fs::read_dir(&self.config.key_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().is_some_and(|extension| extension == "key") {
                let key_id = path
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .ok_or_else(|| KmsError::configuration_error("Local KMS key file name must be valid UTF-8"))?;
                key_ids.push(key_id.to_string());
                continue;
            }
            if entry.file_type().await?.is_file()
                && let Some(file_name) = path.file_name().and_then(|name| name.to_str())
                && is_orphan_commit_temp_name(file_name)
            {
                orphan_temps.push(path);
            }
        }

        for temp_path in orphan_temps {
            // Best effort: a temp file that cannot be removed is inert, so
            // startup proceeds and retries on the next initialization.
            match durable_file::remove_durably(temp_path.clone()).await {
                Ok(()) => warn!(path = ?temp_path, "Removed orphaned Local KMS commit temp file"),
                Err(error) => warn!(path = ?temp_path, %error, "Failed to remove orphaned Local KMS commit temp file"),
            }
        }

        for key_id in key_ids {
            self.decode_stored_key(&key_id).await?;
        }
        Ok(())
    }

    /// Encrypt data using a master key
    async fn encrypt_with_master_key(&self, key_id: &str, plaintext: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
        // Load the actual master key material
        let key_material = self.get_key_material(key_id).await?;
        self.dek_crypto.encrypt(&key_material, plaintext).await
    }

    /// Decrypt data using a master key
    async fn decrypt_with_master_key(&self, key_id: &str, ciphertext: &[u8], nonce: &[u8]) -> Result<Vec<u8>> {
        // Load the actual master key material
        let key_material = self.get_key_material(key_id).await?;
        self.dek_crypto.decrypt(&key_material, ciphertext, nonce).await
    }
}

impl LocalKmsClient {
    pub(crate) async fn generate_data_key(
        &self,
        request: &GenerateKeyRequest,
        context: Option<&OperationContext>,
    ) -> Result<DataKeyInfo> {
        debug!("Generating data key for master key: {}", request.master_key_id);

        let key_info = self.describe_key(&request.master_key_id, context).await?;
        ensure_key_status_permits(&request.master_key_id, &key_info.status, StateGatedOperation::GenerateDataKey)?;

        // Generate random data key material
        let key_length = match request.key_spec.as_str() {
            "AES_256" => 32,
            "AES_128" => 16,
            _ => return Err(KmsError::unsupported_algorithm(&request.key_spec)),
        };

        let mut plaintext_key = vec![0u8; key_length];
        rand::rng().fill(&mut plaintext_key[..]);

        // Encrypt the data key with the master key
        let (encrypted_key, nonce) = self.encrypt_with_master_key(&request.master_key_id, &plaintext_key).await?;

        // Local rotation is rejected, so every envelope is wrapped by the key's sole
        // material and needs no master key version.
        let envelope = DataKeyEnvelope {
            key_id: uuid::Uuid::new_v4().to_string(),
            master_key_id: request.master_key_id.clone(),
            key_spec: request.key_spec.clone(),
            encrypted_key,
            nonce,
            encryption_context: request.encryption_context.clone(),
            created_at: Zoned::now(),
            master_key_version: None,
        };

        // Serialize the envelope as the ciphertext
        let ciphertext = serde_json::to_vec(&envelope)?;

        let data_key = DataKeyInfo::new(envelope.key_id, 1, Some(plaintext_key), ciphertext, request.key_spec.clone());

        debug!(key_id = %request.master_key_id, "Local KMS data key generated");
        Ok(data_key)
    }

    pub(crate) async fn encrypt(&self, request: &EncryptRequest, context: Option<&OperationContext>) -> Result<EncryptResponse> {
        debug!("Encrypting data with key: {}", request.key_id);

        // Verify key exists and its state allows encryption
        let key_info = self.describe_key(&request.key_id, context).await?;
        ensure_key_status_permits(&request.key_id, &key_info.status, StateGatedOperation::Encrypt)?;

        let (ciphertext, _nonce) = self.encrypt_with_master_key(&request.key_id, &request.plaintext).await?;

        Ok(EncryptResponse {
            ciphertext,
            key_id: request.key_id.clone(),
            key_version: key_info.version,
            algorithm: key_info.algorithm,
        })
    }

    pub(crate) async fn decrypt(&self, request: &DecryptRequest, _context: Option<&OperationContext>) -> Result<Vec<u8>> {
        debug!("Decrypting data");

        // Parse the data key envelope from ciphertext
        let envelope: DataKeyEnvelope = serde_json::from_slice(&request.ciphertext)?;

        // Verify encryption context matches
        // Check that all keys in envelope.encryption_context are present in request.encryption_context
        // and their values match. This ensures the context used for decryption matches what was used for encryption.
        for (key, expected_value) in &envelope.encryption_context {
            if let Some(actual_value) = request.encryption_context.get(key) {
                if actual_value != expected_value {
                    return Err(KmsError::context_mismatch(format!(
                        "Context mismatch for key '{key}': expected '{expected_value}', got '{actual_value}'"
                    )));
                }
            } else {
                // If request.encryption_context is empty, allow decryption (backward compatibility)
                // Otherwise, require all envelope context keys to be present
                if !request.encryption_context.is_empty() {
                    return Err(KmsError::context_mismatch(format!("Missing context key '{key}'")));
                }
            }
        }

        // Decrypt the data key
        let plaintext = self
            .decrypt_with_master_key(&envelope.master_key_id, &envelope.encrypted_key, &envelope.nonce)
            .await?;

        debug!("Local KMS data decrypted");
        Ok(plaintext)
    }

    /// Test-only lifecycle driver: the product path goes through [`KmsBackend`].
    #[cfg(test)]
    pub(crate) async fn create_key(
        &self,
        key_id: &str,
        algorithm: &str,
        context: Option<&OperationContext>,
    ) -> Result<MasterKeyInfo> {
        debug!("Creating master key: {}", key_id);

        // Check if key already exists
        if self.master_key_path(key_id)?.exists() {
            return Err(KmsError::key_already_exists(key_id));
        }

        // Validate algorithm
        if algorithm != "AES_256" {
            return Err(KmsError::unsupported_algorithm(algorithm));
        }

        // Generate key material
        let key_material = generate_key_material(algorithm)?;

        let created_by = context
            .map(|ctx| ctx.principal.clone())
            .unwrap_or_else(|| "local-kms".to_string());

        let master_key = MasterKeyInfo::new_with_description(key_id.to_string(), algorithm.to_string(), Some(created_by), None);

        // Save to disk
        self.save_new_master_key(&master_key, &key_material).await?;

        debug!(key_id, "Local KMS master key created");
        Ok(master_key)
    }

    pub(crate) async fn describe_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<KeyInfo> {
        debug!("Describing key: {}", key_id);

        let master_key = self.load_master_key(key_id).await?;
        Ok(master_key.into())
    }

    pub(crate) async fn list_keys(
        &self,
        request: &ListKeysRequest,
        _context: Option<&OperationContext>,
    ) -> Result<ListKeysResponse> {
        debug!("Listing keys");

        let mut keys = Vec::new();
        let limit = request.limit.unwrap_or(100) as usize;
        let mut count = 0;

        let mut entries = fs::read_dir(&self.config.key_dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            if count >= limit {
                break;
            }

            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "key")
                && let Some(stem) = path.file_stem()
                && let Some(key_id) = stem.to_str()
                && let Ok(key_info) = self.describe_key(key_id, None).await
            {
                // Apply filters
                if let Some(ref status_filter) = request.status_filter
                    && &key_info.status != status_filter
                {
                    continue;
                }
                if let Some(ref usage_filter) = request.usage_filter
                    && &key_info.usage != usage_filter
                {
                    continue;
                }

                keys.push(key_info);
                count += 1;
            }
        }

        Ok(ListKeysResponse {
            keys,
            next_marker: None, // Simple implementation without pagination
            truncated: false,
        })
    }

    pub(crate) async fn enable_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<()> {
        debug!("Enabling key: {}", key_id);

        let _write_guard = self.lock_key_for_write(key_id).await;
        let mut master_key = self.load_master_key(key_id).await?;
        ensure_key_status_permits(key_id, &master_key.status, StateGatedOperation::Enable)?;
        master_key.status = KeyStatus::Active;

        // Preserve the existing key material. Regenerating it on a pure status change would
        // destroy the original master key and make every DEK ever wrapped by it permanently
        // undecryptable (silent data loss).
        let key_material = self.get_key_material(key_id).await?;
        self.save_master_key(&master_key, &key_material).await?;

        debug!(key_id, "Local KMS key enabled");
        Ok(())
    }

    pub(crate) async fn disable_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<()> {
        debug!("Disabling key: {}", key_id);

        let _write_guard = self.lock_key_for_write(key_id).await;
        let mut master_key = self.load_master_key(key_id).await?;
        ensure_key_status_permits(key_id, &master_key.status, StateGatedOperation::Disable)?;
        master_key.status = KeyStatus::Disabled;

        // Preserve the existing key material (see enable_key): a status change must never
        // regenerate the master key, or every DEK wrapped by it becomes undecryptable.
        let key_material = self.get_key_material(key_id).await?;
        self.save_master_key(&master_key, &key_material).await?;

        debug!(key_id, "Local KMS key disabled");
        Ok(())
    }

    /// Test-only lifecycle driver: the product path goes through [`KmsBackend`].
    #[cfg(test)]
    pub(crate) async fn schedule_key_deletion(
        &self,
        key_id: &str,
        pending_window_days: u32,
        _context: Option<&OperationContext>,
    ) -> Result<()> {
        debug!("Scheduling deletion for key: {}", key_id);

        let _write_guard = self.lock_key_for_write(key_id).await;
        let mut master_key = self.load_master_key(key_id).await?;
        ensure_key_status_permits(key_id, &master_key.status, StateGatedOperation::ScheduleDeletion)?;
        master_key.status = KeyStatus::PendingDeletion;
        master_key.deletion_date = Some(Zoned::now() + Duration::from_secs(pending_window_days as u64 * 86400));

        // Preserve the existing key material (see enable_key): scheduling deletion must not
        // regenerate the master key, or cancelling the deletion later would recover a key that
        // can no longer decrypt existing data.
        let key_material = self.get_key_material(key_id).await?;
        self.save_master_key(&master_key, &key_material).await?;

        debug!(key_id, "Local KMS key deletion scheduled");
        Ok(())
    }

    /// Test-only lifecycle driver: the product path goes through [`KmsBackend`].
    #[cfg(test)]
    pub(crate) async fn cancel_key_deletion(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<()> {
        debug!("Canceling deletion for key: {}", key_id);

        let _write_guard = self.lock_key_for_write(key_id).await;
        let mut master_key = self.load_master_key(key_id).await?;
        if master_key.status != KeyStatus::PendingDeletion {
            return Err(KmsError::invalid_key_state(format!("Key {key_id} is not pending deletion")));
        }
        master_key.status = KeyStatus::Active;
        master_key.deletion_date = None;

        // Preserve the existing key material (see enable_key): cancelling deletion must recover
        // the ORIGINAL key, not mint a new one that cannot decrypt existing data.
        let key_material = self.get_key_material(key_id).await?;
        self.save_master_key(&master_key, &key_material).await?;

        debug!(key_id, "Local KMS key deletion canceled");
        Ok(())
    }

    /// Test-only lifecycle driver: the product path goes through [`KmsBackend`].
    #[cfg(test)]
    pub(crate) async fn rotate_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<MasterKeyInfo> {
        if !fs::try_exists(self.master_key_path(key_id)?).await? {
            return Err(KmsError::key_not_found(key_id));
        }
        Err(KmsError::invalid_operation(
            "Local KMS key rotation is unavailable until historical key versions can be retained",
        ))
    }

    pub(crate) async fn health_check(&self) -> Result<()> {
        // Check if key directory is accessible
        if !self.config.key_dir.exists() {
            return Err(KmsError::backend_error("Key directory does not exist"));
        }

        // Try to read the directory
        let _ = fs::read_dir(&self.config.key_dir).await?;

        Ok(())
    }
}

/// LocalKmsBackend wraps LocalKmsClient and implements the KmsBackend trait
pub struct LocalKmsBackend {
    client: LocalKmsClient,
}

impl LocalKmsBackend {
    /// Lifecycle driver for the shared state-machine contract tests.
    #[cfg(test)]
    pub(crate) fn lifecycle_client(&self) -> &LocalKmsClient {
        &self.client
    }

    /// Create a new LocalKmsBackend
    pub async fn new(config: KmsConfig) -> Result<Self> {
        config.validate()?;

        let local_config = match &config.backend_config {
            crate::config::BackendConfig::Local(local_config) => local_config.clone(),
            crate::config::BackendConfig::VaultKv2(_)
            | crate::config::BackendConfig::VaultTransit(_)
            | crate::config::BackendConfig::Static(_) => {
                return Err(KmsError::configuration_error("Expected Local backend configuration"));
            }
        };

        let client = LocalKmsClient::new(local_config).await?;
        Ok(Self { client })
    }
}

#[async_trait]
impl KmsBackend for LocalKmsBackend {
    async fn create_key(&self, request: CreateKeyRequest) -> Result<CreateKeyResponse> {
        let key_id = request.key_name.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        if self.client.master_key_path(&key_id)?.exists() {
            return Err(KmsError::key_already_exists(&key_id));
        }

        // Create master key with description directly
        let _master_key = {
            let algorithm = "AES_256";
            // Generate key material
            let key_material = generate_key_material(algorithm)?;

            let master_key = MasterKeyInfo::new_with_description(
                key_id.clone(),
                algorithm.to_string(),
                Some("local-kms".to_string()),
                request.description.clone(),
            );

            // Save to disk
            self.client.save_new_master_key(&master_key, &key_material).await?;

            master_key
        };

        let metadata = KeyMetadata {
            key_id: key_id.clone(),
            key_state: KeyState::Enabled,
            key_usage: request.key_usage,
            description: request.description,
            creation_date: Zoned::now(),
            deletion_date: None,
            origin: "KMS".to_string(),
            key_manager: "CUSTOMER".to_string(),
            tags: request.tags,
        };

        Ok(CreateKeyResponse {
            key_id,
            key_metadata: metadata,
        })
    }

    async fn encrypt(&self, request: EncryptRequest) -> Result<EncryptResponse> {
        let encrypt_request = EncryptRequest {
            key_id: request.key_id.clone(),
            plaintext: request.plaintext,
            encryption_context: request.encryption_context,
            grant_tokens: request.grant_tokens,
        };

        let response = self.client.encrypt(&encrypt_request, None).await?;

        Ok(EncryptResponse {
            ciphertext: response.ciphertext,
            key_id: response.key_id,
            key_version: response.key_version,
            algorithm: response.algorithm,
        })
    }

    async fn decrypt(&self, request: DecryptRequest) -> Result<DecryptResponse> {
        let plaintext = self.client.decrypt(&request, None).await?;

        // For simplicity, return basic response - in real implementation would extract more info from ciphertext
        Ok(DecryptResponse {
            plaintext,
            key_id: "unknown".to_string(), // Would be extracted from ciphertext metadata
            encryption_algorithm: Some("AES-256-GCM".to_string()),
        })
    }

    async fn generate_data_key(&self, request: GenerateDataKeyRequest) -> Result<GenerateDataKeyResponse> {
        let generate_request = GenerateKeyRequest {
            master_key_id: request.key_id.clone(),
            key_spec: request.key_spec.as_str().to_string(),
            key_length: Some(request.key_spec.key_size() as u32),
            encryption_context: request.encryption_context,
            grant_tokens: Vec::new(),
        };

        let data_key = self.client.generate_data_key(&generate_request, None).await?;

        Ok(GenerateDataKeyResponse {
            key_id: request.key_id,
            plaintext_key: data_key.plaintext.clone().unwrap_or_default(),
            ciphertext_blob: data_key.ciphertext.clone(),
        })
    }

    async fn describe_key(&self, request: DescribeKeyRequest) -> Result<DescribeKeyResponse> {
        let key_info = self.client.describe_key(&request.key_id, None).await?;
        let deletion_date = if key_info.status == KeyStatus::PendingDeletion {
            self.client.load_master_key(&request.key_id).await?.deletion_date
        } else {
            None
        };

        let metadata = KeyMetadata {
            key_id: key_info.key_id,
            key_state: match key_info.status {
                KeyStatus::Active => KeyState::Enabled,
                KeyStatus::Disabled => KeyState::Disabled,
                KeyStatus::PendingDeletion => KeyState::PendingDeletion,
                KeyStatus::Deleted => KeyState::Unavailable,
            },
            key_usage: key_info.usage,
            description: key_info.description,
            creation_date: key_info.created_at,
            deletion_date,
            origin: "KMS".to_string(),
            key_manager: "CUSTOMER".to_string(),
            tags: key_info.tags,
        };

        Ok(DescribeKeyResponse { key_metadata: metadata })
    }

    async fn list_keys(&self, request: ListKeysRequest) -> Result<ListKeysResponse> {
        let response = self.client.list_keys(&request, None).await?;
        Ok(response)
    }

    async fn delete_key(&self, request: DeleteKeyRequest) -> Result<DeleteKeyResponse> {
        // For local backend, we'll implement immediate deletion by default
        // unless a pending window is specified
        let key_id = &request.key_id;

        // Deletion is a read-modify-write (or read-then-remove) cycle, so hold
        // the per-key write lock across it.
        let _write_guard = self.client.lock_key_for_write(key_id).await;

        // First, load the key from disk to get the master key
        let mut master_key = self
            .client
            .load_master_key(key_id)
            .await
            .map_err(|_| KmsError::key_not_found(format!("Key {key_id} not found")))?;

        let (deletion_date_str, deletion_date_dt) = if request.force_immediate.unwrap_or(false) {
            // Tombstone first: mark the record Deleted before removing the
            // file, so a crash between the two steps leaves a key that is
            // already unusable and whose removal can simply be re-run.
            match self.client.decode_stored_key(key_id).await {
                Ok((_stored, key_material)) => {
                    let mut tombstone = master_key.clone();
                    tombstone.status = KeyStatus::Deleted;
                    tombstone.deletion_date = Some(Zoned::now());
                    self.client.save_master_key(&tombstone, &key_material).await?;
                }
                Err(error) => {
                    // A record whose material can no longer be decoded cannot be
                    // re-encrypted into a tombstone; proceed with the removal.
                    warn!(key_id, %error, "skipping tombstone for undecodable key record");
                }
            }
            let key_path = self.client.master_key_path(key_id)?;
            durable_file::remove_durably(key_path)
                .await
                .map_err(|e| KmsError::internal_error(format!("Failed to delete key file: {e}")))?;

            debug!(key_id, "Local KMS key deleted immediately");

            // Return success response for immediate deletion
            let key_metadata = KeyMetadata {
                key_id: master_key.key_id.clone(),
                description: master_key.description.clone(),
                key_usage: master_key.usage,
                key_state: KeyState::PendingDeletion, // AWS KMS compatibility
                creation_date: master_key.created_at,
                deletion_date: Some(Zoned::now()),
                key_manager: "CUSTOMER".to_string(),
                origin: "AWS_KMS".to_string(),
                tags: master_key.metadata,
            };

            return Ok(DeleteKeyResponse {
                key_id: key_id.clone(),
                deletion_date: None, // No deletion date for immediate deletion
                key_metadata,
            });
        } else {
            // Schedule for deletion (default 30 days)
            ensure_key_status_permits(key_id, &master_key.status, StateGatedOperation::ScheduleDeletion)?;

            let days = request.pending_window_in_days.unwrap_or(30);
            if !(7..=30).contains(&days) {
                return Err(KmsError::invalid_parameter("pending_window_in_days must be between 7 and 30".to_string()));
            }

            let deletion_date = Zoned::now() + Duration::from_secs(days as u64 * 86400);
            master_key.status = KeyStatus::PendingDeletion;
            master_key.deletion_date = Some(deletion_date.clone());

            (Some(deletion_date.to_string()), Some(deletion_date))
        };

        // Save the updated key to disk - preserve existing key material!
        // Load and decode the stored key to get the existing key material
        let (_stored_key, existing_key_material) = self
            .client
            .decode_stored_key(key_id)
            .await
            .map_err(|e| KmsError::internal_error(format!("Failed to decode key: {e}")))?;

        self.client.save_master_key(&master_key, &existing_key_material).await?;

        // Convert master_key to KeyMetadata for response
        let key_metadata = KeyMetadata {
            key_id: master_key.key_id.clone(),
            description: master_key.description.clone(),
            key_usage: master_key.usage,
            key_state: KeyState::PendingDeletion,
            creation_date: master_key.created_at,
            deletion_date: deletion_date_dt,
            key_manager: "CUSTOMER".to_string(),
            origin: "AWS_KMS".to_string(),
            tags: master_key.metadata,
        };

        Ok(DeleteKeyResponse {
            key_id: key_id.clone(),
            deletion_date: deletion_date_str,
            key_metadata,
        })
    }

    async fn cancel_key_deletion(&self, request: CancelKeyDeletionRequest) -> Result<CancelKeyDeletionResponse> {
        let key_id = &request.key_id;

        // Cancelling is a read-modify-write cycle, so hold the per-key write lock.
        let _write_guard = self.client.lock_key_for_write(key_id).await;

        // Load the key from disk to get the master key
        let mut master_key = self
            .client
            .load_master_key(key_id)
            .await
            .map_err(|_| KmsError::key_not_found(format!("Key {key_id} not found")))?;

        if master_key.status != KeyStatus::PendingDeletion {
            return Err(KmsError::invalid_key_state(format!("Key {key_id} is not pending deletion")));
        }

        // Cancel the deletion by resetting the state
        master_key.status = KeyStatus::Active;
        master_key.deletion_date = None;

        // Save the updated key to disk - this is the missing critical step!
        // Preserve existing key material instead of generating new one
        let (_stored_key, existing_key_material) = self
            .client
            .decode_stored_key(key_id)
            .await
            .map_err(|e| KmsError::internal_error(format!("Failed to decode key: {e}")))?;

        self.client.save_master_key(&master_key, &existing_key_material).await?;

        // Convert master_key to KeyMetadata for response
        let key_metadata = KeyMetadata {
            key_id: master_key.key_id.clone(),
            description: master_key.description.clone(),
            key_usage: master_key.usage,
            key_state: KeyState::Enabled,
            creation_date: master_key.created_at,
            deletion_date: None,
            key_manager: "CUSTOMER".to_string(),
            origin: "AWS_KMS".to_string(),
            tags: master_key.metadata,
        };

        Ok(CancelKeyDeletionResponse {
            key_id: key_id.clone(),
            key_metadata,
        })
    }

    async fn enable_key(&self, key_id: &str) -> Result<()> {
        self.client.enable_key(key_id, None).await
    }

    async fn disable_key(&self, key_id: &str) -> Result<()> {
        self.client.disable_key(key_id, None).await
    }

    async fn health_check(&self) -> Result<bool> {
        self.client.health_check().await.map(|_| true)
    }

    fn capabilities(&self) -> BackendCapabilities {
        // Rotation stays unadvertised until historical key versions can be
        // retained (see LocalKmsClient::rotate_key); without version history
        // there is also no versioning capability.
        BackendCapabilities::minimal()
            .with_enable_disable(true)
            .with_schedule_deletion(true)
            .with_physical_delete(true)
    }

    async fn remove_expired_key(&self, key_id: &str, now: &Zoned) -> Result<ExpiredKeyRemoval> {
        // The per-key write lock serializes this against a concurrent
        // cancellation, closing the check-then-remove race.
        let _write_guard = self.client.lock_key_for_write(key_id).await;

        if !fs::try_exists(self.client.master_key_path(key_id)?).await? {
            return Ok(ExpiredKeyRemoval::Removed);
        }
        let master_key = self.client.load_master_key(key_id).await?;
        match master_key.status {
            // Tombstone left by a crashed removal: complete it.
            KeyStatus::Deleted => {}
            KeyStatus::PendingDeletion => {
                match &master_key.deletion_date {
                    Some(deadline) if deadline <= now => {}
                    // Not yet due, or a legacy record without a persisted
                    // deadline — never auto-remove those.
                    _ => return Ok(ExpiredKeyRemoval::NotExpired),
                }
                // Tombstone first (see delete_key): a crash between the state
                // write and the file removal must leave an unusable record.
                match self.client.decode_stored_key(key_id).await {
                    Ok((_stored, key_material)) => {
                        let mut tombstone = master_key.clone();
                        tombstone.status = KeyStatus::Deleted;
                        tombstone.deletion_date = Some(now.clone());
                        self.client.save_master_key(&tombstone, &key_material).await?;
                    }
                    Err(error) => {
                        warn!(key_id, %error, "skipping tombstone for undecodable key record");
                    }
                }
            }
            KeyStatus::Active | KeyStatus::Disabled => return Ok(ExpiredKeyRemoval::StateChanged),
        }

        durable_file::remove_durably(self.client.master_key_path(key_id)?)
            .await
            .map_err(|e| KmsError::internal_error(format!("Failed to delete key file: {e}")))?;
        debug!(key_id, "Local KMS expired key removed");
        Ok(ExpiredKeyRemoval::Removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tempfile::TempDir;

    async fn create_test_client() -> (LocalKmsClient, TempDir) {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = LocalConfig {
            key_dir: temp_dir.path().to_path_buf(),
            master_key: Some("test-master-key".to_string()),
            file_permissions: Some(0o600),
        };
        let client = LocalKmsClient::new(config).await.expect("Failed to create client");
        (client, temp_dir)
    }

    async fn create_dev_mode_client() -> (LocalKmsClient, TempDir) {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = LocalConfig {
            key_dir: temp_dir.path().to_path_buf(),
            master_key: None,
            file_permissions: Some(0o600),
        };
        let client = LocalKmsClient::new(config).await.expect("Failed to create dev-mode client");
        (client, temp_dir)
    }

    #[tokio::test]
    async fn test_key_lifecycle() {
        let (client, _temp_dir) = create_test_client().await;

        let key_id = "test-key";
        let algorithm = "AES_256";

        // Create key
        let master_key = client
            .create_key(key_id, algorithm, None)
            .await
            .expect("Failed to create key");
        assert_eq!(master_key.key_id, key_id);
        assert_eq!(master_key.algorithm, algorithm);
        assert_eq!(master_key.status, KeyStatus::Active);

        // Describe key
        let key_info = client.describe_key(key_id, None).await.expect("Failed to describe key");
        assert_eq!(key_info.key_id, key_id);
        assert_eq!(key_info.status, KeyStatus::Active);

        // List keys
        let list_response = client
            .list_keys(&ListKeysRequest::default(), None)
            .await
            .expect("Failed to list keys");
        assert_eq!(list_response.keys.len(), 1);
        assert_eq!(list_response.keys[0].key_id, key_id);

        // Disable key
        client.disable_key(key_id, None).await.expect("Failed to disable key");
        let key_info = client.describe_key(key_id, None).await.expect("Failed to describe key");
        assert_eq!(key_info.status, KeyStatus::Disabled);

        // Enable key
        client.enable_key(key_id, None).await.expect("Failed to enable key");
        let key_info = client.describe_key(key_id, None).await.expect("Failed to describe key");
        assert_eq!(key_info.status, KeyStatus::Active);
    }

    #[tokio::test]
    async fn test_data_key_operations() {
        let (client, _temp_dir) = create_test_client().await;

        let key_id = "test-key";
        client
            .create_key(key_id, "AES_256", None)
            .await
            .expect("Failed to create key");

        // Generate data key
        let request = GenerateKeyRequest::new(key_id.to_string(), "AES_256".to_string())
            .with_context("bucket".to_string(), "test-bucket".to_string());

        let data_key = client
            .generate_data_key(&request, None)
            .await
            .expect("Failed to generate data key");
        assert!(data_key.plaintext.is_some());
        assert!(!data_key.ciphertext.is_empty());

        // Decrypt data key
        let decrypt_request =
            DecryptRequest::new(data_key.ciphertext.clone()).with_context("bucket".to_string(), "test-bucket".to_string());

        let decrypted = client.decrypt(&decrypt_request, None).await.expect("Failed to decrypt");
        assert_eq!(decrypted, data_key.plaintext.clone().expect("No plaintext"));
    }

    #[tokio::test]
    async fn key_state_transitions_preserve_master_key_material() {
        // Regression: enable/disable/schedule_deletion/cancel_deletion previously regenerated the
        // master key material on a pure status change, permanently destroying the ability to
        // decrypt any DEK wrapped by that key. A status cycle must preserve the material.
        let (client, _temp_dir) = create_test_client().await;

        let key_id = "state-cycle-key";
        client.create_key(key_id, "AES_256", None).await.expect("create");

        let request = GenerateKeyRequest::new(key_id.to_string(), "AES_256".to_string())
            .with_context("bucket".to_string(), "b".to_string());
        let data_key = client.generate_data_key(&request, None).await.expect("generate data key");
        let ciphertext = data_key.ciphertext.clone();
        let plaintext = data_key.plaintext.clone().expect("no plaintext");

        // Cycle through every status-changing method the fix touches.
        client.disable_key(key_id, None).await.expect("disable");
        client.enable_key(key_id, None).await.expect("enable");
        client
            .schedule_key_deletion(key_id, 7, None)
            .await
            .expect("schedule deletion");
        client.cancel_key_deletion(key_id, None).await.expect("cancel deletion");

        // Pre-fix, each of those regenerated the master key, so this unwrap fails with an AEAD
        // error. Post-fix, the original material is preserved and the DEK still decrypts.
        let decrypt_request = DecryptRequest::new(ciphertext).with_context("bucket".to_string(), "b".to_string());
        let decrypted = client
            .decrypt(&decrypt_request, None)
            .await
            .expect("DEK must still decrypt after status transitions");
        assert_eq!(decrypted, plaintext, "master key material must survive status transitions");
    }

    /// Snapshot every file in the key directory as (name, content SHA-256, mtime).
    /// Comparing snapshots proves the read paths performed no persistent write at all —
    /// no rewrite, no "repair", no temp-file leftovers.
    async fn snapshot_key_dir(dir: &std::path::Path) -> Vec<(String, Vec<u8>, std::time::SystemTime)> {
        let mut entries = fs::read_dir(dir).await.expect("read key dir");
        let mut snapshot = Vec::new();
        while let Some(entry) = entries.next_entry().await.expect("next key dir entry") {
            let content = fs::read(entry.path()).await.expect("read key dir file");
            let modified = entry
                .metadata()
                .await
                .expect("key dir file metadata")
                .modified()
                .expect("key dir file mtime");
            snapshot.push((
                entry.file_name().to_string_lossy().into_owned(),
                Sha256::digest(&content).to_vec(),
                modified,
            ));
        }
        snapshot.sort();
        snapshot
    }

    /// Poison matrix guard: every corruption class must surface its precise typed error
    /// from every read path (get_key_material / describe_key / decrypt), and the key
    /// directory must stay byte-for-byte identical. Restoring any historical "self-heal"
    /// behaviour (regenerating or rewriting material on a failed read) flips either the
    /// error assertion (read succeeds) or the snapshot assertion (directory changed).
    #[tokio::test]
    async fn read_paths_fail_closed_never_write_on_poisoned_key_files() {
        let (client, temp_dir) = create_test_client().await;
        let key_id = "poisoned-key";
        client.create_key(key_id, "AES_256", None).await.expect("create key");

        // Wrap a DEK while the key is healthy so the decrypt path can be exercised
        // against each poisoned state of its master key.
        let request = GenerateKeyRequest::new(key_id.to_string(), "AES_256".to_string());
        let data_key = client.generate_data_key(&request, None).await.expect("generate data key");
        let envelope_ciphertext = data_key.ciphertext.clone();

        let key_path = client.master_key_path(key_id).expect("valid key id");
        let pristine: serde_json::Value =
            serde_json::from_slice(&fs::read(&key_path).await.expect("read pristine key file")).expect("decode pristine record");
        let pristine_bytes = serde_json::to_vec_pretty(&pristine).expect("encode pristine record");

        let with_field = |field: &str, value: serde_json::Value| {
            let mut record = pristine.clone();
            record[field] = value;
            serde_json::to_vec_pretty(&record).expect("encode poisoned record")
        };

        let tampered_material = {
            let mut material = BASE64
                .decode(pristine["encrypted_key_material"].as_str().expect("material is a string"))
                .expect("decode pristine material");
            *material.last_mut().expect("material is not empty") ^= 0x01;
            BASE64.encode(&material)
        };

        type PoisonCase = (&'static str, Vec<u8>, fn(&KmsError) -> bool);
        let poisons: Vec<PoisonCase> = vec![
            ("empty material", with_field("encrypted_key_material", serde_json::json!("")), |e| {
                matches!(e, KmsError::MaterialMissing { .. })
            }),
            ("truncated JSON", pristine_bytes[..pristine_bytes.len() / 2].to_vec(), |e| {
                matches!(e, KmsError::MaterialCorrupt { .. })
            }),
            (
                "invalid base64",
                with_field("encrypted_key_material", serde_json::json!("!!!not-base64!!!")),
                |e| matches!(e, KmsError::MaterialCorrupt { .. }),
            ),
            ("wrong nonce length", with_field("nonce", serde_json::json!([0, 1, 2])), |e| {
                matches!(e, KmsError::MaterialCorrupt { .. })
            }),
            (
                "tampered AEAD",
                with_field("encrypted_key_material", serde_json::json!(tampered_material)),
                |e| matches!(e, KmsError::MaterialAuthenticationFailed { .. }),
            ),
            (
                "unknown protection marker",
                with_field("at_rest_protection", serde_json::json!("post-quantum-v2")),
                |e| matches!(e, KmsError::UnsupportedFormatVersion { version, .. } if version == "post-quantum-v2"),
            ),
        ];

        for (name, poisoned_content, expected) in poisons {
            fs::write(&key_path, &poisoned_content).await.expect("write poisoned record");
            let before = snapshot_key_dir(temp_dir.path()).await;

            let error = client
                .get_key_material(key_id)
                .await
                .expect_err("get_key_material must fail on poisoned material");
            assert!(expected(&error), "{name}: get_key_material returned wrong variant: {error:?}");

            let error = client
                .describe_key(key_id, None)
                .await
                .expect_err("describe_key must fail on poisoned material");
            assert!(expected(&error), "{name}: describe_key returned wrong variant: {error:?}");

            let error = client
                .decrypt(&DecryptRequest::new(envelope_ciphertext.clone()), None)
                .await
                .expect_err("decrypt must fail on poisoned material");
            assert!(expected(&error), "{name}: decrypt returned wrong variant: {error:?}");

            assert_eq!(
                snapshot_key_dir(temp_dir.path()).await,
                before,
                "{name}: read paths must not write to the key directory"
            );
        }
    }

    #[tokio::test]
    async fn test_encryption_operations() {
        let (client, _temp_dir) = create_test_client().await;

        let key_id = "test-key";
        client
            .create_key(key_id, "AES_256", None)
            .await
            .expect("Failed to create key");

        let plaintext = b"Hello, World!";
        let encrypt_request = EncryptRequest::new(key_id.to_string(), plaintext.to_vec());

        // Encrypt
        let encrypt_response = client.encrypt(&encrypt_request, None).await.expect("Failed to encrypt");
        assert!(!encrypt_response.ciphertext.is_empty());
        assert_eq!(encrypt_response.key_id, key_id);

        // Note: Direct decryption of encrypt() results is not implemented in this simple version
        // In a real implementation, encrypt() would create a different envelope format
    }

    #[tokio::test]
    async fn test_encrypted_master_key_storage_uses_explicit_protection_and_salt() {
        let (client, _temp_dir) = create_test_client().await;
        client
            .create_key("encrypted-key", "AES_256", None)
            .await
            .expect("Failed to create encrypted key");

        let salt = fs::read(LocalKmsClient::master_key_salt_path(&client.config))
            .await
            .expect("master key salt should exist");
        assert_eq!(salt.len(), LOCAL_KMS_MASTER_KEY_SALT_LEN);

        let stored: StoredMasterKey = serde_json::from_slice(
            &fs::read(client.master_key_path("encrypted-key").expect("valid key id"))
                .await
                .expect("stored key should exist"),
        )
        .expect("stored encrypted key should deserialize");
        assert_eq!(stored.at_rest_protection, StoredKeyProtection::EncryptedMasterKey);
        assert_eq!(stored.nonce.len(), 12);

        let wrong_master_error = match LocalKmsClient::new(LocalConfig {
            key_dir: client.config.key_dir.clone(),
            master_key: Some("wrong-master-key".to_string()),
            file_permissions: Some(0o600),
        })
        .await
        {
            Ok(_) => panic!("wrong master key must fail initialization"),
            Err(error) => error,
        };
        assert!(matches!(wrong_master_error, KmsError::MaterialAuthenticationFailed { .. }));
    }

    #[tokio::test]
    async fn key_export_uses_existing_local_decryption_path_without_writing_files() {
        let (client, _temp_dir) = create_test_client().await;
        let key_id = "export-key";
        client
            .create_key(key_id, "AES_256", None)
            .await
            .expect("create encrypted key");
        let expected = client.get_key_material(key_id).await.expect("load expected key material");
        let salt_path = LocalKmsClient::master_key_salt_path(&client.config);
        let salt_before = fs::read(&salt_path).await.expect("read existing salt");

        let export_client = LocalKmsClient::new_for_key_export(client.config.clone())
            .await
            .expect("open read-only export client");
        let exported = export_client
            .decrypt_key_material_for_export(key_id)
            .await
            .expect("decrypt key for export");

        assert_eq!(exported.as_ref(), expected.as_slice());
        assert_eq!(fs::read(&salt_path).await.expect("read unchanged salt"), salt_before);
    }

    #[tokio::test]
    async fn key_export_accepts_plaintext_dev_only_key_without_master_key() {
        let (client, _temp_dir) = create_dev_mode_client().await;
        let key_id = "plaintext-export-key";
        client
            .create_key(key_id, "AES_256", None)
            .await
            .expect("create plaintext-dev-only key");
        let expected = client.get_key_material(key_id).await.expect("load expected key material");

        let export_client = LocalKmsClient::new_for_key_export(client.config.clone())
            .await
            .expect("open read-only export client");
        let exported = export_client
            .decrypt_key_material_for_export(key_id)
            .await
            .expect("export plaintext-dev-only key");

        assert_eq!(exported.as_ref(), expected.as_slice());
        assert!(!LocalKmsClient::master_key_salt_path(&client.config).exists());
    }

    #[tokio::test]
    async fn test_plaintext_dev_only_storage_is_explicit_and_loadable() {
        let (client, _temp_dir) = create_dev_mode_client().await;
        client
            .create_key("plaintext-key", "AES_256", None)
            .await
            .expect("Failed to create plaintext-dev-only key");

        let stored: StoredMasterKey = serde_json::from_slice(
            &fs::read(client.master_key_path("plaintext-key").expect("valid key id"))
                .await
                .expect("stored key should exist"),
        )
        .expect("stored plaintext key should deserialize");
        assert_eq!(stored.at_rest_protection, StoredKeyProtection::PlaintextDevOnly);
        assert!(stored.nonce.is_empty(), "plaintext-dev-only keys should not store a nonce");

        let key_info = client
            .describe_key("plaintext-key", None)
            .await
            .expect("plaintext-dev-only key should remain readable");
        assert_eq!(key_info.key_id, "plaintext-key");
    }

    #[tokio::test]
    async fn test_encrypted_key_requires_master_key_to_load() {
        let (client, temp_dir) = create_test_client().await;
        client
            .create_key("encrypted-key", "AES_256", None)
            .await
            .expect("Failed to create encrypted key");

        let config = LocalConfig {
            key_dir: temp_dir.path().to_path_buf(),
            master_key: None,
            file_permissions: Some(0o600),
        };
        let err = match LocalKmsClient::new(config).await {
            Ok(_) => panic!("initialization must reject an unreadable encrypted key"),
            Err(error) => error,
        };
        assert!(err.to_string().contains("requires a configured master key"));
    }

    #[tokio::test]
    async fn local_key_rotation_is_rejected_without_overwriting_key_material() {
        let (client, _temp_dir) = create_test_client().await;
        let key_id = "rotation-key";
        client.create_key(key_id, "AES_256", None).await.expect("create key");
        let original_material = client.get_key_material(key_id).await.expect("load original material");

        let error = client
            .rotate_key(key_id, None)
            .await
            .expect_err("rotation must remain unavailable without historical key versions");

        assert!(matches!(error, KmsError::InvalidOperation { .. }));
        assert_eq!(
            client.get_key_material(key_id).await.expect("reload original material"),
            original_material
        );
    }

    /// Mixed-format regression for rustfs/backlog#1565: a batch interleaving
    /// pre-versioning envelopes (no master_key_version field) with versioned ones
    /// must route and decrypt in full, and a rejected rotation in the middle must
    /// not disturb either format.
    #[tokio::test]
    async fn mixed_format_envelopes_decrypt_across_rejected_rotation() {
        let (client, _temp_dir) = create_test_client().await;
        let key_id = "mixed-format-key";
        client.create_key(key_id, "AES_256", None).await.expect("create key");

        let request = GenerateKeyRequest::new(key_id.to_string(), "AES_256".to_string());
        let mut batch = Vec::new();
        for index in 0..4 {
            let data_key = client.generate_data_key(&request, None).await.expect("generate data key");
            let ciphertext = if index % 2 == 0 {
                // Legacy shape: the local backend already omits master_key_version.
                let envelope: serde_json::Value = serde_json::from_slice(&data_key.ciphertext).expect("parse envelope");
                assert!(
                    !envelope
                        .as_object()
                        .expect("envelope is an object")
                        .contains_key("master_key_version"),
                    "local envelopes must keep the pre-versioning shape"
                );
                data_key.ciphertext.clone()
            } else {
                // Versioned shape, as a rotation-aware writer would emit it.
                let mut envelope: serde_json::Value = serde_json::from_slice(&data_key.ciphertext).expect("parse envelope");
                envelope
                    .as_object_mut()
                    .expect("envelope is an object")
                    .insert("master_key_version".to_string(), serde_json::json!(1));
                serde_json::to_vec(&envelope).expect("serialize versioned envelope")
            };
            assert!(
                crate::encryption::is_data_key_envelope(&ciphertext),
                "batch member {index} must still route as a KMS envelope"
            );
            batch.push((ciphertext, data_key.plaintext.clone().expect("plaintext")));
        }

        // A rejected rotation in the middle of the batch's lifetime must leave
        // every already-issued envelope decryptable.
        let error = client
            .rotate_key(key_id, None)
            .await
            .expect_err("local rotation must stay rejected");
        assert!(matches!(error, KmsError::InvalidOperation { .. }));

        for (index, (ciphertext, plaintext)) in batch.iter().enumerate() {
            let decrypted = client
                .decrypt(&DecryptRequest::new(ciphertext.clone()), None)
                .await
                .unwrap_or_else(|error| panic!("batch member {index} must decrypt: {error}"));
            assert_eq!(&decrypted, plaintext, "batch member {index} plaintext must round-trip");
        }
    }

    #[tokio::test]
    async fn startup_rejects_key_file_with_mismatched_embedded_id() {
        let (client, temp_dir) = create_dev_mode_client().await;
        client.create_key("file-name", "AES_256", None).await.expect("create key");
        let key_path = client.master_key_path("file-name").expect("valid key id");
        let mut stored: serde_json::Value =
            serde_json::from_slice(&fs::read(&key_path).await.expect("read key file")).expect("decode key file");
        stored["key_id"] = serde_json::json!("embedded-name");
        fs::write(&key_path, serde_json::to_vec_pretty(&stored).expect("encode mismatched key"))
            .await
            .expect("write mismatched key");

        let error = match LocalKmsClient::new(LocalConfig {
            key_dir: temp_dir.path().to_path_buf(),
            master_key: None,
            file_permissions: Some(0o600),
        })
        .await
        {
            Ok(_) => panic!("mismatched key identity must fail initialization"),
            Err(error) => error,
        };
        assert!(matches!(error, KmsError::InvalidKey { .. }));
    }

    #[tokio::test]
    async fn test_load_master_key_accepts_legacy_rfc3339_timestamp() {
        let (client, _temp_dir) = create_dev_mode_client().await;

        let stored_key = serde_json::json!({
            "key_id": "legacy-key",
            "version": 1u32,
            "algorithm": "AES_256",
            "usage": "EncryptDecrypt",
            "status": "Active",
            "description": serde_json::Value::Null,
            "metadata": HashMap::<String, String>::new(),
            "created_at": "2024-01-01T00:00:00+00:00",
            "rotated_at": serde_json::Value::Null,
            "created_by": "legacy-test",
            "encrypted_key_material": BASE64.encode([7u8; 32]),
            "nonce": Vec::<u8>::new()
        });

        let key_path = client.master_key_path("legacy-key").expect("valid key id");
        fs::write(&key_path, serde_json::to_vec_pretty(&stored_key).expect("serialize test key"))
            .await
            .expect("write legacy key");

        let key_info = client.load_master_key("legacy-key").await.expect("legacy key should load");
        assert_eq!(key_info.key_id, "legacy-key");
        assert_eq!(key_info.created_at.time_zone().iana_name(), Some("UTC"));
    }

    #[tokio::test]
    async fn test_load_master_key_accepts_legacy_encrypted_record_without_protection_field() {
        let (client, temp_dir) = create_test_client().await;
        client
            .create_key("legacy-encrypted-key", "AES_256", None)
            .await
            .expect("Failed to create encrypted key");

        let key_path = client.master_key_path("legacy-encrypted-key").expect("valid key id");
        let mut stored_json: serde_json::Value =
            serde_json::from_slice(&fs::read(&key_path).await.expect("stored key should exist"))
                .expect("stored key should deserialize");
        stored_json
            .as_object_mut()
            .expect("stored key should be an object")
            .remove("at_rest_protection");
        fs::write(
            &key_path,
            serde_json::to_vec_pretty(&stored_json).expect("legacy record should serialize"),
        )
        .await
        .expect("legacy record should be writable");

        let legacy_client = LocalKmsClient::new(LocalConfig {
            key_dir: temp_dir.path().to_path_buf(),
            master_key: Some("test-master-key".to_string()),
            file_permissions: Some(0o600),
        })
        .await
        .expect("legacy client should initialize");

        let key_info = legacy_client
            .describe_key("legacy-encrypted-key", None)
            .await
            .expect("legacy encrypted record should remain readable");
        assert_eq!(key_info.key_id, "legacy-encrypted-key");
    }

    #[tokio::test]
    async fn test_load_master_key_accepts_beta5_sha256_encrypted_record() {
        let temp_dir = TempDir::new().expect("create beta.5 fixture directory");
        let stored_key = serde_json::json!({
            "key_id": "beta5-key",
            "version": 1u32,
            "algorithm": "AES_256",
            "usage": "EncryptDecrypt",
            "status": "Active",
            "description": serde_json::Value::Null,
            "metadata": HashMap::<String, String>::new(),
            "created_at": "2024-01-01T00:00:00+00:00",
            "rotated_at": serde_json::Value::Null,
            "created_by": "beta5-fixture",
            "encrypted_key_material": "xjwGa4Lj4qzKg6XQl8s2btyFkPHPChMAkjqs268TFGyvFUv8WjDD5HQCUDLViZmt",
            "nonce": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
        });
        fs::write(
            temp_dir.path().join("beta5-key.key"),
            serde_json::to_vec_pretty(&stored_key).expect("serialize beta.5 fixture"),
        )
        .await
        .expect("write beta.5 fixture");
        let client = LocalKmsClient::new(LocalConfig {
            key_dir: temp_dir.path().to_path_buf(),
            master_key: Some("beta5-test-master-key".to_string()),
            file_permissions: Some(0o600),
        })
        .await
        .expect("initialize local KMS for beta.5 fixture");

        let material = client
            .get_key_material("beta5-key")
            .await
            .expect("decrypt beta.5 SHA-256 protected key");
        assert_eq!(material, vec![0x42; 32]);

        let mut explicit_protection = stored_key.clone();
        let explicit_object = explicit_protection.as_object_mut().expect("beta.5 fixture is a JSON object");
        explicit_object.insert("key_id".to_string(), serde_json::json!("beta5-explicit-key"));
        explicit_object.insert("at_rest_protection".to_string(), serde_json::json!("encrypted-master-key"));
        fs::write(
            temp_dir.path().join("beta5-explicit-key.key"),
            serde_json::to_vec_pretty(&explicit_protection).expect("serialize explicit-protection fixture"),
        )
        .await
        .expect("write explicit-protection fixture");
        let explicit_error = client
            .get_key_material("beta5-explicit-key")
            .await
            .expect_err("explicit current protection must not fall back to the beta.5 KDF");
        assert!(matches!(explicit_error, KmsError::MaterialAuthenticationFailed { .. }));

        let wrong_key_error = match LocalKmsClient::new(LocalConfig {
            key_dir: temp_dir.path().to_path_buf(),
            master_key: Some("wrong-beta5-master-key".to_string()),
            file_permissions: Some(0o600),
        })
        .await
        {
            Ok(_) => panic!("wrong beta.5 master key must fail initialization"),
            Err(error) => error,
        };
        assert!(matches!(wrong_key_error, KmsError::MaterialAuthenticationFailed { .. }));
    }

    /// R03-CAN-072 / R03-CAN-073: key identifiers arrive from request input, so every path
    /// derived from one must stay inside the configured key directory. Traversal here would
    /// turn CreateKey into a constrained arbitrary-file write and DeleteKey into a
    /// cross-directory delete.
    #[tokio::test]
    async fn master_key_path_confines_key_ids_to_the_key_directory() {
        let (client, temp_dir) = create_test_client().await;

        // The invariant is containment, so assert that directly: whatever the input, the
        // result is either refused or a path whose parent is exactly the key directory.
        // Note `.` and `..` are contained rather than refused — the `.key` suffix turns
        // them into the ordinary filenames `..key` and `...key`.
        for candidate in [
            "../escape",
            "../../etc/rustfs",
            "sub/dir",
            "..",
            ".",
            "",
            "/absolute",
            "back\\slash",
            "nul\0byte",
            "....//....//escape",
        ] {
            match client.master_key_path(candidate) {
                Err(KmsError::InvalidKey { .. }) => {}
                Err(other) => panic!("unexpected error kind for {candidate:?}: {other:?}"),
                Ok(path) => assert_eq!(
                    path.parent(),
                    Some(temp_dir.path()),
                    "{candidate:?} was accepted but escapes the key directory: {path:?}"
                ),
            }
        }

        // The traversal forms specifically must be refused, not merely contained.
        for escaping in ["../escape", "sub/dir", "/absolute", "back\\slash", "nul\0byte", ""] {
            let err = client.master_key_path(escaping).expect_err("traversal must be refused");
            assert!(
                matches!(err, KmsError::InvalidKey { .. }),
                "expected InvalidKey for {escaping:?}, got {err:?}"
            );
        }

        // Ordinary identifiers, including the UUID form used when no name is supplied,
        // must still resolve — and must land directly in the key directory.
        for ok in ["test-key", "a.b_c-1", "3f2504e0-4f89-11d3-9a0c-0305e82c3301"] {
            let path = client.master_key_path(ok).expect("valid key id must be accepted");
            assert_eq!(
                path.parent(),
                Some(temp_dir.path()),
                "{ok:?} must resolve directly inside the key directory"
            );
        }
    }

    /// R07-CAN-103: creating a duplicate key must preserve its original material.
    #[tokio::test]
    async fn backend_create_key_refuses_to_replace_existing_key_material() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let client = LocalKmsClient::new(LocalConfig {
            key_dir: temp_dir.path().to_path_buf(),
            master_key: Some("test-master-key".to_string()),
            file_permissions: Some(0o600),
        })
        .await
        .expect("Failed to create client");
        let backend = LocalKmsBackend { client };

        let request = || CreateKeyRequest {
            key_name: Some("duplicate-key".to_string()),
            ..Default::default()
        };

        backend.create_key(request()).await.expect("first create must succeed");
        let original = backend
            .client
            .get_key_material("duplicate-key")
            .await
            .expect("key material must be readable after creation");

        let err = backend
            .create_key(request())
            .await
            .expect_err("creating a key under an existing name must be refused");
        assert!(matches!(err, KmsError::KeyAlreadyExists { .. }), "expected KeyAlreadyExists, got {err:?}");

        let after = backend
            .client
            .get_key_material("duplicate-key")
            .await
            .expect("original key material must survive the refused create");
        assert_eq!(original, after, "existing key material must not be replaced");
    }

    #[tokio::test]
    async fn concurrent_backend_create_allows_only_one_writer() {
        let temp_dir = TempDir::new().expect("create key directory");
        let config = || LocalConfig {
            key_dir: temp_dir.path().to_path_buf(),
            master_key: Some("test-master-key".to_string()),
            file_permissions: Some(0o600),
        };
        let first = LocalKmsBackend {
            client: LocalKmsClient::new(config()).await.expect("create first client"),
        };
        let second = LocalKmsBackend {
            client: LocalKmsClient::new(config()).await.expect("create second client"),
        };
        let request = || CreateKeyRequest {
            key_name: Some("concurrent-key".to_string()),
            ..Default::default()
        };

        let (first_result, second_result) = tokio::join!(first.create_key(request()), second.create_key(request()));
        assert_ne!(first_result.is_ok(), second_result.is_ok(), "exactly one create must succeed");
        let error = first_result
            .err()
            .or_else(|| second_result.err())
            .expect("one create must fail");
        assert!(matches!(error, KmsError::KeyAlreadyExists { .. }));
        assert_eq!(first.client.get_key_material("concurrent-key").await.expect("load key").len(), 32);
    }

    fn test_config(dir: &std::path::Path) -> LocalConfig {
        LocalConfig {
            key_dir: dir.to_path_buf(),
            master_key: Some("test-master-key".to_string()),
            file_permissions: Some(0o600),
        }
    }

    async fn sorted_dir_file_names(dir: &std::path::Path) -> Vec<String> {
        let mut names = Vec::new();
        let mut entries = fs::read_dir(dir).await.expect("read key directory");
        while let Some(entry) = entries.next_entry().await.expect("read directory entry") {
            names.push(entry.file_name().to_str().expect("UTF-8 file name").to_string());
        }
        names.sort();
        names
    }

    const ALL_COMMIT_STEPS: [durable_file::CommitStep; 4] = [
        durable_file::CommitStep::TempWritten,
        durable_file::CommitStep::FileSynced,
        durable_file::CommitStep::Published,
        durable_file::CommitStep::DirSynced,
    ];

    #[test]
    fn orphan_commit_temp_matcher_is_strict() {
        let uuid = uuid::Uuid::new_v4();
        // The two shapes the backend actually produces.
        assert!(is_orphan_commit_temp_name(&format!("mykey.tmp-{uuid}")));
        assert!(is_orphan_commit_temp_name(&format!(".master-key.salt.tmp-{uuid}")));
        // Authoritative files must never match, even with temp-looking names.
        assert!(!is_orphan_commit_temp_name("mykey.key"));
        assert!(!is_orphan_commit_temp_name(&format!("decoy.tmp-{uuid}.key")));
        assert!(!is_orphan_commit_temp_name(".master-key.salt"));
        // Near misses stay untouched.
        assert!(!is_orphan_commit_temp_name("mykey.tmp-not-a-uuid"));
        assert!(!is_orphan_commit_temp_name(&format!("mykey.tmp-{}", uuid.simple())));
        assert!(!is_orphan_commit_temp_name(&format!(".tmp-{uuid}")));
        assert!(!is_orphan_commit_temp_name("mykey.tmp-"));
    }

    #[tokio::test]
    async fn durable_commit_fsyncs_every_write_path() {
        use durable_file::fsync_recorder;

        let (client, temp_dir) = create_test_client().await;
        let dir = temp_dir.path();

        // Salt creation during construction is itself a durable commit.
        let salt_path = LocalKmsClient::master_key_salt_path(&client.config);
        assert!(fsync_recorder::file_sync_count(&salt_path) >= 1, "salt file must be fsynced");
        assert!(fsync_recorder::dir_sync_count(dir) >= 1, "salt publish must fsync the key directory");

        let key_path = client.master_key_path("durable-key").expect("valid key id");
        let files_before = fsync_recorder::file_sync_count(&key_path);
        let dirs_before = fsync_recorder::dir_sync_count(dir);
        client.create_key("durable-key", "AES_256", None).await.expect("create key");
        assert!(
            fsync_recorder::file_sync_count(&key_path) > files_before,
            "create must fsync the key file"
        );
        assert!(fsync_recorder::dir_sync_count(dir) > dirs_before, "create must fsync the key directory");

        let files_before = fsync_recorder::file_sync_count(&key_path);
        let dirs_before = fsync_recorder::dir_sync_count(dir);
        client.disable_key("durable-key", None).await.expect("disable key");
        assert!(
            fsync_recorder::file_sync_count(&key_path) > files_before,
            "update must fsync the key file"
        );
        assert!(fsync_recorder::dir_sync_count(dir) > dirs_before, "update must fsync the key directory");

        let backend = LocalKmsBackend { client };
        let dirs_before = fsync_recorder::dir_sync_count(dir);
        backend
            .delete_key(DeleteKeyRequest {
                key_id: "durable-key".to_string(),
                pending_window_in_days: None,
                force_immediate: Some(true),
            })
            .await
            .expect("delete key");
        assert!(!key_path.exists(), "immediate delete must remove the key file");
        assert!(fsync_recorder::dir_sync_count(dir) > dirs_before, "delete must fsync the key directory");
    }

    #[tokio::test]
    async fn interrupted_update_commit_recovers_to_complete_old_or_new_state() {
        use durable_file::{CommitStep, failpoint};

        for step in ALL_COMMIT_STEPS {
            let (client, temp_dir) = create_test_client().await;
            let key_id = "crash-update-key";
            client.create_key(key_id, "AES_256", None).await.expect("create key");
            let original_material = client.get_key_material(key_id).await.expect("original material");

            failpoint::arm(temp_dir.path(), step);
            let error = client
                .disable_key(key_id, None)
                .await
                .expect_err("armed commit must simulate a crash");
            failpoint::disarm(temp_dir.path());
            assert!(error.to_string().contains("injected crash"), "unexpected error: {error}");
            drop(client);

            // Restart on the same directory: recovery must observe either the
            // complete old state or the complete new state, with temps cleaned.
            let recovered = LocalKmsClient::new(test_config(temp_dir.path()))
                .await
                .expect("recovery after an interrupted update must succeed");
            let status = recovered
                .describe_key(key_id, None)
                .await
                .expect("key must survive an interrupted update")
                .status;
            let expected = if matches!(step, CommitStep::TempWritten | CommitStep::FileSynced) {
                // Crash before publish: the old state is authoritative.
                KeyStatus::Active
            } else {
                // Crash after publish: the new state is authoritative.
                KeyStatus::Disabled
            };
            assert_eq!(status, expected, "step {step:?} must recover to a complete state");
            assert_eq!(
                recovered.get_key_material(key_id).await.expect("material must survive"),
                original_material,
                "step {step:?} must preserve key material"
            );
            assert_eq!(
                sorted_dir_file_names(temp_dir.path()).await,
                vec![".master-key.salt".to_string(), format!("{key_id}.key")],
                "step {step:?} must leave no commit temps behind"
            );
        }
    }

    #[tokio::test]
    async fn interrupted_create_commit_recovers_to_absent_or_complete_key() {
        use durable_file::{CommitStep, failpoint};

        for step in ALL_COMMIT_STEPS {
            let (client, temp_dir) = create_test_client().await;
            let key_id = "crash-create-key";

            failpoint::arm(temp_dir.path(), step);
            client
                .create_key(key_id, "AES_256", None)
                .await
                .expect_err("armed commit must simulate a crash");
            failpoint::disarm(temp_dir.path());
            drop(client);

            let recovered = LocalKmsClient::new(test_config(temp_dir.path()))
                .await
                .expect("recovery after an interrupted create must succeed");
            if matches!(step, CommitStep::TempWritten | CommitStep::FileSynced) {
                // Crash before publish: the key was never created.
                let error = recovered
                    .describe_key(key_id, None)
                    .await
                    .expect_err("unpublished key must not exist after recovery");
                assert!(matches!(error, KmsError::KeyNotFound { .. }), "step {step:?}: {error:?}");
                assert_eq!(
                    sorted_dir_file_names(temp_dir.path()).await,
                    vec![".master-key.salt".to_string()],
                    "step {step:?} must remove the unpublished temp"
                );
            } else {
                // Crash after publish: the key is complete and usable.
                let material = recovered
                    .get_key_material(key_id)
                    .await
                    .expect("published key must survive recovery");
                assert_eq!(material.len(), 32, "step {step:?} must keep complete key material");
                assert_eq!(
                    sorted_dir_file_names(temp_dir.path()).await,
                    vec![".master-key.salt".to_string(), format!("{key_id}.key")],
                    "step {step:?} must leave only the published key"
                );
            }
        }
    }

    #[tokio::test]
    async fn interrupted_salt_commit_recovers_cleanly() {
        use durable_file::{CommitStep, failpoint};

        for step in ALL_COMMIT_STEPS {
            let temp_dir = TempDir::new().expect("create temp dir");
            let config = test_config(temp_dir.path());
            let salt_path = LocalKmsClient::master_key_salt_path(&config);

            failpoint::arm(temp_dir.path(), step);
            let error = match LocalKmsClient::new(config.clone()).await {
                Ok(_) => panic!("armed salt commit must fail initialization"),
                Err(error) => error,
            };
            failpoint::disarm(temp_dir.path());
            assert!(error.to_string().contains("injected crash"), "unexpected error: {error}");

            // If the crash hit after publish, the salt is durable and must be
            // reused on restart; before publish, a fresh one may be generated.
            let published_salt = if matches!(step, CommitStep::Published | CommitStep::DirSynced) {
                Some(fs::read(&salt_path).await.expect("published salt must exist"))
            } else {
                assert!(!salt_path.exists(), "step {step:?} must not publish a salt");
                None
            };

            let client = LocalKmsClient::new(config).await.expect("recovery must succeed");
            let salt_now = fs::read(&salt_path).await.expect("salt must exist after recovery");
            if let Some(published) = published_salt {
                assert_eq!(salt_now, published, "step {step:?}: a published salt must be reused");
            }
            assert_eq!(
                sorted_dir_file_names(temp_dir.path()).await,
                vec![".master-key.salt".to_string()],
                "step {step:?} must leave exactly one salt file"
            );
            client
                .create_key("post-recovery-key", "AES_256", None)
                .await
                .expect("create key");
        }
    }

    #[tokio::test]
    async fn startup_removes_only_strictly_matching_commit_temps() {
        let (client, temp_dir) = create_test_client().await;
        client.create_key("real-key", "AES_256", None).await.expect("create key");
        // A key whose name itself looks like a temp is stored with `.key` and
        // must survive cleanup.
        let decoy_id = format!("decoy.tmp-{}", uuid::Uuid::new_v4());
        client.create_key(&decoy_id, "AES_256", None).await.expect("create decoy key");
        drop(client);

        let key_temp = temp_dir.path().join(format!("real-key.tmp-{}", uuid::Uuid::new_v4()));
        let salt_temp = temp_dir.path().join(format!(".master-key.salt.tmp-{}", uuid::Uuid::new_v4()));
        let not_a_uuid = temp_dir.path().join("real-key.tmp-not-a-uuid");
        let stray = temp_dir.path().join("operator-notes.txt");
        for path in [&key_temp, &salt_temp, &not_a_uuid, &stray] {
            fs::write(path, b"leftover").await.expect("seed leftover file");
        }

        let client = LocalKmsClient::new(test_config(temp_dir.path()))
            .await
            .expect("restart with leftover temps must succeed");

        assert!(!key_temp.exists(), "key commit temp must be removed");
        assert!(!salt_temp.exists(), "salt commit temp must be removed");
        assert!(not_a_uuid.exists(), "non-UUID suffixes must not match the temp pattern");
        assert!(stray.exists(), "unrelated files must be left alone");
        client
            .describe_key("real-key", None)
            .await
            .expect("real key must survive cleanup");
        client
            .describe_key(&decoy_id, None)
            .await
            .expect("temp-looking key name must survive cleanup");
    }

    #[tokio::test]
    async fn missing_salt_with_encrypted_keys_fails_closed_without_generating_a_salt() {
        let (client, temp_dir) = create_test_client().await;
        client
            .create_key("sealed-key", "AES_256", None)
            .await
            .expect("create encrypted key");
        let config = client.config.clone();
        drop(client);

        let salt_path = LocalKmsClient::master_key_salt_path(&config);
        fs::remove_file(&salt_path).await.expect("remove salt file");

        let error = match LocalKmsClient::new(config).await {
            Ok(_) => panic!("missing salt with encrypted keys must fail initialization"),
            Err(error) => error,
        };
        assert!(
            matches!(error, KmsError::ConfigurationError { .. }),
            "expected a salt-specific configuration error, got {error:?}"
        );
        assert!(error.to_string().contains("salt"), "error must point at the missing salt: {error}");
        assert!(!salt_path.exists(), "a replacement salt must never be generated");
        assert_eq!(
            sorted_dir_file_names(temp_dir.path()).await,
            vec!["sealed-key.key".to_string()],
            "the failed startup must not modify the key directory"
        );
    }

    #[tokio::test]
    async fn missing_salt_with_only_plaintext_dev_keys_still_initializes() {
        let (dev_client, temp_dir) = create_dev_mode_client().await;
        dev_client
            .create_key("plain-key", "AES_256", None)
            .await
            .expect("create plaintext-dev-only key");
        drop(dev_client);

        // Enabling a master key over a directory of plaintext-dev-only keys is
        // a legitimate first-time salt creation, not a lost salt.
        let client = LocalKmsClient::new(test_config(temp_dir.path()))
            .await
            .expect("salt creation must proceed for plaintext-dev-only directories");
        assert!(LocalKmsClient::master_key_salt_path(&client.config).exists());
    }

    #[tokio::test]
    async fn per_key_write_lock_blocks_concurrent_status_updates() {
        let (client, _temp_dir) = create_test_client().await;
        let client = Arc::new(client);
        client.create_key("locked-key", "AES_256", None).await.expect("create key");
        let key_path = client.master_key_path("locked-key").expect("valid key id");

        let guard = client.lock_key_for_write("locked-key").await;
        let contender = {
            let client = Arc::clone(&client);
            tokio::spawn(async move { client.disable_key("locked-key", None).await })
        };
        // Drive the runtime through enough polls and blocking-pool round trips
        // that the contender would have finished if it did not honor the lock.
        for _ in 0..8 {
            tokio::task::yield_now().await;
            let _ = fs::metadata(&key_path).await;
        }
        let status = client.describe_key("locked-key", None).await.expect("describe key").status;
        assert_eq!(
            status,
            KeyStatus::Active,
            "a status update must not proceed while the per-key write lock is held"
        );

        drop(guard);
        contender
            .await
            .expect("join contender")
            .expect("disable must succeed once the lock is released");
        let status = client.describe_key("locked-key", None).await.expect("describe key").status;
        assert_eq!(status, KeyStatus::Disabled);
    }

    #[tokio::test]
    async fn concurrent_status_updates_preserve_material_and_a_complete_state() {
        let (client, _temp_dir) = create_test_client().await;
        let key_id = "contended-key";
        client.create_key(key_id, "AES_256", None).await.expect("create key");
        let original_material = client.get_key_material(key_id).await.expect("original material");

        let (disable, schedule, enable) = tokio::join!(
            client.disable_key(key_id, None),
            client.schedule_key_deletion(key_id, 7, None),
            client.enable_key(key_id, None),
        );
        // The per-key lock serializes the three transitions in an arbitrary
        // order, and the state gate may legitimately reject a transition that
        // lost the race (e.g. enable after deletion was scheduled). Any other
        // error kind would still mean corrupted storage.
        for result in [disable, schedule, enable] {
            match result {
                Ok(()) | Err(KmsError::InvalidOperation { .. }) => {}
                Err(other) => panic!("concurrent transition must only fail with a state rejection, got {other:?}"),
            }
        }

        // Whatever the serialization order, the file must be one writer's
        // complete output with the original material intact.
        let info = client.describe_key(key_id, None).await.expect("key file must stay decodable");
        assert!(matches!(
            info.status,
            KeyStatus::Active | KeyStatus::Disabled | KeyStatus::PendingDeletion
        ));
        assert_eq!(
            client.get_key_material(key_id).await.expect("material must stay readable"),
            original_material,
            "concurrent status updates must never lose or regenerate key material"
        );
    }

    /// Records written before deadline persistence landed have no
    /// deletion_date field and must keep deserializing (as None).
    #[tokio::test]
    async fn stored_master_key_without_deletion_date_still_deserializes() {
        let (client, _temp_dir) = create_test_client().await;
        client.create_key("legacy-key", "AES_256", None).await.expect("create key");

        let path = client.master_key_path("legacy-key").expect("key path");
        let bytes = fs::read(&path).await.expect("read stored key");
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).expect("stored key must be JSON");
        value
            .as_object_mut()
            .expect("stored key must be a JSON object")
            .remove("deletion_date")
            .expect("current records must carry the field");

        let stored: StoredMasterKey = serde_json::from_value(value).expect("legacy record must deserialize");
        assert!(stored.deletion_date.is_none());
    }

    #[tokio::test]
    async fn remove_expired_key_completes_a_tombstone_and_stays_idempotent() {
        let temp_dir = TempDir::new().expect("temp dir");
        let config = KmsConfig::local(temp_dir.path().to_path_buf()).with_insecure_development_defaults();
        let backend = LocalKmsBackend::new(config).await.expect("backend");
        let created = backend
            .create_key(CreateKeyRequest {
                key_name: Some("tombstoned-key".to_string()),
                key_usage: KeyUsage::EncryptDecrypt,
                ..Default::default()
            })
            .await
            .expect("create key");
        let key_id = created.key_id;

        // Craft the state a removal crashed in: tombstone written, file not
        // yet removed.
        let client = backend.lifecycle_client();
        let (_stored, key_material) = client.decode_stored_key(&key_id).await.expect("decode stored key");
        let mut tombstone = client.load_master_key(&key_id).await.expect("load key");
        tombstone.status = KeyStatus::Deleted;
        tombstone.deletion_date = Some(Zoned::now());
        client
            .save_master_key(&tombstone, &key_material)
            .await
            .expect("write tombstone");

        // The sweep primitive completes the crashed removal...
        let outcome = backend
            .remove_expired_key(&key_id, &Zoned::now())
            .await
            .expect("tombstone completion");
        assert_eq!(outcome, crate::backends::ExpiredKeyRemoval::Removed);
        assert!(!client.master_key_path(&key_id).expect("key path").exists());

        // ...and stays idempotent once the key is gone.
        let outcome = backend
            .remove_expired_key(&key_id, &Zoned::now())
            .await
            .expect("repeat removal");
        assert_eq!(outcome, crate::backends::ExpiredKeyRemoval::Removed);
    }
}
