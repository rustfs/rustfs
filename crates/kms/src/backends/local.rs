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

use crate::backends::{
    BackendCapabilities, ExpiredKeyRemoval, KmsBackend, ListedKeyFailure, StateGatedOperation, UnreadableKeys,
    classify_listed_key_failure, ensure_key_status_permits, ensure_tag_keys_are_mutable, paginate_keys, started_at_the_first_key,
};
use crate::config::KmsConfig;
use crate::config::LocalConfig;
use crate::encryption::{AesDekCrypto, DataKeyEnvelope, DekCrypto, generate_key_material};
use crate::error::{KmsError, Result};
use crate::persisted_observability::{BoundedUnknownFieldName, UnknownFieldSummary};
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
use serde::de::{self, IgnoredAny, MapAccess, Visitor};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
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
///
/// pub(crate) because the backup restore path applies the same containment
/// rule to key identifiers recovered from bundle artifacts.
pub(crate) fn validate_key_id(key_id: &str) -> Result<()> {
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

// The salt and restore-marker file names are pub(crate) so the backup/restore
// modules (`crate::backup`) address the exact on-disk names instead of copies
// that could drift.
pub(crate) const LOCAL_KMS_MASTER_KEY_SALT_FILE: &str = ".master-key.salt";
/// Commit marker of an in-progress Local restore cutover (see
/// `crate::backup::local_restore`). Its presence means the key directory is
/// mid-cutover: startup must fail closed until the restore is rolled forward
/// or explicitly aborted.
pub(crate) const LOCAL_RESTORE_COMMIT_MARKER_FILE: &str = ".restore-commit.json";
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
///
/// pub(crate) because the backup restore path applies the same classification
/// when it re-enters an interrupted run: a leftover commit temp is never
/// authoritative state, so it does not make a target non-empty.
pub(crate) fn is_orphan_commit_temp_name(file_name: &str) -> bool {
    if file_name.ends_with(".key") {
        return false;
    }
    let Some((prefix, suffix)) = file_name.rsplit_once(".tmp-") else {
        return false;
    };
    !prefix.is_empty() && suffix.len() == 36 && uuid::Uuid::try_parse(suffix).is_ok()
}

/// Mode every key-directory file is written with when the deployment does not
/// name one.
///
/// `file_permissions` is optional in the persisted configuration and stays
/// optional for compatibility, but "unspecified" must not mean "whatever the
/// umask says": a `0` umask — the default in a good many container images —
/// would publish master key records world-readable. Owner-only is the only
/// defensible reading of an absent value for a file holding key material.
pub(crate) const DEFAULT_KEY_FILE_MODE: u32 = 0o600;

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
///
/// pub(crate) because the backup restore path (`crate::backup::local_restore`)
/// commits staged files and its cutover marker through the same protocol.
pub(crate) mod durable_file {
    use std::io::{self, Write};
    use std::path::{Path, PathBuf};

    /// How the fully written temp file becomes visible under its final name.
    pub(crate) enum Publish {
        /// Atomically replace whatever is at the destination via `rename`.
        Replace,
        /// Publish via `hard_link`, failing with [`CommitError::AlreadyExists`]
        /// when the destination exists so concurrent creates stay linearized.
        NoClobber,
    }

    #[derive(Debug)]
    pub(crate) enum CommitError {
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
    pub(crate) enum CommitStep {
        TempWritten,
        FileSynced,
        Published,
        DirSynced,
    }

    pub(crate) async fn commit(
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
    pub(crate) async fn remove_durably(path: PathBuf) -> io::Result<()> {
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

    /// Publish an already-durable file under a second name via `hard_link`,
    /// then fsync the destination's parent directory.
    ///
    /// This is the restore cutover primitive: the source (a staged file that
    /// went through [`commit`]) is already durable, so linking plus a parent
    /// fsync is a complete publish. `AlreadyExists` is idempotent success only
    /// when the destination content is byte-identical to the source — that is
    /// exactly the re-entry case of a cutover interrupted after this link —
    /// and a hard failure otherwise, so the primitive can never clobber or
    /// silently accept foreign state.
    pub(crate) async fn link_durably(source: PathBuf, dest: PathBuf) -> io::Result<()> {
        tokio::task::spawn_blocking(move || {
            match std::fs::hard_link(&source, &dest) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    let existing = std::fs::read(&dest)?;
                    let staged = std::fs::read(&source)?;
                    if existing != staged {
                        return Err(io::Error::new(
                            io::ErrorKind::AlreadyExists,
                            format!("destination {} already exists with different content", dest.display()),
                        ));
                    }
                }
                Err(error) => return Err(error),
            }
            let parent = dest
                .parent()
                .ok_or_else(|| io::Error::other("destination has no parent directory"))?;
            fsync_dir(parent)
        })
        .await
        .map_err(io::Error::other)?
    }

    /// The mode a key-directory file is published with.
    ///
    /// Exposed so the "absent means owner-only" rule can be asserted directly:
    /// observing it through a written file only proves anything on a host whose
    /// umask is not already masking the same bits.
    pub(crate) fn resolved_file_mode(permissions: Option<u32>) -> u32 {
        permissions.unwrap_or(super::DEFAULT_KEY_FILE_MODE)
    }

    fn commit_blocking(
        temp_path: &Path,
        final_path: &Path,
        content: &[u8],
        permissions: Option<u32>,
        publish: &Publish,
    ) -> Result<(), CommitError> {
        // Resolved here rather than at each call site so no caller can publish
        // a key-directory file at the umask's mercy by leaving the mode unset —
        // the backup restore path did exactly that, and its files landed in the
        // same directory as records written owner-only by every other path.
        let permissions = Some(resolved_file_mode(permissions));
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
    pub(crate) mod failpoint {
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
    /// Directory-wide writer fence for backup export (see
    /// [`Self::acquire_export_fence`]). Writers hold the read side; an export
    /// snapshot holds the write side so it observes a single-generation view.
    export_fence: Arc<tokio::sync::RwLock<()>>,
}

/// Guard pairing the export-fence read lock with a per-key write mutex.
///
/// Dropping it releases both, so every existing `lock_key_for_write` call
/// site participates in the export fence without changes.
#[must_use]
struct KeyWriteGuard {
    _fence: tokio::sync::OwnedRwLockReadGuard<()>,
    _key: tokio::sync::OwnedMutexGuard<()>,
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

pub(crate) const UNKNOWN_STORED_KEY_PROTECTION: &str = "unknown-at-rest-protection";
const MAX_PROTECTION_MARKER_RAW_BYTES: usize = 128;

impl UnknownFieldSummary {
    fn record_for_local_key(&self) {
        let Some((field, field_name_truncated, field_count)) = self.record("local-key-record") else {
            return;
        };

        static RECORDS_WITH_UNKNOWN_FIELDS: AtomicU64 = AtomicU64::new(0);
        let observed_records = RECORDS_WITH_UNKNOWN_FIELDS.fetch_add(1, Ordering::Relaxed).saturating_add(1);
        if observed_records.is_power_of_two() {
            tracing::warn!(
                field = ?field,
                field_name_truncated,
                field_count,
                observed_records,
                "Local KMS key record contains unknown fields"
            );
        }
    }
}

/// Reports whether the record's `at_rest_protection` value is unknown to this
/// build. `false` means the marker is absent (pre-beta.9 records), null, or
/// names a protection mode this build implements.
///
/// Every reader of a stored key record must consult this before its own
/// schema parse. Letting a strict [`StoredKeyProtection`] field fail inside a
/// larger struct collapses "written by a newer build" into "corrupt", and an
/// operator who reads corruption starts a disaster recovery instead of a
/// version rollback. The probe deliberately ignores every other field, so the
/// verdict is available even for records whose schema this build cannot
/// satisfy. The raw marker is borrowed and length-bounded before enum parsing;
/// it is never propagated into a caller-visible error or diagnostic.
///
/// `Err` carries the JSON error so callers can keep their own classification
/// for bytes that are not a record at all.
pub(crate) fn has_unknown_protection_marker(record: &[u8]) -> serde_json::Result<bool> {
    #[derive(Deserialize)]
    struct MarkerProbe<'a> {
        #[serde(default)]
        #[serde(borrow)]
        at_rest_protection: Option<&'a serde_json::value::RawValue>,
    }

    let Some(marker) = serde_json::from_slice::<MarkerProbe>(record)?.at_rest_protection else {
        return Ok(false);
    };
    if marker.get().len() > MAX_PROTECTION_MARKER_RAW_BYTES {
        return Ok(true);
    }
    if serde_json::from_str::<StoredKeyProtection>(marker.get()).is_ok() {
        return Ok(false);
    }
    Ok(true)
}

/// Serializable representation of a master key stored on disk
#[derive(Debug, Clone, Serialize)]
struct StoredMasterKey {
    /// Persisted record schema version. Records written before this field was
    /// introduced default to version 1 during deserialization.
    #[serde(default = "default_stored_master_key_format_version")]
    format_version: u32,
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

pub(crate) const STORED_MASTER_KEY_FORMAT_VERSION: u32 = 1;

fn default_stored_master_key_format_version() -> u32 {
    STORED_MASTER_KEY_FORMAT_VERSION
}

/// Read only the schema marker before attempting the complete key-record
/// decode. A future record may add or remove required fields, but its version
/// still needs to be reported as unsupported rather than as generic corruption.
pub(crate) fn stored_master_key_format_version(record: &[u8]) -> serde_json::Result<u32> {
    #[derive(Deserialize)]
    struct FormatProbe {
        #[serde(default = "default_stored_master_key_format_version")]
        format_version: u32,
    }

    Ok(serde_json::from_slice::<FormatProbe>(record)?.format_version)
}

impl<'de> Deserialize<'de> for StoredMasterKey {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        enum Field {
            FormatVersion,
            KeyId,
            Version,
            Algorithm,
            Usage,
            Status,
            Description,
            Metadata,
            CreatedAt,
            RotatedAt,
            CreatedBy,
            DeletionDate,
            EncryptedKeyMaterial,
            Nonce,
            AtRestProtection,
            Unknown(BoundedUnknownFieldName),
        }

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct FieldVisitor;

                impl Visitor<'_> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                        formatter.write_str("a Local KMS key record field name")
                    }

                    fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
                    where
                        E: de::Error,
                    {
                        Ok(match value {
                            "format_version" => Field::FormatVersion,
                            "key_id" => Field::KeyId,
                            "version" => Field::Version,
                            "algorithm" => Field::Algorithm,
                            "usage" => Field::Usage,
                            "status" => Field::Status,
                            "description" => Field::Description,
                            "metadata" => Field::Metadata,
                            "created_at" => Field::CreatedAt,
                            "rotated_at" => Field::RotatedAt,
                            "created_by" => Field::CreatedBy,
                            "deletion_date" => Field::DeletionDate,
                            "encrypted_key_material" => Field::EncryptedKeyMaterial,
                            "nonce" => Field::Nonce,
                            "at_rest_protection" => Field::AtRestProtection,
                            _ => Field::Unknown(BoundedUnknownFieldName::new(value)),
                        })
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        #[derive(Deserialize)]
        struct ZonedValue(#[serde(with = "crate::time_serde::zoned")] Zoned);

        #[derive(Deserialize)]
        struct OptionalZonedValue(#[serde(with = "crate::time_serde::option_zoned")] Option<Zoned>);

        struct StoredMasterKeyVisitor;

        impl<'de> Visitor<'de> for StoredMasterKeyVisitor {
            type Value = StoredMasterKey;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a Local KMS key record")
            }

            fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                macro_rules! read_field {
                    ($slot:ident, $name:literal) => {{
                        if $slot.is_some() {
                            return Err(de::Error::duplicate_field($name));
                        }
                        $slot = Some(map.next_value()?);
                    }};
                }

                let mut format_version = None;
                let mut key_id = None;
                let mut version = None;
                let mut algorithm = None;
                let mut usage = None;
                let mut status = None;
                let mut description = None;
                let mut metadata = None;
                let mut created_at: Option<ZonedValue> = None;
                let mut rotated_at: Option<OptionalZonedValue> = None;
                let mut created_by = None;
                let mut deletion_date: Option<OptionalZonedValue> = None;
                let mut encrypted_key_material = None;
                let mut nonce = None;
                let mut at_rest_protection = None;
                let mut unknown_fields = UnknownFieldSummary::default();

                while let Some(field) = map.next_key()? {
                    match field {
                        Field::FormatVersion => read_field!(format_version, "format_version"),
                        Field::KeyId => read_field!(key_id, "key_id"),
                        Field::Version => read_field!(version, "version"),
                        Field::Algorithm => read_field!(algorithm, "algorithm"),
                        Field::Usage => read_field!(usage, "usage"),
                        Field::Status => read_field!(status, "status"),
                        Field::Description => read_field!(description, "description"),
                        Field::Metadata => read_field!(metadata, "metadata"),
                        Field::CreatedAt => read_field!(created_at, "created_at"),
                        Field::RotatedAt => read_field!(rotated_at, "rotated_at"),
                        Field::CreatedBy => read_field!(created_by, "created_by"),
                        Field::DeletionDate => read_field!(deletion_date, "deletion_date"),
                        Field::EncryptedKeyMaterial => read_field!(encrypted_key_material, "encrypted_key_material"),
                        Field::Nonce => read_field!(nonce, "nonce"),
                        Field::AtRestProtection => read_field!(at_rest_protection, "at_rest_protection"),
                        Field::Unknown(field) => {
                            let _: IgnoredAny = map.next_value()?;
                            unknown_fields.observe(field);
                        }
                    }
                }

                let key = StoredMasterKey {
                    format_version: format_version.unwrap_or_else(default_stored_master_key_format_version),
                    key_id: key_id.ok_or_else(|| de::Error::missing_field("key_id"))?,
                    version: version.ok_or_else(|| de::Error::missing_field("version"))?,
                    algorithm: algorithm.ok_or_else(|| de::Error::missing_field("algorithm"))?,
                    usage: usage.ok_or_else(|| de::Error::missing_field("usage"))?,
                    status: status.ok_or_else(|| de::Error::missing_field("status"))?,
                    description: description.unwrap_or(None),
                    metadata: metadata.ok_or_else(|| de::Error::missing_field("metadata"))?,
                    created_at: created_at.ok_or_else(|| de::Error::missing_field("created_at"))?.0,
                    rotated_at: rotated_at.map(|value: OptionalZonedValue| value.0).unwrap_or(None),
                    created_by: created_by.unwrap_or(None),
                    deletion_date: deletion_date.map(|value: OptionalZonedValue| value.0).unwrap_or(None),
                    encrypted_key_material: encrypted_key_material
                        .ok_or_else(|| de::Error::missing_field("encrypted_key_material"))?,
                    nonce: nonce.ok_or_else(|| de::Error::missing_field("nonce"))?,
                    at_rest_protection: at_rest_protection.unwrap_or_default(),
                };
                unknown_fields.record_for_local_key();
                Ok(key)
            }
        }

        const FIELDS: &[&str] = &[
            "format_version",
            "key_id",
            "version",
            "algorithm",
            "usage",
            "status",
            "description",
            "metadata",
            "created_at",
            "rotated_at",
            "created_by",
            "deletion_date",
            "encrypted_key_material",
            "nonce",
            "at_rest_protection",
        ];
        deserializer.deserialize_struct("StoredMasterKey", FIELDS, StoredMasterKeyVisitor)
    }
}

impl LocalKmsClient {
    /// Create a new local KMS client
    pub async fn new(config: LocalConfig) -> Result<Self> {
        // Create key directory if it doesn't exist
        if !fs::try_exists(&config.key_dir).await? {
            Self::create_key_dir(&config.key_dir).await?;
            debug!(path = ?config.key_dir, "KMS key directory created");
        }
        Self::secure_key_dir(&config.key_dir).await?;

        // The restore-marker guard must run before anything else touches the
        // directory (in particular before salt load/creation): a directory
        // mid-cutover holds an arbitrary mix of old and new state.
        Self::ensure_no_restore_marker(&config).await?;

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
            export_fence: Arc::new(tokio::sync::RwLock::new(())),
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
        Self::ensure_no_restore_marker(&config).await?;

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
            export_fence: Arc::new(tokio::sync::RwLock::new(())),
        })
    }

    /// Serialize writers of one key within this process.
    ///
    /// Status updates are read-modify-write cycles over the key file, so two
    /// concurrent writers would silently drop one update or interleave a
    /// delete with a rewrite. Cross-process writers sharing a key directory
    /// remain unsupported. Entries live for the client's lifetime; the table
    /// is bounded by the number of distinct key ids this process touches.
    async fn lock_key_for_write(&self, key_id: &str) -> KeyWriteGuard {
        // Fence first, per-key mutex second: the ordering is uniform across
        // all writers, so an export waiting on the write side can never
        // deadlock with a writer holding a key mutex.
        let fence = Arc::clone(&self.export_fence).read_owned().await;
        let lock = {
            let mut locks = self.key_write_locks.lock().expect("Local KMS key write lock table poisoned");
            Arc::clone(locks.entry(key_id.to_string()).or_default())
        };
        KeyWriteGuard {
            _fence: fence,
            _key: lock.lock_owned().await,
        }
    }

    /// Block every key-directory writer while a backup export collects its
    /// snapshot, so all records belong to one generation.
    ///
    /// Mutating operations hold the read side (via [`Self::lock_key_for_write`]
    /// or [`Self::save_new_master_key`]); the export holds the write side only
    /// for the collection phase, never while encrypting or writing the bundle.
    pub(crate) async fn acquire_export_fence(&self) -> tokio::sync::OwnedRwLockWriteGuard<()> {
        Arc::clone(&self.export_fence).write_owned().await
    }

    /// Key directory root, exposed for the backup export module.
    pub(crate) fn key_directory(&self) -> &Path {
        &self.config.key_dir
    }

    /// Absolute path of the master-key KDF salt file, exposed for the backup
    /// export module.
    pub(crate) fn master_key_salt_file(&self) -> PathBuf {
        Self::master_key_salt_path(&self.config)
    }

    /// Operator-configured master key string, exposed for the backup export
    /// module so it can record a one-way verifier in the bundle manifest.
    /// Never log or persist this value.
    pub(crate) fn configured_master_key(&self) -> Option<&str> {
        self.config.master_key.as_deref()
    }

    /// Fail closed while a restore cutover marker is present: the directory
    /// then holds an arbitrary mix of pre-restore and restored state, and the
    /// only valid next steps are re-running the restore with the same bundle
    /// (roll forward) or explicitly aborting it. This mirrors the missing-salt
    /// guard: startup must never paper over a half-applied restore.
    /// Create the key directory, and every directory leading to it, owner-only.
    ///
    /// `DirBuilder::mode` applies to each directory the recursive create makes,
    /// so an intermediate component cannot be left at the umask's mercy, and it
    /// closes the window a create-then-chmod pair leaves open — during which
    /// the directory exists at whatever the umask allowed.
    async fn create_key_dir(key_dir: &Path) -> Result<()> {
        let key_dir = key_dir.to_path_buf();
        tokio::task::spawn_blocking(move || {
            let mut builder = std::fs::DirBuilder::new();
            builder.recursive(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(Self::KEY_DIR_MODE);
            }
            builder.create(&key_dir)
        })
        .await
        .map_err(|error| KmsError::internal_error(format!("key directory creation task failed: {error}")))??;
        Ok(())
    }

    /// Mode the key directory is held at: owner-only.
    #[cfg(unix)]
    pub(crate) const KEY_DIR_MODE: u32 = 0o700;

    /// Bring the key directory down to owner-only, and keep it there.
    ///
    /// A directory wider than owner-only is rarely a decision anyone made. The
    /// platform picks it: kubelet creates an `emptyDir` `0o777`, several PVC
    /// provisioners `mkdir -m 0777`, a `--tmpfs` mount lands at `1777`, and
    /// `create_dir_all` under a container's `0` umask does the same. Write
    /// access here is the power to delete a key — destroying every object it
    /// protects — or to plant a record for a key id that does not exist yet.
    ///
    /// So this narrows rather than refuses, matching how the observability
    /// stack already treats its own directory (`ensure_dir_permissions`).
    /// Refusing would turn every one of those platform defaults into a server
    /// that will not start — `init_kms_system` propagates out of startup — and
    /// would leave the exposure in place on the way out. Narrowing removes it.
    /// Only a directory this process cannot secure is fatal: at that point the
    /// mode is both dangerous and outside our control, and proceeding would
    /// write key material into it anyway.
    async fn secure_key_dir(key_dir: &Path) -> Result<()> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            // The full mode, sticky bit included: reporting `0o777` for a
            // `1777` directory sends an operator looking for something
            // `ls -ld` does not show.
            let before = fs::metadata(key_dir).await?.permissions().mode() & 0o7777;
            if before == Self::KEY_DIR_MODE {
                return Ok(());
            }

            if let Err(error) = fs::set_permissions(key_dir, std::fs::Permissions::from_mode(Self::KEY_DIR_MODE)).await {
                return Err(KmsError::configuration_error(format!(
                    "Local KMS key directory {} has mode {before:#o} and cannot be narrowed to {:#o} ({error}); key material must not be written into a directory this process cannot secure",
                    key_dir.display(),
                    Self::KEY_DIR_MODE
                )));
            }

            // Verified rather than assumed: a filesystem that ignores `chmod`
            // would otherwise leave the directory wide open behind a log line
            // saying it had been narrowed.
            let after = fs::metadata(key_dir).await?.permissions().mode() & 0o7777;
            if after != Self::KEY_DIR_MODE {
                return Err(KmsError::configuration_error(format!(
                    "Local KMS key directory {} is still mode {after:#o} after being set to {:#o}; this filesystem does not enforce permissions and must not hold key material",
                    key_dir.display(),
                    Self::KEY_DIR_MODE
                )));
            }

            if before & 0o077 != 0 {
                warn!(
                    path = ?key_dir,
                    previous_mode = format!("{before:#o}"),
                    "Local KMS key directory was reachable beyond its owner and has been narrowed to 0o700"
                );
            }
        }
        #[cfg(not(unix))]
        let _ = key_dir;
        Ok(())
    }

    async fn ensure_no_restore_marker(config: &LocalConfig) -> Result<()> {
        let marker = config.key_dir.join(LOCAL_RESTORE_COMMIT_MARKER_FILE);
        if fs::try_exists(&marker).await? {
            return Err(KmsError::configuration_error(format!(
                "Local KMS key directory has an unfinished restore (marker {} present); \
                 re-run the restore with the same bundle to roll it forward or abort it explicitly",
                marker.display()
            )));
        }
        Ok(())
    }

    /// Derive a 256-bit key from the master key string using a persistent Argon2id salt.
    ///
    /// pub(crate) because the backup restore path derives the same key from
    /// the operator-supplied master key and the bundled salt for its verifier
    /// check and staged decryption probe.
    pub(crate) fn derive_master_key(master_key: &str, salt: &[u8]) -> Result<Key<Aes256Gcm>> {
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

    pub(crate) fn derive_legacy_master_key(master_key: &str) -> Result<Key<Aes256Gcm>> {
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
    /// A record this build cannot read or cannot interpret blocks generation
    /// just as hard. Skipping it would publish a fresh salt over a directory
    /// whose protection state is unknown, and that publication is
    /// irreversible in the only way that matters: the next startup finds a
    /// salt file, never re-enters this guard, and the evidence that the real
    /// salt was missing is gone. Every record this rejects also fails startup
    /// key validation a few lines later, so no directory that initializes
    /// today stops initializing — the salt is simply no longer written first.
    ///
    /// Legacy pre-marker files stay allowed: they parse here, pre-beta.9
    /// directories legitimately have no salt file yet, and an empty directory
    /// must keep initializing as before.
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
            let content = fs::read(&path).await.map_err(|error| {
                KmsError::configuration_error(format!(
                    "Local KMS master key salt at {} is missing and key record {} cannot be read ({error}); \
                     refusing to generate a replacement salt while a record's protection state is unknown",
                    Self::master_key_salt_path(config).display(),
                    path.display()
                ))
            })?;
            let format_version = stored_master_key_format_version(&content).map_err(|error| {
                KmsError::configuration_error(format!(
                    "Local KMS master key salt at {} is missing and key record {} is not interpretable by this build ({error}); \
                     refusing to generate a replacement salt — restore the salt file from backup, or run a build that \
                     understands the record",
                    Self::master_key_salt_path(config).display(),
                    path.display()
                ))
            })?;
            if format_version > STORED_MASTER_KEY_FORMAT_VERSION {
                return Err(KmsError::configuration_error(format!(
                    "Local KMS master key salt at {} is missing and key record {} declares unsupported format version {format_version}; \
                     refusing to generate a replacement salt",
                    Self::master_key_salt_path(config).display(),
                    path.display()
                )));
            }
            let has_unknown_marker = has_unknown_protection_marker(&content).map_err(|error| {
                KmsError::configuration_error(format!(
                    "Local KMS master key salt at {} is missing and key record {} is not a readable JSON object ({error}); \
                     refusing to generate a replacement salt",
                    Self::master_key_salt_path(config).display(),
                    path.display()
                ))
            })?;
            if has_unknown_marker {
                return Err(KmsError::configuration_error(format!(
                    "Local KMS master key salt at {} is missing and key record {} uses {UNKNOWN_STORED_KEY_PROTECTION}; \
                     refusing to generate a replacement salt",
                    Self::master_key_salt_path(config).display(),
                    path.display()
                )));
            }
            let probe = serde_json::from_slice::<ProtectionProbe>(&content).map_err(|error| {
                KmsError::configuration_error(format!(
                    "Local KMS master key salt at {} is missing and key record {} is not interpretable by this build ({error}); \
                     refusing to generate a replacement salt — restore the salt file from backup, or run a build that \
                     understands the record",
                    Self::master_key_salt_path(config).display(),
                    path.display()
                ))
            })?;
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

        let format_version = stored_master_key_format_version(&content)
            .map_err(|e| KmsError::material_corrupt(key_id, format!("stored key record is not a readable JSON object: {e}")))?;
        if format_version > STORED_MASTER_KEY_FORMAT_VERSION {
            return Err(KmsError::unsupported_format_version(key_id, format_version.to_string()));
        }

        // Two-stage parse so an unrecognised protection marker is reported as an
        // unsupported format (a newer build may still read the key) instead of being
        // folded into generic corruption with every other malformed record.
        let has_unknown_marker = has_unknown_protection_marker(&content)
            .map_err(|e| KmsError::material_corrupt(key_id, format!("stored key record is not a readable JSON object: {e}")))?;
        if has_unknown_marker {
            return Err(KmsError::unsupported_format_version(key_id, UNKNOWN_STORED_KEY_PROTECTION));
        }
        let stored_key: StoredMasterKey = serde_json::from_slice(&content)
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
        // Creates never take the per-key write lock (`NoClobber` publishing
        // already linearizes them), so they join the export fence here. This
        // must stay the only fence acquisition on the create path: the fence
        // read lock is not reentrant while an export waits for the write side.
        let _fence = Arc::clone(&self.export_fence).read_owned().await;
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
            format_version: STORED_MASTER_KEY_FORMAT_VERSION,
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
            // `file_type` does not follow symlinks, and a symlink is exactly
            // as much an orphan of this protocol as a regular file is: the
            // commit protocol only ever creates temps with `create_new`, so
            // anything wearing a temp name is either our own leftover or
            // something planted, and neither belongs in the key directory.
            // Requiring `is_file` left symlinked temp names behind forever.
            if !entry.file_type().await?.is_dir()
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

        let (encrypted_key, nonce) = self.encrypt_with_master_key(&request.key_id, &request.plaintext).await?;

        // The ciphertext must be the same envelope `decrypt` parses: the nonce
        // and the bound context live in it, so handing back the bare AES-GCM
        // output would make every `encrypt` result permanently unopenable.
        let envelope = DataKeyEnvelope {
            key_id: uuid::Uuid::new_v4().to_string(),
            master_key_id: request.key_id.clone(),
            key_spec: key_info.algorithm.clone(),
            encrypted_key,
            nonce,
            encryption_context: request.encryption_context.clone(),
            created_at: Zoned::now(),
            // Local rotation is rejected, so the key has a single material version.
            master_key_version: None,
        };
        let ciphertext = serde_json::to_vec(&envelope)?;

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

        // NOTE: this comparison is an authorization check, not a cryptographic
        // binding. `DekCrypto` seals only the plaintext, so `encryption_context`
        // rides in the envelope unauthenticated: anyone able to rewrite the
        // stored envelope can rewrite this field and present a matching context.
        // The Static and Vault Transit backends do bind it (as AEAD AAD and as
        // the Transit KDF context respectively); closing the gap here needs a
        // versioned envelope, since existing ciphertext was sealed without AAD.
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

    /// Every key identifier in the key directory, sorted.
    ///
    /// `read_dir` order is arbitrary and may differ between calls over the same
    /// directory, so it cannot carry a pagination cursor. Sorting gives the key
    /// set a stable total order that a marker can point into.
    async fn sorted_key_ids(&self) -> Result<Vec<String>> {
        let mut key_ids = Vec::new();
        let mut entries = fs::read_dir(&self.config.key_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "key")
                && let Some(key_id) = path.file_stem().and_then(|stem| stem.to_str())
            {
                key_ids.push(key_id.to_string());
            }
        }
        key_ids.sort_unstable();
        Ok(key_ids)
    }

    /// One page of the key set, ordered by key identifier.
    ///
    /// Paging is real here rather than a first-page-only approximation:
    /// callers that must see every key — the deletion sweep above all, which
    /// only destroys expired material for keys it actually lists — depend on
    /// `truncated` and `next_marker` to reach past the first page.
    pub(crate) async fn list_keys(
        &self,
        request: &ListKeysRequest,
        _context: Option<&OperationContext>,
    ) -> Result<ListKeysResponse> {
        debug!("Listing keys");

        let key_ids = self.sorted_key_ids().await?;
        let page = paginate_keys(&key_ids, request, String::as_str);

        // Only the page is read from disk, so the cost of a list stays bounded
        // by the requested limit rather than by the size of the key set.
        let mut keys = Vec::with_capacity(page.items.len());
        let mut unreadable = UnreadableKeys::default();
        for key_id in page.items {
            let key_info = match self.describe_key(key_id, None).await {
                Ok(key_info) => {
                    unreadable.saw_readable();
                    key_info
                }
                Err(error) => match classify_listed_key_failure(&error) {
                    Some(ListedKeyFailure::Vanished) => {
                        debug!(key_id, "skipping key removed while listing");
                        continue;
                    }
                    Some(ListedKeyFailure::Unreadable) => {
                        warn!(key_id, %error, "listing a key record this build cannot describe");
                        unreadable.record(key_id, error);
                        continue;
                    }
                    None => return Err(error),
                },
            };

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
        }

        Ok(ListKeysResponse {
            keys,
            next_marker: page.next_marker,
            truncated: page.truncated,
            unreadable_key_ids: unreadable.into_reported_ids(!page.truncated && started_at_the_first_key(request))?,
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

    /// Read-modify-write of a key's mutable metadata under the per-key write
    /// lock, carrying the existing key material over untouched.
    ///
    /// `mutate` reports whether it changed anything; an unchanged record is
    /// never rewritten, so a repeated no-op update neither rewrites the key
    /// file nor risks a failed commit on an unrelated call.
    async fn update_key_metadata<F>(&self, key_id: &str, mutate: F) -> Result<()>
    where
        F: FnOnce(&mut MasterKeyInfo) -> Result<bool>,
    {
        let _write_guard = self.lock_key_for_write(key_id).await;
        let mut master_key = self.load_master_key(key_id).await?;
        if !mutate(&mut master_key)? {
            return Ok(());
        }

        // Preserve the existing key material (see enable_key): a metadata edit
        // must never regenerate the master key, or every DEK wrapped by it
        // becomes undecryptable.
        let key_material = self.get_key_material(key_id).await?;
        self.save_master_key(&master_key, &key_material).await?;

        debug!(key_id, "Local KMS key metadata updated");
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
            | crate::config::BackendConfig::Static(_)
            | crate::config::BackendConfig::Aws(_) => {
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

            let mut master_key = MasterKeyInfo::new_with_description(
                key_id.clone(),
                algorithm.to_string(),
                Some("local-kms".to_string()),
                request.description.clone(),
            );
            // Persist the caller's tags: the response below reports them, and
            // describe_key reads them back out of this record.
            master_key.metadata = request.tags.clone();

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

        // The envelope that was just opened names the master key that opened it.
        // Reporting "unknown" left every caller unable to tell which key was
        // actually used, which is what audit and key-rotation checks read.
        let envelope: DataKeyEnvelope = serde_json::from_slice(&request.ciphertext)?;

        Ok(DecryptResponse {
            plaintext,
            key_id: envelope.master_key_id,
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

            // Defensive: KmsManager::delete_key is the enforcement point for the
            // waiting window and rejects out-of-range requests before any
            // backend runs. This repeats the bound for callers holding a backend
            // handle directly (tests, maintenance tasks).
            let days = request.pending_window_in_days.unwrap_or(DEFAULT_PENDING_DELETION_WINDOW_DAYS);
            if !(MIN_PENDING_DELETION_WINDOW_DAYS..=MAX_PENDING_DELETION_WINDOW_DAYS).contains(&days) {
                return Err(KmsError::invalid_parameter(format!(
                    "pending_window_in_days must be between {MIN_PENDING_DELETION_WINDOW_DAYS} and {MAX_PENDING_DELETION_WINDOW_DAYS}"
                )));
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

    async fn update_key_description(&self, key_id: &str, description: Option<&str>) -> Result<()> {
        self.client
            .update_key_metadata(key_id, |master_key| {
                if master_key.description.as_deref() == description {
                    return Ok(false);
                }
                master_key.description = description.map(str::to_string);
                Ok(true)
            })
            .await
    }

    async fn tag_key(&self, key_id: &str, tags: &HashMap<String, String>) -> Result<()> {
        ensure_tag_keys_are_mutable(tags.keys().map(String::as_str))?;
        self.client
            .update_key_metadata(key_id, |master_key| {
                let mut changed = false;
                for (tag_key, value) in tags {
                    changed |= master_key.metadata.insert(tag_key.clone(), value.clone()).as_ref() != Some(value);
                }
                Ok(changed)
            })
            .await
    }

    async fn untag_key(&self, key_id: &str, tag_keys: &[String]) -> Result<()> {
        ensure_tag_keys_are_mutable(tag_keys.iter().map(String::as_str))?;
        self.client
            .update_key_metadata(key_id, |master_key| {
                let mut changed = false;
                for tag_key in tag_keys {
                    changed |= master_key.metadata.remove(tag_key).is_some();
                }
                Ok(changed)
            })
            .await
    }

    async fn health_check(&self) -> Result<bool> {
        self.client.health_check().await.map(|_| true)
    }

    fn local_backup_client(&self) -> Option<&LocalKmsClient> {
        Some(&self.client)
    }

    fn capabilities(&self) -> BackendCapabilities {
        // Rotation stays unadvertised until historical key versions can be
        // retained (see LocalKmsClient::rotate_key); without version history
        // there is also no versioning capability.
        BackendCapabilities::minimal()
            .with_enable_disable(true)
            .with_schedule_deletion(true)
            .with_physical_delete(true)
            .with_update_key_metadata(true)
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
    use crate::test_support::{deserialize_with_ignored_only_unknown, unknown_field_metric};
    use metrics_util::debugging::DebuggingRecorder;
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
                with_field("at_rest_protection", serde_json::json!("secret-marker-value-must-not-leak")),
                |e| matches!(e, KmsError::UnsupportedFormatVersion { version, .. } if version == UNKNOWN_STORED_KEY_PROTECTION),
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
    async fn stored_master_key_format_version_is_explicit_and_legacy_defaults_to_v1() {
        let (client, _temp_dir) = create_dev_mode_client().await;
        client.create_key("format-key", "AES_256", None).await.expect("create key");

        let key_path = client.master_key_path("format-key").expect("valid key id");
        let current_record = fs::read(&key_path).await.expect("read key record");
        let mut record: serde_json::Value = serde_json::from_slice(&current_record).expect("decode key record");
        assert_eq!(record.get("format_version"), Some(&serde_json::json!(STORED_MASTER_KEY_FORMAT_VERSION)));

        #[derive(Deserialize)]
        struct LegacyStoredMasterKeyProbe {
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
            #[serde(default, with = "crate::time_serde::option_zoned")]
            deletion_date: Option<Zoned>,
            encrypted_key_material: String,
            nonce: Vec<u8>,
            #[serde(default)]
            at_rest_protection: StoredKeyProtection,
        }
        let legacy: LegacyStoredMasterKeyProbe =
            serde_json::from_slice(&current_record).expect("the pre-format-version reader must accept a v1 record");
        let LegacyStoredMasterKeyProbe {
            key_id,
            version,
            algorithm,
            usage: _usage,
            status: _status,
            description: _description,
            metadata: _metadata,
            created_at: _created_at,
            rotated_at: _rotated_at,
            created_by: _created_by,
            deletion_date: _deletion_date,
            encrypted_key_material,
            nonce,
            at_rest_protection,
        } = legacy;
        assert_eq!(key_id, "format-key");
        assert_eq!(version, 1);
        assert_eq!(algorithm, "AES_256");
        assert!(!encrypted_key_material.is_empty());
        assert!(nonce.is_empty());
        assert_eq!(at_rest_protection, StoredKeyProtection::PlaintextDevOnly);

        // A record from before the explicit field was added remains readable.
        record
            .as_object_mut()
            .expect("key record is an object")
            .remove("format_version")
            .expect("current records carry format_version");
        fs::write(&key_path, serde_json::to_vec_pretty(&record).expect("encode legacy key record"))
            .await
            .expect("write legacy key record");
        let info = client
            .describe_key("format-key", None)
            .await
            .expect("legacy key record should load");
        assert_eq!(info.key_id, "format-key");
    }

    #[tokio::test]
    async fn stored_master_key_accepts_an_older_numeric_format_version() {
        let (client, _temp_dir) = create_dev_mode_client().await;
        client
            .create_key("older-format-key", "AES_256", None)
            .await
            .expect("create key");

        let key_path = client.master_key_path("older-format-key").expect("valid key id");
        let mut record: serde_json::Value =
            serde_json::from_slice(&fs::read(&key_path).await.expect("read key record")).expect("decode key record");
        record["format_version"] = serde_json::json!(0);
        fs::write(&key_path, serde_json::to_vec_pretty(&record).expect("encode older key record"))
            .await
            .expect("write older key record");

        let info = client
            .describe_key("older-format-key", None)
            .await
            .expect("older format version should remain readable");
        assert_eq!(info.key_id, "older-format-key");
    }

    #[tokio::test]
    async fn stored_master_key_rejects_a_newer_format_version_before_decrypting() {
        let (client, _temp_dir) = create_dev_mode_client().await;
        client
            .create_key("future-format-key", "AES_256", None)
            .await
            .expect("create key");

        let key_path = client.master_key_path("future-format-key").expect("valid key id");
        let mut record: serde_json::Value =
            serde_json::from_slice(&fs::read(&key_path).await.expect("read key record")).expect("decode key record");
        record["format_version"] = serde_json::json!(99);
        record.as_object_mut().expect("key record is an object").remove("usage");
        fs::write(&key_path, serde_json::to_vec_pretty(&record).expect("encode future key record"))
            .await
            .expect("write future key record");

        let error = client
            .describe_key("future-format-key", None)
            .await
            .expect_err("a newer key format must fail closed");
        assert!(matches!(
            error,
            KmsError::UnsupportedFormatVersion { key_id, version }
                if key_id == "future-format-key" && version == "99"
        ));
    }

    #[test]
    fn unknown_protection_marker_is_static_for_every_unknown_shape() {
        for marker in [
            serde_json::json!("secret-string-must-not-leak"),
            serde_json::json!({"future_mode": "secret-object-must-not-leak"}),
            serde_json::json!(["secret-array-must-not-leak"]),
            serde_json::json!(99),
        ] {
            let record =
                serde_json::to_vec(&serde_json::json!({"at_rest_protection": marker})).expect("encode protection marker");
            assert!(has_unknown_protection_marker(&record).expect("probe protection marker"));
        }
        let long_marker = "secret-marker-must-not-be-copied".repeat(1024);
        let record =
            serde_json::to_vec(&serde_json::json!({"at_rest_protection": long_marker})).expect("encode long protection marker");
        assert!(has_unknown_protection_marker(&record).expect("probe long protection marker"));

        for marker in [
            serde_json::Value::Null,
            serde_json::json!("legacy-unspecified"),
            serde_json::json!("encrypted-master-key"),
            serde_json::json!("plaintext-dev-only"),
        ] {
            let record =
                serde_json::to_vec(&serde_json::json!({"at_rest_protection": marker})).expect("encode protection marker");
            assert!(!has_unknown_protection_marker(&record).expect("probe protection marker"));
        }
    }

    #[tokio::test]
    async fn stored_master_key_unknown_fields_remain_readable() {
        const UNKNOWN_FIELD_VALUE: &str = "field value must not be logged";
        let (client, _temp_dir) = create_dev_mode_client().await;
        client
            .create_key("unknown-field-key", "AES_256", None)
            .await
            .expect("create key");

        let key_path = client.master_key_path("unknown-field-key").expect("valid key id");
        let record: serde_json::Value =
            serde_json::from_slice(&fs::read(&key_path).await.expect("read key record")).expect("decode key record");
        let long_field = format!("{}界", "a".repeat(126));
        let long_prefix = "a".repeat(126);
        let injection_field = "b\n\u{1b}[31m";
        let record_with_unknown = |field: &str| {
            let mut record = record.clone();
            let object = record.as_object_mut().expect("key record is an object");
            object.insert(field.to_owned(), serde_json::json!(UNKNOWN_FIELD_VALUE));
            object.insert("zeta_extension".to_owned(), serde_json::json!("another value must not be logged"));
            serde_json::to_vec_pretty(&record).expect("encode key record with unknown fields")
        };
        let long_record = record_with_unknown(&long_field);
        let mut injection_record: serde_json::Value =
            serde_json::from_slice(&record_with_unknown(injection_field)).expect("decode injection record");
        injection_record["key_id"] = serde_json::json!("tenant-secret-or-untrusted-record-id");
        let injection_record = serde_json::to_vec(&injection_record).expect("encode injection record");
        let logs = crate::test_support::CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::WARN)
            .with_writer(logs.clone())
            .finish();
        let dispatch = tracing::Dispatch::new(subscriber);
        let parse = |record: &[u8]| {
            let recorder = DebuggingRecorder::new();
            let stored = metrics::with_local_recorder(&recorder, || {
                tracing::dispatcher::with_default(&dispatch, || {
                    serde_json::from_slice(record).expect("unknown fields must remain forward-compatible")
                })
            });
            assert_eq!(unknown_field_metric(&recorder, "local-key-record"), 2);
            stored
        };
        let stored: StoredMasterKey = parse(&long_record);
        let _: StoredMasterKey = parse(&long_record);
        let _: StoredMasterKey = parse(&injection_record);
        let _: StoredMasterKey = parse(&injection_record);
        assert_eq!(stored.key_id, "unknown-field-key");

        let output = logs.output();
        assert!(output.contains("WARN"));
        assert_eq!(output.matches("Local KMS key record contains unknown fields").count(), 3);
        assert!(output.contains(&long_prefix));
        assert!(!output.contains(&long_field));
        assert!(output.contains("field_name_truncated=true"));
        assert!(output.contains(r#"\n\u{1b}[31m"#));
        assert!(!output.contains("zeta_extension"));
        assert!(output.contains("field_count=2"));
        for observed_records in [1, 2, 4] {
            assert!(output.contains(&format!("observed_records={observed_records}")));
        }
        assert!(!output.contains("observed_records=3"));
        assert!(!output.contains("tenant-secret-or-untrusted-record-id"));
        assert!(!output.contains(UNKNOWN_FIELD_VALUE));
        assert!(!output.contains("another value must not be logged"));

        let streamed: StoredMasterKey = deserialize_with_ignored_only_unknown(record, "stream_only_extension")
            .expect("unknown values must be consumed through deserialize_ignored_any");
        assert_eq!(streamed.key_id, "unknown-field-key");
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
    async fn link_durably_publishes_no_clobber_and_is_content_idempotent() {
        let temp_dir = TempDir::new().expect("temp dir");
        let source = temp_dir.path().join("staged");
        let dest = temp_dir.path().join("published");
        fs::write(&source, b"staged-content").await.expect("write source");

        durable_file::link_durably(source.clone(), dest.clone())
            .await
            .expect("first link must succeed");
        assert_eq!(fs::read(&dest).await.expect("read dest"), b"staged-content");

        // Re-entry with identical content is idempotent success — exactly the
        // resumed-cutover case.
        durable_file::link_durably(source.clone(), dest.clone())
            .await
            .expect("re-linking identical content must be idempotent");

        // Existing content that differs is a hard failure, never a clobber.
        let foreign = temp_dir.path().join("foreign");
        fs::write(&foreign, b"different-content").await.expect("write foreign");
        let error = durable_file::link_durably(foreign, dest.clone())
            .await
            .expect_err("differing content must not be clobbered");
        assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
        assert_eq!(fs::read(&dest).await.expect("dest unchanged"), b"staged-content");
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
                confirm_key_id: None,
            })
            .await
            .expect("delete key");
        assert!(!key_path.exists(), "immediate delete must remove the key file");
        assert!(fsync_recorder::dir_sync_count(dir) > dirs_before, "delete must fsync the key directory");
    }

    /// KmsManager::delete_key is the enforcement point for the waiting window;
    /// this pins the backend's defensive copy of the same bound, which is all
    /// that stands between a direct backend caller and a one-day window.
    #[tokio::test]
    async fn delete_key_refuses_a_window_outside_the_supported_range() {
        let (client, _temp_dir) = create_test_client().await;
        let key_id = "window-bounds-key";
        client.create_key(key_id, "AES_256", None).await.expect("create key");
        let backend = LocalKmsBackend { client };

        for days in [MIN_PENDING_DELETION_WINDOW_DAYS - 1, MAX_PENDING_DELETION_WINDOW_DAYS + 1] {
            let result = backend
                .delete_key(DeleteKeyRequest {
                    key_id: key_id.to_string(),
                    pending_window_in_days: Some(days),
                    ..Default::default()
                })
                .await;
            assert!(
                matches!(result, Err(KmsError::InvalidOperation { .. })),
                "a {days}-day window must be refused, got {result:?}"
            );
        }

        let state = backend
            .describe_key(DescribeKeyRequest {
                key_id: key_id.to_string(),
            })
            .await
            .expect("describe should succeed")
            .key_metadata
            .key_state;
        assert_eq!(state, KeyState::Enabled, "a refused window must not schedule the key");
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

    /// The salt guard must fail closed on every record it cannot interpret,
    /// not just on the ones that spell out `encrypted-master-key`.
    ///
    /// Skipping such a record publishes a fresh salt, and that write is the
    /// irreversible step: the next startup sees a salt file, never re-enters
    /// the guard, and the only signal that the real salt was lost is gone.
    /// Every input below also fails startup key validation, so this rejects
    /// nothing that used to initialize — it only stops the salt from being
    /// written before that failure.
    #[tokio::test]
    async fn missing_salt_with_uninterpretable_key_records_fails_closed_without_generating_a_salt() {
        let (client, temp_dir) = create_test_client().await;
        client.create_key("sealed-key", "AES_256", None).await.expect("create key");
        let config = client.config.clone();
        let key_path = client.master_key_path("sealed-key").expect("valid key id");
        let salt_path = LocalKmsClient::master_key_salt_path(&config);
        let pristine: serde_json::Value =
            serde_json::from_slice(&fs::read(&key_path).await.expect("read key file")).expect("decode record");
        drop(client);

        const UNKNOWN_MARKER_VALUE: &str = "secret-marker-value-must-not-leak";
        let mut newer_build_record = pristine.clone();
        newer_build_record["at_rest_protection"] = serde_json::json!(UNKNOWN_MARKER_VALUE);
        let newer_build_record = serde_json::to_vec_pretty(&newer_build_record).expect("encode record");
        let mut future_format_record = pristine.clone();
        future_format_record["format_version"] = serde_json::json!(99);
        let future_format_record = serde_json::to_vec_pretty(&future_format_record).expect("encode record");
        let truncated = {
            let bytes = serde_json::to_vec_pretty(&pristine).expect("encode record");
            bytes[..bytes.len() / 2].to_vec()
        };

        for (name, content, expected_error) in [
            ("record from a newer build", newer_build_record, Some(UNKNOWN_STORED_KEY_PROTECTION)),
            (
                "record with a future format version",
                future_format_record,
                Some("unsupported format version 99"),
            ),
            ("record that does not decode", truncated, None),
        ] {
            fs::write(&key_path, &content).await.expect("write record");
            fs::remove_file(&salt_path).await.ok();

            let error = match LocalKmsClient::new(config.clone()).await {
                Ok(_) => panic!("{name}: a missing salt must not be papered over"),
                Err(error) => error,
            };
            // Asserted first: publishing a replacement salt is the
            // irreversible step, and it happens before any error is produced.
            assert!(!salt_path.exists(), "{name}: a replacement salt must never be generated");
            assert!(
                matches!(error, KmsError::ConfigurationError { .. }),
                "{name}: expected a salt-specific configuration error, got {error:?}"
            );
            assert!(error.to_string().contains("salt"), "{name}: error must point at the salt: {error}");
            assert!(
                !error.to_string().contains(UNKNOWN_MARKER_VALUE),
                "{name}: raw marker values must stay redacted"
            );
            if let Some(expected_error) = expected_error {
                assert!(
                    error.to_string().contains(expected_error),
                    "{name}: error must explain the incompatibility: {error}"
                );
            }
            assert_eq!(
                sorted_dir_file_names(temp_dir.path()).await,
                vec!["sealed-key.key".to_string()],
                "{name}: the failed startup must not modify the key directory"
            );
        }
    }

    /// Compatibility floor for the guard above: a record written before the
    /// protection marker existed carries no marker at all, and such a
    /// directory legitimately has no salt yet. It must keep initializing.
    /// A record this build cannot interpret must not be edited out of a
    /// listing. The page would claim to describe the key set while omitting a
    /// key that is still on disk, and the deletion sweep — which counts the
    /// lifecycle gauges out of the pages it lists — would report a census it
    /// never fully saw as complete.
    ///
    /// It must not fail the whole listing either: one damaged record would then
    /// stop every readable key from ever being listed, and with it every
    /// scheduled deletion on this node. The identifier is reported alongside the
    /// keys that did read, so the page is honest and the caller still advances.
    #[tokio::test]
    async fn list_keys_reports_a_record_it_cannot_interpret_without_dropping_it() {
        let (client, _temp_dir) = create_test_client().await;
        client.create_key("alpha", "AES_256", None).await.expect("create alpha");
        client.create_key("beta", "AES_256", None).await.expect("create beta");

        let key_path = client.master_key_path("beta").expect("valid key id");
        let mut record: serde_json::Value =
            serde_json::from_slice(&fs::read(&key_path).await.expect("read record")).expect("decode record");
        record["at_rest_protection"] = serde_json::json!({
            "future_mode": ["secret-marker-value-must-not-leak"]
        });
        fs::write(&key_path, serde_json::to_vec_pretty(&record).expect("encode record"))
            .await
            .expect("write record");

        let response = client
            .list_keys(&ListKeysRequest::default(), None)
            .await
            .expect("one unreadable record must not fail the whole listing");
        assert_eq!(
            response.keys.iter().map(|key| key.key_id.as_str()).collect::<Vec<_>>(),
            vec!["alpha"],
            "the readable key must still be listed"
        );
        assert_eq!(
            response.unreadable_key_ids,
            vec!["beta".to_string()],
            "a key this build cannot read must be named, not quietly omitted"
        );

        // Describing it directly still fails closed with the typed error, and
        // the raw marker value stays out of the message.
        let error = client
            .describe_key("beta", None)
            .await
            .expect_err("describe must fail closed");
        assert!(
            matches!(&error, KmsError::UnsupportedFormatVersion { key_id, version }
                if key_id == "beta" && version == UNKNOWN_STORED_KEY_PROTECTION),
            "got {error:?}"
        );
        assert!(!error.to_string().contains("secret-marker-value-must-not-leak"));
    }

    /// Per-key attribution is only honest while some key on the page reads.
    ///
    /// When none does, the cause is almost certainly shared — a node reading
    /// records written in a format it has no reader for, or a policy that
    /// denies the whole subtree — and answering `200 OK` with an empty `keys`
    /// list is indistinguishable, to every client that predates
    /// `unreadable_key_ids`, from a deployment that simply has no keys. The
    /// operator response to that is to provision a new key, which is the
    /// destructive move the fail-closed rules exist to prevent.
    #[tokio::test]
    async fn a_page_whose_keys_are_all_unreadable_fails_instead_of_looking_empty() {
        let (client, _temp_dir) = create_test_client().await;
        for key_id in ["alpha", "beta"] {
            client.create_key(key_id, "AES_256", None).await.expect("create key");
            let key_path = client.master_key_path(key_id).expect("valid key id");
            let mut record: serde_json::Value =
                serde_json::from_slice(&fs::read(&key_path).await.expect("read record")).expect("decode record");
            record["at_rest_protection"] = serde_json::json!({ "future_mode": ["opaque"] });
            fs::write(&key_path, serde_json::to_vec_pretty(&record).expect("encode record"))
                .await
                .expect("write record");
        }

        let error = client
            .list_keys(&ListKeysRequest::default(), None)
            .await
            .expect_err("a page with nothing readable must fail, not report an empty key set");
        assert!(
            matches!(&error, KmsError::UnsupportedFormatVersion { .. }),
            "the failure must name what went wrong: {error:?}"
        );

        // An empty marker is not the same as no marker to a caller, but it is
        // to the pager: both start at the first key. A generated client that
        // always emits its cursor parameter must not fall through the guard.
        let error = client
            .list_keys(
                &ListKeysRequest {
                    marker: Some(String::new()),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect_err("an empty marker starts at the first key and must not bypass the guard");
        assert!(matches!(&error, KmsError::UnsupportedFormatVersion { .. }), "got {error:?}");
    }

    /// The all-unreadable guard must never become a cursor trap.
    ///
    /// It only fires for a listing that both started at the beginning and
    /// reached the end, because such a page has no successor to advance to.
    /// Applying it per page instead would mean a caller with `limit=1` gets a
    /// failure — and a failure carries no `next_marker` — the moment its page
    /// lands on the damaged key, leaving every key behind it permanently
    /// unreachable.
    #[tokio::test]
    async fn a_damaged_key_never_blocks_paging_past_it() {
        let (client, _temp_dir) = create_test_client().await;
        for key_id in ["a-first", "b-damaged", "c-last"] {
            client.create_key(key_id, "AES_256", None).await.expect("create key");
        }
        let key_path = client.master_key_path("b-damaged").expect("valid key id");
        let mut record: serde_json::Value =
            serde_json::from_slice(&fs::read(&key_path).await.expect("read record")).expect("decode record");
        record["at_rest_protection"] = serde_json::json!({ "future_mode": ["opaque"] });
        fs::write(&key_path, serde_json::to_vec_pretty(&record).expect("encode record"))
            .await
            .expect("write record");

        // Walk the whole key set one key at a time, exactly as a client that
        // pages until `truncated` is false would.
        let mut marker = None;
        let mut seen = Vec::new();
        let mut reported_unreadable = Vec::new();
        loop {
            let page = client
                .list_keys(
                    &ListKeysRequest {
                        limit: Some(1),
                        marker: marker.clone(),
                        ..Default::default()
                    },
                    None,
                )
                .await
                .expect("a one-key page containing the damaged key must still be answerable");
            seen.extend(page.keys.iter().map(|key| key.key_id.clone()));
            reported_unreadable.extend(page.unreadable_key_ids.clone());
            if !page.truncated {
                break;
            }
            marker = page.next_marker;
            assert!(marker.is_some(), "a truncated page must carry a cursor");
        }

        assert_eq!(
            seen,
            vec!["a-first".to_string(), "c-last".to_string()],
            "paging must reach past the damage"
        );
        assert_eq!(reported_unreadable, vec!["b-damaged".to_string()]);
    }

    #[tokio::test]
    async fn missing_salt_with_pre_marker_key_records_still_initializes() {
        let (dev_client, temp_dir) = create_dev_mode_client().await;
        dev_client
            .create_key("pre-marker-key", "AES_256", None)
            .await
            .expect("create key");
        let key_path = dev_client.master_key_path("pre-marker-key").expect("valid key id");
        let mut record: serde_json::Value =
            serde_json::from_slice(&fs::read(&key_path).await.expect("read key file")).expect("decode record");
        drop(dev_client);

        // Pre-beta.9 records carry no protection marker at all, and their
        // directories legitimately hold no salt file yet.
        record
            .as_object_mut()
            .expect("record is an object")
            .remove("at_rest_protection");
        fs::write(&key_path, serde_json::to_vec_pretty(&record).expect("encode record"))
            .await
            .expect("write record");

        let client = LocalKmsClient::new(test_config(temp_dir.path()))
            .await
            .expect("a pre-marker directory must keep initializing");
        assert!(
            LocalKmsClient::master_key_salt_path(&client.config).exists(),
            "first-time salt creation must still happen"
        );
        client
            .describe_key("pre-marker-key", None)
            .await
            .expect("the pre-marker record must stay readable");
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

    // -- Listing and pagination ---------------------------------------------

    fn page_request(limit: u32, marker: Option<&str>) -> ListKeysRequest {
        ListKeysRequest {
            limit: Some(limit),
            marker: marker.map(str::to_string),
            usage_filter: None,
            status_filter: None,
        }
    }

    fn filtered_page_request(limit: u32, marker: Option<&str>, status: KeyStatus) -> ListKeysRequest {
        ListKeysRequest {
            status_filter: Some(status),
            ..page_request(limit, marker)
        }
    }

    async fn create_keys(client: &LocalKmsClient, key_ids: &[String]) {
        for key_id in key_ids {
            client
                .create_key(key_id, "AES_256", None)
                .await
                .expect("key should be created");
        }
    }

    /// Paging must reach every key exactly once. A backend that reports the
    /// first page as the whole key set strands everything behind it — the
    /// deletion sweep would never destroy expired material past page one.
    #[tokio::test]
    async fn list_keys_pages_through_the_whole_key_set() {
        let (client, _temp_dir) = create_test_client().await;
        let expected: Vec<String> = (0..7).map(|index| format!("page-key-{index:02}")).collect();
        create_keys(&client, &expected).await;

        let mut seen = Vec::new();
        let mut marker: Option<String> = None;
        // Bounded so a listing that cannot advance fails the assertion below
        // instead of hanging the test run.
        for _ in 0..expected.len() + 1 {
            let response = client
                .list_keys(&page_request(3, marker.as_deref()), None)
                .await
                .expect("list should succeed");
            assert!(response.keys.len() <= 3, "a page must not exceed the requested limit");
            seen.extend(response.keys.iter().map(|key| key.key_id.clone()));
            if !response.truncated {
                assert!(response.next_marker.is_none(), "a final page must not offer a cursor");
                break;
            }
            marker = Some(response.next_marker.expect("a truncated page must offer a cursor"));
        }

        assert_eq!(seen, expected, "paging must visit every key exactly once, in identifier order");
    }

    /// Exact-limit boundary: a page that ends on the last key is complete.
    #[tokio::test]
    async fn list_keys_page_ending_on_the_last_key_is_not_truncated() {
        let (client, _temp_dir) = create_test_client().await;
        let key_ids: Vec<String> = (0..3).map(|index| format!("exact-key-{index}")).collect();
        create_keys(&client, &key_ids).await;

        let response = client
            .list_keys(&page_request(3, None), None)
            .await
            .expect("list should succeed");
        assert_eq!(response.keys.len(), 3);
        assert!(!response.truncated);
        assert!(response.next_marker.is_none());

        // One key short of the set, the same listing is truncated.
        let response = client
            .list_keys(&page_request(2, None), None)
            .await
            .expect("list should succeed");
        assert!(response.truncated);
        assert_eq!(response.next_marker.as_deref(), Some("exact-key-1"));
    }

    /// The cursor is an identifier, not an index, so it keeps working after the
    /// key it names is destroyed — which is exactly what the deletion sweep
    /// does to the keys it retires between pages.
    #[tokio::test]
    async fn list_keys_resumes_after_a_deleted_marker_key() {
        let (client, _temp_dir) = create_test_client().await;
        let key_ids: Vec<String> = ["marker-a", "marker-b", "marker-c"].iter().map(|id| id.to_string()).collect();
        create_keys(&client, &key_ids).await;

        fs::remove_file(client.master_key_path("marker-b").expect("key path"))
            .await
            .expect("key file should be removable");

        let response = client
            .list_keys(&page_request(10, Some("marker-b")), None)
            .await
            .expect("list should succeed");
        let listed: Vec<&str> = response.keys.iter().map(|key| key.key_id.as_str()).collect();
        assert_eq!(listed, vec!["marker-c"], "a vanished marker must not restart the listing");
        assert!(!response.truncated);
    }

    /// A zero limit means zero keys, and no cursor to loop on.
    #[tokio::test]
    async fn list_keys_with_zero_limit_returns_an_empty_page() {
        let (client, _temp_dir) = create_test_client().await;
        create_keys(&client, &["zero-limit-key".to_string()]).await;

        let response = client
            .list_keys(&page_request(0, None), None)
            .await
            .expect("a zero-limit list must succeed");
        assert!(response.keys.is_empty());
        assert!(!response.truncated);
        assert!(response.next_marker.is_none());
    }

    /// A filter narrows a page after it has been cut, so a page can come back
    /// empty while keys still remain. The traversal has to continue on
    /// `truncated` rather than on a short page, and the cursor has to advance
    /// across the keys the filter removed — otherwise a filtered listing ends
    /// at the first run of non-matching keys and reports a partial key set as
    /// the whole one.
    #[tokio::test]
    async fn filtered_paging_crosses_a_page_that_matches_nothing() {
        let (client, _temp_dir) = create_test_client().await;
        let key_ids: Vec<String> = (0..12).map(|index| format!("filter-key-{index:02}")).collect();
        create_keys(&client, &key_ids).await;
        // A page worth of adjacent keys is excluded, so one page of the
        // traversal matches nothing at all.
        for key_id in &key_ids[3..6] {
            client.disable_key(key_id, None).await.expect("key should be disabled");
        }
        let still_active: Vec<String> = key_ids
            .iter()
            .enumerate()
            .filter(|(index, _)| !(3..6).contains(index))
            .map(|(_, key_id)| key_id.clone())
            .collect();

        let mut seen = Vec::new();
        let mut pages_without_a_match = 0;
        let mut marker: Option<String> = None;
        // Bounded so a listing that cannot advance fails the assertions below
        // instead of hanging the test run.
        for _ in 0..key_ids.len() + 1 {
            let response = client
                .list_keys(&filtered_page_request(3, marker.as_deref(), KeyStatus::Active), None)
                .await
                .expect("list should succeed");
            assert!(response.keys.len() <= 3, "a page must not exceed the requested limit");
            assert!(
                response.keys.iter().all(|key| key.status == KeyStatus::Active),
                "a filtered page must carry matches only"
            );
            if response.keys.is_empty() {
                pages_without_a_match += 1;
            }
            seen.extend(response.keys.iter().map(|key| key.key_id.clone()));
            if !response.truncated {
                assert!(response.next_marker.is_none(), "a final page must not offer a cursor");
                break;
            }
            marker = Some(response.next_marker.expect("a truncated page must offer a cursor"));
        }

        assert_eq!(seen, still_active, "filtered paging must visit every match exactly once");
        assert_eq!(pages_without_a_match, 1, "the excluded run must produce a page with no match");

        // The keys the first filter excluded are exactly the ones the opposite
        // filter lists, so nothing fell out of the key set on the way.
        let response = client
            .list_keys(&filtered_page_request(key_ids.len() as u32, None, KeyStatus::Disabled), None)
            .await
            .expect("list should succeed");
        let listed: Vec<&str> = response.keys.iter().map(|key| key.key_id.as_str()).collect();
        assert_eq!(listed, key_ids[3..6].iter().map(String::as_str).collect::<Vec<_>>());
        assert!(!response.truncated, "a page covering the whole key set is complete");
    }

    /// A limit past the end of the key set returns everything, once.
    #[tokio::test]
    async fn list_keys_limit_beyond_the_key_set_returns_one_complete_page() {
        let (client, _temp_dir) = create_test_client().await;
        let key_ids: Vec<String> = (0..3).map(|index| format!("huge-limit-key-{index}")).collect();
        create_keys(&client, &key_ids).await;

        let response = client
            .list_keys(&page_request(u32::MAX, None), None)
            .await
            .expect("list should succeed");
        assert_eq!(response.keys.len(), 3);
        assert!(!response.truncated);
        assert!(response.next_marker.is_none());
    }

    // -----------------------------------------------------------------------
    // Filesystem boundaries (rustfs/backlog#1562 P0.4).
    //
    // The commit protocol's durability argument rests on properties of the
    // filesystem it runs on, and those properties are assumptions until
    // something exercises them. Each test below pins one boundary the protocol
    // depends on. Two boundaries in that item cannot be closed here and are
    // recorded in `docs/operations/kms-backend-security.md` instead: a real
    // cross-device rename needs a second filesystem (root or a privileged
    // container), and detecting a key directory swapped between `rename` and
    // the parent `fsync` needs directory file descriptors the protocol does not
    // yet hold.
    // -----------------------------------------------------------------------

    #[cfg(unix)]
    fn mode_of(path: &Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path).expect("stat").permissions().mode() & 0o777
    }

    /// Every file the protocol publishes carries the requested mode, and the
    /// umask cannot widen it — the mode is applied and re-read on the open file
    /// before the content becomes durable, not left to the creation mask.
    #[cfg(unix)]
    #[tokio::test]
    async fn published_files_carry_the_requested_mode_regardless_of_umask() {
        let (client, _temp_dir) = create_test_client().await;
        client.create_key("mode-key", "AES_256", None).await.expect("create key");

        assert_eq!(mode_of(&client.master_key_path("mode-key").expect("valid key id")), 0o600);
        let salt_path = LocalKmsClient::master_key_salt_path(&client.config);
        assert_eq!(mode_of(&salt_path), 0o600);
    }

    /// The key directory ends up owner-only whoever created it and whatever
    /// mode they left it at.
    ///
    /// The platform picks that mode far more often than an operator does:
    /// kubelet creates an `emptyDir` `0o777`, several PVC provisioners
    /// `mkdir -m 0777`, and a `--tmpfs` mount lands at `1777`. Each is a
    /// directory holding every master key that any account on the host can
    /// delete from. The modes below are exercised explicitly rather than left
    /// to the umask, so the test keeps its teeth on a host with any umask.
    #[cfg(unix)]
    #[tokio::test]
    async fn the_key_directory_is_narrowed_to_owner_only_whatever_it_was() {
        use std::os::unix::fs::PermissionsExt;

        // Created by this process, including the intermediate component.
        let temp_dir = TempDir::new().expect("temp dir");
        let key_dir = temp_dir.path().join("nested").join("keys");
        assert!(!key_dir.exists());
        let client = LocalKmsClient::new(LocalConfig {
            key_dir: key_dir.clone(),
            master_key: Some("test-master-key".to_string()),
            file_permissions: Some(0o600),
        })
        .await
        .expect("client must create its key directory");
        client.create_key("nested-key", "AES_256", None).await.expect("create key");
        assert_eq!(mode_of(&key_dir), 0o700);
        assert_eq!(
            mode_of(&temp_dir.path().join("nested")),
            0o700,
            "an intermediate directory must not be left at the umask's mercy"
        );
        drop(client);

        // Placed by the platform at each mode a real one actually produces,
        // sticky bit included.
        for mode in [0o777, 0o1777, 0o770, 0o755] {
            let placed = TempDir::new().expect("temp dir");
            std::fs::set_permissions(placed.path(), std::fs::Permissions::from_mode(mode)).expect("widen mode");

            let client = LocalKmsClient::new(LocalConfig {
                key_dir: placed.path().to_path_buf(),
                master_key: Some("test-master-key".to_string()),
                file_permissions: Some(0o600),
            })
            .await
            .unwrap_or_else(|error| panic!("mode {mode:#o} must start, not fail: {error:?}"));
            client.create_key("placed-key", "AES_256", None).await.expect("create key");

            assert_eq!(mode_of(placed.path()), 0o700, "a {mode:#o} key directory must be narrowed");
        }
    }

    /// An absent `file_permissions` means owner-only, not "whatever the umask
    /// says". A `0` umask is the default in a good many container images, and
    /// under it an unspecified mode used to publish master key records
    /// world-readable.
    ///
    /// The end-to-end assertion below cannot be the whole guard: on a host
    /// whose umask already masks the group and other bits, the un-fixed code
    /// would produce `0o600` by accident and the test would pass while
    /// protecting nothing. So the resolution the protocol performs is asserted
    /// directly, where the umask cannot reach it.
    #[cfg(unix)]
    #[tokio::test]
    async fn unspecified_file_permissions_still_publish_owner_only() {
        assert_eq!(durable_file::resolved_file_mode(None), DEFAULT_KEY_FILE_MODE);
        assert_eq!(durable_file::resolved_file_mode(Some(0o640)), 0o640, "an explicit mode is still honoured");

        let temp_dir = TempDir::new().expect("temp dir");
        let client = LocalKmsClient::new(LocalConfig {
            key_dir: temp_dir.path().to_path_buf(),
            master_key: Some("test-master-key".to_string()),
            file_permissions: None,
        })
        .await
        .expect("client with unspecified permissions must still start");
        client.create_key("default-mode", "AES_256", None).await.expect("create key");

        assert_eq!(
            mode_of(&client.master_key_path("default-mode").expect("valid key id")),
            DEFAULT_KEY_FILE_MODE
        );
        assert_eq!(mode_of(&LocalKmsClient::master_key_salt_path(&client.config)), DEFAULT_KEY_FILE_MODE);
    }

    /// Publishing must replace a symlink sitting at the destination, never
    /// write through it. Following it would let anything with write access to
    /// the key directory redirect a master key record — or a later read of it —
    /// outside the confinement `master_key_path` enforces.
    #[cfg(unix)]
    #[tokio::test]
    async fn publishing_replaces_a_symlink_instead_of_writing_through_it() {
        let (client, temp_dir) = create_test_client().await;
        let outside = temp_dir.path().join("outside.txt");
        fs::write(&outside, b"untouched").await.expect("write decoy");

        // `create_key` publishes with `hard_link`, which refuses a destination
        // that already exists — including a symlink, dangling or not.
        let key_path = client.master_key_path("symlinked").expect("valid key id");
        std::os::unix::fs::symlink(&outside, &key_path).expect("plant symlink");
        let error = client
            .create_key("symlinked", "AES_256", None)
            .await
            .expect_err("a symlink at the destination must not be written through");
        assert!(matches!(error, KmsError::KeyAlreadyExists { .. }), "got {error:?}");
        assert_eq!(fs::read(&outside).await.expect("read decoy"), b"untouched");

        // The update path publishes with `rename`, which replaces the link
        // itself rather than the file it points at.
        std::fs::remove_file(&key_path).expect("clear symlink");
        client.create_key("symlinked", "AES_256", None).await.expect("create key");
        std::fs::remove_file(&key_path).expect("remove record");
        std::os::unix::fs::symlink(&outside, &key_path).expect("re-plant symlink");
        let master_key = MasterKeyInfo::new("symlinked".to_string(), "AES_256".to_string(), None);
        client
            .save_master_key(&master_key, &[7u8; 32])
            .await
            .expect("save over symlink");

        assert_eq!(fs::read(&outside).await.expect("read decoy"), b"untouched");
        assert!(
            !std::fs::symlink_metadata(&key_path).expect("stat").file_type().is_symlink(),
            "the published record must be a regular file, not a link"
        );
    }

    /// A hard link planted at the destination is refused exactly as a regular
    /// file is: `hard_link` fails on an existing name, so no create can adopt
    /// an inode it did not write.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_planted_hard_link_cannot_be_adopted_as_a_key_record() {
        let (client, temp_dir) = create_test_client().await;
        let outside = temp_dir.path().join("outside.txt");
        fs::write(&outside, b"planted").await.expect("write decoy");
        let key_path = client.master_key_path("linked").expect("valid key id");
        std::fs::hard_link(&outside, &key_path).expect("plant hard link");

        let error = client
            .create_key("linked", "AES_256", None)
            .await
            .expect_err("a planted hard link must not be adopted");
        assert!(matches!(error, KmsError::KeyAlreadyExists { .. }), "got {error:?}");
        assert_eq!(fs::read(&outside).await.expect("read decoy"), b"planted");
    }

    /// Startup removes a commit temp that is a symlink, not only a regular
    /// file. The protocol only ever creates temps with `create_new`, so a temp
    /// name wearing any other file type is either our own leftover or something
    /// planted; requiring a regular file left those behind forever.
    #[cfg(unix)]
    #[tokio::test]
    async fn startup_removes_a_symlinked_commit_temp() {
        let (client, temp_dir) = create_test_client().await;
        client.create_key("live", "AES_256", None).await.expect("create key");
        let key_path = client.master_key_path("live").expect("valid key id");

        let symlinked_temp = key_path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
        std::os::unix::fs::symlink(&key_path, &symlinked_temp).expect("plant symlinked temp");
        drop(client);

        let client = LocalKmsClient::new(LocalConfig {
            key_dir: temp_dir.path().to_path_buf(),
            master_key: Some("test-master-key".to_string()),
            file_permissions: Some(0o600),
        })
        .await
        .expect("restart");

        assert!(
            std::fs::symlink_metadata(&symlinked_temp).is_err(),
            "a symlinked commit temp must not survive startup"
        );
        // Removing the link must not have touched the key it pointed at.
        client.describe_key("live", None).await.expect("the key must survive");
    }

    /// The commit protocol never asks the filesystem to rename or link across a
    /// device boundary, because the temp file is always created in the
    /// destination's own directory. That is the invariant that makes `EXDEV`
    /// unreachable; a real cross-device test needs a second filesystem and
    /// cannot run here, so the invariant itself is what gets pinned.
    #[tokio::test]
    async fn commit_temps_always_share_the_destination_directory() {
        let (client, temp_dir) = create_test_client().await;
        client.create_key("same-dir", "AES_256", None).await.expect("create key");

        // Every entry the protocol left behind, temps included, is in the key
        // directory: nothing was staged anywhere a rename could have to cross a
        // device to leave.
        let mut entries = std::fs::read_dir(temp_dir.path()).expect("read key dir");
        assert!(entries.any(|entry| entry.expect("entry").file_name() == "same-dir.key"));

        let key_path = client.master_key_path("same-dir").expect("valid key id");
        let salt_path = LocalKmsClient::master_key_salt_path(&client.config);
        assert_eq!(salt_path.parent(), Some(temp_dir.path()));
        assert_eq!(key_path.parent(), Some(temp_dir.path()));
    }
}
