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

//! KMS backend implementations

use crate::error::{KmsError, Result};
use crate::types::*;
use async_trait::async_trait;
use jiff::Zoned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub mod aws;
#[cfg(test)]
mod contract_tests;
pub mod local;
#[cfg(test)]
pub(crate) mod scripted_vault;
pub mod static_kms;
pub mod vault;
pub(crate) mod vault_credentials;
pub mod vault_transit;

/// Operations whose availability depends on the key's lifecycle state.
///
/// Decryption is deliberately absent: RustFS allows decryption with
/// `Disabled` and `PendingDeletion` keys — an explicit deviation from AWS
/// KMS — because rejecting it would break reads of every object encrypted
/// under a key the moment it is disabled. Deletion cancellation is also
/// absent: it is valid exactly when the key is `PendingDeletion`, which call
/// sites enforce directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StateGatedOperation {
    Encrypt,
    GenerateDataKey,
    Rotate,
    Enable,
    Disable,
    ScheduleDeletion,
}

impl StateGatedOperation {
    fn describe(self) -> &'static str {
        match self {
            Self::Encrypt => "encryption",
            Self::GenerateDataKey => "data key generation",
            Self::Rotate => "rotation",
            Self::Enable => "enabling",
            Self::Disable => "disabling",
            Self::ScheduleDeletion => "deletion scheduling",
        }
    }
}

/// Enforce the shared key state × operation matrix.
///
/// - `Enabled`: every operation is allowed.
/// - `Disabled`: enabling, disabling (idempotent) and deletion scheduling are
///   allowed; encryption, data key generation and rotation are rejected.
/// - `PendingDeletion`: every state-gated operation is rejected, including a
///   repeated deletion schedule; only cancellation and decryption proceed.
/// - `PendingImport`/`Unavailable`: the key is not usable and is reported as
///   not found.
pub(crate) fn ensure_key_state_permits(key_id: &str, state: &KeyState, operation: StateGatedOperation) -> Result<()> {
    match state {
        KeyState::Enabled => Ok(()),
        KeyState::Disabled => match operation {
            StateGatedOperation::Enable | StateGatedOperation::Disable | StateGatedOperation::ScheduleDeletion => Ok(()),
            StateGatedOperation::Encrypt | StateGatedOperation::GenerateDataKey | StateGatedOperation::Rotate => Err(
                KmsError::invalid_key_state(format!("Key {key_id} is disabled: {} is not allowed", operation.describe())),
            ),
        },
        KeyState::PendingDeletion => Err(KmsError::invalid_key_state(format!(
            "Key {key_id} is pending deletion: {} is not allowed",
            operation.describe()
        ))),
        KeyState::PendingImport | KeyState::Unavailable => Err(KmsError::key_not_found(key_id)),
    }
}

/// [`ensure_key_state_permits`] for backends that persist [`KeyStatus`].
pub(crate) fn ensure_key_status_permits(key_id: &str, status: &KeyStatus, operation: StateGatedOperation) -> Result<()> {
    let state = match status {
        KeyStatus::Active => KeyState::Enabled,
        KeyStatus::Disabled => KeyState::Disabled,
        KeyStatus::PendingDeletion => KeyState::PendingDeletion,
        KeyStatus::Deleted => KeyState::Unavailable,
    };
    ensure_key_state_permits(key_id, &state, operation)
}

/// Tag key that carries the key's identity rather than user metadata.
///
/// The key-creation path lifts `name` out of the caller's tag map and uses it
/// as the key id, so a key's `name` tag and its id are the same string.
/// Rewriting or dropping it after creation would leave the key addressable
/// under an id its own metadata no longer states. Metadata updates therefore
/// reject it; only creation may set it.
pub const RESERVED_KEY_NAME_TAG: &str = "name";

/// Reject a metadata update that would rewrite or remove
/// [`RESERVED_KEY_NAME_TAG`].
///
/// Enforced by every backend that implements tag updates, so no call path —
/// including a direct [`KmsBackend`] user that bypasses the manager — can
/// detach a key from its identity.
pub(crate) fn ensure_tag_keys_are_mutable<'a>(tag_keys: impl IntoIterator<Item = &'a str>) -> Result<()> {
    for tag_key in tag_keys {
        if tag_key == RESERVED_KEY_NAME_TAG {
            return Err(KmsError::invalid_parameter(format!(
                "Tag '{RESERVED_KEY_NAME_TAG}' identifies the key and cannot be updated or removed"
            )));
        }
    }
    Ok(())
}

/// Page size used when a [`ListKeysRequest`] does not ask for one.
pub(crate) const DEFAULT_LIST_KEYS_PAGE_SIZE: u32 = 100;

/// One page of a key set the backend has to slice itself.
pub(crate) struct KeyPage<'a, T> {
    /// The identifiers this page covers, in listing order.
    pub(crate) items: &'a [T],
    /// Where the next page resumes; `None` when this page is the last one.
    pub(crate) next_marker: Option<String>,
    /// Whether keys remain beyond this page.
    pub(crate) truncated: bool,
}

/// Cut the page `request` asks for out of `sorted`.
///
/// `sorted` must be ordered by the identifier `key_id_of` returns, and the
/// marker is an *exclusive lower bound* on that identifier rather than an index
/// into the sequence. That is what makes paging survive concurrent mutation:
/// the next page resumes at the first identifier greater than the marker, so a
/// key added or removed elsewhere in the ordering — including the marker key
/// itself, which the deletion sweep routinely destroys — cannot make the
/// listing skip keys or restart from the beginning.
///
/// A `limit` of zero is honoured as written: the caller asked for no keys and
/// gets an empty, non-truncated page (see [`list_keys_page_size`]).
///
/// Filters are applied by the caller to `items` after this slice, so a filtered
/// page can be shorter than `limit` — and even empty — while more keys remain.
/// Callers must page until `truncated` is false rather than until a page comes
/// back short.
pub(crate) fn paginate_keys<'a, T>(sorted: &'a [T], request: &ListKeysRequest, key_id_of: impl Fn(&T) -> &str) -> KeyPage<'a, T> {
    let Some(limit) = list_keys_page_size(request.limit) else {
        return KeyPage {
            items: &[],
            next_marker: None,
            truncated: false,
        };
    };

    let start = match request.marker.as_deref() {
        Some(marker) => sorted.partition_point(|item| key_id_of(item) <= marker),
        None => 0,
    };
    // `partition_point` never exceeds the length, so both bounds stay in range
    // however large `limit` is.
    let end = start.saturating_add(limit).min(sorted.len());
    let items = &sorted[start..end];
    let truncated = end < sorted.len();

    KeyPage {
        items,
        // Resuming from the last identifier on the page, not from an index,
        // keeps the cursor meaningful after the key it names disappears.
        next_marker: if truncated {
            items.last().map(|item| key_id_of(item).to_string())
        } else {
            None
        },
        truncated,
    }
}

/// Resolve the page size of a [`ListKeysRequest`]; `None` means the caller
/// asked for no keys at all.
///
/// `Some(0)` is a well-formed request for an empty page — the reading rustfs
/// already gives `max-keys=0` on the S3 listing path — not a malformed one and
/// not an omitted value. Rounding it up to a default would hand back a full
/// page of keys to a caller that explicitly asked for none.
pub(crate) fn list_keys_page_size(limit: Option<u32>) -> Option<usize> {
    match limit.unwrap_or(DEFAULT_LIST_KEYS_PAGE_SIZE) {
        0 => None,
        size => Some(size as usize),
    }
}

/// Simplified KMS backend interface for manager
#[async_trait]
pub trait KmsBackend: Send + Sync {
    /// Create a new master key
    async fn create_key(&self, request: CreateKeyRequest) -> Result<CreateKeyResponse>;

    /// Encrypt data
    async fn encrypt(&self, request: EncryptRequest) -> Result<EncryptResponse>;

    /// Decrypt data
    async fn decrypt(&self, request: DecryptRequest) -> Result<DecryptResponse>;

    /// Generate a data key
    async fn generate_data_key(&self, request: GenerateDataKeyRequest) -> Result<GenerateDataKeyResponse>;

    /// Describe a key
    async fn describe_key(&self, request: DescribeKeyRequest) -> Result<DescribeKeyResponse>;

    /// List keys
    async fn list_keys(&self, request: ListKeysRequest) -> Result<ListKeysResponse>;

    /// Delete a key
    async fn delete_key(&self, request: DeleteKeyRequest) -> Result<DeleteKeyResponse>;

    /// Cancel key deletion
    async fn cancel_key_deletion(&self, request: CancelKeyDeletionRequest) -> Result<CancelKeyDeletionResponse>;

    /// Enable a disabled key so it can be used for cryptographic operations
    /// again.
    ///
    /// Backends that advertise [`BackendCapabilities::enable_disable`] must
    /// override this method; the default rejects the operation.
    async fn enable_key(&self, _key_id: &str) -> Result<()> {
        Err(KmsError::unsupported_capability("backend without enable/disable support", "enable_key"))
    }

    /// Disable a key, rejecting new cryptographic use while existing data
    /// remains decryptable.
    ///
    /// Backends that advertise [`BackendCapabilities::enable_disable`] must
    /// override this method; the default rejects the operation.
    async fn disable_key(&self, _key_id: &str) -> Result<()> {
        Err(KmsError::unsupported_capability("backend without enable/disable support", "disable_key"))
    }

    /// Rotate a key to a new version while prior versions remain available
    /// for decryption.
    ///
    /// Only backends that advertise [`BackendCapabilities::rotate`] (that is,
    /// backends with retained version history) may override this method; the
    /// default rejects the operation.
    async fn rotate_key(&self, _key_id: &str) -> Result<()> {
        Err(KmsError::unsupported_capability("backend without rotation support", "rotate_key"))
    }

    /// Replace a key's free-form description; `None` clears it.
    ///
    /// Backends that advertise [`BackendCapabilities::update_key_metadata`]
    /// must override this method; the default rejects the operation.
    async fn update_key_description(&self, _key_id: &str, _description: Option<&str>) -> Result<()> {
        Err(KmsError::unsupported_capability(
            "backend without key metadata updates",
            "update_key_description",
        ))
    }

    /// Add or overwrite the given tags, leaving every other tag untouched.
    ///
    /// Implementations must run [`ensure_tag_keys_are_mutable`] before
    /// persisting anything. Backends that advertise
    /// [`BackendCapabilities::update_key_metadata`] must override this method;
    /// the default rejects the operation.
    async fn tag_key(&self, _key_id: &str, _tags: &HashMap<String, String>) -> Result<()> {
        Err(KmsError::unsupported_capability("backend without key metadata updates", "tag_key"))
    }

    /// Remove the given tags.
    ///
    /// Tags that are not set are ignored, so repeating the call is a no-op
    /// rather than an error. Implementations must run
    /// [`ensure_tag_keys_are_mutable`] before persisting anything. Backends
    /// that advertise [`BackendCapabilities::update_key_metadata`] must
    /// override this method; the default rejects the operation.
    async fn untag_key(&self, _key_id: &str, _tag_keys: &[String]) -> Result<()> {
        Err(KmsError::unsupported_capability("backend without key metadata updates", "untag_key"))
    }

    /// Health check
    async fn health_check(&self) -> Result<bool>;

    /// Report which operations this backend actually supports.
    ///
    /// The default is conservative: only the operations every backend is
    /// required to implement by this trait are advertised. Optional lifecycle
    /// operations (rotation, enable/disable, deletion scheduling, ...) must be
    /// opted in by overriding this method.
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::minimal()
    }

    /// Remove a key whose scheduled deletion deadline has passed.
    ///
    /// Used by the background deletion worker. Implementations must re-check
    /// state and deadline under their own write synchronization so that a
    /// concurrent cancellation observed after the caller's inspection wins
    /// ([`ExpiredKeyRemoval::StateChanged`]), must write a tombstone (a
    /// `Deleted`/`Unavailable` record) before destroying material so a crashed
    /// removal can simply be re-run, and must treat an already-removed key as
    /// success so the operation stays idempotent across restarts and nodes.
    ///
    /// The default rejects the operation for backends without deletion
    /// support.
    async fn remove_expired_key(&self, _key_id: &str, _now: &Zoned) -> Result<ExpiredKeyRemoval> {
        Err(KmsError::unsupported_capability("backend without deletion support", "remove_expired_key"))
    }

    /// The running client to export a full-material backup bundle from.
    ///
    /// Only the Local backend owns key material RustFS is allowed to export in
    /// full (see [`crate::backup::BackupResponsibility`]); every other backend
    /// keeps its cryptographic root outside RustFS and returns `None` here.
    ///
    /// The export must run against the *running* client so that its fence
    /// actually blocks concurrent create/delete work — a second client opened
    /// on the same key directory would fence nothing.
    fn local_backup_client(&self) -> Option<&local::LocalKmsClient> {
        None
    }
}

/// Outcome of [`KmsBackend::remove_expired_key`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpiredKeyRemoval {
    /// The key's record and material were removed, or were already gone.
    Removed,
    /// The key is no longer pending deletion (for example the deletion was
    /// cancelled after the caller inspected it); nothing was removed.
    StateChanged,
    /// The key is pending deletion but its deadline has not passed, or it has
    /// no persisted deadline (legacy record) and is never auto-removed.
    NotExpired,
}

/// Set of operations a KMS backend supports.
///
/// Reported by [`KmsBackend::capabilities`] so callers (manager, admin API)
/// can discover what the active backend can do without probing individual
/// operations. Marked `#[non_exhaustive]` so new capability flags can be
/// added without breaking downstream code; construct values through
/// [`BackendCapabilities::minimal`] and the `with_*` builders.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendCapabilities {
    /// Direct encryption of caller-provided plaintext with a master key
    pub encrypt: bool,
    /// Decryption of previously produced ciphertext
    pub decrypt: bool,
    /// Data encryption key (DEK) generation
    pub generate_data_key: bool,
    /// Key rotation that retains prior versions for decryption
    pub rotate: bool,
    /// Enabling and disabling keys
    pub enable_disable: bool,
    /// Scheduling key deletion with a pending window
    pub schedule_deletion: bool,
    /// Multiple key versions addressable after rotation
    pub versioning: bool,
    /// Irreversible physical deletion of key material
    pub physical_delete: bool,
    /// Updating a key's description and tags after creation
    pub update_key_metadata: bool,
}

impl BackendCapabilities {
    /// Conservative baseline: only the operations that every [`KmsBackend`]
    /// implementation is required to provide by the trait. All optional
    /// lifecycle capabilities default to unsupported.
    pub const fn minimal() -> Self {
        Self {
            encrypt: true,
            decrypt: true,
            generate_data_key: true,
            rotate: false,
            enable_disable: false,
            schedule_deletion: false,
            versioning: false,
            physical_delete: false,
            update_key_metadata: false,
        }
    }

    /// Set whether direct encryption is supported
    pub const fn with_encrypt(mut self, encrypt: bool) -> Self {
        self.encrypt = encrypt;
        self
    }

    /// Set whether decryption is supported
    pub const fn with_decrypt(mut self, decrypt: bool) -> Self {
        self.decrypt = decrypt;
        self
    }

    /// Set whether data key generation is supported
    pub const fn with_generate_data_key(mut self, generate_data_key: bool) -> Self {
        self.generate_data_key = generate_data_key;
        self
    }

    /// Set whether version-retaining key rotation is supported
    pub const fn with_rotate(mut self, rotate: bool) -> Self {
        self.rotate = rotate;
        self
    }

    /// Set whether enabling/disabling keys is supported
    pub const fn with_enable_disable(mut self, enable_disable: bool) -> Self {
        self.enable_disable = enable_disable;
        self
    }

    /// Set whether scheduled deletion with a pending window is supported
    pub const fn with_schedule_deletion(mut self, schedule_deletion: bool) -> Self {
        self.schedule_deletion = schedule_deletion;
        self
    }

    /// Set whether multiple key versions are supported
    pub const fn with_versioning(mut self, versioning: bool) -> Self {
        self.versioning = versioning;
        self
    }

    /// Set whether physical deletion of key material is supported
    pub const fn with_physical_delete(mut self, physical_delete: bool) -> Self {
        self.physical_delete = physical_delete;
        self
    }

    /// Set whether description and tag updates are supported
    pub const fn with_update_key_metadata(mut self, update_key_metadata: bool) -> Self {
        self.update_key_metadata = update_key_metadata;
        self
    }
}

impl Default for BackendCapabilities {
    fn default() -> Self {
        Self::minimal()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::KmsConfig;
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD as BASE64;

    /// Backend that implements only the trait-mandated operations and relies
    /// on the default `capabilities` implementation.
    struct MinimalBackend;

    #[async_trait]
    impl KmsBackend for MinimalBackend {
        async fn create_key(&self, _request: CreateKeyRequest) -> Result<CreateKeyResponse> {
            unimplemented!("not exercised by capability tests")
        }

        async fn encrypt(&self, _request: EncryptRequest) -> Result<EncryptResponse> {
            unimplemented!("not exercised by capability tests")
        }

        async fn decrypt(&self, _request: DecryptRequest) -> Result<DecryptResponse> {
            unimplemented!("not exercised by capability tests")
        }

        async fn generate_data_key(&self, _request: GenerateDataKeyRequest) -> Result<GenerateDataKeyResponse> {
            unimplemented!("not exercised by capability tests")
        }

        async fn describe_key(&self, _request: DescribeKeyRequest) -> Result<DescribeKeyResponse> {
            unimplemented!("not exercised by capability tests")
        }

        async fn list_keys(&self, _request: ListKeysRequest) -> Result<ListKeysResponse> {
            unimplemented!("not exercised by capability tests")
        }

        async fn delete_key(&self, _request: DeleteKeyRequest) -> Result<DeleteKeyResponse> {
            unimplemented!("not exercised by capability tests")
        }

        async fn cancel_key_deletion(&self, _request: CancelKeyDeletionRequest) -> Result<CancelKeyDeletionResponse> {
            unimplemented!("not exercised by capability tests")
        }

        async fn health_check(&self) -> Result<bool> {
            Ok(true)
        }
    }

    fn capabilities_snapshot(capabilities: BackendCapabilities) -> std::collections::BTreeMap<String, bool> {
        serde_json::from_value(serde_json::to_value(capabilities).expect("capabilities should serialize"))
            .expect("capabilities should deserialize into a flat bool map")
    }

    #[test]
    fn default_capabilities_are_conservative() {
        let capabilities = MinimalBackend.capabilities();
        assert_eq!(capabilities, BackendCapabilities::minimal());
        assert_eq!(capabilities, BackendCapabilities::default());

        // The conservative baseline advertises only trait-mandated operations.
        assert!(capabilities.encrypt);
        assert!(capabilities.decrypt);
        assert!(capabilities.generate_data_key);
        assert!(!capabilities.rotate);
        assert!(!capabilities.enable_disable);
        assert!(!capabilities.schedule_deletion);
        assert!(!capabilities.versioning);
        assert!(!capabilities.physical_delete);
        assert!(!capabilities.update_key_metadata);
    }

    #[tokio::test]
    async fn default_lifecycle_operations_are_unsupported() {
        for (operation, result) in [
            ("enable_key", MinimalBackend.enable_key("any-key").await),
            ("disable_key", MinimalBackend.disable_key("any-key").await),
            ("rotate_key", MinimalBackend.rotate_key("any-key").await),
            (
                "update_key_description",
                MinimalBackend.update_key_description("any-key", Some("new")).await,
            ),
            ("tag_key", MinimalBackend.tag_key("any-key", &HashMap::new()).await),
            ("untag_key", MinimalBackend.untag_key("any-key", &[]).await),
        ] {
            let error = result.expect_err("backends must opt in to lifecycle operations by overriding them");
            assert!(
                matches!(error, KmsError::UnsupportedCapability { .. }),
                "expected UnsupportedCapability for {operation}, got {error:?}"
            );
        }
    }

    #[tokio::test]
    async fn default_remove_expired_key_is_unsupported() {
        let error = MinimalBackend
            .remove_expired_key("any-key", &jiff::Zoned::now())
            .await
            .expect_err("backends without deletion support must reject expired-key removal");
        assert!(
            matches!(error, KmsError::UnsupportedCapability { .. }),
            "expected UnsupportedCapability, got {error:?}"
        );
    }

    #[test]
    fn identity_tag_is_rejected_by_metadata_updates() {
        let error = ensure_tag_keys_are_mutable([RESERVED_KEY_NAME_TAG])
            .expect_err("the identity tag must not be writable through a metadata update");
        assert!(
            matches!(&error, KmsError::InvalidOperation { message } if message.contains(RESERVED_KEY_NAME_TAG)),
            "expected a typed rejection naming the tag, got {error:?}"
        );

        // Ordinary tags — including ones that merely contain the reserved name
        // — stay writable.
        ensure_tag_keys_are_mutable(["team", "nickname", "Name"]).expect("ordinary tags must remain writable");
    }

    #[tokio::test]
    async fn local_backend_capabilities_golden() {
        let temp_dir = tempfile::tempdir().expect("temp dir should be created");
        let config = KmsConfig::local(temp_dir.path().to_path_buf()).with_insecure_development_defaults();
        let backend = local::LocalKmsBackend::new(config).await.expect("local backend should build");

        insta::assert_json_snapshot!("local_backend_capabilities", capabilities_snapshot(backend.capabilities()));
    }

    #[tokio::test]
    async fn vault_kv2_backend_capabilities_golden() {
        let config = KmsConfig::vault(
            url::Url::parse("http://127.0.0.1:8200").expect("vault URL should parse"),
            "dev-token".to_string(),
        )
        .with_insecure_development_defaults();
        // Constructing the client performs no network I/O with token auth.
        let backend = vault::VaultKmsBackend::new(config)
            .await
            .expect("vault kv2 backend should build");

        insta::assert_json_snapshot!("vault_kv2_backend_capabilities", capabilities_snapshot(backend.capabilities()));
    }

    #[tokio::test]
    async fn vault_transit_backend_capabilities_golden() {
        let config = KmsConfig::vault_transit(
            url::Url::parse("http://127.0.0.1:8200").expect("vault URL should parse"),
            "dev-token".to_string(),
        )
        .with_insecure_development_defaults();
        // Constructing the client performs no network I/O with token auth.
        let backend = vault_transit::VaultTransitKmsBackend::new(config)
            .await
            .expect("vault transit backend should build");

        insta::assert_json_snapshot!("vault_transit_backend_capabilities", capabilities_snapshot(backend.capabilities()));
    }

    #[tokio::test]
    async fn static_backend_capabilities_golden() {
        let config = KmsConfig::static_kms("static-key".to_string(), BASE64.encode([0u8; 32]));
        let backend = static_kms::StaticKmsBackend::new(config)
            .await
            .expect("static backend should build");

        insta::assert_json_snapshot!("static_backend_capabilities", capabilities_snapshot(backend.capabilities()));
    }
}
