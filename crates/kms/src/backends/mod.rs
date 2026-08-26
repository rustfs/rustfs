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

/// Reject a rewrap whose caller cannot reproduce the envelope's encryption
/// context.
///
/// Deliberately the same rule the decrypt paths apply — every context entry the
/// envelope carries must be matched, and an entirely empty request context is
/// accepted for envelopes written before contexts were recorded. Rewrap must
/// never be *stricter* than decrypt: a retirement sweep has to be able to
/// rewrap every envelope that still reads, or the old master key version it is
/// trying to retire stays referenced forever by objects that are perfectly
/// readable.
///
/// It must not be *laxer* either. The context binds an envelope to one
/// bucket/object pair, and a rewrap that skipped the check would let a caller
/// launder an envelope it has no claim on into a freshly wrapped one.
pub(crate) fn ensure_rewrap_context_matches(
    envelope_context: &HashMap<String, String>,
    request_context: &HashMap<String, String>,
) -> Result<()> {
    for (key, expected_value) in envelope_context {
        match request_context.get(key) {
            Some(actual_value) if actual_value == expected_value => {}
            Some(actual_value) => {
                return Err(KmsError::context_mismatch(format!(
                    "Context mismatch for key '{key}': expected '{expected_value}', got '{actual_value}'"
                )));
            }
            None if request_context.is_empty() => {}
            None => return Err(KmsError::context_mismatch(format!("Missing context key '{key}'"))),
        }
    }
    Ok(())
}

/// Page size used when a [`ListKeysRequest`] does not ask for one.
pub(crate) const DEFAULT_LIST_KEYS_PAGE_SIZE: u32 = 100;

/// Largest page a single [`ListKeysRequest`] can be served.
///
/// A page is not a cheap slice: every listed identifier costs the backend one
/// metadata lookup — a disk read on Local, an HTTP round trip on Vault Transit —
/// so an unbounded `limit` turns one request into an unbounded fan-out against
/// the key store. The ceiling is applied where the page is cut rather than at
/// each caller, so no backend can opt out of it.
pub(crate) const MAX_LIST_KEYS_PAGE_SIZE: u32 = 1_000;

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
///
/// A larger request is clamped to [`MAX_LIST_KEYS_PAGE_SIZE`] rather than
/// rejected: the caller still reaches every key by following `next_marker`, so
/// clamping costs it an extra round trip where rejecting would break it.
pub(crate) fn list_keys_page_size(limit: Option<u32>) -> Option<usize> {
    match limit.unwrap_or(DEFAULT_LIST_KEYS_PAGE_SIZE) {
        0 => None,
        size => Some(size.min(MAX_LIST_KEYS_PAGE_SIZE) as usize),
    }
}

/// The response to a request for zero keys.
///
/// `truncated` is false even when keys exist: a zero-length page carries no
/// identifier to resume from, so claiming more results would hand back a cursor
/// the caller can never advance and turn a `while truncated` loop into a
/// non-terminating one.
pub(crate) fn empty_key_page() -> ListKeysResponse {
    ListKeysResponse {
        keys: Vec::new(),
        next_marker: None,
        truncated: false,
        unreadable_key_ids: Vec::new(),
    }
}

/// What a failed per-key describe means for the page being assembled.
pub(crate) enum ListedKeyFailure {
    /// The key disappeared between the identifier scan and the read. Concurrent
    /// removal is normal and the cursor comes from the identifier list, so the
    /// listing drops it and advances.
    Vanished,
    /// The record is still in the store and this build cannot interpret it.
    /// Reported through [`ListKeysResponse::unreadable_key_ids`] rather than
    /// omitted or turned into a whole-page failure.
    Unreadable,
}

/// Decide whether a per-key describe failure may be attributed to that one key.
///
/// `None` means it may not: the error describes something outside the record,
/// so the caller must fail the whole listing. Downgrading a timeout to "this key
/// is unreadable" would report a Vault outage as mass key corruption.
///
/// The material-level variants are all per-record by construction: each names
/// one key and stays true on re-read. That includes
/// [`KmsError::MaterialAuthenticationFailed`], which on the local backend means
/// one record's AEAD tag did not verify — bit rot or a torn write. The
/// systemic reading of the same variant, a process holding the wrong master
/// key, cannot reach a listing: it is rejected when the backend is constructed.
/// The whole-key-set guard in [`UnreadableKeys`] covers whatever slips past
/// that.
pub(crate) fn classify_listed_key_failure(error: &KmsError) -> Option<ListedKeyFailure> {
    match error {
        KmsError::KeyNotFound { .. } => Some(ListedKeyFailure::Vanished),
        KmsError::MaterialMissing { .. }
        | KmsError::MaterialCorrupt { .. }
        | KmsError::MaterialAuthenticationFailed { .. }
        | KmsError::UnsupportedFormatVersion { .. }
        | KmsError::BaselineVersionLost { .. } => Some(ListedKeyFailure::Unreadable),
        _ => None,
    }
}

/// Whether this request starts at the very beginning of the key set.
///
/// An empty `marker` is not the same as no marker to a caller, but it is to
/// [`paginate_keys`]: no identifier sorts at or below the empty string, so the
/// page starts at the first key either way. Testing `Option::is_none` alone
/// would let `?marker=` — which a generated pager that always emits its cursor
/// parameter sends on its first request — slip past the whole-key-set guard
/// below and get the empty, healthy-looking page that guard exists to prevent.
pub(crate) fn started_at_the_first_key(request: &ListKeysRequest) -> bool {
    request.marker.as_deref().is_none_or(str::is_empty)
}

/// The unreadable identifiers of one page, and the guarantee that a *complete*
/// listing never reports every key as damaged while looking empty.
///
/// The failure mode being guarded against is a shared cause — a mixed-version
/// node reading records in a format it has no reader for — that makes every key
/// unreadable at once. Answering that with `200 OK` and an empty `keys` list
/// looks, to any client written before `unreadable_key_ids` existed, exactly
/// like a deployment that has no keys.
///
/// The guard is deliberately scoped to a listing that reached the end of the key
/// set on its first page. Firing it per page instead would be a trap: with
/// `limit=1` a single damaged key would fail its own page, and since a failed
/// page carries no `next_marker` the caller could never advance past it — every
/// key behind the damaged one becomes permanently unreachable. A page that is
/// truncated, or that resumed from a marker, always reports per-key so paging
/// can advance; and an empty `keys` array on such a page is already a documented
/// state, because filters are applied after the page is cut.
#[derive(Default)]
pub(crate) struct UnreadableKeys {
    ids: Vec<String>,
    first_error: Option<KmsError>,
    readable: usize,
}

impl UnreadableKeys {
    pub(crate) fn saw_readable(&mut self) {
        self.readable += 1;
    }

    pub(crate) fn record(&mut self, key_id: &str, error: KmsError) {
        self.ids.push(key_id.to_string());
        self.first_error.get_or_insert(error);
    }

    /// The identifiers to report.
    ///
    /// `whole_key_set` says this page both started at the beginning and reached
    /// the end, so there is no further page a caller could advance to — which is
    /// what makes failing here safe. Every identifier has already been logged
    /// individually by the backend, so the single returned error does not hide
    /// the rest.
    pub(crate) fn into_reported_ids(self, whole_key_set: bool) -> Result<Vec<String>> {
        match self.first_error {
            Some(error) if whole_key_set && self.readable == 0 => Err(error),
            _ => Ok(self.ids),
        }
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

    /// List keys.
    ///
    /// `status_filter` and `usage_filter` narrow a page after it has been cut,
    /// so a filtered page can be shorter than the requested limit — and even
    /// empty — while keys remain: a caller must page until `truncated` is false
    /// rather than until a page comes back short. Every backend applies both
    /// filters; a backend that cannot answer a filter must fail rather than
    /// return the unfiltered set.
    ///
    /// Backends that slice their own key set do so with [`paginate_keys`],
    /// which fixes the ordering and the marker semantics; a backend paging
    /// through a remote API passes that API's own cursor through instead.
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

    /// Re-wrap an existing data key envelope under the key's current version,
    /// returning an envelope that protects the same data key.
    ///
    /// This is the primitive that makes retiring an old master key version
    /// possible at all: until every envelope a version wrapped has been moved
    /// onto the current version, destroying that version's material orphans
    /// every object whose data key it wrapped. Rotation alone does not shrink
    /// the blast radius of a leaked master key version, because envelopes
    /// written before it stay decryptable under the leaked material forever.
    ///
    /// Contract for implementations:
    ///
    /// - The plaintext data key must not be persisted, logged, or returned. It
    ///   is either never materialized at all (backends with a native rewrap) or
    ///   held only for the length of the re-wrap.
    /// - The destination version is always the key's *current* version. Wrapping
    ///   to any older version would create objects that nodes which resolve
    ///   envelopes to the current version cannot read.
    /// - Everything except the wrapping is carried over verbatim, encryption
    ///   context included, so the result is the same data key rewrapped and not
    ///   a new one. An envelope must never be moved between objects.
    /// - Idempotence: an envelope already wrapped by the current version comes
    ///   back byte for byte with [`RewrapDataKeyResponse::rewrapped`] false, so
    ///   re-running a sweep converges and performs no metadata writes.
    ///
    /// Only backends that advertise [`BackendCapabilities::rewrap`] may override
    /// this method; the default rejects the operation. A backend without
    /// retained version history has nothing to rewrap *to*, so it reports the
    /// capability gap rather than performing a re-wrap that changes no version
    /// and buys no security.
    async fn rewrap_data_key(&self, _request: RewrapDataKeyRequest) -> Result<RewrapDataKeyResponse> {
        Err(KmsError::unsupported_capability(
            "backend without versioned key material to rewrap onto",
            "rewrap_data_key",
        ))
    }

    /// Report which master key version wraps an existing data key envelope, and
    /// whether that is the key's current version.
    ///
    /// The read-only counterpart of [`Self::rewrap_data_key`], and the only
    /// supported way to ask the question: each rotating backend records the
    /// version somewhere else, so every caller that parsed envelopes itself
    /// would carry its own copy of that knowledge outside the KMS boundary.
    ///
    /// [`DescribeDataKeyWrappingResponse::is_current`] must be the exact
    /// negation of what [`RewrapDataKeyResponse::rewrapped`] would report for
    /// the same envelope. The two answers drive a single loop — scan to find
    /// work, rewrap to do it, scan again to prove it is done — and a
    /// disagreement would make that loop either never terminate or declare
    /// success while envelopes still reference a version somebody is about to
    /// destroy.
    ///
    /// Deliberately not gated on key state: an inventory has to be able to
    /// count envelopes under keys that are disabled or already scheduled for
    /// deletion, which are precisely the keys whose retirement is in question.
    ///
    /// Gated by the same [`BackendCapabilities::rewrap`] flag, because a backend
    /// that cannot rewrap has no version to report progress against.
    async fn describe_data_key_wrapping(
        &self,
        _request: DescribeDataKeyWrappingRequest,
    ) -> Result<DescribeDataKeyWrappingResponse> {
        Err(KmsError::unsupported_capability(
            "backend without versioned key material to rewrap onto",
            "describe_data_key_wrapping",
        ))
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
    /// Re-wrapping an existing data key envelope onto the key's current version
    pub rewrap: bool,
    /// Whether this backend is positioned for production use.
    ///
    /// Backends that keep their cryptographic root on the local host (Local,
    /// Static) are for development, testing and demos only; this flag is how
    /// that positioning reaches logs, the status API and the console without
    /// each consumer matching on backend names.
    #[serde(default)]
    pub production_supported: bool,
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
            rewrap: false,
            production_supported: false,
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

    /// Set whether envelope rewrap onto the current key version is supported
    pub const fn with_rewrap(mut self, rewrap: bool) -> Self {
        self.rewrap = rewrap;
        self
    }

    /// Set whether the backend is positioned for production use
    pub const fn with_production_supported(mut self, production_supported: bool) -> Self {
        self.production_supported = production_supported;
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
    use base64_simd::STANDARD as BASE64;

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
        assert!(!capabilities.rewrap);
        // Production positioning is an explicit claim, never inherited.
        assert!(!capabilities.production_supported);
    }

    /// Older peers and consoles serialize capabilities without the
    /// positioning flag; deserializing their payloads must not fail and must
    /// default to the conservative claim.
    #[test]
    fn capabilities_without_positioning_field_deserialize_as_non_production() {
        let legacy = serde_json::json!({
            "encrypt": true,
            "decrypt": true,
            "generate_data_key": true,
            "rotate": true,
            "enable_disable": true,
            "schedule_deletion": true,
            "versioning": true,
            "physical_delete": true,
            "update_key_metadata": true,
            "rewrap": true,
        });
        let capabilities: BackendCapabilities =
            serde_json::from_value(legacy).expect("legacy capability payloads must stay deserializable");
        assert!(!capabilities.production_supported);
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
            (
                "rewrap_data_key",
                MinimalBackend
                    .rewrap_data_key(RewrapDataKeyRequest {
                        ciphertext: b"{}".to_vec(),
                        encryption_context: HashMap::new(),
                    })
                    .await
                    .map(|_| ()),
            ),
            (
                "describe_data_key_wrapping",
                MinimalBackend
                    .describe_data_key_wrapping(DescribeDataKeyWrappingRequest {
                        ciphertext: b"{}".to_vec(),
                        encryption_context: HashMap::new(),
                    })
                    .await
                    .map(|_| ()),
            ),
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

    /// The rewrap context guard has to sit exactly where the decrypt guard
    /// sits: strict enough that an envelope cannot be laundered under a
    /// different context, lax enough that everything decrypt accepts can still
    /// be rewrapped — otherwise a retirement sweep stalls on readable objects.
    #[test]
    fn rewrap_context_guard_matches_the_decrypt_rule() {
        let envelope = HashMap::from([
            ("bucket".to_string(), "b/o".to_string()),
            ("tenant".to_string(), "acme".to_string()),
        ]);

        ensure_rewrap_context_matches(&envelope, &envelope).expect("an exact context must be accepted");
        // An empty request context is the pre-context-recording compatibility
        // case decrypt allows; rewrap must not be stricter.
        ensure_rewrap_context_matches(&envelope, &HashMap::new()).expect("an empty request context must stay accepted");
        // Extra entries the envelope does not carry are ignored, as on decrypt.
        let mut superset = envelope.clone();
        superset.insert("extra".to_string(), "ignored".to_string());
        ensure_rewrap_context_matches(&envelope, &superset).expect("extra request context entries must be ignored");

        let mut tampered = envelope.clone();
        tampered.insert("bucket".to_string(), "other/o".to_string());
        assert!(
            matches!(ensure_rewrap_context_matches(&envelope, &tampered), Err(KmsError::ContextMismatch { .. })),
            "a tampered context value must be rejected"
        );

        let partial = HashMap::from([("tenant".to_string(), "acme".to_string())]);
        assert!(
            matches!(ensure_rewrap_context_matches(&envelope, &partial), Err(KmsError::ContextMismatch { .. })),
            "a non-empty context missing an envelope entry must be rejected"
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
        let config = KmsConfig::static_kms("static-key".to_string(), BASE64.encode_to_string([0u8; 32]));
        let backend = static_kms::StaticKmsBackend::new(config)
            .await
            .expect("static backend should build");

        insta::assert_json_snapshot!("static_backend_capabilities", capabilities_snapshot(backend.capabilities()));
    }

    // -- Pagination boundaries ----------------------------------------------

    fn key_ids(count: usize) -> Vec<String> {
        (0..count).map(|index| format!("key-{index:02}")).collect()
    }

    fn page_request(limit: Option<u32>, marker: Option<&str>) -> ListKeysRequest {
        ListKeysRequest {
            limit,
            marker: marker.map(str::to_string),
            usage_filter: None,
            status_filter: None,
        }
    }

    fn page_of(keys: &[String], limit: Option<u32>, marker: Option<&str>) -> (Vec<String>, Option<String>, bool) {
        let page = paginate_keys(keys, &page_request(limit, marker), String::as_str);
        (page.items.to_vec(), page.next_marker, page.truncated)
    }

    /// Zero keys requested, zero keys returned — and no cursor, so a caller
    /// looping on `truncated` terminates instead of asking forever. Slicing a
    /// zero-length page out of a non-empty key set must not reach for the
    /// element before the page either.
    #[test]
    fn zero_limit_returns_an_empty_untruncated_page() {
        let keys = key_ids(3);
        assert_eq!(page_of(&keys, Some(0), None), (Vec::new(), None, false));
        assert_eq!(page_of(&keys, Some(0), Some("key-01")), (Vec::new(), None, false));
        // Also at the ends of the key set, where a page has no predecessor.
        assert_eq!(page_of(&[], Some(0), None), (Vec::new(), None, false));
        assert_eq!(page_of(&keys, Some(0), Some("key-02")), (Vec::new(), None, false));

        assert_eq!(list_keys_page_size(Some(0)), None);
        assert_eq!(list_keys_page_size(None), Some(DEFAULT_LIST_KEYS_PAGE_SIZE as usize));
        assert_eq!(list_keys_page_size(Some(7)), Some(7));
    }

    /// A limit past the end of the key set is not an overflow.
    #[test]
    fn oversized_limit_returns_the_whole_key_set_once() {
        let keys = key_ids(3);
        assert_eq!(page_of(&keys, Some(u32::MAX), None), (keys.clone(), None, false));
        assert_eq!(page_of(&keys, Some(u32::MAX), Some("key-01")), (vec![keys[2].clone()], None, false));
    }

    /// A page is capped however large a limit the caller asks for, and the
    /// capped page still carries a cursor, so the caller reaches every key
    /// instead of being cut off at the ceiling.
    #[test]
    fn page_size_is_capped_and_the_capped_page_still_advances() {
        // Pinned as a number, not only symbolically: the published contract in
        // `docs/operations/kms-admin-contract.md` states this exact ceiling, so
        // raising it is an API change and has to be a deliberate edit here.
        assert_eq!(MAX_LIST_KEYS_PAGE_SIZE, 1_000);
        assert_eq!(
            list_keys_page_size(Some(u32::MAX)),
            Some(MAX_LIST_KEYS_PAGE_SIZE as usize),
            "an unbounded limit must not become an unbounded per-key fan-out"
        );
        assert_eq!(list_keys_page_size(Some(MAX_LIST_KEYS_PAGE_SIZE)), Some(MAX_LIST_KEYS_PAGE_SIZE as usize));
        // Under the ceiling the caller's limit is still honoured exactly.
        assert_eq!(
            list_keys_page_size(Some(MAX_LIST_KEYS_PAGE_SIZE - 1)),
            Some(MAX_LIST_KEYS_PAGE_SIZE as usize - 1)
        );

        // Padded to a fixed width so the vector is sorted by identifier, which
        // is what `paginate_keys` requires of its input.
        let keys: Vec<String> = (0..MAX_LIST_KEYS_PAGE_SIZE as usize + 5)
            .map(|index| format!("key-{index:04}"))
            .collect();
        let (items, next_marker, truncated) = page_of(&keys, Some(u32::MAX), None);
        assert_eq!(items.len(), MAX_LIST_KEYS_PAGE_SIZE as usize);
        assert!(truncated);
        let next_marker = next_marker.expect("a capped page must hand back a cursor");
        assert_eq!(next_marker, items.last().cloned().expect("page is non-empty"));

        let (rest, _, still_truncated) = page_of(&keys, Some(u32::MAX), Some(&next_marker));
        assert_eq!(rest.len(), 5, "following the cursor must reach the keys the cap held back");
        assert!(!still_truncated);
    }

    /// The cursor is an identifier, so a marker naming a key that no longer
    /// exists resumes after where it would have been instead of restarting.
    #[test]
    fn marker_for_a_removed_key_resumes_after_it() {
        let keys = vec!["key-00".to_string(), "key-02".to_string()];
        assert_eq!(page_of(&keys, Some(10), Some("key-01")), (vec!["key-02".to_string()], None, false));
        // A marker past every key ends the listing rather than wrapping.
        assert_eq!(page_of(&keys, Some(10), Some("key-99")), (Vec::new(), None, false));
        // A marker before every key yields the whole set.
        assert_eq!(page_of(&keys, Some(10), Some("key")), (keys.clone(), None, false));
    }

    /// Truncation flips exactly at the page boundary, and paging covers the
    /// key set once end to end.
    #[test]
    fn pages_tile_the_key_set_exactly_at_the_limit_boundary() {
        let keys = key_ids(4);
        assert_eq!(page_of(&keys, Some(4), None), (keys.clone(), None, false));
        assert_eq!(page_of(&keys, Some(3), None), (keys[..3].to_vec(), Some("key-02".to_string()), true));

        let mut seen = Vec::new();
        let mut marker = None;
        loop {
            let (items, next_marker, truncated) = page_of(&keys, Some(2), marker.as_deref());
            seen.extend(items);
            if !truncated {
                assert!(next_marker.is_none(), "a final page must not offer a cursor");
                break;
            }
            marker = Some(next_marker.expect("a truncated page must offer a cursor"));
        }
        assert_eq!(seen, keys, "paging must tile the key set exactly once");
    }
}
