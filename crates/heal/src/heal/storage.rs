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

use crate::{Error, Result};
use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use rustfs_common::heal_channel::{HealOpts, HealScanMode};
use rustfs_madmin::heal_commands::HealResultItem;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, error, warn};

use super::progress::stable_generation;
use super::storage_api::owner::{EcstoreHealLifecycleExpiryContext, ecstore_load_admin_data_usage_from_backend_cached};
use super::storage_api::storage::{
    BucketInfo, BucketOperations, DiskSetSelector, HealOperations as _, ListOperations as _, ObjectIO as _,
    ObjectOperations as _, StorageAdminApi,
};
use super::{DiskStore, ECStore, HealDiskExt as _, StorageError, resume::ReplacementTargetIdentity};
pub use super::{HealObjectInfo, HealObjectOptions, HealPutObjReader};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HealBucketUsageBaseline {
    pub objects_count: u64,
    pub bytes: u64,
    /// Stable identity of the validated usage snapshot and selected scope.
    /// `None` is retained for test/legacy providers that cannot expose one.
    pub generation: Option<u64>,
}

pub struct HealLifecycleExpiryContext {
    inner: HealLifecycleExpiryContextInner,
}

enum HealLifecycleExpiryContextInner {
    Ecstore(EcstoreHealLifecycleExpiryContext),
    #[allow(
        dead_code,
        reason = "constructed by the #[cfg(test)] `test()` helper; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    Test,
}

impl HealLifecycleExpiryContext {
    fn ecstore(inner: EcstoreHealLifecycleExpiryContext) -> Self {
        Self {
            inner: HealLifecycleExpiryContextInner::Ecstore(inner),
        }
    }

    #[cfg(test)]
    pub(crate) fn test() -> Self {
        Self {
            inner: HealLifecycleExpiryContextInner::Test,
        }
    }
}

const LOG_COMPONENT_HEAL: &str = "heal";
const LOG_SUBSYSTEM_STORAGE: &str = "storage";
const EVENT_HEAL_STORAGE_OBJECT_IO: &str = "heal_storage_object_io";
const EVENT_HEAL_STORAGE_OBJECT_READ_LIMIT: &str = "heal_storage_object_read_limit";
const EVENT_HEAL_STORAGE_ADMIN_OP: &str = "heal_storage_admin_op";
const EVENT_HEAL_STORAGE_REPAIR_OP: &str = "heal_storage_repair_op";

pub enum ReplacementResumeDisk {
    Fresh,
    Existing(DiskStore),
}

pub(crate) fn next_heal_listing_token(
    bucket: &str,
    prefix: &str,
    next_token: Option<String>,
    is_truncated: bool,
) -> Result<Option<String>> {
    if !is_truncated {
        return Ok(None);
    }

    match next_token {
        Some(token) => Ok(Some(token)),
        None => {
            // A version listing legitimately reports the final page as truncated
            // when the last object's versions land exactly on the page boundary
            // yet the backend has nothing further to yield. Treat a missing
            // continuation token as end-of-listing rather than a hard error so
            // the heal pass terminates cleanly instead of failing the bucket.
            warn!(
                target: "rustfs::heal::storage",
                event = EVENT_HEAL_STORAGE_ADMIN_OP,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_STORAGE,
                operation = "next_heal_listing_token",
                bucket,
                prefix,
                state = "truncated_without_token",
                "Heal storage object listing truncated without continuation token; treating as end of listing"
            );
            Ok(None)
        }
    }
}

/// Opaque continuation token payload for heal version listing. Encodes the
/// `(marker, version_marker)` pair that `list_object_versions` needs to resume.
#[derive(Debug, Serialize, Deserialize)]
struct HealTokenPayload {
    /// object key marker
    #[serde(rename = "m")]
    m: Option<String>,
    /// version-id marker
    #[serde(rename = "v")]
    v: Option<String>,
}

const HEAL_TOKEN_PREFIX: &str = "v1:";

/// Encode a `(marker, version_marker)` pair into an opaque heal continuation
/// token. The token is `"v1:" + base64url_nopad(json)`.
///
/// Invariant: `list_object_versions` returns `NotImplemented` for
/// `(None, Some(_))`, so callers must never produce that pair. This is checked
/// with a `debug_assert!`.
pub(crate) fn encode_heal_token(marker: Option<&str>, version_marker: Option<&str>) -> String {
    debug_assert!(
        !(marker.is_none() && version_marker.is_some()),
        "encode_heal_token must never be called with (None, Some(_))"
    );

    let payload = HealTokenPayload {
        m: marker.map(str::to_string),
        v: version_marker.map(str::to_string),
    };
    // serde_json of a simple two-Option struct cannot fail; fall back to an
    // empty object rather than panicking if it somehow does.
    let json = serde_json::to_vec(&payload).unwrap_or_else(|_| b"{}".to_vec());
    format!("{HEAL_TOKEN_PREFIX}{}", URL_SAFE_NO_PAD.encode(json))
}

/// Decode an opaque heal continuation token back into `(marker, version_marker)`.
///
/// TOTAL function: an empty token, a missing `"v1:"` prefix, invalid base64, or
/// invalid JSON all decode to `(None, None)` (start from the beginning). A
/// decoded `(None, Some(_))` is coerced to `(None, None)` to preserve the
/// `list_object_versions` invariant.
pub(crate) fn decode_heal_token(token: &str) -> (Option<String>, Option<String>) {
    if token.is_empty() {
        return (None, None);
    }

    let Some(encoded) = token.strip_prefix(HEAL_TOKEN_PREFIX) else {
        warn!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_ADMIN_OP,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "decode_heal_token",
            state = "missing_prefix",
            "Heal continuation token missing version prefix; restarting listing"
        );
        return (None, None);
    };

    let bytes = match URL_SAFE_NO_PAD.decode(encoded) {
        Ok(bytes) => bytes,
        Err(e) => {
            warn!(
                target: "rustfs::heal::storage",
                event = EVENT_HEAL_STORAGE_ADMIN_OP,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_STORAGE,
                operation = "decode_heal_token",
                state = "bad_base64",
                error = %e,
                "Heal continuation token has invalid base64; restarting listing"
            );
            return (None, None);
        }
    };

    let payload: HealTokenPayload = match serde_json::from_slice(&bytes) {
        Ok(payload) => payload,
        Err(e) => {
            warn!(
                target: "rustfs::heal::storage",
                event = EVENT_HEAL_STORAGE_ADMIN_OP,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_STORAGE,
                operation = "decode_heal_token",
                state = "bad_json",
                error = %e,
                "Heal continuation token has invalid payload; restarting listing"
            );
            return (None, None);
        }
    };

    // Preserve the list_object_versions invariant: (None, Some(_)) is illegal.
    if payload.m.is_none() && payload.v.is_some() {
        warn!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_ADMIN_OP,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "decode_heal_token",
            state = "illegal_version_only_marker",
            "Heal continuation token had a version marker without an object marker; restarting listing"
        );
        return (None, None);
    }

    (payload.m, payload.v)
}

const DISK_WALK_TOKEN_PREFIX: &str = "dw1:";

/// Encode the disk-walk resume cursor (`next_forward` object key) into an opaque
/// continuation token: `"dw1:" + base64url_nopad(forward)`.
///
/// The `dw1:` namespace is DISJOINT from the B5 `v1:` token namespace so the two
/// enumerators can never misread each other's cursor: a `dw1:` token decodes to
/// `(None, None)` under the B5 decoder, and a `v1:` token decodes to `None` here.
pub(crate) fn encode_disk_walk_token(next_forward: &str) -> String {
    format!("{DISK_WALK_TOKEN_PREFIX}{}", URL_SAFE_NO_PAD.encode(next_forward.as_bytes()))
}

/// Decode a disk-walk continuation token back into the `next_forward` object key.
///
/// TOTAL function: an empty token, a missing `"dw1:"` prefix (including a foreign
/// B5 `v1:` token), invalid base64, or invalid UTF-8 all decode to `None` (start
/// the walk from the beginning). This makes a restart across an enumerator switch
/// idempotent rather than corrupting.
pub(crate) fn decode_disk_walk_token(token: &str) -> Option<String> {
    if token.is_empty() {
        return None;
    }

    let Some(encoded) = token.strip_prefix(DISK_WALK_TOKEN_PREFIX) else {
        warn!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_ADMIN_OP,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "decode_disk_walk_token",
            state = "foreign_or_missing_prefix",
            "Disk-walk continuation token missing dw1 prefix; restarting walk"
        );
        return None;
    };

    let bytes = match URL_SAFE_NO_PAD.decode(encoded) {
        Ok(bytes) => bytes,
        Err(e) => {
            warn!(
                target: "rustfs::heal::storage",
                event = EVENT_HEAL_STORAGE_ADMIN_OP,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_STORAGE,
                operation = "decode_disk_walk_token",
                state = "bad_base64",
                error = %e,
                "Disk-walk continuation token has invalid base64; restarting walk"
            );
            return None;
        }
    };

    match String::from_utf8(bytes) {
        Ok(forward) if !forward.is_empty() => Some(forward),
        Ok(_) => None,
        Err(e) => {
            warn!(
                target: "rustfs::heal::storage",
                event = EVENT_HEAL_STORAGE_ADMIN_OP,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_STORAGE,
                operation = "decode_disk_walk_token",
                state = "bad_utf8",
                error = %e,
                "Disk-walk continuation token has invalid utf8; restarting walk"
            );
            None
        }
    }
}

/// A single object version to heal.
///
/// `is_delete_marker` is OBSERVABILITY-ONLY (metrics / logging / e2e
/// assertions); it MUST NOT gate healing logic. Whether the delete-marker path
/// or the data path is taken is decided internally in `ops/heal.rs` from
/// `latest_meta.deleted`. `version_id` is normalized (nil/absent UUID => `None`)
/// at the single construction point in `list_objects_for_heal_page`.
#[derive(Debug, Clone)]
pub struct HealListItem {
    /// object key
    pub name: String,
    /// normalized version id (`None` when the version is nil/absent)
    pub version_id: Option<String>,
    /// version modification time as Unix nanoseconds
    pub mod_time_unix_nanos: Option<i128>,
    /// object snapshot for lifecycle evaluation
    pub lifecycle_object_info: Option<HealObjectInfo>,
    /// whether this version is a delete marker (observability only)
    pub is_delete_marker: bool,
}

/// Heal storage layer interface
#[async_trait]
pub trait HealStorageAPI: Send + Sync {
    /// Get object meta
    ///
    /// Reserved for HS-01 MRF wiring (rustfs/backlog#1865): MRF intents
    /// currently execute through `heal_object`; keep this entry point for the
    /// metadata-corruption variant that must inspect metadata first.
    async fn get_object_meta(&self, bucket: &str, object: &str) -> Result<Option<HealObjectInfo>>;

    /// EC decode rebuild
    ///
    /// Reserved for HS-01 MRF wiring (rustfs/backlog#1865): urgent ECDecode
    /// requests currently execute through `heal_object`; keep the explicit
    /// rebuild-and-read path for the decode-failure fast variant.
    async fn ec_decode_rebuild(&self, bucket: &str, object: &str) -> Result<Vec<u8>>;

    /// Get bucket info
    async fn get_bucket_info(&self, bucket: &str) -> Result<Option<BucketInfo>>;

    /// Aggregate usage-cache baselines for the requested buckets.
    async fn erasure_set_usage_baseline(&self, _buckets: &[String]) -> Result<Option<HealBucketUsageBaseline>> {
        Ok(None)
    }

    /// Load per-bucket lifecycle expiry context for heal skips.
    async fn load_heal_lifecycle_expiry_context(&self, _bucket: &str) -> Result<Option<HealLifecycleExpiryContext>> {
        Ok(None)
    }

    /// Queue lifecycle expiry for a version that heal can skip.
    async fn enqueue_heal_lifecycle_expiry(
        &self,
        _context: &HealLifecycleExpiryContext,
        _bucket: &str,
        _object: &str,
        _version_id: Option<&str>,
        _object_info: Option<&HealObjectInfo>,
    ) -> Result<bool> {
        Ok(false)
    }

    /// Get all buckets
    async fn list_buckets(&self) -> Result<Vec<BucketInfo>>;

    /// Check object exists
    async fn object_exists(&self, bucket: &str, object: &str) -> Result<bool>;

    /// Heal object using ecstore
    async fn heal_object(
        &self,
        bucket: &str,
        object: &str,
        version_id: Option<&str>,
        opts: &HealOpts,
    ) -> Result<(HealResultItem, Option<Error>)>;

    /// Heal bucket using ecstore
    async fn heal_bucket(&self, bucket: &str, opts: &HealOpts) -> Result<HealResultItem>;

    /// Heal format using ecstore
    async fn heal_format(&self, dry_run: bool) -> Result<(HealResultItem, Option<Error>)>;

    /// Heal only the explicitly admitted replacement targets in one erasure set.
    ///
    /// The default is deliberately fail-closed so alternate storage
    /// implementations cannot accidentally fall back to the global format path.
    async fn heal_replacement_format(
        &self,
        _dry_run: bool,
        _pool_index: usize,
        _set_index: usize,
        _targets: &[String],
    ) -> Result<(HealResultItem, Option<Error>)> {
        Err(Error::other("target-scoped replacement format is unsupported"))
    }

    /// Recheck admitted replacement targets immediately before destructive work.
    async fn replacement_targets_ready(&self, _targets: &[String]) -> Result<bool> {
        Ok(false)
    }

    /// Read target-specific physical evidence for one replacement version.
    ///
    /// This is only used by automatic replacement healing after the normal
    /// transaction returns success. The conservative default prevents an
    /// alternate backend from turning an unverified replacement into a
    /// completed generation.
    async fn replacement_targets_have_version(
        &self,
        _bucket: &str,
        _object: &str,
        _version_id: Option<&str>,
        _opts: &HealOpts,
        _targets: &[String],
    ) -> Result<bool> {
        Ok(false)
    }

    /// List object versions for healing with pagination (returns one page and continuation token)
    /// Returns (versions, next_continuation_token, is_truncated). The continuation token is an
    /// opaque composite `(marker, version_marker)` value — see `encode_heal_token`/`decode_heal_token`.
    async fn list_objects_for_heal_page(
        &self,
        bucket: &str,
        prefix: &str,
        continuation_token: Option<&str>,
        include_lifecycle_object_info: bool,
    ) -> Result<(Vec<HealListItem>, Option<String>, bool)>;

    /// List versions for healing via a per-erasure-set DISK-WALK union enumerator
    /// (backlog#920). Unlike `list_objects_for_heal_page` (which reflects only the
    /// READ-QUORUM metadata view via `list_object_versions`), this surfaces every
    /// `(object, version)` present on ANY disk in the set identified by
    /// `set_disk_id`, so sub-quorum-but-reconstructable versions are healed.
    ///
    /// The continuation token uses the disjoint `"dw1:"` namespace. The DEFAULT
    /// implementation falls back to the read-quorum listing so mock/alternate
    /// storages keep compiling and behaving; `ECStoreHealStorage` overrides it
    /// with the real disk walk.
    async fn list_versions_for_heal_page_disk_walk(
        &self,
        _set_disk_id: &str,
        bucket: &str,
        prefix: &str,
        continuation_token: Option<&str>,
        include_lifecycle_object_info: bool,
    ) -> Result<(Vec<HealListItem>, Option<String>, bool)> {
        self.list_objects_for_heal_page(bucket, prefix, continuation_token, include_lifecycle_object_info)
            .await
    }

    /// Get disk for resume functionality.
    async fn get_disk_for_resume(&self, set_disk_id: &str) -> Result<DiskStore>;

    /// Get a healthy non-target disk for durable replacement state.
    async fn get_disk_for_resume_excluding(&self, _set_disk_id: &str, _excluded_targets: &[String]) -> Result<DiskStore> {
        Err(Error::other("target-excluding resume disk selection is unsupported"))
    }

    /// Reopen the exact surviving disk that owns an existing replacement
    /// intent. Falling back to another disk would create a second copy of the
    /// same generation and split its progress.
    async fn get_replacement_resume_disk(
        &self,
        _set_disk_id: &str,
        _task_id: &str,
        _excluded_targets: &[String],
    ) -> Result<ReplacementResumeDisk> {
        Err(Error::other("durable replacement resume selection is unsupported"))
    }

    /// Capture the mounted replacement instance before it is formatted.
    async fn replacement_target_identities(&self, _targets: &[String]) -> Result<Vec<ReplacementTargetIdentity>> {
        Err(Error::other("replacement target identity collection is unsupported"))
    }
}

/// ECStore Heal storage layer implementation
pub struct ECStoreHealStorage {
    ecstore: Arc<ECStore>,
}

impl ECStoreHealStorage {
    pub fn new(ecstore: Arc<ECStore>) -> Self {
        Self { ecstore }
    }

    /// Read back an object's bytes, capped to bound memory.
    ///
    /// Private support for the reserved `ec_decode_rebuild` (HS-01); not part
    /// of the storage trait surface.
    async fn get_object_data(&self, bucket: &str, object: &str) -> Result<Option<Vec<u8>>> {
        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_OBJECT_IO,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "get_object_data",
            bucket,
            object,
            "Heal storage request started"
        );

        let reader = match (*self.ecstore)
            .get_object_reader(bucket, object, None, Default::default(), &Default::default())
            .await
        {
            Ok(reader) => reader,
            Err(e) => {
                error!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_OBJECT_IO,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    operation = "get_object_data",
                    bucket,
                    object,
                    result = "failed",
                    error = %e,
                    "Heal storage request failed"
                );
                return Err(Error::other(e));
            }
        };

        // WARNING: Returning Vec<u8> for large objects is dangerous. To avoid OOM, cap the read size.
        // If needed, refactor callers to stream instead of buffering entire object.
        const MAX_READ_BYTES: usize = 16 * 1024 * 1024; // 16 MiB cap
        let mut buf = Vec::with_capacity(1024 * 1024);
        use tokio::io::AsyncReadExt as _;
        let mut n_read: usize = 0;
        let mut stream = reader.stream;
        loop {
            // Read in chunks
            let mut chunk = vec![0u8; 1024 * 1024];
            match stream.read(&mut chunk).await {
                Ok(0) => break,
                Ok(n) => {
                    buf.extend_from_slice(&chunk[..n]);
                    n_read += n;
                    if n_read > MAX_READ_BYTES {
                        warn!(
                            target: "rustfs::heal::storage",
                            event = EVENT_HEAL_STORAGE_OBJECT_READ_LIMIT,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_STORAGE,
                            bucket,
                            object,
                            max_read_bytes = MAX_READ_BYTES,
                            bytes_read = n_read,
                            "Heal storage aborted object read after reaching safety cap"
                        );
                        return Err(Error::other(format!(
                            "Object too large: {n_read} bytes (max: {MAX_READ_BYTES} bytes) for {bucket}/{object}"
                        )));
                    }
                }
                Err(e) => {
                    error!(
                        target: "rustfs::heal::storage",
                        event = EVENT_HEAL_STORAGE_OBJECT_IO,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_STORAGE,
                        operation = "read_object_data",
                        bucket,
                        object,
                        result = "failed",
                        error = %e,
                        "Heal storage request failed"
                    );
                    return Err(Error::other(e));
                }
            }
        }
        Ok(Some(buf))
    }
}

fn is_transient_object_exists_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();

    [
        "failed to acquire read lock",
        "lock acquisition failed",
        "lock acquisition timeout",
        "quorum not reached",
        "deadline has elapsed",
        "timed out",
        "network error",
        "transport error",
        "connection refused",
    ]
    .iter()
    .any(|pattern| message.contains(pattern))
}

fn is_transient_object_exists_error(err: &StorageError) -> bool {
    if err.is_quorum_error() {
        return true;
    }

    match err {
        StorageError::Lock(lock_err) => lock_err.is_retryable() || is_transient_object_exists_message(&lock_err.to_string()),
        StorageError::Io(io_err) => is_transient_object_exists_message(&io_err.to_string()),
        StorageError::SlowDown | StorageError::OperationCanceled => true,
        _ => false,
    }
}

#[async_trait]
impl HealStorageAPI for ECStoreHealStorage {
    async fn get_object_meta(&self, bucket: &str, object: &str) -> Result<Option<HealObjectInfo>> {
        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_OBJECT_IO,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "get_object_meta",
            bucket,
            object,
            "Heal storage request started"
        );

        match self.ecstore.get_object_info(bucket, object, &Default::default()).await {
            Ok(info) => Ok(Some(info)),
            Err(e) => {
                // Map ObjectNotFound to None to align with Option return type
                if matches!(e, StorageError::ObjectNotFound(_, _)) {
                    debug!(
                        target: "rustfs::heal::storage",
                        event = EVENT_HEAL_STORAGE_OBJECT_IO,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_STORAGE,
                        operation = "get_object_meta",
                        bucket,
                        object,
                        result = "not_found",
                        "Heal storage object metadata missing"
                    );
                    Ok(None)
                } else {
                    error!(
                        target: "rustfs::heal::storage",
                        event = EVENT_HEAL_STORAGE_OBJECT_IO,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_STORAGE,
                        operation = "get_object_meta",
                        bucket,
                        object,
                        result = "failed",
                        error = %e,
                        "Heal storage request failed"
                    );
                    Err(Error::other(e))
                }
            }
        }
    }

    async fn ec_decode_rebuild(&self, bucket: &str, object: &str) -> Result<Vec<u8>> {
        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_REPAIR_OP,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "ec_decode_rebuild",
            bucket,
            object,
            state = "started",
            "Heal storage repair started"
        );

        // Use ecstore's heal_object to rebuild the object
        let heal_opts = HealOpts {
            recursive: false,
            dry_run: false,
            remove: false,
            recreate: true,
            scan_mode: HealScanMode::Deep,
            update_parity: true,
            no_lock: false,
            pool: None,
            set: None,
        };

        match self.heal_object(bucket, object, None, &heal_opts).await {
            Ok((_result, error)) => {
                if error.is_some() {
                    return Err(Error::TaskExecutionFailed {
                        message: format!("Heal failed: {error:?}"),
                    });
                }

                // After healing, try to read the object data
                match self.get_object_data(bucket, object).await? {
                    Some(data) => {
                        debug!(
                            target: "rustfs::heal::storage",
                            event = EVENT_HEAL_STORAGE_REPAIR_OP,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_STORAGE,
                            operation = "ec_decode_rebuild",
                            bucket,
                            object,
                            bytes = data.len(),
                            state = "ok",
                            "Heal storage EC decode rebuild completed"
                        );
                        Ok(data)
                    }
                    None => {
                        error!(
                            target: "rustfs::heal::storage",
                            event = EVENT_HEAL_STORAGE_REPAIR_OP,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_STORAGE,
                            operation = "ec_decode_rebuild",
                            bucket,
                            object,
                            state = "missing_after_heal",
                            "Heal storage repair failed"
                        );
                        Err(Error::TaskExecutionFailed {
                            message: format!("Object not found after heal: {bucket}/{object}"),
                        })
                    }
                }
            }
            Err(e) => {
                error!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_REPAIR_OP,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    operation = "ec_decode_rebuild",
                    bucket,
                    object,
                    state = "failed",
                    error = %e,
                    "Heal storage repair failed"
                );
                Err(e)
            }
        }
    }

    async fn get_bucket_info(&self, bucket: &str) -> Result<Option<BucketInfo>> {
        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_ADMIN_OP,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "get_bucket_info",
            bucket,
            state = "started",
            "Heal storage admin operation started"
        );

        match self.ecstore.get_bucket_info(bucket, &Default::default()).await {
            Ok(info) => Ok(Some(info)),
            Err(e) => {
                error!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_ADMIN_OP,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    operation = "get_bucket_info",
                    bucket,
                    result = "failed",
                    error = %e,
                    "Heal storage admin operation failed"
                );
                Err(Error::other(e))
            }
        }
    }

    async fn erasure_set_usage_baseline(&self, buckets: &[String]) -> Result<Option<HealBucketUsageBaseline>> {
        if buckets.is_empty() {
            return Ok(None);
        }

        let info = match ecstore_load_admin_data_usage_from_backend_cached(self.ecstore.clone()).await {
            Ok(info) if info.is_complete_bucket_usage_snapshot() => info,
            Ok(_) | Err(_) => return Ok(None),
        };

        let mut baseline = HealBucketUsageBaseline::default();
        for bucket in buckets {
            if let Some(usage) = info.buckets_usage.get(bucket) {
                baseline.objects_count = match baseline.objects_count.checked_add(usage.objects_count) {
                    Some(total) => total,
                    // A corrupt/overflowing usage snapshot is not a usable
                    // denominator.  Leave progress indeterminate instead of
                    // turning saturation into a plausible percentage.
                    None => return Ok(None),
                };
                baseline.bytes = match baseline.bytes.checked_add(usage.size) {
                    Some(total) => total,
                    None => return Ok(None),
                };
            }
        }

        let identity = info.snapshot_identity();
        let mut canonical = Vec::new();
        match identity.last_update {
            Some(last_update) => {
                canonical.push(1);
                canonical.extend_from_slice(
                    &last_update
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos()
                        .to_be_bytes(),
                );
            }
            None => canonical.push(0),
        }
        for value in [identity.scanner_cycle, identity.scanner_epoch] {
            match value {
                Some(value) => {
                    canonical.push(1);
                    canonical.extend_from_slice(&value.to_be_bytes());
                }
                None => canonical.push(0),
            }
        }
        let mut scope = buckets.to_vec();
        scope.sort_unstable();
        for bucket in scope {
            canonical.extend_from_slice(&(bucket.len() as u64).to_be_bytes());
            canonical.extend_from_slice(bucket.as_bytes());
        }
        baseline.generation = Some(stable_generation(&[&canonical]));

        Ok(Some(baseline))
    }

    async fn load_heal_lifecycle_expiry_context(&self, bucket: &str) -> Result<Option<HealLifecycleExpiryContext>> {
        match self.ecstore.load_heal_lifecycle_expiry_context(bucket).await {
            Ok(Some(context)) => Ok(Some(HealLifecycleExpiryContext::ecstore(context))),
            Ok(None) => Ok(None),
            Err(err) => {
                debug!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_ADMIN_OP,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    operation = "load_heal_lifecycle_expiry_context",
                    bucket,
                    result = "failed",
                    error = %err,
                    "Heal storage lifecycle expiry context load failed"
                );
                Ok(None)
            }
        }
    }

    async fn enqueue_heal_lifecycle_expiry(
        &self,
        context: &HealLifecycleExpiryContext,
        bucket: &str,
        object: &str,
        version_id: Option<&str>,
        object_info: Option<&HealObjectInfo>,
    ) -> Result<bool> {
        let context = match &context.inner {
            HealLifecycleExpiryContextInner::Ecstore(context) => context,
            HealLifecycleExpiryContextInner::Test => return Ok(false),
        };
        match self
            .ecstore
            .enqueue_heal_lifecycle_expiry(context, bucket, object, version_id, object_info)
            .await
        {
            Ok(queued) => Ok(queued),
            Err(err) => {
                debug!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_ADMIN_OP,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    operation = "enqueue_heal_lifecycle_expiry",
                    bucket,
                    object,
                    version_id = ?version_id,
                    result = "failed",
                    error = %err,
                    "Heal storage lifecycle expiry check failed"
                );
                Ok(false)
            }
        }
    }

    async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_ADMIN_OP,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "list_buckets",
            state = "started",
            "Heal storage admin operation started"
        );

        match self.ecstore.list_bucket(&Default::default()).await {
            Ok(buckets) => Ok(buckets),
            Err(e) => {
                error!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_ADMIN_OP,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    operation = "list_buckets",
                    result = "failed",
                    error = %e,
                    "Heal storage admin operation failed"
                );
                Err(Error::other(e))
            }
        }
    }

    async fn object_exists(&self, bucket: &str, object: &str) -> Result<bool> {
        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_OBJECT_IO,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "object_exists",
            bucket,
            object,
            "Heal storage request started"
        );

        // Existence checks are best-effort for background heal scheduling, so avoid
        // acquiring an extra namespace read lock here.
        let opts = HealObjectOptions {
            no_lock: true,
            ..Default::default()
        };

        match self.ecstore.get_object_info(bucket, object, &opts).await {
            Ok(_) => Ok(true), // Object exists
            Err(e) => {
                if matches!(e, StorageError::ObjectNotFound(_, _)) {
                    debug!(
                        target: "rustfs::heal::storage",
                        event = EVENT_HEAL_STORAGE_OBJECT_IO,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_STORAGE,
                        operation = "object_exists",
                        bucket,
                        object,
                        result = "not_found",
                        "Heal storage object absence confirmed"
                    );
                    Ok(false)
                } else if is_transient_object_exists_error(&e) {
                    warn!(
                        target: "rustfs::heal::storage",
                        event = EVENT_HEAL_STORAGE_OBJECT_IO,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_STORAGE,
                        operation = "object_exists",
                        bucket,
                        object,
                        result = "transient_skip",
                        error = %e,
                        "Heal storage request skipped due to transient error"
                    );
                    Err(Error::transient_skip(format!(
                        "Skipped object existence check for {bucket}/{object}: {e}"
                    )))
                } else {
                    error!(
                        target: "rustfs::heal::storage",
                        event = EVENT_HEAL_STORAGE_OBJECT_IO,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_STORAGE,
                        operation = "object_exists",
                        bucket,
                        object,
                        result = "failed",
                        error = %e,
                        "Heal storage request failed"
                    );
                    Err(Error::other(e))
                }
            }
        }
    }

    async fn heal_object(
        &self,
        bucket: &str,
        object: &str,
        version_id: Option<&str>,
        opts: &HealOpts,
    ) -> Result<(HealResultItem, Option<Error>)> {
        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_REPAIR_OP,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "heal_object",
            bucket,
            object,
            version_id = ?version_id,
            scan_mode = %opts.scan_mode.as_str(),
            dry_run = opts.dry_run,
            state = "started",
            "Heal storage repair started"
        );

        let version_id_str = version_id.unwrap_or("");

        match self.ecstore.heal_object(bucket, object, version_id_str, opts).await {
            Ok((result, ecstore_error)) => {
                let error = ecstore_error.map(Error::Storage);
                debug!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_REPAIR_OP,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    operation = "heal_object",
                    bucket,
                    object,
                    version_id = ?version_id,
                    drives_after = result.after.drives.len(),
                    has_error = error.is_some(),
                    result = "ok",
                    "Heal storage object repair completed"
                );
                Ok((result, error))
            }
            Err(e) => {
                error!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_REPAIR_OP,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    operation = "heal_object",
                    bucket,
                    object,
                    version_id = ?version_id,
                    result = "failed",
                    error = %e,
                    "Heal storage repair failed"
                );
                Err(Error::Storage(e))
            }
        }
    }

    async fn heal_bucket(&self, bucket: &str, opts: &HealOpts) -> Result<HealResultItem> {
        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_REPAIR_OP,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "heal_bucket",
            bucket,
            dry_run = opts.dry_run,
            recursive = opts.recursive,
            state = "started",
            "Heal storage repair started"
        );

        match self.ecstore.heal_bucket(bucket, opts).await {
            Ok(result) => {
                debug!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_REPAIR_OP,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    operation = "heal_bucket",
                    bucket,
                    drives_after = result.after.drives.len(),
                    result = "ok",
                    "Heal storage bucket repair completed"
                );
                Ok(result)
            }
            Err(e) => {
                error!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_REPAIR_OP,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    operation = "heal_bucket",
                    bucket,
                    result = "failed",
                    error = %e,
                    "Heal storage repair failed"
                );
                Err(Error::Storage(e))
            }
        }
    }

    async fn heal_format(&self, dry_run: bool) -> Result<(HealResultItem, Option<Error>)> {
        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_REPAIR_OP,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "heal_format",
            dry_run,
            state = "started",
            "Heal storage repair started"
        );

        match self.ecstore.heal_format(dry_run).await {
            Ok((result, ecstore_error)) => {
                let error = ecstore_error.map(Error::Storage);
                debug!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_REPAIR_OP,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    operation = "heal_format",
                    drives_after = result.after.drives.len(),
                    has_error = error.is_some(),
                    result = "ok",
                    "Heal storage format repair completed"
                );
                Ok((result, error))
            }
            Err(e) => {
                error!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_REPAIR_OP,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    operation = "heal_format",
                    result = "failed",
                    error = %e,
                    "Heal storage repair failed"
                );
                Err(Error::Storage(e))
            }
        }
    }

    async fn heal_replacement_format(
        &self,
        dry_run: bool,
        pool_index: usize,
        set_index: usize,
        targets: &[String],
    ) -> Result<(HealResultItem, Option<Error>)> {
        self.ecstore
            .heal_replacement_format(dry_run, pool_index, set_index, targets)
            .await
            .map(|(result, error)| (result, error.map(Error::Storage)))
            .map_err(Error::Storage)
    }

    async fn replacement_targets_ready(&self, targets: &[String]) -> Result<bool> {
        Ok(super::replacement_readiness::auto_replacement_targets_ready(targets).await)
    }

    async fn replacement_targets_have_version(
        &self,
        bucket: &str,
        object: &str,
        version_id: Option<&str>,
        opts: &HealOpts,
        targets: &[String],
    ) -> Result<bool> {
        let pool_index = opts
            .pool
            .ok_or_else(|| Error::other("replacement target readback is missing pool scope"))?;
        let set_index = opts
            .set
            .ok_or_else(|| Error::other("replacement target readback is missing set scope"))?;
        self.ecstore
            .replacement_targets_have_version(bucket, object, version_id.unwrap_or(""), pool_index, set_index, targets)
            .await
            .map_err(Error::Storage)
    }

    async fn list_objects_for_heal_page(
        &self,
        bucket: &str,
        prefix: &str,
        continuation_token: Option<&str>,
        include_lifecycle_object_info: bool,
    ) -> Result<(Vec<HealListItem>, Option<String>, bool)> {
        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_ADMIN_OP,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "list_objects_for_heal_page",
            bucket,
            prefix,
            continuation_token = ?continuation_token,
            state = "started",
            "Heal storage admin operation started"
        );

        const MAX_KEYS: i32 = 1000;
        // Decode the opaque composite token into the (marker, version_marker)
        // pair that list_object_versions consumes. Malformed tokens restart the
        // listing from the beginning (decode_heal_token is total).
        let (marker, version_marker) = decode_heal_token(continuation_token.unwrap_or(""));

        // Enumerate EVERY version (not just the latest) so old versions and
        // delete-marker-latest objects are healed too.
        let list_info = match self
            .ecstore
            .clone()
            .list_object_versions(bucket, prefix, marker, version_marker, None, MAX_KEYS)
            .await
        {
            Ok(info) => info,
            Err(e) => {
                error!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_ADMIN_OP,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    operation = "list_objects_for_heal_page",
                    bucket,
                    prefix,
                    result = "failed",
                    error = %e,
                    "Heal storage admin operation failed"
                );
                return Err(Error::other(e));
            }
        };

        // Collect versions from this page. version_id is normalized to Option<String>
        // here at the single construction point: nil/absent UUID => None.
        let page_objects: Vec<HealListItem> = list_info
            .objects
            .into_iter()
            .map(|mut obj| {
                obj.version_id = obj.version_id.filter(|u| !u.is_nil());
                let version_id = obj.version_id.map(|u| u.to_string());
                let mod_time_unix_nanos = obj.mod_time.map(|mod_time| mod_time.unix_timestamp_nanos());
                let is_delete_marker = obj.delete_marker;
                if include_lifecycle_object_info {
                    HealListItem {
                        name: obj.name.clone(),
                        version_id,
                        mod_time_unix_nanos,
                        lifecycle_object_info: Some(obj),
                        is_delete_marker,
                    }
                } else {
                    HealListItem {
                        name: obj.name,
                        version_id,
                        mod_time_unix_nanos,
                        lifecycle_object_info: None,
                        is_delete_marker,
                    }
                }
            })
            .collect();
        let page_count = page_objects.len();

        let next_token = if list_info.is_truncated {
            Some(encode_heal_token(
                list_info.next_marker.as_deref(),
                list_info.next_version_idmarker.as_deref(),
            ))
        } else {
            None
        };

        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_ADMIN_OP,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "list_objects_for_heal_page",
            bucket,
            prefix,
            version_count = page_count,
            is_truncated = list_info.is_truncated,
            state = "page_loaded",
            "Heal storage version listing page loaded"
        );

        Ok((page_objects, next_token, list_info.is_truncated))
    }

    async fn list_versions_for_heal_page_disk_walk(
        &self,
        set_disk_id: &str,
        bucket: &str,
        prefix: &str,
        continuation_token: Option<&str>,
        include_lifecycle_object_info: bool,
    ) -> Result<(Vec<HealListItem>, Option<String>, bool)> {
        // Per-page bounds for the disk-walk union enumerator. Objects are atomic
        // (never split across pages), so version_budget only bounds how many
        // versions accumulate before the page is cut at the next object boundary.
        const BATCH_OBJECTS: usize = 1000;
        const VERSION_BUDGET: usize = 10_000;

        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_ADMIN_OP,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "list_versions_for_heal_page_disk_walk",
            set_disk_id,
            bucket,
            prefix,
            continuation_token = ?continuation_token,
            state = "started",
            "Heal storage disk-walk union enumeration started"
        );

        let (pool_idx, set_idx) = crate::heal::utils::parse_set_disk_id(set_disk_id)?;
        // Decode the dw1: cursor into the forward_to object key. Malformed/foreign
        // tokens restart the walk from the beginning (decode_disk_walk_token is total).
        let forward_to = decode_disk_walk_token(continuation_token.unwrap_or(""));

        let (versions, next_forward, is_truncated) = self
            .ecstore
            .heal_walk_versions_page(
                pool_idx,
                set_idx,
                bucket,
                prefix,
                forward_to.as_deref(),
                BATCH_OBJECTS,
                VERSION_BUDGET,
                include_lifecycle_object_info,
            )
            .await
            .map_err(|e| {
                error!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_ADMIN_OP,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    operation = "list_versions_for_heal_page_disk_walk",
                    set_disk_id,
                    bucket,
                    prefix,
                    result = "failed",
                    error = %e,
                    "Heal storage disk-walk union enumeration failed"
                );
                Error::other(e)
            })?;

        let page_objects: Vec<HealListItem> = versions
            .into_iter()
            .map(|v| HealListItem {
                name: v.name,
                version_id: v.version_id,
                mod_time_unix_nanos: v.mod_time_unix_nanos,
                lifecycle_object_info: v.lifecycle_object_info,
                is_delete_marker: v.is_delete_marker,
            })
            .collect();
        let page_count = page_objects.len();

        let next_token = if is_truncated {
            next_forward.map(|fw| encode_disk_walk_token(&fw))
        } else {
            None
        };

        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_ADMIN_OP,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "list_versions_for_heal_page_disk_walk",
            set_disk_id,
            bucket,
            prefix,
            version_count = page_count,
            is_truncated,
            state = "page_loaded",
            "Heal storage disk-walk union enumeration page loaded"
        );

        Ok((page_objects, next_token, is_truncated))
    }

    async fn get_disk_for_resume(&self, set_disk_id: &str) -> Result<DiskStore> {
        self.get_disk_for_resume_excluding(set_disk_id, &[]).await
    }

    async fn get_disk_for_resume_excluding(&self, set_disk_id: &str, excluded_targets: &[String]) -> Result<DiskStore> {
        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_ADMIN_OP,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "get_disk_for_resume",
            set_disk_id,
            state = "started",
            "Heal storage admin operation started"
        );

        // Parse set_disk_id to extract pool and set indices
        let (pool_idx, set_idx) = crate::heal::utils::parse_set_disk_id(set_disk_id)?;

        // Get the first available disk from the set
        let disks = StorageAdminApi::disk_set_inventory(self.ecstore.as_ref(), DiskSetSelector::new(pool_idx, set_idx))
            .await
            .map_err(|e| Error::TaskExecutionFailed {
                message: format!("Failed to get disks for pool {pool_idx} set {set_idx}: {e}"),
            })?;

        // The replacement target is unformatted before repair and must never
        // host the intent that authorizes its own formatting.
        for disk_store in disks.into_iter().flatten() {
            if !disk_store.endpoint().is_local {
                continue;
            }
            if excluded_targets.contains(&disk_store.endpoint().to_string()) {
                continue;
            }
            if !matches!(disk_store.get_disk_id().await, Ok(Some(id)) if !id.is_nil()) {
                continue;
            }
            debug!(
                target: "rustfs::heal::storage",
                event = EVENT_HEAL_STORAGE_ADMIN_OP,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_STORAGE,
                operation = "get_disk_for_resume",
                set_disk_id,
                result = "ok",
                disk = ?disk_store,
                "Heal storage resume disk resolved"
            );
            return Ok(disk_store);
        }

        Err(Error::TaskExecutionFailed {
            message: format!("No available disk found for set_disk_id: {set_disk_id}"),
        })
    }

    async fn get_replacement_resume_disk(
        &self,
        set_disk_id: &str,
        task_id: &str,
        excluded_targets: &[String],
    ) -> Result<ReplacementResumeDisk> {
        let (pool_idx, set_idx) = crate::heal::utils::parse_set_disk_id(set_disk_id)?;
        let disks = StorageAdminApi::disk_set_inventory(self.ecstore.as_ref(), DiskSetSelector::new(pool_idx, set_idx))
            .await
            .map_err(|e| Error::TaskExecutionFailed {
                message: format!("Failed to get disks for pool {pool_idx} set {set_idx}: {e}"),
            })?;
        let mut existing = None;
        for disk_store in disks.into_iter().flatten() {
            if !disk_store.endpoint().is_local || excluded_targets.contains(&disk_store.endpoint().to_string()) {
                continue;
            }
            if !matches!(disk_store.get_disk_id().await, Ok(Some(id)) if !id.is_nil()) {
                continue;
            }
            if super::resume::ResumeManager::has_replacement_intent(&disk_store, task_id).await
                && existing.replace(disk_store).is_some()
            {
                return Err(Error::TaskExecutionFailed {
                    message: format!("Replacement resume intent is duplicated for set_disk_id: {set_disk_id}"),
                });
            }
        }
        Ok(existing.map_or(ReplacementResumeDisk::Fresh, ReplacementResumeDisk::Existing))
    }

    async fn replacement_target_identities(&self, targets: &[String]) -> Result<Vec<ReplacementTargetIdentity>> {
        super::replacement_readiness::auto_replacement_target_identities(targets)
            .await
            .ok_or_else(|| Error::other("replacement target is not a stable mounted disk"))
    }
}

#[cfg(test)]
mod tests {
    use super::super::StorageError;
    use super::{
        decode_disk_walk_token, decode_heal_token, encode_disk_walk_token, encode_heal_token, is_transient_object_exists_error,
        is_transient_object_exists_message, next_heal_listing_token,
    };
    use base64::Engine as _;

    #[test]
    fn next_heal_listing_token_returns_none_for_complete_page() {
        assert_eq!(
            next_heal_listing_token("bucket", "prefix", None, false).expect("complete page should not fail"),
            None
        );
    }

    #[test]
    fn next_heal_listing_token_returns_token_for_truncated_page() {
        assert_eq!(
            next_heal_listing_token("bucket", "prefix", Some("token-1".to_string()), true)
                .expect("truncated page with token should continue"),
            Some("token-1".to_string())
        );
    }

    #[test]
    fn next_heal_listing_token_treats_truncated_page_without_token_as_end() {
        // A version listing can report the final page as truncated with no
        // continuation token; that must terminate the scan cleanly, not error.
        assert_eq!(
            next_heal_listing_token("bucket", "prefix", None, true).expect("truncated without token ends listing"),
            None
        );
    }

    #[test]
    fn test_heal_token_roundtrip() {
        let token = encode_heal_token(Some("obj/key"), Some("v-123"));
        assert!(token.starts_with("v1:"));
        assert_eq!(decode_heal_token(&token), (Some("obj/key".to_string()), Some("v-123".to_string())));

        // marker only (no version marker) round-trips.
        let token = encode_heal_token(Some("obj/key"), None);
        assert_eq!(decode_heal_token(&token), (Some("obj/key".to_string()), None));

        // (None, None) round-trips.
        let token = encode_heal_token(None, None);
        assert_eq!(decode_heal_token(&token), (None, None));
    }

    #[test]
    fn test_heal_token_malformed_resets_to_start() {
        // empty, wrong prefix, bad base64, and bad json all reset to (None, None).
        assert_eq!(decode_heal_token(""), (None, None));
        assert_eq!(decode_heal_token("no-prefix-here"), (None, None));
        assert_eq!(decode_heal_token("v1:!!!not-base64!!!"), (None, None));
        // valid base64 of non-JSON bytes.
        let bad_json = format!("v1:{}", base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"not json"));
        assert_eq!(decode_heal_token(&bad_json), (None, None));
        // a raw v2-style list_objects_v2 token (no "v1:" prefix) resets cleanly.
        assert_eq!(decode_heal_token("some-opaque-legacy-token"), (None, None));
    }

    #[test]
    fn test_heal_token_none_and_marker_only() {
        // A decoded payload must NEVER yield (None, Some(_)) because
        // list_object_versions returns NotImplemented for that pairing.
        // Craft a token whose JSON encodes (None, Some) directly and confirm coercion.
        let json = br#"{"m":null,"v":"orphan-version"}"#;
        let token = format!("v1:{}", base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json));
        assert_eq!(decode_heal_token(&token), (None, None), "version-only marker must coerce to (None, None)");
    }

    #[test]
    fn disk_walk_cursor_round_trip_and_foreign_token_restarts() {
        // Round-trip: a real object key survives encode -> decode.
        let token = encode_disk_walk_token("some/deep/object.bin");
        assert!(token.starts_with("dw1:"));
        assert_eq!(decode_disk_walk_token(&token), Some("some/deep/object.bin".to_string()));

        // Empty / garbage / missing-prefix all restart the walk (None).
        assert_eq!(decode_disk_walk_token(""), None);
        assert_eq!(decode_disk_walk_token("dw1:!!!not-base64!!!"), None);
        assert_eq!(decode_disk_walk_token("no-prefix-here"), None);

        // CROSS-DECODER ISOLATION (both directions): a dw1 token must not be
        // misread as a B5 (marker, version_marker) pair, and a v1 token must not
        // be misread as a disk-walk forward cursor.
        let dw = encode_disk_walk_token("obj/key");
        assert_eq!(
            decode_heal_token(&dw),
            (None, None),
            "a dw1: token must decode to (None, None) under the B5 decoder"
        );

        let v1 = encode_heal_token(Some("obj/key"), Some("v-123"));
        assert!(v1.starts_with("v1:"));
        assert_eq!(
            decode_disk_walk_token(&v1),
            None,
            "a v1: token must decode to None under the disk-walk decoder"
        );
    }

    #[test]
    fn transient_object_exists_message_matches_lock_quorum_failures() {
        assert!(is_transient_object_exists_message(
            "Failed to acquire read lock: ns_loc: read lock acquisition failed on bucket/object: Quorum not reached: required 2, achieved 0"
        ));
        assert!(is_transient_object_exists_message("deadline has elapsed"));
    }

    #[test]
    fn transient_object_exists_error_matches_quorum_variants() {
        assert!(is_transient_object_exists_error(&StorageError::ErasureReadQuorum));
        assert!(is_transient_object_exists_error(&StorageError::InsufficientReadQuorum(
            "bucket".to_string(),
            "object".to_string(),
        )));
    }

    #[test]
    fn transient_object_exists_error_does_not_treat_not_found_as_transient() {
        assert!(!is_transient_object_exists_error(&StorageError::ObjectNotFound(
            "bucket".to_string(),
            "object".to_string(),
        )));
    }
}
