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
use base64_simd::URL_SAFE_NO_PAD;
use rustfs_common::mrf_channel::MrfDeleteMarkerPurge;
use rustfs_heal_contracts::heal_channel::{DriveState, HealOpts, HealScanMode};
use rustfs_madmin::heal_commands::HealResultItem;
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, sync::Arc};
use tracing::{debug, error, warn};
use uuid::Uuid;

use super::outcome::{HealObjectDisposition, HealObjectIdentity, HealObjectKind, HealObjectReceipt};
use super::progress::stable_generation;
pub use super::replacement_execution::ReplacementExecution;
use super::storage_api::owner::{EcstoreHealLifecycleExpiryContext, ecstore_load_admin_data_usage_from_backend_cached};
use super::storage_api::storage::{
    BucketInfo, BucketOperations, DiskSetSelector, EcstoreHealObjectStorageResult, HealOperations as _, ListOperations as _,
    ObjectIO as _, ObjectOperations as _, StorageAdminApi,
};
use super::{
    DiskError, DiskStore, ECStore, HealDiskExt as _, StorageError, local_disk_map_read,
    resume::{ReplacementRecoveryCandidate, ReplacementTargetIdentity, merge_replacement_recovery_candidate},
};
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

#[derive(Debug, Default)]
pub struct HealStorageObjectResult {
    pub item: HealResultItem,
    pub error: Option<Error>,
    pub receipt: Option<HealObjectReceipt>,
}

fn incarnation_storage_error(bucket: &str, expected: Uuid, error: StorageError) -> Error {
    match error {
        StorageError::BucketNotFound(_) => Error::StaleBucketIncarnation {
            bucket: bucket.to_owned(),
            expected: Some(expected),
        },
        error => Error::Storage(error),
    }
}

impl From<(HealResultItem, Option<Error>)> for HealStorageObjectResult {
    fn from((item, error): (HealResultItem, Option<Error>)) -> Self {
        Self {
            item,
            error,
            receipt: None,
        }
    }
}

fn verified_object_receipt(
    bucket: &str,
    object: &str,
    version_id: Option<&str>,
    opts: &HealOpts,
    item: &HealResultItem,
    bucket_incarnation_id: Uuid,
) -> Option<HealObjectReceipt> {
    if opts.dry_run
        || (!item.integrity_verified && !item.repair_verified && !item.metadata_verified && !item.metadata_repair_verified)
    {
        return None;
    }
    let resolved_version = Uuid::from_bytes(item.resolved_version_id?);
    if let Some(requested) = version_id.filter(|version| !version.is_empty())
        && Uuid::parse_str(requested).ok()? != resolved_version
    {
        return None;
    }
    item.drives_reported()?;
    let drives_healed = item.drives_healed()?;
    if (item.repair_verified || item.metadata_repair_verified) && drives_healed == 0 {
        return None;
    }
    let ok_drive_state = DriveState::Ok.to_string();
    if !item.after.drives.iter().all(|drive| drive.state == ok_drive_state) {
        return None;
    }
    let receipt_version_id = if resolved_version.is_nil() {
        version_id.filter(|version| !version.is_empty()).map(ToOwned::to_owned)
    } else {
        Some(resolved_version.to_string())
    };
    Some(HealObjectReceipt {
        identity: HealObjectIdentity {
            kind: HealObjectKind::Object,
            bucket: bucket.to_string(),
            object: object.to_string(),
            version_id: receipt_version_id,
            bucket_incarnation_id: Some(bucket_incarnation_id),
            pool_index: opts.pool,
            set_index: opts.set,
        },
        disposition: if drives_healed > 0 && (item.integrity_verified || item.repair_verified || item.metadata_repair_verified) {
            HealObjectDisposition::Repaired
        } else if drives_healed == 0 && item.integrity_verified {
            HealObjectDisposition::VerifiedHealthy
        } else if drives_healed == 0 && item.metadata_verified {
            HealObjectDisposition::MetadataHealthy
        } else {
            return None;
        },
    })
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

fn replacement_resume_conflict_for_other_tasks(
    requested_task_id: &str,
    observed_other_tasks: &std::collections::HashSet<String>,
    set_disk_id: &str,
) -> Option<Error> {
    let mut task_ids = observed_other_tasks.iter().collect::<Vec<_>>();
    task_ids.sort_unstable();
    task_ids.first().map(|other_task_id| Error::ReplacementGenerationConflict {
        task_id: requested_task_id.to_string(),
        reason: format!("durable generation {other_task_id} already owns set {set_disk_id}"),
    })
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
    format!("{HEAL_TOKEN_PREFIX}{}", URL_SAFE_NO_PAD.encode_to_string(json))
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

    let bytes = match URL_SAFE_NO_PAD.decode_to_vec(encoded) {
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
    format!("{DISK_WALK_TOKEN_PREFIX}{}", URL_SAFE_NO_PAD.encode_to_string(next_forward.as_bytes()))
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

    let bytes = match URL_SAFE_NO_PAD.decode_to_vec(encoded) {
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
/// `latest_meta.deleted`. Every enumerated version has an exact selector:
/// nil/absent metadata UUIDs select the nil UUID, never an unspecified latest.
#[derive(Debug, Clone)]
pub struct HealListItem {
    /// object key
    pub name: String,
    /// Exact version id, including the nil UUID for the null slot.
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
    /// Delete one exact marker from every member of its original erasure set.
    async fn purge_delete_marker(
        &self,
        _bucket: &str,
        _object: &str,
        _version_id: &str,
        _purge: &MrfDeleteMarkerPurge,
        _opts: &HealOpts,
    ) -> Result<HealStorageObjectResult> {
        Err(Error::other("delete-marker purge is unsupported by this heal storage backend"))
    }

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

    /// Return the current bucket incarnation for exact MRF durable proof
    /// matching. Alternate backends that cannot expose this must return
    /// `None`, leaving replay anchors retained instead of acknowledged with an
    /// incomplete identity.
    async fn mrf_bucket_incarnation_id(&self, _bucket: &str) -> Result<Option<Uuid>> {
        Ok(None)
    }

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

    /// Stable bucket incarnation observed before an object heal starts.
    async fn bucket_incarnation_id(&self, _bucket: &str) -> Result<Option<Uuid>> {
        Ok(None)
    }

    /// Admission must use authoritative metadata, not an outcome cache.
    async fn admit_bucket_incarnation(&self, bucket: &str) -> Result<Uuid> {
        self.bucket_incarnation_id(bucket)
            .await?
            .filter(|id| !id.is_nil())
            .ok_or_else(|| Error::StaleBucketIncarnation {
                bucket: bucket.to_owned(),
                expected: None,
            })
    }

    async fn validate_bucket_incarnation(&self, bucket: &str, expected: Option<Uuid>) -> Result<()> {
        let stale = || Error::StaleBucketIncarnation {
            bucket: bucket.to_owned(),
            expected,
        };
        let expected = expected.filter(|id| !id.is_nil()).ok_or_else(stale)?;
        match self.admit_bucket_incarnation(bucket).await {
            Ok(current) if current == expected => Ok(()),
            Ok(_) | Err(Error::StaleBucketIncarnation { .. }) => Err(stale()),
            Err(error) => Err(error),
        }
    }

    /// Implementations must retain the bucket lifecycle fence through storage mutation.
    async fn heal_bucket_at_incarnation(&self, _bucket: &str, _expected: Uuid, _opts: &HealOpts) -> Result<HealResultItem> {
        Err(Error::other("storage does not support incarnation-bound bucket healing"))
    }

    async fn heal_object_at_incarnation(
        &self,
        _bucket: &str,
        _object: &str,
        _version_id: Option<&str>,
        _expected: Uuid,
        _opts: &HealOpts,
    ) -> Result<HealStorageObjectResult> {
        Err(Error::other("storage does not support incarnation-bound object healing"))
    }

    /// Durable MRF repair may request an authoritative unversioned absence proof.
    async fn heal_mrf_object_at_incarnation(
        &self,
        bucket: &str,
        object: &str,
        version_id: Option<&str>,
        expected: Uuid,
        opts: &HealOpts,
    ) -> Result<HealStorageObjectResult> {
        self.heal_object_at_incarnation(bucket, object, version_id, expected, opts)
            .await
    }

    /// Heal object using ecstore
    async fn heal_object(
        &self,
        bucket: &str,
        object: &str,
        version_id: Option<&str>,
        opts: &HealOpts,
    ) -> Result<(HealResultItem, Option<Error>)>;

    async fn heal_object_with_receipt(
        &self,
        bucket: &str,
        object: &str,
        version_id: Option<&str>,
        opts: &HealOpts,
    ) -> Result<HealStorageObjectResult> {
        self.heal_object(bucket, object, version_id, opts).await.map(Into::into)
    }

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

    /// Whether the explicitly scoped replacement set owns its pool's metadata.
    /// Backends must use authoritative placement, not treat missing shards as
    /// evidence that metadata is unnecessary. Unknown placement fails closed.
    fn replacement_pool_metadata_required(&self, _opts: &HealOpts) -> Result<bool> {
        Err(Error::other("replacement pool metadata placement is unsupported"))
    }

    /// Heal and physically verify bucket configuration on its replacement targets.
    async fn heal_replacement_bucket_metadata(&self, _bucket: &str, _opts: &HealOpts, _targets: &[String]) -> Result<()> {
        Err(Error::Storage(StorageError::PreconditionFailed))
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

    /// Return the live erasure sets selected by this heal request.
    ///
    /// Recursive admin heals use these scopes with the cross-disk union walk so
    /// objects surviving on only one returning disk are still discovered. The
    /// `None` default preserves the read-quorum listing for alternate backends;
    /// `Some(Vec::new())` means the selected topology currently has no live set.
    async fn heal_erasure_set_scopes(&self, _opts: &HealOpts) -> Result<Option<Vec<(usize, usize)>>> {
        Ok(None)
    }

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

    /// Return every formatted disk that may durably host or expose a
    /// replacement intent. Implementations should order local disks first so
    /// discovery remains deterministic and preserves the local preference.
    async fn replacement_intent_disks(&self, _set_disk_id: &str) -> Result<Vec<DiskStore>> {
        Ok(Vec::new())
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

    /// Check whether a replacement generation may consume another retry.
    /// Implementations must verify the mounted incarnation and that every
    /// target is formatted in the expected erasure-set slot.  The default is
    /// deliberately permissive for alternate/mock backends that do not expose
    /// those disk-level probes.
    async fn replacement_targets_ready_for_retry(
        &self,
        _set_disk_id: &str,
        _targets: &[String],
        _expected_identities: &[ReplacementTargetIdentity],
    ) -> Result<bool> {
        Ok(true)
    }

    async fn replacement_execution(&self, _targets: &[String]) -> Result<Arc<ReplacementExecution>> {
        Err(Error::other("replacement execution lease acquisition is unsupported"))
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

    async fn resume_disk_inventory(&self, set_disk_id: &str) -> Result<Vec<DiskStore>> {
        let (pool_idx, set_idx) = crate::heal::utils::parse_set_disk_id(set_disk_id)?;
        let disks = StorageAdminApi::disk_set_inventory(self.ecstore.as_ref(), DiskSetSelector::new(pool_idx, set_idx))
            .await
            .map_err(|e| Error::TaskExecutionFailed {
                message: format!("Failed to get disks for pool {pool_idx} set {set_idx}: {e}"),
            })?;
        Ok(disks.into_iter().flatten().collect())
    }

    async fn object_result_with_receipt(
        &self,
        bucket: &str,
        object: &str,
        version_id: Option<&str>,
        opts: &HealOpts,
        result: EcstoreHealObjectStorageResult,
        expected: Option<Uuid>,
    ) -> HealStorageObjectResult {
        let item = result.item;
        let error = result.error.map(Error::Storage);
        let receipt = if let Some(proof) = result.absence {
            if error.is_none()
                && !opts.dry_run
                && proof.bucket == bucket
                && proof.object == object
                && proof.version_id == version_id.unwrap_or("")
                && proof.pool_index == opts.pool
                && proof.set_index == opts.set
                && !proof.bucket_incarnation_id.is_nil()
                && expected.is_none_or(|expected| expected == proof.bucket_incarnation_id)
                && !proof.locations.is_empty()
                && proof.locations.iter().all(|(pool, set)| {
                    opts.pool.is_none_or(|expected| expected == *pool) && opts.set.is_none_or(|expected| expected == *set)
                })
            {
                Some(HealObjectReceipt {
                    identity: HealObjectIdentity {
                        kind: HealObjectKind::Object,
                        bucket: proof.bucket,
                        object: proof.object,
                        version_id: version_id.map(ToOwned::to_owned),
                        bucket_incarnation_id: Some(proof.bucket_incarnation_id),
                        pool_index: proof.pool_index,
                        set_index: proof.set_index,
                    },
                    // A committed cleanup repaired the stale replica. A replay
                    // observing an already absent version made no new repair.
                    disposition: if proof.removed {
                        HealObjectDisposition::Repaired
                    } else {
                        HealObjectDisposition::AuthoritativelyAbsent
                    },
                })
            } else {
                None
            }
        } else if error.is_none()
            && !opts.dry_run
            && (item.integrity_verified || item.repair_verified || item.metadata_verified || item.metadata_repair_verified)
        {
            let bucket_incarnation_id = match expected {
                Some(expected) => Some(expected),
                None => self.ecstore.bucket_incarnation_id(bucket).await.ok(),
            };
            bucket_incarnation_id
                .and_then(|incarnation| verified_object_receipt(bucket, object, version_id, opts, &item, incarnation))
        } else {
            None
        };
        HealStorageObjectResult { item, error, receipt }
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
    async fn purge_delete_marker(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        purge: &MrfDeleteMarkerPurge,
        opts: &HealOpts,
    ) -> Result<HealStorageObjectResult> {
        let removed = self
            .ecstore
            .purge_delete_marker_with_proof(bucket, object, version_id, purge, opts)
            .await
            .map_err(Error::Storage)?;
        Ok(HealStorageObjectResult {
            item: HealResultItem::default(),
            error: None,
            receipt: Some(HealObjectReceipt {
                identity: HealObjectIdentity {
                    kind: HealObjectKind::DeleteMarkerPurge,
                    bucket: bucket.to_owned(),
                    object: object.to_owned(),
                    version_id: Some(version_id.to_owned()),
                    bucket_incarnation_id: Some(purge.bucket_incarnation_id),
                    pool_index: opts.pool,
                    set_index: opts.set,
                },
                disposition: if removed {
                    HealObjectDisposition::Repaired
                } else {
                    HealObjectDisposition::AuthoritativelyAbsent
                },
            }),
        })
    }

    async fn admit_bucket_incarnation(&self, bucket: &str) -> Result<Uuid> {
        match self.ecstore.bucket_incarnation_id_from_disk(bucket).await {
            Ok(id) if !id.is_nil() => Ok(id),
            Ok(_) | Err(StorageError::BucketNotFound(_)) => Err(Error::StaleBucketIncarnation {
                bucket: bucket.to_owned(),
                expected: None,
            }),
            Err(error) => Err(Error::Storage(error)),
        }
    }

    async fn heal_bucket_at_incarnation(&self, bucket: &str, expected: Uuid, opts: &HealOpts) -> Result<HealResultItem> {
        self.ecstore
            .heal_bucket_at_incarnation(bucket, expected, opts)
            .await
            .map_err(|error| incarnation_storage_error(bucket, expected, error))
    }

    async fn heal_object_at_incarnation(
        &self,
        bucket: &str,
        object: &str,
        version_id: Option<&str>,
        expected: Uuid,
        opts: &HealOpts,
    ) -> Result<HealStorageObjectResult> {
        let result = self
            .ecstore
            .heal_object_at_incarnation(bucket, object, version_id.unwrap_or_default(), expected, opts)
            .await
            .map_err(|error| incarnation_storage_error(bucket, expected, error))?;
        Ok(self
            .object_result_with_receipt(bucket, object, version_id, opts, result, Some(expected))
            .await)
    }

    async fn heal_mrf_object_at_incarnation(
        &self,
        bucket: &str,
        object: &str,
        version_id: Option<&str>,
        expected: Uuid,
        opts: &HealOpts,
    ) -> Result<HealStorageObjectResult> {
        let result = self
            .ecstore
            .heal_mrf_object_at_incarnation(bucket, object, version_id.unwrap_or_default(), expected, opts)
            .await
            .map_err(|error| incarnation_storage_error(bucket, expected, error))?;
        Ok(self
            .object_result_with_receipt(bucket, object, version_id, opts, result, Some(expected))
            .await)
    }

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
            read_repair: false,
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

    async fn mrf_bucket_incarnation_id(&self, bucket: &str) -> Result<Option<Uuid>> {
        self.ecstore
            .bucket_incarnation_id(bucket)
            .await
            .map(Some)
            .map_err(Error::Storage)
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

    async fn bucket_incarnation_id(&self, bucket: &str) -> Result<Option<Uuid>> {
        self.ecstore
            .bucket_incarnation_id(bucket)
            .await
            .map(Some)
            .map_err(Error::Storage)
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

    async fn heal_object_with_receipt(
        &self,
        bucket: &str,
        object: &str,
        version_id: Option<&str>,
        opts: &HealOpts,
    ) -> Result<HealStorageObjectResult> {
        let result = self
            .ecstore
            .heal_object_with_proof(bucket, object, version_id.unwrap_or(""), opts)
            .await
            .map_err(Error::Storage)?;
        Ok(self
            .object_result_with_receipt(bucket, object, version_id, opts, result, None)
            .await)
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

    fn replacement_pool_metadata_required(&self, opts: &HealOpts) -> Result<bool> {
        let pool_index = opts
            .pool
            .ok_or_else(|| Error::other("replacement pool metadata placement is missing pool scope"))?;
        let set_index = opts
            .set
            .ok_or_else(|| Error::other("replacement pool metadata placement is missing set scope"))?;
        self.ecstore
            .replacement_pool_metadata_required(pool_index, set_index)
            .map_err(Error::Storage)
    }

    async fn heal_replacement_bucket_metadata(&self, bucket: &str, opts: &HealOpts, targets: &[String]) -> Result<()> {
        self.ecstore
            .heal_replacement_bucket_metadata(bucket, opts, targets)
            .await
            .map_err(Error::Storage)
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

        // Listing has already selected a concrete version. Preserve the null
        // slot's identity even when a newer UUID has become latest.
        let page_objects: Vec<HealListItem> = list_info
            .objects
            .into_iter()
            .map(|mut obj| {
                let version_id = Some(obj.version_id.unwrap_or_default().to_string());
                obj.version_id = obj.version_id.filter(|u| !u.is_nil());
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

    async fn heal_erasure_set_scopes(&self, opts: &HealOpts) -> Result<Option<Vec<(usize, usize)>>> {
        self.ecstore
            .heal_erasure_set_scopes(opts)
            .await
            .map(Some)
            .map_err(Error::Storage)
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
        for disk in self.resume_disk_inventory(set_disk_id).await? {
            if disk.endpoint().is_local && matches!(disk.get_disk_id().await, Ok(Some(id)) if !id.is_nil()) {
                return Ok(disk);
            }
        }
        Err(Error::TaskExecutionFailed {
            message: format!("No available disk found for set_disk_id: {set_disk_id}"),
        })
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

        // The replacement target is unformatted before repair and must never
        // host the intent that authorizes its own formatting.
        let disks = self.resume_disk_inventory(set_disk_id).await?;
        for prefer_local in [true, false] {
            for disk_store in &disks {
                if disk_store.endpoint().is_local != prefer_local
                    || excluded_targets.contains(&disk_store.endpoint().to_string())
                    || !matches!(disk_store.get_disk_id().await, Ok(Some(id)) if !id.is_nil())
                {
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
                return Ok(disk_store.clone());
            }
        }

        Err(Error::TaskExecutionFailed {
            message: format!("No available disk found for set_disk_id: {set_disk_id}"),
        })
    }

    async fn replacement_intent_disks(&self, set_disk_id: &str) -> Result<Vec<DiskStore>> {
        let disks = self.resume_disk_inventory(set_disk_id).await?;
        let mut formatted = Vec::with_capacity(disks.len());
        for prefer_local in [true, false] {
            for disk in &disks {
                if disk.endpoint().is_local != prefer_local {
                    continue;
                }
                match disk.get_disk_id().await {
                    Ok(Some(id)) if !id.is_nil() => formatted.push(disk.clone()),
                    Ok(_) | Err(DiskError::UnformattedDisk) => {}
                    Err(error) => {
                        return Err(Error::TaskExecutionFailed {
                            message: format!(
                                "Failed to inspect replacement intent disk {} for set_disk_id {set_disk_id}: {error}",
                                disk.endpoint()
                            ),
                        });
                    }
                }
            }
        }
        Ok(formatted)
    }

    async fn get_replacement_resume_disk(
        &self,
        set_disk_id: &str,
        task_id: &str,
        excluded_targets: &[String],
    ) -> Result<ReplacementResumeDisk> {
        let disks = self.resume_disk_inventory(set_disk_id).await?;
        let mut selected = None;
        let mut observed_other_tasks = HashSet::new();
        for disk_store in &disks {
            if excluded_targets.contains(&disk_store.endpoint().to_string())
                || !matches!(disk_store.get_disk_id().await, Ok(Some(id)) if !id.is_nil())
            {
                continue;
            }
            let mut replacement_tasks = super::resume::ResumeUtils::get_replacement_intent_tasks(disk_store).await?;
            if !replacement_tasks.iter().any(|candidate| candidate == task_id)
                && super::resume::ResumeManager::has_replacement_intent(disk_store, task_id).await
            {
                replacement_tasks.push(task_id.to_string());
            }
            if replacement_tasks.is_empty() {
                continue;
            }
            for candidate_task_id in replacement_tasks {
                // A request with a fresh UUID must not silently create a new
                // generation while another durable intent already owns this
                // set. Inspect one copy of each other task and let the
                // caller reconcile it through the normal recovery scanner.
                if candidate_task_id != task_id && !observed_other_tasks.insert(candidate_task_id.clone()) {
                    continue;
                }
                let manager =
                    super::resume::ResumeManager::load_replacement_intent(disk_store.clone(), &candidate_task_id).await?;
                let state = manager.get_state().await;
                if state.set_disk_id != set_disk_id {
                    return Err(Error::ReplacementGenerationConflict {
                        task_id: candidate_task_id,
                        reason: format!("replacement intent belongs to {}, not {set_disk_id}", state.set_disk_id),
                    });
                }
                if candidate_task_id != task_id {
                    continue;
                }
                let candidate =
                    ReplacementRecoveryCandidate::new(state, disk_store.endpoint().to_string(), disk_store.endpoint().is_local)?;
                merge_replacement_recovery_candidate(&mut selected, candidate)?;
            }
        }

        let Some(candidate) = selected else {
            if let Some(error) = replacement_resume_conflict_for_other_tasks(task_id, &observed_other_tasks, set_disk_id) {
                return Err(error);
            }
            return Ok(ReplacementResumeDisk::Fresh);
        };
        let disk = disks
            .iter()
            .find(|disk| disk.endpoint().to_string() == candidate.anchor)
            .ok_or_else(|| Error::TaskExecutionFailed {
                message: format!("Selected replacement resume anchor disappeared for set_disk_id: {set_disk_id}"),
            })?;
        Ok(ReplacementResumeDisk::Existing(disk.clone()))
    }

    async fn replacement_target_identities(&self, targets: &[String]) -> Result<Vec<ReplacementTargetIdentity>> {
        super::replacement_readiness::auto_replacement_target_identities(targets)
            .await
            .ok_or_else(|| Error::ReplacementTargetNotReady("replacement target is not a stable mounted disk".to_string()))
    }

    async fn replacement_targets_ready_for_retry(
        &self,
        set_disk_id: &str,
        targets: &[String],
        expected_identities: &[ReplacementTargetIdentity],
    ) -> Result<bool> {
        let identities = match self.replacement_target_identities(targets).await {
            Ok(identities) => identities,
            Err(_) => return Ok(false),
        };
        if identities != expected_identities {
            return Ok(false);
        }

        let local_disks = local_disk_map_read()
            .await
            .values()
            .flatten()
            .filter(|disk| disk.endpoint().is_local)
            .cloned()
            .collect::<Vec<_>>();
        for target in targets {
            let Some(disk) = local_disks.iter().find(|disk| disk.endpoint().to_string() == *target) else {
                return Ok(false);
            };
            let endpoint = disk.endpoint();
            if crate::heal::utils::format_set_disk_id_from_i32(endpoint.pool_idx, endpoint.set_idx).as_deref()
                != Some(set_disk_id)
            {
                return Ok(false);
            }
            if !matches!(disk.get_disk_id().await, Ok(Some(id)) if !id.is_nil()) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn replacement_execution(&self, targets: &[String]) -> Result<Arc<ReplacementExecution>> {
        ReplacementExecution::acquire(targets).await
    }
}

#[cfg(test)]
mod tests {
    use super::super::StorageError;
    use super::{
        decode_disk_walk_token, decode_heal_token, encode_disk_walk_token, encode_heal_token, is_transient_object_exists_error,
        is_transient_object_exists_message, next_heal_listing_token, replacement_resume_conflict_for_other_tasks,
    };
    use std::collections::HashSet;

    #[test]
    fn object_receipt_requires_integrity_and_resolved_version_evidence() {
        use super::{HealObjectDisposition, HealOpts, HealResultItem, Uuid, verified_object_receipt};
        use rustfs_madmin::heal_commands::HealDriveInfo;
        let incarnation = Uuid::new_v4();
        let null = Uuid::nil().to_string();
        let latest = Uuid::new_v4();
        let latest_version = latest.to_string();
        let options = HealOpts::default();
        let mut item = HealResultItem {
            integrity_verified: true,
            version_id: null.clone(),
            resolved_version_id: Some(*latest.as_bytes()),
            ..Default::default()
        };
        let healthy = HealDriveInfo {
            state: "ok".to_string(),
            ..Default::default()
        };
        item.before.drives.push(healthy.clone());
        item.after.drives.push(healthy);
        assert!(
            verified_object_receipt("bucket", "object", Some(&null), &options, &item, incarnation).is_none(),
            "echoing null cannot certify a different resolved version"
        );
        let receipt = verified_object_receipt("bucket", "object", None, &options, &item, incarnation)
            .expect("an omitted selector still means latest");
        assert_eq!(receipt.identity.version_id.as_deref(), Some(latest_version.as_str()));
        let receipt = verified_object_receipt("bucket", "object", Some(""), &options, &item, incarnation)
            .expect("an empty selector still means latest");
        assert_eq!(receipt.identity.version_id.as_deref(), Some(latest_version.as_str()));
        assert!(
            verified_object_receipt("bucket", "object", Some("null"), &options, &item, incarnation).is_none(),
            "the internal boundary requires a UUID, not an S3 spelling"
        );
        let requested_latest = latest_version.to_uppercase();
        let receipt = verified_object_receipt("bucket", "object", Some(&requested_latest), &options, &item, incarnation)
            .expect("the exact UUID selector should be certifiable");
        assert_eq!(receipt.identity.version_id.as_deref(), Some(latest_version.as_str()));

        item.resolved_version_id = Some([0; 16]);
        let receipt = verified_object_receipt("bucket", "object", None, &options, &item, incarnation)
            .expect("an omitted selector should preserve unversioned identity");
        assert_eq!(receipt.identity.version_id, None);
        let receipt = verified_object_receipt("bucket", "object", Some(""), &options, &item, incarnation)
            .expect("an empty selector should preserve unversioned identity");
        assert_eq!(receipt.identity.version_id, None);
        let receipt = verified_object_receipt("bucket", "object", Some(&null), &options, &item, incarnation)
            .expect("the exact healthy null version should be certifiable");
        assert_eq!(receipt.identity.version_id.as_deref(), Some(null.as_str()));
        assert_eq!(receipt.disposition, HealObjectDisposition::VerifiedHealthy);
        item.before.drives[0].state = "missing".to_string();
        assert_eq!(
            verified_object_receipt("bucket", "object", Some(&null), &options, &item, incarnation)
                .expect("the restored null version should be certifiable")
                .disposition,
            HealObjectDisposition::Repaired
        );
        item.integrity_verified = false;
        assert!(
            verified_object_receipt("bucket", "object", Some(&null), &options, &item, incarnation).is_none(),
            "the exact version cannot certify unverified shard integrity"
        );
        assert!(verified_object_receipt("bucket", "object", None, &options, &item, incarnation).is_none());
        item.repair_verified = true;
        item.resolved_version_id = Some([0; 16]);
        assert_eq!(
            verified_object_receipt("bucket", "object", Some(&null), &options, &item, incarnation)
                .expect("a protected repaired shard should carry a repair receipt")
                .disposition,
            HealObjectDisposition::Repaired
        );
        item.before.drives[0].state = "ok".to_string();
        assert!(
            verified_object_receipt("bucket", "object", Some(&null), &options, &item, incarnation).is_none(),
            "repair proof without a repaired drive must remain unresolved"
        );
        item.repair_verified = false;
        item.integrity_verified = true;
        item.resolved_version_id = None;
        assert!(
            verified_object_receipt("bucket", "object", Some(&null), &options, &item, incarnation).is_none(),
            "legacy results cannot prove the selected version"
        );
        assert!(verified_object_receipt("bucket", "object", None, &options, &item, incarnation).is_none());
    }

    #[test]
    fn metadata_health_and_marker_repair_have_distinct_receipts() {
        use super::{HealObjectDisposition, HealOpts, HealResultItem, Uuid, verified_object_receipt};
        use rustfs_madmin::heal_commands::HealDriveInfo;

        let incarnation = Uuid::new_v4();
        let version = Uuid::new_v4().to_string();
        let options = HealOpts::default();
        let healthy = HealDriveInfo {
            state: "ok".to_string(),
            ..Default::default()
        };
        let mut item = HealResultItem {
            metadata_verified: true,
            version_id: version.clone(),
            resolved_version_id: Some(*Uuid::parse_str(&version).expect("version UUID").as_bytes()),
            ..Default::default()
        };
        item.before.drives.push(healthy.clone());
        item.after.drives.push(healthy);

        let receipt = verified_object_receipt("bucket", "object", Some(&version), &options, &item, incarnation)
            .expect("metadata quorum should certify metadata health");
        assert_eq!(receipt.disposition, HealObjectDisposition::MetadataHealthy);

        item.integrity_verified = true;
        let receipt = verified_object_receipt("bucket", "object", Some(&version), &options, &item, incarnation)
            .expect("stronger integrity proof should remain available");
        assert_eq!(receipt.disposition, HealObjectDisposition::VerifiedHealthy);

        item.integrity_verified = false;
        item.metadata_verified = false;
        item.metadata_repair_verified = true;
        item.before.drives[0].state = "missing".to_string();
        let receipt = verified_object_receipt("bucket", "object", Some(&version), &options, &item, incarnation)
            .expect("a committed metadata repair should certify the marker/version");
        assert_eq!(receipt.disposition, HealObjectDisposition::Repaired);

        item.before.drives[0].state = "ok".to_string();
        assert!(
            verified_object_receipt("bucket", "object", Some(&version), &options, &item, incarnation).is_none(),
            "repair proof without a repaired drive must fail closed"
        );
    }

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
        let bad_json = format!("v1:{}", base64_simd::URL_SAFE_NO_PAD.encode_to_string(b"not json"));
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
        let token = format!("v1:{}", base64_simd::URL_SAFE_NO_PAD.encode_to_string(json));
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

    #[test]
    fn fresh_replacement_cannot_ignore_another_durable_generation() {
        let other = "00000000-0000-4000-8000-000000000001".to_string();
        let observed = HashSet::from([other]);
        let error =
            replacement_resume_conflict_for_other_tasks("00000000-0000-4000-8000-000000000002", &observed, "pool_0_set_0")
                .expect("an unmatched durable owner must produce a conflict");
        assert!(matches!(error, crate::Error::ReplacementGenerationConflict { .. }));
        assert!(replacement_resume_conflict_for_other_tasks("task", &HashSet::new(), "pool_0_set_0").is_none());
    }
}
