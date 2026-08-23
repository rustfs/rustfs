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

use super::*;
use crate::storage_api_contracts::{
    list::VersionMarker,
    object::{
        HTTPPreconditions, ObjectLockRetentionOptions, ObjectPreconditionError, ObjectPreconditionPart, ObjectPreconditionState,
    },
};

#[derive(Clone)]
pub struct NamespaceLockFence {
    signals: Arc<Vec<Arc<rustfs_lock::distributed_lock::LockLostSignal>>>,
    #[cfg(test)]
    forced_lost: Arc<Vec<Arc<std::sync::atomic::AtomicBool>>>,
}

impl Debug for NamespaceLockFence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NamespaceLockFence")
            .field("signal_count", &self.signals.len())
            .finish()
    }
}

impl NamespaceLockFence {
    fn new() -> Self {
        Self {
            signals: Arc::default(),
            #[cfg(test)]
            forced_lost: Arc::new(vec![Arc::new(std::sync::atomic::AtomicBool::new(false))]),
        }
    }

    pub(crate) fn is_lock_lost(&self) -> bool {
        #[cfg(test)]
        if self
            .forced_lost
            .iter()
            .any(|lost| lost.load(std::sync::atomic::Ordering::Acquire))
        {
            return true;
        }
        self.signals.iter().any(|signal| signal.is_lost())
    }

    pub(crate) fn add_signal(&mut self, signal: Arc<rustfs_lock::distributed_lock::LockLostSignal>) {
        Arc::make_mut(&mut self.signals).push(signal);
    }

    fn extend(&mut self, other: &Self) {
        if !Arc::ptr_eq(&self.signals, &other.signals) {
            Arc::make_mut(&mut self.signals).extend(other.signals.iter().cloned());
        }
        #[cfg(test)]
        if !Arc::ptr_eq(&self.forced_lost, &other.forced_lost) {
            Arc::make_mut(&mut self.forced_lost).extend(other.forced_lost.iter().cloned());
        }
    }

    #[cfg(test)]
    pub(crate) fn lost_for_test() -> Self {
        let fence = Self::new();
        fence.forced_lost[0].store(true, std::sync::atomic::Ordering::Release);
        fence
    }

    #[cfg(test)]
    pub(crate) fn loss_handle_for_test() -> (Self, Arc<std::sync::atomic::AtomicBool>) {
        let fence = Self::new();
        (fence.clone(), Arc::clone(&fence.forced_lost[0]))
    }
}

#[derive(Debug)]
pub struct ObjectLockConfigSnapshot {
    store_id: Option<Uuid>,
    bucket: Option<String>,
    bucket_incarnation_id: Option<Uuid>,
    config_revision: Option<OffsetDateTime>,
    state: crate::bucket::metadata_sys::ObjectLockConfigState,
    lifecycle_fence: NamespaceLockFence,
    _lifecycle_guard: Option<rustfs_lock::NamespaceLockGuard>,
    metadata_transaction_guard: Option<rustfs_lock::NamespaceLockGuard>,
}

impl ObjectLockConfigSnapshot {
    pub(crate) fn new(state: crate::bucket::metadata_sys::ObjectLockConfigState) -> Self {
        Self {
            store_id: None,
            bucket: None,
            bucket_incarnation_id: None,
            config_revision: None,
            state,
            lifecycle_fence: NamespaceLockFence::new(),
            _lifecycle_guard: None,
            metadata_transaction_guard: None,
        }
    }

    pub(crate) fn for_store_bucket(
        store_id: Uuid,
        bucket: &str,
        bucket_incarnation_id: Uuid,
        config_revision: OffsetDateTime,
        state: crate::bucket::metadata_sys::ObjectLockConfigState,
    ) -> Self {
        Self {
            store_id: Some(store_id),
            bucket: Some(bucket.to_string()),
            bucket_incarnation_id: Some(bucket_incarnation_id),
            config_revision: Some(config_revision),
            state,
            lifecycle_fence: NamespaceLockFence::new(),
            _lifecycle_guard: None,
            metadata_transaction_guard: None,
        }
    }

    pub(crate) fn for_guarded_store_bucket(
        store_id: Uuid,
        bucket: &str,
        bucket_incarnation_id: Uuid,
        config_revision: OffsetDateTime,
        state: crate::bucket::metadata_sys::ObjectLockConfigState,
        lifecycle_guard: rustfs_lock::NamespaceLockGuard,
        metadata_transaction_guard: rustfs_lock::NamespaceLockGuard,
    ) -> Self {
        let mut lifecycle_fence = NamespaceLockFence::new();
        if let Some(signal) = lifecycle_guard.lock_lost_signal() {
            lifecycle_fence.add_signal(signal);
        }
        Self {
            store_id: Some(store_id),
            bucket: Some(bucket.to_string()),
            bucket_incarnation_id: Some(bucket_incarnation_id),
            config_revision: Some(config_revision),
            state,
            lifecycle_fence,
            _lifecycle_guard: Some(lifecycle_guard),
            metadata_transaction_guard: Some(metadata_transaction_guard),
        }
    }

    pub(crate) fn for_store_bucket_under_lifecycle_fence(
        store_id: Uuid,
        bucket: &str,
        bucket_incarnation_id: Uuid,
        config_revision: OffsetDateTime,
        state: crate::bucket::metadata_sys::ObjectLockConfigState,
        lifecycle_fence: NamespaceLockFence,
        metadata_transaction_guard: rustfs_lock::NamespaceLockGuard,
    ) -> Self {
        Self {
            store_id: Some(store_id),
            bucket: Some(bucket.to_string()),
            bucket_incarnation_id: Some(bucket_incarnation_id),
            config_revision: Some(config_revision),
            state,
            lifecycle_fence,
            _lifecycle_guard: None,
            metadata_transaction_guard: Some(metadata_transaction_guard),
        }
    }

    #[allow(dead_code, reason = "snapshot-scope predicate asserted by this file's tests (backlog#1823)")]
    pub(crate) fn is_for_store_bucket(
        &self,
        store_id: Uuid,
        bucket: &str,
        bucket_incarnation_id: Uuid,
        config_revision: OffsetDateTime,
    ) -> bool {
        self.store_id == Some(store_id)
            && self.bucket.as_deref() == Some(bucket)
            && self.bucket_incarnation_id == Some(bucket_incarnation_id)
            && self.config_revision == Some(config_revision)
    }

    pub fn state(&self) -> &crate::bucket::metadata_sys::ObjectLockConfigState {
        &self.state
    }

    pub(crate) fn is_valid_for_destructive_put(&self, store_id: Uuid, bucket: &str, bucket_incarnation_id: Uuid) -> bool {
        self.store_id == Some(store_id)
            && self.bucket.as_deref() == Some(bucket)
            && self.bucket_incarnation_id == Some(bucket_incarnation_id)
            && self.config_revision.is_some()
            && !self.lifecycle_fence.is_lock_lost()
            && self
                .metadata_transaction_guard
                .as_ref()
                .is_some_and(|guard| !guard.is_lock_lost())
    }

    pub(crate) fn add_lock_fences(&self, opts: &mut ObjectOptions) {
        opts.bucket_lifecycle_lock_fence
            .get_or_insert_with(NamespaceLockFence::new)
            .extend(&self.lifecycle_fence);
        if let Some(guard) = self.metadata_transaction_guard.as_ref() {
            opts.add_namespace_lock_guard(guard);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuotaAdmission {
    current_usage: u64,
    quota_limit: u64,
}

#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleDeleteAllRequest {
    pub(crate) version_id: Option<Uuid>,
    pub(crate) delete_marker: bool,
    pub(crate) action: rustfs_common::metrics::IlmAction,
    pub(crate) rule_id: String,
    pub(crate) phase: LifecycleDeleteAllPhase,
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleDeleteAllPhase {
    Preflight,
    History,
    FinalPreflight,
    Trigger,
}

#[doc(hidden)]
#[derive(Default)]
pub struct LifecycleDeleteAllJournalState {
    prepared: HashMap<String, crate::bucket::lifecycle::tier_sweeper::Jentry>,
    mutation_started: bool,
}

impl Debug for LifecycleDeleteAllJournalState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LifecycleDeleteAllJournalState")
            .field("prepared_count", &self.prepared.len())
            .field("mutation_started", &self.mutation_started)
            .finish()
    }
}

impl LifecycleDeleteAllJournalState {
    pub(crate) fn contains(&self, name: &str) -> bool {
        self.prepared.contains_key(name)
    }

    pub(crate) fn insert(&mut self, name: String, entry: crate::bucket::lifecycle::tier_sweeper::Jentry) {
        self.prepared.insert(name, entry);
    }

    pub(crate) fn prepared_entries(&self) -> Vec<crate::bucket::lifecycle::tier_sweeper::Jentry> {
        self.prepared.values().cloned().collect()
    }

    pub(crate) fn mark_mutation_started(&mut self) {
        self.mutation_started = true;
    }

    pub(crate) fn mutation_started(&self) -> bool {
        self.mutation_started
    }
}

impl QuotaAdmission {
    pub(crate) fn current_usage(self) -> u64 {
        self.current_usage
    }

    pub(crate) fn quota_limit(self) -> u64 {
        self.quota_limit
    }

    pub(crate) fn remaining(self) -> u64 {
        self.quota_limit - self.current_usage
    }
}

#[derive(Debug, Default, Clone)]
pub struct ObjectOptions {
    // Use the maximum parity (N/2), used when saving server configuration files
    pub max_parity: bool,
    pub mod_time: Option<OffsetDateTime>,
    pub part_number: Option<usize>,

    pub delete_prefix: bool,
    pub delete_prefix_object: bool,
    pub version_id: Option<String>,
    /// Lifecycle-only staged purge request checked under the object write lock.
    #[doc(hidden)]
    pub lifecycle_delete_all: Option<LifecycleDeleteAllRequest>,
    #[doc(hidden)]
    pub lifecycle_delete_all_journal: Option<Arc<parking_lot::Mutex<LifecycleDeleteAllJournalState>>>,
    /// RustFS-only compare-and-set condition checked under the object write lock.
    pub expected_current_version_id: Option<String>,
    /// Persisted bucket incarnation observed before authorization.
    pub expected_bucket_incarnation_id: Option<Uuid>,
    pub no_lock: bool,
    /// True when an upper layer already holds the object read lock before
    /// forwarding a no_lock read to the set layer.
    pub metadata_cache_safe: bool,

    pub versioned: bool,
    pub version_suspended: bool,
    pub incl_free_versions: bool,

    pub skip_decommissioned: bool,
    pub skip_rebalancing: bool,
    pub skip_free_version: bool,

    pub data_movement: bool,
    pub raw_data_movement_read: bool,
    /// Materialize the data-movement per-part checksum sidecar for APIs that
    /// return part checksums. Ordinary object reads leave it encoded.
    pub include_part_checksums: bool,
    pub src_pool_idx: usize,
    pub user_defined: HashMap<String, String>,
    pub preserve_etag: Option<String>,
    pub metadata_chg: bool,
    pub http_preconditions: Option<HTTPPreconditions>,

    pub delete_replication: Option<ReplicationState>,
    pub delete_replication_config_snapshot: Option<Arc<DeleteReplicationConfigSnapshot>>,
    pub namespace_lock_fence: Option<NamespaceLockFence>,
    /// Proves an upper layer holds the bucket lifecycle sentinel. A separate
    /// fence avoids recursively acquiring the read lock behind a queued writer.
    pub bucket_lifecycle_lock_fence: Option<NamespaceLockFence>,
    pub replication_request: bool,
    /// True when the inbound request carried the
    /// `{x-rustfs-,x-minio-}source-proxy-request` header family with the
    /// value "true": the request was already proxied by a replication peer,
    /// so this server must not proxy a local miss onward (anti-loop,
    /// MinIO-compatible). The header only disables proxying — it grants no
    /// capability — so no authorization gate is required to honor it.
    pub proxy_request: bool,
    /// True when the `source-proxy-request` header family was present at
    /// all, regardless of value (MinIO's `ProxyHeaderSet`). A replication
    /// peer sends `source-proxy-request: false` on its worker convergence
    /// HEADs precisely so the receiver answers locally instead of proxying
    /// back — otherwise a proxied 404->200 echo makes the worker believe the
    /// object already converged and it never replicates it.
    pub proxy_header_set: bool,
    /// Source-cluster LWW timestamps carried by an authorized replication
    /// request; None when the source never modified the category. Only the
    /// replication-authorized options builders may set these.
    pub replication_tagging_timestamp: Option<OffsetDateTime>,
    pub replication_retention_timestamp: Option<OffsetDateTime>,
    pub replication_legalhold_timestamp: Option<OffsetDateTime>,
    /// Authorized SSE-C replication passthrough: the body is already
    /// ciphertext, so the write path must not encrypt or compress it and
    /// stores the restored encryption metadata verbatim. Only the
    /// replication-authorized options builders may set this.
    pub preserve_ciphertext: bool,
    pub delete_marker: bool,
    pub synthetic_version_id: bool,

    pub transition: TransitionOptions,
    pub expiration: ExpirationOptions,
    pub lifecycle_audit_event: LcAuditEvent,

    pub eval_metadata: Option<HashMap<String, String>>,
    pub object_lock_retention: Option<ObjectLockRetentionOptions>,
    pub object_lock_delete: Option<crate::storage_api_contracts::object::ObjectLockDeleteOptions>,
    /// Authoritative bucket Object Lock snapshot installed inside `ECStore`
    /// before a destructive commit reaches the set layer.
    pub object_lock_config_snapshot: Option<Arc<ObjectLockConfigSnapshot>>,

    pub want_checksum: Option<Checksum>,
    pub skip_verify_bitrot: bool,
    pub capacity_scope_token: Option<Uuid>,
    /// Server-derived bucket-quota snapshot for commit-boundary admission.
    pub quota_admission: Option<QuotaAdmission>,
    /// Storage-owned journal writer used by the atomic delete path. This is
    /// populated only by the `ECStore` wrapper that holds the namespace locks.
    pub tier_delete_journal_api: Option<Arc<crate::store::ECStore>>,
}

impl ObjectOptions {
    pub fn set_quota_admission(&mut self, current_usage: u64, quota_limit: u64) -> bool {
        self.quota_admission = (current_usage <= quota_limit).then_some(QuotaAdmission {
            current_usage,
            quota_limit,
        });
        self.quota_admission.is_some()
    }

    pub(crate) fn overwrites_existing_version(&self) -> bool {
        self.version_id.is_some() || !self.versioned || self.version_suspended
    }

    pub(crate) fn add_namespace_lock_lost_signal(&mut self, signal: Arc<rustfs_lock::distributed_lock::LockLostSignal>) {
        self.namespace_lock_fence
            .get_or_insert_with(NamespaceLockFence::new)
            .add_signal(signal);
    }

    pub(crate) fn ensure_namespace_lock_fence(&mut self) {
        self.namespace_lock_fence.get_or_insert_with(NamespaceLockFence::new);
    }

    #[cfg(test)]
    pub(crate) fn add_namespace_lock_fence_for_test(&mut self, fence: &NamespaceLockFence) {
        self.namespace_lock_fence
            .get_or_insert_with(NamespaceLockFence::new)
            .extend(fence);
    }

    pub(crate) fn ensure_lifecycle_delete_all_journal(&mut self) {
        self.lifecycle_delete_all_journal
            .get_or_insert_with(|| Arc::new(parking_lot::Mutex::new(LifecycleDeleteAllJournalState::default())));
    }

    pub(crate) fn lifecycle_delete_all_journal(&self) -> Option<&Arc<parking_lot::Mutex<LifecycleDeleteAllJournalState>>> {
        self.lifecycle_delete_all_journal.as_ref()
    }

    pub fn add_namespace_lock_guard(&mut self, guard: &rustfs_lock::NamespaceLockGuard) {
        if let Some(signal) = guard.lock_lost_signal() {
            self.add_namespace_lock_lost_signal(signal);
        }
    }

    pub fn add_bucket_lifecycle_lock_guard(&mut self, guard: &rustfs_lock::NamespaceLockGuard) {
        let fence = self.bucket_lifecycle_lock_fence.get_or_insert_with(NamespaceLockFence::new);
        if let Some(signal) = guard.lock_lost_signal() {
            fence.add_signal(signal);
        }
    }

    pub fn set_delete_replication_state(&mut self, dsc: ReplicateDecision) {
        let mut rs = ReplicationState {
            replicate_decision_str: dsc.to_string(),
            ..Default::default()
        };
        if self.version_id.is_none() {
            rs.replication_status_internal = dsc.pending_status();
            rs.targets = replication_statuses_map(rs.replication_status_internal.as_deref().unwrap_or_default());
        } else {
            rs.version_purge_status_internal = dsc.pending_status();
            rs.purge_targets = version_purge_statuses_map(rs.version_purge_status_internal.as_deref().unwrap_or_default());
        }

        self.delete_replication = Some(rs)
    }

    pub fn set_replica_status(&mut self, status: ReplicationStatusType) {
        if let Some(rs) = self.delete_replication.as_mut() {
            rs.replica_status = status;
            rs.replica_timestamp = Some(OffsetDateTime::now_utc());
        } else {
            self.delete_replication = Some(ReplicationState {
                replica_status: status,
                replica_timestamp: Some(OffsetDateTime::now_utc()),
                ..Default::default()
            });
        }
    }

    pub fn version_purge_status(&self) -> VersionPurgeStatusType {
        self.delete_replication
            .as_ref()
            .map(|v| v.composite_version_purge_status())
            .unwrap_or(VersionPurgeStatusType::Empty)
    }

    pub fn delete_marker_replication_status(&self) -> ReplicationStatusType {
        self.delete_replication
            .as_ref()
            .map(|v| v.composite_replication_status())
            .unwrap_or(ReplicationStatusType::Empty)
    }

    pub fn put_replication_state(&self) -> ReplicationState {
        if self
            .delete_replication
            .as_ref()
            .is_some_and(|state| !state.replica_status.is_empty())
        {
            return self.delete_replication.clone().unwrap_or_default();
        }

        let rs = match rustfs_utils::http::get_str(&self.user_defined, rustfs_utils::http::SUFFIX_REPLICATION_STATUS) {
            Some(v) => v,
            None => return ReplicationState::default(),
        };

        ReplicationState {
            replication_status_internal: Some(rs.to_string()),
            targets: replication_statuses_map(rs.as_str()),
            ..Default::default()
        }
    }

    pub fn precondition_check(&self, obj_info: &ObjectInfo) -> Result<()> {
        let requested_part = self.part_number.and_then(|part_number| {
            if part_number > 1 && !obj_info.parts.is_empty() {
                Some(ObjectPreconditionPart {
                    number: part_number,
                    exists: obj_info.parts.iter().any(|pi| pi.number == part_number),
                })
            } else {
                None
            }
        });
        let state = ObjectPreconditionState {
            etag: obj_info.etag.as_deref(),
            mod_time: obj_info.mod_time,
            requested_part,
        };

        state.check(self.http_preconditions.as_ref()).map_err(|err| match err {
            ObjectPreconditionError::InvalidPartNumber(part_number) => Error::InvalidPartNumber(part_number),
            ObjectPreconditionError::NotModified => Error::NotModified,
            ObjectPreconditionError::PreconditionFailed => Error::PreconditionFailed,
        })
    }
}

#[derive(Debug, Default)]
pub struct ObjectInfo {
    pub bucket: String,
    pub name: String,
    pub storage_class: Option<String>,
    pub mod_time: Option<OffsetDateTime>,
    pub size: i64,
    // Actual size is the real size of the object uploaded by client.
    pub actual_size: i64,
    pub is_dir: bool,
    pub user_defined: Arc<HashMap<String, String>>,
    pub parity_blocks: usize,
    pub data_blocks: usize,
    pub version_id: Option<Uuid>,
    /// xl.meta directory UUID for this version, regenerated on every body write.
    /// A write-unique token: the object data cache keys on it so an overwrite
    /// cannot be served the previous body under an MD5 collision (backlog#1111).
    pub data_dir: Option<Uuid>,
    pub delete_marker: bool,
    pub transitioned_object: TransitionedObject,
    pub transition_version_state: rustfs_filemeta::TransitionVersionState,
    pub restore_ongoing: bool,
    pub restore_expires: Option<OffsetDateTime>,
    pub user_tags: Arc<String>,
    pub parts: Arc<Vec<ObjectPartInfo>>,
    pub is_latest: bool,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub expires: Option<OffsetDateTime>,
    pub num_versions: usize,
    pub successor_mod_time: Option<OffsetDateTime>,
    pub put_object_reader: Option<PutObjReader>,
    pub etag: Option<String>,
    pub inlined: bool,
    pub metadata_only: bool,
    pub version_only: bool,
    pub replication_status_internal: Option<String>,
    pub replication_status: ReplicationStatusType,
    pub version_purge_status_internal: Option<String>,
    pub version_purge_status: VersionPurgeStatusType,
    pub replication_decision: String,
    pub checksum: Option<Bytes>,
}

impl Clone for ObjectInfo {
    fn clone(&self) -> Self {
        Self {
            bucket: self.bucket.clone(),
            name: self.name.clone(),
            storage_class: self.storage_class.clone(),
            mod_time: self.mod_time,
            size: self.size,
            actual_size: self.actual_size,
            is_dir: self.is_dir,
            user_defined: self.user_defined.clone(),
            parity_blocks: self.parity_blocks,
            data_blocks: self.data_blocks,
            version_id: self.version_id,
            data_dir: self.data_dir,
            delete_marker: self.delete_marker,
            transitioned_object: self.transitioned_object.clone(),
            transition_version_state: self.transition_version_state,
            restore_ongoing: self.restore_ongoing,
            restore_expires: self.restore_expires,
            user_tags: self.user_tags.clone(),
            parts: self.parts.clone(),
            is_latest: self.is_latest,
            content_type: self.content_type.clone(),
            content_encoding: self.content_encoding.clone(),
            num_versions: self.num_versions,
            successor_mod_time: self.successor_mod_time,
            put_object_reader: None, // reader can not clone
            etag: self.etag.clone(),
            inlined: self.inlined,
            metadata_only: self.metadata_only,
            version_only: self.version_only,
            replication_status_internal: self.replication_status_internal.clone(),
            replication_status: self.replication_status.clone(),
            version_purge_status_internal: self.version_purge_status_internal.clone(),
            version_purge_status: self.version_purge_status.clone(),
            replication_decision: self.replication_decision.clone(),
            checksum: self.checksum.clone(),
            expires: self.expires,
        }
    }
}

impl ObjectInfo {
    pub fn is_compressed(&self) -> bool {
        rustfs_utils::http::contains_key_str(&self.user_defined, rustfs_utils::http::SUFFIX_COMPRESSION)
    }

    pub fn is_compressed_ok(&self) -> Result<(CompressionAlgorithm, bool)> {
        let (algorithm, _, compressed) = self.compression_read_plan()?;
        Ok((algorithm, compressed))
    }

    pub fn compression_read_plan(&self) -> Result<(CompressionAlgorithm, crate::io_support::rio::ReadCompressionBackend, bool)> {
        let scheme = rustfs_utils::http::get_str(&self.user_defined, rustfs_utils::http::SUFFIX_COMPRESSION);

        if let Some(scheme) = scheme {
            let (algorithm, backend) = crate::io_support::rio::compression_scheme_to_read_plan(&scheme)?;
            Ok((algorithm, backend, true))
        } else {
            Ok((CompressionAlgorithm::None, crate::io_support::rio::ReadCompressionBackend::Legacy, false))
        }
    }

    pub fn is_multipart(&self) -> bool {
        self.etag.as_ref().is_some_and(|v| v.len() != 32)
    }

    pub fn is_encrypted(&self) -> bool {
        self.user_defined
            .keys()
            .any(|key| rustfs_utils::http::is_object_encryption_marker(key))
    }

    /// Maximum inline size for non-versioned objects (128 KiB).
    /// Matches `DEFAULT_INLINE_BLOCK` in `storageclass.rs`.
    pub const INLINE_MAX_SIZE: i64 = 128 * 1024;

    /// Maximum inline size for versioned objects (16 KiB).
    /// Matches `DEFAULT_INLINE_BLOCK / 8` in `storageclass.rs`.
    pub const INLINE_MAX_SIZE_VERSIONED: i64 = 16 * 1024;

    /// Returns `true` when this object qualifies for the inline data fast path.
    ///
    /// The inline fast path decodes erasure-coded data entirely in memory,
    /// bypassing disk I/O, duplex pipes, and the disk-read semaphore.
    ///
    /// The `inlined` flag is the primary signal — PUT sets it through the
    /// captured storage-class snapshot's `Config::should_inline`, which applies
    /// the correct version-aware threshold (128 KiB non-versioned, 16 KiB versioned).
    /// The size check below is a safety net using the same thresholds.
    ///
    /// Additional conditions:
    /// - Single part
    /// - Not encrypted
    /// - Not compressed
    /// - Not transitioned to remote tier
    pub fn is_inline_fast_path_eligible(&self) -> bool {
        if !self.inlined {
            return false;
        }
        // Apply the same version-aware threshold as PUT (storageclass.rs).
        let max_size = if self.version_id.is_some() {
            Self::INLINE_MAX_SIZE_VERSIONED
        } else {
            Self::INLINE_MAX_SIZE
        };
        self.parts.len() == 1
            && self.size <= max_size
            && !self.is_encrypted()
            && !self.is_compressed()
            && self.transitioned_object.tier.is_empty()
    }

    pub fn encryption_original_size(&self) -> std::io::Result<Option<i64>> {
        rustfs_utils::http::get_object_encryption_original_size(&self.user_defined)
    }

    pub fn decrypted_size(&self) -> std::io::Result<i64> {
        Ok(self.encryption_original_size()?.unwrap_or(self.size))
    }

    pub fn get_actual_size(&self) -> std::io::Result<i64> {
        if self.actual_size < -1 || (self.actual_size == -1 && !self.is_compressed()) {
            return Err(std::io::Error::other("invalid negative actual size"));
        }
        if self.actual_size > 0 {
            return Ok(self.actual_size);
        }

        if self.is_compressed() {
            if let Some(size_str) = rustfs_utils::http::get_str(&self.user_defined, rustfs_utils::http::SUFFIX_ACTUAL_SIZE)
                && !size_str.is_empty()
            {
                let size = size_str.parse::<i64>().map_err(|e| std::io::Error::other(e.to_string()))?;
                return Ok(size);
            }
            if self.actual_size == -1 && self.parts.is_empty() {
                return Ok(-1);
            }
            let mut actual_size = 0_i64;
            let mut unknown = false;
            for part in self.parts.iter() {
                match part.actual_size {
                    -1 => unknown = true,
                    size if size >= 0 => {
                        actual_size = actual_size
                            .checked_add(size)
                            .ok_or_else(|| std::io::Error::other("compressed actual size overflow"))?;
                    }
                    _ => return Err(std::io::Error::other("invalid negative compressed part size")),
                }
            }
            if unknown {
                return Ok(-1);
            }
            if actual_size == 0 && actual_size != self.size {
                return Err(std::io::Error::other(format!("invalid decompressed size {} {}", actual_size, self.size)));
            }

            return Ok(actual_size);
        }

        if let Some(size) = self.encryption_original_size()? {
            return Ok(size);
        }

        Ok(self.size)
    }

    /// Returns a non-negative size for client and replication boundaries.
    ///
    /// Compressed legacy metadata can retain the internal `-1` unknown-size
    /// sentinel. Those boundaries cannot emit a negative length, so they use
    /// the persisted physical size while quota accounting keeps the sentinel
    /// distinction in [`crate::data_usage::quota_object_size`].
    pub fn get_actual_size_or_physical(&self) -> i64 {
        self.get_actual_size()
            .map(|size| if size >= 0 { size } else { self.size.max(0) })
            .unwrap_or_else(|_| self.size.max(0))
    }

    pub fn from_file_info(fi: &FileInfo, bucket: &str, object: &str, versioned: bool) -> ObjectInfo {
        let mut version_id = fi.version_id;

        if versioned && version_id.is_none() {
            version_id = Some(Uuid::nil())
        }

        Self::from_file_info_with_version_id(fi, bucket, object, version_id)
    }

    pub(crate) fn from_file_info_with_version_id(
        fi: &FileInfo,
        bucket: &str,
        object: &str,
        version_id: Option<Uuid>,
    ) -> ObjectInfo {
        let name = decode_dir_object(object);

        // etag
        let (content_type, content_encoding, etag) = {
            let content_type = fi.metadata.get("content-type").cloned();
            let content_encoding = fi.metadata.get("content-encoding").cloned();
            let etag = fi.metadata.get("etag").cloned();

            (content_type, content_encoding, etag)
        };

        // tags
        let user_tags: Arc<String> = fi
            .metadata
            .get(AMZ_OBJECT_TAGGING)
            .map(|s| Arc::new(s.clone()))
            .unwrap_or_default();

        let inlined = fi.inline_data();

        // Parse expires from metadata (HTTP date format RFC 7231 or ISO 8601)
        let expires = fi.metadata.get("expires").and_then(|s| {
            // Try parsing as ISO 8601 first
            OffsetDateTime::parse(s, &time::format_description::well_known::Iso8601::DEFAULT)
                .or_else(|_| {
                    // Try RFC 2822 format
                    OffsetDateTime::parse(s, &time::format_description::well_known::Rfc2822)
                })
                .or_else(|_| {
                    // Try RFC 3339 format
                    OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
                })
                .ok()
        });

        let replication_status_internal = fi
            .replication_state_internal
            .as_ref()
            .and_then(|v| v.replication_status_internal.clone());
        let version_purge_status_internal = fi
            .replication_state_internal
            .as_ref()
            .and_then(|v| v.version_purge_status_internal.clone());
        let replication_decision = fi
            .replication_state_internal
            .as_ref()
            .map(|v| v.replicate_decision_str.clone())
            .unwrap_or_default();

        let mut replication_status = replication_status_from_filemeta(fi.replication_status());
        if replication_status.is_empty()
            && let Some(status) = fi.metadata.get(AMZ_BUCKET_REPLICATION_STATUS).cloned()
            && status == ReplicationStatusType::Replica.as_str()
        {
            replication_status = ReplicationStatusType::Replica;
        }

        let version_purge_status = version_purge_status_from_filemeta(fi.version_purge_status());

        let transitioned_object = TransitionedObject {
            name: fi.transitioned_objname.clone(),
            version_id: fi
                .transition_version
                .clone()
                .or_else(|| fi.transition_version_id.map(|version_id| version_id.to_string()))
                .unwrap_or_default(),
            status: fi.transition_status.clone(),
            free_version: fi.tier_free_version(),
            tier: fi.transition_tier.clone(),
        };

        let metadata = {
            let mut v = fi.metadata.clone();
            clean_metadata(&mut v);
            v
        };

        let storage_class = Some(
            storageclass::effective_class(
                fi.metadata.get(AMZ_STORAGE_CLASS).map(String::as_str),
                (fi.transition_status == rustfs_filemeta::TRANSITION_COMPLETE && !fi.transition_tier.is_empty())
                    .then_some(fi.transition_tier.as_str()),
            )
            .to_string(),
        );

        let mut restore_ongoing = false;
        let mut restore_expires = None;
        if let Some(restore_status) = fi.metadata.get(AMZ_RESTORE).cloned()
            && let Ok(restore_status) = parse_restore_obj_status(&restore_status)
        {
            restore_ongoing = restore_status.on_going();
            restore_expires = restore_status.expiry();
        }

        // Convert parts from rustfs_filemeta::ObjectPartInfo to object_api::ObjectPartInfo
        let parts = fi
            .parts
            .iter()
            .map(|part| ObjectPartInfo {
                etag: part.etag.clone(),
                index: part.index.clone(),
                size: part.size,
                actual_size: part.actual_size,
                mod_time: part.mod_time,
                checksums: part.checksums.clone(),
                number: part.number,
                error: part.error.clone(),
            })
            .collect::<Vec<_>>();

        ObjectInfo {
            bucket: bucket.to_string(),
            name,
            is_dir: object.starts_with('/'),
            parity_blocks: fi.erasure.parity_blocks,
            data_blocks: fi.erasure.data_blocks,
            version_id,
            data_dir: fi.data_dir,
            delete_marker: fi.deleted,
            mod_time: fi.mod_time,
            size: fi.size,
            parts: Arc::new(parts),
            is_latest: fi.is_latest,
            user_tags,
            content_type,
            content_encoding,
            expires,
            num_versions: fi.num_versions,
            successor_mod_time: fi.successor_mod_time,
            etag,
            inlined,
            user_defined: Arc::new(metadata),
            transitioned_object,
            transition_version_state: fi.transition_version_state,
            checksum: fi.checksum.clone(),
            storage_class,
            restore_ongoing,
            restore_expires,
            replication_status_internal,
            replication_status,
            version_purge_status_internal,
            version_purge_status,
            replication_decision,
            ..Default::default()
        }
    }

    pub async fn from_meta_cache_entries_sorted_versions(
        entries: &MetaCacheEntriesSorted,
        bucket: &str,
        prefix: &str,
        delimiter: Option<String>,
        after_version_marker: Option<VersionMarker>,
    ) -> Vec<ObjectInfo> {
        Self::from_meta_cache_entries_sorted_versions_with_purge(
            entries,
            bucket,
            prefix,
            delimiter,
            after_version_marker,
            false,
            false,
        )
        .await
        .0
    }

    pub(crate) async fn from_meta_cache_entries_sorted_versions_for_lifecycle(
        entries: &MetaCacheEntriesSorted,
        bucket: &str,
        prefix: &str,
        delimiter: Option<String>,
        after_version_marker: Option<VersionMarker>,
    ) -> Vec<ObjectInfo> {
        Self::from_meta_cache_entries_sorted_versions_with_purge(
            entries,
            bucket,
            prefix,
            delimiter,
            after_version_marker,
            true,
            false,
        )
        .await
        .0
    }

    pub(crate) async fn from_meta_cache_entries_sorted_versions_for_recursive_delete(
        entries: &MetaCacheEntriesSorted,
        bucket: &str,
        prefix: &str,
        delimiter: Option<String>,
        after_version_marker: Option<VersionMarker>,
    ) -> Result<Vec<ObjectInfo>> {
        let (objects, error) = Self::from_meta_cache_entries_sorted_versions_with_purge(
            entries,
            bucket,
            prefix,
            delimiter,
            after_version_marker,
            true,
            true,
        )
        .await;
        match error {
            Some(error) => Err(error),
            None => Ok(objects),
        }
    }

    async fn from_meta_cache_entries_sorted_versions_with_purge(
        entries: &MetaCacheEntriesSorted,
        bucket: &str,
        prefix: &str,
        delimiter: Option<String>,
        after_version_marker: Option<VersionMarker>,
        include_version_purge: bool,
        fail_on_decode_error: bool,
    ) -> (Vec<ObjectInfo>, Option<Error>) {
        let vcfg = get_versioning_config(bucket).await.ok();
        let mut objects = Vec::with_capacity(entries.entries().len());
        let mut prev_prefix = "";
        let mut after_version_marker = after_version_marker;
        for entry in entries.entries() {
            if entry.is_object() {
                if let Some(delimiter) = &delimiter {
                    let remaining = if entry.name.starts_with(prefix) {
                        &entry.name[prefix.len()..]
                    } else {
                        entry.name.as_str()
                    };
                    if let Some(idx) = remaining.find(delimiter.as_str()) {
                        let idx = prefix.len() + idx + delimiter.len();
                        if let Some(curr_prefix) = entry.name.get(0..idx) {
                            if curr_prefix == prev_prefix {
                                continue;
                            }

                            prev_prefix = curr_prefix;

                            objects.push(ObjectInfo {
                                is_dir: true,
                                bucket: bucket.to_owned(),
                                name: curr_prefix.to_owned(),
                                ..Default::default()
                            });
                        }
                        continue;
                    }
                }

                let file_infos = match entry.file_info_versions(bucket) {
                    Ok(res) => res,
                    Err(err) => {
                        if fail_on_decode_error {
                            return (objects, Some(err.into()));
                        }
                        warn!("file_info_versions err {:?}", err);
                        continue;
                    }
                };

                let versions = if let Some(marker) = after_version_marker.take() {
                    versions_after_marker(&file_infos, marker)
                } else {
                    &file_infos.versions
                };

                for fi in versions.iter() {
                    if !include_version_purge && !fi.version_purge_status().is_empty() {
                        continue;
                    }

                    let versioned = vcfg.clone().map(|v| v.0.versioned(&entry.name)).unwrap_or_default();
                    objects.push(ObjectInfo::from_file_info(fi, bucket, &entry.name, versioned));
                }
                continue;
            }

            if entry.is_dir()
                && let Some(delimiter) = &delimiter
                && let Some(idx) = {
                    let remaining = if entry.name.starts_with(prefix) {
                        &entry.name[prefix.len()..]
                    } else {
                        entry.name.as_str()
                    };
                    remaining.find(delimiter.as_str())
                }
            {
                let idx = prefix.len() + idx + delimiter.len();
                if let Some(curr_prefix) = entry.name.get(0..idx) {
                    if curr_prefix == prev_prefix {
                        continue;
                    }

                    prev_prefix = curr_prefix;

                    objects.push(ObjectInfo {
                        is_dir: true,
                        bucket: bucket.to_owned(),
                        name: curr_prefix.to_owned(),
                        ..Default::default()
                    });
                }
            }
        }

        (objects, None)
    }

    pub async fn from_meta_cache_entries_sorted_infos(
        entries: &MetaCacheEntriesSorted,
        bucket: &str,
        prefix: &str,
        delimiter: Option<String>,
    ) -> Vec<ObjectInfo> {
        let vcfg = get_versioning_config(bucket).await.ok();
        let mut objects = Vec::with_capacity(entries.entries().len());
        let mut prev_prefix = "";
        for entry in entries.entries() {
            if entry.is_object() {
                if let Some(delimiter) = &delimiter {
                    let remaining = if entry.name.starts_with(prefix) {
                        &entry.name[prefix.len()..]
                    } else {
                        entry.name.as_str()
                    };
                    if let Some(idx) = remaining.find(delimiter.as_str()) {
                        let idx = prefix.len() + idx + delimiter.len();
                        if let Some(curr_prefix) = entry.name.get(0..idx) {
                            if curr_prefix == prev_prefix {
                                continue;
                            }

                            prev_prefix = curr_prefix;

                            objects.push(ObjectInfo {
                                is_dir: true,
                                bucket: bucket.to_owned(),
                                name: curr_prefix.to_owned(),
                                ..Default::default()
                            });
                        }
                        continue;
                    }
                }

                let fi = match entry.to_fileinfo(bucket) {
                    Ok(res) => res,
                    Err(err) => {
                        warn!("file_info_versions err {:?}", err);
                        continue;
                    }
                };

                // TODO(backlog): handle VersionPurgeStatus in object listing
                let versioned = vcfg.clone().map(|v| v.0.versioned(&entry.name)).unwrap_or_default();
                objects.push(ObjectInfo::from_file_info(&fi, bucket, &entry.name, versioned));

                continue;
            }

            if entry.is_dir()
                && let Some(delimiter) = &delimiter
                && let Some(idx) = {
                    let remaining = if entry.name.starts_with(prefix) {
                        &entry.name[prefix.len()..]
                    } else {
                        entry.name.as_str()
                    };
                    remaining.find(delimiter.as_str())
                }
            {
                let idx = prefix.len() + idx + delimiter.len();
                if let Some(curr_prefix) = entry.name.get(0..idx) {
                    if curr_prefix == prev_prefix {
                        continue;
                    }

                    prev_prefix = curr_prefix;

                    objects.push(ObjectInfo {
                        is_dir: true,
                        bucket: bucket.to_owned(),
                        name: curr_prefix.to_owned(),
                        ..Default::default()
                    });
                }
            }
        }

        objects
    }

    pub fn replication_state(&self) -> ReplicationState {
        // Derived from the durable internal keys, not from the wire form: the
        // state's positional encoding skips this map.
        let (target_delete_marker_version_ids, target_delete_marker_version_ids_corrupt) =
            rustfs_utils::http::target_delete_marker_versions(&self.user_defined);
        ReplicationState {
            replication_status_internal: self.replication_status_internal.clone(),
            version_purge_status_internal: self.version_purge_status_internal.clone(),
            replicate_decision_str: self.replication_decision.clone(),
            targets: replication_statuses_map(self.replication_status_internal.clone().unwrap_or_default().as_str()),
            purge_targets: version_purge_statuses_map(self.version_purge_status_internal.clone().unwrap_or_default().as_str()),
            reset_statuses_map: self
                .user_defined
                .iter()
                .filter_map(|(k, v)| {
                    rustfs_utils::http::internal_key_strip_suffix_prefix(
                        k,
                        rustfs_utils::http::SUFFIX_REPLICATION_RESET_ARN_PREFIX,
                    )
                    .map(|arn| (arn, v.clone()))
                })
                .collect(),
            target_delete_marker_version_ids,
            target_delete_marker_version_ids_corrupt,
            ..Default::default()
        }
    }

    pub fn target_replication_status(&self, arn: &str) -> ReplicationStatusType {
        self.replication_status_internal
            .as_deref()
            .unwrap_or_default()
            .split(';')
            .find_map(|entry| {
                let (target_arn, status) = entry.split_once('=')?;
                (!target_arn.is_empty() && target_arn == arn).then(|| ReplicationStatusType::from(status))
            })
            .unwrap_or_default()
    }

    pub fn decrypt_checksums(&self, part: usize, _headers: &HeaderMap) -> Result<(HashMap<String, String>, bool)> {
        if part > 0
            && let Some(checksums) = self.parts.iter().find(|p| p.number == part).and_then(|p| p.checksums.clone())
        {
            return Ok((checksums, true));
        }

        if let Some(data) = &self.checksum {
            if self.is_encrypted() {
                // Object-level encrypted checksum bytes require SSE decrypt material,
                // so do not expose them as plaintext checksum headers here. The
                // `false` multipart flag feeds the response-path COMPOSITE
                // fallback; callers that need accurate multipart routing must
                // consult `is_multipart()` instead of this value.
                return Ok((HashMap::new(), false));
            }

            let (checksums, is_multipart) = rustfs_rio::read_checksums(data.as_ref(), 0);
            return Ok((checksums, is_multipart));
        }

        Ok((HashMap::new(), false))
    }
}

fn versions_after_marker(file_infos: &rustfs_filemeta::FileInfoVersions, marker: VersionMarker) -> &[FileInfo] {
    let marker_idx = match marker {
        VersionMarker::Null => file_infos.versions.iter().position(|version| version.version_id.is_none()),
        VersionMarker::Version(vid) => file_infos.find_version_index(vid),
    };

    marker_idx
        .map(|idx| &file_infos.versions[idx + 1..])
        .unwrap_or(&file_infos.versions)
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_lock_config_snapshot_is_bound_to_store_bucket_and_incarnation() {
        let store_id = Uuid::new_v4();
        let incarnation_id = Uuid::new_v4();
        let snapshot = ObjectLockConfigSnapshot::for_store_bucket(
            store_id,
            "source-bucket",
            incarnation_id,
            OffsetDateTime::UNIX_EPOCH,
            crate::bucket::metadata_sys::ObjectLockConfigState::ConfirmedAbsent,
        );

        assert!(snapshot.is_for_store_bucket(store_id, "source-bucket", incarnation_id, OffsetDateTime::UNIX_EPOCH));
        assert!(!snapshot.is_for_store_bucket(Uuid::new_v4(), "source-bucket", incarnation_id, OffsetDateTime::UNIX_EPOCH));
        assert!(!snapshot.is_for_store_bucket(store_id, "other-bucket", incarnation_id, OffsetDateTime::UNIX_EPOCH));
        assert!(!snapshot.is_for_store_bucket(store_id, "source-bucket", Uuid::new_v4(), OffsetDateTime::UNIX_EPOCH));
        assert!(!snapshot.is_for_store_bucket(store_id, "source-bucket", incarnation_id, OffsetDateTime::now_utc()));
    }
    use rustfs_filemeta::{FileInfo, FileMeta, MetaCacheEntry, TRANSITION_COMPLETE};

    fn inline_fast_path_object(size: i64, versioned: bool) -> ObjectInfo {
        ObjectInfo {
            size,
            inlined: true,
            version_id: versioned.then(|| Uuid::from_u128(1)),
            parts: Arc::new(vec![ObjectPartInfo::default()]),
            ..Default::default()
        }
    }

    #[test]
    fn inline_fast_path_eligibility_preserves_exact_versioned_boundaries() {
        for (case, size, versioned, expected) in [
            ("unversioned below", 128 * 1024 - 1, false, true),
            ("unversioned exact", 128 * 1024, false, true),
            ("unversioned above", 128 * 1024 + 1, false, false),
            ("versioned below", 16 * 1024 - 1, true, true),
            ("versioned exact", 16 * 1024, true, true),
            ("versioned above", 16 * 1024 + 1, true, false),
        ] {
            assert_eq!(
                inline_fast_path_object(size, versioned).is_inline_fast_path_eligible(),
                expected,
                "{case}: object_size={size}, versioned={versioned}"
            );
        }
    }

    #[test]
    fn inline_fast_path_eligibility_rejects_incompatible_object_shapes() {
        let mut object = inline_fast_path_object(ObjectInfo::INLINE_MAX_SIZE, false);

        object.inlined = false;
        assert!(!object.is_inline_fast_path_eligible(), "non-inline objects must fall back");

        object.inlined = true;
        object.parts = Arc::new(vec![ObjectPartInfo::default(), ObjectPartInfo::default()]);
        assert!(!object.is_inline_fast_path_eligible(), "multipart objects must fall back");

        object.parts = Arc::new(vec![ObjectPartInfo::default()]);
        object.user_defined = Arc::new(HashMap::from([("x-amz-server-side-encryption".to_string(), "AES256".to_string())]));
        assert!(!object.is_inline_fast_path_eligible(), "encrypted objects must fall back");

        object.user_defined = Arc::new(HashMap::from([(
            rustfs_utils::http::internal_key_rustfs(rustfs_utils::http::SUFFIX_COMPRESSION),
            "zstd".to_string(),
        )]));
        assert!(!object.is_inline_fast_path_eligible(), "compressed objects must fall back");

        object.user_defined = Arc::default();
        object.transitioned_object.tier = "remote-tier".to_string();
        assert!(!object.is_inline_fast_path_eligible(), "transitioned objects must fall back");
    }

    #[test]
    fn minio_internal_encryption_metadata_is_not_treated_as_plaintext() {
        let object = ObjectInfo {
            user_defined: Arc::new(HashMap::from([(
                "X-Minio-Internal-Server-Side-Encryption-Sealed-Key".to_string(),
                "sealed".to_string(),
            )])),
            ..Default::default()
        };

        assert!(object.is_encrypted());
    }

    #[test]
    fn versions_after_marker_handles_null_version_marker() {
        let first_version = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
        let last_version = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
        let file_infos = rustfs_filemeta::FileInfoVersions {
            versions: vec![
                FileInfo {
                    version_id: Some(first_version),
                    ..Default::default()
                },
                FileInfo {
                    version_id: None,
                    ..Default::default()
                },
                FileInfo {
                    version_id: Some(last_version),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let versions = versions_after_marker(&file_infos, VersionMarker::Null);

        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].version_id, Some(last_version));
    }

    #[test]
    fn put_replication_state_preserves_replica_status() {
        let opts = ObjectOptions {
            delete_replication: Some(ReplicationState {
                replica_status: ReplicationStatusType::Replica,
                ..Default::default()
            }),
            ..Default::default()
        };

        let state = opts.put_replication_state();

        assert_eq!(state.composite_replication_status(), ReplicationStatusType::Replica);
    }

    #[test]
    fn object_info_replication_helpers_parse_target_status_and_reset_headers() {
        let reset_key = rustfs_utils::http::internal_key_rustfs("replication-reset-arn:target-a");
        let user_defined = HashMap::from([(reset_key, "reset-id".to_string())]);
        let object = ObjectInfo {
            replication_status_internal: Some("arn:target-a=COMPLETED;arn:target-b=FAILED;".to_string()),
            version_purge_status_internal: Some("arn:target-a=PENDING;".to_string()),
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };

        let state = object.replication_state();

        assert_eq!(object.target_replication_status("arn:target-a"), ReplicationStatusType::Completed);
        assert_eq!(object.target_replication_status("arn:target-b"), ReplicationStatusType::Failed);
        assert_eq!(object.target_replication_status("arn:missing"), ReplicationStatusType::Empty);
        assert_eq!(state.targets.get("arn:target-b"), Some(&ReplicationStatusType::Failed));
        assert_eq!(state.purge_targets.get("arn:target-a"), Some(&VersionPurgeStatusType::Pending));
        assert_eq!(state.reset_statuses_map.get("arn:target-a"), Some(&"reset-id".to_string()));
    }

    #[test]
    fn versions_after_marker_handles_uuid_version_marker() {
        let first_version = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
        let last_version = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
        let file_infos = rustfs_filemeta::FileInfoVersions {
            versions: vec![
                FileInfo {
                    version_id: Some(first_version),
                    ..Default::default()
                },
                FileInfo {
                    version_id: None,
                    ..Default::default()
                },
                FileInfo {
                    version_id: Some(last_version),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let versions = versions_after_marker(&file_infos, VersionMarker::Version(first_version));

        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].version_id, None);
        assert_eq!(versions[1].version_id, Some(last_version));
    }

    #[test]
    fn versions_after_marker_preserves_stale_marker_compatibility() {
        let existing_version =
            Uuid::parse_str("11111111-2222-3333-4444-555555555555").expect("existing version UUID should parse");
        let deleted_marker = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").expect("delete marker UUID should parse");
        let file_infos = rustfs_filemeta::FileInfoVersions {
            versions: vec![FileInfo {
                version_id: Some(existing_version),
                ..Default::default()
            }],
            ..Default::default()
        };

        let versions = versions_after_marker(&file_infos, VersionMarker::Version(deleted_marker));

        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].version_id, Some(existing_version));
    }

    #[tokio::test]
    async fn versions_listing_applies_version_marker_only_to_first_entry() {
        let metadata = rustfs_filemeta::test_data::create_real_xlmeta().expect("test metadata should be valid");
        let entries = MetaCacheEntriesSorted {
            o: rustfs_filemeta::MetaCacheEntries(vec![
                Some(rustfs_filemeta::MetaCacheEntry {
                    name: "obj-a".to_owned(),
                    metadata: metadata.clone(),
                    ..Default::default()
                }),
                Some(rustfs_filemeta::MetaCacheEntry {
                    name: "obj-b".to_owned(),
                    metadata,
                    ..Default::default()
                }),
            ]),
            ..Default::default()
        };
        let marker_version = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();

        let objects = ObjectInfo::from_meta_cache_entries_sorted_versions(
            &entries,
            "bucket",
            "",
            None,
            Some(VersionMarker::Version(marker_version)),
        )
        .await;

        let obj_a_count = objects.iter().filter(|object| object.name == "obj-a").count();
        let obj_b_count = objects.iter().filter(|object| object.name == "obj-b").count();

        assert_eq!(obj_a_count, 2);
        assert_eq!(obj_b_count, 3);
        assert_eq!(objects.len(), 5);
    }

    #[tokio::test]
    async fn versions_listing_excludes_tier_free_versions_from_delete_marker_count() {
        let object_version_id = Uuid::new_v4();
        let remote_version_id = Uuid::new_v4();
        let free_version_id = Uuid::new_v4();
        let delete_marker_id = Uuid::new_v4();
        let base_time = OffsetDateTime::now_utc();
        let mut fm = FileMeta::new();

        fm.add_version(FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(object_version_id),
            transition_status: TRANSITION_COMPLETE.to_string(),
            transitioned_objname: "remote/object".to_string(),
            transition_version_id: Some(remote_version_id),
            transition_tier: "WARM".to_string(),
            mod_time: Some(base_time),
            ..Default::default()
        })
        .expect("transitioned object version should be added");

        let mut delete_fi = FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(object_version_id),
            mod_time: Some(base_time),
            ..Default::default()
        };
        delete_fi.set_tier_free_version_id(&free_version_id.to_string());
        fm.delete_version(&delete_fi)
            .expect("transitioned delete should create a free-version record");

        fm.add_version(FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(delete_marker_id),
            deleted: true,
            mod_time: Some(base_time + time::Duration::seconds(1)),
            ..Default::default()
        })
        .expect("delete marker should be added");

        let entries = MetaCacheEntriesSorted {
            o: rustfs_filemeta::MetaCacheEntries(vec![Some(MetaCacheEntry {
                name: "object".to_string(),
                metadata: fm.marshal_msg().expect("metadata should marshal"),
                ..Default::default()
            })]),
            ..Default::default()
        };

        let objects = ObjectInfo::from_meta_cache_entries_sorted_versions(&entries, "bucket", "", None, None).await;

        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].name, "object");
        assert!(objects[0].delete_marker);
        assert!(objects[0].is_latest);
        assert_eq!(objects[0].num_versions, 1);
    }

    #[tokio::test]
    async fn lifecycle_versions_listing_preserves_purge_pending_versions() {
        let visible_version_id = Uuid::new_v4();
        let purge_version_id = Uuid::new_v4();
        let base_time = OffsetDateTime::now_utc();
        let mut fm = FileMeta::new();

        fm.add_version(FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(purge_version_id),
            mod_time: Some(base_time),
            ..Default::default()
        })
        .expect("version pending purge should be added");
        fm.add_version(FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(visible_version_id),
            mod_time: Some(base_time + time::Duration::seconds(1)),
            ..Default::default()
        })
        .expect("visible version should be added");
        fm.delete_version(&FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(purge_version_id),
            replication_state_internal: Some(crate::bucket::replication::replication_state_to_filemeta(&ReplicationState {
                version_purge_status_internal: Some("arn:target-a=PENDING;".to_string()),
                purge_targets: version_purge_statuses_map("arn:target-a=PENDING;"),
                ..Default::default()
            })),
            ..Default::default()
        })
        .expect("version purge status should be persisted");

        let entries = MetaCacheEntriesSorted {
            o: rustfs_filemeta::MetaCacheEntries(vec![Some(MetaCacheEntry {
                name: "object".to_string(),
                metadata: fm.marshal_msg().expect("metadata should marshal"),
                ..Default::default()
            })]),
            ..Default::default()
        };

        let public_objects = ObjectInfo::from_meta_cache_entries_sorted_versions(&entries, "bucket", "", None, None).await;
        let lifecycle_objects =
            ObjectInfo::from_meta_cache_entries_sorted_versions_for_lifecycle(&entries, "bucket", "", None, None).await;

        assert_eq!(public_objects.len(), 1);
        assert_eq!(public_objects[0].version_id, Some(visible_version_id));
        assert_eq!(public_objects[0].num_versions, 2);
        assert_eq!(lifecycle_objects.len(), 2);
        assert!(
            lifecycle_objects
                .iter()
                .any(|object| object.version_purge_status == VersionPurgeStatusType::Pending)
        );
        assert!(lifecycle_objects.iter().all(|object| object.num_versions == 2));
    }

    #[test]
    fn get_actual_size_prefers_actual_size_field() {
        let info = ObjectInfo {
            size: 5,
            actual_size: 10,
            ..Default::default()
        };

        assert_eq!(info.get_actual_size().unwrap(), 10);
    }

    #[test]
    fn get_actual_size_uses_compressed_metadata_size() {
        let user_defined = {
            let mut map = HashMap::new();
            rustfs_utils::http::insert_str(&mut map, rustfs_utils::http::SUFFIX_COMPRESSION, "zstd".to_string());
            rustfs_utils::http::insert_str(&mut map, rustfs_utils::http::SUFFIX_ACTUAL_SIZE, "42".to_string());
            map
        };

        let info = ObjectInfo {
            size: 100,
            actual_size: 0,
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };

        assert_eq!(info.get_actual_size().unwrap(), 42);
    }

    #[test]
    fn get_actual_size_falls_back_to_encrypted_original_size_metadata() {
        let user_defined = {
            let mut map = HashMap::new();
            map.insert("x-amz-server-side-encryption-customer-original-size".to_string(), "77".to_string());
            map
        };

        let info = ObjectInfo {
            size: 100,
            actual_size: 0,
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };

        assert_eq!(info.get_actual_size().unwrap(), 77);
    }

    #[test]
    fn precondition_check_ignores_empty_etag_conditions() {
        let opts = ObjectOptions {
            http_preconditions: Some(HTTPPreconditions {
                if_match: Some(String::new()),
                if_none_match: Some(" ".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let info = ObjectInfo {
            mod_time: Some(OffsetDateTime::now_utc()),
            etag: Some("\"abc\"".to_string()),
            ..Default::default()
        };

        assert!(opts.precondition_check(&info).is_ok());
    }

    #[test]
    fn from_file_info_preserves_replication_decision() {
        let fi = FileInfo {
            replication_state_internal: Some(crate::bucket::replication::replication_state_to_filemeta(&ReplicationState {
                replicate_decision_str: "arn=true;false;arn:replication::1:dest;rule-id".to_string(),
                ..Default::default()
            })),
            ..Default::default()
        };

        let info = ObjectInfo::from_file_info(&fi, "bucket", "object", true);

        assert_eq!(info.replication_decision, "arn=true;false;arn:replication::1:dest;rule-id");
    }

    #[test]
    fn from_file_info_with_version_id_keeps_normalized_absent_version() {
        let fi = FileInfo {
            version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };

        let info = ObjectInfo::from_file_info_with_version_id(&fi, "bucket", "object", None);

        assert_eq!(info.version_id, None, "a normalized absent version must not be rewritten to nil");
    }

    #[test]
    fn from_file_info_reports_effective_storage_class_for_legacy_metadata() {
        for legacy_label in [
            storageclass::STANDARD_IA,
            storageclass::ONEZONE_IA,
            storageclass::INTELLIGENT_TIERING,
            storageclass::GLACIER,
        ] {
            let fi = FileInfo {
                metadata: HashMap::from([(AMZ_STORAGE_CLASS.to_string(), legacy_label.to_string())]),
                ..Default::default()
            };

            let info = ObjectInfo::from_file_info(&fi, "bucket", "legacy-object", true);

            assert_eq!(
                info.storage_class.as_deref(),
                Some(storageclass::STANDARD),
                "{legacy_label} was only a label and must report the effective STANDARD layout"
            );
        }
    }

    #[test]
    fn from_file_info_preserves_transitioned_tier_storage_class() {
        let fi = FileInfo {
            metadata: HashMap::from([(AMZ_STORAGE_CLASS.to_string(), storageclass::STANDARD_IA.to_string())]),
            transition_tier: "WARM-TIER".to_string(),
            transition_status: TRANSITION_COMPLETE.to_string(),
            ..Default::default()
        };

        let info = ObjectInfo::from_file_info(&fi, "bucket", "transitioned-object", true);

        assert_eq!(info.storage_class.as_deref(), Some("WARM-TIER"));
        assert_eq!(info.transitioned_object.tier, "WARM-TIER");
    }

    #[test]
    fn from_file_info_ignores_a_tier_name_without_a_completed_transition() {
        let fi = FileInfo {
            metadata: HashMap::from([(AMZ_STORAGE_CLASS.to_string(), storageclass::STANDARD_IA.to_string())]),
            transition_tier: "WARM-TIER".to_string(),
            ..Default::default()
        };

        let info = ObjectInfo::from_file_info(&fi, "bucket", "incomplete-transition", true);

        assert_eq!(info.storage_class.as_deref(), Some(storageclass::STANDARD));
        assert_eq!(info.transitioned_object.tier, "WARM-TIER");
    }

    #[test]
    fn get_actual_size_uses_compressed_parts_actual_size_when_metadata_missing() {
        let user_defined = {
            let mut map = HashMap::new();
            rustfs_utils::http::insert_str(&mut map, rustfs_utils::http::SUFFIX_COMPRESSION, "zstd".to_string());
            map
        };

        let info = ObjectInfo {
            size: 12,
            actual_size: 0,
            user_defined: Arc::new(user_defined),
            parts: Arc::new(vec![
                ObjectPartInfo {
                    actual_size: 4,
                    ..Default::default()
                },
                ObjectPartInfo {
                    actual_size: 5,
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };

        assert_eq!(info.get_actual_size().unwrap(), 9);
    }

    #[test]
    fn get_actual_size_returns_error_when_compressed_parts_missing_and_size_mismatch() {
        let user_defined = {
            let mut map = HashMap::new();
            rustfs_utils::http::insert_str(&mut map, rustfs_utils::http::SUFFIX_COMPRESSION, "zstd".to_string());
            map
        };

        let info = ObjectInfo {
            size: 12,
            actual_size: 0,
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };

        assert!(info.get_actual_size().is_err());
    }

    #[test]
    fn is_encrypted_correct_for_old_version_fileinfo() {
        let mut user_defined: HashMap<String, String> = HashMap::new();

        let metadata = vec![
            ("content-type", "text/plain"),
            ("etag", "e4336b5de4e2180a53fe2e17d03abe4f-4"),
            ("x-minio-internal-actual-size", "67108864"),
            ("x-rustfs-encryption-original-size", "67108864"),
            ("x-rustfs-internal-actual-size", "67108864"),
        ];

        metadata.into_iter().for_each(|(key, value)| {
            user_defined.insert(key.to_string(), value.to_string());
        });

        let info = ObjectInfo {
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };

        assert!(!info.is_encrypted());
    }

    #[test]
    fn is_encrypted_returns_true_when_encryption_metadata_present() {
        let mut user_defined: HashMap<String, String> = HashMap::new();

        let metadata = vec![
            ("content-type", "text/plain"),
            ("etag", "f1c9645dbc14efddc7d8a322685f26eb"),
            ("x-amz-server-side-encryption", "AES256"),
            ("x-rustfs-encryption-algorithm", "AES256"),
            ("x-rustfs-encryption-iv", "Fb9moBlEBRE0D14F"),
            (
                "x-rustfs-encryption-key",
                "QUFBQUFBQUFBQUFBQUFBQTpZQk5sNnNJdmJHWWl3QmxZbCtsMTJlVlZCeXVoVml4UlV4b3JPbTNoRk5odUlYVnBPdlpXNWVyT0FTcklXMWJr",
            ),
            ("x-rustfs-encryption-key-id", "default"),
            ("x-rustfs-encryption-original-size", "10485760"),
        ];

        metadata.into_iter().for_each(|(key, value)| {
            user_defined.insert(key.to_string(), value.to_string());
        });

        let info = ObjectInfo {
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };

        assert!(info.is_encrypted());
    }

    #[test]
    fn is_encrypted_handles_case_insensitive_rustfs_metadata_keys() {
        let mut user_defined: HashMap<String, String> = HashMap::new();
        user_defined.insert("X-Rustfs-Encryption-Key".to_string(), "encrypted-key".to_string());

        let info = ObjectInfo {
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };

        assert!(info.is_encrypted());
    }

    #[test]
    fn decrypt_checksums_reads_plain_object_checksum() {
        let checksum = rustfs_rio::Checksum::new_from_data(rustfs_rio::ChecksumType::CRC32, b"plain-object")
            .expect("test checksum should be valid");
        let checksum_key = checksum.checksum_type.to_string();
        let expected_checksum = checksum.encoded.clone();
        let info = ObjectInfo {
            checksum: Some(checksum.to_bytes(&[])),
            ..Default::default()
        };

        let (checksums, is_multipart) = info
            .decrypt_checksums(0, &HeaderMap::new())
            .expect("plain checksum should decode");

        assert!(!is_multipart);
        assert_eq!(checksums.get(&checksum_key), Some(&expected_checksum));
    }

    #[test]
    fn decrypt_checksums_hides_encrypted_object_checksum_without_decrypt_material() {
        let checksum = rustfs_rio::Checksum::new_from_data(rustfs_rio::ChecksumType::CRC32, b"encrypted-object")
            .expect("test checksum should be valid");
        let info = ObjectInfo {
            checksum: Some(checksum.to_bytes(&[])),
            user_defined: Arc::new(HashMap::from([(
                rustfs_utils::http::headers::AMZ_SERVER_SIDE_ENCRYPTION.to_string(),
                "AES256".to_string(),
            )])),
            ..Default::default()
        };

        let (checksums, is_multipart) = info
            .decrypt_checksums(0, &HeaderMap::new())
            .expect("encrypted checksum should fail closed");

        assert!(!is_multipart);
        assert!(checksums.is_empty());
    }

    #[test]
    fn decrypt_checksums_keeps_encrypted_multipart_flag_false_for_response_paths() {
        let checksum = rustfs_rio::Checksum::new_from_data(rustfs_rio::ChecksumType::CRC32, b"encrypted-object")
            .expect("test checksum should be valid");
        let info = ObjectInfo {
            checksum: Some(checksum.to_bytes(&[])),
            // Multipart ETag shape: md5-of-md5s with a part-count suffix.
            etag: Some("0123456789abcdef0123456789abcdef-3".to_string()),
            user_defined: Arc::new(HashMap::from([(
                rustfs_utils::http::headers::AMZ_SERVER_SIDE_ENCRYPTION.to_string(),
                "AES256".to_string(),
            )])),
            ..Default::default()
        };

        let (checksums, is_multipart) = info
            .decrypt_checksums(0, &HeaderMap::new())
            .expect("encrypted checksum should fail closed");

        // The response path infers COMPOSITE from is_multipart=true when the
        // checksum type is unreadable, so encrypted objects must keep the
        // flag false here even when the object itself is multipart. Callers
        // that need routing (replication) consult is_multipart() directly.
        assert!(checksums.is_empty());
        assert!(!is_multipart);
        assert!(info.is_multipart());
    }

    #[test]
    fn decrypt_checksums_keeps_encrypted_part_checksum_metadata() {
        let checksum = rustfs_rio::Checksum::new_from_data(rustfs_rio::ChecksumType::CRC32, b"encrypted-object")
            .expect("test checksum should be valid");
        let part_checksums = HashMap::from([("x-amz-checksum-crc32".to_string(), "AAAAAA==".to_string())]);
        let info = ObjectInfo {
            checksum: Some(checksum.to_bytes(&[])),
            user_defined: Arc::new(HashMap::from([(
                rustfs_utils::http::headers::AMZ_SERVER_SIDE_ENCRYPTION.to_string(),
                "AES256".to_string(),
            )])),
            parts: Arc::new(vec![ObjectPartInfo {
                number: 2,
                checksums: Some(part_checksums.clone()),
                ..Default::default()
            }]),
            ..Default::default()
        };

        let (checksums, is_multipart) = info
            .decrypt_checksums(2, &HeaderMap::new())
            .expect("part checksum metadata should remain readable");

        assert!(is_multipart);
        assert_eq!(checksums, part_checksums);
    }

    #[test]
    fn objectinfo_clone_shares_arc_data_and_is_correct() {
        let mut ud = HashMap::new();
        ud.insert("content-type".to_string(), "application/octet-stream".to_string());
        ud.insert("x-custom-header".to_string(), "custom-value".to_string());

        let original = ObjectInfo {
            bucket: "test-bucket".to_string(),
            name: "test-object".to_string(),
            user_defined: Arc::new(ud),
            user_tags: Arc::new("env=prod&team=storage".to_string()),
            parts: Arc::new(vec![
                ObjectPartInfo {
                    number: 1,
                    size: 1024,
                    actual_size: 1024,
                    ..Default::default()
                },
                ObjectPartInfo {
                    number: 2,
                    size: 512,
                    actual_size: 512,
                    ..Default::default()
                },
            ]),
            size: 1536,
            etag: Some("abc123".to_string()),
            ..Default::default()
        };

        let cloned = original.clone();

        // Verify cloned values are correct
        assert_eq!(cloned.bucket, "test-bucket");
        assert_eq!(cloned.name, "test-object");
        assert_eq!(cloned.size, 1536);
        assert_eq!(cloned.etag, Some("abc123".to_string()));

        // Verify Arc fields share the same allocation
        assert!(Arc::ptr_eq(&original.user_defined, &cloned.user_defined));
        assert!(Arc::ptr_eq(&original.user_tags, &cloned.user_tags));
        assert!(Arc::ptr_eq(&original.parts, &cloned.parts));

        // Verify Arc-wrapped data is accessible through the clone
        assert_eq!(
            cloned.user_defined.get("content-type").map(String::as_str),
            Some("application/octet-stream")
        );
        assert_eq!(cloned.user_tags.as_str(), "env=prod&team=storage");
        assert_eq!(cloned.parts.len(), 2);
        assert_eq!(cloned.parts[0].number, 1);
        assert_eq!(cloned.parts[1].size, 512);

        // Verify default ObjectInfo clone also works
        let default_obj = ObjectInfo::default();
        let default_cloned = default_obj.clone();
        assert!(default_obj.user_defined.is_empty());
        assert!(default_cloned.user_defined.is_empty());
        assert!(default_cloned.user_tags.is_empty());
        assert!(default_cloned.parts.is_empty());
    }

    #[test]
    fn object_options_default_does_not_allocate_lifecycle_delete_all_journal() {
        let mut opts = ObjectOptions::default();

        assert!(opts.lifecycle_delete_all_journal().is_none());
        opts.ensure_lifecycle_delete_all_journal();
        assert!(opts.lifecycle_delete_all_journal().is_some());
    }
}
