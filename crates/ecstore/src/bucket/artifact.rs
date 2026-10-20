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

//! Persistent artifact attempt fences and exact-version repair retirement.
//! These records do not authorize object deletion or establish GC eligibility.

mod delete;
pub mod inventory;
#[doc(hidden)]
pub use delete::ArtifactDeleteTarget;

use super::versioning::VersioningApi as _;
use super::{metadata::BucketMetadata, metadata_sys};
use crate::config::com::{read_config_observation_no_lock, save_config_no_lock, with_config_object_write_lock};
use crate::disk::{BUCKET_META_PREFIX, RUSTFS_META_BUCKET};
use crate::error::{Error, Result};
use crate::object_api::{NamespaceLockFence, ObjectLockConfigSnapshot, ObjectOptions, WriteCommitGuard};
use crate::set_disk::get_lock_acquire_timeout;
use crate::storage_api_contracts::{
    bucket::{BucketOperations as _, MakeBucketOptions},
    namespace::NamespaceLocking,
};
use crate::store::{BucketIncarnationFenceGuard, ECStore};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PutSealRecord {
    version: u8,
    bucket: String,
    incarnation_id: Uuid,
    base_prefix: String,
    attempt_prefix: String,
}

/// An ordinary PUT target derived from one authoritative bucket metadata snapshot.
/// Fields are private so ObjectOptions cannot fabricate a different namespace.
#[doc(hidden)]
#[derive(Debug)]
pub struct ArtifactPutTarget {
    record: PutSealRecord,
    object: String,
}

impl ArtifactPutTarget {
    pub(crate) fn from_metadata(metadata: &BucketMetadata, object: &str) -> Result<Option<Arc<Self>>> {
        let Some(base_prefix) = metadata.artifact_base_prefix.as_deref() else {
            return Ok(None);
        };
        let attempt_prefix = attempt_prefix(base_prefix, object)?;
        if object.len() == attempt_prefix.len() {
            return Err(Error::other("artifact PUT requires an object within an attempt"));
        }
        Ok(Some(Arc::new(Self {
            record: PutSealRecord::new(metadata, attempt_prefix)?,
            object: object.to_owned(),
        })))
    }

    pub(crate) async fn validate_for_put(
        ctx: &crate::runtime::instance::InstanceContext,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> Result<Option<Uuid>> {
        if super::utils::is_meta_bucketname(bucket) {
            return Ok(None);
        }
        let metadata = metadata_sys::get_bucket_incarnation_metadata_in(ctx, bucket).await?;
        if opts
            .expected_bucket_incarnation_id
            .is_some_and(|expected| expected != metadata.bucket_incarnation_id)
        {
            return Err(Error::BucketNotFound(bucket.to_owned()));
        }
        let store = metadata_sys::object_store_in(ctx).await?;
        store.validate_write_lifecycle_owner(bucket, metadata.bucket_incarnation_id, opts)?;
        match (Self::from_metadata(&metadata, object)?, opts.artifact_put_target.as_deref()) {
            (None, None) => {}
            (Some(expected), Some(actual)) if expected.record == actual.record && expected.object == actual.object => {}
            _ => return Err(Error::other("artifact PUT requires its matching managed target")),
        }
        Ok(Some(metadata.bucket_incarnation_id))
    }

    pub(crate) async fn acquire(
        &self,
        store: Arc<ECStore>,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> Result<ArtifactPutGuard> {
        require_put_seal_topology(&store).await?;
        if store.ctx.lock_manager().is_disabled() {
            return Err(Error::other("artifact PUT requires namespace locking"));
        }
        if self.record.bucket != bucket
            || self.object != object
            || opts.expected_bucket_incarnation_id != Some(self.record.incarnation_id)
        {
            return Err(Error::other("artifact PUT target identity mismatch"));
        }
        // Lock order: lifecycle read -> attempt record write -> target object write.
        // A destructive PUT already owns the lifecycle and metadata guards in
        // its Object Lock snapshot. Retain that owner rather than queueing a
        // recursive read behind a writer that is waiting for this PUT.
        let (lifecycle, snapshot) = if let Some(owner) = opts.bucket_incarnation_fence.as_ref() {
            if !owner.matches(&store, bucket, self.record.incarnation_id) {
                return Err(Error::other("artifact PUT requires an owned lifecycle fence"));
            }
            (Some(Arc::clone(owner)), opts.object_lock_config_snapshot.clone())
        } else if opts.bucket_lifecycle_lock_fence.is_some() {
            let snapshot = opts
                .object_lock_config_snapshot
                .as_ref()
                .filter(|snapshot| snapshot.owns_bucket_lifecycle_fence(store.id, bucket, self.record.incarnation_id))
                .ok_or_else(|| Error::other("artifact PUT requires an owned lifecycle fence"))?;
            (None, Some(Arc::clone(snapshot)))
        } else {
            (
                Some(Arc::new(
                    store
                        .acquire_bucket_incarnation_fence(bucket, self.record.incarnation_id)
                        .await?,
                )),
                None,
            )
        };
        let path = self.record.path();
        let lock = store.new_ns_lock(RUSTFS_META_BUCKET, &path).await?;
        let attempt = WriteCommitGuard::acquire(&lock, get_lock_acquire_timeout()).await?;
        let guard = ArtifactPutGuard {
            identity: self.record.identity(),
            attempt,
            lifecycle,
            _snapshot: snapshot,
            lifecycle_fence: opts.bucket_lifecycle_lock_fence.clone(),
        };
        if guard.is_lock_lost() {
            return Err(Error::other("artifact PUT fence was lost"));
        }
        if self.record.exists(store, &guard.attempt).await? {
            return Err(Error::other("artifact attempt PUTs are sealed"));
        }
        if guard.is_lock_lost() {
            return Err(Error::other("artifact PUT fence was lost"));
        }
        Ok(guard)
    }
}

/// Internal, metadata-derived identity carried to each physical disk writer.
#[derive(Clone, Copy)]
pub(crate) struct ArtifactPutIdentity {
    pub(crate) incarnation: Uuid,
    pub(crate) receiver_key: [u8; 32],
}

pub(crate) struct ArtifactPutGuard {
    pub(crate) identity: ArtifactPutIdentity,
    attempt: WriteCommitGuard,
    lifecycle: Option<Arc<BucketIncarnationFenceGuard>>,
    _snapshot: Option<Arc<ObjectLockConfigSnapshot>>,
    lifecycle_fence: Option<NamespaceLockFence>,
}

impl ArtifactPutGuard {
    pub(crate) fn is_lock_lost(&self) -> bool {
        self.attempt.is_lock_lost()
            || self.lifecycle.as_ref().is_some_and(|guard| guard.is_lock_lost())
            || self.lifecycle_fence.as_ref().is_some_and(NamespaceLockFence::is_lock_lost)
    }
}

/// One object repair owns the same attempt barrier as ordinary publication.
/// The disk mutation lease retains this owner after an async waiter is dropped.
pub(crate) struct ArtifactRepairScope {
    store: Arc<ECStore>,
    artifact: Option<PutSealRecord>,
    bucket: String,
    object: String,
    attempt: Option<WriteCommitGuard>,
    lifecycle: Option<BucketIncarnationFenceGuard>,
    bucket_heal: Option<Arc<crate::store::BucketHealScope>>,
}

impl ArtifactRepairScope {
    pub(crate) fn receiver_identity(&self) -> Option<ArtifactPutIdentity> {
        self.artifact.as_ref().map(PutSealRecord::identity)
    }

    pub(crate) async fn check_destructive_repair(&self) -> crate::disk::error::Result<()> {
        self.check().map_err(crate::disk::error::Error::other)?;
        if self.artifact.is_some() && self.store.ctx.is_dist_erasure().await {
            return Err(crate::disk::error::Error::other("distributed artifact repair deletion is unsupported"));
        }
        Ok(())
    }

    pub(crate) fn check(&self) -> Result<()> {
        if self.attempt.as_ref().is_some_and(WriteCommitGuard::is_lock_lost)
            || self.lifecycle.as_ref().is_some_and(BucketIncarnationFenceGuard::is_lock_lost)
        {
            return Err(Error::other("artifact repair fence was lost"));
        }
        if let Some(scope) = &self.bucket_heal {
            scope.check().map_err(Error::from)?;
        }
        Ok(())
    }

    pub(crate) async fn check_version(&self, version: Option<Uuid>) -> Result<()> {
        self.check()?;
        let Some(attempt) = &self.artifact else {
            return Ok(());
        };
        let version = version
            .filter(|version| !version.is_nil())
            .ok_or_else(|| Error::other("artifact repair requires an exact version"))?;
        let record = ArtifactVersionRetirementRecord::new(attempt, &self.object, version)?;
        // The attempt owner excludes retirement throughout repair, including its
        // physical tail. Each selected version still needs its own record check.
        let lock = self.store.new_ns_lock(RUSTFS_META_BUCKET, &record.path()).await?;
        let guard = WriteCommitGuard::acquire(&lock, get_lock_acquire_timeout()).await?;
        let retired = record.exists(Arc::clone(&self.store), &guard).await?;
        self.check()?;
        if retired {
            return Err(Error::other("artifact object version is retired"));
        }
        Ok(())
    }

    pub(crate) fn lifecycle_guard(&self) -> Option<&rustfs_lock::NamespaceLockGuard> {
        self.lifecycle
            .as_ref()
            .and_then(BucketIncarnationFenceGuard::namespace_lock_guard)
            .or_else(|| self.bucket_heal.as_ref().and_then(|scope| scope.lifecycle_guard()))
    }
}

tokio::task_local! {
    static ARTIFACT_REPAIR_SCOPE: Arc<ArtifactRepairScope>;
}

pub(crate) fn artifact_repair_scope(bucket: &str, object: &str) -> Option<Arc<ArtifactRepairScope>> {
    let object = rustfs_utils::path::encode_dir_object(object);
    ARTIFACT_REPAIR_SCOPE
        .try_with(|scope| (scope.bucket == bucket && scope.object == object).then(|| Arc::clone(scope)))
        .ok()
        .flatten()
}

pub(crate) async fn with_artifact_repair_scope<T>(
    ctx: &crate::runtime::instance::InstanceContext,
    bucket: &str,
    object: &str,
    opts: &rustfs_heal_contracts::heal_channel::HealOpts,
    operation: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    if super::utils::is_meta_bucketname(bucket) {
        return operation.await;
    }
    let store = metadata_sys::object_store_in(ctx).await?;
    if let Some(scope) = artifact_repair_scope(bucket, object) {
        if scope.store.id != store.id {
            return Err(Error::other("artifact repair owner belongs to another store"));
        }
        scope.check()?;
        return operation.await;
    }
    let metadata = metadata_sys::get_bucket_incarnation_metadata_in(ctx, bucket).await?;
    if opts.no_lock {
        return Err(Error::other("repair requires an owned bucket lifecycle fence"));
    }
    let record = match metadata.artifact_base_prefix.as_deref() {
        Some(base_prefix) => {
            require_put_seal_topology(&store).await?;
            if ctx.lock_manager().is_disabled() {
                return Err(Error::other("artifact repair requires namespace locking"));
            }
            Some(PutSealRecord::new(&metadata, attempt_prefix(base_prefix, object)?)?)
        }
        None => None,
    };
    let bucket_heal = crate::store::bucket_heal_scope(bucket);
    let lifecycle = if let Some(scope) = &bucket_heal {
        if scope.store.id != store.id || scope.incarnation != metadata.bucket_incarnation_id {
            return Err(Error::other("artifact repair bucket incarnation mismatch"));
        }
        scope.check().map_err(Error::from)?;
        None
    } else {
        Some(
            store
                .acquire_bucket_incarnation_fence(bucket, metadata.bucket_incarnation_id)
                .await?,
        )
    };
    // Lifecycle -> attempt -> capacity/object -> physical mutation lease.
    let attempt = if let Some(record) = &record {
        let lock = store.new_ns_lock(RUSTFS_META_BUCKET, &record.path()).await?;
        Some(WriteCommitGuard::acquire(&lock, get_lock_acquire_timeout()).await?)
    } else {
        None
    };
    let scope = Arc::new(ArtifactRepairScope {
        store: Arc::clone(&store),
        artifact: record,
        bucket: bucket.to_owned(),
        object: rustfs_utils::path::encode_dir_object(object),
        attempt,
        lifecycle,
        bucket_heal,
    });
    scope.check()?;
    // Ordinary PUT sealing permits repair. A separate repair seal closes admission
    // under the same attempt owner retained by every physical repair tail.
    if let (Some(record), Some(attempt)) = (&scope.artifact, &scope.attempt) {
        record.exists(Arc::clone(&store), attempt).await?;
        let repair_seal = ArtifactRepairSealRecord::new(record);
        let lock = store.new_ns_lock(RUSTFS_META_BUCKET, &repair_seal.path()).await?;
        let guard = WriteCommitGuard::acquire(&lock, get_lock_acquire_timeout()).await?;
        if repair_seal.exists(store, &guard).await? {
            return Err(Error::other("artifact attempt repairs are sealed"));
        }
        scope.check()?;
    }
    ARTIFACT_REPAIR_SCOPE
        .scope(Arc::clone(&scope), async move {
            let result = operation.await;
            scope.check()?;
            result
        })
        .await
}

fn local_put_seal_endpoints(endpoints: &[crate::layout::endpoint::Endpoint]) -> bool {
    !endpoints.is_empty() && endpoints.iter().all(|endpoint| endpoint.is_local)
}

fn put_seal_endpoints(endpoints: &[crate::layout::endpoint::Endpoint], distributed: bool) -> bool {
    if !distributed {
        return local_put_seal_endpoints(endpoints);
    }
    cfg!(unix)
        && !endpoints.is_empty()
        && endpoints
            .iter()
            .all(|endpoint| endpoint.get_type() == crate::layout::endpoint::EndpointType::Url)
}

async fn require_put_seal_topology(store: &ECStore) -> Result<()> {
    let distributed = store.ctx.is_dist_erasure().await;
    // Initialization validates endpoint uniqueness and layout. Check configured
    // members here, not only online disks; seal confirms every receiver later.
    if store.pools.is_empty()
        || store
            .pools
            .iter()
            .any(|pool| !put_seal_endpoints(pool.endpoints.endpoints.as_ref(), distributed))
    {
        return Err(Error::other(
            "artifact PUT sealing requires local storage or a distributed Unix URL topology",
        ));
    }
    Ok(())
}

async fn require_local_put_seal_topology(store: &ECStore) -> Result<()> {
    // Use the configured endpoints retained by every pool, including offline
    // members. Online disk snapshots cannot prove absence of remote writers.
    if store.pools.is_empty()
        || store
            .pools
            .iter()
            .any(|pool| !local_put_seal_endpoints(pool.endpoints.endpoints.as_ref()))
        || store.ctx.is_dist_erasure().await
    {
        return Err(Error::other(
            "artifact PUT sealing requires local-only storage; distributed or remote endpoints are unsupported",
        ));
    }
    Ok(())
}

impl ECStore {
    /// Legacy disk RPCs cannot publish into a managed artifact namespace.
    pub async fn reject_legacy_artifact_mutation(&self, bucket: &str) -> crate::disk::error::Result<()> {
        use crate::disk::error::DiskError;
        if super::utils::has_bad_path_component(bucket) {
            return Err(DiskError::FileAccessDenied);
        }
        if super::utils::is_meta_bucketname(bucket) {
            return Ok(());
        }
        // Receiver classification must not migrate a legacy incarnation or
        // reacquire the bucket namespace lock held by a DeleteBucket caller.
        let (metadata, persisted) = metadata_sys::get_config_from_disk_with_presence_in(&self.ctx, bucket)
            .await
            .map_err(|_| DiskError::FileAccessDenied)?;
        if !persisted || metadata.artifact_base_prefix.is_some() {
            return Err(DiskError::FileAccessDenied);
        }
        Ok(())
    }

    pub(crate) async fn artifact_receiver_identity(
        &self,
        bucket: &str,
        expected: Uuid,
        object: &str,
        seal: bool,
    ) -> Result<ArtifactPutIdentity> {
        let metadata = metadata_sys::get_bucket_incarnation_metadata_in(&self.ctx, bucket).await?;
        if expected.is_nil() || metadata.name != bucket || metadata.bucket_incarnation_id != expected {
            return Err(Error::other("artifact receiver bucket incarnation mismatch"));
        }
        if seal {
            Ok(PutSealRecord::new(&metadata, object)?.identity())
        } else {
            Ok(ArtifactPutTarget::from_metadata(&metadata, object)?
                .ok_or_else(|| Error::other("artifact receiver requires a managed bucket"))?
                .record
                .identity())
        }
    }

    /// Validate a receiver publication without entering the coordinator-owned
    /// attempt write lock. The independent disk lease closes physical admission.
    pub(crate) async fn artifact_repair_receiver_identity(
        &self,
        bucket: &str,
        expected: Uuid,
        object: &str,
        version: Option<Uuid>,
    ) -> Result<(ArtifactPutIdentity, Arc<WriteCommitGuard>)> {
        let metadata = metadata_sys::get_bucket_incarnation_metadata_in(&self.ctx, bucket).await?;
        if expected.is_nil() || metadata.name != bucket || metadata.bucket_incarnation_id != expected {
            return Err(Error::other("artifact repair receiver bucket incarnation mismatch"));
        }
        let target = ArtifactPutTarget::from_metadata(&metadata, object)?
            .ok_or_else(|| Error::other("artifact repair receiver requires a managed bucket"))?;
        let version = version
            .filter(|value| !value.is_nil())
            .ok_or_else(|| Error::other("artifact repair receiver requires an exact version"))?;
        let store = metadata_sys::object_store_in(&self.ctx).await?;
        if store.id != self.id {
            return Err(Error::other("artifact repair receiver belongs to another store"));
        }
        // Check each independent record before taking a disk repair lease. A
        // sealer may hold its record lock while draining admitted disk owners.
        let repair_seal = ArtifactRepairSealRecord::new(&target.record);
        let lock = self.new_ns_lock(RUSTFS_META_BUCKET, &repair_seal.path()).await?;
        let guard = WriteCommitGuard::acquire(&lock, get_lock_acquire_timeout()).await?;
        if repair_seal.exists(Arc::clone(&store), &guard).await? {
            return Err(Error::other("artifact attempt repairs are sealed"));
        }
        drop(guard);
        let retirement = ArtifactVersionRetirementRecord::new(&target.record, object, version)?;
        let lock = self.new_ns_lock(RUSTFS_META_BUCKET, &retirement.path()).await?;
        let guard = WriteCommitGuard::acquire(&lock, get_lock_acquire_timeout()).await?;
        if retirement.exists(store, &guard).await? {
            return Err(Error::other("artifact object version is retired"));
        }
        // Direct receivers do not own the coordinator attempt lock. Retain this
        // version fence through physical publication so retirement cannot pass
        // an admitted receiver whose RPC waiter has already gone away.
        Ok((target.record.identity(), Arc::new(guard)))
    }

    /// Create a dedicated versioned bucket and observe its current incarnation.
    /// A successful receipt does not assert that this request created that generation.
    pub async fn make_artifact_bucket(self: Arc<Self>, bucket: &str, base_prefix: &str) -> Result<Uuid> {
        require_put_seal_topology(&self).await?;
        if self.ctx.lock_manager().is_disabled() {
            return Err(Error::other("artifact bucket creation requires namespace locking"));
        }
        self.make_bucket(
            bucket,
            &MakeBucketOptions {
                artifact_base_prefix: Some(base_prefix.to_owned()),
                versioning_enabled: true,
                ..Default::default()
            },
        )
        .await?;

        self.observe_artifact_bucket_incarnation(bucket, base_prefix).await
    }

    /// Observe an existing managed bucket without creating or migrating metadata.
    /// This identity must still be checked by every subsequent storage operation.
    pub async fn inspect_artifact_bucket(&self, bucket: &str, base_prefix: &str) -> Result<Uuid> {
        require_put_seal_topology(self).await?;
        if self.ctx.lock_manager().is_disabled() {
            return Err(Error::other("artifact bucket inspection requires namespace locking"));
        }
        self.observe_artifact_bucket_incarnation(bucket, base_prefix).await
    }

    pub(crate) async fn observe_artifact_bucket_incarnation(&self, bucket: &str, base_prefix: &str) -> Result<Uuid> {
        // The lifecycle read lock prevents replacement during this observation.
        // Metadata is one authoritative snapshot, possibly already cached after
        // a committed write; a cache miss acquires its own metadata locks.
        let lifecycle = self.acquire_bucket_lifecycle_read_lock(bucket).await?;
        // Unlike the incarnation getter, get_config never migrates a legacy
        // identity or completes a pending creation. Fabricated defaults fail
        // the committed identity checks below.
        let sys = metadata_sys::require_bucket_metadata_sys_in(&self.ctx)?;
        let sys = sys.read().await.clone();
        let (metadata, _) = sys.get_config(bucket).await?;
        if metadata.name != bucket
            || !metadata.bucket_creation_committed
            || !metadata.bucket_incarnation_sidecar
            || metadata.bucket_incarnation_id.is_nil()
            || metadata.artifact_base_prefix.as_deref() != Some(base_prefix)
            || !metadata.versioning_config.as_ref().is_some_and(|config| {
                config.enabled()
                    && !config.exclude_folders.unwrap_or(false)
                    && config.excluded_prefixes.as_ref().is_none_or(Vec::is_empty)
            })
        {
            return Err(Error::other("artifact bucket identity could not be confirmed"));
        }
        if lifecycle.is_lock_lost() {
            return Err(Error::other("artifact bucket creation observation fence was lost"));
        }
        Ok(metadata.bucket_incarnation_id)
    }

    pub(crate) fn validate_write_lifecycle_owner(&self, bucket: &str, incarnation: Uuid, opts: &ObjectOptions) -> Result<()> {
        let invalid = || Error::other("write requires an owned bucket lifecycle fence");
        if let Some(owner) = &opts.bucket_incarnation_fence {
            if !owner.matches(self, bucket, incarnation)
                || opts
                    .object_lock_config_snapshot
                    .as_ref()
                    .is_some_and(|snapshot| !snapshot.is_valid_for_destructive_put(self.id, bucket, incarnation))
            {
                return Err(invalid());
            }
        } else if let Some(snapshot) = &opts.object_lock_config_snapshot {
            if !snapshot.owns_bucket_lifecycle_fence(self.id, bucket, incarnation) {
                return Err(invalid());
            }
        } else if opts.bucket_lifecycle_lock_fence.is_some() {
            return Err(invalid());
        }
        Ok(())
    }

    pub(crate) async fn bind_artifact_write_metadata(
        &self,
        bucket: &str,
        opts: &mut ObjectOptions,
    ) -> Result<Option<Arc<BucketMetadata>>> {
        if super::utils::is_meta_bucketname(bucket) {
            return Ok(None);
        }
        let metadata = metadata_sys::get_bucket_incarnation_metadata_in(&self.ctx, bucket).await?;
        if opts
            .expected_bucket_incarnation_id
            .is_some_and(|expected| expected != metadata.bucket_incarnation_id)
        {
            return Err(Error::BucketNotFound(bucket.to_owned()));
        }
        // The classification and subsequent commit must refer to the same generation.
        opts.expected_bucket_incarnation_id = Some(metadata.bucket_incarnation_id);
        // A loss signal cannot authorize skipping the downstream generation lock.
        // Keep a matching owner in opts, including for ordinary PUT and Copy.
        self.validate_write_lifecycle_owner(bucket, metadata.bucket_incarnation_id, opts)?;
        if let Some(owner) = opts.bucket_incarnation_fence.clone() {
            owner.attach_owner(opts);
        }
        if let Some(snapshot) = opts.object_lock_config_snapshot.clone() {
            snapshot.add_lock_fences(opts);
        }
        Ok(Some(metadata))
    }

    pub(crate) async fn prepare_unsupported_artifact_write(
        &self,
        bucket: &str,
        opts: &ObjectOptions,
    ) -> Result<(ObjectOptions, Option<Arc<BucketIncarnationFenceGuard>>)> {
        let mut opts = opts.clone();
        let Some(metadata) = self.bind_artifact_write_metadata(bucket, &mut opts).await? else {
            return Ok((opts, None));
        };
        reject_unsupported_artifact_write(&metadata)?;
        // Lifecycle read -> metadata transaction read -> downstream object lock.
        // Binding above verified and retained any supplied snapshot's actual owner.
        let guard = if opts.object_lock_config_snapshot.is_some() || opts.bucket_incarnation_fence.is_some() {
            None
        } else {
            Some(Arc::new(
                self.acquire_bucket_incarnation_fence(bucket, metadata.bucket_incarnation_id)
                    .await?,
            ))
        };
        if let Some(guard) = guard.as_ref() {
            guard.attach_owner(&mut opts);
        }
        let current = metadata_sys::get_bucket_incarnation_id_for_options_in(&self.ctx, bucket, &opts).await?;
        if opts.expected_bucket_incarnation_id != Some(current) {
            return Err(Error::BucketNotFound(bucket.to_owned()));
        }
        if opts
            .bucket_lifecycle_lock_fence
            .as_ref()
            .is_some_and(NamespaceLockFence::is_lock_lost)
        {
            return Err(Error::other("bucket lifecycle lock was lost before the write"));
        }
        Ok((opts, guard))
    }
}

pub(crate) fn reject_unsupported_artifact_write(metadata: &BucketMetadata) -> Result<()> {
    if metadata.artifact_base_prefix.is_some() {
        return Err(Error::other("artifact bucket does not support this write operation"));
    }
    Ok(())
}

impl PutSealRecord {
    fn identity(&self) -> ArtifactPutIdentity {
        // Stable disk key, independent of JSON field order and seal serialization.
        let mut digest = Sha256::new();
        digest.update(b"rustfs-artifact-put-receiver-v1\0");
        digest.update((self.bucket.len() as u64).to_be_bytes());
        digest.update(self.bucket.as_bytes());
        digest.update(self.incarnation_id.as_bytes());
        digest.update((self.attempt_prefix.len() as u64).to_be_bytes());
        digest.update(self.attempt_prefix.as_bytes());
        ArtifactPutIdentity {
            incarnation: self.incarnation_id,
            receiver_key: digest.finalize().into(),
        }
    }

    fn new(metadata: &BucketMetadata, attempt_prefix: &str) -> Result<Self> {
        let base_prefix = metadata
            .artifact_base_prefix
            .as_ref()
            .ok_or_else(|| Error::other("bucket is not an artifact bucket"))?;
        if metadata.bucket_incarnation_id.is_nil()
            || attempt_prefix.len() >= 1024
            || self::attempt_prefix(base_prefix, attempt_prefix)? != attempt_prefix
        {
            return Err(Error::other("invalid artifact attempt identity"));
        }
        Ok(Self {
            version: 1,
            bucket: metadata.name.clone(),
            incarnation_id: metadata.bucket_incarnation_id,
            base_prefix: base_prefix.clone(),
            attempt_prefix: attempt_prefix.to_owned(),
        })
    }

    fn path(&self) -> String {
        artifact_put_seal_record_path(&self.bucket, self.incarnation_id, &self.attempt_prefix)
    }

    async fn exists(&self, store: Arc<ECStore>, guard: &WriteCommitGuard) -> Result<bool> {
        let bytes = match read_config_observation_no_lock(store, &self.path(), 16 * 1024, guard).await {
            Ok((bytes, _)) => bytes,
            Err(Error::ConfigNotFound) => return Ok(false),
            Err(error) => return Err(error),
        };
        let record: Self = serde_json::from_slice(&bytes)?;
        if record != *self {
            return Err(Error::other("artifact PUT seal identity mismatch"));
        }
        Ok(true)
    }
}

pub(crate) fn artifact_put_seal_record_path(bucket: &str, incarnation: Uuid, attempt_prefix: &str) -> String {
    let digest = hex_simd::encode_to_string(Sha256::digest(attempt_prefix.as_bytes()), hex_simd::AsciiCase::Lower);
    format!("{BUCKET_META_PREFIX}/{bucket}/artifact-put-seals/{incarnation}/{digest}.json")
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactRepairSealRecord {
    format_version: u8,
    bucket: String,
    incarnation_id: Uuid,
    base_prefix: String,
    attempt_prefix: String,
}

impl ArtifactRepairSealRecord {
    fn new(attempt: &PutSealRecord) -> Self {
        Self {
            format_version: 1,
            bucket: attempt.bucket.clone(),
            incarnation_id: attempt.incarnation_id,
            base_prefix: attempt.base_prefix.clone(),
            attempt_prefix: attempt.attempt_prefix.clone(),
        }
    }

    fn path(&self) -> String {
        artifact_repair_seal_record_path(&self.bucket, self.incarnation_id, &self.attempt_prefix)
    }

    async fn exists(&self, store: Arc<ECStore>, guard: &WriteCommitGuard) -> Result<bool> {
        let bytes = match read_config_observation_no_lock(store, &self.path(), 16 * 1024, guard).await {
            Ok((bytes, _)) => bytes,
            Err(Error::ConfigNotFound) => return Ok(false),
            Err(error) => return Err(error),
        };
        let actual: Self = serde_json::from_slice(&bytes)?;
        if actual != *self {
            return Err(Error::other("artifact repair seal identity mismatch"));
        }
        Ok(true)
    }
}

pub(crate) fn artifact_repair_seal_record_path(bucket: &str, incarnation: Uuid, attempt_prefix: &str) -> String {
    let digest = hex_simd::encode_to_string(Sha256::digest(attempt_prefix.as_bytes()), hex_simd::AsciiCase::Lower);
    format!("{BUCKET_META_PREFIX}/{bucket}/artifact-repair-seals/{incarnation}/{digest}.json")
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactVersionRetirementRecord {
    format_version: u8,
    bucket: String,
    incarnation_id: Uuid,
    base_prefix: String,
    attempt_prefix: String,
    object: String,
    version_id: Uuid,
}

impl ArtifactVersionRetirementRecord {
    fn new(attempt: &PutSealRecord, object: &str, version_id: Uuid) -> Result<Self> {
        if version_id.is_nil()
            || object.len() <= attempt.attempt_prefix.len()
            || attempt_prefix(&attempt.base_prefix, object)? != attempt.attempt_prefix
        {
            return Err(Error::other("invalid artifact version retirement target"));
        }
        Ok(Self {
            format_version: 1,
            bucket: attempt.bucket.clone(),
            incarnation_id: attempt.incarnation_id,
            base_prefix: attempt.base_prefix.clone(),
            attempt_prefix: attempt.attempt_prefix.clone(),
            object: rustfs_utils::path::encode_dir_object(object),
            version_id,
        })
    }

    fn path(&self) -> String {
        artifact_version_retirement_record_path(
            &self.bucket,
            self.incarnation_id,
            &self.attempt_prefix,
            &self.object,
            self.version_id,
        )
    }

    async fn exists(&self, store: Arc<ECStore>, guard: &WriteCommitGuard) -> Result<bool> {
        let bytes = match read_config_observation_no_lock(store, &self.path(), 16 * 1024, guard).await {
            Ok((bytes, _)) => bytes,
            Err(Error::ConfigNotFound) => return Ok(false),
            Err(error) => return Err(error),
        };
        let actual: Self = serde_json::from_slice(&bytes)?;
        if actual != *self {
            return Err(Error::other("artifact version retirement identity mismatch"));
        }
        Ok(true)
    }
}

pub(crate) fn artifact_version_retirement_record_path(
    bucket: &str,
    incarnation: Uuid,
    attempt_prefix: &str,
    object: &str,
    version: Uuid,
) -> String {
    let attempt = hex_simd::encode_to_string(Sha256::digest(attempt_prefix.as_bytes()), hex_simd::AsciiCase::Lower);
    let object = rustfs_utils::path::encode_dir_object(object);
    let object = hex_simd::encode_to_string(Sha256::digest(object.as_bytes()), hex_simd::AsciiCase::Lower);
    format!("{BUCKET_META_PREFIX}/{bucket}/artifact-version-retirements/{incarnation}/{attempt}/{object}/{version}.json")
}

fn attempt_prefix<'a>(base_prefix: &str, object: &'a str) -> Result<&'a str> {
    let invalid = || Error::other("invalid artifact attempt prefix");
    let suffix = object.strip_prefix(base_prefix).ok_or_else(invalid)?;
    let mut components = suffix.splitn(4, '/');
    let tenant = components.next().ok_or_else(invalid)?;
    let run = components.next().ok_or_else(invalid)?;
    let attempt = components.next().ok_or_else(invalid)?;
    components.next().ok_or_else(invalid)?;
    if tenant.is_empty()
        || tenant.len() % 2 != 0
        || !tenant
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid());
    }
    for value in [run, attempt] {
        let id = Uuid::parse_str(value).map_err(|_| invalid())?;
        if id.is_nil() || id.hyphenated().to_string() != value {
            return Err(invalid());
        }
    }
    Ok(&object[..base_prefix.len() + tenant.len() + run.len() + attempt.len() + 3])
}

impl ECStore {
    /// Seal repair admission for one ordinary-PUT-sealed artifact attempt.
    /// This neither deletes versions nor proves all writers or GC eligibility.
    pub async fn seal_artifact_attempt_repairs(
        self: Arc<Self>,
        bucket: &str,
        expected_incarnation: Uuid,
        attempt_prefix: &str,
    ) -> Result<()> {
        require_put_seal_topology(&self).await?;
        if self.ctx.lock_manager().is_disabled() {
            return Err(Error::other("artifact repair sealing requires namespace locking"));
        }
        let metadata = metadata_sys::get_bucket_incarnation_metadata_in(&self.ctx, bucket).await?;
        if expected_incarnation.is_nil() || metadata.name != bucket || metadata.bucket_incarnation_id != expected_incarnation {
            return Err(Error::other("artifact repair seal bucket incarnation mismatch"));
        }
        let seal = PutSealRecord::new(&metadata, attempt_prefix)?;
        let record = ArtifactRepairSealRecord::new(&seal);
        let lifecycle = self.acquire_bucket_incarnation_fence(bucket, expected_incarnation).await?;
        let store = Arc::clone(&self);
        // Lifecycle -> attempt -> repair seal record. Acquiring the attempt waits
        // for physical repair owners, including tails whose waiter was cancelled.
        // The spawned transaction retains these owners until persistence drains.
        with_config_object_write_lock(self, seal.path(), move |attempt| async move {
            if lifecycle.is_lock_lost() || attempt.is_lock_lost() {
                return Err(Error::other("artifact repair seal fence was lost"));
            }
            if !seal.exists(Arc::clone(&store), &attempt).await? {
                return Err(Error::other("artifact repair sealing requires a sealed attempt"));
            }
            let lock = store.new_ns_lock(RUSTFS_META_BUCKET, &record.path()).await?;
            let guard = WriteCommitGuard::acquire(&lock, get_lock_acquire_timeout()).await?;
            if !record.exists(Arc::clone(&store), &guard).await? {
                save_config_no_lock(Arc::clone(&store), &record.path(), serde_json::to_vec(&record)?, &guard).await?;
            }
            Box::pin(store.close_artifact_receivers(&seal, crate::disk::local::ArtifactLeaseDomain::Repair)).await?;
            if lifecycle.is_lock_lost() || attempt.is_lock_lost() || guard.is_lock_lost() {
                return Err(Error::other("artifact repair seal fence was lost"));
            }
            Ok(())
        })
        .await?
    }

    /// Persist repair retirement for one exact version in a sealed attempt.
    /// This does not delete the version or authorize garbage collection.
    pub async fn retire_artifact_object_version(
        self: Arc<Self>,
        bucket: &str,
        expected_incarnation: Uuid,
        attempt_prefix: &str,
        object: &str,
        version: Uuid,
    ) -> Result<()> {
        require_local_put_seal_topology(&self).await?;
        if self.ctx.lock_manager().is_disabled() {
            return Err(Error::other("artifact version retirement requires namespace locking"));
        }
        let metadata = metadata_sys::get_bucket_incarnation_metadata_in(&self.ctx, bucket).await?;
        if expected_incarnation.is_nil() || metadata.name != bucket || metadata.bucket_incarnation_id != expected_incarnation {
            return Err(Error::other("artifact version retirement bucket incarnation mismatch"));
        }
        let seal = PutSealRecord::new(&metadata, attempt_prefix)?;
        // Validate logical membership before applying the storage directory encoding.
        let record = ArtifactVersionRetirementRecord::new(&seal, object, version)?;
        let lifecycle = self.acquire_bucket_incarnation_fence(bucket, expected_incarnation).await?;
        let store = Arc::clone(&self);
        // Lifecycle -> attempt -> retirement record. The spawned transaction
        // retains both outer owners when the request waiter is cancelled.
        with_config_object_write_lock(self, seal.path(), move |attempt| async move {
            if lifecycle.is_lock_lost() || attempt.is_lock_lost() {
                return Err(Error::other("artifact version retirement fence was lost"));
            }
            if !seal.exists(Arc::clone(&store), &attempt).await? {
                return Err(Error::other("artifact version retirement requires a sealed attempt"));
            }
            let lock = store.new_ns_lock(RUSTFS_META_BUCKET, &record.path()).await?;
            let guard = WriteCommitGuard::acquire(&lock, get_lock_acquire_timeout()).await?;
            if !record.exists(Arc::clone(&store), &guard).await? {
                save_config_no_lock(store, &record.path(), serde_json::to_vec(&record)?, &guard).await?;
            }
            if lifecycle.is_lock_lost() || attempt.is_lock_lost() || guard.is_lock_lost() {
                return Err(Error::other("artifact version retirement fence was lost"));
            }
            Ok(())
        })
        .await?
    }
}

type ArtifactReceiverSnapshot = std::collections::BTreeMap<String, (crate::disk::DiskStore, Uuid)>;

impl ECStore {
    async fn artifact_receiver_inventory(&self) -> Result<std::collections::BTreeMap<String, crate::disk::DiskStore>> {
        use crate::disk::DiskAPI as _;
        let mut disks = std::collections::BTreeMap::new();
        // Keep every configured slot, including offline members. A quorum is
        // insufficient to close a receiver that may still own physical work.
        for pool in &self.pools {
            for set in &pool.disk_set {
                let members = set.disk_inventory().await;
                if members.len() != set.set_drive_count {
                    return Err(Error::other("artifact seal configured disk count mismatch"));
                }
                for disk in members {
                    let disk = disk.ok_or_else(|| Error::other("artifact seal requires every configured disk"))?;
                    if disks.insert(disk.endpoint().to_string(), disk).is_some() {
                        return Err(Error::other("artifact seal disk topology is ambiguous"));
                    }
                }
            }
        }
        let mut configured = std::collections::BTreeSet::new();
        for endpoint in self.pools.iter().flat_map(|pool| pool.endpoints.endpoints.as_ref()) {
            if !configured.insert(endpoint.to_string()) {
                return Err(Error::other("artifact seal configured disk topology is ambiguous"));
            }
        }
        if configured.is_empty() || disks.keys().cloned().collect::<std::collections::BTreeSet<_>>() != configured {
            return Err(Error::other("artifact seal configured disk topology mismatch"));
        }
        Ok(disks)
    }

    async fn capture_artifact_receivers(&self) -> Result<ArtifactReceiverSnapshot> {
        use crate::disk::Disk;
        let mut receivers = ArtifactReceiverSnapshot::new();
        let mut identities = std::collections::BTreeSet::new();
        for (endpoint, disk) in self.artifact_receiver_inventory().await? {
            // Remote IDs are expectations only. The signed close response must
            // bind that expectation to the actual receiving disk on every call.
            let expected = disk
                .cached_disk_id()
                .await
                .filter(|id| !id.is_nil())
                .ok_or_else(|| Error::other("artifact seal disk identity is unavailable"))?;
            if let Disk::Local(local) = disk.as_ref()
                && local.artifact_disk_id().await.map_err(Error::other)? != expected
            {
                return Err(Error::other("artifact seal physical disk identity changed"));
            }
            if !identities.insert(expected) {
                return Err(Error::other("artifact seal disk identities are ambiguous"));
            }
            receivers.insert(endpoint, (disk, expected));
        }
        Ok(receivers)
    }

    async fn close_artifact_receiver_snapshot(
        &self,
        record: &PutSealRecord,
        domain: crate::disk::local::ArtifactLeaseDomain,
        receivers: &ArtifactReceiverSnapshot,
    ) -> Result<()> {
        use crate::disk::{Disk, local::ArtifactLeaseDomain};
        let key = record.identity().receiver_key;
        futures::future::try_join_all(receivers.values().map(|(disk, expected)| async move {
            match (disk.as_ref(), domain) {
                (Disk::Local(local), domain) => {
                    if local.artifact_disk_id().await.map_err(Error::other)? != *expected {
                        return Err(Error::other("artifact seal physical disk identity changed"));
                    }
                    match domain {
                        ArtifactLeaseDomain::Put => local.seal_artifact_puts(key).await,
                        ArtifactLeaseDomain::Repair => local.seal_artifact_repairs(key).await,
                    }
                    .map_err(Error::other)?;
                    if local.artifact_disk_id().await.map_err(Error::other)? != *expected {
                        return Err(Error::other("artifact seal physical disk identity changed"));
                    }
                    Ok(())
                }
                (Disk::Remote(remote), ArtifactLeaseDomain::Put) => remote
                    .seal_artifact_puts(*expected, &record.bucket, record.incarnation_id, &record.attempt_prefix)
                    .await
                    .map_err(Error::other),
                (Disk::Remote(remote), ArtifactLeaseDomain::Repair) => remote
                    .seal_artifact_repairs(*expected, &record.bucket, record.incarnation_id, &record.attempt_prefix)
                    .await
                    .map_err(Error::other),
            }
        }))
        .await?;
        Ok(())
    }

    async fn verify_artifact_receiver_snapshot(
        &self,
        record: &PutSealRecord,
        receivers: &ArtifactReceiverSnapshot,
    ) -> Result<()> {
        use crate::disk::Disk;
        for (endpoint, current) in self.artifact_receiver_inventory().await? {
            if !receivers
                .get(&endpoint)
                .is_some_and(|(captured, _)| Arc::ptr_eq(&current, captured))
            {
                return Err(Error::other("artifact seal receiver membership changed"));
            }
        }
        futures::future::try_join_all(receivers.values().map(|(disk, expected)| async move {
            if disk.cached_disk_id().await != Some(*expected) {
                return Err(Error::other("artifact seal receiver disk identity changed"));
            }
            match disk.as_ref() {
                Disk::Local(local) => {
                    if local.artifact_disk_id().await.map_err(Error::other)? != *expected {
                        return Err(Error::other("artifact seal physical disk identity changed"));
                    }
                }
                Disk::Remote(remote) => {
                    // Another endpoint may have taken a long time to drain.
                    // Confirm this receiver again with a fresh signed physical
                    // identity response; its cached UUID cannot prove that.
                    remote
                        .seal_artifact_repairs(*expected, &record.bucket, record.incarnation_id, &record.attempt_prefix)
                        .await
                        .map_err(Error::other)?;
                }
            }
            Ok(())
        }))
        .await?;
        for (endpoint, current) in self.artifact_receiver_inventory().await? {
            if !receivers
                .get(&endpoint)
                .is_some_and(|(captured, _)| Arc::ptr_eq(&current, captured))
            {
                return Err(Error::other("artifact seal receiver membership changed"));
            }
        }
        Ok(())
    }

    async fn close_artifact_receivers(
        &self,
        record: &PutSealRecord,
        domain: crate::disk::local::ArtifactLeaseDomain,
    ) -> Result<()> {
        let receivers = self.capture_artifact_receivers().await?;
        self.close_artifact_receiver_snapshot(record, domain, &receivers).await
    }

    /// Permanently close and drain the managed artifact profile's PUT and
    /// repair domains on one captured configured receiver set. This is not
    /// exact-version retirement, physical absence, or GC authorization.
    pub async fn seal_artifact_attempt_writes(
        self: Arc<Self>,
        bucket: &str,
        expected_incarnation: Uuid,
        attempt_prefix: &str,
    ) -> Result<()> {
        Box::pin(async move {
            require_put_seal_topology(&self).await?;
            if self.ctx.lock_manager().is_disabled() {
                return Err(Error::other("artifact writes sealing requires namespace locking"));
            }
            let metadata = metadata_sys::get_bucket_incarnation_metadata_in(&self.ctx, bucket).await?;
            if expected_incarnation.is_nil() || metadata.name != bucket || metadata.bucket_incarnation_id != expected_incarnation
            {
                return Err(Error::other("artifact writes seal bucket incarnation mismatch"));
            }
            let put = PutSealRecord::new(&metadata, attempt_prefix)?;
            let repair = ArtifactRepairSealRecord::new(&put);
            let lifecycle = self.acquire_bucket_incarnation_fence(bucket, expected_incarnation).await?;
            let store = Arc::clone(&self);
            // Lifecycle -> attempt -> repair record. The owned transaction and
            // disk lease workers survive cancellation of the request waiter.
            with_config_object_write_lock(self, put.path(), move |attempt| async move {
                if lifecycle.is_lock_lost() || attempt.is_lock_lost() {
                    return Err(Error::other("artifact writes seal fence was lost"));
                }
                let receivers = store.capture_artifact_receivers().await?;
                if !put.exists(Arc::clone(&store), &attempt).await? {
                    save_config_no_lock(Arc::clone(&store), &put.path(), serde_json::to_vec(&put)?, &attempt).await?;
                }
                store
                    .close_artifact_receiver_snapshot(&put, crate::disk::local::ArtifactLeaseDomain::Put, &receivers)
                    .await?;
                let lock = store.new_ns_lock(RUSTFS_META_BUCKET, &repair.path()).await?;
                let guard = WriteCommitGuard::acquire(&lock, get_lock_acquire_timeout()).await?;
                if !repair.exists(Arc::clone(&store), &guard).await? {
                    save_config_no_lock(Arc::clone(&store), &repair.path(), serde_json::to_vec(&repair)?, &guard).await?;
                }
                store
                    .close_artifact_receiver_snapshot(&put, crate::disk::local::ArtifactLeaseDomain::Repair, &receivers)
                    .await?;
                store.verify_artifact_receiver_snapshot(&put, &receivers).await?;
                if lifecycle.is_lock_lost() || attempt.is_lock_lost() || guard.is_lock_lost() {
                    return Err(Error::other("artifact writes seal fence was lost"));
                }
                Ok(())
            })
            .await?
        })
        .await
    }

    /// Seal ordinary ECStore PUTs for one artifact attempt. This does not seal
    /// CopyObject, multipart completion, replication, restore or repair paths.
    pub async fn seal_artifact_attempt_puts(
        self: Arc<Self>,
        bucket: &str,
        expected_incarnation: Uuid,
        attempt_prefix: &str,
    ) -> Result<()> {
        // Keep the transaction and receiver fan-out out of caller future layouts.
        Box::pin(async move {
            require_put_seal_topology(&self).await?;
            if self.ctx.lock_manager().is_disabled() {
                return Err(Error::other("artifact PUT sealing requires namespace locking"));
            }
            let metadata = metadata_sys::get_bucket_incarnation_metadata_in(&self.ctx, bucket).await?;
            if expected_incarnation.is_nil() || metadata.name != bucket || metadata.bucket_incarnation_id != expected_incarnation
            {
                return Err(Error::other("artifact PUT seal bucket incarnation mismatch"));
            }
            let record = PutSealRecord::new(&metadata, attempt_prefix)?;
            let lifecycle = self.acquire_bucket_incarnation_fence(bucket, expected_incarnation).await?;
            let store = Arc::clone(&self);
            // The config transaction owns the closure and lifecycle guard even if
            // its request waiter is cancelled. Its save drains the physical tail.
            with_config_object_write_lock(Arc::clone(&self), record.path(), move |guard| async move {
                if lifecycle.is_lock_lost() || guard.is_lock_lost() {
                    return Err(Error::other("artifact PUT seal fence was lost"));
                }
                if !record.exists(Arc::clone(&store), &guard).await? {
                    save_config_no_lock(Arc::clone(&store), &record.path(), serde_json::to_vec(&record)?, &guard).await?;
                }
                Box::pin(store.close_artifact_receivers(&record, crate::disk::local::ArtifactLeaseDomain::Put)).await?;
                if lifecycle.is_lock_lost() || guard.is_lock_lost() {
                    return Err(Error::other("artifact PUT seal fence was lost"));
                }
                Ok(())
            })
            .await?
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artifact_put_seal_topology_requires_configured_local_endpoints() {
        use crate::layout::endpoint::Endpoint;

        let local = Endpoint::try_from("/tmp/artifact-local-disk").unwrap();
        // No remote Disk or online connection exists: the configuration alone
        // must reject this member rather than losing it in an online snapshot.
        let remote = Endpoint::try_from("http://192.0.2.1:9000/artifact-remote-disk").unwrap();
        assert!(local_put_seal_endpoints(std::slice::from_ref(&local)));
        assert!(!local_put_seal_endpoints(&[]), "ARTIFACT_PUT_LOCAL_TOPOLOGY_REQUIRED");
        assert!(!local_put_seal_endpoints(&[local, remote]), "ARTIFACT_PUT_LOCAL_TOPOLOGY_REQUIRED");
    }

    #[test]
    fn artifact_put_seal_topology_matches_endpoint_and_lock_modes() {
        use crate::layout::endpoint::Endpoint;

        let local = Endpoint::try_from("/tmp/artifact-local-disk").expect("local endpoint");
        let mut local_url = Endpoint::try_from("http://127.0.0.1:9000/disk").expect("local URL endpoint");
        local_url.is_local = true;
        let remote = Endpoint::try_from("http://192.0.2.1:9000/disk").expect("remote endpoint");

        assert!(put_seal_endpoints(std::slice::from_ref(&local), false));
        assert!(put_seal_endpoints(std::slice::from_ref(&local_url), false));
        assert!(
            !put_seal_endpoints(std::slice::from_ref(&local), true),
            "dist flag cannot convert file endpoints"
        );
        assert!(
            !put_seal_endpoints(std::slice::from_ref(&remote), false),
            "remote writers require distributed locks"
        );
        assert!(
            !put_seal_endpoints(&[local, remote.clone()], true),
            "mixed file and URL endpoints are unsupported"
        );
        assert!(!put_seal_endpoints(&[], false));
        assert!(!put_seal_endpoints(&[], true));
        // The configured remote member needs no online Disk to be included.
        assert_eq!(put_seal_endpoints(&[local_url, remote], true), cfg!(unix));
    }

    #[test]
    fn artifact_put_seal_attempt_identity_requires_canonical_components() {
        let run = "12345678-1234-4234-8234-123456789abc";
        let attempt = "23456789-1234-4234-8234-123456789abc";
        let prefix = format!("outputs/6169/{run}/{attempt}/");
        assert_eq!(attempt_prefix("outputs/", &format!("{prefix}image.png")).unwrap(), prefix);
        assert_eq!(attempt_prefix("outputs/", &prefix).unwrap(), prefix);
        for tenant in ["", "f", "GG", "AB", ".."] {
            assert!(attempt_prefix("outputs/", &format!("outputs/{tenant}/{run}/{attempt}/image.png")).is_err());
        }
        for invalid_run in [
            "00000000-0000-0000-0000-000000000000",
            "12345678123442348234123456789abc",
            "../",
        ] {
            assert!(attempt_prefix("outputs/", &format!("outputs/6169/{invalid_run}/{attempt}/image.png")).is_err());
        }
        assert!(attempt_prefix("outputs/", &format!("elsewhere/6169/{run}/{attempt}/image.png")).is_err());
        assert!(attempt_prefix("outputs/", prefix.trim_end_matches('/')).is_err());
    }
}
