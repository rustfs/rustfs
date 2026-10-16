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

//! Persistent attempt fences for ordinary ECStore PUTs. Other publication
//! paths do not yet participate; these records are not authority for GC.

use super::{metadata::BucketMetadata, metadata_sys};
use crate::config::com::{read_config_observation_no_lock, save_config_no_lock, with_config_object_write_lock};
use crate::disk::{BUCKET_META_PREFIX, RUSTFS_META_BUCKET};
use crate::error::{Error, Result};
use crate::object_api::{NamespaceLockFence, ObjectLockConfigSnapshot, ObjectOptions, WriteCommitGuard};
use crate::set_disk::get_lock_acquire_timeout;
use crate::storage_api_contracts::namespace::NamespaceLocking;
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
        require_local_put_seal_topology(&store).await?;
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

pub(crate) struct ArtifactPutGuard {
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
    store_id: Uuid,
    bucket: String,
    object: String,
    attempt: Option<WriteCommitGuard>,
    lifecycle: Option<BucketIncarnationFenceGuard>,
    bucket_heal: Option<Arc<crate::store::BucketHealScope>>,
}

impl ArtifactRepairScope {
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
        if scope.store_id != store.id {
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
            require_local_put_seal_topology(&store).await?;
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
        store_id: store.id,
        bucket: bucket.to_owned(),
        object: rustfs_utils::path::encode_dir_object(object),
        attempt,
        lifecycle,
        bucket_heal,
    });
    scope.check()?;
    // A seal freezes logical publication, not repair of an existing version.
    // Still validate any persisted record; unreadable or mismatched records fail closed.
    if let (Some(record), Some(attempt)) = (&record, &scope.attempt) {
        record.exists(store, attempt).await?;
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
    /// Seal ordinary ECStore PUTs for one artifact attempt. This does not seal
    /// CopyObject, multipart completion, replication, restore or repair paths.
    pub async fn seal_artifact_attempt_puts(
        self: Arc<Self>,
        bucket: &str,
        expected_incarnation: Uuid,
        attempt_prefix: &str,
    ) -> Result<()> {
        require_local_put_seal_topology(&self).await?;
        if self.ctx.lock_manager().is_disabled() {
            return Err(Error::other("artifact PUT sealing requires namespace locking"));
        }
        let metadata = metadata_sys::get_bucket_incarnation_metadata_in(&self.ctx, bucket).await?;
        if expected_incarnation.is_nil() || metadata.name != bucket || metadata.bucket_incarnation_id != expected_incarnation {
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
                save_config_no_lock(store, &record.path(), serde_json::to_vec(&record)?, &guard).await?;
            }
            if lifecycle.is_lock_lost() || guard.is_lock_lost() {
                return Err(Error::other("artifact PUT seal fence was lost"));
            }
            Ok(())
        })
        .await?
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
