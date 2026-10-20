// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::storage_api_contracts::object::ObjectOperations as _;

/// In-process authority for exactly one drained managed version. No caller can
/// construct this capability or serialize it into a disk/RPC request.
#[doc(hidden)]
pub struct ArtifactDeleteTarget {
    store: Uuid,
    record: ArtifactVersionRetirementRecord,
    lifecycle: Arc<rustfs_lock::NamespaceLockGuard>,
    attempt: WriteCommitGuard,
    repair: WriteCommitGuard,
}

impl ArtifactDeleteTarget {
    fn check_live(&self) -> Result<()> {
        if self.lifecycle.is_lock_lost() || self.attempt.is_lock_lost() || self.repair.is_lock_lost() {
            return Err(Error::other("artifact exact delete owned fence was lost"));
        }
        Ok(())
    }

    pub(crate) fn lifecycle(&self) -> Arc<rustfs_lock::NamespaceLockGuard> {
        Arc::clone(&self.lifecycle)
    }

    pub(crate) fn validate(&self, store: &ECStore, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        if self.store != store.id
            || bucket != self.record.bucket
            || rustfs_utils::path::encode_dir_object(object) != self.record.object
            || opts.expected_bucket_incarnation_id != Some(self.record.incarnation_id)
            || opts.version_id.as_deref() != Some(self.record.version_id.to_string().as_str())
            || !opts.versioned
            || opts.version_suspended
            || opts.delete_prefix
            || opts.delete_marker
            || opts.data_movement
            || opts.lifecycle_delete_all.is_some()
            || self.lifecycle.is_lock_lost()
            || self.attempt.is_lock_lost()
            || self.repair.is_lock_lost()
        {
            return Err(Error::other("artifact exact delete target or owned fence mismatch"));
        }
        Ok(())
    }
}

impl ECStore {
    /// Read-only capability observation; each mutation revalidates the topology.
    pub async fn supports_artifact_exact_delete(&self) -> bool {
        require_local_put_seal_topology(self).await.is_ok() && !self.ctx.lock_manager().is_disabled()
    }

    /// Delete an exact version in a fully sealed attempt. This authenticates the
    /// storage identity and drains writers; the caller still owns GC eligibility
    /// and cleanup IAM. Distributed receiver deletion is not yet supported.
    pub async fn delete_artifact_object_version(
        self: Arc<Self>,
        bucket: &str,
        expected_incarnation: Uuid,
        attempt_prefix: &str,
        object: &str,
        version: Uuid,
    ) -> Result<()> {
        require_local_put_seal_topology(&self).await?;
        if self.ctx.lock_manager().is_disabled() {
            return Err(Error::other("artifact exact deletion requires namespace locking"));
        }
        let lifecycle = Arc::new(self.acquire_bucket_lifecycle_read_lock(bucket).await?);
        let put = self
            .inventory_artifact_record(bucket, expected_incarnation, attempt_prefix)
            .await?;
        let repair = ArtifactRepairSealRecord::new(&put);
        let record = ArtifactVersionRetirementRecord::new(&put, object, version)?;
        let object = object.to_owned();
        let store = Arc::clone(&self);
        // Lifecycle -> attempt -> repair seal -> metadata transaction -> object.
        // The spawned owner, and the disk deletion tail's typed capability, keep
        // these locks after cancellation of the HTTP caller.
        with_config_object_write_lock(self, put.path(), move |attempt| async move {
            if !put.exists(Arc::clone(&store), &attempt).await? {
                return Err(Error::other("artifact exact deletion requires sealed PUTs"));
            }
            let lock = store.new_ns_lock(RUSTFS_META_BUCKET, &repair.path()).await?;
            let repair_owner = WriteCommitGuard::acquire(&lock, get_lock_acquire_timeout()).await?;
            if !repair.exists(Arc::clone(&store), &repair_owner).await? {
                return Err(Error::other("artifact exact deletion requires sealed repairs"));
            }
            let receivers = store.capture_artifact_receivers().await?;
            store
                .close_artifact_receiver_snapshot(&put, crate::disk::local::ArtifactLeaseDomain::Put, &receivers)
                .await?;
            store
                .close_artifact_receiver_snapshot(&put, crate::disk::local::ArtifactLeaseDomain::Repair, &receivers)
                .await?;
            store.verify_artifact_receiver_snapshot(&put, &receivers).await?;
            let target = Arc::new(ArtifactDeleteTarget {
                store: store.id,
                record,
                lifecycle,
                attempt,
                repair: repair_owner,
            });
            let mut opts = ObjectOptions {
                versioned: true,
                version_id: Some(version.to_string()),
                expected_bucket_incarnation_id: Some(expected_incarnation),
                artifact_delete_target: Some(Arc::clone(&target)),
                delete_mutation_owner: Some(target.clone()),
                ..Default::default()
            };
            opts.add_write_commit_guard(&target.attempt);
            opts.add_write_commit_guard(&target.repair);
            // These owners fence seal records, not the object. Ordinary DELETE
            // must still acquire and retain its exact object WRITE owner.
            opts.no_lock = false;
            target.check_live()?;
            // Preserve the normal exact DELETE's Object Lock, quorum, journal,
            // cache and capacity behavior. Its success is logical deletion, not
            // a claim that every offline shard has been physically erased.
            match store.delete_object(&put.bucket, &object, opts).await {
                Ok(info) => {
                    // Match the S3 request layer's committed-generation quota
                    // delta. A strict absence retry must not subtract twice.
                    if !info.name.is_empty()
                        && !info.delete_marker
                        && let Ok(size) = crate::data_usage::quota_object_size(&info)
                    {
                        crate::data_usage::record_bucket_object_delete_memory(&put.bucket, size, false).await;
                    }
                }
                Err(Error::VersionNotFound(bucket, object, version))
                    if bucket == target.record.bucket
                        && rustfs_utils::path::encode_dir_object(&object) == target.record.object
                        && version == target.record.version_id.to_string() => {}
                Err(Error::ObjectNotFound(bucket, object))
                    if bucket == target.record.bucket
                        && rustfs_utils::path::encode_dir_object(&object) == target.record.object => {}
                Err(error) => return Err(error),
            }
            store.verify_artifact_receiver_snapshot(&put, &receivers).await?;
            store
                .inventory_artifact_record(&put.bucket, expected_incarnation, &put.attempt_prefix)
                .await?;
            target.check_live()
        })
        .await?
    }
}
