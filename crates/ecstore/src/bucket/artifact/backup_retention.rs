// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::bucket::metadata::BUCKET_INCARNATION_FILE;
use crate::config::com::read_config_observation;
use crate::object_api::WriteCompletion;

impl ECStore {
    /// Permanently retain this managed bucket's versions for historical restores.
    /// Lifecycle WRITE drains earlier exact deletes. The detached owner keeps
    /// lifecycle -> metadata transaction locks until every metadata write drains.
    pub async fn retain_artifact_backups(self: Arc<Self>, bucket: &str, incarnation: Uuid, base_prefix: &str) -> Result<()> {
        let bucket = bucket.to_owned();
        let base_prefix = base_prefix.to_owned();
        let ctx = Arc::clone(&self.ctx);
        ctx.run_detached_mutation(async move {
            require_put_seal_topology(&self).await?;
            if self.ctx.lock_manager().is_disabled() {
                return Err(Error::other("artifact backup retention requires namespace locking"));
            }
            let lifecycle = self.acquire_bucket_lifecycle_write_lock(&bucket).await?;
            let transaction = metadata_sys::acquire_bucket_metadata_transaction_lock_in(&self.ctx, &bucket).await?;
            let mut metadata = self.backup_retention_metadata(&bucket, incarnation, &base_prefix).await?;
            let check = || {
                if lifecycle.is_lock_lost() || transaction.is_lock_lost() {
                    Err(Error::other("artifact backup retention fence was lost"))
                } else {
                    Ok(())
                }
            };
            check()?;
            // Even an idempotent retry rewrites with TailDrained: an earlier
            // interrupted call may have reached only some metadata replicas.
            metadata.artifact_backup_retained = true;
            let mut opts = ObjectOptions {
                max_parity: true,
                write_completion: WriteCompletion::TailDrained,
                ..Default::default()
            };
            opts.add_bucket_lifecycle_lock_guard(&lifecycle);
            opts.add_namespace_lock_guard(&transaction);
            metadata.save_with_store_options(self.clone(), &opts).await?;
            check()?;
            let sys = metadata_sys::require_bucket_metadata_sys_in(&self.ctx)?;
            let sys = sys.read().await.clone();
            sys.set_validated(metadata).await?;
            check()
        })
        .await
        .map_err(|_| Error::other("artifact backup retention owner failed"))?
    }

    /// Observe the immutable bucket identity and retention in one locked read.
    pub async fn inspect_artifact_backup_retention(&self, bucket: &str, incarnation: Uuid, base_prefix: &str) -> Result<bool> {
        require_put_seal_topology(self).await?;
        if self.ctx.lock_manager().is_disabled() {
            return Err(Error::other("artifact backup retention requires namespace locking"));
        }
        let lifecycle = self.acquire_bucket_lifecycle_read_lock(bucket).await?;
        let metadata = self.backup_retention_metadata(bucket, incarnation, base_prefix).await?;
        if lifecycle.is_lock_lost() {
            return Err(Error::other("artifact backup retention observation fence was lost"));
        }
        Ok(metadata.artifact_backup_retained)
    }

    async fn backup_retention_metadata(&self, bucket: &str, incarnation: Uuid, base_prefix: &str) -> Result<BucketMetadata> {
        if incarnation.is_nil() || !crate::storage_api_contracts::bucket::valid_artifact_base_prefix(base_prefix) {
            return Err(Error::other("artifact backup retention identity is invalid"));
        }
        let mut metadata = self.read_backup_retention_metadata(bucket).await?;
        let store = metadata_sys::object_store_in(&self.ctx).await?;
        let (sidecar, _) =
            read_config_observation(store, &format!("{BUCKET_META_PREFIX}/{bucket}/{BUCKET_INCARNATION_FILE}"), 16).await?;
        metadata.parse_all_configs()?;
        if metadata.bucket_incarnation_id != incarnation
            || Uuid::from_slice(&sidecar).ok() != Some(incarnation)
            || metadata.artifact_base_prefix.as_deref() != Some(base_prefix)
            || !metadata.bucket_creation_committed
            || !metadata.versioning_config.as_ref().is_some_and(|versioning| {
                versioning.enabled()
                    && !versioning.exclude_folders.unwrap_or(false)
                    && versioning.excluded_prefixes.as_ref().is_none_or(Vec::is_empty)
            })
        {
            return Err(Error::other("artifact backup retention identity mismatch"));
        }
        metadata.bucket_incarnation_sidecar = true;
        Ok(metadata)
    }

    /// Caller must hold the bucket lifecycle fence. An unreadable or missing
    /// record cannot prove that historical backup protection was never enabled.
    pub(crate) async fn require_no_artifact_backup_retention(&self, bucket: &str) -> Result<()> {
        if self.read_backup_retention_metadata(bucket).await?.artifact_backup_retained {
            return Err(Error::other("artifact versions are retained for backups"));
        }
        Ok(())
    }

    async fn read_backup_retention_metadata(&self, bucket: &str) -> Result<BucketMetadata> {
        if bucket.is_empty()
            || super::super::utils::has_bad_path_component(bucket)
            || super::super::utils::is_meta_bucketname(bucket)
        {
            return Err(Error::other("artifact backup retention bucket is invalid"));
        }
        let store = metadata_sys::object_store_in(&self.ctx).await?;
        if store.id != self.id {
            return Err(Error::other("artifact backup retention belongs to another store"));
        }
        let (bytes, _) = read_config_observation(store, &BucketMetadata::new(bucket).save_file_path(), 64 * 1024 * 1024).await?;
        let metadata = BucketMetadata::decode_file(&bytes)?;
        if metadata.name != bucket {
            return Err(Error::other("artifact backup retention bucket mismatch"));
        }
        Ok(metadata)
    }
}
