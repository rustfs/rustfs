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
use crate::bucket::utils::has_bad_path_component;
use crate::disk::error::{DiskError, Result as DiskResult};
use crate::disk::{
    DeleteOptions, DeleteRecoveryRpcOperation, DeleteRecoveryRpcReply, Disk, GuardedDeleteVersionsPayload, LegacyFileWrite,
    LegacyMetadataWrite, LegacyPartMutation, LegacyPartOperation, LegacyPathDelete, LegacyPathDeleteOperation, LegacyRenameData,
    LegacyRenameFile, RenameDataGuards, RenameDataResp,
};
use crate::runtime::instance::{InstanceContext, NamespaceCommitGuard};
use crate::runtime::sources as runtime_sources;
use rustfs_io_metrics::{
    PUT_STAGE_SET_DISK_RENAME_REMOTE_HANDLER_ADMIT, PUT_STAGE_SET_DISK_RENAME_REMOTE_HANDLER_LOCAL_RENAME,
    PUT_STAGE_SET_DISK_RENAME_REMOTE_HANDLER_LOOKUP, PUT_STAGE_SET_DISK_RENAME_REMOTE_HANDLER_OVERHEAD,
    PUT_STAGE_SET_DISK_RENAME_REMOTE_HANDLER_TOTAL,
};
use tracing::{debug, error};

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_DISK_STARTUP: &str = "disk_startup";
const EVENT_LOCAL_DISK_ID_PREWARM_SKIPPED: &str = "local_disk_id_prewarm_skipped";
const EVENT_LOCK_CLIENT_INITIALIZATION_FAILED: &str = "lock_client_initialization_failed";

/// An instance-bound capability for internal writes before ECStore/IAM startup.
/// Its private context and volume checks cannot be replaced by a caller guard.
#[derive(Clone)]
pub struct BootstrapLocalTarget {
    ctx: Arc<InstanceContext>,
}

impl BootstrapLocalTarget {
    pub async fn recover_local_delete_transaction(
        &self,
        disk_ref: &str,
        volume: &str,
        operation: DeleteRecoveryRpcOperation,
    ) -> DiskResult<DeleteRecoveryRpcReply> {
        validate_bootstrap_volume(volume)?;
        recover_local_delete_transaction_with_ctx(&self.ctx, disk_ref, volume, operation, None).await
    }

    pub async fn guarded_local_delete_versions(
        &self,
        disk_ref: &str,
        volume: &str,
        payload: GuardedDeleteVersionsPayload,
    ) -> DiskResult<Vec<Option<DiskError>>> {
        validate_bootstrap_volume(volume)?;
        guarded_local_delete_versions_with_ctx(&self.ctx, disk_ref, volume, payload, None).await
    }

    pub async fn begin_legacy_local_file_write(
        &self,
        disk: &DiskStore,
        volume: &str,
        external_guard: Option<Arc<dyn Send + Sync>>,
    ) -> DiskResult<LegacyFileWrite> {
        validate_bootstrap_volume(volume)?;
        begin_legacy_local_file_write_with_ctx(&self.ctx, disk, volume, external_guard).await
    }

    pub async fn begin_legacy_local_residual_version_delete(
        &self,
        disk_ref: &str,
        volume: &str,
        path: &str,
        undo_write: bool,
    ) -> DiskResult<LegacyMetadataWrite> {
        validate_bootstrap_volume(volume)?;
        begin_legacy_local_residual_version_delete_with_ctx(&self.ctx, disk_ref, volume, path, undo_write).await
    }

    pub async fn begin_legacy_local_path_delete(
        &self,
        disk_ref: &str,
        volume: &str,
        operation: LegacyPathDeleteOperation,
        external_guard: Option<Arc<dyn Send + Sync>>,
    ) -> DiskResult<LegacyPathDelete> {
        validate_bootstrap_volume(volume)?;
        begin_legacy_local_path_delete_with_ctx(&self.ctx, disk_ref, volume, operation, external_guard).await
    }

    pub async fn begin_legacy_local_part_mutation(
        &self,
        disk_ref: &str,
        source: Option<(&str, &str)>,
        destination: (&str, &str),
        operation: LegacyPartOperation,
    ) -> DiskResult<LegacyPartMutation> {
        if let Some(source) = source {
            validate_bootstrap_volume(source.0)?;
        }
        validate_bootstrap_volume(destination.0)?;
        begin_legacy_local_part_mutation_with_ctx(&self.ctx, disk_ref, source, destination, operation).await
    }

    pub async fn begin_legacy_local_rename_file(
        &self,
        disk_ref: &str,
        source: (&str, &str),
        destination: (&str, &str),
        durable: bool,
    ) -> DiskResult<LegacyRenameFile> {
        validate_bootstrap_volume(source.0)?;
        validate_bootstrap_volume(destination.0)?;
        begin_legacy_local_rename_file_with_ctx(&self.ctx, disk_ref, source, destination, durable).await
    }

    pub async fn begin_legacy_local_delete_versions(&self, disk_ref: &str, volume: &str) -> DiskResult<LegacyFileWrite> {
        validate_bootstrap_volume(volume)?;
        begin_legacy_local_delete_versions_with_ctx(&self.ctx, disk_ref, volume).await
    }

    /// Resolve only this listener's active local registry; never an ambient store.
    pub async fn local_mutation_disk(&self, disk_ref: &str) -> DiskResult<DiskStore> {
        local_mutation_disk(&self.ctx, disk_ref).await
    }

    pub fn new(ctx: Arc<InstanceContext>) -> Self {
        Self { ctx }
    }

    pub fn is_for_store(&self, store: &ECStore) -> bool {
        Arc::ptr_eq(&self.ctx, &store.ctx)
    }

    pub async fn begin_legacy_local_rename_data(
        &self,
        disk_ref: &str,
        source: (&str, &str),
        fi: &FileInfo,
        destination: (&str, &str),
    ) -> DiskResult<LegacyRenameData> {
        validate_bootstrap_volume(source.0)?;
        validate_bootstrap_volume(destination.0)?;
        begin_legacy_local_rename_data_with_ctx(&self.ctx, disk_ref, source, fi, destination).await
    }

    pub async fn rename_local_data(
        &self,
        disk_ref: &str,
        source: (&str, &str),
        fi: &FileInfo,
        destination: (&str, &str),
        scanner_token: Option<Uuid>,
    ) -> DiskResult<RenameDataResp> {
        if scanner_token.is_some() {
            return Err(DiskError::other("bootstrap rename cannot use a scanner publication lease"));
        }
        validate_bootstrap_volume(source.0)?;
        validate_bootstrap_volume(destination.0)?;
        rename_local_data_with_ctx(&self.ctx, disk_ref, source, fi, destination, RenameDataGuards::default()).await
    }

    pub async fn begin_legacy_local_metadata_mutation(
        &self,
        disk_ref: &str,
        volume: &str,
        path: &str,
    ) -> DiskResult<LegacyMetadataWrite> {
        validate_bootstrap_volume(volume)?;
        begin_legacy_local_metadata_mutation_with_ctx(&self.ctx, disk_ref, volume, path).await
    }

    pub async fn undo_local_write(
        &self,
        disk_ref: &str,
        volume: &str,
        path: &str,
        fi: FileInfo,
        opts: DeleteOptions,
    ) -> DiskResult<()> {
        validate_bootstrap_volume(volume)?;
        undo_local_write_with_ctx(&self.ctx, disk_ref, volume, path, fi, opts).await
    }
}

fn validate_bootstrap_volume(volume: &str) -> DiskResult<()> {
    // Prefix membership alone permits aliases such as .rustfs.sys/../bucket.
    // Validate both raw rename volumes before any disk lookup or admission.
    if has_bad_path_component(volume) || !is_meta_bucketname(volume) {
        return Err(DiskError::FileAccessDenied);
    }
    Ok(())
}

fn canonical_artifact_path(path: &str) -> bool {
    // Directory keys replace their final slash with the longer on-disk suffix.
    let logical_len = path
        .strip_suffix(rustfs_utils::path::GLOBAL_DIR_SUFFIX)
        .map_or(path.len(), |prefix| prefix.len() + 1);
    !path.is_empty()
        && logical_len <= 1024
        && !has_bad_path_component(path)
        && !path.contains('\\')
        && !path.contains('\0')
        && path.split('/').all(|part| !part.is_empty())
}

fn validate_artifact_repair_target(target: (&str, &str), fi: &FileInfo, disk: Uuid, incarnation: Uuid) -> DiskResult<()> {
    if disk.is_nil()
        || incarnation.is_nil()
        || fi.version_id.is_none_or(|version| version.is_nil())
        || has_bad_path_component(target.0)
        || is_meta_bucketname(target.0)
        || !canonical_artifact_path(target.1)
        || fi.name != target.1
        || (!fi.volume.is_empty() && fi.volume != target.0)
    {
        return Err(DiskError::FileAccessDenied);
    }
    fi.validate_for_metadata_read()?;
    Ok(())
}

impl ECStore {
    pub async fn recover_local_delete_transaction(
        &self,
        disk_ref: &str,
        volume: &str,
        operation: DeleteRecoveryRpcOperation,
    ) -> DiskResult<DeleteRecoveryRpcReply> {
        recover_local_delete_transaction_with_ctx(&self.ctx, disk_ref, volume, operation, Some(self)).await
    }

    pub async fn guarded_local_delete_versions(
        &self,
        disk_ref: &str,
        volume: &str,
        payload: GuardedDeleteVersionsPayload,
    ) -> DiskResult<Vec<Option<DiskError>>> {
        guarded_local_delete_versions_with_ctx(&self.ctx, disk_ref, volume, payload, Some(self)).await
    }

    pub async fn begin_legacy_local_file_write(
        &self,
        disk: &DiskStore,
        volume: &str,
        external_guard: Option<Arc<dyn Send + Sync>>,
    ) -> DiskResult<LegacyFileWrite> {
        begin_legacy_local_file_write_with_ctx(&self.ctx, disk, volume, external_guard).await
    }

    pub async fn begin_legacy_local_residual_version_delete(
        &self,
        disk_ref: &str,
        volume: &str,
        path: &str,
        undo_write: bool,
    ) -> DiskResult<LegacyMetadataWrite> {
        begin_legacy_local_residual_version_delete_with_ctx(&self.ctx, disk_ref, volume, path, undo_write).await
    }

    pub async fn begin_legacy_local_path_delete(
        &self,
        disk_ref: &str,
        volume: &str,
        operation: LegacyPathDeleteOperation,
        external_guard: Option<Arc<dyn Send + Sync>>,
    ) -> DiskResult<LegacyPathDelete> {
        begin_legacy_local_path_delete_with_ctx(&self.ctx, disk_ref, volume, operation, external_guard).await
    }

    pub async fn begin_legacy_local_part_mutation(
        &self,
        disk_ref: &str,
        source: Option<(&str, &str)>,
        destination: (&str, &str),
        operation: LegacyPartOperation,
    ) -> DiskResult<LegacyPartMutation> {
        begin_legacy_local_part_mutation_with_ctx(&self.ctx, disk_ref, source, destination, operation).await
    }

    pub async fn begin_legacy_local_rename_file(
        &self,
        disk_ref: &str,
        source: (&str, &str),
        destination: (&str, &str),
        durable: bool,
    ) -> DiskResult<LegacyRenameFile> {
        begin_legacy_local_rename_file_with_ctx(&self.ctx, disk_ref, source, destination, durable).await
    }

    pub async fn begin_legacy_local_delete_versions(&self, disk_ref: &str, volume: &str) -> DiskResult<LegacyFileWrite> {
        begin_legacy_local_delete_versions_with_ctx(&self.ctx, disk_ref, volume).await
    }

    pub async fn local_mutation_disk(&self, disk_ref: &str) -> DiskResult<DiskStore> {
        local_mutation_disk(&self.ctx, disk_ref).await
    }

    pub async fn begin_legacy_local_rename_data(
        &self,
        disk_ref: &str,
        source: (&str, &str),
        fi: &FileInfo,
        destination: (&str, &str),
    ) -> DiskResult<LegacyRenameData> {
        begin_legacy_local_rename_data_with_ctx(&self.ctx, disk_ref, source, fi, destination).await
    }

    /// Admit an artifact publication on the receiving instance, retaining its
    /// own lifecycle and disk lease after the originating RPC is cancelled.
    pub async fn rename_local_artifact_data(
        &self,
        disk_ref: &str,
        source: (&str, &str),
        fi: &FileInfo,
        destination: (&str, &str),
        expected: Uuid,
    ) -> DiskResult<RenameDataResp> {
        if source.0 != crate::disk::RUSTFS_META_TMP_BUCKET
            || fi.version_id.is_none_or(|version| version.is_nil())
            || fi.deleted
            || self.ctx.lock_manager().is_disabled()
        {
            return Err(DiskError::FileAccessDenied);
        }
        // Receiver lifecycle read -> local admission -> physical disk mutation.
        // Never reacquire the coordinator's distributed attempt write lock.
        let lifecycle = Arc::new(
            self.acquire_bucket_incarnation_fence(destination.0, expected)
                .await
                .map_err(|_| DiskError::FileAccessDenied)?,
        );
        let identity = self
            .artifact_receiver_identity(destination.0, expected, destination.1, false)
            .await
            .map_err(|_| DiskError::FileAccessDenied)?;
        if lifecycle.is_lock_lost() {
            return Err(DiskError::FileAccessDenied);
        }
        rename_local_data_with_ctx(
            &self.ctx,
            disk_ref,
            source,
            fi,
            destination,
            RenameDataGuards {
                artifact_put: Some(identity),
                external_guard: Some(lifecycle),
                ..Default::default()
            },
        )
        .await
    }

    pub async fn rename_local_artifact_repair_data(
        &self,
        disk_ref: &str,
        expected_disk: Uuid,
        source: (&str, &str),
        fi: &FileInfo,
        destination: (&str, &str),
        expected_incarnation: Uuid,
    ) -> DiskResult<RenameDataResp> {
        validate_artifact_repair_target(destination, fi, expected_disk, expected_incarnation)?;
        if source.0 != crate::disk::RUSTFS_META_TMP_BUCKET
            || !canonical_artifact_path(source.1)
            || self.ctx.lock_manager().is_disabled()
        {
            return Err(DiskError::FileAccessDenied);
        }
        // Resolve once; every later identity check and physical worker uses this
        // captured disk even if its registry entry is replaced during admission.
        let (disk, id) = local_disk_candidate(&self.ctx, disk_ref).await?;
        let lifecycle = Arc::new(
            self.acquire_bucket_incarnation_fence(destination.0, expected_incarnation)
                .await
                .map_err(|_| DiskError::FileAccessDenied)?,
        );
        let (identity, retirement) = self
            .artifact_repair_receiver_identity(destination.0, expected_incarnation, destination.1, fi.version_id)
            .await
            .map_err(|_| DiskError::FileAccessDenied)?;
        let owner = admit_local_disk(&self.ctx, &disk, id, true).await?;
        if disk.get_disk_id().await? != Some(expected_disk) || lifecycle.is_lock_lost() || retirement.is_lock_lost() {
            return Err(DiskError::FileAccessDenied);
        }
        let Disk::Local(local) = disk.as_ref() else {
            return Err(DiskError::DiskNotFound);
        };
        let lease = local.acquire_artifact_repair_lease(identity.receiver_key).await?;
        if lifecycle.is_lock_lost() || retirement.is_lock_lost() {
            return Err(DiskError::FileAccessDenied);
        }
        let physical_owner: Arc<dyn Send + Sync> = Arc::new((owner, Arc::clone(&lifecycle), Arc::clone(&retirement), lease));
        let result = disk
            .rename_data_borrowed_with_fence_observed(
                source.0,
                source.1,
                fi,
                destination.0,
                destination.1,
                RenameDataGuards {
                    namespace_owner: Some(physical_owner),
                    ..Default::default()
                },
            )
            .await
            .result;
        if lifecycle.is_lock_lost() || retirement.is_lock_lost() {
            return Err(DiskError::FileAccessDenied);
        }
        result
    }

    pub async fn write_local_artifact_repair_metadata(
        &self,
        disk_ref: &str,
        expected_disk: Uuid,
        target: (&str, &str),
        fi: FileInfo,
        expected_incarnation: Uuid,
    ) -> DiskResult<()> {
        validate_artifact_repair_target(target, &fi, expected_disk, expected_incarnation)?;
        if self.ctx.lock_manager().is_disabled() {
            return Err(DiskError::FileAccessDenied);
        }
        let (disk, id) = local_disk_candidate(&self.ctx, disk_ref).await?;
        // Lifecycle -> authoritative records -> disk repair admission -> metadata
        // RMW/publication locks. No receiver reacquires the attempt write lock.
        let lifecycle = Arc::new(
            self.acquire_bucket_incarnation_fence(target.0, expected_incarnation)
                .await
                .map_err(|_| DiskError::FileAccessDenied)?,
        );
        let (identity, retirement) = self
            .artifact_repair_receiver_identity(target.0, expected_incarnation, target.1, fi.version_id)
            .await
            .map_err(|_| DiskError::FileAccessDenied)?;
        let owner = admit_local_disk(&self.ctx, &disk, id, true).await?;
        if disk.get_disk_id().await? != Some(expected_disk) || lifecycle.is_lock_lost() || retirement.is_lock_lost() {
            return Err(DiskError::FileAccessDenied);
        }
        let Disk::Local(local) = disk.as_ref() else {
            return Err(DiskError::DiskNotFound);
        };
        let lease = local.acquire_artifact_repair_lease(identity.receiver_key).await?;
        if lifecycle.is_lock_lost() || retirement.is_lock_lost() {
            return Err(DiskError::FileAccessDenied);
        }
        let physical_owner: Arc<dyn Send + Sync> = Arc::new((owner, Arc::clone(&lifecycle), Arc::clone(&retirement), lease));
        let publication = disk
            .begin_legacy_metadata_write_with_namespace_owner(target.0, target.1, Some(physical_owner))
            .await?;
        let result = publication.write_metadata(fi).await;
        if lifecycle.is_lock_lost() || retirement.is_lock_lost() {
            return Err(DiskError::FileAccessDenied);
        }
        result
    }

    /// The coordinator owns lifecycle -> attempt -> repair locks. A receiver
    /// must not reacquire their distributed READ behind a queued lifecycle WRITE.
    /// Its authority comes from persisted managed metadata, its physical scope
    /// from the captured disk, and closure from the existing two-domain owners.
    pub async fn inventory_local_artifact_attempt(
        &self,
        disk_ref: &str,
        expected_disk: Uuid,
        bucket: &str,
        incarnation: Uuid,
        attempt_prefix: &str,
        phase: rustfs_protos::proto_gen::node_service::ArtifactInventoryPhase,
        deadline: tokio::time::Instant,
        output: tokio::sync::mpsc::Sender<crate::disk::ArtifactInventoryFrame>,
    ) -> DiskResult<()> {
        use rustfs_protos::proto_gen::node_service::ArtifactInventoryPhase;
        if expected_disk.is_nil()
            || incarnation.is_nil()
            || self.ctx.lock_manager().is_disabled()
            || has_bad_path_component(bucket)
            || is_meta_bucketname(bucket)
        {
            return Err(DiskError::FileAccessDenied);
        }
        let identity = self
            .artifact_inventory_receiver_identity(bucket, incarnation, attempt_prefix)
            .await
            .map_err(|_| DiskError::FileAccessDenied)?;
        let (disk, id) = local_disk_candidate(&self.ctx, disk_ref).await?;
        admit_local_disk(&self.ctx, &disk, id, false).await?;
        let Disk::Local(local) = disk.as_ref() else {
            return Err(DiskError::DiskNotFound);
        };
        let local = local.get_disk();
        match phase {
            ArtifactInventoryPhase::ArtifactInventoryScan => {
                local
                    .inventory_artifact_attempt(expected_disk, bucket, attempt_prefix, identity, deadline, output)
                    .await?;
            }
            ArtifactInventoryPhase::ArtifactInventoryVerify => {
                let _verified = local
                    .verify_artifact_inventory_closed(expected_disk, identity, deadline)
                    .await?;
                let current = self
                    .artifact_inventory_receiver_identity(bucket, incarnation, attempt_prefix)
                    .await
                    .map_err(|_| DiskError::FileAccessDenied)?;
                if current.receiver_key != identity.receiver_key {
                    return Err(DiskError::FileAccessDenied);
                }
                admit_local_disk(&self.ctx, &disk, None, false).await?;
                return Ok(());
            }
        }
        let current = self
            .artifact_inventory_receiver_identity(bucket, incarnation, attempt_prefix)
            .await
            .map_err(|_| DiskError::FileAccessDenied)?;
        if current.receiver_key != identity.receiver_key || local.artifact_disk_id().await? != expected_disk {
            return Err(DiskError::FileAccessDenied);
        }
        admit_local_disk(&self.ctx, &disk, None, false).await?;
        Ok(())
    }

    pub async fn seal_local_artifact_repairs(
        &self,
        disk_ref: &str,
        expected_disk: Uuid,
        bucket: &str,
        expected_incarnation: Uuid,
        attempt_prefix: &str,
    ) -> DiskResult<Uuid> {
        if expected_disk.is_nil()
            || expected_incarnation.is_nil()
            || has_bad_path_component(bucket)
            || is_meta_bucketname(bucket)
            || self.ctx.lock_manager().is_disabled()
        {
            return Err(DiskError::FileAccessDenied);
        }
        let (disk, id) = local_disk_candidate(&self.ctx, disk_ref).await?;
        let lifecycle = self
            .acquire_bucket_incarnation_fence(bucket, expected_incarnation)
            .await
            .map_err(|_| DiskError::FileAccessDenied)?;
        let identity = self
            .artifact_receiver_identity(bucket, expected_incarnation, attempt_prefix, true)
            .await
            .map_err(|_| DiskError::FileAccessDenied)?;
        let _owner = admit_local_disk(&self.ctx, &disk, id, true).await?;
        let Disk::Local(local) = disk.as_ref() else {
            return Err(DiskError::DiskNotFound);
        };
        if local.artifact_disk_id().await? != expected_disk || lifecycle.is_lock_lost() {
            return Err(DiskError::FileAccessDenied);
        }
        local.seal_artifact_repairs(identity.receiver_key).await?;
        if local.artifact_disk_id().await? != expected_disk || lifecycle.is_lock_lost() {
            return Err(DiskError::FileAccessDenied);
        }
        Ok(expected_disk)
    }

    /// A receiver acknowledgment means this exact disk has durably closed the
    /// attempt and drained physical owners. An interrupted waiter proves neither.
    pub async fn seal_local_artifact_puts(
        &self,
        disk_ref: &str,
        expected_disk: Uuid,
        bucket: &str,
        expected_incarnation: Uuid,
        attempt_prefix: &str,
    ) -> DiskResult<Uuid> {
        if expected_disk.is_nil() || self.ctx.lock_manager().is_disabled() {
            return Err(DiskError::FileAccessDenied);
        }
        let lifecycle = self
            .acquire_bucket_incarnation_fence(bucket, expected_incarnation)
            .await
            .map_err(|_| DiskError::FileAccessDenied)?;
        let identity = self
            .artifact_receiver_identity(bucket, expected_incarnation, attempt_prefix, true)
            .await
            .map_err(|_| DiskError::FileAccessDenied)?;
        let (disk, id) = local_disk_candidate(&self.ctx, disk_ref).await?;
        let _owner = admit_local_disk(&self.ctx, &disk, id, true).await?;
        let Disk::Local(local) = disk.as_ref() else {
            return Err(DiskError::DiskNotFound);
        };
        if local.artifact_disk_id().await? != expected_disk || lifecycle.is_lock_lost() {
            return Err(DiskError::FileAccessDenied);
        }
        local.seal_artifact_puts(identity.receiver_key).await?;
        if local.artifact_disk_id().await? != expected_disk || lifecycle.is_lock_lost() {
            return Err(DiskError::FileAccessDenied);
        }
        Ok(expected_disk)
    }

    /// Resolve a quota lease against this request's captured storage instance.
    pub async fn quota_mutation_fence_disk(&self, disk_ref: &str) -> DiskResult<DiskStore> {
        let (disk, id) = local_disk_candidate(&self.ctx, disk_ref).await?;
        let _owner = admit_local_disk(&self.ctx, &disk, id, false).await?;
        Ok(disk)
    }

    pub async fn write_local_metadata_at_incarnation(
        self: &Arc<Self>,
        disk_ref: &str,
        target: (&str, &str),
        fi: FileInfo,
        expected: Uuid,
    ) -> DiskResult<()> {
        let disk_ref = disk_ref.to_owned();
        let path = target.1.to_owned();
        self.run_bucket_heal_at_incarnation(target.0, expected, &Default::default(), move |store, volume, _| async move {
            let (disk, id) = local_disk_candidate(&store.ctx, &disk_ref).await?;
            let _owner = admit_local_disk(&store.ctx, &disk, id, true).await?;
            disk.write_metadata("", &volume, &path, fi).await.map_err(Into::into)
        })
        .await
        .map_err(|error| DiskError::other(error.to_string()))
    }

    pub async fn delete_local_path_at_incarnation(
        self: &Arc<Self>,
        disk_ref: &str,
        target: (&str, &str),
        opts: DeleteOptions,
        expected: Uuid,
    ) -> DiskResult<()> {
        let disk_ref = disk_ref.to_owned();
        let path = target.1.to_owned();
        self.run_bucket_heal_at_incarnation(target.0, expected, &Default::default(), move |store, volume, _| async move {
            let (disk, id) = local_disk_candidate(&store.ctx, &disk_ref).await?;
            let owner = admit_local_disk(&store.ctx, &disk, id, true).await?;
            let scope = super::bucket_heal_scope(&volume).ok_or_else(|| StorageError::other("missing bucket heal scope"))?;
            scope.check()?;
            disk.delete_with_namespace_owner(&volume, &path, opts, Some(Arc::new((scope, owner))))
                .await
                .map_err(Into::into)
        })
        .await
        .map_err(|error| DiskError::other(error.to_string()))
    }

    pub async fn delete_local_version_at_incarnation(
        &self,
        disk_ref: &str,
        target: (&str, &str),
        fi: FileInfo,
        force_del_marker: bool,
        opts: DeleteOptions,
        expected: Uuid,
    ) -> DiskResult<()> {
        if expected.is_nil() || opts.undo_write || self.ctx.lock_manager().is_disabled() {
            return Err(DiskError::other("invalid incarnation-bound heal deletion"));
        }
        let fence = Arc::new(
            self.acquire_bucket_incarnation_fence(target.0, expected)
                .await
                .map_err(|error| DiskError::other(error.to_string()))?,
        );
        let (disk, id) = local_disk_candidate(&self.ctx, disk_ref).await?;
        let owner = admit_local_disk(&self.ctx, &disk, id, true).await?;
        if fence.is_lock_lost() {
            return Err(DiskError::other("bucket heal incarnation fence was lost"));
        }
        disk.delete_version_with_namespace_owner(target.0, target.1, fi, force_del_marker, opts, Some(Arc::new((fence, owner))))
            .await
    }

    /// Target-side admission for an incarnation-bound heal rename. The physical
    /// namespace operations retain the lifecycle owner after RPC cancellation.
    pub async fn rename_local_data_at_incarnation(
        &self,
        disk_ref: &str,
        source: (&str, &str),
        fi: &FileInfo,
        destination: (&str, &str),
        expected: Uuid,
    ) -> DiskResult<RenameDataResp> {
        if expected.is_nil() || self.ctx.lock_manager().is_disabled() {
            return Err(DiskError::other(
                "incarnation-bound rename requires namespace locking and a non-nil identity",
            ));
        }
        let bucket = if destination.0 == RUSTFS_META_BUCKET {
            super::heal::bucket_metadata_owner(destination.1).ok_or(DiskError::FileAccessDenied)?
        } else {
            destination.0
        };
        let guard = Arc::new(
            self.acquire_bucket_incarnation_fence(bucket, expected)
                .await
                .map_err(|error| DiskError::other(error.to_string()))?,
        );
        if guard.is_lock_lost() {
            return Err(DiskError::other("bucket heal incarnation fence was lost"));
        }
        let transaction = if destination.0 == RUSTFS_META_BUCKET {
            // The receiver must retain the config fence after an RPC timeout.
            // A delayed request must also reject a newer config in the same
            // bucket incarnation before publishing its older repair payload.
            let transaction = crate::bucket::metadata_sys::acquire_bucket_metadata_transaction_read_lock_in(&self.ctx, bucket)
                .await
                .map_err(|error| DiskError::other(error.to_string()))?;
            let (current, _) = self
                .get_pool_info_for_delete_marker(
                    destination.0,
                    destination.1,
                    &ObjectOptions {
                        no_lock: true,
                        ..Default::default()
                    },
                )
                .await
                .map_err(|error| DiskError::other(error.to_string()))?;
            let current = current.object_info;
            if transaction.is_lock_lost()
                || guard.is_lock_lost()
                || fi.mod_time.is_none()
                || current.mod_time != fi.mod_time
                || current.data_dir != fi.data_dir
                || current.size != fi.size
                || current.etag != fi.get_etag()
            {
                return Err(DiskError::FileAccessDenied);
            }
            Some(transaction)
        } else {
            None
        };
        rename_local_data_with_ctx(
            &self.ctx,
            disk_ref,
            source,
            fi,
            destination,
            RenameDataGuards {
                external_guard: Some(Arc::new((guard, transaction))),
                ..Default::default()
            },
        )
        .await
    }

    /// Execute on this instance's active local disk through the physical owner.
    pub async fn rename_local_data(
        &self,
        disk_ref: &str,
        source: (&str, &str),
        fi: &FileInfo,
        destination: (&str, &str),
        scanner_token: Option<Uuid>,
    ) -> DiskResult<RenameDataResp> {
        let external_guard: Option<Arc<dyn Send + Sync>> = if let Some(token) = scanner_token {
            Some(
                self.acquire_scanner_publication_lease_guard_for_rename(token, destination.0, destination.1)
                    .await
                    .map_err(|err| DiskError::other(err.to_string()))? as Arc<dyn Send + Sync>,
            )
        } else {
            None
        };
        rename_local_data_with_ctx(
            &self.ctx,
            disk_ref,
            source,
            fi,
            destination,
            RenameDataGuards {
                scanner_publication_lease_token: scanner_token,
                external_guard,
                namespace_owner: None,
                ..Default::default()
            },
        )
        .await
    }

    pub async fn begin_legacy_local_metadata_mutation(
        &self,
        disk_ref: &str,
        volume: &str,
        path: &str,
    ) -> DiskResult<LegacyMetadataWrite> {
        begin_legacy_local_metadata_mutation_with_ctx(&self.ctx, disk_ref, volume, path).await
    }

    pub async fn undo_local_write(
        &self,
        disk_ref: &str,
        volume: &str,
        path: &str,
        fi: FileInfo,
        opts: DeleteOptions,
    ) -> DiskResult<()> {
        undo_local_write_with_ctx(&self.ctx, disk_ref, volume, path, fi, opts).await
    }
}

async fn recover_local_delete_transaction_with_ctx(
    ctx: &Arc<InstanceContext>,
    disk_ref: &str,
    volume: &str,
    operation: DeleteRecoveryRpcOperation,
    authority: Option<&ECStore>,
) -> DiskResult<DeleteRecoveryRpcReply> {
    match operation {
        DeleteRecoveryRpcOperation::ApplyVersion {
            object,
            file_info,
            force_del_marker,
            mut opts,
        } => {
            crate::disk::validate_delete_recovery_options(&opts)?;
            if object.starts_with('/') {
                return Err(DiskError::InvalidPath);
            }
            let write = begin_legacy_local_metadata_mutation_with_ctx(ctx, disk_ref, volume, &object).await?;
            // Classify only after this exact physical volume is admitted. The
            // capability retains the same owner through publication and its tail.
            if let Some(store) = authority {
                store.reject_legacy_artifact_mutation(volume).await?;
            }
            opts.recoverable_delete = true;
            write.stage_delete_version(*file_info, force_del_marker, opts).await?;
            Ok(DeleteRecoveryRpcReply::AppliedVersion)
        }
        DeleteRecoveryRpcOperation::ApplyVersions { versions, mut opts } => {
            crate::disk::validate_delete_recovery_options(&opts)?;
            if versions.iter().any(|version| version.name.starts_with('/')) {
                return Err(DiskError::InvalidPath);
            }
            let write = begin_legacy_local_delete_versions_with_ctx(ctx, disk_ref, volume).await?;
            if let Some(store) = authority {
                store.reject_legacy_artifact_mutation(volume).await?;
            }
            opts.recoverable_delete = true;
            let errors = write.delete_versions(versions, opts).await;
            Ok(DeleteRecoveryRpcReply::AppliedVersions(
                errors.into_iter().map(|error| error.map(|error| error.to_u32())).collect(),
            ))
        }
        DeleteRecoveryRpcOperation::Settle {
            object,
            transaction,
            expected_fingerprint,
            action,
        } => {
            let write = begin_legacy_local_metadata_mutation_with_ctx(ctx, disk_ref, volume, &object).await?;
            if let Some(store) = authority {
                store.reject_legacy_artifact_mutation(volume).await?;
            }
            write
                .settle_delete_transaction(transaction, expected_fingerprint, action)
                .await
                .map(DeleteRecoveryRpcReply::Settled)
        }
        operation => {
            let disk = local_mutation_disk(ctx, disk_ref).await?;
            match operation {
                DeleteRecoveryRpcOperation::Exists { object } => disk
                    .has_delete_transaction_evidence(volume, &object)
                    .await
                    .map(|present| DeleteRecoveryRpcReply::Evidence { present }),
                DeleteRecoveryRpcOperation::InspectOrphanDataDir { path } => disk
                    .is_plain_orphan_data_dir(volume, &path)
                    .await
                    .map(|plain| DeleteRecoveryRpcReply::OrphanDataDir { plain }),
                DeleteRecoveryRpcOperation::Discover { object, cursor, limit } => disk
                    .discover_delete_transactions(volume, object.as_deref(), cursor, limit)
                    .await
                    .map(DeleteRecoveryRpcReply::Discovered),
                DeleteRecoveryRpcOperation::Inspect { object, transaction } => disk
                    .inspect_delete_transaction(volume, &object, transaction)
                    .await
                    .map(DeleteRecoveryRpcReply::Inspected),
                _ => Err(DiskError::Unexpected),
            }
        }
    }
}

async fn guarded_local_delete_versions_with_ctx(
    ctx: &Arc<InstanceContext>,
    disk_ref: &str,
    volume: &str,
    payload: GuardedDeleteVersionsPayload,
    authority: Option<&ECStore>,
) -> DiskResult<Vec<Option<DiskError>>> {
    payload.validate()?;
    let write = begin_legacy_local_delete_versions_with_ctx(ctx, disk_ref, volume).await?;
    if let Some(store) = authority {
        store.reject_legacy_artifact_mutation(volume).await?;
    }
    Ok(write.delete_versions_guarded(payload.versions, payload.opts).await)
}

async fn local_mutation_disk(ctx: &Arc<InstanceContext>, disk_ref: &str) -> DiskResult<DiskStore> {
    let (disk, id) = local_disk_candidate(ctx, disk_ref).await?;
    admit_local_disk(ctx, &disk, id, false).await?;
    Ok(disk)
}

// The optional ID is a cold lookup to cache only after final admission.
async fn local_disk_candidate(ctx: &Arc<InstanceContext>, disk_ref: &str) -> DiskResult<(DiskStore, Option<Uuid>)> {
    let map = ctx.local_disk_map();
    if let Some(disk) = map.read().await.get(disk_ref).and_then(Option::as_ref).cloned() {
        return Ok((disk, None));
    }
    let disk_id = Uuid::parse_str(disk_ref).map_err(|_| DiskError::DiskNotFound)?;
    let cached_path = ctx.local_disk_id_map().read().await.get(&disk_id).cloned();
    if let Some(path) = cached_path {
        let cached_disk = map.read().await.get(&path).and_then(Option::as_ref).cloned();
        if let Some(disk) = cached_disk
            && matches!(disk.as_ref(), Disk::Local(_))
            && disk.get_disk_id().await? == Some(disk_id)
        {
            return Ok((disk, None));
        }
    }
    let disks: Vec<_> = map.read().await.values().filter_map(Clone::clone).collect();
    // Disk identity may perform format I/O. No registry guard spans this await.
    for disk in disks {
        if matches!(disk.as_ref(), Disk::Local(_)) && disk.get_disk_id().await.ok().flatten() == Some(disk_id) {
            return Ok((disk, Some(disk_id)));
        }
    }
    Err(DiskError::DiskNotFound)
}

async fn admit_local_disk(
    ctx: &Arc<InstanceContext>,
    disk: &DiskStore,
    disk_id: Option<Uuid>,
    mutates_namespace: bool,
) -> DiskResult<Option<Arc<NamespaceCommitGuard>>> {
    if !matches!(disk.as_ref(), Disk::Local(_)) {
        return Err(DiskError::DiskNotFound);
    }
    let map = ctx.local_disk_map();
    let active = map.read().await;
    if !active
        .get(&disk.endpoint().to_string())
        .and_then(Option::as_ref)
        .is_some_and(|current| Arc::ptr_eq(current, disk))
    {
        return Err(DiskError::DiskNotFound);
    }
    // Preserve registry -> ID-cache lock order; no filesystem I/O under either.
    if let Some(disk_id) = disk_id {
        ctx.local_disk_id_map()
            .write()
            .await
            .insert(disk_id, disk.endpoint().to_string());
    }
    // Admission linearizes under the registry read: replacement/quarantine
    // before this point rejects; later changes do not revoke physical I/O.
    Ok(mutates_namespace.then(|| ctx.begin_namespace_commit()))
}

async fn begin_legacy_local_rename_data_with_ctx(
    ctx: &Arc<InstanceContext>,
    disk_ref: &str,
    source: (&str, &str),
    fi: &FileInfo,
    destination: (&str, &str),
) -> DiskResult<LegacyRenameData> {
    let (disk, disk_id) = local_disk_candidate(ctx, disk_ref).await?;
    let mutates_namespace = !is_meta_bucketname(source.0) || !is_meta_bucketname(destination.0);
    let owner = admit_local_disk(ctx, &disk, disk_id, mutates_namespace).await?;
    disk.begin_legacy_rename_data(source, fi, destination, owner.map(|owner| owner as Arc<dyn Send + Sync>))
        .await
}

async fn rename_local_data_with_ctx(
    ctx: &Arc<InstanceContext>,
    disk_ref: &str,
    source: (&str, &str),
    fi: &FileInfo,
    destination: (&str, &str),
    mut guards: RenameDataGuards,
) -> DiskResult<RenameDataResp> {
    let handler_started = rustfs_io_metrics::put_stage_timer();
    let lookup_started = rustfs_io_metrics::put_stage_timer();
    let (disk, disk_id) = local_disk_candidate(ctx, disk_ref).await?;
    let lookup_ms = lookup_started.map(|started| started.elapsed().as_secs_f64() * 1000.0);
    if let Some(duration_ms) = lookup_ms {
        rustfs_io_metrics::record_put_object_stage_duration(PUT_STAGE_SET_DISK_RENAME_REMOTE_HANDLER_LOOKUP, duration_ms);
    }
    let mutates_namespace = !is_meta_bucketname(source.0) || !is_meta_bucketname(destination.0);
    let admit_started = rustfs_io_metrics::put_stage_timer();
    let owner = admit_local_disk(ctx, &disk, disk_id, mutates_namespace).await?;
    let admit_ms = admit_started.map(|started| started.elapsed().as_secs_f64() * 1000.0);
    if let Some(duration_ms) = admit_ms {
        rustfs_io_metrics::record_put_object_stage_duration(PUT_STAGE_SET_DISK_RENAME_REMOTE_HANDLER_ADMIT, duration_ms);
    }
    guards.namespace_owner = owner.as_ref().map(|owner| owner.clone() as Arc<dyn Send + Sync>);
    let local_rename_started = rustfs_io_metrics::put_stage_timer();
    let result = disk
        .rename_data_borrowed_with_fence_observed(source.0, source.1, fi, destination.0, destination.1, guards)
        .await
        .result;
    let local_rename_ms = local_rename_started.map(|started| started.elapsed().as_secs_f64() * 1000.0);
    if let Some(duration_ms) = local_rename_ms {
        rustfs_io_metrics::record_put_object_stage_duration(PUT_STAGE_SET_DISK_RENAME_REMOTE_HANDLER_LOCAL_RENAME, duration_ms);
    }
    drop(owner);
    if let Some(handler_started) = handler_started {
        let handler_ms = handler_started.elapsed().as_secs_f64() * 1000.0;
        rustfs_io_metrics::record_put_object_stage_duration(PUT_STAGE_SET_DISK_RENAME_REMOTE_HANDLER_TOTAL, handler_ms);
        let child_ms = lookup_ms.unwrap_or(0.0) + admit_ms.unwrap_or(0.0) + local_rename_ms.unwrap_or(0.0);
        let overhead_ms = (handler_ms - child_ms).max(0.0);
        rustfs_io_metrics::record_put_object_stage_duration(PUT_STAGE_SET_DISK_RENAME_REMOTE_HANDLER_OVERHEAD, overhead_ms);
    }
    result
}

async fn begin_legacy_local_file_write_with_ctx(
    ctx: &Arc<InstanceContext>,
    disk: &DiskStore,
    volume: &str,
    external_guard: Option<Arc<dyn Send + Sync>>,
) -> DiskResult<LegacyFileWrite> {
    let deadline = tokio::time::Instant::now() + crate::disk::disk_store::get_max_timeout_duration();
    tokio::time::timeout_at(deadline, async {
        // The HTTP target mutex belongs to this exact captured disk, never a
        // replacement resolved after the caller has joined its queue.
        let owner = admit_local_disk(ctx, disk, None, !is_meta_bucketname(volume)).await?;
        disk.begin_legacy_file_write_with_namespace_owner(volume, Some(Arc::new((owner, external_guard))))
            .await
    })
    .await
    .map_err(|_| DiskError::Timeout)?
}

async fn begin_legacy_local_delete_versions_with_ctx(
    ctx: &Arc<InstanceContext>,
    disk_ref: &str,
    volume: &str,
) -> DiskResult<LegacyFileWrite> {
    let (disk, disk_id) = local_disk_candidate(ctx, disk_ref).await?;
    let owner = admit_local_disk(ctx, &disk, disk_id, !is_meta_bucketname(volume)).await?;
    disk.begin_legacy_file_write_with_namespace_owner(volume, owner.map(|owner| owner as Arc<dyn Send + Sync>))
        .await
}

async fn begin_legacy_local_metadata_mutation_with_ctx(
    ctx: &Arc<InstanceContext>,
    disk_ref: &str,
    volume: &str,
    path: &str,
) -> DiskResult<LegacyMetadataWrite> {
    let (disk, disk_id) = local_disk_candidate(ctx, disk_ref).await?;
    let owner = admit_local_disk(ctx, &disk, disk_id, !is_meta_bucketname(volume)).await?;
    disk.begin_legacy_metadata_write_with_namespace_owner(volume, path, owner.map(|owner| owner as Arc<dyn Send + Sync>))
        .await
}

async fn undo_local_write_with_ctx(
    ctx: &Arc<InstanceContext>,
    disk_ref: &str,
    volume: &str,
    path: &str,
    fi: FileInfo,
    opts: DeleteOptions,
) -> DiskResult<()> {
    if !opts.undo_write {
        return Err(DiskError::other("target undo requires undo_write"));
    }
    let (disk, disk_id) = local_disk_candidate(ctx, disk_ref).await?;
    let owner = admit_local_disk(ctx, &disk, disk_id, !is_meta_bucketname(volume)).await?;
    let physical_owner = owner.as_ref().map(|owner| owner.clone() as Arc<dyn Send + Sync>);
    let result = disk
        .undo_write_with_namespace_owner(volume, path, fi, opts, physical_owner)
        .await;
    drop(owner);
    result
}

async fn remember_local_disk_id(disk: &DiskStore) -> Option<Uuid> {
    remember_local_disk_id_with_instance_ctx(&crate::runtime::global::current_ctx(), disk).await
}

async fn remember_local_disk_id_with_instance_ctx(instance_ctx: &Arc<InstanceContext>, disk: &DiskStore) -> Option<Uuid> {
    let disk_id = disk.get_disk_id().await.ok().flatten()?;
    record_local_disk_id_if_active(instance_ctx, disk, disk_id)
        .await
        .then_some(disk_id)
}

async fn record_local_disk_id_if_active(instance_ctx: &Arc<InstanceContext>, disk: &DiskStore, disk_id: Uuid) -> bool {
    let endpoint = disk.endpoint().to_string();
    let local_disk_map = instance_ctx.local_disk_map();
    let local_disks = local_disk_map.read().await;
    let Some(active_disk) = local_disks.get(&endpoint).and_then(Option::as_ref) else {
        return false;
    };
    if !Arc::ptr_eq(active_disk, disk) {
        return false;
    }

    // Lock order is local_disk_map -> local_disk_id_map so quarantine is the
    // linearization point for rejecting an in-flight stale disk snapshot.
    instance_ctx.local_disk_id_map().write().await.insert(disk_id, endpoint);
    true
}

pub async fn find_local_disk(disk_path: &str) -> Option<DiskStore> {
    runtime_sources::local_disk_by_path(disk_path).await
}

pub async fn find_local_disk_by_ref(disk_ref: &str) -> Option<DiskStore> {
    if let Some(disk) = find_local_disk(disk_ref).await {
        let _ = remember_local_disk_id(&disk).await;
        return Some(disk);
    }

    let Ok(disk_id) = Uuid::parse_str(disk_ref) else {
        return None;
    };

    if let Some(disk_path) = runtime_sources::local_disk_path_by_id(&disk_id).await
        && let Some(disk) = find_local_disk(&disk_path).await
    {
        return Some(disk);
    }

    for disk in all_local_disk().await {
        if remember_local_disk_id(&disk).await == Some(disk_id) {
            return Some(disk);
        }
    }

    None
}

pub async fn all_local_disk_path() -> Vec<String> {
    runtime_sources::local_disk_paths().await
}

pub async fn all_local_disk() -> Vec<DiskStore> {
    runtime_sources::local_disks().await
}

pub async fn prewarm_local_disk_id_map() {
    prewarm_local_disk_id_map_with_instance_ctx(&crate::runtime::global::current_ctx()).await
}

/// Prewarm the disk-id map of an explicit instance context (Phase 5 follow-up,
/// backlog#1052): startup passes the context whose disk map it just populated
/// instead of resolving the process-level default.
pub async fn prewarm_local_disk_id_map_with_instance_ctx(instance_ctx: &Arc<InstanceContext>) {
    let disks: Vec<DiskStore> = instance_ctx
        .local_disk_map()
        .read()
        .await
        .values()
        .filter_map(|v| v.as_ref().cloned())
        .collect();
    for disk in disks {
        if let Err(err) = disk.get_disk_id().await {
            debug!(
                event = EVENT_LOCAL_DISK_ID_PREWARM_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_DISK_STARTUP,
                disk_endpoint = %disk.endpoint(),
                error = %err,
                "Skipped local disk id prewarm"
            );
            continue;
        }

        let _ = remember_local_disk_id_with_instance_ctx(instance_ctx, &disk).await;
    }
}

pub async fn init_local_disks(endpoint_pools: EndpointServerPools) -> Result<()> {
    init_local_disks_with_instance_ctx(&crate::runtime::global::current_ctx(), endpoint_pools).await
}

/// Register the pools' local disks into an explicit instance context (Phase 5
/// follow-up, backlog#1052). The legacy [`init_local_disks`] entry resolves the
/// process-level default context; startup paths that own a context pass it here
/// so a future second instance's disks cannot leak into the first one's registry.
pub async fn init_local_disks_with_instance_ctx(
    instance_ctx: &Arc<InstanceContext>,
    endpoint_pools: EndpointServerPools,
) -> Result<()> {
    let opt = &DiskOption {
        cleanup: true,
        health_check: true,
    };

    runtime_sources::initialize_local_disk_maps(instance_ctx, endpoint_pools, opt).await
}

pub fn init_lock_clients(endpoint_pools: EndpointServerPools) {
    let mut unique_endpoints: HashMap<String, &Endpoint> = HashMap::new();

    for pool_eps in endpoint_pools.as_ref().iter() {
        for ep in pool_eps.endpoints.as_ref().iter() {
            unique_endpoints.insert(ep.host_port(), ep);
        }
    }

    let mut clients = HashMap::new();
    let mut first_local_client_set = false;

    for (key, endpoint) in unique_endpoints {
        if endpoint.is_local {
            let local_client = Arc::new(LocalClient::new()) as Arc<dyn LockClient>;

            // Store the first LocalClient globally for use by other modules
            if !first_local_client_set {
                if let Err(e) = runtime_sources::set_primary_lock_client(local_client.clone()) {
                    // If already set, ignore the error (another thread may have set it)
                    debug!(
                        event = EVENT_LOCK_CLIENT_INITIALIZATION_FAILED,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DISK_STARTUP,
                        error = ?e,
                        reason = "global_lock_client_already_set",
                        "Skipped global lock client publication"
                    );
                } else {
                    first_local_client_set = true;
                }
            }

            clients.insert(key, local_client);
        } else {
            clients.insert(key, Arc::new(RemoteClient::new(endpoint.url.to_string())) as Arc<dyn LockClient>);
        }
    }

    // Store the lock clients map globally
    if runtime_sources::set_lock_clients(clients).is_err() {
        error!(
            event = EVENT_LOCK_CLIENT_INITIALIZATION_FAILED,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_DISK_STARTUP,
            reason = "set_global_lock_clients_failed",
            "Failed to initialize lock clients"
        );
    }
}

fn endpoint_rpc_authority(endpoint: &Endpoint) -> Option<String> {
    let host = endpoint.url.host_str()?;
    let host = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_string()
    };
    Some(match endpoint.url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host,
    })
}

pub(super) async fn init_local_peer(endpoint_pools: &EndpointServerPools, host: &String, port: &String) {
    let mut peer_set = Vec::new();
    endpoint_pools.as_ref().iter().for_each(|endpoints| {
        endpoints.endpoints.as_ref().iter().for_each(|endpoint| {
            if endpoint.get_type() == EndpointType::Url
                && endpoint.is_local
                && let Some(authority) = endpoint_rpc_authority(endpoint)
            {
                peer_set.push(authority);
            }
        });
    });

    if peer_set.is_empty() {
        if !host.is_empty() {
            runtime_sources::set_local_node_name(format!("{host}:{port}")).await;
            return;
        }

        runtime_sources::set_local_node_name(format!("127.0.0.1:{port}")).await;
        return;
    }

    runtime_sources::set_local_node_name(peer_set[0].clone()).await;
}

pub async fn get_disk_infos(disks: &[Option<DiskStore>]) -> Vec<Option<DiskInfo>> {
    let opts = &DiskInfoOptions::default();
    let mut res = vec![None; disks.len()];
    for (idx, disk_op) in disks.iter().enumerate() {
        if let Some(disk) = disk_op
            && let Ok(info) = disk.disk_info(opts).await
        {
            res[idx] = Some(info);
        }
    }

    res
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::new_disk;
    use crate::layout::endpoints::{Endpoints, PoolEndpoints};

    #[test]
    fn artifact_repair_path_length_uses_logical_directory_key() {
        use rustfs_utils::path::encode_dir_object;

        for len in 1017..=1024 {
            let key = format!("{}/", "a".repeat(len - 1));
            assert!(canonical_artifact_path(&encode_dir_object(&key)), "logical length {len}");
        }
        assert!(canonical_artifact_path(&"a".repeat(1024)));
        assert!(!canonical_artifact_path(&"a".repeat(1025)));
        assert!(!canonical_artifact_path(&encode_dir_object(&format!("{}/", "a".repeat(1024)))));
        assert!(!canonical_artifact_path(&format!("{}__XLDIR____XLDIR__", "a".repeat(1023))));
        for path in ["", "a//b", "a/../b", "a\\b", "a\0b"] {
            assert!(!canonical_artifact_path(path), "noncanonical path {path:?}");
        }
    }

    fn single_local_disk_pools(dir: &std::path::Path) -> EndpointServerPools {
        let mut endpoint = Endpoint::try_from(dir.to_str().expect("temp dir path should be utf-8")).expect("local endpoint");
        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(0);

        EndpointServerPools(vec![PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 1,
            endpoints: Endpoints::from(vec![endpoint]),
            cmd_line: "instance-ctx-disk-registry-test".to_string(),
            platform: "test".to_string(),
        }])
    }

    #[tokio::test(start_paused = true)]
    async fn legacy_path_delete_admission_deadline_bounds_candidate_wait() {
        let ctx = Arc::new(InstanceContext::new());
        let registry = ctx.local_disk_map();
        let guard = registry.write().await;
        let disk_ref = Uuid::new_v4().to_string();
        for variant in 0..4 {
            let mut admission = Box::pin(async {
                if variant < 2 {
                    let operation = if variant == 0 {
                        LegacyPathDeleteOperation::Delete
                    } else {
                        LegacyPathDeleteOperation::DeletePaths
                    };
                    begin_legacy_local_path_delete_with_ctx(&ctx, &disk_ref, "bucket", operation, None)
                        .await
                        .map(|_| ())
                } else {
                    begin_legacy_local_residual_version_delete_with_ctx(&ctx, &disk_ref, "bucket", "object", variant == 3)
                        .await
                        .map(|_| ())
                }
            });
            assert!(futures::poll!(admission.as_mut()).is_pending(), "candidate is waiting on the registry");
            tokio::time::advance(crate::disk::disk_store::get_max_timeout_duration() + std::time::Duration::from_secs(1)).await;
            assert_eq!(
                tokio::time::timeout(std::time::Duration::from_secs(1), admission).await,
                Ok(Err(DiskError::Timeout)),
                "candidate timeout must not become a lookup error"
            );
        }
        drop(guard);
        assert!(!ctx.namespace_commits_pending(), "timed-out lookup never admits a writer");
    }

    async fn target_disk(ctx: &Arc<InstanceContext>, root: &std::path::Path, id: Uuid) -> DiskStore {
        let mut format = crate::layout::format::FormatV3::new(1, 1);
        format.erasure.this = id;
        format.erasure.sets[0][0] = id;
        let meta = root.join(crate::disk::RUSTFS_META_BUCKET);
        tokio::fs::create_dir_all(&meta).await.expect("create format volume");
        tokio::fs::write(
            meta.join(crate::disk::FORMAT_CONFIG_FILE),
            serde_json::to_vec(&format).expect("encode format"),
        )
        .await
        .expect("write real disk identity");
        let mut endpoint = Endpoint::try_from(root.to_str().expect("UTF-8 root")).expect("endpoint");
        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(0);
        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("open real local disk");
        assert_eq!(disk.get_disk_id().await.expect("read disk format identity"), Some(id));
        ctx.local_disk_map()
            .write()
            .await
            .insert(disk.endpoint().to_string(), Some(disk.clone()));
        disk
    }

    fn target_file_info(object: &str, version: Uuid, body: &'static [u8]) -> FileInfo {
        let mut fi = FileInfo::new(object, 1, 0);
        fi.erasure.index = 1;
        fi.version_id = Some(version);
        fi.mod_time = Some(OffsetDateTime::now_utc());
        fi.size = i64::try_from(body.len()).expect("fixture length");
        fi.parts = vec![rustfs_filemeta::ObjectPartInfo {
            number: 1,
            size: body.len(),
            actual_size: fi.size,
            ..Default::default()
        }];
        fi.data = Some(bytes::Bytes::from_static(body));
        fi.set_inline_data();
        fi
    }

    async fn seed_target(disk: &DiskStore, volume: &str, object: &str, fi: FileInfo) -> Vec<u8> {
        let dir = disk.path().join(volume);
        tokio::fs::create_dir_all(&dir).await.expect("real fixture volume");
        disk.write_metadata(volume, volume, object, fi.clone())
            .await
            .expect("seed real metadata");
        let read = disk
            .read_version(
                volume,
                volume,
                object,
                &fi.version_id.expect("fixture version").to_string(),
                &crate::disk::ReadOptions {
                    read_data: true,
                    ..Default::default()
                },
            )
            .await
            .expect("read fixture before mutation");
        assert_eq!(read.data, fi.data, "fixture must contain readable inline bytes");
        tokio::fs::read(dir.join(object).join(crate::disk::STORAGE_FORMAT_FILE))
            .await
            .expect("seeded metadata bytes")
    }

    #[tokio::test]
    async fn legacy_undo_target_keeps_same_uuid_disk_after_registry_replacement() {
        for warm in [false, true] {
            let ctx_a = Arc::new(InstanceContext::new());
            let ctx_b = Arc::new(InstanceContext::new());
            let root_a = tempfile::tempdir().expect("undo A root");
            let root_b = tempfile::tempdir().expect("undo B root");
            let id = Uuid::new_v4();
            let disk_a = target_disk(&ctx_a, root_a.path(), id).await;
            let disk_b = target_disk(&ctx_b, root_b.path(), id).await;
            if warm {
                assert!(record_local_disk_id_if_active(&ctx_a, &disk_a, id).await);
                assert!(record_local_disk_id_if_active(&ctx_b, &disk_b, id).await);
            }
            let version = Uuid::new_v4();
            let previous_a = target_file_info("object", version, b"complete old A payload");
            let previous_b = target_file_info("object", version, b"complete old B payload");
            let a_before = seed_target(&disk_a, "undo-target", "object", previous_a.clone()).await;
            seed_target(&disk_b, "undo-target", "object", previous_b).await;
            let incoming = target_file_info("object", version, b"complete new rollback payload");
            let mut backups = Vec::new();
            for disk in [&disk_a, &disk_b] {
                seed_target(disk, ".rustfs.sys/tmp", "staged", incoming.clone()).await;
                let commit = disk
                    .rename_data(".rustfs.sys/tmp", "staged", incoming.clone(), "undo-target", "object")
                    .await
                    .expect("real overwrite creates backup");
                backups.push(commit.rollback_data_dir.expect("rollback backup"));
            }
            let b_meta = root_b.path().join("undo-target/object/xl.meta");
            let b_before = tokio::fs::read(&b_meta).await.expect("B committed metadata");
            let b_backup = root_b
                .path()
                .join("undo-target/object")
                .join(backups[1].to_string())
                .join("xl.meta.bkp");
            let b_backup_before = tokio::fs::read(&b_backup).await.expect("B rollback backup");
            let store = super::super::tests::build_store_with_ctx(ctx_a.clone());
            let generation = ctx_a.namespace_commit_generation();
            let token = store
                .begin_legacy_local_metadata_mutation(&id.to_string(), "undo-target", "object")
                .await
                .expect("capture A through real UUID lookup");
            assert!(ctx_a.namespace_commits_pending());
            assert!(!ctx_b.namespace_commits_pending());
            {
                let map = ctx_a.local_disk_map();
                let mut active = map.write().await;
                active.clear();
                active.insert(disk_b.endpoint().to_string(), Some(disk_b.clone()));
            }
            ctx_a
                .local_disk_id_map()
                .write()
                .await
                .insert(id, disk_b.endpoint().to_string());
            let (replacement, _) = local_disk_candidate(&ctx_a, &id.to_string())
                .await
                .expect("later lookup resolves B");
            assert!(Arc::ptr_eq(&replacement, &disk_b));
            token
                .undo_write(
                    incoming,
                    DeleteOptions {
                        undo_write: true,
                        old_data_dir: Some(backups[0]),
                        ..Default::default()
                    },
                )
                .await
                .expect("admitted undo remains on captured A");
            assert_eq!(
                tokio::fs::read(root_a.path().join("undo-target/object/xl.meta"))
                    .await
                    .expect("restored A metadata"),
                a_before
            );
            let actual = disk_a
                .read_version(
                    "undo-target",
                    "undo-target",
                    "object",
                    &version.to_string(),
                    &crate::disk::ReadOptions {
                        read_data: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("A restored version");
            assert_eq!(actual.version_id, Some(version));
            assert_eq!(actual.data, previous_a.data);
            assert_eq!(tokio::fs::read(&b_meta).await.expect("B unchanged metadata"), b_before);
            assert_eq!(tokio::fs::read(&b_backup).await.expect("B unchanged backup"), b_backup_before);
            assert!(!ctx_a.namespace_commits_pending());
            assert_eq!(ctx_a.namespace_commit_generation(), generation + 2);
            assert_eq!(ctx_b.namespace_commit_generation(), 0);
        }
    }

    #[tokio::test]
    async fn legacy_undo_bootstrap_checks_raw_volumes_and_allows_internal_rollback() {
        let ctx = Arc::new(InstanceContext::new());
        let root = tempfile::tempdir().expect("bootstrap undo root");
        let disk = target_disk(&ctx, root.path(), Uuid::new_v4()).await;
        let target = BootstrapLocalTarget::new(ctx.clone());
        let fi = target_file_info("object", Uuid::new_v4(), b"unchanged ordinary payload");
        let before = seed_target(&disk, "victim", "object", fi).await;
        for volume in [
            "victim",
            ".rustfs.sys/../victim",
            ".rustfs.sys/./tmp",
            ".rustfs.sys/ .. /victim",
            ".rustfs.sys\\..\\victim",
            ".minio.sys/../victim",
        ] {
            assert_eq!(
                target
                    .begin_legacy_local_metadata_mutation(&disk.endpoint().to_string(), volume, "object")
                    .await
                    .expect_err("bootstrap raw volume must reject before admission"),
                DiskError::FileAccessDenied,
                "{volume}"
            );
        }
        assert_eq!(
            tokio::fs::read(root.path().join("victim/object/xl.meta"))
                .await
                .expect("ordinary source unchanged"),
            before
        );
        for volume in [".rustfs.sys/tmp", ".rustfs.sys/multipart", ".minio.sys/config"] {
            let previous = target_file_info("object", Uuid::new_v4(), b"complete internal previous payload");
            let before = seed_target(&disk, volume, "object", previous.clone()).await;
            let incoming = target_file_info("object", previous.version_id.expect("version"), b"new internal overwritten payload");
            seed_target(&disk, volume, "staged", incoming.clone()).await;
            let commit = disk
                .rename_data(volume, "staged", incoming.clone(), volume, "object")
                .await
                .expect("real internal overwrite");
            target
                .begin_legacy_local_metadata_mutation(&disk.endpoint().to_string(), volume, "object")
                .await
                .expect("legitimate internal prepare")
                .undo_write(
                    incoming,
                    DeleteOptions {
                        undo_write: true,
                        old_data_dir: Some(commit.rollback_data_dir.expect("internal rollback backup")),
                        ..Default::default()
                    },
                )
                .await
                .expect("prepared internal undo");
            assert_eq!(
                tokio::fs::read(root.path().join(volume).join("object/xl.meta"))
                    .await
                    .expect("restored internal bytes"),
                before
            );
            let actual = disk
                .read_version(
                    volume,
                    volume,
                    "object",
                    &previous.version_id.expect("version").to_string(),
                    &crate::disk::ReadOptions {
                        read_data: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("restored internal version");
            assert_eq!(actual.version_id, previous.version_id);
            assert_eq!(actual.data, previous.data);
            assert_eq!(ctx.namespace_commit_generation(), 0);
            assert!(!ctx.namespace_commits_pending());
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn legacy_undo_target_cancellation_retains_instance_owner_until_restore_drains() {
        use crate::disk::os::{self, prepared_publication_test_hooks as hooks};
        use std::time::Duration;
        let ctx = Arc::new(InstanceContext::new());
        let sibling = Arc::new(InstanceContext::new());
        let root = tempfile::tempdir().expect("physical undo owner root");
        let disk = target_disk(&ctx, root.path(), Uuid::new_v4()).await;
        let previous = target_file_info("object", Uuid::new_v4(), b"complete previous rollback payload");
        let before = seed_target(&disk, "undo-owner", "object", previous.clone()).await;
        let incoming = target_file_info("object", previous.version_id.expect("version"), b"complete overwritten payload");
        seed_target(&disk, ".rustfs.sys/tmp", "staged", incoming.clone()).await;
        let committed = disk
            .rename_data(".rustfs.sys/tmp", "staged", incoming.clone(), "undo-owner", "object")
            .await
            .expect("real overwrite before cancellation");
        let store = super::super::tests::build_store_with_ctx(ctx.clone());
        let generation = ctx.namespace_commit_generation();
        let token = store
            .begin_legacy_local_metadata_mutation(&disk.endpoint().to_string(), "undo-owner", "object")
            .await
            .expect("prepare with captured instance owner");
        let destination = disk
            .get_object_path_for_io_if_local("undo-owner", "object/xl.meta")
            .expect("local disk")
            .expect("physical metadata key");
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let _hook = hooks::install_at(hooks::Stage::Rename, &destination, move || {
            let _ = entered_tx.send(());
            let _ = release_rx.recv();
        });
        let task = tokio::spawn(async move {
            token
                .undo_write(
                    incoming,
                    DeleteOptions {
                        undo_write: true,
                        old_data_dir: Some(committed.rollback_data_dir.expect("real backup")),
                        ..Default::default()
                    },
                )
                .await
        });
        let entered = tokio::time::timeout(Duration::from_secs(10), entered_rx).await;
        task.abort();
        let joined = task.await;
        let pending = ctx.namespace_commits_pending();
        let generation_while_paused = ctx.namespace_commit_generation();
        let volume_held = os::disk_volume_mutation_lock(&disk.path(), "undo-owner")
            .try_write_owned()
            .is_err();
        let sibling_untouched = !sibling.namespace_commits_pending() && sibling.namespace_commit_generation() == 0;
        drop(release_tx);
        tokio::time::timeout(Duration::from_secs(10), hooks::drain_namespace_key(&destination))
            .await
            .expect("physical restore releases final namespace");
        tokio::time::timeout(Duration::from_secs(10), async {
            while ctx.namespace_commits_pending() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("physical completion releases instance owner");
        entered
            .expect("bounded physical entry")
            .expect("actual restore worker entered");
        assert!(joined.expect_err("waiter cancellation acknowledged").is_cancelled());
        assert!(pending, "cancelled waiter must not drop physical instance owner");
        assert_eq!(generation_while_paused, generation + 1);
        assert!(volume_held, "actual worker retains admitted volume READ");
        assert!(sibling_untouched);
        assert_eq!(ctx.namespace_commit_generation(), generation + 2);
        assert_eq!(
            tokio::fs::read(root.path().join("undo-owner/object/xl.meta"))
                .await
                .expect("physical restored metadata"),
            before
        );
        let actual = disk
            .read_version(
                "undo-owner",
                "undo-owner",
                "object",
                &previous.version_id.expect("version").to_string(),
                &crate::disk::ReadOptions {
                    read_data: true,
                    ..Default::default()
                },
            )
            .await
            .expect("read restored payload after real drain");
        assert_eq!(actual.version_id, previous.version_id);
        assert_eq!(actual.data, previous.data);
    }

    #[tokio::test]
    async fn target_uuid_lookup_binds_real_disk_and_owner_to_one_instance() {
        for warm in [false, true] {
            let ctx_a = Arc::new(InstanceContext::new());
            let ctx_b = Arc::new(InstanceContext::new());
            let a = tempfile::tempdir().expect("A root");
            let b = tempfile::tempdir().expect("B root");
            let id = Uuid::new_v4();
            let disk_a = target_disk(&ctx_a, a.path(), id).await;
            let disk_b = target_disk(&ctx_b, b.path(), id).await;
            if warm {
                assert!(record_local_disk_id_if_active(&ctx_a, &disk_a, id).await);
                assert!(record_local_disk_id_if_active(&ctx_b, &disk_b, id).await);
            }
            let version = Uuid::new_v4();
            let fi = target_file_info("destination", version, b"new-A");
            for disk in [&disk_a, &disk_b] {
                seed_target(disk, "target-bucket", "staged", fi.clone()).await;
            }
            let b_before = seed_target(
                &disk_b,
                "target-bucket",
                "destination",
                target_file_info("destination", version, b"old-B"),
            )
            .await;
            let store = super::super::tests::build_store_with_ctx(ctx_a.clone());
            store
                .rename_local_data(&id.to_string(), ("target-bucket", "staged"), &fi, ("target-bucket", "destination"), None)
                .await
                .expect("rename on A");
            let read = disk_a
                .read_version(
                    "target-bucket",
                    "target-bucket",
                    "destination",
                    &version.to_string(),
                    &crate::disk::ReadOptions {
                        read_data: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("read committed A");
            assert_eq!(read.data, fi.data, "warm={warm}");
            assert_eq!(
                tokio::fs::read(b.path().join("target-bucket/destination/xl.meta"))
                    .await
                    .expect("B metadata"),
                b_before
            );
            assert!(b.path().join("target-bucket/staged/xl.meta").exists());
            assert!(ctx_a.namespace_commit_generation() > 0);
            assert_eq!(ctx_b.namespace_commit_generation(), 0);
            assert!(!ctx_a.namespace_commits_pending());
            assert!(!ctx_b.namespace_commits_pending());
            assert_eq!(ctx_a.local_disk_id_map().read().await.get(&id), Some(&disk_a.endpoint().to_string()));
        }
    }

    #[tokio::test]
    async fn target_admission_rejects_removed_quarantined_and_replaced_arcs() {
        let ctx = Arc::new(InstanceContext::new());
        let root = tempfile::tempdir().expect("root");
        let disk = target_disk(&ctx, root.path(), Uuid::new_v4()).await;
        let endpoint = disk.endpoint().to_string();
        for state in ["removed", "quarantined", "replaced"] {
            let replacement = new_disk(
                &disk.endpoint(),
                &DiskOption {
                    cleanup: false,
                    health_check: false,
                },
            )
            .await
            .expect("separate active Arc");
            let map = ctx.local_disk_map();
            let mut entries = map.write().await;
            match state {
                "removed" => {
                    entries.remove(&endpoint);
                }
                "quarantined" => {
                    entries.insert(endpoint.clone(), None);
                }
                _ => {
                    entries.insert(endpoint.clone(), Some(replacement));
                }
            }
            drop(entries);
            assert!(
                matches!(admit_local_disk(&ctx, &disk, None, true).await, Err(DiskError::DiskNotFound)),
                "{state}"
            );
            assert!(!ctx.namespace_commits_pending());
            assert_eq!(ctx.namespace_commit_generation(), 0);
        }
    }

    #[tokio::test]
    async fn target_uuid_cache_cannot_admit_a_different_format_at_the_same_path() {
        let ctx = Arc::new(InstanceContext::new());
        let root = tempfile::tempdir().expect("root");
        let old_id = Uuid::new_v4();
        let old = target_disk(&ctx, root.path(), old_id).await;
        assert!(record_local_disk_id_if_active(&ctx, &old, old_id).await);
        let replacement_id = Uuid::new_v4();
        let replacement = target_disk(&ctx, root.path(), replacement_id).await;
        assert!(!Arc::ptr_eq(&old, &replacement));
        assert!(matches!(
            local_disk_candidate(&ctx, &old_id.to_string()).await,
            Err(DiskError::DiskNotFound)
        ));
        let (candidate, verified) = local_disk_candidate(&ctx, &replacement_id.to_string())
            .await
            .expect("replacement UUID");
        assert!(Arc::ptr_eq(&candidate, &replacement));
        assert_eq!(verified, Some(replacement_id));
        assert!(!ctx.namespace_commits_pending());
    }

    #[tokio::test]
    async fn bootstrap_rejects_user_volumes_aliases_and_scanner_tokens_without_mutation() {
        let ctx = Arc::new(InstanceContext::new());
        let root = tempfile::tempdir().expect("root");
        let disk = target_disk(&ctx, root.path(), Uuid::new_v4()).await;
        let target = BootstrapLocalTarget::new(ctx.clone());
        let fi = target_file_info("destination", Uuid::new_v4(), b"body");
        let user_before = seed_target(&disk, "victim", "staged", fi.clone()).await;
        let meta_before = seed_target(&disk, ".rustfs.sys/tmp", "staged", fi.clone()).await;
        for invalid in [
            "victim",
            ".rustfs.sys/../victim",
            ".rustfs.sys/./tmp",
            ".rustfs.sys/ .. /victim",
            ".rustfs.sys\\..\\victim",
            ".minio.sys/../victim",
        ] {
            for (src, dst) in [(invalid, ".rustfs.sys/tmp"), (".rustfs.sys/tmp", invalid)] {
                assert_eq!(
                    target
                        .begin_legacy_local_rename_data(&disk.endpoint().to_string(), (src, "staged"), &fi, (dst, "destination"))
                        .await
                        .expect_err("bootstrap prepare must validate both raw volumes"),
                    DiskError::FileAccessDenied,
                    "src={src}, dst={dst}"
                );
                assert!(
                    target
                        .rename_local_data(&disk.endpoint().to_string(), (src, "staged"), &fi, (dst, "destination"), None)
                        .await
                        .is_err(),
                    "src={src}, dst={dst}"
                );
            }
            assert!(
                target
                    .undo_local_write(
                        &disk.endpoint().to_string(),
                        invalid,
                        "staged",
                        fi.clone(),
                        DeleteOptions {
                            undo_write: true,
                            ..Default::default()
                        }
                    )
                    .await
                    .is_err(),
                "{invalid}"
            );
        }
        assert!(
            target
                .rename_local_data(
                    &disk.endpoint().to_string(),
                    (".rustfs.sys/tmp", "staged"),
                    &fi,
                    (".rustfs.sys/tmp", "destination"),
                    Some(Uuid::new_v4())
                )
                .await
                .is_err()
        );
        assert_eq!(
            tokio::fs::read(root.path().join("victim/staged/xl.meta"))
                .await
                .expect("user source"),
            user_before
        );
        assert_eq!(
            tokio::fs::read(root.path().join(".rustfs.sys/tmp/staged/xl.meta"))
                .await
                .expect("metadata source"),
            meta_before
        );
        assert!(!root.path().join("victim/destination").exists());
        assert!(!root.path().join(".rustfs.sys/tmp/destination").exists());
        assert_eq!(ctx.namespace_commit_generation(), 0);
        assert!(!ctx.namespace_commits_pending());
    }

    #[tokio::test]
    async fn bootstrap_allows_internal_multisegment_rename_without_namespace_owner() {
        for volume in [".rustfs.sys/tmp", ".rustfs.sys/multipart", ".minio.sys/config"] {
            let ctx = Arc::new(InstanceContext::new());
            let root = tempfile::tempdir().expect("root");
            let disk = target_disk(&ctx, root.path(), Uuid::new_v4()).await;
            let fi = target_file_info("destination", Uuid::new_v4(), b"internal-CAS-body");
            seed_target(&disk, volume, "staged", fi.clone()).await;
            BootstrapLocalTarget::new(ctx.clone())
                .rename_local_data(&disk.endpoint().to_string(), (volume, "staged"), &fi, (volume, "destination"), None)
                .await
                .expect("legitimate bootstrap metadata write");
            let read = disk
                .read_version(
                    volume,
                    volume,
                    "destination",
                    &fi.version_id.expect("version").to_string(),
                    &crate::disk::ReadOptions {
                        read_data: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("read bootstrap result");
            assert_eq!(read.data, fi.data);
            seed_target(&disk, volume, "prepared-staged", fi.clone()).await;
            BootstrapLocalTarget::new(ctx.clone())
                .begin_legacy_local_rename_data(
                    &disk.endpoint().to_string(),
                    (volume, "prepared-staged"),
                    &fi,
                    (volume, "prepared-destination"),
                )
                .await
                .expect("legitimate bootstrap preparation")
                .commit()
                .await
                .expect("legitimate prepared bootstrap commit");
            let prepared_read = disk
                .read_version(
                    volume,
                    volume,
                    "prepared-destination",
                    &fi.version_id.expect("version").to_string(),
                    &crate::disk::ReadOptions {
                        read_data: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("read prepared bootstrap result");
            assert_eq!(prepared_read.version_id, fi.version_id);
            assert_eq!(prepared_read.data, fi.data);
            assert_eq!(ctx.namespace_commit_generation(), 0);
            assert!(!ctx.namespace_commits_pending());
        }
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn target_user_source_to_internal_destination_retains_owner_after_cancellation() {
        use crate::disk::os::prepared_publication_test_hooks as hooks;
        use futures::FutureExt;
        use std::time::Duration;

        let ctx = Arc::new(InstanceContext::new());
        let root = tempfile::tempdir().expect("source-volume root");
        let disk = target_disk(&ctx, root.path(), Uuid::new_v4()).await;
        let store = super::super::tests::build_store_with_ctx(ctx.clone());
        let version = Uuid::new_v4();
        let fi = target_file_info("object", version, b"user-source-inline-body");
        fi.validate_for_metadata_read().expect("valid real inline metadata");
        let source_before = seed_target(&disk, "photos", "object", fi.clone()).await;
        assert!(!source_before.is_empty());
        tokio::fs::create_dir_all(root.path().join(crate::disk::RUSTFS_META_TMP_BUCKET))
            .await
            .expect("internal staging volume");
        let destination = disk
            .get_object_path_for_io_if_local(crate::disk::RUSTFS_META_TMP_BUCKET, "object")
            .expect("local disk")
            .expect("actual destination object key");
        let destination_metadata = destination.join(crate::disk::STORAGE_FORMAT_FILE);
        assert!(!destination_metadata.exists());
        assert!(!ctx.namespace_commits_pending());
        let generation = ctx.namespace_commit_generation();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let _hook = hooks::install(&destination_metadata, move || {
            let _ = entered_tx.send(());
            let _ = release_rx.recv();
        });
        let disk_ref = disk.endpoint().to_string();
        let rename_fi = fi.clone();
        let mut rename = tokio::spawn(async move {
            store
                .rename_local_data(
                    &disk_ref,
                    ("photos", "object"),
                    &rename_fi,
                    (crate::disk::RUSTFS_META_TMP_BUCKET, "object"),
                    None,
                )
                .await
        });
        let mut entered = false;
        let mut joined = false;
        let observations = std::panic::AssertUnwindSafe(async {
            tokio::time::timeout(Duration::from_secs(10), async {
                tokio::select! {
                    result = &mut rename => {
                        joined = true;
                        panic!("rename completed before prepared publication: {result:?}");
                    }
                    result = entered_rx => {
                        result.expect("real prepared rename must enter");
                        entered = true;
                    }
                }
            })
            .await
            .expect("bounded physical entry");
            let at_entry = (ctx.namespace_commits_pending(), ctx.namespace_commit_generation());
            assert!(!rename.is_finished(), "caller must still await the paused physical rename");
            rename.abort();
            let cancelled = tokio::time::timeout(Duration::from_secs(5), &mut rename)
                .await
                .expect("caller cancellation must finish while physical publication is paused");
            joined = true;
            let after_cancel = (ctx.namespace_commits_pending(), ctx.namespace_commit_generation());
            (at_entry, after_cancel, cancelled)
        })
        .catch_unwind()
        .await;

        // Release on every observation failure. Pending alone is not a drain
        // oracle: the implementation under test can fail to create the owner.
        drop(release_tx);
        if !joined {
            rename.abort();
            joined = tokio::time::timeout(Duration::from_secs(5), &mut rename).await.is_ok();
        }
        let physical_drained = tokio::time::timeout(Duration::from_secs(10), hooks::drain_namespace_key(&destination)).await;
        let owner_drained = tokio::time::timeout(Duration::from_secs(10), async {
            while ctx.namespace_commits_pending() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        if !entered || !joined || physical_drained.is_err() || owner_drained.is_err() {
            // Without proven physical entry/drain, keep the root instead of
            // deleting files that a detached local executor may still use.
            let retained = root.keep();
            eprintln!("source-volume cleanup incomplete: entered={entered}, joined={joined}, retained={retained:?}");
            if let Err(panic) = observations {
                std::panic::resume_unwind(panic);
            }
            panic!("source-volume physical cleanup did not finish: retained={retained:?}");
        }
        let (at_entry, after_cancel, cancelled) = match observations {
            Ok(observations) => observations,
            Err(panic) => std::panic::resume_unwind(panic),
        };
        let latest = tokio::time::timeout(
            Duration::from_secs(5),
            disk.read_version(
                crate::disk::RUSTFS_META_TMP_BUCKET,
                crate::disk::RUSTFS_META_TMP_BUCKET,
                "object",
                "",
                &crate::disk::ReadOptions {
                    read_data: true,
                    ..Default::default()
                },
            ),
        )
        .await;
        let source =
            tokio::time::timeout(Duration::from_secs(5), tokio::fs::read(root.path().join("photos/object/xl.meta"))).await;
        let after_drain = (ctx.namespace_commits_pending(), ctx.namespace_commit_generation());
        assert!(cancelled.expect_err("caller must return cancellation").is_cancelled());
        let latest = latest
            .expect("latest read must finish")
            .expect("late physical commit must be readable");
        assert_eq!(latest.data, fi.data);
        assert_eq!(latest.version_id, Some(version));
        assert_eq!(
            source
                .expect("source observation must finish")
                .expect_err("user source metadata must have moved")
                .kind(),
            std::io::ErrorKind::NotFound
        );
        assert_eq!(
            (at_entry, after_cancel, after_drain),
            ((true, generation + 1), (true, generation + 1), (false, generation + 2)),
            "a real user source mutation must remain counted through its cancelled caller and physical drain"
        );
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn target_rename_cancellation_retains_real_namespace_and_scanner_owners() {
        use crate::disk::os::prepared_publication_test_hooks as hooks;
        let ctx = Arc::new(InstanceContext::new());
        let sibling = Arc::new(InstanceContext::new());
        let root = tempfile::tempdir().expect("root");
        let disk = target_disk(&ctx, root.path(), Uuid::new_v4()).await;
        let store = super::super::tests::build_store_with_ctx(ctx.clone());
        let fi = target_file_info("destination", Uuid::new_v4(), b"physically-owned");
        seed_target(&disk, "target-bucket", "staged", fi.clone()).await;
        let (token, _) = store
            .acquire_scanner_publication_lease(0, crate::runtime::instance::SCANNER_PUBLICATION_LEASE_TTL)
            .await
            .expect("real scanner token in A");
        let destination = disk
            .get_object_path_for_io_if_local("target-bucket", "destination/xl.meta")
            .expect("local disk")
            .expect("destination IO path");
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let _hook = hooks::install(&destination, move || {
            let _ = entered_tx.send(());
            let _ = release_rx.recv();
        });
        let disk_ref = disk.endpoint().to_string();
        let mut rename = Box::pin(store.rename_local_data(
            &disk_ref,
            ("target-bucket", "staged"),
            &fi,
            ("target-bucket", "destination"),
            Some(token),
        ));
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::select! {
                result = &mut rename => panic!("rename completed before physical pause: {result:?}"),
                entered = entered_rx => entered.expect("physical rename entered"),
            }
        })
        .await
        .expect("bounded physical entry");
        drop(rename);
        assert!(
            !store.scanner_data_usage_publication_blocked().await,
            "a pending namespace commit is not a local publication barrier"
        );
        assert!(ctx.namespace_commits_pending());
        assert!(!sibling.namespace_commits_pending());
        // The pending physical rename owns the namespace commit, which is
        // ordinary namespace activity and no longer fences another scanner
        // admission on the same token.
        drop(
            store
                .acquire_scanner_publication_lease_guard(token)
                .await
                .expect("a pending ordinary rename does not fence another scanner admission"),
        );
        assert!(store.release_scanner_publication_lease(token).await, "remove registered token");
        let gate = ctx.data_movement_operation_gate();
        assert!(
            gate.clone().try_write_owned().is_err(),
            "physical operation still owns the scanner read guard"
        );
        drop(release_tx);
        let _drained = tokio::time::timeout(std::time::Duration::from_secs(10), gate.write_owned())
            .await
            .expect("physical tail must release scanner guard");
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while ctx.namespace_commits_pending() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("namespace owner drains");
        let read = disk
            .read_version(
                "target-bucket",
                "target-bucket",
                "destination",
                &fi.version_id.expect("version").to_string(),
                &crate::disk::ReadOptions {
                    read_data: true,
                    ..Default::default()
                },
            )
            .await
            .expect("read actual late commit");
        assert_eq!(read.data, fi.data);
        assert!(ctx.namespace_commit_generation() >= 2);
        assert_eq!(sibling.namespace_commit_generation(), 0);
    }

    #[tokio::test]
    async fn target_ready_rejects_unknown_foreign_released_and_expired_scanner_tokens() {
        let ctx = Arc::new(InstanceContext::new());
        let other = Arc::new(InstanceContext::new());
        let store = super::super::tests::build_store_with_ctx(ctx.clone());
        let other_store = super::super::tests::build_store_with_ctx(other);
        let root = tempfile::tempdir().expect("root");
        let disk = target_disk(&ctx, root.path(), Uuid::new_v4()).await;
        let fi = target_file_info("destination", Uuid::new_v4(), b"unchanged");
        let before = seed_target(&disk, "target-bucket", "staged", fi.clone()).await;
        let ttl = crate::runtime::instance::SCANNER_PUBLICATION_LEASE_TTL;
        let (foreign, _) = other_store.acquire_scanner_publication_lease(0, ttl).await.expect("B token");
        let (released, _) = store.acquire_scanner_publication_lease(0, ttl).await.expect("A token");
        assert!(store.release_scanner_publication_lease(released).await);
        let (valid, _) = store.acquire_scanner_publication_lease(0, ttl).await.expect("new A token");
        for token in [Uuid::new_v4(), foreign, released] {
            assert!(
                store
                    .rename_local_data(
                        &disk.endpoint().to_string(),
                        ("target-bucket", "staged"),
                        &fi,
                        ("target-bucket", "destination"),
                        Some(token)
                    )
                    .await
                    .is_err()
            );
        }
        tokio::time::pause();
        tokio::time::advance(ttl + std::time::Duration::from_secs(1)).await;
        tokio::time::resume();
        assert!(
            store
                .rename_local_data(
                    &disk.endpoint().to_string(),
                    ("target-bucket", "staged"),
                    &fi,
                    ("target-bucket", "destination"),
                    Some(valid)
                )
                .await
                .is_err(),
            "expired real token"
        );
        let _ = other_store.release_scanner_publication_lease(foreign).await;
        assert_eq!(
            tokio::fs::read(root.path().join("target-bucket/staged/xl.meta"))
                .await
                .expect("source bytes"),
            before
        );
        assert!(!root.path().join("target-bucket/destination").exists());
        assert!(!ctx.namespace_commits_pending());
    }

    #[tokio::test]
    async fn observational_lease_renames_only_the_observed_usage_object() {
        let ctx = Arc::new(InstanceContext::new());
        let root = tempfile::tempdir().expect("target root");
        let disk = target_disk(&ctx, root.path(), Uuid::new_v4()).await;
        let store = super::super::tests::build_store_with_ctx(ctx.clone());
        let disk_ref = disk.endpoint().to_string();
        let volume = crate::disk::RUSTFS_META_BUCKET;
        let observed_path = format!(
            "{}/{}",
            crate::disk::BUCKET_META_PREFIX,
            rustfs_data_usage::DATA_USAGE_OBSERVED_OBJECT_NAME
        );
        let source_path = format!("{}/.staged-observation", crate::disk::BUCKET_META_PREFIX);
        let fi = target_file_info(&observed_path, Uuid::new_v4(), b"non-authoritative");
        seed_target(&disk, volume, &source_path, fi.clone()).await;

        let pending = ctx.begin_namespace_commit();
        let (token, generation) = store
            .acquire_scanner_publication_lease_with_purpose(0, crate::runtime::instance::SCANNER_PUBLICATION_LEASE_TTL, true)
            .await
            .expect("observational lease should permit a pending namespace commit");
        let error = store
            .rename_local_data(&disk_ref, (volume, &source_path), &fi, (volume, "buckets/.usage.v2.json"), Some(token))
            .await
            .expect_err("observational token must not authorize the authoritative usage path");
        assert!(error.to_string().contains("observed usage publication"));
        assert!(root.path().join(volume).join(&source_path).join("xl.meta").exists());

        store
            .rename_local_data(&disk_ref, (volume, &source_path), &fi, (volume, &observed_path), Some(token))
            .await
            .expect("observational token may rename only the observed usage key");
        assert!(
            store
                .validate_scanner_publication_lease_with_purpose(token, generation, true)
                .await
                .is_ok(),
            "namespace commits do not invalidate the movement-only observation lease"
        );
        let wrong_purpose = store
            .validate_scanner_publication_lease_with_purpose(token, generation, false)
            .await
            .expect_err("the same token must not validate as authoritative");
        assert!(wrong_purpose.to_string().contains("purpose"));
        drop(pending);
        assert!(store.release_scanner_publication_lease(token).await);
        let observed = disk
            .read_version(
                volume,
                volume,
                &observed_path,
                &fi.version_id.expect("fixture version").to_string(),
                &crate::disk::ReadOptions {
                    read_data: true,
                    ..Default::default()
                },
            )
            .await
            .expect("observed metadata should be present");
        assert_eq!(observed.data.as_deref(), Some(&b"non-authoritative"[..]));
    }

    #[tokio::test]
    async fn completed_namespace_commit_keeps_scanner_target_admission() {
        use futures::FutureExt;
        use std::time::{Duration, Instant};
        use tokio::time::timeout;

        let ttl = crate::runtime::instance::SCANNER_PUBLICATION_LEASE_TTL;
        let ctx = Arc::new(InstanceContext::new());
        let root = tempfile::tempdir().expect("target root");
        let user_volume = "target-bucket";
        let metadata_volume = ".rustfs.sys/tmp";
        let setup = std::panic::AssertUnwindSafe(timeout(ttl / 2, async {
            let disk = target_disk(&ctx, root.path(), Uuid::new_v4()).await;
            let old_time = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("fixed fixture modtime");
            let new_time = old_time + time::Duration::seconds(1);
            let mut user_old = target_file_info("destination", Uuid::new_v4(), b"user-before-scan");
            let mut user_new = target_file_info("destination", Uuid::new_v4(), b"user-after-scan");
            let mut metadata_old = target_file_info("destination", Uuid::new_v4(), b"metadata-before-stale-admission");
            let mut metadata_new = target_file_info("destination", Uuid::new_v4(), b"metadata-from-stale-scan");
            user_old.mod_time = Some(old_time);
            metadata_old.mod_time = Some(old_time);
            user_new.mod_time = Some(new_time);
            metadata_new.mod_time = Some(new_time);
            seed_target(&disk, user_volume, "destination", user_old).await;
            seed_target(&disk, user_volume, "staged", user_new.clone()).await;
            let metadata_before = seed_target(&disk, metadata_volume, "destination", metadata_old.clone()).await;
            seed_target(&disk, metadata_volume, "staged", metadata_new.clone()).await;
            (disk, user_new, metadata_old, metadata_new, metadata_before)
        }))
        .catch_unwind()
        .await;
        let (disk, user_new, metadata_old, metadata_new, metadata_before) = match setup {
            Ok(Ok(setup)) => setup,
            Ok(Err(error)) => {
                let retained = root.keep();
                panic!("fixture initialization must finish before lease acquisition: {error}; retained={retained:?}");
            }
            Err(panic) => {
                let retained = root.keep();
                eprintln!("fixture setup panicked; retained={retained:?}");
                std::panic::resume_unwind(panic);
            }
        };
        let store = super::super::tests::build_store_with_ctx(ctx.clone());
        let disk_ref = disk.endpoint().to_string();
        let metadata_path = root.path().join(metadata_volume).join("destination/xl.meta");
        let read_options = crate::disk::ReadOptions {
            read_data: true,
            ..Default::default()
        };
        let namespace_before = ctx.namespace_commit_generation();
        let namespace_completed = namespace_before.checked_add(2).expect("one begin and one physical drain");
        let movement_before = ctx.data_movement_generation();
        let operation_epoch_before = ctx.data_movement_operation_epoch();
        let mut acquired_token = None;

        // Use real monotonic time: expiry must not supply a false rejection.
        let started = Instant::now();
        let observations = std::panic::AssertUnwindSafe(timeout(ttl / 2, async {
            assert!(!ctx.namespace_commits_pending(), "all fixture writes precede the baseline");
            let (token, generation) = store
                .acquire_scanner_publication_lease(movement_before, ttl)
                .await
                .expect("precondition: acquire a real current lease");
            acquired_token = Some(token);
            assert_eq!(generation, movement_before);
            store
                .validate_scanner_publication_lease(token, movement_before)
                .await
                .expect("precondition: validate succeeds before the ordinary commit");
            drop(
                store
                    .acquire_scanner_publication_lease_guard(token)
                    .await
                    .expect("precondition: actual target guard lookup accepts the live token"),
            );
            assert_eq!(ctx.namespace_commit_generation(), namespace_before);
            store
                .rename_local_data(&disk_ref, (user_volume, "staged"), &user_new, (user_volume, "destination"), None)
                .await
                .expect("precondition: complete a real ordinary rename after the successful lease checks");
            let ordinary = disk
                .read_version(user_volume, user_volume, "destination", "", &read_options)
                .await
                .expect("precondition: read latest ordinary committed body");
            assert_eq!(ordinary.data, user_new.data);
            assert_eq!(ordinary.version_id, user_new.version_id);
            while ctx.namespace_commits_pending() {
                tokio::task::yield_now().await;
            }
            assert_eq!(
                ctx.namespace_commit_generation(),
                namespace_completed,
                "ordinary physical owner must fully drain"
            );
            assert_eq!(ctx.data_movement_generation(), movement_before);
            assert_eq!(ctx.data_movement_operation_epoch(), operation_epoch_before);

            // Collect both admissions and the physical result before asserting
            // rejection, so the first failure cannot hide the second entrypoint.
            let stale_validate = store.validate_scanner_publication_lease(token, movement_before).await;
            let target_rename = store
                .rename_local_data(
                    &disk_ref,
                    (metadata_volume, "staged"),
                    &metadata_new,
                    (metadata_volume, "destination"),
                    Some(token),
                )
                .await;
            let metadata_latest = disk
                .read_version(metadata_volume, metadata_volume, "destination", "", &read_options)
                .await;
            let metadata_after = tokio::fs::read(&metadata_path).await;
            let user_latest = disk
                .read_version(user_volume, user_volume, "destination", "", &read_options)
                .await;
            let final_state = (
                ctx.namespace_commits_pending(),
                ctx.namespace_commit_generation(),
                ctx.data_movement_generation(),
                ctx.data_movement_operation_epoch(),
            );
            (stale_validate, target_rename, metadata_latest, metadata_after, user_latest, final_state)
        }))
        .catch_unwind()
        .await;
        let observed_after = started.elapsed();
        // A table release does not drain an independently owned metadata call.
        // Collect every release outcome before checking either ownership chain.
        let cleanup = std::panic::AssertUnwindSafe(async {
            let mut releases = Vec::new();
            if let Some(token) = acquired_token {
                releases.push(
                    std::panic::AssertUnwindSafe(timeout(Duration::from_secs(5), store.release_scanner_publication_lease(token)))
                        .catch_unwind()
                        .await,
                );
            }
            let movement_guard = timeout(Duration::from_secs(5), ctx.data_movement_operation_gate().write_owned()).await;
            let namespace_drained = timeout(Duration::from_secs(5), async {
                while ctx.namespace_commits_pending() {
                    tokio::task::yield_now().await;
                }
            })
            .await;
            let drained = releases.iter().all(|release| matches!(release, Ok(Ok(_))))
                && movement_guard.is_ok()
                && namespace_drained.is_ok();
            #[cfg(not(windows))]
            let drained = {
                use crate::disk::os::prepared_publication_test_hooks as hooks;

                let mut keys_drained = true;
                for volume in [user_volume, metadata_volume] {
                    // The physical lease uses the actual IO object directory,
                    // including descriptor-rooted aliases, not its xl.meta file.
                    let key_drained = match disk.get_object_path_for_io_if_local(volume, "destination") {
                        Some(Ok(path)) => timeout(Duration::from_secs(5), hooks::drain_namespace_key(&path))
                            .await
                            .is_ok(),
                        _ => false,
                    };
                    keys_drained &= key_drained;
                }
                drained && keys_drained
            };
            drop(movement_guard);
            if let Some(panic) = releases.into_iter().find_map(|release| release.err()) {
                std::panic::resume_unwind(panic);
            }
            drained
        })
        .catch_unwind()
        .await;
        // Generic cancelled IO cannot be proved drained by a namespace key.
        // Retain on any failed observation, even if best-effort cleanup succeeds.
        if !matches!(&observations, Ok(Ok((_, _, Ok(_), Ok(_), Ok(_), _)))) || !matches!(&cleanup, Ok(true)) {
            let retained = root.keep();
            eprintln!("fixture observations or physical cleanup incomplete; retained={retained:?}");
        }
        let observations = match observations {
            Ok(result) => result.expect("fixture timed out before complete observations; not a namespace rejection result"),
            Err(panic) => std::panic::resume_unwind(panic),
        };
        match cleanup {
            Ok(drained) => assert!(drained, "fixture physical cleanup must finish before deleting its root"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        assert!(
            observed_after < ttl,
            "fixture lease expired before observation: elapsed={observed_after:?}, ttl={ttl:?}"
        );
        assert!(
            observed_after < ttl / 2,
            "fixture exceeded its half-TTL observation budget: {observed_after:?}"
        );
        let (stale_validate, target_rename, metadata_latest, metadata_after, user_latest, final_state) = observations;
        let metadata_latest = metadata_latest.expect("observation: latest metadata must remain decodable");
        let metadata_after = metadata_after.expect("observation: read raw destination xl.meta");
        let user_latest = user_latest.expect("observation: latest ordinary body must remain decodable");
        let metadata_changed = metadata_after != metadata_before;
        let latest_is_staged = metadata_latest.version_id == metadata_new.version_id && metadata_latest.data == metadata_new.data;
        eprintln!(
            "namespace admission observation: stale_validate={stale_validate:?}, target_rename={:?}, metadata_changed={metadata_changed}, latest_is_staged={latest_is_staged}, latest_version={:?}, latest_body={:?}, final_state={final_state:?}, elapsed={observed_after:?}, ttl={ttl:?}",
            target_rename.as_ref().map(|_| ()),
            metadata_latest.version_id,
            metadata_latest.data,
        );
        assert_eq!(final_state, (false, namespace_completed, movement_before, operation_epoch_before));
        assert_eq!(user_latest.data, user_new.data);
        assert_eq!(user_latest.version_id, user_new.version_id);
        // An ordinary commit that completed after the lease was granted is
        // namespace activity the scanner certifies per bucket; it is not a
        // reason to withhold the scanner-owned metadata rename.
        assert!(
            stale_validate.is_ok() && target_rename.is_ok(),
            "a completed ordinary commit must keep both admissions: validate={stale_validate:?}, target={:?}, metadata_changed={metadata_changed}",
            target_rename.as_ref().map(|_| ()),
        );
        assert!(metadata_changed, "the admitted target publishes the staged metadata");
        assert!(latest_is_staged, "latest metadata must be the published one");
        assert_ne!(metadata_latest.data, metadata_old.data);
    }

    #[tokio::test]
    async fn scanner_lease_allows_real_metadata_publication_after_an_ordinary_write() {
        use futures::FutureExt;
        use std::time::Duration;
        use tokio::time::timeout;

        let ctx = Arc::new(InstanceContext::new());
        let store = super::super::tests::build_store_with_ctx(ctx.clone());
        let root = tempfile::tempdir().expect("target root");
        let ttl = crate::runtime::instance::SCANNER_PUBLICATION_LEASE_TTL;
        let user_volume = "target-bucket";
        let metadata_volume = ".rustfs.sys/tmp";
        let setup = std::panic::AssertUnwindSafe(timeout(ttl / 2, async {
            let disk = target_disk(&ctx, root.path(), Uuid::new_v4()).await;
            let user_new = target_file_info("destination", Uuid::new_v4(), b"completed-user-write");
            let mut metadata_old = target_file_info("destination", Uuid::new_v4(), b"old-metadata");
            let mut metadata_new = target_file_info("destination", Uuid::new_v4(), b"metadata-from-renewed-scan");
            metadata_old.mod_time = Some(OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("fixed old time"));
            metadata_new.mod_time = metadata_old.mod_time.map(|old| old + time::Duration::seconds(1));
            seed_target(&disk, user_volume, "staged", user_new.clone()).await;
            let metadata_before = seed_target(&disk, metadata_volume, "destination", metadata_old).await;
            seed_target(&disk, metadata_volume, "staged", metadata_new.clone()).await;
            (disk, user_new, metadata_new, metadata_before)
        }))
        .catch_unwind()
        .await;
        let (disk, user_new, metadata_new, metadata_before) = match setup {
            Ok(Ok(setup)) => setup,
            Ok(Err(error)) => {
                let retained = root.keep();
                panic!("fixture initialization must finish before lease acquisition: {error}; retained={retained:?}");
            }
            Err(panic) => {
                let retained = root.keep();
                eprintln!("fixture setup panicked; retained={retained:?}");
                std::panic::resume_unwind(panic);
            }
        };
        let disk_ref = disk.endpoint().to_string();
        let movement_generation = ctx.data_movement_generation();
        let namespace_generation = store.scanner_namespace_mutation_generation();
        let read_options = crate::disk::ReadOptions {
            read_data: true,
            ..Default::default()
        };
        let mut tokens = Vec::new();
        let result = std::panic::AssertUnwindSafe(timeout(ttl / 2, async {
            let (old_token, _) = store
                .acquire_scanner_publication_lease(movement_generation, ttl)
                .await
                .expect("original lease");
            tokens.push(old_token);
            store
                .rename_local_data(&disk_ref, (user_volume, "staged"), &user_new, (user_volume, "destination"), None)
                .await
                .expect("ordinary namespace write");
            while ctx.namespace_commits_pending() {
                tokio::task::yield_now().await;
            }
            assert_eq!(ctx.namespace_commit_generation(), 2);
            assert_eq!(ctx.data_movement_generation(), movement_generation);
            let user_latest = disk
                .read_version(user_volume, user_volume, "destination", "", &read_options)
                .await
                .expect("latest ordinary committed object");
            assert_eq!(user_latest.version_id, user_new.version_id);
            assert_eq!(user_latest.data, user_new.data);
            store
                .validate_scanner_publication_lease(old_token, movement_generation)
                .await
                .expect("an ordinary write after the grant does not stale the lease");
            assert_eq!(
                ctx.scanner_publication_lease_generations(old_token).await,
                Some((
                    movement_generation,
                    Some(namespace_generation),
                    crate::runtime::instance::ScannerPublicationLeasePurpose::Authoritative,
                )),
                "validation must not refresh or discard the lease"
            );
            let (fresh_token, fresh_generation) = store
                .acquire_scanner_publication_lease(movement_generation, ttl)
                .await
                .expect("a new scan can acquire a current lease after the completed write");
            tokens.push(fresh_token);
            store
                .validate_scanner_publication_lease(fresh_token, fresh_generation)
                .await
                .expect("fresh lease validates");
            store
                .rename_local_data(
                    &disk_ref,
                    (metadata_volume, "staged"),
                    &metadata_new,
                    (metadata_volume, "destination"),
                    Some(fresh_token),
                )
                .await
                .expect("fresh lease authorizes real internal metadata publication");
            let latest = disk
                .read_version(metadata_volume, metadata_volume, "destination", "", &read_options)
                .await
                .expect("latest published metadata");
            let raw = tokio::fs::read(root.path().join(metadata_volume).join("destination/xl.meta"))
                .await
                .expect("raw published metadata");
            assert_eq!(latest.version_id, metadata_new.version_id);
            assert_eq!(latest.data, metadata_new.data);
            assert_ne!(raw, metadata_before);
            assert!(!ctx.namespace_commits_pending());
            assert_eq!(
                ctx.namespace_commit_generation(),
                2,
                "internal metadata does not mutate the user namespace"
            );
            assert_eq!(ctx.data_movement_generation(), movement_generation);
        }))
        .catch_unwind()
        .await;
        // A table release does not drain an independently owned metadata call.
        // Collect every release outcome before checking either ownership chain.
        let cleanup = std::panic::AssertUnwindSafe(async {
            let mut releases = Vec::new();
            for token in tokens {
                releases.push(
                    std::panic::AssertUnwindSafe(timeout(Duration::from_secs(5), store.release_scanner_publication_lease(token)))
                        .catch_unwind()
                        .await,
                );
            }
            let movement_guard = timeout(Duration::from_secs(5), ctx.data_movement_operation_gate().write_owned()).await;
            let namespace_drained = timeout(Duration::from_secs(5), async {
                while ctx.namespace_commits_pending() {
                    tokio::task::yield_now().await;
                }
            })
            .await;
            let drained = releases.iter().all(|release| matches!(release, Ok(Ok(_))))
                && movement_guard.is_ok()
                && namespace_drained.is_ok();
            #[cfg(not(windows))]
            let drained = {
                use crate::disk::os::prepared_publication_test_hooks as hooks;

                let mut keys_drained = true;
                for volume in [user_volume, metadata_volume] {
                    // The physical lease uses the actual IO object directory,
                    // including descriptor-rooted aliases, not its xl.meta file.
                    let key_drained = match disk.get_object_path_for_io_if_local(volume, "destination") {
                        Some(Ok(path)) => timeout(Duration::from_secs(5), hooks::drain_namespace_key(&path))
                            .await
                            .is_ok(),
                        _ => false,
                    };
                    keys_drained &= key_drained;
                }
                drained && keys_drained
            };
            drop(movement_guard);
            if let Some(panic) = releases.into_iter().find_map(|release| release.err()) {
                std::panic::resume_unwind(panic);
            }
            drained
        })
        .catch_unwind()
        .await;
        // Generic cancelled IO cannot be proved drained by a namespace key.
        // Retain on any failed observation, even if best-effort cleanup succeeds.
        if !matches!(&result, Ok(Ok(()))) || !matches!(&cleanup, Ok(true)) {
            let retained = root.keep();
            eprintln!("fixture observations or physical cleanup incomplete; retained={retained:?}");
        }
        match result {
            Ok(result) => result.expect("publication must finish before the original lease can expire"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        match cleanup {
            Ok(drained) => assert!(drained, "fixture physical cleanup must finish before deleting its root"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        assert!(ctx.data_movement_operation_gate().try_write_owned().is_ok());
    }

    #[cfg(not(windows))]
    #[tokio::test]
    #[serial_test::serial]
    async fn target_ordinary_timeout_keeps_its_physical_namespace_owner() {
        use crate::disk::os::prepared_publication_test_hooks as hooks;
        // Allow real fixture I/O to finish under load. Expire the operation
        // with virtual time only after its physical publication is paused.
        temp_env::async_with_vars([(rustfs_config::ENV_DRIVE_MAX_TIMEOUT_DURATION, Some("60"))], async {
            let ctx = Arc::new(InstanceContext::new());
            let store = super::super::tests::build_store_with_ctx(ctx.clone());
            let root = tempfile::tempdir().expect("root");
            let disk = target_disk(&ctx, root.path(), Uuid::new_v4()).await;
            let fi = target_file_info("destination", Uuid::new_v4(), b"timed-out-physical-commit");
            seed_target(&disk, "target-bucket", "staged", fi.clone()).await;
            let path = disk
                .get_object_path_for_io_if_local("target-bucket", "destination/xl.meta")
                .expect("local")
                .expect("destination IO path");
            let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
            let _hook = hooks::install(&path, move || {
                let _ = entered_tx.send(());
                let _ = release_rx.recv();
            });
            let disk_ref = disk.endpoint().to_string();
            let mut rename = Box::pin(store.rename_local_data(
                &disk_ref,
                ("target-bucket", "staged"),
                &fi,
                ("target-bucket", "destination"),
                None,
            ));
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                tokio::select! {
                    result = &mut rename => panic!("completed before physical pause: {result:?}"),
                    entered = entered_rx => entered.expect("physical entry"),
                }
            })
            .await
            .expect("bounded entry");
            tokio::time::pause();
            tokio::time::advance(std::time::Duration::from_secs(61)).await;
            tokio::time::resume();
            let result = tokio::time::timeout(std::time::Duration::from_secs(5), &mut rename)
                .await
                .expect("ordinary deadline remains enabled");
            assert!(matches!(result, Err(DiskError::Timeout)), "{result:?}");
            drop(rename);
            assert!(ctx.namespace_commits_pending(), "timeout is not a physical drain");
            drop(release_tx);
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                while ctx.namespace_commits_pending() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("late physical owner drains");
            let read = disk
                .read_version(
                    "target-bucket",
                    "target-bucket",
                    "destination",
                    &fi.version_id.expect("version").to_string(),
                    &crate::disk::ReadOptions {
                        read_data: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("read actual timeout tail");
            assert_eq!(read.data, fi.data);
        })
        .await;
    }

    #[test]
    fn endpoint_rpc_authority_preserves_port_and_ipv6_brackets() {
        let endpoint = Endpoint::try_from("https://127.0.0.1:9001/d1").expect("URL endpoint");
        assert_eq!(endpoint_rpc_authority(&endpoint).as_deref(), Some("127.0.0.1:9001"));

        let endpoint = Endpoint::try_from("https://[::1]:9002/d1").expect("IPv6 URL endpoint");
        assert_eq!(endpoint_rpc_authority(&endpoint).as_deref(), Some("[::1]:9002"));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn init_local_peer_publishes_complete_rpc_authority() {
        let previous = rustfs_common::get_global_local_node_name().await;
        let mut endpoint = Endpoint::try_from("https://127.0.0.1:9001/d1").expect("URL endpoint");
        endpoint.is_local = true;
        let endpoint_pools = EndpointServerPools(vec![PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 1,
            endpoints: Endpoints::from(vec![endpoint]),
            cmd_line: "rpc-authority-test".to_string(),
            platform: "test".to_string(),
        }]);

        let host = String::new();
        let port = "9000".to_string();
        init_local_peer(&endpoint_pools, &host, &port).await;
        assert_eq!(rustfs_common::try_get_global_local_node_name().as_deref(), Some("127.0.0.1:9001"));
        rustfs_common::set_global_local_node_name(&previous).await;
    }

    // Phase 5 follow-up (backlog#1052): registering local disks through the
    // ctx-explicit entry writes the passed context's registry only — the
    // process bootstrap context (and any other instance) stays clean, so a
    // future second server's disks cannot leak into the first one's registry.
    #[tokio::test]
    async fn init_local_disks_with_instance_ctx_isolates_disk_registry() {
        let temp_dir = tempfile::tempdir().expect("create temp disk dir");
        let endpoint_pools = single_local_disk_pools(temp_dir.path());
        let instance_ctx = Arc::new(InstanceContext::new());

        init_local_disks_with_instance_ctx(&instance_ctx, endpoint_pools)
            .await
            .expect("local disks should register into the passed context");

        let registered: Vec<String> = instance_ctx.local_disk_map().read().await.keys().cloned().collect();
        assert_eq!(registered.len(), 1, "the passed context must hold exactly the one local disk");
        assert_eq!(
            instance_ctx.local_disk_set_drives().read().await.len(),
            1,
            "the passed context must hold the pool/set/drive layout"
        );

        let bootstrap = crate::runtime::instance::bootstrap_ctx();
        let bootstrap_map = bootstrap.local_disk_map();
        let bootstrap_map = bootstrap_map.read().await;
        let sibling = InstanceContext::new();
        for key in &registered {
            assert!(
                !bootstrap_map.contains_key(key),
                "bootstrap context must not absorb a disk registered into an explicit context"
            );
            assert!(
                !sibling.local_disk_map().read().await.contains_key(key),
                "a sibling context must not observe another instance's disks"
            );
        }
    }

    #[tokio::test]
    async fn stale_local_disk_snapshot_cannot_repopulate_the_id_registry() {
        let temp_dir = tempfile::tempdir().expect("create temp disk dir");
        let endpoint_pools = single_local_disk_pools(temp_dir.path());
        let instance_ctx = Arc::new(InstanceContext::new());
        init_local_disks_with_instance_ctx(&instance_ctx, endpoint_pools)
            .await
            .expect("local disk should be registered");
        let disk = instance_ctx
            .local_disk_map()
            .read()
            .await
            .values()
            .find_map(|disk| disk.clone())
            .expect("registered local disk");
        let endpoint = disk.endpoint().to_string();
        let disk_id = Uuid::new_v4();

        let local_disk_map = instance_ctx.local_disk_map();
        let mut quarantine = local_disk_map.write().await;
        let replacement = new_disk(
            &disk.endpoint(),
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("replacement disk should initialize");
        assert!(!Arc::ptr_eq(&disk, &replacement));
        let task_ctx = instance_ctx.clone();
        let task_disk = disk.clone();
        let remember = tokio::spawn(async move { record_local_disk_id_if_active(&task_ctx, &task_disk, disk_id).await });
        tokio::task::yield_now().await;
        quarantine.insert(endpoint.clone(), Some(replacement.clone()));
        drop(quarantine);

        assert!(!remember.await.expect("stale lookup task should complete"));
        assert!(!instance_ctx.local_disk_id_map().read().await.contains_key(&disk_id));
        let active = instance_ctx
            .local_disk_map()
            .read()
            .await
            .get(&endpoint)
            .cloned()
            .flatten()
            .expect("replacement disk should remain registered");
        assert!(Arc::ptr_eq(&active, &replacement));
        assert!(record_local_disk_id_if_active(&instance_ctx, &replacement, disk_id).await);
        assert_eq!(instance_ctx.local_disk_id_map().read().await.get(&disk_id), Some(&endpoint));
    }
}

async fn begin_legacy_local_rename_file_with_ctx(
    ctx: &Arc<InstanceContext>,
    disk_ref: &str,
    source: (&str, &str),
    destination: (&str, &str),
    durable: bool,
) -> DiskResult<LegacyRenameFile> {
    let (disk, disk_id) = local_disk_candidate(ctx, disk_ref).await?;
    let owner =
        admit_local_disk(ctx, &disk, disk_id, !is_meta_bucketname(source.0) || !is_meta_bucketname(destination.0)).await?;
    disk.begin_legacy_rename_file(source, destination, durable, owner.map(|owner| owner as Arc<dyn Send + Sync>))
        .await
}

async fn begin_legacy_local_part_mutation_with_ctx(
    ctx: &Arc<InstanceContext>,
    disk_ref: &str,
    source: Option<(&str, &str)>,
    destination: (&str, &str),
    operation: LegacyPartOperation,
) -> DiskResult<LegacyPartMutation> {
    let started = std::time::Instant::now();
    let (disk, disk_id) = local_disk_candidate(ctx, disk_ref).await?;
    let result = async {
        let ordinary = source.is_some_and(|s| !is_meta_bucketname(s.0)) || !is_meta_bucketname(destination.0);
        let owner = admit_local_disk(ctx, &disk, disk_id, ordinary).await?;
        disk.begin_legacy_part_mutation(source, destination, owner.map(|owner| owner as Arc<dyn Send + Sync>), operation)
            .await
    }
    .await;
    if let Err(error) = &result {
        disk.record_legacy_admission_failure(operation.name(), error, started);
    }
    result
}

async fn begin_legacy_local_path_delete_with_ctx(
    ctx: &Arc<InstanceContext>,
    disk_ref: &str,
    volume: &str,
    operation: LegacyPathDeleteOperation,
    external_guard: Option<Arc<dyn Send + Sync>>,
) -> DiskResult<LegacyPathDelete> {
    let started = std::time::Instant::now();
    let deadline = tokio::time::Instant::now() + crate::disk::disk_store::get_max_timeout_duration();
    let (disk, disk_id) = tokio::time::timeout_at(deadline, local_disk_candidate(ctx, disk_ref))
        .await
        .map_err(|_| DiskError::Timeout)?
        .map_err(|_| DiskError::other("cannot find disk"))?;
    let result = tokio::time::timeout_at(deadline, async {
        let instance = admit_local_disk(ctx, &disk, disk_id, !is_meta_bucketname(volume))
            .await
            .map_err(|_| DiskError::other("cannot find disk"))?;
        let owner: Arc<dyn Send + Sync> = Arc::new((instance, external_guard));
        disk.begin_legacy_path_delete(volume, operation, Some(owner), deadline).await
    })
    .await
    .map_err(|_| DiskError::Timeout)
    .and_then(|result| result);
    if let Err(error) = &result {
        disk.record_legacy_admission_failure(operation.name(), error, started);
    }
    result
}

async fn begin_legacy_local_residual_version_delete_with_ctx(
    ctx: &Arc<InstanceContext>,
    disk_ref: &str,
    volume: &str,
    path: &str,
    undo_write: bool,
) -> DiskResult<LegacyMetadataWrite> {
    let started = std::time::Instant::now();
    let deadline = tokio::time::Instant::now() + crate::disk::disk_store::get_max_timeout_duration();
    let compatible_lookup_error = |error| {
        if undo_write {
            error
        } else {
            DiskError::other("cannot find disk")
        }
    };
    let (disk, disk_id) = tokio::time::timeout_at(deadline, local_disk_candidate(ctx, disk_ref))
        .await
        .map_err(|_| DiskError::Timeout)?
        .map_err(compatible_lookup_error)?;
    let result = tokio::time::timeout_at(deadline, async {
        let owner = admit_local_disk(ctx, &disk, disk_id, !is_meta_bucketname(volume))
            .await
            .map_err(compatible_lookup_error)?;
        disk.begin_legacy_metadata_write_with_namespace_owner(volume, path, owner.map(|owner| owner as Arc<dyn Send + Sync>))
            .await
            .map(|token| token.with_deadline(deadline))
    })
    .await
    .map_err(|_| DiskError::Timeout)
    .and_then(|result| result);
    if let Err(error) = &result {
        disk.record_legacy_admission_failure("delete_version", error, started);
    }
    result
}
