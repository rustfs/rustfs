// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::disk::{ArtifactInventoryFrame, ArtifactInventoryVersion, Disk};
use rustfs_protos::proto_gen::node_service::ArtifactInventoryPhase;
use tokio::{sync::mpsc, time::Instant};

/// Physical observations retain their member. Replicas are not extra business
/// versions and this stream makes no quorum-readability or GC claim.
#[derive(Debug)]
pub enum ArtifactAttemptInventoryFrame {
    Observations {
        endpoint: String,
        disk_id: Uuid,
        versions: Vec<ArtifactInventoryVersion>,
    },
    Complete {
        members: usize,
        observations: u64,
    },
}

impl ECStore {
    /// Inventory one fully sealed attempt across every configured receiver.
    /// Earlier output is staging only; only the final frame plus Ok is complete.
    pub async fn inventory_artifact_attempt(
        self: Arc<Self>,
        bucket: &str,
        incarnation: Uuid,
        prefix: &str,
        deadline: Instant,
        output: mpsc::Sender<ArtifactAttemptInventoryFrame>,
    ) -> Result<()> {
        tokio::time::timeout_at(deadline, Box::pin(async move {
            require_put_seal_topology(&self).await?;
            if self.ctx.lock_manager().is_disabled() { return Err(Error::other("artifact inventory requires namespace locking")); }
            let lifecycle = self.acquire_bucket_lifecycle_read_lock(bucket).await?;
            let put = self.inventory_artifact_record(bucket, incarnation, prefix).await?;
            let repair = ArtifactRepairSealRecord::new(&put);
            // Lifecycle -> attempt -> repair record. Receivers do not reenter
            // distributed locks; actual two-domain disk owners guard their reads.
            let store = self.clone();
            let bucket = bucket.to_owned();
            let prefix = prefix.to_owned();
            with_config_object_write_lock(self, put.path(), move |attempt| async move {
                tokio::time::timeout_at(deadline, async {
                    if !put.exists(store.clone(), &attempt).await? { return Err(Error::other("artifact inventory requires PUT seal record")); }
                    let lock = store.new_ns_lock(RUSTFS_META_BUCKET, &repair.path()).await?;
                    let repair_owner = WriteCommitGuard::acquire(&lock, get_lock_acquire_timeout()).await?;
                    if !repair.exists(store.clone(), &repair_owner).await? { return Err(Error::other("artifact inventory requires repair seal record")); }
                    let receivers = store.capture_artifact_receivers().await?;
                    let mut observations = 0u64;
                    for (endpoint, (disk, expected)) in &receivers {
                        if lifecycle.is_lock_lost() || attempt.is_lock_lost() || repair_owner.is_lock_lost() {
                            return Err(Error::other("artifact inventory fence was lost"));
                        }
                        let (sender, mut frames) = mpsc::channel(2);
                        let scan = async {
                            match disk.as_ref() {
                                Disk::Local(local) => local.get_disk().inventory_artifact_attempt(*expected, &bucket, &prefix, put.identity(), deadline, sender).await,
                                Disk::Remote(remote) => remote.inventory_artifact_attempt(*expected, (&bucket, incarnation), &prefix, ArtifactInventoryPhase::ArtifactInventoryScan, deadline, sender).await,
                            }
                        };
                        tokio::pin!(scan);
                        let mut sequence = 0;
                        let mut count = 0u64;
                        let mut terminal = false;
                        let mut ended = false;
                        let mut done = false;
                        while !ended || !done {
                            tokio::select! {
                                result = &mut scan, if !done => { result.map_err(Error::other)?; done = true; },
                                frame = frames.recv(), if !ended => match frame {
                                    Some(ArtifactInventoryFrame::Entries { sequence: actual, versions }) if !terminal && actual == sequence => {
                                        count = count.checked_add(u64::try_from(versions.len()).map_err(Error::other)?).ok_or_else(|| Error::other("artifact inventory count overflow"))?;
                                        output.send(ArtifactAttemptInventoryFrame::Observations { endpoint: endpoint.clone(), disk_id: *expected, versions }).await
                                            .map_err(|_| Error::other("artifact inventory consumer closed"))?;
                                        sequence += 1;
                                    }
                                    Some(ArtifactInventoryFrame::Complete { disk_id, incarnation: actual, attempt_prefix, entries })
                                        if !terminal && disk_id == *expected && actual == incarnation && attempt_prefix == prefix && entries == count => terminal = true,
                                    Some(_) => return Err(Error::other("artifact inventory member stream is invalid")),
                                    None => ended = true,
                                },
                            }
                        }
                        if !terminal { return Err(Error::other("artifact inventory member is incomplete")); }
                        observations = observations.checked_add(count).ok_or_else(|| Error::other("artifact inventory count overflow"))?;
                    }
                    store.verify_inventory_members(&put, &receivers, deadline).await?;
                    let current = store.artifact_inventory_receiver_identity(&bucket, incarnation, &prefix).await?;
                    if current.receiver_key != put.identity().receiver_key || lifecycle.is_lock_lost() || attempt.is_lock_lost() || repair_owner.is_lock_lost() {
                        return Err(Error::other("artifact inventory authority changed"));
                    }
                    output.send(ArtifactAttemptInventoryFrame::Complete { members: receivers.len(), observations }).await
                        .map_err(|_| Error::other("artifact inventory consumer closed"))?;
                    Ok(())
                }).await.map_err(|_| Error::other("artifact inventory deadline exceeded"))?
            }).await?
        })).await.map_err(|_| Error::other("artifact inventory deadline exceeded"))?
    }

    pub(super) async fn inventory_artifact_record(&self, bucket: &str, expected: Uuid, prefix: &str) -> Result<PutSealRecord> {
        use crate::bucket::metadata::BUCKET_INCARNATION_FILE;
        use crate::config::com::read_config_observation;
        if expected.is_nil()
            || crate::bucket::utils::has_bad_path_component(bucket)
            || bucket.is_empty()
            || crate::bucket::utils::is_meta_bucketname(bucket)
        {
            return Err(Error::other("artifact inventory authority is invalid"));
        }
        let store = metadata_sys::object_store_in(&self.ctx).await?;
        if store.id != self.id {
            return Err(Error::other("artifact inventory authority belongs to another store"));
        }
        // Do not enter metadata_sys's lazy bucket READ/migration path: the
        // coordinator already owns lifecycle READ and a writer may be queued.
        // Observation reads bypass cache and suppress physical read repair.
        let path = BucketMetadata::new(bucket).save_file_path();
        let (bytes, _) = read_config_observation(store.clone(), &path, 64 * 1024 * 1024).await?;
        let mut metadata = BucketMetadata::decode_file(&bytes)?;
        let (sidecar, _) =
            read_config_observation(store, &format!("{BUCKET_META_PREFIX}/{bucket}/{BUCKET_INCARNATION_FILE}"), 16).await?;
        if metadata.name != bucket
            || !metadata.bucket_creation_committed
            || metadata.bucket_incarnation_id != expected
            || Uuid::from_slice(&sidecar).ok() != Some(expected)
        {
            return Err(Error::other("artifact inventory incarnation mismatch"));
        }
        metadata.parse_all_configs()?;
        if !metadata.versioning_config.as_ref().is_some_and(|config| {
            config.enabled()
                && !config.exclude_folders.unwrap_or(false)
                && config.excluded_prefixes.as_ref().is_none_or(Vec::is_empty)
        }) {
            return Err(Error::other("artifact inventory requires managed versioning"));
        }
        PutSealRecord::new(&metadata, prefix)
    }

    pub(crate) async fn artifact_inventory_receiver_identity(
        &self,
        bucket: &str,
        expected: Uuid,
        prefix: &str,
    ) -> Result<ArtifactPutIdentity> {
        Ok(self.inventory_artifact_record(bucket, expected, prefix).await?.identity())
    }

    async fn verify_inventory_members(
        &self,
        record: &PutSealRecord,
        captured: &ArtifactReceiverSnapshot,
        deadline: Instant,
    ) -> Result<()> {
        for final_pass in [false, true] {
            let current = self.artifact_receiver_inventory().await?;
            if current.len() != captured.len()
                || current
                    .iter()
                    .any(|(endpoint, disk)| !captured.get(endpoint).is_some_and(|(old, _)| Arc::ptr_eq(old, disk)))
            {
                return Err(Error::other("artifact inventory membership changed"));
            }
            if final_pass {
                for (disk, expected) in captured.values() {
                    if disk.cached_disk_id().await != Some(*expected) {
                        return Err(Error::other("artifact inventory expected identity changed"));
                    }
                }
                break;
            }
            for (disk, expected) in captured.values() {
                if disk.cached_disk_id().await != Some(*expected) {
                    return Err(Error::other("artifact inventory expected identity changed"));
                }
                match disk.as_ref() {
                    Disk::Local(local) => {
                        let _verification = local
                            .get_disk()
                            .verify_artifact_inventory_closed(*expected, record.identity(), deadline)
                            .await
                            .map_err(Error::other)?;
                    }
                    Disk::Remote(remote) => {
                        let (output, _unused) = mpsc::channel(1);
                        remote
                            .inventory_artifact_attempt(
                                *expected,
                                (&record.bucket, record.incarnation_id),
                                &record.attempt_prefix,
                                ArtifactInventoryPhase::ArtifactInventoryVerify,
                                deadline,
                                output,
                            )
                            .await
                            .map_err(Error::other)?;
                    }
                }
            }
        }
        Ok(())
    }
}
