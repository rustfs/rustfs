//! Private native object preparation; a data quorum is not publication authority.
use super::ECStore;
use crate::bucket::utils::check_put_object_args;
use crate::disk::DiskAPI;
use crate::{
    disk::local::native_prepare::{
        NativePreparationDescription, NativePreparationProbe, NativePreparationReceipt, NativePrepareInput,
    },
    error::{Error, Result},
    object_api::PutObjReader,
};
use bytes::Bytes;
use uuid::Uuid;

/// Identity supplied to a node-local native preparation operation.
///
/// `store_id` and `bucket_incarnation` are expectations to validate, not
/// authorization tokens. The receiving ECStore establishes its own fence.
#[derive(Clone, Debug)]
pub struct NativePreparationTarget {
    pub store_id: Uuid,
    pub disk_id: Uuid,
    pub bucket: String,
    pub key: String,
    pub preparation: Uuid,
    pub bucket_incarnation: Uuid,
}

/// Metadata and content identity for finalizing a shard already streamed to a disk.
pub struct NativePreparationFinalizeRequest {
    pub target: NativePreparationTarget,
    pub version: Uuid,
    pub object_digest: [u8; 32],
    pub metadata: Bytes,
}

/// Verified local recovery data returned by a target owner.
pub struct NativePreparationRecovery {
    pub receipt: NativePreparationReceipt,
    pub metadata: Bytes,
}

pub struct NativeQuorumPreparation {
    receipts: Vec<NativePreparationReceipt>,
}
impl NativeQuorumPreparation {
    pub(crate) fn into_receipts(self) -> Vec<NativePreparationReceipt> {
        self.receipts
    }
    pub(crate) fn receipts(&self) -> &[NativePreparationReceipt] {
        &self.receipts
    }

    pub fn preparation_id(&self) -> Uuid {
        self.receipts[0].preparation_id()
    }
    pub fn version_id(&self) -> Uuid {
        self.receipts[0].version_id()
    }
    pub fn object_digest(&self) -> [u8; 32] {
        *self.receipts[0].object_digest()
    }
    pub fn object_length(&self) -> u64 {
        self.receipts[0].object_length()
    }
    pub fn receipt_count(&self) -> usize {
        self.receipts().len()
    }
    pub fn disk_ids(&self) -> impl Iterator<Item = Uuid> + '_ {
        self.receipts.iter().map(NativePreparationReceipt::disk_id)
    }
}
#[derive(Clone, PartialEq, Eq)]
struct Identity {
    version: Uuid,
    digest: [u8; 32],
    length: u64,
    set: [u8; 32],
    distribution: [u8; 32],
}
struct Observation {
    set: std::sync::Arc<crate::set_disk::SetDisks>,
    identity: Option<Identity>,
    metadata: Vec<Option<bytes::Bytes>>,
    prepared: Vec<bool>,
    unknown: bool,
}
impl NativeQuorumPreparation {
    pub(crate) fn from_receipts(quorum: usize, receipts: Vec<NativePreparationReceipt>) -> Result<Self> {
        let first = receipts.first().ok_or_else(|| Error::other("empty native quorum"))?;
        let distinct: std::collections::HashSet<_> = receipts.iter().map(NativePreparationReceipt::disk_id).collect();
        if receipts.len() < quorum
            || distinct.len() != receipts.len()
            || receipts.iter().any(|r| !crate::set_disk::same_identity(first, r))
        {
            return Err(Error::other("inconsistent native quorum"));
        }
        Ok(Self { receipts })
    }
}
impl ECStore {
    /// Locate a durable preparation on the exact local disk named by the caller.
    /// The description is local locator data and is not a preparation receipt.
    #[cfg(unix)]
    pub async fn locate_native_preparation_on_disk(
        &self,
        disk_id: Uuid,
        store_id: Uuid,
        preparation: Uuid,
    ) -> Result<Option<NativePreparationDescription>> {
        if store_id != self.id || store_id.is_nil() || disk_id.is_nil() || preparation.is_nil() {
            return Err(Error::other("native preparation locator identity mismatch"));
        }
        let disk = self.resolve_native_local_disk(disk_id).await?;
        disk.locate_native_preparation(store_id, preparation)
            .await
            .map_err(Into::into)
    }

    #[cfg(not(unix))]
    pub async fn locate_native_preparation_on_disk(
        &self,
        _disk_id: Uuid,
        _store_id: Uuid,
        _preparation: Uuid,
    ) -> Result<Option<NativePreparationDescription>> {
        Err(Error::UnsupportedDisk)
    }

    async fn validate_native_target(
        &self,
        target: &NativePreparationTarget,
    ) -> Result<std::sync::Arc<crate::object_api::ObjectLockConfigSnapshot>> {
        if target.store_id != self.id
            || self.id.is_nil()
            || target.disk_id.is_nil()
            || target.preparation.is_nil()
            || target.bucket_incarnation.is_nil()
        {
            return Err(Error::other("native preparation target identity mismatch"));
        }
        check_put_object_args(&target.bucket, &target.key)?;

        let fence = self.object_lock_config_snapshot(&target.bucket).await?;
        let incarnation = self.bucket_incarnation_id_from_disk(&target.bucket).await?;
        if incarnation != target.bucket_incarnation || !fence.is_valid_for_destructive_put(self.id, &target.bucket, incarnation) {
            return Err(Error::other("native preparation target bucket fence mismatch"));
        }
        Ok(fence)
    }

    async fn resolve_native_local_disk(&self, disk_id: Uuid) -> Result<std::sync::Arc<crate::disk::local::LocalDisk>> {
        use crate::disk::{Disk, DiskAPI};

        let mut matches = Vec::new();
        let mut lookup_error = None;
        for pool in &self.pools {
            for set in &pool.disk_set {
                let disks = set.disks.read().await.clone();
                for disk in disks.into_iter().flatten() {
                    let Disk::Local(local) = disk.as_ref() else {
                        continue;
                    };
                    let local_disk = local.get_disk();
                    match local_disk.get_disk_id().await {
                        Ok(Some(id)) if id == disk_id => matches.push(local_disk),
                        Ok(_) => {}
                        Err(error) => {
                            lookup_error.get_or_insert(error);
                        }
                    }
                }
            }
        }
        if matches.len() != 1 {
            if matches.is_empty()
                && let Some(error) = lookup_error
            {
                return Err(error.into());
            }
            return Err(Error::other("native preparation target disk is not uniquely local"));
        }
        matches
            .pop()
            .ok_or_else(|| Error::other("native preparation target disk disappeared"))
    }

    /// Create private stream staging on the exact local disk named by the caller.
    /// The caller holds the distributed object lock across all disk RPCs; this owner must not
    /// reacquire that non-reentrant lock. Store and bucket identities are checked against fresh
    /// local state, while disk operations take their own physical mutation locks.
    pub async fn begin_native_preparation_on_disk(&self, target: &NativePreparationTarget) -> Result<()> {
        let fence = self.validate_native_target(target).await?;
        let disk = self.resolve_native_local_disk(target.disk_id).await?;
        disk.begin_native_stream(target.preparation, self.id, &target.bucket, target.bucket_incarnation, fence)
            .await?;
        Ok(())
    }

    /// Verify and durably attest the stream already written to the named local disk.
    /// This does not select placement or encode the object.
    pub async fn finalize_native_preparation_on_disk(
        &self,
        request: NativePreparationFinalizeRequest,
    ) -> Result<NativePreparationReceipt> {
        let target = &request.target;
        let fence = self.validate_native_target(target).await?;
        let disk = self.resolve_native_local_disk(target.disk_id).await?;
        disk.prepare_native(NativePrepareInput {
            preparation: target.preparation,
            store_id: self.id,
            bucket: target.bucket.clone(),
            bucket_incarnation: target.bucket_incarnation,
            object_digest: request.object_digest,
            key: target.key.clone(),
            version: request.version,
            metadata: request.metadata,
            bucket_fence: fence,
        })
        .await
        .map_err(Into::into)
    }

    /// Inspect the named local disk for a preparation without trusting a remote fence.
    pub async fn probe_native_preparation_on_disk(&self, target: &NativePreparationTarget) -> Result<NativePreparationProbe> {
        let fence = self.validate_native_target(target).await?;
        let disk = self.resolve_native_local_disk(target.disk_id).await?;
        disk.probe_native(target.preparation, self.id, &target.bucket, &target.key, target.bucket_incarnation, fence)
            .await
            .map_err(Into::into)
    }

    /// Recover and revalidate durable preparation state from the named local disk.
    pub async fn recover_native_preparation_on_disk(
        &self,
        target: &NativePreparationTarget,
    ) -> Result<NativePreparationRecovery> {
        let fence = self.validate_native_target(target).await?;
        let disk = self.resolve_native_local_disk(target.disk_id).await?;
        let (receipt, metadata) = disk
            .recover_native_with_metadata(target.preparation, self.id, &target.bucket, target.bucket_incarnation, fence)
            .await?;
        Ok(NativePreparationRecovery { receipt, metadata })
    }

    async fn native_observations(
        &self,
        bucket: &str,
        key: &str,
        preparation: Uuid,
        incarnation: Uuid,
        fence: std::sync::Arc<crate::object_api::ObjectLockConfigSnapshot>,
    ) -> Result<Vec<Observation>> {
        self.native_observations_scan(bucket, key, preparation, incarnation, fence, false)
            .await
    }
    async fn native_observations_scan(
        &self,
        bucket: &str,
        key: &str,
        preparation: Uuid,
        incarnation: Uuid,
        fence: std::sync::Arc<crate::object_api::ObjectLockConfigSnapshot>,
        all_sets: bool,
    ) -> Result<Vec<Observation>> {
        let mut pools = Vec::new();
        for pool in &self.pools {
            let sets = if all_sets {
                pool.disk_set.clone()
            } else {
                vec![pool.get_disks_by_key(key)]
            };
            for set in sets {
                let candidates = set
                    .probe_native_candidates(self.id, bucket, key, preparation, incarnation, fence.clone(), all_sets)
                    .await;
                let mut observed = Observation {
                    set,
                    identity: None,
                    metadata: Vec::new(),
                    prepared: Vec::new(),
                    unknown: false,
                };
                for candidate in candidates {
                    let mut bytes = None;
                    let mut prepared = false;
                    match candidate {
                        Ok(crate::disk::local::native_prepare::NativePreparationProbe::Absent) => {}
                        Ok(crate::disk::local::native_prepare::NativePreparationProbe::Existing {
                            version,
                            prepared: ready,
                            object_digest,
                            object_length,
                            set_digest,
                            distribution_digest,
                            metadata,
                        }) => {
                            let identity = Identity {
                                version,
                                digest: object_digest,
                                length: object_length,
                                set: set_digest,
                                distribution: distribution_digest,
                            };
                            if observed.identity.as_ref().is_some_and(|old| old != &identity) {
                                return Err(Error::other("conflicting native preparation identity"));
                            }
                            observed.identity = Some(identity);
                            bytes = metadata;
                            prepared = ready;
                        }
                        Err(crate::disk::error::Error::Io(error)) if error.kind() == std::io::ErrorKind::InvalidData => {
                            return Err(Error::from(error));
                        }
                        Err(_) => observed.unknown = true,
                    }
                    observed.metadata.push(bytes);
                    observed.prepared.push(prepared);
                }
                pools.push(observed);
            }
        }
        Ok(pools)
    }
    fn existing_native_pool(observations: &[Observation]) -> Result<Option<usize>> {
        let existing: Vec<_> = observations
            .iter()
            .enumerate()
            .filter(|(_, p)| p.identity.is_some())
            .map(|(i, _)| i)
            .collect();
        if existing.len() > 1 {
            return Err(Error::other("native preparation exists in multiple pools"));
        }
        let selected = existing.first().copied();
        if observations.len() > 1 && observations.iter().enumerate().any(|(i, p)| p.unknown && Some(i) != selected) {
            return Err(Error::other("native preparation pool attribution is unknown"));
        }
        Ok(selected)
    }
    pub async fn prepare_native_object(
        &self,
        bucket: &str,
        key: &str,
        preparation: Uuid,
        data: &mut PutObjReader,
    ) -> Result<NativeQuorumPreparation> {
        use sha2::{Digest, Sha256};
        use tokio::io::AsyncReadExt;
        check_put_object_args(bucket, key)?;
        if preparation.is_nil() || self.pools.is_empty() || data.size() < -1 {
            return Err(Error::other("unsupported native preparation"));
        }
        let mut remote = false;
        for pool in &self.pools {
            if pool
                .get_disks_by_key(key)
                .disks
                .read()
                .await
                .iter()
                .flatten()
                .any(|disk| !disk.is_local())
            {
                remote = true;
                break;
            }
        }
        if remote || data.size() < 0 || data.size() > 1024 * 1024 {
            let fence = self.object_lock_config_snapshot(bucket).await?;
            let incarnation = self.bucket_incarnation_id(bucket).await?;
            let owner = std::sync::Arc::new(
                self.handle_new_ns_lock(bucket, key)
                    .await?
                    .get_write_lock(crate::set_disk::get_lock_acquire_timeout())
                    .await?,
            );
            let observations = self
                .native_observations_scan(bucket, key, preparation, incarnation, fence.clone(), true)
                .await?;
            if let Some(selected) = Self::existing_native_pool(&observations)? {
                let identity = observations[selected]
                    .identity
                    .as_ref()
                    .ok_or_else(|| Error::other("missing native identity"))?;
                let mut hash = Sha256::new();
                let mut length = 0_u64;
                let mut buffer = vec![0_u8; 64 * 1024];
                loop {
                    let size = data.stream.read(&mut buffer).await?;
                    if size == 0 {
                        break;
                    }
                    length = length
                        .checked_add(u64::try_from(size).map_err(Error::other)?)
                        .ok_or_else(|| Error::other("native retry length overflow"))?;
                    hash.update(&buffer[..size]);
                }
                if hash.finalize().as_slice() != identity.digest || length != identity.length {
                    return Err(Error::other("native streaming retry content conflict"));
                }
                drop(owner);
                return self.recover_native_object(bucket, key, preparation).await;
            }
            let selected = if self.pools.len() == 1 {
                0
            } else {
                self.get_pool_idx_no_lock(bucket, key, data.size().max(0)).await?
            };
            let set = self.pools[selected].get_disks_by_key(key);
            let (quorum, receipts) =
                Box::pin(set.prepare_native_stream(self.id, bucket, key, preparation, incarnation, data, fence, owner)).await?;
            return NativeQuorumPreparation::from_receipts(quorum, receipts);
        }
        let length = data.size();
        let mut body = Vec::new();
        (&mut data.stream).take(1024 * 1024 + 1).read_to_end(&mut body).await?;
        if body.len() as i64 != length {
            return Err(Error::other("native reader length mismatch"));
        }
        let digest: [u8; 32] = Sha256::digest(&body).into();
        let fence = self.object_lock_config_snapshot(bucket).await?;
        let incarnation = self.bucket_incarnation_id(bucket).await?;
        let owner = std::sync::Arc::new(
            self.handle_new_ns_lock(bucket, key)
                .await?
                .get_write_lock(crate::set_disk::get_lock_acquire_timeout())
                .await?,
        );
        let mut observations = self
            .native_observations(bucket, key, preparation, incarnation, fence.clone())
            .await?;
        let selected = match Self::existing_native_pool(&observations)? {
            Some(index) => index,
            None if observations.len() == 1 => 0,
            None => self.get_pool_idx_no_lock(bucket, key, length).await?,
        };
        let observed = observations.remove(selected);
        if observed
            .identity
            .as_ref()
            .is_some_and(|identity| identity.digest != digest || identity.length != body.len() as u64)
        {
            return Err(Error::other("native preparation content conflict"));
        }
        let (quorum, receipts) = self.pools[selected]
            .get_disks_by_key(key)
            .prepare_native_bytes(
                self.id,
                bucket,
                key,
                preparation,
                incarnation,
                body.into(),
                fence,
                observed.metadata,
                owner,
            )
            .await?;
        NativeQuorumPreparation::from_receipts(quorum, receipts)
    }
    pub async fn recover_native_object(&self, bucket: &str, key: &str, preparation: Uuid) -> Result<NativeQuorumPreparation> {
        check_put_object_args(bucket, key)?;
        if preparation.is_nil() || self.pools.is_empty() {
            return Err(Error::other("invalid native preparation"));
        }
        let _owner = self
            .handle_new_ns_lock(bucket, key)
            .await?
            .get_write_lock(crate::set_disk::get_lock_acquire_timeout())
            .await?;
        self.recover_native_object_unlocked(bucket, key, preparation)
            .await
            .map(|(owner, _)| owner)
    }
    /// Caller owns the distributed namespace lock; each disk still acquires its actual physical recovery owners.
    pub(crate) async fn recover_native_object_unlocked(
        &self,
        bucket: &str,
        key: &str,
        preparation: Uuid,
    ) -> Result<(NativeQuorumPreparation, Vec<(Uuid, bytes::Bytes)>)> {
        check_put_object_args(bucket, key)?;
        if preparation.is_nil() || self.pools.is_empty() {
            return Err(Error::other("invalid native preparation"));
        }
        let fence = self.object_lock_config_snapshot(bucket).await?;
        let incarnation = self.bucket_incarnation_id(bucket).await?;
        let observations = self
            .native_observations_scan(bucket, key, preparation, incarnation, fence.clone(), true)
            .await?;
        let selected = Self::existing_native_pool(&observations)?.ok_or_else(|| Error::other("native preparation absent"))?;
        let (quorum, receipts, metadata) = observations[selected]
            .set
            .recover_native_bytes(
                self.id,
                bucket,
                key,
                preparation,
                incarnation,
                fence,
                observations[selected].prepared.clone(),
            )
            .await?;
        Ok((NativeQuorumPreparation::from_receipts(quorum, receipts)?, metadata))
    }
}

#[cfg(all(test, unix))]
mod target_owner_tests {
    use super::*;
    use crate::{
        bucket::metadata_sys::{init_bucket_metadata_sys, test_support::isolated_store_over_temp_disks},
        disk::DiskAPI,
        object_api::PutObjReader,
        storage_api_contracts::bucket::{BucketOperations as _, MakeBucketOptions},
    };
    use uuid::Uuid;

    #[tokio::test]
    #[serial_test::serial]
    async fn target_owner_apis_use_fresh_local_identity_and_exact_disk() {
        let (dirs, store) = isolated_store_over_temp_disks().await;
        init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let bucket = "native-target-owner";
        let key = "object.bin";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("create target bucket");

        let preparation = Uuid::new_v4();
        let body = b"target-owner facade identity";
        let prepared = store
            .prepare_native_object(bucket, key, preparation, &mut PutObjReader::from_vec(body.to_vec()))
            .await
            .expect("prepare native object");
        let disk = store.pools[0].get_disks_by_key(key).disks.read().await[0]
            .clone()
            .expect("first local disk");
        let disk_id = disk.get_disk_id().await.expect("disk identity").expect("formatted disk");
        assert!(prepared.disk_ids().any(|id| id == disk_id));
        let target = NativePreparationTarget {
            store_id: store.id,
            disk_id,
            bucket: bucket.into(),
            key: key.into(),
            preparation,
            bucket_incarnation: store
                .bucket_incarnation_id_from_disk(bucket)
                .await
                .expect("durable bucket incarnation"),
        };

        let metadata_path = dirs[0]
            .path()
            .join(".rustfs.sys/native-preparations")
            .join(preparation.to_string())
            .join("xl.meta");
        let metadata = Bytes::from(std::fs::read(metadata_path).expect("persisted shard metadata"));
        let unrelated_format = dirs[1].path().join(".rustfs.sys/format.json");
        let saved_unrelated_format = std::fs::read(&unrelated_format).expect("unrelated disk format");
        std::fs::write(&unrelated_format, b"invalid format").expect("damage unrelated disk format");
        let caller_owner = store
            .handle_new_ns_lock(bucket, key)
            .await
            .expect("caller namespace lock")
            .get_write_lock(crate::set_disk::get_lock_acquire_timeout())
            .await
            .expect("caller owns namespace lock");
        let receipt = store
            .finalize_native_preparation_on_disk(NativePreparationFinalizeRequest {
                target: target.clone(),
                version: preparation,
                object_digest: prepared.object_digest(),
                metadata: metadata.clone(),
            })
            .await
            .expect("target-owner idempotent finalize");
        assert_eq!(receipt.disk_id(), disk_id);
        assert_eq!(receipt.object_digest(), &prepared.object_digest());

        assert!(matches!(
            tokio::time::timeout(
                std::time::Duration::from_secs(2),
                store.probe_native_preparation_on_disk(&target),
            )
            .await
            .expect("target-owner must not reacquire the caller's non-reentrant lock")
            .expect("target-owner probe"),
            NativePreparationProbe::Existing { prepared: true, metadata: Some(found), .. } if found == metadata
        ));
        let recovered = store
            .recover_native_preparation_on_disk(&target)
            .await
            .expect("target-owner recovery");
        assert_eq!(recovered.receipt.disk_id(), disk_id);
        assert_eq!(recovered.metadata, metadata);

        let mut wrong_incarnation = target.clone();
        wrong_incarnation.bucket_incarnation = Uuid::new_v4();
        assert!(store.begin_native_preparation_on_disk(&wrong_incarnation).await.is_err());
        let mut wrong_disk = target;
        wrong_disk.disk_id = Uuid::new_v4();
        assert!(store.begin_native_preparation_on_disk(&wrong_disk).await.is_err());
        drop(caller_owner);
        std::fs::write(unrelated_format, saved_unrelated_format).expect("restore unrelated disk format");
    }
}
