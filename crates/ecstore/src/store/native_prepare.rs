//! Private native object preparation; a data quorum is not publication authority.
use super::ECStore;
use crate::bucket::utils::check_put_object_args;
use crate::{
    disk::local::native_prepare::NativePreparationReceipt,
    error::{Error, Result},
    object_api::PutObjReader,
};
use uuid::Uuid;

pub struct NativeQuorumPreparation {
    receipts: Vec<NativePreparationReceipt>,
}
impl NativeQuorumPreparation {
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
        self.receipts.len()
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
    identity: Option<Identity>,
    metadata: Vec<Option<bytes::Bytes>>,
    prepared: Vec<bool>,
    unknown: bool,
}
impl NativeQuorumPreparation {
    fn from_receipts(quorum: usize, receipts: Vec<NativePreparationReceipt>) -> Result<Self> {
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
    async fn native_observations(
        &self,
        bucket: &str,
        key: &str,
        preparation: Uuid,
        incarnation: Uuid,
        fence: std::sync::Arc<crate::object_api::ObjectLockConfigSnapshot>,
    ) -> Result<Vec<Observation>> {
        let mut pools = Vec::new();
        for pool in &self.pools {
            let candidates = pool
                .get_disks_by_key(key)
                .probe_native_candidates(self.id, bucket, key, preparation, incarnation, fence.clone())
                .await;
            let mut observed = Observation {
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
                        if version != preparation || observed.identity.as_ref().is_some_and(|old| old != &identity) {
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
        if preparation.is_nil() || self.pools.is_empty() || data.size() < 0 || data.size() > 1024 * 1024 {
            return Err(Error::other("unsupported native preparation"));
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
        let fence = self.object_lock_config_snapshot(bucket).await?;
        let incarnation = self.bucket_incarnation_id(bucket).await?;
        let _owner = self
            .handle_new_ns_lock(bucket, key)
            .await?
            .get_write_lock(crate::set_disk::get_lock_acquire_timeout())
            .await?;
        let observations = self
            .native_observations(bucket, key, preparation, incarnation, fence.clone())
            .await?;
        let selected = Self::existing_native_pool(&observations)?.ok_or_else(|| Error::other("native preparation absent"))?;
        let (quorum, receipts) = self.pools[selected]
            .get_disks_by_key(key)
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
        NativeQuorumPreparation::from_receipts(quorum, receipts)
    }
}
