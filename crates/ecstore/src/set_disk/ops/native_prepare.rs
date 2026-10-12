//! Encode native candidates privately; only LocalDisk can attest durable shards.
use crate::{
    disk::{
        Disk,
        error::Error as DiskError,
        local::native_prepare::{NativePreparationProbe, NativePreparationReceipt, NativePrepareInput},
    },
    erasure::coding::{BitrotReader, Erasure, encode::IntegrityEncodeMode},
    error::{Error, Result},
    object_api::ObjectLockConfigSnapshot,
    set_disk::{SetDisks, resolve_write_layout},
};
use bytes::Bytes;
use rustfs_filemeta::{FileInfo, FileInfoOpts, FileMeta};
use sha2::{Digest, Sha256};
use std::{io::Cursor, sync::Arc};
use tokio::sync::mpsc;
use uuid::Uuid;

pub(crate) fn same_identity(a: &NativePreparationReceipt, b: &NativePreparationReceipt) -> bool {
    a.preparation_id() == b.preparation_id()
        && a.version_id() == b.version_id()
        && a.bucket() == b.bucket()
        && a.key() == b.key()
        && a.deployment_id() == b.deployment_id()
        && a.bucket_incarnation() == b.bucket_incarnation()
        && a.object_digest() == b.object_digest()
        && a.object_length() == b.object_length()
        && a.set_digest() == b.set_digest()
        && a.distribution_digest() == b.distribution_digest()
        && a.erasure_geometry().0 == b.erasure_geometry().0
        && a.erasure_geometry().1 == b.erasure_geometry().1
}

impl SetDisks {
    pub(crate) async fn prepare_native_bytes(
        &self,
        store_id: Uuid,
        bucket: &str,
        key: &str,
        preparation: Uuid,
        incarnation: Uuid,
        body: Bytes,
        fence: Arc<ObjectLockConfigSnapshot>,
        saved_metadata: Vec<Option<Bytes>>,
        operation_owner: Arc<rustfs_lock::NamespaceLockGuard>,
    ) -> Result<(usize, Vec<NativePreparationReceipt>)> {
        let disks = self.get_disks_internal().await;
        if disks.iter().flatten().any(|disk| matches!(disk.as_ref(), Disk::Remote(_))) {
            return Err(Error::other("native remote preparation unsupported"));
        }
        let layout = resolve_write_layout(
            &self.storage_class_config_snapshot(),
            self.pool_index,
            disks.len(),
            self.default_parity_count,
            None,
            false,
        )?;
        let mut info = FileInfo::new(&format!("{bucket}/{key}"), layout.data_drives, layout.parity_drives);
        info.name = key.to_owned();
        // This is the actual private xl.meta version, not a claim about S3 readability.
        info.version_id = Some(preparation);
        info.data_dir = Some(preparation);
        info.size = i64::try_from(body.len()).map_err(Error::other)?;
        info.versioned = true;
        // Mint one timestamp for new preparation; retries use the persisted writer value.
        let mut modification_time = None;
        for metadata in saved_metadata.iter().flatten() {
            let saved = rustfs_filemeta::get_file_info(
                metadata,
                bucket,
                key,
                &preparation.to_string(),
                FileInfoOpts {
                    data: false,
                    include_free_versions: false,
                    include_part_checksums: true,
                },
            )?;
            let time = saved
                .mod_time
                .ok_or_else(|| Error::other("persisted native modification time missing"))?;
            if modification_time.is_some_and(|old| old != time) {
                return Err(Error::other("conflicting native modification times"));
            }
            modification_time = Some(time);
        }
        info.mod_time = Some(modification_time.unwrap_or_else(time::OffsetDateTime::now_utc));
        info.add_object_part(1, String::new(), body.len(), None, info.size, None, None);
        let erasure = Arc::new(Erasure::try_new_with_options(
            info.erasure.data_blocks,
            info.erasure.parity_blocks,
            info.erasure.block_size,
            false,
        )?);
        if body.len() > erasure.block_size || body.len() > 1024 * 1024 {
            return Err(Error::other("native preparation requires bounded single-block inline data"));
        }
        if !self
            .storage_class_config_snapshot()
            .should_inline(erasure.shard_file_size(info.size), layout.data_drives, true)
        {
            return Err(Error::other("native preparation exceeds configured inline admission"));
        }
        let digest: [u8; 32] = Sha256::digest(&body).into();
        let mut writers = std::iter::repeat_with(|| None).take(disks.len()).collect::<Vec<_>>();
        let (_, size, shards, _) = erasure
            .encode_with_shard_integrity(
                Cursor::new(body),
                &mut writers,
                layout.write_quorum,
                1,
                IntegrityEncodeMode::Inline(usize::try_from(info.size).map_err(Error::other)?),
                false,
            )
            .await?;
        if i64::try_from(size).map_err(Error::other)? != info.size {
            return Err(Error::other("native encoder length mismatch"));
        }
        let shards = shards.ok_or_else(|| Error::other("native inline encoder omitted shards"))?;
        let (send, mut receive) = mpsc::unbounded_channel();
        for (slot, disk) in disks.into_iter().enumerate() {
            let Some(disk) = disk else {
                continue;
            };
            let Disk::Local(wrapper) = disk.as_ref() else {
                return Err(Error::other("native remote preparation owner is not supported"));
            };
            let local = wrapper.get_disk();
            let mut per_disk = info.clone();
            let index = *info
                .erasure
                .distribution
                .get(slot)
                .ok_or_else(|| Error::other("native disk distribution missing"))?;
            per_disk.erasure.index = index;
            per_disk.data = Some(
                shards
                    .get(
                        index
                            .checked_sub(1)
                            .ok_or_else(|| Error::other("invalid native shard index"))?,
                    )
                    .cloned()
                    .ok_or_else(|| Error::other("native encoder omitted assigned shard"))?,
            );
            per_disk.set_inline_data();
            let metadata = if let Some(bytes) = saved_metadata.get(slot).and_then(Option::as_ref) {
                let saved = rustfs_filemeta::get_file_info(
                    bytes,
                    bucket,
                    key,
                    &preparation.to_string(),
                    FileInfoOpts {
                        data: true,
                        include_free_versions: false,
                        include_part_checksums: true,
                    },
                )?;
                if saved.version_id != per_disk.version_id
                    || saved.deleted
                    || saved.mark_deleted
                    || saved.uses_legacy_checksum != per_disk.uses_legacy_checksum
                    || saved.mod_time != per_disk.mod_time
                    || saved.size != per_disk.size
                    || saved.erasure.distribution != per_disk.erasure.distribution
                    || saved.erasure.index != index
                    || saved.erasure.block_size != per_disk.erasure.block_size
                    || saved.erasure.data_blocks != layout.data_drives
                    || saved.erasure.parity_blocks != layout.parity_drives
                    || saved.data != per_disk.data
                {
                    return Err(Error::other("persisted native shard does not encode the request content"));
                }
                bytes.clone()
            } else {
                let mut encoded = FileMeta::new();
                encoded.add_version(per_disk)?;
                Bytes::from(encoded.marshal_msg()?)
            };
            let input = NativePrepareInput {
                preparation,
                store_id,
                bucket: bucket.to_owned(),
                bucket_incarnation: incarnation,
                object_digest: digest,
                key: key.to_owned(),
                version: preparation,
                metadata,
                bucket_fence: fence.clone(),
            };
            let sender = send.clone();
            let parent = operation_owner.clone();
            // No per-disk lock is acquired here. NP01 owns its actual lock chain.
            // Detached tasks retain their owners after quorum or caller cancellation;
            // an unobserved reply leaves durable facts for probe/recovery, never rollback.
            tokio::spawn(async move {
                let result = match local.prepare_native_with_owner(input, parent).await {
                    Ok(receipt) => Ok(Some(receipt)),
                    Err(DiskError::Io(error)) if error.kind() == std::io::ErrorKind::InvalidData => Err(Error::from(error)),
                    Err(_) => Ok(None),
                };
                let _ = sender.send(result);
            });
        }
        drop(send);
        let mut receipts = Vec::with_capacity(layout.write_quorum);
        while let Some(result) = receive.recv().await {
            if let Some(receipt) = result? {
                if let Some(first) = receipts.first() {
                    if !same_identity(first, &receipt) {
                        return Err(Error::other("conflicting native preparation identities"));
                    }
                }
                if receipts
                    .iter()
                    .any(|r: &NativePreparationReceipt| r.disk_id() == receipt.disk_id())
                {
                    return Err(Error::other("duplicate native physical disk"));
                }
                receipts.push(receipt);
                if receipts.len() >= layout.write_quorum {
                    return Ok((layout.write_quorum, receipts));
                }
            }
        }
        Err(Error::ErasureWriteQuorum)
    }

    pub(crate) async fn recover_native_bytes(
        &self,
        store_id: Uuid,
        bucket: &str,
        key: &str,
        preparation: Uuid,
        incarnation: Uuid,
        fence: Arc<ObjectLockConfigSnapshot>,
        prepared_slots: Vec<bool>,
    ) -> Result<(usize, Vec<NativePreparationReceipt>)> {
        let disks = self.get_disks_internal().await;
        let layout = resolve_write_layout(
            &self.storage_class_config_snapshot(),
            self.pool_index,
            disks.len(),
            self.default_parity_count,
            None,
            false,
        )?;
        let mut receipts: Vec<NativePreparationReceipt> = Vec::new();
        let mut readers = std::iter::repeat_with(|| None).take(disks.len()).collect::<Vec<_>>();
        let mut erasure = None;
        let mut modification_time = None;
        let mut legacy_codec = None;
        let mut block_size = None;
        // Inspect every available disk: an identity conflict cannot be majority-voted away.
        for (slot, disk) in disks.into_iter().enumerate() {
            if !prepared_slots.get(slot).copied().unwrap_or(false) {
                continue;
            }
            let disk = disk.ok_or_else(|| Error::other("prepared native disk unavailable"))?;
            let Disk::Local(wrapper) = disk.as_ref() else {
                return Err(Error::other("native remote recovery unsupported"));
            };
            let local = wrapper.get_disk();
            let (receipt, metadata) = match local
                .recover_native_with_metadata(preparation, store_id, bucket, incarnation, fence.clone())
                .await
            {
                Ok(value) => value,
                Err(error) => return Err(error.into()),
            };
            if receipt.key() != key
                || receipt.preparation_id() != preparation
                || receipt.version_id() != preparation
                || receipt.bucket() != bucket
                || receipt.bucket_incarnation() != incarnation
                || receipt.deployment_id() != store_id
            {
                return Err(Error::other("native recovery request identity mismatch"));
            }
            if let Some(first) = receipts.first() {
                if !same_identity(first, &receipt) {
                    return Err(Error::other("conflicting native recovery identities"));
                }
            }
            if receipts.iter().any(|r| r.disk_id() == receipt.disk_id()) {
                return Err(Error::other("duplicate native recovery disk"));
            }
            let info = rustfs_filemeta::get_file_info(
                &metadata,
                bucket,
                key,
                &preparation.to_string(),
                FileInfoOpts {
                    data: true,
                    include_free_versions: false,
                    include_part_checksums: true,
                },
            )?;
            let time = info
                .mod_time
                .ok_or_else(|| Error::other("recovered native modification time missing"))?;
            if modification_time.is_some_and(|old| old != time) {
                return Err(Error::other("conflicting recovered native modification times"));
            }
            modification_time = Some(time);
            if info.erasure.data_blocks != layout.data_drives || info.erasure.parity_blocks != layout.parity_drives {
                return Err(Error::other("native recovery layout changed"));
            }
            if block_size.is_some_and(|old| old != info.erasure.block_size) {
                return Err(Error::other("conflicting recovered native block sizes"));
            }
            block_size = Some(info.erasure.block_size);
            if legacy_codec.is_some_and(|old| old != info.uses_legacy_checksum) {
                return Err(Error::other("conflicting recovered native codecs"));
            }
            legacy_codec = Some(info.uses_legacy_checksum);
            let selected = Erasure::try_new_with_options(
                info.erasure.data_blocks,
                info.erasure.parity_blocks,
                info.erasure.block_size,
                info.uses_legacy_checksum,
            )?;
            let index = info
                .erasure
                .index
                .checked_sub(1)
                .ok_or_else(|| Error::other("invalid recovered native index"))?;
            let slot = readers
                .get_mut(index)
                .ok_or_else(|| Error::other("recovered native index outside set"))?;
            if slot.is_some() {
                return Err(Error::other("duplicate recovered native shard"));
            }
            let checksum = info.erasure.get_checksum_info(1).algorithm;
            let checksum = if info.uses_legacy_checksum && checksum == rustfs_utils::HashAlgorithm::HighwayHash256S {
                rustfs_utils::HashAlgorithm::HighwayHash256SLegacy
            } else {
                checksum
            };
            *slot = Some(BitrotReader::new(
                Cursor::new(info.data.ok_or_else(|| Error::other("native recovery shard missing"))?),
                selected.shard_size(),
                checksum,
                false,
            ));
            erasure = Some(selected);
            receipts.push(receipt);
        }
        if receipts.len() < layout.write_quorum {
            return Err(Error::ErasureWriteQuorum);
        }
        let first = receipts.first().ok_or(Error::ErasureWriteQuorum)?;
        let length = usize::try_from(first.object_length()).map_err(Error::other)?;
        if length > 1024 * 1024 {
            return Err(Error::other("native recovery length exceeds inline limit"));
        }
        let mut output = Vec::with_capacity(length);
        let (size, failure) = erasure
            .ok_or(Error::ErasureWriteQuorum)?
            .decode(&mut output, readers, 0, length, length)
            .await;
        if let Some(error) = failure {
            return Err(Error::from(error));
        }
        if size != length || output.len() != length || Sha256::digest(&output).as_slice() != first.object_digest() {
            return Err(Error::other("native recovered content digest mismatch"));
        }
        Ok((layout.write_quorum, receipts))
    }
}

impl SetDisks {
    pub(crate) async fn probe_native_candidates(
        &self,
        store_id: Uuid,
        bucket: &str,
        key: &str,
        preparation: Uuid,
        incarnation: Uuid,
        fence: Arc<ObjectLockConfigSnapshot>,
    ) -> Vec<std::result::Result<NativePreparationProbe, DiskError>> {
        let mut observations = Vec::new();
        for disk in self.get_disks_internal().await {
            observations.push(match disk {
                Some(disk) => match disk.as_ref() {
                    Disk::Local(local) => {
                        local
                            .get_disk()
                            .probe_native(preparation, store_id, bucket, key, incarnation, fence.clone())
                            .await
                    }
                    Disk::Remote(_) => Err(DiskError::InvalidPath),
                },
                None => Err(DiskError::InvalidPath),
            });
        }
        observations
    }
}
