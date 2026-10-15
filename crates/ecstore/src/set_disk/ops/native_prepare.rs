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

pub(super) struct DigestReader<R> {
    inner: R,
    digest: Sha256,
}
impl<R> DigestReader<R> {
    pub(super) fn new(inner: R) -> Self {
        Self {
            inner,
            digest: Sha256::new(),
        }
    }
    pub(super) fn into_inner_and_digest(self) -> (R, [u8; 32]) {
        (self.inner, self.digest.finalize().into())
    }
}
impl<R: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for DigestReader<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let result = std::pin::Pin::new(&mut self.inner).poll_read(cx, buf);
        if matches!(result, std::task::Poll::Ready(Ok(()))) {
            self.digest.update(&buf.filled()[before..]);
        }
        result
    }
}

struct DigestSink {
    digest: Sha256,
    length: usize,
}
impl tokio::io::AsyncWrite for DigestSink {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.length.checked_add(bytes.len()) {
            Some(length) => {
                self.length = length;
                self.digest.update(bytes);
                std::task::Poll::Ready(Ok(bytes.len()))
            }
            None => std::task::Poll::Ready(Err(std::io::Error::other("native content length overflow"))),
        }
    }
    fn poll_flush(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

impl SetDisks {
    fn native_expected_set_digest(&self, store_id: Uuid) -> Result<[u8; 32]> {
        let set = self
            .format
            .erasure
            .sets
            .get(self.set_index)
            .ok_or_else(|| Error::other("native target set missing"))?;
        if self.format.id != store_id
            || set.len() != self.set_drive_count
            || set.iter().any(Uuid::is_nil)
            || set.iter().enumerate().any(|(slot, id)| set[slot + 1..].contains(id))
        {
            return Err(Error::other("invalid native target physical set"));
        }
        Ok(Sha256::digest(serde_json::to_vec(set)?).into())
    }

    /// Attest already encoded private shards while the caller owns the actual PUT commit guards.
    pub(crate) async fn attest_native_stream(
        &self,
        store_id: Uuid,
        bucket: &str,
        key: &str,
        preparation: Uuid,
        incarnation: Uuid,
        candidates: Vec<(crate::disk::DiskStore, FileInfo)>,
        object_digest: [u8; 32],
        object_length: u64,
        fence: Arc<ObjectLockConfigSnapshot>,
    ) -> Result<crate::store::native_prepare::NativeQuorumPreparation> {
        self.attest_native_stream_owned(
            store_id,
            bucket,
            key,
            preparation,
            incarnation,
            candidates,
            object_digest,
            object_length,
            fence,
            None,
        )
        .await
    }
    async fn attest_native_stream_owned(
        &self,
        store_id: Uuid,
        bucket: &str,
        key: &str,
        preparation: Uuid,
        incarnation: Uuid,
        candidates: Vec<(crate::disk::DiskStore, FileInfo)>,
        object_digest: [u8; 32],
        object_length: u64,
        fence: Arc<ObjectLockConfigSnapshot>,
        operation_owner: Option<Arc<rustfs_lock::NamespaceLockGuard>>,
    ) -> Result<crate::store::native_prepare::NativeQuorumPreparation> {
        let expected_set = self.native_expected_set_digest(store_id)?;
        if !fence.is_valid_for_destructive_put(store_id, bucket, incarnation) {
            return Err(Error::other("native adoption bucket fence mismatch"));
        }
        let layout = resolve_write_layout(
            &self.storage_class_config_snapshot(),
            self.pool_index,
            self.set_drive_count,
            self.default_parity_count,
            None,
            false,
        )?;
        let (send, mut receive) = mpsc::unbounded_channel();
        for (disk, info) in candidates {
            if info.size != i64::try_from(object_length).map_err(Error::other)?
                || (info.name != key && info.name != format!("{bucket}/{key}"))
                || info.inline_data()
                || info.deleted
                || info.mark_deleted
            {
                return Err(Error::other("native adoption metadata identity mismatch"));
            }
            let version = info
                .version_id
                .filter(|id| !id.is_nil())
                .ok_or_else(|| Error::other("native adoption version missing"))?;
            let mut metadata = FileMeta::new();
            metadata.add_version(info)?;
            let input = NativePrepareInput {
                preparation,
                store_id,
                bucket: bucket.into(),
                bucket_incarnation: incarnation,
                object_digest,
                key: key.into(),
                version,
                metadata: metadata.marshal_msg()?.into(),
                bucket_fence: fence.clone(),
            };
            let send = send.clone();
            let owner = operation_owner.clone();
            tokio::spawn(async move {
                let result = match (disk.as_ref(), owner) {
                    (Disk::Local(local), Some(owner)) => local.get_disk().prepare_native_with_owner(input, owner).await,
                    (_, owner) => {
                        let _owner = owner;
                        disk.native_finalize(input).await
                    }
                };
                let _ = send.send(result);
            });
        }
        drop(send);
        let mut receipts = Vec::new();
        while let Some(result) = receive.recv().await {
            let receipt = match result {
                Ok(receipt) => receipt,
                Err(DiskError::Io(error)) if error.kind() == std::io::ErrorKind::InvalidData => return Err(error.into()),
                Err(_) => continue,
            };
            if receipt.set_digest() != &expected_set
                || receipts.first().is_some_and(|old| !same_identity(old, &receipt))
                || receipts
                    .iter()
                    .any(|old: &NativePreparationReceipt| old.disk_id() == receipt.disk_id())
            {
                return Err(Error::other("native adoption quorum identity mismatch"));
            }
            receipts.push(receipt);
        }
        crate::store::native_prepare::NativeQuorumPreparation::from_receipts(layout.write_quorum, receipts)
    }

    pub(crate) async fn prepare_native_stream(
        &self,
        store_id: Uuid,
        bucket: &str,
        key: &str,
        preparation: Uuid,
        incarnation: Uuid,
        data: &mut crate::object_api::PutObjReader,
        fence: Arc<ObjectLockConfigSnapshot>,
        operation_owner: Arc<rustfs_lock::NamespaceLockGuard>,
    ) -> Result<(usize, Vec<NativePreparationReceipt>)> {
        use crate::disk::RUSTFS_META_BUCKET;
        let disks = self.get_disks_internal().await;
        let layout = resolve_write_layout(
            &self.storage_class_config_snapshot(),
            self.pool_index,
            disks.len(),
            self.default_parity_count,
            None,
            false,
        )?;
        let mut info = FileInfo::new(key, layout.data_drives, layout.parity_drives);
        info.name = key.to_owned();
        info.version_id = Some(preparation);
        info.data_dir = Some(preparation);
        info.size = data.size();
        info.versioned = true;
        info.mod_time = Some(time::OffsetDateTime::now_utc());
        let erasure = Arc::new(Erasure::try_new_with_options(
            info.erasure.data_blocks,
            info.erasure.parity_blocks,
            info.erasure.block_size,
            false,
        )?);
        let mut shuffled = Self::shuffle_disks_owned(disks.clone(), &info.erasure.distribution);
        let path = format!("native-preparations/{preparation}/{preparation}/part.1");
        let mut writers = Vec::with_capacity(shuffled.len());
        for disk in &mut shuffled {
            let result = match disk {
                Some(disk) => {
                    if let Err(error) = disk
                        .native_begin_stream(preparation, store_id, bucket, key, incarnation, fence.clone())
                        .await
                    {
                        if matches!(&error, DiskError::Io(error) if error.kind() == std::io::ErrorKind::InvalidData) {
                            return Err(error.into());
                        }
                        writers.push(None);
                        continue;
                    }
                    crate::io_support::bitrot::create_bitrot_writer(
                        false,
                        Some(disk),
                        RUSTFS_META_BUCKET,
                        &path,
                        erasure.shard_file_size(info.size),
                        erasure.shard_size(),
                        rustfs_utils::HashAlgorithm::HighwayHash256S,
                    )
                    .await
                }
                None => Err(DiskError::DiskNotFound),
            };
            match result {
                Ok(writer) => writers.push(Some(writer)),
                Err(_) => {
                    *disk = None;
                    writers.push(None);
                }
            }
        }
        let stream = std::mem::replace(
            &mut data.stream,
            rustfs_rio::HashReader::from_stream(Cursor::new(Vec::new()), 0, 0, None, None, false)?,
        );
        let reader = DigestReader::new(stream);
        let (reader, size, _, _) = erasure
            .encode_with_shard_integrity(reader, &mut writers, layout.write_quorum, 1, IntegrityEncodeMode::Streaming, false)
            .await?;
        let (stream, digest) = reader.into_inner_and_digest();
        data.stream = stream;
        if info.size >= 0 && i64::try_from(size).map_err(Error::other)? != info.size {
            return Err(Error::other("native stream length mismatch"));
        }
        info.size = i64::try_from(size).map_err(Error::other)?;
        info.add_object_part(1, String::new(), size, None, info.size, None, None);
        let mut candidates = Vec::new();
        for (slot, disk) in disks.into_iter().enumerate() {
            let index = *info
                .erasure
                .distribution
                .get(slot)
                .ok_or_else(|| Error::other("native stream distribution missing"))?;
            if writers
                .get(
                    index
                        .checked_sub(1)
                        .ok_or_else(|| Error::other("invalid native stream index"))?,
                )
                .and_then(Option::as_ref)
                .is_none()
            {
                continue;
            }
            let Some(disk) = disk else {
                continue;
            };
            let mut per_disk = info.clone();
            per_disk.erasure.index = index;
            candidates.push((disk, per_disk));
        }
        let owner = self
            .attest_native_stream_owned(
                store_id,
                bucket,
                key,
                preparation,
                incarnation,
                candidates,
                digest,
                u64::try_from(size).map_err(Error::other)?,
                fence,
                Some(operation_owner),
            )
            .await?;
        Ok((layout.write_quorum, owner.into_receipts()))
    }

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
    ) -> Result<(usize, Vec<NativePreparationReceipt>, Vec<(Uuid, Bytes)>)> {
        let disks = self.get_disks_internal().await;
        let expected_set = self.native_expected_set_digest(store_id)?;
        let layout = resolve_write_layout(
            &self.storage_class_config_snapshot(),
            self.pool_index,
            disks.len(),
            self.default_parity_count,
            None,
            false,
        )?;
        let mut receipts: Vec<NativePreparationReceipt> = Vec::new();
        let mut verified_metadata = Vec::new();
        let mut sources: Vec<Option<(crate::disk::DiskStore, FileInfo)>> =
            std::iter::repeat_with(|| None).take(disks.len()).collect();
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
            let (receipt, metadata) = match disk
                .native_recover(preparation, store_id, bucket, key, incarnation, fence.clone())
                .await
            {
                Ok(value) => value,
                Err(error) => return Err(error.into()),
            };
            if receipt.set_digest() != &expected_set {
                return Err(Error::other("native recovery receipt target physical set mismatch"));
            }
            if receipt.key() != key
                || receipt.preparation_id() != preparation
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
            verified_metadata.push((receipt.disk_id(), metadata.clone()));
            let info = rustfs_filemeta::get_file_info(
                &metadata,
                bucket,
                key,
                &receipt.version_id().to_string(),
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
            let slot = sources
                .get_mut(index)
                .ok_or_else(|| Error::other("recovered native index outside set"))?;
            if slot.is_some() {
                return Err(Error::other("duplicate recovered native shard"));
            }
            if let Some((_, previous)) = sources.iter().flatten().next() {
                if previous.parts != info.parts || previous.data_dir != info.data_dir {
                    return Err(Error::other("conflicting native recovered part manifest"));
                }
            }
            sources[index] = Some((disk.clone(), info));
            erasure = Some(selected);
            receipts.push(receipt);
        }
        if receipts.len() < layout.write_quorum {
            return Err(Error::ErasureWriteQuorum);
        }
        let first = receipts.first().ok_or(Error::ErasureWriteQuorum)?;
        let length = usize::try_from(first.object_length()).map_err(Error::other)?;
        let mut output = DigestSink {
            digest: Sha256::new(),
            length: 0,
        };
        let codec = erasure.ok_or(Error::ErasureWriteQuorum)?;
        let parts = sources
            .iter()
            .flatten()
            .next()
            .ok_or(Error::ErasureWriteQuorum)?
            .1
            .parts
            .clone();
        let mut size = 0_usize;
        for part in parts {
            let mut readers = Vec::with_capacity(sources.len());
            for source in &sources {
                let Some((disk, info)) = source else {
                    readers.push(None);
                    continue;
                };
                let checksum = info.erasure.get_checksum_info(part.number).algorithm;
                let checksum = if info.uses_legacy_checksum && checksum == rustfs_utils::HashAlgorithm::HighwayHash256S {
                    rustfs_utils::HashAlgorithm::HighwayHash256SLegacy
                } else {
                    checksum
                };
                let reader = if info.inline_data() {
                    crate::io_support::bitrot::ShardReader::InMemory(Cursor::new(
                        info.data.clone().ok_or_else(|| Error::other("native inline shard missing"))?,
                    ))
                } else {
                    use crate::disk::DiskAPI;
                    let data_dir = info.data_dir.ok_or_else(|| Error::other("native stream data dir missing"))?;
                    let shard_size = usize::try_from(codec.shard_file_size(i64::try_from(part.size).map_err(Error::other)?))
                        .map_err(Error::other)?;
                    let physical = if matches!(
                        checksum,
                        rustfs_utils::HashAlgorithm::HighwayHash256S | rustfs_utils::HashAlgorithm::HighwayHash256SLegacy
                    ) {
                        shard_size
                            .div_ceil(codec.shard_size())
                            .checked_mul(checksum.size())
                            .and_then(|hashes| shard_size.checked_add(hashes))
                            .ok_or_else(|| Error::other("native physical shard length overflow"))?
                    } else {
                        shard_size
                    };
                    crate::io_support::bitrot::ShardReader::Stream(
                        disk.read_file_stream(
                            crate::disk::RUSTFS_META_BUCKET,
                            &format!("native-preparations/{preparation}/{data_dir}/part.{}", part.number),
                            0,
                            physical,
                        )
                        .await?,
                    )
                };
                readers.push(Some(BitrotReader::new(reader, codec.shard_size(), checksum, false)));
            }
            let (decoded, failure) = codec.decode(&mut output, readers, 0, part.size, part.size).await;
            if let Some(error) = failure {
                return Err(error.into());
            }
            if decoded != part.size {
                return Err(Error::other("native recovered part length mismatch"));
            }
            size = size
                .checked_add(decoded)
                .ok_or_else(|| Error::other("native decoded length overflow"))?;
        }
        if size != length || output.length != length || output.digest.finalize().as_slice() != first.object_digest() {
            return Err(Error::other("native recovered content digest mismatch"));
        }
        Ok((layout.write_quorum, receipts, verified_metadata))
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
        match_object: bool,
    ) -> Vec<std::result::Result<NativePreparationProbe, DiskError>> {
        let mut observations = Vec::new();
        for disk in self.get_disks_internal().await {
            observations.push(match disk {
                Some(disk) => {
                    if match_object {
                        match disk.native_locate(store_id, preparation).await {
                            Ok(Some(record))
                                if record.bucket != bucket || record.key != key || record.incarnation != incarnation =>
                            {
                                observations.push(Ok(NativePreparationProbe::Absent));
                                continue;
                            }
                            Err(error) => {
                                observations.push(Err(error));
                                continue;
                            }
                            _ => {}
                        }
                    }
                    disk.native_probe(preparation, store_id, bucket, key, incarnation, fence.clone())
                        .await
                }
                None => Err(DiskError::InvalidPath),
            });
        }
        observations
    }
}
