//! Durable private local preparations. A local receipt does not authorize publication.
use super::LocalDisk;
use crate::{
    disk::{
        DiskAPI,
        error::{DiskError, Result},
    },
    object_api::ObjectLockConfigSnapshot,
};
use rustfs_filemeta::FileInfo;
use std::sync::Arc;
use uuid::Uuid;

/// Trusted writer inputs. The local owner verifies the stored shard separately
/// from the object digest, whose erasure construction belongs to the coordinator.
#[derive(Clone)]
pub struct NativePrepareInput {
    pub preparation: Uuid,
    pub store_id: Uuid,
    pub bucket: String,
    pub bucket_incarnation: Uuid,
    pub object_digest: [u8; 32],
    pub key: String,
    pub version: Uuid,
    pub metadata: bytes::Bytes,
    pub bucket_fence: Arc<ObjectLockConfigSnapshot>,
}
/// Only a successful local durability boundary can construct this value.
pub struct NativePreparationReceipt {
    preparation: Uuid,
    version: Uuid,
    disk: Uuid,
    set_digest: [u8; 32],
    distribution_digest: [u8; 32],
    metadata_digest: [u8; 32],
    shard_digest: [u8; 32],
    bucket: String,
    key: String,
    deployment: Uuid,
    incarnation: Uuid,
    object_digest: [u8; 32],
    object_length: u64,
    data_blocks: usize,
    parity_blocks: usize,
    shard_index: usize,
}
impl NativePreparationReceipt {
    pub fn bucket(&self) -> &str {
        &self.bucket
    }
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn deployment_id(&self) -> Uuid {
        self.deployment
    }
    pub fn bucket_incarnation(&self) -> Uuid {
        self.incarnation
    }
    pub fn object_digest(&self) -> &[u8; 32] {
        &self.object_digest
    }
    pub fn object_length(&self) -> u64 {
        self.object_length
    }
    pub fn erasure_geometry(&self) -> (usize, usize, usize) {
        (self.data_blocks, self.parity_blocks, self.shard_index)
    }
    pub fn preparation_id(&self) -> Uuid {
        self.preparation
    }
    pub fn version_id(&self) -> Uuid {
        self.version
    }
    pub fn disk_id(&self) -> Uuid {
        self.disk
    }
    pub fn set_digest(&self) -> &[u8; 32] {
        &self.set_digest
    }
    pub fn distribution_digest(&self) -> &[u8; 32] {
        &self.distribution_digest
    }
    pub fn metadata_digest(&self) -> &[u8; 32] {
        &self.metadata_digest
    }
    pub fn shard_digest(&self) -> &[u8; 32] {
        &self.shard_digest
    }
}
/// A routing observation, never a durability or publication receipt.
pub(crate) enum NativePreparationProbe {
    Absent,
    Existing {
        version: Uuid,
        prepared: bool,
        object_digest: [u8; 32],
        object_length: u64,
        set_digest: [u8; 32],
        distribution_digest: [u8; 32],
        metadata: Option<bytes::Bytes>,
    },
}
#[cfg(all(test, unix))]
pub(crate) use tests::{clear_native_preparation_probe, pause_native_preparation};
impl LocalDisk {
    #[cfg(unix)]
    pub(crate) async fn probe_native(
        &self,
        preparation: Uuid,
        store_id: Uuid,
        bucket: &str,
        key: &str,
        incarnation: Uuid,
        fence: Arc<ObjectLockConfigSnapshot>,
    ) -> Result<NativePreparationProbe> {
        if preparation.is_nil() || !fence.is_valid_for_destructive_put(store_id, bucket, incarnation) {
            return Err(DiskError::InvalidPath);
        }
        let object_path = self.get_object_path(bucket, key)?;
        // Match the writer's metadata -> object -> preparation lock order.
        let metadata_owner = crate::disk::os::acquire_metadata_mutation_lease(&object_path, Some(fence.clone())).await;
        let object_owner = crate::disk::os::acquire_rename_data_mutation_lease_with_owner(
            &self.root,
            bucket,
            &object_path,
            Some(metadata_owner),
        )
        .await;
        let prep_path = self
            .root
            .join(".rustfs.sys/native-preparations")
            .join(preparation.to_string());
        let owner = crate::disk::os::acquire_namespace_mutation_lease_with_owner(&prep_path, Some(object_owner)).await;
        let disk = self
            .get_disk_id()
            .await?
            .filter(|id| !id.is_nil())
            .ok_or(DiskError::InvalidPath)?;
        let root = self.io_root().to_path_buf();
        let bucket = bucket.to_owned();
        let key = key.to_owned();
        crate::disk::os::run_blocking_namespace_operation(owner, move || {
            if !fence.is_valid_for_destructive_put(store_id, &bucket, incarnation) {
                return Err(std::io::Error::other("native preparation fence changed"));
            }
            let result = durable::probe(&root, preparation, disk, store_id, &bucket, &key, incarnation)?;
            if !fence.is_valid_for_destructive_put(store_id, &bucket, incarnation) {
                return Err(std::io::Error::other("native preparation fence changed"));
            }
            Ok(result)
        })
        .await
        .map_err(Into::into)
    }
    #[cfg(not(unix))]
    pub(crate) async fn probe_native(
        &self,
        _preparation: Uuid,
        _store_id: Uuid,
        _bucket: &str,
        _key: &str,
        _incarnation: Uuid,
        _fence: Arc<ObjectLockConfigSnapshot>,
    ) -> Result<NativePreparationProbe> {
        Err(DiskError::UnsupportedDisk)
    }
    #[cfg(not(unix))]
    pub(crate) async fn recover_native_with_metadata(
        &self,
        _preparation: Uuid,
        _store_id: Uuid,
        _bucket: &str,
        _incarnation: Uuid,
        _fence: Arc<ObjectLockConfigSnapshot>,
    ) -> Result<(NativePreparationReceipt, bytes::Bytes)> {
        Err(DiskError::UnsupportedDisk)
    }
    /// Inspect a persisted preparation under fresh lifecycle and namespace owners.
    /// Recovery re-establishes barriers after an earlier unknown I/O outcome.
    #[cfg(unix)]
    pub async fn recover_native(
        &self,
        preparation: Uuid,
        store_id: Uuid,
        bucket: &str,
        incarnation: Uuid,
        fence: Arc<ObjectLockConfigSnapshot>,
    ) -> Result<NativePreparationReceipt> {
        self.recover_native_with_metadata(preparation, store_id, bucket, incarnation, fence)
            .await
            .map(|(receipt, _)| receipt)
    }
    /// Return the exact metadata verified under the same recovery owners.
    #[cfg(unix)]
    pub(crate) async fn recover_native_with_metadata(
        &self,
        preparation: Uuid,
        store_id: Uuid,
        bucket: &str,
        incarnation: Uuid,
        fence: Arc<ObjectLockConfigSnapshot>,
    ) -> Result<(NativePreparationReceipt, bytes::Bytes)> {
        if preparation.is_nil() || !fence.is_valid_for_destructive_put(store_id, bucket, incarnation) {
            return Err(DiskError::InvalidPath);
        }
        let root = self.io_root().to_path_buf();
        let inspect_root = root.clone();
        let record = tokio::task::spawn_blocking(move || durable::inspect(&inspect_root, preparation))
            .await
            .map_err(|_| DiskError::InvalidPath)??;
        if record.bucket != bucket || record.incarnation != incarnation {
            return Err(DiskError::InvalidPath);
        }
        // Lock order: metadata -> object/volume -> private preparation -> sync admission.
        let object_path = self.get_object_path(bucket, &record.key)?;
        let metadata_owner = crate::disk::os::acquire_metadata_mutation_lease(&object_path, Some(fence.clone())).await;
        let object_owner = crate::disk::os::acquire_rename_data_mutation_lease_with_owner(
            &self.root,
            bucket,
            &object_path,
            Some(metadata_owner),
        )
        .await;
        let prep_path = self
            .root
            .join(".rustfs.sys/native-preparations")
            .join(preparation.to_string());
        let owner = crate::disk::os::acquire_namespace_mutation_lease_with_owner(&prep_path, Some(object_owner)).await;
        let disk = self
            .get_disk_id()
            .await?
            .filter(|id| !id.is_nil())
            .ok_or(DiskError::InvalidPath)?;
        let admission = crate::disk::os::acquire_file_sync_admission(self.file_sync_permits.clone()).await?;
        let runtime = tokio::runtime::Handle::current();
        crate::disk::os::run_blocking_namespace_operation(owner, move || {
            let _admission = admission;
            durable::recover(&root, record, disk, store_id, fence, runtime)
        })
        .await
        .map_err(Into::into)
    }
    /// Retain the real bucket and object owners through every blocking write.
    pub async fn prepare_native(&self, input: NativePrepareInput) -> Result<NativePreparationReceipt> {
        self.prepare_native_inner(input, None).await
    }
    pub(crate) async fn prepare_native_with_owner(
        &self,
        input: NativePrepareInput,
        owner: Arc<rustfs_lock::NamespaceLockGuard>,
    ) -> Result<NativePreparationReceipt> {
        self.prepare_native_inner(input, Some(owner)).await
    }
    async fn prepare_native_inner(
        &self,
        input: NativePrepareInput,
        operation_owner: Option<Arc<rustfs_lock::NamespaceLockGuard>>,
    ) -> Result<NativePreparationReceipt> {
        if input.preparation.is_nil()
            || input.bucket_incarnation.is_nil()
            || !input
                .bucket_fence
                .is_valid_for_destructive_put(input.store_id, &input.bucket, input.bucket_incarnation)
        {
            return Err(DiskError::InvalidPath);
        }
        if input.metadata.len() > 1024 * 1024 || input.version.is_nil() {
            return Err(DiskError::InvalidPath);
        }
        let raw_meta = rustfs_filemeta::FileMeta::load(&input.metadata)?;
        if raw_meta.versions.len() != 1 {
            return Err(DiskError::InvalidPath);
        }
        let metadata = rustfs_filemeta::get_file_info(
            &input.metadata,
            &input.bucket,
            &input.key,
            &input.version.to_string(),
            rustfs_filemeta::FileInfoOpts {
                data: true,
                include_free_versions: false,
                include_part_checksums: true,
            },
        )?;
        if metadata.version_id != Some(input.version) || metadata.deleted || metadata.mark_deleted {
            return Err(DiskError::InvalidPath);
        }
        let version = input.version;
        metadata
            .validate(rustfs_filemeta::ValidationMode::RequireErasure)
            .map_err(|_| DiskError::InvalidPath)?
            .ok_or(DiskError::InvalidPath)?;
        if !metadata.inline_data()
            || metadata.parts.len() != 1
            || metadata.size < 0
            || metadata.erasure.get_checksum_info(1).algorithm.size() == 0
        {
            return Err(DiskError::UnsupportedDisk);
        }
        let shard = metadata.data.as_ref().ok_or(DiskError::InvalidPath)?;
        let part = &metadata.parts[0];
        if part.number != 1 || part.size != usize::try_from(metadata.size).map_err(|_| DiskError::InvalidPath)? {
            return Err(DiskError::InvalidPath);
        }
        let codec = crate::erasure::coding::Erasure::try_new_with_options(
            metadata.erasure.data_blocks,
            metadata.erasure.parity_blocks,
            metadata.erasure.block_size,
            metadata.uses_legacy_checksum,
        )?;
        let checksum = metadata.erasure.get_checksum_info(1).algorithm;
        let checksum = if metadata.uses_legacy_checksum && checksum == rustfs_utils::HashAlgorithm::HighwayHash256S {
            rustfs_utils::HashAlgorithm::HighwayHash256SLegacy
        } else {
            checksum
        };
        let shard_length = usize::try_from(codec.shard_file_size(i64::try_from(part.size).map_err(|_| DiskError::InvalidPath)?))
            .map_err(|_| DiskError::InvalidPath)?;
        crate::erasure::coding::bitrot_verify(
            std::io::Cursor::new(shard),
            shard.len(),
            shard_length,
            checksum,
            codec.shard_size(),
        )
        .await?;
        let disk = self
            .get_disk_id()
            .await?
            .filter(|id| !id.is_nil())
            .ok_or(DiskError::InvalidPath)?;
        // Match publication: metadata -> object/volume -> private preparation -> sync admission.
        let object_path = self.get_object_path(&input.bucket, &metadata.name)?;
        let metadata_owner =
            crate::disk::os::acquire_metadata_mutation_lease(&object_path, Some(input.bucket_fence.clone())).await;
        let object_owner = crate::disk::os::acquire_rename_data_mutation_lease_with_owner(
            &self.root,
            &input.bucket,
            &object_path,
            Some(metadata_owner),
        )
        .await;
        let prep_path = self
            .root
            .join(".rustfs.sys/native-preparations")
            .join(input.preparation.to_string());
        let owner = crate::disk::os::acquire_namespace_mutation_lease_with_owner(&prep_path, Some(object_owner)).await;
        let admission = crate::disk::os::acquire_file_sync_admission(self.file_sync_permits.clone()).await?;
        let root = self.io_root().to_path_buf();
        #[cfg(unix)]
        return crate::disk::os::run_blocking_namespace_operation(owner, move || {
            let _admission = admission;
            let _operation_owner = operation_owner;
            durable::prepare(&root, disk, version, input, metadata)
        })
        .await
        .map_err(Into::into);
        #[cfg(not(unix))]
        {
            let _ = (owner, admission, root, disk, version, input, metadata, operation_owner);
            Err(DiskError::UnsupportedDisk)
        }
    }
}
#[cfg(unix)]
mod durable {
    use super::*;
    use rustix::fs::{self, AtFlags, Mode, OFlags};
    use serde::{Deserialize, Serialize};
    use sha2::{Digest, Sha256};
    use std::{
        fs::File,
        io::{self, Read, Write},
        path::Path,
    };
    const METADATA_LIMIT: usize = 1024 * 1024;
    const RECORD_LIMIT: usize = 2 * METADATA_LIMIT;
    #[derive(Serialize, Deserialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub(super) struct Record {
        format: u32,
        deployment: Uuid,
        disk_format_digest: [u8; 32],
        object_length: u64,
        data_blocks: usize,
        parity_blocks: usize,
        shard_index: usize,
        preparation: Uuid,
        disk: Uuid,
        pub(super) bucket: String,
        pub(super) incarnation: Uuid,
        pub(super) key: String,
        version: Uuid,
        object_digest: [u8; 32],
        set_digest: [u8; 32],
        distribution_digest: [u8; 32],
        metadata_digest: [u8; 32],
        metadata_length: u64,
        shard_digest: [u8; 32],
        shard_length: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery_metadata: Option<String>,
    }
    impl Record {
        pub(super) fn new(
            input: &NativePrepareInput,
            disk: Uuid,
            info: &FileInfo,
            metadata: &[u8],
            shard: &[u8],
            version: Uuid,
            deployment: Uuid,
            disk_format_digest: [u8; 32],
            set: &[Uuid],
        ) -> io::Result<Self> {
            Ok(Self {
                format: 2,
                deployment,
                disk_format_digest,
                set_digest: Sha256::digest(serde_json::to_vec(set).map_err(invalid)?).into(),
                distribution_digest: Sha256::digest(serde_json::to_vec(&info.erasure.distribution).map_err(invalid)?).into(),
                object_length: u64::try_from(info.size).map_err(invalid)?,
                data_blocks: info.erasure.data_blocks,
                parity_blocks: info.erasure.parity_blocks,
                shard_index: info.erasure.index,
                preparation: input.preparation,
                disk,
                bucket: input.bucket.clone(),
                incarnation: input.bucket_incarnation,
                key: input.key.clone(),
                version,
                object_digest: input.object_digest,
                metadata_digest: Sha256::digest(metadata).into(),
                metadata_length: u64::try_from(metadata.len()).map_err(invalid)?,
                shard_digest: Sha256::digest(shard).into(),
                shard_length: u64::try_from(shard.len()).map_err(invalid)?,
                recovery_metadata: Some(base64_simd::STANDARD.encode_to_string(metadata)),
            })
        }
        fn receipt(&self) -> NativePreparationReceipt {
            NativePreparationReceipt {
                preparation: self.preparation,
                version: self.version,
                disk: self.disk,
                set_digest: self.set_digest,
                distribution_digest: self.distribution_digest,
                metadata_digest: self.metadata_digest,
                shard_digest: self.shard_digest,
                bucket: self.bucket.clone(),
                key: self.key.clone(),
                deployment: self.deployment,
                incarnation: self.incarnation,
                object_digest: self.object_digest,
                object_length: self.object_length,
                data_blocks: self.data_blocks,
                parity_blocks: self.parity_blocks,
                shard_index: self.shard_index,
            }
        }
    }
    fn invalid(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, error)
    }
    struct Link {
        parent: File,
        name: String,
        device: u64,
        inode: u64,
    }
    struct Directory {
        file: File,
        links: Vec<Link>,
        preparation: Uuid,
        root: std::path::PathBuf,
        root_device: u64,
        root_inode: u64,
    }
    impl Directory {
        fn open(root: &Path, preparation: Uuid, create: bool) -> io::Result<Self> {
            let mut file = File::from(fs::open(
                root,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )?);
            let root_identity = fs::fstat(&file)?;
            let mut links = Vec::new();
            for name in [
                ".rustfs.sys".to_owned(),
                "native-preparations".to_owned(),
                preparation.to_string(),
            ] {
                if create {
                    match fs::mkdirat(&file, &name, Mode::RUSR | Mode::WUSR | Mode::XUSR) {
                        Ok(()) => {}
                        Err(rustix::io::Errno::EXIST) => {}
                        Err(e) => return Err(e.into()),
                    }
                    // Also redo this barrier for a directory left by an unknown earlier outcome.
                    sync(&file, preparation, &format!("{name}:parent"))?;
                }
                let child = File::from(fs::openat(
                    &file,
                    &name,
                    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                    Mode::empty(),
                )?);
                let identity = fs::fstat(&child)?;
                if name != ".rustfs.sys" && identity.st_mode & 0o077 != 0 {
                    return Err(invalid("native preparation directory is not private"));
                }
                links.push(Link {
                    parent: file,
                    name,
                    device: u64::try_from(identity.st_dev).map_err(invalid)?,
                    inode: u64::try_from(identity.st_ino).map_err(invalid)?,
                });
                file = child;
            }
            Ok(Self {
                file,
                links,
                preparation,
                root: root.to_path_buf(),
                root_device: u64::try_from(root_identity.st_dev).map_err(invalid)?,
                root_inode: u64::try_from(root_identity.st_ino).map_err(invalid)?,
            })
        }
        fn format_bytes(&self) -> io::Result<Vec<u8>> {
            let sys = self.links.get(1).ok_or_else(|| invalid("missing system directory"))?;
            read_file(&sys.parent, "format.json", METADATA_LIMIT)?.ok_or_else(|| invalid("missing disk format"))
        }
        fn check(&self) -> io::Result<()> {
            let root = fs::stat(&self.root)?;
            if u64::try_from(root.st_dev).map_err(invalid)? != self.root_device
                || u64::try_from(root.st_ino).map_err(invalid)? != self.root_inode
            {
                return Err(invalid("native disk root changed"));
            }
            for link in &self.links {
                let now = fs::statat(&link.parent, &link.name, AtFlags::SYMLINK_NOFOLLOW)?;
                if u64::try_from(now.st_dev).map_err(invalid)? != link.device
                    || u64::try_from(now.st_ino).map_err(invalid)? != link.inode
                {
                    return Err(invalid("native preparation directory changed"));
                }
            }
            Ok(())
        }
        fn read(&self, name: &str, limit: usize) -> io::Result<Option<Vec<u8>>> {
            read_file(&self.file, name, limit)
        }
        fn write(&self, name: &str, bytes: &[u8]) -> io::Result<()> {
            self.check()?;
            let tmp = format!(".stage-{}", Uuid::new_v4());
            let fd = fs::openat(
                &self.file,
                &tmp,
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::RUSR | Mode::WUSR,
            )?;
            let mut file = File::from(fd);
            file.write_all(bytes)?;
            sync(&file, self.preparation, &format!("{name}:file"))?;
            self.check()?;
            let source = fs::fstat(&file)?;
            let staged = fs::statat(&self.file, &tmp, AtFlags::SYMLINK_NOFOLLOW)?;
            if source.st_nlink != 1 || source.st_dev != staged.st_dev || source.st_ino != staged.st_ino {
                return Err(invalid("native staged file changed"));
            }
            fs::renameat(&self.file, &tmp, &self.file, name)?;
            sync(&self.file, self.preparation, &format!("{name}:directory"))?;
            let installed = fs::statat(&self.file, name, AtFlags::SYMLINK_NOFOLLOW)?;
            if source.st_dev != installed.st_dev || source.st_ino != installed.st_ino {
                return Err(invalid("native installed file changed"));
            }
            // No rollback: a barrier error cannot prove this rename did not persist.
            Ok(())
        }
        fn sync_existing(&self, name: &str) -> io::Result<()> {
            let file = File::from(fs::openat(
                &self.file,
                name,
                OFlags::RDONLY | OFlags::NONBLOCK | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )?);
            let stat = fs::fstat(&file)?;
            if stat.st_nlink != 1 || !file.metadata()?.is_file() {
                return Err(invalid("native preparation file has aliases"));
            }
            sync(&file, self.preparation, &format!("{name}:recovery"))?;
            Ok(())
        }
    }
    fn sync(file: &File, preparation: Uuid, stage: &str) -> io::Result<()> {
        #[cfg(test)]
        super::tests::child_stop("before", stage);
        #[cfg(test)]
        super::tests::sync_before(preparation, stage)?;
        file.sync_all()?;
        #[cfg(test)]
        super::tests::sync_after(preparation, stage)?;
        #[cfg(test)]
        super::tests::child_stop("after", stage);
        #[cfg(not(test))]
        let _ = (preparation, stage);
        Ok(())
    }
    fn read_file(parent: &File, name: &str, limit: usize) -> io::Result<Option<Vec<u8>>> {
        let fd = match fs::openat(
            parent,
            name,
            OFlags::RDONLY | OFlags::NONBLOCK | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        ) {
            Ok(fd) => fd,
            Err(rustix::io::Errno::NOENT) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let file = File::from(fd);
        let stat = fs::fstat(&file)?;
        if stat.st_nlink != 1 || !file.metadata()?.is_file() {
            return Err(invalid("native preparation file has aliases"));
        }
        let mut bytes = Vec::new();
        file.take(u64::try_from(limit).map_err(invalid)? + 1)
            .read_to_end(&mut bytes)?;
        let current = fs::statat(parent, name, AtFlags::SYMLINK_NOFOLLOW)?;
        if bytes.len() > limit || stat.st_dev != current.st_dev || stat.st_ino != current.st_ino {
            return Err(invalid("native preparation file changed"));
        }
        Ok(Some(bytes))
    }
    #[cfg(test)]
    #[test]
    fn native_prepare_fifo_is_rejected_without_blocking() {
        for resync in [false, true] {
            let root = tempfile::tempdir().expect("private root");
            let preparation = Uuid::new_v4();
            let dir = Directory::open(root.path(), preparation, true).expect("private directory");
            let path = root
                .path()
                .join(".rustfs.sys/native-preparations")
                .join(preparation.to_string())
                .join("intent.json");
            assert!(
                std::process::Command::new("mkfifo")
                    .arg(&path)
                    .status()
                    .expect("create real FIFO")
                    .success()
            );
            let (send, receive) = std::sync::mpsc::channel();
            let worker = std::thread::spawn(move || {
                let result = if resync {
                    dir.sync_existing("intent.json")
                } else {
                    dir.read("intent.json", RECORD_LIMIT).map(|_| ())
                };
                send.send(result.is_err()).expect("report FIFO rejection");
            });
            let outcome = receive.recv_timeout(std::time::Duration::from_millis(500));
            // Release a blocked reader before asserting, so the RED leaves no live worker.
            let release = File::from(
                fs::open(&path, OFlags::RDWR | OFlags::NONBLOCK | OFlags::CLOEXEC, Mode::empty()).expect("release FIFO reader"),
            );
            worker.join().expect("join FIFO worker");
            drop(release);
            assert_eq!(
                outcome.ok(),
                Some(true),
                "NP01_SPECIAL_FILE_BOUND: disk FIFO must fail before blocking with an owner"
            );
        }
    }
    pub(super) fn probe(
        root: &Path,
        preparation: Uuid,
        disk: Uuid,
        store_id: Uuid,
        bucket: &str,
        key: &str,
        incarnation: Uuid,
    ) -> io::Result<NativePreparationProbe> {
        let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
        let root_fd = File::from(fs::open(root, flags, Mode::empty())?);
        let sys_fd = File::from(fs::openat(&root_fd, ".rustfs.sys", flags, Mode::empty())?);
        let root_identity = fs::fstat(&root_fd)?;
        let sys_identity = fs::fstat(&sys_fd)?;
        let format_bytes = read_file(&sys_fd, "format.json", METADATA_LIMIT)?.ok_or_else(|| invalid("missing disk format"))?;
        let format: crate::layout::format::FormatV3 = serde_json::from_slice(&format_bytes).map_err(invalid)?;
        let set = format
            .erasure
            .sets
            .iter()
            .find(|set| set.contains(&disk))
            .ok_or_else(|| invalid("disk absent from native format"))?;
        if format.id != store_id
            || store_id.is_nil()
            || format.erasure.this != disk
            || format.version != crate::layout::format::FormatMetaVersion::V1
            || format.erasure.version != crate::layout::format::FormatErasureVersion::V3
            || !matches!(
                format.format,
                crate::layout::format::FormatBackend::Erasure | crate::layout::format::FormatBackend::ErasureSingle
            )
            || format.erasure.sets.iter().filter(|set| set.contains(&disk)).count() != 1
            || set.iter().any(Uuid::is_nil)
            || set.iter().enumerate().any(|(i, id)| set[i + 1..].contains(id))
        {
            return Err(invalid("invalid native probe disk format"));
        }
        let result = match Directory::open(root, preparation, false) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => NativePreparationProbe::Absent,
            Err(error) => return Err(error),
            Ok(dir) => {
                let record = inspect(root, preparation)?;
                if record.disk != disk
                    || record.deployment != store_id
                    || record.bucket != bucket
                    || record.key != key
                    || record.incarnation != incarnation
                    || record.disk_format_digest.as_slice() != Sha256::digest(&format_bytes).as_slice()
                    || record.set_digest.as_slice() != Sha256::digest(serde_json::to_vec(set).map_err(invalid)?).as_slice()
                    || record.data_blocks == 0
                    || record.data_blocks.checked_add(record.parity_blocks) != Some(set.len())
                    || record.shard_index == 0
                    || record.shard_index > set.len()
                {
                    return Err(invalid("native probe identity mismatch"));
                }
                let metadata = match dir.read("xl.meta", METADATA_LIMIT)? {
                    Some(metadata) => Some(metadata),
                    None => record
                        .recovery_metadata
                        .as_ref()
                        .map(|encoded| base64_simd::STANDARD.decode_to_vec(encoded).map_err(invalid))
                        .transpose()?,
                };
                if let Some(metadata) = &metadata {
                    if metadata.len() > METADATA_LIMIT
                        || u64::try_from(metadata.len()).map_err(invalid)? != record.metadata_length
                        || Sha256::digest(metadata).as_slice() != record.metadata_digest
                    {
                        return Err(invalid("native probe metadata mismatch"));
                    }
                }
                let prepared = match dir.read("PREPARED", 32)? {
                    None => false,
                    Some(marker)
                        if dir.read("xl.meta", METADATA_LIMIT)?.is_some()
                            && marker.as_slice() == Sha256::digest(serde_json::to_vec(&record).map_err(invalid)?).as_slice() =>
                    {
                        true
                    }
                    Some(_) => return Err(invalid("native probe marker mismatch")),
                };
                dir.check()?;
                if dir.read("intent.json", RECORD_LIMIT)?.as_deref()
                    != Some(serde_json::to_vec(&record).map_err(invalid)?.as_slice())
                {
                    return Err(invalid("native probe intent changed"));
                }
                NativePreparationProbe::Existing {
                    version: record.version,
                    prepared,
                    object_digest: record.object_digest,
                    object_length: record.object_length,
                    set_digest: record.set_digest,
                    distribution_digest: record.distribution_digest,
                    metadata: metadata.map(Into::into),
                }
            }
        };
        // ENOENT is absence only while the actual disk and format remain present.
        let root_now = fs::fstat(File::from(fs::open(root, flags, Mode::empty())?))?;
        let sys_now = fs::statat(&root_fd, ".rustfs.sys", AtFlags::SYMLINK_NOFOLLOW)?;
        if root_now.st_dev != root_identity.st_dev
            || root_now.st_ino != root_identity.st_ino
            || sys_now.st_dev != sys_identity.st_dev
            || sys_now.st_ino != sys_identity.st_ino
            || read_file(&sys_fd, "format.json", METADATA_LIMIT)?.as_deref() != Some(format_bytes.as_slice())
        {
            return Err(invalid("native probe disk changed"));
        }
        Ok(result)
    }
    pub(super) fn inspect(root: &Path, preparation: Uuid) -> io::Result<Record> {
        let dir = Directory::open(root, preparation, false)?;
        let record: Record = serde_json::from_slice(
            &dir.read("intent.json", RECORD_LIMIT)?
                .ok_or_else(|| invalid("missing native intent"))?,
        )
        .map_err(invalid)?;
        if !matches!((record.format, record.recovery_metadata.is_some()), (1, false) | (2, true))
            || record.preparation != preparation
            || record.version.is_nil()
            || record.disk.is_nil()
            || record.incarnation.is_nil()
        {
            return Err(invalid("invalid native intent"));
        }
        dir.check()?;
        Ok(record)
    }
    pub(super) fn recover(
        root: &Path,
        expected: Record,
        disk: Uuid,
        store_id: Uuid,
        fence: Arc<ObjectLockConfigSnapshot>,
        runtime: tokio::runtime::Handle,
    ) -> io::Result<(NativePreparationReceipt, bytes::Bytes)> {
        let record = inspect(root, expected.preparation)?;
        if record != expected || disk != record.disk {
            return Err(invalid("native preparation identity changed"));
        }
        let dir = Directory::open(root, record.preparation, false)?;
        let metadata = dir
            .read("xl.meta", METADATA_LIMIT)?
            .ok_or_else(|| invalid("incomplete native preparation"))?;
        if u64::try_from(metadata.len()).map_err(invalid)? != record.metadata_length
            || Sha256::digest(&metadata).as_slice() != record.metadata_digest
        {
            return Err(invalid("corrupt native metadata"));
        }
        let encoded_metadata = rustfs_filemeta::FileMeta::load(&metadata)?;
        if encoded_metadata.versions.len() != 1 {
            return Err(invalid("native metadata must contain one version"));
        }
        let decoded = rustfs_filemeta::get_file_info(
            &metadata,
            &record.bucket,
            &record.key,
            &record.version.to_string(),
            rustfs_filemeta::FileInfoOpts {
                data: true,
                include_free_versions: false,
                include_part_checksums: true,
            },
        )?;
        decoded
            .validate(rustfs_filemeta::ValidationMode::RequireErasure)?
            .ok_or_else(|| invalid("missing native layout"))?;
        if decoded.erasure.get_checksum_info(1).algorithm.size() == 0
            || decoded.version_id != Some(record.version)
            || decoded.deleted
            || decoded.mark_deleted
            || !decoded.inline_data()
            || decoded.parts.len() != 1
            || decoded.size != i64::try_from(record.object_length).map_err(invalid)?
            || decoded.erasure.data_blocks != record.data_blocks
            || decoded.erasure.parity_blocks != record.parity_blocks
            || decoded.erasure.index != record.shard_index
        {
            return Err(invalid("native metadata identity mismatch"));
        }
        if decoded.parts[0].number != 1 || decoded.parts[0].size != usize::try_from(decoded.size).map_err(invalid)? {
            return Err(invalid("native part identity mismatch"));
        }
        let (deployment, format_digest, set) = verify_disk_layout(&dir, disk, store_id, &decoded)?;
        let set_digest: [u8; 32] = Sha256::digest(serde_json::to_vec(&set).map_err(invalid)?).into();
        let distribution_digest: [u8; 32] =
            Sha256::digest(serde_json::to_vec(&decoded.erasure.distribution).map_err(invalid)?).into();
        if record.deployment != deployment
            || record.disk_format_digest != format_digest
            || record.set_digest != set_digest
            || record.distribution_digest != distribution_digest
        {
            return Err(invalid("native preparation physical layout mismatch"));
        }
        let shard = decoded.data.as_ref().ok_or_else(|| invalid("missing native shard"))?;
        if u64::try_from(shard.len()).map_err(invalid)? != record.shard_length
            || Sha256::digest(shard).as_slice() != record.shard_digest
        {
            return Err(invalid("corrupt native shard"));
        }
        let part = &decoded.parts[0];
        let codec = crate::erasure::coding::Erasure::try_new_with_options(
            decoded.erasure.data_blocks,
            decoded.erasure.parity_blocks,
            decoded.erasure.block_size,
            decoded.uses_legacy_checksum,
        )
        .map_err(|error| error.into_io_error())?;
        let checksum = decoded.erasure.get_checksum_info(1).algorithm;
        let checksum = if decoded.uses_legacy_checksum && checksum == rustfs_utils::HashAlgorithm::HighwayHash256S {
            rustfs_utils::HashAlgorithm::HighwayHash256SLegacy
        } else {
            checksum
        };
        let shard_size = usize::try_from(codec.shard_file_size(i64::try_from(part.size).map_err(invalid)?)).map_err(invalid)?;
        runtime.block_on(crate::erasure::coding::bitrot_verify(
            std::io::Cursor::new(shard),
            shard.len(),
            shard_size,
            checksum,
            codec.shard_size(),
        ))?;
        let marker = dir
            .read("PREPARED", 32)?
            .ok_or_else(|| invalid("native preparation not committed"))?;
        if marker.as_slice() != Sha256::digest(serde_json::to_vec(&record).map_err(invalid)?).as_slice()
            || Sha256::digest(dir.format_bytes()?).as_slice() != record.disk_format_digest
            || !fence.is_valid_for_destructive_put(store_id, &record.bucket, record.incarnation)
        {
            return Err(invalid("native preparation fence mismatch"));
        }
        dir.sync_existing("intent.json")?;
        dir.sync_existing("xl.meta")?;
        dir.sync_existing("PREPARED")?;
        sync(&dir.file, record.preparation, "recovery:directory")?;
        dir.check()?;
        if !fence.is_valid_for_destructive_put(store_id, &record.bucket, record.incarnation)
            || Sha256::digest(dir.format_bytes()?).as_slice() != record.disk_format_digest
        {
            return Err(invalid("native preparation fence changed"));
        }
        if dir.read("xl.meta", METADATA_LIMIT)?.as_deref() != Some(metadata.as_slice())
            || dir.read("intent.json", RECORD_LIMIT)?.as_deref() != Some(serde_json::to_vec(&record).map_err(invalid)?.as_slice())
            || dir.read("PREPARED", 32)?.as_deref()
                != Some(Sha256::digest(serde_json::to_vec(&record).map_err(invalid)?).as_slice())
        {
            return Err(invalid("native preparation files changed before receipt"));
        }
        Ok((record.receipt(), metadata.into()))
    }
    fn verify_disk_layout(
        dir: &Directory,
        disk: Uuid,
        store_id: Uuid,
        info: &FileInfo,
    ) -> io::Result<(Uuid, [u8; 32], Vec<Uuid>)> {
        let format_bytes = dir.format_bytes()?;
        let format: crate::layout::format::FormatV3 = serde_json::from_slice(&format_bytes).map_err(invalid)?;
        let set = format
            .erasure
            .sets
            .iter()
            .find(|set| set.contains(&disk))
            .ok_or_else(|| invalid("disk absent from native format"))?;
        if format.id != store_id
            || format.id.is_nil()
            || format.version != crate::layout::format::FormatMetaVersion::V1
            || format.erasure.version != crate::layout::format::FormatErasureVersion::V3
            || format.erasure.this != disk
            || !matches!(
                format.format,
                crate::layout::format::FormatBackend::Erasure | crate::layout::format::FormatBackend::ErasureSingle
            )
            || set.len()
                != info
                    .erasure
                    .data_blocks
                    .checked_add(info.erasure.parity_blocks)
                    .ok_or_else(|| invalid("native geometry overflow"))?
        {
            return Err(invalid("native preparation disk format mismatch"));
        }
        if format.erasure.sets.iter().filter(|set| set.contains(&disk)).count() != 1
            || set.iter().any(Uuid::is_nil)
            || set.iter().enumerate().any(|(index, id)| set[index + 1..].contains(id))
        {
            return Err(invalid("ambiguous native disk membership"));
        }
        let disk_slot = set
            .iter()
            .position(|id| *id == disk)
            .ok_or_else(|| invalid("native disk slot missing"))?;
        if info.erasure.distribution.get(disk_slot).copied() != Some(info.erasure.index) {
            return Err(invalid("native shard assigned to another disk"));
        }
        Ok((format.id, Sha256::digest(&format_bytes).into(), set.clone()))
    }
    pub(super) fn prepare(
        root: &Path,
        disk: Uuid,
        version: Uuid,
        input: NativePrepareInput,
        info: FileInfo,
    ) -> io::Result<NativePreparationReceipt> {
        let metadata = &input.metadata;
        let shard = info.data.as_ref().ok_or_else(|| invalid("missing native shard"))?;

        if metadata.len() > METADATA_LIMIT {
            return Err(invalid("native preparation metadata too large"));
        }
        let dir = Directory::open(root, input.preparation, true)?;
        let (deployment, format_digest, set) = verify_disk_layout(&dir, disk, input.store_id, &info)?;
        let mut record = Record::new(&input, disk, &info, metadata, shard, version, deployment, format_digest, &set)?;
        let existing = dir.read("intent.json", RECORD_LIMIT)?;
        if let Some(bytes) = &existing {
            let saved: Record = serde_json::from_slice(bytes).map_err(invalid)?;
            // Preserve the canonical bytes and marker of an existing v1 intent.
            if saved.format == 1 && saved.recovery_metadata.is_none() {
                record.format = 1;
                record.recovery_metadata = None;
            }
        }
        let encoded = serde_json::to_vec(&record).map_err(invalid)?;
        if encoded.len() > RECORD_LIMIT {
            return Err(invalid("native preparation identity too large"));
        }
        match existing {
            Some(bytes) => {
                let saved: Record = serde_json::from_slice(&bytes).map_err(invalid)?;
                if saved != record {
                    return Err(invalid("native preparation identity conflict"));
                }
                dir.sync_existing("intent.json")?;
                sync(&dir.file, record.preparation, "intent.json:directory")?;
            }
            None => dir.write("intent.json", &encoded)?,
        }
        let ready = dir.read("PREPARED", 32)?;
        if let Some(marker) = ready {
            if marker.as_slice() != Sha256::digest(&encoded).as_slice() {
                return Err(invalid("native preparation marker mismatch"));
            }
            let saved = dir
                .read("xl.meta", METADATA_LIMIT)?
                .ok_or_else(|| invalid("native preparation metadata missing"))?;
            if saved.as_slice() != metadata.as_ref() {
                return Err(invalid("native preparation metadata changed"));
            }
            // Recovery re-establishes all barriers; an earlier error may have been commit-ambiguous.
            dir.sync_existing("intent.json")?;
            dir.sync_existing("xl.meta")?;
            dir.sync_existing("PREPARED")?;
            sync(&dir.file, record.preparation, "recovery:directory")?;
        } else {
            dir.write("xl.meta", &metadata)?;
            if !input
                .bucket_fence
                .is_valid_for_destructive_put(input.store_id, &record.bucket, record.incarnation)
            {
                return Err(invalid("native preparation bucket fence lost"));
            }
            dir.check()?;
            dir.write("PREPARED", &Sha256::digest(&encoded))?;
        }
        if !input
            .bucket_fence
            .is_valid_for_destructive_put(input.store_id, &record.bucket, record.incarnation)
        {
            return Err(invalid("native preparation bucket fence lost"));
        }
        dir.check()?;
        if Sha256::digest(dir.format_bytes()?).as_slice() != record.disk_format_digest {
            return Err(invalid("native preparation format changed"));
        }
        let saved = dir
            .read("xl.meta", METADATA_LIMIT)?
            .ok_or_else(|| invalid("missing native metadata"))?;
        if saved.as_slice() != metadata.as_ref() {
            return Err(invalid("native preparation metadata changed"));
        }
        if dir.read("xl.meta", METADATA_LIMIT)?.as_deref() != Some(metadata.as_ref())
            || dir.read("intent.json", RECORD_LIMIT)?.as_deref() != Some(serde_json::to_vec(&record).map_err(invalid)?.as_slice())
            || dir.read("PREPARED", 32)?.as_deref()
                != Some(Sha256::digest(serde_json::to_vec(&record).map_err(invalid)?).as_slice())
        {
            return Err(invalid("native preparation files changed before receipt"));
        }
        Ok(record.receipt())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::{
        bucket::metadata_sys::{init_bucket_metadata_sys, test_support::isolated_store_over_temp_disks},
        disk::endpoint::Endpoint,
        storage_api_contracts::bucket::{BucketOperations as _, MakeBucketOptions},
    };
    use bytes::Bytes;
    use rustfs_filemeta::ObjectPartInfo;
    use serial_test::serial;
    use sha2::{Digest, Sha256};
    #[derive(Default)]
    struct SyncProbe {
        seen: Vec<String>,
        fail_before: Option<String>,
        fail_after: Option<String>,
        pause: Option<(String, std::sync::mpsc::SyncSender<()>, std::sync::mpsc::Receiver<()>)>,
    }
    static SYNC_PROBES: std::sync::LazyLock<std::sync::Mutex<std::collections::HashMap<Uuid, SyncProbe>>> =
        std::sync::LazyLock::new(Default::default);
    pub(crate) fn pause_native_preparation(
        preparation: Uuid,
        stage: &str,
    ) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::SyncSender<()>) {
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        SYNC_PROBES.lock().expect("probe mutex").insert(
            preparation,
            SyncProbe {
                pause: Some((stage.to_owned(), entered_tx, release_rx)),
                ..Default::default()
            },
        );
        (entered_rx, release_tx)
    }
    pub(crate) fn clear_native_preparation_probe(preparation: Uuid) {
        SYNC_PROBES.lock().expect("probe mutex").remove(&preparation);
    }
    pub(super) fn sync_before(id: Uuid, stage: &str) -> std::io::Result<()> {
        let pause = {
            let mut probes = SYNC_PROBES.lock().expect("probe mutex");
            if let Some(p) = probes.get_mut(&id) {
                p.seen.push(stage.into());
                if p.fail_before.as_deref() == Some(stage) {
                    p.fail_before = None;
                    return Err(std::io::Error::other("injected before native fsync"));
                }
                if p.pause.as_ref().is_some_and(|(name, _, _)| name == stage) {
                    p.pause.take()
                } else {
                    None
                }
            } else {
                None
            }
        };
        if let Some((_, entered, release)) = pause {
            entered.send(()).expect("notify real syscall boundary");
            release.recv().expect("release real syscall boundary");
        }
        Ok(())
    }
    pub(super) fn sync_after(id: Uuid, stage: &str) -> std::io::Result<()> {
        let mut probes = SYNC_PROBES.lock().expect("probe mutex");
        if let Some(p) = probes.get_mut(&id) {
            if p.fail_after.as_deref() == Some(stage) {
                p.fail_after = None;
                return Err(std::io::Error::other("injected unknown native fsync outcome"));
            }
        }
        Ok(())
    }
    pub(super) fn child_stop(phase: &str, stage: &str) {
        if std::env::var("NP01_CHILD_STOP").ok().as_deref() == Some(&format!("{phase}:{stage}")) {
            let signal = std::env::var_os("NP01_CHILD_SIGNAL").expect("child signal path");
            std::fs::write(signal, b"actual native I/O boundary").expect("notify parent");
            loop {
                std::thread::park();
            }
        }
    }
    #[derive(serde::Serialize, serde::Deserialize)]
    struct RestartInput {
        roots: Vec<std::path::PathBuf>,
        preparation: Uuid,
        bucket: String,
        incarnation: Uuid,
        digest: [u8; 32],
        version: Uuid,
        metadata: Vec<u8>,
    }
    #[tokio::test]
    async fn native_prepare_child_process() {
        let Some(path) = std::env::var_os("NP01_CHILD_INPUT") else {
            return;
        };
        let saved: RestartInput = serde_json::from_slice(&std::fs::read(path).expect("child input")).expect("decode child input");
        use crate::{
            layout::endpoints::{EndpointServerPools, Endpoints, PoolEndpoints},
            runtime::instance::InstanceContext,
            store::{ECStore, init_local_disks_with_instance_ctx},
        };
        let endpoints = saved
            .roots
            .iter()
            .enumerate()
            .map(|(index, path)| {
                let mut endpoint = Endpoint::try_from(path.to_str().expect("path")).expect("endpoint");
                endpoint.set_pool_index(0);
                endpoint.set_set_index(0);
                endpoint.set_disk_index(index);
                endpoint
            })
            .collect::<Vec<_>>();
        let pools = EndpointServerPools(vec![PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 4,
            endpoints: Endpoints::from(endpoints.clone()),
            cmd_line: "native-preparation-restart".into(),
            platform: "test".into(),
        }]);
        let context = Arc::new(InstanceContext::new());
        init_local_disks_with_instance_ctx(&context, pools.clone())
            .await
            .expect("existing local disks");
        let store = ECStore::new_with_instance_ctx(
            "127.0.0.1:0".parse().expect("address"),
            pools,
            tokio_util::sync::CancellationToken::new(),
            context,
        )
        .await
        .expect("fresh store context");
        init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let fence = store
            .object_lock_config_snapshot(&saved.bucket)
            .await
            .expect("fresh lifecycle owner");
        let disk = LocalDisk::new(&endpoints[0], false).await.expect("child actual disk");
        disk.prepare_native(NativePrepareInput {
            preparation: saved.preparation,
            store_id: store.id,
            bucket: saved.bucket,
            bucket_incarnation: saved.incarnation,
            object_digest: saved.digest,
            metadata: Bytes::from(saved.metadata),
            key: "key".into(),
            version: saved.version,
            bucket_fence: fence,
        })
        .await
        .expect("child native preparation");
        panic!("child must stop at requested real I/O boundary");
    }
    #[tokio::test]
    #[serial]
    async fn native_preparation_probe_preserves_partial_and_unknown_states() {
        async fn probe(disk: &LocalDisk, input: &NativePrepareInput) -> Result<NativePreparationProbe> {
            disk.probe_native(
                input.preparation,
                input.store_id,
                &input.bucket,
                &input.key,
                input.bucket_incarnation,
                input.bucket_fence.clone(),
            )
            .await
        }
        let (dirs, _store, disk, input) = fixture().await;
        assert!(
            matches!(probe(&disk, &input).await, Ok(NativePreparationProbe::Absent)),
            "NP02_PROBE_ABSENCE_BOUND: only a healthy disk can establish absence"
        );
        disk.prepare_native(input.clone()).await.expect("persist real preparation");
        let (_, metadata) = disk
            .recover_native_with_metadata(
                input.preparation,
                input.store_id,
                &input.bucket,
                input.bucket_incarnation,
                input.bucket_fence.clone(),
            )
            .await
            .expect("recover exact checked bytes");
        assert_eq!(metadata, input.metadata, "NP02_RAW_METADATA_BOUND");
        assert!(
            matches!(
                probe(&disk, &input).await,
                Ok(NativePreparationProbe::Existing {
                    prepared: true,
                    metadata: Some(_),
                    ..
                })
            ),
            "NP02_PROBE_PREPARED_BOUND"
        );
        let dir = dirs[0]
            .path()
            .join(".rustfs.sys/native-preparations")
            .join(input.preparation.to_string());
        std::fs::write(dir.join("PREPARED"), b"corrupt").expect("corrupt own marker");
        assert!(probe(&disk, &input).await.is_err(), "NP02_PROBE_UNKNOWN_BOUND: corruption is not absence");
        std::fs::remove_file(dir.join("PREPARED")).expect("retain incomplete intent");
        assert!(
            matches!(
                probe(&disk, &input).await,
                Ok(NativePreparationProbe::Existing {
                    prepared: false,
                    metadata: Some(_),
                    ..
                })
            ),
            "NP02_PROBE_PARTIAL_BOUND: uncommitted metadata pins routing"
        );
        std::fs::remove_file(dir.join("xl.meta")).expect("retain intent before metadata");
        assert!(
            matches!(
                probe(&disk, &input).await,
                Ok(NativePreparationProbe::Existing {
                    prepared: false,
                    metadata: Some(_),
                    ..
                })
            ),
            "NP02_PROBE_PARTIAL_BOUND: intent alone is not absence"
        );
        let recovered_metadata = match probe(&disk, &input).await.expect("inspect incomplete intent") {
            NativePreparationProbe::Existing {
                metadata: Some(bytes), ..
            } => bytes,
            _ => panic!("NP02_RAW_METADATA_BOUND: intent must retain original metadata"),
        };
        assert_eq!(recovered_metadata, input.metadata, "NP02_RAW_METADATA_BOUND");
        let mut retry = input.clone();
        retry.metadata = recovered_metadata;
        disk.prepare_native(retry).await.expect("repair intent-only preparation");
        let mut legacy: serde_json::Value =
            serde_json::from_slice(&std::fs::read(dir.join("intent.json")).expect("read complete intent"))
                .expect("decode intent");
        legacy["format"] = serde_json::json!(1);
        legacy.as_object_mut().expect("intent object").remove("recovery_metadata");
        let legacy: durable::Record = serde_json::from_value(legacy).expect("decode old intent format");
        let encoded = serde_json::to_vec(&legacy).expect("canonical old intent");
        std::fs::write(dir.join("intent.json"), &encoded).expect("install old intent");
        std::fs::write(dir.join("PREPARED"), Sha256::digest(&encoded)).expect("install old marker");
        disk.prepare_native(input.clone())
            .await
            .expect("retry existing v1 preparation");
        assert_eq!(
            std::fs::read(dir.join("intent.json")).expect("retained old intent"),
            encoded,
            "NP02_INTENT_COMPAT_BOUND: retry must preserve existing canonical bytes"
        );
        disk.recover_native(
            input.preparation,
            input.store_id,
            &input.bucket,
            input.bucket_incarnation,
            input.bucket_fence.clone(),
        )
        .await
        .expect("recover old intent");
        let format = dirs[0].path().join(".rustfs.sys/format.json");
        let saved = std::fs::read(&format).expect("save actual format");
        std::fs::remove_file(&format).expect("make actual disk identity unavailable");
        let result = probe(&disk, &input).await;
        std::fs::write(&format, saved).expect("restore fixture format");
        assert!(result.is_err(), "NP02_PROBE_UNKNOWN_BOUND: missing format is not absence");
    }
    async fn fixture() -> (Vec<tempfile::TempDir>, Arc<crate::store::ECStore>, LocalDisk, NativePrepareInput) {
        let (dirs, store) = isolated_store_over_temp_disks().await;
        init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let bucket = "native-prepare";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("real bucket");
        let incarnation = store.bucket_incarnation_id(bucket).await.expect("durable incarnation");
        let fence = store.object_lock_config_snapshot(bucket).await.expect("actual bucket fence");
        let mut endpoint = Endpoint::try_from(dirs[0].path().to_str().expect("fixture path")).expect("endpoint");
        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(0);
        let disk = LocalDisk::new(&endpoint, false).await.expect("real LocalDisk");
        let data = Bytes::from_static(b"durable shard!!!");
        let mut metadata = FileInfo::new("key", 2, 2);
        metadata.name = "key".into();
        metadata.erasure.index = metadata.erasure.distribution[0];
        metadata.version_id = Some(Uuid::new_v4());
        metadata.size = i64::try_from(data.len()).expect("small fixture");
        metadata.mod_time = Some(time::OffsetDateTime::now_utc());
        metadata.parts = vec![ObjectPartInfo {
            number: 1,
            size: data.len(),
            actual_size: metadata.size,
            ..Default::default()
        }];
        let shard = crate::erasure::coding::Erasure::try_new(2, 2, metadata.erasure.block_size)
            .expect("real erasure layout")
            .encode_data(&data)
            .expect("actual erasure shard")[metadata.erasure.index - 1]
            .clone();
        let mut framed = Vec::new();
        crate::erasure::coding::BitrotWriter::new(
            &mut framed,
            metadata.erasure.shard_size(),
            rustfs_utils::HashAlgorithm::HighwayHash256S,
        )
        .write(&shard)
        .await
        .expect("actual bitrot writer");
        metadata.data = Some(Bytes::from(framed));
        metadata.set_inline_data();
        let version = metadata.version_id.expect("fixture exact version");
        let mut meta = rustfs_filemeta::FileMeta::new();
        meta.add_version(metadata).expect("real writer version");
        let metadata = Bytes::from(meta.marshal_msg().expect("original writer xl.meta"));
        let input = NativePrepareInput {
            preparation: Uuid::new_v4(),
            store_id: store.id,
            bucket: bucket.into(),
            bucket_incarnation: incarnation,
            object_digest: Sha256::digest(&data).into(),
            metadata,
            key: "key".into(),
            version,
            bucket_fence: fence,
        };
        (dirs, store, disk, input)
    }
    #[tokio::test]
    #[serial]
    async fn native_prepare_sync_failure_has_no_receipt() {
        let (dirs, store, disk, input) = fixture().await;
        let preparation = input.preparation;
        SYNC_PROBES.lock().expect("probe").insert(
            preparation,
            SyncProbe {
                fail_before: Some("PREPARED:file".into()),
                ..Default::default()
            },
        );
        assert!(
            disk.prepare_native(input.clone()).await.is_err(),
            "NP01_DURABLE_ORDER_BOUND: failure before marker sync returns no receipt"
        );
        let private = dirs[0]
            .path()
            .join(".rustfs.sys/native-preparations")
            .join(preparation.to_string());
        assert!(!private.join("PREPARED").exists());
        assert_eq!(
            SYNC_PROBES
                .lock()
                .expect("probe")
                .get(&preparation)
                .expect("first operation")
                .seen,
            vec![
                ".rustfs.sys:parent".into(),
                "native-preparations:parent".into(),
                format!("{preparation}:parent"),
                "intent.json:file".into(),
                "intent.json:directory".into(),
                "xl.meta:file".into(),
                "xl.meta:directory".into(),
                "PREPARED:file".into()
            ],
            "NP01_DURABLE_ORDER_BOUND: parent and intent barriers precede payload and commit"
        );
        assert!(
            disk.recover_native(preparation, store.id, &input.bucket, input.bucket_incarnation, input.bucket_fence.clone())
                .await
                .is_err()
        );
        SYNC_PROBES.lock().expect("probe").insert(
            preparation,
            SyncProbe {
                fail_after: Some("PREPARED:directory".into()),
                ..Default::default()
            },
        );
        assert!(
            disk.prepare_native(input.clone()).await.is_err(),
            "unknown outcome must not yield receipt"
        );
        assert!(private.join("PREPARED").is_file(), "failed fsync response does not prove absence");
        let stages = SYNC_PROBES
            .lock()
            .expect("probe")
            .remove(&preparation)
            .expect("recorded actual I/O boundaries")
            .seen;
        assert_eq!(
            &stages[stages.len() - 4..],
            &["xl.meta:file", "xl.meta:directory", "PREPARED:file", "PREPARED:directory"],
            "NP01_DURABLE_ORDER_BOUND"
        );
        let recovered = disk
            .recover_native(preparation, store.id, &input.bucket, input.bucket_incarnation, input.bucket_fence.clone())
            .await
            .expect("validated unknown outcome recovery");
        assert_eq!(recovered.version_id(), input.version);
        assert_eq!(
            disk.prepare_native(input.clone())
                .await
                .expect("idempotent retry")
                .version_id(),
            recovered.version_id()
        );
        let original = rustfs_filemeta::get_file_info(
            &input.metadata,
            &input.bucket,
            &input.key,
            &input.version.to_string(),
            rustfs_filemeta::FileInfoOpts {
                data: true,
                include_free_versions: false,
                include_part_checksums: true,
            },
        )
        .expect("decode original writer bytes");
        let shards = crate::erasure::coding::Erasure::try_new(2, 2, original.erasure.block_size)
            .expect("actual codec")
            .encode_data(b"durable shard!!!")
            .expect("actual 2+2 shards");
        let mut disk_ids = std::collections::HashSet::from([recovered.disk_id()]);
        for (index, dir) in dirs.iter().enumerate().take(3).skip(1) {
            let mut metadata = original.clone();
            metadata.erasure.index = metadata.erasure.distribution[index];
            let mut framed = Vec::new();
            crate::erasure::coding::BitrotWriter::new(
                &mut framed,
                metadata.erasure.shard_size(),
                rustfs_utils::HashAlgorithm::HighwayHash256S,
            )
            .write(&shards[metadata.erasure.index - 1])
            .await
            .expect("actual shard framing");
            metadata.data = Some(Bytes::from(framed));
            let mut meta = rustfs_filemeta::FileMeta::new();
            meta.add_version(metadata).expect("per-disk writer metadata");
            let mut target = input.clone();
            target.metadata = Bytes::from(meta.marshal_msg().expect("fixed per-disk xl.meta"));
            let mut endpoint = Endpoint::try_from(dir.path().to_str().expect("path")).expect("endpoint");
            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(index);
            let target_disk = LocalDisk::new(&endpoint, false).await.expect("another physical local disk");
            let receipt = target_disk.prepare_native(target).await.expect("durable other native shard");
            assert!(disk_ids.insert(receipt.disk_id()), "physical receipts must name distinct disks");
            assert_eq!(
                (
                    receipt.preparation_id(),
                    receipt.version_id(),
                    receipt.object_digest(),
                    receipt.object_length(),
                    receipt.set_digest(),
                    receipt.distribution_digest()
                ),
                (
                    recovered.preparation_id(),
                    recovered.version_id(),
                    recovered.object_digest(),
                    recovered.object_length(),
                    recovered.set_digest(),
                    recovered.distribution_digest()
                ),
                "NP01_DURABLE_ORDER_BOUND: receipts from real Wdata=3 disks compose on the same native object and physical set"
            );
        }
    }
    #[tokio::test]
    #[serial]
    async fn native_prepare_restart_recovers_exact_identity() {
        let (dirs, store, disk, input) = fixture().await;
        let version = input.version;
        for phase in ["before:PREPARED:file", "after:PREPARED:directory"] {
            let mut child_input = input.clone();
            child_input.preparation = Uuid::new_v4();
            let request = RestartInput {
                roots: dirs.iter().map(|dir| dir.path().to_path_buf()).collect(),
                preparation: child_input.preparation,
                bucket: child_input.bucket.clone(),
                incarnation: child_input.bucket_incarnation,
                digest: child_input.object_digest,
                version,
                metadata: child_input.metadata.to_vec(),
            };
            let taskdir = tempfile::tempdir().expect("child request directory");
            let request_path = taskdir.path().join("input.json");
            let signal = taskdir.path().join("signal");
            std::fs::write(&request_path, serde_json::to_vec(&request).expect("encode fixture")).expect("write fixture");
            let log = std::fs::File::create(taskdir.path().join("child.log")).expect("child log");
            let mut child = std::process::Command::new(std::env::current_exe().expect("test binary"))
                .args([
                    "--exact",
                    "disk::local::native_prepare::tests::native_prepare_child_process",
                    "--test-threads=1",
                ])
                .env("NP01_CHILD_INPUT", request_path)
                .env("NP01_CHILD_SIGNAL", &signal)
                .env("NP01_CHILD_STOP", phase)
                .stdout(log.try_clone().expect("clone log"))
                .stderr(log)
                .spawn()
                .expect("real child process");
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
            while !signal.is_file() {
                if let Some(status) = child.try_wait().expect("child status") {
                    panic!(
                        "child exited {status}: {}",
                        std::fs::read_to_string(taskdir.path().join("child.log")).expect("child log")
                    );
                }
                if std::time::Instant::now() > deadline {
                    child.kill().expect("stop timed out child");
                    child.wait().expect("reap child");
                    panic!("child failed to reach actual I/O boundary");
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            child.kill().expect("kill at actual I/O boundary");
            child.wait().expect("reap killed writer");
            let fresh = store
                .object_lock_config_snapshot(&input.bucket)
                .await
                .expect("fresh recovery fence");
            let recovered = disk
                .recover_native(child_input.preparation, store.id, &input.bucket, input.bucket_incarnation, fresh)
                .await;
            assert_eq!(
                recovered.is_ok(),
                phase.starts_with("after:"),
                "NP01_RECOVERY_IDENTITY_BOUND: process kill is not a power-failure simulation"
            );
            if recovered.is_err() {
                disk.prepare_native(child_input)
                    .await
                    .expect("resume incomplete durable intent with same identity");
            }
        }
        let receipt = disk.prepare_native(input.clone()).await.expect("prepared real native shard");
        LocalDisk::cleanup_stale_tmp_objects_with_expiry(
            disk.io_root().to_path_buf(),
            &disk.publication_root,
            std::time::Duration::ZERO,
        )
        .await
        .expect("run actual ordinary temp cleanup");
        drop(disk);
        let mut endpoint = Endpoint::try_from(dirs[0].path().to_str().expect("path")).expect("endpoint");
        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(0);
        let reopened = LocalDisk::new(&endpoint, false).await.expect("reopened actual disk");
        let result = reopened
            .recover_native(
                input.preparation,
                store.id,
                &input.bucket,
                input.bucket_incarnation,
                input.bucket_fence.clone(),
            )
            .await
            .expect("read and validate durable intent after reopen");
        assert_eq!(result.version_id(), version, "NP01_RECOVERY_IDENTITY_BOUND");
        assert_eq!(result.shard_digest(), receipt.shard_digest());
        let prepared = dirs[0]
            .path()
            .join(".rustfs.sys/native-preparations")
            .join(input.preparation.to_string());
        let original_intent = std::fs::read(prepared.join("intent.json")).expect("original durable intent");
        let original_marker = std::fs::read(prepared.join("PREPARED")).expect("original commit marker");
        std::fs::remove_file(prepared.join("PREPARED")).expect("replace marker with special file");
        assert!(
            std::process::Command::new("mkfifo")
                .arg(prepared.join("PREPARED"))
                .status()
                .expect("real marker FIFO")
                .success()
        );
        let bounded = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            reopened.recover_native(
                input.preparation,
                store.id,
                &input.bucket,
                input.bucket_incarnation,
                input.bucket_fence.clone(),
            ),
        )
        .await;
        // O_RDWR releases a broken blocking implementation before the bounded assertion.
        let release = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(prepared.join("PREPARED"))
            .expect("release marker FIFO");
        assert_eq!(
            matches!(bounded, Ok(Err(_))),
            true,
            "NP01_SPECIAL_FILE_BOUND: recover must reject FIFO while retaining real object owners"
        );
        drop(release);
        std::fs::remove_file(prepared.join("PREPARED")).expect("remove FIFO marker");
        std::fs::write(prepared.join("PREPARED"), &original_marker).expect("restore regular marker");
        for field in ["set_digest", "distribution_digest"] {
            let mut value: serde_json::Value = serde_json::from_slice(&original_intent).expect("intent JSON");
            let digest = value[field].as_array_mut().expect("layout digest");
            digest[0] = serde_json::json!(digest[0].as_u64().expect("byte") ^ 1);
            let record: durable::Record = serde_json::from_value(value).expect("syntactically valid forged layout");
            let encoded = serde_json::to_vec(&record).expect("canonical forged record");
            std::fs::write(prepared.join("intent.json"), &encoded).expect("real persistent layout corruption");
            std::fs::write(prepared.join("PREPARED"), Sha256::digest(&encoded)).expect("coherent untrusted marker");
            assert_eq!(
                reopened
                    .recover_native(
                        input.preparation,
                        store.id,
                        &input.bucket,
                        input.bucket_incarnation,
                        input.bucket_fence.clone()
                    )
                    .await
                    .is_err(),
                true,
                "NP01_RECOVERY_IDENTITY_BOUND: coherent marker cannot attest forged physical layout"
            );
            std::fs::write(prepared.join("intent.json"), &original_intent).expect("restore own fixture");
            std::fs::write(prepared.join("PREPARED"), &original_marker).expect("restore fixture marker");
        }

        let meta = dirs[0]
            .path()
            .join(".rustfs.sys/native-preparations")
            .join(input.preparation.to_string())
            .join("xl.meta");
        std::fs::write(&meta, b"tampered metadata").expect("real disk corruption");
        assert!(
            reopened
                .recover_native(
                    input.preparation,
                    store.id,
                    &input.bucket,
                    input.bucket_incarnation,
                    input.bucket_fence.clone()
                )
                .await
                .is_err(),
            "NP01_RECOVERY_IDENTITY_BOUND: corruption cannot produce receipt"
        );
        assert!(
            reopened.prepare_native(input).await.is_err(),
            "committed corruption must not be silently repaired"
        );
    }
    #[tokio::test]
    #[serial]
    async fn native_prepare_incarnation_and_namespace_fenced() {
        let (dirs, _store, disk, mut input) = fixture().await;
        let actual_incarnation = input.bucket_incarnation;
        input.bucket_incarnation = Uuid::new_v4();
        let result = disk.prepare_native(input.clone()).await;
        assert_eq!(
            result.err(),
            Some(DiskError::InvalidPath),
            "NP01_NAMESPACE_OWNER_BOUND: unrelated bucket incarnation is rejected"
        );
        input.bucket_incarnation = actual_incarnation;
        let private = dirs[0].path().join(".rustfs.sys/native-preparations");
        std::fs::create_dir_all(&private).expect("private fixture directory");
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&private, std::fs::Permissions::from_mode(0o700)).expect("actual private directory mode");
        let outside = tempfile::tempdir().expect("outside directory");
        std::os::unix::fs::symlink(outside.path(), private.join(input.preparation.to_string())).expect("symlink attack");
        assert!(
            disk.prepare_native(input.clone()).await.is_err(),
            "NP01_NAMESPACE_OWNER_BOUND: symlink cannot redirect private data"
        );
        assert_eq!(std::fs::read_dir(outside.path()).expect("outside entries").count(), 0);
        std::fs::remove_file(private.join(input.preparation.to_string())).expect("remove own malicious link");
        let preparation = input.preparation;
        let object_path = disk.get_object_path(&input.bucket, &input.key).expect("object path");
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        SYNC_PROBES.lock().expect("probe").insert(
            preparation,
            SyncProbe {
                pause: Some(("PREPARED:file".into(), entered_tx, release_rx)),
                ..Default::default()
            },
        );
        let disk = Arc::new(disk);
        let writer_disk = disk.clone();
        let canceled_input = input.clone();
        let task = tokio::spawn(async move { writer_disk.prepare_native(canceled_input).await });
        tokio::task::spawn_blocking(move || {
            entered_rx
                .recv_timeout(std::time::Duration::from_secs(30))
                .expect("blocking I/O reached")
        })
        .await
        .expect("join observer");
        task.abort();
        assert!(matches!(task.await, Err(error) if error.is_cancelled()));
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(50),
                crate::disk::os::acquire_metadata_mutation_lease(&object_path, None)
            )
            .await
            .is_err(),
            "NP01_NAMESPACE_OWNER_BOUND: cancellation must retain actual metadata owner until blocking syscall finishes"
        );
        release_tx.send(()).expect("release I/O");
        let owner = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            crate::disk::os::acquire_metadata_mutation_lease(&object_path, None),
        )
        .await
        .expect("owner released after syscall");
        drop(owner);
        SYNC_PROBES.lock().expect("probe").remove(&preparation);
        let mut replaced = input;
        replaced.preparation = Uuid::new_v4();
        let id = replaced.preparation;
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        SYNC_PROBES.lock().expect("probe").insert(
            id,
            SyncProbe {
                pause: Some(("PREPARED:file".into(), entered_tx, release_rx)),
                ..Default::default()
            },
        );
        let writer_disk = disk.clone();
        let writer = tokio::spawn(async move { writer_disk.prepare_native(replaced).await });
        tokio::task::spawn_blocking(move || {
            entered_rx
                .recv_timeout(std::time::Duration::from_secs(30))
                .expect("directory swap I/O boundary")
        })
        .await
        .expect("observer");
        let path = private.join(id.to_string());
        std::fs::rename(&path, private.join(format!("{id}.detached")))
            .expect("replace private directory while actual write is active");
        std::fs::create_dir(&path).expect("new directory at same spelling");
        release_tx.send(()).expect("resume real filesystem operation");
        assert!(
            writer.await.expect("writer joins").is_err(),
            "NP01_NAMESPACE_OWNER_BOUND: substituted directory must not produce receipt"
        );
        assert!(!path.join("PREPARED").exists());
        SYNC_PROBES.lock().expect("probe").remove(&id);
    }
}
