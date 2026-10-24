// Copyright 2026 RustFS Team
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

//! Per-attempt conditions for the fixed pool.bin object. These conditions are
//! transport metadata, never part of the persisted object identity.

use super::*;
use crate::disk::{
    PoolMetaNativeBackup, PoolMetaNativeInspection, PoolMetaNativeRecoveryRequest, PoolMetaNativeRecoveryResponse,
};
use rustfs_utils::http::metadata_compat::{contains_key_str, get_consistent_str, insert_str, remove_str};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const OBJECT: &str = "pool.bin";
const CONDITION_SUFFIX: &str = "pool-meta-native-cas";
const MAX_CONDITION_BYTES: usize = 4096;
const MAX_BACKUPS: usize = 4096;
const MAX_INVENTORY_ENTRIES: usize = MAX_BACKUPS * 2 + 8;
const MAX_INVENTORY_BYTES: usize = 64 * 1024 * 1024;

/// Exact identity of a physical pool metadata write.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolMetaNativeFileIdentity {
    pub mod_time_unix_nanos: i128,
    pub etag: String,
    pub data_dir: Option<Uuid>,
}

impl PoolMetaNativeFileIdentity {
    pub fn from_file_info(fi: &FileInfo) -> Result<Self> {
        let identity = Self {
            mod_time_unix_nanos: fi.mod_time.ok_or(DiskError::FileCorrupt)?.unix_timestamp_nanos(),
            etag: fi.get_etag().ok_or(DiskError::FileCorrupt)?,
            data_dir: fi.data_dir,
        };
        identity.validate()?;
        Ok(identity)
    }

    fn validate(&self) -> Result<()> {
        OffsetDateTime::from_unix_timestamp_nanos(self.mod_time_unix_nanos).map_err(|_| DiskError::FileCorrupt)?;
        if self.etag.trim().is_empty() || self.data_dir.is_some_and(|id| id.is_nil() || id == Uuid::max()) {
            return Err(DiskError::FileCorrupt);
        }
        Ok(())
    }
}

/// One physical CAS attempt and the complete parent identity it may replace.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolMetaNativeCasCondition {
    pub attempt_id: Uuid,
    pub parent: Option<PoolMetaNativeFileIdentity>,
    #[serde(default)]
    pub parent_raw_sha256: Option<[u8; 32]>,
}

impl PoolMetaNativeCasCondition {
    pub const METADATA_SUFFIX: &'static str = CONDITION_SUFFIX;

    pub fn has_parent(&self) -> bool {
        self.parent.is_some() || self.parent_raw_sha256.is_some()
    }

    pub fn insert_into(&self, metadata: &mut HashMap<String, String>) -> Result<()> {
        self.validate()?;
        let encoded = serde_json::to_string(self).map_err(|_| DiskError::FileCorrupt)?;
        insert_str(metadata, CONDITION_SUFFIX, encoded);
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        if self.attempt_id.is_nil() || self.attempt_id == Uuid::max() {
            return Err(DiskError::FileCorrupt);
        }
        if self.parent.is_some() && self.parent_raw_sha256.is_some() {
            return Err(DiskError::FileCorrupt);
        }
        if let Some(parent) = &self.parent {
            parent.validate()?;
            if parent.data_dir == Some(self.attempt_id) {
                return Err(DiskError::FileCorrupt);
            }
        }
        Ok(())
    }

    pub(super) fn take(fi: &mut FileInfo) -> Result<Self> {
        let encoded = get_consistent_str(&fi.metadata, CONDITION_SUFFIX).ok_or(DiskError::ConditionalDeleteConflict)?;
        if encoded.len() > MAX_CONDITION_BYTES {
            return Err(DiskError::FileCorrupt);
        }
        let condition: Self = serde_json::from_str(encoded).map_err(|_| DiskError::FileCorrupt)?;
        condition.validate()?;
        remove_str(&mut fi.metadata, CONDITION_SUFFIX);
        Ok(condition)
    }
}

pub(super) fn is_object(volume: &str, path: &str) -> bool {
    volume == RUSTFS_META_BUCKET && clean(&path.replace('\\', "/")).trim_matches('/') == OBJECT
}

pub(super) fn reject_raw_path(volume: &str, path: &str) -> Result<()> {
    if volume == RUSTFS_META_BUCKET {
        let normalized = clean(&path.replace('\\', "/"));
        let normalized = normalized.trim_matches('/');
        if normalized == OBJECT || normalized.starts_with("pool.bin/") {
            return Err(DiskError::ConditionalDeleteConflict);
        }
    }
    Ok(())
}

fn decode(bytes: &[u8]) -> Result<FileInfo> {
    let fi = get_file_info(
        bytes,
        RUSTFS_META_BUCKET,
        OBJECT,
        "",
        FileInfoOpts {
            data: true,
            include_free_versions: false,
            include_part_checksums: true,
        },
    )?;
    fi.validate_for_metadata_read()?;
    if fi.deleted || fi.version_id.is_some_and(|id| !id.is_nil()) || contains_key_str(&fi.metadata, CONDITION_SUFFIX) {
        return Err(DiskError::FileCorrupt);
    }
    Ok(fi)
}

async fn read_optional(path: &Path, root: &os::PublicationRoot) -> Result<Option<Bytes>> {
    read_optional_limited(path, root, MAX_INVENTORY_BYTES).await
}

async fn read_optional_limited(path: &Path, root: &os::PublicationRoot, limit: usize) -> Result<Option<Bytes>> {
    let path = path.to_path_buf();
    let root = root.clone();
    let permit = acquire_context_disk_io_permit().await.map_err(DiskError::Io)?;
    spawn_blocking_with_disk_io_permit(permit, move || {
        let mut file = match root.open_readonly(&path) {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(to_file_error(err).into()),
        };
        let metadata = file.metadata().map_err(to_file_error)?;
        let length = usize::try_from(metadata.len()).map_err(|_| DiskError::FileCorrupt)?;
        if !metadata.is_file() || length > limit {
            return Err(DiskError::FileCorrupt);
        }
        // Check the opened descriptor before allocating. Also cap the read in
        // case another writer grows the inode after its metadata was sampled.
        let read_limit = u64::try_from(limit)
            .map_err(|_| DiskError::FileCorrupt)?
            .checked_add(1)
            .ok_or(DiskError::FileCorrupt)?;
        let mut data = Vec::with_capacity(length);
        std::io::Read::read_to_end(&mut std::io::Read::take(&mut file, read_limit), &mut data).map_err(to_file_error)?;
        if data.len() > limit {
            return Err(DiskError::FileCorrupt);
        }
        Ok(Some(Bytes::from(data)))
    })
    .await
    .map_err(DiskError::from)?
}

fn check_repair_request(
    inspection: &PoolMetaNativeInspection,
    expected_disk_id: Uuid,
    expected_current_sha256: Option<[u8; 32]>,
    expected_inventory_sha256: [u8; 32],
    replacement: &[u8],
) -> Result<FileInfo> {
    let current_sha: Option<[u8; 32]> = inspection.current.as_ref().map(|bytes| Sha256::digest(bytes).into());
    if inspection.disk_id != expected_disk_id
        || current_sha != expected_current_sha256
        || inspection.inventory_sha256()? != expected_inventory_sha256
    {
        return Err(DiskError::ConditionalDeleteConflict);
    }
    let new_fi = decode(replacement)?;
    let new_identity = PoolMetaNativeFileIdentity::from_file_info(&new_fi)?;
    if new_fi.size <= 0
        || !new_fi.inline_data()
        || new_fi.data.as_ref().is_none_or(Bytes::is_empty)
        || new_fi.parts.len() != 1
        || new_fi
            .parts
            .iter()
            .any(|part| part.mod_time.is_some() && part.mod_time != new_fi.mod_time)
    {
        return Err(DiskError::FileCorrupt);
    }
    for bytes in inspection
        .current
        .iter()
        .chain(inspection.backups.iter().map(|backup| &backup.data))
    {
        let previous = decode(bytes)?;
        let previous_identity = PoolMetaNativeFileIdentity::from_file_info(&previous)?;
        if new_identity.mod_time_unix_nanos <= previous_identity.mod_time_unix_nanos || previous_identity == new_identity {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        if new_fi.erasure.data_blocks != previous.erasure.data_blocks
            || new_fi.erasure.parity_blocks != previous.erasure.parity_blocks
            || new_fi.erasure.block_size != previous.erasure.block_size
            || new_fi.erasure.index != previous.erasure.index
            || new_fi.erasure.distribution != previous.erasure.distribution
        {
            return Err(DiskError::FileCorrupt);
        }
    }
    Ok(new_fi)
}

impl LocalDisk {
    pub(super) async fn pool_meta_cleanup_owner(
        &self,
        volume: &str,
        path: &str,
        opts: &DeleteOptions,
        owner: Option<Arc<dyn Send + Sync>>,
    ) -> Result<Option<Arc<dyn Send + Sync>>> {
        if reject_raw_path(volume, path).is_ok() {
            return Ok(owner);
        }
        let normalized = clean(&path.replace('\\', "/"));
        let parts = normalized.trim_matches('/').split('/').collect::<Vec<_>>();
        let directory = match parts.as_slice() {
            [OBJECT, directory] if opts.recursive && !opts.orphan_data_dir => *directory,
            [OBJECT, directory, STORAGE_FORMAT_FILE_BACKUP] if !opts.recursive => *directory,
            _ => return Err(DiskError::ConditionalDeleteConflict),
        };
        let directory = Uuid::parse_str(directory).map_err(|_| DiskError::ConditionalDeleteConflict)?;
        if directory.is_nil() || directory == Uuid::max() {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        let object_key = self.get_object_path(RUSTFS_META_BUCKET, OBJECT)?;
        let object_dir = self.io_get_object_path(RUSTFS_META_BUCKET, OBJECT)?;
        let owner = os::acquire_metadata_mutation_lease(&object_key, owner).await;
        let current = read_optional(&object_dir.join(STORAGE_FORMAT_FILE), &self.publication_root)
            .await?
            .ok_or(DiskError::ConditionalDeleteConflict)?;
        let metadata = FileMeta::load(&current)?;
        if metadata
            .get_data_dirs()?
            .into_iter()
            .flatten()
            .any(|referenced| referenced == directory)
        {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        Ok(Some(owner))
    }

    pub(super) async fn check_pool_meta_parent(&self, condition: &PoolMetaNativeCasCondition, destination: &Path) -> Result<()> {
        let current = read_optional(&destination.join(STORAGE_FORMAT_FILE), &self.publication_root).await?;
        let matches = if let Some(expected) = condition.parent_raw_sha256 {
            current
                .as_ref()
                .is_some_and(|bytes| <[u8; 32]>::from(Sha256::digest(bytes)) == expected)
        } else {
            let current = current.as_deref().map(decode).transpose()?;
            current.as_ref().map(PoolMetaNativeFileIdentity::from_file_info).transpose()? == condition.parent
        };
        if !matches {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        match fs::symlink_metadata(destination.join(condition.attempt_id.to_string())).await {
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
            Ok(_) => Err(DiskError::ConditionalDeleteConflict),
            Err(err) => Err(to_file_error(err).into()),
        }
    }

    pub(super) async fn check_pool_meta_undo(&self, fi: &mut FileInfo, opts: &DeleteOptions, object_dir: &Path) -> Result<()> {
        if !opts.undo_write || opts.undo_delete {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        let condition = PoolMetaNativeCasCondition::take(fi)?;
        let expected_new = PoolMetaNativeFileIdentity::from_file_info(fi)?;
        let current = read_optional(&object_dir.join(STORAGE_FORMAT_FILE), &self.publication_root)
            .await?
            .ok_or(DiskError::ConditionalDeleteConflict)?;
        if PoolMetaNativeFileIdentity::from_file_info(&decode(&current)?)? != expected_new {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        if condition.has_parent() {
            if opts.old_data_dir != Some(condition.attempt_id) {
                return Err(DiskError::ConditionalDeleteConflict);
            }
            let backup = read_optional(
                &object_dir
                    .join(condition.attempt_id.to_string())
                    .join(STORAGE_FORMAT_FILE_BACKUP),
                &self.publication_root,
            )
            .await?
            .ok_or(DiskError::ConditionalDeleteConflict)?;
            let matches = match &condition.parent {
                Some(parent) => PoolMetaNativeFileIdentity::from_file_info(&decode(&backup)?)? == *parent,
                None => condition.parent_raw_sha256 == Some(Sha256::digest(&backup).into()),
            };
            if !matches {
                return Err(DiskError::ConditionalDeleteConflict);
            }
        } else if opts.old_data_dir.is_some() {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        Ok(())
    }

    async fn pool_meta_inventory(&self, object_dir: &Path) -> Result<PoolMetaNativeInspection> {
        let disk_id = self.get_disk_id().await?.ok_or(DiskError::UnformattedDisk)?;
        let identity_path = self.io_get_object_path(RUSTFS_META_BUCKET, "pool.bin.identity/xl.meta")?;
        let identity = read_optional_limited(&identity_path, &self.publication_root, MAX_INVENTORY_BYTES).await?;
        let identity_bytes = identity.as_ref().map_or(0, Bytes::len);
        let remaining = MAX_INVENTORY_BYTES
            .checked_sub(identity_bytes)
            .ok_or(DiskError::FileCorrupt)?;
        let current = read_optional_limited(&object_dir.join(STORAGE_FORMAT_FILE), &self.publication_root, remaining).await?;
        let mut backups = Vec::new();
        let mut total_bytes = identity_bytes
            .checked_add(current.as_ref().map_or(0, Bytes::len))
            .ok_or(DiskError::FileCorrupt)?;
        let mut entries = match fs::read_dir(object_dir).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                return Ok(PoolMetaNativeInspection {
                    disk_id,
                    identity,
                    current,
                    backups,
                });
            }
            Err(err) => return Err(to_file_error(err).into()),
        };
        let mut examined = 0usize;
        while let Some(entry) = entries.next_entry().await.map_err(to_file_error)? {
            examined = examined.checked_add(1).ok_or(DiskError::FileCorrupt)?;
            if examined > MAX_INVENTORY_ENTRIES {
                return Err(DiskError::FileCorrupt);
            }
            let kind = entry.file_type().await.map_err(to_file_error)?;
            if kind.is_symlink() {
                return Err(DiskError::FileCorrupt);
            }
            if !kind.is_dir() {
                continue;
            }
            let remaining = MAX_INVENTORY_BYTES.checked_sub(total_bytes).ok_or(DiskError::FileCorrupt)?;
            let backup =
                read_optional_limited(&entry.path().join(STORAGE_FORMAT_FILE_BACKUP), &self.publication_root, remaining).await?;
            let Some(data) = backup else { continue };
            let directory = entry
                .file_name()
                .to_str()
                .and_then(|name| Uuid::parse_str(name).ok())
                .ok_or(DiskError::FileCorrupt)?;
            if directory.is_nil() || directory == Uuid::max() || backups.len() >= MAX_BACKUPS {
                return Err(DiskError::FileCorrupt);
            }
            total_bytes = total_bytes.checked_add(data.len()).ok_or(DiskError::FileCorrupt)?;
            if total_bytes > MAX_INVENTORY_BYTES {
                return Err(DiskError::FileCorrupt);
            }
            backups.push(PoolMetaNativeBackup { directory, data });
        }
        backups.sort_unstable_by_key(|backup| backup.directory);
        Ok(PoolMetaNativeInspection {
            disk_id,
            identity,
            current,
            backups,
        })
    }

    pub(crate) async fn pool_meta_native_recovery(
        &self,
        request: PoolMetaNativeRecoveryRequest,
    ) -> Result<PoolMetaNativeRecoveryResponse> {
        if matches!(request, PoolMetaNativeRecoveryRequest::Probe { .. }) {
            return Ok(PoolMetaNativeRecoveryResponse::Probed);
        }
        if matches!(request, PoolMetaNativeRecoveryRequest::InspectIdentity { .. }) {
            let identity_key = self.get_object_path(RUSTFS_META_BUCKET, "pool.bin.identity")?;
            let identity_dir = self.io_get_object_path(RUSTFS_META_BUCKET, "pool.bin.identity")?;
            let metadata = os::acquire_metadata_mutation_lease(&identity_key, None).await;
            let owner: Arc<dyn Send + Sync> = Arc::new(metadata);
            let _identity_owner = os::acquire_namespace_mutation_lease_with_owner(&identity_dir, Some(owner)).await;
            let disk_id = self.get_disk_id().await?.ok_or(DiskError::UnformattedDisk)?;
            let raw = read_optional_limited(&identity_dir.join(STORAGE_FORMAT_FILE), &self.publication_root, MAX_INVENTORY_BYTES)
                .await?
                .ok_or(DiskError::FileNotFound)?;
            return Ok(PoolMetaNativeRecoveryResponse::IdentityInspected { disk_id, raw });
        }
        let object_key = self.get_object_path(RUSTFS_META_BUCKET, OBJECT)?;
        let object_dir = self.io_get_object_path(RUSTFS_META_BUCKET, OBJECT)?;
        // Lock order matches RenameData: metadata, object namespace, volume,
        // then backup paths. The publication lease retains every owner through
        // the final rename and directory sync, including cancelled RPCs.
        let identity_key = self.get_object_path(RUSTFS_META_BUCKET, "pool.bin.identity")?;
        let identity_dir = self.io_get_object_path(RUSTFS_META_BUCKET, "pool.bin.identity")?;
        let identity_metadata = os::acquire_metadata_mutation_lease(&identity_key, None).await;
        let metadata = os::acquire_metadata_mutation_lease(&object_key, None).await;
        let metadata: Arc<dyn Send + Sync> = Arc::new((identity_metadata, metadata));
        let identity_owner = os::acquire_namespace_mutation_lease_with_owner(&identity_dir, Some(metadata)).await;
        let owner = os::acquire_rename_data_mutation_lease_with_owner(
            &self.root,
            RUSTFS_META_BUCKET,
            &object_dir,
            None,
            Some(identity_owner),
        )
        .await;
        let inspection = self.pool_meta_inventory(&object_dir).await?;
        let mut backup_leases = Vec::with_capacity(inspection.backups.len());
        for backup in &inspection.backups {
            backup_leases.push(
                os::acquire_namespace_mutation_lease_with_owner(
                    &object_dir.join(backup.directory.to_string()),
                    Some(owner.clone()),
                )
                .await,
            );
        }
        drop(inspection);
        let inspection = self.pool_meta_inventory(&object_dir).await?;
        match request {
            PoolMetaNativeRecoveryRequest::Apply {
                expected_disk_id,
                expected_current_sha256,
                expected_backup_inventory_sha256,
                replacement,
                ..
            } => {
                let new_fi = check_repair_request(
                    &inspection,
                    expected_disk_id,
                    expected_current_sha256,
                    expected_backup_inventory_sha256,
                    &replacement,
                )?;
                let part = new_fi.parts.first().ok_or(DiskError::FileCorrupt)?;
                let erasure = coding::Erasure::try_new_with_options(
                    new_fi.erasure.data_blocks,
                    new_fi.erasure.parity_blocks,
                    new_fi.erasure.block_size,
                    new_fi.uses_legacy_checksum,
                )
                .map_err(DiskError::from)?;
                let checksum = new_fi.erasure.get_checksum_info(part.number);
                let algorithm = if new_fi.uses_legacy_checksum && checksum.algorithm == HashAlgorithm::HighwayHash256S {
                    HashAlgorithm::HighwayHash256SLegacy
                } else {
                    checksum.algorithm
                };
                let data = new_fi.data.as_ref().ok_or(DiskError::FileCorrupt)?;
                let part_size = i64::try_from(part.size).map_err(|_| DiskError::FileCorrupt)?;
                let logical_size = usize::try_from(erasure.shard_file_size(part_size)).map_err(|_| DiskError::FileCorrupt)?;
                bitrot_verify(
                    std::io::Cursor::new(data.clone()),
                    data.len(),
                    logical_size,
                    algorithm,
                    erasure.shard_size(),
                )
                .await
                .map_err(|_| DiskError::FileCorrupt)?;
                let owner: Arc<dyn Send + Sync> = Arc::new((owner, backup_leases));
                let destination = self.io_get_object_path(RUSTFS_META_BUCKET, "pool.bin/xl.meta")?;
                let publication = os::acquire_namespace_mutation_lease_with_owner(&destination, Some(owner)).await;
                let tmp_volume = self.io_get_bucket_path(RUSTFS_META_TMP_BUCKET)?;
                let temporary = self.io_get_object_path(RUSTFS_META_TMP_BUCKET, &Uuid::new_v4().to_string())?;
                self.write_all_internal(&temporary, InternalBuf::Ref(&replacement), SyncMode::FileOnly, &tmp_volume)
                    .await?;
                os::rename_metadata_and_sync_with_lease(
                    temporary,
                    destination,
                    self.io_get_bucket_path(RUSTFS_META_BUCKET)?,
                    self.publication_root.clone(),
                    publication,
                )
                .await?;
                Ok(PoolMetaNativeRecoveryResponse::Applied)
            }
            // Probe and InspectIdentity return before taking pool.bin leases.
            _ => Ok(PoolMetaNativeRecoveryResponse::Inspected(inspection)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::test::{ensure_test_volume, test_file_info, test_meta};
    use super::*;

    fn inline_info(etag: &str, timestamp: i64) -> FileInfo {
        let mut fi = test_file_info(OBJECT, Uuid::nil(), Some(Uuid::new_v4()), Some(Bytes::from_static(b"metadata")));
        fi.mod_time = Some(OffsetDateTime::from_unix_timestamp(timestamp).expect("test timestamp"));
        fi.metadata.insert("etag".to_string(), etag.to_string());
        fi.set_inline_data();
        fi
    }

    async fn formatted_disk() -> (tempfile::TempDir, LocalDisk, Uuid) {
        let dir = tempfile::tempdir().expect("temporary formatted disk");
        let mut endpoint = Endpoint::try_from(dir.path().to_str().expect("UTF-8 temporary disk")).expect("disk endpoint");
        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(0);
        let metadata_dir = dir.path().join(RUSTFS_META_BUCKET);
        fs::create_dir_all(&metadata_dir).await.expect("metadata volume");
        let mut format = crate::disk::format::FormatV3::new(1, 1);
        format.erasure.this = format.erasure.sets[0][0];
        let disk_id = format.erasure.this;
        fs::write(
            metadata_dir.join(crate::disk::FORMAT_CONFIG_FILE),
            format.to_json().expect("disk format encoding"),
        )
        .await
        .expect("disk format fixture");
        let disk = LocalDisk::new(&endpoint, false).await.expect("formatted local disk");
        (dir, disk, disk_id)
    }

    async fn replacement() -> (tempfile::TempDir, LocalDisk, Bytes, FileInfo, Uuid) {
        let dir = tempfile::tempdir().expect("temporary disk");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("UTF-8 temporary disk")).expect("disk endpoint");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk");
        ensure_test_volume(&disk, RUSTFS_META_BUCKET).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;
        let object = dir.path().join(RUSTFS_META_BUCKET).join(OBJECT);
        fs::create_dir_all(&object).await.expect("object directory");
        let parent = inline_info("parent", 10);
        let parent_bytes = Bytes::from(test_meta(parent.clone()));
        fs::write(object.join(STORAGE_FORMAT_FILE), &parent_bytes)
            .await
            .expect("parent metadata");
        fs::create_dir_all(dir.path().join(RUSTFS_META_TMP_BUCKET).join("new"))
            .await
            .expect("staging directory");
        let mut next = inline_info("next", 11);
        let attempt = Uuid::new_v4();
        PoolMetaNativeCasCondition {
            attempt_id: attempt,
            parent: Some(PoolMetaNativeFileIdentity::from_file_info(&parent).expect("parent identity")),
            parent_raw_sha256: None,
        }
        .insert_into(&mut next.metadata)
        .expect("native CAS condition");
        let result = disk
            .rename_data(RUSTFS_META_TMP_BUCKET, "new", next.clone(), RUSTFS_META_BUCKET, OBJECT)
            .await
            .expect("conditional pool metadata rename");
        assert_eq!(result.rollback_data_dir, Some(attempt));
        let current = fs::read(object.join(STORAGE_FORMAT_FILE)).await.expect("committed metadata");
        assert!(!contains_key_str(&decode(&current).expect("decode current").metadata, CONDITION_SUFFIX));
        assert_eq!(
            fs::read(object.join(attempt.to_string()).join(STORAGE_FORMAT_FILE_BACKUP))
                .await
                .expect("owned predecessor"),
            parent_bytes
        );
        (dir, disk, parent_bytes, next, attempt)
    }

    #[test]
    fn pool_meta_native_condition_is_strict_transient_and_dual_prefixed() {
        let mut fi = inline_info("new", 11);
        let condition = PoolMetaNativeCasCondition {
            attempt_id: Uuid::new_v4(),
            parent: None,
            parent_raw_sha256: None,
        };
        condition.insert_into(&mut fi.metadata).expect("encode condition");
        assert!(fi.metadata.contains_key("x-rustfs-internal-pool-meta-native-cas"));
        assert!(fi.metadata.contains_key("x-minio-internal-pool-meta-native-cas"));
        assert_eq!(PoolMetaNativeCasCondition::take(&mut fi).expect("consume condition"), condition);
        assert!(!contains_key_str(&fi.metadata, CONDITION_SUFFIX));
        condition.insert_into(&mut fi.metadata).expect("encode condition again");
        fi.metadata
            .insert("x-minio-internal-pool-meta-native-cas".to_string(), "different".to_string());
        assert!(PoolMetaNativeCasCondition::take(&mut fi).is_err(), "conflicting aliases must fail closed");
        assert!(
            PoolMetaNativeCasCondition {
                attempt_id: Uuid::nil(),
                parent: None,
                parent_raw_sha256: None,
            }
            .insert_into(&mut HashMap::new())
            .is_err()
        );
    }

    #[tokio::test]
    async fn pool_meta_native_initial_cas_distinguishes_absent_from_empty_metadata() {
        for empty_existing in [false, true] {
            let dir = tempfile::tempdir().expect("temporary disk");
            let endpoint = Endpoint::try_from(dir.path().to_str().expect("UTF-8 disk path")).expect("disk endpoint");
            let disk = LocalDisk::new(&endpoint, false).await.expect("local disk");
            ensure_test_volume(&disk, RUSTFS_META_BUCKET).await;
            ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;
            let object = dir.path().join(RUSTFS_META_BUCKET).join(OBJECT);
            if empty_existing {
                fs::create_dir_all(&object).await.expect("existing object directory");
                fs::write(object.join(STORAGE_FORMAT_FILE), b"")
                    .await
                    .expect("existing empty metadata");
            }
            fs::create_dir_all(dir.path().join(RUSTFS_META_TMP_BUCKET).join("initial"))
                .await
                .expect("staging directory");
            let mut fi = inline_info("initial", 11);
            PoolMetaNativeCasCondition {
                attempt_id: Uuid::new_v4(),
                parent: None,
                parent_raw_sha256: None,
            }
            .insert_into(&mut fi.metadata)
            .expect("initial CAS condition");
            let result = disk
                .rename_data(RUSTFS_META_TMP_BUCKET, "initial", fi, RUSTFS_META_BUCKET, OBJECT)
                .await;
            if empty_existing {
                assert!(result.is_err(), "an existing empty xl.meta must not satisfy an absent-parent condition");
                assert!(
                    fs::read(object.join(STORAGE_FORMAT_FILE))
                        .await
                        .expect("empty metadata remains")
                        .is_empty()
                );
            } else {
                let committed = result.expect("an absent parent must permit the first native pool metadata write");
                assert_eq!(committed.rollback_data_dir, None);
                assert_eq!(
                    decode(&fs::read(object.join(STORAGE_FORMAT_FILE)).await.expect("initial metadata"))
                        .expect("decode initial metadata")
                        .get_etag()
                        .as_deref(),
                    Some("initial")
                );
            }
        }
    }

    #[tokio::test]
    async fn pool_meta_native_fresh_undo_removes_only_its_matching_initial_write() {
        for batch in [false, true] {
            let dir = tempfile::tempdir().expect("temporary disk");
            let endpoint = Endpoint::try_from(dir.path().to_str().expect("UTF-8 disk path")).expect("disk endpoint");
            let disk = LocalDisk::new(&endpoint, false).await.expect("local disk");
            for volume in [RUSTFS_META_BUCKET, RUSTFS_META_TMP_BUCKET, RUSTFS_META_TMP_DELETED_BUCKET] {
                ensure_test_volume(&disk, volume).await;
            }
            fs::create_dir_all(dir.path().join(RUSTFS_META_TMP_BUCKET).join("initial"))
                .await
                .expect("staging directory");
            let mut fi = inline_info("initial", 11);
            PoolMetaNativeCasCondition {
                attempt_id: Uuid::new_v4(),
                parent: None,
                parent_raw_sha256: None,
            }
            .insert_into(&mut fi.metadata)
            .expect("initial CAS condition");
            let committed = disk
                .rename_data(RUSTFS_META_TMP_BUCKET, "initial", fi.clone(), RUSTFS_META_BUCKET, OBJECT)
                .await
                .expect("initial pool metadata publication");
            assert_eq!(committed.rollback_data_dir, None);
            let head = dir.path().join(RUSTFS_META_BUCKET).join(OBJECT).join(STORAGE_FORMAT_FILE);
            let original = fs::read(&head).await.expect("initial metadata");
            assert!(matches!(
                disk.delete(
                    RUSTFS_META_BUCKET,
                    OBJECT,
                    DeleteOptions {
                        recursive: true,
                        ..Default::default()
                    },
                )
                .await,
                Err(DiskError::ConditionalDeleteConflict)
            ));
            assert_eq!(fs::read(&head).await.expect("raw deletion must retain the head"), original);
            let opts = DeleteOptions {
                undo_write: true,
                ..Default::default()
            };
            if batch {
                disk.delete_versions_guarded_internal(RUSTFS_META_BUCKET, OBJECT, &[fi], &opts, None, None)
                    .await
                    .expect("batch undo may remove its exact initial publication after quorum failure");
            } else {
                disk.undo_write_with_namespace_owner(RUSTFS_META_BUCKET, OBJECT, fi, opts, None)
                    .await
                    .expect("undo may remove its exact initial publication after quorum failure");
            }
            assert!(!fs::try_exists(&head).await.expect("rolled-back head lookup"));
            assert!(
                fs::try_exists(dir.path().join(RUSTFS_META_BUCKET))
                    .await
                    .expect("metadata volume lookup")
            );
        }
    }

    #[tokio::test]
    async fn pool_meta_native_opaque_corrupt_parent_requires_exact_sha_and_restores_original_bytes() {
        let dir = tempfile::tempdir().expect("temporary disk");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("UTF-8 disk path")).expect("disk endpoint");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk");
        ensure_test_volume(&disk, RUSTFS_META_BUCKET).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;
        let object = dir.path().join(RUSTFS_META_BUCKET).join(OBJECT);
        fs::create_dir_all(&object).await.expect("object directory");
        let corrupt = Bytes::from_static(b"XL2 \x01\x00\x03\x00corrupt metadata payload");
        fs::write(object.join(STORAGE_FORMAT_FILE), &corrupt)
            .await
            .expect("corrupt parent fixture");
        fs::create_dir_all(dir.path().join(RUSTFS_META_TMP_BUCKET).join("opaque"))
            .await
            .expect("staging directory");
        let mut new_fi = inline_info("opaque repair", 11);
        PoolMetaNativeCasCondition {
            attempt_id: Uuid::new_v4(),
            parent: None,
            parent_raw_sha256: Some([0; 32]),
        }
        .insert_into(&mut new_fi.metadata)
        .expect("wrong raw-parent condition");
        let error = disk
            .rename_data(RUSTFS_META_TMP_BUCKET, "opaque", new_fi.clone(), RUSTFS_META_BUCKET, OBJECT)
            .await
            .expect_err("wrong raw-parent SHA must reject before publication");
        assert!(matches!(error, DiskError::ConditionalDeleteConflict));
        assert_eq!(
            fs::read(object.join(STORAGE_FORMAT_FILE))
                .await
                .expect("unchanged corrupt parent"),
            corrupt
        );
        let attempt = Uuid::new_v4();
        PoolMetaNativeCasCondition {
            attempt_id: attempt,
            parent: None,
            parent_raw_sha256: Some(Sha256::digest(&corrupt).into()),
        }
        .insert_into(&mut new_fi.metadata)
        .expect("exact raw-parent condition");
        let response = disk
            .rename_data(RUSTFS_META_TMP_BUCKET, "opaque", new_fi.clone(), RUSTFS_META_BUCKET, OBJECT)
            .await
            .expect("only the exact corrupt parent may be conditionally replaced");
        assert_eq!(response.rollback_data_dir, Some(attempt));
        let backup = object.join(attempt.to_string()).join(STORAGE_FORMAT_FILE_BACKUP);
        assert_eq!(fs::read(&backup).await.expect("opaque parent backup"), corrupt);
        let committed = fs::read(object.join(STORAGE_FORMAT_FILE)).await.expect("committed repair");
        fs::write(&backup, b"changed opaque parent")
            .await
            .expect("changed backup fixture");
        let opts = DeleteOptions {
            undo_write: true,
            old_data_dir: Some(attempt),
            ..Default::default()
        };
        assert!(matches!(
            disk.undo_write_with_namespace_owner(RUSTFS_META_BUCKET, OBJECT, new_fi.clone(), opts.clone(), None)
                .await,
            Err(DiskError::ConditionalDeleteConflict)
        ));
        assert_eq!(
            fs::read(object.join(STORAGE_FORMAT_FILE))
                .await
                .expect("repair remains after rejected undo"),
            committed
        );
        fs::write(&backup, &corrupt)
            .await
            .expect("restore exact predecessor evidence");
        disk.undo_write_with_namespace_owner(RUSTFS_META_BUCKET, OBJECT, new_fi, opts, None)
            .await
            .expect("verified opaque undo should restore the original bytes");
        assert_eq!(
            fs::read(object.join(STORAGE_FORMAT_FILE))
                .await
                .expect("restored original corruption"),
            corrupt
        );
    }

    #[tokio::test]
    async fn pool_meta_native_bounded_read_rejects_oversized_files_before_allocating() {
        let dir = tempfile::tempdir().expect("temporary metadata root");
        let root = os::PublicationRoot::new(dir.path()).expect("anchored metadata root");
        let missing = dir.path().join("missing");
        assert!(
            read_optional_limited(&missing, &root, 8)
                .await
                .expect("missing metadata probe")
                .is_none()
        );
        let empty = dir.path().join("empty");
        fs::write(&empty, b"").await.expect("empty metadata fixture");
        assert_eq!(
            read_optional_limited(&empty, &root, 8).await.expect("empty metadata probe"),
            Some(Bytes::new())
        );
        let huge = dir.path().join("huge");
        let file = std::fs::File::create(&huge).expect("sparse metadata fixture");
        file.set_len(1024 * 1024 * 1024).expect("oversized sparse metadata");
        let error = read_optional_limited(&huge, &root, 8)
            .await
            .expect_err("oversized metadata must fail at its opened descriptor length");
        assert!(matches!(error, DiskError::FileCorrupt));
        fs::write(&huge, b"12345678").await.expect("bounded metadata fixture");
        assert_eq!(
            read_optional_limited(&huge, &root, 8)
                .await
                .expect("exact budget is readable"),
            Some(Bytes::from_static(b"12345678"))
        );
        assert!(matches!(read_optional_limited(&huge, &root, 7).await, Err(DiskError::FileCorrupt)));
    }

    #[tokio::test]
    async fn pool_meta_native_identity_inspection_distinguishes_missing_from_exact_current_bytes() {
        let (dir, disk, expected_disk_id) = formatted_disk().await;
        assert!(matches!(
            disk.pool_meta_native_recovery(PoolMetaNativeRecoveryRequest::InspectIdentity {})
                .await,
            Err(DiskError::FileNotFound)
        ));
        let identity_dir = dir.path().join(RUSTFS_META_BUCKET).join("pool.bin.identity");
        fs::create_dir_all(&identity_dir).await.expect("identity object directory");
        for raw in [Bytes::from_static(b"exact identity xl.meta bytes"), Bytes::new()] {
            fs::write(identity_dir.join(STORAGE_FORMAT_FILE), &raw)
                .await
                .expect("identity metadata fixture");
            assert_eq!(
                disk.pool_meta_native_recovery(PoolMetaNativeRecoveryRequest::InspectIdentity {})
                    .await
                    .expect("identity inspection must return the current raw bytes"),
                PoolMetaNativeRecoveryResponse::IdentityInspected {
                    disk_id: expected_disk_id,
                    raw,
                }
            );
        }
    }

    #[tokio::test]
    async fn pool_meta_native_inventory_allows_a_separate_identity_set_and_binds_colocated_bytes() {
        let (dir, disk, expected_disk_id) = formatted_disk().await;
        let object_dir = dir.path().join(RUSTFS_META_BUCKET).join(OBJECT);
        fs::create_dir_all(&object_dir).await.expect("pool metadata directory");
        let current = Bytes::from(test_meta(inline_info("current", 11)));
        fs::write(object_dir.join(STORAGE_FORMAT_FILE), &current)
            .await
            .expect("pool metadata fixture");
        let request = PoolMetaNativeRecoveryRequest::Inspect {
            object: crate::disk::PoolMetaNativeObject::PoolBin,
        };
        let PoolMetaNativeRecoveryResponse::Inspected(inspection) = disk
            .pool_meta_native_recovery(request.clone())
            .await
            .expect("a pool.bin disk need not own the separately hashed identity object")
        else {
            panic!("pool metadata inspection response");
        };
        assert_eq!(inspection.disk_id, expected_disk_id);
        assert_eq!(inspection.current, Some(current));
        assert_eq!(inspection.identity, None);
        let separate_set_inventory = inspection.inventory_sha256().expect("separate-set inventory digest");

        let identity_dir = dir.path().join(RUSTFS_META_BUCKET).join("pool.bin.identity");
        fs::create_dir_all(&identity_dir)
            .await
            .expect("co-located identity directory");
        fs::write(identity_dir.join(STORAGE_FORMAT_FILE), b"")
            .await
            .expect("empty identity metadata fixture");
        let PoolMetaNativeRecoveryResponse::Inspected(inspection) = disk
            .pool_meta_native_recovery(request)
            .await
            .expect("co-located identity bytes must remain part of the inventory")
        else {
            panic!("pool metadata inspection response");
        };
        assert_eq!(inspection.identity, Some(Bytes::new()));
        assert_ne!(
            inspection.inventory_sha256().expect("co-located inventory digest"),
            separate_set_inventory,
            "a present empty identity must not be interchangeable with a separate identity set"
        );
    }

    #[tokio::test]
    async fn pool_meta_native_late_rename_preserves_current_and_owned_backup() {
        let (dir, disk, _, next, attempt) = replacement().await;
        let object = dir.path().join(RUSTFS_META_BUCKET).join(OBJECT);
        let current = fs::read(object.join(STORAGE_FORMAT_FILE)).await.expect("current metadata");
        let backup = fs::read(object.join(attempt.to_string()).join(STORAGE_FORMAT_FILE_BACKUP))
            .await
            .expect("backup metadata");
        let error = disk
            .rename_data(RUSTFS_META_TMP_BUCKET, "new", next, RUSTFS_META_BUCKET, OBJECT)
            .await
            .expect_err("duplicate or late CAS must not replace its predecessor backup");
        assert!(matches!(error, DiskError::ConditionalDeleteConflict));
        assert_eq!(fs::read(object.join(STORAGE_FORMAT_FILE)).await.expect("retained current"), current);
        assert_eq!(
            fs::read(object.join(attempt.to_string()).join(STORAGE_FORMAT_FILE_BACKUP))
                .await
                .expect("retained backup"),
            backup
        );
    }

    #[tokio::test]
    async fn pool_meta_native_late_undo_rejects_fresh_repaired_identity() {
        let (dir, disk, _, next, attempt) = replacement().await;
        let object = dir.path().join(RUSTFS_META_BUCKET).join(OBJECT);
        let repaired = test_meta(inline_info("repaired", 12));
        fs::write(object.join(STORAGE_FORMAT_FILE), &repaired)
            .await
            .expect("simulate fenced repair publication");
        let error = disk
            .undo_write_with_namespace_owner(
                RUSTFS_META_BUCKET,
                OBJECT,
                next,
                DeleteOptions {
                    undo_write: true,
                    old_data_dir: Some(attempt),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect_err("stale undo must not restore the predecessor over a repaired head");
        assert!(matches!(error, DiskError::ConditionalDeleteConflict));
        assert_eq!(
            fs::read(object.join(STORAGE_FORMAT_FILE))
                .await
                .expect("repaired head remains"),
            repaired
        );
        assert!(object.join(attempt.to_string()).join(STORAGE_FORMAT_FILE_BACKUP).exists());
    }

    #[tokio::test]
    async fn pool_meta_native_batch_undo_checks_the_exact_parent_backup() {
        let (dir, disk, _, next, attempt) = replacement().await;
        let object = dir.path().join(RUSTFS_META_BUCKET).join(OBJECT);
        let current = fs::read(object.join(STORAGE_FORMAT_FILE)).await.expect("current head");
        let foreign = test_meta(inline_info("different-parent", 9));
        fs::write(object.join(attempt.to_string()).join(STORAGE_FORMAT_FILE_BACKUP), &foreign)
            .await
            .expect("replace backup evidence");
        let error = disk
            .delete_versions_guarded_internal(
                RUSTFS_META_BUCKET,
                OBJECT,
                &[next],
                &DeleteOptions {
                    undo_write: true,
                    old_data_dir: Some(attempt),
                    ..Default::default()
                },
                None,
                None,
            )
            .await
            .expect_err("batch undo must not consume a changed predecessor");
        assert!(matches!(error, DiskError::ConditionalDeleteConflict));
        assert_eq!(fs::read(object.join(STORAGE_FORMAT_FILE)).await.expect("retained current"), current);
        assert_eq!(
            fs::read(object.join(attempt.to_string()).join(STORAGE_FORMAT_FILE_BACKUP))
                .await
                .expect("retained foreign evidence"),
            foreign
        );
    }

    #[tokio::test]
    async fn pool_meta_native_owned_undo_restores_the_exact_parent() {
        let (dir, disk, parent_bytes, next, attempt) = replacement().await;
        disk.undo_write_with_namespace_owner(
            RUSTFS_META_BUCKET,
            OBJECT,
            next,
            DeleteOptions {
                undo_write: true,
                old_data_dir: Some(attempt),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("the owning attempt may restore its verified predecessor");
        assert_eq!(
            fs::read(dir.path().join(RUSTFS_META_BUCKET).join(OBJECT).join(STORAGE_FORMAT_FILE))
                .await
                .expect("restored metadata"),
            parent_bytes
        );
    }

    #[tokio::test]
    async fn pool_meta_native_unguarded_entry_is_rejected_and_other_objects_keep_working() {
        let dir = tempfile::tempdir().expect("temporary disk");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("UTF-8 disk path")).expect("disk endpoint");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk");
        ensure_test_volume(&disk, RUSTFS_META_BUCKET).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;
        ensure_test_volume(&disk, "bucket").await;
        fs::create_dir_all(dir.path().join(RUSTFS_META_TMP_BUCKET).join("normal"))
            .await
            .expect("staging directory");
        assert!(
            disk.rename_data(
                RUSTFS_META_TMP_BUCKET,
                "normal",
                inline_info("unconditional", 11),
                RUSTFS_META_BUCKET,
                OBJECT
            )
            .await
            .is_err()
        );
        disk.rename_data(RUSTFS_META_TMP_BUCKET, "normal", inline_info("ordinary", 11), "bucket", "ordinary")
            .await
            .expect("ordinary objects must not acquire the pool.bin condition contract");
        assert!(dir.path().join("bucket/ordinary/xl.meta").exists());
    }

    #[test]
    fn pool_meta_native_repair_rejects_changed_inventory_identity_and_replayed_attempt() {
        let current = Bytes::from(test_meta(inline_info("current", 11)));
        let mut inspection = PoolMetaNativeInspection {
            disk_id: Uuid::new_v4(),
            identity: Some(Bytes::from_static(b"durable identity")),
            current: Some(current.clone()),
            backups: vec![PoolMetaNativeBackup {
                directory: Uuid::new_v4(),
                data: test_meta(inline_info("parent", 10)).into(),
            }],
        };
        let expected_current = Some(Sha256::digest(&current).into());
        let expected_inventory = inspection.inventory_sha256().expect("inventory identity");
        let replacement = test_meta(inline_info("repair", 12));
        check_repair_request(&inspection, inspection.disk_id, expected_current, expected_inventory, &replacement)
            .expect("fresh replacement should match a complete snapshot");
        assert!(
            check_repair_request(&inspection, inspection.disk_id, expected_current, expected_inventory, &current).is_err(),
            "repair must not reuse a prior attempt identity"
        );
        inspection.backups[0].data = test_meta(inline_info("different predecessor", 9)).into();
        assert!(matches!(
            check_repair_request(&inspection, inspection.disk_id, expected_current, expected_inventory, &replacement),
            Err(DiskError::ConditionalDeleteConflict)
        ));
        let expected_inventory = inspection.inventory_sha256().expect("updated backup inventory");
        inspection.identity = Some(Bytes::from_static(b"changed durable identity"));
        assert!(matches!(
            check_repair_request(&inspection, inspection.disk_id, expected_current, expected_inventory, &replacement),
            Err(DiskError::ConditionalDeleteConflict)
        ));
    }

    #[tokio::test]
    async fn pool_meta_native_cleanup_keeps_current_data_and_other_attempt_backups() {
        let (dir, disk, _, next, attempt) = replacement().await;
        let object = dir.path().join(RUSTFS_META_BUCKET).join(OBJECT);
        let current = fs::read(object.join(STORAGE_FORMAT_FILE)).await.expect("current metadata");
        let another = Uuid::new_v4();
        fs::create_dir_all(object.join(another.to_string()))
            .await
            .expect("other attempt directory");
        fs::write(object.join(another.to_string()).join(STORAGE_FORMAT_FILE_BACKUP), b"other evidence")
            .await
            .expect("other attempt evidence");
        let current_dir = next.data_dir.expect("current data identity");
        let rejected = disk
            .delete_data_dir(
                RUSTFS_META_BUCKET,
                &format!("{OBJECT}/{current_dir}"),
                DeleteOptions {
                    recursive: true,
                    ..Default::default()
                },
            )
            .await
            .expect_err("cleanup must not touch any directory referenced by live metadata");
        assert!(matches!(rejected, DiskError::ConditionalDeleteConflict));
        disk.delete(
            RUSTFS_META_BUCKET,
            &format!("{OBJECT}/{attempt}/{STORAGE_FORMAT_FILE_BACKUP}"),
            DeleteOptions::default(),
        )
        .await
        .expect("owner may delete its exact, unreferenced backup file");
        assert!(!object.join(attempt.to_string()).join(STORAGE_FORMAT_FILE_BACKUP).exists());
        assert_eq!(
            fs::read(object.join(another.to_string()).join(STORAGE_FORMAT_FILE_BACKUP))
                .await
                .expect("other attempt backup remains"),
            b"other evidence"
        );
        assert_eq!(
            fs::read(object.join(STORAGE_FORMAT_FILE))
                .await
                .expect("current metadata remains"),
            current
        );
    }
}
