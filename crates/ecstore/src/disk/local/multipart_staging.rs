// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! A completion consumes its own hardlinks, never the authoritative upload.

use super::{LocalDisk, effective_durability, tmp_staging_mutation_path};
use crate::disk::{
    RUSTFS_META_MULTIPART_BUCKET, RUSTFS_META_TMP_BUCKET,
    error::{DiskError, Result},
    os,
};
use rustfs_filemeta::{FileInfo, FileInfoOpts, get_file_info};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock, Weak},
};
use uuid::Uuid;

type UploadLock = tokio::sync::RwLock<()>;

#[derive(Default)]
struct UploadRegistry {
    entries: std::sync::Mutex<HashMap<PathBuf, Weak<UploadEntry>>>,
}

struct UploadEntry {
    key: PathBuf,
    lock: Arc<UploadLock>,
    registry: Weak<UploadRegistry>,
    identity: Weak<UploadEntry>,
}

impl UploadEntry {
    fn new(key: PathBuf, registry: &Arc<UploadRegistry>) -> Arc<Self> {
        Arc::new_cyclic(|identity| Self {
            key,
            lock: Arc::new(UploadLock::new(())),
            registry: Arc::downgrade(registry),
            identity: identity.clone(),
        })
    }
}

impl Drop for UploadEntry {
    fn drop(&mut self) {
        let Some(registry) = self.registry.upgrade() else {
            return;
        };
        let mut entries = registry.entries.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        // A successor may have replaced a dead weak entry while this Drop
        // waited for the registry. Never remove another generation's lock.
        if entries.get(&self.key).is_some_and(|entry| entry.ptr_eq(&self.identity)) {
            entries.remove(&self.key);
        }
    }
}

impl UploadRegistry {
    fn entry(self: &Arc<Self>, key: PathBuf) -> Arc<UploadEntry> {
        let mut entries = self.entries.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(entry) = entries.get(&key).and_then(Weak::upgrade) {
            return entry;
        }
        let entry = UploadEntry::new(key.clone(), self);
        entries.insert(key, Arc::downgrade(&entry));
        entry
    }
}

static UPLOAD_LOCKS: LazyLock<Arc<UploadRegistry>> = LazyLock::new(Default::default);

struct UploadOwner<G> {
    // Drop the actual guard before allowing the registry identity to retire.
    _guard: G,
    _entry: Arc<UploadEntry>,
    _owner: Option<Arc<dyn Send + Sync>>,
}

fn canonical_relative_path(path: &str) -> bool {
    let path = path.strip_suffix('/').unwrap_or(path);
    path.is_empty()
        || (!Path::new(path).is_absolute()
            && !path.contains('\\')
            && path
                .split('/')
                .all(|component| !component.is_empty() && component != "." && component != ".."))
}

fn ascii_component_eq(first: &std::ffi::OsStr, second: &std::ffi::OsStr) -> bool {
    match (first.to_str(), second.to_str()) {
        (Some(first), Some(second)) => first.eq_ignore_ascii_case(second),
        _ => first == second,
    }
}

fn resolved_multipart_scope(root: &Path, volume: &str, path: &str) -> bool {
    let root = super::normalize_path_components(root);
    let (_, object) = super::build_local_disk_object_path(&root, volume, path);
    let object = super::normalize_path_components(object);
    let mut actual = object.components();
    for expected in root.components() {
        if !actual
            .next()
            .is_some_and(|component| ascii_component_eq(component.as_os_str(), expected.as_os_str()))
        {
            return false;
        }
    }
    actual
        .next()
        .and_then(|component| component.as_os_str().to_str())
        .is_some_and(|component| component.eq_ignore_ascii_case(crate::disk::RUSTFS_META_BUCKET))
        && actual
            .next()
            .and_then(|component| component.as_os_str().to_str())
            .is_some_and(|component| component.eq_ignore_ascii_case("multipart"))
}

/// Reject aliases of the physical MPU domain before its path resolver or any
/// lease lookup can observe a different key for the same upload.
pub(super) fn reject_noncanonical_scope(root: &Path, io_root: &Path, volume: &str, path: &str) -> Result<()> {
    if volume == RUSTFS_META_MULTIPART_BUCKET {
        return if canonical_relative_path(path) {
            Ok(())
        } else {
            Err(DiskError::ConditionalDeleteConflict)
        };
    }
    let mut parts = volume.split('/');
    let reserved = parts
        .next()
        .is_some_and(|part| part.eq_ignore_ascii_case(crate::disk::RUSTFS_META_BUCKET))
        && parts.next().is_none_or(|part| part.eq_ignore_ascii_case("multipart"));
    if !reserved && !volume.is_empty() && canonical_relative_path(volume) && canonical_relative_path(path) {
        // Canonical user buckets and TMP cannot cross this namespace boundary.
        return Ok(());
    }
    if resolved_multipart_scope(root, volume, path) || (root != io_root && resolved_multipart_scope(io_root, volume, path)) {
        return Err(DiskError::ConditionalDeleteConflict);
    }
    Ok(())
}

fn part_file_name(path: &str) -> bool {
    let Some(name) = Path::new(path).file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    if !name.get(..5).is_some_and(|prefix| prefix.eq_ignore_ascii_case("part.")) {
        return false;
    }
    let (number, suffix) = name[5..].split_once('.').unwrap_or((&name[5..], ""));
    if !number.parse::<usize>().is_ok_and(|number| number > 0) {
        return false;
    }
    suffix.is_empty()
        || suffix.eq_ignore_ascii_case("meta")
        || (suffix
            .get(..10)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("integrity."))
            && Uuid::parse_str(&suffix[10..]).is_ok_and(|generation| !generation.is_nil()))
}

pub(super) fn upload_scope_root(path: &str, part_path: bool) -> &str {
    let path = path.trim_end_matches('/');
    let mut prior_hash = false;
    let mut offset = 0;
    for component in path.split('/') {
        let token = component.find(['x', 'X']).map_or(component, |index| &component[..index]);
        if prior_hash && Uuid::parse_str(token).is_ok_and(|upload| !upload.is_nil()) {
            return &path[..offset + component.len()];
        }
        prior_hash = component.len() == 64 && component.bytes().all(|byte| byte.is_ascii_hexdigit());
        offset += component.len() + 1;
    }
    // Keep generic short fixtures and legacy exact-key cleanup compatible.
    // Production roots above cover every head, data directory and child path.
    if part_path
        && part_file_name(path)
        && let Some(parent) = Path::new(path)
            .parent()
            .and_then(Path::parent)
            .and_then(Path::to_str)
            .filter(|root| !root.is_empty())
    {
        return parent;
    }
    let name = Path::new(path).file_name().and_then(|name| name.to_str()).unwrap_or_default();
    if (name.eq_ignore_ascii_case("xl.meta") || Uuid::parse_str(name).is_ok_and(|directory| !directory.is_nil()))
        && let Some(parent) = Path::new(path)
            .parent()
            .and_then(Path::to_str)
            .filter(|root| !root.is_empty())
    {
        return parent;
    }
    path
}

/// Rechecked inside each actual filesystem callback while its SourceRW owner
/// is retained. Only the unique initial metadata write may create an upload.
#[derive(Clone)]
pub(super) struct MultipartSourceAdmission {
    source: PathBuf,
    base: PathBuf,
    publication_root: os::PublicationRoot,
    initial: bool,
}
impl MultipartSourceAdmission {
    pub(super) fn source(&self) -> &Path {
        &self.source
    }
    pub(super) fn is_initial(&self) -> bool {
        self.initial
    }
    pub(super) fn precondition(&self) -> Arc<dyn Fn() -> std::io::Result<()> + Send + Sync> {
        let admission = self.clone();
        Arc::new(move || admission.validate())
    }
    pub(super) fn validate(&self) -> std::io::Result<()> {
        super::multipart_source_state::validate_source_mutation(&self.source, &self.base, &self.publication_root, self.initial)
            .map_err(std::io::Error::other)
    }
}

/// The retirement record is native-only, even through raw file RPCs.
/// Durable source records are native-owned input to cold proof hydration.
/// Ordinary file RPCs may mutate parts, never manufacture these records.
pub(super) fn reject_native_control_write(root: &Path, io_root: &Path, volume: &str, path: &str) -> Result<()> {
    reject_noncanonical_scope(root, io_root, volume, path)?;
    reject_reserved_anchor(path)?;
    if volume == RUSTFS_META_MULTIPART_BUCKET
        && path
            .split('/')
            .any(|component| component.get(..5).is_some_and(|prefix| prefix.eq_ignore_ascii_case(".mpu-")))
    {
        return Err(DiskError::ConditionalDeleteConflict);
    }
    Ok(())
}

pub(super) fn reject_reserved_anchor(path: &str) -> Result<()> {
    if Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.eq_ignore_ascii_case(super::multipart_source_state::RETIREMENT_ANCHOR))
    {
        return Err(DiskError::ConditionalDeleteConflict);
    }
    Ok(())
}

pub(super) struct MultipartCommitStaging {
    pub(super) path: String,
    pub(super) lease: Arc<os::NamespaceMutationLease>,
    logical_path: PathBuf,
    io_path: PathBuf,
    owner: Option<Arc<dyn Send + Sync>>,
    cleaned: Arc<std::sync::atomic::AtomicBool>,
    fresh_source_head: Option<bytes::Bytes>,
}

impl MultipartCommitStaging {
    pub(super) fn source_owner(&self) -> Option<Arc<dyn Send + Sync>> {
        self.owner.clone()
    }
    pub(super) fn private_directory(&self, directory: Uuid) -> PathBuf {
        self.io_path.join(directory.to_string())
    }
    pub(super) fn cleanup_handle(&self) -> (PathBuf, Arc<std::sync::atomic::AtomicBool>) {
        (self.io_path.clone(), Arc::clone(&self.cleaned))
    }
    pub(super) fn fresh_source_head(&self) -> Option<bytes::Bytes> {
        self.fresh_source_head.clone()
    }
}

impl Drop for MultipartCommitStaging {
    fn drop(&mut self) {
        if self.cleaned.load(std::sync::atomic::Ordering::Acquire) {
            return;
        }
        let logical_path = self.logical_path.clone();
        let io_path = self.io_path.clone();
        let owner = self.owner.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                // Timed-out preparation/publication retains this same attempt
                // lease in its blocking closure. Cleanup must wait for it.
                let lease = os::acquire_namespace_mutation_lease_with_owner(&logical_path, owner).await;
                let _ = os::run_blocking_namespace_operation(lease, move || match std::fs::remove_dir_all(io_path) {
                    Ok(()) => Ok(()),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                    Err(error) => Err(error),
                })
                .await;
            });
        }
        // Runtime shutdown may leave this unique attempt for the existing tmp
        // sweeper. Its authoritative upload was never modified.
    }
}

impl LocalDisk {
    pub(crate) fn has_clean_active_multipart_source(&self, upload_or_part_path: &str, part_path: bool) -> bool {
        let root = upload_scope_root(upload_or_part_path, part_path);
        let Ok(source) = self.io_get_object_path(RUSTFS_META_MULTIPART_BUCKET, root) else {
            return false;
        };
        super::multipart_source_state::has_clean_active_multipart_source(&source)
    }

    pub(super) fn multipart_source_admission(
        &self,
        volume: &str,
        path: &str,
        part_path: bool,
        initial: bool,
    ) -> Result<Option<MultipartSourceAdmission>> {
        reject_noncanonical_scope(&self.root, self.io_root(), volume, path)?;
        if volume != RUSTFS_META_MULTIPART_BUCKET {
            return Ok(None);
        }
        Ok(Some(MultipartSourceAdmission {
            source: self.io_get_object_path(volume, upload_scope_root(path, part_path))?,
            base: self.io_root().to_path_buf(),
            publication_root: self.publication_root.clone(),
            initial,
        }))
    }

    pub(super) async fn multipart_source_owner(
        &self,
        volume: &str,
        path: &str,
        part_path: bool,
        exclusive: bool,
        owner: Option<Arc<dyn Send + Sync>>,
    ) -> Result<Option<Arc<dyn Send + Sync>>> {
        reject_noncanonical_scope(&self.root, self.io_root(), volume, path)?;
        if volume != RUSTFS_META_MULTIPART_BUCKET {
            return Ok(owner);
        }
        let root = upload_scope_root(path, part_path);
        // Only the internal relative scope is folded: the configured disk root
        // keeps its physical spelling. Case-sensitive filesystems may serialize
        // two case-only upload IDs; insensitive filesystems must share one lock.
        let key = self.root.join(RUSTFS_META_MULTIPART_BUCKET).join(root.to_ascii_lowercase());
        let entry = UPLOAD_LOCKS.entry(key);
        // Upload scope precedes the private attempt, object metadata and
        // publication leases. Independent parts retain shared ownership.
        Ok(Some(if exclusive {
            Arc::new(UploadOwner {
                _guard: Arc::clone(&entry.lock).write_owned().await,
                _entry: entry,
                _owner: owner,
            })
        } else {
            Arc::new(UploadOwner {
                _guard: Arc::clone(&entry.lock).read_owned().await,
                _entry: entry,
                _owner: owner,
            })
        }))
    }

    pub(super) async fn prepare_multipart_commit_staging(
        &self,
        source_path: &str,
        incoming: &FileInfo,
        destination_volume: &str,
        owner: Option<Arc<dyn Send + Sync>>,
    ) -> Result<MultipartCommitStaging> {
        incoming.validate_for_metadata_read()?;
        let final_dir = incoming.data_dir.filter(|dir| !dir.is_nil()).ok_or(DiskError::FileCorrupt)?;
        let owner = self
            .multipart_source_owner(RUSTFS_META_MULTIPART_BUCKET, source_path, false, true, owner)
            .await?;
        let attempt = Uuid::new_v4().to_string();
        let logical_path = tmp_staging_mutation_path(&self.root, &attempt)?;
        let io_path = self.io_get_object_path(RUSTFS_META_TMP_BUCKET, &attempt)?;
        // The attempt root is exactly the namespace used by the tmp sweeper.
        let lease = os::acquire_namespace_mutation_lease_with_owner(&logical_path, owner.clone()).await;
        let mut staging = MultipartCommitStaging {
            path: attempt,
            lease,
            logical_path,
            io_path,
            owner,
            cleaned: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            fresh_source_head: None,
        };
        let canonical_metadata = self.io_get_object_path(RUSTFS_META_MULTIPART_BUCKET, &format!("{source_path}/xl.meta"))?;
        let source_object = self.io_get_object_path(RUSTFS_META_MULTIPART_BUCKET, source_path)?;
        let private_object = staging.io_path.clone();
        let base = self.io_root().to_path_buf();
        let publication_root = self.publication_root.clone();
        let source_path = source_path.to_owned();
        let incoming = incoming.clone();
        let sync_directory = effective_durability(destination_volume).syncs_data_shards();
        let fresh_head = os::run_cancellable_blocking_namespace_operation(Arc::clone(&staging.lease), move |cancelled| {
            if cancelled() {
                return Err(std::io::Error::new(std::io::ErrorKind::Interrupted, "multipart staging cancelled"));
            }
            let cached_head = super::multipart_source_state::get_active_multipart_source_head(&source_object);
            let (raw, source, from_active_cache) = if let Some((raw, source)) = cached_head {
                (raw, source, true)
            } else {
                let raw = os::read_multipart_source_metadata(&canonical_metadata, &base, &publication_root).map_err(|error| {
                    // A retired source has no authority for this delayed completion.
                    // Preserve confinement and other I/O failures as their real errors.
                    if error.kind() == std::io::ErrorKind::NotFound {
                        DiskError::FileNotFound.into()
                    } else {
                        error
                    }
                })?;
                let source = get_file_info(
                    &raw,
                    RUSTFS_META_MULTIPART_BUCKET,
                    &source_path,
                    "",
                    FileInfoOpts {
                        data: false,
                        include_free_versions: false,
                        include_part_checksums: true,
                    },
                )
                .map_err(std::io::Error::other)?;
                (bytes::Bytes::from(raw), source, false)
            };
            let source_dir = source
                .data_dir
                .filter(|dir| !dir.is_nil())
                .ok_or_else(|| std::io::Error::other("multipart source data directory is missing"))?;
            if source.deleted
                || source.erasure.data_blocks != incoming.erasure.data_blocks
                || source.erasure.parity_blocks != incoming.erasure.parity_blocks
                || source.erasure.block_size != incoming.erasure.block_size
                || source.erasure.index != incoming.erasure.index
                || source.erasure.distribution != incoming.erasure.distribution
            {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "multipart source layout changed"));
            }
            let _attempt = os::mkdir_all_below_existing_base_std(&private_object, &base, &publication_root)?;
            if incoming.data.is_none() && !incoming.is_remote() {
                let source_data_dir = source_object.join(source_dir.to_string());
                let mut pinned_source_data = None;
                let mut names = Vec::with_capacity(incoming.parts.len().saturating_mul(3));
                for part in &incoming.parts {
                    if cancelled() {
                        return Err(std::io::Error::new(std::io::ErrorKind::Interrupted, "multipart staging cancelled"));
                    }
                    let meta_name = format!("part.{}.meta", part.number);
                    let actual = if let Some(cached_part) =
                        super::multipart_source_state::get_active_multipart_source_part(&source_object, part.number)
                    {
                        cached_part
                    } else {
                        if pinned_source_data.is_none() {
                            pinned_source_data =
                                Some(os::pin_multipart_source_directory(&source_data_dir, &base, &publication_root)?);
                        }
                        let bytes = os::read_multipart_source_metadata_in_pinned(
                            pinned_source_data.as_ref().expect("pinned source data directory"),
                            &source_data_dir,
                            std::ffi::OsStr::new(&meta_name),
                            &base,
                            &publication_root,
                        )?;
                        rustfs_filemeta::ObjectPartInfo::unmarshal(&bytes).map_err(std::io::Error::other)?
                    };
                    let normalized = i64::try_from(actual.size)
                        .map_err(std::io::Error::other)?
                        .max(actual.actual_size);
                    if actual.number != part.number
                        || actual.size != part.size
                        || actual.etag != part.etag
                        || actual.mod_time != part.mod_time
                        || actual.index != part.index
                        || actual.checksums != part.checksums
                        || actual.integrity != part.integrity
                        || actual.error.is_some()
                        || (actual.actual_size != part.actual_size
                            && !(actual.actual_size >= 0 && normalized == part.actual_size))
                    {
                        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "multipart part generation changed"));
                    }
                    names.push(format!("part.{}", part.number));
                    // Preserve the native completed-MPU directory format. This
                    // immutable, validated generation record is distinct from
                    // the upload's mutable top-level xl.meta.
                    names.push(meta_name);
                    if let Some(integrity) = &part.integrity {
                        names.push(integrity.file_name());
                    }
                }
                os::hardlink_multipart_source_files(
                    &source_data_dir,
                    &private_object.join(final_dir.to_string()),
                    &names,
                    &base,
                    &publication_root,
                    sync_directory,
                    cancelled,
                )?;
            }
            let has_state = !from_active_cache
                && (source_object.join(".mpu-source-state").exists()
                    || source_object.join(".mpu-recovery").exists()
                    || source_object.join(".mpu-completions").exists());
            Ok((!has_state).then_some(raw))
        })
        .await
        .map_err(DiskError::from)?;
        staging.fresh_source_head = fresh_head;
        Ok(staging)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::endpoint::Endpoint;
    use rustfs_filemeta::FileMeta;

    async fn fixture() -> (tempfile::TempDir, LocalDisk, FileInfo, Vec<u8>) {
        let root = tempfile::tempdir().expect("temporary disk");
        let endpoint = Endpoint::try_from(root.path().to_str().expect("UTF-8 temporary disk")).expect("local endpoint");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk");
        let mut upload = FileInfo::new("upload", 2, 2);
        upload.erasure.index = 1;
        upload.data_dir = Some(Uuid::new_v4());
        upload.mod_time = Some(time::OffsetDateTime::now_utc());
        let mut metadata = FileMeta::new();
        metadata.add_version(upload.clone()).expect("source upload metadata");
        let metadata = metadata.marshal_msg().expect("source upload xl.meta");
        let source = root.path().join(RUSTFS_META_MULTIPART_BUCKET).join("upload");
        std::fs::create_dir_all(source.join(upload.data_dir.expect("upload data directory").to_string()))
            .expect("upload directories");
        std::fs::write(source.join("xl.meta"), &metadata).expect("source upload head");
        let source_data = source.join(upload.data_dir.expect("upload data directory").to_string());
        std::fs::write(source_data.join("part.1"), b"immutable shard").expect("source shard");
        let mut completed = upload;
        completed.data_dir = Some(Uuid::new_v4());
        completed.size = 15;
        completed.add_object_part(1, "etag".into(), 15, completed.mod_time, 15, None, None);
        std::fs::write(
            source_data.join("part.1.meta"),
            completed.parts[0].marshal_msg().expect("source part metadata"),
        )
        .expect("source part generation");
        (root, disk, completed, metadata)
    }

    #[tokio::test]
    async fn multipart_source_private_records_reject_public_writers_before_source_lease() {
        use crate::disk::DiskAPI;
        let (_root, disk, completed, original) = fixture().await;
        let source = disk
            .io_get_object_path(RUSTFS_META_MULTIPART_BUCKET, "upload")
            .expect("source root");
        let owner = disk
            .multipart_source_owner(RUSTFS_META_MULTIPART_BUCKET, "upload", false, true, None)
            .await
            .expect("hold source owner");
        for suffix in [
            ".mpu-source-state",
            ".MPU-RECOVERY/round/ready.msgpack",
            ".mpu-completions/owner/original.xl.meta",
            ".mpu-prepared-fake",
        ] {
            let path = format!("upload/{suffix}");
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                disk.write_all(RUSTFS_META_MULTIPART_BUCKET, &path, bytes::Bytes::from_static(b"forged native authority")),
            )
            .await
            .expect("reserved rejection must precede source lease wait");
            assert!(matches!(result, Err(DiskError::ConditionalDeleteConflict)), "reserved input: {suffix}");
            assert!(!source.join(suffix).exists());
        }
        let cas = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            disk.compare_and_update_file(
                RUSTFS_META_MULTIPART_BUCKET,
                "upload/.mpu-source-state",
                None,
                Some(bytes::Bytes::from_static(b"forged")),
            ),
        )
        .await
        .expect("CAS rejection precedes lease");
        assert!(matches!(cas, Err(DiskError::ConditionalDeleteConflict)));
        assert_eq!(
            std::fs::read(source.join("xl.meta")).expect("head after rejected public writes"),
            original
        );
        assert!(!source.join(".mpu-recovery").exists());
        assert!(!source.join(".mpu-completions").exists());
        drop(owner);
        let source_info = get_file_info(
            &original,
            RUSTFS_META_MULTIPART_BUCKET,
            "upload",
            "",
            FileInfoOpts {
                data: false,
                include_free_versions: false,
                include_part_checksums: true,
            },
        )
        .expect("real source FI");
        let part = format!("upload/{}/part.1", source_info.data_dir.expect("real source DD"));
        let part_before = disk
            .read_all(RUSTFS_META_MULTIPART_BUCKET, &part)
            .await
            .expect("existing source part");
        let _ = completed;
        assert!(matches!(
            disk.rename_file(RUSTFS_META_MULTIPART_BUCKET, &part, RUSTFS_META_TMP_BUCKET, "stolen")
                .await,
            Err(DiskError::ConditionalDeleteConflict)
        ));
        assert_eq!(
            disk.read_all(RUSTFS_META_MULTIPART_BUCKET, &part)
                .await
                .expect("rejected raw source consume retains payload"),
            part_before
        );
    }

    #[tokio::test]
    async fn multipart_source_child_fresh_metadata_and_raw_rename_cannot_initialize_upload() {
        use crate::disk::DiskAPI;
        let (_root, disk, mut fi, _original) = fixture().await;
        let upload = format!("{}/{}", "a".repeat(64), Uuid::new_v4());
        let child = format!("{upload}/{}", Uuid::new_v4());
        fi.fresh = true;
        fi.parts.clear();
        fi.size = 0;
        fi.data = None;
        assert!(
            disk.write_metadata("", RUSTFS_META_MULTIPART_BUCKET, &child, fi)
                .await
                .is_err()
        );
        let root = disk
            .io_get_object_path(RUSTFS_META_MULTIPART_BUCKET, &upload)
            .expect("production source root");
        assert!(!root.exists(), "a child FI cannot masquerade as the unique upload Init");
        disk.write_all(RUSTFS_META_TMP_BUCKET, "raw-source", bytes::Bytes::from_static(b"source"))
            .await
            .expect("ordinary TMP write");
        assert!(matches!(
            disk.rename_file(
                RUSTFS_META_TMP_BUCKET,
                "raw-source",
                RUSTFS_META_MULTIPART_BUCKET,
                &format!("{child}/part.1")
            )
            .await,
            Err(DiskError::ConditionalDeleteConflict)
        ));
        assert!(!root.exists());
        assert_eq!(
            disk.read_all(RUSTFS_META_TMP_BUCKET, "raw-source")
                .await
                .expect("rejected rename retains source"),
            bytes::Bytes::from_static(b"source")
        );
    }

    #[tokio::test]
    async fn multipart_source_active_part_uses_tmp_and_raw_streams_cannot_recreate_completed_root() {
        use crate::disk::{DiskAPI, PartTransactionAction};
        let (_root, disk, completed, original) = fixture().await;
        let temporary = disk.io_get_bucket_path(RUSTFS_META_TMP_BUCKET).expect("TMP path");
        std::fs::create_dir_all(&temporary).expect("TMP bucket");
        let source_info = get_file_info(
            &original,
            RUSTFS_META_MULTIPART_BUCKET,
            "upload",
            "",
            FileInfoOpts {
                data: false,
                include_free_versions: false,
                include_part_checksums: true,
            },
        )
        .expect("active source FI");
        let destination = format!("upload/{}/part.1", source_info.data_dir.expect("source part DD"));
        let metadata = bytes::Bytes::from(completed.parts[0].marshal_msg().expect("selected part"));
        disk.write_all(
            RUSTFS_META_TMP_BUCKET,
            "part-source/part.1",
            bytes::Bytes::from_static(b"immutable shard"),
        )
        .await
        .expect("production TMP part write");
        disk.prepare_part_transaction(
            RUSTFS_META_TMP_BUCKET,
            "part-source/part.1",
            RUSTFS_META_MULTIPART_BUCKET,
            &destination,
            metadata.clone(),
        )
        .await
        .expect("active upload prepares part");
        disk.rename_part(
            RUSTFS_META_TMP_BUCKET,
            "part-source/part.1",
            RUSTFS_META_MULTIPART_BUCKET,
            &destination,
            metadata,
        )
        .await
        .expect("active upload publishes part");
        disk.settle_part_transaction(RUSTFS_META_MULTIPART_BUCKET, &destination, PartTransactionAction::Commit)
            .await
            .expect("part settles");
        let source = disk
            .io_get_object_path(RUSTFS_META_MULTIPART_BUCKET, "upload")
            .expect("source root");
        assert_eq!(std::fs::read(source.join("xl.meta")).expect("source head remains active"), original);
        assert_eq!(
            disk.read_all(RUSTFS_META_MULTIPART_BUCKET, &destination)
                .await
                .expect("published part"),
            bytes::Bytes::from_static(b"immutable shard")
        );
        std::fs::remove_dir_all(&source).expect("completed upload cleanup");
        assert!(matches!(
            disk.create_file("", RUSTFS_META_MULTIPART_BUCKET, &destination, 15).await,
            Err(DiskError::ConditionalDeleteConflict)
        ));
        assert!(matches!(
            disk.append_file(RUSTFS_META_MULTIPART_BUCKET, &destination).await,
            Err(DiskError::ConditionalDeleteConflict)
        ));
        assert!(
            disk.write_all(RUSTFS_META_MULTIPART_BUCKET, &destination, bytes::Bytes::from_static(b"late"))
                .await
                .is_err()
        );
        assert!(!source.exists(), "late byte/stream writers must not recreate the completed upload");
        let reserved = format!("stage/{}/{}", Uuid::new_v4(), super::super::multipart_source_state::RETIREMENT_ANCHOR);
        assert!(matches!(
            disk.write_all(RUSTFS_META_TMP_BUCKET, &reserved, bytes::Bytes::from_static(b"forged"))
                .await,
            Err(DiskError::ConditionalDeleteConflict)
        ));
        assert!(!temporary.join(reserved).exists());
    }

    #[tokio::test]
    async fn multipart_source_staging_private_attempt_preserves_original_head_and_data() {
        let (root, disk, completed, original) = fixture().await;
        let staging = disk
            .prepare_multipart_commit_staging("upload", &completed, "bucket", None)
            .await
            .expect("private completion staging");
        let shard = staging
            .io_path
            .join(completed.data_dir.expect("fresh completion directory").to_string())
            .join("part.1");
        assert_eq!(std::fs::read(&shard).expect("private shard"), b"immutable shard");
        std::fs::write(staging.io_path.join("xl.meta"), b"generated destination metadata")
            .expect("independent prepared metadata");
        let source = root.path().join(RUSTFS_META_MULTIPART_BUCKET).join("upload");
        assert_eq!(std::fs::read(source.join("xl.meta")).expect("authoritative upload head"), original);
        let source_info = get_file_info(
            &original,
            RUSTFS_META_MULTIPART_BUCKET,
            "upload",
            "",
            FileInfoOpts {
                data: false,
                include_free_versions: false,
                include_part_checksums: true,
            },
        )
        .expect("source metadata");
        let source_sidecar = source
            .join(source_info.data_dir.expect("original sidecar directory").to_string())
            .join("part.1.meta");
        let staged_sidecar = shard.with_file_name("part.1.meta");
        let sidecar_bytes = std::fs::read(&source_sidecar).expect("authoritative part generation sidecar");
        assert_eq!(std::fs::read(&staged_sidecar).expect("private native part sidecar"), sidecar_bytes);
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let source = std::fs::metadata(&source_sidecar).expect("authoritative sidecar inode");
            let staged = std::fs::metadata(&staged_sidecar).expect("private sidecar inode");
            assert_eq!(
                (source.dev(), source.ino()),
                (staged.dev(), staged.ino()),
                "native sidecars should use immutable hardlinks"
            );
        }
        std::fs::remove_dir_all(&staging.io_path).expect("failed private attempt cleanup");
        assert_eq!(
            std::fs::read(&source_sidecar).expect("authoritative sidecar must survive private cleanup"),
            sidecar_bytes
        );
        assert_eq!(
            std::fs::read(
                source
                    .join(source_info.data_dir.expect("original directory").to_string())
                    .join("part.1")
            )
            .expect("source shard remains"),
            b"immutable shard"
        );
    }

    #[tokio::test]
    async fn multipart_source_staging_failed_batch_keeps_original_upload_retryable() {
        let (root, disk, mut completed, original) = fixture().await;
        completed.add_object_part(2, "missing".into(), 1, completed.mod_time, 1, None, None);
        completed.size += 1;
        assert!(
            disk.prepare_multipart_commit_staging("upload", &completed, "bucket", None)
                .await
                .is_err()
        );
        let source = root.path().join(RUSTFS_META_MULTIPART_BUCKET).join("upload");
        assert_eq!(std::fs::read(source.join("xl.meta")).expect("source after failed preparation"), original);
        let source_info = get_file_info(
            &original,
            RUSTFS_META_MULTIPART_BUCKET,
            "upload",
            "",
            FileInfoOpts {
                data: false,
                include_free_versions: false,
                include_part_checksums: true,
            },
        )
        .expect("source metadata");
        assert_eq!(
            std::fs::read(
                source
                    .join(source_info.data_dir.expect("original directory").to_string())
                    .join("part.1")
            )
            .expect("source part after failed batch"),
            b"immutable shard"
        );
    }

    #[tokio::test]
    async fn multipart_source_staging_owns_the_exact_tmp_sweeper_namespace() {
        let (_root, disk, completed, _) = fixture().await;
        let staging = disk
            .prepare_multipart_commit_staging("upload", &completed, "bucket", None)
            .await
            .expect("private completion staging");
        let logical = tmp_staging_mutation_path(&disk.root, &staging.path).expect("tmp sweeper namespace");
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(20),
                os::acquire_namespace_mutation_lease_with_owner(&logical, None)
            )
            .await
            .is_err(),
            "tmp cleanup must not enter an active completion attempt"
        );
        drop(staging);
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            os::acquire_namespace_mutation_lease_with_owner(&logical, None),
        )
        .await
        .expect("completed attempt should release tmp cleanup ownership");
    }

    #[tokio::test]
    async fn multipart_source_staging_rejects_part_generation_changed_after_coordinator_read() {
        let (root, disk, mut completed, original) = fixture().await;
        completed.parts[0].etag = "stale-coordinator-etag".into();
        assert!(
            disk.prepare_multipart_commit_staging("upload", &completed, "bucket", None)
                .await
                .is_err()
        );
        assert_eq!(
            std::fs::read(root.path().join(RUSTFS_META_MULTIPART_BUCKET).join("upload/xl.meta")).expect("source upload head"),
            original
        );
    }

    #[tokio::test]
    async fn multipart_source_staging_part_readers_remain_parallel_and_exclude_completion() {
        let (_root, disk, completed, _) = fixture().await;
        let first = disk
            .multipart_source_owner(RUSTFS_META_MULTIPART_BUCKET, "upload/data/part.1", true, false, None)
            .await
            .expect("first part reader");
        let second = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            disk.multipart_source_owner(RUSTFS_META_MULTIPART_BUCKET, "upload/data/part.2", true, false, None),
        )
        .await
        .expect("independent part publication must remain parallel")
        .expect("second part reader");
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(20),
                disk.prepare_multipart_commit_staging("upload", &completed, "bucket", None)
            )
            .await
            .is_err()
        );
        drop(first);
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(20),
                disk.prepare_multipart_commit_staging("upload", &completed, "bucket", None)
            )
            .await
            .is_err()
        );
        drop(second);
        disk.prepare_multipart_commit_staging("upload", &completed, "bucket", None)
            .await
            .expect("completion may capture the stable source after both part owners drain");
    }

    fn registry_len(registry: &UploadRegistry) -> usize {
        registry.entries.lock().expect("registry entries").len()
    }

    #[test]
    fn multipart_source_staging_registry_retires_only_its_own_key_generation() {
        let registry = Arc::new(UploadRegistry::default());
        let held = (0..128)
            .map(|index| registry.entry(PathBuf::from(format!("disk/upload-{index}"))))
            .collect::<Vec<_>>();
        assert_eq!(registry_len(&registry), held.len());
        let key = held[0].key.clone();
        let successor = UploadEntry::new(key.clone(), &registry);
        registry
            .entries
            .lock()
            .expect("replace a retiring weak generation")
            .insert(key.clone(), Arc::downgrade(&successor));
        let mut held = held;
        drop(held.remove(0));
        let observed = registry.entry(key);
        assert!(Arc::ptr_eq(&observed, &successor), "late old Drop must not remove a successor entry");
        assert_eq!(registry_len(&registry), 128);
        drop(observed);
        drop(successor);
        assert_eq!(registry_len(&registry), 127);
        for entry in &held {
            let observed = registry.entry(entry.key.clone());
            assert!(Arc::ptr_eq(&observed, entry), "retiring one key must not replace unrelated active locks");
        }
        drop(held);
        assert_eq!(registry_len(&registry), 0, "inactive keys must retire without a registry sweep");
    }

    #[tokio::test]
    async fn multipart_source_staging_registry_cancelled_waiter_keeps_active_owner_and_retires() {
        let registry = Arc::new(UploadRegistry::default());
        let key = PathBuf::from("disk/upload");
        let entry = registry.entry(key.clone());
        let weak = Arc::downgrade(&entry);
        let owner = UploadOwner {
            _guard: Arc::clone(&entry.lock).write_owned().await,
            _entry: entry,
            _owner: None,
        };
        let reached = Arc::new(tokio::sync::Notify::new());
        let waiter = tokio::spawn({
            let registry = Arc::clone(&registry);
            let reached = Arc::clone(&reached);
            async move {
                let entry = registry.entry(key);
                reached.notify_one();
                let guard = Arc::clone(&entry.lock).read_owned().await;
                UploadOwner {
                    _guard: guard,
                    _entry: entry,
                    _owner: None,
                }
            }
        });
        reached.notified().await;
        assert_eq!(weak.strong_count(), 2, "waiting acquisition must retain its registry generation");
        waiter.abort();
        match waiter.await {
            Err(error) => assert!(error.is_cancelled()),
            Ok(_) => panic!("waiter should be cancelled"),
        }
        assert_eq!(
            weak.strong_count(),
            1,
            "cancelled waiter must release its entry without retiring the active owner"
        );
        assert_eq!(registry_len(&registry), 1);
        drop(owner);
        assert_eq!(registry_len(&registry), 0);
        let next = registry.entry(PathBuf::from("disk/upload"));
        assert!(!weak.ptr_eq(&Arc::downgrade(&next)), "later reuse must install a fresh entry identity");
    }

    #[test]
    fn multipart_source_staging_scope_covers_all_production_upload_children() {
        let root = format!("{}/{}x17", "a".repeat(64), Uuid::new_v4());
        let data = Uuid::new_v4();
        for prefix in [String::new(), "data-movement/".to_string()] {
            let root = format!("{prefix}{root}");
            for suffix in [
                String::new(),
                "/xl.meta".to_string(),
                format!("/{data}/"),
                format!("/{data}/part.1"),
                format!("/{data}/part.1.meta"),
                format!("/{data}/part.1.integrity.{}", Uuid::new_v4()),
            ] {
                let child = format!("{root}{suffix}");
                assert_eq!(upload_scope_root(&child, true), root);
                assert_eq!(upload_scope_root(&child, false), root);
                let upper = child.to_ascii_uppercase();
                assert_eq!(upload_scope_root(&upper, true).to_ascii_lowercase(), root);
            }
        }
        assert_eq!(upload_scope_root(&"a".repeat(64), true), "a".repeat(64));
        assert_eq!(upload_scope_root("upload/data/part.1.meta", true), "upload");
    }

    async fn production_fixture() -> (tempfile::TempDir, LocalDisk, FileInfo, Vec<u8>, String) {
        let (root, disk, completed, metadata) = fixture().await;
        let upload = format!("{}/{}x17", "a".repeat(64), Uuid::new_v4());
        let destination = root.path().join(RUSTFS_META_MULTIPART_BUCKET).join(&upload);
        std::fs::create_dir_all(destination.parent().expect("production hash prefix")).expect("hash prefix");
        std::fs::rename(root.path().join(RUSTFS_META_MULTIPART_BUCKET).join("upload"), &destination)
            .expect("production upload placement");
        std::fs::create_dir_all(root.path().join("bucket")).expect("destination bucket");
        std::fs::create_dir_all(root.path().join(RUSTFS_META_TMP_BUCKET)).expect("temporary volume");
        (root, disk, completed, metadata, upload)
    }

    #[tokio::test]
    async fn multipart_source_staging_alias_mutators_reject_before_io_or_lease_and_preserve_source() {
        use crate::disk::{DeleteOptions, DiskAPI as _, PartTransactionAction};
        let (_root, disk, mut completed, metadata, upload) = production_fixture().await;
        let source = disk
            .get_object_path(RUSTFS_META_MULTIPART_BUCKET, &upload)
            .expect("canonical source upload path");
        let original = get_file_info(
            &metadata,
            RUSTFS_META_MULTIPART_BUCKET,
            &upload,
            "",
            FileInfoOpts {
                data: false,
                include_free_versions: false,
                include_part_checksums: true,
            },
        )
        .expect("source FI");
        let data = original.data_dir.expect("original upload directory");
        completed.data_dir = Some(data); // Legacy payload would consume the real source without the guard.
        let part = format!("{data}/part.1");
        let part_meta = completed.parts[0].marshal_msg().expect("part metadata");
        let aliases = vec![
            (format!("{RUSTFS_META_MULTIPART_BUCKET}/."), upload.clone()),
            (format!("{RUSTFS_META_MULTIPART_BUCKET}/"), upload.clone()),
            (format!("{RUSTFS_META_MULTIPART_BUCKET}/{upload}"), String::new()),
            (crate::disk::RUSTFS_META_BUCKET.to_string(), format!("multipart/{upload}")),
            (String::new(), format!("{RUSTFS_META_MULTIPART_BUCKET}/{upload}")),
            (source.to_str().expect("absolute upload path").to_string(), String::new()),
            (
                disk.io_get_object_path(RUSTFS_META_MULTIPART_BUCKET, &upload)
                    .expect("descriptor-rooted source")
                    .to_str()
                    .expect("UTF-8 descriptor path")
                    .to_string(),
                String::new(),
            ),
            (
                disk.root.to_str().expect("canonical absolute disk root").to_string(),
                format!("{RUSTFS_META_MULTIPART_BUCKET}/{upload}"),
            ),
            (RUSTFS_META_MULTIPART_BUCKET.to_ascii_uppercase(), upload.clone()),
            (RUSTFS_META_MULTIPART_BUCKET.to_string(), format!("./{upload}")),
            (RUSTFS_META_MULTIPART_BUCKET.to_string(), format!("{upload}/.")),
        ];
        let _owner = disk
            .multipart_source_owner(RUSTFS_META_MULTIPART_BUCKET, &upload, false, true, None)
            .await
            .expect("exclusive source scope");
        let destination = disk.get_object_path("bucket", "object").expect("destination metadata scope");
        let _destination_owner = os::acquire_metadata_mutation_lease(&destination, None).await;
        for (volume, path) in aliases {
            let expect_rejected = |result: crate::disk::error::Result<()>| {
                assert!(
                    matches!(result, Err(DiskError::ConditionalDeleteConflict)),
                    "alias must fail in pure preflight: {volume}/{path}"
                )
            };
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                disk.rename_data(&volume, &path, completed.clone(), "bucket", "object"),
            )
            .await
            .expect("alias rename must reject before waiting for the source owner");
            assert!(matches!(result, Err(DiskError::ConditionalDeleteConflict)));
            let alias_part = if path.is_empty() {
                part.clone()
            } else {
                format!("{path}/{part}")
            };
            expect_rejected(
                tokio::time::timeout(
                    std::time::Duration::from_secs(1),
                    disk.rename_part(
                        RUSTFS_META_TMP_BUCKET,
                        "missing-part",
                        &volume,
                        &alias_part,
                        bytes::Bytes::copy_from_slice(&part_meta),
                    ),
                )
                .await
                .expect("alias part must reject before IO/lease"),
            );
            expect_rejected(
                tokio::time::timeout(
                    std::time::Duration::from_secs(1),
                    disk.prepare_part_transaction(
                        RUSTFS_META_TMP_BUCKET,
                        "missing-part",
                        &volume,
                        &alias_part,
                        bytes::Bytes::copy_from_slice(&part_meta),
                    ),
                )
                .await
                .expect("alias prepare must reject before IO/lease"),
            );
            expect_rejected(
                tokio::time::timeout(
                    std::time::Duration::from_secs(1),
                    disk.settle_part_transaction(&volume, &alias_part, PartTransactionAction::Rollback),
                )
                .await
                .expect("alias settle must reject before IO/lease"),
            );
            expect_rejected(
                tokio::time::timeout(
                    std::time::Duration::from_secs(1),
                    disk.delete(
                        &volume,
                        &path,
                        DeleteOptions {
                            recursive: true,
                            ..Default::default()
                        },
                    ),
                )
                .await
                .expect("alias abort must reject before IO/lease"),
            );
            expect_rejected(
                disk.rename_part(
                    &volume,
                    &alias_part,
                    RUSTFS_META_TMP_BUCKET,
                    "out",
                    bytes::Bytes::copy_from_slice(&part_meta),
                )
                .await,
            );
            expect_rejected(
                disk.prepare_part_transaction(
                    &volume,
                    &alias_part,
                    RUSTFS_META_TMP_BUCKET,
                    "out",
                    bytes::Bytes::copy_from_slice(&part_meta),
                )
                .await,
            );
            assert_eq!(std::fs::read(source.join("xl.meta")).expect("source head after rejection"), metadata);
            assert_eq!(
                std::fs::read(source.join(&part)).expect("source data after rejection"),
                b"immutable shard"
            );
            assert_eq!(
                std::fs::read(source.join(format!("{part}.meta"))).expect("source part metadata after rejection"),
                part_meta
            );
        }
    }

    #[tokio::test]
    async fn multipart_source_staging_directory_and_case_aliases_share_production_rw_scope() {
        use crate::disk::DiskAPI as _;
        let (_root, disk, completed, metadata, upload) = production_fixture().await;
        let original = get_file_info(
            &metadata,
            RUSTFS_META_MULTIPART_BUCKET,
            &upload,
            "",
            FileInfoOpts {
                data: false,
                include_free_versions: false,
                include_part_checksums: true,
            },
        )
        .expect("original upload FI");
        let data = original.data_dir.expect("original data directory");
        let _owner = disk
            .multipart_source_owner(RUSTFS_META_MULTIPART_BUCKET, &upload, false, true, None)
            .await
            .expect("exclusive source scope");
        for path in [
            format!("{upload}/{data}/"),
            format!("{upload}/{data}/part.1"),
            format!("{upload}/{data}/part.1.meta"),
            format!("{upload}/xl.meta"),
            format!("{upload}/{data}/").to_ascii_uppercase(),
        ] {
            assert!(
                tokio::time::timeout(
                    std::time::Duration::from_millis(20),
                    disk.multipart_source_owner(RUSTFS_META_MULTIPART_BUCKET, &path, true, false, None)
                )
                .await
                .is_err(),
                "every source child must share its whole-upload write exclusion: {path}"
            );
        }
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(20),
                disk.rename_part(
                    RUSTFS_META_TMP_BUCKET,
                    "missing-source/",
                    RUSTFS_META_MULTIPART_BUCKET,
                    &format!("{upload}/{data}/"),
                    bytes::Bytes::new()
                )
            )
            .await
            .is_err(),
            "raw directory publication must wait before touching files"
        );
        let _ = completed;
    }

    #[tokio::test]
    async fn multipart_source_staging_empty_sha_cleanup_keeps_legacy_directory_semantics() {
        use crate::disk::{DeleteOptions, DiskAPI as _};
        let (root, disk, _, _) = fixture().await;
        let sha = "b".repeat(64);
        let directory = root.path().join(RUSTFS_META_MULTIPART_BUCKET).join(&sha);
        std::fs::create_dir_all(&directory).expect("empty hash container");
        disk.delete(RUSTFS_META_MULTIPART_BUCKET, &sha, DeleteOptions::default())
            .await
            .expect("non-part empty hash cleanup must remain supported");
        assert!(!directory.exists(), "a per-upload safety guard must not strand empty hash directories");
    }

    #[tokio::test]
    async fn multipart_source_active_head_cache_populates_and_invalidates_across_part_and_delete_paths() {
        use crate::disk::{DiskAPI as _, PartTransactionAction, ReadOptions};
        let (_root, disk, completed, _) = fixture().await;
        std::fs::create_dir_all(disk.io_get_bucket_path(RUSTFS_META_TMP_BUCKET).expect("TMP path")).expect("TMP bucket");
        let upload = format!("{}/{}", "c".repeat(64), Uuid::new_v4());
        let data_dir = Uuid::new_v4();
        let mut init_fi = FileInfo::new(&upload, 2, 2);
        init_fi.erasure.index = 1;
        init_fi.data_dir = Some(data_dir);
        init_fi.fresh = true;
        init_fi.mod_time = Some(time::OffsetDateTime::now_utc());

        disk.write_metadata("", RUSTFS_META_MULTIPART_BUCKET, &upload, init_fi.clone())
            .await
            .expect("fresh multipart upload metadata");
        assert!(disk.has_clean_active_multipart_source(&upload, false));

        let read_fi = disk
            .read_version("", RUSTFS_META_MULTIPART_BUCKET, &upload, "", &ReadOptions::default())
            .await
            .expect("cached read_version");
        assert_eq!(read_fi.data_dir, Some(data_dir));

        let destination = format!("{upload}/{data_dir}/part.1");
        let part_meta = bytes::Bytes::from(completed.parts[0].marshal_msg().expect("part meta"));
        disk.write_all(
            RUSTFS_META_TMP_BUCKET,
            "cache-test-part/part.1",
            bytes::Bytes::from_static(b"immutable shard"),
        )
        .await
        .expect("stage tmp part");
        disk.prepare_part_transaction(
            RUSTFS_META_TMP_BUCKET,
            "cache-test-part/part.1",
            RUSTFS_META_MULTIPART_BUCKET,
            &destination,
            part_meta.clone(),
        )
        .await
        .expect("prepare part transaction");
        assert!(
            !disk.has_clean_active_multipart_source(&upload, false),
            "in-flight part transaction must mark active source as having unsettled part txn"
        );
        disk.rename_part(
            RUSTFS_META_TMP_BUCKET,
            "cache-test-part/part.1",
            RUSTFS_META_MULTIPART_BUCKET,
            &destination,
            part_meta,
        )
        .await
        .expect("rename part");
        disk.settle_part_transaction(RUSTFS_META_MULTIPART_BUCKET, &destination, PartTransactionAction::Commit)
            .await
            .expect("settle part transaction");
        assert!(
            disk.has_clean_active_multipart_source(&upload, false),
            "settled commit must restore clean active multipart source status"
        );
        let read_parts = disk
            .read_parts(RUSTFS_META_MULTIPART_BUCKET, &[format!("{destination}.meta")])
            .await
            .expect("cached read_parts");
        assert_eq!(read_parts.len(), 1);
        assert_eq!(read_parts[0].number, 1);

        // Prepare part 2, then replay Commit for already-settled part 1: part 2's
        // pending transaction must keep the upload marked as having unsettled part txns.
        let destination2 = format!("{upload}/{data_dir}/part.2");
        let mut part2_info = completed.parts[0].clone();
        part2_info.number = 2;
        let part2_meta = bytes::Bytes::from(part2_info.marshal_msg().expect("part 2 meta"));
        disk.write_all(
            RUSTFS_META_TMP_BUCKET,
            "cache-test-part/part.2",
            bytes::Bytes::from_static(b"immutable shard"),
        )
        .await
        .expect("stage tmp part 2");
        disk.prepare_part_transaction(
            RUSTFS_META_TMP_BUCKET,
            "cache-test-part/part.2",
            RUSTFS_META_MULTIPART_BUCKET,
            &destination2,
            part2_meta.clone(),
        )
        .await
        .expect("prepare part 2 transaction");
        assert!(!disk.has_clean_active_multipart_source(&upload, false));
        disk.settle_part_transaction(RUSTFS_META_MULTIPART_BUCKET, &destination, PartTransactionAction::Commit)
            .await
            .expect("replayed part 1 commit settlement");
        assert!(
            !disk.has_clean_active_multipart_source(&upload, false),
            "replayed settlement for part 1 must not clear part 2's pending transaction"
        );
        disk.rename_part(
            RUSTFS_META_TMP_BUCKET,
            "cache-test-part/part.2",
            RUSTFS_META_MULTIPART_BUCKET,
            &destination2,
            part2_meta,
        )
        .await
        .expect("rename part 2");
        disk.settle_part_transaction(RUSTFS_META_MULTIPART_BUCKET, &destination2, PartTransactionAction::Commit)
            .await
            .expect("settle part 2 transaction");
        assert!(disk.has_clean_active_multipart_source(&upload, false));

        // Deleting {upload}/xl.meta through delete_paths must invalidate the canonical {upload} cache key.
        disk.delete_paths(RUSTFS_META_MULTIPART_BUCKET, &[format!("{upload}/xl.meta")])
            .await
            .expect("delete xl.meta via delete_paths");
        assert!(
            !disk.has_clean_active_multipart_source(&upload, false),
            "per-file head deletion must invalidate the canonical upload cache key"
        );
        assert!(matches!(
            disk.read_version("", RUSTFS_META_MULTIPART_BUCKET, &upload, "", &ReadOptions::default())
                .await,
            Err(DiskError::FileNotFound)
        ));
    }

    #[cfg(not(windows))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn multipart_source_cache_publication_holds_source_lock_against_concurrent_retirement() {
        use crate::disk::{DiskAPI as _, disk_store::LocalDiskWrapper, os::prepared_publication_test_hooks as hooks};

        let (_root, disk, _, _) = fixture().await;
        std::fs::create_dir_all(disk.io_get_bucket_path(RUSTFS_META_TMP_BUCKET).expect("TMP path")).expect("TMP bucket");
        let disk = Arc::new(disk);
        let wrapper = LocalDiskWrapper::new(Arc::clone(&disk), false);
        let upload = format!("{}/{}", "d".repeat(64), Uuid::new_v4());
        let data_dir = Uuid::new_v4();
        let mut init_fi = FileInfo::new(&upload, 2, 2);
        init_fi.erasure.index = 1;
        init_fi.data_dir = Some(data_dir);
        init_fi.fresh = true;
        init_fi.mod_time = Some(time::OffsetDateTime::now_utc());

        let source_dir = disk
            .io_get_object_path(RUSTFS_META_MULTIPART_BUCKET, &upload)
            .expect("source dir");
        let base_dir = disk
            .io_get_bucket_path(RUSTFS_META_MULTIPART_BUCKET)
            .expect("multipart bucket path");
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let _hook = hooks::install_at(hooks::Stage::DirFsync, &source_dir, move || {
            let _ = entered_tx.send(());
            let _ = release_rx.recv();
        });

        let init_wrapper = wrapper.clone();
        let init_upload = upload.clone();
        let init_task = tokio::spawn(async move {
            init_wrapper
                .write_metadata("", RUSTFS_META_MULTIPART_BUCKET, &init_upload, init_fi)
                .await
        });

        tokio::time::timeout(std::time::Duration::from_secs(5), entered_rx)
            .await
            .expect("initial write_metadata reaches parent dir fsync")
            .expect("entered channel");

        let retire_wrapper = wrapper.clone();
        let retire_path = format!("{upload}/xl.meta");
        let mut retire_fut = Box::pin(async move {
            retire_wrapper
                .delete_paths(RUSTFS_META_MULTIPART_BUCKET, &[retire_path])
                .await
        });
        assert!(
            futures::poll!(tokio::task::unconstrained(retire_fut.as_mut())).is_pending(),
            "concurrent retirement must block on SourceRW while initial head publication is in flight"
        );

        let _ = release_tx.send(());
        init_task.await.expect("init join").expect("initial write_metadata succeeds");
        retire_fut
            .await
            .expect("concurrent retirement completes after SourceRW release");

        assert!(
            !super::super::multipart_source_state::has_active_multipart_source_head(&source_dir),
            "retired source must not remain active in cache after interleaved initial publication"
        );
        assert!(
            matches!(
                super::super::multipart_source_state::validate_source_mutation(
                    &source_dir,
                    &base_dir,
                    &disk.publication_root,
                    false
                ),
                Err(DiskError::FileNotFound)
            ),
            "validate_source_mutation must reject non-initial mutation on retired source"
        );
    }
}
