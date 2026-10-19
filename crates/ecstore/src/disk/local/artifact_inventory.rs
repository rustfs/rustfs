// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::{LocalDisk, artifact_lease};
use crate::{
    bucket::artifact::ArtifactPutIdentity,
    disk::error::{DiskError, Result},
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::{sync::mpsc, time::Instant};
use uuid::Uuid;

const FRAME_ENTRIES: usize = 256;
const FRAME_BYTES: usize = 64 * 1024;
const MAX_METADATA: u64 = 64 * 1024 * 1024;
const WORK_QUANTUM: usize = 4096;
const MAX_DEPTH: usize = 128;
const MAX_HANDLES: usize = 64;
const MAX_REFERENCES: usize = 8192;
const POLL: std::time::Duration = std::time::Duration::from_millis(10);

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ArtifactInventoryVersion {
    pub key: String,
    pub version_id: Uuid,
    pub delete_marker: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ArtifactInventoryFrame {
    Entries {
        sequence: u64,
        versions: Vec<ArtifactInventoryVersion>,
    },
    Complete {
        disk_id: Uuid,
        incarnation: Uuid,
        attempt_prefix: String,
        entries: u64,
    },
}

struct CancelOnDrop(Arc<AtomicBool>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

#[derive(Clone, Copy)]
struct Limits {
    depth: usize,
    handles: usize,
}
const LIMITS: Limits = Limits {
    depth: MAX_DEPTH,
    handles: MAX_HANDLES,
};

impl LocalDisk {
    // Kept crate-private for the next disk-bound receiver; no public inventory
    // authority or full-cluster completeness claim is exposed by this primitive.
    #[allow(dead_code)]
    pub(crate) async fn inventory_artifact_attempt(
        &self,
        expected_disk_id: Uuid,
        bucket: &str,
        attempt_prefix: &str,
        identity: ArtifactPutIdentity,
        deadline: Instant,
        output: mpsc::Sender<ArtifactInventoryFrame>,
    ) -> Result<()> {
        self.inventory_artifact_attempt_with_limits(expected_disk_id, bucket, attempt_prefix, identity, deadline, output, LIMITS)
            .await
    }

    async fn inventory_artifact_attempt_with_limits(
        &self,
        expected_disk_id: Uuid,
        bucket: &str,
        attempt_prefix: &str,
        identity: ArtifactPutIdentity,
        deadline: Instant,
        output: mpsc::Sender<ArtifactInventoryFrame>,
        limits: Limits,
    ) -> Result<()> {
        if expected_disk_id.is_nil()
            || identity.incarnation.is_nil()
            || !component(bucket)
            || !attempt_prefix.ends_with('/')
            || attempt_prefix.len() >= 1024
            || !attempt_prefix.trim_end_matches('/').split('/').all(component)
            || attempt_prefix.ends_with("//")
        {
            return Err(DiskError::FileCorrupt);
        }
        let cancelled = CancelOnDrop(Arc::new(AtomicBool::new(false)));
        let check_identity = async {
            if self.artifact_disk_id().await? != expected_disk_id {
                return Err(DiskError::DiskNotFound);
            }
            Ok(())
        };
        tokio::time::timeout_at(deadline, check_identity)
            .await
            .map_err(|_| DiskError::Timeout)??;
        let root = tokio::time::timeout_at(deadline, artifact_lease::pinned_root(self))
            .await
            .map_err(|_| DiskError::Timeout)??;
        let drain =
            tokio::time::timeout_at(deadline, artifact_lease::drain_closed(root.try_clone()?, identity.receiver_key, deadline))
                .await
                .map_err(|_| DiskError::Timeout)??;
        #[cfg(unix)]
        {
            let worker_bucket = bucket.to_owned();
            let worker_prefix = attempt_prefix.to_owned();
            let worker_output = output.clone();
            let worker_cancelled = cancelled.0.clone();
            let worker = tokio::task::spawn_blocking(move || {
                let _drain = drain;
                #[cfg(test)]
                worker_hook::run(expected_disk_id);
                let entries =
                    unix::scan(root, &worker_bucket, &worker_prefix, &worker_output, deadline, &worker_cancelled, limits)?;
                Ok::<_, DiskError>((entries, _drain))
            });
            let (entries, _drain) = tokio::time::timeout_at(deadline, worker)
                .await
                .map_err(|_| DiskError::Timeout)?
                .map_err(DiskError::from)??;
            // Both real active-file owners still exist during the final fresh
            // identity check and terminal send. Cancellation cannot release a
            // worker's owner before its last physical read.
            tokio::time::timeout_at(deadline, async {
                if self.artifact_disk_id().await? != expected_disk_id {
                    return Err(DiskError::DiskNotFound);
                }
                output
                    .send(ArtifactInventoryFrame::Complete {
                        disk_id: expected_disk_id,
                        incarnation: identity.incarnation,
                        attempt_prefix: attempt_prefix.to_owned(),
                        entries,
                    })
                    .await
                    .map_err(|_| DiskError::other("artifact inventory consumer closed"))
            })
            .await
            .map_err(|_| DiskError::Timeout)??;
            drop(_drain);
            Ok(())
        }
        #[cfg(not(unix))]
        {
            let _ = (root, drain, output, limits, cancelled);
            Err(DiskError::UnsupportedDisk)
        }
    }
}

fn component(value: &str) -> bool {
    !value.is_empty() && !matches!(value, "." | "..") && !value.contains(['/', '\\', '\0'])
}

fn check(deadline: Instant, cancelled: &AtomicBool) -> Result<()> {
    if cancelled.load(Ordering::Acquire) {
        return Err(DiskError::other("artifact inventory cancelled"));
    }
    if Instant::now() >= deadline {
        return Err(DiskError::Timeout);
    }
    Ok(())
}

#[cfg(unix)]
mod unix {
    use super::*;
    use rustfs_filemeta::{FileMeta, VersionType};
    use rustfs_utils::path::GLOBAL_DIR_SUFFIX;
    use rustix::fs::{AtFlags, Dir, FileType, Mode, OFlags, openat, statat};
    use std::{
        collections::{BTreeMap, BTreeSet},
        fs::File,
        io::Read,
        os::{fd::AsFd, unix::fs::MetadataExt},
    };

    const DIRECTORY: OFlags = OFlags::RDONLY
        .union(OFlags::DIRECTORY)
        .union(OFlags::NOFOLLOW)
        .union(OFlags::CLOEXEC);
    type Parts = BTreeSet<String>;
    struct Frame {
        key: String,
        entries: Dir,
        metadata_present: bool,
        data_dirs: BTreeMap<String, Parts>,
        parts: Parts,
    }
    struct Emitter<'a> {
        output: &'a mpsc::Sender<ArtifactInventoryFrame>,
        prefix: &'a str,
        deadline: Instant,
        cancelled: &'a AtomicBool,
        versions: Vec<ArtifactInventoryVersion>,
        bytes: usize,
        sequence: u64,
        total: u64,
    }
    impl Emitter<'_> {
        fn flush(&mut self) -> Result<()> {
            if self.versions.is_empty() {
                return Ok(());
            }
            let mut frame = ArtifactInventoryFrame::Entries {
                sequence: self.sequence,
                versions: std::mem::take(&mut self.versions),
            };
            loop {
                check(self.deadline, self.cancelled)?;
                match self.output.try_send(frame) {
                    Ok(()) => break,
                    Err(mpsc::error::TrySendError::Full(returned)) => {
                        frame = returned;
                        std::thread::sleep(POLL);
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        return Err(DiskError::other("artifact inventory consumer closed"));
                    }
                }
            }
            self.sequence = self.sequence.checked_add(1).ok_or(DiskError::FileCorrupt)?;
            self.bytes = 0;
            Ok(())
        }
        fn emit(&mut self, version: ArtifactInventoryVersion) -> Result<()> {
            let bytes = version.key.len().checked_add(64).ok_or(DiskError::FileCorrupt)?;
            if bytes > FRAME_BYTES {
                return Err(DiskError::FileCorrupt);
            }
            if self.versions.len() == FRAME_ENTRIES || self.bytes + bytes > FRAME_BYTES {
                self.flush()?;
            }
            self.bytes += bytes;
            self.total = self.total.checked_add(1).ok_or(DiskError::FileCorrupt)?;
            self.versions.push(version);
            Ok(())
        }
    }

    fn io(error: std::io::Error) -> DiskError {
        if matches!(error.kind(), std::io::ErrorKind::InvalidData | std::io::ErrorKind::NotADirectory)
            || error.raw_os_error() == Some(rustix::io::Errno::LOOP.raw_os_error())
        {
            DiskError::FileCorrupt
        } else {
            DiskError::from(error)
        }
    }
    fn err(error: rustix::io::Errno) -> DiskError {
        io(error.into())
    }

    fn open_directory(parent: impl AsFd, name: &str, device: u64) -> Result<File> {
        let file = File::from(openat(parent, name, DIRECTORY, Mode::empty()).map_err(err)?);
        if file.metadata()?.dev() != device {
            return Err(DiskError::FileCorrupt);
        }
        Ok(file)
    }

    fn frame(directory: File, key: String, parts: Parts, emitter: &mut Emitter<'_>, references: &mut usize) -> Result<Frame> {
        let device = directory.metadata()?.dev();
        let mut data_dirs: BTreeMap<String, Parts> = BTreeMap::new();
        let metadata = match openat(
            &directory,
            "xl.meta",
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
            Mode::empty(),
        ) {
            Ok(fd) => {
                let file = File::from(fd);
                if file.metadata()?.is_dir() { None } else { Some(file) }
            }
            Err(rustix::io::Errno::NOENT) => None,
            Err(error) => return Err(err(error)),
        };
        let metadata_present = metadata.is_some();
        if let Some(mut file) = metadata {
            let stat = file.metadata()?;
            if !stat.is_file() || stat.dev() != device || stat.nlink() != 1 || stat.len() > MAX_METADATA {
                return Err(DiskError::FileCorrupt);
            }
            let mut bytes = Vec::new();
            file.by_ref().take(MAX_METADATA + 1).read_to_end(&mut bytes).map_err(io)?;
            if bytes.len() > usize::try_from(MAX_METADATA).map_err(|_| DiskError::FileCorrupt)? {
                return Err(DiskError::FileCorrupt);
            }
            let metadata = FileMeta::load(&bytes).map_err(|_| DiskError::FileCorrupt)?;
            metadata.validate_integrity().map_err(|_| DiskError::FileCorrupt)?;
            if metadata.versions.is_empty() {
                return Err(DiskError::FileCorrupt);
            }
            let logical_key = key
                .strip_suffix(GLOBAL_DIR_SUFFIX)
                .map_or_else(|| key.clone(), |prefix| format!("{prefix}/"));
            if !logical_key.starts_with(emitter.prefix) || logical_key.len() > 1024 {
                return Err(DiskError::FileCorrupt);
            }
            let mut seen = BTreeSet::new();
            let mut inline_versions = 0;
            for shallow in &metadata.versions {
                check(emitter.deadline, emitter.cancelled)?;
                let version = shallow.parse_version_meta().map_err(|_| DiskError::FileCorrupt)?;
                if !version.valid()
                    || version.free_version()
                    || shallow.header.free_version()
                    || !matches!(version.version_type, VersionType::Object | VersionType::Delete)
                    || shallow.header.version_type != version.version_type
                    || shallow.header.version_id != version.header().version_id
                {
                    return Err(DiskError::FileCorrupt);
                }
                let info = version
                    .into_fileinfo("", &logical_key, true)
                    .map_err(|_| DiskError::FileCorrupt)?;
                let version_id = info.version_id.filter(|id| !id.is_nil()).ok_or(DiskError::FileCorrupt)?;
                if !seen.insert(version_id) || info.tier_free_version() || info.is_remote() {
                    return Err(DiskError::FileCorrupt);
                }
                if !info.deleted && info.inline_data() {
                    inline_versions += 1;
                    if metadata
                        .data
                        .find(&version_id.to_string())
                        .map_err(|_| DiskError::FileCorrupt)?
                        .is_none()
                    {
                        return Err(DiskError::FileCorrupt);
                    }
                }
                if !info.deleted && !info.inline_data() {
                    let dir = info
                        .data_dir
                        .filter(|id| !id.is_nil())
                        .ok_or(DiskError::FileCorrupt)?
                        .to_string();
                    if !data_dirs.contains_key(&dir) {
                        *references += 1;
                    }
                    let expected = data_dirs.entry(dir).or_default();
                    if info.parts.is_empty() {
                        return Err(DiskError::FileCorrupt);
                    }
                    for part in &info.parts {
                        if part.number == 0 {
                            return Err(DiskError::FileCorrupt);
                        }
                        if expected.insert(format!("part.{}", part.number)) {
                            *references += 1;
                        }
                    }
                    if *references > MAX_REFERENCES {
                        return Err(DiskError::FileCorrupt);
                    }
                }
                emitter.emit(ArtifactInventoryVersion {
                    key: logical_key.clone(),
                    version_id,
                    delete_marker: info.deleted,
                })?;
            }
            if metadata.data.entries().map_err(|_| DiskError::FileCorrupt)? != inline_versions {
                return Err(DiskError::FileCorrupt);
            }
        }
        Ok(Frame {
            key,
            entries: Dir::new(directory).map_err(err)?,
            metadata_present,
            data_dirs,
            parts,
        })
    }

    pub(super) fn scan(
        root: File,
        bucket: &str,
        prefix: &str,
        output: &mpsc::Sender<ArtifactInventoryFrame>,
        deadline: Instant,
        cancelled: &AtomicBool,
        limits: Limits,
    ) -> Result<u64> {
        check(deadline, cancelled)?;
        let device = root.metadata()?.dev();
        let raw_device = rustix::fs::fstat(&root).map_err(err)?.st_dev;
        let mut directory = open_directory(&root, bucket, device)?;
        for name in prefix.trim_end_matches('/').split('/') {
            directory = match open_directory(&directory, name, device) {
                Ok(directory) => directory,
                // A closed attempt may never have written to this disk. Only
                // an absent prefix below the already-verified bucket is empty.
                Err(DiskError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
                Err(error) => return Err(error),
            };
        }
        let mut emitter = Emitter {
            output,
            prefix,
            deadline,
            cancelled,
            versions: Vec::new(),
            bytes: 0,
            sequence: 0,
            total: 0,
        };
        let mut references = 0;
        let mut stack = vec![frame(
            directory,
            prefix.trim_end_matches('/').to_owned(),
            Parts::new(),
            &mut emitter,
            &mut references,
        )?];
        let mut visited = 0usize;
        while let Some(mut current) = stack.pop() {
            check(deadline, cancelled)?;
            if stack.len() >= limits.depth || stack.len() >= limits.handles {
                return Err(DiskError::FileCorrupt);
            }
            let entry = match current.entries.next() {
                Some(entry) => entry.map_err(err)?,
                None => {
                    if !current.parts.is_empty() || !current.data_dirs.is_empty() {
                        return Err(DiskError::FileCorrupt);
                    }
                    continue;
                }
            };
            let name = entry.file_name();
            if name.to_bytes() == b"." || name.to_bytes() == b".." {
                stack.push(current);
                continue;
            }
            let name = name.to_str().map_err(|_| DiskError::FileCorrupt)?;
            if !component(name) {
                return Err(DiskError::FileCorrupt);
            }
            visited += 1;
            if visited == WORK_QUANTUM {
                std::thread::yield_now();
                visited = 0;
            }
            let fd = current.entries.fd().map_err(err)?;
            let stat = statat(fd, name, AtFlags::SYMLINK_NOFOLLOW).map_err(err)?;
            if stat.st_dev != raw_device {
                return Err(DiskError::FileCorrupt);
            }
            match FileType::from_raw_mode(stat.st_mode) {
                FileType::Directory => {
                    if stack.len() + 1 >= limits.depth || stack.len() + 1 >= limits.handles {
                        return Err(DiskError::FileCorrupt);
                    }
                    let child = open_directory(fd, name, device)?;
                    let parts = current
                        .data_dirs
                        .remove(name)
                        .map(|parts| {
                            references -= 1;
                            parts
                        })
                        .unwrap_or_default();
                    let key = format!("{}/{name}", current.key);
                    let child = frame(child, key, parts, &mut emitter, &mut references)?;
                    stack.push(current);
                    stack.push(child);
                }
                FileType::RegularFile if name == "xl.meta" && current.metadata_present => stack.push(current),
                FileType::RegularFile if current.parts.remove(name) => {
                    if stat.st_nlink != 1 {
                        return Err(DiskError::FileCorrupt);
                    }
                    references -= 1;
                    stack.push(current);
                }
                _ => return Err(DiskError::FileCorrupt),
            }
        }
        emitter.flush()?;
        check(deadline, cancelled)?;
        Ok(emitter.total)
    }
}

#[cfg(all(test, unix))]
mod worker_hook {
    use super::*;
    type Hook = Box<dyn FnOnce() + Send>;
    static HOOKS: std::sync::LazyLock<std::sync::Mutex<std::collections::HashMap<Uuid, Hook>>> =
        std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));
    pub(super) fn set(id: Uuid, hook: Hook) {
        HOOKS.lock().expect("test hooks").insert(id, hook);
    }
    pub(super) fn run(id: Uuid) {
        let hook = HOOKS.lock().expect("test hooks").remove(&id);
        if let Some(hook) = hook {
            hook();
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::disk::local::test::{test_file_info, test_meta};
    use crate::disk::{DiskAPI as _, Endpoint, FORMAT_CONFIG_FILE, format::FormatV3};
    use bytes::Bytes;
    use rustfs_filemeta::{FileInfo, FileMeta, FileMetaVersion};
    use std::{path::Path, sync::Arc, time::Duration};

    const BUCKET: &str = "artifact-inventory";
    const PREFIX: &str = "outputs/74656e616e74/11111111-1111-4111-8111-111111111111/22222222-2222-4222-8222-222222222222/";

    async fn fixture() -> (tempfile::TempDir, Arc<LocalDisk>, Uuid, ArtifactPutIdentity) {
        let temp = tempfile::tempdir().expect("real disk root");
        let mut endpoint = Endpoint::try_from(temp.path().to_str().expect("fixture root")).expect("local endpoint");
        endpoint.set_set_index(0);
        endpoint.set_disk_index(0);
        let disk = Arc::new(LocalDisk::new(&endpoint, false).await.expect("real local disk"));
        let mut format = FormatV3::new(1, 1);
        format.erasure.this = format.erasure.sets[0][0];
        std::fs::create_dir_all(temp.path().join(".rustfs.sys")).expect("format directory");
        std::fs::write(
            temp.path().join(".rustfs.sys").join(FORMAT_CONFIG_FILE),
            format.to_json().expect("format bytes"),
        )
        .expect("real disk format");
        std::fs::create_dir_all(temp.path().join(BUCKET).join(PREFIX)).expect("attempt directory");
        let identity = ArtifactPutIdentity {
            incarnation: Uuid::new_v4(),
            receiver_key: [19; 32],
        };
        (temp, disk, format.erasure.this, identity)
    }

    fn seed(root: &Path, key: &str, versions: &[Uuid], marker: Option<Uuid>) {
        let mut metadata = FileMeta::new();
        for version in versions {
            let mut info = test_file_info(key, *version, Some(Uuid::new_v4()), Some(Bytes::from_static(b"inline payload")));
            info.set_inline_data();
            metadata.add_version(info).expect("real metadata version");
        }
        if let Some(version) = marker {
            let info = FileInfo {
                name: key.to_owned(),
                version_id: Some(version),
                deleted: true,
                mod_time: Some(time::OffsetDateTime::now_utc()),
                ..Default::default()
            };
            assert!(info.is_canonical_delete_marker(), "canonical marker fixture");
            metadata.add_version(info).expect("real delete marker");
        }
        let directory = root.join(BUCKET).join(key);
        std::fs::create_dir_all(&directory).expect("object directory");
        std::fs::write(directory.join("xl.meta"), metadata.marshal_msg().expect("metadata bytes")).expect("real metadata");
    }

    async fn close(disk: &Arc<LocalDisk>, identity: ArtifactPutIdentity) {
        disk.seal_artifact_puts(identity.receiver_key)
            .await
            .expect("close PUT domain");
        disk.seal_artifact_repairs(identity.receiver_key)
            .await
            .expect("close repair domain");
    }

    async fn collect(disk: &LocalDisk, id: Uuid, identity: ArtifactPutIdentity) -> (Result<()>, Vec<ArtifactInventoryFrame>) {
        let (output, mut input) = mpsc::channel(2);
        let (result, frames) = tokio::join!(
            disk.inventory_artifact_attempt(id, BUCKET, PREFIX, identity, Instant::now() + Duration::from_secs(30), output),
            async {
                let mut frames = Vec::new();
                while let Some(frame) = input.recv().await {
                    frames.push(frame);
                }
                frames
            }
        );
        (result, frames)
    }

    #[tokio::test]
    async fn artifact_inventory_sealed_versions_and_uuid_child_are_complete() {
        let (temp, disk, id, identity) = fixture().await;
        let versions = [Uuid::new_v4(), Uuid::new_v4()];
        let marker = Uuid::new_v4();
        let key = format!("{PREFIX}object");
        seed(temp.path(), &key, &versions, Some(marker));
        // The same UUID directory contains an owned shard and a legal child
        // object's xl.meta: data-directory skipping would lose this version.
        let data_dir = Uuid::new_v4();
        let shard_version = Uuid::new_v4();
        let mut metadata =
            FileMeta::load(&std::fs::read(temp.path().join(BUCKET).join(&key).join("xl.meta")).expect("original metadata"))
                .expect("decode original metadata");
        metadata
            .add_version(test_file_info(&key, shard_version, Some(data_dir), None))
            .expect("sharded version");
        std::fs::write(
            temp.path().join(BUCKET).join(&key).join("xl.meta"),
            metadata.marshal_msg().expect("metadata bytes"),
        )
        .expect("sharded metadata");
        let child = format!("{key}/{data_dir}");
        std::fs::create_dir_all(temp.path().join(BUCKET).join(&child)).expect("data and namespace directory");
        std::fs::write(temp.path().join(BUCKET).join(&child).join("part.1"), b"x").expect("owned shard");
        let child_version = Uuid::new_v4();
        seed(temp.path(), &child, &[child_version], None);
        for index in 0..4100 {
            seed(temp.path(), &format!("{PREFIX}many-{index}"), &[Uuid::new_v4()], None);
        }
        close(&disk, identity).await;
        let before = census(temp.path());
        let (result, frames) = collect(&disk, id, identity).await;
        assert!(result.is_ok(), "A03I1_SEALED_SCAN: {result:?}");
        assert_eq!(
            census(temp.path()),
            before,
            "strict inventory does not mutate metadata, shards or seal records"
        );
        let mut sequence = 0;
        for frame in &frames {
            if let ArtifactInventoryFrame::Entries {
                sequence: actual,
                versions,
            } = frame
            {
                assert_eq!(*actual, sequence);
                assert!(versions.len() <= FRAME_ENTRIES);
                assert!(versions.iter().map(|version| version.key.len() + 64).sum::<usize>() <= FRAME_BYTES);
                sequence += 1;
            }
        }
        assert!(sequence > 1, "real bounded output spans frames");
        let entries: Vec<_> = frames
            .iter()
            .filter_map(|frame| match frame {
                ArtifactInventoryFrame::Entries { versions, .. } => Some(versions),
                _ => None,
            })
            .flatten()
            .collect();
        assert_eq!(entries.len(), 4105, "A03I1_ALL_VERSIONS_AND_WORK_QUANTUM");
        assert!(
            entries
                .iter()
                .any(|entry| entry.key == key && entry.version_id == shard_version)
        );
        for version in versions {
            assert!(
                entries
                    .iter()
                    .any(|entry| entry.key == key && entry.version_id == version && !entry.delete_marker)
            );
        }
        assert!(
            entries
                .iter()
                .any(|entry| entry.key == key && entry.version_id == marker && entry.delete_marker)
        );
        assert!(
            entries
                .iter()
                .any(|entry| entry.key == child && entry.version_id == child_version)
        );
        assert_eq!(
            frames.last(),
            Some(&ArtifactInventoryFrame::Complete {
                disk_id: id,
                incarnation: identity.incarnation,
                attempt_prefix: PREFIX.to_owned(),
                entries: 4105
            })
        );
    }

    // Ignore atime: reads may update it, but must preserve all bytes, inode and
    // ctime. This also catches accidentally creating a missing closure path.
    fn census(root: &Path) -> std::collections::BTreeMap<std::path::PathBuf, (u64, i64, i64, Vec<u8>)> {
        use std::os::unix::fs::MetadataExt;
        let mut result = std::collections::BTreeMap::new();
        let mut pending = vec![root.to_path_buf()];
        while let Some(path) = pending.pop() {
            let metadata = std::fs::symlink_metadata(&path).expect("census metadata");
            let bytes = if metadata.is_file() {
                std::fs::read(&path).expect("census file")
            } else {
                Vec::new()
            };
            result.insert(
                path.strip_prefix(root).expect("relative census path").to_owned(),
                (metadata.ino(), metadata.ctime(), metadata.ctime_nsec(), bytes),
            );
            if metadata.is_dir() {
                for entry in std::fs::read_dir(&path).expect("census directory") {
                    pending.push(entry.expect("census entry").path());
                }
            }
        }
        result
    }

    fn domain_file(root: &Path, identity: ArtifactPutIdentity, repair: bool, name: &str) -> std::path::PathBuf {
        root.join(if repair {
            ".rustfs-artifact-repair-seals"
        } else {
            ".rustfs-artifact-put-seals"
        })
        .join(hex_simd::encode_to_string(identity.receiver_key, hex_simd::AsciiCase::Lower))
        .join(name)
    }

    async fn wait_for(mut predicate: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !predicate() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("observable physical state reached");
    }

    fn drain_available(root: &Path, identity: ArtifactPutIdentity) -> bool {
        use rustix::fs::{FlockOperation, flock};
        let active = std::fs::File::open(domain_file(root, identity, false, "active.lock")).expect("existing active inode");
        match flock(&active, FlockOperation::NonBlockingLockExclusive) {
            Ok(()) => true,
            Err(rustix::io::Errno::WOULDBLOCK) => false,
            Err(error) => panic!("probe actual active owner: {error}"),
        }
    }

    fn assert_incomplete(result: Result<()>, frames: &[ArtifactInventoryFrame]) {
        assert!(result.is_err(), "A03I1_UNCERTAIN_SCAN_MUST_FAIL");
        assert!(
            !frames
                .iter()
                .any(|frame| matches!(frame, ArtifactInventoryFrame::Complete { .. })),
            "A03I1_NO_FALSE_COMPLETE"
        );
    }

    #[tokio::test]
    async fn artifact_inventory_requires_both_closed_and_drained_domains() {
        let (temp, disk, id, identity) = fixture().await;
        let before = census(temp.path());
        let (result, frames) = collect(&disk, id, identity).await;
        assert_incomplete(result, &frames);
        assert_eq!(census(temp.path()), before, "unclosed scan must create nothing");
        disk.seal_artifact_puts(identity.receiver_key).await.expect("PUT-only close");
        let before = census(temp.path());
        let (result, frames) = collect(&disk, id, identity).await;
        assert_incomplete(result, &frames);
        assert_eq!(census(temp.path()), before, "missing repair close stays missing");
        for repair in [false, true] {
            let (temp, disk, id, identity) = fixture().await;
            let owner = if repair {
                disk.acquire_artifact_repair_lease(identity.receiver_key).await
            } else {
                disk.acquire_artifact_put_lease(identity.receiver_key).await
            }
            .expect("real physical writer");
            if repair {
                disk.seal_artifact_puts(identity.receiver_key)
                    .await
                    .expect("other domain closed");
            } else {
                disk.seal_artifact_repairs(identity.receiver_key)
                    .await
                    .expect("other domain closed");
            }
            let seal_disk = disk.clone();
            let sealer = tokio::spawn(async move {
                if repair {
                    seal_disk
                        .seal_artifact_repairs(identity.receiver_key)
                        .await
                        .expect("close held repair domain");
                } else {
                    seal_disk
                        .seal_artifact_puts(identity.receiver_key)
                        .await
                        .expect("close held PUT domain");
                }
            });
            wait_for(|| domain_file(temp.path(), identity, repair, "closed").is_file()).await;
            let (output, mut input) = mpsc::channel(2);
            let denied = disk
                .inventory_artifact_attempt(id, BUCKET, PREFIX, identity, Instant::now() + Duration::from_millis(100), output)
                .await;
            assert!(
                matches!(denied, Err(DiskError::Timeout)),
                "A03I1_ACTIVE_WRITER_PREVENTS_COMPLETE: {denied:?}"
            );
            while let Some(frame) = input.recv().await {
                assert!(!matches!(frame, ArtifactInventoryFrame::Complete { .. }));
            }
            drop(owner);
            sealer.await.expect("real sealer drains");
            let (result, frames) = collect(&disk, id, identity).await;
            result.expect("retry after actual owner drain");
            assert!(matches!(frames.last(), Some(ArtifactInventoryFrame::Complete { entries: 0, .. })));
        }
    }

    #[tokio::test]
    async fn artifact_inventory_rejects_corruption_and_unclassified_residue() {
        for damage in 0..7 {
            let (temp, disk, id, identity) = fixture().await;
            let key = format!("{PREFIX}object");
            seed(temp.path(), &key, &[Uuid::new_v4()], None);
            close(&disk, identity).await;
            let object = temp.path().join(BUCKET).join(&key);
            match damage {
                0 => std::fs::write(object.join("xl.meta"), b"corrupt").expect("corrupt metadata"),
                1 | 2 => {
                    let residue = object.join(Uuid::new_v4().to_string());
                    std::fs::create_dir(&residue).expect("residual directory");
                    std::fs::write(residue.join(if damage == 1 { "part.1" } else { "delete-prepared" }), b"residue")
                        .expect("unclassified residue");
                }
                3 => std::os::unix::fs::symlink(object.join("xl.meta"), object.join("foreign")).expect("symlink residue"),
                4 => {
                    let marker = FileInfo {
                        version_id: Some(Uuid::new_v4()),
                        deleted: true,
                        mod_time: Some(time::OffsetDateTime::now_utc()),
                        ..Default::default()
                    };
                    let mut version = FileMetaVersion::from(marker);
                    rustfs_utils::http::insert_bytes(
                        &mut version.delete_marker.as_mut().expect("actual marker").meta_sys,
                        rustfs_utils::http::SUFFIX_FREE_VERSION,
                        Vec::new(),
                    );
                    assert!(version.free_version());
                    let mut metadata = FileMeta::new();
                    metadata.add_version_filemata(version).expect("encoded free version");
                    std::fs::write(object.join("xl.meta"), metadata.marshal_msg().expect("free-version bytes"))
                        .expect("real free-version metadata");
                }
                5 => std::fs::write(object.join("xl.meta"), FileMeta::new().marshal_msg().expect("empty metadata"))
                    .expect("empty index"),
                6 => std::fs::write(domain_file(temp.path(), identity, true, "closed"), b"corrupt closed marker")
                    .expect("corrupt closure"),
                _ => unreachable!(),
            }
            let before = census(temp.path());
            let (result, frames) = collect(&disk, id, identity).await;
            assert!(
                matches!(result, Err(DiskError::FileCorrupt)),
                "A03I1_RESIDUE_REJECTED case={damage}: {result:?}"
            );
            assert_incomplete(result, &frames);
            assert_eq!(census(temp.path()), before, "failed scan is read-only");
        }
    }

    #[tokio::test]
    async fn artifact_inventory_fresh_identity_and_pinned_root() {
        let (temp, disk, id, identity) = fixture().await;
        seed(temp.path(), &format!("{PREFIX}original"), &[Uuid::new_v4()], None);
        close(&disk, identity).await;
        assert_eq!(disk.get_disk_id().await.expect("warm ID cache"), Some(id));
        let path = temp.path().join(".rustfs.sys").join(FORMAT_CONFIG_FILE);
        let original = std::fs::read(&path).expect("original format");
        for replacement in [Uuid::new_v4(), Uuid::nil()] {
            let mut format: FormatV3 = serde_json::from_slice(&original).expect("format");
            format.erasure.this = replacement;
            format.erasure.sets[0][0] = replacement;
            std::fs::write(&path, format.to_json().expect("replacement format")).expect("replace format without cache expiry");
            let (result, frames) = collect(&disk, id, identity).await;
            assert_incomplete(result, &frames);
        }
        std::fs::write(&path, &original).expect("restore format");
        for restore_before_read in [false, true] {
            let (started, entered) = tokio::sync::oneshot::channel();
            let (release, held) = std::sync::mpsc::channel();
            worker_hook::set(
                id,
                Box::new(move || {
                    let _ = started.send(());
                    held.recv().expect("release physical scanner");
                }),
            );
            let scan_disk = disk.clone();
            let scan = tokio::spawn(async move { collect(&scan_disk, id, identity).await });
            entered.await.expect("actual worker owns pinned root and both drain guards");
            let parking = temp.path().with_extension("inventory-parking");
            std::fs::rename(temp.path(), &parking).expect("park initialized root");
            std::fs::create_dir_all(temp.path().join(".rustfs.sys")).expect("replacement root");
            std::fs::write(temp.path().join(".rustfs.sys").join(FORMAT_CONFIG_FILE), &original)
                .expect("replacement uses same UUID");
            seed(temp.path(), &format!("{PREFIX}foreign"), &[Uuid::new_v4()], None);
            let restore = || {
                std::fs::remove_dir_all(temp.path()).expect("remove task-owned replacement root");
                std::fs::rename(&parking, temp.path()).expect("restore initialized root");
            };
            if restore_before_read {
                restore();
            }
            release.send(()).expect("release real read");
            let (result, frames) = scan.await.expect("scan task");
            if restore_before_read {
                result.expect("A-to-B-to-A scans the initialized descriptor");
                let keys: Vec<_> = frames
                    .iter()
                    .filter_map(|frame| match frame {
                        ArtifactInventoryFrame::Entries { versions, .. } => Some(versions),
                        _ => None,
                    })
                    .flatten()
                    .map(|version| version.key.as_str())
                    .collect();
                assert_eq!(keys, vec![format!("{PREFIX}original")]);
            } else {
                assert!(
                    matches!(result, Err(DiskError::DiskNotFound)),
                    "configured root replacement cannot complete: {result:?}"
                );
                assert_incomplete(result, &frames);
                restore();
            }
        }
    }

    #[tokio::test]
    async fn artifact_inventory_cancel_and_limits_never_complete() {
        let (temp, disk, id, identity) = fixture().await;
        seed(temp.path(), &format!("{PREFIX}object"), &[Uuid::new_v4()], None);
        close(&disk, identity).await;
        let (started, entered) = tokio::sync::oneshot::channel();
        let (release, held) = std::sync::mpsc::channel();
        worker_hook::set(
            id,
            Box::new(move || {
                let _ = started.send(());
                held.recv().expect("release actual worker");
            }),
        );
        let (output, mut input) = mpsc::channel(1);
        let scan_disk = disk.clone();
        let scan = tokio::spawn(async move {
            scan_disk
                .inventory_artifact_attempt(id, BUCKET, PREFIX, identity, Instant::now() + Duration::from_secs(5), output)
                .await
        });
        entered.await.expect("actual physical owner entered");
        assert!(!drain_available(temp.path(), identity));
        scan.abort();
        assert!(scan.await.expect_err("waiter cancelled").is_cancelled());
        assert!(!drain_available(temp.path(), identity), "A03I1_CANCELLED_WAITER_RETAINS_ACTUAL_OWNER");
        release.send(()).expect("finish detached read worker");
        wait_for(|| drain_available(temp.path(), identity)).await;
        while let Some(frame) = input.recv().await {
            assert!(!matches!(frame, ArtifactInventoryFrame::Complete { .. }));
        }
        collect(&disk, id, identity)
            .await
            .0
            .expect("drained cancelled scan can retry");

        std::fs::create_dir_all(temp.path().join(BUCKET).join(PREFIX).join("deep/a/b/c")).expect("depth limit fixture");
        for limits in [Limits { depth: 2, handles: 64 }, Limits { depth: 128, handles: 2 }] {
            let (output, mut input) = mpsc::channel(64);
            let result = disk
                .inventory_artifact_attempt_with_limits(
                    id,
                    BUCKET,
                    PREFIX,
                    identity,
                    Instant::now() + Duration::from_secs(5),
                    output,
                    limits,
                )
                .await;
            let mut frames = Vec::new();
            while let Some(frame) = input.recv().await {
                frames.push(frame);
            }
            assert!(matches!(result, Err(DiskError::FileCorrupt)), "explicit resource cap: {result:?}");
            assert_incomplete(result, &frames);
        }
        for index in 0..600 {
            seed(temp.path(), &format!("{PREFIX}output-{index}"), &[Uuid::new_v4()], None);
        }
        let (output, mut input) = mpsc::channel(1);
        let result = disk
            .inventory_artifact_attempt(id, BUCKET, PREFIX, identity, Instant::now() + Duration::from_millis(200), output)
            .await;
        assert!(matches!(result, Err(DiskError::Timeout)), "bounded backpressure deadline: {result:?}");
        while let Some(frame) = input.recv().await {
            assert!(!matches!(frame, ArtifactInventoryFrame::Complete { .. }));
        }
    }
    #[tokio::test]
    async fn artifact_inventory_sealed_empty_attempt_is_complete() {
        let (temp, disk, id, identity) = fixture().await;
        close(&disk, identity).await;
        // Failed attempts can have no upload, and a configured disk in another
        // erasure set may never have held this attempt's prefix.
        for missing in [PREFIX.trim_end_matches('/'), "outputs/74656e616e74", "outputs"] {
            let path = temp.path().join(BUCKET).join(missing);
            std::fs::remove_dir_all(&path).expect("remove task-owned unused prefix component");
            let before = census(temp.path());
            let (result, frames) = collect(&disk, id, identity).await;
            assert!(result.is_ok(), "A03I1_EMPTY_SEALED_ATTEMPT: missing={missing} result={result:?}");
            assert_eq!(
                frames,
                vec![ArtifactInventoryFrame::Complete {
                    disk_id: id,
                    incarnation: identity.incarnation,
                    attempt_prefix: PREFIX.to_owned(),
                    entries: 0
                }]
            );
            assert_eq!(census(temp.path()), before, "empty inventory does not create the missing prefix");
        }
        std::fs::write(temp.path().join(BUCKET).join("outputs"), b"not a namespace directory").expect("non-directory prefix");
        let (result, frames) = collect(&disk, id, identity).await;
        assert_incomplete(result, &frames);
        std::fs::remove_file(temp.path().join(BUCKET).join("outputs")).expect("remove task-owned file");
        std::os::unix::fs::symlink(temp.path().join(".rustfs.sys"), temp.path().join(BUCKET).join("outputs"))
            .expect("symlink prefix");
        let (result, frames) = collect(&disk, id, identity).await;
        assert_incomplete(result, &frames);
        std::fs::remove_file(temp.path().join(BUCKET).join("outputs")).expect("remove task-owned symlink");
        std::fs::remove_dir(temp.path().join(BUCKET)).expect("remove task-owned empty bucket");
        let (result, frames) = collect(&disk, id, identity).await;
        assert_incomplete(result, &frames);
    }

    // Descriptor-relative fixture creation also works when the legal key plus
    // disk root/data-directory exceeds macOS's pathname-based PATH_MAX.
    fn make_directory_at(root: &std::fs::File, path: &str) -> std::fs::File {
        use rustix::fs::{Mode, OFlags, mkdirat, openat};
        let mut directory = root.try_clone().expect("fixture root descriptor");
        for component in path.split('/') {
            match mkdirat(&directory, component, Mode::RWXU) {
                Ok(()) | Err(rustix::io::Errno::EXIST) => {}
                Err(error) => panic!("fixture mkdirat: {error}"),
            }
            directory = std::fs::File::from(
                openat(
                    &directory,
                    component,
                    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                    Mode::empty(),
                )
                .expect("fixture directory descriptor"),
            );
        }
        directory
    }

    fn write_at(directory: &std::fs::File, name: &str, data: &[u8]) {
        use rustix::fs::{Mode, OFlags, openat};
        use std::io::Write;
        let mut file = std::fs::File::from(
            openat(
                directory,
                name,
                OFlags::CREATE | OFlags::EXCL | OFlags::WRONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::RUSR | Mode::WUSR,
            )
            .expect("fixture file descriptor"),
        );
        file.write_all(data).expect("fixture bytes");
    }

    fn long_key(length: usize, directory: bool) -> String {
        let mut key = PREFIX.to_owned();
        while length - key.len() > 200 {
            key.push_str(&"a".repeat(199));
            key.push('/');
        }
        key.push_str(&"b".repeat(length - key.len() - usize::from(directory)));
        if directory {
            key.push('/');
        }
        assert_eq!(key.len(), length);
        key
    }

    #[tokio::test]
    async fn artifact_inventory_long_data_and_directory_keys_are_complete() {
        let (temp, disk, id, identity) = fixture().await;
        let root = std::fs::File::open(temp.path()).expect("fixture root descriptor");
        let mut expected = std::collections::BTreeSet::new();
        for (length, directory) in [(1000, false), (1024, false), (1017, true), (1024, true)] {
            let key = long_key(length, directory);
            let physical = rustfs_utils::path::encode_dir_object(&key);
            let object = make_directory_at(&root, &format!("{BUCKET}/{physical}"));
            let version = Uuid::new_v4();
            let data_dir = Uuid::new_v4();
            let mut info = test_file_info(&key, version, Some(data_dir), directory.then(Bytes::new));
            if directory {
                info.set_inline_data();
            }
            write_at(&object, "xl.meta", &test_meta(info));
            if !directory {
                let parts = make_directory_at(&object, &data_dir.to_string());
                write_at(&parts, "part.1", b"x");
            }
            expected.insert((key, version));
        }
        close(&disk, identity).await;
        let (result, frames) = collect(&disk, id, identity).await;
        assert!(result.is_ok(), "A03I1_LOGICAL_KEY_BOUNDARY: {result:?}");
        let actual = frames
            .iter()
            .filter_map(|frame| match frame {
                ArtifactInventoryFrame::Entries { versions, .. } => Some(versions),
                _ => None,
            })
            .flatten()
            .map(|version| (version.key.clone(), version.version_id))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            actual, expected,
            "long owned data dirs and encoded directory objects are not logical objects themselves"
        );
        assert!(matches!(frames.last(), Some(ArtifactInventoryFrame::Complete { entries: 4, .. })));

        // A real namespace object's metadata inside the data-dir still has its
        // own logical key limit; only the payload-directory path is exempt.
        let key = long_key(1024, false);
        let object = make_directory_at(&root, &format!("{BUCKET}/{key}"));
        let metadata = FileMeta::load(&{
            use rustix::fs::{Mode, OFlags, openat};
            use std::io::Read;
            let mut file =
                std::fs::File::from(openat(&object, "xl.meta", OFlags::RDONLY, Mode::empty()).expect("original metadata"));
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes).expect("metadata bytes");
            bytes
        })
        .expect("decode real metadata");
        let data_dir = metadata.get_data_dirs().expect("known data directory")[0].expect("non-inline data");
        let child_key = format!("{key}/{data_dir}");
        let child = make_directory_at(&object, &data_dir.to_string());
        let mut info = test_file_info(&child_key, Uuid::new_v4(), Some(Uuid::new_v4()), Some(Bytes::from_static(b"inline")));
        info.set_inline_data();
        write_at(&child, "xl.meta", &test_meta(info));
        let (result, frames) = collect(&disk, id, identity).await;
        assert_incomplete(result, &frames);
    }
}
