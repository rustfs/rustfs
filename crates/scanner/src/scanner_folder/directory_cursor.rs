// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

//! Cycle-local execution can borrow an interrupted physical directory stream.
//! Only the current scanner owns admission and classification; parked streams
//! have no task, budget or authority to update a bucket cache.

use std::collections::HashMap;

mod checkpoint;
mod pages;
pub use checkpoint::DirectoryCheckpoint;
pub(super) use checkpoint::Saved;
pub(crate) use checkpoint::{DirectoryProgress, DirectoryReadProgress};
use checkpoint::{Physical, Recorder, Scope};
use pages::NamePages;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::time::{Duration, Instant, SystemTime};

use super::{DirectoryStream, Disk, FolderScanMode, ScannerBucketPrefixScanScope, native_io};
use crate::data_usage_define::{DataUsageCacheInfo, DataUsageCacheSource, DataUsageScanIdentity, DataUsageScanProgress};
use crate::scanner_budget::ScannerCycleBudget;

pub(super) const PARKED_DIRECTORIES_PER_DISK: usize = 32;

/// Only a positively identified disk can re-adopt a linked tree. A missing
/// format on an otherwise visible local drive uses its directory generation;
/// genuine format I/O errors propagate before any cached subtotals are used.
pub(super) async fn physical_source(disk: &Arc<Disk>) -> Result<String, super::ScannerError> {
    use crate::storage_api::owner::EcstoreDiskAPI;
    use sha2::{Digest, Sha256};
    let endpoint = disk.endpoint().to_string();
    let generation = match disk.get_disk_id().await {
        Ok(Some(id)) if !id.is_nil() => id.to_string(),
        Ok(None) | Err(super::DiskError::UnformattedDisk) => {
            let path = disk.path();
            let stamp = DirectoryStamp::at(path.to_str().ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?).await?;
            // Bucket creation and scanner startup can change the volume root's
            // mtime/ctime. Birth time distinguishes inode reuse without making
            // ordinary directory content changes appear to replace the disk.
            if let Some(generation) = stamp.volume_generation() {
                let bytes = rmp_serde::to_vec_named(&(std::env::consts::OS, generation)).map_err(io::Error::other)?;
                hex_simd::encode_to_string(Sha256::digest(bytes), hex_simd::AsciiCase::Lower)
            } else {
                // The platform cannot attest the generation across processes.
                uuid::Uuid::new_v4().to_string()
            }
        }
        Ok(Some(_)) => return Err(io::Error::new(io::ErrorKind::InvalidData, "scanner disk format contains a nil ID").into()),
        Err(error) => return Err(io::Error::other(error).into()),
    };
    Ok(format!("{generation}|{endpoint}"))
}

#[derive(Clone)]
pub(super) struct DirectoryScope {
    disk: Weak<Disk>,
    bucket: Arc<str>,
    source: DataUsageCacheSource,
    identity: DataUsageScanIdentity,
    progress: DataUsageScanProgress,
    mode: FolderScanMode,
    prefix_scope: Option<ScannerBucketPrefixScanScope>,
    owner: (u64, u64),
    endpoint: String,
    watch: Option<[u8; 32]>,
    continuing: bool,
}

impl DirectoryScope {
    pub(super) fn new(
        disk: &Arc<Disk>,
        cache: &super::DataUsageCache,
        mode: FolderScanMode,
        prefix_scope: Option<ScannerBucketPrefixScanScope>,
    ) -> Option<Self> {
        use crate::storage_api::owner::EcstoreDiskAPI;
        let info = &cache.info;
        Some(Self {
            disk: Arc::downgrade(disk),
            bucket: Arc::from(info.name.as_str()),
            source: info.source?,
            identity: info.scan_identity.filter(DataUsageScanIdentity::is_valid)?,
            progress: info.scan_progress?,
            mode,
            prefix_scope,
            owner: (info.leader_epoch, info.next_cycle),
            endpoint: format!("|{}", disk.endpoint()),
            watch: info
                .scan_usage_watch
                .as_ref()
                .filter(|proof| proof.is_well_formed())
                .map(|proof| proof.digest().0),
            continuing: cache.has_scan_position(),
        })
    }

    pub(super) fn bind_source(&mut self, source: Option<&str>) {
        if let Some(source) = source {
            self.endpoint = source.to_owned();
        }
    }
    pub(super) fn endpoint(&self) -> &str {
        &self.endpoint
    }
    fn matches(&self, other: &Self) -> bool {
        self.endpoint == other.endpoint
            && self.disk.ptr_eq(&other.disk)
            && self.bucket == other.bucket
            && self.source == other.source
            && self.identity == other.identity
            && self.progress == other.progress
            && self.mode == other.mode
            && self.watch == other.watch
            && self.prefix_scope == other.prefix_scope
            && (other.continuing || self.owner == other.owner)
    }
}

#[derive(PartialEq, Eq)]
struct DirectoryStamp {
    modified: SystemTime,
    birth: Option<SystemTime>,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    changed: (i64, i64),
    #[cfg(windows)]
    identity: same_file::Handle,
    #[cfg(windows)]
    physical: Option<(u64, u64, i64)>,
}

impl DirectoryStamp {
    fn volume_generation(&self) -> Option<(u64, u64, SystemTime)> {
        #[cfg(unix)]
        {
            Some((self.device, self.inode, self.birth?))
        }
        #[cfg(windows)]
        {
            let (volume, file, _) = self.physical?;
            Some((volume, file, self.birth?))
        }
        #[cfg(not(any(unix, windows)))]
        {
            None
        }
    }
    async fn at(path: &str) -> io::Result<Self> {
        let path = PathBuf::from(path);
        crate::storage_api::ecstore_spawn_blocking_with_resource_ownership(move || {
            let meta = std::fs::symlink_metadata(&path)?;
            if !meta.is_dir() {
                return Err(io::ErrorKind::NotADirectory.into());
            }
            #[cfg(unix)]
            use std::os::unix::fs::MetadataExt;
            #[cfg(windows)]
            let identity = same_file::Handle::from_path(&path)?;
            #[cfg(windows)]
            let physical = winapi_util::file::information(identity.as_file()).ok().and_then(|info| {
                let created = i64::try_from(info.creation_time()?).ok()?;
                (info.file_index() != 0).then_some((info.volume_serial_number(), info.file_index(), created))
            });
            Ok(Self {
                modified: meta.modified()?,
                birth: meta.created().ok(),
                #[cfg(unix)]
                device: meta.dev(),
                #[cfg(unix)]
                inode: meta.ino(),
                #[cfg(unix)]
                changed: (meta.ctime(), meta.ctime_nsec()),
                #[cfg(windows)]
                identity,
                #[cfg(windows)]
                physical,
            })
        })
        .await
        .map_err(io::Error::other)?
    }
}

#[derive(Default)]
pub(super) struct DirectoryWork {
    names: NamePages,
    // For a prefix tree the canonical child values live in the bucket cache.
    // The final compaction threshold caps how many paths this list can hold.
    pub(super) linked: Vec<String>,
    pub(super) compacted_total: super::DataUsageEntry,
    pub(super) gap: bool,
    compacted: bool,
    recorder: Option<Recorder>,
}

impl DirectoryWork {
    pub(super) fn restore_mode(&mut self, compacted: bool) {
        if self.compacted && !compacted {
            self.rewind();
        }
    }
    fn maybe_checkpoint(&mut self) {
        if let Some(recorder) = &mut self.recorder
            && recorder.last_update.elapsed() >= Duration::from_secs(1)
        {
            recorder.last_update = Instant::now();
            self.checkpoint();
        }
    }
    pub(super) fn checkpoint(&self) {
        if let Some(recorder) = &self.recorder {
            recorder.save(self);
        }
    }
    pub(super) async fn next_name(&mut self) -> io::Result<Option<String>> {
        self.names.next().await
    }
    pub(super) fn acknowledge(&mut self, name: &str, linked: bool, compacted: bool) {
        if self.names.acknowledge() {
            self.compacted = compacted;
            if linked && !compacted {
                self.linked.push(name.to_owned());
            }
            self.maybe_checkpoint();
        }
    }
    pub(super) fn rewind(&mut self) {
        self.compacted = false;
        self.names.rewind();
        self.linked.clear();
        self.compacted_total = super::DataUsageEntry::default();
        self.gap = false;
    }
}

struct Listing {
    reader: Option<DirectoryStream>,
    pending: Option<native_io::DirEntry>,
    delivery: Option<(native_io::DirEntry, bool)>,
    classified: Option<Option<std::fs::FileType>>,
    markers: [Option<native_io::DirEntry>; 2],
    abandoned: Option<super::DataUsageHashMap>,
    work: DirectoryWork,
    failed: bool,
}

struct Parked {
    scope: DirectoryScope,
    parked_at: Instant,
    serial: u64,
    stamp: DirectoryStamp,
    listing: Box<Listing>,
}

static PARKED: LazyLock<Mutex<HashMap<PathBuf, Parked>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

fn expired_slots(slots: &mut HashMap<PathBuf, Parked>) -> Vec<(PathBuf, Parked)> {
    slots
        .extract_if(|_, parked| {
            parked.scope.disk.strong_count() == 0 || parked.parked_at.elapsed() > Duration::from_secs(24 * 3600)
        })
        .collect()
}

/// A routing hint only. The admitted walker revalidates the physical source,
/// bucket instance, coverage plan and directory before using a parked stream.
pub(crate) fn parked_bucket_disks(bucket: &str) -> Vec<Weak<Disk>> {
    let mut slots = PARKED.lock().unwrap_or_else(|p| p.into_inner());
    let expired = expired_slots(&mut slots);
    let matches = slots
        .values()
        .filter(|entry| entry.scope.bucket.as_ref() == bucket)
        .map(|entry| entry.scope.disk.clone())
        .collect();
    drop(slots);
    drop(expired);
    matches
}

pub(super) fn is_parked(path: &str) -> bool {
    PARKED.lock().unwrap_or_else(|p| p.into_inner()).contains_key(Path::new(path))
}

pub(super) fn forget_bucket(disk: &Arc<Disk>, info: &DataUsageCacheInfo) {
    let owner = Arc::downgrade(disk);
    let mut slots = PARKED.lock().unwrap_or_else(|p| p.into_inner());
    let removed = slots
        .extract_if(|_, parked| {
            parked.scope.disk.ptr_eq(&owner)
                && parked.scope.bucket.as_ref() == info.name
                && parked.scope.owner <= (info.leader_epoch, info.next_cycle)
        })
        .collect::<Vec<_>>();
    drop(slots);
    drop(removed);
}

// Prefer existing active work over cyclic LRU eviction. Under pressure, a
// deleted or replaced directory must not monopolize one of the physical slots.
async fn reclaim_retired(scope: &DirectoryScope, path: &Path) {
    static NEXT_PROBE: AtomicUsize = AtomicUsize::new(0);
    let candidate = {
        let slots = PARKED.lock().unwrap_or_else(|p| p.into_inner());
        if slots.contains_key(path) {
            return;
        }
        let disk_slots: Vec<_> = slots
            .iter()
            .filter(|(_, parked)| parked.scope.disk.ptr_eq(&scope.disk))
            .collect();
        if disk_slots.len() < PARKED_DIRECTORIES_PER_DISK {
            return;
        }
        let (path, parked) = disk_slots[NEXT_PROBE.fetch_add(1, Ordering::Relaxed) % disk_slots.len()];
        (path.clone(), parked.serial)
    };
    let Some(candidate_path) = candidate.0.to_str() else { return };
    let current = DirectoryStamp::at(candidate_path).await;
    let mut slots = PARKED.lock().unwrap_or_else(|p| p.into_inner());
    let stale = slots.get(&candidate.0).is_some_and(|parked| {
        parked.serial == candidate.1
            && match &current {
                Ok(stamp) => stamp != &parked.stamp,
                Err(error) => matches!(error.kind(), io::ErrorKind::NotFound | io::ErrorKind::NotADirectory),
            }
    });
    let retired = stale.then(|| slots.remove(&candidate.0));
    drop(slots);
    drop(retired);
}

fn valid_component(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 4096
        && name != "."
        && name != ".."
        && !name.as_bytes().contains(&0)
        && std::path::Path::new(name)
            .file_name()
            .is_some_and(|part| part == std::ffi::OsStr::new(name))
}

pub(super) struct ScannerDirectory {
    path: Arc<Path>,
    scope: Option<(DirectoryScope, DirectoryStamp)>,
    directory_name: Arc<str>,
    budget: Arc<ScannerCycleBudget>,
    listing: Option<Box<Listing>>,
    marker: usize,
    pub(super) enumeration_only: bool,
    resumed: bool,
    pub(super) directory_replaced: bool,
    pub(super) batch_entries: usize,
}

impl ScannerDirectory {
    pub(super) async fn open(
        path: &str,
        directory_name: &str,
        scope: Option<DirectoryScope>,
        budget: Arc<ScannerCycleBudget>,
        saved: Option<Saved>,
    ) -> io::Result<Self> {
        // A different physical directory must never inherit old names. Directory
        // timestamps supplement (and cannot replace) the scanner's plan fences.
        let stamp = if scope.is_some() {
            Some(DirectoryStamp::at(path).await?)
        } else {
            None
        };
        if let Some(scope) = &scope {
            reclaim_retired(scope, Path::new(path)).await;
        }
        let parked = scope.as_ref().and_then(|current| {
            let mut slots = PARKED.lock().unwrap_or_else(|p| p.into_inner());
            let key = Path::new(path);
            if slots
                .get(key)
                .is_some_and(|p| p.scope.disk.ptr_eq(&current.disk) && p.scope.owner > current.owner)
            {
                return None;
            }
            slots.remove(key)
        });
        let directory_replaced = parked.as_ref().is_some_and(|entry| Some(&entry.stamp) != stamp.as_ref());
        let parked =
            parked.filter(|entry| !directory_replaced && scope.as_ref().is_some_and(|current| entry.scope.matches(current)));
        let mut resumed = parked.is_some();
        let mut previous = saved
            .as_ref()
            .and_then(|slot| slot.lock().unwrap_or_else(|p| p.into_inner()).clone());
        let durable_scope = scope.as_ref().map(Scope::new);
        let endpoint = scope.as_ref().map(|s| s.endpoint.clone()).unwrap_or_default();
        let physical = scope
            .as_ref()
            .zip(stamp.as_ref())
            .and_then(|(scope, stamp)| Physical::of(&scope.endpoint, stamp));
        let mut replaced = directory_replaced
            || previous
                .as_ref()
                .is_some_and(|old| old.endpoint() == endpoint && old.physical.is_some() && old.physical != physical);
        previous = previous.filter(|old| {
            durable_scope
                .as_ref()
                .is_some_and(|current| old.restore_allowed(directory_name, current, &physical))
        });
        let mut listing = if let Some(parked) = parked {
            parked.listing
        } else {
            let mut reader = None;
            if let Some(checkpoint) = &previous {
                if checkpoint.sealed {
                    resumed = true;
                } else if let Some(anchor) = &checkpoint.anchor {
                    match DirectoryStream::open_at(path, anchor).await {
                        Ok(restored) => {
                            reader = Some(restored);
                            resumed = true;
                        }
                        Err(error)
                            if matches!(
                                error.kind(),
                                io::ErrorKind::InvalidData
                                    | io::ErrorKind::Unsupported
                                    | io::ErrorKind::NotFound
                                    | io::ErrorKind::InvalidInput
                            ) =>
                        {
                            previous = None;
                            replaced = true;
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
            if !resumed {
                reader = Some(DirectoryStream::open(path, scope.is_some()).await?);
            }
            let mut work = DirectoryWork::default();
            let mut markers = [None, None];
            if let Some(saved) = &previous {
                if resumed && let Some(owner) = &scope {
                    tracing::debug!(
                        event = super::EVENT_SCANNER_FOLDER_STATE,
                        component = super::LOG_COMPONENT_SCANNER,
                        subsystem = super::LOG_SUBSYSTEM_FOLDER,
                        state = "directory_checkpoint_restored",
                        bucket = %owner.bucket,
                        directory = directory_name,
                        disk = %endpoint,
                        leader_epoch = owner.owner.0,
                        cycle = owner.owner.1,
                        restore_mode = if saved.sealed { "sealed_membership" } else { "native_position" },
                        entries_read = saved.entries_read,
                        completed_through = saved.aggregate.as_ref().map(|aggregate| aggregate.through.as_str()),
                        "Restored directory checkpoint"
                    );
                }
                work.names = NamePages::restore(&saved.pages, saved.sealed, saved.aggregate.as_ref().map(|a| a.through.as_str()));
                if let Some(aggregate) = &saved.aggregate {
                    work.compacted = true;
                    work.compacted_total = aggregate.value.clone();
                }
                let physical_path: Arc<Path> = Arc::from(Path::new(path));
                if let Some(part) = &saved.part {
                    markers[0] = Some(native_io::DirEntry::recorded(physical_path.clone(), part.into(), None));
                }
                if saved.metadata {
                    markers[1] = Some(native_io::DirEntry::recorded(physical_path, super::STORAGE_FORMAT_FILE.into(), None));
                }
            }
            Box::new(Listing {
                reader,
                pending: None,
                delivery: None,
                classified: None,
                markers,
                abandoned: None,
                work,
                failed: false,
            })
        };
        if let (Some(slot), Some(durable_scope)) = (saved, durable_scope) {
            if let Some(recorder) = &mut listing.work.recorder {
                recorder.slot = slot;
            } else {
                let recorder = Recorder::new(slot, directory_name, durable_scope, endpoint, physical, previous.as_ref());

                listing.work.recorder = Some(recorder);
            }
        }
        Ok(Self {
            path: Arc::from(Path::new(path)),
            scope: scope.zip(stamp),
            directory_name: Arc::from(directory_name),
            budget,
            listing: Some(listing),
            marker: 0,
            enumeration_only: false,
            resumed,
            directory_replaced: replaced,
            batch_entries: 0,
        })
    }

    pub(super) fn continues_stream(&self) -> bool {
        self.resumed
    }
    pub(super) fn indexed(&self) -> bool {
        self.scope.is_some()
    }
    pub(super) async fn verify_current(&mut self) -> io::Result<()> {
        let Some((_, original)) = &self.scope else { return Ok(()) };
        let path = self.path.to_str().ok_or(io::ErrorKind::InvalidInput)?;
        let current = DirectoryStamp::at(path).await?;
        if *original == current {
            return Ok(());
        }
        if let Some(listing) = &mut self.listing {
            listing.failed = true;
        }
        Err(io::Error::new(io::ErrorKind::Interrupted, "scanner directory changed during traversal"))
    }

    pub(super) fn folder_count(&self) -> Option<usize> {
        self.indexed()
            .then(|| self.listing.as_ref().map_or(0, |l| l.work.names.len()))
    }
    pub(super) fn finished_listing(&self) -> bool {
        self.listing.as_ref().is_some_and(|l| l.work.names.ready())
    }
    pub(super) fn work(&mut self) -> Option<&mut DirectoryWork> {
        if !self.indexed() {
            return None;
        }
        self.listing.as_mut().map(|listing| &mut listing.work)
    }
    pub(super) fn resolve_abandoned(&mut self, path: &str) {
        if let Some(abandoned) = self.listing.as_mut().and_then(|l| l.abandoned.as_mut()) {
            abandoned.remove(path);
        }
    }
    pub(super) fn hold_abandoned(&mut self, baseline: &mut super::DataUsageHashMap) {
        if self.indexed()
            && let Some(listing) = &mut self.listing
            && listing.abandoned.is_none()
        {
            if !baseline.is_empty() && self.resumed {
                // Cold restore carries classified members, not the previous
                // process's transient unresolved heal candidates.
                for page in listing.work.names.snapshot() {
                    for name in page.names() {
                        baseline.remove(&super::path_join_buf(&[&self.directory_name, name]));
                    }
                }
            }
            listing.abandoned = Some(std::mem::take(baseline));
        }
    }
    pub(super) fn restore_abandoned(&self, baseline: &mut super::DataUsageHashMap) {
        if let Some(abandoned) = self.listing.as_ref().and_then(|listing| listing.abandoned.as_ref()) {
            baseline.clone_from(abandoned);
        }
    }

    pub(super) async fn peek_entry(&mut self) -> io::Result<Option<native_io::DirEntry>> {
        if let Some((entry, enumeration_only)) = self.listing.as_ref().and_then(|listing| listing.delivery.as_ref()) {
            self.enumeration_only = *enumeration_only;
            return Ok(Some(entry.clone()));
        }
        let entry = self.next_entry().await?;
        if let (Some(listing), Some(entry)) = (&mut self.listing, &entry) {
            listing.delivery = Some((entry.clone(), self.enumeration_only));
        }
        Ok(entry)
    }

    pub(super) fn accept_entry(&mut self) {
        let Some(listing) = &mut self.listing else { return };
        let Some((entry, _)) = listing.delivery.take() else { return };
        // Discovering a name can precede the I/O admission wait. Commit its
        // classification and native position together only after admission;
        // a cold retry then re-reads any name still pending on cancellation.
        let Some(kind) = listing.classified.take() else { return };
        let name = entry.file_name();
        let text = name.to_string_lossy();
        if let Some(state) = &mut listing.work.recorder {
            state.anchor = entry.anchor().cloned();
            state.entries_read = state.entries_read.saturating_add(1);
            state.valid_names &= name.to_str().is_some_and(valid_component);
        }
        if let Some(kind) = kind {
            let direct_part = kind.is_file() && super::is_erasure_part_name(&text);
            if kind.is_dir() {
                if let Some(abandoned) = &mut listing.abandoned
                    && !abandoned.is_empty()
                {
                    abandoned.remove(&super::path_join_buf(&[&self.directory_name, &text]));
                }
                listing.work.names.add(text.into_owned());
            } else if direct_part {
                if let Some(state) = &mut listing.work.recorder {
                    state.part.get_or_insert_with(|| text.to_string());
                }
                listing.markers[0].get_or_insert_with(|| native_io::DirEntry::recorded(self.path.clone(), name, Some(kind)));
            } else if text == super::STORAGE_FORMAT_FILE {
                if let Some(state) = &mut listing.work.recorder {
                    state.metadata = true;
                }
                listing.markers[1] = Some(native_io::DirEntry::recorded(self.path.clone(), name, Some(kind)));
            }
        }
        listing.work.maybe_checkpoint();
    }

    async fn next_entry(&mut self) -> io::Result<Option<native_io::DirEntry>> {
        let erasure_candidate = self
            .scope
            .as_ref()
            .is_some_and(|(scope, _)| scope.mode != FolderScanMode::Namespace);
        let indexed = self.indexed();
        let listing = self
            .listing
            .as_mut()
            .ok_or_else(|| io::Error::other("scanner directory already returned"))?;
        self.enumeration_only = false;
        if let Some(reader) = listing.reader.as_mut() {
            if !indexed {
                return reader.next_entry().await;
            }
            listing.work.names.flush_full().await?;
            if listing.pending.is_none() {
                listing.pending = match reader.next_entry().await {
                    Ok(entry) => entry,
                    Err(error) => {
                        listing.failed = true;
                        return Err(error);
                    }
                };
            }
            if let Some(entry) = &listing.pending {
                let kind = match entry.file_type().await {
                    Ok(kind) => Some(kind),
                    Err(error) if matches!(error.kind(), io::ErrorKind::NotFound | io::ErrorKind::TooManyLinks) => None,
                    Err(error) => {
                        listing.failed = true;
                        return Err(error);
                    }
                };
                let name = entry.file_name();
                let text = name.to_string_lossy();
                self.enumeration_only = kind.is_none_or(|kind| {
                    (!kind.is_symlink() || text == super::STORAGE_FORMAT_FILE)
                        && !(erasure_candidate && kind.is_file() && super::is_erasure_part_name(&text))
                });
                listing.classified = Some(kind);
                return Ok(listing.pending.take());
            }
            listing.reader = None;
            listing.work.names.finish();
            listing.work.maybe_checkpoint();
        }
        while self.marker < listing.markers.len() {
            let marker = self.marker;
            self.marker += 1;
            if let Some(entry) = &listing.markers[marker] {
                return Ok(Some(entry.clone()));
            }
        }
        Ok(None)
    }
}

impl Drop for ScannerDirectory {
    fn drop(&mut self) {
        let Some((scope, stamp)) = self.scope.take() else { return };
        let Some(listing) = self.listing.take() else { return };
        if listing.failed || self.budget.reason().is_none() {
            if let Some(recorder) = &listing.work.recorder {
                *recorder.slot.lock().unwrap_or_else(|p| p.into_inner()) = None;
            }
            return;
        }
        listing.work.checkpoint();
        let mut slots = PARKED.lock().unwrap_or_else(|p| p.into_inner());
        let expired = expired_slots(&mut slots);
        let key = self.path.to_path_buf();
        let replaced = slots
            .get(&key)
            .is_some_and(|p| !p.scope.disk.ptr_eq(&scope.disk) || p.scope.owner < scope.owner)
            .then(|| slots.remove(&key));
        if slots.values().filter(|p| p.scope.disk.ptr_eq(&scope.disk)).count() < PARKED_DIRECTORIES_PER_DISK
            && let std::collections::hash_map::Entry::Vacant(slot) = slots.entry(key)
        {
            static NEXT_SERIAL: AtomicU64 = AtomicU64::new(0);
            slot.insert(Parked {
                scope,
                parked_at: Instant::now(),
                serial: NEXT_SERIAL.fetch_add(1, Ordering::Relaxed),
                stamp,
                listing,
            });
        }
        drop(slots);
        drop((expired, replaced));
    }
}
