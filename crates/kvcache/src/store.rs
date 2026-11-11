// Copyright 2024 RustFS Team
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

//! The per-node store: one writer per drive, an in-memory index of every
//! entry this node holds, rebuilt from segment files at open.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::{Mutex, RwLock};

use crate::aligned::AlignedBuf;
use crate::capacity::{CapacityPolicy, FsStats};
use crate::error::{KvError, KvResult};
use crate::format::{check_key, entry_footprint};
use crate::segment::{SegmentFile, SegmentId, SegmentWriter, WriterOptions, scan_segment, segment_file_name};

const INDEX_SHARDS: usize = 64;
const SEGMENT_SUFFIX: &str = ".seg";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreOptions {
    pub segment_bytes: u64,
    pub direct_io: bool,
    pub sync_on_seal: bool,
}

impl StoreOptions {
    fn writer(&self) -> WriterOptions {
        WriterOptions {
            segment_bytes: self.segment_bytes,
            direct_io: self.direct_io,
            sync_on_seal: self.sync_on_seal,
        }
    }
}

/// Where an entry lives. Holding it keeps the segment file open.
#[derive(Debug, Clone)]
struct Location {
    drive: u16,
    segment: Arc<SegmentFile>,
    offset: u64,
    value_len: u64,
    value_crc: u32,
    write_time_ms: u64,
}

/// What a lookup reports about an entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryInfo {
    pub drive: u16,
    pub segment: SegmentId,
    pub value_len: u64,
    pub value_crc: u32,
    pub write_time_ms: u64,
}

/// One name of a listing: a stored key, or a common prefix of keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListItem {
    Entry(String, EntryInfo),
    Prefix(String),
}

/// A value read from a segment: an aligned buffer and the value's length.
pub struct Value {
    buf: AlignedBuf,
    len: usize,
}

impl Value {
    pub fn as_slice(&self) -> &[u8] {
        &self.buf[..self.len]
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutOutcome {
    Written,
    /// The key was already stored; nothing was written.
    Exists,
}

/// Where an append landed.
struct Appended {
    id: SegmentId,
    file: Arc<SegmentFile>,
    offset: u64,
    value_crc: u32,
}

/// A segment as the evictor sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentStat {
    pub drive: u16,
    pub id: SegmentId,
    pub sealed: bool,
    /// Write time of the segment's newest entry (unix ms), or its creation.
    pub newest_write_ms: u64,
    pub entries: usize,
    /// Bytes of entries still in the index.
    pub live_bytes: u64,
}

#[derive(Debug)]
struct SegmentInfo {
    file: Arc<SegmentFile>,
    sealed: bool,
    newest_write_ms: u64,
    keys: Vec<String>,
    live_bytes: u64,
}

#[derive(Debug)]
struct DriveState {
    dir: PathBuf,
    writer: Mutex<Option<SegmentWriter>>,
    /// Preallocated empty segments, so a rotation never waits on the
    /// filesystem.
    spares: Mutex<Vec<SegmentWriter>>,
    segments: RwLock<BTreeMap<SegmentId, SegmentInfo>>,
    next_id: Mutex<SegmentId>,
    /// Above the emergency watermark: new entries are refused.
    full: AtomicBool,
}

/// What open found on disk.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RecoveryReport {
    pub segments: usize,
    /// Unsealed segments found (and sealed) at open.
    pub resealed: usize,
    /// Segment files that could not be read and were removed.
    pub removed: usize,
    pub entries: usize,
    /// Entries skipped because an older segment already held the key.
    pub duplicates: usize,
    /// Empty unsealed segments (unused spares, or a crash before the first
    /// entry) that were removed.
    pub empty_removed: usize,
    /// Tombstones replayed (persisted deletes).
    pub tombstones: usize,
}

/// What one eviction pass did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct EvictionReport {
    /// Segments dropped for exceeding the maximum age.
    pub aged_segments: usize,
    /// Segments dropped to bring a drive under its low watermark.
    pub capacity_segments: usize,
    pub keys_removed: usize,
    /// Drives left above the emergency watermark, refusing new entries.
    pub full_drives: Vec<u16>,
}

pub struct KvStore {
    options: StoreOptions,
    drives: Vec<DriveState>,
    index: Vec<RwLock<HashMap<String, Location>>>,
}

fn entry_info(location: &Location) -> EntryInfo {
    EntryInfo {
        drive: location.drive,
        segment: location.segment.id(),
        value_len: location.value_len,
        value_crc: location.value_crc,
        write_time_ms: location.write_time_ms,
    }
}

fn shard_of(key: &str) -> usize {
    crate::format::crc32c(key.as_bytes()) as usize % INDEX_SHARDS
}

fn parse_segment_id(name: &str) -> Option<SegmentId> {
    let hex = name.strip_suffix(SEGMENT_SUFFIX)?;
    if hex.len() != 16 {
        return None;
    }
    SegmentId::from_str_radix(hex, 16).ok()
}

impl KvStore {
    /// Opens the store over `drive_dirs` (index = drive number), creating the
    /// directories, and rebuilds the index from their segment files.
    /// Unsealed segments are cut at their first torn entry and sealed; older
    /// segments win when two hold the same key.
    pub fn open(options: StoreOptions, drive_dirs: Vec<PathBuf>, now_ms: u64) -> KvResult<(Self, RecoveryReport)> {
        if drive_dirs.len() > usize::from(u16::MAX) {
            return Err(KvError::Corrupt("too many drives".into()));
        }
        let store = Self {
            options,
            drives: drive_dirs
                .into_iter()
                .map(|dir| DriveState {
                    dir,
                    writer: Mutex::new(None),
                    spares: Mutex::new(Vec::new()),
                    segments: RwLock::new(BTreeMap::new()),
                    next_id: Mutex::new(1),
                    full: AtomicBool::new(false),
                })
                .collect(),
            index: (0..INDEX_SHARDS).map(|_| RwLock::new(HashMap::new())).collect(),
        };
        let mut report = RecoveryReport::default();
        for drive in 0..store.drives.len() {
            store.recover_drive(drive as u16, now_ms, &mut report)?;
        }
        Ok((store, report))
    }

    fn recover_drive(&self, drive: u16, now_ms: u64, report: &mut RecoveryReport) -> KvResult<()> {
        let state = &self.drives[usize::from(drive)];
        std::fs::create_dir_all(&state.dir)?;
        let mut ids: Vec<SegmentId> = std::fs::read_dir(&state.dir)?
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| parse_segment_id(&entry.file_name().to_string_lossy()))
            .collect();
        ids.sort_unstable();
        *state.next_id.lock() = ids.last().map_or(1, |id| id + 1);

        for id in ids {
            let path = state.dir.join(segment_file_name(id));
            let scanned = match scan_segment(&path, id, self.options.direct_io) {
                Ok(scanned) => scanned,
                Err(KvError::Corrupt(_)) => {
                    std::fs::remove_file(&path)?;
                    report.removed += 1;
                    continue;
                }
                Err(err) => return Err(err),
            };
            if !scanned.sealed && scanned.entries.is_empty() {
                std::fs::remove_file(&path)?;
                report.empty_removed += 1;
                continue;
            }
            if !scanned.sealed {
                let mut writer = SegmentWriter::resume(&scanned, &path, self.options.writer())?;
                writer.seal(now_ms)?;
                report.resealed += 1;
            }
            let file = Arc::new(SegmentFile::open(&path, id, self.options.direct_io)?);
            let mut info = SegmentInfo {
                file: Arc::clone(&file),
                sealed: true,
                newest_write_ms: 0,
                keys: Vec::with_capacity(scanned.entries.len()),
                live_bytes: 0,
            };
            for (offset, meta) in scanned.entries {
                info.newest_write_ms = info.newest_write_ms.max(meta.write_time_ms);
                if meta.tombstone {
                    // A delete replayed in write order: drop the key if a
                    // segment replayed before this one (or this one) holds it.
                    let removed = self.index[shard_of(&meta.key)].write().remove(&meta.key);
                    if let Some(removed) = removed {
                        let footprint = entry_footprint(removed.value_len);
                        if removed.segment.id() == id && usize::from(removed.drive) == usize::from(drive) {
                            info.live_bytes = info.live_bytes.saturating_sub(footprint);
                        } else if let Some(owner) = self.drives[usize::from(removed.drive)]
                            .segments
                            .write()
                            .get_mut(&removed.segment.id())
                        {
                            owner.live_bytes = owner.live_bytes.saturating_sub(footprint);
                        }
                        report.entries -= 1;
                    }
                    report.tombstones += 1;
                    continue;
                }
                let location = Location {
                    drive,
                    segment: Arc::clone(&file),
                    offset,
                    value_len: meta.value_len,
                    value_crc: meta.value_crc,
                    write_time_ms: meta.write_time_ms,
                };
                if self.insert_if_absent(&meta.key, location) {
                    info.live_bytes += entry_footprint(meta.value_len);
                    info.keys.push(meta.key);
                    report.entries += 1;
                } else {
                    report.duplicates += 1;
                }
            }
            if info.newest_write_ms == 0 {
                // An empty segment ages from when it was sealed.
                info.newest_write_ms = scanned.sealed_time_ms.unwrap_or(now_ms);
            }
            state.segments.write().insert(id, info);
            report.segments += 1;
        }
        Ok(())
    }

    fn create_segment(&self, state: &DriveState) -> KvResult<SegmentWriter> {
        let id = {
            let mut next = state.next_id.lock();
            let id = *next;
            *next += 1;
            id
        };
        SegmentWriter::create(&state.dir, id, self.options.writer())
    }

    /// Tops every drive that is not full up to `per_drive` preallocated
    /// spare segments. Returns the number created.
    pub fn replenish_spares(&self, per_drive: usize) -> KvResult<usize> {
        let mut created = 0;
        for state in &self.drives {
            if state.full.load(Ordering::Acquire) {
                continue;
            }
            while state.spares.lock().len() < per_drive {
                let spare = self.create_segment(state)?;
                state.spares.lock().push(spare);
                created += 1;
            }
        }
        Ok(created)
    }

    /// Whether `drive` refuses new entries.
    pub fn is_full(&self, drive: u16) -> bool {
        self.drives
            .get(usize::from(drive))
            .is_some_and(|state| state.full.load(Ordering::Acquire))
    }

    /// One capacity pass over every drive: drop sealed segments older than
    /// the maximum age (sealing an idle open segment that aged out), then,
    /// above the high watermark, drop the oldest sealed segments until the
    /// drive's estimated usage is below the low watermark. A drive still at
    /// or above the emergency watermark refuses new entries until a later
    /// pass finds it below.
    pub fn evict(&self, fs: &dyn FsStats, policy: &CapacityPolicy, now_ms: u64) -> KvResult<EvictionReport> {
        let mut report = EvictionReport::default();
        for (drive, state) in self.drives.iter().enumerate() {
            let drive = drive as u16;
            if let Some(max_age_ms) = policy.max_age_ms {
                let expired = |newest_write_ms: u64| newest_write_ms.saturating_add(max_age_ms) <= now_ms;
                {
                    let mut writer_slot = state.writer.lock();
                    let idle = writer_slot.as_ref().is_some_and(|writer| {
                        state
                            .segments
                            .read()
                            .get(&writer.id())
                            .is_some_and(|info| expired(info.newest_write_ms))
                    });
                    if idle && let Some(mut writer) = writer_slot.take() {
                        writer.seal(now_ms)?;
                        if let Some(info) = state.segments.write().get_mut(&writer.id()) {
                            info.sealed = true;
                        }
                    }
                }
                let aged: Vec<SegmentId> = self
                    .segments(drive)
                    .into_iter()
                    .filter(|segment| segment.sealed && expired(segment.newest_write_ms))
                    .map(|segment| segment.id)
                    .collect();
                for id in aged {
                    report.keys_removed += self.drop_segment(drive, id)?;
                    report.aged_segments += 1;
                }
            }

            let usage = fs.usage(&state.dir)?;
            let total = usage.total_bytes.max(1) as f64;
            let mut used = usage.total_bytes.saturating_sub(usage.available_bytes) as f64;
            if used / total >= policy.watermarks.high {
                let mut candidates: Vec<SegmentStat> =
                    self.segments(drive).into_iter().filter(|segment| segment.sealed).collect();
                candidates.sort_by_key(|segment| (segment.newest_write_ms, segment.id));
                for segment in candidates {
                    if used / total < policy.watermarks.low {
                        break;
                    }
                    report.keys_removed += self.drop_segment(drive, segment.id)?;
                    report.capacity_segments += 1;
                    used -= self.options.segment_bytes as f64;
                }
            }
            let full = used / total >= policy.watermarks.emergency;
            state.full.store(full, Ordering::Release);
            if full {
                // Spares hold space a full drive needs; release them.
                for spare in state.spares.lock().drain(..) {
                    let _ = std::fs::remove_file(spare.path());
                }
                report.full_drives.push(drive);
            }
        }
        Ok(report)
    }

    fn insert_if_absent(&self, key: &str, location: Location) -> bool {
        let mut shard = self.index[shard_of(key)].write();
        if shard.contains_key(key) {
            return false;
        }
        shard.insert(key.to_owned(), location);
        true
    }

    fn locate(&self, key: &str) -> Option<Location> {
        self.index[shard_of(key)].read().get(key).cloned()
    }

    pub fn drive_count(&self) -> usize {
        self.drives.len()
    }

    pub fn contains(&self, key: &str) -> bool {
        self.index[shard_of(key)].read().contains_key(key)
    }

    /// Stores `value` under `key` on `drive` unless the key is already stored.
    pub fn put(&self, drive: u16, key: &str, value: &[u8], now_ms: u64) -> KvResult<PutOutcome> {
        check_key(key)?;
        let state = self
            .drives
            .get(usize::from(drive))
            .ok_or_else(|| KvError::Corrupt(format!("no drive {drive}")))?;
        if self.contains(key) {
            return Ok(PutOutcome::Exists);
        }
        if state.full.load(Ordering::Acquire) {
            return Err(KvError::CacheFull { drive });
        }
        if !SegmentWriter::fits_empty(key, value.len() as u64, self.options.segment_bytes) {
            return Err(KvError::ValueTooLarge {
                len: value.len() as u64,
                segment_bytes: self.options.segment_bytes,
            });
        }

        let mut writer_slot = state.writer.lock();
        // Re-check under the drive's writer lock: a concurrent put of the
        // same key on this drive may have finished meanwhile.
        if self.contains(key) {
            return Ok(PutOutcome::Exists);
        }
        let appended = self.append_locked(state, &mut writer_slot, key, value, now_ms, false)?;
        let location = Location {
            drive,
            segment: Arc::clone(&appended.file),
            offset: appended.offset,
            value_len: value.len() as u64,
            value_crc: appended.value_crc,
            write_time_ms: now_ms,
        };
        let inserted = self.insert_if_absent(key, location);
        if inserted && let Some(info) = state.segments.write().get_mut(&appended.id) {
            info.keys.push(key.to_owned());
            info.live_bytes += entry_footprint(value.len() as u64);
        }
        Ok(if inserted { PutOutcome::Written } else { PutOutcome::Exists })
    }

    /// Appends an entry (or a tombstone) to the drive's open segment under
    /// its writer lock, opening a segment (a spare when one is ready) and
    /// rotating full ones as needed.
    fn append_locked(
        &self,
        state: &DriveState,
        writer_slot: &mut Option<SegmentWriter>,
        key: &str,
        value: &[u8],
        now_ms: u64,
        tombstone: bool,
    ) -> KvResult<Appended> {
        loop {
            if writer_slot.is_none() {
                let writer = match state.spares.lock().pop() {
                    Some(spare) => spare,
                    None => self.create_segment(state)?,
                };
                let id = writer.id();
                let file = Arc::new(SegmentFile::open(writer.path(), id, self.options.direct_io)?);
                state.segments.write().insert(
                    id,
                    SegmentInfo {
                        file,
                        sealed: false,
                        newest_write_ms: now_ms,
                        keys: Vec::new(),
                        live_bytes: 0,
                    },
                );
                *writer_slot = Some(writer);
            }
            let writer = writer_slot.as_mut().expect("writer was just installed");
            let offset = if tombstone {
                writer.append_tombstone(key, now_ms)?
            } else {
                writer.append(key, value, now_ms)?
            };
            match offset {
                Some(offset) => {
                    let id = writer.id();
                    let value_crc = writer.entries().last().expect("entry was just appended").1.value_crc;
                    let mut segments = state.segments.write();
                    let info = segments.get_mut(&id).expect("open segment is registered");
                    info.newest_write_ms = info.newest_write_ms.max(now_ms);
                    return Ok(Appended {
                        id,
                        file: Arc::clone(&info.file),
                        offset,
                        value_crc,
                    });
                }
                None => {
                    // Full: seal it and start the next segment.
                    let mut full = writer_slot.take().expect("writer is present");
                    full.seal(now_ms)?;
                    if let Some(info) = state.segments.write().get_mut(&full.id()) {
                        info.sealed = true;
                    }
                }
            }
        }
    }

    /// Reads `key`. A value that fails its checksum is removed from the index
    /// and reported as a miss.
    pub fn get(&self, key: &str) -> KvResult<Option<Value>> {
        let Some(location) = self.locate(key) else {
            return Ok(None);
        };
        match location
            .segment
            .read_value(location.offset, location.value_len, location.value_crc)
        {
            Ok(buf) => Ok(Some(Value {
                buf,
                len: location.value_len as usize,
            })),
            Err(KvError::ChecksumMismatch) => {
                self.forget(key, &location);
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }

    fn forget(&self, key: &str, location: &Location) {
        let mut shard = self.index[shard_of(key)].write();
        if shard
            .get(key)
            .is_some_and(|current| Arc::ptr_eq(&current.segment, &location.segment) && current.offset == location.offset)
        {
            shard.remove(key);
            drop(shard);
            if let Some(info) = self.drives[usize::from(location.drive)]
                .segments
                .write()
                .get_mut(&location.segment.id())
            {
                info.live_bytes = info.live_bytes.saturating_sub(entry_footprint(location.value_len));
            }
        }
    }

    /// Looks `key` up without touching the segment's reference count, so
    /// concurrent lookups of entries in the same segment do not contend.
    pub fn head(&self, key: &str) -> Option<EntryInfo> {
        self.index[shard_of(key)].read().get(key).map(entry_info)
    }

    /// Deletes `key`: a tombstone is appended to its drive so the delete
    /// survives a restart, then the key leaves the index; its bytes are
    /// reclaimed with its segment. Allowed on a full drive (a tombstone is
    /// one block). Returns whether the key was stored.
    pub fn delete(&self, key: &str, now_ms: u64) -> KvResult<bool> {
        let Some(location) = self.locate(key) else {
            return Ok(false);
        };
        let state = &self.drives[usize::from(location.drive)];
        {
            let mut writer_slot = state.writer.lock();
            self.append_locked(state, &mut writer_slot, key, &[], now_ms, true)?;
        }
        self.forget(key, &location);
        Ok(true)
    }

    /// How many leading `keys` are stored.
    pub fn prefix_query<'a>(&self, keys: impl IntoIterator<Item = &'a str>) -> usize {
        keys.into_iter().take_while(|key| self.contains(key)).count()
    }

    /// Up to `max` stored keys with `prefix`, after `start_after`, sorted.
    /// Best effort: concurrent writes may or may not appear.
    /// The first `max` names after `start_after` under `prefix`, in key
    /// order. With a delimiter, keys that contain it after the prefix are
    /// listed once as their common prefix (S3 ListObjects rules), so a page
    /// of a few common prefixes costs one pass over the index however many
    /// keys they hold. Only the names kept are copied.
    pub fn list(&self, prefix: &str, delimiter: Option<&str>, start_after: Option<&str>, max: usize) -> Vec<ListItem> {
        let delimiter = delimiter.filter(|delimiter| !delimiter.is_empty());
        let mut names: BTreeMap<String, Option<EntryInfo>> = BTreeMap::new();
        if max == 0 {
            return Vec::new();
        }
        for shard in &self.index {
            for (key, location) in shard.read().iter() {
                let Some(rest) = key.strip_prefix(prefix) else {
                    continue;
                };
                let common = delimiter.and_then(|delimiter| rest.find(delimiter).map(|at| prefix.len() + at + delimiter.len()));
                let name = common.map_or(key.as_str(), |end| &key[..end]);
                if start_after.is_some_and(|after| name <= after)
                    || (names.len() == max && names.last_key_value().is_some_and(|(last, _)| name >= last.as_str()))
                    || names.contains_key(name)
                {
                    continue;
                }
                names.insert(name.to_owned(), common.is_none().then(|| entry_info(location)));
                if names.len() > max {
                    names.pop_last();
                }
            }
        }
        names
            .into_iter()
            .map(|(name, info)| match info {
                Some(info) => ListItem::Entry(name, info),
                None => ListItem::Prefix(name),
            })
            .collect()
    }

    /// Number of keys in the index.
    pub fn len(&self) -> usize {
        self.index.iter().map(|shard| shard.read().len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Seals every drive's open segment (shutdown, or ageing out the open
    /// segment so it can be evicted).
    pub fn seal_open_segments(&self, now_ms: u64) -> KvResult<()> {
        for state in &self.drives {
            let mut writer_slot = state.writer.lock();
            if let Some(mut writer) = writer_slot.take() {
                writer.seal(now_ms)?;
                if let Some(info) = state.segments.write().get_mut(&writer.id()) {
                    info.sealed = true;
                }
            }
        }
        Ok(())
    }

    /// Every segment on `drive`, oldest id first.
    pub fn segments(&self, drive: u16) -> Vec<SegmentStat> {
        self.drives
            .get(usize::from(drive))
            .map(|state| {
                state
                    .segments
                    .read()
                    .iter()
                    .map(|(id, info)| SegmentStat {
                        drive,
                        id: *id,
                        sealed: info.sealed,
                        newest_write_ms: info.newest_write_ms,
                        entries: info.keys.len(),
                        live_bytes: info.live_bytes,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Drops a sealed segment: its keys leave the index and its file is
    /// unlinked. Reads already holding the segment finish normally; the
    /// space is returned when the last of them ends. Returns the number of
    /// keys removed.
    pub fn drop_segment(&self, drive: u16, id: SegmentId) -> KvResult<usize> {
        let state = self
            .drives
            .get(usize::from(drive))
            .ok_or_else(|| KvError::Corrupt(format!("no drive {drive}")))?;
        let Some(info) = state.segments.write().remove(&id) else {
            return Ok(0);
        };
        if !info.sealed {
            // The open segment is sealed (and dropped) only by its writer.
            state.segments.write().insert(id, info);
            return Err(KvError::Corrupt(format!("segment {id} is still open")));
        }
        let mut removed = 0;
        for key in &info.keys {
            let mut shard = self.index[shard_of(key)].write();
            if shard
                .get(key)
                .is_some_and(|location| Arc::ptr_eq(&location.segment, &info.file))
            {
                shard.remove(key);
                removed += 1;
            }
        }
        match std::fs::remove_file(info.file.path()) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }
        Ok(removed)
    }

    /// The directory of `drive`.
    pub fn drive_dir(&self, drive: u16) -> Option<&Path> {
        self.drives.get(usize::from(drive)).map(|state| state.dir.as_path())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::capacity::DiskUsage;

    const SEGMENT: u64 = 256 * 1024;

    fn options() -> StoreOptions {
        StoreOptions {
            segment_bytes: SEGMENT,
            direct_io: false,
            sync_on_seal: false,
        }
    }

    fn value(seed: u8, len: usize) -> Vec<u8> {
        (0..len).map(|i| seed.wrapping_add((i % 241) as u8)).collect()
    }

    fn open(dirs: &[PathBuf]) -> (KvStore, RecoveryReport) {
        KvStore::open(options(), dirs.to_vec(), 1_000).expect("open")
    }

    fn drives(root: &Path, count: usize) -> Vec<PathBuf> {
        (0..count).map(|i| root.join(format!("d{i}"))).collect()
    }

    #[test]
    fn puts_read_back_and_writes_of_existing_keys_are_noops() {
        let root = tempfile::tempdir().expect("tempdir");
        let (store, _) = open(&drives(root.path(), 2));
        assert_eq!(store.put(0, "a", &value(1, 10_000), 5).expect("put"), PutOutcome::Written);
        assert_eq!(store.put(1, "b", &value(2, 3), 6).expect("put"), PutOutcome::Written);
        let offset_before = store.segments(0)[0].live_bytes;
        assert_eq!(store.put(0, "a", &value(9, 10_000), 7).expect("put"), PutOutcome::Exists);
        assert_eq!(store.segments(0)[0].live_bytes, offset_before, "an existing key writes nothing");

        assert_eq!(store.get("a").expect("get").expect("hit").as_slice(), &value(1, 10_000)[..]);
        assert_eq!(store.get("b").expect("get").expect("hit").as_slice(), &value(2, 3)[..]);
        assert!(store.get("missing").expect("get").is_none());
        assert_eq!(store.head("b").map(|info| (info.drive, info.value_len)), Some((1, 3)));

        assert!(store.delete("a", 8).expect("delete"));
        assert!(!store.delete("a", 9).expect("delete"));
        assert!(store.get("a").expect("get").is_none());
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn segments_rotate_when_full_and_old_segments_stay_readable() {
        let root = tempfile::tempdir().expect("tempdir");
        let (store, _) = open(&drives(root.path(), 1));
        for i in 0..20u8 {
            store
                .put(0, &format!("k{i:02}"), &value(i, 40_000), u64::from(i))
                .expect("put");
        }
        let segments = store.segments(0);
        assert!(segments.len() >= 4, "{segments:?}");
        assert!(segments[..segments.len() - 1].iter().all(|segment| segment.sealed));
        for i in 0..20u8 {
            let read = store.get(&format!("k{i:02}")).expect("get").expect("hit");
            assert_eq!(read.as_slice(), &value(i, 40_000)[..]);
        }
        assert_eq!(store.prefix_query(["k00", "k01", "k02", "zz", "k03"]), 3);
        let listed: Vec<_> = store
            .list("k1", None, Some("k12"), 3)
            .into_iter()
            .map(|item| match item {
                ListItem::Entry(key, _) => key,
                ListItem::Prefix(prefix) => panic!("no delimiter, got prefix {prefix}"),
            })
            .collect();
        assert_eq!(listed, ["k13", "k14", "k15"]);
    }

    #[test]
    fn listing_with_a_delimiter_rolls_keys_up_to_common_prefixes() {
        let root = tempfile::tempdir().expect("tempdir");
        let (store, _) = open(&drives(root.path(), 2));
        for (i, key) in ["a/1", "a/2", "b/1", "c", "d/x/1", "d/x/2", "d/y"].iter().enumerate() {
            store.put((i % 2) as u16, key, &value(i as u8, 100), 1).expect("put");
        }
        let names = |prefix, start_after, max| -> Vec<String> {
            store
                .list(prefix, Some("/"), start_after, max)
                .into_iter()
                .map(|item| match item {
                    ListItem::Entry(key, _) => key,
                    ListItem::Prefix(prefix) => format!("[{prefix}]"),
                })
                .collect()
        };
        assert_eq!(names("", None, 10), ["[a/]", "[b/]", "c", "[d/]"]);
        // A common prefix given as the start skips every key under it.
        assert_eq!(names("", Some("a/"), 10), ["[b/]", "c", "[d/]"]);
        assert_eq!(names("", None, 2), ["[a/]", "[b/]"]);
        assert_eq!(names("d/", None, 10), ["[d/x/]", "d/y"]);
        assert!(names("", None, 0).is_empty());
        // Without a delimiter every key is listed.
        assert_eq!(store.list("d/", None, None, 10).len(), 3);
    }

    #[test]
    fn reopen_rebuilds_the_index_and_drops_a_torn_tail() {
        let root = tempfile::tempdir().expect("tempdir");
        let dirs = drives(root.path(), 2);
        {
            let (store, _) = open(&dirs);
            for i in 0..12u8 {
                store
                    .put(u16::from(i % 2), &format!("k{i:02}"), &value(i, 30_000), 100 + u64::from(i))
                    .expect("put");
            }
            // Crash: open segments are left unsealed. Tear the last entry of drive 1.
            let last = store.head("k11").expect("present");
            let path = store.drive_dir(1).expect("dir").join(segment_file_name(last.segment));
            let file = std::fs::OpenOptions::new().write(true).open(path).expect("open");
            let location = store.locate("k11").expect("present");
            std::os::unix::fs::FileExt::write_all_at(&file, &[0xEE; 64], location.offset + 4096 + 10).expect("tear");
        }

        let (store, report) = open(&dirs);
        assert_eq!(report.entries, 11);
        assert!(report.resealed >= 2, "{report:?}");
        assert_eq!(store.len(), 11);
        assert!(store.get("k11").expect("get").is_none(), "the torn entry is gone");
        for i in 0..11u8 {
            let read = store.get(&format!("k{i:02}")).expect("get").expect("hit");
            assert_eq!(read.as_slice(), &value(i, 30_000)[..]);
        }
        // New writes go to new segments after the recovered ones.
        store.put(0, "after", b"x", 500).expect("put");
        let ids: Vec<_> = store.segments(0).iter().map(|segment| segment.id).collect();
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));

        let (reopened, report) = open(&dirs);
        assert_eq!(report.resealed, 1, "only the segment written after recovery was open");
        assert_eq!(reopened.len(), 12);
    }

    #[test]
    fn deletes_survive_reopen_and_a_later_put_wins_over_the_delete() {
        let root = tempfile::tempdir().expect("tempdir");
        let dirs = drives(root.path(), 1);
        {
            let (store, _) = open(&dirs);
            for i in 0..12u8 {
                store
                    .put(0, &format!("k{i:02}"), &value(i, 40_000), u64::from(i))
                    .expect("put");
            }
            // k00 lives in an older, sealed segment; its tombstone lands in the open one.
            assert!(store.delete("k00", 20).expect("delete"));
            assert!(store.delete("k11", 21).expect("delete"));
            assert!(!store.delete("missing", 22).expect("delete"));
            assert!(store.delete("k05", 23).expect("delete"));
            assert_eq!(store.put(0, "k05", &value(99, 10), 24).expect("put"), PutOutcome::Written);
            assert_eq!(store.len(), 10);
        }

        let (store, report) = open(&dirs);
        assert_eq!(report.tombstones, 3, "{report:?}");
        assert_eq!(store.len(), 10);
        assert!(store.get("k00").expect("get").is_none(), "a delete survives restart");
        assert!(store.get("k11").expect("get").is_none());
        assert_eq!(
            store.get("k05").expect("get").expect("rewritten").as_slice(),
            &value(99, 10)[..],
            "a put after the delete wins"
        );
        assert_eq!(store.get("k03").expect("get").expect("kept").as_slice(), &value(3, 40_000)[..]);
    }

    #[test]
    fn a_corrupt_value_becomes_a_miss_and_leaves_the_index() {
        let root = tempfile::tempdir().expect("tempdir");
        let (store, _) = open(&drives(root.path(), 1));
        store.put(0, "k", &value(4, 20_000), 1).expect("put");
        let location = store.locate("k").expect("present");
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(location.segment.path())
            .expect("open");
        std::os::unix::fs::FileExt::write_all_at(&file, &[0x11], location.offset + 4096 + 777).expect("flip");

        assert!(store.get("k").expect("get").is_none());
        assert!(!store.contains("k"));
    }

    #[test]
    fn dropping_a_segment_does_not_break_reads_in_flight() {
        let root = tempfile::tempdir().expect("tempdir");
        let (store, _) = open(&drives(root.path(), 1));
        for i in 0..10u8 {
            store.put(0, &format!("k{i}"), &value(i, 40_000), u64::from(i)).expect("put");
        }
        let first = store.segments(0)[0].clone();
        assert!(first.sealed);
        let in_flight = store.locate("k0").expect("present");

        let removed = store.drop_segment(0, first.id).expect("drop");
        assert_eq!(removed, first.entries);
        assert!(store.get("k0").expect("get").is_none());
        assert!(!in_flight.segment.path().exists(), "the file is unlinked");
        // The read that located the entry before the drop still completes.
        let read = in_flight
            .segment
            .read_value(in_flight.offset, in_flight.value_len, in_flight.value_crc)
            .expect("read after unlink");
        assert_eq!(&read[..40_000], &value(0, 40_000)[..]);

        let open_segment = store.segments(0).last().cloned().expect("open segment");
        assert!(!open_segment.sealed);
        assert!(store.drop_segment(0, open_segment.id).is_err(), "the open segment cannot be dropped");
        assert_eq!(store.drop_segment(0, first.id).expect("again"), 0);
    }

    struct FakeFs {
        usage: parking_lot::Mutex<HashMap<PathBuf, DiskUsage>>,
    }

    impl FakeFs {
        fn new() -> Self {
            Self {
                usage: parking_lot::Mutex::new(HashMap::new()),
            }
        }

        /// Sets the drive's usage fraction on a 10-segment filesystem.
        fn set(&self, dir: &Path, used_fraction: f64) {
            let total = 10 * SEGMENT;
            self.usage.lock().insert(
                dir.to_owned(),
                DiskUsage {
                    total_bytes: total,
                    available_bytes: total - (total as f64 * used_fraction) as u64,
                },
            );
        }
    }

    impl FsStats for FakeFs {
        fn usage(&self, dir: &Path) -> std::io::Result<DiskUsage> {
            Ok(*self.usage.lock().get(dir).expect("usage set for drive"))
        }
    }

    /// Fills `segments` sealed segments (five 40 KB entries each) with
    /// write times 100, 200, ... per segment.
    fn fill(store: &KvStore, segments: u64) {
        for segment in 0..segments {
            for entry in 0..5 {
                store
                    .put(0, &format!("s{segment}e{entry}"), &value(entry as u8, 40_000), 100 * (segment + 1))
                    .expect("put");
            }
        }
    }

    #[test]
    fn eviction_drops_oldest_sealed_segments_down_to_the_low_watermark() {
        let root = tempfile::tempdir().expect("tempdir");
        let dirs = drives(root.path(), 1);
        let (store, _) = open(&dirs);
        fill(&store, 6);
        let policy = CapacityPolicy::default();
        let fs = FakeFs::new();

        fs.set(&dirs[0], 0.75);
        assert_eq!(store.evict(&fs, &policy, 1_000).expect("evict"), EvictionReport::default());

        // 85% of ten segments: drop oldest until below 70% (two segments).
        fs.set(&dirs[0], 0.85);
        let report = store.evict(&fs, &policy, 1_000).expect("evict");
        assert_eq!(report.capacity_segments, 2);
        assert_eq!(report.keys_removed, 10);
        assert!(report.full_drives.is_empty());
        assert!(!store.contains("s0e0") && !store.contains("s1e4"));
        assert!(store.contains("s2e0") && store.contains("s5e4"));
        assert!(
            store.segments(0).iter().any(|segment| !segment.sealed),
            "the open segment is never evicted"
        );
    }

    #[test]
    fn a_drive_above_the_emergency_watermark_refuses_writes_until_it_recovers() {
        let root = tempfile::tempdir().expect("tempdir");
        let dirs = drives(root.path(), 1);
        let (store, _) = open(&dirs);
        store.put(0, "only", &value(1, 1_000), 1).expect("put");
        store.replenish_spares(2).expect("spares");
        let fs = FakeFs::new();
        let policy = CapacityPolicy::default();

        // Nothing sealed to drop: still at 95% after the pass.
        fs.set(&dirs[0], 0.95);
        let report = store.evict(&fs, &policy, 10).expect("evict");
        assert_eq!(report.full_drives, vec![0]);
        assert!(store.is_full(0));
        assert!(matches!(store.put(0, "new", b"x", 11), Err(KvError::CacheFull { drive: 0 })));
        assert_eq!(store.put(0, "only", b"x", 11).expect("existing key"), PutOutcome::Exists);
        assert!(store.get("only").expect("get").is_some(), "reads continue");
        assert_eq!(store.replenish_spares(2).expect("spares"), 0, "a full drive gets no spares");

        fs.set(&dirs[0], 0.50);
        assert!(store.evict(&fs, &policy, 12).expect("evict").full_drives.is_empty());
        assert_eq!(store.put(0, "new", b"x", 13).expect("put"), PutOutcome::Written);
    }

    #[test]
    fn aged_segments_are_dropped_even_with_space_left() {
        let root = tempfile::tempdir().expect("tempdir");
        let dirs = drives(root.path(), 1);
        let (store, _) = open(&dirs);
        fill(&store, 3); // segments written at 100, 200, 300; the last stays open
        let fs = FakeFs::new();
        fs.set(&dirs[0], 0.10);
        let policy = CapacityPolicy {
            max_age_ms: Some(1_000),
            ..CapacityPolicy::default()
        };

        let report = store.evict(&fs, &policy, 1_150).expect("evict");
        assert_eq!(report.aged_segments, 1, "only the segment written at 100 is older than 1000 ms");
        assert!(!store.contains("s0e0") && store.contains("s1e0"));

        // Much later the idle open segment is sealed and ages out too.
        let report = store.evict(&fs, &policy, 10_000).expect("evict");
        assert_eq!(report.aged_segments, 2);
        assert!(store.is_empty());
        assert!(store.segments(0).is_empty());
    }

    #[test]
    fn spares_are_used_for_rotation_and_removed_at_recovery_when_unused() {
        let root = tempfile::tempdir().expect("tempdir");
        let dirs = drives(root.path(), 1);
        let count_files = || std::fs::read_dir(&dirs[0]).expect("read_dir").count();
        {
            let (store, _) = open(&dirs);
            assert_eq!(store.replenish_spares(3).expect("spares"), 3);
            assert_eq!(count_files(), 3);
            store.put(0, "k", b"value", 1).expect("put");
            assert_eq!(count_files(), 3, "the first segment came from the spares");
            assert_eq!(store.replenish_spares(3).expect("spares"), 1);
        }
        let (store, report) = open(&dirs);
        assert_eq!(report.empty_removed, 3, "{report:?}");
        assert_eq!(report.resealed, 1);
        assert_eq!(count_files(), 1);
        assert!(store.get("k").expect("get").is_some());
    }

    #[test]
    fn concurrent_puts_of_one_key_store_it_once() {
        let root = tempfile::tempdir().expect("tempdir");
        let (store, _) = open(&drives(root.path(), 1));
        let store = Arc::new(store);
        let outcomes: Vec<PutOutcome> = std::thread::scope(|scope| {
            // Start every writer before joining any of them.
            let mut handles = Vec::new();
            for i in 0..8 {
                let store = Arc::clone(&store);
                handles.push(scope.spawn(move || store.put(0, "same", &value(i, 5_000), 1).expect("put")));
            }
            handles.into_iter().map(|handle| handle.join().expect("join")).collect()
        });
        assert_eq!(outcomes.iter().filter(|outcome| **outcome == PutOutcome::Written).count(), 1);
        assert_eq!(store.segments(0)[0].entries, 1);
    }
}
