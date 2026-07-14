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

use crate::error::StoreError;
use rustfs_config::notify::{COMPRESS_EXT, DEFAULT_EXT};
use rustfs_config::{DEFAULT_LIMIT, DEFAULT_TARGET_STORE_COMPRESS, ENV_TARGET_STORE_COMPRESS, EnableState};
use serde::{Serialize, de::DeserializeOwned};
use snap::raw::{Decoder, Encoder};
use std::{
    collections::HashMap,
    fs::File,
    io::Write,
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tracing::{debug, warn};
use uuid::Uuid;

const LOG_COMPONENT_TARGETS: &str = "targets";
const LOG_SUBSYSTEM_STORE: &str = "store";
const EVENT_TARGET_STORE_STATE: &str = "target_store_state";

/// Suffix used for the temporary file of an in-progress atomic write. A crash
/// between `File::create` and `rename` leaves one of these behind; `open()`
/// removes them so they are never mistaken for committed queue entries.
const TMP_SUFFIX: &str = ".tmp";

/// Upper bound applied to the initial `HashMap`/`Vec` capacities derived from
/// untrusted inputs (`entry_limit`, batch `item_count`). Growth is still lazy,
/// so a huge configured limit or a malicious/corrupt filename can no longer
/// trigger a giant up-front allocation or a capacity-overflow panic.
const MAX_PREALLOC_CAPACITY: usize = 4096;

/// Returns true if `file_name` looks like a committed queue entry for `file_ext`.
///
/// A committed entry is `<...><file_ext>` optionally followed by [`COMPRESS_EXT`]
/// (e.g. `<uuid>.event` or `3:<uuid>.event.snappy`). Any other file in the queue
/// directory (foreign files, leftover temp files) is ignored so it can never be
/// indexed and replayed as if it were a real event.
fn is_queue_file_name(file_name: &str, file_ext: &str) -> bool {
    if file_ext.is_empty() {
        return false;
    }
    let base = file_name.strip_suffix(COMPRESS_EXT).unwrap_or(file_name);
    base.ends_with(file_ext)
}

/// Separator between the batch item count and the entry name in an on-disk batch filename. It must
/// stay outside the Windows reserved filename set so File::create succeeds on NTFS, and outside the
/// UUID alphabet so the count splits unambiguously when the name is parsed back.
const BATCH_COUNT_SEPARATOR: char = '_';

/// Legacy separator for the batch item count. Entries written with it are read back for
/// compatibility, but it is reserved on NTFS so it is never written.
const LEGACY_BATCH_COUNT_SEPARATOR: char = ':';

/// Name of the child directory inside a target queue directory that holds events which failed
/// terminally. It is created lazily on the first failed write, so a target that never fails
/// terminally creates no such directory.
const FAILED_STORE_SUBDIR: &str = "failed";

/// Maximum number of entries retained in the failed store per target, bounded independently of the
/// live queue so an accumulation of terminal failures cannot starve live events. At the bound the
/// oldest failed entry is dropped to admit the newer one, so a newer failure is never lost in favour
/// of an older one.
const FAILED_STORE_MAX_ENTRIES: usize = 10_000;

/// Maximum age of a failed-store entry before it is removed as expired, measured from the file write
/// time, which is the instant the entry entered the failed store.
const FAILED_STORE_TTL: Duration = Duration::from_secs(72 * 60 * 60);

/// Writes payload to a temp file in the same directory, flushes the file to disk, then atomically
/// renames it onto final_path.
///
/// rename within a single directory is atomic on the supported filesystems, so a crash leaves
/// either final_path absent, the prior final_path, or the complete payload, never a partial file.
/// On failure the temp file is removed so an interrupted write leaves nothing behind.
fn write_temp_then_rename(temp_path: &Path, final_path: &Path, payload: &[u8]) -> Result<(), StoreError> {
    if let Err(err) = write_and_sync_temp(temp_path, payload) {
        let _ = std::fs::remove_file(temp_path);
        return Err(err);
    }

    if let Err(err) = std::fs::rename(temp_path, final_path) {
        let _ = std::fs::remove_file(temp_path);
        return Err(StoreError::Io(err));
    }

    Ok(())
}

/// Writes payload to path and flushes the file to disk before returning, so the bytes are durable
/// before the caller renames the file into place.
fn write_and_sync_temp(path: &Path, payload: &[u8]) -> Result<(), StoreError> {
    let mut file = std::fs::File::create(path).map_err(StoreError::Io)?;
    file.write_all(payload).map_err(StoreError::Io)?;
    file.sync_all().map_err(StoreError::Io)?;
    Ok(())
}

fn resolve_queue_store_compression_from_env_value(value: Option<&str>) -> bool {
    value
        .and_then(|value| value.parse::<EnableState>().ok().map(|state| state.is_enabled()))
        .unwrap_or(DEFAULT_TARGET_STORE_COMPRESS)
}

fn queue_store_compression_enabled() -> bool {
    let value = std::env::var(ENV_TARGET_STORE_COMPRESS).ok();
    resolve_queue_store_compression_from_env_value(value.as_deref())
}

/// Represents a key for an entry in the store
#[derive(Debug, Clone)]
pub struct Key {
    /// The name of the key (UUID)
    pub name: String,
    /// The file extension for the entry
    pub extension: String,
    /// The number of items in the entry (for batch storage)
    pub item_count: usize,
    /// Whether the entry is compressed
    pub compress: bool,
}

impl Key {
    /// Converts the key to a string (filename)
    pub fn to_key_string(&self) -> String {
        self.to_key_string_with(BATCH_COUNT_SEPARATOR)
    }

    /// Builds the filename using the given batch count separator.
    fn to_key_string_with(&self, separator: char) -> String {
        let name_part = if self.item_count > 1 {
            format!("{}{separator}{}", self.item_count, self.name)
        } else {
            self.name.clone()
        };

        let mut file_name = name_part;
        if !self.extension.is_empty() {
            file_name.push_str(&self.extension);
        }

        if self.compress {
            file_name.push_str(COMPRESS_EXT);
        }
        file_name
    }
}

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_key_string())
    }
}

/// Parses a string into a Key
pub fn parse_key(s: &str) -> Key {
    debug!(
        event = EVENT_TARGET_STORE_STATE,
        component = LOG_COMPONENT_TARGETS,
        subsystem = LOG_SUBSYSTEM_STORE,
        action = "parse_key",
        key = %s,
        "target store state"
    );

    let mut name = s.to_string();
    let mut extension = String::new();
    let mut item_count = 1;
    let mut compress = false;

    // Check for compressed suffixes
    if name.ends_with(COMPRESS_EXT) {
        compress = true;
        name = name[..name.len() - COMPRESS_EXT.len()].to_string();
    }

    // Number of batch items parsed. The current separator and the legacy one are both accepted so an
    // entry written before the switch to the Windows-safe separator still reads back. The count is
    // stripped only when it exceeds one, mirroring the render side which prefixes the count only for a
    // real batch, so a hostile 1_ or 0_ prefix on a single-item name stays part of the name.
    if let Some(separator_pos) = name.find([BATCH_COUNT_SEPARATOR, LEGACY_BATCH_COUNT_SEPARATOR])
        && let Ok(count) = name[..separator_pos].parse::<usize>()
        && count > 1
    {
        item_count = count;
        name = name[separator_pos + 1..].to_string();
    }

    // Resolve extension
    if let Some(dot_pos) = name.rfind('.') {
        extension = name[dot_pos..].to_string();
        name = name[..dot_pos].to_string();
    }

    debug!(
        event = EVENT_TARGET_STORE_STATE,
        component = LOG_COMPONENT_TARGETS,
        subsystem = LOG_SUBSYSTEM_STORE,
        action = "parse_key",
        result = "parsed",
        key_name = %name,
        extension = %extension,
        item_count,
        compressed = compress,
        "target store state"
    );

    Key {
        name,
        extension,
        item_count,
        compress,
    }
}

pub fn ensure_store_entry_raw_readable<T>(
    store: &(dyn Store<T, Error = StoreError, Key = Key> + Send),
    key: &Key,
) -> Result<bool, StoreError>
where
    T: Send + Sync + 'static + Clone + Serialize,
{
    match store.get_raw(key) {
        Ok(_) => Ok(true),
        Err(StoreError::NotFound) => Ok(false),
        Err(err) => {
            match store.del(key) {
                Ok(()) | Err(StoreError::NotFound) => {}
                Err(del_err) => {
                    return Err(StoreError::Internal(format!("Failed to remove unreadable store entry {key}: {del_err}")));
                }
            }
            Err(err)
        }
    }
}

/// Trait for a store that can store and retrieve items of type T
pub trait Store<T>: Send + Sync
where
    T: Send + Sync + 'static + Clone + Serialize,
{
    /// The error type for the store
    type Error;
    /// The key type for the store
    type Key;

    /// Opens the store
    fn open(&self) -> Result<(), Self::Error>;

    /// Stores a single item
    fn put(&self, item: Arc<T>) -> Result<Self::Key, Self::Error>;

    /// Stores multiple items in a single batch
    fn put_multiple(&self, items: Vec<T>) -> Result<Self::Key, Self::Error>;

    /// Stores raw bytes in a single entry.
    fn put_raw(&self, data: &[u8]) -> Result<Self::Key, Self::Error>;

    /// Retrieves a single item by key
    fn get(&self, key: &Self::Key) -> Result<T, Self::Error>;

    /// Retrieves multiple items by key
    fn get_multiple(&self, key: &Self::Key) -> Result<Vec<T>, Self::Error>;

    /// Retrieves the raw bytes stored for a key.
    fn get_raw(&self, key: &Self::Key) -> Result<Vec<u8>, Self::Error>;

    /// Deletes an item by key
    fn del(&self, key: &Self::Key) -> Result<(), Self::Error>;

    /// Deletes the underlying store directory and clears all in-memory state.
    fn delete(&self) -> Result<(), Self::Error>;

    /// Lists all keys in the store
    fn list(&self) -> Vec<Self::Key>;

    /// Returns the number of items in the store
    fn len(&self) -> usize;

    /// Returns true if the store is empty
    fn is_empty(&self) -> bool;

    /// Clones the store into a boxed trait object
    fn boxed_clone(&self) -> Box<dyn Store<T, Error = Self::Error, Key = Self::Key> + Send + Sync>;
}

/// A failed-events store nested inside the queue directory for terminal entries, kept separate from the generic queue interface
/// so only the paths that record terminal failures carry the surface.
///
/// The failed store holds events that could not be handed off and is bounded independently of the
/// live queue. At the count bound the oldest failed entry is dropped before a new entry is written, so
/// a newer failure is never lost in favour of an older one.
pub trait FailedEventStore: Send + Sync {
    /// Writes pre-encoded bytes to the failed-events store under the given entry name and returns the
    /// written entry id.
    ///
    /// The write is atomic. The entry name derives from the live entry, so a repeated move of the same
    /// entry replaces its earlier failed file instead of accumulating duplicates.
    fn put_failed_raw(&self, entry_name: &str, data: &[u8]) -> Result<String, StoreError>;

    /// Removes failed-store entries older than the retention bound and returns how many were removed.
    ///
    /// Runs on the replay maintenance tick, not a separate timer.
    fn prune_failed_store(&self) -> Result<usize, StoreError>;

    /// Returns the number of entries currently in the failed store.
    fn failed_len(&self) -> usize;

    /// Clones the capability into an owned boxed handle sharing the same backing state, so the
    /// maintenance scan can run inside a blocking task without borrowing the target.
    fn boxed_clone_failed(&self) -> Box<dyn FailedEventStore>;
}

/// A store that uses the filesystem to persist events in a queue
pub struct QueueStore<T> {
    entry_limit: u64,
    directory: PathBuf,
    file_ext: String,
    compress: bool,
    entries: Arc<RwLock<HashMap<String, i64>>>, // key -> modtime as unix nano
    pending_entries: Arc<AtomicU64>,
    /// Cached count of complete failed-store entries, shared across clones. Initialized by one
    /// directory read in open() and kept current by the failed-store write, the capacity trim, and
    /// the expired-entry removal, so failed_len() is a plain atomic load rather than a directory scan.
    failed_count: Arc<AtomicU64>,
    /// Serializes the failed-store writers and the maintenance scan among themselves. put_failed_raw,
    /// the capacity trim it runs, and the expiry-plus-reconcile scan take this guard, so the at-bound
    /// check and the write stay atomic and the scan reconcile cannot overwrite a concurrent write's
    /// count update. The live queue path never takes it. Shared across clones through the Arc.
    failed_store_guard: Arc<Mutex<()>>,
    fs_guard: Arc<RwLock<()>>,
    _phantom: PhantomData<T>,
}

impl<T> Clone for QueueStore<T> {
    fn clone(&self) -> Self {
        QueueStore {
            entry_limit: self.entry_limit,
            directory: self.directory.clone(),
            file_ext: self.file_ext.clone(),
            compress: self.compress,
            entries: Arc::clone(&self.entries),
            pending_entries: Arc::clone(&self.pending_entries),
            failed_count: Arc::clone(&self.failed_count),
            failed_store_guard: Arc::clone(&self.failed_store_guard),
            fs_guard: Arc::clone(&self.fs_guard),
            _phantom: PhantomData,
        }
    }
}

struct EntryReservation<'a> {
    pending_entries: &'a AtomicU64,
}

impl Drop for EntryReservation<'_> {
    fn drop(&mut self) {
        self.pending_entries.fetch_sub(1, Ordering::SeqCst);
    }
}

impl<T: Serialize + DeserializeOwned + Send + Sync> QueueStore<T> {
    /// Creates a new QueueStore
    pub fn new(directory: impl Into<PathBuf>, limit: u64, ext: &str) -> Self {
        Self::new_with_compression(directory, limit, ext, queue_store_compression_enabled())
    }

    /// Creates a new QueueStore with an explicit compression setting.
    pub fn new_with_compression(directory: impl Into<PathBuf>, limit: u64, ext: &str, compress: bool) -> Self {
        let file_ext = if ext.is_empty() { DEFAULT_EXT } else { ext };
        let entry_limit = if limit == 0 { DEFAULT_LIMIT } else { limit };

        QueueStore {
            directory: directory.into(),
            entry_limit,
            file_ext: file_ext.to_string(),
            compress,
            entries: Arc::new(RwLock::new(HashMap::with_capacity((entry_limit as usize).min(MAX_PREALLOC_CAPACITY)))),
            pending_entries: Arc::new(AtomicU64::new(0)),
            failed_count: Arc::new(AtomicU64::new(0)),
            failed_store_guard: Arc::new(Mutex::new(())),
            fs_guard: Arc::new(RwLock::new(())),
            _phantom: PhantomData,
        }
    }

    /// Returns the full path for a key. A batch entry (item_count > 1) may exist on disk under the
    /// legacy separator, so when the current-separator path is absent and the legacy-separator path
    /// exists, the legacy path is returned. Reads and deletes then resolve a legacy file while fresh
    /// writes keep the current separator.
    fn file_path(&self, key: &Key) -> PathBuf {
        let path = self.directory.join(key.to_key_string());
        if key.item_count > 1 && !path.exists() {
            let legacy = self.directory.join(key.to_key_string_with(LEGACY_BATCH_COUNT_SEPARATOR));
            if legacy.exists() {
                return legacy;
            }
        }
        path
    }

    fn build_key(&self, item_count: usize) -> Key {
        Key {
            // UUIDv7 is time-ordered: sorting entries by name reproduces FIFO
            // enqueue order deterministically and, crucially, identically after
            // a restart — the ordering is intrinsic to the persisted filename
            // rather than derived from coarse, clock-dependent file mtimes.
            name: Uuid::now_v7().to_string(),
            extension: self.file_ext.clone(),
            item_count,
            compress: self.compress,
        }
    }

    /// Best-effort `fsync` of the queue directory so a freshly `rename`d entry
    /// (and its containing directory entry) survives a power loss. Failure is
    /// logged at debug and not propagated: the data file itself is already
    /// durably `fsync`ed, and not every filesystem/platform supports directory
    /// fsync.
    fn fsync_dir(dir: &Path) {
        match File::open(dir) {
            Ok(dir_file) => {
                if let Err(err) = dir_file.sync_all() {
                    debug!(
                        event = EVENT_TARGET_STORE_STATE,
                        component = LOG_COMPONENT_TARGETS,
                        subsystem = LOG_SUBSYSTEM_STORE,
                        action = "fsync_dir",
                        dir = %dir.display(),
                        error = %err,
                        "target store state"
                    );
                }
            }
            Err(err) => {
                debug!(
                    event = EVENT_TARGET_STORE_STATE,
                    component = LOG_COMPONENT_TARGETS,
                    subsystem = LOG_SUBSYSTEM_STORE,
                    action = "fsync_dir_open",
                    dir = %dir.display(),
                    error = %err,
                    "target store state"
                );
            }
        }
    }

    /// Reads a file for the given key
    fn read_file(&self, key: &Key) -> Result<Vec<u8>, StoreError> {
        let _fs_guard = self
            .fs_guard
            .read()
            .map_err(|_| StoreError::Internal("Failed to acquire read lock on store filesystem".to_string()))?;
        let path = self.file_path(key);
        debug!(
            event = EVENT_TARGET_STORE_STATE,
            component = LOG_COMPONENT_TARGETS,
            subsystem = LOG_SUBSYSTEM_STORE,
            action = "read_file",
            key = %key,
            path = %path.display(),
            "target store state"
        );
        let data = std::fs::read(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StoreError::NotFound
            } else {
                StoreError::Io(e)
            }
        })?;

        if data.is_empty() {
            return Err(StoreError::NotFound);
        }

        if !key.compress {
            return Ok(data);
        }

        let mut decoder = Decoder::new();
        decoder
            .decompress_vec(&data)
            .map_err(|e| StoreError::Compression(e.to_string()))
    }

    fn reserve_entry_slot(&self) -> Result<EntryReservation<'_>, StoreError> {
        loop {
            let entries = self
                .entries
                .read()
                .map_err(|_| StoreError::Internal("Failed to acquire read lock on entries".to_string()))?;
            let entries_len = entries.len() as u64;
            let pending = self.pending_entries.load(Ordering::SeqCst);

            if entries_len + pending >= self.entry_limit {
                return Err(StoreError::LimitExceeded);
            }

            if self
                .pending_entries
                .compare_exchange(pending, pending + 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return Ok(EntryReservation {
                    pending_entries: self.pending_entries.as_ref(),
                });
            }
        }
    }

    /// Durably and atomically writes data to the file for the given key.
    ///
    /// The write is crash-safe: bytes are written to a per-key temporary file,
    /// `fsync`ed (`sync_all`), and only then `rename`d onto the final path. A
    /// same-directory `rename` is atomic, so a reader (including `open()` after a
    /// restart) observes either the complete previous file or the complete new
    /// one — never a half-written payload. On a crash mid-write the temp file is
    /// left behind and cleaned up by `open()`, so an acknowledged event is never
    /// lost and no ghost/truncated entry is ever indexed.
    fn write_file(&self, key: &Key, data: &[u8]) -> Result<i64, StoreError> {
        let path = self.file_path(key);
        let parent = path
            .parent()
            .ok_or_else(|| StoreError::Internal(format!("store entry path {} has no parent directory", path.display())))?;
        std::fs::create_dir_all(parent).map_err(StoreError::Io)?;

        let payload: std::borrow::Cow<'_, [u8]> = if key.compress {
            let mut encoder = Encoder::new();
            let compressed = encoder
                .compress_vec(data)
                .map_err(|e| StoreError::Compression(e.to_string()))?;
            std::borrow::Cow::Owned(compressed)
        } else {
            std::borrow::Cow::Borrowed(data)
        };

        let tmp_path = {
            let mut file_name = key.to_key_string();
            file_name.push_str(TMP_SUFFIX);
            self.directory.join(file_name)
        };

        // One implementation of the write-fsync-rename invariant, shared with the failed-store write.
        // The temp file is removed on any error path so a failed write leaves no residue.
        write_temp_then_rename(&tmp_path, &path, &payload)?;

        // Best-effort: persist the new directory entry created by rename.
        Self::fsync_dir(&self.directory);

        let modified = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as i64;
        debug!(
            event = EVENT_TARGET_STORE_STATE,
            component = LOG_COMPONENT_TARGETS,
            subsystem = LOG_SUBSYSTEM_STORE,
            action = "write_file",
            key = %key,
            "target store state"
        );
        Ok(modified)
    }

    fn insert_entry(&self, key: &Key, modified: i64) -> Result<(), StoreError> {
        let mut entries = self
            .entries
            .write()
            .map_err(|_| StoreError::Internal("Failed to acquire write lock on entries".to_string()))?;
        entries.insert(key.to_key_string(), modified);
        Ok(())
    }

    fn remove_file_if_present(&self, key: &Key) -> Result<(), StoreError> {
        let path = self.file_path(key);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(StoreError::Io(err)),
        }
    }

    fn write_and_index(&self, key: &Key, data: &[u8]) -> Result<(), StoreError> {
        let modified = self.write_file(key, data)?;
        if let Err(err) = self.insert_entry(key, modified) {
            self.remove_file_if_present(key).map_err(|cleanup_err| {
                StoreError::Internal(format!("Failed to index store entry {key}: {err}; cleanup failed: {cleanup_err}"))
            })?;
            return Err(err);
        }
        Ok(())
    }

    /// Path of the failed-events child directory inside the live queue directory.
    fn failed_dir(&self) -> PathBuf {
        self.directory.join(FAILED_STORE_SUBDIR)
    }

    /// Reads the failed directory once and counts complete entries, skipping non-file entries and
    /// residual temp files from an interrupted write. Off the hot path, so it seeds the cached count
    /// at open and reconciles it on the maintenance scan rather than serving failed_len.
    fn count_failed_entries_on_disk(&self) -> u64 {
        let read_dir = match std::fs::read_dir(self.failed_dir()) {
            Ok(read_dir) => read_dir,
            Err(_) => return 0,
        };

        let mut count = 0u64;
        for entry in read_dir.flatten() {
            if entry.file_name().to_string_lossy().ends_with(TMP_SUFFIX) {
                continue;
            }
            // file_type reads the directory entry kind without a full stat where the platform records it.
            if matches!(entry.file_type(), Ok(file_type) if file_type.is_file()) {
                count += 1;
            }
        }
        count
    }

    /// Lowers the cached failed count by one removed entry, clamped at zero so a decrement can never
    /// wrap. The maintenance scan reconciles any residual drift each interval.
    fn decrement_failed_count(&self) {
        // The closure always returns Some, so the update never fails and the Result is discarded.
        let _ = self
            .failed_count
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| Some(current.saturating_sub(1)));
    }

    /// Maps a per-entry stat outcome inside the ordered failed scan. A NotFound error means the file
    /// was removed between the directory listing and the stat, by the capacity trim or the
    /// expired-entry removal on another handle, so the scan skips that entry (None) instead of
    /// failing as a whole. Any other error still fails the scan.
    fn failed_scan_entry_metadata(outcome: std::io::Result<std::fs::Metadata>) -> Result<Option<std::fs::Metadata>, StoreError> {
        match outcome {
            Ok(metadata) => Ok(Some(metadata)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(StoreError::Io(err)),
        }
    }

    /// Filenames of current failed-store entries paired with their write time, oldest first.
    ///
    /// The directory is read fresh each call rather than indexed in memory, because the failed store
    /// is a low-traffic operator surface and a fresh scan keeps it free of the live-queue bookkeeping.
    /// A missing directory yields an empty list, so a target that never failed reports nothing. An
    /// entry removed concurrently mid-scan is skipped.
    fn failed_entries_oldest_first(&self) -> Result<Vec<(PathBuf, SystemTime)>, StoreError> {
        let dir = self.failed_dir();
        let read_dir = match std::fs::read_dir(&dir) {
            Ok(read_dir) => read_dir,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(StoreError::Io(err)),
        };

        let mut entries = Vec::new();
        for entry in read_dir {
            let entry = entry.map_err(StoreError::Io)?;
            let Some(metadata) = Self::failed_scan_entry_metadata(entry.metadata())? else {
                continue;
            };
            if !metadata.is_file() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            // A residual temp file from an interrupted failed-store write is not a complete entry.
            if name.ends_with(TMP_SUFFIX) {
                let _ = std::fs::remove_file(entry.path());
                continue;
            }
            let written_at = metadata.modified().unwrap_or(UNIX_EPOCH);
            entries.push((entry.path(), written_at));
        }

        entries.sort_by_key(|(_, written_at)| *written_at);
        Ok(entries)
    }

    /// Drops the oldest failed entry to admit a new one when the count bound is reached. The warn log
    /// naming the trimmed entry fires after its unlink succeeds, so the log names exactly the entries
    /// the capacity trim removed.
    fn evict_oldest_failed_if_full(&self, current: &[(PathBuf, SystemTime)]) -> Result<(), StoreError> {
        if current.len() < FAILED_STORE_MAX_ENTRIES {
            return Ok(());
        }

        let drop_count = current.len() - FAILED_STORE_MAX_ENTRIES + 1;
        for (path, _) in current.iter().take(drop_count) {
            let evicted_id = path
                .file_name()
                .map(|name| name.to_string_lossy().to_string())
                .unwrap_or_default();
            match std::fs::remove_file(path) {
                Ok(()) => {
                    self.decrement_failed_count();
                    warn!(
                        event = EVENT_TARGET_STORE_STATE,
                        component = LOG_COMPONENT_TARGETS,
                        subsystem = LOG_SUBSYSTEM_STORE,
                        action = "failed_store_evict",
                        evicted_entry = %evicted_id,
                        reason = "capacity",
                        "target store state"
                    );
                }
                // Already removed by another handle, so nothing was trimmed here and no log fires.
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(StoreError::Io(err)),
            }
        }
        Ok(())
    }
}

impl<T> Store<T> for QueueStore<T>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    type Error = StoreError;
    type Key = Key;

    fn open(&self) -> Result<(), Self::Error> {
        let _fs_guard = self
            .fs_guard
            .write()
            .map_err(|_| StoreError::Internal("Failed to acquire write lock on store filesystem".to_string()))?;
        std::fs::create_dir_all(&self.directory).map_err(StoreError::Io)?;

        let dir_entries = std::fs::read_dir(&self.directory).map_err(StoreError::Io)?;
        let mut entries_map = self
            .entries
            .write()
            .map_err(|_| StoreError::Internal("Failed to acquire write lock on entries".to_string()))?;
        self.pending_entries.store(0, Ordering::SeqCst);
        entries_map.clear();
        for entry in dir_entries {
            let entry = entry.map_err(StoreError::Io)?;
            let metadata = entry.metadata().map_err(StoreError::Io)?;
            if !metadata.is_file() {
                continue;
            }

            let file_name = entry.file_name().to_string_lossy().to_string();

            // Remove leftover temp files from an interrupted atomic write; they
            // are never valid committed entries.
            if file_name.ends_with(TMP_SUFFIX) {
                let _ = std::fs::remove_file(entry.path());
                continue;
            }

            // Ignore foreign files that do not match the queue file extension so
            // externally-dropped files cannot pollute the queue.
            if !is_queue_file_name(&file_name, &self.file_ext) {
                continue;
            }

            // Drop zero-byte files (truncated/empty writes from older code paths
            // or crashes) instead of indexing a ghost entry that read_file would
            // report as NotFound forever.
            if metadata.len() == 0 {
                let _ = std::fs::remove_file(entry.path());
                continue;
            }

            let modified = metadata.modified().map_err(StoreError::Io)?;
            let unix_nano = modified.duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as i64;
            entries_map.insert(file_name, unix_nano);
        }

        // Seed the cached failed count from one read of the failed directory, under the write guard
        // already held, so failed_len() serves an atomic load rather than a directory scan.
        self.failed_count.store(self.count_failed_entries_on_disk(), Ordering::SeqCst);

        debug!(
            event = EVENT_TARGET_STORE_STATE,
            component = LOG_COMPONENT_TARGETS,
            subsystem = LOG_SUBSYSTEM_STORE,
            state = "opened",
            store_dir = ?self.directory,
            entry_count = entries_map.len(),
            "target store state"
        );
        Ok(())
    }

    fn put(&self, item: Arc<T>) -> Result<Self::Key, Self::Error> {
        let _fs_guard = self
            .fs_guard
            .read()
            .map_err(|_| StoreError::Internal("Failed to acquire read lock on store filesystem".to_string()))?;
        let _reservation = self.reserve_entry_slot()?;
        let key = self.build_key(1);
        let data = serde_json::to_vec(&*item).map_err(|e| StoreError::Serialization(e.to_string()))?;
        self.write_and_index(&key, &data)?;

        Ok(key)
    }

    fn put_multiple(&self, items: Vec<T>) -> Result<Self::Key, Self::Error> {
        if items.is_empty() {
            return Err(StoreError::Internal("Cannot put_multiple with empty items list".to_string()));
        }
        let _fs_guard = self
            .fs_guard
            .read()
            .map_err(|_| StoreError::Internal("Failed to acquire read lock on store filesystem".to_string()))?;
        let _reservation = self.reserve_entry_slot()?;
        let key = self.build_key(items.len());

        let mut buffer = Vec::new();
        for item in items {
            serde_json::to_writer(&mut buffer, &item).map_err(|e| StoreError::Serialization(e.to_string()))?;
        }

        self.write_and_index(&key, &buffer)?;

        Ok(key)
    }

    fn put_raw(&self, data: &[u8]) -> Result<Self::Key, Self::Error> {
        let _fs_guard = self
            .fs_guard
            .read()
            .map_err(|_| StoreError::Internal("Failed to acquire read lock on store filesystem".to_string()))?;
        let _reservation = self.reserve_entry_slot()?;
        let key = self.build_key(1);
        self.write_and_index(&key, data)?;

        Ok(key)
    }

    fn get(&self, key: &Self::Key) -> Result<T, Self::Error> {
        if key.item_count != 1 {
            return Err(StoreError::Internal(format!(
                "get() called on a batch key ({} items), use get_multiple()",
                key.item_count
            )));
        }
        let items = self.get_multiple(key)?;
        items.into_iter().next().ok_or(StoreError::NotFound)
    }

    fn get_multiple(&self, key: &Self::Key) -> Result<Vec<T>, Self::Error> {
        debug!(
            event = EVENT_TARGET_STORE_STATE,
            component = LOG_COMPONENT_TARGETS,
            subsystem = LOG_SUBSYSTEM_STORE,
            action = "read_batch",
            key = %key,
            "target store state"
        );
        let data = self.get_raw(key)?;
        if data.is_empty() {
            return Err(StoreError::Deserialization("Cannot deserialize empty data".to_string()));
        }
        // `item_count` is parsed from the (untrusted) filename; clamp the
        // up-front allocation so a corrupt/malicious name cannot drive a huge
        // reservation. The loop below still reads exactly `item_count` items.
        let mut items = Vec::with_capacity(key.item_count.min(MAX_PREALLOC_CAPACITY));

        // let mut deserializer = serde_json::Deserializer::from_slice(&data);
        // while let Ok(item) = serde::Deserialize::deserialize(&mut deserializer) {
        //     items.push(item);
        // }

        // This deserialization logic assumes multiple JSON objects are simply concatenated in the file.
        // This is fragile. It's better to store a JSON array `[item1, item2, ...]`
        // or use a streaming deserializer that can handle multiple top-level objects if that's the format.
        // For now, assuming serde_json::Deserializer::from_slice can handle this if input is well-formed.
        let mut deserializer = serde_json::Deserializer::from_slice(&data).into_iter::<T>();

        for _ in 0..key.item_count {
            match deserializer.next() {
                Some(Ok(item)) => items.push(item),
                Some(Err(e)) => {
                    return Err(StoreError::Deserialization(format!("Failed to deserialize item in batch: {e}")));
                }
                None => {
                    // Reached end of stream before deserializing item_count items: the
                    // batch file was truncated or corrupted. This MUST be surfaced as an
                    // error rather than silently returning the partial set. Returning
                    // Ok(partial) would let the caller treat the batch as fully delivered,
                    // delete the store entry, and permanently lose the missing events.
                    warn!(
                        event = EVENT_TARGET_STORE_STATE,
                        component = LOG_COMPONENT_TARGETS,
                        subsystem = LOG_SUBSYSTEM_STORE,
                        action = "read_batch",
                        key = %key,
                        expected_items = key.item_count,
                        actual_items = items.len(),
                        reason = "truncated_batch_read",
                        "target store state"
                    );
                    return Err(StoreError::Deserialization(format!(
                        "Truncated batch for key {key}: expected {} items but only deserialized {}",
                        key.item_count,
                        items.len()
                    )));
                }
            }
        }

        if items.is_empty() && key.item_count > 0 {
            return Err(StoreError::Deserialization("No items found".to_string()));
        }

        Ok(items)
    }

    fn get_raw(&self, key: &Self::Key) -> Result<Vec<u8>, Self::Error> {
        self.read_file(key)
    }

    fn del(&self, key: &Self::Key) -> Result<(), Self::Error> {
        let _fs_guard = self
            .fs_guard
            .read()
            .map_err(|_| StoreError::Internal("Failed to acquire read lock on store filesystem".to_string()))?;
        let path = self.file_path(key);
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // File already gone — still clean up the entries map to avoid stale keys.
                warn!(
                    event = EVENT_TARGET_STORE_STATE,
                    component = LOG_COMPONENT_TARGETS,
                    subsystem = LOG_SUBSYSTEM_STORE,
                    action = "delete",
                    key = %key,
                    result = "file_missing",
                    "target store state"
                );
            }
            Err(e) => return Err(StoreError::Io(e)),
        }

        // Get the write lock to update the internal state
        let mut entries = self
            .entries
            .write()
            .map_err(|_| StoreError::Internal("Failed to acquire write lock on entries".to_string()))?;

        // A batch entry indexed under the legacy separator is removed under both renderings so its slot is reclaimed.
        let removed_current = entries.remove(&key.to_key_string()).is_some();
        let removed_legacy = key.item_count > 1
            && entries
                .remove(&key.to_key_string_with(LEGACY_BATCH_COUNT_SEPARATOR))
                .is_some();
        if !removed_current && !removed_legacy {
            debug!(
                event = EVENT_TARGET_STORE_STATE,
                component = LOG_COMPONENT_TARGETS,
                subsystem = LOG_SUBSYSTEM_STORE,
                action = "delete",
                key = %key,
                result = "entry_missing",
                "target store state"
            );
        }
        debug!(
            event = EVENT_TARGET_STORE_STATE,
            component = LOG_COMPONENT_TARGETS,
            subsystem = LOG_SUBSYSTEM_STORE,
            action = "delete",
            key = %key,
            result = "deleted",
            "target store state"
        );
        Ok(())
    }

    fn delete(&self) -> Result<(), Self::Error> {
        let _fs_guard = self
            .fs_guard
            .write()
            .map_err(|_| StoreError::Internal("Failed to acquire write lock on store filesystem".to_string()))?;
        let mut entries = self
            .entries
            .write()
            .map_err(|_| StoreError::Internal("Failed to acquire write lock on entries".to_string()))?;
        entries.clear();
        self.pending_entries.store(0, Ordering::SeqCst);
        self.failed_count.store(0, Ordering::SeqCst);

        match std::fs::remove_dir_all(&self.directory) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(StoreError::Io(err)),
        }
    }

    fn list(&self) -> Vec<Self::Key> {
        // Get the read lock to read the internal state
        let entries = match self.entries.read() {
            Ok(entries) => entries,
            Err(_) => {
                debug!(
                    event = EVENT_TARGET_STORE_STATE,
                    component = LOG_COMPONENT_TARGETS,
                    subsystem = LOG_SUBSYSTEM_STORE,
                    action = "list",
                    result = "lock_unavailable",
                    "target store state"
                );
                return Vec::new();
            }
        };

        // Order by the entry name (UUIDv7), which is time-ordered by
        // construction. This yields a stable FIFO order that is identical in a
        // single run and after a restart, because it derives purely from the
        // persisted filename rather than from coarse, clock-dependent file
        // mtimes. The full filename is used as a deterministic tie-breaker.
        let mut entries_vec: Vec<(String, String)> = entries
            .keys()
            .map(|file_name| (parse_key(file_name).name, file_name.clone()))
            .collect();
        entries_vec.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

        entries_vec.into_iter().map(|(_, file_name)| parse_key(&file_name)).collect()
    }

    fn len(&self) -> usize {
        // Get the read lock to read the internal state
        match self.entries.read() {
            Ok(entries) => entries.len(),
            Err(_) => {
                debug!(
                    event = EVENT_TARGET_STORE_STATE,
                    component = LOG_COMPONENT_TARGETS,
                    subsystem = LOG_SUBSYSTEM_STORE,
                    action = "len",
                    result = "lock_unavailable",
                    "target store state"
                );
                0
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn boxed_clone(&self) -> Box<dyn Store<T, Error = Self::Error, Key = Self::Key> + Send + Sync> {
        Box::new(self.clone()) as Box<dyn Store<T, Error = Self::Error, Key = Self::Key> + Send + Sync>
    }
}

impl<T> FailedEventStore for QueueStore<T>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    fn put_failed_raw(&self, entry_name: &str, data: &[u8]) -> Result<String, StoreError> {
        // The name becomes a filename inside the failed directory, so a separator or a dot-only name
        // is rejected before it can address a path outside it.
        if entry_name.is_empty() || entry_name.contains(['/', '\\']) || entry_name == "." || entry_name == ".." {
            return Err(StoreError::Internal(format!("invalid failed-store entry name: {entry_name}")));
        }

        // The seam holds across the at-bound check, the capacity trim, and the count update, so a
        // second writer or the scan cannot interleave. It nests outside fs_guard, the order the scan
        // uses too, so the two never invert.
        let _failed_guard = self
            .failed_store_guard
            .lock()
            .map_err(|_| StoreError::Internal("Failed to acquire the failed-store guard".to_string()))?;
        let _fs_guard = self
            .fs_guard
            .read()
            .map_err(|_| StoreError::Internal("Failed to acquire read lock on store filesystem".to_string()))?;

        let failed_dir = self.failed_dir();
        if !failed_dir.exists() {
            std::fs::create_dir_all(&failed_dir).map_err(StoreError::Io)?;
            // Best-effort: persist the lazily created directory entry in the queue directory, the
            // same durability discipline the live write path applies after its rename.
            Self::fsync_dir(&self.directory);
        }

        let entry_id = entry_name.to_string();
        let final_path = failed_dir.join(&entry_id);
        let is_new_entry = !final_path.exists();

        // A repeated move of the same live entry lands on the same filename and replaces the earlier
        // file. The count bound is enforced only when the write creates a new entry, since an
        // overwrite does not grow the count and a capacity trim for it would drop a genuine entry. The
        // cached count gates the ordered capacity scan with a plain atomic load, so a write below the
        // bound skips the stat-and-sort entirely. The ordered scan runs only at the bound, oldest
        // first, so the new failure is recorded even at capacity.
        if is_new_entry && self.failed_len() >= FAILED_STORE_MAX_ENTRIES {
            let existing = self.failed_entries_oldest_first()?;
            self.evict_oldest_failed_if_full(&existing)?;
        }

        let temp_path = failed_dir.join(format!("{entry_id}.{}{}", Uuid::new_v4(), TMP_SUFFIX));
        // rename replaces an existing destination file on the supported platforms, which the
        // idempotent overwrite relies on.
        write_temp_then_rename(&temp_path, &final_path, data)?;

        // A new failed file grows the cached count. An overwrite of an existing failed file leaves it
        // unchanged.
        if is_new_entry {
            self.failed_count.fetch_add(1, Ordering::SeqCst);
        }

        // Best-effort: persist the directory entry created by the rename, matching the live write
        // path.
        Self::fsync_dir(&failed_dir);

        debug!(
            event = EVENT_TARGET_STORE_STATE,
            component = LOG_COMPONENT_TARGETS,
            subsystem = LOG_SUBSYSTEM_STORE,
            action = "failed_store_write",
            failed_entry = %entry_id,
            "target store state"
        );
        Ok(entry_id)
    }

    fn prune_failed_store(&self) -> Result<usize, StoreError> {
        // Same seam and nesting order as put_failed_raw, so the reconcile store below rests on a
        // listing no concurrent writer can shift, and the count it writes cannot overwrite a write's
        // increment.
        let _failed_guard = self
            .failed_store_guard
            .lock()
            .map_err(|_| StoreError::Internal("Failed to acquire the failed-store guard".to_string()))?;
        let _fs_guard = self
            .fs_guard
            .read()
            .map_err(|_| StoreError::Internal("Failed to acquire read lock on store filesystem".to_string()))?;

        let entries = self.failed_entries_oldest_first()?;
        let materialized_len = entries.len();
        let now = SystemTime::now();
        let mut pruned = 0usize;
        for (path, written_at) in entries {
            let age = now.duration_since(written_at).unwrap_or_default();
            if age < FAILED_STORE_TTL {
                // Entries are oldest first, so the first within the retention bound ends the scan.
                break;
            }
            let pruned_id = path
                .file_name()
                .map(|name| name.to_string_lossy().to_string())
                .unwrap_or_default();
            warn!(
                event = EVENT_TARGET_STORE_STATE,
                component = LOG_COMPONENT_TARGETS,
                subsystem = LOG_SUBSYSTEM_STORE,
                action = "failed_store_prune",
                pruned_entry = %pruned_id,
                reason = "ttl",
                "target store state"
            );
            match std::fs::remove_file(&path) {
                Ok(()) => {
                    pruned += 1;
                    self.decrement_failed_count();
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(StoreError::Io(err)),
            }
        }
        // Reconcile the cached count to this scan's materialized listing under the seam, so a change
        // made outside the store drifts the value by at most one maintenance interval and no concurrent
        // write is overwritten.
        self.failed_count.store((materialized_len - pruned) as u64, Ordering::SeqCst);
        Ok(pruned)
    }

    fn failed_len(&self) -> usize {
        // A plain load of the cached count seeded at open and kept current by the write, the capacity
        // trim, and the expired-entry removal. The maintenance scan reconciles it to the directory
        // each interval, so external drift is corrected within one interval.
        self.failed_count.load(Ordering::SeqCst) as usize
    }

    fn boxed_clone_failed(&self) -> Box<dyn FailedEventStore> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{Arc, Barrier},
        thread,
    };

    fn temp_store_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("rustfs-targets-{name}-{}", Uuid::new_v4()))
    }

    #[test]
    fn resolve_queue_store_compression_defaults_to_true() {
        assert!(resolve_queue_store_compression_from_env_value(None));
    }

    #[test]
    fn resolve_queue_store_compression_respects_disabled_env_value() {
        assert!(!resolve_queue_store_compression_from_env_value(Some("off")));
        assert!(!resolve_queue_store_compression_from_env_value(Some("false")));
    }

    #[test]
    fn put_uses_store_compression_setting_in_key() {
        let dir = temp_store_dir("put-key");
        let store = QueueStore::<String>::new_with_compression(&dir, 8, ".test", false);
        store.open().unwrap();

        let key = store.put(Arc::new("payload".to_string())).unwrap();

        assert!(!key.compress);
        assert!(store.file_path(&key).exists());

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn parse_key_round_trips_batch_and_compression_suffixes() {
        let key = Key {
            name: "event-id".to_string(),
            extension: ".json".to_string(),
            item_count: 3,
            compress: true,
        };

        let parsed = parse_key(&key.to_key_string());

        assert_eq!(parsed.name, key.name);
        assert_eq!(parsed.extension, key.extension);
        assert_eq!(parsed.item_count, key.item_count);
        assert_eq!(parsed.compress, key.compress);
    }

    // A batch filename must stay outside the Windows reserved character set so File::create succeeds
    // on NTFS. The name carries a UUID, matching what build_key produces for a real entry.
    #[test]
    fn batch_key_filename_has_no_windows_reserved_characters() {
        let key = Key {
            name: Uuid::new_v4().to_string(),
            extension: ".json".to_string(),
            item_count: 7,
            compress: true,
        };

        let file_name = key.to_key_string();

        // The characters NTFS rejects in a filename, plus the path separators.
        const WINDOWS_RESERVED: &[char] = &['<', '>', ':', '"', '/', '\\', '|', '?', '*'];
        for reserved in WINDOWS_RESERVED {
            assert!(
                !file_name.contains(*reserved),
                "batch filename {file_name} must not contain the reserved character {reserved}"
            );
        }
        assert!(file_name.contains(BATCH_COUNT_SEPARATOR), "the batch count uses the safe separator");
    }

    // An entry written with the legacy separator still reads back, so a queue populated before the
    // switch to the Windows-safe separator is not misparsed after an upgrade.
    #[test]
    fn parse_key_reads_legacy_batch_separator() {
        let legacy = format!("5{LEGACY_BATCH_COUNT_SEPARATOR}{}.json", "event-id");

        let parsed = parse_key(&legacy);

        assert_eq!(parsed.item_count, 5);
        assert_eq!(parsed.name, "event-id");
        assert_eq!(parsed.extension, ".json");
    }

    // The render side prefixes the count only when it exceeds one, so the parse side strips a count
    // prefix only for the same range. A hostile 1_ or 0_ prefix stays part of the name and round-trips.
    #[test]
    fn parse_key_treats_a_count_of_one_or_zero_as_a_plain_name() {
        for hostile in ["1_name.event", "0_name.event"] {
            let parsed = parse_key(hostile);
            assert_eq!(parsed.item_count, 1, "{hostile} is a single-item entry");
            assert_eq!(parsed.to_key_string(), hostile, "{hostile} round-trips as a plain name");
        }
    }

    #[test]
    fn put_raw_and_get_raw_round_trip_bytes() {
        let dir = temp_store_dir("raw-roundtrip");
        let store = QueueStore::<String>::new_with_compression(&dir, 8, ".test", true);
        store.open().unwrap();

        let payload = br#"{"kind":"notify","bucket":"demo","key":"alpha.txt"}"#;
        let key = store.put_raw(payload).unwrap();
        let raw = store.get_raw(&key).unwrap();

        assert_eq!(raw, payload);

        let _ = store.delete();
    }

    #[test]
    fn delete_removes_directory_and_clears_entries() {
        let dir = temp_store_dir("delete-store");
        let store = QueueStore::<String>::new_with_compression(&dir, 8, ".test", false);
        store.open().unwrap();
        let _ = store.put(Arc::new("payload".to_string())).unwrap();

        store.delete().unwrap();

        assert!(store.list().is_empty());
        assert!(!dir.exists());
    }

    #[test]
    fn put_enforces_entry_limit() {
        let dir = temp_store_dir("limit");
        let store = QueueStore::<String>::new_with_compression(&dir, 1, ".test", false);
        store.open().unwrap();

        let _ = store.put(Arc::new("first".to_string())).unwrap();
        let err = store.put(Arc::new("second".to_string())).unwrap_err();

        assert!(matches!(err, StoreError::LimitExceeded));

        let _ = store.delete();
    }

    #[test]
    fn get_multiple_errors_on_truncated_batch_instead_of_partial_success() {
        let dir = temp_store_dir("truncated-batch");
        let store = QueueStore::<String>::new_with_compression(&dir, 8, ".test", false);
        store.open().unwrap();

        let items = vec!["aa".to_string(), "bb".to_string(), "cc".to_string()];
        let key = store.put_multiple(items).unwrap();
        assert_eq!(key.item_count, 3);

        // Truncate the batch file at a clean boundary after the first two serialized
        // items, so the read of the third item hits end-of-stream. This is the
        // partial-read condition the batch reader must surface as an error.
        let prefix_len =
            serde_json::to_vec(&"aa".to_string()).unwrap().len() + serde_json::to_vec(&"bb".to_string()).unwrap().len();
        let path = store.file_path(&key);
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(prefix_len as u64).unwrap();
        drop(file);

        // A truncated batch must be reported as an error, not silently returned as a
        // partial-but-successful result that would drop the missing "cc" event.
        let err = store.get_multiple(&key).unwrap_err();
        assert!(
            matches!(err, StoreError::Deserialization(_)),
            "expected Deserialization error, got {err:?}"
        );

        // Because get_multiple failed, the batch entry is still on disk for the caller
        // to retry — the missing events are not silently discarded.
        assert!(store.file_path(&key).exists());

        let _ = store.delete();
    }

    #[test]
    fn concurrent_put_raw_respects_entry_limit() {
        let dir = temp_store_dir("concurrent-limit");
        let store = Arc::new(QueueStore::<String>::new_with_compression(&dir, 1, ".test", true));
        store.open().unwrap();

        let start = Arc::new(Barrier::new(4));
        let mut handles = Vec::new();

        for idx in 0..4 {
            let store = Arc::clone(&store);
            let start = Arc::clone(&start);
            handles.push(thread::spawn(move || {
                let payload = vec![b'x'; 32 * 1024 + idx];
                start.wait();
                store.put_raw(&payload)
            }));
        }

        let mut successes = 0;
        let mut limit_errors = 0;
        for handle in handles {
            match handle.join().unwrap() {
                Ok(_) => successes += 1,
                Err(StoreError::LimitExceeded) => limit_errors += 1,
                Err(err) => panic!("unexpected error: {err}"),
            }
        }

        assert_eq!(successes, 1);
        assert_eq!(limit_errors, 3);
        assert_eq!(store.len(), 1);

        let _ = store.delete();
    }

    #[test]
    fn open_cleans_leftover_tmp_and_zero_byte_files() {
        let dir = temp_store_dir("open-cleanup");
        let store = QueueStore::<String>::new_with_compression(&dir, 8, ".test", false);
        store.open().unwrap();

        // A committed entry that must survive open().
        let key = store.put(Arc::new("payload".to_string())).unwrap();
        let good_name = key.to_key_string();

        // Simulate an interrupted atomic write: a temp file that was fsynced but
        // never renamed into place. It must be removed and never indexed.
        let tmp_path = dir.join(format!("{good_name}{TMP_SUFFIX}"));
        std::fs::write(&tmp_path, b"half-written").unwrap();

        // Simulate a zero-byte ghost file with a valid queue extension.
        let zero_name = format!("{}.test", Uuid::now_v7());
        let zero_path = dir.join(&zero_name);
        std::fs::write(&zero_path, b"").unwrap();

        store.open().unwrap();

        // The queue is clean: only the committed entry remains indexed, and both
        // the temp and zero-byte residues are gone from disk.
        assert_eq!(store.len(), 1);
        let listed: Vec<String> = store.list().iter().map(|k| k.to_key_string()).collect();
        assert_eq!(listed, vec![good_name]);
        assert!(!tmp_path.exists(), "leftover temp file should be removed");
        assert!(!zero_path.exists(), "zero-byte file should be removed");

        let _ = store.delete();
    }

    #[test]
    fn open_ignores_foreign_extension_files() {
        let dir = temp_store_dir("open-foreign");
        let store = QueueStore::<String>::new_with_compression(&dir, 8, ".test", false);
        store.open().unwrap();
        let key = store.put(Arc::new("payload".to_string())).unwrap();

        // A non-empty file with an unrelated extension must not be indexed and
        // must be left untouched. Only residue from this store's own writes is cleaned up.
        let foreign_path = dir.join("intruder.txt");
        std::fs::write(&foreign_path, b"not ours").unwrap();

        store.open().unwrap();

        assert_eq!(store.len(), 1);
        let listed: Vec<String> = store.list().iter().map(|k| k.to_key_string()).collect();
        assert_eq!(listed, vec![key.to_key_string()]);
        assert!(foreign_path.exists(), "foreign file should be left in place, just not indexed");

        let _ = store.delete();
    }

    #[test]
    fn list_order_is_stable_across_reopen() {
        let dir = temp_store_dir("list-order");
        let store = QueueStore::<String>::new_with_compression(&dir, 32, ".test", false);
        store.open().unwrap();

        for idx in 0..8 {
            store.put(Arc::new(format!("event-{idx}"))).unwrap();
        }

        let order_before: Vec<String> = store.list().iter().map(|k| k.to_key_string()).collect();
        assert_eq!(order_before.len(), 8);

        // Re-open from disk (simulating a restart) and assert the FIFO order is
        // reproduced exactly — it is derived from the persisted UUIDv7 names, not
        // from coarse file mtimes.
        let reopened = QueueStore::<String>::new_with_compression(&dir, 32, ".test", false);
        reopened.open().unwrap();
        let order_after: Vec<String> = reopened.list().iter().map(|k| k.to_key_string()).collect();

        assert_eq!(order_before, order_after);

        let _ = store.delete();
    }

    #[test]
    fn new_with_huge_limit_does_not_panic() {
        // An absurd configured entry_limit must not drive a giant up-front HashMap
        // allocation or a capacity-overflow panic.
        let dir = temp_store_dir("huge-limit");
        let store = QueueStore::<String>::new_with_compression(&dir, u64::MAX, ".test", false);
        store.open().unwrap();
        assert!(store.is_empty());
        let _ = store.delete();
    }

    #[test]
    fn get_multiple_does_not_overallocate_on_huge_item_count() {
        let dir = temp_store_dir("huge-item-count");
        let store = QueueStore::<String>::new_with_compression(&dir, 8, ".test", false);
        store.open().unwrap();
        std::fs::create_dir_all(&dir).unwrap();

        // Craft a batch file whose name claims a colossal item_count (far beyond
        // MAX_PREALLOC_CAPACITY) but whose body holds only two items — the shape a
        // corrupt/malicious filename would take. The read must fail cleanly with a
        // Deserialization error and must not attempt a giant pre-allocation
        // (Vec::with_capacity is clamped to MAX_PREALLOC_CAPACITY).
        let claimed_count = 1_000_000usize;
        let file_name = format!("{claimed_count}:{}.test", Uuid::now_v7());
        let mut body = Vec::new();
        body.extend_from_slice(&serde_json::to_vec(&"aa".to_string()).unwrap());
        body.extend_from_slice(&serde_json::to_vec(&"bb".to_string()).unwrap());
        std::fs::write(dir.join(&file_name), &body).unwrap();

        let key = parse_key(&file_name);
        assert_eq!(key.item_count, claimed_count);

        let err = store.get_multiple(&key).unwrap_err();
        assert!(
            matches!(err, StoreError::Deserialization(_)),
            "expected Deserialization error, got {err:?}"
        );

        let _ = store.delete();
    }

    fn count_temp_files(dir: &Path) -> usize {
        match std::fs::read_dir(dir) {
            Ok(read_dir) => read_dir
                .filter_map(|entry| entry.ok())
                .filter(|entry| entry.file_name().to_string_lossy().ends_with(TMP_SUFFIX))
                .count(),
            Err(_) => 0,
        }
    }

    /// Directory truth for the failed store: the number of complete failed files on disk, skipping
    /// residual temp files, so a test can assert the cached failed_len matches the filesystem.
    fn count_failed_files_on_disk(dir: &Path) -> usize {
        match std::fs::read_dir(dir.join(FAILED_STORE_SUBDIR)) {
            Ok(read_dir) => read_dir
                .filter_map(|entry| entry.ok())
                .filter(|entry| {
                    let name = entry.file_name().to_string_lossy().into_owned();
                    !name.ends_with(TMP_SUFFIX) && entry.file_type().map(|file_type| file_type.is_file()).unwrap_or(false)
                })
                .count(),
            Err(_) => 0,
        }
    }

    // A completed write leaves the full payload at the final path, readable, and no temp file. The
    // atomic path renames the temp into place rather than writing the final path directly.
    #[test]
    fn write_leaves_complete_file_and_no_temp_residue() {
        let dir = temp_store_dir("atomic-complete");
        let store = QueueStore::<String>::new_with_compression(&dir, 8, ".test", false);
        store.open().unwrap();

        let payload = br#"{"kind":"notify","bucket":"demo","key":"complete.txt"}"#;
        let key = store.put_raw(payload).unwrap();

        let final_path = store.file_path(&key);
        assert!(final_path.exists(), "final entry must exist after a complete write");
        assert_eq!(std::fs::read(&final_path).unwrap(), payload);
        assert_eq!(store.get_raw(&key).unwrap(), payload);
        assert_eq!(count_temp_files(&dir), 0, "a complete write leaves no temp file");

        let _ = store.delete();
    }

    // A temp file from an interrupted write (one that never reached its rename) is not a valid
    // entry. The open-time rescan removes it and never indexes it, so a partial write is absent
    // rather than read as a corrupt entry.
    #[test]
    fn open_discards_residual_temp_file() {
        let dir = temp_store_dir("atomic-residue");
        let store = QueueStore::<String>::new_with_compression(&dir, 8, ".test", false);
        store.open().unwrap();

        let key = store.put_raw(br#"{"complete":true}"#).unwrap();

        // Simulate a write interrupted before its rename. A half-written temp file is left in the
        // store directory under the same naming scheme write_file uses.
        let residual_temp = dir.join(format!("{}.{}{}", "2_orphan.test", Uuid::new_v4(), TMP_SUFFIX));
        std::fs::write(&residual_temp, b"partial payload, never renamed").unwrap();
        assert!(residual_temp.exists());

        // Reopen to drive the rescan.
        store.open().unwrap();

        assert!(!residual_temp.exists(), "open must remove a residual temp file");
        assert_eq!(count_temp_files(&dir), 0);
        assert_eq!(store.len(), 1, "only the complete entry is indexed");
        assert_eq!(store.get_raw(&key).unwrap(), br#"{"complete":true}"#);

        let _ = store.delete();
    }

    // Flag-independent. The queue round-trip (write, read, delete) is unchanged by the atomic write,
    // for both the uncompressed and the snappy-compressed path. The atomic write changes how bytes
    // land, not what round-trips.
    #[test]
    fn queue_round_trip_unchanged_uncompressed_and_compressed() {
        for compress in [false, true] {
            let dir = temp_store_dir("round-trip");
            let store = QueueStore::<String>::new_with_compression(&dir, 8, ".test", compress);
            store.open().unwrap();

            let payload = br#"{"kind":"notify","bucket":"demo","key":"round-trip.txt"}"#;
            let key = store.put_raw(payload).unwrap();
            assert_eq!(key.compress, compress);

            assert_eq!(store.get_raw(&key).unwrap(), payload, "read returns the written bytes");
            assert_eq!(store.len(), 1);

            store.del(&key).unwrap();
            assert!(matches!(store.get_raw(&key), Err(StoreError::NotFound)));
            assert_eq!(store.len(), 0);

            let _ = store.delete();
        }
    }

    // The temp file lives in the same directory as the final path, so the rename stays within one
    // filesystem and is atomic. A cross-directory temp would break that guarantee.
    #[test]
    fn temp_file_shares_directory_with_final_path() {
        let dir = temp_store_dir("same-dir");
        let final_path = dir.join("entry.test");
        let temp_path = dir.join(format!("entry.test.{}{}", Uuid::new_v4(), TMP_SUFFIX));

        assert_eq!(temp_path.parent(), final_path.parent());
    }

    // The failed-events store is a child directory inside the live queue directory and is created only on the
    // first failed write, so a target that never fails terminally leaves no failed directory.
    #[test]
    fn failed_store_directory_is_lazy_and_sibling_of_the_queue() {
        let dir = temp_store_dir("failed-lazy");
        let store = QueueStore::<String>::new_with_compression(&dir, 8, ".test", false);
        store.open().unwrap();

        let failed_dir = dir.join("failed");
        assert!(!failed_dir.exists(), "no failed directory before any failed write");
        assert_eq!(store.failed_len(), 0);

        let first_id = store.put_failed_raw("failed-entry-1", b"failed-entry-1").unwrap();
        assert!(failed_dir.exists(), "the failed directory is created on the first failed write");
        assert_eq!(store.failed_len(), 1);
        assert_eq!(
            store.failed_len(),
            count_failed_files_on_disk(&dir),
            "the cached count matches the directory truth"
        );
        assert_eq!(
            std::fs::read(failed_dir.join(&first_id)).unwrap(),
            b"failed-entry-1",
            "the lazy-create write lands the full payload"
        );

        // A later write finds the directory already present and takes the non-create branch.
        let second_id = store.put_failed_raw("failed-entry-2", b"failed-entry-2").unwrap();
        assert_eq!(store.failed_len(), 2);
        assert_eq!(
            store.failed_len(),
            count_failed_files_on_disk(&dir),
            "the cached count matches the directory truth"
        );
        assert_eq!(
            std::fs::read(failed_dir.join(&second_id)).unwrap(),
            b"failed-entry-2",
            "a write into the existing directory lands the full payload"
        );

        let _ = store.delete();
    }

    // A repeated move of the same live entry lands on the same failed filename, so a persistent
    // delete failure, or a crash that left the entry in both stores followed by another terminal
    // failure, replaces the earlier failed file instead of accumulating duplicates.
    #[test]
    fn failed_store_write_is_idempotent_per_entry() {
        let dir = temp_store_dir("failed-idempotent");
        let store = QueueStore::<String>::new_with_compression(&dir, 8, ".test", false);
        store.open().unwrap();

        let first = store.put_failed_raw("entry-a", b"first-write").unwrap();
        let second = store.put_failed_raw("entry-a", b"second-write").unwrap();
        assert_eq!(first, second, "the same entry keeps the same failed filename");
        assert_eq!(store.failed_len(), 1, "a repeated move yields exactly one failed file");
        assert_eq!(
            store.failed_len(),
            count_failed_files_on_disk(&dir),
            "an overwrite leaves the cached count matching the directory truth"
        );
        assert_eq!(
            std::fs::read(dir.join("failed").join(&second)).unwrap(),
            b"second-write",
            "the re-move replaces the earlier file"
        );

        let _ = store.delete();
    }

    // The failed filename is used inside the failed directory, so a name that could address a path
    // outside it is rejected before any write.
    #[test]
    fn failed_store_write_rejects_a_path_escaping_entry_name() {
        let dir = temp_store_dir("failed-name-guard");
        let store = QueueStore::<String>::new_with_compression(&dir, 8, ".test", false);
        store.open().unwrap();

        for name in ["", ".", "..", "a/b", "a\\b"] {
            assert!(store.put_failed_raw(name, b"payload").is_err(), "name {name:?} is rejected");
        }
        assert_eq!(store.failed_len(), 0, "no rejected name produced a file");

        let _ = store.delete();
    }

    // A per-entry NotFound stat inside the ordered failed scan means the file was removed between
    // the directory listing and the stat, so that entry is skipped and the scan completes. Any other
    // stat error still fails the scan.
    #[test]
    fn failed_scan_skips_an_entry_removed_mid_scan() {
        let dir = temp_store_dir("failed-scan-skip");
        std::fs::create_dir_all(&dir).unwrap();
        let probe = dir.join("probe");
        std::fs::write(&probe, b"probe").unwrap();
        let metadata = std::fs::metadata(&probe).unwrap();

        let present = QueueStore::<String>::failed_scan_entry_metadata(Ok(metadata)).unwrap();
        assert!(present.is_some(), "a present entry passes its metadata through");

        let removed =
            QueueStore::<String>::failed_scan_entry_metadata(Err(std::io::Error::from(std::io::ErrorKind::NotFound))).unwrap();
        assert!(removed.is_none(), "a concurrently removed entry is skipped, not an error");

        let denied =
            QueueStore::<String>::failed_scan_entry_metadata(Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied)));
        assert!(denied.is_err(), "a non-NotFound stat error still fails the scan");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // The failed store is bounded independently of the live queue. A live queue at its limit does not
    // consume failed-store capacity and the reverse holds.
    #[test]
    fn failed_store_is_separate_from_the_live_queue_limit() {
        let dir = temp_store_dir("failed-separate");
        let store = QueueStore::<String>::new_with_compression(&dir, 2, ".test", false);
        store.open().unwrap();

        store.put_raw(b"live-1").unwrap();
        store.put_raw(b"live-2").unwrap();
        assert!(matches!(store.put_raw(b"live-3"), Err(StoreError::LimitExceeded)));

        // The live queue is full, yet failed writes still succeed and grow the separate failed store.
        store.put_failed_raw("failed-1", b"failed-1").unwrap();
        store.put_failed_raw("failed-2", b"failed-2").unwrap();
        store.put_failed_raw("failed-3", b"failed-3").unwrap();
        assert_eq!(store.len(), 2, "the live queue stays at its own limit");
        assert_eq!(store.failed_len(), 3, "the failed store grows past the live limit");

        let _ = store.delete();
    }

    // At the count bound the oldest failed entry is dropped to admit the newer one, so a newer
    // terminal failure is never lost in preference to an older one. The directory is filled to the
    // bound with plain files at distinct write times, then one real failed write triggers the
    // capacity trim of the unambiguous oldest.
    #[test]
    fn failed_store_drops_oldest_at_the_count_bound() {
        let dir = temp_store_dir("failed-evict");
        let store = QueueStore::<String>::new_with_compression(&dir, 8, ".test", false);
        store.open().unwrap();

        let failed_dir = dir.join("failed");
        std::fs::create_dir_all(&failed_dir).unwrap();

        let oldest_name = "0000-oldest";
        for index in 0..FAILED_STORE_MAX_ENTRIES {
            let name = if index == 0 {
                oldest_name.to_string()
            } else {
                format!("{index:06}-{}", Uuid::new_v4())
            };
            let path = failed_dir.join(&name);
            std::fs::write(&path, b"prefilled").unwrap();
            let written_at = UNIX_EPOCH + Duration::from_secs(index as u64);
            std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .unwrap()
                .set_modified(written_at)
                .unwrap();
        }
        // The prefill lands entries directly on disk, so re-open to seed the cached count from the
        // failed directory, the same path a restart with existing failed entries takes.
        store.open().unwrap();
        assert_eq!(store.failed_len(), FAILED_STORE_MAX_ENTRIES);
        assert_eq!(
            store.failed_len(),
            count_failed_files_on_disk(&dir),
            "the seeded count matches the directory truth"
        );

        store.put_failed_raw("failed-newest", b"failed-newest").unwrap();
        assert_eq!(store.failed_len(), FAILED_STORE_MAX_ENTRIES, "the bound holds after the capacity trim");
        assert_eq!(
            store.failed_len(),
            count_failed_files_on_disk(&dir),
            "the cached count matches the directory truth after the trim"
        );
        assert!(
            !failed_dir.join(oldest_name).exists(),
            "the oldest failed entry is trimmed to admit the newer one"
        );

        let _ = store.delete();
    }

    // Entries older than the retention bound are removed as expired on the maintenance tick, while
    // younger entries survive. The age is measured from the file write time.
    #[test]
    fn failed_store_prunes_entries_past_the_ttl() {
        let dir = temp_store_dir("failed-ttl");
        let store = QueueStore::<String>::new_with_compression(&dir, 8, ".test", false);
        store.open().unwrap();

        let failed_dir = dir.join("failed");

        let stale_id = store.put_failed_raw("stale", b"stale").unwrap();
        let fresh_id = store.put_failed_raw("fresh", b"fresh").unwrap();

        // Age the stale entry past the bound, keep the fresh entry recent.
        let stale_time = SystemTime::now() - (FAILED_STORE_TTL + Duration::from_secs(60));
        std::fs::OpenOptions::new()
            .write(true)
            .open(failed_dir.join(&stale_id))
            .unwrap()
            .set_modified(stale_time)
            .unwrap();

        let pruned = store.prune_failed_store().unwrap();
        assert_eq!(pruned, 1, "one entry is past the retention bound");
        assert!(!failed_dir.join(&stale_id).exists(), "the stale entry is removed as expired");
        assert!(failed_dir.join(&fresh_id).exists(), "the fresh entry survives");
        assert_eq!(store.failed_len(), 1, "the cached count drops with the expired removal");
        assert_eq!(
            store.failed_len(),
            count_failed_files_on_disk(&dir),
            "the cached count matches the directory truth after expiry"
        );

        let _ = store.delete();
    }

    // Expired-entry removal on a store with no failed directory is a no-op, so a target that never
    // failed terminally is not charged a maintenance error on the tick.
    #[test]
    fn failed_store_prune_is_a_noop_without_a_failed_directory() {
        let dir = temp_store_dir("failed-prune-noop");
        let store = QueueStore::<String>::new_with_compression(&dir, 8, ".test", false);
        store.open().unwrap();

        assert_eq!(store.prune_failed_store().unwrap(), 0);
        assert!(!dir.join("failed").exists());

        let _ = store.delete();
    }

    // failed_len counts complete failed entries and skips a residual temp file from an interrupted
    // write. The count does not remove the temp file. Removal stays on the ordered scan run by the
    // expired-entry removal.
    #[test]
    fn failed_len_counts_real_entries_and_ignores_temp_files() {
        let dir = temp_store_dir("failed-len");
        let store = QueueStore::<String>::new_with_compression(&dir, 8, ".test", false);
        store.open().unwrap();

        store.put_failed_raw("complete-one", b"complete-one").unwrap();
        store.put_failed_raw("complete-two", b"complete-two").unwrap();
        let failed_dir = dir.join("failed");
        let residual_temp = failed_dir.join(format!("orphan.{}{}", Uuid::new_v4(), TMP_SUFFIX));
        std::fs::write(&residual_temp, b"partial, never renamed").unwrap();

        assert_eq!(store.failed_len(), 2, "only complete entries are counted");
        assert!(residual_temp.exists(), "the count does not remove the residual temp file");

        // The ordered scan run by the expired-entry removal drops the residual temp file.
        store.prune_failed_store().unwrap();
        assert!(!residual_temp.exists(), "the ordered scan removes the residual temp file");

        let _ = store.delete();
    }

    // The maintenance scan reconciles the cached count to the directory truth, so a hand-planted
    // drift is corrected on the next expired-entry removal, bounding external-mutation drift to one
    // interval.
    #[test]
    fn maintenance_reconciles_a_hand_planted_count_drift() {
        let dir = temp_store_dir("failed-reconcile");
        let store = QueueStore::<String>::new_with_compression(&dir, 8, ".test", false);
        store.open().unwrap();

        store.put_failed_raw("entry-a", b"entry-a").unwrap();
        store.put_failed_raw("entry-b", b"entry-b").unwrap();
        assert_eq!(store.failed_len(), 2);

        // Plant a drift so the cached count disagrees with the directory truth.
        store.failed_count.store(99, Ordering::SeqCst);
        assert_eq!(store.failed_len(), 99, "the planted drift is observed before the reconcile");

        // Both entries are within the retention bound, so the scan removes nothing and reconciles the
        // count to the materialized listing.
        let pruned = store.prune_failed_store().unwrap();
        assert_eq!(pruned, 0, "no entry is past the retention bound");
        assert_eq!(store.failed_len(), 2, "the reconcile restores the directory truth");
        assert_eq!(store.failed_len(), count_failed_files_on_disk(&dir));

        let _ = store.delete();
    }

    // The maintenance scan holds the seam across its listing and reconcile, so a racing writer waits
    // and its increment lands after the reconcile rather than being overwritten by it. The scan
    // thread is parked inside the seam by the filesystem write guard, the writer is released against
    // the held seam, and the final count must include the write. A scan that does not take the seam
    // fails the held-seam wait below.
    #[test]
    fn maintenance_reconcile_does_not_overwrite_a_concurrent_failed_write() {
        let dir = temp_store_dir("failed-reconcile-race");
        let store = Arc::new(QueueStore::<String>::new_with_compression(&dir, 8, ".test", false));
        store.open().unwrap();

        store.put_failed_raw("existing", b"existing").unwrap();
        // Plant a drift so the final value proves the reconcile ran and the write survived it. A
        // reconcile that overwrites the write ends at 1, a write with no reconcile ends at 100, the
        // serialized pair ends at 2.
        store.failed_count.store(99, Ordering::SeqCst);

        // Hold the filesystem guard so the scan thread stops inside the seam, before its listing.
        let fs_block = store.fs_guard.write().unwrap();

        let scan_store = Arc::clone(&store);
        let scan = thread::spawn(move || scan_store.prune_failed_store());

        // Wait for the scan to take the seam. The deadline turns a scan that never takes the seam
        // into a failure rather than a hang.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while store.failed_store_guard.try_lock().is_ok() {
            assert!(
                std::time::Instant::now() < deadline,
                "the maintenance scan never took the failed-store seam"
            );
            thread::yield_now();
        }

        // The writer is released while the scan holds the seam, so its write and increment wait for
        // the reconcile.
        let release = Arc::new(Barrier::new(2));
        let writer_store = Arc::clone(&store);
        let writer_release = Arc::clone(&release);
        let writer = thread::spawn(move || {
            writer_release.wait();
            writer_store.put_failed_raw("racing", b"racing").unwrap();
        });
        release.wait();

        drop(fs_block);
        assert_eq!(scan.join().unwrap().unwrap(), 0, "both entries are within the retention bound");
        writer.join().unwrap();

        assert_eq!(store.failed_len(), 2, "the reconcile does not overwrite the racing write");
        assert_eq!(
            store.failed_len(),
            count_failed_files_on_disk(&dir),
            "the cached count matches the directory truth after the race"
        );

        let _ = store.delete();
    }

    // Two writers racing new failed entries at the count bound stay atomic through the seam, so the
    // bound holds and the cached count matches the directory truth. Without the seam the at-bound
    // check and the write interleave and the count overshoots the bound.
    #[test]
    fn concurrent_failed_writes_at_the_bound_hold_the_count() {
        let dir = temp_store_dir("failed-race-bound");
        let store = Arc::new(QueueStore::<String>::new_with_compression(&dir, 8, ".test", false));
        store.open().unwrap();

        let failed_dir = dir.join("failed");
        std::fs::create_dir_all(&failed_dir).unwrap();
        for index in 0..FAILED_STORE_MAX_ENTRIES {
            let path = failed_dir.join(format!("{index:06}-{}", Uuid::new_v4()));
            std::fs::write(&path, b"prefilled").unwrap();
            let written_at = UNIX_EPOCH + Duration::from_secs(index as u64);
            std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .unwrap()
                .set_modified(written_at)
                .unwrap();
        }
        // Seed the cached count from the prefilled directory, the path a restart with existing entries takes.
        store.open().unwrap();
        assert_eq!(store.failed_len(), FAILED_STORE_MAX_ENTRIES);

        let start = Arc::new(Barrier::new(2));
        let mut handles = Vec::new();
        for writer in 0..2 {
            let store = Arc::clone(&store);
            let start = Arc::clone(&start);
            handles.push(thread::spawn(move || {
                start.wait();
                store.put_failed_raw(&format!("racing-newest-{writer}"), b"newest").unwrap();
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(store.failed_len(), FAILED_STORE_MAX_ENTRIES, "the bound holds after two racing writes");
        assert_eq!(
            store.failed_len(),
            count_failed_files_on_disk(&dir),
            "the cached count matches the directory truth after the race"
        );

        let _ = store.delete();
    }

    // Two writers racing the same failed entry name produce one failed file and one count increment,
    // so a concurrent same-key move cannot double-count.
    #[test]
    fn concurrent_same_key_failed_writes_count_once() {
        let dir = temp_store_dir("failed-race-same-key");
        let store = Arc::new(QueueStore::<String>::new_with_compression(&dir, 8, ".test", false));
        store.open().unwrap();

        let start = Arc::new(Barrier::new(2));
        let mut handles = Vec::new();
        for _ in 0..2 {
            let store = Arc::clone(&store);
            let start = Arc::clone(&start);
            handles.push(thread::spawn(move || {
                start.wait();
                store.put_failed_raw("same-entry", b"payload").unwrap();
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(store.failed_len(), 1, "two racing writes of one name count once");
        assert_eq!(
            store.failed_len(),
            count_failed_files_on_disk(&dir),
            "the cached count matches the directory truth"
        );

        let _ = store.delete();
    }
}
