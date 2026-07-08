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
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
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
        let name_part = if self.item_count > 1 {
            format!("{}:{}", self.item_count, self.name)
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

    // Number of batch items parsed
    if let Some(colon_pos) = name.find(':')
        && let Ok(count) = name[..colon_pos].parse::<usize>()
    {
        item_count = count;
        name = name[colon_pos + 1..].to_string();
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

/// A store that uses the filesystem to persist events in a queue
pub struct QueueStore<T> {
    entry_limit: u64,
    directory: PathBuf,
    file_ext: String,
    compress: bool,
    entries: Arc<RwLock<HashMap<String, i64>>>, // key -> modtime as unix nano
    pending_entries: Arc<AtomicU64>,
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
            fs_guard: Arc::new(RwLock::new(())),
            _phantom: PhantomData,
        }
    }

    /// Returns the full path for a key
    fn file_path(&self, key: &Key) -> PathBuf {
        self.directory.join(key.to_key_string())
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
        // Create directory if it doesn't exist
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(StoreError::Io)?;
        }

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

        // Write + fsync the full payload into the temp file, then atomically
        // rename it into place. Any error path removes the temp file so a failed
        // write cannot leave residue that would otherwise be cleaned only on the
        // next open().
        let write_result = (|| -> Result<(), StoreError> {
            let mut file = File::create(&tmp_path).map_err(StoreError::Io)?;
            file.write_all(&payload).map_err(StoreError::Io)?;
            file.sync_all().map_err(StoreError::Io)?;
            Ok(())
        })();

        if let Err(err) = write_result {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(err);
        }

        if let Err(err) = std::fs::rename(&tmp_path, &path) {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(StoreError::Io(err));
        }

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

            // Ignore foreign files that do not match our queue file extension so
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
                    // Reached end of stream before deserializing `item_count` items: the
                    // batch file was truncated or corrupted. This MUST be surfaced as an
                    // error rather than silently returning the partial set. If we returned
                    // `Ok(partial)`, the caller would treat the batch as fully delivered,
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

        if entries.remove(&key.to_key_string()).is_none() {
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
        // items (`"aa""bb"`), so the read of the third item hits end-of-stream — the
        // exact "partial read" condition that previously returned Ok silently.
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
        // must be left untouched (we only clean up our own residue).
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
        // Previously `HashMap::with_capacity(entry_limit as usize)` would attempt
        // a giant allocation / capacity overflow for an absurd configured limit.
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
}
