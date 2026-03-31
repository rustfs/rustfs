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
use std::sync::{Arc, RwLock};
use std::{
    collections::HashMap,
    marker::PhantomData,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};
use tracing::{debug, warn};
use uuid::Uuid;

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
    debug!("Parsing key: {}", s);

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
        "Parsed key - name: {}, extension: {}, item_count: {}, compress: {}",
        name, extension, item_count, compress
    );

    Key {
        name,
        extension,
        item_count,
        compress,
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

    /// Retrieves a single item by key
    fn get(&self, key: &Self::Key) -> Result<T, Self::Error>;

    /// Retrieves multiple items by key
    fn get_multiple(&self, key: &Self::Key) -> Result<Vec<T>, Self::Error>;

    /// Deletes an item by key
    fn del(&self, key: &Self::Key) -> Result<(), Self::Error>;

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
            _phantom: PhantomData,
        }
    }
}

impl<T: Serialize + DeserializeOwned + Send + Sync> QueueStore<T> {
    /// Creates a new QueueStore
    pub fn new(directory: impl Into<PathBuf>, limit: u64, ext: &str) -> Self {
        let file_ext = if ext.is_empty() { DEFAULT_EXT } else { ext };
        let entry_limit = if limit == 0 { DEFAULT_LIMIT } else { limit };

        QueueStore {
            directory: directory.into(),
            entry_limit,
            file_ext: file_ext.to_string(),
            compress: queue_store_compression_enabled(),
            entries: Arc::new(RwLock::new(HashMap::with_capacity(entry_limit as usize))),
            _phantom: PhantomData,
        }
    }

    /// Returns the full path for a key
    fn file_path(&self, key: &Key) -> PathBuf {
        self.directory.join(key.to_key_string())
    }

    fn ensure_capacity(&self) -> Result<(), StoreError> {
        let entries = self
            .entries
            .read()
            .map_err(|_| StoreError::Internal("Failed to acquire read lock on entries".to_string()))?;

        if entries.len() as u64 >= self.entry_limit {
            return Err(StoreError::LimitExceeded);
        }

        Ok(())
    }

    fn build_key(&self, item_count: usize) -> Key {
        Key {
            name: Uuid::new_v4().to_string(),
            extension: self.file_ext.clone(),
            item_count,
            compress: self.compress,
        }
    }

    /// Reads a file for the given key
    fn read_file(&self, key: &Key) -> Result<Vec<u8>, StoreError> {
        let path = self.file_path(key);
        debug!("Reading file for key: {},path: {}", key, path.display());
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

        if key.compress {
            let mut decoder = Decoder::new();
            decoder
                .decompress_vec(&data)
                .map_err(|e| StoreError::Compression(e.to_string()))
        } else {
            Ok(data)
        }
    }

    /// Writes data to a file for the given key
    fn write_file(&self, key: &Key, data: &[u8]) -> Result<(), StoreError> {
        let path = self.file_path(key);
        // Create directory if it doesn't exist
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(StoreError::Io)?;
        }

        let data = if key.compress {
            let mut encoder = Encoder::new();
            encoder
                .compress_vec(data)
                .map_err(|e| StoreError::Compression(e.to_string()))?
        } else {
            data.to_vec()
        };

        std::fs::write(&path, &data).map_err(StoreError::Io)?;
        let modified = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as i64;
        let mut entries = self
            .entries
            .write()
            .map_err(|_| StoreError::Internal("Failed to acquire write lock on entries".to_string()))?;
        entries.insert(key.to_key_string(), modified);
        debug!("Wrote event to store: {}", key);
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
        std::fs::create_dir_all(&self.directory).map_err(StoreError::Io)?;

        let dir_entries = std::fs::read_dir(&self.directory).map_err(StoreError::Io)?;
        let mut entries_map = self
            .entries
            .write()
            .map_err(|_| StoreError::Internal("Failed to acquire write lock on entries".to_string()))?;
        entries_map.clear();
        for entry in dir_entries {
            let entry = entry.map_err(StoreError::Io)?;
            let metadata = entry.metadata().map_err(StoreError::Io)?;
            if metadata.is_file() {
                let modified = metadata.modified().map_err(StoreError::Io)?;
                let unix_nano = modified.duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as i64;

                let file_name = entry.file_name().to_string_lossy().to_string();
                entries_map.insert(file_name, unix_nano);
            }
        }

        debug!("Opened store at: {:?}", self.directory);
        Ok(())
    }

    fn put(&self, item: Arc<T>) -> Result<Self::Key, Self::Error> {
        self.ensure_capacity()?;
        let key = self.build_key(1);
        let data = serde_json::to_vec(&*item).map_err(|e| StoreError::Serialization(e.to_string()))?;
        self.write_file(&key, &data)?;

        Ok(key)
    }

    fn put_multiple(&self, items: Vec<T>) -> Result<Self::Key, Self::Error> {
        if items.is_empty() {
            return Err(StoreError::Internal("Cannot put_multiple with empty items list".to_string()));
        }
        self.ensure_capacity()?;
        let key = self.build_key(items.len());

        let mut buffer = Vec::new();
        for item in items {
            serde_json::to_writer(&mut buffer, &item).map_err(|e| StoreError::Serialization(e.to_string()))?;
        }

        self.write_file(&key, &buffer)?;

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
        debug!("Reading items from store for key: {}", key);
        let data = self.read_file(key)?;
        if data.is_empty() {
            return Err(StoreError::Deserialization("Cannot deserialize empty data".to_string()));
        }
        let mut items = Vec::with_capacity(key.item_count);

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
                    // Reached end of stream sooner than item_count
                    if items.len() < key.item_count && !items.is_empty() {
                        // Partial read
                        warn!(
                            "Expected {} items for key {}, but only found {}. Possible data corruption or incorrect item_count.",
                            key.item_count,
                            key,
                            items.len()
                        );
                        // Depending on strictness, this could be an error.
                    } else if items.is_empty() {
                        // No items at all, but file existed
                        return Err(StoreError::Deserialization(format!(
                            "No items deserialized for key {key} though file existed."
                        )));
                    }
                    break;
                }
            }
        }

        if items.is_empty() && key.item_count > 0 {
            return Err(StoreError::Deserialization("No items found".to_string()));
        }

        Ok(items)
    }

    fn del(&self, key: &Self::Key) -> Result<(), Self::Error> {
        let path = self.file_path(key);
        std::fs::remove_file(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                // If file not found, still try to remove from entries map in case of inconsistency
                warn!("File not found for key {} during del, but proceeding to remove from entries map.", key);
                StoreError::NotFound
            } else {
                StoreError::Io(e)
            }
        })?;

        // Get the write lock to update the internal state
        let mut entries = self
            .entries
            .write()
            .map_err(|_| StoreError::Internal("Failed to acquire write lock on entries".to_string()))?;

        if entries.remove(&key.to_key_string()).is_none() {
            // Key was not in the map, could be an inconsistency or already deleted.
            // This is not necessarily an error if the file deletion succeeded or was NotFound.
            debug!("Key {} not found in entries map during del, might have been already removed.", key);
        }
        debug!("Deleted event from store: {}", key.to_string());
        Ok(())
    }

    fn list(&self) -> Vec<Self::Key> {
        // Get the read lock to read the internal state
        let entries = match self.entries.read() {
            Ok(entries) => entries,
            Err(_) => {
                debug!("Failed to acquire read lock on entries for listing");
                return Vec::new();
            }
        };

        let mut entries_vec: Vec<_> = entries.iter().collect();
        // Sort by modtime (value in HashMap) to process oldest first
        entries_vec.sort_by(|a, b| a.1.cmp(b.1)); // Oldest first

        entries_vec.into_iter().map(|(k, _)| parse_key(k)).collect()
    }

    fn len(&self) -> usize {
        // Get the read lock to read the internal state
        match self.entries.read() {
            Ok(entries) => entries.len(),
            Err(_) => {
                debug!("Failed to acquire read lock on entries for len");
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
        let store = QueueStore::<String> {
            entry_limit: 8,
            directory: dir.clone(),
            file_ext: ".test".to_string(),
            compress: false,
            entries: Arc::new(RwLock::new(HashMap::new())),
            _phantom: PhantomData,
        };
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
}
