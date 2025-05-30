use common::error::{Error, Result};
use serde::{de::DeserializeOwned, Serialize};
use snap::raw::{Decoder, Encoder};
use std::collections::HashMap;
use std::io::Read;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{fs, io};
use uuid::Uuid;

/// Keys in storage
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Key {
    /// Key name
    pub name: String,
    /// Whether to compress
    pub compress: bool,
    /// filename extension
    pub extension: String,
    /// Number of items
    pub item_count: usize,
}

impl Key {
    /// Create a new key
    pub fn new(name: impl Into<String>, extension: impl Into<String>, compress: bool) -> Self {
        Self {
            name: name.into(),
            compress,
            extension: extension.into(),
            item_count: 1,
        }
    }

    /// Convert to string form
    #[allow(clippy::inherent_to_string)]
    pub fn to_string(&self) -> String {
        let mut key_str = self.name.clone();
        if self.item_count > 1 {
            key_str = format!("{}:{}", self.item_count, self.name);
        }

        let compress_ext = if self.compress { ".snappy" } else { "" };
        format!("{}{}{}", key_str, self.extension, compress_ext)
    }
}

/// Parse key from file name
#[allow(clippy::redundant_closure)]
pub fn parse_key(filename: &str) -> Key {
    let compress = filename.ends_with(".snappy");
    let filename = if compress {
        &filename[..filename.len() - 7] // 移除 ".snappy"
    } else {
        filename
    };

    let mut parts = filename.splitn(2, '.');
    let name_part = parts.next().unwrap_or("");
    let extension = parts
        .next()
        .map_or_else(|| String::new(), |ext| format!(".{}", ext))
        .to_string();

    let mut name = name_part.to_string();
    let mut item_count = 1;

    if let Some(pos) = name_part.find(':') {
        if let Ok(count) = name_part[..pos].parse::<usize>() {
            item_count = count;
            name = name_part[pos + 1..].to_string();
        }
    }

    Key {
        name,
        compress,
        extension,
        item_count,
    }
}

/// Store the characteristics of the project
pub trait Store<T>: Send + Sync
where
    T: Serialize + DeserializeOwned + Send + Sync,
{
    /// Store a single item
    fn put(&self, item: T) -> Result<Key>;

    /// Store multiple projects
    fn put_multiple(&self, items: Vec<T>) -> Result<Key>;

    /// Get a single item
    fn get(&self, key: &Key) -> Result<T>;

    /// Get multiple items
    fn get_multiple(&self, key: &Key) -> Result<Vec<T>>;

    /// Get the raw bytes
    fn get_raw(&self, key: &Key) -> Result<Vec<u8>>;

    /// Stores raw bytes
    fn put_raw(&self, data: &[u8]) -> Result<Key>;

    /// Gets the number of items in storage
    fn len(&self) -> usize;

    /// Whether it is empty or not
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Lists all keys
    fn list(&self) -> Vec<Key>;

    /// Delete the key
    fn del(&self, key: &Key) -> Result<()>;

    /// Open Storage
    fn open(&self) -> Result<()>;

    /// Delete the storage
    fn delete(&self) -> Result<()>;
}

const DEFAULT_LIMIT: u64 = 100000;
const DEFAULT_EXT: &str = ".unknown";
const COMPRESS_EXT: &str = ".snappy";

/// Queue storage implementation
pub struct QueueStore<T> {
    /// Project Limitations
    entry_limit: u64,
    /// Storage directory
    directory: PathBuf,
    /// filename extension
    file_ext: String,
    /// Item mapping: key -> modified time (Unix nanoseconds)
    entries: Arc<RwLock<HashMap<String, i64>>>,
    /// Type tags
    _phantom: PhantomData<T>,
}

impl<T> QueueStore<T>
where
    T: Serialize + DeserializeOwned + Send + Sync,
{
    /// Create a new queue store
    pub fn new(directory: impl Into<PathBuf>, limit: u64, ext: Option<String>) -> Self {
        let limit = if limit == 0 { DEFAULT_LIMIT } else { limit };
        let ext = ext.unwrap_or_else(|| DEFAULT_EXT.to_string());

        Self {
            directory: directory.into(),
            entry_limit: limit,
            file_ext: ext,
            entries: Arc::new(RwLock::new(HashMap::with_capacity(limit as usize))),
            _phantom: PhantomData,
        }
    }

    /// Lists all files in the directory, sorted by modification time (oldest takes precedence.)）
    fn list_files(&self) -> Result<Vec<fs::DirEntry>> {
        let mut files = Vec::new();

        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if metadata.is_file() {
                files.push(entry);
            }
        }

        // Sort by modification time
        files.sort_by(|a, b| {
            let a_time = a
                .metadata()
                .map(|m| m.modified())
                .unwrap_or(Ok(UNIX_EPOCH))
                .unwrap_or(UNIX_EPOCH);
            let b_time = b
                .metadata()
                .map(|m| m.modified())
                .unwrap_or(Ok(UNIX_EPOCH))
                .unwrap_or(UNIX_EPOCH);
            a_time.cmp(&b_time)
        });

        Ok(files)
    }

    /// Write the object to a file
    fn write_object(&self, key: &Key, item: &T) -> Result<()> {
        // Serialize the object
        let data = serde_json::to_vec(item)?;
        self.write_bytes(key, &data)
    }

    /// Write multiple objects to a file
    fn write_multiple_objects(&self, key: &Key, items: &[T]) -> Result<()> {
        let mut data = Vec::new();
        for item in items {
            let item_data = serde_json::to_vec(item)?;
            data.extend_from_slice(&item_data);
            data.push(b'\n');
        }
        self.write_bytes(key, &data)
    }

    /// Write bytes to a file
    fn write_bytes(&self, key: &Key, data: &[u8]) -> Result<()> {
        let path = self.directory.join(key.to_string());

        let file_data = if key.compress {
            // Use snap to compress data
            let mut encoder = Encoder::new();
            encoder
                .compress_vec(data)
                .map_err(|e| Error::msg(format!("Compression failed:{}", e)))?
        } else {
            data.to_vec()
        };

        // Make sure the directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Write to the file
        fs::write(&path, &file_data)?;

        // Update the item mapping
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as i64;

        let mut entries = self.entries.write().map_err(|_| Error::msg("获取写锁失败"))?;
        entries.insert(key.to_string(), now);

        Ok(())
    }

    /// Read bytes from a file
    fn read_bytes(&self, key: &Key) -> Result<Vec<u8>> {
        let path = self.directory.join(key.to_string());
        let data = fs::read(&path)?;

        if data.is_empty() {
            return Err(Error::msg("The file is empty"));
        }

        if key.compress {
            // Use Snap to extract the data
            let mut decoder = Decoder::new();
            decoder
                .decompress_vec(&data)
                .map_err(|e| Error::msg(format!("Failed to decompress:{}", e)))
        } else {
            Ok(data)
        }
    }
}

impl<T> Store<T> for QueueStore<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + Clone,
{
    fn open(&self) -> Result<()> {
        // Create a directory (if it doesn't exist)
        fs::create_dir_all(&self.directory)?;

        // Read existing files
        let files = self.list_files()?;

        let mut entries = self
            .entries
            .write()
            .map_err(|_| Error::msg("Failed to obtain a write lock"))?;

        for file in files {
            if let Ok(meta) = file.metadata() {
                if let Ok(modified) = meta.modified() {
                    if let Ok(since_epoch) = modified.duration_since(UNIX_EPOCH) {
                        entries.insert(file.file_name().to_string_lossy().to_string(), since_epoch.as_nanos() as i64);
                    }
                }
            }
        }

        Ok(())
    }

    fn delete(&self) -> Result<()> {
        fs::remove_dir_all(&self.directory)?;
        Ok(())
    }

    fn put(&self, item: T) -> Result<Key> {
        {
            let entries = self.entries.read().map_err(|_| Error::msg("Failed to obtain a read lock"))?;
            if entries.len() as u64 >= self.entry_limit {
                return Err(Error::msg("The storage limit has been reached"));
            }
        }

        // generate a new uuid
        let uuid = Uuid::new_v4();
        let key = Key::new(uuid.to_string(), &self.file_ext, true);

        self.write_object(&key, &item)?;

        Ok(key)
    }

    fn put_multiple(&self, items: Vec<T>) -> Result<Key> {
        if items.is_empty() {
            return Err(Error::msg("The list of items is empty"));
        }

        {
            let entries = self.entries.read().map_err(|_| Error::msg("Failed to obtain a read lock"))?;
            if entries.len() as u64 >= self.entry_limit {
                return Err(Error::msg("The storage limit has been reached"));
            }
        }

        // Generate a new UUID
        let uuid = Uuid::new_v4();
        let mut key = Key::new(uuid.to_string(), &self.file_ext, true);
        key.item_count = items.len();

        self.write_multiple_objects(&key, &items)?;

        Ok(key)
    }

    fn get(&self, key: &Key) -> Result<T> {
        let items = self.get_multiple(key)?;
        if items.is_empty() {
            return Err(Error::msg("Item not found"));
        }

        Ok(items[0].clone())
    }

    fn get_multiple(&self, key: &Key) -> Result<Vec<T>> {
        let data = self.get_raw(key)?;

        let mut items = Vec::with_capacity(key.item_count);
        let mut reader = io::Cursor::new(&data);

        // Try to read each JSON object
        let mut buffer = Vec::new();

        // if the read fails try parsing it once
        if reader.read_to_end(&mut buffer).is_err() {
            // Try to parse the entire data as a single object
            match serde_json::from_slice::<T>(&data) {
                Ok(item) => {
                    items.push(item);
                    return Ok(items);
                }
                Err(_) => {
                    // An attempt was made to resolve to an array of objects
                    match serde_json::from_slice::<Vec<T>>(&data) {
                        Ok(array_items) => return Ok(array_items),
                        Err(e) => return Err(Error::msg(format!("Failed to parse the data:{}", e))),
                    }
                }
            }
        }

        // Read JSON objects by row
        for line in buffer.split(|&b| b == b'\n') {
            if !line.is_empty() {
                match serde_json::from_slice::<T>(line) {
                    Ok(item) => items.push(item),
                    Err(e) => tracing::warn!("Failed to parse row data:{}", e),
                }
            }
        }

        if items.is_empty() {
            return Err(Error::msg("Failed to resolve any items"));
        }

        Ok(items)
    }

    fn get_raw(&self, key: &Key) -> Result<Vec<u8>> {
        let data = self.read_bytes(key)?;

        // Delete the wrong file
        if data.is_empty() {
            let _ = self.del(key);
            return Err(Error::msg("the file is empty"));
        }

        Ok(data)
    }

    fn put_raw(&self, data: &[u8]) -> Result<Key> {
        {
            let entries = self.entries.read().map_err(|_| Error::msg("Failed to obtain a read lock"))?;
            if entries.len() as u64 >= self.entry_limit {
                return Err(Error::msg("the storage limit has been reached"));
            }
        }

        // Generate a new UUID
        let uuid = Uuid::new_v4();
        let key = Key::new(uuid.to_string(), &self.file_ext, true);

        self.write_bytes(&key, data)?;

        Ok(key)
    }

    fn len(&self) -> usize {
        self.entries.read().map(|e| e.len()).unwrap_or(0)
    }

    fn list(&self) -> Vec<Key> {
        let entries = match self.entries.read() {
            Ok(guard) => guard,
            Err(_) => return Vec::new(),
        };

        // Convert entries to vectors and sort by timestamp
        let mut entries_vec: Vec<_> = entries.iter().collect();
        entries_vec.sort_by(|a, b| a.1.cmp(b.1));

        // Parsing key
        entries_vec.iter().map(|(filename, _)| parse_key(filename)).collect()
    }

    fn del(&self, key: &Key) -> Result<()> {
        let path = self.directory.join(key.to_string());

        // Delete the file
        if let Err(e) = fs::remove_file(&path) {
            if e.kind() != io::ErrorKind::NotFound {
                return Err(e.into());
            }
        }

        // Remove the entry from the map
        let mut entries = self
            .entries
            .write()
            .map_err(|_| Error::msg("Failed to obtain a write lock"))?;
        entries.remove(&key.to_string());

        Ok(())
    }
}
