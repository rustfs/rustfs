use crate::store::{parse_key, Key, Store, StoreError, StoreResult, DEFAULT_EXT, DEFAULT_LIMIT};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use snap::raw::{Decoder, Encoder};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs;
use tokio::sync::RwLock;
use uuid::Uuid;

pub struct QueueStore<T> {
    entry_limit: u64,
    directory: PathBuf,
    file_ext: String,
    entries: RwLock<BTreeMap<String, i64>>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> QueueStore<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn new<P: AsRef<Path>>(directory: P, limit: u64, ext: Option<&str>) -> Self {
        let entry_limit = if limit == 0 { DEFAULT_LIMIT } else { limit };
        let ext = ext.unwrap_or(DEFAULT_EXT).to_string();

        Self {
            directory: directory.as_ref().to_path_buf(),
            entry_limit,
            file_ext: ext,
            entries: RwLock::new(BTreeMap::new()),
            _phantom: std::marker::PhantomData,
        }
    }

    async fn write_bytes(&self, key: Key, data: Vec<u8>) -> StoreResult<()> {
        let path = self.directory.join(key.to_string());

        let data = if key.compress {
            let mut encoder = Encoder::new();
            encoder.compress_vec(&data).map_err(|e| StoreError::Other(e.to_string()))?
        } else {
            data
        };

        fs::write(&path, &data).await?;

        // 更新条目映射
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| StoreError::Other(e.to_string()))?
            .as_nanos() as i64;

        self.entries.write().await.insert(key.to_string(), now);

        Ok(())
    }

    async fn write(&self, key: Key, item: T) -> StoreResult<()> {
        let data = serde_json::to_vec(&item)?;
        self.write_bytes(key, data).await
    }

    async fn multi_write(&self, key: Key, items: Vec<T>) -> StoreResult<()> {
        let mut buffer = Vec::new();

        for item in items {
            let item_data = serde_json::to_vec(&item)?;
            buffer.extend_from_slice(&item_data);
            buffer.push(b'\n'); // 使用换行符分隔项目
        }

        self.write_bytes(key, buffer).await
    }

    async fn del_internal(&self, key: &Key) -> StoreResult<()> {
        let path = self.directory.join(key.to_string());

        if let Err(e) = fs::remove_file(&path).await {
            if e.kind() != std::io::ErrorKind::NotFound {
                return Err(e.into());
            }
        }

        self.entries.write().await.remove(&key.to_string());

        Ok(())
    }
}

#[async_trait]
impl<T> Store<T> for QueueStore<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    async fn put(&self, item: T) -> StoreResult<Key> {
        let entries_len = self.entries.read().await.len() as u64;
        if entries_len >= self.entry_limit {
            return Err(StoreError::LimitExceeded);
        }

        // 生成 UUID 作为键
        let uuid = Uuid::new_v4();
        let key = Key::new(uuid.to_string(), self.file_ext.clone());

        self.write(key.clone(), item).await?;

        Ok(key)
    }

    async fn put_multiple(&self, items: Vec<T>) -> StoreResult<Key> {
        let entries_len = self.entries.read().await.len() as u64;
        if entries_len >= self.entry_limit {
            return Err(StoreError::LimitExceeded);
        }

        if items.is_empty() {
            return Err(StoreError::Other("Cannot store empty item list".into()));
        }

        // 生成 UUID 作为键
        let uuid = Uuid::new_v4();
        let key = Key::new(uuid.to_string(), self.file_ext.clone())
            .with_item_count(items.len())
            .with_compression(true);

        self.multi_write(key.clone(), items).await?;

        Ok(key)
    }

    async fn get(&self, key: Key) -> StoreResult<T> {
        let items = self.get_multiple(key).await?;
        items
            .into_iter()
            .next()
            .ok_or_else(|| StoreError::Other("No items found".into()))
    }

    async fn get_multiple(&self, key: Key) -> StoreResult<Vec<T>> {
        let data = self.get_raw(key).await?;

        // 尝试解析为 JSON 数组
        match serde_json::from_slice::<Vec<T>>(&data) {
            Ok(items) if !items.is_empty() => return Ok(items),
            Ok(_) => return Err(StoreError::Other("No items deserialized".into())),
            Err(_) => {} // 失败则尝试按行解析
        }
        // 如果直接解析为 Vec<T> 失败，则尝试按行解析
        // 转换为字符串并按行解析
        let data_str = std::str::from_utf8(&data).map_err(StoreError::Utf8)?;
        // 按行解析（JSON Lines）
        let mut items = Vec::new();
        for line in data_str.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let item = serde_json::from_str::<T>(line).map_err(StoreError::Deserialize)?;
            items.push(item);
        }

        if items.is_empty() {
            return Err(StoreError::Other("Failed to deserialize items".into()));
        }

        Ok(items)
    }

    async fn get_raw(&self, key: Key) -> StoreResult<Vec<u8>> {
        let path = self.directory.join(key.to_string());
        let data = fs::read(&path).await?;

        if data.is_empty() {
            return Err(StoreError::Other("Empty file".into()));
        }

        if key.compress {
            let mut decoder = Decoder::new();
            decoder.decompress_vec(&data).map_err(|e| StoreError::Other(e.to_string()))
        } else {
            Ok(data)
        }
    }

    async fn put_raw(&self, data: Vec<u8>) -> StoreResult<Key> {
        let entries_len = self.entries.read().await.len() as u64;
        if entries_len >= self.entry_limit {
            return Err(StoreError::LimitExceeded);
        }

        // 生成 UUID 作为键
        let uuid = Uuid::new_v4();
        let key = Key::new(uuid.to_string(), self.file_ext.clone());

        self.write_bytes(key.clone(), data).await?;

        Ok(key)
    }

    async fn len(&self) -> usize {
        self.entries.read().await.len()
    }

    async fn list(&self) -> Vec<Key> {
        let entries = self.entries.read().await;

        // 将条目转换为 (key, timestamp) 元组并排序
        let mut entries_vec: Vec<(&String, &i64)> = entries.iter().collect();
        entries_vec.sort_by_key(|(_k, &v)| v);

        // 将排序后的键解析为 Key 结构体
        entries_vec.into_iter().map(|(k, _)| parse_key(k)).collect()
    }

    async fn del(&self, key: Key) -> StoreResult<()> {
        self.del_internal(&key).await
    }

    async fn open(&self) -> StoreResult<()> {
        // 创建目录（如果不存在）
        fs::create_dir_all(&self.directory).await?;

        // 读取已经存在的文件
        let entries = self.entries.write();
        let mut entries = entries.await;
        entries.clear();

        let mut dir_entries = fs::read_dir(&self.directory).await?;
        while let Some(entry) = dir_entries.next_entry().await? {
            if let Ok(metadata) = entry.metadata().await {
                if metadata.is_file() {
                    let modified = metadata
                        .modified()?
                        .duration_since(UNIX_EPOCH)
                        .map_err(|e| StoreError::Other(e.to_string()))?
                        .as_nanos() as i64;

                    entries.insert(entry.file_name().to_string_lossy().to_string(), modified);
                }
            }
        }

        Ok(())
    }

    async fn delete(&self) -> StoreResult<()> {
        fs::remove_dir_all(&self.directory).await?;
        Ok(())
    }
}
