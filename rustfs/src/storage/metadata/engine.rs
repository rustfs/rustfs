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

use crate::storage::metadata::mx::StorageManager;
use crate::storage::metadata::reader::new_chunked_reader;
use crate::storage::metadata::types::{ChunkInfo, IndexMetadata, ObjectMetadata};
use bytes::Bytes;
use ferntree::Tree as IndexTree;
use rustfs_ecstore::disk::{DiskAPI, ReadOptions};
use rustfs_ecstore::error::{Error, Result};
use rustfs_ecstore::store_api::{GetObjectReader, ListObjectsV2Info, ObjectInfo, ObjectOptions};
use rustfs_filemeta::FileInfo;
use rustfs_rio::WarpReader;
use std::collections::HashSet;
use std::io::Cursor;
use std::ops::Bound::{Included, Unbounded};
use std::sync::Arc;
use surrealkv::Tree as KvStore;
use tokio::io::{AsyncRead, AsyncReadExt};

/// LocalMetadataEngine manages object metadata, indexing, and data placement.
/// It serves as the unified entry point for local storage operations.
#[derive(Clone)]
pub struct LocalMetadataEngine {
    /// Persistent Key-Value store for Object Metadata (ACID, MVCC).
    /// Replaces `xl.meta` files.
    /// Key: `{bucket}/{key_hash}/{version_id}` -> Value: `ObjectMetadata` (serialized)
    kv_store: Arc<KvStore>,

    /// B+ Tree index for high-performance ListObjects and prefix search.
    /// Replaces filesystem directory scanning.
    /// Key: `{bucket}/{prefix}/{object_name}` -> Value: `null` (existence check)
    index_tree: Arc<IndexTree<String, Bytes>>,

    /// Storage Manager responsible for data placement (Tiering), caching, and IO.
    /// Handles SSD/HDD layering and hot/cold data movement.
    storage_manager: Arc<dyn StorageManager>,

    /// Legacy FileSystem interface for migration and fallback.
    /// Points to the existing `LocalDisk` implementation.
    legacy_fs: Arc<rustfs_ecstore::disk::local::LocalDisk>,
}

impl LocalMetadataEngine {
    pub fn new(
        kv_store: Arc<KvStore>,
        index_tree: Arc<IndexTree<String, Bytes>>,
        storage_manager: Arc<dyn StorageManager>,
        legacy_fs: Arc<rustfs_ecstore::disk::local::LocalDisk>,
    ) -> Self {
        Self {
            kv_store,
            index_tree,
            storage_manager,
            legacy_fs,
        }
    }

    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        mut reader: Box<dyn AsyncRead + Send + Unpin>,
        size: u64,
        _opts: ObjectOptions,
    ) -> Result<ObjectInfo> {
        let is_inline = size < 128 * 1024; // 128KB threshold
        let mut content_hash = String::new();
        let mut inline_data = None;
        let mut chunks = None;

        // Start transaction for ref counting and metadata
        let mut tx = self.kv_store.begin().map_err(|e| Error::other(e.to_string()))?;

        if is_inline {
            let mut data = Vec::with_capacity(size as usize);
            reader.read_to_end(&mut data).await.map_err(|e| Error::other(e.to_string()))?;
            let data = Bytes::from(data);
            content_hash = blake3::hash(&data).to_hex().to_string();
            inline_data = Some(data);
        } else {
            // Chunking logic
            let chunk_size = 5 * 1024 * 1024; // 5MB chunks
            let mut chunk_infos = Vec::new();
            let mut buffer = vec![0u8; chunk_size];
            let mut offset = 0;
            let mut hasher = blake3::Hasher::new();

            loop {
                let mut bytes_read = 0;
                while bytes_read < chunk_size {
                    let n = reader
                        .read(&mut buffer[bytes_read..])
                        .await
                        .map_err(|e| Error::other(e.to_string()))?;
                    if n == 0 {
                        break;
                    }
                    bytes_read += n;
                }

                if bytes_read == 0 {
                    break;
                }

                let chunk_data = Bytes::copy_from_slice(&buffer[..bytes_read]);
                let chunk_hash = blake3::hash(&chunk_data).to_hex().to_string();

                // Update global hash
                hasher.update(&chunk_data);

                if !self.storage_manager.exists(&chunk_hash).await {
                    self.storage_manager.write_data(&chunk_hash, chunk_data).await?;
                }

                // Increment ref count
                self.inc_ref(&mut tx, &chunk_hash).await?;

                chunk_infos.push(ChunkInfo {
                    hash: chunk_hash,
                    size: bytes_read as u64,
                    offset,
                });

                offset += bytes_read as u64;
            }

            content_hash = hasher.finalize().to_hex().to_string();
            chunks = Some(chunk_infos);

            // If not chunked (legacy path or single large chunk), we might want to ref count the whole content hash too
            // But here we use chunks.
        }

        // 2. Prepare Metadata
        let meta = ObjectMetadata {
            bucket: bucket.to_string(),
            key: key.to_string(),
            content_hash: content_hash.clone(),
            size,
            is_inline,
            inline_data,
            chunks,
            ..Default::default()
        };

        // Key format: "buckets/{bucket}/objects/{key}/meta"
        let meta_key = format!("buckets/{}/objects/{}/meta", bucket, key);
        let meta_bytes = serde_json::to_vec(&meta).map_err(|e| Error::other(e.to_string()))?;

        tx.set(meta_key.as_bytes(), &meta_bytes)
            .map_err(|e| Error::other(e.to_string()))?;

        // Update Index
        let index_key = format!("{}/{}", bucket, key);
        let index_meta = IndexMetadata {
            size: meta.size,
            mod_time: meta.mod_time,
            etag: meta.content_hash.clone(),
        };
        let index_bytes = serde_json::to_vec(&index_meta).map_err(|e| Error::other(e.to_string()))?;
        self.index_tree.insert(index_key, Bytes::from(index_bytes));

        tx.commit().await.map_err(|e| Error::other(e.to_string()))?;

        // Convert ObjectMetadata to ObjectInfo
        Ok(ObjectInfo {
            bucket: meta.bucket,
            name: meta.key,
            size: meta.size as i64,
            etag: Some(meta.content_hash),
            ..Default::default()
        })
    }

    pub async fn get_object_reader(&self, bucket: &str, key: &str, _opts: ObjectOptions) -> Result<GetObjectReader> {
        // 1. KV Lookup
        let meta_key = format!("buckets/{}/objects/{}/meta", bucket, key);
        let tx = self.kv_store.begin().map_err(|e| Error::other(e.to_string()))?;

        if let Some(val) = tx.get(meta_key.as_bytes()).map_err(|e| Error::other(e.to_string()))? {
            let meta: ObjectMetadata = serde_json::from_slice(&val).map_err(|e| Error::other(e.to_string()))?;

            let reader: Box<dyn AsyncRead + Send + Unpin> = if meta.is_inline {
                let data = meta.inline_data.unwrap_or_default();
                Box::new(WarpReader::new(Cursor::new(data)))
            } else if let Some(chunks) = meta.chunks {
                Box::new(new_chunked_reader(self.storage_manager.clone(), chunks))
            } else {
                // Fallback for legacy non-chunked data (if any)
                let data = self.storage_manager.read_data(&meta.content_hash).await?;
                Box::new(WarpReader::new(Cursor::new(data)))
            };

            let info = ObjectInfo {
                bucket: meta.bucket,
                name: meta.key,
                size: meta.size as i64,
                etag: Some(meta.content_hash),
                ..Default::default()
            };

            return Ok(GetObjectReader {
                stream: reader,
                object_info: info,
            });
        }

        // 2. Miss (Fallback & Migrate)
        // Call legacy_fs to read xl.meta
        match self.legacy_fs.read_xl(bucket, key, false).await {
            Ok(_raw_fi) => {
                // Found in legacy!
                // Get full info
                let fi = self
                    .legacy_fs
                    .read_version(bucket, bucket, key, "", &ReadOptions::default())
                    .await?;

                // Convert FileInfo to ObjectInfo
                let object_info = ObjectInfo::from_file_info(&fi, bucket, key, false);

                // Trigger Migration
                let engine = self.clone();
                let bucket_clone = bucket.to_string();
                let key_clone = key.to_string();
                let fi_clone = fi.clone();

                tokio::spawn(async move {
                    if let Err(e) = engine.migrate_object(&bucket_clone, &key_clone, fi_clone).await {
                        tracing::error!("Migration failed for {}/{}: {:?}", bucket_clone, key_clone, e);
                    }
                });

                // Return legacy reader
                let data = self.legacy_fs.read_all(bucket, key).await?;
                let reader = Box::new(WarpReader::new(Cursor::new(data)));

                Ok(GetObjectReader {
                    stream: reader,
                    object_info,
                })
            }
            Err(_) => Err(Error::other("Object not found in new engine or legacy")),
        }
    }

    pub async fn get_object(&self, bucket: &str, key: &str, _opts: ObjectOptions) -> Result<ObjectInfo> {
        // 1. KV Lookup
        let meta_key = format!("buckets/{}/objects/{}/meta", bucket, key);
        let tx = self.kv_store.begin().map_err(|e| Error::other(e.to_string()))?;

        if let Some(val) = tx.get(meta_key.as_bytes()).map_err(|e| Error::other(e.to_string()))? {
            let meta: ObjectMetadata = serde_json::from_slice(&val).map_err(|e| Error::other(e.to_string()))?;

            return Ok(ObjectInfo {
                bucket: meta.bucket,
                name: meta.key,
                size: meta.size as i64,
                etag: Some(meta.content_hash),
                ..Default::default()
            });
        }

        Err(Error::other("Object not found in new engine"))
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        let meta_key = format!("buckets/{}/objects/{}/meta", bucket, key);
        let mut tx = self.kv_store.begin().map_err(|e| Error::other(e.to_string()))?;

        let meta_bytes = if let Some(val) = tx.get(meta_key.as_bytes()).map_err(|e| Error::other(e.to_string()))? {
            val
        } else {
            return Ok(()); // Object not found, treat as success (idempotent)
        };

        let meta: ObjectMetadata = serde_json::from_slice(&meta_bytes).map_err(|e| Error::other(e.to_string()))?;

        // Decrement refs
        let mut hashes_to_delete = Vec::new();

        if !meta.is_inline {
            if let Some(chunks) = &meta.chunks {
                for chunk in chunks {
                    let count = self.dec_ref(&mut tx, &chunk.hash).await?;
                    if count == 0 {
                        hashes_to_delete.push(chunk.hash.clone());
                    }
                }
            } else {
                // Legacy non-chunked
                let count = self.dec_ref(&mut tx, &meta.content_hash).await?;
                if count == 0 {
                    hashes_to_delete.push(meta.content_hash.clone());
                }
            }
        }

        // Delete metadata
        tx.delete(meta_key.as_bytes()).map_err(|e| Error::other(e.to_string()))?;

        // Delete index
        let index_key = format!("{}/{}", bucket, key);
        self.index_tree.remove(&index_key);

        tx.commit().await.map_err(|e| Error::other(e.to_string()))?;

        // Async delete data
        let storage_manager = self.storage_manager.clone();
        tokio::spawn(async move {
            for hash in hashes_to_delete {
                let _ = storage_manager.delete_data(&hash).await;
            }
        });

        Ok(())
    }

    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        delimiter: Option<String>,
        max_keys: usize,
    ) -> Result<ListObjectsV2Info> {
        let start_key = if let Some(m) = marker.clone() {
            format!("{}/{}", bucket, m)
        } else {
            format!("{}/{}", bucket, prefix)
        };

        let scan_prefix = format!("{}/{}", bucket, prefix);

        // Ferntree range scan
        // Note: Ferntree operations are synchronous (in-memory).
        // We can run this directly.

        let range = self.index_tree.range(Included(&start_key), Unbounded);

        let mut objects = Vec::new();
        let mut common_prefixes = HashSet::new();
        let mut next_marker = None;
        let mut count = 0;

        for (k, v) in range {
            if !k.starts_with(&scan_prefix) {
                break;
            }

            let object_key = &k[bucket.len() + 1..];

            // Handle Delimiter
            if let Some(delim) = &delimiter {
                let remaining = &object_key[prefix.len()..];
                if let Some(idx) = remaining.find(delim) {
                    let prefix_end = prefix.len() + idx + delim.len();
                    let common_prefix = object_key[..prefix_end].to_string();

                    if !common_prefixes.contains(&common_prefix) {
                        if count >= max_keys {
                            next_marker = Some(object_key.to_string());
                            break;
                        }
                        common_prefixes.insert(common_prefix);
                        count += 1;
                    }
                    continue;
                }
            }

            if count >= max_keys {
                next_marker = Some(object_key.to_string());
                break;
            }

            // Parse IndexMetadata
            if let Ok(meta) = serde_json::from_slice::<IndexMetadata>(&v) {
                objects.push(ObjectInfo {
                    bucket: bucket.to_string(),
                    name: object_key.to_string(),
                    size: meta.size as i64,
                    mod_time: Some(
                        time::OffsetDateTime::from_unix_timestamp(meta.mod_time).unwrap_or(time::OffsetDateTime::now_utc()),
                    ),
                    etag: Some(meta.etag),
                    is_dir: false,
                    ..Default::default()
                });
            } else {
                // Fallback if parse fails (e.g. old format or empty)
                objects.push(ObjectInfo {
                    bucket: bucket.to_string(),
                    name: object_key.to_string(),
                    ..Default::default()
                });
            }

            count += 1;
        }

        let is_truncated = next_marker.is_some();

        Ok(ListObjectsV2Info {
            is_truncated,
            continuation_token: marker,
            next_continuation_token: next_marker,
            objects,
            prefixes: common_prefixes.into_iter().collect(),
        })
    }

    async fn migrate_object(&self, bucket: &str, key: &str, fi: FileInfo) -> Result<()> {
        // 1. Read data from legacy
        // For simplicity, read all data. For large files, this should be streamed.
        let data = self.legacy_fs.read_all(bucket, key).await?;

        // 2. Put into new engine
        let content_hash = blake3::hash(&data).to_hex().to_string();
        let is_inline = data.len() < 128 * 1024;

        // Start transaction
        let mut tx = self.kv_store.begin().map_err(|e| Error::other(e.to_string()))?;

        if !is_inline {
            if !self.storage_manager.exists(&content_hash).await {
                self.storage_manager.write_data(&content_hash, data.clone()).await?;
            }
            self.inc_ref(&mut tx, &content_hash).await?;
        }

        let meta = ObjectMetadata {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: fi.version_id.map(|v| v.to_string()).unwrap_or_default(),
            content_hash: content_hash.clone(),
            size: data.len() as u64,
            created_at: fi.mod_time.map(|t| t.unix_timestamp()).unwrap_or_default(),
            mod_time: fi.mod_time.map(|t| t.unix_timestamp()).unwrap_or_default(),
            user_metadata: fi.metadata.clone(),
            is_inline,
            inline_data: if is_inline { Some(data) } else { None },
            ..Default::default()
        };

        // Commit to KV
        let meta_key = format!("buckets/{}/objects/{}/meta", bucket, key);
        let meta_bytes = serde_json::to_vec(&meta).map_err(|e| Error::other(e.to_string()))?;
        tx.set(meta_key.as_bytes(), &meta_bytes)
            .map_err(|e| Error::other(e.to_string()))?;

        // Update Index
        let index_key = format!("{}/{}", bucket, key);
        let index_meta = IndexMetadata {
            size: meta.size,
            mod_time: meta.mod_time,
            etag: meta.content_hash.clone(),
        };
        let index_bytes = serde_json::to_vec(&index_meta).map_err(|e| Error::other(e.to_string()))?;
        self.index_tree.insert(index_key, Bytes::from(index_bytes));

        tx.commit().await.map_err(|e| Error::other(e.to_string()))?;

        Ok(())
    }

    async fn inc_ref(&self, tx: &mut surrealkv::Transaction, hash: &str) -> Result<()> {
        let key = format!("refs/{}", hash);
        let count = if let Some(val) = tx.get(key.as_bytes()).map_err(|e| Error::other(e.to_string()))? {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&val);
            u64::from_be_bytes(buf)
        } else {
            0
        };

        let new_count = count + 1;
        tx.set(key.as_bytes(), &new_count.to_be_bytes())
            .map_err(|e| Error::other(e.to_string()))?;
        Ok(())
    }

    async fn dec_ref(&self, tx: &mut surrealkv::Transaction, hash: &str) -> Result<u64> {
        let key = format!("refs/{}", hash);
        let count = if let Some(val) = tx.get(key.as_bytes()).map_err(|e| Error::other(e.to_string()))? {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&val);
            u64::from_be_bytes(buf)
        } else {
            0
        };

        if count == 0 {
            return Ok(0);
        }

        let new_count = count - 1;
        if new_count == 0 {
            tx.del(key.as_bytes()).map_err(|e| Error::other(e.to_string()))?;
        } else {
            tx.set(key.as_bytes(), &new_count.to_be_bytes())
                .map_err(|e| Error::other(e.to_string()))?;
        }

        Ok(new_count)
    }
}
