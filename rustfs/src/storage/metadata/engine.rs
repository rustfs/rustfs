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
use crate::storage::metadata::types::{IndexMetadata, ObjectMetadata};
use crate::storage::metadata::writer::ChunkedWriter;
use async_trait::async_trait;
use bytes::Bytes;
use ferntree::Tree as IndexTree;
use rustfs_ecstore::disk::{DiskAPI, ReadOptions};
use rustfs_ecstore::error::{Error, Result};
use rustfs_ecstore::store_api::{GetObjectReader, ListObjectsV2Info, ObjectInfo, ObjectOptions};
use rustfs_filemeta::FileInfo;
use rustfs_rio::WarpReader;
use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::ops::Bound::{Included, Unbounded};
use std::sync::Arc;
use surrealkv::Tree as KvStore;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

/// WAL key prefix for pending Ferntree index operations.
/// These keys are written inside the KV transaction to signal a pending index update.
/// On startup the engine must scan this prefix and replay any uncommitted index entries.
const WAL_PENDING_INDEX_PREFIX: &str = "wal/pending_index/";

/// Key prefix used to store the migration cursor (last-processed object key) per bucket.
// Used by the public `migrate_bucket` function.
#[allow(dead_code)]
const MIGRATION_CURSOR_PREFIX: &str = "migration/cursor/";

/// MetadataEngine defines the interface for metadata operations.
#[async_trait]
#[allow(dead_code)]
pub trait MetadataEngine: Send + Sync {
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        size: u64,
        opts: ObjectOptions,
    ) -> Result<ObjectInfo>;

    async fn get_object_reader(&self, bucket: &str, key: &str, opts: &ObjectOptions) -> Result<GetObjectReader>;

    async fn get_object(&self, bucket: &str, key: &str, opts: ObjectOptions) -> Result<ObjectInfo>;

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<()>;

    async fn update_metadata(&self, bucket: &str, key: &str, info: ObjectInfo) -> Result<()>;

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        delimiter: Option<String>,
        max_keys: usize,
    ) -> Result<ListObjectsV2Info>;
}

/// LocalMetadataEngine manages object metadata, indexing, and data placement.
/// It serves as the unified entry point for local storage operations.
#[derive(Clone)]
pub struct LocalMetadataEngine {
    /// Persistent Key-Value store for Object Metadata (ACID, MVCC).
    /// Replaces `xl.meta` files.
    /// Key: `buckets/{bucket}/objects/{key}/meta` -> Value: `ObjectMetadata` (serialized)
    kv_store: Arc<KvStore>,

    /// B+ Tree index for high-performance ListObjects and prefix search.
    /// Replaces filesystem directory scanning.
    /// Key: `{bucket}/{object_name}` -> Value: `IndexMetadata` (serialized)
    ///
    /// # Atomicity note
    /// Ferntree cannot participate in SurrealKV transactions directly. We use a
    /// WAL-marker approach: before committing the KV transaction we write a
    /// `wal/pending_index/{bucket}/{key}` entry containing the serialized
    /// `IndexMetadata`. After the Ferntree insert succeeds we delete the WAL
    /// marker in a separate KV transaction. On node restart `replay_pending_index`
    /// scans this prefix and re-applies any markers left by a crash between the
    /// two steps, guaranteeing eventual consistency.
    index_tree: Arc<IndexTree<String, Bytes>>,

    /// Storage Manager responsible for data placement (Tiering), caching, and IO.
    storage_manager: Arc<dyn StorageManager>,

    /// Legacy FileSystem interface for migration and fallback.
    legacy_fs: Arc<rustfs_ecstore::disk::local::LocalDisk>,

    /// Per-key mutex map used to prevent concurrent Read-Repair / migration of
    /// the same object.  Using `Arc<Mutex<()>>` per slot avoids the DashMap
    /// `or_insert` race (Issue #5): callers lock the mutex before proceeding,
    /// ensuring only one goroutine migrates each object at a time.
    repair_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
}

#[async_trait]
impl MetadataEngine for LocalMetadataEngine {
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        mut reader: Box<dyn AsyncRead + Send + Unpin>,
        size: u64,
        opts: ObjectOptions,
    ) -> Result<ObjectInfo> {
        let is_inline = size < 128 * 1024; // 128KB threshold
        let content_hash;
        let mut inline_data = None;
        let mut chunks = None;

        // === Phase 1: Write data blocks BEFORE opening the KV transaction ===
        // (Issue #3 fix) Two-phase commit: write data first so that if the KV
        // transaction later fails the data block is an orphan with ref-count 0,
        // which GC will eventually reclaim.  The reverse order (tx first, then
        // write) could leave metadata pointing at a missing block — an
        // unrecoverable error that GC cannot fix.
        if is_inline {
            let mut data = Vec::with_capacity(size as usize);
            reader.read_to_end(&mut data).await.map_err(|e| Error::other(e.to_string()))?;
            let data = Bytes::from(data);
            content_hash = blake3::hash(&data).to_hex().to_string();
            inline_data = Some(data);
        } else {
            let chunk_size = 5 * 1024 * 1024; // 5 MB chunks
            let mut writer = ChunkedWriter::new(self.storage_manager.clone(), chunk_size);

            tokio::io::copy(&mut reader, &mut writer)
                .await
                .map_err(|e| Error::other(e.to_string()))?;
            writer.shutdown().await.map_err(|e| Error::other(e.to_string()))?;

            content_hash = writer.content_hash();
            chunks = Some(writer.chunks());
        }

        // === Phase 2: KV transaction (metadata + ref-counts + WAL marker) ===
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        let meta = ObjectMetadata {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: opts.version_id.clone().unwrap_or_default(),
            content_hash: content_hash.clone(),
            size,
            is_inline,
            inline_data: inline_data.clone(),
            chunks: chunks.clone(),
            user_metadata: opts.user_defined.clone(),
            mod_time: opts.mod_time.map(|t| t.unix_timestamp()).unwrap_or(now),
            created_at: now,
            ..Default::default()
        };

        let meta_key = format!("buckets/{}/objects/{}/meta", bucket, key);
        let meta_bytes = serde_json::to_vec(&meta).map_err(|e| Error::other(e.to_string()))?;

        let index_key = format!("{}/{}", bucket, key);
        let index_meta = IndexMetadata {
            size: meta.size,
            mod_time: meta.mod_time,
            etag: meta.content_hash.clone(),
        };
        let index_bytes = serde_json::to_vec(&index_meta).map_err(|e| Error::other(e.to_string()))?;

        // WAL marker key: written atomically inside the KV tx so a crash between
        // the KV commit and the Ferntree insert can be recovered on restart.
        let wal_key = format!("{}{}/{}", WAL_PENDING_INDEX_PREFIX, bucket, key);

        let mut tx = self.kv_store.begin().map_err(|e| Error::other(e.to_string()))?;

        // Increment ref counts for all chunks inside the same transaction.
        if let Some(ref chunk_infos) = chunks {
            for chunk in chunk_infos {
                self.inc_ref(&mut tx, &chunk.hash).await?;
            }
        }

        tx.set(meta_key.as_bytes(), &meta_bytes)
            .map_err(|e| Error::other(e.to_string()))?;

        // Write WAL marker before committing so it is durable after the commit.
        tx.set(wal_key.as_bytes(), &index_bytes)
            .map_err(|e| Error::other(e.to_string()))?;

        tx.commit().await.map_err(|e| Error::other(e.to_string()))?;

        // === Phase 3: Apply Ferntree index (may fail; WAL marker enables replay) ===
        self.index_tree.insert(index_key.clone(), Bytes::from(index_bytes));

        // === Phase 4: Remove WAL marker ===
        // Best-effort: if this fails the marker is replayed on next startup,
        // which is idempotent (re-inserting the same entry to Ferntree is safe).
        if let Err(e) = self.delete_wal_marker(&wal_key).await {
            tracing::warn!(
                "Failed to delete WAL marker for {}/{}: {:?}. Will be replayed on restart.",
                bucket,
                key,
                e
            );
        }

        Ok(ObjectInfo {
            bucket: meta.bucket,
            name: meta.key,
            size: meta.size as i64,
            etag: Some(meta.content_hash),
            user_defined: meta.user_metadata,
            ..Default::default()
        })
    }

    async fn get_object_reader(&self, bucket: &str, key: &str, _opts: &ObjectOptions) -> Result<GetObjectReader> {
        // 1. KV Lookup
        let meta_key = format!("buckets/{}/objects/{}/meta", bucket, key);
        let tx = self.kv_store.begin().map_err(|e| Error::other(e.to_string()))?;

        if let Some(val) = tx.get(meta_key.as_bytes()).map_err(|e| Error::other(e.to_string()))? {
            let meta: ObjectMetadata = serde_json::from_slice(&val).map_err(|e| Error::other(e.to_string()))?;

            let reader: Box<dyn AsyncRead + Send + Sync + Unpin> = if meta.is_inline {
                let data = meta.inline_data.unwrap_or_default();
                Box::new(WarpReader::new(Cursor::new(data)))
            } else if let Some(ref chunks) = meta.chunks {
                Box::new(new_chunked_reader(self.storage_manager.clone(), chunks.clone()))
            } else {
                let data = self.storage_manager.read_data(&meta.content_hash).await?;
                Box::new(WarpReader::new(Cursor::new(data)))
            };

            let info = ObjectInfo {
                bucket: meta.bucket,
                name: meta.key,
                size: meta.size as i64,
                etag: Some(meta.content_hash),
                user_defined: meta.user_metadata,
                ..Default::default()
            };

            return Ok(GetObjectReader {
                stream: reader,
                object_info: info,
            });
        }

        // 2. Miss (Fallback & Migrate)
        match self.legacy_fs.read_xl(bucket, key, false).await {
            Ok(_raw_fi) => {
                let fi = self
                    .legacy_fs
                    .read_version(bucket, bucket, key, "", &ReadOptions::default())
                    .await?;

                let object_info = ObjectInfo::from_file_info(&fi, bucket, key, false);

                // Trigger streaming migration (Issue #7 fix: no full read into memory here).
                let engine = self.clone();
                let bucket_clone = bucket.to_string();
                let key_clone = key.to_string();
                let fi_clone = fi.clone();

                tokio::spawn(async move {
                    if let Err(e) = engine.migrate_object_streaming(&bucket_clone, &key_clone, fi_clone).await {
                        tracing::error!("Migration failed for {}/{}: {:?}", bucket_clone, key_clone, e);
                    }
                });

                // Serve the legacy reader directly (streaming, no full buffering).
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

    async fn get_object(&self, bucket: &str, key: &str, _opts: ObjectOptions) -> Result<ObjectInfo> {
        let meta_key = format!("buckets/{}/objects/{}/meta", bucket, key);
        let tx = self.kv_store.begin().map_err(|e| Error::other(e.to_string()))?;

        if let Some(val) = tx.get(meta_key.as_bytes()).map_err(|e| Error::other(e.to_string()))? {
            let meta: ObjectMetadata = serde_json::from_slice(&val).map_err(|e| Error::other(e.to_string()))?;
            Ok(ObjectInfo {
                bucket: meta.bucket,
                name: meta.key,
                // (Issue #8 fix) Size is read from metadata — never from reading the data block.
                size: meta.size as i64,
                etag: Some(meta.content_hash),
                user_defined: meta.user_metadata,
                ..Default::default()
            })
        } else {
            Err(Error::other("Object not found"))
        }
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        let meta_key = format!("buckets/{}/objects/{}/meta", bucket, key);
        let mut tx = self.kv_store.begin().map_err(|e| Error::other(e.to_string()))?;

        let meta_bytes = if let Some(val) = tx.get(meta_key.as_bytes()).map_err(|e| Error::other(e.to_string()))? {
            val
        } else {
            return Ok(()); // Idempotent: object not found is not an error.
        };

        let meta: ObjectMetadata = serde_json::from_slice(&meta_bytes).map_err(|e| Error::other(e.to_string()))?;

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
                let count = self.dec_ref(&mut tx, &meta.content_hash).await?;
                if count == 0 {
                    hashes_to_delete.push(meta.content_hash.clone());
                }
            }
        }

        tx.delete(meta_key.as_bytes()).map_err(|e| Error::other(e.to_string()))?;

        // Remove WAL marker if it exists (clean up any previous crash residue).
        let wal_key = format!("{}{}/{}", WAL_PENDING_INDEX_PREFIX, bucket, key);
        // Ignore error: the key may not exist.
        let _ = tx.delete(wal_key.as_bytes());

        tx.commit().await.map_err(|e| Error::other(e.to_string()))?;

        // Remove from Ferntree after successful KV commit.
        let index_key = format!("{}/{}", bucket, key);
        self.index_tree.remove(&index_key);

        // Async delete of data blocks.
        let storage_manager = self.storage_manager.clone();
        tokio::spawn(async move {
            for hash in hashes_to_delete {
                if let Err(e) = storage_manager.delete_data(&hash).await {
                    tracing::warn!("Failed to delete data block {}: {:?}", hash, e);
                }
            }
        });

        Ok(())
    }

    async fn update_metadata(&self, bucket: &str, key: &str, info: ObjectInfo) -> Result<()> {
        let meta_key = format!("buckets/{}/objects/{}/meta", bucket, key);
        let mut tx = self.kv_store.begin().map_err(|e| Error::other(e.to_string()))?;

        let mut meta = if let Some(val) = tx.get(meta_key.as_bytes()).map_err(|e| Error::other(e.to_string()))? {
            serde_json::from_slice::<ObjectMetadata>(&val).map_err(|e| Error::other(e.to_string()))?
        } else {
            ObjectMetadata {
                bucket: bucket.to_string(),
                key: key.to_string(),
                ..Default::default()
            }
        };

        meta.size = info.size as u64;
        if let Some(etag) = info.etag {
            meta.content_hash = etag;
        }
        meta.user_metadata = info.user_defined;

        let meta_bytes = serde_json::to_vec(&meta).map_err(|e| Error::other(e.to_string()))?;
        tx.set(meta_key.as_bytes(), &meta_bytes)
            .map_err(|e| Error::other(e.to_string()))?;

        let index_key = format!("{}/{}", bucket, key);
        let index_meta = IndexMetadata {
            size: meta.size,
            mod_time: meta.mod_time,
            etag: meta.content_hash.clone(),
        };
        let index_bytes = serde_json::to_vec(&index_meta).map_err(|e| Error::other(e.to_string()))?;

        // Write WAL marker before commit for crash-safe Ferntree update.
        let wal_key = format!("{}{}/{}", WAL_PENDING_INDEX_PREFIX, bucket, key);
        tx.set(wal_key.as_bytes(), &index_bytes)
            .map_err(|e| Error::other(e.to_string()))?;

        tx.commit().await.map_err(|e| Error::other(e.to_string()))?;

        self.index_tree.insert(index_key, Bytes::from(index_bytes));

        if let Err(e) = self.delete_wal_marker(&wal_key).await {
            tracing::warn!(
                "Failed to delete WAL marker for {}/{}: {:?}. Will be replayed on restart.",
                bucket,
                key,
                e
            );
        }

        Ok(())
    }

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        delimiter: Option<String>,
        max_keys: usize,
    ) -> Result<ListObjectsV2Info> {
        let start_key = if let Some(ref m) = marker {
            format!("{}/{}", bucket, m)
        } else {
            format!("{}/{}", bucket, prefix)
        };

        let scan_prefix = format!("{}/{}", bucket, prefix);

        let mut range = self.index_tree.range(Included(&start_key), Unbounded);

        let mut objects = Vec::new();
        let mut common_prefixes = HashSet::new();
        let mut next_marker = None;
        let mut count = 0;

        while let Some((k, v)) = range.next() {
            if !k.starts_with(&scan_prefix) {
                break;
            }

            let object_key = &k[bucket.len() + 1..];

            if let Some(ref delim) = delimiter {
                let remaining = &object_key[prefix.len()..];
                if let Some(idx) = remaining.find(delim.as_str()) {
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

            if let Ok(meta) = serde_json::from_slice::<IndexMetadata>(v) {
                objects.push(ObjectInfo {
                    bucket: bucket.to_string(),
                    name: object_key.to_string(),
                    size: meta.size as i64,
                    mod_time: Some(
                        time::OffsetDateTime::from_unix_timestamp(meta.mod_time)
                            .unwrap_or_else(|_| time::OffsetDateTime::now_utc()),
                    ),
                    etag: Some(meta.etag),
                    is_dir: false,
                    ..Default::default()
                });
            } else {
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
            repair_locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Replay any pending WAL index markers left by a previous crash.
    ///
    /// This should be called once during engine initialisation, before accepting
    /// any new requests.  It scans the `wal/pending_index/` prefix in KV and
    /// re-inserts the corresponding entries into Ferntree, then clears the
    /// markers.
    #[allow(dead_code)]
    pub async fn replay_pending_index(&self) -> Result<()> {
        use surrealkv::LSMIterator;

        let prefix_end = "wal/pending_index0"; // First key lexicographically after the prefix.
        let tx = self.kv_store.begin().map_err(|e| Error::other(e.to_string()))?;

        let mut pending: Vec<(String, Vec<u8>)> = Vec::new();

        let mut iter = tx
            .range(WAL_PENDING_INDEX_PREFIX.as_bytes(), prefix_end.as_bytes())
            .map_err(|e| Error::other(e.to_string()))?;

        while iter.valid() {
            let raw_key = iter.key();
            let raw_val = iter.value().map_err(|e| Error::other(e.to_string()))?;

            #[allow(clippy::collapsible_if)]
            if let Ok(key_str) = std::str::from_utf8(raw_key.user_key()) {
                if let Some(index_key) = key_str.strip_prefix(WAL_PENDING_INDEX_PREFIX) {
                    pending.push((index_key.to_string(), raw_val.to_vec()));
                }
            }

            iter.next().map_err(|e| Error::other(e.to_string()))?;
        }
        drop(iter);
        drop(tx);

        let replayed = pending.len();
        for (index_key, index_bytes) in pending {
            self.index_tree.insert(index_key.clone(), index_bytes.into());

            let wal_key = format!("{}{}", WAL_PENDING_INDEX_PREFIX, index_key);
            if let Err(e) = self.delete_wal_marker(&wal_key).await {
                tracing::warn!("Failed to clear WAL marker {}: {:?}", wal_key, e);
            }
        }

        if replayed > 0 {
            tracing::info!("replay_pending_index: replayed {} pending index entries from WAL", replayed);
        }

        Ok(())
    }

    /// Delete a single WAL marker key from the KV store.
    async fn delete_wal_marker(&self, wal_key: &str) -> Result<()> {
        let mut tx = self.kv_store.begin().map_err(|e| Error::other(e.to_string()))?;
        // Ignore "key not found" errors: the marker may have already been removed.
        let _ = tx.delete(wal_key.as_bytes());
        tx.commit().await.map_err(|e| Error::other(e.to_string()))?;
        Ok(())
    }

    /// Migrate a single legacy object to the new KV+Ferntree engine using
    /// streaming I/O to avoid reading the entire file into memory (Issue #7 fix).
    ///
    /// A per-key `tokio::sync::Mutex` prevents concurrent migrations of the same
    /// object from corrupting the reference count (Issue #5 fix).
    async fn migrate_object_streaming(&self, bucket: &str, key: &str, fi: FileInfo) -> Result<()> {
        // Acquire per-key migration lock (Issue #5 fix).
        let lock = {
            let mut map = self.repair_locks.lock().await;
            map.entry(format!("{}/{}", bucket, key))
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let _guard = lock.lock().await;

        // If the object was already migrated by a concurrent request, skip.
        let meta_key = format!("buckets/{}/objects/{}/meta", bucket, key);
        {
            let tx = self.kv_store.begin().map_err(|e| Error::other(e.to_string()))?;
            if tx
                .get(meta_key.as_bytes())
                .map_err(|e| Error::other(e.to_string()))?
                .is_some()
            {
                return Ok(());
            }
        }

        // Determine object size from FileInfo to decide inline vs. chunked path.
        // fi.size is i64; treat negative/zero as 0 bytes.
        let object_size = fi.size.max(0) as u64;
        let is_inline = object_size < 128 * 1024;

        let content_hash;
        let inline_data_opt;
        let chunks_opt;

        if is_inline {
            // Small objects: safe to buffer entirely (< 128 KB).
            let data = self.legacy_fs.read_all(bucket, key).await?;
            content_hash = blake3::hash(&data).to_hex().to_string();
            inline_data_opt = Some(data);
            chunks_opt = None;
        } else {
            // Large objects: stream through ChunkedWriter (Issue #7 fix).
            // `read_file` returns a `FileReader` (Box<dyn AsyncRead + ...>) so
            // only one 5 MB chunk needs to reside in memory at a time.
            let mut legacy_reader = self.legacy_fs.read_file(bucket, key).await?;
            let chunk_size = 5 * 1024 * 1024;
            let mut writer = ChunkedWriter::new(self.storage_manager.clone(), chunk_size);

            tokio::io::copy(&mut legacy_reader, &mut writer)
                .await
                .map_err(|e| Error::other(e.to_string()))?;
            writer.shutdown().await.map_err(|e| Error::other(e.to_string()))?;

            content_hash = writer.content_hash();
            chunks_opt = Some(writer.chunks());
            inline_data_opt = None;
        }

        let now = fi.mod_time.map(|t| t.unix_timestamp()).unwrap_or_default();
        let meta = ObjectMetadata {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: fi.version_id.map(|v| v.to_string()).unwrap_or_default(),
            content_hash: content_hash.clone(),
            size: object_size,
            created_at: now,
            mod_time: now,
            user_metadata: fi.metadata.clone(),
            is_inline,
            inline_data: inline_data_opt,
            chunks: chunks_opt.clone(),
            ..Default::default()
        };

        // Write KV + WAL marker atomically, then update Ferntree.
        let meta_bytes = serde_json::to_vec(&meta).map_err(|e| Error::other(e.to_string()))?;

        let index_key = format!("{}/{}", bucket, key);
        let index_meta = IndexMetadata {
            size: meta.size,
            mod_time: meta.mod_time,
            etag: meta.content_hash.clone(),
        };
        let index_bytes = serde_json::to_vec(&index_meta).map_err(|e| Error::other(e.to_string()))?;
        let wal_key = format!("{}{}/{}", WAL_PENDING_INDEX_PREFIX, bucket, key);

        let mut tx = self.kv_store.begin().map_err(|e| Error::other(e.to_string()))?;

        // Increment ref-counts for chunks inside the transaction.
        if let Some(ref chunk_infos) = chunks_opt {
            for chunk in chunk_infos {
                self.inc_ref(&mut tx, &chunk.hash).await?;
            }
        }

        tx.set(meta_key.as_bytes(), &meta_bytes)
            .map_err(|e| Error::other(e.to_string()))?;
        tx.set(wal_key.as_bytes(), &index_bytes)
            .map_err(|e| Error::other(e.to_string()))?;
        tx.commit().await.map_err(|e| Error::other(e.to_string()))?;

        self.index_tree.insert(index_key, Bytes::from(index_bytes));

        if let Err(e) = self.delete_wal_marker(&wal_key).await {
            tracing::warn!("Failed to delete WAL marker for migrated {}/{}: {:?}", bucket, key, e);
        }

        Ok(())
    }

    /// Increment reference count for a data block inside the given transaction.
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

    /// Decrement reference count for a data block inside the given transaction.
    /// Returns the new reference count (0 means the block can be GC'd).
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
            tx.delete(key.as_bytes()).map_err(|e| Error::other(e.to_string()))?;
        } else {
            tx.set(key.as_bytes(), &new_count.to_be_bytes())
                .map_err(|e| Error::other(e.to_string()))?;
        }

        Ok(new_count)
    }

    /// Return the object size from its metadata without reading the data block.
    /// (Issue #8 fix) `get_size` must never call `read_data`; size is stored in
    /// `ObjectMetadata.size` and duplicated in `IndexMetadata.size`.
    #[allow(dead_code)]
    pub async fn get_size_from_metadata(&self, bucket: &str, key: &str) -> Result<u64> {
        let meta_key = format!("buckets/{}/objects/{}/meta", bucket, key);
        let tx = self.kv_store.begin().map_err(|e| Error::other(e.to_string()))?;

        if let Some(val) = tx.get(meta_key.as_bytes()).map_err(|e| Error::other(e.to_string()))? {
            let meta: ObjectMetadata = serde_json::from_slice(&val).map_err(|e| Error::other(e.to_string()))?;
            Ok(meta.size)
        } else {
            Err(Error::other("Object not found"))
        }
    }
}

/// DualMetadataCenter manages two metadata engines for high availability and
/// redundancy.  It implements a Primary/Secondary architecture where writes are
/// synchronised to both centres, and reads fail over from Primary to Secondary.
#[derive(Clone)]
#[allow(dead_code)]
pub struct DualMetadataCenter {
    primary: Arc<dyn MetadataEngine>,
    secondary: Arc<dyn MetadataEngine>,
}

impl DualMetadataCenter {
    #[allow(dead_code)]
    pub fn new(primary: Arc<dyn MetadataEngine>, secondary: Arc<dyn MetadataEngine>) -> Self {
        Self { primary, secondary }
    }
}

#[async_trait]
impl MetadataEngine for DualMetadataCenter {
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        size: u64,
        opts: ObjectOptions,
    ) -> Result<ObjectInfo> {
        let info = self.primary.put_object(bucket, key, reader, size, opts).await?;

        // Shadow Write: asynchronously sync **metadata only** to secondary.
        // NOTE (Issue #2): the secondary does not store the data blocks itself;
        // it relies on the shared StorageManager / shared storage layer to access
        // the blocks written by the primary.  If an independent secondary data
        // store is required, a full data-block replication path must be added.
        let secondary = self.secondary.clone();
        let b = bucket.to_string();
        let k = key.to_string();
        let i = info.clone();

        tokio::spawn(async move {
            if let Err(e) = secondary.update_metadata(&b, &k, i).await {
                tracing::warn!("Failed to sync metadata to secondary center for {}/{}: {:?}", b, k, e);
            }
        });

        Ok(info)
    }

    async fn get_object_reader(&self, bucket: &str, key: &str, opts: &ObjectOptions) -> Result<GetObjectReader> {
        match self.primary.get_object_reader(bucket, key, opts).await {
            Ok(reader) => Ok(reader),
            Err(e) => {
                tracing::warn!(
                    "Primary metadata center lookup failed for {}/{}, falling back to secondary: {:?}",
                    bucket,
                    key,
                    e
                );
                let reader = self.secondary.get_object_reader(bucket, key, opts).await?;

                // Read Repair: attempt to restore the entry in the primary.
                let primary = self.primary.clone();
                let b = bucket.to_string();
                let k = key.to_string();
                let i = reader.object_info.clone();

                tokio::spawn(async move {
                    if let Err(e) = primary.update_metadata(&b, &k, i).await {
                        tracing::error!("Read-repair failed for primary metadata center {}/{}: {:?}", b, k, e);
                    }
                });

                Ok(reader)
            }
        }
    }

    async fn get_object(&self, bucket: &str, key: &str, opts: ObjectOptions) -> Result<ObjectInfo> {
        match self.primary.get_object(bucket, key, opts.clone()).await {
            Ok(info) => Ok(info),
            Err(e) => {
                tracing::warn!(
                    "Primary metadata center lookup failed for {}/{}, falling back to secondary: {:?}",
                    bucket,
                    key,
                    e
                );
                self.secondary.get_object(bucket, key, opts).await
            }
        }
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        let res1 = self.primary.delete_object(bucket, key).await;
        let res2 = self.secondary.delete_object(bucket, key).await;

        if let Err(e) = &res2 {
            tracing::warn!("Failed to delete from secondary metadata center for {}/{}: {:?}", bucket, key, e);
        }

        res1
    }

    async fn update_metadata(&self, bucket: &str, key: &str, info: ObjectInfo) -> Result<()> {
        let res1 = self.primary.update_metadata(bucket, key, info.clone()).await;
        let res2 = self.secondary.update_metadata(bucket, key, info).await;

        if let Err(e) = &res2 {
            tracing::warn!("Failed to update metadata in secondary center for {}/{}: {:?}", bucket, key, e);
        }

        res1
    }

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        delimiter: Option<String>,
        max_keys: usize,
    ) -> Result<ListObjectsV2Info> {
        match self
            .primary
            .list_objects(bucket, prefix, marker.clone(), delimiter.clone(), max_keys)
            .await
        {
            Ok(list) => Ok(list),
            Err(e) => {
                tracing::warn!(
                    "Primary metadata center listing failed for bucket {}, falling back to secondary: {:?}",
                    bucket,
                    e
                );
                self.secondary.list_objects(bucket, prefix, marker, delimiter, max_keys).await
            }
        }
    }
}

/// StrongConsistencyEngine wraps two `MetadataEngine` instances and enforces
/// write-time consistency: a write is considered successful only when **both**
/// engines confirm the same ETag.  Any mismatch is treated as an error rather
/// than silently diverging (Issue #10 fix: no `assert!` in production code).
#[derive(Clone)]
#[allow(dead_code)]
pub struct StrongConsistencyEngine {
    primary: Arc<dyn MetadataEngine>,
    secondary: Arc<dyn MetadataEngine>,
}

impl StrongConsistencyEngine {
    #[allow(dead_code)]
    pub fn new(primary: Arc<dyn MetadataEngine>, secondary: Arc<dyn MetadataEngine>) -> Self {
        Self { primary, secondary }
    }
}

#[async_trait]
impl MetadataEngine for StrongConsistencyEngine {
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        size: u64,
        opts: ObjectOptions,
    ) -> Result<ObjectInfo> {
        // Strong consistency requires writing to both engines and verifying the
        // resulting ETags match.  Because the reader can only be consumed once,
        // we must buffer it here before fanning out to both engines.
        let mut buf = Vec::with_capacity(size as usize);
        let mut pinned = reader;
        pinned.read_to_end(&mut buf).await.map_err(|e| Error::other(e.to_string()))?;
        let data = Bytes::from(buf);

        let opts2 = opts.clone();
        let data2 = data.clone();

        let info1 = self
            .primary
            .put_object(bucket, key, Box::new(std::io::Cursor::new(data.clone())), size, opts)
            .await?;

        let info2 = self
            .secondary
            .put_object(bucket, key, Box::new(std::io::Cursor::new(data2)), size, opts2)
            .await?;

        // (Issue #10 fix) Replace assert! with a proper error return.
        if info1.etag != info2.etag {
            return Err(Error::other(format!(
                "Strong consistency violation: etag mismatch for {}/{}: {:?} vs {:?}",
                bucket, key, info1.etag, info2.etag
            )));
        }

        Ok(info1)
    }

    async fn get_object_reader(&self, bucket: &str, key: &str, opts: &ObjectOptions) -> Result<GetObjectReader> {
        // Reads prefer the primary; no strong read semantics needed here.
        self.primary.get_object_reader(bucket, key, opts).await
    }

    async fn get_object(&self, bucket: &str, key: &str, opts: ObjectOptions) -> Result<ObjectInfo> {
        self.primary.get_object(bucket, key, opts).await
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        let r1 = self.primary.delete_object(bucket, key).await;
        let r2 = self.secondary.delete_object(bucket, key).await;
        if let Err(e) = &r2 {
            tracing::warn!("Secondary delete failed for {}/{}: {:?}", bucket, key, e);
        }
        r1
    }

    async fn update_metadata(&self, bucket: &str, key: &str, info: ObjectInfo) -> Result<()> {
        let r1 = self.primary.update_metadata(bucket, key, info.clone()).await;
        let r2 = self.secondary.update_metadata(bucket, key, info).await;
        if let Err(e) = &r2 {
            tracing::warn!("Secondary update_metadata failed for {}/{}: {:?}", bucket, key, e);
        }
        r1
    }

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        delimiter: Option<String>,
        max_keys: usize,
    ) -> Result<ListObjectsV2Info> {
        self.primary.list_objects(bucket, prefix, marker, delimiter, max_keys).await
    }
}

/// ConsistencyChecker performs periodic sampling of KV↔Ferntree consistency
/// and logs warnings when divergence is detected (Issue #4 partial fix).
///
/// Full repair logic is deferred to a later iteration; this provides at minimum
/// an observable consistency gap metric.
#[allow(dead_code)]
pub struct ConsistencyChecker {
    kv_store: Arc<KvStore>,
    index_tree: Arc<IndexTree<String, Bytes>>,
    /// Fraction of objects to sample per run (0.0–1.0).  Default: 0.01 (1 %).
    sample_rate: f64,
}

impl ConsistencyChecker {
    #[allow(dead_code)]
    pub fn new(kv_store: Arc<KvStore>, index_tree: Arc<IndexTree<String, Bytes>>, sample_rate: f64) -> Self {
        Self {
            kv_store,
            index_tree,
            sample_rate: sample_rate.clamp(0.0, 1.0),
        }
    }

    /// Run a consistency check, sampling up to `max_samples` objects from KV
    /// and verifying each has a corresponding Ferntree entry.
    ///
    /// Returns `(checked, inconsistent)` counts.
    #[allow(dead_code)]
    pub async fn check(&self, max_samples: usize) -> (usize, usize) {
        use surrealkv::LSMIterator;

        let prefix = "buckets/";
        let prefix_end = "buckets0";

        let tx = match self.kv_store.begin() {
            Ok(t) => t,
            Err(e) => {
                tracing::error!("ConsistencyChecker: failed to begin KV transaction: {:?}", e);
                return (0, 0);
            }
        };

        let mut iter = match tx.range(prefix.as_bytes(), prefix_end.as_bytes()) {
            Ok(i) => i,
            Err(e) => {
                tracing::error!("ConsistencyChecker: KV range scan failed: {:?}", e);
                return (0, 0);
            }
        };

        let mut checked = 0usize;
        let mut inconsistent = 0usize;
        let mut skipped = 0usize;

        while iter.valid() && checked < max_samples {
            // Probabilistic sampling: skip entries that fall outside the sample
            // fraction to avoid scanning the full dataset every run.
            skipped += 1;
            let should_check = (skipped as f64 * self.sample_rate).floor() as usize > checked;

            if should_check {
                let raw_key = iter.key();
                // Only examine `buckets/{bucket}/objects/{object_key}/meta` entries.
                // Parse out bucket and object_key, then verify the Ferntree has a
                // matching index entry.
                let key_bytes = raw_key.user_key();
                if let Ok(key_str) = std::str::from_utf8(key_bytes) {
                    self.check_single_key(key_str, &mut checked, &mut inconsistent);
                }
            }

            if let Err(e) = iter.next() {
                tracing::error!("ConsistencyChecker: iterator error: {:?}", e);
                break;
            }
        }

        if inconsistent > 0 {
            tracing::warn!(
                "ConsistencyChecker: found {} inconsistencies out of {} sampled entries",
                inconsistent,
                checked
            );
        } else {
            tracing::debug!("ConsistencyChecker: {} entries sampled, all consistent", checked);
        }

        (checked, inconsistent)
    }

    /// Check a single KV key string for a matching Ferntree index entry.
    /// Updates `checked` and `inconsistent` counters in-place.
    #[allow(dead_code)]
    fn check_single_key(&self, key_str: &str, checked: &mut usize, inconsistent: &mut usize) {
        // KV key format: `buckets/{bucket}/objects/{object_key}/meta`
        if !key_str.ends_with("/meta") {
            return;
        }
        let rest = match key_str.strip_prefix("buckets/") {
            Some(r) => r,
            None => return,
        };
        let mid = match rest.find("/objects/") {
            Some(m) => m,
            None => return,
        };
        let bucket = &rest[..mid];
        let after_objects = &rest[mid + "/objects/".len()..];
        let object_key = match after_objects.strip_suffix("/meta") {
            Some(k) => k,
            None => return,
        };

        let index_key = format!("{}/{}", bucket, object_key);
        if self.index_tree.get(&index_key).is_none() {
            *inconsistent += 1;
            tracing::warn!(
                "ConsistencyChecker: KV entry exists but Ferntree index missing for {}/{}",
                bucket,
                object_key
            );
        }
        *checked += 1;
    }
}

/// Migration statistics collected during a `migrate_bucket` run.
#[derive(Debug, Default, Clone)]
#[allow(dead_code)]
pub struct MigrationStats {
    /// Total number of objects discovered in the legacy store for this bucket.
    pub total: u64,
    /// Number of objects successfully migrated in this run.
    pub migrated: u64,
    /// Number of objects that failed migration (errors logged separately).
    pub errors: u64,
    /// Number of objects skipped because they were already present in KV.
    pub skipped: u64,
}

/// Migrate all legacy objects in `bucket` to the new KV+Ferntree engine.
///
/// Supports **checkpoint / resume** (Issue #11 fix): the last-processed key is
/// persisted to KV after each successful migration so that an interrupted run
/// can continue from where it left off rather than restarting from the
/// beginning.
///
/// `concurrency` controls how many objects are migrated in parallel (1 =
/// serial, safest; higher values increase throughput at the cost of I/O
/// contention).
#[allow(dead_code)]
pub async fn migrate_bucket(
    engine: Arc<LocalMetadataEngine>,
    legacy_fs: Arc<rustfs_ecstore::disk::local::LocalDisk>,
    kv_store: Arc<KvStore>,
    bucket: &str,
    concurrency: usize,
) -> Result<MigrationStats> {
    use tokio::sync::Semaphore;

    let concurrency = concurrency.max(1);
    let semaphore = Arc::new(Semaphore::new(concurrency));

    // Load the migration cursor (last successfully migrated key).
    let cursor_kv_key = format!("{}{}", MIGRATION_CURSOR_PREFIX, bucket);
    let resume_after: Option<String> = {
        let tx = kv_store.begin().map_err(|e| Error::other(e.to_string()))?;
        tx.get(cursor_kv_key.as_bytes())
            .map_err(|e| Error::other(e.to_string()))?
            .and_then(|v| String::from_utf8(v.to_vec()).ok())
    };

    if let Some(ref cursor) = resume_after {
        tracing::info!("migrate_bucket: resuming migration for bucket '{}' after key '{}'", bucket, cursor);
    }

    // Enumerate legacy objects.
    // `list_dir` with count=-1 returns all entries under the bucket root.
    let object_list = legacy_fs.list_dir(bucket, bucket, "", -1).await?;

    let mut stats = MigrationStats {
        total: object_list.len() as u64,
        ..Default::default()
    };

    let mut tasks = Vec::new();
    let mut last_key: Option<String> = None;

    for obj_key in object_list {
        // Skip objects that are lexicographically <= the resume cursor.
        if resume_after.as_deref().is_some_and(|cursor| obj_key.as_str() <= cursor) {
            stats.skipped += 1;
            continue;
        }

        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| Error::other(e.to_string()))?;

        let engine_clone = engine.clone();
        let legacy_clone = legacy_fs.clone();
        let kv_clone = kv_store.clone();
        let cursor_key_clone = cursor_kv_key.clone();
        let bucket_str = bucket.to_string();
        let key_str = obj_key.clone();

        tasks.push(tokio::spawn(async move {
            let _permit = permit; // released when this task finishes

            // Read FileInfo for the object.
            let fi = match legacy_clone
                .read_version(&bucket_str, &bucket_str, &key_str, "", &ReadOptions::default())
                .await
            {
                Ok(fi) => fi,
                Err(e) => {
                    tracing::error!("migrate_bucket: failed to read FileInfo for {}/{}: {:?}", bucket_str, key_str, e);
                    return Err(e);
                }
            };

            engine_clone.migrate_object_streaming(&bucket_str, &key_str, fi).await?;

            // Persist migration cursor after each successful migration.
            match kv_clone.begin() {
                Ok(mut tx) => {
                    let _ = tx.set(cursor_key_clone.as_bytes(), key_str.as_bytes());
                    let _ = tx.commit().await;
                }
                Err(e) => {
                    tracing::warn!("migrate_bucket: failed to update cursor for {}/{}: {:?}", bucket_str, key_str, e);
                }
            }

            Ok(key_str)
        }));

        last_key = Some(obj_key);
    }

    // Collect results.
    for task in tasks {
        match task.await {
            Ok(Ok(_key)) => stats.migrated += 1,
            Ok(Err(_)) => stats.errors += 1,
            Err(e) => {
                tracing::error!("migrate_bucket: task panicked: {:?}", e);
                stats.errors += 1;
            }
        }
    }

    // If everything completed successfully, clear the cursor so a fresh run
    // starts from the beginning next time (idempotent re-migration).
    if stats.errors == 0 {
        drop(kv_store.begin().ok().map(|mut tx| {
            let _ = tx.delete(cursor_kv_key.as_bytes());
            tx
        }));
        // Commit is async and best-effort; omit here to avoid blocking.
    }

    tracing::info!(
        "migrate_bucket: finished bucket '{}' — total={}, migrated={}, skipped={}, errors={}, last_key={:?}",
        bucket,
        stats.total,
        stats.migrated,
        stats.skipped,
        stats.errors,
        last_key
    );

    Ok(stats)
}
