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

use crate::disk::RUSTFS_META_BUCKET;
use crate::error::{Error, Result};
use crate::store::ECStore;
use crate::store_api::{ObjectIO, ObjectOptions};
use crate::store_list_objects::ListPathOptions;
use rustfs_filemeta::{MetaCacheEntriesSorted, MetaCacheEntry, MetacacheReader, MetacacheWriter};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;
use tracing::{debug, warn};
use uuid::Uuid;

/// Scan status for metacache entries
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanStatus {
    None = 0,
    Started = 1,
    Success = 2,
    Error = 3,
}

impl Default for ScanStatus {
    fn default() -> Self {
        Self::None
    }
}

/// Metacache entry representing a list operation
#[derive(Debug, Clone)]
pub struct Metacache {
    pub id: String,
    pub bucket: String,
    pub root: String,
    pub filter: Option<String>,
    pub status: ScanStatus,
    pub started: SystemTime,
    pub ended: Option<SystemTime>,
    pub last_handout: SystemTime,
    pub last_update: SystemTime,
    pub error: Option<String>,
    pub file_not_found: bool,
    pub recursive: bool,
    pub data_version: u8,
}

impl Metacache {
    pub fn new(opts: &ListPathOptions) -> Self {
        let now = SystemTime::now();
        Self {
            id: opts.id.clone().unwrap_or_else(|| Uuid::new_v4().to_string()),
            bucket: opts.bucket.clone(),
            root: opts.base_dir.clone(),
            filter: opts.filter_prefix.clone(),
            status: ScanStatus::Started,
            started: now,
            ended: None,
            last_handout: now,
            last_update: now,
            error: None,
            file_not_found: false,
            recursive: opts.recursive,
            data_version: 2,
        }
    }

    pub fn finished(&self) -> bool {
        self.ended.is_some()
    }

    /// Check if the cache is worth keeping
    pub fn worth_keeping(&self) -> bool {
        const MAX_RUNNING_AGE: Duration = Duration::from_secs(3600); // 1 hour
        const MAX_CLIENT_WAIT: Duration = Duration::from_secs(180); // 3 minutes
        const MAX_FINISHED_WAIT: Duration = Duration::from_secs(900); // 15 minutes
        const MAX_ERROR_WAIT: Duration = Duration::from_secs(300); // 5 minutes

        let now = SystemTime::now();

        match self.status {
            ScanStatus::Started => {
                // Not finished and update for MAX_RUNNING_AGE, discard it
                if let Ok(elapsed) = now.duration_since(self.last_update) {
                    elapsed < MAX_RUNNING_AGE
                } else {
                    false
                }
            }
            ScanStatus::Success => {
                // Keep for MAX_FINISHED_WAIT after we last saw the client
                if let Ok(elapsed) = now.duration_since(self.last_handout) {
                    elapsed < MAX_FINISHED_WAIT
                } else {
                    false
                }
            }
            ScanStatus::Error | ScanStatus::None => {
                // Remove failed listings after MAX_ERROR_WAIT
                if let Ok(elapsed) = now.duration_since(self.last_update) {
                    elapsed < MAX_ERROR_WAIT
                } else {
                    false
                }
            }
        }
    }

    /// Update cache with new status
    pub fn update(&mut self, update: &Metacache) {
        let now = SystemTime::now();
        self.last_update = now;

        if update.last_handout > self.last_handout {
            self.last_handout = update.last_update;
            if self.last_handout > now {
                self.last_handout = now;
            }
        }

        if self.status == ScanStatus::Started && update.status == ScanStatus::Success {
            self.ended = Some(now);
        }

        if self.status == ScanStatus::Started && update.status != ScanStatus::Started {
            self.status = update.status;
        }

        if self.status == ScanStatus::Started {
            if let Ok(elapsed) = now.duration_since(self.last_handout) {
                if elapsed > Duration::from_secs(180) {
                    // Drop if client hasn't been seen for 3 minutes
                    self.status = ScanStatus::Error;
                    self.error = Some("client not seen".to_string());
                }
            }
        }

        if self.error.is_none() && update.error.is_some() {
            self.error = update.error.clone();
            self.status = ScanStatus::Error;
            self.ended = Some(now);
        }

        self.file_not_found = self.file_not_found || update.file_not_found;
    }
}

/// Bucket-level metacache manager
#[derive(Debug)]
pub struct BucketMetacache {
    bucket: String,
    caches: HashMap<String, Metacache>,
    caches_root: HashMap<String, Vec<String>>,
    updated: bool,
}

impl BucketMetacache {
    pub fn new(bucket: String) -> Self {
        Self {
            bucket,
            caches: HashMap::new(),
            caches_root: HashMap::new(),
            updated: false,
        }
    }

    /// Find or create a cache entry
    pub fn find_cache(&mut self, opts: &ListPathOptions) -> Metacache {
        // Check if exists already
        if let Some(mut cache) = self.caches.get(&opts.id.clone().unwrap_or_default()).cloned() {
            cache.last_handout = SystemTime::now();
            self.caches.insert(cache.id.clone(), cache.clone());
            debug!("returning existing cache {}", cache.id);
            return cache;
        }

        if !opts.create {
            return Metacache {
                id: opts.id.clone().unwrap_or_default(),
                bucket: opts.bucket.clone(),
                root: opts.base_dir.clone(),
                filter: opts.filter_prefix.clone(),
                status: ScanStatus::None,
                started: SystemTime::now(),
                ended: None,
                last_handout: SystemTime::now(),
                last_update: SystemTime::now(),
                error: None,
                file_not_found: false,
                recursive: opts.recursive,
                data_version: 2,
            };
        }

        // Create new cache
        let cache = Metacache::new(opts);
        let root = cache.root.clone();
        let id = cache.id.clone();
        self.caches.insert(id.clone(), cache.clone());
        self.caches_root.entry(root).or_default().push(id);
        self.updated = true;
        debug!("returning new cache {}, bucket: {}", cache.id, cache.bucket);
        cache
    }

    /// Update cache entry
    pub fn update_cache_entry(&mut self, update: Metacache) -> Result<Metacache> {
        if let Some(cache) = self.caches.get_mut(&update.id) {
            cache.update(&update);
            self.updated = true;
            Ok(cache.clone())
        } else {
            // Create new entry
            let root = update.root.clone();
            let id = update.id.clone();
            self.caches.insert(id.clone(), update.clone());
            self.caches_root.entry(root).or_default().push(id);
            self.updated = true;
            Ok(update)
        }
    }

    /// Get cache by ID
    pub fn get_cache(&self, id: &str) -> Option<&Metacache> {
        self.caches.get(id)
    }

    /// Cleanup outdated entries
    pub fn cleanup(&mut self) {
        let mut to_remove = Vec::new();
        for (id, cache) in &self.caches {
            if !cache.worth_keeping() {
                to_remove.push(id.clone());
            }
        }

        for id in to_remove {
            if let Some(cache) = self.caches.remove(&id) {
                // Remove from root index
                if let Some(ids) = self.caches_root.get_mut(&cache.root) {
                    ids.retain(|x| x != &id);
                    if ids.is_empty() {
                        self.caches_root.remove(&cache.root);
                    }
                }
                debug!("removed outdated cache {}", id);
            }
        }
    }
}

/// Global metacache manager
pub struct MetacacheManager {
    buckets: HashMap<String, Arc<RwLock<BucketMetacache>>>,
    trash: HashMap<String, Metacache>,
}

impl Default for MetacacheManager {
    fn default() -> Self {
        Self::new()
    }
}

impl MetacacheManager {
    pub fn new() -> Self {
        Self {
            buckets: HashMap::new(),
            trash: HashMap::new(),
        }
    }

    /// Get or create bucket metacache
    pub fn get_bucket(&mut self, bucket: &str) -> Arc<RwLock<BucketMetacache>> {
        if let Some(bm) = self.buckets.get(bucket) {
            return bm.clone();
        }

        let bm = Arc::new(RwLock::new(BucketMetacache::new(bucket.to_string())));
        self.buckets.insert(bucket.to_string(), bm.clone());
        bm
    }

    /// Find cache for given options
    pub async fn find_cache(&self, opts: &ListPathOptions) -> Metacache {
        if let Some(bm) = self.buckets.get(&opts.bucket) {
            let mut bm = bm.write().await;
            bm.find_cache(opts)
        } else {
            // Return empty cache if bucket not found
            Metacache {
                id: opts.id.clone().unwrap_or_default(),
                bucket: opts.bucket.clone(),
                root: opts.base_dir.clone(),
                filter: opts.filter_prefix.clone(),
                status: ScanStatus::None,
                started: SystemTime::now(),
                ended: None,
                last_handout: SystemTime::now(),
                last_update: SystemTime::now(),
                error: None,
                file_not_found: false,
                recursive: opts.recursive,
                data_version: 2,
            }
        }
    }

    /// Update cache entry
    pub async fn update_cache_entry(&mut self, update: Metacache) -> Result<Metacache> {
        // Check trash first
        if let Some(mut meta) = self.trash.get(&update.id).cloned() {
            meta.update(&update);
            return Ok(meta);
        }

        // Get or create bucket metacache
        let bm = self.get_bucket(&update.bucket);
        let mut bm = bm.write().await;
        bm.update_cache_entry(update)
    }

    /// Cleanup outdated entries
    pub async fn cleanup(&mut self) {
        const MAX_RUNNING_AGE: Duration = Duration::from_secs(3600);

        // Cleanup buckets
        for bm in self.buckets.values() {
            let mut bm = bm.write().await;
            bm.cleanup();
        }

        // Cleanup trash
        let mut to_remove = Vec::new();
        for (id, cache) in &self.trash {
            if let Ok(elapsed) = SystemTime::now().duration_since(cache.last_update) {
                if elapsed > MAX_RUNNING_AGE {
                    to_remove.push(id.clone());
                }
            }
        }

        for id in to_remove {
            self.trash.remove(&id);
        }
    }

    /// Get cache path for storage
    fn get_cache_path(bucket: &str, id: &str) -> String {
        format!("buckets/{}/.metacache/{}", bucket, id)
    }

    /// Save cache entries to storage
    pub async fn save_cache_entries(&self, store: Arc<ECStore>, cache: &Metacache, entries: &[MetaCacheEntry]) -> Result<()> {
        let path = Self::get_cache_path(&cache.bucket, &cache.id);

        // Create a writer that writes to store
        let mut writer = Vec::new();
        let mut cache_writer = MetacacheWriter::new(&mut writer);

        for entry in entries {
            cache_writer.write_obj(entry).await?;
        }
        cache_writer.close().await?;

        // Write to store
        use crate::store_api::PutObjReader;
        let mut reader = PutObjReader::from_vec(writer);
        store
            .put_object(
                RUSTFS_META_BUCKET,
                &path,
                &mut reader,
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await?;

        debug!("saved cache entries for {}: {} entries", cache.id, entries.len());
        Ok(())
    }

    /// Stream cache entries from storage
    pub async fn stream_cache_entries(
        &self,
        store: Arc<ECStore>,
        cache: &Metacache,
        marker: Option<String>,
        limit: usize,
    ) -> Result<MetaCacheEntriesSorted> {
        let path = Self::get_cache_path(&cache.bucket, &cache.id);

        // Read from store
        use crate::store_api::ObjectIO;
        use http::HeaderMap;
        let mut reader = store
            .get_object_reader(
                RUSTFS_META_BUCKET,
                &path,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await?;

        let mut cache_reader = MetacacheReader::new(&mut reader);
        let mut entries = Vec::new();
        let mut last_skipped: Option<String> = None;

        while entries.len() < limit {
            match cache_reader.peek().await {
                Ok(Some(entry)) => {
                    // Skip entries before marker (not equal)
                    // Marker is the last object from previous page, so we should start from the next object (> marker)
                    // This matches the behavior of gather_results which uses < marker
                    if let Some(ref m) = marker {
                        if entry.name <= *m {
                            last_skipped = Some(entry.name.clone());
                            // peek() already consumed the entry, so we just continue
                            continue;
                        }
                    }

                    // Add the entry (peek already read it)
                    entries.push(Some(entry));
                }
                Ok(None) => break,
                Err(e) => {
                    warn!("error reading cache entry: {:?}", e);
                    break;
                }
            }
        }

        Ok(MetaCacheEntriesSorted {
            o: rustfs_filemeta::MetaCacheEntries(entries),
            list_id: Some(cache.id.clone()),
            reuse: true,
            last_skipped_entry: last_skipped,
        })
    }
}

/// Global metacache manager instance
static GLOBAL_METACACHE_MANAGER: OnceLock<Arc<RwLock<MetacacheManager>>> = OnceLock::new();

/// Initialize global metacache manager
pub fn init_metacache_manager() -> Arc<RwLock<MetacacheManager>> {
    GLOBAL_METACACHE_MANAGER
        .get_or_init(|| Arc::new(RwLock::new(MetacacheManager::new())))
        .clone()
}

/// Get global metacache manager
pub fn get_metacache_manager() -> Result<Arc<RwLock<MetacacheManager>>> {
    GLOBAL_METACACHE_MANAGER
        .get()
        .cloned()
        .ok_or_else(|| Error::other("metacache manager not initialized"))
}
