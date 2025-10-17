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

//! High-performance file content and metadata caching using moka
//!
//! This module provides optimized caching for file operations to reduce
//! redundant I/O and improve overall system performance.

use super::disk::error::{Error, Result};
use bytes::Bytes;
use moka::future::Cache;
use rustfs_filemeta::FileMeta;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

pub struct OptimizedFileCache {
    // Use moka as high-performance async cache
    metadata_cache: Cache<PathBuf, Arc<FileMeta>>,
    file_content_cache: Cache<PathBuf, Bytes>,
    // Performance monitoring
    cache_hits: std::sync::atomic::AtomicU64,
    cache_misses: std::sync::atomic::AtomicU64,
}

impl OptimizedFileCache {
    pub fn new() -> Self {
        Self {
            metadata_cache: Cache::builder()
                .max_capacity(2048)
                .time_to_live(Duration::from_secs(300)) // 5 minutes TTL
                .time_to_idle(Duration::from_secs(60)) // 1 minute idle
                .build(),

            file_content_cache: Cache::builder()
                .max_capacity(512) // Smaller file content cache
                .time_to_live(Duration::from_secs(120))
                .weigher(|_key: &PathBuf, value: &Bytes| value.len() as u32)
                .build(),

            cache_hits: std::sync::atomic::AtomicU64::new(0),
            cache_misses: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub async fn get_metadata(&self, path: PathBuf) -> Result<Arc<FileMeta>> {
        if let Some(cached) = self.metadata_cache.get(&path).await {
            self.cache_hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(cached);
        }

        self.cache_misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Cache miss, read file
        let data = tokio::fs::read(&path)
            .await
            .map_err(|e| Error::other(format!("Read metadata failed: {e}")))?;

        let mut meta = FileMeta::default();
        meta.unmarshal_msg(&data)?;

        let arc_meta = Arc::new(meta);
        self.metadata_cache.insert(path, arc_meta.clone()).await;

        Ok(arc_meta)
    }

    pub async fn get_file_content(&self, path: PathBuf) -> Result<Bytes> {
        if let Some(cached) = self.file_content_cache.get(&path).await {
            self.cache_hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(cached);
        }

        self.cache_misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let data = tokio::fs::read(&path)
            .await
            .map_err(|e| Error::other(format!("Read file failed: {e}")))?;

        let bytes = Bytes::from(data);
        self.file_content_cache.insert(path, bytes.clone()).await;

        Ok(bytes)
    }

    // Prefetch related files
    pub async fn prefetch_related(&self, base_path: &Path, patterns: &[&str]) {
        let mut prefetch_tasks = Vec::new();

        for pattern in patterns {
            let path = base_path.join(pattern);
            if tokio::fs::metadata(&path).await.is_ok() {
                let cache = self.clone();
                let path_clone = path.clone();
                prefetch_tasks.push(async move {
                    let _ = cache.get_metadata(path_clone).await;
                });
            }
        }

        // Parallel prefetch, don't wait for completion
        if !prefetch_tasks.is_empty() {
            tokio::spawn(async move {
                futures::future::join_all(prefetch_tasks).await;
            });
        }
    }

    // Batch metadata reading with deduplication
    pub async fn get_metadata_batch(
        &self,
        paths: Vec<PathBuf>,
    ) -> Vec<std::result::Result<Arc<FileMeta>, rustfs_filemeta::Error>> {
        let mut results = Vec::with_capacity(paths.len());
        let mut cache_futures = Vec::new();

        // First, attempt to get from cache
        for (i, path) in paths.iter().enumerate() {
            if let Some(cached) = self.metadata_cache.get(path).await {
                results.push((i, Ok(cached)));
                self.cache_hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            } else {
                cache_futures.push((i, path.clone()));
            }
        }

        // For cache misses, read from filesystem
        if !cache_futures.is_empty() {
            let mut fs_results = Vec::new();

            for (i, path) in cache_futures {
                self.cache_misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                match tokio::fs::read(&path).await {
                    Ok(data) => {
                        let mut meta = FileMeta::default();
                        match meta.unmarshal_msg(&data) {
                            Ok(_) => {
                                let arc_meta = Arc::new(meta);
                                self.metadata_cache.insert(path, arc_meta.clone()).await;
                                fs_results.push((i, Ok(arc_meta)));
                            }
                            Err(e) => {
                                fs_results.push((i, Err(e)));
                            }
                        }
                    }
                    Err(_e) => {
                        fs_results.push((i, Err(rustfs_filemeta::Error::Unexpected)));
                    }
                }
            }

            results.extend(fs_results);
        }

        // Sort results back to original order
        results.sort_by_key(|(i, _)| *i);
        results.into_iter().map(|(_, result)| result).collect()
    }

    // Invalidate cache entries for a path
    pub async fn invalidate(&self, path: &Path) {
        self.metadata_cache.remove(path).await;
        self.file_content_cache.remove(path).await;
    }

    // Get cache statistics
    pub fn get_stats(&self) -> FileCacheStats {
        let hits = self.cache_hits.load(std::sync::atomic::Ordering::Relaxed);
        let misses = self.cache_misses.load(std::sync::atomic::Ordering::Relaxed);
        let hit_rate = if hits + misses > 0 {
            (hits as f64 / (hits + misses) as f64) * 100.0
        } else {
            0.0
        };

        FileCacheStats {
            metadata_cache_size: self.metadata_cache.entry_count(),
            content_cache_size: self.file_content_cache.entry_count(),
            cache_hits: hits,
            cache_misses: misses,
            hit_rate,
            total_weight: 0, // Simplified for compatibility
        }
    }

    // Clear all caches
    pub async fn clear(&self) {
        self.metadata_cache.invalidate_all();
        self.file_content_cache.invalidate_all();

        // Wait for invalidation to complete
        self.metadata_cache.run_pending_tasks().await;
        self.file_content_cache.run_pending_tasks().await;
    }
}

impl Clone for OptimizedFileCache {
    fn clone(&self) -> Self {
        Self {
            metadata_cache: self.metadata_cache.clone(),
            file_content_cache: self.file_content_cache.clone(),
            cache_hits: std::sync::atomic::AtomicU64::new(self.cache_hits.load(std::sync::atomic::Ordering::Relaxed)),
            cache_misses: std::sync::atomic::AtomicU64::new(self.cache_misses.load(std::sync::atomic::Ordering::Relaxed)),
        }
    }
}

#[derive(Debug)]
pub struct FileCacheStats {
    pub metadata_cache_size: u64,
    pub content_cache_size: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub hit_rate: f64,
    pub total_weight: u64,
}

impl Default for OptimizedFileCache {
    fn default() -> Self {
        Self::new()
    }
}

// Global cache instance
use std::sync::OnceLock;

static GLOBAL_FILE_CACHE: OnceLock<OptimizedFileCache> = OnceLock::new();

pub fn get_global_file_cache() -> &'static OptimizedFileCache {
    GLOBAL_FILE_CACHE.get_or_init(OptimizedFileCache::new)
}

// Utility functions for common operations
pub async fn read_metadata_cached(path: PathBuf) -> Result<Arc<FileMeta>> {
    get_global_file_cache().get_metadata(path).await
}

pub async fn read_file_content_cached(path: PathBuf) -> Result<Bytes> {
    get_global_file_cache().get_file_content(path).await
}

pub async fn prefetch_metadata_patterns(base_path: &Path, patterns: &[&str]) {
    get_global_file_cache().prefetch_related(base_path, patterns).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_file_cache_basic() {
        let cache = OptimizedFileCache::new();

        // Create a temporary file
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.txt");
        let mut file = std::fs::File::create(&file_path).unwrap();
        writeln!(file, "test content").unwrap();
        drop(file);

        // First read should be cache miss
        let content1 = cache.get_file_content(file_path.clone()).await.unwrap();
        assert_eq!(content1, Bytes::from("test content\n"));

        // Second read should be cache hit
        let content2 = cache.get_file_content(file_path.clone()).await.unwrap();
        assert_eq!(content2, content1);

        let stats = cache.get_stats();
        assert!(stats.cache_hits > 0);
        assert!(stats.cache_misses > 0);
    }

    #[tokio::test]
    async fn test_metadata_batch_read() {
        let cache = OptimizedFileCache::new();

        // Create test files
        let dir = tempdir().unwrap();
        let mut paths = Vec::new();

        for i in 0..5 {
            let file_path = dir.path().join(format!("test_{i}.txt"));
            let mut file = std::fs::File::create(&file_path).unwrap();
            writeln!(file, "content {i}").unwrap();
            paths.push(file_path);
        }

        // Note: This test would need actual FileMeta files to work properly
        // For now, we just test that the function runs without errors
        let results = cache.get_metadata_batch(paths).await;
        assert_eq!(results.len(), 5);
    }

    #[tokio::test]
    async fn test_cache_invalidation() {
        let cache = OptimizedFileCache::new();

        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.txt");
        let mut file = std::fs::File::create(&file_path).unwrap();
        writeln!(file, "test content").unwrap();
        drop(file);

        // Read file to populate cache
        let _ = cache.get_file_content(file_path.clone()).await.unwrap();

        // Invalidate cache
        cache.invalidate(&file_path).await;

        // Next read should be cache miss again
        let _ = cache.get_file_content(file_path.clone()).await.unwrap();

        let stats = cache.get_stats();
        assert!(stats.cache_misses >= 2);
    }
}
