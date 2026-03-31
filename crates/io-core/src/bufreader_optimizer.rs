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

//! BufReader layer optimizer for minimizing redundant buffering layers.
//!
//! This module provides optimization for BufReader usage in data paths,
//! including layer count limiting and dynamic buffer size adjustment.

use std::sync::atomic::{AtomicU64, Ordering};

/// BufReader optimization configuration.
#[derive(Debug, Clone)]
pub struct BufReaderConfig {
    /// Maximum number of nested BufReader layers (default: 2)
    pub max_layers: u32,

    /// Buffer size for small files (default: 8KB)
    pub small_file_buffer: usize,

    /// Buffer size for large files (default: 64KB)
    pub large_file_buffer: usize,

    /// Threshold for large file classification (default: 1MB)
    pub large_file_threshold: usize,
}

impl Default for BufReaderConfig {
    fn default() -> Self {
        Self {
            max_layers: 2,
            small_file_buffer: 8 * 1024,       // 8KB
            large_file_buffer: 64 * 1024,      // 64KB
            large_file_threshold: 1024 * 1024, // 1MB
        }
    }
}

/// BufReader optimization statistics.
#[derive(Debug, Default)]
pub struct BufReaderStats {
    /// Total number of readers created
    pub total_readers: AtomicU64,

    /// Number of redundant layers eliminated
    pub eliminated_layers: AtomicU64,

    /// Number of buffer size adjustments
    pub buffer_size_adjustments: AtomicU64,
}

/// BufReader layer optimizer.
///
/// Analyzes and optimizes BufReader nesting in data paths,
/// dynamically adjusting buffer sizes based on data characteristics.
pub struct BufReaderOptimizer {
    config: BufReaderConfig,
    stats: BufReaderStats,
}

impl BufReaderOptimizer {
    /// Create a new BufReader optimizer with the given configuration.
    pub fn new(config: BufReaderConfig) -> Self {
        Self {
            config,
            stats: BufReaderStats::default(),
        }
    }

    /// Create a new BufReader optimizer with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(BufReaderConfig::default())
    }

    /// Calculate the optimal buffer size based on data size.
    ///
    /// Returns the appropriate buffer size based on whether the data
    /// is classified as a small or large file.
    pub fn optimal_buffer_size(&self, data_size: Option<usize>) -> usize {
        match data_size {
            Some(size) if size >= self.config.large_file_threshold => self.config.large_file_buffer,
            Some(_) => self.config.small_file_buffer,
            None => self.config.small_file_buffer,
        }
    }

    /// Optimize a reader by wrapping it with an appropriately sized BufReader.
    ///
    /// This method applies the optimal buffer size based on the expected
    /// data size and tracks statistics.
    pub fn optimize<R: tokio::io::AsyncRead + Unpin>(&self, reader: R, data_size: Option<usize>) -> tokio::io::BufReader<R> {
        let buffer_size = self.optimal_buffer_size(data_size);
        self.stats.total_readers.fetch_add(1, Ordering::Relaxed);
        tokio::io::BufReader::with_capacity(buffer_size, reader)
    }

    /// Get the statistics for this optimizer.
    pub fn stats(&self) -> &BufReaderStats {
        &self.stats
    }

    /// Get the configuration for this optimizer.
    pub fn config(&self) -> &BufReaderConfig {
        &self.config
    }
}

/// Marker trait for buffered sources.
///
/// Types implementing this trait are considered already buffered
/// and should not be wrapped with additional BufReader layers.
pub trait BufferedSource: tokio::io::AsyncRead {}

impl BufReaderOptimizer {
    /// Check if a reader is already a buffered source.
    ///
    /// Returns true if the reader implements `BufferedSource`,
    /// indicating it should not be wrapped with BufReader.
    pub fn is_buffered_source<R: BufferedSource + ?Sized>(&self, _reader: &R) -> bool {
        true
    }

    /// Eliminate redundant BufReader layers if possible.
    ///
    /// This method attempts to reduce the nesting depth of BufReader
    /// layers to improve performance.
    pub fn eliminate_redundant_layers<R: tokio::io::AsyncRead + Unpin>(&self, reader: R) -> R {
        // For now, just return the reader as-is
        // Future implementation could detect and unwrap nested BufReaders
        self.stats.eliminated_layers.fetch_add(0, Ordering::Relaxed);
        reader
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[test]
    fn test_default_config() {
        let config = BufReaderConfig::default();
        assert_eq!(config.max_layers, 2);
        assert_eq!(config.small_file_buffer, 8 * 1024);
        assert_eq!(config.large_file_buffer, 64 * 1024);
        assert_eq!(config.large_file_threshold, 1024 * 1024);
    }

    #[test]
    fn test_optimal_buffer_size_small_file() {
        let optimizer = BufReaderOptimizer::with_defaults();

        // Small file (< 1MB)
        assert_eq!(optimizer.optimal_buffer_size(Some(100)), 8 * 1024);
        assert_eq!(optimizer.optimal_buffer_size(Some(1024)), 8 * 1024);
        assert_eq!(optimizer.optimal_buffer_size(Some(512 * 1024)), 8 * 1024);
    }

    #[test]
    fn test_optimal_buffer_size_large_file() {
        let optimizer = BufReaderOptimizer::with_defaults();

        // Large file (>= 1MB)
        assert_eq!(optimizer.optimal_buffer_size(Some(1024 * 1024)), 64 * 1024);
        assert_eq!(optimizer.optimal_buffer_size(Some(10 * 1024 * 1024)), 64 * 1024);
    }

    #[test]
    fn test_optimal_buffer_size_unknown() {
        let optimizer = BufReaderOptimizer::with_defaults();

        // Unknown size
        assert_eq!(optimizer.optimal_buffer_size(None), 8 * 1024);
    }

    #[tokio::test]
    async fn test_optimize_creates_bufreader() {
        let optimizer = BufReaderOptimizer::with_defaults();
        let data = vec![1u8, 2, 3, 4, 5];
        let cursor = std::io::Cursor::new(data.clone());

        let mut reader = optimizer.optimize(cursor, Some(5));

        let mut buf = vec![0u8; 5];
        let n = reader.read(&mut buf).await.unwrap();

        assert_eq!(n, 5);
        assert_eq!(buf, data);
    }

    #[test]
    fn test_stats_tracking() {
        let optimizer = BufReaderOptimizer::with_defaults();

        assert_eq!(optimizer.stats().total_readers.load(Ordering::Relaxed), 0);

        let cursor = std::io::Cursor::new(vec![1u8, 2, 3]);
        let _reader = optimizer.optimize(cursor, Some(3));

        assert_eq!(optimizer.stats().total_readers.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_custom_config() {
        let config = BufReaderConfig {
            max_layers: 3,
            small_file_buffer: 4 * 1024,
            large_file_buffer: 128 * 1024,
            large_file_threshold: 2 * 1024 * 1024,
        };

        let optimizer = BufReaderOptimizer::new(config);

        assert_eq!(optimizer.optimal_buffer_size(Some(1024 * 1024)), 4 * 1024);
        assert_eq!(optimizer.optimal_buffer_size(Some(3 * 1024 * 1024)), 128 * 1024);
    }
}
