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

//! Shared types used across the log-cleanup sub-modules.
//!
//! These types deliberately stay lightweight because they are passed between
//! the scanner, selector, compressor, and deletion stages. Keeping them small
//! and explicit makes the cleaner easier to reason about and cheaper to move
//! across worker threads in the parallel pipeline.

use rustfs_config::observability::{
    DEFAULT_OBS_LOG_COMPRESSION_ALGORITHM, DEFAULT_OBS_LOG_COMPRESSION_ALGORITHM_GZIP,
    DEFAULT_OBS_LOG_COMPRESSION_ALGORITHM_ZSTD, DEFAULT_OBS_LOG_GZIP_COMPRESSION_ALL_EXTENSION,
    DEFAULT_OBS_LOG_GZIP_COMPRESSION_EXTENSION, DEFAULT_OBS_LOG_MATCH_MODE, DEFAULT_OBS_LOG_MATCH_MODE_PREFIX,
    DEFAULT_OBS_LOG_ZSTD_COMPRESSION_ALL_EXTENSION, DEFAULT_OBS_LOG_ZSTD_COMPRESSION_EXTENSION,
};
use std::fmt;
use std::path::PathBuf;
use std::time::SystemTime;

/// Strategy for matching log files against a pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileMatchMode {
    /// The filename must start with the pattern (e.g. "app.log." matches "app.log.2024-01-01").
    /// Corresponds to config value "prefix".
    Prefix,
    /// The filename must end with the pattern (e.g. ".log" matches "2024-01-01.log").
    /// Corresponds to config value "suffix".
    Suffix,
}

impl FileMatchMode {
    /// Returns the string representation of the match mode.
    pub fn as_str(&self) -> &'static str {
        match self {
            FileMatchMode::Prefix => DEFAULT_OBS_LOG_MATCH_MODE_PREFIX,
            FileMatchMode::Suffix => DEFAULT_OBS_LOG_MATCH_MODE,
        }
    }

    /// Parse a config value into a [`FileMatchMode`].
    ///
    /// Any non-`prefix` value falls back to [`FileMatchMode::Suffix`] to keep
    /// configuration handling permissive and aligned with the historical
    /// cleaner default used by rolling log filenames.
    pub fn from_config_str(value: &str) -> Self {
        if value.trim().eq_ignore_ascii_case(DEFAULT_OBS_LOG_MATCH_MODE_PREFIX) {
            Self::Prefix
        } else {
            Self::Suffix
        }
    }
}

impl fmt::Display for FileMatchMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Compression algorithm used by the cleaner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionAlgorithm {
    /// Gzip keeps backward compatibility with existing `.gz` archives.
    Gzip,
    /// Zstd provides better ratio and higher decompression throughput.
    Zstd,
}

impl CompressionAlgorithm {
    /// Parse a normalized lowercase configuration token or extension alias.
    fn parse_normalized(value: &str) -> Option<Self> {
        if value == DEFAULT_OBS_LOG_COMPRESSION_ALGORITHM_GZIP || value == DEFAULT_OBS_LOG_GZIP_COMPRESSION_EXTENSION {
            Some(Self::Gzip)
        } else if value == DEFAULT_OBS_LOG_COMPRESSION_ALGORITHM_ZSTD || value == DEFAULT_OBS_LOG_ZSTD_COMPRESSION_EXTENSION {
            Some(Self::Zstd)
        } else {
            None
        }
    }

    /// Parse from a user-facing configuration string.
    ///
    /// Supported values include both semantic names (`gzip`, `zstd`) and file
    /// extension aliases (`gz`, `zst`). Unknown values intentionally fall back
    /// to the crate default so observability startup remains resilient.
    pub fn from_config_str(value: &str) -> Self {
        let normalized = value.trim().to_ascii_lowercase();
        Self::parse_normalized(&normalized).unwrap_or_default()
    }

    /// Archive suffix (without dot) used for this algorithm.
    ///
    /// The returned value is suitable for appending to an existing filename,
    /// rather than replacing the source extension.
    pub fn extension(self) -> &'static str {
        match self {
            Self::Gzip => DEFAULT_OBS_LOG_GZIP_COMPRESSION_ALL_EXTENSION.trim_start_matches('.'),
            Self::Zstd => DEFAULT_OBS_LOG_ZSTD_COMPRESSION_ALL_EXTENSION.trim_start_matches('.'),
        }
    }

    /// Supported compressed suffixes used by scanner retention logic.
    ///
    /// The scanner uses this list to recognize already-archived files and to
    /// keep them on a separate retention path from plain log files.
    pub fn compressed_suffixes() -> [&'static str; 2] {
        [
            DEFAULT_OBS_LOG_GZIP_COMPRESSION_ALL_EXTENSION,
            DEFAULT_OBS_LOG_ZSTD_COMPRESSION_ALL_EXTENSION,
        ]
    }

    /// Stable lowercase string form used in logs and configuration echoes.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Gzip => DEFAULT_OBS_LOG_COMPRESSION_ALGORITHM_GZIP,
            Self::Zstd => DEFAULT_OBS_LOG_COMPRESSION_ALGORITHM_ZSTD,
        }
    }
}

impl std::str::FromStr for CompressionAlgorithm {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let normalized = value.trim().to_ascii_lowercase();
        Self::parse_normalized(&normalized).ok_or("invalid compression algorithm")
    }
}

impl Default for CompressionAlgorithm {
    fn default() -> Self {
        Self::from_config_str(DEFAULT_OBS_LOG_COMPRESSION_ALGORITHM)
    }
}

impl fmt::Display for CompressionAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Worker-thread default used by parallel compressor.
///
/// The worker count follows CPU capacity but stays within [4, 8] to keep
/// throughput stable and avoid oversubscription. The lower bound helps the
/// work-stealing path on small machines still exercise concurrency, while the
/// upper bound avoids swamping the host when each task may also use internal
/// codec threads.
pub fn default_parallel_workers() -> usize {
    num_cpus::get().clamp(4, 8)
}

/// Metadata for a single log file discovered by the scanner.
///
/// This snapshot is intentionally immutable after discovery. The cleaner uses
/// it to sort candidates by age, evaluate retention constraints, and report
/// deletion metrics without re-reading metadata during every later stage.
#[derive(Debug, Clone)]
pub(super) struct FileInfo {
    /// Absolute or scanner-produced path to the file on disk.
    pub path: PathBuf,
    /// File size in bytes at the time of discovery.
    ///
    /// This value is used for retention accounting and freed-byte metrics.
    pub size: u64,
    /// Last-modification timestamp from the filesystem.
    ///
    /// The selection phase sorts on this timestamp so the oldest files are
    /// processed first.
    pub modified: SystemTime,
}

#[cfg(test)]
mod tests {
    use super::CompressionAlgorithm;

    #[test]
    fn compression_algorithm_accepts_full_names_and_aliases() {
        assert_eq!(CompressionAlgorithm::from_config_str("gzip"), CompressionAlgorithm::Gzip);
        assert_eq!(CompressionAlgorithm::from_config_str("GZ"), CompressionAlgorithm::Gzip);
        assert_eq!(CompressionAlgorithm::from_config_str("zstd"), CompressionAlgorithm::Zstd);
        assert_eq!(CompressionAlgorithm::from_config_str(" zst "), CompressionAlgorithm::Zstd);
    }

    #[test]
    fn compression_algorithm_defaults_or_errors_for_invalid_values() {
        assert_eq!(CompressionAlgorithm::from_config_str("brotli"), CompressionAlgorithm::default());
        assert!("brotli".parse::<CompressionAlgorithm>().is_err());
    }
}
