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

//! Disk usage and the capacity policy the evictor applies.

use std::io;
use std::path::Path;

/// Space on the filesystem holding a drive directory. Everything on the
/// filesystem counts, not only the cache's own files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskUsage {
    pub total_bytes: u64,
    pub available_bytes: u64,
}

/// Reads disk usage; a trait so the evictor can be tested without filling
/// disks.
pub trait FsStats: Send + Sync {
    fn usage(&self, dir: &Path) -> io::Result<DiskUsage>;
}

/// `statvfs` of the filesystem holding the directory.
#[derive(Debug, Default, Clone, Copy)]
pub struct StatvfsFs;

impl FsStats for StatvfsFs {
    fn usage(&self, dir: &Path) -> io::Result<DiskUsage> {
        let stats = rustix::fs::statvfs(dir)?;
        Ok(DiskUsage {
            total_bytes: stats.f_blocks.saturating_mul(stats.f_frsize),
            available_bytes: stats.f_bavail.saturating_mul(stats.f_frsize),
        })
    }
}

/// Usage fractions of a drive's filesystem.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Watermarks {
    /// Start evicting at or above this usage.
    pub high: f64,
    /// Evict until usage is below this.
    pub low: f64,
    /// Refuse new entries at or above this usage.
    pub emergency: f64,
}

impl Default for Watermarks {
    fn default() -> Self {
        Self {
            high: 0.80,
            low: 0.70,
            emergency: 0.90,
        }
    }
}

impl Watermarks {
    /// `0 < low < high <= emergency <= 1`.
    pub fn validate(&self) -> Result<(), String> {
        let ordered = 0.0 < self.low && self.low < self.high && self.high <= self.emergency && self.emergency <= 1.0;
        if ordered {
            Ok(())
        } else {
            Err(format!(
                "watermarks must satisfy 0 < low < high <= emergency <= 1, got low {} high {} emergency {}",
                self.low, self.high, self.emergency
            ))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct CapacityPolicy {
    pub watermarks: Watermarks,
    /// Drop segments whose newest entry is older than this, even with space
    /// left. `None` keeps entries until capacity requires dropping them.
    pub max_age_ms: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watermarks_must_be_ordered() {
        assert!(Watermarks::default().validate().is_ok());
        for (low, high, emergency) in [(0.0, 0.8, 0.9), (0.8, 0.7, 0.9), (0.7, 0.95, 0.9), (0.7, 0.8, 1.1)] {
            assert!(Watermarks { high, low, emergency }.validate().is_err(), "{low} {high} {emergency}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn statvfs_reports_the_filesystem_of_a_directory() {
        let dir = tempfile::tempdir().expect("tempdir");
        let usage = StatvfsFs.usage(dir.path()).expect("statvfs");
        assert!(usage.total_bytes > 0);
        assert!(usage.available_bytes <= usage.total_bytes);
    }
}
