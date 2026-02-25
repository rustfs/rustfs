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

use super::*;

impl FileMeta {
    /// Check if the metadata format is compatible
    pub fn is_compatible_with_meta(&self) -> bool {
        // Check version compatibility
        if self.meta_ver != XL_META_VERSION {
            return false;
        }

        // For compatibility, we allow versions with different types
        // Just check basic structure validity
        true
    }

    /// Validate metadata integrity
    pub fn validate_integrity(&self) -> Result<()> {
        // Check if versions are sorted by modification time
        if !self.is_sorted_by_mod_time() {
            return Err(Error::other("versions not sorted by modification time"));
        }

        // Validate inline data if present
        self.data.validate()?;

        Ok(())
    }

    /// Check if versions are sorted by modification time (newest first)
    pub(crate) fn is_sorted_by_mod_time(&self) -> bool {
        if self.versions.len() <= 1 {
            return true;
        }

        for i in 1..self.versions.len() {
            let prev_time = self.versions[i - 1].header.mod_time;
            let curr_time = self.versions[i].header.mod_time;

            match (prev_time, curr_time) {
                (Some(prev), Some(curr)) => {
                    if prev < curr {
                        return false;
                    }
                }
                (None, Some(_)) => return false,
                _ => continue,
            }
        }

        true
    }

    /// Get statistics about versions
    pub fn get_version_stats(&self) -> VersionStats {
        let mut stats = VersionStats {
            total_versions: self.versions.len(),
            ..Default::default()
        };

        for version in &self.versions {
            match version.header.version_type {
                VersionType::Object => stats.object_versions += 1,
                VersionType::Delete => stats.delete_markers += 1,
                VersionType::Invalid | VersionType::Legacy => stats.invalid_versions += 1,
            }

            if version.header.free_version() {
                stats.free_versions += 1;
            }
        }

        stats
    }
}

#[derive(Debug, Default, Clone)]
pub struct VersionStats {
    pub total_versions: usize,
    pub object_versions: usize,
    pub delete_markers: usize,
    pub invalid_versions: usize,
    pub free_versions: usize,
}

impl FileMetaVersionHeader {
    // ... existing code ...

    pub fn is_valid(&self) -> bool {
        // Check if version type is valid
        if !self.version_type.valid() {
            return false;
        }

        // Check if modification time is reasonable (not too far in the future)
        if let Some(mod_time) = self.mod_time {
            let now = OffsetDateTime::now_utc();
            let future_limit = now + time::Duration::hours(24); // Allow 24 hours in future
            if mod_time > future_limit {
                return false;
            }
        }

        // Check erasure coding parameters
        if self.has_ec() && (self.ec_n == 0 || self.ec_m == 0 || self.ec_m < self.ec_n) {
            return false;
        }

        true
    }

    // ... existing code ...
}

/// Enhanced version statistics with more detailed information
#[derive(Debug, Default, Clone)]
pub struct DetailedVersionStats {
    pub total_versions: usize,
    pub object_versions: usize,
    pub delete_markers: usize,
    pub invalid_versions: usize,
    pub legacy_versions: usize,
    pub free_versions: usize,
    pub versions_with_data_dir: usize,
    pub versions_with_inline_data: usize,
    pub total_size: i64,
    pub latest_mod_time: Option<OffsetDateTime>,
}

impl FileMeta {
    /// Get detailed statistics about versions
    pub fn get_detailed_version_stats(&self) -> DetailedVersionStats {
        let mut stats = DetailedVersionStats {
            total_versions: self.versions.len(),
            ..Default::default()
        };

        for version in &self.versions {
            match version.header.version_type {
                VersionType::Object => {
                    stats.object_versions += 1;
                    if let Ok(ver) = FileMetaVersion::try_from(version.meta.as_slice())
                        && let Some(obj) = &ver.object
                    {
                        stats.total_size += obj.size;
                        if obj.uses_data_dir() {
                            stats.versions_with_data_dir += 1;
                        }
                        if obj.inlinedata() {
                            stats.versions_with_inline_data += 1;
                        }
                    }
                }
                VersionType::Delete => stats.delete_markers += 1,
                VersionType::Legacy => stats.legacy_versions += 1,
                VersionType::Invalid => stats.invalid_versions += 1,
            }

            if version.header.free_version() {
                stats.free_versions += 1;
            }

            if stats.latest_mod_time.is_none()
                || (version.header.mod_time.is_some() && version.header.mod_time > stats.latest_mod_time)
            {
                stats.latest_mod_time = version.header.mod_time;
            }
        }

        stats
    }
}
