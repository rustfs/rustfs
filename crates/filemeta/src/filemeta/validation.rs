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

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_utils::http::SUFFIX_INLINE_DATA;

    #[test]
    fn validation_and_stats_cover_incompat_unsorted_and_header_invariants() {
        let t1 = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("t1");
        let t2 = OffsetDateTime::from_unix_timestamp(1_700_000_010).expect("t2");

        let mut fm = FileMeta::new();
        assert!(fm.is_compatible_with_meta());
        fm.meta_ver = XL_META_VERSION.wrapping_add(1);
        assert!(!fm.is_compatible_with_meta());
        fm.meta_ver = XL_META_VERSION;

        // Unsorted (older before newer) must fail validate_integrity, while newest-first succeeds.
        fm.versions = vec![
            FileMetaShallowVersion {
                header: FileMetaVersionHeader {
                    version_type: VersionType::Object,
                    mod_time: Some(t1),
                    ..Default::default()
                },
                meta: Vec::new(),
            },
            FileMetaShallowVersion {
                header: FileMetaVersionHeader {
                    version_type: VersionType::Object,
                    mod_time: Some(t2),
                    ..Default::default()
                },
                meta: Vec::new(),
            },
        ];
        assert!(!fm.is_sorted_by_mod_time());
        assert!(fm.validate_integrity().is_err());

        fm.versions.reverse();
        assert!(fm.is_sorted_by_mod_time());
        fm.validate_integrity().expect("sorted newest-first metadata must pass");

        // (None, Some(_)) in mod_time must also be rejected as unsorted.
        fm.versions[0].header.mod_time = None;
        assert!(!fm.is_sorted_by_mod_time());
        assert!(fm.validate_integrity().is_err());

        // Header validation: valid baseline vs deterministic far-future timestamp and ec_m < ec_n.
        let valid_hdr = FileMetaVersionHeader {
            version_type: VersionType::Object,
            mod_time: Some(t2),
            ec_m: 2,
            ec_n: 2,
            ..Default::default()
        };
        assert!(valid_hdr.is_valid(), "valid header with ec_m == ec_n must pass");

        let future_hdr = FileMetaVersionHeader {
            version_type: VersionType::Object,
            mod_time: Some(OffsetDateTime::now_utc() + time::Duration::days(3650)),
            ec_m: 2,
            ec_n: 1,
            ..Default::default()
        };
        assert!(!future_hdr.is_valid(), "far-future mod_time must be invalid");

        let bad_ec_hdr = FileMetaVersionHeader {
            version_type: VersionType::Object,
            mod_time: Some(t2),
            ec_m: 1,
            ec_n: 2, // ec_m < ec_n when has_ec() is true
            ..Default::default()
        };
        assert!(!bad_ec_hdr.is_valid(), "ec_m < ec_n must be invalid");

        // Detailed stats: data_dir, inline_data, free_version, Legacy, and Invalid.
        let mut obj_with_ddir = MetaObject {
            version_id: Some(Uuid::from_u128(1)),
            data_dir: Some(Uuid::from_u128(2)),
            erasure_algorithm: ErasureAlgo::ReedSolomon,
            bitrot_checksum_algo: ChecksumAlgo::HighwayHash,
            size: 100,
            mod_time: Some(t2),
            ..Default::default()
        };
        obj_with_ddir.meta_user.insert(
            metadata_keys::RESTORE.to_string(),
            "ongoing-request=\"false\", expiry-date=\"9999-01-01T00:00:00Z\"".to_string(),
        );
        let ver_ddir = FileMetaShallowVersion::try_from(FileMetaVersion {
            version_type: VersionType::Object,
            object: Some(obj_with_ddir),
            ..Default::default()
        })
        .expect("shallow version with data_dir");

        let mut obj_inline = MetaObject {
            version_id: Some(Uuid::from_u128(3)),
            erasure_algorithm: ErasureAlgo::ReedSolomon,
            bitrot_checksum_algo: ChecksumAlgo::HighwayHash,
            size: 25,
            mod_time: Some(t1),
            ..Default::default()
        };
        insert_bytes(&mut obj_inline.meta_sys, SUFFIX_INLINE_DATA, b"true".to_vec());
        let ver_inline = FileMetaShallowVersion::try_from(FileMetaVersion {
            version_type: VersionType::Object,
            object: Some(obj_inline),
            ..Default::default()
        })
        .expect("shallow version with inline data");

        let free_del = FileMetaShallowVersion {
            header: FileMetaVersionHeader {
                version_type: VersionType::Delete,
                mod_time: Some(t1),
                flags: Flags::FreeVersion as u8,
                ..Default::default()
            },
            meta: Vec::new(),
        };
        let legacy_ver = FileMetaShallowVersion {
            header: FileMetaVersionHeader {
                version_type: VersionType::Legacy,
                mod_time: Some(t1),
                ..Default::default()
            },
            meta: Vec::new(),
        };
        let invalid_ver = FileMetaShallowVersion {
            header: FileMetaVersionHeader {
                version_type: VersionType::Invalid,
                mod_time: None,
                ..Default::default()
            },
            meta: Vec::new(),
        };

        // Place `ver_inline` (t1) before `ver_ddir` (t2) so `get_detailed_version_stats`
        // exercises both the initial `latest_mod_time.is_none()` assignment and the
        // subsequent `version.header.mod_time > stats.latest_mod_time` update.
        let stats_fm = FileMeta {
            versions: vec![ver_inline, ver_ddir, free_del, legacy_ver, invalid_ver],
            meta_ver: XL_META_VERSION,
            data: InlineData::new(),
        };
        let basic = stats_fm.get_version_stats();
        assert_eq!(basic.total_versions, 5);
        assert_eq!(basic.object_versions, 2);
        assert_eq!(basic.delete_markers, 1);
        assert_eq!(basic.invalid_versions, 2);
        assert_eq!(basic.free_versions, 1);

        let detailed = stats_fm.get_detailed_version_stats();
        assert_eq!(detailed.total_versions, 5);
        assert_eq!(detailed.object_versions, 2);
        assert_eq!(detailed.delete_markers, 1);
        assert_eq!(detailed.legacy_versions, 1);
        assert_eq!(detailed.invalid_versions, 1);
        assert_eq!(detailed.free_versions, 1);
        assert_eq!(detailed.versions_with_data_dir, 1);
        assert_eq!(detailed.versions_with_inline_data, 1);
        assert_eq!(detailed.total_size, 125);
        assert_eq!(detailed.latest_mod_time, Some(t2));
    }
}
