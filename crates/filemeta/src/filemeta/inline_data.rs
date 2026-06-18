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
use rustfs_utils::http::{SUFFIX_TRANSITION_STATUS, get_bytes};
use std::collections::HashSet;

fn physical_data_dir(version: &FileMetaShallowVersion) -> Result<Option<Uuid>> {
    if version.header.version_type != VersionType::Object {
        return Ok(None);
    }

    let version = FileMetaVersion::try_from(version.meta.as_slice())?;
    let Some(obj) = version.object else {
        return Ok(None);
    };

    if obj.inlinedata() {
        return Ok(None);
    }

    if let Some(status) = get_bytes(&obj.meta_sys, SUFFIX_TRANSITION_STATUS)
        && status.as_slice() == TRANSITION_COMPLETE.as_bytes()
        && !is_restored_object_on_disk(&obj.meta_user)
    {
        return Ok(None);
    }

    Ok(obj.data_dir.filter(|dir| !dir.is_nil()))
}

impl FileMeta {
    pub fn find_unshared_data_dir_for_version(&self, version_id: Option<Uuid>) -> Option<Uuid> {
        let vid = version_id.unwrap_or_default();
        let mut target_data_dir = None;
        let mut target_selected = false;
        let mut other_data_dirs = HashSet::new();

        for version in self.versions.iter().filter(|v| v.header.version_type == VersionType::Object) {
            let is_target_version = version.header.version_id.unwrap_or_default() == vid;
            let dir = match physical_data_dir(version) {
                Ok(dir) => dir,
                Err(_) => return None,
            };

            if is_target_version {
                if target_selected {
                    continue;
                }

                target_selected = true;
                target_data_dir = dir;
                if let Some(dir) = target_data_dir
                    && other_data_dirs.contains(&dir)
                {
                    return None;
                }
                continue;
            }

            if let Some(dir) = dir {
                if target_data_dir == Some(dir) {
                    return None;
                }
                other_data_dirs.insert(dir);
            }
        }

        target_data_dir
    }

    pub fn shard_data_dir_count(&self, vid: &Option<Uuid>, data_dir: &Option<Uuid>) -> usize {
        let vid = vid.unwrap_or_default();
        let Some(data_dir) = data_dir else {
            return 0;
        };

        let mut count = 0;
        for version in self
            .versions
            .iter()
            .filter(|v| v.header.version_type == VersionType::Object && v.header.version_id != Some(vid))
        {
            match physical_data_dir(version) {
                Ok(Some(dir)) if dir == *data_dir => count += 1,
                Ok(_) => {}
                Err(_) => return 1,
            }
        }
        count
    }

    pub fn get_data_dirs(&self) -> Result<Vec<Option<Uuid>>> {
        let mut data_dirs = Vec::new();
        for version in &self.versions {
            if version.header.version_type == VersionType::Object {
                let ver = FileMetaVersion::try_from(version.meta.as_slice())?;
                data_dirs.push(ver.get_data_dir());
            }
        }
        Ok(data_dirs)
    }

    /// Count shared data directories
    pub fn shared_data_dir_count(&self, version_id: Option<Uuid>, data_dir: Option<Uuid>) -> usize {
        let vid = version_id.unwrap_or_default();
        let Some(data_dir) = data_dir else {
            return 0;
        };

        if self.data.entries().unwrap_or_default() > 0
            && self.find_inline_data_for_version(version_id).unwrap_or_default().is_some()
        {
            return 0;
        }

        let mut count = 0;
        for version in self
            .versions
            .iter()
            .filter(|v| v.header.version_type == VersionType::Object && v.header.version_id != Some(vid))
        {
            match physical_data_dir(version) {
                Ok(Some(dir)) if dir == data_dir => count += 1,
                Ok(_) => {}
                Err(_) => return 1,
            }
        }
        count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use s3s::header::X_AMZ_RESTORE;
    use time::format_description::well_known::Rfc3339;
    use time::{Duration, OffsetDateTime};

    fn make_plain_file_info(version_id: Uuid, data_dir: Uuid) -> FileInfo {
        make_file_info_with_metadata(version_id, data_dir, HashMap::from([("etag".to_string(), format!("etag-{version_id}"))]))
    }

    fn make_plain_null_version_file_info(data_dir: Uuid) -> FileInfo {
        let mut fi = make_plain_file_info(Uuid::nil(), data_dir);
        fi.version_id = None;
        fi
    }

    fn make_file_info(version_id: Uuid, data_dir: Uuid) -> FileInfo {
        let restore_header = format!(
            "ongoing-request=\"false\", expiry-date=\"{}\"",
            (OffsetDateTime::now_utc() + Duration::days(1))
                .format(&Rfc3339)
                .expect("format restore expiry"),
        );
        make_file_info_with_metadata(
            version_id,
            data_dir,
            HashMap::from([
                ("etag".to_string(), format!("etag-{version_id}")),
                (X_AMZ_RESTORE.as_str().to_string(), restore_header),
            ]),
        )
    }

    fn make_file_info_with_metadata(version_id: Uuid, data_dir: Uuid, metadata: HashMap<String, String>) -> FileInfo {
        FileInfo {
            version_id: Some(version_id),
            data_dir: Some(data_dir),
            size: 64 * 1024,
            mod_time: Some(OffsetDateTime::now_utc()),
            metadata,
            erasure: ErasureInfo {
                algorithm: ErasureAlgo::ReedSolomon.to_string(),
                data_blocks: 4,
                parity_blocks: 2,
                block_size: 1024 * 1024,
                index: 1,
                distribution: vec![1, 2, 3, 4, 5, 6],
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn find_unshared_data_dir_for_version_returns_data_dir_when_unique() {
        let target_version = Uuid::new_v4();
        let target_data_dir = Uuid::new_v4();
        let mut meta = FileMeta::new();
        meta.add_version(make_file_info(target_version, target_data_dir))
            .expect("seed target version");
        meta.add_version(make_file_info(Uuid::new_v4(), Uuid::new_v4()))
            .expect("seed non-shared version");

        let got = meta.find_unshared_data_dir_for_version(Some(target_version));
        assert_eq!(got, Some(target_data_dir));
    }

    #[test]
    fn find_unshared_data_dir_for_version_returns_none_when_shared() {
        let target_version = Uuid::new_v4();
        let shared_data_dir = Uuid::new_v4();
        let mut meta = FileMeta::new();
        meta.add_version(make_file_info(target_version, shared_data_dir))
            .expect("seed target version");
        meta.add_version(make_file_info(Uuid::new_v4(), shared_data_dir))
            .expect("seed shared version");

        let got = meta.find_unshared_data_dir_for_version(Some(target_version));
        assert_eq!(got, None);
    }

    #[test]
    fn find_unshared_data_dir_for_version_uses_data_dir_when_header_flag_is_false() {
        let target_version = Uuid::new_v4();
        let target_data_dir = Uuid::new_v4();
        let mut meta = FileMeta::new();
        meta.add_version(make_plain_file_info(target_version, target_data_dir))
            .expect("seed target version");
        meta.add_version(make_plain_file_info(Uuid::new_v4(), Uuid::new_v4()))
            .expect("seed non-shared version");

        let target = meta
            .versions
            .iter()
            .find(|version| version.header.version_id == Some(target_version))
            .expect("target version header");
        assert!(!target.header.uses_data_dir());

        let got = meta.find_unshared_data_dir_for_version(Some(target_version));
        assert_eq!(got, Some(target_data_dir));
    }

    #[test]
    fn find_unshared_data_dir_for_null_version_uses_data_dir_when_header_flag_is_false() {
        let target_data_dir = Uuid::new_v4();
        let mut meta = FileMeta::new();
        meta.add_version(make_plain_null_version_file_info(target_data_dir))
            .expect("seed null version");

        let target = meta
            .versions
            .iter()
            .find(|version| version.header.version_id == Some(Uuid::nil()))
            .expect("null version header");
        assert!(!target.header.uses_data_dir());

        let got = meta.find_unshared_data_dir_for_version(None);
        assert_eq!(got, Some(target_data_dir));
    }

    #[test]
    fn shared_data_dir_count_uses_data_dir_when_header_flag_is_false() {
        let target_version = Uuid::new_v4();
        let other_version = Uuid::new_v4();
        let shared_data_dir = Uuid::new_v4();
        let mut meta = FileMeta::new();
        meta.add_version(make_plain_file_info(target_version, shared_data_dir))
            .expect("seed target version");
        meta.add_version(make_plain_file_info(other_version, shared_data_dir))
            .expect("seed shared version");

        assert!(meta.versions.iter().all(|version| !version.header.uses_data_dir()));
        assert_eq!(meta.shared_data_dir_count(Some(target_version), Some(shared_data_dir)), 1);

        let old_dir = meta
            .delete_version(&FileInfo {
                version_id: Some(target_version),
                ..Default::default()
            })
            .expect("delete target version");
        assert_eq!(old_dir, None);
    }

    #[test]
    fn find_unshared_data_dir_for_version_skips_remote_transitioned_data_dir() {
        let target_version = Uuid::new_v4();
        let target_data_dir = Uuid::new_v4();
        let mut fi = make_plain_file_info(target_version, target_data_dir);
        fi.transition_status = TRANSITION_COMPLETE.to_string();

        let mut meta = FileMeta::new();
        meta.add_version(fi).expect("seed remote transitioned version");

        let got = meta.find_unshared_data_dir_for_version(Some(target_version));
        assert_eq!(got, None);
    }
}
