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
        self.shared_data_dir_count_with_inline_data(version_id, data_dir, None)
    }

    pub(super) fn shared_data_dir_count_with_inline_data(
        &self,
        version_id: Option<Uuid>,
        data_dir: Option<Uuid>,
        has_inline_data: Option<bool>,
    ) -> usize {
        let vid = version_id.unwrap_or_default();
        let Some(data_dir) = data_dir else {
            return 0;
        };

        if has_inline_data.unwrap_or_else(|| {
            self.data.entries().unwrap_or_default() > 0
                && self.find_inline_data_for_version(version_id).unwrap_or_default().is_some()
        }) {
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
                (metadata_keys::RESTORE.to_string(), restore_header),
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

    fn sequential_delete(meta: &mut FileMeta, requests: &[FileInfo]) -> Result<Vec<Option<Uuid>>> {
        let mut dirs = Vec::new();
        for request in requests {
            match meta.delete_version(request) {
                Ok(dir) => {
                    if let Some(dir) = dir {
                        meta.data.remove(vec![request.version_id.unwrap_or_default(), dir])?;
                    }
                    dirs.push(dir);
                }
                Err(Error::FileNotFound | Error::FileVersionNotFound) if !request.deleted => dirs.push(None),
                Err(err) => return Err(err),
            }
        }
        Ok(dirs)
    }

    fn assert_batch_matches_sequential(original: &FileMeta, requests: &[FileInfo]) {
        let mut expected = original.clone();
        let expected_dirs = sequential_delete(&mut expected, requests);
        let mut actual = original.clone();
        assert_eq!(actual.delete_versions(requests), expected_dirs);
        let decode = |meta: &FileMeta| {
            meta.versions
                .iter()
                .map(|version| version.parse_version_meta().expect("decode retained version"))
                .collect::<Vec<_>>()
        };
        assert_eq!(decode(&actual), decode(&expected));
        assert_eq!(actual.data, expected.data, "retained inline bytes and ordering must be identical");
    }

    #[test]
    fn delete_versions_matches_sequential_mixed_version_subsets() {
        let mut original = FileMeta::new();
        let mut requests = Vec::new();
        for id in 0_u128..8 {
            let version = Uuid::from_u128(id);
            // Versions 2 and 3 share physical data. Version 4's data-dir key
            // aliases version 5's inline key, so later decisions must see its removal.
            let dir = Uuid::from_u128(match id {
                2 | 3 | 5 => 100,
                4 => 5,
                _ => 200 + id,
            });
            let mut fi = make_plain_file_info(version, dir);
            fi.mod_time =
                Some(OffsetDateTime::UNIX_EPOCH + Duration::seconds(1_700_000_000 + i64::try_from(id).expect("small id")));
            if !matches!(id, 2 | 3) {
                fi.data = Some(Bytes::from(vec![u8::try_from(id).expect("small id"); 97]));
                fi.set_inline_data();
            }
            original.add_version(fi).expect("seed mixed version chain");
            requests.push(FileInfo {
                version_id: Some(version),
                ..Default::default()
            });
        }
        for mask in 0_u32..256 {
            let selected: Vec<_> = requests
                .iter()
                .enumerate()
                .filter(|(i, _)| mask & (1 << i) != 0)
                .map(|(_, fi)| fi.clone())
                .collect();
            assert_batch_matches_sequential(&original, &selected);
            let reversed: Vec<_> = selected.into_iter().rev().collect();
            assert_batch_matches_sequential(&original, &reversed);
        }
        let mut duplicate_and_missing = requests.clone();
        duplicate_and_missing.extend(requests.iter().rev().cloned());
        duplicate_and_missing.push(FileInfo {
            version_id: Some(Uuid::from_u128(999)),
            ..Default::default()
        });
        assert_batch_matches_sequential(&original, &duplicate_and_missing);
    }

    #[test]
    fn delete_versions_preserves_pending_purge_and_stops_at_invalid_request() {
        let mut original = FileMeta::new();
        let mut fi = make_plain_file_info(Uuid::from_u128(1), Uuid::from_u128(101));
        fi.data = Some(Bytes::from_static(b"retained until replication completes"));
        fi.set_inline_data();
        original.add_version(fi).expect("seed inline version");
        let pending = FileInfo {
            version_id: Some(Uuid::from_u128(1)),
            mark_deleted: true,
            replication_state_internal: Some(ReplicationState {
                version_purge_status_internal: Some("target=PENDING;".into()),
                purge_targets: version_purge_statuses_map("target=PENDING;"),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_batch_matches_sequential(&original, std::slice::from_ref(&pending));
        let completed = FileInfo {
            version_id: pending.version_id,
            replication_state_internal: Some(ReplicationState {
                version_purge_status_internal: Some("target=COMPLETE;".into()),
                purge_targets: version_purge_statuses_map("target=COMPLETE;"),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_batch_matches_sequential(&original, &[pending, completed]);
        let valid = FileInfo {
            version_id: Some(Uuid::from_u128(1)),
            ..Default::default()
        };
        let invalid = FileInfo {
            parts: vec![
                ObjectPartInfo {
                    number: 1,
                    ..Default::default()
                };
                2
            ],
            ..Default::default()
        };
        assert!(invalid.validate_for_delete_operation().is_err());
        assert_batch_matches_sequential(&original, &[valid, invalid]);
    }

    #[test]
    fn delete_versions_rejects_corrupt_inline_before_mutating_versions() {
        let mut meta = FileMeta::new();
        meta.add_version(make_plain_file_info(Uuid::from_u128(1), Uuid::from_u128(101)))
            .expect("seed version");
        for bytes in [
            vec![1, 0x81, 0xa1, b'x', 0xc4, 255],
            vec![2, 0x80],
            vec![0, 0x80],
            vec![1, 0x81, 0xa1, 0xff, 0xc4, 0],
            vec![1, 0x81, 0xdb, 1, 0, 0, 1],
            vec![1, 0x81, 0xa1, b'x', 0xc6, 0xff, 0xff, 0xff, 0xff],
        ] {
            meta.data.update(&bytes);
            let before = meta.clone();
            assert!(
                meta.delete_versions(&[])
                    .expect("empty batch does not inspect metadata")
                    .is_empty()
            );
            assert!(
                meta.delete_versions(&[FileInfo {
                    version_id: Some(Uuid::from_u128(1)),
                    ..Default::default()
                }])
                .is_err()
            );
            assert_eq!(meta, before);
        }
    }

    #[test]
    fn delete_versions_preserves_both_null_inline_key_encodings() {
        for legacy in [false, true] {
            let mut original = FileMeta::new();
            let mut fi = make_plain_null_version_file_info(Uuid::from_u128(100));
            fi.data = Some(Bytes::from_static(b"null version body"));
            fi.set_inline_data();
            original.add_version(fi).expect("seed null version");
            if legacy {
                let value = original
                    .data
                    .find(NULL_VERSION_ID)
                    .expect("find null body")
                    .expect("null body");
                original.data.remove_key(NULL_VERSION_ID).expect("remove modern null key");
                original
                    .data
                    .replace(&Uuid::nil().to_string(), value)
                    .expect("write legacy null key");
            }
            for version_id in [None, Some(Uuid::nil())] {
                assert_batch_matches_sequential(
                    &original,
                    &[FileInfo {
                        version_id,
                        ..Default::default()
                    }],
                );
            }
        }
    }
}
