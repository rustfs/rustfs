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

fn physical_data_dir(version: &FileMetaShallowVersion) -> Result<Option<Uuid>> {
    if version.header.version_type != VersionType::Object {
        return Ok(None);
    }

    if let Ok(summary) = FileMetaVersion::inspect_v2_object_slice(&version.meta) {
        return Ok(summary.physical_data_dir);
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
        let (target_idx, target_version) = self
            .versions
            .iter()
            .enumerate()
            .find(|(_, v)| v.header.version_type == VersionType::Object && v.header.version_id.unwrap_or_default() == vid)?;
        let target_data_dir = physical_data_dir(target_version).ok()??;

        for (idx, version) in self.versions.iter().enumerate() {
            if idx == target_idx || version.header.version_type != VersionType::Object {
                continue;
            }
            match physical_data_dir(version) {
                Ok(Some(dir)) if version.header.version_id.unwrap_or_default() != vid && dir == target_data_dir => {
                    return None;
                }
                Ok(_) => {}
                Err(_) => return None,
            }
        }

        Some(target_data_dir)
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
        let mut data_dirs = Vec::with_capacity(self.versions.len());
        for version in &self.versions {
            if version.header.version_type == VersionType::Object {
                data_dirs.push(FileMetaVersion::decode_data_dir_from_meta(&version.meta)?);
            }
        }
        Ok(data_dirs)
    }

    /// Count shared data directories
    pub fn shared_data_dir_count(&self, version_id: Option<Uuid>, data_dir: Option<Uuid>) -> usize {
        self.shared_data_dir_count_with_inline_data(version_id, data_dir, None, false)
    }

    pub(super) fn shared_data_dir_count_with_inline_data(
        &self,
        version_id: Option<Uuid>,
        data_dir: Option<Uuid>,
        has_inline_data: Option<bool>,
        stop_on_first: bool,
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
                Ok(Some(dir)) if dir == data_dir => {
                    count += 1;
                    if stop_on_first {
                        return count;
                    }
                }
                Ok(_) => {}
                Err(_) => return 1,
            }
        }
        count
    }
}

#[derive(Clone, Copy)]
struct IndexedDataDir {
    data_dir: Option<Uuid>,
    physical: bool,
}

impl IndexedDataDir {
    fn physical_dir(self) -> Option<Uuid> {
        self.data_dir.filter(|_| self.physical)
    }
}

pub(super) enum IndexedDelete {
    Applied(Option<Uuid>),
    Fallback,
}

/// Batch-local state: version bodies stay immutable until one final compaction.
/// Shared-directory counts are built only when a non-inline removal needs them.
pub(super) struct BatchDeleteIndex {
    by_version: HashMap<Uuid, usize>,
    removed: Vec<bool>,
    summaries: Vec<Option<IndexedDataDir>>,
    directory_refs: Option<HashMap<Uuid, usize>>,
}

impl BatchDeleteIndex {
    pub(super) fn new(metadata: &FileMeta, requests: &[FileInfo]) -> Option<Self> {
        // Inline removals already skip sibling decodes; they need a larger
        // batch to amortize indexing over the scalar lookup/shift path.
        if requests.len() < 8 || metadata.versions.len() < 8 {
            return None;
        }
        let minimum = if metadata.data.entries().ok()? == 0 { 8 } else { 128 };
        if requests.len() < minimum
            || metadata.versions.len() < minimum
            || requests.iter().any(|request| {
                request.deleted
                    || request.mark_deleted
                    || request.expire_restored
                    || !request.transition_status.is_empty()
                    || !request.version_purge_status().is_empty()
                    || request.tier_free_version()
            })
        {
            return None;
        }

        let mut by_version = HashMap::with_capacity(metadata.versions.len());
        for (index, version) in metadata.versions.iter().enumerate() {
            let id = version.header.version_id?;
            if by_version.insert(id, index).is_some() {
                return None;
            }
        }
        if requests.iter().any(|request| {
            by_version
                .get(&request.version_id.unwrap_or_default())
                .is_some_and(|index| metadata.versions[*index].header.version_type != VersionType::Object)
        }) {
            return None;
        }

        Some(Self {
            by_version,
            removed: vec![false; metadata.versions.len()],
            summaries: vec![None; metadata.versions.len()],
            directory_refs: None,
        })
    }

    fn summary(&mut self, versions: &[FileMetaShallowVersion], index: usize) -> Option<IndexedDataDir> {
        if let Some(summary) = self.summaries[index] {
            return Some(summary);
        }
        let version = &versions[index];
        let summary = FileMetaVersion::inspect_v2_object_slice(&version.meta).ok()?;
        // Transition/restore state is time-sensitive and may require a free
        // version. Preserve the canonical mutation path for those records.
        if summary.transition_complete || Some(summary.version_id.unwrap_or_default()) != version.header.version_id {
            return None;
        }
        let summary = IndexedDataDir {
            data_dir: summary.data_dir,
            physical: summary.physical_data_dir.is_some(),
        };
        self.summaries[index] = Some(summary);
        Some(summary)
    }

    fn directory_counts(&mut self, versions: &[FileMetaShallowVersion]) -> Option<HashMap<Uuid, usize>> {
        let mut counts = HashMap::with_capacity(self.by_version.len());
        for (index, version) in versions.iter().enumerate() {
            if self.removed[index] || version.header.version_type != VersionType::Object {
                continue;
            }
            if let Some(dir) = self.summary(versions, index)?.physical_dir() {
                *counts.entry(dir).or_insert(0) += 1;
            }
        }
        Some(counts)
    }

    pub(super) fn delete(
        &mut self,
        versions: &[FileMetaShallowVersion],
        request: &FileInfo,
        has_inline_data: bool,
    ) -> Result<IndexedDelete> {
        request.validate_for_delete_operation()?;
        let id = request.version_id.unwrap_or_default();
        let Some(&index) = self.by_version.get(&id) else {
            return Ok(IndexedDelete::Applied(None));
        };
        let Some(summary) = self.summary(versions, index) else {
            return Ok(IndexedDelete::Fallback);
        };
        if !has_inline_data && summary.data_dir.is_some() && self.directory_refs.is_none() {
            let Some(counts) = self.directory_counts(versions) else {
                return Ok(IndexedDelete::Fallback);
            };
            self.directory_refs = Some(counts);
        }
        if let Some(counts) = self.directory_refs.as_mut()
            && let Some(dir) = summary.physical_dir()
        {
            let count = counts.get_mut(&dir).ok_or(Error::FileCorrupt)?;
            *count = count.checked_sub(1).ok_or(Error::FileCorrupt)?;
        }

        self.by_version.remove(&id);
        self.removed[index] = true;
        if !has_inline_data
            && let Some(dir) = summary.data_dir
            && self
                .directory_refs
                .as_ref()
                .and_then(|counts| counts.get(&dir))
                .is_some_and(|count| *count > 0)
        {
            return Ok(IndexedDelete::Applied(None));
        }
        Ok(IndexedDelete::Applied(summary.data_dir))
    }

    pub(super) fn finish(self, versions: &mut Vec<FileMetaShallowVersion>) {
        let mut index = 0;
        versions.retain(|_| {
            let retain = !self.removed[index];
            index += 1;
            retain
        });
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
        assert_eq!(
            actual.versions.iter().map(|version| &version.header).collect::<Vec<_>>(),
            expected.versions.iter().map(|version| &version.header).collect::<Vec<_>>(),
            "version identities and deterministic signatures must agree"
        );
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

    fn indexed_batch_fixture(inline_mode: u8, shared: bool) -> (FileMeta, Vec<FileInfo>) {
        let mut metadata = FileMeta::new();
        let mut requests = Vec::new();
        for id in 0_u128..256 {
            let version = Uuid::from_u128(id);
            let dir = if inline_mode == 1 && id == 0 {
                Uuid::from_u128(1)
            } else {
                Uuid::from_u128(10_000 + if shared { id % 7 } else { id })
            };
            let mut info = make_plain_file_info(version, dir);
            info.mod_time = Some(OffsetDateTime::UNIX_EPOCH + Duration::seconds(1_700_000_000 + i64::try_from(id).expect("id")));
            if inline_mode == 2 || (inline_mode == 1 && id % 3 != 2) {
                info.data = Some(Bytes::from_static(b"indexed payload"));
                info.set_inline_data();
            }
            metadata.add_version(info).expect("seed indexed graph");
            requests.push(FileInfo {
                version_id: Some(version),
                ..Default::default()
            });
        }
        (metadata, requests)
    }

    fn assert_indexed_matches_scalar(original: &FileMeta, requests: &[FileInfo]) {
        let mut scalar = original.clone();
        let expected = scalar.delete_versions_with_index(requests, None);
        let mut indexed = original.clone();
        assert_eq!(indexed.delete_versions(requests), expected);
        assert_eq!(indexed, scalar, "results, retained metadata, and inline bytes must agree");
    }

    #[test]
    fn indexed_batches_match_scalar_for_shared_inline_null_and_repeated_versions() {
        for inline_mode in 0..3 {
            for shared in [false, true] {
                let (original, requests) = indexed_batch_fixture(inline_mode, shared);
                assert!(BatchDeleteIndex::new(&original, &requests).is_some());
                let mut reverse = requests.clone();
                reverse.reverse();
                let alternating: Vec<_> = requests.iter().step_by(2).cloned().collect();
                let mut duplicate_and_missing = requests.clone();
                duplicate_and_missing.extend_from_slice(&requests[..64]);
                duplicate_and_missing.push(FileInfo {
                    version_id: Some(Uuid::from_u128(999)),
                    ..Default::default()
                });
                for selected in [&requests, &reverse, &alternating, &duplicate_and_missing, &requests[..32]] {
                    assert_indexed_matches_scalar(&original, selected);
                }
                if inline_mode > 0 {
                    let mut legacy_null = original.clone();
                    let payload = legacy_null
                        .data
                        .find(NULL_VERSION_ID)
                        .expect("null lookup")
                        .expect("null payload");
                    legacy_null.data.remove_key(NULL_VERSION_ID).expect("remove modern null key");
                    legacy_null
                        .data
                        .replace(&Uuid::nil().to_string(), payload)
                        .expect("legacy null key");
                    let mut null_request = requests.clone();
                    null_request[0].version_id = None;
                    assert_indexed_matches_scalar(&legacy_null, &null_request);
                }
            }
        }
    }

    #[test]
    fn indexed_batches_flush_partial_removals_before_validation_or_decode_failure() {
        let (original, requests) = indexed_batch_fixture(2, false);
        let mut invalid = requests.clone();
        invalid[8].parts = vec![
            ObjectPartInfo {
                number: 1,
                ..Default::default()
            };
            2
        ];
        assert!(invalid[8].validate_for_delete_operation().is_err());
        assert_indexed_matches_scalar(&original, &invalid);

        let mut corrupt = original;
        let bad = requests[8].version_id;
        corrupt
            .versions
            .iter_mut()
            .find(|version| version.header.version_id == bad)
            .expect("target")
            .meta = vec![0xc1];
        let mut index = BatchDeleteIndex::new(&corrupt, &requests).expect("header index remains eligible");
        for request in &requests[..8] {
            assert!(matches!(
                index.delete(&corrupt.versions, request, true).expect("inline delete"),
                IndexedDelete::Applied(_)
            ));
        }
        assert!(index.directory_refs.is_none(), "inline removals must not decode unrelated siblings");
        assert_eq!(index.summaries.iter().flatten().count(), 8);
        assert!(matches!(
            index
                .delete(&corrupt.versions, &requests[8], true)
                .expect("fallback decision"),
            IndexedDelete::Fallback
        ));
        assert_indexed_matches_scalar(&corrupt, &requests);
    }

    #[test]
    fn indexed_batches_keep_canonical_cleanup_for_corrupt_and_transitioned_siblings() {
        let (original, requests) = indexed_batch_fixture(0, true);
        for transition in [false, true] {
            let mut metadata = original.clone();
            let sibling = metadata
                .versions
                .iter_mut()
                .find(|version| version.header.version_id == requests[255].version_id)
                .expect("sibling");
            if transition {
                let mut decoded = sibling.parse_version_meta().expect("decode sibling");
                let object = decoded.object.as_mut().expect("object");
                insert_bytes(&mut object.meta_sys, SUFFIX_TRANSITION_STATUS, TRANSITION_COMPLETE.as_bytes().to_vec());
                object.meta_user.insert(
                    metadata_keys::RESTORE.to_string(),
                    "ongoing-request=\"false\", expiry-date=\"9999-01-01T00:00:00Z\"".to_string(),
                );
                sibling.meta = decoded.marshal_msg().expect("encode transition");
            } else {
                sibling.meta = vec![0xc1];
            }
            let selected = &requests[..128];
            let mut index = BatchDeleteIndex::new(&metadata, selected).expect("eligible request headers");
            assert!(matches!(
                index
                    .delete(&metadata.versions, &selected[0], false)
                    .expect("fallback decision"),
                IndexedDelete::Fallback
            ));
            assert!(!index.removed.iter().any(|removed| *removed));
            assert_indexed_matches_scalar(&metadata, selected);
            if !transition {
                let mut actual = metadata;
                assert!(
                    actual
                        .delete_versions(selected)
                        .expect("delete valid targets")
                        .iter()
                        .all(Option::is_none)
                );
            }
        }
    }

    #[test]
    fn inconsistent_indexed_reference_counts_cannot_publish_cleanup() {
        let (original, requests) = indexed_batch_fixture(0, false);
        for counts in [HashMap::new(), HashMap::from([(Uuid::from_u128(10_000), 0)])] {
            let mut metadata = original.clone();
            let mut index = BatchDeleteIndex::new(&metadata, &requests).expect("indexed graph");
            index.directory_refs = Some(counts);
            assert!(matches!(index.delete(&metadata.versions, &requests[0], false), Err(Error::FileCorrupt)));
            assert!(!index.removed.iter().any(|removed| *removed));
            index.finish(&mut metadata.versions);
            assert_eq!(metadata, original, "an inconsistent cache cannot authorize removal");
        }
    }

    #[test]
    fn indexed_selection_distinguishes_empty_inline_framing_from_payload_entries() {
        let (mut metadata, requests) = indexed_batch_fixture(0, false);
        metadata.data.update(&[1, 0x80]);
        assert!(BatchDeleteIndex::new(&metadata, &requests[..8]).is_some());
        assert_indexed_matches_scalar(&metadata, &requests[..8]);
        assert!(BatchDeleteIndex::new(&metadata, &requests[..1]).is_none());
        let (inline, inline_requests) = indexed_batch_fixture(2, false);
        assert!(BatchDeleteIndex::new(&inline, &inline_requests[..32]).is_none());
        assert!(BatchDeleteIndex::new(&inline, &inline_requests[..128]).is_some());
    }

    #[test]
    fn indexed_physical_batches_decode_each_version_once() {
        use super::super::version::V2_OBJECT_SCAN_COUNT;

        let (original, requests) = indexed_batch_fixture(0, false);
        let versions = original.versions.len();
        let mut scalar = original.clone();
        V2_OBJECT_SCAN_COUNT.with(|count| count.set(0));
        let expected = scalar.delete_versions_with_index(&requests, None).expect("scalar batch");
        let scalar_scans = V2_OBJECT_SCAN_COUNT.with(std::cell::Cell::get);
        let mut indexed = original;
        V2_OBJECT_SCAN_COUNT.with(|count| count.set(0));
        let actual = indexed.delete_versions(&requests).expect("indexed batch");
        let indexed_scans = V2_OBJECT_SCAN_COUNT.with(std::cell::Cell::get);
        assert_eq!(actual, expected);
        assert_eq!(indexed, scalar);
        assert_eq!(scalar_scans, versions * (versions + 1) / 2);
        assert_eq!(indexed_scans, versions, "a batch must not re-decode retained siblings");
        println!("V2 scans for {versions} versions: scalar={scalar_scans}, indexed={indexed_scans}");
    }

    #[test]
    fn indexed_batches_decline_ambiguous_headers_and_nonordinary_requests() {
        let (original, requests) = indexed_batch_fixture(0, false);
        let mut duplicate = original.clone();
        duplicate.versions[0].header.version_id = duplicate.versions[1].header.version_id;
        assert!(BatchDeleteIndex::new(&duplicate, &requests).is_none());
        assert_indexed_matches_scalar(&duplicate, &requests);
        let mut absent_id = original.clone();
        absent_id.versions[0].header.version_id = None;
        assert!(BatchDeleteIndex::new(&absent_id, &requests).is_none());
        assert_indexed_matches_scalar(&absent_id, &requests);
        let mut marker = requests[0].clone();
        marker.deleted = true;
        marker.mod_time = Some(OffsetDateTime::UNIX_EPOCH + Duration::seconds(1_700_000_000));
        let mut retained = requests[0].clone();
        retained.mark_deleted = true;
        let mut restored = requests[0].clone();
        restored.expire_restored = true;
        let mut transition = requests[0].clone();
        transition.transition_status = TRANSITION_COMPLETE.to_string();
        let mut free_version = requests[0].clone();
        free_version.set_tier_free_version();
        let mut pending = requests[0].clone();
        pending.replication_state_internal = Some(ReplicationState {
            version_purge_status_internal: Some("target=PENDING;".into()),
            purge_targets: version_purge_statuses_map("target=PENDING;"),
            ..Default::default()
        });
        for operation in [marker, retained, restored, transition, free_version, pending] {
            let mut complex = requests.clone();
            complex[0] = operation;
            assert!(BatchDeleteIndex::new(&original, &complex).is_none());
            // State changes re-encode unordered maps; their semantic and
            // signature contract is covered by the canonical sequential oracle.
            assert_batch_matches_sequential(&original, &complex);
        }

        let mut corrupt_inline = original;
        corrupt_inline.data.update(&[1, 0x81, 0xa1, b'x', 0xc4, 255]);
        let before = corrupt_inline.clone();
        assert!(corrupt_inline.delete_versions(&requests).is_err());
        assert_eq!(corrupt_inline, before);
    }

    #[test]
    fn find_unshared_data_dir_and_shared_count_fail_closed_on_corrupt_sibling() {
        let target_version = Uuid::new_v4();
        let target_data_dir = Uuid::new_v4();
        let mut meta = FileMeta::new();
        meta.add_version(make_plain_file_info(target_version, target_data_dir))
            .expect("seed target version");
        meta.versions.push(FileMetaShallowVersion {
            header: FileMetaVersionHeader {
                version_id: Some(Uuid::new_v4()),
                version_type: VersionType::Object,
                ..Default::default()
            },
            meta: vec![0x81, 0xa4, b'V', b'2', b'O', b'b'],
        });

        assert_eq!(meta.find_unshared_data_dir_for_version(Some(target_version)), None);
        assert_eq!(meta.shared_data_dir_count(Some(target_version), Some(target_data_dir)), 1);
    }
}
