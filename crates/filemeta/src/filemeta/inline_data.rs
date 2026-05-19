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
    pub fn find_unshared_data_dir_for_version(&self, version_id: Option<Uuid>) -> Option<Uuid> {
        let vid = version_id.unwrap_or_default();
        let mut decoded_dirs = Vec::with_capacity(self.versions.len());

        for version in self
            .versions
            .iter()
            .filter(|v| v.header.version_type == VersionType::Object && v.header.uses_data_dir())
        {
            decoded_dirs.push((
                version.header.version_id.unwrap_or_default(),
                FileMetaVersion::decode_data_dir_from_meta(&version.meta).unwrap_or_default(),
            ));
        }

        let target_data_dir = decoded_dirs
            .iter()
            .find(|(id, _)| *id == vid)
            .and_then(|(_, data_dir)| *data_dir);
        let target_data_dir = target_data_dir?;

        if decoded_dirs
            .iter()
            .any(|(id, data_dir)| *id != vid && *data_dir == Some(target_data_dir))
        {
            return None;
        }

        Some(target_data_dir)
    }

    pub fn shard_data_dir_count(&self, vid: &Option<Uuid>, data_dir: &Option<Uuid>) -> usize {
        let vid = vid.unwrap_or_default();
        self.versions
            .iter()
            .filter(|v| {
                v.header.version_type == VersionType::Object && v.header.version_id != Some(vid) && v.header.uses_data_dir()
            })
            .map(|v| FileMetaVersion::decode_data_dir_from_meta(&v.meta).unwrap_or_default())
            .filter(|v| v == data_dir)
            .count()
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

        if self.data.entries().unwrap_or_default() > 0
            && self.find_inline_data_for_version(version_id).unwrap_or_default().is_some()
        {
            return 0;
        }

        self.versions
            .iter()
            .filter(|v| {
                v.header.version_type == VersionType::Object && v.header.version_id != Some(vid) && v.header.uses_data_dir()
            })
            .filter_map(|v| FileMetaVersion::decode_data_dir_from_meta(&v.meta).ok())
            .filter(|&dir| dir.is_some() && dir == data_dir)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use s3s::header::X_AMZ_RESTORE;
    use time::format_description::well_known::Rfc3339;
    use time::{Duration, OffsetDateTime};

    fn make_file_info(version_id: Uuid, data_dir: Uuid) -> FileInfo {
        let restore_header = format!(
            "ongoing-request=\"false\", expiry-date=\"{}\"",
            (OffsetDateTime::now_utc() + Duration::days(1))
                .format(&Rfc3339)
                .expect("format restore expiry"),
        );
        FileInfo {
            version_id: Some(version_id),
            data_dir: Some(data_dir),
            size: 64 * 1024,
            mod_time: Some(OffsetDateTime::now_utc()),
            metadata: [
                ("etag".to_string(), format!("etag-{version_id}")),
                (X_AMZ_RESTORE.as_str().to_string(), restore_header),
            ]
            .into_iter()
            .collect(),
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
}
