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
        let version_id = version_id.unwrap_or_default();

        if self.data.entries().unwrap_or_default() > 0
            && self.data.find(version_id.to_string().as_str()).unwrap_or_default().is_some()
        {
            return 0;
        }

        self.versions
            .iter()
            .filter(|v| {
                v.header.version_type == VersionType::Object
                    && v.header.version_id != Some(version_id)
                    && v.header.uses_data_dir()
            })
            .filter_map(|v| FileMetaVersion::decode_data_dir_from_meta(&v.meta).ok())
            .filter(|&dir| dir.is_none() || dir != data_dir)
            //.filter(|&dir| dir != data_dir)
            .count()
    }
}
