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

use std::collections::HashMap;
use time::OffsetDateTime;

#[derive(Debug, Default, Clone)]
pub struct MultipartUploadResult {
    pub upload_id: String,
    pub checksum_algo: Option<String>,
    pub checksum_type: Option<String>,
}

#[derive(Debug, Default, Clone)]
pub struct PartInfo {
    pub part_num: usize,
    pub last_mod: Option<OffsetDateTime>,
    pub size: usize,
    pub etag: Option<String>,
    pub actual_size: i64,
}

#[derive(Debug, Clone, Default)]
pub struct MultipartInfo {
    pub bucket: String,
    pub object: String,
    pub upload_id: String,
    pub initiated: Option<OffsetDateTime>,
    pub user_defined: HashMap<String, String>,
}

#[derive(Debug, Clone, Default)]
pub struct ListMultipartsInfo {
    pub key_marker: Option<String>,
    pub upload_id_marker: Option<String>,
    pub next_key_marker: Option<String>,
    pub next_upload_id_marker: Option<String>,
    pub max_uploads: usize,
    pub is_truncated: bool,
    pub uploads: Vec<MultipartInfo>,
    pub prefix: String,
    pub delimiter: Option<String>,
    pub common_prefixes: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ListPartsInfo {
    pub bucket: String,
    pub object: String,
    pub upload_id: String,
    pub storage_class: String,
    pub part_number_marker: usize,
    pub next_part_number_marker: usize,
    pub max_parts: usize,
    pub is_truncated: bool,
    pub parts: Vec<PartInfo>,
    pub user_defined: HashMap<String, String>,
    pub checksum_algorithm: String,
    pub checksum_type: String,
}

#[derive(Debug, Clone, Default)]
pub struct CompletePart {
    pub part_num: usize,
    pub etag: Option<String>,
    pub checksum_crc32: Option<String>,
    pub checksum_crc32c: Option<String>,
    pub checksum_sha1: Option<String>,
    pub checksum_sha256: Option<String>,
    pub checksum_crc64nvme: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_part_defaults_preserve_empty_checksums() {
        let part = CompletePart::default();

        assert_eq!(part.part_num, 0);
        assert!(part.etag.is_none());
        assert!(part.checksum_crc32.is_none());
        assert!(part.checksum_crc32c.is_none());
        assert!(part.checksum_sha1.is_none());
        assert!(part.checksum_sha256.is_none());
        assert!(part.checksum_crc64nvme.is_none());
    }
}
