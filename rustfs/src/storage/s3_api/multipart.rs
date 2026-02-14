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

use crate::storage::ecfs::RUSTFS_OWNER;
use rustfs_ecstore::client::object_api_utils::to_s3s_etag;
use rustfs_ecstore::store_api::ListPartsInfo;
use s3s::dto::{Initiator, ListPartsOutput, Part, Timestamp};

pub(crate) fn build_list_parts_output(res: ListPartsInfo) -> ListPartsOutput {
    ListPartsOutput {
        bucket: Some(res.bucket),
        key: Some(res.object),
        upload_id: Some(res.upload_id),
        parts: Some(
            res.parts
                .into_iter()
                .map(|p| Part {
                    e_tag: p.etag.map(|etag| to_s3s_etag(&etag)),
                    last_modified: p.last_mod.map(Timestamp::from),
                    part_number: Some(p.part_num as i32),
                    size: Some(p.size as i64),
                    ..Default::default()
                })
                .collect(),
        ),
        owner: Some(RUSTFS_OWNER.to_owned()),
        initiator: Some(Initiator {
            id: RUSTFS_OWNER.id.clone(),
            display_name: RUSTFS_OWNER.display_name.clone(),
        }),
        is_truncated: Some(res.is_truncated),
        next_part_number_marker: res.next_part_number_marker.try_into().ok(),
        max_parts: res.max_parts.try_into().ok(),
        part_number_marker: res.part_number_marker.try_into().ok(),
        storage_class: if res.storage_class.is_empty() {
            None
        } else {
            Some(res.storage_class.into())
        },
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::build_list_parts_output;
    use rustfs_ecstore::client::object_api_utils::to_s3s_etag;
    use rustfs_ecstore::store_api::{ListPartsInfo, PartInfo};

    #[test]
    fn test_list_parts_output_maps_parts_and_owner() {
        let input = ListPartsInfo {
            bucket: "bucket-a".to_string(),
            object: "obj-a".to_string(),
            upload_id: "upload-a".to_string(),
            storage_class: "STANDARD".to_string(),
            part_number_marker: 1,
            next_part_number_marker: 2,
            max_parts: 1000,
            is_truncated: true,
            parts: vec![PartInfo {
                part_num: 1,
                size: 11,
                etag: Some("etag-1".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        };

        let output = build_list_parts_output(input);
        let parts = output.parts.as_ref().expect("parts should be present");

        assert_eq!(output.bucket.as_deref(), Some("bucket-a"));
        assert_eq!(output.key.as_deref(), Some("obj-a"));
        assert_eq!(output.upload_id.as_deref(), Some("upload-a"));
        assert_eq!(output.storage_class.as_ref().map(|v| v.as_str()), Some("STANDARD"));
        assert_eq!(output.part_number_marker, Some(1));
        assert_eq!(output.next_part_number_marker, Some(2));
        assert_eq!(output.max_parts, Some(1000));
        assert_eq!(output.is_truncated, Some(true));
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].part_number, Some(1));
        assert_eq!(parts[0].size, Some(11));
        assert_eq!(parts[0].e_tag, Some(to_s3s_etag("etag-1")));
        assert!(output.owner.is_some());
        assert!(output.initiator.is_some());
    }

    #[test]
    fn test_list_parts_output_handles_empty_storage_class_and_overflow_markers() {
        let input = ListPartsInfo {
            storage_class: String::new(),
            part_number_marker: usize::MAX,
            next_part_number_marker: usize::MAX,
            max_parts: usize::MAX,
            ..Default::default()
        };

        let output = build_list_parts_output(input);

        assert_eq!(output.storage_class, None);
        assert_eq!(output.part_number_marker, None);
        assert_eq!(output.next_part_number_marker, None);
        assert_eq!(output.max_parts, None);
    }
}
