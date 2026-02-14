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

use crate::storage::s3_api::common::{rustfs_initiator, rustfs_owner};
use rustfs_ecstore::client::object_api_utils::to_s3s_etag;
use rustfs_ecstore::set_disk::MAX_PARTS_COUNT;
use rustfs_ecstore::store_api::{ListMultipartsInfo, ListPartsInfo};
use s3s::dto::{CommonPrefix, ListMultipartUploadsOutput, ListPartsOutput, MultipartUpload, Part, Timestamp};
use s3s::{S3Error, S3ErrorCode};

pub(crate) fn build_list_parts_output(res: ListPartsInfo) -> ListPartsOutput {
    let owner = rustfs_owner();
    let initiator = rustfs_initiator();

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
                    part_number: p.part_num.try_into().ok(),
                    size: p.size.try_into().ok(),
                    ..Default::default()
                })
                .collect(),
        ),
        owner: Some(owner),
        initiator: Some(initiator),
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

pub(crate) fn parse_list_parts_params(
    part_number_marker: Option<i32>,
    max_parts: Option<i32>,
) -> Result<(Option<usize>, usize), S3Error> {
    let part_number_marker = part_number_marker.map(|x| x as usize);
    let max_parts = match max_parts {
        Some(parts) => {
            if !(1..=1000).contains(&parts) {
                return Err(S3Error::with_message(
                    S3ErrorCode::InvalidArgument,
                    "max-parts must be between 1 and 1000".to_string(),
                ));
            }
            parts as usize
        }
        None => 1000,
    };

    Ok((part_number_marker, max_parts))
}

pub(crate) fn parse_list_multipart_uploads_params(
    prefix: Option<String>,
    key_marker: Option<String>,
    max_uploads: Option<i32>,
) -> Result<(String, Option<String>, usize), S3Error> {
    let prefix = prefix.unwrap_or_default();
    let max_uploads = match max_uploads {
        Some(value) => {
            let value = usize::try_from(value).map_err(|_| {
                S3Error::with_message(
                    S3ErrorCode::InvalidArgument,
                    format!("max-uploads must be between 1 and {}", MAX_PARTS_COUNT),
                )
            })?;

            if value == 0 || value > MAX_PARTS_COUNT {
                return Err(S3Error::with_message(
                    S3ErrorCode::InvalidArgument,
                    format!("max-uploads must be between 1 and {}", MAX_PARTS_COUNT),
                ));
            }

            value
        }
        None => MAX_PARTS_COUNT,
    };

    if let Some(key_marker) = &key_marker
        && !key_marker.starts_with(prefix.as_str())
    {
        return Err(S3Error::with_message(S3ErrorCode::NotImplemented, "Invalid key marker".to_string()));
    }

    Ok((prefix, key_marker, max_uploads))
}

pub(crate) fn build_list_multipart_uploads_output(
    bucket: String,
    prefix: String,
    result: ListMultipartsInfo,
) -> ListMultipartUploadsOutput {
    ListMultipartUploadsOutput {
        bucket: Some(bucket),
        prefix: Some(prefix),
        delimiter: result.delimiter,
        key_marker: result.key_marker,
        upload_id_marker: result.upload_id_marker,
        max_uploads: Some(result.max_uploads as i32),
        is_truncated: Some(result.is_truncated),
        uploads: Some(
            result
                .uploads
                .into_iter()
                .map(|u| MultipartUpload {
                    key: Some(u.object),
                    upload_id: Some(u.upload_id),
                    initiated: u.initiated.map(Timestamp::from),
                    ..Default::default()
                })
                .collect(),
        ),
        common_prefixes: Some(
            result
                .common_prefixes
                .into_iter()
                .map(|c| CommonPrefix { prefix: Some(c) })
                .collect(),
        ),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_list_multipart_uploads_output, build_list_parts_output, parse_list_multipart_uploads_params,
        parse_list_parts_params,
    };
    use rustfs_ecstore::client::object_api_utils::to_s3s_etag;
    use rustfs_ecstore::set_disk::MAX_PARTS_COUNT;
    use rustfs_ecstore::store_api::{ListMultipartsInfo, ListPartsInfo, MultipartInfo, PartInfo};
    use s3s::S3ErrorCode;
    use s3s::dto::Timestamp;
    use time::OffsetDateTime;

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
            parts: vec![PartInfo {
                part_num: usize::MAX,
                size: usize::MAX,
                ..Default::default()
            }],
            ..Default::default()
        };

        let output = build_list_parts_output(input);
        let parts = output.parts.as_ref().expect("parts should be present");

        assert_eq!(output.storage_class, None);
        assert_eq!(output.part_number_marker, None);
        assert_eq!(output.next_part_number_marker, None);
        assert_eq!(output.max_parts, None);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].part_number, None);
        assert_eq!(parts[0].size, None);
    }

    #[test]
    fn test_list_multipart_uploads_output_maps_uploads_and_prefixes() {
        let result = ListMultipartsInfo {
            delimiter: Some("/".to_string()),
            key_marker: Some("key-marker".to_string()),
            upload_id_marker: Some("upload-id-marker".to_string()),
            max_uploads: 1000,
            is_truncated: true,
            uploads: vec![MultipartInfo {
                object: "obj-a".to_string(),
                upload_id: "upload-a".to_string(),
                initiated: Some(OffsetDateTime::UNIX_EPOCH),
                ..Default::default()
            }],
            common_prefixes: vec!["prefix-a/".to_string(), "prefix-b/".to_string()],
            ..Default::default()
        };

        let output = build_list_multipart_uploads_output("bucket-a".to_string(), "root/".to_string(), result);

        let uploads = output.uploads.as_ref().expect("uploads should be present");
        let common_prefixes = output.common_prefixes.as_ref().expect("common prefixes should be present");

        assert_eq!(output.bucket.as_deref(), Some("bucket-a"));
        assert_eq!(output.prefix.as_deref(), Some("root/"));
        assert_eq!(output.delimiter.as_deref(), Some("/"));
        assert_eq!(output.key_marker.as_deref(), Some("key-marker"));
        assert_eq!(output.upload_id_marker.as_deref(), Some("upload-id-marker"));
        assert_eq!(output.max_uploads, Some(1000));
        assert_eq!(output.is_truncated, Some(true));

        assert_eq!(uploads.len(), 1);
        assert_eq!(uploads[0].key.as_deref(), Some("obj-a"));
        assert_eq!(uploads[0].upload_id.as_deref(), Some("upload-a"));
        assert_eq!(uploads[0].initiated, Some(Timestamp::from(OffsetDateTime::UNIX_EPOCH)));

        assert_eq!(common_prefixes.len(), 2);
        assert_eq!(common_prefixes[0].prefix.as_deref(), Some("prefix-a/"));
        assert_eq!(common_prefixes[1].prefix.as_deref(), Some("prefix-b/"));
    }

    #[test]
    fn test_parse_list_parts_params_defaults_and_valid_values() {
        let (part_number_marker, max_parts) = parse_list_parts_params(Some(5), Some(100)).expect("expected valid params");
        assert_eq!(part_number_marker, Some(5));
        assert_eq!(max_parts, 100);

        let (part_number_marker, max_parts) = parse_list_parts_params(None, None).expect("expected default params");
        assert_eq!(part_number_marker, None);
        assert_eq!(max_parts, 1000);
    }

    #[test]
    fn test_parse_list_parts_params_rejects_invalid_max_parts() {
        let err = parse_list_parts_params(None, Some(0)).expect_err("expected invalid max_parts");
        assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);

        let err = parse_list_parts_params(None, Some(1001)).expect_err("expected invalid max_parts");
        assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn test_parse_list_multipart_uploads_params_defaults_and_valid_values() {
        let (prefix, key_marker, max_uploads) =
            parse_list_multipart_uploads_params(Some("prefix/".to_string()), Some("prefix/key-marker".to_string()), Some(100))
                .expect("expected valid params");
        assert_eq!(prefix, "prefix/");
        assert_eq!(key_marker.as_deref(), Some("prefix/key-marker"));
        assert_eq!(max_uploads, 100);

        let (prefix, key_marker, max_uploads) =
            parse_list_multipart_uploads_params(None, None, None).expect("expected default params");
        assert_eq!(prefix, "");
        assert_eq!(key_marker, None);
        assert_eq!(max_uploads, MAX_PARTS_COUNT);
    }

    #[test]
    fn test_parse_list_multipart_uploads_params_rejects_invalid_key_marker() {
        let err = parse_list_multipart_uploads_params(Some("prefix/".to_string()), Some("other/key-marker".to_string()), None)
            .expect_err("expected invalid key marker");

        assert_eq!(*err.code(), S3ErrorCode::NotImplemented);
        assert_eq!(err.message(), Some("Invalid key marker"));
    }

    #[test]
    fn test_parse_list_multipart_uploads_params_rejects_invalid_max_uploads() {
        let err = parse_list_multipart_uploads_params(Some("prefix/".to_string()), None, Some(-1))
            .expect_err("expected invalid max_uploads");
        assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);

        let err = parse_list_multipart_uploads_params(Some("prefix/".to_string()), None, Some(0))
            .expect_err("expected invalid max_uploads");
        assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);

        let err = parse_list_multipart_uploads_params(Some("prefix/".to_string()), None, Some((MAX_PARTS_COUNT + 1) as i32))
            .expect_err("expected invalid max_uploads");
        assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);
    }
}
