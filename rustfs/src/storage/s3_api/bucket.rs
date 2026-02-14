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

use rustfs_ecstore::client::object_api_utils::to_s3s_etag;
use rustfs_ecstore::store_api::ListObjectsV2Info;
use s3s::dto::{
    CommonPrefix, EncodingType, ListObjectsOutput, ListObjectsV2Output, Object, ObjectStorageClass, Owner, Timestamp,
};
use urlencoding::encode;

pub(crate) fn build_list_objects_output(v2: ListObjectsV2Output, request_marker: Option<String>) -> ListObjectsOutput {
    let next_marker = calculate_next_marker(&v2);

    // S3 API requires marker field in response, echoing back the request marker.
    // If no marker was provided in request, return empty string per S3 standard.
    let marker = Some(request_marker.unwrap_or_default());

    ListObjectsOutput {
        contents: v2.contents,
        delimiter: v2.delimiter,
        encoding_type: v2.encoding_type,
        name: v2.name,
        prefix: v2.prefix,
        max_keys: v2.max_keys,
        common_prefixes: v2.common_prefixes,
        is_truncated: v2.is_truncated,
        marker,
        next_marker,
        ..Default::default()
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_list_objects_v2_output(
    object_infos: ListObjectsV2Info,
    fetch_owner: bool,
    max_keys: i32,
    bucket: String,
    prefix: String,
    delimiter: Option<String>,
    encoding_type: Option<EncodingType>,
    response_continuation_token: Option<String>,
    response_start_after: Option<String>,
) -> ListObjectsV2Output {
    // Apply URL encoding if encoding_type is "url".
    // S3 URL encoding encodes special characters but keeps '/' unencoded.
    let should_encode = encoding_type.as_ref().is_some_and(|e| e.as_str() == EncodingType::URL);

    let encode_s3_name = |name: &str| -> String {
        name.split('/')
            .map(|part| encode(part).to_string())
            .collect::<Vec<_>>()
            .join("/")
    };

    let objects: Vec<Object> = object_infos
        .objects
        .iter()
        .filter(|v| !v.name.is_empty())
        .map(|v| {
            let key = if should_encode {
                encode_s3_name(&v.name)
            } else {
                v.name.to_owned()
            };
            let mut obj = Object {
                key: Some(key),
                last_modified: v.mod_time.map(Timestamp::from),
                size: Some(v.get_actual_size().unwrap_or_default()),
                e_tag: v.etag.clone().map(|etag| to_s3s_etag(&etag)),
                storage_class: v.storage_class.clone().map(ObjectStorageClass::from),
                ..Default::default()
            };

            if fetch_owner {
                obj.owner = Some(Owner {
                    display_name: Some("rustfs".to_owned()),
                    id: Some("v0.1".to_owned()),
                });
            }

            obj
        })
        .collect();

    let common_prefixes: Vec<CommonPrefix> = object_infos
        .prefixes
        .into_iter()
        .map(|v| {
            let prefix = if should_encode { encode_s3_name(&v) } else { v };
            CommonPrefix { prefix: Some(prefix) }
        })
        .collect();

    // KeyCount should include both objects and common prefixes per S3 API spec.
    let key_count = (objects.len() + common_prefixes.len()) as i32;

    // Encode next_continuation_token to base64.
    let next_continuation_token = object_infos
        .next_continuation_token
        .map(|token| base64_simd::STANDARD.encode_to_string(token.as_bytes()));

    ListObjectsV2Output {
        is_truncated: Some(object_infos.is_truncated),
        continuation_token: response_continuation_token,
        next_continuation_token,
        start_after: response_start_after,
        key_count: Some(key_count),
        max_keys: Some(max_keys),
        contents: Some(objects),
        delimiter,
        encoding_type,
        name: Some(bucket),
        prefix: Some(prefix),
        common_prefixes: Some(common_prefixes),
        ..Default::default()
    }
}

fn calculate_next_marker(v2: &ListObjectsV2Output) -> Option<String> {
    // For ListObjects (v1) API, NextMarker should be the last item returned when truncated.
    // When both Contents and CommonPrefixes are present, NextMarker should be the
    // lexicographically last item (either last key or last prefix).
    if !v2.is_truncated.unwrap_or(false) {
        return None;
    }

    let last_key = v2
        .contents
        .as_ref()
        .and_then(|contents| contents.last())
        .and_then(|obj| obj.key.as_ref())
        .cloned();

    let last_prefix = v2
        .common_prefixes
        .as_ref()
        .and_then(|prefixes| prefixes.last())
        .and_then(|prefix| prefix.prefix.as_ref())
        .cloned();

    // NextMarker should be the lexicographically last item.
    // This matches S3 standard behavior.
    match (last_key, last_prefix) {
        (Some(k), Some(p)) => {
            if k > p {
                Some(k)
            } else {
                Some(p)
            }
        }
        (Some(k), None) => Some(k),
        (None, Some(p)) => Some(p),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{build_list_objects_output, build_list_objects_v2_output};
    use rustfs_ecstore::store_api::{ListObjectsV2Info, ObjectInfo};
    use s3s::dto::{CommonPrefix, EncodingType, ListObjectsV2Output, Object};

    #[test]
    fn test_list_objects_marker_echoes_request_value() {
        let output = build_list_objects_output(ListObjectsV2Output::default(), Some("m-1".to_string()));
        assert_eq!(output.marker, Some("m-1".to_string()));
    }

    #[test]
    fn test_list_objects_marker_defaults_to_empty_string() {
        let output = build_list_objects_output(ListObjectsV2Output::default(), None);
        assert_eq!(output.marker, Some(String::new()));
    }

    #[test]
    fn test_list_objects_next_marker_uses_lexicographically_last_item() {
        let v2 = ListObjectsV2Output {
            is_truncated: Some(true),
            contents: Some(vec![Object {
                key: Some("apple".to_string()),
                ..Default::default()
            }]),
            common_prefixes: Some(vec![CommonPrefix {
                prefix: Some("zebra/".to_string()),
            }]),
            ..Default::default()
        };

        let output = build_list_objects_output(v2, None);
        assert_eq!(output.next_marker, Some("zebra/".to_string()));
    }

    #[test]
    fn test_list_objects_next_marker_is_none_when_not_truncated() {
        let v2 = ListObjectsV2Output {
            is_truncated: Some(false),
            contents: Some(vec![Object {
                key: Some("only-item".to_string()),
                ..Default::default()
            }]),
            ..Default::default()
        };

        let output = build_list_objects_output(v2, None);
        assert_eq!(output.next_marker, None);
    }

    #[test]
    fn test_list_objects_v2_key_count_includes_objects_and_prefixes() {
        let object_infos = ListObjectsV2Info {
            objects: vec![object_info("obj-a"), object_info("")],
            prefixes: vec!["p1/".to_string(), "p2/".to_string()],
            ..Default::default()
        };

        let output = build_list_objects_v2_output(
            object_infos,
            false,
            1000,
            "bucket-a".to_string(),
            "prefix-a".to_string(),
            Some("/".to_string()),
            None,
            None,
            None,
        );

        assert_eq!(output.key_count, Some(3));
        assert_eq!(output.contents.as_ref().map(std::vec::Vec::len), Some(1));
        assert_eq!(output.common_prefixes.as_ref().map(std::vec::Vec::len), Some(2));
    }

    #[test]
    fn test_list_objects_v2_url_encoding_preserves_slash() {
        let object_infos = ListObjectsV2Info {
            objects: vec![object_info("dir a/file+b%.txt")],
            prefixes: vec!["prefix a/sub+".to_string()],
            ..Default::default()
        };

        let output = build_list_objects_v2_output(
            object_infos,
            true,
            1000,
            "bucket-b".to_string(),
            "prefix-b".to_string(),
            Some("/".to_string()),
            Some(EncodingType::from_static(EncodingType::URL)),
            None,
            None,
        );

        let contents = output.contents.as_ref().expect("contents should exist");
        let common_prefixes = output.common_prefixes.as_ref().expect("common prefixes should exist");

        assert_eq!(contents[0].key.as_deref(), Some("dir%20a/file%2Bb%25.txt"));
        assert_eq!(common_prefixes[0].prefix.as_deref(), Some("prefix%20a/sub%2B"));
        assert!(contents[0].owner.is_some());
        assert_eq!(output.encoding_type.as_ref().map(EncodingType::as_str), Some(EncodingType::URL));
    }

    #[test]
    fn test_list_objects_v2_next_continuation_token_is_base64_encoded() {
        let object_infos = ListObjectsV2Info {
            next_continuation_token: Some("token-123".to_string()),
            ..Default::default()
        };

        let output = build_list_objects_v2_output(
            object_infos,
            false,
            1000,
            "bucket-c".to_string(),
            "prefix-c".to_string(),
            None,
            None,
            Some(String::new()),
            Some("start-after".to_string()),
        );

        assert_eq!(output.continuation_token, Some(String::new()));
        assert_eq!(output.start_after, Some("start-after".to_string()));
        assert_eq!(
            output.next_continuation_token,
            Some(base64_simd::STANDARD.encode_to_string("token-123".as_bytes()))
        );
    }

    fn object_info(name: &str) -> ObjectInfo {
        ObjectInfo {
            name: name.to_string(),
            ..Default::default()
        }
    }
}
