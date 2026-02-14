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

use s3s::dto::{ListObjectsOutput, ListObjectsV2Output};

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
    use super::build_list_objects_output;
    use s3s::dto::{CommonPrefix, ListObjectsV2Output, Object};

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
}
