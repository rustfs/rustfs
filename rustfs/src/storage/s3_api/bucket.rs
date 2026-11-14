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

use s3s::dto::EncodingType;
use s3s::{S3Error, S3ErrorCode};
use tracing::debug;
use urlencoding::encode;

const S3_MAX_KEYS: i32 = 1000;

fn invalid_continuation_token() -> S3Error {
    S3Error::with_message(S3ErrorCode::InvalidArgument, "Invalid continuation token".to_string())
}

/// Encodes the server-side cursor into the wire `NextContinuationToken`.
///
/// AWS defines the token as opaque, so nothing in it is a promise to clients.
/// The cursor is the plain last key of an ordinary listing, or the envelope
/// another listing layer (on-demand migration list-through, table isolation)
/// hands out; base64 keeps it URL-safe and is the only transform applied, so
/// nodes from earlier releases read the same token.
pub(crate) fn encode_list_objects_v2_continuation_token(cursor: &str) -> String {
    base64_simd::STANDARD.encode_to_string(cursor.as_bytes())
}

/// Decodes a wire `ContinuationToken` into the server-side cursor it carries.
/// A cursor issued by a 1.0.x server may still end in the metacache tag;
/// ecstore strips it on the way in.
fn decode_list_objects_v2_continuation_token(token: &str) -> Result<String, S3Error> {
    let bytes = base64_simd::STANDARD
        .decode_to_vec(token.as_bytes())
        .map_err(|_| invalid_continuation_token())?;
    String::from_utf8(bytes).map_err(|_| invalid_continuation_token())
}

fn normalize_max_keys(max_keys: i32) -> i32 {
    max_keys.min(S3_MAX_KEYS)
}

fn should_encode_url(encoding_type: Option<&EncodingType>) -> bool {
    encoding_type.is_some_and(|e| e.as_str() == EncodingType::URL)
}

fn encode_s3_name(name: &str) -> String {
    name.split('/')
        .map(|part| encode(part).to_string())
        .collect::<Vec<_>>()
        .join("/")
}

pub(crate) fn encode_list_output_value(value: String, encoding_type: Option<&EncodingType>) -> String {
    if should_encode_url(encoding_type) {
        encode_s3_name(&value)
    } else {
        value
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ListObjectVersionsParams {
    pub prefix: String,
    pub delimiter: Option<String>,
    pub key_marker: Option<String>,
    pub version_id_marker: Option<String>,
    pub max_keys: i32,
}
#[derive(Debug)]
pub(crate) struct ListObjectsV2Params {
    pub prefix: String,
    pub max_keys: i32,
    pub delimiter: Option<String>,
    pub response_start_after: Option<String>,
    pub start_after_for_query: Option<String>,
    pub response_continuation_token: Option<String>,
    /// Server-side cursor carried by the request token; see
    /// [`decode_list_objects_v2_continuation_token`].
    pub decoded_continuation_token: Option<String>,
}

pub(crate) fn parse_list_object_versions_params(
    prefix: Option<String>,
    delimiter: Option<String>,
    key_marker: Option<String>,
    version_id_marker: Option<String>,
    max_keys: Option<i32>,
) -> Result<ListObjectVersionsParams, S3Error> {
    let prefix = prefix.unwrap_or_default();
    let delimiter = delimiter.filter(|v| !v.is_empty());
    let key_marker = key_marker.filter(|v| !v.is_empty());
    let version_id_marker = version_id_marker.filter(|v| !v.is_empty());
    let max_keys = max_keys.unwrap_or(S3_MAX_KEYS);
    if max_keys < 0 {
        return Err(S3Error::with_message(S3ErrorCode::InvalidArgument, "Invalid max keys".to_string()));
    }
    let max_keys = normalize_max_keys(max_keys);

    Ok(ListObjectVersionsParams {
        prefix,
        delimiter,
        key_marker,
        version_id_marker,
        max_keys,
    })
}

pub(crate) fn parse_list_objects_v2_params(
    prefix: Option<String>,
    delimiter: Option<String>,
    max_keys: Option<i32>,
    continuation_token: Option<String>,
    start_after: Option<String>,
) -> Result<ListObjectsV2Params, S3Error> {
    let prefix = prefix.unwrap_or_default();

    // Log debug info for prefixes with special characters to help diagnose encoding issues
    if prefix.contains([' ', '+', '%', '\n', '\r', '\0']) {
        debug!("LIST objects with special characters in prefix: {:?}", prefix);
    }

    let max_keys = max_keys.unwrap_or(S3_MAX_KEYS);
    if max_keys < 0 {
        return Err(S3Error::with_message(S3ErrorCode::InvalidArgument, "Invalid max keys".to_string()));
    }
    let max_keys = normalize_max_keys(max_keys);

    let delimiter = delimiter.filter(|v| !v.is_empty());

    // Save original start_after for response (per S3 API spec, must echo back if provided)
    let response_start_after = start_after.clone();
    let start_after_for_query = start_after.filter(|v| !v.is_empty());

    // Save original continuation_token for response (per S3 API spec, must echo back if provided)
    let response_continuation_token = continuation_token.clone();

    let decoded_continuation_token = continuation_token
        .filter(|token| !token.is_empty())
        .map(|token| decode_list_objects_v2_continuation_token(&token))
        .transpose()?;

    Ok(ListObjectsV2Params {
        prefix,
        max_keys,
        delimiter,
        response_start_after,
        start_after_for_query,
        response_continuation_token,
        decoded_continuation_token,
    })
}

#[cfg(test)]
mod tests {
    use super::{encode_list_objects_v2_continuation_token, parse_list_object_versions_params, parse_list_objects_v2_params};
    use s3s::S3ErrorCode;

    // A key that literally ends in the retired metacache tag survives the V2
    // round trip unchanged: the API layer no longer strips anything.
    #[test]
    fn test_parse_list_objects_v2_params_round_trips_a_legacy_looking_literal_key() {
        let key = "b[rustfs_cache:v1,return:]";
        let wire = encode_list_objects_v2_continuation_token(key);

        let parsed = parse_list_objects_v2_params(None, None, Some(2), Some(wire), None).expect("parse should succeed");

        assert_eq!(parsed.decoded_continuation_token.as_deref(), Some(key));
    }

    #[test]
    fn test_parse_list_object_versions_params_defaults_and_filters_empty_values() {
        let parsed = parse_list_object_versions_params(None, Some(String::new()), Some(String::new()), None, None)
            .expect("parse should succeed");

        assert_eq!(parsed.prefix, String::new());
        assert_eq!(parsed.delimiter, None);
        assert_eq!(parsed.key_marker, None);
        assert_eq!(parsed.version_id_marker, None);
        assert_eq!(parsed.max_keys, 1000);
    }

    #[test]
    fn test_parse_list_object_versions_params_rejects_negative_max_keys() {
        let err = parse_list_object_versions_params(None, None, None, None, Some(-1))
            .expect_err("negative max_keys should be rejected");

        assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn test_parse_list_objects_v2_params_defaults_and_echo_behavior() {
        let parsed = parse_list_objects_v2_params(None, Some(String::new()), None, None, Some(String::new()))
            .expect("parse should succeed");

        assert_eq!(parsed.prefix, String::new());
        assert_eq!(parsed.max_keys, 1000);
        assert_eq!(parsed.delimiter, None);
        assert_eq!(parsed.response_start_after, Some(String::new()));
        assert_eq!(parsed.start_after_for_query, None);
        assert_eq!(parsed.response_continuation_token, None);
        assert_eq!(parsed.decoded_continuation_token, None);
    }

    #[test]
    fn test_parse_list_objects_v2_params_caps_large_max_keys() {
        let parsed = parse_list_objects_v2_params(None, None, Some(1001), None, None).expect("parse should succeed");

        assert_eq!(parsed.max_keys, 1000);
    }

    #[test]
    fn test_parse_list_object_versions_params_caps_large_max_keys() {
        let parsed = parse_list_object_versions_params(None, None, None, None, Some(1001)).expect("parse should succeed");

        assert_eq!(parsed.max_keys, 1000);
    }

    #[test]
    fn test_parse_list_objects_v2_params_rejects_negative_max_keys() {
        let err =
            parse_list_objects_v2_params(None, None, Some(-1), None, None).expect_err("negative max_keys should be rejected");

        assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn test_parse_list_objects_v2_params_preserves_empty_continuation_token() {
        let parsed = parse_list_objects_v2_params(None, None, Some(1000), Some(String::new()), None)
            .expect("empty continuation token should be accepted");

        assert_eq!(parsed.response_continuation_token, Some(String::new()));
        assert_eq!(parsed.decoded_continuation_token, None);
    }

    // A token issued by a 1.0.x server is base64 of the marker with the metacache
    // tag appended; it still paginates, and the tag is left for ecstore to strip.
    #[test]
    fn test_parse_list_objects_v2_params_decodes_plain_and_legacy_tagged_tokens() {
        for raw in ["token-123", "key-998[rustfs_cache:v2,id:list-cache-id,src:walker,gen:live]"] {
            let encoded = base64_simd::STANDARD.encode_to_string(raw.as_bytes());

            let parsed =
                parse_list_objects_v2_params(None, None, Some(1000), Some(encoded.clone()), None).expect("parse should succeed");

            assert_eq!(parsed.response_continuation_token, Some(encoded));
            assert_eq!(parsed.decoded_continuation_token.as_deref(), Some(raw));
        }
    }

    #[test]
    fn test_parse_list_objects_v2_params_rejects_invalid_continuation_token() {
        let err = parse_list_objects_v2_params(None, None, Some(1000), Some("%%%".to_string()), None)
            .expect_err("invalid base64 token should be rejected");

        assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn test_parse_list_objects_v2_params_rejects_non_utf8_continuation_token() {
        let invalid_utf8 = base64_simd::STANDARD.encode_to_string([0xff, 0xfe, 0xfd]);
        let err = parse_list_objects_v2_params(None, None, Some(1000), Some(invalid_utf8), None)
            .expect_err("non-utf8 token should be rejected");

        assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);
    }
}
