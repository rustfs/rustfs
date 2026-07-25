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

use percent_encoding::percent_decode_str;
use s3s::dto::{Tag, TaggingDirective};
use s3s::{S3Error, S3ErrorCode, S3Result};
use std::collections::HashSet;
use std::sync::OnceLock;
use url::form_urlencoded;

use crate::storage::storage_api::encode_tags;

fn invalid_tag_error(message: &str) -> S3Error {
    S3Error::with_message(S3ErrorCode::InvalidTag, message.to_string())
}

fn is_valid_tag_text(value: &str) -> bool {
    static VALID_OBJECT_TAG_TEXT: OnceLock<Result<regex::Regex, regex::Error>> = OnceLock::new();
    VALID_OBJECT_TAG_TEXT
        .get_or_init(|| regex::Regex::new(r"^[\p{L}\p{N}\p{Z}+\-=._:/@]*$"))
        .as_ref()
        .is_ok_and(|pattern| pattern.is_match(value))
}

pub(crate) fn validate_object_tag_set(tag_set: &[Tag]) -> S3Result<()> {
    if tag_set.len() > 10 {
        return Err(invalid_tag_error("Cannot have more than 10 tags per object"));
    }

    let mut tag_keys = HashSet::with_capacity(tag_set.len());
    for tag in tag_set {
        let key = tag
            .key
            .as_deref()
            .filter(|key| !key.is_empty())
            .ok_or_else(|| invalid_tag_error("Tag key cannot be empty"))?;

        if key.encode_utf16().count() > 128 || !is_valid_tag_text(key) {
            return Err(invalid_tag_error("The TagKey you have provided is invalid"));
        }

        let value = tag
            .value
            .as_deref()
            .ok_or_else(|| invalid_tag_error("Tag value cannot be null"))?;

        if value.encode_utf16().count() > 256 || !is_valid_tag_text(value) {
            return Err(invalid_tag_error("The TagValue you have provided is invalid"));
        }

        if !tag_keys.insert(key) {
            return Err(invalid_tag_error("Cannot provide multiple Tags with the same key"));
        }
    }

    Ok(())
}

fn validate_tag_component_encoding(component: &str) -> S3Result<()> {
    let bytes = component.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() || !bytes[index + 1].is_ascii_hexdigit() || !bytes[index + 2].is_ascii_hexdigit() {
                return Err(invalid_tag_error("The Tagging header contains invalid percent encoding"));
            }
            index += 3;
        } else {
            index += 1;
        }
    }

    percent_decode_str(component)
        .decode_utf8()
        .map_err(|_| invalid_tag_error("The Tagging header contains invalid UTF-8"))?;
    Ok(())
}

pub(crate) fn parse_object_tag_header(tagging: &str) -> S3Result<Vec<Tag>> {
    let mut tag_set = Vec::with_capacity(10);
    for pair in tagging.split('&').filter(|pair| !pair.is_empty()) {
        if tag_set.len() == 10 {
            return Err(invalid_tag_error("Cannot have more than 10 tags per object"));
        }

        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        validate_tag_component_encoding(key)?;
        validate_tag_component_encoding(value)?;
        let mut parsed = form_urlencoded::parse(pair.as_bytes());
        if let Some((key, value)) = parsed.next() {
            tag_set.push(Tag {
                key: Some(key.into_owned()),
                value: Some(value.into_owned()),
            });
        }
    }

    validate_object_tag_set(&tag_set)?;
    Ok(tag_set)
}

pub(crate) fn parse_copy_object_tags(tagging: &str) -> S3Result<String> {
    parse_object_tag_header(tagging).map(encode_tags)
}

pub(crate) fn resolve_copy_object_tags(
    tagging: Option<&str>,
    tagging_directive: Option<&TaggingDirective>,
) -> S3Result<Option<String>> {
    match tagging_directive.map(TaggingDirective::as_str) {
        None | Some(TaggingDirective::COPY) => {
            if tagging.is_some() {
                return Err(S3Error::with_message(
                    S3ErrorCode::InvalidRequest,
                    "The Tagging header requires the REPLACE tagging directive".to_string(),
                ));
            }
            Ok(None)
        }
        Some(TaggingDirective::REPLACE) => parse_copy_object_tags(tagging.unwrap_or_default()).map(Some),
        Some(_) => Err(S3Error::with_message(
            S3ErrorCode::InvalidArgument,
            "The TaggingDirective header is invalid".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_copy_object_tags, resolve_copy_object_tags, validate_object_tag_set};
    use s3s::S3ErrorCode;
    use s3s::dto::{Tag, TaggingDirective};

    fn tag(key: Option<&str>, value: Option<&str>) -> Tag {
        Tag {
            key: key.map(ToOwned::to_owned),
            value: value.map(ToOwned::to_owned),
        }
    }

    #[test]
    fn test_validate_object_tag_set_accepts_valid_tag_set() {
        let tag_set = vec![tag(Some("k1"), Some("v1")), tag(Some("k2"), Some(""))];
        let result = validate_object_tag_set(&tag_set);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_object_tag_set_rejects_more_than_ten_tags() {
        let tag_set: Vec<Tag> = (0..11).map(|i| tag(Some(&format!("k{i}")), Some(&format!("v{i}")))).collect();

        let err = validate_object_tag_set(&tag_set).expect_err("tag set with more than ten tags must be rejected");
        assert_eq!(*err.code(), S3ErrorCode::InvalidTag);
        assert!(err.to_string().contains("Cannot have more than 10 tags per object"));
    }

    #[test]
    fn test_validate_object_tag_set_rejects_empty_tag_key() {
        let err = validate_object_tag_set(&[tag(Some(""), Some("v1"))]).expect_err("empty tag key must be rejected");
        assert_eq!(*err.code(), S3ErrorCode::InvalidTag);
        assert!(err.to_string().contains("Tag key cannot be empty"));
    }

    #[test]
    fn test_validate_object_tag_set_rejects_too_long_tag_key() {
        let long_key = "k".repeat(129);
        let err = validate_object_tag_set(&[tag(Some(&long_key), Some("v1"))]).expect_err("too long tag key must be rejected");
        assert_eq!(*err.code(), S3ErrorCode::InvalidTag);
        assert!(err.to_string().contains("The TagKey you have provided is invalid"));
    }

    #[test]
    fn test_validate_object_tag_set_rejects_null_tag_value() {
        let err = validate_object_tag_set(&[tag(Some("k1"), None)]).expect_err("null tag value must be rejected");
        assert_eq!(*err.code(), S3ErrorCode::InvalidTag);
        assert!(err.to_string().contains("Tag value cannot be null"));
    }

    #[test]
    fn test_validate_object_tag_set_rejects_too_long_tag_value() {
        let long_value = "v".repeat(257);
        let err =
            validate_object_tag_set(&[tag(Some("k1"), Some(&long_value))]).expect_err("too long tag value must be rejected");
        assert_eq!(*err.code(), S3ErrorCode::InvalidTag);
        assert!(err.to_string().contains("The TagValue you have provided is invalid"));
    }

    #[test]
    fn test_validate_object_tag_set_rejects_duplicate_tag_key() {
        let err = validate_object_tag_set(&[tag(Some("k1"), Some("v1")), tag(Some("k1"), Some("v2"))])
            .expect_err("duplicate tag key must be rejected");
        assert_eq!(*err.code(), S3ErrorCode::InvalidTag);
        assert!(err.to_string().contains("Cannot provide multiple Tags with the same key"));
    }

    #[test]
    fn test_validate_object_tag_set_accepts_maximum_lengths_and_allowed_characters() {
        let key = "𐐀".repeat(64);
        let value = format!("{}=", "v".repeat(255));

        assert!(validate_object_tag_set(&[tag(Some(&key), Some(&value))]).is_ok());
    }

    #[test]
    fn test_validate_object_tag_set_counts_utf16_code_units() {
        let too_long_key = "𐐀".repeat(65);
        let err = validate_object_tag_set(&[tag(Some(&too_long_key), Some("value"))])
            .expect_err("a key longer than 128 UTF-16 code units must be rejected");

        assert_eq!(*err.code(), S3ErrorCode::InvalidTag);
    }

    #[test]
    fn test_validate_object_tag_set_rejects_unsupported_characters() {
        let key_err = validate_object_tag_set(&[tag(Some("invalid?key"), Some("value"))]).expect_err("unsupported key character");
        assert_eq!(*key_err.code(), S3ErrorCode::InvalidTag);

        let value_err =
            validate_object_tag_set(&[tag(Some("key"), Some("invalid&value"))]).expect_err("unsupported value character");
        assert_eq!(*value_err.code(), S3ErrorCode::InvalidTag);

        let combining_mark_err = validate_object_tag_set(&[tag(Some("invalid\u{0345}key"), Some("value"))])
            .expect_err("a combining mark outside the S3 letter/number/separator categories must be rejected");
        assert_eq!(*combining_mark_err.code(), S3ErrorCode::InvalidTag);
    }

    #[test]
    fn test_parse_copy_object_tags_accepts_encoded_tag_set() {
        let tags =
            parse_copy_object_tags("project=rustfs&label=copy%20test").expect("valid URL-encoded CopyObject tags should parse");

        assert_eq!(tags, "project=rustfs&label=copy+test");
    }

    #[test]
    fn test_parse_copy_object_tags_accepts_empty_replacement() {
        let tags = parse_copy_object_tags("").expect("an empty replacement tag set should parse");

        assert!(tags.is_empty());
    }

    #[test]
    fn test_parse_copy_object_tags_rejects_invalid_percent_encoding() {
        let err = parse_copy_object_tags("project=rustfs%ZZ")
            .expect_err("invalid percent encoding must be rejected before CopyObject writes");

        assert_eq!(*err.code(), S3ErrorCode::InvalidTag);
    }

    #[test]
    fn test_parse_copy_object_tags_rejects_duplicate_keys() {
        let err = parse_copy_object_tags("project=rustfs&project=cli").expect_err("duplicate tag keys must be rejected");

        assert_eq!(*err.code(), S3ErrorCode::InvalidTag);
    }

    #[test]
    fn test_parse_copy_object_tags_stops_at_object_tag_limit() {
        let tagging = (0..11)
            .map(|index| format!("k{index}=v{index}"))
            .collect::<Vec<_>>()
            .join("&");
        let err = parse_copy_object_tags(&tagging).expect_err("the eleventh object tag must be rejected");

        assert_eq!(*err.code(), S3ErrorCode::InvalidTag);
        assert!(err.to_string().contains("Cannot have more than 10 tags per object"));
    }

    #[test]
    fn test_parse_copy_object_tags_rejects_control_whitespace() {
        let err = parse_copy_object_tags("project=line%0Abreak").expect_err("control whitespace is not valid in S3 object tags");

        assert_eq!(*err.code(), S3ErrorCode::InvalidTag);
    }

    #[test]
    fn test_resolve_copy_object_tags_preserves_source_for_default_and_copy() {
        assert_eq!(
            resolve_copy_object_tags(None, None).expect("default directive should preserve source tags"),
            None
        );
        let copy = TaggingDirective::from_static(TaggingDirective::COPY);
        assert_eq!(
            resolve_copy_object_tags(None, Some(&copy)).expect("COPY directive should preserve source tags"),
            None
        );
    }

    #[test]
    fn test_resolve_copy_object_tags_replaces_with_exact_requested_set() {
        let replace = TaggingDirective::from_static(TaggingDirective::REPLACE);
        assert_eq!(
            resolve_copy_object_tags(Some("project=rustfs"), Some(&replace))
                .expect("REPLACE directive should accept a valid tag set"),
            Some("project=rustfs".to_string())
        );
        assert_eq!(
            resolve_copy_object_tags(None, Some(&replace)).expect("REPLACE without Tagging should clear tags"),
            Some(String::new())
        );
    }

    #[test]
    fn test_resolve_copy_object_tags_rejects_discarded_or_unknown_requests() {
        let err = resolve_copy_object_tags(Some("project=rustfs"), None)
            .expect_err("a Tagging header without REPLACE must not be silently discarded");
        assert_eq!(*err.code(), S3ErrorCode::InvalidRequest);

        let unknown = TaggingDirective::from_static("UNKNOWN");
        let err = resolve_copy_object_tags(None, Some(&unknown)).expect_err("an unknown tagging directive must fail");
        assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);
    }
}
