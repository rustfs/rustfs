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

use s3s::dto::{
    DeleteBucketTaggingOutput, DeleteObjectTaggingOutput, GetBucketTaggingOutput, GetObjectTaggingOutput, PutBucketTaggingOutput,
    PutObjectTaggingOutput, Tag,
};
use s3s::{S3Error, S3ErrorCode, S3Result};
use std::collections::HashSet;

fn invalid_tag_error(message: &str) -> S3Error {
    S3Error::with_message(S3ErrorCode::InvalidTag, message.to_string())
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

        if key.len() > 128 {
            return Err(invalid_tag_error("Tag key is too long, maximum allowed length is 128 characters"));
        }

        let value = tag
            .value
            .as_deref()
            .ok_or_else(|| invalid_tag_error("Tag value cannot be null"))?;

        if value.len() > 256 {
            return Err(invalid_tag_error("Tag value is too long, maximum allowed length is 256 characters"));
        }

        if !tag_keys.insert(key) {
            return Err(invalid_tag_error("Cannot provide multiple Tags with the same key"));
        }
    }

    Ok(())
}

pub(crate) fn build_get_bucket_tagging_output(tag_set: Vec<Tag>) -> GetBucketTaggingOutput {
    GetBucketTaggingOutput { tag_set }
}

pub(crate) fn build_get_object_tagging_output(tag_set: Vec<Tag>, version_id: Option<String>) -> GetObjectTaggingOutput {
    GetObjectTaggingOutput { tag_set, version_id }
}

pub(crate) fn build_put_object_tagging_output(version_id: Option<String>) -> PutObjectTaggingOutput {
    PutObjectTaggingOutput { version_id }
}

pub(crate) fn build_delete_object_tagging_output(version_id: Option<String>) -> DeleteObjectTaggingOutput {
    DeleteObjectTaggingOutput { version_id }
}

pub(crate) fn build_put_bucket_tagging_output() -> PutBucketTaggingOutput {
    PutBucketTaggingOutput::default()
}

pub(crate) fn build_delete_bucket_tagging_output() -> DeleteBucketTaggingOutput {
    DeleteBucketTaggingOutput {}
}

#[cfg(test)]
mod tests {
    use super::{
        build_delete_bucket_tagging_output, build_delete_object_tagging_output, build_get_bucket_tagging_output,
        build_get_object_tagging_output, build_put_bucket_tagging_output, build_put_object_tagging_output,
        validate_object_tag_set,
    };
    use s3s::S3ErrorCode;
    use s3s::dto::{DeleteBucketTaggingOutput, Tag};

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
        assert!(
            err.to_string()
                .contains("Tag key is too long, maximum allowed length is 128 characters")
        );
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
        assert!(
            err.to_string()
                .contains("Tag value is too long, maximum allowed length is 256 characters")
        );
    }

    #[test]
    fn test_validate_object_tag_set_rejects_duplicate_tag_key() {
        let err = validate_object_tag_set(&[tag(Some("k1"), Some("v1")), tag(Some("k1"), Some("v2"))])
            .expect_err("duplicate tag key must be rejected");
        assert_eq!(*err.code(), S3ErrorCode::InvalidTag);
        assert!(err.to_string().contains("Cannot provide multiple Tags with the same key"));
    }

    #[test]
    fn test_build_tagging_outputs_preserve_fields() {
        let tag_set = vec![tag(Some("k1"), Some("v1"))];
        let version_id = Some("vid-1".to_string());

        let bucket_output = build_get_bucket_tagging_output(tag_set.clone());
        let get_object_output = build_get_object_tagging_output(tag_set.clone(), version_id.clone());
        let put_object_output = build_put_object_tagging_output(version_id.clone());
        let delete_object_output = build_delete_object_tagging_output(version_id.clone());

        assert_eq!(bucket_output.tag_set, tag_set);
        assert_eq!(get_object_output.tag_set, vec![tag(Some("k1"), Some("v1"))]);
        assert_eq!(get_object_output.version_id, version_id);
        assert_eq!(put_object_output.version_id, Some("vid-1".to_string()));
        assert_eq!(delete_object_output.version_id, Some("vid-1".to_string()));
    }

    #[test]
    fn test_build_bucket_tagging_outputs_are_default_shape() {
        let put_output = build_put_bucket_tagging_output();
        let delete_output = build_delete_bucket_tagging_output();

        assert_eq!(put_output, Default::default());
        assert_eq!(delete_output, DeleteBucketTaggingOutput {});
    }
}
