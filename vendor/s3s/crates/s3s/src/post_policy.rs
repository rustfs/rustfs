//! POST Object policy parsing and validation.
//!
//! A POST policy is a base64-encoded JSON document attached to a browser-based
//! upload that restricts what the form fields may contain.
//!
//! See <https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-HTTPPOSTConstructPolicy.html>

use crate::S3Error;
use crate::S3ErrorCode;
use crate::S3Result;
use crate::dto::Timestamp;
use crate::dto::TimestampFormat;
use crate::http::Multipart;

use std::borrow::Cow;
use std::collections::HashMap;

use serde::Deserialize;
use serde::de::{Deserializer, MapAccess, SeqAccess, Visitor};

/// POST Object Policy
///
/// A POST policy is a JSON document that specifies conditions
/// that the request must meet when uploading objects using POST.
#[derive(Debug, Clone)]
pub struct PostPolicy {
    /// The expiration date of the policy in ISO 8601 format
    pub expiration: Timestamp,
    /// The conditions that must be met for the upload to succeed
    pub conditions: Vec<PostPolicyCondition>,
}

/// A condition in the POST policy
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PostPolicyCondition {
    /// Exact match condition: the field value must equal the specified value
    Eq {
        /// Field name (without '$' prefix, lowercase)
        field: String,
        /// Expected value
        value: String,
    },
    /// Prefix match condition: the field value must start with the specified prefix
    StartsWith {
        /// Field name (without '$' prefix, lowercase)
        field: String,
        /// Expected prefix (empty string matches any value)
        prefix: String,
    },
    /// Content length range condition
    ContentLengthRange {
        /// Minimum content length (inclusive)
        min: u64,
        /// Maximum content length (inclusive)
        max: u64,
    },
}

/// Error type for POST policy parsing
#[derive(Debug, thiserror::Error)]
pub enum PostPolicyError {
    #[error("invalid base64 encoding: {0}")]
    Base64(#[from] base64_simd::Error),
    #[error("invalid UTF-8 encoding: {0}")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("missing required field: {0}")]
    MissingField(&'static str),
    #[error("invalid condition format")]
    InvalidCondition,
    #[error("invalid expiration format: {0}")]
    InvalidExpiration(String),
}

impl PostPolicy {
    /// Parse a POST policy from a base64-encoded JSON string
    ///
    /// # Errors
    /// Returns an error if the base64 decoding or JSON parsing fails
    pub fn from_base64(encoded: &str) -> Result<Self, PostPolicyError> {
        let decoded = base64_simd::STANDARD.decode_to_vec(encoded)?;
        let json_str = std::str::from_utf8(&decoded)?;
        Self::from_json(json_str)
    }

    /// Parse a POST policy from a JSON string
    ///
    /// # Errors
    /// Returns an error if the JSON parsing fails
    pub fn from_json(json: &str) -> Result<Self, PostPolicyError> {
        let raw: RawPostPolicy = serde_json::from_str(json)?;

        let expiration = Timestamp::parse(TimestampFormat::DateTime, &raw.expiration)
            .map_err(|_| PostPolicyError::InvalidExpiration(raw.expiration.clone()))?;

        let conditions = raw
            .conditions
            .into_iter()
            .map(RawCondition::into_condition)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self { expiration, conditions })
    }

    /// Validate only the policy conditions, skipping the expiration check.
    ///
    /// When `url_bucket` is provided it is authoritative for the `bucket`
    /// policy condition. If a `bucket` form field is also present but differs
    /// from `url_bucket`, the request is rejected with `InvalidPolicyDocument`.
    ///
    /// # Arguments
    /// * `multipart` - The multipart form data
    /// * `file_size` - The size of the uploaded file in bytes
    /// * `url_bucket` - The bucket name from the URL path/host. When present,
    ///   it is the source of truth for the `bucket` condition.
    ///
    /// # Errors
    /// Returns `InvalidPolicyDocument` if any condition is not satisfied
    pub(crate) fn validate_conditions_only(
        &self,
        multipart: &Multipart,
        file_size: u64,
        url_bucket: Option<&str>,
    ) -> S3Result<()> {
        // Check all conditions against form fields
        for condition in &self.conditions {
            Self::validate_condition(condition, multipart, file_size, url_bucket)?;
        }

        // Check that every form field is covered by a policy condition.
        // Per AWS SigV4 docs, these fields are exempt:
        //   x-amz-signature, file, submit, policy, and x-ignore-* fields.
        // For SigV2 POSTs, we also exempt: signature and awsaccesskeyid.
        // See https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-HTTPPOSTConstructPolicy.html
        for (name, _) in multipart.fields() {
            if Self::is_field_exempt_from_policy(name) {
                continue;
            }
            if !self.has_condition_for_field(name) {
                return Err(S3Error::with_message(
                    S3ErrorCode::AccessDenied,
                    format!(
                        "Each form field that you specify in a form must appear in the list \
                         of policy conditions. \"{name}\" not specified in the policy."
                    ),
                ));
            }
        }

        Ok(())
    }

    /// Returns true if the given form field is exempt from the policy condition requirement.
    fn is_field_exempt_from_policy(name: &str) -> bool {
        // SigV4: x-amz-signature; SigV2: signature, awsaccesskeyid.
        // Keep the MinIO-compatible exception set for encryption-specific fields,
        // which are allowed outside policy conditions for browser POST uploads.
        matches!(name, "x-amz-signature" | "signature" | "awsaccesskeyid" | "file" | "submit" | "policy")
            || name.starts_with("x-amz-server-side-encryption-")
            || name.starts_with("x-ignore-")
    }

    /// Returns true if the policy contains at least one `Eq` or `StartsWith` condition for the field.
    fn has_condition_for_field(&self, name: &str) -> bool {
        self.conditions.iter().any(|c| match c {
            PostPolicyCondition::Eq { field, .. } | PostPolicyCondition::StartsWith { field, .. } => field == name,
            PostPolicyCondition::ContentLengthRange { .. } => false,
        })
    }

    fn validate_condition(
        condition: &PostPolicyCondition,
        multipart: &Multipart,
        file_size: u64,
        url_bucket: Option<&str>,
    ) -> S3Result<()> {
        match condition {
            PostPolicyCondition::Eq { field, value } => {
                let actual = Self::get_field_value(field, multipart, url_bucket)?;
                if actual.as_deref() != Some(value.as_str()) {
                    return Err(S3Error::with_message(
                        S3ErrorCode::InvalidPolicyDocument,
                        format!(
                            "Policy condition 'eq' for field '{field}' failed: expected '{value}', got '{}'",
                            actual.as_deref().unwrap_or_default()
                        ),
                    ));
                }
            }
            PostPolicyCondition::StartsWith { field, prefix } => {
                let actual = Self::get_field_value(field, multipart, url_bucket)?;
                let actual_str = actual.as_deref().unwrap_or("");
                if !actual_str.starts_with(prefix.as_str()) {
                    return Err(S3Error::with_message(
                        S3ErrorCode::InvalidPolicyDocument,
                        format!(
                            "Policy condition 'starts-with' for field '{field}' failed: expected prefix '{prefix}', got '{actual_str}'"
                        ),
                    ));
                }
            }
            PostPolicyCondition::ContentLengthRange { min, max } => {
                if file_size > *max {
                    return Err(s3_error!(EntityTooLarge, "Your proposed upload exceeds the maximum allowed object size."));
                }
                if file_size < *min {
                    return Err(s3_error!(
                        EntityTooSmall,
                        "Your proposed upload is smaller than the minimum allowed object size."
                    ));
                }
            }
        }
        Ok(())
    }

    /// Resolve the effective value for a policy condition field.
    ///
    /// For the `bucket` field:
    /// - If `url_bucket` is provided it is the authoritative value.
    /// - If a `bucket` form field is also present and differs from `url_bucket`,
    ///   the request is rejected with `InvalidPolicyDocument` rather than
    ///   silently discarding one of them.
    /// - If `url_bucket` is absent, fall back to the form field.
    ///
    /// For all other fields only the multipart form fields are consulted.
    fn get_field_value<'a>(field: &str, multipart: &'a Multipart, url_bucket: Option<&'a str>) -> S3Result<Option<Cow<'a, str>>> {
        if field == "bucket" {
            let form_bucket = multipart.find_field_value(field);
            if let Some(ub) = url_bucket {
                // If the client also sent a `bucket` form field it must agree
                // with the URL bucket; a mismatch is an unambiguous error.
                if let Some(fb) = form_bucket
                    && fb != ub
                {
                    return Err(s3_error!(
                        InvalidPolicyDocument,
                        "Bucket in form field '{fb}' does not match bucket in URL '{ub}'"
                    ));
                }
                return Ok(Some(Cow::Borrowed(ub)));
            }
            return Ok(form_bucket.map(Cow::Borrowed));
        }

        Ok(multipart.find_field_value(field).map(Cow::Borrowed))
    }

    /// Get the content-length-range condition if present
    #[must_use]
    pub fn content_length_range(&self) -> Option<(u64, u64)> {
        for condition in &self.conditions {
            if let PostPolicyCondition::ContentLengthRange { min, max } = condition {
                return Some((*min, *max));
            }
        }
        None
    }
}

/// Raw POST policy for deserialization
#[derive(Debug, Deserialize)]
struct RawPostPolicy {
    expiration: String,
    conditions: Vec<RawCondition>,
}

/// Raw condition that can be either array format or object format
#[derive(Debug)]
enum RawCondition {
    /// Array format: `["eq", "$key", "value"]` or `["starts-with", "$key", "prefix"]` or `["content-length-range", min, max]`
    Array(Vec<serde_json::Value>),
    /// Object format: `{"bucket": "mybucket"}` (shorthand for eq)
    Object(HashMap<String, String>),
}

impl<'de> Deserialize<'de> for RawCondition {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RawConditionVisitor;

        impl<'de> Visitor<'de> for RawConditionVisitor {
            type Value = RawCondition;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an array or object")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut items = Vec::new();
                while let Some(item) = seq.next_element()? {
                    items.push(item);
                }
                Ok(RawCondition::Array(items))
            }

            fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut items = HashMap::new();
                while let Some((key, value)) = map.next_entry::<String, String>()? {
                    items.insert(key, value);
                }
                Ok(RawCondition::Object(items))
            }
        }

        deserializer.deserialize_any(RawConditionVisitor)
    }
}

impl RawCondition {
    fn into_condition(self) -> Result<PostPolicyCondition, PostPolicyError> {
        match self {
            RawCondition::Array(items) => Self::parse_array_condition(&items),
            RawCondition::Object(map) => Self::parse_object_condition(map),
        }
    }

    fn parse_array_condition(items: &[serde_json::Value]) -> Result<PostPolicyCondition, PostPolicyError> {
        if items.is_empty() {
            return Err(PostPolicyError::InvalidCondition);
        }

        let operator = items[0].as_str().ok_or(PostPolicyError::InvalidCondition)?;

        match operator.to_ascii_lowercase().as_str() {
            "eq" => {
                if items.len() != 3 {
                    return Err(PostPolicyError::InvalidCondition);
                }
                let field = items[1].as_str().ok_or(PostPolicyError::InvalidCondition)?;
                let value = items[2].as_str().ok_or(PostPolicyError::InvalidCondition)?;
                Ok(PostPolicyCondition::Eq {
                    field: normalize_field_name(field),
                    value: value.to_owned(),
                })
            }
            "starts-with" => {
                if items.len() != 3 {
                    return Err(PostPolicyError::InvalidCondition);
                }
                let field = items[1].as_str().ok_or(PostPolicyError::InvalidCondition)?;
                let prefix = items[2].as_str().ok_or(PostPolicyError::InvalidCondition)?;
                Ok(PostPolicyCondition::StartsWith {
                    field: normalize_field_name(field),
                    prefix: prefix.to_owned(),
                })
            }
            "content-length-range" => {
                if items.len() != 3 {
                    return Err(PostPolicyError::InvalidCondition);
                }
                let min = items[1].as_u64().ok_or(PostPolicyError::InvalidCondition)?;
                let max = items[2].as_u64().ok_or(PostPolicyError::InvalidCondition)?;
                if min > max {
                    return Err(PostPolicyError::InvalidCondition);
                }
                Ok(PostPolicyCondition::ContentLengthRange { min, max })
            }
            _ => Err(PostPolicyError::InvalidCondition),
        }
    }

    fn parse_object_condition(map: HashMap<String, String>) -> Result<PostPolicyCondition, PostPolicyError> {
        // Object format is shorthand for exact match
        // {"bucket": "mybucket"} means ["eq", "$bucket", "mybucket"]
        if map.len() != 1 {
            return Err(PostPolicyError::InvalidCondition);
        }
        let (field, value) = map.into_iter().next().ok_or(PostPolicyError::InvalidCondition)?;
        Ok(PostPolicyCondition::Eq {
            field: normalize_field_name(&field),
            value,
        })
    }
}

/// Normalize field name by removing '$' prefix and converting to lowercase
fn normalize_field_name(field: &str) -> String {
    let field = field.strip_prefix('$').unwrap_or(field);
    field.to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_timestamp(s: &str) -> Timestamp {
        Timestamp::parse(TimestampFormat::DateTime, s).unwrap()
    }

    #[test]
    fn test_parse_policy_json() {
        let json = r#"{
            "expiration": "2030-01-01T00:00:00.000Z",
            "conditions": [
                ["eq", "$bucket", "mybucket"],
                ["starts-with", "$key", "user/"],
                ["content-length-range", 0, 10485760],
                {"acl": "public-read"}
            ]
        }"#;

        let policy = PostPolicy::from_json(json).unwrap();

        assert_eq!(policy.expiration, make_timestamp("2030-01-01T00:00:00.000Z"));
        assert_eq!(policy.conditions.len(), 4);

        assert_eq!(
            policy.conditions[0],
            PostPolicyCondition::Eq {
                field: "bucket".to_owned(),
                value: "mybucket".to_owned()
            }
        );

        assert_eq!(
            policy.conditions[1],
            PostPolicyCondition::StartsWith {
                field: "key".to_owned(),
                prefix: "user/".to_owned()
            }
        );

        assert_eq!(policy.conditions[2], PostPolicyCondition::ContentLengthRange { min: 0, max: 10_485_760 });

        assert_eq!(
            policy.conditions[3],
            PostPolicyCondition::Eq {
                field: "acl".to_owned(),
                value: "public-read".to_owned()
            }
        );
    }

    #[test]
    fn test_parse_policy_base64() {
        let json = r#"{"expiration":"2030-01-01T00:00:00.000Z","conditions":[["eq","$bucket","test"]]}"#;
        let encoded = base64_simd::STANDARD.encode_to_string(json);

        let policy = PostPolicy::from_base64(&encoded).unwrap();
        assert_eq!(policy.conditions.len(), 1);
    }

    #[test]
    fn test_parse_invalid_base64() {
        PostPolicy::from_base64("not-valid-base64!!!").unwrap_err();
    }

    #[test]
    fn test_parse_invalid_json() {
        let encoded = base64_simd::STANDARD.encode_to_string("{invalid json}");
        let error = PostPolicy::from_base64(&encoded).unwrap_err();
        assert!(matches!(error, PostPolicyError::Json(_)));
    }

    #[test]
    fn test_parse_invalid_expiration() {
        let json = r#"{"expiration":"not-a-date","conditions":[]}"#;
        let result = PostPolicy::from_json(json);
        assert!(matches!(result, Err(PostPolicyError::InvalidExpiration(_))));
    }

    #[test]
    fn test_parse_invalid_condition_format() {
        let json = r#"{"expiration":"2030-01-01T00:00:00.000Z","conditions":[["unknown-op","$key","value"]]}"#;
        let result = PostPolicy::from_json(json);
        assert!(matches!(result, Err(PostPolicyError::InvalidCondition)));
    }

    #[test]
    fn test_content_length_range() {
        let json = r#"{
            "expiration": "2030-01-01T00:00:00.000Z",
            "conditions": [
                ["content-length-range", 100, 1000]
            ]
        }"#;

        let policy = PostPolicy::from_json(json).unwrap();
        assert_eq!(policy.content_length_range(), Some((100, 1000)));
    }

    #[test]
    fn test_no_content_length_range() {
        let json = r#"{
            "expiration": "2030-01-01T00:00:00.000Z",
            "conditions": [
                ["eq", "$bucket", "test"]
            ]
        }"#;

        let policy = PostPolicy::from_json(json).unwrap();
        assert_eq!(policy.content_length_range(), None);
    }

    #[test]
    fn test_normalize_field_name() {
        assert_eq!(normalize_field_name("$bucket"), "bucket");
        assert_eq!(normalize_field_name("$Key"), "key");
        assert_eq!(normalize_field_name("bucket"), "bucket");
        assert_eq!(normalize_field_name("X-Amz-Meta-Custom"), "x-amz-meta-custom");
    }

    #[test]
    fn test_invalid_content_length_range_min_greater_than_max() {
        let json = r#"{
            "expiration": "2030-01-01T00:00:00.000Z",
            "conditions": [
                ["content-length-range", 1000, 100]
            ]
        }"#;

        let result = PostPolicy::from_json(json);
        assert!(matches!(result, Err(PostPolicyError::InvalidCondition)));
    }

    #[test]
    fn test_valid_content_length_range_equal() {
        let json = r#"{
            "expiration": "2030-01-01T00:00:00.000Z",
            "conditions": [
                ["content-length-range", 100, 100]
            ]
        }"#;

        let result = PostPolicy::from_json(json);
        assert!(result.is_ok());
        let policy = result.unwrap();
        assert_eq!(policy.content_length_range(), Some((100, 100)));
    }

    // Helper function to create a mock Multipart for testing
    fn create_test_multipart(fields: Vec<(&str, &str)>, content_type: Option<&str>) -> Multipart {
        use crate::http::File;

        let fields: Vec<(String, String)> = fields.into_iter().map(|(k, v)| (k.to_owned(), v.to_owned())).collect();

        let file = File {
            name: "test.txt".to_owned(),
            content_type: content_type.map(String::from),
            stream: None,
        };

        Multipart::new_for_test(fields, file)
    }

    #[test]
    fn test_validate_condition_eq_success() {
        let multipart = create_test_multipart(vec![("bucket", "mybucket"), ("key", "mykey")], None);
        let condition = PostPolicyCondition::Eq {
            field: "bucket".to_owned(),
            value: "mybucket".to_owned(),
        };

        let result = PostPolicy::validate_condition(&condition, &multipart, 0, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_condition_eq_failure() {
        let multipart = create_test_multipart(vec![("bucket", "mybucket"), ("key", "mykey")], None);
        let condition = PostPolicyCondition::Eq {
            field: "bucket".to_owned(),
            value: "wrongbucket".to_owned(),
        };

        let e = PostPolicy::validate_condition(&condition, &multipart, 0, None).unwrap_err();
        assert!(matches!(e.code(), S3ErrorCode::InvalidPolicyDocument));
    }

    #[test]
    fn test_validate_condition_starts_with_success() {
        let multipart = create_test_multipart(vec![("key", "user/alice/file.txt")], None);
        let condition = PostPolicyCondition::StartsWith {
            field: "key".to_owned(),
            prefix: "user/".to_owned(),
        };

        let result = PostPolicy::validate_condition(&condition, &multipart, 0, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_condition_starts_with_empty_prefix() {
        let multipart = create_test_multipart(vec![("key", "anyvalue")], None);
        let condition = PostPolicyCondition::StartsWith {
            field: "key".to_owned(),
            prefix: String::new(),
        };

        let result = PostPolicy::validate_condition(&condition, &multipart, 0, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_condition_starts_with_failure() {
        let multipart = create_test_multipart(vec![("key", "public/file.txt")], None);
        let condition = PostPolicyCondition::StartsWith {
            field: "key".to_owned(),
            prefix: "user/".to_owned(),
        };

        let e = PostPolicy::validate_condition(&condition, &multipart, 0, None).unwrap_err();
        assert!(matches!(e.code(), S3ErrorCode::InvalidPolicyDocument));
    }

    #[test]
    fn test_validate_condition_content_length_range_success() {
        let multipart = create_test_multipart(vec![], None);
        let condition = PostPolicyCondition::ContentLengthRange { min: 100, max: 1000 };

        let result = PostPolicy::validate_condition(&condition, &multipart, 500, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_condition_content_length_range_at_min() {
        let multipart = create_test_multipart(vec![], None);
        let condition = PostPolicyCondition::ContentLengthRange { min: 100, max: 1000 };

        let result = PostPolicy::validate_condition(&condition, &multipart, 100, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_condition_content_length_range_at_max() {
        let multipart = create_test_multipart(vec![], None);
        let condition = PostPolicyCondition::ContentLengthRange { min: 100, max: 1000 };

        let result = PostPolicy::validate_condition(&condition, &multipart, 1000, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_condition_content_length_range_too_small() {
        let multipart = create_test_multipart(vec![], None);
        let condition = PostPolicyCondition::ContentLengthRange { min: 100, max: 1000 };

        let e = PostPolicy::validate_condition(&condition, &multipart, 99, None).unwrap_err();
        assert!(matches!(e.code(), S3ErrorCode::EntityTooSmall));
    }

    #[test]
    fn test_validate_condition_content_length_range_too_large() {
        let multipart = create_test_multipart(vec![], None);
        let condition = PostPolicyCondition::ContentLengthRange { min: 100, max: 1000 };

        let e = PostPolicy::validate_condition(&condition, &multipart, 1001, None).unwrap_err();
        assert!(matches!(e.code(), S3ErrorCode::EntityTooLarge));
    }

    #[test]
    fn test_validate_condition_file_content_type() {
        let multipart = create_test_multipart(vec![], Some("image/jpeg"));
        let condition = PostPolicyCondition::Eq {
            field: "content-type".to_owned(),
            value: "image/jpeg".to_owned(),
        };

        let e = PostPolicy::validate_condition(&condition, &multipart, 0, None).unwrap_err();
        assert!(matches!(e.code(), S3ErrorCode::InvalidPolicyDocument));
    }

    #[test]
    fn test_validate_condition_field_content_type() {
        let multipart = create_test_multipart(vec![("Content-Type", "image/jpg")], Some("image/jpeg"));
        let condition = PostPolicyCondition::Eq {
            field: "content-type".to_owned(),
            value: "image/jpeg".to_owned(),
        };

        let e = PostPolicy::validate_condition(&condition, &multipart, 0, None).unwrap_err();
        assert!(matches!(e.code(), S3ErrorCode::InvalidPolicyDocument));
    }

    #[test]
    fn test_validate_condition_field_content_type_right() {
        let multipart = create_test_multipart(vec![("Content-Type", "image/jpeg")], Some("image/jpg"));
        let condition = PostPolicyCondition::Eq {
            field: "content-type".to_owned(),
            value: "image/jpeg".to_owned(),
        };

        let result = PostPolicy::validate_condition(&condition, &multipart, 0, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_condition_missing_field() {
        let multipart = create_test_multipart(vec![], None);
        let condition = PostPolicyCondition::Eq {
            field: "bucket".to_owned(),
            value: "mybucket".to_owned(),
        };

        let e = PostPolicy::validate_condition(&condition, &multipart, 0, None).unwrap_err();
        assert!(matches!(e.code(), S3ErrorCode::InvalidPolicyDocument));
    }

    /// Regression test for <https://github.com/rustfs/rustfs/issues/1785>
    /// Bucket condition should be validated against `url_bucket` when not in form fields.
    #[test]
    fn test_validate_bucket_condition_from_url() {
        // No "bucket" field in form data (like boto3 presigned POST)
        let multipart = create_test_multipart(vec![("key", "mykey")], None);
        let condition = PostPolicyCondition::Eq {
            field: "bucket".to_owned(),
            value: "mybucket".to_owned(),
        };

        // With matching url_bucket -> should succeed
        let result = PostPolicy::validate_condition(&condition, &multipart, 0, Some("mybucket"));
        assert!(result.is_ok());
    }

    /// Regression test for <https://github.com/rustfs/rustfs/issues/1785>
    /// Bucket condition should fail when `url_bucket` doesn't match.
    #[test]
    fn test_validate_bucket_condition_from_url_mismatch() {
        let multipart = create_test_multipart(vec![("key", "mykey")], None);
        let condition = PostPolicyCondition::Eq {
            field: "bucket".to_owned(),
            value: "mybucket".to_owned(),
        };

        // With mismatching url_bucket -> should fail
        let e = PostPolicy::validate_condition(&condition, &multipart, 0, Some("wrongbucket")).unwrap_err();
        assert!(matches!(e.code(), S3ErrorCode::InvalidPolicyDocument));
    }

    /// When both a `bucket` form field and `url_bucket` are present but conflict,
    /// `get_field_value` must return an error rather than silently choosing one.
    #[test]
    fn test_validate_bucket_form_field_conflicts_with_url_bucket() {
        // "bucket" IS in form data AND url_bucket is provided, and they differ
        let multipart = create_test_multipart(vec![("bucket", "form-bucket"), ("key", "mykey")], None);
        let condition = PostPolicyCondition::Eq {
            field: "bucket".to_owned(),
            value: "form-bucket".to_owned(),
        };

        // Conflict between form field and url_bucket must be rejected outright
        let e = PostPolicy::validate_condition(&condition, &multipart, 0, Some("url-bucket")).unwrap_err();
        assert!(matches!(e.code(), S3ErrorCode::InvalidPolicyDocument));
    }

    /// Regression test for <https://github.com/rustfs/rustfs/issues/1785>
    /// `validate_conditions_only` with `url_bucket` should work for boto3-style policies.
    #[test]
    fn test_validate_conditions_only_with_url_bucket() {
        let json = r#"{
            "expiration": "2030-01-01T00:00:00.000Z",
            "conditions": [
                {"bucket": "mybucket"},
                ["eq", "$key", "mykey"]
            ]
        }"#;
        let policy = PostPolicy::from_json(json).unwrap();

        // No bucket in form fields, but url_bucket matches
        let multipart = create_test_multipart(vec![("key", "mykey")], None);
        let result = policy.validate_conditions_only(&multipart, 0, Some("mybucket"));
        assert!(result.is_ok());
    }

    // Tests for RawCondition parsing error branches

    // Helper function to test multiple invalid JSON strings that should return InvalidCondition
    fn assert_invalid_condition(json_samples: &[&str]) {
        for json in json_samples {
            let error = PostPolicy::from_json(json).unwrap_err();
            assert!(
                matches!(error, PostPolicyError::InvalidCondition),
                "Expected InvalidCondition for: {json}"
            );
        }
    }

    // Helper function to test multiple invalid JSON strings that should return Json error
    fn assert_json_error(json_samples: &[&str]) {
        for json in json_samples {
            let error = PostPolicy::from_json(json).unwrap_err();
            assert!(matches!(error, PostPolicyError::Json(_)), "Expected Json error for: {json}");
        }
    }

    #[test]
    fn test_parse_array_condition_errors() {
        let invalid_jsons = vec![
            // Empty array
            r#"{"expiration": "2030-01-01T00:00:00.000Z", "conditions": [[]]}"#,
            // Operator not string
            r#"{"expiration": "2030-01-01T00:00:00.000Z", "conditions": [[123, "$key", "value"]]}"#,
            // Unknown operator
            r#"{"expiration": "2030-01-01T00:00:00.000Z", "conditions": [["unknown-op", "$key", "value"]]}"#,
        ];
        assert_invalid_condition(&invalid_jsons);
    }

    #[test]
    fn test_parse_eq_condition_errors() {
        let invalid_jsons = vec![
            // Insufficient items
            r#"{"expiration": "2030-01-01T00:00:00.000Z", "conditions": [["eq", "$key"]]}"#,
            // Field not string
            r#"{"expiration": "2030-01-01T00:00:00.000Z", "conditions": [["eq", 123, "value"]]}"#,
            // Value not string
            r#"{"expiration": "2030-01-01T00:00:00.000Z", "conditions": [["eq", "$key", 123]]}"#,
        ];
        assert_invalid_condition(&invalid_jsons);
    }

    #[test]
    fn test_parse_starts_with_condition_errors() {
        let invalid_jsons = vec![
            // Insufficient items
            r#"{"expiration": "2030-01-01T00:00:00.000Z", "conditions": [["starts-with", "$key"]]}"#,
            // Field not string
            r#"{"expiration": "2030-01-01T00:00:00.000Z", "conditions": [["starts-with", 123, "prefix"]]}"#,
            // Prefix not string
            r#"{"expiration": "2030-01-01T00:00:00.000Z", "conditions": [["starts-with", "$key", 123]]}"#,
        ];
        assert_invalid_condition(&invalid_jsons);
    }

    #[test]
    fn test_parse_content_length_range_condition_errors() {
        let invalid_jsons = vec![
            // Insufficient items
            r#"{"expiration": "2030-01-01T00:00:00.000Z", "conditions": [["content-length-range", 100]]}"#,
            // Min not number
            r#"{"expiration": "2030-01-01T00:00:00.000Z", "conditions": [["content-length-range", "100", 1000]]}"#,
            // Max not number
            r#"{"expiration": "2030-01-01T00:00:00.000Z", "conditions": [["content-length-range", 100, "1000"]]}"#,
        ];
        assert_invalid_condition(&invalid_jsons);
    }

    #[test]
    fn test_parse_object_condition_errors() {
        let invalid_jsons = vec![
            // Multiple keys
            r#"{"expiration": "2030-01-01T00:00:00.000Z", "conditions": [{"bucket": "mybucket", "key": "mykey"}]}"#,
        ];
        assert_invalid_condition(&invalid_jsons);
    }

    #[test]
    fn test_parse_object_condition_value_type_error() {
        let invalid_jsons = vec![
            // Value not string (serde deserialization error)
            r#"{"expiration": "2030-01-01T00:00:00.000Z", "conditions": [{"bucket": 123}]}"#,
        ];
        assert_json_error(&invalid_jsons);
    }

    #[test]
    fn test_parse_condition_invalid_types() {
        // Test RawCondition Deserialize expecting() when condition is neither array nor object
        let invalid_jsons = vec![
            // String instead of array/object
            r#"{"expiration": "2030-01-01T00:00:00.000Z", "conditions": ["invalid string"]}"#,
            // Number instead of array/object
            r#"{"expiration": "2030-01-01T00:00:00.000Z", "conditions": [123]}"#,
            // Boolean instead of array/object
            r#"{"expiration": "2030-01-01T00:00:00.000Z", "conditions": [true]}"#,
            // Null instead of array/object
            r#"{"expiration": "2030-01-01T00:00:00.000Z", "conditions": [null]}"#,
        ];
        assert_json_error(&invalid_jsons);
    }

    // ---- Tests for form-field-in-policy validation ----

    /// A form field not declared in any policy condition must be rejected with `AccessDenied`.
    #[test]
    fn test_form_field_not_in_policy_is_rejected() {
        let json = r#"{
            "expiration": "2030-01-01T00:00:00.000Z",
            "conditions": [
                {"bucket": "mybucket"},
                ["eq", "$key", "mykey"]
            ]
        }"#;
        let policy = PostPolicy::from_json(json).unwrap();

        // "success_action_status" is NOT in the policy but IS in form fields
        let multipart = create_test_multipart(vec![("key", "mykey"), ("success_action_status", "200")], None);
        let err = policy.validate_conditions_only(&multipart, 0, Some("mybucket")).unwrap_err();
        assert!(matches!(err.code(), S3ErrorCode::AccessDenied));
        assert!(
            err.message().unwrap_or("").contains("success_action_status"),
            "error message should mention the undeclared field"
        );
    }

    /// Exempt fields (x-amz-signature, file, submit, policy, x-ignore-*) are allowed
    /// even when not declared in the policy.
    #[test]
    fn test_exempt_fields_not_in_policy_are_allowed() {
        let json = r#"{
            "expiration": "2030-01-01T00:00:00.000Z",
            "conditions": [
                {"bucket": "mybucket"},
                ["eq", "$key", "mykey"]
            ]
        }"#;
        let policy = PostPolicy::from_json(json).unwrap();

        // SigV4 exempt fields
        let multipart = create_test_multipart(vec![("key", "mykey"), ("policy", "abc"), ("x-amz-signature", "sig123")], None);
        let result = policy.validate_conditions_only(&multipart, 0, Some("mybucket"));
        assert!(result.is_ok(), "SigV4 exempt fields should not be rejected");

        // SigV2 exempt fields
        let multipart_v2 = create_test_multipart(
            vec![
                ("awsaccesskeyid", "AKID"),
                ("key", "mykey"),
                ("policy", "abc"),
                ("signature", "sig456"),
            ],
            None,
        );
        let result_v2 = policy.validate_conditions_only(&multipart_v2, 0, Some("mybucket"));
        assert!(result_v2.is_ok(), "SigV2 exempt fields should not be rejected");
    }

    #[test]
    fn test_encryption_fields_not_in_policy_are_allowed() {
        let json = r#"{
            "expiration": "2030-01-01T00:00:00.000Z",
            "conditions": [
                {"bucket": "mybucket"},
                ["eq", "$key", "mykey"]
            ]
        }"#;
        let policy = PostPolicy::from_json(json).unwrap();

        let multipart = create_test_multipart(
            vec![
                ("key", "mykey"),
                ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
                ("x-amz-server-side-encryption-customer-key", "ZmFrZS1rZXk="),
                ("x-amz-server-side-encryption-customer-key-md5", "ZmFrZS1tZDU="),
                ("x-amz-server-side-encryption-aws-kms-key-id", "test-key"),
                ("x-amz-server-side-encryption-bucket-key-enabled", "true"),
                ("x-amz-server-side-encryption-context", "e30="),
                ("x-amz-server-side-encryption-custom-flag", "custom"),
            ],
            None,
        );
        let result = policy.validate_conditions_only(&multipart, 0, Some("mybucket"));
        assert!(result.is_ok(), "encryption exception fields should not be rejected");
    }

    /// A field declared via starts-with condition should also be accepted.
    #[test]
    fn test_starts_with_condition_covers_field() {
        let json = r#"{
            "expiration": "2030-01-01T00:00:00.000Z",
            "conditions": [
                {"bucket": "mybucket"},
                ["eq", "$key", "mykey"],
                ["starts-with", "$success_action_status", ""]
            ]
        }"#;
        let policy = PostPolicy::from_json(json).unwrap();

        let multipart = create_test_multipart(vec![("key", "mykey"), ("success_action_status", "200")], None);
        let result = policy.validate_conditions_only(&multipart, 0, Some("mybucket"));
        assert!(result.is_ok(), "field covered by starts-with should be accepted");
    }

    /// x-ignore- prefixed fields are always allowed without a condition.
    #[test]
    fn test_x_ignore_prefixed_fields_are_allowed() {
        let json = r#"{
            "expiration": "2030-01-01T00:00:00.000Z",
            "conditions": [
                {"bucket": "mybucket"},
                ["eq", "$key", "mykey"]
            ]
        }"#;
        let policy = PostPolicy::from_json(json).unwrap();

        let multipart = create_test_multipart(vec![("key", "mykey"), ("x-ignore-custom", "anything")], None);
        let result = policy.validate_conditions_only(&multipart, 0, Some("mybucket"));
        assert!(result.is_ok(), "x-ignore- fields should not be rejected");
    }
}
