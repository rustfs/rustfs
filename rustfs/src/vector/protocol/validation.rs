// Copyright 2026 RustFS Team
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

use super::ProtocolError;
use super::dto::*;
use regex::Regex;
use std::collections::HashSet;
use std::sync::LazyLock;

pub(crate) const MAX_REQUEST_BODY_BYTES: usize = 20 * 1024 * 1024;

static TAG_CHARACTERS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\A[\p{L}\p{Z}\p{N}_.:/=+\-@]*\z").expect("the pinned tag character pattern is valid"));
static KMS_KEY_ARN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\Aarn:aws[-a-z0-9]*:kms:[-a-z0-9]*:[0-9]{12}:key/.+\z").expect("the pinned KMS ARN pattern is valid")
});

pub(super) trait Validate {
    fn validate(&self, path: &str, region: &str) -> Result<(), ProtocolError>;
}

pub(super) fn validate_body_length(length: usize) -> Result<(), ProtocolError> {
    validate_length(length, 0, MAX_REQUEST_BODY_BYTES, "request")
}

fn child_path(parent: &str, member: &str) -> String {
    if parent.is_empty() {
        member.to_owned()
    } else {
        format!("{parent}.{member}")
    }
}

fn validate_length(length: usize, min: usize, max: usize, path: &str) -> Result<(), ProtocolError> {
    if (min..=max).contains(&length) {
        return Ok(());
    }
    Err(ProtocolError::validation(path, format!("Length must be between {min} and {max}")))
}

fn validate_range(value: i64, min: i64, max: i64, path: &str) -> Result<(), ProtocolError> {
    if (min..=max).contains(&value) {
        return Ok(());
    }
    Err(ProtocolError::validation(path, format!("Value must be between {min} and {max}")))
}

fn validate_mode(mode: IndexMode, path: &str) -> Result<(), ProtocolError> {
    if mode == IndexMode::Enhanced {
        return Ok(());
    }
    Err(ProtocolError::validation(path, "Only ENHANCED mode is supported"))
}

fn validate_unique(values: &[String], path: &str) -> Result<(), ProtocolError> {
    let mut seen = HashSet::with_capacity(values.len());
    if values.iter().all(|value| seen.insert(value)) {
        return Ok(());
    }
    Err(ProtocolError::validation(path, "Metadata keys must be unique"))
}

fn validate_name(value: &str, index: bool, path: &str) -> Result<(), ProtocolError> {
    validate_length(value.chars().count(), 3, 63, path)?;
    let alphanumeric = |byte: u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
    let bytes = value.as_bytes();
    if bytes.first().copied().is_some_and(alphanumeric)
        && bytes.last().copied().is_some_and(alphanumeric)
        && bytes
            .iter()
            .all(|byte| alphanumeric(*byte) || *byte == b'-' || (index && *byte == b'.'))
    {
        return Ok(());
    }
    Err(ProtocolError::validation(path, "Invalid resource name"))
}

fn validate_arn(value: &str, index: Option<bool>, region: &str, path: &str) -> Result<(), ProtocolError> {
    let Some(resource) = value
        .strip_prefix("arn:aws:s3vectors:")
        .and_then(|tail| tail.strip_prefix(region))
        .and_then(|tail| tail.strip_prefix("::bucket/"))
    else {
        return Err(ProtocolError::validation(path, "ARN must identify this region with an empty account"));
    };
    if region.is_empty()
        || !region
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err(ProtocolError::validation(path, "Invalid deployment region"));
    }
    let parts: Vec<_> = resource.split('/').collect();
    match parts.as_slice() {
        [bucket] if index != Some(true) => validate_name(bucket, false, path),
        [bucket, "index", name] if index != Some(false) => {
            validate_name(bucket, false, path)?;
            validate_name(name, true, path)
        }
        _ => Err(ProtocolError::validation(path, "ARN has an invalid resource kind or path")),
    }
}

fn validate_string(shape: &str, value: &str, path: &str, region: &str) -> Result<(), ProtocolError> {
    let (min, max) = match shape {
        "VectorBucketName" => return validate_name(value, false, path),
        "IndexName" => return validate_name(value, true, path),
        "VectorBucketArn" => return validate_arn(value, Some(false), region, path),
        "IndexArn" => return validate_arn(value, Some(true), region, path),
        "ResourceARN" => {
            validate_length(value.chars().count(), 0, 1011, path)?;
            return validate_arn(value, None, region, path);
        }
        "VectorKey" => (1, 1024),
        "MetadataKey" | "ListIndexesPrefix" | "ListVectorBucketsPrefix" => (1, 63),
        "ListIndexesNextToken" | "ListVectorBucketsNextToken" => (1, 512),
        "ListVectorsNextToken" => (1, 2048),
        "QueryVectorsNextToken" => (1, 4096),
        "KmsKeyArn" => (1, 2048),
        "TagKey" => (1, 128),
        "TagValue" => (0, 256),
        _ => unreachable!("only pinned input string shapes call this validator"),
    };
    validate_length(value.chars().count(), min, max, path)?;
    if (matches!(shape, "TagKey" | "TagValue") && !TAG_CHARACTERS.is_match(value))
        || (shape == "KmsKeyArn" && !KMS_KEY_ARN.is_match(value))
    {
        return Err(ProtocolError::validation(path, "Value does not match the required pattern"));
    }
    Ok(())
}

fn validate_bucket_selector(name: Option<&str>, arn: Option<&str>) -> Result<(), ProtocolError> {
    if name.is_some() != arn.is_some() {
        return Ok(());
    }
    Err(ProtocolError::validation(
        "vectorBucketName",
        "Provide exactly one of vectorBucketName and vectorBucketArn",
    ))
}

fn validate_index_selector(bucket: Option<&str>, name: Option<&str>, arn: Option<&str>) -> Result<(), ProtocolError> {
    if matches!((bucket, name, arn), (Some(_), Some(_), None) | (None, None, Some(_))) {
        return Ok(());
    }
    Err(ProtocolError::validation(
        "indexArn",
        "Provide either indexArn or both vectorBucketName and indexName",
    ))
}

fn validate_segments(count: Option<i32>, index: Option<i32>) -> Result<(), ProtocolError> {
    match (count, index) {
        (None, None) => Ok(()),
        (Some(count), Some(index)) if index < count => Ok(()),
        _ => Err(ProtocolError::validation(
            "segmentIndex",
            "segmentCount and segmentIndex must be supplied together, with segmentIndex less than segmentCount",
        )),
    }
}

fn validate_encryption(configuration: &EncryptionConfiguration) -> Result<(), ProtocolError> {
    match (configuration.sse_type.unwrap_or(SseType::Aes256), configuration.kms_key_arn.as_ref()) {
        (SseType::Aes256, None) | (SseType::AwsKms, Some(_)) => Ok(()),
        (SseType::Aes256, Some(_)) => Err(ProtocolError::validation(
            "encryptionConfiguration.kmsKeyArn",
            "kmsKeyArn requires sseType aws:kms",
        )),
        (SseType::AwsKms, None) => Err(ProtocolError::validation(
            "encryptionConfiguration.kmsKeyArn",
            "aws:kms requires kmsKeyArn",
        )),
    }
}
impl Validate for CreateIndexInput {
    fn validate(&self, path: &str, region: &str) -> Result<(), ProtocolError> {
        if let Some(value) = &self.vector_bucket_name {
            let field_path = child_path(path, "vectorBucketName");
            validate_string("VectorBucketName", value, &field_path, region)?;
        }
        if let Some(value) = &self.vector_bucket_arn {
            let field_path = child_path(path, "vectorBucketArn");
            validate_string("VectorBucketArn", value, &field_path, region)?;
        }
        {
            let value = &self.index_name;
            let field_path = child_path(path, "indexName");
            validate_string("IndexName", value, &field_path, region)?;
        }
        {
            let value = &self.dimension;
            let field_path = child_path(path, "dimension");
            validate_range(i64::from(*value), 1, 4096, &field_path)?;
        }
        if let Some(value) = &self.metadata_configuration {
            let field_path = child_path(path, "metadataConfiguration");
            value.validate(&field_path, region)?;
        }
        if let Some(value) = &self.encryption_configuration {
            let field_path = child_path(path, "encryptionConfiguration");
            value.validate(&field_path, region)?;
        }
        if let Some(value) = &self.tags {
            let field_path = child_path(path, "tags");
            validate_length(value.len(), 0, 50, &field_path)?;
            for (key, value) in value {
                validate_string("TagKey", key, &field_path, region)?;
                validate_string("TagValue", value, &field_path, region)?;
            }
        }
        validate_bucket_selector(self.vector_bucket_name.as_deref(), self.vector_bucket_arn.as_deref())?;
        Ok(())
    }
}

impl Validate for CreateVectorBucketInput {
    fn validate(&self, path: &str, region: &str) -> Result<(), ProtocolError> {
        {
            let value = &self.vector_bucket_name;
            let field_path = child_path(path, "vectorBucketName");
            validate_string("VectorBucketName", value, &field_path, region)?;
        }
        if let Some(value) = &self.encryption_configuration {
            let field_path = child_path(path, "encryptionConfiguration");
            value.validate(&field_path, region)?;
        }
        if let Some(value) = &self.tags {
            let field_path = child_path(path, "tags");
            validate_length(value.len(), 0, 50, &field_path)?;
            for (key, value) in value {
                validate_string("TagKey", key, &field_path, region)?;
                validate_string("TagValue", value, &field_path, region)?;
            }
        }
        Ok(())
    }
}

impl Validate for DeleteIndexInput {
    fn validate(&self, path: &str, region: &str) -> Result<(), ProtocolError> {
        if let Some(value) = &self.vector_bucket_name {
            let field_path = child_path(path, "vectorBucketName");
            validate_string("VectorBucketName", value, &field_path, region)?;
        }
        if let Some(value) = &self.index_name {
            let field_path = child_path(path, "indexName");
            validate_string("IndexName", value, &field_path, region)?;
        }
        if let Some(value) = &self.index_arn {
            let field_path = child_path(path, "indexArn");
            validate_string("IndexArn", value, &field_path, region)?;
        }
        validate_index_selector(self.vector_bucket_name.as_deref(), self.index_name.as_deref(), self.index_arn.as_deref())?;
        Ok(())
    }
}

impl Validate for DeleteVectorBucketInput {
    fn validate(&self, path: &str, region: &str) -> Result<(), ProtocolError> {
        if let Some(value) = &self.vector_bucket_name {
            let field_path = child_path(path, "vectorBucketName");
            validate_string("VectorBucketName", value, &field_path, region)?;
        }
        if let Some(value) = &self.vector_bucket_arn {
            let field_path = child_path(path, "vectorBucketArn");
            validate_string("VectorBucketArn", value, &field_path, region)?;
        }
        validate_bucket_selector(self.vector_bucket_name.as_deref(), self.vector_bucket_arn.as_deref())?;
        Ok(())
    }
}

impl Validate for DeleteVectorBucketPolicyInput {
    fn validate(&self, path: &str, region: &str) -> Result<(), ProtocolError> {
        if let Some(value) = &self.vector_bucket_name {
            let field_path = child_path(path, "vectorBucketName");
            validate_string("VectorBucketName", value, &field_path, region)?;
        }
        if let Some(value) = &self.vector_bucket_arn {
            let field_path = child_path(path, "vectorBucketArn");
            validate_string("VectorBucketArn", value, &field_path, region)?;
        }
        validate_bucket_selector(self.vector_bucket_name.as_deref(), self.vector_bucket_arn.as_deref())?;
        Ok(())
    }
}

impl Validate for DeleteVectorsInput {
    fn validate(&self, path: &str, region: &str) -> Result<(), ProtocolError> {
        if let Some(value) = &self.vector_bucket_name {
            let field_path = child_path(path, "vectorBucketName");
            validate_string("VectorBucketName", value, &field_path, region)?;
        }
        if let Some(value) = &self.index_name {
            let field_path = child_path(path, "indexName");
            validate_string("IndexName", value, &field_path, region)?;
        }
        if let Some(value) = &self.index_arn {
            let field_path = child_path(path, "indexArn");
            validate_string("IndexArn", value, &field_path, region)?;
        }
        {
            let value = &self.keys;
            let field_path = child_path(path, "keys");
            validate_length(value.len(), 1, 500, &field_path)?;
            for (index, item) in value.iter().enumerate() {
                let item_path = format!("{field_path}[{index}]");
                validate_string("VectorKey", item, &item_path, region)?;
            }
        }
        validate_index_selector(self.vector_bucket_name.as_deref(), self.index_name.as_deref(), self.index_arn.as_deref())?;
        Ok(())
    }
}

impl Validate for EncryptionConfiguration {
    fn validate(&self, path: &str, region: &str) -> Result<(), ProtocolError> {
        if let Some(value) = &self.kms_key_arn {
            let field_path = child_path(path, "kmsKeyArn");
            validate_string("KmsKeyArn", value, &field_path, region)?;
        }
        validate_encryption(self)?;
        Ok(())
    }
}

impl Validate for GetIndexInput {
    fn validate(&self, path: &str, region: &str) -> Result<(), ProtocolError> {
        if let Some(value) = &self.vector_bucket_name {
            let field_path = child_path(path, "vectorBucketName");
            validate_string("VectorBucketName", value, &field_path, region)?;
        }
        if let Some(value) = &self.index_name {
            let field_path = child_path(path, "indexName");
            validate_string("IndexName", value, &field_path, region)?;
        }
        if let Some(value) = &self.index_arn {
            let field_path = child_path(path, "indexArn");
            validate_string("IndexArn", value, &field_path, region)?;
        }
        validate_index_selector(self.vector_bucket_name.as_deref(), self.index_name.as_deref(), self.index_arn.as_deref())?;
        Ok(())
    }
}

impl Validate for GetVectorBucketInput {
    fn validate(&self, path: &str, region: &str) -> Result<(), ProtocolError> {
        if let Some(value) = &self.vector_bucket_name {
            let field_path = child_path(path, "vectorBucketName");
            validate_string("VectorBucketName", value, &field_path, region)?;
        }
        if let Some(value) = &self.vector_bucket_arn {
            let field_path = child_path(path, "vectorBucketArn");
            validate_string("VectorBucketArn", value, &field_path, region)?;
        }
        validate_bucket_selector(self.vector_bucket_name.as_deref(), self.vector_bucket_arn.as_deref())?;
        Ok(())
    }
}

impl Validate for GetVectorBucketPolicyInput {
    fn validate(&self, path: &str, region: &str) -> Result<(), ProtocolError> {
        if let Some(value) = &self.vector_bucket_name {
            let field_path = child_path(path, "vectorBucketName");
            validate_string("VectorBucketName", value, &field_path, region)?;
        }
        if let Some(value) = &self.vector_bucket_arn {
            let field_path = child_path(path, "vectorBucketArn");
            validate_string("VectorBucketArn", value, &field_path, region)?;
        }
        validate_bucket_selector(self.vector_bucket_name.as_deref(), self.vector_bucket_arn.as_deref())?;
        Ok(())
    }
}

impl Validate for GetVectorsInput {
    fn validate(&self, path: &str, region: &str) -> Result<(), ProtocolError> {
        if let Some(value) = &self.vector_bucket_name {
            let field_path = child_path(path, "vectorBucketName");
            validate_string("VectorBucketName", value, &field_path, region)?;
        }
        if let Some(value) = &self.index_name {
            let field_path = child_path(path, "indexName");
            validate_string("IndexName", value, &field_path, region)?;
        }
        if let Some(value) = &self.index_arn {
            let field_path = child_path(path, "indexArn");
            validate_string("IndexArn", value, &field_path, region)?;
        }
        {
            let value = &self.keys;
            let field_path = child_path(path, "keys");
            validate_length(value.len(), 1, 100, &field_path)?;
            for (index, item) in value.iter().enumerate() {
                let item_path = format!("{field_path}[{index}]");
                validate_string("VectorKey", item, &item_path, region)?;
            }
        }
        validate_index_selector(self.vector_bucket_name.as_deref(), self.index_name.as_deref(), self.index_arn.as_deref())?;
        Ok(())
    }
}

impl Validate for ListIndexesInput {
    fn validate(&self, path: &str, region: &str) -> Result<(), ProtocolError> {
        if let Some(value) = &self.vector_bucket_name {
            let field_path = child_path(path, "vectorBucketName");
            validate_string("VectorBucketName", value, &field_path, region)?;
        }
        if let Some(value) = &self.vector_bucket_arn {
            let field_path = child_path(path, "vectorBucketArn");
            validate_string("VectorBucketArn", value, &field_path, region)?;
        }
        if let Some(value) = &self.max_results {
            let field_path = child_path(path, "maxResults");
            validate_range(i64::from(*value), 1, 500, &field_path)?;
        }
        if let Some(value) = &self.next_token {
            let field_path = child_path(path, "nextToken");
            validate_string("ListIndexesNextToken", value, &field_path, region)?;
        }
        if let Some(value) = &self.prefix {
            let field_path = child_path(path, "prefix");
            validate_string("ListIndexesPrefix", value, &field_path, region)?;
        }
        validate_bucket_selector(self.vector_bucket_name.as_deref(), self.vector_bucket_arn.as_deref())?;
        Ok(())
    }
}

impl Validate for ListTagsForResourceInput {
    fn validate(&self, path: &str, region: &str) -> Result<(), ProtocolError> {
        {
            let value = &self.resource_arn;
            let field_path = child_path(path, "resourceArn");
            validate_string("ResourceARN", value, &field_path, region)?;
        }
        Ok(())
    }
}

impl Validate for ListVectorBucketsInput {
    fn validate(&self, path: &str, region: &str) -> Result<(), ProtocolError> {
        if let Some(value) = &self.max_results {
            let field_path = child_path(path, "maxResults");
            validate_range(i64::from(*value), 1, 500, &field_path)?;
        }
        if let Some(value) = &self.next_token {
            let field_path = child_path(path, "nextToken");
            validate_string("ListVectorBucketsNextToken", value, &field_path, region)?;
        }
        if let Some(value) = &self.prefix {
            let field_path = child_path(path, "prefix");
            validate_string("ListVectorBucketsPrefix", value, &field_path, region)?;
        }
        Ok(())
    }
}

impl Validate for ListVectorsInput {
    fn validate(&self, path: &str, region: &str) -> Result<(), ProtocolError> {
        if let Some(value) = &self.vector_bucket_name {
            let field_path = child_path(path, "vectorBucketName");
            validate_string("VectorBucketName", value, &field_path, region)?;
        }
        if let Some(value) = &self.index_name {
            let field_path = child_path(path, "indexName");
            validate_string("IndexName", value, &field_path, region)?;
        }
        if let Some(value) = &self.index_arn {
            let field_path = child_path(path, "indexArn");
            validate_string("IndexArn", value, &field_path, region)?;
        }
        if let Some(value) = &self.max_results {
            let field_path = child_path(path, "maxResults");
            validate_range(i64::from(*value), 1, 1000, &field_path)?;
        }
        if let Some(value) = &self.next_token {
            let field_path = child_path(path, "nextToken");
            validate_string("ListVectorsNextToken", value, &field_path, region)?;
        }
        if let Some(value) = &self.segment_count {
            let field_path = child_path(path, "segmentCount");
            validate_range(i64::from(*value), 1, 16, &field_path)?;
        }
        if let Some(value) = &self.segment_index {
            let field_path = child_path(path, "segmentIndex");
            validate_range(i64::from(*value), 0, 15, &field_path)?;
        }
        validate_index_selector(self.vector_bucket_name.as_deref(), self.index_name.as_deref(), self.index_arn.as_deref())?;
        validate_segments(self.segment_count, self.segment_index)?;
        Ok(())
    }
}

impl Validate for MetadataConfiguration {
    fn validate(&self, path: &str, region: &str) -> Result<(), ProtocolError> {
        {
            let value = &self.non_filterable_metadata_keys;
            let field_path = child_path(path, "nonFilterableMetadataKeys");
            validate_length(value.len(), 1, 10, &field_path)?;
            for (index, item) in value.iter().enumerate() {
                let item_path = format!("{field_path}[{index}]");
                validate_string("MetadataKey", item, &item_path, region)?;
            }
            validate_unique(value, &field_path)?;
        }
        Ok(())
    }
}

impl Validate for PutInputVector {
    fn validate(&self, path: &str, region: &str) -> Result<(), ProtocolError> {
        {
            let value = &self.key;
            let field_path = child_path(path, "key");
            validate_string("VectorKey", value, &field_path, region)?;
        }
        {
            let value = &self.data;
            let field_path = child_path(path, "data");
            value.validate(&field_path, region)?;
        }
        Ok(())
    }
}

impl Validate for PutVectorBucketDefaultIndexModeInput {
    fn validate(&self, path: &str, region: &str) -> Result<(), ProtocolError> {
        if let Some(value) = &self.vector_bucket_name {
            let field_path = child_path(path, "vectorBucketName");
            validate_string("VectorBucketName", value, &field_path, region)?;
        }
        if let Some(value) = &self.vector_bucket_arn {
            let field_path = child_path(path, "vectorBucketArn");
            validate_string("VectorBucketArn", value, &field_path, region)?;
        }
        {
            let value = &self.default_index_mode;
            let field_path = child_path(path, "defaultIndexMode");
            validate_mode(*value, &field_path)?;
        }
        validate_bucket_selector(self.vector_bucket_name.as_deref(), self.vector_bucket_arn.as_deref())?;
        Ok(())
    }
}

impl Validate for PutVectorBucketPolicyInput {
    fn validate(&self, path: &str, region: &str) -> Result<(), ProtocolError> {
        if let Some(value) = &self.vector_bucket_name {
            let field_path = child_path(path, "vectorBucketName");
            validate_string("VectorBucketName", value, &field_path, region)?;
        }
        if let Some(value) = &self.vector_bucket_arn {
            let field_path = child_path(path, "vectorBucketArn");
            validate_string("VectorBucketArn", value, &field_path, region)?;
        }
        validate_bucket_selector(self.vector_bucket_name.as_deref(), self.vector_bucket_arn.as_deref())?;
        Ok(())
    }
}

impl Validate for PutVectorsInput {
    fn validate(&self, path: &str, region: &str) -> Result<(), ProtocolError> {
        if let Some(value) = &self.vector_bucket_name {
            let field_path = child_path(path, "vectorBucketName");
            validate_string("VectorBucketName", value, &field_path, region)?;
        }
        if let Some(value) = &self.index_name {
            let field_path = child_path(path, "indexName");
            validate_string("IndexName", value, &field_path, region)?;
        }
        if let Some(value) = &self.index_arn {
            let field_path = child_path(path, "indexArn");
            validate_string("IndexArn", value, &field_path, region)?;
        }
        {
            let value = &self.vectors;
            let field_path = child_path(path, "vectors");
            validate_length(value.len(), 1, 500, &field_path)?;
            for (index, item) in value.iter().enumerate() {
                let item_path = format!("{field_path}[{index}]");
                item.validate(&item_path, region)?;
            }
        }
        validate_index_selector(self.vector_bucket_name.as_deref(), self.index_name.as_deref(), self.index_arn.as_deref())?;
        Ok(())
    }
}

impl Validate for QueryVectorsInput {
    fn validate(&self, path: &str, region: &str) -> Result<(), ProtocolError> {
        if let Some(value) = &self.vector_bucket_name {
            let field_path = child_path(path, "vectorBucketName");
            validate_string("VectorBucketName", value, &field_path, region)?;
        }
        if let Some(value) = &self.index_name {
            let field_path = child_path(path, "indexName");
            validate_string("IndexName", value, &field_path, region)?;
        }
        if let Some(value) = &self.index_arn {
            let field_path = child_path(path, "indexArn");
            validate_string("IndexArn", value, &field_path, region)?;
        }
        {
            let value = &self.top_k;
            let field_path = child_path(path, "topK");
            validate_range(i64::from(*value), 1, 10000, &field_path)?;
        }
        {
            let value = &self.query_vector;
            let field_path = child_path(path, "queryVector");
            value.validate(&field_path, region)?;
        }
        if let Some(value) = &self.query_mode {
            let field_path = child_path(path, "queryMode");
            validate_mode(*value, &field_path)?;
        }
        if let Some(value) = &self.next_token {
            let field_path = child_path(path, "nextToken");
            validate_string("QueryVectorsNextToken", value, &field_path, region)?;
        }
        validate_index_selector(self.vector_bucket_name.as_deref(), self.index_name.as_deref(), self.index_arn.as_deref())?;
        Ok(())
    }
}

impl Validate for TagResourceInput {
    fn validate(&self, path: &str, region: &str) -> Result<(), ProtocolError> {
        {
            let value = &self.resource_arn;
            let field_path = child_path(path, "resourceArn");
            validate_string("ResourceARN", value, &field_path, region)?;
        }
        {
            let value = &self.tags;
            let field_path = child_path(path, "tags");
            validate_length(value.len(), 0, 50, &field_path)?;
            for (key, value) in value {
                validate_string("TagKey", key, &field_path, region)?;
                validate_string("TagValue", value, &field_path, region)?;
            }
        }
        Ok(())
    }
}

impl Validate for UntagResourceInput {
    fn validate(&self, path: &str, region: &str) -> Result<(), ProtocolError> {
        {
            let value = &self.resource_arn;
            let field_path = child_path(path, "resourceArn");
            validate_string("ResourceARN", value, &field_path, region)?;
        }
        {
            let value = &self.tag_keys;
            let field_path = child_path(path, "tagKeys");
            for (index, item) in value.iter().enumerate() {
                let item_path = format!("{field_path}[{index}]");
                validate_string("TagKey", item, &item_path, region)?;
            }
        }
        Ok(())
    }
}

impl Validate for UpdateIndexModeInput {
    fn validate(&self, path: &str, region: &str) -> Result<(), ProtocolError> {
        if let Some(value) = &self.vector_bucket_name {
            let field_path = child_path(path, "vectorBucketName");
            validate_string("VectorBucketName", value, &field_path, region)?;
        }
        if let Some(value) = &self.index_name {
            let field_path = child_path(path, "indexName");
            validate_string("IndexName", value, &field_path, region)?;
        }
        if let Some(value) = &self.index_arn {
            let field_path = child_path(path, "indexArn");
            validate_string("IndexArn", value, &field_path, region)?;
        }
        {
            let value = &self.index_mode;
            let field_path = child_path(path, "indexMode");
            validate_mode(*value, &field_path)?;
        }
        validate_index_selector(self.vector_bucket_name.as_deref(), self.index_name.as_deref(), self.index_arn.as_deref())?;
        Ok(())
    }
}

impl Validate for VectorData {
    fn validate(&self, path: &str, _region: &str) -> Result<(), ProtocolError> {
        {
            let value = &self.float32;
            let field_path = child_path(path, "float32");
            validate_length(value.len(), 1, 4096, &field_path)?;
            if let Some((index, _)) = value.iter().enumerate().find(|(_, item)| !item.is_finite()) {
                let item_path = format!("{field_path}[{index}]");
                return Err(ProtocolError::validation(&item_path, "Vector values must be finite float32 numbers"));
            }
        }
        Ok(())
    }
}
