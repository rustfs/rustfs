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

//! Owned input and output for the CopyObject storage path.

use rustfs_gateway_types::CopySourceValue;
use std::collections::HashMap;
use time::OffsetDateTime;

/// Comparison strength for decoded source conditions.
pub(crate) enum CopySourceCondition {
    Any,
    Strong(String),
    Weak,
}

impl CopySourceCondition {
    pub(crate) fn strong(&self) -> Option<&str> {
        match self {
            Self::Strong(value) => Some(value),
            Self::Any | Self::Weak => None,
        }
    }
}

// Deliberately no Debug: these fields may contain customer encryption keys.
pub(crate) struct CopyObjectWriteInput {
    pub(crate) bucket: String,
    pub(crate) key: String,
    /// Only bucket-path sources are supported.
    pub(crate) copy_source: Option<CopySourceValue>,
    pub(crate) version_id: Option<String>,
    pub(crate) specific_if_none_match: bool,
    pub(crate) server_side_encryption: Option<String>,
    pub(crate) ssekms_key_id: Option<String>,
    pub(crate) sse_customer_algorithm: Option<String>,
    pub(crate) sse_customer_key: Option<String>,
    pub(crate) sse_customer_key_md5: Option<String>,
    pub(crate) copy_source_sse_customer_algorithm: Option<String>,
    pub(crate) copy_source_sse_customer_key: Option<String>,
    pub(crate) copy_source_sse_customer_key_md5: Option<String>,
    pub(crate) metadata_directive: Option<String>,
    pub(crate) metadata: Option<HashMap<String, String>>,
    pub(crate) tagging: Option<String>,
    pub(crate) tagging_directive: Option<String>,
    pub(crate) copy_source_if_match: Option<CopySourceCondition>,
    pub(crate) copy_source_if_none_match: Option<CopySourceCondition>,
    pub(crate) cache_control: Option<String>,
    pub(crate) content_disposition: Option<String>,
    pub(crate) content_encoding: Option<String>,
    pub(crate) content_language: Option<String>,
    pub(crate) content_type: Option<String>,
    pub(crate) expires: Option<String>,
    pub(crate) website_redirect_location: Option<String>,
    pub(crate) object_lock_legal_hold_status: Option<String>,
    pub(crate) object_lock_mode: Option<String>,
    pub(crate) object_lock_retain_until_date: Option<OffsetDateTime>,
    pub(crate) storage_class: Option<String>,
    pub(crate) checksum_algorithm: Option<String>,
}

#[derive(Debug)]
pub(crate) struct CopyObjectWriteOutput {
    pub(crate) e_tag: Option<String>,
    pub(crate) last_modified: Option<OffsetDateTime>,
    pub(crate) copy_source_version_id: Option<String>,
    pub(crate) version_id: Option<String>,
    pub(crate) server_side_encryption: Option<String>,
    pub(crate) ssekms_key_id: Option<String>,
    pub(crate) sse_customer_algorithm: Option<String>,
    pub(crate) sse_customer_key_md5: Option<String>,
    pub(crate) checksum_crc32: Option<String>,
    pub(crate) checksum_crc32c: Option<String>,
    pub(crate) checksum_sha1: Option<String>,
    pub(crate) checksum_sha256: Option<String>,
    pub(crate) checksum_crc64nvme: Option<String>,
    pub(crate) checksum_md5: Option<String>,
    pub(crate) checksum_sha512: Option<String>,
    pub(crate) checksum_xxhash3: Option<String>,
    pub(crate) checksum_xxhash64: Option<String>,
    pub(crate) checksum_xxhash128: Option<String>,
    pub(crate) checksum_type: Option<String>,
}

#[cfg(test)]
impl CopyObjectWriteInput {
    /// A test's s3s fixture request as the envelope and input a gateway handler hands the use case.
    pub(crate) fn from_test_request(
        req: crate::app::storage_api::s3::S3Request<crate::app::storage_api::s3::dto::CopyObjectInput>,
    ) -> (crate::app::RequestEnvelope, Self) {
        use crate::app::object_usecase::CopySourceCondition;
        use crate::app::storage_api::s3::dto::{CopySource, ETag, ETagCondition};

        let envelope = crate::app::RequestEnvelope::from_s3s(&req);
        let input = req.input;
        let condition = |value: ETagCondition| match value {
            ETagCondition::Any => CopySourceCondition::Any,
            ETagCondition::ETag(ETag::Strong(value)) => CopySourceCondition::Strong(value),
            ETagCondition::ETag(ETag::Weak(_)) => CopySourceCondition::Weak,
        };
        let copy_source = match &input.copy_source {
            CopySource::AccessPoint { .. } | CopySource::Outpost { .. } => None,
            CopySource::Bucket { bucket, key, version_id } => Some(rustfs_gateway_types::CopySourceValue::Bucket {
                bucket: bucket.to_string(),
                key: key.to_string(),
                version_id: version_id.as_ref().map(|v| v.to_string()),
            }),
        };
        let write_input = CopyObjectWriteInput {
            copy_source,
            bucket: input.bucket,
            key: input.key,
            version_id: input.version_id,
            specific_if_none_match: matches!(input.if_none_match, Some(ETagCondition::ETag(_))),
            server_side_encryption: input.server_side_encryption.map(|value| value.as_str().to_owned()),
            ssekms_key_id: input.ssekms_key_id,
            sse_customer_algorithm: input.sse_customer_algorithm,
            sse_customer_key: input.sse_customer_key,
            sse_customer_key_md5: input.sse_customer_key_md5,
            copy_source_sse_customer_algorithm: input.copy_source_sse_customer_algorithm,
            copy_source_sse_customer_key: input.copy_source_sse_customer_key,
            copy_source_sse_customer_key_md5: input.copy_source_sse_customer_key_md5,
            metadata_directive: input.metadata_directive.map(|value| value.as_str().to_owned()),
            metadata: input.metadata,
            tagging: input.tagging,
            tagging_directive: input.tagging_directive.map(|value| value.as_str().to_owned()),
            copy_source_if_match: input.copy_source_if_match.map(condition),
            copy_source_if_none_match: input.copy_source_if_none_match.map(condition),
            cache_control: input.cache_control,
            content_disposition: input.content_disposition,
            content_encoding: input.content_encoding,
            content_language: input.content_language,
            content_type: input.content_type,
            expires: input.expires,
            website_redirect_location: input.website_redirect_location,
            object_lock_legal_hold_status: input.object_lock_legal_hold_status.map(|value| value.as_str().to_owned()),
            object_lock_mode: input.object_lock_mode.map(|value| value.as_str().to_owned()),
            object_lock_retain_until_date: input.object_lock_retain_until_date.map(time::OffsetDateTime::from),
            storage_class: input.storage_class.map(|value| value.as_str().to_owned()),
            checksum_algorithm: input.checksum_algorithm.map(|value| value.as_str().to_owned()),
        };
        (envelope, write_input)
    }
}
