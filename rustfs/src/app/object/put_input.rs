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

//! Owned values consumed by the PutObject and archive upload use cases.
//! Values remain unvalidated here so the business path retains refusal ordering.

use rustfs_gateway_stream::Body;
use rustfs_rio::SharedTrailerSource;
use std::collections::HashMap;

// Deliberately no Debug: customer encryption keys must never reach diagnostics.
#[derive(Default)]
pub(crate) struct PutObjectWriteInput {
    pub(crate) bucket: String,
    pub(crate) key: String,
    pub(crate) body: Option<Body>,
    pub(crate) post_archive: Option<super::post_body::PreflightedPostArchive>,
    pub(crate) frame_trailers: Option<std::sync::Arc<super::native_body::FrameTrailers>>,
    pub(crate) trailing_headers: Option<SharedTrailerSource>,
    pub(crate) content_length: Option<i64>,
    pub(crate) metadata: Option<HashMap<String, String>>,
    /// Optional POST form projection; the original request headers remain available for audit.
    pub(crate) metadata_headers: Option<http::HeaderMap>,
    /// Optional checksum projection used only by the body integrity reader.
    pub(crate) checksum_headers: Option<http::HeaderMap>,
    pub(crate) specific_if_none_match: bool,
    pub(crate) object_lock_retain_until_date: Option<time::OffsetDateTime>,
    pub(crate) cache_control: Option<String>,
    pub(crate) content_disposition: Option<String>,
    pub(crate) content_encoding: Option<String>,
    pub(crate) content_language: Option<String>,
    pub(crate) content_type: Option<String>,
    pub(crate) expires: Option<String>,
    pub(crate) tagging: Option<String>,
    pub(crate) version_id: Option<String>,
    pub(crate) server_side_encryption: Option<String>,
    pub(crate) sse_customer_algorithm: Option<String>,
    pub(crate) sse_customer_key: Option<String>,
    pub(crate) sse_customer_key_md5: Option<String>,
    pub(crate) ssekms_key_id: Option<String>,
    pub(crate) content_md5: Option<String>,
    pub(crate) object_lock_legal_hold_status: Option<String>,
    pub(crate) object_lock_mode: Option<String>,
    pub(crate) storage_class: Option<String>,
    pub(crate) website_redirect_location: Option<String>,
    pub(crate) checksum_algorithm: Option<String>,
    pub(crate) checksum_crc32: Option<String>,
    pub(crate) checksum_crc32c: Option<String>,
    pub(crate) checksum_sha1: Option<String>,
    pub(crate) checksum_sha256: Option<String>,
    pub(crate) checksum_crc64nvme: Option<String>,
}

#[derive(Debug, Default)]
pub(crate) struct PutObjectWriteOutput {
    pub(crate) e_tag: Option<String>,
    pub(crate) version_id: Option<String>,
    pub(crate) expiration: Option<String>,
    pub(crate) server_side_encryption: Option<String>,
    pub(crate) sse_customer_algorithm: Option<String>,
    pub(crate) sse_customer_key_md5: Option<String>,
    pub(crate) ssekms_key_id: Option<String>,
    pub(crate) checksum_crc32: Option<String>,
    pub(crate) checksum_crc32c: Option<String>,
    pub(crate) checksum_sha1: Option<String>,
    pub(crate) checksum_sha256: Option<String>,
    pub(crate) checksum_crc64nvme: Option<String>,
    pub(crate) headers: http::HeaderMap,
    pub(crate) status: Option<http::StatusCode>,
}

impl PutObjectWriteInput {
    /// Gateway frames publish to the same source that checksum readers observe.
    pub(crate) fn capture_frame_trailers(&mut self) {
        let trailers = std::sync::Arc::new(super::native_body::FrameTrailers::default());
        self.frame_trailers = Some(trailers.clone());
        self.trailing_headers = Some(trailers);
    }
}

#[cfg(test)]
impl PutObjectWriteInput {
    pub(crate) fn from_legacy(
        input: crate::app::storage_api::s3::PutObjectInput,
        trailing_headers: Option<crate::app::storage_api::s3::TrailingHeaders>,
    ) -> Self {
        use crate::app::storage_api::s3::dto::ETagCondition;
        Self {
            bucket: input.bucket,
            key: input.key,
            body: input.body.map(crate::app::object::legacy_put_body),
            post_archive: None,
            frame_trailers: None,
            trailing_headers: crate::app::trailer_adapter::trailer_source(trailing_headers),
            content_length: input.content_length,
            metadata: input.metadata,
            metadata_headers: None,
            checksum_headers: None,
            specific_if_none_match: matches!(input.if_none_match, Some(ETagCondition::ETag(_))),
            object_lock_retain_until_date: input.object_lock_retain_until_date.map(time::OffsetDateTime::from),
            cache_control: input.cache_control,
            content_disposition: input.content_disposition,
            content_encoding: input.content_encoding,
            content_language: input.content_language,
            content_type: input.content_type,
            expires: input.expires,
            tagging: input.tagging,
            version_id: input.version_id,
            server_side_encryption: input.server_side_encryption.map(|value| value.as_str().to_owned()),
            sse_customer_algorithm: input.sse_customer_algorithm,
            sse_customer_key: input.sse_customer_key,
            sse_customer_key_md5: input.sse_customer_key_md5,
            ssekms_key_id: input.ssekms_key_id,
            content_md5: input.content_md5,
            object_lock_legal_hold_status: input.object_lock_legal_hold_status.map(|value| value.as_str().to_owned()),
            object_lock_mode: input.object_lock_mode.map(|value| value.as_str().to_owned()),
            storage_class: input.storage_class.map(|value| value.as_str().to_owned()),
            website_redirect_location: input.website_redirect_location,
            checksum_algorithm: input.checksum_algorithm.map(|value| value.as_str().to_owned()),
            checksum_crc32: input.checksum_crc32,
            checksum_crc32c: input.checksum_crc32c,
            checksum_sha1: input.checksum_sha1,
            checksum_sha256: input.checksum_sha256,
            checksum_crc64nvme: input.checksum_crc64nvme,
        }
    }
}
