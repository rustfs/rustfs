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
    /// A test's s3s fixture request as the envelope and input a gateway handler hands the use case.
    pub(crate) fn from_test_request(
        req: crate::app::storage_api::s3::S3Request<crate::app::storage_api::s3::PutObjectInput>,
    ) -> (crate::app::RequestEnvelope, Self) {
        let envelope = crate::app::RequestEnvelope::from_s3s(&req);
        (envelope, Self::from_legacy(req.input, req.trailing_headers))
    }
}
