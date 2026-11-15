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

//! Owned UploadPart values projected by each S3 edge.

use crate::app::object::FrameTrailers;
use rustfs_gateway_stream::Body;
#[cfg(any(feature = "gateway-stack", test))]
use rustfs_gateway_types::{BucketName, ObjectKey, UploadIdClaim};
use rustfs_rio::SharedTrailerSource;
use std::sync::Arc;

pub(crate) enum UploadPartTarget {
    Legacy {
        bucket: String,
        key: String,
        upload_id: String,
    },
    #[cfg(any(feature = "gateway-stack", test))]
    Gateway {
        bucket: BucketName,
        key: ObjectKey,
        upload_id: UploadIdClaim,
    },
}

impl UploadPartTarget {
    pub(super) fn resource(&self) -> (&str, &str) {
        match self {
            Self::Legacy { bucket, key, .. } => (bucket, key),
            #[cfg(any(feature = "gateway-stack", test))]
            Self::Gateway { bucket, key, .. } => (bucket.as_str(), key.as_str()),
        }
    }
}

pub(crate) struct UploadPartWriteInput {
    pub(crate) target: UploadPartTarget,
    pub(crate) part_number: i32,
    pub(crate) content_length: Option<i64>,
    pub(crate) body: Option<Body>,
    pub(crate) trailing_headers: Option<SharedTrailerSource>,
    pub(crate) frame_trailers: Option<Arc<FrameTrailers>>,
    pub(crate) content_md5: Option<String>,
    pub(crate) checksum_algorithm: Option<String>,
    pub(crate) checksum_crc32: Option<String>,
    pub(crate) checksum_crc32c: Option<String>,
    pub(crate) checksum_crc64nvme: Option<String>,
    pub(crate) checksum_sha1: Option<String>,
    pub(crate) checksum_sha256: Option<String>,
    pub(crate) sse_customer_algorithm: Option<String>,
    pub(crate) sse_customer_key: Option<String>,
    pub(crate) sse_customer_key_md5: Option<String>,
}

pub(crate) struct UploadPartWriteOutput {
    pub(crate) e_tag: Option<String>,
    pub(crate) checksum_crc32: Option<String>,
    pub(crate) checksum_crc32c: Option<String>,
    pub(crate) checksum_crc64nvme: Option<String>,
    pub(crate) checksum_sha1: Option<String>,
    pub(crate) checksum_sha256: Option<String>,
    pub(crate) server_side_encryption: Option<String>,
    pub(crate) ssekms_key_id: Option<String>,
    pub(crate) sse_customer_algorithm: Option<String>,
    pub(crate) sse_customer_key_md5: Option<String>,
    pub(crate) headers: http::HeaderMap,
}
