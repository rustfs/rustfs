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
