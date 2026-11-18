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

//! Values consumed and produced by multipart initiation.

use std::collections::HashMap;
use time::OffsetDateTime;

/// Keeps the decoded values in their original form until the use case validates them.
// The customer key must never appear in diagnostics.
pub(crate) struct CreateMultipartUploadFields {
    pub(crate) bucket: String,
    pub(crate) key: String,
    pub(crate) metadata: Option<HashMap<String, String>>,
    pub(crate) tagging: Option<String>,
    pub(crate) storage_class: Option<String>,
    pub(crate) server_side_encryption: Option<String>,
    pub(crate) sse_customer_algorithm: Option<String>,
    pub(crate) sse_customer_key: Option<String>,
    pub(crate) sse_customer_key_md5: Option<String>,
    pub(crate) ssekms_key_id: Option<String>,
    pub(crate) object_lock_legal_hold_status: Option<String>,
    pub(crate) object_lock_mode: Option<String>,
    pub(crate) object_lock_retain_until_date: Option<OffsetDateTime>,
}

pub(crate) struct CreateMultipartUploadResult {
    pub(crate) bucket: String,
    pub(crate) key: String,
    pub(crate) upload_id: String,
    pub(crate) server_side_encryption: Option<String>,
    pub(crate) sse_customer_algorithm: Option<String>,
    pub(crate) sse_customer_key_md5: Option<String>,
    pub(crate) ssekms_key_id: Option<String>,
    pub(crate) checksum_algorithm: Option<String>,
    pub(crate) checksum_type: Option<String>,
}
