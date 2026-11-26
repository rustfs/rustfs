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

#[derive(Default)]
pub(crate) struct CopySourceDates {
    modified_since: Option<i64>,
    unmodified_since: Option<i64>,
}

impl CopySourceDates {
    pub(crate) fn new(
        modified_since: Option<rustfs_gateway_types::Timestamp>,
        unmodified_since: Option<rustfs_gateway_types::Timestamp>,
    ) -> Self {
        Self {
            modified_since: modified_since.map(|date| date.secs()),
            unmodified_since: unmodified_since.map(|date| date.secs()),
        }
    }

    pub(crate) fn permits_copy(
        &self,
        if_match: Option<&CopySourceCondition>,
        if_none_match: Option<&CopySourceCondition>,
        etag: Option<&str>,
        last_modified: Option<OffsetDateTime>,
    ) -> bool {
        let matches = |condition: &CopySourceCondition| condition.strong().is_some_and(|tag| Some(tag) == etag);
        if if_match.is_some_and(|condition| !matches(condition)) || if_none_match.is_some_and(matches) {
            return false;
        }
        let Some(last_modified) = last_modified else { return true };
        // HTTP dates have one-second precision. Each ETag condition takes precedence over its paired date.
        let seconds = last_modified.unix_timestamp();
        (if_none_match.is_some() || self.modified_since.is_none_or(|date| seconds > date))
            && (if_match.is_some() || self.unmodified_since.is_none_or(|date| seconds <= date))
    }
}

// Deliberately no Debug: these fields may contain customer encryption keys.
#[cfg_attr(test, derive(Default))]
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
    pub(crate) copy_source_dates: CopySourceDates,
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
mod source_condition_tests {
    use super::{CopySourceCondition, CopySourceDates};
    use rustfs_gateway_types::Timestamp;
    use time::OffsetDateTime;

    #[test]
    fn copy_source_dates_compare_the_selected_versions_http_second() {
        let stored = OffsetDateTime::from_unix_timestamp_nanos(10_900_000_000).unwrap();
        for (modified, unmodified, expected) in [
            (Some(9), None, true),
            (Some(10), None, false),
            (None, Some(9), false),
            (None, Some(10), true),
            (Some(9), Some(10), true),
        ] {
            let dates = CopySourceDates::new(modified.map(Timestamp::from_secs), unmodified.map(Timestamp::from_secs));
            assert_eq!(dates.permits_copy(None, None, None, Some(stored)), expected);
        }
    }

    #[test]
    fn copy_source_etag_precedence_only_applies_to_the_paired_date() {
        let matched = CopySourceCondition::Strong("etag".to_owned());
        let other = CopySourceCondition::Strong("other".to_owned());
        let stored = Some(OffsetDateTime::UNIX_EPOCH);
        let unmodified = CopySourceDates::new(None, Some(Timestamp::from_secs(-1)));
        assert!(unmodified.permits_copy(Some(&matched), None, Some("etag"), stored));
        assert!(!unmodified.permits_copy(None, Some(&other), Some("etag"), stored));
        let modified = CopySourceDates::new(Some(Timestamp::from_secs(-1)), None);
        assert!(!modified.permits_copy(None, Some(&matched), Some("etag"), stored));
        assert!(modified.permits_copy(None, None, Some("etag"), stored));
    }
}
