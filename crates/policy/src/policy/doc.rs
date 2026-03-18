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

use serde::{Deserialize, Serialize, de::Error};
use time::OffsetDateTime;

use super::Policy;

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct PolicyDoc {
    /// Version (omitempty), legacy: version.
    #[serde(rename = "Version", alias = "version", default)]
    pub version: i64,
    /// Policy, legacy: policy. Serialize as Policy.
    #[serde(rename = "Policy", alias = "policy")]
    pub policy: Policy,
    /// CreateDate (RFC3339), legacy: create_date. Serialize as CreateDate.
    #[serde(
        rename = "CreateDate",
        alias = "create_date",
        default,
        with = "crate::serde_datetime::option"
    )]
    pub create_date: Option<OffsetDateTime>,
    /// UpdateDate (RFC3339), legacy: update_date. Serialize as UpdateDate.
    #[serde(
        rename = "UpdateDate",
        alias = "update_date",
        default,
        with = "crate::serde_datetime::option"
    )]
    pub update_date: Option<OffsetDateTime>,
}

impl PolicyDoc {
    pub fn new(policy: Policy) -> Self {
        Self {
            version: 1,
            policy,
            create_date: Some(OffsetDateTime::now_utc()),
            update_date: Some(OffsetDateTime::now_utc()),
        }
    }

    pub fn update(&mut self, policy: Policy) {
        self.version += 1;
        self.policy = policy;
        self.update_date = Some(OffsetDateTime::now_utc());

        if self.create_date.is_none() {
            self.create_date = self.update_date;
        }
    }

    pub fn default_policy(policy: Policy) -> Self {
        Self {
            version: 1,
            policy,
            create_date: None,
            update_date: None,
        }
    }
}

impl TryFrom<Vec<u8>> for PolicyDoc {
    type Error = serde_json::Error;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        // Try to parse as PolicyDoc first
        if let Ok(policy_doc) = serde_json::from_slice::<PolicyDoc>(&value) {
            return Ok(policy_doc);
        }

        // Fall back to parsing as Policy and wrap in PolicyDoc
        serde_json::from_slice::<Policy>(&value)
            .map(|policy| Self {
                policy,
                ..Default::default()
            })
            .map_err(|_| serde_json::Error::custom("Failed to parse as PolicyDoc or Policy".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::Policy;

    #[test]
    fn test_policy_doc_timestamps_serialize_as_rfc3339() {
        let policy = Policy::default();
        let doc = PolicyDoc::new(policy);
        let json = serde_json::to_string(&doc).expect("serialize");
        // RFC3339 uses 'T' between date and time and 'Z' or offset for UTC
        assert!(json.contains('T'), "PolicyDoc timestamps should be RFC3339 (contain 'T'); got: {}", json);
        assert!(
            json.contains('Z') || json.contains("+00:00"),
            "PolicyDoc timestamps should be RFC3339 (contain 'Z' or +00:00); got: {}",
            json
        );
    }

    #[test]
    fn test_policy_doc_deserialize_minio_style_rfc3339_timestamps() {
        let minio_style = r#"{"Version":1,"Policy":{"Version":"2012-10-17","Statement":[]},"CreateDate":"2025-03-07T12:00:00Z","UpdateDate":"2025-03-07T12:00:00Z"}"#;
        let doc: PolicyDoc = serde_json::from_str(minio_style).expect("deserialize MinIO-style JSON");
        assert_eq!(doc.version, 1);
        assert!(doc.create_date.is_some());
        assert!(doc.update_date.is_some());
    }

    /// Round-trip: serialize then deserialize PolicyDoc; timestamps must match.
    #[test]
    fn test_policy_doc_timestamp_roundtrip() {
        let policy = Policy::default();
        let doc = PolicyDoc::new(policy);
        let json = serde_json::to_string(&doc).expect("serialize");
        let restored: PolicyDoc = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(doc.create_date, restored.create_date);
        assert_eq!(doc.update_date, restored.update_date);
    }
}
