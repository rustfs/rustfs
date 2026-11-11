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

use crate::name::{EngineName, OperationName};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Object operations an engine may serve for its buckets through the S3 API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EngineObjectOperation {
    GetObject,
    HeadObject,
    PutObject,
    DeleteObject,
    ListObjects,
}

/// Bucket features an engine may refuse for its buckets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BucketFeature {
    Versioning,
    ObjectLock,
    Quota,
    Replication,
    Lifecycle,
    Notifications,
    TableCatalog,
}

impl BucketFeature {
    pub const ALL: [Self; 7] = [
        Self::Versioning,
        Self::ObjectLock,
        Self::Quota,
        Self::Replication,
        Self::Lifecycle,
        Self::Notifications,
        Self::TableCatalog,
    ];
}

/// The access an engine custom operation needs, evaluated with the same IAM and
/// bucket-policy rules as the matching S3 action before the engine is called.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationAction {
    /// `s3:GetObject`
    GetObject,
    /// `s3:PutObject`
    PutObject,
    /// `s3:DeleteObject`
    DeleteObject,
    /// `s3:ListBucket`
    ListBucket,
}

/// A custom operation and the access it requires.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CustomOperationSpec {
    pub name: OperationName,
    pub action: OperationAction,
    /// Largest request body the framework accepts for this operation.
    pub max_request_bytes: u64,
}

/// What an engine serves and refuses.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct EngineCapabilities {
    /// Object operations served through the S3 API; any other object operation
    /// on a bound bucket is refused by the framework.
    pub object_operations: BTreeSet<EngineObjectOperation>,
    pub custom_operations: Vec<CustomOperationSpec>,
    /// Whether ListObjects reflects every acknowledged write.
    pub consistent_listing: bool,
    /// Bucket features refused when configured on a bound bucket.
    pub refused_features: BTreeSet<BucketFeature>,
    /// The engine moves object data over RDMA when a request carries a
    /// transfer descriptor (`ObjectWriteMeta::rdma`, the `rdma` argument of
    /// `get_object`). Engines without it never receive RDMA requests.
    #[serde(default)]
    pub rdma: bool,
}

impl EngineCapabilities {
    pub fn serves(&self, operation: EngineObjectOperation) -> bool {
        self.object_operations.contains(&operation)
    }

    pub fn custom_operation(&self, name: &OperationName) -> Option<&CustomOperationSpec> {
        self.custom_operations.iter().find(|spec| &spec.name == name)
    }

    pub fn refuses(&self, feature: BucketFeature) -> bool {
        self.refused_features.contains(&feature)
    }
}

/// Identity and capabilities of an engine, as listed in the extension catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EngineDescriptor {
    pub name: EngineName,
    /// Engine implementation version.
    pub version: String,
    /// Highest bucket config schema version the engine accepts.
    pub config_version: u32,
    pub capabilities: EngineCapabilities,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capabilities_answer_membership() {
        let op = OperationName::new("prefix-query").expect("valid operation name");
        let caps = EngineCapabilities {
            object_operations: BTreeSet::from([EngineObjectOperation::GetObject]),
            custom_operations: vec![CustomOperationSpec {
                name: op.clone(),
                action: OperationAction::GetObject,
                max_request_bytes: 1 << 20,
            }],
            consistent_listing: false,
            refused_features: BTreeSet::from(BucketFeature::ALL),
            rdma: false,
        };
        assert!(caps.serves(EngineObjectOperation::GetObject));
        assert!(!caps.serves(EngineObjectOperation::PutObject));
        assert_eq!(caps.custom_operation(&op).map(|spec| spec.action), Some(OperationAction::GetObject));
        assert!(caps.custom_operation(&OperationName::new("other").expect("valid")).is_none());
        assert!(caps.refuses(BucketFeature::Versioning));
    }

    #[test]
    fn capabilities_serialize_in_stable_order() {
        let caps = EngineCapabilities {
            object_operations: BTreeSet::from([EngineObjectOperation::PutObject, EngineObjectOperation::GetObject]),
            ..Default::default()
        };
        let json = serde_json::to_string(&caps).expect("capabilities serialize");
        assert!(json.contains(r#""object_operations":["get_object","put_object"]"#), "{json}");
    }
}
