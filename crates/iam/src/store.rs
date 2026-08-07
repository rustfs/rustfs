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

pub mod object;

use crate::cache::Cache;
use crate::error::Result;
use rustfs_policy::{auth::UserIdentity, policy::PolicyDoc};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::{HashMap, HashSet};
use time::OffsetDateTime;

#[async_trait::async_trait]
pub trait Store: Clone + Send + Sync + 'static {
    fn has_watcher(&self) -> bool;
    async fn save_iam_config<Item: Serialize + Send>(&self, item: Item, path: impl AsRef<str> + Send) -> Result<()>;
    async fn load_iam_config<Item: DeserializeOwned>(&self, path: impl AsRef<str> + Send) -> Result<Item>;
    async fn delete_iam_config(&self, path: impl AsRef<str> + Send) -> Result<()>;

    async fn save_user_identity(&self, name: &str, user_type: UserType, item: UserIdentity, ttl: Option<usize>) -> Result<()>;
    async fn delete_user_identity(&self, name: &str, user_type: UserType) -> Result<()>;
    async fn load_user_identity(&self, name: &str, user_type: UserType) -> Result<UserIdentity>;

    async fn load_user(&self, name: &str, user_type: UserType, m: &mut HashMap<String, UserIdentity>) -> Result<()>;
    async fn load_users(&self, user_type: UserType, m: &mut HashMap<String, UserIdentity>) -> Result<()>;
    async fn load_secret_key(&self, name: &str, user_type: UserType) -> Result<String>;

    async fn save_group_info(&self, name: &str, item: GroupInfo) -> Result<()>;
    async fn delete_group_info(&self, name: &str) -> Result<()>;
    async fn load_group(&self, name: &str, m: &mut HashMap<String, GroupInfo>) -> Result<()>;
    async fn load_groups(&self, m: &mut HashMap<String, GroupInfo>) -> Result<()>;

    async fn save_policy_doc(&self, name: &str, item: PolicyDoc) -> Result<()>;
    async fn delete_policy_doc(&self, name: &str) -> Result<()>;
    async fn load_policy(&self, name: &str) -> Result<PolicyDoc>;
    async fn load_policy_doc(&self, name: &str, m: &mut HashMap<String, PolicyDoc>) -> Result<()>;
    async fn load_policy_docs(&self, m: &mut HashMap<String, PolicyDoc>) -> Result<()>;

    async fn save_mapped_policy(
        &self,
        name: &str,
        user_type: UserType,
        is_group: bool,
        item: MappedPolicy,
        ttl: Option<usize>,
    ) -> Result<()>;
    async fn delete_mapped_policy(&self, name: &str, user_type: UserType, is_group: bool) -> Result<()>;
    async fn load_mapped_policy(
        &self,
        name: &str,
        user_type: UserType,
        is_group: bool,
        m: &mut HashMap<String, MappedPolicy>,
    ) -> Result<()>;
    async fn load_mapped_policies(
        &self,
        user_type: UserType,
        is_group: bool,
        m: &mut HashMap<String, MappedPolicy>,
    ) -> Result<()>;

    async fn load_all(&self, cache: &Cache) -> Result<()>;

    // Lock-free variants used by the cross-node notification handlers.
    //
    // Notification-path cache refreshes are asynchronous, best-effort, and
    // already tolerate stale values (the periodic reload converges them), so
    // they must not depend on the node-counted namespace-lock quorum — the
    // same rationale as the lock-free bootstrap `load_all` (rustfs#4304;
    // MinIO's readConfig takes no distributed lock either). The defaults
    // forward to the locked variants so existing `Store` implementations
    // (including test mocks) keep their behavior; `ObjectStore` overrides
    // them with lock-free reads.
    async fn load_user_no_lock(&self, name: &str, user_type: UserType, m: &mut HashMap<String, UserIdentity>) -> Result<()> {
        self.load_user(name, user_type, m).await
    }
    async fn load_group_no_lock(&self, name: &str, m: &mut HashMap<String, GroupInfo>) -> Result<()> {
        self.load_group(name, m).await
    }
    async fn load_policy_doc_no_lock(&self, name: &str, m: &mut HashMap<String, PolicyDoc>) -> Result<()> {
        self.load_policy_doc(name, m).await
    }
    async fn load_mapped_policy_no_lock(
        &self,
        name: &str,
        user_type: UserType,
        is_group: bool,
        m: &mut HashMap<String, MappedPolicy>,
    ) -> Result<()> {
        self.load_mapped_policy(name, user_type, is_group, m).await
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum UserType {
    Svc,
    Sts,
    Reg,
    None,
}

impl UserType {
    pub fn prefix(&self) -> &'static str {
        match self {
            UserType::Svc => "service-accounts/",
            UserType::Sts => "sts/",
            UserType::Reg => "users/",
            UserType::None => "",
        }
    }
    pub fn to_u64(&self) -> u64 {
        match self {
            UserType::Svc => 1,
            UserType::Sts => 2,
            UserType::Reg => 3,
            UserType::None => 0,
        }
    }

    pub fn from_u64(u64: u64) -> Option<Self> {
        match u64 {
            1 => Some(UserType::Svc),
            2 => Some(UserType::Sts),
            3 => Some(UserType::Reg),
            0 => Some(UserType::None),
            _ => None,
        }
    }
}

/// Encode a [`UserType`] as the site-replication wire value for
/// `SRPolicyMapping.userType` / `SRCredInfo.iamUserType`.
///
/// The wire uses MinIO's `IAMUserType` table (cmd/iam.go):
///
/// | wire | MinIO meaning |
/// |------|---------------|
/// | -1   | unknown       |
/// | 0    | regUser       |
/// | 1    | stsUser       |
/// | 2    | svcUser       |
///
/// This is deliberately distinct from the internal encoding
/// [`UserType::to_u64`]/[`UserType::from_u64`] (None=0, Svc=1, Sts=2, Reg=3),
/// which is used by intra-cluster node RPC and must never change (a rolling
/// restart mixes old and new nodes on that RPC). Do not "unify" the two
/// tables: internal values on the SR wire mislabel users on MinIO peers.
///
/// Group mappings always encode as 0: MinIO routes group mappings by the
/// `isGroup` flag (userType is effectively ignored), and pre-fix RustFS peers
/// sent 0 for groups, so 0 is the one value every peer generation accepts.
pub fn sr_wire_user_type(user_type: UserType, is_group: bool) -> i64 {
    if is_group {
        return 0;
    }
    match user_type {
        UserType::Reg | UserType::None => 0,
        UserType::Sts => 1,
        UserType::Svc => 2,
    }
}

/// Decode a site-replication wire `userType` value (see [`sr_wire_user_type`]
/// for the table) into a [`UserType`].
///
/// - `-1` (MinIO unknown, sent for group mappings) maps to [`UserType::None`];
///   `policy_db_set` routes group items by `is_group`, and for non-group items
///   `None` shares the users prefix with `Reg`.
/// - `3` is a permanent alias for [`UserType::Reg`]: pre-fix RustFS peers sent
///   the internal encoding (`Reg.to_u64() == 3`) on the wire. Keep it forever
///   for mixed-version site replication; do not remove.
/// - Anything else is unknown and rejected (`None`), so callers fail closed.
pub fn user_type_from_sr_wire(v: i64) -> Option<UserType> {
    match v {
        -1 => Some(UserType::None),
        0 => Some(UserType::Reg),
        1 => Some(UserType::Sts),
        2 => Some(UserType::Svc),
        3 => Some(UserType::Reg),
        _ => None,
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct MappedPolicy {
    pub version: i64,
    /// policy, legacy: policies. Serialize as policy.
    #[serde(rename = "policy", alias = "policies")]
    pub policies: String,
    /// updatedAt (RFC3339), legacy: update_at. Serialize as updatedAt.
    #[serde(rename = "updatedAt", alias = "update_at", with = "rustfs_policy::serde_datetime")]
    pub update_at: OffsetDateTime,
}

impl Default for MappedPolicy {
    fn default() -> Self {
        Self {
            version: 0,
            policies: "".to_owned(),
            update_at: OffsetDateTime::now_utc(),
        }
    }
}

impl MappedPolicy {
    pub fn new(policy: &str) -> Self {
        Self {
            version: 1,
            policies: policy.to_owned(),
            update_at: OffsetDateTime::now_utc(),
        }
    }

    pub fn to_slice(&self) -> Vec<String> {
        self.policies
            .split(",")
            .filter(|v| !v.trim().is_empty())
            .map(|v| v.to_string())
            .collect()
    }

    pub fn policy_set(&self) -> HashSet<String> {
        self.policies
            .split(",")
            .filter(|v| !v.trim().is_empty())
            .map(|v| v.to_string())
            .collect()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct GroupInfo {
    pub version: i64,
    pub status: String,
    pub members: Vec<String>,
    /// updatedAt (RFC3339), legacy: update_at. Serialize as updatedAt.
    #[serde(
        rename = "updatedAt",
        alias = "update_at",
        default,
        with = "rustfs_policy::serde_datetime::option"
    )]
    pub update_at: Option<OffsetDateTime>,
}

impl GroupInfo {
    pub fn new(members: Vec<String>) -> Self {
        Self {
            version: 1,
            status: "enabled".to_owned(),
            members,
            update_at: Some(OffsetDateTime::now_utc()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{GroupInfo, MappedPolicy, UserType, sr_wire_user_type, user_type_from_sr_wire};

    /// Site-replication inbound decode of `SRPolicyMapping.userType` must
    /// follow MinIO IAMUserType wire semantics (cmd/iam.go): stsUser = 1.
    /// The internal `UserType::from_u64` table maps 1 to Svc — reusing it at
    /// the SR boundary lands federated STS mappings under the wrong prefix
    /// and silently drops their effect.
    #[test]
    fn sr_inbound_decodes_minio_sts_wire_value_as_sts() {
        assert_eq!(user_type_from_sr_wire(1), Some(UserType::Sts));
    }

    /// Wire-constant contract: literal MinIO IAMUserType values (cmd/iam.go).
    /// WARNING: these literals are the cross-vendor wire format. Never "tidy"
    /// them to match `UserType::to_u64`/`from_u64` — that internal table
    /// (None=0, Svc=1, Sts=2, Reg=3) belongs to intra-cluster node RPC only.
    #[test]
    fn sr_wire_decode_matches_minio_iam_user_type_table() {
        assert_eq!(user_type_from_sr_wire(-1), Some(UserType::None)); // MinIO unknown (group mappings)
        assert_eq!(user_type_from_sr_wire(0), Some(UserType::Reg)); // MinIO regUser
        assert_eq!(user_type_from_sr_wire(1), Some(UserType::Sts)); // MinIO stsUser
        assert_eq!(user_type_from_sr_wire(2), Some(UserType::Svc)); // MinIO svcUser
        // Permanent alias: pre-fix RustFS peers sent internal Reg=3 on the wire.
        assert_eq!(user_type_from_sr_wire(3), Some(UserType::Reg));
        // Unknown values fail closed.
        assert_eq!(user_type_from_sr_wire(4), None);
        assert_eq!(user_type_from_sr_wire(-2), None);
    }

    /// Wire-constant contract for the outbound direction.
    #[test]
    fn sr_wire_encode_matches_minio_iam_user_type_table() {
        assert_eq!(sr_wire_user_type(UserType::Reg, false), 0); // MinIO regUser
        assert_eq!(sr_wire_user_type(UserType::Sts, false), 1); // MinIO stsUser
        assert_eq!(sr_wire_user_type(UserType::Svc, false), 2); // MinIO svcUser
        assert_eq!(sr_wire_user_type(UserType::None, false), 0);
        // Group mappings always go out as 0 — the value both MinIO (routes by
        // isGroup) and pre-fix RustFS peers accept.
        for ut in [UserType::Reg, UserType::Sts, UserType::Svc, UserType::None] {
            assert_eq!(sr_wire_user_type(ut, true), 0);
        }
    }

    /// Mixed-version matrix: every value a peer generation can emit decodes to
    /// a `UserType` the receiver stores correctly.
    #[test]
    fn sr_wire_round_trip_covers_old_rustfs_and_minio_peers() {
        // Old RustFS outbound: user mappings as internal Reg=3, groups as 0.
        assert_eq!(user_type_from_sr_wire(3), Some(UserType::Reg));
        assert_eq!(user_type_from_sr_wire(0), Some(UserType::Reg));
        // New RustFS outbound decodes on its own kind (self round-trip).
        for (ut, is_group) in [
            (UserType::Reg, false),
            (UserType::Sts, false),
            (UserType::Svc, false),
            (UserType::None, true),
        ] {
            assert!(user_type_from_sr_wire(sr_wire_user_type(ut, is_group)).is_some());
        }
        // Internal RPC encoding is untouched (rolling-restart contract).
        assert_eq!(UserType::None.to_u64(), 0);
        assert_eq!(UserType::Svc.to_u64(), 1);
        assert_eq!(UserType::Sts.to_u64(), 2);
        assert_eq!(UserType::Reg.to_u64(), 3);
        assert_eq!(UserType::from_u64(1), Some(UserType::Svc));
    }

    /// uses RFC3339 for updatedAt. MappedPolicy must serialize as RFC3339.
    #[test]
    fn test_mapped_policy_timestamps_serialize_as_rfc3339() {
        let mp = MappedPolicy::new("readwrite");
        let json = serde_json::to_string(&mp).expect("serialize");
        assert!(json.contains('T'), "MappedPolicy updatedAt should be RFC3339; got: {}", json);
        assert!(
            json.contains('Z') || json.contains("+00:00"),
            "MappedPolicy updatedAt should be RFC3339; got: {}",
            json
        );
    }

    /// Deserialize MappedPolicy from JSON (RFC3339 updatedAt).
    #[test]
    fn test_mapped_policy_deserialize_minio_style_rfc3339() {
        let minio_style = r#"{"version":1,"policy":"readwrite","updatedAt":"2025-03-07T12:00:00Z"}"#;
        let mp: MappedPolicy = serde_json::from_str(minio_style).expect("deserialize");
        assert_eq!(mp.policies, "readwrite");
    }

    /// GroupInfo updatedAt: uses RFC3339.
    #[test]
    fn test_group_info_timestamps_serialize_as_rfc3339() {
        let g = GroupInfo::new(vec!["u1".to_string()]);
        let json = serde_json::to_string(&g).expect("serialize");
        assert!(json.contains('T'), "GroupInfo updatedAt should be RFC3339; got: {}", json);
    }

    /// Deserialize GroupInfo from JSON (RFC3339 updatedAt).
    #[test]
    fn test_group_info_deserialize_minio_style_rfc3339() {
        let minio_style = r#"{"version":1,"status":"enabled","members":["u1"],"updatedAt":"2025-03-07T12:00:00Z"}"#;
        let g: GroupInfo = serde_json::from_str(minio_style).expect("deserialize");
        assert_eq!(g.members, ["u1"]);
        assert!(g.update_at.is_some());
    }
}
