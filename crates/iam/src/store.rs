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

#[derive(Serialize, Deserialize, Clone)]
pub struct MappedPolicy {
    pub version: i64,
    pub policies: String,
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
