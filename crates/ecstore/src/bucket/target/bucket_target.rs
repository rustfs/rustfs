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

use crate::error::{Error, Result};
use rmp_serde::Serializer as rmpSerializer;
use serde::{Deserialize, Serialize};
use serde_with::{DurationSeconds, serde_as};
use std::{
    fmt::{self, Display},
    time::Duration,
};
use time::OffsetDateTime;
use url::Url;

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct Credentials {
    #[serde(rename = "accessKey")]
    pub access_key: String,
    #[serde(rename = "secretKey")]
    pub secret_key: String,
    pub session_token: Option<String>,
    pub expiration: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub enum ServiceType {
    #[default]
    Replication,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct LatencyStat {
    pub curr: Duration, // 当前延迟
    pub avg: Duration,  // 平均延迟
    pub max: Duration,  // 最大延迟
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub enum BucketTargetType {
    #[default]
    None,
    #[serde(rename = "replication")]
    ReplicationService,
    #[serde(rename = "ilm")]
    IlmService,
}

impl BucketTargetType {
    pub fn is_valid(&self) -> bool {
        match self {
            BucketTargetType::None => false,
            BucketTargetType::ReplicationService | BucketTargetType::IlmService => true,
        }
    }
}

impl fmt::Display for BucketTargetType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BucketTargetType::None => write!(f, ""),
            BucketTargetType::ReplicationService => write!(f, "replication"),
            BucketTargetType::IlmService => write!(f, "ilm"),
        }
    }
}

// 定义 BucketTarget 结构体
#[derive(Debug, Deserialize, Serialize, Default, Clone)]
#[serde_as]
pub struct BucketTarget {
    #[serde(rename = "sourcebucket")]
    pub source_bucket: String,

    pub endpoint: String,

    pub credentials: Option<Credentials>,
    #[serde(rename = "targetbucket")]
    pub target_bucket: String,

    pub secure: bool,
    pub path: String,

    pub api: String,

    pub arn: String,
    #[serde(rename = "type")]
    pub target_type: BucketTargetType,

    pub region: String,

    pub bandwidth_limit: i64,

    #[serde(rename = "replicationSync")]
    pub replication_sync: bool,

    pub storage_class: String,
    #[serde(rename = "healthCheckDuration")]
    #[serde_as(as = "DurationSeconds<u64>")]
    pub health_check_duration: Duration,
    #[serde(rename = "disableProxy")]
    pub disable_proxy: bool,

    #[serde(rename = "resetBeforeDate")]
    pub reset_before_date: Option<OffsetDateTime>,
    pub reset_id: String,
    #[serde(rename = "totalDowntime")]
    #[serde_as(as = "DurationSeconds<u64>")]
    pub total_downtime: Duration,

    pub last_online: Option<OffsetDateTime>,
    #[serde(rename = "isOnline")]
    pub online: bool,

    pub latency: LatencyStat,

    pub deployment_id: String,

    pub edge: bool,
    #[serde(rename = "edgeSyncBeforeExpiry")]
    pub edge_sync_before_expiry: bool,
    #[serde(rename = "offlineCount")]
    pub offline_count: u64,
}

impl BucketTarget {
    pub fn is_empty(self) -> bool {
        self.target_bucket.is_empty() && self.endpoint.is_empty() && self.arn.is_empty()
    }
    pub fn url(&self) -> Result<Url> {
        let scheme = if self.secure { "https" } else { "http" };
        Url::parse(&format!("{}://{}", scheme, self.endpoint)).map_err(Error::other)
    }
}

impl Display for BucketTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ", self.endpoint)?;
        write!(f, "{}", self.target_bucket.clone())?;
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct BucketTargets {
    pub targets: Vec<BucketTarget>,
}

impl BucketTargets {
    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        self.serialize(&mut rmpSerializer::new(&mut buf).with_struct_map())?;

        Ok(buf)
    }

    pub fn unmarshal(buf: &[u8]) -> Result<Self> {
        let t: BucketTargets = rmp_serde::from_slice(buf)?;
        Ok(t)
    }

    pub fn is_empty(&self) -> bool {
        if self.targets.is_empty() {
            return true;
        }

        for target in &self.targets {
            if !target.clone().is_empty() {
                return false;
            }
        }

        true
    }
}
