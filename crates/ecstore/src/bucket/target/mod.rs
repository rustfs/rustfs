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

use crate::error::Result;
use rmp_serde::Serializer as rmpSerializer;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

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
    curr: u64, // 当前延迟
    avg: u64,  // 平均延迟
    max: u64,  // 最大延迟
}

// 定义 BucketTarget 结构体
#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct BucketTarget {
    #[serde(rename = "sourcebucket")]
    pub source_bucket: String,

    pub endpoint: String,

    pub credentials: Option<Credentials>,
    #[serde(rename = "targetbucket")]
    pub target_bucket: String,

    secure: bool,
    pub path: Option<String>,

    api: Option<String>,

    pub arn: Option<String>,
    #[serde(rename = "type")]
    pub type_: Option<String>,

    pub region: Option<String>,

    bandwidth_limit: Option<i64>,

    #[serde(rename = "replicationSync")]
    replication_sync: bool,

    storage_class: Option<String>,
    #[serde(rename = "healthCheckDuration")]
    health_check_duration: u64,
    #[serde(rename = "disableProxy")]
    disable_proxy: bool,

    #[serde(rename = "resetBeforeDate")]
    reset_before_date: String,
    reset_id: Option<String>,
    #[serde(rename = "totalDowntime")]
    total_downtime: u64,

    last_online: Option<OffsetDateTime>,
    #[serde(rename = "isOnline")]
    online: bool,

    latency: Option<LatencyStat>,

    deployment_id: Option<String>,

    edge: bool,
    #[serde(rename = "edgeSyncBeforeExpiry")]
    edge_sync_before_expiry: bool,
}

impl BucketTarget {
    pub fn is_empty(self) -> bool {
        //self.target_bucket.is_empty() && self.endpoint.is_empty() && self.arn.is_empty()
        self.target_bucket.is_empty() && self.endpoint.is_empty() && self.arn.is_none()
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
