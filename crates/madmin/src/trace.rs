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

use std::{collections::HashMap, time::Duration};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::heal_commands::HealResultItem;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct TraceType(u64);

impl TraceType {
    // 定义一些常量
    pub const OS: TraceType = TraceType(1 << 0);
    pub const STORAGE: TraceType = TraceType(1 << 1);
    pub const S3: TraceType = TraceType(1 << 2);
    pub const INTERNAL: TraceType = TraceType(1 << 3);
    pub const SCANNER: TraceType = TraceType(1 << 4);
    pub const DECOMMISSION: TraceType = TraceType(1 << 5);
    pub const HEALING: TraceType = TraceType(1 << 6);
    pub const BATCH_REPLICATION: TraceType = TraceType(1 << 7);
    pub const BATCH_KEY_ROTATION: TraceType = TraceType(1 << 8);
    pub const BATCH_EXPIRE: TraceType = TraceType(1 << 9);
    pub const REBALANCE: TraceType = TraceType(1 << 10);
    pub const REPLICATION_RESYNC: TraceType = TraceType(1 << 11);
    pub const BOOTSTRAP: TraceType = TraceType(1 << 12);
    pub const FTP: TraceType = TraceType(1 << 13);
    pub const ILM: TraceType = TraceType(1 << 14);

    // MetricsAll must be last.
    pub const ALL: TraceType = TraceType((1 << 15) - 1);

    pub fn new(t: u64) -> Self {
        Self(t)
    }
}

impl TraceType {
    pub fn contains(&self, x: &TraceType) -> bool {
        (self.0 & x.0) == x.0
    }

    pub fn overlaps(&self, x: &TraceType) -> bool {
        (self.0 & x.0) != 0
    }

    pub fn single_type(&self) -> bool {
        todo!()
    }

    pub fn merge(&mut self, other: &TraceType) {
        self.0 |= other.0
    }

    pub fn set_if(&mut self, b: bool, other: &TraceType) {
        if b {
            self.0 |= other.0
        }
    }

    pub fn mask(&self) -> u64 {
        self.0
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct TraceInfo {
    #[serde(rename = "type")]
    trace_type: u64,
    #[serde(rename = "nodename")]
    node_name: String,
    #[serde(rename = "funcname")]
    func_name: String,
    #[serde(rename = "time")]
    time: DateTime<Utc>,
    #[serde(rename = "path")]
    path: String,
    #[serde(rename = "dur")]
    duration: Duration,
    #[serde(rename = "bytes", skip_serializing_if = "Option::is_none")]
    bytes: Option<i64>,
    #[serde(rename = "msg", skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(rename = "error", skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(rename = "custom", skip_serializing_if = "Option::is_none")]
    custom: Option<HashMap<String, String>>,
    #[serde(rename = "http", skip_serializing_if = "Option::is_none")]
    http: Option<TraceHTTPStats>,
    #[serde(rename = "healResult", skip_serializing_if = "Option::is_none")]
    heal_result: Option<HealResultItem>,
}

impl TraceInfo {
    pub fn mask(&self) -> u64 {
        TraceType::new(self.trace_type).mask()
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct TraceInfoLegacy {
    trace_info: TraceInfo,
    #[serde(rename = "request")]
    req_info: Option<TraceRequestInfo>,
    #[serde(rename = "response")]
    resp_info: Option<TraceResponseInfo>,
    #[serde(rename = "stats")]
    call_stats: Option<TraceCallStats>,
    #[serde(rename = "storageStats")]
    storage_stats: Option<StorageStats>,
    #[serde(rename = "osStats")]
    os_stats: Option<OSStats>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StorageStats {
    path: String,
    duration: Duration,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct OSStats {
    path: String,
    duration: Duration,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct TraceHTTPStats {
    req_info: TraceRequestInfo,
    resp_info: TraceResponseInfo,
    call_stats: TraceCallStats,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct TraceCallStats {
    input_bytes: i32,
    output_bytes: i32,
    latency: Duration,
    time_to_first_byte: Duration,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct TraceRequestInfo {
    time: DateTime<Utc>,
    proto: String,
    method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    raw_query: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    headers: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    body: Option<Vec<u8>>,
    client: String,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct TraceResponseInfo {
    time: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    headers: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    body: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status_code: Option<i32>,
}
