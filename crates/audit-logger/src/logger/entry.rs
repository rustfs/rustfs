//  Copyright 2024 RustFS Team
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

#![allow(dead_code)]

use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::HashMap;
use uuid::Uuid;

///A Trait for a log entry that can be serialized and sent
pub trait Loggable: Serialize + Send + Sync + 'static {
    fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

/// Standard log entries
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct LogEntry {
    pub deployment_id: String,
    pub level: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace: Option<Trace>,
    pub time: DateTime<Utc>,
    pub request_id: String,
}

impl Loggable for LogEntry {}

/// Audit log entry
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct AuditEntry {
    pub version: String,
    pub deployment_id: String,
    pub time: DateTime<Utc>,
    pub trigger: String,
    pub api: ApiDetails,
    pub remote_host: String,
    pub request_id: String,
    pub user_agent: String,
    pub access_key: String,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub tags: HashMap<String, String>,
}

impl Loggable for AuditEntry {}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Trace {
    pub message: String,
    pub source: Vec<String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub variables: HashMap<String, String>,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ApiDetails {
    pub name: String,
    pub bucket: String,
    pub object: String,
    pub status: String,
    pub status_code: u16,
    pub time_to_first_byte: String,
    pub time_to_response: String,
}

// Helper functions to create entries
impl AuditEntry {
    pub fn new(api_name: &str, bucket: &str, object: &str) -> Self {
        AuditEntry {
            version: "1".to_string(),
            deployment_id: "global-deployment-id".to_string(),
            time: Utc::now(),
            trigger: "incoming".to_string(),
            api: ApiDetails {
                name: api_name.to_string(),
                bucket: bucket.to_string(),
                object: object.to_string(),
                status: "OK".to_string(),
                status_code: 200,
                time_to_first_byte: "10ms".to_string(),
                time_to_response: "50ms".to_string(),
            },
            remote_host: "127.0.0.1".to_string(),
            request_id: Uuid::new_v4().to_string(),
            user_agent: "Rust-Client/1.0".to_string(),
            access_key: "minioadmin".to_string(),
            tags: HashMap::new(),
        }
    }
}
