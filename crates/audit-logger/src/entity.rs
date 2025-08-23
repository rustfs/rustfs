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

use chrono::{DateTime, Utc};
use rustfs_targets::EventName;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// ObjectVersion object version key/versionId
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectVersion {
    #[serde(rename = "objectName")]
    pub object_name: String,
    #[serde(rename = "versionId", skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
}

/// AuditEntry - audit entry logs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    pub version: String,
    #[serde(rename = "deploymentid", skip_serializing_if = "Option::is_none")]
    pub deployment_id: Option<String>,
    #[serde(rename = "siteName", skip_serializing_if = "Option::is_none")]
    pub site_name: Option<String>,
    pub time: DateTime<Utc>,
    pub event: EventName,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub entry_type: Option<String>,
    pub trigger: String,
    pub api: ApiDetails,
    #[serde(rename = "remotehost", skip_serializing_if = "Option::is_none")]
    pub remote_host: Option<String>,
    #[serde(rename = "requestID", skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(rename = "userAgent", skip_serializing_if = "Option::is_none")]
    pub user_agent: Option<String>,
    #[serde(rename = "requestPath", skip_serializing_if = "Option::is_none")]
    pub req_path: Option<String>,
    #[serde(rename = "requestHost", skip_serializing_if = "Option::is_none")]
    pub req_host: Option<String>,
    #[serde(rename = "requestNode", skip_serializing_if = "Option::is_none")]
    pub req_node: Option<String>,
    #[serde(rename = "requestClaims", skip_serializing_if = "Option::is_none")]
    pub req_claims: Option<HashMap<String, serde_json::Value>>,
    #[serde(rename = "requestQuery", skip_serializing_if = "Option::is_none")]
    pub req_query: Option<HashMap<String, String>>,
    #[serde(rename = "requestHeader", skip_serializing_if = "Option::is_none")]
    pub req_header: Option<HashMap<String, String>>,
    #[serde(rename = "responseHeader", skip_serializing_if = "Option::is_none")]
    pub resp_header: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, serde_json::Value>>,
    #[serde(rename = "accessKey", skip_serializing_if = "Option::is_none")]
    pub access_key: Option<String>,
    #[serde(rename = "parentUser", skip_serializing_if = "Option::is_none")]
    pub parent_user: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// API details within the audit entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiDetails {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub objects: Option<Vec<ObjectVersion>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_code: Option<i32>,
    #[serde(rename = "rx", skip_serializing_if = "Option::is_none")]
    pub input_bytes: Option<i64>,
    #[serde(rename = "tx", skip_serializing_if = "Option::is_none")]
    pub output_bytes: Option<i64>,
    #[serde(rename = "txHeaders", skip_serializing_if = "Option::is_none")]
    pub header_bytes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_to_first_byte: Option<String>,
    #[serde(rename = "timeToFirstByteInNS", skip_serializing_if = "Option::is_none")]
    pub time_to_first_byte_in_ns: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_to_response: Option<String>,
    #[serde(rename = "timeToResponseInNS", skip_serializing_if = "Option::is_none")]
    pub time_to_response_in_ns: Option<String>,
}
