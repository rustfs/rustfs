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

use chrono::{DateTime, Utc};
use rustfs_targets::EventName;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Trait for types that can be serialized to JSON and have a timestamp
pub trait LogRecord {
    /// Serialize the record to a JSON string
    fn to_json(&self) -> String;
    /// Get the timestamp of the record
    fn get_timestamp(&self) -> chrono::DateTime<chrono::Utc>;
}

/// ObjectVersion represents an object version with key and versionId
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ObjectVersion {
    #[serde(rename = "objectName")]
    pub object_name: String,
    #[serde(rename = "versionId", skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
}

impl ObjectVersion {
    /// Set the object name (chainable)
    pub fn set_object_name(&mut self, name: String) -> &mut Self {
        self.object_name = name;
        self
    }
    /// Set the version ID (chainable)
    pub fn set_version_id(&mut self, version_id: Option<String>) -> &mut Self {
        self.version_id = version_id;
        self
    }
}

/// ApiDetails contains API information for the audit entry
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
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

impl ApiDetails {
    /// Set API name (chainable)
    pub fn set_name(&mut self, name: Option<String>) -> &mut Self {
        self.name = name;
        self
    }
    /// Set bucket name (chainable)
    pub fn set_bucket(&mut self, bucket: Option<String>) -> &mut Self {
        self.bucket = bucket;
        self
    }
    /// Set object name (chainable)
    pub fn set_object(&mut self, object: Option<String>) -> &mut Self {
        self.object = object;
        self
    }
    /// Set objects list (chainable)
    pub fn set_objects(&mut self, objects: Option<Vec<ObjectVersion>>) -> &mut Self {
        self.objects = objects;
        self
    }
    /// Set status (chainable)
    pub fn set_status(&mut self, status: Option<String>) -> &mut Self {
        self.status = status;
        self
    }
    /// Set status code (chainable)
    pub fn set_status_code(&mut self, code: Option<i32>) -> &mut Self {
        self.status_code = code;
        self
    }
    /// Set input bytes (chainable)
    pub fn set_input_bytes(&mut self, bytes: Option<i64>) -> &mut Self {
        self.input_bytes = bytes;
        self
    }
    /// Set output bytes (chainable)
    pub fn set_output_bytes(&mut self, bytes: Option<i64>) -> &mut Self {
        self.output_bytes = bytes;
        self
    }
    /// Set header bytes (chainable)
    pub fn set_header_bytes(&mut self, bytes: Option<i64>) -> &mut Self {
        self.header_bytes = bytes;
        self
    }
    /// Set time to first byte (chainable)
    pub fn set_time_to_first_byte(&mut self, t: Option<String>) -> &mut Self {
        self.time_to_first_byte = t;
        self
    }
    /// Set time to first byte in nanoseconds (chainable)
    pub fn set_time_to_first_byte_in_ns(&mut self, t: Option<String>) -> &mut Self {
        self.time_to_first_byte_in_ns = t;
        self
    }
    /// Set time to response (chainable)
    pub fn set_time_to_response(&mut self, t: Option<String>) -> &mut Self {
        self.time_to_response = t;
        self
    }
    /// Set time to response in nanoseconds (chainable)
    pub fn set_time_to_response_in_ns(&mut self, t: Option<String>) -> &mut Self {
        self.time_to_response_in_ns = t;
        self
    }
}

/// AuditEntry represents an audit log entry
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
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
    pub req_claims: Option<HashMap<String, Value>>,
    #[serde(rename = "requestQuery", skip_serializing_if = "Option::is_none")]
    pub req_query: Option<HashMap<String, String>>,
    #[serde(rename = "requestHeader", skip_serializing_if = "Option::is_none")]
    pub req_header: Option<HashMap<String, String>>,
    #[serde(rename = "responseHeader", skip_serializing_if = "Option::is_none")]
    pub resp_header: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, Value>>,
    #[serde(rename = "accessKey", skip_serializing_if = "Option::is_none")]
    pub access_key: Option<String>,
    #[serde(rename = "parentUser", skip_serializing_if = "Option::is_none")]
    pub parent_user: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl AuditEntry {
    /// Create a new AuditEntry with required fields
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        version: String,
        deployment_id: Option<String>,
        site_name: Option<String>,
        time: DateTime<Utc>,
        event: EventName,
        entry_type: Option<String>,
        trigger: String,
        api: ApiDetails,
    ) -> Self {
        AuditEntry {
            version,
            deployment_id,
            site_name,
            time,
            event,
            entry_type,
            trigger,
            api,
            remote_host: None,
            request_id: None,
            user_agent: None,
            req_path: None,
            req_host: None,
            req_node: None,
            req_claims: None,
            req_query: None,
            req_header: None,
            resp_header: None,
            tags: None,
            access_key: None,
            parent_user: None,
            error: None,
        }
    }

    /// Set version (chainable)
    pub fn set_version(&mut self, version: String) -> &mut Self {
        self.version = version;
        self
    }
    /// Set deployment ID (chainable)
    pub fn set_deployment_id(&mut self, id: Option<String>) -> &mut Self {
        self.deployment_id = id;
        self
    }
    /// Set site name (chainable)
    pub fn set_site_name(&mut self, name: Option<String>) -> &mut Self {
        self.site_name = name;
        self
    }
    /// Set time (chainable)
    pub fn set_time(&mut self, time: DateTime<Utc>) -> &mut Self {
        self.time = time;
        self
    }
    /// Set event (chainable)
    pub fn set_event(&mut self, event: EventName) -> &mut Self {
        self.event = event;
        self
    }
    /// Set entry type (chainable)
    pub fn set_entry_type(&mut self, entry_type: Option<String>) -> &mut Self {
        self.entry_type = entry_type;
        self
    }
    /// Set trigger (chainable)
    pub fn set_trigger(&mut self, trigger: String) -> &mut Self {
        self.trigger = trigger;
        self
    }
    /// Set API details (chainable)
    pub fn set_api(&mut self, api: ApiDetails) -> &mut Self {
        self.api = api;
        self
    }
    /// Set remote host (chainable)
    pub fn set_remote_host(&mut self, host: Option<String>) -> &mut Self {
        self.remote_host = host;
        self
    }
    /// Set request ID (chainable)
    pub fn set_request_id(&mut self, id: Option<String>) -> &mut Self {
        self.request_id = id;
        self
    }
    /// Set user agent (chainable)
    pub fn set_user_agent(&mut self, agent: Option<String>) -> &mut Self {
        self.user_agent = agent;
        self
    }
    /// Set request path (chainable)
    pub fn set_req_path(&mut self, path: Option<String>) -> &mut Self {
        self.req_path = path;
        self
    }
    /// Set request host (chainable)
    pub fn set_req_host(&mut self, host: Option<String>) -> &mut Self {
        self.req_host = host;
        self
    }
    /// Set request node (chainable)
    pub fn set_req_node(&mut self, node: Option<String>) -> &mut Self {
        self.req_node = node;
        self
    }
    /// Set request claims (chainable)
    pub fn set_req_claims(&mut self, claims: Option<HashMap<String, Value>>) -> &mut Self {
        self.req_claims = claims;
        self
    }
    /// Set request query (chainable)
    pub fn set_req_query(&mut self, query: Option<HashMap<String, String>>) -> &mut Self {
        self.req_query = query;
        self
    }
    /// Set request header (chainable)
    pub fn set_req_header(&mut self, header: Option<HashMap<String, String>>) -> &mut Self {
        self.req_header = header;
        self
    }
    /// Set response header (chainable)
    pub fn set_resp_header(&mut self, header: Option<HashMap<String, String>>) -> &mut Self {
        self.resp_header = header;
        self
    }
    /// Set tags (chainable)
    pub fn set_tags(&mut self, tags: Option<HashMap<String, Value>>) -> &mut Self {
        self.tags = tags;
        self
    }
    /// Set access key (chainable)
    pub fn set_access_key(&mut self, key: Option<String>) -> &mut Self {
        self.access_key = key;
        self
    }
    /// Set parent user (chainable)
    pub fn set_parent_user(&mut self, user: Option<String>) -> &mut Self {
        self.parent_user = user;
        self
    }
    /// Set error message (chainable)
    pub fn set_error(&mut self, error: Option<String>) -> &mut Self {
        self.error = error;
        self
    }

    /// Build AuditEntry from context or parameters (example, can be extended)
    pub fn from_context(
        version: String,
        deployment_id: Option<String>,
        time: DateTime<Utc>,
        event: EventName,
        trigger: String,
        api: ApiDetails,
        tags: Option<HashMap<String, Value>>,
    ) -> Self {
        AuditEntry {
            version,
            deployment_id,
            site_name: None,
            time,
            event,
            entry_type: None,
            trigger,
            api,
            remote_host: None,
            request_id: None,
            user_agent: None,
            req_path: None,
            req_host: None,
            req_node: None,
            req_claims: None,
            req_query: None,
            req_header: None,
            resp_header: None,
            tags,
            access_key: None,
            parent_user: None,
            error: None,
        }
    }
}

impl LogRecord for AuditEntry {
    /// Serialize AuditEntry to JSON string
    fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| String::from("{}"))
    }
    /// Get the timestamp of the audit entry
    fn get_timestamp(&self) -> DateTime<Utc> {
        self.time
    }
}
