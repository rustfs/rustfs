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
use hashbrown::HashMap;
use rustfs_targets::EventName;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// ObjectVersion represents an object version with key and versionId
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ObjectVersion {
    #[serde(rename = "objectName")]
    pub object_name: String,
    #[serde(rename = "versionId", skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
}

impl ObjectVersion {
    pub fn new(object_name: String, version_id: Option<String>) -> Self {
        Self { object_name, version_id }
    }
}

/// `ApiDetails` contains API information for the audit entry.
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

/// Builder for `ApiDetails`.
#[derive(Default, Clone)]
pub struct ApiDetailsBuilder(pub ApiDetails);

impl ApiDetailsBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.0.name = Some(name.into());
        self
    }

    pub fn bucket(mut self, bucket: impl Into<String>) -> Self {
        self.0.bucket = Some(bucket.into());
        self
    }

    pub fn object(mut self, object: impl Into<String>) -> Self {
        self.0.object = Some(object.into());
        self
    }

    pub fn objects(mut self, objects: Vec<ObjectVersion>) -> Self {
        self.0.objects = Some(objects);
        self
    }

    pub fn status(mut self, status: impl Into<String>) -> Self {
        self.0.status = Some(status.into());
        self
    }

    pub fn status_code(mut self, code: i32) -> Self {
        self.0.status_code = Some(code);
        self
    }

    pub fn input_bytes(mut self, bytes: i64) -> Self {
        self.0.input_bytes = Some(bytes);
        self
    }

    pub fn output_bytes(mut self, bytes: i64) -> Self {
        self.0.output_bytes = Some(bytes);
        self
    }

    pub fn header_bytes(mut self, bytes: i64) -> Self {
        self.0.header_bytes = Some(bytes);
        self
    }

    pub fn time_to_first_byte(mut self, t: impl Into<String>) -> Self {
        self.0.time_to_first_byte = Some(t.into());
        self
    }

    pub fn time_to_first_byte_in_ns(mut self, t: impl Into<String>) -> Self {
        self.0.time_to_first_byte_in_ns = Some(t.into());
        self
    }

    pub fn time_to_response(mut self, t: impl Into<String>) -> Self {
        self.0.time_to_response = Some(t.into());
        self
    }

    pub fn time_to_response_in_ns(mut self, t: impl Into<String>) -> Self {
        self.0.time_to_response_in_ns = Some(t.into());
        self
    }

    pub fn build(self) -> ApiDetails {
        self.0
    }
}

/// `AuditEntry` represents an audit log entry.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AuditEntry {
    pub version: String,
    #[serde(rename = "deploymentid", skip_serializing_if = "Option::is_none")]
    pub deployment_id: Option<String>,
    #[serde(rename = "siteName", skip_serializing_if = "Option::is_none")]
    pub site_name: Option<String>,
    #[serde(with = "chrono::serde::ts_milliseconds")]
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

/// Constructor for `AuditEntry`.
pub struct AuditEntryBuilder(AuditEntry);

impl AuditEntryBuilder {
    /// Create a new builder with all required fields.
    pub fn new(version: impl Into<String>, event: EventName, trigger: impl Into<String>, api: ApiDetails) -> Self {
        Self(AuditEntry {
            version: version.into(),
            time: Utc::now(),
            event,
            trigger: trigger.into(),
            api,
            ..Default::default()
        })
    }

    // event
    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.0.version = version.into();
        self
    }

    pub fn event(mut self, event: EventName) -> Self {
        self.0.event = event;
        self
    }

    pub fn api(mut self, api_details: ApiDetails) -> Self {
        self.0.api = api_details;
        self
    }

    pub fn deployment_id(mut self, id: impl Into<String>) -> Self {
        self.0.deployment_id = Some(id.into());
        self
    }

    pub fn site_name(mut self, name: impl Into<String>) -> Self {
        self.0.site_name = Some(name.into());
        self
    }

    pub fn time(mut self, time: DateTime<Utc>) -> Self {
        self.0.time = time;
        self
    }

    pub fn entry_type(mut self, entry_type: impl Into<String>) -> Self {
        self.0.entry_type = Some(entry_type.into());
        self
    }

    pub fn remote_host(mut self, host: impl Into<String>) -> Self {
        self.0.remote_host = Some(host.into());
        self
    }

    pub fn request_id(mut self, id: impl Into<String>) -> Self {
        self.0.request_id = Some(id.into());
        self
    }

    pub fn user_agent(mut self, agent: impl Into<String>) -> Self {
        self.0.user_agent = Some(agent.into());
        self
    }

    pub fn req_path(mut self, path: impl Into<String>) -> Self {
        self.0.req_path = Some(path.into());
        self
    }

    pub fn req_host(mut self, host: impl Into<String>) -> Self {
        self.0.req_host = Some(host.into());
        self
    }

    pub fn req_node(mut self, node: impl Into<String>) -> Self {
        self.0.req_node = Some(node.into());
        self
    }

    pub fn req_claims(mut self, claims: HashMap<String, Value>) -> Self {
        self.0.req_claims = Some(claims);
        self
    }

    pub fn req_query(mut self, query: HashMap<String, String>) -> Self {
        self.0.req_query = Some(query);
        self
    }

    pub fn req_header(mut self, header: HashMap<String, String>) -> Self {
        self.0.req_header = Some(header);
        self
    }

    pub fn resp_header(mut self, header: HashMap<String, String>) -> Self {
        self.0.resp_header = Some(header);
        self
    }

    pub fn tags(mut self, tags: HashMap<String, Value>) -> Self {
        self.0.tags = Some(tags);
        self
    }

    pub fn access_key(mut self, key: impl Into<String>) -> Self {
        self.0.access_key = Some(key.into());
        self
    }

    pub fn parent_user(mut self, user: impl Into<String>) -> Self {
        self.0.parent_user = Some(user.into());
        self
    }

    pub fn error(mut self, error: impl Into<String>) -> Self {
        self.0.error = Some(error.into());
        self
    }

    /// Construct the final `AuditEntry`.
    pub fn build(self) -> AuditEntry {
        self.0
    }
}
