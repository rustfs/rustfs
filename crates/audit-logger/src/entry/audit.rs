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

#![allow(dead_code)]

use crate::{BaseLogEntry, LogRecord, ObjectVersion};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// API details structure
/// ApiDetails is used to define the details of an API operation
///
/// The `ApiDetails` structure contains the following fields:
/// - `name` - the name of the API operation
/// - `bucket` - the bucket name
/// - `object` - the object name
/// - `objects` - the list of objects
/// - `status` - the status of the API operation
/// - `status_code` - the status code of the API operation
/// - `input_bytes` - the input bytes
/// - `output_bytes` - the output bytes
/// - `header_bytes` - the header bytes
/// - `time_to_first_byte` - the time to first byte
/// - `time_to_first_byte_in_ns` - the time to first byte in nanoseconds
/// - `time_to_response` - the time to response
/// - `time_to_response_in_ns` - the time to response in nanoseconds
///
/// The `ApiDetails` structure contains the following methods:
/// - `new` - create a new `ApiDetails` with default values
/// - `set_name` - set the name
/// - `set_bucket` - set the bucket
/// - `set_object` - set the object
/// - `set_objects` - set the objects
/// - `set_status` - set the status
/// - `set_status_code` - set the status code
/// - `set_input_bytes` - set the input bytes
/// - `set_output_bytes` - set the output bytes
/// - `set_header_bytes` - set the header bytes
/// - `set_time_to_first_byte` - set the time to first byte
/// - `set_time_to_first_byte_in_ns` - set the time to first byte in nanoseconds
/// - `set_time_to_response` - set the time to response
/// - `set_time_to_response_in_ns` - set the time to response in nanoseconds
///
/// # Example
/// ```
/// use rustfs_audit_logger::ApiDetails;
/// use rustfs_audit_logger::ObjectVersion;
///
/// let api = ApiDetails::new()
///     .set_name(Some("GET".to_string()))
///     .set_bucket(Some("my-bucket".to_string()))
///     .set_object(Some("my-object".to_string()))
///     .set_objects(vec![ObjectVersion::new_with_object_name("my-object".to_string())])
///     .set_status(Some("OK".to_string()))
///     .set_status_code(Some(200))
///     .set_input_bytes(100)
///     .set_output_bytes(200)
///     .set_header_bytes(Some(50))
///     .set_time_to_first_byte(Some("100ms".to_string()))
///     .set_time_to_first_byte_in_ns(Some("100000000ns".to_string()))
///     .set_time_to_response(Some("200ms".to_string()))
///     .set_time_to_response_in_ns(Some("200000000ns".to_string()));
/// ```
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq, Eq)]
pub struct ApiDetails {
    #[serde(rename = "name", skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(rename = "bucket", skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
    #[serde(rename = "object", skip_serializing_if = "Option::is_none")]
    pub object: Option<String>,
    #[serde(rename = "objects", skip_serializing_if = "Vec::is_empty", default)]
    pub objects: Vec<ObjectVersion>,
    #[serde(rename = "status", skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(rename = "statusCode", skip_serializing_if = "Option::is_none")]
    pub status_code: Option<i32>,
    #[serde(rename = "rx")]
    pub input_bytes: i64,
    #[serde(rename = "tx")]
    pub output_bytes: i64,
    #[serde(rename = "txHeaders", skip_serializing_if = "Option::is_none")]
    pub header_bytes: Option<i64>,
    #[serde(rename = "timeToFirstByte", skip_serializing_if = "Option::is_none")]
    pub time_to_first_byte: Option<String>,
    #[serde(rename = "timeToFirstByteInNS", skip_serializing_if = "Option::is_none")]
    pub time_to_first_byte_in_ns: Option<String>,
    #[serde(rename = "timeToResponse", skip_serializing_if = "Option::is_none")]
    pub time_to_response: Option<String>,
    #[serde(rename = "timeToResponseInNS", skip_serializing_if = "Option::is_none")]
    pub time_to_response_in_ns: Option<String>,
}

impl ApiDetails {
    /// Create a new `ApiDetails` with default values
    pub fn new() -> Self {
        ApiDetails {
            name: None,
            bucket: None,
            object: None,
            objects: Vec::new(),
            status: None,
            status_code: None,
            input_bytes: 0,
            output_bytes: 0,
            header_bytes: None,
            time_to_first_byte: None,
            time_to_first_byte_in_ns: None,
            time_to_response: None,
            time_to_response_in_ns: None,
        }
    }

    /// Set the name
    pub fn set_name(mut self, name: Option<String>) -> Self {
        self.name = name;
        self
    }

    /// Set the bucket
    pub fn set_bucket(mut self, bucket: Option<String>) -> Self {
        self.bucket = bucket;
        self
    }

    /// Set the object
    pub fn set_object(mut self, object: Option<String>) -> Self {
        self.object = object;
        self
    }

    /// Set the objects
    pub fn set_objects(mut self, objects: Vec<ObjectVersion>) -> Self {
        self.objects = objects;
        self
    }

    /// Set the status
    pub fn set_status(mut self, status: Option<String>) -> Self {
        self.status = status;
        self
    }

    /// Set the status code
    pub fn set_status_code(mut self, status_code: Option<i32>) -> Self {
        self.status_code = status_code;
        self
    }

    /// Set the input bytes
    pub fn set_input_bytes(mut self, input_bytes: i64) -> Self {
        self.input_bytes = input_bytes;
        self
    }

    /// Set the output bytes
    pub fn set_output_bytes(mut self, output_bytes: i64) -> Self {
        self.output_bytes = output_bytes;
        self
    }

    /// Set the header bytes
    pub fn set_header_bytes(mut self, header_bytes: Option<i64>) -> Self {
        self.header_bytes = header_bytes;
        self
    }

    /// Set the time to first byte
    pub fn set_time_to_first_byte(mut self, time_to_first_byte: Option<String>) -> Self {
        self.time_to_first_byte = time_to_first_byte;
        self
    }

    /// Set the time to first byte in nanoseconds
    pub fn set_time_to_first_byte_in_ns(mut self, time_to_first_byte_in_ns: Option<String>) -> Self {
        self.time_to_first_byte_in_ns = time_to_first_byte_in_ns;
        self
    }

    /// Set the time to response
    pub fn set_time_to_response(mut self, time_to_response: Option<String>) -> Self {
        self.time_to_response = time_to_response;
        self
    }

    /// Set the time to response in nanoseconds
    pub fn set_time_to_response_in_ns(mut self, time_to_response_in_ns: Option<String>) -> Self {
        self.time_to_response_in_ns = time_to_response_in_ns;
        self
    }
}

/// Entry - audit entry logs
/// AuditLogEntry is used to define the structure of an audit log entry
///
/// The `AuditLogEntry` structure contains the following fields:
/// - `base` - the base log entry
/// - `version` - the version of the audit log entry
/// - `deployment_id` - the deployment ID
/// - `event` - the event
/// - `entry_type` - the type of audit message
/// - `api` - the API details
/// - `remote_host` - the remote host
/// - `user_agent` - the user agent
/// - `req_path` - the request path
/// - `req_host` - the request host
/// - `req_claims` - the request claims
/// - `req_query` - the request query
/// - `req_header` - the request header
/// - `resp_header` - the response header
/// - `access_key` - the access key
/// - `parent_user` - the parent user
/// - `error` - the error
///
/// The `AuditLogEntry` structure contains the following methods:
/// - `new` - create a new `AuditEntry` with default values
/// - `new_with_values` - create a new `AuditEntry` with version, time, event and api details
/// - `with_base` - set the base log entry
/// - `set_version` - set the version
/// - `set_deployment_id` - set the deployment ID
/// - `set_event` - set the event
/// - `set_entry_type` - set the entry type
/// - `set_api` - set the API details
/// - `set_remote_host` - set the remote host
/// - `set_user_agent` - set the user agent
/// - `set_req_path` - set the request path
/// - `set_req_host` - set the request host
/// - `set_req_claims` - set the request claims
/// - `set_req_query` - set the request query
/// - `set_req_header` - set the request header
/// - `set_resp_header` - set the response header
/// - `set_access_key` - set the access key
/// - `set_parent_user` - set the parent user
/// - `set_error` - set the error
///
/// # Example
/// ```
/// use rustfs_audit_logger::AuditLogEntry;
/// use rustfs_audit_logger::ApiDetails;
/// use std::collections::HashMap;
///
/// let entry = AuditLogEntry::new()
///     .set_version("1.0".to_string())
///     .set_deployment_id(Some("123".to_string()))
///     .set_event("event".to_string())
///     .set_entry_type(Some("type".to_string()))
///     .set_api(ApiDetails::new())
///     .set_remote_host(Some("remote-host".to_string()))
///     .set_user_agent(Some("user-agent".to_string()))
///     .set_req_path(Some("req-path".to_string()))
///     .set_req_host(Some("req-host".to_string()))
///     .set_req_claims(Some(HashMap::new()))
///     .set_req_query(Some(HashMap::new()))
///     .set_req_header(Some(HashMap::new()))
///     .set_resp_header(Some(HashMap::new()))
///     .set_access_key(Some("access-key".to_string()))
///     .set_parent_user(Some("parent-user".to_string()))
///     .set_error(Some("error".to_string()));
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct AuditLogEntry {
    #[serde(flatten)]
    pub base: BaseLogEntry,
    pub version: String,
    #[serde(rename = "deploymentid", skip_serializing_if = "Option::is_none")]
    pub deployment_id: Option<String>,
    pub event: String,
    // Class of audit message - S3, admin ops, bucket management
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub entry_type: Option<String>,
    pub api: ApiDetails,
    #[serde(rename = "remotehost", skip_serializing_if = "Option::is_none")]
    pub remote_host: Option<String>,
    #[serde(rename = "userAgent", skip_serializing_if = "Option::is_none")]
    pub user_agent: Option<String>,
    #[serde(rename = "requestPath", skip_serializing_if = "Option::is_none")]
    pub req_path: Option<String>,
    #[serde(rename = "requestHost", skip_serializing_if = "Option::is_none")]
    pub req_host: Option<String>,
    #[serde(rename = "requestClaims", skip_serializing_if = "Option::is_none")]
    pub req_claims: Option<HashMap<String, Value>>,
    #[serde(rename = "requestQuery", skip_serializing_if = "Option::is_none")]
    pub req_query: Option<HashMap<String, String>>,
    #[serde(rename = "requestHeader", skip_serializing_if = "Option::is_none")]
    pub req_header: Option<HashMap<String, String>>,
    #[serde(rename = "responseHeader", skip_serializing_if = "Option::is_none")]
    pub resp_header: Option<HashMap<String, String>>,
    #[serde(rename = "accessKey", skip_serializing_if = "Option::is_none")]
    pub access_key: Option<String>,
    #[serde(rename = "parentUser", skip_serializing_if = "Option::is_none")]
    pub parent_user: Option<String>,
    #[serde(rename = "error", skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl AuditLogEntry {
    /// Create a new `AuditEntry` with default values
    pub fn new() -> Self {
        AuditLogEntry {
            base: BaseLogEntry::new(),
            version: String::new(),
            deployment_id: None,
            event: String::new(),
            entry_type: None,
            api: ApiDetails::new(),
            remote_host: None,
            user_agent: None,
            req_path: None,
            req_host: None,
            req_claims: None,
            req_query: None,
            req_header: None,
            resp_header: None,
            access_key: None,
            parent_user: None,
            error: None,
        }
    }

    /// Create a new `AuditEntry` with version, time, event and api details
    pub fn new_with_values(version: String, time: DateTime<Utc>, event: String, api: ApiDetails) -> Self {
        let mut base = BaseLogEntry::new();
        base.timestamp = time;

        AuditLogEntry {
            base,
            version,
            deployment_id: None,
            event,
            entry_type: None,
            api,
            remote_host: None,
            user_agent: None,
            req_path: None,
            req_host: None,
            req_claims: None,
            req_query: None,
            req_header: None,
            resp_header: None,
            access_key: None,
            parent_user: None,
            error: None,
        }
    }

    /// Set the base log entry
    pub fn with_base(mut self, base: BaseLogEntry) -> Self {
        self.base = base;
        self
    }

    /// Set the version
    pub fn set_version(mut self, version: String) -> Self {
        self.version = version;
        self
    }

    /// Set the deployment ID
    pub fn set_deployment_id(mut self, deployment_id: Option<String>) -> Self {
        self.deployment_id = deployment_id;
        self
    }

    /// Set the event
    pub fn set_event(mut self, event: String) -> Self {
        self.event = event;
        self
    }

    /// Set the entry type
    pub fn set_entry_type(mut self, entry_type: Option<String>) -> Self {
        self.entry_type = entry_type;
        self
    }

    /// Set the API details
    pub fn set_api(mut self, api: ApiDetails) -> Self {
        self.api = api;
        self
    }

    /// Set the remote host
    pub fn set_remote_host(mut self, remote_host: Option<String>) -> Self {
        self.remote_host = remote_host;
        self
    }

    /// Set the user agent
    pub fn set_user_agent(mut self, user_agent: Option<String>) -> Self {
        self.user_agent = user_agent;
        self
    }

    /// Set the request path
    pub fn set_req_path(mut self, req_path: Option<String>) -> Self {
        self.req_path = req_path;
        self
    }

    /// Set the request host
    pub fn set_req_host(mut self, req_host: Option<String>) -> Self {
        self.req_host = req_host;
        self
    }

    /// Set the request claims
    pub fn set_req_claims(mut self, req_claims: Option<HashMap<String, Value>>) -> Self {
        self.req_claims = req_claims;
        self
    }

    /// Set the request query
    pub fn set_req_query(mut self, req_query: Option<HashMap<String, String>>) -> Self {
        self.req_query = req_query;
        self
    }

    /// Set the request header
    pub fn set_req_header(mut self, req_header: Option<HashMap<String, String>>) -> Self {
        self.req_header = req_header;
        self
    }

    /// Set the response header
    pub fn set_resp_header(mut self, resp_header: Option<HashMap<String, String>>) -> Self {
        self.resp_header = resp_header;
        self
    }

    /// Set the access key
    pub fn set_access_key(mut self, access_key: Option<String>) -> Self {
        self.access_key = access_key;
        self
    }

    /// Set the parent user
    pub fn set_parent_user(mut self, parent_user: Option<String>) -> Self {
        self.parent_user = parent_user;
        self
    }

    /// Set the error
    pub fn set_error(mut self, error: Option<String>) -> Self {
        self.error = error;
        self
    }
}

impl LogRecord for AuditLogEntry {
    fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| String::from("{}"))
    }

    fn get_timestamp(&self) -> DateTime<Utc> {
        self.base.timestamp
    }
}
