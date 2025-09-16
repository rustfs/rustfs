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

//! Audit log entities and event types

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Audit log entry that matches S3/MinIO audit format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    /// Audit log version
    pub version: String,
    /// Deployment ID
    #[serde(rename = "deploymentid", skip_serializing_if = "Option::is_none")]
    pub deployment_id: Option<String>,
    /// Timestamp in RFC3339 format with nanosecond precision
    pub time: DateTime<Utc>,
    /// Event name (S3 API operation)
    pub event: String,
    /// Event type/class (S3, admin, bucket management)
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub event_type: Option<String>,
    /// API details
    pub api: ApiDetails,
    /// Remote host/client IP
    #[serde(rename = "remotehost", skip_serializing_if = "Option::is_none")]
    pub remote_host: Option<String>,
    /// User agent string
    #[serde(rename = "userAgent", skip_serializing_if = "Option::is_none")]
    pub user_agent: Option<String>,
    /// Request path
    #[serde(rename = "requestPath", skip_serializing_if = "Option::is_none")]
    pub request_path: Option<String>,
    /// Request host
    #[serde(rename = "requestHost", skip_serializing_if = "Option::is_none")]
    pub request_host: Option<String>,
    /// Request claims (JWT/auth data)
    #[serde(rename = "requestClaims", skip_serializing_if = "Option::is_none")]
    pub request_claims: Option<HashMap<String, serde_json::Value>>,
    /// Request query parameters
    #[serde(rename = "requestQuery", skip_serializing_if = "Option::is_none")]
    pub request_query: Option<HashMap<String, String>>,
    /// Request headers (redacted)
    #[serde(rename = "requestHeader", skip_serializing_if = "Option::is_none")]
    pub request_header: Option<HashMap<String, String>>,
    /// Response headers
    #[serde(rename = "responseHeader", skip_serializing_if = "Option::is_none")]
    pub response_header: Option<HashMap<String, String>>,
    /// Access key used for the request
    #[serde(rename = "accessKey", skip_serializing_if = "Option::is_none")]
    pub access_key: Option<String>,
    /// Parent user (for STS/assumed roles)
    #[serde(rename = "parentUser", skip_serializing_if = "Option::is_none")]
    pub parent_user: Option<String>,
    /// Error message if request failed
    #[serde(rename = "error", skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// API operation details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiDetails {
    /// API operation name
    pub name: String,
    /// Bucket name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
    /// Object key
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object: Option<String>,
    /// List of objects for batch operations
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub objects: Vec<ObjectVersion>,
    /// Response status text
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    /// HTTP status code
    #[serde(rename = "statusCode", skip_serializing_if = "Option::is_none")]
    pub status_code: Option<u16>,
    /// Request size in bytes
    #[serde(rename = "rx")]
    pub request_bytes: u64,
    /// Response size in bytes
    #[serde(rename = "tx")]
    pub response_bytes: u64,
    /// Response header size in bytes
    #[serde(rename = "txHeaders", skip_serializing_if = "Option::is_none")]
    pub response_header_bytes: Option<u64>,
    /// Time to first byte
    #[serde(rename = "timeToFirstByte", skip_serializing_if = "Option::is_none")]
    pub time_to_first_byte: Option<String>,
    /// Time to first byte in nanoseconds
    #[serde(rename = "timeToFirstByteInNS", skip_serializing_if = "Option::is_none")]
    pub time_to_first_byte_ns: Option<u64>,
    /// Total response time
    #[serde(rename = "timeToResponse", skip_serializing_if = "Option::is_none")]
    pub time_to_response: Option<String>,
    /// Total response time in nanoseconds
    #[serde(rename = "timeToResponseInNS", skip_serializing_if = "Option::is_none")]
    pub time_to_response_ns: Option<u64>,
}

/// Object version information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectVersion {
    /// Object key
    pub key: String,
    /// Object version ID
    #[serde(rename = "versionId", skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
}

impl Default for AuditEntry {
    fn default() -> Self {
        Self::new()
    }
}

impl AuditEntry {
    /// Create a new audit entry with default values
    pub fn new() -> Self {
        Self {
            version: "1".to_string(),
            deployment_id: None,
            time: Utc::now(),
            event: String::new(),
            event_type: None,
            api: ApiDetails::default(),
            remote_host: None,
            user_agent: None,
            request_path: None,
            request_host: None,
            request_claims: None,
            request_query: None,
            request_header: None,
            response_header: None,
            access_key: None,
            parent_user: None,
            error: None,
        }
    }

    /// Create audit entry for an S3 operation
    pub fn for_s3_operation(event: &str, api_name: &str, bucket: Option<&str>, object: Option<&str>) -> Self {
        let mut entry = Self::new();
        entry.event = event.to_string();
        entry.event_type = Some("S3".to_string());
        entry.api.name = api_name.to_string();
        entry.api.bucket = bucket.map(|s| s.to_string());
        entry.api.object = object.map(|s| s.to_string());
        entry
    }

    /// Set request context information
    pub fn with_request_context(
        mut self,
        remote_host: Option<String>,
        user_agent: Option<String>,
        path: Option<String>,
        host: Option<String>,
    ) -> Self {
        self.remote_host = remote_host;
        self.user_agent = user_agent;
        self.request_path = path;
        self.request_host = host;
        self
    }

    /// Set authentication context
    pub fn with_auth_context(
        mut self,
        access_key: Option<String>,
        parent_user: Option<String>,
        claims: Option<HashMap<String, serde_json::Value>>,
    ) -> Self {
        self.access_key = access_key;
        self.parent_user = parent_user;
        self.request_claims = claims;
        self
    }

    /// Set response status and timing
    pub fn with_response_status(
        mut self,
        status: Option<String>,
        status_code: Option<u16>,
        error: Option<String>,
        time_to_response_ns: Option<u64>,
    ) -> Self {
        self.api.status = status;
        self.api.status_code = status_code;
        self.error = error;
        self.api.time_to_response_ns = time_to_response_ns;
        if let Some(ns) = time_to_response_ns {
            self.api.time_to_response = Some(format!("{}ms", ns / 1_000_000));
        }
        self
    }

    /// Set request/response sizes
    pub fn with_byte_counts(mut self, request_bytes: u64, response_bytes: u64, response_header_bytes: Option<u64>) -> Self {
        self.api.request_bytes = request_bytes;
        self.api.response_bytes = response_bytes;
        self.api.response_header_bytes = response_header_bytes;
        self
    }

    /// Apply header redaction according to blacklist
    pub fn redact_headers(&mut self, blacklist: &[String]) {
        if let Some(ref mut headers) = self.request_header {
            for key in blacklist {
                if let Some(value) = headers.get_mut(key) {
                    *value = "***REDACTED***".to_string();
                }
            }
        }
        if let Some(ref mut headers) = self.response_header {
            for key in blacklist {
                if let Some(value) = headers.get_mut(key) {
                    *value = "***REDACTED***".to_string();
                }
            }
        }
    }

    /// Convert to JSON string
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Convert to pretty JSON string
    pub fn to_json_pretty(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

impl Default for ApiDetails {
    fn default() -> Self {
        Self {
            name: String::new(),
            bucket: None,
            object: None,
            objects: Vec::new(),
            status: None,
            status_code: None,
            request_bytes: 0,
            response_bytes: 0,
            response_header_bytes: None,
            time_to_first_byte: None,
            time_to_first_byte_ns: None,
            time_to_response: None,
            time_to_response_ns: None,
        }
    }
}

impl ObjectVersion {
    /// Create a new object version
    pub fn new(key: String, version_id: Option<String>) -> Self {
        Self { key, version_id }
    }

    /// Create from key only
    pub fn from_key(key: String) -> Self {
        Self::new(key, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_entry_creation() {
        let entry = AuditEntry::for_s3_operation("s3:GetObject", "GetObject", Some("test-bucket"), Some("test-object.txt"));

        assert_eq!(entry.event, "s3:GetObject");
        assert_eq!(entry.event_type, Some("S3".to_string()));
        assert_eq!(entry.api.name, "GetObject");
        assert_eq!(entry.api.bucket, Some("test-bucket".to_string()));
        assert_eq!(entry.api.object, Some("test-object.txt".to_string()));
    }

    #[test]
    fn test_audit_entry_json_serialization() {
        let mut entry = AuditEntry::for_s3_operation("s3:PutObject", "PutObject", Some("my-bucket"), Some("my-file.txt"));

        entry = entry
            .with_request_context(
                Some("192.168.1.100".to_string()),
                Some("aws-cli/2.0".to_string()),
                Some("/my-bucket/my-file.txt".to_string()),
                Some("s3.amazonaws.com".to_string()),
            )
            .with_response_status(
                Some("OK".to_string()),
                Some(200),
                None,
                Some(150_000_000), // 150ms
            )
            .with_byte_counts(1024, 0, Some(256));

        let json = entry.to_json().expect("Should serialize to JSON");
        assert!(json.contains("s3:PutObject"));
        assert!(json.contains("PutObject"));
        assert!(json.contains("my-bucket"));
        assert!(json.contains("192.168.1.100"));
    }

    #[test]
    fn test_header_redaction() {
        let mut entry = AuditEntry::new();
        let mut headers = HashMap::new();
        headers.insert("Authorization".to_string(), "Bearer secret-token".to_string());
        headers.insert("Content-Type".to_string(), "application/json".to_string());
        headers.insert("X-Api-Key".to_string(), "super-secret".to_string());
        entry.request_header = Some(headers);

        let blacklist = vec!["Authorization".to_string(), "X-Api-Key".to_string()];
        entry.redact_headers(&blacklist);

        let headers = entry.request_header.unwrap();
        assert_eq!(headers.get("Authorization").unwrap(), "***REDACTED***");
        assert_eq!(headers.get("X-Api-Key").unwrap(), "***REDACTED***");
        assert_eq!(headers.get("Content-Type").unwrap(), "application/json");
    }
}
