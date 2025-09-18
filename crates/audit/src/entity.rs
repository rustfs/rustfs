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

//! Audit entry definitions and S3 event helpers

use chrono::{DateTime, Utc};
use rustfs_obs::entry::audit::AuditLogEntry;
use rustfs_targets::EventName;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Type alias for the standard audit entry format compatible with S3/MinIO
pub type AuditEntry = AuditLogEntry;

/// S3 event helpers for creating audit entries
pub mod s3_events {
    use super::*;
    use rustfs_obs::entry::{ApiDetails, BaseLogEntry};
    
    /// Create an audit entry for S3 GetObject operation
    pub fn get_object(bucket: &str, key: &str) -> AuditEntry {
        AuditEntry {
            base: BaseLogEntry::new(),
            version: "1.0".to_string(),
            deployment_id: None,
            event: "s3:GetObject".to_string(),
            entry_type: Some("S3".to_string()),
            api: ApiDetails {
                name: "GetObject".to_string(),
                bucket: Some(bucket.to_string()),
                object: Some(key.to_string()),
                ..Default::default()
            },
            remote_host: None,
            user_agent: None,
            req_path: Some(format!("/{}/{}", bucket, key)),
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

    /// Create an audit entry for S3 PutObject operation
    pub fn put_object(bucket: &str, key: &str) -> AuditEntry {
        AuditEntry {
            base: BaseLogEntry::new(),
            version: "1.0".to_string(),
            deployment_id: None,
            event: "s3:PutObject".to_string(),
            entry_type: Some("S3".to_string()),
            api: ApiDetails {
                name: "PutObject".to_string(),
                bucket: Some(bucket.to_string()),
                object: Some(key.to_string()),
                ..Default::default()
            },
            remote_host: None,
            user_agent: None,
            req_path: Some(format!("/{}/{}", bucket, key)),
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

    /// Create an audit entry for S3 DeleteObject operation
    pub fn delete_object(bucket: &str, key: &str) -> AuditEntry {
        AuditEntry {
            base: BaseLogEntry::new(),
            version: "1.0".to_string(),
            deployment_id: None,
            event: "s3:DeleteObject".to_string(),
            entry_type: Some("S3".to_string()),
            api: ApiDetails {
                name: "DeleteObject".to_string(),
                bucket: Some(bucket.to_string()),
                object: Some(key.to_string()),
                ..Default::default()
            },
            remote_host: None,
            user_agent: None,
            req_path: Some(format!("/{}/{}", bucket, key)),
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

    /// Create an audit entry for S3 ListObjects operation
    pub fn list_objects(bucket: &str) -> AuditEntry {
        AuditEntry {
            base: BaseLogEntry::new(),
            version: "1.0".to_string(),
            deployment_id: None,
            event: "s3:ListObjects".to_string(),
            entry_type: Some("S3".to_string()),
            api: ApiDetails {
                name: "ListObjects".to_string(),
                bucket: Some(bucket.to_string()),
                object: None,
                ..Default::default()
            },
            remote_host: None,
            user_agent: None,
            req_path: Some(format!("/{}", bucket)),
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

    /// Create an audit entry for S3 CreateBucket operation
    pub fn create_bucket(bucket: &str) -> AuditEntry {
        AuditEntry {
            base: BaseLogEntry::new(),
            version: "1.0".to_string(),
            deployment_id: None,
            event: "s3:CreateBucket".to_string(),
            entry_type: Some("S3".to_string()),
            api: ApiDetails {
                name: "CreateBucket".to_string(),
                bucket: Some(bucket.to_string()),
                object: None,
                ..Default::default()
            },
            remote_host: None,
            user_agent: None,
            req_path: Some(format!("/{}", bucket)),
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

    /// Create an audit entry for S3 DeleteBucket operation
    pub fn delete_bucket(bucket: &str) -> AuditEntry {
        AuditEntry {
            base: BaseLogEntry::new(),
            version: "1.0".to_string(),
            deployment_id: None,
            event: "s3:DeleteBucket".to_string(),
            entry_type: Some("S3".to_string()),
            api: ApiDetails {
                name: "DeleteBucket".to_string(),
                bucket: Some(bucket.to_string()),
                object: None,
                ..Default::default()
            },
            remote_host: None,
            user_agent: None,
            req_path: Some(format!("/{}", bucket)),
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
}

impl AuditEntry {
    /// Builder methods for fluent API
    pub fn with_request_context(
        mut self, 
        remote_host: Option<String>,
        user_agent: Option<String>,
        req_host: Option<String>,
    ) -> Self {
        self.remote_host = remote_host;
        self.user_agent = user_agent;
        self.req_host = req_host;
        self
    }

    /// Add request headers with optional redaction
    pub fn with_request_headers(mut self, headers: HashMap<String, String>) -> Self {
        self.req_header = Some(headers);
        self
    }

    /// Add response information
    pub fn with_response_status(
        mut self,
        status: Option<String>,
        status_code: Option<u16>,
        response_headers: Option<HashMap<String, String>>,
    ) -> Self {
        if let Some(code) = status_code {
            self.api.status_code = Some(code as i32);
        }
        self.resp_header = response_headers;
        self
    }

    /// Add authentication information
    pub fn with_auth_info(mut self, access_key: Option<String>, parent_user: Option<String>) -> Self {
        self.access_key = access_key;
        self.parent_user = parent_user;
        self
    }

    /// Add error information
    pub fn with_error(mut self, error: String) -> Self {
        self.error = Some(error);
        self
    }

    /// Add deployment ID
    pub fn with_deployment_id(mut self, deployment_id: String) -> Self {
        self.deployment_id = Some(deployment_id);
        self
    }

    /// Redact sensitive headers according to security policy
    pub fn redact_sensitive_headers(&mut self, blacklist: &[String]) {
        if let Some(ref mut headers) = self.req_header {
            for header_name in blacklist {
                if headers.contains_key(header_name) {
                    headers.insert(header_name.clone(), "***REDACTED***".to_string());
                }
            }
        }
        
        if let Some(ref mut headers) = self.resp_header {
            for header_name in blacklist {
                if headers.contains_key(header_name) {
                    headers.insert(header_name.clone(), "***REDACTED***".to_string());
                }
            }
        }
    }
}