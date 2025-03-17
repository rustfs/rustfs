use crate::ObjectVersion;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// API details structure
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
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
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct AuditEntry {
    pub version: String,
    #[serde(rename = "deploymentid", skip_serializing_if = "Option::is_none")]
    pub deployment_id: Option<String>,
    pub time: DateTime<Utc>,
    pub event: String,

    // Class of audit message - S3, admin ops, bucket management
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub entry_type: Option<String>,
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
    #[serde(rename = "requestClaims", skip_serializing_if = "Option::is_none")]
    pub req_claims: Option<HashMap<String, Value>>,
    #[serde(rename = "requestQuery", skip_serializing_if = "Option::is_none")]
    pub req_query: Option<HashMap<String, String>>,
    #[serde(rename = "requestHeader", skip_serializing_if = "Option::is_none")]
    pub req_header: Option<HashMap<String, String>>,
    #[serde(rename = "responseHeader", skip_serializing_if = "Option::is_none")]
    pub resp_header: Option<HashMap<String, String>>,
    #[serde(rename = "tags", skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, Value>>,
    #[serde(rename = "accessKey", skip_serializing_if = "Option::is_none")]
    pub access_key: Option<String>,
    #[serde(rename = "parentUser", skip_serializing_if = "Option::is_none")]
    pub parent_user: Option<String>,
    #[serde(rename = "error", skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl AuditEntry {
    /// Create a new `AuditEntry` with default values
    pub fn new() -> Self {
        AuditEntry {
            version: String::new(),
            deployment_id: None,
            time: Utc::now(),
            event: String::new(),
            entry_type: None,
            api: ApiDetails::new(),
            remote_host: None,
            request_id: None,
            user_agent: None,
            req_path: None,
            req_host: None,
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

    /// Create a new `AuditEntry` with version and time event and api details
    /// # Arguments
    /// * `version` - Version of the audit entry
    /// * `time` - Time of the audit entry
    /// * `event` - Event of the audit entry
    /// * `api` - API details of the audit entry
    /// # Returns
    /// * `AuditEntry` with the given values
    /// # Example
    /// ```
    /// use chrono::Utc;
    /// use rustfs_obs::{ApiDetails, AuditEntry};
    /// let entry = AuditEntry::new_with_values(
    ///    "v1".to_string(),
    ///     Utc::now(),
    ///     "event".to_string(),
    ///     ApiDetails::new(),
    /// );
    /// ```
    /// # Remarks
    /// This is a convenience method to create an `AuditEntry` with the given values
    /// without having to set each field individually
    /// This is useful when you want to create an `AuditEntry` with the given values
    /// without having to set each field individually
    pub fn new_with_values(version: String, time: DateTime<Utc>, event: String, api: ApiDetails) -> Self {
        AuditEntry {
            version,
            deployment_id: None,
            time,
            event,
            entry_type: None,
            api,
            remote_host: None,
            request_id: None,
            user_agent: None,
            req_path: None,
            req_host: None,
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

    /// Set the time
    pub fn set_time(mut self, time: DateTime<Utc>) -> Self {
        self.time = time;
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

    /// Set the request ID
    pub fn set_request_id(mut self, request_id: Option<String>) -> Self {
        self.request_id = request_id;
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

    /// Set the tags
    pub fn set_tags(mut self, tags: Option<HashMap<String, Value>>) -> Self {
        self.tags = tags;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_entry() {
        let entry = AuditEntry::new()
            .set_version("v1".to_string())
            .set_deployment_id(Some("12345".to_string()))
            .set_time(Utc::now())
            .set_event("event".to_string())
            .set_entry_type(Some("type".to_string()))
            .set_api(ApiDetails::new())
            .set_remote_host(Some("localhost".to_string()))
            .set_request_id(Some("req-12345".to_string()))
            .set_user_agent(Some("user-agent".to_string()))
            .set_req_path(Some("/path".to_string()))
            .set_req_host(Some("localhost".to_string()))
            .set_req_claims(Some(HashMap::new()))
            .set_req_query(Some(HashMap::new()))
            .set_req_header(Some(HashMap::new()))
            .set_resp_header(Some(HashMap::new()))
            .set_tags(Some(HashMap::new()))
            .set_access_key(Some("access-key".to_string()))
            .set_parent_user(Some("parent-user".to_string()))
            .set_error(Some("error".to_string()));

        assert_eq!(entry.version, "v1");
        assert_eq!(entry.deployment_id, Some("12345".to_string()));
        assert_eq!(entry.event, "event");
        assert_eq!(entry.entry_type, Some("type".to_string()));
        assert_eq!(entry.remote_host, Some("localhost".to_string()));
        assert_eq!(entry.request_id, Some("req-12345".to_string()));
        assert_eq!(entry.user_agent, Some("user-agent".to_string()));
        assert_eq!(entry.req_path, Some("/path".to_string()));
        assert_eq!(entry.req_host, Some("localhost".to_string()));
        assert_eq!(entry.access_key, Some("access-key".to_string()));
        assert_eq!(entry.parent_user, Some("parent-user".to_string()));
        assert_eq!(entry.error, Some("error".to_string()));
    }

    #[test]
    fn test_api_details() {
        let api = ApiDetails::new()
            .set_name(Some("name".to_string()))
            .set_bucket(Some("bucket".to_string()))
            .set_object(Some("object".to_string()))
            .set_objects(vec![ObjectVersion {
                object_name: "object".to_string(),
                version_id: Some("12345".to_string()),
            }])
            .set_status(Some("status".to_string()))
            .set_status_code(Some(200))
            .set_input_bytes(100)
            .set_output_bytes(200)
            .set_header_bytes(Some(300))
            .set_time_to_first_byte(Some("100ms".to_string()))
            .set_time_to_first_byte_in_ns(Some("100ns".to_string()))
            .set_time_to_response(Some("200ms".to_string()))
            .set_time_to_response_in_ns(Some("200ns".to_string()));

        assert_eq!(api.name, Some("name".to_string()));
        assert_eq!(api.bucket, Some("bucket".to_string()));
        assert_eq!(api.object, Some("object".to_string()));
        assert_eq!(api.objects.len(), 1);
        assert_eq!(api.status, Some("status".to_string()));
        assert_eq!(api.status_code, Some(200));
        assert_eq!(api.input_bytes, 100);
        assert_eq!(api.output_bytes, 200);
        assert_eq!(api.header_bytes, Some(300));
        assert_eq!(api.time_to_first_byte, Some("100ms".to_string()));
        assert_eq!(api.time_to_first_byte_in_ns, Some("100ns".to_string()));
        assert_eq!(api.time_to_response, Some("200ms".to_string()));
        assert_eq!(api.time_to_response_in_ns, Some("200ns".to_string()));
    }
}
