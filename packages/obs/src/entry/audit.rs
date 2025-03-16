use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// ObjectVersion object version key/versionId
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ObjectVersion {
    #[serde(rename = "objectName")]
    pub object_name: String,
    #[serde(rename = "versionId", skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
}

/// API details structure
#[derive(Debug, Serialize, Deserialize, Clone)]
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

/// Entry - audit entry logs
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AuditEntry {
    pub version: String,
    #[serde(rename = "deploymentid", skip_serializing_if = "Option::is_none")]
    pub deployment_id: Option<String>,
    pub time: DateTime<Utc>,
    pub event: String,

    // Class of audit message - S3, admin ops, bucket management
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub entry_type: Option<String>,

    // Deprecated, replaced by 'Event'
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
