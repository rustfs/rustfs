use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// ObjectVersion object version key/versionId
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectVersion {
    #[serde(rename = "objectName")]
    pub object_name: String,
    #[serde(rename = "versionId", skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
}

/// Args - defines the arguments for the API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Args {
    #[serde(rename = "bucket", skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
    #[serde(rename = "object", skip_serializing_if = "Option::is_none")]
    pub object: Option<String>,
    #[serde(rename = "versionId", skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
    #[serde(rename = "objects", skip_serializing_if = "Option::is_none")]
    pub objects: Option<Vec<ObjectVersion>>,
    #[serde(rename = "metadata", skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
}

/// Trace - defines the trace
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trace {
    #[serde(rename = "message", skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(rename = "source", skip_serializing_if = "Option::is_none")]
    pub source: Option<Vec<String>>,
    #[serde(rename = "variables", skip_serializing_if = "Option::is_none")]
    pub variables: Option<HashMap<String, Value>>,
}

/// API - defines the api type and its args
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct API {
    #[serde(rename = "name", skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(rename = "args", skip_serializing_if = "Option::is_none")]
    pub args: Option<Args>,
}

/// Log kind/level enum
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogKind {
    #[serde(rename = "INFO")]
    Info,
    #[serde(rename = "WARNING")]
    Warning,
    #[serde(rename = "ERROR")]
    Error,
    #[serde(rename = "FATAL")]
    Fatal,
}

/// Entry - defines fields and values of each log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    #[serde(rename = "site", skip_serializing_if = "Option::is_none")]
    pub site: Option<String>,

    #[serde(rename = "deploymentid", skip_serializing_if = "Option::is_none")]
    pub deployment_id: Option<String>,

    pub level: LogKind,

    #[serde(rename = "errKind", skip_serializing_if = "Option::is_none")]
    pub log_kind: Option<LogKind>, // Deprecated Jan 2024

    pub time: DateTime<Utc>,

    #[serde(rename = "api", skip_serializing_if = "Option::is_none")]
    pub api: Option<API>,

    #[serde(rename = "remotehost", skip_serializing_if = "Option::is_none")]
    pub remote_host: Option<String>,

    #[serde(rename = "host", skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,

    #[serde(rename = "requestID", skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,

    #[serde(rename = "userAgent", skip_serializing_if = "Option::is_none")]
    pub user_agent: Option<String>,

    #[serde(rename = "message", skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    #[serde(rename = "error", skip_serializing_if = "Option::is_none")]
    pub trace: Option<Trace>,
}

/// Info holds console log messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Info {
    #[serde(flatten)]
    pub entry: Entry,

    pub console_msg: String,

    #[serde(rename = "node")]
    pub node_name: String,

    #[serde(skip)]
    pub err: Option<String>,
}
