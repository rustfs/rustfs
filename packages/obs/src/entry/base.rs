use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// ObjectVersion object version key/versionId
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Default)]
pub struct ObjectVersion {
    #[serde(rename = "objectName")]
    pub object_name: String,
    #[serde(rename = "versionId", skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
}

impl ObjectVersion {
    /// Create a new ObjectVersion object
    pub fn new() -> Self {
        ObjectVersion {
            object_name: String::new(),
            version_id: None,
        }
    }

    /// Create a new ObjectVersion object with object name
    pub fn new_with_object_name(object_name: String) -> Self {
        ObjectVersion {
            object_name,
            version_id: None,
        }
    }
    /// Set the object name
    pub fn set_object_name(mut self, object_name: String) -> Self {
        self.object_name = object_name;
        self
    }

    /// Set the version ID
    pub fn set_version_id(mut self, version_id: Option<String>) -> Self {
        self.version_id = version_id;
        self
    }
}

/// Args - defines the arguments for the API
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
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

impl Args {
    /// Create a new Args object
    pub fn new() -> Self {
        Args {
            bucket: None,
            object: None,
            version_id: None,
            objects: None,
            metadata: None,
        }
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

    /// Set the version ID
    pub fn set_version_id(mut self, version_id: Option<String>) -> Self {
        self.version_id = version_id;
        self
    }

    /// Set the objects
    pub fn set_objects(mut self, objects: Option<Vec<ObjectVersion>>) -> Self {
        self.objects = objects;
        self
    }

    /// Set the metadata
    pub fn set_metadata(mut self, metadata: Option<HashMap<String, String>>) -> Self {
        self.metadata = metadata;
        self
    }
}

/// Trace - defines the trace
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct Trace {
    #[serde(rename = "message", skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(rename = "source", skip_serializing_if = "Option::is_none")]
    pub source: Option<Vec<String>>,
    #[serde(rename = "variables", skip_serializing_if = "Option::is_none")]
    pub variables: Option<HashMap<String, Value>>,
}

impl Trace {
    /// Create a new Trace object
    pub fn new() -> Self {
        Trace {
            message: None,
            source: None,
            variables: None,
        }
    }

    /// Set the message
    pub fn set_message(mut self, message: Option<String>) -> Self {
        self.message = message;
        self
    }

    /// Set the source
    pub fn set_source(mut self, source: Option<Vec<String>>) -> Self {
        self.source = source;
        self
    }

    /// Set the variables
    pub fn set_variables(mut self, variables: Option<HashMap<String, Value>>) -> Self {
        self.variables = variables;
        self
    }
}

/// API - defines the api type and its args
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct API {
    #[serde(rename = "name", skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(rename = "args", skip_serializing_if = "Option::is_none")]
    pub args: Option<Args>,
}

impl API {
    /// Create a new API object
    pub fn new() -> Self {
        API { name: None, args: None }
    }

    /// Set the name
    pub fn set_name(mut self, name: Option<String>) -> Self {
        self.name = name;
        self
    }

    /// Set the args
    pub fn set_args(mut self, args: Option<Args>) -> Self {
        self.args = args;
        self
    }
}

/// Log kind/level enum
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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

impl Default for LogKind {
    fn default() -> Self {
        LogKind::Info
    }
}

/// Entry - defines fields and values of each log entry
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
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

impl Entry {
    /// Create a new Entry object with default values
    pub fn new() -> Self {
        Entry {
            site: None,
            deployment_id: None,
            level: LogKind::Info,
            log_kind: None,
            time: Utc::now(),
            api: None,
            remote_host: None,
            host: None,
            request_id: None,
            user_agent: None,
            message: None,
            trace: None,
        }
    }

    /// Set the site
    pub fn set_site(mut self, site: Option<String>) -> Self {
        self.site = site;
        self
    }

    /// Set the deployment ID
    pub fn set_deployment_id(mut self, deployment_id: Option<String>) -> Self {
        self.deployment_id = deployment_id;
        self
    }

    /// Set the level
    pub fn set_level(mut self, level: LogKind) -> Self {
        self.level = level;
        self
    }

    /// Set the log kind
    pub fn set_log_kind(mut self, log_kind: Option<LogKind>) -> Self {
        self.log_kind = log_kind;
        self
    }

    /// Set the time
    pub fn set_time(mut self, time: DateTime<Utc>) -> Self {
        self.time = time;
        self
    }

    /// Set the API
    pub fn set_api(mut self, api: Option<API>) -> Self {
        self.api = api;
        self
    }

    /// Set the remote host
    pub fn set_remote_host(mut self, remote_host: Option<String>) -> Self {
        self.remote_host = remote_host;
        self
    }

    /// Set the host
    pub fn set_host(mut self, host: Option<String>) -> Self {
        self.host = host;
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

    /// Set the message
    pub fn set_message(mut self, message: Option<String>) -> Self {
        self.message = message;
        self
    }

    /// Set the trace
    pub fn set_trace(mut self, trace: Option<Trace>) -> Self {
        self.trace = trace;
        self
    }
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

impl Info {
    /// Create a new Info object with default values
    pub fn new() -> Self {
        Info {
            entry: Entry::new(),
            console_msg: String::new(),
            node_name: String::new(),
            err: None,
        }
    }

    /// Create a new Info object with console message and node name
    pub fn new_with_console_msg(console_msg: String, node_name: String) -> Self {
        Info {
            entry: Entry::new(),
            console_msg,
            node_name,
            err: None,
        }
    }

    /// Set the node name
    pub fn set_node_name(&mut self, node_name: String) {
        self.node_name = node_name;
    }

    /// Set the entry
    pub fn set_entry(&mut self, entry: Entry) {
        self.entry = entry;
    }

    /// Set the console message
    pub fn set_console_msg(&mut self, console_msg: String) {
        self.console_msg = console_msg;
    }

    /// Set the error message
    pub fn set_err(&mut self, err: Option<String>) {
        self.err = err;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_version() {
        let mut object_version = ObjectVersion::new();
        object_version = object_version.clone().set_object_name("object".to_string());
        object_version = object_version.clone().set_version_id(Some("version".to_string()));
        assert_eq!(object_version.object_name, "object".to_string());
        assert_eq!(object_version.version_id, Some("version".to_string()));
    }

    #[test]
    fn test_object_version_with_object_name() {
        let object_version = ObjectVersion::new_with_object_name("object".to_string());
        assert_eq!(object_version.object_name, "object".to_string());
        assert_eq!(object_version.version_id, None);
    }

    #[test]
    fn test_args() {
        let mut obj = ObjectVersion::new();
        obj.object_name = "object".to_string();
        obj.version_id = Some("version".to_string());
        let objs = vec![obj];
        let args = Args::new()
            .set_bucket(Some("bucket".to_string()))
            .set_object(Some("object".to_string()))
            .set_version_id(Some("version".to_string()))
            .set_objects(Some(objs.clone()))
            .set_metadata(Some(HashMap::new()));

        assert_eq!(args.bucket, Some("bucket".to_string()));
        assert_eq!(args.object, Some("object".to_string()));
        assert_eq!(args.version_id, Some("version".to_string()));
        assert_eq!(args.objects, Some(objs));
        assert_eq!(args.metadata, Some(HashMap::new()));
    }

    #[test]
    fn test_trace() {
        let trace = Trace::new()
            .set_message(Some("message".to_string()))
            .set_source(Some(vec!["source".to_string()]))
            .set_variables(Some(HashMap::new()));

        assert_eq!(trace.message, Some("message".to_string()));
        assert_eq!(trace.source, Some(vec!["source".to_string()]));
        assert_eq!(trace.variables, Some(HashMap::new()));
    }

    #[test]
    fn test_api() {
        let api = API::new().set_name(Some("name".to_string())).set_args(Some(Args::new()));

        assert_eq!(api.name, Some("name".to_string()));
        assert_eq!(api.args, Some(Args::new()));
    }

    #[test]
    fn test_log_kind() {
        assert_eq!(LogKind::default(), LogKind::Info);
    }

    #[test]
    fn test_entry() {
        let entry = Entry::new()
            .set_site(Some("site".to_string()))
            .set_deployment_id(Some("deployment_id".to_string()))
            .set_level(LogKind::Info)
            .set_log_kind(Some(LogKind::Info))
            .set_time(Utc::now())
            .set_api(Some(API::new()))
            .set_remote_host(Some("remote_host".to_string()))
            .set_host(Some("host".to_string()))
            .set_request_id(Some("request_id".to_string()))
            .set_user_agent(Some("user_agent".to_string()))
            .set_message(Some("message".to_string()))
            .set_trace(Some(Trace::new()));

        assert_eq!(entry.site, Some("site".to_string()));
        assert_eq!(entry.deployment_id, Some("deployment_id".to_string()));
        assert_eq!(entry.level, LogKind::Info);
        assert_eq!(entry.log_kind, Some(LogKind::Info));
        assert_eq!(entry.api, Some(API::new()));
        assert_eq!(entry.remote_host, Some("remote_host".to_string()));
        assert_eq!(entry.host, Some("host".to_string()));
        assert_eq!(entry.request_id, Some("request_id".to_string()));
        assert_eq!(entry.user_agent, Some("user_agent".to_string()));
        assert_eq!(entry.message, Some("message".to_string()));
        assert_eq!(entry.trace, Some(Trace::new()));
    }

    #[test]
    fn test_info() {
        let mut info = Info::new();
        info.set_node_name("node_name".to_string());
        info.set_entry(Entry::new());
        info.set_console_msg("console_msg".to_string());
        info.set_err(Some("err".to_string()));

        assert_eq!(info.node_name, "node_name".to_string());
        // assert_eq!(info.entry, Entry::new());
        assert_eq!(info.console_msg, "console_msg".to_string());
        assert_eq!(info.err, Some("err".to_string()));
    }

    #[test]
    fn test_info_with_console_msg() {
        let info = Info::new_with_console_msg("console_msg".to_string(), "node_name".to_string());

        assert_eq!(info.node_name, "node_name".to_string());
        assert_eq!(info.console_msg, "console_msg".to_string());
        assert_eq!(info.err, None);
    }
}
