use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub event_version: String,
    pub event_source: String,
    pub aws_region: String,
    pub event_time: String,
    pub event_name: String,
    pub user_identity: Identity,
    pub request_parameters: HashMap<String, String>,
    pub response_elements: HashMap<String, String>,
    pub s3: Metadata,
    pub source: Source,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Identity {
    pub principal_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub schema_version: String,
    pub configuration_id: String,
    pub bucket: Bucket,
    pub object: Object,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bucket {
    pub name: String,
    pub owner_identity: Identity,
    pub arn: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Object {
    pub key: String,
    pub version_id: Option<String>,
    pub sequencer: String,
    pub size: Option<u64>,
    pub etag: Option<String>,
    pub content_type: Option<String>,
    pub user_metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Source {
    pub host: String,
    pub user_agent: String,
}
