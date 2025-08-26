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
use std::collections::HashMap;
use url::form_urlencoded;

/// Represents the identity of the user who triggered the event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Identity {
    /// The principal ID of the user
    pub principal_id: String,
}

/// Represents the bucket that the object is in
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bucket {
    /// The name of the bucket
    pub name: String,
    /// The owner identity of the bucket
    pub owner_identity: Identity,
    /// The Amazon Resource Name (ARN) of the bucket
    pub arn: String,
}

/// Represents the object that the event occurred on
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Object {
    /// The key (name) of the object
    pub key: String,
    /// The size of the object in bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<i64>,
    /// The entity tag (ETag) of the object
    #[serde(skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    /// The content type of the object
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    /// User-defined metadata associated with the object
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_metadata: Option<HashMap<String, String>>,
    /// The version ID of the object (if versioning is enabled)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
    /// A unique identifier for the event
    pub sequencer: String,
}

/// Metadata about the event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    /// The schema version of the event
    #[serde(rename = "s3SchemaVersion")]
    pub schema_version: String,
    /// The ID of the configuration that triggered the event
    pub configuration_id: String,
    /// Information about the bucket
    pub bucket: Bucket,
    /// Information about the object
    pub object: Object,
}

/// Information about the source of the event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Source {
    /// The host where the event originated
    pub host: String,
    /// The port on the host
    pub port: String,
    /// The user agent that caused the event
    #[serde(rename = "userAgent")]
    pub user_agent: String,
}

/// Represents a storage event
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    /// The version of the event
    pub event_version: String,
    /// The source of the event
    pub event_source: String,
    /// The AWS region where the event occurred
    pub aws_region: String,
    /// The time when the event occurred
    pub event_time: DateTime<Utc>,
    /// The name of the event
    pub event_name: EventName,
    /// The identity of the user who triggered the event
    pub user_identity: Identity,
    /// Parameters from the request that caused the event
    pub request_parameters: HashMap<String, String>,
    /// Elements from the response
    pub response_elements: HashMap<String, String>,
    /// Metadata about the event
    pub s3: Metadata,
    /// Information about the source of the event
    pub source: Source,
}

impl Event {
    /// Creates a test event for a given bucket and object
    pub fn new_test_event(bucket: &str, key: &str, event_name: EventName) -> Self {
        let mut user_metadata = HashMap::new();
        user_metadata.insert("x-amz-meta-test".to_string(), "value".to_string());
        user_metadata.insert("x-amz-storage-storage-options".to_string(), "value".to_string());
        user_metadata.insert("x-amz-meta-".to_string(), "value".to_string());
        user_metadata.insert("x-rustfs-meta-".to_string(), "rustfs-value".to_string());
        user_metadata.insert("x-request-id".to_string(), "request-id-123".to_string());
        user_metadata.insert("x-bucket".to_string(), "bucket".to_string());
        user_metadata.insert("x-object".to_string(), "object".to_string());
        user_metadata.insert("x-rustfs-origin-endpoint".to_string(), "http://127.0.0.1".to_string());
        user_metadata.insert("x-rustfs-user-metadata".to_string(), "metadata".to_string());
        user_metadata.insert("x-rustfs-deployment-id".to_string(), "deployment-id-123".to_string());
        user_metadata.insert("x-rustfs-origin-endpoint-code".to_string(), "http://127.0.0.1".to_string());
        user_metadata.insert("x-rustfs-bucket-name".to_string(), "bucket".to_string());
        user_metadata.insert("x-rustfs-object-key".to_string(), key.to_string());
        user_metadata.insert("x-rustfs-object-size".to_string(), "1024".to_string());
        user_metadata.insert("x-rustfs-object-etag".to_string(), "etag123".to_string());
        user_metadata.insert("x-rustfs-object-version-id".to_string(), "1".to_string());
        user_metadata.insert("x-request-time".to_string(), Utc::now().to_rfc3339());

        Event {
            event_version: "2.1".to_string(),
            event_source: "rustfs:s3".to_string(),
            aws_region: "us-east-1".to_string(),
            event_time: Utc::now(),
            event_name,
            user_identity: Identity {
                principal_id: "rustfs".to_string(),
            },
            request_parameters: HashMap::new(),
            response_elements: HashMap::new(),
            s3: Metadata {
                schema_version: "1.0".to_string(),
                configuration_id: "test-config".to_string(),
                bucket: Bucket {
                    name: bucket.to_string(),
                    owner_identity: Identity {
                        principal_id: "rustfs".to_string(),
                    },
                    arn: format!("arn:rustfs:s3:::{bucket}"),
                },
                object: Object {
                    key: key.to_string(),
                    size: Some(1024),
                    etag: Some("etag123".to_string()),
                    content_type: Some("application/octet-stream".to_string()),
                    user_metadata: Some(user_metadata),
                    version_id: Some("1".to_string()),
                    sequencer: "0055AED6DCD90281E5".to_string(),
                },
            },
            source: Source {
                host: "127.0.0.1".to_string(),
                port: "9000".to_string(),
                user_agent: "RustFS (linux; amd64) rustfs-rs/0.1".to_string(),
            },
        }
    }
    /// Return event mask
    pub fn mask(&self) -> u64 {
        self.event_name.mask()
    }

    pub fn new(args: EventArgs) -> Self {
        let event_time = Utc::now().naive_local();
        let unique_id = match args.object.mod_time {
            Some(t) => format!("{:X}", t.unix_timestamp_nanos()),
            None => format!("{:X}", event_time.and_utc().timestamp_nanos_opt().unwrap_or(0)),
        };

        let mut resp_elements = args.resp_elements.clone();
        initialize_response_elements(&mut resp_elements, &["x-amz-request-id", "x-amz-id-2"]);

        // URL encoding of object keys
        let key_name = form_urlencoded::byte_serialize(args.object.name.as_bytes()).collect::<String>();
        let principal_id = args.req_params.get("principalId").unwrap_or(&String::new()).to_string();

        let version_id = match args.object.version_id {
            Some(id) => Some(id.to_string()),
            None => Some(args.version_id.clone()),
        };

        let mut s3_metadata = Metadata {
            schema_version: "1.0".to_string(),
            configuration_id: "Config".to_string(), // or from args
            bucket: Bucket {
                name: args.bucket_name.clone(),
                owner_identity: Identity {
                    principal_id: principal_id.clone(),
                },
                arn: format!("arn:aws:s3:::{}", args.bucket_name),
            },
            object: Object {
                key: key_name,
                version_id,
                sequencer: unique_id,
                ..Default::default()
            },
        };

        let is_removed_event = matches!(
            args.event_name,
            EventName::ObjectRemovedDelete | EventName::ObjectRemovedDeleteMarkerCreated
        );

        if !is_removed_event {
            s3_metadata.object.size = Some(args.object.size);
            s3_metadata.object.etag = args.object.etag.clone();
            s3_metadata.object.content_type = args.object.content_type.clone();
            // Filter out internal reserved metadata
            let mut user_metadata = HashMap::new();
            for (k, v) in args.object.user_defined.iter() {
                if !k.to_lowercase().starts_with("x-amz-meta-internal-") {
                    user_metadata.insert(k.clone(), v.clone());
                }
            }
            s3_metadata.object.user_metadata = Some(user_metadata);
        }

        Self {
            event_version: "2.1".to_string(),
            event_source: "rustfs:s3".to_string(),
            aws_region: args.req_params.get("region").cloned().unwrap_or_default(),
            event_time: event_time.and_utc(),
            event_name: args.event_name,
            user_identity: Identity { principal_id },
            request_parameters: args.req_params,
            response_elements: resp_elements,
            s3: s3_metadata,
            source: Source {
                host: args.host,
                port: "".to_string(),
                user_agent: args.user_agent,
            },
        }
    }
}

fn initialize_response_elements(elements: &mut HashMap<String, String>, keys: &[&str]) {
    for key in keys {
        elements.entry(key.to_string()).or_default();
    }
}

#[derive(Debug, Clone)]
pub struct EventArgs {
    pub event_name: EventName,
    pub bucket_name: String,
    pub object: rustfs_ecstore::store_api::ObjectInfo,
    pub req_params: HashMap<String, String>,
    pub resp_elements: HashMap<String, String>,
    pub version_id: String,
    pub host: String,
    pub user_agent: String,
}

impl EventArgs {
    // Helper function to check if it is a copy request
    pub fn is_replication_request(&self) -> bool {
        self.req_params.contains_key("x-rustfs-source-replication-request")
    }
}
