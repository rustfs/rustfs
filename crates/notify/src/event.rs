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

use chrono::{DateTime, SecondsFormat, Utc};
use hashbrown::HashMap;
use rustfs_s3_common::EventName;
use serde::{Deserialize, Serialize};
use url::form_urlencoded;

/// Represents the identity of the user who triggered the event
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Identity {
    /// The principal ID of the user
    pub principal_id: String,
}

/// Represents the bucket that the object is in
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
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
#[serde(rename_all = "camelCase")]
pub struct Object {
    /// The key (name) of the object
    pub key: String,
    /// The size of the object in bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<i64>,
    /// The entity tag (ETag) of the object
    #[serde(skip_serializing_if = "Option::is_none")]
    pub e_tag: Option<String>,
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
#[serde(rename_all = "camelCase")]
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
#[serde(rename_all = "camelCase")]
pub struct Source {
    /// The host where the event originated
    pub host: String,
    /// The port on the host
    pub port: String,
    /// The user agent that caused the event
    pub user_agent: String,
}

/// Additional data included for restore-completed events.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GlacierEventData {
    pub restore_event_data: RestoreEventData,
}

/// Restore-specific event attributes for `s3:ObjectRestore:Completed`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RestoreEventData {
    pub lifecycle_restoration_expiry_time: String,
    pub lifecycle_restore_storage_class: String,
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
    /// Additional restore event data when present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub glacier_event_data: Option<GlacierEventData>,
    /// Information about the source of the event
    pub source: Source,
}

impl Event {
    fn event_version_for(event_name: EventName) -> &'static str {
        match event_name {
            EventName::ObjectReplicationFailed
            | EventName::ObjectReplicationComplete
            | EventName::ObjectReplicationMissedThreshold
            | EventName::ObjectReplicationReplicatedAfterThreshold
            | EventName::ObjectReplicationNotTracked => "2.2",
            EventName::ObjectRestoreCompleted
            | EventName::ObjectAclPut
            | EventName::ObjectTaggingPut
            | EventName::ObjectTaggingDelete
            | EventName::LifecycleExpirationDelete
            | EventName::LifecycleExpirationDeleteMarkerCreated
            | EventName::LifecycleTransition
            | EventName::IntelligentTiering => "2.3",
            _ => "2.1",
        }
    }

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
            event_version: Self::event_version_for(event_name).to_string(),
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
                    e_tag: Some("etag123".to_string()),
                    content_type: Some("application/octet-stream".to_string()),
                    user_metadata: Some(user_metadata),
                    version_id: Some("1".to_string()),
                    sequencer: "0055AED6DCD90281E5".to_string(),
                },
            },
            glacier_event_data: None,
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
        let sequencer = match args.object.mod_time {
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
                sequencer,
                ..Default::default()
            },
        };

        let is_removed_event = matches!(
            args.event_name,
            EventName::ObjectRemovedDelete | EventName::ObjectRemovedDeleteMarkerCreated
        );

        if !is_removed_event {
            s3_metadata.object.size = Some(args.object.size);
            s3_metadata.object.e_tag = args.object.etag.clone();
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

        let glacier_event_data = if args.event_name == EventName::ObjectRestoreCompleted {
            args.object.restore_expires.and_then(|expiry| {
                let expiry_time = DateTime::<Utc>::from_timestamp(expiry.unix_timestamp(), expiry.nanosecond())?;
                let storage_class = args.object.storage_class.clone().or_else(|| {
                    (!args.object.transitioned_object.tier.is_empty()).then_some(args.object.transitioned_object.tier.clone())
                })?;
                Some(GlacierEventData {
                    restore_event_data: RestoreEventData {
                        lifecycle_restoration_expiry_time: expiry_time.to_rfc3339_opts(SecondsFormat::Millis, true),
                        lifecycle_restore_storage_class: storage_class,
                    },
                })
            })
        } else {
            None
        };

        Self {
            event_version: Self::event_version_for(args.event_name).to_string(),
            event_source: "rustfs:s3".to_string(),
            aws_region: args.req_params.get("region").cloned().unwrap_or_default(),
            event_time: event_time.and_utc(),
            event_name: args.event_name,
            user_identity: Identity { principal_id },
            request_parameters: args.req_params,
            response_elements: resp_elements,
            s3: s3_metadata,
            glacier_event_data,
            source: Source {
                host: args.host,
                port: if args.port == 0 {
                    "".to_string()
                } else {
                    args.port.to_string()
                },
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
    pub port: u16,
    pub user_agent: String,
}

impl EventArgs {
    /// True when the RustFS replication header is explicitly enabled (`true` or `1`).
    ///
    /// Only `x-rustfs-source-replication-request` is considered here. Many clients (including the
    /// console) send `x-minio-source-replication-request` for MinIO compatibility; treating that
    /// as replication would suppress webhooks on normal browser deletes. Storage still honors both
    /// prefixes when parsing the typed HTTP headers for `ObjectOptions`.
    pub fn is_replication_request(&self) -> bool {
        self.replication_header_value_true("x-rustfs-source-replication-request")
    }

    fn replication_header_value_true(&self, key: &str) -> bool {
        self.req_params
            .get(key)
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .unwrap_or(false)
    }
}

/// Builder for [`EventArgs`].
///
/// This builder provides a fluent API to construct an `EventArgs` instance,
/// ensuring that all required fields are provided.
///
/// # Example
///
/// ```ignore
/// let args = EventArgsBuilder::new(
///     EventName::ObjectCreatedPut,
///     "my-bucket",
///     object_info,
/// )
/// .host("localhost:9000")
/// .user_agent("my-app/1.0")
/// .build();
/// ```
#[derive(Debug, Clone, Default)]
pub struct EventArgsBuilder {
    event_name: EventName,
    bucket_name: String,
    object: rustfs_ecstore::store_api::ObjectInfo,
    req_params: HashMap<String, String>,
    resp_elements: HashMap<String, String>,
    version_id: String,
    host: String,
    port: u16,
    user_agent: String,
}

impl EventArgsBuilder {
    /// Creates a new builder with the required fields.
    pub fn new(event_name: EventName, bucket_name: impl Into<String>, object: rustfs_ecstore::store_api::ObjectInfo) -> Self {
        Self {
            event_name,
            bucket_name: bucket_name.into(),
            object,
            ..Default::default()
        }
    }

    /// Sets the event name.
    pub fn event_name(mut self, event_name: EventName) -> Self {
        self.event_name = event_name;
        self
    }

    /// Sets the bucket name.
    pub fn bucket_name(mut self, bucket_name: impl Into<String>) -> Self {
        self.bucket_name = bucket_name.into();
        self
    }

    /// Sets the object information.
    pub fn object(mut self, object: rustfs_ecstore::store_api::ObjectInfo) -> Self {
        self.object = object;
        self
    }

    /// Sets the request parameters.
    pub fn req_params(mut self, req_params: HashMap<String, String>) -> Self {
        self.req_params = req_params;
        self
    }

    /// Adds a single request parameter.
    pub fn req_param(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.req_params.insert(key.into(), value.into());
        self
    }

    /// Sets the response elements.
    pub fn resp_elements(mut self, resp_elements: HashMap<String, String>) -> Self {
        self.resp_elements = resp_elements;
        self
    }

    /// Adds a single response element.
    pub fn resp_element(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.resp_elements.insert(key.into(), value.into());
        self
    }

    /// Sets the version ID.
    pub fn version_id(mut self, version_id: impl Into<String>) -> Self {
        self.version_id = version_id.into();
        self
    }

    /// Sets the host.
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = host.into();
        self
    }

    /// Sets the port.
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Sets the user agent.
    pub fn user_agent(mut self, user_agent: impl Into<String>) -> Self {
        self.user_agent = user_agent.into();
        self
    }

    /// Builds the final `EventArgs` instance.
    ///
    /// This method consumes the builder and returns the constructed `EventArgs`.
    pub fn build(self) -> EventArgs {
        EventArgs {
            event_name: self.event_name,
            bucket_name: self.bucket_name,
            object: self.object,
            req_params: self.req_params,
            resp_elements: self.resp_elements,
            version_id: self.version_id,
            host: self.host,
            port: self.port,
            user_agent: self.user_agent,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_test_event_uses_aws_compatible_event_versions() {
        let acl_event = Event::new_test_event("bucket", "key", EventName::ObjectAclPut);
        assert_eq!(acl_event.event_version, "2.3");

        let tagging_event = Event::new_test_event("bucket", "key", EventName::ObjectTaggingPut);
        assert_eq!(tagging_event.event_version, "2.3");

        let lifecycle_event = Event::new_test_event("bucket", "key", EventName::LifecycleExpirationDelete);
        assert_eq!(lifecycle_event.event_version, "2.3");

        let put_event = Event::new_test_event("bucket", "key", EventName::ObjectCreatedPut);
        assert_eq!(put_event.event_version, "2.1");
    }

    #[test]
    fn event_new_uses_aws_compatible_event_versions() {
        let args = EventArgsBuilder::new(
            EventName::LifecycleTransition,
            "bucket",
            rustfs_ecstore::store_api::ObjectInfo {
                bucket: "bucket".to_string(),
                name: "key".to_string(),
                ..Default::default()
            },
        )
        .build();
        let event = Event::new(args);
        assert_eq!(event.event_version, "2.3");
    }

    #[test]
    fn object_restore_completed_includes_glacier_event_data() {
        let args = EventArgsBuilder::new(
            EventName::ObjectRestoreCompleted,
            "bucket",
            rustfs_ecstore::store_api::ObjectInfo {
                bucket: "bucket".to_string(),
                name: "key".to_string(),
                restore_expires: Some(time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()),
                storage_class: Some("GLACIER".to_string()),
                ..Default::default()
            },
        )
        .build();
        let event = Event::new(args);

        assert_eq!(event.event_version, "2.3");
        let glacier = event.glacier_event_data.expect("glacier event data should be present");
        assert_eq!(glacier.restore_event_data.lifecycle_restoration_expiry_time, "2023-11-14T22:13:20.000Z");
        assert_eq!(glacier.restore_event_data.lifecycle_restore_storage_class, "GLACIER");
    }
}

#[cfg(test)]
mod event_args_tests {
    use super::EventArgs;
    use hashbrown::HashMap;
    use rustfs_ecstore::store_api::ObjectInfo;
    use rustfs_s3_common::EventName;

    fn args_with_headers(pairs: &[(&str, &str)]) -> EventArgs {
        let mut req_params = HashMap::new();
        for (k, v) in pairs {
            req_params.insert((*k).to_string(), (*v).to_string());
        }
        EventArgs {
            event_name: EventName::ObjectRemovedDelete,
            bucket_name: "b".to_string(),
            object: ObjectInfo::default(),
            req_params,
            resp_elements: HashMap::new(),
            version_id: String::new(),
            host: String::new(),
            port: 0,
            user_agent: String::new(),
        }
    }

    #[test]
    fn replication_request_requires_true_value() {
        assert!(!args_with_headers(&[("x-rustfs-source-replication-request", "")]).is_replication_request());
        assert!(!args_with_headers(&[("x-rustfs-source-replication-request", "false")]).is_replication_request());
        assert!(args_with_headers(&[("x-rustfs-source-replication-request", "true")]).is_replication_request());
        assert!(args_with_headers(&[("x-rustfs-source-replication-request", "True")]).is_replication_request());
        assert!(!args_with_headers(&[("x-minio-source-replication-request", "true")]).is_replication_request());
    }
}
