use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// 当解析事件名称字符串失败时返回的错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseEventNameError(String);

impl fmt::Display for ParseEventNameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "无效的事件名称：{}", self.0)
    }
}

impl std::error::Error for ParseEventNameError {}

/// 表示对象上发生的事件类型。
/// 基于 AWS S3 事件类型，并包含 RustFS 扩展。
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum EventName {
    // 单一事件类型 (值为 1-32 以兼容掩码逻辑)
    ObjectAccessedGet = 1,
    ObjectAccessedGetRetention = 2,
    ObjectAccessedGetLegalHold = 3,
    ObjectAccessedHead = 4,
    ObjectAccessedAttributes = 5,
    ObjectCreatedCompleteMultipartUpload = 6,
    ObjectCreatedCopy = 7,
    ObjectCreatedPost = 8,
    ObjectCreatedPut = 9,
    ObjectCreatedPutRetention = 10,
    ObjectCreatedPutLegalHold = 11,
    ObjectCreatedPutTagging = 12,
    ObjectCreatedDeleteTagging = 13,
    ObjectRemovedDelete = 14,
    ObjectRemovedDeleteMarkerCreated = 15,
    ObjectRemovedDeleteAllVersions = 16,
    ObjectRemovedNoOP = 17,
    BucketCreated = 18,
    BucketRemoved = 19,
    ObjectReplicationFailed = 20,
    ObjectReplicationComplete = 21,
    ObjectReplicationMissedThreshold = 22,
    ObjectReplicationReplicatedAfterThreshold = 23,
    ObjectReplicationNotTracked = 24,
    ObjectRestorePost = 25,
    ObjectRestoreCompleted = 26,
    ObjectTransitionFailed = 27,
    ObjectTransitionComplete = 28,
    ScannerManyVersions = 29,                // 对应 Go 的 ObjectManyVersions
    ScannerLargeVersions = 30,               // 对应 Go 的 ObjectLargeVersions
    ScannerBigPrefix = 31,                   // 对应 Go 的 PrefixManyFolders
    LifecycleDelMarkerExpirationDelete = 32, // 对应 Go 的 ILMDelMarkerExpirationDelete

    // 复合 "All" 事件类型 (没有用于掩码的顺序值)
    ObjectAccessedAll,
    ObjectCreatedAll,
    ObjectRemovedAll,
    ObjectReplicationAll,
    ObjectRestoreAll,
    ObjectTransitionAll,
    ObjectScannerAll, // 新增，来自 Go
    Everything,       // 新增，来自 Go
}

// 用于 Everything.expand() 的单一事件类型顺序数组
const SINGLE_EVENT_NAMES_IN_ORDER: [EventName; 32] = [
    EventName::ObjectAccessedGet,
    EventName::ObjectAccessedGetRetention,
    EventName::ObjectAccessedGetLegalHold,
    EventName::ObjectAccessedHead,
    EventName::ObjectAccessedAttributes,
    EventName::ObjectCreatedCompleteMultipartUpload,
    EventName::ObjectCreatedCopy,
    EventName::ObjectCreatedPost,
    EventName::ObjectCreatedPut,
    EventName::ObjectCreatedPutRetention,
    EventName::ObjectCreatedPutLegalHold,
    EventName::ObjectCreatedPutTagging,
    EventName::ObjectCreatedDeleteTagging,
    EventName::ObjectRemovedDelete,
    EventName::ObjectRemovedDeleteMarkerCreated,
    EventName::ObjectRemovedDeleteAllVersions,
    EventName::ObjectRemovedNoOP,
    EventName::BucketCreated,
    EventName::BucketRemoved,
    EventName::ObjectReplicationFailed,
    EventName::ObjectReplicationComplete,
    EventName::ObjectReplicationMissedThreshold,
    EventName::ObjectReplicationReplicatedAfterThreshold,
    EventName::ObjectReplicationNotTracked,
    EventName::ObjectRestorePost,
    EventName::ObjectRestoreCompleted,
    EventName::ObjectTransitionFailed,
    EventName::ObjectTransitionComplete,
    EventName::ScannerManyVersions,
    EventName::ScannerLargeVersions,
    EventName::ScannerBigPrefix,
    EventName::LifecycleDelMarkerExpirationDelete,
];

const LAST_SINGLE_TYPE_VALUE: u32 = EventName::LifecycleDelMarkerExpirationDelete as u32;

impl EventName {
    /// 解析字符串为 EventName。
    pub fn parse(s: &str) -> Result<Self, ParseEventNameError> {
        match s {
            "s3:BucketCreated:*" => Ok(EventName::BucketCreated),
            "s3:BucketRemoved:*" => Ok(EventName::BucketRemoved),
            "s3:ObjectAccessed:*" => Ok(EventName::ObjectAccessedAll),
            "s3:ObjectAccessed:Get" => Ok(EventName::ObjectAccessedGet),
            "s3:ObjectAccessed:GetRetention" => Ok(EventName::ObjectAccessedGetRetention),
            "s3:ObjectAccessed:GetLegalHold" => Ok(EventName::ObjectAccessedGetLegalHold),
            "s3:ObjectAccessed:Head" => Ok(EventName::ObjectAccessedHead),
            "s3:ObjectAccessed:Attributes" => Ok(EventName::ObjectAccessedAttributes),
            "s3:ObjectCreated:*" => Ok(EventName::ObjectCreatedAll),
            "s3:ObjectCreated:CompleteMultipartUpload" => {
                Ok(EventName::ObjectCreatedCompleteMultipartUpload)
            }
            "s3:ObjectCreated:Copy" => Ok(EventName::ObjectCreatedCopy),
            "s3:ObjectCreated:Post" => Ok(EventName::ObjectCreatedPost),
            "s3:ObjectCreated:Put" => Ok(EventName::ObjectCreatedPut),
            "s3:ObjectCreated:PutRetention" => Ok(EventName::ObjectCreatedPutRetention),
            "s3:ObjectCreated:PutLegalHold" => Ok(EventName::ObjectCreatedPutLegalHold),
            "s3:ObjectCreated:PutTagging" => Ok(EventName::ObjectCreatedPutTagging),
            "s3:ObjectCreated:DeleteTagging" => Ok(EventName::ObjectCreatedDeleteTagging),
            "s3:ObjectRemoved:*" => Ok(EventName::ObjectRemovedAll),
            "s3:ObjectRemoved:Delete" => Ok(EventName::ObjectRemovedDelete),
            "s3:ObjectRemoved:DeleteMarkerCreated" => {
                Ok(EventName::ObjectRemovedDeleteMarkerCreated)
            }
            "s3:ObjectRemoved:NoOP" => Ok(EventName::ObjectRemovedNoOP),
            "s3:ObjectRemoved:DeleteAllVersions" => Ok(EventName::ObjectRemovedDeleteAllVersions),
            "s3:LifecycleDelMarkerExpiration:Delete" => {
                Ok(EventName::LifecycleDelMarkerExpirationDelete)
            }
            "s3:Replication:*" => Ok(EventName::ObjectReplicationAll),
            "s3:Replication:OperationFailedReplication" => Ok(EventName::ObjectReplicationFailed),
            "s3:Replication:OperationCompletedReplication" => {
                Ok(EventName::ObjectReplicationComplete)
            }
            "s3:Replication:OperationMissedThreshold" => {
                Ok(EventName::ObjectReplicationMissedThreshold)
            }
            "s3:Replication:OperationReplicatedAfterThreshold" => {
                Ok(EventName::ObjectReplicationReplicatedAfterThreshold)
            }
            "s3:Replication:OperationNotTracked" => Ok(EventName::ObjectReplicationNotTracked),
            "s3:ObjectRestore:*" => Ok(EventName::ObjectRestoreAll),
            "s3:ObjectRestore:Post" => Ok(EventName::ObjectRestorePost),
            "s3:ObjectRestore:Completed" => Ok(EventName::ObjectRestoreCompleted),
            "s3:ObjectTransition:Failed" => Ok(EventName::ObjectTransitionFailed),
            "s3:ObjectTransition:Complete" => Ok(EventName::ObjectTransitionComplete),
            "s3:ObjectTransition:*" => Ok(EventName::ObjectTransitionAll),
            "s3:Scanner:ManyVersions" => Ok(EventName::ScannerManyVersions),
            "s3:Scanner:LargeVersions" => Ok(EventName::ScannerLargeVersions),
            "s3:Scanner:BigPrefix" => Ok(EventName::ScannerBigPrefix),
            // ObjectScannerAll 和 Everything 不能从字符串解析，因为 Go 版本也没有定义它们的字符串表示
            _ => Err(ParseEventNameError(s.to_string())),
        }
    }

    /// 返回事件类型的字符串表示。
    pub fn as_str(&self) -> &'static str {
        match self {
            EventName::BucketCreated => "s3:BucketCreated:*",
            EventName::BucketRemoved => "s3:BucketRemoved:*",
            EventName::ObjectAccessedAll => "s3:ObjectAccessed:*",
            EventName::ObjectAccessedGet => "s3:ObjectAccessed:Get",
            EventName::ObjectAccessedGetRetention => "s3:ObjectAccessed:GetRetention",
            EventName::ObjectAccessedGetLegalHold => "s3:ObjectAccessed:GetLegalHold",
            EventName::ObjectAccessedHead => "s3:ObjectAccessed:Head",
            EventName::ObjectAccessedAttributes => "s3:ObjectAccessed:Attributes",
            EventName::ObjectCreatedAll => "s3:ObjectCreated:*",
            EventName::ObjectCreatedCompleteMultipartUpload => {
                "s3:ObjectCreated:CompleteMultipartUpload"
            }
            EventName::ObjectCreatedCopy => "s3:ObjectCreated:Copy",
            EventName::ObjectCreatedPost => "s3:ObjectCreated:Post",
            EventName::ObjectCreatedPut => "s3:ObjectCreated:Put",
            EventName::ObjectCreatedPutTagging => "s3:ObjectCreated:PutTagging",
            EventName::ObjectCreatedDeleteTagging => "s3:ObjectCreated:DeleteTagging",
            EventName::ObjectCreatedPutRetention => "s3:ObjectCreated:PutRetention",
            EventName::ObjectCreatedPutLegalHold => "s3:ObjectCreated:PutLegalHold",
            EventName::ObjectRemovedAll => "s3:ObjectRemoved:*",
            EventName::ObjectRemovedDelete => "s3:ObjectRemoved:Delete",
            EventName::ObjectRemovedDeleteMarkerCreated => "s3:ObjectRemoved:DeleteMarkerCreated",
            EventName::ObjectRemovedNoOP => "s3:ObjectRemoved:NoOP",
            EventName::ObjectRemovedDeleteAllVersions => "s3:ObjectRemoved:DeleteAllVersions",
            EventName::LifecycleDelMarkerExpirationDelete => {
                "s3:LifecycleDelMarkerExpiration:Delete"
            }
            EventName::ObjectReplicationAll => "s3:Replication:*",
            EventName::ObjectReplicationFailed => "s3:Replication:OperationFailedReplication",
            EventName::ObjectReplicationComplete => "s3:Replication:OperationCompletedReplication",
            EventName::ObjectReplicationNotTracked => "s3:Replication:OperationNotTracked",
            EventName::ObjectReplicationMissedThreshold => {
                "s3:Replication:OperationMissedThreshold"
            }
            EventName::ObjectReplicationReplicatedAfterThreshold => {
                "s3:Replication:OperationReplicatedAfterThreshold"
            }
            EventName::ObjectRestoreAll => "s3:ObjectRestore:*",
            EventName::ObjectRestorePost => "s3:ObjectRestore:Post",
            EventName::ObjectRestoreCompleted => "s3:ObjectRestore:Completed",
            EventName::ObjectTransitionAll => "s3:ObjectTransition:*",
            EventName::ObjectTransitionFailed => "s3:ObjectTransition:Failed",
            EventName::ObjectTransitionComplete => "s3:ObjectTransition:Complete",
            EventName::ScannerManyVersions => "s3:Scanner:ManyVersions",
            EventName::ScannerLargeVersions => "s3:Scanner:LargeVersions",
            EventName::ScannerBigPrefix => "s3:Scanner:BigPrefix",
            // Go 的 String() 对 ObjectScannerAll 和 Everything 返回 ""
            EventName::ObjectScannerAll => "s3:Scanner:*", // 遵循 Go Expand 中的模式
            EventName::Everything => "",                   // Go String() 对未处理的返回 ""
        }
    }

    /// 返回缩写事件类型的扩展值。
    pub fn expand(&self) -> Vec<Self> {
        match self {
            EventName::ObjectAccessedAll => vec![
                EventName::ObjectAccessedGet,
                EventName::ObjectAccessedHead,
                EventName::ObjectAccessedGetRetention,
                EventName::ObjectAccessedGetLegalHold,
                EventName::ObjectAccessedAttributes,
            ],
            EventName::ObjectCreatedAll => vec![
                EventName::ObjectCreatedCompleteMultipartUpload,
                EventName::ObjectCreatedCopy,
                EventName::ObjectCreatedPost,
                EventName::ObjectCreatedPut,
                EventName::ObjectCreatedPutRetention,
                EventName::ObjectCreatedPutLegalHold,
                EventName::ObjectCreatedPutTagging,
                EventName::ObjectCreatedDeleteTagging,
            ],
            EventName::ObjectRemovedAll => vec![
                EventName::ObjectRemovedDelete,
                EventName::ObjectRemovedDeleteMarkerCreated,
                EventName::ObjectRemovedNoOP,
                EventName::ObjectRemovedDeleteAllVersions,
            ],
            EventName::ObjectReplicationAll => vec![
                EventName::ObjectReplicationFailed,
                EventName::ObjectReplicationComplete,
                EventName::ObjectReplicationNotTracked,
                EventName::ObjectReplicationMissedThreshold,
                EventName::ObjectReplicationReplicatedAfterThreshold,
            ],
            EventName::ObjectRestoreAll => vec![
                EventName::ObjectRestorePost,
                EventName::ObjectRestoreCompleted,
            ],
            EventName::ObjectTransitionAll => vec![
                EventName::ObjectTransitionFailed,
                EventName::ObjectTransitionComplete,
            ],
            EventName::ObjectScannerAll => vec![
                // 新增
                EventName::ScannerManyVersions,
                EventName::ScannerLargeVersions,
                EventName::ScannerBigPrefix,
            ],
            EventName::Everything => {
                // 新增
                SINGLE_EVENT_NAMES_IN_ORDER.to_vec()
            }
            // 单一类型直接返回自身
            _ => vec![*self],
        }
    }

    /// 返回类型的掩码。
    /// 复合 "All" 类型会被展开。
    pub fn mask(&self) -> u64 {
        let value = *self as u32;
        if value > 0 && value <= LAST_SINGLE_TYPE_VALUE {
            // 是单一类型
            1u64 << (value - 1)
        } else {
            // 是复合类型
            let mut mask = 0u64;
            for n in self.expand() {
                mask |= n.mask(); // 递归调用 mask
            }
            mask
        }
    }
}

impl fmt::Display for EventName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// 根据字符串转换为 `EventName`
impl From<&str> for EventName {
    fn from(event_str: &str) -> Self {
        EventName::parse(event_str).unwrap_or_else(|e| panic!("{}", e))
    }
}

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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub user_agent: String,
}

/// Represents a storage event
#[derive(Debug, Clone, Serialize, Deserialize)]
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
        user_metadata.insert(
            "x-amz-storage-storage-options".to_string(),
            "value".to_string(),
        );
        user_metadata.insert("x-amz-meta-".to_string(), "value".to_string());
        user_metadata.insert("x-rustfs-meta-".to_string(), "rustfs-value".to_string());
        user_metadata.insert("x-request-id".to_string(), "request-id-123".to_string());
        user_metadata.insert("x-bucket".to_string(), "bucket".to_string());
        user_metadata.insert("x-object".to_string(), "object".to_string());
        user_metadata.insert(
            "x-rustfs-origin-endpoint".to_string(),
            "http://127.0.0.1".to_string(),
        );
        user_metadata.insert("x-rustfs-user-metadata".to_string(), "metadata".to_string());
        user_metadata.insert(
            "x-rustfs-deployment-id".to_string(),
            "deployment-id-123".to_string(),
        );
        user_metadata.insert(
            "x-rustfs-origin-endpoint-code".to_string(),
            "http://127.0.0.1".to_string(),
        );
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
                    arn: format!("arn:rustfs:s3:::{}", bucket),
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
    /// 返回事件掩码
    pub fn mask(&self) -> u64 {
        self.event_name.mask()
    }
}

/// Represents a log of events for sending to targets
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventLog {
    /// The event name
    pub event_name: EventName,
    /// The object key
    pub key: String,
    /// The list of events
    pub records: Vec<Event>,
}
