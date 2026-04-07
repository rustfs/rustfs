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

use std::fmt;

/// Error returned when parsing event name string fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseEventNameError(String);

impl fmt::Display for ParseEventNameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Invalid event name:{}", self.0)
    }
}

impl std::error::Error for ParseEventNameError {}

/// Represents the type of event that occurs on the object.
/// Based on AWS S3 event type and includes RustFS extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum EventName {
    // Single event type (values are sequential for compatible mask logic)
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
    ObjectTaggingPut = 12,
    ObjectTaggingDelete = 13,
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
    ScannerManyVersions = 29,                // ObjectManyVersions corresponding to Go
    ScannerLargeVersions = 30,               // ObjectLargeVersions corresponding to Go
    ScannerBigPrefix = 31,                   // PrefixManyFolders corresponding to Go
    LifecycleDelMarkerExpirationDelete = 32, // ILMDelMarkerExpirationDelete corresponding to Go
    ObjectAclPut = 33,
    LifecycleExpirationDelete = 34,
    LifecycleExpirationDeleteMarkerCreated = 35,
    LifecycleTransition = 36,
    IntelligentTiering = 37,

    // Compound "All" event type (no sequential value for mask)
    ObjectAccessedAll,
    ObjectCreatedAll,
    ObjectRemovedAll,
    ObjectReplicationAll,
    ObjectRestoreAll,
    ObjectTaggingAll,
    LifecycleExpirationAll,
    ObjectTransitionAll,
    ObjectScannerAll, // New, from Go
    #[default]
    Everything, // New, from Go

    // Internal events for metrics (not exposed to S3 notifications)
    ObjectRemovedAbortMultipartUpload,
    ObjectCreatedCreateMultipartUpload,
    ObjectRemovedDeleteObjects,
}

// Single event type sequential array for Everything.expand()
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
    EventName::ObjectTaggingPut,
    EventName::ObjectTaggingDelete,
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

const SINGLE_AWS_AND_EXTENSION_EVENTS_AFTER_COMPAT: [EventName; 5] = [
    EventName::ObjectAclPut,
    EventName::LifecycleExpirationDelete,
    EventName::LifecycleExpirationDeleteMarkerCreated,
    EventName::LifecycleTransition,
    EventName::IntelligentTiering,
];

const LAST_SINGLE_TYPE_VALUE: u32 = EventName::IntelligentTiering as u32;

impl EventName {
    /// The parsed string is EventName.
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
            "s3:ObjectCreated:CompleteMultipartUpload" => Ok(EventName::ObjectCreatedCompleteMultipartUpload),
            "s3:ObjectCreated:Copy" => Ok(EventName::ObjectCreatedCopy),
            "s3:ObjectCreated:Post" => Ok(EventName::ObjectCreatedPost),
            "s3:ObjectCreated:Put" => Ok(EventName::ObjectCreatedPut),
            "s3:ObjectCreated:PutRetention" => Ok(EventName::ObjectCreatedPutRetention),
            "s3:ObjectCreated:PutLegalHold" => Ok(EventName::ObjectCreatedPutLegalHold),
            "s3:ObjectCreated:PutTagging" => Ok(EventName::ObjectTaggingPut),
            "s3:ObjectCreated:DeleteTagging" => Ok(EventName::ObjectTaggingDelete),
            "s3:ObjectTagging:*" => Ok(EventName::ObjectTaggingAll),
            "s3:ObjectTagging:Put" => Ok(EventName::ObjectTaggingPut),
            "s3:ObjectTagging:Delete" => Ok(EventName::ObjectTaggingDelete),
            "s3:ObjectAcl:Put" => Ok(EventName::ObjectAclPut),
            "s3:ObjectRemoved:*" => Ok(EventName::ObjectRemovedAll),
            "s3:ObjectRemoved:Delete" => Ok(EventName::ObjectRemovedDelete),
            "s3:ObjectRemoved:DeleteMarkerCreated" => Ok(EventName::ObjectRemovedDeleteMarkerCreated),
            "s3:ObjectRemoved:NoOP" => Ok(EventName::ObjectRemovedNoOP),
            "s3:ObjectRemoved:DeleteAllVersions" => Ok(EventName::ObjectRemovedDeleteAllVersions),
            "s3:LifecycleDelMarkerExpiration:Delete" => Ok(EventName::LifecycleDelMarkerExpirationDelete),
            "s3:LifecycleExpiration:*" => Ok(EventName::LifecycleExpirationAll),
            "s3:LifecycleExpiration:Delete" => Ok(EventName::LifecycleExpirationDelete),
            "s3:LifecycleExpiration:DeleteMarkerCreated" => Ok(EventName::LifecycleExpirationDeleteMarkerCreated),
            "s3:Replication:*" => Ok(EventName::ObjectReplicationAll),
            "s3:Replication:OperationFailedReplication" => Ok(EventName::ObjectReplicationFailed),
            "s3:Replication:OperationCompletedReplication" => Ok(EventName::ObjectReplicationComplete),
            "s3:Replication:OperationMissedThreshold" => Ok(EventName::ObjectReplicationMissedThreshold),
            "s3:Replication:OperationReplicatedAfterThreshold" => Ok(EventName::ObjectReplicationReplicatedAfterThreshold),
            "s3:Replication:OperationNotTracked" => Ok(EventName::ObjectReplicationNotTracked),
            "s3:ObjectRestore:*" => Ok(EventName::ObjectRestoreAll),
            "s3:ObjectRestore:Post" => Ok(EventName::ObjectRestorePost),
            "s3:ObjectRestore:Completed" => Ok(EventName::ObjectRestoreCompleted),
            "s3:ObjectTransition:Failed" => Ok(EventName::ObjectTransitionFailed),
            "s3:ObjectTransition:Complete" => Ok(EventName::ObjectTransitionComplete),
            "s3:ObjectTransition:*" => Ok(EventName::ObjectTransitionAll),
            "s3:LifecycleTransition" => Ok(EventName::LifecycleTransition),
            "s3:IntelligentTiering" => Ok(EventName::IntelligentTiering),
            "s3:Scanner:ManyVersions" => Ok(EventName::ScannerManyVersions),
            "s3:Scanner:LargeVersions" => Ok(EventName::ScannerLargeVersions),
            "s3:Scanner:BigPrefix" => Ok(EventName::ScannerBigPrefix),
            // ObjectScannerAll and Everything cannot be parsed from strings, because the Go version also does not define their string representation.
            _ => Err(ParseEventNameError(s.to_string())),
        }
    }

    /// Returns a string representation of the event type.
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
            EventName::ObjectCreatedCompleteMultipartUpload => "s3:ObjectCreated:CompleteMultipartUpload",
            EventName::ObjectCreatedCopy => "s3:ObjectCreated:Copy",
            EventName::ObjectCreatedPost => "s3:ObjectCreated:Post",
            EventName::ObjectCreatedPut => "s3:ObjectCreated:Put",
            EventName::ObjectCreatedPutRetention => "s3:ObjectCreated:PutRetention",
            EventName::ObjectCreatedPutLegalHold => "s3:ObjectCreated:PutLegalHold",
            EventName::ObjectTaggingAll => "s3:ObjectTagging:*",
            EventName::ObjectTaggingPut => "s3:ObjectTagging:Put",
            EventName::ObjectTaggingDelete => "s3:ObjectTagging:Delete",
            EventName::ObjectAclPut => "s3:ObjectAcl:Put",
            EventName::ObjectRemovedAll => "s3:ObjectRemoved:*",
            EventName::ObjectRemovedDelete => "s3:ObjectRemoved:Delete",
            EventName::ObjectRemovedDeleteMarkerCreated => "s3:ObjectRemoved:DeleteMarkerCreated",
            EventName::ObjectRemovedNoOP => "s3:ObjectRemoved:NoOP",
            EventName::ObjectRemovedDeleteAllVersions => "s3:ObjectRemoved:DeleteAllVersions",
            EventName::LifecycleDelMarkerExpirationDelete => "s3:LifecycleDelMarkerExpiration:Delete",
            EventName::LifecycleExpirationAll => "s3:LifecycleExpiration:*",
            EventName::LifecycleExpirationDelete => "s3:LifecycleExpiration:Delete",
            EventName::LifecycleExpirationDeleteMarkerCreated => "s3:LifecycleExpiration:DeleteMarkerCreated",
            EventName::ObjectReplicationAll => "s3:Replication:*",
            EventName::ObjectReplicationFailed => "s3:Replication:OperationFailedReplication",
            EventName::ObjectReplicationComplete => "s3:Replication:OperationCompletedReplication",
            EventName::ObjectReplicationNotTracked => "s3:Replication:OperationNotTracked",
            EventName::ObjectReplicationMissedThreshold => "s3:Replication:OperationMissedThreshold",
            EventName::ObjectReplicationReplicatedAfterThreshold => "s3:Replication:OperationReplicatedAfterThreshold",
            EventName::ObjectRestoreAll => "s3:ObjectRestore:*",
            EventName::ObjectRestorePost => "s3:ObjectRestore:Post",
            EventName::ObjectRestoreCompleted => "s3:ObjectRestore:Completed",
            EventName::ObjectTransitionAll => "s3:ObjectTransition:*",
            EventName::ObjectTransitionFailed => "s3:ObjectTransition:Failed",
            EventName::ObjectTransitionComplete => "s3:ObjectTransition:Complete",
            EventName::LifecycleTransition => "s3:LifecycleTransition",
            EventName::IntelligentTiering => "s3:IntelligentTiering",
            EventName::ScannerManyVersions => "s3:Scanner:ManyVersions",
            EventName::ScannerLargeVersions => "s3:Scanner:LargeVersions",
            EventName::ScannerBigPrefix => "s3:Scanner:BigPrefix",
            // Go's String() returns "" for ObjectScannerAll and Everything
            EventName::ObjectScannerAll => "s3:Scanner:*", // Follow the pattern in Go Expand
            EventName::Everything => "",                   // Go String() returns "" to unprocessed
            EventName::ObjectRemovedAbortMultipartUpload => "s3:ObjectRemoved:AbortMultipartUpload",
            EventName::ObjectCreatedCreateMultipartUpload => "s3:ObjectCreated:CreateMultipartUpload",
            EventName::ObjectRemovedDeleteObjects => "s3:ObjectRemoved:DeleteObjects",
        }
    }

    /// Returns the extended value of the abbreviation event type.
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
            ],
            EventName::ObjectTaggingAll => vec![EventName::ObjectTaggingPut, EventName::ObjectTaggingDelete],
            EventName::ObjectRemovedAll => vec![EventName::ObjectRemovedDelete, EventName::ObjectRemovedDeleteMarkerCreated],
            EventName::ObjectReplicationAll => vec![
                EventName::ObjectReplicationFailed,
                EventName::ObjectReplicationComplete,
                EventName::ObjectReplicationNotTracked,
                EventName::ObjectReplicationMissedThreshold,
                EventName::ObjectReplicationReplicatedAfterThreshold,
            ],
            EventName::ObjectRestoreAll => vec![EventName::ObjectRestorePost, EventName::ObjectRestoreCompleted],
            EventName::LifecycleExpirationAll => vec![
                EventName::LifecycleExpirationDelete,
                EventName::LifecycleExpirationDeleteMarkerCreated,
            ],
            EventName::ObjectTransitionAll => vec![
                EventName::ObjectTransitionFailed,
                EventName::ObjectTransitionComplete,
                EventName::LifecycleTransition,
            ],
            EventName::ObjectScannerAll => vec![
                // New
                EventName::ScannerManyVersions,
                EventName::ScannerLargeVersions,
                EventName::ScannerBigPrefix,
            ],
            EventName::Everything => {
                // New
                let mut all = SINGLE_EVENT_NAMES_IN_ORDER.to_vec();
                all.extend(SINGLE_AWS_AND_EXTENSION_EVENTS_AFTER_COMPAT);
                all
            }
            // A single type returns to itself directly
            _ => vec![*self],
        }
    }

    /// Returns the mask of type.
    /// The compound "All" type will be expanded.
    pub fn mask(&self) -> u64 {
        let value = *self as u32;
        if value > 0 && value <= LAST_SINGLE_TYPE_VALUE {
            // It's a single type
            1u64 << (value - 1)
        } else {
            // It's a compound type
            let mut mask = 0u64;
            for n in self.expand() {
                mask |= n.mask(); // Recursively call mask
            }
            mask
        }
    }

    /// Returns the corresponding S3Operation if the event triggers a notification event.
    pub fn to_s3_operation(&self) -> Option<S3Operation> {
        match self {
            EventName::BucketCreated => Some(S3Operation::CreateBucket),
            EventName::BucketRemoved => Some(S3Operation::DeleteBucket),
            EventName::ObjectAccessedGet => Some(S3Operation::GetObject),
            EventName::ObjectAccessedGetRetention => Some(S3Operation::GetObjectRetention),
            EventName::ObjectAccessedGetLegalHold => Some(S3Operation::GetObjectLegalHold),
            EventName::ObjectAccessedHead => Some(S3Operation::HeadObject),
            EventName::ObjectAccessedAttributes => Some(S3Operation::GetObjectAttributes),
            EventName::ObjectCreatedCompleteMultipartUpload => Some(S3Operation::CompleteMultipartUpload),
            EventName::ObjectCreatedCopy => Some(S3Operation::CopyObject),
            EventName::ObjectCreatedPost => Some(S3Operation::PutObject),
            EventName::ObjectCreatedPut => Some(S3Operation::PutObject),
            EventName::ObjectCreatedPutRetention => Some(S3Operation::PutObjectRetention),
            EventName::ObjectCreatedPutLegalHold => Some(S3Operation::PutObjectLegalHold),
            EventName::ObjectTaggingPut => Some(S3Operation::PutObjectTagging),
            EventName::ObjectTaggingDelete => Some(S3Operation::DeleteObjectTagging),
            EventName::ObjectAclPut => Some(S3Operation::PutObjectAcl),
            EventName::ObjectRemovedDelete => Some(S3Operation::DeleteObject),
            EventName::ObjectRemovedDeleteMarkerCreated => Some(S3Operation::DeleteObject),
            EventName::ObjectRemovedDeleteAllVersions => Some(S3Operation::DeleteObject),
            EventName::ObjectRestorePost => Some(S3Operation::RestoreObject),
            EventName::ObjectRemovedAbortMultipartUpload => Some(S3Operation::AbortMultipartUpload),
            EventName::ObjectCreatedCreateMultipartUpload => Some(S3Operation::CreateMultipartUpload),
            EventName::ObjectRemovedDeleteObjects => Some(S3Operation::DeleteObjects),
            _ => None,
        }
    }
}

impl fmt::Display for EventName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Convert to `EventName` according to string
impl From<&str> for EventName {
    fn from(event_str: &str) -> Self {
        EventName::parse(event_str).unwrap_or_else(|e| panic!("{}", e))
    }
}

impl serde::ser::Serialize for EventName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> serde::de::Deserialize<'de> for EventName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let s = Self::parse(&s).map_err(serde::de::Error::custom)?;
        Ok(s)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum S3Operation {
    AbortMultipartUpload,
    CompleteMultipartUpload,
    CopyObject,
    CreateBucket,
    CreateMultipartUpload,
    DeleteBucket,
    DeleteBucketCors,
    DeleteBucketEncryption,
    DeleteBucketLifecycle,
    DeleteBucketPolicy,
    DeleteBucketReplication,
    DeleteBucketTagging,
    DeleteObject,
    DeleteObjectTagging,
    DeleteObjects,
    DeletePublicAccessBlock,
    GetBucketAcl,
    GetBucketCors,
    GetBucketEncryption,
    GetBucketLifecycleConfiguration,
    GetBucketLocation,
    GetBucketLogging,
    GetBucketNotificationConfiguration,
    GetBucketPolicy,
    GetBucketPolicyStatus,
    GetBucketReplication,
    GetBucketTagging,
    GetBucketVersioning,
    GetObject,
    GetObjectAcl,
    GetObjectAttributes,
    GetObjectLegalHold,
    GetObjectLockConfiguration,
    GetObjectRetention,
    GetObjectTagging,
    GetObjectTorrent,
    GetPublicAccessBlock,
    HeadBucket,
    HeadObject,
    ListBuckets,
    ListMultipartUploads,
    ListObjectVersions,
    ListObjects,
    ListObjectsV2,
    ListParts,
    PutBucketAcl,
    PutBucketCors,
    PutBucketEncryption,
    PutBucketLifecycleConfiguration,
    PutBucketLogging,
    PutBucketNotificationConfiguration,
    PutBucketPolicy,
    PutBucketReplication,
    PutBucketTagging,
    PutBucketVersioning,
    PutObject,
    PutObjectAcl,
    PutObjectLegalHold,
    PutObjectLockConfiguration,
    PutObjectRetention,
    PutObjectTagging,
    PutPublicAccessBlock,
    RestoreObject,
    SelectObjectContent,
    UploadPart,
    UploadPartCopy,
}

impl S3Operation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AbortMultipartUpload => "s3:AbortMultipartUpload",
            Self::CompleteMultipartUpload => "s3:CompleteMultipartUpload",
            Self::CopyObject => "s3:CopyObject",
            Self::CreateBucket => "s3:CreateBucket",
            Self::CreateMultipartUpload => "s3:CreateMultipartUpload",
            Self::DeleteBucket => "s3:DeleteBucket",
            Self::DeleteBucketCors => "s3:DeleteBucketCors",
            Self::DeleteBucketEncryption => "s3:DeleteBucketEncryption",
            Self::DeleteBucketLifecycle => "s3:DeleteBucketLifecycle",
            Self::DeleteBucketPolicy => "s3:DeleteBucketPolicy",
            Self::DeleteBucketReplication => "s3:DeleteBucketReplication",
            Self::DeleteBucketTagging => "s3:DeleteBucketTagging",
            Self::DeleteObject => "s3:DeleteObject",
            Self::DeleteObjectTagging => "s3:DeleteObjectTagging",
            Self::DeleteObjects => "s3:DeleteObjects",
            Self::DeletePublicAccessBlock => "s3:DeletePublicAccessBlock",
            Self::GetBucketAcl => "s3:GetBucketAcl",
            Self::GetBucketCors => "s3:GetBucketCors",
            Self::GetBucketEncryption => "s3:GetBucketEncryption",
            Self::GetBucketLifecycleConfiguration => "s3:GetBucketLifecycleConfiguration",
            Self::GetBucketLocation => "s3:GetBucketLocation",
            Self::GetBucketLogging => "s3:GetBucketLogging",
            Self::GetBucketNotificationConfiguration => "s3:GetBucketNotificationConfiguration",
            Self::GetBucketPolicy => "s3:GetBucketPolicy",
            Self::GetBucketPolicyStatus => "s3:GetBucketPolicyStatus",
            Self::GetBucketReplication => "s3:GetBucketReplication",
            Self::GetBucketTagging => "s3:GetBucketTagging",
            Self::GetBucketVersioning => "s3:GetBucketVersioning",
            Self::GetObject => "s3:GetObject",
            Self::GetObjectAcl => "s3:GetObjectAcl",
            Self::GetObjectAttributes => "s3:GetObjectAttributes",
            Self::GetObjectLegalHold => "s3:GetObjectLegalHold",
            Self::GetObjectLockConfiguration => "s3:GetObjectLockConfiguration",
            Self::GetObjectRetention => "s3:GetObjectRetention",
            Self::GetObjectTagging => "s3:GetObjectTagging",
            Self::GetObjectTorrent => "s3:GetObjectTorrent",
            Self::GetPublicAccessBlock => "s3:GetPublicAccessBlock",
            Self::HeadBucket => "s3:HeadBucket",
            Self::HeadObject => "s3:HeadObject",
            Self::ListBuckets => "s3:ListBuckets",
            Self::ListMultipartUploads => "s3:ListMultipartUploads",
            Self::ListObjectVersions => "s3:ListObjectVersions",
            Self::ListObjects => "s3:ListObjects",
            Self::ListObjectsV2 => "s3:ListObjectsV2",
            Self::ListParts => "s3:ListParts",
            Self::PutBucketAcl => "s3:PutBucketAcl",
            Self::PutBucketCors => "s3:PutBucketCors",
            Self::PutBucketEncryption => "s3:PutBucketEncryption",
            Self::PutBucketLifecycleConfiguration => "s3:PutBucketLifecycleConfiguration",
            Self::PutBucketLogging => "s3:PutBucketLogging",
            Self::PutBucketNotificationConfiguration => "s3:PutBucketNotificationConfiguration",
            Self::PutBucketPolicy => "s3:PutBucketPolicy",
            Self::PutBucketReplication => "s3:PutBucketReplication",
            Self::PutBucketTagging => "s3:PutBucketTagging",
            Self::PutBucketVersioning => "s3:PutBucketVersioning",
            Self::PutObject => "s3:PutObject",
            Self::PutObjectAcl => "s3:PutObjectAcl",
            Self::PutObjectLegalHold => "s3:PutObjectLegalHold",
            Self::PutObjectLockConfiguration => "s3:PutObjectLockConfiguration",
            Self::PutObjectRetention => "s3:PutObjectRetention",
            Self::PutObjectTagging => "s3:PutObjectTagging",
            Self::PutPublicAccessBlock => "s3:PutPublicAccessBlock",
            Self::RestoreObject => "s3:RestoreObject",
            Self::SelectObjectContent => "s3:SelectObjectContent",
            Self::UploadPart => "s3:UploadPart",
            Self::UploadPartCopy => "s3:UploadPartCopy",
        }
    }

    /// Returns the corresponding EventName if the operation triggers a notification event.
    pub fn to_event_name(self) -> Option<EventName> {
        match self {
            Self::CompleteMultipartUpload => Some(EventName::ObjectCreatedCompleteMultipartUpload),
            Self::CopyObject => Some(EventName::ObjectCreatedCopy),
            Self::CreateBucket => Some(EventName::BucketCreated),
            Self::DeleteBucket => Some(EventName::BucketRemoved),
            Self::DeleteObject => Some(EventName::ObjectRemovedDelete),
            Self::DeleteObjects => Some(EventName::ObjectRemovedDeleteObjects),
            Self::DeleteObjectTagging => Some(EventName::ObjectTaggingDelete),
            Self::GetObject => Some(EventName::ObjectAccessedGet),
            Self::GetObjectAttributes => Some(EventName::ObjectAccessedAttributes),
            Self::GetObjectLegalHold => Some(EventName::ObjectAccessedGetLegalHold),
            Self::GetObjectRetention => Some(EventName::ObjectAccessedGetRetention),
            Self::HeadObject => Some(EventName::ObjectAccessedHead),
            Self::PutObject => Some(EventName::ObjectCreatedPut),
            Self::PutObjectAcl => Some(EventName::ObjectAclPut),
            Self::PutObjectLegalHold => Some(EventName::ObjectCreatedPutLegalHold),
            Self::PutObjectRetention => Some(EventName::ObjectCreatedPutRetention),
            Self::PutObjectTagging => Some(EventName::ObjectTaggingPut),
            Self::RestoreObject => Some(EventName::ObjectRestorePost),
            Self::SelectObjectContent => Some(EventName::ObjectAccessedGet),
            Self::AbortMultipartUpload => Some(EventName::ObjectRemovedAbortMultipartUpload),
            Self::CreateMultipartUpload => Some(EventName::ObjectCreatedCreateMultipartUpload),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // test serialization
    #[test]
    fn test_event_name_serialization_and_deserialization() {
        struct TestCase {
            event: EventName,
            serialized_str: &'static str,
        }

        let test_cases = vec![
            TestCase {
                event: EventName::BucketCreated,
                serialized_str: "\"s3:BucketCreated:*\"",
            },
            TestCase {
                event: EventName::ObjectCreatedAll,
                serialized_str: "\"s3:ObjectCreated:*\"",
            },
            TestCase {
                event: EventName::ObjectCreatedPut,
                serialized_str: "\"s3:ObjectCreated:Put\"",
            },
            TestCase {
                event: EventName::ObjectTaggingPut,
                serialized_str: "\"s3:ObjectTagging:Put\"",
            },
        ];

        for case in &test_cases {
            let serialized = serde_json::to_string(&case.event);
            assert!(serialized.is_ok(), "Serialization failed for `{}`", case.serialized_str);
            assert_eq!(serialized.unwrap(), case.serialized_str);

            let deserialized = serde_json::from_str::<EventName>(case.serialized_str);
            assert!(deserialized.is_ok(), "Deserialization failed for `{}`", case.serialized_str);
            assert_eq!(deserialized.unwrap(), case.event);
        }
    }

    #[test]
    fn test_invalid_event_name_deserialization() {
        let invalid_str = "\"s3:InvalidEvent:Test\"";
        let deserialized = serde_json::from_str::<EventName>(invalid_str);
        assert!(deserialized.is_err(), "Deserialization should fail for invalid event name");

        // Serializing EventName::Everything produces an empty string, but deserializing an empty string should fail.
        let event_name = EventName::Everything;
        let serialized_str = "\"\"";
        let serialized = serde_json::to_string(&event_name);
        assert!(serialized.is_ok(), "Serialization failed for `{serialized_str}`");
        assert_eq!(serialized.unwrap(), serialized_str);

        let deserialized = serde_json::from_str::<EventName>(serialized_str);
        assert!(deserialized.is_err(), "Deserialization should fail for empty string");
    }

    #[test]
    fn test_s3_operation_to_event_name() {
        assert_eq!(S3Operation::PutObject.to_event_name(), Some(EventName::ObjectCreatedPut));
        assert_eq!(S3Operation::PutObjectAcl.to_event_name(), Some(EventName::ObjectAclPut));
        assert_eq!(S3Operation::PutObjectTagging.to_event_name(), Some(EventName::ObjectTaggingPut));
        assert_eq!(S3Operation::DeleteObjectTagging.to_event_name(), Some(EventName::ObjectTaggingDelete));
        assert_eq!(S3Operation::GetObject.to_event_name(), Some(EventName::ObjectAccessedGet));
        assert_eq!(S3Operation::ListBuckets.to_event_name(), None);
        assert_eq!(S3Operation::RestoreObject.to_event_name(), Some(EventName::ObjectRestorePost));
        assert_eq!(S3Operation::SelectObjectContent.to_event_name(), Some(EventName::ObjectAccessedGet));
        assert_eq!(
            S3Operation::AbortMultipartUpload.to_event_name(),
            Some(EventName::ObjectRemovedAbortMultipartUpload)
        );
    }

    #[test]
    fn test_event_name_to_s3_operation() {
        assert_eq!(EventName::ObjectCreatedPut.to_s3_operation(), Some(S3Operation::PutObject));
        assert_eq!(EventName::ObjectAclPut.to_s3_operation(), Some(S3Operation::PutObjectAcl));
        assert_eq!(EventName::ObjectTaggingPut.to_s3_operation(), Some(S3Operation::PutObjectTagging));
        assert_eq!(EventName::ObjectTaggingDelete.to_s3_operation(), Some(S3Operation::DeleteObjectTagging));
        assert_eq!(EventName::ObjectAccessedGet.to_s3_operation(), Some(S3Operation::GetObject));
        assert_eq!(EventName::BucketCreated.to_s3_operation(), Some(S3Operation::CreateBucket));
        assert_eq!(EventName::Everything.to_s3_operation(), None);
        assert_eq!(EventName::ObjectRestorePost.to_s3_operation(), Some(S3Operation::RestoreObject));
        assert_eq!(EventName::ObjectCreatedPost.to_s3_operation(), Some(S3Operation::PutObject));
        assert_eq!(
            EventName::ObjectRemovedAbortMultipartUpload.to_s3_operation(),
            Some(S3Operation::AbortMultipartUpload)
        );
    }

    #[test]
    fn test_event_name_aliases_parse_to_aws_compatible_variants() {
        assert_eq!(EventName::parse("s3:ObjectCreated:PutTagging").unwrap(), EventName::ObjectTaggingPut);
        assert_eq!(
            EventName::parse("s3:ObjectCreated:DeleteTagging").unwrap(),
            EventName::ObjectTaggingDelete
        );
        assert_eq!(
            EventName::parse("s3:ObjectTransition:Complete").unwrap(),
            EventName::ObjectTransitionComplete
        );
        assert_eq!(
            EventName::parse("s3:LifecycleDelMarkerExpiration:Delete").unwrap(),
            EventName::LifecycleDelMarkerExpirationDelete
        );
    }

    #[test]
    fn test_object_created_all_expansion_matches_aws_scope() {
        let expanded = EventName::ObjectCreatedAll.expand();
        assert_eq!(
            expanded,
            vec![
                EventName::ObjectCreatedCompleteMultipartUpload,
                EventName::ObjectCreatedCopy,
                EventName::ObjectCreatedPost,
                EventName::ObjectCreatedPut,
            ]
        );
    }
}
