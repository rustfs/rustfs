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

use serde::{Deserialize, Serialize};
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
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum EventName {
    // Single event type (values are 1-32 for compatible mask logic)
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
    ScannerManyVersions = 29,                // ObjectManyVersions corresponding to Go
    ScannerLargeVersions = 30,               // ObjectLargeVersions corresponding to Go
    ScannerBigPrefix = 31,                   // PrefixManyFolders corresponding to Go
    LifecycleDelMarkerExpirationDelete = 32, // ILMDelMarkerExpirationDelete corresponding to Go

    // Compound "All" event type (no sequential value for mask)
    ObjectAccessedAll,
    ObjectCreatedAll,
    ObjectRemovedAll,
    ObjectReplicationAll,
    ObjectRestoreAll,
    ObjectTransitionAll,
    ObjectScannerAll, // New, from Go
    Everything,       // New, from Go
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
            "s3:ObjectCreated:PutTagging" => Ok(EventName::ObjectCreatedPutTagging),
            "s3:ObjectCreated:DeleteTagging" => Ok(EventName::ObjectCreatedDeleteTagging),
            "s3:ObjectRemoved:*" => Ok(EventName::ObjectRemovedAll),
            "s3:ObjectRemoved:Delete" => Ok(EventName::ObjectRemovedDelete),
            "s3:ObjectRemoved:DeleteMarkerCreated" => Ok(EventName::ObjectRemovedDeleteMarkerCreated),
            "s3:ObjectRemoved:NoOP" => Ok(EventName::ObjectRemovedNoOP),
            "s3:ObjectRemoved:DeleteAllVersions" => Ok(EventName::ObjectRemovedDeleteAllVersions),
            "s3:LifecycleDelMarkerExpiration:Delete" => Ok(EventName::LifecycleDelMarkerExpirationDelete),
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
            EventName::ObjectCreatedPutTagging => "s3:ObjectCreated:PutTagging",
            EventName::ObjectCreatedDeleteTagging => "s3:ObjectCreated:DeleteTagging",
            EventName::ObjectCreatedPutRetention => "s3:ObjectCreated:PutRetention",
            EventName::ObjectCreatedPutLegalHold => "s3:ObjectCreated:PutLegalHold",
            EventName::ObjectRemovedAll => "s3:ObjectRemoved:*",
            EventName::ObjectRemovedDelete => "s3:ObjectRemoved:Delete",
            EventName::ObjectRemovedDeleteMarkerCreated => "s3:ObjectRemoved:DeleteMarkerCreated",
            EventName::ObjectRemovedNoOP => "s3:ObjectRemoved:NoOP",
            EventName::ObjectRemovedDeleteAllVersions => "s3:ObjectRemoved:DeleteAllVersions",
            EventName::LifecycleDelMarkerExpirationDelete => "s3:LifecycleDelMarkerExpiration:Delete",
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
            EventName::ScannerManyVersions => "s3:Scanner:ManyVersions",
            EventName::ScannerLargeVersions => "s3:Scanner:LargeVersions",
            EventName::ScannerBigPrefix => "s3:Scanner:BigPrefix",
            // Go's String() returns "" for ObjectScannerAll and Everything
            EventName::ObjectScannerAll => "s3:Scanner:*", // Follow the pattern in Go Expand
            EventName::Everything => "",                   // Go String() returns "" to unprocessed
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
            EventName::ObjectRestoreAll => vec![EventName::ObjectRestorePost, EventName::ObjectRestoreCompleted],
            EventName::ObjectTransitionAll => vec![EventName::ObjectTransitionFailed, EventName::ObjectTransitionComplete],
            EventName::ObjectScannerAll => vec![
                // New
                EventName::ScannerManyVersions,
                EventName::ScannerLargeVersions,
                EventName::ScannerBigPrefix,
            ],
            EventName::Everything => {
                // New
                SINGLE_EVENT_NAMES_IN_ORDER.to_vec()
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
