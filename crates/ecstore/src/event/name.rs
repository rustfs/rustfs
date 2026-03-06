#![allow(unused_variables)]
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

#[derive(Default, Clone)]
pub enum EventName {
    ObjectAccessedGet,
    ObjectAccessedGetRetention,
    ObjectAccessedGetLegalHold,
    ObjectAccessedHead,
    ObjectAccessedAttributes,
    ObjectCreatedCompleteMultipartUpload,
    ObjectCreatedCopy,
    ObjectCreatedPost,
    ObjectCreatedPut,
    ObjectCreatedPutRetention,
    ObjectCreatedPutLegalHold,
    ObjectCreatedPutTagging,
    ObjectCreatedDeleteTagging,
    ObjectRemovedDelete,
    ObjectRemovedDeleteMarkerCreated,
    ObjectRemovedDeleteAllVersions,
    ObjectRemovedNoOP,
    BucketCreated,
    BucketRemoved,
    ObjectReplicationFailed,
    ObjectReplicationComplete,
    ObjectReplicationMissedThreshold,
    ObjectReplicationReplicatedAfterThreshold,
    ObjectReplicationNotTracked,
    ObjectRestorePost,
    ObjectRestoreCompleted,
    ObjectTransitionFailed,
    ObjectTransitionComplete,
    ObjectManyVersions,
    ObjectLargeVersions,
    PrefixManyFolders,
    ILMDelMarkerExpirationDelete,
    ObjectSingleTypesEnd,
    ObjectAccessedAll,
    ObjectCreatedAll,
    ObjectRemovedAll,
    ObjectReplicationAll,
    ObjectRestoreAll,
    ObjectTransitionAll,
    ObjectScannerAll,
    #[default]
    Everything,
}

impl EventName {
    fn expand(&self) -> Vec<EventName> {
        match self.clone() {
            EventName::Everything => vec![
                EventName::BucketCreated,
                EventName::BucketRemoved,
                EventName::ObjectAccessedAll,
                EventName::ObjectCreatedAll,
                EventName::ObjectRemovedAll,
                EventName::ObjectManyVersions,
                EventName::ObjectLargeVersions,
                EventName::PrefixManyFolders,
                EventName::ILMDelMarkerExpirationDelete,
                EventName::ObjectReplicationAll,
                EventName::ObjectRestoreAll,
                EventName::ObjectTransitionAll,
            ],
            EventName::ObjectAccessedAll => vec![
                EventName::ObjectAccessedGet,
                EventName::ObjectAccessedGetRetention,
                EventName::ObjectAccessedGetLegalHold,
                EventName::ObjectAccessedHead,
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
            EventName::ObjectSingleTypesEnd | EventName::ObjectScannerAll => vec![self.clone()],
            _ => vec![self.clone()],
        }
    }

    fn mask(&self) -> u64 {
        match self {
            EventName::Everything => u64::MAX,
            EventName::BucketCreated => 1_u64 << 0,
            EventName::BucketRemoved => 1_u64 << 1,
            EventName::ObjectAccessedGet => 1_u64 << 2,
            EventName::ObjectAccessedGetRetention => 1_u64 << 3,
            EventName::ObjectAccessedGetLegalHold => 1_u64 << 4,
            EventName::ObjectAccessedHead => 1_u64 << 5,
            EventName::ObjectAccessedAttributes => 1_u64 << 6,
            EventName::ObjectCreatedCompleteMultipartUpload => 1_u64 << 7,
            EventName::ObjectCreatedCopy => 1_u64 << 8,
            EventName::ObjectCreatedPost => 1_u64 << 9,
            EventName::ObjectCreatedPut => 1_u64 << 10,
            EventName::ObjectCreatedPutRetention => 1_u64 << 11,
            EventName::ObjectCreatedPutLegalHold => 1_u64 << 12,
            EventName::ObjectCreatedPutTagging => 1_u64 << 13,
            EventName::ObjectCreatedDeleteTagging => 1_u64 << 14,
            EventName::ObjectRemovedDelete => 1_u64 << 15,
            EventName::ObjectRemovedDeleteMarkerCreated => 1_u64 << 16,
            EventName::ObjectRemovedDeleteAllVersions => 1_u64 << 17,
            EventName::ObjectRemovedNoOP => 1_u64 << 18,
            EventName::ObjectManyVersions => 1_u64 << 19,
            EventName::ObjectLargeVersions => 1_u64 << 20,
            EventName::PrefixManyFolders => 1_u64 << 21,
            EventName::ILMDelMarkerExpirationDelete => 1_u64 << 22,
            EventName::ObjectReplicationFailed => 1_u64 << 23,
            EventName::ObjectReplicationComplete => 1_u64 << 24,
            EventName::ObjectReplicationMissedThreshold => 1_u64 << 25,
            EventName::ObjectReplicationReplicatedAfterThreshold => 1_u64 << 26,
            EventName::ObjectReplicationNotTracked => 1_u64 << 27,
            EventName::ObjectRestorePost => 1_u64 << 28,
            EventName::ObjectRestoreCompleted => 1_u64 << 29,
            EventName::ObjectRestoreAll => 1_u64 << 30,
            EventName::ObjectTransitionFailed => 1_u64 << 31,
            EventName::ObjectTransitionComplete => 1_u64 << 32,
            EventName::ObjectAccessedAll => {
                EventName::ObjectAccessedGet.mask()
                    | EventName::ObjectAccessedGetRetention.mask()
                    | EventName::ObjectAccessedGetLegalHold.mask()
                    | EventName::ObjectAccessedHead.mask()
                    | EventName::ObjectAccessedAttributes.mask()
            }
            EventName::ObjectCreatedAll => {
                EventName::ObjectCreatedCompleteMultipartUpload.mask()
                    | EventName::ObjectCreatedCopy.mask()
                    | EventName::ObjectCreatedPost.mask()
                    | EventName::ObjectCreatedPut.mask()
                    | EventName::ObjectCreatedPutRetention.mask()
                    | EventName::ObjectCreatedPutLegalHold.mask()
                    | EventName::ObjectCreatedPutTagging.mask()
                    | EventName::ObjectCreatedDeleteTagging.mask()
            }
            EventName::ObjectRemovedAll => {
                EventName::ObjectRemovedDelete.mask()
                    | EventName::ObjectRemovedDeleteMarkerCreated.mask()
                    | EventName::ObjectRemovedNoOP.mask()
                    | EventName::ObjectRemovedDeleteAllVersions.mask()
            }
            EventName::ObjectReplicationAll => {
                EventName::ObjectReplicationFailed.mask()
                    | EventName::ObjectReplicationComplete.mask()
                    | EventName::ObjectReplicationMissedThreshold.mask()
                    | EventName::ObjectReplicationReplicatedAfterThreshold.mask()
                    | EventName::ObjectReplicationNotTracked.mask()
            }
            EventName::ObjectTransitionAll => {
                EventName::ObjectTransitionFailed.mask() | EventName::ObjectTransitionComplete.mask()
            }
            EventName::ObjectSingleTypesEnd | EventName::ObjectScannerAll => 0,
        }
    }
}

impl AsRef<str> for EventName {
    fn as_ref(&self) -> &str {
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
            EventName::ILMDelMarkerExpirationDelete => "s3:LifecycleDelMarkerExpiration:Delete",
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
            EventName::ObjectManyVersions => "s3:Scanner:ManyVersions",
            EventName::ObjectLargeVersions => "s3:Scanner:LargeVersions",
            EventName::PrefixManyFolders => "s3:Scanner:BigPrefix",
            _ => "",
        }
    }
}

impl From<&str> for EventName {
    fn from(s: &str) -> Self {
        match s {
            "s3:BucketCreated:*" => EventName::BucketCreated,
            "s3:BucketRemoved:*" => EventName::BucketRemoved,
            "s3:ObjectAccessed:*" => EventName::ObjectAccessedAll,
            "s3:ObjectAccessed:Get" => EventName::ObjectAccessedGet,
            "s3:ObjectAccessed:GetRetention" => EventName::ObjectAccessedGetRetention,
            "s3:ObjectAccessed:GetLegalHold" => EventName::ObjectAccessedGetLegalHold,
            "s3:ObjectAccessed:Head" => EventName::ObjectAccessedHead,
            "s3:ObjectAccessed:Attributes" => EventName::ObjectAccessedAttributes,
            "s3:ObjectCreated:*" => EventName::ObjectCreatedAll,
            "s3:ObjectCreated:CompleteMultipartUpload" => EventName::ObjectCreatedCompleteMultipartUpload,
            "s3:ObjectCreated:Copy" => EventName::ObjectCreatedCopy,
            "s3:ObjectCreated:Post" => EventName::ObjectCreatedPost,
            "s3:ObjectCreated:Put" => EventName::ObjectCreatedPut,
            "s3:ObjectCreated:PutRetention" => EventName::ObjectCreatedPutRetention,
            "s3:ObjectCreated:PutLegalHold" => EventName::ObjectCreatedPutLegalHold,
            "s3:ObjectCreated:PutTagging" => EventName::ObjectCreatedPutTagging,
            "s3:ObjectCreated:DeleteTagging" => EventName::ObjectCreatedDeleteTagging,
            "s3:ObjectRemoved:*" => EventName::ObjectRemovedAll,
            "s3:ObjectRemoved:Delete" => EventName::ObjectRemovedDelete,
            "s3:ObjectRemoved:DeleteMarkerCreated" => EventName::ObjectRemovedDeleteMarkerCreated,
            "s3:ObjectRemoved:NoOP" => EventName::ObjectRemovedNoOP,
            "s3:ObjectRemoved:DeleteAllVersions" => EventName::ObjectRemovedDeleteAllVersions,
            "s3:LifecycleDelMarkerExpiration:Delete" => EventName::ILMDelMarkerExpirationDelete,
            "s3:Replication:*" => EventName::ObjectReplicationAll,
            "s3:Replication:OperationFailedReplication" => EventName::ObjectReplicationFailed,
            "s3:Replication:OperationCompletedReplication" => EventName::ObjectReplicationComplete,
            "s3:Replication:OperationMissedThreshold" => EventName::ObjectReplicationMissedThreshold,
            "s3:Replication:OperationReplicatedAfterThreshold" => EventName::ObjectReplicationReplicatedAfterThreshold,
            "s3:Replication:OperationNotTracked" => EventName::ObjectReplicationNotTracked,
            "s3:ObjectRestore:*" => EventName::ObjectRestoreAll,
            "s3:ObjectRestore:Post" => EventName::ObjectRestorePost,
            "s3:ObjectRestore:Completed" => EventName::ObjectRestoreCompleted,
            "s3:ObjectTransition:Failed" => EventName::ObjectTransitionFailed,
            "s3:ObjectTransition:Complete" => EventName::ObjectTransitionComplete,
            "s3:ObjectTransition:*" => EventName::ObjectTransitionAll,
            "s3:Scanner:ManyVersions" => EventName::ObjectManyVersions,
            "s3:Scanner:LargeVersions" => EventName::ObjectLargeVersions,
            "s3:Scanner:BigPrefix" => EventName::PrefixManyFolders,
            _ => EventName::Everything,
        }
    }
}
