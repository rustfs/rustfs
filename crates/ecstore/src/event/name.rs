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

#[derive(Default)]
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
        todo!();
    }

    fn mask(&self) -> u64 {
        todo!();
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
