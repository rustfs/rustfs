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

use rustfs_s3_types::EventName;

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
            Self::DeleteBucketCors => None,
            Self::DeleteBucketEncryption => None,
            Self::DeleteBucketLifecycle => None,
            Self::DeleteBucketPolicy => None,
            Self::DeleteBucketReplication => None,
            Self::DeleteBucketTagging => None,
            Self::DeletePublicAccessBlock => None,
            Self::GetBucketAcl => None,
            Self::GetBucketCors => None,
            Self::GetBucketEncryption => None,
            Self::GetBucketLifecycleConfiguration => None,
            Self::GetBucketLocation => None,
            Self::GetBucketLogging => None,
            Self::GetBucketNotificationConfiguration => None,
            Self::GetBucketPolicy => None,
            Self::GetBucketPolicyStatus => None,
            Self::GetBucketReplication => None,
            Self::GetBucketTagging => None,
            Self::GetBucketVersioning => None,
            Self::GetObjectAcl => None,
            Self::GetObjectLockConfiguration => None,
            Self::GetObjectTagging => None,
            Self::GetObjectTorrent => None,
            Self::GetPublicAccessBlock => None,
            Self::HeadBucket => None,
            Self::ListBuckets => None,
            Self::ListMultipartUploads => None,
            Self::ListObjectVersions => None,
            Self::ListObjects => None,
            Self::ListObjectsV2 => None,
            Self::ListParts => None,
            Self::PutBucketAcl => None,
            Self::PutBucketCors => None,
            Self::PutBucketEncryption => None,
            Self::PutBucketLifecycleConfiguration => None,
            Self::PutBucketLogging => None,
            Self::PutBucketNotificationConfiguration => None,
            Self::PutBucketPolicy => None,
            Self::PutBucketReplication => None,
            Self::PutBucketTagging => None,
            Self::PutBucketVersioning => None,
            Self::PutObjectLockConfiguration => None,
            Self::PutPublicAccessBlock => None,
            Self::UploadPart => None,
            Self::UploadPartCopy => None,
        }
    }
}

pub fn event_name_to_s3_operation(event_name: EventName) -> Option<S3Operation> {
    match event_name {
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

/// Returns whether an S3 operation is semantically compatible with an event name.
///
/// Some S3 operations intentionally map to multiple event variants:
/// - `PutObject` can emit both `ObjectCreatedPut` and `ObjectCreatedPost`.
/// - `DeleteObject` can emit delete/delete-marker/all-versions variants.
/// - `DeleteObjects` can emit per-object delete events in addition to the
///   internal batch-delete event.
pub fn operation_matches_event_name(op: S3Operation, event_name: EventName) -> bool {
    match op {
        S3Operation::PutObject => {
            matches!(event_name, EventName::ObjectCreatedPut | EventName::ObjectCreatedPost)
        }
        S3Operation::DeleteObject => matches!(
            event_name,
            EventName::ObjectRemovedDelete
                | EventName::ObjectRemovedDeleteMarkerCreated
                | EventName::ObjectRemovedDeleteAllVersions
        ),
        S3Operation::DeleteObjects => {
            matches!(event_name, EventName::ObjectRemovedDeleteObjects | EventName::ObjectRemovedDelete)
        }
        S3Operation::AbortMultipartUpload
        | S3Operation::CompleteMultipartUpload
        | S3Operation::CopyObject
        | S3Operation::CreateBucket
        | S3Operation::CreateMultipartUpload
        | S3Operation::DeleteBucket
        | S3Operation::DeleteObjectTagging
        | S3Operation::GetObject
        | S3Operation::GetObjectAttributes
        | S3Operation::GetObjectLegalHold
        | S3Operation::GetObjectRetention
        | S3Operation::HeadObject
        | S3Operation::PutObjectAcl
        | S3Operation::PutObjectLegalHold
        | S3Operation::PutObjectRetention
        | S3Operation::PutObjectTagging
        | S3Operation::RestoreObject
        | S3Operation::SelectObjectContent => op.to_event_name() == Some(event_name),
        S3Operation::DeleteBucketCors
        | S3Operation::DeleteBucketEncryption
        | S3Operation::DeleteBucketLifecycle
        | S3Operation::DeleteBucketPolicy
        | S3Operation::DeleteBucketReplication
        | S3Operation::DeleteBucketTagging
        | S3Operation::DeletePublicAccessBlock
        | S3Operation::GetBucketAcl
        | S3Operation::GetBucketCors
        | S3Operation::GetBucketEncryption
        | S3Operation::GetBucketLifecycleConfiguration
        | S3Operation::GetBucketLocation
        | S3Operation::GetBucketLogging
        | S3Operation::GetBucketNotificationConfiguration
        | S3Operation::GetBucketPolicy
        | S3Operation::GetBucketPolicyStatus
        | S3Operation::GetBucketReplication
        | S3Operation::GetBucketTagging
        | S3Operation::GetBucketVersioning
        | S3Operation::GetObjectAcl
        | S3Operation::GetObjectLockConfiguration
        | S3Operation::GetObjectTagging
        | S3Operation::GetObjectTorrent
        | S3Operation::GetPublicAccessBlock
        | S3Operation::HeadBucket
        | S3Operation::ListBuckets
        | S3Operation::ListMultipartUploads
        | S3Operation::ListObjectVersions
        | S3Operation::ListObjects
        | S3Operation::ListObjectsV2
        | S3Operation::ListParts
        | S3Operation::PutBucketAcl
        | S3Operation::PutBucketCors
        | S3Operation::PutBucketEncryption
        | S3Operation::PutBucketLifecycleConfiguration
        | S3Operation::PutBucketLogging
        | S3Operation::PutBucketNotificationConfiguration
        | S3Operation::PutBucketPolicy
        | S3Operation::PutBucketReplication
        | S3Operation::PutBucketTagging
        | S3Operation::PutBucketVersioning
        | S3Operation::PutObjectLockConfiguration
        | S3Operation::PutPublicAccessBlock
        | S3Operation::UploadPart
        | S3Operation::UploadPartCopy => false,
    }
}

/// Resolves the object-delete notification event name from delete-marker state.
#[inline]
pub fn delete_event_name_for_marker(delete_marker: bool) -> EventName {
    if delete_marker {
        EventName::ObjectRemovedDeleteMarkerCreated
    } else {
        EventName::ObjectRemovedDelete
    }
}

/// Resolves the object-create notification event name from POST-object mode.
#[inline]
pub fn put_event_name_for_post_object(is_post_object: bool) -> EventName {
    if is_post_object {
        EventName::ObjectCreatedPost
    } else {
        EventName::ObjectCreatedPut
    }
}

/// Returns `true` when the event is one of object-remove notification variants
/// that should omit size/etag metadata.
#[inline]
pub fn is_object_removed_event(event_name: EventName) -> bool {
    matches!(event_name, EventName::ObjectRemovedDelete | EventName::ObjectRemovedDeleteMarkerCreated)
}

/// Returns the event mask that matches both PUT and POST object-created events.
#[inline]
pub fn put_object_created_event_mask() -> u64 {
    EventName::ObjectCreatedPut.mask() | EventName::ObjectCreatedPost.mask()
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(event_name_to_s3_operation(EventName::ObjectCreatedPut), Some(S3Operation::PutObject));
        assert_eq!(event_name_to_s3_operation(EventName::ObjectAclPut), Some(S3Operation::PutObjectAcl));
        assert_eq!(
            event_name_to_s3_operation(EventName::ObjectTaggingPut),
            Some(S3Operation::PutObjectTagging)
        );
        assert_eq!(
            event_name_to_s3_operation(EventName::ObjectTaggingDelete),
            Some(S3Operation::DeleteObjectTagging)
        );
        assert_eq!(event_name_to_s3_operation(EventName::ObjectAccessedGet), Some(S3Operation::GetObject));
        assert_eq!(event_name_to_s3_operation(EventName::BucketCreated), Some(S3Operation::CreateBucket));
        assert_eq!(event_name_to_s3_operation(EventName::Everything), None);
        assert_eq!(event_name_to_s3_operation(EventName::ObjectRestorePost), Some(S3Operation::RestoreObject));
        assert_eq!(event_name_to_s3_operation(EventName::ObjectCreatedPost), Some(S3Operation::PutObject));
        assert_eq!(
            event_name_to_s3_operation(EventName::ObjectRemovedAbortMultipartUpload),
            Some(S3Operation::AbortMultipartUpload)
        );
    }

    #[test]
    fn test_operation_matches_event_name() {
        assert!(operation_matches_event_name(S3Operation::PutObject, EventName::ObjectCreatedPut));
        assert!(operation_matches_event_name(S3Operation::PutObject, EventName::ObjectCreatedPost));
        assert!(operation_matches_event_name(
            S3Operation::DeleteObject,
            EventName::ObjectRemovedDeleteMarkerCreated
        ));
        assert!(operation_matches_event_name(S3Operation::DeleteObjects, EventName::ObjectRemovedDelete));

        assert!(!operation_matches_event_name(S3Operation::GetObject, EventName::ObjectCreatedPut));
    }

    #[test]
    fn test_delete_event_name_for_marker() {
        assert_eq!(delete_event_name_for_marker(true), EventName::ObjectRemovedDeleteMarkerCreated);
        assert_eq!(delete_event_name_for_marker(false), EventName::ObjectRemovedDelete);
    }

    #[test]
    fn test_put_event_name_for_post_object() {
        assert_eq!(put_event_name_for_post_object(true), EventName::ObjectCreatedPost);
        assert_eq!(put_event_name_for_post_object(false), EventName::ObjectCreatedPut);
    }

    #[test]
    fn test_is_object_removed_event() {
        assert!(is_object_removed_event(EventName::ObjectRemovedDelete));
        assert!(is_object_removed_event(EventName::ObjectRemovedDeleteMarkerCreated));
        assert!(!is_object_removed_event(EventName::ObjectCreatedPut));
    }

    #[test]
    fn test_put_object_created_event_mask() {
        let mask = put_object_created_event_mask();
        assert_ne!(mask & EventName::ObjectCreatedPut.mask(), 0);
        assert_ne!(mask & EventName::ObjectCreatedPost.mask(), 0);
        assert_eq!(mask & EventName::ObjectRemovedDelete.mask(), 0);
    }

    #[test]
    fn test_operations_without_event_mapping_remain_unmapped() {
        let unmapped = [
            S3Operation::GetBucketAcl,
            S3Operation::ListObjectsV2,
            S3Operation::PutBucketNotificationConfiguration,
            S3Operation::UploadPart,
        ];
        for op in unmapped {
            assert_eq!(op.to_event_name(), None);
            assert!(!operation_matches_event_name(op, EventName::ObjectCreatedPut));
        }
    }
}
