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

use metrics::{counter, describe_counter};
use std::sync::OnceLock;

const S3_OPS_METRIC: &str = "rustfs_s3_operations_total";

#[derive(Clone, Copy)]
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
            Self::AbortMultipartUpload => "AbortMultipartUpload",
            Self::CompleteMultipartUpload => "CompleteMultipartUpload",
            Self::CopyObject => "CopyObject",
            Self::CreateBucket => "CreateBucket",
            Self::CreateMultipartUpload => "CreateMultipartUpload",
            Self::DeleteBucket => "DeleteBucket",
            Self::DeleteBucketCors => "DeleteBucketCors",
            Self::DeleteBucketEncryption => "DeleteBucketEncryption",
            Self::DeleteBucketLifecycle => "DeleteBucketLifecycle",
            Self::DeleteBucketPolicy => "DeleteBucketPolicy",
            Self::DeleteBucketReplication => "DeleteBucketReplication",
            Self::DeleteBucketTagging => "DeleteBucketTagging",
            Self::DeleteObject => "DeleteObject",
            Self::DeleteObjectTagging => "DeleteObjectTagging",
            Self::DeleteObjects => "DeleteObjects",
            Self::DeletePublicAccessBlock => "DeletePublicAccessBlock",
            Self::GetBucketAcl => "GetBucketAcl",
            Self::GetBucketCors => "GetBucketCors",
            Self::GetBucketEncryption => "GetBucketEncryption",
            Self::GetBucketLifecycleConfiguration => "GetBucketLifecycleConfiguration",
            Self::GetBucketLocation => "GetBucketLocation",
            Self::GetBucketLogging => "GetBucketLogging",
            Self::GetBucketNotificationConfiguration => "GetBucketNotificationConfiguration",
            Self::GetBucketPolicy => "GetBucketPolicy",
            Self::GetBucketPolicyStatus => "GetBucketPolicyStatus",
            Self::GetBucketReplication => "GetBucketReplication",
            Self::GetBucketTagging => "GetBucketTagging",
            Self::GetBucketVersioning => "GetBucketVersioning",
            Self::GetObject => "GetObject",
            Self::GetObjectAcl => "GetObjectAcl",
            Self::GetObjectAttributes => "GetObjectAttributes",
            Self::GetObjectLegalHold => "GetObjectLegalHold",
            Self::GetObjectLockConfiguration => "GetObjectLockConfiguration",
            Self::GetObjectRetention => "GetObjectRetention",
            Self::GetObjectTagging => "GetObjectTagging",
            Self::GetObjectTorrent => "GetObjectTorrent",
            Self::GetPublicAccessBlock => "GetPublicAccessBlock",
            Self::HeadBucket => "HeadBucket",
            Self::HeadObject => "HeadObject",
            Self::ListBuckets => "ListBuckets",
            Self::ListMultipartUploads => "ListMultipartUploads",
            Self::ListObjectVersions => "ListObjectVersions",
            Self::ListObjects => "ListObjects",
            Self::ListObjectsV2 => "ListObjectsV2",
            Self::ListParts => "ListParts",
            Self::PutBucketAcl => "PutBucketAcl",
            Self::PutBucketCors => "PutBucketCors",
            Self::PutBucketEncryption => "PutBucketEncryption",
            Self::PutBucketLifecycleConfiguration => "PutBucketLifecycleConfiguration",
            Self::PutBucketLogging => "PutBucketLogging",
            Self::PutBucketNotificationConfiguration => "PutBucketNotificationConfiguration",
            Self::PutBucketPolicy => "PutBucketPolicy",
            Self::PutBucketReplication => "PutBucketReplication",
            Self::PutBucketTagging => "PutBucketTagging",
            Self::PutBucketVersioning => "PutBucketVersioning",
            Self::PutObject => "PutObject",
            Self::PutObjectAcl => "PutObjectAcl",
            Self::PutObjectLegalHold => "PutObjectLegalHold",
            Self::PutObjectLockConfiguration => "PutObjectLockConfiguration",
            Self::PutObjectRetention => "PutObjectRetention",
            Self::PutObjectTagging => "PutObjectTagging",
            Self::PutPublicAccessBlock => "PutPublicAccessBlock",
            Self::RestoreObject => "RestoreObject",
            Self::SelectObjectContent => "SelectObjectContent",
            Self::UploadPart => "UploadPart",
            Self::UploadPartCopy => "UploadPartCopy",
        }
    }
}

pub fn record_s3_op(op: S3Operation, bucket: &str) {
    counter!(S3_OPS_METRIC, "op" => op.as_str(), "bucket" => bucket.to_owned()).increment(1);
}

/// One-time registration of indicator meta information
/// This function ensures that metric descriptors are registered only once.
pub fn init_s3_metrics() {
    static METRICS_DESC_INIT: OnceLock<()> = OnceLock::new();
    METRICS_DESC_INIT.get_or_init(|| {
        describe_counter!(S3_OPS_METRIC, "Total number of S3 API operations handled");
    });
}
