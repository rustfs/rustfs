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

use rustfs_policy::policy::action::S3Action as PolicyS3Action;

/// S3 actions that can be performed through the gateway
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum S3Action {
    // Bucket operations
    CreateBucket,
    DeleteBucket,
    ListBucket,
    ListBuckets,
    HeadBucket,

    // Object operations
    GetObject,
    PutObject,
    DeleteObject,
    HeadObject,

    // Multipart operations
    CreateMultipartUpload,
    UploadPart,
    CompleteMultipartUpload,
    AbortMultipartUpload,
    ListMultipartUploads,
    ListParts,

    // ACL operations
    GetBucketAcl,
    PutBucketAcl,
    GetObjectAcl,
    PutObjectAcl,

    // Other operations
    CopyObject,
}

impl From<S3Action> for PolicyS3Action {
    fn from(action: S3Action) -> Self {
        match action {
            S3Action::CreateBucket => PolicyS3Action::CreateBucketAction,
            S3Action::DeleteBucket => PolicyS3Action::DeleteBucketAction,
            S3Action::ListBucket => PolicyS3Action::ListBucketAction,
            S3Action::ListBuckets => PolicyS3Action::ListAllMyBucketsAction,
            S3Action::HeadBucket => PolicyS3Action::HeadBucketAction,
            S3Action::GetObject => PolicyS3Action::GetObjectAction,
            S3Action::PutObject => PolicyS3Action::PutObjectAction,
            S3Action::DeleteObject => PolicyS3Action::DeleteObjectAction,
            S3Action::HeadObject => PolicyS3Action::GetObjectAction,
            S3Action::CreateMultipartUpload => PolicyS3Action::PutObjectAction,
            S3Action::UploadPart => PolicyS3Action::PutObjectAction,
            S3Action::CompleteMultipartUpload => PolicyS3Action::PutObjectAction,
            S3Action::AbortMultipartUpload => PolicyS3Action::AbortMultipartUploadAction,
            S3Action::ListMultipartUploads => PolicyS3Action::ListBucketMultipartUploadsAction,
            S3Action::ListParts => PolicyS3Action::ListMultipartUploadPartsAction,
            S3Action::GetBucketAcl => PolicyS3Action::GetBucketPolicyAction,
            S3Action::PutBucketAcl => PolicyS3Action::PutBucketPolicyAction,
            S3Action::GetObjectAcl => PolicyS3Action::GetObjectAction,
            S3Action::PutObjectAcl => PolicyS3Action::PutObjectAction,
            S3Action::CopyObject => PolicyS3Action::PutObjectAction,
        }
    }
}

impl From<S3Action> for rustfs_policy::policy::action::Action {
    fn from(action: S3Action) -> Self {
        rustfs_policy::policy::action::Action::S3Action(action.into())
    }
}

impl S3Action {
    /// Get the string representation of the action
    pub fn as_str(&self) -> &'static str {
        match self {
            S3Action::CreateBucket => "s3:CreateBucket",
            S3Action::DeleteBucket => "s3:DeleteBucket",
            S3Action::ListBucket => "s3:ListBucket",
            S3Action::ListBuckets => "s3:ListAllMyBuckets",
            S3Action::HeadBucket => "s3:ListBucket",
            S3Action::GetObject => "s3:GetObject",
            S3Action::PutObject => "s3:PutObject",
            S3Action::DeleteObject => "s3:DeleteObject",
            S3Action::HeadObject => "s3:GetObject",
            S3Action::CreateMultipartUpload => "s3:PutObject",
            S3Action::UploadPart => "s3:PutObject",
            S3Action::CompleteMultipartUpload => "s3:PutObject",
            S3Action::AbortMultipartUpload => "s3:AbortMultipartUpload",
            S3Action::ListMultipartUploads => "s3:ListBucketMultipartUploads",
            S3Action::ListParts => "s3:ListMultipartUploadParts",
            S3Action::GetBucketAcl => "s3:GetBucketAcl",
            S3Action::PutBucketAcl => "s3:PutBucketAcl",
            S3Action::GetObjectAcl => "s3:GetObjectAcl",
            S3Action::PutObjectAcl => "s3:PutObjectAcl",
            S3Action::CopyObject => "s3:PutObject",
        }
    }
}
