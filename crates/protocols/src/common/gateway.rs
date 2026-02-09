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

use rustfs_credentials;
use rustfs_policy::policy::action::S3Action as PolicyS3Action;
use serde_json;
use std::collections::HashMap;
use thiserror::Error;
use tracing::error;

use super::session::SessionContext;

/// Authorization errors
#[derive(Debug, Error)]
pub enum AuthorizationError {
    #[error("Access denied")]
    AccessDenied,
}

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
    CopyObject,

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

/// Check if operation is supported for the given protocol
pub fn is_operation_supported(protocol: super::session::Protocol, action: &S3Action) -> bool {
    match protocol {
        super::session::Protocol::Ftps => match action {
            // Bucket operations
            S3Action::CreateBucket => true,
            S3Action::DeleteBucket => true,

            // Object operations
            S3Action::GetObject => true,    // RETR command
            S3Action::PutObject => true,    // STOR and APPE commands both map to PutObject
            S3Action::DeleteObject => true, // DELE command
            S3Action::HeadObject => true,   // SIZE command

            // Multipart operations
            S3Action::CreateMultipartUpload => false,
            S3Action::UploadPart => false,
            S3Action::CompleteMultipartUpload => false,
            S3Action::AbortMultipartUpload => false,
            S3Action::ListMultipartUploads => false,
            S3Action::ListParts => false,

            // ACL operations
            S3Action::GetBucketAcl => false,
            S3Action::PutBucketAcl => false,
            S3Action::GetObjectAcl => false,
            S3Action::PutObjectAcl => false,

            // Other operations
            S3Action::CopyObject => false, // No native copy support in FTPS
            S3Action::ListBucket => true,  // LIST command
            S3Action::ListBuckets => true, // LIST at root level
            S3Action::HeadBucket => true,  // Can check if directory exists
        },
    }
}

/// Check if a principal is allowed to perform an S3 action
pub async fn is_authorized(session_context: &SessionContext, action: &S3Action, bucket: &str, object: Option<&str>) -> bool {
    let iam_sys = match rustfs_iam::get() {
        Ok(sys) => sys,
        Err(e) => {
            error!("IAM system unavailable: {}", e);
            return false;
        }
    };

    // Create policy arguments
    let mut claims = HashMap::new();
    claims.insert(
        "principal".to_string(),
        serde_json::Value::String(session_context.principal.access_key().to_string()),
    );

    let policy_action: rustfs_policy::policy::action::Action = action.clone().into();

    // Check if user is the owner (admin)
    let is_owner = if let Some(global_cred) = rustfs_credentials::get_global_action_cred() {
        session_context.principal.access_key() == global_cred.access_key
    } else {
        false
    };

    let args = rustfs_policy::policy::Args {
        account: session_context.principal.access_key(),
        groups: &session_context.principal.user_identity.credentials.groups,
        action: policy_action,
        bucket,
        conditions: &HashMap::new(),
        is_owner,
        object: object.unwrap_or(""),
        claims: &claims,
        deny_only: false,
    };

    iam_sys.is_allowed(&args).await
}

/// Authorize an operation and return an error if not authorized
pub async fn authorize_operation(
    session_context: &SessionContext,
    action: &S3Action,
    bucket: &str,
    object: Option<&str>,
) -> Result<(), AuthorizationError> {
    // check if the operation is supported
    if !is_operation_supported(session_context.protocol, action) {
        return Err(AuthorizationError::AccessDenied);
    }

    // check IAM authorization
    if is_authorized(session_context, action, bucket, object).await {
        Ok(())
    } else {
        Err(AuthorizationError::AccessDenied)
    }
}
