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

//! The policy action each owned authorization picks for an operation, and the answer a terminal
//! IAM refusal becomes.
//!
//! Responsible for: `storage::authorization_actions` (the action selection both edges share) and
//! `storage::authorization::iam_authorization_error`.
//! Not responsible for: evaluating a policy against the chosen action, or actions an operation's
//! gateway handler names inline.
//! Upstream: none. Downstream: `storage::authorization_actions` and `storage::authorization`.

use crate::storage::authorization::iam_authorization_error;
use crate::storage::authorization_actions::{
    complete_multipart_upload_authorize_action, list_parts_authorize_action, post_object_authorize_action, versioned_read_action,
};
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_s3_types::{S3Error, S3ErrorCode};

#[test]
fn terminal_iam_denial_is_access_denied_and_backend_failure_stays_internal() {
    let denied = S3Error::from(iam_authorization_error(rustfs_iam::error::Error::IAMActionNotAllowed));
    assert_eq!(denied.code(), &S3ErrorCode::AccessDenied);
    let unavailable = S3Error::from(iam_authorization_error(rustfs_iam::error::Error::IamSysNotInitialized));
    assert_eq!(unavailable.code(), &S3ErrorCode::InternalError);
}

#[test]
fn ghsa_3ppv_versioned_read_selects_get_object_version_action() {
    // Regression (GHSA-3ppv): a read that names an explicit version must
    // authorize against s3:GetObjectVersion so that a principal holding only
    // s3:GetObject cannot read historical versions. Applies to GetObject and
    // the CopyObject / UploadPartCopy sources.
    assert_eq!(
        versioned_read_action(Some("0194e0f1-0000-7000-8000-000000000000")),
        Action::S3Action(S3Action::GetObjectVersionAction),
        "an explicit versionId must require GetObjectVersion"
    );
    assert_eq!(
        versioned_read_action(Some("null")),
        Action::S3Action(S3Action::GetObjectVersionAction),
        "the sentinel `null` version is still an explicit version selector"
    );
    assert_eq!(
        versioned_read_action(None),
        Action::S3Action(S3Action::GetObjectAction),
        "a current-object read (no versionId) authorizes against GetObject"
    );
}

#[test]
fn post_object_uses_put_object_action() {
    assert_eq!(post_object_authorize_action(), Action::S3Action(S3Action::PutObjectAction));
}

#[test]
fn complete_multipart_upload_uses_put_object_action() {
    assert_eq!(complete_multipart_upload_authorize_action(), Action::S3Action(S3Action::PutObjectAction));
}

#[test]
fn list_parts_uses_list_multipart_upload_parts_action() {
    assert_eq!(list_parts_authorize_action(), Action::S3Action(S3Action::ListMultipartUploadPartsAction));
}
