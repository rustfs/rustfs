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

//! Which IAM action an object request authorizes against.
//!
//! Not responsible for evaluating the action. Upstream: the use cases and owned
//! authorization shapes that name an object version; downstream: `authorize_request`.

use rustfs_policy::policy::action::{Action, S3Action};

pub(crate) fn delete_object_authorize_action(version_id: Option<&str>) -> Action {
    Action::S3Action(if version_id.is_some() {
        S3Action::DeleteObjectVersionAction
    } else {
        S3Action::DeleteObjectAction
    })
}

/// GHSA-3ppv: select the IAM action for an object read by whether the request
/// names an explicit object version. A read that targets a specific version must
/// authorize against `s3:GetObjectVersion`; a current-object read authorizes
/// against `s3:GetObject`. Reading a historical version while only holding
/// `s3:GetObject` is an information-disclosure bypass, so the two must not be
/// conflated at the authorization boundary.
///
/// Note: `ActionSet::is_match` still maps a `s3:GetObjectVersion` grant onto a
/// `s3:GetObject` request. It only broadens a Version grant toward current
/// reads, never the reverse.
pub(crate) fn versioned_read_action(version_id: Option<&str>) -> Action {
    if version_id.is_some() {
        Action::S3Action(S3Action::GetObjectVersionAction)
    } else {
        Action::S3Action(S3Action::GetObjectAction)
    }
}

pub(crate) fn versioned_tagging_action(action: S3Action, version_id: Option<&str>) -> Action {
    let action = if version_id.is_some() {
        match action {
            S3Action::GetObjectTaggingAction => S3Action::GetObjectVersionTaggingAction,
            S3Action::PutObjectTaggingAction => S3Action::PutObjectVersionTaggingAction,
            S3Action::DeleteObjectTaggingAction => S3Action::DeleteObjectVersionTaggingAction,
            action => action,
        }
    } else {
        action
    };
    Action::S3Action(action)
}
