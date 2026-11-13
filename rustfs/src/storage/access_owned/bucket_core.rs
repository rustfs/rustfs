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

//! Authorization shapes that only migration batch A4
//! (rustfs/backlog#2765) uses: its composite hooks.
//!
//! Responsible for: the owned authorization function of each such operation, generic over
//! `AuthorizationRequest`, so the legacy `S3Access` hook and `Served::authorize` call the same code.
//! Not responsible for: shapes more than one batch shares (those stay in `storage/access.rs`), the
//! policy evaluation itself (`authorize_request`), or the operation's use case.
//! Upstream: the legacy hook in `storage/access.rs` and the batch's `server/gateway_handlers/bucket_core.rs`.
//! Downstream: the shared shapes in `storage/access.rs`.

use super::super::{AuthorizationRequest, authorize_request, authorize_request_with_location_constraint, missing_req_info_error};
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_s3_types::S3Result;

/// CreateBucket's hook: `s3:CreateBucket` with the body's `LocationConstraint` as a condition
/// value, then, when the request enables object lock, `s3:PutBucketObjectLockConfiguration` and
/// `s3:PutBucketVersioning`. The first decision does not restore presigned `x-amz-meta-*` query
/// parameters into headers; the two that follow do.
///
/// # Errors
///
/// `InternalError` when the access phase installed no `ReqInfo`; otherwise the first refusal of
/// the three decisions.
pub(crate) async fn authorize_create_bucket<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    bucket: &str,
    location_constraint: Option<String>,
    object_lock_enabled: bool,
) -> S3Result<()> {
    req.req_info_mut().ok_or_else(missing_req_info_error)?.bucket = Some(bucket.to_owned());
    authorize_request_with_location_constraint(req, Action::S3Action(S3Action::CreateBucketAction), location_constraint).await?;
    if object_lock_enabled {
        authorize_request(req, Action::S3Action(S3Action::PutBucketObjectLockConfigurationAction)).await?;
        authorize_request(req, Action::S3Action(S3Action::PutBucketVersioningAction)).await?;
    }
    Ok(())
}
