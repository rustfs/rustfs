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

//! Authorization shapes that only migration batch B1 (rustfs/backlog#2765) uses: object ACL, tagging, legal hold, retention and torrent.
//!
//! Responsible for: the owned authorization function of each such operation, generic over
//! `AuthorizationRequest`, so the legacy `S3Access` hook and `Served::authorize` call the same code.
//! Not responsible for: shapes more than one batch shares (those stay in `storage/access.rs`), the
//! policy evaluation itself (`authorize_request`), or the operation's use case.
//! Upstream: the legacy hook in `storage/access.rs` and the batch's `server/gateway_handlers/object_meta.rs`.
//! Downstream: the shared shapes in `storage/access.rs`.

use super::super::{
    AuthorizationRequest, S3Result, ServerContextSlot, authorize_object_request, authorize_request, has_bypass_governance_header,
    load_bucket_generation, req_info_mut, set_requested_object_lock_retain_until, versioned_tagging_action,
};
pub(crate) use super::super::{RequestedObjectLegalHold, RequestedObjectLockRetention, RequestedObjectTags};
use rustfs_policy::policy::action::{Action, S3Action};

/// GetObjectTagging and DeleteObjectTagging: the plain object shape, with the tagging action
/// swapped for its version action when the request names a version.
pub(crate) async fn authorize_object_tagging_request<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    action: S3Action,
) -> S3Result<()> {
    authorize_object_request(req, bucket, key, version_id, versioned_tagging_action(action, version_id)).await
}

/// Authorize the version and tag conditions using the original query and XML tag set.
pub(crate) async fn authorize_put_object_tagging<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    tags: RequestedObjectTags,
) -> S3Result<()> {
    req.install_requested_object_tags(tags);
    authorize_object_tagging_request(req, bucket, key, version_id, S3Action::PutObjectTaggingAction).await
}

/// Authorize the hold from the decoded XML for the exact version the request names.
pub(crate) async fn authorize_put_object_legal_hold<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    server: &ServerContextSlot,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    legal_hold: RequestedObjectLegalHold,
) -> S3Result<()> {
    req.install_requested_legal_hold(legal_hold);
    let info = req_info_mut(req)?;
    info.bucket = Some(bucket.to_owned());
    info.object = Some(key.to_owned());
    info.version_id = version_id.map(str::to_owned);

    let generation = load_bucket_generation(server, req, bucket).await;
    authorize_request(req, Action::S3Action(S3Action::PutObjectLegalHoldAction)).await?;
    req.install_bucket_generation(generation?);
    Ok(())
}

/// Authorize the requested retention, then separately authorize a true governance bypass.
pub(crate) async fn authorize_put_object_retention<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    server: &ServerContextSlot,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    retention: RequestedObjectLockRetention,
) -> S3Result<()> {
    let info = req_info_mut(req)?;
    info.bucket = Some(bucket.to_owned());
    info.object = Some(key.to_owned());
    info.version_id = version_id.map(str::to_owned);

    let generation = load_bucket_generation(server, req, bucket).await;
    set_requested_object_lock_retain_until(req, retention.retain_until);
    req.install_requested_retention(retention);
    authorize_request(req, Action::S3Action(S3Action::PutObjectRetentionAction)).await?;
    if has_bypass_governance_header(req.headers()) {
        authorize_request(req, Action::S3Action(S3Action::BypassGovernanceRetentionAction)).await?;
    }
    req.install_bucket_generation(generation?);
    Ok(())
}
