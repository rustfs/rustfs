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

//! Authorization shapes that only migration batch E2 (rustfs/backlog#2765) uses: DeleteObject, DeleteObjects, RestoreObject and SelectObjectContent.
//!
//! Responsible for: the owned authorization function of each such operation, generic over
//! `AuthorizationRequest`, so the legacy `S3Access` hook and `Served::authorize` call the same code.
//! Not responsible for: shapes more than one batch shares (those stay in `storage/access.rs`), the
//! policy evaluation itself (`authorize_request`), or the operation's use case.
//! Upstream: the legacy hook in `storage/access.rs` and the batch's `server/gateway_handlers/object_misc.rs`.
//! Downstream: the shared shapes in `storage/access.rs`.
//!
//! DeleteObject: the decisions its use case adds afterwards (`ReplicateDelete`, every descendant of
//! a recursive force delete) are not here.

use crate::storage::ReplicationStatusType;
#[cfg(test)]
use crate::storage::access::wait_for_object_generation_test_hook;
use crate::storage::access::{
    AuthorizationRequest, BucketGenerationGuard, PendingDeleteBucketGenerationGuard, S3Error, S3ErrorCode, S3Result,
    ServerContextSlot, audit_request_denial, authorize_request, delete_object_authorize_action, has_bypass_governance_header,
    invalid_force_delete_header, load_bucket_generation, missing_req_info_error, recursive_force_delete_has_authenticated_caller,
    s3_error,
};
use crate::storage::denial_audit::DenialAuditor;
use crate::storage::options::destination_version_id;
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_utils::http::{AMZ_BUCKET_REPLICATION_STATUS, force_delete_header};

/// DeleteObject's authorization on either edge: the bucket generation is loaded before the
/// decision, and the guard it becomes is installed for the use case after an allow.
pub(crate) async fn authorize_delete_object<R: AuthorizationRequest + ?Sized>(
    server_ctx: &ServerContextSlot,
    req: &mut R,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> S3Result<()> {
    let bucket_generation = load_bucket_generation(server_ctx, req, bucket).await;
    #[cfg(test)]
    wait_for_object_generation_test_hook(bucket).await;
    let guard = decide_delete_object(req, bucket, key, version_id, bucket_generation).await?;
    req.install_pending_delete_bucket_generation(guard);
    Ok(())
}

/// DeleteObject's decision, in the legacy order: `NoSuchBucket` before policy, then
/// `DeleteObject[Version]` for the version storage deletes (a replica delete names it through its
/// source-version header), `ForceDeleteObject` for a force delete, the authenticated-caller rule
/// for a recursive force delete, and `BypassGovernanceRetention` when the header asks for it.
/// Returns the guard `bucket_generation` becomes.
async fn decide_delete_object<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    bucket_generation: S3Result<BucketGenerationGuard>,
) -> S3Result<PendingDeleteBucketGenerationGuard> {
    if let Err(err) = &bucket_generation
        && err.code() == &S3ErrorCode::NoSuchBucket
    {
        return Err(S3Error::with_message(
            S3ErrorCode::NoSuchBucket,
            err.message().unwrap_or("The specified bucket does not exist").to_string(),
        ));
    }

    let replica_request = req
        .headers()
        .get(AMZ_BUCKET_REPLICATION_STATUS)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == ReplicationStatusType::Replica.as_str());
    // Storage reads the source-version header only for a replica request, so any other request is
    // authorized for the version its query names.
    let version_id = if replica_request {
        destination_version_id(version_id, req.headers())
    } else {
        version_id.map(str::to_owned)
    };
    let req_info = req.req_info_mut().ok_or_else(missing_req_info_error)?;
    req_info.bucket = Some(bucket.to_owned());
    req_info.object = Some(key.to_owned());
    req_info.version_id = version_id;
    let authenticated = req_info.is_owner || req_info.cred.is_some();
    let action = delete_object_authorize_action(req_info.version_id.as_deref());

    authorize_request(req, action).await?;

    let force_delete = match force_delete_header(req.headers()) {
        Ok(value) => value.unwrap_or(false),
        Err(_) => return Err(invalid_force_delete_header()),
    };
    // The REPLICA header is caller-supplied, so it does not skip this check; `s3:*` does not
    // grant `s3:ForceDeleteObject`.
    if force_delete {
        authorize_request(req, Action::S3Action(S3Action::ForceDeleteObjectAction)).await?;
    }
    if !recursive_force_delete_has_authenticated_caller(req.headers(), authenticated, replica_request) {
        audit_request_denial(
            req,
            DenialAuditor::global(),
            "recursive_force_delete_requires_authenticated_caller",
            Action::S3Action(S3Action::ForceDeleteObjectAction),
        );
        return Err(s3_error!(AccessDenied, "Recursive force-delete requires an authenticated caller"));
    }

    if has_bypass_governance_header(req.headers()) {
        authorize_request(req, Action::S3Action(S3Action::BypassGovernanceRetentionAction)).await?;
    }

    Ok(PendingDeleteBucketGenerationGuard::from_result(bucket_generation))
}

#[cfg(test)]
#[path = "object_misc_test.rs"]
mod tests;
