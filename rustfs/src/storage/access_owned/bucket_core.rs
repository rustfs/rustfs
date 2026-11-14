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

use super::super::{
    AuthorizationRequest, authorize_request, authorize_request_with_location_constraint, invalid_force_delete_header,
    missing_req_info_error,
};
use http::{HeaderMap, HeaderValue};
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_s3_types::S3Result;
use rustfs_utils::http::force_delete_header;

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

/// The force delete a DeleteBucket request asks for, the one reading both the hook and the use
/// case use: `Some(true)` deletes a non-empty bucket, `Some(false)` and `None` do not.
///
/// `x-minio-force-delete` is `true` or `false` exactly, as the legacy edge decodes it; RustFS's
/// own `x-rustfs-force-delete` keeps its wider boolean grammar and wins when both are present.
///
/// # Errors
///
/// `InvalidArgument` for an `x-minio-force-delete` value other than `true` or `false`;
/// `InvalidRequest` for an `x-rustfs-force-delete` value that is not a boolean.
pub(crate) fn requested_bucket_force_delete(headers: &HeaderMap) -> S3Result<Option<bool>> {
    check_minio_force_delete(headers)?;
    force_delete_header(headers).map_err(|_| invalid_force_delete_header().into())
}

/// Refuses an `x-minio-force-delete` value other than `true` or `false`.
fn check_minio_force_delete(headers: &HeaderMap) -> S3Result<()> {
    match headers.get(MINIO_FORCE_DELETE).map(HeaderValue::as_bytes) {
        None | Some(b"true" | b"false") => Ok(()),
        Some(_) => Err(rustfs_s3_types::s3_error!(InvalidArgument, "Invalid x-minio-force-delete header value")),
    }
}

const MINIO_FORCE_DELETE: &str = "x-minio-force-delete";

/// DeleteBucket's hook: an `x-minio-force-delete` value the legacy edge refuses before its hook
/// is refused first; then `s3:DeleteBucket`, then `s3:ForceDeleteBucket` whenever a force-delete
/// header is present, `false` included (only a parsed `true` later skips the emptiness check).
/// `s3:*` does not grant `s3:ForceDeleteBucket`.
///
/// # Errors
///
/// `InternalError` when the access phase installed no `ReqInfo`; the refusals of
/// [`requested_bucket_force_delete`], `InvalidRequest` only after `s3:DeleteBucket` allowed the
/// caller; otherwise the first refusal of the two decisions.
pub(crate) async fn authorize_delete_bucket<R: AuthorizationRequest + ?Sized>(req: &mut R, bucket: &str) -> S3Result<()> {
    check_minio_force_delete(req.headers())?;
    req.req_info_mut().ok_or_else(missing_req_info_error)?.bucket = Some(bucket.to_owned());
    authorize_request(req, Action::S3Action(S3Action::DeleteBucketAction)).await?;
    if requested_bucket_force_delete(req.headers())?.is_some() {
        authorize_request(req, Action::S3Action(S3Action::ForceDeleteBucketAction)).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{HeaderMap, HeaderValue, MINIO_FORCE_DELETE, requested_bucket_force_delete};

    /// Each spelling is read by its own grammar: RustFS's wide one for `x-rustfs-force-delete`,
    /// exactly `true` / `false` for `x-minio-force-delete`, with each spelling's own refusal.
    #[test]
    fn each_force_delete_spelling_is_read_by_its_own_grammar() {
        let rustfs = "x-rustfs-force-delete";
        let minio = MINIO_FORCE_DELETE;
        for (name, value, expected) in [
            (rustfs, "true", Ok(Some(true))),
            (rustfs, "TRUE", Ok(Some(true))),
            (rustfs, "1", Ok(Some(true))),
            (rustfs, "false", Ok(Some(false))),
            (rustfs, "maybe", Err("InvalidRequest")),
            (minio, "true", Ok(Some(true))),
            (minio, "false", Ok(Some(false))),
            (minio, "TRUE", Err("InvalidArgument")),
            (minio, "1", Err("InvalidArgument")),
            (minio, "maybe", Err("InvalidArgument")),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(name, HeaderValue::from_static(value));
            let read = requested_bucket_force_delete(&headers);
            let read = read.as_ref().map_err(|error| error.code().as_str());
            assert_eq!(read, expected.as_ref().map_err(|code| *code), "{name}: {value}");
        }
        assert_eq!(requested_bucket_force_delete(&HeaderMap::new()).ok(), Some(None));
    }
}
