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

//! Authorization shapes that only migration batch E1 (rustfs/backlog#2765) uses: CopyObject, UploadPartCopy and CompleteMultipartUpload.
//!
//! Responsible for: the owned authorization function of each such operation, generic over
//! `AuthorizationRequest`, so the legacy `S3Access` hook and `Served::authorize` call the same code.
//! Not responsible for: shapes more than one batch shares (those stay in `storage/access.rs`), the
//! policy evaluation itself (`authorize_request`), or the operation's use case.
//! Upstream: the legacy hook in `storage/access.rs` and the batch's `server/gateway_handlers/object_commit.rs`.
//! Downstream: the shared shapes in `storage/access.rs`.

use super::super::{
    AuthorizationRequest, CopySourceBucketGenerationGuard, RequestedCopySource, S3Result, ServerContextSlot, authorize_request,
    missing_req_info_error, s3_error, versioned_read_action,
};
use crate::error::ApiError;

/// Authorizes completion and records the bucket generation before publication.
pub(crate) async fn authorize_complete_multipart_upload<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    server: &ServerContextSlot,
    bucket: &str,
    key: &str,
    specific_etag: bool,
) -> S3Result<()> {
    use super::super::{
        authorize_replication_only_put_headers, authorize_write_etag_condition, complete_multipart_upload_authorize_action,
        load_bucket_generation,
    };
    let generation = load_bucket_generation(server, req, bucket).await;
    let info = req.req_info_mut().ok_or_else(missing_req_info_error)?;
    info.bucket = Some(bucket.to_owned());
    info.object = Some(key.to_owned());
    authorize_request(req, complete_multipart_upload_authorize_action()).await?;
    authorize_write_etag_condition(req, specific_etag).await?;
    authorize_replication_only_put_headers(req).await?;
    req.install_bucket_generation(generation?);
    Ok(())
}

/// The object a copy names as its source, as decoded from `x-amz-copy-source`.
pub(crate) struct CopySourceObject<'a> {
    pub(crate) bucket: &'a str,
    pub(crate) key: &'a str,
    pub(crate) version_id: Option<&'a str>,
}

/// The source half of a copy, which runs before anything about the destination is decided.
///
/// `None` is an access-point or Outposts ARN source and is `NotImplemented` before any policy is
/// read. Otherwise the source is recorded for the `x-amz-copy-source` condition, its bucket
/// incarnation is captured, reading it is authorized (`s3:GetObjectVersion` when a version is
/// named) and only then is a failed capture surfaced or the incarnation handed on. `ReqInfo` is
/// left naming the source; the caller points it at the destination.
pub(crate) async fn authorize_copy_source<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    server: &ServerContextSlot,
    source: Option<CopySourceObject<'_>>,
) -> S3Result<()> {
    let Some(source) = source else {
        return Err(s3_error!(NotImplemented));
    };
    req.install_requested_copy_source(RequestedCopySource::from_object(source.bucket, source.key, source.version_id));
    let generation = load_copy_source_bucket_generation(server, source.bucket).await;

    let req_info = req.req_info_mut().ok_or_else(missing_req_info_error)?;
    req_info.bucket = Some(source.bucket.to_owned());
    req_info.object = Some(source.key.to_owned());
    req_info.version_id = source.version_id.map(str::to_owned);
    // GHSA-3ppv: a versioned copy source must authorize against s3:GetObjectVersion.
    authorize_request(req, versioned_read_action(source.version_id)).await?;

    req.install_copy_source_bucket_generation(generation?);
    Ok(())
}

async fn load_copy_source_bucket_generation(
    server: &ServerContextSlot,
    bucket: &str,
) -> S3Result<CopySourceBucketGenerationGuard> {
    let store = server
        .object_store()
        .ok_or_else(|| s3_error!(InternalError, "object store is not initialized"))?;
    let incarnation_id = store.bucket_incarnation_id(bucket).await.map_err(ApiError::from)?;
    Ok(CopySourceBucketGenerationGuard {
        bucket: bucket.to_string(),
        incarnation_id,
    })
}

#[cfg(test)]
#[path = "object_commit_test.rs"]
mod tests;
