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

//! Authorization shapes that only migration batch D (rustfs/backlog#2765) uses: PutObject, UploadPart and PostObject.
//!
//! Responsible for: the owned authorization function of each such operation, generic over
//! `AuthorizationRequest`, so the legacy `S3Access` hook and `Served::authorize` call the same code.
//! Not responsible for: shapes more than one batch shares (those stay in `storage/access.rs`), the
//! policy evaluation itself (`authorize_request`), or the operation's use case.
//! Upstream: the legacy hook in `storage/access.rs` and the batch's `server/gateway_handlers/object_write.rs`.
//! Downstream: the shared shapes in `storage/access.rs`.

use super::super::{
    Action, ApiError, AuthorizationRequest, S3Action, S3Error, S3ErrorCode, S3Result, ServerContextSlot,
    authorize_replication_only_put_headers, authorize_request, authorize_staged_multipart_part, authorize_write_etag_condition,
    has_write_offset_bytes_header, load_bucket_generation, missing_req_info_error, set_requested_object_lock_retain_until,
};

pub(crate) async fn authorize_upload_part<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    server: &ServerContextSlot,
    bucket: &str,
    key: &str,
) -> S3Result<()> {
    let generation = load_bucket_generation(server, req, bucket).await;
    let info = req.req_info_mut().ok_or_else(missing_req_info_error)?;
    info.bucket = Some(bucket.to_owned());
    info.object = Some(key.to_owned());
    authorize_staged_multipart_part(req).await?;
    req.install_bucket_generation(generation?);
    Ok(())
}

/// Request facts needed by both PutObject authorization edges.
pub(crate) struct PutObjectAuthorization<'a> {
    pub(crate) bucket: &'a str,
    pub(crate) key: &'a str,
    pub(crate) version_id: Option<&'a str>,
    pub(crate) retain_until: Option<time::OffsetDateTime>,
    pub(crate) specific_etag: bool,
    pub(crate) legal_hold: bool,
    pub(crate) retention: bool,
    pub(crate) post: bool,
}

/// Preserves write refusal ordering and captures the generation authorized for publication.
pub(crate) async fn authorize_put_object<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    server: &ServerContextSlot,
    input: PutObjectAuthorization<'_>,
    prepare_policy: impl FnOnce(&mut R) -> S3Result<()> + Send,
) -> S3Result<()> {
    let version = crate::storage::options::destination_version_id(input.version_id, req.headers());
    let info = req.req_info_mut().ok_or_else(missing_req_info_error)?;
    info.bucket = Some(input.bucket.to_owned());
    info.object = Some(input.key.to_owned());
    info.version_id = version;
    if has_write_offset_bytes_header(req.headers()) {
        return Err(S3Error::with_message(
            S3ErrorCode::NotImplemented,
            ApiError::error_code_to_message(&S3ErrorCode::NotImplemented),
        ));
    }
    // Capture first, but let authorization refusals precede bucket-state failures.
    let generation = load_bucket_generation(server, req, input.bucket).await;
    #[cfg(test)]
    let generation = if super::super::PUT_GENERATION_READ_QUORUM_FAILURE.try_with(|_| ()).is_ok() {
        Err(ApiError::from(super::super::StorageError::ErasureReadQuorum).into())
    } else {
        generation
    };
    set_requested_object_lock_retain_until(req, input.retain_until);
    prepare_policy(req)?;
    authorize_request(req, Action::S3Action(S3Action::PutObjectAction)).await?;
    authorize_write_etag_condition(req, input.specific_etag).await?;
    req.install_bucket_generation(generation?);
    if !input.post {
        authorize_replication_only_put_headers(req).await?;
    }
    if input.legal_hold {
        authorize_request(req, Action::S3Action(S3Action::PutObjectLegalHoldAction)).await?;
    }
    if input.retention {
        authorize_request(req, Action::S3Action(S3Action::PutObjectRetentionAction)).await?;
    }
    Ok(())
}
