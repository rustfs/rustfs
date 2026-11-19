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

use crate::error::ApiError;
use crate::storage::ObjectOptions;
use crate::storage::authorization::{
    AuthorizationRequest, CopySourceBucketGenerationGuard, ObjectTagConditions, authorize_replication_only_put_headers,
    authorize_request, authorize_staged_multipart_part, authorize_write_etag_condition, get_or_fetch_object_tag_conditions,
    load_bucket_generation, missing_req_info_error, req_info_mut, req_info_ref, request_object_store,
    set_requested_object_lock_retain_until,
};
use crate::storage::authorization_actions::{
    complete_multipart_upload_authorize_action, versioned_read_action, versioned_tagging_action,
};
use crate::storage::request_markers::{RequestedCopySource, RequestedObjectTags};
use crate::storage::storage_api::runtime_sources_consumer::ServerContextSlot;
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_s3_types::{S3Result, s3_error};

/// Authorizes completion and records the bucket generation before publication.
pub(crate) async fn authorize_complete_multipart_upload<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    server: &ServerContextSlot,
    bucket: &str,
    key: &str,
    specific_etag: bool,
) -> S3Result<()> {
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
#[derive(Clone, Copy)]
pub(crate) struct CopySourceObject<'a> {
    pub(crate) bucket: &'a str,
    pub(crate) key: &'a str,
    pub(crate) version_id: Option<&'a str>,
}

pub(crate) async fn authorize_upload_part_copy<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    server: &ServerContextSlot,
    bucket: &str,
    key: &str,
    source: Option<CopySourceObject<'_>>,
) -> S3Result<()> {
    let generation = load_bucket_generation(server, req, bucket).await;
    authorize_copy_source(req, server, source).await?;
    let info = req.req_info_mut().ok_or_else(missing_req_info_error)?;
    info.bucket = Some(bucket.to_owned());
    info.object = Some(key.to_owned());
    info.version_id = None;
    authorize_staged_multipart_part(req).await?;
    req.install_bucket_generation(generation?);
    Ok(())
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

/// Request facts shared by both CopyObject access hooks.
pub(crate) struct CopyObjectAuthorization<'a> {
    pub(crate) bucket: &'a str,
    pub(crate) key: &'a str,
    pub(crate) source: Option<CopySourceObject<'a>>,
    pub(crate) query_version: Option<&'a str>,
    pub(crate) tagging: Option<&'a str>,
    pub(crate) tagging_directive: Option<&'a str>,
    pub(crate) retain_until: Option<time::OffsetDateTime>,
    pub(crate) legal_hold: bool,
    pub(crate) retention: bool,
}

/// Authorize the exact tag set selected by CopyObject's source reader before publication.
/// The access hook also calls this with a preliminary snapshot, so tag-dependent
/// destination Allow statements can be evaluated before opening either object.
pub(crate) async fn authorize_copy_object_tags<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    source: CopySourceObject<'_>,
    copies_tags: bool,
    source_tags: &str,
    effective_tags: &str,
) -> S3Result<()> {
    // Direct internal use-case calls with no tags retain their existing contract.
    // Public CopyObject requests always carry the access hook's requested-tag snapshot.
    if effective_tags.is_empty() && req.requested_object_tags().is_none() {
        return Ok(());
    }
    let tag_set = crate::storage::s3_api::tagging::parse_object_tag_header(effective_tags)?;
    if copies_tags && !tag_set.is_empty() {
        let destination = req_info_ref(req)?.clone();
        // Reuse the reader's metadata instead of reopening the source under its
        // read guard (or the self-copy write guard). These are the tags being copied.
        let source_conditions = crate::storage::s3_api::tagging::parse_object_tag_header(source_tags)?
            .into_iter()
            .filter_map(|tag| Some((format!("ExistingObjectTag/{}", tag.key?), vec![tag.value?])))
            .collect();
        let destination_tags = req.take_object_tag_conditions();
        let destination_requested_tags = req.take_requested_object_tags();
        req.install_object_tag_conditions(ObjectTagConditions::new(
            source.bucket,
            source.key,
            source.version_id,
            source_conditions,
        ));
        let info = req_info_mut(req)?;
        info.bucket = Some(source.bucket.to_owned());
        info.object = Some(source.key.to_owned());
        info.version_id = source.version_id.map(str::to_owned);
        let result = authorize_request(req, versioned_tagging_action(S3Action::GetObjectTaggingAction, source.version_id)).await;
        *req_info_mut(req)? = destination;
        req.take_object_tag_conditions();
        if let Some(tags) = destination_tags {
            req.install_object_tag_conditions(tags);
        }
        if let Err(error) = result {
            if let Some(tags) = destination_requested_tags {
                req.install_requested_object_tags(tags);
            }
            return Err(error);
        }
    }
    req.install_requested_object_tags(RequestedObjectTags(
        tag_set.into_iter().filter_map(|tag| Some((tag.key?, tag.value?))).collect(),
    ));
    authorize_request(req, Action::S3Action(S3Action::PutObjectAction)).await
}

/// Authorizes both resources and records the generation and tag snapshots for the copy.
pub(crate) async fn authorize_copy_object<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    server: &ServerContextSlot,
    facts: CopyObjectAuthorization<'_>,
) -> S3Result<()> {
    use crate::storage::storage_api::contract::object::ObjectOperations as _;

    let bucket_generation = load_bucket_generation(server, req, facts.bucket).await;
    #[cfg(test)]
    crate::storage::authorization::wait_for_object_generation_test_hook(facts.bucket).await;
    authorize_copy_source(req, server, facts.source).await?;
    let source = facts.source.ok_or_else(|| s3_error!(NotImplemented))?;
    let destination_version = crate::storage::options::destination_version_id(facts.query_version, req.headers());
    let info = req_info_mut(req)?;
    info.bucket = Some(facts.bucket.to_owned());
    info.object = Some(facts.key.to_owned());
    info.version_id = destination_version.clone();
    set_requested_object_lock_retain_until(req, facts.retain_until);
    let replacement = crate::storage::s3_api::tagging::resolve_copy_object_tags(facts.tagging, facts.tagging_directive)?;
    let store = request_object_store(req)?;
    let copies_tags = replacement.is_none();
    let source_tags = if copies_tags {
        match store
            .get_object_tags(
                source.bucket,
                source.key,
                &ObjectOptions {
                    // Keep the decoded version spelling for IAM, but address its storage version.
                    version_id: source.version_id.map(|version| {
                        let version = version.trim();
                        if version.eq_ignore_ascii_case("null") {
                            uuid::Uuid::nil().to_string()
                        } else {
                            version.to_owned()
                        }
                    }),
                    ..Default::default()
                },
            )
            .await
        {
            Ok(tags) => tags,
            Err(error) if crate::storage::is_err_object_not_found(&error) || crate::storage::is_err_version_not_found(&error) => {
                String::new()
            }
            Err(error) => return Err(ApiError::from(error).into()),
        }
    } else {
        String::new()
    };
    // The copy body holds the destination write lock when it rechecks actual tags.
    if source.bucket == facts.bucket && source.key == facts.key {
        get_or_fetch_object_tag_conditions(
            req,
            store.as_ref(),
            facts.bucket,
            facts.key,
            destination_version.as_deref(),
            Action::S3Action(S3Action::PutObjectAction),
        )
        .await?;
    }
    req.install_requested_object_tags(RequestedObjectTags::default());
    authorize_copy_object_tags(req, source, copies_tags, &source_tags, replacement.as_deref().unwrap_or(&source_tags)).await?;
    authorize_replication_only_put_headers(req).await?;
    if facts.legal_hold {
        authorize_request(req, Action::S3Action(S3Action::PutObjectLegalHoldAction)).await?;
    }
    if facts.retention {
        authorize_request(req, Action::S3Action(S3Action::PutObjectRetentionAction)).await?;
    }
    req.install_bucket_generation(bucket_generation?);
    check_copy_source_owner(req.headers())
}

/// Checks a copy's source-owner assertion against the owner RustFS reports.
pub(crate) fn check_copy_source_owner(headers: &http::HeaderMap) -> S3Result<()> {
    let mut values = headers.get_all("x-amz-source-expected-bucket-owner").iter();
    let Some(value) = values.next() else {
        return Ok(());
    };
    if values.next().is_some() {
        return Err(s3_error!(InvalidRequest, "Multiple source expected owner headers"));
    }
    let expected = value
        .to_str()
        .map_err(|_| s3_error!(InvalidRequest, "Invalid source expected owner header"))?
        .trim();
    if expected.is_empty() {
        return Ok(());
    }
    if expected != crate::storage::s3_api::common::RUSTFS_OWNER_ID {
        return Err(s3_error!(AccessDenied));
    }
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
