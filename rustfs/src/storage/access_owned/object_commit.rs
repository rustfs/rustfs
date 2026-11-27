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
    load_bucket_generation, missing_req_info_error, replication_request_authorized, req_info_mut, req_info_ref,
    request_object_store, set_requested_object_lock_retain_until,
};
use crate::storage::authorization_actions::{
    complete_multipart_upload_authorize_action, versioned_read_action, versioned_tagging_action,
};
use crate::storage::request_markers::{RequestedCopySource, RequestedObjectTags};
use crate::storage::storage_api::runtime_sources_consumer::ServerContextSlot;
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_s3_types::{S3Result, s3_error};
use rustfs_utils::http::{SUFFIX_SOURCE_REPLICATION_REQUEST, get_header};

/// Returns true when `x-amz-tagging` carries at least one tag (or fails closed on duplicate/invalid
/// header values so authorization never silently skips a tagging check).
pub(crate) fn has_non_empty_request_tags(headers: &http::HeaderMap) -> bool {
    let mut values = headers.get_all("x-amz-tagging").iter();
    let Some(first) = values.next() else {
        return false;
    };
    if values.next().is_some() {
        return true;
    }
    let Ok(raw) = first.to_str() else {
        return true;
    };
    crate::storage::s3_api::tagging::parse_object_tag_header(raw).map_or(true, |tags| !tags.is_empty())
}

/// Action required to write object tags during an object-write operation.
///
/// Authorized inbound replication requests (never browser POST forms) require `s3:ReplicateTags`;
/// all other tagged writes require `s3:PutObjectTagging`.
pub(crate) fn write_tagging_action<R: AuthorizationRequest + ?Sized>(req: &R, post: bool) -> Action {
    let inbound_replication = !post
        && replication_request_authorized(req)
        && get_header(req.headers(), SUFFIX_SOURCE_REPLICATION_REQUEST).as_deref() == Some("true");
    if inbound_replication {
        Action::S3Action(S3Action::ReplicateTagsAction)
    } else {
        Action::S3Action(S3Action::PutObjectTaggingAction)
    }
}

/// Authorizes object tagging requested via `x-amz-tagging` on PutObject, PostObject, or
/// CreateMultipartUpload.
pub(crate) async fn authorize_write_request_tagging<R: AuthorizationRequest + ?Sized>(req: &mut R, post: bool) -> S3Result<()> {
    if !has_non_empty_request_tags(req.policy_headers()) {
        return Ok(());
    }
    authorize_request(req, write_tagging_action(req, post)).await
}

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
    let writes_tags = !tag_set.is_empty();
    if copies_tags && writes_tags {
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
    authorize_request(req, Action::S3Action(S3Action::PutObjectAction)).await?;
    if writes_tags {
        if !replication_request_authorized(req) {
            authorize_replication_only_put_headers(req).await?;
        }
        authorize_request(req, write_tagging_action(req, false)).await?;
    }
    Ok(())
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
    let (retain_until, legal_hold, retention) = super::object_write::resolve_hoisted_object_lock_authorization(
        req,
        false,
        facts.retain_until,
        facts.legal_hold,
        facts.retention,
    )?;
    set_requested_object_lock_retain_until(req, retain_until);
    let hoisted_tagging = match facts.tagging {
        Some(tagging) => Some(tagging.to_owned()),
        None => req
            .headers()
            .get("x-amz-tagging")
            .map(|value| {
                value
                    .to_str()
                    .map(str::to_owned)
                    .map_err(|_| s3_error!(InvalidTag, "The Tagging header contains invalid UTF-8"))
            })
            .transpose()?,
    };
    let hoisted_tagging_directive = match facts.tagging_directive {
        Some(directive) => Some(directive.to_owned()),
        None => req
            .headers()
            .get("x-amz-tagging-directive")
            .map(|value| {
                value
                    .to_str()
                    .map(str::to_owned)
                    .map_err(|_| s3_error!(InvalidArgument, "The TaggingDirective header is invalid"))
            })
            .transpose()?,
    };
    let replacement = crate::storage::s3_api::tagging::resolve_copy_object_tags(
        hoisted_tagging.as_deref(),
        hoisted_tagging_directive.as_deref(),
    )?;
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
    if !replication_request_authorized(req) {
        authorize_replication_only_put_headers(req).await?;
    }
    if legal_hold {
        authorize_request(req, Action::S3Action(S3Action::PutObjectLegalHoldAction)).await?;
    }
    if retention {
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

#[cfg(test)]
mod tagging_authz_tests {
    use super::{CopyObjectAuthorization, CopySourceObject, ObjectOptions, authorize_copy_object};
    use crate::storage::access_owned::listing_multipart::{CreateMultipartAuthorization, authorize_create_multipart_upload};
    use crate::storage::access_owned::object_write::{
        PostObjectFormPolicy, PostObjectPolicyFields, PutObjectAuthorization, authorize_put_object,
    };
    use crate::storage::authorization::tests::fixture::{request, server_context};
    use crate::storage::storage_api::contract::bucket::{BucketOperations as _, MakeBucketOptions};
    use crate::storage::storage_api::contract::object::ObjectIO as _;
    use crate::storage::storage_api::runtime_sources_consumer::ECStore;
    use http::Method;
    use rustfs_s3_types::S3ErrorCode;
    use std::collections::HashMap;

    async fn publish_policy(store: &ECStore, bucket: &str, statements: Vec<serde_json::Value>) {
        let policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": statements,
        });
        store
            .update_bucket_metadata_config(
                bucket,
                crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
                serde_json::to_vec(&policy).expect("serialize policy"),
            )
            .await
            .expect("publish policy");
    }

    #[test]
    #[serial_test::serial]
    fn tagged_object_writes_require_put_object_tagging_and_evaluate_request_tag_conditions() {
        crate::app::gating_test_env::run_large_stack_test("tagged-writes-authz", || async {
            let store = crate::app::gating_test_env::shared_gating_ecstore().await;
            let slot = server_context(store.clone());
            let bucket = format!("tagged-writes-{}", uuid::Uuid::new_v4().simple());
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("create bucket");

            for (key, tags) in [("src-untagged", ""), ("src-dev", "env=dev"), ("src-prod", "env=prod")] {
                let mut reader = crate::storage::PutObjReader::from_vec(b"data".to_vec());
                let mut user_defined = HashMap::new();
                if !tags.is_empty() {
                    user_defined.insert(rustfs_utils::http::AMZ_OBJECT_TAGGING.to_string(), tags.to_string());
                }
                store
                    .put_object(
                        &bucket,
                        key,
                        &mut reader,
                        &ObjectOptions {
                            user_defined,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("seed source object");
            }

            let base_statements = vec![serde_json::json!({
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": ["s3:PutObject", "s3:GetObject", "s3:GetObjectTagging"],
                "Resource": format!("arn:aws:s3:::{bucket}/*"),
            })];

            // Both "missing s3:PutObjectTagging" and "explicit Deny s3:PutObjectTagging" must
            // block tagged writes while continuing to allow untagged writes.
            for with_explicit_deny in [false, true] {
                let mut statements = base_statements.clone();
                if with_explicit_deny {
                    statements.push(serde_json::json!({
                        "Effect": "Deny",
                        "Principal": {"AWS": "*"},
                        "Action": "s3:PutObjectTagging",
                        "Resource": format!("arn:aws:s3:::{bucket}/*"),
                    }));
                }
                publish_policy(&store, &bucket, statements).await;

                // PutObject: tagged denied, untagged and empty tagging header allowed.
                let mut put_tagged = request(Method::PUT)
                    .header("x-amz-tagging", "env=dev")
                    .req_info()
                    .server_context(&slot)
                    .build();
                let err = authorize_put_object(
                    &mut put_tagged,
                    &slot,
                    PutObjectAuthorization {
                        bucket: &bucket,
                        key: "dst-put",
                        version_id: None,
                        retain_until: None,
                        specific_etag: false,
                        legal_hold: false,
                        retention: false,
                        post: false,
                    },
                    |_| Ok(()),
                )
                .await
                .expect_err("tagged PutObject without PutObjectTagging must be denied");
                assert_eq!(err.code(), &S3ErrorCode::AccessDenied);

                for empty_tagging in [None, Some("")] {
                    let mut builder = request(Method::PUT).req_info().server_context(&slot);
                    if let Some(value) = empty_tagging {
                        builder = builder.header("x-amz-tagging", value);
                    }
                    let mut put_untagged = builder.build();
                    authorize_put_object(
                        &mut put_untagged,
                        &slot,
                        PutObjectAuthorization {
                            bucket: &bucket,
                            key: "dst-put",
                            version_id: None,
                            retain_until: None,
                            specific_etag: false,
                            legal_hold: false,
                            retention: false,
                            post: false,
                        },
                        |_| Ok(()),
                    )
                    .await
                    .expect("untagged PutObject must still succeed");
                }

                // PostObject: form x-amz-tagging denied; spoofed wire header without form field allowed.
                let mut post_tagged = request(Method::POST).req_info().server_context(&slot).build();
                let err = authorize_put_object(
                    &mut post_tagged,
                    &slot,
                    PutObjectAuthorization {
                        bucket: &bucket,
                        key: "dst-post",
                        version_id: None,
                        retain_until: None,
                        specific_etag: false,
                        legal_hold: false,
                        retention: false,
                        post: true,
                    },
                    |req| {
                        PostObjectFormPolicy::new(PostObjectPolicyFields {
                            tagging: Some("env=dev"),
                            ..Default::default()
                        })?
                        .install(req);
                        Ok(())
                    },
                )
                .await
                .expect_err("tagged PostObject without PutObjectTagging must be denied");
                assert_eq!(err.code(), &S3ErrorCode::AccessDenied);

                let mut post_untagged = request(Method::POST)
                    .header("x-amz-tagging", "env=spoofed-wire")
                    .req_info()
                    .server_context(&slot)
                    .build();
                authorize_put_object(
                    &mut post_untagged,
                    &slot,
                    PutObjectAuthorization {
                        bucket: &bucket,
                        key: "dst-post",
                        version_id: None,
                        retain_until: None,
                        specific_etag: false,
                        legal_hold: false,
                        retention: false,
                        post: true,
                    },
                    |req| {
                        PostObjectFormPolicy::new(PostObjectPolicyFields::default())?.install(req);
                        Ok(())
                    },
                )
                .await
                .expect("untagged PostObject ignores wire x-amz-tagging and succeeds");

                // CreateMultipartUpload: tagged denied, untagged allowed.
                let mut mpu_tagged = request(Method::POST)
                    .header("x-amz-tagging", "env=dev")
                    .req_info()
                    .server_context(&slot)
                    .build();
                let err = authorize_create_multipart_upload(
                    &slot,
                    &mut mpu_tagged,
                    CreateMultipartAuthorization {
                        bucket: &bucket,
                        key: "dst-mpu",
                        query_version: None,
                        retain_until: None,
                        legal_hold_present: false,
                        retention_present: false,
                    },
                )
                .await
                .expect_err("tagged CreateMultipartUpload without PutObjectTagging must be denied");
                assert_eq!(err.code(), &S3ErrorCode::AccessDenied);

                let mut mpu_untagged = request(Method::POST).req_info().server_context(&slot).build();
                authorize_create_multipart_upload(
                    &slot,
                    &mut mpu_untagged,
                    CreateMultipartAuthorization {
                        bucket: &bucket,
                        key: "dst-mpu",
                        query_version: None,
                        retain_until: None,
                        legal_hold_present: false,
                        retention_present: false,
                    },
                )
                .await
                .expect("untagged CreateMultipartUpload must still succeed");

                // CopyObject REPLACE with non-empty tags denied; REPLACE empty allowed.
                let mut copy_replace = request(Method::PUT).req_info().server_context(&slot).build();
                let err = authorize_copy_object(
                    &mut copy_replace,
                    &slot,
                    CopyObjectAuthorization {
                        bucket: &bucket,
                        key: "dst-copy",
                        source: Some(CopySourceObject {
                            bucket: &bucket,
                            key: "src-untagged",
                            version_id: None,
                        }),
                        query_version: None,
                        tagging: Some("env=dev"),
                        tagging_directive: Some("REPLACE"),
                        retain_until: None,
                        legal_hold: false,
                        retention: false,
                    },
                )
                .await
                .expect_err("CopyObject REPLACE with tags without PutObjectTagging must be denied");
                assert_eq!(err.code(), &S3ErrorCode::AccessDenied);

                let mut copy_replace_empty = request(Method::PUT).req_info().server_context(&slot).build();
                authorize_copy_object(
                    &mut copy_replace_empty,
                    &slot,
                    CopyObjectAuthorization {
                        bucket: &bucket,
                        key: "dst-copy",
                        source: Some(CopySourceObject {
                            bucket: &bucket,
                            key: "src-dev",
                            version_id: None,
                        }),
                        query_version: None,
                        tagging: Some(""),
                        tagging_directive: Some("REPLACE"),
                        retain_until: None,
                        legal_hold: false,
                        retention: false,
                    },
                )
                .await
                .expect("CopyObject REPLACE clearing tags must still succeed");

                // CopyObject COPY from tagged source denied; COPY from untagged source allowed.
                let mut copy_tagged_src = request(Method::PUT).req_info().server_context(&slot).build();
                let err = authorize_copy_object(
                    &mut copy_tagged_src,
                    &slot,
                    CopyObjectAuthorization {
                        bucket: &bucket,
                        key: "dst-copy",
                        source: Some(CopySourceObject {
                            bucket: &bucket,
                            key: "src-dev",
                            version_id: None,
                        }),
                        query_version: None,
                        tagging: None,
                        tagging_directive: Some("COPY"),
                        retain_until: None,
                        legal_hold: false,
                        retention: false,
                    },
                )
                .await
                .expect_err("CopyObject COPY of tagged source without PutObjectTagging must be denied");
                assert_eq!(err.code(), &S3ErrorCode::AccessDenied);

                let mut copy_untagged_src = request(Method::PUT).req_info().server_context(&slot).build();
                authorize_copy_object(
                    &mut copy_untagged_src,
                    &slot,
                    CopyObjectAuthorization {
                        bucket: &bucket,
                        key: "dst-copy",
                        source: Some(CopySourceObject {
                            bucket: &bucket,
                            key: "src-untagged",
                            version_id: None,
                        }),
                        query_version: None,
                        tagging: None,
                        tagging_directive: Some("COPY"),
                        retain_until: None,
                        legal_hold: false,
                        retention: false,
                    },
                )
                .await
                .expect("CopyObject COPY of untagged source must still succeed");
            }

            // Grant s3:PutObjectTagging conditioned on s3:RequestObjectTag/env == "dev".
            let mut conditioned = base_statements;
            conditioned.push(serde_json::json!({
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "s3:PutObjectTagging",
                "Resource": format!("arn:aws:s3:::{bucket}/*"),
                "Condition": {"StringEquals": {"s3:RequestObjectTag/env": "dev"}},
            }));
            publish_policy(&store, &bucket, conditioned).await;

            for (tag, source_key, expected_ok) in [("env=dev", "src-dev", true), ("env=prod", "src-prod", false)] {
                let mut put_req = request(Method::PUT)
                    .header("x-amz-tagging", tag)
                    .req_info()
                    .server_context(&slot)
                    .build();
                let res = authorize_put_object(
                    &mut put_req,
                    &slot,
                    PutObjectAuthorization {
                        bucket: &bucket,
                        key: "dst-put",
                        version_id: None,
                        retain_until: None,
                        specific_etag: false,
                        legal_hold: false,
                        retention: false,
                        post: false,
                    },
                    |_| Ok(()),
                )
                .await;
                assert_eq!(res.is_ok(), expected_ok, "PutObject with {tag}");

                let mut post_req = request(Method::POST).req_info().server_context(&slot).build();
                let res = authorize_put_object(
                    &mut post_req,
                    &slot,
                    PutObjectAuthorization {
                        bucket: &bucket,
                        key: "dst-post",
                        version_id: None,
                        retain_until: None,
                        specific_etag: false,
                        legal_hold: false,
                        retention: false,
                        post: true,
                    },
                    |req| {
                        PostObjectFormPolicy::new(PostObjectPolicyFields {
                            tagging: Some(tag),
                            ..Default::default()
                        })?
                        .install(req);
                        Ok(())
                    },
                )
                .await;
                assert_eq!(res.is_ok(), expected_ok, "PostObject with {tag}");

                let mut mpu_req = request(Method::POST)
                    .header("x-amz-tagging", tag)
                    .req_info()
                    .server_context(&slot)
                    .build();
                let res = authorize_create_multipart_upload(
                    &slot,
                    &mut mpu_req,
                    CreateMultipartAuthorization {
                        bucket: &bucket,
                        key: "dst-mpu",
                        query_version: None,
                        retain_until: None,
                        legal_hold_present: false,
                        retention_present: false,
                    },
                )
                .await;
                assert_eq!(res.is_ok(), expected_ok, "CreateMultipartUpload with {tag}");

                let mut copy_replace = request(Method::PUT).req_info().server_context(&slot).build();
                let res = authorize_copy_object(
                    &mut copy_replace,
                    &slot,
                    CopyObjectAuthorization {
                        bucket: &bucket,
                        key: "dst-copy",
                        source: Some(CopySourceObject {
                            bucket: &bucket,
                            key: "src-untagged",
                            version_id: None,
                        }),
                        query_version: None,
                        tagging: Some(tag),
                        tagging_directive: Some("REPLACE"),
                        retain_until: None,
                        legal_hold: false,
                        retention: false,
                    },
                )
                .await;
                assert_eq!(res.is_ok(), expected_ok, "CopyObject REPLACE with {tag}");

                let mut copy_preserve = request(Method::PUT).req_info().server_context(&slot).build();
                let res = authorize_copy_object(
                    &mut copy_preserve,
                    &slot,
                    CopyObjectAuthorization {
                        bucket: &bucket,
                        key: "dst-copy",
                        source: Some(CopySourceObject {
                            bucket: &bucket,
                            key: source_key,
                            version_id: None,
                        }),
                        query_version: None,
                        tagging: None,
                        tagging_directive: Some("COPY"),
                        retain_until: None,
                        legal_hold: false,
                        retention: false,
                    },
                )
                .await;
                assert_eq!(res.is_ok(), expected_ok, "CopyObject COPY with {source_key}");
            }
        });
    }

    #[test]
    #[serial_test::serial]
    fn inbound_replication_tagged_writes_require_replicate_tags() {
        crate::app::gating_test_env::run_large_stack_test("replication-tagged-writes", || async {
            let store = crate::app::gating_test_env::shared_gating_ecstore().await;
            let slot = server_context(store.clone());
            let bucket = format!("repl-tagged-writes-{}", uuid::Uuid::new_v4().simple());
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("create bucket");

            let mut reader = crate::storage::PutObjReader::from_vec(b"data".to_vec());
            store
                .put_object(&bucket, "src", &mut reader, &ObjectOptions::default())
                .await
                .expect("seed source");

            // Authorized replicator: has ReplicateObject + ReplicateTags, but PutObjectTagging is denied.
            publish_policy(
                &store,
                &bucket,
                vec![
                    serde_json::json!({
                        "Effect": "Allow",
                        "Principal": {"AWS": "*"},
                        "Action": ["s3:PutObject", "s3:GetObject", "s3:ReplicateObject", "s3:ReplicateTags"],
                        "Resource": format!("arn:aws:s3:::{bucket}/*"),
                    }),
                    serde_json::json!({
                        "Effect": "Deny",
                        "Principal": {"AWS": "*"},
                        "Action": "s3:PutObjectTagging",
                        "Resource": format!("arn:aws:s3:::{bucket}/*"),
                    }),
                ],
            )
            .await;

            for repl_header in ["x-rustfs-source-replication-request", "x-minio-source-replication-request"] {
                let mut put_repl = request(Method::PUT)
                    .header(repl_header, "true")
                    .header("x-amz-tagging", "env=replica")
                    .req_info()
                    .server_context(&slot)
                    .build();
                authorize_put_object(
                    &mut put_repl,
                    &slot,
                    PutObjectAuthorization {
                        bucket: &bucket,
                        key: "dst-put",
                        version_id: None,
                        retain_until: None,
                        specific_etag: false,
                        legal_hold: false,
                        retention: false,
                        post: false,
                    },
                    |_| Ok(()),
                )
                .await
                .expect("inbound replication tagged PutObject with ReplicateTags must succeed");
            }

            let mut mpu_repl = request(Method::POST)
                .header("x-rustfs-source-replication-request", "true")
                .header("x-amz-tagging", "env=replica")
                .req_info()
                .server_context(&slot)
                .build();
            authorize_create_multipart_upload(
                &slot,
                &mut mpu_repl,
                CreateMultipartAuthorization {
                    bucket: &bucket,
                    key: "dst-mpu",
                    query_version: None,
                    retain_until: None,
                    legal_hold_present: false,
                    retention_present: false,
                },
            )
            .await
            .expect("inbound replication tagged CreateMultipartUpload with ReplicateTags must succeed");

            let mut copy_repl = request(Method::PUT)
                .header("x-rustfs-source-replication-request", "true")
                .req_info()
                .server_context(&slot)
                .build();
            authorize_copy_object(
                &mut copy_repl,
                &slot,
                CopyObjectAuthorization {
                    bucket: &bucket,
                    key: "dst-copy",
                    source: Some(CopySourceObject {
                        bucket: &bucket,
                        key: "src",
                        version_id: None,
                    }),
                    query_version: None,
                    tagging: Some("env=replica"),
                    tagging_directive: Some("REPLACE"),
                    retain_until: None,
                    legal_hold: false,
                    retention: false,
                },
            )
            .await
            .expect("inbound replication tagged CopyObject with ReplicateTags must succeed");

            // Browser POST form must never use the replication exception even if the header is set.
            let mut post_repl = request(Method::POST)
                .header("x-rustfs-source-replication-request", "true")
                .req_info()
                .server_context(&slot)
                .build();
            let err = authorize_put_object(
                &mut post_repl,
                &slot,
                PutObjectAuthorization {
                    bucket: &bucket,
                    key: "dst-post",
                    version_id: None,
                    retain_until: None,
                    specific_etag: false,
                    legal_hold: false,
                    retention: false,
                    post: true,
                },
                |req| {
                    PostObjectFormPolicy::new(PostObjectPolicyFields {
                        tagging: Some("env=replica"),
                        ..Default::default()
                    })?
                    .install(req);
                    Ok(())
                },
            )
            .await
            .expect_err("PostObject must require PutObjectTagging even with replication header");
            assert_eq!(err.code(), &S3ErrorCode::AccessDenied);

            // Without the inbound replication header, the same principal is still denied PutObjectTagging.
            let mut put_normal = request(Method::PUT)
                .header("x-amz-tagging", "env=replica")
                .req_info()
                .server_context(&slot)
                .build();
            let err = authorize_put_object(
                &mut put_normal,
                &slot,
                PutObjectAuthorization {
                    bucket: &bucket,
                    key: "dst-put",
                    version_id: None,
                    retain_until: None,
                    specific_etag: false,
                    legal_hold: false,
                    retention: false,
                    post: false,
                },
                |_| Ok(()),
            )
            .await
            .expect_err("non-replication tagged PutObject must still require PutObjectTagging");
            assert_eq!(err.code(), &S3ErrorCode::AccessDenied);

            // Replicator missing s3:ReplicateTags must be denied tagged inbound replication writes.
            publish_policy(
                &store,
                &bucket,
                vec![serde_json::json!({
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},
                    "Action": ["s3:PutObject", "s3:ReplicateObject"],
                    "Resource": format!("arn:aws:s3:::{bucket}/*"),
                })],
            )
            .await;

            let mut put_missing_repl_tags = request(Method::PUT)
                .header("x-rustfs-source-replication-request", "true")
                .header("x-amz-tagging", "env=replica")
                .req_info()
                .server_context(&slot)
                .build();
            let err = authorize_put_object(
                &mut put_missing_repl_tags,
                &slot,
                PutObjectAuthorization {
                    bucket: &bucket,
                    key: "dst-put",
                    version_id: None,
                    retain_until: None,
                    specific_etag: false,
                    legal_hold: false,
                    retention: false,
                    post: false,
                },
                |_| Ok(()),
            )
            .await
            .expect_err("inbound replication tagged PutObject without ReplicateTags must be denied");
            assert_eq!(err.code(), &S3ErrorCode::AccessDenied);

            let mut mpu_missing_repl_tags = request(Method::POST)
                .header("x-rustfs-source-replication-request", "true")
                .header("x-amz-tagging", "env=replica")
                .req_info()
                .server_context(&slot)
                .build();
            let err = authorize_create_multipart_upload(
                &slot,
                &mut mpu_missing_repl_tags,
                CreateMultipartAuthorization {
                    bucket: &bucket,
                    key: "dst-mpu",
                    query_version: None,
                    retain_until: None,
                    legal_hold_present: false,
                    retention_present: false,
                },
            )
            .await
            .expect_err("inbound replication tagged CreateMultipartUpload without ReplicateTags must be denied");
            assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
        });
    }
}
