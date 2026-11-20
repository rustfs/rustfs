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

//! The copy-source, copy and completion authorization on the owned request envelope: the
//! decision and the state handed on, allowed and refused.
//!
//! Tests: `authorize_copy_source`, `authorize_copy_object`, `authorize_upload_part_copy` and
//! `authorize_complete_multipart_upload`. Not tested: decoding `x-amz-copy-source` (the facts are
//! written out as the gateway handler derives them) or the copy body.
//! Upstream: `storage::authorization::tests::fixture`. Downstream: none.

use crate::storage::access_owned::object_commit::{
    CopyObjectAuthorization, CopySourceObject, authorize_complete_multipart_upload, authorize_copy_object, authorize_copy_source,
    authorize_upload_part_copy,
};
use crate::storage::authorization::tests::fixture::{request, server_context};
use crate::storage::authorization::{AuthorizationRequest, CopySourceBucketGenerationGuard};
use crate::storage::storage_api::contract::bucket::{BucketOperations as _, MakeBucketOptions};
use crate::storage::storage_api::runtime_sources_consumer::{ECStore, ServerContextSlot};
use http::Method;
use rustfs_s3_types::{S3ErrorCode, S3Result};
use serial_test::serial;
use std::sync::Arc;

const VERSION: &str = "0194e0f1-0000-7000-8000-000000000000";

/// What one call decided and handed on.
#[derive(Debug, PartialEq)]
struct Observed {
    refusal: Option<S3ErrorCode>,
    requested_source: Option<String>,
    generation: Option<(String, uuid::Uuid)>,
    req_info: Option<(Option<String>, Option<String>, Option<String>)>,
}

fn observe<R: AuthorizationRequest>(
    req: &R,
    result: S3Result<()>,
    generation: Option<&CopySourceBucketGenerationGuard>,
) -> Observed {
    Observed {
        refusal: result.err().map(|error| error.code().clone()),
        requested_source: req.requested_copy_source().map(|source| source.0.clone()),
        generation: generation.map(|guard| (guard.bucket.clone(), guard.incarnation_id)),
        req_info: req
            .req_info()
            .map(|info| (info.bucket.clone(), info.object.clone(), info.version_id.clone())),
    }
}

/// Runs the copy-source authorization once with the given facts: `served` is the request's
/// server context, `capture` the one the source incarnation is read from.
async fn on_owned_edge(
    served: &Arc<ServerContextSlot>,
    capture: &ServerContextSlot,
    with_req_info: bool,
    source: Option<(&str, &str, Option<&str>)>,
) -> Observed {
    let object = source.map(|(bucket, key, version_id)| CopySourceObject { bucket, key, version_id });
    let mut builder = request(Method::PUT);
    if with_req_info {
        builder = builder.req_info();
    }
    let mut envelope = builder.server_context(served).build();
    let result = authorize_copy_source(&mut envelope, capture, object).await;
    observe(&envelope, result, envelope.copy_source_bucket_generation())
}

async fn source_bucket(store: &ECStore, actions: &[&str]) -> String {
    let bucket = format!("copy-source-parity-{}", uuid::Uuid::new_v4());
    store
        .make_bucket(&bucket, &MakeBucketOptions::default())
        .await
        .expect("create the copy source bucket");
    if actions.is_empty() {
        return bucket;
    }
    let policy = serde_json::json!({"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},
        "Action":actions,"Resource":[format!("arn:aws:s3:::{bucket}/*")]}]})
    .to_string();
    let mut metadata = (*crate::storage::get_bucket_metadata(&bucket)
        .await
        .expect("source bucket metadata is cached"))
    .clone();
    metadata.policy_config = Some(serde_json::from_str(&policy).expect("source policy parses"));
    metadata.policy_config_json = policy.into_bytes();
    crate::storage::storage_api::set_bucket_metadata(bucket.clone(), metadata)
        .await
        .expect("publish the source policy");
    bucket
}

#[test]
#[serial]
fn copy_source_authorization_decides_and_hands_on_the_same_on_both_edges() {
    crate::app::gating_test_env::run_large_stack_test("copy-source-parity", copy_source_parity_inner);
}

async fn copy_source_parity_inner() {
    let store = crate::app::gating_test_env::shared_gating_ecstore().await;
    let server = server_context(Arc::clone(&store));
    let unready = ServerContextSlot::new();

    let readable = source_bucket(&store, &["s3:GetObject"]).await;
    let version_readable = source_bucket(&store, &["s3:GetObjectVersion"]).await;
    let closed = source_bucket(&store, &[]).await;
    let incarnation = |bucket: String| {
        let store = Arc::clone(&store);
        async move {
            let id = store
                .bucket_incarnation_id(&bucket)
                .await
                .expect("read the source incarnation");
            Some((bucket, id))
        }
    };
    let named =
        |bucket: &str, version: Option<&str>| Some((Some(bucket.to_owned()), Some("key".to_owned()), version.map(str::to_owned)));
    let nothing_named = Some((None, None, None));

    let denied = |requested: String, req_info| Observed {
        refusal: Some(S3ErrorCode::AccessDenied),
        requested_source: Some(requested),
        generation: None,
        req_info,
    };
    let cases = [
        (
            "an ARN source is NotImplemented before any policy or state",
            Arc::clone(&server),
            true,
            None,
            Observed {
                refusal: Some(S3ErrorCode::NotImplemented),
                requested_source: None,
                generation: None,
                req_info: nothing_named.clone(),
            },
        ),
        (
            "a source without a read policy is refused and hands on no incarnation",
            Arc::clone(&server),
            true,
            Some((closed.as_str(), "key", None)),
            denied(format!("{closed}/key"), named(&closed, None)),
        ),
        (
            "a named version needs s3:GetObjectVersion, not s3:GetObject",
            Arc::clone(&server),
            true,
            Some((readable.as_str(), "key", Some(VERSION))),
            denied(format!("{readable}/key?versionId={VERSION}"), named(&readable, Some(VERSION))),
        ),
        (
            "a deny wins over a failed incarnation capture",
            Arc::clone(&unready),
            true,
            Some((closed.as_str(), "key", None)),
            denied(format!("{closed}/key"), named(&closed, None)),
        ),
        (
            "a failed incarnation capture is surfaced after the allow",
            Arc::clone(&unready),
            true,
            Some((readable.as_str(), "key", None)),
            Observed {
                refusal: Some(S3ErrorCode::InternalError),
                requested_source: Some(format!("{readable}/key")),
                generation: None,
                req_info: named(&readable, None),
            },
        ),
        (
            "a request without ReqInfo is refused as internal",
            Arc::clone(&server),
            false,
            Some((readable.as_str(), "key", None)),
            Observed {
                refusal: Some(S3ErrorCode::InternalError),
                requested_source: Some(format!("{readable}/key")),
                generation: None,
                req_info: None,
            },
        ),
        (
            "a readable source is allowed and hands on its incarnation",
            Arc::clone(&server),
            true,
            Some((readable.as_str(), "key", None)),
            Observed {
                refusal: None,
                requested_source: Some(format!("{readable}/key")),
                generation: incarnation(readable.clone()).await,
                req_info: named(&readable, None),
            },
        ),
        (
            "a readable version is allowed under s3:GetObjectVersion",
            Arc::clone(&server),
            true,
            Some((version_readable.as_str(), "key", Some(VERSION))),
            Observed {
                refusal: None,
                requested_source: Some(format!("{version_readable}/key?versionId={VERSION}")),
                generation: incarnation(version_readable.clone()).await,
                req_info: named(&version_readable, Some(VERSION)),
            },
        ),
    ];
    // Every case is checked before failing, so one run names every case a defect breaks.
    let mut mismatches = Vec::new();
    for (case, slot, with_req_info, source, expected) in cases {
        let owned = on_owned_edge(&server, &slot, with_req_info, source).await;
        if owned != expected {
            mismatches.push(format!("{case}: expected {expected:?}, owned {owned:?}"));
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}

#[test]
#[serial]
fn copy_hooks_decide_the_source_before_the_destination() {
    crate::app::gating_test_env::run_large_stack_test("copy-source-first", copy_hooks_source_first_inner);
}

/// The destination half refuses a `Tagging` header without the REPLACE directive as
/// `InvalidRequest`; that refusal must never be reached for a source the caller may not read.
async fn copy_hooks_source_first_inner() {
    let store = crate::app::gating_test_env::shared_gating_ecstore().await;
    let server = server_context(Arc::clone(&store));
    let closed = source_bucket(&store, &[]).await;
    let readable = source_bucket(&store, &["s3:GetObject"]).await;
    let destination = source_bucket(&store, &["s3:PutObject"]).await;

    // The copy source bucket as the request decoder hands it on (key `key`, no version); `None`
    // is an access-point ARN such as
    // `arn:aws:s3:us-east-1:123456789012:accesspoint/my-access-point/object/key`.
    for (case, source_bucket_name, copy_expected, part_expected) in [
        (
            "a denied source",
            Some(closed.as_str()),
            S3ErrorCode::AccessDenied,
            S3ErrorCode::AccessDenied,
        ),
        ("an ARN source", None, S3ErrorCode::NotImplemented, S3ErrorCode::NotImplemented),
        // Control: with the source readable, the destination half is reached and refuses.
        (
            "a readable source",
            Some(readable.as_str()),
            S3ErrorCode::InvalidRequest,
            S3ErrorCode::AccessDenied,
        ),
    ] {
        let source = || {
            source_bucket_name.map(|bucket| CopySourceObject {
                bucket,
                key: "key",
                version_id: None,
            })
        };
        let mut copy = request(Method::PUT).req_info().server_context(&server).build();
        let copy_error = authorize_copy_object(
            &mut copy,
            &server,
            CopyObjectAuthorization {
                bucket: &destination,
                key: "copy",
                source: source(),
                query_version: None,
                tagging: Some("a=b"),
                tagging_directive: None,
                retain_until: None,
                legal_hold: false,
                retention: false,
            },
        )
        .await
        .expect_err(case);
        assert_eq!(copy_error.code(), &copy_expected, "CopyObject: {case}");

        let mut part = request(Method::PUT).req_info().server_context(&server).build();
        let part_error = authorize_upload_part_copy(&mut part, &server, &closed, "copy", source())
            .await
            .expect_err(case);
        assert_eq!(part_error.code(), &part_expected, "UploadPartCopy: {case}");
    }
}

#[tokio::test]
#[serial]
async fn completion_authorization_requires_write_read_and_replication_permissions() {
    let store = crate::app::gating_test_env::shared_gating_ecstore().await;
    let server = server_context(Arc::clone(&store));
    for (actions, specific_etag, replica, allowed) in [
        (vec!["s3:PutObject"], false, false, true),
        (vec![], false, false, false),
        (vec!["s3:PutObject"], true, false, false),
        (vec!["s3:PutObject"], false, true, false),
        (vec!["s3:PutObject", "s3:GetObject"], true, false, true),
        (vec!["s3:GetObject"], false, false, false),
    ] {
        let bucket = source_bucket(&store, &actions).await;
        let mut builder = request(Method::POST).server_context(&server).req_info();
        if replica {
            builder = builder.header("x-amz-replication-status", "REPLICA");
        }
        let mut envelope = builder.build();
        let result = authorize_complete_multipart_upload(&mut envelope, &server, &bucket, "object", specific_etag).await;
        if allowed {
            result.expect("permitted completion");
        } else {
            assert_eq!(result.expect_err("completion refused").code(), &S3ErrorCode::AccessDenied);
        }
        assert_eq!(envelope.bucket_generation().is_some(), allowed);
        assert_eq!(envelope.req_info().unwrap().bucket.as_deref(), Some(bucket.as_str()));
        assert_eq!(envelope.req_info().unwrap().object.as_deref(), Some("object"));
    }
}
