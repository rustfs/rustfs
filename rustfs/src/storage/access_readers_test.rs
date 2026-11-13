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

//! Owned-versus-legacy parity of the state an access hook hands to a use case: every reader and
//! installer must answer the same through an s3s request and through the `RequestEnvelope` built
//! from it, with the state present and absent, and each answer is pinned to its expected value so
//! the parity cannot hold vacuously.
//!
//! Not covered here: policy decisions (`app/owned_authorization_test.rs`).
//! Upstream: the readers in `storage/access.rs`; downstream: none.

use super::{
    Action, AuthorizationRequest, BucketConfigMutationSnapshot, BucketGenerationGuard, CopySourceBucketGenerationGuard, HashMap,
    HeaderMap, ObjectOptions, OdmReadGenerationGuard, PendingDeleteBucketGenerationGuard, RemoteAddr, ReqInfo, RequestContext,
    S3Action, S3ErrorCode, S3Request, S3Result, TableDataPlanePublicationGuards, apply_bucket_generation_guard,
    apply_copy_source_bucket_generation_guard, authorize_object_request, bucket_config_mutation_incarnation,
    capture_odm_read_generation, odm_read_generation, replication_request_authorized, request_context_from_req,
    resource_free_condition_values, retained_table_data_plane_publication_guards,
};
use crate::app::RequestEnvelope;
use crate::runtime_sources::ServerContextSlot;
use rustfs_trusted_proxies::ClientInfo;
use std::net::SocketAddr;

const BUCKET: &str = "guarded-bucket";

fn incarnation() -> uuid::Uuid {
    uuid::Uuid::from_u128(0x7107)
}

fn guard(bucket: &str) -> BucketGenerationGuard {
    BucketGenerationGuard {
        bucket: bucket.to_owned(),
        incarnation_id: incarnation(),
    }
}

fn legacy(fill: impl FnOnce(&mut http::Extensions)) -> S3Request<()> {
    let mut extensions = http::Extensions::new();
    fill(&mut extensions);
    S3Request {
        input: (),
        method: http::Method::GET,
        uri: "/guarded-bucket/key".parse().expect("static uri"),
        headers: HeaderMap::new(),
        extensions,
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

fn server_request(extensions: &mut http::Extensions) {
    extensions.insert(ServerContextSlot::new());
}

/// The incarnation a reader bound, or the refusal's code and message.
type Answer = Result<Option<uuid::Uuid>, (S3ErrorCode, String)>;

fn answer<T>(result: S3Result<T>, bound: impl FnOnce(T) -> Option<uuid::Uuid>) -> Answer {
    result
        .map(bound)
        .map_err(|err| (err.code().clone(), err.message().unwrap_or_default().to_owned()))
}

fn refused(code: S3ErrorCode, message: &str) -> Answer {
    Err((code, message.to_owned()))
}

fn bucket_fence<R: AuthorizationRequest + ?Sized>(req: &R) -> Answer {
    let mut opts = ObjectOptions::default();
    let result = apply_bucket_generation_guard(req, BUCKET, &mut opts);
    answer(result, |()| opts.expected_bucket_incarnation_id)
}

fn copy_source_fence<R: AuthorizationRequest + ?Sized>(req: &R) -> Answer {
    let mut opts = ObjectOptions::default();
    let result = apply_copy_source_bucket_generation_guard(req, BUCKET, &mut opts);
    answer(result, |()| opts.expected_bucket_incarnation_id)
}

fn odm<R: AuthorizationRequest + ?Sized>(req: &R) -> Answer {
    answer(odm_read_generation(req, BUCKET), |id| id)
}

fn config_mutation<R: AuthorizationRequest + ?Sized>(req: &R) -> Answer {
    answer(bucket_config_mutation_incarnation(req, BUCKET), |id| id)
}

/// Asserts that `read` gives `expected` through both request forms of every case.
fn assert_cases(
    read: fn(&S3Request<()>) -> Answer,
    owned: fn(&RequestEnvelope) -> Answer,
    cases: Vec<(&str, S3Request<()>, Answer)>,
) {
    for (name, req, expected) in cases {
        let envelope = RequestEnvelope::from_s3s(&req);
        assert_eq!(read(&req), expected, "legacy form: {name}");
        assert_eq!(owned(&envelope), expected, "owned form: {name}");
    }
}

#[test]
fn bucket_generation_guard_reads_the_same_through_both_forms() {
    let missing = "bucket generation guard is missing";
    let mismatch = "bucket generation guard does not match request bucket";
    assert_cases(
        bucket_fence,
        bucket_fence,
        vec![
            (
                "captured guard binds",
                legacy(|ext| {
                    ext.insert(guard(BUCKET));
                }),
                Ok(Some(incarnation())),
            ),
            (
                "pending-delete guard wins over the captured guard",
                legacy(|ext| {
                    ext.insert(guard("other-bucket"));
                    ext.insert(PendingDeleteBucketGenerationGuard::Ready(guard(BUCKET)));
                }),
                Ok(Some(incarnation())),
            ),
            (
                "guard of another bucket",
                legacy(|ext| {
                    ext.insert(guard("other-bucket"));
                }),
                refused(S3ErrorCode::InternalError, mismatch),
            ),
            (
                "failed pending-delete capture",
                legacy(|ext| {
                    ext.insert(guard(BUCKET));
                    ext.insert(PendingDeleteBucketGenerationGuard::Failed {
                        code: S3ErrorCode::NoSuchBucket,
                        message: "bucket is gone".to_owned(),
                    });
                }),
                refused(S3ErrorCode::NoSuchBucket, "bucket is gone"),
            ),
            (
                "server request without a guard",
                legacy(server_request),
                refused(S3ErrorCode::InternalError, missing),
            ),
            ("direct use-case call without a guard", legacy(|_| {}), Ok(None)),
        ],
    );
}

#[test]
fn copy_source_generation_guard_reads_the_same_through_both_forms() {
    let copy_guard = |bucket: &str| CopySourceBucketGenerationGuard {
        bucket: bucket.to_owned(),
        incarnation_id: incarnation(),
    };
    assert_cases(
        copy_source_fence,
        copy_source_fence,
        vec![
            (
                "captured guard binds",
                legacy(|ext| {
                    ext.insert(copy_guard(BUCKET));
                }),
                Ok(Some(incarnation())),
            ),
            (
                "guard of another bucket",
                legacy(|ext| {
                    ext.insert(copy_guard("other-bucket"));
                }),
                refused(
                    S3ErrorCode::InternalError,
                    "copy source bucket generation guard does not match request bucket",
                ),
            ),
            (
                "the destination guard does not stand in for the source",
                legacy(|ext| {
                    server_request(ext);
                    ext.insert(guard(BUCKET));
                }),
                refused(S3ErrorCode::InternalError, "copy source bucket generation guard is missing"),
            ),
            ("direct use-case call without a guard", legacy(|_| {}), Ok(None)),
        ],
    );
}

#[test]
fn odm_read_generation_reads_the_same_through_both_forms() {
    assert_cases(
        odm,
        odm,
        vec![
            (
                "captured source generation",
                legacy(|ext| {
                    ext.insert(OdmReadGenerationGuard::Ready(guard(BUCKET)));
                }),
                Ok(Some(incarnation())),
            ),
            (
                "source generation of another bucket",
                legacy(|ext| {
                    ext.insert(OdmReadGenerationGuard::Ready(guard("other-bucket")));
                }),
                refused(S3ErrorCode::InternalError, "source generation guard does not match request bucket"),
            ),
            (
                "failed capture surfaces its error",
                legacy(|ext| {
                    ext.insert(OdmReadGenerationGuard::Failed {
                        code: S3ErrorCode::SlowDown,
                        message: "capture failed".to_owned(),
                    });
                }),
                refused(S3ErrorCode::SlowDown, "capture failed"),
            ),
            (
                "no source configured",
                legacy(|ext| {
                    ext.insert(OdmReadGenerationGuard::Unavailable);
                }),
                Ok(None),
            ),
            ("nothing captured", legacy(|_| {}), Ok(None)),
        ],
    );
}

#[test]
fn bucket_config_mutation_incarnation_reads_the_same_through_both_forms() {
    let snapshot = |bucket: &str| BucketConfigMutationSnapshot {
        bucket: bucket.to_owned(),
        incarnation_id: incarnation(),
    };
    assert_cases(
        config_mutation,
        config_mutation,
        vec![
            (
                "captured snapshot",
                legacy(|ext| {
                    ext.insert(snapshot(BUCKET));
                }),
                Ok(Some(incarnation())),
            ),
            (
                "snapshot of another bucket",
                legacy(|ext| {
                    ext.insert(snapshot("other-bucket"));
                }),
                refused(
                    S3ErrorCode::InternalError,
                    "bucket config mutation snapshot does not match request bucket",
                ),
            ),
            (
                "server request without a snapshot",
                legacy(server_request),
                refused(S3ErrorCode::InternalError, "bucket config mutation snapshot is missing"),
            ),
            ("direct use-case call without a snapshot", legacy(|_| {}), Ok(None)),
        ],
    );
}

#[test]
fn installed_state_reads_back_through_the_envelope() {
    let mut envelope = RequestEnvelope::from_s3s(&legacy(server_request));
    assert_eq!(
        bucket_fence(&envelope),
        refused(S3ErrorCode::InternalError, "bucket generation guard is missing")
    );
    envelope.install_bucket_generation(guard(BUCKET));
    assert_eq!(bucket_fence(&envelope), Ok(Some(incarnation())));
    envelope.install_pending_delete_bucket_generation(PendingDeleteBucketGenerationGuard::Failed {
        code: S3ErrorCode::NoSuchBucket,
        message: "bucket is gone".to_owned(),
    });
    assert_eq!(bucket_fence(&envelope), refused(S3ErrorCode::NoSuchBucket, "bucket is gone"));

    assert_eq!(odm(&envelope), Ok(None));
    envelope.install_odm_read_generation(OdmReadGenerationGuard::Ready(guard("other-bucket")));
    assert_eq!(
        odm(&envelope),
        refused(S3ErrorCode::InternalError, "source generation guard does not match request bucket")
    );
}

#[tokio::test]
async fn odm_capture_without_a_configured_source_is_unavailable_in_both_forms() {
    let slot = ServerContextSlot::new();
    let req = legacy(|ext| {
        ext.insert(guard(BUCKET));
    });
    let envelope = RequestEnvelope::from_s3s(&req);
    for captured in [
        capture_odm_read_generation(&slot, &req, BUCKET).await,
        capture_odm_read_generation(&slot, &envelope, BUCKET).await,
    ] {
        assert!(matches!(captured, OdmReadGenerationGuard::Unavailable), "{captured:?}");
    }
}

#[test]
fn replication_authorization_flag_reads_the_same_through_both_forms() {
    let with_flag = |flag: bool| {
        legacy(move |ext| {
            ext.insert(ReqInfo {
                replication_request_authorized: flag,
                ..Default::default()
            });
        })
    };
    for (name, req, expected) in [
        ("authorized replication request", with_flag(true), true),
        ("ordinary request", with_flag(false), false),
        ("request without ReqInfo", legacy(|_| {}), false),
    ] {
        let envelope = RequestEnvelope::from_s3s(&req);
        assert_eq!(replication_request_authorized(&req), expected, "legacy form: {name}");
        assert_eq!(replication_request_authorized(&envelope), expected, "owned form: {name}");
    }
}

#[test]
fn publication_guards_and_request_context_read_the_same_through_both_forms() {
    let context = |id: &str| RequestContext {
        request_id: id.to_owned(),
        x_amz_request_id: id.to_owned(),
        trace_id: None,
        span_id: None,
        start_time: std::time::Instant::now(),
    };
    let nested = |id: &str| ReqInfo {
        request_context: Some(context(id)),
        ..Default::default()
    };
    for (name, req, expected) in [
        (
            "direct context",
            legacy(|ext| {
                ext.insert(context("direct"));
            }),
            Some("direct"),
        ),
        (
            "context inside ReqInfo",
            legacy(|ext| {
                ext.insert(nested("nested"));
            }),
            Some("nested"),
        ),
        (
            "the direct context wins",
            legacy(|ext| {
                ext.insert(context("direct"));
                ext.insert(nested("nested"));
            }),
            Some("direct"),
        ),
        ("no context", legacy(|_| {}), None),
    ] {
        let envelope = RequestEnvelope::from_s3s(&req);
        let id = |found: Option<RequestContext>| found.map(|context| context.request_id);
        assert_eq!(id(request_context_from_req(&req)).as_deref(), expected, "legacy form: {name}");
        assert_eq!(id(request_context_from_req(&envelope)).as_deref(), expected, "owned form: {name}");
    }

    let with_guards = legacy(|ext| {
        ext.insert(TableDataPlanePublicationGuards::default());
    });
    assert!(retained_table_data_plane_publication_guards(&with_guards).is_some());
    assert!(retained_table_data_plane_publication_guards(&RequestEnvelope::from_s3s(&with_guards)).is_some());
    let without = legacy(|_| {});
    assert!(retained_table_data_plane_publication_guards(&without).is_none());
    assert!(retained_table_data_plane_publication_guards(&RequestEnvelope::from_s3s(&without)).is_none());
}

#[test]
fn resource_free_conditions_read_the_same_client_through_both_forms() {
    let peer: SocketAddr = "192.0.2.2:9000".parse().expect("peer address");
    let client: SocketAddr = "198.51.100.7:443".parse().expect("client address");
    let cred = rustfs_credentials::Credentials {
        access_key: "condition-user".to_owned(),
        ..Default::default()
    };
    let source_ip = |conditions: &HashMap<String, Vec<String>>| conditions.get("SourceIp").cloned();
    for (name, req, expected) in [
        (
            "forwarded client wins over the peer",
            legacy(|ext| {
                ext.insert(Some(RemoteAddr(peer)));
                ext.insert(ClientInfo::direct(client));
            }),
            Some(vec!["198.51.100.7".to_owned()]),
        ),
        (
            "peer only",
            legacy(|ext| {
                ext.insert(Some(RemoteAddr(peer)));
            }),
            Some(vec!["192.0.2.2".to_owned()]),
        ),
    ] {
        let envelope = RequestEnvelope::from_s3s(&req);
        // The clock values are read at each call; every request-derived value must match.
        let without_clock = |mut conditions: HashMap<String, Vec<String>>| {
            conditions.remove("CurrentTime");
            conditions.remove("EpochTime");
            conditions
        };
        let legacy_conditions = without_clock(resource_free_condition_values(&req, &cred));
        assert_eq!(
            legacy_conditions,
            without_clock(resource_free_condition_values(&envelope, &cred)),
            "{name}"
        );
        assert_eq!(source_ip(&legacy_conditions), expected, "{name}");
    }
}

#[tokio::test]
async fn object_request_without_req_info_is_refused_in_both_forms() {
    let mut req = legacy(|_| {});
    let mut envelope = RequestEnvelope::from_s3s(&req);
    let action = Action::S3Action(S3Action::GetObjectAclAction);
    for result in [
        authorize_object_request(&mut req, BUCKET, "key", Some("v1"), action).await,
        authorize_object_request(&mut envelope, BUCKET, "key", Some("v1"), action).await,
    ] {
        let err = result.expect_err("a request without ReqInfo is refused");
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
        assert_eq!(err.message(), Some("ReqInfo not found in request extensions"));
    }
}

#[test]
fn a_forked_envelope_reads_like_the_original_and_installs_alone() {
    let original = RequestEnvelope::from_s3s(&legacy(|ext| {
        server_request(ext);
        ext.insert(guard(BUCKET));
        ext.insert(ReqInfo {
            replication_request_authorized: true,
            ..Default::default()
        });
    }));
    let mut fork = original.fork_for_authorization();
    assert_eq!(bucket_fence(&fork), bucket_fence(&original));
    assert_eq!(bucket_fence(&fork), Ok(Some(incarnation())));
    assert!(replication_request_authorized(&fork));

    fork.install_bucket_generation(guard("other-bucket"));
    fork.install_odm_read_generation(OdmReadGenerationGuard::Ready(guard(BUCKET)));
    if let Some(req_info) = fork.req_info_mut() {
        req_info.object = Some("member".to_owned());
    }
    assert_eq!(
        bucket_fence(&fork),
        refused(S3ErrorCode::InternalError, "bucket generation guard does not match request bucket")
    );
    assert_eq!(bucket_fence(&original), Ok(Some(incarnation())));
    assert_eq!(odm(&original), Ok(None));
    assert_eq!(original.req_info().and_then(|info| info.object.clone()), None);
}
