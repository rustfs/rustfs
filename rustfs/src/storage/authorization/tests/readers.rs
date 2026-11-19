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

//! The state an access phase hands to a use case, read back through the `RequestEnvelope`: every
//! reader and installer gives its pinned answer with the state present and absent, and the
//! request-derived condition values come out of the envelope's request facts.
//!
//! Not covered here: policy decisions (`app/owned_authorization_test.rs`), or any parity with a
//! legacy request type.
//! Upstream: the readers and installers in `storage::authorization` and the envelope's
//! `AuthorizationRequest` impl; downstream: none.

use crate::app::RequestEnvelope;
use crate::shared_types::RemoteAddr;
use crate::storage::ObjectOptions;
use crate::storage::authorization::tests::fixture::{RequestBuilder, request};
use crate::storage::authorization::{
    AuthorizationRequest, BucketConfigMutationSnapshot, BucketGenerationGuard, CopySourceBucketGenerationGuard,
    OdmReadGenerationGuard, PendingDeleteBucketGenerationGuard, TableDataPlanePublicationGuards, apply_bucket_generation_guard,
    apply_copy_source_bucket_generation_guard, authorize_object_request, bucket_config_mutation_incarnation,
    capture_odm_read_generation, odm_read_generation, replication_request_authorized, request_context_from_req,
    resource_free_condition_values, retained_table_data_plane_publication_guards,
};
use crate::storage::req_info::ReqInfo;
use crate::storage::request_context::RequestContext;
use crate::storage::storage_api::runtime_sources_consumer::ServerContextSlot;
use http::header::{HeaderName, HeaderValue};
use http::{HeaderMap, Method};
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_s3_types::S3ErrorCode;
use rustfs_trusted_proxies::ClientInfo;
use std::collections::HashMap;
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

/// A GET of `/guarded-bucket/key` carrying what `fill` adds.
fn envelope(fill: impl FnOnce(RequestBuilder) -> RequestBuilder) -> RequestEnvelope {
    fill(request(Method::GET).uri("/guarded-bucket/key")).build()
}

/// Marks the request as one the server received, as the edge does by carrying its context.
fn server_request(builder: RequestBuilder) -> RequestBuilder {
    builder.server_context(&ServerContextSlot::new())
}

/// The incarnation a reader bound, or the refusal's code and message.
type Answer = Result<Option<uuid::Uuid>, (S3ErrorCode, String)>;

fn answer<T>(result: rustfs_s3_types::S3Result<T>, bound: impl FnOnce(T) -> Option<uuid::Uuid>) -> Answer {
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

/// Asserts that `read` gives `expected` for every case.
fn assert_cases(read: fn(&RequestEnvelope) -> Answer, cases: Vec<(&str, RequestEnvelope, Answer)>) {
    for (name, req, expected) in cases {
        assert_eq!(read(&req), expected, "{name}");
    }
}

#[test]
fn bucket_generation_guard_reads_the_same_through_both_forms() {
    let missing = "bucket generation guard is missing";
    let mismatch = "bucket generation guard does not match request bucket";
    assert_cases(
        bucket_fence,
        vec![
            (
                "captured guard binds",
                envelope(|req| req.extension(guard(BUCKET))),
                Ok(Some(incarnation())),
            ),
            (
                "pending-delete guard wins over the captured guard",
                envelope(|req| {
                    req.extension(guard("other-bucket"))
                        .extension(PendingDeleteBucketGenerationGuard::Ready(guard(BUCKET)))
                }),
                Ok(Some(incarnation())),
            ),
            (
                "guard of another bucket",
                envelope(|req| req.extension(guard("other-bucket"))),
                refused(S3ErrorCode::InternalError, mismatch),
            ),
            (
                "failed pending-delete capture",
                envelope(|req| {
                    req.extension(guard(BUCKET))
                        .extension(PendingDeleteBucketGenerationGuard::Failed {
                            code: S3ErrorCode::NoSuchBucket,
                            message: "bucket is gone".to_owned(),
                        })
                }),
                refused(S3ErrorCode::NoSuchBucket, "bucket is gone"),
            ),
            (
                "server request without a guard",
                envelope(server_request),
                refused(S3ErrorCode::InternalError, missing),
            ),
            ("direct use-case call without a guard", envelope(|req| req), Ok(None)),
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
        vec![
            (
                "captured guard binds",
                envelope(|req| req.extension(copy_guard(BUCKET))),
                Ok(Some(incarnation())),
            ),
            (
                "guard of another bucket",
                envelope(|req| req.extension(copy_guard("other-bucket"))),
                refused(
                    S3ErrorCode::InternalError,
                    "copy source bucket generation guard does not match request bucket",
                ),
            ),
            (
                "the destination guard does not stand in for the source",
                envelope(|req| server_request(req).extension(guard(BUCKET))),
                refused(S3ErrorCode::InternalError, "copy source bucket generation guard is missing"),
            ),
            ("direct use-case call without a guard", envelope(|req| req), Ok(None)),
        ],
    );
}

#[test]
fn odm_read_generation_reads_the_same_through_both_forms() {
    assert_cases(
        odm,
        vec![
            (
                "captured source generation",
                envelope(|req| req.extension(OdmReadGenerationGuard::Ready(guard(BUCKET)))),
                Ok(Some(incarnation())),
            ),
            (
                "source generation of another bucket",
                envelope(|req| req.extension(OdmReadGenerationGuard::Ready(guard("other-bucket")))),
                refused(S3ErrorCode::InternalError, "source generation guard does not match request bucket"),
            ),
            (
                "failed capture surfaces its error",
                envelope(|req| {
                    req.extension(OdmReadGenerationGuard::Failed {
                        code: S3ErrorCode::SlowDown,
                        message: "capture failed".to_owned(),
                    })
                }),
                refused(S3ErrorCode::SlowDown, "capture failed"),
            ),
            (
                "no source configured",
                envelope(|req| req.extension(OdmReadGenerationGuard::Unavailable)),
                Ok(None),
            ),
            ("nothing captured", envelope(|req| req), Ok(None)),
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
        vec![
            (
                "captured snapshot",
                envelope(|req| req.extension(snapshot(BUCKET))),
                Ok(Some(incarnation())),
            ),
            (
                "snapshot of another bucket",
                envelope(|req| req.extension(snapshot("other-bucket"))),
                refused(
                    S3ErrorCode::InternalError,
                    "bucket config mutation snapshot does not match request bucket",
                ),
            ),
            (
                "server request without a snapshot",
                envelope(server_request),
                refused(S3ErrorCode::InternalError, "bucket config mutation snapshot is missing"),
            ),
            ("direct use-case call without a snapshot", envelope(|req| req), Ok(None)),
        ],
    );
}

#[test]
fn installed_state_reads_back_through_the_envelope() {
    let mut envelope = envelope(server_request);
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
    let envelope = envelope(|req| req.extension(guard(BUCKET)));
    let captured = capture_odm_read_generation(&slot, &envelope, BUCKET).await;
    assert!(matches!(captured, OdmReadGenerationGuard::Unavailable), "{captured:?}");
}

#[test]
fn replication_authorization_flag_reads_the_same_through_both_forms() {
    let with_flag = |flag: bool| {
        envelope(move |req| {
            req.extension(ReqInfo {
                replication_request_authorized: flag,
                ..Default::default()
            })
        })
    };
    for (name, req, expected) in [
        ("authorized replication request", with_flag(true), true),
        ("ordinary request", with_flag(false), false),
        ("request without ReqInfo", envelope(|req| req), false),
    ] {
        assert_eq!(replication_request_authorized(&req), expected, "{name}");
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
        ("direct context", envelope(|req| req.extension(context("direct"))), Some("direct")),
        ("context inside ReqInfo", envelope(|req| req.extension(nested("nested"))), Some("nested")),
        (
            "the direct context wins",
            envelope(|req| req.extension(context("direct")).extension(nested("nested"))),
            Some("direct"),
        ),
        ("no context", envelope(|req| req), None),
    ] {
        let id = |found: Option<RequestContext>| found.map(|context| context.request_id);
        assert_eq!(id(request_context_from_req(&req)).as_deref(), expected, "{name}");
    }

    let with_guards = envelope(|req| req.extension(TableDataPlanePublicationGuards::default()));
    assert!(retained_table_data_plane_publication_guards(&with_guards).is_some());
    let without = envelope(|req| req);
    assert!(retained_table_data_plane_publication_guards(&without).is_none());
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
            envelope(|req| req.extension(Some(RemoteAddr(peer))).extension(ClientInfo::direct(client))),
            Some(vec!["198.51.100.7".to_owned()]),
        ),
        (
            "peer only",
            envelope(|req| req.extension(Some(RemoteAddr(peer)))),
            Some(vec!["192.0.2.2".to_owned()]),
        ),
    ] {
        assert_eq!(source_ip(&resource_free_condition_values(&req, &cred)), expected, "{name}");
    }
}

#[tokio::test]
async fn object_request_without_req_info_is_refused_in_both_forms() {
    let mut envelope = envelope(|req| req);
    let action = Action::S3Action(S3Action::GetObjectAclAction);
    let err = authorize_object_request(&mut envelope, BUCKET, "key", Some("v1"), action)
        .await
        .expect_err("a request without ReqInfo is refused");
    assert_eq!(err.code(), &S3ErrorCode::InternalError);
    assert_eq!(err.message(), Some("ReqInfo not found in request extensions"));
}

#[test]
fn a_forked_envelope_reads_like_the_original_and_installs_alone() {
    let original = envelope(|req| {
        server_request(req).extension(guard(BUCKET)).extension(ReqInfo {
            replication_request_authorized: true,
            ..Default::default()
        })
    });
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

#[test]
fn resource_free_conditions_preserve_legacy_projection_and_erased_keystone_session_facts() {
    for original_session in [None, Some(false), Some(true)] {
        for forwarded in [false, true] {
            let peer: SocketAddr = "192.0.2.9:9000".parse().unwrap();
            let client = forwarded.then(|| {
                let mut client = ClientInfo::direct("198.51.100.9:443".parse().unwrap());
                client.forwarded_proto = Some("https".into());
                client
            });
            let mut headers = HeaderMap::new();
            for (name, value) in [
                ("user-agent", "condition-client"),
                ("referer", "https://example.test/"),
                ("authorization", "original-authorization"),
                ("x-amz-security-token", "header-session"),
                ("x-amz-server-side-encryption-customer-key", "customer-key"),
                ("x-amz-copy-source-server-side-encryption-customer-key", "source-key"),
                ("x-amz-meta-tag", "retained-value"),
            ] {
                headers.insert(HeaderName::from_static(name), HeaderValue::from_static(value));
            }
            let original = rustfs_credentials::Credentials {
                access_key: "keystone:request-user".into(),
                parent_user: "request-parent".into(),
                session_token: if original_session == Some(true) {
                    "original-session".into()
                } else {
                    String::new()
                },
                expiration: Some(time::OffsetDateTime::now_utc() + time::Duration::hours(1)),
                groups: Some(vec!["request-group".into()]),
                claims: Some(HashMap::from([("keystone_roles".into(), serde_json::json!(["member"]))])),
                ..Default::default()
            };
            let mut credential = original.clone();
            if original_session.is_some() {
                credential.session_token.clear();
            }
            let mut builder = request(Method::GET)
                .uri("/guarded-bucket/key")
                .headers(headers.clone())
                .extension(Some(RemoteAddr(peer)))
                .extension(ReqInfo {
                    cred: Some(credential.clone()),
                    keystone_session_present: original_session,
                    ..Default::default()
                });
            if let Some(client) = client.clone() {
                builder = builder.extension(client);
            }
            let req = builder.build();
            let without_clock = |mut values: HashMap<String, Vec<String>>| {
                assert!(
                    time::OffsetDateTime::parse(
                        values.get("CurrentTime").unwrap()[0].as_str(),
                        &time::format_description::well_known::Rfc3339
                    )
                    .is_ok()
                );
                assert!(values.get("EpochTime").unwrap()[0].parse::<i64>().is_ok());
                values.remove("CurrentTime");
                values.remove("EpochTime");
                values
            };
            // The oracle reads the raw request parts, not the envelope.
            let expected = without_clock(crate::auth::get_condition_values_with_client_info(
                &headers,
                &original,
                None,
                None,
                Some(peer),
                client.as_ref(),
            ));
            let actual = without_clock(resource_free_condition_values(&req, &credential));
            assert_eq!(actual, expected, "session={original_session:?}, forwarded={forwarded}");
            assert_eq!(
                actual.get("username").unwrap(),
                &vec![
                    if original_session == Some(true) {
                        "request-parent"
                    } else {
                        "keystone:request-user"
                    }
                    .to_owned()
                ]
            );
        }
    }
}
