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

//! Wire behaviour of the replication and listen extension routes, driven
//! through the production admin router: claim, admission, authorization and
//! the first observable answer of each route.
//!
//! Not responsible for: the helpers behind each route (their own modules test
//! those) or object lambda, which is still served by the router itself.

use super::{AdminOperation, Body, S3ErrorCode, S3Request, S3Response, S3Result, S3Route, S3Router};
use crate::admin::storage_api::contract::bucket::MakeBucketOptions;
use crate::app::storage_api::s3::Credentials;
use http::{Extensions, HeaderMap, Method, StatusCode, header};
use http_body_util::BodyExt;
use rustfs_iam::store::{Store as _, UserType, object::ObjectStore};
use rustfs_madmin::{AccountStatus, AddOrUpdateUserReq};
use rustfs_notify::Event as NotificationEvent;
use rustfs_policy::policy::Policy;
use rustfs_s3_types::EventName;
use std::sync::Arc;
use std::time::Duration;

const SECRET: &str = "s3-extension-secret";

/// Every request shape the extension routes claim, with `{b}` standing for the bucket.
const ROUTES: [(Method, &str); 8] = [
    (Method::PUT, "/{b}?replication-reset"),
    (Method::GET, "/{b}?replication-reset-status"),
    (Method::GET, "/{b}?replication-metrics=2"),
    (Method::GET, "/{b}?replication-metrics"),
    (Method::GET, "/{b}?replication-check"),
    (Method::GET, "/?events=s3:ObjectCreated:*"),
    (Method::GET, "/{b}?events=s3:ObjectCreated:*"),
    (Method::GET, "/{b}/?events=s3:ObjectCreated:*"),
];

fn router() -> S3Router<AdminOperation> {
    let mut router = S3Router::new(false);
    crate::admin::register_admin_routes(&mut router).expect("production admin routes register");
    router
}

fn request(method: Method, uri: &str, access_key: Option<&str>) -> S3Request<Body> {
    S3Request {
        input: Body::empty(),
        method,
        uri: uri.parse().expect("test uri parses"),
        headers: HeaderMap::new(),
        extensions: Extensions::new(),
        credentials: access_key.map(|access_key| Credentials {
            access_key: access_key.to_owned(),
            secret_key: SECRET.into(),
        }),
        region: None,
        service: None,
        trailing_headers: None,
    }
}

fn uri_for(template: &str, bucket: &str) -> String {
    template.replace("{b}", bucket)
}

async fn send(router: &S3Router<AdminOperation>, mut req: S3Request<Body>) -> S3Result<S3Response<Body>> {
    router.check_access(&mut req).await?;
    router.call(req).await
}

fn error_code(result: S3Result<S3Response<Body>>, context: &str) -> S3ErrorCode {
    match result {
        Ok(response) => panic!("{context}: expected a refusal, got status {:?}", response.status),
        Err(error) => error.code().clone(),
    }
}

/// The callers the tests present, created in the process IAM under unique names.
struct Callers {
    no_policy: String,
    reader: String,
    full: String,
}

/// Creates the three callers in the ambient context's IAM, which every global
/// lookup on these routes resolves, and a bucket that exists without any
/// replication configuration.
async fn install_identities() -> (Callers, String) {
    let ambient = crate::app::gating_test_env::shared_gating_ambient().await;
    // Root credentials must exist for any key lookup; a context that already has them keeps its own.
    let _ = ambient.publish_action_credentials(rustfs_credentials::Credentials {
        access_key: "s3-extension-root".into(),
        secret_key: "s3-extension-root-secret".into(),
        status: "on".into(),
        ..Default::default()
    });
    // The shared store has no IAM format record until a test writes one; writing it again is harmless.
    ObjectStore::new(ambient.object_store())
        .save_iam_config(
            serde_json::json!({"version":1}),
            format!("{}/format.json", *rustfs_iam::store::object::IAM_CONFIG_PREFIX),
        )
        .await
        .expect("IAM format");
    let iam = ambient.iam().handle();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let callers = Callers {
        no_policy: format!("ext-no-policy-{suffix}"),
        reader: format!("ext-reader-{suffix}"),
        full: format!("ext-full-{suffix}"),
    };
    for (user, actions) in [
        (&callers.no_policy, None),
        (&callers.reader, Some(vec!["s3:GetReplicationConfiguration"])),
        (
            &callers.full,
            Some(vec![
                "s3:GetReplicationConfiguration",
                "s3:PutReplicationConfiguration",
                "s3:ResetBucketReplicationState",
                "s3:ListenNotification",
                "s3:ListenBucketNotification",
            ]),
        ),
    ] {
        iam.create_user(
            user,
            &AddOrUpdateUserReq {
                secret_key: SECRET.into(),
                policy: None,
                status: AccountStatus::Enabled,
            },
        )
        .await
        .expect("create user");
        if let Some(actions) = actions {
            let policy = serde_json::json!({"Version":"2012-10-17","Statement":[
                {"Effect":"Allow","Action":actions,"Resource":"arn:aws:s3:::*"}]});
            iam.set_policy(
                user,
                Policy::parse_config(&serde_json::to_vec(&policy).expect("policy JSON")).expect("policy"),
            )
            .await
            .expect("set policy");
            iam.policy_db_set(user, UserType::Reg, false, user)
                .await
                .expect("attach policy");
        }
    }
    let bucket = format!("ext-routes-{suffix}");
    crate::admin::storage_api::contract::bucket::BucketOperations::make_bucket(
        ambient.object_store().as_ref(),
        &bucket,
        &MakeBucketOptions::default(),
    )
    .await
    .expect("bucket");
    (callers, bucket)
}

#[tokio::test]
async fn extension_routes_refuse_anonymous_callers_before_dispatch() {
    let router = router();
    for (method, template) in ROUTES {
        let uri = uri_for(template, "demo-bucket");
        let mut req = request(method.clone(), &uri, None);
        assert!(
            router.is_match(&req.method, &req.uri, &req.headers, &mut req.extensions),
            "{method} {uri} must be claimed by the admin router"
        );
        let error = router
            .check_access(&mut req)
            .await
            .expect_err("an anonymous extension request is refused");
        assert_eq!(error.code(), &S3ErrorCode::AccessDenied, "{method} {uri}");
    }
}

#[tokio::test]
async fn extension_routes_are_not_claimed_for_other_methods_or_sub_resources() {
    let router = router();
    let headers = HeaderMap::new();
    for (method, uri) in [
        (Method::POST, "/demo-bucket?replication-metrics"),
        (Method::PUT, "/demo-bucket?replication-check"),
        (Method::GET, "/demo-bucket?replication-reset"),
        (Method::DELETE, "/demo-bucket?replication-reset"),
        (Method::PUT, "/demo-bucket?replication-reset-status"),
        (Method::GET, "/demo-bucket?replication-metrics=3"),
        (Method::GET, "/demo-bucket?replication-check=1"),
        (Method::GET, "/demo-bucket/object?replication-metrics"),
        (Method::GET, "/demo-bucket/object?events=s3:ObjectCreated:*"),
        (Method::PUT, "/?events=s3:ObjectCreated:*"),
        (Method::GET, "/demo-bucket?replication-status"),
    ] {
        let uri = uri.parse().expect("test uri parses");
        assert!(
            !router.is_match(&method, &uri, &headers, &mut Extensions::new()),
            "{method} {uri} must stay with the S3 service"
        );
    }
}

#[test]
#[serial_test::serial]
fn extension_routes_authorize_each_route_with_its_own_action() {
    crate::app::gating_test_env::run_large_stack_test("s3-extension-authz", || async {
        let (callers, bucket) = install_identities().await;
        let router = router();

        for (method, template) in ROUTES {
            let uri = uri_for(template, &bucket);
            let result = send(&router, request(method.clone(), &uri, Some(&callers.no_policy))).await;
            let code = error_code(result, &format!("no-policy {method} {uri}"));
            // Pinned current behaviour: an IAM denial of the bucketless listen route falls
            // through to a bucket-policy lookup for the empty bucket name, which fails as
            // InternalError rather than AccessDenied. Either way the stream is never opened.
            let expected = if template.starts_with("/?") {
                S3ErrorCode::InternalError
            } else {
                S3ErrorCode::AccessDenied
            };
            assert_eq!(code, expected, "no-policy {method} {uri}");
        }
        assert_eq!(S3ErrorCode::AccessDenied.status_code(), Some(StatusCode::FORBIDDEN));

        // The reader holds only s3:GetReplicationConfiguration: the metrics routes
        // reach the handler, every other route is refused on its own action.
        for (method, template, expected) in [
            (Method::PUT, "/{b}?replication-reset", S3ErrorCode::AccessDenied),
            (Method::GET, "/{b}?replication-check", S3ErrorCode::AccessDenied),
            (Method::GET, "/?events=s3:ObjectCreated:*", S3ErrorCode::InternalError),
            (Method::GET, "/{b}?events=s3:ObjectCreated:*", S3ErrorCode::AccessDenied),
            (Method::GET, "/{b}?replication-reset-status", S3ErrorCode::AccessDenied),
            (
                Method::GET,
                "/{b}?replication-metrics=2",
                S3ErrorCode::ReplicationConfigurationNotFoundError,
            ),
            (
                Method::GET,
                "/{b}?replication-metrics",
                S3ErrorCode::ReplicationConfigurationNotFoundError,
            ),
        ] {
            let uri = uri_for(template, &bucket);
            let result = send(&router, request(method.clone(), &uri, Some(&callers.reader))).await;
            assert_eq!(error_code(result, &format!("reader {method} {uri}")), expected, "reader {method} {uri}");
        }

        // The fully permitted caller reaches each handler's first decision.
        for (method, template, expected) in [
            (Method::PUT, "/{b}?replication-reset", S3ErrorCode::ReplicationConfigurationNotFoundError),
            (
                Method::GET,
                "/{b}?replication-reset-status",
                S3ErrorCode::ReplicationConfigurationNotFoundError,
            ),
            (
                Method::GET,
                "/{b}?replication-metrics=2",
                S3ErrorCode::ReplicationConfigurationNotFoundError,
            ),
            (
                Method::GET,
                "/{b}?replication-metrics",
                S3ErrorCode::ReplicationConfigurationNotFoundError,
            ),
            (Method::GET, "/{b}?replication-check", S3ErrorCode::InvalidRequest),
            (Method::GET, "/missing-{b}?replication-metrics", S3ErrorCode::NoSuchBucket),
            (Method::GET, "/missing-{b}?events=s3:ObjectCreated:*", S3ErrorCode::NoSuchBucket),
            (Method::GET, "/{b}?events=not-an-event", S3ErrorCode::InvalidArgument),
        ] {
            let uri = uri_for(template, &bucket);
            let result = send(&router, request(method.clone(), &uri, Some(&callers.full))).await;
            assert_eq!(error_code(result, &format!("full {method} {uri}")), expected, "full {method} {uri}");
        }

        for template in [
            "/?events=s3:ObjectCreated:*",
            "/{b}?events=s3:ObjectCreated:*",
            "/{b}/?events=s3:ObjectCreated:*",
        ] {
            let uri = uri_for(template, &bucket);
            let response = send(&router, request(Method::GET, &uri, Some(&callers.full)))
                .await
                .unwrap_or_else(|error| panic!("full GET {uri} is served: {error:?}"));
            assert_eq!(response.status, Some(StatusCode::OK), "{uri}");
            assert_eq!(
                response.headers.get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()),
                Some("text/event-stream"),
                "{uri}"
            );
        }
    });
}

#[test]
#[serial_test::serial]
fn listen_stream_stays_open_delivers_a_later_event_and_ends_when_the_client_leaves() {
    crate::app::gating_test_env::run_large_stack_test("s3-extension-listen", || async {
        let (callers, bucket) = install_identities().await;
        let router = router();
        let system = rustfs_notify::ensure_live_events();
        assert!(!system.has_live_listeners(), "no listener before the request");

        let uri = format!("/{bucket}?events=s3:ObjectCreated:*");
        let response = send(&router, request(Method::GET, &uri, Some(&callers.full)))
            .await
            .expect("permitted listen request is served");
        assert_eq!(response.status, Some(StatusCode::OK));
        assert_eq!(
            response.headers.get(header::CACHE_CONTROL).and_then(|v| v.to_str().ok()),
            Some("no-cache")
        );
        assert_eq!(response.headers.get("x-accel-buffering").and_then(|v| v.to_str().ok()), Some("no"));
        assert!(system.has_live_listeners(), "the open stream subscribes to live events");
        let mut body = response.output;

        // Nothing is due yet: the stream neither ends nor yields on its own.
        assert!(
            tokio::time::timeout(Duration::from_millis(200), body.frame()).await.is_err(),
            "a fresh listen stream must stay pending"
        );

        system
            .send_event(Arc::new(NotificationEvent::new_test_event(
                "unrelated-bucket",
                "skipped.txt",
                EventName::ObjectCreatedPut,
            )))
            .await;
        system
            .send_event(Arc::new(NotificationEvent::new_test_event(
                &bucket,
                "later.txt",
                EventName::ObjectCreatedPut,
            )))
            .await;
        let delivered = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let frame = body
                    .frame()
                    .await
                    .expect("the stream stays open until the event arrives")
                    .expect("stream frames carry no error");
                let Ok(data) = frame.into_data() else { continue };
                if data.iter().all(u8::is_ascii_whitespace) {
                    continue;
                }
                break String::from_utf8(data.to_vec()).expect("event payload is UTF-8");
            }
        })
        .await
        .expect("the later event is delivered");
        assert!(delivered.contains("later.txt"), "{delivered}");
        assert!(!delivered.contains("skipped.txt"), "{delivered}");
        assert!(delivered.ends_with('\n'));

        drop(body);
        tokio::time::timeout(Duration::from_secs(5), async {
            while system.has_live_listeners() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the producer releases its subscription once the client is gone");
    });
}
