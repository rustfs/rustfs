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

use super::*;
use crate::admin::operation::test_support::TestRequest;
use crate::admin::runtime_sources::ServerContextSlot;
use crate::admin::storage_api::access::ReqInfo;
use crate::admin::storage_api::contract::bucket::BucketOperations as _;
use http::Method;
use std::sync::Arc;

#[test]
fn validate_listen_notification_query_accepts_valid_values() {
    let uri: Uri = "/demo-bucket?events=s3:ObjectCreated:*&prefix=logs/&suffix=.json&ping=3"
        .parse()
        .expect("uri should parse");

    assert!(validate_listen_notification_query(&uri).is_ok());
}

#[test]
fn validate_listen_notification_query_rejects_invalid_event_or_duplicate_filters() {
    let invalid_event: Uri = "/demo-bucket?events=invalid-event".parse().expect("uri should parse");
    let duplicate_prefix: Uri = "/demo-bucket?events=s3:ObjectCreated:*&prefix=a&prefix=b"
        .parse()
        .expect("uri should parse");
    let invalid_ping: Uri = "/demo-bucket?events=s3:ObjectCreated:*&ping=0"
        .parse()
        .expect("uri should parse");

    assert_eq!(
        validate_listen_notification_query(&invalid_event)
            .expect_err("invalid event should fail")
            .code(),
        &S3ErrorCode::InvalidArgument
    );
    assert_eq!(
        validate_listen_notification_query(&duplicate_prefix)
            .expect_err("duplicate prefix should fail")
            .code(),
        &S3ErrorCode::InvalidArgument
    );
    assert_eq!(
        validate_listen_notification_query(&invalid_ping)
            .expect_err("invalid ping should fail")
            .code(),
        &S3ErrorCode::InvalidArgument
    );
}

#[test]
fn listen_notification_keepalive_plan_defaults_to_space_keepalive() {
    let uri: Uri = "/demo-bucket?events=s3:ObjectCreated:Put".parse().expect("uri should parse");
    let (interval, payload) = listen_notification_keepalive_plan(&uri);
    assert_eq!(interval, Duration::from_millis(500));
    assert_eq!(payload, Bytes::from_static(b" "));
}

#[test]
fn listen_notification_keepalive_plan_uses_empty_record_payload_when_ping_is_present() {
    let uri: Uri = "/demo-bucket?events=s3:ObjectCreated:Put&ping=3"
        .parse()
        .expect("uri should parse");
    let (interval, payload) = listen_notification_keepalive_plan(&uri);
    assert_eq!(interval, Duration::from_secs(3));
    assert_eq!(payload, Bytes::from_static(b"{\"Records\":[]}\n"));
}

#[test]
fn parse_listen_notification_filter_expands_event_mask_and_filters() {
    let uri: Uri = "/demo-bucket?events=s3:ObjectCreated:*&events=s3:ObjectRemoved:Delete&prefix=logs/&suffix=.json"
        .parse()
        .expect("uri should parse");

    let filter = parse_listen_notification_filter(&uri, Some("demo-bucket")).expect("filter should parse");

    assert_eq!(filter.bucket.as_deref(), Some("demo-bucket"));
    assert_eq!(filter.prefix.as_deref(), Some("logs/"));
    assert_eq!(filter.suffix.as_deref(), Some(".json"));
    assert_ne!(filter.event_mask & EventName::ObjectCreatedPut.mask(), 0);
    assert_ne!(filter.event_mask & EventName::ObjectRemovedDelete.mask(), 0);
    assert_eq!(filter.event_mask & EventName::ObjectAccessedGet.mask(), 0);
}

#[test]
fn event_matches_listen_notification_respects_bucket_event_and_object_filters() {
    let filter = ListenNotificationFilter {
        bucket: Some("demo-bucket".to_string()),
        event_mask: rustfs_s3_ops::put_object_created_event_mask(),
        prefix: Some("logs/".to_string()),
        suffix: Some(".json".to_string()),
    };

    let matched = NotificationEvent::new_test_event("demo-bucket", "logs/app.json", EventName::ObjectCreatedPut);
    assert!(event_matches_listen_notification(&matched, &filter));

    let wrong_bucket = NotificationEvent::new_test_event("other-bucket", "logs/app.json", EventName::ObjectCreatedPut);
    assert!(!event_matches_listen_notification(&wrong_bucket, &filter));

    let wrong_event = NotificationEvent::new_test_event("demo-bucket", "logs/app.json", EventName::ObjectRemovedDelete);
    assert!(!event_matches_listen_notification(&wrong_event, &filter));

    let wrong_prefix = NotificationEvent::new_test_event("demo-bucket", "archive/app.json", EventName::ObjectCreatedPut);
    assert!(!event_matches_listen_notification(&wrong_prefix, &filter));

    let wrong_suffix = NotificationEvent::new_test_event("demo-bucket", "logs/app.txt", EventName::ObjectCreatedPut);
    assert!(!event_matches_listen_notification(&wrong_suffix, &filter));
}

#[test]
fn event_matches_listen_notification_decodes_object_key_before_filtering() {
    let filter = ListenNotificationFilter {
        bucket: Some("demo-bucket".to_string()),
        event_mask: EventName::ObjectCreatedPut.mask(),
        prefix: Some("logs/".to_string()),
        suffix: Some(".json".to_string()),
    };

    let encoded = NotificationEvent::new_test_event("demo-bucket", "logs%2Fapp.json", EventName::ObjectCreatedPut);
    assert!(event_matches_listen_notification(&encoded, &filter));
}

#[test]
fn serialize_listen_notification_event_wraps_records_payload() {
    let event = NotificationEvent::new_test_event("demo-bucket", "logs/app.json", EventName::ObjectCreatedPut);

    let payload = serialize_listen_notification_event(&event).expect("payload should serialize");
    let body = std::str::from_utf8(payload.as_ref()).expect("payload should be utf-8");

    assert!(body.contains("\"Records\":["));
    assert!(body.contains("\"name\":\"demo-bucket\""));
    assert!(body.contains("\"eventName\":\"ObjectCreatedPut\"") || body.contains("s3:ObjectCreated:Put"));
    assert!(body.ends_with('\n'));
}

#[test]
#[serial_test::serial]
fn listen_notification_streams_close_after_identity_policy_or_bucket_revocation() {
    crate::app::gating_test_env::run_large_stack_test("listen-revocation", || async {
        use crate::admin::runtime_sources::{AppContext, IamInterface};
        use crate::admin::storage_api::contract::bucket::{DeleteBucketOptions, MakeBucketOptions};
        use rustfs_iam::store::{Store as _, UserType, object::ObjectStore};
        use rustfs_madmin::{AccountStatus, AddOrUpdateUserReq};
        use rustfs_policy::policy::Policy;
        struct ReadyIam(Arc<rustfs_iam::sys::IamSys<ObjectStore>>);
        impl IamInterface for ReadyIam {
            fn handle(&self) -> Arc<rustfs_iam::sys::IamSys<ObjectStore>> {
                Arc::clone(&self.0)
            }
            fn is_ready(&self) -> bool {
                true
            }
        }
        let (_directory, _paths, store) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
        ObjectStore::new(store.clone())
            .save_iam_config(
                serde_json::json!({"version":1}),
                format!("{}/format.json", *rustfs_iam::store::object::IAM_CONFIG_PREFIX),
            )
            .await
            .expect("IAM format");
        let iam = rustfs_iam::build_iam_sys(store.clone()).await.expect("IAM");
        let ambient = crate::app::gating_test_env::shared_gating_ambient().await;
        let context = Arc::new(AppContext::new(store.clone(), Arc::new(ReadyIam(iam.clone())), ambient.kms()));
        assert!(context.publish_action_credentials(rustfs_credentials::Credentials {
            access_key: "listen-test-root".into(),
            secret_key: "listen-root-secret".into(),
            status: "on".into(),
            ..Default::default()
        }));
        let slot = ServerContextSlot::new();
        assert!(slot.install(context));
        for (case, bucket_scoped) in [
            ("detach", false),
            ("disable", true),
            ("delete", false),
            ("rotate", true),
            ("recreate", true),
            ("expire", true),
        ] {
            let key = format!("listen-{case}");
            let bucket = format!("listen-{case}-{}", uuid::Uuid::new_v4());
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("bucket");
            iam.create_user(
                &key,
                &AddOrUpdateUserReq {
                    secret_key: "listen-user-secret".into(),
                    policy: None,
                    status: AccountStatus::Enabled,
                },
            )
            .await
            .expect("listener");
            let policy = serde_json::json!({"Version":"2012-10-17","Statement":[{"Effect":"Allow",
                "Action":["s3:ListenNotification","s3:ListenBucketNotification"],"Resource":"arn:aws:s3:::*"}]});
            iam.set_policy(
                &key,
                Policy::parse_config(&serde_json::to_vec(&policy).expect("policy JSON")).expect("policy"),
            )
            .await
            .expect("set policy");
            iam.policy_db_set(&key, UserType::Reg, false, &key)
                .await
                .expect("attach policy");
            let temporary = if case == "expire" {
                let claims = std::collections::HashMap::from([
                    (
                        "exp".to_string(),
                        serde_json::json!((time::OffsetDateTime::now_utc() + time::Duration::seconds(5)).unix_timestamp()),
                    ),
                    ("parent".to_string(), serde_json::json!(key)),
                ]);
                let mut cred = rustfs_policy::auth::get_new_credentials_with_metadata(&claims, "listen-root-secret")
                    .expect("signed temporary credentials");
                cred.parent_user = key.clone();
                iam.set_temp_user(&cred.access_key, &cred, None)
                    .await
                    .expect("temporary listener");
                Some(cred)
            } else {
                None
            };
            let uri: Uri = if bucket_scoped {
                format!("/{bucket}?events=s3:ObjectCreated:*&ping=1")
            } else {
                "/?events=s3:ObjectCreated:*&ping=1".to_string()
            }
            .parse()
            .expect("listen URI");
            let mut request = TestRequest::new(Method::GET, uri.clone())
                .extension(Arc::clone(&slot))
                .extension(ReqInfo {
                    bucket: bucket_scoped.then(|| bucket.clone()),
                    ..Default::default()
                });
            request = match &temporary {
                Some(cred) => request.header("x-amz-security-token", &cred.session_token).credentials(
                    &cred.access_key,
                    &cred.secret_key,
                    None,
                ),
                None => request.credentials(&key, "listen-user-secret", None),
            };
            let action = Action::S3Action(if bucket_scoped {
                S3Action::ListenBucketNotificationAction
            } else {
                S3Action::ListenNotificationAction
            });
            let mut authorization = ListenNotificationAuthorization::capture(
                request.build().into_envelope(),
                bucket_scoped.then_some(bucket.as_str()),
                action,
            )
            .await
            .expect("admit subscription");
            assert!(authorization.still_allowed().await, "initial listener {case}");
            // Fill a real stream's output channel, then revoke while its next
            // send is waiting. A consumer pause cannot defer reauthorization.
            let (tx, mut rx) = mpsc::channel(1);
            assert!(authorization.send(&tx, Bytes::from_static(b"before")).await);
            match case {
                "expire" => {
                    tokio::time::sleep(Duration::from_secs(6)).await;
                }
                "detach" => {
                    iam.policy_db_set(&key, UserType::Reg, false, "").await.expect("detach");
                }
                "disable" => {
                    iam.set_user_status(&key, AccountStatus::Disabled).await.expect("disable");
                }
                "delete" => {
                    iam.delete_user(&key, false).await.expect("delete user");
                }
                "rotate" => {
                    iam.rotate_user_secret_key(&key, "replacement-listen-secret")
                        .await
                        .expect("rotate");
                }
                "recreate" => {
                    store
                        .delete_bucket(&bucket, &DeleteBucketOptions::default())
                        .await
                        .expect("delete bucket");
                    store
                        .make_bucket(&bucket, &MakeBucketOptions::default())
                        .await
                        .expect("recreate bucket");
                }
                _ => unreachable!(),
            }
            assert!(
                !tokio::time::timeout(Duration::from_secs(8), authorization.send(&tx, Bytes::from_static(b"after")))
                    .await
                    .expect("revocation must retire a backpressured stream"),
                "{case}"
            );
            assert_eq!(rx.recv().await.expect("previous event").expect("bytes"), Bytes::from_static(b"before"));
            assert!(rx.try_recv().is_err(), "revoked event leaked: {case}");
            let response = build_listen_notification_response(&uri, bucket_scoped.then_some(bucket.as_str()), authorization)
                .expect("stream headers");
            tokio::time::timeout(Duration::from_secs(8), http_body_util::BodyExt::collect(response.into_parts().2))
                .await
                .expect("an idle revoked stream must terminate")
                .expect("stream ends cleanly");
        }
    });
}

#[tokio::test]
async fn build_listen_notification_response_sets_event_stream_headers() {
    let uri: Uri = "/demo-bucket?events=s3:ObjectCreated:Put&ping=1"
        .parse()
        .expect("uri should parse");

    let authorization = ListenNotificationAuthorization {
        envelope: TestRequest::new(Method::GET, uri.clone()).build().into_envelope(),
        action: Action::S3Action(S3Action::ListenBucketNotificationAction),
        bucket_incarnation: None,
        keystone_credentials: None,
        refresh_at: tokio::time::Instant::now(),
    };
    let resp = build_listen_notification_response(&uri, Some("demo-bucket"), authorization).expect("response should build");

    let (status, headers, _) = resp.into_parts();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get(CONTENT_TYPE).and_then(|v| v.to_str().ok()), Some("text/event-stream"));
    assert_eq!(headers.get(CACHE_CONTROL).and_then(|v| v.to_str().ok()), Some("no-cache"));
    assert_eq!(headers.get("x-accel-buffering").and_then(|v| v.to_str().ok()), Some("no"));
}
