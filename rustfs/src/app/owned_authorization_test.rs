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

//! Owned-versus-legacy authorization parity: the same request through the
//! s3s edge and through `RequestEnvelope` must reach the same decision, audit
//! entry and condition values. These live in the app layer because the
//! envelope (and its impl of the storage facts trait) belongs here.

use crate::app::RequestEnvelope;
use crate::app::storage_api::context::MultipartStagingOperation;
use crate::app::storage_api::owned_parity::FS;
use crate::app::storage_api::owned_parity::parity_test_support as support;
use crate::app::storage_api::owned_parity::{DenialAuditor, DenialRateLimiter};
use crate::app::storage_api::owned_parity::{
    ObjectTagConditions, PostObjectPolicyHeaders, PostObjectRequestMarker, ReqInfo, RequestedCopySource,
    RequestedObjectLegalHold, RequestedObjectLockRetainUntil, RequestedObjectLockRetention, RequestedObjectTags,
    TableDataPlanePublicationGuards,
};
use crate::app::storage_api::s3::{
    BucketLocationConstraint, CreateBucketConfiguration, CreateBucketInput, CreateMultipartUploadInput, DeleteObjectInput,
    ObjectLockLegalHold, ObjectLockLegalHoldStatus, ObjectLockRetention, PutObjectInput, S3Access, S3ErrorCode, S3Request, Tag,
    Tagging,
};
use crate::auth::facts::{AuthzPrincipal, AuthzTarget};
use crate::storage::storage_api::contract::bucket::{BucketOperations as _, MakeBucketOptions};
use crate::storage::storage_api::contract::object::{ObjectIO as _, ObjectOperations as _};
use crate::storage::storage_api::runtime_sources_consumer::{AppContext, IamInterface, KmsInterface, ServerContextSlot};
use http::{Extensions, HeaderMap, HeaderValue, Method, Uri};
use rustfs_iam::{store::object::ObjectStore, sys::IamSys};
use rustfs_kms::KmsServiceManager;
use rustfs_policy::policy::action::{Action, S3Action};
use serial_test::serial;
use std::time::Instant;
use std::{collections::HashMap, sync::Arc};

struct UnreadyIam;

impl IamInterface for UnreadyIam {
    fn handle(&self) -> Arc<IamSys<ObjectStore>> {
        panic!("parity tests do not resolve IAM through the unready slot")
    }

    fn is_ready(&self) -> bool {
        false
    }
}

struct TestKms;

impl KmsInterface for TestKms {
    fn handle(&self) -> Arc<KmsServiceManager> {
        Arc::new(KmsServiceManager::new())
    }
}

fn build_request<T>(input: T, method: Method) -> S3Request<T> {
    S3Request {
        input,
        method,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions: Extensions::new(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

fn ensure_req_info<T>(req: &mut S3Request<T>) {
    req.extensions.insert(ReqInfo::default());
}

#[test]
fn owned_and_legacy_denials_share_audit_fields_and_rate_limit() {
    use crate::app::storage_api::owned_parity::DenialScope;
    use crate::app::storage_api::owned_parity::ReqInfo;
    use crate::app::storage_api::owned_parity::RequestContext;
    use crate::app::storage_api::s3::S3Request;
    use http::{Extensions, HeaderMap, Method, Uri};
    use rustfs_credentials::Credentials;
    use rustfs_policy::policy::action::{Action, S3Action};
    use std::sync::{Arc, Mutex};

    let now = Instant::now();
    let captured = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&captured);
    let auditor = DenialAuditor::new(
        DenialRateLimiter::new(2, 1, now),
        || true,
        move |context, entry| {
            sink.lock().unwrap().push((context, entry));
        },
    )
    .with_clock(move || now);
    let mut req = S3Request {
        input: (),
        method: Method::GET,
        uri: Uri::from_static("/uri-bucket/uri-object"),
        headers: HeaderMap::new(),
        extensions: Extensions::new(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    };
    req.extensions.insert(ReqInfo {
        cred: Some(Credentials {
            access_key: "audit-user".to_string(),
            ..Default::default()
        }),
        bucket: Some("named-bucket".to_string()),
        object: Some("named-object".to_string()),
        request_context: Some(RequestContext {
            request_id: "nested-id".to_string(),
            x_amz_request_id: "nested-id".to_string(),
            trace_id: None,
            span_id: None,
            start_time: now,
        }),
        ..Default::default()
    });
    let envelope = crate::app::RequestEnvelope::from_s3s(&req);
    let action = Action::S3Action(S3Action::GetObjectAction);
    auditor.audit(&req, &action, "denied", DenialScope::Request);
    auditor.audit_owned(&envelope.audit_view(), &action, "denied", DenialScope::Request);
    auditor.audit_owned(&envelope.audit_view(), &action, "denied", DenialScope::Request);

    let entries = captured.lock().unwrap();
    assert_eq!(entries.len(), 2);
    for (context, entry) in entries.iter() {
        assert_eq!(context.as_ref().map(|ctx| ctx.request_id.as_str()), Some("nested-id"));
        assert_eq!(entry.request_id.as_deref(), Some("nested-id"));
        assert_eq!(entry.access_key.as_deref(), Some("audit-user"));
        assert_eq!(entry.api.bucket.as_deref(), Some("named-bucket"));
        assert_eq!(entry.api.object.as_deref(), Some("named-object"));
    }
}

#[test]
fn owned_and_legacy_denials_preserve_context_and_address_precedence() {
    use crate::app::storage_api::owned_parity::ReqInfo;
    use crate::app::storage_api::owned_parity::RequestContext;
    use crate::app::storage_api::owned_parity::{DenialScope, authorization_denial_entry, authorization_denial_entry_for_view};
    use crate::app::storage_api::s3::S3Request;
    use crate::shared_types::RemoteAddr;
    use http::{Extensions, HeaderMap, Method, Uri};
    use rustfs_policy::policy::action::{Action, S3Action};
    use rustfs_trusted_proxies::ClientInfo;
    use std::net::SocketAddr;

    let now = Instant::now();
    let context = |id: &str| RequestContext {
        request_id: id.to_owned(),
        x_amz_request_id: id.to_owned(),
        trace_id: None,
        span_id: None,
        start_time: now,
    };
    let mut req = S3Request {
        input: (),
        method: Method::GET,
        uri: Uri::from_static("/uri-bucket/uri-object"),
        headers: HeaderMap::new(),
        extensions: Extensions::new(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    };
    req.extensions.insert(ReqInfo {
        bucket: Some(String::new()),
        object: Some(String::new()),
        request_context: Some(context("nested")),
        ..Default::default()
    });
    req.extensions.insert(context("direct"));
    req.extensions
        .insert(Some(RemoteAddr("192.0.2.2:9000".parse::<SocketAddr>().unwrap())));
    req.extensions.insert(ClientInfo::direct("198.51.100.7:443".parse().unwrap()));
    let action = Action::S3Action(S3Action::GetObjectAction);

    for (client_present, expected_host) in [(true, "198.51.100.7"), (false, "192.0.2.2")] {
        if !client_present {
            req.extensions.remove::<ClientInfo>();
            req.extensions.remove::<RequestContext>();
        }
        let envelope = crate::app::RequestEnvelope::from_s3s(&req);
        let expected_id = if client_present { "direct" } else { "nested" };
        let legacy = authorization_denial_entry(&req, &action, "denied", DenialScope::Request);
        let owned = authorization_denial_entry_for_view(&envelope.audit_view(), &action, "denied", DenialScope::Request);
        for entry in [legacy, owned] {
            assert_eq!(
                (
                    entry.request_id.as_deref(),
                    entry.remote_host.as_deref(),
                    entry.api.bucket.as_deref(),
                    entry.api.object.as_deref()
                ),
                (Some(expected_id), Some(expected_host), Some("uri-bucket"), Some("uri-object")),
            );
        }
    }
}

#[test]
fn anonymous_owned_and_legacy_denials_share_limit_and_bulk_scope() {
    use crate::app::storage_api::owned_parity::DenialScope;
    use crate::app::storage_api::s3::S3Request;
    use http::{Extensions, HeaderMap, Method, Uri};
    use rustfs_policy::policy::action::{Action, S3Action};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    let now = Instant::now();
    let captured = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&captured);
    let auditor = DenialAuditor::new(
        DenialRateLimiter::new(2, 1, now),
        || true,
        move |context, entry| {
            sink.lock().unwrap().push((context, entry));
        },
    )
    .with_clock(move || now + Duration::from_secs(1));
    let req = S3Request {
        input: (),
        method: Method::POST,
        uri: Uri::from_static("/bucket/key"),
        headers: HeaderMap::new(),
        extensions: Extensions::new(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    };
    let envelope = crate::app::RequestEnvelope::from_s3s(&req);
    let action = Action::S3Action(S3Action::DeleteObjectAction);
    auditor.audit_at(&req, &action, "denied", DenialScope::Keys(3), now);
    auditor.audit_at(&req, &action, "denied", DenialScope::Keys(3), now);
    auditor.audit_owned(&envelope.audit_view(), &action, "denied", DenialScope::Keys(3));

    let entries = captured.lock().unwrap();
    assert_eq!(entries.len(), 2);
    for (context, entry) in entries.iter() {
        assert_eq!(
            (
                context.is_none(),
                entry.api.bucket.as_deref(),
                entry.api.object.as_deref(),
                entry
                    .tags
                    .as_ref()
                    .and_then(|tags| tags.get("deniedKeys"))
                    .and_then(|v| v.as_u64())
            ),
            (true, Some("bucket"), None, Some(3)),
        );
    }
    assert_eq!(
        entries[1]
            .1
            .tags
            .as_ref()
            .and_then(|tags| tags.get(crate::app::storage_api::owned_parity::SUPPRESSED_DENIALS_TAG))
            .and_then(|v| v.as_u64()),
        Some(1)
    );
}

#[tokio::test]
#[serial]
async fn owned_authorization_reloads_tags_when_version_changes() {
    let (_temp_dir, _disk_paths, store) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
    let bucket = format!("version-tag-{}", uuid::Uuid::new_v4());
    store
        .make_bucket(&bucket, &MakeBucketOptions::default())
        .await
        .expect("create bucket");
    let mut versions = Vec::new();
    for tag in ["classification=allowed", "classification=denied"] {
        let mut reader = crate::storage::PutObjReader::from_vec(tag.as_bytes().to_vec());
        let info = store
            .put_object(
                &bucket,
                "object",
                &mut reader,
                &crate::storage::ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("create version");
        let version = info.version_id.expect("versioned write").to_string();
        store
            .put_object_tags(
                &bucket,
                "object",
                tag,
                &crate::storage::ObjectOptions {
                    version_id: Some(version.clone()),
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("tag version");
        versions.push(version);
    }
    assert_ne!(versions[0], versions[1], "writes must create distinct versions");
    let policy = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"*"}},"Action":"s3:PutObject","Resource":"arn:aws:s3:::{bucket}/*","Condition":{{"StringEquals":{{"s3:ExistingObjectTag/classification":"allowed"}}}}}}]}}"#
    );
    store
        .update_bucket_metadata_config(
            &bucket,
            crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
            policy.into_bytes(),
        )
        .await
        .expect("set policy");
    let slot = ServerContextSlot::new();
    assert!(slot.install(Arc::new(AppContext::new(store, Arc::new(UnreadyIam), Arc::new(TestKms)))));
    let mut legacy = build_request((), Method::PUT);
    legacy.extensions.insert(ReqInfo {
        bucket: Some(bucket.clone()),
        object: Some("object".to_owned()),
        ..Default::default()
    });
    legacy.extensions.insert(slot);
    let mut owned = crate::app::RequestEnvelope::from_s3s(&legacy);
    for (index, allowed, expected_tag) in [(0, true, "allowed"), (1, false, "denied"), (0, true, "allowed")] {
        let version = &versions[index];
        legacy.extensions.get_mut::<ReqInfo>().expect("legacy info").version_id = Some(version.clone());
        owned.req_info_mut().expect("owned info").version_id = Some(version.clone());
        let action = Action::S3Action(S3Action::PutObjectAction);
        let legacy_result = crate::app::storage_api::owned_parity::authorize_request(&mut legacy, action).await;
        let owned_result = crate::app::authorize_owned_request(&mut owned, action, None).await;
        if allowed {
            legacy_result.expect("legacy version allowed");
            owned_result.expect("owned version allowed");
        } else {
            assert_eq!(legacy_result.expect_err("legacy version denied").code(), &S3ErrorCode::AccessDenied);
            assert_eq!(
                owned_result.expect_err("owned version denied").code(),
                &rustfs_s3_types::S3ErrorCode::AccessDenied
            );
        }
        let old = legacy.extensions.get::<ObjectTagConditions>().expect("legacy cache");
        let new = owned.object_tag_conditions().expect("owned cache");
        assert_eq!(support::tag_conditions_parts(old).2, Some(version.as_str()));
        assert_eq!(support::tag_conditions_parts(new).2, Some(version.as_str()));
        assert_eq!(
            support::tag_conditions_parts(old).3.get("ExistingObjectTag/classification"),
            Some(&vec![expected_tag.to_owned()])
        );
        assert_eq!(
            support::tag_conditions_parts(new).3.get("ExistingObjectTag/classification"),
            Some(&vec![expected_tag.to_owned()])
        );
    }
}

#[tokio::test]
#[serial]
async fn table_publication_guard_timeout_blocks_put_delete_and_multipart_authorization() {
    use crate::storage::storage_api::contract::namespace::NamespaceLocking as _;
    use http_body_util::BodyExt as _;
    use std::time::Duration;

    let store = crate::app::gating_test_env::shared_gating_ecstore().await;
    let server_ctx = ServerContextSlot::new();
    assert!(server_ctx.install(Arc::new(AppContext::new(Arc::clone(&store), Arc::new(UnreadyIam), Arc::new(TestKms)))));
    let fs = FS::with_server_ctx(server_ctx);
    let bucket = format!("publication-timeout-{}", uuid::Uuid::new_v4());
    store
        .make_bucket(&bucket, &MakeBucketOptions::default())
        .await
        .expect("test bucket should be created");
    let policy_json = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"*"}},"Action":["s3:PutObject","s3:DeleteObject"],"Resource":["arn:aws:s3:::{bucket}/*"]}}]}}"#
    );
    let mut metadata = (*crate::storage::get_bucket_metadata(&bucket)
        .await
        .expect("test bucket metadata should be cached"))
    .clone();
    metadata.policy_config = Some(serde_json::from_str(&policy_json).expect("test policy should parse"));
    metadata.policy_config_json = policy_json.into_bytes();
    crate::storage::storage_api::set_bucket_metadata(bucket.clone(), metadata)
        .await
        .expect("test policy should be published");
    let lock_object = crate::table_catalog::default_table_bucket_publication_lock_path();
    let lock = store
        .new_ns_lock(&bucket, &lock_object)
        .await
        .expect("publication lock should be created");
    let mut writer = lock
        .get_write_lock(Duration::from_secs(1))
        .await
        .expect("publication writer should hold the fence");
    let mut put_req = build_request(
        PutObjectInput::builder()
            .bucket(bucket.clone())
            .key("object".to_string())
            .build()
            .expect("PUT input should build"),
        Method::PUT,
    );
    let mut multipart_req = build_request(
        CreateMultipartUploadInput::builder()
            .bucket(bucket.clone())
            .key("multipart-object".to_string())
            .build()
            .expect("multipart input should build"),
        Method::POST,
    );
    let mut delete_req = build_request(
        DeleteObjectInput::builder()
            .bucket(bucket.clone())
            .key("object".to_string())
            .build()
            .expect("DELETE input should build"),
        Method::DELETE,
    );
    ensure_req_info(&mut delete_req);
    delete_req.extensions.insert(fs.server_ctx().clone());
    ensure_req_info(&mut put_req);
    ensure_req_info(&mut multipart_req);
    put_req.extensions.insert(fs.server_ctx().clone());
    multipart_req.extensions.insert(fs.server_ctx().clone());

    let mut owned_req = crate::app::RequestEnvelope::from_s3s(&put_req);
    let owned_info = owned_req.req_info_mut().expect("owned request info");
    owned_info.bucket = Some(bucket.clone());
    owned_info.object = Some("object".to_string());
    let owned_err = crate::app::authorize_owned_request(&mut owned_req, Action::S3Action(S3Action::PutObjectAction), None)
        .await
        .expect_err("owned PUT must wait for the publication writer");
    assert_eq!(owned_err.code(), &rustfs_s3_types::S3ErrorCode::ServiceUnavailable);
    assert!(owned_req.table_publication_guards().is_none());

    for (operation, err) in [
        (
            "PutObject",
            fs.put_object(&mut put_req)
                .await
                .expect_err("PUT must wait for the publication writer"),
        ),
        (
            "DeleteObject",
            fs.delete_object(&mut delete_req)
                .await
                .expect_err("DELETE must wait for the publication writer"),
        ),
        (
            "CreateMultipartUpload",
            fs.create_multipart_upload(&mut multipart_req)
                .await
                .expect_err("multipart initialization must wait for the publication writer"),
        ),
    ] {
        assert_eq!(err.code(), &S3ErrorCode::ServiceUnavailable, "{operation}: {err}");
        let source = err.source().expect("retain the typed publication lock failure");
        let storage = source
            .downcast_ref::<crate::storage_api::error::StorageError>()
            .expect("use the canonical storage error boundary");
        assert!(matches!(
            storage,
            crate::storage_api::error::StorageError::Lock(rustfs_lock::LockError::Timeout { .. })
        ));
        let response = err.to_http_response().expect("serialize publication failure");
        assert_eq!(response.status(), http::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[http::header::RETRY_AFTER], "1");
        let body = response.into_body().collect().await.expect("read error XML").to_bytes();
        let body = String::from_utf8(body.to_vec()).expect("S3 XML is UTF-8");
        assert!(body.contains("<Code>ServiceUnavailable</Code>"));
        assert!(!body.contains(&bucket));
        assert!(!body.contains(&lock_object));
    }
    for extensions in [&put_req.extensions, &delete_req.extensions, &multipart_req.extensions] {
        assert!(extensions.get::<TableDataPlanePublicationGuards>().is_none());
    }

    assert!(writer.release());
    for _ in 0..2 {
        crate::app::authorize_owned_request(&mut owned_req, Action::S3Action(S3Action::PutObjectAction), None)
            .await
            .expect("owned PUT may retry after publication finishes");
        let retained = owned_req
            .table_publication_guards()
            .expect("owned request retains the publication guard");
        {
            let (held, guards) = support::publication_guard_holds(retained, &(bucket.clone(), lock_object.clone()));
            assert!(held);
            assert_eq!(guards, 1, "repeated owned admission must reuse its retained guard");
        }
        fs.put_object(&mut put_req)
            .await
            .expect("PUT may retry after publication finishes");
        fs.create_multipart_upload(&mut multipart_req)
            .await
            .expect("multipart initialization may retry after publication finishes");
        fs.delete_object(&mut delete_req)
            .await
            .expect("DELETE may retry after publication finishes");
        for extensions in [&put_req.extensions, &delete_req.extensions, &multipart_req.extensions] {
            let retained = extensions
                .get::<TableDataPlanePublicationGuards>()
                .expect("successful admission retains the guard");
            let (held, guards) = support::publication_guard_holds(retained, &(bucket.clone(), lock_object.clone()));
            assert!(held);
            assert_eq!(guards, 1, "repeated admission must reuse its retained guard");
        }
    }
    drop(put_req);
    drop(multipart_req);
    drop(delete_req);
    drop(owned_req);
    lock.get_write_lock(Duration::from_secs(1))
        .await
        .expect("dropping the request must release its publication guard");
}

#[tokio::test]
#[serial]
async fn owned_and_legacy_create_bucket_authorization_use_location_constraint() {
    use rustfs_iam::store::{Store as _, UserType};
    use rustfs_madmin::{AccountStatus, AddOrUpdateUserReq};
    use rustfs_policy::policy::Policy;

    struct ReadyIam(Arc<IamSys<ObjectStore>>);
    impl IamInterface for ReadyIam {
        fn handle(&self) -> Arc<IamSys<ObjectStore>> {
            Arc::clone(&self.0)
        }

        fn is_ready(&self) -> bool {
            true
        }
    }

    let (_temp_dir, _disk_paths, store) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
    ObjectStore::new(Arc::clone(&store))
        .save_iam_config(
            serde_json::json!({"version": 1}),
            format!("{}/format.json", *rustfs_iam::store::object::IAM_CONFIG_PREFIX),
        )
        .await
        .expect("seed IAM format");
    let iam = rustfs_iam::build_iam_sys(Arc::clone(&store)).await.expect("initialize IAM");
    let access_key = "location-test-user";
    iam.create_user(
        access_key,
        &AddOrUpdateUserReq {
            secret_key: "location-test-secret-key".to_owned(),
            policy: None,
            status: AccountStatus::Enabled,
        },
    )
    .await
    .expect("create IAM user");
    iam.set_policy(
        "location-create-policy",
        Policy::parse_config(
            br#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:CreateBucket","Resource":"arn:aws:s3:::location-policy-bucket","Condition":{"StringEquals":{"s3:LocationConstraint":"eu-west-1"}}}]}"#,
        )
        .expect("parse location policy"),
    )
    .await
    .expect("store location policy");
    iam.policy_db_set(access_key, UserType::Reg, false, "location-create-policy")
        .await
        .expect("attach location policy");
    let credentials = iam.get_user(access_key).await.expect("load IAM user").credentials;
    let context = Arc::new(AppContext::new(store, Arc::new(ReadyIam(iam)), Arc::new(TestKms)));
    let slot = ServerContextSlot::new();
    assert!(slot.install(context), "install request context");

    for (region, allowed) in [("eu-west-1", true), ("us-west-2", false)] {
        let mut legacy = build_request(
            CreateBucketInput {
                bucket: "location-policy-bucket".to_owned(),
                create_bucket_configuration: Some(CreateBucketConfiguration {
                    location_constraint: Some(BucketLocationConstraint::from(region.to_owned())),
                    ..Default::default()
                }),
                ..Default::default()
            },
            Method::PUT,
        );
        legacy.uri = Uri::from_static("/location-policy-bucket");
        legacy.extensions.insert(Arc::clone(&slot));
        legacy.extensions.insert(ReqInfo {
            cred: Some(credentials.clone()),
            bucket: Some("location-policy-bucket".to_owned()),
            ..Default::default()
        });
        let location = support::requested_bucket_location_constraint(&legacy.input);
        let mut owned = crate::app::RequestEnvelope::from_s3s(&legacy);
        let action = Action::S3Action(S3Action::CreateBucketAction);
        let legacy_result = support::authorize_request_with_location_constraint(&mut legacy, action, location.clone()).await;
        let owned_result = crate::app::authorize_owned_request(&mut owned, action, location).await;
        if allowed {
            legacy_result.expect("legacy allows the matched location");
            owned_result.expect("owned allows the matched location");
        } else {
            assert_eq!(
                legacy_result.expect_err("legacy denies the mismatched location").code(),
                &S3ErrorCode::AccessDenied
            );
            assert_eq!(
                owned_result.expect_err("owned denies the mismatched location").code(),
                &rustfs_s3_types::S3ErrorCode::AccessDenied,
            );
        }
    }
}

#[test]
fn owned_and_legacy_conditions_read_the_same_request_body_policy_inputs() {
    let credentials = rustfs_credentials::Credentials::default();
    let principal = AuthzPrincipal::new(&credentials, "");
    let target = |action| AuthzTarget {
        action: Action::S3Action(action),
        bucket: "bucket",
        object: "object",
        version_id: None,
        location_constraint: None,
    };
    let conditions_of = |req: &S3Request<()>, action| {
        let owned = crate::app::RequestEnvelope::from_s3s(req);
        let legacy = support::authorization_conditions(req, &principal, &support::request_authz_facts(req, target(action)))
            .expect("legacy conditions");
        let owned = support::authorization_conditions(&owned, &principal, &support::request_authz_facts(&owned, target(action)))
            .expect("owned conditions");
        (legacy, owned)
    };
    let values = |conditions: &HashMap<String, Vec<String>>, key: &str| conditions.get(key).cloned();

    // A POST upload's form fields and a resolved copy source replace what the headers say.
    let mut post = build_request((), Method::POST);
    post.headers
        .insert("x-amz-tagging", HeaderValue::from_static("source=header"));
    let mut form = post.headers.clone();
    form.insert("x-amz-tagging", HeaderValue::from_static("source=form"));
    post.extensions.insert(PostObjectPolicyHeaders(form));
    post.extensions.insert(RequestedCopySource("src-bucket/src-key".to_owned()));
    let (legacy, owned) = conditions_of(&post, S3Action::PutObjectAction);
    for key in ["RequestObjectTag/source", "RequestObjectTagKeys", "x-amz-copy-source"] {
        assert_eq!(values(&owned, key), values(&legacy, key), "{key}");
    }
    assert_eq!(values(&owned, "RequestObjectTag/source"), Some(vec!["form".to_owned()]));
    assert_eq!(values(&owned, "x-amz-copy-source"), Some(vec!["src-bucket/src-key".to_owned()]));

    // Standalone legal hold and tagging writes apply their XML bodies, not the headers.
    let mut body = build_request((), Method::PUT);
    body.headers.insert("x-amz-tagging", HeaderValue::from_static("body=header"));
    body.headers
        .insert("x-amz-object-lock-legal-hold", HeaderValue::from_static("OFF"));
    let legal_hold = ObjectLockLegalHold {
        status: Some(ObjectLockLegalHoldStatus::from_static(ObjectLockLegalHoldStatus::ON)),
    };
    body.extensions.insert(RequestedObjectLegalHold(legal_hold));
    body.extensions.insert(RequestedObjectTags(Tagging {
        tag_set: vec![Tag {
            key: Some("body".to_owned()),
            value: Some("xml".to_owned()),
        }],
    }));
    let (legacy, owned) = conditions_of(&body, S3Action::PutObjectAction);
    for key in ["object-lock-legal-hold", "RequestObjectTag/body", "RequestObjectTagKeys"] {
        assert_eq!(values(&owned, key), values(&legacy, key), "{key}");
    }
    assert_eq!(values(&owned, "object-lock-legal-hold"), Some(vec!["ON".to_owned()]));
    assert_eq!(values(&owned, "RequestObjectTag/body"), Some(vec!["xml".to_owned()]));
}

#[test]
fn create_bucket_location_constraint_conditions_use_only_xml() {
    let configuration = |region: &str| CreateBucketConfiguration {
        location_constraint: Some(BucketLocationConstraint::from(region.to_owned())),
        ..Default::default()
    };
    for (configuration, expected) in [
        (None, None),
        (Some(CreateBucketConfiguration::default()), None),
        (Some(configuration("")), None),
        (Some(configuration("eu-west-1")), Some("eu-west-1")),
        (Some(configuration("EU")), Some("EU")),
        (Some(configuration("tenant-region-1")), Some("tenant-region-1")),
    ] {
        let mut req = build_request(
            CreateBucketInput {
                bucket: "location-policy-bucket".to_string(),
                create_bucket_configuration: configuration,
                ..Default::default()
            },
            Method::PUT,
        );
        req.region = Some("us-east-1".parse().expect("signing region"));
        req.uri = Uri::from_static("/location-policy-bucket?LocationConstraint=forged&s3%3ALocationConstraint=forged");
        req.headers.insert("locationconstraint", HeaderValue::from_static("forged"));
        let credentials = rustfs_credentials::Credentials {
            claims: Some(
                [
                    ("LocationConstraint".to_string(), serde_json::json!("forged")),
                    ("ldapLocationConstraint".to_string(), serde_json::json!("forged")),
                ]
                .into_iter()
                .collect(),
            ),
            ..Default::default()
        };
        let location = support::requested_bucket_location_constraint(&req.input);
        let principal = AuthzPrincipal::new(&credentials, "");
        let owned = crate::app::RequestEnvelope::from_s3s(&req);
        let facts = support::request_authz_facts(
            &req,
            AuthzTarget {
                action: Action::S3Action(S3Action::CreateBucketAction),
                bucket: "",
                object: "",
                version_id: None,
                location_constraint: location.as_deref(),
            },
        );
        let conditions = support::authorization_conditions(&req, &principal, &facts).expect("CreateBucket conditions");
        let owned_facts = support::request_authz_facts(
            &owned,
            AuthzTarget {
                action: Action::S3Action(S3Action::CreateBucketAction),
                bucket: "",
                object: "",
                version_id: None,
                location_constraint: location.as_deref(),
            },
        );
        let owned_conditions =
            support::authorization_conditions(&owned, &principal, &owned_facts).expect("owned CreateBucket conditions");
        assert_eq!(owned_conditions.get("LocationConstraint"), conditions.get("LocationConstraint"));
        assert_eq!(
            conditions
                .get("LocationConstraint")
                .and_then(|values| values.first())
                .map(String::as_str),
            expected,
        );
        for action in [
            S3Action::PutBucketObjectLockConfigurationAction,
            S3Action::PutBucketVersioningAction,
            S3Action::GetObjectAction,
        ] {
            let facts = support::request_authz_facts(
                &req,
                AuthzTarget {
                    action: Action::S3Action(action),
                    bucket: "",
                    object: "",
                    version_id: None,
                    location_constraint: expected,
                },
            );
            let conditions = support::authorization_conditions(&req, &principal, &facts).expect("non-CreateBucket conditions");
            assert!(!conditions.contains_key("LocationConstraint"), "{action:?}");
            let owned_facts = support::request_authz_facts(
                &owned,
                AuthzTarget {
                    action: Action::S3Action(action),
                    bucket: "",
                    object: "",
                    version_id: None,
                    location_constraint: expected,
                },
            );
            let owned_conditions =
                support::authorization_conditions(&owned, &principal, &owned_facts).expect("owned non-CreateBucket conditions");
            assert!(!owned_conditions.contains_key("LocationConstraint"), "{action:?}");
        }
    }
}

#[tokio::test]
#[serial]
async fn request_slot_keeps_bucket_policy_bound_to_its_store() {
    let store_a = crate::app::gating_test_env::shared_gating_ecstore().await;
    let (_store_b_temp, _store_b_paths, store_b) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
    let bucket = format!("request-policy-isolation-{}", uuid::Uuid::new_v4());
    for store in [&store_a, &store_b] {
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create isolated policy test bucket");
    }

    let allow_policy = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"*"}},"Action":["s3:PutObject"],"Resource":["arn:aws:s3:::{bucket}/*"]}}]}}"#
    );
    let deny_policy = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Deny","Principal":{{"AWS":"*"}},"Action":["s3:PutObject"],"Resource":["arn:aws:s3:::{bucket}/*"]}}]}}"#
    );
    for (store, policy) in [(&store_a, allow_policy), (&store_b, deny_policy)] {
        store
            .update_bucket_metadata_config(
                &bucket,
                crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
                policy.into_bytes(),
            )
            .await
            .expect("publish isolated bucket policy");
    }

    let slot_a = ServerContextSlot::new();
    assert!(slot_a.install(Arc::new(AppContext::new(Arc::clone(&store_a), Arc::new(UnreadyIam), Arc::new(TestKms),))));
    let slot_b = ServerContextSlot::new();
    assert!(slot_b.install(Arc::new(AppContext::new(Arc::clone(&store_b), Arc::new(UnreadyIam), Arc::new(TestKms),))));

    let mut request_a = build_request((), Method::PUT);
    request_a.extensions.insert(ReqInfo {
        bucket: Some(bucket.clone()),
        object: Some("object".to_string()),
        ..Default::default()
    });
    request_a.extensions.insert(slot_a);
    let mut owned_a = crate::app::RequestEnvelope::from_s3s(&request_a);
    crate::app::storage_api::owned_parity::authorize_request(&mut request_a, Action::S3Action(S3Action::PutObjectAction))
        .await
        .expect("store A policy should authorize store A request");
    crate::app::authorize_owned_request(&mut owned_a, Action::S3Action(S3Action::PutObjectAction), None)
        .await
        .expect("store A policy should authorize owned request");

    let mut request_b = build_request((), Method::PUT);
    request_b.extensions.insert(ReqInfo {
        bucket: Some(bucket.clone()),
        object: Some("object".to_string()),
        ..Default::default()
    });
    request_b.extensions.insert(slot_b);
    let mut owned_b = crate::app::RequestEnvelope::from_s3s(&request_b);
    let err =
        crate::app::storage_api::owned_parity::authorize_request(&mut request_b, Action::S3Action(S3Action::PutObjectAction))
            .await
            .expect_err("store B policy must deny store B request");
    assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    let owned_err = crate::app::authorize_owned_request(&mut owned_b, Action::S3Action(S3Action::PutObjectAction), None)
        .await
        .expect_err("store B policy must deny owned request");
    assert_eq!(owned_err.code(), &rustfs_s3_types::S3ErrorCode::AccessDenied);

    let allow_policy = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"*"}},"Action":["s3:PutObject"],"Resource":["arn:aws:s3:::{bucket}/*"]}}]}}"#
    );
    store_b
        .update_bucket_metadata_config(
            &bucket,
            crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
            allow_policy.into_bytes(),
        )
        .await
        .expect("replace store B bucket policy");
    let slot_b = ServerContextSlot::new();
    assert!(slot_b.install(Arc::new(AppContext::new(Arc::clone(&store_b), Arc::new(UnreadyIam), Arc::new(TestKms),))));
    let mut allowed_request_b = build_request((), Method::PUT);
    allowed_request_b.extensions.insert(ReqInfo {
        bucket: Some(bucket.clone()),
        object: Some("object".to_string()),
        ..Default::default()
    });
    allowed_request_b.extensions.insert(slot_b);
    let mut owned_allowed_b = crate::app::RequestEnvelope::from_s3s(&allowed_request_b);
    crate::app::storage_api::owned_parity::authorize_request(&mut allowed_request_b, Action::S3Action(S3Action::PutObjectAction))
        .await
        .expect("store B policy should allow before its public access block is installed");
    crate::app::authorize_owned_request(&mut owned_allowed_b, Action::S3Action(S3Action::PutObjectAction), None)
        .await
        .expect("store B policy should authorize owned request before public access block");

    store_b
        .update_bucket_metadata_config(
            &bucket,
            crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_PUBLIC_ACCESS_BLOCK_CONFIG,
            br#"<PublicAccessBlockConfiguration><RestrictPublicBuckets>true</RestrictPublicBuckets></PublicAccessBlockConfiguration>"#
                .to_vec(),
        )
        .await
        .expect("publish store B public access block configuration");
    let slot_b = ServerContextSlot::new();
    assert!(slot_b.install(Arc::new(AppContext::new(Arc::clone(&store_b), Arc::new(UnreadyIam), Arc::new(TestKms),))));
    let mut restricted_request_b = build_request((), Method::PUT);
    restricted_request_b.extensions.insert(ReqInfo {
        bucket: Some(bucket.clone()),
        object: Some("object".to_string()),
        ..Default::default()
    });
    restricted_request_b.extensions.insert(slot_b);
    let mut owned_restricted_b = crate::app::RequestEnvelope::from_s3s(&restricted_request_b);
    let err = crate::app::storage_api::owned_parity::authorize_request(
        &mut restricted_request_b,
        Action::S3Action(S3Action::PutObjectAction),
    )
    .await
    .expect_err("store B public access block must restrict store B request");
    assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    let owned_err =
        crate::app::authorize_owned_request(&mut owned_restricted_b, Action::S3Action(S3Action::PutObjectAction), None)
            .await
            .expect_err("store B public access block must restrict owned request");
    assert_eq!(owned_err.code(), &rustfs_s3_types::S3ErrorCode::AccessDenied);

    let tag_bucket = format!("request-tag-isolation-{}", uuid::Uuid::new_v4());
    for (store, tags) in [(&store_a, "classification=ambient"), (&store_b, "classification=request")] {
        store
            .make_bucket(&tag_bucket, &MakeBucketOptions::default())
            .await
            .expect("create isolated tag test bucket");
        let mut reader = crate::storage::PutObjReader::from_vec(b"tagged object".to_vec());
        store
            .put_object(&tag_bucket, "object", &mut reader, &crate::storage::ObjectOptions::default())
            .await
            .expect("create isolated tagged object");
        store
            .put_object_tags(&tag_bucket, "object", tags, &crate::storage::ObjectOptions::default())
            .await
            .expect("publish isolated object tags");
    }
    let tag_policy = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"*"}},"Action":["s3:PutObject"],"Resource":["arn:aws:s3:::{tag_bucket}/*"],"Condition":{{"StringEquals":{{"s3:ExistingObjectTag/classification":"request"}}}}}}]}}"#
    );
    store_b
        .update_bucket_metadata_config(
            &tag_bucket,
            crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
            tag_policy.into_bytes(),
        )
        .await
        .expect("publish store B tag-conditioned policy");
    let slot_b = ServerContextSlot::new();
    assert!(slot_b.install(Arc::new(AppContext::new(Arc::clone(&store_b), Arc::new(UnreadyIam), Arc::new(TestKms),))));
    let mut tagged_request_b = build_request((), Method::PUT);
    tagged_request_b.extensions.insert(ReqInfo {
        bucket: Some(tag_bucket),
        object: Some("object".to_string()),
        ..Default::default()
    });
    tagged_request_b.extensions.insert(slot_b);
    let mut owned_tagged_b = crate::app::RequestEnvelope::from_s3s(&tagged_request_b);
    crate::app::storage_api::owned_parity::authorize_request(&mut tagged_request_b, Action::S3Action(S3Action::PutObjectAction))
        .await
        .expect("store B existing object tags should authorize store B request");
    crate::app::authorize_owned_request(&mut owned_tagged_b, Action::S3Action(S3Action::PutObjectAction), None)
        .await
        .expect("store B existing object tags should authorize owned request");
    let legacy_tags = tagged_request_b
        .extensions
        .get::<ObjectTagConditions>()
        .expect("legacy tag cache");
    let owned_tags = owned_tagged_b.object_tag_conditions().expect("owned tag cache");
    assert_eq!(support::tag_conditions_parts(owned_tags), support::tag_conditions_parts(legacy_tags));
    assert_eq!(
        support::tag_conditions_parts(owned_tags)
            .3
            .get("ExistingObjectTag/classification"),
        Some(&vec!["request".to_string()])
    );

    let mut unready_request = build_request((), Method::PUT);
    unready_request.extensions.insert(ServerContextSlot::new());
    ensure_req_info(&mut unready_request);
    let mut owned_unready = crate::app::RequestEnvelope::from_s3s(&unready_request);
    let err = support::request_object_store(&unready_request).expect_err("an unready request slot must not use an ambient store");
    assert_eq!(err.code(), &S3ErrorCode::InternalError);
    let owned_err = crate::app::authorize_owned_request(&mut owned_unready, Action::S3Action(S3Action::PutObjectAction), None)
        .await
        .expect_err("unready request slot must not fall back to an ambient store");
    assert_eq!(owned_err.code(), &rustfs_s3_types::S3ErrorCode::InternalError);
}

/// Case name, request, action, and how to supply the missing body input.
type FailClosedCase = (&'static str, S3Request<()>, Action, fn(&mut Extensions));

fn request(extensions: Extensions) -> S3Request<()> {
    let mut req = build_request((), Method::PUT);
    req.uri = Uri::from_static("/bucket/key");
    req.extensions = extensions;
    req
}

const SIGV4_HEADER: &str =
    "AWS4-HMAC-SHA256 Credential=AKIAENVELOPE/20260101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=0123";

fn signed_zip_request(query: &str) -> S3Request<()> {
    let mut req = request(Extensions::new());
    req.method = Method::POST;
    req.uri = format!("/rustfs/admin/v3/object-zip-downloads?{query}")
        .parse()
        .expect("static uri");
    req.headers
        .insert(http::header::AUTHORIZATION, HeaderValue::from_static(SIGV4_HEADER));
    req
}

fn get_object() -> rustfs_policy::policy::action::Action {
    rustfs_policy::policy::action::Action::S3Action(rustfs_policy::policy::action::S3Action::GetObjectAction)
}

#[tokio::test]
async fn owned_entry_points_reject_duplicate_signed_metadata_like_the_legacy_edge() {
    let query = "x-amz-meta-a=1&x-amz-meta-a=2";
    let mut legacy = signed_zip_request(query);
    let legacy_err = crate::app::storage_api::owned_parity::authorize_request(&mut legacy, get_object())
        .await
        .expect_err("legacy rejects a repeated metadata key");
    assert_eq!(legacy_err.code(), &S3ErrorCode::InvalidArgument);

    let mut owned = RequestEnvelope::from_s3s(&signed_zip_request(query));
    let owned_err = crate::app::authorize_owned_request(&mut owned, get_object(), None)
        .await
        .expect_err("owned rejects a repeated metadata key");
    assert_eq!(owned_err.code(), &rustfs_s3_types::S3ErrorCode::InvalidArgument);

    let mut internal = RequestEnvelope::from_s3s(&signed_zip_request(query));
    let internal_err = crate::app::authorize_owned_internal_object_request(&mut internal, get_object())
        .await
        .expect_err("owned internal rejects a repeated metadata key");
    assert_eq!(internal_err.code(), &rustfs_s3_types::S3ErrorCode::InvalidArgument);
}

#[tokio::test]
async fn owned_entry_points_hoist_signed_metadata_into_the_headers_policy_reads() {
    let query = "x-amz-meta-a=1";
    let mut legacy = signed_zip_request(query);
    // Neither request carries ReqInfo, so both stop at the core after the hoist.
    let legacy_err = crate::app::storage_api::owned_parity::authorize_request(&mut legacy, get_object())
        .await
        .expect_err("no ReqInfo");
    assert_eq!(legacy_err.code(), &S3ErrorCode::InternalError);
    let mut owned = RequestEnvelope::from_s3s(&signed_zip_request(query));
    let owned_err = crate::app::authorize_owned_request(&mut owned, get_object(), None)
        .await
        .expect_err("no ReqInfo");
    assert_eq!(owned_err.code(), &rustfs_s3_types::S3ErrorCode::InternalError);

    assert_eq!(legacy.headers.get("x-amz-meta-a"), Some(&HeaderValue::from_static("1")));
    assert_eq!(owned.headers(), &legacy.headers, "both edges evaluate the same headers");
}

#[tokio::test]
async fn owned_entry_point_fails_closed_without_a_body_policy_input() {
    use crate::app::storage_api::owned_parity::{
        PostObjectPolicyHeaders, RequestedCopySource, RequestedObjectLegalHold, RequestedObjectLockRetention, RequestedObjectTags,
    };
    use rustfs_policy::policy::action::{Action, S3Action};

    let put_object = Action::S3Action(S3Action::PutObjectAction);
    let copy = || {
        let mut req = request(Extensions::new());
        req.headers
            .insert("x-amz-copy-source", HeaderValue::from_static("source/key"));
        req
    };
    let post = || {
        let mut extensions = Extensions::new();
        extensions.insert(PostObjectRequestMarker);
        request(extensions)
    };
    let cases: Vec<FailClosedCase> = vec![
        (
            "legal hold",
            request(Extensions::new()),
            Action::S3Action(S3Action::PutObjectLegalHoldAction),
            |e| {
                e.insert(RequestedObjectLegalHold(Default::default()));
            },
        ),
        (
            "tagging",
            request(Extensions::new()),
            Action::S3Action(S3Action::PutObjectTaggingAction),
            |e| {
                e.insert(RequestedObjectTags(Default::default()));
            },
        ),
        (
            "version tagging",
            request(Extensions::new()),
            Action::S3Action(S3Action::PutObjectVersionTaggingAction),
            |e| {
                e.insert(RequestedObjectTags(Default::default()));
            },
        ),
        (
            "retention",
            request(Extensions::new()),
            Action::S3Action(S3Action::PutObjectRetentionAction),
            |e| {
                e.insert(RequestedObjectLockRetention(Default::default()));
            },
        ),
        ("post form", post(), put_object, |e| {
            e.insert(PostObjectPolicyHeaders(HeaderMap::new()));
        }),
        ("copy source", copy(), put_object, |e| {
            e.insert(RequestedCopySource("source/key".to_owned()));
        }),
    ];
    for (name, req, action, supply) in cases {
        let mut missing = RequestEnvelope::from_s3s(&req);
        let err = crate::app::authorize_owned_request(&mut missing, action, None)
            .await
            .expect_err(name);
        assert_eq!(err.code(), &rustfs_s3_types::S3ErrorCode::AccessDenied, "{name}: missing input must deny");
        let mut internal = RequestEnvelope::from_s3s(&req);
        let err = crate::app::authorize_owned_internal_object_request(&mut internal, action)
            .await
            .expect_err(name);
        assert_eq!(
            err.code(),
            &rustfs_s3_types::S3ErrorCode::AccessDenied,
            "{name}: internal missing input must deny"
        );

        // With the input carried, the request reaches the core, which stops on the absent ReqInfo.
        let mut supplied_req = req;
        supply(&mut supplied_req.extensions);
        let mut supplied = RequestEnvelope::from_s3s(&supplied_req);
        let err = crate::app::authorize_owned_request(&mut supplied, action, None)
            .await
            .expect_err(name);
        assert_eq!(
            err.code(),
            &rustfs_s3_types::S3ErrorCode::InternalError,
            "{name}: supplied input passes the gate"
        );
    }
}

/// One request shape for the decision matrix: who asks, for what, and which
/// request facts it carries.
struct DecisionCase {
    name: &'static str,
    bucket: &'static str,
    object: &'static str,
    principal: Principal,
    action: S3Action,
    facts: fn(&mut S3Request<()>),
    allowed: bool,
}

#[derive(Clone, Copy)]
enum Principal {
    Anonymous,
    User,
    Owner,
}

fn no_facts(_: &mut S3Request<()>) {}

fn peer(req: &mut S3Request<()>, addr: [u8; 4]) {
    req.extensions
        .insert(Some(crate::shared_types::RemoteAddr(std::net::SocketAddr::from((addr, 40000)))));
}

const PRIVATE: &str = "parity-private";
const PUBLIC: &str = "parity-public";

/// What the decision parity tests share: two buckets on an isolated store, `PUBLIC` with a bucket
/// policy for anonymous callers, an IAM user whose policy covers `PRIVATE`, the owner, and a
/// request slot bound to that store. The first element keeps the store's directories alive.
async fn decision_world() -> (
    impl Sized,
    Arc<crate::app::storage_api::test::ECStore>,
    Arc<ServerContextSlot>,
    rustfs_credentials::Credentials,
    rustfs_credentials::Credentials,
) {
    use rustfs_iam::store::{Store as _, UserType};
    use rustfs_madmin::{AccountStatus, AddOrUpdateUserReq};
    use rustfs_policy::policy::Policy;

    struct ReadyIam(Arc<IamSys<ObjectStore>>);
    impl IamInterface for ReadyIam {
        fn handle(&self) -> Arc<IamSys<ObjectStore>> {
            Arc::clone(&self.0)
        }

        fn is_ready(&self) -> bool {
            true
        }
    }

    let (temp_dir, disk_paths, store) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
    for bucket in [PRIVATE, PUBLIC] {
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("create bucket");
    }
    let public_policy = serde_json::json!({"Version":"2012-10-17","Statement":[
        {"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":format!("arn:aws:s3:::{PUBLIC}/open/*")},
        {"Effect":"Deny","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":format!("arn:aws:s3:::{PUBLIC}/open/bucket-denied")},
        {"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":format!("arn:aws:s3:::{PUBLIC}/ip/*"),
            "Condition":{"IpAddress":{"aws:SourceIp":"203.0.113.0/24"}}},
        {"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:PutObjectLegalHold","Resource":format!("arn:aws:s3:::{PUBLIC}/body/*"),
            "Condition":{"StringEquals":{"s3:object-lock-legal-hold":"ON"}}},
        {"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:PutObjectTagging","Resource":format!("arn:aws:s3:::{PUBLIC}/body/*"),
            "Condition":{"StringEquals":{"s3:RequestObjectTag/class":"public"}}},
        {"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:PutObjectRetention","Resource":format!("arn:aws:s3:::{PUBLIC}/body/*"),
            "Condition":{"StringEquals":{"s3:object-lock-mode":"GOVERNANCE"},"NumericGreaterThanEquals":{"s3:object-lock-remaining-retention-days":"30"}}},
        {"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:PutObject","Resource":format!("arn:aws:s3:::{PUBLIC}/copy/*"),
            "Condition":{"StringEquals":{"s3:x-amz-copy-source":"src/key"}}},
        {"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:PutObject","Resource":format!("arn:aws:s3:::{PUBLIC}/stage/*"),
            "Condition":{"Bool":{"s3:ObjectCreationOperation":"false"}}},
        {"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:PutObject","Resource":format!("arn:aws:s3:::{PUBLIC}/post/*"),
            "Condition":{"StringEquals":{"s3:RequestObjectTag/class":"public"}}},
        {"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:PutBucketCors","Resource":format!("arn:aws:s3:::{PUBLIC}")}
    ]});
    store
        .update_bucket_metadata_config(
            PUBLIC,
            crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
            serde_json::to_vec(&public_policy).expect("serialize policy"),
        )
        .await
        .expect("publish public bucket policy");

    ObjectStore::new(Arc::clone(&store))
        .save_iam_config(
            serde_json::json!({"version": 1}),
            format!("{}/format.json", *rustfs_iam::store::object::IAM_CONFIG_PREFIX),
        )
        .await
        .expect("seed IAM format");
    let iam = rustfs_iam::build_iam_sys(Arc::clone(&store)).await.expect("initialize IAM");
    let access_key = "parity-user";
    iam.create_user(
        access_key,
        &AddOrUpdateUserReq {
            secret_key: "parity-user-secret-key".to_owned(),
            policy: None,
            status: AccountStatus::Enabled,
        },
    )
    .await
    .expect("create IAM user");
    iam.set_policy(
        "parity-user-policy",
        Policy::parse_config(
            format!(
                r#"{{"Version":"2012-10-17","Statement":[
                    {{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::{PRIVATE}/*"}},
                    {{"Effect":"Deny","Action":"s3:GetObject","Resource":"arn:aws:s3:::{PRIVATE}/iam-denied"}},
                    {{"Effect":"Allow","Action":"s3:PutBucketTagging","Resource":"arn:aws:s3:::{PRIVATE}"}}
                ]}}"#
            )
            .as_bytes(),
        )
        .expect("parse user policy"),
    )
    .await
    .expect("store user policy");
    iam.policy_db_set(access_key, UserType::Reg, false, "parity-user-policy")
        .await
        .expect("attach user policy");
    let user = iam.get_user(access_key).await.expect("load IAM user").credentials;
    let owner = rustfs_credentials::Credentials {
        access_key: "parity-owner".to_owned(),
        secret_key: "parity-owner-secret".to_owned(),
        ..Default::default()
    };
    let context = Arc::new(AppContext::new(Arc::clone(&store), Arc::new(ReadyIam(iam)), Arc::new(TestKms)));
    let slot = ServerContextSlot::new();
    assert!(slot.install(context), "install request context");
    ((temp_dir, disk_paths), store, slot, user, owner)
}

#[tokio::test]
#[serial]
async fn owned_and_legacy_entry_points_reach_the_same_decisions() {
    let (_store_dirs, _store, slot, user, owner) = decision_world().await;

    let cases = [
        DecisionCase {
            name: "user allowed by IAM",
            bucket: PRIVATE,
            object: "allowed",
            principal: Principal::User,
            action: S3Action::GetObjectAction,
            facts: no_facts,
            allowed: true,
        },
        DecisionCase {
            name: "user IAM explicit deny",
            bucket: PRIVATE,
            object: "iam-denied",
            principal: Principal::User,
            action: S3Action::GetObjectAction,
            facts: no_facts,
            allowed: false,
        },
        DecisionCase {
            name: "owner on private bucket",
            bucket: PRIVATE,
            object: "owner-only",
            principal: Principal::Owner,
            action: S3Action::GetObjectAction,
            facts: no_facts,
            allowed: true,
        },
        DecisionCase {
            name: "anonymous on private bucket",
            bucket: PRIVATE,
            object: "allowed",
            principal: Principal::Anonymous,
            action: S3Action::GetObjectAction,
            facts: no_facts,
            allowed: false,
        },
        DecisionCase {
            name: "anonymous on public bucket",
            bucket: PUBLIC,
            object: "open/readme",
            principal: Principal::Anonymous,
            action: S3Action::GetObjectAction,
            facts: no_facts,
            allowed: true,
        },
        DecisionCase {
            name: "bucket-policy explicit deny",
            bucket: PUBLIC,
            object: "open/bucket-denied",
            principal: Principal::Anonymous,
            action: S3Action::GetObjectAction,
            facts: no_facts,
            allowed: false,
        },
        DecisionCase {
            name: "SourceIp from the peer, inside",
            bucket: PUBLIC,
            object: "ip/file",
            principal: Principal::Anonymous,
            action: S3Action::GetObjectAction,
            facts: |req| peer(req, [203, 0, 113, 9]),
            allowed: true,
        },
        DecisionCase {
            name: "SourceIp from the peer, outside",
            bucket: PUBLIC,
            object: "ip/file",
            principal: Principal::Anonymous,
            action: S3Action::GetObjectAction,
            facts: |req| peer(req, [198, 51, 100, 1]),
            allowed: false,
        },
        DecisionCase {
            name: "SourceIp from the verified client",
            bucket: PUBLIC,
            object: "ip/file",
            principal: Principal::Anonymous,
            action: S3Action::GetObjectAction,
            facts: |req| {
                peer(req, [198, 51, 100, 1]);
                req.extensions
                    .insert(rustfs_trusted_proxies::ClientInfo::direct(std::net::SocketAddr::from((
                        [203, 0, 113, 9],
                        443,
                    ))));
            },
            allowed: true,
        },
        DecisionCase {
            name: "legal hold body ON, header OFF",
            bucket: PUBLIC,
            object: "body/obj",
            principal: Principal::Anonymous,
            action: S3Action::PutObjectLegalHoldAction,
            facts: |req| {
                req.headers
                    .insert("x-amz-object-lock-legal-hold", HeaderValue::from_static("OFF"));
                req.extensions.insert(RequestedObjectLegalHold(ObjectLockLegalHold {
                    status: Some(ObjectLockLegalHoldStatus::from_static(ObjectLockLegalHoldStatus::ON)),
                }));
            },
            allowed: true,
        },
        DecisionCase {
            name: "legal hold body OFF, header ON",
            bucket: PUBLIC,
            object: "body/obj",
            principal: Principal::Anonymous,
            action: S3Action::PutObjectLegalHoldAction,
            facts: |req| {
                req.headers
                    .insert("x-amz-object-lock-legal-hold", HeaderValue::from_static("ON"));
                req.extensions.insert(RequestedObjectLegalHold(ObjectLockLegalHold {
                    status: Some(ObjectLockLegalHoldStatus::from_static(ObjectLockLegalHoldStatus::OFF)),
                }));
            },
            allowed: false,
        },
        DecisionCase {
            name: "tagging body public, header secret",
            bucket: PUBLIC,
            object: "body/obj",
            principal: Principal::Anonymous,
            action: S3Action::PutObjectTaggingAction,
            facts: |req| {
                req.headers.insert("x-amz-tagging", HeaderValue::from_static("class=secret"));
                req.extensions.insert(RequestedObjectTags(Tagging {
                    tag_set: vec![Tag {
                        key: Some("class".to_owned()),
                        value: Some("public".to_owned()),
                    }],
                }));
            },
            allowed: true,
        },
        DecisionCase {
            name: "tagging body secret, header public",
            bucket: PUBLIC,
            object: "body/obj",
            principal: Principal::Anonymous,
            action: S3Action::PutObjectTaggingAction,
            facts: |req| {
                req.headers.insert("x-amz-tagging", HeaderValue::from_static("class=public"));
                req.extensions.insert(RequestedObjectTags(Tagging {
                    tag_set: vec![Tag {
                        key: Some("class".to_owned()),
                        value: Some("secret".to_owned()),
                    }],
                }));
            },
            allowed: false,
        },
        DecisionCase {
            name: "retention body governance for 40 days",
            bucket: PUBLIC,
            object: "body/obj",
            principal: Principal::Anonymous,
            action: S3Action::PutObjectRetentionAction,
            facts: |req| retain(req, "GOVERNANCE", 40),
            allowed: true,
        },
        DecisionCase {
            name: "retention body compliance",
            bucket: PUBLIC,
            object: "body/obj",
            principal: Principal::Anonymous,
            action: S3Action::PutObjectRetentionAction,
            facts: |req| retain(req, "COMPLIANCE", 40),
            allowed: false,
        },
        DecisionCase {
            name: "retention for 10 days",
            bucket: PUBLIC,
            object: "body/obj",
            principal: Principal::Anonymous,
            action: S3Action::PutObjectRetentionAction,
            facts: |req| retain(req, "GOVERNANCE", 10),
            allowed: false,
        },
        DecisionCase {
            name: "resolved copy source matches",
            bucket: PUBLIC,
            object: "copy/obj",
            principal: Principal::Anonymous,
            action: S3Action::PutObjectAction,
            facts: |req| {
                req.headers.insert("x-amz-copy-source", HeaderValue::from_static("other/key"));
                req.extensions.insert(RequestedCopySource("src/key".to_owned()));
            },
            allowed: true,
        },
        DecisionCase {
            name: "resolved copy source differs",
            bucket: PUBLIC,
            object: "copy/obj",
            principal: Principal::Anonymous,
            action: S3Action::PutObjectAction,
            facts: |req| {
                req.headers.insert("x-amz-copy-source", HeaderValue::from_static("src/key"));
                req.extensions.insert(RequestedCopySource("other/key".to_owned()));
            },
            allowed: false,
        },
        DecisionCase {
            name: "POST form tag public, header secret",
            bucket: PUBLIC,
            object: "post/obj",
            principal: Principal::Anonymous,
            action: S3Action::PutObjectAction,
            facts: |req| post_form(req, "class=public", "class=secret"),
            allowed: true,
        },
        DecisionCase {
            name: "POST form tag secret, header public",
            bucket: PUBLIC,
            object: "post/obj",
            principal: Principal::Anonymous,
            action: S3Action::PutObjectAction,
            facts: |req| post_form(req, "class=secret", "class=public"),
            allowed: false,
        },
        DecisionCase {
            name: "multipart staging does not create an object",
            bucket: PUBLIC,
            object: "stage/obj",
            principal: Principal::Anonymous,
            action: S3Action::PutObjectAction,
            facts: |req| {
                req.extensions.insert(MultipartStagingOperation);
            },
            allowed: true,
        },
        DecisionCase {
            name: "a plain PutObject creates an object",
            bucket: PUBLIC,
            object: "stage/obj",
            principal: Principal::Anonymous,
            action: S3Action::PutObjectAction,
            facts: no_facts,
            allowed: false,
        },
    ];

    let mut mismatches = Vec::new();
    for case in cases {
        let mut legacy = build_request((), Method::PUT);
        legacy.uri = format!("/{}/{}", case.bucket, case.object).parse().expect("case uri");
        legacy.extensions.insert(Arc::clone(&slot));
        let (cred, is_owner) = match case.principal {
            Principal::Anonymous => (None, false),
            Principal::User => (Some(user.clone()), false),
            Principal::Owner => (Some(owner.clone()), true),
        };
        legacy.extensions.insert(ReqInfo {
            cred,
            is_owner,
            bucket: Some(case.bucket.to_owned()),
            object: Some(case.object.to_owned()),
            ..Default::default()
        });
        (case.facts)(&mut legacy);
        let mut owned = RequestEnvelope::from_s3s(&legacy);
        // The object shape both edges' access hooks call: it must reach the same decision.
        let mut owned_shape = RequestEnvelope::from_s3s(&legacy);
        let action = Action::S3Action(case.action);
        let legacy_result = crate::app::storage_api::owned_parity::authorize_request(&mut legacy, action).await;
        let owned_result = crate::app::authorize_owned_request(&mut owned, action, None).await;
        let shape_result = crate::app::storage_api::owned_parity::authorize_object_request(
            &mut owned_shape,
            case.bucket,
            case.object,
            None,
            action,
        )
        .await;
        let legacy_ok = match &legacy_result {
            Ok(()) => case.allowed,
            Err(err) => !case.allowed && err.code() == &S3ErrorCode::AccessDenied,
        };
        let owned_ok = match &owned_result {
            Ok(()) => case.allowed,
            Err(err) => !case.allowed && err.code() == &rustfs_s3_types::S3ErrorCode::AccessDenied,
        };
        let shape_ok = match &shape_result {
            Ok(()) => case.allowed,
            Err(err) => !case.allowed && err.code() == &S3ErrorCode::AccessDenied,
        };
        if !legacy_ok || !owned_ok || !shape_ok {
            mismatches.push(format!(
                "{} (expected allowed={}): legacy {legacy_result:?}, owned {owned_result:?}, object shape {shape_result:?}",
                case.name, case.allowed
            ));
        }
    }
    // Every case is evaluated before failing, so one report names each broken carrier.
    assert!(mismatches.is_empty(), "decision mismatches:\n{}", mismatches.join("\n"));
}

fn retain(req: &mut S3Request<()>, mode: &'static str, days: i64) {
    let retention = ObjectLockRetention {
        mode: Some(mode.to_owned().into()),
        ..Default::default()
    };
    req.extensions.insert(RequestedObjectLockRetention(retention));
    req.extensions.insert(RequestedObjectLockRetainUntil(
        time::OffsetDateTime::now_utc() + time::Duration::days(days),
    ));
    // Headers name a different mode; only the body may describe the request.
    req.headers
        .insert("x-amz-object-lock-mode", HeaderValue::from_static("COMPLIANCE"));
}

fn post_form(req: &mut S3Request<()>, form_tagging: &'static str, header_tagging: &'static str) {
    req.method = Method::POST;
    req.headers.insert("x-amz-tagging", HeaderValue::from_static(header_tagging));
    let mut form = req.headers.clone();
    form.insert("x-amz-tagging", HeaderValue::from_static(form_tagging));
    req.extensions.insert(PostObjectRequestMarker);
    req.extensions.insert(PostObjectPolicyHeaders(form));
}

/// What a bucket-configuration mutation answered, compared across the two request forms.
#[derive(Debug, PartialEq)]
enum MutationOutcome {
    /// Allowed, with the incarnation snapshot the use case reads back.
    Allowed(Option<uuid::Uuid>),
    Denied,
    /// Refused with another code after the decision, such as a snapshot failure.
    Failed(String),
}

#[tokio::test]
#[serial]
async fn owned_and_legacy_bucket_config_mutations_hand_on_the_same_decision_and_snapshot() {
    use crate::app::storage_api::owned_parity::{authorize_bucket_config_mutation, bucket_config_mutation_incarnation};

    const MISSING: &str = "parity-missing";
    let (_store_dirs, store, slot, user, owner) = decision_world().await;
    let private = Some(store.bucket_incarnation_id(PRIVATE).await.expect("private incarnation"));
    let public = Some(store.bucket_incarnation_id(PUBLIC).await.expect("public incarnation"));
    let failed = |code: &str| MutationOutcome::Failed(code.to_owned());
    let cases = [
        (
            "IAM policy allows the user",
            PRIVATE,
            Principal::User,
            S3Action::PutBucketTaggingAction,
            MutationOutcome::Allowed(private),
        ),
        (
            "bucket policy allows anonymous",
            PUBLIC,
            Principal::Anonymous,
            S3Action::PutBucketCorsAction,
            MutationOutcome::Allowed(public),
        ),
        (
            "owner",
            PRIVATE,
            Principal::Owner,
            S3Action::PutBucketVersioningAction,
            MutationOutcome::Allowed(private),
        ),
        (
            "user policy names another bucket",
            PUBLIC,
            Principal::User,
            S3Action::PutBucketTaggingAction,
            MutationOutcome::Denied,
        ),
        (
            "user without a policy for the action",
            PRIVATE,
            Principal::User,
            S3Action::PutBucketCorsAction,
            MutationOutcome::Denied,
        ),
        (
            "anonymous on a private bucket",
            PRIVATE,
            Principal::Anonymous,
            S3Action::PutBucketTaggingAction,
            MutationOutcome::Denied,
        ),
        (
            "anonymous on a missing bucket",
            MISSING,
            Principal::Anonymous,
            S3Action::PutBucketCorsAction,
            MutationOutcome::Denied,
        ),
        (
            "owner on a missing bucket",
            MISSING,
            Principal::Owner,
            S3Action::PutBucketVersioningAction,
            failed("NoSuchBucket"),
        ),
    ];

    let mut mismatches = Vec::new();
    for (name, bucket, principal, action, expected) in cases {
        let mut legacy = build_request((), Method::PUT);
        legacy.uri = format!("/{bucket}").parse().expect("case uri");
        legacy.extensions.insert(Arc::clone(&slot));
        let (cred, is_owner) = match principal {
            Principal::Anonymous => (None, false),
            Principal::User => (Some(user.clone()), false),
            Principal::Owner => (Some(owner.clone()), true),
        };
        legacy.extensions.insert(ReqInfo {
            cred,
            is_owner,
            ..Default::default()
        });
        let mut owned = RequestEnvelope::from_s3s(&legacy);
        let action = Action::S3Action(action);

        let legacy_result = authorize_bucket_config_mutation(&slot, &mut legacy, bucket, action).await;
        let owned_result = authorize_bucket_config_mutation(&slot, &mut owned, bucket, action).await;
        let outcome = |result: Result<(), S3ErrorCode>, snapshot: Result<Option<uuid::Uuid>, S3ErrorCode>| match result {
            Ok(()) => MutationOutcome::Allowed(snapshot.ok().flatten()),
            Err(S3ErrorCode::AccessDenied) => MutationOutcome::Denied,
            Err(code) => MutationOutcome::Failed(code.as_str().to_owned()),
        };
        let legacy_outcome = outcome(
            legacy_result.map_err(|err| err.code().clone()),
            bucket_config_mutation_incarnation(&legacy, bucket).map_err(|err| err.code().clone()),
        );
        let owned_outcome = outcome(
            owned_result.map_err(|err| err.code().clone()),
            bucket_config_mutation_incarnation(&owned, bucket).map_err(|err| err.code().clone()),
        );
        // A refused request hands no snapshot on: the use case's reader refuses to run without one.
        let refused_without_snapshot = |snapshot: Result<Option<uuid::Uuid>, S3ErrorCode>| {
            matches!(expected, MutationOutcome::Allowed(_)) || snapshot == Err(S3ErrorCode::InternalError)
        };
        let both_refused_without_snapshot =
            refused_without_snapshot(bucket_config_mutation_incarnation(&legacy, bucket).map_err(|err| err.code().clone()))
                && refused_without_snapshot(bucket_config_mutation_incarnation(&owned, bucket).map_err(|err| err.code().clone()));
        let bucket_recorded = [legacy.extensions.get::<ReqInfo>(), owned.req_info()]
            .iter()
            .all(|info| info.and_then(|info| info.bucket.as_deref()) == Some(bucket));
        if legacy_outcome != expected || owned_outcome != expected || !both_refused_without_snapshot || !bucket_recorded {
            mismatches.push(format!(
                "{name}: expected {expected:?}, legacy {legacy_outcome:?}, owned {owned_outcome:?}"
            ));
        }
    }
    assert!(mismatches.is_empty(), "bucket config mutation mismatches:\n{}", mismatches.join("\n"));
}
