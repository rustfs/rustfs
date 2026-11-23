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

use super::{AuditRequestView, OperationHelper};
use crate::server::{refresh_audit_module_enabled, refresh_notify_module_enabled};
use crate::shared_types::RemoteAddr;
use crate::storage::authorization::{BucketGenerationGuard, PendingDeleteBucketGenerationGuard};
use crate::storage::req_info::ReqInfo;
use crate::storage::request_context::RequestContext;
use http::{Extensions, HeaderMap, HeaderValue, Method, StatusCode, Uri};
use rustfs_audit::ObjectVersion;
use rustfs_credentials::Credentials;
use rustfs_s3_ops::S3Operation;
use rustfs_s3_types::{EventName, S3Error, S3ErrorCode, S3Result};
use rustfs_utils::http::headers::{AMZ_REQUEST_ID, REQUEST_ID_HEADER};
use std::sync::Arc;
use temp_env::{async_with_vars, with_vars};

#[cfg(feature = "diagnostic-read-version-caller")]
#[tokio::test]
async fn diagnostic_background_tasks_keep_existing_entry_across_nested_spawns() {
    use super::ecstore_diagnostic::{ReadVersionEntrySource, current_read_version_entry_source, with_read_version_entry};
    use rustfs_io_metrics::s3_http_metrics::{
        S3HttpRequestGuard, current_s3_http_operation_context, current_s3_http_operation_name, observe_s3_http_operation,
        with_s3_http_operation_context,
    };

    let mut request = S3HttpRequestGuard::new("GET");
    let operation = request.in_scope(|| {
        observe_s3_http_operation(S3Operation::GetObject);
        current_s3_http_operation_context().expect("captured GET operation")
    });
    for has_request_context in [false, true] {
        let context = has_request_context.then(|| RequestContext::from_headers(&HeaderMap::new()));
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let (tx, rx) = tokio::sync::oneshot::channel();
        with_s3_http_operation_context(Some(operation), async {
            with_read_version_entry(ReadVersionEntrySource::TierConfigReload, async {
                super::spawn_background_with_context(context, async move {
                    release_rx.await.expect("parent releases background work");
                    with_read_version_entry(ReadVersionEntrySource::ConfigRead, async move {
                        super::spawn_background_with_context(None, async move {
                            tx.send((current_s3_http_operation_name(), current_read_version_entry_source()))
                                .expect("context observer remains live");
                        });
                    })
                    .await;
                });
            })
            .await;
        })
        .await;
        assert_eq!(current_read_version_entry_source(), None);
        release_tx.send(()).expect("detached background work remains alive");
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(5), rx)
                .await
                .expect("background completion deadline")
                .expect("context result"),
            (Some("s3:GetObject"), Some(ReadVersionEntrySource::TierConfigReload))
        );
    }
    drop(request);
    let (tx, rx) = tokio::sync::oneshot::channel();
    super::spawn_background(async move {
        tx.send((current_s3_http_operation_name(), current_read_version_entry_source()))
            .expect("unscoped observer remains live");
    });
    assert_eq!(rx.await.expect("unscoped task completes"), (None, None));
}

#[tokio::test]
async fn owned_mutation_keeps_keystone_context_and_audits_panics_as_failure() {
    let bucket = "owned-mutation-panic";
    let probe = super::MutationCompletionProbe::install(bucket);
    let credentials = Credentials {
        access_key: "keystone:test".to_owned(),
        ..Default::default()
    };
    let result: S3Result<()> = rustfs_keystone::KEYSTONE_CREDENTIALS
        .scope(Some(credentials), async {
            super::run_owned_mutation(async {
                assert_eq!(
                    rustfs_keystone::KEYSTONE_CREDENTIALS
                        .with(|credentials| credentials.as_ref().map(|value| value.access_key.clone())),
                    Some("keystone:test".to_owned())
                );
                let req = request(Method::DELETE, Uri::from_static("/owned-mutation-panic/object"), |_, _| {});
                let _helper =
                    OperationHelper::from_view(&req.audit_view(), EventName::ObjectRemovedDelete, S3Operation::DeleteObject);
                panic!("injected mutation panic before reporting a commit");
            })
            .await
        })
        .await;
    assert_eq!(result.expect_err("join failure").code(), &S3ErrorCode::InternalError);
    probe.wait_for(1, 0).await;
    let audits = probe.audits.lock().expect("audits");
    assert_eq!(audits.len(), 1);
    assert_eq!(audits[0].api.status.as_deref(), Some("failure"));
    assert_eq!(audits[0].api.status_code, Some(500));
    assert!(probe.events.lock().expect("events").is_empty());
}

#[cfg(feature = "diagnostic-read-version-caller")]
#[test]
fn diagnostic_background_tasks_keep_existing_entry_on_fallback_thread() {
    use super::ecstore_diagnostic::{ReadVersionEntrySource, current_read_version_entry_source, with_read_version_entry};
    use rustfs_io_metrics::s3_http_metrics::{
        S3HttpRequestGuard, current_s3_http_operation_context, current_s3_http_operation_name, observe_s3_http_operation,
        with_s3_http_operation_context,
    };

    assert!(tokio::runtime::Handle::try_current().is_err());
    let mut request = S3HttpRequestGuard::new("DELETE");
    let operation = request.in_scope(|| {
        observe_s3_http_operation(S3Operation::DeleteObject);
        current_s3_http_operation_context().expect("captured DELETE operation")
    });
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let (tx, rx) = std::sync::mpsc::channel();
    futures::executor::block_on(with_s3_http_operation_context(Some(operation), async {
        with_read_version_entry(ReadVersionEntrySource::TierConfigReload, async {
            super::spawn_background(async move {
                release_rx.await.expect("parent releases fallback work");
                tx.send((current_s3_http_operation_name(), current_read_version_entry_source()))
                    .expect("fallback observer remains live");
            });
        })
        .await;
    }));
    drop(request);
    assert_eq!(current_read_version_entry_source(), None);
    release_tx.send(()).expect("fallback work remains alive");
    assert_eq!(
        rx.recv_timeout(std::time::Duration::from_secs(5))
            .expect("fallback thread completes"),
        (Some("s3:DeleteObject"), Some(ReadVersionEntrySource::TierConfigReload))
    );
    assert_eq!(current_s3_http_operation_name(), None);
}

#[tokio::test]
async fn background_tasks_keep_operation_after_parent_scope_ends() {
    use rustfs_io_metrics::s3_http_metrics::{S3HttpRequestGuard, current_s3_http_operation_name, observe_s3_http_operation};

    for context in [None, Some(RequestContext::from_headers(&HeaderMap::new()))] {
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let (completed_tx, completed_rx) = tokio::sync::oneshot::channel();
        let mut request = S3HttpRequestGuard::new("GET");
        request.in_scope(|| {
            observe_s3_http_operation(S3Operation::GetObject);
            super::spawn_background_with_context(context, async move {
                release_rx.await.expect("parent releases detached task");
                completed_tx
                    .send(current_s3_http_operation_name())
                    .expect("receiver remains live");
            });
        });
        drop(request);
        assert_eq!(current_s3_http_operation_name(), None);
        release_tx.send(()).expect("detached task remains live");
        let observed = tokio::time::timeout(std::time::Duration::from_secs(5), completed_rx)
            .await
            .expect("detached completion deadline")
            .expect("detached task sends operation");
        assert_eq!(observed, Some("s3:GetObject"));
    }

    let (tx, rx) = tokio::sync::oneshot::channel();
    super::spawn_background_with_context(None, async move {
        tx.send(current_s3_http_operation_name())
            .expect("internal receiver remains live");
    });
    assert_eq!(rx.await.expect("internal task completes"), None);
}

#[test]
fn background_tasks_keep_operation_on_fallback_thread() {
    use rustfs_io_metrics::s3_http_metrics::{S3HttpRequestGuard, current_s3_http_operation_name, observe_s3_http_operation};

    assert!(tokio::runtime::Handle::try_current().is_err());
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let (tx, rx) = std::sync::mpsc::channel();
    let mut request = S3HttpRequestGuard::new("DELETE");
    request.in_scope(|| {
        observe_s3_http_operation(S3Operation::DeleteObject);
        super::spawn_background_with_context(Some(RequestContext::from_headers(&HeaderMap::new())), async move {
            release_rx.await.expect("parent releases fallback thread");
            tx.send(current_s3_http_operation_name())
                .expect("fallback receiver remains live");
        });
    });
    drop(request);
    release_tx.send(()).expect("fallback task remains live");
    assert_eq!(
        rx.recv_timeout(std::time::Duration::from_secs(5))
            .expect("fallback thread completes"),
        Some("s3:DeleteObject")
    );
    assert_eq!(current_s3_http_operation_name(), None);
}

#[test]
fn notification_view_uses_the_generation_validated_for_deferred_deletes() {
    let ordinary = uuid::Uuid::new_v4();
    let deleted = uuid::Uuid::new_v4();
    let generation = |pending: Option<PendingDeleteBucketGenerationGuard>| {
        let envelope = request(Method::DELETE, Uri::from_static("/bucket/object"), |_, extensions| {
            extensions.insert(BucketGenerationGuard {
                bucket: "bucket".to_owned(),
                incarnation_id: ordinary,
            });
            if let Some(pending) = pending {
                extensions.insert(pending);
            }
        });
        envelope.audit_view().bucket_generation.map(|guard| guard.incarnation_id)
    };
    assert_eq!(
        generation(Some(PendingDeleteBucketGenerationGuard::Ready(BucketGenerationGuard {
            bucket: "bucket".to_owned(),
            incarnation_id: deleted,
        }))),
        Some(deleted),
        "delete notifications must use the deferred guard that storage validates"
    );
    assert_eq!(
        generation(Some(PendingDeleteBucketGenerationGuard::Failed {
            code: rustfs_s3_types::S3ErrorCode::NoSuchBucket,
            message: "bucket generation unavailable".to_owned(),
        })),
        None,
        "a failed deferred capture must not fall back to another generation"
    );
    assert_eq!(generation(None), Some(ordinary), "ordinary operations retain their original generation");
}

struct TestAuditRequest {
    headers: HeaderMap,
    uri: Uri,
    extensions: Extensions,
}

impl TestAuditRequest {
    fn audit_view(&self) -> AuditRequestView<'_> {
        let remote_addr = self
            .extensions
            .get::<Option<RemoteAddr>>()
            .and_then(Option::as_ref)
            .map(|addr| addr.0);
        let bucket_generation = PendingDeleteBucketGenerationGuard::resolve_generation(
            self.extensions.get::<BucketGenerationGuard>(),
            self.extensions.get::<PendingDeleteBucketGenerationGuard>(),
        )
        .ok()
        .flatten();
        AuditRequestView::from_facts(
            &self.headers,
            &self.uri,
            self.extensions.get::<ReqInfo>(),
            self.extensions.get::<rustfs_trusted_proxies::ClientInfo>(),
            remote_addr,
            self.extensions.get::<RequestContext>(),
            bucket_generation,
        )
    }

    fn req_info(&self) -> Option<&ReqInfo> {
        self.extensions.get()
    }
}

fn request(_method: Method, uri: Uri, configure: impl FnOnce(&mut HeaderMap, &mut Extensions)) -> TestAuditRequest {
    let mut headers = HeaderMap::new();
    let mut extensions = Extensions::new();
    configure(&mut headers, &mut extensions);
    TestAuditRequest {
        headers,
        uri,
        extensions,
    }
}

#[test]
fn operation_helper_uses_req_info_for_notification_context() {
    with_vars(
        [
            (rustfs_config::ENV_NOTIFY_ENABLE, Some("true")),
            (rustfs_config::ENV_AUDIT_ENABLE, Some("true")),
        ],
        || {
            refresh_notify_module_enabled();
            refresh_audit_module_enabled();

            let req = request(Method::DELETE, Uri::from_static("/from-uri/ignored"), |headers, extensions| {
                headers.insert("host", HeaderValue::from_static("example.com"));
                headers.insert("user-agent", HeaderValue::from_static("rustfs-test"));
                extensions.insert(ReqInfo {
                    cred: Some(Credentials {
                        access_key: "notifyTag".to_string(),
                        ..Default::default()
                    }),
                    bucket: Some("issue-2292-bucket".to_string()),
                    object: Some("prefix/issue-2292.txt".to_string()),
                    version_id: Some("version-123".to_string()),
                    ..Default::default()
                });
            });
            let helper =
                OperationHelper::from_view(&req.audit_view(), EventName::ObjectTaggingPut, S3Operation::PutObjectTagging);
            let OperationHelper::Enabled(state) = &helper else {
                panic!("helper should be enabled when notify/audit switches are on");
            };
            let event_args = state.event_builder.clone().expect("event builder should exist").build();

            assert_eq!(event_args.bucket_name, "issue-2292-bucket");
            assert_eq!(event_args.object.bucket, "issue-2292-bucket");
            assert_eq!(event_args.object.name, "prefix/issue-2292.txt");
            assert_eq!(event_args.version_id, "version-123");
            assert_eq!(event_args.req_params.get("principalId").map(String::as_str), Some("notifyTag"));
        },
    );
}

/// Operations without a notification event still produce an audit entry, and the
/// audit-only constructor never builds a notification event, even with notify on.
#[test]
fn audit_only_helper_audits_operations_without_a_notification_event() {
    with_vars(
        [
            (rustfs_config::ENV_NOTIFY_ENABLE, Some("true")),
            (rustfs_config::ENV_AUDIT_ENABLE, Some("true")),
        ],
        || {
            refresh_notify_module_enabled();
            refresh_audit_module_enabled();

            for (op, uri, expected_event) in [
                (S3Operation::UploadPart, "/audit-bucket/object.txt", EventName::AuditS3Operation),
                (S3Operation::UploadPartCopy, "/audit-bucket/object.txt", EventName::AuditS3Operation),
                (S3Operation::PutBucketPolicy, "/audit-bucket", EventName::AuditS3Operation),
                (S3Operation::PutBucketVersioning, "/audit-bucket", EventName::AuditS3Operation),
                (
                    S3Operation::AbortMultipartUpload,
                    "/audit-bucket/object.txt",
                    EventName::ObjectRemovedAbortMultipartUpload,
                ),
            ] {
                let req = request(Method::PUT, Uri::from_static(uri), |_, extensions| {
                    extensions.insert(ReqInfo {
                        cred: Some(Credentials {
                            access_key: "auditor".to_string(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    });
                });
                let result = Ok(());
                let mut helper = OperationHelper::audit_only_from_view(&req.audit_view(), op).complete_owned(
                    &result,
                    StatusCode::OK,
                    &HeaderMap::new(),
                );
                assert!(!helper.wants_object_info(), "{} must not build a notification event", op.as_str());
                assert!(helper.event_args().is_none(), "{} must not build a notification event", op.as_str());
                let OperationHelper::Enabled(state) = &mut helper else {
                    panic!("helper should be enabled when the audit switch is on");
                };
                let entry = state.audit_builder.take().expect("audit builder should exist").build();
                assert_eq!(entry.event, expected_event, "{}", op.as_str());
                assert_eq!(entry.api.name.as_deref(), Some(op.as_str()));
                assert_eq!(entry.api.bucket.as_deref(), Some("audit-bucket"));
                assert_eq!(entry.api.status.as_deref(), Some("success"));
                assert_eq!(entry.access_key.as_deref(), Some("auditor"));
            }
        },
    );
}

/// With audit off, the audit-only helper does no work: it is the disabled helper.
#[test]
fn audit_only_helper_is_disabled_when_audit_is_off() {
    with_vars(
        [
            (rustfs_config::ENV_NOTIFY_ENABLE, Some("true")),
            (rustfs_config::ENV_AUDIT_ENABLE, Some("false")),
        ],
        || {
            refresh_notify_module_enabled();
            refresh_audit_module_enabled();

            let req = request(Method::PUT, Uri::from_static("/audit-bucket/object.txt"), |_, _| {});
            assert!(matches!(
                OperationHelper::audit_only_from_view(&req.audit_view(), S3Operation::UploadPart),
                OperationHelper::Disabled
            ));
        },
    );
}

/// A failed audit-only operation is audited with a failure status, the error message and
/// the error's status code.
#[test]
fn audit_only_helper_records_failure_status_and_error() {
    with_vars(
        [
            (rustfs_config::ENV_NOTIFY_ENABLE, Some("false")),
            (rustfs_config::ENV_AUDIT_ENABLE, Some("true")),
        ],
        || {
            refresh_notify_module_enabled();
            refresh_audit_module_enabled();

            let req = request(Method::PUT, Uri::from_static("/audit-bucket"), |_, _| {});
            let result: S3Result<()> = Err(S3Error::with_message(S3ErrorCode::AccessDenied, "policy refused".to_string()));
            let mut helper = OperationHelper::audit_only_from_view(&req.audit_view(), S3Operation::PutBucketPolicy)
                .complete_owned(&result, StatusCode::OK, &HeaderMap::new());
            let OperationHelper::Enabled(state) = &mut helper else {
                panic!("helper should be enabled when the audit switch is on");
            };
            let entry = state.audit_builder.take().expect("audit builder should exist").build();
            assert_eq!(entry.api.status.as_deref(), Some("failure"));
            assert_eq!(entry.api.status_code, Some(403));
            assert_eq!(entry.error.as_deref(), Some("policy refused"));
        },
    );
}

#[test]
fn operation_helper_audits_the_authenticated_request_identity() {
    with_vars(
        [
            (rustfs_config::ENV_NOTIFY_ENABLE, Some("false")),
            (rustfs_config::ENV_AUDIT_ENABLE, Some("true")),
        ],
        || {
            refresh_notify_module_enabled();
            refresh_audit_module_enabled();

            for (access_key, parent_user) in [
                (Some("testuser"), ""),
                (Some("rustfsadmin"), ""),
                (Some("service-account"), "testuser"),
                (Some("sts-session"), "testuser"),
                (None, ""),
                (Some(""), ""),
                (Some(""), "unverified-parent"),
            ] {
                let expected_access_key = access_key.filter(|key| !key.is_empty());
                for outcome in [None, Some(true), Some(false)] {
                    let req = request(Method::GET, Uri::from_static("/audit-bucket/object.txt"), |_, extensions| {
                        extensions.insert(ReqInfo {
                            cred: access_key.map(|key| Credentials {
                                access_key: key.to_string(),
                                secret_key: "audit-secret-must-not-leak".to_string(),
                                session_token: "audit-token-must-not-leak".to_string(),
                                parent_user: parent_user.to_string(),
                                claims: match key {
                                    "service-account" => Some(std::collections::HashMap::from([(
                                        rustfs_credentials::IAM_POLICY_CLAIM_NAME_SA.to_string(),
                                        serde_json::json!(rustfs_credentials::INHERITED_POLICY_TYPE),
                                    )])),
                                    "sts-session" => Some(std::collections::HashMap::from([(
                                        "sessionPolicy".to_string(),
                                        serde_json::json!("private-session-policy"),
                                    )])),
                                    _ => None,
                                },
                                ..Default::default()
                            }),
                            ..Default::default()
                        });
                    });
                    let mut helper =
                        OperationHelper::from_view(&req.audit_view(), EventName::ObjectAccessedGet, S3Operation::GetObject);
                    if let Some(success) = outcome {
                        let result = if success {
                            Ok(())
                        } else {
                            Err(S3Error::new(S3ErrorCode::NoSuchKey))
                        };
                        helper = helper.complete_owned(&result, StatusCode::OK, &HeaderMap::new());
                    }
                    let OperationHelper::Enabled(state) = &mut helper else {
                        panic!("helper should be enabled when the audit switch is on");
                    };
                    let entry = state.audit_builder.take().expect("audit builder should exist").build();
                    assert_eq!(entry.access_key.as_deref(), expected_access_key);
                    let json = serde_json::to_value(&entry).expect("audit entry should serialize");
                    assert_eq!(json.get("accessKey").cloned(), expected_access_key.map(serde_json::Value::from));
                    let expected_parent_user = (expected_access_key.is_some() && !parent_user.is_empty()).then_some(parent_user);
                    assert_eq!(entry.parent_user.as_deref(), expected_parent_user);
                    assert_eq!(json.get("parentUser").cloned(), expected_parent_user.map(serde_json::Value::from));
                    assert!(!json.as_object().unwrap().contains_key("requestClaims"));
                    let rendered = serde_json::to_string(&entry).expect("audit entry should serialize");
                    assert!(!rendered.contains("audit-secret-must-not-leak"));
                    assert!(!rendered.contains("audit-token-must-not-leak"));
                    assert!(!rendered.contains("private-session-policy"));
                }
            }
        },
    );
}

fn oidc_audit_credentials(issuer: Option<&str>) -> Credentials {
    let mut claims = std::collections::HashMap::from([
        ("sub".to_string(), serde_json::json!(" subject-9170 ")),
        ("iss".to_string(), serde_json::json!("rustfs-oidc")),
        ("oidc_provider".to_string(), serde_json::json!("default")),
        ("email".to_string(), serde_json::json!("private@example.test")),
        ("preferred_username".to_string(), serde_json::json!("private-user")),
        ("groups".to_string(), serde_json::json!(["private-group"])),
        ("roles".to_string(), serde_json::json!(["private-role"])),
        ("policy".to_string(), serde_json::json!("private-policy")),
        ("sessionPolicy".to_string(), serde_json::json!("private-encoded-policy")),
        ("sessionPolicy-extracted".to_string(), serde_json::json!("private-decoded-policy")),
        ("custom".to_string(), serde_json::json!({"token": "private-custom-token"})),
    ]);
    if let Some(issuer) = issuer {
        claims.insert(rustfs_iam::sys::OIDC_ISSUER_CLAIM.to_string(), serde_json::json!(issuer));
    }
    Credentials {
        access_key: "oidc-session".to_string(),
        parent_user: "oidc-parent".to_string(),
        secret_key: "audit-secret-must-not-leak".to_string(),
        session_token: "audit-token-must-not-leak".to_string(),
        claims: Some(claims),
        ..Default::default()
    }
}

#[test]
fn operation_helper_preserves_oidc_audit_identity_without_sensitive_claims() {
    with_vars(
        [
            (rustfs_config::ENV_NOTIFY_ENABLE, Some("false")),
            (rustfs_config::ENV_AUDIT_ENABLE, Some("true")),
        ],
        || {
            refresh_notify_module_enabled();
            refresh_audit_module_enabled();

            for issuer in [Some("https://idp-a.example.test"), Some("https://idp-b.example.test"), None] {
                for service_account in [false, true] {
                    let mut credentials = oidc_audit_credentials(issuer);
                    if service_account {
                        credentials.access_key = "oidc-service-account".to_string();
                        credentials.claims.as_mut().unwrap().insert(
                            rustfs_credentials::IAM_POLICY_CLAIM_NAME_SA.to_string(),
                            serde_json::json!(rustfs_credentials::INHERITED_POLICY_TYPE),
                        );
                    }
                    let before = serde_json::to_value(&credentials).expect("credentials should serialize");
                    let mut expected_claims = serde_json::json!({
                        "sub": " subject-9170 ",
                        "iss": "rustfs-oidc",
                        "oidc_provider": "default"
                    });
                    if let Some(issuer) = issuer {
                        expected_claims["x-rustfs-internal-oidc-issuer"] = serde_json::json!(issuer);
                    }

                    for outcome in [None, Some(true), Some(false)] {
                        let req = request(Method::GET, Uri::from_static("/audit-bucket/object.txt"), |_, extensions| {
                            extensions.insert(ReqInfo {
                                cred: Some(credentials.clone()),
                                ..Default::default()
                            });
                        });
                        let mut helper =
                            OperationHelper::from_view(&req.audit_view(), EventName::ObjectAccessedGet, S3Operation::GetObject);
                        if let Some(success) = outcome {
                            let result = if success {
                                Ok(())
                            } else {
                                Err(S3Error::new(S3ErrorCode::NoSuchKey))
                            };
                            helper = helper.complete_owned(&result, StatusCode::OK, &HeaderMap::new());
                        }
                        let OperationHelper::Enabled(state) = &mut helper else {
                            panic!("audit helper should be enabled");
                        };
                        let entry = state.audit_builder.take().expect("audit builder should exist").build();
                        let json = serde_json::to_value(&entry).expect("audit entry should serialize");
                        assert_eq!(json["accessKey"], credentials.access_key);
                        assert_eq!(json["parentUser"], "oidc-parent");
                        assert_eq!(json["requestClaims"], expected_claims);
                        let rendered = serde_json::to_string(&entry).expect("audit entry should serialize");
                        assert!(!rendered.contains("private-"));
                        assert!(!rendered.contains("private@example.test"));
                        assert!(!rendered.contains("audit-secret-must-not-leak"));
                        assert!(!rendered.contains("audit-token-must-not-leak"));
                        assert_eq!(serde_json::to_value(req.req_info().unwrap().cred.as_ref().unwrap()).unwrap(), before);
                    }
                }
            }
        },
    );
}

#[test]
fn authorization_denial_preserves_oidc_audit_identity() {
    use rustfs_policy::policy::action::{Action, S3Action};

    let credentials = oidc_audit_credentials(Some("https://idp.example.test"));
    let before = serde_json::to_value(&credentials).unwrap();
    let req = request(Method::GET, Uri::from_static("/audit-bucket/object.txt"), |_, extensions| {
        extensions.insert(ReqInfo {
            cred: Some(credentials),
            ..Default::default()
        });
    });
    let entry = super::authorization_denial_entry_for_view(
        &req.audit_view(),
        &Action::S3Action(S3Action::GetObjectAction),
        "iam_explicit_deny",
        super::DenialScope::Request,
    );
    let json = serde_json::to_value(entry).expect("denial should serialize");
    assert_eq!(json["parentUser"], "oidc-parent");
    assert_eq!(
        json["requestClaims"],
        serde_json::json!({
            "sub": " subject-9170 ",
            "iss": "rustfs-oidc",
            "oidc_provider": "default",
            "x-rustfs-internal-oidc-issuer": "https://idp.example.test"
        })
    );
    assert_eq!(json["api"]["status_code"], 403);
    assert_eq!(json["tags"]["denialReason"], "iam_explicit_deny");
    let rendered = json.to_string();
    assert!(!rendered.contains("private"));
    assert!(!rendered.contains("audit-secret-must-not-leak"));
    assert!(!rendered.contains("audit-token-must-not-leak"));
    assert_eq!(serde_json::to_value(req.req_info().unwrap().cred.as_ref().unwrap()).unwrap(), before);
}

#[test]
fn request_audit_identity_omits_non_oidc_and_empty_claims() {
    for (key, value, expected_oidc) in [
        (rustfs_iam::sys::OIDC_ISSUER_CLAIM, serde_json::json!(""), true),
        (rustfs_iam::sys::OIDC_ISSUER_CLAIM, serde_json::json!(["https://idp.example.test"]), true),
        ("iss", serde_json::json!("other-session"), false),
        ("sub", serde_json::json!(""), false),
        ("oidc_provider", serde_json::Value::Null, false),
    ] {
        let mut credentials = oidc_audit_credentials(None);
        credentials.claims.as_mut().unwrap().insert(key.to_string(), value);
        let req = request(Method::GET, Uri::from_static("/audit-bucket/object.txt"), |_, extensions| {
            extensions.insert(ReqInfo {
                cred: Some(credentials),
                ..Default::default()
            });
        });
        let entry = super::request_audit_builder(
            &req.audit_view(),
            EventName::ObjectAccessedGet,
            "GetObject",
            rustfs_audit::ApiDetails::default(),
            "audit-identity-boundary",
        )
        .build();
        let json = serde_json::to_value(entry).expect("audit entry should serialize");
        assert_eq!(json["parentUser"], "oidc-parent");
        if expected_oidc {
            assert_eq!(
                json["requestClaims"],
                serde_json::json!({"sub": " subject-9170 ", "iss": "rustfs-oidc", "oidc_provider": "default"})
            );
        } else {
            assert!(!json.as_object().unwrap().contains_key("requestClaims"));
        }
    }
}

#[test]
fn operation_helper_audits_verified_client_addresses() {
    use rustfs_trusted_proxies::{ClientInfo, ValidationMode};
    use std::net::SocketAddr;

    with_vars(
        [
            (rustfs_config::ENV_NOTIFY_ENABLE, Some("false")),
            (rustfs_config::ENV_AUDIT_ENABLE, Some("true")),
        ],
        || {
            refresh_notify_module_enabled();
            refresh_audit_module_enabled();

            let peer: SocketAddr = "192.0.2.10:54321".parse().unwrap();
            let ipv6_peer: SocketAddr = "[2001:db8::10]:54321".parse().unwrap();
            let proxied = ClientInfo::from_trusted_proxy(
                "198.51.100.20".parse().unwrap(),
                None,
                None,
                peer.ip(),
                1,
                ValidationMode::Strict,
                Vec::new(),
            );
            for (remote_addr, client_info, expected) in [
                (Some(RemoteAddr(peer)), None, "192.0.2.10"),
                (Some(RemoteAddr(ipv6_peer)), None, "2001:db8::10"),
                (Some(RemoteAddr(peer)), Some(proxied), "198.51.100.20"),
                (Some(RemoteAddr(peer)), Some(ClientInfo::direct(peer)), "192.0.2.10"),
                (None, None, ""),
            ] {
                for spoofed in [false, true] {
                    let req = request(Method::GET, Uri::from_static("/audit-bucket/object.txt"), |headers, extensions| {
                        extensions.insert(remote_addr);
                        if let Some(client_info) = client_info.as_ref() {
                            extensions.insert(client_info.clone());
                        }
                        if spoofed {
                            headers.insert("x-forwarded-for", HeaderValue::from_static("203.0.113.99"));
                            headers.insert("x-real-ip", HeaderValue::from_static("203.0.113.98"));
                        }
                    });
                    let result = Ok(());
                    let mut helper =
                        OperationHelper::from_view(&req.audit_view(), EventName::ObjectAccessedGet, S3Operation::GetObject)
                            .complete_owned(&result, StatusCode::OK, &HeaderMap::new());
                    let OperationHelper::Enabled(state) = &mut helper else {
                        panic!("helper should be enabled when the audit switch is on");
                    };
                    let entry = state.audit_builder.take().expect("audit builder should exist").build();
                    assert_eq!(entry.remote_host.as_deref(), Some(expected));
                    let rendered = serde_json::to_value(&entry).expect("audit entry should serialize");
                    assert_eq!(rendered["remotehost"], expected);
                }
            }
        },
    );
}

#[test]
fn operation_helper_adds_objects_to_audit_details() {
    with_vars(
        [
            (rustfs_config::ENV_NOTIFY_ENABLE, Some("false")),
            (rustfs_config::ENV_AUDIT_ENABLE, Some("true")),
        ],
        || {
            refresh_notify_module_enabled();
            refresh_audit_module_enabled();

            let req = request(Method::DELETE, Uri::from_static("/test-bucket"), |_, _| {});
            let objects = vec![
                ObjectVersion::new("first-key".to_string(), None),
                ObjectVersion::new("second-key".to_string(), Some("version-123".to_string())),
            ];
            let mut empty_helper =
                OperationHelper::from_view(&req.audit_view(), EventName::ObjectRemovedDelete, S3Operation::DeleteObjects)
                    .audit_objects(Vec::new());
            let OperationHelper::Enabled(empty_state) = &mut empty_helper else {
                panic!("helper should be enabled when the audit switch is on");
            };
            assert!(empty_state.api_builder.0.objects.is_none());
            empty_state.audit_builder.take();

            let result = Ok(());
            let mut helper =
                OperationHelper::from_view(&req.audit_view(), EventName::ObjectRemovedDelete, S3Operation::DeleteObjects)
                    .audit_objects(objects.clone())
                    .complete_owned(&result, StatusCode::OK, &HeaderMap::new());

            let OperationHelper::Enabled(state) = &mut helper else {
                panic!("helper should be enabled when the audit switch is on");
            };
            let audit_entry = state.audit_builder.take().expect("audit builder should exist").build();
            assert_eq!(audit_entry.api.objects, Some(objects));
        },
    );
}

#[test]
fn operation_helper_initializes_audit_api_details_before_completion() {
    with_vars(
        [
            (rustfs_config::ENV_NOTIFY_ENABLE, Some("false")),
            (rustfs_config::ENV_AUDIT_ENABLE, Some("true")),
        ],
        || {
            refresh_notify_module_enabled();
            refresh_audit_module_enabled();

            let req = request(Method::GET, Uri::from_static("/audit-bucket/missing/object.txt"), |_, _| {});
            let mut helper = OperationHelper::from_view(&req.audit_view(), EventName::ObjectAccessedGet, S3Operation::GetObject);

            let OperationHelper::Enabled(state) = &mut helper else {
                panic!("helper should be enabled when the audit switch is on");
            };
            let audit_entry = state.audit_builder.take().expect("audit builder should exist").build();

            assert_eq!(audit_entry.api.name.as_deref(), Some("s3:GetObject"));
            assert_eq!(audit_entry.api.bucket.as_deref(), Some("audit-bucket"));
            assert_eq!(audit_entry.api.object.as_deref(), Some("missing/object.txt"));
            assert!(audit_entry.api.status_code.is_none());
        },
    );
}

#[test]
fn operation_helper_complete_records_failed_status_code() {
    with_vars(
        [
            (rustfs_config::ENV_NOTIFY_ENABLE, Some("false")),
            (rustfs_config::ENV_AUDIT_ENABLE, Some("true")),
        ],
        || {
            refresh_notify_module_enabled();
            refresh_audit_module_enabled();

            let req = request(Method::GET, Uri::from_static("/audit-bucket/missing/object.txt"), |_, _| {});
            let result: S3Result<()> = Err(S3Error::new(S3ErrorCode::NoSuchKey));
            let mut helper = OperationHelper::from_view(&req.audit_view(), EventName::ObjectAccessedGet, S3Operation::GetObject)
                .complete_owned(&result, StatusCode::OK, &HeaderMap::new());

            let OperationHelper::Enabled(state) = &mut helper else {
                panic!("helper should be enabled when the audit switch is on");
            };
            let audit_entry = state.audit_builder.take().expect("audit builder should exist").build();

            assert_eq!(audit_entry.api.name.as_deref(), Some("s3:GetObject"));
            assert_eq!(audit_entry.api.status.as_deref(), Some("failure"));
            assert_eq!(audit_entry.api.status_code, Some(404));
        },
    );
}

#[test]
fn operation_helper_prioritizes_request_context_for_request_id() {
    with_vars(
        [
            (rustfs_config::ENV_NOTIFY_ENABLE, Some("true")),
            (rustfs_config::ENV_AUDIT_ENABLE, Some("true")),
        ],
        || {
            refresh_notify_module_enabled();
            refresh_audit_module_enabled();

            let req = request(Method::DELETE, Uri::from_static("/test-bucket/test-key"), |headers, extensions| {
                headers.insert("host", HeaderValue::from_static("example.com"));
                headers.insert("user-agent", HeaderValue::from_static("rustfs-test"));
                headers.insert(REQUEST_ID_HEADER, HeaderValue::from_static("ingress-canonical-uuid"));
                headers.insert("x-amz-request-id", HeaderValue::from_static("client-supplied-request-id"));

                extensions.insert(RequestContext {
                    request_id: "ingress-canonical-uuid".to_string(),
                    x_amz_request_id: "client-supplied-request-id".to_string(),
                    trace_id: None,
                    span_id: None,
                    start_time: std::time::Instant::now(),
                });

                extensions.insert(ReqInfo {
                    bucket: Some("test-bucket".to_string()),
                    object: Some("test-key".to_string()),
                    ..Default::default()
                });
            });
            let result = Ok(());
            let mut helper = OperationHelper::from_view(&req.audit_view(), EventName::ObjectAccessedGet, S3Operation::GetObject)
                .complete_owned(&result, StatusCode::OK, &HeaderMap::new());

            // Verify the helper stored the RequestContext
            let OperationHelper::Enabled(state) = &mut helper else {
                panic!("helper should be enabled when notify/audit switches are on");
            };
            assert_eq!(state.request_context.request_id, "ingress-canonical-uuid");
            let audit_entry = state.audit_builder.take().expect("audit builder should exist").build();
            assert_eq!(audit_entry.request_id.as_deref(), Some("ingress-canonical-uuid"));
            let event_args = state.event_builder.clone().expect("event builder should exist").build();
            assert_eq!(
                event_args.req_params.get(AMZ_REQUEST_ID).map(String::as_str),
                Some("client-supplied-request-id")
            );
            assert_eq!(
                event_args.resp_elements.get(AMZ_REQUEST_ID).map(String::as_str),
                Some("ingress-canonical-uuid")
            );
        },
    );
}

#[test]
fn operation_helper_reuses_generated_context_when_headers_absent() {
    with_vars(
        [
            (rustfs_config::ENV_NOTIFY_ENABLE, Some("true")),
            (rustfs_config::ENV_AUDIT_ENABLE, Some("true")),
        ],
        || {
            refresh_notify_module_enabled();
            refresh_audit_module_enabled();

            let req = request(Method::DELETE, Uri::from_static("/test-bucket/test-key"), |headers, extensions| {
                headers.insert("host", HeaderValue::from_static("example.com"));
                headers.insert("user-agent", HeaderValue::from_static("rustfs-test"));

                // No RequestContext inserted
                extensions.insert(ReqInfo {
                    bucket: Some("test-bucket".to_string()),
                    object: Some("test-key".to_string()),
                    ..Default::default()
                });
            });
            let helper = OperationHelper::from_view(&req.audit_view(), EventName::ObjectAccessedGet, S3Operation::GetObject);
            let request_context = helper.request_context_or_from_view(&req.audit_view());
            let request_id = request_context.request_id;
            assert!(uuid::Uuid::parse_str(&request_id).is_ok());
            let result = Ok(());
            let helper = helper.complete_owned(&result, StatusCode::OK, &HeaderMap::new());
            let OperationHelper::Enabled(state) = &helper else {
                panic!("helper should be enabled when notify/audit switches are on");
            };
            assert_eq!(state.request_context.request_id, request_id);
            let event_args = state.event_builder.clone().expect("event builder should exist").build();
            assert!(!event_args.req_params.contains_key(AMZ_REQUEST_ID));
            assert_eq!(
                event_args.resp_elements.get(AMZ_REQUEST_ID).map(String::as_str),
                Some(request_id.as_str())
            );
        },
    );
}

#[test]
fn operation_helper_returns_disabled_when_both_switches_off() {
    with_vars(
        [
            (rustfs_config::ENV_NOTIFY_ENABLE, Some("false")),
            (rustfs_config::ENV_AUDIT_ENABLE, Some("false")),
        ],
        || {
            refresh_notify_module_enabled();
            refresh_audit_module_enabled();

            let req = request(Method::DELETE, Uri::from_static("/test-bucket/test-key"), |headers, extensions| {
                headers.insert(REQUEST_ID_HEADER, HeaderValue::from_static("client-request-id"));
                extensions.insert(RequestContext {
                    request_id: "server-request-id".to_string(),
                    x_amz_request_id: "server-request-id".to_string(),
                    trace_id: None,
                    span_id: None,
                    start_time: std::time::Instant::now(),
                });
            });
            let helper = OperationHelper::from_view(&req.audit_view(), EventName::ObjectAccessedGet, S3Operation::GetObject);

            assert!(matches!(&helper, OperationHelper::Disabled));
            assert_eq!(helper.request_context_or_from_view(&req.audit_view()).request_id, "server-request-id");
        },
    );
}

#[tokio::test]
async fn operation_helper_omits_kms_tags_for_requests_that_used_no_sse() {
    async_with_vars(
        [
            (rustfs_config::ENV_NOTIFY_ENABLE, Some("false")),
            (rustfs_config::ENV_AUDIT_ENABLE, Some("true")),
        ],
        async {
            refresh_notify_module_enabled();
            refresh_audit_module_enabled();

            let req = request(Method::GET, Uri::from_static("/finance/plain.txt"), |_, extensions| {
                extensions.insert(RequestContext {
                    request_id: "plain-request".to_string(),
                    x_amz_request_id: "plain-request".to_string(),
                    trace_id: None,
                    span_id: None,
                    start_time: std::time::Instant::now(),
                });
            });
            let result = Ok(());
            let mut helper = OperationHelper::from_view(&req.audit_view(), EventName::ObjectAccessedGet, S3Operation::GetObject)
                .complete_owned(&result, StatusCode::OK, &HeaderMap::new());
            let OperationHelper::Enabled(state) = &mut helper else {
                panic!("helper should be enabled when the audit switch is on");
            };
            let entry = state.audit_builder.take().expect("audit builder should exist").build();

            assert!(
                entry.tags.is_none_or(|tags| tags.keys().all(|key| !key.starts_with("kms"))),
                "a request that never reached KMS must not be tagged as if it had"
            );
        },
    )
    .await;
}
