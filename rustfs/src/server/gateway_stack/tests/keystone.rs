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

//! Exercises the Keystone HTTP validation, request bridge and real S3 authorization together.
//! Does not replace Keystone's validation or the gateway floor with a test authenticator.
//! Upstream: KeystoneAuthLayer and GatewayStack. Downstream: real ListBuckets and HeadBucket.

use super::*;
use rustfs_keystone::{KeystoneAuthLayer, KeystoneAuthProvider, KeystoneClient, KeystoneVersion};
use serial_test::serial;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tower::{Layer, ServiceExt};

struct KeystoneStub {
    provider: Arc<KeystoneAuthProvider>,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl KeystoneStub {
    fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind Keystone stub");
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = Arc::clone(&stop);
        let thread = std::thread::spawn(move || {
            while !stopping.load(Ordering::Acquire) {
                let (mut socket, _) = match listener.accept() {
                    Ok(value) => value,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(error) => panic!("accept Keystone validation: {error}"),
                };
                socket.set_nonblocking(false).expect("blocking accepted socket");
                socket.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                let mut bytes = Vec::new();
                let mut chunk = [0; 1024];
                while !bytes.windows(4).any(|value| value == b"\r\n\r\n") {
                    let read = socket.read(&mut chunk).unwrap();
                    assert!(read > 0 && bytes.len() < 16_384, "complete validation head");
                    bytes.extend_from_slice(&chunk[..read]);
                }
                let head = String::from_utf8(bytes).unwrap();
                assert!(head.starts_with("GET /v3/auth/tokens HTTP/1.1\r\n"));
                let token = head
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("x-subject-token").then(|| value.trim())
                    })
                    .expect("subject token");
                let token = token
                    .strip_prefix("opaque-")
                    .and_then(|value| value.strip_suffix("-TOKEN-SENTINEL"))
                    .unwrap_or(token);
                let status = if token == "invalid" { "401 Unauthorized" } else { "200 OK" };
                let expires = OffsetDateTime::now_utc()
                    + if token == "expired" {
                        time::Duration::hours(-1)
                    } else {
                        time::Duration::hours(1)
                    };
                let role = if token.starts_with("keystone-bridge-reader") {
                    "keystone-bridge-reader"
                } else {
                    token
                };
                let body = serde_json::json!({"token": {
                    "user": {"id": format!("{token}-id"), "name": format!("{token}-username")},
                    "roles": [{"name": role}],
                    "expires_at": expires.format(&Rfc3339).unwrap()
                }})
                .to_string();
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket.write_all(response.as_bytes()).unwrap();
            }
        });
        let client = KeystoneClient::new(
            endpoint,
            KeystoneVersion::V3,
            None,
            None,
            None,
            "Default".into(),
            true,
            Duration::from_secs(5),
        );
        Self {
            provider: Arc::new(KeystoneAuthProvider::new(client, 10, Duration::from_secs(1), false)),
            stop,
            thread: Some(thread),
        }
    }

    fn finish(mut self) {
        self.stop.store(true, Ordering::Release);
        self.thread
            .take()
            .expect("stub thread")
            .join()
            .expect("Keystone stub completed");
    }
}

impl Drop for KeystoneStub {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

async fn middleware_answer(
    stack: GatewayStack,
    provider: Arc<KeystoneAuthProvider>,
    request: Request<Empty<Bytes>>,
) -> (StatusCode, HeaderMap, String) {
    let service = tower::service_fn(move |request| {
        let stack = stack.clone();
        async move { Ok::<_, std::convert::Infallible>(stack.call(request).await) }
    });
    let response = KeystoneAuthLayer::new(Some(provider))
        .layer(service)
        .oneshot(request)
        .await
        .unwrap();
    let (parts, body) = response.into_parts();
    (
        parts.status,
        parts.headers,
        String::from_utf8_lossy(&body.collect().await.unwrap().to_bytes()).into_owned(),
    )
}

async fn validated_request(provider: Arc<KeystoneAuthProvider>, token: &str) -> Request<Empty<Bytes>> {
    let captured = Arc::new(Mutex::new(None));
    let observed = Arc::clone(&captured);
    let service = tower::service_fn(move |request| {
        *observed.lock().unwrap() = Some(request);
        async { Ok::<_, std::convert::Infallible>(Response::new(http_body_util::Full::new(Bytes::new()))) }
    });
    KeystoneAuthLayer::new(Some(provider))
        .layer(service)
        .oneshot(token_request(Method::GET, "/", token))
        .await
        .unwrap();
    captured
        .lock()
        .unwrap()
        .take()
        .expect("middleware forwarded a validated request")
}

fn token_request(method: Method, path: &str, token: &str) -> Request<Empty<Bytes>> {
    request(method, path)
        .header("x-auth-token", format!("opaque-{token}-TOKEN-SENTINEL"))
        .body(Empty::new())
        .unwrap()
}

#[test]
#[serial]
fn keystone_http_tokens_authorize_real_s3_by_roles_groups_and_username() {
    crate::app::gating_test_env::run_large_stack_test("keystone-owned-s3", || async {
        use crate::storage_api::server::http::access::test_support::{
            BucketOperations as _, MakeBucketOptions, get_bucket_metadata, set_bucket_metadata,
        };
        let stub = KeystoneStub::new();
        let context = crate::app::gating_test_env::shared_gating_ambient().await;
        let root = rustfs_credentials::Credentials {
            access_key: ROOT_ACCESS_KEY.into(),
            secret_key: ROOT_SECRET_KEY.into(),
            ..Default::default()
        };
        assert!(context.publish_action_credentials(root.clone()));
        let server_ctx = ServerContextSlot::new();
        assert!(server_ctx.install(Arc::clone(&context)));
        let bucket = format!("keystone-{}", uuid::Uuid::new_v4());
        context
            .object_store()
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .unwrap();
        let iam = context.iam().handle();
        let group = "keystone-bridge-reader";
        let policy = format!("keystone-{}", uuid::Uuid::new_v4());
        iam.set_policy(
            &policy,
            rustfs_policy::policy::Policy::parse_config(
                &serde_json::to_vec(&serde_json::json!({
                    "Version": "2012-10-17", "Statement": [{
                        "Effect": "Allow", "Action": ["s3:ListAllMyBuckets", "s3:ListBucket"], "Resource": ["arn:aws:s3:::*"],
                        "Condition": {"StringEquals": {"aws:username": format!("keystone:{group}-id")}}
                    }]
                }))
                .unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        iam.policy_db_set(group, rustfs_iam::store::UserType::Reg, true, &policy)
            .await
            .unwrap();
        for id in ["keystone-bridge-reader", "keystone-bridge-reader-wrong"] {
            iam.create_user(
                &format!("keystone:{id}-id"),
                &rustfs_madmin::AddOrUpdateUserReq {
                    secret_key: "unrelated-iam-secret".into(),
                    policy: None,
                    status: rustfs_madmin::AccountStatus::Enabled,
                },
            )
            .await
            .unwrap();
        }
        iam.add_users_to_group(group, vec!["keystone:keystone-bridge-reader-wrong-id".to_owned()])
            .await
            .unwrap();
        let bucket_policy = serde_json::json!({"Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Principal": {"AWS": "*"}, "Action": "s3:ListBucket", "Resource": format!("arn:aws:s3:::{bucket}"),
            "Condition": {"StringEquals": {"aws:username": "keystone:bucket-only-id"}}
        }]})
        .to_string();
        let mut metadata = (*get_bucket_metadata(&bucket).await.unwrap()).clone();
        metadata.policy_config = Some(serde_json::from_str(&bucket_policy).unwrap());
        metadata.policy_config_json = bucket_policy.into_bytes();
        set_bucket_metadata(bucket.clone(), metadata).await.unwrap();
        let stack = GatewayStack::assemble(GatewayAssembly {
            server_ctx: Arc::clone(&server_ctx),
            root_access_key: root.access_key.clone(),
            root_secret_key: root.secret_key.clone(),
            keystone_auth: Some(Arc::clone(&stub.provider)),
            ..assembly()
        })
        .unwrap();
        let mut failures = Vec::new();
        for (token, expected) in [
            ("admin", StatusCode::OK),
            ("reseller_admin", StatusCode::OK),
            (group, StatusCode::OK),
            ("keystone-bridge-reader-wrong", StatusCode::FORBIDDEN),
            ("keystone-bridge-reader-unmapped", StatusCode::FORBIDDEN),
            ("Admin", StatusCode::FORBIDDEN),
            ("member", StatusCode::FORBIDDEN),
            ("invalid", StatusCode::UNAUTHORIZED),
            ("expired", StatusCode::UNAUTHORIZED),
        ] {
            for (method, path) in [(Method::GET, "/".to_owned()), (Method::HEAD, format!("/{bucket}"))] {
                let answer =
                    middleware_answer(stack.clone(), Arc::clone(&stub.provider), token_request(method, &path, token)).await;
                if answer.0 != expected {
                    failures.push(format!("{token} {path}: got {}, expected {expected}: {}", answer.0, answer.2));
                }
                if answer.0 == StatusCode::OK {
                    assert!(answer.1.get("vary").unwrap().to_str().unwrap().contains("x-auth-token"));
                    assert!(answer.1.get("cache-control").unwrap().to_str().unwrap().contains("private"));
                }
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("; "));
        for (method, path, expected) in [
            (Method::HEAD, format!("/{bucket}"), StatusCode::OK),
            (Method::GET, "/".into(), StatusCode::FORBIDDEN),
        ] {
            let result =
                middleware_answer(stack.clone(), Arc::clone(&stub.provider), token_request(method, &path, "bucket-only")).await;
            assert_eq!(result.0, expected, "bucket-policy-only caller: {}", result.2);
        }
        let validated = validated_request(Arc::clone(&stub.provider), "admin").await;
        let carrier = validated
            .extensions()
            .get::<rustfs_keystone::KeystoneRequestIdentity>()
            .unwrap()
            .clone();
        assert_eq!(answer_request(&stack, validated).await.0, StatusCode::OK);
        let mut replay = token_request(Method::GET, "/", "admin");
        replay.extensions_mut().insert(carrier);
        assert_eq!(answer_request(&stack, replay).await.0, StatusCode::FORBIDDEN);
        for _ in 0..3 {
            let (allowed, denied) = tokio::join!(
                middleware_answer(stack.clone(), Arc::clone(&stub.provider), token_request(Method::GET, "/", "admin")),
                middleware_answer(
                    stack.clone(),
                    Arc::clone(&stub.provider),
                    token_request(Method::HEAD, &format!("/{bucket}"), "member")
                )
            );
            assert_eq!(allowed.0, StatusCode::OK);
            assert_eq!(denied.0, StatusCode::FORBIDDEN);
        }
        let unsigned = answer(&stack, request(Method::GET, "/")).await;
        assert_eq!(unsigned.0, StatusCode::FORBIDDEN);
        let signed = signed_request(Signed::Header, Method::GET, "/", "", &root.access_key, &root.secret_key);
        assert_eq!(answer_request(&stack, signed).await.0, StatusCode::OK);
        stub.finish();
    });
}

#[tokio::test]
async fn keystone_floor_refuses_alias_mixed_and_missing_proof() {
    let stub = KeystoneStub::new();
    let stack = GatewayStack::assemble(GatewayAssembly {
        keystone_auth: Some(Arc::clone(&stub.provider)),
        ..assembly()
    })
    .unwrap();
    assert_eq!(
        answer_request(&stack, token_request(Method::GET, "/", "admin")).await.0,
        StatusCode::FORBIDDEN
    );
    let alias = request(Method::GET, "/")
        .header("x-storage-token", "admin")
        .body(Empty::new())
        .unwrap();
    assert_eq!(
        middleware_answer(stack.clone(), Arc::clone(&stub.provider), alias).await.0,
        StatusCode::FORBIDDEN
    );
    let mut mixed = signed_request(Signed::Header, Method::GET, "/", "", ROOT_ACCESS_KEY, ROOT_SECRET_KEY);
    mixed.headers_mut().insert("x-auth-token", "admin".parse().unwrap());
    assert_eq!(
        middleware_answer(stack.clone(), Arc::clone(&stub.provider), mixed).await.0,
        StatusCode::FORBIDDEN
    );
    stub.finish();
}

fn probe_stack<H: rustfs_gateway::Handler<rustfs_gateway::dto::ListBuckets>>(
    provider: Arc<KeystoneAuthProvider>,
    probe: Arc<H>,
    now: Option<i64>,
) -> GatewayStack {
    let a = GatewayAssembly {
        keystone_auth: Some(provider),
        ..assembly()
    };
    let mut builder =
        GatewayStack::configured_builder(&a, Arc::new(ScriptedIam { ready: true }), root().source(), early_gate(OPERATIONS))
            .unwrap();
    if let Some(now) = now {
        builder = builder.clock_with_skew_ack(
            rustfs_gateway::FixedClock::at_unix_seconds(now),
            rustfs_gateway::ClockSkewAck::i_understand_a_skewed_clock_can_disable_signature_expiry(),
        );
    }
    GatewayStack {
        service: builder
            .register::<rustfs_gateway::dto::ListBuckets, _>(probe)
            .build()
            .unwrap(),
        keystone_auth: a.keystone_auth,
    }
}

#[tokio::test]
async fn keystone_bridge_refuses_provider_mismatch_and_post_validation_expiry() {
    let stub = KeystoneStub::new();
    let other = KeystoneStub::new();
    for expiry in [false, true] {
        let probe = Arc::new(EnvelopeProbe {
            expected: None,
            called: AtomicBool::new(false),
        });
        let now = expiry.then(|| OffsetDateTime::now_utc().unix_timestamp() + 7200);
        let stack = probe_stack(Arc::clone(&stub.provider), Arc::clone(&probe), now);
        let validating_provider = if expiry { &stub.provider } else { &other.provider };
        let request = validated_request(Arc::clone(validating_provider), "admin").await;
        assert_eq!(answer_request(&stack, request).await.0, StatusCode::FORBIDDEN);
        assert!(!probe.called.load(Ordering::Acquire), "refused identity reached the handler");
    }
    other.finish();
    stub.finish();
}

struct EnvelopeProbe {
    expected: Option<rustfs_s3_types::S3ErrorCode>,
    called: AtomicBool,
}

impl rustfs_gateway::Handler<rustfs_gateway::dto::ListBuckets> for EnvelopeProbe {
    async fn call(
        &self,
        request: rustfs_gateway::Req<rustfs_gateway::dto::ListBuckets>,
    ) -> rustfs_gateway::HandlerResult<rustfs_gateway::dto::ListBuckets> {
        assert!(request.context().principal().is_none());
        assert!(!request.context().headers().iter_raw().any(|(name, value)| {
            name == "x-auth-token"
                || name == "x-storage-token"
                || value
                    .as_bytes()
                    .windows(b"TOKEN-SENTINEL".len())
                    .any(|part| part == b"TOKEN-SENTINEL")
        }));
        if let Some(owned) = request.context().transport_extensions().get::<OwnedKeystoneIdentity>() {
            assert!(owned.credentials.secret_key.is_empty() && owned.credentials.session_token.is_empty());
            assert!(!serde_json::to_string(&owned.credentials).unwrap().contains("TOKEN-SENTINEL"));
        }
        let result = crate::app::RequestEnvelope::from_gateway(request.context(), &ServerContextSlot::new(), Default::default());
        assert_eq!(result.as_ref().err().map(|error| error.code()), self.expected.as_ref());
        if let Ok(envelope) = result {
            let credentials = envelope.credentials().expect("host token caller must be retained");
            assert_eq!(credentials.access_key(), "keystone:admin-id");
            assert!(credentials.session_token().is_none());
        }
        self.called.store(true, Ordering::Release);
        Ok(rustfs_gateway::Resp::new(Default::default()))
    }
}

#[tokio::test]
async fn keystone_envelope_refuses_missing_and_mismatched_owned_identity() {
    for mode in ["missing", "mismatch", "matching"] {
        let stub = KeystoneStub::new();
        let mut request = validated_request(Arc::clone(&stub.provider), "admin").await;
        let carrier = request
            .extensions_mut()
            .remove::<rustfs_keystone::KeystoneRequestIdentity>()
            .unwrap();
        let (proof, mut credentials, had_session_token) = carrier.take_for(&stub.provider).unwrap();
        establish_keystone_identity(request.extensions_mut(), proof).unwrap();
        if mode != "missing" {
            if mode == "mismatch" {
                credentials.access_key = "keystone:another-user".into();
            }
            request.extensions_mut().insert(OwnedKeystoneIdentity {
                credentials,
                had_session_token,
            });
        }
        let probe = Arc::new(EnvelopeProbe {
            expected: (mode != "matching").then_some(rustfs_s3_types::S3ErrorCode::AccessDenied),
            called: AtomicBool::new(false),
        });
        let a = GatewayAssembly {
            keystone_auth: Some(Arc::clone(&stub.provider)),
            ..assembly()
        };
        let builder =
            GatewayStack::configured_builder(&a, Arc::new(ScriptedIam { ready: true }), root().source(), early_gate(OPERATIONS))
                .unwrap();
        let service = builder
            .register::<rustfs_gateway::dto::ListBuckets, _>(Arc::clone(&probe))
            .build()
            .unwrap();
        assert_eq!(service.call(request).await.status(), StatusCode::OK);
        assert!(probe.called.load(Ordering::Acquire), "probe must inspect the actual header-token context");
        stub.finish();
    }
}

#[tokio::test]
async fn keystone_stack_discards_stale_established_and_owned_payloads() {
    let stub = KeystoneStub::new();
    let mut request = validated_request(Arc::clone(&stub.provider), "admin").await;
    let carrier = request
        .extensions_mut()
        .remove::<rustfs_keystone::KeystoneRequestIdentity>()
        .unwrap();
    let (proof, credentials, had_session_token) = carrier.take_for(&stub.provider).unwrap();
    establish_keystone_identity(request.extensions_mut(), proof).unwrap();
    request.extensions_mut().insert(OwnedKeystoneIdentity {
        credentials,
        had_session_token,
    });
    let probe = Arc::new(EnvelopeProbe {
        expected: None,
        called: AtomicBool::new(false),
    });
    let stack = probe_stack(Arc::clone(&stub.provider), Arc::clone(&probe), None);
    assert_eq!(answer_request(&stack, request).await.0, StatusCode::FORBIDDEN);
    assert!(!probe.called.load(Ordering::Acquire));
    stub.finish();
}

#[tokio::test]
async fn keystone_outer_validation_precedes_non_s3_refusal_and_legacy_stays_selected() {
    use crate::config::S3Stack;
    use crate::server::stack::select_stack;
    for stack in [S3Stack::Gateway, S3Stack::Legacy] {
        let stub = KeystoneStub::new();
        for (method, path) in [
            (Method::GET, "/rustfs/admin/v3/info"),
            (Method::POST, "/?Action=AssumeRole&Version=2011-06-15"),
        ] {
            for header in ["x-auth-token", "x-storage-token"] {
                for (token, expected) in [
                    ("invalid", StatusCode::UNAUTHORIZED),
                    (
                        "admin",
                        if stack == S3Stack::Gateway {
                            StatusCode::FORBIDDEN
                        } else {
                            StatusCode::OK
                        },
                    ),
                ] {
                    let legacy = tower::service_fn(|_request: Request<Empty<Bytes>>| async {
                        Ok::<_, std::convert::Infallible>(Response::new(http_body_util::Full::new(Bytes::from_static(b"legacy"))))
                    });
                    let gateway = tower::service_fn(|_request: Request<Empty<Bytes>>| async {
                        Ok::<_, std::convert::Infallible>(Response::new(http_body_util::Full::new(Bytes::from_static(
                            b"gateway",
                        ))))
                    });
                    let edge = select_stack(stack, legacy, || Ok::<_, std::convert::Infallible>(gateway)).unwrap();
                    let mut builder = request(method.clone(), path).header(header, token);
                    if method == Method::POST {
                        builder = builder.header(http::header::CONTENT_TYPE, "application/x-www-form-urlencoded");
                    }
                    let request = builder.body(Empty::new()).unwrap();
                    let response = KeystoneAuthLayer::new(Some(Arc::clone(&stub.provider)))
                        .layer(edge)
                        .oneshot(request)
                        .await
                        .unwrap();
                    assert_eq!(response.status(), expected, "{stack:?} {header} {token} {path}");
                    let body = response.into_body().collect().await.unwrap().to_bytes();
                    if expected == StatusCode::OK {
                        assert_eq!(body, "legacy");
                    }
                }
            }
        }
        stub.finish();
    }
}

#[tokio::test]
async fn keystone_middleware_replaces_stale_request_and_tasklocal_identity_before_call_and_poll() {
    for has_token in [false, true] {
        let stub = KeystoneStub::new();
        let mut request = validated_request(Arc::clone(&stub.provider), "admin").await;
        if has_token {
            request
                .headers_mut()
                .insert("x-auth-token", "opaque-member-TOKEN-SENTINEL".parse().unwrap());
        } else {
            request.headers_mut().remove("x-auth-token");
        }
        let expected = has_token.then_some("keystone:member-id");
        let service = tower::service_fn(move |request: Request<Empty<Bytes>>| {
            let call_identity = rustfs_keystone::KEYSTONE_CREDENTIALS
                .with(|value| value.as_ref().map(|credentials| credentials.access_key.clone()));
            assert_eq!(call_identity.as_deref(), expected);
            assert_eq!(
                request
                    .extensions()
                    .get::<rustfs_keystone::KeystoneRequestIdentity>()
                    .is_some(),
                has_token
            );
            async move {
                let poll_identity = rustfs_keystone::KEYSTONE_CREDENTIALS
                    .with(|value| value.as_ref().map(|credentials| credentials.access_key.clone()));
                assert_eq!(poll_identity.as_deref(), expected);
                Ok::<_, std::convert::Infallible>(Response::new(http_body_util::Full::new(Bytes::new())))
            }
        });
        let outer = rustfs_credentials::Credentials {
            access_key: "outer-identity".into(),
            ..Default::default()
        };
        rustfs_keystone::KEYSTONE_CREDENTIALS
            .scope(Some(outer), async {
                let response = KeystoneAuthLayer::new(Some(Arc::clone(&stub.provider)))
                    .layer(service)
                    .oneshot(request)
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::OK);
                assert_eq!(
                    rustfs_keystone::KEYSTONE_CREDENTIALS
                        .with(|value| value.as_ref().map(|credentials| credentials.access_key.clone())),
                    Some("outer-identity".into())
                );
            })
            .await;
        stub.finish();
    }
}

struct CleanSignedProbe(AtomicBool);
impl rustfs_gateway::Handler<rustfs_gateway::dto::ListBuckets> for CleanSignedProbe {
    async fn call(
        &self,
        request: rustfs_gateway::Req<rustfs_gateway::dto::ListBuckets>,
    ) -> rustfs_gateway::HandlerResult<rustfs_gateway::dto::ListBuckets> {
        assert!(request.context().principal().is_some());
        assert!(
            request
                .context()
                .transport_extensions()
                .get::<KeystoneEstablished>()
                .is_none()
        );
        assert!(
            request
                .context()
                .transport_extensions()
                .get::<OwnedKeystoneIdentity>()
                .is_none()
        );
        self.0.store(true, Ordering::Release);
        Ok(rustfs_gateway::Resp::new(Default::default()))
    }
}

#[tokio::test]
async fn keystone_stack_clears_both_payloads_before_a_signed_request() {
    let stub = KeystoneStub::new();
    let mut prior = validated_request(Arc::clone(&stub.provider), "admin").await;
    let carrier = prior
        .extensions_mut()
        .remove::<rustfs_keystone::KeystoneRequestIdentity>()
        .unwrap();
    let (proof, credentials, had_session_token) = carrier.take_for(&stub.provider).unwrap();
    let mut signed = signed_request(Signed::Header, Method::GET, "/", "", ROOT_ACCESS_KEY, ROOT_SECRET_KEY);
    establish_keystone_identity(signed.extensions_mut(), proof).unwrap();
    signed.extensions_mut().insert(OwnedKeystoneIdentity {
        credentials,
        had_session_token,
    });
    let probe = Arc::new(CleanSignedProbe(AtomicBool::new(false)));
    let stack = probe_stack(Arc::clone(&stub.provider), Arc::clone(&probe), None);
    assert_eq!(answer_request(&stack, signed).await.0, StatusCode::OK);
    assert!(probe.0.load(Ordering::Acquire));
    stub.finish();
}
