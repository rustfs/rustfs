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

//! Differential proof that protocol authorization hands the policy engine the
//! same inputs through the owned request envelope as through the retired s3s
//! request, which is kept here verbatim as the oracle.
//!
//! Not responsible for policy semantics; `protocols::policy_test` covers those
//! end to end. Upstream is `ProtocolStorageClient::protocol_authorization_envelope`;
//! downstream is the shared `authorize_request`.

use super::*;
use crate::app::storage_api::access::AuthorizationRequest;
use crate::runtime_sources::ServerContextSlot;
use rustfs_credentials::Credentials;
use rustfs_policy::policy::action::{Action, S3Action as PolicyS3Action};
use rustfs_protocols::common::session::{Protocol, ProtocolPrincipal};
use serde_json::{Value, json};
use std::sync::Arc;

/// The retired request builder of `authorize_protocol_request`, verbatim.
fn legacy_request(
    client: &ProtocolStorageClient,
    session: &SessionContext,
    action: &S3Action,
    bucket: &str,
    object: Option<&str>,
    listing: Option<&gateway_dto::ListObjectsV2Input>,
    (credentials, is_owner): (&Credentials, bool),
) -> rustfs_s3_types::S3Result<S3Request<()>> {
    let uri = if let Some(input) = listing {
        build_list_objects_uri(input)?
    } else if let Some(key) = object {
        build_object_uri(bucket, key, &[])?
    } else {
        build_bucket_uri(bucket, &[])?
    };
    let mut request = ProtocolStorageClient::create_request(
        (),
        Method::GET,
        uri,
        RequestParams {
            bucket: Some(bucket.to_owned()),
            object: object.map(str::to_owned),
            credentials,
        },
    )?;
    request.extensions.insert(client.server_ctx.clone());
    if let Some(info) = request.extensions.get_mut::<ReqInfo>() {
        info.is_owner = is_owner;
    }
    let remote_addr = std::net::SocketAddr::new(session.source_ip, 0);
    request.extensions.insert(Some(RemoteAddr(remote_addr)));
    let mut client_info = ClientInfo::direct(remote_addr);
    client_info.forwarded_proto = Some(if session.secure_transport { "https" } else { "http" }.to_owned());
    request.extensions.insert(client_info);
    request.headers = session.request_headers.clone();
    if matches!(action, S3Action::CreateMultipartUpload | S3Action::UploadPart) {
        request.extensions.insert(MultipartStagingOperation);
    }
    Ok(request)
}

/// The retired inline policy-action mapping, verbatim.
fn legacy_policy_action(action: &S3Action) -> Action {
    if matches!(action, S3Action::HeadBucket) {
        S3Action::ListBucket.into()
    } else {
        action.clone().into()
    }
}

/// Every input `authorize_request` and its denial audit read from a request.
fn authorization_inputs<R: AuthorizationRequest>(req: &R) -> Value {
    let info = req.req_info().expect("protocol authorization always carries ReqInfo");
    json!({
        "uri": req.uri().to_string(),
        "headers": format!("{:?}", req.headers()),
        "policy_headers": format!("{:?}", req.policy_headers()),
        "remote_addr": req.remote_addr().map(|addr| addr.to_string()),
        "client_info": req.client_info().map(|info| format!("{info:?}")),
        "server_context": req.server_context().map(|slot| Arc::as_ptr(slot) as usize),
        "request_context": req.request_context().is_some(),
        "cred": info.cred.as_ref().map(|cred| serde_json::to_value(cred).expect("encode credentials")),
        "is_owner": info.is_owner,
        "keystone_session_present": info.keystone_session_present,
        "bucket": info.bucket,
        "object": info.object,
        "version_id": info.version_id,
        "replication_request_authorized": info.replication_request_authorized,
        "region": info.region.is_some(),
        "req_info_request_context": info
            .request_context
            .as_ref()
            .map(|context| (context.trace_id.clone(), context.span_id.clone(), context.request_id.is_empty())),
        "suppress": [info.suppress_denial_log, info.suppress_denial_audit],
        "permission_probe": [
            req.permission_probe(),
            req.permission_probe_caller_authentication().is_some(),
            req.permission_probe_auth_type().is_some(),
        ],
        "internal_object_authorization": req.internal_object_authorization(),
        "multipart_staging": req.multipart_staging(),
        "staged_multipart_part": req.staged_multipart_part(),
        "requested": [
            req.requested_retention().is_some(),
            req.requested_retain_until().is_some(),
            req.requested_legal_hold().is_some(),
            req.requested_object_tags().is_some(),
            req.requested_copy_source().is_some(),
            req.object_tag_conditions().is_some(),
        ],
        "guards": [
            req.bucket_generation().is_some(),
            req.pending_delete_bucket_generation().is_some(),
            req.copy_source_bucket_generation().is_some(),
            req.odm_read_generation().is_some(),
            req.bucket_config_mutation().is_some(),
            req.publication_guards().is_some(),
        ],
    })
}

fn session(protocol: Protocol, source_ip: &str, secure_transport: bool, headers: &[(&'static str, &str)]) -> SessionContext {
    let mut session = SessionContext::new(
        ProtocolPrincipal::new(Arc::new(rustfs_policy::auth::UserIdentity::default())),
        protocol,
        source_ip.parse().expect("test address"),
    );
    session.secure_transport = secure_transport;
    for (name, value) in headers {
        session
            .request_headers
            .insert(*name, value.parse().expect("test header value"));
    }
    session
}

fn credentials(access_key: &str) -> Credentials {
    Credentials {
        access_key: access_key.into(),
        secret_key: format!("{access_key}-secret"),
        status: "on".into(),
        ..Default::default()
    }
}

/// A temporary credential with a parent, groups, a session token and claims.
fn session_credentials() -> Credentials {
    let mut claims = std::collections::HashMap::new();
    claims.insert("parent".to_owned(), json!("protocol-user"));
    claims.insert("sessionPolicy".to_owned(), json!("eyJWZXJzaW9uIjoiMjAxMi0xMC0xNyJ9"));
    Credentials {
        session_token: "protocol-session-token".into(),
        parent_user: "protocol-user".into(),
        groups: Some(vec!["protocol-group".to_owned()]),
        claims: Some(claims),
        ..credentials("protocol-session")
    }
}

fn listing(bucket: &str) -> gateway_dto::ListObjectsV2Input {
    gateway_dto::ListObjectsV2Input::builder()
        .bucket(BucketName::new(bucket.to_owned()).expect("valid bucket name"))
        .prefix("private /+&=照片/".into())
        .delimiter("/".into())
        .max_keys(0)
        .continuation_token(OpaqueString::from("token/+=".to_owned()))
        .start_after("after key".into())
        .build()
}

#[test]
fn protocol_authorization_envelope_carries_the_legacy_request_inputs() {
    let client = ProtocolStorageClient::new(ServerContextSlot::new());
    let sessions = [
        session(Protocol::Ftps, "127.0.0.1", false, &[]),
        session(
            Protocol::Sftp,
            "::1",
            true,
            &[("user-agent", "sftp-client"), ("x-amz-meta-color", "blue")],
        ),
        session(
            Protocol::WebDav,
            "10.1.2.3",
            true,
            &[("x-amz-copy-source", "/other/key"), ("x-amz-acl", "public-read")],
        ),
    ];
    let principals = [
        (credentials("protocol-root"), true),
        (credentials("protocol-user"), false),
        (session_credentials(), false),
        (Credentials::default(), false),
    ];
    let bucket = "protocol-differential";
    let list_input = listing(bucket);
    let operations = [
        (S3Action::GetObject, Some("dir/a b+c?versionId=1"), None),
        (S3Action::HeadObject, Some("照片"), None),
        (S3Action::PutObject, Some("key"), None),
        (S3Action::DeleteObject, Some("key/"), None),
        (S3Action::CopyObject, Some("key"), None),
        (S3Action::CreateMultipartUpload, Some("key"), None),
        (S3Action::UploadPart, Some("key"), None),
        (S3Action::CompleteMultipartUpload, Some("key"), None),
        (S3Action::AbortMultipartUpload, Some("key"), None),
        (S3Action::CreateBucket, None, None),
        (S3Action::DeleteBucket, None, None),
        (S3Action::HeadBucket, None, None),
        (S3Action::ListBucket, None, Some(&list_input)),
    ];
    for session in &sessions {
        for (credentials, is_owner) in &principals {
            for (action, object, listing) in &operations {
                let legacy = legacy_request(&client, session, action, bucket, *object, *listing, (credentials, *is_owner))
                    .expect("legacy request");
                let owned = client
                    .protocol_authorization_envelope(session, action, bucket, *object, *listing, (credentials.clone(), *is_owner))
                    .expect("owned envelope");
                let (legacy_inputs, owned_inputs) = (authorization_inputs(&legacy), authorization_inputs(&owned));
                assert_eq!(
                    legacy_inputs, owned_inputs,
                    "{action:?} {:?} {:?}",
                    session.protocol, credentials.access_key
                );
                assert_eq!(legacy_policy_action(action), protocol_policy_action(action), "{action:?}");

                // What the legacy path handed the policy engine, written down.
                let expected_path = match object {
                    Some(key) => format!("/{bucket}/{}", encode_object_key_path(key)),
                    None => format!("/{bucket}"),
                };
                assert_eq!(legacy.uri.path(), expected_path, "{action:?}");
                assert_eq!(
                    legacy.uri.query(),
                    listing.map(|_| "list-type=2&prefix=private%20%2F%2B%26%3D%E7%85%A7%E7%89%87%2F&delimiter=%2F&max-keys=0&continuation-token=token%2F%2B%3D&start-after=after%20key"),
                    "{action:?}"
                );
                assert_eq!(legacy_inputs["bucket"], json!(bucket));
                assert_eq!(legacy_inputs["object"], json!(object));
                assert_eq!(legacy_inputs["is_owner"], json!(is_owner));
                assert_eq!(legacy_inputs["cred"]["accessKey"], json!(credentials.access_key));
                assert_eq!(legacy_inputs["cred"]["parentUser"], json!(credentials.parent_user));
                assert_eq!(legacy_inputs["cred"]["sessionToken"], json!(credentials.session_token));
                assert_eq!(legacy_inputs["cred"]["groups"], json!(credentials.groups));
                assert_eq!(legacy_inputs["cred"]["claims"], json!(credentials.claims));
                assert_eq!(legacy_inputs["headers"], json!(format!("{:?}", session.request_headers)));
                assert_eq!(
                    legacy_inputs["remote_addr"],
                    json!(std::net::SocketAddr::new(session.source_ip, 0).to_string())
                );
                assert_eq!(
                    legacy_inputs["server_context"],
                    json!(Arc::as_ptr(&client.server_ctx) as usize),
                    "the request is judged by this server's context"
                );
                assert_eq!(
                    legacy_inputs["multipart_staging"],
                    json!(matches!(action, S3Action::CreateMultipartUpload | S3Action::UploadPart))
                );
                assert_eq!(legacy_inputs["internal_object_authorization"], json!(false));
                assert_eq!(legacy_inputs["permission_probe"], json!([false, false, false]));
                assert_eq!(legacy_inputs["version_id"], Value::Null);
            }
        }
    }
    assert_eq!(
        legacy_policy_action(&S3Action::HeadBucket),
        Action::S3Action(PolicyS3Action::ListBucketAction)
    );
    assert_eq!(
        legacy_policy_action(&S3Action::CreateMultipartUpload),
        Action::S3Action(PolicyS3Action::PutObjectAction)
    );
}

/// One decision both paths must reach; `expected` is the legacy error code, `None` for an allow.
struct Case<'a> {
    name: &'static str,
    key: (Credentials, bool),
    action: S3Action,
    bucket: &'a str,
    object: Option<&'a str>,
    listing: Option<&'a gateway_dto::ListObjectsV2Input>,
    expected: Option<&'static str>,
}

#[test]
#[serial_test::serial]
fn protocol_authorization_envelope_reaches_the_legacy_policy_decision() {
    crate::app::gating_test_env::run_large_stack_test("protocol-authorization-differential", || async {
        use crate::storage_api::protocols::client::{
            BucketOperations as _, MakeBucketOptions, ObjectIO as _, StorageObjectOptions, StoragePutObjReader,
        };
        use rustfs_iam::store::UserType;
        let root = credentials("protocol-diff-root");
        let (_temp, context, iam) = crate::protocols::policy_test::protocol_test_context(root.clone()).await;
        let store = context.object_store();
        let slot = ServerContextSlot::new();
        assert!(slot.install(context.clone()));
        let client = ProtocolStorageClient::new(slot);
        let (bucket, other_bucket) = ("protocol-diff", "protocol-diff-other");
        for name in [bucket, other_bucket] {
            store
                .make_bucket(name, &MakeBucketOptions::default())
                .await
                .expect("create bucket");
        }
        let mut reader = StoragePutObjReader::from_vec(b"content".to_vec());
        store
            .put_object(bucket, "public.txt", &mut reader, &StorageObjectOptions::default())
            .await
            .expect("seed object");
        iam.create_user(
            "protocol-user",
            &rustfs_madmin::AddOrUpdateUserReq {
                secret_key: "protocol-user-secret".into(),
                policy: None,
                status: rustfs_madmin::AccountStatus::Enabled,
            },
        )
        .await
        .expect("create user");
        let policy = json!({"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":["s3:ListBucket"],"Resource":[format!("arn:aws:s3:::{bucket}")]},
            {"Effect":"Allow","Action":["s3:GetObject","s3:PutObject"],"Resource":[format!("arn:aws:s3:::{bucket}/*")]},
            {"Effect":"Deny","Action":["s3:PutObject"],"Resource":[format!("arn:aws:s3:::{bucket}/locked/*")]}
        ]});
        iam.set_policy("protocol-diff-policy", serde_json::from_str(&policy.to_string()).expect("parse policy"))
            .await
            .expect("save policy");
        iam.policy_db_set("protocol-user", UserType::Reg, false, "protocol-diff-policy")
            .await
            .expect("attach policy");
        let bucket_policy = json!({"Version":"2012-10-17","Statement":[{
            "Effect":"Deny","Principal":{"AWS":"*"},"Action":["s3:GetObject"],
            "Resource":[format!("arn:aws:s3:::{bucket}/secret*")]
        }]});
        store
            .update_bucket_metadata_config(bucket, "policy.json", serde_json::to_vec(&bucket_policy).expect("encode policy"))
            .await
            .expect("save bucket policy");

        let (user, user_is_owner) = crate::auth::check_key_valid_with_context("", "protocol-user", Some(context.as_ref()))
            .await
            .expect("user key");
        let (root_cred, root_is_owner) = crate::auth::check_key_valid_with_context("", &root.access_key, Some(context.as_ref()))
            .await
            .expect("root key");
        assert!(root_is_owner && !user_is_owner);
        let list_input = listing(bucket);
        let denied = Some("AccessDenied");
        let cases = [
            Case {
                name: "root object read",
                key: (root_cred.clone(), true),
                action: S3Action::GetObject,
                bucket,
                object: Some("public.txt"),
                listing: None,
                expected: None,
            },
            Case {
                name: "user object read",
                key: (user.clone(), false),
                action: S3Action::GetObject,
                bucket,
                object: Some("public.txt"),
                listing: None,
                expected: None,
            },
            Case {
                name: "user listing",
                key: (user.clone(), false),
                action: S3Action::ListBucket,
                bucket,
                object: None,
                listing: Some(&list_input),
                expected: None,
            },
            Case {
                name: "user head bucket is judged as listing",
                key: (user.clone(), false),
                action: S3Action::HeadBucket,
                bucket,
                object: None,
                listing: None,
                expected: None,
            },
            Case {
                name: "user staged upload",
                key: (user.clone(), false),
                action: S3Action::CreateMultipartUpload,
                bucket,
                object: Some("upload.bin"),
                listing: None,
                expected: None,
            },
            Case {
                name: "bucket policy explicit deny",
                key: (user.clone(), false),
                action: S3Action::GetObject,
                bucket,
                object: Some("secret.txt"),
                listing: None,
                expected: denied,
            },
            Case {
                name: "bucket policy deny binds root",
                key: (root_cred.clone(), true),
                action: S3Action::GetObject,
                bucket,
                object: Some("secret.txt"),
                listing: None,
                expected: denied,
            },
            Case {
                name: "identity policy explicit deny",
                key: (user.clone(), false),
                action: S3Action::PutObject,
                bucket,
                object: Some("locked/key"),
                listing: None,
                expected: denied,
            },
            Case {
                name: "identity deny on staged upload",
                key: (user.clone(), false),
                action: S3Action::UploadPart,
                bucket,
                object: Some("locked/key"),
                listing: None,
                expected: denied,
            },
            Case {
                name: "wrong bucket",
                key: (user.clone(), false),
                action: S3Action::GetObject,
                bucket: other_bucket,
                object: Some("public.txt"),
                listing: None,
                expected: denied,
            },
            Case {
                name: "wrong bucket head",
                key: (user.clone(), false),
                action: S3Action::HeadBucket,
                bucket: other_bucket,
                object: None,
                listing: None,
                expected: denied,
            },
            Case {
                name: "action not granted",
                key: (user.clone(), false),
                action: S3Action::DeleteObject,
                bucket,
                object: Some("public.txt"),
                listing: None,
                expected: denied,
            },
            Case {
                name: "bucket action not granted",
                key: (user.clone(), false),
                action: S3Action::DeleteBucket,
                bucket,
                object: None,
                listing: None,
                expected: denied,
            },
            Case {
                name: "unknown key",
                key: (credentials("protocol-unknown"), false),
                action: S3Action::GetObject,
                bucket,
                object: Some("public.txt"),
                listing: None,
                expected: denied,
            },
            Case {
                name: "anonymous",
                key: (Credentials::default(), false),
                action: S3Action::GetObject,
                bucket,
                object: Some("public.txt"),
                listing: None,
                expected: denied,
            },
            Case {
                name: "unbacked session credential",
                key: (session_credentials(), false),
                action: S3Action::GetObject,
                bucket,
                object: Some("public.txt"),
                listing: None,
                expected: denied,
            },
            Case {
                name: "owner claim without root key",
                key: (user.clone(), true),
                action: S3Action::GetObject,
                bucket,
                object: Some("secret.txt"),
                listing: None,
                expected: denied,
            },
        ];
        let positives = cases.iter().filter(|case| case.expected.is_none()).count();
        assert!(cases.len() - positives > positives, "negative cases must outnumber positive ones");
        let session = session(Protocol::Sftp, "127.0.0.1", true, &[]);
        let code = |result: rustfs_s3_types::S3Result<()>| result.err().map(|error| error.code().as_str().to_owned());
        for case in cases {
            let Case {
                name,
                key,
                action,
                bucket,
                object,
                listing,
                expected,
            } = case;
            let mut legacy =
                legacy_request(&client, &session, &action, bucket, object, listing, (&key.0, key.1)).expect("legacy request");
            let mut owned = client
                .protocol_authorization_envelope(&session, &action, bucket, object, listing, key)
                .expect("owned envelope");
            let legacy_result = code(authorize_request(&mut legacy, legacy_policy_action(&action)).await);
            let owned_result = code(authorize_request(&mut owned, protocol_policy_action(&action)).await);
            assert_eq!(legacy_result.as_deref(), expected, "{name}: legacy decision");
            assert_eq!(owned_result, legacy_result, "{name}: owned decision");
            assert_eq!(
                authorization_inputs(&owned),
                authorization_inputs(&legacy),
                "{name}: state after authorization"
            );
        }
    });
}
