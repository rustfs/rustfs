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

//! Regression coverage for policy enforcement at the protocol/backend boundary.

use super::client::ProtocolStorageClient;
use crate::runtime_sources::{AppContext, IamInterface, KmsInterface, ServerContextSlot};
use crate::storage_api::protocols::client::{BucketOperations, FS, MakeBucketOptions};
use rustfs_credentials::Credentials;
use rustfs_iam::{
    store::{
        Store, UserType,
        object::{IAM_CONFIG_PREFIX, ObjectStore},
    },
    sys::IamSys,
};
use rustfs_kms::KmsServiceManager;
use rustfs_protocols::common::{
    client::s3::StorageBackend,
    gateway::{AuthorizationError, S3Action},
    session::{Protocol, ProtocolPrincipal, SessionContext},
};
use serde_json::json;
use std::sync::Arc;

struct TestIam(Arc<IamSys<ObjectStore>>);
impl IamInterface for TestIam {
    fn handle(&self) -> Arc<IamSys<ObjectStore>> {
        self.0.clone()
    }
    fn is_ready(&self) -> bool {
        true
    }
}
struct TestKms;
impl KmsInterface for TestKms {
    fn handle(&self) -> Arc<KmsServiceManager> {
        Arc::new(KmsServiceManager::new())
    }
}

fn decode_policy(value: serde_json::Value) -> Result<rustfs_policy::policy::Policy, serde_json::Error> {
    serde_json::from_str(&value.to_string())
}

#[test]
#[serial_test::serial]
fn protocol_policy_denials_and_request_conditions() {
    crate::app::gating_test_env::run_large_stack_test("protocol-policy-authorization", || async {
        let (_temp, _paths, store) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
        ObjectStore::new(store.clone())
            .save_iam_config(json!({"version": 1}), format!("{}/format.json", *IAM_CONFIG_PREFIX))
            .await
            .expect("seed IAM format");
        let iam = rustfs_iam::init_iam_sys_for_context(store.clone())
            .await
            .expect("initialize IAM");
        let root = Credentials {
            access_key: "protocol-root".into(),
            secret_key: "protocol-root-secret".into(),
            status: "on".into(),
            ..Default::default()
        };
        let context = Arc::new(AppContext::new(store.clone(), Arc::new(TestIam(iam.clone())), Arc::new(TestKms)));
        assert!(context.publish_action_credentials(root.clone()));
        let slot = ServerContextSlot::new();
        assert!(slot.install(context));
        let backend = ProtocolStorageClient::new(FS::with_server_ctx(slot));
        let bucket = "protocol-policy-test";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("create bucket");
        iam.create_user(
            "protocol-user",
            &rustfs_madmin::AddOrUpdateUserReq {
                secret_key: "protocol-user-secret".into(),
                policy: None,
                status: rustfs_madmin::AccountStatus::Enabled,
            },
        )
        .await
        .expect("create protocol user");
        let identity = iam
            .check_key("protocol-user")
            .await
            .expect("load user")
            .0
            .expect("user exists");
        let allow = json!({"Effect":"Allow","Action":["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:ListBucket"],
        "Resource":[format!("arn:aws:s3:::{bucket}"),format!("arn:aws:s3:::{bucket}/*")]});
        iam.set_policy(
            "protocol-policy",
            decode_policy(json!({"Version":"2012-10-17","Statement":[allow.clone()]})).expect("parse allow"),
        )
        .await
        .expect("save allow");
        iam.policy_db_set("protocol-user", UserType::Reg, false, "protocol-policy")
            .await
            .expect("attach policy");
        use crate::storage_api::protocols::client::{ObjectIO, StorageObjectOptions, StoragePutObjReader};
        let mut reader = StoragePutObjReader::from_vec(b"protected content".to_vec());
        store
            .put_object(bucket, "secret.txt", &mut reader, &StorageObjectOptions::default())
            .await
            .expect("seed protected object");
        let mut sessions: Vec<_> = [Protocol::WebDav, Protocol::Ftps, Protocol::Sftp]
            .into_iter()
            .map(|protocol| {
                SessionContext::new(
                    ProtocolPrincipal::new(Arc::new(identity.clone())),
                    protocol,
                    "127.0.0.1".parse().expect("loopback"),
                )
            })
            .collect();
        for session in &sessions {
            backend
                .authorize_operation(session, &S3Action::GetObject, bucket, Some("secret.txt"))
                .await
                .expect("IAM allow works");
        }
        let bucket_policy = json!({"Version":"2012-10-17","Statement":[{
            "Effect":"Deny","Principal":{"AWS":"*"},"Action":["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:ListBucket"],
            "Resource":[format!("arn:aws:s3:::{bucket}"),format!("arn:aws:s3:::{bucket}/*")],
            "Condition":{"StringLike":{"aws:username":"*"}}
        }]});
        store
            .update_bucket_metadata_config(bucket, "policy.json", serde_json::to_vec(&bucket_policy).expect("encode policy"))
            .await
            .expect("save bucket deny");
        for session in &sessions {
            for action in [
                S3Action::GetObject,
                S3Action::HeadObject,
                S3Action::PutObject,
                S3Action::DeleteObject,
                S3Action::ListBucket,
                S3Action::HeadBucket,
            ] {
                assert!(
                    matches!(
                        backend
                            .authorize_operation(session, &action, bucket, Some("secret.txt"))
                            .await,
                        Err(AuthorizationError::AccessDenied)
                    ),
                    "bucket deny must constrain {action:?} over {:?}",
                    session.protocol
                );
            }
        }
        let root_session = SessionContext::new(
            ProtocolPrincipal::new(Arc::new(rustfs_policy::auth::UserIdentity {
                credentials: root,
                ..Default::default()
            })),
            Protocol::Sftp,
            "127.0.0.1".parse().expect("loopback"),
        );
        assert!(
            matches!(
                backend
                    .authorize_operation(&root_session, &S3Action::GetObject, bucket, Some("secret.txt"))
                    .await,
                Err(AuthorizationError::AccessDenied)
            ),
            "bucket deny constrains root object access"
        );
        store
            .update_bucket_metadata_config(
                bucket,
                "policy.json",
                serde_json::to_vec(&json!({"Version":"2012-10-17","Statement":[]})).expect("encode empty policy"),
            )
            .await
            .expect("clear bucket policy");
        for (condition, should_deny) in [
            (json!({"IpAddress":{"aws:SourceIp":"127.0.0.0/8"}}), true),
            (json!({"IpAddress":{"aws:SourceIp":"192.0.2.0/24"}}), false),
            (json!({"NotIpAddress":{"aws:SourceIp":"127.0.0.0/8"}}), false),
            (json!({"NotIpAddress":{"aws:SourceIp":"192.0.2.0/24"}}), true),
        ] {
            iam.set_policy(
                "protocol-policy",
                decode_policy(json!({"Version":"2012-10-17","Statement":[allow.clone(),{
                    "Effect":"Deny","Action":"s3:GetObject","Resource":format!("arn:aws:s3:::{bucket}/*"),"Condition":condition
                }]}))
                .expect("parse condition policy"),
            )
            .await
            .expect("save condition policy");
            for session in &mut sessions {
                session
                    .request_headers
                    .insert("x-forwarded-for", "192.0.2.1".parse().expect("forwarded IP"));
                let result = backend
                    .authorize_operation(session, &S3Action::GetObject, bucket, Some("secret.txt"))
                    .await;
                if should_deny {
                    assert!(matches!(result, Err(AuthorizationError::AccessDenied)), "matching IAM deny must apply");
                } else {
                    result.expect("non-matching deny must permit the request");
                }
            }
        }
        for (header, condition_key) in [("user-agent", "aws:UserAgent"), ("referer", "aws:Referer")] {
            iam.set_policy(
                "protocol-policy",
                decode_policy(json!({"Version":"2012-10-17","Statement":[allow.clone(),{
                    "Effect":"Deny","Action":"s3:GetObject","Resource":format!("arn:aws:s3:::{bucket}/*"),
                    "Condition":{"StringEquals":{(condition_key):"blocked-client"}}
                }]}))
                .expect("parse header condition"),
            )
            .await
            .expect("save header condition");
            let session = &mut sessions[0];
            session.request_headers.insert(
                http::header::HeaderName::from_static(header),
                "blocked-client".parse().expect("header value"),
            );
            assert!(
                matches!(
                    backend
                        .authorize_operation(session, &S3Action::GetObject, bucket, Some("secret.txt"))
                        .await,
                    Err(AuthorizationError::AccessDenied)
                ),
                "WebDAV header condition must be enforced"
            );
            session.request_headers.remove(header);
            backend
                .authorize_operation(session, &S3Action::GetObject, bucket, Some("secret.txt"))
                .await
                .expect("absent header follows condition semantics");
        }
        let conditional_allow = json!({"Version":"2012-10-17","Statement":[{
            "Effect":"Allow","Action":"s3:GetObject","Resource":format!("arn:aws:s3:::{bucket}/*"),
            "Condition":{"IpAddress":{"aws:SourceIp":"127.0.0.0/8"}}
        }]});
        iam.set_policy("protocol-policy", decode_policy(conditional_allow).expect("parse conditional allow"))
            .await
            .expect("save conditional allow");
        for session in &sessions {
            backend
                .authorize_operation(session, &S3Action::GetObject, bucket, Some("secret.txt"))
                .await
                .expect("matching conditional allow");
            assert!(
                matches!(
                    backend
                        .authorize_operation(session, &S3Action::PutObject, bucket, Some("secret.txt"))
                        .await,
                    Err(AuthorizationError::AccessDenied)
                ),
                "read-only user cannot write"
            );
        }
        #[cfg(feature = "ftps")]
        {
            // Drive libunftp's real authentication handoff: a lost peer address would
            // make this conditional Allow fail even though a manually built session works.
            let reservation = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("reserve FTP address");
            let address = reservation.local_addr().expect("FTP address");
            drop(reservation);
            let server = rustfs_protocols::FtpsServer::new(
                rustfs_protocols::FtpsConfig {
                    bind_addr: address,
                    tls_enabled: false,
                    ftps_required: false,
                    ..Default::default()
                },
                backend.clone(),
            )
            .await
            .expect("create FTP server");
            let (shutdown, receiver) = tokio::sync::broadcast::channel(1);
            let server_task = tokio::spawn(async move { server.start(receiver).await });
            use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
            let stream = tokio::time::timeout(std::time::Duration::from_secs(10), async {
                loop {
                    match tokio::net::TcpStream::connect(address).await {
                        Ok(stream) => break stream,
                        Err(_) => tokio::time::sleep(std::time::Duration::from_millis(20)).await,
                    }
                }
            })
            .await
            .expect("FTP listener ready");
            let (read, mut write) = stream.into_split();
            let mut read = tokio::io::BufReader::new(read);
            async fn reply(read: &mut tokio::io::BufReader<tokio::net::tcp::OwnedReadHalf>) -> String {
                let mut line = String::new();
                tokio::time::timeout(std::time::Duration::from_secs(10), read.read_line(&mut line))
                    .await
                    .expect("FTP response deadline")
                    .expect("read FTP response");
                line
            }
            assert!(reply(&mut read).await.starts_with("220"));
            for (command, status) in [
                ("USER protocol-user\r\n", "331"),
                ("PASS protocol-user-secret\r\n", "230"),
                ("TYPE I\r\n", "200"),
            ] {
                write.write_all(command.as_bytes()).await.expect("write FTP command");
                let response = reply(&mut read).await;
                assert!(response.starts_with(status), "unexpected FTP response: {response}");
            }
            write
                .write_all(format!("SIZE /{bucket}/secret.txt\r\n").as_bytes())
                .await
                .expect("send SIZE");
            let response = reply(&mut read).await;
            assert!(response.starts_with("213"), "real FTP peer must satisfy conditional Allow: {response}");
            iam.set_policy(
                "protocol-policy",
                decode_policy(json!({"Version":"2012-10-17","Statement":[allow,{
                    "Effect":"Deny","Action":"s3:GetObject","Resource":format!("arn:aws:s3:::{bucket}/*"),
                    "Condition":{"IpAddress":{"aws:SourceIp":"127.0.0.0/8"}}
                }]}))
                .expect("parse FTP deny"),
            )
            .await
            .expect("save FTP deny");
            write
                .write_all(format!("SIZE /{bucket}/secret.txt\r\n").as_bytes())
                .await
                .expect("send denied SIZE");
            let response = reply(&mut read).await;
            assert!(response.starts_with("550"), "real FTP peer must match conditional Deny: {response}");
            write.write_all(b"QUIT\r\n").await.expect("send QUIT");
            assert!(reply(&mut read).await.starts_with("221"));
            shutdown.send(()).expect("stop FTP server");
            server_task.await.expect("FTP server task").expect("FTP server shutdown");
        }
    });
}
