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

//! Observe GetObjectAttributes completion at the real server's audit and event boundary.

use bytes::Bytes;
use http_body_util::{BodyExt as _, Empty};
use hyper_util::rt::TokioIo;

fn get_object_attributes_completion(stack: Option<&'static str>) {
    temp_env::with_vars([("RUSTFS_S3_STACK", stack), ("RUSTFS_AUDIT_ENABLE", Some("true"))], || {
        crate::app::gating_test_env::run_large_stack_test("attributes-completion", || async {
            use crate::app::storage_api::object_usecase::helper::MutationCompletionProbe;
            use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};
            use crate::app::storage_api::test::contract::object::ObjectIO as _;
            use crate::embedded::{RustFSServerBuilder, find_available_port};

            let server = RustFSServerBuilder::new()
                .address(format!("127.0.0.1:{}", find_available_port().unwrap()))
                .access_key("attributesprobe")
                .secret_key("attributes-fixture-test-secret")
                .build()
                .await
                .unwrap();
            crate::server::refresh_audit_module_enabled();
            let context = crate::app::runtime_sources::current_app_context().unwrap();
            let store = context.object_store();
            let bucket = "attributes-completion";
            store.make_bucket(bucket, &MakeBucketOptions::default()).await.unwrap();
            let policy = serde_json::json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},
                    "Action": ["s3:GetObject", "s3:GetObjectAttributes"],
                    "Resource": [format!("arn:aws:s3:::{bucket}/object"), format!("arn:aws:s3:::{bucket}/missing")],
                }],
            });
            store
                .update_bucket_metadata_config(
                    bucket,
                    crate::app::storage_api::object_usecase::bucket::metadata::BUCKET_POLICY_CONFIG,
                    serde_json::to_vec(&policy).unwrap(),
                )
                .await
                .unwrap();
            store
                .put_object(
                    bucket,
                    "object",
                    &mut crate::app::storage_api::object_usecase::StoragePutObjReader::from_vec(b"payload".to_vec()),
                    &Default::default(),
                )
                .await
                .unwrap();
            let probe = MutationCompletionProbe::install(bucket);
            for (key, expected) in [("object", 200), ("missing", 404), ("other", 403)] {
                let socket = tokio::net::TcpStream::connect(server.address()).await.unwrap();
                let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(socket)).await.unwrap();
                let transport = tokio::spawn(connection);
                let request = http::Request::get(format!("/{bucket}/{key}?attributes"))
                    .header(http::header::HOST, "localhost")
                    .header("x-amz-object-attributes", "ObjectSize")
                    .body(Empty::<Bytes>::new())
                    .unwrap();
                let response = sender.send_request(request).await.unwrap();
                assert_eq!(response.status().as_u16(), expected);
                response.into_body().collect().await.unwrap();
                drop(sender);
                transport.abort();
                let _ = transport.await;
            }
            probe.wait_for(2, 0).await;
            let audits = probe
                .audits
                .lock()
                .unwrap()
                .iter()
                .filter(|entry| entry.api.name.as_deref() == Some(rustfs_s3_ops::S3Operation::GetObjectAttributes.as_str()))
                .map(|entry| (entry.api.object.clone(), entry.api.status.clone(), entry.api.status_code))
                .collect::<Vec<_>>();
            assert_eq!(
                audits,
                [
                    (Some("object".to_owned()), Some("success".to_owned()), Some(200)),
                    (Some("missing".to_owned()), Some("failure".to_owned()), Some(500)),
                ],
            );
            assert!(probe.events.lock().unwrap().is_empty());
            server.shutdown().await;
        })
    })
}

#[test]
#[serial_test::serial]
fn legacy_get_object_attributes_completion() {
    get_object_attributes_completion(None);
}

#[cfg(feature = "gateway-stack")]
#[test]
#[serial_test::serial]
fn gateway_get_object_attributes_completion() {
    get_object_attributes_completion(Some("gateway"));
}
