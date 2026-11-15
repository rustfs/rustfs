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

//! Read authorization tests for the legacy request and owned envelope.

use super::authorize_head_object;
use crate::app::RequestEnvelope;
use crate::storage::access::{ReqInfo, S3ErrorCode, S3Request, ServerContextSlot};
use crate::storage::storage_api::contract::bucket::{BucketOperations as _, MakeBucketOptions};
use http::{Extensions, HeaderMap, HeaderValue, Method, Uri};
use std::sync::Arc;

#[test]
#[serial_test::serial]
fn head_replication_marker_requires_its_own_permission() {
    crate::app::gating_test_env::run_large_stack_test("head-replica-marker", || async {
        let context = crate::app::gating_test_env::shared_gating_ambient().await;
        let store = context.object_store();
        let bucket = format!("head-replica-marker-{}", uuid::Uuid::new_v4().simple());
        store.make_bucket(&bucket, &MakeBucketOptions::default()).await.unwrap();
        let slot = ServerContextSlot::new();
        assert!(slot.install(Arc::clone(&context)));
        for (allow, deny) in [(false, false), (true, true), (true, false)] {
            let mut statements = vec![
                serde_json::json!({"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":format!("arn:aws:s3:::{bucket}/*")}),
            ];
            for (enabled, effect) in [(allow, "Allow"), (deny, "Deny")] {
                if enabled {
                    statements.push(serde_json::json!({"Effect":effect,"Principal":{"AWS":"*"},"Action":"s3:ReplicateObject","Resource":format!("arn:aws:s3:::{bucket}/*")}));
                }
            }
            store
                .update_bucket_metadata_config(
                    &bucket,
                    crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
                    serde_json::to_vec(&serde_json::json!({"Version":"2012-10-17","Statement":statements})).unwrap(),
                )
                .await
                .unwrap();
            let headers = if !allow && !deny {
                vec![
                    None,
                    Some("x-rustfs-source-replication-check"),
                    Some("x-minio-source-replication-check"),
                ]
            } else {
                vec![
                    Some("x-rustfs-source-replication-check"),
                    Some("x-minio-source-replication-check"),
                ]
            };
            for header in headers {
                let mut request = S3Request {
                    input: (),
                    method: Method::HEAD,
                    uri: Uri::from_static("/bucket/key"),
                    headers: HeaderMap::new(),
                    extensions: Extensions::new(),
                    credentials: None,
                    region: None,
                    service: None,
                    trailing_headers: None,
                };
                request.extensions.insert(ReqInfo::default());
                request.extensions.insert(Arc::clone(&slot));
                if let Some(header) = header {
                    request.headers.insert(header, HeaderValue::from_static("true"));
                }
                let mut envelope = RequestEnvelope::from_s3s(&request);
                let result = authorize_head_object(&slot, &mut envelope, &bucket, "key", None).await;
                let permitted = header.is_none() || (allow && !deny);
                if permitted {
                    result.expect("read-only control or separately permitted replication");
                } else {
                    assert_eq!(result.unwrap_err().code(), &S3ErrorCode::AccessDenied);
                }
                assert_eq!(envelope.req_info().unwrap().replication_request_authorized, header.is_some() && permitted);
            }
        }
    });
}

#[test]
#[serial_test::serial]
fn get_authorization_binds_version_and_explicit_denial() {
    crate::app::gating_test_env::run_large_stack_test("get-owned-authorization", || async {
        let context = crate::app::gating_test_env::shared_gating_ambient().await;
        let store = context.object_store();
        let slot = ServerContextSlot::new();
        assert!(slot.install(Arc::clone(&context)));
        let mut failures = Vec::new();
        for (actions, explicit_deny, current_allowed, version_allowed) in [
            (&["s3:PutObject"][..], false, false, false),
            (&["s3:GetObject"][..], false, true, false),
            (&["s3:GetObjectVersion"][..], false, true, true),
            (&["s3:GetObject", "s3:GetObjectVersion"][..], true, false, false),
        ] {
            let bucket = format!("get-auth-{}", uuid::Uuid::new_v4().simple());
            store.make_bucket(&bucket, &MakeBucketOptions::default()).await.unwrap();
            let resource = format!("arn:aws:s3:::{bucket}/key");
            let mut statements =
                vec![serde_json::json!({"Effect":"Allow","Principal":{"AWS":"*"},"Action":actions,"Resource":resource})];
            if explicit_deny {
                statements
                    .push(serde_json::json!({"Effect":"Deny","Principal":{"AWS":"*"},"Action":actions,"Resource":resource}));
            }
            store
                .update_bucket_metadata_config(
                    &bucket,
                    crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
                    serde_json::to_vec(&serde_json::json!({"Version":"2012-10-17","Statement":statements})).unwrap(),
                )
                .await
                .unwrap();
            for (version, key) in [None, Some("null"), Some("selected-version")]
                .into_iter()
                .flat_map(|version| ["key", "other"].map(|key| (version, key)))
            {
                let mut request = S3Request {
                    input: (),
                    method: Method::GET,
                    uri: Uri::from_static("/bucket/key"),
                    headers: HeaderMap::new(),
                    extensions: Extensions::new(),
                    credentials: None,
                    region: None,
                    service: None,
                    trailing_headers: None,
                };
                request.extensions.insert(ReqInfo::default());
                request.extensions.insert(Arc::clone(&slot));
                let mut envelope = RequestEnvelope::from_s3s(&request);
                let legacy = super::authorize_get_object(&slot, &mut request, &bucket, key, version).await;
                let native = super::authorize_get_object(&slot, &mut envelope, &bucket, key, version).await;
                let allowed = key == "key" && if version.is_some() { version_allowed } else { current_allowed };
                for (edge, result) in [("legacy", legacy), ("native", native)] {
                    let label = format!("actions={actions:?} deny={explicit_deny} version={version:?} key={key} edge={edge}");
                    let expected = if allowed { None } else { Some(&S3ErrorCode::AccessDenied) };
                    if result.as_ref().err().map(|error| error.code()) != expected {
                        failures.push(format!("{label}: expected {expected:?}, got {result:?}"));
                    }
                }
                for (edge, info) in [
                    ("legacy", request.extensions.get::<ReqInfo>().unwrap()),
                    ("native", envelope.req_info().unwrap()),
                ] {
                    let actual = (info.bucket.as_deref(), info.object.as_deref(), info.version_id.as_deref());
                    let expected = (Some(bucket.as_str()), Some(key), version);
                    if actual != expected {
                        failures.push(format!("{edge}: expected resource {expected:?}, got {actual:?}"));
                    }
                }
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    });
}
