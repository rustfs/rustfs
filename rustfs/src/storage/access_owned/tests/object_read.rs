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

//! Read authorization tests on the owned request envelope.
//!
//! Tests: `authorize_head_object`, `authorize_get_object` and `authorize_get_object_attributes`
//! against bucket policies, and the resource each leaves in `ReqInfo`. Not tested: the read
//! handlers or the request decoder.
//! Upstream: `storage::authorization::tests::fixture`. Downstream: none.

use crate::storage::access_owned::object_read::{authorize_get_object, authorize_get_object_attributes, authorize_head_object};
use crate::storage::authorization::tests::fixture::request;
use crate::storage::storage_api::contract::bucket::{BucketOperations as _, MakeBucketOptions};
use crate::storage::storage_api::runtime_sources_consumer::ServerContextSlot;
use http::Method;
use rustfs_s3_types::S3ErrorCode;
use std::sync::Arc;

#[test]
#[serial_test::serial]
fn missing_object_read_permissions_ignore_list_query_conditions() {
    use crate::storage::authorization::{authorize_bucket_request, authorize_missing_object_disclosure};
    use crate::storage::req_info::ReqInfo;
    use rustfs_policy::policy::action::{Action, S3Action};

    crate::app::gating_test_env::run_large_stack_test("object-read-list-conditions", || async {
        let context = crate::app::gating_test_env::shared_gating_ambient().await;
        let store = context.object_store();
        let bucket = format!("read-list-conditions-{}", uuid::Uuid::new_v4().simple());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create policy fixture bucket");
        let allow_list_if = |condition| {
            serde_json::json!({
                "Effect": "Allow", "Principal": "*", "Action": "s3:ListBucket", "Resource": format!("arn:aws:s3:::{bucket}"),
                "Condition": {"StringEquals": condition}
            })
        };
        let policy = serde_json::json!({"Version": "2012-10-17", "Statement": [
            allow_list_if(serde_json::json!({"s3:prefix": "reports/"})),
            allow_list_if(serde_json::json!({"aws:UserAgent": "fixture-reader"})),
        ]});
        store
            .update_bucket_metadata_config(
                &bucket,
                crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
                serde_json::to_vec(&policy).expect("serialize bucket policy"),
            )
            .await
            .expect("install bucket policy");
        let slot = ServerContextSlot::new();
        assert!(slot.install(context));
        let fixture = |method| request(method).uri("/?prefix=reports/").server_context(&slot);
        let mut listing = fixture(Method::GET).req_info().build();
        authorize_bucket_request(&mut listing, &bucket, Action::S3Action(S3Action::ListBucketAction))
            .await
            .expect("prefix permits the actual listing");
        for method in [Method::GET, Method::HEAD] {
            let info = ReqInfo {
                bucket: Some(bucket.clone()),
                object: Some("document".into()),
                ..Default::default()
            };
            let mut read = fixture(method.clone()).extension(info.clone()).build();
            let error = authorize_missing_object_disclosure(&mut read)
                .await
                .expect_err("object reads have no listing prefix");
            assert_eq!(error.code(), &S3ErrorCode::AccessDenied);
            let mut read = fixture(method).header("user-agent", "fixture-reader").extension(info).build();
            authorize_missing_object_disclosure(&mut read)
                .await
                .expect("an independent request condition can allow listing");
        }
    });
}

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
                let mut builder = request(Method::HEAD).uri("/bucket/key").req_info().server_context(&slot);
                if let Some(header) = header {
                    builder = builder.header(header, "true");
                }
                let mut envelope = builder.build();
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
                let mut envelope = request(Method::GET)
                    .uri("/bucket/key")
                    .req_info()
                    .server_context(&slot)
                    .build();
                let result = authorize_get_object(&slot, &mut envelope, &bucket, key, version).await;
                let allowed = key == "key" && if version.is_some() { version_allowed } else { current_allowed };
                let label = format!("actions={actions:?} deny={explicit_deny} version={version:?} key={key}");
                let expected = if allowed { None } else { Some(&S3ErrorCode::AccessDenied) };
                if result.as_ref().err().map(|error| error.code()) != expected {
                    failures.push(format!("{label}: expected {expected:?}, got {result:?}"));
                }
                let info = envelope.req_info().unwrap();
                let actual = (info.bucket.as_deref(), info.object.as_deref(), info.version_id.as_deref());
                let expected = (Some(bucket.as_str()), Some(key), version);
                if actual != expected {
                    failures.push(format!("{label}: expected resource {expected:?}, got {actual:?}"));
                }
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    });
}

#[test]
#[serial_test::serial]
fn attributes_authorization_requires_each_action_on_the_original_version() {
    crate::app::gating_test_env::run_large_stack_test("attributes-authorization", || async {
        let context = crate::app::gating_test_env::shared_gating_ambient().await;
        let store = context.object_store();
        let slot = ServerContextSlot::new();
        assert!(slot.install(Arc::clone(&context)));
        for (version, required) in [
            (None, ["s3:GetObjectAttributes", "s3:GetObject"]),
            (Some(""), ["s3:GetObjectVersionAttributes", "s3:GetObjectVersion"]),
            (Some("null"), ["s3:GetObjectVersionAttributes", "s3:GetObjectVersion"]),
            (
                Some(" aedfa2f8-840d-4a1c-b717-04411d82389e "),
                ["s3:GetObjectVersionAttributes", "s3:GetObjectVersion"],
            ),
        ] {
            for (allowed, denied, key, permitted) in [
                (&required[..], None, "key", true),
                (&required[..1], None, "key", false),
                (&required[1..], None, "key", false),
                (&required[..], Some(required[0]), "key", false),
                (&required[..], Some(required[1]), "key", false),
                (&required[..], None, "unrelated", false),
            ] {
                let bucket = format!("attributes-auth-{}", uuid::Uuid::new_v4().simple());
                store.make_bucket(&bucket, &MakeBucketOptions::default()).await.unwrap();
                let resource = format!("arn:aws:s3:::{bucket}/key");
                let mut statements = vec![serde_json::json!({
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},
                    "Action": allowed,
                    "Resource": resource,
                })];
                if let Some(action) = denied {
                    statements.push(serde_json::json!({
                        "Effect": "Deny",
                        "Principal": {"AWS": "*"},
                        "Action": action,
                        "Resource": resource,
                    }));
                }
                store
                    .update_bucket_metadata_config(
                        &bucket,
                        crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
                        serde_json::to_vec(&serde_json::json!({"Version":"2012-10-17","Statement":statements})).unwrap(),
                    )
                    .await
                    .unwrap();
                let mut envelope = request(Method::GET)
                    .uri("/bucket/key?attributes")
                    .req_info()
                    .server_context(&slot)
                    .build();
                let result = authorize_get_object_attributes(&mut envelope, &bucket, key, version).await;
                let expected = (!permitted).then_some(&S3ErrorCode::AccessDenied);
                let info = envelope.req_info().unwrap();
                assert_eq!(
                    result.as_ref().err().map(|error| error.code()),
                    expected,
                    "allow={allowed:?}, deny={denied:?}, key={key}, version={version:?}",
                );
                assert_eq!(
                    (info.bucket.as_deref(), info.object.as_deref(), info.version_id.as_deref()),
                    (Some(bucket.as_str()), Some(key), version)
                );
            }
        }
    });
}
