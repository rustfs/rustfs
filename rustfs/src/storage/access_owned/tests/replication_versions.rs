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

//! Replication requests against the owned per-operation authorization: a replication HEAD needs
//! read permission before replication permission, and a replica write or delete is authorized
//! against the version its replication header names.
//!
//! Responsible for: the allow/deny decisions and the `ReqInfo` version that `authorize_head_object`,
//! `authorize_put_object`, `authorize_copy_object`, `authorize_create_multipart_upload` and
//! `authorize_delete_object` reach for replication requests, against real bucket policies.
//! Not responsible for: decoding the replication headers from the wire, or the storage use cases
//! that run after authorization.
//! Upstream: the owned request fixture and the gating test store. Downstream: `storage::access_owned`.

use crate::storage::access_owned::listing_multipart::{CreateMultipartAuthorization, authorize_create_multipart_upload};
use crate::storage::access_owned::object_commit::{CopyObjectAuthorization, CopySourceObject, authorize_copy_object};
use crate::storage::access_owned::object_misc::authorize_delete_object;
use crate::storage::access_owned::object_read::authorize_head_object;
use crate::storage::access_owned::object_write::{PutObjectAuthorization, authorize_put_object};
use crate::storage::authorization::replication_request_authorized;
use crate::storage::authorization::tests::fixture::{request, server_context};
use crate::storage::storage_api::contract::bucket::{BucketOperations as _, MakeBucketOptions};
use crate::storage::storage_api::contract::object::{ObjectIO as _, ObjectOperations as _};
use http::Method;
use rustfs_s3_types::S3ErrorCode;

#[test]
fn replication_head_checks_read_permissions_before_replication_permissions() {
    crate::app::gating_test_env::run_large_stack_test("replication-head-permissions", || async {
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        let bucket = format!("replica-head-{}", uuid::Uuid::new_v4());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create bucket");
        let slot = server_context(store.clone());
        for (grant_read, deny_read) in [(false, false), (true, true), (true, false)] {
            let mut statements = vec![
                serde_json::json!({"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:ReplicateObject","Resource":format!("arn:aws:s3:::{bucket}/*")}),
            ];
            for (enabled, effect) in [(grant_read, "Allow"), (deny_read, "Deny")] {
                if enabled {
                    statements.push(serde_json::json!({"Effect":effect,"Principal":{"AWS":"*"},"Action":["s3:GetObject","s3:GetObjectVersion"],"Resource":format!("arn:aws:s3:::{bucket}/*")}));
                }
            }
            store
                .update_bucket_metadata_config(
                    &bucket,
                    crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
                    serde_json::to_vec(&serde_json::json!({"Version":"2012-10-17","Statement":statements})).unwrap(),
                )
                .await
                .expect("publish policy");
            for version in [None, Some("null"), Some("12345678-1234-1234-1234-123456789abc")] {
                for header in ["x-rustfs-source-replication-check", "x-minio-source-replication-check"] {
                    let mut envelope = request(Method::HEAD)
                        .req_info()
                        .server_context(&slot)
                        .header(header, "true")
                        .build();
                    let result = authorize_head_object(&slot, &mut envelope, &bucket, "object", version).await;
                    if grant_read && !deny_read {
                        result.expect("read and replication permissions allow metadata check");
                        assert!(replication_request_authorized(&envelope));
                    } else {
                        assert_eq!(
                            result.expect_err("replication cannot replace read permission").code(),
                            &S3ErrorCode::AccessDenied
                        );
                        assert!(!replication_request_authorized(&envelope));
                    }
                }
            }
        }
    });
}

#[test]
fn replication_write_hooks_authorize_the_destination_version_and_tags() {
    crate::app::gating_test_env::run_large_stack_test("replication-version-permissions", || async {
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        let bucket = format!("replica-write-{}", uuid::Uuid::new_v4());
        store
            .make_bucket(
                &bucket,
                &MakeBucketOptions {
                    versioning_enabled: true,
                    ..Default::default()
                },
            )
            .await
            .expect("create versioned bucket");
        let slot = server_context(store.clone());
        let mut versions = Vec::new();
        for (body, tags) in [
            (b"secret".to_vec(), "classification=secret"),
            (b"public".to_vec(), "classification=public"),
        ] {
            let object = store
                .put_object(
                    &bucket,
                    "target",
                    &mut crate::storage::PutObjReader::from_vec(body),
                    &crate::storage::ObjectOptions {
                        versioned: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("seed version");
            let version = object.version_id.expect("UUID version").to_string();
            store
                .put_object_tags(
                    &bucket,
                    "target",
                    tags,
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
        for condition in [
            serde_json::json!({"s3:versionid":versions[0]}),
            serde_json::json!({"s3:ExistingObjectTag/classification":"secret"}),
        ] {
            let policy = serde_json::json!({"Version":"2012-10-17","Statement":[
                {"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:*","Resource":format!("arn:aws:s3:::{bucket}/*")},
                {"Effect":"Deny","Principal":{"AWS":"*"},"Action":"s3:PutObject","Resource":format!("arn:aws:s3:::{bucket}/target"),"Condition":{"StringEquals":condition}}
            ]});
            store
                .update_bucket_metadata_config(
                    &bucket,
                    crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
                    serde_json::to_vec(&policy).unwrap(),
                )
                .await
                .expect("publish version policy");
            for (index, version) in versions.iter().enumerate() {
                for header in [None, Some("x-rustfs-source-version-id"), Some("x-minio-source-version-id")] {
                    for operation in ["put", "copy", "multipart"] {
                        let query_version = header.is_none().then(|| version.clone());
                        // A query version alone also requires replication authorization.
                        let mut builder = request(Method::PUT)
                            .req_info()
                            .server_context(&slot)
                            .header("x-rustfs-source-replication-request", "true");
                        if let Some(header) = header {
                            builder = builder.header(header, version);
                        }
                        let mut envelope = builder.build();
                        let result = match operation {
                            "put" => {
                                let facts = PutObjectAuthorization {
                                    bucket: &bucket,
                                    key: "target",
                                    version_id: query_version.as_deref(),
                                    retain_until: None,
                                    specific_etag: false,
                                    legal_hold: false,
                                    retention: false,
                                    post: false,
                                };
                                authorize_put_object(&mut envelope, &slot, facts, |_| Ok(())).await
                            }
                            "copy" => {
                                // `x-amz-copy-source: /{bucket}/source`, no tagging, no Object Lock.
                                let facts = CopyObjectAuthorization {
                                    bucket: &bucket,
                                    key: "target",
                                    source: Some(CopySourceObject {
                                        bucket: &bucket,
                                        key: "source",
                                        version_id: None,
                                    }),
                                    query_version: query_version.as_deref(),
                                    tagging: None,
                                    tagging_directive: None,
                                    retain_until: None,
                                    legal_hold: false,
                                    retention: false,
                                };
                                authorize_copy_object(&mut envelope, &slot, facts).await
                            }
                            _ => {
                                let facts = CreateMultipartAuthorization {
                                    bucket: &bucket,
                                    key: "target",
                                    query_version: query_version.as_deref(),
                                    retain_until: None,
                                    legal_hold_present: false,
                                    retention_present: false,
                                };
                                authorize_create_multipart_upload(&slot, &mut envelope, facts).await
                            }
                        };
                        let info = envelope.req_info().unwrap().clone();
                        assert_eq!(info.version_id.as_deref(), Some(version.as_str()), "{operation}");
                        if index == 0 {
                            assert_eq!(
                                result.expect_err("protected destination version must be denied").code(),
                                &S3ErrorCode::AccessDenied,
                                "{operation}"
                            );
                        } else {
                            result.expect("unprotected destination version remains authorized");
                        }
                    }
                }
            }
        }
    });
}

#[test]
fn replication_delete_hook_authorizes_the_header_selected_version() {
    crate::app::gating_test_env::run_large_stack_test("replication-delete-version-permissions", || async {
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        let bucket = format!("replica-delete-{}", uuid::Uuid::new_v4());
        store
            .make_bucket(
                &bucket,
                &MakeBucketOptions {
                    versioning_enabled: true,
                    ..Default::default()
                },
            )
            .await
            .expect("create versioned bucket");
        let slot = server_context(store.clone());
        let mut versions = Vec::new();
        for body in [b"protected".to_vec(), b"disposable".to_vec()] {
            let object = store
                .put_object(
                    &bucket,
                    "target",
                    &mut crate::storage::PutObjReader::from_vec(body),
                    &crate::storage::ObjectOptions {
                        versioned: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("seed version");
            versions.push(object.version_id.expect("UUID version").to_string());
        }
        let policy = serde_json::json!({"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:*","Resource":format!("arn:aws:s3:::{bucket}/*")},
            {"Effect":"Deny","Principal":{"AWS":"*"},"Action":"s3:DeleteObjectVersion","Resource":format!("arn:aws:s3:::{bucket}/target"),"Condition":{"StringEquals":{"s3:versionid":versions[0]}}}
        ]});
        store
            .update_bucket_metadata_config(
                &bucket,
                crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
                serde_json::to_vec(&policy).unwrap(),
            )
            .await
            .expect("publish version policy");
        // The version named only by the replication header must be the one
        // the policy sees: a plain `s3:DeleteObject` evaluation would let a
        // replica delete remove the protected version. A request without
        // the REPLICA status is authorized for the query version alone,
        // because storage ignores the header for it (a stray header must
        // not switch the evaluated action to `s3:DeleteObjectVersion`).
        for (header, version, replica, expect_denied) in [
            ("x-rustfs-source-version-id", &versions[0], true, true),
            ("x-minio-source-version-id", &versions[0], true, true),
            ("x-rustfs-source-version-id", &versions[0], false, false),
            ("x-rustfs-source-version-id", &versions[1], true, false),
        ] {
            let mut builder = request(Method::DELETE).req_info().server_context(&slot);
            if replica {
                builder = builder
                    .header("x-rustfs-source-replication-request", "true")
                    .header(rustfs_utils::http::headers::AMZ_BUCKET_REPLICATION_STATUS, "REPLICA");
            }
            let mut envelope = builder.header(header, version).build();
            // DeleteObject with no `versionId` query parameter.
            let result = authorize_delete_object(&slot, &mut envelope, &bucket, "target", None).await;
            let info = envelope.req_info().unwrap();
            let expected_version = replica.then_some(version.as_str());
            assert_eq!(info.version_id.as_deref(), expected_version, "{header} replica={replica}");
            if expect_denied {
                assert_eq!(
                    result
                        .expect_err("protected version named by the header must be denied")
                        .code(),
                    &S3ErrorCode::AccessDenied,
                    "{header}"
                );
            } else {
                result.expect("a plain delete, or a replica delete of the unprotected version, succeeds");
            }
        }
        store
            .get_object_info(
                &bucket,
                "target",
                &crate::storage::ObjectOptions {
                    version_id: Some(versions[0].clone()),
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("the protected version survives the denied replica deletes");
    });
}
