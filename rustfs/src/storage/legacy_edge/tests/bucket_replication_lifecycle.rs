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

//! Tests of the `bucket_replication_lifecycle` legacy-edge handlers (batch A3), moved unchanged
//! from `ecfs.rs` and `ecfs_test.rs`.

use super::super::bucket_replication_lifecycle::{
    SITE_REPLICATION_GATE_FORCE_DISABLED, SITE_REPLICATION_GATE_FORCE_ENABLED, SITE_REPLICATION_GATE_TEST_OVERRIDE,
};
use super::{build_request, store_uninitialized_premise_holds};
use crate::storage::access::ReqInfo;
use crate::storage::ecfs::*;
use http::Method;
use http::StatusCode;
use std::sync::atomic::Ordering;

fn replication_config_edit_request<T>(input: T, is_owner: bool) -> S3Request<T> {
    let mut req = S3Request {
        input,
        method: Method::PUT,
        uri: http::Uri::from_static("/"),
        headers: http::HeaderMap::new(),
        extensions: http::Extensions::new(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    };
    req.extensions.insert(ReqInfo {
        is_owner,
        ..Default::default()
    });
    req
}

fn put_bucket_replication_input() -> PutBucketReplicationInput {
    PutBucketReplicationInput {
        bucket: "test-bucket".to_string(),
        checksum_algorithm: None,
        content_md5: None,
        expected_bucket_owner: None,
        replication_configuration: ReplicationConfiguration {
            role: String::new(),
            rules: Vec::new(),
        },
        token: None,
    }
}

fn delete_bucket_replication_input() -> DeleteBucketReplicationInput {
    DeleteBucketReplicationInput {
        bucket: "test-bucket".to_string(),
        expected_bucket_owner: None,
    }
}

fn assert_replication_deny_edit(err: &S3Error) {
    match err.code() {
        S3ErrorCode::Custom(code) => assert_eq!(code, "XMinioReplicationDenyEdit"),
        other => panic!("expected XMinioReplicationDenyEdit, got {other:?}"),
    }
    assert_eq!(err.status_code(), Some(StatusCode::BAD_REQUEST));
}

/// Single test on purpose: the branches share the process-wide gate
/// override, and parallel tests would race it.
#[tokio::test]
async fn replication_config_edit_gate_denies_only_non_owner_under_site_replication() {
    let fs = FS::new();
    SITE_REPLICATION_GATE_TEST_OVERRIDE.store(SITE_REPLICATION_GATE_FORCE_ENABLED, Ordering::SeqCst);

    // Non-owner PUT/DELETE through the real S3 handlers: denied by the
    // gate before the usecase (and thus the store) is ever touched.
    let err = fs
        .put_bucket_replication(replication_config_edit_request(put_bucket_replication_input(), false))
        .await
        .expect_err("non-owner PutBucketReplication must be denied while site replication is enabled");
    assert_replication_deny_edit(&err);
    let err = fs
        .delete_bucket_replication(replication_config_edit_request(delete_bucket_replication_input(), false))
        .await
        .expect_err("non-owner DeleteBucketReplication must be denied while site replication is enabled");
    assert_replication_deny_edit(&err);

    // Owner passes the gate (the usecase's empty-rules structure error
    // proves the request reached the usecase instead of the deny path).
    let err = fs
        .put_bucket_replication(replication_config_edit_request(put_bucket_replication_input(), true))
        .await
        .expect_err("owner request should pass the gate and fail later on config validation");
    assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);

    // Without site replication the policy check alone still governs the edit.
    SITE_REPLICATION_GATE_TEST_OVERRIDE.store(SITE_REPLICATION_GATE_FORCE_DISABLED, Ordering::SeqCst);
    let err = fs
        .put_bucket_replication(replication_config_edit_request(put_bucket_replication_input(), false))
        .await
        .expect_err("non-owner request should pass the gate and fail later on config validation");
    assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);

    SITE_REPLICATION_GATE_TEST_OVERRIDE.store(0, Ordering::SeqCst);
}

#[tokio::test]
async fn test_put_object_lock_configuration_returns_internal_error_when_store_uninitialized() {
    if !store_uninitialized_premise_holds() {
        return;
    }
    let input = PutObjectLockConfigurationInput::builder()
        .bucket("test-bucket".to_string())
        .object_lock_configuration(Some(ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            rule: None,
        }))
        .build()
        .unwrap();

    let fs = FS::new();
    let err = fs
        .put_object_lock_configuration(build_request(input, Method::PUT))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &S3ErrorCode::InternalError);
}
