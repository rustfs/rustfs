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

//! Tests of the `bucket_access` legacy-edge handlers (batch A2), moved unchanged from `ecfs.rs` and
//! `ecfs_test.rs`.

use super::{build_request, store_uninitialized_premise_holds};
use crate::storage::ecfs::*;
use http::Method;

#[tokio::test]
async fn test_get_bucket_acl_returns_internal_error_when_store_uninitialized() {
    if !store_uninitialized_premise_holds() {
        return;
    }
    let input = GetBucketAclInput::builder()
        .bucket("test-bucket".to_string())
        .build()
        .unwrap();

    let fs = FS::new();
    let err = fs.get_bucket_acl(build_request(input, Method::GET)).await.unwrap_err();
    assert_eq!(err.code(), &S3ErrorCode::InternalError);
}

#[tokio::test]
async fn test_put_bucket_acl_returns_internal_error_when_store_uninitialized() {
    if !store_uninitialized_premise_holds() {
        return;
    }
    let input = PutBucketAclInput::builder()
        .bucket("test-bucket".to_string())
        .build()
        .unwrap();

    let fs = FS::new();
    let err = fs.put_bucket_acl(build_request(input, Method::PUT)).await.unwrap_err();
    assert_eq!(err.code(), &S3ErrorCode::InternalError);
}
