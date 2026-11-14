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

//! Legacy HeadBucket compatibility for protocol roots and ordinary buckets.

use super::build_request;
use crate::app::gating_test_env::{run_large_stack_test, shared_gating_ambient};
use crate::app::storage_api::test::contract::bucket::{BucketOperations, MakeBucketOptions};
use crate::runtime_sources::ServerContextSlot;
use crate::storage::ecfs::*;
use http::Method;

fn head_bucket_count() -> u64 {
    rustfs_io_metrics::s3_op_metrics_snapshot()
        .into_iter()
        .find(|snapshot| snapshot.op == S3Operation::HeadBucket.as_str())
        .map(|snapshot| snapshot.total)
        .unwrap_or_default()
}

#[test]
#[serial_test::serial]
fn head_bucket_preserves_protocol_root_response() {
    run_large_stack_test("head-bucket-protocol-root", || async {
        let context = shared_gating_ambient().await;
        let slot = ServerContextSlot::new();
        assert!(slot.install(context));
        let store = slot.object_store().expect("initialized store");
        let bucket = format!("head-root-{}", uuid::Uuid::new_v4().simple());
        store.make_bucket(&bucket, &MakeBucketOptions::default()).await.unwrap();
        let fs = FS::with_server_ctx(slot);
        for name in ["", bucket.as_str()] {
            let input = HeadBucketInput::builder().bucket(name.to_owned()).build().unwrap();
            let before = head_bucket_count();
            let response = fs
                .head_bucket(build_request(input, Method::HEAD))
                .await
                .expect("bucket exists");
            assert!(response.status.is_none());
            assert!(response.headers.is_empty());
            assert_eq!(head_bucket_count(), before + 1);
        }
        let input = HeadBucketInput::builder()
            .bucket(format!("missing-{bucket}"))
            .build()
            .unwrap();
        let before = head_bucket_count();
        let error = fs.head_bucket(build_request(input, Method::HEAD)).await.unwrap_err();
        assert_eq!(error.code(), &S3ErrorCode::NoSuchBucket);
        assert_eq!(head_bucket_count(), before + 1);
    });
}

#[tokio::test]
async fn head_bucket_root_requires_initialized_store() {
    let fs = FS::with_server_ctx(ServerContextSlot::new());
    let input = HeadBucketInput::builder().bucket(String::new()).build().unwrap();
    let before = head_bucket_count();
    let error = fs.head_bucket(build_request(input, Method::HEAD)).await.unwrap_err();
    assert_eq!(error.code(), &S3ErrorCode::InternalError);
    assert_eq!(error.message(), Some("Not init"));
    assert_eq!(head_bucket_count(), before + 1);
}
