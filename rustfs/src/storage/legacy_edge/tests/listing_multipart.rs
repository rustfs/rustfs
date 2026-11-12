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

//! Tests of the `listing_multipart` legacy-edge handlers (batch B2), moved unchanged from `ecfs.rs`
//! and `ecfs_test.rs`.

use super::build_request;
use crate::storage::StorageError;
use crate::storage::ecfs::*;
use http::{Method, StatusCode};

#[test]
#[serial_test::serial]
fn list_apis_report_storage_read_quorum_as_unavailable() {
    crate::app::gating_test_env::run_large_stack_test("list-quorum-surface", || async {
        use crate::storage::storage_api::contract::bucket::{BucketOperations as _, MakeBucketOptions};
        crate::app::gating_test_env::shared_gating_ambient().await;
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        let bucket = format!("list-quorum-{}", uuid::Uuid::new_v4().simple());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create listing bucket");
        let set = &store.pools[0].disk_set[0];
        let saved_disks = set.disks.read().await.clone();
        {
            let mut disks = set.disks.write().await;
            for disk in disks.iter_mut().skip(1) {
                *disk = None;
            }
        }
        let fs = FS::new();
        let errors = [
            S3::list_objects(
                &fs,
                build_request(
                    ListObjectsInput {
                        bucket: bucket.clone(),
                        ..Default::default()
                    },
                    Method::GET,
                ),
            )
            .await
            .expect_err("V1 cannot list below storage read quorum"),
            S3::list_objects_v2(
                &fs,
                build_request(
                    ListObjectsV2Input {
                        bucket: bucket.clone(),
                        ..Default::default()
                    },
                    Method::GET,
                ),
            )
            .await
            .expect_err("V2 cannot list below storage read quorum"),
            S3::list_object_versions(
                &fs,
                build_request(
                    ListObjectVersionsInput {
                        bucket,
                        ..Default::default()
                    },
                    Method::GET,
                ),
            )
            .await
            .expect_err("versions cannot list below storage read quorum"),
        ];
        *set.disks.write().await = saved_disks;
        for error in errors {
            assert_eq!(error.code(), &S3ErrorCode::ServiceUnavailable);
            assert_eq!(error.status_code(), Some(StatusCode::SERVICE_UNAVAILABLE));
            assert_eq!(error.message(), Some("The service is unavailable. Please retry."));
            let source = error.source().expect("retain storage diagnostics");
            assert!(
                matches!(
                    source.downcast_ref::<StorageError>(),
                    Some(StorageError::ErasureReadQuorum | StorageError::InsufficientReadQuorum(_, _))
                ),
                "the production LIST boundary must expose a typed read-quorum failure: {source}"
            );
        }
    });
}
