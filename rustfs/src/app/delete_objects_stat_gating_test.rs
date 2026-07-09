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

//! Regression coverage for backlog#929 (HP-8): the DeleteObjects batch path
//! gates its two per-object metadata stat fanouts on the bucket configuration.
//! These tests run against a real 4-disk `ECStore` with the bucket metadata
//! sys initialized, so both gate branches are exercised with production
//! metadata resolution:
//!
//! - buckets created with Object Lock keep the held-lock stat and the #4297
//!   delete protection (explicit-version deletes of retained objects are
//!   rejected);
//! - buckets without Object Lock take the gated (stat-skipping) path and must
//!   behave exactly as before: unversioned batch deletes remove objects and
//!   report per-key results, versioned batch deletes still create delete
//!   markers and preserve the underlying version.

use super::gating_test_env::shared_gating_ecstore;
use super::storage_api::test::contract::bucket::{BucketOperations, MakeBucketOptions};
use super::storage_api::test::contract::object::{ObjectIO as _, ObjectOperations as _};
use super::storage_api::test::{StorageObjectOptions as ObjectOptions, StoragePutObjReader as PutObjReader};
use crate::storage::storage_api::{StorageObjectLockDeleteOptions, StorageObjectToDelete as ObjectToDelete};
use serial_test::serial;
use uuid::Uuid;

fn compliance_retention_metadata() -> std::collections::HashMap<String, String> {
    let retain_until = time::OffsetDateTime::now_utc() + time::Duration::days(30);
    let mut user_defined = std::collections::HashMap::new();
    user_defined.insert("x-amz-object-lock-mode".to_string(), "COMPLIANCE".to_string());
    user_defined.insert(
        "x-amz-object-lock-retain-until-date".to_string(),
        retain_until
            .format(&time::format_description::well_known::Rfc3339)
            .expect("retain-until date should format"),
    );
    user_defined
}

#[tokio::test]
#[serial]
async fn object_lock_bucket_batch_delete_keeps_held_lock_protection() {
    let ecstore = shared_gating_ecstore().await;
    let bucket = format!("hp8-lock-{}", Uuid::new_v4());

    ecstore
        .make_bucket(
            &bucket,
            &MakeBucketOptions {
                lock_enabled: true,
                ..Default::default()
            },
        )
        .await
        .expect("create object-lock bucket");

    let mut reader = PutObjReader::from_vec(b"retained payload".to_vec());
    let put_info = ecstore
        .put_object(
            &bucket,
            "retained.bin",
            &mut reader,
            &ObjectOptions {
                versioned: true,
                user_defined: compliance_retention_metadata(),
                ..Default::default()
            },
        )
        .await
        .expect("put retained object");
    let version_id = put_info.version_id.expect("lock bucket writes must be versioned");

    let (_deleted, errs) = ecstore
        .delete_objects(
            &bucket,
            vec![ObjectToDelete {
                object_name: "retained.bin".to_string(),
                version_id: Some(version_id),
                ..Default::default()
            }],
            ObjectOptions {
                versioned: true,
                object_lock_delete: Some(StorageObjectLockDeleteOptions {
                    bypass_governance: false,
                }),
                ..Default::default()
            },
        )
        .await;

    assert!(
        errs[0].is_some(),
        "explicit-version delete of a COMPLIANCE-retained object must be rejected on lock buckets"
    );

    ecstore
        .get_object_info(
            &bucket,
            "retained.bin",
            &ObjectOptions {
                version_id: Some(version_id.to_string()),
                versioned: true,
                ..Default::default()
            },
        )
        .await
        .expect("retained version must survive the batch delete");
}

#[tokio::test]
#[serial]
async fn non_lock_versioned_bucket_batch_delete_still_creates_delete_marker() {
    let ecstore = shared_gating_ecstore().await;
    let bucket = format!("hp8-versioned-{}", Uuid::new_v4());

    ecstore
        .make_bucket(
            &bucket,
            &MakeBucketOptions {
                versioning_enabled: true,
                ..Default::default()
            },
        )
        .await
        .expect("create versioned bucket");

    let mut reader = PutObjReader::from_vec(b"versioned payload".to_vec());
    let put_info = ecstore
        .put_object(
            &bucket,
            "versioned.bin",
            &mut reader,
            &ObjectOptions {
                versioned: true,
                ..Default::default()
            },
        )
        .await
        .expect("put versioned object");
    let version_id = put_info.version_id.expect("versioned write must return a version id");

    // No explicit version id: this is the delete-marker-creating shape that
    // skips both stat fanouts on a non-lock, non-replicated bucket.
    let (deleted, errs) = ecstore
        .delete_objects(
            &bucket,
            vec![ObjectToDelete {
                object_name: "versioned.bin".to_string(),
                ..Default::default()
            }],
            ObjectOptions {
                versioned: true,
                object_lock_delete: Some(StorageObjectLockDeleteOptions {
                    bypass_governance: false,
                }),
                ..Default::default()
            },
        )
        .await;

    assert!(errs[0].is_none(), "delete-marker creation must succeed: {:?}", errs[0]);
    assert!(
        deleted[0].delete_marker,
        "versioned delete without version id must create a delete marker"
    );
    assert!(
        deleted[0].delete_marker_version_id.is_some(),
        "delete marker must carry its own version id"
    );

    ecstore
        .get_object_info(
            &bucket,
            "versioned.bin",
            &ObjectOptions {
                version_id: Some(version_id.to_string()),
                versioned: true,
                ..Default::default()
            },
        )
        .await
        .expect("original version must survive delete-marker creation");
}

#[tokio::test]
#[serial]
async fn non_lock_unversioned_bucket_batch_delete_reports_per_key_results() {
    let ecstore = shared_gating_ecstore().await;
    let bucket = format!("hp8-plain-{}", Uuid::new_v4());

    ecstore
        .make_bucket(&bucket, &MakeBucketOptions::default())
        .await
        .expect("create plain bucket");

    for object in ["keep-a.bin", "keep-b.bin"] {
        let mut reader = PutObjReader::from_vec(b"plain payload".to_vec());
        ecstore
            .put_object(&bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("put plain object");
    }

    let (deleted, errs) = ecstore
        .delete_objects(
            &bucket,
            vec![
                ObjectToDelete {
                    object_name: "keep-a.bin".to_string(),
                    ..Default::default()
                },
                ObjectToDelete {
                    object_name: "missing.bin".to_string(),
                    ..Default::default()
                },
                ObjectToDelete {
                    object_name: "keep-b.bin".to_string(),
                    ..Default::default()
                },
            ],
            ObjectOptions {
                object_lock_delete: Some(StorageObjectLockDeleteOptions {
                    bypass_governance: false,
                }),
                ..Default::default()
            },
        )
        .await;

    assert!(
        errs.iter().all(Option::is_none),
        "batch delete on the gated (stat-skipping) path must keep S3 per-key semantics: {errs:?}"
    );
    assert_eq!(deleted[0].object_name, "keep-a.bin");
    assert_eq!(deleted[1].object_name, "missing.bin");
    assert_eq!(deleted[2].object_name, "keep-b.bin");

    for object in ["keep-a.bin", "keep-b.bin"] {
        ecstore
            .get_object_info(&bucket, object, &ObjectOptions::default())
            .await
            .expect_err("deleted object must be gone");
    }
}
