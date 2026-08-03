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
//! - delete-marker creation can skip the held-lock stat, while destructive
//!   deletes still inspect legacy or corrupt explicit Object Lock metadata even
//!   when the bucket configuration is confirmed absent.

use super::gating_test_env::shared_gating_ecstore;
use super::storage_api::test::contract::bucket::{BucketOperations, MakeBucketOptions};
use super::storage_api::test::contract::object::{ObjectIO as _, ObjectOperations as _};
use super::storage_api::test::{StorageObjectOptions as ObjectOptions, StoragePutObjReader as PutObjReader};
use crate::storage::storage_api::{StorageError, StorageObjectLockDeleteOptions, StorageObjectToDelete as ObjectToDelete};
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

    assert!(matches!(errs[0], Some(StorageError::PrefixAccessDenied(_, _))));

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
async fn object_lock_batch_delete_preserves_explicit_null_version_protection() {
    let ecstore = shared_gating_ecstore().await;
    let bucket = format!("explicit-null-lock-{}", Uuid::new_v4());

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
    let mut reader = PutObjReader::from_vec(b"retained null version".to_vec());
    ecstore
        .put_object(
            &bucket,
            "null.bin",
            &mut reader,
            &ObjectOptions {
                version_suspended: true,
                user_defined: compliance_retention_metadata(),
                ..Default::default()
            },
        )
        .await
        .expect("put retained null version");

    let (_deleted, errs) = ecstore
        .delete_objects(
            &bucket,
            vec![ObjectToDelete {
                object_name: "null.bin".to_string(),
                version_id: Some(Uuid::nil()),
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

    assert!(matches!(errs[0], Some(StorageError::PrefixAccessDenied(_, _))));
    ecstore
        .get_object_info(
            &bucket,
            "null.bin",
            &ObjectOptions {
                version_id: Some(Uuid::nil().to_string()),
                versioned: true,
                ..Default::default()
            },
        )
        .await
        .expect("retained null version must survive the batch delete");
}

#[tokio::test]
#[serial]
async fn recursive_force_delete_is_blocked_for_object_lock_bucket() {
    let ecstore = shared_gating_ecstore().await;
    let bucket = format!("force-delete-lock-{}", Uuid::new_v4());

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

    let mut reader = PutObjReader::from_vec(b"protected payload".to_vec());
    ecstore
        .put_object(
            &bucket,
            "protected/object.bin",
            &mut reader,
            &ObjectOptions {
                versioned: true,
                ..Default::default()
            },
        )
        .await
        .expect("put object under protected prefix");

    let err = ecstore
        .delete_object(
            &bucket,
            "protected",
            ObjectOptions {
                delete_prefix: true,
                ..Default::default()
            },
        )
        .await
        .expect_err("recursive force-delete must be rejected for Object Lock buckets");

    assert!(matches!(err, StorageError::InvalidArgument(_, _, _)));
    ecstore
        .get_object_info(&bucket, "protected/object.bin", &ObjectOptions::default())
        .await
        .expect("rejected recursive delete must preserve the protected object");
}

#[tokio::test]
#[serial]
async fn lifecycle_style_delete_all_versions_rechecks_each_retained_version() {
    let ecstore = shared_gating_ecstore().await;
    let bucket = format!("delete-all-lock-{}", Uuid::new_v4());

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

    let err = ecstore
        .delete_object(
            &bucket,
            "retained.bin",
            ObjectOptions {
                delete_prefix: true,
                delete_prefix_object: true,
                versioned: true,
                ..Default::default()
            },
        )
        .await
        .expect_err("delete-all must recheck every retained version under the object lock");

    assert!(matches!(err, StorageError::PrefixAccessDenied(_, _)));
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
        .expect("retained version must survive rejected delete-all");
}

#[tokio::test]
#[serial]
async fn malformed_persisted_retention_metadata_blocks_version_delete() {
    let ecstore = shared_gating_ecstore().await;
    let bucket = format!("malformed-retention-{}", Uuid::new_v4());

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

    let mut malformed = std::collections::HashMap::new();
    malformed.insert("x-amz-object-lock-mode".to_string(), "COMPLIANCE".to_string());
    let mut reader = PutObjReader::from_vec(b"must survive".to_vec());
    let put_info = ecstore
        .put_object(
            &bucket,
            "malformed.bin",
            &mut reader,
            &ObjectOptions {
                versioned: true,
                user_defined: malformed,
                ..Default::default()
            },
        )
        .await
        .expect("storage fixture should persist the malformed boundary value");
    let version_id = put_info.version_id.expect("lock bucket writes must be versioned");

    ecstore
        .delete_object(
            &bucket,
            "malformed.bin",
            ObjectOptions {
                version_id: Some(version_id.to_string()),
                versioned: true,
                ..Default::default()
            },
        )
        .await
        .expect_err("incomplete persisted retention metadata must fail closed");

    ecstore
        .get_object_info(
            &bucket,
            "malformed.bin",
            &ObjectOptions {
                version_id: Some(version_id.to_string()),
                versioned: true,
                ..Default::default()
            },
        )
        .await
        .expect("malformed retained object must survive the rejected delete");
}

#[tokio::test]
#[serial]
async fn recursive_force_delete_remains_allowed_for_plain_bucket() {
    let ecstore = shared_gating_ecstore().await;
    let bucket = format!("force-delete-plain-{}", Uuid::new_v4());

    ecstore
        .make_bucket(&bucket, &MakeBucketOptions::default())
        .await
        .expect("create plain bucket");

    for object in ["prefix/a.bin", "prefix/b.bin"] {
        let mut reader = PutObjReader::from_vec(b"plain payload".to_vec());
        ecstore
            .put_object(&bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("put object under plain prefix");
    }

    ecstore
        .delete_object(
            &bucket,
            "prefix",
            ObjectOptions {
                delete_prefix: true,
                ..Default::default()
            },
        )
        .await
        .expect("recursive force-delete should remain allowed for a plain bucket");

    for object in ["prefix/a.bin", "prefix/b.bin"] {
        ecstore
            .get_object_info(&bucket, object, &ObjectOptions::default())
            .await
            .expect_err("recursive force-delete should remove every matching object");
    }
}

#[tokio::test]
#[serial]
async fn plain_bucket_explicit_retention_blocks_every_destructive_delete_shape() {
    let ecstore = shared_gating_ecstore().await;
    let bucket = format!("plain-explicit-lock-{}", Uuid::new_v4());
    ecstore
        .make_bucket(&bucket, &MakeBucketOptions::default())
        .await
        .expect("create plain bucket fixture");

    for object in ["batch.bin", "delete-all.bin", "prefix/retained.bin"] {
        let mut reader = PutObjReader::from_vec(b"legacy retained payload".to_vec());
        ecstore
            .put_object(
                &bucket,
                object,
                &mut reader,
                &ObjectOptions {
                    user_defined: compliance_retention_metadata(),
                    ..Default::default()
                },
            )
            .await
            .expect("storage fixture should persist explicit retention metadata");
    }

    let (_deleted, errors) = ecstore
        .delete_objects(
            &bucket,
            vec![ObjectToDelete {
                object_name: "batch.bin".to_string(),
                ..Default::default()
            }],
            ObjectOptions::default(),
        )
        .await;
    assert!(matches!(errors[0], Some(StorageError::PrefixAccessDenied(_, _))));

    let delete_all_error = ecstore
        .delete_object(
            &bucket,
            "delete-all.bin",
            ObjectOptions {
                delete_prefix: true,
                delete_prefix_object: true,
                ..Default::default()
            },
        )
        .await
        .expect_err("delete-all must inspect explicit retention in a plain bucket");
    assert!(matches!(delete_all_error, StorageError::PrefixAccessDenied(_, _)));

    let prefix_error = ecstore
        .delete_object(
            &bucket,
            "prefix",
            ObjectOptions {
                delete_prefix: true,
                ..Default::default()
            },
        )
        .await
        .expect_err("recursive force-delete must inspect every matching object");
    assert!(matches!(prefix_error, StorageError::PrefixAccessDenied(_, _)));

    for object in ["batch.bin", "delete-all.bin", "prefix/retained.bin"] {
        ecstore
            .get_object_info(&bucket, object, &ObjectOptions::default())
            .await
            .expect("retained object must survive every rejected delete shape");
    }
}

#[tokio::test]
#[serial]
async fn batch_delete_can_purge_an_explicit_delete_marker_version() {
    let ecstore = shared_gating_ecstore().await;
    let bucket = format!("batch-delete-marker-{}", Uuid::new_v4());
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

    let deleted = ecstore
        .delete_object(
            &bucket,
            "marker.bin",
            ObjectOptions {
                versioned: true,
                ..Default::default()
            },
        )
        .await
        .expect("create a delete marker");
    let marker_version = deleted.version_id.expect("delete marker should have a version id");

    let (_deleted, errors) = ecstore
        .delete_objects(
            &bucket,
            vec![ObjectToDelete {
                object_name: "marker.bin".to_string(),
                version_id: Some(marker_version),
                ..Default::default()
            }],
            ObjectOptions {
                versioned: true,
                ..Default::default()
            },
        )
        .await;
    assert!(errors[0].is_none(), "explicit delete-marker purge must succeed: {:?}", errors[0]);
}

#[tokio::test]
#[serial]
async fn delete_all_exact_object_preserves_a_retained_child_key() {
    let ecstore = shared_gating_ecstore().await;
    let bucket = format!("delete-all-child-{}", Uuid::new_v4());
    ecstore
        .make_bucket(&bucket, &MakeBucketOptions::default())
        .await
        .expect("create plain bucket fixture");

    let mut parent_reader = PutObjReader::from_vec(b"parent".to_vec());
    ecstore
        .put_object(&bucket, "foo", &mut parent_reader, &ObjectOptions::default())
        .await
        .expect("put exact parent object");
    let mut child_reader = PutObjReader::from_vec(b"retained child".to_vec());
    ecstore
        .put_object(
            &bucket,
            "foo/bar",
            &mut child_reader,
            &ObjectOptions {
                user_defined: compliance_retention_metadata(),
                ..Default::default()
            },
        )
        .await
        .expect("put retained child object");

    ecstore
        .delete_object(
            &bucket,
            "foo",
            ObjectOptions {
                delete_prefix: true,
                delete_prefix_object: true,
                ..Default::default()
            },
        )
        .await
        .expect("delete-all should remove only the exact object");

    ecstore
        .get_object_info(&bucket, "foo", &ObjectOptions::default())
        .await
        .expect_err("exact parent should be deleted");
    ecstore
        .get_object_info(&bucket, "foo/bar", &ObjectOptions::default())
        .await
        .expect("retained child key must survive exact delete-all");
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
