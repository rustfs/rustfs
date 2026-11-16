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

//! Tests of the `object_meta` legacy-edge handlers (batch B1), moved unchanged from `ecfs.rs` and
//! `ecfs_test.rs`.

use super::{build_request, store_uninitialized_premise_holds};
use crate::storage::ecfs::*;
use http::Method;

#[tokio::test]
async fn test_get_object_acl_returns_internal_error_when_store_uninitialized() {
    if !store_uninitialized_premise_holds() {
        return;
    }
    let input = GetObjectAclInput::builder()
        .bucket("test-bucket".to_string())
        .key("test-key".to_string())
        .build()
        .unwrap();

    let fs = FS::new();
    let err = fs.get_object_acl(build_request(input, Method::GET)).await.unwrap_err();
    assert_eq!(err.code(), &S3ErrorCode::InternalError);
}

#[tokio::test]
async fn test_get_object_legal_hold_returns_internal_error_when_store_uninitialized() {
    if !store_uninitialized_premise_holds() {
        return;
    }
    let input = GetObjectLegalHoldInput::builder()
        .bucket("test-bucket".to_string())
        .key("test-key".to_string())
        .build()
        .unwrap();

    let fs = FS::new();
    let err = fs.get_object_legal_hold(build_request(input, Method::GET)).await.unwrap_err();
    assert_eq!(err.code(), &S3ErrorCode::InternalError);
}

#[tokio::test]
async fn test_get_object_retention_returns_internal_error_when_store_uninitialized() {
    let input = GetObjectRetentionInput::builder()
        .bucket("test-bucket".to_string())
        .key("test-key".to_string())
        .build()
        .unwrap();

    let fs = FS::new();
    let err = fs.get_object_retention(build_request(input, Method::GET)).await.unwrap_err();
    assert_eq!(err.code(), &S3ErrorCode::InternalError);
}

#[tokio::test]
async fn test_put_object_legal_hold_returns_internal_error_when_store_uninitialized() {
    if !store_uninitialized_premise_holds() {
        return;
    }
    let input = PutObjectLegalHoldInput::builder()
        .bucket("test-bucket".to_string())
        .key("test-key".to_string())
        .build()
        .unwrap();

    let fs = FS::new();
    let err = fs.put_object_legal_hold(build_request(input, Method::PUT)).await.unwrap_err();
    assert_eq!(err.code(), &S3ErrorCode::InternalError);
}

#[tokio::test]
async fn test_put_object_acl_returns_internal_error_when_store_uninitialized() {
    if !store_uninitialized_premise_holds() {
        return;
    }
    let input = PutObjectAclInput::builder()
        .bucket("test-bucket".to_string())
        .key("test-key".to_string())
        .build()
        .unwrap();

    let fs = FS::new();
    let err = fs.put_object_acl(build_request(input, Method::PUT)).await.unwrap_err();
    assert_eq!(err.code(), &S3ErrorCode::InternalError);
}

#[tokio::test]
async fn test_put_object_retention_returns_internal_error_when_store_uninitialized() {
    let input = PutObjectRetentionInput::builder()
        .bucket("test-bucket".to_string())
        .key("test-key".to_string())
        .build()
        .unwrap();

    let fs = FS::new();
    let err = fs.put_object_retention(build_request(input, Method::PUT)).await.unwrap_err();
    assert_eq!(err.code(), &S3ErrorCode::InternalError);
}

#[tokio::test]
async fn test_get_object_tagging_returns_internal_error_when_store_uninitialized() {
    if !store_uninitialized_premise_holds() {
        return;
    }
    let input = GetObjectTaggingInput::builder()
        .bucket("test-bucket".to_string())
        .key("test-key".to_string())
        .build()
        .unwrap();

    let fs = FS::new();
    let err = fs.get_object_tagging(build_request(input, Method::GET)).await.unwrap_err();
    assert_eq!(err.code(), &S3ErrorCode::InternalError);
}

#[tokio::test]
async fn test_put_object_tagging_rejects_too_many_tags() {
    let tag_set = (0..11)
        .map(|index| Tag {
            key: Some(format!("k{index}")),
            value: Some(format!("v{index}")),
        })
        .collect();
    let input = PutObjectTaggingInput::builder()
        .bucket("test-bucket".to_string())
        .key("test-key".to_string())
        .tagging(Tagging { tag_set })
        .build()
        .unwrap();

    let fs = FS::new();
    let err = fs.put_object_tagging(build_request(input, Method::PUT)).await.unwrap_err();
    assert_eq!(err.code(), &S3ErrorCode::InvalidTag);
    assert!(err.to_string().contains("Cannot have more than 10 tags per object"));
}

#[tokio::test]
async fn test_put_object_tagging_rejects_empty_tag_key_before_store_lookup() {
    let input = PutObjectTaggingInput::builder()
        .bucket("test-bucket".to_string())
        .key("test-key".to_string())
        .tagging(Tagging {
            tag_set: vec![Tag {
                key: Some(String::new()),
                value: Some("v1".to_string()),
            }],
        })
        .build()
        .unwrap();

    let fs = FS::new();
    let err = fs.put_object_tagging(build_request(input, Method::PUT)).await.unwrap_err();
    assert_eq!(err.code(), &S3ErrorCode::InvalidTag);
    assert!(err.to_string().contains("Tag key cannot be empty"));
}

#[tokio::test]
async fn test_put_object_tagging_preserves_missing_tag_fields_before_version_validation() {
    let fs = FS::new();
    for (key, value, message) in [
        (None, Some("value"), "Tag key cannot be empty"),
        (Some("key"), None, "Tag value cannot be null"),
    ] {
        let input = PutObjectTaggingInput::builder()
            .bucket("test-bucket".to_owned())
            .key("test-key".to_owned())
            .version_id(Some("invalid".to_owned()))
            .tagging(Tagging {
                tag_set: vec![Tag {
                    key: key.map(str::to_owned),
                    value: value.map(str::to_owned),
                }],
            })
            .build()
            .unwrap();
        let error = fs.put_object_tagging(build_request(input, Method::PUT)).await.unwrap_err();
        assert_eq!(error.code(), &S3ErrorCode::InvalidTag);
        assert!(error.to_string().contains(message), "{error}");
    }
}

#[tokio::test]
async fn test_put_object_tagging_returns_internal_error_when_store_uninitialized() {
    if !store_uninitialized_premise_holds() {
        return;
    }
    let input = PutObjectTaggingInput::builder()
        .bucket("test-bucket".to_string())
        .key("test-key".to_string())
        .tagging(Tagging {
            tag_set: vec![Tag {
                key: Some("k".to_string()),
                value: Some("v".to_string()),
            }],
        })
        .build()
        .unwrap();

    let fs = FS::new();
    let err = fs.put_object_tagging(build_request(input, Method::PUT)).await.unwrap_err();
    assert_eq!(err.code(), &S3ErrorCode::InternalError);
}

#[tokio::test]
async fn test_delete_object_tagging_returns_internal_error_when_store_uninitialized() {
    if !store_uninitialized_premise_holds() {
        return;
    }
    let input = DeleteObjectTaggingInput::builder()
        .bucket("test-bucket".to_string())
        .key("test-key".to_string())
        .build()
        .unwrap();

    let fs = FS::new();
    let err = fs
        .delete_object_tagging(build_request(input, Method::DELETE))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &S3ErrorCode::InternalError);
}

#[test]
#[serial_test::serial]
fn put_object_tagging_completes_once_for_committed_metadata() {
    use crate::app::{RequestEnvelope, gating_test_env, object_meta::ObjectMetaUsecase};
    use crate::storage::helper::MutationCompletionProbe;
    use crate::storage::storage_api::StoragePutObjReader;
    use crate::storage::storage_api::contract::bucket::{BucketOperations, MakeBucketOptions};
    use crate::storage::storage_api::contract::object::ObjectIO;
    use rustfs_scanner::{SegmentInvalidationProducerIdentity, set_scanner_dirty_usage_mutation_observer};
    use std::sync::{Arc, Mutex};

    gating_test_env::run_large_stack_test("put-tagging-completion", || async {
        let context = gating_test_env::shared_gating_ambient().await;
        let store = context.object_store();
        let usecase = ObjectMetaUsecase::with_context(Some(context));
        let bucket = format!("put-tagging-{}", uuid::Uuid::new_v4().simple());
        store.make_bucket(&bucket, &MakeBucketOptions::default()).await.unwrap();
        let mut body = StoragePutObjReader::from_vec(b"preserved".to_vec());
        store
            .put_object(&bucket, "present", &mut body, &Default::default())
            .await
            .unwrap();

        let probe = MutationCompletionProbe::install(&bucket);
        let dirty = Arc::new(Mutex::new(Vec::new()));
        let observe = {
            let dirty = Arc::clone(&dirty);
            let bucket = bucket.clone();
            Arc::new(move |observed_bucket: &str, key: &str, producer| {
                if observed_bucket == bucket && producer == SegmentInvalidationProducerIdentity::ObjectMetadata {
                    dirty.lock().unwrap().push(key.to_owned());
                }
            })
        };
        struct RestoreObserver(Option<rustfs_scanner::ScannerDirtyUsageMutationObserver>);
        impl Drop for RestoreObserver {
            fn drop(&mut self) {
                set_scanner_dirty_usage_mutation_observer(self.0.take());
            }
        }
        let _observer = RestoreObserver(set_scanner_dirty_usage_mutation_observer(Some(observe)));

        let put = |key: &'static str| {
            let mut req = build_request((), Method::PUT);
            req.uri = format!("/{bucket}/{key}?tagging").parse().unwrap();
            usecase.put_object_tagging_fields(
                RequestEnvelope::from_s3s(&req),
                &bucket,
                key,
                None,
                vec![rustfs_gateway_types::dto::Tag {
                    key: Some("project".into()),
                    value: Some("approved".into()),
                }],
            )
        };
        let missing = put("missing").await.unwrap_err();
        assert_eq!(missing.code(), &rustfs_s3_types::S3ErrorCode::NoSuchKey);
        assert!(probe.events.lock().unwrap().is_empty(), "missing object emitted a tagging event");
        assert!(dirty.lock().unwrap().is_empty(), "missing object marked usage dirty");
        put("present").await.unwrap();
        let events = probe.events.lock().unwrap();
        assert_eq!(events.len(), 1, "one committed tagging event");
        assert_eq!(events[0].event_name, rustfs_s3_types::EventName::ObjectTaggingPut);
        assert_eq!(events[0].object.name, "present");
        assert_eq!(*dirty.lock().unwrap(), ["present"], "one committed dirty marker");
    });
}

#[derive(Clone, Copy)]
enum LockWrite {
    LegalHold,
    Retention,
}

impl LockWrite {
    fn subresource(self) -> &'static str {
        match self {
            Self::LegalHold => "legal-hold",
            Self::Retention => "retention",
        }
    }

    fn operation(self) -> rustfs_s3_ops::S3Operation {
        match self {
            Self::LegalHold => rustfs_s3_ops::S3Operation::PutObjectLegalHold,
            Self::Retention => rustfs_s3_ops::S3Operation::PutObjectRetention,
        }
    }

    async fn authorize(self, envelope: &mut crate::app::RequestEnvelope, fs: &FS, bucket: &str, version: Option<&str>) {
        use crate::storage::access::access_owned::object_meta::{
            RequestedObjectLegalHold, authorize_put_object_legal_hold, authorize_put_object_retention,
        };
        match self {
            Self::LegalHold => authorize_put_object_legal_hold(
                envelope,
                fs.server_ctx(),
                bucket,
                "object",
                version,
                RequestedObjectLegalHold(Some("ON".into())),
            )
            .await
            .unwrap(),
            Self::Retention => {
                authorize_put_object_retention(envelope, fs.server_ctx(), bucket, "object", version, Self::requested_retention())
                    .await
                    .unwrap()
            }
        }
    }

    fn requested_retention() -> crate::storage::access::RequestedObjectLockRetention {
        crate::storage::access::RequestedObjectLockRetention {
            mode: Some("GOVERNANCE".into()),
            retain_until: Some(time::macros::datetime!(2099-01-02 00:00:00 UTC)),
        }
    }

    async fn execute(
        self,
        usecase: &crate::app::object_meta::ObjectMetaUsecase,
        envelope: crate::app::RequestEnvelope,
        bucket: &str,
        version: Option<String>,
    ) -> rustfs_s3_types::S3Result<()> {
        use crate::storage::access::RequestedObjectLegalHold;
        match self {
            Self::LegalHold => usecase
                .put_object_legal_hold_fields(
                    envelope,
                    bucket,
                    "object",
                    version,
                    Some(RequestedObjectLegalHold(Some("ON".into()))),
                )
                .await
                .map(drop),
            Self::Retention => usecase
                .put_object_retention_fields(envelope, bucket, "object", version, Some(Self::requested_retention()))
                .await
                .map(drop),
        }
    }
}

fn lock_write_rejects_bucket_replacement(write: LockWrite) {
    use crate::app::{RequestEnvelope, gating_test_env, object_meta::ObjectMetaUsecase};
    use crate::storage::access::ReqInfo;
    use crate::storage::helper::MutationCompletionProbe;
    use crate::storage::storage_api::contract::{
        bucket::{BucketOperations, DeleteBucketOptions, MakeBucketOptions},
        object::{ObjectIO, ObjectOperations},
    };
    use crate::storage::storage_api::{StorageObjectOptions, StoragePutObjReader};
    use rustfs_scanner::{SegmentInvalidationProducerIdentity, set_scanner_dirty_usage_mutation_observer};
    use std::sync::{Arc, Mutex};

    gating_test_env::run_large_stack_test("object-lock-replacement", move || async move {
        let context = gating_test_env::shared_gating_ambient().await;
        let (paths, store) = gating_test_env::shared_gating_ecstore_and_disk_paths().await;
        let usecase = ObjectMetaUsecase::with_context(Some(Arc::clone(&context)));
        let bucket = format!("lock-write-{}", uuid::Uuid::new_v4().simple());
        let make_object = |body: &'static [u8]| {
            let (store, bucket) = (store.clone(), bucket.clone());
            async move {
                store
                    .make_bucket(
                        &bucket,
                        &MakeBucketOptions {
                            lock_enabled: true,
                            versioning_enabled: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
                store
                    .put_object(
                        &bucket,
                        "object",
                        &mut StoragePutObjReader::from_vec(body.to_vec()),
                        &StorageObjectOptions {
                            versioned: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap()
                    .version_id
                    .unwrap()
                    .to_string()
            }
        };
        let original = make_object(b"original object").await;
        let slot = crate::runtime_sources::ServerContextSlot::new();
        assert!(slot.install(context));
        let fs = FS::with_server_ctx(slot);
        let envelope = |version: Option<&str>| {
            let mut request = build_request((), Method::PUT);
            request.extensions.insert(ReqInfo {
                cred: Some(rustfs_credentials::Credentials::default()),
                is_owner: true,
                ..Default::default()
            });
            request.extensions.insert(fs.server_ctx().clone());
            let query = version.map_or_else(String::new, |value| format!("&versionId={value}"));
            request.uri = format!("/{bucket}/object?{}{query}", write.subresource()).parse().unwrap();
            RequestEnvelope::from_s3s(&request)
        };
        let mut stale = envelope(None);
        write.authorize(&mut stale, &fs, &bucket, None).await;
        store
            .delete_object(
                &bucket,
                "object",
                StorageObjectOptions {
                    version_id: Some(original.clone()),
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        store.delete_bucket(&bucket, &DeleteBucketOptions::default()).await.unwrap();
        let replacement = make_object(b"replacement object").await;
        assert_ne!(replacement, original);
        let xl_meta: Vec<_> = paths.iter().map(|base| base.join(&bucket).join("object/xl.meta")).collect();
        let disk_bytes = || xl_meta.iter().map(|path| std::fs::read(path).unwrap()).collect::<Vec<_>>();
        let before = disk_bytes();

        let probe = MutationCompletionProbe::install(&bucket);
        let dirty = Arc::new(Mutex::new(Vec::new()));
        let observe = {
            let dirty = Arc::clone(&dirty);
            let bucket = bucket.clone();
            Arc::new(move |observed_bucket: &str, key: &str, producer| {
                if observed_bucket == bucket && producer == SegmentInvalidationProducerIdentity::ObjectMetadata {
                    dirty.lock().unwrap().push(key.to_owned());
                }
            })
        };
        struct RestoreObserver(Option<rustfs_scanner::ScannerDirtyUsageMutationObserver>);
        impl Drop for RestoreObserver {
            fn drop(&mut self) {
                set_scanner_dirty_usage_mutation_observer(self.0.take());
            }
        }
        let _observer = RestoreObserver(set_scanner_dirty_usage_mutation_observer(Some(observe)));

        let refusal = write.execute(&usecase, stale, &bucket, None).await;
        let unchanged = disk_bytes() == before;
        assert!(
            refusal.is_err() && unchanged,
            "stale write {refusal:?}, replacement bytes unchanged: {unchanged}"
        );
        assert!(dirty.lock().unwrap().is_empty(), "refusal dirtied scanner");
        assert!(probe.events.lock().unwrap().is_empty(), "refusal emitted an event");

        let mut fresh = envelope(Some(&replacement));
        write.authorize(&mut fresh, &fs, &bucket, Some(&replacement)).await;
        write
            .execute(&usecase, fresh, &bucket, Some(replacement.clone()))
            .await
            .unwrap();
        assert!(disk_bytes().iter().zip(&before).all(|(after, before)| after != before));
        assert_eq!(*dirty.lock().unwrap(), ["object"], "committed write marks scanner once");
        assert!(probe.events.lock().unwrap().is_empty(), "lock metadata write emitted a bucket event");
        let audits = probe.audits.lock().unwrap();
        assert_eq!(audits.len(), 2, "one audit per admitted attempt");
        assert_eq!(
            audits[0].api.status_code,
            Some(500),
            "early write refusal uses the helper's fallback audit"
        );
        assert_eq!(audits[1].api.name.as_deref(), Some(write.operation().as_str()));
        assert_eq!(audits[1].api.status_code, Some(200), "successful metadata write completes audit");
        assert_eq!(audits[1].req_path.as_deref(), Some(format!("/{bucket}/object").as_str()));
        assert_eq!(audits[1].api.object.as_deref(), Some("object"));
    });
}

#[test]
#[serial_test::serial]
fn put_object_legal_hold_fences_replacement_and_completes_once() {
    lock_write_rejects_bucket_replacement(LockWrite::LegalHold);
}

#[test]
#[serial_test::serial]
fn put_object_retention_fences_replacement_and_completes_once() {
    lock_write_rejects_bucket_replacement(LockWrite::Retention);
}

#[test]
#[serial_test::serial]
fn put_object_retention_rechecks_a_competing_extension_at_commit() {
    use crate::app::storage_api::test::set_disk::{PutObjectCommitBarrier, PutObjectCommitPause};
    use crate::app::{RequestEnvelope, gating_test_env, object_meta::ObjectMetaUsecase};
    use crate::storage::storage_api::contract::{
        bucket::{BucketOperations, MakeBucketOptions},
        object::{ObjectIO, ObjectOperations},
    };
    use crate::storage::storage_api::{StorageObjectOptions, StoragePutObjReader};

    gating_test_env::run_large_stack_test("object-lock-retention-commit", || async {
        let context = gating_test_env::shared_gating_ambient().await;
        let store = context.object_store();
        let usecase = ObjectMetaUsecase::with_context(Some(context));
        let bucket = format!("lock-race-{}", uuid::Uuid::new_v4().simple());
        store
            .make_bucket(
                &bucket,
                &MakeBucketOptions {
                    lock_enabled: true,
                    versioning_enabled: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let put = |options: StorageObjectOptions| {
            let (store, bucket) = (store.clone(), bucket.clone());
            async move {
                store
                    .put_object(
                        &bucket,
                        "object",
                        &mut StoragePutObjReader::from_vec(b"unchanged body".to_vec()),
                        &options,
                    )
                    .await
                    .unwrap()
            }
        };
        let first = put(StorageObjectOptions {
            versioned: true,
            ..Default::default()
        })
        .await;
        let version = first.version_id.unwrap().to_string();
        let barrier = PutObjectCommitBarrier::install(&bucket, "object", PutObjectCommitPause::BeforeMetadata);
        let mut request = build_request((), Method::PUT);
        request.uri = format!("/{bucket}/object?retention&versionId={version}").parse().unwrap();
        let envelope = RequestEnvelope::from_s3s(&request);
        let attempted_bucket = bucket.clone();
        let attempted_version = version.clone();
        let short = tokio::spawn(async move {
            LockWrite::Retention
                .execute(&usecase, envelope, &attempted_bucket, Some(attempted_version))
                .await
        });
        barrier.wait_until_paused().await;
        // Commit a longer retention through the storage writer for this same version while
        // the metadata request is past its optimistic check and has yet to take the write lock.
        const LONGER: &str = "2099-01-03T00:00:00Z";
        put(StorageObjectOptions {
            versioned: true,
            version_id: Some(version.clone()),
            user_defined: [
                ("x-amz-object-lock-mode".into(), "GOVERNANCE".into()),
                ("x-amz-object-lock-retain-until-date".into(), LONGER.into()),
            ]
            .into(),
            ..Default::default()
        })
        .await;
        barrier.release();
        let refusal = short.await.unwrap();
        let disk = store
            .get_object_info(
                &bucket,
                "object",
                &StorageObjectOptions {
                    versioned: true,
                    version_id: Some(version),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let retained = disk
            .user_defined
            .get("x-amz-object-lock-retain-until-date")
            .map(String::as_str);
        assert!(
            refusal
                .as_ref()
                .is_err_and(|error| error.code() == &rustfs_s3_types::S3ErrorCode::AccessDenied)
                && retained == Some(LONGER),
            "late check {refusal:?}, current cutoff {retained:?}"
        );
    });
}
