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
