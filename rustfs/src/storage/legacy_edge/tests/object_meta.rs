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
