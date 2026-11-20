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

//! Bucket-generation snapshots and early refusals of the owned object authorization functions.
//!
//! Responsible for: the destination and copy-source bucket generations an authorization captures
//! and keeps across a recreation of the same bucket name, the use case refusing to act on the
//! recreated bucket, the fail-closed refusal of a request without its generation guard or whose
//! policy metadata is unavailable, the write-offset refusal ahead of authorization, and the
//! request facts DeleteObjects leaves for its use case.
//! Not responsible for: tag- or condition-dependent decisions (see `copy_tags.rs`), or the policy
//! evaluation itself.
//! Upstream: the owned request fixture in `storage::authorization::tests::fixture`.
//! Downstream: `storage::access_owned::{object_write, object_misc, object_commit, listing_multipart}`,
//! the generation guards in `storage::authorization`, and the Delete, Restore, Copy and
//! UploadPartCopy use cases.

use crate::app::multipart_usecase::{DefaultMultipartUsecase, UploadPartCopyWriteInput, UploadPartTarget};
use crate::app::object_usecase::{CopyObjectWriteInput, DefaultObjectUsecase};
use crate::storage::access_owned::listing_multipart::authorize_abort_multipart_upload;
use crate::storage::access_owned::object_commit::{
    CopyObjectAuthorization, CopySourceObject, authorize_complete_multipart_upload, authorize_copy_object,
    authorize_upload_part_copy,
};
use crate::storage::access_owned::object_misc::{authorize_delete_object, authorize_delete_objects, authorize_restore_object};
use crate::storage::access_owned::object_write::{PutObjectAuthorization, authorize_put_object};
use crate::storage::authorization::tests::fixture::{TestKms, UnreadyIam, request, server_context};
use crate::storage::authorization::{
    apply_bucket_generation_guard, apply_copy_source_bucket_generation_guard, existing_object_tag_conditions,
    has_write_offset_bytes_header, install_object_generation_test_hook,
};
use crate::storage::req_info::ReqInfo;
use crate::storage::storage_api::contract::bucket::{BucketOperations as _, DeleteBucketOptions, MakeBucketOptions};
use crate::storage::storage_api::contract::multipart::MultipartOperations as _;
use crate::storage::storage_api::contract::object::{ObjectIO as _, ObjectOperations as _};
use crate::storage::storage_api::runtime_sources_consumer::{AppContext, ServerContextSlot};
use http::{HeaderMap, Method};
use rustfs_gateway_types::CopySourceValue;
use rustfs_s3_types::S3ErrorCode;
use serial_test::serial;
use std::sync::Arc;

/// The copy use case input the legacy edge built from a CopyObject request that names only a
/// destination and a bucket-path source.
fn copy_write_input(bucket: &str, key: &str, copy_source: CopySourceValue) -> CopyObjectWriteInput {
    CopyObjectWriteInput {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        copy_source: Some(copy_source),
        version_id: None,
        specific_if_none_match: false,
        server_side_encryption: None,
        ssekms_key_id: None,
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
        copy_source_sse_customer_algorithm: None,
        copy_source_sse_customer_key: None,
        copy_source_sse_customer_key_md5: None,
        metadata_directive: None,
        metadata: None,
        tagging: None,
        tagging_directive: None,
        copy_source_if_match: None,
        copy_source_if_none_match: None,
        cache_control: None,
        content_disposition: None,
        content_encoding: None,
        content_language: None,
        content_type: None,
        expires: None,
        website_redirect_location: None,
        object_lock_legal_hold_status: None,
        object_lock_mode: None,
        object_lock_retain_until_date: None,
        storage_class: None,
        checksum_algorithm: None,
    }
}

/// CopyObject's authorization facts for a request naming only a destination and an unversioned
/// bucket-path source.
fn copy_facts<'a>(bucket: &'a str, key: &'a str, source_bucket: &'a str, source_key: &'a str) -> CopyObjectAuthorization<'a> {
    CopyObjectAuthorization {
        bucket,
        key,
        source: Some(CopySourceObject {
            bucket: source_bucket,
            key: source_key,
            version_id: None,
        }),
        query_version: None,
        tagging: None,
        tagging_directive: None,
        retain_until: None,
        legal_hold: false,
        retention: false,
    }
}

/// PutObject's authorization facts for a request naming only a bucket and key.
fn put_facts<'a>(bucket: &'a str, key: &'a str) -> PutObjectAuthorization<'a> {
    PutObjectAuthorization {
        bucket,
        key,
        version_id: None,
        retain_until: None,
        specific_etag: false,
        legal_hold: false,
        retention: false,
        post: false,
    }
}

#[tokio::test]
#[serial]
async fn put_object_access_captures_authorized_bucket_incarnation() {
    let store = crate::app::gating_test_env::shared_gating_ecstore().await;
    let slot = server_context(Arc::clone(&store));

    let bucket = "generation-guard-collision";
    store
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("test bucket should be created");
    let policy_json = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"*"}},"Action":["s3:PutObject"],"Resource":["arn:aws:s3:::{bucket}/*"]}}]}}"#
    );
    let mut metadata = (*crate::storage::get_bucket_metadata(bucket)
        .await
        .expect("new bucket metadata should be cached"))
    .clone();
    metadata.policy_config = Some(serde_json::from_str(&policy_json).expect("test bucket policy should parse"));
    metadata.policy_config_json = policy_json.into_bytes();
    crate::storage::storage_api::set_bucket_metadata(bucket.to_string(), metadata)
        .await
        .expect("test bucket policy should be published");

    let mut envelope = request(Method::PUT).req_info().server_context(&slot).build();
    authorize_put_object(&mut envelope, &slot, put_facts(bucket, bucket), |_| Ok(()))
        .await
        .expect("anonymous PutObject should be authorized by the test policy");
    let mut opts = crate::storage::ObjectOptions::default();
    apply_bucket_generation_guard(&envelope, bucket, &mut opts).expect("request snapshot should apply to PutObject options");
    assert_eq!(
        opts.expected_bucket_incarnation_id,
        Some(
            store
                .bucket_incarnation_id(bucket)
                .await
                .expect("bucket incarnation should remain readable")
        )
    );
}

#[test]
#[serial]
fn delete_object_access_keeps_snapshot_across_recreation_before_admission() {
    crate::app::gating_test_env::run_large_stack_test(
        "delete-object-generation-guard",
        delete_object_access_keeps_snapshot_across_recreation_before_admission_inner,
    );
}

async fn delete_object_access_keeps_snapshot_across_recreation_before_admission_inner() {
    let store = crate::app::gating_test_env::shared_gating_ecstore().await;
    let slot = ServerContextSlot::new();
    let app_context = Arc::new(AppContext::new(Arc::clone(&store), Arc::new(UnreadyIam), Arc::new(TestKms)));
    assert!(slot.install(Arc::clone(&app_context)));

    let bucket = format!("delete-generation-guard-{}", uuid::Uuid::new_v4());
    store
        .make_bucket(&bucket, &MakeBucketOptions::default())
        .await
        .expect("test bucket should be created");
    assert!(
        existing_object_tag_conditions(store.as_ref(), &bucket, "missing-object", None)
            .await
            .expect("a missing object must be treated as having no policy tags")
            .is_empty()
    );
    assert!(
        existing_object_tag_conditions(store.as_ref(), &format!("missing-{bucket}"), "missing-object", None)
            .await
            .expect("a missing bucket must be indistinguishable during pre-authorization tag lookup")
            .is_empty()
    );
    let policy_json = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"*"}},"Action":["s3:DeleteObject"],"Resource":["arn:aws:s3:::{bucket}/*"]}}]}}"#
    );
    let mut metadata = (*crate::storage::get_bucket_metadata(&bucket)
        .await
        .expect("new bucket metadata should be cached"))
    .clone();
    metadata.policy_config = Some(serde_json::from_str(&policy_json).expect("test bucket policy should parse"));
    metadata.policy_config_json = policy_json.clone().into_bytes();
    crate::storage::storage_api::set_bucket_metadata(bucket.clone(), metadata)
        .await
        .expect("test bucket policy should be published");

    let mut envelope = request(Method::DELETE).req_info().server_context(&slot).build();

    let captured_incarnation_id = store
        .bucket_incarnation_id(&bucket)
        .await
        .expect("read the bucket generation before admission");
    let (loaded_tx, loaded_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
    install_object_generation_test_hook(bucket.clone(), loaded_tx, resume_rx);
    let access = {
        let slot = slot.clone();
        let bucket = bucket.clone();
        tokio::spawn(async move {
            let result = authorize_delete_object(&slot, &mut envelope, &bucket, "object", None).await;
            (result, envelope)
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(10), loaded_rx)
        .await
        .expect("DeleteObject should reach the generation snapshot hook")
        .expect("DeleteObject access should remain paused before admission");

    store
        .delete_bucket(&bucket, &DeleteBucketOptions::default())
        .await
        .expect("delete the authorized bucket generation");
    store
        .make_bucket(&bucket, &MakeBucketOptions::default())
        .await
        .expect("recreate the same bucket name");
    let mut metadata = (*crate::storage::get_bucket_metadata(&bucket)
        .await
        .expect("recreated bucket metadata should be cached"))
    .clone();
    metadata.policy_config = Some(serde_json::from_str(&policy_json).expect("test policy should parse"));
    metadata.policy_config_json = policy_json.into_bytes();
    crate::storage::storage_api::set_bucket_metadata(bucket.clone(), metadata)
        .await
        .expect("republish the same policy for admission");
    let mut reader = crate::storage::PutObjReader::from_vec(b"new generation".to_vec());
    store
        .put_object(&bucket, "object", &mut reader, &crate::storage::ObjectOptions::default())
        .await
        .expect("put the new-generation object");

    assert_ne!(
        captured_incarnation_id,
        store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("read the recreated bucket generation")
    );
    resume_tx
        .send(())
        .expect("DeleteObject should still be paused before admission");
    let (result, envelope) = access.await.expect("DeleteObject access task should join");
    result.expect("the equivalent policy should authorize DeleteObject");
    let mut opts = crate::storage::ObjectOptions::default();
    apply_bucket_generation_guard(&envelope, &bucket, &mut opts).expect("request snapshot should apply to DeleteObject options");
    assert_eq!(opts.expected_bucket_incarnation_id, Some(captured_incarnation_id));

    let err = Box::pin(DefaultObjectUsecase::with_context(Some(app_context)).execute_delete_object_fields(
        envelope,
        bucket.clone(),
        "object".to_string(),
        None,
    ))
    .await
    .expect_err("the old authorization must not delete from the recreated bucket");
    assert_eq!(err.code(), &S3ErrorCode::NoSuchBucket);
    store
        .get_object_info(&bucket, "object", &crate::storage::ObjectOptions::default())
        .await
        .expect("the new-generation object must survive the stale request");
}

#[tokio::test]
#[serial]
async fn copy_operations_reject_recreated_source_bucket_after_authorization() {
    let store = crate::app::gating_test_env::shared_gating_ecstore().await;
    let slot = ServerContextSlot::new();
    let app_context = Arc::new(AppContext::new(Arc::clone(&store), Arc::new(UnreadyIam), Arc::new(TestKms)));
    assert!(slot.install(Arc::clone(&app_context)));

    let suffix = uuid::Uuid::new_v4();
    let src_bucket = format!("copy-src-generation-{suffix}");
    let dst_bucket = format!("copy-dst-generation-{suffix}");
    for bucket in [&src_bucket, &dst_bucket] {
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("create copy test bucket");
    }

    let src_policy_json = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"*"}},"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::{src_bucket}/*"]}}]}}"#
    );
    let mut src_metadata = (*crate::storage::get_bucket_metadata(&src_bucket)
        .await
        .expect("source bucket metadata should be cached"))
    .clone();
    src_metadata.policy_config = Some(serde_json::from_str(&src_policy_json).expect("source policy should parse"));
    src_metadata.policy_config_json = src_policy_json.into_bytes();
    crate::storage::storage_api::set_bucket_metadata(src_bucket.clone(), src_metadata)
        .await
        .expect("publish source policy");

    let dst_policy_json = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"*"}},"Action":["s3:PutObject"],"Resource":["arn:aws:s3:::{dst_bucket}/*"]}}]}}"#
    );
    let mut dst_metadata = (*crate::storage::get_bucket_metadata(&dst_bucket)
        .await
        .expect("destination bucket metadata should be cached"))
    .clone();
    dst_metadata.policy_config = Some(serde_json::from_str(&dst_policy_json).expect("destination policy should parse"));
    dst_metadata.policy_config_json = dst_policy_json.into_bytes();
    crate::storage::storage_api::set_bucket_metadata(dst_bucket.clone(), dst_metadata)
        .await
        .expect("publish destination policy");

    let mut envelope = request(Method::PUT).req_info().server_context(&slot).build();
    authorize_copy_object(&mut envelope, &slot, copy_facts(&dst_bucket, "copied", &src_bucket, "secret"))
        .await
        .expect("test policies should authorize the copy request");
    let mut source_opts = crate::storage::ObjectOptions::default();
    apply_copy_source_bucket_generation_guard(&envelope, &src_bucket, &mut source_opts)
        .expect("authorized source generation should be attached");
    let authorized_source_incarnation_id = source_opts
        .expected_bucket_incarnation_id
        .expect("source incarnation should be captured");

    let upload = store
        .new_multipart_upload(&dst_bucket, "multipart-copy", &crate::storage::ObjectOptions::default())
        .await
        .expect("create destination multipart upload");
    let upload_id = upload.upload_id;
    let mut upload_envelope = request(Method::PUT).req_info().server_context(&slot).build();
    authorize_upload_part_copy(
        &mut upload_envelope,
        &slot,
        &dst_bucket,
        "multipart-copy",
        Some(CopySourceObject {
            bucket: &src_bucket,
            key: "secret",
            version_id: None,
        }),
    )
    .await
    .expect("test policies should authorize the part copy request");

    store
        .delete_bucket(&src_bucket, &DeleteBucketOptions::default())
        .await
        .expect("delete the authorized source generation");
    store
        .make_bucket(&src_bucket, &MakeBucketOptions::default())
        .await
        .expect("recreate the source bucket name");
    assert_ne!(
        authorized_source_incarnation_id,
        store
            .bucket_incarnation_id(&src_bucket)
            .await
            .expect("read recreated source incarnation")
    );
    let mut reader = crate::storage::PutObjReader::from_vec(b"new generation secret".to_vec());
    store
        .put_object(&src_bucket, "secret", &mut reader, &crate::storage::ObjectOptions::default())
        .await
        .expect("write the new-generation source object");

    let source = || CopySourceValue::Bucket {
        bucket: src_bucket.clone(),
        key: "secret".to_owned(),
        version_id: None,
    };
    let err = DefaultObjectUsecase::with_context(Some(Arc::clone(&app_context)))
        .execute_copy_object_owned(envelope, copy_write_input(&dst_bucket, "copied", source()))
        .await
        .expect_err("stale authorization must not read from the recreated source bucket");
    assert_eq!(err.code(), &S3ErrorCode::NoSuchBucket);
    let upload_input = UploadPartCopyWriteInput {
        target: UploadPartTarget::Legacy {
            bucket: dst_bucket.clone(),
            key: "multipart-copy".to_owned(),
            upload_id,
        },
        part_number: 1,
        copy_source: source(),
        copy_source_range: None,
        copy_source_if_match: None,
        copy_source_if_none_match: None,
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
        copy_source_sse_customer_algorithm: None,
        copy_source_sse_customer_key: None,
        copy_source_sse_customer_key_md5: None,
    };
    let err = DefaultMultipartUsecase::with_context(Some(app_context))
        .execute_upload_part_copy_owned(upload_envelope, upload_input)
        .await
        .err()
        .expect("stale authorization must not read the recreated source into a part");
    assert_eq!(err.code(), &S3ErrorCode::NoSuchBucket);
    store
        .get_object_info(&dst_bucket, "copied", &crate::storage::ObjectOptions::default())
        .await
        .expect_err("the new-generation source object must not be copied");
}

#[tokio::test]
#[serial]
async fn copy_object_rejects_recreated_destination_bucket_after_authorization() {
    let store = crate::app::gating_test_env::shared_gating_ecstore().await;
    let slot = ServerContextSlot::new();
    let context = Arc::new(AppContext::new(store.clone(), Arc::new(UnreadyIam), Arc::new(TestKms)));
    assert!(slot.install(context.clone()));
    let src = format!("copy-src-{}", uuid::Uuid::new_v4());
    let dst = format!("copy-dst-{}", uuid::Uuid::new_v4());
    let policy = |bucket: &str, action: &str| {
        serde_json::to_vec(&serde_json::json!({"Version":"2012-10-17","Statement":[{
            "Effect":"Allow","Principal":{"AWS":"*"},"Action":action,"Resource":format!("arn:aws:s3:::{bucket}/*")
        }]}))
        .unwrap()
    };
    for (bucket, action) in [(&src, "s3:GetObject"), (&dst, "s3:PutObject")] {
        store.make_bucket(bucket, &MakeBucketOptions::default()).await.unwrap();
        store
            .update_bucket_metadata_config(
                bucket,
                crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
                policy(bucket, action),
            )
            .await
            .unwrap();
    }
    let opts = crate::storage::ObjectOptions::default();
    let mut body = crate::storage::PutObjReader::from_vec(b"source contents".to_vec());
    store.put_object(&src, "source", &mut body, &opts).await.unwrap();
    let mut envelope = request(Method::PUT).req_info().server_context(&slot).build();
    let captured_incarnation = store.bucket_incarnation_id(&dst).await.unwrap();
    let (loaded_tx, loaded_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
    install_object_generation_test_hook(dst.clone(), loaded_tx, resume_rx);
    let access = {
        let (slot, src, dst) = (slot.clone(), src.clone(), dst.clone());
        tokio::spawn(async move {
            let result = authorize_copy_object(&mut envelope, &slot, copy_facts(&dst, "copied", &src, "source")).await;
            (result, envelope)
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(10), loaded_rx)
        .await
        .expect("destination generation capture must be reached")
        .expect("copy access ended before capture");
    store.delete_bucket(&dst, &DeleteBucketOptions::default()).await.unwrap();
    store.make_bucket(&dst, &MakeBucketOptions::default()).await.unwrap();
    store
        .update_bucket_metadata_config(
            &dst,
            crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
            policy(&dst, "s3:PutObject"),
        )
        .await
        .unwrap();
    assert_ne!(captured_incarnation, store.bucket_incarnation_id(&dst).await.unwrap());
    resume_tx.send(()).expect("resume copy before admission");
    let (result, envelope) = access.await.unwrap();
    result.expect("the equivalent policy should admit the copy");
    let mut captured = crate::storage::ObjectOptions::default();
    apply_bucket_generation_guard(&envelope, &dst, &mut captured).expect("copy carries the destination snapshot");
    assert_eq!(captured.expected_bucket_incarnation_id, Some(captured_incarnation));

    let input = copy_write_input(
        &dst,
        "copied",
        CopySourceValue::Bucket {
            bucket: src.clone(),
            key: "source".to_owned(),
            version_id: None,
        },
    );
    let error = DefaultObjectUsecase::with_context(Some(context))
        .execute_copy_object_owned(envelope, input)
        .await
        .expect_err("must refuse before publishing to the recreated bucket");
    assert_eq!(error.code(), &S3ErrorCode::NoSuchBucket);
    let missing = store
        .get_object_info(&dst, "copied", &opts)
        .await
        .expect_err("stale copy wrote an object");
    assert!(crate::storage::is_err_object_not_found(&missing), "{missing}");
}

#[test]
#[serial]
fn restore_object_access_keeps_snapshot_across_recreation_before_admission() {
    crate::app::gating_test_env::run_large_stack_test(
        "restore-object-generation-guard",
        restore_object_access_keeps_snapshot_across_recreation_before_admission_inner,
    );
}

async fn restore_object_access_keeps_snapshot_across_recreation_before_admission_inner() {
    let store = crate::app::gating_test_env::shared_gating_ecstore().await;
    let slot = ServerContextSlot::new();
    let app_context = Arc::new(AppContext::new(Arc::clone(&store), Arc::new(UnreadyIam), Arc::new(TestKms)));
    assert!(slot.install(Arc::clone(&app_context)));

    let bucket = format!("restore-generation-guard-{}", uuid::Uuid::new_v4());
    store
        .make_bucket(&bucket, &MakeBucketOptions::default())
        .await
        .expect("create the bucket generation being authorized");
    let authorized_incarnation_id = store
        .bucket_incarnation_id(&bucket)
        .await
        .expect("read the authorized bucket incarnation");
    let policy_json = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"*"}},"Action":["s3:RestoreObject"],"Resource":["arn:aws:s3:::{bucket}/*"]}}]}}"#
    );
    let mut metadata = (*crate::storage::get_bucket_metadata(&bucket)
        .await
        .expect("authorized bucket metadata should be cached"))
    .clone();
    metadata.policy_config = Some(serde_json::from_str(&policy_json).expect("test policy should parse"));
    metadata.policy_config_json = policy_json.clone().into_bytes();
    crate::storage::storage_api::set_bucket_metadata(bucket.clone(), metadata)
        .await
        .expect("publish the RestoreObject policy");

    let mut envelope = request(Method::POST).req_info().server_context(&slot).build();
    let (authorized_tx, authorized_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
    install_object_generation_test_hook(bucket.clone(), authorized_tx, resume_rx);

    let access = {
        let slot = slot.clone();
        let bucket = bucket.clone();
        tokio::spawn(async move {
            let result = authorize_restore_object(&slot, &mut envelope, &bucket, "object", None).await;
            (result, envelope)
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(10), authorized_rx)
        .await
        .expect("RestoreObject should reach the generation snapshot hook")
        .expect("RestoreObject access must not fail before reaching the test hook");

    store
        .delete_bucket(&bucket, &DeleteBucketOptions::default())
        .await
        .expect("delete the authorized bucket generation");
    store
        .make_bucket(&bucket, &MakeBucketOptions::default())
        .await
        .expect("recreate the bucket with the same name");
    let mut metadata = (*crate::storage::get_bucket_metadata(&bucket)
        .await
        .expect("recreated bucket metadata should be cached"))
    .clone();
    metadata.policy_config = Some(serde_json::from_str(&policy_json).expect("test policy should parse"));
    metadata.policy_config_json = policy_json.into_bytes();
    crate::storage::storage_api::set_bucket_metadata(bucket.clone(), metadata)
        .await
        .expect("republish the same policy for admission");
    let recreated_incarnation_id = store
        .bucket_incarnation_id(&bucket)
        .await
        .expect("read the recreated bucket incarnation");
    assert_ne!(authorized_incarnation_id, recreated_incarnation_id);
    let replacement = b"recreated bucket object";
    store
        .put_object(
            &bucket,
            "object",
            &mut crate::storage::PutObjReader::from_vec(replacement.to_vec()),
            &crate::storage::ObjectOptions::default(),
        )
        .await
        .expect("write the recreated bucket's object");
    resume_tx
        .send(())
        .expect("RestoreObject access should still be waiting at the test hook");

    let (result, envelope) = access.await.expect("RestoreObject access task should join");
    result.expect("the equivalent policy should authorize RestoreObject without refreshing its snapshot");
    let mut opts = crate::storage::ObjectOptions::default();
    apply_bucket_generation_guard(&envelope, &bucket, &mut opts).expect("apply the RestoreObject authorization guard");

    // The RestoreObject usecase future is large enough that heap-pinning a
    // single future was not enough on Linux: the whole scenario runs on a
    // dedicated large stack (see the `#[test]` wrapper above) so unrelated
    // growth in bucket metadata futures cannot tip the test over.
    let restore_request = crate::app::storage_api::object_usecase::bucket::RestoreRequest {
        days: Some(1),
        description: None,
        glacier_job_parameters: None,
        output_location: None,
        select_parameters: None,
        tier: None,
        type_: None,
    };
    let result = Box::pin(DefaultObjectUsecase::with_context(Some(app_context)).execute_restore_object_owned(
        envelope,
        bucket.clone(),
        "object".to_string(),
        None,
        Some(restore_request),
    ))
    .await;
    use tokio::io::AsyncReadExt as _;
    let mut reader = store
        .get_object_reader(&bucket, "object", None, HeaderMap::new(), &crate::storage::ObjectOptions::default())
        .await
        .expect("read the recreated bucket's object");
    let mut bytes = Vec::new();
    reader.stream.read_to_end(&mut bytes).await.expect("read replacement bytes");
    assert_eq!(
        (
            opts.expected_bucket_incarnation_id,
            result.as_ref().err().map(|error| error.code()),
            bytes.as_slice(),
            reader.object_info.restore_ongoing
        ),
        (
            Some(authorized_incarnation_id),
            Some(&S3ErrorCode::NoSuchBucket),
            replacement.as_slice(),
            false
        ),
        "stale RestoreObject must refuse and preserve the recreated bucket's object"
    );
    drop(reader);
    store
        .delete_bucket(
            &bucket,
            &DeleteBucketOptions {
                force: true,
                ..Default::default()
            },
        )
        .await
        .expect("clean up the recreated bucket");
}

#[test]
fn dispatched_put_object_fails_closed_without_generation_guard() {
    let envelope = request(Method::PUT).server_context(&ServerContextSlot::new()).build();

    let mut opts = crate::storage::ObjectOptions::default();
    let err = apply_bucket_generation_guard(&envelope, "generation-guard-bucket", &mut opts)
        .expect_err("a dispatched PutObject must not commit without its authorization fence");
    assert_eq!(err.code(), &S3ErrorCode::InternalError);
}

#[test]
fn write_offset_bytes_header_detection_is_case_insensitive() {
    let mut headers = HeaderMap::new();
    headers.insert("X-Amz-Write-Offset-Bytes", http::HeaderValue::from_static("0"));

    assert!(has_write_offset_bytes_header(&headers));
}

#[tokio::test]
async fn put_object_rejects_write_offset_bytes_before_authorize_request() {
    let mut envelope = request(Method::PUT)
        .uri("/test-bucket/test-key")
        .header("x-amz-write-offset-bytes", "0")
        .extension(ReqInfo {
            cred: Some(rustfs_credentials::Credentials::default()),
            ..ReqInfo::default()
        })
        .build();

    let err = authorize_put_object(&mut envelope, &ServerContextSlot::new(), put_facts("test-bucket", "test-key"), |_| Ok(()))
        .await
        .expect_err("write-offset-bytes requests should be rejected before authorization");

    assert_eq!(err.code(), &S3ErrorCode::NotImplemented);
    // The message `ApiError::error_code_to_message` gives `NotImplemented`.
    assert_eq!(err.message(), Some("A header you provided implies functionality that is not implemented"));

    let req_info = envelope.req_info().expect("req info should remain available");
    assert_eq!(req_info.bucket.as_deref(), Some("test-bucket"));
    assert_eq!(req_info.object.as_deref(), Some("test-key"));
    assert_eq!(req_info.version_id, None);
}

#[tokio::test]
async fn delete_objects_defers_object_authorization_to_usecase() {
    let mut envelope = request(Method::POST)
        .extension(ReqInfo {
            cred: Some(rustfs_credentials::Credentials::default()),
            bucket: Some("previous-bucket".to_string()),
            object: Some("previous-key".to_string()),
            version_id: Some("previous-version".to_string()),
            ..ReqInfo::default()
        })
        .build();

    authorize_delete_objects(&mut envelope, "test-bucket")
        .expect("DeleteObjects access hook should not require bucket-level DeleteObject");

    let req_info = envelope.req_info().expect("req info should remain available");
    assert_eq!(req_info.bucket.as_deref(), Some("test-bucket"));
    assert_eq!(req_info.object, None);
    assert_eq!(req_info.version_id, None);
}

#[tokio::test]
async fn abort_multipart_upload_fails_closed_when_policy_metadata_is_unavailable() {
    let mut envelope = request(Method::DELETE).req_info().build();

    let err = authorize_abort_multipart_upload(&ServerContextSlot::new(), &mut envelope, "bucket", "object")
        .await
        .expect_err("unavailable policy metadata must fail closed");
    assert_eq!(err.code(), &S3ErrorCode::InternalError);
}

#[tokio::test]
async fn complete_multipart_upload_fails_closed_when_policy_metadata_is_unavailable() {
    let mut envelope = request(Method::POST).req_info().build();

    let err = authorize_complete_multipart_upload(&mut envelope, &ServerContextSlot::new(), "bucket", "object", false)
        .await
        .expect_err("unavailable policy metadata must fail closed");
    assert_eq!(err.code(), &S3ErrorCode::InternalError);
}

#[tokio::test]
async fn upload_part_copy_fails_closed_when_policy_metadata_is_unavailable() {
    let mut envelope = request(Method::PUT).req_info().build();

    let source = CopySourceObject {
        bucket: "src-bucket",
        key: "src-object",
        version_id: None,
    };
    let err = authorize_upload_part_copy(&mut envelope, &ServerContextSlot::new(), "dst-bucket", "dst-object", Some(source))
        .await
        .expect_err("unavailable policy metadata must fail closed");
    assert_eq!(err.code(), &S3ErrorCode::InternalError);
}
