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

use super::super::test_support::*;
use super::*;
use crate::app::storage_api::multipart_usecase::access::{AuthorizationRequest, load_bucket_generation_from_store};
use crate::app::storage_api::multipart_usecase::helper::MutationCompletionProbe;
use crate::app::storage_api::multipart_usecase::request_context::RequestContext;
use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, DeleteBucketOptions, MakeBucketOptions};
use crate::runtime_sources::ServerContextSlot;
use rustfs_scanner::{SegmentInvalidationProducerIdentity, set_scanner_dirty_usage_mutation_observer};
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
async fn execute_abort_multipart_upload_returns_internal_error_when_store_uninitialized() {
    let request = build_request((), Method::DELETE);
    let envelope = RequestEnvelope::from_s3s(&request);
    let before = s3_op_total(S3Operation::AbortMultipartUpload);
    let target = AbortMultipartUploadTarget::Legacy {
        bucket: "bucket".to_owned(),
        key: "object".to_owned(),
        upload_id: "upload-id".to_owned(),
    };

    let err = make_usecase()
        .execute_abort_multipart_upload(envelope, Ok(target))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &S3ErrorCode::InternalError);
    assert_eq!(err.message(), Some("Not init"));
    assert_eq!(s3_op_total(S3Operation::AbortMultipartUpload), before);
}

#[test]
#[serial_test::serial]
fn native_abort_requires_current_generation_and_audits_each_result() {
    crate::app::gating_test_env::run_large_stack_test("abort-multipart-generation", || async {
        let context = crate::app::gating_test_env::shared_gating_ambient().await;
        let store = context.object_store();
        let usecase = DefaultMultipartUsecase::with_context(Some(context.clone()));
        let slot = ServerContextSlot::new();
        assert!(slot.install(context));
        let bucket = format!("abort-multipart-{}", Uuid::new_v4().simple());
        let other = format!("abort-multipart-{}", Uuid::new_v4().simple());
        let key = "object";
        let opts = ObjectOptions::default();
        let request_context = RequestContext::fallback();
        let request = || {
            let mut req = build_request((), Method::DELETE);
            req.uri = format!("/{bucket}/{key}").parse().expect("fixture URI");
            req.extensions.insert(slot.clone());
            req.extensions.insert(request_context.clone());
            req
        };
        for name in [&bucket, &other] {
            store
                .make_bucket(name, &MakeBucketOptions::default())
                .await
                .expect("create bucket");
        }
        let original = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("previous bucket incarnation");
        let mut stale = request();
        let captured = load_bucket_generation_from_store(&store, &stale, &bucket)
            .await
            .expect("capture prior bucket");
        stale.install_bucket_generation(captured);
        store
            .delete_bucket(&bucket, &DeleteBucketOptions::default())
            .await
            .expect("delete original bucket");
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("recreate bucket");
        assert_ne!(
            store
                .bucket_incarnation_id(&bucket)
                .await
                .expect("current bucket incarnation"),
            original
        );

        let upload = store
            .new_multipart_upload(&bucket, key, &opts)
            .await
            .expect("create current upload");
        let mut reader = PutObjReader::from_vec(b"current part".to_vec());
        store
            .put_object_part(&bucket, key, &upload.upload_id, 1, &mut reader, &opts)
            .await
            .expect("stage current part");
        let target = || AbortMultipartUploadTarget::Gateway {
            bucket: BucketName::new(bucket.clone()).expect("bucket name"),
            key: ObjectKey::new(key).expect("object key"),
            upload_id: UploadIdClaim::from_wire(upload.upload_id.as_str()),
        };

        let dirty = Arc::new(AtomicUsize::new(0));
        let observer = {
            let dirty = dirty.clone();
            let bucket = bucket.clone();
            Arc::new(move |observed_bucket: &str, observed_key: &str, producer| {
                if observed_bucket == bucket
                    && observed_key == key
                    && producer == SegmentInvalidationProducerIdentity::AbortMultipartUpload
                {
                    dirty.fetch_add(1, Ordering::Relaxed);
                }
            }) as rustfs_scanner::ScannerDirtyUsageMutationObserver
        };
        struct RestoreObserver(Option<rustfs_scanner::ScannerDirtyUsageMutationObserver>);
        impl Drop for RestoreObserver {
            fn drop(&mut self) {
                set_scanner_dirty_usage_mutation_observer(self.0.take());
            }
        }
        let _observer = RestoreObserver(set_scanner_dirty_usage_mutation_observer(Some(observer)));
        let probe = MutationCompletionProbe::install(&bucket);

        let missing = request();
        let mut wrong = request();
        let captured = load_bucket_generation_from_store(&store, &wrong, &other)
            .await
            .expect("capture unrelated bucket");
        wrong.install_bucket_generation(captured);
        for (req, code) in [
            (stale, S3ErrorCode::NoSuchBucket),
            (missing, S3ErrorCode::InternalError),
            (wrong, S3ErrorCode::InternalError),
        ] {
            let err = usecase
                .execute_abort_multipart_upload(RequestEnvelope::from_s3s(&req), Ok(target()))
                .await
                .expect_err("Abort requires current generation");
            assert_eq!(err.code(), &code);
            let parts = store
                .list_object_parts(&bucket, key, &upload.upload_id, None, 10, &opts)
                .await
                .expect("current upload persists");
            assert_eq!(parts.parts.len(), 1, "current staged part persists");
        }
        assert_eq!(dirty.load(Ordering::Relaxed), 0);
        assert_eq!(probe.audits.lock().unwrap().len(), 3);

        let mut current = request();
        let captured = load_bucket_generation_from_store(&store, &current, &bucket)
            .await
            .expect("capture current bucket");
        current.install_bucket_generation(captured);
        usecase
            .execute_abort_multipart_upload(RequestEnvelope::from_s3s(&current), Ok(target()))
            .await
            .expect("current generation allows Abort");
        assert!(matches!(
            store.get_multipart_info(&bucket, key, &upload.upload_id, &opts).await,
            Err(StorageError::InvalidUploadID(..))
        ));
        assert_eq!(dirty.load(Ordering::Relaxed), 1);
        {
            let audits = probe.audits.lock().unwrap();
            assert_eq!(audits.len(), 4);
            assert_eq!(audits.last().expect("successful audit").api.status_code, Some(200));
        }

        let legacy = store
            .new_multipart_upload(&bucket, key, &opts)
            .await
            .expect("create legacy-path upload");
        usecase
            .execute_abort_multipart_upload(
                RequestEnvelope::from_s3s(&current),
                Ok(AbortMultipartUploadTarget::Legacy {
                    bucket: bucket.clone(),
                    key: key.to_owned(),
                    upload_id: legacy.upload_id,
                }),
            )
            .await
            .expect("legacy target with the same authorized generation");
        assert_eq!(dirty.load(Ordering::Relaxed), 2);

        let audits = probe.audits.lock().unwrap();
        assert_eq!(audits.len(), 5, "one audit per owned Abort");
        assert_eq!(audits.last().expect("legacy success audit").api.status_code, Some(200));
        for audit in audits.iter() {
            assert_eq!(audit.request_id.as_deref(), Some(request_context.request_id.as_str()));
            assert_eq!(audit.api.name.as_deref(), Some(S3Operation::AbortMultipartUpload.as_str()));
        }
        assert!(probe.events.lock().unwrap().is_empty());
    });
}
