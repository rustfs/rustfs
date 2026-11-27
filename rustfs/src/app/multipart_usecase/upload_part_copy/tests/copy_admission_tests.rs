// Copyright 2026 RustFS Team
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

use super::*;
use crate::app::storage_api::test::contract::bucket::{BucketOperations, MakeBucketOptions};
use crate::app::storage_api::test::contract::object::{ObjectIO, ObjectOperations};
use rustfs_s3_types::S3ErrorCode;

#[test]
#[serial_test::serial]
fn upload_part_copy_shares_foreground_admission_with_upload_part() {
    crate::app::gating_test_env::run_large_stack_test("copy-part-admission", || async {
        struct RestoreGetMetrics(bool);
        impl Drop for RestoreGetMetrics {
            fn drop(&mut self) {
                rustfs_io_metrics::set_get_stage_metrics_enabled(self.0);
            }
        }
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        let ambient = crate::app::gating_test_env::shared_gating_ambient().await;
        let context = Arc::new(AppContext::new(Arc::clone(&store), ambient.iam(), ambient.kms()));
        let bucket = format!("copy-part-admission-{}", Uuid::new_v4().simple());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create bucket");
        let bytes = vec![0x5a; 2 * 1024 * 1024];
        store
            .put_object(&bucket, "source", &mut PutObjReader::from_vec(bytes.clone()), &ObjectOptions::default())
            .await
            .expect("write copy source");
        let upload = store
            .new_multipart_upload(&bucket, "object", &ObjectOptions::default())
            .await
            .expect("create session");
        let manager = Arc::new(ConcurrencyManager::with_large_put_admission_for_test(true, 1, 1, Duration::ZERO));
        let held = manager.admit_multipart_part(1024).await.expect("hold UploadPart permit");
        assert!(matches!(held, ForegroundWriteAdmission::Admitted(_)));
        let usecase = DefaultMultipartUsecase::with_context_and_concurrency_manager(Some(context), Arc::clone(&manager));
        let input = || {
            copy_part_input(
                &bucket,
                "object",
                &upload.upload_id,
                1,
                CopySourceValue::Bucket {
                    bucket: bucket.clone(),
                    key: "source".into(),
                    version_id: None,
                },
            )
        };
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _recorder = metrics::set_default_local_recorder(&recorder);
        let _restore = RestoreGetMetrics(rustfs_io_metrics::get_stage_metrics_enabled());
        rustfs_io_metrics::set_get_stage_metrics_enabled(true);
        let source_size_bucket = rustfs_io_metrics::get_object_size_bucket(i64::try_from(bytes.len()).expect("source size"));
        let source_reader_observed = || {
            snapshotter.snapshot().into_vec().iter().any(|(key, _, _, _)| {
                key.key().name() == "rustfs_io_get_object_reader_path_by_size_total"
                    && key
                        .key()
                        .labels()
                        .any(|label| label.key() == "size_bucket" && label.value() == source_size_bucket)
            })
        };
        let error = match Box::pin(usecase.execute_upload_part_copy_with_audit(owned_envelope(Method::PUT), input())).await {
            Err(error) => error,
            Ok(_) => panic!("copy must not bypass the occupied UploadPart pool"),
        };
        assert_eq!(error.code(), &S3ErrorCode::SlowDown);
        assert!(!source_reader_observed(), "saturated copy must not construct a source body reader");
        let parts = store
            .list_object_parts(&bucket, "object", &upload.upload_id, None, 1000, &ObjectOptions::default())
            .await
            .expect("list parts");
        assert!(parts.parts.is_empty(), "rejected copy must not create a destination part");
        drop(held);
        let copied = match Box::pin(usecase.execute_upload_part_copy_with_audit(owned_envelope(Method::PUT), input())).await {
            Ok(output) => output,
            Err(error) => panic!("retry copy after releasing UploadPart permit: {error:?}"),
        };
        assert!(source_reader_observed());
        assert_eq!(
            manager.put_object_admission_snapshot().active,
            Some(0),
            "success releases the copy permit"
        );
        let etag = copied.e_tag.expect("copy ETag");
        Arc::clone(&store)
            .complete_multipart_upload(
                &bucket,
                "object",
                &upload.upload_id,
                vec![CompletePart {
                    part_num: 1,
                    etag: Some(etag),
                    ..Default::default()
                }],
                &ObjectOptions::default(),
            )
            .await
            .expect("commit copied object");
        let mut reader = store
            .get_object_reader(&bucket, "object", None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("read committed copy");
        let mut actual = Vec::new();
        reader.stream.read_to_end(&mut actual).await.expect("read all copied bytes");
        assert_eq!(actual, bytes);
        drop(reader);
        let marker = store
            .delete_object(
                &bucket,
                "source",
                ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("create source delete marker");
        assert!(marker.delete_marker);
        let marker_version = marker.version_id.expect("marker version").to_string();
        let deleted_upload = store
            .new_multipart_upload(&bucket, "deleted-copy", &ObjectOptions::default())
            .await
            .expect("deleted-source destination session");
        let _held = manager
            .admit_multipart_part(1024)
            .await
            .expect("saturate gate for deleted source");
        for (version_id, expected) in [
            (None, S3ErrorCode::NoSuchKey),
            (Some(marker_version), S3ErrorCode::MethodNotAllowed),
        ] {
            let input = UploadPartCopyWriteInput {
                copy_source_if_match: Some(CopySourceCondition::Strong("wrong-etag".to_owned())),
                ..copy_part_input(
                    &bucket,
                    "deleted-copy",
                    &deleted_upload.upload_id,
                    1,
                    CopySourceValue::Bucket {
                        bucket: bucket.clone(),
                        key: "source".into(),
                        version_id,
                    },
                )
            };
            let error = match Box::pin(usecase.execute_upload_part_copy_with_audit(owned_envelope(Method::PUT), input)).await {
                Err(error) => error,
                Ok(_) => panic!("deleted source must fail before conditions or admission"),
            };
            assert_eq!(error.code(), &expected);
        }
    });
}

#[test]
#[serial_test::serial]
fn upload_part_copy_range_error_and_cancellation_release_foreground_permit() {
    crate::app::gating_test_env::run_large_stack_test("copy-part-range-admission", || async {
        use rustfs_utils::http::SUFFIX_MAX_TOTAL_OBJECT_SIZE;
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        let ambient = crate::app::gating_test_env::shared_gating_ambient().await;
        let context = Arc::new(AppContext::new(Arc::clone(&store), ambient.iam(), ambient.kms()));
        let bucket = format!("copy-part-range-{}", Uuid::new_v4().simple());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create range bucket");
        let bytes = (0..2 * 1024 * 1024)
            .map(|i| u8::try_from(i % 251).expect("byte pattern"))
            .collect::<Vec<_>>();
        store
            .put_object(&bucket, "source", &mut PutObjReader::from_vec(bytes.clone()), &ObjectOptions::default())
            .await
            .expect("write non-inline source");
        let manager = Arc::new(ConcurrencyManager::with_large_put_admission_for_test(true, 1, 1, Duration::ZERO));
        let usecase = DefaultMultipartUsecase::with_context_and_concurrency_manager(Some(context), Arc::clone(&manager));
        let cancelled_upload = store
            .new_multipart_upload(&bucket, "cancelled", &ObjectOptions::default())
            .await
            .expect("cancelled destination session");
        let cancelled_input = copy_part_input(
            &bucket,
            "cancelled",
            &cancelled_upload.upload_id,
            1,
            CopySourceValue::Bucket {
                bucket: bucket.clone(),
                key: "source".into(),
                version_id: None,
            },
        );
        let mut cancelled_copy =
            Box::pin(usecase.execute_upload_part_copy_with_audit(owned_envelope(Method::PUT), cancelled_input));
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                biased;
                () = async {
                    while manager.put_object_admission_snapshot().active != Some(1) {
                        tokio::task::yield_now().await;
                    }
                } => {}
                result = &mut cancelled_copy => match result {
                    Ok(_) => panic!("copy completed before cancellation"),
                    Err(error) => panic!("copy completed before cancellation: {error:?}"),
                },
            }
        })
        .await
        .expect("copy acquires its permit");
        drop(cancelled_copy);
        assert_eq!(manager.put_object_admission_snapshot().active, Some(0));
        tokio::time::timeout(
            Duration::from_secs(10),
            store.put_object(&bucket, "source", &mut PutObjReader::from_vec(bytes.clone()), &ObjectOptions::default()),
        )
        .await
        .expect("cancelled source reader releases its namespace lock")
        .expect("overwrite source after cancellation");
        store
            .abort_multipart_upload(&bucket, "cancelled", &cancelled_upload.upload_id, &ObjectOptions::default())
            .await
            .expect("abort cancelled session");
        for capped in [true, false] {
            let mut opts = ObjectOptions::default();
            if capped {
                insert_str(&mut opts.user_defined, SUFFIX_MAX_TOTAL_OBJECT_SIZE, "1024".to_owned());
            }
            let upload = store
                .new_multipart_upload(&bucket, "object", &opts)
                .await
                .expect("range destination session");
            let copy_input = || {
                copy_part_input(
                    &bucket,
                    "object",
                    &upload.upload_id,
                    1,
                    CopySourceValue::Bucket {
                        bucket: bucket.clone(),
                        key: "source".into(),
                        version_id: None,
                    },
                )
            };
            let mut input = copy_input();
            let envelope = if capped {
                input.copy_source_range = Some("bytes=12345-1060920".to_owned());
                owned_envelope(Method::PUT)
            } else {
                let presigned = |properties: &str| {
                    let uri: http::Uri =
                        format!("/?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test&X-Amz-Signature=test&{properties}")
                            .parse()
                            .expect("copy query");
                    let mut headers = HeaderMap::new();
                    crate::auth::hoist_signed_query_params(&mut headers, uri.query()).expect("hoist signed copy properties");
                    RequestEnvelope::from_parts(
                        crate::app::RequestHead {
                            method: Method::PUT,
                            uri,
                            headers,
                            credentials: None,
                            region: None,
                            service: None,
                        },
                        Extensions::new(),
                    )
                };
                for condition in [
                    "x-amz-copy-source-if-match=%22stale%22",
                    "x-amz-copy-source-if-unmodified-since=1970-01-01T00%3A00%3A00Z",
                ] {
                    let response =
                        Box::pin(usecase.execute_upload_part_copy_with_audit(presigned(condition), copy_input())).await;
                    assert_eq!(
                        response.err().expect("stale copy source must be rejected").code(),
                        &S3ErrorCode::PreconditionFailed
                    );
                }
                presigned("x-amz-copy-source-range=bytes%3D12345-1060920")
            };
            let response = Box::pin(usecase.execute_upload_part_copy_with_audit(envelope, input)).await;
            assert_eq!(
                manager.put_object_admission_snapshot().active,
                Some(0),
                "copy releases its permit on every outcome"
            );
            if capped {
                let error = match response {
                    Err(error) => error,
                    Ok(_) => panic!("range exceeds destination size limit"),
                };
                assert_eq!(error.code(), &S3ErrorCode::EntityTooLarge);
                let parts = store
                    .list_object_parts(&bucket, "object", &upload.upload_id, None, 1000, &ObjectOptions::default())
                    .await
                    .expect("failed copy parts");
                assert!(parts.parts.is_empty());
                store
                    .abort_multipart_upload(&bucket, "object", &upload.upload_id, &ObjectOptions::default())
                    .await
                    .expect("abort capped session");
            } else {
                let copied = match response {
                    Ok(output) => output,
                    Err(error) => panic!("range copy after failed request released its source and permit: {error:?}"),
                };
                let etag = copied.e_tag.expect("range ETag");
                Arc::clone(&store)
                    .complete_multipart_upload(
                        &bucket,
                        "object",
                        &upload.upload_id,
                        vec![CompletePart {
                            part_num: 1,
                            etag: Some(etag),
                            ..Default::default()
                        }],
                        &ObjectOptions::default(),
                    )
                    .await
                    .expect("commit range copy");
                let mut reader = store
                    .get_object_reader(&bucket, "object", None, HeaderMap::new(), &ObjectOptions::default())
                    .await
                    .expect("open range copy");
                let mut actual = Vec::new();
                reader.stream.read_to_end(&mut actual).await.expect("read full range");
                assert_eq!(actual, bytes[12345..=1060920]);
            }
        }
    });
}
