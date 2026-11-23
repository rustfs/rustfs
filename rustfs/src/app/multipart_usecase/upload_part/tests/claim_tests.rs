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

//! Upload target ownership and refusal order against real multipart state.

use super::super::super::test_support::owned_envelope;
use super::*;
use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};
use rustfs_gateway_types::{BucketName, ObjectKey, UploadIdClaim};
use std::cell::Cell;

#[derive(Clone, Copy, Debug)]
enum LookupResult {
    Real,
    ForeignBucket,
    ForeignKey,
    MissingOwner,
    Outage,
}

tokio::task_local! {
    static LOOKUP: (Cell<usize>, LookupResult);
}

pub(in crate::app::multipart_usecase::upload_part) fn observe_lookup(
    result: Result<(MultipartInfo, Option<MultipartOwner>), StorageError>,
) -> Result<(MultipartInfo, Option<MultipartOwner>), StorageError> {
    let Ok(mode) = LOOKUP.try_with(|(calls, mode)| {
        calls.set(calls.get() + 1);
        *mode
    }) else {
        return result;
    };
    let (info, mut owner) = result?;
    match mode {
        LookupResult::Real => {}
        LookupResult::ForeignBucket => owner.as_mut().expect("real owner").bucket = "foreign-bucket".to_owned(),
        LookupResult::ForeignKey => owner.as_mut().expect("real owner").object = "foreign-key".to_owned(),
        LookupResult::MissingOwner => owner = None,
        LookupResult::Outage => return Err(StorageError::ErasureReadQuorum),
    }
    Ok((info, owner))
}

#[tokio::test]
#[serial_test::serial]
async fn native_claim_checks_order_ownership_and_one_active_lookup() {
    let store = crate::app::gating_test_env::shared_gating_ecstore().await;
    let context = crate::app::gating_test_env::shared_gating_ambient().await;
    let usecase = DefaultMultipartUsecase::with_context(Some(context));
    let bucket = format!("part-claim-{}", Uuid::new_v4().simple());
    let key = "encoded + key/";
    let opts = ObjectOptions::default();
    store.make_bucket(&bucket, &MakeBucketOptions::default()).await.unwrap();
    let active = store.new_multipart_upload(&bucket, key, &opts).await.unwrap().upload_id;

    let mut absence_facts = None;
    for (number, id, length, body, mode, calls, expected) in [
        (1, active.as_str(), Some(3), true, LookupResult::Real, 1, None),
        (0, "/impossible", Some(3), true, LookupResult::Real, 0, Some("InvalidArgument")),
        (-1, "/impossible", Some(3), true, LookupResult::Real, 0, Some("InvalidArgument")),
        (10001, "/impossible", Some(3), true, LookupResult::Real, 0, Some("InvalidArgument")),
        (2, "/impossible", Some(3), false, LookupResult::Real, 0, Some("IncompleteBody")),
        (2, active.as_str(), Some(3), false, LookupResult::Real, 0, Some("IncompleteBody")),
        (2, "/impossible", Some(i64::MAX), true, LookupResult::Real, 0, Some("EntityTooLarge")),
        (2, "", Some(3), true, LookupResult::Real, 0, Some("NoSuchUpload")),
        (2, "/impossible", Some(-1), true, LookupResult::Real, 0, Some("NoSuchUpload")),
        (2, "malformed", None, true, LookupResult::Real, 1, Some("NoSuchUpload")),
        (2, "malformed", Some(-1), true, LookupResult::Real, 1, Some("NoSuchUpload")),
        (2, active.as_str(), Some(-1), true, LookupResult::Real, 1, Some("UnexpectedContent")),
        (2, active.as_str(), None, true, LookupResult::Real, 1, Some("MissingContentLength")),
        (2, active.as_str(), Some(3), true, LookupResult::ForeignBucket, 1, Some("NoSuchUpload")),
        (2, active.as_str(), Some(3), true, LookupResult::ForeignKey, 1, Some("NoSuchUpload")),
        (2, active.as_str(), Some(3), true, LookupResult::MissingOwner, 1, Some("NoSuchUpload")),
        (2, active.as_str(), None, true, LookupResult::MissingOwner, 1, Some("NoSuchUpload")),
        (2, active.as_str(), Some(3), true, LookupResult::Outage, 1, Some("ServiceUnavailable")),
        (10000, active.as_str(), Some(3), true, LookupResult::Real, 1, None),
    ] {
        let mut input = UploadPartWriteInput::from_legacy(UploadPartInput::default(), None);
        input.target = UploadPartTarget::Gateway {
            bucket: BucketName::new(bucket.clone()).unwrap(),
            key: ObjectKey::new(key).unwrap(),
            upload_id: UploadIdClaim::from_wire(id),
        };
        input.part_number = number;
        input.content_length = length;
        input.body = body.then(|| {
            if length.is_none() {
                legacy_put_body(StreamingBlob::wrap(futures::stream::pending::<Result<Bytes, std::io::Error>>()))
            } else {
                Body::from(Bytes::from_static(b"abc"))
            }
        });
        let envelope = owned_envelope(Method::PUT);
        LOOKUP
            .scope((Cell::new(0), mode), async {
                let result = usecase.execute_upload_part_with_audit(envelope, input).await;
                assert_eq!(
                    result.as_ref().err().map(|err| err.code().as_str()),
                    expected,
                    "part {number}, lookup {mode:?}"
                );
                assert_eq!(LOOKUP.with(|(observed, _)| observed.get()), calls, "part {number}, lookup {mode:?}");
                if expected == Some("NoSuchUpload") {
                    let error = match &result {
                        Err(error) => error,
                        Ok(_) => panic!("expected missing upload"),
                    };
                    let facts = (error.status_code().map(|status| status.as_u16()), error.message().map(str::to_owned));
                    if let Some(expected) = &absence_facts {
                        assert_eq!(&facts, expected, "part {number}, lookup {mode:?}");
                    } else {
                        absence_facts = Some(facts);
                    }
                }
                if let Ok(output) = result {
                    assert_eq!(output.e_tag.as_deref(), Some("900150983cd24fb0d6963f7d28e17f72"));
                }
            })
            .await;
    }

    let parts = store
        .list_object_parts(&bucket, key, &active, None, 20, &opts)
        .await
        .unwrap()
        .parts;
    let parts: Vec<_> = parts
        .iter()
        .map(|part| (part.part_num, part.size, part.etag.as_deref()))
        .collect();
    let etag = Some("900150983cd24fb0d6963f7d28e17f72");
    assert_eq!(parts, [(1, 3, etag), (10000, 3, etag)]);
}

#[test]
#[serial_test::serial]
fn native_copy_part_rejects_unproved_owner_without_changing_parts() {
    use crate::app::multipart_usecase::UploadPartCopyWriteInput;
    use crate::app::storage_api::test::contract::object::ObjectIO as _;
    use rustfs_gateway_types::CopySourceValue;

    crate::app::gating_test_env::run_large_stack_test("copy-part-claim", || async {
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        let ambient = crate::app::gating_test_env::shared_gating_ambient().await;
        let context = Arc::new(AppContext::new(Arc::clone(&store), ambient.iam(), ambient.kms()));
        let usecase = DefaultMultipartUsecase::with_context(Some(context));
        let bucket = format!("part-copy-claim-{}", Uuid::new_v4().simple());
        let key = "destination";
        let options = ObjectOptions::default();
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create bucket");
        store
            .put_object(&bucket, "source", &mut PutObjReader::from_vec(b"abc".to_vec()), &options)
            .await
            .expect("create source");
        let active = store
            .new_multipart_upload(&bucket, key, &options)
            .await
            .expect("create upload")
            .upload_id;
        let abandoned = store
            .new_multipart_upload(&bucket, key, &options)
            .await
            .expect("create abandoned upload")
            .upload_id;
        store
            .abort_multipart_upload(&bucket, key, &abandoned, &options)
            .await
            .expect("abort abandoned upload");
        let mut absence_facts = None;
        for (number, upload_id, mode, expected) in [
            (1, active.as_str(), LookupResult::Real, None),
            (2, abandoned.as_str(), LookupResult::Real, Some("NoSuchUpload")),
            (2, active.as_str(), LookupResult::ForeignBucket, Some("NoSuchUpload")),
            (2, active.as_str(), LookupResult::ForeignKey, Some("NoSuchUpload")),
            (2, active.as_str(), LookupResult::MissingOwner, Some("NoSuchUpload")),
            (2, active.as_str(), LookupResult::Outage, Some("ServiceUnavailable")),
        ] {
            let input = UploadPartCopyWriteInput {
                target: UploadPartTarget::Gateway {
                    bucket: BucketName::new(bucket.clone()).expect("bucket"),
                    key: ObjectKey::new(key).expect("key"),
                    upload_id: UploadIdClaim::from_wire(upload_id),
                },
                part_number: number,
                copy_source: CopySourceValue::Bucket {
                    bucket: bucket.clone(),
                    key: "source".to_owned(),
                    version_id: None,
                },
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
            let envelope = owned_envelope(Method::PUT);
            LOOKUP
                .scope((Cell::new(0), mode), async {
                    let result = Box::pin(usecase.execute_upload_part_copy_with_audit(envelope, input)).await;
                    assert_eq!(result.as_ref().err().map(|error| error.code().as_str()), expected, "{mode:?}");
                    assert_eq!(LOOKUP.with(|(calls, _)| calls.get()), 1, "{mode:?}");
                    if expected == Some("NoSuchUpload") {
                        let Err(actual) = result else { panic!("expected missing upload") };
                        let facts = (
                            actual.code().as_str().to_owned(),
                            actual.status_code().map(|status| status.as_u16()),
                            actual.message().map(str::to_owned),
                        );
                        if let Some(expected) = &absence_facts {
                            assert_eq!(&facts, expected, "{mode:?}");
                        } else {
                            absence_facts = Some(facts);
                        }
                    } else if matches!(mode, LookupResult::Outage) {
                        let Err(actual) = result else { panic!("expected read quorum failure") };
                        let expected: crate::app::storage_api::s3::S3Error =
                            ApiError::from(StorageError::ErasureReadQuorum).into();
                        assert_eq!(actual.code().as_str(), expected.code().as_str());
                        assert_eq!(actual.status_code(), expected.status_code());
                        assert_eq!(actual.message(), expected.message());
                        assert!(
                            actual
                                .source()
                                .and_then(|cause| cause.downcast_ref::<StorageError>())
                                .is_some()
                        );
                    }
                })
                .await;
            let parts = store
                .list_object_parts(&bucket, key, &active, None, 100, &options)
                .await
                .expect("list upload");
            assert_eq!(parts.parts.len(), 1, "{mode:?}");
            assert_eq!(parts.parts[0].part_num, 1, "{mode:?}");
            assert_eq!(parts.parts[0].size, 3, "{mode:?}");
            assert_eq!(parts.parts[0].etag.as_deref(), Some("900150983cd24fb0d6963f7d28e17f72"), "{mode:?}");
        }
    });
}
