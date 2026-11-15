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

use super::*;
use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};
use rustfs_gateway_types::{BucketName, ObjectKey, UploadIdClaim};
use std::cell::Cell;

#[derive(Clone, Copy, Debug)]
enum LookupResult {
    Real,
    ForeignBucket,
    ForeignKey,
    Outage,
}

tokio::task_local! {
    static LOOKUP: (Cell<usize>, LookupResult);
}

pub(in crate::app::multipart_usecase::upload_part) fn observe_lookup(
    result: Result<MultipartInfo, StorageError>,
) -> Result<MultipartInfo, StorageError> {
    let Ok(mode) = LOOKUP.try_with(|(calls, mode)| {
        calls.set(calls.get() + 1);
        *mode
    }) else {
        return result;
    };
    let mut info = result?;
    match mode {
        LookupResult::Real => {}
        LookupResult::ForeignBucket => info.bucket = "foreign-bucket".to_owned(),
        LookupResult::ForeignKey => info.object = "foreign-key".to_owned(),
        LookupResult::Outage => return Err(StorageError::ErasureReadQuorum),
    }
    Ok(info)
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
        let envelope = RequestEnvelope::from_s3s(&build_request((), Method::PUT));
        LOOKUP
            .scope((Cell::new(0), mode), async {
                let result = usecase.execute_upload_part_owned(envelope, input).await;
                assert_eq!(
                    result.as_ref().err().map(|err| err.code().as_str()),
                    expected,
                    "part {number}, lookup {mode:?}"
                );
                assert_eq!(LOOKUP.with(|(observed, _)| observed.get()), calls, "part {number}, lookup {mode:?}");
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
