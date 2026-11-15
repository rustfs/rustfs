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

//! ListParts execution against real multipart metadata and the native claim exchange.

use super::super::test_support::{Method, build_request};
use super::*;
use crate::app::gating_test_env::{run_large_stack_test, shared_gating_ambient, shared_gating_ecstore_and_disk_paths};
use crate::app::storage_api::multipart_usecase::access::{AuthorizationRequest, load_bucket_generation_from_store};
use crate::app::storage_api::s3::S3Error as LegacyS3Error;
use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, DeleteBucketOptions, MakeBucketOptions};
use crate::runtime_sources::ServerContextSlot;
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use tracing::instrument::WithSubscriber as _;
use tracing::{Subscriber, span};
use tracing_subscriber::{Layer, layer::Context, layer::SubscriberExt as _};

#[derive(Clone, Default)]
struct UploadLookups(Arc<AtomicUsize>);

impl<S: Subscriber> Layer<S> for UploadLookups {
    fn on_new_span(&self, attrs: &span::Attributes<'_>, _: &span::Id, _: Context<'_, S>) {
        let meta = attrs.metadata();
        if meta
            .file()
            .is_some_and(|file| file.ends_with("crates/ecstore/src/store/mod.rs"))
            && matches!(meta.name(), "list_object_parts" | "get_multipart_info")
        {
            self.0.fetch_add(1, Relaxed);
        }
    }
}

fn gateway_fields(bucket: &str, key: &str, id: &str, marker: Option<&str>, max_parts: Option<i32>) -> ListPartsFields {
    ListPartsFields {
        target: ListPartsTarget::Gateway {
            bucket: BucketName::new(bucket).expect("fixture bucket"),
            key: ObjectKey::new(key).expect("fixture key"),
            upload_id: UploadIdClaim::from_wire(id),
        },
        part_number_marker: marker.map(str::to_owned),
        max_parts,
    }
}

async fn execute_counted(
    usecase: &DefaultMultipartUsecase,
    envelope: RequestEnvelope,
    fields: ListPartsFields,
    expected: usize,
) -> S3Result<ListPartsInfo> {
    let lookups = UploadLookups::default();
    let result = usecase
        .execute_list_parts(envelope, fields)
        .with_subscriber(tracing_subscriber::registry().with(lookups.clone()))
        .await;
    assert_eq!(lookups.0.load(Relaxed), expected, "storage upload lookups");
    result
}

#[test]
#[serial_test::serial]
fn list_parts_resolves_real_owners_and_retains_storage_errors() {
    run_large_stack_test("list-parts-ownership", || async {
        let (paths, store) = shared_gating_ecstore_and_disk_paths().await;
        let usecase = DefaultMultipartUsecase::with_context(Some(shared_gating_ambient().await));
        let bucket = format!("list-parts-{}", Uuid::new_v4().simple());
        let foreign_bucket = format!("list-parts-{}", Uuid::new_v4().simple());
        let key = "fixture/one";
        let foreign_key = "fixture/two";
        let opts = ObjectOptions::default();
        let envelope = || RequestEnvelope::from_s3s(&build_request((), Method::GET));
        for name in [&bucket, &foreign_bucket] {
            store
                .make_bucket(name, &MakeBucketOptions::default())
                .await
                .expect("create a fixture bucket");
        }
        let upload = store
            .new_multipart_upload(&bucket, key, &opts)
            .await
            .expect("create an active session");
        let id = upload.upload_id.as_str();

        for (claim, marker, max, lookups, code) in [
            (id, None, None, 1, None),
            ("", None, None, 0, Some("NoSuchUpload")),
            ("/impossible", None, None, 0, Some("NoSuchUpload")),
            ("malformed", None, None, 1, Some("NoSuchUpload")),
            ("/impossible", Some("-1"), Some(0), 0, Some("InvalidArgument")),
            ("/impossible", None, Some(0), 0, Some("InvalidArgument")),
        ] {
            let result = execute_counted(&usecase, envelope(), gateway_fields(&bucket, key, claim, marker, max), lookups).await;
            assert_eq!(
                result.as_ref().err().map(|err| err.code().as_str()),
                code,
                "marker {marker:?}, max {max:?}"
            );
            if let Ok(page) = result {
                assert!(page.parts.is_empty());
                assert!(page.upload_id == id);
            }
        }

        let typed_bucket = BucketName::new(bucket.clone()).unwrap();
        let typed_key = ObjectKey::new(key).unwrap();
        let claim = UploadIdClaim::from_wire(id);
        for (owner_bucket, owner_key) in [(foreign_bucket.as_str(), key), (bucket.as_str(), foreign_key)] {
            let other = store
                .new_multipart_upload(owner_bucket, owner_key, &opts)
                .await
                .expect("create an independent active session");
            let mut actual_page = store
                .list_object_parts(owner_bucket, owner_key, &other.upload_id, None, 1, &opts)
                .await
                .expect("the independent session returns a real page");
            // Only the durable owner observation is relevant, even if presentation agrees.
            actual_page.bucket.clone_from(&bucket);
            actual_page.object = key.to_owned();
            let refusal = resolve_list_parts(&claim, &typed_bucket, &typed_key, |supplied| {
                assert!(supplied == id, "the callback receives the original upload ID");
                std::future::ready(Ok(actual_page))
            })
            .await
            .expect_err("a foreign recorded owner cannot be returned");
            assert_eq!(refusal.code(), &S3ErrorCode::NoSuchUpload);
        }

        let mut unproved = store
            .list_object_parts(&bucket, key, id, None, 1, &opts)
            .await
            .expect("live session");
        unproved.recorded_owner = None;
        let refusal = resolve_list_parts(&claim, &typed_bucket, &typed_key, |_| std::future::ready(Ok(unproved)))
            .await
            .expect_err("a page without owner proof must be refused");
        assert_eq!(refusal.code(), &S3ErrorCode::NoSuchUpload);

        let staging_name = hex_simd::encode_to_string(Sha256::digest(format!("{bucket}/{key}")), hex_simd::AsciiCase::Lower);
        for disk in &paths[..3] {
            let staging = disk.join(".rustfs.sys/multipart").join(&staging_name);
            let uploads: Vec<_> = std::fs::read_dir(&staging).expect("real upload storage namespace").collect();
            assert_eq!(uploads.len(), 1, "the test owns this upload namespace");
            let path = uploads.into_iter().next().unwrap().unwrap().path().join("xl.meta");
            assert!(path.is_file(), "the upload has persisted metadata");
            std::fs::write(path, b"unreadable test metadata").expect("damage the fixture metadata only");
        }
        let storage_error = store
            .list_object_parts(&bucket, key, id, None, 1000, &opts)
            .await
            .expect_err("loss of three upload metadata replicas fails the read");
        assert!(!matches!(
            storage_error,
            StorageError::InvalidUploadID(..) | StorageError::MalformedUploadID(..)
        ));
        let expected = multipart_upload_api_error(storage_error);
        for target in [
            gateway_fields(&bucket, key, id, None, None),
            ListPartsFields {
                target: ListPartsTarget::Legacy {
                    bucket: bucket.clone(),
                    key: key.to_owned(),
                    upload_id: id.to_owned(),
                },
                part_number_marker: None,
                max_parts: None,
            },
        ] {
            let refusal = execute_counted(&usecase, envelope(), target, 1)
                .await
                .expect_err("surface the outage");
            let legacy: LegacyS3Error = refusal.into();
            assert_eq!(legacy.code(), expected.code());
            assert_eq!(legacy.status_code(), expected.status_code());
            assert_eq!(legacy.message(), expected.message());
            assert!(
                legacy
                    .source()
                    .and_then(|cause| cause.downcast_ref::<StorageError>())
                    .is_some()
            );
        }
    });
}

#[test]
#[serial_test::serial]
fn list_parts_applies_the_authorized_generation_before_lookup() {
    run_large_stack_test("list-parts-generation", || async {
        let context = shared_gating_ambient().await;
        let store = context.object_store();
        let usecase = DefaultMultipartUsecase::with_context(Some(context.clone()));
        let slot = ServerContextSlot::new();
        assert!(slot.install(context));
        let bucket = format!("list-parts-{}", Uuid::new_v4().simple());
        let other = format!("list-parts-{}", Uuid::new_v4().simple());
        let key = "fixture";
        let opts = ObjectOptions::default();
        for name in [&bucket, &other] {
            store
                .make_bucket(name, &MakeBucketOptions::default())
                .await
                .expect("create a fixture bucket");
        }
        let req = || {
            let mut req = build_request((), Method::GET);
            req.extensions.insert(slot.clone());
            req
        };
        let capability = |query: &str| {
            let mut request = req();
            request.uri = format!("/fixture?{query}").parse().expect("fixture URI");
            RequestEnvelope::from_s3s(&request)
        };
        for (query, first) in [
            (
                "x-rustfs-max-total-object-size=1&x-rustfs-max-content-length=1",
                "x-rustfs-max-total-object-size",
            ),
            ("x-rustfs-max-content-length=1", "x-rustfs-max-content-length"),
        ] {
            let err = execute_counted(
                &usecase,
                capability(query),
                gateway_fields(&bucket, key, "/impossible", Some("-1"), None),
                0,
            )
            .await
            .expect_err("capability validation precedes generation, pagination and claim resolution");
            assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
            assert!(err.message().is_some_and(|message| message.starts_with(first)));
        }

        let old = load_bucket_generation_from_store(&store, &req(), &bucket)
            .await
            .expect("old generation");
        let other_guard = load_bucket_generation_from_store(&store, &req(), &other)
            .await
            .expect("other generation");
        store
            .delete_bucket(&bucket, &DeleteBucketOptions::default())
            .await
            .expect("remove the original bucket");
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("recreate the bucket");
        let upload = store
            .new_multipart_upload(&bucket, key, &opts)
            .await
            .expect("session from the recreated bucket");
        let current = load_bucket_generation_from_store(&store, &req(), &bucket)
            .await
            .expect("current generation");
        let mut veeam_request = req();
        veeam_request.install_bucket_generation(current.clone());
        temp_env::async_with_vars([(rustfs_config::ENV_VEEAM_SOSAPI_ENABLE, Some("true"))], async {
            let err = execute_counted(
                &usecase,
                RequestEnvelope::from_s3s(&veeam_request),
                gateway_fields(
                    &bucket,
                    ".system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml",
                    "/impossible",
                    Some("-1"),
                    None,
                ),
                0,
            )
            .await
            .expect_err("reserved objects are checked before pagination and upload claims");
            assert_eq!(err.code(), &S3ErrorCode::MethodNotAllowed);
        })
        .await;
        for (guard, marker, lookups, code) in [
            (Some(old), None, 1, Some("NoSuchBucket")),
            (Some(other_guard), Some("-1"), 0, Some("InternalError")),
            (None, Some("-1"), 0, Some("InternalError")),
            (Some(current), None, 1, None),
        ] {
            let mut request = req();
            if let Some(guard) = guard {
                request.install_bucket_generation(guard);
            }
            let result = execute_counted(
                &usecase,
                RequestEnvelope::from_s3s(&request),
                gateway_fields(&bucket, key, &upload.upload_id, marker, None),
                lookups,
            )
            .await;
            assert_eq!(result.as_ref().err().map(|err| err.code().as_str()), code);
            if let Ok(page) = result {
                assert!(page.upload_id == upload.upload_id);
            }
        }
    });
}
