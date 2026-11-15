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

//! CreateMultipartUpload.

use super::*;
use crate::app::RequestEnvelope;
use crate::app::object::shared::build_put_like_object_lock_metadata_native;
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result, s3_error};

fn create_multipart_upload_metadata(
    input_metadata: Option<HashMap<String, String>>,
    headers: &HeaderMap,
    tagging: Option<String>,
    storage_class: Option<&str>,
) -> HashMap<String, String> {
    let mut metadata = input_metadata.unwrap_or_default();
    namespace_reserved_user_metadata(&mut metadata);
    extract_metadata_from_mime(headers, &mut metadata);

    if let Some(tags) = tagging {
        metadata.insert(AMZ_OBJECT_TAGGING.to_owned(), tags);
    }
    if let Some(storage_class) = storage_class {
        metadata.insert(AMZ_STORAGE_CLASS.to_owned(), storage_class.to_owned());
    }

    metadata
}

/// A multipart session advertises disk compression only when the staged-rollout
/// switch (`RUSTFS_COMPRESSION_MULTIPART_ENABLED`) is on, the object key/headers
/// qualify, AND the session is not an SSE-C ciphertext-passthrough replication
/// session, which must preserve source bytes verbatim.
///
/// The rollout switch defaults to off so a rolling upgrade never creates new
/// compressed multipart objects while pre-fix nodes (whose decompressor is not
/// resumable) may still serve reads. Enable it once the fleet has converged on a
/// fixed build; the default flips per the `multipart-compression-default-off-window`
/// entry in docs/architecture/compat-cleanup-register.md.
///
/// Each part is compressed as an independent stream; the GET path decodes across part
/// boundaries (see `ReadTransform::Compressed`), so the session may advertise
/// object-level compression again.
///
/// Unlike single PUT there is no `MIN_DISK_COMPRESSIBLE_SIZE` floor here: the total
/// object size is unknown at CreateMultipartUpload time, so tiny multipart objects pay
/// the (harmless) framing overhead. This is a deliberate trade-off, not a bug.
fn should_advertise_session_compression(multipart_enabled: bool, ciphertext_passthrough: bool, disk_compressible: bool) -> bool {
    multipart_enabled && !ciphertext_passthrough && disk_compressible
}

impl DefaultMultipartUsecase {
    #[instrument(level = "debug", skip(self, envelope, input, query_version_id))]
    #[hotpath::measure(impl_type = "MultipartUsecase")]
    pub(crate) async fn execute_create_multipart_upload(
        &self,
        envelope: RequestEnvelope,
        input: CreateMultipartUploadFields,
        query_version_id: Option<String>,
    ) -> S3Result<CreateMultipartUploadResult> {
        validate_new_object_key(&input.key, "CreateMultipartUpload")?;
        let headers = envelope.headers();
        let multipart_max_total_object_size = parse_presigned_multipart_max_total_object_size(
            headers,
            envelope.uri().query(),
            envelope.verified_sigv4().is_some(),
        )?;
        reject_presigned_put_max_content_length_for_other_operation(
            headers,
            envelope.uri().query(),
            envelope.verified_presigned().is_some(),
        )?;
        let helper = OperationHelper::from_view(
            &envelope.audit_view(),
            EventName::ObjectCreatedCreateMultipartUpload,
            S3Operation::CreateMultipartUpload,
        )
        .suppress_event();
        let replication_authorized = replication_request_authorized(&envelope);
        let CreateMultipartUploadFields {
            bucket,
            key,
            tagging,
            storage_class,
            server_side_encryption,
            sse_customer_algorithm,
            sse_customer_key,
            sse_customer_key_md5,
            ssekms_key_id,
            object_lock_legal_hold_status,
            object_lock_mode,
            object_lock_retain_until_date,
            metadata: input_metadata,
        } = input;

        let server_side_encryption = server_side_encryption.or(extract_server_side_encryption_from_headers(headers)?);
        let ssekms_key_id = ssekms_key_id.or_else(|| {
            headers
                .get("x-amz-server-side-encryption-aws-kms-key-id")
                .and_then(|value| value.to_str().ok())
                .map(ToOwned::to_owned)
        });

        // Validate storage class if provided
        if let Some(storage_class) = storage_class.as_deref()
            && !is_valid_storage_class(storage_class)
        {
            return Err(s3_error!(InvalidStorageClass, "The storage class you specified is not valid."));
        }

        reject_veeam_sosapi_mutation(|| self.object_store(), &bucket, &key).await?;
        validate_table_catalog_object_mutation(&bucket, &key).await?;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        validate_archive_content_encoding(
            &key,
            headers.get("content-type").and_then(|value| value.to_str().ok()),
            headers.get("content-encoding").and_then(|value| value.to_str().ok()),
        )?;

        let mut metadata = create_multipart_upload_metadata(input_metadata, headers, tagging, storage_class.as_deref());
        if let Some(limit) = multipart_max_total_object_size {
            insert_str(&mut metadata, SUFFIX_MAX_TOTAL_OBJECT_SIZE, limit.to_string());
        }
        if bucket_uses_xxh3_etag(&bucket) {
            insert_str(&mut metadata, SUFFIX_ETAG_ALGORITHM, ETAG_ALGORITHM_XXH3.to_string());
        }

        let has_explicit_object_lock_retention = object_lock_mode.is_some()
            || object_lock_retain_until_date.is_some()
            || has_replication_retention_update(headers, replication_authorized);
        let object_lock_config_state = load_bucket_object_lock_config_state(&bucket).await?;
        if let Some(object_lock_metadata) = build_put_like_object_lock_metadata_native(
            &bucket,
            &object_lock_config_state,
            object_lock_legal_hold_status,
            object_lock_mode,
            object_lock_retain_until_date,
        )? {
            metadata.extend(object_lock_metadata);
        }
        apply_bucket_default_lock_retention(
            &bucket,
            &object_lock_config_state,
            &mut metadata,
            has_explicit_object_lock_retention,
        )?;
        let (header_sse_customer_algorithm, header_sse_customer_key, header_sse_customer_key_md5) =
            extract_ssec_params_from_headers(headers)?;
        let sse_customer_algorithm = sse_customer_algorithm.or(header_sse_customer_algorithm);
        let sse_customer_key = sse_customer_key.or(header_sse_customer_key);
        let sse_customer_key_md5 = sse_customer_key_md5.or(header_sse_customer_key_md5);

        // The session data key is generated here, so this is where a multipart upload is held
        // to the KMS key it names. Parts and completion also authorize their own caller.
        let session_principal = SseKmsPrincipal::from_request(&envelope);
        let encryption_request = PrepareEncryptionRequest {
            bucket: &bucket,
            key: &key,
            server_side_encryption,
            ssekms_key_id,
            ssekms_context: extract_ssekms_context_from_headers(headers)?,
            sse_customer_algorithm: sse_customer_algorithm.clone(),
            sse_customer_key,
            sse_customer_key_md5: sse_customer_key_md5.clone(),
            principal: session_principal.as_ref(),
        };

        // SSE-C ciphertext passthrough: parts are already encrypted, so no
        // session DEK is prepared; a session marker tells UploadPart to store
        // the ciphertext verbatim instead of recovering encryption material.
        let ciphertext_passthrough = replication_authorized
            && get_header(headers, SUFFIX_SOURCE_REPLICATION_REQUEST).as_deref() == Some("true")
            && rustfs_utils::http::ssec_transport_to_stored_metadata(headers).is_some();
        if ciphertext_passthrough && let Some(metadata_sys) = self.bucket_metadata_sys() {
            let check_result = map_quota_check_outcome(
                &bucket,
                QuotaChecker::new(metadata_sys)
                    .check_quota(&bucket, QuotaOperation::PutObject, 0)
                    .await,
            )?;
            if check_result.quota_limit.is_some() {
                return Err(S3Error::with_message(
                    S3ErrorCode::InvalidRequest,
                    "SSE-C ciphertext replication is unavailable for quota-enabled buckets".to_string(),
                ));
            }
        }
        if ciphertext_passthrough {
            insert_str(&mut metadata, SUFFIX_REPLICATION_PRESERVE_CIPHERTEXT, "true".to_string());
        }

        let prepared_material = if ciphertext_passthrough {
            None
        } else {
            sse_prepare_encryption(encryption_request).await?
        };
        let (effective_sse, effective_kms_key_id) = match prepared_material {
            Some(material) => {
                let server_side_encryption = Some(material.server_side_encryption.clone());
                let ssekms_key_id = material.response_kms_key_id();

                let mut encryption_metadata = encryption_material_to_metadata(&material)?;
                if material.key_kind == EncryptionKeyKind::Object {
                    mark_encrypted_multipart_metadata(&mut encryption_metadata);
                }
                metadata.extend(encryption_metadata);

                (server_side_encryption, ssekms_key_id)
            }
            None => (None, None),
        };

        if should_advertise_session_compression(
            is_multipart_disk_compression_enabled(),
            ciphertext_passthrough,
            is_disk_compressible(headers, &key),
        ) {
            rustfs_utils::http::insert_str(
                &mut metadata,
                rustfs_utils::http::SUFFIX_COMPRESSION,
                compression_metadata_value(CompressionAlgorithm::default()),
            );
        }

        let mt2 = metadata.clone();
        let mut opts: ObjectOptions =
            put_opts_with_replication_authorization(&bucket, &key, query_version_id, headers, metadata, replication_authorized)
                .await
                .map_err(ApiError::from)?;
        apply_bucket_generation_guard(&envelope, &bucket, &mut opts)?;

        let dsc =
            must_replicate_object(&bucket, &key, &mt2, "".to_string(), opts.delete_marker_replication_status(), opts.clone())
                .await;
        if dsc.replicate_any() {
            insert_str(&mut opts.user_defined, SUFFIX_REPLICATION_GENERATION, Uuid::new_v4().to_string());
            insert_str(&mut opts.user_defined, SUFFIX_REPLICATION_TIMESTAMP, jiff::Zoned::now().to_string());
            insert_str(
                &mut opts.user_defined,
                SUFFIX_REPLICATION_STATUS,
                dsc.pending_status().unwrap_or_default(),
            );
        }

        if !super::super::object::shared::put_like_write_creates_new_version(&opts)
            && super::super::object::shared::object_lock_checks_required_for_state(&object_lock_config_state)
        {
            let current_opts: ObjectOptions = internal_object_info_lookup_opts(
                get_opts(&bucket, &key, opts.version_id.clone(), None, headers)
                    .await
                    .map_err(ApiError::from)?,
            );
            match store.get_object_info(&bucket, &key, &current_opts).await {
                Ok(existing_obj_info) => {
                    validate_existing_object_lock_for_write(&object_lock_config_state, &existing_obj_info, &opts)?
                }
                Err(err) => {
                    if !is_err_object_not_found(&err) && !is_err_version_not_found(&err) {
                        return Err(ApiError::from(err).into());
                    }
                }
            }
        }

        let checksum_type = rustfs_rio::ChecksumType::from_header(headers);
        if checksum_type.is(rustfs_rio::ChecksumType::INVALID) {
            return Err(s3_error!(InvalidArgument, "Invalid checksum type"));
        } else if checksum_type.is_set() && !checksum_type.is(rustfs_rio::ChecksumType::TRAILING) {
            opts.want_checksum = Some(rustfs_rio::Checksum {
                checksum_type,
                ..Default::default()
            });
        }
        if effective_sse.is_some() && opts.want_checksum.is_some() {
            insert_str(&mut opts.user_defined, SUFFIX_PLAINTEXT_CHECKSUM, "true".to_string());
        }

        let MultipartUploadResult {
            upload_id,
            checksum_algo,
            checksum_type,
        } = store
            .new_multipart_upload(&bucket, &key, &opts)
            .await
            .map_err(ApiError::from)?;

        let output = CreateMultipartUploadResult {
            bucket,
            key,
            upload_id,
            server_side_encryption: effective_sse,
            sse_customer_algorithm,
            sse_customer_key_md5,
            ssekms_key_id: effective_kms_key_id,
            checksum_algorithm: checksum_algo,
            checksum_type,
        };

        let result = Ok(output);
        let _ = helper.complete_owned(&result, http::StatusCode::OK, &HeaderMap::new());
        result
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::*;
    use super::*;
    use crate::app::storage_api::s3::{S3ErrorCode, dto::CreateMultipartUploadInput};

    #[tokio::test]
    async fn create_multipart_baseline_legacy_direct() {
        use crate::app::storage_api::s3::dto::Timestamp;

        let date = time::Date::from_calendar_date(-1, time::Month::January, 1)
            .unwrap()
            .midnight()
            .assume_utc();
        let long_key = "k".repeat(1025);
        let invalid_class = ("InvalidStorageClass", "The storage class you specified is not valid.");
        for (bucket, key, invalid, date, expected) in [
            ("ab", "object", true, None, invalid_class),
            ("a/b", "object", true, None, invalid_class),
            ("bucket", "", true, None, ("InvalidArgument", "Invalid object key")),
            (
                "bucket",
                "a\0b",
                true,
                None,
                ("InvalidArgument", "Object key contains invalid control characters: \"a\\0b\""),
            ),
            ("bucket", long_key.as_str(), true, None, invalid_class),
            ("bucket", "object", false, Some(date), ("InternalError", "Not init")),
            ("bucket", "object", true, Some(date), invalid_class),
        ] {
            let input = CreateMultipartUploadInput::builder()
                .bucket(bucket.to_owned())
                .key(key.to_owned())
                .storage_class(invalid.then(|| StorageClass::from("unknown-class".to_owned())))
                .object_lock_retain_until_date(date.map(Timestamp::from))
                .build()
                .expect("direct fixture DTO");
            let error = make_usecase()
                .execute_create_multipart_upload_from_s3s(build_request(input, Method::POST))
                .await
                .expect_err("direct fixture does not create an upload");
            assert_eq!(
                (error.code().as_str(), error.message()),
                (expected.0, Some(expected.1)),
                "{bucket:?}/{key:?}"
            );
        }
    }

    #[test]
    fn session_compression_is_advertised_only_for_non_passthrough_compressible_uploads() {
        // (multipart_enabled, ciphertext_passthrough, disk_compressible, expected)
        let cases = [
            (true, false, false, false),
            (true, false, true, true),
            (true, true, false, false),
            (true, true, true, false),
            // The staged-rollout switch keeps multipart compression dark by
            // default regardless of the other gates.
            (false, false, true, false),
            (false, false, false, false),
            (false, true, true, false),
            (false, true, false, false),
        ];

        for (multipart_enabled, ciphertext_passthrough, disk_compressible, expected) in cases {
            assert_eq!(
                should_advertise_session_compression(multipart_enabled, ciphertext_passthrough, disk_compressible),
                expected,
                "multipart_enabled={multipart_enabled} ciphertext_passthrough={ciphertext_passthrough} disk_compressible={disk_compressible}"
            );
        }
    }

    #[tokio::test]
    async fn execute_create_multipart_upload_rejects_invalid_storage_class() {
        let input = CreateMultipartUploadInput::builder()
            .bucket("bucket".to_string())
            .key("object".to_string())
            .storage_class(Some(StorageClass::from("invalid".to_string())))
            .build()
            .unwrap();
        let req = build_request(input, Method::POST);

        let err = make_usecase()
            .execute_create_multipart_upload_from_s3s(req)
            .await
            .unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidStorageClass);
    }

    #[test]
    fn create_multipart_upload_metadata_persists_dto_storage_class_without_raw_header() {
        let metadata = create_multipart_upload_metadata(None, &HeaderMap::new(), None, Some("REDUCED_REDUNDANCY"));

        assert_eq!(metadata.get(AMZ_STORAGE_CLASS), Some(&"REDUCED_REDUNDANCY".to_string()));
    }

    #[test]
    fn create_multipart_upload_metadata_keeps_user_and_system_namespaces_separate() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/octet-stream"));
        headers.insert(AMZ_STORAGE_CLASS, HeaderValue::from_static("STANDARD"));
        let input_metadata = HashMap::from([
            ("content-type".to_string(), "user-content-type".to_string()),
            (AMZ_STORAGE_CLASS.to_string(), "user-storage-class".to_string()),
        ]);

        let metadata = create_multipart_upload_metadata(
            Some(input_metadata),
            &headers,
            Some("project=rustfs".to_string()),
            Some("REDUCED_REDUNDANCY"),
        );

        assert_eq!(metadata.get("content-type"), Some(&"application/octet-stream".to_string()));
        assert_eq!(metadata.get("x-amz-meta-content-type"), Some(&"user-content-type".to_string()));
        assert_eq!(metadata.get(AMZ_STORAGE_CLASS), Some(&"REDUCED_REDUNDANCY".to_string()));
        assert_eq!(metadata.get("x-amz-meta-x-amz-storage-class"), Some(&"user-storage-class".to_string()));
        assert_eq!(metadata.get(AMZ_OBJECT_TAGGING), Some(&"project=rustfs".to_string()));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn create_multipart_rejects_ciphertext_replication_before_parts_are_staged() {
        let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("ciphertext-multipart-quota", 4096).await;
        let usecase = DefaultMultipartUsecase::from_global();
        let input = CreateMultipartUploadInput::builder()
            .bucket(bucket.clone())
            .key("object".to_string())
            .build()
            .expect("create multipart request should build");
        let mut request = build_request(input, Method::POST);
        insert_header(&mut request.headers, SUFFIX_SOURCE_REPLICATION_REQUEST, "true");
        request
            .headers
            .insert(rustfs_utils::http::REPLICATION_SSEC_ALGORITHM_HEADER, HeaderValue::from_static("AES256"));
        request.extensions.insert(crate::storage::access::ReqInfo {
            replication_request_authorized: true,
            ..Default::default()
        });

        let result = usecase.execute_create_multipart_upload_from_s3s(request).await;
        assert!(
            store
                .list_multipart_uploads(&bucket, "", None, None, None, 100)
                .await
                .expect("list durable uploads")
                .uploads
                .is_empty()
        );
        let err = result.expect_err("quota-enabled ciphertext multipart replication must be refused");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert_eq!(
            err.message(),
            Some("SSE-C ciphertext replication is unavailable for quota-enabled buckets")
        );
    }

    #[test]
    #[serial_test::serial]
    fn create_multipart_checks_kms_permission_before_creating_a_session() {
        crate::app::gating_test_env::run_large_stack_test("create-multipart-kms-policy", || async {
            use crate::app::storage_api::s3::dto::ServerSideEncryption;
            use crate::app::storage_api::test::{
                ReqInfo,
                sse::{RecordingKmsKeyAuthorizer, TestAuthorizationHooks},
            };
            use rustfs_policy::policy::action::KmsAction;

            let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("create-multipart-kms", 16_384).await;
            let context = crate::app::runtime_sources::current_app_context().expect("test app context");
            let usecase = DefaultMultipartUsecase::with_context(Some(context.clone()));
            let key_id = "multipart-test-key";
            let kms = context.kms().handle();
            kms.reconfigure(rustfs_kms::config::KmsConfig::static_kms(
                key_id.to_owned(),
                base64_simd::STANDARD.encode_to_string([1_u8; 32]),
            ))
            .await
            .expect("configure test KMS");
            for allowed in [false, true] {
                let input = CreateMultipartUploadInput {
                    bucket: bucket.clone(),
                    key: "object".to_owned(),
                    server_side_encryption: Some(ServerSideEncryption::from_static("aws:kms")),
                    ssekms_key_id: Some(key_id.to_owned()),
                    ..Default::default()
                };
                let authorizer = RecordingKmsKeyAuthorizer::new(allowed);
                let mut req = build_request(input, Method::POST);
                req.extensions.insert(ReqInfo {
                    cred: Some(rustfs_credentials::Credentials {
                        access_key: "test-kms-writer".to_owned(),
                        ..Default::default()
                    }),
                    ..Default::default()
                });
                req.extensions.insert(TestAuthorizationHooks {
                    enforced: true,
                    authorizer: authorizer.clone(),
                });
                let result = usecase.execute_create_multipart_upload_from_s3s(req).await;
                let uploads = store
                    .list_multipart_uploads(&bucket, "", None, None, None, 100)
                    .await
                    .expect("list uploads after KMS decision")
                    .uploads;
                assert_eq!(uploads.len(), usize::from(allowed));
                assert_eq!(authorizer.calls(), [(KmsAction::GenerateDataKeyAction, key_id.to_owned())]);
                if allowed {
                    let output = result.expect("KMS authorizes the selected key").output;
                    assert_eq!(output.upload_id.as_deref(), Some(uploads[0].upload_id.as_str()));
                } else {
                    let error = result.expect_err("KMS denied the selected key");
                    assert_eq!(error.code(), &S3ErrorCode::AccessDenied);
                }
            }
            kms.stop().await.expect("stop test KMS");
        });
    }

    #[test]
    fn create_multipart_preserves_legacy_date_precision() {
        use time::{Date, Month, Time};

        let date = Date::from_calendar_date(-1, Month::January, 1)
            .unwrap()
            .with_time(Time::from_hms_nano(0, 0, 0, 123_456_789).unwrap())
            .assume_utc();
        let input = CreateMultipartUploadInput {
            bucket: "sthree-multipart".to_owned(),
            key: "object".to_owned(),
            object_lock_retain_until_date: Some(date.into()),
            ..Default::default()
        };
        let fields = CreateMultipartUploadFields::from_legacy(input);
        assert_eq!(fields.object_lock_retain_until_date, Some(date));
    }

    #[test]
    #[serial_test::serial]
    fn create_multipart_generation_fences_the_durable_upload() {
        crate::app::gating_test_env::run_large_stack_test("create-multipart-generation", || async {
            use crate::app::storage_api::multipart_usecase::access::{AuthorizationRequest, load_bucket_generation_from_store};
            use crate::app::storage_api::test::contract::bucket::{
                BucketOperations as _, DeleteBucketOptions, MakeBucketOptions,
            };
            use crate::runtime_sources::ServerContextSlot;

            let context = crate::app::gating_test_env::shared_gating_ambient().await;
            let store = context.object_store();
            let usecase = DefaultMultipartUsecase::with_context(Some(context.clone()));
            let slot = ServerContextSlot::new();
            assert!(slot.install(context));
            let bucket = format!("create-multipart-{}", Uuid::new_v4().simple());
            let other = format!("create-multipart-{}", Uuid::new_v4().simple());
            let request = |bucket: &str| {
                let input = CreateMultipartUploadInput {
                    bucket: bucket.to_owned(),
                    key: "object".to_owned(),
                    ..Default::default()
                };
                let mut request = build_request(input, Method::POST);
                request.uri = format!("/{bucket}/object?uploads").parse().expect("fixture URI");
                request.extensions.insert(slot.clone());
                request
            };
            for name in [&bucket, &other] {
                store
                    .make_bucket(name, &MakeBucketOptions::default())
                    .await
                    .expect("create bucket");
            }

            let mut stale = request(&bucket);
            let guard = load_bucket_generation_from_store(&store, &stale, &bucket)
                .await
                .expect("capture original incarnation");
            stale.install_bucket_generation(guard);
            let original = store.bucket_incarnation_id(&bucket).await.expect("old bucket generation");

            store
                .delete_bucket(&bucket, &DeleteBucketOptions::default())
                .await
                .expect("delete original bucket");
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("recreate bucket");
            let current = store.bucket_incarnation_id(&bucket).await.expect("new bucket generation");
            assert_ne!(original, current);

            let missing = request(&bucket);
            let mut wrong = request(&bucket);
            let guard = load_bucket_generation_from_store(&store, &wrong, &other)
                .await
                .expect("capture the other bucket");
            wrong.install_bucket_generation(guard);
            for (request, code) in [
                (stale, S3ErrorCode::NoSuchBucket),
                (missing, S3ErrorCode::InternalError),
                (wrong, S3ErrorCode::InternalError),
            ] {
                let result = usecase.execute_create_multipart_upload_from_s3s(request).await;
                assert!(
                    store
                        .list_multipart_uploads(&bucket, "", None, None, None, 100)
                        .await
                        .expect("list uploads in the recreated bucket")
                        .uploads
                        .is_empty()
                );
                let error = result.expect_err("creation requires the target incarnation");
                assert_eq!(error.code(), &code);
            }

            let probe = crate::app::storage_api::multipart_usecase::helper::MutationCompletionProbe::install(&bucket);
            let mut allowed = request(&bucket);
            let guard = load_bucket_generation_from_store(&store, &allowed, &bucket)
                .await
                .expect("capture current incarnation");
            allowed.install_bucket_generation(guard);
            let response = usecase
                .execute_create_multipart_upload_from_s3s(allowed)
                .await
                .expect("a current guard allows creation");
            let uploads = store
                .list_multipart_uploads(&bucket, "", None, None, None, 100)
                .await
                .expect("list durable uploads")
                .uploads;
            assert_eq!(uploads.len(), 1);
            assert_eq!(Some(uploads[0].upload_id.as_str()), response.output.upload_id.as_deref());
            assert_eq!(probe.audits.lock().unwrap().len(), 1);
            assert!(probe.events.lock().unwrap().is_empty());
        });
    }
}
