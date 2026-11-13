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
use crate::app::storage_api::multipart_usecase::{
    DEFAULT_EC_BLOCK_SIZE, EC_BLOCK_SIZE_HINT_INTERNAL_SUFFIX, EC_BLOCK_SIZE_HINT_METADATA_KEY, EC_BLOCK_SIZE_HINT_REASON_HEADER,
    EC_BLOCK_SIZE_HINT_STATUS_HEADER, EC_BLOCK_SIZE_RESPONSE_HEADER, EcBlockSizeHintOutcome, resolve_ec_block_size_hint,
};
use crate::app::storage_api::s3::dto::{
    ChecksumAlgorithm, ChecksumType, CreateMultipartUploadInput, CreateMultipartUploadOutput,
};
use crate::app::storage_api::s3::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};

fn create_multipart_upload_metadata(
    input_metadata: Option<HashMap<String, String>>,
    headers: &HeaderMap,
    tagging: Option<String>,
    storage_class: Option<&crate::app::storage_api::s3::dto::StorageClass>,
) -> HashMap<String, String> {
    let mut metadata = input_metadata.unwrap_or_default();
    namespace_reserved_user_metadata(&mut metadata);
    extract_metadata_from_mime(headers, &mut metadata);

    if let Some(tags) = tagging {
        metadata.insert(AMZ_OBJECT_TAGGING.to_owned(), tags);
    }
    if let Some(storage_class) = storage_class {
        metadata.insert(AMZ_STORAGE_CLASS.to_owned(), storage_class.as_str().to_owned());
    }

    metadata
}

fn extract_multipart_ec_block_size_hint(headers: &HeaderMap, metadata: &HashMap<String, String>) -> S3Result<Option<String>> {
    let enabled = rustfs_utils::get_env_bool(
        rustfs_config::ENV_EC_BLOCK_SIZE_HINT_ENABLE,
        rustfs_config::DEFAULT_EC_BLOCK_SIZE_HINT_ENABLE,
    );
    let fleet_confirmed = rustfs_utils::get_env_bool(
        rustfs_config::ENV_EC_BLOCK_SIZE_HINT_FLEET_CONFIRMED,
        rustfs_config::DEFAULT_EC_BLOCK_SIZE_HINT_FLEET_CONFIRMED,
    );
    let strict = enabled && fleet_confirmed;
    let header_name = http::HeaderName::from_static("x-amz-meta-rustfs-ec-block-size-hint");
    let raw_values = headers
        .get_all(header_name)
        .iter()
        .map(|value| {
            if strict {
                value.to_str().map(str::to_owned).map_err(|_| {
                    S3Error::with_message(
                        S3ErrorCode::InvalidArgument,
                        "rustfs-ec-block-size-hint must be ASCII decimal digits".to_string(),
                    )
                })
            } else {
                Ok(String::from_utf8_lossy(value.as_bytes()).into_owned())
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    if strict && raw_values.len() > 1 {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidArgument,
            "duplicate rustfs-ec-block-size-hint metadata is not allowed".to_string(),
        ));
    }

    let metadata_values = metadata
        .iter()
        .filter(|(key, _)| key.eq_ignore_ascii_case(EC_BLOCK_SIZE_HINT_METADATA_KEY))
        .map(|(_, value)| value.clone())
        .collect::<Vec<_>>();
    if strict && metadata_values.len() > 1 {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidArgument,
            "duplicate rustfs-ec-block-size-hint metadata is not allowed".to_string(),
        ));
    }

    let hint = raw_values.into_iter().next().or_else(|| metadata_values.into_iter().next());
    if strict && let Some(value) = hint.as_deref() {
        resolve_ec_block_size_hint(Some(value), true, true, true).map_err(|error| {
            S3Error::with_message(S3ErrorCode::InvalidArgument, format!("invalid rustfs-ec-block-size-hint: {error}"))
        })?;
    }
    Ok(hint)
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
    #[instrument(level = "debug", skip(self, req))]
    #[hotpath::measure(impl_type = "MultipartUsecase")]
    pub async fn execute_create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        validate_new_object_key(&req.input.key, "CreateMultipartUpload")?;
        let multipart_max_total_object_size = parse_presigned_multipart_max_total_object_size(
            &req.headers,
            req.uri.query(),
            req.extensions.get::<VerifiedSigV4Request>().is_some(),
        )?;
        reject_presigned_put_max_content_length_for_other_operation(
            &req.headers,
            req.uri.query(),
            req.extensions.get::<VerifiedPresignedRequest>().is_some(),
        )?;
        let helper =
            OperationHelper::new(&req, EventName::ObjectCreatedCreateMultipartUpload, S3Operation::CreateMultipartUpload)
                .suppress_event();
        let replication_authorized = replication_request_authorized(&req);
        let CreateMultipartUploadInput {
            bucket,
            key,
            tagging,
            version_id,
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
            ..
        } = req.input.clone();

        let server_side_encryption = server_side_encryption.or(extract_server_side_encryption_from_headers(&req.headers)?);
        let ssekms_key_id = ssekms_key_id.or_else(|| {
            req.headers
                .get("x-amz-server-side-encryption-aws-kms-key-id")
                .and_then(|value| value.to_str().ok())
                .map(ToOwned::to_owned)
        });

        // Validate storage class if provided
        if let Some(ref storage_class) = storage_class
            && !is_valid_storage_class(storage_class.as_str())
        {
            return Err(s3_error!(InvalidStorageClass));
        }

        reject_veeam_sosapi_mutation(|| self.object_store(), &bucket, &key).await?;
        validate_table_catalog_object_mutation(&bucket, &key).await?;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        validate_archive_content_encoding(
            &key,
            req.headers.get("content-type").and_then(|value| value.to_str().ok()),
            req.headers.get("content-encoding").and_then(|value| value.to_str().ok()),
        )?;

        let mut metadata = create_multipart_upload_metadata(input_metadata, &req.headers, tagging, storage_class.as_ref());
        let ec_block_size_hint = extract_multipart_ec_block_size_hint(&req.headers, &metadata)?;
        if let Some(limit) = multipart_max_total_object_size {
            insert_str(&mut metadata, SUFFIX_MAX_TOTAL_OBJECT_SIZE, limit.to_string());
        }
        if bucket_uses_xxh3_etag(&bucket) {
            insert_str(&mut metadata, SUFFIX_ETAG_ALGORITHM, ETAG_ALGORITHM_XXH3.to_string());
        }

        let has_explicit_object_lock_retention = object_lock_mode.is_some()
            || object_lock_retain_until_date.is_some()
            || has_replication_retention_update(&req.headers, replication_authorized);
        let object_lock_config_state = load_bucket_object_lock_config_state(&bucket).await?;
        if let Some(object_lock_metadata) = build_put_like_object_lock_metadata(
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
            extract_ssec_params_from_headers(&req.headers)?;
        let sse_customer_algorithm = sse_customer_algorithm.or(header_sse_customer_algorithm);
        let sse_customer_key = sse_customer_key.or(header_sse_customer_key);
        let sse_customer_key_md5 = sse_customer_key_md5.or(header_sse_customer_key_md5);

        // The session data key is generated here, so this is where a multipart upload is held
        // to the KMS key it names. Parts and completion also authorize their own caller.
        let session_principal = SseKmsPrincipal::from_request(&req);
        let encryption_request = PrepareEncryptionRequest {
            bucket: &bucket,
            key: &key,
            server_side_encryption,
            ssekms_key_id,
            ssekms_context: extract_ssekms_context_from_headers(&req.headers)?,
            sse_customer_algorithm: sse_customer_algorithm.clone(),
            sse_customer_key,
            sse_customer_key_md5: sse_customer_key_md5.clone(),
            principal: session_principal.as_ref(),
        };

        // SSE-C ciphertext passthrough: parts are already encrypted, so no
        // session DEK is prepared; a session marker tells UploadPart to store
        // the ciphertext verbatim instead of recovering encryption material.
        let ciphertext_passthrough = replication_authorized
            && get_header(&req.headers, SUFFIX_SOURCE_REPLICATION_REQUEST).as_deref() == Some("true")
            && rustfs_utils::http::ssec_transport_to_stored_metadata(&req.headers).is_some();
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

        let session_compression = should_advertise_session_compression(
            is_multipart_disk_compression_enabled(),
            ciphertext_passthrough,
            is_disk_compressible(&req.headers, &key),
        );
        if session_compression {
            rustfs_utils::http::insert_str(
                &mut metadata,
                rustfs_utils::http::SUFFIX_COMPRESSION,
                compression_metadata_value(CompressionAlgorithm::default()),
            );
        }

        let mt2 = metadata.clone();
        let mut opts: ObjectOptions =
            put_opts_with_replication_authorization(&bucket, &key, version_id, &req.headers, metadata, replication_authorized)
                .await
                .map_err(ApiError::from)?;
        apply_bucket_generation_guard(&req, &bucket, &mut opts)?;
        let shard_integrity_rollout =
            rustfs_utils::get_env_bool(rustfs_config::ENV_SHARD_INTEGRITY_WRITE, rustfs_config::DEFAULT_SHARD_INTEGRITY_WRITE)
                && rustfs_utils::get_env_bool(
                    rustfs_config::ENV_SHARD_INTEGRITY_FLEET_CONFIRMED,
                    rustfs_config::DEFAULT_SHARD_INTEGRITY_FLEET_CONFIRMED,
                );

        let ec_block_size_hint_resolution = resolve_ec_block_size_hint(
            ec_block_size_hint.as_deref(),
            rustfs_utils::get_env_bool(
                rustfs_config::ENV_EC_BLOCK_SIZE_HINT_ENABLE,
                rustfs_config::DEFAULT_EC_BLOCK_SIZE_HINT_ENABLE,
            ),
            rustfs_utils::get_env_bool(
                rustfs_config::ENV_EC_BLOCK_SIZE_HINT_FLEET_CONFIRMED,
                rustfs_config::DEFAULT_EC_BLOCK_SIZE_HINT_FLEET_CONFIRMED,
            ),
            !session_compression
                && effective_sse.is_none()
                && sse_customer_algorithm.is_none()
                && effective_kms_key_id.is_none()
                && !ciphertext_passthrough
                && !replication_authorized
                && !opts.data_movement
                && opts.shard_integrity_write_mode.is_none()
                && !shard_integrity_rollout,
        )
        .map_err(|err| {
            S3Error::with_message(S3ErrorCode::InvalidArgument, format!("invalid rustfs-ec-block-size-hint: {err}"))
        })?;
        // A 1 MiB hint selects the historical storage default, so avoid
        // writing a redundant transient marker on the multipart hot path.
        if matches!(ec_block_size_hint_resolution.outcome, EcBlockSizeHintOutcome::Applied)
            && ec_block_size_hint_resolution.effective_block_size != DEFAULT_EC_BLOCK_SIZE
        {
            rustfs_utils::http::metadata_compat::insert_str(
                &mut opts.user_defined,
                EC_BLOCK_SIZE_HINT_INTERNAL_SUFFIX,
                ec_block_size_hint_resolution.effective_block_size.to_string(),
            );
        }

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
                get_opts(&bucket, &key, opts.version_id.clone(), None, &req.headers)
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

        let checksum_type = rustfs_rio::ChecksumType::from_header(&req.headers);
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
            effective_ec_block_size,
        } = store
            .new_multipart_upload(&bucket, &key, &opts)
            .await
            .map_err(ApiError::from)?;

        let output = CreateMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            upload_id: Some(upload_id),
            server_side_encryption: effective_sse,
            sse_customer_algorithm,
            sse_customer_key_md5,
            ssekms_key_id: effective_kms_key_id,
            checksum_algorithm: checksum_algo.map(ChecksumAlgorithm::from),
            checksum_type: checksum_type.map(ChecksumType::from),
            ..Default::default()
        };

        let mut response = S3Response::new(output);
        if let Some(block_size) = effective_ec_block_size
            && let Ok(value) = HeaderValue::from_str(&block_size.to_string())
        {
            response.headers.insert(EC_BLOCK_SIZE_RESPONSE_HEADER, value);
        }
        if let Some(status) = ec_block_size_hint_resolution.outcome.status_header() {
            response
                .headers
                .insert(EC_BLOCK_SIZE_HINT_STATUS_HEADER, HeaderValue::from_static(status));
        }
        if let Some(reason) = ec_block_size_hint_resolution.outcome.reason_header() {
            response
                .headers
                .insert(EC_BLOCK_SIZE_HINT_REASON_HEADER, HeaderValue::from_static(reason));
        }
        let result = Ok(response);
        let _ = helper.complete(&result);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::*;
    use super::*;

    #[test]
    #[serial_test::serial(ec_block_size_hint)]
    fn multipart_ec_block_size_hint_validates_create_upload_metadata() {
        temp_env::with_vars(
            [
                (rustfs_config::ENV_EC_BLOCK_SIZE_HINT_ENABLE, Some("true")),
                (rustfs_config::ENV_EC_BLOCK_SIZE_HINT_FLEET_CONFIRMED, Some("true")),
            ],
            || {
                let header_name = http::HeaderName::from_static("x-amz-meta-rustfs-ec-block-size-hint");
                let mut headers = HeaderMap::new();
                headers.insert(header_name, HeaderValue::from_static("65_536"));
                let error =
                    extract_multipart_ec_block_size_hint(&headers, &HashMap::new()).expect_err("malformed create-upload hint");
                assert_eq!(error.code(), &S3ErrorCode::InvalidArgument);
            },
        );

        temp_env::with_vars(
            [
                (rustfs_config::ENV_EC_BLOCK_SIZE_HINT_ENABLE, Some("false")),
                (rustfs_config::ENV_EC_BLOCK_SIZE_HINT_FLEET_CONFIRMED, Some("false")),
            ],
            || {
                let header_name = http::HeaderName::from_static("x-amz-meta-rustfs-ec-block-size-hint");
                let mut headers = HeaderMap::new();
                headers.insert(header_name, HeaderValue::from_static("not-a-number"));
                assert_eq!(
                    extract_multipart_ec_block_size_hint(&headers, &HashMap::new())
                        .unwrap()
                        .as_deref(),
                    Some("not-a-number")
                );
            },
        );
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

        let err = make_usecase().execute_create_multipart_upload(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidStorageClass);
    }

    #[test]
    fn create_multipart_upload_metadata_persists_dto_storage_class_without_raw_header() {
        let metadata = create_multipart_upload_metadata(
            None,
            &HeaderMap::new(),
            None,
            Some(&StorageClass::from_static("REDUCED_REDUNDANCY")),
        );

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
            Some(&StorageClass::from_static("REDUCED_REDUNDANCY")),
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
        let (_store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("ciphertext-multipart-quota", 4096).await;
        let usecase = DefaultMultipartUsecase::from_global();
        let input = CreateMultipartUploadInput::builder()
            .bucket(bucket)
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

        let err = usecase
            .execute_create_multipart_upload(request)
            .await
            .expect_err("quota-enabled ciphertext multipart replication should fail before upload creation");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    }
}
