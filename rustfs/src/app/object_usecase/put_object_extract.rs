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

use super::*;
use crate::app::context::NotifyInterface;
use rustfs_object_io::put::{
    apply_extract_entry_pax_extensions, apply_trailing_checksums, is_sse_kms_requested, map_extract_archive_error,
    normalize_extract_entry_key, resolve_put_object_extract_options,
};

impl DefaultObjectUsecase {
    pub(super) async fn run_put_object_extract_flow(
        input: PutObjectInput,
        request_context: PutObjectRequestContext,
        notify: Arc<dyn NotifyInterface>,
        resolved_size: i64,
    ) -> S3Result<PutObjectOutput> {
        if is_sse_kms_requested(&input, &request_context.headers) {
            return Err(s3_error!(NotImplemented, "SSE-KMS is not supported for extract uploads"));
        }

        let PutObjectInput {
            body,
            bucket,
            key,
            version_id,
            cache_control,
            content_disposition,
            content_encoding,
            content_length: _content_length,
            content_language,
            content_type,
            content_md5,
            expires,
            object_lock_legal_hold_status,
            object_lock_mode,
            object_lock_retain_until_date,
            server_side_encryption,
            sse_customer_algorithm,
            sse_customer_key,
            sse_customer_key_md5,
            ssekms_key_id,
            storage_class,
            tagging,
            website_redirect_location,
            ..
        } = input;

        let event_version_id = version_id;
        let (h_algo, h_key, h_md5) = extract_ssec_params_from_headers(&request_context.headers)?;
        let sse_customer_algorithm = sse_customer_algorithm.or(h_algo);
        let sse_customer_key = sse_customer_key.or(h_key);
        let sse_customer_key_md5 = sse_customer_key_md5.or(h_md5);

        let original_sse = server_side_encryption.or(extract_server_side_encryption_from_headers(&request_context.headers)?);
        let bucket_sse_config = metadata_sys::get_sse_config(&bucket).await.ok();
        let mut effective_sse = original_sse.or_else(|| {
            bucket_sse_config.as_ref().and_then(|(config, _timestamp)| {
                config.rules.first().and_then(|rule| {
                    rule.apply_server_side_encryption_by_default
                        .as_ref()
                        .map(|sse| match sse.sse_algorithm.as_str() {
                            "AES256" => ServerSideEncryption::from_static(ServerSideEncryption::AES256),
                            "aws:kms" => ServerSideEncryption::from_static(ServerSideEncryption::AWS_KMS),
                            _ => ServerSideEncryption::from_static(ServerSideEncryption::AES256),
                        })
                })
            })
        });
        let mut effective_kms_key_id = ssekms_key_id.or_else(|| {
            bucket_sse_config.as_ref().and_then(|(config, _timestamp)| {
                config.rules.first().and_then(|rule| {
                    rule.apply_server_side_encryption_by_default
                        .as_ref()
                        .and_then(|sse| sse.kms_master_key_id.clone())
                })
            })
        });
        if effective_sse
            .as_ref()
            .is_some_and(|sse| sse.as_str().eq_ignore_ascii_case(ServerSideEncryption::AWS_KMS))
        {
            return Err(s3_error!(NotImplemented, "SSE-KMS is not supported for extract uploads"));
        }
        validate_sse_headers_for_write(
            effective_sse.as_ref(),
            effective_kms_key_id.as_ref(),
            sse_customer_algorithm.as_ref(),
            sse_customer_key.as_ref(),
            sse_customer_key_md5.as_ref(),
            true,
        )?;
        let Some(body) = body else { return Err(s3_error!(IncompleteBody)) };

        let size = resolved_size;
        validate_object_key(&key, "PUT")?;

        let buffer_size = get_buffer_size_opt_in(size);
        let body = tokio::io::BufReader::with_capacity(
            buffer_size,
            StreamReader::new(body.map(|f| f.map_err(|e| std::io::Error::other(e.to_string())))),
        );

        let Some(ext) = Path::new(&key).extension().and_then(|s| s.to_str()) else {
            return Err(s3_error!(InvalidArgument, "key extension not found"));
        };

        let ext = ext.to_owned();

        let md5hex = if let Some(base64_md5) = content_md5 {
            let md5 = base64_simd::STANDARD
                .decode_to_vec(base64_md5.as_bytes())
                .map_err(|e| ApiError::from(StorageError::other(format!("Invalid content MD5: {e}"))))?;
            Some(hex_simd::encode_to_string(&md5, hex_simd::AsciiCase::Lower))
        } else {
            None
        };

        let sha256hex = get_content_sha256_with_query(&request_context.headers, request_context.uri_query.as_deref());
        let actual_size = size;

        let mut archive_reader =
            HashReader::from_stream(body, size, actual_size, md5hex, sha256hex, false).map_err(ApiError::from)?;

        if let Err(err) =
            archive_reader.add_checksum_from_s3s(&request_context.headers, request_context.trailing_headers.clone(), false)
        {
            return Err(ApiError::from(err).into());
        }

        let archive_etag = Arc::new(Mutex::new(None));
        let decoder = CompressionFormat::from_extension(&ext)
            .get_decoder(ExtractArchiveEtagReader::new(archive_reader, archive_etag.clone()))
            .map_err(|e| {
                error!("get_decoder err {:?}", e);
                s3_error!(InvalidArgument, "get_decoder err")
            })?;

        let mut ar = Archive::new(decoder);
        let mut entries = ar.entries().map_err(|e| {
            error!("get entries err {:?}", e);
            s3_error!(InvalidArgument, "get entries err")
        })?;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let extract_options = resolve_put_object_extract_options(&request_context.headers);
        let version_id = match event_version_id {
            Some(v) => v.to_string(),
            None => String::new(),
        };

        let req_params = extract_params_header(&request_context.headers);
        let host = get_request_host(&request_context.headers);
        let port = get_request_port(&request_context.headers);
        let user_agent = get_request_user_agent(&request_context.headers);
        let tracing_context = request_context
            .extensions
            .get::<crate::storage::request_context::RequestContext>()
            .cloned();

        while let Some(entry) = entries.next().await {
            let mut f = match entry {
                Ok(f) => f,
                Err(e) => {
                    if extract_options.ignore_errors {
                        warn!("Skipping archive entry because read failed and ignore-errors is enabled: {e}");
                        continue;
                    }
                    error!("Failed to read archive entry: {}", e);
                    return Err(s3_error!(InvalidArgument, "Failed to read archive entry: {:?}", e));
                }
            };

            let fpath = match f.path() {
                Ok(path) => path,
                Err(e) => {
                    if extract_options.ignore_errors {
                        warn!("Skipping archive entry because path decode failed and ignore-errors is enabled: {e}");
                        continue;
                    }
                    return Err(s3_error!(InvalidArgument, "Failed to decode archive entry path"));
                }
            };

            let is_dir = f.header().entry_type().is_dir();
            let fpath = normalize_extract_entry_key(&fpath.to_string_lossy(), extract_options.prefix.as_deref(), is_dir);

            authorize_extract_put_target(&request_context, &bucket, &fpath).await?;

            let mut size = f.header().size().unwrap_or_default() as i64;
            let archive_entry_mod_time = f
                .header()
                .mtime()
                .ok()
                .and_then(|modified_at_secs| OffsetDateTime::from_unix_timestamp(modified_at_secs as i64).ok());
            let mut metadata = HashMap::new();
            apply_put_request_metadata(
                &mut metadata,
                &request_context.headers,
                &fpath,
                cache_control.clone(),
                content_disposition.clone(),
                content_encoding.clone(),
                content_language.clone(),
                content_type.clone(),
                expires.clone(),
                website_redirect_location.clone(),
                tagging.clone(),
                storage_class.clone(),
            )?;
            let mut opts = put_opts(&bucket, &fpath, None, &request_context.headers, metadata.clone())
                .await
                .map_err(ApiError::from)?;
            apply_extract_entry_pax_extensions(&mut f, &mut metadata, &mut opts).await?;
            if archive_entry_mod_time.is_some() {
                opts.mod_time = archive_entry_mod_time;
            }

            debug!("Extracting file: {}, size: {} bytes", fpath, size);

            if is_dir {
                if extract_options.ignore_dirs {
                    debug!("Skipping directory entry during archive extract: {}", fpath);
                    continue;
                }
                size = 0;
            }

            let actual_size = size;
            let should_compress = !is_dir && is_compressible(&HeaderMap::new(), &fpath) && size > MIN_COMPRESSIBLE_SIZE as i64;

            let mut hrd = if is_dir {
                HashReader::from_stream(std::io::Cursor::new(Vec::new()), size, actual_size, None, None, false)
                    .map_err(ApiError::from)?
            } else if should_compress {
                insert_str(&mut metadata, SUFFIX_COMPRESSION, CompressionAlgorithm::default().to_string());
                insert_str(&mut metadata, SUFFIX_ACTUAL_SIZE, size.to_string());

                let hrd = HashReader::from_stream(f, size, actual_size, None, None, false).map_err(ApiError::from)?;
                size = HashReader::SIZE_PRESERVE_LAYER;
                HashReader::from_reader(
                    CompressReader::new(hrd, CompressionAlgorithm::default()),
                    size,
                    actual_size,
                    None,
                    None,
                    false,
                )
                .map_err(ApiError::from)?
            } else {
                HashReader::from_stream(f, size, actual_size, None, None, false).map_err(ApiError::from)?
            };
            apply_put_request_object_lock_opts(
                &bucket,
                object_lock_legal_hold_status.clone(),
                object_lock_mode.clone(),
                object_lock_retain_until_date.clone(),
                &mut opts,
            )
            .await?;
            if let Some(material) = sse_encryption(EncryptionRequest {
                bucket: &bucket,
                key: &fpath,
                server_side_encryption: effective_sse.clone(),
                ssekms_key_id: effective_kms_key_id.clone(),
                sse_customer_algorithm: sse_customer_algorithm.clone(),
                sse_customer_key: sse_customer_key.clone(),
                sse_customer_key_md5: sse_customer_key_md5.clone(),
                content_size: actual_size,
                part_number: None,
                part_key: None,
                part_nonce: None,
            })
            .await?
            {
                effective_sse = Some(material.server_side_encryption.clone());
                effective_kms_key_id = material.kms_key_id.clone();

                let encrypted_reader = material.wrap_reader(hrd);
                hrd = HashReader::from_reader(encrypted_reader, HashReader::SIZE_PRESERVE_LAYER, actual_size, None, None, false)
                    .map_err(ApiError::from)?;

                let encryption_metadata = material.metadata;
                metadata.extend(encryption_metadata.clone());
                opts.user_defined.extend(encryption_metadata);
            }
            opts.user_defined.extend(metadata);
            let capacity_scope_token = Uuid::new_v4();
            opts.capacity_scope_token = Some(capacity_scope_token);
            let mut reader = rustfs_ecstore::store_api::ChunkNativePutData::new(hrd);

            let obj_info = match store.put_object(&bucket, &fpath, &mut reader, &opts).await {
                Ok(info) => info,
                Err(e) => {
                    if extract_options.ignore_errors {
                        warn!("Skipping archive entry because object write failed and ignore-errors is enabled: {e}");
                        continue;
                    }
                    return Err(ApiError::from(e).into());
                }
            };
            record_capacity_write_with_scope_token(Some(capacity_scope_token)).await;

            let e_tag = obj_info.etag.clone().map(|etag| to_s3s_etag(&etag));

            let output = PutObjectOutput {
                e_tag,
                ..Default::default()
            };

            spawn_put_extract_notification(
                notify.clone(),
                tracing_context.clone(),
                bucket.clone(),
                req_params.clone(),
                version_id.clone(),
                host.clone(),
                port,
                user_agent.clone(),
                obj_info.clone(),
                output,
            );
        }

        let mut checksums = PutObjectChecksums {
            crc32: input.checksum_crc32,
            crc32c: input.checksum_crc32c,
            sha1: input.checksum_sha1,
            sha256: input.checksum_sha256,
            crc64nvme: input.checksum_crc64nvme,
        };
        apply_trailing_checksums(
            input.checksum_algorithm.as_ref().map(|a| a.as_str()),
            &request_context.trailing_headers,
            &mut checksums,
        );

        drop(entries);
        let mut decoder = match ar.into_inner() {
            Ok(decoder) => decoder,
            Err(_) => return Err(s3_error!(InvalidArgument, "Failed to finalize archive reader")),
        };
        tokio::io::copy(&mut decoder, &mut tokio::io::sink())
            .await
            .map_err(map_extract_archive_error)?;
        let archive_etag = archive_etag
            .lock()
            .ok()
            .and_then(|etag| etag.clone())
            .map(|etag| to_s3s_etag(&etag));

        let output = PutObjectOutput {
            e_tag: archive_etag,
            checksum_crc32: checksums.crc32,
            checksum_crc32c: checksums.crc32c,
            checksum_sha1: checksums.sha1,
            checksum_sha256: checksums.sha256,
            checksum_crc64nvme: checksums.crc64nvme,
            ..Default::default()
        };
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{Extensions, HeaderMap, HeaderValue, Method, Uri};
    use rustfs_utils::http::headers::{AMZ_SERVER_SIDE_ENCRYPTION, AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID, AMZ_SNOWBALL_EXTRACT};

    fn build_request<T>(input: T, method: Method) -> S3Request<T> {
        S3Request {
            input,
            method,
            uri: Uri::from_static("/"),
            headers: HeaderMap::new(),
            extensions: Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    #[tokio::test]
    async fn execute_put_object_rejects_post_object_sse_kms_from_input() {
        let input = PutObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .server_side_encryption(Some(ServerSideEncryption::from_static(ServerSideEncryption::AWS_KMS)))
            .build()
            .unwrap();

        let mut req = build_request(input, Method::POST);
        req.extensions.insert(PostObjectRequestMarker);

        let usecase = DefaultObjectUsecase::without_context();
        let fs = FS::new();

        let err = usecase.execute_put_object(&fs, req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::NotImplemented);
    }

    #[tokio::test]
    async fn execute_put_object_rejects_extract_sse_kms() {
        let input = PutObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("archive.tar".to_string())
            .server_side_encryption(Some(ServerSideEncryption::from_static(ServerSideEncryption::AWS_KMS)))
            .build()
            .unwrap();

        let mut req = build_request(input, Method::PUT);
        req.headers.insert(AMZ_SNOWBALL_EXTRACT, HeaderValue::from_static("true"));

        let usecase = DefaultObjectUsecase::without_context();
        let fs = FS::new();

        let err = usecase.execute_put_object(&fs, req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::NotImplemented);
    }

    #[tokio::test]
    async fn execute_put_object_extract_rejects_invalid_storage_class() {
        let input = PutObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("archive.tar".to_string())
            .storage_class(Some(StorageClass::from_static("INVALID")))
            .build()
            .unwrap();

        let mut req = build_request(input, Method::PUT);
        req.headers.insert(AMZ_SNOWBALL_EXTRACT, HeaderValue::from_static("true"));

        let usecase = DefaultObjectUsecase::without_context();
        let fs = FS::new();

        let err = usecase.execute_put_object(&fs, req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidStorageClass);
    }

    #[tokio::test]
    async fn execute_put_object_rejects_post_object_sse_kms_from_headers() {
        let input = PutObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let mut req = build_request(input, Method::POST);
        req.extensions.insert(PostObjectRequestMarker);
        req.headers
            .insert(AMZ_SERVER_SIDE_ENCRYPTION, HeaderValue::from_static("aws:kms"));

        let usecase = DefaultObjectUsecase::without_context();
        let fs = FS::new();

        let err = usecase.execute_put_object(&fs, req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::NotImplemented);
    }

    #[tokio::test]
    async fn execute_put_object_rejects_post_object_sse_kms_key_id_header() {
        let input = PutObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let mut req = build_request(input, Method::POST);
        req.extensions.insert(PostObjectRequestMarker);
        req.headers
            .insert(AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID, HeaderValue::from_static("test-kms-key-id"));

        let usecase = DefaultObjectUsecase::without_context();
        let fs = FS::new();

        let err = usecase.execute_put_object(&fs, req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::NotImplemented);
    }
}
