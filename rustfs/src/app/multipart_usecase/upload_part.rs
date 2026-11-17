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

//! UploadPart.

use super::*;
use crate::app::RequestEnvelope;
use crate::app::object::BodyChunks;
use crate::app::object::request_body::{DemandReader, UploadChunks, guard_native_upload_progress};
use crate::app::storage_api::multipart_usecase::access::AuthorizationRequest;
use crate::app::storage_api::multipart_usecase::contract::multipart::MultipartInfo;
use http_body::Body as _;
use rustfs_gateway_stream::Body;
#[cfg(any(feature = "gateway-stack", test))]
use rustfs_gateway_types::{RecordedUpload, resolve_upload_async};
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result, s3_error};

#[cfg(any(feature = "gateway-stack", test))]
struct PartUpload(MultipartInfo);

#[cfg(any(feature = "gateway-stack", test))]
impl RecordedUpload for PartUpload {
    fn bucket(&self) -> &str {
        &self.0.bucket
    }

    fn key(&self) -> &str {
        &self.0.object
    }
}

async fn lookup_part_upload(store: &ECStore, bucket: &str, key: &str, id: &str, opts: &ObjectOptions) -> S3Result<MultipartInfo> {
    let result = store.get_multipart_info(bucket, key, id, opts).await;
    #[cfg(test)]
    let result = tests::observe_lookup(result);
    result.map_err(|error| multipart_upload_api_error(error).into())
}

fn resolve_upload_part_size(content_length: Option<i64>, body: Option<&Body>) -> rustfs_s3_types::S3Result<Option<i64>> {
    if let Some(length) = content_length {
        return Ok(Some(length));
    }
    body.and_then(|body| body.size_hint().exact())
        .map(i64::try_from)
        .transpose()
        .map_err(|_| rustfs_s3_types::s3_error!(UnexpectedContent))
}

fn require_upload_part_size(size: Option<i64>, capped: bool) -> rustfs_s3_types::S3Result<i64> {
    match size {
        Some(size) if size >= 0 => Ok(size),
        Some(_) => Err(rustfs_s3_types::s3_error!(UnexpectedContent)),
        None if capped => Err(rustfs_s3_types::s3_error!(UnexpectedContent)),
        None => Err(rustfs_s3_types::S3Error::new(rustfs_s3_types::S3ErrorCode::MissingContentLength)),
    }
}

fn upload_part_body_read_timeout(configured: Duration, capped: bool) -> Duration {
    if capped {
        configured.max(Duration::from_secs(rustfs_config::DEFAULT_HTTP_REQUEST_BODY_READ_TIMEOUT))
    } else {
        configured
    }
}

/// Plaintext length of one SSE-C passthrough part, declared by the sender on
/// UploadPart (backlog#2363).
fn passthrough_part_actual_size(headers: &HeaderMap) -> Option<i64> {
    get_header(headers, rustfs_utils::http::SUFFIX_REPLICATION_PART_ACTUAL_SIZE)?
        .parse::<i64>()
        .ok()
        .filter(|size| *size > 0)
}

// Hoisted values are signed query parameters, not HTTP headers. Normalize only
// after access control has verified the presigned request.
fn normalize_presigned_part_checksums(req: &mut RequestEnvelope, input: &mut UploadPartWriteInput) -> S3Result<()> {
    if req.verified_presigned().is_none() {
        return Ok(());
    }
    let uri = req.uri().clone();
    let Some(query) = uri.query() else {
        return Ok(());
    };
    let mut seen = std::collections::HashSet::new();
    for (name, value) in url::form_urlencoded::parse(query.as_bytes()) {
        let field = match name.as_ref() {
            "x-amz-checksum-crc32" => Some(&mut input.checksum_crc32),
            "x-amz-checksum-crc32c" => Some(&mut input.checksum_crc32c),
            "x-amz-checksum-crc64nvme" => Some(&mut input.checksum_crc64nvme),
            "x-amz-checksum-sha1" => Some(&mut input.checksum_sha1),
            "x-amz-checksum-sha256" => Some(&mut input.checksum_sha256),
            "x-amz-sdk-checksum-algorithm" => None,
            _ => continue,
        };
        if !seen.insert(name.clone()) {
            return Err(S3Error::with_message(S3ErrorCode::InvalidRequest, "Duplicate checksum query parameter"));
        }
        if let Some(header) = req.headers().get(name.as_ref())
            && header.as_bytes() != value.as_bytes()
        {
            return Err(S3Error::with_message(
                S3ErrorCode::InvalidRequest,
                "Conflicting checksum header and query parameter",
            ));
        }
        let header = http::HeaderValue::from_str(&value)
            .map_err(|_| S3Error::with_message(S3ErrorCode::InvalidArgument, "Invalid checksum query parameter"))?;
        req.headers_mut().insert(
            http::header::HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| S3Error::with_message(S3ErrorCode::InvalidArgument, "Invalid checksum query parameter"))?,
            header,
        );
        if let Some(field) = field {
            *field = Some(value.into_owned());
        } else {
            input.checksum_algorithm = Some(value.into_owned());
        }
    }
    Ok(())
}

impl DefaultMultipartUsecase {
    #[instrument(name = "execute_upload_part", level = "debug", skip(self, req, input))]
    #[hotpath::measure(impl_type = "MultipartUsecase")]
    pub(crate) async fn execute_upload_part_owned(
        &self,
        mut req: RequestEnvelope,
        mut input: UploadPartWriteInput,
    ) -> S3Result<UploadPartWriteOutput> {
        reject_presigned_multipart_max_total_object_size_for_other_operation(
            req.headers(),
            req.uri().query(),
            req.verified_sigv4().is_some(),
        )?;
        reject_presigned_put_max_content_length_for_other_operation(
            req.headers(),
            req.uri().query(),
            req.verified_presigned().is_some(),
        )?;
        normalize_presigned_part_checksums(&mut req, &mut input)?;
        let (bucket, key) = input.target.resource();
        let mut opts = ObjectOptions::default();
        apply_bucket_generation_guard(&req, bucket, &mut opts)?;
        let part_principal = SseKmsPrincipal::from_request(&req);
        let UploadPartWriteInput {
            body,
            frame_trailers,
            trailing_headers,
            part_number,
            content_length,
            sse_customer_algorithm,
            sse_customer_key,
            sse_customer_key_md5,
            ..
        } = input;

        let part_id = parse_upload_part_number(part_number)?;

        reject_veeam_sosapi_mutation(|| self.object_store(), bucket, key).await?;
        validate_table_catalog_object_mutation(bucket, key).await?;

        // cuObject S3-over-RDMA UploadPart: an empty HTTP body plus
        // `x-amz-rdma-token` means the part bytes are read from the client's
        // registered buffer (see `object::cuobject::rdma_put_body`).
        #[cfg(all(any(feature = "cuobject", feature = "nixl"), target_os = "linux"))]
        let (body, content_length, rdma_put) = match crate::app::object::cuobject::rdma_token_for_request(req.headers()) {
            Some(token) if content_length.unwrap_or(0) == 0 => {
                let (body, total) =
                    crate::app::object::cuobject::rdma_put_body(bucket, key, &token, true).map_err(S3Error::from)?;
                let total = i64::try_from(total).map_err(|_| S3Error::new(S3ErrorCode::EntityTooLarge))?;
                (Some(crate::app::object::legacy_put_body(body)), Some(total), Some(total))
            }
            _ => (body, content_length, None),
        };
        #[cfg(not(all(any(feature = "cuobject", feature = "nixl"), target_os = "linux")))]
        let rdma_put: Option<i64> = None;
        let size = resolve_upload_part_size(content_length, body.as_ref())?;
        if let Some(size) = size {
            reject_oversize_single_upload(size)?;
        }
        let body_stream = body.ok_or_else(|| s3_error!(IncompleteBody))?;
        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };
        let multipart_info_started = rustfs_io_metrics::put_stage_timer();
        let (upload_id, fi) = match &input.target {
            UploadPartTarget::Legacy { upload_id, .. } => {
                let info = lookup_part_upload(&store, bucket, key, upload_id, &opts).await?;
                (upload_id.clone(), info)
            }
            #[cfg(any(feature = "gateway-stack", test))]
            UploadPartTarget::Gateway {
                bucket: bucket_name,
                key: object_key,
                upload_id,
            } => {
                let lookup_store = &store;
                let lookup_opts = &opts;
                let (id, upload) = resolve_upload_async(upload_id, bucket_name, object_key, |id| async move {
                    lookup_part_upload(lookup_store, bucket, key, id, lookup_opts)
                        .await
                        .map(|info| Some(PartUpload(info)))
                })
                .await?
                .map_err(|rejection| S3Error::with_message(S3ErrorCode::NoSuchUpload, rejection.reason()))?;
                (id.id().to_owned(), upload.0)
            }
        };
        rustfs_io_metrics::record_put_object_stage_duration_from("multipart_get_info", multipart_info_started);
        authorize_sse_kms_multipart_write(part_principal.as_ref(), &fi.user_defined).await?;
        let max_total_object_size = multipart_max_total_object_size(&fi.user_defined)?;
        let mut size = require_upload_part_size(size, max_total_object_size.is_some())?;
        if let Some(limit) = max_total_object_size
            && u64::try_from(size).is_ok_and(|size| size > limit)
        {
            return Err(S3Error::new(S3ErrorCode::EntityTooLarge));
        }
        let admission_wait_started = rustfs_io_metrics::put_stage_timer();
        let admission = self.concurrency_manager().admit_multipart_part(size).await;
        rustfs_io_metrics::record_put_object_stage_duration_from("multipart_admission_wait", admission_wait_started);
        let upload_part_admission = match admission
            .map_err(|_| S3Error::with_message(S3ErrorCode::InternalError, "foreground write admission closed"))?
        {
            ForegroundWriteAdmission::Disabled => None,
            ForegroundWriteAdmission::Admitted(permit) => {
                counter!("rustfs.upload_part.foreground_admission.total", "result" => "admitted").increment(1);
                Some(permit)
            }
            ForegroundWriteAdmission::Rejected => {
                counter!("rustfs.upload_part.foreground_admission.total", "result" => "rejected").increment(1);
                return Err(S3Error::with_message(
                    S3ErrorCode::SlowDown,
                    "foreground write concurrency limit reached, please reduce your request rate",
                ));
            }
        };
        let (chunks, _) = BodyChunks::with_trailers(body_stream, frame_trailers);
        let body_stream: UploadChunks = Box::new(chunks);
        let request_id = req.request_context().map(|ctx| ctx.request_id.as_str()).unwrap_or_default();
        let timeout = upload_part_body_read_timeout(put_object_body_read_timeout(), max_total_object_size.is_some());
        let (body_stream, progress_control) = if let Some(control) = req.body_read_control() {
            control.activate_upload(timeout, bucket, key, request_id, size.max(0) as u64);
            (body_stream, control.clone())
        } else {
            guard_native_upload_progress(body_stream, timeout, bucket, key, request_id, size.max(0) as u64)
        };

        let ingress_stage_start = rustfs_io_metrics::put_stage_metrics_enabled().then(std::time::Instant::now);

        // Apply adaptive buffer sizing based on part size for optimal streaming performance.
        // Uses workload profile configuration (enabled by default) to select appropriate buffer size.
        // Buffer sizes range from 32KB to 4MB depending on part size and configured workload profile.
        let buffer_size = get_buffer_size_opt_in(size);
        let body = tokio::io::BufReader::with_capacity(
            buffer_size,
            StreamReader::new(body_stream.map(|f| f.map_err(s3s_body_error_to_io))),
        );

        // An SSE-C passthrough session stores ciphertext parts verbatim: a
        // compression key restored on the session describes those stored
        // bytes and must not add a second compression layer, and each part's
        // plaintext length comes from the sender (backlog#2363).
        let preserve_ciphertext = contains_key_str(&fi.user_defined, SUFFIX_REPLICATION_PRESERVE_CIPHERTEXT);
        let is_disk_compressed = !preserve_ciphertext
            && rustfs_utils::http::contains_key_str(&fi.user_defined, rustfs_utils::http::SUFFIX_COMPRESSION);

        let actual_size = if preserve_ciphertext {
            passthrough_part_actual_size(req.headers()).unwrap_or(size)
        } else {
            size
        };

        let mut md5hex = if let Some(base64_md5) = input.content_md5 {
            let md5 = base64_simd::STANDARD
                .decode_to_vec(base64_md5.as_bytes())
                .map_err(|e| ApiError::from(StorageError::other(format!("Invalid content MD5: {e}"))))?;
            Some(hex_simd::encode_to_string(&md5, hex_simd::AsciiCase::Lower))
        } else {
            None
        };

        let mut sha256hex = get_content_sha256_with_query(req.headers(), req.uri().query());

        let mut write_plan = WritePlan::new();
        let etag_algorithm = multipart_etag_algorithm(&fi.user_defined);
        let mut reader = if is_disk_compressed {
            let algorithm = CompressionAlgorithm::default();
            let mut hrd = HashReader::from_stream_with_etag(
                body,
                size,
                actual_size,
                md5hex.take(),
                sha256hex.take(),
                false,
                etag_algorithm,
            )
            .map_err(ApiError::from)?;

            if let Err(err) = hrd.add_checksum(req.headers(), trailing_headers.clone(), false) {
                return Err(ApiError::from(err).into());
            }

            size = HashReader::SIZE_PRESERVE_LAYER;
            write_plan = write_plan.with_compression(algorithm);
            hrd
        } else {
            HashReader::from_stream_with_etag(body, size, actual_size, md5hex, sha256hex, false, etag_algorithm)
                .map_err(ApiError::from)?
        };

        if let Err(err) = reader.add_checksum(req.headers(), trailing_headers.clone(), size < 0) {
            return Err(ApiError::from(err).into());
        }
        opts.want_checksum = reader.checksum();

        // An SSE-C passthrough session stores ciphertext parts verbatim: no
        // material recovery, no validation against the (absent) customer key.
        let has_ssec = !preserve_ciphertext
            && fi
                .user_defined
                .contains_key("x-amz-server-side-encryption-customer-algorithm");
        let (server_side_encryption, ssekms_key_id) = if has_ssec || preserve_ciphertext {
            (None, None)
        } else {
            let sse = fi.user_defined.get("x-amz-server-side-encryption").cloned();
            let key_id = match sse.as_ref() {
                Some(sse) if sse.as_str() == "aws:kms" => fi
                    .user_defined
                    .get("x-amz-server-side-encryption-aws-kms-key-id")
                    .map(|s| s.to_string()),
                _ => None,
            };
            (sse, key_id)
        };
        if !preserve_ciphertext {
            EncryptionRequest {
                bucket,
                key,
                server_side_encryption: server_side_encryption.clone(),
                ssekms_key_id: ssekms_key_id.clone(),
                ssekms_context: None,
                sse_customer_algorithm: sse_customer_algorithm.clone(),
                sse_customer_key: sse_customer_key.clone(),
                sse_customer_key_md5: sse_customer_key_md5.clone(),
                content_size: actual_size,
                principal: None,
            }
            .validate_multipart_ssec(&fi.user_defined)?;
        }
        let (requested_sse, requested_kms_key_id) = if has_ssec {
            let ssec_material = sse_decryption(DecryptionRequest {
                bucket,
                key,
                metadata: &fi.user_defined,
                sse_customer_key: sse_customer_key.as_ref(),
                sse_customer_key_md5: sse_customer_key_md5.as_ref(),
                principal: None,
            })
            .await?
            .ok_or_else(|| ApiError::from(StorageError::other("Missing SSE-C session material")))?;
            let ssec_write = match ssec_material.key_kind {
                EncryptionKeyKind::Object => WriteEncryption::multipart_object_key(ssec_material.key_bytes, part_id as u32),
                EncryptionKeyKind::Direct => {
                    WriteEncryption::multipart(ssec_material.key_bytes, ssec_material.base_nonce, part_id)
                }
            };
            write_plan = write_plan.with_encryption(ssec_write);
            (Some(ssec_material.server_side_encryption), ssec_material.kms_key_id)
        } else if let Some(server_side_encryption) = server_side_encryption {
            // Reuse the stored envelope after authorizing this caller against its
            // pinned destination key above.
            let managed_material = sse_decryption(DecryptionRequest {
                bucket,
                key,
                metadata: &fi.user_defined,
                sse_customer_key: None,
                sse_customer_key_md5: None,
                principal: None,
            })
            .await?
            .ok_or_else(|| ApiError::from(StorageError::other("Missing managed SSE session material")))?;
            let managed_write = match managed_material.key_kind {
                EncryptionKeyKind::Object => WriteEncryption::multipart_object_key(managed_material.key_bytes, part_id as u32),
                EncryptionKeyKind::Direct => {
                    WriteEncryption::multipart(managed_material.key_bytes, managed_material.base_nonce, part_id)
                }
            };
            write_plan = write_plan.with_encryption(managed_write);
            (Some(server_side_encryption), ssekms_key_id)
        } else {
            (None, None)
        };

        reader = write_plan.apply(reader, actual_size).map_err(ApiError::from)?;

        {
            use rustfs_rio::HashReaderMut;
            let inner = reader.take_inner();
            reader.inner = rustfs_rio::boxed_reader(DemandReader::new(inner, progress_control));
        }

        let mut reader = PutObjReader::new(reader);

        if let Some(stage_start) = ingress_stage_start {
            rustfs_io_metrics::record_put_object_stage_duration(
                "multipart_ingress_prepare",
                stage_start.elapsed().as_secs_f64() * 1000.0,
            );
        }

        let _upload_part_admission = upload_part_admission;
        let store_write_started = rustfs_io_metrics::put_stage_timer();
        let result = store
            .put_object_part(bucket, key, &upload_id, part_id, &mut reader, &opts)
            .await;
        rustfs_io_metrics::record_put_object_stage_duration_from("multipart_store_write", store_write_started);
        drop(_upload_part_admission);
        let info = result.map_err(ApiError::from)?;

        let mut checksum_crc32 = input.checksum_crc32;
        let mut checksum_crc32c = input.checksum_crc32c;
        let mut checksum_sha1 = input.checksum_sha1;
        let mut checksum_sha256 = input.checksum_sha256;
        let mut checksum_crc64nvme = input.checksum_crc64nvme;

        if let (Some(algorithm), Some(trailers)) = (input.checksum_algorithm.as_deref(), trailing_headers) {
            let echo = match algorithm {
                "CRC32" => Some(("x-amz-checksum-crc32", &mut checksum_crc32)),
                "CRC32C" => Some(("x-amz-checksum-crc32c", &mut checksum_crc32c)),
                "SHA1" => Some(("x-amz-checksum-sha1", &mut checksum_sha1)),
                "SHA256" => Some(("x-amz-checksum-sha256", &mut checksum_sha256)),
                "CRC64NVME" => Some(("x-amz-checksum-crc64nvme", &mut checksum_crc64nvme)),
                _ => None,
            };
            if let Some((name, field)) = echo {
                match trailers.lookup(name) {
                    rustfs_rio::TrailerValue::Present(value) => *field = Some(value),
                    rustfs_rio::TrailerValue::Missing => *field = None,
                    rustfs_rio::TrailerValue::Pending => {}
                }
            }
        }

        // XXHash3/64/128 and SHA-512 have no typed UploadPartOutput field; echo the
        // server-computed part checksum as a raw response header (#1261).
        #[cfg_attr(
            not(all(any(feature = "cuobject", feature = "nixl"), target_os = "linux")),
            allow(unused_mut, reason = "mutated only by the cuObject RDMA branch")
        )]
        let mut upload_part_extra_checksum_headers =
            crate::app::object_usecase::additional_checksum_echo_pairs(&opts.want_checksum);
        #[cfg(not(all(any(feature = "cuobject", feature = "nixl"), target_os = "linux")))]
        let _ = rdma_put;
        #[cfg(all(any(feature = "cuobject", feature = "nixl"), target_os = "linux"))]
        if let Some(moved) = rdma_put {
            upload_part_extra_checksum_headers.push((crate::app::object::cuobject::RDMA_REPLY_HEADER, "200".to_owned()));
            upload_part_extra_checksum_headers
                .push((crate::app::object::cuobject::RDMA_BYTES_TRANSFERRED_HEADER, moved.to_string()));
        }

        let mut headers = HeaderMap::new();
        crate::app::object_usecase::inject_additional_checksum_headers(&mut headers, &upload_part_extra_checksum_headers);
        Ok(UploadPartWriteOutput {
            server_side_encryption: requested_sse,
            ssekms_key_id: requested_kms_key_id,
            sse_customer_algorithm,
            sse_customer_key_md5,
            checksum_crc32,
            checksum_crc32c,
            checksum_sha1,
            checksum_sha256,
            checksum_crc64nvme,
            e_tag: info.etag,
            headers,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::*;
    use super::*;
    use crate::app::object::legacy_put_body;
    use crate::app::storage_api::s3::S3ErrorCode as LegacyErrorCode;
    use crate::app::storage_api::s3::dto::{StreamingBlob, UploadPartInput};

    fn presigned_checksum_request(query: &str) -> (RequestEnvelope, UploadPartWriteInput) {
        let mut req = build_request(UploadPartInput::default(), Method::PUT);
        req.uri = format!("/bucket/object?{query}").parse().unwrap();
        req.extensions.insert(VerifiedPresignedRequest);
        (RequestEnvelope::from_s3s(&req), UploadPartWriteInput::from_legacy(req.input, None))
    }

    #[tokio::test]
    async fn normalize_presigned_part_checksums_validates_body() {
        // SHA256("abc"), including percent-encoded base64 punctuation.
        let checksum = "ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0=";
        let query =
            "x-amz-checksum-sha256=ungWv48Bz%2BpBQUDeXa4iI7ADYaOWF3qctBD%2FYfIAFa0%3D&x-amz-sdk-checksum-algorithm=SHA256";
        for payload in [b"abc", b"abd"] {
            let (mut req, mut input) = presigned_checksum_request(query);
            normalize_presigned_part_checksums(&mut req, &mut input).unwrap();
            assert_eq!(input.checksum_sha256.as_deref(), Some(checksum));
            assert_eq!(input.checksum_algorithm.as_deref(), Some("SHA256"));
            let mut reader = HashReader::from_stream(Cursor::new(payload), 3, 3, None, None, false).unwrap();
            reader.add_checksum(req.headers(), None, false).unwrap();
            assert_eq!(reader.content_crc_type(), Some(rustfs_rio::ChecksumType::SHA256));
            let mut bytes = Vec::new();
            let result = reader.read_to_end(&mut bytes).await;
            if payload == b"abc" {
                result.unwrap();
                assert_eq!(bytes, payload);
                assert_eq!(reader.content_crc().get("SHA256").map(String::as_str), Some(checksum));
            } else {
                let err = S3Error::from(ApiError::from(result.unwrap_err()));
                assert_eq!(*err.code(), S3ErrorCode::BadDigest);
            }
        }
    }

    #[tokio::test]
    async fn normalize_presigned_part_checksums_accepts_sdk_crc32_default() {
        let (mut req, mut input) =
            presigned_checksum_request("x-amz-checksum-crc32=y%2FQ5Jg%3D%3D&x-amz-sdk-checksum-algorithm=CRC32");
        normalize_presigned_part_checksums(&mut req, &mut input).unwrap();
        assert_eq!(input.checksum_crc32.as_deref(), Some("y/Q5Jg=="));
        let mut reader = HashReader::from_stream(Cursor::new(b"123456789"), 9, 9, None, None, false).unwrap();
        reader.add_checksum(req.headers(), None, false).unwrap();
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, b"123456789");
        assert_eq!(reader.content_crc().get("CRC32").map(String::as_str), Some("y/Q5Jg=="));
    }

    #[test]
    fn normalize_presigned_part_checksums_rejects_ambiguous_values() {
        for query in [
            "x-amz-checksum-sha256=one&x-amz-checksum-sha256=two",
            "x-amz-sdk-checksum-algorithm=SHA256&x-amz-sdk-checksum-algorithm=CRC32",
        ] {
            let (mut req, mut input) = presigned_checksum_request(query);
            assert_eq!(
                *normalize_presigned_part_checksums(&mut req, &mut input).unwrap_err().code(),
                S3ErrorCode::InvalidRequest
            );
        }
        let (mut req, mut input) = presigned_checksum_request("x-amz-checksum-sha256=one");
        req.headers_mut()
            .insert("x-amz-checksum-sha256", HeaderValue::from_static("two"));
        assert_eq!(
            *normalize_presigned_part_checksums(&mut req, &mut input).unwrap_err().code(),
            S3ErrorCode::InvalidRequest
        );
    }

    #[test]
    fn normalize_presigned_part_checksums_preserves_headers_and_rejects_invalid_values() {
        let (mut req, mut input) = presigned_checksum_request("partNumber=1&uploadId=test");
        input.checksum_sha256 = Some("existing".to_owned());
        req.headers_mut()
            .insert("x-amz-checksum-sha256", HeaderValue::from_static("existing"));
        normalize_presigned_part_checksums(&mut req, &mut input).unwrap();
        assert_eq!(input.checksum_sha256.as_deref(), Some("existing"));
        assert_eq!(req.headers()["x-amz-checksum-sha256"], "existing");

        let (mut req, mut input) = presigned_checksum_request("x-amz-checksum-sha256=%0D%0A");
        assert_eq!(
            *normalize_presigned_part_checksums(&mut req, &mut input).unwrap_err().code(),
            S3ErrorCode::InvalidArgument
        );
    }

    #[test]
    fn normalize_presigned_part_checksums_requires_verified_presign() {
        let mut legacy = build_request(UploadPartInput::default(), Method::PUT);
        legacy.uri = "/bucket/object?x-amz-checksum-sha256=one".parse().unwrap();
        let mut req = RequestEnvelope::from_s3s(&legacy);
        let mut input = UploadPartWriteInput::from_legacy(legacy.input, None);
        normalize_presigned_part_checksums(&mut req, &mut input).unwrap();
        assert!(input.checksum_sha256.is_none());
        assert!(req.headers().is_empty());
    }

    #[test]
    fn resolve_upload_part_size_uses_normalized_logical_length() {
        let result: rustfs_s3_types::S3Result<Option<i64>> = resolve_upload_part_size(Some(5242880), None);
        assert_eq!(result.expect("DTO length"), Some(5242880));
        assert_eq!(resolve_upload_part_size(None, None).expect("unknown length"), None);
        let body = Body::from(Bytes::from_static(b"abc"));
        assert_eq!(resolve_upload_part_size(None, Some(&body)).expect("exact bytes"), Some(3));
        assert_eq!(resolve_upload_part_size(Some(0), Some(&body)).expect("explicit length wins"), Some(0));
        let body = StreamingBlob::wrap(futures::stream::iter([Ok::<_, std::io::Error>(Bytes::from_static(b"abc"))]));
        assert_eq!(futures::Stream::size_hint(&body), (1, Some(1)), "the stream knows its item count");
        assert_eq!(
            resolve_upload_part_size(None, Some(&legacy_put_body(body))).expect("unknown byte length"),
            None
        );
    }

    #[test]
    fn upload_part_length_contract_rejects_unknown_and_all_negative_lengths() {
        let result: rustfs_s3_types::S3Result<i64> = require_upload_part_size(None, false);
        assert_eq!(
            result.expect_err("ordinary unknown").code(),
            &rustfs_s3_types::S3ErrorCode::MissingContentLength
        );
        assert_eq!(
            require_upload_part_size(None, true).expect_err("capped unknown").code(),
            &rustfs_s3_types::S3ErrorCode::UnexpectedContent
        );
        for capped in [false, true] {
            for size in [-1, -2, i64::MIN] {
                assert_eq!(
                    require_upload_part_size(Some(size), capped).expect_err("negative").code(),
                    &rustfs_s3_types::S3ErrorCode::UnexpectedContent
                );
            }
            assert_eq!(require_upload_part_size(Some(0), capped).expect("zero is valid"), 0);
        }
    }

    #[test]
    fn upload_part_timeout_policy_preserves_disabled_and_capped_floor() {
        for seconds in [0, 1, 299, 300, 601] {
            let configured = Duration::from_secs(seconds);
            assert_eq!(upload_part_body_read_timeout(configured, false), configured);
            assert_eq!(upload_part_body_read_timeout(configured, true), Duration::from_secs(seconds.max(300)));
        }
    }

    #[tokio::test]
    async fn execute_upload_part_rejects_missing_body() {
        let input = UploadPartInput::builder()
            .bucket("bucket".to_string())
            .key("object".to_string())
            .upload_id("upload-id".to_string())
            .part_number(1)
            .build()
            .unwrap();
        let req = build_request(input, Method::PUT);

        let err = make_usecase().execute_upload_part(req).await.unwrap_err();
        assert_eq!(err.code(), &LegacyErrorCode::IncompleteBody);
    }

    /// issue #7596: a part whose declared length exceeds the 5 GiB
    /// single-request ceiling is rejected before the body is polled or the
    /// store is consulted. Exact-cap and zero-length parts pass admission.
    #[tokio::test]
    async fn execute_upload_part_rejects_oversize_declared_part_before_reading_the_body() {
        let ceiling = i64::try_from(rustfs_config::MAX_SINGLE_PUT_OBJECT_SIZE).expect("ceiling fits i64");

        for (declared, expect_too_large) in [(ceiling + 1, true), (ceiling, false), (0, false)] {
            let (body, polls) = crate::app::object::PollCountingBody::streaming_blob();
            let input = UploadPartInput::builder()
                .bucket("bucket".to_string())
                .key("object".to_string())
                .upload_id("upload-id".to_string())
                .part_number(1)
                .body(Some(body))
                .content_length(Some(declared))
                .build()
                .unwrap();
            let req = build_request(input, Method::PUT);

            let err = make_usecase().execute_upload_part(req).await.unwrap_err();
            if expect_too_large {
                assert_eq!(err.code(), &LegacyErrorCode::EntityTooLarge, "declared {declared}");
                assert_eq!(polls.load(std::sync::atomic::Ordering::SeqCst), 0, "body must not be polled");
            } else {
                assert_ne!(err.code(), &LegacyErrorCode::EntityTooLarge, "declared {declared} must pass admission");
            }
        }
    }

    #[tokio::test]
    async fn execute_upload_part_rejects_invalid_part_number_before_body_lookup() {
        for part_number in [-1, 0, 10001] {
            let input = UploadPartInput::builder()
                .bucket("bucket".to_string())
                .key("object".to_string())
                .upload_id("upload-id".to_string())
                .part_number(part_number)
                .build()
                .unwrap();
            let req = build_request(input, Method::PUT);

            let err = make_usecase().execute_upload_part(req).await.unwrap_err();
            assert_eq!(err.code(), &LegacyErrorCode::InvalidArgument);
            assert_eq!(err.message(), Some("partNumber must be between 1 and 10000"));
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn execute_upload_part_rejects_unknown_length_before_admission_or_body_polling() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations, MakeBucketOptions};

        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        let ambient = crate::app::gating_test_env::shared_gating_ambient().await;
        let context = Arc::new(AppContext::new(Arc::clone(&store), ambient.iam(), ambient.kms()));
        let bucket = format!("upload-part-length-{}", Uuid::new_v4().simple());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket");
        let concurrency_manager = Arc::new(ConcurrencyManager::with_large_put_admission_for_test(
            true,
            1,
            rustfs_config::DEFAULT_PUT_LARGE_FOREGROUND_ADMISSION_MIN_SIZE_BYTES,
            Duration::ZERO,
        ));
        let held = concurrency_manager
            .admit_multipart_part(1024)
            .await
            .expect("hold the only permit");
        let usecase = DefaultMultipartUsecase::with_context_and_concurrency_manager(Some(context), concurrency_manager);

        for capped in [false, true] {
            let mut options = ObjectOptions::default();
            if capped {
                insert_str(&mut options.user_defined, SUFFIX_MAX_TOTAL_OBJECT_SIZE, "1024".to_owned());
            }
            let upload = store
                .new_multipart_upload(&bucket, "object", &options)
                .await
                .expect("upload session");
            for content_length in [None, Some(-1)] {
                for declared_chunk_encoding in [false, true] {
                    let (body, polls) = crate::app::object::PollCountingBody::streaming_blob();
                    let input = UploadPartInput::builder()
                        .bucket(bucket.clone())
                        .key("object".to_owned())
                        .upload_id(upload.upload_id.clone())
                        .part_number(1)
                        .content_length(content_length)
                        .body(Some(body))
                        .build()
                        .expect("part request");
                    let mut request = build_request(input, Method::PUT);
                    request
                        .headers
                        .insert("x-amz-decoded-content-length", HeaderValue::from_static("1024"));
                    if declared_chunk_encoding {
                        request
                            .headers
                            .insert("content-encoding", HeaderValue::from_static("aws-chunked"));
                    }
                    let error = usecase
                        .execute_upload_part(request)
                        .await
                        .expect_err("unknown or negative logical size");
                    assert_eq!(
                        error.code(),
                        &if capped || content_length.is_some() {
                            LegacyErrorCode::UnexpectedContent
                        } else {
                            LegacyErrorCode::MissingContentLength
                        }
                    );
                    assert_eq!(polls.load(std::sync::atomic::Ordering::Relaxed), 0);
                }
            }
            let parts = store
                .list_object_parts(&bucket, "object", &upload.upload_id, None, 1000, &ObjectOptions::default())
                .await
                .expect("list rejected session");
            assert!(parts.parts.is_empty());
        }
        drop(held);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn execute_upload_part_progress_timeout_releases_admission_and_leaves_no_part() {
        use crate::app::object::request_body::{BodyReadControl, ObservedBody};
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};
        use http_body::Frame;
        use http_body_util::StreamBody;
        use tokio_stream::wrappers::UnboundedReceiverStream;
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        crate::app::runtime_sources::install_test_app_context(Arc::clone(&store)).await;
        let ambient = crate::app::runtime_sources::current_app_context().expect("test app context");
        let context = Arc::new(AppContext::new(Arc::clone(&store), ambient.iam(), ambient.kms()));
        let bucket = format!("part-progress-{}", Uuid::new_v4().simple());
        store.make_bucket(&bucket, &MakeBucketOptions::default()).await.unwrap();
        let manager = Arc::new(ConcurrencyManager::with_put_admission_for_test(true, 1, Duration::ZERO));
        let usecase = DefaultMultipartUsecase::with_context_and_concurrency_manager(Some(context), Arc::clone(&manager));
        let upload = store
            .new_multipart_upload(&bucket, "object", &ObjectOptions::default())
            .await
            .unwrap();
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel::<Result<Frame<Bytes>, std::io::Error>>();
        let control = BodyReadControl::default();
        let body = StreamingBlob::from(crate::app::storage_api::s3::Body::http_body_unsync(ObservedBody::new(
            StreamBody::new(UnboundedReceiverStream::new(receiver)),
            control.clone(),
        )));
        let input = UploadPartInput::builder()
            .bucket(bucket.clone())
            .key("object".to_owned())
            .upload_id(upload.upload_id.clone())
            .part_number(1)
            .content_length(Some(65536))
            .body(Some(body))
            .build()
            .unwrap();
        let mut request = build_request(input, Method::PUT);
        request.extensions.insert(control.clone());
        let owner = tokio::spawn(async move { usecase.execute_upload_part(request).await });
        control.wait_until_pending_for_test().await;
        assert!(matches!(
            manager.admit_put_object(65536).await.unwrap(),
            ForegroundWriteAdmission::Rejected
        ));
        control.advance_wait_for_test(Duration::from_secs(60));
        sender.send(Ok(Frame::data(Bytes::from_static(b"x")))).unwrap();
        assert_eq!(owner.await.unwrap().unwrap_err().code(), &LegacyErrorCode::RequestTimeout);
        assert!(matches!(
            manager.admit_put_object(65536).await.unwrap(),
            ForegroundWriteAdmission::Admitted(_)
        ));
        let parts = store
            .list_object_parts(&bucket, "object", &upload.upload_id, None, 1000, &ObjectOptions::default())
            .await
            .unwrap();
        assert!(parts.parts.is_empty());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn execute_upload_part_rejects_when_foreground_write_admission_is_full() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations, MakeBucketOptions};

        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        let ambient = crate::app::gating_test_env::shared_gating_ambient().await;
        let context = Arc::new(AppContext::new(Arc::clone(&store), ambient.iam(), ambient.kms()));
        let bucket = format!("upload-part-admission-{}", Uuid::new_v4().simple());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create upload part admission test bucket");
        let upload = store
            .new_multipart_upload(&bucket, "object", &ObjectOptions::default())
            .await
            .expect("create multipart upload");
        let concurrency_manager = Arc::new(ConcurrencyManager::with_large_put_admission_for_test(
            true,
            1,
            rustfs_config::DEFAULT_PUT_LARGE_FOREGROUND_ADMISSION_MIN_SIZE_BYTES,
            Duration::ZERO,
        ));
        let held = concurrency_manager
            .admit_multipart_part(1024)
            .await
            .expect("first multipart part should acquire the only permit");
        let usecase =
            DefaultMultipartUsecase::with_context_and_concurrency_manager(Some(context), Arc::clone(&concurrency_manager));
        let body = StreamingBlob::wrap(futures::stream::pending::<Result<Bytes, std::io::Error>>());
        let input = UploadPartInput::builder()
            .bucket(bucket)
            .key("object".to_string())
            .upload_id(upload.upload_id)
            .part_number(1)
            .content_length(Some(1024))
            .body(Some(body))
            .build()
            .expect("upload part input should build");

        let err = usecase
            .execute_upload_part(build_request(input, Method::PUT))
            .await
            .expect_err("full foreground write admission should reject before body ingest");
        assert_eq!(err.code(), &LegacyErrorCode::SlowDown);
        drop(held);
    }

    mod body_read_tests;
    mod claim_tests;
    pub(super) use claim_tests::observe_lookup;
}
