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

//! Internal object write entry points for trusted in-process callers.
//!
//! A system write (on-demand migration write-back, future replays) must look
//! like an ordinary client write: bucket default SSE, quota, versioning,
//! Object Lock defaults, replication scheduling and creation events all apply.
//! The single-object entry point runs the same [`DefaultObjectUsecase::put_object_core`]
//! as the S3 PutObject handler; the multipart entry points mirror the S3
//! multipart handlers' policy steps against the same storage contract.

use super::*;

use crate::app::object_data_cache::invalidate_object_data_cache_after_complete_multipart_success;
use crate::app::storage_api::multipart_usecase::contract::multipart::{CompletePart, MultipartOperations as _};
use crate::app::storage_api::object_usecase::compression::is_multipart_disk_compression_enabled;
use crate::app::storage_api::object_usecase::io::WriteEncryption;
use crate::app::storage_api::object_usecase::options::{
    extract_metadata_from_mime, get_complete_multipart_upload_opts_with_replication_authorization,
};
use crate::app::storage_api::object_usecase::sse::{
    EncryptionKeyKind, PrepareEncryptionRequest, mark_encrypted_multipart_metadata, sse_decryption, sse_prepare_encryption,
};
use crate::capacity::record_capacity_write;
use crate::runtime_sources::NotifyInterface;
use http::HeaderName;

/// Inputs of an internal object write. Content and user metadata follow the
/// S3 request shape so the shared write path treats them exactly like a
/// client PUT: `content_headers` are the standard object headers
/// (`Content-Type`, `Cache-Control`, `Content-Encoding`, `Content-Disposition`,
/// `Content-Language`, `Expires`), `user_metadata` carries `x-amz-meta-*`
/// entries with the prefix stripped, `tags` is the `x-amz-tagging` query
/// string and `internal_metadata` holds `x-rustfs-internal-*` /
/// `x-minio-internal-*` keys written verbatim.
pub(crate) struct InternalPutContext {
    pub(crate) bucket: String,
    pub(crate) key: String,
    /// Plaintext object length. The single-object path requires it, exactly
    /// like S3 PutObject rejects an unknown `Content-Length`.
    pub(crate) size: Option<u64>,
    /// Lowercase hex MD5 the body must hash to; the write fails with
    /// `BadDigest` otherwise and nothing is committed.
    pub(crate) expected_md5_hex: Option<String>,
    /// ETag to store instead of the computed one.
    pub(crate) preserve_etag: Option<String>,
    pub(crate) content_headers: HashMap<String, String>,
    pub(crate) user_metadata: HashMap<String, String>,
    pub(crate) tags: Option<String>,
    pub(crate) internal_metadata: HashMap<String, String>,
    /// Publish the `s3:ObjectCreated:*` event for the write.
    pub(crate) emit_events: bool,
    /// `userIdentity.principalId` of the creation event.
    pub(crate) principal_id: &'static str,
}

const INTERNAL_PUT_METHOD_NAME: &str = "PUT";

fn api_error_from_s3(err: S3Error) -> ApiError {
    ApiError {
        code: err.code().clone(),
        message: err.message().unwrap_or_default().to_string(),
        source: None,
    }
}

fn not_initialized() -> ApiError {
    ApiError {
        code: S3ErrorCode::InternalError,
        message: "Not init".to_string(),
        source: None,
    }
}

/// Build the header view of an internal write so header-driven policy
/// (content-type detection, compressibility, standard metadata capture) runs
/// unchanged.
fn internal_put_headers(content_headers: &HashMap<String, String>) -> Result<HeaderMap, ApiError> {
    let mut headers = HeaderMap::with_capacity(content_headers.len());
    for (name, value) in content_headers {
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|err| ApiError::invalid_request(format!("invalid content header name {name:?}: {err}")))?;
        let value = HeaderValue::from_str(value)
            .map_err(|err| ApiError::invalid_request(format!("invalid content header value for {name}: {err}")))?;
        headers.insert(name, value);
    }
    Ok(headers)
}

fn header_string(headers: &HeaderMap, name: &str) -> Option<String> {
    headers.get(name).and_then(|value| value.to_str().ok()).map(ToOwned::to_owned)
}

fn internal_put_content_input(headers: &HeaderMap, tags: Option<String>) -> PutObjectContentInput {
    PutObjectContentInput {
        cache_control: header_string(headers, "cache-control"),
        content_disposition: header_string(headers, "content-disposition"),
        content_encoding: header_string(headers, "content-encoding"),
        content_language: header_string(headers, "content-language"),
        content_type: header_string(headers, "content-type"),
        expires: header_string(headers, "expires"),
        website_redirect_location: None,
        tagging: tags,
        storage_class: None,
    }
}

async fn validate_internal_write_target(key: &str, bucket: &str, headers: &HeaderMap) -> Result<(), ApiError> {
    validate_object_key(key, INTERNAL_PUT_METHOD_NAME).map_err(api_error_from_s3)?;
    validate_table_catalog_object_mutation(bucket, key)
        .await
        .map_err(api_error_from_s3)?;
    validate_archive_content_encoding(
        key,
        headers.get("content-type").and_then(|value| value.to_str().ok()),
        headers.get("content-encoding").and_then(|value| value.to_str().ok()),
    )
    .map_err(api_error_from_s3)
}

fn internal_events_wanted() -> bool {
    crate::module_switches::is_notify_module_enabled()
        || rustfs_notify::notification_system().is_some_and(|system| system.has_live_listeners())
}

/// Creation event of an internal write, published on successful completion
/// the way [`OperationHelper`] publishes it for an S3 request.
pub(super) struct InternalPutObjectEvent {
    builder: EventArgsBuilder,
    notify: Arc<dyn NotifyInterface>,
    request_context: request_context::RequestContext,
}

impl InternalPutObjectEvent {
    /// `None` when neither the notify module nor a live listener wants events.
    pub(super) fn new(
        notify: Arc<dyn NotifyInterface>,
        request_context: request_context::RequestContext,
        event_name: EventName,
        bucket: &str,
        key: &str,
        principal_id: &'static str,
    ) -> Option<Self> {
        if !internal_events_wanted() {
            return None;
        }
        Some(Self {
            builder: Self::builder(event_name, bucket, key, principal_id),
            notify,
            request_context,
        })
    }

    pub(super) fn builder(event_name: EventName, bucket: &str, key: &str, principal_id: &'static str) -> EventArgsBuilder {
        // The object is a placeholder until `object()` supplies the committed
        // ObjectInfo, matching the S3 helper.
        let placeholder = ObjectInfo {
            bucket: bucket.to_string(),
            name: key.to_string(),
            ..Default::default()
        };
        EventArgsBuilder::new(event_name, bucket.to_string(), convert_ecstore_object_info(placeholder))
            .req_param("principalId", principal_id)
    }

    pub(super) fn object(self, obj_info: ObjectInfo) -> Self {
        let Self {
            builder,
            notify,
            request_context,
        } = self;
        Self {
            builder: builder.object(convert_ecstore_object_info(obj_info)),
            notify,
            request_context,
        }
    }

    pub(super) fn version_id(self, version_id: String) -> Self {
        let Self {
            builder,
            notify,
            request_context,
        } = self;
        Self {
            builder: builder.version_id(version_id),
            notify,
            request_context,
        }
    }

    /// Publish the event when `result` is a success; failures publish nothing.
    pub(super) fn complete<T>(self, result: &S3Result<S3Response<T>>) {
        let Ok(response) = result else {
            return;
        };
        let Self {
            builder,
            notify,
            request_context,
        } = self;
        let event_args = builder
            .resp_elements(build_event_resp_elements(response, &request_context.request_id))
            .build();
        spawn_background_with_context(Some(request_context), async move {
            notify.notify(event_args).await;
        });
    }
}

impl DefaultObjectUsecase {
    /// Write one object through the ordinary PutObject path on behalf of a
    /// trusted internal caller.
    ///
    /// The body is consumed exactly like a client request body: hashed
    /// against `expected_md5_hex`, compressed and encrypted per bucket policy,
    /// and committed under quota admission. On any failure nothing is left
    /// behind.
    pub(crate) async fn internal_put_object<B>(&self, ctx: InternalPutContext, body: B) -> Result<ObjectInfo, ApiError>
    where
        B: Stream<Item = io::Result<Bytes>> + Send + Sync + 'static,
    {
        let start_time = Instant::now();
        let InternalPutContext {
            bucket,
            key,
            size,
            expected_md5_hex,
            preserve_etag,
            content_headers,
            user_metadata,
            tags,
            internal_metadata,
            emit_events,
            principal_id,
        } = ctx;
        let Some(size) = size else {
            return Err(ApiError::invalid_request("internal put requires a known object size"));
        };
        let size = i64::try_from(size).map_err(|_| ApiError::invalid_request("internal put size exceeds the supported range"))?;

        let headers = internal_put_headers(&content_headers)?;
        validate_internal_write_target(&key, &bucket, &headers).await?;

        let write = PutObjectWriteRequest {
            bucket,
            key,
            size,
            quota_operation: QuotaOperation::PutObject,
            ciphertext_passthrough: false,
            inbound_replication_put: false,
            headers: &headers,
            query: None,
            trailing_headers: None,
            version_id: None,
            sse: PutObjectSseInput {
                server_side_encryption: None,
                ssekms_key_id: None,
                sse_customer_algorithm: None,
                sse_customer_key: None,
                sse_customer_key_md5: None,
            },
            user_metadata,
            internal_metadata,
            content: internal_put_content_input(&headers, tags),
            object_lock: PutObjectLockInput {
                legal_hold_status: None,
                mode: None,
                retain_until_date: None,
            },
            content_md5: expected_md5_hex.map(PutObjectContentMd5::Hex),
            preserve_etag,
            origin: PutObjectOrigin::Internal {
                principal_id,
                emit_events,
            },
        };
        let committed = self
            .put_object_core(write, StreamingBlob::wrap(body), start_time)
            .await
            .map_err(api_error_from_s3)?;

        let obj_info = committed.obj_info.clone();
        let result: S3Result<S3Response<()>> = Ok(S3Response::new(()));
        committed.finish(&result);
        Ok(obj_info)
    }

    /// Start a multipart upload for an internal write. The session carries the
    /// same metadata, bucket default SSE session material, Object Lock
    /// defaults and replication decision a client-initiated session would.
    pub(crate) async fn internal_create_multipart_upload(&self, ctx: &InternalPutContext) -> Result<String, ApiError> {
        let headers = internal_put_headers(&ctx.content_headers)?;
        validate_internal_write_target(&ctx.key, &ctx.bucket, &headers).await?;
        let store = self.object_store().ok_or_else(not_initialized)?;

        let mut metadata = ctx.user_metadata.clone();
        namespace_reserved_user_metadata(&mut metadata);
        extract_metadata_from_mime(&headers, &mut metadata);
        if let Some(tags) = ctx.tags.clone() {
            metadata.insert(AMZ_OBJECT_TAGGING.to_owned(), tags);
        }

        let object_lock_config_state = load_bucket_object_lock_config_state(&ctx.bucket)
            .await
            .map_err(api_error_from_s3)?;
        apply_bucket_default_lock_retention(&ctx.bucket, &object_lock_config_state, &mut metadata, false)
            .map_err(api_error_from_s3)?;

        // Internal callers carry no credential; a bucket default of SSE-KMS is
        // authorized as an internal write, like every other system write.
        let prepared_material = sse_prepare_encryption(PrepareEncryptionRequest {
            bucket: &ctx.bucket,
            key: &ctx.key,
            server_side_encryption: None,
            ssekms_key_id: None,
            ssekms_context: None,
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            principal: None,
        })
        .await?;
        if let Some(material) = prepared_material {
            let mut encryption_metadata = encryption_material_to_metadata(&material)?;
            if material.key_kind == EncryptionKeyKind::Object {
                mark_encrypted_multipart_metadata(&mut encryption_metadata);
            }
            metadata.extend(encryption_metadata);
        }

        if is_multipart_disk_compression_enabled() && is_disk_compressible(&headers, &ctx.key) {
            insert_str(
                &mut metadata,
                SUFFIX_COMPRESSION,
                compression_metadata_value(CompressionAlgorithm::default()),
            );
        }
        metadata.extend(ctx.internal_metadata.clone());

        let mt2 = metadata.clone();
        let mut opts = put_opts_with_replication_authorization(&ctx.bucket, &ctx.key, None, &headers, metadata, false)
            .await
            .map_err(ApiError::from)?;

        let dsc = must_replicate_object(
            &ctx.bucket,
            &ctx.key,
            &mt2,
            "".to_string(),
            opts.delete_marker_replication_status(),
            opts.clone(),
        )
        .await;
        if dsc.replicate_any() {
            insert_str(&mut opts.user_defined, SUFFIX_REPLICATION_TIMESTAMP, jiff::Zoned::now().to_string());
            insert_str(
                &mut opts.user_defined,
                SUFFIX_REPLICATION_STATUS,
                dsc.pending_status().unwrap_or_default(),
            );
        }

        let current_opts = get_opts(&ctx.bucket, &ctx.key, opts.version_id.clone(), None, &headers)
            .await
            .map_err(ApiError::from)?;
        match store.get_object_info(&ctx.bucket, &ctx.key, &current_opts).await {
            Ok(existing_obj_info) => {
                validate_existing_object_lock_for_write(&object_lock_config_state, &existing_obj_info, &opts)
                    .map_err(api_error_from_s3)?;
            }
            Err(err) => {
                if !is_err_object_not_found(&err) && !is_err_version_not_found(&err) {
                    return Err(ApiError::from(err));
                }
            }
        }

        let upload = store
            .new_multipart_upload(&ctx.bucket, &ctx.key, &opts)
            .await
            .map_err(ApiError::from)?;
        Ok(upload.upload_id)
    }

    /// Stage one part of an internal multipart upload. Compression and managed
    /// SSE follow the session metadata recorded at creation; the staged part
    /// is verified against `expected_md5_hex` when given.
    pub(crate) async fn internal_upload_part<B>(
        &self,
        ctx: &InternalPutContext,
        upload_id: &str,
        part_number: usize,
        size: u64,
        expected_md5_hex: Option<String>,
        body: B,
    ) -> Result<CompletePart, ApiError>
    where
        B: Stream<Item = io::Result<Bytes>> + Send + Sync + Unpin + 'static,
    {
        let size =
            i64::try_from(size).map_err(|_| ApiError::invalid_request("internal part size exceeds the supported range"))?;
        let bucket = ctx.bucket.as_str();
        let key = ctx.key.as_str();
        let store = self.object_store().ok_or_else(not_initialized)?;
        let mut opts = ObjectOptions::default();
        let session = store
            .get_multipart_info(bucket, key, upload_id, &opts)
            .await
            .map_err(ApiError::from)?;

        let upload_part_admission = match get_concurrency_manager()
            .admit_multipart_part(size)
            .await
            .map_err(|_| ApiError::other(io::Error::other("foreground write admission closed")))?
        {
            ForegroundWriteAdmission::Disabled => None,
            ForegroundWriteAdmission::Admitted(permit) => {
                counter!("rustfs.upload_part.foreground_admission.total", "result" => "admitted").increment(1);
                Some(permit)
            }
            ForegroundWriteAdmission::Rejected => {
                counter!("rustfs.upload_part.foreground_admission.total", "result" => "rejected").increment(1);
                return Err(ApiError {
                    code: S3ErrorCode::SlowDown,
                    message: "foreground write concurrency limit reached, please reduce your request rate".to_string(),
                    source: None,
                });
            }
        };

        let buffer_size = get_buffer_size_opt_in(size);
        let body = tokio::io::BufReader::with_capacity(buffer_size, StreamReader::new(body));
        let actual_size = size;
        let mut write_plan = WritePlan::new();
        let mut reader = if rustfs_utils::http::contains_key_str(&session.user_defined, SUFFIX_COMPRESSION) {
            let hrd = HashReader::from_stream(body, size, actual_size, expected_md5_hex, None, false).map_err(ApiError::from)?;
            write_plan = write_plan.with_compression(CompressionAlgorithm::default());
            hrd
        } else {
            HashReader::from_stream(body, size, actual_size, expected_md5_hex, None, false).map_err(ApiError::from)?
        };
        opts.want_checksum = reader.checksum();

        if session
            .user_defined
            .contains_key("x-amz-server-side-encryption-customer-algorithm")
        {
            return Err(ApiError::invalid_request("internal multipart writes cannot continue an SSE-C session"));
        }
        if session.user_defined.contains_key("x-amz-server-side-encryption") {
            // Reuses the envelope prepared at creation; the session pins the key.
            let managed_material = sse_decryption(DecryptionRequest {
                bucket,
                key,
                metadata: &session.user_defined,
                sse_customer_key: None,
                sse_customer_key_md5: None,
                principal: None,
            })
            .await?
            .ok_or_else(|| ApiError::from(StorageError::other("Missing managed SSE session material")))?;
            let managed_write = match managed_material.key_kind {
                EncryptionKeyKind::Object => {
                    WriteEncryption::multipart_object_key(managed_material.key_bytes, part_number as u32)
                }
                EncryptionKeyKind::Direct => {
                    WriteEncryption::multipart(managed_material.key_bytes, managed_material.base_nonce, part_number)
                }
            };
            write_plan = write_plan.with_encryption(managed_write);
        }

        reader = write_plan.apply(reader, actual_size).map_err(ApiError::from)?;
        let mut reader = PutObjReader::new(reader);

        let _upload_part_admission = upload_part_admission;
        let info = store
            .put_object_part(bucket, key, upload_id, part_number, &mut reader, &opts)
            .await
            .map_err(ApiError::from)?;
        drop(_upload_part_admission);

        Ok(CompletePart {
            part_num: info.part_num,
            etag: info.etag,
            ..Default::default()
        })
    }

    /// Complete an internal multipart upload: versioning, quota admission,
    /// Object Lock validation of the overwritten version, usage accounting,
    /// immediate ILM transition, replication scheduling and the creation
    /// event, as the S3 CompleteMultipartUpload handler performs them.
    pub(crate) async fn internal_complete_multipart_upload(
        &self,
        ctx: &InternalPutContext,
        upload_id: &str,
        parts: Vec<CompletePart>,
    ) -> Result<ObjectInfo, ApiError> {
        let bucket = ctx.bucket.clone();
        let key = ctx.key.clone();
        if parts.is_empty() {
            return Err(ApiError::invalid_request("You must specify at least one part"));
        }
        if parts.windows(2).any(|pair| pair[0].part_num >= pair[1].part_num) {
            return Err(ApiError::invalid_request("multipart parts must be listed in ascending part number order"));
        }
        validate_table_catalog_object_mutation(&bucket, &key)
            .await
            .map_err(api_error_from_s3)?;
        let store = self.object_store().ok_or_else(not_initialized)?;

        let headers = HeaderMap::new();
        let mut opts =
            get_complete_multipart_upload_opts_with_replication_authorization(&headers, false).map_err(ApiError::from)?;
        opts.preserve_etag = ctx.preserve_etag.clone();
        let versioned = BucketVersioningSys::prefix_enabled(&bucket, &key).await;
        opts.versioned = versioned;
        opts.version_suspended = BucketVersioningSys::prefix_suspended(&bucket, &key).await;
        let capacity_scope_token = Uuid::new_v4();
        opts.capacity_scope_token = Some(capacity_scope_token);

        let current_opts =
            internal_object_info_lookup_opts(get_opts(&bucket, &key, None, None, &headers).await.map_err(ApiError::from)?);
        let object_lock_config_state = load_bucket_object_lock_config_state(&bucket)
            .await
            .map_err(api_error_from_s3)?;
        let previous_current_sizes = match store.get_object_info(&bucket, &key, &current_opts).await {
            Ok(existing_obj_info) => {
                validate_existing_object_lock_for_write(&object_lock_config_state, &existing_obj_info, &current_opts)
                    .map_err(api_error_from_s3)?;
                let physical_size = existing_obj_info.size.max(0) as u64;
                let logical_size = quota_object_size(&existing_obj_info);
                Some((physical_size, logical_size))
            }
            Err(err) => {
                if !is_err_object_not_found(&err) && !is_err_version_not_found(&err) {
                    return Err(ApiError::from(err));
                }
                None
            }
        };

        let cache_adapter = self.object_data_cache();
        let _ = invalidate_object_data_cache_before_mutation(&cache_adapter, &bucket, &key).await;

        let quota_metadata_sys = self.bucket_metadata_sys();
        let quota_tracking = quota_metadata_sys.is_some();
        let mut quota_enabled = false;
        if let Some(metadata_sys) = quota_metadata_sys {
            let quota_checker = QuotaChecker::new(metadata_sys);
            let check_result =
                map_quota_check_outcome(&bucket, quota_checker.check_quota(&bucket, QuotaOperation::PutObject, 0).await)
                    .map_err(api_error_from_s3)?;
            quota_enabled = check_result.quota_limit.is_some();
            apply_quota_admission(&mut opts, &check_result).map_err(api_error_from_s3)?;
        }

        let previous_current_size = match previous_current_sizes {
            Some((_, Ok(logical_size))) if quota_enabled => Some(logical_size),
            Some((_, Err(err))) if quota_enabled => return Err(ApiError::from(err)),
            Some((physical_size, _)) => Some(physical_size),
            None => None,
        };

        let event = ctx.emit_events.then(|| {
            InternalPutObjectEvent::new(
                current_notify_interface_for_context(self.context.as_deref()),
                request_context::RequestContext::fallback(),
                EventName::ObjectCreatedCompleteMultipartUpload,
                &bucket,
                &key,
                ctx.principal_id,
            )
        });

        // The spawned task owns the commit so a cancelled caller cannot leave
        // the bookkeeping half done.
        let complete_commit = spawn_traced_join({
            let store = Arc::clone(&store);
            let bucket = bucket.clone();
            let key = key.clone();
            let upload_id = upload_id.to_string();
            let opts = opts.clone();
            async move {
                let obj_info = store
                    .clone()
                    .complete_multipart_upload(&bucket, &key, &upload_id, parts, &opts)
                    .await
                    .map_err(ApiError::from)?;
                let _ = invalidate_object_data_cache_after_complete_multipart_success(&cache_adapter, &bucket, &key).await;
                record_capacity_write(Some(capacity_scope_token)).await;

                if quota_tracking {
                    let committed_size = quota_accounting_object_size(&obj_info, quota_enabled).map_err(api_error_from_s3)?;
                    if versioned {
                        record_bucket_object_version_write_memory(&bucket, previous_current_size, committed_size).await;
                    } else {
                        record_bucket_object_write_memory(&bucket, previous_current_size, committed_size).await;
                    }
                }

                enqueue_transition_immediate(&obj_info, LcEventSrc::S3CompleteMultipartUpload).await;

                let mt2 = obj_info.user_defined.clone();
                let dsc = must_replicate_object(
                    &bucket,
                    &key,
                    &mt2,
                    "".to_string(),
                    opts.delete_marker_replication_status(),
                    opts.clone(),
                )
                .await;
                if dsc.replicate_any() {
                    schedule_object_replication(obj_info.clone(), store, dsc).await;
                }

                rustfs_scanner::record_dirty_usage_bucket(&bucket);
                Ok::<_, ApiError>(obj_info)
            }
        });
        let obj_info = complete_commit.await.map_err(|err| {
            ApiError::other(io::Error::other(format!("complete multipart upload commit owner task failed: {err}")))
        })??;

        if let Some(event) = event.flatten() {
            let mut event = event.object(obj_info.clone());
            if versioned && let Some(version_id) = obj_info.version_id {
                event = event.version_id(version_id.to_string());
            }
            let result: S3Result<S3Response<()>> = Ok(S3Response::new(()));
            event.complete(&result);
        }
        Ok(obj_info)
    }

    /// Discard an internal multipart upload and its staged parts.
    pub(crate) async fn internal_abort_multipart_upload(&self, bucket: &str, key: &str, upload_id: &str) -> Result<(), ApiError> {
        let store = self.object_store().ok_or_else(not_initialized)?;
        store
            .abort_multipart_upload(bucket, key, upload_id, &ObjectOptions::default())
            .await
            .map_err(ApiError::from)?;
        rustfs_scanner::record_dirty_usage_bucket(bucket);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};
    use rustfs_utils::http::{
        MINIO_INTERNAL_PREFIX, RUSTFS_INTERNAL_PREFIX, SUFFIX_ODM_PULLED_AT, SUFFIX_ODM_SOURCE, SUFFIX_ODM_SOURCE_ETAG,
        SUFFIX_ODM_SOURCE_LAST_MODIFIED, SUFFIX_ODM_SOURCE_VERSION_ID, contains_key_str, get_str,
    };

    const TEST_PRINCIPAL: &str = "rustfs-internal-put-test";

    fn md5_hex(body: &[u8]) -> String {
        hex_simd::encode_to_string(Md5::digest(body), hex_simd::AsciiCase::Lower)
    }

    fn body_stream(chunks: Vec<Bytes>) -> impl Stream<Item = io::Result<Bytes>> + Send + Sync + Unpin + 'static {
        futures::stream::iter(chunks.into_iter().map(Ok))
    }

    fn provenance_metadata() -> HashMap<String, String> {
        let mut internal_metadata = HashMap::new();
        insert_str(&mut internal_metadata, SUFFIX_ODM_SOURCE, "s3:source-bucket".to_string());
        insert_str(
            &mut internal_metadata,
            SUFFIX_ODM_SOURCE_ETAG,
            "\"0123456789abcdef0123456789abcdef-3\"".to_string(),
        );
        insert_str(
            &mut internal_metadata,
            SUFFIX_ODM_SOURCE_LAST_MODIFIED,
            "2026-01-02T03:04:05Z".to_string(),
        );
        insert_str(&mut internal_metadata, SUFFIX_ODM_SOURCE_VERSION_ID, String::new());
        insert_str(&mut internal_metadata, SUFFIX_ODM_PULLED_AT, "2026-09-02T00:00:00Z".to_string());
        internal_metadata
    }

    fn internal_context(bucket: &str, key: &str, body: &[u8]) -> InternalPutContext {
        InternalPutContext {
            bucket: bucket.to_string(),
            key: key.to_string(),
            size: Some(body.len() as u64),
            expected_md5_hex: Some(md5_hex(body)),
            preserve_etag: None,
            content_headers: HashMap::from([
                ("Content-Type".to_string(), "text/plain".to_string()),
                ("Cache-Control".to_string(), "max-age=60".to_string()),
            ]),
            user_metadata: HashMap::from([("origin".to_string(), "unit-test".to_string())]),
            tags: Some("team=storage".to_string()),
            internal_metadata: provenance_metadata(),
            emit_events: false,
            principal_id: TEST_PRINCIPAL,
        }
    }

    async fn internal_put_test_bucket(prefix: &str) -> (Arc<ECStore>, String) {
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        crate::app::runtime_sources::install_test_app_context(Arc::clone(&store)).await;
        let bucket = format!("{prefix}-{}", Uuid::new_v4().simple());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create internal put test bucket");
        (store, bucket)
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn internal_put_object_writes_through_the_shared_put_path() {
        let (store, bucket) = internal_put_test_bucket("internal-put").await;
        let body = b"internal write-back body".to_vec();
        let ctx = internal_context(&bucket, "dir/object.txt", &body);

        let obj_info = DefaultObjectUsecase::from_global()
            .internal_put_object(ctx, body_stream(vec![Bytes::from(body.clone())]))
            .await
            .expect("internal put must succeed");

        assert_eq!(obj_info.etag.as_deref(), Some(md5_hex(&body).as_str()));
        assert_eq!(obj_info.size, body.len() as i64);

        let stored = store
            .get_object_info(&bucket, "dir/object.txt", &ObjectOptions::default())
            .await
            .expect("internal put must leave a readable object");
        let metadata = &stored.user_defined;
        assert_eq!(metadata.get("content-type").map(String::as_str), Some("text/plain"));
        assert_eq!(metadata.get("cache-control").map(String::as_str), Some("max-age=60"));
        assert_eq!(metadata.get("origin").map(String::as_str), Some("unit-test"));
        assert_eq!(stored.user_tags.as_str(), "team=storage", "tags must be committed as object tags");
        for suffix in [
            SUFFIX_ODM_SOURCE,
            SUFFIX_ODM_SOURCE_ETAG,
            SUFFIX_ODM_SOURCE_LAST_MODIFIED,
            SUFFIX_ODM_SOURCE_VERSION_ID,
            SUFFIX_ODM_PULLED_AT,
        ] {
            assert!(
                metadata.contains_key(&format!("{RUSTFS_INTERNAL_PREFIX}{suffix}")),
                "missing RustFS provenance key {suffix}"
            );
            assert!(
                metadata.contains_key(&format!("{MINIO_INTERNAL_PREFIX}{suffix}")),
                "missing MinIO provenance key {suffix}"
            );
        }
        assert_eq!(get_str(metadata, SUFFIX_ODM_SOURCE).as_deref(), Some("s3:source-bucket"));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn internal_put_object_preserves_the_caller_etag() {
        let (store, bucket) = internal_put_test_bucket("internal-put-etag").await;
        let body = b"etag is preserved verbatim".to_vec();
        let mut ctx = internal_context(&bucket, "preserved.bin", &body);
        ctx.preserve_etag = Some("0123456789abcdef0123456789abcdef-3".to_string());

        let obj_info = DefaultObjectUsecase::from_global()
            .internal_put_object(ctx, body_stream(vec![Bytes::from(body.clone())]))
            .await
            .expect("internal put with a preserved ETag must succeed");
        assert_eq!(obj_info.etag.as_deref(), Some("0123456789abcdef0123456789abcdef-3"));

        let stored = store
            .get_object_info(&bucket, "preserved.bin", &ObjectOptions::default())
            .await
            .expect("preserved-ETag object must be readable");
        assert_eq!(stored.etag.as_deref(), Some("0123456789abcdef0123456789abcdef-3"));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn internal_put_object_rejects_a_digest_mismatch_without_committing() {
        let (store, bucket) = internal_put_test_bucket("internal-put-digest").await;
        let body = b"body whose digest will not match".to_vec();
        let mut ctx = internal_context(&bucket, "mismatch.bin", &body);
        ctx.expected_md5_hex = Some(md5_hex(b"a different body"));

        let err = DefaultObjectUsecase::from_global()
            .internal_put_object(ctx, body_stream(vec![Bytes::from(body)]))
            .await
            .expect_err("digest mismatch must fail the internal put");
        assert_eq!(err.code, S3ErrorCode::BadDigest, "unexpected error: {err}");

        let lookup = store
            .get_object_info(&bucket, "mismatch.bin", &ObjectOptions::default())
            .await;
        assert!(
            lookup.as_ref().is_err_and(is_err_object_not_found),
            "a rejected internal put must not leave an object behind"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn internal_put_object_requires_a_known_size() {
        let (_store, bucket) = internal_put_test_bucket("internal-put-size").await;
        let body = b"unknown length".to_vec();
        let mut ctx = internal_context(&bucket, "unknown.bin", &body);
        ctx.size = None;

        let err = DefaultObjectUsecase::from_global()
            .internal_put_object(ctx, body_stream(vec![Bytes::from(body)]))
            .await
            .expect_err("an unknown size must be rejected before the body is read");
        assert_eq!(err.code, S3ErrorCode::InvalidRequest);
    }

    #[test]
    #[serial_test::serial]
    fn internal_multipart_roundtrip_completes_and_abort_leaves_nothing() {
        crate::app::gating_test_env::run_large_stack_test(
            "internal-multipart-roundtrip",
            internal_multipart_roundtrip_completes_and_abort_leaves_nothing_inner,
        );
    }

    async fn internal_multipart_roundtrip_completes_and_abort_leaves_nothing_inner() {
        const FIRST_PART_SIZE: usize = 5 * 1024 * 1024;
        let (store, bucket) = internal_put_test_bucket("internal-mpu").await;
        let usecase = DefaultObjectUsecase::from_global();
        let first_part = vec![0x41u8; FIRST_PART_SIZE];
        let last_part = b"tail of the multipart object".to_vec();
        let mut ctx = internal_context(&bucket, "multipart/object.bin", &[]);
        ctx.size = None;
        ctx.expected_md5_hex = None;
        ctx.preserve_etag = Some("0123456789abcdef0123456789abcdef-2".to_string());

        let upload_id = usecase
            .internal_create_multipart_upload(&ctx)
            .await
            .expect("internal multipart create must succeed");
        let part_one = usecase
            .internal_upload_part(
                &ctx,
                &upload_id,
                1,
                first_part.len() as u64,
                Some(md5_hex(&first_part)),
                body_stream(vec![Bytes::from(first_part.clone())]),
            )
            .await
            .expect("first internal part must stage");
        let part_two = usecase
            .internal_upload_part(
                &ctx,
                &upload_id,
                2,
                last_part.len() as u64,
                Some(md5_hex(&last_part)),
                body_stream(vec![Bytes::from(last_part.clone())]),
            )
            .await
            .expect("last internal part must stage");
        assert_eq!(part_one.part_num, 1);
        assert_eq!(part_two.part_num, 2);

        let obj_info = usecase
            .internal_complete_multipart_upload(&ctx, &upload_id, vec![part_one, part_two])
            .await
            .expect("internal multipart complete must succeed");
        assert_eq!(obj_info.size, (first_part.len() + last_part.len()) as i64);
        assert_eq!(obj_info.parts.len(), 2);
        assert_eq!(obj_info.etag.as_deref(), Some("0123456789abcdef0123456789abcdef-2"));
        let stored = store
            .get_object_info(&bucket, &ctx.key, &ObjectOptions::default())
            .await
            .expect("completed multipart object must be readable");
        assert_eq!(stored.user_defined.get("content-type").map(String::as_str), Some("text/plain"));
        assert_eq!(stored.user_defined.get("origin").map(String::as_str), Some("unit-test"));
        assert!(contains_key_str(&stored.user_defined, SUFFIX_ODM_SOURCE));

        let aborted_upload_id = usecase
            .internal_create_multipart_upload(&ctx)
            .await
            .expect("second internal multipart create must succeed");
        usecase
            .internal_upload_part(
                &ctx,
                &aborted_upload_id,
                1,
                last_part.len() as u64,
                None,
                body_stream(vec![Bytes::from(last_part.clone())]),
            )
            .await
            .expect("part of the aborted upload must stage");
        usecase
            .internal_abort_multipart_upload(&bucket, &ctx.key, &aborted_upload_id)
            .await
            .expect("internal abort must succeed");
        let uploads = store
            .list_multipart_uploads(&bucket, &ctx.key, None, None, None, 100)
            .await
            .expect("list multipart uploads after abort");
        assert!(
            uploads.uploads.iter().all(|upload| upload.upload_id != aborted_upload_id),
            "aborted internal upload must not linger: {:?}",
            uploads.uploads
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn internal_complete_multipart_upload_rejects_unordered_parts() {
        let (_store, bucket) = internal_put_test_bucket("internal-mpu-order").await;
        let ctx = internal_context(&bucket, "unordered.bin", &[]);
        let parts = vec![
            CompletePart {
                part_num: 2,
                ..Default::default()
            },
            CompletePart {
                part_num: 1,
                ..Default::default()
            },
        ];
        let err = DefaultObjectUsecase::from_global()
            .internal_complete_multipart_upload(&ctx, "upload", parts)
            .await
            .expect_err("unordered parts must be rejected before touching the store");
        assert_eq!(err.code, S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn internal_put_event_names_the_principal_and_creation_event() {
        let event_args = InternalPutObjectEvent::builder(EventName::ObjectCreatedPut, "bucket", "key", TEST_PRINCIPAL).build();
        assert_eq!(event_args.event_name, EventName::ObjectCreatedPut);
        assert_eq!(event_args.bucket_name, "bucket");
        assert_eq!(event_args.req_params.get("principalId").map(String::as_str), Some(TEST_PRINCIPAL));
        assert!(!event_args.is_replication_request());
    }

    #[test]
    fn internal_put_headers_normalize_names_and_reject_invalid_values() {
        let headers = internal_put_headers(&HashMap::from([
            ("Content-Type".to_string(), "application/json".to_string()),
            ("Cache-Control".to_string(), "no-cache".to_string()),
        ]))
        .expect("valid content headers must build");
        assert_eq!(headers.get("content-type").and_then(|v| v.to_str().ok()), Some("application/json"));
        let content = internal_put_content_input(&headers, Some("a=b".to_string()));
        assert_eq!(content.content_type.as_deref(), Some("application/json"));
        assert_eq!(content.cache_control.as_deref(), Some("no-cache"));
        assert_eq!(content.tagging.as_deref(), Some("a=b"));
        assert!(content.storage_class.is_none());

        let err = internal_put_headers(&HashMap::from([("Content-Type".to_string(), "bad\nvalue".to_string())]))
            .expect_err("a header value with a control character must be rejected");
        assert_eq!(err.code, S3ErrorCode::InvalidRequest);
    }
}
