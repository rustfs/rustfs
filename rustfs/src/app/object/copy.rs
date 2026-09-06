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

//! CopyObject path.

use super::*;

use crate::auth::{VerifiedPresignedRequest, reject_presigned_put_max_content_length_for_other_operation};
use crate::error::ServerSideSourceReadError;

struct CopySourceReadStream<R> {
    inner: R,
    remaining: i64,
}

impl<R> CopySourceReadStream<R> {
    fn new(inner: R, expected_size: i64) -> Self {
        Self {
            inner,
            remaining: expected_size.max(0),
        }
    }
}

fn copy_source_read_stream<R>(inner: R, expected_size: i64) -> CopySourceReadStream<R> {
    CopySourceReadStream::new(inner, expected_size)
}

fn copy_source_read_error(source: std::io::Error) -> std::io::Error {
    let kind = source.kind();
    std::io::Error::new(kind, ServerSideSourceReadError::new("CopyObject", source))
}

fn copy_source_incomplete_body_error(remaining: i64) -> std::io::Error {
    copy_source_read_error(std::io::Error::new(
        std::io::ErrorKind::UnexpectedEof,
        rustfs_rio::IncompleteBody { remaining },
    ))
}

impl<R> AsyncRead for CopySourceReadStream<R>
where
    R: AsyncRead + Unpin,
{
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        match Pin::new(&mut this.inner).poll_read(cx, buf) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(err)) => Poll::Ready(Err(copy_source_read_error(err))),
            Poll::Ready(Ok(())) => {
                let read = buf.filled().len() - before;
                if read == 0 {
                    if this.remaining > 0 {
                        return Poll::Ready(Err(copy_source_incomplete_body_error(this.remaining)));
                    }
                    return Poll::Ready(Ok(()));
                }

                let read = match i64::try_from(read) {
                    Ok(read) => read,
                    Err(_) => {
                        return Poll::Ready(Err(copy_source_read_error(std::io::Error::other(
                            "copy source read count exceeds i64::MAX",
                        ))));
                    }
                };
                this.remaining = this.remaining.saturating_sub(read);
                Poll::Ready(Ok(()))
            }
        }
    }
}

fn copy_namespace_lock_error(bucket: &str, object: &str, mode: &'static str, err: rustfs_lock::LockError) -> StorageError {
    match err {
        rustfs_lock::LockError::QuorumNotReached { required, achieved } => StorageError::NamespaceLockQuorumUnavailable {
            mode,
            bucket: bucket.to_owned(),
            object: object.to_owned(),
            required,
            achieved,
        },
        other => StorageError::Lock(other),
    }
}

async fn acquire_self_copy_namespace_lock<S>(store: &S, bucket: &str, object: &str) -> S3Result<NamespaceLockGuard>
where
    S: NamespaceLocking<Error = EcstoreError, NamespaceLock = rustfs_lock::NamespaceLockWrapper> + ?Sized,
{
    let object = encode_dir_object(object);
    let lock = store.new_ns_lock(bucket, &object).await.map_err(ApiError::from)?;
    lock.get_write_lock(get_lock_acquire_timeout())
        .await
        .map_err(|err| ApiError::from(copy_namespace_lock_error(bucket, &object, "write", err)).into())
}

pub(crate) async fn acquire_copy_bucket_lifecycle_lock<S>(store: &S, bucket: &str) -> S3Result<NamespaceLockGuard>
where
    S: NamespaceLocking<Error = EcstoreError, NamespaceLock = rustfs_lock::NamespaceLockWrapper> + ?Sized,
{
    let lock = store
        .new_ns_lock(bucket, BUCKET_LIFECYCLE_LOCK_OBJECT)
        .await
        .map_err(ApiError::from)?;
    lock.get_read_lock(get_lock_acquire_timeout()).await.map_err(|err| {
        ApiError::from(copy_namespace_lock_error(
            bucket,
            BUCKET_LIFECYCLE_LOCK_OBJECT,
            "bucket_lifecycle_read",
            err,
        ))
        .into()
    })
}

pub(crate) async fn acquire_copy_bucket_lifecycle_locks<S>(
    store: &S,
    source_bucket: &str,
    destination_bucket: &str,
) -> S3Result<(NamespaceLockGuard, Option<NamespaceLockGuard>)>
where
    S: NamespaceLocking<Error = EcstoreError, NamespaceLock = rustfs_lock::NamespaceLockWrapper> + ?Sized,
{
    if source_bucket == destination_bucket {
        return Ok((acquire_copy_bucket_lifecycle_lock(store, source_bucket).await?, None));
    }

    if source_bucket < destination_bucket {
        let source_guard = acquire_copy_bucket_lifecycle_lock(store, source_bucket).await?;
        let destination_guard = acquire_copy_bucket_lifecycle_lock(store, destination_bucket).await?;
        Ok((source_guard, Some(destination_guard)))
    } else {
        let destination_guard = acquire_copy_bucket_lifecycle_lock(store, destination_bucket).await?;
        let source_guard = acquire_copy_bucket_lifecycle_lock(store, source_bucket).await?;
        Ok((source_guard, Some(destination_guard)))
    }
}

impl DefaultObjectUsecase {
    pub fn execute_copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> impl std::future::Future<Output = S3Result<S3Response<CopyObjectOutput>>> + Send + '_ {
        Box::pin(self.execute_copy_object_inner(req))
    }

    #[instrument(name = "execute_copy_object", level = "debug", skip(self, req))]
    async fn execute_copy_object_inner(&self, req: S3Request<CopyObjectInput>) -> S3Result<S3Response<CopyObjectOutput>> {
        reject_presigned_put_max_content_length_for_other_operation(
            &req.headers,
            req.uri.query(),
            req.extensions.get::<VerifiedPresignedRequest>().is_some(),
        )?;
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let mut helper = OperationHelper::new(&req, EventName::ObjectCreatedCopy, S3Operation::CopyObject);
        let CopyObjectInput {
            copy_source,
            bucket,
            key,
            version_id: dest_version_id,
            server_side_encryption: requested_sse,
            ssekms_key_id: requested_kms_key_id,
            sse_customer_algorithm,
            sse_customer_key,
            sse_customer_key_md5,
            copy_source_sse_customer_algorithm,
            copy_source_sse_customer_key,
            copy_source_sse_customer_key_md5,
            metadata_directive,
            metadata,
            tagging,
            tagging_directive,
            copy_source_if_match,
            copy_source_if_none_match,
            cache_control,
            content_disposition,
            content_encoding,
            content_language,
            content_type,
            expires,
            website_redirect_location,
            object_lock_legal_hold_status,
            object_lock_mode,
            object_lock_retain_until_date,
            storage_class,
            checksum_algorithm,
            ..
        } = req.input.clone();
        let requested_checksum_type = checksum_algorithm
            .as_ref()
            .map(|algorithm| rustfs_rio::ChecksumType::from_string(algorithm.as_str()));
        if requested_checksum_type.is_some_and(|checksum_type| !checksum_type.is_set()) {
            return Err(s3_error!(InvalidArgument, "Unsupported checksum algorithm"));
        }
        let (src_bucket, src_key, version_id) = match copy_source {
            CopySource::AccessPoint { .. } => return Err(s3_error!(NotImplemented)),
            CopySource::Outpost { .. } => return Err(s3_error!(NotImplemented)),
            CopySource::Bucket {
                ref bucket,
                ref key,
                version_id,
            } => (bucket.to_string(), key.to_string(), version_id.map(|v| v.to_string())),
        };

        // Normalize the copy-source version id like GET/HEAD do: trim, treat "null" as the
        // nil UUID, and reject malformed ids up front (issue #4238).
        let version_id = match version_id {
            Some(v) => {
                let trimmed = v.trim();
                if trimmed.eq_ignore_ascii_case("null") {
                    Some(Uuid::nil().to_string())
                } else if Uuid::parse_str(trimmed).is_ok() {
                    Some(trimmed.to_string())
                } else {
                    return Err(s3_error!(InvalidArgument, "Invalid version id specified in copy source"));
                }
            }
            None => None,
        };

        if let Some(ref sc) = storage_class
            && !is_valid_storage_class(sc.as_str())
        {
            return Err(s3_error!(InvalidStorageClass));
        }
        let ssekms_context = extract_ssekms_context_from_headers(&req.headers)?;
        validate_sse_headers_for_write(
            requested_sse.as_ref(),
            requested_kms_key_id.as_ref(),
            ssekms_context.as_ref(),
            sse_customer_algorithm.as_ref(),
            sse_customer_key.as_ref(),
            sse_customer_key_md5.as_ref(),
            true,
        )?;
        let has_explicit_ssec = sse_customer_algorithm.is_some() || sse_customer_key.is_some() || sse_customer_key_md5.is_some();

        // Validate both source and destination keys
        validate_object_key(&src_key, "COPY (source)")?;
        validate_object_key(&key, "COPY (dest)")?;
        validate_table_catalog_object_mutation(&bucket, &key).await?;
        let replaces_metadata = match metadata_directive.as_ref().map(|directive| directive.as_str()) {
            None | Some(MetadataDirective::COPY) => false,
            Some(MetadataDirective::REPLACE) => true,
            Some(_) => {
                return Err(S3Error::with_message(
                    S3ErrorCode::InvalidArgument,
                    "The MetadataDirective header is invalid".to_string(),
                ));
            }
        };
        let expires_timestamp = parse_expires_header(expires.as_deref())?;
        let replacement_metadata = if replaces_metadata {
            validate_archive_content_encoding(&key, content_type.as_deref(), content_encoding.as_deref())?;
            let mut replacement_metadata = metadata.unwrap_or_default();
            namespace_reserved_user_metadata(&mut replacement_metadata);
            apply_standard_object_metadata(
                &mut replacement_metadata,
                cache_control.as_deref(),
                content_disposition.as_deref(),
                content_encoding.as_deref(),
                content_language.as_deref(),
                content_type.as_deref(),
                expires_timestamp.as_ref(),
                website_redirect_location.as_deref(),
            )?;
            Some(replacement_metadata)
        } else {
            None
        };

        // AWS S3 allows self-copy when metadata directive is REPLACE (used to update metadata in-place),
        // when an explicit storage class change is requested, or when restoring a specific historical
        // version onto the current key (source carries a versionId). Reject only a true no-op self-copy
        // where none of these apply (issue #4238).
        let replacement_tags = crate::app::storage_api::object_usecase::s3_api::tagging::resolve_copy_object_tags(
            tagging.as_deref(),
            tagging_directive.as_ref(),
        )?;

        if !replaces_metadata
            && tagging_directive.as_ref().map(TaggingDirective::as_str) != Some(TaggingDirective::REPLACE)
            && storage_class.is_none()
            && version_id.is_none()
            && src_bucket == bucket
            && src_key == key
        {
            error!(bucket, key, "Rejected self-copy operation");
            return Err(s3_error!(
                InvalidRequest,
                "Cannot copy an object to itself. Source and destination must be different."
            ));
        }

        // warn!("copy_object {}/{}, to {}/{}", &src_bucket, &src_key, &bucket, &key);

        let mut src_opts = copy_src_opts(&src_bucket, &src_key, &req.headers).map_err(ApiError::from)?;

        src_opts.version_id = version_id.clone();

        let mut src_get_opts = ObjectOptions {
            version_id: src_opts.version_id.clone(),
            versioned: src_opts.versioned,
            version_suspended: src_opts.version_suspended,
            ..Default::default()
        };
        apply_copy_source_bucket_generation_guard(&req, &src_bucket, &mut src_get_opts)?;

        let mut dst_opts = copy_dst_opts_with_replication_authorization(
            &bucket,
            &key,
            dest_version_id.clone(),
            &req.headers,
            HashMap::new(),
            replication_request_authorized(&req),
        )
        .await
        .map_err(ApiError::from)?;
        apply_bucket_generation_guard(&req, &bucket, &mut dst_opts)?;

        let cp_src_dst_same = path_join_buf(&[&src_bucket, &src_key]) == path_join_buf(&[&bucket, &key]);
        let expected_current_version_id = expected_current_version_id(&req.headers)?;
        if expected_current_version_id.is_some()
            && (!cp_src_dst_same || version_id.is_none() || dest_version_id.is_some() || !dst_opts.versioned)
        {
            return Err(s3_error!(
                InvalidRequest,
                "Expected current version precondition requires a versioned same-object historical copy that creates a new version"
            ));
        }

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };
        let (source_bucket_lifecycle_guard, destination_bucket_lifecycle_guard_storage) =
            acquire_copy_bucket_lifecycle_locks(store.as_ref(), &src_bucket, &bucket).await?;
        let current_source_incarnation_id = store
            .bucket_incarnation_id_from_disk(&src_bucket)
            .await
            .map_err(ApiError::from)?;
        if src_get_opts
            .expected_bucket_incarnation_id
            .is_some_and(|expected| expected != current_source_incarnation_id)
        {
            return Err(ApiError::from(StorageError::BucketNotFound(src_bucket.clone())).into());
        }
        let current_destination_incarnation_id = if src_bucket == bucket {
            current_source_incarnation_id
        } else {
            store.bucket_incarnation_id_from_disk(&bucket).await.map_err(ApiError::from)?
        };
        if dst_opts
            .expected_bucket_incarnation_id
            .is_some_and(|expected| expected != current_destination_incarnation_id)
        {
            return Err(ApiError::from(StorageError::BucketNotFound(bucket.clone())).into());
        }
        let destination_bucket_lifecycle_guard = destination_bucket_lifecycle_guard_storage
            .as_ref()
            .unwrap_or(&source_bucket_lifecycle_guard);
        if source_bucket_lifecycle_guard.is_lock_lost() || destination_bucket_lifecycle_guard.is_lock_lost() {
            return Err(ApiError::from(StorageError::NamespaceLockQuorumUnavailable {
                mode: "copy_bucket_generation",
                bucket: bucket.clone(),
                object: key.clone(),
                required: 1,
                achieved: 0,
            })
            .into());
        }
        src_get_opts.expected_bucket_incarnation_id = Some(current_source_incarnation_id);
        dst_opts.expected_bucket_incarnation_id = Some(current_destination_incarnation_id);
        if src_bucket != bucket {
            dst_opts.add_bucket_lifecycle_lock_guard(&source_bucket_lifecycle_guard);
        }
        dst_opts.add_bucket_lifecycle_lock_guard(destination_bucket_lifecycle_guard);

        // Bucket metadata uses the bucket name as its namespace-lock key. Load
        // every copy-time bucket snapshot before a same-object key can collide
        // with that key (for example, copying `bucket/bucket` onto itself).
        let bucket_sse_config = load_bucket_default_sse_config(&bucket).await?;
        let object_lock_config_state = load_bucket_object_lock_config_state(&bucket).await?;
        if cp_src_dst_same && key == bucket {
            dst_opts.object_lock_config_snapshot =
                Some(store.object_lock_config_snapshot(&bucket).await.map_err(ApiError::from)?);
        }
        let mut current_opts: ObjectOptions = internal_object_info_lookup_opts(
            get_opts(&bucket, &key, dest_version_id.clone(), None, &req.headers)
                .await
                .map_err(ApiError::from)?,
        );

        // Hold the self-copy namespace write lock before opening the source reader, including
        // expected-current historical copies. With lock optimization disabled, the source
        // stream retains its read guard until EOF; taking the write lock later in ECStore
        // would self-deadlock before the copy can consume that stream.
        let _self_copy_lock_guard = if cp_src_dst_same {
            let guard = acquire_self_copy_namespace_lock(store.as_ref(), &bucket, &key).await?;
            src_opts.no_lock = true;
            src_get_opts.no_lock = true;
            dst_opts.no_lock = true;
            Some(guard)
        } else {
            None
        };
        if let Some(guard) = _self_copy_lock_guard.as_ref() {
            dst_opts.add_namespace_lock_guard(guard);
        }
        dst_opts.expected_current_version_id = expected_current_version_id.clone();

        if _self_copy_lock_guard.is_some() {
            current_opts.no_lock = true;
        }
        let previous_current_sizes = match store.get_object_info(&bucket, &key, &current_opts).await {
            Ok(existing_obj_info) => {
                validate_existing_object_lock_for_write(&object_lock_config_state, &existing_obj_info, &dst_opts)?;
                if let Some(expected) = expected_current_version_id.as_deref()
                    && existing_obj_info.version_id.unwrap_or_default().to_string() != expected
                {
                    return Err(s3_error!(PreconditionFailed));
                }
                Some((existing_obj_info.size.max(0) as u64, quota_object_size(&existing_obj_info)))
            }
            Err(err) => {
                if expected_current_version_id.is_some() {
                    return Err(s3_error!(PreconditionFailed));
                }
                if !is_err_object_not_found(&err) && !is_err_version_not_found(&err) {
                    return Err(ApiError::from(err).into());
                }
                None
            }
        };

        let (mut effective_sse, mut effective_kms_key_id) = resolve_bucket_default_sse(
            bucket_sse_config.as_ref().map(|(config, _)| config),
            requested_sse,
            requested_kms_key_id,
            has_explicit_ssec,
        );

        let h = build_ssec_read_headers(
            copy_source_sse_customer_algorithm.as_ref(),
            copy_source_sse_customer_key.as_ref(),
            copy_source_sse_customer_key_md5.as_ref(),
        );

        let copy_principal = SseKmsPrincipal::from_request(&req);

        if source_bucket_lifecycle_guard.is_lock_lost() {
            return Err(ApiError::from(StorageError::NamespaceLockQuorumUnavailable {
                mode: "copy_source_bucket_generation",
                bucket: src_bucket.clone(),
                object: src_key.clone(),
                required: 1,
                achieved: 0,
            })
            .into());
        }

        let (gr, source_cancellation) = store
            .get_object_reader_for_copy(&src_bucket, &src_key, None, h, &src_get_opts)
            .await
            .map_err(map_get_object_reader_error)?;

        // The commit owner is intentionally detached so SetDisk can finish
        // its rename/cleanup and post-commit publication if the HTTP caller
        // goes away.  Keep a request-owned guard for the source producer:
        // cancellation drops the source read promptly, while the detached
        // commit task retains the guards it needs to complete safely.
        let _source_cancellation_guard = source_cancellation.clone().drop_guard();

        let mut src_info = gr.object_info.clone();

        // A copy reads the source plaintext, so it needs the source key's decrypt permission
        // as well as the destination key's generate permission below. The source read resolves
        // its material inside the object layer, which has no request identity, so the check
        // happens here.
        authorize_sse_kms_object_read(copy_principal.as_ref(), &src_info.user_defined).await?;

        // Capture the version actually read from the source before src_info is mutated/consumed
        // below. This is the exact source version copied (issue #4976): the response must echo it
        // via x-amz-copy-source-version-id, distinct from the destination version_id.
        let src_resolved_version_id = src_info.version_id;

        // Source object's existing checksum, if any. When the copy does not request a new
        // algorithm, AWS preserves the source object's checksum on the destination (#4996); the
        // copy does not transform the plaintext, so we carry the stored value over unchanged
        // rather than re-hashing every byte.
        let src_checksum = src_info.checksum.as_ref().and_then(|bytes| {
            let (pairs, _) = rustfs_rio::read_checksums(bytes.as_ref(), 0);
            pairs
                .into_iter()
                .find_map(|(k, v)| rustfs_rio::Checksum::new_from_string(&k, &v))
        });

        // Validate copy source conditions
        if let Some(if_match) = copy_source_if_match {
            if let Some(ref etag) = src_info.etag {
                if let Some(strong_etag) = if_match.into_etag() {
                    if ETag::Strong(etag.clone()) != strong_etag {
                        return Err(s3_error!(PreconditionFailed));
                    }
                } else {
                    // Weak ETag or Any (*) in If-Match should fail per RFC 9110
                    return Err(s3_error!(PreconditionFailed));
                }
            } else {
                return Err(s3_error!(PreconditionFailed));
            }
        }

        if let Some(if_none_match) = copy_source_if_none_match
            && let Some(ref etag) = src_info.etag
            && let Some(strong_etag) = if_none_match.into_etag()
            && ETag::Strong(etag.clone()) == strong_etag
        {
            return Err(s3_error!(PreconditionFailed));
        }

        // A same-name copy is normally serviced as a metadata-only update: the store layer
        // rewrites xl.meta in place and leaves the data blocks alone. That shortcut is only sound
        // when the destination's physical bytes are identical to the source's, and encryption
        // breaks exactly that. The destination metadata is rebuilt from scratch below —
        // `strip_managed_encryption_metadata` drops the source DEK and `sse_encryption` mints a
        // fresh one — so reusing the stored ciphertext would leave a new DEK sitting beside bytes
        // it cannot decrypt, permanently destroying the object (GET fails with an AEAD tag
        // mismatch). The mirror case is worse because it is silent: an encrypted source copied
        // without any destination SSE keeps its ciphertext while losing the key metadata, so GET
        // hands back raw ciphertext as if it were plaintext. So whenever either side is
        // encrypted, leave metadata_only = false and let the store layer do a full read/write
        // rewrite through put_object, the same resolution the versioned historical-restore path
        // uses (issue #4238, crates/ecstore/src/store/object.rs).
        //
        // This mirrors MinIO's `isSourceEncrypted || isTargetEncrypted -> metadataOnly = false`
        // in CopyObjectHandler, with one deliberate difference: MinIO decides "target encrypted"
        // from request headers alone, while `effective_sse` here also resolves the bucket default
        // encryption rule. `sse_encryption` mints a DEK from that resolved value, so a
        // header-only check would miss a self-copy under a bucket default rule. The source half
        // deliberately reuses `ObjectInfo::is_encrypted` rather than naming individual headers,
        // so a future encryption flavour is covered here the moment it is recognised there.
        //
        // The zero-copy shortcut is only recoverable for encrypted objects by *preserving* the
        // DEK that sealed the bytes and re-wrapping it under a new master key (MinIO's
        // `rotateKey` + `keyRotation` flag, which is why it may keep metadataOnly = true). RustFS
        // has no such rewrap primitive today; adding one is backlog#1637, and it would enter here
        // as an explicit exception rather than by relaxing this guard.
        let copy_changes_encryption = src_info.is_encrypted() || effective_sse.is_some() || has_explicit_ssec;
        if cp_src_dst_same && src_info.transitioned_object.tier.is_empty() && !copy_changes_encryption {
            src_info.metadata_only = true;
        }

        // Extract user_defined from Arc for mutation; it will be re-wrapped after all edits.
        let mut user_defined = (*src_info.user_defined).clone();
        let effective_tags = replacement_tags.unwrap_or_else(|| (*src_info.user_tags).clone());
        if !replaces_metadata {
            let source_expires = src_info.expires.map(Timestamp::from);
            insert_expires_metadata(&mut user_defined, source_expires.as_ref())?;
        }

        strip_managed_encryption_metadata(&mut user_defined);
        remove_str(&mut user_defined, SUFFIX_PLAINTEXT_CHECKSUM);

        let destination_storage_class = storage_class
            .as_ref()
            .map(StorageClass::as_str)
            .unwrap_or(storageclass::STANDARD);
        src_info.storage_class = Some(destination_storage_class.to_string());

        let actual_size = src_info.get_actual_size().map_err(ApiError::from)?;

        let length = actual_size;

        let mut compress_metadata = HashMap::new();

        let should_compress = is_disk_compressible(&req.headers, &key) && actual_size > MIN_DISK_COMPRESSIBLE_SIZE as i64;

        if should_compress {
            insert_str(
                &mut compress_metadata,
                SUFFIX_COMPRESSION,
                compression_metadata_value(CompressionAlgorithm::default()),
            );
            insert_str(&mut compress_metadata, SUFFIX_ACTUAL_SIZE, actual_size.to_string());
        } else {
            remove_str(&mut user_defined, SUFFIX_COMPRESSION);
            remove_str(&mut user_defined, SUFFIX_ACTUAL_SIZE);
            remove_str(&mut user_defined, SUFFIX_COMPRESSION_SIZE);
        }

        // Handle MetadataDirective REPLACE: replace user metadata while preserving system metadata.
        // System metadata (compression, encryption) is added after this block to ensure
        // it's not cleared by the REPLACE operation.
        if let Some(replacement_metadata) = replacement_metadata {
            user_defined = replacement_metadata;
            src_info.content_type = content_type.clone();
            src_info.content_encoding = content_encoding.as_deref().and_then(normalize_content_encoding_for_storage);
            src_info.expires = expires_timestamp.map(OffsetDateTime::from);
        } else if metadata_directive.is_some() || website_redirect_location.is_some() {
            user_defined.retain(|key, _| !key.eq_ignore_ascii_case(AMZ_WEBSITE_REDIRECT_LOCATION));
            if let Some(website_redirect_location) = website_redirect_location {
                user_defined.insert(AMZ_WEBSITE_REDIRECT_LOCATION.to_string(), website_redirect_location);
            }
        }

        user_defined.retain(|key, _| !key.eq_ignore_ascii_case(AMZ_STORAGE_CLASS));
        if destination_storage_class != storageclass::STANDARD {
            user_defined.insert(AMZ_STORAGE_CLASS.to_string(), destination_storage_class.to_string());
        }

        user_defined.retain(|key, _| !key.eq_ignore_ascii_case(AMZ_OBJECT_TAGGING));
        if !effective_tags.is_empty() {
            user_defined.insert(AMZ_OBJECT_TAGGING.to_string(), effective_tags.clone());
        }
        src_info.user_tags = Arc::new(effective_tags);

        let has_explicit_object_lock_retention = object_lock_mode.is_some() || object_lock_retain_until_date.is_some();
        remove_object_lock_metadata_for_copy(&mut user_defined);
        if let Some(object_lock_metadata) = build_put_like_object_lock_metadata(
            &bucket,
            &object_lock_config_state,
            object_lock_legal_hold_status,
            object_lock_mode,
            object_lock_retain_until_date,
        )? {
            user_defined.extend(object_lock_metadata);
        }
        apply_bucket_default_lock_retention(
            &bucket,
            &object_lock_config_state,
            &mut user_defined,
            has_explicit_object_lock_retention,
        )?;

        let mut write_plan = WritePlan::new();
        let mut reader = if should_compress {
            let algorithm = CompressionAlgorithm::default();
            let hrd = HashReader::from_stream(copy_source_read_stream(gr.stream, length), length, actual_size, None, None, false)
                .map_err(ApiError::from)?;
            write_plan = write_plan.with_compression(algorithm);
            hrd
        } else {
            HashReader::from_stream(copy_source_read_stream(gr.stream, length), length, actual_size, None, None, false)
                .map_err(ApiError::from)?
        };

        // Give the destination object a checksum so CopyObject returns it and a later checksum-mode
        // HEAD/GET matches (#4996). When the caller requests an algorithm, compute it fresh over the
        // copied plaintext (the hasher sits on the innermost reader so it digests plaintext). When
        // none is requested, carry the source object's stored checksum over unchanged — the copy
        // does not alter the plaintext, so re-hashing would be wasted work and would flatten a
        // multipart composite value.
        let destination_has_checksum = requested_checksum_type.is_some() || src_checksum.is_some();
        match requested_checksum_type {
            Some(checksum_type) => {
                reader.add_calculated_checksum(checksum_type).map_err(ApiError::from)?;
            }
            None => {
                if let Some(cs) = src_checksum {
                    reader.add_non_trailing_checksum(Some(cs), true).map_err(ApiError::from)?;
                }
            }
        }

        let encryption_request = EncryptionRequest {
            bucket: &bucket,
            key: &key,
            server_side_encryption: effective_sse.clone(),
            ssekms_key_id: effective_kms_key_id.clone(),
            ssekms_context,
            sse_customer_algorithm: sse_customer_algorithm.clone(),
            sse_customer_key,
            sse_customer_key_md5: sse_customer_key_md5.clone(),
            content_size: actual_size,
            principal: copy_principal.as_ref(),
        };

        if let Some(material) = sse_encryption(encryption_request).await? {
            effective_sse = Some(material.server_side_encryption.clone());
            effective_kms_key_id = material.kms_key_id.clone();

            write_plan = write_plan.with_encryption(material.write_encryption(None));

            user_defined.extend(encryption_material_to_metadata(&material)?);
            if destination_has_checksum {
                insert_str(&mut user_defined, SUFFIX_PLAINTEXT_CHECKSUM, "true".to_string());
            }
        }

        reader = write_plan.apply(reader, actual_size).map_err(ApiError::from)?;

        src_info.put_object_reader = Some(PutObjReader::new(reader));

        // check quota

        for (k, v) in compress_metadata {
            user_defined.insert(k, v);
        }

        // The source object's replication bookkeeping (internal status/timestamp,
        // replica state, and the surfaced x-amz-replication-status) describes the
        // SOURCE's replication history; carried onto the destination it fakes a
        // COMPLETED/REPLICA state for an object that never replicated (MinIO
        // filterReplicationStatusMetadata parity). Inbound replica writes are
        // exempt: the authorized replication request owns these keys (see
        // copy_dst_opts_with_replication_authorization above).
        if !dst_opts.replication_request {
            remove_source_replication_bookkeeping(&mut user_defined);
        }

        // Compute the replication decision exactly once per copy. The same
        // immutable `dsc` drives both the pending metadata written below and the
        // post-commit schedule (see the reuse site after copy_object), so a
        // replication-config hot update cannot split the two phases — same
        // contract as the PUT path (https://github.com/rustfs/backlog/issues/1320).
        // `must_replicate_object` itself declines inbound replica writes
        // (replication_request / REPLICA status), so replicas are never
        // re-scheduled outbound.
        let dsc = must_replicate_object(
            &bucket,
            &key,
            &user_defined,
            "".to_string(),
            dst_opts.delete_marker_replication_status(),
            dst_opts.clone(),
        )
        .await;
        if dsc.replicate_any() {
            insert_str(&mut user_defined, SUFFIX_REPLICATION_GENERATION, Uuid::new_v4().to_string());
            insert_str(&mut user_defined, SUFFIX_REPLICATION_TIMESTAMP, jiff::Zoned::now().to_string());
            insert_str(&mut user_defined, SUFFIX_REPLICATION_STATUS, dsc.pending_status().unwrap_or_default());
        }

        src_info.user_defined = Arc::new(user_defined);

        let quota_check = self
            .check_bucket_quota(
                &bucket,
                QuotaOperation::CopyObject,
                u64::try_from(actual_size).map_err(|_| S3Error::new(S3ErrorCode::UnexpectedContent))?,
            )
            .await?;
        let quota_enabled = quota_check.as_ref().is_some_and(|result| result.quota_limit.is_some());
        if let Some(quota_check) = quota_check.as_ref() {
            apply_quota_admission(&mut dst_opts, quota_check)?;
        }
        let previous_current_size = match previous_current_sizes {
            Some((_, Ok(logical_size))) if quota_enabled => Some(logical_size),
            Some((_, Err(err))) if quota_enabled => return Err(ApiError::from(err).into()),
            Some((physical_size, _)) => Some(physical_size),
            None => None,
        };
        if let Some(quota_check) = quota_check.as_ref() {
            ensure_object_size_within_quota(
                quota_check,
                u64::try_from(actual_size).map_err(|_| S3Error::new(S3ErrorCode::UnexpectedContent))?,
            )?;
        }
        let has_bucket_metadata = self.bucket_metadata_sys().is_some();
        let cache_adapter = self.object_data_cache();
        let _ = invalidate_object_data_cache_before_mutation(&cache_adapter, &bucket, &key).await;

        let copy_commit = spawn_traced_join({
            let store = Arc::clone(&store);
            let src_bucket = src_bucket.clone();
            let src_key = src_key.clone();
            let bucket = bucket.clone();
            let key = key.clone();
            let src_opts = src_opts.clone();
            let dst_opts = dst_opts.clone();
            async move {
                let _source_bucket_lifecycle_guard = source_bucket_lifecycle_guard;
                let _destination_bucket_lifecycle_guard_storage = destination_bucket_lifecycle_guard_storage;
                let _self_copy_lock_guard = _self_copy_lock_guard;

                let oi = store
                    .copy_object(&src_bucket, &src_key, &bucket, &key, &mut src_info, &src_opts, &dst_opts)
                    .await
                    .map_err(ApiError::from)?;

                // Reuse the single pre-commit replication decision (see `dsc` above) so
                // the persisted pending marker and the schedule always agree, mirroring
                // the PUT path.
                if dsc.replicate_any() {
                    schedule_object_replication(oi.clone(), Arc::clone(&store), dsc).await;
                }

                maybe_enqueue_transition_immediate(&oi, LcEventSrc::S3CopyObject).await;
                let _ = invalidate_object_data_cache_after_copy_success(&cache_adapter, &bucket, &key).await;

                let dest_versioned = BucketVersioningSys::prefix_enabled(&bucket, &key).await;
                if has_bucket_metadata {
                    let committed_size = quota_accounting_object_size(&oi, quota_enabled)?;
                    if dest_versioned {
                        record_bucket_object_version_write_memory(&bucket, previous_current_size, committed_size).await;
                    } else {
                        record_bucket_object_write_memory(&bucket, previous_current_size, committed_size).await;
                    }
                }

                rustfs_scanner::record_dirty_usage_object(&bucket, &key);
                Ok::<_, S3Error>((oi, dest_versioned))
            }
        });
        let (oi, dest_versioned) = copy_commit.await.map_err(|err| {
            S3Error::with_message(S3ErrorCode::InternalError, format!("copy object commit owner task failed: {err}"))
        })??;

        let raw_dest_version = oi.version_id.map(|v| v.to_string());
        let dest_version = if dest_versioned { raw_dest_version } else { None };

        // Echo the source version that was copied via x-amz-copy-source-version-id (issue #4976).
        // AWS/MinIO return this whenever the source bucket carries versioning (enabled or
        // suspended); render the null version as "null" like GET/HEAD do. This is the exact source
        // version, kept distinct from the destination version_id above.
        let src_versioned = BucketVersioningSys::prefix_enabled(&src_bucket, &src_key).await
            || BucketVersioningSys::prefix_suspended(&src_bucket, &src_key).await;
        let copy_source_version_id = if src_versioned {
            src_resolved_version_id.map(|vid| {
                if vid == Uuid::nil() {
                    "null".to_string()
                } else {
                    vid.to_string()
                }
            })
        } else {
            None
        };

        // Report the destination object's checksum in the response, decoded the same way GetObject
        // / HeadObject do so the value is identical to a later checksum-mode HEAD/GET (#4996).
        let response_checksums = oi
            .decrypt_checksums(0, &req.headers)
            .map(|(pairs, is_multipart)| classify_response_checksums(pairs, is_multipart))
            .unwrap_or_default();

        // warn!("copy_object oi {:?}", &oi);
        let object_info = oi.clone();
        let mut checksum_md5 = None;
        let mut checksum_sha512 = None;
        let mut checksum_xxhash3 = None;
        let mut checksum_xxhash64 = None;
        let mut checksum_xxhash128 = None;
        for (name, value) in response_checksums.extra {
            match name {
                "x-amz-checksum-md5" => checksum_md5 = Some(value),
                "x-amz-checksum-sha512" => checksum_sha512 = Some(value),
                "x-amz-checksum-xxhash3" => checksum_xxhash3 = Some(value),
                "x-amz-checksum-xxhash64" => checksum_xxhash64 = Some(value),
                "x-amz-checksum-xxhash128" => checksum_xxhash128 = Some(value),
                _ => {}
            }
        }
        let copy_object_result = CopyObjectResult {
            e_tag: oi.etag.as_ref().map(|etag| to_s3s_etag(etag)),
            last_modified: oi.mod_time.map(Timestamp::from),
            checksum_crc32: response_checksums.crc32,
            checksum_crc32c: response_checksums.crc32c,
            checksum_sha1: response_checksums.sha1,
            checksum_sha256: response_checksums.sha256,
            checksum_crc64nvme: response_checksums.crc64nvme,
            checksum_md5,
            checksum_sha512,
            checksum_xxhash3,
            checksum_xxhash64,
            checksum_xxhash128,
            checksum_type: response_checksums.checksum_type,
        };

        let output = CopyObjectOutput {
            copy_object_result: Some(copy_object_result),
            copy_source_version_id,
            server_side_encryption: effective_sse,
            ssekms_key_id: effective_kms_key_id,
            sse_customer_algorithm,
            sse_customer_key_md5,
            version_id: dest_version,
            ..Default::default()
        };

        let version_id = req.input.version_id.clone().unwrap_or_default();
        helper = helper.object(object_info).version_id(version_id);

        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderValue, Method};
    use s3s::dto::{ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration, ServerSideEncryptionRule};
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;

    #[test]
    fn local_copy_does_not_inherit_source_replication_generation() {
        let mut metadata = HashMap::from([
            (AMZ_BUCKET_REPLICATION_STATUS.to_string(), "COMPLETED".to_string()),
            ("x-amz-meta-owner".to_string(), "source".to_string()),
        ]);
        for suffix in [
            SUFFIX_REPLICATION_GENERATION,
            SUFFIX_REPLICATION_STATUS,
            SUFFIX_REPLICATION_TIMESTAMP,
            SUFFIX_REPLICA_STATUS,
            SUFFIX_REPLICA_TIMESTAMP,
        ] {
            insert_str(&mut metadata, suffix, format!("source-{suffix}"));
        }

        remove_source_replication_bookkeeping(&mut metadata);

        assert_eq!(metadata.get("x-amz-meta-owner").map(String::as_str), Some("source"));
        assert!(!metadata.contains_key(AMZ_BUCKET_REPLICATION_STATUS));
        for suffix in [
            SUFFIX_REPLICATION_GENERATION,
            SUFFIX_REPLICATION_STATUS,
            SUFFIX_REPLICATION_TIMESTAMP,
            SUFFIX_REPLICA_STATUS,
            SUFFIX_REPLICA_TIMESTAMP,
        ] {
            assert!(!rustfs_utils::http::contains_key_str(&metadata, suffix));
        }
    }

    // A malformed bucket-default algorithm reaches this resolution only through
    // corrupt or hand-edited bucket metadata (PutBucketEncryption validates the
    // value), so the invariant is pinned here rather than end-to-end: the copy
    // path must resolve managed AES256 exactly like PUT/extract. With an
    // unencrypted same-name source and no SSE-C, the resolved default alone
    // keeps `copy_changes_encryption` true, so the metadata-only shortcut stays
    // off while `sse_encryption` mints a fresh DEK (backlog#1826).
    #[test]
    fn copy_bucket_default_unknown_sse_algorithm_falls_back_to_aes256() {
        let config = ServerSideEncryptionConfiguration {
            rules: vec![ServerSideEncryptionRule {
                apply_server_side_encryption_by_default: Some(ServerSideEncryptionByDefault {
                    sse_algorithm: ServerSideEncryption::from(String::from("garbage")),
                    kms_master_key_id: None,
                }),
                blocked_encryption_types: None,
                bucket_key_enabled: None,
            }],
        };

        let effective_sse = config
            .rules
            .first()
            .and_then(|rule| rule.apply_server_side_encryption_by_default.as_ref())
            .map(bucket_default_write_sse);

        assert_eq!(effective_sse.as_ref().map(|sse| sse.as_str()), Some(ServerSideEncryption::AES256));

        // Valid algorithms map to themselves, byte-identical to the PUT path.
        for (configured, expected) in [
            (ServerSideEncryption::AES256, ServerSideEncryption::AES256),
            (ServerSideEncryption::AWS_KMS, ServerSideEncryption::AWS_KMS),
        ] {
            let sse = ServerSideEncryptionByDefault {
                sse_algorithm: ServerSideEncryption::from_static(configured),
                kms_master_key_id: None,
            };
            assert_eq!(bucket_default_write_sse(&sse).as_str(), expected);
        }
    }

    #[tokio::test]
    async fn copy_source_read_stream_maps_short_eof_to_service_unavailable() {
        let source = std::io::Cursor::new(b"abc".to_vec());
        let mut reader = HashReader::from_stream(copy_source_read_stream(source, 4), 4, 4, None, None, false)
            .expect("copy source hash reader should build");

        let mut output = Vec::new();
        let err = reader
            .read_to_end(&mut output)
            .await
            .expect_err("short copy source must fail before destination write succeeds");
        let api_error = ApiError::from(err);

        assert_eq!(api_error.code, S3ErrorCode::ServiceUnavailable);
        assert_ne!(api_error.code, S3ErrorCode::IncompleteBody);
    }

    #[tokio::test]
    async fn execute_copy_object_rejects_self_copy_without_replace_directive() {
        let input = CopyObjectInput::builder()
            .copy_source(CopySource::Bucket {
                bucket: "test-bucket".into(),
                key: "test-key".into(),
                version_id: None,
            })
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultObjectUsecase::without_context();

        let err = Box::pin(usecase.execute_copy_object(req)).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[tokio::test]
    async fn execute_copy_object_rejects_invalid_storage_class() {
        let input = CopyObjectInput::builder()
            .copy_source(CopySource::Bucket {
                bucket: "src-bucket".into(),
                key: "src-key".into(),
                version_id: None,
            })
            .bucket("dst-bucket".to_string())
            .key("dst-key".to_string())
            .storage_class(Some(StorageClass::from_static("INVALID")))
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultObjectUsecase::without_context();

        let err = Box::pin(usecase.execute_copy_object(req)).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidStorageClass);
    }

    #[tokio::test]
    async fn execute_copy_object_allows_self_copy_with_storage_class_change() {
        let input = CopyObjectInput::builder()
            .copy_source(CopySource::Bucket {
                bucket: "test-bucket".into(),
                key: "test-key".into(),
                version_id: None,
            })
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .storage_class(Some(StorageClass::from_static(storageclass::RRS)))
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultObjectUsecase::without_context();

        let err = Box::pin(usecase.execute_copy_object(req)).await.unwrap_err();
        // Self-copy with explicit storage class change must pass the self-copy guard.
        assert_ne!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn execute_self_copy_when_object_name_equals_bucket_observes_lock_order() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, DeleteBucketOptions, MakeBucketOptions};
        use s3s::access::S3Access as _;

        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        if current_app_context().is_none() {
            crate::app::runtime_sources::install_test_app_context(Arc::clone(&store)).await;
        }
        let ambient = current_app_context().expect("self-copy lock-order test requires an AppContext");
        let context = Arc::new(AppContext::new(Arc::clone(&store), ambient.iam(), ambient.kms()));
        let server_ctx = crate::app::runtime_sources::ServerContextSlot::new();
        assert!(server_ctx.install(Arc::clone(&context)));
        let fs = FS::with_server_ctx(server_ctx);

        let bucket = format!("self-copy-lock-order-{}", Uuid::new_v4());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create self-copy test bucket");
        let payload = b"object whose key equals its bucket".to_vec();
        let mut reader = PutObjReader::from_vec(payload.clone());
        let setup_opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };
        store
            .put_object(&bucket, &bucket, &mut reader, &setup_opts)
            .await
            .expect("put object whose key equals its bucket");

        let policy_json = format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"*"}},"Action":["s3:GetObject","s3:PutObject"],"Resource":["arn:aws:s3:::{bucket}/*"]}}]}}"#
        );
        let mut bucket_metadata = (*crate::storage::get_bucket_metadata(&bucket)
            .await
            .expect("self-copy bucket metadata should be cached"))
        .clone();
        bucket_metadata.policy_config = Some(serde_json::from_str(&policy_json).expect("self-copy policy should parse"));
        bucket_metadata.policy_config_json = policy_json.into_bytes();
        crate::storage::storage_api::set_bucket_metadata(bucket.clone(), bucket_metadata)
            .await
            .expect("publish self-copy test policy");

        let input = CopyObjectInput::builder()
            .copy_source(CopySource::Bucket {
                bucket: bucket.clone().into(),
                key: bucket.clone().into(),
                version_id: None,
            })
            .bucket(bucket.clone())
            .key(bucket.clone())
            .metadata_directive(Some(MetadataDirective::from_static(MetadataDirective::REPLACE)))
            .metadata(Some(HashMap::from([("lock-order".to_string(), "verified".to_string())])))
            .build()
            .expect("self-copy input should build");
        let mut req = build_request(input, Method::PUT);
        req.extensions.insert(crate::storage::access::ReqInfo::default());
        fs.copy_object(&mut req)
            .await
            .expect("authorize self-copy whose object key equals its bucket");

        let response = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            DefaultObjectUsecase::with_context(Some(context)).execute_copy_object(req),
        )
        .await
        .expect("lifecycle, authority, and exact object locks must not deadlock")
        .expect("self-copy whose object key equals its bucket should succeed");
        assert!(response.output.copy_object_result.is_some());
        let info = store
            .get_object_info(&bucket, &bucket, &ObjectOptions::default())
            .await
            .expect("self-copied object should remain readable");
        assert_eq!(info.size, payload.len() as i64);

        store
            .delete_bucket(
                &bucket,
                &DeleteBucketOptions {
                    force: true,
                    ..Default::default()
                },
            )
            .await
            .expect("clean up self-copy test bucket");
    }

    #[tokio::test]
    async fn execute_copy_object_allows_tiered_self_copy_with_storage_class_change() {
        let input = CopyObjectInput::builder()
            .copy_source(CopySource::Bucket {
                bucket: "test-bucket".into(),
                key: "test-key".into(),
                version_id: None,
            })
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .storage_class(Some(StorageClass::from_static(storageclass::STANDARD)))
            .metadata_directive(Some(MetadataDirective::from_static(MetadataDirective::REPLACE)))
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultObjectUsecase::without_context();

        let err = Box::pin(usecase.execute_copy_object(req)).await.unwrap_err();
        // Tiered self-copy with STANDARD storage class must pass all validation checks.
        // The call fails at store init (no store in unit tests), not at validation.
        assert_ne!(err.code(), &S3ErrorCode::InvalidRequest);
        assert_ne!(err.code(), &S3ErrorCode::NotImplemented);
    }

    #[tokio::test]
    async fn execute_copy_object_allows_self_copy_of_historical_version() {
        // Restoring a specific historical version onto the current key (same bucket/key with a
        // source versionId, default COPY directive) must pass the self-copy guard (issue #4238).
        let input = CopyObjectInput::builder()
            .copy_source(CopySource::Bucket {
                bucket: "test-bucket".into(),
                key: "test-key".into(),
                version_id: Some("11111111-1111-1111-1111-111111111111".into()),
            })
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultObjectUsecase::without_context();

        let err = Box::pin(usecase.execute_copy_object(req)).await.unwrap_err();
        // Must not be rejected by the self-copy guard; it fails later at store init instead.
        assert_ne!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn execute_copy_object_expected_current_historical_self_copy_releases_reader_before_write() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, DeleteBucketOptions, MakeBucketOptions};

        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        if current_app_context().is_none() {
            crate::app::runtime_sources::install_test_app_context(Arc::clone(&store)).await;
        }
        let ambient = current_app_context().expect("expected-current copy test requires an AppContext");
        let context = Arc::new(AppContext::new(Arc::clone(&store), ambient.iam(), ambient.kms()));
        let bucket = format!("copy-current-lock-{}", Uuid::new_v4());
        let object = "object.bin";
        store
            .make_bucket(
                &bucket,
                &MakeBucketOptions {
                    versioning_enabled: true,
                    ..Default::default()
                },
            )
            .await
            .expect("versioned copy test bucket should be created");

        const STREAMING_TEST_BODY_SIZE: usize = 256 * 1024;
        let historical_body = vec![b'h'; STREAMING_TEST_BODY_SIZE];
        let current_body = vec![b'c'; STREAMING_TEST_BODY_SIZE];
        let mut old_reader = PutObjReader::from_vec(historical_body.clone());
        let old = store
            .put_object(
                &bucket,
                object,
                &mut old_reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("historical source version should be written");
        assert!(!old.is_inline_fast_path_eligible(), "historical source must use the streaming path");
        let mut current_reader = PutObjReader::from_vec(current_body);
        let current = store
            .put_object(
                &bucket,
                object,
                &mut current_reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("current destination version should be written");
        let source_version_id = old.version_id.expect("historical source should have a version id");
        let current_version_id = current.version_id.expect("current destination should have a version id");

        let input = CopyObjectInput::builder()
            .copy_source(CopySource::Bucket {
                bucket: bucket.clone().into(),
                key: object.to_string().into(),
                version_id: Some(source_version_id.to_string().into()),
            })
            .bucket(bucket.clone())
            .key(object.to_string())
            .build()
            .expect("expected-current historical copy input should build");
        let mut req = build_request(input, Method::PUT);
        req.headers.insert(
            RUSTFS_EXPECTED_CURRENT_VERSION_ID,
            HeaderValue::from_str(&current_version_id.to_string()).expect("current version header should be valid"),
        );
        let response = temp_env::async_with_vars(
            [(rustfs_config::ENV_OBJECT_LOCK_OPTIMIZATION_ENABLE, Some("false"))],
            tokio::time::timeout(
                Duration::from_secs(10),
                DefaultObjectUsecase::with_context(Some(context)).execute_copy_object(req),
            ),
        )
        .await
        .expect("nonbuffered source reader must not deadlock behind the self-copy write lock")
        .expect("expected-current historical self-copy should succeed");
        assert!(response.output.copy_object_result.is_some());
        let source_version_id_text = source_version_id.to_string();
        assert_eq!(response.output.copy_source_version_id.as_deref(), Some(source_version_id_text.as_str()));
        let destination_version_id = response
            .output
            .version_id
            .clone()
            .expect("versioned self-copy should return its destination version");
        assert_ne!(destination_version_id, source_version_id_text);
        assert_ne!(destination_version_id, current_version_id.to_string());
        use tokio::io::AsyncReadExt;
        let verify_opts = ObjectOptions {
            version_id: Some(destination_version_id.clone()),
            versioned: true,
            ..Default::default()
        };
        let mut verified = store
            .get_object_reader(&bucket, object, None, HeaderMap::new(), &verify_opts)
            .await
            .expect("copied destination version should be readable");
        let mut copied_body = Vec::new();
        verified
            .stream
            .read_to_end(&mut copied_body)
            .await
            .expect("copied destination body should be readable");
        assert_eq!(copied_body, historical_body);
        assert_eq!(verified.object_info.version_id.map(|id| id.to_string()), Some(destination_version_id));

        store
            .delete_bucket(
                &bucket,
                &DeleteBucketOptions {
                    force: true,
                    ..Default::default()
                },
            )
            .await
            .expect("expected-current copy test bucket should be removed");
    }

    #[tokio::test]
    async fn execute_copy_object_allows_self_copy_of_null_version() {
        // A "null" source version id is a restore of the null version, not a no-op self-copy.
        let input = CopyObjectInput::builder()
            .copy_source(CopySource::Bucket {
                bucket: "test-bucket".into(),
                key: "test-key".into(),
                version_id: Some("null".into()),
            })
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultObjectUsecase::without_context();

        let err = Box::pin(usecase.execute_copy_object(req)).await.unwrap_err();
        assert_ne!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[tokio::test]
    async fn execute_copy_object_rejects_malformed_copy_source_version_id() {
        // A malformed (non-null, non-UUID) source version id is rejected up front.
        let input = CopyObjectInput::builder()
            .copy_source(CopySource::Bucket {
                bucket: "src-bucket".into(),
                key: "src-key".into(),
                version_id: Some("not-a-uuid".into()),
            })
            .bucket("dst-bucket".to_string())
            .key("dst-key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultObjectUsecase::without_context();

        let err = Box::pin(usecase.execute_copy_object(req)).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn execute_copy_object_rejects_expected_version_for_different_destination() {
        let input = CopyObjectInput::builder()
            .copy_source(CopySource::Bucket {
                bucket: "test-bucket".into(),
                key: "source-key".into(),
                version_id: Some(Uuid::new_v4().to_string().into()),
            })
            .bucket("test-bucket".to_string())
            .key("destination-key".to_string())
            .build()
            .unwrap();
        let mut req = build_request(input, Method::PUT);
        req.headers.insert(
            RUSTFS_EXPECTED_CURRENT_VERSION_ID,
            HeaderValue::from_str(&Uuid::new_v4().to_string()).unwrap(),
        );

        let err = Box::pin(DefaultObjectUsecase::without_context().execute_copy_object(req))
            .await
            .unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn execute_copy_object_refuses_a_bucket_whose_encryption_config_is_unreadable() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};

        let (store, context) = real_store_test_context().await;
        let bucket = format!("copy-sse-unreadable-{}", Uuid::new_v4());
        let source = "source.bin";
        let destination = "destination.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("unreadable-encryption copy bucket must be created");
        let mut reader = PutObjReader::from_vec(b"copied while the bucket still had a readable configuration".to_vec());
        store
            .put_object(&bucket, source, &mut reader, &ObjectOptions::default())
            .await
            .expect("copy source object must be written");
        install_unreadable_bucket_sse_config(&bucket).await;

        let input = CopyObjectInput::builder()
            .copy_source(CopySource::Bucket {
                bucket: bucket.clone().into(),
                key: source.into(),
                version_id: None,
            })
            .bucket(bucket.clone())
            .key(destination.to_string())
            .build()
            .expect("copy input must build");
        let usecase = DefaultObjectUsecase::with_context(Some(Arc::clone(&context)));

        let err = Box::pin(usecase.execute_copy_object(build_request(input, Method::PUT)))
            .await
            .expect_err("an unreadable bucket encryption configuration must refuse the copy");

        assert_eq!(err.code(), &S3ErrorCode::InternalError);
        let lookup_err = store
            .get_object_info(&bucket, destination, &ObjectOptions::default())
            .await
            .expect_err("a refused copy must not leave a destination object behind");
        assert!(is_err_object_not_found(&lookup_err), "{lookup_err}");
    }
}
