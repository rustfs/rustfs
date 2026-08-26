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

//! RestoreObject path.

use super::*;

impl DefaultObjectUsecase {
    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_restore_object(&self, req: S3Request<RestoreObjectInput>) -> S3Result<S3Response<RestoreObjectOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let mut helper = OperationHelper::new(&req, EventName::ObjectRestorePost, S3Operation::RestoreObject);
        let RestoreObjectInput {
            bucket,
            key: object,
            restore_request: rreq,
            version_id,
            ..
        } = req.input.clone();

        validate_table_catalog_object_mutation(&bucket, &object).await?;

        let rreq = rreq.ok_or_else(|| {
            S3Error::with_message(S3ErrorCode::Custom("ErrValidRestoreObject".into()), "restore request is required")
        })?;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let version_id_str = version_id.clone().unwrap_or_default();
        let mut opts = post_restore_opts(&version_id_str, &bucket, &object)
            .await
            .map_err(|_| S3Error::with_message(S3ErrorCode::Custom("ErrPostRestoreOpts".into()), "restore object failed."))?;
        apply_bucket_generation_guard(&req, &bucket, &mut opts)?;
        // `apply_bucket_generation_guard` deliberately tolerates a missing guard
        // (only the S3 access layer installs one), so this must not hard-require
        // it. Resolve the current generation instead, exactly as the copy path
        // does. The fence is unaffected: the value is re-read from disk and
        // compared below, before the restore is admitted.
        let restore_bucket_incarnation_id = match opts.expected_bucket_incarnation_id {
            Some(incarnation_id) => incarnation_id,
            None => {
                let incarnation_id = store.bucket_incarnation_id_from_disk(&bucket).await.map_err(ApiError::from)?;
                opts.expected_bucket_incarnation_id = Some(incarnation_id);
                incarnation_id
            }
        };

        // SELECT-type restores skip both the ongoing check and the metadata
        // write below, so the accept guard would protect nothing for them —
        // they keep the plain (read-locked) accept path.
        let is_select = rreq.type_.as_ref().is_some_and(|t| t.as_str() == "SELECT");

        // Hold the restore-accept guard across the restore-status read, the
        // ongoing/already-restored decision, and the metadata write below, so
        // two concurrent (non-SELECT) POST ?restore cannot both observe
        // ongoing=false and both start a copy-back (backlog#1304). Reads and
        // writes inside this scope run with no_lock; the guard is dropped
        // before the copy-back is spawned so it never blocks readers.
        // Contention on the accept guard (e.g. a concurrent accept or an
        // in-flight commit on the same object) is transient — answer 503
        // SlowDown so SDK clients back off and retry instead of treating it
        // as a hard failure.
        let restore_bucket_lifecycle_guard = Some(acquire_copy_bucket_lifecycle_lock(store.as_ref(), &bucket).await?);
        if store.bucket_incarnation_id_from_disk(&bucket).await.map_err(ApiError::from)? != restore_bucket_incarnation_id {
            return Err(ApiError::from(StorageError::BucketNotFound(bucket.clone())).into());
        }
        let accept_guard = if is_select {
            None
        } else {
            let guard = store
                .acquire_restore_accept_guard(&bucket, &object)
                .await
                .map_err(|_| S3Error::with_message(S3ErrorCode::SlowDown, "restore object failed."))?;
            opts.no_lock = true;
            Some(guard)
        };

        let mut obj_info = store
            .get_object_info(&bucket, &object, &opts)
            .await
            .map_err(|_| S3Error::with_message(S3ErrorCode::Custom("ErrInvalidObjectState".into()), "restore object failed."))?;

        // Check if object is in a transitioned state
        if obj_info.transitioned_object.status != lifecycle::TRANSITION_COMPLETE {
            return Err(S3Error::with_message(
                S3ErrorCode::Custom("ErrInvalidTransitionedState".into()),
                "restore object failed.",
            ));
        }

        // Validate restore request
        if let Err(e) = validate_restore_request(&rreq, store.clone()) {
            return Err(S3Error::with_message(
                S3ErrorCode::Custom("ErrValidRestoreObject".into()),
                format!("Restore object validation failed: {}", e),
            ));
        }

        // Check if restore is already in progress. AWS answers this with
        // 409 RestoreAlreadyInProgress; a Custom code would serialize as a
        // retryable 500 and make SDK clients retry the conflict (backlog#1304).
        if obj_info.restore_ongoing && !is_select {
            return Err(S3Error::with_message(
                S3ErrorCode::RestoreAlreadyInProgress,
                "Object restore is already in progress.",
            ));
        }

        let mut already_restored = false;
        if let Some(restore_expires) = obj_info.restore_expires
            && !obj_info.restore_ongoing
            && restore_expires.unix_timestamp() != 0
        {
            already_restored = true;
        }

        let restore_expiry = lifecycle::expected_expiry_time(OffsetDateTime::now_utc(), *rreq.days.as_ref().unwrap_or(&1));
        let mut metadata = (*obj_info.user_defined).clone();
        let restore_operation_id = (!is_select && !already_restored).then(Uuid::new_v4);

        let mut header = HeaderMap::new();

        let event_object_info = obj_info.clone();
        let obj_info_ = obj_info.clone();
        if !is_select {
            obj_info.metadata_only = true;
            metadata.insert(AMZ_RESTORE_EXPIRY_DAYS.to_string(), rreq.days.unwrap_or(1).to_string());
            let request_date = OffsetDateTime::now_utc().format(&Rfc3339).map_err(|e| {
                S3Error::with_message(S3ErrorCode::InternalError, format!("format restore request date failed: {}", e))
            })?;
            metadata.insert(AMZ_RESTORE_REQUEST_DATE.to_string(), request_date);
            if already_restored {
                metadata.insert(
                    X_AMZ_RESTORE.as_str().to_string(),
                    RestoreStatus {
                        is_restore_in_progress: Some(false),
                        restore_expiry_date: Some(Timestamp::from(restore_expiry)),
                    }
                    .to_string(),
                );
            } else {
                metadata.insert(
                    X_AMZ_RESTORE.as_str().to_string(),
                    RestoreStatus {
                        is_restore_in_progress: Some(true),
                        restore_expiry_date: Some(Timestamp::from(OffsetDateTime::now_utc())),
                    }
                    .to_string(),
                );
                if let Some(id) = restore_operation_id {
                    insert_str(&mut metadata, SUFFIX_RESTORE_OPERATION_ID, id.to_string());
                }
            }
            obj_info.user_defined = Arc::new(metadata);

            // Fence the compare-and-set write: if the accept guard was lost
            // (lock-service degradation), another node may have concurrently
            // accepted this restore — back off instead of committing a second
            // ongoing flag and double-starting the copy-back.
            if accept_guard.as_ref().is_some_and(|g| g.is_lock_lost()) {
                return Err(S3Error::with_message(S3ErrorCode::SlowDown, "restore object failed."));
            }

            let mut restore_dst_opts = ObjectOptions {
                version_id: obj_info_.version_id.map(|v| v.to_string()),
                mod_time: obj_info_.mod_time,
                no_lock: true,
                expected_bucket_incarnation_id: Some(restore_bucket_incarnation_id),
                ..Default::default()
            };
            if let Some(guard) = restore_bucket_lifecycle_guard.as_ref() {
                restore_dst_opts.add_bucket_lifecycle_lock_guard(guard);
            }
            if let Some(guard) = accept_guard.as_ref() {
                guard.add_namespace_lock_fence(&mut restore_dst_opts);
            }
            store
                .clone()
                .copy_object(
                    &bucket,
                    &object,
                    &bucket,
                    &object,
                    &mut obj_info,
                    &ObjectOptions {
                        version_id: obj_info_.version_id.map(|v| v.to_string()),
                        // Inside the accept-guard critical section (see above).
                        no_lock: true,
                        ..Default::default()
                    },
                    &restore_dst_opts,
                )
                .await
                .map_err(|_| S3Error::with_message(S3ErrorCode::Custom("ErrCopyObject".into()), "restore object failed."))?;
            rustfs_scanner::record_dirty_usage_bucket(&bucket);

            if already_restored {
                let output = RestoreObjectOutput {
                    request_charged: Some(RequestCharged::from_static(RequestCharged::REQUESTER)),
                    restore_output_path: None,
                };
                helper = helper
                    .object(event_object_info.clone())
                    .version_id(version_id_str.clone())
                    .suppress_event();
                let result = Ok(S3Response::new(output));
                let _ = helper.complete(&result);
                return result;
            }
        }

        // The accept decision is committed; release the object write lock so
        // the background copy-back and concurrent reads are never blocked on it.
        drop(accept_guard);
        drop(restore_bucket_lifecycle_guard);

        // Handle output location for SELECT requests
        if let Some(output_location) = &rreq.output_location
            && let Some(s3) = &output_location.s3
            && !s3.bucket_name.is_empty()
        {
            let restore_object = Uuid::new_v4().to_string();
            if let Ok(header_value) = format!("{}{}{}", s3.bucket_name, s3.prefix, restore_object).parse() {
                header.insert(X_AMZ_RESTORE_OUTPUT_PATH, header_value);
            }
        }

        // Spawn restoration task in the background. Pin the copy-back to the
        // version the accept resolved and flagged: with a versionless request
        // on a versioned bucket, a PUT landing between the accept and the
        // copy-back would otherwise re-resolve "latest" to the new version,
        // fail (not transitioned), and strand the flagged version at
        // ongoing=true forever (backlog#1304).
        let store_clone = store.clone();
        let bucket_clone = bucket.clone();
        let object_clone = object.clone();
        let rreq_clone = rreq.clone();
        let version_id_clone = obj_info_
            .version_id
            .map(|v| v.to_string())
            .or_else(|| (opts.versioned || opts.version_suspended).then(|| Uuid::nil().to_string()));
        let versioned = opts.versioned;
        let version_suspended = opts.version_suspended;
        let mut restore_operation_metadata = HashMap::new();
        if let Some(id) = restore_operation_id {
            insert_str(&mut restore_operation_metadata, SUFFIX_RESTORE_OPERATION_ID, id.to_string());
        }

        spawn_traced(async move {
            let opts = ObjectOptions {
                transition: TransitionOptions {
                    restore_request: rreq_clone,
                    restore_expiry,
                    ..Default::default()
                },
                version_id: version_id_clone,
                versioned,
                version_suspended,
                expected_bucket_incarnation_id: Some(restore_bucket_incarnation_id),
                user_defined: restore_operation_metadata,
                ..Default::default()
            };

            if let Err(err) = store_clone
                .restore_transitioned_object(&bucket_clone, &object_clone, &opts)
                .await
            {
                warn!(
                    "unable to restore transitioned bucket/object {}/{}: {}",
                    bucket_clone,
                    object_clone,
                    err.to_string()
                );
            } else {
                rustfs_scanner::record_dirty_usage_bucket(&bucket_clone);
                debug!(bucket = %bucket_clone, object = %object_clone, "Transitioned object restored");
            }
        });

        let output = RestoreObjectOutput {
            request_charged: Some(RequestCharged::from_static(RequestCharged::REQUESTER)),
            restore_output_path: None,
        };
        helper = helper.object(event_object_info).version_id(version_id_str);
        let result = Ok(S3Response::with_headers(output, header));
        let _ = helper.complete(&result);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Method;
    use s3s::dto::RestoreRequest;

    #[tokio::test]
    async fn execute_restore_object_rejects_missing_restore_request() {
        let input = RestoreObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::POST);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_restore_object(req).await.unwrap_err();
        match err.code() {
            S3ErrorCode::Custom(code) => assert_eq!(code, "ErrValidRestoreObject"),
            code => panic!("unexpected error code: {:?}", code),
        }
    }

    #[tokio::test]
    async fn execute_restore_object_returns_internal_error_when_store_uninitialized() {
        let restore_request = RestoreRequest {
            days: Some(1),
            description: None,
            glacier_job_parameters: None,
            output_location: None,
            select_parameters: None,
            tier: None,
            type_: None,
        };
        let input = RestoreObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .restore_request(Some(restore_request))
            .build()
            .unwrap();

        let req = build_request(input, Method::POST);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_restore_object(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }
}
