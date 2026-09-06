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

// RUSTFS_COMPAT_TODO(backlog-1337): legacy restores lack a liveness marker. Remove after the minimum supported release writes v1 on every restore.
const LEGACY_RESTORE_ORPHAN_GRACE: time::Duration = time::Duration::hours(24);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OngoingRestoreRecovery {
    ActiveOrUnsafe,
    ProbeWorker(Uuid),
    SupersedeLegacy,
}

fn consistent_metadata_value_case_insensitive<'a>(metadata: &'a HashMap<String, String>, key: &str) -> Option<&'a str> {
    let mut value = None;
    for (candidate_key, candidate_value) in metadata {
        if !candidate_key.eq_ignore_ascii_case(key) {
            continue;
        }
        if candidate_value.is_empty() || value.is_some_and(|current| current != candidate_value) {
            return None;
        }
        value = Some(candidate_value.as_str());
    }
    value
}

fn restore_request_date(metadata: &HashMap<String, String>) -> Option<OffsetDateTime> {
    let raw = consistent_metadata_value_case_insensitive(metadata, AMZ_RESTORE_REQUEST_DATE)?;
    OffsetDateTime::parse(raw, &Rfc3339)
        .or_else(|_| OffsetDateTime::parse(raw, &Rfc2822))
        .ok()
}

fn restore_operation_id(metadata: &HashMap<String, String>) -> Option<Uuid> {
    let raw = get_consistent_str(metadata, SUFFIX_RESTORE_OPERATION_ID)?;
    Uuid::parse_str(raw).ok().filter(|operation_id| !operation_id.is_nil())
}

fn classify_ongoing_restore(metadata: &HashMap<String, String>, now: OffsetDateTime) -> OngoingRestoreRecovery {
    let Some(operation_id) = restore_operation_id(metadata) else {
        return OngoingRestoreRecovery::ActiveOrUnsafe;
    };

    match get_consistent_str(metadata, SUFFIX_RESTORE_WORKER_LOCK) {
        Some(RESTORE_WORKER_LOCK_PROTOCOL_V1) => OngoingRestoreRecovery::ProbeWorker(operation_id),
        Some(_) => OngoingRestoreRecovery::ActiveOrUnsafe,
        None if contains_key_str(metadata, SUFFIX_RESTORE_WORKER_LOCK) => OngoingRestoreRecovery::ActiveOrUnsafe,
        None => {
            let Some(requested_at) = restore_request_date(metadata) else {
                return OngoingRestoreRecovery::ActiveOrUnsafe;
            };
            if requested_at
                .checked_add(LEGACY_RESTORE_ORPHAN_GRACE)
                .is_some_and(|reap_after| now >= reap_after)
            {
                OngoingRestoreRecovery::SupersedeLegacy
            } else {
                OngoingRestoreRecovery::ActiveOrUnsafe
            }
        }
    }
}

#[cfg(test)]
struct RestoreStatusCommitBarrierState {
    bucket: String,
    object: String,
    arrived: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(test)]
static RESTORE_STATUS_COMMIT_BARRIER: OnceLock<Mutex<Option<Arc<RestoreStatusCommitBarrierState>>>> = OnceLock::new();

#[cfg(test)]
pub(crate) struct RestoreStatusCommitBarrier {
    state: Arc<RestoreStatusCommitBarrierState>,
}

#[cfg(test)]
impl RestoreStatusCommitBarrier {
    pub(crate) fn install(bucket: &str, object: &str) -> Self {
        let state = Arc::new(RestoreStatusCommitBarrierState {
            bucket: bucket.to_string(),
            object: object.to_string(),
            arrived: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let mut slot = RESTORE_STATUS_COMMIT_BARRIER
            .get_or_init(|| Mutex::new(None))
            .lock()
            .expect("restore status commit barrier mutex should not poison");
        assert!(slot.is_none(), "restore status commit barrier must be installed by one test at a time");
        *slot = Some(Arc::clone(&state));
        Self { state }
    }

    pub(crate) async fn wait_until_paused(&self) {
        tokio::time::timeout(Duration::from_secs(30), self.state.arrived.notified())
            .await
            .expect("restore accept should reach the post-commit barrier");
    }
}

#[cfg(test)]
impl Drop for RestoreStatusCommitBarrier {
    fn drop(&mut self) {
        let mut slot = RESTORE_STATUS_COMMIT_BARRIER
            .get_or_init(|| Mutex::new(None))
            .lock()
            .expect("restore status commit barrier mutex should not poison");
        if slot.as_ref().is_some_and(|state| Arc::ptr_eq(state, &self.state)) {
            *slot = None;
        }
        self.state.release.notify_waiters();
    }
}

#[cfg(test)]
async fn maybe_pause_after_restore_status_commit(bucket: &str, object: &str) {
    let state = RESTORE_STATUS_COMMIT_BARRIER
        .get_or_init(|| Mutex::new(None))
        .lock()
        .expect("restore status commit barrier mutex should not poison")
        .as_ref()
        .filter(|state| state.bucket == bucket && state.object == object)
        .cloned();
    if let Some(state) = state {
        state.arrived.notify_one();
        state.release.notified().await;
    }
}

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

        // Typed S3 errors on every RestoreObject failure (backlog#2205): a
        // `Custom` code serializes as a generic 500, which makes SDK clients
        // retry client errors and conflicts alike.
        let rreq = rreq.ok_or_else(|| S3Error::with_message(S3ErrorCode::MalformedXML, "restore request is required"))?;

        // SELECT-type restore is not supported (backlog#1341). The restore
        // path can only write the retrieved bytes back to the source key, so
        // honouring a SELECT request overwrote the source object with
        // SELECT-only metadata (dropping `x-amz-restore`, user metadata and
        // tags on an unversioned bucket, or publishing a bogus latest version
        // on a versioned one) while never writing anything to
        // `OutputLocation.S3`. Reject before any guard, metadata write or
        // fabricated `x-amz-restore-output-path` response header.
        if rreq
            .type_
            .as_ref()
            .is_some_and(|type_| type_.as_str() == RestoreRequestType::SELECT)
        {
            return Err(S3Error::with_message(
                S3ErrorCode::NotImplemented,
                "SELECT restore requests are not supported.",
            ));
        }

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        // Validate the request shape before taking any lock or reading the
        // object: a malformed request or an illegal `Days` value is a client
        // error, and the validator messages are static — they carry no
        // backend or credential detail.
        if let Err(e) = validate_restore_request(&rreq, store.clone()) {
            return Err(S3Error::with_message(
                S3ErrorCode::InvalidRequest,
                format!("Restore object validation failed: {e}"),
            ));
        }

        let version_id_str = version_id.clone().unwrap_or_default();
        let mut opts = post_restore_opts(&version_id_str, &bucket, &object)
            .await
            .map_err(ApiError::from)?;
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

        let restore_operation_id = Some(Uuid::new_v4());
        let mut restore_worker_guard = if let Some(operation_id) = restore_operation_id {
            Some(
                store
                    .acquire_restore_worker_guard(operation_id)
                    .await
                    .map_err(|_| S3Error::with_message(S3ErrorCode::SlowDown, "restore object failed."))?,
            )
        } else {
            None
        };

        // Hold the restore-accept guard across the restore-status read, the
        // ongoing/already-restored decision, and the metadata write below, so
        // two concurrent POST ?restore cannot both observe ongoing=false and
        // both start a copy-back (backlog#1304). Reads and writes inside this
        // scope run with no_lock; the guard is dropped before the copy-back is
        // spawned so it never blocks readers.
        // Contention on the accept guard (e.g. a concurrent accept or an
        // in-flight commit on the same object) is transient — answer 503
        // SlowDown so SDK clients back off and retry instead of treating it
        // as a hard failure.
        let mut restore_bucket_lifecycle_guard = Some(acquire_copy_bucket_lifecycle_lock(store.as_ref(), &bucket).await?);
        if store.bucket_incarnation_id_from_disk(&bucket).await.map_err(ApiError::from)? != restore_bucket_incarnation_id {
            return Err(ApiError::from(StorageError::BucketNotFound(bucket.clone())).into());
        }
        let mut accept_guard = {
            let guard = store
                .acquire_restore_accept_guard(&bucket, &object)
                .await
                .map_err(|_| S3Error::with_message(S3ErrorCode::SlowDown, "restore object failed."))?;
            opts.no_lock = true;
            Some(guard)
        };

        // A missing key or version must stay NoSuchKey / NoSuchVersion, and an
        // authorization or storage failure must keep its own identity, so map
        // the storage error instead of flattening it (backlog#2205).
        let mut obj_info = store.get_object_info(&bucket, &object, &opts).await.map_err(ApiError::from)?;

        // Restoring an object that was never transitioned is the S3
        // InvalidObjectState case, not an internal error.
        if obj_info.transitioned_object.status != lifecycle::TRANSITION_COMPLETE {
            return Err(S3Error::with_message(
                S3ErrorCode::InvalidObjectState,
                "The operation is not valid for the object's storage class.",
            ));
        }

        // A v1 generation owns a distributed worker-liveness lock. Probe that
        // lock only after releasing the object/bucket guards: the worker holds
        // worker-lock -> object-commit-lock, so probing in the opposite order
        // would create an ABBA cycle. If the probe succeeds, reacquire and
        // re-read the object before replacing the exact orphan generation.
        let mut superseded_worker_guard = None;
        if obj_info.restore_ongoing {
            match classify_ongoing_restore(obj_info.user_defined.as_ref(), OffsetDateTime::now_utc()) {
                OngoingRestoreRecovery::ActiveOrUnsafe => {
                    return Err(S3Error::with_message(
                        S3ErrorCode::RestoreAlreadyInProgress,
                        "Object restore is already in progress.",
                    ));
                }
                OngoingRestoreRecovery::SupersedeLegacy => {}
                OngoingRestoreRecovery::ProbeWorker(previous_operation_id) => {
                    drop(accept_guard.take());
                    drop(restore_bucket_lifecycle_guard.take());
                    let previous_worker_guard = store
                        .try_acquire_restore_worker_guard(previous_operation_id)
                        .await
                        .map_err(|_| S3Error::with_message(S3ErrorCode::SlowDown, "restore object failed."))?
                        .ok_or_else(|| {
                            S3Error::with_message(S3ErrorCode::RestoreAlreadyInProgress, "Object restore is already in progress.")
                        })?;

                    restore_bucket_lifecycle_guard = Some(acquire_copy_bucket_lifecycle_lock(store.as_ref(), &bucket).await?);
                    if store.bucket_incarnation_id_from_disk(&bucket).await.map_err(ApiError::from)?
                        != restore_bucket_incarnation_id
                    {
                        return Err(ApiError::from(StorageError::BucketNotFound(bucket.clone())).into());
                    }
                    accept_guard = Some(
                        store
                            .acquire_restore_accept_guard(&bucket, &object)
                            .await
                            .map_err(|_| S3Error::with_message(S3ErrorCode::SlowDown, "restore object failed."))?,
                    );
                    opts.no_lock = true;
                    obj_info = store.get_object_info(&bucket, &object, &opts).await.map_err(ApiError::from)?;
                    if obj_info.transitioned_object.status != lifecycle::TRANSITION_COMPLETE {
                        return Err(S3Error::with_message(
                            S3ErrorCode::InvalidObjectState,
                            "The operation is not valid for the object's storage class.",
                        ));
                    }
                    if obj_info.restore_ongoing {
                        if classify_ongoing_restore(obj_info.user_defined.as_ref(), OffsetDateTime::now_utc())
                            != OngoingRestoreRecovery::ProbeWorker(previous_operation_id)
                        {
                            return Err(S3Error::with_message(S3ErrorCode::SlowDown, "restore object failed."));
                        }
                        superseded_worker_guard = Some(previous_worker_guard);
                    }
                }
            }
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
        remove_str(&mut metadata, SUFFIX_RESTORE_OPERATION_ID);
        remove_str(&mut metadata, SUFFIX_RESTORE_WORKER_LOCK);

        let event_object_info = obj_info.clone();
        let obj_info_ = obj_info.clone();
        // Scopes the accept-guarded metadata write: everything below runs
        // inside the accept critical section, which is released right after.
        {
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
                    insert_str(&mut metadata, SUFFIX_RESTORE_WORKER_LOCK, RESTORE_WORKER_LOCK_PROTOCOL_V1.to_string());
                }
            }
            obj_info.user_defined = Arc::new(metadata);

            // Fence the compare-and-set write: if the accept guard was lost
            // (lock-service degradation), another node may have concurrently
            // accepted this restore — back off instead of committing a second
            // ongoing flag and double-starting the copy-back.
            if accept_guard.as_ref().is_some_and(|guard| guard.is_lock_lost())
                || restore_worker_guard.as_ref().is_some_and(|guard| guard.is_lock_lost())
            {
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
                .map_err(ApiError::from)?;
            rustfs_scanner::record_dirty_usage_object(&bucket, &object);
            #[cfg(test)]
            maybe_pause_after_restore_status_commit(&bucket, &object).await;
            drop(superseded_worker_guard.take());

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

        let restore_worker_guard = restore_worker_guard.take();
        spawn_traced(async move {
            let _restore_worker_guard = restore_worker_guard;
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
                rustfs_scanner::record_dirty_usage_object(&bucket_clone, &object_clone);
                debug!(bucket = %bucket_clone, object = %object_clone, "Transitioned object restored");
            }
        });

        let output = RestoreObjectOutput {
            request_charged: Some(RequestCharged::from_static(RequestCharged::REQUESTER)),
            restore_output_path: None,
        };
        helper = helper.object(event_object_info).version_id(version_id_str);
        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Method;
    use s3s::dto::RestoreRequest;

    fn ongoing_metadata(operation_id: Uuid) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        insert_str(&mut metadata, SUFFIX_RESTORE_OPERATION_ID, operation_id.to_string());
        metadata
    }

    #[test]
    fn ongoing_restore_v1_requires_consistent_nonempty_protocol_and_generation() {
        let operation_id = Uuid::from_u128(1);
        let now = OffsetDateTime::parse("2026-02-02T00:00:00Z", &Rfc3339).unwrap();

        let mut dual = ongoing_metadata(operation_id);
        insert_str(&mut dual, SUFFIX_RESTORE_WORKER_LOCK, RESTORE_WORKER_LOCK_PROTOCOL_V1.to_string());
        assert_eq!(classify_ongoing_restore(&dual, now), OngoingRestoreRecovery::ProbeWorker(operation_id));

        let mut single = HashMap::new();
        single.insert(
            rustfs_utils::http::internal_key_rustfs(SUFFIX_RESTORE_OPERATION_ID),
            operation_id.to_string(),
        );
        single.insert(
            rustfs_utils::http::internal_key_rustfs(SUFFIX_RESTORE_WORKER_LOCK),
            RESTORE_WORKER_LOCK_PROTOCOL_V1.to_string(),
        );
        assert_eq!(classify_ongoing_restore(&single, now), OngoingRestoreRecovery::ProbeWorker(operation_id));

        let mut minio_only = HashMap::new();
        minio_only.insert(
            format!("{}{}", rustfs_utils::http::MINIO_INTERNAL_PREFIX, SUFFIX_RESTORE_OPERATION_ID),
            operation_id.to_string(),
        );
        minio_only.insert(
            format!("{}{}", rustfs_utils::http::MINIO_INTERNAL_PREFIX, SUFFIX_RESTORE_WORKER_LOCK),
            RESTORE_WORKER_LOCK_PROTOCOL_V1.to_string(),
        );
        assert_eq!(
            classify_ongoing_restore(&minio_only, now),
            OngoingRestoreRecovery::ProbeWorker(operation_id)
        );

        let mut unknown_protocol = dual.clone();
        insert_str(&mut unknown_protocol, SUFFIX_RESTORE_WORKER_LOCK, "v2".to_string());
        assert_eq!(classify_ongoing_restore(&unknown_protocol, now), OngoingRestoreRecovery::ActiveOrUnsafe);

        let mut empty_protocol = dual.clone();
        insert_str(&mut empty_protocol, SUFFIX_RESTORE_WORKER_LOCK, String::new());
        assert_eq!(classify_ongoing_restore(&empty_protocol, now), OngoingRestoreRecovery::ActiveOrUnsafe);

        let mut conflicting_protocol = dual.clone();
        conflicting_protocol.insert(
            format!("{}{}", rustfs_utils::http::MINIO_INTERNAL_PREFIX, SUFFIX_RESTORE_WORKER_LOCK),
            "v2".to_string(),
        );
        assert_eq!(
            classify_ongoing_restore(&conflicting_protocol, now),
            OngoingRestoreRecovery::ActiveOrUnsafe
        );

        for invalid_generation in [String::new(), Uuid::nil().to_string(), "not-a-uuid".to_string()] {
            let mut metadata = dual.clone();
            insert_str(&mut metadata, SUFFIX_RESTORE_OPERATION_ID, invalid_generation);
            assert_eq!(classify_ongoing_restore(&metadata, now), OngoingRestoreRecovery::ActiveOrUnsafe);
        }

        let mut conflicting_generation = dual;
        conflicting_generation.insert(
            format!("{}{}", rustfs_utils::http::MINIO_INTERNAL_PREFIX, SUFFIX_RESTORE_OPERATION_ID),
            Uuid::from_u128(2).to_string(),
        );
        assert_eq!(
            classify_ongoing_restore(&conflicting_generation, now),
            OngoingRestoreRecovery::ActiveOrUnsafe
        );
    }

    #[test]
    fn legacy_ongoing_restore_is_superseded_only_after_a_valid_stale_request_date() {
        let operation_id = Uuid::from_u128(1);
        let now = OffsetDateTime::parse("2026-02-02T00:00:00Z", &Rfc3339).unwrap();

        for stale_date in [
            "2026-02-01T00:00:00Z",
            "2026-01-31T23:59:59Z",
            "Sun, 1 Feb 2026 00:00:00 GMT",
            "Sat, 31 Jan 2026 23:59:59 GMT",
        ] {
            let mut metadata = ongoing_metadata(operation_id);
            metadata.insert(AMZ_RESTORE_REQUEST_DATE.to_string(), stale_date.to_string());
            assert_eq!(
                classify_ongoing_restore(&metadata, now),
                OngoingRestoreRecovery::SupersedeLegacy,
                "legacy date {stale_date} should be stale"
            );
        }

        for unsafe_date in [
            None,
            Some("2026-02-01T00:00:01Z"),
            Some("2026-02-03T00:00:00Z"),
            Some("invalid"),
        ] {
            let mut metadata = ongoing_metadata(operation_id);
            if let Some(date) = unsafe_date {
                metadata.insert(AMZ_RESTORE_REQUEST_DATE.to_string(), date.to_string());
            }
            assert_eq!(
                classify_ongoing_restore(&metadata, now),
                OngoingRestoreRecovery::ActiveOrUnsafe,
                "legacy date {unsafe_date:?} must fail closed"
            );
        }

        let mut conflicting_date = ongoing_metadata(operation_id);
        conflicting_date.insert(AMZ_RESTORE_REQUEST_DATE.to_string(), "2026-01-01T00:00:00Z".to_string());
        conflicting_date.insert(AMZ_RESTORE_REQUEST_DATE.to_ascii_lowercase(), "2025-01-01T00:00:00Z".to_string());
        assert_eq!(classify_ongoing_restore(&conflicting_date, now), OngoingRestoreRecovery::ActiveOrUnsafe);
    }

    fn restore_request(days: Option<i32>) -> RestoreRequest {
        RestoreRequest {
            days,
            description: None,
            glacier_job_parameters: None,
            output_location: None,
            select_parameters: None,
            tier: None,
            type_: None,
        }
    }

    fn restore_input(bucket: &str, key: &str, rreq: RestoreRequest) -> RestoreObjectInput {
        RestoreObjectInput::builder()
            .bucket(bucket.to_string())
            .key(key.to_string())
            .restore_request(Some(rreq))
            .build()
            .expect("restore input should build")
    }

    /// backlog#2205: a missing restore body is a client error, not a 500.
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
        assert_eq!(err.code(), &S3ErrorCode::MalformedXML);
    }

    /// backlog#1341: a SELECT restore must be rejected outright — the restore
    /// path can only write back to the source key, never to
    /// `OutputLocation.S3`. Rejection happens before the store is resolved, so
    /// an uninitialized usecase still answers NotImplemented rather than the
    /// InternalError every request that gets past this point returns.
    #[tokio::test]
    async fn execute_restore_object_rejects_select_type() {
        let mut rreq = restore_request(None);
        rreq.type_ = Some(s3s::dto::RestoreRequestType::from_static(s3s::dto::RestoreRequestType::SELECT));

        let req = build_request(restore_input("test-bucket", "test-key", rreq), Method::POST);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_restore_object(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::NotImplemented);
    }

    /// backlog#2205: every RestoreObject failure that reaches storage must
    /// keep its typed S3 identity. Before this, a missing key, a malformed
    /// version-id, an illegal `Days` and an object that was never transitioned
    /// all collapsed into `Custom(...)` codes, which serialize as a retryable
    /// HTTP 500.
    #[tokio::test]
    #[serial_test::serial]
    async fn execute_restore_object_maps_failures_to_typed_s3_errors() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};

        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        let context = crate::app::gating_test_env::shared_gating_ambient().await;
        let bucket = format!("restore-typed-errors-{}", Uuid::new_v4().simple());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create restore test bucket");
        let mut reader = PutObjReader::from_vec(b"never transitioned".to_vec());
        store
            .put_object(&bucket, "local-object", &mut reader, &ObjectOptions::default())
            .await
            .expect("put untransitioned test object");

        let usecase = DefaultObjectUsecase::with_context(Some(context));

        // An illegal `Days` is a client error, rejected before any lock or
        // object read.
        let err = usecase
            .execute_restore_object(build_request(
                restore_input(&bucket, "local-object", restore_request(Some(0))),
                Method::POST,
            ))
            .await
            .expect_err("days=0 must be rejected");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);

        // A malformed version-id keeps InvalidArgument instead of being
        // flattened inside `post_restore_opts`.
        let mut input = restore_input(&bucket, "local-object", restore_request(Some(1)));
        input.version_id = Some("not-a-uuid".to_string());
        let err = usecase
            .execute_restore_object(build_request(input, Method::POST))
            .await
            .expect_err("malformed version-id must be rejected");
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);

        // A missing key stays NoSuchKey.
        let err = usecase
            .execute_restore_object(build_request(
                restore_input(&bucket, "missing-object", restore_request(Some(1))),
                Method::POST,
            ))
            .await
            .expect_err("missing key must be rejected");
        assert_eq!(err.code(), &S3ErrorCode::NoSuchKey);

        // Restoring an object that was never transitioned is the S3
        // InvalidObjectState case, not an internal error.
        let err = usecase
            .execute_restore_object(build_request(
                restore_input(&bucket, "local-object", restore_request(Some(1))),
                Method::POST,
            ))
            .await
            .expect_err("untransitioned object must be rejected");
        assert_eq!(err.code(), &S3ErrorCode::InvalidObjectState);
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
