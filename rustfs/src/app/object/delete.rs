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

//! DeleteObject / DeleteObjects path.

use super::*;

fn successful_delete_audit_objects(
    delete: &s3s::dto::Delete,
    successful_results: impl IntoIterator<Item = bool>,
) -> Vec<AuditObjectVersion> {
    delete
        .objects
        .iter()
        .zip(successful_results)
        .filter(|(_, successful)| *successful)
        .map(|(requested, _)| AuditObjectVersion::new(requested.key.clone(), requested.version_id.clone()))
        .collect()
}

fn normalize_delete_objects_version_id(
    version_id: Option<String>,
) -> std::result::Result<(Option<String>, Option<Uuid>), String> {
    let version_id = version_id.map(|v| v.trim().to_string()).filter(|v| !v.is_empty());
    match version_id {
        Some(id) => {
            if id.eq_ignore_ascii_case("null") {
                Ok((Some("null".to_string()), Some(Uuid::nil())))
            } else {
                let uuid = Uuid::parse_str(&id).map_err(|e| e.to_string())?;
                Ok((Some(id), Some(uuid)))
            }
        }
        None => Ok((None, None)),
    }
}

#[cfg(test)]
type DeleteSnapshotTestHook = (String, Arc<tokio::sync::Barrier>, Arc<tokio::sync::Barrier>);

#[cfg(test)]
static DELETE_SNAPSHOT_TEST_HOOK: OnceLock<Mutex<Option<DeleteSnapshotTestHook>>> = OnceLock::new();

#[cfg(test)]
static DELETE_SOURCE_TEST_HOOK: OnceLock<Mutex<Option<DeleteSnapshotTestHook>>> = OnceLock::new();

#[cfg(test)]
static DELETE_OBJECTS_AUTH_TEST_HOOK: OnceLock<Mutex<Option<DeleteSnapshotTestHook>>> = OnceLock::new();

#[cfg(test)]
pub(crate) fn install_delete_snapshot_test_hook(
    bucket: String,
    loaded: Arc<tokio::sync::Barrier>,
    resume: Arc<tokio::sync::Barrier>,
) {
    *DELETE_SNAPSHOT_TEST_HOOK
        .get_or_init(|| Mutex::new(None))
        .lock()
        .expect("delete snapshot test hook lock should not be poisoned") = Some((bucket, loaded, resume));
}

#[cfg(test)]
async fn wait_for_delete_snapshot_test_hook(bucket: &str) {
    let hook = {
        let mut slot = DELETE_SNAPSHOT_TEST_HOOK
            .get_or_init(|| Mutex::new(None))
            .lock()
            .expect("delete snapshot test hook lock should not be poisoned");
        if slot.as_ref().is_some_and(|(expected_bucket, _, _)| expected_bucket == bucket) {
            slot.take()
        } else {
            None
        }
    };
    if let Some((_bucket, loaded, resume)) = hook {
        loaded.wait().await;
        resume.wait().await;
    }
}

#[cfg(test)]
pub(crate) fn install_delete_source_test_hook(
    bucket: String,
    loaded: Arc<tokio::sync::Barrier>,
    resume: Arc<tokio::sync::Barrier>,
) {
    *DELETE_SOURCE_TEST_HOOK
        .get_or_init(|| Mutex::new(None))
        .lock()
        .expect("delete source test hook lock should not be poisoned") = Some((bucket, loaded, resume));
}

#[cfg(test)]
async fn wait_for_delete_source_test_hook(bucket: &str) {
    let hook = {
        let mut slot = DELETE_SOURCE_TEST_HOOK
            .get_or_init(|| Mutex::new(None))
            .lock()
            .expect("delete source test hook lock should not be poisoned");
        if slot.as_ref().is_some_and(|(expected_bucket, _, _)| expected_bucket == bucket) {
            slot.take()
        } else {
            None
        }
    };
    if let Some((_bucket, loaded, resume)) = hook {
        loaded.wait().await;
        resume.wait().await;
    }
}

#[cfg(test)]
pub(crate) fn install_delete_objects_auth_test_hook(
    bucket: String,
    loaded: Arc<tokio::sync::Barrier>,
    resume: Arc<tokio::sync::Barrier>,
) {
    *DELETE_OBJECTS_AUTH_TEST_HOOK
        .get_or_init(|| Mutex::new(None))
        .lock()
        .expect("delete objects auth test hook lock should not be poisoned") = Some((bucket, loaded, resume));
}

#[cfg(test)]
async fn wait_for_delete_objects_auth_test_hook(bucket: &str) {
    let hook = {
        let mut slot = DELETE_OBJECTS_AUTH_TEST_HOOK
            .get_or_init(|| Mutex::new(None))
            .lock()
            .expect("delete objects auth test hook lock should not be poisoned");
        if slot.as_ref().is_some_and(|(expected_bucket, _, _)| expected_bucket == bucket) {
            slot.take()
        } else {
            None
        }
    };
    if let Some((_bucket, loaded, resume)) = hook {
        loaded.wait().await;
        resume.wait().await;
    }
}

fn enrich_delete_replication_state_if_needed(
    snapshot: &DeleteReplicationConfigSnapshot,
    delete_object: &mut StorageDeletedObject,
    obj_info: &ObjectInfo,
) {
    let Some(replication_state) = delete_object.replication_state.as_ref() else {
        return;
    };
    if obj_info.replication_status != ReplicationStatusType::Replica
        && !replication_state.replicate_decision_str.is_empty()
        && (!replication_state.targets.is_empty() || !replication_state.purge_targets.is_empty())
    {
        return;
    }

    let Some(config) = snapshot.replication_config() else {
        return;
    };
    let version_id = if delete_object.delete_marker {
        None
    } else if delete_object.delete_marker_version_id.is_some() {
        delete_object.delete_marker_version_id
    } else {
        delete_object.version_id
    };
    if let Some(local_state) = delete_replication_state_from_config(
        config,
        obj_info,
        version_id,
        obj_info.replication_status == ReplicationStatusType::Replica,
    ) {
        set_deleted_object_replication_state(delete_object, &local_state);
    }
}

fn should_schedule_replica_delete_replication(
    snapshot: &DeleteReplicationConfigSnapshot,
    replication_source: &ObjectInfo,
    version_id: Option<Uuid>,
) -> bool {
    let Some(config) = snapshot.replication_config() else {
        return false;
    };

    delete_replication_state_from_config(config, replication_source, version_id, true).is_some()
}

fn validate_undo_delete_version(expected: Option<&str>, requested: Option<&str>) -> S3Result<()> {
    if expected.is_some() && expected != requested {
        return Err(s3_error!(PreconditionFailed));
    }
    Ok(())
}

fn delete_creates_delete_marker(opts: &ObjectOptions) -> bool {
    opts.version_id.is_none() && opts.versioned && !opts.version_suspended
}

fn delete_removes_current_object(opts: &ObjectOptions) -> bool {
    delete_request_targets_current(
        opts.version_id
            .as_deref()
            .and_then(|version_id| Uuid::parse_str(version_id).ok()),
    )
}

fn delete_request_targets_current(version_id: Option<Uuid>) -> bool {
    version_id.is_none() || version_id.is_some_and(|version_id| version_id.is_nil())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeleteMemoryUpdate {
    DeleteMarker,
    Object { size: u64, removed_current_object: bool },
}

fn delete_memory_update(
    creates_delete_marker: bool,
    committed_delete_marker: bool,
    requested_current: bool,
    accounting_size: Option<u64>,
    removed_current_object: bool,
) -> Option<DeleteMemoryUpdate> {
    if creates_delete_marker || (committed_delete_marker && requested_current) {
        return Some(DeleteMemoryUpdate::DeleteMarker);
    }

    (!committed_delete_marker)
        .then_some(accounting_size)
        .flatten()
        .map(|size| DeleteMemoryUpdate::Object {
            size,
            removed_current_object,
        })
}

async fn apply_delete_memory_update(bucket: &str, update: Option<DeleteMemoryUpdate>) {
    match update {
        Some(DeleteMemoryUpdate::DeleteMarker) => record_bucket_delete_marker_memory(bucket).await,
        Some(DeleteMemoryUpdate::Object {
            size,
            removed_current_object,
        }) => record_bucket_object_delete_memory(bucket, size, removed_current_object).await,
        None => {}
    }
}

/// `DeleteObjects` is idempotent. A raw filesystem `NotFound` can cross the
/// distributed delete path instead of its usual typed missing-object error.
fn is_delete_objects_not_found(error: &EcstoreError) -> bool {
    is_err_object_not_found(error)
        || is_err_version_not_found(error)
        || matches!(error, StorageError::Io(source) if source.kind() == std::io::ErrorKind::NotFound)
}

/// Bounded concurrency for the per-object pre-delete stat fanout in
/// `execute_delete_objects` (backlog#929 / HP-8). Keeps the metadata reads for
/// a 1000-key batch from serializing while capping the disk fanout pressure.
const DELETE_OBJECTS_PRE_STAT_CONCURRENCY: usize = 16;

/// backlog#929 (HP-8): whether the pre-delete `get_object_info` for one entry
/// of a DeleteObjects batch can be skipped without changing behavior.
///
/// The stat result feeds four consumers, and each must be provably idle:
/// - the app-layer object-lock admission check never runs for deletes that
///   create a delete marker, and non-lock buckets cannot hold retention or
///   legal-hold metadata (`bucket_lock_enabled == false`);
/// - replication reads its authoritative source metadata later while the
///   SetDisks write lock is held, so it does not consume this advisory stat;
/// - usage accounting for delete-marker creation goes through
///   `record_bucket_delete_marker_memory` and never reads the object size
///   (`accounting_creates_delete_marker` is computed from the same versioning
///   snapshot the accounting branch uses);
/// - transitioned-object (ILM tier) cleanup journaling is a no-op for
///   delete-marker creation because no version is removed, so `ObjSweeper`
///   produces no journal entry regardless of the stat result.
///
/// Object-lock enabled buckets always keep the stat, so their delete path is
/// byte-for-byte the pre-#929 one (see PR #4297).
fn can_skip_delete_objects_pre_stat(
    bucket_lock_enabled: bool,
    opts: &ObjectOptions,
    accounting_creates_delete_marker: bool,
) -> bool {
    !bucket_lock_enabled && delete_creates_delete_marker(opts) && accounting_creates_delete_marker
}

fn complete_delete_noop(
    helper: OperationHelper,
    bucket: String,
    key: String,
    version_id: Option<String>,
) -> (S3Result<S3Response<DeleteObjectOutput>>, OperationHelper) {
    let helper = helper
        .event_name(EventName::ObjectRemovedNoOP)
        .object(ObjectInfo {
            name: key,
            bucket,
            ..Default::default()
        })
        .version_id(version_id.unwrap_or_default());
    let result = Ok(S3Response::with_status(DeleteObjectOutput::default(), StatusCode::NO_CONTENT));
    let helper = helper.complete(&result);
    (result, helper)
}

fn delete_response_version_id(version_id: Option<Uuid>, synthetic_version_id: bool) -> Option<String> {
    if synthetic_version_id {
        None
    } else if version_id == Some(Uuid::nil()) {
        Some(NULL_VERSION_ID.to_string())
    } else {
        version_id.map(|version_id| version_id.to_string())
    }
}

/// Version identity for a `DeleteObjects` `<Deleted>` entry (and its
/// notification). A delete marker removed by version id carries no storage
/// `version_id` on the committed result, so fall back to the identity the
/// request addressed — clients correlate response entries against what they
/// sent (issue #6745). Marker creation keeps `None`: its request carried no
/// version id.
fn delete_entry_response_version_id(
    committed_version_id: Option<Uuid>,
    committed_delete_marker: bool,
    requested_version_id: Option<Uuid>,
    synthetic_version_id: bool,
) -> Option<String> {
    delete_response_version_id(committed_version_id, synthetic_version_id).or_else(|| {
        if committed_delete_marker {
            delete_response_version_id(requested_version_id, synthetic_version_id)
        } else {
            None
        }
    })
}

fn reduce_delete_objects_result<'a>(
    object: &ObjectToDelete,
    deleted: &'a StorageDeletedObject,
    error: Option<&EcstoreError>,
    synthetic_version_id: bool,
) -> Result<&'a StorageDeletedObject, s3s::dto::Error> {
    match error {
        None => Ok(deleted),
        Some(error) if is_delete_objects_not_found(error) => Ok(deleted),
        Some(error) => {
            let api_error = ApiError::from(error.clone());
            Err(s3s::dto::Error {
                code: Some(api_error.code.as_str().to_string()),
                key: Some(object.object_name.clone()),
                message: Some(api_error.message),
                version_id: delete_response_version_id(object.version_id, synthetic_version_id),
            })
        }
    }
}

impl DefaultObjectUsecase {
    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_delete_objects(
        &self,
        mut req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let helper = OperationHelper::new(&req, EventName::ObjectRemovedDelete, S3Operation::DeleteObjects).suppress_event();
        let request_context = helper.request_context_or_from_request(&req);
        let (bucket, delete) = {
            let bucket = req.input.bucket.clone();
            let delete = req.input.delete.clone();
            (bucket, delete)
        };

        if delete.objects.is_empty() || delete.objects.len() > 1000 {
            return Err(S3Error::with_message(
                S3ErrorCode::InvalidArgument,
                "No objects to delete or too many objects to delete".to_string(),
            ));
        }

        let is_owner = req_info_ref(&req).map(|info| info.is_owner).unwrap_or(false);
        if !recursive_force_delete_is_authorized(&req.headers, is_owner, false) {
            return Err(S3Error::with_message(
                S3ErrorCode::AccessDenied,
                "Recursive force-delete is restricted to administrative requests",
            ));
        }

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };
        // Capture the bucket generation before per-object authorization, but
        // do not expose a bucket-state error unless at least one object is authorized.
        let bucket_generation = load_bucket_generation_from_store(store.as_ref(), &req, &bucket).await;

        let bypass_governance = has_bypass_governance_header(&req.headers);

        #[derive(Default, Clone)]
        struct DeleteResult {
            delete_object: Option<StorageDeletedObject>,
            error: Option<s3s::dto::Error>,
            synthetic_version_id: bool,
            // The version identity the request addressed, before any
            // synthetic-directory synthesis. `None` means the request carried
            // no version id, i.e. a committed delete marker was created, not
            // removed (issue #6745).
            requested_version_id: Option<Uuid>,
        }

        let mut delete_results = vec![DeleteResult::default(); delete.objects.len()];

        struct AuthorizedDelete {
            idx: usize,
            object: ObjectToDelete,
        }

        let mut authorized_deletes = Vec::with_capacity(delete.objects.len());
        // Issue #5740: keep the first per-key denial of this bulk request at
        // warn and demote the rest to debug, so a denied 1000-key DeleteObjects
        // cannot flood the log.
        let mut bulk_denial_logged = false;
        for (idx, obj_id) in delete.objects.iter().enumerate() {
            let raw_version_id = obj_id.version_id.clone();
            let (version_id, version_uuid) = match normalize_delete_objects_version_id(raw_version_id.clone()) {
                Ok(parsed) => parsed,
                Err(err) => {
                    delete_results[idx].error = Some(s3s::dto::Error {
                        code: Some("NoSuchVersion".to_string()),
                        key: Some(obj_id.key.clone()),
                        message: Some(err),
                        version_id: raw_version_id,
                    });
                    continue;
                }
            };

            {
                let req_info = req_info_mut(&mut req)?;
                req_info.bucket = Some(bucket.clone());
                req_info.object = Some(obj_id.key.clone());
                req_info.version_id = version_id.clone();
            }

            let auth_res = authorize_request(&mut req, Action::S3Action(S3Action::DeleteObjectAction)).await;
            if auth_res.is_err() {
                if !bulk_denial_logged {
                    bulk_denial_logged = true;
                    req_info_mut(&mut req)?.suppress_denial_log = true;
                }
                delete_results[idx].error = Some(s3s::dto::Error {
                    code: Some("AccessDenied".to_string()),
                    key: Some(obj_id.key.clone()),
                    message: Some("Access Denied".to_string()),
                    version_id: version_id.clone(),
                });
                continue;
            }

            if bypass_governance {
                let auth_res = authorize_request(&mut req, Action::S3Action(S3Action::BypassGovernanceRetentionAction)).await;
                if auth_res.is_err() {
                    if !bulk_denial_logged {
                        bulk_denial_logged = true;
                        req_info_mut(&mut req)?.suppress_denial_log = true;
                    }
                    delete_results[idx].error = Some(s3s::dto::Error {
                        code: Some("AccessDenied".to_string()),
                        key: Some(obj_id.key.clone()),
                        message: Some("Access Denied".to_string()),
                        version_id: version_id.clone(),
                    });
                    continue;
                }
            }

            if let Err(err) = validate_table_catalog_object_mutation(&bucket, &obj_id.key).await {
                delete_results[idx].error = Some(s3s::dto::Error {
                    code: Some("InvalidRequest".to_string()),
                    key: Some(obj_id.key.clone()),
                    message: Some(err.to_string()),
                    version_id: version_id.clone(),
                });
                continue;
            }

            let synthetic_version_id = version_id.is_none() && is_dir_object(&obj_id.key);
            let object = ObjectToDelete {
                object_name: obj_id.key.clone(),
                version_id: version_uuid,
                synthetic_version_id,
                ..Default::default()
            };
            delete_results[idx].synthetic_version_id = synthetic_version_id;
            delete_results[idx].requested_version_id = version_uuid;

            authorized_deletes.push(AuthorizedDelete { idx, object });
        }

        if authorized_deletes.is_empty() {
            let output = DeleteObjectsOutput {
                deleted: Some(Vec::new()),
                errors: Some(delete_results.into_iter().filter_map(|result| result.error).collect()),
                ..Default::default()
            };
            let result = Ok(S3Response::new(output));
            let _ = helper.complete(&result);
            return result;
        }
        #[cfg(test)]
        wait_for_delete_objects_auth_test_hook(&bucket).await;
        req.extensions.insert(bucket_generation?);
        let bucket_lock_enabled = object_lock_checks_required(&bucket).await;

        let delete_config_snapshot = Arc::new(
            load_delete_config_snapshot(store.as_ref(), &bucket)
                .await
                .map_err(ApiError::from)?,
        );
        let version_cfg = delete_config_snapshot.versioning_config();
        let replicate_deletes = authorized_deletes
            .iter()
            .any(|authorized| has_active_delete_rule(&delete_config_snapshot, &authorized.object.object_name));

        struct PreparedDelete {
            idx: usize,
            object: ObjectToDelete,
            opts: ObjectOptions,
            skip_stat: bool,
        }

        // Phase 1 (serial): derive storage options from the request-scoped
        // configuration after every candidate has passed authorization.
        let mut prepared_deletes: Vec<PreparedDelete> = Vec::with_capacity(authorized_deletes.len());
        for authorized in authorized_deletes {
            let AuthorizedDelete { idx, object } = authorized;

            let metadata = extract_metadata(&req.headers);
            let opts: ObjectOptions = del_opts_with_versioning(
                &bucket,
                &object.object_name,
                object.version_id.map(|f| f.to_string()),
                &req.headers,
                metadata,
                version_cfg,
                false,
            )
            .map_err(ApiError::from)?;

            // backlog#929 (HP-8): the accounting branch after the store delete
            // decides delete-marker vs object-delete from this exact snapshot,
            // so evaluate it here with the same inputs to keep the stat-skip
            // decision and the accounting path provably consistent.
            let accounting_creates_delete_marker = object.version_id.is_none() && opts.versioned && !opts.version_suspended;
            let skip_stat = can_skip_delete_objects_pre_stat(bucket_lock_enabled, &opts, accounting_creates_delete_marker);

            prepared_deletes.push(PreparedDelete {
                idx,
                object,
                opts,
                skip_stat,
            });
        }

        struct AdmittedDelete {
            idx: usize,
            object: ObjectToDelete,
            versioned: bool,
            version_suspended: bool,
        }

        // Phase 2 (bounded concurrency, backlog#929 / HP-8): collect the
        // metadata needed for accounting and tier cleanup. Entries are
        // independent per key, and `buffered` preserves input order. Object
        // Lock admission is enforced later in set_disk under the write lock.
        let store_ref = &store;
        let bucket_ref = bucket.as_str();
        let admitted_deletes: Vec<AdmittedDelete> =
            futures::stream::iter(prepared_deletes.into_iter().map(|prepared| async move {
                let PreparedDelete {
                    idx,
                    mut object,
                    opts,
                    skip_stat,
                } = prepared;
                let synthetic_version_id = object.version_id.is_none() && is_dir_object(&object.object_name);
                if !skip_stat {
                    match store_ref.get_object_info(bucket_ref, &object.object_name, &opts).await {
                        Ok(_) => {}
                        Err(err) if is_err_object_not_found(&err) || is_err_version_not_found(&err) => {}
                        Err(err) => return Err(ApiError::from(err)),
                    }
                }

                if synthetic_version_id {
                    object.version_id = Some(Uuid::nil());
                }

                Ok::<_, ApiError>(AdmittedDelete {
                    idx,
                    object,
                    versioned: opts.versioned,
                    version_suspended: opts.version_suspended,
                })
            }))
            .buffered(DELETE_OBJECTS_PRE_STAT_CONCURRENCY)
            .try_collect()
            .await?;

        // Phase 3 (serial): apply outcomes in the original request order so
        // per-key success/failure reporting is unchanged.
        let mut object_to_delete = Vec::new();
        let mut object_to_delete_idx = Vec::new();
        let mut object_versioning = Vec::new();
        for admitted in admitted_deletes {
            object_to_delete_idx.push(admitted.idx);
            object_versioning.push((admitted.versioned, admitted.version_suspended));
            object_to_delete.push(admitted.object);
        }
        let cache_adapter = self.object_data_cache();
        let cache_keys_before_delete = object_to_delete
            .iter()
            .map(|object| object.object_name.clone())
            .collect::<Vec<_>>();
        invalidate_object_data_cache_objects_before_mutation(&cache_adapter, &bucket, cache_keys_before_delete.iter()).await;

        let mut storage_delete_opts = ObjectOptions {
            versioned: version_cfg.enabled(),
            version_suspended: version_cfg.suspended(),
            delete_replication_config_snapshot: Some(Arc::clone(&delete_config_snapshot)),
            object_lock_delete: Some(StorageObjectLockDeleteOptions { bypass_governance }),
            ..Default::default()
        };
        apply_bucket_generation_guard(&req, &bucket, &mut storage_delete_opts)?;
        let (dobjs, errs, accounting) = store
            .delete_objects_with_tier_delete_journal_and_accounting(&bucket, object_to_delete.clone(), storage_delete_opts)
            .await;

        let _manager = get_concurrency_manager();
        let _bucket_clone = bucket.clone();
        let _deleted_objects = dobjs.clone();
        if !errs.is_empty() && errs.iter().all(|err| err.as_ref().is_some_and(is_err_bucket_not_found)) {
            let result = Err(S3Error::with_message(S3ErrorCode::NoSuchBucket, "Bucket not found".to_string()));
            let _ = helper.complete(&result);
            return result;
        }

        for (i, err) in errs.iter().enumerate() {
            let didx = object_to_delete_idx[i];

            match reduce_delete_objects_result(
                &object_to_delete[i],
                &dobjs[i],
                err.as_ref(),
                delete_results[didx].synthetic_version_id,
            ) {
                Ok(deleted_object) => {
                    delete_results[didx].delete_object = Some(deleted_object.clone());
                    let (versioned, version_suspended) = object_versioning[i];
                    let creates_delete_marker = object_to_delete[i].version_id.is_none() && versioned && !version_suspended;
                    let committed_delete_marker = dobjs[i].delete_marker;
                    let delete_accounting = accounting.get(i).and_then(Option::as_ref);
                    // `requested_version_id` distinguishes marker creation
                    // (no version in the request) from marker removal by
                    // version id; an explicit null-version request maps to
                    // `Some(Uuid::nil())`, which `delete_request_targets_current`
                    // would treat as versionless and mis-record a marker
                    // removal as a marker creation (issue #6745).
                    let update = delete_memory_update(
                        creates_delete_marker,
                        committed_delete_marker,
                        delete_results[didx].requested_version_id.is_none(),
                        delete_accounting.and_then(|value| value.size),
                        delete_accounting.is_some_and(|value| value.removed_current_object),
                    );
                    apply_delete_memory_update(&bucket, update).await;
                }
                Err(error) => {
                    delete_results[didx].error = Some(error);
                }
            }
        }

        let deleted = delete_results
            .iter()
            .filter_map(|result| result.delete_object.as_ref().map(|object| (result, object)))
            .map(|(result, object)| DeletedObject {
                delete_marker: { if object.delete_marker { Some(true) } else { None } },
                delete_marker_version_id: delete_response_version_id(
                    object.delete_marker_version_id,
                    result.synthetic_version_id,
                ),
                key: Some(object.object_name.clone()),
                version_id: delete_entry_response_version_id(
                    object.version_id,
                    object.delete_marker,
                    result.requested_version_id,
                    result.synthetic_version_id,
                ),
            })
            .collect();
        let deleted_cache_keys = delete_results
            .iter()
            .filter_map(|result| result.delete_object.as_ref().map(|deleted| deleted.object_name.clone()))
            .collect::<Vec<_>>();
        invalidate_object_data_cache_objects_after_delete_success(&cache_adapter, &bucket, deleted_cache_keys.iter()).await;

        let errors = delete_results
            .iter()
            .filter_map(|v| v.error.clone())
            .collect::<Vec<s3s::dto::Error>>();
        let output = DeleteObjectsOutput {
            deleted: Some(deleted),
            errors: Some(errors),
            ..Default::default()
        };
        let helper = if helper.wants_audit_object_info() {
            let audit_objects =
                successful_delete_audit_objects(&delete, delete_results.iter().map(|result| result.delete_object.is_some()));
            helper.audit_objects(audit_objects)
        } else {
            helper
        };

        let replication_deletes = if replicate_deletes {
            delete_results
                .iter()
                .filter_map(|result| result.delete_object.as_ref())
                .filter(|dobj| deleted_object_has_pending_replication_delete(dobj))
                .cloned()
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        if !replication_deletes.is_empty() {
            let bucket_for_replication = bucket.clone();
            let replication_task = tokio::spawn(async move {
                let _activity_guard = DeleteTailActivityGuard::new(DeleteTailStage::Replication);
                schedule_replication_deletes(replication_deletes, bucket_for_replication, REPLICATE_INCOMING_DELETE.to_string())
                    .await;
            });
            // The spawned task owns every locally committed delete. Dropping the
            // join handle on request cancellation therefore cannot lose the tail.
            let _ = replication_task.await;
        }

        let req_headers = req.headers.clone();
        let notify = current_notify_interface_for_context(self.context.as_deref());
        let req_params = rustfs_targets::extract_params_header(&req_headers);
        let resp_elements =
            build_event_resp_elements(&S3Response::new(DeleteObjectsOutput::default()), &request_context.request_id);
        let deleted_any = delete_results.iter().any(|result| result.delete_object.is_some());
        let notify_bucket = bucket.clone();
        spawn_background_with_context(Some(request_context), async move {
            let _activity_guard = DeleteTailActivityGuard::new(DeleteTailStage::Notify);
            for res in delete_results {
                if let Some(dobj) = res.delete_object {
                    // `DeleteMarkerCreated` is a creation event; a delete
                    // marker removed by an explicit version id is a plain
                    // versioned delete (issue #6745).
                    let event_name = delete_event_name_for_marker(dobj.delete_marker && res.requested_version_id.is_none());
                    let event_args = EventArgsBuilder::new(
                        event_name,
                        notify_bucket.clone(),
                        convert_ecstore_object_info(ObjectInfo {
                            name: dobj.object_name.clone(),
                            bucket: notify_bucket.clone(),
                            ..Default::default()
                        }),
                    )
                    .version_id(
                        delete_entry_response_version_id(
                            dobj.version_id,
                            dobj.delete_marker,
                            res.requested_version_id,
                            res.synthetic_version_id,
                        )
                        .unwrap_or_default(),
                    )
                    .req_params(req_params.clone())
                    .resp_elements(resp_elements.clone())
                    .host(get_request_host(&req_headers))
                    .user_agent(get_request_user_agent(&req_headers))
                    .build();

                    notify.notify(event_args).await;
                }
            }
        });

        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        if deleted_any {
            rustfs_scanner::record_dirty_usage_bucket(&bucket);
        }
        // Record write operation for capacity management (inline to avoid per-request tokio::spawn overhead)
        let manager = get_capacity_manager();
        manager.record_write_operation().await;
        result
    }

    #[instrument(level = "info", skip(self, req))]
    pub async fn execute_delete_object(&self, mut req: S3Request<DeleteObjectInput>) -> S3Result<S3Response<DeleteObjectOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let mut helper = OperationHelper::new(&req, EventName::ObjectRemovedDelete, S3Operation::DeleteObject);
        let DeleteObjectInput {
            bucket, key, version_id, ..
        } = req.input.clone();

        // Validate object key
        validate_object_key(&key, "DELETE")?;

        let replica = req
            .headers
            .get(AMZ_BUCKET_REPLICATION_STATUS)
            .map(|v| v.to_str().unwrap_or_default() == ReplicationStatusType::Replica.as_str())
            .unwrap_or_default();

        if replica {
            authorize_request(&mut req, Action::S3Action(S3Action::ReplicateDeleteAction)).await?;
        }

        let is_owner = req_info_ref(&req).map(|info| info.is_owner).unwrap_or(false);
        if !recursive_force_delete_is_authorized(&req.headers, is_owner, replica) {
            return Err(S3Error::with_message(
                S3ErrorCode::AccessDenied,
                "Recursive force-delete is restricted to internal or administrative requests",
            ));
        }
        validate_table_catalog_object_mutation(&bucket, &key).await?;

        // Establish bucket existence before any bucket-metadata work (matches
        // PUT/GET): nonexistent buckets fail here instead of paying the
        // versioning lookups in del_opts/get_opts first. Resolve the store
        // through the request-bound server context (backlog#1052 S6), not the
        // process-global handle.
        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };
        validate_bucket_exists(&store, &bucket).await?;

        let metadata = extract_metadata(&req.headers);
        // Clone version_id before it's moved
        let version_id_clone = version_id.clone();
        let synthetic_version_id = version_id_clone.is_none() && is_dir_object(&key);

        let delete_config_snapshot = Arc::new(
            load_delete_config_snapshot(store.as_ref(), &bucket)
                .await
                .map_err(ApiError::from)?,
        );
        #[cfg(test)]
        wait_for_delete_snapshot_test_hook(&bucket).await;
        let version_cfg = delete_config_snapshot.versioning_config();
        let mut opts: ObjectOptions =
            del_opts_with_versioning(&bucket, &key, version_id, &req.headers, metadata, version_cfg, replica)
                .map_err(ApiError::from)?;
        opts.delete_replication_config_snapshot = Some(Arc::clone(&delete_config_snapshot));
        opts.object_lock_delete = Some(StorageObjectLockDeleteOptions {
            bypass_governance: has_bypass_governance_header(&req.headers),
        });
        apply_bucket_generation_guard(&req, &bucket, &mut opts)?;
        let force_delete = opts.delete_prefix;

        // let mut vid = opts.version_id.clone();

        if replica {
            opts.set_replica_status(ReplicationStatusType::Replica);

            // if opts.version_purge_status().is_empty() {
            //     vid = None;
            // }
        }

        let expected_current_version_id = expected_current_version_id(&req.headers)?;
        if expected_current_version_id.is_some() && (force_delete || !opts.versioned) {
            return Err(s3_error!(
                InvalidRequest,
                "Expected current version precondition requires a version-specific delete in a versioned bucket"
            ));
        }
        validate_undo_delete_version(expected_current_version_id.as_deref(), opts.version_id.as_deref())?;
        opts.expected_current_version_id = expected_current_version_id.clone();

        let replicate_force_delete = force_delete && !replica && has_active_delete_rule(&delete_config_snapshot, &key);
        let mut force_delete_intent = None;

        let get_opts = opts.clone();
        let existing_object_info = match store.get_object_info(&bucket, &key, &get_opts).await {
            Ok(obj_info) => Some(obj_info),
            Err(err) => {
                // If object not found, allow deletion to proceed (will return 204 No Content)
                if !is_err_object_not_found(&err) && !is_err_version_not_found(&err) {
                    return Err(ApiError::from(err).into());
                }
                None
            }
        };
        #[cfg(test)]
        wait_for_delete_source_test_hook(&bucket).await;

        let cache_adapter = self.object_data_cache();
        // A force (delete_prefix) delete removes every object under `key` as a
        // prefix, so invalidating only the exact key would strand every cached
        // body beneath it. Use the prefix primitive in that branch (ODC-27).
        if force_delete {
            let _ = invalidate_object_data_cache_prefix_before_mutation(&cache_adapter, &bucket, &key).await;
        } else {
            let _ = invalidate_object_data_cache_before_mutation(&cache_adapter, &bucket, &key).await;
        }

        if replicate_force_delete
            && let Some((target_arns, generation)) = force_delete_target_set(&delete_config_snapshot, &key)
            && !target_arns.is_empty()
        {
            let operation_id =
                persist_force_delete_intent(store.clone(), bucket.clone(), key.clone(), target_arns.clone(), generation)
                    .await
                    .map_err(ApiError::from)?;
            force_delete_intent = Some((operation_id, target_arns, generation));
        }

        let obj_info = {
            match store
                .delete_object_with_tier_delete_journal(&bucket, &key, opts.clone())
                .await
            {
                Ok(obj) => obj,
                Err(err) => {
                    if let Some((operation_id, _, _)) = force_delete_intent.as_ref()
                        && let Err(cleanup_error) =
                            crate::storage::storage_api::complete_force_delete_intent(store.clone(), *operation_id).await
                    {
                        warn!(
                            bucket = %bucket,
                            object = %key,
                            operation_id = %operation_id,
                            error = %cleanup_error,
                            "failed to remove uncommitted force-delete intent after local delete failure"
                        );
                    }
                    if is_err_bucket_not_found(&err) {
                        return Err(S3Error::with_message(S3ErrorCode::NoSuchBucket, "Bucket not found".to_string()));
                    }

                    if is_err_object_not_found(&err) || is_err_version_not_found(&err) {
                        let (result, _helper) = complete_delete_noop(helper, bucket, key, version_id_clone);
                        return result;
                    }

                    if matches!(&err, StorageError::PrefixAccessDenied(_, _))
                        && let Some(existing_object_info) = existing_object_info.as_ref()
                        && let Some(reason) = check_object_lock_for_deletion(
                            &bucket,
                            existing_object_info,
                            has_bypass_governance_header(&req.headers),
                        )
                        .await
                    {
                        return Err(S3Error::with_message(S3ErrorCode::AccessDenied, reason.error_message()));
                    }

                    return Err(ApiError::from(err).into());
                }
            }
        };

        if force_delete {
            let _ = invalidate_object_data_cache_prefix_after_delete(&cache_adapter, &bucket, &key).await;
        } else {
            let _ = invalidate_object_data_cache_after_delete_success(&cache_adapter, &bucket, &key).await;
        }

        // Fast in-memory update for immediate quota and admin usage consistency.
        // Prefix/force deletes and synthetic directory entries do not carry one
        // committed object identity; leave their cache delta to reconciliation.
        let update = if force_delete || obj_info.name.is_empty() || synthetic_version_id {
            None
        } else {
            // The storage commit returns this object's metadata while its
            // generation lock is held. Never fall back to a pre-delete stat:
            // an overwrite can commit between that stat and this delete.
            delete_memory_update(
                delete_creates_delete_marker(&opts),
                obj_info.delete_marker,
                opts.version_id.is_none(),
                quota_object_size(&obj_info).ok(),
                delete_removes_current_object(&opts),
            )
        };
        apply_delete_memory_update(&bucket, update).await;

        if obj_info.name.is_empty() {
            if let Some((operation_id, target_arns, generation)) = force_delete_intent {
                if let Err(error) = commit_force_delete_intent(store.clone(), operation_id).await {
                    warn!(
                        bucket = %bucket,
                        object = %key,
                        operation_id = %operation_id,
                        error = %error,
                        "failed to mark force-delete intent committed after local delete"
                    );
                }
                let generation = i64::try_from(generation.unix_timestamp_nanos()).unwrap_or(i64::MAX);
                schedule_replication_delete(
                    StorageDeletedObject {
                        object_name: key.clone(),
                        force_delete: true,
                        force_delete_id: Some(operation_id),
                        force_delete_target_arns: target_arns,
                        force_delete_generation: Some(generation),
                        ..Default::default()
                    },
                    bucket.clone(),
                    REPLICATE_INCOMING_DELETE.to_string(),
                )
                .await;
            } else if replicate_force_delete {
                let mut delete_object = StorageDeletedObject {
                    object_name: key.clone(),
                    force_delete: true,
                    ..Default::default()
                };
                if let Some(replication_state) = delete_replication_state_from_config(
                    delete_config_snapshot
                        .replication_config()
                        .unwrap_or_else(|| unreachable!("force-delete requires a replication config")),
                    &ObjectInfo {
                        bucket: bucket.clone(),
                        name: key.clone(),
                        ..Default::default()
                    },
                    None,
                    false,
                ) {
                    set_deleted_object_replication_state(&mut delete_object, &replication_state);
                }
                schedule_replication_delete(delete_object, bucket.clone(), REPLICATE_INCOMING_DELETE.to_string()).await;
            }
            // Prefix/force-delete returns empty ObjectInfo; still emit bucket notification so webhooks match S3 DELETE.
            helper = helper
                .event_name(delete_event_name_for_marker(false))
                .object(ObjectInfo {
                    name: key.clone(),
                    bucket: bucket.clone(),
                    ..Default::default()
                })
                .version_id(String::new());
            let result = Ok(S3Response::with_status(DeleteObjectOutput::default(), StatusCode::NO_CONTENT));
            // Match non-empty delete path: capacity manager write-op telemetry.
            let manager = get_capacity_manager();
            manager.record_write_operation().await;
            let _ = helper.complete(&result);
            rustfs_scanner::record_dirty_usage_bucket(&bucket);
            return result;
        }

        let deleted_replication_info = existing_object_info
            .as_ref()
            .filter(|_| should_use_existing_delete_replication_info(&opts, opts.version_id.is_some()));
        let _delete_tail_guard = DeleteTailActivityGuard::new(DeleteTailStage::Tail);
        let deleted_object_source = deleted_replication_info.unwrap_or(&obj_info);
        let replication_state_source = &obj_info;
        let deleted_delete_marker_version = deleted_replication_info.is_some_and(|info| info.delete_marker);

        let delete_replication_version_id = delete_replication_version_id(deleted_object_source, deleted_delete_marker_version);
        let schedule_delete_replication = if opts.replication_request && replica {
            should_schedule_replica_delete_replication(
                &delete_config_snapshot,
                replication_state_source,
                delete_replication_version_id,
            )
        } else {
            should_schedule_delete_replication(
                &opts,
                replication_state_source,
                deleted_delete_marker_version,
                opts.version_id.is_some(),
            )
        };

        if schedule_delete_replication {
            let _activity_guard = DeleteTailActivityGuard::new(DeleteTailStage::Replication);
            let mut deleted_object = StorageDeletedObject {
                delete_marker: deleted_object_source.delete_marker && !deleted_delete_marker_version,
                delete_marker_version_id: if deleted_object_source.delete_marker {
                    deleted_object_source.version_id
                } else {
                    None
                },
                object_name: key.clone(),
                version_id: if deleted_object_source.delete_marker {
                    None
                } else {
                    deleted_object_source.version_id
                },
                delete_marker_mtime: deleted_object_source.mod_time,
                replication_state: None,
                ..Default::default()
            };
            set_deleted_object_replication_state(&mut deleted_object, &replication_state_source.replication_state());
            enrich_delete_replication_state_if_needed(&delete_config_snapshot, &mut deleted_object, replication_state_source);
            schedule_replication_delete(deleted_object, bucket.clone(), REPLICATE_INCOMING_DELETE.to_string()).await;
        }

        let delete_marker = obj_info.delete_marker;
        let version_id = obj_info.version_id;
        let response_version_id = delete_response_version_id(version_id, synthetic_version_id);

        let output = DeleteObjectOutput {
            delete_marker: Some(delete_marker),
            version_id: response_version_id.clone(),
            ..Default::default()
        };

        // `DeleteMarkerCreated` is a creation event; a delete marker removed
        // by an explicit version id is a plain versioned delete (issue #6745).
        let event_name = delete_event_name_for_marker(delete_marker && version_id_clone.is_none());

        helper = helper.event_name(event_name);
        helper = helper.object(obj_info).version_id(response_version_id.unwrap_or_default());

        let result = Ok(S3Response::new(output));
        // Record write operation for capacity management (inline to avoid per-request tokio::spawn overhead)
        let manager = get_capacity_manager();
        manager.record_write_operation().await;
        let _ = helper.complete(&result);
        rustfs_scanner::record_dirty_usage_object(&bucket, &key);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, HeaderValue, Method};
    use s3s::dto::{
        Delete, DeleteMarkerReplication, DeleteMarkerReplicationStatus, DeleteReplication, DeleteReplicationStatus, Destination,
        ExistingObjectReplication, ExistingObjectReplicationStatus, ObjectIdentifier, ReplicaModifications,
        ReplicaModificationsStatus, ReplicationConfiguration, ReplicationRule, ReplicationRuleStatus, SourceSelectionCriteria,
    };
    use std::sync::Arc;

    #[test]
    fn delete_response_version_id_preserves_null_and_synthetic_semantics() {
        let version_id = Uuid::new_v4();

        assert_eq!(delete_response_version_id(Some(version_id), false), Some(version_id.to_string()));
        assert_eq!(delete_response_version_id(Some(Uuid::nil()), false), Some("null".to_string()));
        assert_eq!(delete_response_version_id(Some(Uuid::nil()), true), None);
        assert_eq!(delete_response_version_id(None, false), None);
    }

    #[test]
    fn delete_entry_response_version_id_echoes_requested_identity_for_marker_removal() {
        let version_id = Uuid::new_v4();

        // Marker removed by explicit null version id: echo `null`.
        assert_eq!(
            delete_entry_response_version_id(None, true, Some(Uuid::nil()), false),
            Some("null".to_string())
        );
        // Marker removed by a real version id: echo that id.
        assert_eq!(
            delete_entry_response_version_id(None, true, Some(version_id), false),
            Some(version_id.to_string())
        );
        // Marker creation (no version in the request): no version identity.
        assert_eq!(delete_entry_response_version_id(None, true, None, false), None);
        // Non-marker deletes keep the committed identity and never fall back.
        assert_eq!(
            delete_entry_response_version_id(Some(version_id), false, Some(Uuid::nil()), false),
            Some(version_id.to_string())
        );
        assert_eq!(delete_entry_response_version_id(None, false, Some(version_id), false), None);
        // Synthetic directory deletes stay without a version identity.
        assert_eq!(delete_entry_response_version_id(None, true, Some(Uuid::nil()), true), None);
    }

    #[tokio::test]
    async fn execute_delete_object_rejects_invalid_object_key() {
        let input = DeleteObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("bad\0key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::DELETE);
        let usecase = DefaultObjectUsecase::without_context();

        let err = Box::pin(usecase.execute_delete_object(req)).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn delete_not_found_completes_noop_event_with_version_context() {
        temp_env::with_var(rustfs_config::ENV_NOTIFY_ENABLE, Some("true"), || {
            crate::server::refresh_notify_module_enabled();
            for (version_id, expected_version) in [(None, ""), (Some("requested-version".to_string()), "requested-version")] {
                let input = DeleteObjectInput::builder()
                    .bucket("test-bucket".to_string())
                    .key("missing-key".to_string())
                    .version_id(version_id.clone())
                    .build()
                    .expect("delete input should build");
                let mut req = build_request(input, Method::DELETE);
                req.extensions.insert(crate::storage::access::ReqInfo {
                    bucket: Some("test-bucket".to_string()),
                    object: Some("missing-key".to_string()),
                    version_id: version_id.clone(),
                    ..Default::default()
                });
                let helper = OperationHelper::new(&req, EventName::ObjectRemovedDelete, S3Operation::DeleteObject);

                let (result, helper) =
                    complete_delete_noop(helper, "test-bucket".to_string(), "missing-key".to_string(), version_id);
                let event = helper.event_args().expect("successful no-op delete should retain an event");

                assert_eq!(result.expect("no-op delete should succeed").status, Some(StatusCode::NO_CONTENT));
                assert_eq!(event.event_name, EventName::ObjectRemovedNoOP);
                assert_eq!(event.bucket_name, "test-bucket");
                assert_eq!(event.object.name, "missing-key");
                assert_eq!(event.version_id, expected_version);
            }
        });
        crate::server::refresh_notify_module_enabled();
    }

    #[test]
    fn undo_delete_requires_version_id_to_match_expected_current() {
        let expected = Uuid::new_v4().to_string();
        assert!(validate_undo_delete_version(Some(&expected), Some(&expected)).is_ok());
        assert_eq!(
            validate_undo_delete_version(Some(&expected), Some(&Uuid::new_v4().to_string()))
                .unwrap_err()
                .code(),
            &S3ErrorCode::PreconditionFailed
        );
        assert_eq!(
            validate_undo_delete_version(Some(&expected), None).unwrap_err().code(),
            &S3ErrorCode::PreconditionFailed
        );
        assert!(validate_undo_delete_version(None, None).is_ok());
    }

    #[tokio::test]
    async fn execute_delete_objects_rejects_empty_object_list() {
        let input = DeleteObjectsInput::builder()
            .bucket("test-bucket".to_string())
            .delete(Delete {
                objects: vec![],
                quiet: None,
            })
            .build()
            .unwrap();

        let req = build_request(input, Method::POST);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_delete_objects(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn execute_delete_objects_rejects_more_than_one_thousand_objects_before_store_lookup() {
        let objects = (0..1001)
            .map(|idx| ObjectIdentifier {
                key: format!("test-key-{idx}"),
                version_id: None,
                ..Default::default()
            })
            .collect();
        let input = DeleteObjectsInput::builder()
            .bucket("test-bucket".to_string())
            .delete(Delete { objects, quiet: None })
            .build()
            .expect("delete objects input should build");

        let req = build_request(input, Method::POST);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_delete_objects(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn execute_delete_objects_returns_internal_error_when_store_uninitialized() {
        let input = DeleteObjectsInput::builder()
            .bucket("test-bucket".to_string())
            .delete(Delete {
                objects: vec![ObjectIdentifier {
                    key: "test-key".to_string(),
                    version_id: None,
                    ..Default::default()
                }],
                quiet: None,
            })
            .build()
            .unwrap();

        let req = build_request(input, Method::POST);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_delete_objects(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
        assert_eq!(err.message(), Some("Not init"));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn execute_delete_objects_rejects_bucket_recreated_after_authorization() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, DeleteBucketOptions, MakeBucketOptions};

        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        if current_app_context().is_none() {
            crate::app::runtime_sources::install_test_app_context(Arc::clone(&store)).await;
        }
        let context = current_app_context().expect("delete objects generation test requires an AppContext");
        let bucket = format!("delete-objects-generation-{}", Uuid::new_v4());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create authorized bucket generation");
        let mut reader = PutObjReader::from_vec(b"old generation".to_vec());
        store
            .put_object(&bucket, "object", &mut reader, &ObjectOptions::default())
            .await
            .expect("put old-generation object");

        let policy_json = format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"*"}},"Action":["s3:DeleteObject"],"Resource":["arn:aws:s3:::{bucket}/*"]}}]}}"#
        );
        let mut metadata = (*crate::storage::get_bucket_metadata(&bucket)
            .await
            .expect("authorized bucket metadata should be cached"))
        .clone();
        metadata.policy_config = Some(serde_json::from_str(&policy_json).expect("test policy should parse"));
        metadata.policy_config_json = policy_json.into_bytes();
        crate::storage::storage_api::set_bucket_metadata(bucket.clone(), metadata)
            .await
            .expect("publish test bucket policy");

        let input = DeleteObjectsInput::builder()
            .bucket(bucket.clone())
            .delete(Delete {
                objects: vec![ObjectIdentifier {
                    key: "object".to_string(),
                    version_id: None,
                    ..Default::default()
                }],
                quiet: None,
            })
            .build()
            .expect("delete objects input should build");
        let mut req = build_request(input, Method::POST);
        req.extensions.insert(crate::storage::access::ReqInfo::default());
        let loaded = Arc::new(tokio::sync::Barrier::new(2));
        let resume = Arc::new(tokio::sync::Barrier::new(2));
        install_delete_objects_auth_test_hook(bucket.clone(), Arc::clone(&loaded), Arc::clone(&resume));

        let usecase = DefaultObjectUsecase::with_context(Some(context));
        let delete = tokio::spawn(async move { usecase.execute_delete_objects(req).await });
        loaded.wait().await;

        store
            .delete_bucket(
                &bucket,
                &DeleteBucketOptions {
                    force: true,
                    ..Default::default()
                },
            )
            .await
            .expect("delete authorized bucket generation");
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("recreate same bucket name");
        let mut reader = PutObjReader::from_vec(b"new generation".to_vec());
        store
            .put_object(&bucket, "object", &mut reader, &ObjectOptions::default())
            .await
            .expect("put new-generation object");
        resume.wait().await;

        let err = delete
            .await
            .expect("delete objects task should join")
            .expect_err("old authorization must not delete from the recreated bucket");
        assert_eq!(err.code(), &S3ErrorCode::NoSuchBucket);
        store
            .get_object_info(&bucket, "object", &ObjectOptions::default())
            .await
            .expect("new-generation object must survive the stale batch request");
    }

    #[tokio::test]
    async fn execute_delete_object_allows_non_force_request_without_req_info_until_store_lookup() {
        let input = DeleteObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let err = DefaultObjectUsecase::without_context()
            .execute_delete_object(build_request(input, Method::DELETE))
            .await
            .expect_err("an uninitialized store should be reported after non-force admission");
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
        assert_eq!(err.message(), Some("Not init"));
    }

    #[test]
    fn delete_objects_audit_details_include_only_successful_request_entries() {
        let requested = vec![
            ObjectIdentifier {
                key: "first-key".to_string(),
                version_id: None,
                ..Default::default()
            },
            ObjectIdentifier {
                key: "denied-key".to_string(),
                version_id: Some(Uuid::new_v4().to_string()),
                ..Default::default()
            },
            ObjectIdentifier {
                key: "versioned-key".to_string(),
                version_id: Some("requested-version".to_string()),
                ..Default::default()
            },
        ];

        let objects = successful_delete_audit_objects(
            &Delete {
                objects: requested,
                quiet: Some(true),
            },
            [true, false, true],
        );

        assert_eq!(
            objects,
            vec![
                AuditObjectVersion::new("first-key".to_string(), None),
                AuditObjectVersion::new("versioned-key".to_string(), Some("requested-version".to_string())),
            ]
        );
    }

    #[test]
    fn delete_objects_audit_details_are_empty_when_every_entry_fails() {
        let requested = vec![ObjectIdentifier {
            key: "failed-key".to_string(),
            version_id: None,
            ..Default::default()
        }];

        assert!(
            successful_delete_audit_objects(
                &Delete {
                    objects: requested,
                    quiet: None,
                },
                [false]
            )
            .is_empty()
        );
    }

    #[test]
    fn normalize_delete_objects_version_id_preserves_explicit_null_marker() {
        let (wire_version_id, internal_version_id) =
            normalize_delete_objects_version_id(Some("null".to_string())).expect("null version marker should parse");

        assert_eq!(wire_version_id.as_deref(), Some("null"));
        assert_eq!(internal_version_id, Some(Uuid::nil()));

        let (wire_version_id, internal_version_id) =
            normalize_delete_objects_version_id(Some(" \t ".to_string())).expect("empty version marker should normalize");
        assert_eq!(wire_version_id, None);
        assert_eq!(internal_version_id, None);
    }

    #[test]
    fn delete_objects_treats_raw_io_not_found_as_idempotent() {
        assert!(is_delete_objects_not_found(&StorageError::FileNotFound));
        assert!(is_delete_objects_not_found(&StorageError::Io(std::io::Error::from(
            std::io::ErrorKind::NotFound,
        ))));
        assert!(!is_delete_objects_not_found(&StorageError::Io(std::io::Error::from(
            std::io::ErrorKind::PermissionDenied,
        ))));
        assert!(!is_delete_objects_not_found(&StorageError::DiskNotFound));
    }

    #[test]
    fn delete_objects_result_reducer_reports_raw_not_found_as_deleted() {
        let object = ObjectToDelete {
            object_name: "missing-key".to_string(),
            ..Default::default()
        };
        let deleted = StorageDeletedObject {
            object_name: object.object_name.clone(),
            ..Default::default()
        };
        let error = StorageError::Io(std::io::Error::from(std::io::ErrorKind::NotFound));

        let deleted = reduce_delete_objects_result(&object, &deleted, Some(&error), false)
            .expect("raw not-found must produce a deleted result");
        assert_eq!(deleted.object_name, "missing-key");
    }

    #[test]
    fn recursive_force_delete_requires_administrative_or_replica_context() {
        let mut headers = HeaderMap::new();
        headers.insert("x-rustfs-force-delete", HeaderValue::from_static("true"));

        assert!(!recursive_force_delete_is_authorized(&headers, false, false));
        assert!(recursive_force_delete_is_authorized(&headers, true, false));
        assert!(recursive_force_delete_is_authorized(&headers, false, true));
        assert!(recursive_force_delete_is_authorized(&HeaderMap::new(), false, false));
    }

    #[tokio::test]
    async fn execute_delete_object_rejects_untrusted_force_delete_before_store_access() {
        let input = DeleteObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("prefix/object".to_string())
            .build()
            .unwrap();
        let mut req = build_request(input, Method::DELETE);
        req.headers.insert("x-rustfs-force-delete", HeaderValue::from_static("true"));
        req.extensions.insert(crate::storage::access::ReqInfo::default());

        let err = DefaultObjectUsecase::without_context()
            .execute_delete_object(req)
            .await
            .expect_err("untrusted force-delete must be rejected before storage lookup");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }

    #[tokio::test]
    async fn execute_delete_objects_rejects_untrusted_force_delete_before_store_access() {
        let input = DeleteObjectsInput::builder()
            .bucket("test-bucket".to_string())
            .delete(Delete {
                objects: vec![ObjectIdentifier {
                    key: "prefix/object".to_string(),
                    version_id: None,
                    ..Default::default()
                }],
                quiet: None,
            })
            .build()
            .unwrap();
        let mut req = build_request(input, Method::POST);
        req.headers.insert("x-rustfs-force-delete", HeaderValue::from_static("true"));
        req.extensions.insert(crate::storage::access::ReqInfo::default());

        let err = DefaultObjectUsecase::without_context()
            .execute_delete_objects(req)
            .await
            .expect_err("untrusted force-delete must be rejected before storage lookup");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }

    // backlog#929 (HP-8): the pre-delete stat may only be skipped when every
    // consumer of its result is provably idle. Each guard flips one condition
    // to prove the skip is fenced on all four data dependencies.
    fn delete_marker_creating_opts() -> ObjectOptions {
        ObjectOptions {
            version_id: None,
            versioned: true,
            version_suspended: false,
            ..Default::default()
        }
    }

    #[test]
    fn delete_objects_pre_stat_skippable_for_delete_marker_on_plain_bucket() {
        assert!(can_skip_delete_objects_pre_stat(false, &delete_marker_creating_opts(), true));
    }

    #[test]
    fn delete_objects_pre_stat_kept_for_object_lock_buckets() {
        assert!(!can_skip_delete_objects_pre_stat(true, &delete_marker_creating_opts(), true));
    }

    #[test]
    fn delete_objects_pre_stat_kept_for_explicit_version_deletes() {
        let opts = ObjectOptions {
            version_id: Some(Uuid::new_v4().to_string()),
            versioned: true,
            version_suspended: false,
            ..Default::default()
        };
        assert!(!can_skip_delete_objects_pre_stat(false, &opts, true));
    }

    #[test]
    fn delete_objects_pre_stat_kept_for_unversioned_buckets() {
        // Unversioned deletes remove the current object: usage accounting needs
        // the object size and ILM tier cleanup needs the transition metadata.
        let opts = ObjectOptions {
            version_id: None,
            versioned: false,
            version_suspended: false,
            ..Default::default()
        };
        assert!(!can_skip_delete_objects_pre_stat(false, &opts, false));
    }

    #[test]
    fn delete_objects_pre_stat_kept_for_suspended_versioning() {
        let opts = ObjectOptions {
            version_id: None,
            versioned: true,
            version_suspended: true,
            ..Default::default()
        };
        assert!(!can_skip_delete_objects_pre_stat(false, &opts, false));
    }

    #[test]
    fn delete_objects_pre_stat_kept_when_accounting_snapshot_disagrees() {
        // If the accounting-side versioning snapshot does not also classify the
        // delete as a delete-marker creation, the stat must stay so usage
        // accounting keeps its size input.
        assert!(!can_skip_delete_objects_pre_stat(false, &delete_marker_creating_opts(), false));
    }

    #[test]
    fn delete_accounting_recognizes_explicit_null_as_current_object() {
        let opts = ObjectOptions {
            version_id: Some(Uuid::nil().to_string()),
            version_suspended: true,
            ..Default::default()
        };
        assert!(delete_removes_current_object(&opts));
        assert!(delete_request_targets_current(Some(Uuid::nil())));
        assert!(!delete_request_targets_current(Some(Uuid::new_v4())));
        assert!(!delete_removes_current_object(&ObjectOptions {
            version_id: Some(Uuid::new_v4().to_string()),
            ..Default::default()
        }));
    }

    #[test]
    fn compressed_object_delete_restores_usage_baseline() {
        let mut metadata = HashMap::new();
        insert_str(&mut metadata, SUFFIX_COMPRESSION, "klauspost/compress/s2".to_string());
        let object = ObjectInfo {
            size: 400,
            actual_size: 1000,
            user_defined: Arc::new(metadata),
            ..Default::default()
        };
        let accounting_size = quota_object_size(&object).expect("logical compressed size should be canonical");

        assert_eq!(
            delete_memory_update(false, false, true, Some(accounting_size), true),
            Some(DeleteMemoryUpdate::Object {
                size: 1000,
                removed_current_object: true,
            })
        );
    }

    #[test]
    fn invalid_accounting_metadata_is_reconciled_without_overflow() {
        assert_eq!(delete_memory_update(false, false, true, None, true), None);
        assert_eq!(
            delete_memory_update(false, true, true, None, true),
            Some(DeleteMemoryUpdate::DeleteMarker)
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn compressed_delete_requests_update_observed_usage_without_releasing_quota_floor() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, DeleteBucketOptions, MakeBucketOptions};
        use crate::app::storage_api::test::data_usage::apply_bucket_usage_memory_overlay;

        async fn observed_bucket_usage(bucket: &str) -> Option<u64> {
            let mut usage = rustfs_data_usage::DataUsageInfo::default();
            apply_bucket_usage_memory_overlay(&mut usage).await;
            usage.buckets_usage.get(bucket).map(|value| value.size)
        }

        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        if current_app_context().is_none() {
            crate::app::runtime_sources::install_test_app_context(Arc::clone(&store)).await;
        }
        let bucket = format!("compressed-delete-request-{}", Uuid::new_v4().simple());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create compressed delete request bucket");

        // Seed the process-local usage with the canonical logical bytes. The
        // direct storage PUT below intentionally does not apply an app-layer
        // usage delta; the two real DELETE requests must remove exactly this
        // amount through their request-layer wiring.
        crate::app::storage_api::test::data_usage::seed_bucket_usage_memory_for_test(&bucket, 2_000).await;

        for object in ["single", "batch"] {
            let mut metadata = HashMap::new();
            insert_str(&mut metadata, SUFFIX_COMPRESSION, "klauspost/compress/s2".to_string());
            insert_str(&mut metadata, SUFFIX_ACTUAL_SIZE, "1000".to_string());
            let reader = HashReader::from_stream(std::io::Cursor::new(vec![0x5a; 400]), 400, 1000, None, None, false)
                .expect("compressed fixture reader should be valid");
            let mut reader = PutObjReader::new(reader);
            store
                .put_object(
                    &bucket,
                    object,
                    &mut reader,
                    &ObjectOptions {
                        user_defined: metadata,
                        ..Default::default()
                    },
                )
                .await
                .expect("compressed fixture object should be written");
        }

        let mut single_req = build_request(
            DeleteObjectInput::builder()
                .bucket(bucket.clone())
                .key("single".to_string())
                .build()
                .expect("single delete input should build"),
            Method::DELETE,
        );
        single_req.extensions.insert(crate::storage::access::ReqInfo {
            cred: Some(rustfs_credentials::Credentials::default()),
            is_owner: true,
            ..Default::default()
        });
        DefaultObjectUsecase::from_global()
            .execute_delete_object(single_req)
            .await
            .expect("single compressed delete should succeed");
        assert_eq!(
            observed_bucket_usage(&bucket).await,
            Some(1_000),
            "single delete must subtract the logical accounting size"
        );
        assert_eq!(
            crate::app::storage_api::test::data_usage::get_bucket_usage_memory(&bucket).await,
            Some(2_000),
            "quota must retain the pre-delete floor until scanner reconciliation"
        );

        let mut batch_req = build_request(
            DeleteObjectsInput::builder()
                .bucket(bucket.clone())
                .delete(Delete {
                    objects: vec![ObjectIdentifier {
                        key: "batch".to_string(),
                        ..Default::default()
                    }],
                    quiet: None,
                })
                .build()
                .expect("batch delete input should build"),
            Method::POST,
        );
        batch_req.extensions.insert(crate::storage::access::ReqInfo {
            cred: Some(rustfs_credentials::Credentials::default()),
            is_owner: true,
            ..Default::default()
        });
        DefaultObjectUsecase::from_global()
            .execute_delete_objects(batch_req)
            .await
            .expect("batch compressed delete should succeed");
        assert_eq!(
            observed_bucket_usage(&bucket).await,
            Some(0),
            "batch delete must subtract the committed logical accounting size"
        );
        assert_eq!(
            crate::app::storage_api::test::data_usage::get_bucket_usage_memory(&bucket).await,
            Some(2_000),
            "quota must retain both pending deletes until scanner reconciliation"
        );

        store
            .delete_bucket(
                &bucket,
                &DeleteBucketOptions {
                    force: true,
                    ..Default::default()
                },
            )
            .await
            .expect("clean up compressed delete request bucket");
    }

    #[test]
    fn delete_replication_state_from_config_tracks_downstream_delete_marker_targets() {
        let arn = "arn:aws:s3:::target-bucket".to_string();
        let config = ReplicationConfiguration {
            role: arn.clone(),
            rules: vec![ReplicationRule {
                delete_marker_replication: Some(DeleteMarkerReplication {
                    status: Some(DeleteMarkerReplicationStatus::from_static(DeleteMarkerReplicationStatus::ENABLED)),
                }),
                delete_replication: None,
                destination: Destination {
                    bucket: arn.clone(),
                    ..Default::default()
                },
                existing_object_replication: Some(ExistingObjectReplication {
                    status: ExistingObjectReplicationStatus::from_static(ExistingObjectReplicationStatus::ENABLED),
                }),
                filter: None,
                id: Some("rule-1".to_string()),
                prefix: Some("test/".to_string()),
                priority: Some(1),
                source_selection_criteria: Some(SourceSelectionCriteria {
                    replica_modifications: Some(ReplicaModifications {
                        status: ReplicaModificationsStatus::from_static(ReplicaModificationsStatus::ENABLED),
                    }),
                    sse_kms_encrypted_objects: None,
                }),
                status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
            }],
        };
        let obj_info = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "test/object.txt".to_string(),
            delete_marker: true,
            replication_status: ReplicationStatusType::Replica,
            ..Default::default()
        };

        let state = delete_replication_state_from_config(&config, &obj_info, None, true)
            .expect("replica delete marker should be forwarded to downstream targets");
        let pending = format!("{arn}=PENDING;");

        assert_eq!(state.replication_status_internal.as_deref(), Some(pending.as_str()));
        assert_eq!(state.replicate_decision_str, format!("{arn}=true;false;{arn};"));
        assert!(state.targets.contains_key(&arn));
    }

    #[test]
    fn delete_replication_state_from_config_skips_replica_delete_without_replica_modifications() {
        let arn = "arn:aws:s3:::target-bucket".to_string();
        let config = ReplicationConfiguration {
            role: arn.clone(),
            rules: vec![ReplicationRule {
                delete_marker_replication: Some(DeleteMarkerReplication {
                    status: Some(DeleteMarkerReplicationStatus::from_static(DeleteMarkerReplicationStatus::ENABLED)),
                }),
                delete_replication: None,
                destination: Destination {
                    bucket: arn,
                    ..Default::default()
                },
                existing_object_replication: Some(ExistingObjectReplication {
                    status: ExistingObjectReplicationStatus::from_static(ExistingObjectReplicationStatus::ENABLED),
                }),
                filter: None,
                id: Some("rule-1".to_string()),
                prefix: Some("test/".to_string()),
                priority: Some(1),
                source_selection_criteria: None,
                status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
            }],
        };
        let obj_info = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "test/object.txt".to_string(),
            delete_marker: true,
            replication_status: ReplicationStatusType::Replica,
            ..Default::default()
        };

        assert!(
            delete_replication_state_from_config(&config, &obj_info, None, true).is_none(),
            "replica deletes must only fan out when ReplicaModifications are enabled"
        );
    }

    #[test]
    fn delete_replication_state_from_config_requires_delete_switch_for_marker_version_purges() {
        let arn = "arn:aws:s3:::target-bucket".to_string();
        let mut config = ReplicationConfiguration {
            role: arn.clone(),
            rules: vec![ReplicationRule {
                delete_marker_replication: Some(DeleteMarkerReplication {
                    status: Some(DeleteMarkerReplicationStatus::from_static(DeleteMarkerReplicationStatus::ENABLED)),
                }),
                delete_replication: None,
                destination: Destination {
                    bucket: arn.clone(),
                    ..Default::default()
                },
                existing_object_replication: Some(ExistingObjectReplication {
                    status: ExistingObjectReplicationStatus::from_static(ExistingObjectReplicationStatus::ENABLED),
                }),
                filter: None,
                id: Some("rule-1".to_string()),
                prefix: Some("test/".to_string()),
                priority: Some(1),
                source_selection_criteria: None,
                status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
            }],
        };
        let obj_info = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "test/object.txt".to_string(),
            delete_marker: true,
            replication_status: ReplicationStatusType::Completed,
            ..Default::default()
        };

        let version_id = Some(Uuid::new_v4());
        assert!(
            delete_replication_state_from_config(&config, &obj_info, version_id, false).is_none(),
            "delete-marker version purge must not use DeleteMarkerReplication"
        );

        config.rules[0].delete_replication = Some(DeleteReplication {
            status: DeleteReplicationStatus::from_static(DeleteReplicationStatus::ENABLED),
        });
        let state = delete_replication_state_from_config(&config, &obj_info, version_id, false)
            .expect("delete-marker version purge should honor DeleteReplication");
        let pending = format!("{arn}=PENDING;");

        assert_eq!(state.version_purge_status_internal.as_deref(), Some(pending.as_str()));
        assert_eq!(state.replicate_decision_str, format!("{arn}=true;false;{arn};"));
        assert!(state.purge_targets.contains_key(&arn));
    }

    #[test]
    fn replica_delete_enrichment_must_not_reuse_upstream_targets() {
        let upstream_state = ReplicationState {
            replicate_decision_str: "arn:aws:s3:::upstream=true;false;arn:aws:s3:::upstream;".to_string(),
            replication_status_internal: Some("arn:aws:s3:::upstream=COMPLETED;".to_string()),
            targets: replication_statuses_map("arn:aws:s3:::upstream=COMPLETED;"),
            ..Default::default()
        };
        let mut delete_object = StorageDeletedObject::default();
        set_deleted_object_replication_state(&mut delete_object, &upstream_state);
        let obj_info = ObjectInfo {
            replication_status: ReplicationStatusType::Replica,
            ..Default::default()
        };

        let should_keep_existing = delete_object.replication_state.as_ref().is_some_and(|state| {
            obj_info.replication_status != ReplicationStatusType::Replica
                && !state.replicate_decision_str.is_empty()
                && (!state.targets.is_empty() || !state.purge_targets.is_empty())
        });

        assert!(
            !should_keep_existing,
            "replica fanout deletes must recompute targets from the local bucket config instead of reusing upstream replication state"
        );
    }

    #[test]
    fn delete_replication_version_id_uses_none_for_delete_marker_creation() {
        let source = ObjectInfo {
            delete_marker: true,
            version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };

        assert_eq!(
            delete_replication_version_id(&source, false),
            None,
            "delete-marker creation must stay on the delete-marker replication path"
        );
    }

    #[test]
    fn delete_replication_version_id_keeps_version_for_marker_purge() {
        let version_id = Uuid::new_v4();
        let source = ObjectInfo {
            delete_marker: true,
            version_id: Some(version_id),
            ..Default::default()
        };

        assert_eq!(
            delete_replication_version_id(&source, true),
            Some(version_id),
            "delete-marker version purge must preserve the concrete version id for downstream purge replication"
        );
    }

    #[test]
    fn should_use_existing_delete_replication_info_ignores_replication_delete_marker_creation() {
        let opts = ObjectOptions {
            version_id: Some(Uuid::new_v4().to_string()),
            delete_marker: true,
            ..Default::default()
        };

        assert!(
            !should_use_existing_delete_replication_info(&opts, true),
            "replicated delete-marker creation carries a source version id header but must not be treated as a version purge"
        );
    }

    #[test]
    fn should_use_existing_delete_replication_info_keeps_version_delete_requests() {
        let opts = ObjectOptions {
            version_id: Some(Uuid::new_v4().to_string()),
            ..Default::default()
        };

        assert!(
            should_use_existing_delete_replication_info(&opts, true),
            "true version-delete requests should keep using the pre-delete object info"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn durable_quota_reclaims_overwrites_and_deleted_bytes() {
        let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("quota-delta-reconcile", 4096).await;

        for byte in [0x41, 0x42] {
            let mut reader = PutObjReader::from_vec(vec![byte; 4096]);
            store
                .put_object(&bucket, "object", &mut reader, &ObjectOptions::default())
                .await
                .expect("same-size overwrite must consume no additional quota");
        }

        store
            .delete_object(&bucket, "object", ObjectOptions::default())
            .await
            .expect("delete quota-tracked object");
        let mut replacement = PutObjReader::from_vec(vec![0x43; 4096]);
        store
            .put_object(&bucket, "replacement", &mut replacement, &ObjectOptions::default())
            .await
            .expect("deleted bytes must be reclaimed before rejecting a replacement");

        let mut excess = PutObjReader::from_vec(vec![0x44]);
        let err = store
            .put_object(&bucket, "excess", &mut excess, &ObjectOptions::default())
            .await
            .expect_err("one byte beyond the reclaimed exact quota must be denied");
        assert!(matches!(
            err,
            StorageError::QuotaExceeded {
                current: 4096,
                limit: 4096
            }
        ));
    }
}
