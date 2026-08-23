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
/// Data-usage snapshot persistence: CAS store pipeline, epoch baselines, and observed-snapshot cleanup.
use super::*;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum DataUsagePersistOutcome {
    #[default]
    NoUpdate,
    Current,
    AlreadyDurable,
    PriorCycleDurable,
    Saved,
    /// The metadata route is temporarily unavailable (for example while a
    /// terminal decommission state keeps the source pool suspended).  The
    /// caller must retry without acknowledging dirty usage.
    Deferred(ScannerCycleDeferReason),
    Failed,
}

#[derive(Clone, Debug)]
pub(super) struct DataUsagePersistBaseline {
    pub(super) data: Option<Bytes>,
    pub(super) revision: DataUsageCacheRevision,
}

#[derive(Debug)]
pub(super) enum DataUsagePersistTaskResult {
    Completed(DataUsagePersistOutcome),
    Cancelled,
    TimedOut,
    JoinFailed(tokio::task::JoinError),
}

pub(super) async fn wait_for_data_usage_persist_task(
    ctx: &CancellationToken,
    task: &mut AbortOnDropHandle<DataUsagePersistOutcome>,
    timeout: Duration,
) -> DataUsagePersistTaskResult {
    tokio::select! {
        biased;
        result = &mut *task => match result {
            Ok(outcome) => DataUsagePersistTaskResult::Completed(outcome),
            Err(err) => DataUsagePersistTaskResult::JoinFailed(err),
        },
        _ = ctx.cancelled() => {
            task.abort();
            let _ = (&mut *task).await;
            DataUsagePersistTaskResult::Cancelled
        },
        _ = tokio::time::sleep(timeout) => {
            task.abort();
            let _ = (&mut *task).await;
            DataUsagePersistTaskResult::TimedOut
        }
    }
}

#[instrument(skip(ctx, storeapi))]
pub async fn store_data_usage_in_backend(
    ctx: CancellationToken,
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    receiver: mpsc::Receiver<DataUsageInfo>,
) {
    let _ = store_data_usage_in_backend_with_outcome(ctx, storeapi, receiver).await;
}

pub(super) async fn store_data_usage_in_backend_with_outcome(
    ctx: CancellationToken,
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    receiver: mpsc::Receiver<DataUsageInfo>,
) -> DataUsagePersistOutcome {
    store_data_usage_in_backend_with_outcome_for_epoch(ctx, storeapi, receiver, None).await
}

pub(super) async fn store_data_usage_in_backend_with_outcome_for_epoch(
    ctx: CancellationToken,
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    receiver: mpsc::Receiver<DataUsageInfo>,
    leader_epoch: Option<u64>,
) -> DataUsagePersistOutcome {
    store_data_usage_in_backend_with_outcome_for_epoch_and_baseline(ctx, storeapi, receiver, leader_epoch, None).await
}

pub(super) async fn store_data_usage_in_backend_with_outcome_for_epoch_and_baseline(
    ctx: CancellationToken,
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    receiver: mpsc::Receiver<DataUsageInfo>,
    leader_epoch: Option<u64>,
    initial_baseline: Option<DataUsagePersistBaseline>,
) -> DataUsagePersistOutcome {
    store_data_usage_in_backend_with_outcome_for_epoch_and_baseline_and_route_probe(
        ctx,
        storeapi,
        receiver,
        leader_epoch,
        initial_baseline,
        || async { false },
    )
    .await
}

pub(super) async fn store_data_usage_in_backend_with_outcome_for_epoch_and_baseline_and_route_probe<F, Fut>(
    ctx: CancellationToken,
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    receiver: mpsc::Receiver<DataUsageInfo>,
    leader_epoch: Option<u64>,
    initial_baseline: Option<DataUsagePersistBaseline>,
    route_probe: F,
) -> DataUsagePersistOutcome
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = bool> + Send,
{
    store_data_usage_in_backend_with_outcome_for_epoch_and_baseline_and_route_probe_for_publication_epoch(
        ctx,
        storeapi,
        receiver,
        leader_epoch,
        initial_baseline,
        None,
        route_probe,
    )
    .await
}

pub(super) async fn store_data_usage_in_backend_with_outcome_for_epoch_and_baseline_and_route_probe_for_publication_epoch<
    F,
    Fut,
>(
    ctx: CancellationToken,
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    mut receiver: mpsc::Receiver<DataUsageInfo>,
    leader_epoch: Option<u64>,
    initial_baseline: Option<DataUsagePersistBaseline>,
    expected_publication_epoch: Option<u64>,
    route_probe: F,
) -> DataUsagePersistOutcome
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = bool> + Send,
{
    let mut outcome = DataUsagePersistOutcome::NoUpdate;
    let mut next_baseline = initial_baseline;

    'updates: while let Some(mut data_usage_info) = receiver.recv().await {
        let _activity_guard = ScannerActivityGuard::new();
        if ctx.is_cancelled() {
            break;
        }
        if let Some(leader_epoch) = leader_epoch {
            data_usage_info.scanner_epoch = Some(leader_epoch);
        }
        if let Some(expected_epoch) = expected_publication_epoch
            && scanner_publication_admission_for_epoch(storeapi.clone(), expected_epoch)
                .await
                .is_none()
        {
            outcome = DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement);
            break 'updates;
        }
        let observational = data_usage_info.usage_snapshot_converged == Some(false);
        let target_path = if observational {
            DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str()
        } else {
            DATA_USAGE_OBJ_NAME_PATH.as_str()
        };
        if route_probe().await {
            debug!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                path = %target_path,
                state = "publication_blocked_before_reconcile",
                "Scanner data usage publication deferred by the pool-state fence"
            );
            outcome = DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement);
            break;
        }

        let mut publication_epoch = expected_publication_epoch;
        if observational && data_usage_info.usage_snapshot_authoritative_baseline.is_none() {
            let read_epoch = match expected_publication_epoch {
                Some(expected_epoch) => {
                    if scanner_publication_admission_for_epoch(storeapi.clone(), expected_epoch)
                        .await
                        .is_none()
                    {
                        outcome = DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement);
                        break 'updates;
                    }
                    expected_epoch
                }
                None => {
                    let Some(read_epoch) = scanner_publication_epoch(storeapi.clone()).await else {
                        outcome = DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement);
                        break 'updates;
                    };
                    read_epoch
                }
            };
            publication_epoch = Some(read_epoch);
            let authoritative_data = match next_baseline.as_ref() {
                Some(baseline) => baseline.data.clone(),
                None => match read_config_with_revision(storeapi.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str()).await {
                    Ok((data, _)) => data.map(Bytes::from),
                    Err(err) => {
                        error!(
                            target: "rustfs::scanner",
                            event = EVENT_SCANNER_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_RUNTIME,
                            path = %DATA_USAGE_OBJ_NAME_PATH.as_str(),
                            state = "observed_baseline_load_failed",
                            error = %err,
                            "Scanner could not identify the authoritative baseline for an observation"
                        );
                        outcome = DataUsagePersistOutcome::Failed;
                        continue;
                    }
                },
            };
            let Some(authoritative_data) = authoritative_data else {
                warn!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    path = %DATA_USAGE_OBJ_NAME_PATH.as_str(),
                    state = "observed_baseline_missing",
                    "Scanner deferred observational publication until an authoritative usage baseline exists"
                );
                outcome = DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement);
                break 'updates;
            };
            let authoritative = match serde_json::from_slice::<DataUsageInfo>(&authoritative_data) {
                Ok(info) if data_usage_info_has_persisted_baseline_identity(&info) => info,
                Ok(_) => {
                    error!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        path = %DATA_USAGE_OBJ_NAME_PATH.as_str(),
                        state = "observed_baseline_identity_missing",
                        "Scanner refused to publish an observation without authoritative baseline identity"
                    );
                    outcome = DataUsagePersistOutcome::Failed;
                    continue;
                }
                Err(err) => {
                    error!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        path = %DATA_USAGE_OBJ_NAME_PATH.as_str(),
                        state = "observed_baseline_decode_failed",
                        error = %err,
                        "Scanner refused to publish an observation from an invalid authoritative baseline"
                    );
                    outcome = DataUsagePersistOutcome::Failed;
                    continue;
                }
            };
            data_usage_info.usage_snapshot_authoritative_baseline = Some(authoritative.snapshot_identity());
        }

        if !data_usage_info.is_complete_bucket_usage_snapshot() && !data_usage_info.usage_snapshot_partial {
            error!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                path = %target_path,
                state = "reject_incomplete_snapshot",
                "Scanner refused to persist an incomplete data usage snapshot"
            );
            global_metrics().record_scanner_usage_save_result(ScannerUsageSaveResult::Failed);
            outcome = DataUsagePersistOutcome::Failed;
            continue;
        }

        let data = match serde_json::to_vec(&data_usage_info) {
            Ok(data) => data,
            Err(e) => {
                error!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    path = %target_path,
                    state = "encode_failed",
                    error = %e,
                    "Scanner data usage encode failed"
                );
                global_metrics().record_scanner_usage_save_result(ScannerUsageSaveResult::EncodeFailed);
                outcome = DataUsagePersistOutcome::Failed;
                continue;
            }
        };
        let sha256hex = (!data.is_empty()).then(|| hex_simd::encode_to_string(Sha256::digest(&data), hex_simd::AsciiCase::Lower));
        let data = Bytes::from(data);
        let backup_due = !observational && data_usage_backup_due(&data_usage_info);
        let mut cas_retry = 0usize;
        let save_outcome = loop {
            if ctx.is_cancelled() {
                break 'updates;
            }

            let publication_epoch_for_save = match expected_publication_epoch {
                Some(expected_epoch) => {
                    if scanner_publication_admission_for_epoch(storeapi.clone(), expected_epoch)
                        .await
                        .is_none()
                    {
                        break DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement);
                    }
                    expected_epoch
                }
                None => match publication_epoch.take() {
                    Some(epoch) => epoch,
                    None => {
                        let Some(epoch) = scanner_publication_epoch(storeapi.clone()).await else {
                            break DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement);
                        };
                        epoch
                    }
                },
            };
            let baseline = if !observational && cas_retry == 0 {
                next_baseline.take()
            } else {
                None
            };
            let (existing_data, revision) = match baseline {
                Some(baseline) => (baseline.data, baseline.revision),
                None => match read_config_with_revision(storeapi.clone(), target_path).await {
                    Ok((data, revision)) => (data.map(Bytes::from), revision),
                    Err(e) => {
                        error!(
                            target: "rustfs::scanner",
                            event = EVENT_SCANNER_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_RUNTIME,
                            path = %target_path,
                            state = "revision_load_failed",
                            error = %e,
                            "Scanner data usage revision load failed"
                        );
                        break DataUsagePersistOutcome::Failed;
                    }
                },
            };
            let existing = existing_data
                .as_deref()
                .and_then(|buf| serde_json::from_slice::<DataUsageInfo>(buf).ok());
            if cas_retry > 0 && data_usage_reintroduces_missing_bucket(&data_usage_info, existing.as_ref()) {
                debug!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    path = %target_path,
                    incoming_scanner_epoch = ?data_usage_info.scanner_epoch,
                    incoming_scanner_cycle = ?data_usage_info.scanner_cycle,
                    state = "skip_deleted_bucket_reintroduction",
                    "Scanner usage update skipped after a concurrent bucket removal"
                );
                break DataUsagePersistOutcome::Current;
            }
            if let Some(existing) = existing.as_ref() {
                if existing == &data_usage_info {
                    break DataUsagePersistOutcome::AlreadyDurable;
                }
                if existing.scanner_epoch.is_some()
                    && existing.scanner_epoch == data_usage_info.scanner_epoch
                    && existing.scanner_cycle.is_some()
                    && existing.scanner_cycle == data_usage_info.scanner_cycle
                {
                    break DataUsagePersistOutcome::PriorCycleDurable;
                }
                if let Some(reason) = stale_data_usage_update_reason(&data_usage_info, existing, std::time::SystemTime::now()) {
                    debug!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        path = %target_path,
                        incoming_scanner_epoch = ?data_usage_info.scanner_epoch,
                        existing_scanner_epoch = ?existing.scanner_epoch,
                        incoming_scanner_cycle = ?data_usage_info.scanner_cycle,
                        existing_scanner_cycle = ?existing.scanner_cycle,
                        incoming_last_update = ?data_usage_info.last_update,
                        existing_last_update = ?existing.last_update,
                        reason = reason,
                        state = "skip_stale_update",
                        "Scanner stale data usage update skipped"
                    );
                    break DataUsagePersistOutcome::Current;
                }
            }
            if ctx.is_cancelled() {
                break 'updates;
            }
            if route_probe().await {
                debug!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    path = %target_path,
                    state = "publication_blocked_before_save",
                    "Scanner data usage publication deferred by the final pool-state fence"
                );
                break DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement);
            }

            let done_save = Metrics::time(Metric::SaveUsage);
            let save_result = {
                let Some(_publication_admission) =
                    scanner_publication_admission_for_epoch(storeapi.clone(), publication_epoch_for_save).await
                else {
                    done_save();
                    break DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement);
                };
                save_config_shared_with_preconditions(
                    storeapi.clone(),
                    target_path,
                    data.clone(),
                    sha256hex.clone(),
                    revision.preconditions(),
                )
                .await
            };
            done_save();

            match save_result {
                Ok(object_info) => {
                    if !observational {
                        next_baseline = object_info
                            .etag
                            .filter(|etag| !etag.is_empty())
                            .map(|etag| DataUsagePersistBaseline {
                                data: Some(data.clone()),
                                revision: DataUsageCacheRevision::Etag(etag),
                            });
                    }
                    break DataUsagePersistOutcome::Saved;
                }
                Err(EcstoreError::PreconditionFailed) if cas_retry < SCANNER_PERSIST_CAS_RETRIES => {
                    cas_retry += 1;
                    debug!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        path = %target_path,
                        state = "conflict_retry",
                        retry = cas_retry,
                        "Scanner data usage CAS conflict will be reconciled"
                    );
                }
                Err(e @ EcstoreError::ObjectNotFound(_, _)) => {
                    let route_blocked = route_probe().await;
                    if route_blocked {
                        warn!(
                            target: "rustfs::scanner",
                            event = EVENT_SCANNER_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_RUNTIME,
                            path = %target_path,
                            state = "publication_deferred",
                            error = %e,
                            "Scanner data usage route is blocked by data movement; retrying later"
                        );
                        break DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement);
                    }
                    error!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        path = %target_path,
                        state = "save_failed",
                        error = %e,
                        "Scanner data usage save failed"
                    );
                    break DataUsagePersistOutcome::Failed;
                }
                Err(e) => {
                    error!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        path = %target_path,
                        state = if matches!(e, EcstoreError::PreconditionFailed) {
                            "conflict_retries_exhausted"
                        } else {
                            "save_failed"
                        },
                        error = %e,
                        "Scanner data usage save failed"
                    );
                    break DataUsagePersistOutcome::Failed;
                }
            }
        };

        match save_outcome {
            DataUsagePersistOutcome::Current => {
                if observational {
                    invalidate_admin_data_usage_snapshot_cache().await;
                } else {
                    invalidate_data_usage_snapshot_cache().await;
                }
                global_metrics().record_scanner_usage_save_result(ScannerUsageSaveResult::SkippedStale);
                outcome = DataUsagePersistOutcome::Current;
                continue;
            }
            DataUsagePersistOutcome::AlreadyDurable => {
                if observational {
                    invalidate_admin_data_usage_snapshot_cache().await;
                } else {
                    let cleanup_ok = cleanup_observed_data_usage_snapshot_for_epoch(
                        storeapi.clone(),
                        &data_usage_info,
                        expected_publication_epoch,
                    )
                    .await;
                    if expected_publication_epoch.is_some() && !cleanup_ok {
                        outcome = DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement);
                        break 'updates;
                    }
                    invalidate_data_usage_snapshot_cache().await;
                    replace_bucket_usage_memory_from_info(&data_usage_info).await;
                }
                global_metrics().record_scanner_usage_save_result(ScannerUsageSaveResult::Success);
                outcome = DataUsagePersistOutcome::AlreadyDurable;
            }
            DataUsagePersistOutcome::PriorCycleDurable => {
                if observational {
                    invalidate_admin_data_usage_snapshot_cache().await;
                } else {
                    let cleanup_ok = cleanup_observed_data_usage_snapshot_for_epoch(
                        storeapi.clone(),
                        &data_usage_info,
                        expected_publication_epoch,
                    )
                    .await;
                    if expected_publication_epoch.is_some() && !cleanup_ok {
                        outcome = DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement);
                        break 'updates;
                    }
                    invalidate_data_usage_snapshot_cache().await;
                }
                global_metrics().record_scanner_usage_save_result(ScannerUsageSaveResult::Success);
                outcome = DataUsagePersistOutcome::PriorCycleDurable;
            }
            DataUsagePersistOutcome::Failed | DataUsagePersistOutcome::NoUpdate => {
                global_metrics().record_scanner_usage_save_result(ScannerUsageSaveResult::Failed);
                outcome = DataUsagePersistOutcome::Failed;
                continue;
            }
            DataUsagePersistOutcome::Deferred(reason) => {
                // A deferred publication is an intentional retryable state, not a
                // failed save. Keep the last real save result so admin freshness
                // reporting does not turn a pool-recovery fence into a false error.
                outcome = DataUsagePersistOutcome::Deferred(reason);
                break 'updates;
            }
            DataUsagePersistOutcome::Saved => {
                if observational {
                    invalidate_admin_data_usage_snapshot_cache().await;
                } else {
                    let cleanup_ok = cleanup_observed_data_usage_snapshot_for_epoch(
                        storeapi.clone(),
                        &data_usage_info,
                        expected_publication_epoch,
                    )
                    .await;
                    if expected_publication_epoch.is_some() && !cleanup_ok {
                        outcome = DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement);
                        break 'updates;
                    }
                    invalidate_data_usage_snapshot_cache().await;
                    replace_bucket_usage_memory_from_info(&data_usage_info).await;
                }
                global_metrics().record_scanner_usage_save_result(ScannerUsageSaveResult::Success);
                outcome = DataUsagePersistOutcome::Saved;
            }
        }

        if backup_due {
            let done_save = Metrics::time(Metric::SaveUsage);
            let backup_result =
                sync_data_usage_backup_from_primary_for_epoch(&ctx, storeapi.clone(), expected_publication_epoch).await;
            done_save();
            if let Err(e) = backup_result {
                warn!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    path = %format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str()),
                    state = "backup_save_failed",
                    error = %e,
                    "Scanner data usage backup save failed"
                );
                if scanner_publication_epoch_changed(&e) {
                    outcome = DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement);
                    break 'updates;
                }
                outcome = DataUsagePersistOutcome::Failed;
                break 'updates;
            }
        }
    }

    outcome
}

pub(super) async fn cleanup_observed_data_usage_snapshot_for_epoch(
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    authoritative: &DataUsageInfo,
    expected_publication_epoch: Option<u64>,
) -> bool {
    let read_epoch = match expected_publication_epoch {
        Some(expected_epoch) => {
            if scanner_publication_admission_for_epoch(storeapi.clone(), expected_epoch)
                .await
                .is_none()
            {
                return false;
            }
            expected_epoch
        }
        None => match scanner_publication_epoch(storeapi.clone()).await {
            Some(read_epoch) => read_epoch,
            None => return false,
        },
    };
    if expected_publication_epoch.is_some()
        && scanner_publication_admission_for_epoch(storeapi.clone(), read_epoch)
            .await
            .is_none()
    {
        return false;
    }
    let (observed_data, revision) =
        match read_config_with_revision(storeapi.clone(), DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str()).await {
            Ok((Some(data), revision)) => (data, revision),
            Ok((None, _)) => return true,
            Err(err) => {
                error!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    path = %DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str(),
                    state = "observed_cleanup_read_failed",
                    error = %err,
                    "Scanner could not inspect observational data usage snapshot before authoritative cleanup"
                );
                return true;
            }
        };
    let observed = match serde_json::from_slice::<DataUsageInfo>(&observed_data) {
        Ok(observed) => observed,
        Err(err) => {
            error!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                path = %DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str(),
                state = "observed_cleanup_decode_failed",
                error = %err,
                "Scanner refused to remove an invalid observational data usage snapshot after authoritative save"
            );
            return true;
        }
    };
    if observed_data_usage_is_newer(&observed, authoritative) {
        return true;
    }

    let result = delete_config_with_publication_admission_for_epoch(
        storeapi,
        RUSTFS_META_BUCKET,
        DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str(),
        ScannerObjectOptions {
            delete_prefix: true,
            delete_prefix_object: true,
            http_preconditions: Some(revision.preconditions()),
            ..Default::default()
        },
        read_epoch,
    )
    .await;

    match result {
        Ok(_)
        | Err(
            EcstoreError::FileNotFound
            | EcstoreError::ConfigNotFound
            | EcstoreError::ObjectNotFound(_, _)
            | EcstoreError::PreconditionFailed,
        ) => {}
        Err(err) => {
            error!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                path = %DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str(),
                state = "observed_cleanup_failed",
                error = %err,
                "Scanner could not remove stale observational data usage snapshot after authoritative save"
            );
            if scanner_publication_epoch_changed(&err) {
                return false;
            }
        }
    }
    true
}
