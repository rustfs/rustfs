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
/// Leader-lock claiming, usage-epoch fencing, and lock-loss handling.
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ScannerLeadershipClaimReconcile {
    Durable,
    Changed,
    Unchanged,
}

pub(super) async fn reconcile_scanner_leadership_claim(
    storeapi: Arc<impl ScannerObjectIO>,
    attempted: &[u8],
    previous_revision: &DataUsageCacheRevision,
    claimed_epoch: u64,
    cycle_info: &mut CurrentCycle,
    revision: &mut DataUsageCacheRevision,
    persisted_epoch: &mut u64,
) -> Result<ScannerLeadershipClaimReconcile, ScannerError> {
    let (persisted, persisted_revision) = read_config_with_revision(storeapi, DATA_USAGE_BLOOM_NAME_PATH.as_str())
        .await
        .map_err(|err| ScannerError::Other(format!("failed to reconcile scanner leadership claim: {err}")))?;
    let revision_changed = &persisted_revision != previous_revision;
    *revision = persisted_revision;

    let Some(persisted) = persisted else {
        *cycle_info = CurrentCycle::default();
        return Ok(if revision_changed {
            ScannerLeadershipClaimReconcile::Changed
        } else {
            ScannerLeadershipClaimReconcile::Unchanged
        });
    };
    if persisted == attempted {
        *persisted_epoch = claimed_epoch;
        return Ok(ScannerLeadershipClaimReconcile::Durable);
    }

    let (current, epoch) = decode_scanner_cycle_state(&persisted)
        .map_err(|err| ScannerError::Other(format!("scanner leadership conflict winner is invalid: {err}")))?;
    *cycle_info = current;
    *persisted_epoch = (*persisted_epoch).max(epoch);
    Ok(if revision_changed {
        ScannerLeadershipClaimReconcile::Changed
    } else {
        ScannerLeadershipClaimReconcile::Unchanged
    })
}

pub(super) fn decode_usage_snapshot_for_epoch_fence(data: &[u8], path: &str) -> Result<DataUsageInfo, ScannerError> {
    let usage: DataUsageInfo = serde_json::from_slice(data)
        .map_err(|err| ScannerError::Other(format!("failed to decode scanner usage epoch fence from {path}: {err}")))?;
    if !data_usage_info_has_persisted_baseline_identity(&usage) {
        return Err(ScannerError::Other(format!(
            "scanner usage epoch fence from {path} has no persisted baseline identity"
        )));
    }
    Ok(usage)
}

pub(super) async fn usage_snapshot_for_epoch_fence(
    storeapi: Arc<impl ScannerObjectIO>,
    primary: Option<&[u8]>,
) -> Result<Option<DataUsageInfo>, ScannerError> {
    if let Some(primary) = primary {
        return decode_usage_snapshot_for_epoch_fence(primary, DATA_USAGE_OBJ_NAME_PATH.as_str()).map(Some);
    }

    let backup_path = format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str());
    let (backup, _) = read_config_with_revision(storeapi.clone(), &backup_path)
        .await
        .map_err(|err| ScannerError::Other(format!("failed to read scanner usage epoch fence backup: {err}")))?;
    if let Some(backup) = backup.as_deref() {
        return decode_usage_snapshot_for_epoch_fence(backup, &backup_path).map(Some);
    }

    for path in [
        LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str().to_string(),
        format!("{}.bkp", LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str()),
    ] {
        let (legacy, _) = read_config_with_revision(storeapi.clone(), &path)
            .await
            .map_err(|err| ScannerError::Other(format!("failed to read legacy scanner usage epoch fence: {err}")))?;
        if let Some(legacy) = legacy.as_deref() {
            return decode_usage_snapshot_for_epoch_fence(legacy, &path).map(Some);
        }
    }
    // A missing usage snapshot is an uninitialized state, not an empty
    // snapshot. Leadership fencing may proceed without creating a plausible
    // default; the first authoritative scanner publication will create it.
    Ok(None)
}

pub(super) async fn fence_scanner_usage_epoch_with_expected_epoch(
    ctx: &CancellationToken,
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    claimed_epoch: u64,
    expected_publication_epoch: Option<u64>,
) -> Result<(), ScannerError> {
    for retry in 0..=SCANNER_PERSIST_CAS_RETRIES {
        if ctx.is_cancelled() {
            return Err(ScannerError::Other("scanner leadership was cancelled before usage fencing".to_string()));
        }

        let Some(read_epoch) = scanner_publication_epoch(storeapi.clone()).await else {
            return Err(ScannerError::Other(
                "scanner usage epoch fence publication is blocked by data movement".to_string(),
            ));
        };
        if expected_publication_epoch.is_some_and(|expected| expected != read_epoch) {
            if retry < SCANNER_PERSIST_CAS_RETRIES {
                continue;
            }
            return Err(ScannerError::Other(
                "scanner usage epoch fence changed while recovery reset was in progress".to_string(),
            ));
        }
        let (primary, revision) = read_config_with_revision(storeapi.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
            .await
            .map_err(|err| ScannerError::Other(format!("failed to read scanner usage epoch fence: {err}")))?;
        let Some(mut usage) = usage_snapshot_for_epoch_fence(storeapi.clone(), primary.as_deref()).await? else {
            let Some(_publication_admission) = scanner_publication_admission_for_epoch(storeapi.clone(), read_epoch).await else {
                if retry < SCANNER_PERSIST_CAS_RETRIES {
                    continue;
                }
                return Err(ScannerError::Other(
                    "scanner usage epoch fence changed while confirming a missing usage baseline".to_string(),
                ));
            };
            return Err(ScannerError::Other("authoritative scanner usage baseline is missing".to_string()));
        };
        match usage.scanner_epoch {
            Some(epoch) if epoch > claimed_epoch => {
                return Err(ScannerError::Other(format!(
                    "scanner usage epoch fence lost to newer leader: claimed={claimed_epoch}, persisted={epoch}"
                )));
            }
            Some(epoch) if epoch == claimed_epoch => return Ok(()),
            Some(_) | None => {}
        }
        usage.scanner_epoch = Some(claimed_epoch);
        let data = serde_json::to_vec(&usage)
            .map_err(|err| ScannerError::Other(format!("failed to encode scanner usage epoch fence: {err}")))?;

        let save_result = {
            let Some(_publication_admission) = scanner_publication_admission_for_epoch(storeapi.clone(), read_epoch).await else {
                if retry < SCANNER_PERSIST_CAS_RETRIES {
                    continue;
                }
                return Err(ScannerError::Other(
                    "scanner usage epoch fence changed while preparing its conditional write".to_string(),
                ));
            };
            save_config_with_preconditions(storeapi.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str(), data, revision.preconditions())
                .await
        };
        if save_result
            .as_ref()
            .ok()
            .and_then(|object_info| object_info.etag.as_deref())
            .is_some_and(|etag| !etag.is_empty())
        {
            return Ok(());
        }

        let (persisted, persisted_revision) = read_config_with_revision(storeapi.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
            .await
            .map_err(|err| ScannerError::Other(format!("failed to reconcile scanner usage epoch fence: {err}")))?;
        if let Some(persisted) = persisted {
            let persisted = decode_usage_snapshot_for_epoch_fence(&persisted, DATA_USAGE_OBJ_NAME_PATH.as_str())?;
            match persisted.scanner_epoch {
                Some(epoch) if epoch == claimed_epoch => return Ok(()),
                Some(epoch) if epoch > claimed_epoch => {
                    return Err(ScannerError::Other(format!(
                        "scanner usage epoch fence lost to newer leader: claimed={claimed_epoch}, persisted={epoch}"
                    )));
                }
                Some(_) | None => {}
            }
        }

        let precondition_failed = matches!(save_result, Err(EcstoreError::PreconditionFailed));
        if retry < SCANNER_PERSIST_CAS_RETRIES && (precondition_failed || persisted_revision != revision) {
            continue;
        }
        return Err(ScannerError::Other(match save_result {
            Ok(_) => "scanner usage epoch fence returned no ETag and could not be confirmed".to_string(),
            Err(err) => format!("scanner usage epoch fence save failed: {err}"),
        }));
    }

    Err(ScannerError::Other("scanner usage epoch fence retries exhausted".to_string()))
}

pub(super) async fn complete_scanner_leadership_claim(
    ctx: &CancellationToken,
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    claimed_epoch: u64,
    expected_publication_epoch: Option<u64>,
) -> bool {
    if let Err(err) =
        fence_scanner_usage_epoch_with_expected_epoch(ctx, storeapi, claimed_epoch, expected_publication_epoch).await
    {
        error!(
            target: "rustfs::scanner",
            event = EVENT_SCANNER_PERSIST_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            path = %DATA_USAGE_OBJ_NAME_PATH.as_str(),
            state = "usage_epoch_fence_failed",
            claimed_epoch,
            error = %err,
            "Scanner leadership usage epoch fencing failed"
        );
        return false;
    }
    !ctx.is_cancelled()
}

pub(super) async fn claim_scanner_leadership(
    ctx: &CancellationToken,
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    cycle_info: &mut CurrentCycle,
    revision: &mut DataUsageCacheRevision,
    persisted_epoch: &mut u64,
) -> bool {
    for retry in 0..=SCANNER_PERSIST_CAS_RETRIES {
        if ctx.is_cancelled() {
            return false;
        }
        let Some(claimed_epoch) = persisted_epoch.checked_add(1).filter(|epoch| *epoch < u64::MAX) else {
            error!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                state = "leader_epoch_exhausted",
                "Scanner leadership epoch is exhausted"
            );
            return false;
        };
        let data = match encode_scanner_cycle_state(cycle_info, claimed_epoch) {
            Ok(data) => data,
            Err(err) => {
                error!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                    state = "leader_claim_encode_failed",
                    error = %err,
                    "Scanner leadership claim encoding failed"
                );
                return false;
            }
        };
        let previous_revision = revision.clone();

        let Some(read_epoch) = scanner_publication_epoch(storeapi.clone()).await else {
            return false;
        };
        let (usage_primary, _) = match read_config_with_revision(storeapi.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str()).await {
            Ok(result) => result,
            Err(err) => {
                error!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    path = %DATA_USAGE_OBJ_NAME_PATH.as_str(),
                    state = "leader_usage_baseline_read_failed",
                    error = %err,
                    "Scanner leadership claim deferred because the usage baseline could not be read"
                );
                return false;
            }
        };
        match usage_snapshot_for_epoch_fence(storeapi.clone(), usage_primary.as_deref()).await {
            Ok(Some(_)) => {}
            Ok(None) => {
                warn!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    path = %DATA_USAGE_OBJ_NAME_PATH.as_str(),
                    state = "leader_usage_baseline_missing",
                    "Scanner leadership claim deferred until a usage baseline is published"
                );
                return false;
            }
            Err(err) => {
                error!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    path = %DATA_USAGE_OBJ_NAME_PATH.as_str(),
                    state = "leader_usage_baseline_invalid",
                    error = %err,
                    "Scanner leadership claim deferred because the usage baseline is invalid"
                );
                return false;
            }
        }
        let save_result = {
            let Some(_publication_admission) = scanner_publication_admission_for_epoch(storeapi.clone(), read_epoch).await else {
                if retry < SCANNER_PERSIST_CAS_RETRIES {
                    continue;
                }
                return false;
            };
            save_config_with_preconditions(storeapi.clone(), &DATA_USAGE_BLOOM_NAME_PATH, data.clone(), revision.preconditions())
                .await
        };
        match save_result {
            Ok(object_info) => {
                if let Some(etag) = object_info.etag.filter(|etag| !etag.is_empty()) {
                    *revision = DataUsageCacheRevision::Etag(etag);
                    *persisted_epoch = claimed_epoch;
                    return complete_scanner_leadership_claim(ctx, storeapi, claimed_epoch, Some(read_epoch)).await;
                }

                match reconcile_scanner_leadership_claim(
                    storeapi.clone(),
                    &data,
                    &previous_revision,
                    claimed_epoch,
                    cycle_info,
                    revision,
                    persisted_epoch,
                )
                .await
                {
                    Ok(ScannerLeadershipClaimReconcile::Durable) => {
                        return complete_scanner_leadership_claim(ctx, storeapi, claimed_epoch, Some(read_epoch)).await;
                    }
                    Ok(ScannerLeadershipClaimReconcile::Changed) if retry < SCANNER_PERSIST_CAS_RETRIES => continue,
                    Ok(ScannerLeadershipClaimReconcile::Changed | ScannerLeadershipClaimReconcile::Unchanged) => {
                        error!(
                            target: "rustfs::scanner",
                            event = EVENT_SCANNER_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_RUNTIME,
                            path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                            state = "leader_claim_missing_revision",
                            "Scanner leadership claim returned no ETag and could not be confirmed"
                        );
                        return false;
                    }
                    Err(err) => {
                        error!(
                            target: "rustfs::scanner",
                            event = EVENT_SCANNER_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_RUNTIME,
                            path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                            state = "leader_claim_reconcile_failed",
                            error = %err,
                            "Scanner leadership claim read-back failed"
                        );
                        return false;
                    }
                }
            }
            Err(err) => {
                let precondition_failed = matches!(err, EcstoreError::PreconditionFailed);
                match reconcile_scanner_leadership_claim(
                    storeapi.clone(),
                    &data,
                    &previous_revision,
                    claimed_epoch,
                    cycle_info,
                    revision,
                    persisted_epoch,
                )
                .await
                {
                    Ok(ScannerLeadershipClaimReconcile::Durable) => {
                        return complete_scanner_leadership_claim(ctx, storeapi, claimed_epoch, Some(read_epoch)).await;
                    }
                    Ok(ScannerLeadershipClaimReconcile::Changed)
                        if retry < SCANNER_PERSIST_CAS_RETRIES && !ctx.is_cancelled() =>
                    {
                        continue;
                    }
                    Ok(ScannerLeadershipClaimReconcile::Unchanged)
                        if precondition_failed && retry < SCANNER_PERSIST_CAS_RETRIES && !ctx.is_cancelled() =>
                    {
                        continue;
                    }
                    Ok(ScannerLeadershipClaimReconcile::Changed | ScannerLeadershipClaimReconcile::Unchanged) => {
                        error!(
                            target: "rustfs::scanner",
                            event = EVENT_SCANNER_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_RUNTIME,
                            path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                            state = if precondition_failed {
                                "leader_claim_conflicts_exhausted"
                            } else {
                                "leader_claim_failed"
                            },
                            error = %err,
                            "Scanner leadership claim failed"
                        );
                        return false;
                    }
                    Err(reconcile_err) => {
                        error!(
                            target: "rustfs::scanner",
                            event = EVENT_SCANNER_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_RUNTIME,
                            path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                            state = "leader_claim_reload_failed",
                            error = %reconcile_err,
                            save_error = %err,
                            "Scanner leadership claim reconciliation failed"
                        );
                        return false;
                    }
                }
            }
        }
    }

    false
}

pub(super) async fn record_scanner_leader_lock_lost(message: &'static str) {
    reset_scanner_cycle_schedule();
    record_scanner_leader_lock_state("lost");
    global_metrics()
        .record_scanner_leader_liveness("lost", false, "leader lock refresh quorum lost")
        .await;
    warn!(
        target: "rustfs::scanner",
        event = EVENT_SCANNER_LOCK_STATE,
        component = LOG_COMPONENT_SCANNER,
        subsystem = LOG_SUBSYSTEM_RUNTIME,
        lock_name = "leader.lock",
        state = "lost",
        reason = message,
        "Scanner leader lock lost"
    );
}
