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
/// Scanner cycle-state codec, persisted usage floors, and cycle-state persistence.
use super::*;

#[derive(Debug, thiserror::Error)]
pub(super) enum ScannerCycleStateError {
    #[error("failed to encode scanner cycle state: {0}")]
    Encode(#[from] rmp_serde::encode::Error),
    #[error("failed to decode scanner cycle state: {0}")]
    Decode(#[from] rmp_serde::decode::Error),
    #[error("{0}")]
    InvalidData(&'static str),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct PersistedUsageFloor {
    pub(super) next_cycle: u64,
    pub(super) leader_epoch: u64,
}

pub(super) fn encode_scanner_cycle_state(
    cycle_info: &CurrentCycle,
    leader_epoch: u64,
) -> Result<Vec<u8>, ScannerCycleStateError> {
    if cycle_info.next == u64::MAX {
        return Err(ScannerCycleStateError::InvalidData("scanner cycle counter is exhausted"));
    }
    let cycle_info_buf = rmp_serde::to_vec(cycle_info)?;
    let mut buf = Vec::with_capacity(cycle_info_buf.len() + SCANNER_CYCLE_STATE_HEADER_LEN);
    buf.extend_from_slice(&cycle_info.next.to_le_bytes());
    buf.extend_from_slice(SCANNER_CYCLE_STATE_MAGIC);
    buf.extend_from_slice(&leader_epoch.to_le_bytes());
    buf.extend_from_slice(&cycle_info_buf);
    Ok(buf)
}

pub(super) fn decode_scanner_cycle_state(buf: &[u8]) -> Result<(CurrentCycle, u64), ScannerCycleStateError> {
    if buf.len() < 8 {
        return Err(ScannerCycleStateError::InvalidData("scanner cycle state is truncated"));
    }

    let persisted_next = u64::from_le_bytes(
        buf[0..8]
            .try_into()
            .map_err(|_| ScannerCycleStateError::InvalidData("scanner cycle counter is truncated"))?,
    );
    if persisted_next == u64::MAX {
        return Err(ScannerCycleStateError::InvalidData("scanner cycle counter is exhausted"));
    }
    if buf.len() == 8 {
        return Ok((
            CurrentCycle {
                next: persisted_next,
                ..Default::default()
            },
            0,
        ));
    }

    let (leader_epoch, payload) = if buf.len() >= 16 && &buf[8..16] == SCANNER_CYCLE_STATE_MAGIC {
        if buf.len() < SCANNER_CYCLE_STATE_HEADER_LEN {
            return Err(ScannerCycleStateError::InvalidData("scanner cycle fencing header is truncated"));
        }
        let epoch = u64::from_le_bytes(
            buf[16..24]
                .try_into()
                .map_err(|_| ScannerCycleStateError::InvalidData("scanner leader epoch is truncated"))?,
        );
        if epoch == 0 {
            return Err(ScannerCycleStateError::InvalidData("scanner leader epoch is zero"));
        }
        (epoch, &buf[SCANNER_CYCLE_STATE_HEADER_LEN..])
    } else {
        (0, &buf[8..])
    };

    let cycle_info = rmp_serde::from_slice::<CurrentCycle>(payload)?;
    if cycle_info.next != persisted_next {
        return Err(ScannerCycleStateError::InvalidData("scanner cycle counter disagrees with encoded state"));
    }
    Ok((cycle_info, leader_epoch))
}

pub(crate) fn decode_persisted_scanner_cycle_fence(buf: &[u8]) -> Result<(u64, u64), ScannerError> {
    decode_scanner_cycle_state(buf)
        .map(|(cycle, leader_epoch)| (cycle.next, leader_epoch))
        .map_err(|err| ScannerError::Other(format!("persisted scanner cycle state is invalid: {err}")))
}

#[cfg(test)]
pub(crate) fn encode_scanner_cycle_fence_for_test(next_cycle: u64, leader_epoch: u64) -> Vec<u8> {
    encode_scanner_cycle_state(
        &CurrentCycle {
            next: next_cycle,
            ..Default::default()
        },
        leader_epoch,
    )
    .expect("test scanner cycle fence should encode")
}

pub(crate) async fn current_scanner_leader_epoch() -> Result<u64, ScannerError> {
    let store = crate::resolve_scanner_object_store_handle()
        .ok_or_else(|| ScannerError::Other("scanner object layer is unavailable".to_string()))?;
    match read_config(store, &DATA_USAGE_BLOOM_NAME_PATH).await {
        Ok(buf) => {
            let (_, leader_epoch) = decode_persisted_scanner_cycle_fence(&buf)?;
            if leader_epoch == 0 {
                return Err(ScannerError::Other("persisted scanner cycle state has no leader epoch".to_string()));
            }
            Ok(leader_epoch)
        }
        Err(err) => Err(ScannerError::Other(format!("failed to read persisted scanner leader epoch: {err}"))),
    }
}

pub(super) fn decode_scanner_cycle_state_for_startup(buf: &[u8]) -> Result<(CurrentCycle, u64), ScannerCycleStateError> {
    if buf.is_empty() {
        Ok((CurrentCycle::default(), 0))
    } else {
        decode_scanner_cycle_state(buf)
    }
}

pub(super) fn advance_scanner_cycle(cycle_info: &mut CurrentCycle) -> Result<(), ScannerCycleStateError> {
    let next = cycle_info
        .next
        .checked_add(1)
        .filter(|next| *next < u64::MAX)
        .ok_or(ScannerCycleStateError::InvalidData("scanner cycle counter is exhausted"))?;
    cycle_info.next = next;
    Ok(())
}

pub(super) async fn persisted_usage_floor(storeapi: Arc<impl ScannerObjectIO>) -> Result<PersistedUsageFloor, ScannerError> {
    let mut floor = PersistedUsageFloor::default();
    let update_floor = |floor: &mut PersistedUsageFloor, usage: DataUsageInfo, path: &str| -> Result<(), ScannerError> {
        floor.leader_epoch = floor.leader_epoch.max(usage.scanner_epoch.unwrap_or_default());
        if let Some(completed_cycle) = usage.scanner_cycle {
            let next_cycle = completed_cycle
                .checked_add(1)
                .filter(|next| *next < u64::MAX)
                .ok_or_else(|| ScannerError::Other(format!("persisted scanner usage cycle is exhausted in {path}")))?;
            floor.next_cycle = floor.next_cycle.max(next_cycle);
        }
        Ok(())
    };
    for primary_path in [DATA_USAGE_OBJ_NAME_PATH.as_str(), LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str()] {
        let backup_path = format!("{primary_path}.bkp");
        let mut pair_found = false;
        for path in [primary_path, backup_path.as_str()] {
            let data = match read_config(storeapi.clone(), path).await {
                Ok(data) => {
                    pair_found = true;
                    data
                }
                Err(EcstoreError::ConfigNotFound) => continue,
                Err(err) => {
                    return Err(ScannerError::Other(format!(
                        "failed to read scanner usage epoch floor from {path}: {err}"
                    )));
                }
            };
            let usage = serde_json::from_slice::<DataUsageInfo>(&data)
                .map_err(|err| ScannerError::Other(format!("failed to decode scanner usage floor from {path}: {err}")))?;
            update_floor(&mut floor, usage, path)?;
        }
        if pair_found {
            break;
        }
    }
    Ok(floor)
}

pub(super) fn apply_persisted_usage_floor(cycle_info: &mut CurrentCycle, leader_epoch: &mut u64, floor: PersistedUsageFloor) {
    cycle_info.next = cycle_info.next.max(floor.next_cycle);
    *leader_epoch = (*leader_epoch).max(floor.leader_epoch);
}

pub(super) async fn persist_scanner_cycle_state(
    ctx: &CancellationToken,
    storeapi: Arc<impl ScannerObjectIO>,
    cycle_info: &mut CurrentCycle,
    revision: &mut DataUsageCacheRevision,
    leader_epoch: u64,
) -> bool {
    let buf = match encode_scanner_cycle_state(cycle_info, leader_epoch) {
        Ok(buf) => buf,
        Err(e) => {
            error!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                state = "encode_failed",
                error = %e,
                "Scanner state encoding failed"
            );
            return false;
        }
    };

    for retry in 0..=SCANNER_PERSIST_CAS_RETRIES {
        if ctx.is_cancelled() {
            debug!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                state = "cancelled_before_save",
                retry,
                "Scanner state persistence cancelled by the leader fence"
            );
            return false;
        }

        #[cfg(test)]
        notify_scanner_cycle_state_persist_test_hook(leader_epoch);
        match save_config_with_preconditions(storeapi.clone(), &DATA_USAGE_BLOOM_NAME_PATH, buf.clone(), revision.preconditions())
            .await
        {
            Ok(object_info) => {
                let Some(etag) = object_info.etag.filter(|etag| !etag.is_empty()) else {
                    error!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                        state = "missing_revision",
                        "Scanner state save returned no ETag"
                    );
                    return false;
                };
                *revision = DataUsageCacheRevision::Etag(etag);
                if ctx.is_cancelled() {
                    debug!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                        state = "cancelled_after_save",
                        retry,
                        "Scanner state save completed after the leader fence was cancelled"
                    );
                    return false;
                }
                debug!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                    state = "saved",
                    "Scanner state saved"
                );
                return true;
            }
            Err(EcstoreError::PreconditionFailed) => {
                let (persisted, persisted_revision) =
                    match read_config_with_revision(storeapi.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str()).await {
                        Ok(result) => result,
                        Err(e) => {
                            error!(
                                target: "rustfs::scanner",
                                event = EVENT_SCANNER_PERSIST_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_RUNTIME,
                                path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                                state = "conflict_reload_failed",
                                error = %e,
                                "Scanner state conflict reconciliation failed"
                            );
                            return false;
                        }
                    };
                *revision = persisted_revision;
                if ctx.is_cancelled() {
                    debug!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                        state = "cancelled_after_conflict",
                        retry,
                        "Scanner state conflict reconciliation cancelled by the leader fence"
                    );
                    return false;
                }

                if let Some(persisted) = persisted {
                    if persisted.len() < 8 {
                        error!(
                            target: "rustfs::scanner",
                            event = EVENT_SCANNER_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_RUNTIME,
                            path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                            state = "conflict_state_invalid",
                            length = persisted.len(),
                            "Scanner state conflict winner is truncated"
                        );
                        return false;
                    }

                    let (persisted_cycle, persisted_epoch) = match decode_scanner_cycle_state(&persisted) {
                        Ok(state) => state,
                        Err(e) => {
                            error!(
                                target: "rustfs::scanner",
                                event = EVENT_SCANNER_PERSIST_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_RUNTIME,
                                path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                                state = "conflict_state_decode_failed",
                                error = %e,
                                "Scanner state conflict winner could not be decoded"
                            );
                            return false;
                        }
                    };
                    if persisted_epoch != leader_epoch {
                        error!(
                            target: "rustfs::scanner",
                            event = EVENT_SCANNER_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_RUNTIME,
                            path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                            state = "leader_epoch_fenced",
                            expected_epoch = leader_epoch,
                            persisted_epoch,
                            "Scanner state save rejected by a newer leadership epoch"
                        );
                        return false;
                    }

                    if persisted_cycle.next >= cycle_info.next {
                        *cycle_info = persisted_cycle;
                        debug!(
                            target: "rustfs::scanner",
                            event = EVENT_SCANNER_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_RUNTIME,
                            path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                            state = "conflict_reconciled",
                            retry,
                            "Scanner state adopted the current persisted cycle"
                        );
                        return true;
                    }
                }

                if retry < SCANNER_PERSIST_CAS_RETRIES {
                    debug!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                        state = "conflict_retry",
                        retry = retry + 1,
                        "Scanner state CAS conflict will be retried"
                    );
                    continue;
                }

                error!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                    state = "conflict_retries_exhausted",
                    retries = SCANNER_PERSIST_CAS_RETRIES,
                    "Scanner state CAS conflict retries exhausted"
                );
                return false;
            }
            Err(e) => {
                error!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                    state = "failed",
                    error = %e,
                    "Scanner state persistence failed"
                );
                return false;
            }
        }
    }

    false
}

pub(super) async fn finalize_partial_scan_cycle(
    ctx: &CancellationToken,
    storeapi: Arc<impl ScannerObjectIO>,
    cycle_info: &mut CurrentCycle,
    revision: &mut DataUsageCacheRevision,
    leader_epoch: u64,
    cycle_metrics_guard: &mut ScannerCycleMetricsGuard,
) -> bool {
    // A budget-limited cycle is deliberate pacing, not a failure. The cycle counter
    // must still advance (and persist) because per-bucket next_cycle is stamped from
    // it and compacted folders are only rescanned when their hash matches
    // next_cycle % DATA_USAGE_UPDATE_DIR_CYCLES; a pinned counter starves lifecycle
    // expiry and usage refresh on every folder outside the stuck window.
    if let Err(err) = advance_scanner_cycle(cycle_info) {
        error!(
            target: "rustfs::scanner",
            event = EVENT_SCANNER_PERSIST_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            state = "cycle_counter_exhausted",
            error = %err,
            "Scanner partial cycle could not advance"
        );
        mark_scan_cycle_idle(cycle_info, cycle_metrics_guard).await;
        return false;
    }
    cycle_info.current = 0;
    global_metrics().clear_current_scan_mode();
    let persisted = persist_scanner_cycle_state(ctx, storeapi, cycle_info, revision, leader_epoch).await;
    cycle_metrics_guard.finish(cycle_info.clone()).await;
    persisted
}

pub(super) async fn persist_required_scanner_cycle_floor(
    ctx: &CancellationToken,
    storeapi: Arc<impl ScannerObjectIO>,
    cycle_info: &mut CurrentCycle,
    revision: &mut DataUsageCacheRevision,
    leader_epoch: u64,
    required_cycle: u64,
    cycle_metrics_guard: &mut ScannerCycleMetricsGuard,
) -> bool {
    if required_cycle <= cycle_info.current || required_cycle == u64::MAX {
        error!(
            target: "rustfs::scanner",
            event = EVENT_SCANNER_PERSIST_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            current_cycle = cycle_info.current,
            required_cycle,
            state = "invalid_cache_cycle_floor",
            "Scanner cache cycle floor is invalid"
        );
        mark_scan_cycle_idle(cycle_info, cycle_metrics_guard).await;
        return false;
    }

    cycle_info.next = cycle_info.next.max(required_cycle);
    cycle_info.current = 0;
    global_metrics().clear_current_scan_mode();
    let persisted = persist_scanner_cycle_state(ctx, storeapi, cycle_info, revision, leader_epoch).await;
    cycle_metrics_guard.finish(cycle_info.clone()).await;
    persisted
}

pub(super) async fn await_scanner_cycle_with_lock_fence<Cycle, LockLost>(
    cycle_ctx: &CancellationToken,
    cycle: Cycle,
    lock_lost: LockLost,
) -> Option<Cycle::Output>
where
    Cycle: Future,
    LockLost: Future<Output = ()>,
{
    tokio::pin!(cycle);
    tokio::pin!(lock_lost);
    tokio::select! {
        biased;
        _ = &mut lock_lost => {
            cycle_ctx.cancel();
            tokio::time::timeout(SCANNER_LOCK_LOSS_SHUTDOWN_TIMEOUT, &mut cycle).await.ok()
        }
        output = &mut cycle => Some(output),
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum ScannerCycleWaitOutcome<T> {
    Completed(T),
    LockLost,
    Cancelled,
    Deadline { worker_stopped: bool },
}

pub(super) async fn await_scanner_cycle_with_budget_fence<Cycle, LockLost>(
    cycle_ctx: &CancellationToken,
    budget: &ScannerCycleBudget,
    cycle: Cycle,
    lock_lost: LockLost,
) -> ScannerCycleWaitOutcome<Cycle::Output>
where
    Cycle: Future,
    LockLost: Future<Output = ()>,
{
    tokio::pin!(cycle);
    tokio::pin!(lock_lost);
    let deadline = async {
        if let Some(deadline) = budget.deadline() {
            tokio::time::sleep_until(deadline).await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    tokio::pin!(deadline);
    tokio::select! {
        biased;
        _ = &mut lock_lost => {
            cycle_ctx.cancel();
            let _ = tokio::time::timeout(SCANNER_LOCK_LOSS_SHUTDOWN_TIMEOUT, &mut cycle).await;
            ScannerCycleWaitOutcome::LockLost
        }
        _ = &mut deadline => {
            budget.cancel_for_runtime();
            // Let the budget cancellation reach the scanner first so it can
            // persist a partial cursor. Only an uncooperative worker gets the
            // parent cancellation, and it is dropped after the bounded window;
            // the caller fences its epoch next.
            let worker_stopped = if tokio::time::timeout(SCANNER_LOCK_LOSS_SHUTDOWN_TIMEOUT, &mut cycle)
                .await
                .is_ok()
            {
                true
            } else {
                cycle_ctx.cancel();
                false
            };
            ScannerCycleWaitOutcome::Deadline { worker_stopped }
        }
        _ = cycle_ctx.cancelled() => {
            let _ = tokio::time::timeout(SCANNER_LOCK_LOSS_SHUTDOWN_TIMEOUT, &mut cycle).await;
            ScannerCycleWaitOutcome::Cancelled
        }
        output = &mut cycle => ScannerCycleWaitOutcome::Completed(output),
    }
}
