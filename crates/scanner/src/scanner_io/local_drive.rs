// Copyright 2026 RustFS Team
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
use crate::storage_api::owner::EcstoreDiskAPI;
use std::sync::OnceLock;

/// A local scan interruption is distinct from a cycle budget or leader fence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScannerLocalStopReason {
    Retired,
    Unavailable,
    Replaced,
}

impl ScannerLocalStopReason {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Retired => "retired",
            Self::Unavailable => "unavailable",
            Self::Replaced => "replaced",
        }
    }
}

/// Process-local outcome of one scan attempt; never serialized into a checkpoint.
#[derive(Debug, Default)]
pub struct ScannerLocalStop {
    reason: OnceLock<ScannerLocalStopReason>,
}

impl ScannerLocalStop {
    /// The first local interruption, independent of ordinary cancellation.
    pub fn reason(&self) -> Option<ScannerLocalStopReason> {
        self.reason.get().copied()
    }

    pub(crate) fn stop(&self, reason: ScannerLocalStopReason, scan: &CancellationToken) {
        let _ = self.reason.set(reason);
        scan.cancel();
    }
}

pub(super) fn retain_interrupted_local_cache(cache: &mut DataUsageCache) {
    cache.info.snapshot_complete = false;
    // An EOF racing with retirement must be verified again, not published as
    // a completed companion or reused under an old coverage proof.
    cache.info.scan_plan_digest = None;
    cache.info.scan_bucket_observations.clear();
    cache.info.scan_bucket_value_origins.clear();
}

const LOCAL_SCANNER_LOOKUP_TIMEOUT: Duration = Duration::from_secs(5);

/// Bind the assigned local topology slot to its currently usable instance.
/// The format identity fences resume across a physical drive replacement.
pub(super) async fn current_local_scanner_disk(
    set: &SetDisks,
    assigned: &Arc<Disk>,
    ctx: &CancellationToken,
    budget: &ScannerCycleBudget,
) -> Option<Arc<Disk>> {
    // This also bounds the first binding, while the coordinator owns a scan
    // permit. Waiting for a format probe must respect source cancellation and its turn.
    let lookup = async {
        let location = EcstoreDiskAPI::get_disk_location(assigned.as_ref());
        let expected_id = location
            .disk_idx
            .and_then(|index| set.format.erasure.sets.get(set.set_index)?.get(index))
            .copied()
            .filter(|id| !id.is_nil());
        let expected_id = expected_id?;
        let candidate = scanner_set_disk_inventory(set).await.into_iter().find(|candidate| {
            EcstoreDiskAPI::is_local(candidate.as_ref())
                && EcstoreDiskAPI::endpoint(candidate.as_ref()) == EcstoreDiskAPI::endpoint(assigned.as_ref())
        });
        let candidate = candidate?;
        #[cfg(test)]
        super::tests::local_drive_recovery::before_binding_probe().await;
        if candidate.local_retirement_token().is_some_and(|token| token.is_cancelled()) || !candidate.is_online().await {
            return None;
        }
        match candidate.get_disk_id().await {
            Ok(Some(id)) if id == expected_id => Some(candidate),
            Ok(_) | Err(_) => None,
        }
    };
    tokio::select! {
        biased;
        _ = ctx.cancelled() => None,
        _ = budget.wait_for_stop() => None,
        _ = tokio::time::sleep(LOCAL_SCANNER_LOOKUP_TIMEOUT) => None,
        disk = lookup => disk,
    }
}

/// Wait without a cache lease or disk scan permit. Each bucket gets at most
/// one such recovery opportunity per source turn, including zero-progress stops.
pub(super) async fn wait_for_local_scanner_recovery(
    set: &SetDisks,
    assigned: &Arc<Disk>,
    ctx: &CancellationToken,
    budget: &ScannerCycleBudget,
) -> Option<Arc<Disk>> {
    const MAX_WAIT: Duration = Duration::from_secs(5);
    let deadline = tokio::time::Instant::now() + MAX_WAIT;
    let mut interval = tokio::time::interval(SCANNER_CACHE_LOCK_POLL_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            _ = ctx.cancelled() => return None,
            _ = budget.wait_for_stop() => return None,
            _ = tokio::time::sleep_until(deadline) => return None,
            _ = interval.tick() => {
                if budget.budget_elapsed() {
                    return None;
                }
                let current = tokio::select! {
                    biased;
                    _ = ctx.cancelled() => return None,
                    _ = budget.wait_for_stop() => return None,
                    _ = tokio::time::sleep_until(deadline) => return None,
                    disk = current_local_scanner_disk(set, assigned, ctx, budget) => disk,
                };
                if let Some(disk) = current {
                    return Some(disk);
                }
            }
        }
    }
}

/// Inspect runtime health and ownership without probing the drive or holding inventory guards during I/O.
pub(super) async fn local_scanner_stop_reason(set: &SetDisks, disk: &Arc<Disk>) -> Option<ScannerLocalStopReason> {
    if disk.local_retirement_token().is_some_and(|token| token.is_cancelled()) {
        return Some(ScannerLocalStopReason::Retired);
    }
    if !disk.runtime_state().is_snapshot_eligible() {
        return Some(ScannerLocalStopReason::Unavailable);
    }
    (!scanner_set_disk_inventory(set)
        .await
        .iter()
        .any(|current| Arc::ptr_eq(current, disk)))
    .then_some(ScannerLocalStopReason::Replaced)
}
