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
/// Persisted best-effort retry hints. Existing TTL/count pruning applies only
/// to this hint cache, never to a committed durable repair obligation.
use super::*;

const PENDING_HEAL_RETRY_BASE_SECS: u64 = 15 * 60;
const PENDING_HEAL_RETRY_CAP_SECS: u64 = 6 * 60 * 60;

pub(super) struct PendingHealSyncBatch<'a> {
    pub(super) scanner: &'a mut FolderScanner,
}

impl<'a> PendingHealSyncBatch<'a> {
    pub(super) fn new(scanner: &'a mut FolderScanner) -> Self {
        scanner.pending_heal_sync_deferred = true;
        scanner.pending_heal_batch_dirty = false;
        Self { scanner }
    }
}

impl Drop for PendingHealSyncBatch<'_> {
    fn drop(&mut self) {
        self.scanner.pending_heal_sync_deferred = false;
        if self.scanner.pending_heal_batch_dirty {
            self.scanner.pending_heal_batch_dirty = false;
            self.scanner.sync_pending_heals();
        }
    }
}

pub(super) fn record_pending_heal_attempt(entry: &mut PendingScannerHeal, now: u64) {
    entry.last_attempt = now;
    entry.attempts = entry.attempts.saturating_add(1);
}

pub(super) fn observe_pending_heal_admission(entry: &mut PendingScannerHeal, result: HealAdmissionResult) {
    // Rediscovery and coalesced admissions must not postpone an armed retry.
    entry.last_admission_result = result.result_label().to_string();
    entry.last_admission_reason = result.reason_label().to_string();
}

impl FolderScanner {
    pub(super) fn sync_pending_heals(&mut self) {
        self.pending_heals_changed = true;
        if self.pending_heal_sync_deferred {
            self.pending_heal_batch_dirty = true;
            return;
        }
        self.update_cache.info.pending_heals = self.new_cache.info.pending_heals.clone();
        #[cfg(test)]
        {
            self.pending_heal_sync_count += 1;
        }
    }

    pub(super) fn clear_pending_scanner_heal(
        &mut self,
        kind: PendingScannerHealKind,
        bucket: &str,
        object: Option<&str>,
        version_id: Option<&str>,
    ) {
        let before = self.new_cache.info.pending_heals.len();
        self.new_cache
            .info
            .pending_heals
            .retain(|entry| !pending_scanner_heal_matches(entry, kind, bucket, object, version_id));
        if self.new_cache.info.pending_heals.len() != before {
            self.sync_pending_heals();
        }
    }

    pub(super) fn record_pending_scanner_heal(
        &mut self,
        kind: PendingScannerHealKind,
        bucket: &str,
        object: Option<&str>,
        version_id: Option<&str>,
        scan_mode: HealScanMode,
        result: HealAdmissionResult,
    ) {
        let now = Self::now_secs();
        if let Some(entry) = self
            .new_cache
            .info
            .pending_heals
            .iter_mut()
            .find(|entry| pending_scanner_heal_matches(entry, kind, bucket, object, version_id))
        {
            observe_pending_heal_admission(entry, result);
            self.sync_pending_heals();
            return;
        }

        self.new_cache.info.pending_heals.push(PendingScannerHeal {
            kind,
            bucket: bucket.to_string(),
            object: object.map(ToOwned::to_owned),
            version_id: version_id.map(ToOwned::to_owned),
            scan_mode,
            first_seen: now,
            last_attempt: now,
            attempts: 1,
            last_admission_result: result.result_label().to_string(),
            last_admission_reason: result.reason_label().to_string(),
        });
        if self.prune_pending_scanner_heal_capacity() == 0 {
            self.sync_pending_heals();
        }
    }

    /// Preserve the discovery reason when a candidate could not be admitted
    /// immediately. The existing string field is intentionally reused so the
    /// scanner's map-encoded cache schema stays backward compatible.
    pub(super) fn mark_pending_scanner_heal_reason(
        &mut self,
        kind: PendingScannerHealKind,
        bucket: &str,
        object: Option<&str>,
        version_id: Option<&str>,
        reason: &str,
    ) {
        if let Some(entry) = self
            .new_cache
            .info
            .pending_heals
            .iter_mut()
            .find(|entry| pending_scanner_heal_matches(entry, kind, bucket, object, version_id))
        {
            entry.last_admission_reason = reason.to_string();
            self.sync_pending_heals();
        }
    }

    pub(super) fn prune_pending_scanner_heals(&mut self) {
        let now = Self::now_secs();
        let before_expiry = self.new_cache.info.pending_heals.len();
        self.new_cache
            .info
            .pending_heals
            .retain(|entry| now.saturating_sub(entry.first_seen) <= MAX_PENDING_SCANNER_HEAL_AGE_SECS);
        let expired = before_expiry.saturating_sub(self.new_cache.info.pending_heals.len());
        if expired > 0 {
            counter!(
                METRIC_SCANNER_PENDING_HEAL_PRUNE_TOTAL,
                "bucket" => self.new_cache.info.name.clone()
            )
            .increment(u64::try_from(expired).unwrap_or(u64::MAX));
            warn!(
                target: "rustfs::scanner::folder",
                event = EVENT_SCANNER_HEAL_ADMISSION,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_HEAL,
                bucket = %self.new_cache.info.name,
                pruned = expired,
                remaining = self.new_cache.info.pending_heals.len(),
                state = "pending_heal_expired",
                "Scanner pending heal ledger expired old entries"
            );
            self.sync_pending_heals();
        }

        self.prune_pending_scanner_heal_capacity();
    }

    fn prune_pending_scanner_heal_capacity(&mut self) -> usize {
        let len = self.new_cache.info.pending_heals.len();
        if len <= MAX_PENDING_SCANNER_HEALS_PER_BUCKET {
            return 0;
        }

        sort_pending_scanner_heals_for_retry(&mut self.new_cache.info.pending_heals);
        let remove_count = len.saturating_sub(MAX_PENDING_SCANNER_HEALS_PER_BUCKET);
        self.new_cache.info.pending_heals.drain(..remove_count);
        self.sync_pending_heals();
        counter!(
            METRIC_SCANNER_PENDING_HEAL_PRUNE_TOTAL,
            "bucket" => self.new_cache.info.name.clone()
        )
        .increment(u64::try_from(remove_count).unwrap_or(u64::MAX));
        warn!(
            target: "rustfs::scanner::folder",
            event = EVENT_SCANNER_HEAL_ADMISSION,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_HEAL,
            bucket = %self.new_cache.info.name,
            pruned = remove_count,
            remaining = self.new_cache.info.pending_heals.len(),
            state = "pending_heal_pruned",
            "Scanner pending heal ledger pruned oldest entries"
        );
        remove_count
    }

    pub(super) fn update_pending_scanner_heal_after_admission(
        &mut self,
        kind: PendingScannerHealKind,
        bucket: &str,
        object: Option<&str>,
        version_id: Option<&str>,
        scan_mode: HealScanMode,
        result: HealAdmissionResult,
    ) {
        match result {
            HealAdmissionResult::Full | HealAdmissionResult::Dropped(HealAdmissionDropReason::QueueFull) => {
                self.record_pending_scanner_heal(kind, bucket, object, version_id, scan_mode, result);
            }
            HealAdmissionResult::Accepted
            | HealAdmissionResult::Merged
            | HealAdmissionResult::Dropped(HealAdmissionDropReason::PolicyDropped)
            | HealAdmissionResult::Dropped(HealAdmissionDropReason::AlreadyRunning)
            | HealAdmissionResult::Dropped(HealAdmissionDropReason::OverlappingPaths) => {
                // Admission is neither repair completion nor a durable
                // successor receipt. Preserve existing responsibility without
                // turning every newly admitted hint into a persisted intent.
                if let Some(entry) = self
                    .new_cache
                    .info
                    .pending_heals
                    .iter_mut()
                    .find(|entry| pending_scanner_heal_matches(entry, kind, bucket, object, version_id))
                {
                    observe_pending_heal_admission(entry, result);
                    self.sync_pending_heals();
                }
            }
        }
    }

    pub(super) async fn retry_pending_scanner_heals(&mut self) -> Result<(), ScannerError> {
        let batch = PendingHealSyncBatch::new(self);
        let scanner = &mut *batch.scanner;
        if !scanner.should_heal().await {
            return Ok(());
        }

        let bucket = scanner.new_cache.info.name.clone();
        // Legacy notices cannot bind a verified disposition to the current
        // incarnation, kind, set scope and responsibility generation.
        let repaired = rustfs_common::mrf_channel::take_mrf_repaired_events_for(&bucket);
        if !repaired.is_empty() {
            counter!("rustfs_scanner_unverified_repair_notices_total")
                .increment(u64::try_from(repaired.len()).unwrap_or(u64::MAX));
        }
        scanner.prune_pending_scanner_heals();
        for pending in pending_scanner_heal_retry_candidates(&scanner.new_cache.info.pending_heals, &bucket) {
            if !scanner.should_heal().await {
                break;
            }

            let Some(request) = build_pending_scanner_heal_request(&pending) else {
                scanner.clear_pending_scanner_heal(pending.kind, &pending.bucket, None, pending.version_id.as_deref());
                counter!(
                    METRIC_SCANNER_PENDING_HEAL_MALFORMED_TOTAL,
                    "bucket" => pending.bucket.clone(),
                    "type" => pending_scanner_heal_candidate_type(pending.kind).to_string()
                )
                .increment(1);
                warn!(
                    target: "rustfs::scanner::folder",
                    event = EVENT_SCANNER_HEAL_ADMISSION,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_HEAL,
                    bucket = %pending.bucket,
                    state = "pending_heal_malformed",
                    "Scanner dropped malformed pending heal entry"
                );
                continue;
            };

            if let Some(entry) = scanner.new_cache.info.pending_heals.iter_mut().find(|entry| {
                pending_scanner_heal_matches(
                    entry,
                    pending.kind,
                    &pending.bucket,
                    pending.object.as_deref(),
                    pending.version_id.as_deref(),
                )
            }) {
                record_pending_heal_attempt(entry, Self::now_secs());
                scanner.sync_pending_heals();
            }
            scanner
                .send_required_scanner_heal_request(
                    pending.kind,
                    pending.bucket.clone(),
                    pending.object.clone(),
                    pending.version_id.clone(),
                    request,
                )
                .await?;
        }

        Ok(())
    }
}
pub(super) fn pending_scanner_heal_candidate_type(kind: PendingScannerHealKind) -> &'static str {
    match kind {
        PendingScannerHealKind::Bucket => "bucket",
        PendingScannerHealKind::Object => "object",
    }
}

pub(super) fn pending_scanner_heal_matches(
    entry: &PendingScannerHeal,
    kind: PendingScannerHealKind,
    bucket: &str,
    object: Option<&str>,
    version_id: Option<&str>,
) -> bool {
    entry.kind == kind && entry.bucket == bucket && entry.object.as_deref() == object && entry.version_id.as_deref() == version_id
}

pub(super) fn pending_scanner_heal_identity(entry: &PendingScannerHeal) -> (u8, &str, Option<&str>, Option<&str>) {
    let kind = match entry.kind {
        PendingScannerHealKind::Bucket => 0,
        PendingScannerHealKind::Object => 1,
    };
    (kind, entry.bucket.as_str(), entry.object.as_deref(), entry.version_id.as_deref())
}

pub(super) fn sort_pending_scanner_heals_for_retry(entries: &mut [PendingScannerHeal]) {
    entries.sort_by(|a, b| {
        a.last_attempt
            .cmp(&b.last_attempt)
            .then_with(|| a.attempts.cmp(&b.attempts))
            .then_with(|| pending_scanner_heal_identity(a).cmp(&pending_scanner_heal_identity(b)))
    });
}

pub(super) fn pending_scanner_heal_retry_candidates(
    pending_heals: &[PendingScannerHeal],
    bucket: &str,
) -> Vec<PendingScannerHeal> {
    pending_scanner_heal_retry_candidates_at(pending_heals, bucket, FolderScanner::now_secs())
}

pub(super) fn pending_scanner_heal_retry_candidates_at(
    pending_heals: &[PendingScannerHeal],
    bucket: &str,
    now: u64,
) -> Vec<PendingScannerHeal> {
    // Schedule across scanner cycles rather than allocating a timer per hint.
    // A later Full response must not reset an already retried hint's backoff.
    let mut entries: Vec<&PendingScannerHeal> = pending_heals
        .iter()
        .filter(|entry| {
            let exponent = entry.attempts.saturating_sub(1).min(31);
            let delay = PENDING_HEAL_RETRY_BASE_SECS
                .saturating_mul(1_u64 << exponent)
                .min(PENDING_HEAL_RETRY_CAP_SECS);
            entry.bucket == bucket && now.checked_sub(entry.last_attempt).is_some_and(|age| age >= delay)
        })
        .collect();
    entries.sort_by(|a, b| {
        a.last_attempt
            .cmp(&b.last_attempt)
            .then_with(|| a.attempts.cmp(&b.attempts))
            .then_with(|| pending_scanner_heal_identity(a).cmp(&pending_scanner_heal_identity(b)))
    });
    entries.truncate(MAX_PENDING_SCANNER_HEAL_RETRIES_PER_BUCKET);
    entries.into_iter().cloned().collect()
}

pub(super) fn build_pending_scanner_heal_request(entry: &PendingScannerHeal) -> Option<HealChannelRequest> {
    let priority = if entry.last_admission_result == "full"
        || (entry.last_admission_result == "dropped" && entry.last_admission_reason == "queue_full")
    {
        HealChannelPriority::High
    } else {
        HealChannelPriority::Low
    };
    match entry.kind {
        PendingScannerHealKind::Bucket => Some(build_bucket_heal_request(entry.bucket.clone(), priority)),
        PendingScannerHealKind::Object => entry.object.as_ref().map(|object| {
            if entry.version_id.is_none() {
                build_non_destructive_object_heal_request(entry.bucket.clone(), object.clone(), entry.scan_mode, priority)
            } else {
                build_object_heal_request(
                    entry.bucket.clone(),
                    object.clone(),
                    entry.version_id.clone(),
                    entry.scan_mode,
                    priority,
                )
            }
        }),
    }
}
