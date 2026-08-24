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
/// The pending-scanner-heal ledger: durable heal intents recorded during scans and retried after MRF consumption.
use super::*;

impl FolderScanner {
    pub(super) fn sync_pending_heals(&mut self) {
        self.update_cache.info.pending_heals = self.new_cache.info.pending_heals.clone();
        self.pending_heals_changed = true;
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

    /// Batched variant of [`Self::clear_pending_scanner_heal`] for repaired
    /// notices (backlog#1894 axis B): one retain pass and one ledger sync
    /// for the whole notice set, so a mass-recovery first sweep cannot turn
    /// into thousands of full-table clones on the scan task. Only Object
    /// entries match — bucket-level heals are never the MRF consumer's work.
    pub(super) fn clear_pending_scanner_heals_for_repaired(&mut self, events: &[rustfs_common::mrf_channel::MrfRepairedEvent]) {
        // Pre-resolve the notice version strings once; each ledger entry then
        // compares against plain Option<&str>.
        let targets: Vec<(&str, &str, Option<String>)> = events
            .iter()
            .map(|event| (event.bucket.as_ref(), event.object.as_ref(), mrf_repaired_version_id(event.version_id)))
            .collect();
        let before = self.new_cache.info.pending_heals.len();
        self.new_cache.info.pending_heals.retain(|entry| {
            entry.kind != PendingScannerHealKind::Object
                || !targets.iter().any(|(bucket, object, version)| {
                    entry.bucket.as_str() == *bucket
                        && entry.object.as_deref() == Some(*object)
                        && entry.version_id.as_deref() == version.as_deref()
                })
        });
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
            entry.last_attempt = now;
            entry.attempts = entry.attempts.saturating_add(1);
            entry.last_admission_result = result.result_label().to_string();
            entry.last_admission_reason = result.reason_label().to_string();
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
            HealAdmissionResult::Accepted | HealAdmissionResult::Merged => {
                self.clear_pending_scanner_heal(kind, bucket, object, version_id);
            }
            HealAdmissionResult::Full | HealAdmissionResult::Dropped(HealAdmissionDropReason::QueueFull) => {
                self.record_pending_scanner_heal(kind, bucket, object, version_id, scan_mode, result);
            }
            HealAdmissionResult::Dropped(HealAdmissionDropReason::PolicyDropped) => {
                self.clear_pending_scanner_heal(kind, bucket, object, version_id);
            }
            // Admin-only overlap rejections (HS-06); the scanner never sees
            // them, but if it ever does, treat them as terminal like any
            // other policy drop rather than endlessly retrying.
            HealAdmissionResult::Dropped(HealAdmissionDropReason::AlreadyRunning)
            | HealAdmissionResult::Dropped(HealAdmissionDropReason::OverlappingPaths) => {
                self.clear_pending_scanner_heal(kind, bucket, object, version_id);
            }
        }
    }

    pub(super) async fn retry_pending_scanner_heals(&mut self) -> Result<(), ScannerError> {
        if !self.should_heal().await {
            return Ok(());
        }

        let bucket = self.new_cache.info.name.clone();
        // Backlog#1894 axis B: repairs the MRF consumer landed hand the
        // manager the heal task, so the matching pending-ledger entries are
        // retried nowhere — drop them here. Best-effort: a lost notice just
        // leaves the entry to expire through its own attempts/age limits.
        let repaired = rustfs_common::mrf_channel::take_mrf_repaired_events_for(&bucket);
        if !repaired.is_empty() {
            self.clear_pending_scanner_heals_for_repaired(&repaired);
        }
        self.prune_pending_scanner_heals();
        for pending in pending_scanner_heal_retry_candidates(&self.new_cache.info.pending_heals, &bucket) {
            if !self.should_heal().await {
                break;
            }

            let Some(request) = build_pending_scanner_heal_request(&pending) else {
                self.clear_pending_scanner_heal(pending.kind, &pending.bucket, None, pending.version_id.as_deref());
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

            self.send_required_scanner_heal_request(
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

/// Decode an MRF repaired-notice version id for ledger matching. A nil UUID
/// means "no value" per the repo-wide defensive-UUID invariant, so it maps
/// to `None` and matches unversioned ledger entries only.
pub(super) fn mrf_repaired_version_id(version_id: Option<[u8; 16]>) -> Option<String> {
    version_id
        .map(uuid::Uuid::from_bytes)
        .filter(|uuid| !uuid.is_nil())
        .map(|uuid| uuid.to_string())
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
    let mut entries: Vec<PendingScannerHeal> = pending_heals.iter().filter(|entry| entry.bucket == bucket).cloned().collect();
    sort_pending_scanner_heals_for_retry(&mut entries);
    entries.truncate(MAX_PENDING_SCANNER_HEAL_RETRIES_PER_BUCKET);
    entries
}

pub(super) fn build_pending_scanner_heal_request(entry: &PendingScannerHeal) -> Option<HealChannelRequest> {
    match entry.kind {
        PendingScannerHealKind::Bucket => Some(build_bucket_heal_request(entry.bucket.clone(), HealChannelPriority::High)),
        PendingScannerHealKind::Object => entry.object.as_ref().map(|object| {
            if entry.version_id.is_none() {
                build_non_destructive_object_heal_request(
                    entry.bucket.clone(),
                    object.clone(),
                    entry.scan_mode,
                    HealChannelPriority::High,
                )
            } else {
                build_object_heal_request(
                    entry.bucket.clone(),
                    object.clone(),
                    entry.version_id.clone(),
                    entry.scan_mode,
                    HealChannelPriority::High,
                )
            }
        }),
    }
}
