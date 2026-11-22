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

/// Transient indexes over the bucket's persisted unknown-size records. Folder completion
/// touches refreshed objects and the oldest debt, including when the namespace is very wide.
#[derive(Default)]
pub(super) struct SizeLedgerIndex {
    pub(super) by_scope: HashMap<String, HashSet<String>>,
    pub(super) by_age: std::collections::BTreeSet<(u64, String)>,
    pub(super) bytes: usize,
}

impl SizeLedgerIndex {
    pub(super) fn valid(entry: &SizeReconciliationEntry) -> bool {
        entry.key.len() <= 4096
            && entry.bucket.len() <= 512
            && entry.object.len() <= 512
            && entry.version_id.as_deref().is_none_or(|v| v.len() <= 64)
            && entry.generation.as_deref().is_none_or(|v| v.len() <= 64)
            && entry.reason.len() <= 64
    }

    pub(super) fn from_persisted(records: &mut HashMap<String, SizeReconciliationEntry>, now: u64) -> Self {
        let mut index = Self::default();
        let mut evicted_through: Option<(u64, String)> = None;
        for (key, mut entry) in std::mem::take(records) {
            if key != entry.key || !Self::valid(&entry) {
                continue;
            }
            if entry.first_seen == 0 || entry.first_seen > now {
                entry.first_seen = now;
            }
            if now.saturating_sub(entry.first_seen) > MAX_SIZE_RECONCILIATION_AGE_SECS {
                continue;
            }
            // HashMap iteration can deliver a smaller, older record after a byte eviction.
            // Every record through the last evicted age/key would also lose a full ordered prune.
            if evicted_through
                .as_ref()
                .is_some_and(|(seen, cutoff)| (entry.first_seen, key.as_str()) <= (*seen, cutoff.as_str()))
            {
                continue;
            }
            index.add(&entry);
            records.insert(key, entry);
            if records.len() > MAX_SIZE_RECONCILIATION_ENTRIES_PER_BUCKET
                || index.bytes > MAX_SIZE_RECONCILIATION_BYTES_PER_BUCKET
            {
                evicted_through = index.prune(records, now);
            }
        }
        index
    }

    pub(super) fn add(&mut self, entry: &SizeReconciliationEntry) {
        self.bytes += size_reconciliation_entry_bytes(entry);
        self.by_age.insert((entry.first_seen, entry.key.clone()));
        self.by_scope
            .entry(size_reconciliation_scope_key(&entry.bucket, &entry.object))
            .or_default()
            .insert(entry.key.clone());
    }

    fn remove(&mut self, entry: &SizeReconciliationEntry) {
        self.by_age.remove(&(entry.first_seen, entry.key.clone()));
        self.bytes -= size_reconciliation_entry_bytes(entry);
        let scope = size_reconciliation_scope_key(&entry.bucket, &entry.object);
        if let Some(keys) = self.by_scope.get_mut(&scope) {
            keys.remove(&entry.key);
            if keys.is_empty() {
                self.by_scope.remove(&scope);
            }
        }
    }

    pub(super) fn prune(&mut self, records: &mut HashMap<String, SizeReconciliationEntry>, now: u64) -> Option<(u64, String)> {
        let mut last = None;
        while let Some((first_seen, key)) = self.by_age.first() {
            if now.saturating_sub(*first_seen) <= MAX_SIZE_RECONCILIATION_AGE_SECS
                && records.len() <= MAX_SIZE_RECONCILIATION_ENTRIES_PER_BUCKET
                && self.bytes <= MAX_SIZE_RECONCILIATION_BYTES_PER_BUCKET
            {
                break;
            }
            if let Some(entry) = records.remove(key) {
                self.remove(&entry);
                last = Some((entry.first_seen, entry.key));
            } else {
                self.by_age.pop_first();
            }
        }
        last
    }
}

impl FolderScanner {
    /// A missing path on one representative cannot discharge an earlier set-wide debt. Only
    /// examine old, unobserved scopes after a completed walk, and require every configured slot.
    pub(super) async fn retire_confirmed_absent_sizes(&mut self, ctx: &CancellationToken, configured: Option<&HashSet<String>>) {
        use crate::storage_api::owner::EcstoreDiskAPI;
        let Some(configured) = configured else { return };
        let info = &self.new_cache.info;
        if info.size_reconciliation.is_empty()
            || self.unobserved_size_scopes.is_empty()
            || configured.is_empty()
            || info.scan_identity.as_ref().is_none_or(|identity| !identity.is_valid())
            || info
                .scan_progress
                .is_some_and(|progress| progress.started_plan != progress.requested_plan)
            || info.source.is_none()
        {
            return;
        }
        // A single-drive set has no peer heal inventory. Its representative owns the only slot.
        let disks = if self.disks.is_empty() && configured.len() == 1 {
            vec![self.local_disk.clone()]
        } else {
            self.disks.clone()
        };
        if disks.len() != configured.len()
            || disks.iter().any(|disk| scanner_disk_heal_scope(disk) != info.source)
            || disks.iter().map(|disk| disk.endpoint().to_string()).collect::<HashSet<_>>() != *configured
        {
            return;
        }
        let bucket = info.name.clone();
        let Some(before) = self.size_debt_membership(ctx, &disks, &bucket).await else { return };
        let scopes: Vec<_> = self.unobserved_size_scopes.drain().collect();
        // Keep completed proofs when a later group is interrupted by the bucket deadline.
        for group in scopes.chunks(32) {
            let mut absent = Vec::new();
            for scope in group {
                if ctx.is_cancelled() || self.budget.budget_elapsed() {
                    return;
                }
                self.collect_absent_size_keys(ctx, &disks, &bucket, scope, &mut absent).await;
            }
            if absent.is_empty() {
                continue;
            }
            if self.size_debt_membership(ctx, &disks, &bucket).await.as_ref() != Some(&before) || ctx.is_cancelled() {
                return;
            }
            for key in absent {
                if let Some(entry) = self.new_cache.info.size_reconciliation.remove(&key) {
                    self.size_ledger_index.remove(&entry);
                }
            }
        }
    }

    async fn collect_absent_size_keys(
        &self,
        ctx: &CancellationToken,
        disks: &[Arc<Disk>],
        bucket: &str,
        scope: &str,
        absent: &mut Vec<String>,
    ) {
        use crate::storage_api::owner::EcstoreDiskAPI;
        let Some(keys) = self.size_ledger_index.by_scope.get(scope) else { return };
        // Oversized names have a digest for display. A literal S3 name can resemble
        // that display value; only raw length-prefixed identities authorize a probe.
        let raw_prefix = format!("{scope}|");
        let Some(entry) = keys
            .iter()
            .filter(|key| key.starts_with(&raw_prefix))
            .find_map(|key| self.new_cache.info.size_reconciliation.get(key))
        else {
            return;
        };
        if entry.bucket != bucket
            || entry.object.is_empty()
            || entry.object.starts_with('/')
            || entry.object.contains(['\0', '\\'])
            || entry.object.split('/').any(|part| part == "." || part == "..")
        {
            return;
        }
        let encoded = rustfs_utils::path::encode_dir_object_ref(&entry.object);
        let path = path_join_buf(&[encoded.as_ref(), STORAGE_FORMAT_FILE]);
        for disk in disks {
            let result = self
                .size_evidence_io(ctx, disk, 1, 1, disk.read_file_stream(bucket, &path, 0, 1))
                .await;
            if !matches!(result, Some(Err(DiskError::FileNotFound))) {
                return;
            }
        }
        absent.extend(keys.iter().filter(|key| key.starts_with(&raw_prefix)).cloned());
    }

    async fn size_debt_membership(
        &self,
        ctx: &CancellationToken,
        disks: &[Arc<Disk>],
        bucket: &str,
    ) -> Option<Vec<(uuid::Uuid, time::OffsetDateTime)>> {
        use crate::storage_api::owner::EcstoreDiskAPI;
        let mut drives = Vec::with_capacity(disks.len());
        let mut unique = HashSet::with_capacity(disks.len());
        for disk in disks {
            let (id, volume) = self
                .size_evidence_io(ctx, disk, 2, 0, async {
                    let id = disk.get_disk_id().await?;
                    Ok((id, disk.stat_volume(bucket).await?))
                })
                .await?
                .ok()?;
            let id = id.filter(|id| !id.is_nil())?;
            if volume.name != bucket || !unique.insert(id) {
                return None;
            }
            drives.push((id, volume.created?));
        }
        Some(drives)
    }

    async fn size_evidence_io<T>(
        &self,
        ctx: &CancellationToken,
        disk: &Disk,
        ops: u64,
        bytes: u64,
        read: impl std::future::Future<Output = Result<T, DiskError>>,
    ) -> Option<Result<T, DiskError>> {
        wait_for_scanner_io(ctx, self.sleeper.admit_disk_io(ops, bytes)).await.ok()?;
        let operation = crate::workload_admission::with_scanner_physical_disk(
            disk.physical_admission(),
            run_scanner_metadata_operation(
                self.node_admission.as_ref(),
                ctx,
                crate::workload_admission::with_scanner_metadata_io(read),
            ),
        );
        wait_for_scanner_io(ctx, operation).await.ok()?.ok()
    }
}

const PENDING_HEAL_RETRY_BASE_SECS: u64 = 15 * 60;
const PENDING_HEAL_RETRY_CAP_SECS: u64 = 6 * 60 * 60;

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
            self.pending_heals_changed = true;
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
            self.pending_heals_changed = true;
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
        self.pending_heals_changed = true;
        self.prune_pending_scanner_heal_capacity();
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
            self.pending_heals_changed = true;
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
            self.pending_heals_changed = true;
        }

        self.prune_pending_scanner_heal_capacity();
    }

    fn prune_pending_scanner_heal_capacity(&mut self) {
        let len = self.new_cache.info.pending_heals.len();
        if len <= MAX_PENDING_SCANNER_HEALS_PER_BUCKET {
            return;
        }

        sort_pending_scanner_heals_for_retry(&mut self.new_cache.info.pending_heals);
        let remove_count = len.saturating_sub(MAX_PENDING_SCANNER_HEALS_PER_BUCKET);
        self.new_cache.info.pending_heals.drain(..remove_count);
        self.pending_heals_changed = true;
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
                    self.pending_heals_changed = true;
                }
            }
        }
    }

    pub(super) async fn retry_pending_scanner_heals(&mut self) -> Result<(), ScannerError> {
        if !self.try_should_heal().await? {
            return Ok(());
        }

        let bucket = clone_cache_value(&self.new_cache.info.name);
        // Legacy notices cannot bind a verified disposition to the current
        // incarnation, kind, set scope and responsibility generation.
        let repaired = rustfs_common::mrf_channel::take_mrf_repaired_events_for(&bucket);
        if !repaired.is_empty() {
            counter!("rustfs_scanner_unverified_repair_notices_total")
                .increment(u64::try_from(repaired.len()).unwrap_or(u64::MAX));
        }
        self.prune_pending_scanner_heals();
        for pending in pending_scanner_heal_retry_candidates(&self.new_cache.info.pending_heals, &bucket) {
            if !self.try_should_heal().await? {
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

            if let Some(entry) = self.new_cache.info.pending_heals.iter_mut().find(|entry| {
                pending_scanner_heal_matches(
                    entry,
                    pending.kind,
                    &pending.bucket,
                    pending.object.as_deref(),
                    pending.version_id.as_deref(),
                )
            }) {
                record_pending_heal_attempt(entry, Self::now_secs());
                self.pending_heals_changed = true;
            }
            self.send_required_scanner_heal_request(
                pending.kind,
                clone_cache_value(&pending.bucket),
                clone_cache_value(&pending.object),
                clone_cache_value(&pending.version_id),
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
    entries.into_iter().map(clone_cache_value).collect()
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
        PendingScannerHealKind::Bucket => Some(build_bucket_heal_request(clone_cache_value(&entry.bucket), priority)),
        PendingScannerHealKind::Object => entry.object.as_ref().map(|object| {
            if entry.version_id.is_none() {
                build_non_destructive_object_heal_request(
                    clone_cache_value(&entry.bucket),
                    clone_cache_value(object),
                    entry.scan_mode,
                    priority,
                )
            } else {
                build_object_heal_request(
                    clone_cache_value(&entry.bucket),
                    clone_cache_value(object),
                    clone_cache_value(&entry.version_id),
                    entry.scan_mode,
                    priority,
                )
            }
        }),
    }
}
