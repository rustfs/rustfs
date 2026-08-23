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
/// scanner cache locks and the cache snapshot persist/publish path.
use super::*;

pub(crate) fn scanner_cache_lock_resource(cache_name: &str, source: DataUsageCacheSource) -> String {
    let lock_name = format!("{SCANNER_CACHE_LOCK_SUFFIX}.pool-{}.set-{}", source.pool_index, source.set_index);
    path_join_buf(&[crate::BUCKET_META_PREFIX, cache_name, &lock_name])
}

pub(crate) fn scanner_cache_lock_timeout() -> Duration {
    Duration::from_secs(rustfs_utils::get_env_u64("RUSTFS_LOCK_ACQUIRE_TIMEOUT", 5))
}

#[derive(Debug)]
pub(crate) struct ScannerCacheLockGuards {
    scoped: NamespaceLockGuard,
}

impl ScannerCacheLockGuards {
    pub(crate) fn is_lock_lost(&self) -> bool {
        self.scoped.is_lock_lost()
    }
}

#[derive(Debug)]
pub(crate) enum ScannerCacheLockError {
    Create { resource: String, source: Error },
    Acquire { resource: String, source: LockError },
}

impl ScannerCacheLockError {
    pub(crate) fn state(&self) -> &'static str {
        match self {
            Self::Create { .. } => "lock_create_failed",
            Self::Acquire { .. } => "lock_acquire_failed",
        }
    }

    pub(crate) fn is_contention(&self) -> bool {
        matches!(
            self,
            Self::Acquire {
                source: LockError::Timeout { .. } | LockError::AlreadyLocked { .. },
                ..
            }
        )
    }
}

impl std::fmt::Display for ScannerCacheLockError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Create { resource, source } => write!(formatter, "create scanner cache lock {resource}: {source}"),
            Self::Acquire { resource, source } => write!(formatter, "acquire scanner cache lock {resource}: {source}"),
        }
    }
}

pub(crate) async fn acquire_scanner_cache_locks(
    store: &SetDisks,
    cache_name: &str,
    source: DataUsageCacheSource,
) -> std::result::Result<ScannerCacheLockGuards, ScannerCacheLockError> {
    let timeout = scanner_cache_lock_timeout();
    let scoped_resource = scanner_cache_lock_resource(cache_name, source);
    let scoped_lock = store
        .new_ns_lock(RUSTFS_META_BUCKET, &scoped_resource)
        .await
        .map_err(|source| ScannerCacheLockError::Create {
            resource: scoped_resource.clone(),
            source,
        })?;
    let scoped = scoped_lock
        .get_write_lock_quiet(timeout)
        .await
        .map_err(|source| ScannerCacheLockError::Acquire {
            resource: scoped_resource,
            source,
        })?;

    Ok(ScannerCacheLockGuards { scoped })
}

pub(super) async fn await_scanner_disk_shutdown<F>(scan: Pin<&mut F>)
where
    F: Future,
{
    let _ = tokio::time::timeout(SCANNER_CACHE_LOCK_LOSS_SHUTDOWN_TIMEOUT, scan).await;
}

pub(crate) fn current_cache_root_entry(
    cache: &DataUsageCache,
    name: &str,
    source: DataUsageCacheSource,
    next_cycle: u64,
    leader_epoch: u64,
    scan_plan_digest: DataUsageScanPlanDigest,
) -> std::result::Result<Option<DataUsageEntryInfo>, ScannerError> {
    let metadata_is_current = cache.info.name == name
        && cache.info.source == Some(source)
        && cache.info.snapshot_complete
        && cache.info.scan_plan_digest == Some(scan_plan_digest)
        && cache.info.last_update.is_some()
        && cache.info.next_cycle == next_cycle
        && cache.info.leader_epoch == leader_epoch
        && cache.info.cache_key_format == DATA_USAGE_CACHE_KEY_FORMAT;
    if !metadata_is_current {
        return Ok(None);
    }

    cache_root_entry_info(cache).map(Some)
}

pub(crate) enum DataUsageCacheScanState {
    Current(Box<DataUsageEntryInfo>),
    Prepared {
        outcome: DataUsageCachePrepareOutcome,
        invalid_current: Option<ScannerError>,
    },
}

pub(crate) fn current_cache_root_or_prepare(
    cache: &mut DataUsageCache,
    name: &str,
    source: DataUsageCacheSource,
    next_cycle: u64,
    leader_epoch: u64,
    scan_plan_digest: DataUsageScanPlanDigest,
    require_source: bool,
) -> DataUsageCacheScanState {
    match current_cache_root_entry(cache, name, source, next_cycle, leader_epoch, scan_plan_digest) {
        Ok(Some(root)) => DataUsageCacheScanState::Current(Box::new(root)),
        current => DataUsageCacheScanState::Prepared {
            invalid_current: current.err(),
            outcome: cache.prepare_for_scan(name, next_cycle, leader_epoch, source, scan_plan_digest, require_source),
        },
    }
}

#[cfg(test)]
pub(super) fn cache_snapshot_is_current(
    cache: &DataUsageCache,
    name: &str,
    source: DataUsageCacheSource,
    next_cycle: u64,
    leader_epoch: u64,
    scan_plan_digest: DataUsageScanPlanDigest,
) -> bool {
    matches!(
        current_cache_root_entry(cache, name, source, next_cycle, leader_epoch, scan_plan_digest),
        Ok(Some(_))
    )
}

pub(super) fn completed_data_usage_info(
    results: &[DataUsageCache],
    expected_sources: &HashSet<DataUsageCacheSource>,
    all_buckets: &[String],
    bucket_plan_complete: bool,
    budget_elapsed: bool,
    cancelled: bool,
) -> Option<(DataUsageInfo, SystemTime)> {
    if !bucket_plan_complete {
        return None;
    }
    let completed_set_count = results.iter().filter(|result| result.info.last_update.is_some()).count();
    if !should_publish_completed_snapshot(completed_set_count, results.len(), budget_elapsed, cancelled) {
        return None;
    }
    if !scanner_results_form_complete_snapshot(results, expected_sources) {
        return None;
    }

    if results.iter().any(|result| result.root().is_none()) {
        return None;
    }

    let mut total = DataUsageEntry::default();
    let mut bucket_entries = HashMap::with_capacity(all_buckets.len());
    for bucket in all_buckets {
        let mut merged = DataUsageEntry::default();
        for result in results {
            let entry = result.checked_flatten(bucket)?;
            if !merged.checked_merge(&entry) {
                return None;
            }
        }
        if !total.checked_merge(&merged) {
            return None;
        }
        bucket_entries.insert(bucket.clone(), merged);
    }

    let merged_last_update = results.iter().filter_map(|result| result.info.last_update).max()?;
    let buckets_usage = bucket_entries
        .iter()
        .map(|(bucket, entry)| Some((bucket.clone(), checked_bucket_usage_info(entry)?)))
        .collect::<Option<HashMap<_, _>>>()?;
    let bucket_sizes = buckets_usage
        .iter()
        .map(|(bucket, usage)| (bucket.clone(), usage.size))
        .collect();
    let data_usage_info = DataUsageInfo {
        last_update: Some(merged_last_update),
        scanner_cycle: Some(results.first()?.info.next_cycle),
        objects_total_count: u64::try_from(total.objects).ok()?,
        versions_total_count: u64::try_from(total.versions).ok()?,
        delete_markers_total_count: u64::try_from(total.delete_markers).ok()?,
        objects_total_size: u64::try_from(total.size).ok()?,
        tier_stats: total.all_tier_stats.filter(|tiers| !tiers.is_empty()),
        buckets_count: u64::try_from(all_buckets.len()).ok()?,
        bucket_sizes,
        buckets_usage,
        usage_snapshot_complete: true,
        ..Default::default()
    };
    Some((data_usage_info, merged_last_update))
}

/// Build a non-authoritative view from the set snapshots that completed this
/// cycle plus compatible per-set last-known-good caches.  The caller must
/// persist this result only on the observational object; a missing set is
/// intentionally represented by an incomplete state and is never treated as
/// an empty set.
pub(super) fn observational_data_usage_info(
    results: &[DataUsageCache],
    expected_sources: &HashSet<DataUsageCacheSource>,
    all_buckets: &[String],
    expected_plan_digest: DataUsageScanPlanDigest,
    scanner_cycle: u64,
    leader_epoch: u64,
) -> Option<(DataUsageInfo, SystemTime)> {
    let mut by_source = HashMap::with_capacity(results.len());
    for result in results {
        let source = result.info.source?;
        if !expected_sources.contains(&source) || by_source.insert(source, result).is_some() {
            return None;
        }
    }

    let mut usable = Vec::new();
    let mut set_states = Vec::with_capacity(expected_sources.len());
    let mut sources = expected_sources.iter().copied().collect::<Vec<_>>();
    sources.sort_by_key(|source| (source.pool_index, source.set_index));
    for source in sources {
        let result = by_source.get(&source).copied();
        let current = result.filter(|result| {
            result.info.snapshot_complete
                && result.info.next_cycle == scanner_cycle
                && result.info.leader_epoch == leader_epoch
                && result.info.scan_plan_digest == Some(expected_plan_digest)
        });
        let lkg = result.filter(|result| {
            !result.info.snapshot_complete
                && result.info.lkg_snapshot_complete
                && result.info.lkg_scan_plan_digest == Some(expected_plan_digest)
                && result.info.lkg_leader_epoch.is_some_and(|epoch| {
                    epoch < leader_epoch
                        || (epoch == leader_epoch && result.info.lkg_next_cycle.is_some_and(|cycle| cycle <= scanner_cycle))
                })
        });
        let current_snapshot = current.is_some();
        let selected = current.or(lkg);
        if let Some(selected) = selected {
            let (cycle, epoch, digest, last_update, complete) = if current_snapshot {
                (
                    Some(selected.info.next_cycle),
                    Some(selected.info.leader_epoch),
                    selected.info.scan_plan_digest.map(|digest| digest.0),
                    selected.info.last_update,
                    true,
                )
            } else {
                (
                    selected.info.lkg_next_cycle,
                    selected.info.lkg_leader_epoch,
                    selected.info.lkg_scan_plan_digest.map(|digest| digest.0),
                    selected.info.lkg_last_update,
                    false,
                )
            };
            set_states.push(DataUsageSnapshotSetState {
                pool_index: u64::try_from(source.pool_index).ok()?,
                set_index: u64::try_from(source.set_index).ok()?,
                scanner_cycle: cycle,
                scanner_epoch: epoch,
                scan_plan_digest: digest,
                complete,
                tombstone: false,
            });
            usable.push((selected, last_update));
        } else {
            set_states.push(DataUsageSnapshotSetState {
                pool_index: u64::try_from(source.pool_index).ok()?,
                set_index: u64::try_from(source.set_index).ok()?,
                scanner_cycle: None,
                scanner_epoch: None,
                scan_plan_digest: Some(expected_plan_digest.0),
                complete: false,
                tombstone: false,
            });
        }
    }
    if usable.is_empty() {
        return None;
    }

    let mut total = DataUsageEntry::default();
    let mut bucket_entries = HashMap::with_capacity(all_buckets.len());
    let mut merged_last_update = None;
    for (result, last_update) in usable {
        if let Some(update) = last_update {
            merged_last_update = Some(merged_last_update.map_or(update, |current: SystemTime| current.max(update)));
        }
        for bucket in all_buckets {
            let Some(entry) = result.checked_flatten(bucket) else {
                continue;
            };
            let bucket_entry = bucket_entries.entry(bucket.clone()).or_insert_with(DataUsageEntry::default);
            if !bucket_entry.checked_merge(&entry) {
                return None;
            }
            if !total.checked_merge(&entry) {
                return None;
            }
        }
    }
    let merged_last_update = merged_last_update?;
    let buckets_usage = bucket_entries
        .iter()
        .map(|(bucket, entry)| Some((bucket.clone(), checked_bucket_usage_info(entry)?)))
        .collect::<Option<HashMap<_, _>>>()?;
    Some((
        DataUsageInfo {
            last_update: Some(merged_last_update),
            scanner_cycle: Some(scanner_cycle),
            scanner_epoch: Some(leader_epoch),
            objects_total_count: u64::try_from(total.objects).ok()?,
            versions_total_count: u64::try_from(total.versions).ok()?,
            delete_markers_total_count: u64::try_from(total.delete_markers).ok()?,
            objects_total_size: u64::try_from(total.size).ok()?,
            tier_stats: total.all_tier_stats.filter(|tiers| !tiers.is_empty()),
            buckets_count: u64::try_from(buckets_usage.len()).ok()?,
            bucket_sizes: buckets_usage
                .iter()
                .map(|(bucket, usage)| (bucket.clone(), usage.size))
                .collect(),
            buckets_usage,
            usage_snapshot_complete: false,
            usage_snapshot_partial: true,
            usage_snapshot_converged: Some(false),
            usage_snapshot_set_states: set_states,
            ..Default::default()
        },
        merged_last_update,
    ))
}

pub(super) async fn send_cache_root_entry_info(
    bucket_result_tx: &mpsc::Sender<DataUsageEntryInfo>,
    cache: &DataUsageCache,
    pending_maintenance_work: &AtomicBool,
) -> std::result::Result<(), ScannerError> {
    let root = cache_root_entry_info(cache)?;
    send_cache_root_entry(bucket_result_tx, root, cache, pending_maintenance_work).await
}

pub(super) async fn send_cache_root_entry(
    bucket_result_tx: &mpsc::Sender<DataUsageEntryInfo>,
    root: DataUsageEntryInfo,
    cache: &DataUsageCache,
    pending_maintenance_work: &AtomicBool,
) -> std::result::Result<(), ScannerError> {
    record_bucket_pending_maintenance_work(cache, pending_maintenance_work);
    bucket_result_tx
        .send(root)
        .await
        .map_err(|err| ScannerError::Other(format!("scanner cache root channel closed: {err}")))
}

pub(super) async fn persist_and_publish_cache_snapshot(
    store: Arc<SetDisks>,
    updates: &mpsc::Sender<DataUsageCache>,
    mut cache_snapshot: DataUsageCache,
    cache_cycle_floor: &AtomicU64,
    expected_publication_epoch: u64,
) -> Option<SystemTime> {
    let source = cache_snapshot.info.source?;
    let guard = match acquire_scanner_cache_locks(store.as_ref(), DATA_USAGE_CACHE_NAME, source).await {
        Ok(guard) => guard,
        Err(err) => {
            error!(
                target: "rustfs::scanner::io",
                event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_IO,
                cache_name = DATA_USAGE_CACHE_NAME,
                state = err.state(),
                error = %err,
                "Scanner cache snapshot lock acquisition failed"
            );
            return None;
        }
    };

    let mut persisted = DataUsageCache::default();
    let revisions = match persisted.load_with_revisions(store.clone(), DATA_USAGE_CACHE_NAME).await {
        Ok(revisions) => revisions,
        Err(err) => {
            error!(
                target: "rustfs::scanner::io",
                event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_IO,
                cache_name = DATA_USAGE_CACHE_NAME,
                state = "load_or_revision_lookup_failed",
                error = %err,
                "Scanner cache snapshot load or revision lookup failed"
            );
            return None;
        }
    };
    let scan_plan_digest = cache_snapshot.info.scan_plan_digest?;
    if persisted.info.next_cycle > cache_snapshot.info.next_cycle {
        cache_cycle_floor.fetch_max(persisted.info.next_cycle, Ordering::AcqRel);
        warn!(
            target: "rustfs::scanner::io",
            event = EVENT_SCANNER_CACHE_PERSIST_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_IO,
            cache_name = DATA_USAGE_CACHE_NAME,
            requested_cycle = cache_snapshot.info.next_cycle,
            persisted_cycle = persisted.info.next_cycle,
            state = "stale_cycle_rejected",
            "Scanner rejected a set cache cycle regression"
        );
        return None;
    }
    if persisted.info.leader_epoch > cache_snapshot.info.leader_epoch {
        error!(
            target: "rustfs::scanner::io",
            event = EVENT_SCANNER_CACHE_PERSIST_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_IO,
            cache_name = DATA_USAGE_CACHE_NAME,
            requested_epoch = cache_snapshot.info.leader_epoch,
            persisted_epoch = persisted.info.leader_epoch,
            state = "stale_leader_rejected",
            "Scanner rejected a set cache snapshot from an older leader epoch"
        );
        return None;
    }
    if matches!(
        current_cache_root_entry(
            &persisted,
            DATA_USAGE_ROOT,
            source,
            cache_snapshot.info.next_cycle,
            cache_snapshot.info.leader_epoch,
            scan_plan_digest,
        ),
        Ok(Some(_))
    ) {
        cache_snapshot = persisted;
    } else {
        if guard.is_lock_lost() {
            error!(
                target: "rustfs::scanner::io",
                event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_IO,
                cache_name = DATA_USAGE_CACHE_NAME,
                state = "lock_lost",
                "Scanner cache snapshot save skipped after lock loss"
            );
            return None;
        }

        let done_save = Metrics::time(Metric::SaveUsage);
        if let Err(e) = cache_snapshot
            .save_with_revisions_for_epoch(store.clone(), DATA_USAGE_CACHE_NAME, &revisions, expected_publication_epoch)
            .await
        {
            error!(
                target: "rustfs::scanner::io",
                event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_IO,
                cache_name = DATA_USAGE_CACHE_NAME,
                state = "save_failed",
                error = %e,
                "Scanner cache snapshot persistence failed"
            );
            done_save();
            return None;
        }
        done_save();
    }
    if guard.is_lock_lost() {
        error!(
            target: "rustfs::scanner::io",
            event = EVENT_SCANNER_CACHE_PERSIST_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_IO,
            cache_name = DATA_USAGE_CACHE_NAME,
            state = "lock_lost_after_save",
            "Scanner cache snapshot publish skipped after lock loss"
        );
        return None;
    }
    // The persisted-root fast path performs no PUT, so it also needs the
    // cycle token re-admission before forwarding the root to the aggregate.
    // This final check covers both the fast path and a successful save.
    if scanner_publication_admission_for_epoch(store.clone(), expected_publication_epoch)
        .await
        .is_none()
    {
        error!(
            target: "rustfs::scanner::io",
            event = EVENT_SCANNER_CACHE_PERSIST_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_IO,
            cache_name = DATA_USAGE_CACHE_NAME,
            state = "publication_epoch_changed_before_publish",
            "Scanner cache root publish skipped after movement epoch change"
        );
        return None;
    }
    drop(guard);
    let last_update = cache_snapshot.info.last_update;

    if let Err(e) = updates.send(cache_snapshot).await {
        error!(
            target: "rustfs::scanner::io",
            event = EVENT_SCANNER_CACHE_PERSIST_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_IO,
            cache_name = DATA_USAGE_CACHE_NAME,
            state = "publish_failed",
            error = %e,
            "Scanner cache snapshot publish failed"
        );
    }

    last_update
}

pub(super) async fn send_data_usage_update(updates: &mpsc::Sender<DataUsageInfo>, data_usage_info: DataUsageInfo) -> Result<()> {
    updates.send(data_usage_info).await.map_err(|e| {
        error!(
            target: "rustfs::scanner::io",
            event = EVENT_SCANNER_DATA_USAGE_STREAM,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_IO,
            state = "send_failed",
            error = %e,
            "Scanner data usage publish failed"
        );
        StorageError::other("scanner data usage receiver closed before update delivery")
    })
}
