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
/// ScannerIOCache implementation for SetDisks: bucket ordering, worker fan-out, merge, and publish.
use super::*;

pub(super) async fn collect_completed_bucket_results(
    ctx: CancellationToken,
    mut cache: DataUsageCache,
    mut results: mpsc::Receiver<DataUsageEntryInfo>,
) -> (DataUsageCache, HashSet<String>) {
    let mut cancelled = false;
    let mut completed = HashSet::new();
    let mut received_roots = 0usize;
    let mut duplicate_applied_roots = 0usize;
    let mut rejected_roots = 0usize;
    loop {
        tokio::select! {
            _ = ctx.cancelled(), if !cancelled => cancelled = true,
            result = results.recv() => {
                let Some(result) = result else {
                    debug!(
                        target: "rustfs::scanner::io",
                        event = EVENT_SCANNER_SET_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_IO,
                        state = "bucket_root_collector_drained",
                        collector_cache_pool = cache.info.source.map(|source| source.pool_index),
                        collector_cache_set = cache.info.source.map(|source| source.set_index),
                        collector_cache_cycle = cache.info.next_cycle,
                        collector_cache_leader_epoch = cache.info.leader_epoch,
                        received_roots,
                        distinct_applied_roots = completed.len(),
                        duplicate_applied_roots,
                        rejected_roots,
                        cancellation_observed = cancelled,
                        cancelled_at_drain = ctx.is_cancelled(),
                        "Scanner bucket root collector drained"
                    );
                    return (cache, completed);
                };
                received_roots = received_roots.saturating_add(1);
                if let Some(bucket) = apply_bucket_result_to_cache(&mut cache, result, SystemTime::now()) {
                    if !completed.insert(bucket) {
                        duplicate_applied_roots = duplicate_applied_roots.saturating_add(1);
                    }
                } else {
                    rejected_roots = rejected_roots.saturating_add(1);
                }
            }
        }
    }
}

/// Stand in for every bucket of this set whose walk stopped early with the
/// bucket's last certified value from the previous complete set cache.
///
/// Usage is a sum: a certified bucket's value plus an uncertified bucket's
/// last certified value is a valid snapshot (see
/// `docs/architecture/scanner-per-bucket-convergence.md`), so a walk that
/// stops early no longer makes the whole set, and with it the whole cycle,
/// incomplete. The cycle reports the bucket pending and walks it again. A
/// bucket that never certified on this set, failed, was not found, or
/// changed incarnation is not covered and keeps the set incomplete. The
/// stand-in is the previous certified value, never this walk's running
/// total.
async fn carry_partial_bucket_values(
    cache: &mut DataUsageCache,
    old_cache: &DataUsageCache,
    old_cache_certified: bool,
    buckets: &[BucketInfo],
    completed_buckets: &HashSet<String>,
    bucket_failures: &ScannerBucketFailureState,
    current_bucket_incarnations: Option<&HashMap<String, uuid::Uuid>>,
) -> Vec<String> {
    if !old_cache_certified {
        return Vec::new();
    }
    let Some(current_bucket_incarnations) = current_bucket_incarnations else {
        return Vec::new();
    };
    // Snapshot each set in turn; the workers of other sets take these locks
    // one at a time and must never wait behind a finalising set.
    let partial = bucket_failures.partial.lock().await.clone();
    let failed = bucket_failures.hard.lock().await.clone();
    let namespace_not_found = bucket_failures.namespace_not_found.lock().await.clone();
    let mut carried = Vec::new();
    for bucket in buckets {
        let name = bucket.name.as_str();
        if completed_buckets.contains(name)
            || !partial.contains(name)
            || failed.contains(name)
            || namespace_not_found.contains(name)
        {
            continue;
        }
        let current_incarnation = current_bucket_incarnations
            .get(name)
            .filter(|incarnation| !incarnation.is_nil());
        let certified_incarnation = old_cache
            .info
            .scan_bucket_incarnations
            .get(name)
            .filter(|incarnation| !incarnation.is_nil());
        if current_incarnation.is_none() || certified_incarnation != current_incarnation {
            continue;
        }
        let Some(certified) = old_cache.checked_flatten(name) else {
            continue;
        };
        cache.replace(name, DATA_USAGE_ROOT, certified);
        carried.push(bucket.name.clone());
    }
    carried
}

#[derive(Clone, Copy)]
pub(super) struct ScannerSetCacheGeneration {
    pub(super) want_cycle: u64,
    pub(super) leader_epoch: u64,
    pub(super) tier_registry_generation: u64,
    pub(super) source: DataUsageCacheSource,
    pub(super) scan_plan_digest: DataUsageScanPlanDigest,
}

pub(super) struct PreparedScopedSetScan {
    pub(super) buckets: Vec<BucketInfo>,
    pub(super) cache: DataUsageCache,
    pub(super) cold_bucket_reuse_proof: Option<ScopedColdBucketReuseProof>,
}

pub(super) struct ScopedColdBucketReuseProof {
    pub(super) baseline_scan_plan_digest: DataUsageScanPlanDigest,
    pub(super) source: DataUsageCacheSource,
    pub(super) bucket_incarnations: HashMap<String, uuid::Uuid>,
}

impl ScopedColdBucketReuseProof {
    fn authorizes(&self, source: DataUsageCacheSource, baseline_scan_plan_digest: DataUsageScanPlanDigest) -> bool {
        self.source == source
            && self.baseline_scan_plan_digest == baseline_scan_plan_digest
            && !self.bucket_incarnations.is_empty()
    }
}

pub(super) fn prepare_scoped_set_scan(
    old_cache: &DataUsageCache,
    set_buckets: &[BucketInfo],
    all_buckets: &[BucketInfo],
    scope: &ScannerBucketScanScope,
    generation: ScannerSetCacheGeneration,
    current_bucket_incarnations: Option<&HashMap<String, uuid::Uuid>>,
) -> Option<PreparedScopedSetScan> {
    let (Some(selected_buckets), Some(baseline_scan_plan_digest)) = (&scope.selected_buckets, scope.baseline_scan_plan_digest)
    else {
        return None;
    };
    if selected_buckets.is_empty()
        || !old_cache.info.snapshot_complete
        || old_cache.info.last_update.is_none()
        || old_cache.info.name != DATA_USAGE_ROOT
        || old_cache.info.next_cycle > generation.want_cycle
        || old_cache.info.leader_epoch != generation.leader_epoch
        || old_cache.info.tier_registry_generation != Some(generation.tier_registry_generation)
        || old_cache.info.source != Some(generation.source)
        || old_cache.info.scan_plan_digest != Some(baseline_scan_plan_digest)
        || old_cache.info.cache_key_format != DATA_USAGE_CACHE_KEY_FORMAT
        || !old_cache.has_complete_root_inventory(&old_cache.find(DATA_USAGE_ROOT)?.children)
    {
        return None;
    }
    let unselected_bucket_incarnations =
        unselected_bucket_incarnation_bindings(old_cache, all_buckets, selected_buckets, current_bucket_incarnations)?;
    let cold_bucket_reuse_proof = (!unselected_bucket_incarnations.is_empty()).then_some(ScopedColdBucketReuseProof {
        baseline_scan_plan_digest,
        source: generation.source,
        bucket_incarnations: unselected_bucket_incarnations,
    });

    let mut cache = DataUsageCache {
        info: DataUsageCacheInfo {
            name: DATA_USAGE_ROOT.to_string(),
            next_cycle: generation.want_cycle,
            leader_epoch: generation.leader_epoch,
            tier_registry_generation: Some(generation.tier_registry_generation),
            source: Some(generation.source),
            snapshot_complete: false,
            scan_plan_digest: Some(generation.scan_plan_digest),
            cache_key_format: DATA_USAGE_CACHE_KEY_FORMAT,
            lkg_snapshot_complete: true,
            lkg_next_cycle: Some(old_cache.info.next_cycle),
            lkg_last_update: old_cache.info.last_update,
            lkg_leader_epoch: Some(old_cache.info.leader_epoch),
            lkg_scan_plan_digest: old_cache.info.scan_plan_digest,
            scan_bucket_incarnations: old_cache.info.scan_bucket_incarnations.clone(),
            ..Default::default()
        },
        cache: HashMap::new(),
    };
    cache.replace(DATA_USAGE_ROOT, "", DataUsageEntry::default());
    let mut current_bucket_names = HashSet::with_capacity(all_buckets.len());
    for bucket in all_buckets {
        if !current_bucket_names.insert(bucket.name.as_str()) {
            return None;
        }
        if selected_buckets.contains(&bucket.name) {
            cache.replace(&bucket.name, DATA_USAGE_ROOT, DataUsageEntry::default());
        } else {
            cache.copy_with_children(
                old_cache,
                &rustfs_data_usage::hash_path(&bucket.name),
                &Some(rustfs_data_usage::hash_path(DATA_USAGE_ROOT)),
            );
        }
    }

    Some(PreparedScopedSetScan {
        buckets: set_buckets
            .iter()
            .filter(|bucket| selected_buckets.contains(&bucket.name))
            .cloned()
            .collect(),
        cache,
        cold_bucket_reuse_proof,
    })
}

fn unselected_bucket_incarnation_bindings(
    old_cache: &DataUsageCache,
    all_buckets: &[BucketInfo],
    selected_buckets: &HashSet<String>,
    current_bucket_incarnations: Option<&HashMap<String, uuid::Uuid>>,
) -> Option<HashMap<String, uuid::Uuid>> {
    let mut unselected_buckets = all_buckets
        .iter()
        .filter(|bucket| !selected_buckets.contains(&bucket.name))
        .peekable();
    let Some(current_bucket_incarnations) = current_bucket_incarnations else {
        return unselected_buckets.peek().is_none().then(HashMap::new);
    };
    let mut bound_incarnations = HashMap::new();
    for bucket in unselected_buckets {
        let current = current_bucket_incarnations
            .get(&bucket.name)
            .filter(|incarnation| !incarnation.is_nil())?;
        if old_cache.find(&bucket.name).is_none()
            || old_cache
                .info
                .scan_bucket_incarnations
                .get(&bucket.name)
                .filter(|cached| !cached.is_nil())
                != Some(current)
        {
            return None;
        }
        bound_incarnations.insert(bucket.name.clone(), *current);
    }
    Some(bound_incarnations)
}

async fn scanner_current_bucket_incarnations(set: &SetDisks, all_buckets: &[BucketInfo]) -> Option<HashMap<String, uuid::Uuid>> {
    let mut incarnations = HashMap::with_capacity(all_buckets.len());
    for bucket in all_buckets {
        let Ok(incarnation) = set.bucket_incarnation_id_from_disk(&bucket.name).await else {
            return None;
        };
        if incarnation.is_nil() || incarnations.insert(bucket.name.clone(), incarnation).is_some() {
            return None;
        }
    }
    Some(incarnations)
}

#[async_trait::async_trait]
impl ScannerIOCache for SetDisks {
    #[tracing::instrument(skip(self, budget, scan_plan, updates))]
    async fn nsscanner_cache(
        self: Arc<Self>,
        ctx: CancellationToken,
        budget: Arc<ScannerCycleBudget>,
        scan_plan: ScannerBucketScanPlan,
        updates: mpsc::Sender<DataUsageCache>,
        want_cycle: u64,
        scan_mode: HealScanMode,
    ) -> Result<()> {
        let ScannerBucketScanPlan {
            mut buckets,
            all_buckets,
            scope,
            digest: scan_plan_digest,
            bucket_coverage_digest,
            checkpoint_coverage_digest,
            requires_full_scan,
            service_cohort,
            execution_digest,
            leader_epoch,
            tier_registry_generation,
            publication_epoch,
            dirty_usage_buckets,
            bucket_failures,
            bucket_completions,
            pending_maintenance_work,
            cache_cycle_floor,
            cold_zero_walk_reuse_observed,
            segment_invalidation_proof,
        } = scan_plan;
        let structural_scan_plan_digest = scan_plan_digest;
        let scan_plan_digest = scanner_bucket_work_digest(scan_plan_digest, scan_mode, requires_full_scan);
        let bucket_work_digest = scanner_bucket_work_digest(bucket_coverage_digest, scan_mode, requires_full_scan);
        let bucket_checkpoint_work_digest = scanner_bucket_work_digest(checkpoint_coverage_digest, scan_mode, requires_full_scan);
        let pool_label = self.pool_index.to_string();
        let set_label = self.set_index.to_string();

        let source = DataUsageCacheSource::new(self.pool_index, self.set_index);
        let expected_publication_epoch = match publication_epoch {
            Some(epoch) => epoch,
            None => scanner_publication_epoch(self.clone())
                .await
                .ok_or_else(|| StorageError::other("scanner cache publication is blocked by data movement"))?,
        };
        let mut old_cache = DataUsageCache::default();
        let initial_revisions = match old_cache.load_with_revisions(self.clone(), DATA_USAGE_CACHE_NAME).await {
            Ok(revisions) => Some(revisions),
            Err(e) => {
                warn!(
                    target: "rustfs::scanner::io",
                    event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_IO,
                    pool = self.pool_index,
                    set = self.set_index,
                    cache_name = DATA_USAGE_CACHE_NAME,
                    state = "old_cache_load_failed",
                    error = %e,
                    "Scanner old data usage cache load failed; rebuilding from bucket caches"
                );
                None
            }
        };
        // Captured before `prepare_for_scan` clears the mark: only a complete
        // snapshot may stand in for a bucket whose walk stops early.
        let old_cache_certified = old_cache.info.snapshot_complete && old_cache.info.last_update.is_some();
        let current_bucket_incarnations = scanner_current_bucket_incarnations(self.as_ref(), &all_buckets).await;
        let scoped_scan = prepare_scoped_set_scan(
            &old_cache,
            &buckets,
            &all_buckets,
            &scope,
            ScannerSetCacheGeneration {
                want_cycle,
                leader_epoch,
                tier_registry_generation,
                source,
                scan_plan_digest,
            },
            current_bucket_incarnations.as_ref(),
        );
        let cold_zero_walk_reuse_candidate = scoped_scan.as_ref().is_some_and(|prepared| {
            old_cache.info.next_cycle < want_cycle
                && scope.baseline_scan_plan_digest.is_some_and(|baseline_scan_plan_digest| {
                    prepared
                        .cold_bucket_reuse_proof
                        .as_ref()
                        .is_some_and(|proof| proof.authorizes(source, baseline_scan_plan_digest))
                })
        });
        let mut scoped_cache = scoped_scan.map(|mut prepared| {
            buckets = prepared.buckets;
            prepared.cache.info.scan_coverage_digest = Some(bucket_coverage_digest);
            prepared.cache
        });
        if buckets.is_empty() {
            let now = SystemTime::now();
            let completed_segment_invalidation_proof =
                scanner_completed_set_segment_invalidation_proof(&segment_invalidation_proof, cold_zero_walk_reuse_candidate);
            let mut cache = match scoped_cache.take() {
                Some(cache) => cache,
                None => {
                    let mut cache = DataUsageCache {
                        info: DataUsageCacheInfo {
                            name: DATA_USAGE_ROOT.to_string(),
                            next_cycle: want_cycle,
                            leader_epoch,
                            tier_registry_generation: Some(tier_registry_generation),
                            source: Some(source),
                            scan_plan_digest: Some(scan_plan_digest),
                            scan_coverage_digest: Some(bucket_coverage_digest),
                            cache_key_format: DATA_USAGE_CACHE_KEY_FORMAT,
                            segment_invalidation_proof: completed_segment_invalidation_proof.clone(),
                            scan_bucket_incarnations: current_bucket_incarnations.clone().unwrap_or_default(),
                            ..Default::default()
                        },
                        cache: HashMap::new(),
                    };
                    cache.replace(DATA_USAGE_ROOT, "", DataUsageEntry::default());
                    for bucket in all_buckets.iter() {
                        cache.replace(&bucket.name, DATA_USAGE_ROOT, DataUsageEntry::default());
                    }
                    cache
                }
            };
            cache.info.last_update = Some(now);
            cache.info.snapshot_complete = true;
            let replication_execution = super::cache::scanner_replication_execution_digest(execution_digest, &cache);
            cache.info.scan_execution_digest = Some(replication_execution);
            cache.info.segment_invalidation_proof = completed_segment_invalidation_proof;
            cache.info.lkg_snapshot_complete = false;
            cache.info.lkg_next_cycle = None;
            cache.info.lkg_last_update = None;
            cache.info.lkg_leader_epoch = None;
            cache.info.lkg_scan_plan_digest = None;
            if cold_zero_walk_reuse_candidate {
                cold_zero_walk_reuse_observed.store(true, Ordering::Release);
            }
            if cache.find(DATA_USAGE_ROOT).is_none() {
                cache.replace(DATA_USAGE_ROOT, "", DataUsageEntry::default());
            }
            reset_disk_bucket_scan_gauges(&pool_label, &set_label);
            return persist_and_publish_cache_snapshot(
                self,
                &updates,
                cache,
                initial_revisions.as_ref(),
                cache_cycle_floor.as_ref(),
                expected_publication_epoch,
            )
            .await
            .map(|_| ())
            .ok_or_else(|| StorageError::other("failed to persist empty scanner set scope"));
        }

        let (disks, healing) = self.get_online_disks_with_healing(false).await;
        if disks.is_empty() {
            debug!(
                target: "rustfs::scanner::io",
                event = EVENT_SCANNER_SET_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_IO,
                pool = self.pool_index,
                set = self.set_index,
                state = "no_online_disks",
                "Scanner set state found no online disks"
            );
            reset_disk_bucket_scan_gauges(&pool_label, &set_label);
            let lkg = old_cache.info.snapshot_complete.then(|| old_cache.clone());
            let mut incomplete_scope = lkg.clone().unwrap_or_default();
            incomplete_scope.info.name = DATA_USAGE_ROOT.to_string();
            incomplete_scope.info.next_cycle = want_cycle;
            incomplete_scope.info.last_update = None;
            incomplete_scope.info.leader_epoch = leader_epoch;
            incomplete_scope.info.source = Some(source);
            incomplete_scope.info.snapshot_complete = false;
            incomplete_scope.info.scan_plan_digest = Some(scan_plan_digest);
            incomplete_scope.info.cache_key_format = DATA_USAGE_CACHE_KEY_FORMAT;
            if let Some(lkg) = lkg {
                incomplete_scope.info.lkg_snapshot_complete = true;
                incomplete_scope.info.lkg_next_cycle = Some(lkg.info.next_cycle);
                incomplete_scope.info.lkg_last_update = lkg.info.last_update;
                incomplete_scope.info.lkg_leader_epoch = Some(lkg.info.leader_epoch);
                incomplete_scope.info.lkg_scan_plan_digest = lkg.info.scan_plan_digest;
            }
            let _ = updates.send(incomplete_scope).await;
            return Ok(());
        }
        // Preserve the original set topology across capability filtering. During
        // rolling upgrades, an old remote peer must not make a distributed set
        // look local and allow an unscoped legacy cache to be adopted.
        let require_cache_source = disks.iter().any(|disk| !disk.is_local());
        let mut coordinator_disks = Vec::new();
        let mut remote_candidates = Vec::new();
        for disk in disks {
            if disk.is_local() {
                coordinator_disks.push(disk);
            } else {
                remote_candidates.push(disk);
            }
        }
        let remote_groups = group_remote_disks_by_peer(remote_candidates, |disk| disk.host_name());
        let capability_results = join_all(remote_groups.into_iter().map(|disks| async move {
            let server_epoch = match disks.first() {
                Some(disk) => disk.ns_scanner_server_epoch().await,
                None => Ok(None),
            };
            (disks, server_epoch)
        }))
        .await;
        let mut remote_disks = Vec::new();
        let mut unsupported_remote_disks = 0_usize;
        for (disks, server_epoch) in capability_results {
            match server_epoch {
                Ok(Some(server_epoch)) => {
                    remote_disks.extend(disks.into_iter().map(|disk| (disk, server_epoch)));
                }
                Ok(None) => {
                    let disk_count = disks.len();
                    let peer = disks.first().map(|disk| disk.host_name()).unwrap_or_default();
                    debug!(
                        target: "rustfs::scanner::io",
                        event = EVENT_SCANNER_SET_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_IO,
                        pool = self.pool_index,
                        set = self.set_index,
                        peer = %peer,
                        disk_count,
                        state = "remote_scanner_unsupported",
                        "Scanner found a peer without remote namespace scanner support"
                    );
                    unsupported_remote_disks = unsupported_remote_disks.saturating_add(disk_count);
                    coordinator_disks.extend(disks);
                }
                Err(err) => {
                    let peer = disks.first().map(|disk| disk.host_name()).unwrap_or_default();
                    let disk_count = disks.len();
                    debug!(
                        target: "rustfs::scanner::io",
                        event = EVENT_SCANNER_SET_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_IO,
                        pool = self.pool_index,
                        set = self.set_index,
                        peer = %peer,
                        disk_count,
                        state = "remote_scanner_probe_failed",
                        error = %err,
                        "Scanner skipped a peer whose remote namespace scanner capability could not be confirmed"
                    );
                }
            }
        }
        let remote_disk_count = remote_disks.len();
        let workers = namespace_scanner_workers(coordinator_disks, remote_disks);
        if workers.is_empty() {
            debug!(
                target: "rustfs::scanner::io",
                event = EVENT_SCANNER_SET_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_IO,
                pool = self.pool_index,
                set = self.set_index,
                state = "no_compatible_disks",
                "Scanner set state found no usable namespace scanner disks"
            );
            reset_disk_bucket_scan_gauges(&pool_label, &set_label);
            let lkg = old_cache.info.snapshot_complete.then(|| old_cache.clone());
            let mut incomplete_scope = lkg.clone().unwrap_or_default();
            incomplete_scope.info.name = DATA_USAGE_ROOT.to_string();
            incomplete_scope.info.next_cycle = want_cycle;
            incomplete_scope.info.last_update = None;
            incomplete_scope.info.leader_epoch = leader_epoch;
            incomplete_scope.info.source = Some(source);
            incomplete_scope.info.snapshot_complete = false;
            incomplete_scope.info.scan_plan_digest = Some(scan_plan_digest);
            incomplete_scope.info.cache_key_format = DATA_USAGE_CACHE_KEY_FORMAT;
            if let Some(lkg) = lkg {
                incomplete_scope.info.lkg_snapshot_complete = true;
                incomplete_scope.info.lkg_next_cycle = Some(lkg.info.next_cycle);
                incomplete_scope.info.lkg_last_update = lkg.info.last_update;
                incomplete_scope.info.lkg_leader_epoch = Some(lkg.info.leader_epoch);
                incomplete_scope.info.lkg_scan_plan_digest = lkg.info.scan_plan_digest;
            }
            let _ = updates.send(incomplete_scope).await;
            return Ok(());
        }
        let set_disk_inventory = Arc::new(scanner_set_disk_inventory(self.as_ref()).await);
        if unsupported_remote_disks > 0 {
            debug!(
                target: "rustfs::scanner::io",
                event = EVENT_SCANNER_SET_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_IO,
                pool = self.pool_index,
                set = self.set_index,
                v4_disks = remote_disk_count,
                unsupported_remote_disks,
                state = "unsupported_remote_disks_using_coordinator",
                "Scanner set assigned remote disks without namespace scanner support to coordinator-driven workers"
            );
        }
        let online_disks = workers.len();
        let walkers_per_disk = crate::runtime_config::scanner_walkers_per_disk_configured();
        let walkers = namespace_scanner_walkers(workers, walkers_per_disk, |disk| disk.is_local());
        let dispatch_limit = scanner_max_concurrent_disk_scans(walkers.len());
        let configured_disk_scans = crate::runtime_config::scanner_max_concurrent_disk_scans_configured();
        let serial_progress_accounting = budget.requires_serial_progress_accounting();
        let disk_scan_limit = scanner_budgeted_concurrency_limit(dispatch_limit, serial_progress_accounting);
        record_disk_scan_concurrency_limit(&pool_label, &set_label, disk_scan_limit);
        tracing::info!(
            target: "rustfs::scanner::io",
            event = EVENT_SCANNER_SET_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_IO,
            state = "disk_concurrency_budget",
            reason = if serial_progress_accounting {
                "count_budget"
            } else if configured_disk_scans != 0 && configured_disk_scans < walkers.len() {
                "configured_cap"
            } else {
                "available_walkers"
            },
            pool = self.pool_index,
            set = self.set_index,
            online_disks,
            walkers_per_disk,
            walkers = walkers.len(),
            dispatch_limit,
            concurrency_limit = disk_scan_limit,
            "Scanner disk concurrency budget resolved"
        );
        let disk_scan_semaphore = Arc::new(Semaphore::new(disk_scan_limit));
        let queued_disk_bucket_scans = Arc::new(AtomicUsize::new(buckets.len()));
        let active_disk_bucket_scans = Arc::new(AtomicUsize::new(0));
        record_disk_bucket_scans_queued(buckets.len(), &pool_label, &set_label);
        record_disk_bucket_scans_active(0, &pool_label, &set_label);
        let _reset_disk_bucket_scan_gauges = DiskBucketScanGaugeReset::new(pool_label.clone(), set_label.clone());

        let mut cache = if let Some(cache) = scoped_cache.take() {
            cache
        } else {
            // Fence a stale set aggregate before copying entries into per-bucket work caches.
            if old_cache.info.next_cycle <= want_cycle
                && old_cache.info.leader_epoch <= leader_epoch
                && old_cache.info.tier_registry_generation != Some(tier_registry_generation)
            {
                old_cache.info.scan_plan_digest = None;
            }
            let old_lkg = old_cache.info.snapshot_complete.then_some({
                (
                    old_cache.info.next_cycle,
                    old_cache.info.last_update,
                    old_cache.info.leader_epoch,
                    old_cache.info.scan_plan_digest,
                )
            });
            // A complete set cache written under the same structural plan with
            // another work flag (a full or Deep sweep against a scoped one)
            // describes the same namespace. Let `prepare_for_scan` keep its
            // entries as seeds, so a bucket whose walk stops early in this
            // cycle can publish its last certified value
            // (`carry_partial_bucket_values`); the cycle-level baseline proof
            // accepts the same variants. The LKG identity above keeps the
            // digest the cache was written under.
            if old_cache_certified
                && old_cache.info.scan_plan_digest != Some(scan_plan_digest)
                && old_cache
                    .info
                    .scan_plan_digest
                    .is_some_and(|digest| scanner_plan_digest_is_work_variant(structural_scan_plan_digest, digest))
            {
                old_cache.info.scan_plan_digest = Some(scan_plan_digest);
            }
            let prepare_outcome = match old_cache.prepare_for_scan(
                DATA_USAGE_ROOT,
                want_cycle,
                leader_epoch,
                source,
                scan_plan_digest,
                require_cache_source,
            ) {
                DataUsageCachePrepareOutcome::RejectedNewerCycle => {
                    cache_cycle_floor.fetch_max(old_cache.info.next_cycle, Ordering::AcqRel);
                    warn!(
                        target: "rustfs::scanner::io",
                        event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_IO,
                        pool = self.pool_index,
                        set = self.set_index,
                        cache_name = DATA_USAGE_CACHE_NAME,
                        requested_cycle = want_cycle,
                        cached_cycle = old_cache.info.next_cycle,
                        state = "stale_cycle_rejected",
                        "Scanner rejected a set cache cycle regression"
                    );
                    return Ok(());
                }
                DataUsageCachePrepareOutcome::RejectedNewerLeader => {
                    warn!(
                        target: "rustfs::scanner::io",
                        event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_IO,
                        pool = self.pool_index,
                        set = self.set_index,
                        cache_name = DATA_USAGE_CACHE_NAME,
                        requested_epoch = leader_epoch,
                        cached_epoch = old_cache.info.leader_epoch,
                        state = "stale_leader_rejected",
                        "Scanner rejected work from an older leader epoch"
                    );
                    return Ok(());
                }
                outcome => outcome,
            };
            if matches!(prepare_outcome, DataUsageCachePrepareOutcome::Reused)
                && let Some((cycle, last_update, epoch, digest)) = old_lkg
            {
                old_cache.info.lkg_snapshot_complete = true;
                old_cache.info.lkg_next_cycle = Some(cycle);
                old_cache.info.lkg_last_update = last_update;
                old_cache.info.lkg_leader_epoch = Some(epoch);
                old_cache.info.lkg_scan_plan_digest = digest;
            }

            let mut cache = DataUsageCache {
                info: DataUsageCacheInfo {
                    name: DATA_USAGE_ROOT.to_string(),
                    next_cycle: want_cycle,
                    leader_epoch,
                    tier_registry_generation: Some(tier_registry_generation),
                    source: Some(source),
                    snapshot_complete: false,
                    scan_plan_digest: Some(scan_plan_digest),
                    scan_coverage_digest: Some(bucket_coverage_digest),
                    cache_key_format: DATA_USAGE_CACHE_KEY_FORMAT,
                    lkg_snapshot_complete: old_cache.info.lkg_snapshot_complete,
                    lkg_next_cycle: old_cache.info.lkg_next_cycle,
                    lkg_last_update: old_cache.info.lkg_last_update,
                    lkg_leader_epoch: old_cache.info.lkg_leader_epoch,
                    lkg_scan_plan_digest: old_cache.info.lkg_scan_plan_digest,
                    segment_invalidation_proof: None,
                    scan_bucket_incarnations: current_bucket_incarnations.clone().unwrap_or_default(),
                    ..Default::default()
                },
                cache: HashMap::new(),
            };
            cache.replace(DATA_USAGE_ROOT, "", DataUsageEntry::default());
            for bucket in all_buckets.iter() {
                cache.replace(&bucket.name, DATA_USAGE_ROOT, DataUsageEntry::default());
            }
            cache
        };

        let (bucket_tx, bucket_rx) = mpsc::channel::<BucketInfo>(buckets.len());

        let mut permutes = buckets.clone();
        permutes.shuffle(&mut rand::rng());
        let mut scan_order = bucket_usage_scan_order(&permutes, &old_cache, &dirty_usage_buckets);
        if let Some(cohort) = &service_cohort {
            cohort
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .order_buckets(source, &mut scan_order);
        }

        for bucket in scan_order.iter() {
            if let Some(c) = old_cache.find(&bucket.name) {
                cache.replace(&bucket.name, DATA_USAGE_ROOT, c.clone());
            }

            if let Err(e) = bucket_tx.send(bucket.clone()).await {
                record_failed_dirty_bucket(&bucket_failures.hard, &bucket.name).await;
                error!(
                    target: "rustfs::scanner::io",
                    event = EVENT_SCANNER_SET_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_IO,
                    bucket = %bucket.name,
                    state = "send_bucket_failed",
                    error = %e,
                    "Scanner bucket dispatch failed"
                );
            }
        }

        let (bucket_result_tx, bucket_result_rx) = mpsc::channel::<DataUsageEntryInfo>(walkers.len());
        let collect_bucket_results_fut =
            AbortOnDropHandle::new(tokio::spawn(collect_completed_bucket_results(ctx.clone(), cache, bucket_result_rx)));

        let mut futs = Vec::new();

        let bucket_rx_mutex: Arc<Mutex<mpsc::Receiver<BucketInfo>>> = Arc::new(Mutex::new(bucket_rx));
        let remaining_bucket_work = Arc::new(AtomicUsize::new(buckets.len()));
        let bucket_work_complete = CancellationToken::new();
        for NamespaceScannerWalker {
            disk,
            mode: worker_mode,
            local_admission,
        } in walkers
        {
            let service_cohort_clone = service_cohort.clone();
            let bucket_rx_mutex_clone = bucket_rx_mutex.clone();
            let bucket_tx_clone = bucket_tx.clone();
            let remaining_bucket_work_clone = remaining_bucket_work.clone();
            let bucket_work_complete_clone = bucket_work_complete.clone();
            let ctx_clone = ctx.clone();
            let budget_clone = budget.clone();
            let store_clone_clone = self.clone();
            let bucket_result_tx_clone = bucket_result_tx.clone();
            let set_disk_inventory_clone = set_disk_inventory.clone();
            let disk_scan_semaphore_clone = disk_scan_semaphore.clone();
            let queued_disk_bucket_scans_clone = queued_disk_bucket_scans.clone();
            let active_disk_bucket_scans_clone = active_disk_bucket_scans.clone();
            let pool_label_clone = pool_label.clone();
            let set_label_clone = set_label.clone();
            let failed_dirty_buckets_clone = bucket_failures.hard.clone();
            let partial_dirty_buckets_clone = bucket_failures.partial.clone();
            let pending_maintenance_work_clone = pending_maintenance_work.clone();
            let dirty_usage_buckets_clone = dirty_usage_buckets.clone();
            let scope_clone = scope.clone();
            let cache_cycle_floor_clone = cache_cycle_floor.clone();
            let expected_publication_epoch_clone = expected_publication_epoch;
            #[cfg(feature = "e2e-test-hooks")]
            let worker_incarnations = current_bucket_incarnations.clone();
            let remote_server_epoch = match worker_mode {
                NamespaceScannerWorkerMode::RemoteV4(server_epoch) => Some(server_epoch),
                NamespaceScannerWorkerMode::Coordinator => None,
            };
            #[cfg(test)]
            let worker_index = futs.len();
            futs.push(AbortOnDropHandle::new(tokio::spawn(async move {
                #[cfg(test)]
                super::tests::observe_coordinator_bucket_worker_started(worker_index).await;
                let remote_session_id = uuid::Uuid::new_v4();
                let mut remote_session_sequence = 0_u64;
                loop {
                    // Do not prefetch a FIFO member into an independently
                    // scheduled permit waiter: that can reorder admissions.
                    let permit_wait_start = Instant::now();
                    let Some(_permit) =
                        wait_for_bucket_scan_permit(&disk_scan_semaphore_clone, &ctx_clone, &bucket_work_complete_clone).await
                    else {
                        break;
                    };
                    if ctx_clone.is_cancelled() || budget_clone.budget_elapsed() {
                        break;
                    }
                    let permit_wait_elapsed = permit_wait_start.elapsed();
                    let bucket = tokio::select! {
                        _ = bucket_work_complete_clone.cancelled() => break,
                        _ = ctx_clone.cancelled() => break,
                        bucket = async { bucket_rx_mutex_clone.lock().await.recv().await } => {
                            let Some(bucket) = bucket else {
                                break;
                            };
                            bucket
                        }
                    };
                    let mut work_guard =
                        BucketWorkGuard::new(remaining_bucket_work_clone.clone(), bucket_work_complete_clone.clone());
                    // Prefix hints are process-local. Never hand one to a
                    // remote or legacy-coordinator disk path.
                    let prefix_scan_scope = disk.is_local().then(|| scope_clone.prefix_scope_for(&bucket.name)).flatten();

                    metrics::histogram!(
                        METRIC_SCANNER_DISK_SCAN_WAIT_SECONDS,
                        "pool" => pool_label_clone.clone(),
                        "set" => set_label_clone.clone()
                    )
                    .record(permit_wait_elapsed.as_secs_f64());
                    decrement_disk_bucket_scans_queued(&queued_disk_bucket_scans_clone, &pool_label_clone, &set_label_clone);
                    let _active_guard = DiskBucketScanActiveGuard::new(
                        active_disk_bucket_scans_clone.clone(),
                        pool_label_clone.clone(),
                        set_label_clone.clone(),
                    );
                    if ctx_clone.is_cancelled() || budget_clone.budget_elapsed() {
                        break;
                    }
                    if let Some(cohort) = &service_cohort_clone {
                        cohort
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .record_admitted(source, &bucket.name);
                    }

                    debug!(
                        target: "rustfs::scanner::io",
                        event = EVENT_SCANNER_DISK_BUCKET_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_IO,
                        bucket = %bucket.name,
                        state = "scan_started",
                        "Scanner disk bucket scan started"
                    );

                    let cache_name = path_join_buf(&[&bucket.name, DATA_USAGE_CACHE_NAME]);
                    let bucket_dirty_generation = dirty_usage_buckets_clone.get(&bucket.name).copied();
                    // The reuse plan a complete cache is current under, and the
                    // coverage plan a sweep is started and resumed under.
                    let bucket_plans = ScannerBucketPlanDigests::from_work_plans(
                        bucket_work_digest,
                        bucket_checkpoint_work_digest,
                        bucket_dirty_generation,
                    );
                    let bucket_scan_plan_digest = bucket_plans.reuse;
                    let bucket_checkpoint_plan_digest = bucket_plans.coverage;

                    if let Some(server_epoch) = remote_server_epoch {
                        let request_sequence = remote_session_sequence;
                        let Some(next_sequence) = remote_session_sequence.checked_add(1) else {
                            record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                            error!(
                                target: "rustfs::scanner::io",
                                event = EVENT_SCANNER_DISK_BUCKET_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_IO,
                                bucket = %bucket.name,
                                state = "remote_session_sequence_exhausted",
                                "Remote scanner session sequence exhausted"
                            );
                            break;
                        };
                        remote_session_sequence = next_sequence;
                        let remote_scan = crate::remote_scanner::scan_remote_bucket(
                            &disk,
                            ctx_clone.clone(),
                            budget_clone.clone(),
                            crate::remote_scanner::RemoteScannerScanSpec {
                                bucket: &bucket.name,
                                next_cycle: want_cycle,
                                leader_epoch,
                                server_epoch,
                                session_id: remote_session_id,
                                session_sequence: request_sequence,
                                plans: bucket_plans,
                                tier_registry_generation,
                                skip_healing: healing,
                                scan_mode,
                            },
                        );
                        #[cfg(feature = "e2e-test-hooks")]
                        let remote_scan = crate::e2e_barrier::worker_observation::with_incarnation(
                            worker_incarnations.as_ref().and_then(|values| values.get(&bucket.name)).copied(),
                            remote_scan,
                        );
                        let remote_outcome = remote_scan.await;
                        match remote_outcome {
                            Ok(crate::remote_scanner::RemoteScannerOutcome::Complete {
                                usage,
                                pending_maintenance_work,
                            }) => {
                                if pending_maintenance_work {
                                    pending_maintenance_work_clone.store(true, Ordering::Release);
                                }
                                if let Err(e) = bucket_result_tx_clone.send(*usage).await {
                                    record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                                    error!(
                                        target: "rustfs::scanner::io",
                                        event = EVENT_SCANNER_DATA_USAGE_STREAM,
                                        component = LOG_COMPONENT_SCANNER,
                                        subsystem = LOG_SUBSYSTEM_IO,
                                        bucket = %bucket.name,
                                        state = "send_remote_root_failed",
                                        error = %e,
                                        "Remote scanner root entry publish failed"
                                    );
                                }
                            }
                            Ok(crate::remote_scanner::RemoteScannerOutcome::Partial) => {
                                record_partial_dirty_bucket(&partial_dirty_buckets_clone, &bucket.name).await;
                                debug!(
                                    target: "rustfs::scanner::io",
                                    event = EVENT_SCANNER_DISK_BUCKET_STATE,
                                    component = LOG_COMPONENT_SCANNER,
                                    subsystem = LOG_SUBSYSTEM_IO,
                                    state = "remote_scan_partial",
                                    bucket = %bucket.name,
                                    pool = source.pool_index,
                                    set = source.set_index,
                                    cycle = want_cycle,
                                    disk = %disk.path().display(),
                                    "Remote scanner bucket returned partial coverage"
                                );
                            }
                            Ok(crate::remote_scanner::RemoteScannerOutcome::NamespaceNotFound) => {
                                if requeue_bucket_work(&bucket_tx_clone, &bucket, &mut work_guard).await {
                                    increment_disk_bucket_scans_queued(
                                        &queued_disk_bucket_scans_clone,
                                        &pool_label_clone,
                                        &set_label_clone,
                                    );
                                } else {
                                    record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                                }
                                debug!(
                                    target: "rustfs::scanner::io",
                                    event = EVENT_SCANNER_DISK_BUCKET_STATE,
                                    component = LOG_COMPONENT_SCANNER,
                                    subsystem = LOG_SUBSYSTEM_IO,
                                    bucket = %bucket.name,
                                    state = "remote_namespace_missing_requeued",
                                    "Remote scanner requeued a bucket missing from this disk"
                                );
                                break;
                            }
                            Ok(crate::remote_scanner::RemoteScannerOutcome::CycleAhead(required_cycle)) => {
                                cache_cycle_floor_clone.fetch_max(required_cycle, Ordering::AcqRel);
                                record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                                warn!(
                                    target: "rustfs::scanner::io",
                                    event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                                    component = LOG_COMPONENT_SCANNER,
                                    subsystem = LOG_SUBSYSTEM_IO,
                                    bucket = %bucket.name,
                                    requested_cycle = want_cycle,
                                    cached_cycle = required_cycle,
                                    state = "remote_stale_cycle_rejected",
                                    "Remote scanner rejected a bucket cache cycle regression"
                                );
                            }
                            Err(e) => {
                                if e.retry_bucket_work() {
                                    if requeue_bucket_work(&bucket_tx_clone, &bucket, &mut work_guard).await {
                                        increment_disk_bucket_scans_queued(
                                            &queued_disk_bucket_scans_clone,
                                            &pool_label_clone,
                                            &set_label_clone,
                                        );
                                    } else {
                                        record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                                    }
                                    debug!(
                                        target: "rustfs::scanner::io",
                                        event = EVENT_SCANNER_DISK_BUCKET_STATE,
                                        component = LOG_COMPONENT_SCANNER,
                                        subsystem = LOG_SUBSYSTEM_IO,
                                        state = "remote_zero_progress_requeued",
                                        bucket = %bucket.name,
                                        error = %e,
                                        "Remote scanner requeued bucket work"
                                    );
                                    break;
                                }
                                record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                                if ctx_clone.is_cancelled() {
                                    debug!(
                                        target: "rustfs::scanner::io",
                                        event = EVENT_SCANNER_DISK_BUCKET_STATE,
                                        component = LOG_COMPONENT_SCANNER,
                                        subsystem = LOG_SUBSYSTEM_IO,
                                        bucket = %bucket.name,
                                        state = "remote_cancelled",
                                        error = %e,
                                        "Remote scanner bucket scan cancelled"
                                    );
                                } else {
                                    error!(
                                        target: "rustfs::scanner::io",
                                        event = EVENT_SCANNER_DISK_BUCKET_STATE,
                                        component = LOG_COMPONENT_SCANNER,
                                        subsystem = LOG_SUBSYSTEM_IO,
                                        bucket = %bucket.name,
                                        state = "remote_scan_failed",
                                        error = %e,
                                        "Remote scanner bucket scan failed"
                                    );
                                }
                                if e.retire_worker() {
                                    break;
                                }
                            }
                        }
                        continue;
                    }

                    let _local_admission = if let Some(share) = &local_admission {
                        match share.acquire(&disk) {
                            Ok(admission) => Some(admission),
                            Err(e) => {
                                if requeue_bucket_work(&bucket_tx_clone, &bucket, &mut work_guard).await {
                                    increment_disk_bucket_scans_queued(
                                        &queued_disk_bucket_scans_clone,
                                        &pool_label_clone,
                                        &set_label_clone,
                                    );
                                } else {
                                    record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                                }
                                debug!(
                                    target: "rustfs::scanner::io",
                                    event = EVENT_SCANNER_DISK_BUCKET_STATE,
                                    component = LOG_COMPONENT_SCANNER,
                                    subsystem = LOG_SUBSYSTEM_IO,
                                    bucket = %bucket.name,
                                    state = "local_disk_busy",
                                    error = %e,
                                    "Scanner local disk is already serving namespace scanner work"
                                );
                                break;
                            }
                        }
                    } else {
                        None
                    };
                    #[cfg(test)]
                    super::tests::observe_coordinator_bucket_scan_admitted(worker_index, &bucket.name).await;

                    // Lock order: scanner leader fence -> set-scoped per-bucket cache lock ->
                    // cache object read/write.
                    let mut cache_guard = match acquire_scanner_cache_locks(store_clone_clone.as_ref(), &cache_name, source).await {
                        Ok(guard) => guard,
                        Err(e) => {
                            if e.is_contention() {
                                if requeue_bucket_work(&bucket_tx_clone, &bucket, &mut work_guard).await {
                                    increment_disk_bucket_scans_queued(
                                        &queued_disk_bucket_scans_clone,
                                        &pool_label_clone,
                                        &set_label_clone,
                                    );
                                } else {
                                    record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                                }
                                debug!(
                                    target: "rustfs::scanner::io",
                                    event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                                    component = LOG_COMPONENT_SCANNER,
                                    subsystem = LOG_SUBSYSTEM_IO,
                                    bucket = %bucket.name,
                                    cache_name = %cache_name,
                                    state = "lock_contention_requeued",
                                    error = %e,
                                    "Scanner bucket cache lock contention requeued bucket work"
                                );
                                break;
                            }

                            record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                            error!(
                                target: "rustfs::scanner::io",
                                event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_IO,
                                bucket = %bucket.name,
                                cache_name = %cache_name,
                                state = e.state(),
                                error = %e,
                                "Scanner bucket cache lock acquisition failed"
                            );
                            continue;
                        }
                    };

                    let mut cache = DataUsageCache::default();
                    let mut revisions = match cache.load_with_revisions(store_clone_clone.clone(), &cache_name).await {
                        Ok(revisions) => revisions,
                        Err(e) => {
                            record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                            error!(
                                target: "rustfs::scanner::io",
                                event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_IO,
                                bucket = %bucket.name,
                                cache_name = %cache_name,
                                state = "load_or_revision_lookup_failed",
                                error = %e,
                                "Scanner bucket cache load or revision lookup failed"
                            );
                            continue;
                        }
                    };
                    // Lack of an authoritative legacy identity disables the new
                    // checkpoint protocol; the existing full rebuild remains available.
                    let checkpoint_identity = match scanner_bucket_checkpoint_identity(
                        &store_clone_clone,
                        &bucket.name,
                        expected_publication_epoch_clone,
                        tier_registry_generation,
                        scan_mode,
                    )
                    .await
                    {
                        Ok(identity) => identity,
                        Err(error) => {
                            record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                            warn!(
                                target: "rustfs::scanner::io",
                                event = EVENT_SCANNER_DISK_BUCKET_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_IO,
                                state = "checkpoint_identity_unavailable",
                                stage = "admission",
                                bucket = %bucket.name,
                                pool = source.pool_index,
                                set = source.set_index,
                                cycle = want_cycle,
                                leader_epoch,
                                error = %error,
                                "Scanner retained the bucket checkpoint after an identity lookup failure"
                            );
                            continue;
                        }
                    };
                    let scan_state = current_cache_root_or_prepare_with_generation(
                        &mut cache,
                        &bucket.name,
                        source,
                        want_cycle,
                        leader_epoch,
                        bucket_scan_plan_digest,
                        DataUsageCacheReuseOptions {
                            require_source: require_cache_source,
                            tier_registry_generation: Some(tier_registry_generation),
                            checkpoint_identity,
                            checkpoint_plan_digest: Some(bucket_checkpoint_plan_digest),
                        },
                    );
                    let outcome = match scan_state {
                        DataUsageCacheScanState::Current(root) => {
                            if cache_guard.is_lock_lost() {
                                record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                                error!(
                                    target: "rustfs::scanner::io",
                                    event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                                    component = LOG_COMPONENT_SCANNER,
                                    subsystem = LOG_SUBSYSTEM_IO,
                                    bucket = %bucket.name,
                                    cache_name = %cache_name,
                                    state = "lock_lost_before_reuse",
                                    "Current scanner bucket cache root publish skipped after lock loss"
                                );
                                continue;
                            }
                            if scanner_publication_admission_for_epoch(store_clone_clone.clone(), expected_publication_epoch)
                                .await
                                .is_none()
                            {
                                record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                                error!(
                                    target: "rustfs::scanner::io",
                                    event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                                    component = LOG_COMPONENT_SCANNER,
                                    subsystem = LOG_SUBSYSTEM_IO,
                                    bucket = %bucket.name,
                                    cache_name = %cache_name,
                                    state = "publication_epoch_changed_before_reuse",
                                    "Current scanner bucket cache root publish skipped after movement epoch change"
                                );
                                continue;
                            }
                            if let Err(e) =
                                send_cache_root_entry(&bucket_result_tx_clone, *root, &cache, &pending_maintenance_work_clone)
                                    .await
                            {
                                record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                                error!(
                                    target: "rustfs::scanner::io",
                                    event = EVENT_SCANNER_DATA_USAGE_STREAM,
                                    component = LOG_COMPONENT_SCANNER,
                                    subsystem = LOG_SUBSYSTEM_IO,
                                    bucket = %bucket.name,
                                    state = "send_current_root_failed",
                                    error = %e,
                                    "Current scanner bucket cache root entry publish failed"
                                );
                            }
                            continue;
                        }
                        DataUsageCacheScanState::Prepared {
                            outcome,
                            invalid_current,
                        } => {
                            if let Some(e) = invalid_current {
                                warn!(
                                    target: "rustfs::scanner::io",
                                    event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                                    component = LOG_COMPONENT_SCANNER,
                                    subsystem = LOG_SUBSYSTEM_IO,
                                    bucket = %bucket.name,
                                    cache_name = %cache_name,
                                    state = "current_cache_invalid",
                                    error = %e,
                                    "Current scanner bucket cache is invalid; rebuilding"
                                );
                            }
                            outcome
                        }
                    };

                    match outcome {
                        DataUsageCachePrepareOutcome::RejectedNewerCycle => {
                            cache_cycle_floor_clone.fetch_max(cache.info.next_cycle, Ordering::AcqRel);
                            record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                            error!(
                                target: "rustfs::scanner::io",
                                event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_IO,
                                bucket = %bucket.name,
                                cache_name = %cache_name,
                                requested_cycle = want_cycle,
                                cached_cycle = cache.info.next_cycle,
                                state = "stale_cycle_rejected",
                                "Scanner rejected a bucket cache cycle regression"
                            );
                            continue;
                        }
                        DataUsageCachePrepareOutcome::RejectedNewerLeader => {
                            record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                            error!(
                                target: "rustfs::scanner::io",
                                event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_IO,
                                bucket = %bucket.name,
                                cache_name = %cache_name,
                                requested_epoch = leader_epoch,
                                cached_epoch = cache.info.leader_epoch,
                                state = "stale_leader_rejected",
                                "Scanner rejected bucket work from an older leader epoch"
                            );
                            continue;
                        }
                        DataUsageCachePrepareOutcome::Reused | DataUsageCachePrepareOutcome::Reset => {}
                    }
                    cache.info.skip_healing = healing;

                    debug!(
                        target: "rustfs::scanner::io",
                        event = EVENT_SCANNER_DISK_BUCKET_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_IO,
                        bucket = %bucket.name,
                        cache_name = ?cache.info.name,
                        state = "cache_ready",
                        "Scanner disk bucket cache ready"
                    );

                    let (checkpoint_tx, mut checkpoint_rx) = mpsc::channel::<DataUsageCache>(1);
                    let (node_tx, mut node_rx) = crate::workload_admission::scanner_node_request_channel();

                    let scan_ctx = ctx_clone.child_token();
                    #[cfg(feature = "e2e-test-hooks")]
                    {
                        let disk_uuid = if crate::e2e_barrier::worker_observation::enabled() {
                            crate::storage_api::owner::EcstoreDiskAPI::get_disk_id(disk.as_ref()).await.ok().flatten()
                        } else { None };
                        let location = disk.get_disk_location();
                        let _ = crate::e2e_barrier::checkpoint("bucket_walk", serde_json::json!({
                            "epoch": leader_epoch, "cycle": want_cycle,
                            "pool": source.pool_index, "set": source.set_index,
                            "bucket": bucket.name, "drive": disk.path().display().to_string(),
                            "worker_kind": "local", "worker_path": disk.path().display().to_string(),
                            "worker_endpoint": crate::storage_api::owner::EcstoreDiskAPI::endpoint(disk.as_ref()).grid_host(),
                            "disk_uuid": disk_uuid,
                            "disk_index": location.disk_idx,
                            "bucket_incarnation": worker_incarnations.as_ref().and_then(|values| values.get(&bucket.name)),
                        })).await;
                    }
                    let scan = crate::workload_admission::with_scanner_node_requests(node_tx, disk.clone().nsscanner_disk(
                        scan_ctx.clone(),
                        budget_clone.clone(),
                        set_disk_inventory_clone.as_ref().clone(),
                        cache,
                        None,
                        ScannerDiskScanOptions {
                            scan_mode,
                            prefix_scan_scope,
                            checkpoint_tx: Some(checkpoint_tx),
                        },
                    ));
                    tokio::pin!(scan);
                    let mut lock_watch = tokio::time::interval(SCANNER_CACHE_LOCK_POLL_INTERVAL);
                    lock_watch.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    let mut checkpoint_channel_closed = false;
                    let scan_result = loop {
                        tokio::select! {
                            result = &mut scan => break result,
                            request = async {
                                match node_rx.as_mut() {
                                    Some(receiver) => receiver.recv().await,
                                    None => std::future::pending().await,
                                }
                            } => {
                                let Some(request) = request else {
                                    node_rx = None;
                                    continue;
                                };
                                if let Err(error) = cache_guard.resume_node_operation(
                                    store_clone_clone.clone(), &cache_name, source, &revisions, request,
                                ).await {
                                    scan_ctx.cancel();
                                    await_scanner_disk_shutdown(scan.as_mut()).await;
                                    break Err(error);
                                }
                            }
                            _ = lock_watch.tick() => {
                                if cache_guard.is_lock_lost() {
                                    scan_ctx.cancel();
                                    await_scanner_disk_shutdown(scan.as_mut()).await;
                                    break Err(Error::other("scanner bucket cache lock was lost during bucket scan"));
                                }
                            }
                            checkpoint = checkpoint_rx.recv(), if !checkpoint_channel_closed => {
                                let Some(checkpoint) = checkpoint else {
                                    checkpoint_channel_closed = true;
                                    continue;
                                };
                                if cache_guard.is_lock_lost() {
                                    scan_ctx.cancel();
                                    await_scanner_disk_shutdown(scan.as_mut()).await;
                                    break Err(Error::other("scanner bucket cache lock was lost before checkpoint save"));
                                }
                                if ctx_clone.is_cancelled() {
                                    scan_ctx.cancel();
                                    await_scanner_disk_shutdown(scan.as_mut()).await;
                                    break Err(Error::other("scanner leader fence changed before checkpoint save"));
                                }
                                let Some(fence_store) = crate::resolve_scanner_object_store_handle() else {
                                    scan_ctx.cancel();
                                    await_scanner_disk_shutdown(scan.as_mut()).await;
                                    break Err(Error::other("scanner global cycle fence store is unavailable before checkpoint save"));
                                };
                                match persist_scanner_checkpoint(
                                    store_clone_clone.clone(),
                                    fence_store,
                                    ScannerCheckpointPersistContext {
                                        ctx: &ctx_clone,
                                        expected_publication_epoch: expected_publication_epoch_clone,
                                        cycle: want_cycle,
                                        leader_epoch,
                                    },
                                    cache_name.as_str(),
                                    &checkpoint,
                                    &mut revisions,
                                )
                                .await
                                {
                                    ScannerCheckpointPersistResult::Saved | ScannerCheckpointPersistResult::Skipped => {
                                        if cache_guard.is_lock_lost() || ctx_clone.is_cancelled() {
                                            scan_ctx.cancel();
                                            await_scanner_disk_shutdown(scan.as_mut()).await;
                                            break Err(Error::other("scanner bucket cache fence changed after checkpoint save"));
                                        }
                                    }
                                    ScannerCheckpointPersistResult::FenceChanged => {
                                        scan_ctx.cancel();
                                        await_scanner_disk_shutdown(scan.as_mut()).await;
                                        break Err(Error::other("scanner bucket cache fence changed during checkpoint save"));
                                    }
                                    ScannerCheckpointPersistResult::RetryBucket(error) => {
                                        scan_ctx.cancel();
                                        await_scanner_disk_shutdown(scan.as_mut()).await;
                                        break Err(error);
                                    }
                                }
                            }
                        }
                    };
                    if let Some(expected) = checkpoint_identity {
                        let current_identity = match scanner_bucket_checkpoint_identity(
                            &store_clone_clone,
                            &bucket.name,
                            expected_publication_epoch_clone,
                            tier_registry_generation,
                            scan_mode,
                        )
                        .await
                        {
                            Ok(identity) => identity,
                            Err(error) => {
                                record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                                warn!(
                                    target: "rustfs::scanner::io",
                                    event = EVENT_SCANNER_DISK_BUCKET_STATE,
                                    component = LOG_COMPONENT_SCANNER,
                                    subsystem = LOG_SUBSYSTEM_IO,
                                    state = "checkpoint_identity_unavailable",
                                    stage = "post_scan",
                                    bucket = %bucket.name,
                                    pool = source.pool_index,
                                    set = source.set_index,
                                    cycle = want_cycle,
                                    leader_epoch,
                                    error = %error,
                                    "Scanner could not verify the bucket identity before persistence"
                                );
                                continue;
                            }
                        };
                        if current_identity != Some(expected) {
                            record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                            warn!(
                                target: "rustfs::scanner::io",
                                event = EVENT_SCANNER_DISK_BUCKET_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_IO,
                                state = "checkpoint_identity_changed",
                                stage = "post_scan",
                                bucket = %bucket.name,
                                pool = source.pool_index,
                                set = source.set_index,
                                cycle = want_cycle,
                                leader_epoch,
                                "Scanner rejected the bucket result after an identity change"
                            );
                            continue;
                        }
                    }
                    let scan_outcome = match scan_result {
                        Ok(scan_outcome) => scan_outcome,
                        Err(e) => {
                            record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                            if ctx_clone.is_cancelled() {
                                debug!(
                                    target: "rustfs::scanner::io",
                                    event = EVENT_SCANNER_DISK_BUCKET_STATE,
                                    component = LOG_COMPONENT_SCANNER,
                                    subsystem = LOG_SUBSYSTEM_IO,
                                    bucket = %bucket.name,
                                    state = "cancelled",
                                    error = %e,
                                    "Scanner disk bucket scan cancelled"
                                );
                            } else {
                                error!(
                                    target: "rustfs::scanner::io",
                                    event = EVENT_SCANNER_DISK_BUCKET_STATE,
                                    component = LOG_COMPONENT_SCANNER,
                                    subsystem = LOG_SUBSYSTEM_IO,
                                    bucket = %bucket.name,
                                    state = "scan_failed",
                                    error = %e,
                                    "Scanner disk bucket scan failed"
                                );
                            }

                            continue;
                        }
                    };

                    let (cache, partial) = match scan_outcome {
                        ScannerDiskScanOutcome::Complete(mut completed_cache) => {
                            // The sweep finished under its coverage plan; the
                            // complete cache is current under the reuse plan,
                            // which a restart or a maintenance publication
                            // changes so the bucket is walked again next time.
                            if completed_cache.info.snapshot_complete {
                                completed_cache.info.scan_plan_digest = Some(bucket_scan_plan_digest);
                            }
                            (completed_cache, false)
                        }
                        ScannerDiskScanOutcome::Partial(partial_cache) => (partial_cache, true),
                        ScannerDiskScanOutcome::NamespaceNotFound(_) => {
                            if requeue_bucket_work(&bucket_tx_clone, &bucket, &mut work_guard).await {
                                increment_disk_bucket_scans_queued(
                                    &queued_disk_bucket_scans_clone,
                                    &pool_label_clone,
                                    &set_label_clone,
                                );
                            } else {
                                record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                            }
                            debug!(
                                target: "rustfs::scanner::io",
                                event = EVENT_SCANNER_DISK_BUCKET_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_IO,
                                bucket = %bucket.name,
                                state = "local_namespace_missing_requeued",
                                "Scanner requeued a bucket missing from this local disk"
                            );
                            break;
                        }
                    };
                    if partial {
                        record_partial_dirty_bucket(&partial_dirty_buckets_clone, &bucket.name).await;
                        let done_save = Metrics::time(Metric::SaveUsage);
                        let partial_saved = if cache_guard.is_lock_lost() {
                            false
                        } else {
                            match cache
                                .save_with_revisions_for_epoch(
                                    store_clone_clone.clone(),
                                    cache_name.as_str(),
                                    &revisions,
                                    expected_publication_epoch_clone,
                                )
                                .await
                            {
                                Ok(()) => true,
                                Err(e) => {
                                    error!(
                                        target: "rustfs::scanner::io",
                                        event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                                        component = LOG_COMPONENT_SCANNER,
                                        subsystem = LOG_SUBSYSTEM_IO,
                                        bucket = %bucket.name,
                                        cache_name = %cache_name,
                                        state = "partial_save_failed",
                                        error = %e,
                                        "Scanner partial bucket cache save failed"
                                    );
                                    false
                                }
                            }
                        };
                        done_save();
                        if !partial_saved {
                            record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                        }
                        if partial_saved {
                            log_saved_partial_bucket_cache(&cache, &cache_name);
                        }

                        continue;
                    }
                    debug!(
                        target: "rustfs::scanner::io",
                        event = EVENT_SCANNER_DISK_BUCKET_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_IO,
                        bucket = %bucket.name,
                        cache_name = %cache.info.name,
                        state = "scan_completed",
                        "Scanner disk bucket scan completed"
                    );

                    if ctx_clone.is_cancelled() {
                        break;
                    }

                    if cache_guard.is_lock_lost() {
                        record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                        error!(
                            target: "rustfs::scanner::io",
                            event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_IO,
                            bucket = %bucket.name,
                            cache_name = %cache_name,
                            state = "lock_lost",
                            "Scanner bucket cache save skipped after lock loss"
                        );
                        continue;
                    }

                    let done_save = Metrics::time(Metric::SaveUsage);
                    if let Err(e) = cache
                        .save_with_revisions_for_epoch(
                            store_clone_clone.clone(),
                            &cache_name,
                            &revisions,
                            expected_publication_epoch_clone,
                        )
                        .await
                    {
                        done_save();
                        record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                        error!(
                            target: "rustfs::scanner::io",
                            event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_IO,
                            bucket = %bucket.name,
                            cache_name = %cache_name,
                            state = "save_failed",
                            error = %e,
                            "Scanner bucket cache save failed"
                        );
                        continue;
                    }
                    done_save();

                    if cache_guard.is_lock_lost() {
                        record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                        error!(
                            target: "rustfs::scanner::io",
                            event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_IO,
                            bucket = %bucket.name,
                            cache_name = %cache_name,
                            state = "lock_lost_after_save",
                            "Scanner bucket cache root publish skipped after lock loss"
                        );
                        continue;
                    }

                    if scanner_publication_admission_for_epoch(store_clone_clone.clone(), expected_publication_epoch_clone)
                        .await
                        .is_none()
                    {
                        record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                        error!(
                            target: "rustfs::scanner::io",
                            event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_IO,
                            bucket = %bucket.name,
                            cache_name = %cache_name,
                            state = "publication_epoch_changed_after_save",
                            "Scanner bucket cache root publish skipped after movement epoch change"
                        );
                        continue;
                    }

                    debug!(
                        target: "rustfs::scanner::io",
                        event = EVENT_SCANNER_DATA_USAGE_STREAM,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_IO,
                        bucket = %bucket.name,
                        cache_name = %cache.info.name,
                        state = "send_root_entry",
                        "Scanner root entry publish started"
                    );

                    if let Err(e) =
                        send_cache_root_entry_info(&bucket_result_tx_clone, &cache, &pending_maintenance_work_clone).await
                    {
                        record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                        error!(
                            target: "rustfs::scanner::io",
                            event = EVENT_SCANNER_DATA_USAGE_STREAM,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_IO,
                            bucket = %bucket.name,
                            state = "send_root_failed",
                            error = %e,
                            "Scanner root entry publish failed"
                        );
                    }
                }
            })));
        }
        drop(bucket_tx);
        drop(bucket_result_tx);

        let mut first_join_err = None;
        for join_result in join_all(futs).await {
            if let Err(err) = join_result {
                error!(
                    target: "rustfs::scanner::io",
                    event = EVENT_SCANNER_DISK_BUCKET_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_IO,
                    pool = self.pool_index,
                    set = self.set_index,
                    state = "disk_bucket_task_join_failed",
                    error = %err,
                    "Scanner disk bucket task join failed"
                );
                record_set_scan_failure(&mut first_join_err, scanner_task_join_error("scanner disk bucket", err));
            }
        }
        let unprocessed_buckets = mark_unprocessed_bucket_work_failed(
            bucket_rx_mutex.as_ref(),
            &remaining_bucket_work,
            &bucket_work_complete,
            &bucket_failures.hard,
        )
        .await;
        if unprocessed_buckets > 0 {
            warn!(
                target: "rustfs::scanner::io",
                event = EVENT_SCANNER_SET_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_IO,
                pool = self.pool_index,
                set = self.set_index,
                unprocessed_buckets,
                state = "workers_exhausted",
                "Scanner marked queued bucket work failed after all disk workers exited"
            );
        }
        record_disk_scan_concurrency_limit(&pool_label, &set_label, 0);
        record_disk_bucket_scans_queued(0, &pool_label, &set_label);
        record_disk_bucket_scans_active(0, &pool_label, &set_label);

        let (mut cache, completed_buckets) = collect_bucket_results_fut
            .await
            .map_err(|err| scanner_task_join_error("scanner bucket result collector", err))?;

        if let Some(err) = first_join_err {
            return Err(err);
        }

        // Record only actual worker completions, before adding carried values.
        // The root's cold entries cannot certify a newly observed peer tuple.
        if let Some(completions) = bucket_completions {
            let completed_incarnations = completed_buckets
                .iter()
                .filter_map(|bucket| {
                    current_bucket_incarnations
                        .as_ref()?
                        .get(bucket)
                        .filter(|incarnation| !incarnation.is_nil())
                        .map(|incarnation| (bucket.clone(), *incarnation))
                })
                .collect();
            completions.lock().await.insert(source, completed_incarnations);
        }

        let carried_buckets = carry_partial_bucket_values(
            &mut cache,
            &old_cache,
            old_cache_certified,
            &buckets,
            &completed_buckets,
            &bucket_failures,
            current_bucket_incarnations.as_ref(),
        )
        .await;
        if !carried_buckets.is_empty() {
            debug!(
                target: "rustfs::scanner::io",
                event = EVENT_SCANNER_SET_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_IO,
                state = "partial_buckets_carried",
                pool = source.pool_index,
                set = source.set_index,
                cycle = want_cycle,
                carried_buckets = carried_buckets.len(),
                first_carried_bucket = carried_buckets.iter().min(),
                "Scanner set published the last certified value for buckets whose walk stopped early"
            );
            bucket_failures
                .partial_carried
                .lock()
                .await
                .extend(carried_buckets.iter().cloned());
        }
        let completed_count = completed_buckets.len();
        let covered_count = completed_count.saturating_add(carried_buckets.len());
        let coverage_complete =
            should_publish_completed_snapshot(covered_count, buckets.len(), budget.budget_elapsed(), ctx.is_cancelled());
        if coverage_complete {
            // A carried bucket was not walked to its end, so this set cannot
            // prove the producer window that prefix-level reuse would need.
            let completed_segment_invalidation_proof = if carried_buckets.is_empty() {
                scanner_completed_set_segment_invalidation_proof(&segment_invalidation_proof, cold_zero_walk_reuse_candidate)
            } else {
                None
            };
            cache.info.next_cycle = want_cycle;
            cache.info.last_update.get_or_insert_with(SystemTime::now);
            cache.info.snapshot_complete = true;
            let carried_execution = super::cache::scanner_carried_execution_digest(execution_digest, &carried_buckets);
            let replication_execution = super::cache::scanner_replication_execution_digest(carried_execution, &cache);
            cache.info.scan_execution_digest = Some(replication_execution);
            cache.info.segment_invalidation_proof = completed_segment_invalidation_proof;
            cache.info.lkg_snapshot_complete = false;
            cache.info.lkg_next_cycle = None;
            cache.info.lkg_last_update = None;
            cache.info.lkg_leader_epoch = None;
            cache.info.lkg_scan_plan_digest = None;
            if cold_zero_walk_reuse_candidate {
                cold_zero_walk_reuse_observed.store(true, Ordering::Release);
            }
            let _ = persist_and_publish_cache_snapshot(
                self.clone(),
                &updates,
                cache,
                initial_revisions.as_ref(),
                cache_cycle_floor.as_ref(),
                expected_publication_epoch,
            )
            .await;
        } else {
            let mut incomplete_scope = cache;
            incomplete_scope.info.name = DATA_USAGE_ROOT.to_string();
            incomplete_scope.info.next_cycle = want_cycle;
            incomplete_scope.info.last_update = None;
            incomplete_scope.info.leader_epoch = leader_epoch;
            incomplete_scope.info.tier_registry_generation = Some(tier_registry_generation);
            incomplete_scope.info.source = Some(source);
            incomplete_scope.info.snapshot_complete = false;
            incomplete_scope.info.segment_invalidation_proof = None;
            incomplete_scope.info.scan_plan_digest = Some(scan_plan_digest);
            incomplete_scope.info.cache_key_format = DATA_USAGE_CACHE_KEY_FORMAT;
            if let Err(e) = updates.send(incomplete_scope).await {
                error!(
                    target: "rustfs::scanner::io",
                    event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_IO,
                    pool = self.pool_index,
                    set = self.set_index,
                    state = "incomplete_scope_publish_failed",
                    error = %e,
                    "Scanner incomplete set scope publish failed"
                );
            }
            debug!(
                target: "rustfs::scanner::io",
                event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_IO,
                state = "set_cache_publish_skipped",
                pool = source.pool_index,
                set = source.set_index,
                cycle = want_cycle,
                leader_epoch,
                completed_buckets = completed_count,
                carried_buckets = carried_buckets.len(),
                total_buckets = buckets.len(),
                first_incomplete_bucket = buckets
                    .iter()
                    .map(|bucket| bucket.name.as_str())
                    .filter(|bucket| !completed_buckets.contains(*bucket) && !carried_buckets.iter().any(|carried| carried == bucket))
                    .min(),
                unprocessed_buckets,
                budget_elapsed = budget.budget_elapsed(),
                cancelled = ctx.is_cancelled(),
                "Scanner set cache publish skipped because cycle did not complete cleanly"
            );
        }

        debug!(
            target: "rustfs::scanner::io",
            event = EVENT_SCANNER_DISK_BUCKET_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_IO,
            state = "set_scan_completed",
            pool = source.pool_index,
            set = source.set_index,
            cycle = want_cycle,
            completed_buckets = completed_count,
            total_buckets = buckets.len(),
            coverage_complete,
            "Scanner set workers finished"
        );

        Ok(())
    }
}
