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
/// ScannerIO/ScannerIOCycle implementations for ECStore: bucket planning and per-set fan-out.
use super::*;

#[async_trait::async_trait]
impl ScannerIO for ECStore {
    async fn nsscanner(
        &self,
        ctx: CancellationToken,
        budget: Arc<ScannerCycleBudget>,
        updates: mpsc::Sender<DataUsageInfo>,
        want_cycle: u64,
        scan_mode: HealScanMode,
    ) -> Result<()> {
        // This public path can prove delivery to the receiver, but not that
        // the receiver persisted the update. Keep dirty usage pending unless
        // the main scanner confirms durability through nsscanner_with_status.
        let leader_epoch = crate::scanner::current_scanner_leader_epoch()
            .await
            .map_err(|err| StorageError::other(format!("failed to resolve scanner leader epoch: {err}")))?;
        ScannerIOCycle::nsscanner_with_status(self, ctx, budget, updates, want_cycle, leader_epoch, scan_mode).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl ScannerIOCycle for ECStore {
    #[tracing::instrument(skip(self, budget, updates))]
    async fn nsscanner_with_status(
        &self,
        ctx: CancellationToken,
        budget: Arc<ScannerCycleBudget>,
        updates: mpsc::Sender<DataUsageInfo>,
        want_cycle: u64,
        leader_epoch: u64,
        scan_mode: HealScanMode,
    ) -> Result<ScannerCycleResult> {
        let child_token = ctx.child_token();

        // Check the local pool metadata before listing buckets. A failed or
        // canceled decommission remains suspended after its worker exits, so
        // starting a scan in that state could build a snapshot that cannot be
        // routed to the authoritative metadata object.
        if self.scanner_data_usage_publication_blocked().await {
            debug!(
                target: "rustfs::scanner::io",
                event = EVENT_SCANNER_SET_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_IO,
                state = "cycle_data_usage_route_blocked",
                "Scanner cycle deferred while data usage metadata remains hidden by data movement"
            );
            return Ok(ScannerCycleResult::new(
                ScannerCycleStatus::Deferred(ScannerCycleDeferReason::DataMovement),
                None,
            ));
        }

        // Capture one storage-owned movement epoch for the entire cycle.  Set
        // workers must not each observe a fresh epoch: a movement transition
        // between sets would otherwise allow a mixed-generation aggregate.
        let publication_epoch = match self.scanner_data_usage_publication_admission().await {
            Some(admission) => Some(admission.epoch()),
            None => {
                return Ok(ScannerCycleResult::new(
                    ScannerCycleStatus::Deferred(ScannerCycleDeferReason::DataMovement),
                    None,
                ));
            }
        };

        let distributed = self.setup_is_dist_erasure().await;
        let activity_before = match scanner_activity_preflight(crate::scanner::probe_scanner_activity(self, distributed).await) {
            ScannerActivityPreflight::Ready(snapshot) => snapshot,
            ScannerActivityPreflight::ActivityBaselineUnavailable(err) => {
                warn!(
                    target: "rustfs::scanner::io",
                    event = EVENT_SCANNER_SET_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_IO,
                    state = "cycle_activity_baseline_failed",
                    error = %err,
                    "Scanner cycle skipped because cluster activity could not be baselined"
                );
                return Ok(ScannerCycleResult::new(
                    ScannerCycleStatus::Deferred(ScannerCycleDeferReason::ActivityBaselineUnavailable),
                    None,
                ));
            }
            ScannerActivityPreflight::DataMovement => {
                debug!(
                    target: "rustfs::scanner::io",
                    event = EVENT_SCANNER_SET_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_IO,
                    state = "cycle_data_movement_active",
                    "Scanner cycle deferred while rebalance or decommission data movement is active"
                );
                return Ok(ScannerCycleResult::new(
                    ScannerCycleStatus::Deferred(ScannerCycleDeferReason::DataMovement),
                    None,
                ));
            }
        };
        let dirty_generation_before_bucket_list = dirty_usage_generation();
        let bucket_listing = self.list_bucket_for_scanner(&BucketOptions::default()).await?;
        let mut bucket_plan_complete = bucket_listing.topology_complete;
        let all_buckets = Arc::new(bucket_listing.buckets);
        let expected_sources = Arc::new(
            self.pools
                .iter()
                .flat_map(|pool| {
                    pool.disk_set
                        .iter()
                        .map(|set| DataUsageCacheSource::new(set.pool_index, set.set_index))
                })
                .collect::<HashSet<_>>(),
        );
        let mut buckets_by_source = HashMap::with_capacity(bucket_listing.set_buckets.len());
        for scope in bucket_listing.set_buckets {
            let source = DataUsageCacheSource::new(scope.pool_index, scope.set_index);
            if buckets_by_source.insert(source, scope.buckets).is_some() {
                bucket_plan_complete = false;
            }
        }
        bucket_plan_complete &= buckets_by_source.keys().copied().collect::<HashSet<_>>() == *expected_sources;
        let scan_plan_digest =
            scanner_bucket_plan_digest(&all_buckets, crate::scanner::scanner_activity_snapshot_digest(&activity_before));
        let dirty_usage_snapshot = Arc::new(snapshot_dirty_usage_buckets(&all_buckets, dirty_generation_before_bucket_list));
        let cache_cycle_floor = Arc::new(AtomicU64::new(want_cycle));

        if all_buckets.is_empty() {
            reset_set_scan_gauges();
            if !bucket_plan_complete {
                return Ok(
                    ScannerCycleResult::new(ScannerCycleStatus::Incomplete, None).with_publication_epoch(publication_epoch)
                );
            }
            let activity_status = scanner_cycle_activity_status(self, distributed, &activity_before).await;
            let dirty_usage_status = dirty_usage_snapshot_status(&dirty_usage_snapshot);
            let status = classify_nsscanner_cycle(
                true,
                false,
                ctx.is_cancelled(),
                ScannerBucketScanStatus::Complete,
                dirty_usage_status,
                activity_status,
            );
            if !publish_usage_snapshot(
                &updates,
                status,
                DataUsageInfo {
                    last_update: Some(SystemTime::now()),
                    scanner_cycle: Some(want_cycle),
                    usage_snapshot_complete: true,
                    ..Default::default()
                },
            )
            .await?
            {
                return Ok(ScannerCycleResult::new(status, None).with_publication_epoch(publication_epoch));
            }
            let dirty_usage_clear =
                (status == ScannerCycleStatus::Complete).then(|| dirty_usage_snapshot.buckets.as_ref().clone());
            let remote_dirty_usage_acknowledgements = if status == ScannerCycleStatus::Complete {
                crate::scanner::scanner_dirty_usage_acknowledgements(&activity_before)
            } else {
                Vec::new()
            };
            return Ok(ScannerCycleResult::new(status, dirty_usage_clear)
                .with_publication_epoch(publication_epoch)
                .with_remote_dirty_usage_acknowledgements(remote_dirty_usage_acknowledgements));
        }

        let total_results = expected_sources.len();
        if total_results == 0 {
            warn!(
                target: "rustfs::scanner::io",
                event = EVENT_SCANNER_SET_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_IO,
                bucket_count = all_buckets.len(),
                state = "no_disk_sets",
                "Scanner set state update detected missing disk sets"
            );
            reset_set_scan_gauges();
            return Ok(ScannerCycleResult::new(ScannerCycleStatus::Incomplete, None).with_publication_epoch(publication_epoch));
        }

        let set_scan_limit = scanner_budgeted_concurrency_limit(
            scanner_max_concurrent_set_scans(total_results),
            budget.requires_serial_progress_accounting(),
        );
        let bucket_failures = ScannerBucketFailureState::default();
        let pending_maintenance_work = Arc::new(AtomicBool::new(false));
        record_set_scan_concurrency_limit(set_scan_limit);
        debug!(
            target: "rustfs::scanner::io",
            event = EVENT_SCANNER_SET_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_IO,
            total_sets = total_results,
            concurrency_limit = set_scan_limit,
            state = "concurrency_budget",
            "Scanner set concurrency budget resolved"
        );
        let set_scan_semaphore = Arc::new(Semaphore::new(set_scan_limit));
        let queued_set_scans = Arc::new(AtomicUsize::new(total_results));
        let active_set_scans = Arc::new(AtomicUsize::new(0));
        record_set_scans_queued(total_results);
        record_set_scans_active(0);

        let results = vec![DataUsageCache::default(); total_results];
        let results_mutex: Arc<Mutex<Vec<DataUsageCache>>> = Arc::new(Mutex::new(results));
        let first_err_mutex: Arc<Mutex<Option<Error>>> = Arc::new(Mutex::new(None));
        let mut results_index = 0usize;
        let mut wait_futs = Vec::new();

        for pool in self.pools.iter() {
            for set in pool.disk_set.iter() {
                let results_index_clone = results_index;
                results_index += 1;
                // Clone the Arc to move it into the spawned task
                let set_clone: Arc<SetDisks> = Arc::clone(set);
                let source = DataUsageCacheSource::new(set.pool_index, set.set_index);
                let set_buckets = buckets_by_source.remove(&source).unwrap_or_default();
                let pool_label = set.pool_index.to_string();
                let set_label = set.set_index.to_string();

                let child_token_clone = child_token.clone();
                let budget_clone = budget.clone();
                let want_cycle_clone = want_cycle;
                let scan_mode_clone = scan_mode;
                let results_mutex_clone = results_mutex.clone();
                let first_err_mutex_clone = first_err_mutex.clone();
                let set_scan_semaphore_clone = set_scan_semaphore.clone();
                let queued_set_scans_clone = queued_set_scans.clone();
                let active_set_scans_clone = active_set_scans.clone();

                let (tx, mut rx) = mpsc::channel::<DataUsageCache>(1);
                let failed_scope_tx = tx.clone();

                // Spawn task to receive and store results
                let receiver_fut = tokio::spawn(async move {
                    while let Some(result) = rx.recv().await {
                        let mut results = results_mutex_clone.lock().await;
                        results[results_index_clone] = result;
                    }
                });
                wait_futs.push(AbortOnDropHandle::new(receiver_fut));

                let scan_plan = ScannerBucketScanPlan {
                    buckets: set_buckets,
                    all_buckets: Arc::clone(&all_buckets),
                    digest: scan_plan_digest,
                    leader_epoch,
                    publication_epoch,
                    dirty_usage_buckets: dirty_usage_snapshot.buckets.clone(),
                    bucket_failures: bucket_failures.clone(),
                    pending_maintenance_work: pending_maintenance_work.clone(),
                    cache_cycle_floor: cache_cycle_floor.clone(),
                };
                // Spawn task to run the scanner
                let scanner_fut = tokio::spawn(async move {
                    let permit_wait = child_token_clone.clone();
                    let permit_wait_start = Instant::now();
                    let _permit = tokio::select! {
                        permit = set_scan_semaphore_clone.acquire_owned() => match permit {
                            Ok(permit) => permit,
                            Err(_) => return,
                        },
                        _ = permit_wait.cancelled() => return,
                    };
                    metrics::histogram!(
                        METRIC_SCANNER_SET_SCAN_WAIT_SECONDS,
                        "pool" => pool_label.clone(),
                        "set" => set_label.clone()
                    )
                    .record(permit_wait_start.elapsed().as_secs_f64());
                    let queued_count = decrement_atomic_usize(&queued_set_scans_clone);
                    record_set_scans_queued(queued_count);
                    let _active_guard = SetScanActiveGuard::new(active_set_scans_clone);

                    if let Err(e) = set_clone
                        .nsscanner_cache(
                            child_token_clone.clone(),
                            budget_clone,
                            scan_plan,
                            tx,
                            want_cycle_clone,
                            scan_mode_clone,
                        )
                        .await
                    {
                        if child_token_clone.is_cancelled() {
                            debug!(
                                pool = %pool_label,
                                set = %set_label,
                                error = %e,
                                "Scanner set scan stopped after cancellation"
                            );
                            return;
                        }

                        counter!(
                            "rustfs_scanner_set_failure_total",
                            "pool" => pool_label.clone(),
                            "set" => set_label.clone(),
                            "stage" => "nsscanner_cache".to_string()
                        )
                        .increment(1);
                        error!(
                            target: "rustfs::scanner::io",
                            event = EVENT_SCANNER_SET_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_IO,
                            pool = %pool_label,
                            set = %set_label,
                            error = %e,
                            state = "set_scan_failed",
                            "Scanner set scan failed; continuing cycle"
                        );
                        let _ = failed_scope_tx
                            .send(DataUsageCache {
                                info: DataUsageCacheInfo {
                                    name: DATA_USAGE_ROOT.to_string(),
                                    next_cycle: want_cycle_clone,
                                    leader_epoch,
                                    source: Some(source),
                                    snapshot_complete: false,
                                    scan_plan_digest: Some(scan_plan_digest),
                                    cache_key_format: DATA_USAGE_CACHE_KEY_FORMAT,
                                    ..Default::default()
                                },
                                cache: HashMap::new(),
                            })
                            .await;
                        let mut first_err = first_err_mutex_clone.lock().await;
                        record_set_scan_failure(&mut first_err, e);
                    }
                });
                wait_futs.push(AbortOnDropHandle::new(scanner_fut));
            }
        }

        for join_result in join_all(wait_futs).await {
            if let Err(err) = join_result {
                error!(
                    target: "rustfs::scanner::io",
                    event = EVENT_SCANNER_SET_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_IO,
                    state = "set_task_join_failed",
                    error = %err,
                    "Scanner set task join failed"
                );
                let mut first_err = first_err_mutex.lock().await;
                record_set_scan_failure(&mut first_err, scanner_task_join_error("scanner set", err));
            }
        }
        record_set_scan_concurrency_limit(0);
        record_set_scans_queued(0);
        record_set_scans_active(0);

        let first_err = first_err_mutex.lock().await.take();
        let results = results_mutex.lock().await.clone();
        let completed_all_sets = bucket_plan_complete && scanner_results_form_complete_snapshot(&results, &expected_sources);
        let result = finalize_nsscanner_result(&results, first_err);
        let failed_buckets = bucket_failures.hard.lock().await.clone();
        let partial_buckets = bucket_failures.partial.lock().await.clone();
        let namespace_not_found_buckets = bucket_failures.namespace_not_found.lock().await.clone();
        let scan_scope_matches = scanner_results_match_scan_scope(&results, &expected_sources);
        let bucket_scan_status = scanner_bucket_scan_status(
            !failed_buckets.is_empty(),
            scan_scope_matches && !partial_buckets.is_empty(),
            scan_scope_matches && !namespace_not_found_buckets.is_empty(),
        );
        let pending_maintenance_work = pending_maintenance_work_for_cycle(&pending_maintenance_work, &results);
        let observed_cycle_floor = cache_cycle_floor.load(Ordering::Acquire);
        let required_cycle_floor = (observed_cycle_floor > want_cycle).then_some(observed_cycle_floor);
        let budget_elapsed = budget.budget_elapsed();
        let dirty_usage_status = dirty_usage_snapshot_status(&dirty_usage_snapshot);
        let dirty_usage_current = dirty_usage_status == DirtyUsageSnapshotStatus::Current;
        let activity_status = scanner_cycle_activity_status(self, distributed, &activity_before).await;
        let all_bucket_names = all_buckets.iter().map(|bucket| bucket.name.clone()).collect::<Vec<_>>();
        let completed_usage = completed_data_usage_info(
            &results,
            &expected_sources,
            &all_bucket_names,
            bucket_plan_complete,
            budget_elapsed,
            ctx.is_cancelled(),
        );
        let observational_usage = completed_usage
            .is_none()
            .then(|| {
                observational_data_usage_info(
                    &results,
                    &expected_sources,
                    &all_bucket_names,
                    scan_plan_digest,
                    want_cycle,
                    leader_epoch,
                )
            })
            .flatten();
        let structurally_complete_snapshot = result.is_ok() && completed_all_sets && completed_usage.is_some();
        let cycle_status = classify_nsscanner_cycle(
            structurally_complete_snapshot,
            budget_elapsed,
            ctx.is_cancelled(),
            bucket_scan_status,
            dirty_usage_status,
            activity_status,
        );
        if let Some((data_usage_info, _)) = completed_usage {
            publish_usage_snapshot(&updates, cycle_status, data_usage_info).await?;
        } else if !ctx.is_cancelled()
            && let Some((data_usage_info, _)) = observational_usage
        {
            publish_observational_snapshot(&updates, data_usage_info).await?;
        }
        let dirty_usage_clear = should_clear_dirty_usage_snapshot(
            result.is_ok(),
            structurally_complete_snapshot,
            budget_elapsed,
            activity_status == ScannerCycleActivityStatus::Unchanged && dirty_usage_current,
            &dirty_usage_snapshot.buckets,
            &failed_buckets,
        );
        result?;
        let remote_dirty_usage_acknowledgements = if cycle_status == ScannerCycleStatus::Complete {
            crate::scanner::scanner_dirty_usage_acknowledgements(&activity_before)
        } else {
            Vec::new()
        };
        Ok(ScannerCycleResult::new(cycle_status, dirty_usage_clear)
            .with_publication_epoch(publication_epoch)
            .with_remote_dirty_usage_acknowledgements(remote_dirty_usage_acknowledgements)
            .with_failed_dirty_usage(!failed_buckets.is_empty())
            .with_pending_maintenance_work(pending_maintenance_work)
            .with_required_cycle_floor(required_cycle_floor))
    }
}
