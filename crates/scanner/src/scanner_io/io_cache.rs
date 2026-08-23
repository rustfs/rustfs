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
            buckets,
            all_buckets,
            digest: scan_plan_digest,
            leader_epoch,
            publication_epoch,
            dirty_usage_buckets,
            bucket_failures,
            pending_maintenance_work,
            cache_cycle_floor,
        } = scan_plan;
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
        if let Err(e) = old_cache.load(self.clone(), DATA_USAGE_CACHE_NAME).await {
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
        }
        if buckets.is_empty() {
            let now = SystemTime::now();
            let mut cache = DataUsageCache {
                info: DataUsageCacheInfo {
                    name: DATA_USAGE_ROOT.to_string(),
                    next_cycle: want_cycle,
                    last_update: Some(now),
                    leader_epoch,
                    source: Some(source),
                    snapshot_complete: true,
                    scan_plan_digest: Some(scan_plan_digest),
                    cache_key_format: DATA_USAGE_CACHE_KEY_FORMAT,
                    ..Default::default()
                },
                cache: HashMap::new(),
            };
            cache.replace(DATA_USAGE_ROOT, "", DataUsageEntry::default());
            for bucket in all_buckets.iter() {
                cache.replace(&bucket.name, DATA_USAGE_ROOT, DataUsageEntry::default());
            }
            reset_disk_bucket_scan_gauges(&pool_label, &set_label);
            return persist_and_publish_cache_snapshot(
                self,
                &updates,
                cache,
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
        let disk_scan_limit = scanner_budgeted_concurrency_limit(
            scanner_max_concurrent_disk_scans(workers.len()),
            budget.requires_serial_progress_accounting(),
        );
        record_disk_scan_concurrency_limit(&pool_label, &set_label, disk_scan_limit);
        debug!(
            target: "rustfs::scanner::io",
            event = EVENT_SCANNER_SET_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_IO,
            pool = self.pool_index,
            set = self.set_index,
            online_disks = workers.len(),
            concurrency_limit = disk_scan_limit,
            state = "disk_concurrency_budget",
            "Scanner disk concurrency budget resolved"
        );
        let disk_scan_semaphore = Arc::new(Semaphore::new(disk_scan_limit));
        let queued_disk_bucket_scans = Arc::new(AtomicUsize::new(buckets.len()));
        let active_disk_bucket_scans = Arc::new(AtomicUsize::new(0));
        record_disk_bucket_scans_queued(buckets.len(), &pool_label, &set_label);
        record_disk_bucket_scans_active(0, &pool_label, &set_label);
        let _reset_disk_bucket_scan_gauges = DiskBucketScanGaugeReset::new(pool_label.clone(), set_label.clone());

        let old_lkg = old_cache.info.snapshot_complete.then(|| {
            (
                old_cache.info.next_cycle,
                old_cache.info.last_update,
                old_cache.info.leader_epoch,
                old_cache.info.scan_plan_digest,
            )
        });
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
                source: Some(source),
                snapshot_complete: false,
                scan_plan_digest: Some(scan_plan_digest),
                cache_key_format: DATA_USAGE_CACHE_KEY_FORMAT,
                ..Default::default()
            },
            cache: HashMap::new(),
        };
        cache.replace(DATA_USAGE_ROOT, "", DataUsageEntry::default());
        for bucket in all_buckets.iter() {
            cache.replace(&bucket.name, DATA_USAGE_ROOT, DataUsageEntry::default());
        }

        let (bucket_tx, bucket_rx) = mpsc::channel::<BucketInfo>(buckets.len());

        let mut permutes = buckets.clone();
        permutes.shuffle(&mut rand::rng());
        let scan_order = bucket_usage_scan_order(&permutes, &old_cache, &dirty_usage_buckets);

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

        let cache_mutex: Arc<Mutex<DataUsageCache>> = Arc::new(Mutex::new(cache));

        let (bucket_result_tx, mut bucket_result_rx) = mpsc::channel::<DataUsageEntryInfo>(workers.len());

        let cache_mutex_clone = cache_mutex.clone();
        let ctx_clone = ctx.clone();
        let completed_bucket_count = Arc::new(AtomicUsize::new(0));
        let completed_bucket_count_clone = completed_bucket_count.clone();
        let collect_bucket_results_fut = AbortOnDropHandle::new(tokio::spawn(async move {
            let mut cancelled = false;

            loop {
                tokio::select! {
                    _ = ctx_clone.cancelled(), if !cancelled => {
                        cancelled = true;
                    }
                    result = bucket_result_rx.recv() => {
                        let Some(result) = result else {
                            return;
                        };

                        let mut cache = cache_mutex_clone.lock().await;
                        apply_bucket_result_to_cache(&mut cache, result, SystemTime::now());
                        completed_bucket_count_clone.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));

        let mut futs = Vec::new();

        let bucket_rx_mutex: Arc<Mutex<mpsc::Receiver<BucketInfo>>> = Arc::new(Mutex::new(bucket_rx));
        let remaining_bucket_work = Arc::new(AtomicUsize::new(buckets.len()));
        let bucket_work_complete = CancellationToken::new();
        for (disk, worker_mode) in workers {
            let bucket_rx_mutex_clone = bucket_rx_mutex.clone();
            let bucket_tx_clone = bucket_tx.clone();
            let remaining_bucket_work_clone = remaining_bucket_work.clone();
            let bucket_work_complete_clone = bucket_work_complete.clone();
            let ctx_clone = ctx.clone();
            let budget_clone = budget.clone();
            let store_clone_clone = self.clone();
            let bucket_result_tx_clone = bucket_result_tx.clone();
            let disk_clone = disk.clone();
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
            let cache_cycle_floor_clone = cache_cycle_floor.clone();
            let expected_publication_epoch_clone = expected_publication_epoch;
            let remote_server_epoch = match worker_mode {
                NamespaceScannerWorkerMode::RemoteV4(server_epoch) => Some(server_epoch),
                NamespaceScannerWorkerMode::Coordinator => None,
            };
            futs.push(AbortOnDropHandle::new(tokio::spawn(async move {
                let remote_session_id = uuid::Uuid::new_v4();
                let mut remote_session_sequence = 0_u64;
                loop {
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

                    let permit_wait = ctx_clone.clone();
                    let permit_wait_start = Instant::now();
                    let _permit = tokio::select! {
                        permit = disk_scan_semaphore_clone.clone().acquire_owned() => match permit {
                            Ok(permit) => permit,
                            Err(_) => {
                                decrement_disk_bucket_scans_queued(
                                    &queued_disk_bucket_scans_clone,
                                    &pool_label_clone,
                                    &set_label_clone,
                                );
                                break;
                            },
                        },
                        _ = permit_wait.cancelled() => {
                            decrement_disk_bucket_scans_queued(
                                &queued_disk_bucket_scans_clone,
                                &pool_label_clone,
                                &set_label_clone,
                            );
                            break;
                        },
                    };
                    metrics::histogram!(
                        METRIC_SCANNER_DISK_SCAN_WAIT_SECONDS,
                        "pool" => pool_label_clone.clone(),
                        "set" => set_label_clone.clone()
                    )
                    .record(permit_wait_start.elapsed().as_secs_f64());
                    decrement_disk_bucket_scans_queued(&queued_disk_bucket_scans_clone, &pool_label_clone, &set_label_clone);
                    let _active_guard = DiskBucketScanActiveGuard::new(
                        active_disk_bucket_scans_clone.clone(),
                        pool_label_clone.clone(),
                        set_label_clone.clone(),
                    );

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
                    let bucket_scan_plan_digest =
                        scanner_bucket_cache_digest(scan_plan_digest, dirty_usage_buckets_clone.get(&bucket.name).copied());

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
                        let remote_outcome = crate::remote_scanner::scan_remote_bucket(
                            &disk_clone,
                            ctx_clone.clone(),
                            budget_clone.clone(),
                            crate::remote_scanner::RemoteScannerScanSpec {
                                bucket: &bucket.name,
                                next_cycle: want_cycle,
                                leader_epoch,
                                server_epoch,
                                session_id: remote_session_id,
                                session_sequence: request_sequence,
                                scan_plan_digest: bucket_scan_plan_digest,
                                skip_healing: healing,
                                scan_mode,
                            },
                        )
                        .await;
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
                                        bucket = %bucket.name,
                                        state = "remote_zero_progress_requeued",
                                        error = %e,
                                        "Remote scanner requeued bucket rejected before execution"
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

                    let _local_admission = if disk_clone.is_local() {
                        match crate::remote_scanner::try_admit_remote_scanner(&disk_clone) {
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

                    // Lock order: scanner leader fence -> set-scoped per-bucket cache lock ->
                    // cache object read/write.
                    let cache_guard = match acquire_scanner_cache_locks(store_clone_clone.as_ref(), &cache_name, source).await {
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
                    let revisions = match cache.load_with_revisions(store_clone_clone.clone(), &cache_name).await {
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
                    let scan_state = current_cache_root_or_prepare(
                        &mut cache,
                        &bucket.name,
                        source,
                        want_cycle,
                        leader_epoch,
                        bucket_scan_plan_digest,
                        require_cache_source,
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

                    let before = cache.info.last_update;

                    let scan_ctx = ctx_clone.child_token();
                    let scan = disk_clone.clone().nsscanner_disk(
                        scan_ctx.clone(),
                        budget_clone.clone(),
                        set_disk_inventory_clone.as_ref().clone(),
                        cache.clone(),
                        None,
                        scan_mode,
                    );
                    tokio::pin!(scan);
                    let mut lock_watch = tokio::time::interval(SCANNER_CACHE_LOCK_POLL_INTERVAL);
                    lock_watch.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    let scan_result = loop {
                        tokio::select! {
                            result = &mut scan => break result,
                            _ = lock_watch.tick() => {
                                if cache_guard.is_lock_lost() {
                                    scan_ctx.cancel();
                                    await_scanner_disk_shutdown(scan.as_mut()).await;
                                    break Err(Error::other("scanner bucket cache lock was lost during bucket scan"));
                                }
                            }
                        }
                    };
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

                            if !cache_guard.is_lock_lost()
                                && let (Some(last_update), Some(before_update)) = (cache.info.last_update, before)
                                && last_update > before_update
                            {
                                let done_save = Metrics::time(Metric::SaveUsage);
                                if let Err(e) = cache
                                    .save_with_revisions_for_epoch(
                                        store_clone_clone.clone(),
                                        cache_name.as_str(),
                                        &revisions,
                                        expected_publication_epoch_clone,
                                    )
                                    .await
                                {
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
                                }
                                done_save();
                            }

                            continue;
                        }
                    };

                    let partial = match scan_outcome {
                        ScannerDiskScanOutcome::Complete(completed_cache) => {
                            cache = completed_cache;
                            None
                        }
                        ScannerDiskScanOutcome::Partial(partial_cache) => Some((partial_cache, &partial_dirty_buckets_clone)),
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
                    if let Some((partial_cache, failure_buckets)) = partial {
                        record_partial_dirty_bucket(failure_buckets, &bucket.name).await;
                        let done_save = Metrics::time(Metric::SaveUsage);
                        let partial_saved = if cache_guard.is_lock_lost() {
                            false
                        } else {
                            match partial_cache
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
                            debug!(
                                target: "rustfs::scanner::io",
                                event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_IO,
                                bucket = %bucket.name,
                                cache_name = %cache_name,
                                state = "partial_saved_not_published",
                                "Scanner partial bucket cache saved without publishing usage aggregate"
                            );
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

        if let Err(err) = collect_bucket_results_fut.await {
            return Err(scanner_task_join_error("scanner bucket result collector", err));
        }

        if let Some(err) = first_join_err {
            return Err(err);
        }

        let completed_count = completed_bucket_count.load(Ordering::Relaxed);
        if should_publish_completed_snapshot(completed_count, buckets.len(), budget.budget_elapsed(), ctx.is_cancelled()) {
            let cache_snapshot = {
                let mut cache = cache_mutex.lock().await;
                cache.info.next_cycle = want_cycle;
                cache.info.last_update.get_or_insert_with(SystemTime::now);
                cache.info.snapshot_complete = true;
                cache.info.lkg_snapshot_complete = false;
                cache.info.lkg_next_cycle = None;
                cache.info.lkg_last_update = None;
                cache.info.lkg_leader_epoch = None;
                cache.info.lkg_scan_plan_digest = None;
                cache.clone()
            };
            let _ = persist_and_publish_cache_snapshot(
                self.clone(),
                &updates,
                cache_snapshot,
                cache_cycle_floor.as_ref(),
                expected_publication_epoch,
            )
            .await;
        } else {
            let mut incomplete_scope = cache_mutex.lock().await.clone();
            incomplete_scope.info.name = DATA_USAGE_ROOT.to_string();
            incomplete_scope.info.next_cycle = want_cycle;
            incomplete_scope.info.last_update = None;
            incomplete_scope.info.leader_epoch = leader_epoch;
            incomplete_scope.info.source = Some(source);
            incomplete_scope.info.snapshot_complete = false;
            incomplete_scope.info.scan_plan_digest = Some(scan_plan_digest);
            incomplete_scope.info.cache_key_format = DATA_USAGE_CACHE_KEY_FORMAT;
            incomplete_scope.info.lkg_snapshot_complete = old_cache.info.lkg_snapshot_complete;
            incomplete_scope.info.lkg_next_cycle = old_cache.info.lkg_next_cycle;
            incomplete_scope.info.lkg_last_update = old_cache.info.lkg_last_update;
            incomplete_scope.info.lkg_leader_epoch = old_cache.info.lkg_leader_epoch;
            incomplete_scope.info.lkg_scan_plan_digest = old_cache.info.lkg_scan_plan_digest;
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
                completed_buckets = completed_count,
                total_buckets = buckets.len(),
                budget_elapsed = budget.budget_elapsed(),
                cancelled = ctx.is_cancelled(),
                state = "set_cache_publish_skipped",
                "Scanner set cache publish skipped because cycle did not complete cleanly"
            );
        }

        debug!(
            target: "rustfs::scanner::io",
            event = EVENT_SCANNER_DISK_BUCKET_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_IO,
            state = "set_scan_completed",
            "Scanner set scan completed"
        );

        Ok(())
    }
}
