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
/// ScannerIODisk implementation for Disk: get_size and the per-disk bucket scan.
use super::*;
use crate::UNKNOWN_TIER;

///
/// Seed [`SizeSummary::tier_stats`] from the cached tier-name list.
///
/// Preserves the original no-tier shape: with no tiers configured the map
/// stays completely empty (STANDARD/RRS/UNKNOWN are not seeded either).
/// Otherwise the standard storage classes and one fixed unknown bucket are
/// seeded alongside every configured tier so per-object accounting never
/// inserts an untrusted metadata key.
pub(super) fn tier_stats_template(tier_names: &[String]) -> HashMap<String, TierStats> {
    let mut tier_stats = HashMap::with_capacity(tier_names.len() + 3);
    for tier_name in tier_names {
        if tier_name != UNKNOWN_TIER {
            tier_stats.insert(tier_name.clone(), TierStats::default());
        }
    }
    if !tier_stats.is_empty() {
        tier_stats.insert(storageclass::STANDARD.to_string(), TierStats::default());
        tier_stats.insert(storageclass::RRS.to_string(), TierStats::default());
        tier_stats.insert(UNKNOWN_TIER.to_string(), TierStats::default());
    }
    tier_stats
}

#[async_trait::async_trait]
impl ScannerIODisk for Disk {
    async fn get_size(&self, item: ScannerItem) -> Result<SizeSummary> {
        self.get_size_with_tier_names(item, &runtime_tier_names().await).await
    }

    async fn get_size_with_tier_names(&self, mut item: ScannerItem, tier_names: &[String]) -> Result<SizeSummary> {
        let done_object = Metrics::time(Metric::ScanObject);

        if !is_xl_meta_path(&item.path) {
            return Err(StorageError::other(SCANNER_SKIP_FILE_ERROR.to_string()));
        }

        let metadata_object_path = item.object_path();
        let data = match self.read_metadata(&item.bucket, &metadata_object_path).await {
            Ok(data) => data,
            Err(e) if DiskError::is_err_object_not_found(&e) || DiskError::is_err_version_not_found(&e) => {
                return Err(StorageError::other(SCANNER_SKIP_FILE_ERROR.to_string()));
            }
            Err(e) => {
                return Err(scanner_metadata_transient_error(
                    format!("failed to read metadata: {e}"),
                    &item.bucket,
                    &metadata_object_path,
                ));
            }
        };

        item.transform_meta_dir();
        let object_path = item.object_path();

        let meta = FileMeta::load(&data)
            .map_err(|e| scanner_metadata_corrupt_error(format!("failed to load metadata: {e}"), &item.bucket, &object_path))?;
        let fivs = match meta.get_file_info_versions(item.bucket.as_str(), object_path.as_str(), false) {
            Ok(versions) => versions,
            Err(e) => {
                return Err(scanner_metadata_corrupt_error(
                    format!("failed to resolve file info versions: {e}"),
                    &item.bucket,
                    &object_path,
                ));
            }
        };

        // Single versioning lookup per object, shared with `apply_actions`
        // (which used to query it a second time). On failure keep the
        // historical fallback: default configuration (versioned = false) plus
        // the warn that `apply_actions` used to emit.
        let versioning_config = match BucketVersioningSys::get(&item.bucket).await {
            Ok(versioning_config) => versioning_config,
            Err(_) => {
                warn!(
                    target: "rustfs::scanner::folder",
                    event = EVENT_SCANNER_LIFECYCLE_ACTION,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    bucket = %item.bucket,
                    state = "versioning_lookup_failed_defaulting",
                    "Scanner lifecycle action falling back to default bucket versioning"
                );
                VersioningConfiguration::default()
            }
        };
        let versioned = versioning_config.versioned(&object_path);

        let object_infos = fivs
            .versions
            .iter()
            .map(|v| ObjectInfo::from_file_info(v, item.bucket.as_str(), object_path.as_str(), versioned))
            .collect::<Vec<ObjectInfo>>();
        let free_version_infos = fivs
            .free_versions
            .iter()
            .map(|v| ObjectInfo::from_file_info(v, item.bucket.as_str(), object_path.as_str(), versioned))
            .collect::<Vec<ObjectInfo>>();

        // The caller supplies one registry snapshot for the whole folder scan;
        // seeding from it prevents a TTL refresh from mixing generations in a
        // single result.
        let mut size_summary = SizeSummary {
            tier_stats: tier_stats_template(tier_names),
            ..Default::default()
        };

        let lock_config = object_lock_config_for_scanner_item(&item).await;

        // Count every version this object contributes to the scan, independent
        // of any lifecycle configuration, so scan-coverage metrics stay honest
        // on clusters without ILM rules. Recorded before `apply_actions` moves
        // `object_infos`.
        global_metrics().record_scanner_versions_scanned(object_infos.len() as u64);

        item.apply_actions(object_infos, lock_config, versioning_config, tier_names, &mut size_summary)
            .await;

        if !free_version_infos.is_empty() {
            for oi in free_version_infos {
                if ScannerItem::tier_is_known(&oi, tier_names) {
                    enqueue_runtime_free_version(oi).await;
                }
            }
        }

        done_object();

        Ok(size_summary)
    }

    #[tracing::instrument(skip(self, budget, updates, cache, set_disks, options), fields(scan_mode = ?options.scan_mode))]
    async fn nsscanner_disk(
        self: Arc<Self>,
        ctx: CancellationToken,
        budget: Arc<ScannerCycleBudget>,
        set_disks: Vec<Arc<Disk>>,
        cache: DataUsageCache,
        updates: Option<mpsc::Sender<DataUsageEntry>>,
        options: ScannerDiskScanOptions,
    ) -> Result<ScannerDiskScanOutcome> {
        let ScannerDiskScanOptions {
            scan_mode,
            prefix_scan_scope,
        } = options;
        let done_drive = Metrics::time(Metric::ScanBucketDrive);
        let drive_start = std::time::Instant::now();
        let bucket = cache.info.name.clone();
        let disk_path = self.path().to_string_lossy().to_string();
        let source = match scan_mode {
            HealScanMode::Deep => rustfs_scanner_metrics::metrics::ScannerWorkSource::Bitrot,
            HealScanMode::Normal | HealScanMode::Unknown => rustfs_scanner_metrics::metrics::ScannerWorkSource::Usage,
        };
        global_metrics().record_scan_bucket_drive_start(source, &bucket, &disk_path);
        let mut failure_guard = BucketDriveFailureGuard::new(source, &bucket, &disk_path);
        let _guard = self.start_scan();

        let mut cache = cache;

        let (lifecycle_config, _) = get_lifecycle_config(&cache.info.name)
            .await
            .unwrap_or_else(|_| (BucketLifecycleConfiguration::default(), OffsetDateTime::now_utc()));

        if lifecycle_config.has_active_rules("") {
            cache.info.lifecycle = Some(Arc::new(lifecycle_config));
        }

        let (replication_config, _) = get_replication_config(&cache.info.name).await.unwrap_or((
            ReplicationConfiguration {
                role: "".to_string(),
                rules: vec![],
            },
            OffsetDateTime::now_utc(),
        ));

        if replication_config.has_active_rules("", true)
            && let Ok(targets) = BucketTargetSys::get().list_bucket_targets(&cache.info.name).await
        {
            cache.info.replication = Some(Arc::new(ReplicationConfig::new(Some(replication_config), Some(targets))));
        }

        if let Ok((object_lock_config, _)) = get_object_lock_config(&cache.info.name).await
            && object_lock_config_enabled(&object_lock_config)
        {
            cache.info.object_lock = Some(Arc::new(object_lock_config));
        }

        // Prefix reuse never crosses semantic maintenance boundaries. A
        // lifecycle, replication, Object Lock, or erasure health walk can
        // make a clean data subtree require scanner-side work even without a
        // direct object mutation in the local journal. The folder scanner
        // separately rejects scopes in erasure mode.
        let prefix_scan_scope = (scan_mode == HealScanMode::Normal
            && cache.info.lifecycle.is_none()
            && cache.info.replication.is_none()
            && cache.info.object_lock.is_none())
        .then_some(prefix_scan_scope)
        .flatten();

        let result = scan_data_folder_scoped(
            ctx.clone(),
            budget,
            set_disks,
            self.clone(),
            cache,
            updates,
            scan_mode,
            SCANNER_SLEEPER.clone(),
            prefix_scan_scope,
        )
        .await;

        match result {
            Ok(mut data_usage_info) => {
                done_drive();
                emit_scan_bucket_drive_complete(source, true, &bucket, &disk_path, drive_start.elapsed());
                data_usage_info.info.last_update = Some(SystemTime::now());
                failure_guard.mark_not_failed();
                Ok(ScannerDiskScanOutcome::Complete(data_usage_info))
            }
            Err(ScannerError::PartialCache(mut partial_cache)) => {
                done_drive();
                emit_scan_bucket_drive_partial(source, &bucket, &disk_path, drive_start.elapsed());
                partial_cache.info.last_update.get_or_insert_with(SystemTime::now);
                failure_guard.mark_not_failed();
                Ok(ScannerDiskScanOutcome::Partial(*partial_cache))
            }
            Err(ScannerError::NamespaceNotFoundCache(mut partial_cache)) => {
                done_drive();
                emit_scan_bucket_drive_partial(source, &bucket, &disk_path, drive_start.elapsed());
                partial_cache.info.last_update.get_or_insert_with(SystemTime::now);
                failure_guard.mark_not_failed();
                Ok(ScannerDiskScanOutcome::NamespaceNotFound(*partial_cache))
            }
            Err(e) => {
                if ctx.is_cancelled() {
                    emit_scan_bucket_drive_partial(source, &bucket, &disk_path, drive_start.elapsed());
                    failure_guard.mark_not_failed();
                } else {
                    done_drive();
                    emit_scan_bucket_drive_complete(source, false, &bucket, &disk_path, drive_start.elapsed());
                }
                Err(StorageError::other(format!("Failed to scan data folder: {e}")))
            }
        }
    }
}
