// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;
use rustfs_data_usage::BucketScanProgress;
use sha2::{Digest, Sha256};

/// Response-only view of persisted usage. Initial progress is never inserted
/// into the authoritative map or used by quota admission.
#[derive(Debug, serde::Serialize)]
pub struct AdminUsageSnapshot {
    #[serde(flatten)]
    pub usage: DataUsageInfo,
    pub snapshot_view: bool,
    pub initial_scan_progress: HashMap<String, BucketScanProgress>,
    pub unmeasured_buckets: Vec<String>,
}

pub(super) fn initial_progress_allowed(previous: Option<&BucketUsageInfo>, incarnation: uuid::Uuid) -> bool {
    if incarnation.is_nil() {
        return false;
    }
    match previous {
        None => true,
        Some(previous) => previous
            .bucket_incarnation
            .as_deref()
            .and_then(|value| uuid::Uuid::parse_str(value).ok())
            .filter(|previous| !previous.is_nil())
            .is_some_and(|previous| previous != incarnation),
    }
}

fn initial_snapshot_available(raw: &DataUsageInfo, source: UsageSnapshotSource) -> bool {
    source == UsageSnapshotSource::Primary
        && raw.usage_snapshot_initial_view_version == Some(1)
        && if raw.usage_snapshot_bootstrap_pending {
            raw.usage_snapshot_initial_bootstrap
        } else {
            raw.is_complete_bucket_usage_snapshot() && !raw.usage_snapshot_observational && !raw.usage_snapshot_admin_projection
        }
}

/// Read complete persisted values before considering initial progress.
///
/// The caller supplies only authorized bucket names. Primary/backup failures
/// propagate; a filtered or unreadable historical value is not a first scan.
/// Progress reads are bounded and optional, and never trigger object listing.
///
/// # Errors
/// Returns the persisted snapshot's read or decode error when neither copy
/// supplies a usable baseline.
pub async fn load_admin_usage_snapshot(store: Arc<ECStore>, buckets: &[String]) -> Result<AdminUsageSnapshot, Error> {
    let (raw, source) = load_data_usage_snapshot(store.clone()).await?;
    let certified = load_certified_buckets(store.clone(), &raw).await;
    let history_available = certified.is_ok();
    let mut history = raw.clone();
    let mut merged_completed_buckets = false;
    if let Ok(Some(certified)) = certified {
        for (bucket, value) in certified.buckets_usage {
            let replace = history.buckets_usage.get(&bucket).is_none_or(|previous| {
                match (value.usage_certification, previous.usage_certification) {
                    (Some(next), Some(old)) => {
                        (next.epoch, next.cycle, next.certified_at) > (old.epoch, old.cycle, old.certified_at)
                    }
                    (Some(_), None) => true,
                    _ => false,
                }
            });
            if replace {
                history.buckets_usage.insert(bucket, value);
                merged_completed_buckets = true;
            }
        }
    }
    let mut usage = normalize_loaded_data_usage(raw.clone(), source.is_authoritative()).await.0;
    for (bucket, value) in &history.buckets_usage {
        if value.usage_certification.is_some() {
            usage.buckets_usage.insert(bucket.clone(), value.clone());
        }
    }
    let original_count = usage.buckets_usage.len();
    usage.buckets_usage.retain(|_, usage| {
        !matches!(
            usage.usage_value_state,
            Some(rustfs_data_usage::BucketUsageValueState::Observed | rustfs_data_usage::BucketUsageValueState::Unknown)
        ) && (!matches!(
            usage.usage_value_state,
            Some(rustfs_data_usage::BucketUsageValueState::Certified | rustfs_data_usage::BucketUsageValueState::Carried)
        ) || usage.usage_certification.is_some())
    });
    if merged_completed_buckets || usage.buckets_usage.len() != original_count {
        refresh_completed_snapshot_totals(&mut usage)?;
    }
    // A complete observation can be newer than the last certified value. This
    // endpoint deliberately keeps the persisted value and its original time.
    usage.usage_snapshot_bucket_observations.clear();
    usage = project_admin_data_usage_for_current_buckets(store.clone(), usage, false).await;
    let mut initial_scan_progress = if history_available && initial_snapshot_available(&raw, source) {
        scan_progress::load_initial_bucket_scan_progress(store, buckets, &history).await
    } else {
        HashMap::new()
    };
    initial_scan_progress.retain(|bucket, _| !usage.buckets_usage.contains_key(bucket));
    let mut unmeasured_buckets = buckets
        .iter()
        .filter(|bucket| !usage.buckets_usage.contains_key(*bucket))
        .cloned()
        .collect::<Vec<_>>();
    unmeasured_buckets.sort();
    unmeasured_buckets.dedup();
    Ok(AdminUsageSnapshot {
        usage,
        snapshot_view: true,
        initial_scan_progress,
        unmeasured_buckets,
    })
}

fn refresh_completed_snapshot_totals(usage: &mut DataUsageInfo) -> Result<(), Error> {
    if !usage.checked_calculate_totals() {
        return Err(Error::other("completed snapshot totals overflow"));
    }
    usage.buckets_count = u64::try_from(usage.buckets_usage.len()).map_err(Error::other)?;
    usage.bucket_sizes = usage
        .buckets_usage
        .iter()
        .map(|(name, bucket)| (name.clone(), bucket.size))
        .collect();
    usage.usage_snapshot_complete = false;
    usage.usage_snapshot_partial = true;
    usage.usage_snapshot_admin_projection = true;
    usage.usage_snapshot_converged = Some(false);
    usage.replication_info.clear();
    usage.tier_stats = None;
    usage.unknown_tier_stats = None;
    Ok(())
}

async fn load_certified_buckets(store: Arc<ECStore>, baseline: &DataUsageInfo) -> Result<Option<DataUsageInfo>, Error> {
    let path = format!("buckets/{}", rustfs_data_usage::DATA_USAGE_CERTIFIED_BUCKETS_OBJECT_NAME);
    let bytes = match read_config_preserve_empty(store.clone(), &path).await {
        Ok(bytes) => bytes,
        Err(Error::ConfigNotFound) => return Ok(None),
        Err(error) => return Err(error),
    };
    let info: DataUsageInfo = serde_json::from_slice(&bytes).map_err(|_| Error::other("invalid completed-bucket snapshot"))?;
    if !info.is_certified_buckets_snapshot() {
        return Err(Error::other("unsupported completed-bucket snapshot"));
    }
    if !info.certified_buckets_match_baseline(baseline) {
        return Ok(None);
    }
    let scope = info
        .usage_snapshot_certified_buckets
        .as_ref()
        .ok_or_else(|| Error::other("missing completed-bucket scope"))?;
    let movement = store.scanner_durable_data_movement_generation().await;
    if movement != Some(scope.movement_generation) {
        return Ok(None);
    }
    let mut sources = Vec::new();
    for set in store.all_set_disks() {
        let Some(disks) = set.format.erasure.sets.get(set.set_index) else { return Ok(None) };
        if set.format.id.is_nil() || disks.is_empty() || disks.iter().any(uuid::Uuid::is_nil) {
            return Ok(None);
        }
        let mut digest = Sha256::new();
        digest.update(set.format.id.as_bytes());
        for disk in disks {
            digest.update(disk.as_bytes());
        }
        sources.push(rustfs_data_usage::CertifiedBucketsSource {
            pool_index: u64::try_from(set.pool_index).map_err(Error::other)?,
            set_index: u64::try_from(set.set_index).map_err(Error::other)?,
            layout: digest.finalize().into(),
        });
    }
    sources.sort_by_key(|source| (source.pool_index, source.set_index));
    if sources != scope.sources || store.scanner_durable_data_movement_generation().await != movement {
        return Ok(None);
    }
    Ok(Some(info))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completed_bucket_snapshot_projection_recalculates_all_totals_without_refreshing_time() {
        let measured_at = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let mut usage = DataUsageInfo {
            last_update: Some(measured_at),
            usage_snapshot_complete: true,
            buckets_usage: HashMap::from([
                (
                    "old".into(),
                    BucketUsageInfo {
                        objects_count: 2,
                        size: 20,
                        versions_count: 4,
                        delete_markers_count: 1,
                        ..Default::default()
                    },
                ),
                (
                    "small".into(),
                    BucketUsageInfo {
                        objects_count: 3,
                        size: 30,
                        versions_count: 5,
                        delete_markers_count: 2,
                        ..Default::default()
                    },
                ),
            ]),
            ..Default::default()
        };
        refresh_completed_snapshot_totals(&mut usage).expect("valid complete buckets");
        assert_eq!(
            (
                usage.buckets_count,
                usage.objects_total_count,
                usage.objects_total_size,
                usage.versions_total_count,
                usage.delete_markers_total_count
            ),
            (2, 5, 50, 9, 3)
        );
        assert_eq!(usage.bucket_sizes, HashMap::from([("old".into(), 20), ("small".into(), 30)]));
        assert_eq!(usage.last_update, Some(measured_at));
        assert!(!usage.usage_snapshot_complete);
        assert!(usage.usage_snapshot_admin_projection);
    }

    #[test]
    fn completed_bucket_snapshot_projection_rejects_counter_overflow() {
        let mut usage = DataUsageInfo {
            buckets_usage: HashMap::from([
                (
                    "old".into(),
                    BucketUsageInfo {
                        objects_count: u64::MAX,
                        ..Default::default()
                    },
                ),
                (
                    "small".into(),
                    BucketUsageInfo {
                        objects_count: 1,
                        ..Default::default()
                    },
                ),
            ]),
            ..Default::default()
        };
        assert!(refresh_completed_snapshot_totals(&mut usage).is_err());
    }

    #[test]
    fn backup_and_recovery_cannot_turn_missing_history_into_first_statistics() {
        let mut raw = DataUsageInfo {
            usage_snapshot_bootstrap_pending: true,
            ..Default::default()
        };
        assert!(!initial_snapshot_available(&raw, UsageSnapshotSource::Primary));
        raw.usage_snapshot_initial_bootstrap = true;
        raw.usage_snapshot_initial_view_version = Some(1);
        assert!(initial_snapshot_available(&raw, UsageSnapshotSource::Primary));
        for source in [
            UsageSnapshotSource::Backup,
            UsageSnapshotSource::LegacyPrimary,
            UsageSnapshotSource::LegacyBackup,
            UsageSnapshotSource::Missing,
        ] {
            assert!(!initial_snapshot_available(&raw, source));
        }
        assert!(!initial_snapshot_available(&DataUsageInfo::default(), UsageSnapshotSource::Primary));
    }

    #[test]
    fn initial_progress_requires_absence_in_the_unfiltered_current_incarnation() {
        let incarnation = uuid::Uuid::from_u128(1);
        assert!(initial_progress_allowed(None, incarnation));
        let mut previous = BucketUsageInfo {
            bucket_incarnation: Some(incarnation.to_string()),
            ..Default::default()
        };
        assert!(!initial_progress_allowed(Some(&previous), incarnation));
        // Filtering this value, a new reader, or a leader handoff cannot make
        // the same persisted bucket a first scan again.
        previous.bucket_incarnation = None;
        assert!(!initial_progress_allowed(Some(&previous), incarnation));
        previous.bucket_incarnation = Some("invalid".into());
        assert!(!initial_progress_allowed(Some(&previous), incarnation));
        previous.bucket_incarnation = Some(uuid::Uuid::nil().to_string());
        assert!(!initial_progress_allowed(Some(&previous), incarnation));
        previous.bucket_incarnation = Some(uuid::Uuid::from_u128(2).to_string());
        assert!(initial_progress_allowed(Some(&previous), incarnation));
        assert!(!initial_progress_allowed(None, uuid::Uuid::nil()));
    }
}
