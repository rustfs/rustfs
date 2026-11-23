// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

//! Display-only scanner progress. These values never authorize quota admission,
//! cache reuse, or publication of a certified usage snapshot.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const BUCKET_SCAN_PROGRESS_VERSION: u16 = 1;
pub const BUCKET_SCAN_PROGRESS_MAX_BYTES: usize = 16 * 1024;

/// The object lives on its contributing erasure set, alongside its bucket cache.
pub fn bucket_scan_progress_path(bucket: &str) -> String {
    format!("buckets/{bucket}/.usage-progress.json")
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BucketScanStage {
    Enumerating,
    Scanning,
    AwaitingSources,
    Completed,
}

/// One replaceable sample from a logical erasure set, never a per-disk delta.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct BucketScanProgressSource {
    pub version: u16,
    pub bucket: String,
    pub bucket_incarnation: String,
    pub pool_index: u64,
    pub set_index: u64,
    pub set_layout: [u8; 32],
    pub publication_epoch: u64,
    /// Shared durable movement scope. A writer's local admission epoch is not
    /// comparable to another node's counter, including after restart.
    #[serde(default)]
    pub movement_generation: Option<u64>,
    pub scanner_epoch: u64,
    pub scanner_cycle: u64,
    /// Unix milliseconds of the sample, preserved when an older sample is carried.
    pub updated_at: u64,
    pub stage: BucketScanStage,
    /// Accepted logical object usage; enumeration alone leaves both absent.
    pub objects: Option<u64>,
    pub size: Option<u64>,
}

/// Response-only progress, deliberately separate from a bucket's total usage.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct BucketScanProgress {
    pub stage: BucketScanStage,
    pub updated_at: u64,
    pub objects: Option<u64>,
    pub size: Option<u64>,
    pub sources_observed: u64,
    pub sources_total: u64,
}

impl BucketScanProgressSource {
    /// Bind a sample to shared storage scope, independently of reader-local epochs.
    pub fn matches(&self, bucket: &str, incarnation: &str, source: (u64, u64), movement_generation: u64) -> bool {
        self.version == BUCKET_SCAN_PROGRESS_VERSION
            && self.bucket == bucket
            && !incarnation.is_empty()
            && incarnation != "00000000-0000-0000-0000-000000000000"
            && self.bucket_incarnation == incarnation
            && (self.pool_index, self.set_index) == source
            && self.movement_generation == Some(movement_generation)
            && self.updated_at > 0
            && self.objects.is_some() == self.size.is_some()
            && (self.stage != BucketScanStage::Completed || self.objects.is_some())
            && self.stage != BucketScanStage::AwaitingSources
    }
}

/// Fold each logical source exactly once. Retries replace samples; replicas do
/// not add to them. Missing sources remain explicit, including on first startup.
/// Overflow or conflicting copies of a source make the view unavailable.
/// Callers first bind each sample to the same current bucket and movement
/// scope with [`BucketScanProgressSource::matches`].
pub fn aggregate_bucket_scan_progress(
    sources: impl IntoIterator<Item = BucketScanProgressSource>,
    expected: &[(u64, u64)],
) -> Option<BucketScanProgress> {
    if expected.is_empty() || expected.windows(2).any(|pair| pair[0] >= pair[1]) {
        return None;
    }
    let mut selected = BTreeMap::new();
    for sample in sources {
        let key = (sample.pool_index, sample.set_index);
        if expected.binary_search(&key).is_err() {
            return None;
        }
        if let Some(previous) = selected.insert(key, sample.clone())
            && previous != sample
        {
            return None;
        }
    }
    let updated_at = selected.values().map(|sample| sample.updated_at).min()?;
    let mut objects = None;
    let mut size = None;
    for sample in selected.values() {
        match (sample.objects, sample.size) {
            (Some(count), Some(bytes)) => {
                objects = Some(objects.unwrap_or(0_u64).checked_add(count)?);
                size = Some(size.unwrap_or(0_u64).checked_add(bytes)?);
            }
            (None, None) => {}
            _ => return None,
        }
    }
    let complete = selected.len() == expected.len() && selected.values().all(|s| s.stage == BucketScanStage::Completed);
    Some(BucketScanProgress {
        stage: if complete {
            BucketScanStage::Completed
        } else if selected.len() < expected.len() {
            BucketScanStage::AwaitingSources
        } else if objects.is_some() {
            BucketScanStage::Scanning
        } else {
            BucketScanStage::Enumerating
        },
        updated_at,
        objects,
        size,
        sources_observed: u64::try_from(selected.len()).ok()?,
        sources_total: u64::try_from(expected.len()).ok()?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(set: u64) -> BucketScanProgressSource {
        BucketScanProgressSource {
            version: BUCKET_SCAN_PROGRESS_VERSION,
            bucket: "bucket".into(),
            bucket_incarnation: "identity".into(),
            pool_index: 0,
            set_index: set,
            set_layout: [1; 32],
            publication_epoch: 0,
            movement_generation: Some(0),
            scanner_epoch: 3,
            scanner_cycle: 4,
            updated_at: 1000,
            stage: BucketScanStage::Scanning,
            objects: Some(7),
            size: Some(77),
        }
    }

    #[test]
    fn repeated_source_samples_never_double_count() {
        let one = sample(0);
        let progress = aggregate_bucket_scan_progress([one.clone(), one], &[(0, 0), (0, 1)]).expect("partial progress");
        assert_eq!(progress.objects, Some(7));
        assert_eq!(progress.size, Some(77));
        assert_eq!(progress.stage, BucketScanStage::AwaitingSources);
        assert_eq!(progress.sources_observed, 1);
    }

    #[test]
    fn enumeration_does_not_invent_empty_usage() {
        let mut one = sample(0);
        one.stage = BucketScanStage::Enumerating;
        one.objects = None;
        one.size = None;
        let progress = aggregate_bucket_scan_progress([one], &[(0, 0)]).expect("enumeration progress");
        assert_eq!(progress.objects, None);
        assert_eq!(progress.size, None);
        assert_eq!(progress.stage, BucketScanStage::Enumerating);
    }

    #[test]
    fn completion_requires_every_source_and_keeps_oldest_timestamp() {
        let mut one = sample(0);
        one.stage = BucketScanStage::Completed;
        let mut two = sample(1);
        two.updated_at = 2000;
        let pending = aggregate_bucket_scan_progress([one.clone(), two.clone()], &[(0, 0), (0, 1)]).expect("pending");
        assert_eq!(pending.stage, BucketScanStage::Scanning);
        two.stage = BucketScanStage::Completed;
        let done = aggregate_bucket_scan_progress([one, two], &[(0, 0), (0, 1)]).expect("completed");
        assert_eq!((done.objects, done.size, done.updated_at), (Some(14), Some(154), 1000));
        assert_eq!(done.stage, BucketScanStage::Completed);
    }

    #[test]
    fn conflicting_replicas_overflow_and_foreign_sources_are_rejected() {
        let mut other = sample(0);
        other.objects = Some(8);
        assert!(aggregate_bucket_scan_progress([sample(0), other], &[(0, 0)]).is_none());
        assert!(aggregate_bucket_scan_progress([sample(1)], &[(0, 0)]).is_none());
        let mut huge = sample(1);
        huge.size = Some(u64::MAX);
        assert!(aggregate_bucket_scan_progress([sample(0), huge], &[(0, 0), (0, 1)]).is_none());
    }

    #[test]
    fn progress_cannot_cross_bucket_incarnation_or_movement_generation() {
        let one = sample(0);
        assert!(one.matches("bucket", "identity", (0, 0), 0));
        assert!(!one.matches("bucket", "recreated", (0, 0), 0));
        assert!(!one.matches("bucket", "identity", (0, 1), 0));
        assert!(!one.matches("bucket", "identity", (0, 0), 1));
    }

    #[test]
    fn progress_reader_local_epoch_does_not_hide_shared_sample() {
        let mut one = sample(0);
        one.publication_epoch = 7;
        assert!(one.matches("bucket", "identity", (0, 0), 0));
        one.movement_generation = None;
        assert!(!one.matches("bucket", "identity", (0, 0), 0), "missing shared scope stays unknown");
    }
}
