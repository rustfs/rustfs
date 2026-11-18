// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

//! Diagnostic provenance and response-only projections of complete bucket observations.

use crate::{BucketUsageInfo, DataUsageInfo, DataUsageSnapshotIdentity};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, time::SystemTime};

pub const DATA_USAGE_BUCKET_OBSERVATION_VERSION: u16 = 1;

/// Provenance of the returned bucket value, independent of quota authority.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BucketUsageValueState {
    Certified,
    Carried,
    Observed,
    #[serde(other)]
    Unknown,
}

/// A certification of this exact bucket value, never its publication timestamp.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct BucketUsageCertification {
    pub epoch: u64,
    pub cycle: u64,
    pub certified_at: SystemTime,
}

/// Immutable provenance of one complete traversal contribution.
///
/// The scanner validates its storage-owned sweep identity before constructing
/// this public projection. Equal start/request plans are allowed when a fresh
/// set contributes alongside another set's mixed traversal.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct DataUsageBucketObservationSource {
    pub pool_index: u64,
    pub set_index: u64,
    pub bucket_incarnation: String,
    pub scanner_epoch: u64,
    pub scanner_cycle: u64,
    pub scan_plan_digest: [u8; 32],
    pub started_plan: [u8; 32],
    pub requested_plan: [u8; 32],
    pub completed_at: SystemTime,
}

/// An additive companion; legacy bucket maps never contain this mixed value.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct DataUsageBucketObservation {
    pub version: u16,
    pub usage: BucketUsageInfo,
    /// Exact enclosing candidate identity. Rewrapping preserves source times.
    pub snapshot_identity: DataUsageSnapshotIdentity,
    /// Oldest contribution time, so a newer set cannot hide an older value.
    pub completed_at: SystemTime,
    /// Strictly ordered, complete coverage of the enclosing live set inventory.
    pub sources: Vec<DataUsageBucketObservationSource>,
}

/// Companion evolution must not make an otherwise usable canonical snapshot
/// unreadable. Unsupported or malformed diagnostic entries never grant proof.
pub(crate) fn deserialize_bucket_observations<'de, D>(
    deserializer: D,
) -> Result<HashMap<String, DataUsageBucketObservation>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Entry {
        Observation(Box<DataUsageBucketObservation>),
        Ignored(serde::de::IgnoredAny),
    }
    Ok(HashMap::<String, Entry>::deserialize(deserializer)?
        .into_iter()
        .filter_map(|(bucket, entry)| match entry {
            Entry::Observation(observation) if observation.version == DATA_USAGE_BUCKET_OBSERVATION_VERSION => {
                Some((bucket, *observation))
            }
            Entry::Observation(_) | Entry::Ignored(_) => None,
        })
        .collect())
}

impl BucketUsageInfo {
    /// Attach a certificate only after the caller proves complete bucket coverage.
    pub fn certify(&mut self, certification: BucketUsageCertification) {
        self.usage_value_state = Some(BucketUsageValueState::Certified);
        self.usage_certification = Some(certification);
    }

    /// Preserve the certificate when carrying the exact prior bucket value.
    pub fn mark_carried(&mut self) {
        self.normalize_usage_provenance();
        self.usage_value_state = Some(BucketUsageValueState::Carried);
    }

    /// The value changed without a complete bucket certificate.
    pub fn mark_observed(&mut self) {
        self.usage_value_state = Some(BucketUsageValueState::Observed);
        self.usage_certification = None;
    }

    /// Future or legacy provenance cannot authorize a retained certificate.
    pub fn normalize_usage_provenance(&mut self) {
        if !matches!(
            self.usage_value_state,
            Some(BucketUsageValueState::Certified | BucketUsageValueState::Carried)
        ) {
            self.usage_certification = None;
        }
        if self.usage_value_state == Some(BucketUsageValueState::Certified) && self.usage_certification.is_none() {
            self.usage_value_state = Some(BucketUsageValueState::Unknown);
        }
    }
}

impl DataUsageBucketObservation {
    /// Verify the public provenance against the enclosing candidate and current
    /// storage-owned incarnation. This never grants publication or ACK authority.
    pub fn matches_snapshot(&self, snapshot: &DataUsageInfo, incarnation: &str) -> bool {
        let Some(root_generation) = snapshot.scanner_epoch.zip(snapshot.scanner_cycle) else {
            return false;
        };
        if self.version != DATA_USAGE_BUCKET_OBSERVATION_VERSION
            || snapshot.usage_snapshot_admin_projection
            || self.snapshot_identity != snapshot.snapshot_identity()
            || incarnation.is_empty()
            || self.usage.bucket_incarnation.as_deref() != Some(incarnation)
            || self.sources.is_empty()
            || self.usage.usage_certification.is_some()
            || self.usage.usage_value_state != Some(BucketUsageValueState::Observed)
            || self.sources.iter().map(|source| source.completed_at).min() != Some(self.completed_at)
        {
            return false;
        }
        let mut previous = None;
        let mut plan = None;
        for state in &snapshot.usage_snapshot_set_states {
            let key = (state.pool_index, state.set_index);
            if previous.is_some_and(|previous| previous >= key)
                || state.scan_plan_digest.is_none()
                || plan.is_some_and(|plan| Some(plan) != state.scan_plan_digest)
            {
                return false;
            }
            previous = Some(key);
            plan = state.scan_plan_digest;
        }
        let mut sources = self.sources.iter();
        for state in snapshot.usage_snapshot_set_states.iter().filter(|state| !state.tombstone) {
            let Some(source) = sources.next() else {
                return false;
            };
            let Some(set_generation) = state.scanner_epoch.zip(state.scanner_cycle) else {
                return false;
            };
            if (source.pool_index, source.set_index) != (state.pool_index, state.set_index)
                || source.bucket_incarnation != incarnation
                || Some(source.scan_plan_digest) != state.scan_plan_digest
                || (source.scanner_epoch, source.scanner_cycle) > set_generation
                || set_generation > root_generation
            {
                return false;
            }
        }
        sources.next().is_none()
    }
}

impl DataUsageInfo {
    /// Overlay validated companions on an owned admin response only.
    ///
    /// `incarnations` must come from current storage metadata, not the companion.
    /// The projection is marked explicitly so persistence and quota boundaries
    /// can reject accidental reuse. Invalid companions leave canonical values
    /// unchanged. Counter overflow rejects the whole projection.
    pub fn project_bucket_observations_for_admin(&mut self, incarnations: &HashMap<String, String>) -> usize {
        if self.usage_snapshot_admin_projection
            || (!self.is_complete_bucket_usage_snapshot() && !self.is_valid_partial_snapshot())
        {
            return 0;
        }
        let observations =
            self.usage_snapshot_bucket_observations
                .iter()
                .filter(|(bucket, observation)| {
                    if self.usage_snapshot_complete
                        && (!self.usage_snapshot_pending_buckets.iter().any(|pending| pending == *bucket)
                            || u64::try_from(self.usage_snapshot_pending_buckets.len()).ok()
                                != Some(self.usage_snapshot_pending_bucket_count))
                    {
                        return false;
                    }
                    if self
                        .buckets_usage
                        .get(*bucket)
                        .and_then(|usage| usage.usage_certification)
                        .is_some_and(|certificate| {
                            observation.sources.iter().all(|source| {
                                (source.scanner_epoch, source.scanner_cycle) <= (certificate.epoch, certificate.cycle)
                            })
                        })
                    {
                        return false;
                    }
                    incarnations.get(*bucket).is_some_and(|incarnation| {
                        self.buckets_usage
                            .get(*bucket)
                            .and_then(|usage| usage.bucket_incarnation.as_ref())
                            .is_none_or(|canonical| canonical == incarnation)
                            && observation.matches_snapshot(self, incarnation)
                    })
                })
                .collect::<HashMap<_, _>>();
        if observations.is_empty() {
            return 0;
        }
        let mut values = self
            .buckets_usage
            .iter()
            .filter(|(bucket, _)| !observations.contains_key(bucket))
            .map(|(_, usage)| usage)
            .chain(observations.values().map(|observation| &observation.usage));
        let totals = values.try_fold([0_u64; 4], |total, usage| {
            Some([
                total[0].checked_add(usage.size)?,
                total[1].checked_add(usage.objects_count)?,
                total[2].checked_add(usage.versions_count)?,
                total[3].checked_add(usage.delete_markers_count)?,
            ])
        });
        let Some([size, objects, versions, markers]) = totals else {
            return 0;
        };
        let projected = observations.len();
        for (bucket, observation) in observations {
            self.buckets_usage.insert(bucket.clone(), observation.usage.clone());
        }
        self.bucket_sizes = self
            .buckets_usage
            .iter()
            .map(|(bucket, usage)| (bucket.clone(), usage.size))
            .collect();
        self.buckets_count = u64::try_from(self.buckets_usage.len()).unwrap_or(u64::MAX);
        self.objects_total_size = size;
        self.objects_total_count = objects;
        self.versions_total_count = versions;
        self.delete_markers_total_count = markers;
        self.replication_info.clear();
        self.tier_stats = None;
        self.unknown_tier_stats = None;
        self.usage_snapshot_complete = false;
        self.usage_snapshot_partial = true;
        self.usage_snapshot_converged = Some(false);
        self.usage_snapshot_observational = true;
        self.usage_snapshot_admin_projection = true;
        projected
    }
}

#[cfg(test)]
mod tests;
