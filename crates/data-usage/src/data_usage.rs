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

use serde::{Deserialize, Serialize, ser::SerializeMap as _};
use std::{
    collections::{HashMap, HashSet},
    hash::{DefaultHasher, Hash, Hasher},
    time::{Duration, SystemTime},
};

/// Maximum amount a persisted `last_update` may lead the local wall clock before the
/// persisted timestamp is treated as untrustworthy.
///
/// Invariant: the "skip stale usage update" monotonicity check (incoming `last_update`
/// <= existing `last_update` => skip persisting) is only valid while the existing
/// timestamp could plausibly have been produced by a healthy clock. If the on-disk
/// snapshot is future-dated beyond this tolerance (NTP step-back, or scanner
/// leadership moving to a node with a slower clock), the comparison would skip every
/// save forever and freeze admin usage stats; callers must bypass the skip instead.
pub const USAGE_LAST_UPDATE_FUTURE_TOLERANCE: Duration = Duration::from_secs(5 * 60);

/// Cluster-wide usage snapshot written by coordinated scanners.
///
/// `usage_snapshot_complete` is an additive JSON field: older readers ignore
/// it, while current readers treat snapshots from older writers as unknown.
/// Keeping the existing object name preserves rolling-upgrade and rollback
/// compatibility without allowing an ambiguous snapshot to become authoritative.
pub const DATA_USAGE_OBJECT_NAME: &str = ".usage.v2.json";
/// Latest structurally complete scanner observation. Unlike
/// [`DATA_USAGE_OBJECT_NAME`], this object is never authoritative for quota
/// admission because namespace activity may have raced the scan.
pub const DATA_USAGE_OBSERVED_OBJECT_NAME: &str = ".usage.observed.json";

/// Usage snapshot written by scanner implementations predating distributed
/// leadership fencing. It is read only when neither authoritative snapshot
/// copy exists.
// RUSTFS_COMPAT_TODO(scanner-usage-v2): keep .usage.json readable and removable during rolling upgrades from pre-v2 scanners. Remove after supported direct-upgrade sources all write .usage.v2.json.
pub const LEGACY_DATA_USAGE_OBJECT_NAME: &str = ".usage.json";

/// Returns true when `existing_last_update` is ahead of `now` by more than
/// [`USAGE_LAST_UPDATE_FUTURE_TOLERANCE`], i.e. the persisted timestamp cannot be
/// trusted for staleness comparisons and a fresh snapshot save must be allowed.
pub fn usage_last_update_is_untrusted_future(existing_last_update: SystemTime, now: SystemTime) -> bool {
    existing_last_update > now + USAGE_LAST_UPDATE_FUTURE_TOLERANCE
}

#[derive(Clone, Copy, Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TierStats {
    pub total_size: u64,
    pub num_versions: u64,
    pub num_objects: u64,
}

impl TierStats {
    pub fn add(&self, u: &TierStats) -> TierStats {
        TierStats {
            total_size: self.total_size.saturating_add(u.total_size),
            num_versions: self.num_versions.saturating_add(u.num_versions),
            num_objects: self.num_objects.saturating_add(u.num_objects),
        }
    }

    /// True when [`TierStats::add`] would report the exact sum instead of saturating.
    pub fn fits_add(&self, u: &TierStats) -> bool {
        self.total_size.checked_add(u.total_size).is_some()
            && self.num_versions.checked_add(u.num_versions).is_some()
            && self.num_objects.checked_add(u.num_objects).is_some()
    }

    /// True when this tier contributed nothing, i.e. merging it is a no-op.
    pub fn is_empty(&self) -> bool {
        self.total_size == 0 && self.num_versions == 0 && self.num_objects == 0
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AllTierStats {
    pub tiers: HashMap<String, TierStats>,
}

impl AllTierStats {
    pub fn new() -> Self {
        Self { tiers: HashMap::new() }
    }

    pub fn is_empty(&self) -> bool {
        self.tiers.is_empty()
    }

    /// Folds a scan summary's per-tier map in.
    ///
    /// Scanners seed the map with a zeroed entry for every configured tier, so
    /// empty contributions are skipped to keep the persisted cache from growing
    /// one key per tier on every folder that never held tiered data.
    pub fn add_sizes(&mut self, tiers: &HashMap<String, TierStats>) {
        for (tier, st) in tiers {
            if st.is_empty() {
                continue;
            }
            let entry = self.tiers.entry(tier.clone()).or_default();
            *entry = entry.add(st);
        }
    }

    pub fn merge(&mut self, other: &AllTierStats) {
        self.add_sizes(&other.tiers);
    }

    /// True when [`AllTierStats::merge`] would report exact sums for every tier.
    pub fn fits_merge(&self, other: &AllTierStats) -> bool {
        other
            .tiers
            .iter()
            .all(|(tier, right)| self.tiers.get(tier).is_none_or(|left| left.fits_add(right)))
    }
}

/// Bucket target usage info provides replication statistics
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BucketTargetUsageInfo {
    pub replication_pending_size: u64,
    pub replication_failed_size: u64,
    pub replicated_size: u64,
    pub replica_size: u64,
    pub replication_pending_count: u64,
    pub replication_failed_count: u64,
    pub replicated_count: u64,
}

/// Bucket usage info provides bucket-level statistics
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BucketUsageInfo {
    pub size: u64,
    // Following five fields suffixed with V1 are here for backward compatibility
    // Total Size for objects that have not yet been replicated
    pub replication_pending_size_v1: u64,
    // Total size for objects that have witness one or more failures and will be retried
    pub replication_failed_size_v1: u64,
    // Total size for objects that have been replicated to destination
    pub replicated_size_v1: u64,
    // Total number of objects pending replication
    pub replication_pending_count_v1: u64,
    // Total number of objects that failed replication
    pub replication_failed_count_v1: u64,

    pub objects_count: u64,
    pub object_size_histogram: HashMap<String, u64>,
    pub object_versions_histogram: HashMap<String, u64>,
    pub versions_count: u64,
    pub delete_markers_count: u64,
    pub replica_size: u64,
    pub replica_count: u64,
    pub replication_info: HashMap<String, BucketTargetUsageInfo>,
}

/// DataUsageInfo represents data usage stats of the underlying storage
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataUsageInfo {
    /// Total capacity
    pub total_capacity: u64,
    /// Total used capacity
    pub total_used_capacity: u64,
    /// Total free capacity
    pub total_free_capacity: u64,

    /// LastUpdate is the timestamp of when the data usage info was last updated
    pub last_update: Option<SystemTime>,

    /// Monotonic scanner cycle that produced this complete snapshot.
    ///
    /// Older snapshots omit this field and continue to use `last_update` for
    /// compatibility. New scanner snapshots use the cycle to fence stale
    /// leaders independently of wall-clock skew.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scanner_cycle: Option<u64>,

    /// Persisted scanner leadership epoch that produced this snapshot.
    ///
    /// The epoch is claimed through the cycle-state CAS before scanning. It
    /// orders snapshots from different leaders even when their wall clocks or
    /// cycle counters coincide.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scanner_epoch: Option<u64>,

    /// Objects total count across all buckets
    pub objects_total_count: u64,
    /// Versions total count across all buckets
    pub versions_total_count: u64,
    /// Delete markers total count across all buckets
    pub delete_markers_total_count: u64,
    /// Objects total size across all buckets
    pub objects_total_size: u64,
    /// Replication info across all buckets
    pub replication_info: HashMap<String, BucketTargetUsageInfo>,
    /// Usage per storage class and remote tier across all buckets.
    ///
    /// Absent on snapshots written before per-tier accounting was published,
    /// and on clusters with no remote tier configured: the scanner classifies
    /// objects by tier (including `STANDARD`/`REDUCED_REDUNDANCY`) only once a
    /// tier exists, so an absent value means "not accounted", never "zero".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tier_stats: Option<AllTierStats>,

    /// Total number of buckets in this cluster
    pub buckets_count: u64,
    /// Buckets usage info provides following information across all buckets
    pub buckets_usage: HashMap<String, BucketUsageInfo>,
    /// Whether this snapshot covers the complete bucket namespace.
    ///
    /// Legacy snapshots default to `false`. A complete snapshot contains an
    /// explicit entry for every bucket, including confirmed-empty buckets.
    #[serde(default)]
    pub usage_snapshot_complete: bool,
    /// Whether no namespace activity or dirty-usage generation changed while
    /// the coordinated snapshot was being produced.
    ///
    /// `false` still describes a structurally complete, useful point-in-time
    /// usage view, but follow-up scanner work remains pending. `None` is kept
    /// for snapshots written before this status became observable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage_snapshot_converged: Option<bool>,
    /// Identity of the authoritative snapshot from which a nonconverged
    /// observation started. Admin readers require an exact match before using
    /// the observation, so bucket namespace mutations fence old observations
    /// without relying on synchronized clocks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage_snapshot_authoritative_baseline: Option<DataUsageSnapshotIdentity>,
    /// Per-set freshness for an observational aggregate.  A set entry is
    /// never sufficient to make the aggregate authoritative; it only records
    /// which last-known-good generation contributed to the view.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub usage_snapshot_set_states: Vec<DataUsageSnapshotSetState>,
    /// An observational view may contain only the sets that completed this
    /// cycle (or retained a compatible last-known-good cache).
    #[serde(default)]
    pub usage_snapshot_partial: bool,
    /// Deprecated kept here for backward compatibility reasons
    pub bucket_sizes: HashMap<String, u64>,
    /// Per-disk snapshot information when available
    #[serde(default)]
    pub disk_usage_status: Vec<DiskUsageStatus>,
}

/// Stable identity fields changed by both coordinated scanner publication and
/// backward-compatible bucket namespace cleanup.
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataUsageSnapshotIdentity {
    pub last_update: Option<SystemTime>,
    pub scanner_cycle: Option<u64>,
    pub scanner_epoch: Option<u64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataUsageSnapshotSetState {
    pub pool_index: u64,
    pub set_index: u64,
    #[serde(default)]
    pub scanner_cycle: Option<u64>,
    #[serde(default)]
    pub scanner_epoch: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scan_plan_digest: Option<[u8; 32]>,
    #[serde(default)]
    pub complete: bool,
    #[serde(default)]
    pub tombstone: bool,
}

impl DataUsageInfo {
    pub fn snapshot_identity(&self) -> DataUsageSnapshotIdentity {
        DataUsageSnapshotIdentity {
            last_update: self.last_update,
            scanner_cycle: self.scanner_cycle,
            scanner_epoch: self.scanner_epoch,
        }
    }
}

/// Return whether `candidate` was produced after `baseline`.
///
/// New coordinated snapshots are ordered by leadership epoch and scanner
/// cycle. The timestamp fallback preserves ordering for legacy snapshots that
/// predate those fields.
pub fn data_usage_snapshot_is_newer(candidate: &DataUsageInfo, baseline: &DataUsageInfo) -> bool {
    match (
        candidate.scanner_epoch.zip(candidate.scanner_cycle),
        baseline.scanner_epoch.zip(baseline.scanner_cycle),
    ) {
        (Some(candidate), Some(baseline)) => candidate > baseline,
        (Some(_), None) => true,
        (None, Some(_)) => false,
        (None, None) => match (candidate.last_update, baseline.last_update) {
            (Some(candidate), Some(baseline)) => candidate > baseline,
            (Some(_), None) => true,
            (None, Some(_) | None) => false,
        },
    }
}

/// Return whether a nonconverged observation may safely supersede the admin
/// view of `authoritative`.
///
/// The exact baseline identity is independent of clock ordering. Older binaries
/// already advance the authoritative timestamp when deleting a bucket, so a
/// rollback delete/recreate fences the previous bucket incarnation too.
pub fn observed_data_usage_is_newer(observed: &DataUsageInfo, authoritative: &DataUsageInfo) -> bool {
    observed.usage_snapshot_converged == Some(false)
        && (observed.is_complete_bucket_usage_snapshot() || observed.is_valid_partial_snapshot())
        && observed.usage_snapshot_authoritative_baseline.as_ref() == Some(&authoritative.snapshot_identity())
        && data_usage_snapshot_is_newer(observed, authoritative)
}

/// Metadata describing the status of a disk-level data usage snapshot.
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DiskUsageStatus {
    pub disk_id: String,
    pub pool_index: Option<usize>,
    pub set_index: Option<usize>,
    pub disk_index: Option<usize>,
    pub last_update: Option<SystemTime>,
    pub snapshot_exists: bool,
}

/// A bounded reconciliation record for an object whose logical size could not
/// be trusted at the scanner boundary. The scanner persists these records in
/// its cache; keeping the model here avoids a second, incompatible accounting
/// representation in storage-facing crates.
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SizeReconciliationEntry {
    /// Stable object/version identity key (not a metrics label).
    pub key: String,
    pub bucket: String,
    pub object: String,
    #[serde(default)]
    pub version_id: Option<String>,
    #[serde(default)]
    pub generation: Option<String>,
    /// Structured reason label; raw metadata values must never be stored here.
    pub reason: String,
    #[serde(default)]
    pub physical_size: Option<u64>,
    #[serde(default)]
    pub first_seen: u64,
    #[serde(default)]
    pub attempts: u32,
}

/// Object scope refreshed by one scanner pass. Existing debts in this scope
/// are removed before the pass's unresolved records are inserted.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SizeReconciliationScope {
    pub bucket: String,
    pub object: String,
}

/// Size summary for a single object or group of objects
#[derive(Debug, Default, Clone)]
pub struct SizeSummary {
    /// Total size
    pub total_size: usize,
    /// Number of versions
    pub versions: usize,
    /// Number of delete markers
    pub delete_markers: usize,
    /// Replicated size
    pub replicated_size: i64,
    /// Replicated count
    pub replicated_count: usize,
    /// Pending size
    pub pending_size: i64,
    /// Failed size
    pub failed_size: i64,
    /// Replica size
    pub replica_size: i64,
    /// Replica count
    pub replica_count: usize,
    /// Pending count
    pub pending_count: usize,
    /// Failed count
    pub failed_count: usize,
    /// Replication target stats
    pub repl_target_stats: HashMap<String, ReplTargetSizeSummary>,
    /// Per-tier accounting, keyed by storage class or remote tier name
    pub tier_stats: HashMap<String, TierStats>,
    /// Size-resolution debts observed while scanning this summary.
    pub size_reconciliation: Vec<SizeReconciliationEntry>,
    /// True when the per-object summary exceeded its bounded debt buffer.
    /// Callers must retain prior ledger entries rather than treating the
    /// partial list as a complete refresh.
    pub size_reconciliation_truncated: bool,
    /// Object scopes refreshed by this summary. They let the durable ledger
    /// remove versions that resolved without allocating one key per healthy
    /// version on the hot path.
    pub reconciliation_scopes: Vec<SizeReconciliationScope>,
}

/// Replication target size summary
#[derive(Debug, Default, Clone)]
pub struct ReplTargetSizeSummary {
    /// Replicated size
    pub replicated_size: i64,
    /// Replicated count
    pub replicated_count: usize,
    /// Pending size
    pub pending_size: i64,
    /// Failed size
    pub failed_size: i64,
    /// Pending count
    pub pending_count: usize,
    /// Failed count
    pub failed_count: usize,
}

// ===== Cache-related data structures =====

/// Data usage hash for path-based caching
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DataUsageHash(pub String);

impl DataUsageHash {
    pub fn string(&self) -> String {
        self.0.clone()
    }

    pub fn key(&self) -> String {
        self.0.clone()
    }

    pub fn mod_(&self, cycle: u32, cycles: u32) -> bool {
        if cycles <= 1 {
            return cycles == 1;
        }

        let hash = self.calculate_hash();
        hash as u32 % cycles == cycle % cycles
    }

    pub fn mod_alt(&self, cycle: u32, cycles: u32) -> bool {
        if cycles <= 1 {
            return cycles == 1;
        }

        let hash = self.calculate_hash();
        (hash >> 32) as u32 % cycles == cycle % cycles
    }

    fn calculate_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.0.hash(&mut hasher);
        hasher.finish()
    }
}

/// Data usage hash map type
pub type DataUsageHashMap = HashSet<String>;

/// Size histogram for object size distribution
const SIZE_HISTOGRAM_LEN: usize = 11;

#[derive(Clone, Debug, Serialize)]
pub struct SizeHistogram(Vec<u64>);

impl Default for SizeHistogram {
    fn default() -> Self {
        Self(vec![0; SIZE_HISTOGRAM_LEN])
    }
}

impl<'de> Deserialize<'de> for SizeHistogram {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let values = Vec::<u64>::deserialize(deserializer)?;
        if values.len() != SIZE_HISTOGRAM_LEN {
            return Err(serde::de::Error::invalid_length(
                values.len(),
                &"exactly 11 object-size histogram buckets",
            ));
        }
        Ok(Self(values))
    }
}

impl SizeHistogram {
    pub fn add(&mut self, size: u64) {
        let intervals = [
            (0, 1024 - 1),                              // LESS_THAN_1024_B
            (1024, 64 * 1024 - 1),                      // BETWEEN_1024_B_AND_64_KB
            (64 * 1024, 256 * 1024 - 1),                // BETWEEN_64_KB_AND_256_KB
            (256 * 1024, 512 * 1024 - 1),               // BETWEEN_256_KB_AND_512_KB
            (512 * 1024, 1024 * 1024 - 1),              // BETWEEN_512_KB_AND_1_MB
            (1024, 1024 * 1024 - 1),                    // BETWEEN_1024B_AND_1_MB
            (1024 * 1024, 10 * 1024 * 1024 - 1),        // BETWEEN_1_MB_AND_10_MB
            (10 * 1024 * 1024, 64 * 1024 * 1024 - 1),   // BETWEEN_10_MB_AND_64_MB
            (64 * 1024 * 1024, 128 * 1024 * 1024 - 1),  // BETWEEN_64_MB_AND_128_MB
            (128 * 1024 * 1024, 512 * 1024 * 1024 - 1), // BETWEEN_128_MB_AND_512_MB
            (512 * 1024 * 1024, u64::MAX),              // GREATER_THAN_512_MB
        ];

        for (idx, (start, end)) in intervals.iter().enumerate() {
            if size >= *start && size <= *end {
                self.0[idx] += 1;
                break;
            }
        }
    }

    pub fn to_map(&self) -> HashMap<String, u64> {
        // Numeric interval bounds, kept in lockstep with `add` above. The
        // rollup for the v1-compat `BETWEEN_1024B_AND_1_MB` bucket is derived
        // from these bounds rather than the display names to avoid undercounting
        // the sub-ranges in [1 KiB, 512 KiB).
        const ONE_MIB: u64 = 1024 * 1024;
        let intervals = [
            (0, 1024 - 1),                      // LESS_THAN_1024_B
            (1024, 64 * 1024 - 1),              // BETWEEN_1024_B_AND_64_KB
            (64 * 1024, 256 * 1024 - 1),        // BETWEEN_64_KB_AND_256_KB
            (256 * 1024, 512 * 1024 - 1),       // BETWEEN_256_KB_AND_512_KB
            (512 * 1024, ONE_MIB - 1),          // BETWEEN_512_KB_AND_1_MB
            (1024, ONE_MIB - 1),                // BETWEEN_1024B_AND_1_MB (v1-compat rollup)
            (ONE_MIB, 10 * ONE_MIB - 1),        // BETWEEN_1_MB_AND_10_MB
            (10 * ONE_MIB, 64 * ONE_MIB - 1),   // BETWEEN_10_MB_AND_64_MB
            (64 * ONE_MIB, 128 * ONE_MIB - 1),  // BETWEEN_64_MB_AND_128_MB
            (128 * ONE_MIB, 512 * ONE_MIB - 1), // BETWEEN_128_MB_AND_512_MB
            (512 * ONE_MIB, u64::MAX),          // GREATER_THAN_512_MB
        ];
        let names = [
            "LESS_THAN_1024_B",
            "BETWEEN_1024_B_AND_64_KB",
            "BETWEEN_64_KB_AND_256_KB",
            "BETWEEN_256_KB_AND_512_KB",
            "BETWEEN_512_KB_AND_1_MB",
            "BETWEEN_1024B_AND_1_MB",
            "BETWEEN_1_MB_AND_10_MB",
            "BETWEEN_10_MB_AND_64_MB",
            "BETWEEN_64_MB_AND_128_MB",
            "BETWEEN_128_MB_AND_512_MB",
            "GREATER_THAN_512_MB",
        ];

        // Sum every sub-bucket whose interval lies entirely within [1024, 1 MiB),
        // excluding the compat bucket itself, to form the v1-compat rollup.
        let compat_rollup: u64 = self
            .0
            .iter()
            .zip(intervals.iter())
            .zip(names.iter())
            .filter(|((_, (start, end)), name)| name != &&"BETWEEN_1024B_AND_1_MB" && *start >= 1024 && *end < ONE_MIB)
            .map(|((count, _), _)| *count)
            .fold(0, u64::saturating_add);

        let mut res = HashMap::new();
        for (count, name) in self.0.iter().zip(names.iter()) {
            if name == &"BETWEEN_1024B_AND_1_MB" {
                res.insert(name.to_string(), compat_rollup);
            } else {
                res.insert(name.to_string(), *count);
            }
        }
        res
    }

    pub fn merge_from(&mut self, other: &Self) {
        for (dst, src) in self.0.iter_mut().zip(other.0.iter()) {
            *dst += src;
        }
    }
}

/// Versions histogram for version count distribution
const VERSIONS_HISTOGRAM_LEN: usize = 7;

#[derive(Clone, Debug, Serialize)]
pub struct VersionsHistogram(Vec<u64>);

impl Default for VersionsHistogram {
    fn default() -> Self {
        Self(vec![0; VERSIONS_HISTOGRAM_LEN])
    }
}

impl<'de> Deserialize<'de> for VersionsHistogram {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let values = Vec::<u64>::deserialize(deserializer)?;
        if values.len() != VERSIONS_HISTOGRAM_LEN {
            return Err(serde::de::Error::invalid_length(
                values.len(),
                &"exactly 7 object-version histogram buckets",
            ));
        }
        Ok(Self(values))
    }
}

impl VersionsHistogram {
    pub fn add(&mut self, count: u64) {
        let intervals = [
            (0, 0),            // UNVERSIONED
            (1, 1),            // SINGLE_VERSION
            (2, 9),            // BETWEEN_2_AND_10
            (10, 99),          // BETWEEN_10_AND_100
            (100, 999),        // BETWEEN_100_AND_1000
            (1000, 9999),      // BETWEEN_1000_AND_10000
            (10000, u64::MAX), // GREATER_THAN_10000
        ];

        for (idx, (start, end)) in intervals.iter().enumerate() {
            if count >= *start && count <= *end {
                self.0[idx] += 1;
                break;
            }
        }
    }

    pub fn to_map(&self) -> HashMap<String, u64> {
        let names = [
            "UNVERSIONED",
            "SINGLE_VERSION",
            "BETWEEN_2_AND_10",
            "BETWEEN_10_AND_100",
            "BETWEEN_100_AND_1000",
            "BETWEEN_1000_AND_10000",
            "GREATER_THAN_10000",
        ];

        let mut res = HashMap::new();
        for (count, name) in self.0.iter().zip(names.iter()) {
            res.insert(name.to_string(), *count);
        }
        res
    }

    pub fn merge_from(&mut self, other: &Self) {
        for (dst, src) in self.0.iter_mut().zip(other.0.iter()) {
            *dst += src;
        }
    }
}

/// Replication statistics for a single target.
///
/// Renamed from `ReplicationStats`; serde field names are preserved
/// byte-identically to maintain wire compatibility with existing snapshots.
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationTargetUsage {
    pub pending_size: u64,
    pub replicated_size: u64,
    pub failed_size: u64,
    pub failed_count: u64,
    pub pending_count: u64,
    pub missed_threshold_size: u64,
    pub after_threshold_size: u64,
    pub missed_threshold_count: u64,
    pub after_threshold_count: u64,
    pub replicated_count: u64,
}

impl ReplicationTargetUsage {
    pub fn is_empty(&self) -> bool {
        let Self {
            pending_size,
            replicated_size,
            failed_size,
            failed_count,
            pending_count,
            missed_threshold_size,
            after_threshold_size,
            missed_threshold_count,
            after_threshold_count,
            replicated_count,
        } = self;

        *pending_size == 0
            && *replicated_size == 0
            && *failed_size == 0
            && *failed_count == 0
            && *pending_count == 0
            && *missed_threshold_size == 0
            && *after_threshold_size == 0
            && *missed_threshold_count == 0
            && *after_threshold_count == 0
            && *replicated_count == 0
    }

    #[deprecated(note = "use is_empty instead")]
    pub fn empty(&self) -> bool {
        self.is_empty()
    }
}

/// Replication statistics for all targets
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ReplicationAllStats {
    pub targets: HashMap<String, ReplicationTargetUsage>,
    pub replica_size: u64,
    pub replica_count: u64,
}

impl ReplicationAllStats {
    pub fn is_empty(&self) -> bool {
        let Self {
            replica_size,
            replica_count,
            targets,
        } = self;

        *replica_size == 0 && *replica_count == 0 && targets.values().all(ReplicationTargetUsage::is_empty)
    }

    #[deprecated(note = "use is_empty instead")]
    pub fn empty(&self) -> bool {
        self.is_empty()
    }
}

/// Data usage cache entry
#[derive(Clone, Debug, Default, Deserialize)]
pub struct DataUsageEntry {
    pub children: DataUsageHashMap,
    // These fields do not include any children.
    pub size: usize,
    pub objects: usize,
    pub versions: usize,
    pub delete_markers: usize,
    pub obj_sizes: SizeHistogram,
    pub obj_versions: VersionsHistogram,
    pub replication_stats: Option<ReplicationAllStats>,
    pub compacted: bool,
    /// Number of objects that failed to scan (e.g., IO errors)
    #[serde(default)]
    pub failed_objects: usize,
    /// Per-tier usage contributed by this entry, present only once a scan
    /// observed tier-classified objects.
    #[serde(default)]
    pub all_tier_stats: Option<AllTierStats>,
}

impl Serialize for DataUsageEntry {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Keep entries map-encoded so older readers can ignore fields appended
        // by newer scanner versions during rolling upgrades. The derived
        // (array) encoding made any appended field a decode error for them.
        let mut state = serializer.serialize_map(Some(11))?;
        state.serialize_entry("children", &self.children)?;
        state.serialize_entry("size", &self.size)?;
        state.serialize_entry("objects", &self.objects)?;
        state.serialize_entry("versions", &self.versions)?;
        state.serialize_entry("delete_markers", &self.delete_markers)?;
        state.serialize_entry("obj_sizes", &self.obj_sizes)?;
        state.serialize_entry("obj_versions", &self.obj_versions)?;
        state.serialize_entry("replication_stats", &self.replication_stats)?;
        state.serialize_entry("compacted", &self.compacted)?;
        state.serialize_entry("failed_objects", &self.failed_objects)?;
        state.serialize_entry("all_tier_stats", &self.all_tier_stats)?;
        state.end()
    }
}

impl DataUsageEntry {
    pub fn add_child(&mut self, hash: &DataUsageHash) {
        if self.children.contains(&hash.key()) {
            return;
        }
        self.children.insert(hash.key());
    }

    pub fn merge(&mut self, other: &DataUsageEntry) {
        self.objects += other.objects;
        self.versions += other.versions;
        self.delete_markers += other.delete_markers;
        self.size += other.size;
        self.failed_objects += other.failed_objects;

        if let Some(o_rep) = &other.replication_stats {
            let s_rep = self.replication_stats.get_or_insert_with(ReplicationAllStats::default);
            s_rep.replica_size += o_rep.replica_size;
            s_rep.replica_count += o_rep.replica_count;
            for (arn, stat) in o_rep.targets.iter() {
                let st = s_rep.targets.entry(arn.clone()).or_default();
                st.pending_size += stat.pending_size;
                st.replicated_size += stat.replicated_size;
                st.failed_size += stat.failed_size;
                st.failed_count += stat.failed_count;
                st.pending_count += stat.pending_count;
                st.missed_threshold_size += stat.missed_threshold_size;
                st.after_threshold_size += stat.after_threshold_size;
                st.missed_threshold_count += stat.missed_threshold_count;
                st.after_threshold_count += stat.after_threshold_count;
                st.replicated_count += stat.replicated_count;
            }
        }

        if let Some(o_tiers) = other.all_tier_stats.as_ref().filter(|tiers| !tiers.is_empty()) {
            self.all_tier_stats.get_or_insert_with(AllTierStats::new).merge(o_tiers);
        }

        self.obj_sizes.merge_from(&other.obj_sizes);
        self.obj_versions.merge_from(&other.obj_versions);
    }

    /// Folds a scan summary's per-tier map into this entry.
    pub fn add_tier_sizes(&mut self, tiers: &HashMap<String, TierStats>) {
        if tiers.values().all(TierStats::is_empty) {
            return;
        }
        self.all_tier_stats.get_or_insert_with(AllTierStats::new).add_sizes(tiers);
    }

    pub fn checked_merge(&mut self, other: &DataUsageEntry) -> bool {
        let scalar_counts_fit = self.objects.checked_add(other.objects).is_some()
            && self.versions.checked_add(other.versions).is_some()
            && self.delete_markers.checked_add(other.delete_markers).is_some()
            && self.size.checked_add(other.size).is_some()
            && self.failed_objects.checked_add(other.failed_objects).is_some();
        let histograms_fit = self.obj_sizes.0.len() == SIZE_HISTOGRAM_LEN
            && other.obj_sizes.0.len() == SIZE_HISTOGRAM_LEN
            && self.obj_versions.0.len() == VERSIONS_HISTOGRAM_LEN
            && other.obj_versions.0.len() == VERSIONS_HISTOGRAM_LEN
            && self
                .obj_sizes
                .0
                .iter()
                .zip(other.obj_sizes.0.iter())
                .all(|(left, right)| left.checked_add(*right).is_some())
            && self
                .obj_versions
                .0
                .iter()
                .zip(other.obj_versions.0.iter())
                .all(|(left, right)| left.checked_add(*right).is_some());
        let replication_fits = match (&self.replication_stats, &other.replication_stats) {
            (_, None) | (None, Some(_)) => true,
            (Some(left), Some(right)) => {
                left.replica_size.checked_add(right.replica_size).is_some()
                    && left.replica_count.checked_add(right.replica_count).is_some()
                    && right.targets.iter().all(|(target, right_stats)| {
                        left.targets.get(target).is_none_or(|left_stats| {
                            left_stats.pending_size.checked_add(right_stats.pending_size).is_some()
                                && left_stats.replicated_size.checked_add(right_stats.replicated_size).is_some()
                                && left_stats.failed_size.checked_add(right_stats.failed_size).is_some()
                                && left_stats.failed_count.checked_add(right_stats.failed_count).is_some()
                                && left_stats.pending_count.checked_add(right_stats.pending_count).is_some()
                                && left_stats
                                    .missed_threshold_size
                                    .checked_add(right_stats.missed_threshold_size)
                                    .is_some()
                                && left_stats
                                    .after_threshold_size
                                    .checked_add(right_stats.after_threshold_size)
                                    .is_some()
                                && left_stats
                                    .missed_threshold_count
                                    .checked_add(right_stats.missed_threshold_count)
                                    .is_some()
                                && left_stats
                                    .after_threshold_count
                                    .checked_add(right_stats.after_threshold_count)
                                    .is_some()
                                && left_stats
                                    .replicated_count
                                    .checked_add(right_stats.replicated_count)
                                    .is_some()
                        })
                    })
            }
        };

        let tier_stats_fit = match (&self.all_tier_stats, &other.all_tier_stats) {
            (_, None) | (None, Some(_)) => true,
            (Some(left), Some(right)) => left.fits_merge(right),
        };

        if !scalar_counts_fit || !histograms_fit || !replication_fits || !tier_stats_fit {
            return false;
        }
        self.merge(other);
        true
    }
}

/// Read-only projection of the scanner's `.usage-cache.bin` info block.
///
/// The canonical wire format is written by the hand-written map-encoded
/// `Serialize` on the scanner-side `DataUsageCacheInfo`
/// (`crates/scanner/src/data_usage_define.rs`), which carries the original 16
/// fields plus an optional reconciliation field.
/// This type decodes only the shared subset and is deliberately not
/// `Serialize`: a derived (array) encoding of this 6-field subset would
/// corrupt the cache for scanner readers, so no write path may exist here.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct DataUsageCacheInfo {
    pub name: String,
    pub next_cycle: u64,
    pub last_update: Option<SystemTime>,
    pub skip_healing: bool,
    #[serde(default)]
    pub failed_objects: HashMap<String, u64>,
    /// Whether this per-set cache was produced by a completed scanner pass.
    ///
    /// Older cache writers omit this field and therefore deserialize as
    /// incomplete instead of exposing partial set totals as confirmed zeros.
    #[serde(default)]
    pub snapshot_complete: bool,
}

/// Prefix-level usage over a raw entry map — the shared core behind
/// [`DataUsageCache::prefix_usage`], usable by any cache-shaped reader (the
/// scanner's writer-side cache has the same map type).
///
/// Cache keys are cleaned literal paths (`bucket/pre/fix`), so sub-prefix
/// names come straight off the child keys — no reverse mapping exists or is
/// needed. A compacted prefix carries its aggregate but no children, which
/// the `compacted` flag reports so callers can say why the breakdown is
/// empty. `truncated` is set when the breakdown exceeded `max_entries` and
/// was cut (largest first).
pub fn prefix_usage_in_cache(
    cache: &HashMap<String, DataUsageEntry>,
    bucket: &str,
    prefix: &str,
    max_entries: usize,
) -> Option<PrefixUsageQuery> {
    let prefix = prefix.trim_matches('/');
    let root = if prefix.is_empty() {
        bucket.to_string()
    } else {
        format!("{bucket}/{prefix}")
    };
    let entry = cache.get(&hash_path(&root).key())?.clone();

    let usage = PrefixUsageSummary::from_entry(&flatten_entry(cache, &entry, 0)?);

    let child_prefix = format!("{root}/");
    let mut sub_prefixes: Vec<PrefixUsageEntry> = entry
        .children
        .iter()
        .filter_map(|child_key| {
            let child = cache.get(child_key)?;
            let child_flat = flatten_entry(cache, child, 1)?;
            // Child keys are literal `bucket/pre/name` paths; a trailing
            // slash marks a directory object and is display-only here.
            let name = child_key
                .strip_prefix(child_prefix.as_str())
                .unwrap_or(child_key.as_str())
                .trim_end_matches('/')
                .to_string();
            Some(PrefixUsageEntry {
                prefix: name,
                usage: PrefixUsageSummary::from_entry(&child_flat),
            })
        })
        .collect();
    sub_prefixes.sort_by(|left, right| {
        right
            .usage
            .size
            .cmp(&left.usage.size)
            .then_with(|| left.prefix.cmp(&right.prefix))
    });
    let truncated = sub_prefixes.len() > max_entries;
    sub_prefixes.truncate(max_entries);

    Some(PrefixUsageQuery {
        usage,
        compacted: entry.compacted,
        truncated,
        sub_prefixes,
    })
}

/// Maximum subtree depth [`flatten_entry`] will walk before declaring the
/// cache corrupt — the same bound the scanner's checked flatten uses.
const PREFIX_USAGE_MAX_DEPTH: usize = 1024;

/// Flatten one entry's subtree into an aggregate: the free-function twin of
/// [`DataUsageCache::flatten`], carrying the scanner checked-flatten
/// hardening so a corrupt cache (cycles, over-deep trees, overflowing
/// counters) yields `None` instead of unbounded recursion or wrapped totals.
fn flatten_entry(cache: &HashMap<String, DataUsageEntry>, root: &DataUsageEntry, depth: usize) -> Option<DataUsageEntry> {
    if depth > PREFIX_USAGE_MAX_DEPTH {
        return None;
    }
    let mut flattened = DataUsageEntry::default();
    if !flattened.checked_merge(root) {
        return None;
    }
    flattened.compacted = root.compacted;
    // The root itself is not pre-seeded: it is merged above, and a corrupt
    // child edge pointing back at the root's own key is still terminated by
    // the visited set on first encounter.
    let mut visited: HashSet<&str> = HashSet::new();
    let mut pending: Vec<(&String, usize)> = root.children.iter().map(|child| (child, depth + 1)).collect();
    while let Some((key, child_depth)) = pending.pop() {
        if child_depth > PREFIX_USAGE_MAX_DEPTH || !visited.insert(key.as_str()) {
            return None;
        }
        let entry = cache.get(key)?;
        if !flattened.checked_merge(entry) {
            return None;
        }
        pending.extend(entry.children.iter().map(|child| (child, child_depth + 1)));
    }
    flattened.children.clear();
    Some(flattened)
}

/// Flattened counters of one prefix subtree, as returned by
/// [`DataUsageCache::prefix_usage`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PrefixUsageSummary {
    pub size: u64,
    pub objects: u64,
    pub versions: u64,
    pub delete_markers: u64,
}

impl PrefixUsageSummary {
    fn from_entry(entry: &DataUsageEntry) -> Self {
        Self {
            size: entry.size as u64,
            objects: entry.objects as u64,
            versions: entry.versions as u64,
            delete_markers: entry.delete_markers as u64,
        }
    }

    /// Add another set's counters into this one (entries are partitioned by
    /// set, so per-set results sum).
    pub fn merge(&mut self, other: &Self) {
        self.size = self.size.saturating_add(other.size);
        self.objects = self.objects.saturating_add(other.objects);
        self.versions = self.versions.saturating_add(other.versions);
        self.delete_markers = self.delete_markers.saturating_add(other.delete_markers);
    }
}

/// One first-level sub-prefix row of a [`PrefixUsageQuery`].
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct PrefixUsageEntry {
    pub prefix: String,
    pub usage: PrefixUsageSummary,
}

/// Result of [`DataUsageCache::prefix_usage`].
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PrefixUsageQuery {
    pub usage: PrefixUsageSummary,
    /// The prefix entry was compacted by the scanner: its aggregate is valid
    /// but no sub-prefix breakdown exists on disk.
    pub compacted: bool,
    /// The breakdown had more entries than `max_entries`; the largest remain.
    pub truncated: bool,
    pub sub_prefixes: Vec<PrefixUsageEntry>,
}

/// Read-only projection of a scanner-written `.usage-cache.bin` file.
///
/// The scanner-side `DataUsageCache` (`crates/scanner/src/data_usage_define.rs`)
/// owns the persisted format; this type only decodes it (see
/// [`DataUsageCacheInfo`]) and must never grow a serialization path.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct DataUsageCache {
    pub info: DataUsageCacheInfo,
    pub cache: HashMap<String, DataUsageEntry>,
}

impl DataUsageCache {
    pub fn replace(&mut self, path: &str, parent: &str, e: DataUsageEntry) {
        let hash = hash_path(path);
        self.cache.insert(hash.key(), e);
        if !parent.is_empty() {
            let phash = hash_path(parent);
            let p = {
                let p = self.cache.entry(phash.key()).or_default();
                p.add_child(&hash);
                p.clone()
            };
            self.cache.insert(phash.key(), p);
        }
    }

    pub fn replace_hashed(&mut self, hash: &DataUsageHash, parent: &Option<DataUsageHash>, e: &DataUsageEntry) {
        self.cache.insert(hash.key(), e.clone());
        if let Some(parent) = parent {
            self.cache.entry(parent.key()).or_default().add_child(hash);
        }
    }

    pub fn find(&self, path: &str) -> Option<DataUsageEntry> {
        self.cache.get(&hash_path(path).key()).cloned()
    }

    pub fn find_children_copy(&mut self, h: DataUsageHash) -> DataUsageHashMap {
        self.cache.entry(h.string()).or_default().children.clone()
    }

    pub fn flatten(&self, root: &DataUsageEntry) -> DataUsageEntry {
        let mut root = root.clone();
        for id in root.children.clone().iter() {
            if let Some(e) = self.cache.get(id) {
                let mut e = e.clone();
                if !e.children.is_empty() {
                    e = self.flatten(&e);
                }
                root.merge(&e);
            }
        }
        root.children.clear();
        root
    }

    pub fn copy_with_children(&mut self, src: &DataUsageCache, hash: &DataUsageHash, parent: &Option<DataUsageHash>) {
        if let Some(e) = src.cache.get(&hash.string()) {
            self.cache.insert(hash.key(), e.clone());
            for ch in e.children.iter() {
                if *ch == hash.key() {
                    return;
                }
                self.copy_with_children(src, &DataUsageHash(ch.to_string()), &Some(hash.clone()));
            }
            if let Some(parent) = parent {
                let p = self.cache.entry(parent.key()).or_default();
                p.add_child(hash);
            }
        }
    }

    pub fn delete_recursive(&mut self, hash: &DataUsageHash) {
        let mut need_remove = Vec::new();
        if let Some(v) = self.cache.get(&hash.string()) {
            for child in v.children.iter() {
                need_remove.push(child.clone());
            }
        }
        self.cache.remove(&hash.string());
        need_remove.iter().for_each(|child| {
            self.delete_recursive(&DataUsageHash(child.to_string()));
        });
    }

    pub fn size_recursive(&self, path: &str) -> Option<DataUsageEntry> {
        match self.find(path) {
            Some(root) => {
                if root.children.is_empty() {
                    return Some(root);
                }
                let mut flat = self.flatten(&root);
                if flat.replication_stats.as_ref().is_some_and(ReplicationAllStats::is_empty) {
                    flat.replication_stats = None;
                }
                Some(flat)
            }
            None => None,
        }
    }

    pub fn search_parent(&self, hash: &DataUsageHash) -> Option<DataUsageHash> {
        let want = hash.key();
        if let Some(last_index) = want.rfind('/')
            && let Some(v) = self.find(&want[0..last_index])
            && v.children.contains(&want)
        {
            let found = hash_path(&want[0..last_index]);
            return Some(found);
        }

        for (k, v) in self.cache.iter() {
            if v.children.contains(&want) {
                let found = DataUsageHash(k.clone());
                return Some(found);
            }
        }
        None
    }

    pub fn is_compacted(&self, hash: &DataUsageHash) -> bool {
        match self.cache.get(&hash.key()) {
            Some(due) => due.compacted,
            None => false,
        }
    }

    /// Prefix-level usage for one bucket subtree, plus the one-level
    /// breakdown below it (rustfs/backlog#1872, MinIO
    /// `loadPrefixUsageFromBackend` parity and beyond: arbitrary prefixes and
    /// full counters instead of first-level sizes only).
    ///
    /// Cache keys are cleaned literal paths (`bucket/pre/fix`), so sub-prefix
    /// names come straight off the child keys — no reverse mapping exists or
    /// is needed. A compacted prefix carries its aggregate but no children,
    /// which the `compacted` flag reports so callers can say why the
    /// breakdown is empty. `truncated` is set when the breakdown exceeded
    /// `max_entries` and was cut (largest first).
    pub fn prefix_usage(&self, bucket: &str, prefix: &str, max_entries: usize) -> Option<PrefixUsageQuery> {
        prefix_usage_in_cache(&self.cache, bucket, prefix, max_entries)
    }

    pub fn force_compact(&mut self, limit: usize) {
        if self.cache.len() < limit {
            return;
        }
        let top = hash_path(&self.info.name).key();
        let top_e = match self.find(&top) {
            Some(e) => e,
            None => return,
        };
        // Note: DATA_SCANNER_FORCE_COMPACT_AT_FOLDERS constant would need to be passed as parameter
        // or defined in common crate if needed
        if top_e.children.len() > 250_000 {
            // DATA_SCANNER_FORCE_COMPACT_AT_FOLDERS
            self.reduce_children_of(&hash_path(&self.info.name), limit, true);
        }
        if self.cache.len() <= limit {
            return;
        }

        let mut found = HashSet::new();
        found.insert(top);
        mark(self, &top_e, &mut found);
        self.cache.retain(|k, _| {
            if !found.contains(k) {
                return false;
            }
            true
        });
    }

    pub fn reduce_children_of(&mut self, path: &DataUsageHash, limit: usize, compact_self: bool) {
        let e = match self.cache.get(&path.key()) {
            Some(e) => e,
            None => return,
        };

        if e.compacted {
            return;
        }

        if e.children.len() > limit && compact_self {
            let mut flat = self.size_recursive(&path.key()).unwrap_or_default();
            flat.compacted = true;
            self.delete_recursive(path);
            self.replace_hashed(path, &None, &flat);
            return;
        }
        let total = self.total_children_rec(&path.key());
        if total < limit {
            return;
        }

        let mut candidates = Vec::new();
        let mut remove = total - limit;
        add(self, path, &mut candidates);
        candidates.sort_by_key(|a| a.objects);

        let mut candidate_index = 0;
        while remove > 0 && candidate_index < candidates.len() {
            let e = &candidates[candidate_index];
            let candidate = e.path.clone();
            if candidate == *path && !compact_self {
                break;
            }
            let removing = self.total_children_rec(&candidate.key());
            let mut flat = match self.size_recursive(&candidate.key()) {
                Some(flat) => flat,
                None => {
                    candidate_index += 1;
                    continue;
                }
            };

            flat.compacted = true;
            self.delete_recursive(&candidate);
            self.replace_hashed(&candidate, &None, &flat);

            remove = remove.saturating_sub(removing);
            candidate_index += 1;
        }
    }

    pub fn total_children_rec(&self, path: &str) -> usize {
        let Some(root) = self.find(path) else {
            return 0;
        };
        if root.children.is_empty() {
            return 0;
        }

        let mut n = root.children.len();
        for ch in root.children.iter() {
            n += self.total_children_rec(ch);
        }
        n
    }

    pub fn merge(&mut self, o: &DataUsageCache) {
        let Some(mut existing_root) = self.root() else {
            if o.root().is_none() {
                return;
            }
            *self = o.clone();
            return;
        };

        let Some(other_root) = o.root() else {
            return;
        };

        if o.info.last_update > self.info.last_update {
            self.info.last_update = o.info.last_update;
        }

        existing_root.merge(&other_root);
        self.cache.insert(hash_path(&self.info.name).key(), existing_root);

        let root_hash = self.root_hash();
        for key in other_root.children.iter() {
            let Some(entry) = o.cache.get(key) else {
                continue;
            };
            let flat = o.flatten(entry);
            if let Some(existing) = self.cache.get_mut(key) {
                existing.merge(&flat);
            } else {
                self.replace_hashed(&DataUsageHash(key.clone()), &Some(root_hash.clone()), &flat);
            }
        }
    }

    pub fn root_hash(&self) -> DataUsageHash {
        hash_path(&self.info.name)
    }

    pub fn root(&self) -> Option<DataUsageEntry> {
        self.find(&self.info.name)
    }

    /// Convert cache to DataUsageInfo for a specific path
    pub fn dui(&self, path: &str, buckets: &[String]) -> DataUsageInfo {
        let e = match self.find(path) {
            Some(e) => e,
            None => return DataUsageInfo::default(),
        };
        let flat = self.flatten(&e);

        let mut buckets_usage = HashMap::new();
        for bucket_name in buckets.iter() {
            let e = match self.find(bucket_name) {
                Some(e) => e,
                None => continue,
            };
            let flat = self.flatten(&e);
            let mut bui = BucketUsageInfo {
                size: flat.size as u64,
                versions_count: flat.versions as u64,
                objects_count: flat.objects as u64,
                delete_markers_count: flat.delete_markers as u64,
                object_size_histogram: flat.obj_sizes.to_map(),
                object_versions_histogram: flat.obj_versions.to_map(),
                ..Default::default()
            };

            if let Some(rs) = &flat.replication_stats {
                bui.replica_size = rs.replica_size;
                bui.replica_count = rs.replica_count;

                for (arn, stat) in rs.targets.iter() {
                    bui.replication_info.insert(
                        arn.clone(),
                        BucketTargetUsageInfo {
                            replication_pending_size: stat.pending_size,
                            replicated_size: stat.replicated_size,
                            replication_failed_size: stat.failed_size,
                            replication_pending_count: stat.pending_count,
                            replication_failed_count: stat.failed_count,
                            replicated_count: stat.replicated_count,
                            ..Default::default()
                        },
                    );
                }
            }
            buckets_usage.insert(bucket_name.clone(), bui);
        }

        DataUsageInfo {
            last_update: self.info.last_update,
            objects_total_count: flat.objects as u64,
            versions_total_count: flat.versions as u64,
            delete_markers_total_count: flat.delete_markers as u64,
            objects_total_size: flat.size as u64,
            tier_stats: flat.all_tier_stats.filter(|tiers| !tiers.is_empty()),
            buckets_count: u64::try_from(buckets.len()).unwrap_or(u64::MAX),
            buckets_usage,
            usage_snapshot_complete: self.info.snapshot_complete,
            ..Default::default()
        }
    }

    pub fn unmarshal(buf: &[u8]) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let t: Self = rmp_serde::from_slice(buf)?;
        Ok(t)
    }
}

// Helper structs and functions for cache operations
#[derive(Default, Clone)]
struct Inner {
    objects: usize,
    path: DataUsageHash,
}

fn add(data_usage_cache: &DataUsageCache, path: &DataUsageHash, candidates: &mut Vec<Inner>) -> usize {
    let e = match data_usage_cache.cache.get(&path.key()) {
        Some(e) => e,
        None => return 0,
    };
    let mut objects = e.objects;
    for ch in e.children.iter() {
        objects += add(data_usage_cache, &DataUsageHash(ch.clone()), candidates);
    }
    // Collect internal nodes (with children) as compaction candidates.
    // Leaf nodes have no children to remove, so compacting them is a no-op.
    if !e.children.is_empty() {
        candidates.push(Inner {
            objects,
            path: path.clone(),
        });
    }
    objects
}

fn mark(duc: &DataUsageCache, entry: &DataUsageEntry, found: &mut HashSet<String>) {
    for k in entry.children.iter() {
        found.insert(k.to_string());
        if let Some(ch) = duc.cache.get(k) {
            mark(duc, ch, found);
        }
    }
}

fn clean_data_usage_path(data: &str) -> String {
    let rooted = data.starts_with('/');
    let mut parts = Vec::new();

    for part in data.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if parts.last().is_some_and(|last| *last != "..") {
                    parts.pop();
                } else if !rooted {
                    parts.push(part);
                }
            }
            _ => parts.push(part),
        }
    }

    let clean = parts.join("/");
    match (rooted, clean.is_empty()) {
        (true, true) => "/".to_string(),
        (true, false) => format!("/{clean}"),
        (false, true) => ".".to_string(),
        (false, false) => clean,
    }
}

/// Hash a slash-separated path for data usage caching.
///
/// Cache identifiers are persisted and exchanged across nodes, so their
/// normalization must not depend on the host operating system.
pub fn hash_path(data: &str) -> DataUsageHash {
    DataUsageHash(clean_data_usage_path(data))
}

impl DataUsageInfo {
    /// Create a new DataUsageInfo
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether this snapshot authoritatively covers every reported bucket.
    pub fn is_complete_bucket_usage_snapshot(&self) -> bool {
        self.usage_snapshot_complete
            && self.last_update.is_some()
            && u64::try_from(self.buckets_usage.len()).ok() == Some(self.buckets_count)
    }

    /// Validate provenance before an observational view can be selected for
    /// admin display. Partial data is accepted only with unique set states,
    /// a plan digest for every state, and at least one usable generation.
    pub fn is_valid_partial_snapshot(&self) -> bool {
        if !self.usage_snapshot_partial
            || self.usage_snapshot_converged != Some(false)
            || self.last_update.is_none()
            || self.scanner_cycle.is_none()
            || self.scanner_epoch.is_none()
            || self.usage_snapshot_set_states.is_empty()
            || u64::try_from(self.buckets_usage.len()).ok() != Some(self.buckets_count)
        {
            return false;
        }

        let mut previous = None;
        let mut plan_digest = None;
        let mut has_source = false;
        for state in &self.usage_snapshot_set_states {
            if state.scan_plan_digest.is_none()
                || plan_digest.is_some_and(|digest| Some(digest) != state.scan_plan_digest)
                || state.scanner_cycle.is_some() != state.scanner_epoch.is_some()
                || previous.is_some_and(|(pool, set)| (pool, set) >= (state.pool_index, state.set_index))
            {
                return false;
            }
            previous = Some((state.pool_index, state.set_index));
            plan_digest = state.scan_plan_digest;
            has_source |= state.scanner_cycle.is_some() && !state.tombstone;
        }
        has_source
    }

    /// Add object metadata to data usage statistics
    pub fn add_object(&mut self, object_path: &str, meta_object: &rustfs_filemeta::MetaObject) {
        // This method is kept for backward compatibility
        // For accurate version counting, use add_object_from_file_meta instead
        let bucket_name = match self.extract_bucket_from_path(object_path) {
            Ok(name) => name,
            Err(_) => return,
        };

        // Update bucket statistics
        if let Some(bucket_usage) = self.buckets_usage.get_mut(&bucket_name) {
            bucket_usage.size += meta_object.size as u64;
            bucket_usage.objects_count += 1;
            bucket_usage.versions_count += 1; // Simplified: assume 1 version per object

            // Update size histogram
            let total_size = meta_object.size as u64;
            let size_ranges = [
                ("0-1KB", 0, 1024),
                ("1KB-1MB", 1024, 1024 * 1024),
                ("1MB-10MB", 1024 * 1024, 10 * 1024 * 1024),
                ("10MB-100MB", 10 * 1024 * 1024, 100 * 1024 * 1024),
                ("100MB-1GB", 100 * 1024 * 1024, 1024 * 1024 * 1024),
                ("1GB+", 1024 * 1024 * 1024, u64::MAX),
            ];

            for (range_name, min_size, max_size) in size_ranges {
                if total_size >= min_size && total_size < max_size {
                    *bucket_usage.object_size_histogram.entry(range_name.to_string()).or_insert(0) += 1;
                    break;
                }
            }

            // Update version histogram (simplified - count as single version)
            *bucket_usage
                .object_versions_histogram
                .entry("SINGLE_VERSION".to_string())
                .or_insert(0) += 1;
        } else {
            // Create new bucket usage
            let mut bucket_usage = BucketUsageInfo {
                size: meta_object.size as u64,
                objects_count: 1,
                versions_count: 1,
                ..Default::default()
            };
            bucket_usage.object_size_histogram.insert("0-1KB".to_string(), 1);
            bucket_usage.object_versions_histogram.insert("SINGLE_VERSION".to_string(), 1);
            self.buckets_usage.insert(bucket_name, bucket_usage);
        }

        // Update global statistics
        self.objects_total_size += meta_object.size as u64;
        self.objects_total_count += 1;
        self.versions_total_count += 1;
    }

    /// Add object from FileMeta for accurate version counting
    pub fn add_object_from_file_meta(&mut self, object_path: &str, file_meta: &rustfs_filemeta::FileMeta) {
        let bucket_name = match self.extract_bucket_from_path(object_path) {
            Ok(name) => name,
            Err(_) => return,
        };

        // Calculate accurate statistics from all versions
        let mut total_size = 0u64;
        let mut versions_count = 0u64;
        let mut delete_markers_count = 0u64;
        let mut latest_object_size = 0u64;

        // Process all versions to get accurate counts
        for version in &file_meta.versions {
            match rustfs_filemeta::FileMetaVersion::try_from(version.clone()) {
                Ok(ver) => {
                    if let Some(obj) = ver.object {
                        total_size += obj.size as u64;
                        versions_count += 1;
                        latest_object_size = obj.size as u64; // Keep track of latest object size
                    } else if ver.delete_marker.is_some() {
                        delete_markers_count += 1;
                    }
                }
                Err(_) => {
                    // Skip invalid versions
                    continue;
                }
            }
        }

        // Update bucket statistics
        if let Some(bucket_usage) = self.buckets_usage.get_mut(&bucket_name) {
            bucket_usage.size += total_size;
            bucket_usage.objects_count += 1;
            bucket_usage.versions_count += versions_count;
            bucket_usage.delete_markers_count += delete_markers_count;

            // Update size histogram based on latest object size
            let size_ranges = [
                ("0-1KB", 0, 1024),
                ("1KB-1MB", 1024, 1024 * 1024),
                ("1MB-10MB", 1024 * 1024, 10 * 1024 * 1024),
                ("10MB-100MB", 10 * 1024 * 1024, 100 * 1024 * 1024),
                ("100MB-1GB", 100 * 1024 * 1024, 1024 * 1024 * 1024),
                ("1GB+", 1024 * 1024 * 1024, u64::MAX),
            ];

            for (range_name, min_size, max_size) in size_ranges {
                if latest_object_size >= min_size && latest_object_size < max_size {
                    *bucket_usage.object_size_histogram.entry(range_name.to_string()).or_insert(0) += 1;
                    break;
                }
            }

            // Update version histogram based on actual version count
            let version_ranges = [
                ("1", 1, 1),
                ("2-5", 2, 5),
                ("6-10", 6, 10),
                ("11-50", 11, 50),
                ("51-100", 51, 100),
                ("100+", 101, usize::MAX),
            ];

            for (range_name, min_versions, max_versions) in version_ranges {
                if versions_count as usize >= min_versions && versions_count as usize <= max_versions {
                    *bucket_usage
                        .object_versions_histogram
                        .entry(range_name.to_string())
                        .or_insert(0) += 1;
                    break;
                }
            }
        } else {
            // Create new bucket usage
            let mut bucket_usage = BucketUsageInfo {
                size: total_size,
                objects_count: 1,
                versions_count,
                delete_markers_count,
                ..Default::default()
            };

            // Set size histogram
            let size_ranges = [
                ("0-1KB", 0, 1024),
                ("1KB-1MB", 1024, 1024 * 1024),
                ("1MB-10MB", 1024 * 1024, 10 * 1024 * 1024),
                ("10MB-100MB", 10 * 1024 * 1024, 100 * 1024 * 1024),
                ("100MB-1GB", 100 * 1024 * 1024, 1024 * 1024 * 1024),
                ("1GB+", 1024 * 1024 * 1024, u64::MAX),
            ];

            for (range_name, min_size, max_size) in size_ranges {
                if latest_object_size >= min_size && latest_object_size < max_size {
                    bucket_usage.object_size_histogram.insert(range_name.to_string(), 1);
                    break;
                }
            }

            // Set version histogram
            let version_ranges = [
                ("1", 1, 1),
                ("2-5", 2, 5),
                ("6-10", 6, 10),
                ("11-50", 11, 50),
                ("51-100", 51, 100),
                ("100+", 101, usize::MAX),
            ];

            for (range_name, min_versions, max_versions) in version_ranges {
                if versions_count as usize >= min_versions && versions_count as usize <= max_versions {
                    bucket_usage.object_versions_histogram.insert(range_name.to_string(), 1);
                    break;
                }
            }

            self.buckets_usage.insert(bucket_name, bucket_usage);
            // Update buckets count when adding new bucket
            self.buckets_count = self.buckets_usage.len() as u64;
        }

        // Update global statistics
        self.objects_total_size += total_size;
        self.objects_total_count += 1;
        self.versions_total_count += versions_count;
        self.delete_markers_total_count += delete_markers_count;
    }

    /// Extract bucket name from object path
    pub fn extract_bucket_from_path(&self, object_path: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let parts: Vec<&str> = object_path.split('/').collect();
        if parts.is_empty() {
            return Err("Invalid object path: empty".into());
        }
        Ok(parts[0].to_string())
    }

    /// Update capacity information
    pub fn update_capacity(&mut self, total: u64, used: u64, free: u64) {
        self.total_capacity = total;
        self.total_used_capacity = used;
        self.total_free_capacity = free;
        self.last_update = Some(SystemTime::now());
    }

    /// Add bucket usage info
    pub fn add_bucket_usage(&mut self, bucket: String, usage: BucketUsageInfo) {
        self.buckets_usage.insert(bucket, usage);
        self.buckets_count = self.buckets_usage.len() as u64;
        self.last_update = Some(SystemTime::now());
    }

    /// Get bucket usage info
    pub fn get_bucket_usage(&self, bucket: &str) -> Option<&BucketUsageInfo> {
        self.buckets_usage.get(bucket)
    }

    /// Calculate total statistics from all buckets
    pub fn calculate_totals(&mut self) {
        self.objects_total_count = 0;
        self.versions_total_count = 0;
        self.delete_markers_total_count = 0;
        self.objects_total_size = 0;

        for usage in self.buckets_usage.values() {
            self.objects_total_count += usage.objects_count;
            self.versions_total_count += usage.versions_count;
            self.delete_markers_total_count += usage.delete_markers_count;
            self.objects_total_size += usage.size;
        }
    }

    /// Merge another DataUsageInfo into this one
    pub fn merge(&mut self, other: &DataUsageInfo) {
        // Merge bucket usage
        for (bucket, usage) in &other.buckets_usage {
            if let Some(existing) = self.buckets_usage.get_mut(bucket) {
                existing.merge(usage);
            } else {
                self.buckets_usage.insert(bucket.clone(), usage.clone());
            }
        }

        self.disk_usage_status.extend(other.disk_usage_status.iter().cloned());

        // Recalculate totals
        self.calculate_totals();

        // Ensure buckets_count stays consistent with buckets_usage
        self.buckets_count = self.buckets_usage.len() as u64;

        // Update last update time
        if let Some(other_update) = other.last_update {
            match self.last_update {
                None => self.last_update = Some(other_update),
                Some(self_update) if other_update > self_update => self.last_update = Some(other_update),
                _ => {}
            }
        }
    }
}

impl BucketUsageInfo {
    /// Create a new BucketUsageInfo
    pub fn new() -> Self {
        Self::default()
    }

    /// Add size summary to this bucket usage
    /// Merge another BucketUsageInfo into this one
    pub fn merge(&mut self, other: &BucketUsageInfo) {
        self.size += other.size;
        self.objects_count += other.objects_count;
        self.versions_count += other.versions_count;
        self.delete_markers_count += other.delete_markers_count;
        self.replica_size += other.replica_size;
        self.replica_count += other.replica_count;

        // Merge histograms
        for (key, value) in &other.object_size_histogram {
            *self.object_size_histogram.entry(key.clone()).or_insert(0) += value;
        }

        for (key, value) in &other.object_versions_histogram {
            *self.object_versions_histogram.entry(key.clone()).or_insert(0) += value;
        }

        // Merge replication info
        for (target, info) in &other.replication_info {
            let entry = self.replication_info.entry(target.clone()).or_default();
            entry.replicated_size += info.replicated_size;
            entry.replica_size += info.replica_size;
            entry.replication_pending_size += info.replication_pending_size;
            entry.replication_failed_size += info.replication_failed_size;
            entry.replication_pending_count += info.replication_pending_count;
            entry.replication_failed_count += info.replication_failed_count;
            entry.replicated_count += info.replicated_count;
        }

        // Merge backward compatibility fields
        self.replication_pending_size_v1 += other.replication_pending_size_v1;
        self.replication_failed_size_v1 += other.replication_failed_size_v1;
        self.replicated_size_v1 += other.replicated_size_v1;
        self.replication_pending_count_v1 += other.replication_pending_count_v1;
        self.replication_failed_count_v1 += other.replication_failed_count_v1;
    }
}

impl SizeSummary {
    /// Create a new SizeSummary
    pub fn new() -> Self {
        Self::default()
    }

    /// Add another SizeSummary to this one.
    ///
    /// Saturating throughout: a scan that overflows a counter should report the
    /// ceiling rather than panic in a debug build or wrap in a release one.
    pub fn add(&mut self, other: &SizeSummary) {
        self.total_size = self.total_size.saturating_add(other.total_size);
        self.versions = self.versions.saturating_add(other.versions);
        self.delete_markers = self.delete_markers.saturating_add(other.delete_markers);
        self.replicated_size = self.replicated_size.saturating_add(other.replicated_size);
        self.replicated_count = self.replicated_count.saturating_add(other.replicated_count);
        self.pending_size = self.pending_size.saturating_add(other.pending_size);
        self.failed_size = self.failed_size.saturating_add(other.failed_size);
        self.replica_size = self.replica_size.saturating_add(other.replica_size);
        self.replica_count = self.replica_count.saturating_add(other.replica_count);
        self.pending_count = self.pending_count.saturating_add(other.pending_count);
        self.failed_count = self.failed_count.saturating_add(other.failed_count);

        // Merge replication target stats
        for (target, stats) in &other.repl_target_stats {
            let entry = self.repl_target_stats.entry(target.clone()).or_default();
            entry.replicated_size = entry.replicated_size.saturating_add(stats.replicated_size);
            entry.replicated_count = entry.replicated_count.saturating_add(stats.replicated_count);
            entry.pending_size = entry.pending_size.saturating_add(stats.pending_size);
            entry.failed_size = entry.failed_size.saturating_add(stats.failed_size);
            entry.pending_count = entry.pending_count.saturating_add(stats.pending_count);
            entry.failed_count = entry.failed_count.saturating_add(stats.failed_count);
        }

        for entry in &other.size_reconciliation {
            self.record_size_reconciliation(entry.clone());
        }
        self.size_reconciliation_truncated |= other.size_reconciliation_truncated;
        for scope in &other.reconciliation_scopes {
            self.record_reconciliation_scope(&scope.bucket, &scope.object);
        }
    }

    /// Add one reconciliation debt, coalescing repeated observations in the
    /// same object summary. The scanner cache applies its own larger bound.
    pub fn record_size_reconciliation(&mut self, entry: SizeReconciliationEntry) {
        const MAX_SUMMARY_RECONCILIATION_ENTRIES: usize = 1024;
        if let Some(existing) = self.size_reconciliation.iter_mut().find(|value| value.key == entry.key) {
            existing.reason = entry.reason;
            existing.physical_size = entry.physical_size;
            existing.generation = entry.generation;
            existing.version_id = entry.version_id;
            return;
        }
        if self.size_reconciliation.len() < MAX_SUMMARY_RECONCILIATION_ENTRIES {
            self.size_reconciliation.push(entry);
        } else {
            self.size_reconciliation_truncated = true;
        }
    }

    /// Mark one object scope as refreshed. Duplicate scopes are suppressed so
    /// merging summaries remains bounded and deterministic.
    pub fn record_reconciliation_scope(&mut self, bucket: &str, object: &str) {
        if !self
            .reconciliation_scopes
            .iter()
            .any(|scope| scope.bucket == bucket && scope.object == object)
        {
            if self.reconciliation_scopes.len() >= 1024 {
                self.size_reconciliation_truncated = true;
                return;
            }
            self.reconciliation_scopes.push(SizeReconciliationScope {
                bucket: bucket.to_string(),
                object: object.to_string(),
            });
        }
    }
}

/// Aggregated compression metrics: original size, compressed size, and operation count.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct CompressionTotalInfo {
    // Total bytes before compression since compression is used.
    pub original_bytes_total: u64,
    // Total bytes after compression since compression is used.
    pub compressed_bytes_total: u64,
    // Total number of compression operations since compression is used.
    pub compression_operations_total: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Deserialize)]
    struct LegacyUsageReader {
        buckets_count: u64,
    }

    fn tier_entry(tier: &str, stats: TierStats) -> DataUsageEntry {
        let mut entry = DataUsageEntry::default();
        entry.add_tier_sizes(&HashMap::from([(tier.to_string(), stats)]));
        entry
    }

    #[test]
    fn tier_stats_survive_entry_merge() {
        let mut left = tier_entry(
            "WARM",
            TierStats {
                total_size: 10,
                num_versions: 2,
                num_objects: 1,
            },
        );
        let mut right = tier_entry(
            "WARM",
            TierStats {
                total_size: 5,
                num_versions: 1,
                num_objects: 1,
            },
        );
        right.add_tier_sizes(&HashMap::from([(
            "COLD".to_string(),
            TierStats {
                total_size: 7,
                num_versions: 1,
                num_objects: 0,
            },
        )]));

        assert!(left.checked_merge(&right), "merging exact tier totals must be accepted");

        let tiers = &left.all_tier_stats.expect("merged entry keeps tier stats").tiers;
        assert_eq!(
            tiers.get("WARM"),
            Some(&TierStats {
                total_size: 15,
                num_versions: 3,
                num_objects: 2,
            })
        );
        assert_eq!(
            tiers.get("COLD"),
            Some(&TierStats {
                total_size: 7,
                num_versions: 1,
                num_objects: 0,
            })
        );
    }

    #[test]
    fn tier_stats_merge_into_an_untiered_entry() {
        let mut left = DataUsageEntry::default();
        let right = tier_entry(
            "WARM",
            TierStats {
                total_size: 10,
                num_versions: 1,
                num_objects: 1,
            },
        );

        assert!(left.checked_merge(&right));

        assert_eq!(
            left.all_tier_stats.expect("tier stats adopted from the merged entry").tiers["WARM"],
            TierStats {
                total_size: 10,
                num_versions: 1,
                num_objects: 1,
            }
        );
    }

    #[test]
    fn checked_merge_rejects_overflowing_tier_totals() {
        let mut left = tier_entry(
            "WARM",
            TierStats {
                total_size: u64::MAX,
                num_versions: 1,
                num_objects: 1,
            },
        );
        let right = tier_entry(
            "WARM",
            TierStats {
                total_size: 1,
                num_versions: 1,
                num_objects: 1,
            },
        );

        assert!(!left.checked_merge(&right), "saturating tier totals must not be published");
        assert_eq!(left.all_tier_stats.expect("left is untouched").tiers["WARM"].total_size, u64::MAX);
    }

    /// Entry shape released before per-tier accounting, using the derived
    /// (array) encoding those writers produced.
    #[derive(Serialize, Deserialize)]
    struct LegacyEntry {
        children: DataUsageHashMap,
        size: usize,
        objects: usize,
        versions: usize,
        delete_markers: usize,
        obj_sizes: SizeHistogram,
        obj_versions: VersionsHistogram,
        replication_stats: Option<ReplicationAllStats>,
        compacted: bool,
        #[serde(default)]
        failed_objects: usize,
    }

    #[test]
    fn entries_are_map_encoded_so_appended_fields_stay_readable() {
        // A derived (array) encoding turns every appended field into a decode
        // error for readers built before it existed, which would cost a mixed
        // -version cluster its whole scan cache. Entries must stay map-encoded.
        let current = tier_entry(
            "WARM",
            TierStats {
                total_size: 3,
                num_versions: 1,
                num_objects: 1,
            },
        );
        let mut encoded = Vec::new();
        current
            .serialize(&mut rmp_serde::Serializer::new(&mut encoded))
            .expect("encode current entry");

        let legacy: LegacyEntry = rmp_serde::from_slice(&encoded).expect("legacy reader should ignore the appended field");
        assert_eq!(legacy.objects, 0);
    }

    #[test]
    fn legacy_array_encoded_entries_still_load() {
        let legacy = LegacyEntry {
            children: DataUsageHashMap::default(),
            size: 12,
            objects: 3,
            versions: 4,
            delete_markers: 1,
            obj_sizes: SizeHistogram::default(),
            obj_versions: VersionsHistogram::default(),
            replication_stats: None,
            compacted: false,
            failed_objects: 2,
        };
        let mut encoded = Vec::new();
        legacy
            .serialize(&mut rmp_serde::Serializer::new(&mut encoded))
            .expect("encode legacy entry");

        let decoded: DataUsageEntry = rmp_serde::from_slice(&encoded).expect("current reader should default the missing field");

        assert_eq!(decoded.size, 12);
        assert_eq!(decoded.failed_objects, 2);
        assert!(decoded.all_tier_stats.is_none());
    }

    /// Scanner-written `.usage-cache.bin` bytes: a 2-element array of the
    /// canonical 16-field map-encoded info block and one map-encoded entry.
    /// Captured from the canonical writer's `marshal_msg` — see
    /// `usage_cache_wire_format_is_pinned` in
    /// `crates/scanner/src/data_usage_define.rs`, which pins these exact
    /// bytes and documents regeneration. Hardcoded here because a
    /// dev-dependency on rustfs-scanner would pull the whole ecstore tree
    /// into this crate's test build, and a fixture generated at test runtime
    /// could not detect writer drift anyway.
    const SCANNER_USAGE_CACHE_WIRE_FIXTURE: &[u8] = &[
        0x92, 0xde, 0x00, 0x10, 0xa4, 0x6e, 0x61, 0x6d, 0x65, 0xab, 0x77, 0x69, 0x72, 0x65, 0x2d, 0x62, 0x75, 0x63, 0x6b, 0x65,
        0x74, 0xaa, 0x6e, 0x65, 0x78, 0x74, 0x5f, 0x63, 0x79, 0x63, 0x6c, 0x65, 0x07, 0xac, 0x6c, 0x65, 0x61, 0x64, 0x65, 0x72,
        0x5f, 0x65, 0x70, 0x6f, 0x63, 0x68, 0x09, 0xab, 0x6c, 0x61, 0x73, 0x74, 0x5f, 0x75, 0x70, 0x64, 0x61, 0x74, 0x65, 0x92,
        0xce, 0x65, 0x53, 0xf1, 0x00, 0x00, 0xac, 0x73, 0x6b, 0x69, 0x70, 0x5f, 0x68, 0x65, 0x61, 0x6c, 0x69, 0x6e, 0x67, 0xc3,
        0xa9, 0x6c, 0x69, 0x66, 0x65, 0x63, 0x79, 0x63, 0x6c, 0x65, 0xc0, 0xab, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74,
        0x69, 0x6f, 0x6e, 0xc0, 0xae, 0x66, 0x61, 0x69, 0x6c, 0x65, 0x64, 0x5f, 0x6f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x73, 0x81,
        0xb0, 0x77, 0x69, 0x72, 0x65, 0x2d, 0x62, 0x75, 0x63, 0x6b, 0x65, 0x74, 0x2f, 0x6c, 0x6f, 0x73, 0x74, 0x0b, 0xb1, 0x73,
        0x63, 0x61, 0x6e, 0x5f, 0x72, 0x65, 0x73, 0x75, 0x6d, 0x65, 0x5f, 0x61, 0x66, 0x74, 0x65, 0x72, 0xb2, 0x77, 0x69, 0x72,
        0x65, 0x2d, 0x62, 0x75, 0x63, 0x6b, 0x65, 0x74, 0x2f, 0x72, 0x65, 0x73, 0x75, 0x6d, 0x65, 0xaf, 0x73, 0x63, 0x61, 0x6e,
        0x5f, 0x63, 0x68, 0x65, 0x63, 0x6b, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0xc0, 0xad, 0x70, 0x65, 0x6e, 0x64, 0x69, 0x6e, 0x67,
        0x5f, 0x68, 0x65, 0x61, 0x6c, 0x73, 0x91, 0x9a, 0xa6, 0x6f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0xab, 0x77, 0x69, 0x72, 0x65,
        0x2d, 0x62, 0x75, 0x63, 0x6b, 0x65, 0x74, 0xa6, 0x62, 0x72, 0x6f, 0x6b, 0x65, 0x6e, 0xc0, 0x01, 0x64, 0xcc, 0xc8, 0x03,
        0xa8, 0x64, 0x65, 0x66, 0x65, 0x72, 0x72, 0x65, 0x64, 0xa6, 0x62, 0x75, 0x64, 0x67, 0x65, 0x74, 0xab, 0x6f, 0x62, 0x6a,
        0x65, 0x63, 0x74, 0x5f, 0x6c, 0x6f, 0x63, 0x6b, 0xc0, 0xa6, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x92, 0x01, 0x02, 0xb1,
        0x73, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x5f, 0x63, 0x6f, 0x6d, 0x70, 0x6c, 0x65, 0x74, 0x65, 0xc3, 0xb0, 0x73,
        0x63, 0x61, 0x6e, 0x5f, 0x70, 0x6c, 0x61, 0x6e, 0x5f, 0x64, 0x69, 0x67, 0x65, 0x73, 0x74, 0xdc, 0x00, 0x20, 0x03, 0x03,
        0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03,
        0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0xb0, 0x63, 0x61, 0x63, 0x68, 0x65, 0x5f, 0x6b, 0x65, 0x79,
        0x5f, 0x66, 0x6f, 0x72, 0x6d, 0x61, 0x74, 0x01, 0x81, 0xab, 0x77, 0x69, 0x72, 0x65, 0x2d, 0x62, 0x75, 0x63, 0x6b, 0x65,
        0x74, 0x8b, 0xa8, 0x63, 0x68, 0x69, 0x6c, 0x64, 0x72, 0x65, 0x6e, 0x90, 0xa4, 0x73, 0x69, 0x7a, 0x65, 0xcd, 0x10, 0x00,
        0xa7, 0x6f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x73, 0x03, 0xa8, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x73, 0x05, 0xae,
        0x64, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x5f, 0x6d, 0x61, 0x72, 0x6b, 0x65, 0x72, 0x73, 0x01, 0xa9, 0x6f, 0x62, 0x6a, 0x5f,
        0x73, 0x69, 0x7a, 0x65, 0x73, 0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xac, 0x6f, 0x62,
        0x6a, 0x5f, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x73, 0x97, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xb1, 0x72,
        0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x73, 0x74, 0x61, 0x74, 0x73, 0xc0, 0xa9, 0x63, 0x6f,
        0x6d, 0x70, 0x61, 0x63, 0x74, 0x65, 0x64, 0xc3, 0xae, 0x66, 0x61, 0x69, 0x6c, 0x65, 0x64, 0x5f, 0x6f, 0x62, 0x6a, 0x65,
        0x63, 0x74, 0x73, 0x02, 0xae, 0x61, 0x6c, 0x6c, 0x5f, 0x74, 0x69, 0x65, 0x72, 0x5f, 0x73, 0x74, 0x61, 0x74, 0x73, 0x91,
        0x81, 0xa4, 0x57, 0x41, 0x52, 0x4d, 0x93, 0xcd, 0x08, 0x00, 0x02, 0x01,
    ];

    #[test]
    fn thin_usage_cache_decodes_scanner_wire_fixture() {
        let decoded =
            DataUsageCache::unmarshal(SCANNER_USAGE_CACHE_WIRE_FIXTURE).expect("thin projection decodes a scanner-written cache");

        // The six fields shared with the scanner's 16-field info block; the
        // remaining ten (lifecycle, replication, checkpoint, heals, ...) must
        // be skipped, not error.
        assert_eq!(decoded.info.name, "wire-bucket");
        assert_eq!(decoded.info.next_cycle, 7);
        assert_eq!(
            decoded.info.last_update,
            Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000))
        );
        assert!(decoded.info.skip_healing);
        assert_eq!(decoded.info.failed_objects.get("wire-bucket/lost"), Some(&11));
        assert!(decoded.info.snapshot_complete);

        // Entries use the shared canonical map-encoded type end to end.
        let entry = decoded.cache.get("wire-bucket").expect("fixture entry decodes");
        assert_eq!(entry.size, 4096);
        assert_eq!(entry.objects, 3);
        assert_eq!(entry.versions, 5);
        assert_eq!(entry.delete_markers, 1);
        assert!(entry.compacted);
        assert_eq!(entry.failed_objects, 2);
        assert_eq!(
            entry.all_tier_stats.as_ref().and_then(|tiers| tiers.tiers.get("WARM")),
            Some(&TierStats {
                total_size: 2048,
                num_versions: 2,
                num_objects: 1,
            })
        );
    }

    /// Build a cache shaped like `bucket/{a,b/{c,d}},bucket/loose` with
    /// distinct counters so aggregation is observable.
    fn prefix_usage_fixture_cache() -> DataUsageCache {
        let mut cache = DataUsageCache::default();
        let mut insert = |path: &str, parent: &str, size: usize, objects: usize, versions: usize, delete_markers: usize| {
            cache.replace(
                path,
                parent,
                DataUsageEntry {
                    size,
                    objects,
                    versions,
                    delete_markers,
                    ..Default::default()
                },
            );
        };
        insert("bucket", "", 0, 0, 0, 0);
        insert("bucket/a", "bucket", 100, 1, 1, 0);
        insert("bucket/b", "bucket", 0, 0, 0, 0);
        insert("bucket/b/c", "bucket/b", 200, 2, 2, 1);
        insert("bucket/b/d", "bucket/b", 40, 1, 3, 0);
        insert("bucket/loose", "bucket", 10, 1, 1, 1);
        cache
    }

    #[test]
    fn prefix_usage_aggregates_bucket_root_and_one_level_below() {
        let cache = prefix_usage_fixture_cache();

        let root = cache
            .prefix_usage("bucket", "", 100)
            .expect("root query must find the bucket entry");
        assert_eq!(root.usage.size, 350, "root aggregate flattens the whole subtree");
        assert_eq!(root.usage.objects, 5);
        assert_eq!(root.usage.versions, 7);
        assert_eq!(root.usage.delete_markers, 2);
        assert!(!root.compacted);
        assert!(!root.truncated);
        // Breakdown is one level: b (240) before a (100) before loose (10),
        // each flattened to its own subtree total.
        let names: Vec<(&str, u64)> = root
            .sub_prefixes
            .iter()
            .map(|entry| (entry.prefix.as_str(), entry.usage.size))
            .collect();
        assert_eq!(names, vec![("b", 240), ("a", 100), ("loose", 10)]);
    }

    #[test]
    fn prefix_usage_drills_into_arbitrary_prefixes() {
        let cache = prefix_usage_fixture_cache();

        let b = cache.prefix_usage("bucket", "b", 100).expect("nested prefix must resolve");
        assert_eq!(b.usage.size, 240);
        assert_eq!(b.usage.versions, 5);
        let names: Vec<&str> = b.sub_prefixes.iter().map(|entry| entry.prefix.as_str()).collect();
        assert_eq!(names, vec!["c", "d"]);

        // Prefix slashes are normalized away.
        let slashed = cache.prefix_usage("bucket", "/b/", 100).expect("slash-insensitive lookup");
        assert_eq!(slashed.usage.size, 240);

        assert!(cache.prefix_usage("bucket", "absent", 100).is_none(), "unknown prefix must be a miss");
        assert!(cache.prefix_usage("other", "", 100).is_none(), "unknown bucket must be a miss");
    }

    #[test]
    fn prefix_usage_reports_and_respects_truncation() {
        let cache = prefix_usage_fixture_cache();
        let capped = cache.prefix_usage("bucket", "", 2).expect("root query");
        assert!(capped.truncated, "three children capped to two must flag truncation");
        let names: Vec<&str> = capped.sub_prefixes.iter().map(|entry| entry.prefix.as_str()).collect();
        assert_eq!(names, vec!["b", "a"], "largest prefixes survive the cut");
    }

    #[test]
    fn prefix_usage_marks_compacted_entries() {
        let mut cache = DataUsageCache::default();
        cache.replace(
            "bucket",
            "",
            DataUsageEntry {
                size: 999,
                objects: 9,
                compacted: true,
                ..Default::default()
            },
        );

        let compacted = cache.prefix_usage("bucket", "", 100).expect("compacted root resolves");
        assert!(compacted.compacted, "compaction must be visible to callers");
        assert_eq!(compacted.usage.size, 999);
        assert!(compacted.sub_prefixes.is_empty(), "a compacted entry carries no children");
    }

    #[test]
    fn prefix_usage_rejects_cyclic_and_dangling_caches() {
        // A self-referencing child (corrupt cache) must yield a miss for the
        // whole query, not unbounded recursion.
        let mut cache = prefix_usage_fixture_cache();
        if let Some(entry) = cache.cache.get_mut("bucket/b") {
            entry.children.insert("bucket/b".to_string());
        }
        assert!(cache.prefix_usage("bucket", "b", 100).is_none(), "a cyclic subtree must be rejected");
        // The unaffected sibling still answers.
        assert!(cache.prefix_usage("bucket", "a", 100).is_some());

        // A child key with no entry (dangling link) is rejected rather than
        // silently dropped: half a tree would under-report usage.
        let mut dangling = prefix_usage_fixture_cache();
        if let Some(entry) = dangling.cache.get_mut("bucket/b") {
            entry.children.insert("bucket/b/ghost".to_string());
        }
        assert!(
            dangling.prefix_usage("bucket", "b", 100).is_none(),
            "a dangling child link must be rejected"
        );
    }

    #[test]
    fn hash_path_uses_portable_slash_semantics() {
        for (input, expected) in [
            ("", "."),
            (".", "."),
            ("/", "/"),
            ("//bucket///prefix/", "/bucket/prefix"),
            ("bucket/./prefix//object", "bucket/prefix/object"),
            ("bucket/a/../b", "bucket/b"),
            ("../bucket/..", ".."),
            ("/../../bucket", "/bucket"),
            ("bucket\\prefix/object", "bucket\\prefix/object"),
        ] {
            assert_eq!(hash_path(input).key(), expected, "unexpected portable cache key for {input:?}");
        }
    }

    #[test]
    fn completeness_marker_is_additive_for_legacy_named_readers() {
        let current = DataUsageInfo {
            last_update: Some(SystemTime::UNIX_EPOCH),
            usage_snapshot_complete: true,
            usage_snapshot_converged: Some(false),
            usage_snapshot_authoritative_baseline: Some(DataUsageSnapshotIdentity::default()),
            ..Default::default()
        };
        let encoded = rmp_serde::to_vec_named(&current).expect("encode current data usage snapshot");
        let legacy: LegacyUsageReader = rmp_serde::from_slice(&encoded).expect("legacy reader should ignore additive fields");

        assert_eq!(legacy.buckets_count, 0);
        assert!(current.is_complete_bucket_usage_snapshot());
        assert_eq!(current.usage_snapshot_converged, Some(false));
    }

    #[test]
    fn convergence_marker_defaults_to_unknown_for_older_snapshots() {
        let encoded = rmp_serde::to_vec_named(&DataUsageInfo {
            last_update: Some(SystemTime::UNIX_EPOCH),
            usage_snapshot_complete: true,
            ..Default::default()
        })
        .expect("encode pre-convergence data usage snapshot");
        let decoded: DataUsageInfo = rmp_serde::from_slice(&encoded).expect("decode older data usage snapshot");

        assert!(decoded.is_complete_bucket_usage_snapshot());
        assert_eq!(decoded.usage_snapshot_converged, None);
    }

    #[test]
    fn observation_selection_is_clock_independent_and_baseline_fenced() {
        let mut authoritative = DataUsageInfo {
            last_update: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(600)),
            scanner_epoch: Some(7),
            scanner_cycle: Some(10),
            usage_snapshot_complete: true,
            ..Default::default()
        };
        let observed = DataUsageInfo {
            // A newer leader may have a slower wall clock.
            last_update: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(300)),
            scanner_epoch: Some(8),
            scanner_cycle: Some(1),
            usage_snapshot_complete: true,
            usage_snapshot_converged: Some(false),
            usage_snapshot_authoritative_baseline: Some(authoritative.snapshot_identity()),
            ..Default::default()
        };

        assert!(observed_data_usage_is_newer(&observed, &authoritative));

        authoritative.last_update = Some(SystemTime::UNIX_EPOCH + Duration::from_secs(601));
        assert!(
            !observed_data_usage_is_newer(&observed, &authoritative),
            "an old-binary namespace mutation must fence the prior bucket incarnation regardless of clock skew"
        );
    }

    #[test]
    fn observation_selection_requires_nonconverged_complete_newer_data() {
        let authoritative = DataUsageInfo {
            last_update: Some(SystemTime::UNIX_EPOCH),
            scanner_epoch: Some(2),
            scanner_cycle: Some(10),
            usage_snapshot_complete: true,
            ..Default::default()
        };
        let baseline = Some(authoritative.snapshot_identity());
        let candidate = |epoch, cycle, converged, complete| DataUsageInfo {
            last_update: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1)),
            scanner_epoch: Some(epoch),
            scanner_cycle: Some(cycle),
            usage_snapshot_complete: complete,
            usage_snapshot_converged: converged,
            usage_snapshot_authoritative_baseline: baseline,
            ..Default::default()
        };

        assert!(observed_data_usage_is_newer(&candidate(2, 11, Some(false), true), &authoritative));
        assert!(!observed_data_usage_is_newer(&candidate(2, 9, Some(false), true), &authoritative));
        assert!(!observed_data_usage_is_newer(&candidate(2, 11, Some(true), true), &authoritative));
        assert!(!observed_data_usage_is_newer(&candidate(2, 11, Some(false), false), &authoritative));

        let mut partial = candidate(2, 11, Some(false), false);
        partial.usage_snapshot_partial = true;
        partial.usage_snapshot_set_states = vec![DataUsageSnapshotSetState {
            pool_index: 0,
            set_index: 0,
            scanner_cycle: Some(10),
            scanner_epoch: Some(2),
            scan_plan_digest: Some([1; 32]),
            complete: false,
            tombstone: false,
        }];
        assert!(observed_data_usage_is_newer(&partial, &authoritative));
    }

    #[test]
    fn mixed_topology_snapshot_is_rejected() {
        let mut partial = DataUsageInfo {
            last_update: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(2)),
            scanner_cycle: Some(11),
            scanner_epoch: Some(2),
            buckets_count: 0,
            usage_snapshot_converged: Some(false),
            usage_snapshot_partial: true,
            usage_snapshot_set_states: vec![
                DataUsageSnapshotSetState {
                    pool_index: 0,
                    set_index: 0,
                    scanner_cycle: Some(11),
                    scanner_epoch: Some(2),
                    scan_plan_digest: Some([1; 32]),
                    complete: true,
                    tombstone: false,
                },
                DataUsageSnapshotSetState {
                    pool_index: 1,
                    set_index: 0,
                    scanner_cycle: Some(10),
                    scanner_epoch: Some(2),
                    scan_plan_digest: Some([2; 32]),
                    complete: false,
                    tombstone: false,
                },
            ],
            ..Default::default()
        };
        assert!(!partial.is_valid_partial_snapshot());
        partial.usage_snapshot_set_states[1].scan_plan_digest = Some([1; 32]);
        assert!(partial.is_valid_partial_snapshot());
    }

    #[test]
    fn completeness_marker_requires_a_snapshot_timestamp() {
        let untimestamped = DataUsageInfo {
            usage_snapshot_complete: true,
            ..Default::default()
        };

        assert!(!untimestamped.is_complete_bucket_usage_snapshot());
    }

    #[test]
    fn test_usage_last_update_future_tolerance_boundary() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);

        // Within tolerance (including the exact boundary) the timestamp is trusted.
        assert!(!usage_last_update_is_untrusted_future(now, now));
        assert!(!usage_last_update_is_untrusted_future(now - Duration::from_secs(60), now));
        assert!(!usage_last_update_is_untrusted_future(now + USAGE_LAST_UPDATE_FUTURE_TOLERANCE, now));

        // Beyond tolerance the persisted timestamp is untrustworthy.
        assert!(usage_last_update_is_untrusted_future(
            now + USAGE_LAST_UPDATE_FUTURE_TOLERANCE + Duration::from_secs(1),
            now
        ));
    }

    #[test]
    fn test_data_usage_info_creation() {
        let mut info = DataUsageInfo::new();
        info.update_capacity(1000, 500, 500);

        assert_eq!(info.total_capacity, 1000);
        assert_eq!(info.total_used_capacity, 500);
        assert_eq!(info.total_free_capacity, 500);
        assert!(info.last_update.is_some());
    }

    #[test]
    fn test_bucket_usage_info_merge() {
        let mut usage1 = BucketUsageInfo::new();
        usage1.size = 100;
        usage1.objects_count = 10;
        usage1.versions_count = 5;

        let mut usage2 = BucketUsageInfo::new();
        usage2.size = 200;
        usage2.objects_count = 20;
        usage2.versions_count = 10;

        usage1.merge(&usage2);

        assert_eq!(usage1.size, 300);
        assert_eq!(usage1.objects_count, 30);
        assert_eq!(usage1.versions_count, 15);
    }

    #[test]
    fn size_summary_add_saturates_instead_of_overflowing() {
        // The scanner folds one summary per object into a per-prefix total, so a
        // counter at its ceiling must stay there rather than panic in a debug
        // build or wrap in a release one (backlog#1828).
        let mut summary = SizeSummary {
            total_size: usize::MAX,
            versions: usize::MAX,
            replicated_size: i64::MAX,
            pending_size: i64::MAX,
            failed_size: i64::MAX,
            replica_size: i64::MAX,
            ..Default::default()
        };
        summary.repl_target_stats.insert(
            "arn".to_string(),
            ReplTargetSizeSummary {
                replicated_size: i64::MAX,
                pending_size: i64::MAX,
                failed_size: i64::MAX,
                ..Default::default()
            },
        );

        let mut increment = SizeSummary {
            total_size: 1,
            versions: 1,
            replicated_size: 1,
            pending_size: 1,
            failed_size: 1,
            replica_size: 1,
            ..Default::default()
        };
        increment.repl_target_stats.insert(
            "arn".to_string(),
            ReplTargetSizeSummary {
                replicated_size: 1,
                pending_size: 1,
                failed_size: 1,
                ..Default::default()
            },
        );

        summary.add(&increment);

        assert_eq!(summary.total_size, usize::MAX);
        assert_eq!(summary.versions, usize::MAX);
        assert_eq!(summary.replicated_size, i64::MAX);
        assert_eq!(summary.pending_size, i64::MAX);
        assert_eq!(summary.failed_size, i64::MAX);
        assert_eq!(summary.replica_size, i64::MAX);

        let target = summary.repl_target_stats.get("arn").expect("target survives the merge");
        assert_eq!(target.replicated_size, i64::MAX);
        assert_eq!(target.pending_size, i64::MAX);
        assert_eq!(target.failed_size, i64::MAX);
    }

    #[test]
    fn test_size_summary_add() {
        let mut summary1 = SizeSummary::new();
        summary1.total_size = 100;
        summary1.versions = 5;

        let mut summary2 = SizeSummary::new();
        summary2.total_size = 200;
        summary2.versions = 10;

        summary1.add(&summary2);

        assert_eq!(summary1.total_size, 300);
        assert_eq!(summary1.versions, 15);
    }

    #[test]
    fn test_size_histogram_compat_rollup_sums_all_sub_buckets() {
        let mut hist = SizeHistogram::default();
        // One object in each of the four sub-ranges within [1024, 1 MiB).
        hist.add(32 * 1024); // [1024, 64 KiB)
        hist.add(128 * 1024); // [64 KiB, 256 KiB)
        hist.add(384 * 1024); // [256 KiB, 512 KiB)
        hist.add(768 * 1024); // [512 KiB, 1 MiB)

        let map = hist.to_map();

        assert_eq!(map["BETWEEN_1024B_AND_1_MB"], 4);
        assert_eq!(map["BETWEEN_1024_B_AND_64_KB"], 1);
        assert_eq!(map["BETWEEN_64_KB_AND_256_KB"], 1);
        assert_eq!(map["BETWEEN_256_KB_AND_512_KB"], 1);
        assert_eq!(map["BETWEEN_512_KB_AND_1_MB"], 1);
    }

    #[test]
    fn test_size_histogram_classifies_adjacent_boundaries_once() {
        let cases = [
            (1023, 0),
            (1024, 1),
            (64 * 1024 - 1, 1),
            (64 * 1024, 2),
            (256 * 1024 - 1, 2),
            (256 * 1024, 3),
            (512 * 1024 - 1, 3),
            (512 * 1024, 4),
            (1024 * 1024 - 1, 4),
            (1024 * 1024, 6),
            (10 * 1024 * 1024 - 1, 6),
            (10 * 1024 * 1024, 7),
            (64 * 1024 * 1024 - 1, 7),
            (64 * 1024 * 1024, 8),
            (128 * 1024 * 1024 - 1, 8),
            (128 * 1024 * 1024, 9),
            (512 * 1024 * 1024 - 1, 9),
            (512 * 1024 * 1024, 10),
        ];

        for (size, expected_bucket) in cases {
            let mut hist = SizeHistogram::default();
            hist.add(size);

            assert_eq!(hist.0.iter().sum::<u64>(), 1, "size {size} must have exactly one physical bucket");
            assert_eq!(hist.0[expected_bucket], 1, "size {size} must select the expected bucket");
        }
    }

    #[test]
    fn test_size_histogram_1024_bytes_contributes_to_compat_rollup() {
        let mut hist = SizeHistogram::default();
        hist.add(1024);

        let map = hist.to_map();
        assert_eq!(map["LESS_THAN_1024_B"], 0);
        assert_eq!(map["BETWEEN_1024_B_AND_64_KB"], 1);
        assert_eq!(map["BETWEEN_1024B_AND_1_MB"], 1);
    }

    #[test]
    fn test_size_histogram_compat_rollup_saturates_on_corrupt_counts() {
        let mut hist = SizeHistogram::default();
        hist.0[1] = u64::MAX;
        hist.0[2] = 1;

        let map = hist.to_map();

        assert_eq!(map["BETWEEN_1024B_AND_1_MB"], u64::MAX);
    }

    #[test]
    fn replication_stats_empty_checks_every_field() {
        type SetField = fn(&mut ReplicationTargetUsage);

        let cases: [(&str, SetField); 10] = [
            ("pending_size", |stats| stats.pending_size = 1),
            ("replicated_size", |stats| stats.replicated_size = 1),
            ("failed_size", |stats| stats.failed_size = 1),
            ("failed_count", |stats| stats.failed_count = 1),
            ("pending_count", |stats| stats.pending_count = 1),
            ("missed_threshold_size", |stats| stats.missed_threshold_size = 1),
            ("after_threshold_size", |stats| stats.after_threshold_size = 1),
            ("missed_threshold_count", |stats| stats.missed_threshold_count = 1),
            ("after_threshold_count", |stats| stats.after_threshold_count = 1),
            ("replicated_count", |stats| stats.replicated_count = 1),
        ];

        assert!(ReplicationTargetUsage::default().is_empty());
        for (field, set_nonzero) in cases {
            let mut stats = ReplicationTargetUsage::default();
            set_nonzero(&mut stats);
            assert!(!stats.is_empty(), "{field} must make replication stats non-empty");
        }
    }

    #[test]
    fn replication_all_stats_empty_checks_aggregate_fields_independently() {
        let cases = [
            (
                "replica_size",
                ReplicationAllStats {
                    replica_size: 1,
                    ..Default::default()
                },
            ),
            (
                "replica_count",
                ReplicationAllStats {
                    replica_count: 1,
                    ..Default::default()
                },
            ),
        ];

        assert!(ReplicationAllStats::default().is_empty());
        for (field, stats) in cases {
            assert!(!stats.is_empty(), "{field} must make aggregate replication stats non-empty");
        }

        let empty_targets = ReplicationAllStats {
            targets: HashMap::from([("arn:test:empty".to_string(), ReplicationTargetUsage::default())]),
            ..Default::default()
        };
        assert!(empty_targets.is_empty(), "all-empty targets must keep aggregate stats empty");

        let stats = ReplicationAllStats {
            targets: HashMap::from([
                ("arn:test:empty".to_string(), ReplicationTargetUsage::default()),
                (
                    "arn:test:non-empty".to_string(),
                    ReplicationTargetUsage {
                        pending_count: 1,
                        ..Default::default()
                    },
                ),
            ]),
            ..Default::default()
        };
        assert!(!stats.is_empty(), "a non-empty target must make aggregate replication stats non-empty");
    }

    #[test]
    fn size_recursive_prunes_empty_and_preserves_pending_replication_stats() {
        let root = hash_path("bucket");
        let child = hash_path("bucket/child");
        let mut cache = DataUsageCache::default();
        cache.replace_hashed(&root, &None, &DataUsageEntry::default());
        cache.replace_hashed(
            &child,
            &Some(root.clone()),
            &DataUsageEntry {
                replication_stats: Some(ReplicationAllStats::default()),
                ..Default::default()
            },
        );

        assert!(
            cache
                .size_recursive("bucket")
                .expect("bucket usage should flatten")
                .replication_stats
                .is_none()
        );

        cache.replace_hashed(
            &child,
            &Some(root.clone()),
            &DataUsageEntry {
                replication_stats: Some(ReplicationAllStats {
                    targets: HashMap::from([(
                        "arn:test:pending".to_string(),
                        ReplicationTargetUsage {
                            pending_count: 1,
                            ..Default::default()
                        },
                    )]),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );

        let flattened = cache.size_recursive("bucket").expect("bucket usage should flatten");
        let replication = flattened
            .replication_stats
            .expect("pending-only replication stats must survive pruning");

        assert_eq!(replication.targets["arn:test:pending"].pending_count, 1);
    }

    #[test]
    fn test_data_usage_cache_merge_adds_missing_child() {
        let mut base = DataUsageCache::default();
        base.info.name = "bucket".to_string();
        base.replace("bucket", "", DataUsageEntry::default());

        let mut other = DataUsageCache::default();
        other.info.name = "bucket".to_string();
        let child = DataUsageEntry {
            size: 42,
            ..Default::default()
        };
        other.replace("bucket/child", "bucket", child);

        base.merge(&other);

        let root = base.find("bucket").expect("root bucket should exist");
        assert_eq!(root.size, 0);
        let child_entry = base.find("bucket/child").expect("merged child should be added");
        assert_eq!(child_entry.size, 42);
    }

    #[test]
    fn test_data_usage_cache_merge_accumulates_existing_child() {
        let mut base = DataUsageCache::default();
        base.info.name = "bucket".to_string();
        base.replace(
            "bucket/child",
            "bucket",
            DataUsageEntry {
                size: 10,
                objects: 1,
                ..Default::default()
            },
        );

        let mut other = DataUsageCache::default();
        other.info.name = "bucket".to_string();
        other.replace(
            "bucket/child",
            "bucket",
            DataUsageEntry {
                size: 20,
                objects: 2,
                ..Default::default()
            },
        );

        base.merge(&other);

        let child_entry = base.find("bucket/child").expect("child should remain after merge");
        assert_eq!(child_entry.size, 30);
        assert_eq!(child_entry.objects, 3);
    }

    #[test]
    fn test_dui_bucket_count_uses_bucket_list_after_compaction() {
        let root_hash = hash_path("root");
        let mut cache = DataUsageCache {
            info: DataUsageCacheInfo {
                name: "root".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        cache.replace_hashed(
            &root_hash,
            &None,
            &DataUsageEntry {
                compacted: true,
                objects: 3,
                ..Default::default()
            },
        );

        let buckets = vec!["bucket-a".to_string(), "bucket-b".to_string()];
        let info = cache.dui("root", &buckets);

        assert_eq!(info.buckets_count, 2);
        assert!(info.buckets_usage.is_empty());
        assert_eq!(info.objects_total_count, 3);
        assert!(info.tier_stats.is_none());
    }

    #[test]
    fn test_dui_reports_tier_usage_from_the_flattened_tree() {
        let root_hash = hash_path("root");
        let bucket_hash = hash_path("bucket-a");
        let mut cache = DataUsageCache {
            info: DataUsageCacheInfo {
                name: "root".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        cache.replace_hashed(&root_hash, &None, &DataUsageEntry::default());
        cache.replace_hashed(
            &bucket_hash,
            &Some(root_hash),
            &tier_entry(
                "WARM",
                TierStats {
                    total_size: 40,
                    num_versions: 2,
                    num_objects: 2,
                },
            ),
        );

        let info = cache.dui("root", &["bucket-a".to_string()]);

        assert_eq!(
            info.tier_stats.expect("child tier usage should roll up to the root").tiers["WARM"],
            TierStats {
                total_size: 40,
                num_versions: 2,
                num_objects: 2,
            }
        );
    }

    #[test]
    fn test_data_usage_entry_merge_preserves_replication_targets() {
        let mut base = DataUsageEntry {
            replication_stats: Some(ReplicationAllStats {
                replica_size: 10,
                replica_count: 1,
                targets: HashMap::from([
                    (
                        "arn:self-only".to_string(),
                        ReplicationTargetUsage {
                            pending_size: 7,
                            pending_count: 1,
                            ..Default::default()
                        },
                    ),
                    (
                        "arn:shared".to_string(),
                        ReplicationTargetUsage {
                            failed_size: 3,
                            failed_count: 1,
                            missed_threshold_size: 2,
                            missed_threshold_count: 1,
                            ..Default::default()
                        },
                    ),
                ]),
            }),
            ..Default::default()
        };
        let other = DataUsageEntry {
            replication_stats: Some(ReplicationAllStats {
                replica_size: 20,
                replica_count: 2,
                targets: HashMap::from([
                    (
                        "arn:shared".to_string(),
                        ReplicationTargetUsage {
                            failed_size: 5,
                            failed_count: 2,
                            after_threshold_size: 4,
                            after_threshold_count: 2,
                            ..Default::default()
                        },
                    ),
                    (
                        "arn:other-only".to_string(),
                        ReplicationTargetUsage {
                            replicated_size: 11,
                            replicated_count: 3,
                            ..Default::default()
                        },
                    ),
                ]),
            }),
            ..Default::default()
        };

        base.merge(&other);

        let stats = base.replication_stats.expect("replication stats should remain present");
        assert_eq!(stats.replica_size, 30);
        assert_eq!(stats.replica_count, 3);
        assert_eq!(stats.targets["arn:self-only"].pending_size, 7);
        assert_eq!(stats.targets["arn:self-only"].pending_count, 1);
        assert_eq!(stats.targets["arn:shared"].failed_size, 8);
        assert_eq!(stats.targets["arn:shared"].failed_count, 3);
        assert_eq!(stats.targets["arn:shared"].missed_threshold_size, 2);
        assert_eq!(stats.targets["arn:shared"].missed_threshold_count, 1);
        assert_eq!(stats.targets["arn:shared"].after_threshold_size, 4);
        assert_eq!(stats.targets["arn:shared"].after_threshold_count, 2);
        assert_eq!(stats.targets["arn:other-only"].replicated_size, 11);
        assert_eq!(stats.targets["arn:other-only"].replicated_count, 3);
    }

    // --- Tests for `add` and `reduce_children_of` (bug fixes) ---

    /// Build a small tree: root -> child1 (leaf), child2 -> grandchild (leaf).
    fn build_test_tree() -> (DataUsageCache, DataUsageHash) {
        let root = hash_path("bucket");
        let c1 = hash_path("bucket/a");
        let c2 = hash_path("bucket/b");
        let gc = hash_path("bucket/b/c");

        let mut cache = DataUsageCache::default();
        cache.replace_hashed(&root, &None, &DataUsageEntry::default());
        cache.replace_hashed(
            &c1,
            &Some(root.clone()),
            &DataUsageEntry {
                objects: 1,
                size: 10,
                ..Default::default()
            },
        );
        cache.replace_hashed(
            &c2,
            &Some(root.clone()),
            &DataUsageEntry {
                objects: 2,
                size: 20,
                ..Default::default()
            },
        );
        cache.replace_hashed(
            &gc,
            &Some(c2.clone()),
            &DataUsageEntry {
                objects: 3,
                size: 30,
                ..Default::default()
            },
        );
        (cache, root)
    }

    fn build_underflow_test_tree() -> (DataUsageCache, DataUsageHash) {
        let root = hash_path("bucket");
        let small = hash_path("bucket/small");
        let small_a = hash_path("bucket/small/a");
        let small_b = hash_path("bucket/small/b");
        let large = hash_path("bucket/large");
        let large_a = hash_path("bucket/large/a");
        let large_b = hash_path("bucket/large/b");

        let mut cache = DataUsageCache::default();
        cache.replace_hashed(
            &root,
            &None,
            &DataUsageEntry {
                objects: 100,
                ..Default::default()
            },
        );
        cache.replace_hashed(&small, &Some(root.clone()), &DataUsageEntry::default());
        cache.replace_hashed(
            &small_a,
            &Some(small.clone()),
            &DataUsageEntry {
                objects: 1,
                ..Default::default()
            },
        );
        cache.replace_hashed(
            &small_b,
            &Some(small.clone()),
            &DataUsageEntry {
                objects: 1,
                ..Default::default()
            },
        );
        cache.replace_hashed(&large, &Some(root.clone()), &DataUsageEntry::default());
        cache.replace_hashed(
            &large_a,
            &Some(large.clone()),
            &DataUsageEntry {
                objects: 10,
                ..Default::default()
            },
        );
        cache.replace_hashed(
            &large_b,
            &Some(large.clone()),
            &DataUsageEntry {
                objects: 10,
                ..Default::default()
            },
        );
        (cache, root)
    }

    #[test]
    fn test_add_collects_internal_nodes_as_compaction_candidates() {
        let (cache, root) = build_test_tree();
        let mut candidates = Vec::new();
        add(&cache, &root, &mut candidates);

        let mut paths: Vec<String> = candidates.iter().map(|l| l.path.key()).collect();
        paths.sort();
        assert_eq!(paths.len(), 2, "add() should find internal nodes with children");
        assert!(paths.contains(&hash_path("bucket").key()));
        assert!(paths.contains(&hash_path("bucket/b").key()));
    }

    #[test]
    fn test_add_skips_leaf_node() {
        let mut cache = DataUsageCache::default();
        let h = hash_path("single-leaf");
        cache.replace_hashed(
            &h,
            &None,
            &DataUsageEntry {
                objects: 5,
                size: 50,
                ..Default::default()
            },
        );

        let mut candidates = Vec::new();
        add(&cache, &h, &mut candidates);
        assert!(candidates.is_empty(), "leaf node should not be a compaction candidate");
    }

    #[test]
    fn test_reduce_children_of_compacts_internal_node() {
        let (mut cache, root) = build_test_tree();
        cache.reduce_children_of(&root, 2, false);

        let entry_c2 = cache.find("bucket/b").unwrap();
        assert!(entry_c2.compacted, "internal node 'bucket/b' should be compacted");
        let entry_c1 = cache.find("bucket/a").unwrap();
        assert!(!entry_c1.compacted, "leaf 'bucket/a' should not be compacted");
        assert!(cache.find("bucket/b/c").is_none(), "grandchild should be removed");
    }

    #[test]
    fn test_reduce_children_of_usize_underflow_saturates() {
        let (mut cache, root) = build_underflow_test_tree();

        // total children=6, limit=5, remove=1. The smallest candidate removes
        // two descendants, so plain subtraction would underflow and compact the
        // next candidate too.
        cache.reduce_children_of(&root, 5, false);

        assert!(cache.find("bucket/small").is_some_and(|entry| entry.compacted));
        assert!(cache.find("bucket/small/a").is_none());
        assert!(cache.find("bucket/small/b").is_none());
        assert!(cache.find("bucket/large").is_some_and(|entry| !entry.compacted));
        assert!(cache.find("bucket/large/a").is_some());
        assert!(cache.find("bucket/large/b").is_some());
    }

    #[test]
    fn checked_merge_rejects_scalar_and_replication_overflow_without_mutation() {
        let mut entry = DataUsageEntry {
            objects: usize::MAX,
            replication_stats: Some(ReplicationAllStats {
                replica_size: 7,
                ..Default::default()
            }),
            ..Default::default()
        };
        let other = DataUsageEntry {
            objects: 1,
            replication_stats: Some(ReplicationAllStats {
                replica_size: u64::MAX,
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(!entry.checked_merge(&other));
        assert_eq!(entry.objects, usize::MAX);
        assert_eq!(entry.replication_stats.as_ref().map(|stats| stats.replica_size), Some(7));
    }

    #[test]
    fn checked_merge_accepts_valid_usage() {
        let mut entry = DataUsageEntry {
            objects: 2,
            size: 20,
            ..Default::default()
        };
        let other = DataUsageEntry {
            objects: 3,
            size: 30,
            ..Default::default()
        };

        assert!(entry.checked_merge(&other));
        assert_eq!(entry.objects, 5);
        assert_eq!(entry.size, 50);
    }

    #[test]
    fn histogram_deserialization_rejects_noncanonical_lengths() {
        let invalid_sizes =
            rmp_serde::to_vec(&vec![0_u64; SIZE_HISTOGRAM_LEN + 1]).expect("encode invalid object-size histogram fixture");
        let invalid_versions =
            rmp_serde::to_vec(&vec![0_u64; VERSIONS_HISTOGRAM_LEN - 1]).expect("encode invalid object-version histogram fixture");

        assert!(rmp_serde::from_slice::<SizeHistogram>(&invalid_sizes).is_err());
        assert!(rmp_serde::from_slice::<VersionsHistogram>(&invalid_versions).is_err());
    }

    #[test]
    fn replication_target_deserialization_preserves_large_historical_maps() {
        let mut stats = ReplicationAllStats::default();
        for index in 0..=1024 {
            stats
                .targets
                .insert(format!("target-{index}"), ReplicationTargetUsage::default());
        }
        let encoded = rmp_serde::to_vec_named(&stats).expect("large replication target fixture should encode");
        let decoded = rmp_serde::from_slice::<ReplicationAllStats>(&encoded)
            .expect("historical replication target maps must remain readable");

        assert_eq!(decoded.targets.len(), stats.targets.len());
    }

    /// Round-trip test: encoding a [`ReplicationTargetUsage`] and decoding it back
    /// must produce the exact same value.  This guards against accidental serde
    /// field-name drift during the `ReplicationStats` -> `ReplicationTargetUsage`
    /// rename.  Wire-level field names are the serialized Rust field identifiers,
    /// which must remain byte-identical.
    #[test]
    fn replication_target_usage_rmp_round_trip() {
        let original = ReplicationTargetUsage {
            pending_size: 100,
            replicated_size: 2_000,
            failed_size: 50,
            failed_count: 3,
            pending_count: 7,
            missed_threshold_size: 11,
            after_threshold_size: 22,
            missed_threshold_count: 1,
            after_threshold_count: 2,
            replicated_count: 99,
        };

        let buf = rmp_serde::to_vec_named(&original).expect("encode ReplicationTargetUsage to msgpack");
        let decoded: ReplicationTargetUsage = rmp_serde::from_slice(&buf).expect("decode ReplicationTargetUsage from msgpack");
        assert_eq!(original, decoded, "round-trip through rmp must preserve every field");

        // Also verify that encoding as an unnamed sequence and then decoding
        // with named fields produces the correct mapping (this catches reordering).
        let named_buf = rmp_serde::to_vec_named(&original).expect("re-encode for field-name pinning");
        // Spot-check that known field names appear in the named encoding.
        let named_str = String::from_utf8_lossy(&named_buf);
        assert!(named_str.contains("pending_size"), "field 'pending_size' must survive the rename");
        assert!(named_str.contains("replicated_size"), "field 'replicated_size' must survive the rename");
        assert!(
            named_str.contains("missed_threshold_size"),
            "field 'missed_threshold_size' must survive the rename"
        );
        assert!(
            named_str.contains("after_threshold_count"),
            "field 'after_threshold_count' must survive the rename"
        );
    }

    #[test]
    fn checked_merge_rejects_noncanonical_histograms_without_mutation() {
        let mut entry = DataUsageEntry {
            objects: 2,
            ..Default::default()
        };
        let other = DataUsageEntry {
            objects: 3,
            obj_sizes: SizeHistogram(vec![0; SIZE_HISTOGRAM_LEN + 1]),
            ..Default::default()
        };

        assert!(!entry.checked_merge(&other));
        assert_eq!(entry.objects, 2);
    }
}
