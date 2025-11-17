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

use path_clean::PathClean;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    hash::{DefaultHasher, Hash, Hasher},
    path::Path,
    time::SystemTime,
};

#[derive(Clone, Copy, Default, Debug, Serialize, Deserialize, PartialEq)]
pub struct TierStats {
    pub total_size: u64,
    pub num_versions: i32,
    pub num_objects: i32,
}

impl TierStats {
    pub fn add(&self, u: &TierStats) -> TierStats {
        TierStats {
            total_size: self.total_size + u.total_size,
            num_versions: self.num_versions + u.num_versions,
            num_objects: self.num_objects + u.num_objects,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct AllTierStats {
    pub tiers: HashMap<String, TierStats>,
}

impl AllTierStats {
    pub fn new() -> Self {
        Self { tiers: HashMap::new() }
    }

    pub fn add_sizes(&mut self, tiers: HashMap<String, TierStats>) {
        for (tier, st) in tiers {
            self.tiers
                .insert(tier.clone(), self.tiers.get(&tier).unwrap_or(&TierStats::default()).add(&st));
        }
    }

    pub fn merge(&mut self, other: AllTierStats) {
        for (tier, st) in other.tiers {
            self.tiers
                .insert(tier.clone(), self.tiers.get(&tier).unwrap_or(&TierStats::default()).add(&st));
        }
    }

    pub fn populate_stats(&self, stats: &mut HashMap<String, TierStats>) {
        for (tier, st) in &self.tiers {
            stats.insert(
                tier.clone(),
                TierStats {
                    total_size: st.total_size,
                    num_versions: st.num_versions,
                    num_objects: st.num_objects,
                },
            );
        }
    }
}

/// Bucket target usage info provides replication statistics
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct DataUsageInfo {
    /// Total capacity
    pub total_capacity: u64,
    /// Total used capacity
    pub total_used_capacity: u64,
    /// Total free capacity
    pub total_free_capacity: u64,

    /// LastUpdate is the timestamp of when the data usage info was last updated
    pub last_update: Option<SystemTime>,

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

    /// Total number of buckets in this cluster
    pub buckets_count: u64,
    /// Buckets usage info provides following information across all buckets
    pub buckets_usage: HashMap<String, BucketUsageInfo>,
    /// Deprecated kept here for backward compatibility reasons
    pub bucket_sizes: HashMap<String, u64>,
    /// Per-disk snapshot information when available
    #[serde(default)]
    pub disk_usage_status: Vec<DiskUsageStatus>,
}

/// Metadata describing the status of a disk-level data usage snapshot.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct DiskUsageStatus {
    pub disk_id: String,
    pub pool_index: Option<usize>,
    pub set_index: Option<usize>,
    pub disk_index: Option<usize>,
    pub last_update: Option<SystemTime>,
    pub snapshot_exists: bool,
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
    pub replicated_size: usize,
    /// Replicated count
    pub replicated_count: usize,
    /// Pending size
    pub pending_size: usize,
    /// Failed size
    pub failed_size: usize,
    /// Replica size
    pub replica_size: usize,
    /// Replica count
    pub replica_count: usize,
    /// Pending count
    pub pending_count: usize,
    /// Failed count
    pub failed_count: usize,
    /// Replication target stats
    pub repl_target_stats: HashMap<String, ReplTargetSizeSummary>,
}

/// Replication target size summary
#[derive(Debug, Default, Clone)]
pub struct ReplTargetSizeSummary {
    /// Replicated size
    pub replicated_size: usize,
    /// Replicated count
    pub replicated_count: usize,
    /// Pending size
    pub pending_size: usize,
    /// Failed size
    pub failed_size: usize,
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
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SizeHistogram(Vec<u64>);

impl Default for SizeHistogram {
    fn default() -> Self {
        Self(vec![0; 11]) // DATA_USAGE_BUCKET_LEN = 11
    }
}

impl SizeHistogram {
    pub fn add(&mut self, size: u64) {
        let intervals = [
            (0, 1024),                                  // LESS_THAN_1024_B
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

        let mut res = HashMap::new();
        let mut spl_count = 0;
        for (count, name) in self.0.iter().zip(names.iter()) {
            if name == &"BETWEEN_1024B_AND_1_MB" {
                res.insert(name.to_string(), spl_count);
            } else if name.starts_with("BETWEEN_") && name.contains("_KB_") && name.contains("_MB") {
                spl_count += count;
                res.insert(name.to_string(), *count);
            } else {
                res.insert(name.to_string(), *count);
            }
        }
        res
    }
}

/// Versions histogram for version count distribution
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VersionsHistogram(Vec<u64>);

impl Default for VersionsHistogram {
    fn default() -> Self {
        Self(vec![0; 7]) // DATA_USAGE_VERSION_LEN = 7
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
}

/// Replication statistics for a single target
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ReplicationStats {
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

impl ReplicationStats {
    pub fn empty(&self) -> bool {
        self.replicated_size == 0 && self.failed_size == 0 && self.failed_count == 0
    }
}

/// Replication statistics for all targets
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ReplicationAllStats {
    pub targets: HashMap<String, ReplicationStats>,
    pub replica_size: u64,
    pub replica_count: u64,
}

impl ReplicationAllStats {
    pub fn empty(&self) -> bool {
        if self.replica_size != 0 && self.replica_count != 0 {
            return false;
        }
        for (_, v) in self.targets.iter() {
            if !v.empty() {
                return false;
            }
        }
        true
    }
}

/// Data usage cache entry
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
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
}

impl DataUsageEntry {
    pub fn add_child(&mut self, hash: &DataUsageHash) {
        if self.children.contains(&hash.key()) {
            return;
        }
        self.children.insert(hash.key());
    }

    pub fn add_sizes(&mut self, summary: &SizeSummary) {
        self.size += summary.total_size;
        self.versions += summary.versions;
        self.delete_markers += summary.delete_markers;
        self.obj_sizes.add(summary.total_size as u64);
        self.obj_versions.add(summary.versions as u64);

        let replication_stats = if self.replication_stats.is_none() {
            self.replication_stats = Some(ReplicationAllStats::default());
            self.replication_stats.as_mut().unwrap()
        } else {
            self.replication_stats.as_mut().unwrap()
        };
        replication_stats.replica_size += summary.replica_size as u64;
        replication_stats.replica_count += summary.replica_count as u64;

        for (arn, st) in &summary.repl_target_stats {
            let tgt_stat = replication_stats
                .targets
                .entry(arn.to_string())
                .or_insert(ReplicationStats::default());
            tgt_stat.pending_size += st.pending_size as u64;
            tgt_stat.failed_size += st.failed_size as u64;
            tgt_stat.replicated_size += st.replicated_size as u64;
            tgt_stat.replicated_count += st.replicated_count as u64;
            tgt_stat.failed_count += st.failed_count as u64;
            tgt_stat.pending_count += st.pending_count as u64;
        }
    }

    pub fn merge(&mut self, other: &DataUsageEntry) {
        self.objects += other.objects;
        self.versions += other.versions;
        self.delete_markers += other.delete_markers;
        self.size += other.size;

        if let Some(o_rep) = &other.replication_stats {
            if self.replication_stats.is_none() {
                self.replication_stats = Some(ReplicationAllStats::default());
            }
            let s_rep = self.replication_stats.as_mut().unwrap();
            s_rep.targets.clear();
            s_rep.replica_size += o_rep.replica_size;
            s_rep.replica_count += o_rep.replica_count;
            for (arn, stat) in o_rep.targets.iter() {
                let st = s_rep.targets.entry(arn.clone()).or_default();
                *st = ReplicationStats {
                    pending_size: stat.pending_size + st.pending_size,
                    failed_size: stat.failed_size + st.failed_size,
                    replicated_size: stat.replicated_size + st.replicated_size,
                    pending_count: stat.pending_count + st.pending_count,
                    failed_count: stat.failed_count + st.failed_count,
                    replicated_count: stat.replicated_count + st.replicated_count,
                    ..Default::default()
                };
            }
        }

        for (i, v) in other.obj_sizes.0.iter().enumerate() {
            self.obj_sizes.0[i] += v;
        }

        for (i, v) in other.obj_versions.0.iter().enumerate() {
            self.obj_versions.0[i] += v;
        }
    }
}

/// Data usage cache info
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DataUsageCacheInfo {
    pub name: String,
    pub next_cycle: u32,
    pub last_update: Option<SystemTime>,
    pub skip_healing: bool,
}

/// Data usage cache
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
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
                if flat.replication_stats.is_some() && flat.replication_stats.as_ref().unwrap().empty() {
                    flat.replication_stats = None;
                }
                Some(flat)
            }
            None => None,
        }
    }

    pub fn search_parent(&self, hash: &DataUsageHash) -> Option<DataUsageHash> {
        let want = hash.key();
        if let Some(last_index) = want.rfind('/') {
            if let Some(v) = self.find(&want[0..last_index]) {
                if v.children.contains(&want) {
                    let found = hash_path(&want[0..last_index]);
                    return Some(found);
                }
            }
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

        let mut leaves = Vec::new();
        let mut remove = total - limit;
        add(self, path, &mut leaves);
        leaves.sort_by(|a, b| a.objects.cmp(&b.objects));

        while remove > 0 && !leaves.is_empty() {
            let e = leaves.first().unwrap();
            let candidate = e.path.clone();
            if candidate == *path && !compact_self {
                break;
            }
            let removing = self.total_children_rec(&candidate.key());
            let mut flat = match self.size_recursive(&candidate.key()) {
                Some(flat) => flat,
                None => {
                    leaves.remove(0);
                    continue;
                }
            };

            flat.compacted = true;
            self.delete_recursive(&candidate);
            self.replace_hashed(&candidate, &None, &flat);

            remove -= removing;
            leaves.remove(0);
        }
    }

    pub fn total_children_rec(&self, path: &str) -> usize {
        let root = self.find(path);

        if root.is_none() {
            return 0;
        }
        let root = root.unwrap();
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
        let mut existing_root = self.root();
        let other_root = o.root();
        if existing_root.is_none() && other_root.is_none() {
            return;
        }
        if other_root.is_none() {
            return;
        }
        if existing_root.is_none() {
            *self = o.clone();
            return;
        }
        if o.info.last_update.gt(&self.info.last_update) {
            self.info.last_update = o.info.last_update;
        }

        existing_root.as_mut().unwrap().merge(other_root.as_ref().unwrap());
        self.cache.insert(hash_path(&self.info.name).key(), existing_root.unwrap());
        let e_hash = self.root_hash();
        for key in other_root.as_ref().unwrap().children.iter() {
            let entry = &o.cache[key];
            let flat = o.flatten(entry);
            let mut existing = self.cache[key].clone();
            existing.merge(&flat);
            self.replace_hashed(&DataUsageHash(key.clone()), &Some(e_hash.clone()), &existing);
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
            buckets_count: e.children.len() as u64,
            buckets_usage,
            ..Default::default()
        }
    }

    pub fn marshal_msg(&self) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        let mut buf = Vec::new();
        self.serialize(&mut rmp_serde::Serializer::new(&mut buf))?;
        Ok(buf)
    }

    pub fn unmarshal(buf: &[u8]) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let t: Self = rmp_serde::from_slice(buf)?;
        Ok(t)
    }

    // Note: load and save methods are storage-specific and should be implemented
    // in the ecstore crate where storage access is available
}

/// Trait for storage-specific operations on DataUsageCache
#[async_trait::async_trait]
pub trait DataUsageCacheStorage {
    /// Load data usage cache from backend storage
    async fn load(store: &dyn std::any::Any, name: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>>
    where
        Self: Sized;

    /// Save data usage cache to backend storage
    async fn save(&self, name: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

// Helper structs and functions for cache operations
#[derive(Default, Clone)]
struct Inner {
    objects: usize,
    path: DataUsageHash,
}

fn add(data_usage_cache: &DataUsageCache, path: &DataUsageHash, leaves: &mut Vec<Inner>) {
    let e = match data_usage_cache.cache.get(&path.key()) {
        Some(e) => e,
        None => return,
    };
    if !e.children.is_empty() {
        return;
    }

    let sz = data_usage_cache.size_recursive(&path.key()).unwrap_or_default();
    leaves.push(Inner {
        objects: sz.objects,
        path: path.clone(),
    });
    for ch in e.children.iter() {
        add(data_usage_cache, &DataUsageHash(ch.clone()), leaves);
    }
}

fn mark(duc: &DataUsageCache, entry: &DataUsageEntry, found: &mut HashSet<String>) {
    for k in entry.children.iter() {
        found.insert(k.to_string());
        if let Some(ch) = duc.cache.get(k) {
            mark(duc, ch, found);
        }
    }
}

/// Hash a path for data usage caching
pub fn hash_path(data: &str) -> DataUsageHash {
    DataUsageHash(Path::new(&data).clean().to_string_lossy().to_string())
}

impl DataUsageInfo {
    /// Create a new DataUsageInfo
    pub fn new() -> Self {
        Self::default()
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
        self.buckets_usage.insert(bucket.clone(), usage);
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
            if self.last_update.is_none() || other_update > self.last_update.unwrap() {
                self.last_update = Some(other_update);
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
    pub fn add_size_summary(&mut self, summary: &SizeSummary) {
        self.size += summary.total_size as u64;
        self.versions_count += summary.versions as u64;
        self.delete_markers_count += summary.delete_markers as u64;
        self.replica_size += summary.replica_size as u64;
        self.replica_count += summary.replica_count as u64;
    }

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

    /// Add another SizeSummary to this one
    pub fn add(&mut self, other: &SizeSummary) {
        self.total_size += other.total_size;
        self.versions += other.versions;
        self.delete_markers += other.delete_markers;
        self.replicated_size += other.replicated_size;
        self.replicated_count += other.replicated_count;
        self.pending_size += other.pending_size;
        self.failed_size += other.failed_size;
        self.replica_size += other.replica_size;
        self.replica_count += other.replica_count;
        self.pending_count += other.pending_count;
        self.failed_count += other.failed_count;

        // Merge replication target stats
        for (target, stats) in &other.repl_target_stats {
            let entry = self.repl_target_stats.entry(target.clone()).or_default();
            entry.replicated_size += stats.replicated_size;
            entry.replicated_count += stats.replicated_count;
            entry.pending_size += stats.pending_size;
            entry.failed_size += stats.failed_size;
            entry.pending_count += stats.pending_count;
            entry.failed_count += stats.failed_count;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
