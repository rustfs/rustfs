use crate::config::common::save_config;
use crate::disk::error::DiskError;
use crate::disk::{BUCKET_META_PREFIX, RUSTFS_META_BUCKET};
use crate::error::{Error, Result};
use crate::new_object_layer_fn;
use crate::set_disk::SetDisks;
use crate::store_api::{BucketInfo, ObjectIO, ObjectOptions};
use bytesize::ByteSize;
use http::HeaderMap;
use path_clean::PathClean;
use rand::Rng;
use rmp_serde::Serializer;
use s3s::dto::ReplicationConfiguration;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::Path;
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc::Sender;
use tokio::time::sleep;
use tracing::warn;

use super::data_scanner::{SizeSummary, DATA_SCANNER_FORCE_COMPACT_AT_FOLDERS};
use super::data_usage::{BucketTargetUsageInfo, BucketUsageInfo, DataUsageInfo, DATA_USAGE_ROOT};

// DATA_USAGE_BUCKET_LEN must be length of ObjectsHistogramIntervals
pub const DATA_USAGE_BUCKET_LEN: usize = 11;
pub const DATA_USAGE_VERSION_LEN: usize = 7;

pub type DataUsageHashMap = HashSet<String>;

struct ObjectHistogramInterval {
    name: &'static str,
    start: u64,
    end: u64,
}

const OBJECTS_HISTOGRAM_INTERVALS: [ObjectHistogramInterval; DATA_USAGE_BUCKET_LEN] = [
    ObjectHistogramInterval {
        name: "LESS_THAN_1024_B",
        start: 0,
        end: ByteSize::kib(1).as_u64() - 1,
    },
    ObjectHistogramInterval {
        name: "BETWEEN_1024_B_AND_64_KB",
        start: ByteSize::kib(1).as_u64(),
        end: ByteSize::kib(64).as_u64() - 1,
    },
    ObjectHistogramInterval {
        name: "BETWEEN_64_KB_AND_256_KB",
        start: ByteSize::kib(64).as_u64(),
        end: ByteSize::kib(256).as_u64() - 1,
    },
    ObjectHistogramInterval {
        name: "BETWEEN_256_KB_AND_512_KB",
        start: ByteSize::kib(256).as_u64(),
        end: ByteSize::kib(512).as_u64() - 1,
    },
    ObjectHistogramInterval {
        name: "BETWEEN_512_KB_AND_1_MB",
        start: ByteSize::kib(512).as_u64(),
        end: ByteSize::mib(1).as_u64() - 1,
    },
    ObjectHistogramInterval {
        name: "BETWEEN_1024B_AND_1_MB",
        start: ByteSize::kib(1).as_u64(),
        end: ByteSize::mib(1).as_u64() - 1,
    },
    ObjectHistogramInterval {
        name: "BETWEEN_1_MB_AND_10_MB",
        start: ByteSize::mib(1).as_u64(),
        end: ByteSize::mib(10).as_u64() - 1,
    },
    ObjectHistogramInterval {
        name: "BETWEEN_10_MB_AND_64_MB",
        start: ByteSize::mib(10).as_u64(),
        end: ByteSize::mib(64).as_u64() - 1,
    },
    ObjectHistogramInterval {
        name: "BETWEEN_64_MB_AND_128_MB",
        start: ByteSize::mib(64).as_u64(),
        end: ByteSize::mib(128).as_u64() - 1,
    },
    ObjectHistogramInterval {
        name: "BETWEEN_128_MB_AND_512_MB",
        start: ByteSize::mib(128).as_u64(),
        end: ByteSize::mib(512).as_u64() - 1,
    },
    ObjectHistogramInterval {
        name: "GREATER_THAN_512_MB",
        start: ByteSize::mib(512).as_u64(),
        end: u64::MAX,
    },
];

const OBJECTS_VERSION_COUNT_INTERVALS: [ObjectHistogramInterval; DATA_USAGE_VERSION_LEN] = [
    ObjectHistogramInterval {
        name: "UNVERSIONED",
        start: 0,
        end: 0,
    },
    ObjectHistogramInterval {
        name: "SINGLE_VERSION",
        start: 1,
        end: 1,
    },
    ObjectHistogramInterval {
        name: "BETWEEN_2_AND_10",
        start: 2,
        end: 9,
    },
    ObjectHistogramInterval {
        name: "BETWEEN_10_AND_100",
        start: 10,
        end: 99,
    },
    ObjectHistogramInterval {
        name: "BETWEEN_100_AND_1000",
        start: 100,
        end: 999,
    },
    ObjectHistogramInterval {
        name: "BETWEEN_1000_AND_10000",
        start: 1000,
        end: 9999,
    },
    ObjectHistogramInterval {
        name: "GREATER_THAN_10000",
        start: 10000,
        end: u64::MAX,
    },
];

// sizeHistogram is a size histogram.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SizeHistogram(Vec<u64>);

impl Default for SizeHistogram {
    fn default() -> Self {
        Self(vec![0; DATA_USAGE_BUCKET_LEN])
    }
}

impl SizeHistogram {
    fn add(&mut self, size: u64) {
        for (idx, interval) in OBJECTS_HISTOGRAM_INTERVALS.iter().enumerate() {
            if size >= interval.start && size <= interval.end {
                self.0[idx] += 1;
                break;
            }
        }
    }

    pub fn to_map(&self) -> HashMap<String, u64> {
        let mut res = HashMap::new();
        let mut spl_count = 0;
        for (count, oh) in self.0.iter().zip(OBJECTS_HISTOGRAM_INTERVALS.iter()) {
            if ByteSize::kib(1).as_u64() == oh.start && oh.end == ByteSize::mib(1).as_u64() - 1 {
                res.insert(oh.name.to_string(), spl_count);
            } else if ByteSize::kib(1).as_u64() <= oh.start && oh.end < ByteSize::mib(1).as_u64() {
                spl_count += count;
                res.insert(oh.name.to_string(), *count);
            } else {
                res.insert(oh.name.to_string(), *count);
            }
        }
        res
    }
}

// versionsHistogram is a histogram of number of versions in an object.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VersionsHistogram(Vec<u64>);

impl Default for VersionsHistogram {
    fn default() -> Self {
        Self(vec![0; DATA_USAGE_VERSION_LEN])
    }
}

impl VersionsHistogram {
    fn add(&mut self, size: u64) {
        for (idx, interval) in OBJECTS_VERSION_COUNT_INTERVALS.iter().enumerate() {
            if size >= interval.start && size <= interval.end {
                self.0[idx] += 1;
                break;
            }
        }
    }

    pub fn to_map(&self) -> HashMap<String, u64> {
        let mut res = HashMap::new();
        for (count, ov) in self.0.iter().zip(OBJECTS_VERSION_COUNT_INTERVALS.iter()) {
            res.insert(ov.name.to_string(), *count);
        }
        res
    }
}

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

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DataUsageEntry {
    pub children: DataUsageHashMap,
    // These fields do no include any children.
    pub size: usize,
    pub objects: usize,
    pub versions: usize,
    pub delete_markers: usize,
    pub obj_sizes: SizeHistogram,
    pub obj_versions: VersionsHistogram,
    pub replication_stats: Option<ReplicationAllStats>,
    // Todo: tier
    // pub all_tier_stats: ,
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
        // Todo:: tiers
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

        // todo: tiers
    }
}

#[derive(Clone)]
pub struct DataUsageEntryInfo {
    pub name: String,
    pub parent: String,
    pub entry: DataUsageEntry,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DataUsageCacheInfo {
    pub name: String,
    pub next_cycle: u32,
    pub last_update: Option<SystemTime>,
    pub skip_healing: bool,
    // todo: life_cycle
    // pub life_cycle:
    #[serde(skip)]
    pub updates: Option<Sender<DataUsageEntry>>,
    #[serde(skip)]
    pub replication: Option<ReplicationConfiguration>,
}

// impl Default for DataUsageCacheInfo {
//     fn default() -> Self {
//         Self {
//             name: Default::default(),
//             next_cycle: Default::default(),
//             last_update: SystemTime::now(),
//             skip_healing: Default::default(),
//             updates: Default::default(),
//             replication: Default::default(),
//         }
//     }
// }

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DataUsageCache {
    pub info: DataUsageCacheInfo,
    pub cache: HashMap<String, DataUsageEntry>,
}

impl DataUsageCache {
    pub async fn load(store: &SetDisks, name: &str) -> Result<Self> {
        let mut d = DataUsageCache::default();
        let mut retries = 0;
        while retries < 5 {
            let path = Path::new(BUCKET_META_PREFIX).join(name);
            warn!("Loading data usage cache from backend: {}", path.display());
            match store
                .get_object_reader(
                    RUSTFS_META_BUCKET,
                    path.to_str().unwrap(),
                    None,
                    HeaderMap::new(),
                    &ObjectOptions {
                        no_lock: true,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(mut reader) => {
                    if let Ok(info) = Self::unmarshal(&reader.read_all().await?) {
                        d = info
                    }
                    break;
                }
                Err(err) => {
                    warn!("Failed to load data usage cache from backend: {}", &err);
                    match err.downcast_ref::<DiskError>() {
                        Some(DiskError::FileNotFound) | Some(DiskError::VolumeNotFound) => {
                            match store
                                .get_object_reader(
                                    RUSTFS_META_BUCKET,
                                    name,
                                    None,
                                    HeaderMap::new(),
                                    &ObjectOptions {
                                        no_lock: true,
                                        ..Default::default()
                                    },
                                )
                                .await
                            {
                                Ok(mut reader) => {
                                    if let Ok(info) = Self::unmarshal(&reader.read_all().await?) {
                                        d = info
                                    }
                                    break;
                                }
                                Err(_) => match err.downcast_ref::<DiskError>() {
                                    Some(DiskError::FileNotFound) | Some(DiskError::VolumeNotFound) => {
                                        break;
                                    }
                                    _ => {}
                                },
                            }
                        }
                        _ => {
                            break;
                        }
                    }
                }
            }
            retries += 1;
            let dur = {
                let mut rng = rand::thread_rng();
                rng.gen_range(0..1_000)
            };
            sleep(Duration::from_millis(dur)).await;
        }
        Ok(d)
    }

    pub async fn save(&self, name: &str) -> Result<()> {
        let Some(store) = new_object_layer_fn() else { return Err(Error::msg("errServerNotInitialized")) };
        let buf = self.marshal_msg()?;
        let buf_clone = buf.clone();

        let store_clone = store.clone();

        let name = Path::new(BUCKET_META_PREFIX).join(name).to_string_lossy().to_string();

        let name_clone = name.clone();
        tokio::spawn(async move {
            let _ = save_config(store_clone, &format!("{}{}", &name_clone, ".bkp"), buf_clone).await;
        });
        save_config(store, &name, buf).await
    }

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
                    return None;
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
        if top_e.children.len() > DATA_SCANNER_FORCE_COMPACT_AT_FOLDERS.try_into().unwrap() {
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

    pub fn dui(&self, path: &str, buckets: &[BucketInfo]) -> DataUsageInfo {
        let e = match self.find(path) {
            Some(e) => e,
            None => return DataUsageInfo::default(),
        };
        let flat = self.flatten(&e);
        DataUsageInfo {
            last_update: self.info.last_update,
            objects_total_count: flat.objects as u64,
            versions_total_count: flat.versions as u64,
            delete_markers_total_count: flat.delete_markers as u64,
            objects_total_size: flat.size as u64,
            buckets_count: e.children.len() as u64,
            buckets_usage: self.buckets_usage_info(buckets),
            ..Default::default()
        }
    }

    pub fn buckets_usage_info(&self, buckets: &[BucketInfo]) -> HashMap<String, BucketUsageInfo> {
        let mut dst = HashMap::new();
        for bucket in buckets.iter() {
            let e = match self.find(&bucket.name) {
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
            dst.insert(bucket.name.clone(), bui);
        }
        dst
    }

    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        self.serialize(&mut Serializer::new(&mut buf))?;

        Ok(buf)
    }

    pub fn unmarshal(buf: &[u8]) -> Result<Self> {
        let t: Self = rmp_serde::from_slice(buf)?;
        Ok(t)
    }
}

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

pub fn hash_path(data: &str) -> DataUsageHash {
    let mut data = data;
    if data != DATA_USAGE_ROOT {
        data = data.trim_matches('/');
    }
    DataUsageHash(Path::new(&data).clean().to_string_lossy().to_string())
}
