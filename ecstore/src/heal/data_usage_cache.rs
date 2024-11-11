use http::HeaderMap;
use rand::Rng;
use rmp_serde::Serializer;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::{Duration, SystemTime};
use s3s::{S3Error, S3ErrorCode};
use tokio::sync::mpsc::Sender;
use tokio::time::sleep;
use crate::config::common::save_config;
use crate::disk::error::DiskError;
use crate::disk::{BUCKET_META_PREFIX, RUSTFS_META_BUCKET};
use crate::error::{Error, Result};
use crate::new_object_layer_fn;
use crate::set_disk::SetDisks;
use crate::store_api::{HTTPRangeSpec, ObjectIO, ObjectOptions, StorageAPI};

use super::data_usage::DATA_USAGE_ROOT;

// DATA_USAGE_BUCKET_LEN must be length of ObjectsHistogramIntervals
pub const DATA_USAGE_BUCKET_LEN: usize = 11;
pub const DATA_USAGE_VERSION_LEN: usize = 7;

type DataUsageHashMap = HashSet<String>;
// sizeHistogram is a size histogram.
type SizeHistogram = Vec<u64>;
// versionsHistogram is a histogram of number of versions in an object.
type VersionsHistogram = Vec<u64>;

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Clone, Serialize, Deserialize)]
pub struct DataUsageEntry {
    pub children: DataUsageHashMap,
    // These fields do no include any children.
    pub size: i64,
    pub objects: u64,
    pub versions: u64,
    pub delete_markers: u64,
    pub obj_sizes: SizeHistogram,
    pub obj_versions: VersionsHistogram,
    pub replication_stats: ReplicationAllStats,
    // Todo: tier
    // pub all_tier_stats: ,
    pub compacted: bool,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct DataUsageCacheInfo {
    pub name: String,
    pub next_cycle: usize,
    pub last_update: SystemTime,
    pub skip_healing: bool,
    // todo: life_cycle
    // pub life_cycle:
    #[serde(skip)]
    pub updates: Option<Sender<DataUsageEntry>>,
    // Todo: replication
    // #[serde(skip_serializing)]
    // replication:
}

impl Default for DataUsageCacheInfo {
    fn default() -> Self {
        Self {
            name: Default::default(),
            next_cycle: Default::default(),
            last_update: SystemTime::now(),
            skip_healing: Default::default(),
            updates: Default::default(),
        }
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
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
            match store
                .get_object_reader(
                    RUSTFS_META_BUCKET,
                    path.to_str().unwrap(),
                    HTTPRangeSpec::nil(),
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
                Err(err) => match err.downcast_ref::<DiskError>() {
                    Some(DiskError::FileNotFound) | Some(DiskError::VolumeNotFound) => {
                        match store
                            .get_object_reader(
                                RUSTFS_META_BUCKET,
                                name,
                                HTTPRangeSpec::nil(),
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
                    _ => {}
                },
            }
            retries += 1;
            let mut rng = rand::thread_rng();
            sleep(Duration::from_millis(rng.gen_range(0..1_000))).await;
        }
        Ok(d)
    }
    
    pub async fn save(&self, name: &str) -> Result<()> {
        let buf = self.marshal_msg()?;
        let buf_clone = buf.clone();
        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = match lock.as_ref() {
            Some(s) => s,
            None => return Err(Error::from(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()))),
        };
        let store_clone = store.clone();
        let name_clone = name.to_string();
        tokio::spawn(async move {
            let _ = save_config(&store_clone, &format!("{}{}", &name_clone, ".bkp"), &buf_clone).await;
        });
        save_config(&store, name, &buf).await
    }
    
    pub fn replace(&mut self, path: &str, parent: &str, e: DataUsageEntry) {
        let hash = hash_path(path);
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

struct DataUsageHash(String);

impl DataUsageHash {
    
}

pub fn hash_path(data: &str) -> DataUsageHash {
    let mut data = data;
    if data != DATA_USAGE_ROOT {
        data = data.trim_matches('/');
    }
    Path::new(&data);
    todo!()
}
