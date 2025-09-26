use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use tokio::fs;

use crate::data_usage::BucketUsageInfo;
use crate::disk::RUSTFS_META_BUCKET;
use crate::error::{Error, Result};

/// Directory used to store per-disk usage snapshots under the metadata bucket.
pub const DATA_USAGE_DIR: &str = "datausage";
/// Directory used to store incremental scan state files under the metadata bucket.
pub const DATA_USAGE_STATE_DIR: &str = "datausage/state";
/// Snapshot file format version, allows forward compatibility if the structure evolves.
pub const LOCAL_USAGE_SNAPSHOT_VERSION: u32 = 1;

/// Additional metadata describing which disk produced the snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LocalUsageSnapshotMeta {
    /// Disk UUID stored as a string for simpler serialization.
    pub disk_id: String,
    /// Pool index if this disk is bound to a specific pool.
    pub pool_index: Option<usize>,
    /// Set index if known.
    pub set_index: Option<usize>,
    /// Disk index inside the set if known.
    pub disk_index: Option<usize>,
}

/// Usage snapshot produced by a single disk.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LocalUsageSnapshot {
    /// Format version recorded in the snapshot.
    pub format_version: u32,
    /// Snapshot metadata, including disk identity.
    pub meta: LocalUsageSnapshotMeta,
    /// Wall-clock timestamp when the snapshot was produced.
    pub last_update: Option<SystemTime>,
    /// Per-bucket usage statistics.
    pub buckets_usage: HashMap<String, BucketUsageInfo>,
    /// Cached bucket count to speed up aggregations.
    pub buckets_count: u64,
    /// Total objects counted on this disk.
    pub objects_total_count: u64,
    /// Total versions counted on this disk.
    pub versions_total_count: u64,
    /// Total delete markers counted on this disk.
    pub delete_markers_total_count: u64,
    /// Total bytes occupied by objects on this disk.
    pub objects_total_size: u64,
}

impl LocalUsageSnapshot {
    /// Create an empty snapshot with the default format version filled in.
    pub fn new(meta: LocalUsageSnapshotMeta) -> Self {
        Self {
            format_version: LOCAL_USAGE_SNAPSHOT_VERSION,
            meta,
            ..Default::default()
        }
    }

    /// Recalculate cached totals from the per-bucket map.
    pub fn recompute_totals(&mut self) {
        let mut buckets_count = 0u64;
        let mut objects_total_count = 0u64;
        let mut versions_total_count = 0u64;
        let mut delete_markers_total_count = 0u64;
        let mut objects_total_size = 0u64;

        for usage in self.buckets_usage.values() {
            buckets_count = buckets_count.saturating_add(1);
            objects_total_count = objects_total_count.saturating_add(usage.objects_count);
            versions_total_count = versions_total_count.saturating_add(usage.versions_count);
            delete_markers_total_count = delete_markers_total_count.saturating_add(usage.delete_markers_count);
            objects_total_size = objects_total_size.saturating_add(usage.size);
        }

        self.buckets_count = buckets_count;
        self.objects_total_count = objects_total_count;
        self.versions_total_count = versions_total_count;
        self.delete_markers_total_count = delete_markers_total_count;
        self.objects_total_size = objects_total_size;
    }
}

/// Build the snapshot file name `<disk-id>.json`.
pub fn snapshot_file_name(disk_id: &str) -> String {
    format!("{disk_id}.json")
}

/// Build the object path relative to `RUSTFS_META_BUCKET`, e.g. `datausage/<disk-id>.json`.
pub fn snapshot_object_path(disk_id: &str) -> String {
    format!("{}/{}", DATA_USAGE_DIR, snapshot_file_name(disk_id))
}

/// Return the absolute path to `.rustfs.sys/datausage` on the given disk root.
pub fn data_usage_dir(root: &Path) -> PathBuf {
    root.join(RUSTFS_META_BUCKET).join(DATA_USAGE_DIR)
}

/// Return the absolute path to `.rustfs.sys/datausage/state` on the given disk root.
pub fn data_usage_state_dir(root: &Path) -> PathBuf {
    root.join(RUSTFS_META_BUCKET).join(DATA_USAGE_STATE_DIR)
}

/// Build the absolute path to the snapshot file for the provided disk ID.
pub fn snapshot_path(root: &Path, disk_id: &str) -> PathBuf {
    data_usage_dir(root).join(snapshot_file_name(disk_id))
}

/// Read a snapshot from disk if it exists.
pub async fn read_snapshot(root: &Path, disk_id: &str) -> Result<Option<LocalUsageSnapshot>> {
    let path = snapshot_path(root, disk_id);
    match fs::read(&path).await {
        Ok(content) => {
            let snapshot = serde_json::from_slice::<LocalUsageSnapshot>(&content)
                .map_err(|err| Error::other(format!("failed to deserialize snapshot {path:?}: {err}")))?;
            Ok(Some(snapshot))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(Error::other(err)),
    }
}

/// Persist a snapshot to disk, creating directories as needed and overwriting any existing file.
pub async fn write_snapshot(root: &Path, disk_id: &str, snapshot: &LocalUsageSnapshot) -> Result<()> {
    let dir = data_usage_dir(root);
    fs::create_dir_all(&dir).await.map_err(Error::other)?;
    let path = dir.join(snapshot_file_name(disk_id));
    let data = serde_json::to_vec_pretty(snapshot)
        .map_err(|err| Error::other(format!("failed to serialize snapshot {path:?}: {err}")))?;
    fs::write(&path, data).await.map_err(Error::other)
}

/// Ensure that the data usage directory structure exists on this disk root.
pub async fn ensure_data_usage_layout(root: &Path) -> Result<()> {
    let usage_dir = data_usage_dir(root);
    fs::create_dir_all(&usage_dir).await.map_err(Error::other)?;
    let state_dir = data_usage_state_dir(root);
    fs::create_dir_all(&state_dir).await.map_err(Error::other)?;
    Ok(())
}
