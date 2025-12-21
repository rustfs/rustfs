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

use crate::{Error, Result};
use rustfs_common::data_usage::DiskUsageStatus;
use rustfs_ecstore::data_usage::{
    LocalUsageSnapshot, LocalUsageSnapshotMeta, data_usage_state_dir, ensure_data_usage_layout, snapshot_file_name,
    write_local_snapshot,
};
use rustfs_ecstore::disk::DiskAPI;
use rustfs_ecstore::store::ECStore;
use rustfs_ecstore::store_api::ObjectInfo;
use rustfs_filemeta::{FileInfo, FileMeta, FileMetaVersion, VersionType};
use serde::{Deserialize, Serialize};
use serde_json::{from_slice, to_vec};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::{fs, task};
use tracing::warn;
use walkdir::WalkDir;

const STATE_FILE_EXTENSION: &str = "";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LocalObjectUsage {
    pub bucket: String,
    pub object: String,
    pub last_modified_ns: Option<i128>,
    pub versions_count: u64,
    pub delete_markers_count: u64,
    pub total_size: u64,
    pub has_live_object: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct IncrementalScanState {
    last_scan_ns: Option<i128>,
    objects: HashMap<String, LocalObjectUsage>,
}

struct DiskScanResult {
    snapshot: LocalUsageSnapshot,
    state: IncrementalScanState,
    objects_by_bucket: HashMap<String, Vec<LocalObjectRecord>>,
    status: DiskUsageStatus,
}

#[derive(Debug, Clone)]
pub struct LocalObjectRecord {
    pub usage: LocalObjectUsage,
    pub object_info: Option<rustfs_ecstore::store_api::ObjectInfo>,
}

#[derive(Debug, Default)]
pub struct LocalScanOutcome {
    pub snapshots: Vec<LocalUsageSnapshot>,
    pub bucket_objects: HashMap<String, Vec<LocalObjectRecord>>,
    pub disk_status: Vec<DiskUsageStatus>,
}

/// Scan all local primary disks and persist refreshed usage snapshots.
pub async fn scan_and_persist_local_usage(store: Arc<ECStore>) -> Result<LocalScanOutcome> {
    let mut snapshots = Vec::new();
    let mut bucket_objects: HashMap<String, Vec<LocalObjectRecord>> = HashMap::new();
    let mut disk_status = Vec::new();

    for (pool_idx, pool) in store.pools.iter().enumerate() {
        for set_disks in pool.disk_set.iter() {
            let disks = {
                let guard = set_disks.disks.read().await;
                guard.clone()
            };

            // Use the first local online disk in the set to avoid missing stats when disk 0 is down
            let mut picked = false;

            for (disk_index, disk_opt) in disks.into_iter().enumerate() {
                let Some(disk) = disk_opt else {
                    continue;
                };

                if !disk.is_local() {
                    continue;
                }

                if picked {
                    continue;
                }

                // Skip offline disks; keep looking for an online candidate
                if !disk.is_online().await {
                    continue;
                }

                picked = true;

                let disk_id = match disk.get_disk_id().await.map_err(Error::from)? {
                    Some(id) => id.to_string(),
                    None => {
                        warn!("Skipping disk without ID: {}", disk.to_string());
                        continue;
                    }
                };

                let root = disk.path();
                ensure_data_usage_layout(root.as_path()).await.map_err(Error::from)?;

                let meta = LocalUsageSnapshotMeta {
                    disk_id: disk_id.clone(),
                    pool_index: Some(pool_idx),
                    set_index: Some(set_disks.set_index),
                    disk_index: Some(disk_index),
                };

                let state_path = state_file_path(root.as_path(), &disk_id);
                let state = read_scan_state(&state_path).await?;

                let root_clone = root.clone();
                let meta_clone = meta.clone();

                let handle = task::spawn_blocking(move || scan_disk_blocking(root_clone, meta_clone, state));

                match handle.await {
                    Ok(Ok(result)) => {
                        write_local_snapshot(root.as_path(), &disk_id, &result.snapshot)
                            .await
                            .map_err(Error::from)?;
                        write_scan_state(&state_path, &result.state).await?;
                        snapshots.push(result.snapshot);
                        for (bucket, records) in result.objects_by_bucket {
                            bucket_objects.entry(bucket).or_default().extend(records.into_iter());
                        }
                        disk_status.push(result.status);
                    }
                    Ok(Err(err)) => {
                        warn!("Failed to scan disk {}: {}", disk.to_string(), err);
                    }
                    Err(join_err) => {
                        warn!("Disk scan task panicked for disk {}: {}", disk.to_string(), join_err);
                    }
                }
            }
        }
    }

    Ok(LocalScanOutcome {
        snapshots,
        bucket_objects,
        disk_status,
    })
}

fn scan_disk_blocking(root: PathBuf, meta: LocalUsageSnapshotMeta, mut state: IncrementalScanState) -> Result<DiskScanResult> {
    let now = SystemTime::now();
    let now_ns = system_time_to_ns(now);
    let mut visited: HashSet<String> = HashSet::new();
    let mut emitted: HashSet<String> = HashSet::new();
    let mut objects_by_bucket: HashMap<String, Vec<LocalObjectRecord>> = HashMap::new();
    let mut status = DiskUsageStatus {
        disk_id: meta.disk_id.clone(),
        pool_index: meta.pool_index,
        set_index: meta.set_index,
        disk_index: meta.disk_index,
        last_update: None,
        snapshot_exists: false,
    };

    for entry in WalkDir::new(&root).follow_links(false).into_iter().filter_map(|res| res.ok()) {
        if !entry.file_type().is_file() {
            continue;
        }

        if entry.file_name() != "xl.meta" {
            continue;
        }

        let xl_path = entry.path().to_path_buf();
        let Some(object_dir) = xl_path.parent() else {
            continue;
        };

        let Some(rel_path) = object_dir.strip_prefix(&root).ok().map(normalize_path) else {
            continue;
        };

        let mut components = rel_path.split('/');
        let Some(bucket_name) = components.next() else {
            continue;
        };

        if bucket_name.starts_with('.') {
            continue;
        }

        let object_key = components.collect::<Vec<_>>().join("/");

        visited.insert(rel_path.clone());

        let metadata = match std::fs::metadata(&xl_path) {
            Ok(meta) => meta,
            Err(err) => {
                warn!("Failed to read metadata for {xl_path:?}: {err}");
                continue;
            }
        };

        let mtime_ns = metadata.modified().ok().map(system_time_to_ns);

        let should_parse = match state.objects.get(&rel_path) {
            Some(existing) => existing.last_modified_ns != mtime_ns,
            None => true,
        };

        if should_parse {
            match std::fs::read(&xl_path) {
                Ok(buf) => match FileMeta::load(&buf) {
                    Ok(file_meta) => match compute_object_usage(bucket_name, object_key.as_str(), &file_meta) {
                        Ok(Some(mut record)) => {
                            record.usage.last_modified_ns = mtime_ns;
                            state.objects.insert(rel_path.clone(), record.usage.clone());
                            emitted.insert(rel_path.clone());
                            objects_by_bucket.entry(record.usage.bucket.clone()).or_default().push(record);
                        }
                        Ok(None) => {
                            state.objects.remove(&rel_path);
                        }
                        Err(err) => {
                            warn!("Failed to parse usage from {:?}: {}", xl_path, err);
                        }
                    },
                    Err(err) => {
                        warn!("Failed to decode xl.meta {:?}: {}", xl_path, err);
                    }
                },
                Err(err) => {
                    warn!("Failed to read xl.meta {:?}: {}", xl_path, err);
                }
            }
        }
    }

    state.objects.retain(|key, _| visited.contains(key));
    state.last_scan_ns = Some(now_ns);

    for (key, usage) in &state.objects {
        if emitted.contains(key) {
            continue;
        }
        objects_by_bucket
            .entry(usage.bucket.clone())
            .or_default()
            .push(LocalObjectRecord {
                usage: usage.clone(),
                object_info: None,
            });
    }

    let snapshot = build_snapshot(meta, &state.objects, now);
    status.snapshot_exists = true;
    status.last_update = Some(now);

    Ok(DiskScanResult {
        snapshot,
        state,
        objects_by_bucket,
        status,
    })
}

fn compute_object_usage(bucket: &str, object: &str, file_meta: &FileMeta) -> Result<Option<LocalObjectRecord>> {
    let mut versions_count = 0u64;
    let mut delete_markers_count = 0u64;
    let mut total_size = 0u64;
    let mut has_live_object = false;

    let mut latest_file_info: Option<FileInfo> = None;

    for shallow in &file_meta.versions {
        match shallow.header.version_type {
            VersionType::Object => {
                let version = match FileMetaVersion::try_from(shallow.meta.as_slice()) {
                    Ok(version) => version,
                    Err(err) => {
                        warn!("Failed to parse file meta version: {}", err);
                        continue;
                    }
                };
                if let Some(obj) = version.object {
                    if !has_live_object {
                        total_size = obj.size.max(0) as u64;
                    }
                    has_live_object = true;
                    versions_count = versions_count.saturating_add(1);

                    if latest_file_info.is_none() {
                        if let Ok(info) = file_meta.into_fileinfo(bucket, object, "", false, false) {
                            latest_file_info = Some(info);
                        }
                    }
                }
            }
            VersionType::Delete => {
                delete_markers_count = delete_markers_count.saturating_add(1);
                versions_count = versions_count.saturating_add(1);
            }
            _ => {}
        }
    }

    if !has_live_object && delete_markers_count == 0 {
        return Ok(None);
    }

    let object_info = latest_file_info.as_ref().map(|fi| {
        let versioned = fi.version_id.is_some();
        ObjectInfo::from_file_info(fi, bucket, object, versioned)
    });

    Ok(Some(LocalObjectRecord {
        usage: LocalObjectUsage {
            bucket: bucket.to_string(),
            object: object.to_string(),
            last_modified_ns: None,
            versions_count,
            delete_markers_count,
            total_size,
            has_live_object,
        },
        object_info,
    }))
}

fn build_snapshot(
    meta: LocalUsageSnapshotMeta,
    objects: &HashMap<String, LocalObjectUsage>,
    now: SystemTime,
) -> LocalUsageSnapshot {
    let mut snapshot = LocalUsageSnapshot::new(meta);

    for usage in objects.values() {
        let bucket_entry = snapshot.buckets_usage.entry(usage.bucket.clone()).or_default();

        if usage.has_live_object {
            bucket_entry.objects_count = bucket_entry.objects_count.saturating_add(1);
        }
        bucket_entry.versions_count = bucket_entry.versions_count.saturating_add(usage.versions_count);
        bucket_entry.delete_markers_count = bucket_entry.delete_markers_count.saturating_add(usage.delete_markers_count);
        bucket_entry.size = bucket_entry.size.saturating_add(usage.total_size);
    }

    snapshot.last_update = Some(now);
    snapshot.recompute_totals();
    snapshot
}

fn normalize_path(path: &Path) -> String {
    path.iter()
        .map(|component| component.to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

fn system_time_to_ns(time: SystemTime) -> i128 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => {
            let secs = duration.as_secs() as i128;
            let nanos = duration.subsec_nanos() as i128;
            secs * 1_000_000_000 + nanos
        }
        Err(err) => {
            let duration = err.duration();
            let secs = duration.as_secs() as i128;
            let nanos = duration.subsec_nanos() as i128;
            -(secs * 1_000_000_000 + nanos)
        }
    }
}

fn state_file_path(root: &Path, disk_id: &str) -> PathBuf {
    let mut path = data_usage_state_dir(root);
    path.push(format!("{}{}", snapshot_file_name(disk_id), STATE_FILE_EXTENSION));
    path
}

async fn read_scan_state(path: &Path) -> Result<IncrementalScanState> {
    match fs::read(path).await {
        Ok(bytes) => from_slice(&bytes).map_err(|err| Error::Serialization(err.to_string())),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(IncrementalScanState::default()),
        Err(err) => Err(err.into()),
    }
}

async fn write_scan_state(path: &Path, state: &IncrementalScanState) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    let data = to_vec(state).map_err(|err| Error::Serialization(err.to_string()))?;
    fs::write(path, data).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_filemeta::{ChecksumAlgo, ErasureAlgo, FileMetaShallowVersion, MetaDeleteMarker, MetaObject};
    use std::collections::HashMap;
    use std::fs;
    use tempfile::TempDir;
    use time::OffsetDateTime;
    use uuid::Uuid;

    fn build_file_meta_with_object(erasure_index: usize, size: i64) -> FileMeta {
        let mut file_meta = FileMeta::default();

        let meta_object = MetaObject {
            version_id: Some(Uuid::new_v4()),
            data_dir: Some(Uuid::new_v4()),
            erasure_algorithm: ErasureAlgo::ReedSolomon,
            erasure_m: 2,
            erasure_n: 2,
            erasure_block_size: 4096,
            erasure_index,
            erasure_dist: vec![0_u8, 1, 2, 3],
            bitrot_checksum_algo: ChecksumAlgo::HighwayHash,
            part_numbers: vec![1],
            part_etags: vec!["etag".to_string()],
            part_sizes: vec![size as usize],
            part_actual_sizes: vec![size],
            part_indices: Vec::new(),
            size,
            mod_time: Some(OffsetDateTime::now_utc()),
            meta_sys: HashMap::new(),
            meta_user: HashMap::new(),
        };

        let version = FileMetaVersion {
            version_type: VersionType::Object,
            object: Some(meta_object),
            delete_marker: None,
            write_version: 1,
        };

        let shallow = FileMetaShallowVersion::try_from(version).expect("convert version");
        file_meta.versions.push(shallow);
        file_meta
    }

    fn build_file_meta_with_delete_marker() -> FileMeta {
        let mut file_meta = FileMeta::default();

        let delete_marker = MetaDeleteMarker {
            version_id: Some(Uuid::new_v4()),
            mod_time: Some(OffsetDateTime::now_utc()),
            meta_sys: HashMap::new(),
        };

        let version = FileMetaVersion {
            version_type: VersionType::Delete,
            object: None,
            delete_marker: Some(delete_marker),
            write_version: 2,
        };

        let shallow = FileMetaShallowVersion::try_from(version).expect("convert delete marker");
        file_meta.versions.push(shallow);
        file_meta
    }

    #[test]
    fn compute_object_usage_primary_disk() {
        let file_meta = build_file_meta_with_object(0, 1024);
        let record = compute_object_usage("bucket", "foo/bar", &file_meta)
            .expect("compute usage")
            .expect("record should exist");

        assert!(record.usage.has_live_object);
        assert_eq!(record.usage.bucket, "bucket");
        assert_eq!(record.usage.object, "foo/bar");
        assert_eq!(record.usage.total_size, 1024);
        assert!(record.object_info.is_some(), "object info should be synthesized");
    }

    #[test]
    fn compute_object_usage_handles_non_primary_disk() {
        let file_meta = build_file_meta_with_object(1, 2048);
        let record = compute_object_usage("bucket", "obj", &file_meta)
            .expect("compute usage")
            .expect("record should exist for non-primary shard");
        assert!(record.usage.has_live_object);
    }

    #[test]
    fn compute_object_usage_reports_delete_marker() {
        let file_meta = build_file_meta_with_delete_marker();
        let record = compute_object_usage("bucket", "obj", &file_meta)
            .expect("compute usage")
            .expect("delete marker record");

        assert!(!record.usage.has_live_object);
        assert_eq!(record.usage.delete_markers_count, 1);
        assert_eq!(record.usage.versions_count, 1);
    }

    #[test]
    fn build_snapshot_accumulates_usage() {
        let mut objects = HashMap::new();
        objects.insert(
            "bucket/a".to_string(),
            LocalObjectUsage {
                bucket: "bucket".to_string(),
                object: "a".to_string(),
                last_modified_ns: None,
                versions_count: 2,
                delete_markers_count: 1,
                total_size: 512,
                has_live_object: true,
            },
        );

        let snapshot = build_snapshot(LocalUsageSnapshotMeta::default(), &objects, SystemTime::now());
        let usage = snapshot.buckets_usage.get("bucket").expect("bucket entry should exist");
        assert_eq!(usage.objects_count, 1);
        assert_eq!(usage.versions_count, 2);
        assert_eq!(usage.delete_markers_count, 1);
        assert_eq!(usage.size, 512);
    }

    #[test]
    fn scan_disk_blocking_handles_incremental_updates() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let root = temp_dir.path();

        let bucket_dir = root.join("bench");
        let object1_dir = bucket_dir.join("obj1");
        fs::create_dir_all(&object1_dir).expect("create first object directory");

        let file_meta = build_file_meta_with_object(0, 1024);
        let bytes = file_meta.marshal_msg().expect("serialize first object");
        fs::write(object1_dir.join("xl.meta"), bytes).expect("write first xl.meta");

        let meta = LocalUsageSnapshotMeta {
            disk_id: "disk-test".to_string(),
            ..Default::default()
        };

        let DiskScanResult {
            snapshot: snapshot1,
            state,
            ..
        } = scan_disk_blocking(root.to_path_buf(), meta.clone(), IncrementalScanState::default()).expect("initial scan succeeds");

        let usage1 = snapshot1.buckets_usage.get("bench").expect("bucket stats recorded");
        assert_eq!(usage1.objects_count, 1);
        assert_eq!(usage1.size, 1024);
        assert_eq!(state.objects.len(), 1);

        let object2_dir = bucket_dir.join("nested").join("obj2");
        fs::create_dir_all(&object2_dir).expect("create second object directory");
        let second_meta = build_file_meta_with_object(0, 2048);
        let bytes = second_meta.marshal_msg().expect("serialize second object");
        fs::write(object2_dir.join("xl.meta"), bytes).expect("write second xl.meta");

        let DiskScanResult {
            snapshot: snapshot2,
            state: state_next,
            ..
        } = scan_disk_blocking(root.to_path_buf(), meta.clone(), state).expect("incremental scan succeeds");

        let usage2 = snapshot2
            .buckets_usage
            .get("bench")
            .expect("bucket stats recorded after addition");
        assert_eq!(usage2.objects_count, 2);
        assert_eq!(usage2.size, 1024 + 2048);
        assert_eq!(state_next.objects.len(), 2);

        fs::remove_dir_all(&object1_dir).expect("remove first object");

        let DiskScanResult {
            snapshot: snapshot3,
            state: state_final,
            ..
        } = scan_disk_blocking(root.to_path_buf(), meta, state_next).expect("scan after deletion succeeds");

        let usage3 = snapshot3
            .buckets_usage
            .get("bench")
            .expect("bucket stats recorded after deletion");
        assert_eq!(usage3.objects_count, 1);
        assert_eq!(usage3.size, 2048);
        assert_eq!(state_final.objects.len(), 1);
        assert!(
            state_final.objects.keys().all(|path| path.contains("nested")),
            "state should only keep surviving object"
        );
    }

    #[test]
    fn scan_disk_blocking_recovers_from_stale_state_entries() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let root = temp_dir.path();

        let mut stale_state = IncrementalScanState::default();
        stale_state.objects.insert(
            "bench/stale".to_string(),
            LocalObjectUsage {
                bucket: "bench".to_string(),
                object: "stale".to_string(),
                last_modified_ns: Some(42),
                versions_count: 1,
                delete_markers_count: 0,
                total_size: 512,
                has_live_object: true,
            },
        );
        stale_state.last_scan_ns = Some(99);

        let meta = LocalUsageSnapshotMeta {
            disk_id: "disk-test".to_string(),
            ..Default::default()
        };

        let DiskScanResult {
            snapshot, state, status, ..
        } = scan_disk_blocking(root.to_path_buf(), meta, stale_state).expect("scan succeeds");

        assert!(state.objects.is_empty(), "stale entries should be cleared when files disappear");
        assert!(
            snapshot.buckets_usage.is_empty(),
            "no real xl.meta files means bucket usage should stay empty"
        );
        assert!(status.snapshot_exists, "snapshot status should indicate a refresh");
    }

    #[test]
    fn scan_disk_blocking_handles_large_volume() {
        const OBJECTS: usize = 256;

        let temp_dir = TempDir::new().expect("create temp dir");
        let root = temp_dir.path();
        let bucket_dir = root.join("bulk");

        for idx in 0..OBJECTS {
            let object_dir = bucket_dir.join(format!("obj-{idx:03}"));
            fs::create_dir_all(&object_dir).expect("create object directory");
            let size = 1024 + idx as i64;
            let file_meta = build_file_meta_with_object(0, size);
            let bytes = file_meta.marshal_msg().expect("serialize file meta");
            fs::write(object_dir.join("xl.meta"), bytes).expect("write xl.meta");
        }

        let meta = LocalUsageSnapshotMeta {
            disk_id: "disk-test".to_string(),
            ..Default::default()
        };

        let DiskScanResult { snapshot, state, .. } =
            scan_disk_blocking(root.to_path_buf(), meta, IncrementalScanState::default()).expect("bulk scan succeeds");

        let bucket_usage = snapshot
            .buckets_usage
            .get("bulk")
            .expect("bucket usage present for bulk scan");
        assert_eq!(bucket_usage.objects_count as usize, OBJECTS, "should count all objects once");
        assert!(
            bucket_usage.size >= (1024 * OBJECTS) as u64,
            "aggregated size should grow with object count"
        );
        assert_eq!(state.objects.len(), OBJECTS, "incremental state tracks every object");
    }
}
