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

use std::collections::HashSet;
use std::fs::FileType;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::ReplTargetSizeSummary;
use crate::data_usage_define::{DataUsageCache, DataUsageEntry, DataUsageHash, DataUsageHashMap, SizeSummary, hash_path};
use crate::error::ScannerError;
use crate::metrics::{UpdateCurrentPathFn, current_path_updater};
use crate::scanner_io::ScannerIODisk as _;
use rustfs_common::heal_channel::{HEAL_DELETE_DANGLING, HealChannelRequest, HealOpts, HealScanMode, send_heal_request};
use rustfs_common::metrics::IlmAction;
use rustfs_ecstore::StorageAPI;
use rustfs_ecstore::bucket::lifecycle::bucket_lifecycle_audit::LcEventSrc;
use rustfs_ecstore::bucket::lifecycle::bucket_lifecycle_ops::apply_expiry_rule;
use rustfs_ecstore::bucket::lifecycle::evaluator::Evaluator;
use rustfs_ecstore::bucket::lifecycle::{
    bucket_lifecycle_ops::apply_transition_rule,
    lifecycle::{Event, Lifecycle, ObjectOpts},
};
use rustfs_ecstore::bucket::replication::{ReplicationConfig, ReplicationConfigurationExt as _, queue_replication_heal_internal};
use rustfs_ecstore::bucket::versioning::VersioningApi;
use rustfs_ecstore::bucket::versioning_sys::BucketVersioningSys;
use rustfs_ecstore::cache_value::metacache_set::{ListPathRawOptions, list_path_raw};
use rustfs_ecstore::disk::error::DiskError;
use rustfs_ecstore::disk::{Disk, DiskAPI as _, DiskInfoOptions};
use rustfs_ecstore::error::StorageError;
use rustfs_ecstore::global::is_erasure;
use rustfs_ecstore::pools::{path2_bucket_object, path2_bucket_object_with_base_path};
use rustfs_ecstore::store_api::{ObjectInfo, ObjectToDelete};
use rustfs_ecstore::store_utils::is_reserved_or_invalid_bucket;
use rustfs_filemeta::{MetaCacheEntries, MetaCacheEntry, MetadataResolutionParams, ReplicationStatusType};
use rustfs_utils::path::{SLASH_SEPARATOR, path_join_buf};
use s3s::dto::{BucketLifecycleConfiguration, ObjectLockConfiguration};
use tokio::select;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

// Constants from Go code
const DATA_SCANNER_SLEEP_PER_FOLDER: Duration = Duration::from_millis(1);
const DATA_USAGE_UPDATE_DIR_CYCLES: u32 = 16;
const DATA_SCANNER_COMPACT_LEAST_OBJECT: usize = 500;
const DATA_SCANNER_COMPACT_AT_CHILDREN: usize = 10000;
const DATA_SCANNER_COMPACT_AT_FOLDERS: usize = DATA_SCANNER_COMPACT_AT_CHILDREN / 4;
const DATA_SCANNER_FORCE_COMPACT_AT_FOLDERS: usize = 250_000;
const DEFAULT_HEAL_OBJECT_SELECT_PROB: u32 = 1024;
const ENV_DATA_USAGE_UPDATE_DIR_CYCLES: &str = "RUSTFS_DATA_USAGE_UPDATE_DIR_CYCLES";
const ENV_HEAL_OBJECT_SELECT_PROB: &str = "RUSTFS_HEAL_OBJECT_SELECT_PROB";

pub fn data_usage_update_dir_cycles() -> u32 {
    rustfs_utils::get_env_u32(ENV_DATA_USAGE_UPDATE_DIR_CYCLES, DATA_USAGE_UPDATE_DIR_CYCLES)
}

pub fn heal_object_select_prob() -> u32 {
    rustfs_utils::get_env_u32(ENV_HEAL_OBJECT_SELECT_PROB, DEFAULT_HEAL_OBJECT_SELECT_PROB)
}

/// Cached folder information for scanning
#[derive(Clone, Debug)]
pub struct CachedFolder {
    pub name: String,
    pub parent: Option<DataUsageHash>,
    pub object_heal_prob_div: u32,
}

/// Type alias for get size function
pub type GetSizeFn = Box<dyn Fn(ScannerItem) -> Result<SizeSummary, StorageError> + Send + Sync>;

/// Scanner item representing a file during scanning
#[derive(Clone, Debug)]
pub struct ScannerItem {
    pub path: String,
    pub bucket: String,
    pub prefix: String,
    pub object_name: String,
    pub file_type: FileType,
    pub lifecycle: Option<Arc<BucketLifecycleConfiguration>>,
    pub replication: Option<Arc<ReplicationConfig>>,
    pub heal_enabled: bool,
    pub heal_bitrot: bool,
    pub debug: bool,
}

impl ScannerItem {
    /// Get the object path (prefix + object_name)
    pub fn object_path(&self) -> String {
        if self.prefix.is_empty() {
            self.object_name.clone()
        } else {
            path_join_buf(&[&self.prefix, &self.object_name])
        }
    }

    /// Transform meta directory by splitting prefix and extracting object name
    /// This converts a directory path like "bucket/dir1/dir2/file" to prefix="bucket/dir1/dir2" and object_name="file"
    pub fn transform_meta_dir(&mut self) {
        let prefix = self.prefix.clone(); // Clone to avoid borrow checker issues
        let split: Vec<&str> = prefix.split(SLASH_SEPARATOR).collect();

        if split.len() > 1 {
            let prefix_parts: Vec<&str> = split[..split.len() - 1].to_vec();
            self.prefix = path_join_buf(&prefix_parts);
        } else {
            self.prefix = String::new();
        }

        // Object name is the last element
        self.object_name = split.last().unwrap_or(&"").to_string();
    }

    pub async fn apply_actions<S: StorageAPI>(
        &mut self,
        store: Arc<S>,
        object_infos: Vec<ObjectInfo>,
        lock_retention: Option<Arc<ObjectLockConfiguration>>,
        size_summary: &mut SizeSummary,
    ) {
        if object_infos.is_empty() {
            debug!("apply_actions: no object infos for object: {}", self.object_path());
            return;
        }
        debug!("apply_actions: applying actions for object: {}", self.object_path());

        let versioning_config = match BucketVersioningSys::get(&self.bucket).await {
            Ok(versioning_config) => versioning_config,
            Err(_) => {
                warn!("apply_actions: Failed to get versioning configuration for bucket {}", self.bucket);
                return;
            }
        };

        let Some(lifecycle) = self.lifecycle.as_ref() else {
            let mut cumulative_size = 0;
            for oi in object_infos.iter() {
                let actual_size = match oi.get_actual_size() {
                    Ok(size) => size,
                    Err(_) => {
                        warn!("apply_actions: Failed to get actual size for object {}", oi.name);
                        continue;
                    }
                };

                let size = self.heal_actions(store.clone(), oi, actual_size, size_summary).await;

                size_summary.actions_accounting(oi, size, actual_size);

                cumulative_size += size;
            }

            self.alert_excessive_versions(object_infos.len(), cumulative_size);

            debug!("apply_actions: done for now no lifecycle config");
            return;
        };

        let object_opts = object_infos
            .iter()
            .map(ObjectOpts::from_object_info)
            .collect::<Vec<ObjectOpts>>();

        let events = match Evaluator::new(lifecycle.clone())
            .with_lock_retention(lock_retention)
            .with_replication_config(self.replication.clone())
            .eval(&object_opts)
            .await
        {
            Ok(events) => events,
            Err(e) => {
                warn!("apply_actions: Failed to evaluate lifecycle for object: {}", e);
                return;
            }
        };
        let mut to_delete_objs: Vec<ObjectToDelete> = Vec::new();
        let mut noncurrent_events: Vec<Event> = Vec::new();
        let mut cumulative_size = 0;
        let mut remaining_versions = object_infos.len();
        'eventLoop: {
            for (i, event) in events.iter().enumerate() {
                let oi = &object_infos[i];
                let actual_size = match oi.get_actual_size() {
                    Ok(size) => size,
                    Err(_) => {
                        warn!("apply_actions: Failed to get actual size for object {}", oi.name);
                        0
                    }
                };

                let mut size = actual_size;

                match event.action {
                    IlmAction::DeleteAllVersionsAction | IlmAction::DelMarkerDeleteAllVersionsAction => {
                        remaining_versions = 0;
                        debug!("apply_actions: applying expiry rule for object: {} {}", oi.name, event.action);
                        apply_expiry_rule(event, &LcEventSrc::Scanner, oi).await;
                        break 'eventLoop;
                    }

                    IlmAction::DeleteAction | IlmAction::DeleteRestoredAction | IlmAction::DeleteRestoredVersionAction => {
                        if !versioning_config.prefix_enabled(&self.object_path()) && event.action == IlmAction::DeleteAction {
                            remaining_versions -= 1;
                            size = 0;
                        }

                        debug!("apply_actions: applying expiry rule for object: {} {}", oi.name, event.action);
                        apply_expiry_rule(event, &LcEventSrc::Scanner, oi).await;
                    }
                    IlmAction::DeleteVersionAction => {
                        remaining_versions -= 1;
                        size = 0;
                        if let Some(opt) = object_opts.get(i) {
                            to_delete_objs.push(ObjectToDelete {
                                object_name: opt.name.clone(),
                                version_id: opt.version_id,
                                ..Default::default()
                            });
                        }
                        noncurrent_events.push(event.clone());
                    }
                    IlmAction::TransitionAction | IlmAction::TransitionVersionAction => {
                        debug!("apply_actions: applying transition rule for object: {} {}", oi.name, event.action);
                        apply_transition_rule(event, &LcEventSrc::Scanner, oi).await;
                    }

                    IlmAction::NoneAction | IlmAction::ActionCount => {
                        size = self.heal_actions(store.clone(), oi, actual_size, size_summary).await;
                    }
                }

                size_summary.actions_accounting(oi, size, actual_size);

                cumulative_size += size;
            }
        }

        if !to_delete_objs.is_empty() {
            // TODO: enqueueNoncurrentVersions
        }
        self.alert_excessive_versions(remaining_versions, cumulative_size);
    }

    async fn heal_actions<S: StorageAPI>(
        &mut self,
        store: Arc<S>,
        oi: &ObjectInfo,
        actual_size: i64,
        size_summary: &mut SizeSummary,
    ) -> i64 {
        let mut size = actual_size;

        if self.heal_enabled {
            size = self.apply_heal(store, oi).await;
        }

        self.heal_replication(oi, size_summary).await;

        size
    }

    async fn heal_replication(&mut self, oi: &ObjectInfo, size_summary: &mut SizeSummary) {
        if oi.version_id.is_none_or(|v| v.is_nil()) {
            return;
        }

        let Some(replication) = self.replication.clone() else {
            return;
        };

        let roi = queue_replication_heal_internal(&oi.bucket, oi.clone(), (*replication).clone(), 0).await;
        if oi.delete_marker || oi.version_purge_status.is_empty() {
            return;
        }

        for (arn, target_status) in roi.target_statuses.iter() {
            if !size_summary.repl_target_stats.contains_key(arn.as_str()) {
                size_summary
                    .repl_target_stats
                    .insert(arn.clone(), ReplTargetSizeSummary::default());
            }

            if let Some(repl_target_size_summary) = size_summary.repl_target_stats.get_mut(arn.as_str()) {
                match target_status {
                    ReplicationStatusType::Pending => {
                        repl_target_size_summary.pending_size += roi.size;
                        repl_target_size_summary.pending_count += 1;
                        size_summary.pending_size += roi.size;
                        size_summary.pending_count += 1;
                    }
                    ReplicationStatusType::Failed => {
                        repl_target_size_summary.failed_size += roi.size;
                        repl_target_size_summary.failed_count += 1;
                        size_summary.failed_size += roi.size;
                        size_summary.failed_count += 1;
                    }
                    ReplicationStatusType::Completed | ReplicationStatusType::CompletedLegacy => {
                        repl_target_size_summary.replicated_size += roi.size;
                        repl_target_size_summary.replicated_count += 1;
                        size_summary.replicated_size += roi.size;
                        size_summary.replicated_count += 1;
                    }
                    _ => {}
                }
            }
        }

        if oi.replication_status == ReplicationStatusType::Replica {
            size_summary.replica_size += roi.size;
            size_summary.replica_count += 1;
        }
    }

    async fn apply_heal<S: StorageAPI>(&mut self, store: Arc<S>, oi: &ObjectInfo) -> i64 {
        debug!(
            "apply_heal: bucket: {}, object_path: {}, version_id: {}",
            self.bucket,
            self.object_path(),
            oi.version_id.unwrap_or_default()
        );

        let scan_mode = if self.heal_bitrot {
            HealScanMode::Deep
        } else {
            HealScanMode::Normal
        };

        match store
            .clone()
            .heal_object(
                self.bucket.as_str(),
                self.object_path().as_str(),
                oi.version_id
                    .map(|v| if v.is_nil() { "".to_string() } else { v.to_string() })
                    .unwrap_or_default()
                    .as_str(),
                &HealOpts {
                    remove: HEAL_DELETE_DANGLING,
                    scan_mode,
                    ..Default::default()
                },
            )
            .await
        {
            Ok((result, err)) => {
                if let Some(err) = err {
                    warn!("apply_heal: failed to heal object: {}", err);
                }
                result.object_size as i64
            }
            Err(e) => {
                warn!("apply_heal: failed to heal object: {}", e);
                0
            }
        }
    }

    fn alert_excessive_versions(&self, _object_infos_length: usize, _cumulative_size: i64) {
        // TODO: Implement alerting for excessive versions
    }
}

/// Folder scanner for scanning directory structures
pub struct FolderScanner {
    root: String,
    old_cache: DataUsageCache,
    new_cache: DataUsageCache,
    update_cache: DataUsageCache,

    data_usage_scanner_debug: bool,
    heal_object_select: u32,
    scan_mode: HealScanMode,

    we_sleep: Box<dyn Fn() -> bool + Send + Sync>,
    // should_heal: Arc<dyn Fn() -> bool + Send + Sync>,
    disks: Vec<Arc<Disk>>,
    disks_quorum: usize,

    updates: Option<mpsc::Sender<DataUsageEntry>>,
    last_update: SystemTime,

    update_current_path: UpdateCurrentPathFn,

    skip_heal: Arc<std::sync::atomic::AtomicBool>,
    local_disk: Arc<Disk>,
}

impl FolderScanner {
    pub async fn should_heal(&self) -> bool {
        if self.skip_heal.load(std::sync::atomic::Ordering::Relaxed) {
            return false;
        }
        if self.heal_object_select == 0 {
            return false;
        }

        if self
            .local_disk
            .disk_info(&DiskInfoOptions::default())
            .await
            .unwrap_or_default()
            .healing
        {
            self.skip_heal.store(true, std::sync::atomic::Ordering::Relaxed);
            return false;
        }

        true
    }

    /// Set heal object select probability
    pub fn set_heal_object_select(&mut self, prob: u32) {
        self.heal_object_select = prob;
    }

    /// Set debug mode
    pub fn set_debug(&mut self, debug: bool) {
        self.data_usage_scanner_debug = debug;
    }

    /// Send update if enough time has passed
    /// Should be called on a regular basis when the new_cache contains more recent total than previously.
    /// May or may not send an update upstream.
    pub async fn send_update(&mut self) {
        // Send at most an update every minute.
        if self.updates.is_none() {
            return;
        }

        let elapsed = self.last_update.elapsed().unwrap_or(Duration::from_secs(0));
        if elapsed < Duration::from_secs(60) {
            return;
        }

        if let Some(flat) = self.update_cache.size_recursive(&self.new_cache.info.name)
            && let Some(ref updates) = self.updates
        {
            // Try to send without blocking
            if let Err(e) = updates.send(flat.clone()).await {
                error!("send_update: failed to send update: {}", e);
            }
            self.last_update = SystemTime::now();
        }
    }

    /// Scan a folder recursively
    /// Files found in the folders will be added to new_cache.
    #[allow(clippy::never_loop)]
    #[allow(unused_assignments)]
    pub async fn scan_folder(
        &mut self,
        ctx: CancellationToken,
        folder: CachedFolder,
        into: &mut DataUsageEntry,
    ) -> Result<(), ScannerError> {
        if ctx.is_cancelled() {
            return Err(ScannerError::Other("Operation cancelled".to_string()));
        }

        let this_hash = hash_path(&folder.name);
        // Store initial compaction state.
        let was_compacted = into.compacted;

        let wait_time = None;

        loop {
            if ctx.is_cancelled() {
                return Err(ScannerError::Other("Operation cancelled".to_string()));
            }

            let mut abandoned_children: DataUsageHashMap = HashSet::new();
            if !into.compacted {
                abandoned_children = self.old_cache.find_children_copy(this_hash.clone());
            }

            debug!("scan_folder : {}/{}", &self.root, &folder.name);
            let (_, prefix) = path2_bucket_object_with_base_path(&self.root, &folder.name);

            let active_life_cycle = if self
                .old_cache
                .info
                .lifecycle
                .as_ref()
                .is_some_and(|v| v.has_active_rules(&prefix))
            {
                self.old_cache.info.lifecycle.clone()
            } else {
                None
            };

            let active_replication =
                if self.old_cache.info.replication.as_ref().is_some_and(|v| {
                    !v.is_empty() && v.config.as_ref().is_some_and(|config| config.has_active_rules(&prefix, true))
                }) {
                    self.old_cache.info.replication.clone()
                } else {
                    None
                };

            if (self.we_sleep)() {
                tokio::time::sleep(DATA_SCANNER_SLEEP_PER_FOLDER).await;
            }

            let mut existing_folders: Vec<CachedFolder> = Vec::new();
            let mut new_folders: Vec<CachedFolder> = Vec::new();
            let mut found_objects = false;

            let dir_path = path_join_buf(&[&self.root, &folder.name]);

            debug!("scan_folder: dir_path: {:?}", dir_path);

            let mut dir_reader = tokio::fs::read_dir(&dir_path)
                .await
                .map_err(|e| ScannerError::Other(e.to_string()))?;

            while let Some(entry) = dir_reader
                .next_entry()
                .await
                .map_err(|e| ScannerError::Other(e.to_string()))?
            {
                let file_name = entry.file_name().to_string_lossy().to_string();
                if file_name.is_empty() || file_name == "." || file_name == ".." {
                    continue;
                }

                let file_path = entry.path().to_string_lossy().to_string();

                let trim_dir_name = file_path.strip_prefix(&dir_path).unwrap_or(&file_path);

                let entry_name = path_join_buf(&[&folder.name, trim_dir_name]);

                if entry_name.is_empty() || entry_name == folder.name {
                    continue;
                }

                let entry_type = entry.file_type().await.map_err(|e| ScannerError::Other(e.to_string()))?;

                // ok

                let (bucket, prefix) = path2_bucket_object_with_base_path(self.root.as_str(), &entry_name);
                if bucket.is_empty() {
                    break;
                }

                if is_reserved_or_invalid_bucket(&bucket, false) {
                    break;
                }

                if ctx.is_cancelled() {
                    break;
                }

                if entry_type.is_dir() {
                    let h = hash_path(&entry_name);

                    if h == this_hash {
                        continue;
                    }

                    let exists = self.old_cache.cache.contains_key(&h.key());

                    let this = CachedFolder {
                        name: entry_name.clone(),
                        parent: Some(this_hash.clone()),
                        object_heal_prob_div: folder.object_heal_prob_div,
                    };

                    abandoned_children.remove(&h.key());

                    if exists {
                        existing_folders.push(this);
                        self.update_cache
                            .copy_with_children(&self.old_cache, &h, &Some(this_hash.clone()));
                    } else {
                        new_folders.push(this);
                    }
                    continue;
                }

                let mut wait = wait_time;

                if (self.we_sleep)() {
                    wait = Some(SystemTime::now());
                }

                let heal_enabled = this_hash.mod_alt(
                    self.old_cache.info.next_cycle as u32 / folder.object_heal_prob_div,
                    self.heal_object_select / folder.object_heal_prob_div,
                ) && self.should_heal().await;

                let mut item = ScannerItem {
                    path: file_path,
                    bucket,
                    prefix: rustfs_utils::path::dir(&prefix),
                    object_name: file_name,
                    lifecycle: active_life_cycle.clone(),
                    replication: active_replication.clone(),
                    heal_enabled,
                    heal_bitrot: self.scan_mode == HealScanMode::Deep,
                    debug: self.data_usage_scanner_debug,
                    file_type: entry_type,
                };

                let sz = match self.local_disk.get_size(item.clone()).await {
                    Ok(sz) => sz,
                    Err(e) => {
                        warn!("scan_folder: failed to get size for item {}: {}", item.path, e);
                        // TODO: check error type
                        if let Some(t) = wait
                            && let Ok(elapsed) = t.elapsed()
                        {
                            tokio::time::sleep(elapsed).await;
                        }

                        if e != StorageError::other("skip file".to_string()) {
                            warn!("scan_folder: failed to get size for item {}: {}", item.path, e);
                        }
                        continue;
                    }
                };

                found_objects = true;

                item.transform_meta_dir();

                abandoned_children.remove(&path_join_buf(&[&item.bucket, &item.object_path()]));

                // TODO: check err
                into.add_sizes(&sz);
                into.objects += 1;

                if let Some(t) = wait
                    && let Ok(elapsed) = t.elapsed()
                {
                    tokio::time::sleep(elapsed).await;
                }
            }

            if found_objects && is_erasure().await {
                // If we found an object in erasure mode, we skip subdirs (only datadirs)...
                info!("scan_folder: done for now found an object in erasure mode");
                break;
            }

            // If we have many subfolders, compact ourself.
            let should_compact = (self.new_cache.info.name != folder.name
                && existing_folders.len() + new_folders.len() >= DATA_SCANNER_COMPACT_AT_FOLDERS)
                || existing_folders.len() + new_folders.len() >= DATA_SCANNER_FORCE_COMPACT_AT_FOLDERS;

            // TODO: Check for excess folders and send events

            if !into.compacted && should_compact {
                into.compacted = true;
                new_folders.append(&mut existing_folders);

                existing_folders.clear();

                if self.data_usage_scanner_debug {
                    debug!("scan_folder: Preemptively compacting: {}, entries: {}", folder.name, new_folders.len());
                }
            }

            if !into.compacted {
                for folder_item in &existing_folders {
                    let h = hash_path(&folder_item.name);
                    self.update_cache.copy_with_children(&self.old_cache, &h, &folder_item.parent);
                }
            }

            // Scan new folders
            for folder_item in new_folders {
                if ctx.is_cancelled() {
                    return Err(ScannerError::Other("Operation cancelled".to_string()));
                }

                let h = hash_path(&folder_item.name);
                // Add new folders to the update tree so totals update for these.
                if !into.compacted {
                    let mut found_any = false;
                    let mut parent = this_hash.clone();
                    let update_cache_name_hash = hash_path(&self.update_cache.info.name);

                    while parent != update_cache_name_hash {
                        let parent_key = parent.key();
                        let e = self.update_cache.find(&parent_key);
                        if e.is_none_or(|v| v.compacted) {
                            found_any = true;
                            break;
                        }
                        if let Some(next) = self.update_cache.search_parent(&parent) {
                            parent = next;
                        } else {
                            found_any = true;
                            break;
                        }
                    }
                    if !found_any {
                        // Add non-compacted empty entry.
                        self.update_cache
                            .replace_hashed(&h, &Some(this_hash.clone()), &DataUsageEntry::default());
                    }
                }

                (self.update_current_path)(&folder_item.name).await;

                let mut dst = if !into.compacted {
                    DataUsageEntry::default()
                } else {
                    into.clone()
                };

                // Use Box::pin for recursive async call
                let fut = Box::pin(self.scan_folder(ctx.clone(), folder_item.clone(), &mut dst));
                fut.await.map_err(|e| ScannerError::Other(e.to_string()))?;

                if !into.compacted {
                    let h = DataUsageHash(folder_item.name.clone());
                    into.add_child(&h);
                    // We scanned a folder, optionally send update.
                    self.update_cache.delete_recursive(&h);
                    self.update_cache.copy_with_children(&self.new_cache, &h, &folder_item.parent);
                    self.send_update().await;
                }

                if !into.compacted && self.update_cache.find(&this_hash.key()).is_some_and(|v| !v.compacted) {
                    self.update_cache.delete_recursive(&h);
                    self.update_cache
                        .copy_with_children(&self.new_cache, &h, &Some(this_hash.clone()));
                }
            }

            // Scan existing folders
            for mut folder_item in existing_folders {
                if ctx.is_cancelled() {
                    return Err(ScannerError::Other("Operation cancelled".to_string()));
                }

                let h = hash_path(&folder_item.name);

                if !into.compacted && self.old_cache.is_compacted(&h) {
                    let next_cycle = self.old_cache.info.next_cycle as u32;
                    if !h.mod_(next_cycle, data_usage_update_dir_cycles()) {
                        // Transfer and add as child...
                        self.new_cache.copy_with_children(&self.old_cache, &h, &folder_item.parent);
                        into.add_child(&h);
                        continue;
                    }

                    folder_item.object_heal_prob_div = data_usage_update_dir_cycles();
                }

                (self.update_current_path)(&folder_item.name).await;

                let mut dst = if !into.compacted {
                    DataUsageEntry::default()
                } else {
                    into.clone()
                };

                // Use Box::pin for recursive async call
                let fut = Box::pin(self.scan_folder(ctx.clone(), folder_item.clone(), &mut dst));
                fut.await.map_err(|e| ScannerError::Other(e.to_string()))?;

                if !into.compacted {
                    let h = DataUsageHash(folder_item.name.clone());
                    into.add_child(&h);
                    // We scanned a folder, optionally send update.
                    self.update_cache.delete_recursive(&h);
                    self.update_cache.copy_with_children(&self.new_cache, &h, &folder_item.parent);
                    self.send_update().await;
                }
            }

            // Scan for healing
            if abandoned_children.is_empty() || !self.should_heal().await {
                debug!("scan_folder: done for now abandoned children are empty or we are not healing");
                // If we are not heal scanning, return now.
                break;
            }

            if self.disks.is_empty() || self.disks_quorum == 0 {
                debug!("scan_folder: done for now disks are empty or quorum is 0");
                break;
            }

            let mut resolver = MetadataResolutionParams {
                dir_quorum: self.disks_quorum,
                obj_quorum: self.disks_quorum,
                bucket: "".to_string(),
                strict: false,
                ..Default::default()
            };

            for name in abandoned_children {
                if !self.should_heal().await {
                    break;
                }

                let (bucket, prefix) = path2_bucket_object(name.as_str());

                if bucket != resolver.bucket {
                    send_heal_request(HealChannelRequest {
                        bucket: bucket.clone(),
                        ..Default::default()
                    })
                    .await
                    .map_err(|e| ScannerError::Other(e.to_string()))?;
                }

                resolver.bucket = bucket.clone();

                let child_ctx = ctx.child_token();

                let (agreed_tx, mut agreed_rx) = mpsc::channel::<String>(1);
                let (partial_tx, mut partial_rx) = mpsc::channel::<MetaCacheEntries>(1);
                let (finished_tx, mut finished_rx) = mpsc::channel::<Vec<Option<DiskError>>>(1);

                let disks = self.disks.iter().cloned().map(Some).collect();
                let disks_quorum = self.disks_quorum;
                let bucket_clone = bucket.clone();
                let prefix_clone = prefix.clone();
                let child_ctx_clone = child_ctx.clone();
                let agreed_tx = agreed_tx.clone();
                let partial_tx = partial_tx.clone();
                let finished_tx = finished_tx.clone();

                tokio::spawn(async move {
                    if let Err(e) = list_path_raw(
                        child_ctx_clone.clone(),
                        ListPathRawOptions {
                            disks,
                            bucket: bucket_clone.clone(),
                            path: prefix_clone.clone(),
                            recursive: true,
                            report_not_found: true,
                            min_disks: disks_quorum,
                            agreed: Some(Box::new(move |entry: MetaCacheEntry| {
                                let entry_name = entry.name.clone();
                                let agreed_tx = agreed_tx.clone();
                                Box::pin(async move {
                                    if let Err(e) = agreed_tx.send(entry_name).await {
                                        error!("scan_folder: list_path_raw: failed to send entry name: {}: {}", entry.name, e);
                                    }
                                })
                            })),
                            partial: Some(Box::new(move |entries: MetaCacheEntries, _: &[Option<DiskError>]| {
                                let partial_tx = partial_tx.clone();
                                Box::pin(async move {
                                    if let Err(e) = partial_tx.send(entries).await {
                                        error!("scan_folder: list_path_raw: failed to send partial err: {}", e);
                                    }
                                })
                            })),
                            finished: Some(Box::new(move |errs: &[Option<DiskError>]| {
                                let finished_tx = finished_tx.clone();
                                let errs_clone = errs.to_vec();
                                Box::pin(async move {
                                    if let Err(e) = finished_tx.send(errs_clone).await {
                                        error!("scan_folder: list_path_raw: failed to send finished errs: {}", e);
                                    }
                                })
                            })),
                            ..Default::default()
                        },
                    )
                    .await
                    {
                        error!("scan_folder: failed to list path: {}/{}: {}", bucket_clone, prefix_clone, e);
                    }
                });

                let mut found_objects = false;

                loop {
                    select! {
                        Some(entry_name) = agreed_rx.recv() => {
                            (self.update_current_path)(&entry_name).await;
                        }
                        Some(entries) = partial_rx.recv() => {
                            if !self.should_heal().await {
                                child_ctx.cancel();
                                break;
                            }

                         let entry_option =  match entries.resolve(resolver.clone()){
                            Some(entry) => {
                                Some(entry)
                            }
                            None => {
                               let (entry,_) = entries.first_found();
                               entry
                            }
                           };


                           let Some(entry) = entry_option else {
                            break;
                           };

                           (self.update_current_path)(&entry.name).await;

                           if entry.is_dir() {
                            continue;
                           }




                           let fivs = match entry.file_info_versions(&bucket) {
                            Ok(fivs) => fivs,
                            Err(e) => {
                                error!("scan_folder: list_path_raw: failed to get file info versions: {}", e);
                                if let Err(e) = send_heal_request(HealChannelRequest {
                                    bucket: bucket.clone(),
                                    object_prefix: Some(entry.name.clone()),
                                    ..Default::default()
                                }).await {
                                    error!("scan_folder: list_path_raw: failed to send heal request: {}", e);
                                    continue;
                                }


                                found_objects = true;

                                continue;
                            }
                           };

                           for fiv in fivs.versions {

                            if let Err(e) = send_heal_request(HealChannelRequest {
                                bucket: bucket.clone(),
                                object_prefix: Some(entry.name.clone()),
                                object_version_id: fiv.version_id.map(|v| v.to_string()),
                                ..Default::default()
                            }).await {
                                error!("scan_folder: list_path_raw: failed to send heal request: {}", e);
                                continue;
                            }

                            found_objects = true;

                           }


                        }
                        Some(errs) = finished_rx.recv() => {
                            error!("scan_folder: list_path_raw: failed to get finished errs: {:?}", errs);
                            child_ctx.cancel();
                        }
                        _ = child_ctx.cancelled() => {
                            break;
                        }
                    }
                }

                if found_objects {
                    let folder_item = CachedFolder {
                        name: name.clone(),
                        parent: Some(this_hash.clone()),
                        object_heal_prob_div: 1,
                    };

                    let mut dst = if !into.compacted {
                        DataUsageEntry::default()
                    } else {
                        into.clone()
                    };

                    // Use Box::pin for recursive async call
                    let fut = Box::pin(self.scan_folder(ctx.clone(), folder_item.clone(), &mut dst));
                    fut.await.map_err(|e| ScannerError::Other(e.to_string()))?;

                    if !into.compacted {
                        let h = DataUsageHash(folder_item.name.clone());
                        into.add_child(&h);
                        // We scanned a folder, optionally send update.
                        self.update_cache.delete_recursive(&h);
                        self.update_cache.copy_with_children(&self.new_cache, &h, &folder_item.parent);
                        self.send_update().await;
                    }
                }
            }

            break;
        }

        if !was_compacted {
            self.new_cache.replace_hashed(&this_hash, &folder.parent, into);
        }

        if !into.compacted
            && self.new_cache.info.name != folder.name
            && let Some(mut flat) = self.new_cache.size_recursive(&this_hash.key())
        {
            flat.compacted = true;
            let mut should_compact = false;

            if flat.objects < DATA_SCANNER_COMPACT_LEAST_OBJECT {
                should_compact = true;
            } else {
                // Compact if we only have objects as children...
                should_compact = true;
                for k in &into.children {
                    if let Some(v) = self.new_cache.cache.get(k)
                        && (!v.children.is_empty() || v.objects > 1)
                    {
                        should_compact = false;
                        break;
                    }
                }
            }

            if should_compact {
                self.new_cache.delete_recursive(&this_hash);
                self.new_cache.replace_hashed(&this_hash, &folder.parent, &flat);
            }
        }

        // Compact if too many children...
        if !into.compacted {
            self.new_cache.reduce_children_of(
                &this_hash,
                DATA_SCANNER_COMPACT_AT_CHILDREN,
                self.new_cache.info.name != folder.name,
            );
        }

        if self.update_cache.cache.contains_key(&this_hash.key()) && !was_compacted {
            // Replace if existed before.
            if let Some(flat) = self.new_cache.size_recursive(&this_hash.key()) {
                self.update_cache.delete_recursive(&this_hash);
                self.update_cache.replace_hashed(&this_hash, &folder.parent, &flat);
            }
        }

        Ok(())
    }

    pub fn as_mut_new_cache(&mut self) -> &mut DataUsageCache {
        &mut self.new_cache
    }
}

/// Scan a data folder
/// This function scans the basepath+cache.info.name and returns an updated cache.
/// The returned cache will always be valid, but may not be updated from the existing.
/// Before each operation sleepDuration is called which can be used to temporarily halt the scanner.
/// If the supplied context is canceled the function will return at the first chance.
#[allow(clippy::too_many_arguments)]
pub async fn scan_data_folder(
    ctx: CancellationToken,
    disks: Vec<Arc<Disk>>,
    local_disk: Arc<Disk>,
    cache: DataUsageCache,
    updates: Option<mpsc::Sender<DataUsageEntry>>,
    scan_mode: HealScanMode,
    we_sleep: Box<dyn Fn() -> bool + Send + Sync>,
) -> Result<DataUsageCache, ScannerError> {
    use crate::data_usage_define::DATA_USAGE_ROOT;

    // Check that we're not trying to scan the root
    if cache.info.name.is_empty() || cache.info.name == DATA_USAGE_ROOT {
        return Err(ScannerError::Other("internal error: root scan attempted".to_string()));
    }

    // Get disk path
    let base_path = local_disk.path().to_string_lossy().to_string();

    let (update_current_path, close_disk) = current_path_updater(&base_path, &cache.info.name);

    // Create skip_heal flag
    let is_erasure_mode = is_erasure().await;
    let skip_heal = Arc::new(std::sync::atomic::AtomicBool::new(!is_erasure_mode || cache.info.skip_healing));

    // Create heal_object_select flag
    let heal_object_select = if is_erasure_mode && !cache.info.skip_healing {
        heal_object_select_prob()
    } else {
        0
    };

    let disks_quorum = disks.len() / 2;

    // Create folder scanner
    let mut scanner = FolderScanner {
        root: base_path,
        old_cache: cache.clone(),
        new_cache: DataUsageCache {
            info: cache.info.clone(),
            ..Default::default()
        },
        update_cache: DataUsageCache {
            info: cache.info.clone(),
            ..Default::default()
        },
        data_usage_scanner_debug: false,
        heal_object_select,
        scan_mode,
        we_sleep,
        disks,
        disks_quorum,
        updates,
        last_update: SystemTime::UNIX_EPOCH,
        update_current_path,
        skip_heal,
        local_disk,
    };

    // Check if context is cancelled
    if ctx.is_cancelled() {
        return Err(ScannerError::Other("Operation cancelled".to_string()));
    }

    // Read top level in bucket
    let mut root = DataUsageEntry::default();
    let folder = CachedFolder {
        name: cache.info.name.clone(),
        parent: None,
        object_heal_prob_div: 1,
    };

    // Scan the folder
    match scanner.scan_folder(ctx, folder, &mut root).await {
        Ok(()) => {
            // Get the new cache and finalize it
            let new_cache = scanner.as_mut_new_cache();
            new_cache.force_compact(DATA_SCANNER_COMPACT_AT_CHILDREN);
            new_cache.info.last_update = Some(SystemTime::now());
            new_cache.info.next_cycle = cache.info.next_cycle;

            close_disk().await;
            Ok(new_cache.clone())
        }
        Err(e) => {
            close_disk().await;
            // No useful information, return original cache
            Err(e)
        }
    }
}
