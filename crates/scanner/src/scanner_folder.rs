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
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use rustfs_common::heal_channel::HealScanMode;
use rustfs_ecstore::StorageAPI;
use rustfs_ecstore::disk::Disk;
use rustfs_ecstore::disk::local::LocalDisk;
use rustfs_ecstore::error::StorageError;
use rustfs_utils::path::path_join_buf;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::data_usage_define::{DataUsageCache, DataUsageEntry, DataUsageHash, DataUsageHashMap, SizeSummary, hash_path};
use crate::error::ScannerError;

// Constants from Go code
const DATA_SCANNER_SLEEP_PER_FOLDER: Duration = Duration::from_millis(1);
const DATA_USAGE_UPDATE_DIR_CYCLES: u32 = 16;
const DATA_SCANNER_COMPACT_LEAST_OBJECT: usize = 500;
const DATA_SCANNER_COMPACT_AT_CHILDREN: usize = 10000;
const DATA_SCANNER_COMPACT_AT_FOLDERS: usize = DATA_SCANNER_COMPACT_AT_CHILDREN / 4;
const DATA_SCANNER_FORCE_COMPACT_AT_FOLDERS: usize = 250_000;
const HEAL_OBJECT_SELECT_PROB: u32 = 1024;

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
}

/// Folder scanner for scanning directory structures
pub struct FolderScanner {
    root: String,
    get_size: GetSizeFn,
    old_cache: DataUsageCache,
    new_cache: DataUsageCache,
    update_cache: DataUsageCache,

    data_usage_scanner_debug: bool,
    heal_object_select: u32,
    scan_mode: HealScanMode,

    we_sleep: Box<dyn Fn() -> bool + Send + Sync>,
    should_heal: Arc<dyn Fn() -> bool + Send + Sync>,

    disks: Vec<Arc<Disk>>,
    disks_quorum: usize,

    updates: Option<mpsc::Sender<DataUsageEntry>>,
    last_update: SystemTime,

    update_current_path: Box<dyn Fn(String) + Send + Sync>,
}

impl FolderScanner {
    /// Create a new folder scanner
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        root: String,
        get_size: GetSizeFn,
        old_cache: DataUsageCache,
        scan_mode: HealScanMode,
        we_sleep: Box<dyn Fn() -> bool + Send + Sync>,
        should_heal: Arc<dyn Fn() -> bool + Send + Sync>,
        disks: Vec<Arc<Disk>>,
        updates: Option<mpsc::Sender<DataUsageEntry>>,
        update_current_path: Box<dyn Fn(String) + Send + Sync>,
    ) -> Self {
        let new_cache = DataUsageCache {
            info: old_cache.info.clone(),
            ..Default::default()
        };

        let update_cache = DataUsageCache {
            info: old_cache.info.clone(),
            ..Default::default()
        };

        let disks_quorum = disks.len() / 2;

        Self {
            root,
            get_size,
            old_cache,
            new_cache,
            update_cache,
            data_usage_scanner_debug: false,
            heal_object_select: 0,
            scan_mode,
            we_sleep,
            should_heal,
            disks,
            disks_quorum,
            updates,
            last_update: SystemTime::now(),
            update_current_path,
        }
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
    pub fn send_update(&mut self) {
        // Send at most an update every minute.
        if self.updates.is_none() {
            return;
        }

        let elapsed = self.last_update.elapsed().unwrap_or(Duration::from_secs(0));
        if elapsed < Duration::from_secs(60) {
            return;
        }

        if let Some(flat) = self.update_cache.size_recursive(&self.new_cache.info.name) {
            if let Some(ref updates) = self.updates {
                // Try to send without blocking
                let _ = updates.try_send(flat.clone());
                self.last_update = SystemTime::now();
            }
        }
    }

    /// Scan a folder recursively
    /// Files found in the folders will be added to new_cache.
    #[allow(clippy::never_loop)]
    pub async fn scan_folder(
        &mut self,
        ctx: CancellationToken,
        folder: CachedFolder,
        into: &mut DataUsageEntry,
    ) -> Result<(), ScannerError> {
        if ctx.is_cancelled() {
            return Err(ScannerError::Other("Operation cancelled".to_string()));
        }

        let scanner_log_prefix = "folder-scanner:";

        let this_hash = hash_path(&folder.name);
        // Store initial compaction state.
        let was_compacted = into.compacted;

        loop {
            if ctx.is_cancelled() {
                return Err(ScannerError::Other("Operation cancelled".to_string()));
            }

            let mut abandoned_children: DataUsageHashMap = HashSet::new();
            if !into.compacted {
                abandoned_children = self.old_cache.find_children_copy(this_hash.clone());
            }

            // TODO: Check for lifecycle rules for the prefix
            // TODO: Check for replication rules for the prefix

            if (self.we_sleep)() {
                tokio::time::sleep(DATA_SCANNER_SLEEP_PER_FOLDER).await;
            }

            let mut existing_folders: Vec<CachedFolder> = Vec::new();
            let mut new_folders: Vec<CachedFolder> = Vec::new();
            let mut found_objects = false;

            // TODO: Implement readDirFn equivalent
            // For now, this is a placeholder that needs to be implemented based on the actual
            // directory reading mechanism in Rust
            // err := readDirFn(pathJoin(f.root, folder.name), func(entName string, typ os.FileMode) error {
            //     ...
            // })

            // Placeholder: This needs to be replaced with actual directory reading logic
            // The Go code reads directory entries and processes them
            // In Rust, this would typically use tokio::fs::read_dir or similar

            if found_objects {
                // If we found an object in erasure mode, we skip subdirs (only datadirs)...
                // TODO: Check if globalIsErasure
                break;
            }

            // If we have many subfolders, compact ourself.
            let should_compact = (self.new_cache.info.name != folder.name
                && existing_folders.len() + new_folders.len() >= DATA_SCANNER_COMPACT_AT_FOLDERS)
                || existing_folders.len() + new_folders.len() >= DATA_SCANNER_FORCE_COMPACT_AT_FOLDERS;

            // TODO: Check for excess folders and send events

            if !into.compacted && should_compact {
                into.compacted = true;
                new_folders.extend(existing_folders.drain(..));
                if self.data_usage_scanner_debug {
                    debug!(
                        "{} Preemptively compacting: {}, entries: {}",
                        scanner_log_prefix,
                        folder.name,
                        new_folders.len()
                    );
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
                        if e.is_none() || e.as_ref().unwrap().compacted {
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

                (self.update_current_path)(folder_item.name.clone());

                let mut dst = if !into.compacted {
                    DataUsageEntry::default()
                } else {
                    into.clone()
                };

                // Use Box::pin for recursive async call
                let fut = Box::pin(self.scan_folder(ctx.clone(), folder_item.clone(), &mut dst));
                fut.await?;

                if !into.compacted {
                    into.add_child(&h);
                    // We scanned a folder, optionally send update.
                    self.update_cache.delete_recursive(&h);
                    self.update_cache.copy_with_children(&self.new_cache, &h, &folder_item.parent);
                    self.send_update();
                }
            }

            // Transfer existing folders
            if !into.compacted {
                for folder_item in &existing_folders {
                    let h = hash_path(&folder_item.name);
                    self.update_cache.copy_with_children(&self.old_cache, &h, &folder_item.parent);
                }
            }

            // Scan existing folders
            for folder_item in existing_folders {
                if ctx.is_cancelled() {
                    return Err(ScannerError::Other("Operation cancelled".to_string()));
                }

                let h = hash_path(&folder_item.name);
                // Check if we should skip scanning folder...
                // We can only skip if we are not indexing into a compacted destination
                // and the entry itself is compacted.
                if !into.compacted && self.old_cache.is_compacted(&h) {
                    let next_cycle = self.old_cache.info.next_cycle as u32;
                    if !h.mod_(next_cycle, DATA_USAGE_UPDATE_DIR_CYCLES) {
                        // Transfer and add as child...
                        self.new_cache.copy_with_children(&self.old_cache, &h, &folder_item.parent);
                        into.add_child(&h);
                        continue;
                    }
                    // Adjust the probability of healing.
                    // This first removes lowest x from the mod check and makes it x times more likely.
                    // So if duudc = 10 and we want heal check every 50 cycles, we check
                    // if (cycle/10) % (50/10) == 0, which would make heal checks run once every 50 cycles,
                    // if the objects are pre-selected as 1:10.
                    // Note: This would require mutating folder_item, but we're iterating
                    // For now, we'll skip this optimization
                }

                (self.update_current_path)(folder_item.name.clone());

                let mut dst = if !into.compacted {
                    DataUsageEntry::default()
                } else {
                    into.clone()
                };

                // Use Box::pin for recursive async call
                let fut = Box::pin(self.scan_folder(ctx.clone(), folder_item.clone(), &mut dst));
                fut.await?;

                if !into.compacted {
                    into.add_child(&h);
                    self.update_cache.delete_recursive(&h);
                    self.update_cache.copy_with_children(&self.new_cache, &h, &folder_item.parent);
                    self.send_update();
                }
            }

            // Scan for healing
            if abandoned_children.is_empty() || !(self.should_heal)() {
                // If we are not heal scanning, return now.
                break;
            }

            if self.disks.is_empty() || self.disks_quorum == 0 {
                break;
            }

            // TODO: Implement healing logic for abandoned children
            // This is a complex part that involves:
            // - Getting heal sequence
            // - Listing paths with listPathRaw equivalent
            // - Queueing heal tasks
            // - Processing found objects

            break;
        }

        if !was_compacted {
            self.new_cache.replace_hashed(&this_hash, &folder.parent, into);
        }

        if !into.compacted && self.new_cache.info.name != folder.name {
            if let Some(mut flat) = self.new_cache.size_recursive(&this_hash.key()) {
                flat.compacted = true;
                let mut should_compact = false;

                if flat.objects < DATA_SCANNER_COMPACT_LEAST_OBJECT {
                    should_compact = true;
                } else {
                    // Compact if we only have objects as children...
                    should_compact = true;
                    for k in &into.children {
                        if let Some(v) = self.new_cache.cache.get(k) {
                            if !v.children.is_empty() || v.objects > 1 {
                                should_compact = false;
                                break;
                            }
                        }
                    }
                }

                if should_compact {
                    self.new_cache.delete_recursive(&this_hash);
                    self.new_cache.replace_hashed(&this_hash, &folder.parent, &flat);
                }
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

        if let Some(_) = self.update_cache.cache.get(&this_hash.key()) {
            if !was_compacted {
                // Replace if existed before.
                if let Some(flat) = self.new_cache.size_recursive(&this_hash.key()) {
                    self.update_cache.delete_recursive(&this_hash);
                    self.update_cache.replace_hashed(&this_hash, &folder.parent, &flat);
                }
            }
        }

        Ok(())
    }

    /// Get the new cache after scanning
    pub fn into_new_cache(mut self) -> DataUsageCache {
        self.new_cache.force_compact(DATA_SCANNER_COMPACT_AT_CHILDREN);
        self.new_cache
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
    base_path: String,
    cache: DataUsageCache,
    get_size: GetSizeFn,
    scan_mode: HealScanMode,
    we_sleep: Box<dyn Fn() -> bool + Send + Sync>,
    is_erasure: bool,
    update_current_path: Box<dyn Fn(String) + Send + Sync>,
    check_disk_healing: Option<Box<dyn Fn() -> bool + Send + Sync>>,
) -> Result<DataUsageCache, ScannerError> {
    use crate::data_usage_define::DATA_USAGE_ROOT;

    // Check that we're not trying to scan the root
    if cache.info.name.is_empty() || cache.info.name == DATA_USAGE_ROOT {
        return Err(ScannerError::Other("internal error: root scan attempted".to_string()));
    }

    // Create skip_heal flag
    let skip_heal = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(!is_erasure || cache.info.skip_healing));

    // Create heal_object_select flag
    let heal_object_select = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));

    // Wrap check_disk_healing in Arc if provided
    let check_disk_healing_arc = check_disk_healing.map(|f| Arc::new(f) as Arc<dyn Fn() -> bool + Send + Sync>);

    // Create should_heal closure
    let should_heal_closure: Arc<dyn Fn() -> bool + Send + Sync> = {
        let skip_heal_clone = skip_heal.clone();
        let heal_object_select_clone = heal_object_select.clone();
        let check_disk_healing_clone = check_disk_healing_arc.clone();
        Arc::new(move || {
            if skip_heal_clone.load(std::sync::atomic::Ordering::Relaxed) {
                return false;
            }
            if heal_object_select_clone.load(std::sync::atomic::Ordering::Relaxed) == 0 {
                return false;
            }
            // Check if disk is healing
            if let Some(ref check_fn) = check_disk_healing_clone {
                if check_fn() {
                    skip_heal_clone.store(true, std::sync::atomic::Ordering::Relaxed);
                    return false;
                }
            }
            true
        })
    };

    // Extract updates sender if available
    let updates_sender = if let Some(ref updates_arc) = cache.info.updates {
        // We can't easily extract the sender from Arc<Mutex<mpsc::Sender>>>
        // For now, we'll pass None and the scanner will handle updates through the cache
        None
    } else {
        None
    };

    // Create folder scanner
    let mut scanner = FolderScanner::new(
        base_path,
        get_size,
        cache.clone(),
        scan_mode,
        we_sleep,
        should_heal_closure.clone(),
        disks,
        updates_sender,
        update_current_path,
    );

    // Enable healing in erasure mode
    if is_erasure && !cache.info.skip_healing {
        // Do a heal check on an object once every n cycles. Must divide into healFolderInclude
        heal_object_select.store(HEAL_OBJECT_SELECT_PROB, std::sync::atomic::Ordering::Relaxed);
        scanner.set_heal_object_select(HEAL_OBJECT_SELECT_PROB);
    }

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
            let mut new_cache = scanner.into_new_cache();
            new_cache.info.last_update = Some(SystemTime::now());
            new_cache.info.next_cycle = cache.info.next_cycle;
            Ok(new_cache)
        }
        Err(e) => {
            // No useful information, return original cache
            Err(e)
        }
    }
}
