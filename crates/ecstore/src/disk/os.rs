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

#[cfg(windows)]
use crate::disk::ConditionalFileUpdate;
use crate::disk::error::DiskError;
use crate::disk::error::Result;
use crate::disk::error_conv::to_file_error;
use futures::TryStreamExt;
use parking_lot::Mutex;
use rustfs_utils::path::SLASH_SEPARATOR;
use std::{
    collections::{HashMap, VecDeque},
    io,
    path::{Component, Path, PathBuf},
    sync::{Arc, LazyLock, Weak},
    time::{Duration, Instant},
};
use tokio::fs;
use tokio::sync::{
    Mutex as AsyncMutex, OwnedMutexGuard, OwnedRwLockReadGuard, OwnedSemaphorePermit, RwLock, Semaphore, SemaphorePermit, oneshot,
};
use tracing::warn;

/// Check path length according to OS limits.
pub fn check_path_length(path_name: &str) -> Result<()> {
    // Apple OS X path length is limited to 1016
    if cfg!(target_os = "macos") && path_name.len() > 1016 {
        return Err(DiskError::FileNameTooLong);
    }

    // Disallow more than 1024 characters on windows, there
    // are no known name_max limits on Windows.
    if cfg!(target_os = "windows") && path_name.len() > 1024 {
        return Err(DiskError::FileNameTooLong);
    }

    // On Unix we reject paths if they are just '.', '..' or '/'
    let invalid_paths = [".", "..", "/"];
    if invalid_paths.contains(&path_name) {
        return Err(DiskError::FileAccessDenied);
    }

    // Check each path segment length is > 255 on all Unix
    // platforms, look for this value as NAME_MAX in
    // /usr/include/linux/limits.h
    let mut count = 0usize;
    for c in path_name.chars() {
        match c {
            '/' => count = 0,
            '\\' if cfg!(target_os = "windows") => count = 0, // Reset
            _ => {
                count += 1;
                if count > 255 {
                    return Err(DiskError::FileNameTooLong);
                }
            }
        }
    }

    // Success.
    Ok(())
}

/// Test-only recorder of every directory passed to [`fsync_dir_std`].
///
/// Durability regressions are invisible to ordinary behavior tests (the data
/// is on disk either way), so unit tests assert directly on which directories
/// were fsynced. Paths are recorded globally; tests must match on paths under
/// their own unique tempdir to stay robust against parallel test execution.
#[cfg(test)]
pub(crate) mod fsync_dir_recorder {
    use std::collections::HashMap;
    use std::io;
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;

    type Hook = Box<dyn FnOnce() + Send>;

    static RECORDED: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());
    static LIMITED: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());
    static GROUPED: Mutex<Vec<(PathBuf, usize)>> = Mutex::new(Vec::new());
    static BEFORE_LIMITED: std::sync::LazyLock<Mutex<HashMap<PathBuf, Hook>>> =
        std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));
    static BEFORE_GROUP_BATCH: std::sync::LazyLock<Mutex<HashMap<PathBuf, Hook>>> =
        std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));
    static AFTER_GROUP_ENQUEUE: std::sync::LazyLock<Mutex<HashMap<PathBuf, Hook>>> =
        std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));
    static BEFORE_GROUPED: std::sync::LazyLock<Mutex<HashMap<PathBuf, Hook>>> =
        std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));
    static GROUPED_FAILURES: std::sync::LazyLock<Mutex<HashMap<PathBuf, io::ErrorKind>>> =
        std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

    fn record_path(paths: &Mutex<Vec<PathBuf>>, path: &Path, description: &str) {
        let mut paths = paths.lock().expect(description);
        paths.push(path.to_path_buf());
        if let Ok(canonical) = path.canonicalize()
            && canonical != path
        {
            paths.push(canonical);
        }
    }

    fn contains_path(paths: &[PathBuf], path: &Path) -> bool {
        let canonical = path.canonicalize().ok();
        paths
            .iter()
            .any(|recorded| recorded == path || canonical.as_ref().is_some_and(|canonical| recorded == canonical))
    }

    fn remove_path_keyed<T>(entries: &Mutex<HashMap<PathBuf, T>>, dir: &Path, description: &str) -> Option<T> {
        let mut entries = entries.lock().expect(description);
        if let Some(value) = entries.remove(dir) {
            return Some(value);
        }
        let canonical = dir.canonicalize().ok();
        let matching_key = entries
            .keys()
            .find(|registered| {
                registered.as_path() == dir
                    || canonical.as_ref().is_some_and(|canonical| *registered == canonical)
                    || registered.canonicalize().ok().is_some_and(|registered_canonical| {
                        registered_canonical == dir || canonical.as_ref() == Some(&registered_canonical)
                    })
            })
            .cloned();
        matching_key.and_then(|key| entries.remove(&key))
    }

    fn remove_hook(hooks: &Mutex<HashMap<PathBuf, Hook>>, dir: &Path, description: &str) -> Option<Hook> {
        remove_path_keyed(hooks, dir, description)
    }

    pub(crate) fn record(dir: &Path) {
        record_path(&RECORDED, dir, "fsync dir recorder");
    }

    pub(crate) fn was_fsynced(dir: &Path) -> bool {
        contains_path(&RECORDED.lock().expect("fsync dir recorder poisoned"), dir)
    }

    pub(crate) fn record_limited(dir: &Path) {
        record_path(&LIMITED, dir, "limited fsync dir recorder");
        let hook = remove_hook(&BEFORE_LIMITED, dir, "limited fsync hook poisoned");
        if let Some(hook) = hook {
            hook();
        }
    }

    pub(crate) fn was_limited(dir: &Path) -> bool {
        contains_path(&LIMITED.lock().expect("limited fsync dir recorder poisoned"), dir)
    }

    pub(crate) fn set_before_limited(dir: &Path, hook: impl FnOnce() + Send + 'static) {
        BEFORE_LIMITED
            .lock()
            .expect("limited fsync hook poisoned")
            .insert(dir.to_path_buf(), Box::new(hook));
    }

    pub(crate) fn record_grouped(dir: &Path, batch_len: usize) {
        let mut grouped = GROUPED.lock().expect("grouped fsync dir recorder poisoned");
        grouped.push((dir.to_path_buf(), batch_len));
        if let Ok(canonical) = dir.canonicalize()
            && canonical != dir
        {
            grouped.push((canonical, batch_len));
        }
        drop(grouped);
        let hook = remove_hook(&BEFORE_GROUPED, dir, "grouped fsync hook poisoned");
        if let Some(hook) = hook {
            hook();
        }
    }

    pub(crate) fn run_before_group_batch(dir: &Path) {
        let hook = remove_hook(&BEFORE_GROUP_BATCH, dir, "grouped fsync batch hook poisoned");
        if let Some(hook) = hook {
            hook();
        }
    }

    pub(crate) fn set_before_group_batch(dir: &Path, hook: impl FnOnce() + Send + 'static) {
        BEFORE_GROUP_BATCH
            .lock()
            .expect("grouped fsync batch hook poisoned")
            .insert(dir.to_path_buf(), Box::new(hook));
    }

    pub(crate) fn run_after_group_enqueue(dir: &Path) {
        let hook = remove_hook(&AFTER_GROUP_ENQUEUE, dir, "grouped fsync enqueue hook poisoned");
        if let Some(hook) = hook {
            hook();
        }
    }

    pub(crate) fn set_after_group_enqueue(dir: &Path, hook: impl FnOnce() + Send + 'static) {
        AFTER_GROUP_ENQUEUE
            .lock()
            .expect("grouped fsync enqueue hook poisoned")
            .insert(dir.to_path_buf(), Box::new(hook));
    }

    pub(crate) fn grouped_batch_sizes(dir: &Path) -> Vec<usize> {
        let grouped = GROUPED.lock().expect("grouped fsync dir recorder poisoned");
        let canonical = dir.canonicalize().ok();
        grouped
            .iter()
            .filter_map(|(recorded, batch_len)| {
                (recorded == dir || canonical.as_ref().is_some_and(|canonical| recorded == canonical)).then_some(*batch_len)
            })
            .collect()
    }

    pub(crate) fn set_before_grouped(dir: &Path, hook: impl FnOnce() + Send + 'static) {
        BEFORE_GROUPED
            .lock()
            .expect("grouped fsync hook poisoned")
            .insert(dir.to_path_buf(), Box::new(hook));
    }

    pub(crate) fn set_grouped_failure(dir: &Path, kind: io::ErrorKind) {
        GROUPED_FAILURES
            .lock()
            .expect("grouped fsync failure hook poisoned")
            .insert(dir.to_path_buf(), kind);
    }

    pub(crate) fn take_grouped_failure(dir: &Path) -> Option<io::ErrorKind> {
        remove_path_keyed(&GROUPED_FAILURES, dir, "grouped fsync failure hook poisoned")
    }
}

#[cfg(all(test, windows))]
pub(crate) mod windows_rename_test_hooks {
    use super::*;

    type Hook = Box<dyn FnOnce() + Send>;

    static BEFORE_SOURCE_WRITE: LazyLock<Mutex<HashMap<PathBuf, Hook>>> = LazyLock::new(|| Mutex::new(HashMap::new()));
    static BEFORE_PUBLICATION: LazyLock<Mutex<HashMap<PathBuf, Hook>>> = LazyLock::new(|| Mutex::new(HashMap::new()));
    static BEFORE_RENAME_RETRY: LazyLock<Mutex<HashMap<PathBuf, Hook>>> = LazyLock::new(|| Mutex::new(HashMap::new()));
    static GUARD_GENERATIONS: LazyLock<Mutex<HashMap<PathBuf, Vec<u64>>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

    pub(crate) fn install_before_source_write(path: &Path, hook: impl FnOnce() + Send + 'static) {
        BEFORE_SOURCE_WRITE.lock().insert(path.to_path_buf(), Box::new(hook));
    }

    pub(crate) fn run_before_source_write(path: &Path) {
        if let Some(hook) = BEFORE_SOURCE_WRITE.lock().remove(path) {
            hook();
        }
    }

    pub(crate) fn install_before_publication(path: &Path, hook: impl FnOnce() + Send + 'static) {
        BEFORE_PUBLICATION.lock().insert(path.to_path_buf(), Box::new(hook));
    }

    pub(crate) fn run_before_publication(path: &Path) {
        if let Some(hook) = BEFORE_PUBLICATION.lock().remove(path) {
            hook();
        }
    }

    pub(crate) fn install_before_rename_retry(path: &Path, hook: impl FnOnce() + Send + 'static) {
        BEFORE_RENAME_RETRY.lock().insert(path.to_path_buf(), Box::new(hook));
    }

    pub(crate) fn run_before_rename_retry(path: &Path) {
        if let Some(hook) = BEFORE_RENAME_RETRY.lock().remove(path) {
            hook();
        }
    }

    pub(crate) fn observe_guard_generations(path: &Path) {
        GUARD_GENERATIONS.lock().insert(path.to_path_buf(), Vec::new());
    }

    pub(crate) fn record_guard_generation(path: &Path, generation: u64) {
        if let Some(generations) = GUARD_GENERATIONS.lock().get_mut(path) {
            generations.push(generation);
        }
    }

    pub(crate) fn take_guard_generations(path: &Path) -> Vec<u64> {
        GUARD_GENERATIONS.lock().remove(path).unwrap_or_default()
    }
}

/// Fsync a directory so recently created or renamed entries survive power loss.
/// No-op on non-Unix platforms where directories cannot be opened for syncing.
pub fn fsync_dir_std(dir: impl AsRef<Path>) -> io::Result<()> {
    #[cfg(test)]
    fsync_dir_recorder::record(dir.as_ref());
    #[cfg(unix)]
    {
        std::fs::File::open(dir.as_ref())?.sync_all()?;
    }
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

/// Async wrapper around [`fsync_dir_std`]; runs the blocking Unix fsync off the runtime.
pub async fn fsync_dir(dir: impl AsRef<Path>) -> io::Result<()> {
    #[cfg(unix)]
    {
        let dir = dir.as_ref().to_path_buf();
        fsync_spawn_blocking(move || fsync_dir_std(dir)).await?
    }

    #[cfg(not(unix))]
    {
        fsync_dir_std(dir)
    }
}

const ENV_DST_DIR_FSYNC_GROUP_COMMIT_ENABLE: &str = "RUSTFS_EXPERIMENTAL_DST_DIR_FSYNC_GROUP_COMMIT_ENABLE";
const DEFAULT_DST_DIR_FSYNC_GROUP_COMMIT_ENABLE: bool = false;
const ENV_FILE_FDATASYNC_GROUP_COMMIT_ENABLE: &str = "RUSTFS_EXPERIMENTAL_FILE_FDATASYNC_GROUP_COMMIT_ENABLE";
const DEFAULT_FILE_FDATASYNC_GROUP_COMMIT_ENABLE: bool = false;
const ENV_FILE_FDATASYNC_GROUP_COMMIT_WAIT_MICROS: &str = "RUSTFS_EXPERIMENTAL_FILE_FDATASYNC_GROUP_COMMIT_WAIT_MICROS";
const DEFAULT_FILE_FDATASYNC_GROUP_COMMIT_WAIT_MICROS: u64 = 0;
const MAX_FILE_FDATASYNC_GROUP_COMMIT_WAIT_MICROS: u64 = 1_000;
#[cfg(not(test))]
const MAX_DST_DIR_FSYNC_GROUPS: usize = 1024;
#[cfg(test)]
const MAX_DST_DIR_FSYNC_GROUPS: usize = 4;
#[cfg(not(test))]
const MAX_DST_DIR_FSYNC_WAITERS: usize = 8192;
#[cfg(test)]
const MAX_DST_DIR_FSYNC_WAITERS: usize = 8;
#[cfg(not(test))]
const MAX_FILE_FDATASYNC_GROUPS: usize = 1024;
#[cfg(test)]
const MAX_FILE_FDATASYNC_GROUPS: usize = 4;
#[cfg(not(test))]
const MAX_FILE_FDATASYNC_WAITERS: usize = 8192;
#[cfg(test)]
const MAX_FILE_FDATASYNC_WAITERS: usize = 8;
#[cfg(not(test))]
const MAX_FILE_FDATASYNC_BATCH_FILES: usize = 1024;
#[cfg(test)]
const MAX_FILE_FDATASYNC_BATCH_FILES: usize = 8;
static DST_DIR_FSYNC_GROUP_COMMIT_ENABLED: LazyLock<bool> = LazyLock::new(|| {
    rustfs_utils::get_env_bool(ENV_DST_DIR_FSYNC_GROUP_COMMIT_ENABLE, DEFAULT_DST_DIR_FSYNC_GROUP_COMMIT_ENABLE)
});
static FILE_FDATASYNC_GROUP_COMMIT_ENABLED: LazyLock<bool> = LazyLock::new(|| {
    rustfs_utils::get_env_bool(ENV_FILE_FDATASYNC_GROUP_COMMIT_ENABLE, DEFAULT_FILE_FDATASYNC_GROUP_COMMIT_ENABLE)
});
fn file_fdatasync_group_commit_wait_duration(wait_micros: u64) -> Duration {
    Duration::from_micros(wait_micros.min(MAX_FILE_FDATASYNC_GROUP_COMMIT_WAIT_MICROS))
}

static FILE_FDATASYNC_GROUP_COMMIT_WAIT: LazyLock<Duration> = LazyLock::new(|| {
    file_fdatasync_group_commit_wait_duration(rustfs_utils::get_env_u64(
        ENV_FILE_FDATASYNC_GROUP_COMMIT_WAIT_MICROS,
        DEFAULT_FILE_FDATASYNC_GROUP_COMMIT_WAIT_MICROS,
    ))
});

#[cfg(test)]
mod dst_dir_fsync_group_commit_override {
    use std::sync::{Mutex, MutexGuard, PoisonError, RwLock};

    static OVERRIDE: RwLock<Option<bool>> = RwLock::new(None);
    static SERIAL: Mutex<()> = Mutex::new(());

    pub(crate) fn get() -> Option<bool> {
        *OVERRIDE.read().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) struct OverrideGuard {
        _serial: MutexGuard<'static, ()>,
    }

    impl Drop for OverrideGuard {
        fn drop(&mut self) {
            *OVERRIDE.write().unwrap_or_else(PoisonError::into_inner) = None;
        }
    }

    pub(crate) fn set(enabled: bool) -> OverrideGuard {
        let serial = SERIAL.lock().unwrap_or_else(PoisonError::into_inner);
        *OVERRIDE.write().unwrap_or_else(PoisonError::into_inner) = Some(enabled);
        OverrideGuard { _serial: serial }
    }
}

#[cfg(test)]
pub(crate) fn set_dst_dir_fsync_group_commit_for_test(enabled: bool) -> dst_dir_fsync_group_commit_override::OverrideGuard {
    dst_dir_fsync_group_commit_override::set(enabled)
}

fn dst_dir_fsync_group_commit_enabled() -> bool {
    #[cfg(test)]
    if let Some(enabled) = dst_dir_fsync_group_commit_override::get() {
        return enabled;
    }

    *DST_DIR_FSYNC_GROUP_COMMIT_ENABLED
}

#[cfg(test)]
mod file_fdatasync_group_commit_override {
    use std::sync::{Mutex, MutexGuard, PoisonError, RwLock};

    static OVERRIDE: RwLock<Option<bool>> = RwLock::new(None);
    static WAIT_OVERRIDE_MICROS: RwLock<Option<u64>> = RwLock::new(None);
    static SERIAL: Mutex<()> = Mutex::new(());

    pub(crate) fn get() -> Option<bool> {
        *OVERRIDE.read().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) struct OverrideGuard {
        _serial: MutexGuard<'static, ()>,
    }

    impl Drop for OverrideGuard {
        fn drop(&mut self) {
            *OVERRIDE.write().unwrap_or_else(PoisonError::into_inner) = None;
            *WAIT_OVERRIDE_MICROS.write().unwrap_or_else(PoisonError::into_inner) = None;
        }
    }

    pub(crate) fn set(enabled: bool) -> OverrideGuard {
        let serial = SERIAL.lock().unwrap_or_else(PoisonError::into_inner);
        *OVERRIDE.write().unwrap_or_else(PoisonError::into_inner) = Some(enabled);
        OverrideGuard { _serial: serial }
    }

    pub(crate) fn set_wait_micros(wait_micros: u64) {
        *WAIT_OVERRIDE_MICROS.write().unwrap_or_else(PoisonError::into_inner) = Some(wait_micros);
    }

    pub(crate) fn wait_micros() -> Option<u64> {
        *WAIT_OVERRIDE_MICROS.read().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
pub(crate) fn set_file_fdatasync_group_commit_for_test(enabled: bool) -> file_fdatasync_group_commit_override::OverrideGuard {
    file_fdatasync_group_commit_override::set(enabled)
}

#[cfg(test)]
fn set_file_fdatasync_group_commit_wait_for_test(wait_micros: u64) {
    file_fdatasync_group_commit_override::set_wait_micros(wait_micros);
}

fn file_fdatasync_group_commit_enabled() -> bool {
    #[cfg(test)]
    if let Some(enabled) = file_fdatasync_group_commit_override::get() {
        return enabled;
    }

    *FILE_FDATASYNC_GROUP_COMMIT_ENABLED
}

fn file_fdatasync_group_commit_wait() -> Duration {
    #[cfg(test)]
    if let Some(wait_micros) = file_fdatasync_group_commit_override::wait_micros() {
        return file_fdatasync_group_commit_wait_duration(wait_micros);
    }

    *FILE_FDATASYNC_GROUP_COMMIT_WAIT
}

#[derive(Clone, Eq, Hash, PartialEq)]
struct DstDirFsyncGroupKey {
    canonical_path: PathBuf,
    #[cfg(unix)]
    dev: u64,
    #[cfg(unix)]
    ino: u64,
}

impl DstDirFsyncGroupKey {
    fn from_metadata(canonical_path: PathBuf, metadata: std::fs::Metadata) -> io::Result<Self> {
        if !metadata.is_dir() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "dst dir fsync group key must be a directory"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            Ok(Self {
                canonical_path,
                dev: metadata.dev(),
                ino: metadata.ino(),
            })
        }
        #[cfg(not(unix))]
        {
            Ok(Self { canonical_path })
        }
    }
}

struct OpenedDstDirFsyncGroup {
    key: DstDirFsyncGroupKey,
    #[cfg(any(test, not(unix)))]
    dir: PathBuf,
    #[cfg(unix)]
    dir_file: Arc<std::fs::File>,
}

impl OpenedDstDirFsyncGroup {
    fn open(dir: &Path) -> io::Result<Self> {
        let canonical_path = dir.canonicalize()?;
        #[cfg(unix)]
        {
            let file = std::fs::File::open(&canonical_path)?;
            let key = DstDirFsyncGroupKey::from_metadata(canonical_path, file.metadata()?)?;
            #[cfg(test)]
            let dir = key.canonical_path.clone();
            Ok(Self {
                key,
                #[cfg(test)]
                dir,
                dir_file: Arc::new(file),
            })
        }
        #[cfg(not(unix))]
        {
            let metadata = std::fs::metadata(&canonical_path)?;
            let key = DstDirFsyncGroupKey::from_metadata(canonical_path, metadata)?;
            let dir = key.canonical_path.clone();
            Ok(Self { key, dir })
        }
    }
}

struct DstDirFsyncWaiter {
    result_tx: oneshot::Sender<SharedDstDirFsyncResult>,
}

#[derive(Clone)]
struct SharedDstDirFsyncError {
    kind: io::ErrorKind,
    message: Arc<str>,
}

impl SharedDstDirFsyncError {
    fn from_error(err: io::Error) -> Self {
        Self {
            kind: err.kind(),
            message: Arc::from(err.to_string()),
        }
    }

    fn into_error(self) -> io::Error {
        io::Error::new(self.kind, self.message.to_string())
    }
}

type SharedDstDirFsyncResult = std::result::Result<(), SharedDstDirFsyncError>;

struct DstDirFsyncGroup {
    key: DstDirFsyncGroupKey,
    #[cfg(any(test, not(unix)))]
    dir: PathBuf,
    #[cfg(unix)]
    dir_file: Arc<std::fs::File>,
    inner: Mutex<DstDirFsyncGroupInner>,
}

#[derive(Default)]
struct DstDirFsyncGroupInner {
    worker_running: bool,
    pending: VecDeque<DstDirFsyncWaiter>,
}

#[derive(Default)]
struct DstDirFsyncGroupCommit {
    inner: Mutex<DstDirFsyncGroupCommitInner>,
}

#[derive(Default)]
struct DstDirFsyncGroupCommitInner {
    groups: HashMap<DstDirFsyncGroupKey, Arc<DstDirFsyncGroup>>,
    total_waiters: usize,
}

static DST_DIR_FSYNC_GROUP_COMMIT: LazyLock<DstDirFsyncGroupCommit> = LazyLock::new(DstDirFsyncGroupCommit::default);

impl DstDirFsyncGroupCommit {
    // Lock order: registry first, then per-group state. No path may hold a
    // group lock while acquiring the registry lock.
    fn enqueue_opened(
        &self,
        opened: OpenedDstDirFsyncGroup,
    ) -> io::Result<(oneshot::Receiver<SharedDstDirFsyncResult>, Option<Arc<DstDirFsyncGroup>>)> {
        let (result_tx, result_rx) = oneshot::channel();
        let mut registry = self.inner.lock();
        if registry.total_waiters >= MAX_DST_DIR_FSYNC_WAITERS {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "dst dir fsync group commit waiter limit reached",
            ));
        }
        let group = if let Some(group) = registry.groups.get(&opened.key) {
            group.clone()
        } else {
            if registry.groups.len() >= MAX_DST_DIR_FSYNC_GROUPS {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "dst dir fsync group commit active group limit reached",
                ));
            }
            let group = Arc::new(DstDirFsyncGroup {
                key: opened.key.clone(),
                #[cfg(any(test, not(unix)))]
                dir: opened.dir,
                #[cfg(unix)]
                dir_file: opened.dir_file,
                inner: Mutex::new(DstDirFsyncGroupInner::default()),
            });
            registry.groups.insert(opened.key, group.clone());
            group
        };
        let mut group_state = group.inner.lock();
        group_state.pending.push_back(DstDirFsyncWaiter { result_tx });
        let start_worker = !group_state.worker_running;
        if start_worker {
            group_state.worker_running = true;
        }
        registry.total_waiters += 1;
        drop(group_state);
        drop(registry);
        #[cfg(test)]
        fsync_dir_recorder::run_after_group_enqueue(&group.dir);

        Ok((result_rx, start_worker.then_some(group)))
    }

    fn complete_batch(&self, count: usize) {
        let mut registry = self.inner.lock();
        registry.total_waiters = registry.total_waiters.saturating_sub(count);
    }

    fn remove_idle_group(&self, group: &Arc<DstDirFsyncGroup>) {
        let mut registry = self.inner.lock();
        let group_state = group.inner.lock();
        if !group_state.worker_running && group_state.pending.is_empty() {
            registry.groups.remove(&group.key);
        }
    }

    #[cfg(test)]
    fn counts_for_test(&self) -> (usize, usize) {
        let registry = self.inner.lock();
        (registry.groups.len(), registry.total_waiters)
    }

    #[cfg(test)]
    fn clear_for_test(&self) {
        let mut registry = self.inner.lock();
        registry.groups.clear();
        registry.total_waiters = 0;
    }

    #[cfg(test)]
    fn enqueue_for_test(
        &self,
        dir: &Path,
    ) -> io::Result<(oneshot::Receiver<SharedDstDirFsyncResult>, Option<Arc<DstDirFsyncGroup>>)> {
        self.enqueue_opened(OpenedDstDirFsyncGroup::open(dir)?)
    }
}

#[cfg(unix)]
async fn fsync_open_dst_dir_group(group: &DstDirFsyncGroup) -> io::Result<()> {
    #[cfg(test)]
    let dir = group.dir.clone();
    let dir_file = group.dir_file.clone();
    fsync_spawn_blocking(move || {
        #[cfg(test)]
        {
            if let Some(kind) = fsync_dir_recorder::take_grouped_failure(&dir) {
                return Err(io::Error::new(kind, "injected grouped dst dir fsync failure"));
            }
            fsync_dir_recorder::record(&dir);
        }
        dir_file.sync_all()
    })
    .await
    .map_err(|err| io::Error::other(format!("blocking dst dir group fsync failed: {err}")))?
}

#[cfg(not(unix))]
async fn fsync_open_dst_dir_group(group: &DstDirFsyncGroup) -> io::Result<()> {
    fsync_dir(&group.dir).await
}

async fn run_dst_dir_fsync_group_worker(group: Arc<DstDirFsyncGroup>) {
    loop {
        #[cfg(test)]
        fsync_dir_recorder::run_before_group_batch(&group.dir);
        tokio::task::yield_now().await;
        let batch: Vec<DstDirFsyncWaiter> = {
            let mut group_state = group.inner.lock();
            group_state.pending.drain(..).collect()
        };
        if batch.is_empty() {
            let mut group_state = group.inner.lock();
            group_state.worker_running = false;
            drop(group_state);
            DST_DIR_FSYNC_GROUP_COMMIT.remove_idle_group(&group);
            return;
        }

        #[cfg(test)]
        fsync_dir_recorder::record_grouped(&group.dir, batch.len());
        let result = fsync_open_dst_dir_group(&group)
            .await
            .map_err(SharedDstDirFsyncError::from_error);
        let batch_len = batch.len();
        DST_DIR_FSYNC_GROUP_COMMIT.complete_batch(batch_len);

        let should_stop = {
            let mut group_state = group.inner.lock();
            if group_state.pending.is_empty() {
                group_state.worker_running = false;
                true
            } else {
                false
            }
        };
        if should_stop {
            DST_DIR_FSYNC_GROUP_COMMIT.remove_idle_group(&group);
        }
        for waiter in batch {
            let _ = waiter.result_tx.send(result.clone());
        }
        if should_stop {
            return;
        }
    }
}

async fn fsync_dst_dir_group_commit_with_enabled(dir: impl AsRef<Path>, enabled: bool) -> io::Result<()> {
    if !enabled {
        return fsync_dir(dir).await;
    }

    let dir = dir.as_ref().to_path_buf();
    let opened = tokio::task::spawn_blocking(move || OpenedDstDirFsyncGroup::open(&dir))
        .await
        .map_err(|err| io::Error::other(format!("blocking dst dir group open failed: {err}")))??;
    let (result_rx, worker) = DST_DIR_FSYNC_GROUP_COMMIT.enqueue_opened(opened)?;
    if let Some(group) = worker {
        tokio::spawn(run_dst_dir_fsync_group_worker(group));
    }

    match result_rx.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => Err(err.into_error()),
        Err(_) => Err(io::Error::other("dst dir fsync group worker dropped the waiter")),
    }
}

pub(crate) async fn fsync_dst_dir_group_commit(dir: impl AsRef<Path>) -> io::Result<()> {
    fsync_dst_dir_group_commit_with_enabled(dir, dst_dir_fsync_group_commit_enabled()).await
}

pub(crate) async fn fsync_dst_dir_group_commit_or_namespace_file_sync_limit(
    dir: impl AsRef<Path>,
    lease: Arc<NamespaceMutationLease>,
    admission: &FileSyncAdmission,
) -> io::Result<()> {
    if dst_dir_fsync_group_commit_enabled() {
        fsync_dst_dir_group_commit_with_enabled(dir, true).await
    } else {
        fsync_dir_with_namespace_file_sync_limit(dir, lease, admission).await
    }
}

#[cfg(test)]
pub(crate) async fn fsync_dst_dir_group_commit_for_test(dir: impl AsRef<Path>, enabled: bool) -> io::Result<()> {
    fsync_dst_dir_group_commit_with_enabled(dir, enabled).await
}

#[cfg(test)]
pub(crate) fn dst_dir_fsync_group_commit_counts_for_test() -> (usize, usize) {
    DST_DIR_FSYNC_GROUP_COMMIT.counts_for_test()
}

#[cfg(test)]
fn clear_dst_dir_fsync_group_commit_for_test() {
    DST_DIR_FSYNC_GROUP_COMMIT.clear_for_test();
}

type FileFdatasyncGroupKey = usize;

struct FileFdatasyncWaiter {
    files: Vec<PathBuf>,
    enqueued_at: Option<Instant>,
    wait_role: &'static str,
    result_tx: oneshot::Sender<SharedFileFdatasyncResult>,
}

#[derive(Clone)]
struct SharedFileFdatasyncError {
    kind: io::ErrorKind,
    message: Arc<str>,
}

impl SharedFileFdatasyncError {
    fn from_error(err: io::Error) -> Self {
        Self {
            kind: err.kind(),
            message: Arc::from(err.to_string()),
        }
    }

    fn into_error(self) -> io::Error {
        io::Error::new(self.kind, self.message.to_string())
    }
}

type SharedFileFdatasyncResult = std::result::Result<(), SharedFileFdatasyncError>;

struct FileFdatasyncGroup {
    key: FileFdatasyncGroupKey,
    disk_permits: Weak<Semaphore>,
    inner: Mutex<FileFdatasyncGroupInner>,
}

#[derive(Default)]
struct FileFdatasyncGroupInner {
    worker_running: bool,
    pending_files: usize,
    pending: VecDeque<FileFdatasyncWaiter>,
}

#[derive(Default)]
struct FileFdatasyncGroupCommit {
    inner: Mutex<FileFdatasyncGroupCommitInner>,
}

#[derive(Default)]
struct FileFdatasyncGroupCommitInner {
    groups: HashMap<FileFdatasyncGroupKey, Arc<FileFdatasyncGroup>>,
    total_waiters: usize,
    total_files: usize,
}

static FILE_FDATASYNC_GROUP_COMMIT: LazyLock<FileFdatasyncGroupCommit> = LazyLock::new(FileFdatasyncGroupCommit::default);

impl FileFdatasyncGroupCommit {
    // Lock order: registry first, then per-group state. No path may hold a
    // group lock while acquiring the registry lock.
    fn enqueue(
        &self,
        disk_permits: Arc<Semaphore>,
        files: Vec<PathBuf>,
    ) -> io::Result<(oneshot::Receiver<SharedFileFdatasyncResult>, Option<Arc<FileFdatasyncGroup>>)> {
        if files.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "file fdatasync group commit needs at least one file",
            ));
        }
        let (result_tx, result_rx) = oneshot::channel();
        let key = Arc::as_ptr(&disk_permits) as FileFdatasyncGroupKey;
        let mut registry = self.inner.lock();
        registry.groups.retain(|_, group| group.disk_permits.strong_count() > 0);
        if registry.total_waiters >= MAX_FILE_FDATASYNC_WAITERS {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "file fdatasync group commit waiter limit reached",
            ));
        }
        if registry.total_files.saturating_add(files.len()) > MAX_FILE_FDATASYNC_BATCH_FILES {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "file fdatasync group commit file limit reached",
            ));
        }
        let group = if let Some(group) = registry.groups.get(&key) {
            group.clone()
        } else {
            if registry.groups.len() >= MAX_FILE_FDATASYNC_GROUPS {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "file fdatasync group commit active group limit reached",
                ));
            }
            let group = Arc::new(FileFdatasyncGroup {
                key,
                disk_permits: Arc::downgrade(&disk_permits),
                inner: Mutex::new(FileFdatasyncGroupInner::default()),
            });
            registry.groups.insert(key, group.clone());
            group
        };
        let file_count = files.len();
        let mut group_state = group.inner.lock();
        let start_worker = !group_state.worker_running;
        let wait_role = if start_worker {
            rustfs_io_metrics::PUT_RENAME_FDATASYNC_GROUP_WAIT_ROLE_LEADER
        } else {
            rustfs_io_metrics::PUT_RENAME_FDATASYNC_GROUP_WAIT_ROLE_FOLLOWER
        };
        group_state.pending.push_back(FileFdatasyncWaiter {
            files,
            enqueued_at: rustfs_io_metrics::put_stage_timer(),
            wait_role,
            result_tx,
        });
        group_state.pending_files += file_count;
        if start_worker {
            group_state.worker_running = true;
        }
        rustfs_io_metrics::record_put_rename_fdatasync_group_outstanding(
            rustfs_io_metrics::PUT_RENAME_FDATASYNC_GROUP_OUTSTANDING_STATE_ENQUEUE_WAITERS,
            group_state.pending.len(),
        );
        rustfs_io_metrics::record_put_rename_fdatasync_group_outstanding(
            rustfs_io_metrics::PUT_RENAME_FDATASYNC_GROUP_OUTSTANDING_STATE_ENQUEUE_FILES,
            group_state.pending_files,
        );
        registry.total_waiters += 1;
        registry.total_files += file_count;
        drop(group_state);
        drop(registry);
        Ok((result_rx, start_worker.then_some(group)))
    }

    fn complete_batch(&self, waiters: usize, files: usize) {
        let mut registry = self.inner.lock();
        registry.total_waiters = registry.total_waiters.saturating_sub(waiters);
        registry.total_files = registry.total_files.saturating_sub(files);
    }

    fn remove_idle_group(&self, group: &Arc<FileFdatasyncGroup>) {
        let mut registry = self.inner.lock();
        let group_state = group.inner.lock();
        if !group_state.worker_running && group_state.pending.is_empty() {
            registry.groups.remove(&group.key);
        }
    }

    #[cfg(test)]
    fn counts_for_test(&self) -> (usize, usize, usize) {
        let registry = self.inner.lock();
        (registry.groups.len(), registry.total_waiters, registry.total_files)
    }

    #[cfg(test)]
    fn clear_for_test(&self) {
        let mut registry = self.inner.lock();
        registry.groups.clear();
        registry.total_waiters = 0;
        registry.total_files = 0;
    }
}

async fn run_file_fdatasync_group_worker(group: Arc<FileFdatasyncGroup>) {
    loop {
        #[cfg(test)]
        file_sync_probe::run_before_group_batch();
        tokio::task::yield_now().await;
        let wait = file_fdatasync_group_commit_wait();
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
        let (batch, batch_file_count): (Vec<FileFdatasyncWaiter>, usize) = {
            let mut group_state = group.inner.lock();
            let batch_file_count = group_state.pending_files;
            group_state.pending_files = 0;
            (group_state.pending.drain(..).collect(), batch_file_count)
        };
        if batch.is_empty() {
            let mut group_state = group.inner.lock();
            group_state.worker_running = false;
            drop(group_state);
            FILE_FDATASYNC_GROUP_COMMIT.remove_idle_group(&group);
            return;
        }

        rustfs_io_metrics::record_put_rename_fdatasync_group_outstanding(
            rustfs_io_metrics::PUT_RENAME_FDATASYNC_GROUP_OUTSTANDING_STATE_BATCH_WAITERS,
            batch.len(),
        );
        rustfs_io_metrics::record_put_rename_fdatasync_group_outstanding(
            rustfs_io_metrics::PUT_RENAME_FDATASYNC_GROUP_OUTSTANDING_STATE_BATCH_FILES,
            batch_file_count,
        );
        for waiter in &batch {
            if let Some(enqueued_at) = waiter.enqueued_at {
                rustfs_io_metrics::record_put_rename_fdatasync_group_wait(
                    waiter.wait_role,
                    enqueued_at.elapsed().as_secs_f64() * 1000.0,
                );
            }
        }
        let batch_files: Vec<PathBuf> = batch.iter().flat_map(|waiter| waiter.files.iter().cloned()).collect();
        #[cfg(test)]
        file_sync_probe::record_group_batch(batch_file_count);
        rustfs_io_metrics::record_put_rename_fdatasync_batch(
            rustfs_io_metrics::PUT_RENAME_FDATASYNC_BATCH_MODE_PARALLEL,
            batch_file_count,
        );
        let result = if let Some(disk_permits) = group.disk_permits.upgrade() {
            run_file_sync_blocking(disk_permits, move || sync_files(&batch_files))
                .await
                .map_err(SharedFileFdatasyncError::from_error)
        } else {
            Err(SharedFileFdatasyncError::from_error(io::Error::other(
                "file fdatasync group commit limiter dropped",
            )))
        };
        FILE_FDATASYNC_GROUP_COMMIT.complete_batch(batch.len(), batch_file_count);

        let should_stop = {
            let mut group_state = group.inner.lock();
            if group_state.pending.is_empty() {
                group_state.worker_running = false;
                true
            } else {
                false
            }
        };
        if should_stop {
            FILE_FDATASYNC_GROUP_COMMIT.remove_idle_group(&group);
        }
        for waiter in batch {
            let _ = waiter.result_tx.send(result.clone());
        }
        if should_stop {
            return;
        }
    }
}

async fn sync_files_group_commit(files: Vec<PathBuf>, disk_permits: Arc<Semaphore>) -> io::Result<()> {
    let (result_rx, worker) = FILE_FDATASYNC_GROUP_COMMIT.enqueue(disk_permits, files)?;
    if let Some(group) = worker {
        tokio::spawn(run_file_fdatasync_group_worker(group));
    }

    match result_rx.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => Err(err.into_error()),
        Err(_) => Err(io::Error::other("file fdatasync group worker dropped the waiter")),
    }
}

#[cfg(test)]
pub(crate) fn file_fdatasync_group_commit_counts_for_test() -> (usize, usize, usize) {
    FILE_FDATASYNC_GROUP_COMMIT.counts_for_test()
}

#[cfg(test)]
fn clear_file_fdatasync_group_commit_for_test() {
    FILE_FDATASYNC_GROUP_COMMIT.clear_for_test();
}

// Small object directories are cheaper to flush in one blocking task. Multipart
// directories fan out only once enough files can amortize per-task scheduling.
const PARALLEL_FILE_SYNC_THRESHOLD: usize = 16;
pub(crate) const MAX_PARALLEL_FILE_SYNCS: usize = 16;
// Scale aggregate fan-out for wider nodes while reserving at least half of the
// configured Tokio blocking pool for unrelated filesystem work.
const MIN_GLOBAL_FILE_SYNCS: usize = 64;
const MAX_GLOBAL_FILE_SYNCS: usize = 512;
#[cfg(test)]
const TEST_GLOBAL_FILE_SYNCS: usize = 64;

static FILE_SYNC_PERMITS: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(global_file_sync_limit()));
static DISK_FILE_SYNC_LIMITERS: LazyLock<Mutex<HashMap<PathBuf, Weak<Semaphore>>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// Dedicated tokio runtime for fsync/fdatasync blocking operations. When
/// configured with >1 threads, isolates device-bound fsync from the main
/// blocking pool so reads (pread/stat/open) are not starved. `None` means
/// fall back to the main runtime (zero behavior change).
static FSYNC_RUNTIME: LazyLock<Option<tokio::runtime::Runtime>> = LazyLock::new(|| {
    let threads =
        rustfs_utils::get_env_usize(rustfs_config::ENV_FSYNC_BLOCKING_THREADS, rustfs_config::DEFAULT_FSYNC_BLOCKING_THREADS);
    if threads <= 1 {
        return None;
    }
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder
        .worker_threads(num_cpus::get().min(8))
        .max_blocking_threads(threads)
        .thread_name("rustfs-fsync")
        .thread_stack_size(512 * 1024)
        .enable_all();
    match builder.build() {
        Ok(rt) => {
            tracing::info!(threads, "fsync dedicated blocking pool enabled");
            Some(rt)
        }
        Err(err) => {
            tracing::warn!(%err, "failed to build fsync runtime, falling back to main pool");
            None
        }
    }
});

/// Spawn a blocking task on the fsync-dedicated runtime if configured,
/// otherwise fall back to the main tokio blocking pool.
fn fsync_spawn_blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> tokio::task::JoinHandle<T> {
    match FSYNC_RUNTIME.as_ref() {
        Some(rt) => rt.spawn_blocking(f),
        None => tokio::task::spawn_blocking(f),
    }
}
static DISK_VOLUME_MUTATION_LOCKS: LazyLock<Mutex<HashMap<PathBuf, Weak<RwLock<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
type NamespaceMutationLock = AsyncMutex<()>;
type NamespaceMutationLockRegistry = HashMap<PathBuf, Weak<NamespaceMutationLock>>;
static DISK_NAMESPACE_MUTATION_LOCKS: LazyLock<Mutex<NamespaceMutationLockRegistry>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn default_global_file_sync_limit(cpu_count: usize, max_blocking_threads: usize) -> usize {
    let cpu_scaled = cpu_count
        .max(1)
        .saturating_mul(MAX_PARALLEL_FILE_SYNCS)
        .clamp(MIN_GLOBAL_FILE_SYNCS, MAX_GLOBAL_FILE_SYNCS);
    cpu_scaled.min((max_blocking_threads.max(1) / 2).max(1))
}

#[cfg(not(test))]
fn global_file_sync_limit() -> usize {
    let max_blocking_threads =
        rustfs_utils::get_env_usize(rustfs_config::ENV_MAX_BLOCKING_THREADS, rustfs_config::DEFAULT_MAX_BLOCKING_THREADS);
    default_global_file_sync_limit(num_cpus::get(), max_blocking_threads)
}

#[cfg(test)]
fn global_file_sync_limit() -> usize {
    TEST_GLOBAL_FILE_SYNCS
}

/// Reuse a disk's limiter across reconnects while detached sync calls still hold it.
pub(crate) fn disk_file_sync_limiter(root: &Path) -> Arc<Semaphore> {
    let mut limiters = DISK_FILE_SYNC_LIMITERS.lock();
    limiters.retain(|_, limiter| limiter.strong_count() > 0);
    if let Some(limiter) = limiters.get(root).and_then(Weak::upgrade) {
        return limiter;
    }

    let limiter = Arc::new(Semaphore::new(MAX_PARALLEL_FILE_SYNCS));
    limiters.insert(root.to_path_buf(), Arc::downgrade(&limiter));
    limiter
}

/// Serialize a bucket's local metadata commits with physical bucket removal.
///
/// The key includes the canonical disk root, so independently reconnected
/// [`LocalDisk`](super::local::LocalDisk) instances share the same lock while
/// disconnected disks do not keep the registry alive.
pub(crate) fn disk_volume_mutation_lock(root: &Path, volume: &str) -> Arc<RwLock<()>> {
    let key = root.join(volume);
    let mut locks = DISK_VOLUME_MUTATION_LOCKS.lock();
    locks.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = locks.get(&key).and_then(Weak::upgrade) {
        return lock;
    }

    let lock = Arc::new(RwLock::new(()));
    locks.insert(key, Arc::downgrade(&lock));
    lock
}

fn disk_namespace_mutation_lock(path: &Path) -> Arc<NamespaceMutationLock> {
    let mut locks = DISK_NAMESPACE_MUTATION_LOCKS.lock();
    locks.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = locks.get(path).and_then(Weak::upgrade) {
        return lock;
    }

    let lock = Arc::new(AsyncMutex::new(()));
    locks.insert(path.to_path_buf(), Arc::downgrade(&lock));
    lock
}

/// Keeps a namespace transaction serialized even when its async waiter is
/// cancelled while a blocking filesystem call is still running.
pub(crate) struct NamespaceMutationLease {
    _namespace_guard: OwnedMutexGuard<()>,
    _volume_guard: Option<OwnedRwLockReadGuard<()>>,
    external_guard: Mutex<Option<Arc<dyn Send + Sync>>>,
}

impl NamespaceMutationLease {
    pub(crate) fn attach_external_guard(&self, guard: Arc<dyn Send + Sync>) {
        *self.external_guard.lock() = Some(guard);
    }
}

async fn acquire_namespace_mutation_lease(path: &Path) -> Arc<NamespaceMutationLease> {
    Arc::new(NamespaceMutationLease {
        _namespace_guard: disk_namespace_mutation_lock(path).lock_owned().await,
        _volume_guard: None,
        external_guard: Mutex::new(None),
    })
}

/// Acquire object serialization before the volume read lock. Bucket deletion
/// only acquires the volume write lock, so this order cannot form a lock cycle.
pub(crate) async fn acquire_rename_data_mutation_lease(
    root: &Path,
    volume: &str,
    destination_object: &Path,
) -> Arc<NamespaceMutationLease> {
    let namespace_guard = disk_namespace_mutation_lock(destination_object).lock_owned().await;
    let volume_guard = disk_volume_mutation_lock(root, volume).read_owned().await;
    Arc::new(NamespaceMutationLease {
        _namespace_guard: namespace_guard,
        _volume_guard: Some(volume_guard),
        external_guard: Mutex::new(None),
    })
}

/// Always acquire the per-disk permit before the process-wide permit. Keeping
/// this order uniform prevents one slow disk from reserving global capacity
/// while it waits for its own concurrency slot.
async fn acquire_file_sync_permits(disk_permits: Arc<Semaphore>) -> io::Result<(OwnedSemaphorePermit, SemaphorePermit<'static>)> {
    let wait_started = rustfs_io_metrics::put_stage_timer();
    let disk_permit = disk_permits
        .acquire_owned()
        .await
        .map_err(|_| io::Error::other("disk file sync concurrency limiter closed"))?;
    let global_permit = FILE_SYNC_PERMITS
        .acquire()
        .await
        .map_err(|_| io::Error::other("global file sync concurrency limiter closed"))?;
    rustfs_io_metrics::record_put_object_stage_duration_from(
        rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_FILE_SYNC_PERMIT_WAIT,
        wait_started,
    );
    Ok((disk_permit, global_permit))
}

/// Keep the per-disk permit with the blocking syscall so cancellation cannot
/// amplify work on a wedged disk. The global permit stays with the async waiter,
/// allowing healthy disks to make progress after a timed-out request is dropped.
async fn run_file_sync_blocking<T, F>(disk_permits: Arc<Semaphore>, work: F) -> io::Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> io::Result<T> + Send + 'static,
{
    let (disk_permit, global_permit) = acquire_file_sync_permits(disk_permits).await?;
    let result = fsync_spawn_blocking(move || {
        let _disk_permit = disk_permit;
        work()
    })
    .await;
    drop(global_permit);
    result?
}

#[cfg(test)]
pub(crate) mod file_sync_probe {
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Condvar, Mutex, RwLock};
    use std::time::Duration;
    use tokio::sync::Notify;
    use tokio::time::timeout;

    static ROOTS: RwLock<Vec<PathBuf>> = RwLock::new(Vec::new());
    static BLOCK_MUTEX: Mutex<()> = Mutex::new(());
    static BLOCK_CONDVAR: Condvar = Condvar::new();
    static ACTIVE_CHANGED: Notify = Notify::const_new();
    static ACTIVE: AtomicUsize = AtomicUsize::new(0);
    static PEAK: AtomicUsize = AtomicUsize::new(0);
    static ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
    static FAIL_ON_ATTEMPT: AtomicUsize = AtomicUsize::new(usize::MAX);
    static BLOCK: AtomicBool = AtomicBool::new(false);
    static GROUP_BATCHES: Mutex<Vec<usize>> = Mutex::new(Vec::new());
    static BEFORE_GROUP_BATCH: Mutex<Option<Box<dyn FnOnce() + Send>>> = Mutex::new(None);
    const WAIT_TIMEOUT: Duration = Duration::from_secs(30);

    pub(crate) struct ProbeGuard;

    pub(super) struct ActiveGuard {
        fail: bool,
    }

    impl ActiveGuard {
        pub(super) fn should_fail(&self) -> bool {
            self.fail
        }
    }

    impl Drop for ActiveGuard {
        fn drop(&mut self) {
            ACTIVE.fetch_sub(1, Ordering::SeqCst);
            ACTIVE_CHANGED.notify_waiters();
        }
    }

    impl Drop for ProbeGuard {
        fn drop(&mut self) {
            release();
            FAIL_ON_ATTEMPT.store(usize::MAX, Ordering::SeqCst);
            GROUP_BATCHES.lock().expect("file sync group batch recorder poisoned").clear();
            BEFORE_GROUP_BATCH.lock().expect("file sync group batch hook poisoned").take();
            ROOTS.write().expect("file sync probe lock poisoned").clear();
        }
    }

    fn configure(root: &Path, fail_on_attempt: Option<usize>, block: bool) -> ProbeGuard {
        ACTIVE.store(0, Ordering::SeqCst);
        PEAK.store(0, Ordering::SeqCst);
        ATTEMPTS.store(0, Ordering::SeqCst);
        FAIL_ON_ATTEMPT.store(fail_on_attempt.unwrap_or(usize::MAX), Ordering::SeqCst);
        GROUP_BATCHES.lock().expect("file sync group batch recorder poisoned").clear();
        BEFORE_GROUP_BATCH.lock().expect("file sync group batch hook poisoned").take();
        {
            let _guard = BLOCK_MUTEX.lock().expect("file sync probe blocker poisoned");
            BLOCK.store(block, Ordering::SeqCst);
        }
        let mut roots = vec![root.to_path_buf()];
        if let Ok(canonical) = root.canonicalize()
            && canonical != root
        {
            roots.push(canonical);
        }
        *ROOTS.write().expect("file sync probe lock poisoned") = roots;
        ProbeGuard
    }

    pub(super) fn set(root: &Path) -> ProbeGuard {
        configure(root, None, false)
    }

    pub(super) fn set_failing(root: &Path) -> ProbeGuard {
        configure(root, Some(1), false)
    }

    pub(super) fn set_failing_blocking(root: &Path) -> ProbeGuard {
        configure(root, Some(1), true)
    }

    pub(crate) fn set_blocking(root: &Path) -> ProbeGuard {
        configure(root, None, true)
    }

    pub(super) fn enter(path: &Path) -> Option<ActiveGuard> {
        let canonical_path = path.canonicalize().ok();
        let enabled =
            ROOTS.read().expect("file sync probe lock poisoned").iter().any(|root| {
                path.starts_with(root) || canonical_path.as_ref().is_some_and(|canonical| canonical.starts_with(root))
            });
        if !enabled {
            return None;
        }

        let attempt = ATTEMPTS.fetch_add(1, Ordering::SeqCst) + 1;
        let active = ACTIVE.fetch_add(1, Ordering::SeqCst) + 1;
        PEAK.fetch_max(active, Ordering::SeqCst);
        ACTIVE_CHANGED.notify_waiters();
        let fail = attempt == FAIL_ON_ATTEMPT.load(Ordering::SeqCst);
        if !fail {
            let guard = BLOCK_MUTEX.lock().expect("file sync probe blocker poisoned");
            drop(
                BLOCK_CONDVAR
                    .wait_while(guard, |_| BLOCK.load(Ordering::SeqCst))
                    .expect("file sync probe blocker poisoned"),
            );
        }
        Some(ActiveGuard { fail })
    }

    pub(crate) fn peak() -> usize {
        PEAK.load(Ordering::SeqCst)
    }

    pub(super) fn attempts() -> usize {
        ATTEMPTS.load(Ordering::SeqCst)
    }

    pub(crate) async fn wait_for_active(target: usize) {
        timeout(WAIT_TIMEOUT, async {
            loop {
                let changed = ACTIVE_CHANGED.notified();
                if ACTIVE.load(Ordering::SeqCst) >= target {
                    return;
                }
                changed.await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "timed out waiting for {target} active file sync probes; active={}, peak={}, attempts={}",
                ACTIVE.load(Ordering::SeqCst),
                PEAK.load(Ordering::SeqCst),
                ATTEMPTS.load(Ordering::SeqCst)
            )
        });
    }

    pub(super) async fn wait_for_idle() {
        timeout(WAIT_TIMEOUT, async {
            loop {
                let changed = ACTIVE_CHANGED.notified();
                if ACTIVE.load(Ordering::SeqCst) == 0 {
                    return;
                }
                changed.await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "timed out waiting for file sync probes to become idle; active={}, peak={}, attempts={}",
                ACTIVE.load(Ordering::SeqCst),
                PEAK.load(Ordering::SeqCst),
                ATTEMPTS.load(Ordering::SeqCst)
            )
        });
    }

    pub(crate) fn release() {
        let _guard = BLOCK_MUTEX.lock().expect("file sync probe blocker poisoned");
        BLOCK.store(false, Ordering::SeqCst);
        BLOCK_CONDVAR.notify_all();
    }

    pub(super) fn record_group_batch(batch_len: usize) {
        GROUP_BATCHES
            .lock()
            .expect("file sync group batch recorder poisoned")
            .push(batch_len);
    }

    pub(crate) fn group_batches() -> Vec<usize> {
        GROUP_BATCHES.lock().expect("file sync group batch recorder poisoned").clone()
    }

    pub(crate) fn set_before_group_batch(hook: impl FnOnce() + Send + 'static) {
        *BEFORE_GROUP_BATCH.lock().expect("file sync group batch hook poisoned") = Some(Box::new(hook));
    }

    pub(super) fn run_before_group_batch() {
        if let Some(hook) = BEFORE_GROUP_BATCH.lock().expect("file sync group batch hook poisoned").take() {
            hook();
        }
    }
}

pub(crate) fn sync_file(path: &Path) -> io::Result<()> {
    #[cfg(test)]
    let _probe = file_sync_probe::enter(path);
    #[cfg(test)]
    if _probe.as_ref().is_some_and(file_sync_probe::ActiveGuard::should_fail) {
        return Err(io::Error::other("injected file sync failure"));
    }
    #[cfg(windows)]
    let file = std::fs::OpenOptions::new().write(true).open(path)?;
    #[cfg(not(windows))]
    let file = std::fs::File::open(path)?;
    file.sync_data()
}

fn sync_file_with_put_stage_metric(path: &Path) -> io::Result<()> {
    let sync_started = rustfs_io_metrics::put_stage_timer();
    let result = sync_file(path);
    rustfs_io_metrics::record_put_object_stage_duration_from(
        rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_FILE_FDATASYNC,
        sync_started,
    );
    result
}

fn sync_files(paths: &[PathBuf]) -> io::Result<()> {
    for path in paths {
        sync_file_with_put_stage_metric(path)?;
    }
    Ok(())
}

fn regular_files(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut files = Vec::with_capacity(PARALLEL_FILE_SYNC_THRESHOLD);
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            files.push(entry.path());
        }
    }
    Ok(files)
}

/// Fdatasync every regular file directly inside `dir`, then fsync the directory
/// itself.
#[allow(
    dead_code,
    reason = "reached only through sync_dir_files, whose callers are tests (backlog#1823)"
)]
pub fn sync_dir_files_std(dir: impl AsRef<Path>) -> io::Result<()> {
    for entry in std::fs::read_dir(dir.as_ref())? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            sync_file(&entry.path())?;
        }
    }
    fsync_dir_std(dir)
}

/// Async wrapper around [`sync_dir_files_std`]. Large directories flush files
/// concurrently, bounded both per directory and process-wide.
#[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
pub async fn sync_dir_files(dir: impl AsRef<Path>) -> io::Result<()> {
    sync_dir_files_with_limiter(dir, Arc::new(Semaphore::new(MAX_PARALLEL_FILE_SYNCS))).await
}

pub(crate) async fn sync_dir_files_with_limiter(dir: impl AsRef<Path>, disk_permits: Arc<Semaphore>) -> io::Result<()> {
    let dir = dir.as_ref().to_path_buf();
    let scan_dir = dir.clone();
    let group_file_fdatasync = file_fdatasync_group_commit_enabled();
    let files = run_file_sync_blocking(disk_permits.clone(), move || {
        let files = regular_files(&scan_dir)?;
        if files.len() < PARALLEL_FILE_SYNC_THRESHOLD {
            if group_file_fdatasync && !files.is_empty() {
                return Ok(Some(files));
            }
            rustfs_io_metrics::record_put_rename_fdatasync_batch(
                rustfs_io_metrics::PUT_RENAME_FDATASYNC_BATCH_MODE_SERIAL,
                files.len(),
            );
            sync_files(&files)?;
            let fsync_started = rustfs_io_metrics::put_stage_timer();
            let result = fsync_dir_std(scan_dir);
            rustfs_io_metrics::record_put_object_stage_duration_from(
                rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_SRC_DIR_FSYNC,
                fsync_started,
            );
            result?;
            return Ok(None);
        }
        Ok::<_, io::Error>(Some(files))
    })
    .await?;

    let Some(files) = files else {
        return Ok(());
    };
    if group_file_fdatasync && files.len() < PARALLEL_FILE_SYNC_THRESHOLD {
        sync_files_group_commit(files, disk_permits.clone()).await?;
        return run_file_sync_blocking(disk_permits, move || {
            let fsync_started = rustfs_io_metrics::put_stage_timer();
            let result = fsync_dir_std(dir);
            rustfs_io_metrics::record_put_object_stage_duration_from(
                rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_SRC_DIR_FSYNC,
                fsync_started,
            );
            result
        })
        .await;
    }
    rustfs_io_metrics::record_put_rename_fdatasync_batch(
        rustfs_io_metrics::PUT_RENAME_FDATASYNC_BATCH_MODE_PARALLEL,
        files.len(),
    );
    futures::stream::iter(files.into_iter().map(Ok::<_, io::Error>))
        .try_for_each_concurrent(MAX_PARALLEL_FILE_SYNCS, |path| {
            let disk_permits = disk_permits.clone();
            async move { run_file_sync_blocking(disk_permits, move || sync_file_with_put_stage_metric(&path)).await }
        })
        .await?;
    run_file_sync_blocking(disk_permits, move || {
        let fsync_started = rustfs_io_metrics::put_stage_timer();
        let result = fsync_dir_std(dir);
        rustfs_io_metrics::record_put_object_stage_duration_from(
            rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_SRC_DIR_FSYNC,
            fsync_started,
        );
        result
    })
    .await
}

/// Check if the given disk path is the root disk.
/// On Windows, always return false.
/// On Unix, compare the disk paths.
#[tracing::instrument(level = "debug", skip_all)]
pub fn is_root_disk(disk_path: &str, root_disk: &str) -> Result<bool> {
    if cfg!(target_os = "windows") {
        return Ok(false);
    }

    rustfs_utils::os::same_disk(disk_path, root_disk).map_err(|e| to_file_error(e).into())
}

/// Create a directory and all its parent components if they are missing.
#[tracing::instrument(level = "debug", skip_all)]
pub async fn make_dir_all(path: impl AsRef<Path>, base_dir: impl AsRef<Path>) -> Result<()> {
    check_path_length(path.as_ref().to_string_lossy().to_string().as_str())?;

    reliable_mkdir_all(path.as_ref(), base_dir.as_ref())
        .await
        .map_err(to_file_error)?;

    Ok(())
}

/// Check if a directory is empty.
/// Only reads one entry to determine if the directory is empty.
#[tracing::instrument(level = "debug", skip_all)]
pub async fn is_empty_dir(path: impl AsRef<Path>) -> bool {
    read_dir(path.as_ref(), 1).await.is_ok_and(|v| v.is_empty())
}

const READ_DIR_PROBE_RAW_LIMIT: usize = 256;

pub(crate) struct ReadDirProbe {
    pub entries: Vec<String>,
    pub complete: bool,
}

pub(crate) fn read_dir_probe(path: impl AsRef<Path>, entry_limit: usize) -> io::Result<ReadDirProbe> {
    let mut dir = std::fs::read_dir(path)?;
    let mut entries = Vec::with_capacity(entry_limit.min(READ_DIR_PROBE_RAW_LIMIT));
    for _ in 0..READ_DIR_PROBE_RAW_LIMIT {
        let Some(entry) = dir.next() else {
            return Ok(ReadDirProbe { entries, complete: true });
        };
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name.is_empty() || name == "." || name == ".." {
            continue;
        }

        let file_type = entry.file_type()?;
        if file_type.is_file() {
            entries.push(name);
        } else if file_type.is_dir() {
            entries.push(format!("{name}{SLASH_SEPARATOR}"));
        } else {
            continue;
        }

        if entries.len() == entry_limit {
            return Ok(ReadDirProbe {
                entries,
                complete: false,
            });
        }
    }

    Ok(ReadDirProbe {
        entries,
        complete: false,
    })
}

// read_dir  count read limit. when count == 0 unlimit.
/// Return file names in the directory.
#[tracing::instrument(level = "debug", skip_all)]
pub async fn read_dir(path: impl AsRef<Path>, count: i32) -> std::io::Result<Vec<String>> {
    let mut entries = fs::read_dir(path.as_ref()).await?;

    let mut volumes = Vec::new();

    let mut count = count;

    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name().to_string_lossy().to_string();

        if name.is_empty() || name == "." || name == ".." {
            continue;
        }

        let file_type = entry.file_type().await?;

        if file_type.is_file() {
            volumes.push(name);
        } else if file_type.is_dir() {
            volumes.push(format!("{name}{SLASH_SEPARATOR}"));
        } else {
            // Entries we don't return (symlinks, sockets, fifos) must not consume
            // the limit: is_empty_dir/list_dir(count=1) would misreport otherwise.
            continue;
        }
        count -= 1;
        if count == 0 {
            break;
        }
    }

    Ok(volumes)
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn rename_all(
    src_file_path: impl AsRef<Path>,
    dst_file_path: impl AsRef<Path>,
    base_dir: impl AsRef<Path>,
    publication_root: &PublicationRoot,
) -> Result<()> {
    reliable_rename(src_file_path, dst_file_path.as_ref(), base_dir, publication_root)
        .await
        .map_err(to_file_error)?;

    Ok(())
}

pub(crate) async fn rename_all_with_lease(
    src_file_path: impl AsRef<Path>,
    dst_file_path: impl AsRef<Path>,
    base_dir: impl AsRef<Path>,
    publication_root: &PublicationRoot,
    lease: Arc<NamespaceMutationLease>,
) -> Result<()> {
    reliable_rename_inner_with_lease(
        src_file_path.as_ref().to_path_buf(),
        dst_file_path.as_ref().to_path_buf(),
        base_dir.as_ref().to_path_buf(),
        publication_root.clone(),
        true,
        lease,
    )
    .await
    .map_err(to_file_error)?;
    Ok(())
}

#[cfg(windows)]
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) async fn rename_all_with_commit_guard(
    src_file_path: impl AsRef<Path>,
    dst_file_path: impl AsRef<Path>,
    base_dir: impl AsRef<Path>,
    _publication_root: &PublicationRoot,
    commit_guard: &RenameCommitGuard,
    lease: Arc<NamespaceMutationLease>,
) -> Result<()> {
    let src_file_path = src_file_path.as_ref().to_path_buf();
    let dst_file_path = dst_file_path.as_ref().to_path_buf();
    let base_dir = base_dir.as_ref().to_path_buf();
    let commit_guard = commit_guard.clone();
    let operation = {
        let src_file_path = src_file_path.clone();
        let dst_file_path = dst_file_path.clone();
        move || rename_with_commit_guard_std(&src_file_path, &dst_file_path, &commit_guard)
    };
    let result = run_blocking_namespace_operation(lease, operation).await;
    if let Err(err) = &result {
        warn_reliable_rename_failure(&src_file_path, &dst_file_path, &base_dir, err);
    }
    result.map_err(to_file_error)?;
    Ok(())
}

pub(crate) struct PreparedRenameSource {
    path: PathBuf,
    #[cfg(windows)]
    source: winapi_util::Handle,
    #[cfg(not(windows))]
    source: std::fs::File,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

impl PreparedRenameSource {
    pub(crate) fn write_all(&mut self, data: &[u8], sync: bool) -> io::Result<()> {
        #[cfg(all(test, windows))]
        windows_rename_test_hooks::run_before_source_write(&self.path);
        #[cfg(windows)]
        std::io::Write::write_all(self.source.as_file_mut(), data)?;
        #[cfg(not(windows))]
        std::io::Write::write_all(&mut self.source, data)?;
        if sync {
            #[cfg(windows)]
            self.source.as_file().sync_data()?;
            #[cfg(not(windows))]
            self.source.sync_data()?;
        }
        Ok(())
    }
}

pub(crate) fn create_prepared_rename_source_with_commit_guard(
    src_file_path: &Path,
    dst_file_path: &Path,
    commit_guard: &RenameCommitGuard,
) -> io::Result<PreparedRenameSource> {
    #[cfg(windows)]
    {
        if src_file_path.parent() != Some(commit_guard.source_parent.as_path()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "rename source parent does not match its commit guard",
            ));
        }
        if dst_file_path.parent() != Some(commit_guard.destination_parent.as_path()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "rename destination parent does not match its commit guard",
            ));
        }
        let source_name = src_file_path
            .file_name()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "rename source must have a file name"))?;
        let source = create_windows_superseding_file(commit_guard.source_parent_guard.last_handle()?, source_name)?;
        return Ok(PreparedRenameSource {
            path: src_file_path.to_path_buf(),
            source,
        });
    }

    #[cfg(not(windows))]
    {
        let _ = (dst_file_path, commit_guard);
        let source = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(src_file_path)?;
        #[cfg(unix)]
        let metadata = source.metadata()?;
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;
        Ok(PreparedRenameSource {
            path: src_file_path.to_path_buf(),
            source,
            #[cfg(unix)]
            device: metadata.dev(),
            #[cfg(unix)]
            inode: metadata.ino(),
        })
    }
}

#[cfg(windows)]
pub(crate) fn read_destination_file_with_commit_guard(
    file_path: &Path,
    commit_guard: &RenameCommitGuard,
) -> io::Result<Option<Vec<u8>>> {
    if file_path.parent() != Some(commit_guard.destination_parent.as_path()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "destination file parent does not match its commit guard",
        ));
    }
    read_windows_relative_file(file_path, &commit_guard.destination_parent_guard)
}

#[cfg(windows)]
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) async fn rename_all_with_prepared_source(
    prepared_source: PreparedRenameSource,
    src_file_path: impl AsRef<Path>,
    dst_file_path: impl AsRef<Path>,
    base_dir: impl AsRef<Path>,
    _publication_root: &PublicationRoot,
    commit_guard: &RenameCommitGuard,
    lease: Arc<NamespaceMutationLease>,
) -> Result<()> {
    let src_file_path = src_file_path.as_ref().to_path_buf();
    let dst_file_path = dst_file_path.as_ref().to_path_buf();
    let base_dir = base_dir.as_ref().to_path_buf();
    let commit_guard = commit_guard.clone();
    let operation = {
        let src_file_path = src_file_path.clone();
        let dst_file_path = dst_file_path.clone();
        move || rename_prepared_source_with_commit_guard_std(&prepared_source, &src_file_path, &dst_file_path, &commit_guard)
    };
    let result = run_blocking_namespace_operation(lease, operation).await;
    if let Err(err) = &result {
        warn_reliable_rename_failure(&src_file_path, &dst_file_path, &base_dir, err);
    }
    result.map_err(to_file_error)?;
    Ok(())
}

#[cfg(not(windows))]
pub(crate) async fn rename_all_with_prepared_source(
    prepared_source: PreparedRenameSource,
    src_file_path: impl AsRef<Path>,
    dst_file_path: impl AsRef<Path>,
    base_dir: impl AsRef<Path>,
    publication_root: &PublicationRoot,
    _commit_guard: &RenameCommitGuard,
    lease: Arc<NamespaceMutationLease>,
) -> Result<()> {
    let src_file_path = src_file_path.as_ref().to_path_buf();
    let dst_file_path = dst_file_path.as_ref().to_path_buf();
    let base_dir = base_dir.as_ref().to_path_buf();
    let publication_root = publication_root.clone();
    let operation = {
        let src_file_path = src_file_path.clone();
        let dst_file_path = dst_file_path.clone();
        let base_dir = base_dir.clone();
        move || {
            validate_prepared_rename_source(&prepared_source, &src_file_path)?;
            let (preparation, attempt) = prepare_rename_with_retry(&src_file_path, &dst_file_path, &base_dir, &publication_root)?;
            rename_prepared(&src_file_path, &dst_file_path, &preparation, attempt)
        }
    };
    let result = run_blocking_namespace_operation(lease, operation).await;
    if let Err(err) = &result {
        warn_reliable_rename_failure(&src_file_path, &dst_file_path, &base_dir, err);
    }
    result.map_err(to_file_error)?;
    Ok(())
}

#[cfg(not(windows))]
pub(crate) async fn rename_all_with_commit_guard(
    src_file_path: impl AsRef<Path>,
    dst_file_path: impl AsRef<Path>,
    base_dir: impl AsRef<Path>,
    publication_root: &PublicationRoot,
    _commit_guard: &RenameCommitGuard,
    lease: Arc<NamespaceMutationLease>,
) -> Result<()> {
    rename_all_with_lease(src_file_path, dst_file_path, base_dir, publication_root, lease).await
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn rename_all_ignore_missing_source(
    src_file_path: impl AsRef<Path>,
    dst_file_path: impl AsRef<Path>,
    base_dir: impl AsRef<Path>,
    publication_root: &PublicationRoot,
) -> Result<()> {
    let src_file_path = src_file_path.as_ref();
    match reliable_rename_inner(src_file_path, dst_file_path.as_ref(), base_dir, publication_root, false).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound && rename_source_is_missing(src_file_path, publication_root) => Ok(()),
        Err(err) => Err(to_file_error(err).into()),
    }
}

#[cfg(windows)]
pub(crate) fn rename_source_is_missing(src_file_path: &Path, publication_root: &PublicationRoot) -> bool {
    let Some(source_parent) = src_file_path.parent() else {
        return false;
    };
    let source_parent_guard = match lock_windows_directory_tree(source_parent, None, publication_root) {
        Ok(guard) => guard,
        Err(err) => return err.kind() == io::ErrorKind::NotFound,
    };
    match open_windows_rename_source_identity(src_file_path, &source_parent_guard) {
        Ok(_) => false,
        Err(err) => err.kind() == io::ErrorKind::NotFound,
    }
}

#[cfg(not(windows))]
pub(crate) fn rename_source_is_missing(src_file_path: &Path, _publication_root: &PublicationRoot) -> bool {
    matches!(std::fs::symlink_metadata(src_file_path), Err(err) if err.kind() == io::ErrorKind::NotFound)
}

async fn reliable_rename(
    src_file_path: impl AsRef<Path>,
    dst_file_path: impl AsRef<Path>,
    base_dir: impl AsRef<Path>,
    publication_root: &PublicationRoot,
) -> io::Result<()> {
    reliable_rename_inner(src_file_path, dst_file_path, base_dir, publication_root, true).await
}

async fn reliable_rename_inner(
    src_file_path: impl AsRef<Path>,
    dst_file_path: impl AsRef<Path>,
    base_dir: impl AsRef<Path>,
    publication_root: &PublicationRoot,
    warn_on_missing_source: bool,
) -> io::Result<()> {
    let src_file_path = src_file_path.as_ref().to_path_buf();
    let dst_file_path = dst_file_path.as_ref().to_path_buf();
    let base_dir = base_dir.as_ref().to_path_buf();
    let lease = acquire_namespace_mutation_lease(&dst_file_path).await;
    reliable_rename_inner_with_lease(
        src_file_path,
        dst_file_path,
        base_dir,
        publication_root.clone(),
        warn_on_missing_source,
        lease,
    )
    .await
}

async fn reliable_rename_inner_with_lease(
    src_file_path: PathBuf,
    dst_file_path: PathBuf,
    base_dir: PathBuf,
    publication_root: PublicationRoot,
    warn_on_missing_source: bool,
    lease: Arc<NamespaceMutationLease>,
) -> io::Result<()> {
    let operation = {
        let src_file_path = src_file_path.clone();
        let dst_file_path = dst_file_path.clone();
        let base_dir = base_dir.clone();
        move || {
            let (preparation, attempt) = prepare_rename_with_retry(&src_file_path, &dst_file_path, &base_dir, &publication_root)?;
            rename_prepared(&src_file_path, &dst_file_path, &preparation, attempt)
        }
    };
    let result = run_blocking_namespace_operation(lease, operation).await;
    if let Err(err) = &result
        && (warn_on_missing_source || err.kind() != io::ErrorKind::NotFound)
    {
        warn_reliable_rename_failure(&src_file_path, &dst_file_path, &base_dir, err);
    }
    result
}

#[cfg(not(windows))]
fn validate_prepared_rename_source(prepared_source: &PreparedRenameSource, src_file_path: &Path) -> io::Result<()> {
    if prepared_source.path != src_file_path {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "prepared rename source does not match the requested source path",
        ));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        let metadata = std::fs::symlink_metadata(src_file_path)?;
        if metadata.dev() != prepared_source.device || metadata.ino() != prepared_source.inode {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "rename source identity changed while publication was prepared",
            ));
        }
    }

    Ok(())
}

#[cfg(windows)]
fn rename_with_commit_guard_std(src_file_path: &Path, dst_file_path: &Path, commit_guard: &RenameCommitGuard) -> io::Result<()> {
    let prepared_source = PreparedRenameSource {
        path: src_file_path.to_path_buf(),
        source: prepare_windows_rename_source(src_file_path, dst_file_path, commit_guard)?,
    };
    rename_prepared_source_with_commit_guard_std(&prepared_source, src_file_path, dst_file_path, commit_guard)
}

#[cfg(windows)]
fn prepare_windows_rename_source(
    src_file_path: &Path,
    _dst_file_path: &Path,
    commit_guard: &RenameCommitGuard,
) -> io::Result<winapi_util::Handle> {
    if src_file_path.parent() != Some(commit_guard.source_parent.as_path()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "rename source parent does not match its commit guard",
        ));
    }
    let (source_identity_anchor, expected_source_identity) =
        open_windows_rename_source_identity(src_file_path, &commit_guard.source_parent_guard)?;
    let mut attempt = 0;
    let source = loop {
        match open_windows_rename_source(src_file_path, &commit_guard.source_parent_guard) {
            Ok(source) => break source,
            Err(err) if should_retry_rename(&err, attempt) => {
                #[cfg(test)]
                windows_rename_test_hooks::run_before_rename_retry(_dst_file_path);
                attempt += 1;
            }
            Err(err) => return Err(err),
        }
    };
    if windows_file_identity(&source)? != expected_source_identity {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "rename source identity changed while publication was prepared",
        ));
    }
    drop(source_identity_anchor);

    Ok(source)
}

#[cfg(windows)]
fn rename_prepared_source_with_commit_guard_std(
    prepared_source: &PreparedRenameSource,
    src_file_path: &Path,
    dst_file_path: &Path,
    commit_guard: &RenameCommitGuard,
) -> io::Result<()> {
    if prepared_source.path != src_file_path {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "prepared rename source does not match the requested source path",
        ));
    }
    if src_file_path.parent() != Some(commit_guard.source_parent.as_path()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "rename source parent does not match its commit guard",
        ));
    }
    if dst_file_path.parent() != Some(commit_guard.destination_parent.as_path()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "rename destination parent does not match its commit guard",
        ));
    }

    rename_windows_prepared(dst_file_path, &commit_guard.destination_parent_guard, &prepared_source.source, 0)
}

/// Run a blocking namespace operation without making its async waiter
/// uncancellable. The owned lease moves into the closure, so a timed-out task
/// cannot release transaction serialization before the syscall returns.
pub(crate) async fn run_blocking_namespace_operation<T: Send + 'static>(
    lease: Arc<NamespaceMutationLease>,
    operation: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> io::Result<T> {
    tokio::task::spawn_blocking(move || {
        let _lease = lease;
        operation()
    })
    .await
    .map_err(|err| io::Error::other(format!("blocking namespace operation failed: {err}")))?
}

/// Admit one strict inline commit under the disk sync limit. The caller already
/// owns the namespace lease, establishing namespace -> disk ordering. Holding
/// admission across adjacent durability barriers prevents one transaction from
/// repeatedly joining the disk semaphore tail.
pub(crate) struct FileSyncAdmission {
    disk_permit: Arc<OwnedSemaphorePermit>,
}

pub(crate) async fn acquire_file_sync_admission(disk_permits: Arc<Semaphore>) -> io::Result<FileSyncAdmission> {
    let wait_started = rustfs_io_metrics::put_stage_timer();
    let disk_permit = disk_permits
        .acquire_owned()
        .await
        .map_err(|_| io::Error::other("disk file sync concurrency limiter closed"))?;
    rustfs_io_metrics::record_put_object_stage_duration_from(
        rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_FILE_SYNC_PERMIT_WAIT,
        wait_started,
    );
    Ok(FileSyncAdmission {
        disk_permit: Arc::new(disk_permit),
    })
}

/// Keep the disk admission and namespace lease with the blocking syscall if
/// the async waiter is cancelled. The process-wide admission remains with the
/// waiter so cancellation cannot starve healthy disks.
pub(crate) async fn run_blocking_namespace_file_sync_operation<T: Send + 'static>(
    lease: Arc<NamespaceMutationLease>,
    admission: &FileSyncAdmission,
    operation: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> io::Result<T> {
    run_blocking_namespace_file_sync_operation_with_global(lease, admission, &FILE_SYNC_PERMITS, operation).await
}

async fn run_blocking_namespace_file_sync_operation_with_global<T: Send + 'static>(
    lease: Arc<NamespaceMutationLease>,
    admission: &FileSyncAdmission,
    global_permits: &Semaphore,
    operation: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> io::Result<T> {
    let wait_started = rustfs_io_metrics::put_stage_timer();
    let global_permit = global_permits
        .acquire()
        .await
        .map_err(|_| io::Error::other("global file sync concurrency limiter closed"))?;
    rustfs_io_metrics::record_put_object_stage_duration_from(
        rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_GLOBAL_FILE_SYNC_PERMIT_WAIT,
        wait_started,
    );
    let disk_permit = admission.disk_permit.clone();
    let result = fsync_spawn_blocking(move || {
        let _lease = lease;
        let _disk_permit = disk_permit;
        operation()
    })
    .await;
    drop(global_permit);
    result.map_err(|err| io::Error::other(format!("blocking namespace file sync operation failed: {err}")))?
}

pub(crate) async fn fsync_dir_with_namespace_file_sync_limit(
    dir: impl AsRef<Path>,
    lease: Arc<NamespaceMutationLease>,
    admission: &FileSyncAdmission,
) -> io::Result<()> {
    #[cfg(unix)]
    {
        let dir = dir.as_ref().to_path_buf();
        run_blocking_namespace_file_sync_operation(lease, admission, move || {
            #[cfg(test)]
            fsync_dir_recorder::record_limited(&dir);
            fsync_dir_std(dir)
        })
        .await
    }

    #[cfg(not(unix))]
    {
        let _ = (lease, admission);
        fsync_dir_std(dir)
    }
}

struct RenamePreparation {
    parent_guard: Option<ExistingBaseDirectoryGuard>,
    #[cfg(windows)]
    _source_parent_guard: ExistingBaseDirectoryGuard,
    #[cfg(windows)]
    source: winapi_util::Handle,
}

#[cfg(not(windows))]
fn prepare_rename_with_retry(
    src_file_path: &Path,
    dst_file_path: &Path,
    base_dir: &Path,
    publication_root: &PublicationRoot,
) -> io::Result<(RenamePreparation, usize)> {
    let mut attempt = 0;
    loop {
        match prepare_rename(src_file_path, dst_file_path, base_dir, publication_root) {
            Ok(preparation) => return Ok((preparation, attempt)),
            Err(err) if should_retry_rename(&err, attempt) => {
                attempt += 1;
            }
            Err(err) => return Err(err),
        }
    }
}

#[cfg(windows)]
fn prepare_rename_with_retry(
    src_file_path: &Path,
    dst_file_path: &Path,
    base_dir: &Path,
    publication_root: &PublicationRoot,
) -> io::Result<(RenamePreparation, usize)> {
    let source_parent = src_file_path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "rename source must have a parent directory"))?;
    let destination_parent = dst_file_path.parent();
    let mut attempt = 0;
    let prepare_destination_parent = |attempt: &mut usize| -> io::Result<Option<ExistingBaseDirectoryGuard>> {
        loop {
            let result = destination_parent
                .map(|parent| mkdir_all_below_existing_base_std(parent, base_dir, publication_root))
                .transpose();
            match result {
                Ok(parent_guard) => break Ok(parent_guard),
                Err(err) if should_retry_rename(&err, *attempt) => {
                    #[cfg(test)]
                    windows_rename_test_hooks::run_before_rename_retry(dst_file_path);
                    *attempt += 1;
                }
                Err(err) => break Err(err),
            }
        }
    };
    let same_parent = match destination_parent {
        Some(destination_parent) => {
            publication_root.relative_path(source_parent)? == publication_root.relative_path(destination_parent)?
        }
        None => false,
    };
    let (source_parent_guard, parent_guard, source_identity_anchor, expected_source_identity) = if same_parent {
        let parent_guard = prepare_destination_parent(&mut attempt)?;
        let source_parent_guard = parent_guard
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "rename destination must have a parent directory"))?
            .clone();
        let (source_identity_anchor, expected_source_identity) =
            open_windows_rename_source_identity(src_file_path, &source_parent_guard)?;
        (source_parent_guard, parent_guard, source_identity_anchor, expected_source_identity)
    } else {
        let source_parent_guard = lock_windows_directory_tree(source_parent, destination_parent, publication_root)?;
        let (source_identity_anchor, expected_source_identity) =
            open_windows_rename_source_identity(src_file_path, &source_parent_guard)?;
        let parent_guard = prepare_destination_parent(&mut attempt)?;
        (source_parent_guard, parent_guard, source_identity_anchor, expected_source_identity)
    };
    let source = loop {
        match open_windows_rename_source(src_file_path, &source_parent_guard) {
            Ok(source) => break source,
            Err(err) if should_retry_rename(&err, attempt) => {
                #[cfg(test)]
                windows_rename_test_hooks::run_before_rename_retry(dst_file_path);
                attempt += 1;
            }
            Err(err) => return Err(err),
        }
    };
    if windows_file_identity(&source)? != expected_source_identity {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "rename source identity changed while publication was prepared",
        ));
    }
    drop(source_identity_anchor);

    Ok((
        RenamePreparation {
            parent_guard,
            _source_parent_guard: source_parent_guard,
            source,
        },
        attempt,
    ))
}

#[cfg(not(windows))]
fn prepare_rename(
    _src_file_path: &Path,
    dst_file_path: &Path,
    base_dir: &Path,
    publication_root: &PublicationRoot,
) -> io::Result<RenamePreparation> {
    let parent_guard = dst_file_path
        .parent()
        .map(|parent| mkdir_all_below_existing_base_std(parent, base_dir, publication_root))
        .transpose()?;
    Ok(RenamePreparation { parent_guard })
}

fn rename_prepared(
    _src_file_path: &Path,
    dst_file_path: &Path,
    preparation: &RenamePreparation,
    attempt: usize,
) -> io::Result<()> {
    #[cfg(windows)]
    {
        let parent_guard = preparation
            .parent_guard
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "rename destination must have a parent directory"))?;
        rename_windows_prepared(dst_file_path, parent_guard, &preparation.source, attempt)
    }

    #[cfg(not(windows))]
    {
        let mut attempt = attempt;
        loop {
            let rename_result = rename_into_existing_parent(_src_file_path, dst_file_path, preparation.parent_guard.as_ref());
            match rename_result {
                Ok(()) => return Ok(()),
                Err(err) if should_retry_rename(&err, attempt) => {
                    attempt += 1;
                }
                Err(err) => return Err(err),
            }
        }
    }
}

#[cfg(windows)]
fn rename_windows_prepared(
    dst_file_path: &Path,
    parent_guard: &ExistingBaseDirectoryGuard,
    source: &winapi_util::Handle,
    mut attempt: usize,
) -> io::Result<()> {
    loop {
        #[cfg(test)]
        windows_rename_test_hooks::record_guard_generation(dst_file_path, parent_guard.generation);
        match rename_into_existing_parent(dst_file_path, Some(parent_guard), source) {
            Ok(()) => return Ok(()),
            Err(err) if should_retry_rename(&err, attempt) => {
                #[cfg(test)]
                windows_rename_test_hooks::run_before_rename_retry(dst_file_path);
                attempt += 1;
            }
            Err(err) => return Err(err),
        }
    }
}

#[cfg(unix)]
fn rename_into_existing_parent(
    src_file_path: &Path,
    dst_file_path: &Path,
    parent_guard: Option<&ExistingBaseDirectoryGuard>,
) -> io::Result<()> {
    use rustix::fs::{Mode, OFlags, open, renameat};

    let Some(parent_guard) = parent_guard else {
        let rename_started = rustfs_io_metrics::put_stage_timer();
        let result = super::fs::rename_std(src_file_path, dst_file_path);
        rustfs_io_metrics::record_put_object_stage_duration_from(
            rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_RENAME_SYSCALL,
            rename_started,
        );
        return result;
    };
    let src_parent = src_file_path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "rename source must have a parent directory"))?;
    let src_name = src_file_path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "rename source must have a file name"))?;
    let dst_name = dst_file_path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "rename destination must have a file name"))?;
    let src_parent = open(
        src_parent,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(io::Error::from)?;
    let dst_parent = parent_guard
        .last()
        .ok_or_else(|| io::Error::other("rename destination parent guard is empty"))?;

    let rename_started = rustfs_io_metrics::put_stage_timer();
    let result = renameat(&src_parent, src_name, dst_parent, dst_name).map_err(io::Error::from);
    rustfs_io_metrics::record_put_object_stage_duration_from(
        rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_RENAME_SYSCALL,
        rename_started,
    );
    result
}

#[cfg(windows)]
// SAFETY: this helper builds the variable-length FILE_RENAME_INFORMATION buffer
// with checked sizes and passes borrowed live handles only to synchronous NT calls.
#[allow(unsafe_code)]
fn rename_into_existing_parent(
    dst_file_path: &Path,
    parent_guard: Option<&ExistingBaseDirectoryGuard>,
    source: &winapi_util::Handle,
) -> io::Result<()> {
    use std::{
        mem::size_of,
        os::windows::{ffi::OsStrExt, io::AsRawHandle},
    };
    use windows_sys::{
        Wdk::Storage::FileSystem::{
            FILE_RENAME_INFORMATION, FILE_RENAME_INFORMATION_0, FileRenameInformation, FileRenameInformationEx,
            NtSetInformationFile,
        },
        Win32::{
            Foundation::{ERROR_ACCESS_DENIED, ERROR_SHARING_VIOLATION, RtlNtStatusToDosError},
            System::IO::IO_STATUS_BLOCK,
        },
    };

    let parent_guard = parent_guard
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "rename destination must have a parent directory"))?;
    #[cfg(test)]
    windows_rename_test_hooks::run_before_publication(dst_file_path);
    let dst_parent = parent_guard.last_handle()?;
    let dst_name = dst_file_path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "rename destination must have a file name"))?;
    let dst_name = dst_name.encode_wide().collect::<Vec<_>>();
    if dst_name.is_empty() || dst_name.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "rename destination file name is empty or contains a NUL",
        ));
    }

    let file_name_bytes = dst_name
        .len()
        .checked_mul(size_of::<u16>())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "rename destination file name is too long"))?;
    let file_name_length = u32::try_from(file_name_bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "rename destination file name is too long"))?;
    let buffer_size = size_of::<FILE_RENAME_INFORMATION>()
        .checked_add(file_name_bytes)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "rename information buffer is too large"))?;
    let buffer_size_u32 = u32::try_from(buffer_size)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "rename information buffer is too large"))?;
    let words = buffer_size.div_ceil(size_of::<usize>());
    let mut buffer = vec![0usize; words];
    let rename_info = buffer.as_mut_ptr().cast::<FILE_RENAME_INFORMATION>();

    // SAFETY: `buffer` is aligned for FILE_RENAME_INFORMATION and large enough for
    // its header, the complete UTF-16 name, and trailing zeroed storage.
    // `dst_parent` and `source` remain live until the synchronous call returns.
    unsafe {
        (*rename_info).Anonymous = FILE_RENAME_INFORMATION_0 { ReplaceIfExists: true };
        (*rename_info).RootDirectory = dst_parent.as_raw_handle();
        (*rename_info).FileNameLength = file_name_length;
        std::ptr::copy_nonoverlapping(
            dst_name.as_ptr(),
            std::ptr::addr_of_mut!((*rename_info).FileName).cast::<u16>(),
            dst_name.len(),
        );
    }

    // Keep the target relative to the retained parent handle so publication
    // cannot be redirected by replacing a pathname component.
    let mut io_status = IO_STATUS_BLOCK::default();
    let status = unsafe {
        NtSetInformationFile(
            source.as_raw_handle(),
            &mut io_status,
            rename_info.cast(),
            buffer_size_u32,
            FileRenameInformation,
        )
    };
    if status >= 0 {
        return Ok(());
    }

    let status_error = |status| {
        let code = unsafe { RtlNtStatusToDosError(status) };
        let error = match i32::try_from(code) {
            Ok(code) => io::Error::from_raw_os_error(code),
            Err(_) => io::Error::other(format!("Windows rename failed with NTSTATUS {status:#x}")),
        };
        (code, error)
    };
    let (legacy_error_code, legacy_error) = status_error(status);
    if !matches!(legacy_error_code, ERROR_ACCESS_DENIED | ERROR_SHARING_VIOLATION) {
        return Err(legacy_error);
    }

    // Match std::fs::rename's Windows fallback for read-only or open
    // destinations while retaining the guarded, handle-relative target. Older
    // FileRenameInformationEx implementations reject IGNORE_READONLY; retry
    // without only that optional flag so open-destination replacement remains
    // compatible while read-only destinations still fail explicitly there.
    windows_extended_rename_with_compatibility_fallback(legacy_error, |flags| {
        unsafe {
            (*rename_info).Anonymous = FILE_RENAME_INFORMATION_0 { Flags: flags };
        }
        let status = unsafe {
            NtSetInformationFile(
                source.as_raw_handle(),
                &mut io_status,
                rename_info.cast(),
                buffer_size_u32,
                FileRenameInformationEx,
            )
        };
        if status >= 0 { Ok(()) } else { Err(status_error(status).1) }
    })
}

#[cfg(windows)]
fn windows_extended_rename_with_compatibility_fallback(
    legacy_error: io::Error,
    mut rename: impl FnMut(u32) -> io::Result<()>,
) -> io::Result<()> {
    use windows_sys::{
        Wdk::Storage::FileSystem::{
            FILE_RENAME_IGNORE_READONLY_ATTRIBUTE, FILE_RENAME_POSIX_SEMANTICS, FILE_RENAME_REPLACE_IF_EXISTS,
        },
        Win32::Foundation::{ERROR_INVALID_FUNCTION, ERROR_INVALID_PARAMETER, ERROR_NOT_SUPPORTED},
    };

    let compatible_flags = FILE_RENAME_REPLACE_IF_EXISTS | FILE_RENAME_POSIX_SEMANTICS;
    let result = match rename(compatible_flags | FILE_RENAME_IGNORE_READONLY_ATTRIBUTE) {
        Err(err)
            if err
                .raw_os_error()
                .and_then(|code| u32::try_from(code).ok())
                .is_some_and(|code| matches!(code, ERROR_INVALID_FUNCTION | ERROR_INVALID_PARAMETER | ERROR_NOT_SUPPORTED)) =>
        {
            rename(compatible_flags)
        }
        result => result,
    };
    match result {
        Err(err)
            if err
                .raw_os_error()
                .and_then(|code| u32::try_from(code).ok())
                .is_some_and(|code| matches!(code, ERROR_INVALID_FUNCTION | ERROR_INVALID_PARAMETER | ERROR_NOT_SUPPORTED)) =>
        {
            Err(legacy_error)
        }
        result => result,
    }
}

#[cfg(all(not(unix), not(windows)))]
fn rename_into_existing_parent(
    src_file_path: &Path,
    dst_file_path: &Path,
    _parent_guard: Option<&ExistingBaseDirectoryGuard>,
) -> io::Result<()> {
    super::fs::rename_std(src_file_path, dst_file_path)
}

#[cfg(windows)]
#[derive(Clone)]
struct WindowsDirectoryHandle {
    handle: Arc<winapi_util::Handle>,
}

/// Stable root for namespace-changing disk operations.
///
/// Windows opens the configured endpoint once and keeps that directory identity
/// pinned for the lifetime of the disk. Publication then resolves every source
/// and destination component relative to this handle instead of re-entering the
/// mutable pathname namespace. Other platforms retain the path so callers use a
/// uniform API while their existing `openat`/`renameat` guards remain unchanged.
#[derive(Clone)]
pub(crate) struct PublicationRoot {
    path: PathBuf,
    #[cfg(windows)]
    configured_path: PathBuf,
    #[cfg(windows)]
    directory: WindowsDirectoryHandle,
}

impl PublicationRoot {
    pub(crate) fn new(path: &Path) -> io::Result<Self> {
        if !path.is_absolute() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "publication root must be absolute"));
        }

        #[cfg(windows)]
        let (resolved_path, directory) = open_windows_publication_root(path)?;

        Ok(Self {
            #[cfg(not(windows))]
            path: path.to_path_buf(),
            #[cfg(windows)]
            path: resolved_path,
            #[cfg(windows)]
            configured_path: path.to_path_buf(),
            #[cfg(windows)]
            directory,
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    #[cfg(windows)]
    fn relative_path<'a>(&self, path: &'a Path) -> io::Result<&'a Path> {
        // The configured path only derives a suffix; traversal stays rooted at the pinned directory handle.
        path.strip_prefix(&self.path)
            .or_else(|_| path.strip_prefix(&self.configured_path))
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path must remain below its publication root"))
    }
}

#[cfg(windows)]
#[derive(Clone)]
pub(crate) struct ExistingBaseDirectoryGuard {
    handles: Vec<WindowsDirectoryHandle>,
    #[cfg(test)]
    generation: u64,
}

#[cfg(all(test, windows))]
static WINDOWS_DIRECTORY_GUARD_GENERATION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

#[cfg(windows)]
impl ExistingBaseDirectoryGuard {
    fn new(handles: Vec<WindowsDirectoryHandle>) -> Self {
        Self {
            handles,
            #[cfg(test)]
            generation: WINDOWS_DIRECTORY_GUARD_GENERATION.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        }
    }

    fn last_handle(&self) -> io::Result<&winapi_util::Handle> {
        self.handles
            .last()
            .map(|directory| directory.handle.as_ref())
            .ok_or_else(|| io::Error::other("Windows directory guard is empty"))
    }
}

#[cfg(unix)]
pub(crate) type ExistingBaseDirectoryGuard = Vec<std::os::fd::OwnedFd>;

#[cfg(all(not(unix), not(windows)))]
pub(crate) type ExistingBaseDirectoryGuard = ();

#[derive(Clone)]
pub(crate) struct RenameCommitGuard {
    #[cfg(windows)]
    source_parent: PathBuf,
    #[cfg(windows)]
    destination_parent: PathBuf,
    #[cfg(windows)]
    source_parent_guard: ExistingBaseDirectoryGuard,
    #[cfg(windows)]
    destination_parent_guard: ExistingBaseDirectoryGuard,
}

pub(crate) struct RenameDestinationPathGuard {
    #[cfg(windows)]
    directory: PathBuf,
    #[cfg(windows)]
    _directory_guard: ExistingBaseDirectoryGuard,
}

impl RenameDestinationPathGuard {
    pub(crate) fn write_file_for_path_access(
        &self,
        file_path: &Path,
        data: &[u8],
        sync_file: bool,
        sync_parent: bool,
    ) -> io::Result<()> {
        #[cfg(windows)]
        {
            if file_path.parent() != Some(self.directory.as_path()) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "guarded destination file must be an immediate child of its directory",
                ));
            }
            file_path
                .file_name()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "guarded destination file must have a name"))?;
            let staging_name = format!(".rustfs-write-{}", uuid::Uuid::new_v4());
            let WindowsStagedFile { mut writer, publication } =
                create_windows_staged_file(self._directory_guard.last_handle()?, staging_name.as_ref())?;
            let write_result: io::Result<()> = (|| {
                std::io::Write::write_all(writer.as_file_mut(), data)?;
                if sync_file {
                    writer.as_file().sync_data()?;
                }
                Ok(())
            })();
            if let Err(write_err) = write_result {
                if let Err(cleanup_err) = set_windows_file_delete_on_close(&publication, true) {
                    return Err(io::Error::new(
                        write_err.kind(),
                        format!("{write_err}; failed to schedule staged file cleanup: {cleanup_err}"),
                    ));
                }
                return Err(write_err);
            }
            // Windows rejects replacement while the staged entry still has an
            // active data writer, even though that writer shares deletion. Keep
            // the separate publication handle as the identity anchor and close
            // the writer before issuing the handle-relative rename.
            drop(writer);
            if let Err(rename_err) = rename_windows_prepared(file_path, &self._directory_guard, &publication, 0) {
                if let Err(cleanup_err) = set_windows_file_delete_on_close(&publication, true) {
                    return Err(io::Error::new(
                        rename_err.kind(),
                        format!("{rename_err}; failed to schedule staged file cleanup: {cleanup_err}"),
                    ));
                }
                return Err(rename_err);
            }
            drop(publication);
            if sync_parent {
                fsync_dir_std(&self.directory)?;
            }
            return Ok(());
        }

        #[cfg(not(windows))]
        {
            let _ = self;
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(file_path)?;
            std::io::Write::write_all(&mut file, data)?;
            if sync_file {
                file.sync_data()?;
            }
            if sync_parent && let Some(parent) = file_path.parent() {
                fsync_dir_std(parent)?;
            }
            Ok(())
        }
    }
}

impl RenameCommitGuard {
    #[cfg(windows)]
    pub(crate) fn lock_source_directory_for_path_access(&self, directory: &Path) -> io::Result<RenameDestinationPathGuard> {
        if directory != self.source_parent {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "guarded source directory does not match the rename source parent",
            ));
        }
        Ok(RenameDestinationPathGuard {
            directory: directory.to_path_buf(),
            _directory_guard: self.source_parent_guard.clone(),
        })
    }

    pub(crate) fn create_destination_directory_for_path_access(
        &self,
        directory: &Path,
    ) -> io::Result<RenameDestinationPathGuard> {
        self.destination_directory_guard(directory, true)
    }

    /// Reopen a destination tree for handle-relative child publication.
    /// Ancestors remain write-exclusive while the final parent shares writes
    /// required by the kernel's relative rename. Delete sharing stays omitted
    /// throughout, so every retained directory identity remains pinned.
    fn destination_directory_guard(&self, directory: &Path, create_missing: bool) -> io::Result<RenameDestinationPathGuard> {
        #[cfg(windows)]
        {
            use windows_sys::Wdk::Storage::FileSystem::{FILE_OPEN, FILE_OPEN_IF};
            use windows_sys::Win32::Storage::FileSystem::{FILE_SHARE_READ, FILE_SHARE_WRITE};

            let relative = directory.strip_prefix(&self.destination_parent).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "guarded path must remain below the rename destination parent",
                )
            })?;
            for component in relative.components() {
                if !matches!(component, Component::Normal(_) | Component::CurDir) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "guarded destination path contains an invalid component",
                    ));
                }
            }

            let component = self.destination_parent.file_name().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "cannot safely reopen the publication root for pathname access",
                )
            })?;
            let parent_index = self.destination_parent_guard.handles.len().checked_sub(2).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "cannot safely reopen the publication root for pathname access",
                )
            })?;
            let mut handles = self.destination_parent_guard.handles[..=parent_index].to_vec();
            let parent = handles
                .last()
                .ok_or_else(|| io::Error::other("Windows destination guard lost its parent handle"))?;
            let mut relative_components = relative
                .components()
                .filter_map(|component| match component {
                    Component::Normal(component) => Some(component),
                    _ => None,
                })
                .peekable();
            let destination_parent_share = if relative_components.peek().is_none() {
                FILE_SHARE_READ | FILE_SHARE_WRITE
            } else {
                FILE_SHARE_READ
            };
            handles.push(open_windows_relative_directory_component(
                parent,
                component,
                FILE_OPEN,
                destination_parent_share,
            )?);
            while let Some(component) = relative_components.next() {
                let parent = handles
                    .last()
                    .ok_or_else(|| io::Error::other("Windows destination path guard lost its parent handle"))?;
                let disposition = if create_missing { FILE_OPEN_IF } else { FILE_OPEN };
                let share_access = if relative_components.peek().is_none() {
                    FILE_SHARE_READ | FILE_SHARE_WRITE
                } else {
                    FILE_SHARE_READ
                };
                handles.push(open_windows_relative_directory_component(parent, component, disposition, share_access)?);
            }
            Ok(RenameDestinationPathGuard {
                directory: directory.to_path_buf(),
                _directory_guard: ExistingBaseDirectoryGuard::new(handles),
            })
        }

        #[cfg(not(windows))]
        {
            let _ = (self, directory, create_missing);
            Ok(RenameDestinationPathGuard {})
        }
    }
}

pub(crate) fn prepare_rename_commit_guard(
    source_parent: &Path,
    destination_parent: &Path,
    destination_base: &Path,
    publication_root: &PublicationRoot,
) -> io::Result<RenameCommitGuard> {
    #[cfg(windows)]
    {
        // A same-directory rename must use one shared-write parent handle:
        // retaining a second read-only-share handle would block the kernel's
        // relative target open. Delete sharing stays excluded, so identity is
        // still pinned. Distinct source trees remain strict.
        let same_parent = publication_root.relative_path(source_parent)? == publication_root.relative_path(destination_parent)?;
        let (source_parent_guard, destination_parent_guard) = if same_parent {
            let destination_parent_guard =
                mkdir_all_below_existing_base_std(destination_parent, destination_base, publication_root)?;
            (destination_parent_guard.clone(), destination_parent_guard)
        } else {
            // The source parent also hosts private rollback staging files.
            // Their handle-relative publication needs write sharing on this
            // final directory while delete sharing remains excluded.
            let source_parent_guard = lock_windows_directory_tree(source_parent, Some(source_parent), publication_root)?;
            let destination_parent_guard =
                mkdir_all_below_existing_base_std(destination_parent, destination_base, publication_root)?;
            (source_parent_guard, destination_parent_guard)
        };
        Ok(RenameCommitGuard {
            source_parent: source_parent.to_path_buf(),
            destination_parent: destination_parent.to_path_buf(),
            source_parent_guard,
            destination_parent_guard,
        })
    }

    #[cfg(not(windows))]
    {
        let _ = (source_parent, destination_parent, destination_base, publication_root);
        Ok(RenameCommitGuard {})
    }
}

#[cfg(windows)]
fn open_windows_publication_root(path: &Path) -> io::Result<(PathBuf, WindowsDirectoryHandle)> {
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_FLAG_BACKUP_SEMANTICS, FILE_READ_ATTRIBUTES, FILE_SHARE_READ, FILE_SHARE_WRITE, FILE_TRAVERSE,
    };

    // Follow a configured endpoint mount/junction once, then pin the resolved
    // directory identity. The configured root is the trust boundary: allow
    // ordinary writes beneath it, but omit delete sharing so its directory entry
    // cannot be replaced while this disk is active. Publication resolves all
    // children relative to the retained identity.
    let file = std::fs::OpenOptions::new()
        .access_mode(FILE_TRAVERSE | FILE_READ_ATTRIBUTES)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)?;
    let directory = windows_directory_handle(winapi_util::Handle::from_file(file))?;
    let resolved_path = windows_final_path(directory.handle.as_ref())?;
    Ok((resolved_path, directory))
}

#[cfg(windows)]
fn windows_final_path(handle: &winapi_util::Handle) -> io::Result<PathBuf> {
    use windows_sys::Win32::Storage::FileSystem::{FILE_NAME_NORMALIZED, VOLUME_NAME_DOS, VOLUME_NAME_GUID, VOLUME_NAME_NT};

    windows_final_path_with_fallbacks(
        windows_final_path_with_flags(handle, FILE_NAME_NORMALIZED | VOLUME_NAME_DOS),
        || windows_final_path_with_flags(handle, FILE_NAME_NORMALIZED | VOLUME_NAME_GUID),
        || windows_final_path_with_flags(handle, FILE_NAME_NORMALIZED | VOLUME_NAME_NT).and_then(windows_nt_path_to_global_root),
    )
}

#[cfg(windows)]
fn windows_final_path_with_fallbacks(
    dos_path: io::Result<PathBuf>,
    guid_path: impl FnOnce() -> io::Result<PathBuf>,
    nt_path: impl FnOnce() -> io::Result<PathBuf>,
) -> io::Result<PathBuf> {
    use windows_sys::Win32::Foundation::ERROR_PATH_NOT_FOUND;

    match dos_path {
        Ok(path) => Ok(path),
        Err(err)
            if err
                .raw_os_error()
                .and_then(|code| u32::try_from(code).ok())
                .is_some_and(|code| code == ERROR_PATH_NOT_FOUND) =>
        {
            match guid_path() {
                Ok(path) => Ok(path),
                Err(err)
                    if err
                        .raw_os_error()
                        .and_then(|code| u32::try_from(code).ok())
                        .is_some_and(|code| code == ERROR_PATH_NOT_FOUND) =>
                {
                    nt_path()
                }
                Err(err) => Err(err),
            }
        }
        Err(err) => Err(err),
    }
}

#[cfg(windows)]
fn windows_nt_path_to_global_root(path: PathBuf) -> io::Result<PathBuf> {
    use std::ffi::OsString;

    if !matches!(path.components().next(), Some(Component::RootDir)) {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Windows NT final path is not rooted"));
    }

    let mut global_root = OsString::from(r"\\?\GLOBALROOT");
    global_root.push(path.as_os_str());
    Ok(PathBuf::from(global_root))
}

#[cfg(windows)]
// SAFETY: the output buffer is owned and sized in UTF-16 code units, and the
// borrowed root handle remains live for both synchronous queries.
#[allow(unsafe_code)]
fn windows_final_path_with_flags(handle: &winapi_util::Handle, flags: u32) -> io::Result<PathBuf> {
    use std::{ffi::OsString, os::windows::ffi::OsStringExt, os::windows::io::AsRawHandle};
    use windows_sys::Win32::Storage::FileSystem::GetFinalPathNameByHandleW;

    let required = unsafe { GetFinalPathNameByHandleW(handle.as_raw_handle(), std::ptr::null_mut(), 0, flags) };
    if required == 0 {
        return Err(io::Error::last_os_error());
    }
    let capacity = required
        .checked_add(1)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Windows final path length overflow"))?;
    let capacity_usize = usize::try_from(capacity)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Windows final path length exceeds usize"))?;
    let mut buffer = vec![0u16; capacity_usize];
    let length = unsafe { GetFinalPathNameByHandleW(handle.as_raw_handle(), buffer.as_mut_ptr(), capacity, flags) };
    if length == 0 {
        return Err(io::Error::last_os_error());
    }
    if length >= capacity {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Windows final path changed while it was queried",
        ));
    }
    buffer.truncate(
        usize::try_from(length)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Windows final path length exceeds usize"))?,
    );
    let path = PathBuf::from(OsString::from_wide(&buffer));
    Ok(rustfs_utils::simplified(&path).to_path_buf())
}

#[cfg(windows)]
fn windows_directory_handle(handle: winapi_util::Handle) -> io::Result<WindowsDirectoryHandle> {
    use windows_sys::Win32::Storage::FileSystem::{FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT};

    let info = windows_file_attribute_tag(&handle)?;
    if info.FileAttributes & FILE_ATTRIBUTE_DIRECTORY == 0 || info.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(io::Error::from(io::ErrorKind::NotADirectory));
    }
    Ok(WindowsDirectoryHandle {
        handle: Arc::new(handle),
    })
}

#[cfg(windows)]
// SAFETY: the output buffer has the exact FILE_ATTRIBUTE_TAG_INFO layout and
// the borrowed handle remains live for the synchronous query.
#[allow(unsafe_code)]
fn windows_file_attribute_tag(
    handle: &winapi_util::Handle,
) -> io::Result<windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_TAG_INFO> {
    use std::{mem::size_of, os::windows::io::AsRawHandle};
    use windows_sys::Win32::{
        Foundation::{ERROR_INVALID_FUNCTION, ERROR_INVALID_PARAMETER, ERROR_NOT_SUPPORTED},
        Storage::FileSystem::{
            FILE_ATTRIBUTE_REPARSE_POINT, FILE_ATTRIBUTE_TAG_INFO, FileAttributeTagInfo, GetFileInformationByHandleEx,
        },
    };

    let mut info = FILE_ATTRIBUTE_TAG_INFO::default();
    let info_size = u32::try_from(size_of::<FILE_ATTRIBUTE_TAG_INFO>())
        .map_err(|_| io::Error::other("Windows file attribute tag information size exceeds u32"))?;
    let queried = unsafe {
        GetFileInformationByHandleEx(
            handle.as_raw_handle(),
            FileAttributeTagInfo,
            std::ptr::addr_of_mut!(info).cast(),
            info_size,
        )
    };
    if queried != 0 {
        return Ok(info);
    }

    let err = io::Error::last_os_error();
    let unsupported = err
        .raw_os_error()
        .and_then(|code| u32::try_from(code).ok())
        .is_some_and(|code| matches!(code, ERROR_INVALID_FUNCTION | ERROR_INVALID_PARAMETER | ERROR_NOT_SUPPORTED));
    if !unsupported {
        return Err(err);
    }

    // Some local Windows filesystems do not implement FileAttributeTagInfo.
    // The legacy handle query is enough for ordinary entries; fail closed for
    // reparse points because it cannot identify a safe tag.
    let legacy = winapi_util::file::information(handle)?;
    let attributes = u32::try_from(legacy.file_attributes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Windows file attributes exceed u32"))?;
    if attributes & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(err);
    }
    Ok(FILE_ATTRIBUTE_TAG_INFO {
        FileAttributes: attributes,
        ReparseTag: 0,
    })
}

#[cfg(windows)]
fn lock_windows_directory_tree(
    path: &Path,
    shared_write_ancestor: Option<&Path>,
    publication_root: &PublicationRoot,
) -> io::Result<ExistingBaseDirectoryGuard> {
    use windows_sys::Wdk::Storage::FileSystem::FILE_OPEN;
    use windows_sys::Win32::Storage::FileSystem::{FILE_SHARE_READ, FILE_SHARE_WRITE};

    let relative = publication_root.relative_path(path)?;
    let shared_write_relative = shared_write_ancestor
        .map(|ancestor| publication_root.relative_path(ancestor))
        .transpose()?
        .filter(|ancestor| relative.starts_with(*ancestor));
    let mut handles = Vec::with_capacity(relative.components().count().saturating_add(1));
    handles.push(publication_root.directory.clone());
    let mut opened_relative = PathBuf::new();

    for component in relative.components() {
        let Component::Normal(component) = component else {
            if matches!(component, Component::CurDir) {
                continue;
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Windows guarded path contains an invalid component",
            ));
        };
        let parent = handles
            .last()
            .ok_or_else(|| io::Error::other("Windows directory guard lost its root handle"))?;
        opened_relative.push(component);
        let child = if shared_write_relative.is_some_and(|ancestor| opened_relative.as_path() == ancestor) {
            // A handle-relative rename opens the target parent for write. When
            // that parent is also a source ancestor, this source-side handle
            // must share write access or the transaction blocks itself. Delete
            // sharing remains omitted, so the directory identity stays pinned.
            open_windows_relative_directory_component(parent, component, FILE_OPEN, FILE_SHARE_READ | FILE_SHARE_WRITE)?
        } else {
            open_windows_directory_component(parent, component, FILE_OPEN)?
        };
        handles.push(child);
    }

    Ok(ExistingBaseDirectoryGuard::new(handles))
}

#[cfg(windows)]
// SAFETY: the object attributes borrow a checked UTF-16 component and live
// parent handle for the duration of the synchronous NtCreateFile call.
#[allow(unsafe_code)]
fn open_windows_relative(
    parent: &winapi_util::Handle,
    component: &std::ffi::OsStr,
    desired_access: u32,
    share_access: u32,
    create_disposition: u32,
    create_options: u32,
    file_attributes: u32,
    dont_reparse: bool,
) -> io::Result<winapi_util::Handle> {
    use std::{
        mem::size_of,
        os::windows::{ffi::OsStrExt, io::AsRawHandle, io::FromRawHandle},
    };
    use windows_sys::{
        Wdk::{Foundation::OBJECT_ATTRIBUTES, Storage::FileSystem::NtCreateFile},
        Win32::{
            Foundation::{HANDLE, OBJ_CASE_INSENSITIVE, OBJ_DONT_REPARSE, RtlNtStatusToDosError, UNICODE_STRING},
            System::IO::IO_STATUS_BLOCK,
        },
    };

    let mut name = component.encode_wide().collect::<Vec<_>>();
    if name.is_empty() || name.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "relative Windows file name is empty or contains a NUL",
        ));
    }
    let name_bytes = name
        .len()
        .checked_mul(size_of::<u16>())
        .and_then(|length| u16::try_from(length).ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "relative Windows file name is too long"))?;
    let unicode_name = UNICODE_STRING {
        Length: name_bytes,
        MaximumLength: name_bytes,
        Buffer: name.as_mut_ptr(),
    };
    let object_attributes_length = u32::try_from(size_of::<OBJECT_ATTRIBUTES>())
        .map_err(|_| io::Error::other("Windows object attributes size exceeds u32"))?;
    let object_attributes = OBJECT_ATTRIBUTES {
        Length: object_attributes_length,
        RootDirectory: parent.as_raw_handle(),
        ObjectName: &unicode_name,
        Attributes: OBJ_CASE_INSENSITIVE | if dont_reparse { OBJ_DONT_REPARSE } else { 0 },
        SecurityDescriptor: std::ptr::null(),
        SecurityQualityOfService: std::ptr::null(),
    };
    let mut handle: HANDLE = std::ptr::null_mut();
    let mut io_status = IO_STATUS_BLOCK::default();
    let status = unsafe {
        NtCreateFile(
            &mut handle,
            desired_access,
            &object_attributes,
            &mut io_status,
            std::ptr::null(),
            file_attributes,
            share_access,
            create_disposition,
            create_options,
            std::ptr::null(),
            0,
        )
    };
    if status < 0 {
        return match i32::try_from(unsafe { RtlNtStatusToDosError(status) }) {
            Ok(code) => Err(io::Error::from_raw_os_error(code)),
            Err(_) => Err(io::Error::other(format!("Windows relative open failed with NTSTATUS {status:#x}"))),
        };
    }
    if handle.is_null() {
        return Err(io::Error::other("Windows relative open returned an invalid handle"));
    }

    Ok(unsafe { winapi_util::Handle::from_raw_handle(handle) })
}

#[cfg(windows)]
fn create_windows_superseding_file(parent: &winapi_util::Handle, component: &std::ffi::OsStr) -> io::Result<winapi_util::Handle> {
    use windows_sys::Wdk::Storage::FileSystem::FILE_SUPERSEDE;

    create_windows_owned_file(parent, component, FILE_SUPERSEDE)
}

#[cfg(windows)]
struct WindowsStagedFile {
    writer: winapi_util::Handle,
    publication: winapi_util::Handle,
}

#[cfg(windows)]
fn create_windows_staged_file(parent: &winapi_util::Handle, component: &std::ffi::OsStr) -> io::Result<WindowsStagedFile> {
    use windows_sys::{
        Wdk::Storage::FileSystem::{
            FILE_CREATE, FILE_NON_DIRECTORY_FILE, FILE_OPEN, FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT,
        },
        Win32::Storage::FileSystem::{
            DELETE, FILE_ATTRIBUTE_NORMAL, FILE_READ_ATTRIBUTES, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
            FILE_WRITE_DATA, SYNCHRONIZE,
        },
    };

    // Keep writing and namespace mutation on separate handles. Both handles
    // must share deletion because Windows requires every open source handle to
    // allow deletion before a rename. The random staging name and publication
    // handle retain the exact file identity while excluding other writers.
    let writer = open_windows_relative(
        parent,
        component,
        SYNCHRONIZE | FILE_READ_ATTRIBUTES | FILE_WRITE_DATA,
        FILE_SHARE_READ | FILE_SHARE_DELETE,
        FILE_CREATE,
        FILE_NON_DIRECTORY_FILE | FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT,
        FILE_ATTRIBUTE_NORMAL,
        true,
    )?;
    validate_windows_owned_file(&writer)?;
    let expected_identity = windows_file_identity(&writer)?;
    let publication = open_windows_relative(
        parent,
        component,
        DELETE | SYNCHRONIZE | FILE_READ_ATTRIBUTES,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        FILE_OPEN,
        FILE_NON_DIRECTORY_FILE | FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT,
        0,
        false,
    )?;
    validate_windows_owned_file(&publication)?;
    if windows_file_identity(&publication)? != expected_identity {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "staged Windows metadata identity changed while publication was prepared",
        ));
    }

    Ok(WindowsStagedFile { writer, publication })
}

#[cfg(windows)]
fn create_windows_owned_file(
    parent: &winapi_util::Handle,
    component: &std::ffi::OsStr,
    create_disposition: u32,
) -> io::Result<winapi_util::Handle> {
    use windows_sys::{
        Wdk::Storage::FileSystem::{FILE_NON_DIRECTORY_FILE, FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT},
        Win32::Storage::FileSystem::{
            DELETE, FILE_ATTRIBUTE_NORMAL, FILE_READ_ATTRIBUTES, FILE_SHARE_READ, FILE_WRITE_DATA, SYNCHRONIZE,
        },
    };

    // Open relative to the retained parent so the caller's disposition cannot
    // be redirected through a replaced path component or final reparse point.
    let file = open_windows_relative(
        parent,
        component,
        DELETE | SYNCHRONIZE | FILE_READ_ATTRIBUTES | FILE_WRITE_DATA,
        FILE_SHARE_READ,
        create_disposition,
        FILE_NON_DIRECTORY_FILE | FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT,
        FILE_ATTRIBUTE_NORMAL,
        true,
    )?;
    validate_windows_owned_file(&file)?;
    Ok(file)
}

#[cfg(windows)]
fn validate_windows_owned_file(file: &winapi_util::Handle) -> io::Result<()> {
    use windows_sys::Win32::Storage::FileSystem::{FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT};

    let info = windows_file_attribute_tag(file)?;
    if info.FileAttributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT) != 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "guarded Windows metadata entry is not an ordinary file",
        ));
    }
    if winapi_util::file::information(file)?.number_of_links() != 1 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "guarded Windows metadata entry retained an unexpected hard link",
        ));
    }
    Ok(())
}

#[cfg(windows)]
// SAFETY: the disposition buffer has the exact kernel layout and the borrowed
// file handle remains live for the synchronous NtSetInformationFile call.
#[allow(unsafe_code)]
fn set_windows_file_delete_on_close(file: &winapi_util::Handle, delete_file: bool) -> io::Result<()> {
    use std::{mem::size_of, os::windows::io::AsRawHandle};
    use windows_sys::{
        Wdk::Storage::FileSystem::{FILE_DISPOSITION_INFORMATION, FileDispositionInformation, NtSetInformationFile},
        Win32::{Foundation::RtlNtStatusToDosError, System::IO::IO_STATUS_BLOCK},
    };

    let mut disposition = FILE_DISPOSITION_INFORMATION { DeleteFile: delete_file };
    let length = u32::try_from(size_of::<FILE_DISPOSITION_INFORMATION>())
        .map_err(|_| io::Error::other("Windows file disposition size exceeds u32"))?;
    let mut io_status = IO_STATUS_BLOCK::default();
    let status = unsafe {
        NtSetInformationFile(
            file.as_raw_handle(),
            &mut io_status,
            std::ptr::addr_of_mut!(disposition).cast(),
            length,
            FileDispositionInformation,
        )
    };
    if status >= 0 {
        return Ok(());
    }
    let code = unsafe { RtlNtStatusToDosError(status) };
    match i32::try_from(code) {
        Ok(code) => Err(io::Error::from_raw_os_error(code)),
        Err(_) => Err(io::Error::other(format!("Windows file disposition failed with NTSTATUS {status:#x}"))),
    }
}

#[cfg(windows)]
fn read_windows_relative_file(file_path: &Path, parent_guard: &ExistingBaseDirectoryGuard) -> io::Result<Option<Vec<u8>>> {
    use windows_sys::{
        Wdk::Storage::FileSystem::{FILE_NON_DIRECTORY_FILE, FILE_OPEN, FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT},
        Win32::Storage::FileSystem::{
            FILE_READ_ATTRIBUTES, FILE_READ_DATA, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, SYNCHRONIZE,
        },
    };

    let file_name = file_path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "destination metadata must have a file name"))?;
    let identity_anchor = match open_windows_relative(
        parent_guard.last_handle()?,
        file_name,
        SYNCHRONIZE | FILE_READ_ATTRIBUTES,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        FILE_OPEN,
        FILE_NON_DIRECTORY_FILE | FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT,
        0,
        false,
    ) {
        Ok(file) => file,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    let anchor_info = windows_file_attribute_tag(&identity_anchor)?;
    if !windows_rename_source_is_allowed(anchor_info.FileAttributes, anchor_info.ReparseTag) {
        return Err(io::Error::new(io::ErrorKind::PermissionDenied, WINDOWS_RENAME_SOURCE_REPARSE_ERROR));
    }
    let expected_identity = windows_file_identity(&identity_anchor)?;

    // Data Dedup entries must be opened normally for reads. The reparse-point
    // anchor above validates the tag first, and the identity comparison below
    // rejects any final-entry substitution between the two opens.
    let mut file = open_windows_relative(
        parent_guard.last_handle()?,
        file_name,
        SYNCHRONIZE | FILE_READ_ATTRIBUTES | FILE_READ_DATA,
        FILE_SHARE_READ,
        FILE_OPEN,
        FILE_NON_DIRECTORY_FILE | FILE_SYNCHRONOUS_IO_NONALERT,
        0,
        false,
    )?;
    if windows_file_identity(&file)? != expected_identity {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "destination metadata identity changed while it was opened",
        ));
    }
    drop(identity_anchor);

    let file_size = winapi_util::file::information(&file)?.file_size();
    let capacity = usize::try_from(file_size)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "destination metadata size exceeds usize"))?;
    let mut data = Vec::new();
    data.try_reserve_exact(capacity)
        .map_err(|err| io::Error::other(format!("failed to reserve destination metadata buffer: {err}")))?;
    std::io::Read::read_to_end(file.as_file_mut(), &mut data)?;
    Ok(Some(data))
}

#[cfg(windows)]
pub(crate) fn compare_and_update_control_file(
    file_path: &Path,
    expected: Option<&[u8]>,
    replacement: Option<&[u8]>,
    sync_metadata: bool,
    publication_root: &PublicationRoot,
) -> io::Result<ConditionalFileUpdate> {
    use windows_sys::{
        Wdk::Storage::FileSystem::{
            FILE_NON_DIRECTORY_FILE, FILE_OPEN, FILE_OPEN_IF, FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT,
        },
        Win32::Storage::FileSystem::{
            DELETE, FILE_ATTRIBUTE_NORMAL, FILE_READ_ATTRIBUTES, FILE_SHARE_READ, FILE_SHARE_WRITE, FILE_WRITE_DATA, SYNCHRONIZE,
        },
    };

    let parent = file_path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "conditional file has no parent"))?;
    let parent_guard = lock_windows_directory_tree(parent, Some(parent), publication_root)?;
    let lock = open_windows_relative(
        parent_guard.last_handle()?,
        std::ffi::OsStr::new(".rustfs-cas.lock"),
        SYNCHRONIZE | FILE_READ_ATTRIBUTES | FILE_WRITE_DATA,
        FILE_SHARE_READ | FILE_SHARE_WRITE,
        FILE_OPEN_IF,
        FILE_NON_DIRECTORY_FILE | FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT,
        FILE_ATTRIBUTE_NORMAL,
        true,
    )?;
    validate_windows_owned_file(&lock)?;
    match lock.as_file().try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => return Err(io::Error::from(io::ErrorKind::WouldBlock)),
        Err(std::fs::TryLockError::Error(err)) => return Err(err),
    }

    let current = read_windows_relative_file(file_path, &parent_guard)?;
    let matches = match (&current, expected) {
        (None, None) => true,
        (Some(current), Some(expected)) => current.as_slice() == expected,
        _ => false,
    };
    if !matches {
        return Ok(match current {
            None => ConditionalFileUpdate::Missing,
            Some(_) => ConditionalFileUpdate::Mismatch,
        });
    }

    match replacement {
        Some(replacement) => RenameDestinationPathGuard {
            directory: parent.to_path_buf(),
            _directory_guard: parent_guard,
        }
        .write_file_for_path_access(file_path, replacement, sync_metadata, sync_metadata)?,
        None => {
            let file_name = file_path
                .file_name()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "conditional file must have a name"))?;
            let file = open_windows_relative(
                parent_guard.last_handle()?,
                file_name,
                DELETE | SYNCHRONIZE | FILE_READ_ATTRIBUTES,
                FILE_SHARE_READ,
                FILE_OPEN,
                FILE_NON_DIRECTORY_FILE | FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT,
                0,
                true,
            )?;
            validate_windows_owned_file(&file)?;
            set_windows_file_delete_on_close(&file, true)?;
            drop(file);
            if sync_metadata {
                fsync_dir_std(parent)?;
            }
        }
    }

    Ok(ConditionalFileUpdate::Updated)
}

#[cfg(windows)]
fn open_windows_directory_component(
    parent: &WindowsDirectoryHandle,
    component: &std::ffi::OsStr,
    create_disposition: u32,
) -> io::Result<WindowsDirectoryHandle> {
    use windows_sys::Win32::Storage::FileSystem::FILE_SHARE_READ;

    open_windows_relative_directory_component(parent, component, create_disposition, FILE_SHARE_READ)
}

#[cfg(windows)]
fn open_windows_relative_directory_component(
    parent: &WindowsDirectoryHandle,
    component: &std::ffi::OsStr,
    create_disposition: u32,
    share_access: u32,
) -> io::Result<WindowsDirectoryHandle> {
    use windows_sys::{
        Wdk::Storage::FileSystem::{FILE_DIRECTORY_FILE, FILE_OPEN_REPARSE_POINT},
        Win32::Storage::FileSystem::{
            FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT, FILE_READ_ATTRIBUTES, FILE_TRAVERSE,
        },
    };

    let anchor = open_windows_relative(
        &parent.handle,
        component,
        FILE_TRAVERSE | FILE_READ_ATTRIBUTES,
        share_access,
        create_disposition,
        FILE_DIRECTORY_FILE | FILE_OPEN_REPARSE_POINT,
        FILE_ATTRIBUTE_DIRECTORY,
        true,
    )?;
    let info = windows_file_attribute_tag(&anchor)?;
    if info.FileAttributes & FILE_ATTRIBUTE_DIRECTORY == 0 {
        return Err(io::Error::from(io::ErrorKind::NotADirectory));
    }
    if info.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT == 0 {
        return Ok(WindowsDirectoryHandle {
            handle: Arc::new(anchor),
        });
    }
    Err(io::Error::new(
        io::ErrorKind::PermissionDenied,
        "guarded Windows path contains a reparse point below its publication root",
    ))
}

#[cfg(windows)]
const WINDOWS_RENAME_SOURCE_REPARSE_ERROR: &str = "rename source must be an ordinary file or a Windows data-dedup entry";

#[cfg(windows)]
fn open_windows_rename_source(
    src_file_path: &Path,
    source_parent_guard: &ExistingBaseDirectoryGuard,
) -> io::Result<winapi_util::Handle> {
    use windows_sys::{
        Wdk::Storage::FileSystem::{FILE_OPEN, FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT},
        Win32::Storage::FileSystem::{DELETE, FILE_READ_ATTRIBUTES, FILE_SHARE_READ, SYNCHRONIZE},
    };

    let src_name = src_file_path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "rename source must have a file name"))?;
    // The parent tree is already pinned and reparse-free. Open the final entry
    // itself so an approved Data Dedup reparse point can be tag-validated below.
    let source = open_windows_relative(
        source_parent_guard.last_handle()?,
        src_name,
        DELETE | SYNCHRONIZE | FILE_READ_ATTRIBUTES,
        FILE_SHARE_READ,
        FILE_OPEN,
        FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT,
        0,
        false,
    )?;
    let source_info = windows_file_attribute_tag(&source)?;
    if !windows_rename_source_is_allowed(source_info.FileAttributes, source_info.ReparseTag) {
        return Err(io::Error::new(io::ErrorKind::PermissionDenied, WINDOWS_RENAME_SOURCE_REPARSE_ERROR));
    }
    Ok(source)
}

#[cfg(windows)]
fn open_windows_rename_source_identity(
    src_file_path: &Path,
    source_parent_guard: &ExistingBaseDirectoryGuard,
) -> io::Result<(winapi_util::Handle, (u64, [u8; 16]))> {
    use windows_sys::{
        Wdk::Storage::FileSystem::{FILE_OPEN, FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT},
        Win32::Storage::FileSystem::{FILE_READ_ATTRIBUTES, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, SYNCHRONIZE},
    };

    let src_name = src_file_path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "rename source must have a file name"))?;
    // Match the rename handle: bypass final-entry reparse processing, then
    // admit only ordinary files or the Data Dedup tag below.
    let source = open_windows_relative(
        source_parent_guard.last_handle()?,
        src_name,
        SYNCHRONIZE | FILE_READ_ATTRIBUTES,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        FILE_OPEN,
        FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT,
        0,
        false,
    )?;
    let source_info = windows_file_attribute_tag(&source)?;
    if !windows_rename_source_is_allowed(source_info.FileAttributes, source_info.ReparseTag) {
        return Err(io::Error::new(io::ErrorKind::PermissionDenied, WINDOWS_RENAME_SOURCE_REPARSE_ERROR));
    }
    let identity = windows_file_identity(&source)?;
    Ok((source, identity))
}

#[cfg(windows)]
// SAFETY: FILE_ID_INFO is an initialized fixed-size output buffer and the
// borrowed handle remains live for the synchronous query.
#[allow(unsafe_code)]
fn windows_file_identity(handle: &winapi_util::Handle) -> io::Result<(u64, [u8; 16])> {
    use std::{mem::size_of, os::windows::io::AsRawHandle};
    use windows_sys::Win32::{
        Foundation::{ERROR_INVALID_FUNCTION, ERROR_INVALID_PARAMETER, ERROR_NOT_SUPPORTED},
        Storage::FileSystem::{FILE_ID_INFO, FileIdInfo, GetFileInformationByHandleEx},
    };

    let mut identity = FILE_ID_INFO::default();
    let identity_size = u32::try_from(size_of::<FILE_ID_INFO>())
        .map_err(|_| io::Error::other("Windows file identity information size exceeds u32"))?;
    let queried = unsafe {
        GetFileInformationByHandleEx(handle.as_raw_handle(), FileIdInfo, std::ptr::addr_of_mut!(identity).cast(), identity_size)
    };
    if queried != 0 {
        if windows_file_id_is_available(&identity.FileId.Identifier) {
            return Ok((identity.VolumeSerialNumber, identity.FileId.Identifier));
        }
        return windows_legacy_file_identity(handle);
    }

    let err = io::Error::last_os_error();
    let unsupported = err
        .raw_os_error()
        .and_then(|code| u32::try_from(code).ok())
        .is_some_and(|code| matches!(code, ERROR_INVALID_FUNCTION | ERROR_INVALID_PARAMETER | ERROR_NOT_SUPPORTED));
    if !unsupported {
        return Err(err);
    }

    windows_legacy_file_identity(handle)
}

#[cfg(windows)]
fn windows_legacy_file_identity(handle: &winapi_util::Handle) -> io::Result<(u64, [u8; 16])> {
    use std::mem::size_of;

    // FileIdInfo is unavailable on a few older local filesystems. Keep the
    // source identity pinned by its live anchor handle and compare the legacy
    // volume/file index instead of silently disabling the check.
    let information = winapi_util::file::information(handle)?;
    let legacy_file_id = information.file_index().to_ne_bytes();
    if !windows_file_id_is_available(&legacy_file_id) {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Windows filesystem did not provide a stable 64-bit file identity",
        ));
    }
    let mut file_id = [0; 16];
    file_id[..size_of::<u64>()].copy_from_slice(&legacy_file_id);
    Ok((information.volume_serial_number(), file_id))
}

#[cfg(windows)]
fn windows_file_id_is_available(file_id: &[u8]) -> bool {
    file_id.iter().any(|byte| *byte != 0) && file_id.iter().any(|byte| *byte != u8::MAX)
}

#[cfg(windows)]
fn windows_rename_source_is_allowed(attributes: u32, reparse_tag: u32) -> bool {
    use windows_sys::Win32::{Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT, System::SystemServices::IO_REPARSE_TAG_DEDUP};

    attributes & FILE_ATTRIBUTE_REPARSE_POINT == 0 || reparse_tag == IO_REPARSE_TAG_DEDUP
}

pub(crate) fn mkdir_all_below_existing_base_std(
    dir_path: &Path,
    base_dir: &Path,
    publication_root: &PublicationRoot,
) -> io::Result<ExistingBaseDirectoryGuard> {
    let relative = dir_path
        .strip_prefix(base_dir)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "rename destination must remain below its base directory"))?;
    for component in relative.components() {
        if !matches!(component, Component::Normal(_) | Component::CurDir) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "rename destination contains an invalid path component",
            ));
        }
    }

    #[cfg(unix)]
    {
        let _ = publication_root;
        use rustix::fs::{Mode, OFlags, mkdirat, open, openat};
        use rustix::io::Errno;

        let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
        let mode = Mode::RWXU | Mode::RWXG | Mode::RWXO;
        let mut parents = vec![open(base_dir, flags, Mode::empty()).map_err(io::Error::from)?];

        for component in relative.components() {
            let Component::Normal(component) = component else {
                continue;
            };
            let parent = parents
                .last()
                .expect("base directory guard should contain the base directory");
            match mkdirat(parent, component, mode) {
                Ok(()) => {}
                Err(Errno::EXIST) => {}
                Err(err) => return Err(err.into()),
            }
            parents.push(openat(parent, component, flags, Mode::empty()).map_err(io::Error::from)?);
        }

        Ok(parents)
    }

    #[cfg(windows)]
    {
        use windows_sys::Wdk::Storage::FileSystem::{FILE_OPEN, FILE_OPEN_IF};
        use windows_sys::Win32::Storage::FileSystem::{FILE_SHARE_READ, FILE_SHARE_WRITE};

        let base_relative = publication_root.relative_path(base_dir)?;
        let capacity = base_relative
            .components()
            .count()
            .saturating_add(relative.components().count())
            .saturating_add(1);
        let mut handles = Vec::with_capacity(capacity);
        handles.push(publication_root.directory.clone());
        let mut guard = ExistingBaseDirectoryGuard::new(handles);
        for component in base_relative.components() {
            let Component::Normal(component) = component else {
                if matches!(component, Component::CurDir) {
                    continue;
                }
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "rename base directory contains an invalid path component",
                ));
            };
            let parent = guard
                .handles
                .last()
                .ok_or_else(|| io::Error::other("Windows publication root guard is empty"))?;
            let child = open_windows_directory_component(parent, component, FILE_OPEN)?;
            guard.handles.push(child);
        }
        for component in relative.components() {
            let Component::Normal(component) = component else {
                continue;
            };
            let parent = guard
                .handles
                .last()
                .ok_or_else(|| io::Error::other("Windows base directory guard is empty"))?;
            let child = open_windows_directory_component(parent, component, FILE_OPEN_IF)?;
            guard.handles.push(child);
        }

        // Windows resolves a handle-relative rename by opening the target for
        // write. Keep every ancestor strict, but let that internal open share
        // the final parent. Delete sharing remains omitted, so the retained
        // directory entry cannot be renamed or removed during publication.
        if guard.handles.len() > 1 {
            let component = dir_path
                .file_name()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "rename destination parent must have a name"))?;
            let parent_index = guard.handles.len() - 2;
            let parent = guard
                .handles
                .get(parent_index)
                .ok_or_else(|| io::Error::other("Windows destination guard lost its parent handle"))?;
            let rename_parent =
                open_windows_relative_directory_component(parent, component, FILE_OPEN, FILE_SHARE_READ | FILE_SHARE_WRITE)?;
            *guard
                .handles
                .last_mut()
                .ok_or_else(|| io::Error::other("Windows destination guard is empty"))? = rename_parent;
        }

        Ok(guard)
    }

    #[cfg(all(not(unix), not(windows)))]
    {
        let _ = (relative, publication_root);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "safe recursive directory creation is unavailable on this platform",
        ))
    }
}

fn warn_reliable_rename_failure(src_file_path: &Path, dst_file_path: &Path, base_dir: &Path, err: &io::Error) {
    warn!(
        "reliable_rename failed. src_file_path: {:?}, dst_file_path: {:?}, base_dir: {:?}, err: {:?}",
        src_file_path, dst_file_path, base_dir, err
    );
}

/// Whether a failed `rename` in [`reliable_rename_inner`] should be retried.
///
/// Only the first failure is retried, and `NotFound` is never retried: the
/// stable parent guard cannot recreate a missing source or base directory, so
/// a second attempt is guaranteed to fail identically. This spares speculative
/// cleanup renames (e.g. `move_to_trash` on an already-removed tmp path) a
/// pointless second syscall. This predicate is shared by the `rename_data`
/// commit path via `rename_all`, so any relaxation here must keep genuine
/// transient errors retryable.
fn should_retry_rename(err: &io::Error, attempt: usize) -> bool {
    attempt == 0 && err.kind() != io::ErrorKind::NotFound
}

pub async fn reliable_mkdir_all(path: impl AsRef<Path>, base_dir: impl AsRef<Path>) -> io::Result<()> {
    let mut i = 0;

    let mut base_dir = base_dir.as_ref();
    loop {
        if let Err(e) = os_mkdir_all(path.as_ref(), base_dir).await {
            if e.kind() == io::ErrorKind::NotFound && i == 0 {
                i += 1;

                if let Some(base_parent) = base_dir.parent()
                    && let Some(c) = base_parent.components().next()
                    && c != Component::RootDir
                {
                    base_dir = base_parent
                }
                continue;
            }

            return Err(e);
        }

        break;
    }

    Ok(())
}

/// Create a directory and all its parent components if they are missing.
/// Without recursion support, fall back to create_dir_all
/// This function will not create directories under base_dir.
#[tracing::instrument(level = "debug", skip_all)]
pub async fn os_mkdir_all(dir_path: impl AsRef<Path>, base_dir: impl AsRef<Path>) -> io::Result<()> {
    if !base_dir.as_ref().to_string_lossy().is_empty() && base_dir.as_ref().starts_with(dir_path.as_ref()) {
        return Ok(());
    }

    if let Err(e) = super::fs::mkdir(dir_path.as_ref()).await {
        if e.kind() == io::ErrorKind::AlreadyExists {
            return Ok(());
        }

        if e.kind() != io::ErrorKind::NotFound {
            return Err(e);
        }

        if let Some(parent) = dir_path.as_ref().parent() {
            // Fall back to creating the missing parent chain only when the direct mkdir proves it is required.
            if let Err(parent_err) = super::fs::make_dir_all(parent).await
                && parent_err.kind() != io::ErrorKind::AlreadyExists
            {
                return Err(parent_err);
            }
        }

        if let Err(retry_err) = super::fs::mkdir(dir_path.as_ref()).await
            && retry_err.kind() != io::ErrorKind::AlreadyExists
        {
            return Err(retry_err);
        }
    }

    Ok(())
}

/// Whether an [`io::Error`] means "the directory is not empty".
///
/// POSIX lets `rmdir`/`rename` report a non-empty directory as either
/// `ENOTEMPTY` or `EEXIST`. Linux uses `ENOTEMPTY` (which Rust surfaces as
/// [`io::ErrorKind::DirectoryNotEmpty`]); illumos/Solaris return `EEXIST`
/// (errno 17), which Rust surfaces as [`io::ErrorKind::AlreadyExists`] and
/// which the `DirectoryNotEmpty` kind therefore never catches. Matching only on
/// the kind silently misclassifies the Solaris case as a hard failure, so
/// callers that must treat a still-populated directory as benign (deleting the
/// object metadata while a rollback-staging dir remains, non-force
/// `DeleteBucket` on a populated bucket) have to test the raw errno as well.
/// Mirrors MinIO's `isSysErrNotEmpty`.
pub fn is_dir_not_empty_error(err: &io::Error) -> bool {
    // Linux/Windows: ENOTEMPTY / ERROR_DIR_NOT_EMPTY -> DirectoryNotEmpty.
    if err.kind() == io::ErrorKind::DirectoryNotEmpty {
        return true;
    }
    // illumos/Solaris report a non-empty `rmdir`/`rename` as EEXIST (errno 17),
    // which Rust surfaces as `AlreadyExists` (so the `DirectoryNotEmpty` kind
    // never catches it). Confirm against the raw errno directly so the
    // classification holds regardless of how the platform std maps it.
    #[cfg(unix)]
    if matches!(err.raw_os_error(), Some(libc::ENOTEMPTY) | Some(libc::EEXIST)) {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_metrics::CapturingRecorder;
    use std::sync::Mutex;
    use std::time::Duration;
    use tempfile::tempdir;
    use tracing_subscriber::fmt::MakeWriter;

    fn file_sync_limiter() -> Arc<Semaphore> {
        Arc::new(Semaphore::new(MAX_PARALLEL_FILE_SYNCS))
    }

    fn test_publication_root(paths: &[&Path]) -> PublicationRoot {
        let mut common = paths
            .first()
            .expect("test publication root requires at least one path")
            .to_path_buf();
        while !paths.iter().all(|path| path.starts_with(&common)) {
            assert!(common.pop(), "test paths must share an absolute root");
        }
        PublicationRoot::new(&common).expect("test publication root should open")
    }

    #[test]
    #[serial_test::serial(file_sync_metrics)]
    fn sync_file_with_put_stage_metric_records_fdatasync_only_when_enabled() {
        let previous_gate = rustfs_io_metrics::put_stage_metrics_enabled();
        rustfs_io_metrics::set_put_stage_metrics_enabled(false);
        let dir = tempdir().expect("temp dir should be created");
        let path = dir.path().join("part.1");
        std::fs::write(&path, b"payload").expect("test file should be written");
        let recorder = CapturingRecorder::default();

        metrics::with_local_recorder(&recorder, || {
            sync_file_with_put_stage_metric(&path).expect("disabled metric sync_file should succeed");
            assert_eq!(
                recorder.histogram_sample_count("rustfs_s3_put_object_stage_duration_ms"),
                0,
                "disabled PUT stage metrics must not emit fdatasync samples"
            );

            rustfs_io_metrics::set_put_stage_metrics_enabled(true);
            sync_file_with_put_stage_metric(&path).expect("enabled metric sync_file should succeed");
            rustfs_io_metrics::set_put_stage_metrics_enabled(false);
        });

        assert_eq!(
            recorder
                .histogram_values(
                    "rustfs_s3_put_object_stage_duration_ms",
                    &[("stage", rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_FILE_FDATASYNC)]
                )
                .len(),
            1,
            "enabled PUT stage metrics must emit one fdatasync sample"
        );
        rustfs_io_metrics::set_put_stage_metrics_enabled(previous_gate);
    }

    async fn rename_all(
        src_file_path: impl AsRef<Path>,
        dst_file_path: impl AsRef<Path>,
        base_dir: impl AsRef<Path>,
    ) -> Result<()> {
        let src_file_path = src_file_path.as_ref();
        let dst_file_path = dst_file_path.as_ref();
        let base_dir = base_dir.as_ref();
        let publication_root = test_publication_root(&[src_file_path, dst_file_path, base_dir]);
        super::rename_all(src_file_path, dst_file_path, base_dir, &publication_root).await
    }

    async fn rename_all_ignore_missing_source(
        src_file_path: impl AsRef<Path>,
        dst_file_path: impl AsRef<Path>,
        base_dir: impl AsRef<Path>,
    ) -> Result<()> {
        let src_file_path = src_file_path.as_ref();
        let dst_file_path = dst_file_path.as_ref();
        let base_dir = base_dir.as_ref();
        let publication_root = test_publication_root(&[src_file_path, dst_file_path, base_dir]);
        super::rename_all_ignore_missing_source(src_file_path, dst_file_path, base_dir, &publication_root).await
    }

    fn mkdir_all_below_existing_base_std(dir_path: &Path, base_dir: &Path) -> io::Result<ExistingBaseDirectoryGuard> {
        let publication_root = test_publication_root(&[dir_path, base_dir]);
        super::mkdir_all_below_existing_base_std(dir_path, base_dir, &publication_root)
    }

    #[tokio::test]
    async fn disk_volume_mutation_lock_is_shared_per_root_and_volume() {
        let temp_dir = tempdir().expect("create temp dir");
        let first = disk_volume_mutation_lock(temp_dir.path(), "bucket");
        let second = disk_volume_mutation_lock(temp_dir.path(), "bucket");
        let other = disk_volume_mutation_lock(temp_dir.path(), "other-bucket");

        assert!(Arc::ptr_eq(&first, &second), "reconnected disks must share a bucket mutation lock");
        assert!(!Arc::ptr_eq(&first, &other), "different buckets must not serialize each other");

        let _write_guard = first.write().await;
        assert!(second.try_read().is_err(), "a bucket delete lock must exclude local commits");
    }

    #[derive(Clone, Default)]
    struct CapturedLogs {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    struct CapturedLogWriter {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    impl CapturedLogs {
        fn contents(&self) -> String {
            let buffer = self
                .buffer
                .lock()
                .expect("captured logs mutex should not be poisoned")
                .clone();
            String::from_utf8(buffer).expect("captured logs should be valid UTF-8")
        }
    }

    impl std::io::Write for CapturedLogWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.buffer
                .lock()
                .expect("captured logs mutex should not be poisoned")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for CapturedLogs {
        type Writer = CapturedLogWriter;

        fn make_writer(&'a self) -> Self::Writer {
            CapturedLogWriter {
                buffer: Arc::clone(&self.buffer),
            }
        }
    }

    /// Holds a `warn_capture()` capture alive: the thread-local subscriber, plus
    /// the pin that keeps tracing's process-global callsite-interest cache from
    /// being decided by some other test's thread.
    struct WarnCaptureGuard {
        _subscriber: tracing::subscriber::DefaultGuard,
        _callsite_pin: tracing::Dispatch,
    }

    /// Capture WARN-level output on the current thread; tokio tests here run on
    /// the current-thread runtime, so the guard covers the whole test body.
    ///
    /// The callsite pin matters because `warn_reliable_rename_failure` is a
    /// single production callsite shared with tests that call `rename_all`
    /// *without* installing a subscriber — `rename_all_missing_source_returns_file_not_found`
    /// is one. Whichever thread reaches it first fixes its `Interest`
    /// process-wide, so without the pin that sibling can cache
    /// `Interest::never()` and the WARN never fires here at all, leaving the
    /// "must keep the WARN" assertions staring at empty output. See
    /// [`crate::test_tracing::pin_callsite_interest_for_test`].
    fn warn_capture() -> (CapturedLogs, WarnCaptureGuard) {
        let logs = CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_writer(logs.clone())
            .with_ansi(false)
            .without_time()
            .finish();
        let guard = WarnCaptureGuard {
            _subscriber: tracing::subscriber::set_default(subscriber),
            _callsite_pin: crate::test_tracing::pin_callsite_interest_for_test(),
        };
        (logs, guard)
    }

    #[cfg(windows)]
    fn try_set_windows_mount_point(directory: &winapi_util::Handle, target: &Path) -> io::Result<()> {
        use std::os::windows::ffi::OsStrExt;

        const VERBATIM_PREFIX: [u16; 4] = [b'\\' as u16, b'\\' as u16, b'?' as u16, b'\\' as u16];
        const NT_PREFIX: [u16; 4] = [b'\\' as u16, b'?' as u16, b'?' as u16, b'\\' as u16];

        let target = std::fs::canonicalize(target)?;
        let target_name = target.as_os_str().encode_wide().collect::<Vec<_>>();
        let target_without_prefix = target_name.strip_prefix(&VERBATIM_PREFIX).unwrap_or(&target_name);
        let substitute_name = NT_PREFIX
            .into_iter()
            .chain(target_without_prefix.iter().copied())
            .collect::<Vec<_>>();
        try_set_windows_mount_point_names(directory, &substitute_name, &target_name)
    }

    #[cfg(windows)]
    // SAFETY: this test helper passes a valid live directory handle and a
    // fully initialized mount-point reparse buffer to synchronous DeviceIoControl.
    #[allow(unsafe_code)]
    fn try_set_windows_mount_point_names(
        directory: &winapi_util::Handle,
        substitute_name: &[u16],
        print_name: &[u16],
    ) -> io::Result<()> {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::System::{
            IO::DeviceIoControl, Ioctl::FSCTL_SET_REPARSE_POINT, SystemServices::IO_REPARSE_TAG_MOUNT_POINT,
        };

        const REPARSE_HEADER_SIZE: usize = 8;
        const MOUNT_POINT_HEADER_SIZE: usize = 8;

        let substitute_name_bytes = substitute_name
            .len()
            .checked_mul(std::mem::size_of::<u16>())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "mount-point target is too long"))?;
        let print_name_bytes = print_name
            .len()
            .checked_mul(std::mem::size_of::<u16>())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "mount-point target is too long"))?;
        let substitute_name_length = u16::try_from(substitute_name_bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "mount-point target is too long"))?;
        let print_name_offset = u16::try_from(substitute_name_bytes + std::mem::size_of::<u16>())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "mount-point target is too long"))?;
        let print_name_length = u16::try_from(print_name_bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "mount-point target is too long"))?;

        let mut path_buffer = Vec::with_capacity(substitute_name_bytes + print_name_bytes + 2 * std::mem::size_of::<u16>());
        for unit in substitute_name
            .iter()
            .copied()
            .chain([0])
            .chain(print_name.iter().copied())
            .chain([0])
        {
            path_buffer.extend_from_slice(&unit.to_le_bytes());
        }
        let reparse_data_length = u16::try_from(MOUNT_POINT_HEADER_SIZE + path_buffer.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "mount-point reparse buffer is too large"))?;
        let mut buffer = Vec::with_capacity(REPARSE_HEADER_SIZE + usize::from(reparse_data_length));
        buffer.extend_from_slice(&IO_REPARSE_TAG_MOUNT_POINT.to_le_bytes());
        buffer.extend_from_slice(&reparse_data_length.to_le_bytes());
        buffer.extend_from_slice(&0u16.to_le_bytes());
        buffer.extend_from_slice(&0u16.to_le_bytes());
        buffer.extend_from_slice(&substitute_name_length.to_le_bytes());
        buffer.extend_from_slice(&print_name_offset.to_le_bytes());
        buffer.extend_from_slice(&print_name_length.to_le_bytes());
        buffer.extend_from_slice(&path_buffer);
        let input_size = u32::try_from(buffer.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "mount-point reparse buffer is too large"))?;
        let mut bytes_returned = 0;

        // SAFETY: `directory` and `buffer` remain live for the synchronous
        // call, and the buffer lengths above match REPARSE_DATA_BUFFER layout.
        let changed = unsafe {
            DeviceIoControl(
                directory.as_raw_handle(),
                FSCTL_SET_REPARSE_POINT,
                buffer.as_ptr().cast(),
                input_size,
                std::ptr::null_mut(),
                0,
                &mut bytes_returned,
                std::ptr::null_mut(),
            )
        };
        if changed == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    #[cfg(windows)]
    // SAFETY: this test helper passes a valid live directory handle and the
    // documented mount-point delete header to synchronous DeviceIoControl.
    #[allow(unsafe_code)]
    fn try_delete_windows_mount_point(directory: &winapi_util::Handle) -> io::Result<()> {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::System::{
            IO::DeviceIoControl, Ioctl::FSCTL_DELETE_REPARSE_POINT, SystemServices::IO_REPARSE_TAG_MOUNT_POINT,
        };

        let mut buffer = [0u8; 8];
        buffer[..4].copy_from_slice(&IO_REPARSE_TAG_MOUNT_POINT.to_le_bytes());
        let mut bytes_returned = 0;
        // SAFETY: `directory` and `buffer` remain live for the synchronous
        // call, and the eight-byte input is the documented delete header.
        let changed = unsafe {
            DeviceIoControl(
                directory.as_raw_handle(),
                FSCTL_DELETE_REPARSE_POINT,
                buffer.as_ptr().cast(),
                u32::try_from(buffer.len()).expect("reparse delete header length fits in u32"),
                std::ptr::null_mut(),
                0,
                &mut bytes_returned,
                std::ptr::null_mut(),
            )
        };
        if changed == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    #[cfg(unix)]
    #[test]
    fn read_dir_probe_bounds_unsupported_entries() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempdir().expect("create temp dir");
        for index in 0..=READ_DIR_PROBE_RAW_LIMIT {
            symlink("missing", temp_dir.path().join(format!("ignored-link-{index:04}"))).expect("create symlink");
        }

        let probe = read_dir_probe(temp_dir.path(), 1).expect("probe directory");

        assert!(probe.entries.is_empty());
        assert!(!probe.complete, "a bounded probe must not claim that an oversized directory is complete");
    }

    #[test]
    fn global_file_sync_limit_scales_and_preserves_blocking_capacity() {
        assert_eq!(default_global_file_sync_limit(1, 1024), MIN_GLOBAL_FILE_SYNCS);
        assert_eq!(default_global_file_sync_limit(16, 1024), 256);
        assert_eq!(default_global_file_sync_limit(64, 1024), MAX_GLOBAL_FILE_SYNCS);
        assert_eq!(default_global_file_sync_limit(64, 128), 64);
        assert_eq!(default_global_file_sync_limit(0, 0), 1);
    }

    #[tokio::test]
    async fn rename_all_missing_source_returns_file_not_found() {
        let temp_dir = tempdir().expect("create temp dir");
        let src = temp_dir.path().join("missing");
        let dst = temp_dir.path().join("dst");

        let err = rename_all(&src, &dst, temp_dir.path())
            .await
            .expect_err("missing source must fail");

        assert!(matches!(err, DiskError::FileNotFound));
        assert!(!dst.exists());
    }

    #[tokio::test]
    async fn rename_all_ignore_missing_source_returns_ok_without_warn() {
        let temp_dir = tempdir().expect("create temp dir");
        let src = temp_dir.path().join("missing");
        let dst = temp_dir.path().join("dst");

        let (logs, _guard) = warn_capture();
        rename_all_ignore_missing_source(&src, &dst, temp_dir.path())
            .await
            .expect("missing cleanup source must be ignored");

        assert!(!dst.exists());
        assert!(!logs.contents().contains("reliable_rename failed"));
    }

    #[tokio::test]
    async fn rename_all_ignore_missing_source_preserves_a_source_when_the_destination_base_is_missing() {
        let temp_dir = tempdir().expect("create temp dir");
        let src = temp_dir.path().join("source");
        let base = temp_dir.path().join("missing-base");
        let dst = base.join("destination");
        std::fs::write(&src, b"payload").expect("write source");

        let err = rename_all_ignore_missing_source(&src, &dst, &base)
            .await
            .expect_err("a missing destination base must not masquerade as a missing source");

        assert!(matches!(err, DiskError::FileNotFound));
        assert_eq!(std::fs::read(&src).expect("source must remain readable"), b"payload");
        assert!(!base.exists());
    }

    #[tokio::test]
    async fn rename_all_missing_source_still_warns() {
        let temp_dir = tempdir().expect("create temp dir");
        let src = temp_dir.path().join("missing");
        let dst = temp_dir.path().join("dst");

        let (logs, _guard) = warn_capture();
        let err = rename_all(&src, &dst, temp_dir.path())
            .await
            .expect_err("missing source must still fail");

        assert!(matches!(err, DiskError::FileNotFound));
        let captured = logs.contents();
        assert!(
            captured.contains("reliable_rename failed"),
            "ordinary missing-source failures must keep the WARN, got: {captured}"
        );
    }

    #[tokio::test]
    async fn rename_all_real_failure_still_warns() {
        // Renaming a file onto an existing directory fails on every platform
        // with a non-NotFound error; genuine failures must keep the WARN.
        let temp_dir = tempdir().expect("create temp dir");
        let src = temp_dir.path().join("src");
        std::fs::write(&src, b"payload").expect("write src");
        let dst = temp_dir.path().join("dst-dir");
        std::fs::create_dir(&dst).expect("create dst dir");

        let (logs, _guard) = warn_capture();
        rename_all(&src, &dst, temp_dir.path())
            .await
            .expect_err("rename onto an existing directory must fail");

        let captured = logs.contents();
        assert!(
            captured.contains("reliable_rename failed"),
            "genuine rename failure must keep the WARN, got: {captured}"
        );
    }

    #[test]
    fn rename_retry_never_retries_not_found() {
        // NotFound is terminal for the retry loop: the retry does not recreate
        // the missing source/base, so a second rename would fail identically.
        let not_found = io::Error::new(io::ErrorKind::NotFound, "missing");
        assert!(!should_retry_rename(&not_found, 0));
        assert!(!should_retry_rename(&not_found, 1));
    }

    #[test]
    fn rename_retry_allows_single_retry_for_other_errors() {
        let denied = io::Error::new(io::ErrorKind::PermissionDenied, "denied");
        assert!(should_retry_rename(&denied, 0));
        assert!(!should_retry_rename(&denied, 1));
    }

    #[test]
    fn is_dir_not_empty_error_recognizes_directory_not_empty_kind() {
        let err = io::Error::from(io::ErrorKind::DirectoryNotEmpty);
        assert!(is_dir_not_empty_error(&err));
    }

    #[cfg(unix)]
    #[test]
    fn is_dir_not_empty_error_recognizes_raw_enotempty() {
        // Linux/BSD/macOS non-empty rmdir/rename errno.
        let err = io::Error::from_raw_os_error(libc::ENOTEMPTY);
        assert!(is_dir_not_empty_error(&err));
    }

    #[cfg(unix)]
    #[test]
    fn is_dir_not_empty_error_recognizes_solaris_eexist() {
        // illumos/Solaris report a non-empty rmdir/rename as EEXIST, which Rust
        // surfaces as `AlreadyExists` (never `DirectoryNotEmpty`). This is the
        // core of rustfs/rustfs#4978: matching only the kind misclassified this
        // benign condition as a hard failure.
        let err = io::Error::from_raw_os_error(libc::EEXIST);
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        assert!(is_dir_not_empty_error(&err));
    }

    #[test]
    fn is_dir_not_empty_error_rejects_unrelated_errors() {
        assert!(!is_dir_not_empty_error(&io::Error::from(io::ErrorKind::NotFound)));
        assert!(!is_dir_not_empty_error(&io::Error::from(io::ErrorKind::PermissionDenied)));
        #[cfg(unix)]
        {
            assert!(!is_dir_not_empty_error(&io::Error::from_raw_os_error(libc::EACCES)));
            assert!(!is_dir_not_empty_error(&io::Error::from_raw_os_error(libc::ENOENT)));
        }
    }

    #[tokio::test]
    async fn is_dir_not_empty_error_matches_real_non_empty_rmdir() {
        // Validate against the host's actual errno, whatever it is: Linux/macOS
        // return ENOTEMPTY, illumos/Solaris return EEXIST. The removal must be
        // classified as "not empty" on every platform.
        let temp_dir = tempdir().expect("create temp dir");
        let populated = temp_dir.path().join("populated");
        std::fs::create_dir(&populated).expect("create dir");
        std::fs::write(populated.join("child"), b"x").expect("write child");

        let err = std::fs::remove_dir(&populated).expect_err("non-empty rmdir must fail");
        assert!(
            is_dir_not_empty_error(&err),
            "non-empty rmdir must classify as not-empty, got kind {:?} errno {:?}",
            err.kind(),
            err.raw_os_error()
        );
    }

    #[tokio::test]
    async fn rename_all_moves_existing_directory_tree() {
        // Guards the rename_data commit path, which funnels through
        // reliable_rename_inner via rename_all.
        let temp_dir = tempdir().expect("create temp dir");
        let src = temp_dir.path().join("src-dir");
        std::fs::create_dir_all(src.join("nested")).expect("create src tree");
        std::fs::write(src.join("nested").join("part.1"), b"payload").expect("write part");
        let dst = temp_dir.path().join("dst-parent").join("dst-dir");

        rename_all(&src, &dst, temp_dir.path()).await.expect("rename must succeed");

        assert!(!src.exists());
        assert_eq!(std::fs::read(dst.join("nested").join("part.1")).expect("read moved part"), b"payload");
    }

    #[tokio::test]
    async fn rename_all_does_not_recreate_missing_base_directory() {
        let temp_dir = tempdir().expect("create temp dir");
        let base = temp_dir.path().join("bucket");
        std::fs::create_dir(&base).expect("create destination base");
        let src = temp_dir.path().join("staged-object");
        std::fs::write(&src, b"payload").expect("write staged object");
        let dst = base.join("object").join("xl.meta");
        std::fs::remove_dir(&base).expect("delete destination base before commit");

        let err = rename_all(&src, &dst, &base)
            .await
            .expect_err("rename must not recreate a deleted destination base");

        assert!(matches!(err, DiskError::FileNotFound));
        assert!(src.exists(), "failed commit must preserve the staged source");
        assert!(!base.exists(), "failed commit must not recreate the deleted bucket");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rename_all_rejects_a_replaced_base_with_an_existing_parent() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempdir().expect("create temp dir");
        let base = temp_dir.path().join("bucket");
        let outside = temp_dir.path().join("outside");
        std::fs::create_dir(&base).expect("create destination base");
        std::fs::create_dir_all(outside.join("object")).expect("create outside destination parent");
        let src = temp_dir.path().join("staged-object");
        std::fs::write(&src, b"payload").expect("write staged object");
        let dst = base.join("object").join("xl.meta");

        std::fs::remove_dir(&base).expect("remove destination base before replacement");
        symlink(&outside, &base).expect("replace destination base with a symlink");

        rename_all(&src, &dst, &base)
            .await
            .expect_err("rename must reject an existing destination parent below a replaced base");

        assert!(src.exists(), "rejected rename must preserve the staged source");
        assert!(
            !outside.join("object/xl.meta").exists(),
            "rename must not publish through the replacement symlink"
        );
    }

    #[cfg(windows)]
    #[test]
    fn windows_parent_guard_blocks_parent_replacement() {
        let temp_dir = tempdir().expect("create temp dir");
        let base = temp_dir.path().join("bucket");
        std::fs::create_dir(&base).expect("create destination base");
        let parent = base.join("object").join("nested");
        let guard = mkdir_all_below_existing_base_std(&parent, &base).expect("create and lock destination parents");

        std::fs::read_dir(&parent).expect("the locked parent must remain readable");
        std::fs::rename(&base, temp_dir.path().join("replacement-base"))
            .expect_err("the locked base must not be replaceable before commit");
        std::fs::rename(base.join("object"), base.join("replacement-object"))
            .expect_err("a locked intermediate directory must not be replaceable before commit");
        std::fs::rename(&parent, base.join("replacement-parent"))
            .expect_err("the locked destination parent must not be replaceable before commit");
        assert!(parent.is_dir(), "failed replacement must leave the guarded parent in place");

        drop(guard);
        std::fs::rename(base.join("object"), base.join("replacement-object"))
            .expect("replacement should succeed after the commit guard is released");
    }

    #[cfg(windows)]
    #[test]
    fn windows_publication_root_keeps_normal_root_writes_available() {
        let temp_dir = tempdir().expect("create temp dir");
        let publication_root = PublicationRoot::new(temp_dir.path()).expect("open publication root");

        let bucket = temp_dir.path().join("bucket-created-after-root-open");
        std::fs::create_dir(&bucket).expect("root handle must not block normal bucket creation");
        std::fs::write(bucket.join("marker"), b"payload").expect("root handle must not block normal writes");

        drop(publication_root);
        assert_eq!(std::fs::read(bucket.join("marker")).expect("read marker"), b"payload");
    }

    #[cfg(windows)]
    #[test]
    fn windows_rename_all_publication_root_excludes_delete_sharing() {
        let temp_dir = tempdir().expect("create temp dir");
        let root = temp_dir.path().join("publication-root");
        let replacement = temp_dir.path().join("replacement-root");
        std::fs::create_dir(&root).expect("create publication root");
        let publication_root = PublicationRoot::new(&root).expect("open publication root");

        std::fs::rename(&root, &replacement).expect_err("the live publication root must not be replaceable");
        assert!(root.is_dir(), "failed replacement must retain the configured root");

        drop(publication_root);
        std::fs::rename(&root, &replacement).expect("replacement should succeed after the root handle is released");
    }

    #[cfg(windows)]
    #[test]
    fn windows_final_path_falls_back_from_dos_to_guid_and_nt_paths() {
        use windows_sys::Win32::Foundation::{ERROR_ACCESS_DENIED, ERROR_PATH_NOT_FOUND};

        let path_not_found =
            || io::Error::from_raw_os_error(i32::try_from(ERROR_PATH_NOT_FOUND).expect("Windows error code should fit i32"));
        let guid_path = PathBuf::from(r"\\?\Volume{11111111-2222-3333-4444-555555555555}\data");
        let resolved = windows_final_path_with_fallbacks(
            Err(path_not_found()),
            || Ok(guid_path.clone()),
            || panic!("a successful GUID lookup must not query the NT path"),
        )
        .expect("a volume without a DOS name should use its GUID path");
        assert_eq!(resolved, guid_path);

        let nt_path = PathBuf::from(r"\\?\GLOBALROOT\Device\HarddiskVolume42\data");
        let resolved = windows_final_path_with_fallbacks(Err(path_not_found()), || Err(path_not_found()), || Ok(nt_path.clone()))
            .expect("a volume without Mount Manager names should use its NT path");
        assert_eq!(resolved, nt_path);

        let access_denied = i32::try_from(ERROR_ACCESS_DENIED).expect("Windows error code should fit i32");
        let err = windows_final_path_with_fallbacks(
            Err(io::Error::from_raw_os_error(access_denied)),
            || panic!("non-path errors must not be hidden by a GUID retry"),
            || panic!("non-path errors must not be hidden by an NT retry"),
        )
        .expect_err("a non-path error should be preserved");
        assert_eq!(err.raw_os_error(), Some(access_denied));
    }

    #[cfg(windows)]
    #[test]
    fn windows_rename_all_queries_a_real_volume_guid_path() {
        use windows_sys::Win32::Storage::FileSystem::{FILE_NAME_NORMALIZED, VOLUME_NAME_GUID, VOLUME_NAME_NT};

        let temp_dir = tempdir().expect("create temp dir");
        let root = temp_dir.path().join("publication-root");
        std::fs::create_dir(&root).expect("create publication root");
        let publication_root = PublicationRoot::new(&root).expect("open publication root");
        let guid_path =
            windows_final_path_with_flags(publication_root.directory.handle.as_ref(), FILE_NAME_NORMALIZED | VOLUME_NAME_GUID)
                .expect("query the root through its real volume GUID path");

        assert!(guid_path.is_absolute(), "the volume GUID result must be absolute");
        assert!(std::fs::metadata(guid_path).expect("stat the volume GUID path").is_dir());

        let nt_path =
            windows_final_path_with_flags(publication_root.directory.handle.as_ref(), FILE_NAME_NORMALIZED | VOLUME_NAME_NT)
                .and_then(windows_nt_path_to_global_root)
                .expect("query the root through its real NT path");
        assert!(nt_path.is_absolute(), "the GLOBALROOT result must be absolute");
        assert!(std::fs::metadata(nt_path).expect("stat the GLOBALROOT NT path").is_dir());
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_publication_root_follows_a_configured_junction_once() {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::{
            Foundation::GENERIC_WRITE,
            Storage::FileSystem::{FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ, FILE_SHARE_WRITE},
        };

        let temp_dir = tempdir().expect("create temp dir");
        let target = temp_dir.path().join("target");
        let mount = temp_dir.path().join("configured-root");
        std::fs::create_dir(&target).expect("create configured target");
        std::fs::create_dir(&mount).expect("create configured mount point");
        let mount_writer = std::fs::OpenOptions::new()
            .access_mode(GENERIC_WRITE)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
            .open(&mount)
            .map(winapi_util::Handle::from_file)
            .expect("open configured mount point");
        try_set_windows_mount_point(&mount_writer, &target).expect("configure root junction");
        drop(mount_writer);

        let publication_root = PublicationRoot::new(&mount).expect("configured root junction should be followed once");
        let resolved_root = publication_root.path().to_path_buf();
        assert_eq!(
            resolved_root,
            rustfs_utils::canonicalize(&target).expect("canonicalize configured target")
        );
        let configured_base = mount.join("configured-bucket");
        let configured_src = mount.join("configured-staging");
        let configured_dst = configured_base.join("object");
        std::fs::create_dir(&configured_base).expect("create bucket through configured root");
        std::fs::write(&configured_src, b"configured").expect("write staged object through configured root");
        super::rename_all(&configured_src, &configured_dst, &configured_base, &publication_root)
            .await
            .expect("publish a configured path relative to the pinned root");
        assert_eq!(
            std::fs::read(target.join("configured-bucket/object")).expect("read configured-path publication"),
            b"configured"
        );

        let resolved_base = resolved_root.join("resolved-bucket");
        let resolved_src = resolved_root.join("resolved-staging");
        let resolved_dst = resolved_base.join("object");
        std::fs::create_dir(&resolved_base).expect("create bucket through resolved root");
        std::fs::write(&resolved_src, b"resolved").expect("write staged object through resolved root");
        super::rename_all(&resolved_src, &resolved_dst, &resolved_base, &publication_root)
            .await
            .expect("publish a resolved path relative to the pinned root");

        assert_eq!(
            std::fs::read(target.join("resolved-bucket/object")).expect("read resolved-path publication"),
            b"resolved"
        );
        drop(publication_root);
        let mount_writer = std::fs::OpenOptions::new()
            .access_mode(GENERIC_WRITE)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
            .open(&mount)
            .map(winapi_util::Handle::from_file)
            .expect("reopen configured mount point for cleanup");
        try_delete_windows_mount_point(&mount_writer).expect("remove configured root junction");
    }

    #[cfg(windows)]
    #[test]
    fn windows_rename_source_reparse_policy_only_allows_data_dedup() {
        use windows_sys::Win32::{
            Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT,
            System::SystemServices::{IO_REPARSE_TAG_DEDUP, IO_REPARSE_TAG_MOUNT_POINT, IO_REPARSE_TAG_SYMLINK},
        };

        assert!(windows_rename_source_is_allowed(0, 0));
        assert!(windows_rename_source_is_allowed(FILE_ATTRIBUTE_REPARSE_POINT, IO_REPARSE_TAG_DEDUP));
        assert!(!windows_rename_source_is_allowed(
            FILE_ATTRIBUTE_REPARSE_POINT,
            IO_REPARSE_TAG_MOUNT_POINT
        ));
        assert!(!windows_rename_source_is_allowed(FILE_ATTRIBUTE_REPARSE_POINT, IO_REPARSE_TAG_SYMLINK));
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_rename_all_publishes_a_direct_child_with_a_short_name() {
        let temp_dir = tempdir().expect("create temp dir");
        let base = temp_dir.path().join("bucket");
        std::fs::create_dir(&base).expect("create destination base");
        let src = temp_dir.path().join("staged-object");
        let dst = base.join("x");
        std::fs::write(&src, b"payload").expect("write staged object");

        rename_all(&src, &dst, &base).await.expect("direct-child rename must succeed");

        assert!(!src.exists());
        assert_eq!(std::fs::read(&dst).expect("read published object"), b"payload");
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_rename_commit_guard_reuses_source_and_destination_trees() {
        let temp_dir = tempdir().expect("create temp dir");
        let source_parent = temp_dir.path().join("staging/object");
        let destination_base = temp_dir.path().join("bucket");
        let destination_parent = destination_base.join("object");
        std::fs::create_dir_all(&source_parent).expect("create staging parent");
        std::fs::create_dir(&destination_base).expect("create destination base");
        let publication_root = PublicationRoot::new(temp_dir.path()).expect("open publication root");
        let commit_guard = prepare_rename_commit_guard(&source_parent, &destination_parent, &destination_base, &publication_root)
            .expect("prepare shared commit guard");
        let mutation_lease = acquire_namespace_mutation_lease(&destination_parent).await;

        let first_src = source_parent.join("part.1");
        let second_src = source_parent.join("xl.meta");
        let first_dst = destination_parent.join("part.1");
        let second_dst = destination_parent.join("xl.meta");
        std::fs::write(&first_src, b"part").expect("write staged part");
        std::fs::write(&second_src, b"meta").expect("write staged metadata");
        windows_rename_test_hooks::observe_guard_generations(&first_dst);
        windows_rename_test_hooks::observe_guard_generations(&second_dst);

        super::rename_all_with_commit_guard(
            &first_src,
            &first_dst,
            &destination_base,
            &publication_root,
            &commit_guard,
            mutation_lease.clone(),
        )
        .await
        .expect("publish first entry with shared guards");
        super::rename_all_with_commit_guard(
            &second_src,
            &second_dst,
            &destination_base,
            &publication_root,
            &commit_guard,
            mutation_lease,
        )
        .await
        .expect("publish second entry with shared guards");

        let first_generation = windows_rename_test_hooks::take_guard_generations(&first_dst);
        let second_generation = windows_rename_test_hooks::take_guard_generations(&second_dst);
        assert_eq!(first_generation.len(), 1);
        assert_eq!(second_generation, first_generation);
        std::fs::rename(&source_parent, temp_dir.path().join("replacement-staging"))
            .expect_err("the retained source parent must not be replaceable");
        std::fs::rename(&destination_parent, destination_base.join("replacement-object"))
            .expect_err("the retained destination parent must not be replaceable");

        drop(commit_guard);
        std::fs::rename(&source_parent, temp_dir.path().join("replacement-staging"))
            .expect("source replacement should succeed after guard release");
        std::fs::rename(&destination_parent, destination_base.join("replacement-object"))
            .expect("destination replacement should succeed after guard release");
    }

    #[cfg(windows)]
    #[test]
    fn windows_rename_commit_guard_publishes_data_directory_with_prepared_metadata() {
        let temp_dir = tempdir().expect("create temp dir");
        let configured_root = temp_dir.path();
        let publication_root = PublicationRoot::new(configured_root).expect("open publication root");
        let root = publication_root.path();
        let source_parent = root.join(".rustfs.sys/tmp/staged-object");
        let destination_base = root.join("bucket");
        let destination_parent = destination_base.join("object");
        let source_data = source_parent.join("data-dir");
        let destination_data = destination_parent.join("data-dir");
        std::fs::create_dir_all(&source_data).expect("create staged data directory");
        std::fs::write(source_data.join("part.1"), b"payload").expect("write staged part");
        std::fs::create_dir(&destination_base).expect("create destination bucket");

        let commit_guard = prepare_rename_commit_guard(&source_parent, &destination_parent, &destination_base, &publication_root)
            .expect("prepare shared commit guard");
        let source_metadata = source_parent.join("xl.meta");
        let destination_metadata = destination_parent.join("xl.meta");
        let mut prepared_metadata =
            create_prepared_rename_source_with_commit_guard(&source_metadata, &destination_metadata, &commit_guard)
                .expect("prepare staged metadata");
        prepared_metadata
            .write_all(b"metadata", false)
            .expect("write staged metadata");

        rename_with_commit_guard_std(&source_data, &destination_data, &commit_guard)
            .expect("publish staged data directory while metadata source remains open");
        rename_prepared_source_with_commit_guard_std(&prepared_metadata, &source_metadata, &destination_metadata, &commit_guard)
            .expect("publish prepared metadata");

        assert_eq!(std::fs::read(destination_data.join("part.1")).expect("read published part"), b"payload");
        assert_eq!(std::fs::read(destination_metadata).expect("read published metadata"), b"metadata");
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_rename_commit_guard_shares_a_same_parent_handle() {
        let temp_dir = tempdir().expect("create temp dir");
        let destination_base = temp_dir.path().join("bucket");
        let parent = destination_base.join("object");
        let src = parent.join("staged-xl.meta");
        let dst = parent.join("xl.meta");
        std::fs::create_dir_all(&parent).expect("create shared parent");
        std::fs::write(&src, b"metadata").expect("write staged metadata");
        let publication_root = PublicationRoot::new(temp_dir.path()).expect("open publication root");
        let commit_guard = prepare_rename_commit_guard(&parent, &parent, &destination_base, &publication_root)
            .expect("prepare same-parent commit guard");
        let mutation_lease = acquire_namespace_mutation_lease(&parent).await;
        assert_eq!(
            commit_guard.source_parent_guard.generation, commit_guard.destination_parent_guard.generation,
            "same-parent publication must reuse one guarded directory identity"
        );

        super::rename_all_with_commit_guard(&src, &dst, &destination_base, &publication_root, &commit_guard, mutation_lease)
            .await
            .expect("same-parent commit rename must not conflict with its own guard");

        assert!(!src.exists());
        assert_eq!(std::fs::read(dst).expect("read committed metadata"), b"metadata");
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_rename_all_supports_same_parent_publication() {
        let temp_dir = tempdir().expect("create temp dir");
        let parent = temp_dir.path().join("metadata");
        let src = parent.join("temporary-format.json");
        let dst = parent.join("format.json");
        std::fs::create_dir(&parent).expect("create metadata directory");
        std::fs::write(&src, b"format").expect("write temporary format");

        rename_all(&src, &dst, &parent)
            .await
            .expect("same-parent reliable rename must not conflict with its source guard");

        assert!(!src.exists());
        assert_eq!(std::fs::read(dst).expect("read published format"), b"format");
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_rename_all_supports_child_to_parent_rollback_publication() {
        let temp_dir = tempdir().expect("create temp dir");
        let object_dir = temp_dir.path().join("bucket/object");
        let rollback_dir = object_dir.join("rollback-id");
        let src = rollback_dir.join("xl.meta.backup");
        let dst = object_dir.join("xl.meta");
        std::fs::create_dir_all(&rollback_dir).expect("create rollback directory");
        std::fs::write(&src, b"old-metadata").expect("write rollback metadata");
        std::fs::write(&dst, b"uncommitted-metadata").expect("write metadata to replace");
        let publication_root = PublicationRoot::new(temp_dir.path()).expect("open publication root above the object tree");

        super::rename_all(&src, &dst, &object_dir, &publication_root)
            .await
            .expect("a rollback source below its destination parent must not conflict with its own guards");

        assert!(!src.exists());
        assert_eq!(std::fs::read(dst).expect("read restored metadata"), b"old-metadata");
    }

    #[cfg(windows)]
    #[test]
    fn windows_rename_all_rejects_unavailable_modern_file_ids() {
        assert!(!super::windows_file_id_is_available(&[0; 16]));
        assert!(!super::windows_file_id_is_available(&[u8::MAX; 16]));
        assert!(!super::windows_file_id_is_available(&[0; 8]));
        assert!(!super::windows_file_id_is_available(&[u8::MAX; 8]));

        let mut available = [0; 16];
        available[0] = 1;
        assert!(super::windows_file_id_is_available(&available));
    }

    #[cfg(windows)]
    #[test]
    fn windows_rename_all_retries_without_unsupported_readonly_flag() {
        use windows_sys::{
            Wdk::Storage::FileSystem::{
                FILE_RENAME_IGNORE_READONLY_ATTRIBUTE, FILE_RENAME_POSIX_SEMANTICS, FILE_RENAME_REPLACE_IF_EXISTS,
            },
            Win32::Foundation::{ERROR_ACCESS_DENIED, ERROR_DISK_FULL, ERROR_INVALID_PARAMETER},
        };

        let invalid_parameter = i32::try_from(ERROR_INVALID_PARAMETER).expect("Windows error code should fit i32");
        let access_denied = i32::try_from(ERROR_ACCESS_DENIED).expect("Windows error code should fit i32");
        let disk_full = i32::try_from(ERROR_DISK_FULL).expect("Windows error code should fit i32");
        let mut attempts = Vec::new();
        windows_extended_rename_with_compatibility_fallback(io::Error::from_raw_os_error(access_denied), |flags| {
            attempts.push(flags);
            if attempts.len() == 1 {
                Err(io::Error::from_raw_os_error(invalid_parameter))
            } else {
                Ok(())
            }
        })
        .expect("unsupported optional flags should use the compatible fallback");
        assert_eq!(
            attempts,
            vec![
                FILE_RENAME_REPLACE_IF_EXISTS | FILE_RENAME_POSIX_SEMANTICS | FILE_RENAME_IGNORE_READONLY_ATTRIBUTE,
                FILE_RENAME_REPLACE_IF_EXISTS | FILE_RENAME_POSIX_SEMANTICS,
            ]
        );

        let mut attempts = 0;
        let err = windows_extended_rename_with_compatibility_fallback(io::Error::from_raw_os_error(invalid_parameter), |_| {
            attempts += 1;
            Err(io::Error::from_raw_os_error(access_denied))
        })
        .expect_err("ordinary rename failures must not be retried with weaker flags");
        assert_eq!(attempts, 1);
        assert_eq!(err.raw_os_error(), Some(access_denied));

        let mut attempts = 0;
        let err = windows_extended_rename_with_compatibility_fallback(io::Error::from_raw_os_error(access_denied), |_| {
            attempts += 1;
            Err(io::Error::from_raw_os_error(invalid_parameter))
        })
        .expect_err("an unsupported extended rename must preserve the legacy error");
        assert_eq!(attempts, 2);
        assert_eq!(err.raw_os_error(), Some(access_denied));

        let mut attempts = 0;
        let err = windows_extended_rename_with_compatibility_fallback(io::Error::from_raw_os_error(access_denied), |_| {
            attempts += 1;
            if attempts == 1 {
                Err(io::Error::from_raw_os_error(invalid_parameter))
            } else {
                Err(io::Error::from_raw_os_error(disk_full))
            }
        })
        .expect_err("a supported extended rename failure must replace the stale legacy error");
        assert_eq!(attempts, 2);
        assert_eq!(err.raw_os_error(), Some(disk_full));
    }

    #[cfg(windows)]
    #[test]
    fn windows_rename_all_legacy_file_identity_distinguishes_sources() {
        let temp_dir = tempdir().expect("create temp dir");
        let first_path = temp_dir.path().join("first");
        let second_path = temp_dir.path().join("second");
        std::fs::write(&first_path, b"first").expect("write first source");
        std::fs::write(&second_path, b"second").expect("write second source");
        let first = winapi_util::Handle::from_file(std::fs::File::open(first_path).expect("open first source"));
        let second = winapi_util::Handle::from_file(std::fs::File::open(second_path).expect("open second source"));

        let first_identity = windows_legacy_file_identity(&first).expect("query first legacy identity");
        assert_eq!(
            windows_legacy_file_identity(&first).expect("repeat first legacy identity"),
            first_identity
        );
        assert_ne!(
            windows_legacy_file_identity(&second).expect("query second legacy identity"),
            first_identity
        );
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_rename_all_replaces_a_read_only_destination() {
        let temp_dir = tempdir().expect("create temp dir");
        let base = temp_dir.path().join("bucket");
        std::fs::create_dir(&base).expect("create destination base");
        let src = temp_dir.path().join("staged-object");
        let dst = base.join("xl.meta");
        std::fs::write(&src, b"new").expect("write staged object");
        std::fs::write(&dst, b"old").expect("write old destination");
        let mut permissions = std::fs::metadata(&dst).expect("inspect old destination").permissions();
        permissions.set_readonly(true);
        std::fs::set_permissions(&dst, permissions).expect("make old destination read-only");

        let result = rename_all(&src, &dst, &base).await;
        if result.is_err() && dst.exists() {
            let mut permissions = std::fs::metadata(&dst).expect("inspect failed destination").permissions();
            permissions.set_readonly(false);
            std::fs::set_permissions(&dst, permissions).expect("restore failed destination permissions");
        }
        result.expect("read-only destination replacement must match std::fs::rename");

        assert_eq!(std::fs::read(&dst).expect("read replacement"), b"new");
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_rename_all_replaces_an_open_destination() {
        use std::io::Read;
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::{FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE};

        let temp_dir = tempdir().expect("create temp dir");
        let base = temp_dir.path().join("bucket");
        std::fs::create_dir(&base).expect("create destination base");
        let src = temp_dir.path().join("staged-object");
        let dst = base.join("xl.meta");
        std::fs::write(&src, b"new").expect("write staged object");
        std::fs::write(&dst, b"old").expect("write old destination");
        let mut open_destination = std::fs::OpenOptions::new()
            .read(true)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
            .open(&dst)
            .expect("open destination with delete sharing");

        rename_all(&src, &dst, &base)
            .await
            .expect("an open destination that shares delete must remain replaceable");

        let mut old_contents = Vec::new();
        open_destination
            .read_to_end(&mut old_contents)
            .expect("read replaced file through its retained handle");
        assert_eq!(old_contents, b"old");
        assert_eq!(std::fs::read(&dst).expect("read replacement by path"), b"new");
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_rename_all_fails_closed_during_parent_reparse_mutation() {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::{
            Foundation::GENERIC_WRITE,
            Storage::FileSystem::{FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ, FILE_SHARE_WRITE},
        };

        let temp_dir = tempdir().expect("create temp dir");
        let base = temp_dir.path().join("bucket");
        let parent = base.join("object");
        let outside = temp_dir.path().join("outside");
        std::fs::create_dir_all(&parent).expect("create destination parent");
        std::fs::create_dir(&outside).expect("create outside target");
        let src = temp_dir.path().join("staged-object");
        let dst = parent.join("xl.meta");
        std::fs::write(&src, b"payload").expect("write staged object");

        let parent_for_hook = parent.clone();
        let outside_for_hook = outside.clone();
        let reparse_writer = Arc::new(std::sync::Mutex::new(None));
        let reparse_writer_for_hook = Arc::clone(&reparse_writer);
        windows_rename_test_hooks::install_before_publication(&dst, move || {
            let writable_parent = std::fs::OpenOptions::new()
                .access_mode(GENERIC_WRITE)
                .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
                .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
                .open(&parent_for_hook)
                .map(winapi_util::Handle::from_file)
                .expect("open the retained parent for a concurrent reparse mutation");
            try_set_windows_mount_point(&writable_parent, &outside_for_hook)
                .expect("redirect the destination parent after it is pinned");
            *reparse_writer_for_hook.lock().expect("reparse writer mutex poisoned") = Some(writable_parent);
        });

        rename_all(&src, &dst, &base)
            .await
            .expect_err("publication must fail closed when the retained parent becomes a reparse point");

        assert!(src.exists(), "failed publication must retain its staged source");
        assert!(!outside.join("xl.meta").exists(), "reparse mutation must not redirect publication");
        let writable_parent = reparse_writer
            .lock()
            .expect("reparse writer mutex poisoned")
            .take()
            .expect("publication hook must retain the parent handle");
        try_delete_windows_mount_point(&writable_parent).expect("remove destination parent mount point");
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_rename_all_rejects_a_preexisting_reparse_base() {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::{
            Foundation::GENERIC_WRITE,
            Storage::FileSystem::{FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ, FILE_SHARE_WRITE},
        };

        let temp_dir = tempdir().expect("create temp dir");
        let base = temp_dir.path().join("bucket");
        let outside = temp_dir.path().join("outside");
        let src = temp_dir.path().join("staged-object");
        let dst = base.join("object").join("xl.meta");
        std::fs::create_dir(&base).expect("create destination base");
        std::fs::create_dir(&outside).expect("create outside target");
        std::fs::write(&src, b"payload").expect("write staged object");
        let writable_base = std::fs::OpenOptions::new()
            .access_mode(GENERIC_WRITE)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
            .open(&base)
            .map(winapi_util::Handle::from_file)
            .expect("open destination base for reparse mutation");
        try_set_windows_mount_point(&writable_base, &outside).expect("redirect the destination base");
        drop(writable_base);

        let err = rename_all(&src, &dst, &base)
            .await
            .expect_err("a reparse destination base must be rejected");

        assert!(matches!(err, DiskError::FileAccessDenied));
        assert!(src.exists(), "rejected publication must preserve the staged source");
        assert!(!outside.join("object").exists(), "reparse base must not redirect parent creation");
        let writable_base = std::fs::OpenOptions::new()
            .access_mode(GENERIC_WRITE)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
            .open(&base)
            .map(winapi_util::Handle::from_file)
            .expect("reopen destination base to remove its mount point");
        try_delete_windows_mount_point(&writable_base).expect("remove destination base mount point");
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_rename_all_rejects_a_preexisting_reparse_intermediate() {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::{
            Foundation::GENERIC_WRITE,
            Storage::FileSystem::{FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ, FILE_SHARE_WRITE},
        };

        let temp_dir = tempdir().expect("create temp dir");
        let base = temp_dir.path().join("bucket");
        let linked = base.join("linked");
        let outside = temp_dir.path().join("outside");
        let src = temp_dir.path().join("staged-object");
        let dst = linked.join("object").join("xl.meta");
        std::fs::create_dir(&base).expect("create destination base");
        std::fs::create_dir(&linked).expect("create intermediate directory");
        std::fs::create_dir(&outside).expect("create outside target");
        std::fs::write(&src, b"payload").expect("write staged object");
        let writable_intermediate = std::fs::OpenOptions::new()
            .access_mode(GENERIC_WRITE)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
            .open(&linked)
            .map(winapi_util::Handle::from_file)
            .expect("open intermediate directory for reparse mutation");
        try_set_windows_mount_point(&writable_intermediate, &outside).expect("redirect the intermediate directory");
        drop(writable_intermediate);

        let err = rename_all(&src, &dst, &base)
            .await
            .expect_err("a reparse destination intermediate must be rejected");

        assert!(matches!(err, DiskError::FileAccessDenied));
        assert!(src.exists(), "rejected publication must preserve the staged source");
        assert!(!outside.join("object").exists(), "reparse intermediate must not redirect parent creation");
        let writable_intermediate = std::fs::OpenOptions::new()
            .access_mode(GENERIC_WRITE)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
            .open(&linked)
            .map(winapi_util::Handle::from_file)
            .expect("reopen intermediate directory to remove its mount point");
        try_delete_windows_mount_point(&writable_intermediate).expect("remove intermediate mount point");
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_rename_all_rejects_a_reparse_ancestor_of_nested_base() {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::{
            Foundation::GENERIC_WRITE,
            Storage::FileSystem::{FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ, FILE_SHARE_WRITE},
        };

        let temp_dir = tempdir().expect("create temp dir");
        let storage = temp_dir.path().join("storage");
        let linked = storage.join("linked");
        let outside = temp_dir.path().join("outside");
        let outside_base = outside.join("bucket");
        let base = linked.join("bucket");
        let src = temp_dir.path().join("staged-object");
        let dst = base.join("object").join("xl.meta");
        std::fs::create_dir(&storage).expect("create storage root");
        std::fs::create_dir(&linked).expect("create base ancestor");
        std::fs::create_dir(&outside).expect("create outside target");
        std::fs::create_dir(&outside_base).expect("create terminal base through redirect target");
        std::fs::write(&src, b"payload").expect("write staged object");
        let writable_ancestor = std::fs::OpenOptions::new()
            .access_mode(GENERIC_WRITE)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
            .open(&linked)
            .map(winapi_util::Handle::from_file)
            .expect("open base ancestor for reparse mutation");
        try_set_windows_mount_point(&writable_ancestor, &outside).expect("redirect the base ancestor");
        drop(writable_ancestor);

        let err = rename_all(&src, &dst, &base)
            .await
            .expect_err("a reparse ancestor before a nested base must be rejected");

        assert!(matches!(err, DiskError::FileAccessDenied));
        assert!(src.exists(), "rejected publication must preserve the staged source");
        assert!(
            !outside_base.join("object").exists(),
            "a reparse ancestor must not redirect publication outside the guarded tree"
        );
        let writable_ancestor = std::fs::OpenOptions::new()
            .access_mode(GENERIC_WRITE)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
            .open(&linked)
            .map(winapi_util::Handle::from_file)
            .expect("reopen base ancestor to remove its mount point");
        try_delete_windows_mount_point(&writable_ancestor).expect("remove base ancestor mount point");
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_rename_all_rejects_a_reparse_ancestor_of_source_parent() {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::{
            Foundation::GENERIC_WRITE,
            Storage::FileSystem::{FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ, FILE_SHARE_WRITE},
        };

        let temp_dir = tempdir().expect("create temp dir");
        let base = temp_dir.path().join("bucket");
        let staging = temp_dir.path().join("staging");
        let linked = staging.join("linked");
        let outside = temp_dir.path().join("outside");
        let outside_parent = outside.join("parent");
        let src = linked.join("parent").join("staged-object");
        let dst = base.join("published-object");
        std::fs::create_dir(&base).expect("create destination base");
        std::fs::create_dir(&staging).expect("create staging root");
        std::fs::create_dir(&linked).expect("create source ancestor");
        std::fs::create_dir(&outside).expect("create outside target");
        std::fs::create_dir(&outside_parent).expect("create redirected source parent");
        std::fs::write(outside_parent.join("staged-object"), b"outside").expect("write redirected source object");
        let writable_ancestor = std::fs::OpenOptions::new()
            .access_mode(GENERIC_WRITE)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
            .open(&linked)
            .map(winapi_util::Handle::from_file)
            .expect("open source ancestor for reparse mutation");
        try_set_windows_mount_point(&writable_ancestor, &outside).expect("redirect the source ancestor");
        drop(writable_ancestor);

        let err = rename_all(&src, &dst, &base)
            .await
            .expect_err("a reparse ancestor before the source parent must be rejected");

        assert!(matches!(err, DiskError::FileAccessDenied));
        assert!(!dst.exists(), "rejected publication must not create a destination");
        assert_eq!(
            std::fs::read(outside_parent.join("staged-object")).expect("read unchanged redirected source"),
            b"outside"
        );
        let writable_ancestor = std::fs::OpenOptions::new()
            .access_mode(GENERIC_WRITE)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
            .open(&linked)
            .map(winapi_util::Handle::from_file)
            .expect("reopen source ancestor to remove its mount point");
        try_delete_windows_mount_point(&writable_ancestor).expect("remove source ancestor mount point");
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_rename_retry_retains_destination_identity() {
        let temp_dir = tempdir().expect("create temp dir");
        let base = temp_dir.path().join("bucket");
        let parent = base.join("object");
        let src = temp_dir.path().join("staged-directory");
        let dst = parent.join("published-directory");
        std::fs::create_dir_all(&parent).expect("create destination parent");
        std::fs::create_dir(&src).expect("create staged directory");

        let dst_for_first_attempt = dst.clone();
        windows_rename_test_hooks::install_before_publication(&dst, move || {
            std::fs::create_dir(&dst_for_first_attempt).expect("create conflicting destination directory");
            std::fs::write(dst_for_first_attempt.join("child"), b"occupied").expect("populate conflicting destination");
        });
        let parent_for_retry = parent.clone();
        let replacement = base.join("replacement-object");
        let replacement_for_retry = replacement.clone();
        let replacement_source = temp_dir.path().join("replacement-source");
        let src_for_retry = src.clone();
        let replacement_source_for_retry = replacement_source.clone();
        let dst_for_retry = dst.clone();
        windows_rename_test_hooks::install_before_rename_retry(&dst, move || {
            std::fs::rename(&parent_for_retry, &replacement_for_retry)
                .expect_err("the destination guard must remain held between rename attempts");
            std::fs::rename(&src_for_retry, &replacement_source_for_retry)
                .expect_err("the source handle must remain held between rename attempts");
            std::fs::remove_file(dst_for_retry.join("child")).expect("remove retry conflict child");
            std::fs::remove_dir(&dst_for_retry).expect("remove retry conflict directory");
        });
        windows_rename_test_hooks::observe_guard_generations(&dst);

        rename_all(&src, &dst, &base)
            .await
            .expect("the second rename attempt must publish through the original guard");

        let generations = windows_rename_test_hooks::take_guard_generations(&dst);
        assert_eq!(generations.len(), 2, "the retry test must observe both publication attempts");
        assert_eq!(generations[0], generations[1], "both attempts must retain the same destination guard");
        assert!(dst.is_dir(), "the staged directory must be published");
        assert!(!replacement.exists(), "the guarded destination parent must not be replaced");
        assert!(!replacement_source.exists(), "the guarded source entry must not be replaced");
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_rename_retry_recovers_from_a_transient_source_open_conflict() {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::{FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE};

        let temp_dir = tempdir().expect("create temp dir");
        let base = temp_dir.path().join("bucket");
        let src = temp_dir.path().join("staged-object");
        let dst = base.join("published-object");
        std::fs::create_dir(&base).expect("create destination base");
        std::fs::write(&src, b"payload").expect("write staged object");
        let writer = std::fs::OpenOptions::new()
            .write(true)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
            .open(&src)
            .expect("retain a transient source writer");
        windows_rename_test_hooks::install_before_rename_retry(&dst, move || drop(writer));

        rename_all(&src, &dst, &base)
            .await
            .expect("the preparation retry must succeed after the writer closes");

        assert!(!src.exists());
        assert_eq!(std::fs::read(&dst).expect("read retried publication"), b"payload");
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_rename_retry_rejects_a_replaced_source_entry() {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::{FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE};

        let temp_dir = tempdir().expect("create temp dir");
        let base = temp_dir.path().join("bucket");
        let src = temp_dir.path().join("staged-object");
        let original = temp_dir.path().join("original-staged-object");
        let dst = base.join("published-object");
        std::fs::create_dir(&base).expect("create destination base");
        std::fs::write(&src, b"original").expect("write staged object");
        let writer = std::fs::OpenOptions::new()
            .write(true)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
            .open(&src)
            .expect("retain a transient source writer");
        let src_for_retry = src.clone();
        let original_for_retry = original.clone();
        windows_rename_test_hooks::install_before_rename_retry(&dst, move || {
            drop(writer);
            std::fs::rename(&src_for_retry, &original_for_retry).expect("move the original staged object aside");
            std::fs::write(&src_for_retry, b"replacement").expect("install a replacement staged object");
        });

        let err = rename_all(&src, &dst, &base)
            .await
            .expect_err("a retry must not publish a replacement source entry");

        assert!(matches!(err, DiskError::FileCorrupt));
        assert!(!dst.exists(), "the replacement source must not be published");
        assert_eq!(std::fs::read(&src).expect("read replacement source"), b"replacement");
        assert_eq!(std::fs::read(&original).expect("read original source"), b"original");
    }

    #[cfg(windows)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn windows_cancelled_rename_serializes_retry_until_preparation_finishes() {
        use std::os::windows::fs::OpenOptionsExt;
        use std::sync::mpsc;
        use windows_sys::Win32::Storage::FileSystem::{FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE};

        let temp_dir = tempdir().expect("create temp dir");
        let base = temp_dir.path().join("bucket");
        let src = temp_dir.path().join("staged-object");
        let retry_src = temp_dir.path().join("retry-staged-object");
        let dst = base.join("object");
        std::fs::create_dir(&base).expect("create destination base");
        std::fs::write(&src, b"payload").expect("write staged object");
        std::fs::write(&retry_src, b"retry-payload").expect("write retry staged object");
        let writer = std::fs::OpenOptions::new()
            .write(true)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
            .open(&src)
            .expect("retain a transient source writer");

        let (release_tx, release_rx) = mpsc::channel();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        windows_rename_test_hooks::install_before_rename_retry(&dst, move || {
            entered_tx.send(()).expect("signal preparation hook entry");
            release_rx.recv().expect("wait until the operation has been cancelled");
            drop(writer);
        });

        let destination = dst.clone();
        let retry_destination = dst.clone();
        let retry_base = base.clone();
        let rename = tokio::spawn(async move { rename_all(&src, &dst, &base).await });
        tokio::time::timeout(std::time::Duration::from_secs(30), entered_rx)
            .await
            .expect("timed out waiting for preparation to start before cancellation")
            .expect("preparation hook sender dropped before cancellation");
        rename.abort();
        let cancellation = tokio::time::timeout(std::time::Duration::from_secs(1), rename)
            .await
            .expect("the async waiter should observe cancellation without waiting for the blocking syscall")
            .expect_err("the aborted rename task should be cancelled");
        assert!(cancellation.is_cancelled(), "the rename waiter should report cancellation");

        let mut retry = tokio::spawn(async move { rename_all(&retry_src, &retry_destination, &retry_base).await });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut retry)
                .await
                .is_err(),
            "a retry must wait while cancelled preparation still owns the destination namespace"
        );
        release_tx.send(()).expect("release preparation after cancellation");
        tokio::time::timeout(std::time::Duration::from_secs(10), retry)
            .await
            .expect("retry should finish after cancelled preparation releases the namespace")
            .expect("retry task should not panic")
            .expect("retry publication should succeed");
        assert_eq!(
            std::fs::read(destination).expect("read retried publication"),
            b"retry-payload",
            "the serialized retry must be the final destination value"
        );
    }

    #[cfg(windows)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn windows_cancelled_rename_serializes_retry_until_publication_finishes() {
        use std::sync::mpsc;

        let temp_dir = tempdir().expect("create temp dir");
        let base = temp_dir.path().join("bucket");
        let src = temp_dir.path().join("staged-object");
        let retry_src = temp_dir.path().join("retry-staged-object");
        let dst = base.join("object");
        std::fs::create_dir(&base).expect("create destination base");
        std::fs::write(&src, b"payload").expect("write staged object");
        std::fs::write(&retry_src, b"retry-payload").expect("write retry staged object");

        let (release_tx, release_rx) = mpsc::channel();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        windows_rename_test_hooks::install_before_publication(&dst, move || {
            entered_tx.send(()).expect("signal publication hook entry");
            release_rx.recv().expect("wait until the operation has been cancelled");
        });

        let destination = dst.clone();
        let retry_destination = dst.clone();
        let retry_base = base.clone();
        let rename = tokio::spawn(async move { rename_all(&src, &dst, &base).await });
        tokio::time::timeout(std::time::Duration::from_secs(30), entered_rx)
            .await
            .expect("timed out waiting for publication to start before cancellation")
            .expect("publication hook sender dropped before cancellation");
        rename.abort();
        let cancellation = tokio::time::timeout(std::time::Duration::from_secs(1), rename)
            .await
            .expect("the async waiter should observe cancellation without waiting for publication")
            .expect_err("the aborted rename task should be cancelled");
        assert!(cancellation.is_cancelled(), "the rename waiter should report cancellation");

        let mut retry = tokio::spawn(async move { rename_all(&retry_src, &retry_destination, &retry_base).await });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut retry)
                .await
                .is_err(),
            "a retry must wait while cancelled publication still owns the destination namespace"
        );
        release_tx.send(()).expect("release publication after cancellation");
        tokio::time::timeout(std::time::Duration::from_secs(10), retry)
            .await
            .expect("retry should finish after cancelled publication releases the namespace")
            .expect("retry task should not panic")
            .expect("retry publication should succeed");
        assert_eq!(
            std::fs::read(destination).expect("read retried publication"),
            b"retry-payload",
            "the serialized retry must be the final destination value"
        );
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_rename_all_rejects_a_source_reparse_entry() {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::{
            Foundation::GENERIC_WRITE,
            Storage::FileSystem::{FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ, FILE_SHARE_WRITE},
        };

        let temp_dir = tempdir().expect("create temp dir");
        let base = temp_dir.path().join("bucket");
        let src = temp_dir.path().join("staged-directory");
        let outside = temp_dir.path().join("outside");
        let dst = base.join("published-directory");
        std::fs::create_dir(&base).expect("create destination base");
        std::fs::create_dir(&src).expect("create source reparse entry");
        std::fs::create_dir(&outside).expect("create source target");
        std::fs::write(outside.join("marker"), b"outside").expect("write source target marker");
        let source_reparse = std::fs::OpenOptions::new()
            .access_mode(GENERIC_WRITE)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
            .open(&src)
            .map(winapi_util::Handle::from_file)
            .expect("open source reparse entry");
        try_set_windows_mount_point(&source_reparse, &outside).expect("redirect the staged source");
        drop(source_reparse);

        let publication_root = test_publication_root(&[&src, &dst, &base]);
        let source_parent_guard =
            lock_windows_directory_tree(src.parent().expect("source reparse entry must have a parent"), None, &publication_root)
                .expect("pin the source parent tree");
        let identity_error = match open_windows_rename_source_identity(&src, &source_parent_guard) {
            Ok(_) => panic!("a non-dedup source reparse entry must be rejected by its tag"),
            Err(err) => err,
        };
        assert_eq!(identity_error.to_string(), WINDOWS_RENAME_SOURCE_REPARSE_ERROR);
        let rename_error = match open_windows_rename_source(&src, &source_parent_guard) {
            Ok(_) => panic!("the rename handle must apply the same final-entry tag policy"),
            Err(err) => err,
        };
        assert_eq!(rename_error.to_string(), WINDOWS_RENAME_SOURCE_REPARSE_ERROR);

        let err = rename_all(&src, &dst, &base)
            .await
            .expect_err("a staged reparse point must not be published into the object tree");

        assert!(matches!(err, DiskError::FileAccessDenied));
        assert!(!dst.exists(), "rejected reparse publication must not create a destination");
        let retained_reparse = std::fs::OpenOptions::new()
            .access_mode(GENERIC_WRITE)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
            .open(&src)
            .map(winapi_util::Handle::from_file)
            .expect("open retained source reparse entry");
        try_delete_windows_mount_point(&retained_reparse).expect("remove retained source reparse point");
        drop(retained_reparse);
        assert!(src.is_dir(), "failed publication must preserve the staged source entry");
        assert_eq!(std::fs::read(outside.join("marker")).expect("read unchanged target marker"), b"outside");
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_rename_all_excludes_source_parent_reparse_writers() {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::{
            Foundation::GENERIC_WRITE,
            Storage::FileSystem::{FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ, FILE_SHARE_WRITE},
        };

        let temp_dir = tempdir().expect("create temp dir");
        let base = temp_dir.path().join("bucket");
        let source_parent = temp_dir.path().join("staging");
        let src = source_parent.join("staged-object");
        let dst = base.join("published-object");
        std::fs::create_dir(&base).expect("create destination base");
        std::fs::create_dir(&source_parent).expect("create source parent");
        std::fs::write(&src, b"original").expect("write original staged object");

        let source_parent_for_hook = source_parent.clone();
        windows_rename_test_hooks::install_before_publication(&dst, move || {
            std::fs::OpenOptions::new()
                .access_mode(GENERIC_WRITE)
                .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
                .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
                .open(&source_parent_for_hook)
                .expect_err("the source-parent guard must exclude a reparse writer");
        });

        rename_all(&src, &dst, &base)
            .await
            .expect("source publication must use the anchored source parent");

        assert!(!src.exists(), "the original staged entry must be moved");
        assert_eq!(std::fs::read(&dst).expect("read published original object"), b"original");
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_rename_all_allows_source_readers_but_excludes_writers_and_deleters() {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::{DELETE, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE};

        let temp_dir = tempdir().expect("create temp dir");
        let base = temp_dir.path().join("bucket");
        std::fs::create_dir(&base).expect("create destination base");
        let share_all = FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;

        let readable_src = temp_dir.path().join("readable-stage");
        let readable_dst = base.join("readable-object");
        std::fs::write(&readable_src, b"reader").expect("write readable source");
        let _reader = std::fs::OpenOptions::new()
            .read(true)
            .share_mode(share_all)
            .open(&readable_src)
            .expect("retain a source reader");
        rename_all(&readable_src, &readable_dst, &base)
            .await
            .expect("a retained reader must not block publication");
        assert_eq!(std::fs::read(&readable_dst).expect("read reader-compatible publication"), b"reader");

        let writable_src = temp_dir.path().join("writable-stage");
        let writable_dst = base.join("writable-object");
        std::fs::write(&writable_src, b"writer").expect("write writable source");
        let writer = std::fs::OpenOptions::new()
            .write(true)
            .share_mode(share_all)
            .open(&writable_src)
            .expect("retain a source writer");
        rename_all(&writable_src, &writable_dst, &base)
            .await
            .expect_err("a retained writer must block publication");
        assert!(writable_src.exists(), "failed writer-conflicting publication must preserve its source");
        assert!(
            !writable_dst.exists(),
            "failed writer-conflicting publication must not create a destination"
        );
        drop(writer);

        let deletable_src = temp_dir.path().join("deletable-stage");
        let deletable_dst = base.join("deletable-object");
        std::fs::write(&deletable_src, b"deleter").expect("write deletable source");
        let deleter = std::fs::OpenOptions::new()
            .access_mode(DELETE)
            .share_mode(share_all)
            .open(&deletable_src)
            .expect("retain a source delete handle");
        rename_all(&deletable_src, &deletable_dst, &base)
            .await
            .expect_err("a retained delete handle must block duplicate publication");
        assert!(deletable_src.exists(), "failed delete-conflicting publication must preserve its source");
        assert!(
            !deletable_dst.exists(),
            "failed delete-conflicting publication must not create a destination"
        );
        drop(deleter);
    }

    #[cfg(windows)]
    #[test]
    fn windows_rename_all_preserves_directory_not_empty_error() {
        use windows_sys::Win32::Foundation::ERROR_DIR_NOT_EMPTY;

        let temp_dir = tempdir().expect("create temp dir");
        let base = temp_dir.path().join("bucket");
        let src = temp_dir.path().join("source-directory");
        let dst = base.join("destination-directory");
        std::fs::create_dir(&base).expect("create destination base");
        std::fs::create_dir(&src).expect("create source directory");
        std::fs::create_dir(&dst).expect("create destination directory");
        std::fs::write(dst.join("child"), b"occupied").expect("populate destination directory");
        let guard = mkdir_all_below_existing_base_std(&base, &base).expect("guard destination base");
        let publication_root = test_publication_root(&[&src, &dst, &base]);
        let source_parent_guard =
            lock_windows_directory_tree(src.parent().expect("source path must have a parent"), None, &publication_root)
                .expect("anchor source parent");
        let source = open_windows_rename_source(&src, &source_parent_guard).expect("anchor source entry");

        let err = rename_into_existing_parent(&dst, Some(&guard), &source)
            .expect_err("replacing a non-empty destination directory must fail");

        assert_eq!(err.raw_os_error(), i32::try_from(ERROR_DIR_NOT_EMPTY).ok());
        assert!(src.is_dir(), "failed directory replacement must preserve the source");
        assert!(dst.join("child").is_file(), "failed directory replacement must preserve the destination");
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_guarded_parent_allows_same_and_descendant_publication() {
        let temp_dir = tempdir().expect("create temp dir");
        let base = temp_dir.path().join("bucket");
        let parent = base.join("object");
        std::fs::create_dir_all(&parent).expect("create destination parent");
        let _guard = mkdir_all_below_existing_base_std(&parent, &base).expect("guard destination parent");
        let first_src = temp_dir.path().join("first-stage");
        let second_src = temp_dir.path().join("second-stage");
        std::fs::write(&first_src, b"first").expect("write first source");
        std::fs::write(&second_src, b"second").expect("write second source");

        rename_all(&first_src, parent.join("first"), &base)
            .await
            .expect("same-parent rename must succeed while a guard is held");
        rename_all(&second_src, parent.join("nested").join("second"), &base)
            .await
            .expect("descendant-parent rename must succeed while an ancestor guard is held");
        assert_eq!(std::fs::read(parent.join("first")).expect("read first destination"), b"first");
        assert_eq!(
            std::fs::read(parent.join("nested").join("second")).expect("read second destination"),
            b"second"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rename_parent_creation_rejects_symlinked_base() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempdir().expect("create temp dir");
        let outside = temp_dir.path().join("outside");
        std::fs::create_dir(&outside).expect("create outside directory");
        let base = temp_dir.path().join("bucket");
        symlink(&outside, &base).expect("create symlinked base");

        mkdir_all_below_existing_base_std(&base.join("object"), &base).expect_err("symlinked base must be rejected");

        assert!(!outside.join("object").exists(), "parent creation must remain confined to the base");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rename_parent_creation_rejects_symlink_below_base() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempdir().expect("create temp dir");
        let base = temp_dir.path().join("bucket");
        let outside = temp_dir.path().join("outside");
        std::fs::create_dir(&base).expect("create destination base");
        std::fs::create_dir(&outside).expect("create outside directory");
        symlink(&outside, base.join("linked")).expect("create symlink below base");

        mkdir_all_below_existing_base_std(&base.join("linked/object"), &base).expect_err("symlink below base must be rejected");

        assert!(
            !outside.join("object").exists(),
            "parent creation must not follow a symlink outside the base"
        );
    }

    #[tokio::test]
    async fn fsync_dir_succeeds_on_directory() {
        let temp_dir = tempdir().expect("create temp dir");

        fsync_dir(temp_dir.path()).await.expect("fsync dir must succeed");
    }

    async fn wait_for_dst_dir_fsync_group_commit_idle() {
        for _ in 0..100 {
            if dst_dir_fsync_group_commit_counts_for_test() == (0, 0) {
                return;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            dst_dir_fsync_group_commit_counts_for_test(),
            (0, 0),
            "dst dir fsync group registry must release idle groups and waiters"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial(dst_dir_fsync_group_commit)]
    async fn dst_dir_fsync_group_commit_default_off_uses_direct_fsync() {
        let temp_dir = tempdir().expect("create temp dir");
        let dir = temp_dir.path().join("object");
        std::fs::create_dir(&dir).expect("create object dir");

        fsync_dst_dir_group_commit_for_test(&dir, false)
            .await
            .expect("direct dst dir fsync should succeed");

        assert!(fsync_dir_recorder::was_fsynced(&dir), "default-off path must still fsync the dst dir");
        assert!(
            fsync_dir_recorder::grouped_batch_sizes(&dir).is_empty(),
            "default-off path must not enter the group commit coordinator"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial(dst_dir_fsync_group_commit)]
    async fn dst_dir_fsync_group_commit_batches_same_directory_waiters() {
        use std::sync::mpsc;
        let temp_dir = tempdir().expect("create temp dir");
        let dir = temp_dir.path().join("object");
        std::fs::create_dir(&dir).expect("create object dir");
        let (batch_entered_tx, batch_entered_rx) = mpsc::channel();
        let (release_batch_tx, release_batch_rx) = mpsc::channel();
        fsync_dir_recorder::set_before_group_batch(&dir, move || {
            batch_entered_tx.send(()).expect("signal first worker before freezing batch");
            release_batch_rx.recv().expect("wait until second waiter is queued");
        });

        let first_dir = dir.clone();
        let first = tokio::spawn(async move { fsync_dst_dir_group_commit_for_test(first_dir, true).await });
        tokio::task::spawn_blocking(move || batch_entered_rx.recv_timeout(Duration::from_secs(30)))
            .await
            .expect("batch hook waiter should run")
            .expect("first worker should reach the batch hook");

        let (second_enqueued_tx, second_enqueued_rx) = mpsc::channel();
        fsync_dir_recorder::set_after_group_enqueue(&dir, move || {
            second_enqueued_tx.send(()).expect("signal second waiter enqueue");
        });
        let second_dir = dir.clone();
        let second = tokio::spawn(async move { fsync_dst_dir_group_commit_for_test(second_dir, true).await });
        tokio::task::spawn_blocking(move || second_enqueued_rx.recv_timeout(Duration::from_secs(30)))
            .await
            .expect("enqueue hook waiter should run")
            .expect("second waiter should be enqueued");
        assert_eq!(
            dst_dir_fsync_group_commit_counts_for_test(),
            (1, 2),
            "second waiter must be queued before the first batch is released"
        );
        release_batch_tx.send(()).expect("release first batch");

        let (first_result, second_result) = tokio::time::timeout(Duration::from_secs(30), async { tokio::join!(first, second) })
            .await
            .expect("same-directory fsync waiters should complete");
        first_result
            .expect("first waiter task should not panic")
            .expect("first waiter should observe successful fsync");
        second_result
            .expect("second waiter task should not panic")
            .expect("second waiter should observe successful fsync");

        assert_eq!(
            fsync_dir_recorder::grouped_batch_sizes(&dir),
            vec![2],
            "two waiters queued before the batch freezes must share exactly one dst dir fsync"
        );
        wait_for_dst_dir_fsync_group_commit_idle().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial(dst_dir_fsync_group_commit)]
    async fn dst_dir_fsync_group_commit_late_join_waits_for_next_fsync() {
        use std::sync::mpsc;
        let temp_dir = tempdir().expect("create temp dir");
        let dir = temp_dir.path().join("object");
        std::fs::create_dir(&dir).expect("create object dir");
        let (fsync_entered_tx, fsync_entered_rx) = mpsc::channel();
        let (release_fsync_tx, release_fsync_rx) = mpsc::channel();
        fsync_dir_recorder::set_before_grouped(&dir, move || {
            fsync_entered_tx.send(()).expect("signal first frozen batch");
            release_fsync_rx.recv().expect("wait until late waiter is queued");
        });

        let first_dir = dir.clone();
        let first = tokio::spawn(async move { fsync_dst_dir_group_commit_for_test(first_dir, true).await });
        tokio::task::spawn_blocking(move || fsync_entered_rx.recv_timeout(Duration::from_secs(30)))
            .await
            .expect("grouped fsync hook waiter should run")
            .expect("first batch should reach fsync");

        let second_dir = dir.clone();
        let second = tokio::spawn(async move { fsync_dst_dir_group_commit_for_test(second_dir, true).await });
        release_fsync_tx.send(()).expect("release first fsync");

        let (first_result, second_result) = tokio::time::timeout(Duration::from_secs(30), async { tokio::join!(first, second) })
            .await
            .expect("late waiter should complete after a second fsync");
        first_result
            .expect("first waiter task should not panic")
            .expect("first waiter should observe successful fsync");
        second_result
            .expect("second waiter task should not panic")
            .expect("late waiter should observe successful fsync");

        assert_eq!(
            fsync_dir_recorder::grouped_batch_sizes(&dir),
            vec![1, 1],
            "a waiter queued after the first batch is frozen must not be covered by the earlier fsync"
        );
        wait_for_dst_dir_fsync_group_commit_idle().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial(dst_dir_fsync_group_commit)]
    async fn dst_dir_fsync_group_commit_propagates_shared_fsync_failure() {
        let temp_dir = tempdir().expect("create temp dir");
        let dir = temp_dir.path().join("object");
        std::fs::create_dir(&dir).expect("create object dir");
        fsync_dir_recorder::set_grouped_failure(&dir, io::ErrorKind::Other);

        let err = fsync_dst_dir_group_commit_for_test(&dir, true)
            .await
            .expect_err("shared dst dir fsync failure must be returned to the waiter");

        assert_eq!(err.kind(), io::ErrorKind::Other);
        wait_for_dst_dir_fsync_group_commit_idle().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial(dst_dir_fsync_group_commit)]
    async fn dst_dir_fsync_group_commit_cancellation_releases_waiter_state() {
        use std::sync::mpsc;
        let temp_dir = tempdir().expect("create temp dir");
        let dir = temp_dir.path().join("object");
        std::fs::create_dir(&dir).expect("create object dir");
        let (fsync_entered_tx, fsync_entered_rx) = mpsc::channel();
        let (release_fsync_tx, release_fsync_rx) = mpsc::channel();
        fsync_dir_recorder::set_before_grouped(&dir, move || {
            fsync_entered_tx.send(()).expect("signal grouped fsync");
            release_fsync_rx.recv().expect("wait for cancellation");
        });

        let cancelled_dir = dir.clone();
        let cancelled = tokio::spawn(async move { fsync_dst_dir_group_commit_for_test(cancelled_dir, true).await });
        tokio::task::spawn_blocking(move || fsync_entered_rx.recv_timeout(Duration::from_secs(30)))
            .await
            .expect("grouped fsync hook waiter should run")
            .expect("first grouped fsync should start");
        cancelled.abort();
        assert!(
            cancelled
                .await
                .expect_err("cancelled waiter task should abort")
                .is_cancelled(),
            "waiter cancellation must be observable"
        );
        release_fsync_tx.send(()).expect("release grouped fsync");

        fsync_dst_dir_group_commit_for_test(&dir, true)
            .await
            .expect("a later waiter should not be blocked by cancelled waiter state");
        wait_for_dst_dir_fsync_group_commit_idle().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial(dst_dir_fsync_group_commit)]
    async fn dst_dir_fsync_group_commit_recreated_directory_gets_new_group() {
        use std::sync::mpsc;
        let temp_dir = tempdir().expect("create temp dir");
        let dir = temp_dir.path().join("object");
        std::fs::create_dir(&dir).expect("create object dir");
        let (fsync_entered_tx, fsync_entered_rx) = mpsc::channel();
        let (release_fsync_tx, release_fsync_rx) = mpsc::channel();
        let dir_for_hook = dir.clone();
        fsync_dir_recorder::set_before_grouped(&dir, move || {
            std::fs::remove_dir(&dir_for_hook).expect("remove old object dir");
            std::fs::create_dir(&dir_for_hook).expect("recreate object dir at the same path");
            fsync_entered_tx.send(()).expect("signal grouped fsync");
            release_fsync_rx.recv().expect("wait until recreated dir is enqueued");
        });

        let first_dir = dir.clone();
        let first = tokio::spawn(async move { fsync_dst_dir_group_commit_for_test(first_dir, true).await });
        tokio::task::spawn_blocking(move || fsync_entered_rx.recv_timeout(Duration::from_secs(30)))
            .await
            .expect("grouped fsync hook waiter should run")
            .expect("first grouped fsync should start");

        let (_result_rx, worker) = DST_DIR_FSYNC_GROUP_COMMIT
            .enqueue_for_test(&dir)
            .expect("recreated dir should enqueue separately");
        assert!(worker.is_some(), "same path with a new inode must not join the stale in-flight group");
        assert_eq!(
            dst_dir_fsync_group_commit_counts_for_test().0,
            2,
            "old and recreated directory identities must be tracked as separate active groups"
        );

        release_fsync_tx.send(()).expect("release grouped fsync");
        first
            .await
            .expect("first waiter task should not panic")
            .expect("first stale directory fd should still fsync successfully");
        clear_dst_dir_fsync_group_commit_for_test();
        assert_eq!(
            dst_dir_fsync_group_commit_counts_for_test(),
            (0, 0),
            "test registry cleanup must release the unstarted recreated-directory waiter"
        );
    }

    #[test]
    #[serial_test::serial(dst_dir_fsync_group_commit)]
    fn dst_dir_fsync_group_commit_rejects_active_group_overflow() {
        let temp_dir = tempdir().expect("create temp dir");
        let mut receivers = Vec::new();
        for index in 0..MAX_DST_DIR_FSYNC_GROUPS {
            let dir = temp_dir.path().join(format!("object-{index}"));
            std::fs::create_dir(&dir).expect("create object dir");
            let (result_rx, _worker) = DST_DIR_FSYNC_GROUP_COMMIT
                .enqueue_for_test(&dir)
                .expect("group below cap should enqueue");
            receivers.push(result_rx);
        }
        let overflow_dir = temp_dir.path().join("overflow");
        std::fs::create_dir(&overflow_dir).expect("create overflow dir");

        let err = match DST_DIR_FSYNC_GROUP_COMMIT.enqueue_for_test(&overflow_dir) {
            Ok(_) => panic!("active group max+1 must fail closed"),
            Err(err) => err,
        };

        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
        clear_dst_dir_fsync_group_commit_for_test();
        assert_eq!(dst_dir_fsync_group_commit_counts_for_test(), (0, 0));
        drop(receivers);
    }

    #[test]
    #[serial_test::serial(dst_dir_fsync_group_commit)]
    fn dst_dir_fsync_group_commit_rejects_waiter_overflow() {
        let temp_dir = tempdir().expect("create temp dir");
        let dir = temp_dir.path().join("object");
        std::fs::create_dir(&dir).expect("create object dir");
        let mut receivers = Vec::new();
        for _ in 0..MAX_DST_DIR_FSYNC_WAITERS {
            let (result_rx, _worker) = DST_DIR_FSYNC_GROUP_COMMIT
                .enqueue_for_test(&dir)
                .expect("waiter below cap should enqueue");
            receivers.push(result_rx);
        }

        let err = match DST_DIR_FSYNC_GROUP_COMMIT.enqueue_for_test(&dir) {
            Ok(_) => panic!("waiter max+1 must fail closed"),
            Err(err) => err,
        };

        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
        clear_dst_dir_fsync_group_commit_for_test();
        assert_eq!(dst_dir_fsync_group_commit_counts_for_test(), (0, 0));
        drop(receivers);
    }

    #[tokio::test]
    async fn file_sync_admission_is_reused_across_commit_barriers() {
        let temp_dir = tempdir().expect("create temp dir");
        let limiter = Arc::new(Semaphore::new(1));
        let lease = acquire_namespace_mutation_lease(temp_dir.path()).await;
        let admission = acquire_file_sync_admission(limiter.clone())
            .await
            .expect("first commit should acquire admission");

        run_blocking_namespace_file_sync_operation(lease.clone(), &admission, || Ok(()))
            .await
            .expect("first barrier should complete under the admission");
        let mut waiting = Box::pin(acquire_file_sync_admission(limiter));
        assert!(
            futures::poll!(&mut waiting).is_pending(),
            "another commit must remain queued between durability barriers"
        );
        run_blocking_namespace_file_sync_operation(lease, &admission, || Ok(()))
            .await
            .expect("later barrier should reuse admission without requeuing");

        drop(admission);
        tokio::time::timeout(Duration::from_secs(30), waiting)
            .await
            .expect("queued commit should acquire admission after release")
            .expect("queued commit should acquire admission");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_file_sync_waiter_keeps_disk_admission_until_blocking_work_finishes() {
        use std::sync::mpsc;

        let temp_dir = tempdir().expect("create temp dir");
        let limiter = Arc::new(Semaphore::new(1));
        let global_permits = Arc::new(Semaphore::new(1));
        let lease = acquire_namespace_mutation_lease(temp_dir.path()).await;
        let admission = acquire_file_sync_admission(limiter.clone())
            .await
            .expect("file sync admission should be acquired");
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let waiter_global_permits = global_permits.clone();
        let waiter = tokio::spawn(async move {
            run_blocking_namespace_file_sync_operation_with_global(lease, &admission, waiter_global_permits.as_ref(), move || {
                entered_tx.send(()).expect("signal blocking work");
                release_rx.recv().expect("wait for blocking work release");
                Ok(())
            })
            .await
        });

        tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(30)))
            .await
            .expect("blocking work waiter should run")
            .expect("blocking work should start");
        waiter.abort();
        assert!(waiter.await.expect_err("waiter should be cancelled").is_cancelled());
        let returned_global_permit = global_permits
            .try_acquire()
            .expect("cancelled waiter must return global capacity for healthy disks");
        assert!(
            limiter.clone().try_acquire_owned().is_err(),
            "cancelled waiter must not return disk capacity while blocking work is active"
        );

        release_tx.send(()).expect("release blocking work");
        let _returned_permit = tokio::time::timeout(Duration::from_secs(30), limiter.acquire_owned())
            .await
            .expect("disk capacity should return after blocking work finishes")
            .expect("disk limiter should remain open");
        drop(returned_global_permit);
    }

    #[tokio::test]
    #[serial_test::serial(file_sync_probe)]
    async fn sync_dir_files_syncs_regular_files_and_dir() {
        let temp_dir = tempdir().expect("create temp dir");
        std::fs::write(temp_dir.path().join("part.1"), b"shard-one").expect("write part.1");
        std::fs::write(temp_dir.path().join("part.2"), b"shard-two").expect("write part.2");
        std::fs::create_dir(temp_dir.path().join("subdir")).expect("create subdir");
        let _probe = file_sync_probe::set(temp_dir.path());

        sync_dir_files(temp_dir.path()).await.expect("sync dir files must succeed");

        assert_eq!(std::fs::read(temp_dir.path().join("part.1")).expect("read part.1"), b"shard-one");
        assert!(
            fsync_dir_recorder::was_fsynced(temp_dir.path()),
            "successful sequential sync must fsync the directory"
        );
    }

    #[cfg(windows)]
    #[tokio::test]
    #[serial_test::serial(file_sync_probe)]
    async fn windows_sync_dir_files_opens_shards_for_flushing() {
        let temp_dir = tempdir().expect("create temp dir");
        std::fs::write(temp_dir.path().join("part.1"), b"shard").expect("write shard");
        let _probe = file_sync_probe::set(temp_dir.path());

        sync_dir_files(temp_dir.path())
            .await
            .expect("Windows shard handles must carry write access for FlushFileBuffers");
    }

    #[tokio::test]
    #[serial_test::serial(file_sync_probe)]
    async fn sync_dir_files_parallelizes_large_directories() {
        let temp_dir = tempdir().expect("create temp dir");
        for index in 0..PARALLEL_FILE_SYNC_THRESHOLD {
            std::fs::write(temp_dir.path().join(format!("part.{index}")), b"shard").expect("write part");
        }
        let _probe = file_sync_probe::set_blocking(temp_dir.path());
        let path = temp_dir.path().to_path_buf();
        let task = tokio::spawn(async move { sync_dir_files_with_limiter(path, file_sync_limiter()).await });
        file_sync_probe::wait_for_active(MAX_PARALLEL_FILE_SYNCS).await;

        assert!(file_sync_probe::peak() > 1, "large directories must sync more than one file concurrently");
        assert!(
            file_sync_probe::peak() <= MAX_PARALLEL_FILE_SYNCS.min(TEST_GLOBAL_FILE_SYNCS),
            "file sync concurrency must remain bounded"
        );
        file_sync_probe::release();
        task.await
            .expect("join parallel file sync")
            .expect("parallel file sync must succeed");
        assert!(
            fsync_dir_recorder::was_fsynced(temp_dir.path()),
            "successful parallel sync must fsync the directory"
        );
    }

    #[tokio::test]
    #[serial_test::serial(file_sync_probe)]
    async fn sync_dir_files_keeps_small_directories_sequential() {
        let temp_dir = tempdir().expect("create temp dir");
        for index in 0..(PARALLEL_FILE_SYNC_THRESHOLD - 1) {
            std::fs::write(temp_dir.path().join(format!("part.{index}")), b"shard").expect("write part");
        }
        let _probe = file_sync_probe::set_blocking(temp_dir.path());
        let path = temp_dir.path().to_path_buf();
        let task = tokio::spawn(async move { sync_dir_files_with_limiter(path, file_sync_limiter()).await });
        file_sync_probe::wait_for_active(1).await;

        assert_eq!(file_sync_probe::peak(), 1, "small directories must avoid parallel task overhead");
        file_sync_probe::release();
        task.await
            .expect("join sequential file sync")
            .expect("sequential file sync must succeed");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial(file_sync_probe)]
    async fn file_fdatasync_group_commit_default_off_keeps_small_directory_serial() {
        let _group_commit = set_file_fdatasync_group_commit_for_test(false);
        let temp_dir = tempdir().expect("create temp dir");
        std::fs::write(temp_dir.path().join("part.1"), b"shard").expect("write part");
        let _probe = file_sync_probe::set_blocking(temp_dir.path());
        let path = temp_dir.path().to_path_buf();
        let task = tokio::spawn(async move { sync_dir_files_with_limiter(path, file_sync_limiter()).await });
        file_sync_probe::wait_for_active(1).await;

        assert_eq!(
            file_sync_probe::group_batches(),
            Vec::<usize>::new(),
            "default-off small directory sync must not enter the file fdatasync group coordinator"
        );
        file_sync_probe::release();
        task.await
            .expect("join default-off file sync")
            .expect("default-off file sync must succeed");
        assert!(
            fsync_dir_recorder::was_fsynced(temp_dir.path()),
            "default-off successful sync must fsync the source directory"
        );
        assert_eq!(file_fdatasync_group_commit_counts_for_test(), (0, 0, 0));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial(file_sync_probe)]
    async fn file_fdatasync_group_commit_batches_same_disk_small_directories() {
        use std::sync::mpsc;

        let _group_commit = set_file_fdatasync_group_commit_for_test(true);
        set_file_fdatasync_group_commit_wait_for_test(0);
        clear_file_fdatasync_group_commit_for_test();
        let temp_dir = tempdir().expect("create temp dir");
        let first_dir = temp_dir.path().join("first");
        let second_dir = temp_dir.path().join("second");
        std::fs::create_dir(&first_dir).expect("create first dir");
        std::fs::create_dir(&second_dir).expect("create second dir");
        std::fs::write(first_dir.join("part.1"), b"first").expect("write first part");
        std::fs::write(second_dir.join("part.1"), b"second").expect("write second part");
        let _probe = file_sync_probe::set_blocking(temp_dir.path());
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_batch_tx, release_batch_rx) = mpsc::channel();
        file_sync_probe::set_before_group_batch(move || {
            entered_tx.send(()).expect("signal first file fdatasync group worker");
            release_batch_rx.recv().expect("wait until second waiter is queued");
        });

        let limiter = file_sync_limiter();
        let first_limiter = limiter.clone();
        let first_path = first_dir.clone();
        let first = tokio::spawn(async move { sync_dir_files_with_limiter(first_path, first_limiter).await });
        tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(30)))
            .await
            .expect("group worker hook waiter should run")
            .expect("first file fdatasync group worker should start");

        let second_limiter = limiter.clone();
        let second_path = second_dir.clone();
        let second = tokio::spawn(async move { sync_dir_files_with_limiter(second_path, second_limiter).await });
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if file_fdatasync_group_commit_counts_for_test().1 == 2 {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("second waiter should enqueue before releasing the grouped batch");
        release_batch_tx.send(()).expect("release group batch hook");
        file_sync_probe::wait_for_active(1).await;

        assert_eq!(
            file_sync_probe::group_batches(),
            vec![2],
            "same-disk small directory fdatasync waiters must share one observable batch"
        );
        file_sync_probe::release();
        first
            .await
            .expect("join first grouped file sync")
            .expect("first grouped file sync must succeed");
        second
            .await
            .expect("join second grouped file sync")
            .expect("second grouped file sync must succeed");
        assert!(
            fsync_dir_recorder::was_fsynced(&first_dir),
            "first source directory must still be fsynced"
        );
        assert!(
            fsync_dir_recorder::was_fsynced(&second_dir),
            "second source directory must still be fsynced"
        );
        assert_eq!(file_fdatasync_group_commit_counts_for_test(), (0, 0, 0));
    }

    #[test]
    fn file_fdatasync_group_commit_wait_duration_uses_default_and_cap() {
        assert_eq!(DEFAULT_FILE_FDATASYNC_GROUP_COMMIT_WAIT_MICROS, 0);
        assert_eq!(
            file_fdatasync_group_commit_wait_duration(DEFAULT_FILE_FDATASYNC_GROUP_COMMIT_WAIT_MICROS),
            Duration::ZERO
        );
        assert_eq!(file_fdatasync_group_commit_wait_duration(250), Duration::from_micros(250));
        assert_eq!(
            file_fdatasync_group_commit_wait_duration(MAX_FILE_FDATASYNC_GROUP_COMMIT_WAIT_MICROS),
            Duration::from_micros(MAX_FILE_FDATASYNC_GROUP_COMMIT_WAIT_MICROS)
        );
        assert_eq!(
            file_fdatasync_group_commit_wait_duration(u64::MAX),
            Duration::from_micros(MAX_FILE_FDATASYNC_GROUP_COMMIT_WAIT_MICROS)
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[serial_test::serial(file_sync_probe)]
    async fn file_fdatasync_group_commit_wait_budget_batches_late_follower() {
        use std::sync::mpsc;

        let _group_commit = set_file_fdatasync_group_commit_for_test(true);
        let wait_budget_micros = 1_000;
        let wait_budget = file_fdatasync_group_commit_wait_duration(wait_budget_micros);
        set_file_fdatasync_group_commit_wait_for_test(wait_budget_micros);
        clear_file_fdatasync_group_commit_for_test();
        let temp_dir = tempdir().expect("create temp dir");
        let first_dir = temp_dir.path().join("first");
        let second_dir = temp_dir.path().join("second");
        std::fs::create_dir(&first_dir).expect("create first dir");
        std::fs::create_dir(&second_dir).expect("create second dir");
        std::fs::write(first_dir.join("part.1"), b"first").expect("write first part");
        std::fs::write(second_dir.join("part.1"), b"second").expect("write second part");
        let _probe = file_sync_probe::set_blocking(temp_dir.path());
        let (entered_tx, entered_rx) = mpsc::channel();
        file_sync_probe::set_before_group_batch(move || {
            entered_tx.send(()).expect("signal first file fdatasync group worker");
        });

        let limiter = file_sync_limiter();
        let first_limiter = limiter.clone();
        let first_path = first_dir.clone();
        let first = tokio::spawn(async move { sync_dir_files_with_limiter(first_path, first_limiter).await });
        tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(30)))
            .await
            .expect("group worker hook waiter should run")
            .expect("first file fdatasync group worker should start");

        let second_limiter = limiter.clone();
        let second_path = second_dir.clone();
        let second = tokio::spawn(async move { sync_dir_files_with_limiter(second_path, second_limiter).await });
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if file_fdatasync_group_commit_counts_for_test().1 == 2 {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("second waiter should enqueue during the configured wait budget");
        tokio::time::advance(wait_budget).await;
        tokio::task::yield_now().await;
        file_sync_probe::wait_for_active(1).await;

        assert_eq!(
            file_sync_probe::group_batches(),
            vec![2],
            "configured wait budget should let a follower join the leader's batch"
        );
        file_sync_probe::release();
        first
            .await
            .expect("join first wait-budget file sync")
            .expect("first wait-budget file sync must succeed");
        second
            .await
            .expect("join second wait-budget file sync")
            .expect("second wait-budget file sync must succeed");
        assert!(
            fsync_dir_recorder::was_fsynced(&first_dir),
            "first source directory must still be fsynced"
        );
        assert!(
            fsync_dir_recorder::was_fsynced(&second_dir),
            "second source directory must still be fsynced"
        );
        assert_eq!(file_fdatasync_group_commit_counts_for_test(), (0, 0, 0));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial(file_sync_probe)]
    async fn file_fdatasync_group_commit_failure_fails_all_waiters_before_dir_fsync() {
        use std::sync::mpsc;

        let _group_commit = set_file_fdatasync_group_commit_for_test(true);
        set_file_fdatasync_group_commit_wait_for_test(0);
        clear_file_fdatasync_group_commit_for_test();
        let temp_dir = tempdir().expect("create temp dir");
        let first_dir = temp_dir.path().join("first");
        let second_dir = temp_dir.path().join("second");
        std::fs::create_dir(&first_dir).expect("create first dir");
        std::fs::create_dir(&second_dir).expect("create second dir");
        std::fs::write(first_dir.join("part.1"), b"first").expect("write first part");
        std::fs::write(second_dir.join("part.1"), b"second").expect("write second part");
        let _probe = file_sync_probe::set_failing_blocking(temp_dir.path());
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_batch_tx, release_batch_rx) = mpsc::channel();
        file_sync_probe::set_before_group_batch(move || {
            entered_tx.send(()).expect("signal first file fdatasync group worker");
            release_batch_rx.recv().expect("wait until second waiter is queued");
        });

        let limiter = file_sync_limiter();
        let first_limiter = limiter.clone();
        let first_path = first_dir.clone();
        let first = tokio::spawn(async move { sync_dir_files_with_limiter(first_path, first_limiter).await });
        tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(30)))
            .await
            .expect("group worker hook waiter should run")
            .expect("first file fdatasync group worker should start");

        let second_limiter = limiter.clone();
        let second_path = second_dir.clone();
        let second = tokio::spawn(async move { sync_dir_files_with_limiter(second_path, second_limiter).await });
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if file_fdatasync_group_commit_counts_for_test().1 == 2 {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("second waiter should enqueue before releasing the grouped batch");
        release_batch_tx.send(()).expect("release group batch hook");

        let first_err = first
            .await
            .expect("join first grouped file sync")
            .expect_err("first grouped waiter must fail closed");
        let second_err = second
            .await
            .expect("join second grouped file sync")
            .expect_err("second grouped waiter must fail closed");

        assert_eq!(first_err.kind(), io::ErrorKind::Other);
        assert_eq!(second_err.kind(), io::ErrorKind::Other);
        assert_eq!(file_sync_probe::group_batches(), vec![2]);
        assert!(
            !fsync_dir_recorder::was_fsynced(&first_dir) && !fsync_dir_recorder::was_fsynced(&second_dir),
            "source directories must not be fsynced after grouped file fdatasync failure"
        );
        assert_eq!(file_fdatasync_group_commit_counts_for_test(), (0, 0, 0));
        file_sync_probe::release();
        file_sync_probe::wait_for_idle().await;
    }

    #[test]
    #[serial_test::serial(file_sync_probe)]
    fn file_fdatasync_group_commit_rejects_active_group_overflow() {
        let _group_commit = set_file_fdatasync_group_commit_for_test(true);
        clear_file_fdatasync_group_commit_for_test();
        let mut receivers = Vec::new();
        let mut limiters = Vec::new();
        for index in 0..MAX_FILE_FDATASYNC_GROUPS {
            let limiter = Arc::new(Semaphore::new(1));
            let (result_rx, _worker) = FILE_FDATASYNC_GROUP_COMMIT
                .enqueue(limiter.clone(), vec![PathBuf::from(format!("part-{index}"))])
                .expect("group below cap should enqueue");
            limiters.push(limiter);
            receivers.push(result_rx);
        }

        let overflow_limiter = Arc::new(Semaphore::new(1));
        let err = match FILE_FDATASYNC_GROUP_COMMIT.enqueue(overflow_limiter, vec![PathBuf::from("overflow")]) {
            Ok(_) => panic!("active group max+1 must fail closed"),
            Err(err) => err,
        };

        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
        clear_file_fdatasync_group_commit_for_test();
        assert_eq!(file_fdatasync_group_commit_counts_for_test(), (0, 0, 0));
        drop(receivers);
        drop(limiters);
    }

    #[test]
    #[serial_test::serial(file_sync_probe)]
    fn file_fdatasync_group_commit_rejects_waiter_and_file_overflow() {
        let _group_commit = set_file_fdatasync_group_commit_for_test(true);
        clear_file_fdatasync_group_commit_for_test();
        let limiter = Arc::new(Semaphore::new(1));
        let mut receivers = Vec::new();
        for index in 0..MAX_FILE_FDATASYNC_WAITERS {
            let (result_rx, _worker) = FILE_FDATASYNC_GROUP_COMMIT
                .enqueue(limiter.clone(), vec![PathBuf::from(format!("part-{index}"))])
                .expect("waiter below cap should enqueue");
            receivers.push(result_rx);
        }

        let waiter_err = match FILE_FDATASYNC_GROUP_COMMIT.enqueue(limiter, vec![PathBuf::from("overflow-waiter")]) {
            Ok(_) => panic!("waiter max+1 must fail closed"),
            Err(err) => err,
        };

        assert_eq!(waiter_err.kind(), io::ErrorKind::WouldBlock);
        clear_file_fdatasync_group_commit_for_test();
        drop(receivers);

        let mut receivers = Vec::new();
        let file_limit_limiter = Arc::new(Semaphore::new(1));
        let (result_rx, _worker) = FILE_FDATASYNC_GROUP_COMMIT
            .enqueue(
                file_limit_limiter.clone(),
                (0..MAX_FILE_FDATASYNC_BATCH_FILES)
                    .map(|index| PathBuf::from(format!("part-{index}")))
                    .collect(),
            )
            .expect("file count up to cap should enqueue");
        receivers.push(result_rx);

        let file_err = match FILE_FDATASYNC_GROUP_COMMIT.enqueue(file_limit_limiter, vec![PathBuf::from("overflow-file")]) {
            Ok(_) => panic!("file max+1 must fail closed"),
            Err(err) => err,
        };

        assert_eq!(file_err.kind(), io::ErrorKind::WouldBlock);
        clear_file_fdatasync_group_commit_for_test();
        assert_eq!(file_fdatasync_group_commit_counts_for_test(), (0, 0, 0));
        drop(receivers);
    }

    #[tokio::test]
    #[serial_test::serial(file_sync_probe)]
    async fn sync_dir_files_bounds_concurrency_across_directories() {
        let temp_dir = tempdir().expect("create temp dir");
        let directory_count = TEST_GLOBAL_FILE_SYNCS / MAX_PARALLEL_FILE_SYNCS + 1;
        let mut directories = Vec::with_capacity(directory_count);
        for directory_index in 0..directory_count {
            let directory = temp_dir.path().join(format!("disk.{directory_index}"));
            std::fs::create_dir(&directory).expect("create disk directory");
            for file_index in 0..PARALLEL_FILE_SYNC_THRESHOLD {
                std::fs::write(directory.join(format!("part.{file_index}")), b"shard").expect("write part");
            }
            directories.push(directory);
        }
        let _probe = file_sync_probe::set_blocking(temp_dir.path());
        let task = tokio::spawn(async move {
            futures::future::join_all(
                directories
                    .iter()
                    .map(|directory| sync_dir_files_with_limiter(directory, file_sync_limiter())),
            )
            .await
        });
        file_sync_probe::wait_for_active(TEST_GLOBAL_FILE_SYNCS).await;

        assert!(
            file_sync_probe::peak() > MAX_PARALLEL_FILE_SYNCS,
            "independent directories should share the global sync capacity"
        );
        assert!(
            file_sync_probe::peak() <= TEST_GLOBAL_FILE_SYNCS,
            "aggregate file sync concurrency must remain process-bounded"
        );
        file_sync_probe::release();
        let results = task.await.expect("join cross-directory file syncs");
        assert!(results.iter().all(std::result::Result::is_ok), "all directory syncs must succeed");
    }

    #[tokio::test]
    #[serial_test::serial(file_sync_probe)]
    async fn sync_dir_files_bounds_concurrency_per_disk() {
        let temp_dir = tempdir().expect("create temp dir");
        let mut directories = Vec::with_capacity(2);
        for directory_index in 0..2 {
            let directory = temp_dir.path().join(format!("disk.{directory_index}"));
            std::fs::create_dir(&directory).expect("create disk directory");
            for file_index in 0..PARALLEL_FILE_SYNC_THRESHOLD {
                std::fs::write(directory.join(format!("part.{file_index}")), b"shard").expect("write part");
            }
            directories.push(directory);
        }
        let _probe = file_sync_probe::set_blocking(temp_dir.path());
        let disk_permits = file_sync_limiter();
        let task = tokio::spawn(async move {
            futures::future::join_all(
                directories
                    .iter()
                    .map(|directory| sync_dir_files_with_limiter(directory, disk_permits.clone())),
            )
            .await
        });
        file_sync_probe::wait_for_active(MAX_PARALLEL_FILE_SYNCS).await;

        assert!(file_sync_probe::peak() > 1, "one disk should sync multiple files concurrently");
        assert!(
            file_sync_probe::peak() <= MAX_PARALLEL_FILE_SYNCS,
            "one disk must not exceed its own sync capacity"
        );
        file_sync_probe::release();
        let results = task.await.expect("join per-disk file syncs");
        assert!(results.iter().all(std::result::Result::is_ok), "all directory syncs must succeed");
    }

    #[tokio::test]
    #[serial_test::serial(file_sync_probe)]
    async fn sync_dir_files_acquires_disk_capacity_before_global_capacity() {
        let global_reservation = FILE_SYNC_PERMITS
            .acquire_many(TEST_GLOBAL_FILE_SYNCS as u32)
            .await
            .expect("global file sync limiter must remain open");
        let disk_permits = Arc::new(Semaphore::new(1));
        let mut acquisition = Box::pin(acquire_file_sync_permits(disk_permits.clone()));

        assert!(futures::poll!(&mut acquisition).is_pending());
        assert_eq!(
            disk_permits.available_permits(),
            0,
            "a waiter blocked on global capacity must already hold its disk permit"
        );

        drop(acquisition);
        assert_eq!(disk_permits.available_permits(), 1, "cancelling the waiter must return its disk permit");
        drop(global_reservation);
    }

    #[tokio::test]
    #[serial_test::serial(file_sync_probe)]
    async fn sync_dir_files_does_not_fsync_dir_after_sequential_file_failure() {
        let temp_dir = tempdir().expect("create temp dir");
        std::fs::write(temp_dir.path().join("part.1"), b"shard").expect("write part");
        let _probe = file_sync_probe::set_failing(temp_dir.path());

        let err = sync_dir_files_with_limiter(temp_dir.path(), file_sync_limiter())
            .await
            .expect_err("file sync failure must propagate");

        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert!(
            !fsync_dir_recorder::was_fsynced(temp_dir.path()),
            "directory must not be fsynced after a file sync failure"
        );
    }

    #[tokio::test]
    #[serial_test::serial(file_sync_probe)]
    async fn sync_dir_files_parallel_failure_stops_new_work_and_skips_dir_fsync() {
        let temp_dir = tempdir().expect("create temp dir");
        let file_count = TEST_GLOBAL_FILE_SYNCS * 16;
        for index in 0..file_count {
            std::fs::write(temp_dir.path().join(format!("part.{index}")), b"shard").expect("write part");
        }
        let _probe = file_sync_probe::set_failing_blocking(temp_dir.path());

        let err = sync_dir_files_with_limiter(temp_dir.path(), file_sync_limiter())
            .await
            .expect_err("parallel file sync failure must propagate");

        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert!(
            file_sync_probe::attempts() <= MAX_PARALLEL_FILE_SYNCS,
            "parallel sync must stop scheduling files after the first failure"
        );
        assert!(
            !fsync_dir_recorder::was_fsynced(temp_dir.path()),
            "directory must not be fsynced after a parallel file sync failure"
        );
        file_sync_probe::release();
        file_sync_probe::wait_for_idle().await;
    }

    #[tokio::test]
    #[serial_test::serial(file_sync_probe)]
    async fn sync_dir_files_cancellation_isolates_global_capacity_from_stuck_disk_work() {
        let temp_dir = tempdir().expect("create temp dir");
        for index in 0..PARALLEL_FILE_SYNC_THRESHOLD {
            std::fs::write(temp_dir.path().join(format!("part.{index}")), b"shard").expect("write part");
        }
        let _probe = file_sync_probe::set_blocking(temp_dir.path());
        let disk_permits = file_sync_limiter();
        let initial_disk_permits = disk_permits.available_permits();
        let global_reservation = FILE_SYNC_PERMITS
            .acquire_many((TEST_GLOBAL_FILE_SYNCS - MAX_PARALLEL_FILE_SYNCS) as u32)
            .await
            .expect("global file sync limiter must remain open");
        let path = temp_dir.path().to_path_buf();
        let task = tokio::spawn({
            let disk_permits = disk_permits.clone();
            async move { sync_dir_files_with_limiter(path, disk_permits).await }
        });
        file_sync_probe::wait_for_active(MAX_PARALLEL_FILE_SYNCS).await;

        task.abort();
        let join_err = task.await.expect_err("file sync task must be cancelled");

        assert!(join_err.is_cancelled(), "task abort must cancel the outer file sync future");
        assert_eq!(
            disk_permits.available_permits(),
            0,
            "detached blocking syncs must retain their per-disk permits"
        );
        let returned_global_permits = FILE_SYNC_PERMITS
            .try_acquire_many(MAX_PARALLEL_FILE_SYNCS as u32)
            .expect("cancelled work must return global capacity for healthy disks");
        file_sync_probe::release();
        file_sync_probe::wait_for_idle().await;
        let returned_disk_permits = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            disk_permits.clone().acquire_many_owned(initial_disk_permits as u32),
        )
        .await
        .expect("blocking syncs must return their per-disk permits")
        .expect("disk file sync limiter must remain open");
        drop(returned_disk_permits);
        drop(returned_global_permits);
        drop(global_reservation);
    }

    #[tokio::test]
    #[serial_test::serial(file_sync_probe)]
    async fn sync_dir_files_missing_dir_returns_not_found() {
        let temp_dir = tempdir().expect("create temp dir");
        let missing = temp_dir.path().join("missing");
        let _probe = file_sync_probe::set(temp_dir.path());

        let err = sync_dir_files(&missing).await.expect_err("missing dir must fail");
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }
}
