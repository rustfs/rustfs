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

use crate::disk::error::DiskError;
use crate::disk::error::Result;
use crate::disk::error_conv::to_file_error;
use futures::TryStreamExt;
use parking_lot::Mutex;
use rustfs_utils::path::SLASH_SEPARATOR;
use std::{
    collections::HashMap,
    io,
    path::{Component, Path, PathBuf},
    sync::{Arc, LazyLock, Weak},
};
use tokio::fs;
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore, SemaphorePermit};
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
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;

    static RECORDED: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

    pub(crate) fn record(dir: &Path) {
        RECORDED.lock().expect("fsync dir recorder poisoned").push(dir.to_path_buf());
    }

    pub(crate) fn was_fsynced(dir: &Path) -> bool {
        RECORDED.lock().expect("fsync dir recorder poisoned").iter().any(|p| p == dir)
    }
}

#[cfg(all(test, windows))]
pub(crate) mod windows_rename_test_hooks {
    use super::*;

    type Hook = Box<dyn FnOnce() + Send>;

    static BEFORE_PUBLICATION: LazyLock<Mutex<HashMap<PathBuf, Hook>>> = LazyLock::new(|| Mutex::new(HashMap::new()));
    static BEFORE_RENAME_RETRY: LazyLock<Mutex<HashMap<PathBuf, Hook>>> = LazyLock::new(|| Mutex::new(HashMap::new()));
    static GUARD_GENERATIONS: LazyLock<Mutex<HashMap<PathBuf, Vec<u64>>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

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

/// Async wrapper around [`fsync_dir_std`]; runs the blocking fsync off the runtime.
pub async fn fsync_dir(dir: impl AsRef<Path>) -> io::Result<()> {
    let dir = dir.as_ref().to_path_buf();
    tokio::task::spawn_blocking(move || fsync_dir_std(dir)).await?
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
static DISK_VOLUME_MUTATION_LOCKS: LazyLock<Mutex<HashMap<PathBuf, Weak<RwLock<()>>>>> =
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

/// Always acquire the per-disk permit before the process-wide permit. Keeping
/// this order uniform prevents one slow disk from reserving global capacity
/// while it waits for its own concurrency slot.
async fn acquire_file_sync_permits(disk_permits: Arc<Semaphore>) -> io::Result<(OwnedSemaphorePermit, SemaphorePermit<'static>)> {
    let disk_permit = disk_permits
        .acquire_owned()
        .await
        .map_err(|_| io::Error::other("disk file sync concurrency limiter closed"))?;
    let global_permit = FILE_SYNC_PERMITS
        .acquire()
        .await
        .map_err(|_| io::Error::other("global file sync concurrency limiter closed"))?;
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
    let result = tokio::task::spawn_blocking(move || {
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

    static ROOT: RwLock<Option<PathBuf>> = RwLock::new(None);
    static BLOCK_MUTEX: Mutex<()> = Mutex::new(());
    static BLOCK_CONDVAR: Condvar = Condvar::new();
    static ACTIVE_CHANGED: Notify = Notify::const_new();
    static ACTIVE: AtomicUsize = AtomicUsize::new(0);
    static PEAK: AtomicUsize = AtomicUsize::new(0);
    static ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
    static FAIL_ON_ATTEMPT: AtomicUsize = AtomicUsize::new(usize::MAX);
    static BLOCK: AtomicBool = AtomicBool::new(false);
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
            *ROOT.write().expect("file sync probe lock poisoned") = None;
        }
    }

    fn configure(root: &Path, fail_on_attempt: Option<usize>, block: bool) -> ProbeGuard {
        ACTIVE.store(0, Ordering::SeqCst);
        PEAK.store(0, Ordering::SeqCst);
        ATTEMPTS.store(0, Ordering::SeqCst);
        FAIL_ON_ATTEMPT.store(fail_on_attempt.unwrap_or(usize::MAX), Ordering::SeqCst);
        {
            let _guard = BLOCK_MUTEX.lock().expect("file sync probe blocker poisoned");
            BLOCK.store(block, Ordering::SeqCst);
        }
        *ROOT.write().expect("file sync probe lock poisoned") = Some(root.to_path_buf());
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
        let enabled = ROOT
            .read()
            .expect("file sync probe lock poisoned")
            .as_ref()
            .is_some_and(|root| path.starts_with(root));
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
}

fn sync_file(path: &Path) -> io::Result<()> {
    #[cfg(test)]
    let _probe = file_sync_probe::enter(path);
    #[cfg(test)]
    if _probe.as_ref().is_some_and(file_sync_probe::ActiveGuard::should_fail) {
        return Err(io::Error::other("injected file sync failure"));
    }
    std::fs::File::open(path)?.sync_data()
}

fn sync_files(paths: &[PathBuf]) -> io::Result<()> {
    for path in paths {
        sync_file(path)?;
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
pub async fn sync_dir_files(dir: impl AsRef<Path>) -> io::Result<()> {
    sync_dir_files_with_limiter(dir, Arc::new(Semaphore::new(MAX_PARALLEL_FILE_SYNCS))).await
}

pub(crate) async fn sync_dir_files_with_limiter(dir: impl AsRef<Path>, disk_permits: Arc<Semaphore>) -> io::Result<()> {
    let dir = dir.as_ref().to_path_buf();
    let scan_dir = dir.clone();
    let files = run_file_sync_blocking(disk_permits.clone(), move || {
        let files = regular_files(&scan_dir)?;
        if files.len() < PARALLEL_FILE_SYNC_THRESHOLD {
            sync_files(&files)?;
            fsync_dir_std(scan_dir)?;
            return Ok(None);
        }
        Ok::<_, io::Error>(Some(files))
    })
    .await?;

    let Some(files) = files else {
        return Ok(());
    };
    futures::stream::iter(files.into_iter().map(Ok::<_, io::Error>))
        .try_for_each_concurrent(MAX_PARALLEL_FILE_SYNCS, |path| {
            let disk_permits = disk_permits.clone();
            async move { run_file_sync_blocking(disk_permits, move || sync_file(&path)).await }
        })
        .await?;
    run_file_sync_blocking(disk_permits, move || fsync_dir_std(dir)).await
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

#[tracing::instrument(level = "debug", skip_all)]
pub async fn rename_all_ignore_missing_source(
    src_file_path: impl AsRef<Path>,
    dst_file_path: impl AsRef<Path>,
    base_dir: impl AsRef<Path>,
    publication_root: &PublicationRoot,
) -> Result<()> {
    match reliable_rename_inner(src_file_path, dst_file_path.as_ref(), base_dir, publication_root, false).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(to_file_error(err).into()),
    }
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
    let operation = || {
        let (preparation, attempt) = prepare_rename_with_retry(&src_file_path, &dst_file_path, &base_dir, publication_root)?;
        rename_prepared(&src_file_path, &dst_file_path, &preparation, attempt)
    };
    let result = run_blocking_namespace_operation(operation);
    if let Err(err) = &result
        && (warn_on_missing_source || err.kind() != io::ErrorKind::NotFound)
    {
        warn_reliable_rename_failure(&src_file_path, &dst_file_path, &base_dir, err);
    }
    result
}

/// Run one blocking namespace transaction without detaching it from its task.
///
/// A cancelled `spawn_blocking` future leaves its closure running after the
/// caller's namespace lock has been released. Keep directory preparation and
/// publication in the operation task; a multi-thread runtime can replace the
/// blocked worker while a current-thread runtime must finish the short local
/// filesystem transaction before observing cancellation.
pub(crate) fn run_blocking_namespace_operation<T>(operation: impl FnOnce() -> io::Result<T>) -> io::Result<T> {
    match tokio::runtime::Handle::current().runtime_flavor() {
        tokio::runtime::RuntimeFlavor::MultiThread => tokio::task::block_in_place(operation),
        _ => operation(),
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
    let source_parent_guard = lock_windows_directory_tree(source_parent, publication_root)?;
    let (source_identity_anchor, expected_source_identity) =
        open_windows_rename_source_identity(src_file_path, &source_parent_guard)?;
    let mut attempt = 0;
    let parent_guard = loop {
        let result = dst_file_path
            .parent()
            .map(|parent| mkdir_all_below_existing_base_std(parent, base_dir, publication_root))
            .transpose();
        match result {
            Ok(parent_guard) => break parent_guard,
            Err(err) if should_retry_rename(&err, attempt) => {
                #[cfg(test)]
                windows_rename_test_hooks::run_before_rename_retry(dst_file_path);
                attempt += 1;
            }
            Err(err) => return Err(err),
        }
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
    mut attempt: usize,
) -> io::Result<()> {
    loop {
        #[cfg(all(test, windows))]
        if let Some(parent_guard) = preparation.parent_guard.as_ref() {
            windows_rename_test_hooks::record_guard_generation(dst_file_path, parent_guard.generation);
        }
        #[cfg(windows)]
        let rename_result = rename_into_existing_parent(dst_file_path, preparation.parent_guard.as_ref(), &preparation.source);
        #[cfg(not(windows))]
        let rename_result = rename_into_existing_parent(_src_file_path, dst_file_path, preparation.parent_guard.as_ref());
        match rename_result {
            Ok(()) => return Ok(()),
            Err(err) if should_retry_rename(&err, attempt) => {
                #[cfg(all(test, windows))]
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
        return super::fs::rename_std(src_file_path, dst_file_path);
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

    renameat(&src_parent, src_name, dst_parent, dst_name).map_err(io::Error::from)
}

#[cfg(windows)]
// SAFETY: this helper builds the variable-length FILE_RENAME_INFO buffer with
// checked sizes and passes borrowed live handles only to synchronous Win32 calls.
#[allow(unsafe_code)]
fn rename_into_existing_parent(
    dst_file_path: &Path,
    parent_guard: Option<&ExistingBaseDirectoryGuard>,
    source: &winapi_util::Handle,
) -> io::Result<()> {
    use std::{
        mem::{offset_of, size_of},
        os::windows::{ffi::OsStrExt, io::AsRawHandle},
    };
    use windows_sys::Win32::{
        Foundation::{ERROR_ACCESS_DENIED, ERROR_DIR_NOT_EMPTY, GetLastError},
        Storage::FileSystem::{
            FILE_RENAME_INFO, FILE_RENAME_INFO_0, FileRenameInfo, FileRenameInfoEx, SetFileInformationByHandle,
        },
        System::WindowsProgramming::{FILE_RENAME_FLAG_POSIX_SEMANTICS, FILE_RENAME_FLAG_REPLACE_IF_EXISTS},
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
    let buffer_size = offset_of!(FILE_RENAME_INFO, FileName)
        .checked_add(file_name_bytes)
        .and_then(|size| size.checked_add(size_of::<u16>()))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "rename information buffer is too large"))?;
    let buffer_size_u32 = u32::try_from(buffer_size)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "rename information buffer is too large"))?;
    let words = buffer_size.div_ceil(size_of::<usize>());
    let mut buffer = vec![0usize; words];
    let rename_info = buffer.as_mut_ptr().cast::<FILE_RENAME_INFO>();

    // SAFETY: `buffer` is aligned for FILE_RENAME_INFO and large enough for
    // its header, the complete UTF-16 name, and trailing zeroed storage.
    // `dst_parent` and `source` remain live until the synchronous call returns.
    unsafe {
        (*rename_info).Anonymous = FILE_RENAME_INFO_0 { ReplaceIfExists: true };
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
    let renamed =
        unsafe { SetFileInformationByHandle(source.as_raw_handle(), FileRenameInfo, rename_info.cast(), buffer_size_u32) };
    if renamed != 0 {
        return Ok(());
    }

    let legacy_error_code = unsafe { GetLastError() };
    let legacy_error = io::Error::from_raw_os_error(legacy_error_code as i32);
    if legacy_error_code != ERROR_ACCESS_DENIED {
        return Err(legacy_error);
    }

    // Match std::fs::rename's Windows 10 fallback for read-only or open
    // destinations while retaining the guarded, handle-relative target.
    unsafe {
        (*rename_info).Anonymous = FILE_RENAME_INFO_0 {
            Flags: FILE_RENAME_FLAG_REPLACE_IF_EXISTS | FILE_RENAME_FLAG_POSIX_SEMANTICS,
        };
    }
    let renamed =
        unsafe { SetFileInformationByHandle(source.as_raw_handle(), FileRenameInfoEx, rename_info.cast(), buffer_size_u32) };
    if renamed != 0 {
        return Ok(());
    }

    let extended_error_code = unsafe { GetLastError() };
    if extended_error_code == ERROR_DIR_NOT_EMPTY {
        Err(io::Error::from_raw_os_error(extended_error_code as i32))
    } else {
        Err(legacy_error)
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
    directory: WindowsDirectoryHandle,
}

impl PublicationRoot {
    pub(crate) fn new(path: &Path) -> io::Result<Self> {
        if !path.is_absolute() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "publication root must be absolute"));
        }

        #[cfg(windows)]
        let (path, directory) = open_windows_publication_root(path)?;

        Ok(Self {
            path: path.to_path_buf(),
            #[cfg(windows)]
            directory,
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(windows)]
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
    use windows_sys::Win32::Storage::FileSystem::{FILE_NAME_NORMALIZED, VOLUME_NAME_DOS, VOLUME_NAME_GUID};

    windows_final_path_with_guid_fallback(windows_final_path_with_flags(handle, FILE_NAME_NORMALIZED | VOLUME_NAME_DOS), || {
        windows_final_path_with_flags(handle, FILE_NAME_NORMALIZED | VOLUME_NAME_GUID)
    })
}

#[cfg(windows)]
fn windows_final_path_with_guid_fallback(
    dos_path: io::Result<PathBuf>,
    guid_path: impl FnOnce() -> io::Result<PathBuf>,
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
            guid_path()
        }
        Err(err) => Err(err),
    }
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
fn lock_windows_directory_tree(path: &Path, publication_root: &PublicationRoot) -> io::Result<ExistingBaseDirectoryGuard> {
    use windows_sys::Wdk::Storage::FileSystem::FILE_OPEN;

    let relative = path.strip_prefix(&publication_root.path).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidInput, "guarded Windows path must remain below its publication root")
    })?;
    let mut handles = vec![publication_root.directory.clone()];

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
        handles.push(open_windows_directory_component(parent, component, FILE_OPEN)?);
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
fn open_windows_directory_component(
    parent: &WindowsDirectoryHandle,
    component: &std::ffi::OsStr,
    create_disposition: u32,
) -> io::Result<WindowsDirectoryHandle> {
    use windows_sys::{
        Wdk::Storage::FileSystem::{FILE_DIRECTORY_FILE, FILE_OPEN_REPARSE_POINT},
        Win32::Storage::FileSystem::{
            FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT, FILE_READ_ATTRIBUTES, FILE_SHARE_READ, FILE_TRAVERSE,
        },
    };

    // Omitting write and delete sharing prevents the component from becoming a
    // reparse point, being renamed, or being removed while publication uses it.
    let anchor = open_windows_relative(
        &parent.handle,
        component,
        FILE_TRAVERSE | FILE_READ_ATTRIBUTES,
        FILE_SHARE_READ,
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
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "rename source must be an ordinary file or a Windows data-dedup entry",
        ));
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
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "rename source must be an ordinary file or a Windows data-dedup entry",
        ));
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
    use windows_sys::Win32::Storage::FileSystem::{FILE_ID_INFO, FileIdInfo, GetFileInformationByHandleEx};

    let mut identity = FILE_ID_INFO::default();
    let identity_size = u32::try_from(size_of::<FILE_ID_INFO>())
        .map_err(|_| io::Error::other("Windows file identity information size exceeds u32"))?;
    let queried = unsafe {
        GetFileInformationByHandleEx(handle.as_raw_handle(), FileIdInfo, std::ptr::addr_of_mut!(identity).cast(), identity_size)
    };
    if queried != 0 && identity.FileId.Identifier != [0; 16] {
        return Ok((identity.VolumeSerialNumber, identity.FileId.Identifier));
    }

    // FileIdInfo is unavailable on a few older or non-NTFS filesystems. Keep
    // the source identity pinned by its live anchor handle and compare the
    // legacy volume/file index instead of silently disabling the check.
    let information = winapi_util::file::information(handle)?;
    let mut file_id = [0; 16];
    file_id[..size_of::<u64>()].copy_from_slice(&information.file_index().to_ne_bytes());
    Ok((information.volume_serial_number(), file_id))
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

        let base_relative = base_dir.strip_prefix(&publication_root.path).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "rename base directory must remain below its publication root",
            )
        })?;
        let mut guard = ExistingBaseDirectoryGuard::new(vec![publication_root.directory.clone()]);
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

/// Check if a file exists.
/// Returns true if the file exists, false otherwise.
#[tracing::instrument(level = "debug", skip_all)]
pub fn file_exists(path: impl AsRef<Path>) -> bool {
    std::fs::metadata(path.as_ref()).map(|_| true).unwrap_or(false)
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
    use std::sync::Mutex;
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
    fn windows_final_path_falls_back_to_a_volume_guid_when_no_dos_name_exists() {
        use windows_sys::Win32::Foundation::{ERROR_ACCESS_DENIED, ERROR_PATH_NOT_FOUND};

        let guid_path = PathBuf::from(r"\\?\Volume{11111111-2222-3333-4444-555555555555}\data");
        let resolved = windows_final_path_with_guid_fallback(
            Err(io::Error::from_raw_os_error(
                i32::try_from(ERROR_PATH_NOT_FOUND).expect("Windows error code should fit i32"),
            )),
            || Ok(guid_path.clone()),
        )
        .expect("a volume without a DOS name should use its GUID path");
        assert_eq!(resolved, guid_path);

        let access_denied = i32::try_from(ERROR_ACCESS_DENIED).expect("Windows error code should fit i32");
        let err = windows_final_path_with_guid_fallback(Err(io::Error::from_raw_os_error(access_denied)), || {
            panic!("non-path errors must not be hidden by a GUID retry")
        })
        .expect_err("a non-path error should be preserved");
        assert_eq!(err.raw_os_error(), Some(access_denied));
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
        let base = resolved_root.join("bucket");
        let src = resolved_root.join("staging");
        let dst = base.join("object");
        std::fs::create_dir(&base).expect("create bucket through configured root");
        std::fs::write(&src, b"payload").expect("write staged object through configured root");
        super::rename_all(&src, &dst, &base, &publication_root)
            .await
            .expect("publish relative to the pinned configured root");

        assert_eq!(std::fs::read(target.join("bucket/object")).expect("read target publication"), b"payload");
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
    async fn windows_rename_all_blocks_parent_reparse_mutation_during_publication() {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::{
            Foundation::GENERIC_WRITE,
            Storage::FileSystem::{FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ, FILE_SHARE_WRITE},
        };

        let temp_dir = tempdir().expect("create temp dir");
        let base = temp_dir.path().join("bucket");
        let parent = base.join("object");
        std::fs::create_dir_all(&parent).expect("create destination parent");
        let src = temp_dir.path().join("staged-object");
        let dst = parent.join("xl.meta");
        std::fs::write(&src, b"payload").expect("write staged object");

        let parent_for_hook = parent.clone();
        windows_rename_test_hooks::install_before_publication(&dst, move || {
            std::fs::OpenOptions::new()
                .access_mode(GENERIC_WRITE)
                .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
                .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
                .open(&parent_for_hook)
                .expect_err("the destination guard must exclude a reparse writer");
        });

        rename_all(&src, &dst, &base)
            .await
            .expect("publication must succeed while the parent identity is frozen");

        assert_eq!(std::fs::read(&dst).expect("read protected publication"), b"payload");
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

        rename_all(&src, &dst, &base)
            .await
            .expect_err("a reparse destination base must be rejected");

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

        rename_all(&src, &dst, &base)
            .await
            .expect_err("a reparse destination intermediate must be rejected");

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

        rename_all(&src, &dst, &base)
            .await
            .expect_err("a reparse ancestor before a nested base must be rejected");

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

        rename_all(&src, &dst, &base)
            .await
            .expect_err("a reparse ancestor before the source parent must be rejected");

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
    async fn windows_rename_all_does_not_detach_preparation_after_cancellation() {
        use std::os::windows::fs::OpenOptionsExt;
        use std::sync::mpsc;
        use windows_sys::Win32::Storage::FileSystem::{FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE};

        let temp_dir = tempdir().expect("create temp dir");
        let base = temp_dir.path().join("bucket");
        let src = temp_dir.path().join("staged-object");
        let dst = base.join("object");
        std::fs::create_dir(&base).expect("create destination base");
        std::fs::write(&src, b"payload").expect("write staged object");
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

        let source = src.clone();
        let destination = dst.clone();
        let mut rename = tokio::spawn(async move { rename_all(&src, &dst, &base).await });
        entered_rx.await.expect("preparation must start before cancellation");
        rename.abort();
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(
            !rename.is_finished(),
            "cancellation must not detach preparation that can still create destination parents"
        );
        release_tx.send(()).expect("release preparation after cancellation");

        match (&mut rename).await {
            Ok(Ok(())) => assert_eq!(std::fs::read(destination).expect("read published object"), b"payload"),
            Ok(Err(err)) => panic!("an uncancelled preparation must not fail: {err:?}"),
            Err(err) if err.is_cancelled() => {
                assert!(source.exists(), "cancellation before publication must retain the source");
                assert!(!destination.exists(), "cancellation before publication must not create the destination");
            }
            Err(err) => panic!("preparation task failed unexpectedly: {err}"),
        }
    }

    #[cfg(windows)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn windows_rename_all_does_not_detach_started_publication_after_cancellation() {
        use std::sync::mpsc;

        let temp_dir = tempdir().expect("create temp dir");
        let base = temp_dir.path().join("bucket");
        let src = temp_dir.path().join("staged-object");
        let dst = base.join("object");
        std::fs::create_dir(&base).expect("create destination base");
        std::fs::write(&src, b"payload").expect("write staged object");

        let (release_tx, release_rx) = mpsc::channel();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        windows_rename_test_hooks::install_before_publication(&dst, move || {
            entered_tx.send(()).expect("signal publication hook entry");
            release_rx.recv().expect("wait until the operation has been cancelled");
        });

        let destination = dst.clone();
        let mut rename = tokio::spawn(async move { rename_all(&src, &dst, &base).await });
        entered_rx.await.expect("publication must start before cancellation");
        rename.abort();
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(
            !rename.is_finished(),
            "cancellation must not detach a namespace mutation that has already started"
        );
        release_tx.send(()).expect("release publication after cancellation");

        let _ = (&mut rename).await;
        assert_eq!(std::fs::read(destination).expect("read published object"), b"payload");
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
            lock_windows_directory_tree(src.parent().expect("source path must have a parent"), &publication_root)
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
