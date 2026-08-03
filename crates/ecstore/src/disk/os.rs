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
) -> Result<()> {
    reliable_rename(src_file_path, dst_file_path.as_ref(), base_dir)
        .await
        .map_err(to_file_error)?;

    Ok(())
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn rename_all_ignore_missing_source(
    src_file_path: impl AsRef<Path>,
    dst_file_path: impl AsRef<Path>,
    base_dir: impl AsRef<Path>,
) -> Result<()> {
    match reliable_rename_inner(src_file_path, dst_file_path.as_ref(), base_dir, false).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(to_file_error(err).into()),
    }
}

async fn reliable_rename(
    src_file_path: impl AsRef<Path>,
    dst_file_path: impl AsRef<Path>,
    base_dir: impl AsRef<Path>,
) -> io::Result<()> {
    reliable_rename_inner(src_file_path, dst_file_path, base_dir, true).await
}

async fn reliable_rename_inner(
    src_file_path: impl AsRef<Path>,
    dst_file_path: impl AsRef<Path>,
    base_dir: impl AsRef<Path>,
    warn_on_missing_source: bool,
) -> io::Result<()> {
    let mut i = 0;
    loop {
        let parent_guard = match dst_file_path.as_ref().parent() {
            Some(parent) => mkdir_all_below_existing_base(parent, base_dir.as_ref()).await.map(Some),
            None => Ok(None),
        };
        let result = match parent_guard {
            Ok(parent_guard) => {
                rename_into_existing_parent(src_file_path.as_ref(), dst_file_path.as_ref(), parent_guard.as_ref())
            }
            Err(err) => Err(err),
        };
        if let Err(e) = result {
            if should_retry_rename(&e, i) {
                i += 1;
                continue;
            }
            if warn_on_missing_source || e.kind() != io::ErrorKind::NotFound {
                warn_reliable_rename_failure(src_file_path.as_ref(), dst_file_path.as_ref(), base_dir.as_ref(), &e);
            }
            return Err(e);
        }

        break;
    }

    Ok(())
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
    src_file_path: &Path,
    dst_file_path: &Path,
    parent_guard: Option<&ExistingBaseDirectoryGuard>,
) -> io::Result<()> {
    use std::{
        mem::{offset_of, size_of},
        os::windows::{ffi::OsStrExt, fs::OpenOptionsExt, io::AsRawHandle},
    };
    use windows_sys::Win32::{
        Foundation::{ERROR_ACCESS_DENIED, ERROR_DIR_NOT_EMPTY},
        Storage::FileSystem::{
            DELETE, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_RENAME_INFO, FILE_RENAME_INFO_0,
            FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, FileRenameInfo, FileRenameInfoEx, SYNCHRONIZE,
            SetFileInformationByHandle,
        },
        System::WindowsProgramming::{FILE_RENAME_FLAG_POSIX_SEMANTICS, FILE_RENAME_FLAG_REPLACE_IF_EXISTS},
    };

    let Some(parent_guard) = parent_guard else {
        return super::fs::rename_std(src_file_path, dst_file_path);
    };
    let dst_parent = parent_guard
        .last()
        .ok_or_else(|| io::Error::other("rename destination parent guard is empty"))?;
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

    let source = std::fs::OpenOptions::new()
        .access_mode(DELETE | SYNCHRONIZE)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
        .open(src_file_path)?;

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

    // SAFETY: `source` and `buffer` stay live for the synchronous call, and
    // `buffer_size_u32` describes the initialized FILE_RENAME_INFO payload.
    let renamed =
        unsafe { SetFileInformationByHandle(source.as_raw_handle(), FileRenameInfo, rename_info.cast(), buffer_size_u32) };
    if renamed != 0 {
        return Ok(());
    }

    let legacy_error = io::Error::last_os_error();
    if legacy_error.raw_os_error().and_then(|code| u32::try_from(code).ok()) != Some(ERROR_ACCESS_DENIED) {
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

    let extended_error = io::Error::last_os_error();
    if extended_error.raw_os_error().and_then(|code| u32::try_from(code).ok()) == Some(ERROR_DIR_NOT_EMPTY) {
        Err(extended_error)
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

async fn mkdir_all_below_existing_base(dir_path: &Path, base_dir: &Path) -> io::Result<ExistingBaseDirectoryGuard> {
    let dir_path = dir_path.to_path_buf();
    let base_dir = base_dir.to_path_buf();

    tokio::task::spawn_blocking(move || mkdir_all_below_existing_base_std(&dir_path, &base_dir)).await?
}

#[cfg(windows)]
pub(crate) type ExistingBaseDirectoryGuard = Vec<winapi_util::Handle>;

#[cfg(unix)]
pub(crate) type ExistingBaseDirectoryGuard = Vec<std::os::fd::OwnedFd>;

#[cfg(all(not(unix), not(windows)))]
pub(crate) type ExistingBaseDirectoryGuard = ();

#[cfg(windows)]
fn open_windows_directory(path: &Path, share_mode: u32) -> io::Result<winapi_util::Handle> {
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
        FILE_READ_ATTRIBUTES, FILE_TRAVERSE,
    };

    let file = std::fs::OpenOptions::new()
        .access_mode(FILE_TRAVERSE | FILE_READ_ATTRIBUTES)
        .share_mode(share_mode)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)?;
    let handle = winapi_util::Handle::from_file(file);
    let info = winapi_util::file::information(&handle)?;
    if info.file_attributes() & u64::from(FILE_ATTRIBUTE_DIRECTORY) == 0
        || info.file_attributes() & u64::from(FILE_ATTRIBUTE_REPARSE_POINT) != 0
    {
        return Err(io::Error::from(io::ErrorKind::NotADirectory));
    }
    Ok(handle)
}

#[cfg(windows)]
fn lock_windows_directory(path: &Path) -> io::Result<winapi_util::Handle> {
    use windows_sys::Win32::Storage::FileSystem::{FILE_SHARE_READ, FILE_SHARE_WRITE};

    // Child creation and handle-relative publication require write sharing.
    // Omitting delete sharing keeps the opened directory identity anchored.
    open_windows_directory(path, FILE_SHARE_READ | FILE_SHARE_WRITE)
}

pub(crate) fn mkdir_all_below_existing_base_std(dir_path: &Path, base_dir: &Path) -> io::Result<ExistingBaseDirectoryGuard> {
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
        use windows_sys::Win32::Storage::FileSystem::FILE_SHARE_READ;

        let mut handles = vec![lock_windows_directory(base_dir)?];
        let mut paths = vec![base_dir.to_path_buf()];
        let mut current = base_dir.to_path_buf();
        for component in relative.components() {
            let Component::Normal(component) = component else {
                continue;
            };
            current.push(component);
            match std::fs::create_dir(&current) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {}
                Err(err) => return Err(err),
            }
            handles.push(lock_windows_directory(&current)?);
            paths.push(current.clone());
        }

        // A short read-only validation pass excludes concurrent writers while
        // confirming that every pathname still resolves to the anchored
        // directory opened during creation. The returned handles then allow
        // child writes but keep delete sharing disabled until publication.
        let validation_handles = paths
            .iter()
            .map(|path| open_windows_directory(path, FILE_SHARE_READ))
            .collect::<io::Result<Vec<_>>>()?;
        for (anchored, validated) in handles.iter().zip(&validation_handles) {
            let anchored_info = winapi_util::file::information(anchored)?;
            let validated_info = winapi_util::file::information(validated)?;
            if anchored_info.volume_serial_number() != validated_info.volume_serial_number()
                || anchored_info.file_index() != validated_info.file_index()
            {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "rename destination parent identity changed during validation",
                ));
            }
        }

        Ok(handles)
    }

    #[cfg(all(not(unix), not(windows)))]
    {
        let _ = relative;
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
/// retry does not recreate the missing source or base directory, so a second
/// attempt is guaranteed to fail identically. Destination-parent preparation
/// is already part of every attempt. Skipping it spares speculative
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
    // SAFETY: this test helper passes a valid live directory handle and a
    // fully initialized mount-point reparse buffer to synchronous DeviceIoControl.
    #[allow(unsafe_code)]
    fn try_set_windows_mount_point(directory: &winapi_util::Handle, target: &Path) -> io::Result<()> {
        use std::os::windows::{ffi::OsStrExt, io::AsRawHandle};
        use windows_sys::Win32::System::{
            IO::DeviceIoControl, Ioctl::FSCTL_SET_REPARSE_POINT, SystemServices::IO_REPARSE_TAG_MOUNT_POINT,
        };

        const VERBATIM_PREFIX: [u16; 4] = [b'\\' as u16, b'\\' as u16, b'?' as u16, b'\\' as u16];
        const NT_PREFIX: [u16; 4] = [b'\\' as u16, b'?' as u16, b'?' as u16, b'\\' as u16];
        const REPARSE_HEADER_SIZE: usize = 8;
        const MOUNT_POINT_HEADER_SIZE: usize = 8;

        let target = std::fs::canonicalize(target)?;
        let target_name = target.as_os_str().encode_wide().collect::<Vec<_>>();
        let target_without_prefix = target_name.strip_prefix(&VERBATIM_PREFIX).unwrap_or(&target_name);
        let substitute_name = NT_PREFIX
            .into_iter()
            .chain(target_without_prefix.iter().copied())
            .collect::<Vec<_>>();
        let substitute_name_bytes = substitute_name
            .len()
            .checked_mul(std::mem::size_of::<u16>())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "mount-point target is too long"))?;
        let print_name_bytes = target_name
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
        for unit in substitute_name.into_iter().chain([0]).chain(target_name).chain([0]) {
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
    #[test]
    fn windows_handle_relative_rename_ignores_parent_reparse_mutation() {
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
        std::fs::create_dir(&outside).expect("create mount-point target");
        std::fs::write(outside.join("marker"), b"outside").expect("write outside marker");
        let guard = mkdir_all_below_existing_base_std(&parent, &base).expect("guard destination parent");
        let writable_parent = std::fs::OpenOptions::new()
            .access_mode(GENERIC_WRITE)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
            .open(&parent)
            .map(winapi_util::Handle::from_file)
            .expect("open guarded parent for reparse mutation");
        try_set_windows_mount_point(&writable_parent, &outside)
            .expect("the publication guard must retain Windows write compatibility");
        assert_eq!(
            std::fs::read(parent.join("marker")).expect("read marker through mount point"),
            b"outside",
            "the test mutation must redirect pathname traversal"
        );

        let src = temp_dir.path().join("staged-object");
        std::fs::write(&src, b"payload").expect("write staged object");
        let result = rename_into_existing_parent(&src, &parent.join("xl.meta"), Some(&guard));
        let outside_was_untouched = !outside.join("xl.meta").exists();
        try_delete_windows_mount_point(&writable_parent).expect("remove test mount point without deleting its directory");
        drop(writable_parent);
        result.expect("handle-relative publication must survive pathname redirection");

        assert!(outside_was_untouched, "publication must not follow the mutated parent pathname");
        assert_eq!(std::fs::read(parent.join("xl.meta")).expect("read anchored publication"), b"payload");
        assert_eq!(std::fs::read(outside.join("marker")).expect("read outside marker"), b"outside");
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

        mkdir_all_below_existing_base(&base.join("object"), &base)
            .await
            .expect_err("symlinked base must be rejected");

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

        mkdir_all_below_existing_base(&base.join("linked/object"), &base)
            .await
            .expect_err("symlink below base must be rejected");

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
