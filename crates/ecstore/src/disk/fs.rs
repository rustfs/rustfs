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

use std::{
    collections::HashMap,
    fs::Metadata,
    path::{Path, PathBuf},
    sync::Mutex,
    time::{Duration, Instant},
};
use tokio::{fs::File, io};

async fn native_io<T: Send + 'static>(operation: impl FnOnce() -> io::Result<T> + Send + 'static) -> io::Result<T> {
    super::io_admission::spawn_blocking_with_resource_ownership(operation)
        .await
        .map_err(|_| io::Error::other("background task failed"))?
}

#[cfg(not(windows))]
pub fn same_file(f1: &Metadata, f2: &Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;

    if f1.dev() != f2.dev() {
        return false;
    }

    if f1.ino() != f2.ino() {
        return false;
    }

    if f1.size() != f2.size() {
        return false;
    }
    if f1.permissions() != f2.permissions() {
        return false;
    }

    if f1.mtime() != f2.mtime() {
        return false;
    }

    true
}

#[cfg(windows)]
pub fn same_file(f1: &Metadata, f2: &Metadata) -> bool {
    if f1.permissions() != f2.permissions() {
        return false;
    }

    if f1.file_type() != f2.file_type() {
        return false;
    }

    if f1.len() != f2.len() {
        return false;
    }
    true
}

type FileMode = usize;

pub const O_RDONLY: FileMode = 0x00000;
pub const O_WRONLY: FileMode = 0x00001;
pub const O_RDWR: FileMode = 0x00002;
pub const O_CREATE: FileMode = 0x00040;
// pub const O_EXCL: FileMode = 0x00080;
// pub const O_NOCTTY: FileMode = 0x00100;
pub const O_TRUNC: FileMode = 0x00200;
// pub const O_NONBLOCK: FileMode = 0x00800;
pub const O_APPEND: FileMode = 0x00400;
// pub const O_SYNC: FileMode = 0x01000;
// pub const O_ASYNC: FileMode = 0x02000;
// pub const O_CLOEXEC: FileMode = 0x80000;

//      read: bool,
//     write: bool,
//     append: bool,
//     truncate: bool,
//     create: bool,
//     create_new: bool,

pub async fn open_file(path: impl AsRef<Path>, mode: FileMode) -> io::Result<File> {
    if mode & O_CREATE != 0 {
        super::os::record_native_publication(path.as_ref());
    }
    let path = path.as_ref().to_path_buf();
    native_io(move || {
        let mut options = std::fs::OpenOptions::new();
        match mode & (O_RDONLY | O_WRONLY | O_RDWR) {
            O_WRONLY => options.write(true),
            O_RDWR => options.read(true).write(true),
            _ => options.read(true),
        }
        .create(mode & O_CREATE != 0)
        .append(mode & O_APPEND != 0)
        .truncate(mode & O_TRUNC != 0);
        #[cfg(all(any(test, feature = "test-util"), not(windows)))]
        if mode == O_RDONLY {
            super::os::prepared_publication_test_hooks::run(
                super::os::prepared_publication_test_hooks::Stage::NativeReadOpen,
                &path,
            );
        }
        options.open(path).map(File::from_std)
    })
    .await
}

pub async fn access(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref().to_path_buf();
    native_io(move || std::fs::metadata(path).map(|_| ())).await
}

pub fn access_std(path: impl AsRef<Path>) -> io::Result<()> {
    std::fs::metadata(path)?;
    Ok(())
}

pub async fn lstat(path: impl AsRef<Path>) -> io::Result<Metadata> {
    let path = path.as_ref().to_path_buf();
    native_io(move || std::fs::symlink_metadata(path)).await
}

pub fn lstat_std(path: impl AsRef<Path>) -> io::Result<Metadata> {
    std::fs::symlink_metadata(path)
}

pub fn create_dir_all_std(path: &Path) -> io::Result<()> {
    match std::fs::create_dir(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => std::fs::create_dir_all(path),
        Err(e) => Err(e),
    }
}

pub async fn make_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    super::os::record_native_publication(path.as_ref());
    let path = path.as_ref().to_path_buf();
    native_io(move || create_dir_all_std(&path)).await
}

fn is_dir_error(e: &io::Error) -> bool {
    e.raw_os_error() == Some(libc::EISDIR)
        || e.kind() == io::ErrorKind::IsADirectory
        // macOS: remove_file on a directory returns EPERM
        || (cfg!(target_os = "macos") && e.raw_os_error() == Some(libc::EPERM))
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn remove(path: impl AsRef<Path>) -> io::Result<()> {
    // Try remove_file first; fall back to remove_dir if it's a directory
    let file_path = path.as_ref().to_path_buf();
    match native_io(move || std::fs::remove_file(file_path)).await {
        Ok(()) => Ok(()),
        Err(e) if is_dir_error(&e) => {
            let path = path.as_ref().to_path_buf();
            native_io(move || std::fs::remove_dir(path)).await
        }
        Err(e) => Err(e),
    }
}

#[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
pub async fn remove_all(path: impl AsRef<Path>) -> io::Result<()> {
    // Try remove_file first; fall back to remove_dir_all if it's a directory
    let file_path = path.as_ref().to_path_buf();
    match native_io(move || std::fs::remove_file(file_path)).await {
        Ok(()) => Ok(()),
        Err(e) if is_dir_error(&e) => {
            let path = path.as_ref().to_path_buf();
            native_io(move || std::fs::remove_dir_all(path)).await
        }
        Err(e) => Err(e),
    }
}

#[tracing::instrument(level = "debug", skip_all)]
pub fn remove_std(path: impl AsRef<Path>) -> io::Result<()> {
    // Try remove_file first; fall back to remove_dir if it's a directory
    match std::fs::remove_file(path.as_ref()) {
        Ok(()) => Ok(()),
        Err(e) if is_dir_error(&e) => std::fs::remove_dir(path.as_ref()),
        Err(e) => Err(e),
    }
}

pub fn remove_all_std(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref();
    let meta = std::fs::metadata(path)?;
    if meta.is_dir() {
        std::fs::remove_dir_all(path)
    } else {
        std::fs::remove_file(path)
    }
}

#[cfg(test)]
pub async fn mkdir(path: impl AsRef<Path>) -> io::Result<()> {
    super::os::record_native_publication(path.as_ref());
    let path = path.as_ref().to_path_buf();
    native_io(move || std::fs::create_dir(path)).await
}

pub async fn rename(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
    super::os::record_native_publication(to.as_ref());
    let from = from.as_ref().to_path_buf();
    let to = to.as_ref().to_path_buf();
    native_io(move || std::fs::rename(from, to)).await
}

#[cfg(any(not(windows), test))]
pub fn rename_std(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
    super::os::record_native_publication(to.as_ref());
    std::fs::rename(from, to)
}

// Bucket existence cache - reduces statx syscalls for repeated bucket checks

/// Cache for bucket directory existence checks.
struct BucketExistenceCache {
    cache: Mutex<HashMap<PathBuf, (Instant, bool)>>,
    ttl: Duration,
}

impl BucketExistenceCache {
    fn new(ttl: Duration) -> Self {
        Self {
            cache: Mutex::new(HashMap::new()),
            ttl,
        }
    }

    fn check_exists(&self, path: &Path) -> Option<bool> {
        let mut cache = self.cache.lock().ok()?;
        if let Some((timestamp, exists)) = cache.get(path) {
            if timestamp.elapsed() < self.ttl {
                return Some(*exists);
            }
            cache.remove(path);
        }
        None
    }

    fn record(&self, path: PathBuf, exists: bool) {
        if let Ok(mut cache) = self.cache.lock() {
            cache.insert(path, (Instant::now(), exists));
        }
    }

    fn invalidate(&self, path: &Path) {
        if let Ok(mut cache) = self.cache.lock() {
            cache.remove(path);
        }
    }
}

static BUCKET_EXISTENCE_CACHE: std::sync::LazyLock<BucketExistenceCache> =
    std::sync::LazyLock::new(|| BucketExistenceCache::new(Duration::from_secs(60)));

/// Cached access check - reduces statx syscalls
pub async fn cached_access(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref();

    if let Some(exists) = BUCKET_EXISTENCE_CACHE.check_exists(path) {
        if exists {
            return Ok(());
        }
        return Err(io::Error::new(io::ErrorKind::NotFound, "bucket not found (cached)"));
    }

    let path_buf = path.to_path_buf();
    let result = access(&path_buf).await;

    match &result {
        Ok(_) => BUCKET_EXISTENCE_CACHE.record(path_buf, true),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            BUCKET_EXISTENCE_CACHE.record(path_buf, false);
        }
        _ => {}
    }

    result?;
    Ok(())
}

pub fn invalidate_bucket_cache(path: impl AsRef<Path>) {
    crate::bucket::metadata_sys::bump_local_meta_mutation_gen();
    BUCKET_EXISTENCE_CACHE.invalidate(path.as_ref());
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tokio::io::AsyncWriteExt;

    #[test]
    fn cancelled_filesystem_jobs_retain_physical_resources() {
        use crate::disk::io_admission::{test_resource_units, with_optional_resource_unit};

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .expect("single native worker");
        runtime.block_on(async {
            let mut observations = Vec::new();
            for count in 1..=2 {
                for operation in [
                    "access",
                    "cached_access",
                    "lstat",
                    "mkdir",
                    "make_dir_all",
                    "remove",
                    "remove_all",
                    "rename",
                    "open",
                ] {
                    let dir = TempDir::new().expect("filesystem ownership fixture");
                    let source = dir.path().join("source");
                    let target = dir.path().join("target");
                    match operation {
                        "mkdir" | "make_dir_all" | "open" => {}
                        "remove_all" => {
                            std::fs::create_dir(&source).expect("recursive removal directory");
                            std::fs::write(source.join("child"), b"payload").expect("recursive removal child");
                        }
                        _ => std::fs::write(&source, b"payload").expect("existing file"),
                    }
                    let (resources, gates) = test_resource_units(count);
                    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
                    let (release_tx, release_rx) = std::sync::mpsc::channel();
                    let blocker = tokio::task::spawn_blocking(move || {
                        let _ = started_tx.send(());
                        let _ = release_rx.recv();
                    });
                    started_rx.await.expect("native worker occupied");
                    let mut future = Box::pin(with_optional_resource_unit(Some(resources), async {
                        match operation {
                            "access" => access(&source).await,
                            "cached_access" => cached_access(&source).await,
                            "lstat" => lstat(&source).await.map(|_| ()),
                            "mkdir" => mkdir(&source).await,
                            "make_dir_all" => make_dir_all(source.join("child")).await,
                            "remove" => remove(&source).await,
                            "remove_all" => remove_all(&source).await,
                            "rename" => rename(&source, &target).await,
                            "open" => open_file(&source, O_CREATE | O_WRONLY).await.map(|_| ()),
                            _ => unreachable!("fixed operation list"),
                        }
                    }));
                    assert!(futures::poll!(&mut future).is_pending(), "{operation}: native job queued");
                    drop(future);
                    let held: Vec<_> = gates.iter().map(|gate| gate.snapshot().active).collect();
                    release_tx.send(()).expect("release native worker before assertions");
                    blocker.await.expect("native blocker completed");
                    tokio::task::spawn_blocking(|| ()).await.expect("native queue drained");
                    assert!(gates.iter().all(|gate| gate.snapshot().active == 0));
                    match operation {
                        "mkdir" => assert!(source.is_dir()),
                        "make_dir_all" => assert!(source.join("child").is_dir()),
                        "open" => assert!(source.is_file()),
                        "remove" => assert!(!source.exists()),
                        "rename" => {
                            assert!(!source.exists());
                            assert_eq!(std::fs::read(&target).expect("renamed file"), b"payload");
                        }
                        _ => {}
                    }
                    observations.push((count, operation, held));
                }
            }
            for (count, operation, held) in observations {
                assert_eq!(held, vec![1; count], "{operation}: queued native job keeps its physical resources");
            }
        });
    }

    #[tokio::test]
    async fn test_file_mode_constants() {
        assert_eq!(O_RDONLY, 0x00000);
        assert_eq!(O_WRONLY, 0x00001);
        assert_eq!(O_RDWR, 0x00002);
        assert_eq!(O_CREATE, 0x00040);
        assert_eq!(O_TRUNC, 0x00200);
        assert_eq!(O_APPEND, 0x00400);
    }

    #[tokio::test]
    async fn test_open_file_read_only() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_readonly.txt");

        // Create a test file
        tokio::fs::write(&file_path, b"test content").await.unwrap();

        // Test opening in read-only mode
        let file = open_file(&file_path, O_RDONLY).await;
        assert!(file.is_ok());
    }

    #[tokio::test]
    async fn test_open_file_write_only() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_writeonly.txt");

        // Test opening in write-only mode with create flag
        let mut file = open_file(&file_path, O_WRONLY | O_CREATE).await.unwrap();

        // Should be able to write
        file.write_all(b"write test").await.unwrap();
        file.flush().await.unwrap();
    }

    #[tokio::test]
    async fn test_open_file_read_write() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_readwrite.txt");

        // Test opening in read-write mode with create flag
        let mut file = open_file(&file_path, O_RDWR | O_CREATE).await.unwrap();

        // Should be able to write and read
        file.write_all(b"read-write test").await.unwrap();
        file.flush().await.unwrap();
    }

    #[tokio::test]
    async fn test_open_file_append() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_append.txt");

        // Create initial content
        tokio::fs::write(&file_path, b"initial").await.unwrap();

        // Open in append mode
        let mut file = open_file(&file_path, O_WRONLY | O_APPEND).await.unwrap();
        file.write_all(b" appended").await.unwrap();
        file.flush().await.unwrap();

        // Verify content
        let content = tokio::fs::read_to_string(&file_path).await.unwrap();
        assert_eq!(content, "initial appended");
    }

    #[tokio::test]
    async fn test_open_file_truncate() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_truncate.txt");

        // Create initial content
        tokio::fs::write(&file_path, b"initial content").await.unwrap();

        // Open with truncate flag
        let mut file = open_file(&file_path, O_WRONLY | O_TRUNC).await.unwrap();
        file.write_all(b"new").await.unwrap();
        file.flush().await.unwrap();

        // Verify content was truncated
        let content = tokio::fs::read_to_string(&file_path).await.unwrap();
        assert_eq!(content, "new");
    }

    #[tokio::test]
    async fn test_access() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_access.txt");

        // Should fail for non-existent file
        assert!(access(&file_path).await.is_err());

        // Create file and test again
        tokio::fs::write(&file_path, b"test").await.unwrap();
        assert!(access(&file_path).await.is_ok());
    }

    #[test]
    fn test_access_std() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_access_std.txt");

        // Should fail for non-existent file
        assert!(access_std(&file_path).is_err());

        // Create file and test again
        std::fs::write(&file_path, b"test").unwrap();
        assert!(access_std(&file_path).is_ok());
    }

    #[tokio::test]
    async fn test_lstat() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_lstat.txt");

        // Create test file
        tokio::fs::write(&file_path, b"test content").await.unwrap();

        // Test lstat
        let metadata = lstat(&file_path).await.unwrap();
        assert!(metadata.is_file());
        assert_eq!(metadata.len(), 12); // "test content" is 12 bytes
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_lstat_preserves_symlink_metadata() {
        use std::os::unix::fs::symlink;

        let temp_dir = TempDir::new().unwrap();
        let target_path = temp_dir.path().join("target.txt");
        let link_path = temp_dir.path().join("link.txt");

        tokio::fs::write(&target_path, b"test content").await.unwrap();
        symlink(&target_path, &link_path).unwrap();

        let metadata = lstat(&link_path).await.unwrap();
        assert!(metadata.file_type().is_symlink());
    }

    #[test]
    fn test_lstat_std() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_lstat_std.txt");

        // Create test file
        std::fs::write(&file_path, b"test content").unwrap();

        // Test lstat_std
        let metadata = lstat_std(&file_path).unwrap();
        assert!(metadata.is_file());
        assert_eq!(metadata.len(), 12); // "test content" is 12 bytes
    }

    #[cfg(unix)]
    #[test]
    fn test_lstat_std_preserves_symlink_metadata() {
        use std::os::unix::fs::symlink;

        let temp_dir = TempDir::new().unwrap();
        let target_path = temp_dir.path().join("target-std.txt");
        let link_path = temp_dir.path().join("link-std.txt");

        std::fs::write(&target_path, b"test content").unwrap();
        symlink(&target_path, &link_path).unwrap();

        let metadata = lstat_std(&link_path).unwrap();
        assert!(metadata.file_type().is_symlink());
    }

    #[tokio::test]
    async fn test_make_dir_all() {
        let temp_dir = TempDir::new().unwrap();
        let nested_path = temp_dir.path().join("level1").join("level2").join("level3");

        // Should create nested directories
        assert!(make_dir_all(&nested_path).await.is_ok());
        assert!(nested_path.exists());
        assert!(nested_path.is_dir());
    }

    #[tokio::test]
    async fn test_remove_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_remove.txt");

        // Create test file
        tokio::fs::write(&file_path, b"test").await.unwrap();
        assert!(file_path.exists());

        // Remove file
        assert!(remove(&file_path).await.is_ok());
        assert!(!file_path.exists());
    }

    #[tokio::test]
    async fn test_remove_directory() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().join("test_remove_dir");

        // Create test directory
        tokio::fs::create_dir(&dir_path).await.unwrap();
        assert!(dir_path.exists());

        // Remove directory
        assert!(remove(&dir_path).await.is_ok());
        assert!(!dir_path.exists());
    }

    #[tokio::test]
    async fn test_remove_all() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().join("test_remove_all");
        let file_path = dir_path.join("nested_file.txt");

        // Create nested structure
        tokio::fs::create_dir(&dir_path).await.unwrap();
        tokio::fs::write(&file_path, b"nested content").await.unwrap();

        // Remove all
        assert!(remove_all(&dir_path).await.is_ok());
        assert!(!dir_path.exists());
    }

    #[test]
    fn test_remove_std() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_remove_std.txt");

        // Create test file
        std::fs::write(&file_path, b"test").unwrap();
        assert!(file_path.exists());

        // Remove file
        assert!(remove_std(&file_path).is_ok());
        assert!(!file_path.exists());
    }

    #[test]
    fn test_remove_all_std() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().join("test_remove_all_std");
        let file_path = dir_path.join("nested_file.txt");

        // Create nested structure
        std::fs::create_dir(&dir_path).unwrap();
        std::fs::write(&file_path, b"nested content").unwrap();

        // Remove all
        assert!(remove_all_std(&dir_path).is_ok());
        assert!(!dir_path.exists());
    }

    #[tokio::test]
    async fn test_mkdir() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().join("test_mkdir");

        // Create directory
        assert!(mkdir(&dir_path).await.is_ok());
        assert!(dir_path.exists());
        assert!(dir_path.is_dir());
    }

    #[tokio::test]
    async fn test_rename() {
        let temp_dir = TempDir::new().unwrap();
        let old_path = temp_dir.path().join("old_name.txt");
        let new_path = temp_dir.path().join("new_name.txt");

        // Create test file
        tokio::fs::write(&old_path, b"test content").await.unwrap();
        assert!(old_path.exists());
        assert!(!new_path.exists());

        // Rename file
        assert!(rename(&old_path, &new_path).await.is_ok());
        assert!(!old_path.exists());
        assert!(new_path.exists());

        // Verify content preserved
        let content = tokio::fs::read_to_string(&new_path).await.unwrap();
        assert_eq!(content, "test content");
    }

    #[test]
    fn test_rename_std() {
        let temp_dir = TempDir::new().unwrap();
        let old_path = temp_dir.path().join("old_name_std.txt");
        let new_path = temp_dir.path().join("new_name_std.txt");

        // Create test file
        std::fs::write(&old_path, b"test content").unwrap();
        assert!(old_path.exists());
        assert!(!new_path.exists());

        // Rename file
        assert!(rename_std(&old_path, &new_path).is_ok());
        assert!(!old_path.exists());
        assert!(new_path.exists());

        // Verify content preserved
        let content = std::fs::read_to_string(&new_path).unwrap();
        assert_eq!(content, "test content");
    }

    #[tokio::test]
    async fn test_same_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_same.txt");

        // Create test file
        tokio::fs::write(&file_path, b"test content").await.unwrap();

        // Get metadata twice
        let metadata1 = tokio::fs::metadata(&file_path).await.unwrap();
        let metadata2 = tokio::fs::metadata(&file_path).await.unwrap();

        // Should be the same file
        assert!(same_file(&metadata1, &metadata2));
    }

    #[tokio::test]
    async fn test_different_files() {
        let temp_dir = TempDir::new().unwrap();
        let file1_path = temp_dir.path().join("file1.txt");
        let file2_path = temp_dir.path().join("file2.txt");

        // Create two different files
        tokio::fs::write(&file1_path, b"content1").await.unwrap();
        tokio::fs::write(&file2_path, b"different content").await.unwrap();

        // Get metadata
        let metadata1 = tokio::fs::metadata(&file1_path).await.unwrap();
        let metadata2 = tokio::fs::metadata(&file2_path).await.unwrap();

        // Should be different files
        assert!(!same_file(&metadata1, &metadata2));
    }
}
