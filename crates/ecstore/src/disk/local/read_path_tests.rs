// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;
use std::cell::RefCell;

struct Observation {
    rebuilds: usize,
    normalizations: usize,
    before_recheck: Option<Box<dyn FnOnce()>>,
}

thread_local! {
    static OBSERVATION: RefCell<Option<Observation>> = const { RefCell::new(None) };
}

struct Observe;

impl Observe {
    fn new(before_recheck: Option<Box<dyn FnOnce()>>) -> Self {
        OBSERVATION.with(|state| {
            assert!(state.borrow().is_none(), "read-path observations must not nest");
            *state.borrow_mut() = Some(Observation {
                rebuilds: 0,
                normalizations: 0,
                before_recheck,
            });
        });
        Self
    }

    fn normalizations(&self) -> usize {
        OBSERVATION.with(|state| state.borrow().as_ref().expect("active observation").normalizations)
    }

    fn rebuilds(&self) -> usize {
        OBSERVATION.with(|state| state.borrow().as_ref().expect("active observation").rebuilds)
    }
}

impl Drop for Observe {
    fn drop(&mut self) {
        OBSERVATION.with(|state| *state.borrow_mut() = None);
    }
}

pub(super) fn record_bucket_rebuild() {
    OBSERVATION.with(|state| {
        if let Some(observation) = state.borrow_mut().as_mut() {
            observation.rebuilds += 1;
        }
    });
}

pub(super) fn record_normalization() {
    OBSERVATION.with(|state| {
        if let Some(observation) = state.borrow_mut().as_mut() {
            observation.normalizations += 1;
        }
    });
}

pub(super) fn before_bucket_recheck() {
    let hook = OBSERVATION.with(|state| state.borrow_mut().as_mut().and_then(|value| value.before_recheck.take()));
    if let Some(hook) = hook {
        hook();
    }
}

async fn local_disk(root: &Path) -> LocalDisk {
    let endpoint = Endpoint::try_from(root.to_string_lossy().as_ref()).expect("local endpoint");
    let disk = LocalDisk::new(&endpoint, false).await.expect("local disk");
    disk.make_volume("bucket").await.expect("bucket");
    disk
}

#[tokio::test(flavor = "current_thread")]
async fn checked_read_paths_reuse_bucket_construction() {
    let root = tempfile::tempdir().expect("disk root");
    let disk = local_disk(root.path()).await;
    std::fs::create_dir_all(root.path().join("bucket/prefix/object")).expect("object directories");
    for key in ["prefix/object/xl.meta", "", "prefix/../object/xl.meta"] {
        let expected = build_local_disk_object_path(disk.io_root(), "bucket", key);
        let observation = Observe::new(None);
        let actual = disk.io_get_checked_read_paths("bucket", key).expect("checked read paths");
        assert_eq!(actual, expected, "returned paths must retain their existing spelling");
        assert_eq!(observation.rebuilds(), 0, "the validated tuple already owns its bucket path");
        // path_join_buf already resolves dot components in object keys.
        let expected_normalizations = if cfg!(windows) { 3 } else { 0 };
        assert_eq!(observation.normalizations(), expected_normalizations);
        #[cfg(not(target_os = "linux"))]
        {
            assert_eq!(
                disk.io_get_bucket_and_object_open_path("bucket", key)
                    .expect("read-version paths"),
                expected
            );
            assert_eq!(observation.rebuilds(), 0, "read_version shares the same checked path core");
            assert_eq!(observation.normalizations(), 2 * expected_normalizations);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn checked_read_paths_preserve_containment_errors() {
    let root = tempfile::tempdir().expect("disk root");
    let disk = local_disk(root.path()).await;
    for (bucket, key) in [("../outside", "object"), ("bucket", "../outside"), ("bucket", "/outside")] {
        let old = disk
            .io_get_object_path(bucket, key)
            .and_then(|path| disk.io_get_bucket_path(bucket).map(|volume| (volume, path)));
        let new = disk.io_get_checked_read_paths(bucket, key);
        assert_eq!(new, old, "path and error behavior for {bucket}/{key}");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn checked_read_paths_metadata_bytes_and_absence_errors() {
    let root = tempfile::tempdir().expect("disk root");
    let disk = local_disk(root.path()).await;
    let object = root.path().join("bucket/prefix/object");
    std::fs::create_dir_all(&object).expect("object");
    let metadata = b"opaque metadata bytes passed through the read API";
    std::fs::write(object.join(STORAGE_FORMAT_FILE), metadata).expect("metadata");
    {
        let observation = Observe::new(None);
        let actual = disk
            .read_metadata("bucket", "prefix/object/xl.meta")
            .await
            .expect("read metadata");
        assert_eq!(actual.as_ref(), metadata);
        assert_eq!(observation.rebuilds(), 0, "scanner metadata reads use the shared path core");
        assert_eq!(observation.normalizations(), if cfg!(windows) { 3 } else { 0 });
    }
    assert!(matches!(
        disk.read_metadata("bucket", "missing/xl.meta").await,
        Err(DiskError::FileNotFound)
    ));
    assert!(matches!(
        disk.read_metadata("missing", "object/xl.meta").await,
        Err(DiskError::VolumeNotFound)
    ));
}

#[cfg(unix)]
#[tokio::test(flavor = "current_thread")]
async fn checked_read_paths_recheck_bucket_after_object_validation() {
    let root = tempfile::tempdir().expect("disk root");
    let outside = tempfile::tempdir().expect("outside root");
    let disk = local_disk(root.path()).await;
    std::fs::create_dir_all(root.path().join("bucket/object")).expect("object");
    std::fs::write(root.path().join("bucket/object/xl.meta"), b"inside").expect("inside metadata");
    let bucket = root.path().join("bucket");
    let saved = root.path().join("saved");
    let target = outside.path().to_path_buf();
    let _observation = Observe::new(Some(Box::new(move || {
        std::fs::rename(&bucket, saved).expect("replace bucket after full-path validation");
        std::os::unix::fs::symlink(target, bucket).expect("bucket replacement symlink");
    })));
    assert!(
        matches!(disk.io_get_checked_read_paths("bucket", "object/xl.meta"), Err(DiskError::InvalidPath)),
        "the second bucket check must remain effective before queuing native IO"
    );
}

#[test]
fn read_path_normalization_matches_component_normalizer() {
    for value in [
        "",
        ".",
        "..",
        "./a",
        "a/./b",
        "a//b/",
        "a/../b",
        "../../a",
        "/",
        "//a///b/",
        "/a/../b",
        "/../../a",
        r"C:\a\b",
        r"C:\a\..\b",
        r"\\server\share\a",
        r"\\?\C:\a\.\b",
    ] {
        let path = Path::new(value);
        let expected = normalize_path_components(path);
        let observation = Observe::new(None);
        let actual = normalize_path_cow(path);
        assert_eq!(
            actual.components().collect::<Vec<_>>(),
            expected.components().collect::<Vec<_>>(),
            "{value}"
        );
        let needs_buffer = cfg!(windows)
            || path
                .components()
                .any(|component| matches!(component, std::path::Component::ParentDir | std::path::Component::CurDir));
        assert_eq!(matches!(actual, std::borrow::Cow::Owned(_)), needs_buffer, "{value}");
        assert_eq!(observation.normalizations(), usize::from(needs_buffer), "{value}");
    }
}
