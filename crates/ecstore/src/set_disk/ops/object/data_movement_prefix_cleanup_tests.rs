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

//! Data-movement source cleanup of an object whose name is a path prefix of
//! another object (`a/b` and `a/b/c`). Cleanup of the parent must only remove
//! the parent's own protocol data and never touch the child's xl.meta or data.

use super::hermetic_set_disks_support::{HermeticSetFixture, hermetic_set_disks_isolated};
use super::*;
use crate::data_movement::{SourceCleanupBucketFence, SourceCleanupError, cleanup_source_entry_if_unchanged};
use rustfs_filemeta::FileInfoVersions;
use std::sync::atomic::AtomicBool;
use tempfile::TempDir;

const PARENT: &str = "nested/c.bin";
const CHILD: &str = "nested/c.bin/child.bin";

async fn put(set_disks: &Arc<SetDisks>, bucket: &str, object: &str, payload: &[u8]) {
    let mut reader = PutObjReader::from_vec(payload.to_vec());
    set_disks
        .put_object(bucket, object, &mut reader, &ObjectOptions::default())
        .await
        .unwrap_or_else(|err| panic!("{object} should be written: {err:?}"));
}

async fn read(set_disks: &Arc<SetDisks>, bucket: &str, object: &str) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    set_disks
        .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
        .await?
        .stream
        .read_to_end(&mut body)
        .await
        .map_err(Error::other)?;
    Ok(body)
}

async fn source_versions(set_disks: &Arc<SetDisks>, bucket: &str, object: &str) -> FileInfoVersions {
    set_disks
        .load_file_info_versions_exact(bucket, object)
        .await
        .expect("source versions should be readable")
        .expect("source versions should exist")
}

async fn cleanup(
    set_disks: Arc<SetDisks>,
    bucket: &str,
    object: &str,
    expected: FileInfoVersions,
) -> std::result::Result<ObjectInfo, SourceCleanupError> {
    cleanup_source_entry_if_unchanged(
        set_disks,
        bucket,
        object,
        &expected,
        &[],
        &[],
        SourceCleanupBucketFence::default(),
        "test_data_movement_prefix_cleanup",
    )
    .await
}

/// Every file below the child's own directory, per disk.
fn child_tree(temp_dirs: &[TempDir], bucket: &str) -> Vec<Vec<String>> {
    temp_dirs
        .iter()
        .map(|dir| {
            let root = dir.path().join(bucket).join(CHILD);
            let mut files = Vec::new();
            let mut stack = vec![root.clone()];
            while let Some(path) = stack.pop() {
                let Ok(entries) = std::fs::read_dir(&path) else {
                    continue;
                };
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        stack.push(path);
                    } else {
                        files.push(path.strip_prefix(&root).expect("child file").display().to_string());
                    }
                }
            }
            files.sort();
            files
        })
        .collect()
}

async fn setup(payload_len: usize) -> (HermeticSetFixture, Arc<SetDisks>, &'static str, Vec<u8>, Vec<u8>) {
    let (temp_dirs, disk_stores, set_disks) = hermetic_set_disks_isolated(4).await;
    let bucket = "data-movement-prefix-cleanup";
    for disk in &disk_stores {
        disk.make_volume(bucket).await.expect("bucket volume should be created");
    }
    let parent_payload = vec![b'p'; payload_len];
    let child_payload = vec![b'c'; payload_len];
    put(&set_disks, bucket, PARENT, &parent_payload).await;
    put(&set_disks, bucket, CHILD, &child_payload).await;
    (temp_dirs, set_disks, bucket, parent_payload, child_payload)
}

async fn assert_parent_cleanup_preserves_child(payload_len: usize) {
    let (temp_dirs, set_disks, bucket, _parent_payload, child_payload) = setup(payload_len).await;
    // A quorum PUT may finish its last disk's rename after acknowledging.
    let mut child_files_before = child_tree(&temp_dirs, bucket);
    for _ in 0..100 {
        if child_files_before
            .iter()
            .all(|files| files.iter().any(|file| file == "xl.meta"))
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        child_files_before = child_tree(&temp_dirs, bucket);
    }
    assert!(
        child_files_before
            .iter()
            .all(|files| files.iter().any(|file| file == "xl.meta")),
        "setup must write the child's xl.meta on every disk: {child_files_before:?}"
    );
    let child_before = source_versions(&set_disks, bucket, CHILD).await;
    let parent = source_versions(&set_disks, bucket, PARENT).await;

    cleanup(Arc::clone(&set_disks), bucket, PARENT, parent)
        .await
        .expect("parent cleanup should succeed");

    assert!(
        set_disks
            .load_file_info_versions_exact(bucket, PARENT)
            .await
            .expect("parent lookup should succeed")
            .is_none(),
        "parent versions must be removed"
    );
    assert_eq!(
        child_tree(&temp_dirs, bucket),
        child_files_before,
        "parent cleanup must leave every file of the child untouched"
    );
    let child_after = source_versions(&set_disks, bucket, CHILD).await;
    assert_eq!(child_after.versions.len(), child_before.versions.len());
    assert_eq!(read(&set_disks, bucket, CHILD).await.expect("child must stay readable"), child_payload);
}

#[tokio::test]
async fn parent_cleanup_preserves_prefixed_child_inline() {
    assert_parent_cleanup_preserves_child(2048).await;
}

#[tokio::test]
async fn parent_cleanup_preserves_prefixed_child_with_data_dir() {
    assert_parent_cleanup_preserves_child(256 * 1024).await;
}

async fn assert_concurrent_parent_and_child_cleanup(payload_len: usize) {
    for round in 0..16 {
        let (temp_dirs, set_disks, bucket, _parent_payload, _child_payload) = setup(payload_len).await;
        let parent = source_versions(&set_disks, bucket, PARENT).await;
        let child = source_versions(&set_disks, bucket, CHILD).await;

        let parent_task = tokio::spawn(cleanup(Arc::clone(&set_disks), bucket, PARENT, parent));
        let child_task = tokio::spawn(cleanup(Arc::clone(&set_disks), bucket, CHILD, child));
        let parent_result = parent_task.await.expect("parent cleanup should not panic");
        let child_result = child_task.await.expect("child cleanup should not panic");
        parent_result.unwrap_or_else(|err| panic!("round {round}: parent cleanup failed: {err:?}"));
        child_result.unwrap_or_else(|err| panic!("round {round}: child cleanup failed: {err:?}"));

        for object in [PARENT, CHILD] {
            assert!(
                set_disks
                    .load_file_info_versions_exact(bucket, object)
                    .await
                    .expect("lookup should succeed")
                    .is_none(),
                "round {round}: {object} must be removed"
            );
        }
        drop(temp_dirs);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_parent_and_child_cleanup_both_commit_inline() {
    assert_concurrent_parent_and_child_cleanup(2048).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_parent_and_child_cleanup_both_commit_with_data_dir() {
    assert_concurrent_parent_and_child_cleanup(256 * 1024).await;
}

/// The child keeps committing new data directories below the parent's
/// directory while the parent's source cleanup runs; the parent's delete
/// transaction must still reach quorum.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parent_cleanup_commits_while_child_is_rewritten() {
    for round in 0..8 {
        let (_temp_dirs, set_disks, bucket, _parent_payload, child_payload) = setup(256 * 1024).await;
        let parent = source_versions(&set_disks, bucket, PARENT).await;
        let stop = Arc::new(AtomicBool::new(false));
        let writer_set = Arc::clone(&set_disks);
        let writer_stop = Arc::clone(&stop);
        let writer_payload = child_payload.clone();
        let writer = tokio::spawn(async move {
            let mut writes = 0usize;
            while !writer_stop.load(Ordering::Acquire) || writes == 0 {
                put(&writer_set, bucket, CHILD, &writer_payload).await;
                writes += 1;
            }
            writes
        });

        let result = cleanup(Arc::clone(&set_disks), bucket, PARENT, parent).await;
        stop.store(true, Ordering::Release);
        let writes = writer.await.expect("child writer should not panic");
        result.unwrap_or_else(|err| panic!("round {round}: parent cleanup failed after {writes} child writes: {err:?}"));
        assert_eq!(read(&set_disks, bucket, CHILD).await.expect("child must stay readable"), child_payload);
    }
}
