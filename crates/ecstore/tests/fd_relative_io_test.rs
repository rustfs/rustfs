// Copyright 2026 RustFS Team
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

#![cfg(all(target_os = "linux", feature = "test-util"))]

mod storage_api;

use bytes::Bytes;
use rustfs_filemeta::{FileInfo, ObjectPartInfo};
use std::{path::Path, time::Duration};
use storage_api::fd_relative::{
    DiskAPI, DiskError, DiskOption, DiskStore, Endpoint, LocalPublicationPause, LocalPublicationStage, ReadOptions, new_disk,
};
use time::OffsetDateTime;
use uuid::Uuid;

const RUSTFS_META_TMP_BUCKET: &str = ".rustfs.sys/tmp";

async fn disk_at(path: &Path) -> DiskStore {
    new_disk(
        &Endpoint::try_from(path.to_str().expect("UTF-8 test path")).expect("endpoint"),
        &DiskOption::default(),
    )
    .await
    .expect("local disk")
}

fn metadata(data: Bytes, inline: bool, directory: Uuid) -> FileInfo {
    let mut fi = FileInfo::new("object", 1, 0);
    fi.erasure.index = 1;
    fi.version_id = Some(Uuid::nil());
    fi.data_dir = Some(directory);
    fi.size = i64::try_from(data.len()).expect("test data size");
    fi.parts = vec![ObjectPartInfo {
        number: 1,
        size: data.len(),
        actual_size: fi.size,
        ..Default::default()
    }];
    fi.mod_time = Some(OffsetDateTime::now_utc());
    fi.data = inline.then_some(data);
    if inline {
        fi.set_inline_data();
    }
    fi
}

async fn put(disk: &DiskStore, staging: &str, data: Bytes, inline: bool) -> Uuid {
    let directory = Uuid::new_v4();
    if !inline {
        disk.write_all(RUSTFS_META_TMP_BUCKET, &format!("{staging}/{directory}/part.1"), data.clone())
            .await
            .expect("stage shard");
    }
    disk.rename_data(RUSTFS_META_TMP_BUCKET, staging, metadata(data, inline, directory), "bucket", "object")
        .await
        .expect("commit object");
    directory
}

#[tokio::test]
async fn pinned_root_reads_and_writes_original_disk_after_path_replacement() {
    let temp = tempfile::tempdir().expect("test directory");
    let root = temp.path().join("disk");
    std::fs::create_dir(&root).expect("disk root");
    let disk = disk_at(&root).await;
    disk.make_volume("bucket").await.expect("bucket");
    disk.write_all("bucket", "metadata", Bytes::from_static(b"original"))
        .await
        .expect("initial write");
    let moved = temp.path().join("original");
    std::fs::rename(&root, &moved).expect("move mount path");
    std::fs::create_dir_all(root.join("bucket")).expect("replacement disk");
    std::fs::write(root.join("bucket/metadata"), b"replacement").expect("replacement sentinel");
    assert_eq!(
        disk.read_all("bucket", "metadata").await.expect("pinned read"),
        Bytes::from_static(b"original")
    );
    disk.write_all("bucket", "metadata", Bytes::from_static(b"updated"))
        .await
        .expect("pinned write");
    assert_eq!(std::fs::read(moved.join("bucket/metadata")).expect("original disk"), b"updated");
    assert_eq!(std::fs::read(root.join("bucket/metadata")).expect("replacement disk"), b"replacement");
}

#[tokio::test]
async fn relative_reads_and_writes_reject_symlink_escape() {
    let temp = tempfile::tempdir().expect("test directory");
    let disk = disk_at(temp.path()).await;
    disk.make_volume("bucket").await.expect("bucket");
    let outside = tempfile::tempdir().expect("outside directory");
    std::fs::write(outside.path().join("sentinel"), b"preserved").expect("sentinel");
    std::os::unix::fs::symlink(outside.path(), temp.path().join("bucket/link")).expect("directory link");
    std::os::unix::fs::symlink(outside.path().join("sentinel"), temp.path().join("bucket/file-link")).expect("file link");
    for path in ["link/sentinel", "file-link"] {
        assert!(matches!(disk.read_all("bucket", path).await, Err(DiskError::InvalidPath)));
        assert!(matches!(
            disk.write_all("bucket", path, Bytes::from_static(b"unsafe")).await,
            Err(DiskError::InvalidPath)
        ));
    }
    assert_eq!(std::fs::read(outside.path().join("sentinel")).expect("outside bytes"), b"preserved");
}

#[tokio::test]
async fn inline_and_non_inline_overwrites_remain_readable_after_reopen() {
    for inline in [true, false] {
        let temp = tempfile::tempdir().expect("test directory");
        let disk = disk_at(temp.path()).await;
        disk.make_volume("bucket").await.expect("bucket");
        put(&disk, "first", Bytes::from_static(b"old bytes"), inline).await;
        let directory = put(&disk, "second", Bytes::from_static(b"new bytes"), inline).await;
        drop(disk);
        let disk = disk_at(temp.path()).await;
        let fi = disk
            .read_version(
                "bucket",
                "bucket",
                "object",
                "",
                &ReadOptions {
                    read_data: true,
                    ..Default::default()
                },
            )
            .await
            .expect("committed metadata after reopen");
        assert_eq!(fi.data_dir, Some(directory));
        if inline {
            assert_eq!(fi.data, Some(Bytes::from_static(b"new bytes")));
        } else {
            assert_eq!(
                disk.read_all("bucket", &format!("object/{directory}/part.1"))
                    .await
                    .expect("shard bytes"),
                Bytes::from_static(b"new bytes")
            );
        }
    }
}

#[tokio::test]
async fn rename_rejects_symlinked_destination_metadata() {
    for inline in [true, false] {
        let temp = tempfile::tempdir().expect("disk directory");
        let outside = tempfile::tempdir().expect("outside directory");
        let disk = disk_at(temp.path()).await;
        disk.make_volume("bucket").await.expect("bucket");
        put(&disk, "seed", Bytes::from_static(b"old object"), inline).await;
        let destination = temp.path().join("bucket/object/xl.meta");
        let original = std::fs::read(&destination).expect("valid existing metadata");
        let external_metadata = outside.path().join("xl.meta");
        std::fs::write(&external_metadata, &original).expect("external valid metadata");
        std::fs::remove_file(&destination).expect("replace destination metadata");
        std::os::unix::fs::symlink(&external_metadata, &destination).expect("metadata escape link");

        let directory = Uuid::new_v4();
        let data = Bytes::from_static(b"replacement");
        if !inline {
            disk.write_all(RUSTFS_META_TMP_BUCKET, &format!("replacement/{directory}/part.1"), data.clone())
                .await
                .expect("stage non-inline data");
        }
        let result = disk
            .rename_data(
                RUSTFS_META_TMP_BUCKET,
                "replacement",
                metadata(data, inline, directory),
                "bucket",
                "object",
            )
            .await;
        assert!(
            matches!(result, Err(DiskError::InvalidPath)),
            "symlinked metadata must be rejected: {result:?}"
        );
        assert_eq!(std::fs::read_link(&destination).expect("link must remain"), external_metadata);
        assert_eq!(std::fs::read(&external_metadata).expect("external metadata must survive"), original);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_publication_rejects_source_or_directory_replacement() {
    for replace_directory in [false, true] {
        let temp = tempfile::tempdir().expect("test directory");
        let disk = disk_at(temp.path()).await;
        disk.make_volume("bucket").await.expect("bucket");
        put(&disk, "first", Bytes::from_static(b"old bytes"), true).await;
        let old = std::fs::read(temp.path().join("bucket/object/xl.meta")).expect("old metadata");
        let mut pause = LocalPublicationPause::install(&disk, "bucket", "object/xl.meta", LocalPublicationStage::PreparedRename)
            .expect("publication pause");
        let writer = disk.clone();
        let task = tokio::spawn(async move {
            writer
                .rename_data(
                    RUSTFS_META_TMP_BUCKET,
                    "second",
                    metadata(Bytes::from_static(b"new bytes"), true, Uuid::new_v4()),
                    "bucket",
                    "object",
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(30), pause.entered())
            .await
            .expect("publication reached pause")
            .expect("pause entered");
        if replace_directory {
            std::fs::rename(temp.path().join("bucket/object"), temp.path().join("detached")).expect("detach object");
            std::fs::create_dir(temp.path().join("bucket/object")).expect("replacement directory");
            std::fs::write(temp.path().join("bucket/object/xl.meta"), &old).expect("replacement metadata");
        } else {
            let src = temp.path().join(RUSTFS_META_TMP_BUCKET).join("second/xl.meta");
            std::fs::rename(&src, src.with_extension("detached")).expect("detach staged metadata");
            std::fs::write(src, b"unrelated").expect("replacement staged file");
        }
        drop(pause);
        assert!(task.await.expect("writer task").is_err());
        assert_eq!(
            std::fs::read(temp.path().join("bucket/object/xl.meta")).expect("old metadata survives"),
            old
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_waiter_keeps_publication_serialized_until_physical_completion() {
    let temp = tempfile::tempdir().expect("test directory");
    let disk = disk_at(temp.path()).await;
    disk.make_volume("bucket").await.expect("bucket");
    put(&disk, "first", Bytes::from_static(b"old bytes"), true).await;
    let mut pause = LocalPublicationPause::install(&disk, "bucket", "object/xl.meta", LocalPublicationStage::PreparedRename)
        .expect("publication pause");
    let writer = disk.clone();
    let pending = tokio::spawn(async move { put(&writer, "shared-stage", Bytes::from_static(b"cancelled waiter"), true).await });
    tokio::time::timeout(Duration::from_secs(30), pause.entered())
        .await
        .expect("publication reached pause")
        .expect("pause entered");
    pending.abort();
    assert!(pending.await.expect_err("waiter aborted").is_cancelled());
    let writer = disk.clone();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let mut retry = tokio::spawn(async move {
        let _ = started_tx.send(());
        put(&writer, "shared-stage", Bytes::from_static(b"latest bytes"), true).await
    });
    started_rx.await.expect("retry started");
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut retry).await.is_err(),
        "a retry must not publish while the cancelled writer still owns physical IO"
    );
    drop(pause);
    tokio::time::timeout(Duration::from_secs(30), retry)
        .await
        .expect("retry completed")
        .expect("retry task");
    let fi = disk
        .read_version(
            "bucket",
            "bucket",
            "object",
            "",
            &ReadOptions {
                read_data: true,
                ..Default::default()
            },
        )
        .await
        .expect("latest metadata");
    assert_eq!(fi.data, Some(Bytes::from_static(b"latest bytes")));
}
