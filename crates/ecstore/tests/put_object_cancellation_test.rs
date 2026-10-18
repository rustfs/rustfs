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

#![cfg(feature = "test-util")]

mod storage_api;

use std::{path::PathBuf, time::Duration};
use storage_api::metadata_lock::{
    ObjectIO, ObjectOptions, PutObjReader, PutObjectCommitBarrier, PutObjectCommitPause, isolated_store_over_temp_disks,
};
use tempfile::TempDir;
use tokio::{
    fs,
    io::AsyncReadExt,
    time::{Instant, timeout},
};

async fn staged_workspaces(dirs: &[TempDir]) -> Vec<PathBuf> {
    let mut staged = Vec::new();
    for dir in dirs {
        let mut entries = fs::read_dir(dir.path().join(".rustfs.sys/tmp"))
            .await
            .expect("read temporary namespace");
        while let Some(entry) = entries.next_entry().await.expect("read temporary entry") {
            if entry.file_name() != ".trash" {
                staged.push(entry.path());
            }
        }
    }
    staged
}

async fn wait_for_staging_cleanup(dirs: &[TempDir]) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let staged = staged_workspaces(dirs).await;
        if staged.is_empty() {
            return;
        }
        assert!(Instant::now() < deadline, "cancelled PUT left staging: {staged:?}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn checkpoint_timeout_at(pause: PutObjectCommitPause) {
    let (dirs, store) = isolated_store_over_temp_disks().await;
    let bucket = ".rustfs.sys";
    let object = "buckets/scanner-timeout/.usage-cache.bin.bkp";
    let old_body = vec![0x31; 1 << 20];
    let opts = ObjectOptions {
        max_parity: true,
        ..Default::default()
    };
    store
        .put_object(bucket, object, &mut PutObjReader::from_vec(old_body.clone()), &opts)
        .await
        .expect("seed previous checkpoint");
    wait_for_staging_cleanup(&dirs).await;

    let barrier = PutObjectCommitBarrier::install(bucket, object, pause);
    let mut reader = PutObjReader::from_vec(vec![0x32; 1 << 20]);
    let mut put = Box::pin(store.put_object(bucket, object, &mut reader, &opts));
    tokio::select! {
        result = &mut put => panic!("replacement completed before the barrier: {result:?}"),
        paused = timeout(Duration::from_secs(10), barrier.wait_until_paused()) => {
            paused.expect("replacement must reach its commit barrier");
        }
    }
    let staged = staged_workspaces(&dirs).await;
    assert_eq!(staged.len(), 4, "replacement must stage on every disk before cancellation");
    for workspace in staged {
        let mut entries = fs::read_dir(workspace).await.expect("read staged workspace");
        let data_dir = entries
            .next_entry()
            .await
            .expect("read staged directory")
            .expect("a real shard");
        assert!(
            fs::metadata(data_dir.path().join("part.1"))
                .await
                .expect("real staged payload")
                .len()
                > 0
        );
    }
    // Hold the deterministic barrier so the deadline, not disk speed, drops
    // the save future. No detached owner may finish and mask missing cleanup.
    timeout(Duration::from_secs(1), put)
        .await
        .expect_err("held replacement must time out");
    // delete_all hands staging to .trash for ordinary background GC; it does
    // not promise immediate space reclamation when the request is cancelled.
    wait_for_staging_cleanup(&dirs).await;
    drop(barrier);
    let mut reader = store
        .get_object_reader(bucket, object, None, Default::default(), &ObjectOptions::default())
        .await
        .expect("previous checkpoint remains readable");
    let mut actual = Vec::new();
    reader
        .stream
        .read_to_end(&mut actual)
        .await
        .expect("read previous checkpoint");
    assert_eq!(actual, old_body, "timeout must neither publish replacement nor delete old data");
}

#[tokio::test]
async fn checkpoint_timeout_before_namespace_cleans_staging() {
    checkpoint_timeout_at(PutObjectCommitPause::BeforeNamespace).await;
}

#[tokio::test]
async fn checkpoint_timeout_before_commit_owner_cleans_staging() {
    checkpoint_timeout_at(PutObjectCommitPause::AfterNamespace).await;
}

#[tokio::test]
async fn checkpoint_timeout_after_commit_owner_cleans_staging() {
    checkpoint_timeout_at(PutObjectCommitPause::AfterQuotaReservation).await;
}
