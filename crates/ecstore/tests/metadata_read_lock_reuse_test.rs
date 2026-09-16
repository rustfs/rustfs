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

use std::sync::Arc;
use std::time::Duration;
use storage_api::metadata_lock::{
    BucketOperations, CompletePart, Error, MakeBucketOptions, MultipartOperations, NamespaceLocking, ObjectIO, ObjectOperations,
    ObjectOptions, PutObjReader, PutObjectCommitBarrier, PutObjectCommitPause, init_bucket_metadata_sys,
    isolated_store_over_temp_disks,
};
use tokio::io::AsyncReadExt;
use tokio::time::timeout;
use uuid::Uuid;

#[tokio::test]
async fn replica_put_completes_while_metadata_writer_waits_for_its_snapshot() {
    replica_write_with_waiting_metadata_writer(false).await;
}

#[tokio::test]
async fn replica_multipart_completes_while_metadata_writer_waits_for_its_snapshot() {
    replica_write_with_waiting_metadata_writer(true).await;
}

async fn replica_write_with_waiting_metadata_writer(multipart: bool) {
    let (_dirs, store) = isolated_store_over_temp_disks().await;
    init_bucket_metadata_sys(Arc::clone(&store), Vec::new()).await;
    let bucket = "replica-metadata-read-reuse";
    store
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("create bucket");
    store
        .update_bucket_metadata_config(
            bucket,
            "lifecycle.xml",
            br#"<LifecycleConfiguration><Rule><ID>expire-old-versions</ID><Status>Enabled</Status><Filter><Prefix></Prefix></Filter><NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays></NoncurrentVersionExpiration></Rule></LifecycleConfiguration>"#.to_vec(),
        )
        .await
        .expect("configure post-write lifecycle evaluation");
    let opts = ObjectOptions {
        versioned: true,
        version_id: Some(Uuid::new_v4().to_string()),
        replication_request: true,
        expected_bucket_incarnation_id: Some(store.bucket_incarnation_id(bucket).await.expect("load incarnation")),
        object_lock_config_snapshot: Some(
            store
                .object_lock_config_snapshot(bucket)
                .await
                .expect("capture Object Lock snapshot"),
        ),
        ..Default::default()
    };
    let upload = if multipart {
        let upload = store
            .new_multipart_upload(bucket, "object", &opts)
            .await
            .expect("stage upload");
        let mut data = PutObjReader::from_vec(b"replicated version".to_vec());
        let part = store
            .put_object_part(bucket, "object", &upload.upload_id, 1, &mut data, &opts)
            .await
            .expect("stage part");
        Some((
            upload.upload_id,
            vec![CompletePart {
                part_num: 1,
                etag: part.etag,
                ..Default::default()
            }],
        ))
    } else {
        None
    };
    let lock = store
        .new_ns_lock(".rustfs.sys", &format!("bucket-targets/{bucket}/transaction.lock"))
        .await
        .expect("create metadata writer");
    let writer = lock.get_write_lock(Duration::from_secs(5));
    tokio::pin!(writer);
    // Leave the writer registered while the replica continues. A second read
    // acquisition would queue behind this writer, which needs our first read.
    assert!(futures::poll!(&mut writer).is_pending());
    timeout(Duration::from_secs(2), async {
        if let Some((upload_id, parts)) = upload {
            store
                .clone()
                .complete_multipart_upload(bucket, "object", &upload_id, parts, &opts)
                .await
        } else {
            let mut data = PutObjReader::from_vec(b"replicated version".to_vec());
            store.put_object(bucket, "object", &mut data, &opts).await
        }
    })
    .await
    .expect("replica must reuse its metadata read lock instead of waiting behind the writer")
    .expect("replica PUT should commit");
    assert!(futures::poll!(&mut writer).is_pending(), "the snapshot must still fence metadata updates");
    let read_opts = ObjectOptions {
        version_id: opts.version_id.clone(),
        ..Default::default()
    };
    drop(opts);
    timeout(Duration::from_secs(2), writer)
        .await
        .expect("metadata writer must proceed after the snapshot drops")
        .expect("acquire metadata writer");
    let mut reader = store
        .get_object_reader(bucket, "object", None, Default::default(), &read_opts)
        .await
        .expect("read committed replica version");
    let mut bytes = Vec::new();
    reader.stream.read_to_end(&mut bytes).await.expect("read replica body");
    assert_eq!(bytes, b"replicated version");
}

#[tokio::test]
async fn reused_metadata_lock_still_enforces_quota() {
    let (_dirs, store) = isolated_store_over_temp_disks().await;
    init_bucket_metadata_sys(Arc::clone(&store), Vec::new()).await;
    let bucket = "metadata-read-reuse-quota";
    store
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("create bucket");
    store
        .update_bucket_metadata_config(bucket, "quota.json", br#"{"quota":1}"#.to_vec())
        .await
        .expect("set quota");
    let mut opts = ObjectOptions {
        versioned: true,
        version_id: Some(Uuid::new_v4().to_string()),
        expected_bucket_incarnation_id: Some(store.bucket_incarnation_id(bucket).await.expect("load incarnation")),
        object_lock_config_snapshot: Some(store.object_lock_config_snapshot(bucket).await.expect("capture snapshot")),
        ..Default::default()
    };
    assert!(opts.set_quota_admission(0, 1));
    let lock = store
        .new_ns_lock(".rustfs.sys", &format!("bucket-targets/{bucket}/transaction.lock"))
        .await
        .expect("create writer");
    let writer = lock.get_write_lock(Duration::from_secs(5));
    tokio::pin!(writer);
    assert!(futures::poll!(&mut writer).is_pending());
    let mut data = PutObjReader::from_vec(b"too large".to_vec());
    let error = timeout(Duration::from_secs(2), store.put_object(bucket, "object", &mut data, &opts))
        .await
        .expect("quota admission must not reacquire the lock")
        .expect_err("quota must reject growth");
    assert!(matches!(error, Error::QuotaExceeded { limit: 1, .. }), "{error:?}");
    drop(opts);
    timeout(Duration::from_secs(2), writer)
        .await
        .expect("release metadata reader after denial")
        .expect("acquire writer");
}

#[tokio::test]
async fn cancelled_put_releases_shared_metadata_guard() {
    let (_dirs, store) = isolated_store_over_temp_disks().await;
    init_bucket_metadata_sys(Arc::clone(&store), Vec::new()).await;
    let bucket = "metadata-read-reuse-cancel";
    store
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("create bucket");
    let opts = ObjectOptions {
        versioned: true,
        version_id: Some(Uuid::new_v4().to_string()),
        object_lock_config_snapshot: Some(store.object_lock_config_snapshot(bucket).await.expect("capture snapshot")),
        ..Default::default()
    };
    let barrier = PutObjectCommitBarrier::install(bucket, "object", PutObjectCommitPause::AfterQuotaReservation);
    let put_store = Arc::clone(&store);
    let put = tokio::spawn(async move {
        let mut data = PutObjReader::from_vec(b"cancel before commit".to_vec());
        put_store.put_object(bucket, "object", &mut data, &opts).await
    });
    barrier.wait_until_paused().await;
    let lock = store
        .new_ns_lock(".rustfs.sys", &format!("bucket-targets/{bucket}/transaction.lock"))
        .await
        .expect("create writer");
    let writer = lock.get_write_lock(Duration::from_secs(5));
    tokio::pin!(writer);
    assert!(futures::poll!(&mut writer).is_pending());
    put.abort();
    assert!(put.await.expect_err("PUT should be cancelled").is_cancelled());
    timeout(Duration::from_secs(2), writer)
        .await
        .expect("all metadata reader owners must drop after cancellation")
        .expect("acquire writer");
}

#[tokio::test]
async fn multipart_rejects_metadata_snapshots_from_another_scope() {
    let (_dirs, store) = isolated_store_over_temp_disks().await;
    init_bucket_metadata_sys(Arc::clone(&store), Vec::new()).await;
    let (_other_dirs, other_store) = isolated_store_over_temp_disks().await;
    init_bucket_metadata_sys(Arc::clone(&other_store), Vec::new()).await;
    let bucket = "metadata-read-reuse-scope";
    for target in [&store, &other_store] {
        target
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("create bucket");
    }
    store
        .make_bucket("other-bucket", &MakeBucketOptions::default())
        .await
        .expect("create other bucket");
    let other_store_incarnation = other_store
        .bucket_incarnation_id(bucket)
        .await
        .expect("load other store incarnation");
    let other_bucket_incarnation = store
        .bucket_incarnation_id("other-bucket")
        .await
        .expect("load other bucket incarnation");
    for (snapshot_store, snapshot_bucket, expected) in [
        (&other_store, bucket, other_store_incarnation),
        (&store, "other-bucket", other_bucket_incarnation),
        (&store, bucket, Uuid::new_v4()),
    ] {
        let opts = ObjectOptions {
            expected_bucket_incarnation_id: Some(expected),
            object_lock_config_snapshot: Some(
                snapshot_store
                    .object_lock_config_snapshot(snapshot_bucket)
                    .await
                    .expect("capture snapshot"),
            ),
            ..Default::default()
        };
        let error = store
            .new_multipart_upload(bucket, "object", &opts)
            .await
            .expect_err("foreign snapshot must be rejected");
        assert!(error.to_string().contains("valid metadata transaction fence"), "{error:?}");
    }
}

#[tokio::test]
async fn delete_prefix_reuses_its_metadata_snapshot_for_generation_validation() {
    let (_dirs, store) = isolated_store_over_temp_disks().await;
    init_bucket_metadata_sys(Arc::clone(&store), Vec::new()).await;
    let bucket = "metadata-read-reuse-delete";
    store
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("create bucket");
    let mut data = PutObjReader::from_vec(b"remove prefix".to_vec());
    store
        .put_object(bucket, "prefix/object", &mut data, &ObjectOptions::default())
        .await
        .expect("seed object");
    store
        .delete_object(
            bucket,
            "prefix/",
            ObjectOptions {
                delete_prefix: true,
                ..Default::default()
            },
        )
        .await
        .expect("delete prefix with an internally captured metadata snapshot");
    let error = store
        .get_object_info(bucket, "prefix/object", &ObjectOptions::default())
        .await
        .expect_err("prefix object must be deleted");
    assert!(matches!(error, Error::ObjectNotFound(..)), "{error:?}");
}
