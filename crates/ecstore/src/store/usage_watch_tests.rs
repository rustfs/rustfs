// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::bucket::metadata_sys::{init_bucket_metadata_sys, test_support::isolated_store_over_temp_disks};
use crate::runtime::usage_watch::{ScannerUsageWatchKnown, ScannerUsageWatchRequest};
use crate::storage_api_contracts::object::{ObjectIO as _, ObjectOperations as _};

async fn known(store: &ECStore, request: &ScannerUsageWatchRequest) -> ScannerUsageWatchKnown {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let response = store.scanner_usage_watch(std::slice::from_ref(request)).await;
            assert_eq!(response.len(), 1);
            assert_eq!(response[0].bucket, request.bucket);
            let proof = response[0].known.clone().expect("storage-backed watch proof");
            if proof.inflight == 0 {
                return proof;
            }
            // API quorum completion can precede a remaining physical tail.
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("all counted physical tails settle")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn usage_watch_api_tracks_real_put_delete_and_bucket_recreation() {
    let (_dirs, store) = isolated_store_over_temp_disks().await;
    init_bucket_metadata_sys(store.clone(), Vec::new()).await;
    let bucket = "watch-real-store";
    store
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("create watched bucket");
    let incarnation = store
        .bucket_incarnation_id_from_disk(bucket)
        .await
        .expect("bucket incarnation");
    let mut request = ScannerUsageWatchRequest {
        bucket: bucket.into(),
        bucket_incarnation: incarnation,
        watch_id: None,
        release: false,
        session_id: None,
    };
    let first = known(&store, &request).await;
    assert!(first.is_well_formed());
    assert_eq!(first.inflight, 0);
    request.watch_id = Some(first.watch_id);
    assert!(known(&store, &request).await.unchanged_since(&first));

    let reader =
        rustfs_rio::HashReader::from_stream(std::io::Cursor::new(b"payload"), 7, 7, None, None, false).expect("PUT body reader");
    store
        .put_object(bucket, "object", &mut PutObjReader::new(reader), &ObjectOptions::default())
        .await
        .expect("real PUT");
    let written = known(&store, &request).await;
    assert_eq!(written.watch_id, first.watch_id);
    assert!(written.revision > first.revision);
    assert_eq!(written.inflight, 0);

    store
        .delete_object(bucket, "object", ObjectOptions::default())
        .await
        .expect("real object delete");
    let deleted = known(&store, &request).await;
    assert_eq!(deleted.watch_id, first.watch_id);
    assert!(deleted.revision > written.revision);
    assert_eq!(deleted.inflight, 0);
    store
        .delete_bucket(bucket, &DeleteBucketOptions::default())
        .await
        .expect("delete empty bucket");
    assert_eq!(store.scanner_usage_watch(std::slice::from_ref(&request)).await[0].known, None);

    store
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("recreate watched bucket");
    let recreated_incarnation = store.bucket_incarnation_id_from_disk(bucket).await.expect("new incarnation");
    assert_ne!(recreated_incarnation, incarnation);
    assert_eq!(store.scanner_usage_watch(std::slice::from_ref(&request)).await[0].known, None);
    request.bucket_incarnation = recreated_incarnation;
    assert_eq!(store.scanner_usage_watch(std::slice::from_ref(&request)).await[0].known, None);
    request.watch_id = None;
    let recreated = known(&store, &request).await;
    assert_ne!(recreated.watch_id, first.watch_id);
    assert!(!recreated.unchanged_since(&first));
    assert_eq!(recreated.inflight, 0);
}
