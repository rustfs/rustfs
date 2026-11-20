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

fn watch_node_topology(local_nodes: &[usize]) -> EndpointServerPools {
    let endpoints = (0..4)
        .map(|slot| {
            let node = slot % 2;
            let url = format!("http://node-{node}:9000/data{slot}");
            let mut endpoint = Endpoint::try_from(url.as_str()).expect("node endpoint");
            endpoint.is_local = local_nodes.contains(&node);
            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(slot);
            endpoint
        })
        .collect::<Vec<_>>();
    vec![crate::layout::endpoints::PoolEndpoints {
        legacy: false,
        set_count: 1,
        drives_per_set: endpoints.len(),
        endpoints: endpoints.into(),
        cmd_line: String::new(),
        platform: String::new(),
    }]
    .into()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_deployment_issues_distinct_usage_watch_owners_through_both_node_views() {
    // Open the same formatted storage with two independent producer registries.
    // Each sees the same deployment and bucket through its own node's routing view.
    let (_dirs, first) = isolated_store_over_temp_disks().await;
    let disks = EndpointServerPools::from(vec![first.pools[0].endpoints.clone()]);
    let second_ctx = Arc::new(InstanceContext::new());
    init_local_disks_with_instance_ctx(&second_ctx, disks.clone())
        .await
        .expect("second disk registry");
    let second = ECStore::new_with_instance_ctx(
        "127.0.0.1:0".parse().expect("local test address"),
        disks,
        CancellationToken::new(),
        second_ctx,
    )
    .await
    .expect("second storage instance");
    assert_eq!(first.id, second.id, "both producers read the same deployment from disk");
    assert_eq!(
        first.scanner_usage_watch_owner_id(),
        Some(first.id),
        "a direct file-backed store keeps its owner"
    );
    assert_eq!(first.scanner_usage_watch_peer_owner_id("http://node-1:9000"), None);
    first.ctx.set_endpoints(watch_node_topology(&[0]));
    second.ctx.set_endpoints(watch_node_topology(&[1]));
    init_bucket_metadata_sys(first.clone(), Vec::new()).await;
    init_bucket_metadata_sys(second.clone(), Vec::new()).await;
    let bucket = "shared-watch-deployment";
    first
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("create shared bucket");
    let incarnation = first
        .bucket_incarnation_id_from_disk(bucket)
        .await
        .expect("first incarnation");
    assert_eq!(
        second
            .bucket_incarnation_id_from_disk(bucket)
            .await
            .expect("second incarnation"),
        incarnation
    );
    let mut request = ScannerUsageWatchRequest {
        bucket: bucket.into(),
        bucket_incarnation: incarnation,
        watch_id: None,
        session_id: None,
        release: false,
    };
    let a = known(&first, &request).await;
    let b = known(&second, &request).await;
    assert_ne!(a.owner_id, b.owner_id, "watch owners must distinguish nodes in one deployment");
    assert_eq!(first.scanner_usage_watch_owner_id(), Some(a.owner_id));
    assert_eq!(second.scanner_usage_watch_owner_id(), Some(b.owner_id));
    assert_eq!(first.scanner_usage_watch_peer_owner_id("http://node-1:9000"), Some(b.owner_id));
    assert_eq!(second.scanner_usage_watch_peer_owner_id("http://node-0:9000"), Some(a.owner_id));
    assert_eq!(first.scanner_usage_watch_peer_owner_id("http://node-0:9000"), None);
    assert_eq!(first.scanner_usage_watch_peer_owner_id("http://unknown:9000"), None);
    assert_ne!(a.session_id, b.session_id);
    request.watch_id = Some(a.watch_id);
    request.session_id = Some(a.session_id);
    request.release = true;
    second.scanner_release_usage_watch(&request);
    assert!(
        known(
            &second,
            &ScannerUsageWatchRequest {
                watch_id: Some(b.watch_id),
                session_id: Some(b.session_id),
                release: false,
                ..request.clone()
            }
        )
        .await
        .unchanged_since(&b)
    );
    first.scanner_release_usage_watch(&request);
    request.release = false;
    assert!(
        first.scanner_usage_watch(&[request]).await[0].known.is_none(),
        "released token cannot be revived"
    );
}

#[test]
fn usage_watch_routes_require_one_unambiguous_local_node() {
    use crate::runtime::usage_watch::UsageWatchOwners;
    let deployment = Uuid::new_v4();
    for local_nodes in [&[][..], &[0, 1][..]] {
        assert!(UsageWatchOwners::from_topology(deployment, &watch_node_topology(local_nodes)).is_none());
    }
    assert!(UsageWatchOwners::from_topology(Uuid::nil(), &watch_node_topology(&[0])).is_none());
    let mut incomplete = watch_node_topology(&[0]);
    incomplete.0[0].drives_per_set += 1;
    assert!(UsageWatchOwners::from_topology(deployment, &incomplete).is_none());
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
