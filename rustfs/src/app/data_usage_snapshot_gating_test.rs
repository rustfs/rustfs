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

//! Revert detector for rustfs/backlog#1306: the admin data usage endpoint must
//! serve the persisted scanner snapshot plus the in-memory overlay and never
//! trigger a live full-version listing on the request path.
//!
//! The observable is the always-on `live_bucket_usage_computations()` counter
//! incremented by `compute_bucket_usage` (the only entry point into request
//! path full version listings). The test first proves the counter moves when a
//! live computation *does* run (positive control, so a broken counter cannot
//! make the guard pass vacuously), then drives the endpoint use case
//! end-to-end against a pre-seeded snapshot and asserts the counter stays put
//! while the response carries the seeded numbers.

use super::gating_test_env::shared_gating_ecstore;
use super::storage_api::test::StoragePutObjReader as PutObjReader;
use super::storage_api::test::contract::bucket::{BucketOperations, MakeBucketOptions};
use super::storage_api::test::contract::object::ObjectIO as _;
use super::storage_api::test::data_usage::{
    compute_bucket_usage, live_bucket_usage_computations, record_bucket_object_write_memory, store_data_usage_in_backend,
};
use crate::app::admin_usecase::DefaultAdminUsecase;
use rustfs_data_usage::{BucketUsageInfo, DataUsageInfo};
use serial_test::serial;
use std::collections::HashMap;
use std::time::SystemTime;
use uuid::Uuid;

const SEEDED_BUCKET_SIZE: u64 = 123_456;
const SEEDED_BUCKET_OBJECTS: u64 = 42;

fn seeded_data_usage_info(bucket: &str, last_update: SystemTime) -> DataUsageInfo {
    let usage = BucketUsageInfo {
        size: SEEDED_BUCKET_SIZE,
        objects_count: SEEDED_BUCKET_OBJECTS,
        versions_count: SEEDED_BUCKET_OBJECTS,
        ..Default::default()
    };

    let mut info = DataUsageInfo {
        last_update: Some(last_update),
        buckets_count: 1,
        objects_total_count: SEEDED_BUCKET_OBJECTS,
        objects_total_size: SEEDED_BUCKET_SIZE,
        ..Default::default()
    };
    info.buckets_usage = HashMap::from([(bucket.to_string(), usage)]);
    info.bucket_sizes = HashMap::from([(bucket.to_string(), SEEDED_BUCKET_SIZE)]);
    info
}

#[tokio::test]
#[serial]
async fn data_usage_endpoint_serves_snapshot_without_live_listing() {
    let ecstore = shared_gating_ecstore().await;
    let live_bucket = format!("usage-live-{}", Uuid::new_v4());
    let seeded_bucket = format!("usage-seeded-{}", Uuid::new_v4());
    let overlay_bucket = format!("usage-overlay-{}", Uuid::new_v4());

    // Positive control: a direct live computation must move the revert
    // detector counter, otherwise the "no increment" assertion below would be
    // vacuously true with a broken counter.
    ecstore
        .make_bucket(&live_bucket, &MakeBucketOptions::default())
        .await
        .expect("create live bucket");
    for object in ["control-a.bin", "control-b.bin"] {
        let mut reader = PutObjReader::from_vec(b"live payload".to_vec());
        ecstore
            .put_object(&live_bucket, object, &mut reader, &Default::default())
            .await
            .expect("put control object");
    }

    let before_control = live_bucket_usage_computations();
    let control_usage = compute_bucket_usage(ecstore.clone(), &live_bucket)
        .await
        .expect("live computation over the control bucket");
    assert_eq!(control_usage.objects_count, 2, "control bucket must be fully listed");
    assert!(
        live_bucket_usage_computations() > before_control,
        "positive control: compute_bucket_usage must increment the live-listing counter"
    );

    // Pre-seed a scanner snapshot for a bucket the endpoint has to serve
    // verbatim, then record an in-memory overlay write for another bucket.
    ecstore
        .make_bucket(&seeded_bucket, &MakeBucketOptions::default())
        .await
        .expect("create seeded bucket");
    let seeded_at = SystemTime::now();
    store_data_usage_in_backend(seeded_data_usage_info(&seeded_bucket, seeded_at), ecstore.clone())
        .await
        .expect("persist seeded data usage snapshot");

    record_bucket_object_write_memory(&overlay_bucket, None, 512).await;

    let before_endpoint = live_bucket_usage_computations();
    let info = DefaultAdminUsecase::query_data_usage_info_with_store(ecstore.clone())
        .await
        .expect("query data usage info");
    assert_eq!(
        live_bucket_usage_computations(),
        before_endpoint,
        "revert detector: the data usage endpoint must not run live full-version listings"
    );

    // The endpoint must serve the seeded snapshot numbers, not recomputed ones.
    assert_eq!(info.last_update, Some(seeded_at), "endpoint must report the snapshot timestamp");
    let seeded_usage = info
        .buckets_usage
        .get(&seeded_bucket)
        .expect("seeded bucket must come from the snapshot");
    assert_eq!(seeded_usage.size, SEEDED_BUCKET_SIZE);
    assert_eq!(seeded_usage.objects_count, SEEDED_BUCKET_OBJECTS);

    // The in-memory overlay stays applied on top of the snapshot.
    let overlay_usage = info
        .buckets_usage
        .get(&overlay_bucket)
        .expect("overlay bucket must come from the memory overlay");
    assert_eq!(overlay_usage.size, 512);
}

/// Wire pin for the no-snapshot response shape (rustfs/backlog#1306): a
/// default `DataUsageInfo` must keep serializing `last_update` as `null` with
/// empty bucket maps, so "no snapshot yet" stays distinguishable from real
/// stats and a future `skip_serializing_if`/`now()` fallback trips this test.
#[test]
fn data_usage_info_default_serializes_null_last_update_and_empty_buckets() {
    let value = serde_json::to_value(DataUsageInfo::default()).expect("serialize default DataUsageInfo");

    assert!(value["last_update"].is_null(), "last_update must serialize as null: {value}");
    assert_eq!(value["buckets_count"], 0);
    assert!(
        value["buckets_usage"].as_object().is_some_and(|map| map.is_empty()),
        "buckets_usage must serialize as an empty map: {value}"
    );
    assert!(
        value["bucket_sizes"].as_object().is_some_and(|map| map.is_empty()),
        "bucket_sizes must serialize as an empty map: {value}"
    );
}
