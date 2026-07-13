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

//! Regression coverage for rustfs/backlog#1009: the PUT path skips its
//! pre-PUT `get_object_info` only when `put_prelookup_worm_gate` proves the
//! existing-object WORM validation is a no-op. These tests pin the gate's
//! truth table against a real 4-disk `ECStore` with the bucket metadata sys
//! initialized, mirroring the HP-8 delete-gating fixture:
//!
//! - a bucket created with Object Lock keeps the prelookup (gate = true);
//! - a plain bucket takes the skip path (gate = false);
//! - an unknown bucket (metadata lookup error) fails closed (gate = true),
//!   so a degraded metadata subsystem can never silently drop the WORM check.

use super::gating_test_env::shared_gating_ecstore;
use super::object_usecase::put_prelookup_worm_gate;
use super::storage_api::test::contract::bucket::{BucketOperations, MakeBucketOptions};
use serial_test::serial;
use uuid::Uuid;

#[tokio::test]
#[serial]
async fn worm_gate_keeps_prelookup_for_object_lock_bucket() {
    let ecstore = shared_gating_ecstore().await;
    let bucket = format!("put-gate-lock-{}", Uuid::new_v4());

    ecstore
        .make_bucket(
            &bucket,
            &MakeBucketOptions {
                lock_enabled: true,
                ..Default::default()
            },
        )
        .await
        .expect("create object-lock bucket");

    assert!(
        put_prelookup_worm_gate(&bucket).await,
        "an object-lock bucket must keep the pre-PUT lookup"
    );
}

#[tokio::test]
#[serial]
async fn worm_gate_allows_skip_for_plain_bucket() {
    let ecstore = shared_gating_ecstore().await;
    let bucket = format!("put-gate-plain-{}", Uuid::new_v4());

    ecstore
        .make_bucket(&bucket, &MakeBucketOptions::default())
        .await
        .expect("create plain bucket");

    assert!(
        !put_prelookup_worm_gate(&bucket).await,
        "a bucket without object locking must take the prelookup-skip path"
    );
}

#[tokio::test]
#[serial]
async fn worm_gate_fails_closed_when_bucket_metadata_is_unavailable() {
    let _ecstore = shared_gating_ecstore().await;
    let missing_bucket = format!("put-gate-missing-{}", Uuid::new_v4());

    assert!(
        put_prelookup_worm_gate(&missing_bucket).await,
        "a bucket-metadata lookup failure must fail closed and keep the pre-PUT lookup"
    );
}
