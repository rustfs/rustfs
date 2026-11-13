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

//! PutObjectRetention and PutObjectLegalHold tests (batch B1 owns both).

use super::common::*;
use super::*;

/// The object-lock handlers do not go through the object PUT path, so they must schedule
/// replication themselves. Without it a retention or legal hold applied after upload stays
/// local and the replica keeps its previous, unprotected state (a WORM object that is still
/// deletable on the peer). This observes the shared decision counter: each handler must
/// compute a replication decision exactly once when replication is configured, which is
/// the step that drives both the persisted pending marker and the schedule.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "global-state usecase integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
async fn object_lock_handlers_schedule_replication() {
    use super::super::storage_api::object_usecase::bucket::replication::MUST_REPLICATE_OBJECT_CALLS;
    use crate::app::storage_api::s3::S3;
    use std::sync::atomic::Ordering;

    let (_disk_paths, ecstore) = setup_test_env().await;
    let fs = FS::new();

    let bucket = format!("test-lock-repl-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "locked/object.txt";

    (*ecstore)
        .make_bucket(
            bucket.as_str(),
            &MakeBucketOptions {
                lock_enabled: true,
                versioning_enabled: true,
                ..Default::default()
            },
        )
        .await
        .expect("Failed to create object-lock bucket");
    upload_test_object(&ecstore, bucket.as_str(), object, b"worm payload").await;

    // PutObjectRetention must compute a replication decision.
    MUST_REPLICATE_OBJECT_CALLS.store(0, Ordering::SeqCst);
    let retention_input = PutObjectRetentionInput::builder()
        .bucket(bucket.clone())
        .key(object.to_string())
        .retention(Some(ObjectLockRetention {
            mode: Some(ObjectLockRetentionMode::from_static(ObjectLockRetentionMode::COMPLIANCE)),
            retain_until_date: Some(time::macros::datetime!(2030-01-01 00:00:00 UTC).into()),
        }))
        .build()
        .unwrap();
    fs.put_object_retention(build_request(retention_input, Method::PUT))
        .await
        .expect("put_object_retention should succeed");
    assert_eq!(
        MUST_REPLICATE_OBJECT_CALLS.load(Ordering::SeqCst),
        1,
        "PutObjectRetention must compute the replication decision exactly once; 0 means the \
         retention change never replicates and the peer copy stays unprotected"
    );

    // A bucket without a replication configuration can skip the decision.
    MUST_REPLICATE_OBJECT_CALLS.store(0, Ordering::SeqCst);
    let no_replication_input = PutObjectLegalHoldInput::builder()
        .bucket(bucket.clone())
        .key(object.to_string())
        .legal_hold(Some(ObjectLockLegalHold {
            status: Some(ObjectLockLegalHoldStatus::from_static(ObjectLockLegalHoldStatus::OFF)),
        }))
        .build()
        .unwrap();
    fs.put_object_legal_hold(build_request(no_replication_input, Method::PUT))
        .await
        .expect("legal hold without replication should succeed");
    assert_eq!(
        MUST_REPLICATE_OBJECT_CALLS.load(Ordering::SeqCst),
        0,
        "a bucket without replication must skip the decision"
    );

    // A configured bucket must still evaluate the metadata replication decision.
    install_delete_replication_config(&bucket, true, false, None).await;
    MUST_REPLICATE_OBJECT_CALLS.store(0, Ordering::SeqCst);
    let legal_hold_input = PutObjectLegalHoldInput::builder()
        .bucket(bucket.clone())
        .key(object.to_string())
        .legal_hold(Some(ObjectLockLegalHold {
            status: Some(ObjectLockLegalHoldStatus::from_static(ObjectLockLegalHoldStatus::ON)),
        }))
        .build()
        .unwrap();
    fs.put_object_legal_hold(build_request(legal_hold_input, Method::PUT))
        .await
        .expect("put_object_legal_hold should succeed");
    assert_eq!(
        MUST_REPLICATE_OBJECT_CALLS.load(Ordering::SeqCst),
        1,
        "PutObjectLegalHold must compute the replication decision exactly once; 0 means the \
         legal hold never replicates and the peer copy stays deletable"
    );
}
