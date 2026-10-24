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

use super::*;
use crate::storage_api::scan::{BucketOperations, DeleteBucketOptions, MakeBucketOptions};
use crate::storage_api::scanner_io::{BUCKET_LIFECYCLE_CONFIG, BUCKET_REPLICATION_CONFIG, BucketInfo, update_bucket_metadata};

pub(super) fn startup_config(entries: &[(&str, &str)]) -> rustfs_config::server_config::Config {
    init_ecstore_config_for_scanner_tests();
    let mut config = rustfs_config::server_config::Config::new();
    let mut kvs = rustfs_config::server_config::KVS::new();
    for (key, value) in entries {
        kvs.insert((*key).to_owned(), (*value).to_owned());
    }
    config.0.insert(
        rustfs_config::SCANNER_SUB_SYS.to_owned(),
        HashMap::from([(rustfs_config::DEFAULT_DELIMITER.to_owned(), kvs)]),
    );
    config.set_defaults();
    config
}

#[tokio::test(start_paused = true)]
#[serial]
async fn startup_preflight_timeout_drops_read_only_inspection() {
    let ctx = CancellationToken::new();
    let (held, dropped) = tokio::sync::oneshot::channel::<()>();
    let inspection = async move {
        let _held = held;
        std::future::pending::<(bool, bool)>().await
    };
    let started = Instant::now();
    assert!(wait_for_scanner_startup(&ctx, inspection, false, scanner_maintenance_generation()).await);
    assert_eq!(started.elapsed(), SCANNER_STARTUP_USAGE_INSPECTION_TIMEOUT);
    assert!(dropped.await.is_err(), "timeout should drop the pending read-only inspection");
}

#[tokio::test(start_paused = true)]
#[serial]
async fn startup_cancellation_interrupts_preflight_and_long_delay() {
    let config = startup_config(&[("start_delay", "21600"), ("cycle", "21600")]);
    crate::runtime_config::apply_scanner_runtime_config(&config).expect("long startup configuration");
    for pending_inspection in [true, false] {
        let ctx = CancellationToken::new();
        let inspection = async move {
            if pending_inspection {
                std::future::pending::<()>().await;
            }
            (false, true)
        };
        let started = Instant::now();
        let mut gate = Box::pin(wait_for_scanner_startup(&ctx, inspection, false, scanner_maintenance_generation()));
        assert!(matches!(futures::poll!(&mut gate), Poll::Pending));
        ctx.cancel();
        assert!(!gate.await);
        assert_eq!(started.elapsed(), Duration::ZERO);
    }
    crate::runtime_config::refresh_scanner_runtime_config_for_tests();
}

#[tokio::test(start_paused = true)]
#[serial]
async fn startup_shorter_config_uses_elapsed_wait_instead_of_restarting() {
    let config = startup_config(&[("start_delay", "21600"), ("cycle", "21600")]);
    crate::runtime_config::apply_scanner_runtime_config(&config).expect("long startup configuration");
    let ctx = CancellationToken::new();
    let mut gate = Box::pin(wait_for_scanner_startup(
        &ctx,
        std::future::ready((false, true)),
        false,
        scanner_maintenance_generation(),
    ));
    assert!(matches!(futures::poll!(&mut gate), Poll::Pending));
    advance(Duration::from_secs(30)).await;
    let config = startup_config(&[("start_delay", "1"), ("cycle", "21600")]);
    crate::runtime_config::apply_scanner_runtime_config(&config).expect("short startup configuration");
    assert!(matches!(futures::poll!(&mut gate), Poll::Ready(true)));
    assert_eq!(scanner_cycle_interval(), Duration::from_secs(21600));
    crate::runtime_config::refresh_scanner_runtime_config_for_tests();
}

#[tokio::test(start_paused = true)]
#[serial]
async fn startup_unrelated_config_keeps_original_deadline() {
    let config = startup_config(&[("start_delay", "120"), ("cycle", "21600")]);
    crate::runtime_config::apply_scanner_runtime_config(&config).expect("startup configuration");
    let ctx = CancellationToken::new();
    let started = Instant::now();
    let mut gate = Box::pin(wait_for_scanner_startup(
        &ctx,
        std::future::ready((false, true)),
        false,
        scanner_maintenance_generation(),
    ));
    assert!(matches!(futures::poll!(&mut gate), Poll::Pending));
    advance(Duration::from_secs(60)).await;
    let config = startup_config(&[("start_delay", "120"), ("cycle", "21600"), ("alert_excess_versions", "101")]);
    crate::runtime_config::apply_scanner_runtime_config(&config).expect("unrelated configuration update");
    assert!(matches!(futures::poll!(&mut gate), Poll::Pending));
    advance(Duration::from_secs(72)).await;
    assert!(gate.await);
    assert_eq!(started.elapsed(), Duration::from_secs(132));
    crate::runtime_config::refresh_scanner_runtime_config_for_tests();
}

#[tokio::test(start_paused = true)]
#[serial]
async fn runtime_wait_observes_changes_before_and_after_registration() {
    for poll_first in [false, true] {
        for maintenance in [false, true] {
            let config = startup_config(&[("start_delay", "21600"), ("cycle", "21600")]);
            crate::runtime_config::apply_scanner_runtime_config(&config).expect("initial configuration");
            let ctx = CancellationToken::new();
            let started = Instant::now();
            let mut wait = Box::pin(wait_for_scanner_runtime_wake(
                &ctx,
                started + Duration::from_secs(21600),
                scanner_runtime_config_generation(),
                scanner_maintenance_generation(),
            ));
            if poll_first {
                assert!(matches!(futures::poll!(&mut wait), Poll::Pending));
            }
            let expected = if maintenance {
                crate::scanner_io::record_scanner_maintenance_change("startup-rule-change");
                ScannerCycleWakeReason::MaintenanceConfig
            } else {
                let config = startup_config(&[("start_delay", "1"), ("cycle", "21600")]);
                crate::runtime_config::apply_scanner_runtime_config(&config).expect("changed configuration");
                ScannerCycleWakeReason::RuntimeConfig
            };
            assert_eq!(wait.await, expected);
            assert_eq!(started.elapsed(), Duration::ZERO);
        }
    }
    crate::runtime_config::refresh_scanner_runtime_config_for_tests();
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
}

#[async_trait::async_trait]
impl BucketOperations for MemoryConfigStore {
    type Error = EcstoreError;

    async fn make_bucket(&self, _bucket: &str, _opts: &MakeBucketOptions) -> EcstoreResult<()> {
        Err(EcstoreError::other("memory config fixture cannot create buckets"))
    }

    async fn get_bucket_info(&self, _bucket: &str, _opts: &BucketOptions) -> EcstoreResult<BucketInfo> {
        Err(EcstoreError::other("memory config fixture has no bucket metadata"))
    }

    async fn list_bucket(&self, _opts: &BucketOptions) -> EcstoreResult<Vec<BucketInfo>> {
        self.bucket_list_calls.fetch_add(1, Ordering::Relaxed);
        Ok(Vec::new())
    }

    async fn delete_bucket(&self, _bucket: &str, _opts: &DeleteBucketOptions) -> EcstoreResult<()> {
        Err(EcstoreError::other("memory config fixture cannot delete buckets"))
    }
}

#[tokio::test]
async fn startup_only_lists_buckets_for_warm_active_replication() {
    for (cold, replication, expected_lists) in [(true, false, 0), (true, true, 0), (false, false, 0), (false, true, 1)] {
        let store = Arc::new(MemoryConfigStore::default());
        if !cold {
            let usage = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0);
            store.objects.lock().await.insert(
                memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str()),
                serde_json::to_vec(&usage).expect("complete startup snapshot"),
            );
        }
        assert_eq!(initial_scanner_startup_usage_state(&store, replication).await, (cold, false));
        assert_eq!(store.bucket_list_calls.load(Ordering::Relaxed), expected_lists);
    }
}

#[tokio::test]
#[serial]
async fn startup_discovers_replication_after_lifecycle_in_another_bucket() {
    let (_dir, store) = setup_scanner_cycle_store().await;
    for bucket in ["a-lifecycle", "z-replication"] {
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("maintenance fixture bucket");
    }
    let lifecycle = br#"<LifecycleConfiguration><Rule><ID>expire</ID><Status>Enabled</Status><Filter><Prefix></Prefix></Filter><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>"#;
    update_bucket_metadata("a-lifecycle", BUCKET_LIFECYCLE_CONFIG, lifecycle.to_vec())
        .await
        .expect("lifecycle policy");
    let replication = br#"<ReplicationConfiguration><Role>arn:aws:iam::123456789012:role/replication-role</Role><Rule><ID>replicate</ID><Status>Enabled</Status><Prefix></Prefix><Destination><Bucket>arn:aws:s3:::destination-bucket</Bucket></Destination></Rule></ReplicationConfiguration>"#;
    update_bucket_metadata("z-replication", BUCKET_REPLICATION_CONFIG, replication.to_vec())
        .await
        .expect("replication policy");
    let features = detect_scanner_maintenance_features(&store).await;
    assert!(features.lifecycle);
    assert!(features.replication, "an earlier lifecycle bucket must not hide later replication rules");
    assert!(!features.inspection_failed);
    let state = initial_scanner_startup_usage_state(&store, features.replication).await;
    assert!(state.1, "warm replication startup should observe existing buckets");
    assert_eq!(
        initial_scanner_delay_for_startup(
            &ScannerRuntimeConfig {
                start_delay: Some(Duration::from_secs(21600)),
                ..Default::default()
            },
            state.0,
            state.1,
            features.replication,
        ),
        Duration::ZERO
    );
}

#[tokio::test]
#[serial]
async fn startup_replication_discovery_does_not_inspect_irrelevant_lifecycle_tail() {
    let (_dir, store) = setup_scanner_cycle_store().await;
    for bucket in ["a-replication", "z-lifecycle"] {
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("maintenance fixture bucket");
    }
    let replication = br#"<ReplicationConfiguration><Role>arn:aws:iam::123456789012:role/replication-role</Role><Rule><ID>replicate</ID><Status>Enabled</Status><Prefix></Prefix><Destination><Bucket>arn:aws:s3:::destination-bucket</Bucket></Destination></Rule></ReplicationConfiguration>"#;
    update_bucket_metadata("a-replication", BUCKET_REPLICATION_CONFIG, replication.to_vec())
        .await
        .expect("replication policy");
    let lifecycle = br#"<LifecycleConfiguration><Rule><ID>expire</ID><Status>Enabled</Status><Filter><Prefix></Prefix></Filter><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>"#;
    update_bucket_metadata("z-lifecycle", BUCKET_LIFECYCLE_CONFIG, lifecycle.to_vec())
        .await
        .expect("lifecycle policy");
    let features = detect_scanner_maintenance_features(&store).await;
    assert!(features.replication);
    assert!(
        !features.lifecycle,
        "replication already resolves scheduling; the later lifecycle bucket should remain uninspected"
    );
    assert!(!features.inspection_failed);
}
