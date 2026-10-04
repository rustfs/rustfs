// Copyright 2026 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![recursion_limit = "256"]

use http::HeaderMap;
use rustfs_heal::heal::{
    channel::HealChannelProcessor,
    manager::{HealConfig, HealManager},
    outcome::HealExecutionOutcome,
    storage::{ECStoreHealStorage, HealStorageAPI},
    task::HealTaskStatus,
};
use rustfs_heal_contracts::heal_channel::{HealChannelCommand, HealRequestSource, init_heal_channel};
use rustfs_test_utils::TestECStoreEnv;
use std::{sync::Arc, time::Duration};

mod storage_api;
use storage_api::integration::{ObjectIO, ObjectOperations, ObjectOptions, PutObjReader, RUSTFS_META_BUCKET, WriteCompletion};

// Owns the process-global heal receiver in this integration-test binary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn usage_observation_read_repair_survives_snapshot_cleanup() {
    let temp = tempfile::tempdir().expect("test storage directory");
    let env = TestECStoreEnv::builder().base_dir(temp.path()).build().await;
    let bucket = RUSTFS_META_BUCKET;
    let object = "buckets/.usage.observed.json";
    let payload = br#"{"usageSnapshotConverged":false}"#.to_vec();
    env.ecstore
        .put_object(
            bucket,
            object,
            &mut PutObjReader::from_vec(payload.clone()),
            &ObjectOptions {
                write_completion: WriteCompletion::TailDrained,
                ..Default::default()
            },
        )
        .await
        .expect("write the observation to all disks");
    for disk in &env.disk_paths {
        assert!(disk.join(bucket).join(object).join("xl.meta").exists());
    }

    let mut receiver = init_heal_channel().expect("sole heal receiver in this test binary");
    tokio::fs::remove_dir_all(env.disk_paths[0].join(bucket).join(object))
        .await
        .expect("remove one observation replica while retaining read quorum");
    let mut reader = env
        .ecstore
        .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
        .await
        .expect("degraded observation remains readable");
    assert_eq!(reader.read_all().await.expect("read complete observation"), payload);
    drop(reader);

    let (request, response_tx) = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let HealChannelCommand::Start { request, response_tx } = receiver.recv().await.expect("heal channel open") {
                assert_eq!(request.bucket, bucket);
                assert_eq!(request.object_prefix.as_deref(), Some(object));
                break (request, response_tx);
            }
        }
    })
    .await
    .expect("the real degraded GET must emit a repair request");
    assert_eq!(request.source, HealRequestSource::ReadRepair);
    assert_eq!(request.recreate_missing, Some(true));

    // Authoritative usage publication deletes the obsolete observation with
    // these prefix options. Delay admission until that deletion has finished.
    env.ecstore
        .delete_object(
            bucket,
            object,
            ObjectOptions {
                delete_prefix: true,
                delete_prefix_object: true,
                ..Default::default()
            },
        )
        .await
        .expect("clean up the obsolete observation before repair runs");
    let storage = Arc::new(ECStoreHealStorage::new(env.ecstore.clone()));
    assert!(
        !storage
            .object_exists(bucket, object)
            .await
            .expect("check observation absence")
    );

    let manager = Arc::new(HealManager::new(
        storage,
        Some(HealConfig {
            enable_auto_heal: false,
            mainline_throttle_enable: false,
            heal_interval: Duration::from_millis(10),
            ..Default::default()
        }),
    ));
    manager.start().await.expect("start the real heal scheduler");
    let processor = HealChannelProcessor::new(manager.clone());
    let receipt = processor
        .execute_start_request(request)
        .await
        .expect("admit the captured read repair");
    assert!(receipt.result.is_admitted());
    response_tx
        .send(Ok(receipt.result))
        .expect("acknowledge the GET's repair admission");

    let terminal = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let report = manager
                .get_task_report(&receipt.task_id)
                .await
                .expect("query scheduled repair");
            if matches!(report.status, HealTaskStatus::Completed | HealTaskStatus::Failed { .. }) {
                break report;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    manager.stop().await.expect("stop the heal scheduler");
    let report = terminal.expect("repair must reach a terminal result");
    assert_eq!(report.status, HealTaskStatus::Completed, "{report:?}");
    let outcome = report.outcome.expect("scheduler retains the repair outcome");
    assert_eq!(outcome.execution, HealExecutionOutcome::Completed);
    assert_eq!(outcome.counters.failed, 0);
    assert_eq!(outcome.counters.skipped, 1);
    let progress = report.progress.expect("scheduler retains repair progress");
    assert_eq!(progress.objects_healed, 0);
    assert_eq!(progress.skipped_objects, 1);
    for disk in &env.disk_paths {
        assert!(!disk.join(bucket).join(object).join("xl.meta").exists());
    }
}
