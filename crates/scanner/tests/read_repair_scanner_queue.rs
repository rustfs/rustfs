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

//! One process exercises dangling metadata, scanner disk reads, and heal admission.

use std::{collections::HashMap, sync::Arc, time::Duration};

use rustfs_ecstore::api::{
    capacity::path2_bucket_object_with_base_path,
    config::storageclass::{CLASS_STANDARD, DEFAULT_KVS, STANDARD, lookup_config},
    disk::{DeleteOptions, DiskAPI, DiskError, ReadOptions, STORAGE_FORMAT_FILE},
    object::{ObjectOptions, PutObjReader, WriteCompletion},
};
use rustfs_filemeta::FileInfo;
use rustfs_heal::{
    HealChannelProcessor,
    heal::{
        manager::{HealConfig, HealManager},
        storage::ECStoreHealStorage,
        task::HealTaskStatus,
    },
};
use rustfs_heal_contracts::heal_channel::{
    HealChannelCommand, HealChannelResponse, HealRequestSource, init_heal_channels, subscribe_heal_responses,
};
use rustfs_scanner::{scanner_folder::ScannerItem, scanner_heal_observation_status, scanner_io::ScannerIODisk};
use rustfs_storage_api::{ObjectIO, ObjectOperations};
use rustfs_test_utils::TestECStoreEnv;
use time::OffsetDateTime;
use tokio::{sync::broadcast, time::timeout};
use uuid::Uuid;

const DEADLINE: Duration = Duration::from_secs(60);

async fn accepted_response(responses: &mut broadcast::Receiver<HealChannelResponse>, id: &str) {
    let response = timeout(DEADLINE, responses.recv())
        .await
        .expect("heal admission response timed out")
        .expect("heal admission response channel closed or lagged");
    assert_eq!(response.request_id, id, "admission must belong to the forwarded request");
    assert!(response.success, "heal admission failed: {:?}", response.error);
    assert_eq!(response.data.as_deref(), Some(&b"admission=accepted,reason=none"[..]));
}

async fn wait_for_scanner_pending(expected: usize) {
    let expected_candidates = u64::try_from(expected).expect("bounded scanner candidate count");
    timeout(DEADLINE, async {
        loop {
            let status = scanner_heal_observation_status();
            assert_ne!(status.state, "stopped", "scanner observer stopped");
            assert!(status.unknown_candidates.is_empty(), "scanner observer lost a candidate: {status:?}");
            if status.state == "active" {
                assert_eq!(status.lost_completion_notifications, Some(0));
                assert_eq!(status.terminal_task_observations_total, Some(0));
                if status.retained_pending_tasks == Some(expected)
                    && status.retained_pending_candidates == Some(expected_candidates)
                    && status.queued_admissions == Some(0)
                {
                    break;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("scanner receipt observation timed out: {:?}", scanner_heal_observation_status()));
}

#[tokio::test]
async fn dangling_metadata_read_repair_and_scanner_have_distinct_queued_owners() {
    // This test binary owns the process-wide channels and storage-class config.
    temp_env::async_with_vars(
        [
            ("RUSTFS_STORAGE_CLASS_STANDARD", Some("EC:2")),
            ("RUSTFS_STORAGE_CLASS_RRS", Some("EC:1")),
        ],
        async {
            let root = tempfile::tempdir().expect("create isolated six-disk fixture");
            let env = TestECStoreEnv::builder().disk_count(6).base_dir(root.path()).build().await;
            let mut kvs = (*DEFAULT_KVS).clone();
            kvs.insert(CLASS_STANDARD.to_owned(), "EC:2".to_owned());
            let storage_class = lookup_config(&kvs, 6).expect("configure EC 4+2");
            assert_eq!(storage_class.get_parity_for_sc(STANDARD), Some(2));
            env.ecstore.set_storage_class(storage_class);

            let bucket = "read-repair-scanner";
            let object = "torn/latest";
            env.make_bucket(bucket, true).await;
            let sets = env.ecstore.all_set_disks();
            assert_eq!(sets.len(), 1, "fixture must have one erasure set");
            let set = &sets[0];
            assert_eq!((set.pool_index, set.set_index), (0, 0));
            let options = |seconds| ObjectOptions {
                versioned: true,
                mod_time: Some(OffsetDateTime::from_unix_timestamp(seconds).expect("fixture timestamp")),
                write_completion: WriteCompletion::TailDrained,
                ..Default::default()
            };
            let v1 = set
                .put_object(
                    bucket,
                    object,
                    &mut PutObjReader::from_vec(b"version one".to_vec()),
                    &options(1_700_000_000),
                )
                .await
                .expect("write committed first version")
                .version_id
                .expect("first version id");
            let v2 = set
                .put_object(
                    bucket,
                    object,
                    &mut PutObjReader::from_vec(b"version two".to_vec()),
                    &options(1_700_000_060),
                )
                .await
                .expect("write second version")
                .version_id
                .expect("second version id");
            assert_ne!(v1, v2);

            let disks = set.disks.read().await.clone();
            assert_eq!(disks.len(), 6);
            for disk in &disks[3..] {
                let disk = disk.as_ref().expect("all fixture disks are online");
                let mut remove = FileInfo {
                    volume: bucket.to_owned(),
                    name: object.to_owned(),
                    version_id: Some(v2),
                    ..Default::default()
                };
                remove.set_skip_tier_free_version();
                disk.delete_version(bucket, object, remove, false, DeleteOptions::default())
                    .await
                    .expect("remove second version from one fixture disk");
            }
            for (index, disk) in disks.iter().enumerate() {
                let disk = disk.as_ref().expect("all fixture disks are online");
                disk.read_version("", bucket, object, &v1.to_string(), &ReadOptions::default())
                    .await
                    .expect("first version remains on every disk");
                let second = disk
                    .read_version("", bucket, object, &v2.to_string(), &ReadOptions::default())
                    .await;
                if index < 3 {
                    let info = second.expect("second version remains on three disks");
                    assert_eq!((info.erasure.data_blocks, info.erasure.parity_blocks), (4, 2));
                } else {
                    let error = second.expect_err("second version must be absent from this disk");
                    assert!(DiskError::is_err_version_not_found(&error), "unexpected disk error: {error:?}");
                }
            }

            let manager = Arc::new(HealManager::new(
                Arc::new(ECStoreHealStorage::new(env.ecstore.clone())),
                Some(HealConfig {
                    enable_auto_heal: false,
                    low_priority_merge_enable: true,
                    mainline_throttle_enable: false,
                    queue_size: 16,
                    ..Default::default()
                }),
            ));
            // No manager scheduler runs: accepted requests remain in the real queue.
            let (mut commands, mut receipts) = init_heal_channels().expect("initialize process-local heal channels");
            let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel();
            let (receipt_tx, receipt_rx) = tokio::sync::mpsc::unbounded_channel();
            let processor_manager = manager.clone();
            let processor = tokio::spawn(async move {
                HealChannelProcessor::new(processor_manager)
                    .start_with_receipts(command_rx, receipt_rx)
                    .await
            });
            let mut responses = subscribe_heal_responses();

            // The dangling-latest read uses submit_read_repair_heal, whose MRF intent is None.
            let tagged = set
                .put_object_tags(bucket, object, "stage=committed", &ObjectOptions::default())
                .await
                .expect("latest tagging skips the dangling second version");
            assert_eq!(tagged.version_id, Some(v1));

            // Read-repair submits asynchronously, so wait for its actual request and admission.
            let command = timeout(DEADLINE, commands.recv())
                .await
                .expect("read-repair request timed out")
                .expect("legacy heal command channel closed");
            let read_repair_id = match &command {
                HealChannelCommand::Start { request, .. } => {
                    assert_eq!(request.source, HealRequestSource::ReadRepair);
                    assert_eq!(request.bucket, bucket);
                    assert_eq!(request.object_prefix.as_deref(), Some(object));
                    assert_eq!(request.object_version_id.as_deref(), Some(v2.to_string().as_str()));
                    assert_eq!(request.pool_index, Some(set.pool_index));
                    assert_eq!(request.set_index, Some(set.set_index));
                    request.id.clone()
                }
                _ => panic!("expected read-repair start command"),
            };
            command_tx.send(command).expect("forward original read-repair command once");
            accepted_response(&mut responses, &read_repair_id).await;

            // This isolated process has made no scanner admission before the disk read.
            let initial = scanner_heal_observation_status();
            assert_eq!(initial.state, "not_started");
            assert_eq!(initial.retained_pending_tasks, None);
            assert_eq!(initial.retained_pending_candidates, None);

            let scan_disk = disks[0].as_ref().expect("first fixture disk").clone();
            let disk_path = scan_disk.path();
            let metadata_path = disk_path.join(bucket).join(object).join(STORAGE_FORMAT_FILE);
            let (_, scanner_path) = path2_bucket_object_with_base_path(
                disk_path.to_str().expect("fixture disk path is UTF-8"),
                metadata_path.to_str().expect("fixture metadata path is UTF-8"),
            );
            let scanner = ScannerItem {
                path: scanner_path,
                bucket: bucket.to_owned(),
                prefix: object.to_owned(),
                object_name: STORAGE_FORMAT_FILE.to_owned(),
                file_type: tokio::fs::metadata(&metadata_path)
                    .await
                    .expect("stat surviving real metadata")
                    .file_type(),
                lifecycle: None,
                object_lock: None,
                replication: None,
                heal_enabled: true,
                heal_bitrot: false,
                debug: false,
            };
            let scan = tokio::spawn(async move { scan_disk.get_size_with_tier_names(scanner, &[]).await });
            let mut scanner_ids = HashMap::new();
            for index in 0..2 {
                let command = timeout(DEADLINE, receipts.recv())
                    .await
                    .expect("scanner request timed out")
                    .expect("scanner heal receipt channel closed");
                let request = &command.request;
                assert_eq!(request.source, HealRequestSource::Scanner);
                assert_eq!(request.bucket, bucket);
                assert_eq!(request.object_prefix.as_deref(), Some(object));
                assert_eq!((request.pool_index, request.set_index), (None, None));
                let version = Uuid::parse_str(request.object_version_id.as_deref().expect("scanner version id"))
                    .expect("scanner version id is a UUID");
                assert!(version == v1 || version == v2, "unexpected scanner version {version}");
                let id = request.id.clone();
                assert!(scanner_ids.insert(version, id.clone()).is_none(), "duplicate scanner version");
                receipt_tx.send(command).expect("forward original scanner command once");
                accepted_response(&mut responses, &id).await;
                // Broadcast precedes the oneshot; wait until Scanner consumes each receipt.
                wait_for_scanner_pending(index + 1).await;
            }
            timeout(DEADLINE, scan)
                .await
                .expect("scanner disk read timed out")
                .expect("scanner task failed")
                .expect("scanner read real object metadata");
            assert!(
                matches!(receipts.try_recv(), Err(tokio::sync::mpsc::error::TryRecvError::Empty)),
                "scanner submitted an unexpected extra request"
            );
            assert!(
                matches!(commands.try_recv(), Err(tokio::sync::mpsc::error::TryRecvError::Empty)),
                "an unexpected legacy request was submitted"
            );
            assert_eq!(scanner_ids.len(), 2);
            let scanner_v2_id = scanner_ids.get(&v2).expect("scanner submitted the dangling version");
            assert_ne!(scanner_v2_id, &read_repair_id);
            assert_eq!(manager.get_queue_length().await, 3);
            for id in scanner_ids.values().chain(std::iter::once(&read_repair_id)) {
                assert_eq!(manager.get_task_status(id).await.expect("queued owner status"), HealTaskStatus::Pending);
            }

            drop(command_tx);
            drop(receipt_tx);
            timeout(DEADLINE, processor)
                .await
                .expect("heal channel processor shutdown timed out")
                .expect("heal channel processor task failed")
                .expect("heal channel processor stopped");
        },
    )
    .await;
}
