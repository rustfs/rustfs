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

#![recursion_limit = "256"]

use rustfs_heal::heal::{
    channel::HealChannelProcessor,
    manager::{HealConfig, HealManager},
    storage::ECStoreHealStorage,
};
use rustfs_heal_contracts::heal_channel::{HealAdmissionResult, HealChannelRequest, HealRequestSource, HealScanMode};
use rustfs_test_utils::TestECStoreEnv;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use serial_test::serial;
use sha2::{Digest, Sha256};
use std::{future::Future, path::PathBuf, sync::Arc, time::Duration};
use tokio::io::AsyncReadExt as _;

mod storage_api;
use storage_api::integration::{
    DiskAPI, ObjectIO, ObjectOptions, PutObjReader, ReadOptions, ShardIntegrityWriteMode, WriteCompletion,
};

const BUCKET: &str = "admin-selector";
const TARGET: &str = "options/selector-valid/target.bin";

fn run_async<F: Future<Output = ()>>(test: impl FnOnce() -> F + Send + 'static) {
    std::thread::Builder::new()
        .stack_size(8 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("storage contract runtime")
                .block_on(test());
        })
        .expect("storage contract thread")
        .join()
        .expect("storage contract result");
}

#[derive(Serialize, Deserialize)]
struct ObjectFixture {
    object: String,
    shard: PathBuf,
    digest: Vec<u8>,
}

fn body() -> Vec<u8> {
    vec![0x39; 1024 * 1024 + 37]
}

async fn seed(env: &TestECStoreEnv, count: usize, damage: usize, mode: ShardIntegrityWriteMode) -> Vec<ObjectFixture> {
    env.make_bucket(BUCKET, false).await;
    env.make_bucket("admin-empty", false).await;
    let disk = env.ecstore.pools[0].get_disks(0).disks.read().await[3]
        .clone()
        .expect("fourth H1 disk");
    let mut fixtures = Vec::new();
    for index in 0..count {
        let object = if index == 0 {
            TARGET.to_owned()
        } else {
            format!("controls/healthy-{index:03}.bin")
        };
        env.ecstore
            .put_object(
                BUCKET,
                &object,
                &mut PutObjReader::from_vec(body()),
                &ObjectOptions {
                    write_completion: WriteCompletion::TailDrained,
                    shard_integrity_write_mode: Some(mode),
                    ..Default::default()
                },
            )
            .await
            .expect("commit every seed shard");
        let metadata = disk
            .read_version("", BUCKET, &object, "", &ReadOptions::default())
            .await
            .expect("seed metadata");
        let shard = env.disk_paths[3]
            .join(BUCKET)
            .join(&object)
            .join(metadata.data_dir.expect("external shard").to_string())
            .join("part.1");
        let bytes = tokio::fs::read(&shard).await.expect("seed physical shard");
        fixtures.push(ObjectFixture {
            object,
            shard: shard.clone(),
            digest: Sha256::digest(&bytes).to_vec(),
        });
        if index < damage {
            tokio::fs::remove_file(&shard).await.expect("inject exact missing part");
        }
    }
    fixtures
}

fn manager(env: &TestECStoreEnv) -> Arc<HealManager> {
    Arc::new(HealManager::new(
        Arc::new(ECStoreHealStorage::new(env.ecstore.clone())),
        Some(HealConfig {
            enable_auto_heal: false,
            ..Default::default()
        }),
    ))
}

async fn start(processor: &HealChannelProcessor, token: &str) {
    let receipt = processor
        .execute_start_request(HealChannelRequest {
            id: token.to_owned(),
            disk: Some("pool_0_set_0".to_owned()),
            pool_index: Some(0),
            set_index: Some(0),
            scan_mode: Some(HealScanMode::Deep),
            recursive: Some(true),
            dry_run: Some(false),
            remove_corrupted: Some(false),
            recreate_missing: Some(false),
            update_parity: Some(false),
            no_lock: Some(false),
            source: HealRequestSource::Admin,
            ..Default::default()
        })
        .await
        .expect("admin selector admission");
    assert_eq!(receipt.result, HealAdmissionResult::Accepted);
    assert_eq!(receipt.task_id, token);
}

async fn query(processor: &HealChannelProcessor, token: &str) -> Value {
    let response = processor
        .execute_query_request("/".to_owned(), token.to_owned())
        .await
        .expect("admin status query");
    assert!(response.success, "{:?}", response.error);
    serde_json::from_slice(&response.data.expect("status payload")).expect("status JSON")
}

async fn terminal(processor: &HealChannelProcessor, token: &str) -> Value {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let value = query(processor, token).await;
            if value["summary"] != "running" {
                return value;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("admin terminal deadline")
}

async fn verify_data(env: &TestECStoreEnv, fixtures: &[ObjectFixture]) {
    for fixture in fixtures {
        let bytes = tokio::fs::read(&fixture.shard).await.expect("physical shard restored");
        assert_eq!(Sha256::digest(&bytes).as_slice(), fixture.digest);
        let mut reader = env
            .ecstore
            .get_object_reader(BUCKET, &fixture.object, None, Default::default(), &ObjectOptions::default())
            .await
            .expect("S3 object read");
        let mut actual = Vec::new();
        reader.stream.read_to_end(&mut actual).await.expect("S3 object body");
        assert_eq!(actual, body());
    }
}

async fn mixed_contract(mode: ShardIntegrityWriteMode) {
    let root = tempfile::tempdir().expect("selector fixture");
    let env = TestECStoreEnv::builder().base_dir(root.path()).build().await;
    let fixtures = seed(&env, 12, 1, mode).await;
    let manager = manager(&env);
    manager.start().await.expect("start manager");
    let processor = HealChannelProcessor::new(manager.clone());
    let token = uuid::Uuid::new_v4().to_string();
    start(&processor, &token).await;
    let value = terminal(&processor, &token).await;
    assert_eq!(value["summary"], "finished");
    assert_eq!(
        value["settings"],
        json!({"recursive":true,"dryRun":false,"remove":false,"recreate":false,"scanMode":2,"updateParity":false,"nolock":false,"readRepair":false,"pool":0,"set":0})
    );
    assert_eq!(value["outcome"]["coverage"], "complete");
    assert_eq!(value["outcome"]["execution"]["state"], "completed");
    assert_eq!(value["outcome"]["objectsTruncated"], false);
    assert_eq!(value["outcome"]["counters"]["processed"], 12);
    let protected = mode == ShardIntegrityWriteMode::Protected;
    assert_eq!(
        value["outcome"]["counters"],
        json!({"processed":12,"healed":u64::from(protected),"unchanged":if protected {11} else {0},"skipped":if protected {0} else {12},"failed":0,"unknown":if protected {0} else {12},"attemptFailures":0,"overflowed":false})
    );
    // Legacy progress counts successful processing; only outcome proves repair.
    assert_eq!(value["progress"]["objectsScanned"], 12);
    assert_eq!(value["progress"]["objectsHealed"], 12);
    let receipts = value["outcome"]["objects"].as_array().expect("object receipts");
    assert_eq!(receipts.len(), 12);
    for fixture in &fixtures {
        let receipt = receipts
            .iter()
            .find(|item| item["identity"]["object"] == fixture.object)
            .expect("exact object identity");
        assert_eq!(receipt["identity"]["bucket"], BUCKET);
        assert_eq!(receipt["identity"]["versionId"], uuid::Uuid::nil().to_string());
        assert_eq!(receipt["identity"]["poolIndex"], 0);
        assert_eq!(receipt["identity"]["setIndex"], 0);
        assert!(
            !uuid::Uuid::parse_str(
                receipt["identity"]["bucketIncarnationId"]
                    .as_str()
                    .expect("incarnation string")
            )
            .expect("incarnation UUID")
            .is_nil()
        );
        assert_eq!(
            receipt["disposition"]["state"],
            if !protected {
                "unknown"
            } else if fixture.object == TARGET {
                "repaired"
            } else {
                "verified_healthy"
            }
        );
    }
    verify_data(&env, &fixtures).await;
    manager.stop().await.expect("stop manager");
    drop(processor);
    drop(manager);
    let restarted = self::manager(&env);
    restarted.start().await.expect("restart manager");
    let restored = query(&HealChannelProcessor::new(restarted.clone()), &token).await;
    assert_eq!(restored, value, "retain the complete wire response for the same token");
    assert_eq!(restarted.get_queue_length().await, 0);
    restarted.stop().await.expect("stop restarted manager");
}

#[test]
#[serial]
fn admin_selector_distinguishes_one_repair_from_eleven_healthy_objects() {
    run_async(|| mixed_contract(ShardIntegrityWriteMode::Protected));
}

#[test]
#[serial]
fn admin_selector_legacy_repairs_keep_twelve_unknown_outcomes() {
    run_async(|| mixed_contract(ShardIntegrityWriteMode::Legacy));
}

#[cfg(unix)]
#[test]
#[serial]
fn admin_selector_survives_sigkill_with_original_scope_and_receipts() {
    use std::{
        process::{Child, Command, Stdio},
        time::Instant,
    };
    struct ChildGuard(Child);
    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    if let Ok(root) = std::env::var("RUSTFS_ADMIN_HEAL_CRASH_FIXTURE") {
        let replay = std::env::var("RUSTFS_ADMIN_HEAL_CRASH_REPLAY").is_ok();
        run_async(move || crash_child(PathBuf::from(root), replay));
        return;
    }
    let root = tempfile::tempdir().expect("process crash fixture");
    let spawn = |replay: bool| {
        let log = std::fs::File::create(root.path().join(if replay { "replay.log" } else { "prepare.log" })).expect("child log");
        let mut command = Command::new(std::env::current_exe().expect("integration binary"));
        command
            .args([
                "--exact",
                "admin_selector_survives_sigkill_with_original_scope_and_receipts",
                "--nocapture",
            ])
            .env("RUSTFS_ADMIN_HEAL_CRASH_FIXTURE", root.path())
            .env_remove("RUSTFS_ADMIN_HEAL_CRASH_REPLAY")
            .stdout(Stdio::from(log.try_clone().expect("child stdout")))
            .stderr(Stdio::from(log));
        if replay {
            command.env("RUSTFS_ADMIN_HEAL_CRASH_REPLAY", "1");
        }
        ChildGuard(command.spawn().expect("spawn heal process"))
    };
    let mut first = spawn(false);
    let deadline = Instant::now() + Duration::from_secs(60);
    while !root.path().join("acknowledged.json").exists() {
        assert!(
            first.0.try_wait().expect("prepare child status").is_none(),
            "{}",
            std::fs::read_to_string(root.path().join("prepare.log")).expect("prepare log")
        );
        assert!(Instant::now() < deadline, "child never acknowledged partial work");
        std::thread::sleep(Duration::from_millis(5));
    }
    first.0.kill().expect("SIGKILL the interrupted executor");
    use std::os::unix::process::ExitStatusExt as _;
    assert_eq!(first.0.wait().expect("killed process exit").signal(), Some(9));
    drop(first);
    let mut replay = spawn(true);
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(status) = replay.0.try_wait().expect("replay child status") {
            assert!(
                status.success(),
                "{}",
                std::fs::read_to_string(root.path().join("replay.log")).expect("replay log")
            );
            break;
        }
        assert!(Instant::now() < deadline, "replay child did not complete");
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[cfg(unix)]
async fn crash_child(root: PathBuf, replay: bool) {
    let env = TestECStoreEnv::builder().base_dir(root.join("disks")).build().await;
    let token = "dff22383-67ee-4d8b-ae8a-dc6d8276c974";
    if !replay {
        let fixtures = seed(&env, 64, 64, ShardIntegrityWriteMode::Protected).await;
        std::fs::write(root.join("objects.json"), serde_json::to_vec(&fixtures).expect("fixture manifest"))
            .expect("persist fixture manifest");
    } else {
        env.make_bucket("created-after-crash", false).await;
        env.put_object_bytes("created-after-crash", "outside-original-scope", body())
            .await;
    }
    let manager = manager(&env);
    manager.start().await.expect("start durable owner");
    let processor = HealChannelProcessor::new(manager.clone());
    if !replay {
        start(&processor, token).await;
        loop {
            let value = query(&processor, token).await;
            assert_eq!(value["summary"], "running", "crash must precede terminal publication");
            if let Some(processed) = value["outcome"]["counters"]["processed"].as_u64()
                && processed > 0
            {
                assert!(processed < 64, "crash must interrupt a partial traversal");
                // The parent may kill us as soon as the final path exists.
                let mut acknowledged = tempfile::NamedTempFile::new_in(&root).expect("acknowledgement staging file");
                serde_json::to_writer(acknowledged.as_file_mut(), &value["outcome"]).expect("write acknowledged outcome");
                acknowledged.as_file().sync_all().expect("sync acknowledged outcome");
                acknowledged
                    .persist(root.join("acknowledged.json"))
                    .expect("signal committed work");
                // Freeze the current-thread executor at an acknowledged boundary
                // until the parent kills the process without running destructors.
                loop {
                    std::thread::park();
                }
            }
            tokio::task::yield_now().await;
        }
    }
    let value = terminal(&processor, token).await;
    assert_eq!(value["summary"], "finished");
    assert_eq!(value["outcome"]["coverage"], "complete");
    assert_eq!(value["outcome"]["counters"]["processed"], 64);
    let counters = &value["outcome"]["counters"];
    assert_eq!(
        counters["healed"].as_u64().expect("healed count") + counters["unchanged"].as_u64().expect("healthy count"),
        64
    );
    let receipts = value["outcome"]["objects"].as_array().expect("resumed receipts");
    assert_eq!(receipts.len(), 64);
    assert!(receipts.iter().all(|item| item["identity"]["bucket"] == BUCKET));
    let acknowledged: Value = serde_json::from_slice(&std::fs::read(root.join("acknowledged.json")).expect("acknowledged bytes"))
        .expect("acknowledged JSON");
    for receipt in acknowledged["objects"].as_array().expect("acknowledged receipts") {
        assert!(receipts.contains(receipt), "acknowledged repair must not be recounted as healthy");
    }
    let fixtures: Vec<ObjectFixture> =
        serde_json::from_slice(&std::fs::read(root.join("objects.json")).expect("fixture bytes")).expect("fixture JSON");
    for fixture in &fixtures {
        let receipt = receipts
            .iter()
            .find(|item| item["identity"]["object"] == fixture.object)
            .expect("every original object has one resumed responsibility");
        assert_eq!(receipt["identity"]["versionId"], uuid::Uuid::nil().to_string());
        assert_eq!(receipt["identity"]["poolIndex"], 0);
        assert_eq!(receipt["identity"]["setIndex"], 0);
        assert_eq!(
            receipt["identity"]["bucketIncarnationId"],
            acknowledged["objects"][0]["identity"]["bucketIncarnationId"]
        );
    }
    verify_data(&env, &fixtures).await;
    manager.stop().await.expect("stop recovered manager");
}
