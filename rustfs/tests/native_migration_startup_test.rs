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

use reqwest::StatusCode;
use rustfs::embedded::{RustFSServerBuilder, find_available_port};
use rustfs_ecstore::api::config::com::{delete_config, read_config};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;

mod common;

const TEST_NAME: &str = "native_migration_failure_blocks_server_startup_and_repair_preserves_records";
const STAGE_ENV: &str = "RUSTFS_NATIVE_MIGRATION_TEST_STAGE";
const ROOT_ENV: &str = "RUSTFS_NATIVE_MIGRATION_TEST_ROOT";
const ADDRESS_ENV: &str = "RUSTFS_NATIVE_MIGRATION_TEST_ADDRESS";
const FAILURE_ENV: &str = "RUSTFS_NATIVE_MIGRATION_TEST_FAILURE";
const STOP_ENV: &str = "RUSTFS_NATIVE_MIGRATION_TEST_STOP";
const ACCESS_KEY: &str = "native-migration-root";
const SECRET_KEY: &str = "native-migration-root-secret";
const LEGACY_BUCKET: &str = ".minio.sys";
const TARGET_BUCKET: &str = ".rustfs.sys";
const BUCKET_METADATA: &str = "buckets/interop/.metadata.bin";
const IAM_RECORD: &str = "config/iam/groups/migration-group/members.json";
const IAM_FORMAT: &str = "config/iam/format.json";
const EXISTING_FORMAT: &[u8] = br#"{"version":1}"#;
const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Debug)]
enum StartupMode {
    Server,
    Embedded,
}

fn volumes(root: &Path) -> Vec<PathBuf> {
    (1..=4).map(|index| root.join(format!("disk{index}"))).collect()
}

fn minio_bucket_metadata() -> Vec<u8> {
    let hex: String = include_str!("../../crates/ecstore/tests/fixtures/minio/bucket_metadata.blob.hex")
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect();
    hex.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            u8::from_str_radix(std::str::from_utf8(pair).expect("fixture hex is UTF-8"), 16).expect("valid MinIO fixture hex")
        })
        .collect()
}

async fn prepare_or_verify_fixture(root: &Path, seed: bool) {
    let env = rustfs_test_utils::TestECStoreEnv::builder()
        .base_dir(root)
        .disk_count(4)
        .build()
        .await;
    if seed {
        env.make_bucket("interop", false).await;
        env.make_bucket(LEGACY_BUCKET, false).await;
        env.put_object_bytes(LEGACY_BUCKET, BUCKET_METADATA, minio_bucket_metadata())
            .await;
        env.put_object_bytes(
            LEGACY_BUCKET,
            IAM_RECORD,
            br#"{"version":1,"status":"enabled","members":[],"updatedAt":"2026-09-10T00:00:00Z"}"#.to_vec(),
        )
        .await;
        env.put_object_bytes(TARGET_BUCKET, IAM_FORMAT, EXISTING_FORMAT.to_vec())
            .await;
        // A completed record must be skipped before reading even a broken old copy.
        env.put_object_bytes(LEGACY_BUCKET, IAM_FORMAT, b"do not overwrite the existing target".to_vec())
            .await;
        delete_config(env.ecstore.clone(), BUCKET_METADATA)
            .await
            .expect("leave bucket metadata pending migration");
    } else {
        assert_eq!(
            read_config(env.ecstore.clone(), BUCKET_METADATA)
                .await
                .expect("migrated bucket metadata"),
            minio_bucket_metadata(),
            "migration must preserve the MinIO bucket settings"
        );
        let group: serde_json::Value = serde_json::from_slice(
            &read_config(env.ecstore.clone(), IAM_RECORD)
                .await
                .expect("migrated IAM group"),
        )
        .expect("valid migrated IAM JSON");
        assert_eq!(group["status"], "enabled");
        assert_eq!(group["members"], serde_json::json!([]));
    }
    assert_eq!(
        read_config(env.ecstore.clone(), IAM_FORMAT)
            .await
            .expect("existing IAM format"),
        EXISTING_FORMAT,
        "retry must not overwrite records already migrated"
    );
}

async fn run_embedded_child(root: &Path) {
    let address = std::env::var(ADDRESS_ENV).expect("embedded child address");
    let result = RustFSServerBuilder::new()
        .address(address)
        .access_key(ACCESS_KEY)
        .secret_key(SECRET_KEY)
        .volumes(volumes(root).iter().map(|path| path.to_string_lossy().into_owned()).collect())
        .build()
        .await;
    match result {
        Ok(server) => {
            let stop = PathBuf::from(std::env::var_os(STOP_ENV).expect("embedded stop path"));
            while !stop.exists() {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            server.shutdown().await;
        }
        Err(error) => {
            fs::write(std::env::var_os(FAILURE_ENV).expect("embedded failure path"), error.to_string())
                .expect("record the actual embedded startup error");
        }
    }
}

fn child_command(root: &Path, stage: &str, log: &Path) -> Command {
    let mut command = Command::new(std::env::current_exe().expect("integration test executable"));
    command
        .args(["--exact", TEST_NAME, "--nocapture"])
        .env(STAGE_ENV, stage)
        .env(ROOT_ENV, root);
    configure_process(&mut command, log);
    command
}

fn configure_process(command: &mut Command, log: &Path) {
    let output = fs::File::create(log).expect("create isolated process log");
    command
        // These disposable erasure volumes intentionally share the test runner's disk.
        .env("RUSTFS_UNSAFE_BYPASS_DISK_CHECK", "true")
        .env("RUSTFS_CONSOLE_ENABLE", "false")
        .env("NO_PROXY", "localhost,127.0.0.1,::1")
        .env("no_proxy", "localhost,127.0.0.1,::1")
        .env("RUST_LOG", "warn")
        .stdin(Stdio::null())
        .stdout(Stdio::from(output.try_clone().expect("clone process log")))
        .stderr(Stdio::from(output))
        .kill_on_drop(true);
}

async fn fixture_process(root: &Path, stage: &str) {
    let log = root.join(format!("{stage}.log"));
    let status = tokio::time::timeout(STARTUP_TIMEOUT, child_command(root, stage, &log).status())
        .await
        .expect("fixture process must finish")
        .expect("run fixture process");
    assert!(status.success(), "{stage} failed: {}", fs::read_to_string(log).expect("fixture log"));
}

async fn check_startup(root: &Path, mode: StartupMode, failure_record: Option<&str>, label: &str) {
    let ready = failure_record.is_none();
    let address = format!("127.0.0.1:{}", find_available_port().expect("free startup probe port"));
    let log = root.join(format!("{label}.log"));
    let failure = root.join(format!("{label}.failure"));
    let stop = root.join(format!("{label}.stop"));
    let mut command = match mode {
        StartupMode::Server => {
            let mut command = Command::new(env!("CARGO_BIN_EXE_rustfs"));
            command
                .args(["--address", &address, "--access-key", ACCESS_KEY, "--secret-key", SECRET_KEY])
                .args(volumes(root));
            configure_process(&mut command, &log);
            command
        }
        StartupMode::Embedded => {
            let mut command = child_command(root, "embedded", &log);
            command
                .env(ADDRESS_ENV, &address)
                .env(FAILURE_ENV, &failure)
                .env(STOP_ENV, &stop);
            command
        }
    };
    let mut child = command.spawn().expect("start isolated server process");
    let http = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_millis(500))
        .build()
        .expect("local readiness client");
    let result = tokio::time::timeout(STARTUP_TIMEOUT, async {
        loop {
            if let Ok(response) = http.get(format!("http://{address}/health/ready")).send().await
                && response.status() == StatusCode::OK
            {
                assert!(ready, "{mode:?} published Ready after a migration I/O failure");
                return;
            }
            if let Some(status) = child.try_wait().expect("poll server process") {
                let details = fs::read_to_string(&log).expect("startup log");
                assert!(!ready, "{mode:?} exited before Ready ({status}): {details}");
                let record = failure_record.expect("failed startup has an obstructed record");
                match mode {
                    StartupMode::Server => {
                        assert_eq!(status.code(), Some(1), "startup must fail: {details}");
                        assert_migration_io_error(&details, record);
                    }
                    StartupMode::Embedded => {
                        assert!(status.success(), "embedded test process failed unexpectedly: {details}");
                        let error = fs::read_to_string(&failure).expect("embedded startup returned an error");
                        assert_migration_io_error(&error, record);
                    }
                }
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "{mode:?} did not reach the expected startup outcome: {}",
        fs::read_to_string(&log).expect("startup diagnostics")
    );
    if ready {
        match mode {
            StartupMode::Embedded => {
                fs::write(stop, b"stop").expect("request embedded shutdown");
                assert!(
                    tokio::time::timeout(STARTUP_TIMEOUT, child.wait())
                        .await
                        .expect("embedded shutdown completes")
                        .expect("wait for embedded shutdown")
                        .success()
                );
            }
            StartupMode::Server => child.kill().await.expect("stop the isolated server"),
        }
    }
}

fn assert_migration_io_error(error: &str, record: &str) {
    let lower = error.to_ascii_lowercase();
    assert!(
        (lower.contains("access denied")
            || lower.contains("access is denied")
            || lower.contains("not a directory")
            || lower.contains("not regular"))
            && error.contains(&format!("{TARGET_BUCKET}/{record}")),
        "startup must fail because of the obstructed metadata record, not an unrelated initialization error: {error}"
    );
}

async fn run_startup_cases(mode: StartupMode) {
    let ordinary = tempfile::TempDir::with_prefix("rustfs-no-legacy-").expect("ordinary store");
    for volume in volumes(ordinary.path()) {
        fs::create_dir_all(volume).expect("ordinary volume");
    }
    check_startup(ordinary.path(), mode, None, "ordinary").await;

    let control = tempfile::TempDir::with_prefix("rustfs-migration-control-").expect("control fixture");
    fixture_process(control.path(), "seed").await;
    check_startup(control.path(), mode, None, "control").await;
    fixture_process(control.path(), "verify").await;

    for record in [BUCKET_METADATA, IAM_RECORD] {
        let target = tempfile::TempDir::with_prefix("rustfs-migration-failure-").expect("disposable migration target");
        fixture_process(target.path(), "seed").await;
        let blockers: Vec<_> = volumes(target.path())
            .iter()
            .map(|volume| volume.join(TARGET_BUCKET).join(record))
            .collect();
        for blocker in &blockers {
            fs::create_dir_all(blocker.parent().expect("record parent")).expect("create target parent");
            assert!(!blocker.exists(), "the record must still need migration");
            // A non-directory target causes real filesystem I/O errors even when tests run as root.
            fs::write(blocker, b"blocked migration target").expect("block only the destination record");
        }
        check_startup(target.path(), mode, Some(record), "blocked").await;
        for blocker in blockers {
            fs::remove_file(blocker).expect("repair the same partially migrated target");
        }
        check_startup(target.path(), mode, None, "repaired").await;
        fixture_process(target.path(), "verify").await;
    }
}

#[test]
fn native_migration_failure_blocks_server_startup_and_repair_preserves_records() {
    // Cold processes keep failed initialization and cached metadata out of subsequent restart attempts.
    common::run_embedded_test(|| async {
        match std::env::var(STAGE_ENV).ok().as_deref() {
            Some("seed") => {
                prepare_or_verify_fixture(&PathBuf::from(std::env::var_os(ROOT_ENV).expect("fixture root")), true).await
            }
            Some("verify") => {
                prepare_or_verify_fixture(&PathBuf::from(std::env::var_os(ROOT_ENV).expect("fixture root")), false).await
            }
            Some("embedded") => run_embedded_child(&PathBuf::from(std::env::var_os(ROOT_ENV).expect("fixture root"))).await,
            None => run_startup_cases(StartupMode::Server).await,
            Some(stage) => panic!("unknown native migration test stage: {stage}"),
        }
    });
}

#[test]
fn native_migration_failure_blocks_embedded_startup_and_repair_preserves_records() {
    common::run_embedded_test(|| run_startup_cases(StartupMode::Embedded));
}
