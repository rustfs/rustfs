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

//! Erasure-set healing regression tests.

#[cfg(test)]
mod tests {
    use crate::chaos::{VersionShardCensus, census_object_version_on_disk, sha256_hex, signed_admin_post};
    use crate::common::{
        FAST_DATA_USAGE_SCANNER_ENV, RustFSTestClusterEnvironment, RustFSTestEnvironment, admin_request, init_logging,
        rustfs_binary_path,
    };
    use crate::storage_api::RUSTFS_META_BUCKET;
    use aws_sdk_s3::primitives::ByteStream;
    use http::Method;
    use sha2::{Digest, Sha256};
    use std::collections::HashSet;
    use std::error::Error;
    use std::io::{Read, Write};
    use std::net::SocketAddr;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use tokio::net::TcpStream;
    use tokio::time::{Duration, Instant, sleep, timeout};
    use tracing::info;
    #[cfg(target_os = "linux")]
    use tracing::warn;

    const POOL_METADATA_OBJECT: &str = "pool.bin";

    #[derive(serde::Deserialize)]
    struct EvidenceBuild {
        sha256: String,
    }

    #[derive(serde::Deserialize)]
    struct RestartEvidenceRun {
        schema: u32,
        run_id: String,
        source_revision: String,
        test_build: serde_json::Value,
        binary: EvidenceBuild,
        test_binary: EvidenceBuild,
    }

    #[derive(Clone, Copy)]
    struct ScannerHealEvidenceCase {
        id: &'static str,
        oracle: &'static str,
    }

    const BACKGROUND_TARGET_RESTART_EVIDENCE: ScannerHealEvidenceCase = ScannerHealEvidenceCase {
        id: "background-target-restart",
        oracle: "background-target-restart.json",
    };

    struct RestartEvidenceContext {
        directory: PathBuf,
        run: RestartEvidenceRun,
        case: ScannerHealEvidenceCase,
    }

    fn file_sha256(path: &Path) -> Result<String, Box<dyn Error + Send + Sync>> {
        let mut file = std::fs::File::open(path)?;
        let mut digest = Sha256::new();
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            digest.update(&buffer[..read]);
        }
        Ok(digest.finalize().iter().map(|byte| format!("{byte:02x}")).collect())
    }

    fn restart_evidence_run(
        binary: &Path,
        case: ScannerHealEvidenceCase,
    ) -> Result<Option<RestartEvidenceContext>, Box<dyn Error + Send + Sync>> {
        let Some(directory) = std::env::var_os("RUSTFS_SCANNER_HEAL_RUN_DIR") else {
            return Ok(None);
        };
        if case.id.is_empty()
            || case.oracle.is_empty()
            || !case.oracle.ends_with(".json")
            || case.oracle.contains('/')
            || case.oracle.contains('\\')
            || case.oracle.contains("..")
        {
            return Err("invalid scanner/heal evidence case".into());
        }
        let directory = PathBuf::from(directory);
        let receipt = directory.join("run.json");
        if receipt.metadata()?.len() > 1024 * 1024 {
            return Err("oversized scanner/heal execution receipt".into());
        }
        let run: RestartEvidenceRun = serde_json::from_slice(&std::fs::read(receipt)?)?;
        if run.schema != 1 || run.run_id.len() != 32 || run.source_revision.len() != 40 {
            return Err("invalid scanner/heal execution identity".into());
        }
        let built = compiled_test_identity();
        for key in ["source_revision", "dirty", "lock_blob", "features"] {
            assert_eq!(built[key], run.test_build[key], "compiled test identity differs for {key}");
        }
        assert_eq!(file_sha256(binary)?, run.binary.sha256, "server binary must match the run receipt");
        assert_eq!(
            file_sha256(&std::env::current_exe()?)?,
            run.test_binary.sha256,
            "test executable must match the run receipt"
        );
        if directory.join(case.oracle).exists() {
            return Err("scanner/heal oracle already exists; create a new execution receipt".into());
        }
        Ok(Some(RestartEvidenceContext { directory, run, case }))
    }

    fn compiled_test_identity() -> serde_json::Value {
        serde_json::json!({
            "source_revision": env!("RUSTFS_E2E_BUILD_COMMIT"),
            "dirty": env!("RUSTFS_E2E_BUILD_DIRTY") != "false",
            "lock_blob": env!("RUSTFS_E2E_BUILD_LOCK"),
            "features": env!("RUSTFS_E2E_BUILD_FEATURES"),
            "target": env!("RUSTFS_E2E_BUILD_TARGET"),
            "profile": env!("RUSTFS_E2E_BUILD_PROFILE"),
            "rustflags_hex": env!("RUSTFS_E2E_BUILD_RUSTFLAGS_HEX"),
        })
    }

    struct TcpPortBlackhole {
        port: u16,
        comment: String,
        use_sudo: bool,
        active: bool,
    }

    impl TcpPortBlackhole {
        /// Environment flag that turns an unusable fault-injection host into a
        /// hard failure instead of a logged skip. Lanes that provision
        /// `CAP_NET_ADMIN` set it so a broken runner cannot pass silently.
        #[cfg(target_os = "linux")]
        const REQUIRE_ENV: &str = "RUSTFS_E2E_REQUIRE_NET_FAULT_INJECTION";

        /// Probe whether this host can manipulate the OUTPUT chain at all.
        ///
        /// Returns `Ok(Some(reason))` when `iptables` is missing or lacks
        /// `CAP_NET_ADMIN` (the nf_tables backend reports "Permission denied"
        /// even under `sudo` inside an unprivileged container) and the lane did
        /// not demand fault injection; returns an error when the lane demands
        /// it; returns `Ok(None)` when the blackhole can be installed.
        #[cfg(target_os = "linux")]
        fn unavailable_reason() -> Result<Option<String>, Box<dyn Error + Send + Sync>> {
            let id = Command::new("id").arg("-u").output()?;
            if !id.status.success() {
                return Err(format!("failed to determine the test process uid: {}", String::from_utf8_lossy(&id.stderr)).into());
            }
            let use_sudo = String::from_utf8_lossy(&id.stdout).trim() != "0";
            let mut command = if use_sudo {
                let mut command = Command::new("sudo");
                command.args(["-n", "iptables"]);
                command
            } else {
                Command::new("iptables")
            };
            let probe = command.args(["-w", "5", "-S", "OUTPUT"]).output();
            let reason = match probe {
                Ok(output) if output.status.success() => return Ok(None),
                Ok(output) => format!(
                    "iptables cannot read the OUTPUT chain (status {}): {}",
                    output.status,
                    String::from_utf8_lossy(&output.stderr).trim()
                ),
                Err(err) => format!("iptables is not runnable: {err}"),
            };
            if std::env::var_os(Self::REQUIRE_ENV).is_some() {
                return Err(format!("{} is set but network fault injection is unavailable: {reason}", Self::REQUIRE_ENV).into());
            }
            Ok(Some(reason))
        }

        fn install(address: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
            let address = address.parse::<SocketAddr>()?;
            if !address.ip().is_loopback() {
                return Err(format!("refusing to install a test firewall rule for non-loopback address {address}").into());
            }

            let id = Command::new("id").arg("-u").output()?;
            if !id.status.success() {
                return Err(format!("failed to determine the test process uid: {}", String::from_utf8_lossy(&id.stderr)).into());
            }
            let use_sudo = String::from_utf8_lossy(&id.stdout).trim() != "0";
            let mut blackhole = Self {
                port: address.port(),
                comment: format!("rustfs-e2e-{}", uuid::Uuid::new_v4()),
                use_sudo,
                active: false,
            };
            blackhole.run_iptables(true)?;
            blackhole.active = true;
            Ok(blackhole)
        }

        fn restore(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
            if !self.active {
                return Ok(());
            }
            self.run_iptables(false)?;
            self.active = false;
            Ok(())
        }

        fn run_iptables(&self, insert: bool) -> Result<(), Box<dyn Error + Send + Sync>> {
            let mut command = if self.use_sudo {
                let mut command = Command::new("sudo");
                command.args(["-n", "iptables"]);
                command
            } else {
                Command::new("iptables")
            };
            command.args(["-w", "5"]);
            if insert {
                command.args(["-I", "OUTPUT", "1"]);
            } else {
                command.args(["-D", "OUTPUT"]);
            }
            let port = self.port.to_string();
            let output = command
                .args([
                    "-p",
                    "tcp",
                    "-d",
                    "127.0.0.1/32",
                    "--dport",
                    &port,
                    "-m",
                    "comment",
                    "--comment",
                    &self.comment,
                    "-j",
                    "DROP",
                ])
                .output()?;
            if !output.status.success() {
                let action = if insert { "install" } else { "remove" };
                return Err(format!(
                    "failed to {action} endpoint blackhole rule for port {}: stdout={}, stderr={}",
                    self.port,
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                )
                .into());
            }
            Ok(())
        }
    }

    impl Drop for TcpPortBlackhole {
        fn drop(&mut self) {
            if let Err(error) = self.restore() {
                eprintln!("failed to remove {} firewall rule during test cleanup: {error}", self.comment);
            }
        }
    }

    /// Remove a disk directory underneath a running server. Background writers
    /// (scanner, usage cache, heal markers) can recreate entries between the
    /// recursive listing and the final `rmdir`, which surfaces as
    /// `DirectoryNotEmpty` on macOS; retry briefly so the wipe reflects the
    /// operator action rather than a listing race.
    fn wipe_directory_while_server_runs(disk: &Path) -> std::io::Result<()> {
        let mut last_err = None;
        for _ in 0..20 {
            match std::fs::remove_dir_all(disk) {
                Ok(()) => return Ok(()),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(err) if err.kind() == std::io::ErrorKind::DirectoryNotEmpty => {
                    last_err = Some(err);
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
                Err(err) => return Err(err),
            }
        }
        Err(last_err.expect("retry loop only exits without success after recording an error"))
    }

    fn has_file_under(path: &Path) -> bool {
        let Ok(entries) = std::fs::read_dir(path) else {
            return false;
        };

        for entry in entries.filter_map(Result::ok) {
            let path = entry.path();
            if path.is_dir() {
                if has_file_under(&path) {
                    return true;
                }
            } else {
                return true;
            }
        }

        false
    }

    fn object_metadata_exists_on_disk(disk: &Path, bucket: &str, key: &str) -> bool {
        disk.join(bucket).join(key).join("xl.meta").is_file()
    }

    // Healing may rewrite non-identity bookkeeping in xl.meta. The census
    // therefore compares the canonical selected metadata fields plus every
    // physical shard, while the payload seed makes object mix-ups observable.
    #[derive(Debug)]
    struct PhysicalObjectManifest {
        key: String,
        payload_seed: u8,
        shard_census: VersionShardCensus,
    }

    fn deterministic_object_body(len: usize, seed: u8) -> Vec<u8> {
        let mut value = seed;
        std::iter::repeat_with(|| {
            value = value.wrapping_mul(31).wrapping_add(17);
            value
        })
        .take(len)
        .collect()
    }

    fn matching_manifest_count(
        disk: &Path,
        bucket: &str,
        expected_manifests: &[PhysicalObjectManifest],
    ) -> Result<usize, Box<dyn Error + Send + Sync>> {
        let mut matching = 0;
        for expected in expected_manifests {
            let actual = census_object_version_on_disk(disk, bucket, &expected.key, None)?;
            if actual.matches_manifest(&expected.shard_census) {
                matching += 1;
            }
        }
        Ok(matching)
    }

    fn metadata_count(disk: &Path, bucket: &str, expected_manifests: &[PhysicalObjectManifest]) -> usize {
        expected_manifests
            .iter()
            .filter(|expected| object_metadata_exists_on_disk(disk, bucket, &expected.key))
            .count()
    }

    async fn assert_all_nodes_list_exact_keys(
        clients: &[aws_sdk_s3::Client],
        bucket: &str,
        expected_keys: &HashSet<String>,
    ) -> Result<Vec<Vec<String>>, Box<dyn Error + Send + Sync>> {
        const PAGE_SIZE: i32 = 10;
        let mut node_listings = Vec::with_capacity(clients.len());
        for (node_index, client) in clients.iter().enumerate() {
            let mut listed_keys = Vec::new();
            let mut continuation_token = None;
            let max_pages = expected_keys.len().div_ceil(PAGE_SIZE as usize) + 1;
            let mut page_count = 0;
            loop {
                page_count += 1;
                if page_count > max_pages {
                    return Err(format!("node {node_index} listing exceeded the bounded {max_pages}-page budget").into());
                }
                let response = timeout(
                    Duration::from_secs(15),
                    client
                        .list_objects_v2()
                        .bucket(bucket)
                        .max_keys(PAGE_SIZE)
                        .set_continuation_token(continuation_token.clone())
                        .send(),
                )
                .await??;
                listed_keys.extend(
                    response
                        .contents()
                        .iter()
                        .filter_map(|object| object.key().map(str::to_owned)),
                );
                if !response.is_truncated().unwrap_or(false) {
                    break;
                }
                let next_token = response
                    .next_continuation_token()
                    .filter(|token| Some(*token) != continuation_token.as_deref())
                    .ok_or_else(|| format!("node {node_index} returned a truncated listing without a new continuation token"))?;
                continuation_token = Some(next_token.to_owned());
            }

            let listed_key_set = listed_keys.iter().cloned().collect::<HashSet<_>>();
            assert_eq!(
                listed_keys.len(),
                listed_key_set.len(),
                "node {node_index} returned duplicate keys after recovery: {listed_keys:?}"
            );
            assert_eq!(
                &listed_key_set, expected_keys,
                "node {node_index} did not expose the complete recovered namespace"
            );
            listed_keys.sort();
            node_listings.push(listed_keys);
        }
        Ok(node_listings)
    }

    fn heal_task_status_diagnostic(body: &str) -> String {
        let Ok(status) = serde_json::from_str::<serde_json::Value>(body) else {
            return body.to_string();
        };
        let items = status["items"].as_array();
        let mut unresolved_states = HashSet::new();
        for item in items.into_iter().flatten() {
            for drive in item["after"]["drives"].as_array().into_iter().flatten() {
                if let Some(state) = drive["state"].as_str()
                    && state != "ok"
                {
                    unresolved_states.insert(state.to_string());
                }
            }
        }
        let mut unresolved_states = unresolved_states.into_iter().collect::<Vec<_>>();
        unresolved_states.sort();
        format!(
            "summary={:?}, detail={:?}, item_count={}, unresolved_drive_states={unresolved_states:?}",
            status["summary"].as_str(),
            status["detail"].as_str(),
            items.map_or(0, Vec::len)
        )
    }

    fn cluster_heal_is_idle(status: &serde_json::Value) -> bool {
        let operations = &status["healOperations"];
        status["clusterStatusComplete"] == serde_json::Value::Bool(true)
            && status["state"].as_str() == Some("idle")
            && operations["queueLength"].as_u64() == Some(0)
            && operations["activeTasks"].as_u64() == Some(0)
            && operations["retryingTasks"].as_u64() == Some(0)
    }

    // Queued low-priority repairs cannot execute while the single admin slot is
    // occupied; ownership is determined by active and retrying tasks only.
    fn only_admin_heal_is_active(status: &serde_json::Value) -> bool {
        let operations = &status["healOperations"];
        status["clusterStatusComplete"] == serde_json::Value::Bool(true)
            && status["state"].as_str() == Some("active")
            && operations["activeTasks"].as_u64() == Some(1)
            && operations["retryingTasks"].as_u64() == Some(0)
            && operations["activeBySource"]["admin"].as_u64() == Some(1)
    }

    async fn replacement_recovery_status(
        cluster: &RustFSTestClusterEnvironment,
    ) -> Result<serde_json::Value, Box<dyn Error + Send + Sync>> {
        let (status, body) = admin_request(
            &cluster.nodes[0].url,
            Method::GET,
            "/rustfs/admin/v4/heal/replacement-recovery",
            None,
            &cluster.access_key,
            &cluster.secret_key,
        )
        .await?;
        if !status.is_success() {
            return Err(format!("replacement recovery status failed: {status} {body}").into());
        }
        serde_json::from_str(&body).map_err(|err| format!("replacement recovery status is not JSON ({err}): {body}").into())
    }

    async fn assert_object_body(env: &RustFSTestEnvironment, bucket: &str, key: &str, expected: &[u8]) {
        let client = env.create_s3_client();
        let response = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .expect("GET should succeed during/after heal");
        let body = response.body.collect().await.expect("GET body should collect").into_bytes();
        assert_eq!(body.as_ref(), expected, "object body changed for {key}");
    }

    #[tokio::test]
    async fn test_auto_heal_rebuilds_runtime_wiped_disk_without_restart() {
        init_logging();
        info!("Issue #1533: auto heal should rebuild a runtime-wiped disk in a 4-disk single-node erasure set without restart");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        let root = PathBuf::from(env.temp_dir.clone());
        let disk0 = root.join("disk0");
        let disk1 = root.join("disk1");
        let disk2 = root.join("disk2");
        let disk3 = root.join("disk3");
        for disk in [&disk0, &disk1, &disk2, &disk3] {
            std::fs::create_dir_all(disk).expect("disk directory should be created");
        }

        env.temp_dir = disk3.to_string_lossy().to_string();
        let disk0_arg = disk0.to_string_lossy().to_string();
        let disk1_arg = disk1.to_string_lossy().to_string();
        let disk2_arg = disk2.to_string_lossy().to_string();
        env.start_rustfs_server_with_env(
            vec![disk0_arg.as_str(), disk1_arg.as_str(), disk2_arg.as_str()],
            &[
                ("RUSTFS_UNSAFE_BYPASS_DISK_CHECK", "true"),
                ("RUSTFS_HEAL_INTERVAL_SECS", "10"),
            ],
        )
        .await
        .expect("Failed to start 4-disk RustFS");
        env.temp_dir = root.to_string_lossy().to_string();

        let client = env.create_s3_client();
        let bucket = "heal-runtime-wiped-disk";
        let heal_timeout_secs = std::env::var("RUSTFS_AUTO_HEAL_RUNTIME_WIPE_TIMEOUT_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(45);

        let objects: Vec<(String, Vec<u8>, &'static str)> = vec![
            (
                "runtime/alpha.txt".to_string(),
                b"alpha payload for runtime wipe heal".to_vec(),
                "text/plain; charset=utf-8",
            ),
            ("runtime/beta.bin".to_string(), (0..=127).collect::<Vec<u8>>(), "application/octet-stream"),
            (
                "runtime/dir/emoji-free-name.json".to_string(),
                br#"{"status":"runtime-heal"}"#.to_vec(),
                "application/json",
            ),
            (
                "runtime/dir/gamma.txt".to_string(),
                b"gamma payload for runtime wipe heal".to_vec(),
                "text/plain; charset=utf-8",
            ),
        ];

        client
            .create_bucket()
            .bucket(bucket)
            .send()
            .await
            .expect("bucket create should succeed");
        for (key, body, content_type) in &objects {
            client
                .put_object()
                .bucket(bucket)
                .key(key)
                .content_type(*content_type)
                .body(ByteStream::from(body.clone()))
                .send()
                .await
                .expect("PUT should succeed");
        }

        for (key, body, _) in &objects {
            assert_object_body(&env, bucket, key, body).await;
            assert!(
                object_metadata_exists_on_disk(&disk0, bucket, key),
                "disk0 should contain xl.meta before runtime wipe for {key}"
            );
        }

        wipe_directory_while_server_runs(&disk0).expect("disk0 wipe should succeed while server is running");
        std::fs::create_dir_all(&disk0).expect("disk0 should be recreated empty while server is running");
        assert!(!has_file_under(&disk0), "disk0 must be empty immediately after runtime wipe");

        let mut remaining_rebuild_keys: HashSet<String> = objects.iter().map(|(key, _, _)| key.clone()).collect();
        for _ in 0..heal_timeout_secs {
            for (key, body, _) in &objects {
                assert_object_body(&env, bucket, key, body).await;
            }

            if !remaining_rebuild_keys.is_empty() {
                let mut rebuilt = Vec::new();
                for key in &remaining_rebuild_keys {
                    if object_metadata_exists_on_disk(&disk0, bucket, key) {
                        rebuilt.push(key.clone());
                    }
                }
                for key in rebuilt {
                    let _ = remaining_rebuild_keys.remove(&key);
                }
            }

            if remaining_rebuild_keys.is_empty() {
                assert!(
                    disk0.join(".rustfs.sys").join("format.json").is_file(),
                    "runtime-wiped disk should have format.json restored by auto heal"
                );
                return;
            }

            sleep(Duration::from_secs(1)).await;
        }

        panic!("auto heal did not rebuild all files on the runtime-wiped disk within timeout");
    }

    #[tokio::test]
    async fn test_admin_deep_heal_rebuilds_cleared_disk_in_single_node_erasure_set() {
        init_logging();
        info!("Discussion #2964: admin deep heal should rebuild a wiped disk in a 4-disk single-node erasure set");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        let root = PathBuf::from(env.temp_dir.clone());
        let disk0 = root.join("disk0");
        let disk1 = root.join("disk1");
        let disk2 = root.join("disk2");
        let disk3 = root.join("disk3");
        for disk in [&disk0, &disk1, &disk2, &disk3] {
            std::fs::create_dir_all(disk).expect("disk directory should be created");
        }

        // The test helper always appends env.temp_dir as the final storage path.
        // Point it at disk3 and pass the other three disks explicitly.
        env.temp_dir = disk3.to_string_lossy().to_string();
        let disk0_arg = disk0.to_string_lossy().to_string();
        let disk1_arg = disk1.to_string_lossy().to_string();
        let disk2_arg = disk2.to_string_lossy().to_string();
        env.start_rustfs_server_with_env(
            vec![disk0_arg.as_str(), disk1_arg.as_str(), disk2_arg.as_str()],
            &[("RUSTFS_UNSAFE_BYPASS_DISK_CHECK", "true")],
        )
        .await
        .expect("Failed to start 4-disk RustFS");

        let client = env.create_s3_client();
        let bucket = "heal-cleared-disk-regression";
        let target_object_count = std::env::var("RUSTFS_HEAL_REBUILD_OBJECT_COUNT")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(4)
            .max(4);
        let heal_timeout_secs = std::env::var("RUSTFS_HEAL_REBUILD_TIMEOUT_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(60);

        let mut objects: Vec<(String, Vec<u8>, &'static str)> = vec![
            (
                "中文/报告-0001.json".to_string(),
                "{\"message\":\"hello 中文\"}".as_bytes().to_vec(),
                "application/json",
            ),
            (
                "english/images/photo-0002.jpg".to_string(),
                vec![0xff, 0xd8, 0xff, 0x00, 0x42, 0x24],
                "image/jpeg",
            ),
            (
                "mixed/空 格 + symbols @#%.txt".to_string(),
                b"text object with spaces and symbols".to_vec(),
                "text/plain; charset=utf-8",
            ),
            (
                "bin/archive-0004.bin".to_string(),
                (0..=255).collect::<Vec<u8>>(),
                "application/octet-stream",
            ),
        ];
        for index in objects.len()..target_object_count {
            objects.push((
                format!("bulk/prefix-{}/object-{index:04}.txt", index % 17),
                format!("bulk object {index}: heal regression payload").into_bytes(),
                "text/plain; charset=utf-8",
            ));
        }

        let object_keys = objects.iter().map(|(key, _, _)| key.clone()).collect::<Vec<_>>();
        let mut remaining_rebuild_keys: HashSet<String> = object_keys.iter().cloned().collect();

        client
            .create_bucket()
            .bucket(bucket)
            .send()
            .await
            .expect("bucket create should succeed");
        for (key, body, content_type) in &objects {
            client
                .put_object()
                .bucket(bucket)
                .key(key)
                .content_type(*content_type)
                .body(ByteStream::from(body.clone()))
                .send()
                .await
                .expect("PUT should succeed");
        }

        assert!(has_file_under(&disk0), "disk0 should contain object shards before wipe");
        env.stop_server();

        std::fs::remove_dir_all(&disk0).expect("disk0 wipe should succeed");
        std::fs::create_dir_all(&disk0).expect("disk0 should be recreated empty");
        assert!(!has_file_under(&disk0), "disk0 must be empty before restart");

        env.start_rustfs_server_with_env(
            vec![disk0_arg.as_str(), disk1_arg.as_str(), disk2_arg.as_str()],
            &[("RUSTFS_UNSAFE_BYPASS_DISK_CHECK", "true")],
        )
        .await
        .expect("Failed to restart 4-disk RustFS after disk wipe");
        // The helper's Drop cleanup removes env.temp_dir. Reset it to the parent
        // directory after server startup so all four disk directories are cleaned
        // without manually deleting a path Drop will also try to remove.
        env.temp_dir = root.to_string_lossy().to_string();

        let heal_body = r#"{"recursive":true,"dryRun":false,"remove":false,"recreate":true,"scanMode":2,"updateParity":false,"nolock":false}"#;
        let heal_url = format!("{}/rustfs/admin/v3/heal/{}?forceStart=true", env.url, bucket);
        signed_admin_post(&heal_url, Some(heal_body), &env.access_key, &env.secret_key)
            .await
            .expect("admin deep heal should be accepted");

        for _ in 0..heal_timeout_secs {
            if !remaining_rebuild_keys.is_empty() {
                let mut rebuilt = Vec::new();
                for key in &remaining_rebuild_keys {
                    if object_metadata_exists_on_disk(&disk0, bucket, key) {
                        rebuilt.push(key.clone());
                    }
                }
                for key in rebuilt {
                    let _ = remaining_rebuild_keys.remove(&key);
                }
            }

            if remaining_rebuild_keys.is_empty() {
                for (key, body, _) in &objects {
                    assert_object_body(&env, bucket, key, body).await;
                }

                env.stop_server();
                for key in &object_keys {
                    assert!(
                        object_metadata_exists_on_disk(&disk0, bucket, key),
                        "wiped disk should contain rebuilt xl.meta for {key}"
                    );
                }
                return;
            }

            sleep(Duration::from_secs(1)).await;
        }

        panic!("admin deep heal did not rebuild all files on the wiped disk within timeout");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_cluster_root_heal_rebuilds_replaced_remote_disk() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        info!("Root recursive heal should rebuild data on a remote node after its disk is replaced and the node rejoins");

        let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
        cluster.set_env("RUSTFS_UNSAFE_BYPASS_DISK_CHECK", "true");
        cluster.set_env("RUSTFS_HEAL_ENABLED", "true");
        cluster.set_env("RUSTFS_SCANNER_ENABLED", "true");
        cluster.start().await?;
        let clients = cluster.create_all_clients()?;

        let bucket = "heal-replaced-remote-disk";
        clients[0].create_bucket().bucket(bucket).send().await?;

        let online_key = "cluster/online-before-replacement.bin";
        let online_body = b"object written while all cluster nodes are online".to_vec();
        clients[0]
            .put_object()
            .bucket(bucket)
            .key(online_key)
            .body(ByteStream::from(online_body.clone()))
            .send()
            .await?;

        let replaced_disk = PathBuf::from(&cluster.nodes[1].data_dir);
        assert!(
            object_metadata_exists_on_disk(&replaced_disk, bucket, online_key),
            "node 1 should contain metadata before disk replacement"
        );

        cluster.stop_node(1)?;
        std::fs::remove_dir_all(&replaced_disk)?;
        std::fs::create_dir_all(&replaced_disk)?;
        assert!(!has_file_under(&replaced_disk), "replacement disk must start empty");

        let outage_key = "cluster/written-while-node-down.bin";
        let outage_body = b"object written while one remote node is offline".to_vec();
        timeout(Duration::from_secs(30), async {
            clients[0]
                .put_object()
                .bucket(bucket)
                .key(outage_key)
                .body(ByteStream::from(outage_body.clone()))
                .send()
                .await
        })
        .await??;

        cluster.start_node(1).await?;

        let status_url = format!("{}/rustfs/admin/v3/background-heal/status", cluster.nodes[0].url);
        let mut recovered = serde_json::Value::Null;
        for _ in 0..60 {
            let status_body = signed_admin_post(&status_url, None, &cluster.access_key, &cluster.secret_key).await?;
            assert!(
                !status_body.contains("MissingContentLength"),
                "background heal status should not fail without an explicit Content-Length: {status_body}"
            );
            recovered = serde_json::from_str(&status_body)
                .map_err(|err| format!("background heal status is not JSON ({err}): {status_body}"))?;
            if recovered["clusterStatusComplete"] == serde_json::Value::Bool(true) {
                break;
            }
            sleep(Duration::from_secs(1)).await;
        }
        assert_eq!(
            recovered["clusterStatusComplete"],
            serde_json::Value::Bool(true),
            "cluster heal status should recover before root heal starts: {recovered}"
        );

        let heal_body = r#"{"recursive":true,"dryRun":false,"remove":false,"recreate":true,"scanMode":2,"updateParity":false,"nolock":false}"#;
        let heal_url = format!("{}/rustfs/admin/v3/heal/?forceStart=true", cluster.nodes[0].url);
        signed_admin_post(&heal_url, Some(heal_body), &cluster.access_key, &cluster.secret_key).await?;

        let expected_objects = [(online_key, online_body.as_slice()), (outage_key, outage_body.as_slice())];
        let mut remaining_rebuild_keys: HashSet<&str> = expected_objects.iter().map(|(key, _)| *key).collect();
        let heal_timeout_secs = std::env::var("RUSTFS_HEAL_REPLACED_DISK_TIMEOUT_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(90);

        for _ in 0..heal_timeout_secs {
            for (key, body) in &expected_objects {
                let response = clients[0].get_object().bucket(bucket).key(*key).send().await?;
                let actual = response.body.collect().await?.into_bytes();
                assert_eq!(actual.as_ref(), *body, "object body changed for {key}");
            }

            if !remaining_rebuild_keys.is_empty() {
                let rebuilt = remaining_rebuild_keys
                    .iter()
                    .copied()
                    .filter(|key| object_metadata_exists_on_disk(&replaced_disk, bucket, key))
                    .collect::<Vec<_>>();
                for key in rebuilt {
                    let _ = remaining_rebuild_keys.remove(key);
                }
            }

            if remaining_rebuild_keys.is_empty() {
                return Ok(());
            }

            sleep(Duration::from_secs(1)).await;
        }

        Err(format!(
            "admin deep heal did not rebuild replaced remote disk metadata for {remaining_rebuild_keys:?} within timeout"
        )
        .into())
    }

    async fn wait_for_scanner_cycle_after(
        cluster: &RustFSTestClusterEnvironment,
        previous_cycle_end: u64,
    ) -> Result<u64, Box<dyn Error + Send + Sync>> {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let mut latest_cycle_end = 0;
            let mut versions_observed = false;
            let mut observations = Vec::with_capacity(cluster.nodes.len());
            for (node_index, node) in cluster.nodes.iter().enumerate() {
                let (status, body) = timeout(
                    Duration::from_secs(5),
                    admin_request(
                        &node.url,
                        Method::GET,
                        "/rustfs/admin/v3/scanner/status",
                        None,
                        &cluster.access_key,
                        &cluster.secret_key,
                    ),
                )
                .await??;
                assert_eq!(status, 200, "scanner status must be available: {body}");
                let status: serde_json::Value = serde_json::from_str(&body)?;
                assert_eq!(status["enabled"].as_bool(), Some(true), "scanner must stay enabled: {status}");
                let metrics = &status["metrics"];
                let cycle_end = metrics["last_cycle_end_unix_secs"]
                    .as_u64()
                    .ok_or("scanner status is missing its completed-cycle timestamp")?;
                let versions_scanned = metrics["versions_scanned"]
                    .as_u64()
                    .ok_or("scanner status is missing its version-coverage counter")?;
                latest_cycle_end = latest_cycle_end.max(cycle_end);
                versions_observed |= versions_scanned > 0;
                observations.push(format!(
                    "node{node_index}: end={cycle_end}, versions={versions_scanned}, cycle={}, active={}, leader={}, result={}",
                    metrics["current_cycle"],
                    metrics["current_cycle_active"],
                    metrics["leader_lock_state"],
                    metrics["last_cycle_result"],
                ));
            }
            // The coordinator records cycle completion, but remote workers
            // record scanned versions. Both witnesses need not share a node.
            if latest_cycle_end > previous_cycle_end && versions_observed {
                return Ok(latest_cycle_end);
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "enabled scanner did not complete an object-scanning cycle after {previous_cycle_end}: {observations:?}"
                )
                .into());
            }
            sleep(Duration::from_millis(250)).await;
        }
    }

    // Keep the original unformatted-disk scenario above. This case retains the
    // format identity so only the explicit admin task can rebuild missing data.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_cluster_root_heal_resumes_missing_remote_shards_after_node_restart() -> Result<(), Box<dyn Error + Send + Sync>>
    {
        run_cluster_root_heal_interruption(InterruptionScenario::IsolatedTargetRestart).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_cluster_root_heal_recovers_remote_shards_after_background_target_restart()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        timeout(
            Duration::from_secs(420),
            run_cluster_root_heal_interruption(InterruptionScenario::BackgroundTargetRestart),
        )
        .await?
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_cluster_root_heal_recovers_remote_shards_after_coordinator_restart() -> Result<(), Box<dyn Error + Send + Sync>>
    {
        timeout(
            Duration::from_secs(420),
            run_cluster_root_heal_interruption(InterruptionScenario::BackgroundCoordinatorRestart),
        )
        .await?
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_cluster_root_heal_recovers_after_target_endpoint_blackhole() -> Result<(), Box<dyn Error + Send + Sync>> {
        if let Some(reason) = TcpPortBlackhole::unavailable_reason()? {
            init_logging();
            warn!(
                event = "heal_interruption_skipped",
                component = "e2e_test",
                subsystem = "heal",
                interruption_kind = "target_endpoint_blackhole",
                reason,
                "Skipping endpoint blackhole scenario: network fault injection is unavailable on this host"
            );
            return Ok(());
        }
        timeout(
            Duration::from_secs(420),
            run_cluster_root_heal_interruption(InterruptionScenario::TargetEndpointBlackhole),
        )
        .await?
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum InterruptionScenario {
        IsolatedTargetRestart,
        BackgroundTargetRestart,
        BackgroundCoordinatorRestart,
        TargetEndpointBlackhole,
    }

    async fn run_cluster_root_heal_interruption(scenario: InterruptionScenario) -> Result<(), Box<dyn Error + Send + Sync>> {
        let server_binary = rustfs_binary_path();
        let evidence_run = if scenario == InterruptionScenario::BackgroundTargetRestart {
            restart_evidence_run(&server_binary, BACKGROUND_TARGET_RESTART_EVIDENCE)?
        } else {
            None
        };
        let mut evidence_objects = Vec::new();
        let (background_enabled, interruption_node, interruption_kind) = match scenario {
            InterruptionScenario::IsolatedTargetRestart => (false, 1, "target_restart"),
            InterruptionScenario::BackgroundTargetRestart => (true, 1, "background_target_restart"),
            InterruptionScenario::BackgroundCoordinatorRestart => (true, 0, "coordinator_restart"),
            InterruptionScenario::TargetEndpointBlackhole => (false, 1, "target_endpoint_blackhole"),
        };
        init_logging();
        info!(
            event = "heal_interruption_started",
            component = "e2e_test",
            subsystem = "heal",
            background_enabled,
            interruption_node,
            interruption_kind,
            "Starting root-heal interruption test"
        );

        let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
        cluster.set_env("RUSTFS_UNSAFE_BYPASS_DISK_CHECK", "true");
        cluster.set_env("RUSTFS_HEAL_ENABLED", "true");
        // Heal control uses the first lexicographically sorted grid host.
        // Keep that coordinator distinct from the remote target at index 1.
        cluster.nodes.sort_by(|left, right| left.url.cmp(&right.url));
        cluster.set_env("RUSTFS_HEAL_AUTO_HEAL_ENABLE", background_enabled.to_string());
        cluster.set_env("RUSTFS_HEAL_MRF_ENABLE", background_enabled.to_string());
        cluster.set_env("RUSTFS_SCANNER_ENABLED", background_enabled.to_string());
        if background_enabled {
            // Only the scanner cadence is accelerated. Keep normal Heal
            // concurrency and every automatic recovery owner enabled.
            for &(key, value) in FAST_DATA_USAGE_SCANNER_ENV {
                cluster.set_env(key, value);
            }
        } else {
            cluster.set_env("RUSTFS_HEAL_MAX_CONCURRENT_HEALS", "1");
            cluster.set_env("RUSTFS_HEAL_MAX_CONCURRENT_PER_SET", "1");
            cluster.set_env("RUSTFS_HEAL_PAGE_OBJECT_CONCURRENCY", "1");
            cluster.set_env("RUSTFS_HEAL_PAGE_PARALLEL_ENABLE", "false");
        }
        // Keep every node's Heal runtime enabled for normal disk registration.
        let server_rust_log = std::env::var("RUSTFS_HEAL_CHAOS_SERVER_RUST_LOG")
            .unwrap_or_else(|_| "rustfs::heal::task=info,rustfs=error".to_string());
        cluster.set_env("RUST_LOG", server_rust_log);
        let log_dir = std::env::var("RUSTFS_HEAL_CHAOS_LOG_DIR").unwrap_or_else(|_| format!("{}/logs", cluster.temp_dir));
        std::fs::create_dir_all(&log_dir)?;
        for node_index in 0..cluster.nodes.len() {
            cluster.set_node_capture_log_path(node_index, format!("{log_dir}/node{node_index}.log"))?;
        }
        cluster.start_with_binary(&server_binary).await?;
        let clients = cluster.create_all_clients()?;

        let bucket = "heal-restart-during-rebuild";
        clients[0].create_bucket().bucket(bucket).send().await?;

        let replaced_disk = PathBuf::from(&cluster.nodes[1].data_dir);
        let replacement_format_path = replaced_disk.join(".rustfs.sys").join("format.json");
        let replacement_format = std::fs::read(&replacement_format_path).map_err(|err| {
            format!("failed to capture target format before replacement wipe at {replacement_format_path:?}: {err}")
        })?;
        let online_object_count = std::env::var("RUSTFS_HEAL_CHAOS_OBJECT_COUNT")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(24)
            .clamp(8, 64);
        let object_size_bytes = std::env::var("RUSTFS_HEAL_CHAOS_OBJECT_SIZE_BYTES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(4 * 1024 * 1024)
            .clamp(1024 * 1024, 16 * 1024 * 1024);
        let mut expected_manifests = Vec::with_capacity(online_object_count);
        for index in 0..online_object_count {
            let key = format!("cluster/online/object-{index:04}.bin");
            let payload_seed = u8::try_from(index + 1).expect("clamped object count must fit in u8");
            timeout(
                Duration::from_secs(30),
                clients[0]
                    .put_object()
                    .bucket(bucket)
                    .key(&key)
                    .body(ByteStream::from(deterministic_object_body(object_size_bytes, payload_seed)))
                    .send(),
            )
            .await??;
            let shard_census = census_object_version_on_disk(&replaced_disk, bucket, &key, None)?;
            assert!(
                shard_census.is_complete(),
                "node 1 should hold a complete baseline shard for {key}: {shard_census:?}"
            );
            assert!(
                !shard_census.expected_part_numbers.is_empty(),
                "chaos objects must use physical part shards rather than inline data: {shard_census:?}"
            );
            expected_manifests.push(PhysicalObjectManifest {
                key,
                payload_seed,
                shard_census,
            });
        }

        let expected_pool_metadata = if background_enabled {
            let census = census_object_version_on_disk(&replaced_disk, RUSTFS_META_BUCKET, POOL_METADATA_OBJECT, None)?;
            assert!(
                census.is_complete(),
                "target must hold complete pool metadata before the fault: {census:?}"
            );
            wait_for_scanner_cycle_after(&cluster, 0).await?;
            Some(census)
        } else {
            None
        };

        cluster.stop_node(1)?;
        std::fs::remove_dir_all(&replaced_disk)?;
        std::fs::create_dir_all(
            replacement_format_path
                .parent()
                .ok_or("replacement format path has no parent")?,
        )?;
        std::fs::write(&replacement_format_path, replacement_format)?;
        assert!(
            replacement_format_path.is_file(),
            "replacement target must retain only its preformatted topology identity"
        );

        let outage_key = "cluster/written-while-node-down.bin";
        let outage_payload_seed = 0xf1;
        timeout(
            Duration::from_secs(30),
            clients[2]
                .put_object()
                .bucket(bucket)
                .key(outage_key)
                .body(ByteStream::from(deterministic_object_body(object_size_bytes, outage_payload_seed)))
                .send(),
        )
        .await??;

        let mut outage_peer_erasure_indices = HashSet::new();
        for (node_index, node) in cluster.nodes.iter().enumerate() {
            if node_index == 1 {
                continue;
            }
            let census = census_object_version_on_disk(Path::new(&node.data_dir), bucket, outage_key, None)?;
            assert!(
                census.is_complete(),
                "online node {node_index} must hold a complete outage-object shard: {census:?}"
            );
            let erasure_index = census
                .erasure_index
                .ok_or_else(|| format!("online node {node_index} outage-object shard has no erasure index: {census:?}"))?;
            assert!(
                (1..=cluster.nodes.len()).contains(&erasure_index),
                "online node {node_index} outage-object erasure index is out of range: {census:?}"
            );
            assert!(
                outage_peer_erasure_indices.insert(erasure_index),
                "outage-object erasure index {erasure_index} is duplicated across online nodes"
            );
        }
        assert_eq!(
            outage_peer_erasure_indices.len(),
            cluster.nodes.len().saturating_sub(1),
            "every online node must contribute one unique outage-object erasure index"
        );
        let expected_outage_target_erasure_index = (1..=cluster.nodes.len())
            .find(|index| !outage_peer_erasure_indices.contains(index))
            .ok_or("online outage-object shards leave no erasure index for the replacement target")?;

        let heal_body = r#"{"recursive":true,"dryRun":false,"remove":false,"recreate":true,"scanMode":2,"updateParity":false,"nolock":false}"#;
        if !background_enabled {
            // The PUT path may have admitted a direct Internal object repair while
            // node 1 was offline. Cancel the isolated bucket path before the target
            // returns; otherwise it could rebuild the outage object and invalidate
            // the explicit-root ownership assertion below.
            let cancel_outage_heal_path = format!("/rustfs/admin/v3/heal/{bucket}?forceStop=true");
            let (cancel_status, cancel_body) = admin_request(
                &cluster.nodes[0].url,
                Method::POST,
                &cancel_outage_heal_path,
                Some(heal_body.to_string()),
                &cluster.access_key,
                &cluster.secret_key,
            )
            .await?;
            if !cancel_status.is_success() {
                return Err(format!("cancel outage heal failed: {cancel_status} {cancel_body}").into());
            }
        }

        cluster.start_node_from_binary(1, &server_binary).await?;

        let status_url = format!("{}/rustfs/admin/v3/background-heal/status", cluster.nodes[0].url);
        let recovery_deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let status_body = signed_admin_post(&status_url, None, &cluster.access_key, &cluster.secret_key).await?;
            assert!(
                !status_body.contains("MissingContentLength"),
                "background heal status should not fail without an explicit Content-Length: {status_body}"
            );
            let recovered: serde_json::Value = serde_json::from_str(&status_body)
                .map_err(|err| format!("background heal status is not JSON ({err}): {status_body}"))?;
            let ready = if background_enabled {
                recovered["clusterStatusComplete"] == serde_json::Value::Bool(true)
            } else {
                cluster_heal_is_idle(&recovered)
            };
            if ready {
                break;
            }
            if Instant::now() >= recovery_deadline {
                return Err(format!("cluster heal operations did not become idle before root heal: {recovered}").into());
            }
            sleep(Duration::from_millis(250)).await;
        }
        let pre_heal_replacement = replacement_recovery_status(&cluster).await?;
        if !background_enabled {
            assert_eq!(
                matching_manifest_count(&replaced_disk, bucket, &expected_manifests)?,
                0,
                "non-admin Heal is disabled, so the replacement target must remain empty before the explicit root heal"
            );
            assert!(
                !census_object_version_on_disk(&replaced_disk, bucket, outage_key, None)?.has_xl_meta,
                "the object written during the outage must be absent before the explicit root heal"
            );
            assert_eq!(
                pre_heal_replacement["cluster"]["records"].as_array().map(Vec::len),
                Some(0),
                "isolated target must not retain an automatic replacement generation: {pre_heal_replacement}"
            );
        }

        let heal_url = format!("{}/rustfs/admin/v3/heal/?forceStart=true", cluster.nodes[0].url);
        let heal_start_body = signed_admin_post(&heal_url, Some(heal_body), &cluster.access_key, &cluster.secret_key).await?;
        let heal_start: serde_json::Value = serde_json::from_str(&heal_start_body)
            .map_err(|err| format!("heal start response is not JSON ({err}): {heal_start_body}"))?;
        let client_token = heal_start["clientToken"]
            .as_str()
            .filter(|token| !token.is_empty())
            .ok_or_else(|| format!("heal start response has no client token: {heal_start}"))?;
        let task_status_url = format!("{}/rustfs/admin/v3/heal/?clientToken={client_token}", cluster.nodes[0].url);

        let partial_timeout_secs = std::env::var("RUSTFS_HEAL_CHAOS_PARTIAL_TIMEOUT_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(60);
        let partial_deadline = Instant::now() + Duration::from_secs(partial_timeout_secs);
        loop {
            let status_body = signed_admin_post(&status_url, None, &cluster.access_key, &cluster.secret_key).await?;
            let active_status: serde_json::Value = serde_json::from_str(&status_body)
                .map_err(|err| format!("background heal status is not JSON ({err}): {status_body}"))?;
            let active = if background_enabled {
                active_status["state"].as_str() == Some("active")
                    && active_status["healOperations"]["activeBySource"]["admin"].as_u64() == Some(1)
            } else {
                only_admin_heal_is_active(&active_status)
            };
            if active {
                break;
            }
            if Instant::now() >= partial_deadline {
                return Err(format!("root heal never became active within {partial_timeout_secs}s: {active_status}").into());
            }
            sleep(Duration::from_millis(50)).await;
        }
        let (partial_count, partial_manifest) = loop {
            // Hash one committed shard to prove progress without letting a
            // full-corpus hash pass consume the interruption window.
            let materialized = metadata_count(&replaced_disk, bucket, &expected_manifests);
            if materialized > 0
                && materialized < expected_manifests.len()
                && let Some(expected) = expected_manifests
                    .iter()
                    .find(|expected| object_metadata_exists_on_disk(&replaced_disk, bucket, &expected.key))
                && census_object_version_on_disk(&replaced_disk, bucket, &expected.key, None)?
                    .matches_manifest(&expected.shard_census)
            {
                break (materialized, expected);
            }
            if materialized == expected_manifests.len() {
                return Err(format!(
                    "root heal rebuilt all {} baseline objects before the target could be interrupted",
                    expected_manifests.len()
                )
                .into());
            }
            if Instant::now() >= partial_deadline {
                return Err(format!(
                    "root heal made no observable partial progress on the replacement target within {partial_timeout_secs}s"
                )
                .into());
            }
            sleep(Duration::from_millis(10)).await;
        };

        let pre_interrupt_status_body = signed_admin_post(&status_url, None, &cluster.access_key, &cluster.secret_key).await?;
        let pre_interrupt_status: serde_json::Value = serde_json::from_str(&pre_interrupt_status_body)
            .map_err(|err| format!("pre-interrupt background heal status is not JSON ({err}): {pre_interrupt_status_body}"))?;
        let pre_interrupt_replacement = replacement_recovery_status(&cluster).await?;
        let coordinator_log = std::fs::read_to_string(format!("{log_dir}/node0.log"))?;
        assert!(
            coordinator_log
                .lines()
                .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
                .any(|event| {
                    event["event"] == "heal_task_state"
                        && event["task_id"] == client_token
                        && event["heal_type"] == "cluster"
                        && event["state"] == "started"
                }),
            "node 0 must have started the exact admin task before interruption"
        );
        let pre_interrupt_operations = &pre_interrupt_status["healOperations"];
        assert_eq!(
            pre_interrupt_operations["activeBySource"]["admin"].as_u64(),
            Some(1),
            "interruption must occur while the single admin task is active: {pre_interrupt_status}"
        );
        if !background_enabled {
            assert!(
                only_admin_heal_is_active(&pre_interrupt_status),
                "isolated interruption must retain only the admin task: {pre_interrupt_status}"
            );
            assert_eq!(
                pre_interrupt_replacement["cluster"]["records"].as_array().map(Vec::len),
                Some(0),
                "root-heal interruption point must not retain an automatic replacement generation: {pre_interrupt_replacement}"
            );
        }
        info!(
            event = "heal_interruption_checkpoint",
            component = "e2e_test",
            subsystem = "heal",
            background_enabled,
            interruption_node,
            interruption_kind,
            partial_metadata_count = partial_count,
            verified_key = partial_manifest.key,
            "Observed partial rebuild before interruption"
        );

        let target_pid = cluster.nodes[1].process.as_ref().ok_or("target process is not running")?.id();
        if scenario == InterruptionScenario::TargetEndpointBlackhole {
            let node_pids = cluster
                .nodes
                .iter()
                .map(|node| {
                    node.process
                        .as_ref()
                        .ok_or("cluster process is not running")
                        .map(std::process::Child::id)
                })
                .collect::<Result<Vec<_>, _>>()?;

            timeout(Duration::from_secs(2), TcpStream::connect(&cluster.nodes[1].address)).await??;
            let mut blackhole = TcpPortBlackhole::install(&cluster.nodes[1].address)?;
            let blocked_connect = timeout(Duration::from_millis(500), TcpStream::connect(&cluster.nodes[1].address)).await;
            assert!(
                blocked_connect.is_err(),
                "target endpoint connection must time out while the OUTPUT DROP rule is active: {blocked_connect:?}"
            );

            let stable_window_secs = std::env::var("RUSTFS_HEAL_CHAOS_BLACKHOLE_STABLE_SECS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(2)
                .clamp(1, 5);
            let blackhole_timeout_secs = std::env::var("RUSTFS_HEAL_CHAOS_BLACKHOLE_TIMEOUT_SECS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(20)
                .clamp(stable_window_secs + 1, 60);
            let blackhole_deadline = Instant::now() + Duration::from_secs(blackhole_timeout_secs);
            let mut stable_count = metadata_count(&replaced_disk, bucket, &expected_manifests);
            let mut stable_since = Instant::now();
            loop {
                for (node_index, (node, expected_pid)) in cluster.nodes.iter_mut().zip(&node_pids).enumerate() {
                    let process = node
                        .process
                        .as_mut()
                        .ok_or_else(|| format!("node {node_index} process disappeared"))?;
                    assert_eq!(process.id(), *expected_pid, "node {node_index} PID changed during endpoint blackhole");
                    assert!(process.try_wait()?.is_none(), "node {node_index} exited during endpoint blackhole");
                }

                let current_count = metadata_count(&replaced_disk, bucket, &expected_manifests);
                if current_count == expected_manifests.len() {
                    return Err("root heal completed before the endpoint blackhole became observable".into());
                }
                if current_count != stable_count {
                    stable_count = current_count;
                    stable_since = Instant::now();
                }
                if stable_since.elapsed() >= Duration::from_secs(stable_window_secs) {
                    break;
                }
                if Instant::now() >= blackhole_deadline {
                    return Err(format!(
                        "target rebuild never remained stable for {stable_window_secs}s during the endpoint blackhole: last_count={stable_count}, total={}",
                        expected_manifests.len()
                    )
                    .into());
                }
                sleep(Duration::from_millis(100)).await;
            }

            let blocked_task_body = timeout(
                Duration::from_secs(5),
                signed_admin_post(&task_status_url, None, &cluster.access_key, &cluster.secret_key),
            )
            .await??;
            let blocked_task: serde_json::Value = serde_json::from_str(&blocked_task_body)
                .map_err(|err| format!("blackholed task status is not JSON ({err}): {blocked_task_body}"))?;
            assert_eq!(
                blocked_task["summary"].as_str(),
                Some("running"),
                "the original admin task must remain resumable during the endpoint blackhole: {blocked_task}"
            );
            assert!(
                census_object_version_on_disk(&replaced_disk, bucket, &partial_manifest.key, None)?
                    .matches_manifest(&partial_manifest.shard_census),
                "the witnessed complete shard must survive the endpoint blackhole"
            );

            blackhole.restore()?;
            timeout(Duration::from_secs(2), TcpStream::connect(&cluster.nodes[1].address)).await??;
            info!(
                event = "heal_endpoint_blackhole_restored",
                component = "e2e_test",
                subsystem = "heal",
                interruption_kind,
                stable_metadata_count = stable_count,
                stable_window_secs,
                "Restored target endpoint forwarding"
            );
        } else {
            cluster.stop_node(interruption_node)?;
            let stopped_count = metadata_count(&replaced_disk, bucket, &expected_manifests);
            assert!(
                stopped_count > 0 && stopped_count < expected_manifests.len(),
                "node {interruption_node} must stop during a partial rebuild, observed before stop={partial_count}, after stop={stopped_count}, total={}",
                expected_manifests.len()
            );
            assert!(
                census_object_version_on_disk(&replaced_disk, bucket, &partial_manifest.key, None)?
                    .matches_manifest(&partial_manifest.shard_census),
                "the witnessed complete shard must survive interruption"
            );
            let unclean_shutdown_marker = Path::new(&cluster.nodes[interruption_node].data_dir)
                .join(".rustfs.sys")
                .join("unclean-shutdown");
            if background_enabled {
                assert!(
                    unclean_shutdown_marker.is_file(),
                    "background restart must retain the real unclean-shutdown marker"
                );
            } else {
                match std::fs::remove_file(&unclean_shutdown_marker) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => {
                        return Err(
                            format!("failed to isolate unclean recovery marker {unclean_shutdown_marker:?}: {error}").into()
                        );
                    }
                }
            }
            cluster.start_node_from_binary(interruption_node, &server_binary).await?;
            if interruption_node == 0 {
                let target = cluster.nodes[1]
                    .process
                    .as_mut()
                    .ok_or("target process disappeared during coordinator restart")?;
                assert_eq!(target.id(), target_pid, "coordinator restart must not replace the target process");
                assert!(target.try_wait()?.is_none(), "the target must remain alive during coordinator restart");
            }
        }

        let scanner_cycle_floor = if background_enabled {
            Some(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs())
        } else {
            None
        };

        let heal_timeout_secs = std::env::var("RUSTFS_HEAL_REPLACED_DISK_TIMEOUT_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(180);
        let heal_deadline = Instant::now() + Duration::from_secs(heal_timeout_secs);
        loop {
            if metadata_count(&replaced_disk, bucket, &expected_manifests) == expected_manifests.len()
                && object_metadata_exists_on_disk(&replaced_disk, bucket, outage_key)
            {
                let matching = matching_manifest_count(&replaced_disk, bucket, &expected_manifests)?;
                let outage_census = census_object_version_on_disk(&replaced_disk, bucket, outage_key, None)?;
                let pool_metadata_matches = match &expected_pool_metadata {
                    Some(expected) => {
                        census_object_version_on_disk(&replaced_disk, RUSTFS_META_BUCKET, POOL_METADATA_OBJECT, None)?
                            .matches_manifest(expected)
                    }
                    None => true,
                };
                if matching == expected_manifests.len() && outage_census.is_complete() && pool_metadata_matches {
                    break;
                }
            }
            if Instant::now() >= heal_deadline {
                let matching = matching_manifest_count(&replaced_disk, bucket, &expected_manifests)?;
                let outage_census = census_object_version_on_disk(&replaced_disk, bucket, outage_key, None)?;
                let pool_metadata =
                    census_object_version_on_disk(&replaced_disk, RUSTFS_META_BUCKET, POOL_METADATA_OBJECT, None)?;
                let final_status = signed_admin_post(&status_url, None, &cluster.access_key, &cluster.secret_key)
                    .await
                    .unwrap_or_else(|err| format!("status request failed: {err}"));
                let task_status = match timeout(
                    Duration::from_secs(5),
                    signed_admin_post(&task_status_url, None, &cluster.access_key, &cluster.secret_key),
                )
                .await
                {
                    Ok(Ok(body)) => heal_task_status_diagnostic(&body),
                    Ok(Err(err)) => format!("task status request failed: {err}"),
                    Err(_) => "task status request exceeded 5s diagnostic budget".to_string(),
                };
                let replacement_status = match timeout(Duration::from_secs(5), replacement_recovery_status(&cluster)).await {
                    Ok(Ok(status)) => status.to_string(),
                    Ok(Err(err)) => format!("replacement status request failed: {err}"),
                    Err(_) => "replacement status request exceeded 5s diagnostic budget".to_string(),
                };
                return Err(format!(
                    "root heal did not recover after {interruption_kind} within {heal_timeout_secs}s: baseline={matching}/{}, outage={outage_census:?}, pool_metadata={pool_metadata:?}, status={final_status}, task_status={task_status}, pre_interrupt_status={pre_interrupt_status}, pre_heal_replacement={pre_heal_replacement}, pre_interrupt_replacement={pre_interrupt_replacement}, replacement_status={replacement_status}",
                    expected_manifests.len()
                )
                .into());
            }
            sleep(Duration::from_millis(250)).await;
        }

        for expected in &expected_manifests {
            let actual = census_object_version_on_disk(&replaced_disk, bucket, &expected.key, None)?;
            assert!(
                actual.matches_manifest(&expected.shard_census),
                "rebuilt target shard differs from its baseline for {}: {actual:?}",
                expected.key
            );
        }
        let outage_census = census_object_version_on_disk(&replaced_disk, bucket, outage_key, None)?;
        assert!(
            outage_census.is_complete(),
            "outage object must have a complete target shard: {outage_census:?}"
        );
        assert_eq!(
            outage_census.erasure_index,
            Some(expected_outage_target_erasure_index),
            "the outage object must be rebuilt into its own missing erasure slot"
        );

        if let Some(cycle_end) = scanner_cycle_floor {
            wait_for_scanner_cycle_after(&cluster, cycle_end).await?;
        }

        let mut expected_keys = expected_manifests
            .iter()
            .map(|manifest| manifest.key.clone())
            .collect::<HashSet<_>>();
        assert!(expected_keys.insert(outage_key.to_string()));
        let node_listings = assert_all_nodes_list_exact_keys(&clients, bucket, &expected_keys).await?;

        let target_client = cluster.create_s3_client(1)?;
        for expected in &expected_manifests {
            let response = target_client.get_object().bucket(bucket).key(&expected.key).send().await?;
            let actual = response.body.collect().await?.into_bytes();
            let expected_body = deterministic_object_body(object_size_bytes, expected.payload_seed);
            assert_eq!(actual.as_ref(), expected_body.as_slice(), "object body changed for {}", expected.key);
            if evidence_run.is_some() {
                evidence_objects.push(serde_json::json!({
                    "key": expected.key, "version_id": expected.shard_census.version_id,
                    "expected_bytes": expected_body.len(), "actual_bytes": actual.len(),
                    "expected_sha256": sha256_hex(&expected_body),
                    "actual_sha256": sha256_hex(&actual),
                    "expected_physical": expected.shard_census,
                    "physical": census_object_version_on_disk(&replaced_disk, bucket, &expected.key, None)?,
                }));
            }
        }
        let response = target_client.get_object().bucket(bucket).key(outage_key).send().await?;
        let actual = response.body.collect().await?.into_bytes();
        let expected_outage_body = deterministic_object_body(object_size_bytes, outage_payload_seed);
        assert_eq!(actual.as_ref(), expected_outage_body.as_slice(), "object body changed for {outage_key}");
        if evidence_run.is_some() {
            evidence_objects.push(serde_json::json!({
                "key": outage_key, "version_id": null,
                "expected_bytes": expected_outage_body.len(), "actual_bytes": actual.len(),
                "expected_sha256": sha256_hex(&expected_outage_body),
                "actual_sha256": sha256_hex(&actual),
                "expected_physical": null,
                "physical": census_object_version_on_disk(&replaced_disk, bucket, outage_key, None)?,
            }));
        }

        let terminal_deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let status_body = signed_admin_post(&status_url, None, &cluster.access_key, &cluster.secret_key).await?;
            let status: serde_json::Value = serde_json::from_str(&status_body)
                .map_err(|err| format!("background heal status is not JSON ({err}): {status_body}"))?;
            if cluster_heal_is_idle(&status) {
                break;
            }
            if Instant::now() >= terminal_deadline {
                return Err(format!("heal data rebuilt but operations did not converge to terminal idle: {status}").into());
            }
            sleep(Duration::from_millis(250)).await;
        }

        let task_status_body = signed_admin_post(&task_status_url, None, &cluster.access_key, &cluster.secret_key).await?;
        let task_status: serde_json::Value = serde_json::from_str(&task_status_body)
            .map_err(|err| format!("heal task status is not JSON ({err}): {task_status_body}"))?;
        if interruption_node == 0 {
            // Admin tasks are process-local. Physical and queue convergence
            // above establish recovery; a lost task must not report success.
            assert_eq!(
                task_status["summary"].as_str(),
                Some("notFound"),
                "interrupted task status: {task_status}"
            );
            assert_eq!(
                task_status["detail"].as_str(),
                Some("heal task not found or expired"),
                "interrupted admin task must be explicitly unavailable: {task_status}"
            );
            info!(
                event = "heal_interruption_recovered",
                component = "e2e_test",
                subsystem = "heal",
                interruption_node,
                interruption_kind,
                task_state = "not_found",
                "Physical recovery completed after coordinator restart"
            );
            return Ok(());
        }
        if task_status["summary"].as_str() != Some("finished") {
            return Err(format!("heal data rebuilt but task did not finish successfully: {task_status}").into());
        }

        if let Some(evidence_context) = evidence_run {
            let restarted_pid = cluster.nodes[1].process.as_ref().ok_or("restarted target is absent")?.id();
            assert_ne!(target_pid, restarted_pid, "target must be a new process");
            assert_eq!(
                file_sha256(&server_binary)?,
                evidence_context.run.binary.sha256,
                "server build changed during restart"
            );
            let evidence = serde_json::json!({
                "schema": 1, "case": evidence_context.case.id, "evidence": "process-restart",
                "run_id": evidence_context.run.run_id, "source_revision": evidence_context.run.source_revision,
                "test_build": compiled_test_identity(),
                "binary_sha256": evidence_context.run.binary.sha256,
                "test_binary_sha256": evidence_context.run.test_binary.sha256,
                "topology": {"nodes": cluster.nodes.len(), "drives_per_node": cluster.nodes[0].data_dirs.len()},
                "pid_before": target_pid, "pid_after": restarted_pid,
                "objects": evidence_objects, "node_listings": node_listings,
            });
            let data = serde_json::to_vec(&evidence)?;
            if data.len() > 1024 * 1024 {
                return Err("scanner/heal oracle exceeds the 1 MiB artifact budget".into());
            }
            let mut output = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(evidence_context.directory.join(evidence_context.case.oracle))?;
            output.write_all(&data)?;
            output.sync_all()?;
        }

        Ok(())
    }

    /// Issue #5850: `background-heal/status` must answer while a peer is down.
    ///
    /// Exercises the production path in `read_cluster_heal_status` end to end,
    /// which the unit tests around `merge_peer_heal_statuses` cannot: with one
    /// node stopped, the endpoint must return 200 with
    /// `clusterStatusComplete: false` and an explicit `degraded` (or, when
    /// heal work is known active, `active`) state — never the previous
    /// cluster-wide 500 — and must return to a complete, non-degraded answer
    /// once the node rejoins. Reverting either all-or-nothing gate (the
    /// topology early-return or the merge hard-fail) turns the down-window
    /// response into a 500 and fails this test.
    #[tokio::test]
    async fn test_background_heal_status_degrades_while_peer_down_and_recovers_after_rejoin()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        info!("Issue #5850: background-heal/status must degrade, not 500, while a peer is down");

        let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
        cluster.set_env("RUSTFS_UNSAFE_BYPASS_DISK_CHECK", "true");
        cluster.start().await?;

        let status_url = format!("{}/rustfs/admin/v3/background-heal/status", cluster.nodes[0].url);
        // Owned copies: the closure must not borrow `cluster`, which
        // stop_node/start_node need mutably between polls.
        let access_key = cluster.access_key.clone();
        let secret_key = cluster.secret_key.clone();
        let fetch_status = || async {
            let body = signed_admin_post(&status_url, None, &access_key, &secret_key).await?;
            let json: serde_json::Value =
                serde_json::from_str(&body).map_err(|err| format!("heal status response is not JSON ({err}): {body}"))?;
            Ok::<serde_json::Value, Box<dyn Error + Send + Sync>>(json)
        };

        // Healthy cluster: the answer must be definitive. Poll briefly — the
        // peer grid may still be settling right after start().
        let mut healthy = fetch_status().await?;
        for _ in 0..30 {
            if healthy["clusterStatusComplete"] == serde_json::Value::Bool(true) {
                break;
            }
            sleep(Duration::from_secs(1)).await;
            healthy = fetch_status().await?;
        }
        assert_eq!(
            healthy["clusterStatusComplete"],
            serde_json::Value::Bool(true),
            "healthy cluster should report a complete heal status: {healthy}"
        );

        cluster.stop_node(1)?;

        // While the peer is down every response must stay 200 (signed_admin_post
        // fails on any non-2xx, so the old 500 fails the test immediately) and
        // must degrade to an explicitly-partial answer. The peer query timeout
        // is 5 s, so a couple of polls are enough for the dead peer to surface.
        let mut degraded = serde_json::Value::Null;
        for _ in 0..30 {
            degraded = fetch_status().await?;
            if degraded["clusterStatusComplete"] == serde_json::Value::Bool(false) {
                break;
            }
            sleep(Duration::from_secs(1)).await;
        }
        assert_eq!(
            degraded["clusterStatusComplete"],
            serde_json::Value::Bool(false),
            "heal status must mark itself partial while a peer is down: {degraded}"
        );
        let state = degraded["state"].as_str().unwrap_or_default();
        assert!(
            state == "degraded" || state == "active",
            "a partial answer must be labeled degraded (or active for known work), got {state:?}: {degraded}"
        );

        cluster.start_node(1).await?;

        // After the rejoin the endpoint must return to a definitive answer.
        let mut recovered = serde_json::Value::Null;
        for _ in 0..60 {
            recovered = fetch_status().await?;
            if recovered["clusterStatusComplete"] == serde_json::Value::Bool(true) {
                break;
            }
            sleep(Duration::from_secs(1)).await;
        }
        assert_eq!(
            recovered["clusterStatusComplete"],
            serde_json::Value::Bool(true),
            "heal status should be complete again after the node rejoined: {recovered}"
        );
        assert_ne!(
            recovered["state"].as_str().unwrap_or_default(),
            "degraded",
            "a complete answer must not be labeled degraded: {recovered}"
        );

        Ok(())
    }
}
