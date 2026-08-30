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
    use crate::chaos::{VersionShardCensus, census_object_version_on_disk, signed_admin_post};
    use crate::common::{RustFSTestClusterEnvironment, RustFSTestEnvironment, init_logging};
    use aws_sdk_s3::primitives::ByteStream;
    use std::collections::HashSet;
    use std::error::Error;
    use std::path::{Path, PathBuf};
    use tokio::time::{Duration, Instant, sleep, timeout};
    use tracing::info;

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

    fn matching_manifest_count(
        disk: &Path,
        bucket: &str,
        expected_manifests: &[(String, VersionShardCensus)],
    ) -> Result<usize, Box<dyn Error + Send + Sync>> {
        let mut matching = 0;
        for (key, expected) in expected_manifests {
            let actual = census_object_version_on_disk(disk, bucket, key, None)?;
            if actual.matches_manifest(expected) {
                matching += 1;
            }
        }
        Ok(matching)
    }

    fn metadata_count(disk: &Path, bucket: &str, expected_manifests: &[(String, VersionShardCensus)]) -> usize {
        expected_manifests
            .iter()
            .filter(|(key, _)| object_metadata_exists_on_disk(disk, bucket, key))
            .count()
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

        std::fs::remove_dir_all(&disk0).expect("disk0 wipe should succeed while server is running");
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
    async fn test_cluster_root_heal_resumes_replaced_remote_disk_after_node_restart() -> Result<(), Box<dyn Error + Send + Sync>>
    {
        init_logging();
        info!("Root recursive heal should resume after its replacement target restarts during a partial rebuild");

        let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
        cluster.set_env("RUSTFS_UNSAFE_BYPASS_DISK_CHECK", "true");
        cluster.set_env("RUSTFS_HEAL_ENABLED", "true");
        cluster.set_env("RUSTFS_HEAL_AUTO_HEAL_ENABLE", "false");
        cluster.set_env("RUSTFS_SCANNER_ENABLED", "false");
        cluster.set_env("RUSTFS_HEAL_MAX_CONCURRENT_HEALS", "1");
        cluster.set_env("RUSTFS_HEAL_MAX_CONCURRENT_PER_SET", "1");
        cluster.set_env("RUSTFS_HEAL_PAGE_OBJECT_CONCURRENCY", "1");
        cluster.set_env("RUSTFS_HEAL_PAGE_PARALLEL_ENABLE", "false");
        cluster.set_env("RUST_LOG", "rustfs::heal::task=info,rustfs=error");
        cluster.start().await?;
        let clients = cluster.create_all_clients()?;

        let bucket = "heal-restart-during-rebuild";
        clients[0].create_bucket().bucket(bucket).send().await?;

        let replaced_disk = PathBuf::from(&cluster.nodes[1].data_dir);
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
        let expected_body = vec![0x5a; object_size_bytes];
        let mut expected_manifests = Vec::with_capacity(online_object_count);
        for index in 0..online_object_count {
            let key = format!("cluster/online/object-{index:04}.bin");
            timeout(
                Duration::from_secs(30),
                clients[0]
                    .put_object()
                    .bucket(bucket)
                    .key(&key)
                    .body(ByteStream::from(expected_body.clone()))
                    .send(),
            )
            .await??;
            let census = census_object_version_on_disk(&replaced_disk, bucket, &key, None)?;
            assert!(census.is_complete(), "node 1 should hold a complete baseline shard for {key}: {census:?}");
            assert!(
                !census.expected_part_numbers.is_empty(),
                "chaos objects must use physical part shards rather than inline data: {census:?}"
            );
            expected_manifests.push((key, census));
        }

        cluster.stop_node(1)?;
        std::fs::remove_dir_all(&replaced_disk)?;
        std::fs::create_dir_all(&replaced_disk)?;
        assert!(!has_file_under(&replaced_disk), "replacement disk must start empty");

        let outage_key = "cluster/written-while-node-down.bin";
        timeout(
            Duration::from_secs(30),
            clients[0]
                .put_object()
                .bucket(bucket)
                .key(outage_key)
                .body(ByteStream::from(expected_body.clone()))
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
        assert_eq!(
            matching_manifest_count(&replaced_disk, bucket, &expected_manifests)?,
            0,
            "auto heal is disabled, so the replacement target must remain empty before the explicit root heal"
        );
        assert!(
            !census_object_version_on_disk(&replaced_disk, bucket, outage_key, None)?.has_xl_meta,
            "the object written during the outage must be absent before the explicit root heal"
        );

        let heal_body = r#"{"recursive":true,"dryRun":false,"remove":false,"recreate":true,"scanMode":2,"updateParity":false,"nolock":false}"#;
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
            let operations = &active_status["healOperations"];
            let admin_active = operations["activeBySource"]["admin"].as_u64().is_some_and(|count| count > 0)
                || operations["retryingBySource"]["admin"]
                    .as_u64()
                    .is_some_and(|count| count > 0);
            let active = active_status["state"].as_str() == Some("active")
                && (operations["activeTasks"].as_u64().is_some_and(|count| count > 0)
                    || operations["retryingTasks"].as_u64().is_some_and(|count| count > 0))
                && admin_active;
            if active {
                break;
            }
            if Instant::now() >= partial_deadline {
                return Err(format!("root heal never became active within {partial_timeout_secs}s: {active_status}").into());
            }
            sleep(Duration::from_millis(50)).await;
        }
        let partial_count = loop {
            let matching = matching_manifest_count(&replaced_disk, bucket, &expected_manifests)?;
            if matching > 0 && matching < expected_manifests.len() {
                break matching;
            }
            if matching == expected_manifests.len() {
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

        cluster.stop_node(1)?;
        let stopped_count = matching_manifest_count(&replaced_disk, bucket, &expected_manifests)?;
        assert!(
            stopped_count > 0 && stopped_count < expected_manifests.len(),
            "the target must stop after a partial rebuild, observed before stop={partial_count}, after stop={stopped_count}, total={}",
            expected_manifests.len()
        );
        cluster.start_node(1).await?;

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
                if matching == expected_manifests.len() && outage_census.is_complete() {
                    break;
                }
            }
            if Instant::now() >= heal_deadline {
                let matching = matching_manifest_count(&replaced_disk, bucket, &expected_manifests)?;
                let outage_census = census_object_version_on_disk(&replaced_disk, bucket, outage_key, None)?;
                let final_status = signed_admin_post(&status_url, None, &cluster.access_key, &cluster.secret_key)
                    .await
                    .unwrap_or_else(|err| format!("status request failed: {err}"));
                let task_status = match signed_admin_post(&task_status_url, None, &cluster.access_key, &cluster.secret_key).await
                {
                    Ok(body) => heal_task_status_diagnostic(&body),
                    Err(err) => format!("task status request failed: {err}"),
                };
                return Err(format!(
                    "root heal did not resume after target restart within {heal_timeout_secs}s: baseline={matching}/{}, outage={outage_census:?}, status={final_status}, task_status={task_status}",
                    expected_manifests.len()
                )
                .into());
            }
            sleep(Duration::from_millis(250)).await;
        }

        for (key, expected) in &expected_manifests {
            let actual = census_object_version_on_disk(&replaced_disk, bucket, key, None)?;
            assert!(
                actual.matches_manifest(expected),
                "rebuilt target shard differs from its baseline for {key}: {actual:?}"
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

        let target_client = cluster.create_s3_client(1)?;
        for (key, _) in &expected_manifests {
            let response = target_client.get_object().bucket(bucket).key(key).send().await?;
            let actual = response.body.collect().await?.into_bytes();
            assert_eq!(actual.as_ref(), expected_body.as_slice(), "object body changed for {key}");
        }
        let response = target_client.get_object().bucket(bucket).key(outage_key).send().await?;
        let actual = response.body.collect().await?.into_bytes();
        assert_eq!(actual.as_ref(), expected_body.as_slice(), "object body changed for {outage_key}");

        let terminal_deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let status_body = signed_admin_post(&status_url, None, &cluster.access_key, &cluster.secret_key).await?;
            let status: serde_json::Value = serde_json::from_str(&status_body)
                .map_err(|err| format!("background heal status is not JSON ({err}): {status_body}"))?;
            let operations = &status["healOperations"];
            let terminal = status["clusterStatusComplete"] == serde_json::Value::Bool(true)
                && status["state"].as_str() == Some("idle")
                && operations["queueLength"].as_u64() == Some(0)
                && operations["activeTasks"].as_u64() == Some(0)
                && operations["retryingTasks"].as_u64() == Some(0);
            if terminal {
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
        if task_status["summary"].as_str() != Some("finished") {
            return Err(format!("heal data rebuilt but task did not finish successfully: {task_status}").into());
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
