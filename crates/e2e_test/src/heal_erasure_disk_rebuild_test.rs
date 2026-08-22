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
    use crate::chaos::signed_admin_post;
    use crate::common::{RustFSTestClusterEnvironment, RustFSTestEnvironment, init_logging};
    use aws_sdk_s3::primitives::ByteStream;
    use std::collections::HashSet;
    use std::error::Error;
    use std::path::{Path, PathBuf};
    use tokio::time::{Duration, sleep, timeout};
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
