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

//! Regression tests for distributed cluster startup and quorum.
//!
//! Covers the recurring pattern where multi-node clusters fail to start due to
//! lock quorum issues, DNS resolution delays, or erasure quorum deadlocks.
//! This has regressed 7+ times.
//!
//! ## Regression Issues
//!
//! - rustfs#5416: RustFS cannot cold-start with 2/3 quorum when Pod DNS missing
//! - rustfs#2945: Distributed mode fails on K8s: erasure quorum deadlock
//! - rustfs#2794: distributed deployment does not become ready
//! - rustfs#2601: fresh pod immediately enters FaultyDisk state
//! - rustfs#4040: Distributed startup can fail lock quorum before AppContext initializes
//! - rustfs#5655: fix(ecstore): bootstrap fresh four-node clusters reliably
//! - rustfs#4954: S3/health endpoint unavailability after multi-pool scale-up

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestClusterEnvironment, init_logging};
    use aws_sdk_s3::primitives::ByteStream;
    use std::error::Error;
    use tokio::time::{Duration, sleep};
    use tracing::info;

    type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

    /// RT-10: Verify 4-node cluster starts successfully and all nodes are ready.
    ///
    /// Regression pattern: distributed startup fails with quorum deadlock or
    /// lock acquisition timeout (rustfs#2945, rustfs#5655).
    ///
    /// Steps:
    /// 1. Create a 4-node cluster
    /// 2. Start all nodes simultaneously
    /// 3. Verify all nodes report healthy
    /// 4. Verify S3 operations work through any node
    #[tokio::test]
    async fn test_four_node_cluster_startup_and_health() -> TestResult {
        init_logging();
        info!("RT-10: 4-node cluster startup and health");

        let mut cluster = RustFSTestClusterEnvironment::new(4).await.expect("create 4-node cluster");

        cluster.start().await.expect("start 4-node cluster");

        // Create a bucket and verify it's accessible from all nodes
        cluster
            .create_test_bucket("rt10-startup")
            .await
            .expect("create bucket on cluster");

        let clients = cluster.create_all_clients().expect("create per-node clients");

        // Verify S3 operations work from every node
        for (i, client) in clients.iter().enumerate() {
            client
                .put_object()
                .bucket("rt10-startup")
                .key(format!("from-node-{i}.txt"))
                .body(ByteStream::from_static(b"hello from node"))
                .send()
                .await
                .unwrap_or_else(|e| panic!("PUT from node {i} failed: {e}"));
        }

        // Verify all objects are visible from node 0
        let list = clients[0]
            .list_objects_v2()
            .bucket("rt10-startup")
            .send()
            .await
            .expect("list objects from node 0");

        assert_eq!(
            list.contents().len(),
            4,
            "RT-10 FAIL: expected 4 objects (one per node), found {}",
            list.contents().len()
        );

        info!("RT-10 PASS: 4-node cluster starts and serves S3 from all nodes");
        Ok(())
    }

    /// RT-10b: Verify cluster handles node restart gracefully.
    ///
    /// Regression pattern: after a node restart, it cannot rejoin the cluster
    /// or enters a faulty state (rustfs#2601).
    #[tokio::test]
    async fn test_cluster_survives_node_restart() -> TestResult {
        init_logging();
        info!("RT-10b: cluster survives node restart");

        let mut cluster = RustFSTestClusterEnvironment::new(4).await.expect("create 4-node cluster");

        cluster.start().await.expect("start cluster");

        cluster.create_test_bucket("rt10b-restart").await.expect("create bucket");

        // Write data
        let clients = cluster.create_all_clients()?;
        clients[0]
            .put_object()
            .bucket("rt10b-restart")
            .key("before-restart.txt")
            .body(ByteStream::from_static(b"persistent data"))
            .send()
            .await
            .expect("put object before restart");

        // Stop node 3
        cluster.stop_node(3).expect("stop node 3");
        sleep(Duration::from_secs(2)).await;

        // Verify cluster still works with 3/4 nodes (quorum)
        clients[0]
            .put_object()
            .bucket("rt10b-restart")
            .key("during-offline.txt")
            .body(ByteStream::from_static(b"written while node 3 down"))
            .send()
            .await
            .expect("PUT should succeed with 3/4 nodes");

        // Restart node 3
        cluster.start_node(3).await.expect("restart node 3");

        // Wait for node to rejoin
        sleep(Duration::from_secs(3)).await;

        // Verify the restarted node can serve reads
        let list = clients[3]
            .list_objects_v2()
            .bucket("rt10b-restart")
            .send()
            .await
            .expect("list from restarted node");

        assert!(
            list.contents().len() >= 2,
            "RT-10b FAIL: restarted node sees {} objects, expected >= 2",
            list.contents().len()
        );

        info!("RT-10b PASS: cluster survives and recovers from node restart");
        Ok(())
    }

    /// RT-10c: Verify bucket creation persists across all nodes.
    ///
    /// Regression pattern: bucket metadata is not replicated to all nodes,
    /// causing NoSuchBucket errors on some nodes (rustfs#3191).
    #[tokio::test]
    async fn test_bucket_visible_from_all_nodes() -> TestResult {
        init_logging();
        info!("RT-10c: bucket visible from all nodes");

        let mut cluster = RustFSTestClusterEnvironment::new(4).await.expect("create 4-node cluster");

        cluster.start().await.expect("start cluster");

        cluster
            .create_test_bucket("rt10c-bucket-visibility")
            .await
            .expect("create bucket");

        let clients = cluster.create_all_clients()?;

        // Verify the bucket is visible from every node
        for (i, client) in clients.iter().enumerate() {
            let resp = client
                .list_objects_v2()
                .bucket("rt10c-bucket-visibility")
                .send()
                .await
                .unwrap_or_else(|e| panic!("list from node {i} failed (NoSuchBucket?): {e}"));

            assert!(resp.contents().is_empty(), "RT-10c: fresh bucket should be empty on node {i}");
        }

        info!("RT-10c PASS: bucket visible from all 4 nodes");
        Ok(())
    }

    /// A real elected CAS must cross a signed peer RPC while the receiver still
    /// holds its Bootstrap capability, before IAM installs any AppContext.
    #[tokio::test]
    async fn test_fresh_four_node_bootstrap_metadata_cas() -> TestResult {
        use futures::FutureExt;
        use std::path::PathBuf;

        init_logging();
        let nonce = uuid::Uuid::new_v4().to_string();
        let artifact = std::env::var_os("RUSTFS_E2E_STARTUP_CAS_ARTIFACT_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir)
            .join(format!("fresh-startup-cas-{nonce}"));
        std::fs::create_dir_all(&artifact)?;
        let binary_dir = tempfile::tempdir()?;
        let binary = prepare_startup_cas_binary(binary_dir.path(), &artifact)?;
        probe_startup_cas_binary(&binary, &nonce, &artifact).await?;

        let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
        let mut logs = Vec::new();
        let mut disks = Vec::new();
        let mut endpoints = Vec::new();
        let mut releases = StartupCasReleases(Vec::new());
        for i in 0..4 {
            let disk = PathBuf::from(&cluster.nodes[i].data_dir);
            assert!(std::fs::read_dir(&disk)?.next().is_none(), "node {i} must start with an empty disk");
            let log = artifact.join(format!("node-{i}.log"));
            let release = artifact.join(format!("release-{i}"));
            cluster.set_node_capture_log_path(i, log.to_string_lossy())?;
            cluster.set_node_env(i, "RUSTFS_E2E_STARTUP_CAS_RELEASE", release.to_string_lossy())?;
            endpoints.push(format!("http://{}{}", cluster.nodes[i].address, cluster.nodes[i].data_dir));
            disks.push(disk);
            logs.push(log);
            releases.0.push(release);
        }
        assert!(
            cluster.rustfs_volumes_arg().starts_with(&endpoints[0]),
            "node 0 must own the elected first endpoint"
        );
        cluster.set_env("RUSTFS_E2E_STARTUP_CAS_NONCE", &nonce);
        cluster.set_env("RUSTFS_OBS_LOG_DIRECTORY", "");
        cluster.set_env("RUSTFS_OBS_LOG_STDOUT_ENABLED", "true");
        cluster.set_env("RUST_LOG", "rustfs=info,rustfs_ecstore=trace");
        for key in [
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
        ] {
            cluster.set_env(key, "");
        }
        for key in ["NO_PROXY", "no_proxy"] {
            cluster.set_env(key, "127.0.0.1,localhost");
        }

        let attempt = tokio::time::timeout(
            Duration::from_secs(240),
            std::panic::AssertUnwindSafe(async {
                let mut startup = Box::pin(cluster.start_with_binary(&binary));
                let mut controller = Box::pin(wait_startup_cas(&logs, &disks, &endpoints, &nonce, &artifact));
                let (observed, startup_finished) = tokio::select! {
                    observed = tokio::time::timeout(Duration::from_secs(120), &mut controller) => {
                        (observed.map_err(std::io::Error::other).and_then(|result| result), false)
                    }
                    result = &mut startup => {
                        let error = match result {
                            Ok(()) => "startup returned before the unreleased CAS gates".to_owned(),
                            Err(error) => error.to_string(),
                        };
                        // Preserve a real pool.bin rejection instead of relabeling
                        // an earlier identity failure or generic readiness timeout.
                        let observed = match tokio::time::timeout(Duration::from_secs(5), &mut controller).await {
                            Ok(Err(error)) => Err(error),
                            _ => Err(std::io::Error::other(format!("PRECONDITION: startup ended without causal proof: {error}"))),
                        };
                        (observed, true)
                    }
                };
                let release_result = releases.release();
                let drained = if startup_finished {
                    Ok(())
                } else {
                    let deadline = if observed.is_ok() { 60 } else { 5 };
                    tokio::time::timeout(Duration::from_secs(deadline), &mut startup)
                        .await
                        .map_err(std::io::Error::other)
                        .map_err(|error| -> Box<dyn Error + Send + Sync> { error.into() })
                        .and_then(|result| result)
                };
                drop(startup);
                observed?;
                release_result?;
                drained?;
                for (i, node) in cluster.nodes.iter().enumerate() {
                    let pid = node
                        .process
                        .as_ref()
                        .ok_or_else(|| std::io::Error::other("missing child process"))?
                        .id();
                    let records = startup_cas_log(&logs[i])?;
                    assert!(
                        records
                            .iter()
                            .any(|r| r["kind"] == "observer-ready" && r["nonce"] == nonce && r["pid"] == pid),
                        "node {i} observation must belong to the actual harness child"
                    );
                }
                let bucket = format!("fresh-cas-{nonce}");
                tokio::time::timeout(Duration::from_secs(10), cluster.create_test_bucket(&bucket))
                    .await
                    .map_err(std::io::Error::other)??;
                for (i, client) in cluster.create_all_clients()?.iter().enumerate() {
                    let key = format!("node-{i}");
                    let body = format!("fresh four-node body {i} {nonce}").into_bytes();
                    tokio::time::timeout(Duration::from_secs(10), async {
                        client
                            .put_object()
                            .bucket(&bucket)
                            .key(&key)
                            .body(ByteStream::from(body.clone()))
                            .send()
                            .await?;
                        let received = client
                            .get_object()
                            .bucket(&bucket)
                            .key(&key)
                            .send()
                            .await?
                            .body
                            .collect()
                            .await?
                            .into_bytes();
                        assert_eq!(received.as_ref(), body, "node {i} must return the complete object body");
                        Ok::<_, Box<dyn Error + Send + Sync>>(())
                    })
                    .await
                    .map_err(std::io::Error::other)??;
                }
                Ok::<(), Box<dyn Error + Send + Sync>>(())
            })
            .catch_unwind(),
        )
        .await;
        // Drop the borrowed startup future before stopping its child processes.
        // All logs are outside the cluster directory which Drop removes.
        let release_result = releases.release();
        let pids: Vec<_> = cluster
            .nodes
            .iter()
            .map(|node| node.process.as_ref().map(std::process::Child::id))
            .collect();
        let process_record = serde_json::to_vec(&pids)
            .map_err(std::io::Error::other)
            .and_then(|bytes| std::fs::write(artifact.join("processes.json"), bytes));
        cluster.stop();
        eprintln!("fresh startup CAS evidence: {}", artifact.display());
        match attempt {
            Ok(Ok(result)) => {
                result?;
                release_result?;
                process_record?;
                Ok(())
            }
            Ok(Err(panic)) => std::panic::resume_unwind(panic),
            Err(error) => Err(std::io::Error::other(format!("fresh startup CAS fixture deadline: {error}")).into()),
        }
    }

    struct StartupCasReleases(Vec<std::path::PathBuf>);

    impl StartupCasReleases {
        fn release(&mut self) -> std::io::Result<()> {
            let mut failure = None;
            for path in &self.0 {
                if let Err(error) = std::fs::write(path, b"release") {
                    failure.get_or_insert(error);
                }
            }
            failure.map_or(Ok(()), Err)
        }
    }

    impl Drop for StartupCasReleases {
        fn drop(&mut self) {
            let _ = self.release();
        }
    }

    fn startup_cas_sha256(path: &std::path::Path) -> std::io::Result<String> {
        use sha2::{Digest, Sha256};
        use std::io::Read;
        let mut file = std::fs::File::open(path)?;
        let mut hash = Sha256::new();
        let mut buf = [0; 65536];
        loop {
            let len = file.read(&mut buf)?;
            if len == 0 {
                break;
            }
            hash.update(&buf[..len]);
        }
        Ok(rustfs_utils::crypto::hex(hash.finalize()))
    }

    fn startup_cas_git(args: &[&str]) -> std::io::Result<String> {
        let result = std::process::Command::new("git")
            .args(args)
            .current_dir(crate::common::workspace_root())
            .output()?;
        if !result.status.success() {
            return Err(std::io::Error::other("cannot verify startup fixture checkout identity"));
        }
        String::from_utf8(result.stdout)
            .map(|value| value.trim().to_owned())
            .map_err(std::io::Error::other)
    }

    fn prepare_startup_cas_binary(dir: &std::path::Path, artifact: &std::path::Path) -> std::io::Result<std::path::PathBuf> {
        use serde_json::Value;
        use std::path::PathBuf;
        let explicit = std::env::var_os("RUSTFS_E2E_STARTUP_CAS_BINARY")
            .or_else(|| std::env::var_os("CARGO_BIN_EXE_rustfs"))
            .ok_or_else(|| {
                std::io::Error::other("PRECONDITION: provide the existing hooks binary; this fixture never invokes Cargo")
            })?;
        let binary = std::fs::canonicalize(explicit)?;
        if let Some(other) = std::env::var_os("CARGO_BIN_EXE_rustfs") {
            if binary != std::fs::canonicalize(other)? {
                return Err(std::io::Error::other("PRECONDITION: conflicting startup binary paths"));
            }
        }
        let manifest_path = std::env::var_os("RUSTFS_E2E_STARTUP_CAS_BUILD_MANIFEST")
            .ok_or_else(|| std::io::Error::other("PRECONDITION: missing hooks binary build manifest"))?;
        let manifest_bytes = std::fs::read(manifest_path)?;
        let manifest: Value = serde_json::from_slice(&manifest_bytes)?;
        std::fs::write(artifact.join("binary-build.json"), &manifest_bytes)?;
        let checkout = crate::common::workspace_root();
        let sha = startup_cas_sha256(&binary)?;
        let valid = manifest["schema"] == 1
            && manifest["clean_before"] == true
            && manifest["clean_after"] == true
            && env!("RUSTFS_E2E_BUILD_DIRTY") == "false"
            && manifest["commit"] == env!("RUSTFS_E2E_BUILD_COMMIT")
            && manifest["commit"] == startup_cas_git(&["rev-parse", "HEAD"])?
            && manifest["tree"] == startup_cas_git(&["rev-parse", "HEAD^{tree}"])?
            && startup_cas_git(&["status", "--porcelain", "--untracked-files=normal"])?.is_empty()
            && manifest["lock_git_blob"] == env!("RUSTFS_E2E_BUILD_LOCK")
            && manifest["lock_git_blob"] == startup_cas_git(&["hash-object", "Cargo.lock"])?
            && manifest["lock_sha256"] == startup_cas_sha256(&checkout.join("Cargo.lock"))?
            && manifest["binary_sha256"] == sha
            && manifest["target"] == env!("RUSTFS_E2E_BUILD_TARGET")
            && manifest["profile"] == "debug"
            && manifest["features"]
                .as_array()
                .is_some_and(|features| features.iter().any(|f| f == "e2e-test-hooks"))
            && manifest["argv"]
                .as_array()
                .is_some_and(|argv| argv.iter().any(|arg| arg == "--features") && argv.iter().any(|arg| arg == "e2e-test-hooks"))
            && manifest["rustc_verbose"].as_str().is_some_and(|value| !value.is_empty())
            && manifest["build_flags"].is_object();
        if !valid {
            return Err(std::io::Error::other(
                "PRECONDITION: hooks binary identity does not match this clean test checkout",
            ));
        }
        let target = dir.join(format!("rustfs{}", std::env::consts::EXE_SUFFIX));
        std::fs::copy(&binary, &target)?;
        if startup_cas_sha256(&target)? != sha {
            return Err(std::io::Error::other("PRECONDITION: binary changed during fixture copy"));
        }
        std::fs::write(
            artifact.join("runner-build.json"),
            serde_json::to_vec(&serde_json::json!({
                "commit": env!("RUSTFS_E2E_BUILD_COMMIT"), "lock_git_blob": env!("RUSTFS_E2E_BUILD_LOCK"),
                "target": env!("RUSTFS_E2E_BUILD_TARGET"), "profile": env!("RUSTFS_E2E_BUILD_PROFILE"),
                "features": env!("RUSTFS_E2E_BUILD_FEATURES"), "binary_sha256": sha,
                "binary": PathBuf::from(&target),
            }))?,
        )?;
        Ok(target)
    }

    async fn probe_startup_cas_binary(binary: &std::path::Path, nonce: &str, artifact: &std::path::Path) -> std::io::Result<()> {
        struct Probe(std::process::Child);
        impl Drop for Probe {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let path = artifact.join("capability-probe.log");
        let log = std::fs::File::create(&path)?;
        let mut child = Probe(
            std::process::Command::new(binary)
                .arg("--help")
                .env("RUSTFS_E2E_STARTUP_CAS_PROBE", nonce)
                .stdout(log.try_clone()?)
                .stderr(log)
                .spawn()?,
        );
        let status = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(status) = child.0.try_wait()? {
                    return Ok::<_, std::io::Error>(status);
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .map_err(|_| std::io::Error::other("PRECONDITION: binary capability probe timed out"))??;
        let records = startup_cas_log(&path)?;
        let matching: Vec<_> = records
            .iter()
            .filter(|r| r["nonce"] == nonce && r["kind"] == "capability" && r["schema"] == "fresh-startup-cas/v1")
            .collect();
        if !status.success() || matching.len() != 1 {
            return Err(std::io::Error::other(
                "PRECONDITION: binary lacks the startup CAS hooks; no cluster was started",
            ));
        }
        Ok(())
    }

    fn startup_cas_log(path: &std::path::Path) -> std::io::Result<Vec<serde_json::Value>> {
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        if text.len() > 64 * 1024 * 1024 {
            return Err(std::io::Error::other("startup observation log exceeds fixture bound"));
        }
        let mut records = Vec::new();
        for line in text.split_inclusive('\n').filter_map(|line| line.strip_suffix('\n')) {
            if let Some(json) = line.strip_prefix("RUSTFS_E2E_STARTUP_CAS ") {
                records.push(serde_json::from_str(json)?);
            } else if let Ok(json) = serde_json::from_str::<serde_json::Value>(line) {
                // The existing remote-disk trace is JSON with flattened fields.
                records.push(json);
            }
        }
        Ok(records)
    }

    fn startup_cas_remote_matches(sender: &[serde_json::Value], receiver: &serde_json::Value) -> usize {
        sender
            .iter()
            .filter(|event| {
                event["target"]
                    .as_str()
                    .is_some_and(|target| target.split("::").eq(["rustfs_ecstore", "cluster", "rpc", "remote_disk"]))
                    && event["op"] == "rename_data"
                    && event["state"] == "started"
                    && event["endpoint"] == receiver["disk"]
                    && ["src_volume", "src_path", "dst_volume", "dst_path"]
                        .iter()
                        .all(|key| event[*key] == receiver[*key])
            })
            .count()
    }

    async fn wait_startup_cas(
        paths: &[std::path::PathBuf],
        disks: &[std::path::PathBuf],
        endpoints: &[String],
        nonce: &str,
        artifact: &std::path::Path,
    ) -> std::io::Result<()> {
        loop {
            let logs: Vec<_> = paths
                .iter()
                .map(|path| startup_cas_log(path))
                .collect::<std::io::Result<_>>()?;
            let events: Vec<Vec<_>> = logs
                .iter()
                .map(|records| records.iter().filter(|r| r["nonce"] == nonce).collect())
                .collect();
            let source = &events[0];
            for (node, events) in events.iter().enumerate() {
                for event in events.iter().filter(|r| r["kind"] == "cas") {
                    if node != 0 {
                        return Err(std::io::Error::other(format!(
                            "PRECONDITION: non-elected node {node} executed CAS: {event}"
                        )));
                    }
                    if event["ok"] == false {
                        let object = event["object"].as_str().unwrap_or_default();
                        let receiver = logs.iter().skip(1).flat_map(|records| records.iter()).find(|r| {
                            r["nonce"] == nonce
                                && r["kind"] == "receiver"
                                && r["dst_path"] == object
                                && r["ok"] == false
                                && r["target"] == "bootstrap"
                                && startup_cas_remote_matches(&logs[0], r) == 1
                        });
                        if let Some(receiver) = receiver {
                            let identity_ok = source
                                .iter()
                                .take_while(|r| !std::ptr::eq(**r, *event))
                                .any(|r| r["kind"] == "cas" && r["phase"] == "identity_cas" && r["ok"] == true);
                            let class = if object == "pool.bin" && identity_ok {
                                "POOL_BIN_CAUSAL_REJECTION"
                            } else {
                                "PRECONDITION_IDENTITY_OR_STARTUP_FAILURE"
                            };
                            std::fs::write(
                                artifact.join("cas-rejection.json"),
                                serde_json::to_vec_pretty(&serde_json::json!({
                                    "class": class, "sender": event, "receiver": receiver,
                                }))?,
                            )?;
                            return Err(std::io::Error::other(format!("{class}: sender={event}; receiver={receiver}")));
                        }
                    }
                }
            }
            if events
                .iter()
                .all(|records| records.iter().any(|r| r["kind"] == "gate" && r["slot_installed"] == false))
            {
                let mut pids = std::collections::HashSet::new();
                for records in &events {
                    let ready: Vec<_> = records.iter().filter(|r| r["kind"] == "observer-ready").collect();
                    if ready.len() != 1
                        || !pids.insert(
                            ready[0]["pid"]
                                .as_u64()
                                .ok_or_else(|| std::io::Error::other("missing child PID"))?,
                        )
                    {
                        return Err(std::io::Error::other(
                            "PRECONDITION: four independent observer-capable child processes required",
                        ));
                    }
                    if records.iter().any(|r| r["pid"] != ready[0]["pid"]) {
                        return Err(std::io::Error::other("PRECONDITION: observation process mismatch"));
                    }
                }
                let prepare: Vec<_> = source
                    .iter()
                    .filter(|r| r["kind"] == "cas" && r["phase"] == "prepare_cas" && r["object"] == "pool.bin")
                    .collect();
                let commit: Vec<_> = source
                    .iter()
                    .filter(|r| r["kind"] == "cas" && r["phase"] == "commit_cas" && r["object"] == "pool.bin")
                    .collect();
                if prepare.len() != 1 || commit.len() != 1 {
                    return Err(std::io::Error::other("PRECONDITION: startup CAS evidence is absent or ambiguous"));
                }
                let (prepare, commit) = (*prepare[0], *commit[0]);
                for cas in [prepare, commit] {
                    if cas["ok"] != true
                        || cas["tail_drained"] != true
                        || cas["no_lock"] != true
                        || cas["etag"].as_str().is_none_or(str::is_empty)
                    {
                        return Err(std::io::Error::other(format!("actual startup CAS did not complete: {cas}")));
                    }
                }
                if prepare["if_none_match"] != "*"
                    || !prepare["if_match"].is_null()
                    || commit["if_match"] != prepare["etag"]
                    || !commit["if_none_match"].is_null()
                    || prepare["etag"] == commit["etag"]
                    || prepare["payload_sha256"] == commit["payload_sha256"]
                {
                    return Err(std::io::Error::other(
                        "actual prepare/commit conditional revisions do not form the fresh CAS chain",
                    ));
                }
                let confirmed = source.iter().find(|r| {
                    r["kind"] == "confirmed"
                        && r["payload_sha256"] == commit["payload_sha256"]
                        && r["generation"].as_u64().is_some_and(|g| g > 0)
                });
                if confirmed.is_none() {
                    return Err(std::io::Error::other("actual quorum reload did not confirm the committed payload"));
                }
                for node in 1..4 {
                    let mut accepted = Vec::new();
                    for cas in [prepare, commit] {
                        let matching: Vec<_> = events[node]
                            .iter()
                            .filter(|r| {
                                r["kind"] == "receiver"
                                    && r["dst_volume"] == ".rustfs.sys"
                                    && r["dst_path"] == "pool.bin"
                                    && r["etag"] == cas["etag"]
                            })
                            .collect();
                        if matching.len() != 1 {
                            break;
                        }
                        let received = *matching[0];
                        if received["ok"] != true
                            || received["target"] != "bootstrap"
                            || received["disk"] != endpoints[node]
                            || received["body_sha256"].as_str().is_none_or(|hash| hash.len() != 64)
                            || startup_cas_remote_matches(&logs[0], received) != 1
                        {
                            break;
                        }
                        accepted.push(received);
                    }
                    if accepted.len() == 2 {
                        let raw = std::fs::read(disks[node].join(".rustfs.sys/pool.bin/xl.meta"))?;
                        std::fs::write(artifact.join(format!("node-{node}-committed-xl.meta")), &raw)?;
                        let file_info = rustfs_filemeta::get_file_info(
                            &raw,
                            ".rustfs.sys",
                            "pool.bin",
                            "",
                            rustfs_filemeta::FileInfoOpts {
                                data: false,
                                include_free_versions: false,
                                include_part_checksums: false,
                            },
                        )
                        .map_err(std::io::Error::other)?;
                        if raw.is_empty()
                            || file_info.metadata.get("etag").map(String::as_str) != commit["etag"].as_str()
                            || file_info
                                .mod_time
                                .map(|time| time.unix_timestamp_nanos().to_string())
                                .as_deref()
                                != accepted[1]["mod_time"].as_str()
                            || accepted[1]["mod_time"].is_null()
                        {
                            return Err(std::io::Error::other(
                                "latest physical target metadata does not match the accepted commit",
                            ));
                        }
                        std::fs::write(
                            artifact.join("cas-proof.json"),
                            serde_json::to_vec_pretty(&serde_json::json!({
                                "sender": 0, "receiver": node, "prepare": prepare, "commit": commit, "accepted": accepted, "confirmed": confirmed,
                            }))?,
                        )?;
                        return Ok(());
                    }
                }
                // A started trace may still be flushing asynchronously. Keep
                // waiting for its real tuple; the enclosing deadline is finite.
            }
            sleep(Duration::from_millis(25)).await;
        }
    }
}
