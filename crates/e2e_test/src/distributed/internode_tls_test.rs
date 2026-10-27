// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::harness::{DistCluster, DistLayout, TestResult, unique_bucket};
use crate::tls_gen;
use aws_sdk_s3::operation::RequestId;
use aws_sdk_s3::{Client, config::retry::RetryConfig, primitives::ByteStream};
use rand::{Rng, SeedableRng, rngs::StdRng};
use sha2::{Digest, Sha256};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, TryLockError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::task::JoinSet;

const MIB: u64 = 1024 * 1024;
const READ_TIMEOUT: Duration = Duration::from_secs(180);

struct ExpectedRead {
    range: Option<(u64, u64)>,
    length: u64,
    sha256: String,
}

struct ObjectFixture {
    key: String,
    path: PathBuf,
    length: u64,
    reads: Vec<ExpectedRead>,
}

#[derive(Clone)]
struct ConcurrentReadStream {
    stage: &'static str,
    started_ms: Option<u64>,
    headers_ms: Option<u64>,
    body_started_ms: Option<u64>,
    completed_ms: Option<u64>,
    received: u64,
    last_progress_ms: Option<u64>,
    request_id: Option<uuid::Uuid>,
    terminal: Option<&'static str>,
    terminal_ms: Option<u64>,
}

impl Default for ConcurrentReadStream {
    fn default() -> Self {
        Self {
            stage: "queued",
            started_ms: None,
            headers_ms: None,
            body_started_ms: None,
            completed_ms: None,
            received: 0,
            last_progress_ms: None,
            request_id: None,
            terminal: None,
            terminal_ms: None,
        }
    }
}

struct ConcurrentReadProgress {
    started: Instant,
    started_clock: ClockSample,
    expected: u64,
    streams: Vec<Mutex<ConcurrentReadStream>>,
}

#[derive(Clone, Copy)]
struct ClockSample {
    unix_ms: Option<u64>,
    before_ms: Option<u64>,
    after_ms: Option<u64>,
}

impl ClockSample {
    fn capture(started: Instant) -> Self {
        let before_ms = elapsed_ms(started);
        Self::capture_after(started, before_ms)
    }

    fn capture_after(started: Instant, before_ms: Option<u64>) -> Self {
        let unix_ms = unix_ms_at(SystemTime::now());
        let after_ms = elapsed_ms(started);
        Self {
            unix_ms,
            before_ms,
            after_ms,
        }
    }

    fn json(self) -> serde_json::Value {
        serde_json::json!({
            "unix_ms": self.unix_ms,
            "monotonic_before_ms": self.before_ms,
            "monotonic_after_ms": self.after_ms,
        })
    }
}

impl ConcurrentReadProgress {
    fn new(expected: u64, count: usize) -> Self {
        let started = Instant::now();
        Self {
            started,
            started_clock: ClockSample::capture(started),
            expected,
            streams: (0..count).map(|_| Mutex::new(ConcurrentReadStream::default())).collect(),
        }
    }

    fn elapsed_ms(&self) -> Option<u64> {
        elapsed_ms(self.started)
    }

    fn start(&self, node: usize) {
        let mut stream = self.streams[node].lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        stream.started_ms = self.elapsed_ms();
        stream.stage = "waiting_headers";
    }

    fn response(&self, node: usize, request_id: Option<&str>, has_headers: bool) {
        let mut stream = self.streams[node].lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(request_id) = request_id.and_then(|id| uuid::Uuid::parse_str(id).ok()) {
            stream.request_id = Some(request_id);
        }
        if has_headers {
            stream.headers_ms = self.elapsed_ms();
            stream.stage = "headers_received";
        }
    }

    fn body(&self, node: usize) {
        let mut stream = self.streams[node].lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        stream.body_started_ms = self.elapsed_ms();
        stream.stage = "reading_body";
    }

    fn progress(&self, node: usize, received: u64) {
        let mut stream = self.streams[node].lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        stream.last_progress_ms = self.elapsed_ms();
        stream.received = received;
    }

    fn complete(&self, node: usize) {
        let mut stream = self.streams[node].lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        stream.completed_ms = self.elapsed_ms();
        stream.stage = "complete";
    }

    fn terminal(&self, node: usize, terminal: &'static str) {
        let mut stream = self.streams[node].lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        stream.terminal = Some(terminal);
        stream.terminal_ms = self.elapsed_ms();
    }

    fn failure_snapshot(&self, failed_node: Option<usize>, kind: &'static str) -> serde_json::Value {
        let streams: Vec<_> = self
            .streams
            .iter()
            .enumerate()
            .map(|(node, stream)| {
                // Avoid waiting on another task while retaining the first error.
                let mut snapshot = None;
                let mut unavailable = "busy";
                for _ in 0..8 {
                    let before_ms = self.elapsed_ms();
                    match stream.try_lock() {
                        Ok(guard) => {
                            let stream = guard.clone();
                            drop(guard);
                            snapshot = Some((stream, ClockSample::capture_after(self.started, before_ms)));
                            break;
                        }
                        Err(TryLockError::WouldBlock) => std::hint::spin_loop(),
                        Err(TryLockError::Poisoned(_)) => {
                            unavailable = "poisoned";
                            break;
                        }
                    }
                }
                let Some((stream, sampled_clock)) = snapshot else {
                    return serde_json::json!({
                        "node": node,
                        "snapshot": unavailable,
                        "sampled_clock": ClockSample::capture(self.started).json(),
                        "last_stage": "unknown",
                        "terminal": "unknown",
                        "request_id": null,
                        "received_bytes": null,
                        "last_progress_ms": null,
                    });
                };
                serde_json::json!({
                    "node": node,
                    "snapshot": "available",
                    "sampled_clock": sampled_clock.json(),
                    "last_stage": stream.stage,
                    "request_id": stream.request_id.map(|id| id.to_string()),
                    "started_ms": stream.started_ms,
                    "headers_ms": stream.headers_ms,
                    "body_started_ms": stream.body_started_ms,
                    "completed_ms": stream.completed_ms,
                    "received_bytes": stream.received,
                    "last_progress_ms": stream.last_progress_ms,
                    "terminal": stream.terminal.unwrap_or("unknown"),
                    "terminal_ms": stream.terminal_ms,
                })
            })
            .collect();
        serde_json::json!({
            "kind": kind,
            "failed_node": failed_node,
            "elapsed_ms": self.elapsed_ms(),
            "stage_start_clock_sample": self.started_clock.json(),
            "clock_scope": "client_unsynchronized",
            "expected_bytes": self.expected,
            "streams": streams,
        })
    }

    fn report_failure(&self, failed_node: Option<usize>, kind: &'static str) {
        eprintln!("TLS acceptance: concurrent GET failure: {}", self.failure_snapshot(failed_node, kind));
    }
}

fn elapsed_ms(started: Instant) -> Option<u64> {
    u64::try_from(started.elapsed().as_millis()).ok()
}

fn unix_ms_at(time: SystemTime) -> Option<u64> {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
}

fn verify_concurrent_read_failure_snapshot_evidence() {
    let progress = ConcurrentReadProgress::new(100, 4);
    progress.start(0);
    progress.response(0, Some("LEAK-invalid-request-id"), true);
    progress.body(0);
    progress.progress(0, 100);
    progress.complete(0);
    progress.terminal(0, "success");

    let valid_id = "b8172a15-a428-4b42-9cef-1f4a7b96874e";
    progress.start(1);
    progress.response(1, Some(valid_id), true);
    progress.body(1);
    progress.progress(1, 41);
    progress.terminal(1, "failed");

    progress.start(3);
    let _busy = progress.streams[2].lock().expect("hold one stream during the snapshot");
    let snapshot = progress.failure_snapshot(Some(1), "read_failed");
    assert_eq!(snapshot["clock_scope"], "client_unsynchronized");
    assert_eq!(snapshot["failed_node"], 1);
    let start_clock = &snapshot["stage_start_clock_sample"];
    assert!(
        start_clock["monotonic_before_ms"].as_u64().expect("start sample lower bound")
            <= start_clock["monotonic_after_ms"].as_u64().expect("start sample upper bound")
    );
    assert!(!snapshot.to_string().contains("LEAK-invalid-request-id"));

    let streams = snapshot["streams"].as_array().expect("per-stream snapshots");
    assert_eq!(streams.len(), 4);
    for (stream, terminal, expected_bytes) in [(&streams[0], "success", 100), (&streams[1], "failed", 41)] {
        assert_eq!(stream["snapshot"], "available");
        assert_eq!(stream["terminal"], terminal);
        assert_eq!(stream["received_bytes"], expected_bytes);
        let times = [
            "started_ms",
            "headers_ms",
            "body_started_ms",
            "last_progress_ms",
            "terminal_ms",
        ]
        .map(|field| stream[field].as_u64().expect("observed stream time"));
        assert!(times.windows(2).all(|pair| pair[0] <= pair[1]));
        let clock = &stream["sampled_clock"];
        let before = clock["monotonic_before_ms"].as_u64().expect("sample lower bound");
        let after = clock["monotonic_after_ms"].as_u64().expect("sample upper bound");
        assert!(times[4] <= after && before <= after);
    }
    assert_eq!(streams[0]["last_stage"], "complete");
    assert!(streams[0]["request_id"].is_null());
    assert!(
        streams[0]["completed_ms"].as_u64().expect("completion time")
            <= streams[0]["terminal_ms"].as_u64().expect("terminal time")
    );
    assert_eq!(streams[1]["last_stage"], "reading_body");
    assert!(streams[1]["completed_ms"].is_null());
    assert_eq!(streams[1]["request_id"], valid_id);
    assert_eq!(streams[2]["snapshot"], "busy");
    assert_eq!(streams[2]["last_stage"], "unknown");
    assert_eq!(streams[2]["terminal"], "unknown");
    assert!(streams[2]["received_bytes"].is_null());
    assert!(streams[2]["sampled_clock"]["monotonic_before_ms"].is_number());
    assert_eq!(streams[3]["last_stage"], "waiting_headers");
    assert_eq!(streams[3]["terminal"], "unknown");
    assert!(streams[3]["request_id"].is_null());
    assert!(streams[3]["last_progress_ms"].is_null());

    let before_epoch = UNIX_EPOCH
        .checked_sub(Duration::from_millis(1))
        .expect("represent pre-epoch time");
    assert!(unix_ms_at(before_epoch).is_none());
}

fn fixture(directory: &Path, length: u64) -> TestResult<ObjectFixture> {
    let key = format!("object-{length}");
    let path = directory.join(&key);
    let mut file = std::fs::File::create(&path)?;
    let mut rng = StdRng::seed_from_u64(length);
    let mut block = vec![0; usize::try_from(MIB)?];
    let mut remaining = length;
    let mut digest = Sha256::new();
    while remaining != 0 {
        let count = usize::try_from(remaining.min(MIB))?;
        rng.fill_bytes(&mut block[..count]);
        file.write_all(&block[..count])?;
        digest.update(&block[..count]);
        remaining -= u64::try_from(count)?;
    }
    drop(file);
    let mut reads = vec![ExpectedRead {
        range: None,
        length,
        sha256: hex_simd::encode_to_string(digest.finalize(), hex_simd::AsciiCase::Lower),
    }];
    // Unaligned ranges cross TLS frames and EC blocks; the tail detects
    // truncation and incorrect range offsets independently of a full GET.
    for (start, end) in [(17, (MIB + 31).min(length - 1)), (length.saturating_sub(65537), length - 1)] {
        let mut input = std::fs::File::open(&path)?;
        input.seek(SeekFrom::Start(start))?;
        let range_length = end - start + 1;
        let mut hasher = Sha256::new();
        let mut input = input.take(range_length);
        loop {
            let count = input.read(&mut block)?;
            if count == 0 {
                break;
            }
            hasher.update(&block[..count]);
        }
        reads.push(ExpectedRead {
            range: Some((start, end)),
            length: range_length,
            sha256: hex_simd::encode_to_string(hasher.finalize(), hex_simd::AsciiCase::Lower),
        });
    }
    Ok(ObjectFixture {
        key,
        path,
        length,
        reads,
    })
}

async fn verified_get(
    client: &Client,
    node: usize,
    bucket: &str,
    object: &ObjectFixture,
    expected: &ExpectedRead,
    concurrent: bool,
    progress: Option<&ConcurrentReadProgress>,
) -> TestResult<serde_json::Value> {
    let started = Instant::now();
    let mut received = 0u64;
    let mut stage = "response headers";
    if let Some(progress) = progress {
        progress.start(node);
    }
    tokio::time::timeout(READ_TIMEOUT, async {
        let mut request = client.get_object().bucket(bucket).key(&object.key);
        if let Some((start, end)) = expected.range {
            request = request.range(format!("bytes={start}-{end}"));
        }
        let response = request.send().await;
        if let Some(progress) = progress {
            let has_headers = match &response {
                Ok(_) => true,
                Err(error) => error.raw_response().is_some(),
            };
            progress.response(node, response.request_id(), has_headers);
        }
        let mut output = response?;
        assert_eq!(output.content_length(), Some(i64::try_from(expected.length)?));
        if let Some((start, end)) = expected.range {
            assert_eq!(output.content_range(), Some(format!("bytes {start}-{end}/{}", object.length).as_str()));
        } else {
            assert_eq!(output.content_range(), None);
        }
        stage = "response body";
        if let Some(progress) = progress {
            progress.body(node);
        }
        let mut digest = Sha256::new();
        while let Some(chunk) = output.body.next().await {
            let chunk = chunk.inspect_err(|_| {
                eprintln!(
                    "TLS acceptance: GET body failed: node={node}, key={}, range={:?}, concurrent={concurrent}, received={received}/{}, elapsed={:?}",
                    object.key, expected.range, expected.length, started.elapsed()
                );
            })?;
            received += u64::try_from(chunk.len())?;
            if !chunk.is_empty()
                && let Some(progress) = progress
            {
                progress.progress(node, received);
            }
            assert!(received <= expected.length, "node {node}: GET exceeded the requested length");
            digest.update(&chunk);
        }
        let actual_sha256 = hex_simd::encode_to_string(digest.finalize(), hex_simd::AsciiCase::Lower);
        assert_eq!(received, expected.length, "node {node}: truncated GET {}", object.key);
        assert_eq!(actual_sha256, expected.sha256, "node {node}: corrupt GET {}", object.key);
        if let Some(progress) = progress {
            progress.complete(node);
        }
        eprintln!(
            "TLS acceptance: GET node={node}, key={}, range={:?}, concurrent={concurrent}, bytes={received}, elapsed={:?}",
            object.key, expected.range, started.elapsed()
        );
        Ok(serde_json::json!({
            "operation": "get", "node": node, "key": object.key,
            "range": expected.range, "bytes": received, "sha256": actual_sha256,
            "concurrent": concurrent, "elapsed_ms": started.elapsed().as_secs_f64() * 1000.0,
        }))
    })
    .await
    .map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!(
                "TLS GET timed out: node={node}, key={}, range={:?}, concurrent={concurrent}, stage={stage}, received={received}/{}, elapsed={:?}: {error}",
                object.key, expected.range, expected.length, started.elapsed()
            ),
        )
    })?
}

/// This case always exercises the full acceptance sizes; do not substitute
/// tiny objects and count that as a large distributed-read pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn four_node_tls_large_and_range_gets_preserve_bytes() -> TestResult {
    verify_concurrent_read_failure_snapshot_evidence();
    let mode = std::env::var("RUSTFS_E2E_INTERNODE_HTTP_MODE").unwrap_or_else(|_| "http1".into());
    let protocol = match mode.as_str() {
        "http1" => "http1",
        "auto" | "unpooled" => "auto",
        _ => return Err("RUSTFS_E2E_INTERNODE_HTTP_MODE must be http1, auto or unpooled".into()),
    };
    let mut dist = DistCluster::new_stopped_with_env(
        DistLayout::FourByFour,
        &[
            ("RUSTFS_INTERNODE_HTTP_PROTOCOL", protocol),
            ("RUSTFS_SCANNER_ENABLED", "true"),
            ("RUSTFS_SCANNER_START_DELAY_SECS", "0"),
            ("RUSTFS_SCANNER_CYCLE", "1"),
            ("RUSTFS_HEAL_ENABLED", "true"),
            ("RUST_LOG", "warn"),
        ],
    )
    .await?;
    if mode == "unpooled" {
        dist.cluster.set_env("RUSTFS_INTERNODE_HTTP_POOL_MAX_IDLE_PER_HOST", "0");
    }
    let directory = PathBuf::from(&dist.cluster.temp_dir);
    let tls_directory = directory.join("tls");
    tls_gen::run(tls_gen::Args {
        out_dir: tls_directory.clone(),
        days: 1,
        force: false,
    })?;
    dist.cluster.enable_tls(&tls_directory).await?;
    let report_path = std::env::var_os("RUSTFS_INTERNODE_ACCEPTANCE_REPORT").map(PathBuf::from);
    if let Some(path) = &report_path
        && (path.exists() || path.with_extension("pids.json").exists())
    {
        return Err("acceptance report paths must be unused".into());
    }
    let log_directory = report_path.as_ref().map_or_else(
        || {
            std::env::var_os("RUSTFS_E2E_LOG_DIR")
                .map_or_else(|| directory.join("logs"), |root| PathBuf::from(root).join(unique_bucket("internode-tls")))
        },
        |path| path.with_extension("logs"),
    );
    std::fs::create_dir(&log_directory)?;
    dist.capture_logs_under(&log_directory)?;
    dist.cluster.start().await?;
    let clients = dist
        .clients()?
        .into_iter()
        .map(|client| Client::from_conf(client.config().to_builder().retry_config(RetryConfig::disabled()).build()))
        .collect::<Vec<_>>();
    let bucket = unique_bucket("internode-tls");
    clients[0].create_bucket().bucket(&bucket).send().await?;
    let pids = dist
        .cluster
        .nodes
        .iter()
        .map(|node| node.process.as_ref().map(std::process::Child::id))
        .collect::<Vec<_>>();
    if let Some(path) = &report_path {
        std::fs::write(path.with_extension("pids.json"), serde_json::to_vec(&pids)?)?;
    }

    let mut results = Vec::new();
    for length in [32 * 1024, 100 * MIB, 1024 * MIB] {
        let fixture_directory = directory.clone();
        let object = Arc::new(tokio::task::spawn_blocking(move || fixture(&fixture_directory, length)).await??);
        eprintln!("TLS acceptance: fixture key={}, bytes={length}", object.key);
        let started = Instant::now();
        tokio::time::timeout(
            READ_TIMEOUT,
            clients[0]
                .put_object()
                .bucket(&bucket)
                .key(&object.key)
                .body(ByteStream::from_path(&object.path).await?)
                .send(),
        )
        .await
        .map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!(
                    "TLS PUT timed out: key={}, bytes={length}, elapsed={:?}: {error}",
                    object.key,
                    started.elapsed()
                ),
            )
        })??;
        eprintln!("TLS acceptance: PUT key={}, bytes={length}, elapsed={:?}", object.key, started.elapsed());
        results.push(
            serde_json::json!({"operation": "put", "bytes": length, "elapsed_ms": started.elapsed().as_secs_f64() * 1000.0}),
        );
        if length == 32 * 1024 {
            let alternate = dist.client_with_credentials(0, &dist.cluster.access_key, &dist.cluster.secret_key)?;
            let alternate = Client::from_conf(alternate.config().to_builder().retry_config(RetryConfig::disabled()).build());
            results.push(verified_get(&alternate, 0, &bucket, &object, &object.reads[0], false, None).await?);
            let invalid = dist.client_with_credentials(0, "invalid-tls-user", "invalid-tls-secret-password")?;
            let invalid = Client::from_conf(invalid.config().to_builder().retry_config(RetryConfig::disabled()).build());
            let denied = invalid
                .list_buckets()
                .send()
                .await
                .expect_err("alternate credentials must not inherit the root identity");
            assert_eq!(denied.raw_response().map(|response| response.status().as_u16()), Some(403));
        }
        for expected in &object.reads {
            for (node, client) in clients.iter().enumerate() {
                results.push(verified_get(client, node, &bucket, &object, expected, false, None).await?);
            }
        }
        // Each reader owns a Tokio task and its cooperative budget. Polling
        // every body and synchronous digest in one parent task can let an
        // always-ready reader starve its siblings and their deadlines.
        let mut readers = JoinSet::new();
        let progress = Arc::new(ConcurrentReadProgress::new(object.reads[0].length, clients.len()));
        let mut task_nodes = Vec::with_capacity(clients.len());
        for (node, client) in clients.iter().enumerate() {
            let client = client.clone();
            let bucket = bucket.clone();
            let object = Arc::clone(&object);
            let progress = Arc::clone(&progress);
            let handle = readers.spawn(async move {
                let result = verified_get(&client, node, &bucket, &object, &object.reads[0], true, Some(&progress)).await;
                progress.terminal(node, if result.is_ok() { "success" } else { "failed" });
                (node, result)
            });
            task_nodes.push((handle.id(), node));
        }
        while let Some(result) = readers.join_next().await {
            match result {
                Ok((_, Ok(result))) => results.push(result),
                Ok((node, Err(error))) => {
                    readers.abort_all();
                    progress.report_failure(Some(node), "read_failed");
                    return Err(error);
                }
                Err(error) => {
                    let node = task_nodes.iter().find(|(id, _)| *id == error.id()).map(|(_, node)| *node);
                    readers.abort_all();
                    if let Some(node) = node {
                        progress.terminal(node, if error.is_panic() { "panicked" } else { "cancelled" });
                    }
                    progress.report_failure(node, "task_failed");
                    return Err(error.into());
                }
            }
        }
        tokio::fs::remove_file(&object.path).await?;
    }
    for node in 0..clients.len() {
        let log = tokio::fs::read_to_string(log_directory.join(format!("node-{node}.log"))).await?;
        assert!(
            !log.contains("internode body stalled"),
            "node {node}: internode body stall during verified reads"
        );
    }
    let report = serde_json::json!({
        "schema": 1, "mode": mode, "tls_verified": true, "nodes": 4, "drives_per_node": 4,
        "source_revision": env!("RUSTFS_E2E_BUILD_COMMIT"),
        "source_dirty": env!("RUSTFS_E2E_BUILD_DIRTY"),
        "scanner_enabled": true, "heal_enabled": true,
        "scanner_progress_verified": false, "heal_progress_verified": false,
        "results": results,
    });
    if let Some(path) = report_path {
        std::fs::write(path, serde_json::to_vec_pretty(&report)?)?;
    }
    eprintln!("internode_tls_acceptance={report}");
    Ok(())
}
