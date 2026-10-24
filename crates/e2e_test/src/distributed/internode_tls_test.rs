// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::harness::{DistCluster, DistLayout, TestResult, unique_bucket};
use crate::tls_gen;
use aws_sdk_s3::{Client, config::retry::RetryConfig, primitives::ByteStream};
use futures::future::try_join_all;
use rand::{Rng, SeedableRng, rngs::StdRng};
use sha2::{Digest, Sha256};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

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
) -> TestResult<serde_json::Value> {
    tokio::time::timeout(READ_TIMEOUT, async {
        let started = Instant::now();
        let mut request = client.get_object().bucket(bucket).key(&object.key);
        if let Some((start, end)) = expected.range {
            request = request.range(format!("bytes={start}-{end}"));
        }
        let mut output = request.send().await?;
        assert_eq!(output.content_length(), Some(i64::try_from(expected.length)?));
        if let Some((start, end)) = expected.range {
            assert_eq!(output.content_range(), Some(format!("bytes {start}-{end}/{}", object.length).as_str()));
        } else {
            assert_eq!(output.content_range(), None);
        }
        let mut count = 0u64;
        let mut digest = Sha256::new();
        while let Some(chunk) = output.body.next().await {
            let chunk = chunk?;
            count += u64::try_from(chunk.len())?;
            assert!(count <= expected.length, "node {node}: GET exceeded the requested length");
            digest.update(&chunk);
        }
        let actual_sha256 = hex_simd::encode_to_string(digest.finalize(), hex_simd::AsciiCase::Lower);
        assert_eq!(count, expected.length, "node {node}: truncated GET {}", object.key);
        assert_eq!(actual_sha256, expected.sha256, "node {node}: corrupt GET {}", object.key);
        Ok(serde_json::json!({
            "operation": "get", "node": node, "key": object.key,
            "range": expected.range, "bytes": count, "sha256": actual_sha256,
            "concurrent": concurrent, "elapsed_ms": started.elapsed().as_secs_f64() * 1000.0,
        }))
    })
    .await?
}

/// This case always exercises the full acceptance sizes; do not substitute
/// tiny objects and count that as a large distributed-read pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn four_node_tls_large_and_range_gets_preserve_bytes() -> TestResult {
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
    if let Some(path) = &report_path {
        if path.exists() || path.with_extension("pids.json").exists() {
            return Err("acceptance report paths must be unused".into());
        }
    }
    let log_directory = report_path
        .as_ref()
        .map_or_else(|| directory.join("logs"), |path| path.with_extension("logs"));
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
        let object = tokio::task::spawn_blocking(move || fixture(&fixture_directory, length)).await??;
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
        .await??;
        results.push(
            serde_json::json!({"operation": "put", "bytes": length, "elapsed_ms": started.elapsed().as_secs_f64() * 1000.0}),
        );
        if length == 32 * 1024 {
            let alternate = dist.client_with_credentials(0, &dist.cluster.access_key, &dist.cluster.secret_key)?;
            let alternate = Client::from_conf(alternate.config().to_builder().retry_config(RetryConfig::disabled()).build());
            results.push(verified_get(&alternate, 0, &bucket, &object, &object.reads[0], false).await?);
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
                results.push(verified_get(client, node, &bucket, &object, expected, false).await?);
            }
        }
        results.extend(
            try_join_all(
                clients
                    .iter()
                    .enumerate()
                    .map(|(node, client)| verified_get(client, node, &bucket, &object, &object.reads[0], true)),
            )
            .await?,
        );
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
