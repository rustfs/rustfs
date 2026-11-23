// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::{Duration, SystemTime};

use aws_sdk_s3::{Client, primitives::ByteStream};
use futures::{StreamExt, TryStreamExt, stream};
use rustfs_data_usage::{BucketUsageValueState, DataUsageInfo};
use serde::{Deserialize, de::DeserializeOwned};
use tokio::time::{Instant, sleep, timeout};

use crate::common::{RustFSTestEnvironment, signed_request};
use crate::storage_api::scanner_peer_bucket::{BUCKET_META_PREFIX, RUSTFS_META_BUCKET};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const BUCKET: &str = "wide-directory-usage";
const WIDTH: usize = 512;
// The per-disk token bucket allows an initial burst of one second. Even the
// first pass can read at most (1 + 3) * 96 = 384 of these 512 root entries.
const SCAN_ENV: &[(&str, &str)] = &[
    ("RUST_LOG", "warn,rustfs::scanner::io=debug,rustfs::scanner::data_usage=debug"),
    ("RUSTFS_SCANNER_ENABLED", "true"),
    ("RUSTFS_SCANNER_START_DELAY_SECS", "0"),
    ("RUSTFS_SCANNER_CYCLE", "1"),
    ("RUSTFS_SCANNER_CYCLE_MAX_DURATION_SECS", "3"),
    ("RUSTFS_SCANNER_SPEED", "fastest"),
    ("RUSTFS_SCANNER_DISK_IO_BUDGET_IOPS", "96"),
    ("RUSTFS_SCANNER_DISK_IO_BUDGET_BYTES_PER_SEC", "off"),
];

#[derive(Debug, Deserialize)]
struct Status {
    metrics: Metrics,
    disk_io_budget: Vec<DiskBudget>,
}

#[derive(Debug, Deserialize)]
struct Metrics {
    current_cycle_active: bool,
    active_paths: Vec<String>,
    cycle_max_duration_seconds: f64,
    partial_cycles_runtime: u64,
}

#[derive(Debug, Deserialize)]
struct DiskBudget {
    iops: Option<u64>,
}

async fn admin<T: DeserializeOwned>(env: &RustFSTestEnvironment, resource: &str) -> TestResult<T> {
    let url = format!("{}/rustfs/admin/v3/{resource}", env.url);
    let response = signed_request(http::Method::GET, &url, &env.access_key, &env.secret_key, None, None).await?;
    Ok(response.error_for_status()?.json().await?)
}

async fn inventory(client: &Client) -> TestResult<BTreeMap<String, u64>> {
    let mut pages = client.list_objects_v2().bucket(BUCKET).max_keys(37).into_paginator().send();
    let mut sizes = BTreeMap::new();
    while let Some(page) = pages.next().await {
        for object in page?.contents() {
            let key = object.key().ok_or("S3 inventory omitted object key")?;
            let size = u64::try_from(object.size().ok_or("S3 inventory omitted object size")?)?;
            assert!(sizes.insert(key.to_owned(), size).is_none(), "duplicate inventory key {key}");
        }
    }
    Ok(sizes)
}

async fn scan_status(env: &RustFSTestEnvironment) -> TestResult<Status> {
    let status: Status = admin(env, "scanner/status").await?;
    assert_eq!(status.metrics.cycle_max_duration_seconds, 3.0, "effective cycle budget");
    assert!(status.disk_io_budget.iter().all(|disk| disk.iops == Some(96)), "effective disk budget");
    Ok(status)
}

#[tokio::test(flavor = "multi_thread")]
async fn certifies_wide_directory_after_mid_scan_process_restart() -> TestResult {
    let mut env = RustFSTestEnvironment::new().await?;
    let temporary_log = tempfile::NamedTempFile::new()?;
    let server_log = env
        .capture_log_path
        .get_or_insert_with(|| temporary_log.path().display().to_string())
        .clone();
    let bucket_metadata = Path::new(&env.temp_dir)
        .join(RUSTFS_META_BUCKET)
        .join(BUCKET_META_PREFIX)
        .join(BUCKET);
    // Seed a cold bucket without allowing any earlier, larger-budget sweep to count it.
    env.start_rustfs_server_without_cleanup_with_env(&[("RUSTFS_SCANNER_ENABLED", "false")])
        .await?;
    let client = env.create_s3_client();
    client.create_bucket().bucket(BUCKET).send().await?;
    stream::iter(0..WIDTH)
        .map(Ok)
        .try_for_each_concurrent(12, |i| {
            let client = &client;
            async move {
                client
                    .put_object()
                    .bucket(BUCKET)
                    .key(format!("object-{i:064x}"))
                    .body(ByteStream::from(vec![42; 1 + i % 67]))
                    .send()
                    .await
                    .map(|_| ())
            }
        })
        .await?;
    let expected = inventory(&client).await?;
    assert_eq!(expected.len(), WIDTH);

    env.restart_server_preserving_data(vec![], SCAN_ENV).await?;
    let first_pid = env.process.as_ref().ok_or("missing scanner process")?.id();
    timeout(Duration::from_secs(60), async {
        loop {
            let status = scan_status(&env).await?;
            if status.metrics.partial_cycles_runtime >= 1
                && bucket_metadata.join(".usage-cache.bin").join("xl.meta").is_file()
                && bucket_metadata.join(".usage-cache.bin.pages").is_dir()
                && status.metrics.current_cycle_active
                && status
                    .metrics
                    .active_paths
                    .iter()
                    .any(|path| path.split('/').any(|part| part == BUCKET))
            {
                let log = tokio::fs::read_to_string(&server_log).await?;
                if !log
                    .lines()
                    .any(|line| line.contains("partial_saved_not_published") && line.contains(BUCKET))
                {
                    sleep(Duration::from_millis(200)).await;
                    continue;
                }
                assert!(!status.disk_io_budget.is_empty(), "scanner never admitted disk I/O");
                break Ok::<_, Box<dyn std::error::Error + Send + Sync>>(());
            }
            sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .map_err(|_| "scanner did not re-enter the bucket after exhausting the fixed runtime budget")??;
    let partial: DataUsageInfo = admin(&env, "datausageinfo").await?;
    assert!(
        partial
            .buckets_usage
            .get(BUCKET)
            .is_none_or(|value| value.usage_certification.is_none()),
        "the first process certified the bucket before the injected crash"
    );

    env.stop_server();
    // The terminal save has returned and the old process can no longer write.
    assert!(
        bucket_metadata.join(".usage-cache.bin").join("xl.meta").is_file()
            && bucket_metadata.join(".usage-cache.bin.pages").is_dir(),
        "the interrupted process must have committed a paged checkpoint"
    );
    let restart_log_offset = usize::try_from(tokio::fs::metadata(&server_log).await?.len())?;
    let restarted_at = SystemTime::now();
    env.start_rustfs_server_without_cleanup_with_env(SCAN_ENV).await?;
    let second_pid = env.process.as_ref().ok_or("missing restarted scanner process")?.id();
    assert_ne!(first_pid, second_pid);
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        let usage: DataUsageInfo = admin(&env, "datausageinfo").await?;
        let status = scan_status(&env).await?;
        if let Some(bucket) = usage.buckets_usage.get(BUCKET)
            && bucket.usage_value_state == Some(BucketUsageValueState::Certified)
        {
            let log = tokio::fs::read_to_string(&server_log).await?;
            let first_admission = log
                .get(restart_log_offset..)
                .ok_or("server log truncated across restart")?
                .lines()
                .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
                .find(|record| record["state"] == "bucket_checkpoint_prepared" && record["bucket"] == BUCKET)
                .ok_or("no checkpoint admission in the restarted process")?;
            assert_eq!(first_admission["resume_position"], true, "restarted scanner discarded the checkpoint");
            assert!(usage.is_complete_bucket_usage_snapshot());
            let certificate = bucket
                .usage_certification
                .ok_or("missing certificate for the measured bucket")?;
            assert!(certificate.certified_at >= restarted_at, "stale pre-restart certificate");
            assert!(status.metrics.partial_cycles_runtime > 0, "new process must cross a cycle deadline");
            assert!(!status.disk_io_budget.is_empty(), "restarted scanner never admitted disk I/O");
            assert_eq!(inventory(&client).await?, expected, "durable S3 inventory changed across restart");
            let count = u64::try_from(expected.len())?;
            let bytes = expected.values().sum::<u64>();
            assert_eq!((bucket.objects_count, bucket.size), (count, bytes));
            assert_eq!((usage.objects_total_count, usage.objects_total_size), (count, bytes));
            eprintln!(
                "PIDs {first_pid}/{second_pid}, runtime-limited cycles since restart {}, certificate epoch {} cycle {}, S3 and admin {} objects/{} bytes",
                status.metrics.partial_cycles_runtime, certificate.epoch, certificate.cycle, count, bytes
            );
            return Ok(());
        }
        assert!(
            Instant::now() < deadline,
            "no certified bucket after restart: {status:?}, value={:?}",
            usage.buckets_usage.get(BUCKET)
        );
        sleep(Duration::from_millis(500)).await;
    }
}
