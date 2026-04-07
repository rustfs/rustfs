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

use anyhow::{Context, Result, anyhow, bail};
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use aws_sdk_s3::{Client, Config};
use aws_smithy_http_client::Builder as SmithyHttpClientBuilder;
use bytes::Bytes;
use clap::Parser;
use serde::Serialize;
use std::path::PathBuf;
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(name = "small_put_bench")]
#[command(about = "Rust-native small PUT benchmark for RustFS-compatible S3 endpoints")]
struct Args {
    #[arg(long, env = "RUSTFS_BENCH_ENDPOINT")]
    endpoint: String,

    #[arg(long, env = "RUSTFS_BENCH_ACCESS_KEY", default_value = "rustfsadmin")]
    access_key: String,

    #[arg(long, env = "RUSTFS_BENCH_SECRET_KEY", default_value = "rustfsadmin")]
    secret_key: String,

    #[arg(long, env = "RUSTFS_BENCH_REGION", default_value = "us-east-1")]
    region: String,

    #[arg(long, env = "RUSTFS_BENCH_BUCKET", default_value = "small-put-benchmark")]
    bucket: String,

    #[arg(long, env = "RUSTFS_BENCH_SIZES", default_value = "4KiB,16KiB,64KiB,256KiB,1MiB")]
    sizes: String,

    #[arg(long, env = "RUSTFS_BENCH_CONCURRENCY", default_value_t = 8)]
    concurrency: usize,

    #[arg(long, env = "RUSTFS_BENCH_DURATION_SECS", default_value_t = 10)]
    duration_secs: u64,

    #[arg(long, env = "RUSTFS_BENCH_TIMEOUT_SECS", default_value_t = 15)]
    timeout_secs: u64,

    #[arg(long, env = "RUSTFS_BENCH_PREFIX")]
    prefix: Option<String>,

    #[arg(long)]
    output_json: Option<PathBuf>,

    #[arg(long, default_value_t = false)]
    cleanup: bool,
}

#[derive(Clone, Debug)]
struct SizeSpec {
    label: String,
    slug: String,
    bytes: usize,
}

#[derive(Debug)]
struct Sample {
    ok: bool,
    duration_ms: f64,
}

#[derive(Debug, Serialize)]
struct SizeSummary {
    label: String,
    bytes: usize,
    total: usize,
    succeeded: usize,
    failed: usize,
    wall_secs: f64,
    object_rate: f64,
    throughput_mib_per_sec: f64,
    avg_ms: Option<f64>,
    p50_ms: Option<f64>,
    p90_ms: Option<f64>,
    p99_ms: Option<f64>,
}

#[derive(Debug, Serialize)]
struct RunSummary {
    run_id: String,
    endpoint: String,
    bucket: String,
    concurrency: usize,
    duration_secs: u64,
    timeout_secs: u64,
    sizes: Vec<SizeSummary>,
}

fn main() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to build tokio runtime")?;
    runtime.block_on(async_main())
}

async fn async_main() -> Result<()> {
    let args = Args::parse();
    validate_args(&args)?;

    let sizes = parse_size_list(&args.sizes)?;
    let run_id = args.prefix.clone().unwrap_or_else(default_run_id);
    let client = build_s3_client(&args.endpoint, &args.access_key, &args.secret_key, &args.region);

    ensure_bucket(&client, &args.bucket).await?;

    let mut size_summaries = Vec::with_capacity(sizes.len());
    for size in &sizes {
        let summary = run_size_benchmark(
            client.clone(),
            args.bucket.clone(),
            run_id.clone(),
            size.clone(),
            args.concurrency,
            Duration::from_secs(args.duration_secs),
            Duration::from_secs(args.timeout_secs),
        )
        .await?;
        print_size_summary(&summary);
        size_summaries.push(summary);
    }

    if args.cleanup {
        cleanup_prefix(&client, &args.bucket, &run_id).await?;
    }

    let summary = RunSummary {
        run_id,
        endpoint: args.endpoint,
        bucket: args.bucket,
        concurrency: args.concurrency,
        duration_secs: args.duration_secs,
        timeout_secs: args.timeout_secs,
        sizes: size_summaries,
    };

    if let Some(path) = args.output_json {
        let json = serde_json::to_vec_pretty(&summary).context("failed to serialize benchmark summary")?;
        std::fs::write(&path, json).with_context(|| format!("failed to write benchmark summary to {}", path.display()))?;
        println!("Wrote summary to {}", path.display());
    }

    Ok(())
}

fn validate_args(args: &Args) -> Result<()> {
    if args.concurrency == 0 {
        bail!("--concurrency must be greater than zero");
    }
    if args.duration_secs == 0 {
        bail!("--duration-secs must be greater than zero");
    }
    if args.timeout_secs == 0 {
        bail!("--timeout-secs must be greater than zero");
    }
    Ok(())
}

fn build_s3_client(endpoint: &str, access_key: &str, secret_key: &str, region: &str) -> Client {
    let credentials = Credentials::new(access_key, secret_key, None, None, "small-put-bench");
    let mut config = Config::builder()
        .credentials_provider(credentials)
        .region(Region::new(region.to_string()))
        .endpoint_url(endpoint)
        .force_path_style(true)
        .behavior_version_latest();

    if endpoint.starts_with("http://") {
        config = config.http_client(SmithyHttpClientBuilder::new().build_http());
    }

    Client::from_conf(config.build())
}

async fn ensure_bucket(client: &Client, bucket: &str) -> Result<()> {
    if client.head_bucket().bucket(bucket).send().await.is_ok() {
        return Ok(());
    }

    match client.create_bucket().bucket(bucket).send().await {
        Ok(_) => Ok(()),
        Err(err) => {
            let rendered = err.to_string();
            if rendered.contains("BucketAlreadyOwnedByYou") || rendered.contains("BucketAlreadyExists") {
                Ok(())
            } else {
                Err(err).with_context(|| format!("failed to create benchmark bucket {bucket}"))
            }
        }
    }
}

async fn run_size_benchmark(
    client: Client,
    bucket: String,
    run_id: String,
    size: SizeSpec,
    concurrency: usize,
    duration: Duration,
    timeout: Duration,
) -> Result<SizeSummary> {
    let payload = Bytes::from(vec![0_u8; size.bytes]);
    let deadline = Instant::now() + duration;
    let wall_start = Instant::now();

    let mut handles = Vec::with_capacity(concurrency);
    for worker in 0..concurrency {
        let client = client.clone();
        let bucket = bucket.clone();
        let payload = payload.clone();
        let prefix = format!("{run_id}/{}/worker-{worker}", size.slug);
        handles.push(tokio::spawn(async move {
            let mut samples = Vec::new();
            let mut idx = 0usize;

            while Instant::now() < deadline {
                let key = format!("{prefix}/obj-{idx}.bin");
                let started_at = Instant::now();
                let request = client
                    .put_object()
                    .bucket(&bucket)
                    .key(key)
                    .body(ByteStream::from(payload.clone()))
                    .content_type("application/octet-stream");

                let ok = matches!(tokio::time::timeout(timeout, request.send()).await, Ok(Ok(_)));
                samples.push(Sample {
                    ok,
                    duration_ms: started_at.elapsed().as_secs_f64() * 1000.0,
                });
                idx += 1;
            }

            samples
        }));
    }

    let mut samples = Vec::new();
    for handle in handles {
        samples.extend(handle.await.map_err(|err| anyhow!("benchmark worker join error: {err}"))?);
    }

    Ok(build_size_summary(&size, samples, wall_start.elapsed()))
}

fn build_size_summary(size: &SizeSpec, mut samples: Vec<Sample>, wall_elapsed: Duration) -> SizeSummary {
    let total = samples.len();
    let succeeded = samples.iter().filter(|sample| sample.ok).count();
    let failed = total.saturating_sub(succeeded);
    let wall_secs = wall_elapsed.as_secs_f64();
    let object_rate = if wall_secs > 0.0 { succeeded as f64 / wall_secs } else { 0.0 };
    let throughput_mib_per_sec = if wall_secs > 0.0 {
        ((size.bytes * succeeded) as f64 / (1024.0 * 1024.0)) / wall_secs
    } else {
        0.0
    };

    let avg_ms = if total > 0 {
        Some(samples.iter().map(|sample| sample.duration_ms).sum::<f64>() / total as f64)
    } else {
        None
    };

    samples.sort_by(|lhs, rhs| lhs.duration_ms.total_cmp(&rhs.duration_ms));
    let durations: Vec<f64> = samples.into_iter().map(|sample| sample.duration_ms).collect();

    SizeSummary {
        label: size.label.clone(),
        bytes: size.bytes,
        total,
        succeeded,
        failed,
        wall_secs,
        object_rate,
        throughput_mib_per_sec,
        avg_ms,
        p50_ms: percentile(&durations, 0.50),
        p90_ms: percentile(&durations, 0.90),
        p99_ms: percentile(&durations, 0.99),
    }
}

async fn cleanup_prefix(client: &Client, bucket: &str, prefix: &str) -> Result<()> {
    let mut continuation_token = None;
    loop {
        let response = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .set_continuation_token(continuation_token.clone())
            .send()
            .await
            .with_context(|| format!("failed to list objects for cleanup under {bucket}/{prefix}"))?;

        let objects: Vec<ObjectIdentifier> = response
            .contents
            .unwrap_or_default()
            .into_iter()
            .filter_map(|object| object.key.map(|key| ObjectIdentifier::builder().key(key).build().ok()))
            .flatten()
            .collect();

        for chunk in objects.chunks(1_000) {
            if chunk.is_empty() {
                continue;
            }

            client
                .delete_objects()
                .bucket(bucket)
                .delete(
                    Delete::builder()
                        .set_objects(Some(chunk.to_vec()))
                        .quiet(true)
                        .build()
                        .context("failed to build delete request")?,
                )
                .send()
                .await
                .with_context(|| format!("failed to delete cleanup batch under {bucket}/{prefix}"))?;
        }

        if response.is_truncated.unwrap_or(false) {
            continuation_token = response.next_continuation_token;
        } else {
            break;
        }
    }

    Ok(())
}

fn parse_size_list(input: &str) -> Result<Vec<SizeSpec>> {
    input
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(parse_size_spec)
        .collect()
}

fn parse_size_spec(input: &str) -> Result<SizeSpec> {
    let normalized = input.trim();
    let lower = normalized.to_ascii_lowercase();

    let (number_part, multiplier) = if let Some(value) = lower.strip_suffix("kib") {
        (value, 1024usize)
    } else if let Some(value) = lower.strip_suffix("mib") {
        (value, 1024usize * 1024usize)
    } else if let Some(value) = lower.strip_suffix('b') {
        (value, 1usize)
    } else {
        (lower.as_str(), 1usize)
    };

    let value = number_part
        .trim()
        .parse::<usize>()
        .with_context(|| format!("invalid size component: {input}"))?;
    let bytes = value
        .checked_mul(multiplier)
        .ok_or_else(|| anyhow!("size overflow for {input}"))?;

    Ok(SizeSpec {
        label: normalized.to_string(),
        slug: normalized
            .chars()
            .filter(|ch| ch.is_ascii_alphanumeric())
            .collect::<String>()
            .to_ascii_lowercase(),
        bytes,
    })
}

fn percentile(values: &[f64], percentile: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }

    let index = ((values.len() - 1) as f64 * percentile).floor() as usize;
    values.get(index).copied()
}

fn default_run_id() -> String {
    format!("small-put-bench-{}", chrono::Utc::now().format("%Y%m%d-%H%M%S"))
}

fn print_size_summary(summary: &SizeSummary) {
    println!(
        "{}: success={} failed={} obj/s={:.3} MiB/s={:.3} avg={:.3?} p50={:.3?} p90={:.3?} p99={:.3?}",
        summary.label,
        summary.succeeded,
        summary.failed,
        summary.object_rate,
        summary.throughput_mib_per_sec,
        summary.avg_ms,
        summary.p50_ms,
        summary.p90_ms,
        summary.p99_ms,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_size_spec_supports_binary_units() {
        let four_kib = parse_size_spec("4KiB").expect("4KiB should parse");
        assert_eq!(four_kib.bytes, 4 * 1024);

        let one_mib = parse_size_spec("1MiB").expect("1MiB should parse");
        assert_eq!(one_mib.bytes, 1024 * 1024);
    }

    #[test]
    fn percentile_returns_expected_bucket() {
        let values = vec![10.0, 20.0, 30.0, 40.0, 50.0];
        assert_eq!(percentile(&values, 0.50), Some(30.0));
        assert_eq!(percentile(&values, 0.90), Some(40.0));
    }
}
