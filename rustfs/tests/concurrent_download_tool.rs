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

use anyhow::{Context, Result, anyhow};
use futures::stream::{self, StreamExt};
use reqwest::{Client, Url};
use std::env;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::time::{Duration, sleep};

#[derive(Debug)]
struct DownloadSettings {
    urls: Vec<String>,
    output_dir: PathBuf,
    concurrency: usize,
    repeat: usize,
    max_retries: usize,
    retry_backoff_ms: u64,
}

#[derive(Debug)]
struct DownloadSuccess {
    path: PathBuf,
    bytes: usize,
    attempts_used: usize,
    elapsed_ms: u128,
}

#[derive(Debug)]
struct DownloadAttemptError {
    attempts_used: usize,
    error: String,
    elapsed_ms: u128,
}

#[derive(Debug)]
struct DownloadFailure {
    index: usize,
    url: String,
    attempts_used: usize,
    error: String,
}

#[derive(Debug)]
struct DownloadSummary {
    saved_files: Vec<PathBuf>,
    total_tasks: usize,
    succeeded: usize,
    failed: usize,
    total_bytes: usize,
    elapsed_ms: u128,
    throughput_bps: f64,
    total_attempts: usize,
    retried_tasks: usize,
    retry_attempts: usize,
    latency_p50_ms: u128,
    latency_p95_ms: u128,
    failures: Vec<DownloadFailure>,
}

fn should_retry_status(status: reqwest::StatusCode) -> bool {
    status.as_u16() == 429 || status.is_server_error()
}

fn should_retry_reqwest_error(err: &reqwest::Error) -> bool {
    if err.is_timeout() || err.is_connect() || err.is_request() {
        return true;
    }

    match err.status() {
        Some(status) => should_retry_status(status),
        None => false,
    }
}

fn percentile(values: &[u128], p: f64) -> u128 {
    if values.is_empty() {
        return 0;
    }

    let mut sorted = values.to_vec();
    sorted.sort_unstable();

    let rank = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[rank]
}

impl DownloadSettings {
    fn from_env() -> Result<Self> {
        let urls_raw = env::var("DOWNLOAD_URLS").context("missing DOWNLOAD_URLS, expected comma-separated URLs")?;

        let urls: Vec<String> = urls_raw
            .split(',')
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(ToString::to_string)
            .collect();

        if urls.is_empty() {
            return Err(anyhow!("DOWNLOAD_URLS is empty, expected comma-separated URLs"));
        }

        let output_dir = env::var("DOWNLOAD_OUTPUT_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("target/tmp/concurrent_downloads"));

        let concurrency = env::var("DOWNLOAD_CONCURRENCY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(8);

        let repeat = env::var("DOWNLOAD_REPEAT")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(1);

        let max_retries = env::var("DOWNLOAD_MAX_RETRIES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0);

        let retry_backoff_ms = env::var("DOWNLOAD_RETRY_BACKOFF_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(200);

        Ok(Self {
            urls,
            output_dir,
            concurrency,
            repeat,
            max_retries,
            retry_backoff_ms,
        })
    }
}

fn original_filename(url: &str) -> String {
    Url::parse(url)
        .ok()
        .and_then(|parsed| {
            parsed
                .path_segments()
                .and_then(|mut segments| segments.rfind(|s| !s.is_empty()))
                .map(ToString::to_string)
        })
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "download.bin".to_string())
}

fn nanos_prefix() -> Result<u128> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before UNIX_EPOCH")?
        .as_nanos())
}

async fn download_one(
    client: &Client,
    output_dir: &Path,
    index: usize,
    url: String,
    max_retries: usize,
    retry_backoff_ms: u64,
) -> std::result::Result<DownloadSuccess, DownloadAttemptError> {
    let task_started_at = Instant::now();
    let mut attempt = 0usize;
    let mut last_error = String::new();
    let mut retryable = false;

    while attempt <= max_retries {
        attempt += 1;

        let response = match client.get(&url).send().await {
            Ok(resp) => resp,
            Err(err) => {
                retryable = should_retry_reqwest_error(&err);
                last_error = format!("failed request: {url}, error: {err}");
                if retryable && attempt <= max_retries {
                    sleep(Duration::from_millis(retry_backoff_ms)).await;
                    continue;
                }

                break;
            }
        };

        let status = response.status();
        if !status.is_success() {
            retryable = should_retry_status(status);
            last_error = format!("non-success status for URL: {url}, status: {status}");
            if retryable && attempt <= max_retries {
                sleep(Duration::from_millis(retry_backoff_ms)).await;
                continue;
            }

            break;
        }

        let body = match response.bytes().await {
            Ok(bytes) => bytes,
            Err(err) => {
                retryable = should_retry_reqwest_error(&err);
                last_error = format!("failed to read response body: {url}, error: {err}");
                if retryable && attempt <= max_retries {
                    sleep(Duration::from_millis(retry_backoff_ms)).await;
                    continue;
                }

                break;
            }
        };

        let source_name = original_filename(&url);
        let nanos = match nanos_prefix() {
            Ok(v) => v,
            Err(err) => {
                last_error = err.to_string();
                retryable = false;
                break;
            }
        };
        let target_name = format!("{}_{}_{}", nanos, index, source_name);
        let target_path = output_dir.join(target_name);

        let result: Result<DownloadSuccess> = async {
            tokio::fs::write(&target_path, &body)
                .await
                .with_context(|| format!("failed to write file: {}", target_path.display()))?;

            Ok(DownloadSuccess {
                path: target_path,
                bytes: body.len(),
                attempts_used: attempt,
                elapsed_ms: task_started_at.elapsed().as_millis(),
            })
        }
        .await;

        match result {
            Ok(success) => return Ok(success),
            Err(err) => {
                last_error = err.to_string();
                retryable = false;
                if retryable && attempt <= max_retries {
                    sleep(Duration::from_millis(retry_backoff_ms)).await;
                }
                break;
            }
        }
    }

    Err(DownloadAttemptError {
        attempts_used: attempt,
        error: if retryable {
            last_error
        } else {
            format!("{} (non-retryable)", last_error)
        },
        elapsed_ms: task_started_at.elapsed().as_millis(),
    })
}

async fn run_concurrent_downloads(settings: DownloadSettings) -> Result<DownloadSummary> {
    let started_at = Instant::now();

    tokio::fs::create_dir_all(&settings.output_dir)
        .await
        .with_context(|| format!("failed to create output dir: {}", settings.output_dir.display()))?;

    let client = Client::new();
    let tasks = settings
        .urls
        .into_iter()
        .flat_map(|url| (0..settings.repeat).map(move |_| url.clone()))
        .enumerate();

    let results = stream::iter(tasks)
        .map(|(index, url)| {
            let client = client.clone();
            let output_dir = settings.output_dir.clone();
            let max_retries = settings.max_retries;
            let retry_backoff_ms = settings.retry_backoff_ms;
            async move {
                let current_url = url.clone();
                let result = download_one(&client, &output_dir, index, url, max_retries, retry_backoff_ms).await;
                (index, current_url, result)
            }
        })
        .buffer_unordered(settings.concurrency)
        .collect::<Vec<(usize, String, std::result::Result<DownloadSuccess, DownloadAttemptError>)>>()
        .await;

    let mut saved_files = Vec::new();
    let mut total_bytes = 0usize;
    let mut total_attempts = 0usize;
    let mut retried_tasks = 0usize;
    let mut latencies_ms = Vec::new();
    let mut failures = Vec::new();

    for (index, url, item) in results {
        match item {
            Ok(success) => {
                total_bytes += success.bytes;
                total_attempts += success.attempts_used;
                if success.attempts_used > 1 {
                    retried_tasks += 1;
                }
                latencies_ms.push(success.elapsed_ms);
                saved_files.push(success.path);
            }
            Err(err) => {
                total_attempts += err.attempts_used;
                if err.attempts_used > 1 {
                    retried_tasks += 1;
                }
                latencies_ms.push(err.elapsed_ms);
                failures.push(DownloadFailure {
                    index,
                    url,
                    attempts_used: err.attempts_used,
                    error: err.error,
                });
            }
        }
    }

    let total_tasks = saved_files.len() + failures.len();
    let retry_attempts = total_attempts.saturating_sub(total_tasks);
    let elapsed_ms = started_at.elapsed().as_millis();
    let throughput_bps = if elapsed_ms == 0 {
        0.0
    } else {
        (total_bytes as f64) / ((elapsed_ms as f64) / 1000.0)
    };
    let latency_p50_ms = percentile(&latencies_ms, 0.50);
    let latency_p95_ms = percentile(&latencies_ms, 0.95);

    Ok(DownloadSummary {
        total_tasks,
        succeeded: saved_files.len(),
        failed: failures.len(),
        total_bytes,
        elapsed_ms,
        throughput_bps,
        total_attempts,
        retried_tasks,
        retry_attempts,
        latency_p50_ms,
        latency_p95_ms,
        saved_files,
        failures,
    })
}

#[tokio::test]
#[ignore]
async fn concurrent_download_tool() -> Result<()> {
    let settings = DownloadSettings::from_env()?;
    let summary = run_concurrent_downloads(settings).await?;

    for path in &summary.saved_files {
        println!("saved: {}", path.display());
    }

    println!("download complete");
    println!("total tasks: {}", summary.total_tasks);
    println!("succeeded: {}", summary.succeeded);
    println!("failed: {}", summary.failed);
    println!("total bytes: {}", summary.total_bytes);
    println!("elapsed ms: {}", summary.elapsed_ms);
    println!("throughput bps: {:.2}", summary.throughput_bps);
    println!("total attempts: {}", summary.total_attempts);
    println!("retried tasks: {}", summary.retried_tasks);
    println!("retry attempts: {}", summary.retry_attempts);
    println!("latency p50 ms: {}", summary.latency_p50_ms);
    println!("latency p95 ms: {}", summary.latency_p95_ms);

    if !summary.failures.is_empty() {
        println!("failure details:");
        for failure in &summary.failures {
            println!(
                "  [{}] attempts={} {} => {}",
                failure.index, failure.attempts_used, failure.url, failure.error
            );
        }

        return Err(anyhow!("download finished with {} failures", summary.failures.len()));
    }

    Ok(())
}
