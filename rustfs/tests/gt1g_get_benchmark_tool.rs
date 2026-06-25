use anyhow::{Context, Result, anyhow};
use aws_config::BehaviorVersion;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Builder as S3ConfigBuilder, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use std::env;
use std::path::PathBuf;
use std::time::Instant;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::time::{Duration, sleep};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ToolAction {
    Prepare,
    Bench,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BenchMode {
    Sequential,
    RangedParallel,
}

impl BenchMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Sequential => "sequential",
            Self::RangedParallel => "ranged_parallel",
        }
    }
}

#[derive(Clone, Debug)]
struct ObjectSpec {
    label: String,
    key: String,
    size_bytes: i64,
}

#[derive(Clone, Debug)]
struct ToolSettings {
    action: ToolAction,
    endpoint: String,
    access_key: String,
    secret_key: String,
    bucket: String,
    region: String,
    out_dir: PathBuf,
    objects: Vec<ObjectSpec>,
    modes: Vec<BenchMode>,
    concurrencies: Vec<usize>,
    range_workers: Vec<usize>,
    rounds: usize,
    cooldown_secs: u64,
    force: bool,
}

fn parse_action() -> Result<ToolAction> {
    match env::var("GT1G_GET_ACTION")
        .unwrap_or_else(|_| "bench".to_string())
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "prepare" => Ok(ToolAction::Prepare),
        "bench" => Ok(ToolAction::Bench),
        other => Err(anyhow!("unsupported GT1G_GET_ACTION: {other}")),
    }
}

fn parse_size_label_to_bytes(label: &str) -> Result<i64> {
    let normalized = label.trim();
    if let Some(raw) = normalized.strip_suffix("GiB") {
        let value = raw.trim().parse::<i64>()?;
        return Ok(value * 1024 * 1024 * 1024);
    }
    if let Some(raw) = normalized.strip_suffix("MiB") {
        let value = raw.trim().parse::<i64>()?;
        return Ok(value * 1024 * 1024);
    }
    if let Some(raw) = normalized.strip_suffix("KiB") {
        let value = raw.trim().parse::<i64>()?;
        return Ok(value * 1024);
    }
    Err(anyhow!("unsupported size label: {label}"))
}

fn parse_object_specs(raw: &str) -> Result<Vec<ObjectSpec>> {
    raw.split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            let (label, key) = entry.split_once('=').ok_or_else(|| anyhow!("invalid object spec: {entry}"))?;
            Ok(ObjectSpec {
                label: label.trim().to_string(),
                key: key.trim().to_string(),
                size_bytes: parse_size_label_to_bytes(label.trim())?,
            })
        })
        .collect()
}

fn parse_csv_usize(raw: &str, field: &str) -> Result<Vec<usize>> {
    let values: Vec<usize> = raw
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            entry
                .parse::<usize>()
                .with_context(|| format!("invalid {field} value: {entry}"))
        })
        .collect::<Result<Vec<_>>>()?;
    if values.is_empty() {
        return Err(anyhow!("{field} cannot be empty"));
    }
    Ok(values)
}

fn parse_modes(raw: &str) -> Result<Vec<BenchMode>> {
    let modes: Vec<BenchMode> = raw
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| match entry {
            "sequential" => Ok(BenchMode::Sequential),
            "ranged_parallel" => Ok(BenchMode::RangedParallel),
            other => Err(anyhow!("unsupported GT1G_GET_MODES value: {other}")),
        })
        .collect::<Result<Vec<_>>>()?;
    if modes.is_empty() {
        return Err(anyhow!("GT1G_GET_MODES cannot be empty"));
    }
    Ok(modes)
}

impl ToolSettings {
    fn from_env() -> Result<Self> {
        let action = parse_action()?;
        let endpoint = env::var("GT1G_GET_ENDPOINT").context("missing GT1G_GET_ENDPOINT")?;
        let access_key = env::var("GT1G_GET_ACCESS_KEY").context("missing GT1G_GET_ACCESS_KEY")?;
        let secret_key = env::var("GT1G_GET_SECRET_KEY").context("missing GT1G_GET_SECRET_KEY")?;
        let bucket = env::var("GT1G_GET_BUCKET").context("missing GT1G_GET_BUCKET")?;
        let region = env::var("GT1G_GET_REGION").unwrap_or_else(|_| "us-east-1".to_string());
        let out_dir = env::var("GT1G_GET_OUT_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(format!("target/bench/gt1g-get-tool-{}", chrono_like_timestamp())));
        let objects = parse_object_specs(&env::var("GT1G_GET_OBJECTS").context("missing GT1G_GET_OBJECTS")?)?;
        let modes = parse_modes(&env::var("GT1G_GET_MODES").unwrap_or_else(|_| "sequential,ranged_parallel".to_string()))?;
        let concurrencies = parse_csv_usize(
            &env::var("GT1G_GET_CONCURRENCIES").unwrap_or_else(|_| "1,4,8".to_string()),
            "GT1G_GET_CONCURRENCIES",
        )?;
        let range_workers = parse_csv_usize(
            &env::var("GT1G_GET_RANGE_WORKERS").unwrap_or_else(|_| "4".to_string()),
            "GT1G_GET_RANGE_WORKERS",
        )?;
        let rounds = env::var("GT1G_GET_ROUNDS")
            .unwrap_or_else(|_| "3".to_string())
            .parse::<usize>()
            .context("invalid GT1G_GET_ROUNDS")?;
        let cooldown_secs = env::var("GT1G_GET_COOLDOWN_SECS")
            .unwrap_or_else(|_| "15".to_string())
            .parse::<u64>()
            .context("invalid GT1G_GET_COOLDOWN_SECS")?;
        let force = env::var("GT1G_GET_FORCE")
            .ok()
            .map(|raw| matches!(raw.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
            .unwrap_or(false);

        Ok(Self {
            action,
            endpoint,
            access_key,
            secret_key,
            bucket,
            region,
            out_dir,
            objects,
            modes,
            concurrencies,
            range_workers,
            rounds,
            cooldown_secs,
            force,
        })
    }
}

fn chrono_like_timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    now.to_string()
}

async fn build_client(settings: &ToolSettings) -> Result<Client> {
    let region = Region::new(settings.region.clone());
    let region_provider = RegionProviderChain::first_try(Some(region.clone()));
    let shared_config = aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .credentials_provider(Credentials::new(
            settings.access_key.clone(),
            settings.secret_key.clone(),
            None,
            None,
            "issue713-gt1g-get-tool",
        ))
        .load()
        .await;

    let s3_config = S3ConfigBuilder::from(&shared_config)
        .endpoint_url(settings.endpoint.clone())
        .force_path_style(true)
        .region(region)
        .build();
    Ok(Client::from_conf(s3_config))
}

async fn ensure_bucket(client: &Client, bucket: &str) -> Result<()> {
    if client.head_bucket().bucket(bucket).send().await.is_ok() {
        return Ok(());
    }
    client.create_bucket().bucket(bucket).send().await?;
    Ok(())
}

async fn object_matches_size(client: &Client, bucket: &str, object: &ObjectSpec) -> Result<bool> {
    let out = match client.head_object().bucket(bucket).key(&object.key).send().await {
        Ok(out) => out,
        Err(_) => return Ok(false),
    };
    Ok(out.content_length().unwrap_or_default() == object.size_bytes)
}

async fn create_sparse_file(path: &PathBuf, size_bytes: i64) -> Result<()> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .await
        .with_context(|| format!("failed to open sparse file: {}", path.display()))?;
    file.set_len(size_bytes as u64)
        .await
        .with_context(|| format!("failed to set sparse file length: {}", path.display()))?;
    Ok(())
}

async fn prepare_objects(settings: &ToolSettings, client: &Client) -> Result<()> {
    ensure_bucket(client, &settings.bucket).await?;
    tokio::fs::create_dir_all(&settings.out_dir).await?;
    let tmp_root = settings.out_dir.join("prepare-tmp");
    tokio::fs::create_dir_all(&tmp_root).await?;

    for object in &settings.objects {
        if !settings.force && object_matches_size(client, &settings.bucket, object).await? {
            println!("skip existing: s3://{}/{}", settings.bucket, object.key);
            continue;
        }

        let local_path = tmp_root.join(object.key.replace('/', "_"));
        create_sparse_file(&local_path, object.size_bytes).await?;
        println!("uploading: s3://{}/{} ({})", settings.bucket, object.key, object.label);
        client
            .put_object()
            .bucket(&settings.bucket)
            .key(&object.key)
            .body(ByteStream::from_path(&local_path).await?)
            .send()
            .await
            .with_context(|| format!("failed to upload {}", object.key))?;
        let _ = tokio::fs::remove_file(&local_path).await;
    }

    Ok(())
}

async fn drain_body(mut body: ByteStream) -> Result<u64> {
    let mut total = 0u64;
    while let Some(chunk) = body.try_next().await? {
        total += chunk.len() as u64;
    }
    Ok(total)
}

async fn run_sequential_task(client: Client, bucket: String, key: String, expected_size: i64) -> Result<(u64, u128)> {
    let started = Instant::now();
    let output = client.get_object().bucket(bucket).key(key).send().await?;
    let bytes = drain_body(output.body).await?;
    if bytes != expected_size as u64 {
        return Err(anyhow!("downloaded bytes mismatch: expected {}, got {}", expected_size, bytes));
    }
    Ok((bytes, started.elapsed().as_millis()))
}

async fn run_range_worker(client: Client, bucket: String, key: String, start: i64, end: i64) -> Result<u64> {
    let range = format!("bytes={start}-{end}");
    let output = client.get_object().bucket(bucket).key(key).range(range).send().await?;
    let bytes = drain_body(output.body).await?;
    Ok(bytes)
}

async fn run_ranged_parallel_task(
    client: Client,
    bucket: String,
    key: String,
    expected_size: i64,
    range_workers: usize,
) -> Result<(u64, u128)> {
    let started = Instant::now();
    let chunk_size = (expected_size + range_workers as i64 - 1) / range_workers as i64;
    let mut join_set = tokio::task::JoinSet::new();

    for worker_index in 0..range_workers {
        let start = worker_index as i64 * chunk_size;
        if start >= expected_size {
            continue;
        }
        let end = (start + chunk_size - 1).min(expected_size - 1);
        join_set.spawn(run_range_worker(client.clone(), bucket.clone(), key.clone(), start, end));
    }

    let mut total = 0u64;
    while let Some(result) = join_set.join_next().await {
        total += result??;
    }
    if total != expected_size as u64 {
        return Err(anyhow!("ranged download bytes mismatch: expected {}, got {}", expected_size, total));
    }
    Ok((total, started.elapsed().as_millis()))
}

async fn run_bench(settings: &ToolSettings, client: &Client) -> Result<()> {
    tokio::fs::create_dir_all(&settings.out_dir).await?;
    let round_csv = settings.out_dir.join("round_results.csv");
    let median_csv = settings.out_dir.join("median_summary.csv");
    tokio::fs::write(
        &round_csv,
        "object_label,object_key,mode,range_workers,concurrency,round,status,total_bytes,elapsed_ms,throughput_bps,avg_client_latency_ms\n",
    )
    .await?;
    tokio::fs::write(
        &median_csv,
        "object_label,object_key,mode,range_workers,concurrency,successful_rounds,failed_rounds,median_total_bytes,median_elapsed_ms,median_throughput_bps,median_avg_client_latency_ms\n",
    )
    .await?;

    let mut rows = Vec::new();
    for object in &settings.objects {
        for &mode in &settings.modes {
            for &concurrency in &settings.concurrencies {
                let worker_values = if mode == BenchMode::Sequential {
                    vec![1usize]
                } else {
                    settings.range_workers.clone()
                };

                for range_workers in worker_values {
                    for round in 1..=settings.rounds {
                        let started = Instant::now();
                        let mut join_set = tokio::task::JoinSet::new();
                        for _ in 0..concurrency {
                            let task_client = client.clone();
                            let bucket = settings.bucket.clone();
                            let key = object.key.clone();
                            let size = object.size_bytes;
                            if mode == BenchMode::Sequential {
                                join_set.spawn(run_sequential_task(task_client, bucket, key, size));
                            } else {
                                join_set.spawn(run_ranged_parallel_task(task_client, bucket, key, size, range_workers));
                            }
                        }

                        let mut status = "ok".to_string();
                        let mut total_bytes = 0u64;
                        let mut latencies_ms = Vec::new();

                        while let Some(result) = join_set.join_next().await {
                            match result {
                                Ok(Ok((bytes, elapsed_ms))) => {
                                    total_bytes += bytes;
                                    latencies_ms.push(elapsed_ms);
                                }
                                Ok(Err(err)) => {
                                    status = format!("failed:{err}");
                                }
                                Err(err) => {
                                    status = format!("failed:{err}");
                                }
                            }
                        }

                        let elapsed_ms = started.elapsed().as_millis();
                        let avg_client_latency_ms = if latencies_ms.is_empty() {
                            0.0
                        } else {
                            latencies_ms.iter().sum::<u128>() as f64 / latencies_ms.len() as f64
                        };
                        let throughput_bps = if elapsed_ms == 0 {
                            0.0
                        } else {
                            total_bytes as f64 / (elapsed_ms as f64 / 1000.0)
                        };

                        let row = format!(
                            "{},{},{},{},{},{},{},{},{},{:.6},{:.3}\n",
                            object.label,
                            object.key,
                            mode.as_str(),
                            range_workers,
                            concurrency,
                            round,
                            status,
                            total_bytes,
                            elapsed_ms,
                            throughput_bps,
                            avg_client_latency_ms
                        );
                        tokio::fs::OpenOptions::new()
                            .append(true)
                            .open(&round_csv)
                            .await?
                            .write_all(row.as_bytes())
                            .await?;
                        rows.push((
                            object.label.clone(),
                            object.key.clone(),
                            mode.as_str().to_string(),
                            range_workers,
                            concurrency,
                            status,
                            total_bytes,
                            elapsed_ms as f64,
                            throughput_bps,
                            avg_client_latency_ms,
                        ));

                        if settings.cooldown_secs > 0 && round < settings.rounds {
                            sleep(Duration::from_secs(settings.cooldown_secs)).await;
                        }
                    }
                }
            }
        }
    }

    use std::collections::BTreeMap;
    let mut grouped: BTreeMap<(String, String, String, usize, usize), Vec<(bool, u64, f64, f64, f64)>> = BTreeMap::new();
    for (label, key, mode, range_workers, concurrency, status, total_bytes, elapsed_ms, throughput_bps, avg_latency_ms) in rows {
        grouped
            .entry((label, key, mode, range_workers, concurrency))
            .or_default()
            .push((status == "ok", total_bytes, elapsed_ms, throughput_bps, avg_latency_ms));
    }

    for ((label, key, mode, range_workers, concurrency), values) in grouped {
        let successful_rounds = values.iter().filter(|v| v.0).count();
        let failed_rounds = values.len().saturating_sub(successful_rounds);
        let mut bytes = Vec::new();
        let mut elapsed = Vec::new();
        let mut throughput = Vec::new();
        let mut latency = Vec::new();
        for (ok, total_bytes, elapsed_ms, throughput_bps, avg_latency_ms) in values {
            if ok {
                bytes.push(total_bytes as f64);
                elapsed.push(elapsed_ms);
                throughput.push(throughput_bps);
                latency.push(avg_latency_ms);
            }
        }

        let median = |input: &mut Vec<f64>| -> String {
            if input.is_empty() {
                return "N/A".to_string();
            }
            input.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let n = input.len();
            if n % 2 == 1 {
                format!("{:.6}", input[n / 2])
            } else {
                format!("{:.6}", (input[n / 2 - 1] + input[n / 2]) / 2.0)
            }
        };

        let row = format!(
            "{},{},{},{},{},{},{},{},{},{},{}\n",
            label,
            key,
            mode,
            range_workers,
            concurrency,
            successful_rounds,
            failed_rounds,
            median(&mut bytes),
            median(&mut elapsed),
            median(&mut throughput),
            median(&mut latency)
        );
        tokio::fs::OpenOptions::new()
            .append(true)
            .open(&median_csv)
            .await?
            .write_all(row.as_bytes())
            .await?;
    }
    Ok(())
}

#[tokio::test]
#[ignore]
async fn gt1g_get_benchmark_tool() -> Result<()> {
    let settings = ToolSettings::from_env()?;
    let client = build_client(&settings).await?;

    match settings.action {
        ToolAction::Prepare => prepare_objects(&settings, &client).await?,
        ToolAction::Bench => run_bench(&settings, &client).await?,
    }

    Ok(())
}
