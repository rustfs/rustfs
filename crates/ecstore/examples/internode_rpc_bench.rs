// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Read-only authenticated ReadAll/Ping probe against explicitly prepared test disks.
//! Usage: internode_rpc_bench manifest.json output.json
//! Fixtures live under each test disk's .rustfs.sys/tmp/<fixture_id>/ directory.

use rustfs_ecstore::api::{
    disk::{DiskAPI, DiskOption, DiskStore, endpoint::Endpoint, new_disk},
    rpc::{TonicInterceptor, gen_tonic_signature_interceptor, node_service_time_out_client},
};
use rustfs_protos::proto_gen::node_service::PingRequest;
use rustfs_tls_runtime::{OutboundTlsMaterial, TlsGeneration, ensure_crypto_provider, publish_global_outbound_tls_state};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::io::{Error, ErrorKind, Result, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncReadExt;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep, sleep_until, timeout};
use uuid::Uuid;

const MAX_PAYLOAD: usize = 8 * 1024 * 1024;
const MAX_BULK_ROWS: usize = 4_000_000;
const MAX_CONTROL_ROWS: u32 = 1_000_000;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    namespace: String,
    targets: Vec<String>,
    disk_ids: Option<Vec<Uuid>>,
    fixture_id: Uuid,
    fixture_file: PathBuf,
    duration_secs: u32,
    bulk_workers: usize,
    control_rps: u32,
    rpc_timeout_ms: u32,
    tls_ca: Option<PathBuf>,
    start_marker: Option<PathBuf>,
    end_marker: Option<PathBuf>,
}

impl Config {
    fn endpoints(&self) -> Result<Vec<Endpoint>> {
        let valid_namespace = self.namespace.starts_with("grpc-bench-")
            && (12..=64).contains(&self.namespace.len())
            && self
                .namespace
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-');
        if !valid_namespace || self.fixture_id.is_nil() || !(1..=64).contains(&self.targets.len()) {
            return Err(Error::new(ErrorKind::InvalidInput, "invalid test namespace, fixture ID or target count"));
        }
        if self
            .disk_ids
            .as_ref()
            .is_some_and(|ids| ids.len() != self.targets.len() || ids.iter().any(Uuid::is_nil))
        {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "disk IDs must match every declared target and be non-nil",
            ));
        }
        if !(1..=1800).contains(&self.duration_secs)
            || !(1..=128).contains(&self.bulk_workers)
            || !(1..=10_000).contains(&self.control_rps)
            || self
                .duration_secs
                .checked_mul(self.control_rps)
                .is_none_or(|count| count > MAX_CONTROL_ROWS)
            || !(1..=30_000).contains(&self.rpc_timeout_ms)
        {
            return Err(Error::new(ErrorKind::InvalidInput, "workload exceeds the probe's resource bounds"));
        }
        if self.start_marker.is_some() != self.end_marker.is_some()
            || self.start_marker == self.end_marker && self.start_marker.is_some()
        {
            return Err(Error::new(ErrorKind::InvalidInput, "provide distinct start/end markers or neither"));
        }
        let mut seen = BTreeSet::new();
        self.targets
            .iter()
            .map(|target| {
                let mut endpoint = Endpoint::try_from(target.as_str())
                    .map_err(|_| Error::new(ErrorKind::InvalidInput, "invalid test endpoint URL"))?;
                let url = &endpoint.url;
                if target.len() > 1024
                    || !matches!(url.scheme(), "http" | "https")
                    || url.host_str().is_none()
                    || url.port().is_none_or(|port| port < 1024)
                    || !url.username().is_empty()
                    || url.password().is_some()
                    || url.query().is_some()
                    || url.fragment().is_some()
                    || url.path().contains('%')
                    || !url.path().ends_with(&format!("/{}", self.namespace))
                    || !seen.insert(url.to_string())
                    || url.scheme() == "https" && self.tls_ca.is_none()
                {
                    return Err(Error::new(ErrorKind::InvalidInput, "target must name an explicit test-disk URL"));
                }
                // This probe never constructs a local disk or invokes its startup cleanup.
                endpoint.is_local = false;
                Ok(endpoint)
            })
            .collect()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Marker {
    time_ns: u64,
}

#[derive(Serialize)]
struct Sample {
    operation: &'static str,
    peer: usize,
    scheduled_ns: u64,
    dispatched_ns: Option<u64>,
    completed_ns: u64,
    outcome: &'static str,
    bytes: usize,
}

#[derive(Serialize)]
struct Report {
    complete: bool,
    errors: usize,
    manifest_sha256: String,
    fixture_sha256: String,
    start_unix_ns: String,
    observation_end_unix_ns: String,
    offered_duration_secs: u32,
    control_rps: u32,
    bulk_bytes_completed_during_offering: u64,
    bulk_coverage_verified: bool,
    bulk_workers: usize,
    control_expected: u32,
    targets: usize,
    peers: usize,
    samples: Vec<Sample>,
    external_markers: bool,
    scope: &'static str,
}

fn now_ns() -> Result<u64> {
    u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).map_err(Error::other)?.as_nanos()).map_err(Error::other)
}

fn elapsed_ns(origin: Instant) -> Result<u64> {
    u64::try_from(origin.elapsed().as_nanos()).map_err(Error::other)
}

fn measurement_origin(start_ns: u64, current_ns: u64, monotonic_now: Instant) -> Result<Instant> {
    let lag = current_ns
        .checked_sub(start_ns)
        .filter(|lag| *lag <= 1_000_000_000)
        .ok_or_else(|| Error::other("measurement start must be within the preceding second"))?;
    monotonic_now
        .checked_sub(Duration::from_nanos(lag))
        .ok_or_else(|| Error::other("invalid monotonic origin"))
}

fn validate_observation_end(start_ns: u64, end_ns: u64, finished_ns: u64, current_ns: u64, elapsed: u64) -> Result<()> {
    let clock_end = start_ns
        .checked_add(elapsed)
        .ok_or_else(|| Error::other("observation window overflow"))?;
    if end_ns < finished_ns || end_ns > current_ns || clock_end.abs_diff(current_ns) > 100_000_000 {
        return Err(Error::other("invalid measurement-end acknowledgement or clock drift"));
    }
    Ok(())
}

async fn bounded_file(path: &Path, limit: usize) -> Result<Vec<u8>> {
    let file = tokio::fs::File::open(path).await?;
    if !file.metadata().await?.is_file() {
        return Err(Error::new(ErrorKind::InvalidInput, "probe inputs must be regular files"));
    }
    let mut data = Vec::new();
    file.take(u64::try_from(limit).map_err(Error::other)? + 1)
        .read_to_end(&mut data)
        .await?;
    if data.len() > limit {
        return Err(Error::new(ErrorKind::InvalidData, "probe input exceeds its size bound"));
    }
    Ok(data)
}

async fn marker(path: &Path) -> Result<u64> {
    timeout(Duration::from_secs(30), async {
        loop {
            match bounded_file(path, 512).await {
                Ok(data) => {
                    return serde_json::from_slice::<Marker>(&data)
                        .map(|value| value.time_ns)
                        .map_err(Error::other);
                }
                Err(error) if error.kind() == ErrorKind::NotFound => sleep(Duration::from_millis(5)).await,
                Err(error) => return Err(error),
            }
        }
    })
    .await
    .map_err(|_| Error::new(ErrorKind::TimedOut, "measurement marker did not arrive"))?
}

fn phase(name: &str, time_ns: u64) -> Result<()> {
    println!(
        "{}",
        serde_json::json!({"phase":name,"time_ns":time_ns.to_string(),"pid":std::process::id()})
    );
    std::io::stdout().flush()
}

async fn ping(peer: &str) -> Result<(u64, bytes::Bytes)> {
    let mut client = node_service_time_out_client(peer, TonicInterceptor::Signature(gen_tonic_signature_interceptor()))
        .await
        .map_err(|_| Error::other("authenticated control client acquisition failed"))?
        .max_decoding_message_size(4096);
    let response = client
        .ping(PingRequest {
            version: 1,
            body: bytes::Bytes::new(),
        })
        .await
        .map_err(|_| Error::other("authenticated Ping failed"))?
        .into_inner();
    Ok((response.version, response.body))
}

struct Context {
    config: Config,
    disks: Vec<DiskStore>,
    peers: Vec<String>,
    echoes: Vec<bytes::Bytes>,
    payload: Vec<u8>,
    path: String,
}

async fn bulk(context: Arc<Context>, worker: usize, origin: Instant, end: Instant) -> Result<Vec<Sample>> {
    let mut rows = Vec::with_capacity(256);
    let limit = MAX_BULK_ROWS / context.config.bulk_workers;
    for sequence in 0..limit {
        if Instant::now() >= end {
            return Ok(rows);
        }
        let peer = (worker + sequence) % context.disks.len();
        let dispatched = elapsed_ns(origin)?;
        let result = timeout(
            Duration::from_millis(u64::from(context.config.rpc_timeout_ms)),
            context.disks[peer].read_all(".rustfs.sys", &context.path),
        )
        .await;
        let completed = elapsed_ns(origin)?;
        let outcome = match result {
            Ok(Ok(data)) if data.as_ref() == context.payload.as_slice() => "ok",
            Ok(Ok(_)) => "payload_mismatch",
            Ok(Err(_)) => "rpc_error",
            Err(_) => "deadline",
        };
        rows.push(Sample {
            operation: "ReadAll",
            peer,
            scheduled_ns: dispatched,
            dispatched_ns: Some(dispatched),
            completed_ns: completed,
            outcome,
            bytes: if outcome == "ok" { context.payload.len() } else { 0 },
        });
    }
    if Instant::now() >= end {
        Ok(rows)
    } else {
        Err(Error::other("bulk raw-sample limit reached; attribution is invalid"))
    }
}

async fn control(context: Arc<Context>, origin: Instant) -> Result<Vec<Sample>> {
    let count = context.config.duration_secs * context.config.control_rps;
    let capacity = usize::try_from(count).map_err(Error::other)?;
    let mut rows = Vec::with_capacity(capacity);
    let permits = Arc::new(Semaphore::new(128));
    let end = origin + Duration::from_secs(u64::from(context.config.duration_secs));
    let mut tasks = JoinSet::new();
    for sequence in 0..count {
        // Configuration bounds count to one million, so this product fits u64.
        let scheduled = u64::from(sequence) * 1_000_000_000 / u64::from(context.config.control_rps);
        sleep_until(origin + Duration::from_nanos(scheduled)).await;
        while let Some(result) = tasks.try_join_next() {
            rows.push(result.map_err(Error::other)??);
        }
        let peer = usize::try_from(sequence).map_err(Error::other)? % context.peers.len();
        let permit = (Instant::now() < end)
            .then(|| permits.clone().try_acquire_owned())
            .and_then(std::result::Result::ok);
        let Some(permit) = permit else {
            rows.push(Sample {
                operation: "Ping",
                peer,
                scheduled_ns: scheduled,
                dispatched_ns: None,
                completed_ns: elapsed_ns(origin)?,
                outcome: "schedule_drop",
                bytes: 0,
            });
            continue;
        };
        let context = context.clone();
        tasks.spawn(async move {
            let _permit = permit;
            let dispatched = elapsed_ns(origin)?;
            let result = timeout(
                Duration::from_millis(u64::from(context.config.rpc_timeout_ms)),
                ping(&context.peers[peer]),
            )
            .await;
            let completed = elapsed_ns(origin)?;
            let outcome = match result {
                Ok(Ok((1, body))) if body == context.echoes[peer] => "ok",
                Ok(Ok(_)) => "payload_mismatch",
                Ok(Err(_)) => "rpc_error",
                Err(_) => "deadline",
            };
            Ok::<_, Error>(Sample {
                operation: "Ping",
                peer,
                scheduled_ns: scheduled,
                dispatched_ns: Some(dispatched),
                completed_ns: completed,
                outcome,
                bytes: 0,
            })
        });
    }
    while let Some(result) = tasks.join_next().await {
        rows.push(result.map_err(Error::other)??);
    }
    if rows.len() != capacity {
        return Err(Error::other("control arrival accounting is incomplete"));
    }
    Ok(rows)
}

async fn run(config_bytes: &[u8], base: &Path, secret: String) -> Result<Report> {
    let mut config: Config = serde_json::from_slice(config_bytes).map_err(|_| Error::other("invalid probe manifest"))?;
    if config.fixture_file.is_relative() {
        config.fixture_file = base.join(&config.fixture_file);
    }
    for path in [&mut config.tls_ca, &mut config.start_marker, &mut config.end_marker]
        .into_iter()
        .flatten()
    {
        if path.is_relative() {
            *path = base.join(&*path);
        }
    }
    let endpoints = config.endpoints()?;
    for path in [&config.start_marker, &config.end_marker].into_iter().flatten() {
        if tokio::fs::try_exists(path).await? {
            return Err(Error::other("measurement markers must be fresh"));
        }
    }
    // Require the test deployment's explicit secret; never use a default credential.
    if secret.trim().is_empty() {
        return Err(Error::other("RUSTFS_RPC_SECRET is required"));
    }
    rustfs_credentials::set_global_rpc_secret(secret).map_err(|_| Error::other("RPC secret already initialized"))?;
    ensure_crypto_provider().map_err(Error::other)?;
    if let Some(path) = &config.tls_ca {
        let ca = bounded_file(path, 1024 * 1024).await?;
        if ca.is_empty() {
            return Err(Error::other("the explicit test CA must not be empty"));
        }
        publish_global_outbound_tls_state(
            TlsGeneration(1),
            &OutboundTlsMaterial {
                root_ca_pem: ca,
                mtls_identity: None,
            },
        )
        .await;
    }
    let payload = bounded_file(&config.fixture_file, MAX_PAYLOAD).await?;
    if payload.is_empty()
        || payload
            .len()
            .checked_mul(config.bulk_workers)
            .is_none_or(|bytes| bytes > 512 * 1024 * 1024)
    {
        return Err(Error::other("fixture and worker count exceed the payload memory budget"));
    }
    let path = format!("tmp/{}/grpc-bench-payload.bin", config.fixture_id);
    let mut disks = Vec::with_capacity(endpoints.len());
    let mut peers = BTreeSet::new();
    for (index, endpoint) in endpoints.into_iter().enumerate() {
        peers.insert(endpoint.grid_host());
        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: true,
            },
        )
        .await
        .map_err(Error::other)?;
        if let Some(ids) = &config.disk_ids {
            disk.set_disk_id(Some(ids[index])).await.map_err(Error::other)?;
        }
        let data = timeout(Duration::from_secs(30), disk.read_all(".rustfs.sys", &path))
            .await
            .map_err(Error::other)?
            .map_err(|error| Error::other(format!("ReadAll fixture preflight failed: disk_error_code={}", error.to_u32())))?;
        if data.as_ref() != payload.as_slice() {
            return Err(Error::other("ReadAll fixture preflight mismatch"));
        }
        disks.push(disk);
    }
    let peers: Vec<_> = peers.into_iter().collect();
    let mut echoes = Vec::with_capacity(peers.len());
    for peer in &peers {
        let (version, body) = timeout(Duration::from_secs(30), ping(peer)).await.map_err(Error::other)??;
        if version != 1 || body.is_empty() {
            return Err(Error::other("Ping preflight mismatch"));
        }
        echoes.push(body);
    }
    let context = Arc::new(Context {
        config,
        disks,
        peers,
        echoes,
        payload,
        path,
    });
    phase("ready", now_ns()?)?;
    let start_ns = match &context.config.start_marker {
        Some(path) => marker(path).await?,
        None => now_ns()?,
    };
    let current_ns = now_ns()?;
    let origin = measurement_origin(start_ns, current_ns, Instant::now())?;
    let offered = Duration::from_secs(u64::from(context.config.duration_secs));
    let end = origin + offered;
    phase("measure_start", start_ns)?;
    let mut tasks = JoinSet::new();
    for worker in 0..context.config.bulk_workers {
        tasks.spawn(bulk(context.clone(), worker, origin, end));
    }
    tasks.spawn(control(context.clone(), origin));
    let mut samples = Vec::new();
    while let Some(result) = tasks.join_next().await {
        samples.extend(result.map_err(Error::other)??);
    }
    let finished_ns = now_ns()?;
    phase("load_finished", finished_ns)?;
    let end_ns = match &context.config.end_marker {
        Some(path) => marker(path).await?,
        None => now_ns()?,
    };
    let current_ns = now_ns()?;
    validate_observation_end(start_ns, end_ns, finished_ns, current_ns, elapsed_ns(origin)?)?;
    let offered_ns = u64::try_from(offered.as_nanos()).map_err(Error::other)?;
    let errors = samples.iter().filter(|sample| sample.outcome != "ok").count();
    let measured_bytes = samples
        .iter()
        .filter(|sample| sample.operation == "ReadAll" && sample.outcome == "ok" && sample.completed_ns <= offered_ns)
        .try_fold(0_u64, |total, sample| {
            total
                .checked_add(u64::try_from(sample.bytes).map_err(Error::other)?)
                .ok_or_else(|| Error::other("measured payload byte counter overflow"))
        })?;
    let report = Report {
        complete: true,
        errors,
        manifest_sha256: hex_simd::encode_to_string(Sha256::digest(config_bytes), hex_simd::AsciiCase::Lower),
        fixture_sha256: hex_simd::encode_to_string(Sha256::digest(&context.payload), hex_simd::AsciiCase::Lower),
        start_unix_ns: start_ns.to_string(),
        observation_end_unix_ns: end_ns.to_string(),
        offered_duration_secs: context.config.duration_secs,
        control_rps: context.config.control_rps,
        bulk_bytes_completed_during_offering: measured_bytes,
        bulk_coverage_verified: measured_bytes > 0,
        bulk_workers: context.config.bulk_workers,
        control_expected: context.config.duration_secs * context.config.control_rps,
        targets: context.disks.len(),
        peers: context.peers.len(),
        samples,
        external_markers: context.config.start_marker.is_some(),
        scope: "Authenticated production RPC clients and ReadAll temporary fixtures; client telemetry export is disabled. Storage S3/durability, mTLS/rotation and ABBA qualification remain separate.",
    };
    for disk in &context.disks {
        disk.close().await.map_err(Error::other)?;
    }
    phase("measure_end", end_ns)?;
    Ok(report)
}

#[tokio::main(worker_threads = 8)]
async fn main() -> Result<()> {
    let mut args = std::env::args_os().skip(1);
    let config = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| Error::other("usage: internode_rpc_bench manifest.json output.json"))?;
    let output = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| Error::other("output path is required"))?;
    if args.next().is_some() {
        return Err(Error::other("unexpected probe arguments"));
    }
    let input = bounded_file(&config, 64 * 1024).await?;
    let file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)
        .await?
        .into_std()
        .await;
    let secret = std::env::var("RUSTFS_RPC_SECRET").map_err(|_| Error::other("RUSTFS_RPC_SECRET is required"))?;
    let report = run(&input, config.parent().unwrap_or_else(|| Path::new(".")), secret).await?;
    let success = report.errors == 0 && report.bulk_coverage_verified;
    tokio::task::spawn_blocking(move || {
        let mut output = std::io::BufWriter::new(file);
        serde_json::to_writer(&mut output, &report).map_err(Error::other)?;
        output.flush()
    })
    .await
    .map_err(Error::other)??;
    if success {
        Ok(())
    } else {
        Err(Error::other(
            "RPC failures or missing bulk coverage invalidate attribution; see raw samples",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_protos::proto_gen::node_service::{PingResponse, ReadAllRequest, ReadAllResponse};
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::{Context as TaskContext, Poll};
    use tonic::codegen::{Body, BoxFuture, Service, StdError, http};

    fn config() -> Config {
        Config {
            namespace: "grpc-bench-test".into(),
            targets: vec!["http://127.0.0.1:19000/data/grpc-bench-test".into()],
            disk_ids: None,
            fixture_id: Uuid::from_u128(1),
            fixture_file: PathBuf::from("fixture.bin"),
            duration_secs: 1,
            bulk_workers: 2,
            control_rps: 20,
            rpc_timeout_ms: 1000,
            tls_ca: None,
            start_marker: None,
            end_marker: None,
        }
    }

    #[test]
    fn probe_rejects_non_test_and_credential_bearing_targets() {
        for target in [
            "file:///data/grpc-bench-test",
            "http://user:private@127.0.0.1:19000/data/grpc-bench-test",
            "http://127.0.0.1:19000/data/mnmd",
            "http://127.0.0.1:80/data/grpc-bench-test",
            "http://127.0.0.1:19000/data/grpc-bench-test?token=private",
            "http://127.0.0.1:19000/data/%67rpc-bench-test",
            "https://127.0.0.1:19000/data/grpc-bench-test",
        ] {
            let mut config = config();
            config.targets = vec![target.into()];
            assert!(config.endpoints().is_err(), "unsafe target was accepted");
        }
    }

    #[test]
    fn probe_forces_remote_client_and_rejects_duplicate_disks() {
        let mut config = config();
        assert!(!config.endpoints().expect("valid loopback test URL")[0].is_local);
        config.targets.push(config.targets[0].clone());
        assert!(config.endpoints().is_err(), "duplicate target must not inflate the inventory");
    }

    #[test]
    fn probe_requires_a_complete_non_nil_disk_id_mapping() {
        let mut config = config();
        config.disk_ids = Some(vec![]);
        assert!(config.endpoints().is_err());
        config.disk_ids = Some(vec![Uuid::nil()]);
        assert!(config.endpoints().is_err());
        config.disk_ids = Some(vec![Uuid::from_u128(42)]);
        assert!(config.endpoints().is_ok());
    }

    #[test]
    fn probe_bounds_work_and_requires_distinct_measurement_markers() {
        for (seconds, workers, rate, deadline) in [
            (0, 2, 20, 1000),
            (1, 0, 20, 1000),
            (1, 129, 20, 1000),
            (1800, 2, 10_000, 1000),
            (1, 2, 0, 1000),
            (1, 2, 20, 0),
        ] {
            let mut config = config();
            config.duration_secs = seconds;
            config.bulk_workers = workers;
            config.control_rps = rate;
            config.rpc_timeout_ms = deadline;
            assert!(config.endpoints().is_err());
        }
        let mut config = config();
        config.start_marker = Some("marker.json".into());
        assert!(config.endpoints().is_err());
        config.end_marker = config.start_marker.clone();
        assert!(config.endpoints().is_err());
    }

    #[test]
    fn probe_rejects_stale_future_or_truncated_measurement_markers() {
        let now = Instant::now();
        assert_eq!(
            measurement_origin(10_000_000_000, 11_000_000_000, now).expect("one-second lag bound"),
            now - Duration::from_secs(1)
        );
        assert!(measurement_origin(10_000_000_000, 11_000_000_001, now).is_err());
        assert!(measurement_origin(11_000_000_001, 11_000_000_000, now).is_err());
        assert!(validate_observation_end(100, 200, 200, 250, 150).is_ok());
        assert!(validate_observation_end(100, 199, 200, 250, 150).is_err());
        assert!(validate_observation_end(100, 251, 200, 250, 150).is_err());
        assert!(validate_observation_end(100, 200, 200, 250_000_000, 150).is_err());
    }

    #[tokio::test]
    async fn probe_marker_input_has_an_explicit_schema_and_size_bound() {
        let directory = tempfile::tempdir().expect("marker directory");
        let path = directory.path().join("marker.json");
        tokio::fs::write(&path, br#"{"time_ns":100}"#).await.expect("marker");
        assert_eq!(marker(&path).await.expect("valid marker"), 100);
        tokio::fs::write(&path, br#"{"time_ns":100,"unknown":true}"#)
            .await
            .expect("invalid marker");
        assert!(marker(&path).await.is_err());
        tokio::fs::write(&path, vec![b' '; 513]).await.expect("oversized marker");
        assert!(marker(&path).await.is_err());
    }

    #[tokio::test]
    async fn probe_bounds_fixture_reads_and_rejects_directories() {
        let directory = tempfile::tempdir().expect("temporary fixture directory");
        let path = directory.path().join("fixture.bin");
        tokio::fs::write(&path, [7_u8; 32]).await.expect("fixture write");
        assert_eq!(bounded_file(&path, 32).await.expect("exact-size fixture"), vec![7_u8; 32]);
        tokio::fs::write(&path, [7_u8; 33]).await.expect("oversized fixture write");
        assert!(bounded_file(&path, 32).await.is_err());
        assert!(bounded_file(directory.path(), 32).await.is_err());
    }

    #[tokio::test]
    async fn probe_accounts_for_expired_control_arrivals_without_sending_them() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("owned control socket");
        let context = Arc::new(super::Context {
            config: config(),
            disks: vec![],
            peers: vec![format!("http://{}", listener.local_addr().expect("owned address"))],
            echoes: vec![bytes::Bytes::from_static(b"hello")],
            payload: vec![],
            path: String::new(),
        });
        let rows = control(context, Instant::now() - Duration::from_secs(2))
            .await
            .expect("arrival accounting");
        assert_eq!(rows.len(), 20);
        assert!(
            rows.iter()
                .all(|row| row.outcome == "schedule_drop" && row.dispatched_ns.is_none())
        );
    }

    #[derive(Clone)]
    struct ReadFixture(bytes::Bytes, Arc<AtomicBool>);

    impl tonic::server::UnaryService<ReadAllRequest> for ReadFixture {
        type Response = ReadAllResponse;
        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
        fn call(&mut self, request: tonic::Request<ReadAllRequest>) -> Self::Future {
            assert_eq!(request.get_ref().volume, ".rustfs.sys");
            assert_eq!(request.get_ref().disk, Uuid::from_u128(42).to_string());
            assert!(request.get_ref().path.ends_with("/grpc-bench-payload.bin"));
            let data = if self.1.load(Ordering::SeqCst) {
                bytes::Bytes::from_static(b"corrupt fixture")
            } else {
                self.0.clone()
            };
            Box::pin(async move {
                Ok(tonic::Response::new(ReadAllResponse {
                    success: true,
                    data,
                    error: None,
                }))
            })
        }
    }

    #[derive(Clone)]
    struct ReplyPing;

    impl tonic::server::UnaryService<PingRequest> for ReplyPing {
        type Response = PingResponse;
        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
        fn call(&mut self, _: tonic::Request<PingRequest>) -> Self::Future {
            Box::pin(async {
                Ok(tonic::Response::new(PingResponse {
                    version: 1,
                    body: bytes::Bytes::from_static(b"hello, caller"),
                }))
            })
        }
    }

    #[derive(Clone)]
    struct FixtureService {
        data: bytes::Bytes,
        signed: Arc<AtomicUsize>,
        corrupt: Arc<AtomicBool>,
    }

    impl tonic::server::NamedService for FixtureService {
        const NAME: &'static str = "node_service.NodeService";
    }

    impl<B> Service<http::Request<B>> for FixtureService
    where
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::Body>;
        type Error = Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _: &mut TaskContext<'_>) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, request: http::Request<B>) -> Self::Future {
            assert_eq!(
                request
                    .headers()
                    .get("x-rustfs-rpc-auth-version")
                    .expect("signed production client"),
                "2"
            );
            assert!(request.headers().contains_key("x-rustfs-rpc-signature-v2"));
            self.signed.fetch_add(1, Ordering::Relaxed);
            let data = self.data.clone();
            let corrupt = self.corrupt.clone();
            Box::pin(async move {
                if request.uri().path().ends_with("/ReadAll") {
                    Ok(tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                        .unary(ReadFixture(data, corrupt), request)
                        .await)
                } else {
                    assert!(request.uri().path().ends_with("/Ping"));
                    Ok(tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                        .unary(ReplyPing, request)
                        .await)
                }
            })
        }
    }

    #[tokio::test]
    async fn probe_exercises_production_readall_and_fixed_rate_ping_clients() {
        let directory = tempfile::tempdir().expect("probe fixtures");
        let payload = vec![17_u8; 4096];
        let file = directory.path().join("fixture.bin");
        tokio::fs::write(&file, &payload).await.expect("fixture bytes");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("owned RPC listener");
        let address = listener.local_addr().expect("owned RPC address");
        let incoming = futures_util::stream::unfold(listener, |listener| async move {
            Some((listener.accept().await.map(|(stream, _)| stream), listener))
        });
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let signed = Arc::new(AtomicUsize::new(0));
        let corrupt = Arc::new(AtomicBool::new(false));
        let service = FixtureService {
            data: payload.into(),
            signed: signed.clone(),
            corrupt: corrupt.clone(),
        };
        let mut server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(Box::pin(incoming), async {
                    let _ = stopped.await;
                }),
        );
        let mut config = config();
        config.targets = vec![format!("http://{address}/data/grpc-bench-test")];
        config.disk_ids = Some(vec![Uuid::from_u128(42)]);
        config.fixture_file = file;
        let input = serde_json::to_vec(&config).expect("probe manifest");
        let result = run(&input, directory.path(), "test-bench-rpc-secret".into()).await;
        let negative = async {
            let endpoint = config.endpoints()?.remove(0);
            let disk = new_disk(
                &endpoint,
                &DiskOption {
                    cleanup: false,
                    health_check: true,
                },
            )
            .await
            .map_err(Error::other)?;
            disk.set_disk_id(Some(Uuid::from_u128(42))).await.map_err(Error::other)?;
            corrupt.store(true, Ordering::SeqCst);
            let context = Arc::new(super::Context {
                path: format!("tmp/{}/grpc-bench-payload.bin", config.fixture_id),
                config: config.clone(),
                disks: vec![disk.clone()],
                peers: vec![],
                echoes: vec![],
                payload: tokio::fs::read(&config.fixture_file).await?,
            });
            let origin = Instant::now();
            let rows = bulk(context, 0, origin, origin + Duration::from_millis(50)).await?;
            disk.close().await.map_err(Error::other)?;
            Ok::<_, Error>(rows)
        }
        .await;
        let _ = stop.send(());
        let retirement = timeout(Duration::from_secs(5), &mut server).await;
        if retirement.is_err() {
            server.abort();
            let _ = server.await;
        }
        assert!(matches!(retirement, Ok(Ok(Ok(())))), "owned RPC fixture must retire successfully");
        let result = result.expect("production client probe");
        assert!(result.bulk_coverage_verified);
        let report = serde_json::to_value(result).expect("raw sample report");
        assert_eq!(report["errors"], 0);
        assert_eq!(report["control_expected"], 20);
        let rows = report["samples"].as_array().expect("raw rows");
        assert_eq!(rows.iter().filter(|row| row["operation"] == "Ping").count(), 20);
        assert!(rows.iter().any(|row| row["operation"] == "ReadAll"));
        assert!(signed.load(Ordering::Relaxed) > 20);
        let rejected = negative.expect("corrupted-response probe");
        assert!(!rejected.is_empty());
        assert!(rejected.iter().all(|row| row.outcome == "payload_mismatch" && row.bytes == 0));
    }
}
