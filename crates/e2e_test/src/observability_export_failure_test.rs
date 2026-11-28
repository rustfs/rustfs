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

//! End-to-end observability export fault isolation and redaction regressions
//! (rustfs/backlog#2679).
//!
//! Proves against a real `rustfs` server binary and an isolated local fake OTLP
//! HTTP collector that:
//! - OTLP collector faults (401, 500, connection refused, 5s stalled response
//!   against a 1000ms exporter timeout) never block S3 PUT/GET requests
//!   (bounded under 3s per request).
//! - Subsequent telemetry recovers once the collector returns 200 OK.
//! - Invalid startup OTLP TLS CA configuration still fails closed.
//! - Graceful `SIGTERM` shutdown completes within 10 seconds without hanging.
//! - Secret sentinels (OTLP bearer token, endpoint URL userinfo/query) never
//!   leak into server stdout/stderr logs or captured OTLP payloads.

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging, rustfs_binary_path};
    use aws_sdk_s3::Client;
    use aws_sdk_s3::primitives::ByteStream;
    use flate2::read::GzDecoder;
    use std::error::Error;
    use std::io::Read as _;
    use std::net::SocketAddr;
    use std::process::{Command, Stdio};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::{Notify, oneshot};
    use tokio::time::timeout;

    type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

    const S3_REQUEST_BUDGET: Duration = Duration::from_secs(3);
    const SHUTDOWN_BUDGET: Duration = Duration::from_secs(10);
    const COLLECTOR_STALL_DELAY: Duration = Duration::from_secs(5);
    const OTLP_TIMEOUT_MILLIS: &str = "1000";

    const SENTINEL_OTLP_BEARER: &str = "SENTINEL_E2E_OTLP_BEARER_77319";
    const SENTINEL_URL_PASS: &str = "SENTINEL_E2E_URL_PASS_88421";
    const SENTINEL_URL_QUERY: &str = "SENTINEL_E2E_URL_QUERY_99532";

    #[derive(Clone, Copy, Debug)]
    enum CollectorMode {
        Status(u16),
        DelayThenStatus(Duration, u16),
    }

    #[derive(Clone, Debug)]
    struct CapturedOtlpExport {
        path: String,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    }

    struct FakeOtlpCollector {
        addr: SocketAddr,
        mode: Arc<Mutex<CollectorMode>>,
        exports: Arc<Mutex<Vec<CapturedOtlpExport>>>,
        notify: Arc<Notify>,
        shutdown_tx: Option<oneshot::Sender<()>>,
        task: Option<tokio::task::JoinHandle<()>>,
    }

    impl FakeOtlpCollector {
        async fn start(initial: CollectorMode) -> Result<Self, Box<dyn Error + Send + Sync>> {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let addr = listener.local_addr()?;
            let mode = Arc::new(Mutex::new(initial));
            let exports = Arc::new(Mutex::new(Vec::new()));
            let notify = Arc::new(Notify::new());
            let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

            let mode_bg = Arc::clone(&mode);
            let exports_bg = Arc::clone(&exports);
            let notify_bg = Arc::clone(&notify);

            let task = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = &mut shutdown_rx => break,
                        accept_res = listener.accept() => {
                            let Ok((mut stream, _)) = accept_res else { break };
                            let mode_conn = Arc::clone(&mode_bg);
                            let exports_conn = Arc::clone(&exports_bg);
                            let notify_conn = Arc::clone(&notify_bg);
                            tokio::spawn(async move {
                                let mut raw = Vec::new();
                                let mut buf = [0_u8; 8192];
                                let header_end = loop {
                                    let Ok(read) = stream.read(&mut buf).await else { return };
                                    if read == 0 {
                                        return;
                                    }
                                    raw.extend_from_slice(&buf[..read]);
                                    if let Some(pos) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                                        break pos + 4;
                                    }
                                    if raw.len() > 128 * 1024 {
                                        return;
                                    }
                                };
                                let head = String::from_utf8_lossy(&raw[..header_end]);
                                let mut lines = head.split("\r\n");
                                let path = lines
                                    .next()
                                    .and_then(|line| line.split_whitespace().nth(1))
                                    .unwrap_or_default()
                                    .to_string();
                                let mut headers = Vec::new();
                                let mut content_length: usize = 0;
                                for line in lines {
                                    if let Some((k, v)) = line.split_once(':') {
                                        let key = k.trim().to_ascii_lowercase();
                                        let val = v.trim().to_string();
                                        if key == "content-length" {
                                            content_length = val.parse().unwrap_or(0);
                                        }
                                        headers.push((key, val));
                                    }
                                }
                                while raw.len().saturating_sub(header_end) < content_length {
                                    let Ok(read) = stream.read(&mut buf).await else { break };
                                    if read == 0 {
                                        break;
                                    }
                                    raw.extend_from_slice(&buf[..read]);
                                }
                                let body = raw[header_end..].to_vec();
                                exports_conn.lock().expect("lock OTLP exports").push(CapturedOtlpExport {
                                    path,
                                    headers,
                                    body,
                                });
                                notify_conn.notify_waiters();

                                let current_mode = *mode_conn.lock().expect("lock collector mode");
                                let status = match current_mode {
                                    CollectorMode::Status(code) => code,
                                    CollectorMode::DelayThenStatus(delay, code) => {
                                        tokio::time::sleep(delay).await;
                                        code
                                    }
                                };
                                let reason = match status {
                                    200 => "OK",
                                    401 => "Unauthorized",
                                    500 => "Internal Server Error",
                                    _ => "Error",
                                };
                                let response = format!(
                                    "HTTP/1.1 {status} {reason}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                                );
                                let _ = stream.write_all(response.as_bytes()).await;
                                let _ = stream.shutdown().await;
                            });
                        }
                    }
                }
            });

            Ok(Self {
                addr,
                mode,
                exports,
                notify,
                shutdown_tx: Some(shutdown_tx),
                task: Some(task),
            })
        }

        fn credentialed_endpoint(&self) -> String {
            format!("http://otel-user:{SENTINEL_URL_PASS}@{}?api_key={SENTINEL_URL_QUERY}", self.addr)
        }

        fn set_mode(&self, mode: CollectorMode) {
            *self.mode.lock().expect("lock collector mode") = mode;
        }

        fn export_count(&self) -> usize {
            self.exports.lock().expect("lock OTLP exports").len()
        }

        fn snapshot_exports(&self) -> Vec<CapturedOtlpExport> {
            self.exports.lock().expect("lock OTLP exports").clone()
        }

        async fn wait_for_exports_gt(&self, previous: usize, deadline: Duration) -> bool {
            let start = Instant::now();
            loop {
                let notified = self.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.export_count() > previous {
                    return true;
                }
                let remaining = deadline.saturating_sub(start.elapsed());
                if remaining.is_zero() {
                    return false;
                }
                let _ = timeout(remaining, notified).await;
            }
        }

        async fn stop(mut self) {
            if let Some(tx) = self.shutdown_tx.take() {
                let _ = tx.send(());
            }
            if let Some(handle) = self.task.take() {
                handle.abort();
                let _ = handle.await;
            }
        }
    }

    async fn put_and_get_within_budget(client: &Client, bucket: &str, key: &str, payload: &[u8]) -> TestResult {
        let put_start = Instant::now();
        timeout(
            S3_REQUEST_BUDGET,
            client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(ByteStream::from(payload.to_vec()))
                .send(),
        )
        .await
        .map_err(|_| format!("PUT {bucket}/{key} exceeded {S3_REQUEST_BUDGET:?} budget"))??;
        let put_elapsed = put_start.elapsed();
        assert!(
            put_elapsed < S3_REQUEST_BUDGET,
            "PUT {bucket}/{key} took {put_elapsed:?}, exceeding {S3_REQUEST_BUDGET:?}"
        );

        let get_start = Instant::now();
        let response = timeout(S3_REQUEST_BUDGET, client.get_object().bucket(bucket).key(key).send())
            .await
            .map_err(|_| format!("GET {bucket}/{key} headers exceeded {S3_REQUEST_BUDGET:?} budget"))??;
        let body = timeout(S3_REQUEST_BUDGET, response.body.collect())
            .await
            .map_err(|_| format!("GET {bucket}/{key} body exceeded {S3_REQUEST_BUDGET:?} budget"))??
            .into_bytes();
        let get_elapsed = get_start.elapsed();
        assert!(
            get_elapsed < S3_REQUEST_BUDGET,
            "GET {bucket}/{key} took {get_elapsed:?}, exceeding {S3_REQUEST_BUDGET:?}"
        );
        assert_eq!(body.as_ref(), payload, "GET {bucket}/{key} returned corrupted payload");
        Ok(())
    }

    async fn stop_server_gracefully_within_budget(env: &mut RustFSTestEnvironment) -> TestResult {
        let Some(mut child) = env.process.take() else {
            return Ok(());
        };
        let pid = child.id();
        let started = Instant::now();
        #[cfg(unix)]
        {
            let status = Command::new("kill").args(["-TERM", &pid.to_string()]).status()?;
            assert!(status.success(), "failed to send SIGTERM to rustfs pid {pid}");
        }
        #[cfg(not(unix))]
        {
            let _ = child.kill();
        }

        loop {
            if child.try_wait()?.is_some() {
                let elapsed = started.elapsed();
                assert!(
                    elapsed < SHUTDOWN_BUDGET,
                    "rustfs shutdown took {elapsed:?}, exceeding {SHUTDOWN_BUDGET:?}"
                );
                return Ok(());
            }
            if started.elapsed() >= SHUTDOWN_BUDGET {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!("rustfs pid {pid} did not exit within {SHUTDOWN_BUDGET:?} after SIGTERM").into());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn assert_no_secret_sentinels(label: &str, text: &str) {
        for sentinel in [SENTINEL_OTLP_BEARER, SENTINEL_URL_PASS, SENTINEL_URL_QUERY] {
            assert!(!text.contains(sentinel), "{label} leaked secret sentinel `{sentinel}`");
        }
    }

    fn decode_otlp_body_lossy(export: &CapturedOtlpExport) -> String {
        let is_gzip = export
            .headers
            .iter()
            .any(|(k, v)| k == "content-encoding" && v.eq_ignore_ascii_case("gzip"));
        if is_gzip {
            let mut decoder = GzDecoder::new(export.body.as_slice());
            let mut decompressed = Vec::new();
            if decoder.read_to_end(&mut decompressed).is_ok() {
                return String::from_utf8_lossy(&decompressed).into_owned();
            }
        }
        String::from_utf8_lossy(&export.body).into_owned()
    }

    #[tokio::test]
    async fn test_s3_data_plane_isolated_across_otlp_401_500_stalled_timeout_and_recovery() -> TestResult {
        init_logging();

        let collector = FakeOtlpCollector::start(CollectorMode::Status(200)).await?;
        let endpoint = collector.credentialed_endpoint();
        let headers = format!("Authorization=Bearer%20{SENTINEL_OTLP_BEARER}");

        let mut env = RustFSTestEnvironment::new().await?;
        let log_path = format!("{}/server.log", env.temp_dir);
        env.capture_log_path = Some(log_path.clone());

        let extra_env: Vec<(&str, &str)> = vec![
            ("RUSTFS_OBS_ENDPOINT", &endpoint),
            ("RUSTFS_OBS_ENDPOINT_HEADERS", &headers),
            ("RUSTFS_OBS_ENDPOINT_TIMEOUT_MILLIS", OTLP_TIMEOUT_MILLIS),
            ("RUSTFS_OBS_METER_INTERVAL", "1"),
            ("RUSTFS_OBS_USE_STDOUT", "false"),
            ("RUSTFS_OBS_LOG_STDOUT_ENABLED", "true"),
            ("RUSTFS_OBS_LOGGER_LEVEL", "info"),
            ("RUST_LOG", "rustfs=info,rustfs_obs=info"),
            ("NO_PROXY", "127.0.0.1,localhost"),
            ("no_proxy", "127.0.0.1,localhost"),
            ("HTTP_PROXY", ""),
            ("HTTPS_PROXY", ""),
            ("http_proxy", ""),
            ("https_proxy", ""),
            ("ALL_PROXY", ""),
            ("all_proxy", ""),
        ];
        env.start_rustfs_server_without_cleanup_with_env(&extra_env).await?;
        let client = env.create_s3_client();

        let bucket = "obs-fault-isolation";
        timeout(S3_REQUEST_BUDGET, client.create_bucket().bucket(bucket).send())
            .await
            .map_err(|_| "create_bucket exceeded S3 budget")??;

        // 1. Baseline (healthy collector returning 200 OK).
        put_and_get_within_budget(&client, bucket, "phase-baseline.txt", b"baseline-ok-payload").await?;
        assert!(
            collector.wait_for_exports_gt(0, Duration::from_secs(6)).await,
            "fake OTLP collector must observe baseline telemetry exports"
        );
        let baseline_exports = collector.snapshot_exports();
        assert!(
            baseline_exports.iter().any(|export| {
                export
                    .headers
                    .iter()
                    .any(|(k, v)| k == "authorization" && v == &format!("Bearer {SENTINEL_OTLP_BEARER}"))
            }),
            "OTLP collector must observe configured Authorization header on baseline export"
        );

        // 2. Collector fault: HTTP 401 Unauthorized.
        collector.set_mode(CollectorMode::Status(401));
        let before_401 = collector.export_count();
        put_and_get_within_budget(&client, bucket, "phase-401.txt", b"payload-during-401").await?;
        assert!(
            collector.wait_for_exports_gt(before_401, Duration::from_secs(6)).await,
            "collector must observe export attempt during 401 fault window"
        );

        // 3. Collector fault: HTTP 500 Internal Server Error.
        collector.set_mode(CollectorMode::Status(500));
        let before_500 = collector.export_count();
        put_and_get_within_budget(&client, bucket, "phase-500.txt", b"payload-during-500").await?;
        assert!(
            collector.wait_for_exports_gt(before_500, Duration::from_secs(6)).await,
            "collector must observe export attempt during 500 fault window"
        );

        // 4. Collector fault: 5s stall with no response body vs 1000ms exporter timeout.
        // Multiple independent S3 requests must each complete well below 3s.
        collector.set_mode(CollectorMode::DelayThenStatus(COLLECTOR_STALL_DELAY, 200));
        let before_stall = collector.export_count();
        for idx in 0..3 {
            let key = format!("phase-stall-{idx}.txt");
            let data = format!("stalled-collector-isolated-payload-{idx}");
            put_and_get_within_budget(&client, bucket, &key, data.as_bytes()).await?;
        }
        assert!(
            collector.wait_for_exports_gt(before_stall, Duration::from_secs(6)).await,
            "stalled collector must receive concurrent export attempt(s)"
        );

        // 5. Recovery: collector returns 200 OK again; S3 and OTLP both continue cleanly.
        collector.set_mode(CollectorMode::Status(200));
        let before_recovery = collector.export_count();
        put_and_get_within_budget(&client, bucket, "phase-recovered.txt", b"payload-after-recovery").await?;
        assert!(
            collector.wait_for_exports_gt(before_recovery, Duration::from_secs(6)).await,
            "recovered collector must receive fresh telemetry exports"
        );

        // 6. Graceful shutdown must finish within 10s.
        stop_server_gracefully_within_budget(&mut env).await?;

        // 7. Scan captured server logs and OTLP payload bodies for secret sentinels.
        let server_log = std::fs::read_to_string(&log_path)?;
        assert!(
            server_log.contains("Initialized observability"),
            "expected observability startup log to be captured before scanning for secret sentinels; log:\n{server_log}"
        );
        assert_no_secret_sentinels("server stdout/stderr log", &server_log);
        for export in collector.snapshot_exports() {
            let body_text = decode_otlp_body_lossy(&export);
            assert_no_secret_sentinels(&format!("OTLP payload {}", export.path), &body_text);
        }

        collector.stop().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_s3_data_plane_survives_otlp_connection_refused_and_exits_within_budget() -> TestResult {
        init_logging();

        // Reserve and immediately release an ephemeral loopback port to guarantee ECONNREFUSED.
        let closed_listener = TcpListener::bind("127.0.0.1:0").await?;
        let closed_addr = closed_listener.local_addr()?;
        drop(closed_listener);

        let endpoint = format!("http://otel-user:{SENTINEL_URL_PASS}@{closed_addr}?api_key={SENTINEL_URL_QUERY}");
        let headers = format!("Authorization=Bearer%20{SENTINEL_OTLP_BEARER}");

        let mut env = RustFSTestEnvironment::new().await?;
        let log_path = format!("{}/server.log", env.temp_dir);
        env.capture_log_path = Some(log_path.clone());

        let extra_env: Vec<(&str, &str)> = vec![
            ("RUSTFS_OBS_ENDPOINT", &endpoint),
            ("RUSTFS_OBS_ENDPOINT_HEADERS", &headers),
            ("RUSTFS_OBS_ENDPOINT_TIMEOUT_MILLIS", OTLP_TIMEOUT_MILLIS),
            ("RUSTFS_OBS_METER_INTERVAL", "1"),
            ("RUSTFS_OBS_USE_STDOUT", "false"),
            ("RUSTFS_OBS_LOG_STDOUT_ENABLED", "true"),
            ("RUSTFS_OBS_LOGGER_LEVEL", "info"),
            ("RUST_LOG", "rustfs=info,rustfs_obs=info"),
            ("NO_PROXY", "127.0.0.1,localhost"),
            ("no_proxy", "127.0.0.1,localhost"),
            ("HTTP_PROXY", ""),
            ("HTTPS_PROXY", ""),
            ("http_proxy", ""),
            ("https_proxy", ""),
            ("ALL_PROXY", ""),
            ("all_proxy", ""),
        ];
        env.start_rustfs_server_without_cleanup_with_env(&extra_env).await?;
        let client = env.create_s3_client();

        let bucket = "obs-conn-refused";
        timeout(S3_REQUEST_BUDGET, client.create_bucket().bucket(bucket).send())
            .await
            .map_err(|_| "create_bucket exceeded S3 budget with connection-refused OTLP collector")??;
        put_and_get_within_budget(&client, bucket, "conn-refused.txt", b"data-plane-unaffected").await?;

        stop_server_gracefully_within_budget(&mut env).await?;

        let server_log = std::fs::read_to_string(&log_path)?;
        assert!(
            server_log.contains("Initialized observability"),
            "expected observability startup log to be captured before scanning for secret sentinels; log:\n{server_log}"
        );
        assert_no_secret_sentinels("connection-refused server log", &server_log);
        Ok(())
    }

    #[tokio::test]
    async fn test_startup_rejects_invalid_otlp_tls_ca_configuration_fail_closed() -> TestResult {
        init_logging();

        let env = RustFSTestEnvironment::new().await?;
        let binary = rustfs_binary_path();
        let endpoint = format!("http://otel-user:{SENTINEL_URL_PASS}@127.0.0.1:4318?api_key={SENTINEL_URL_QUERY}");
        let headers = format!("Authorization=Bearer%20{SENTINEL_OTLP_BEARER}");

        let started = Instant::now();
        let output = Command::new(&binary)
            .args([
                "--address",
                &env.address,
                "--access-key",
                &env.access_key,
                "--secret-key",
                &env.secret_key,
                &env.temp_dir,
            ])
            .env("RUSTFS_CONSOLE_ENABLE", "false")
            .env("RUSTFS_OBS_ENDPOINT", &endpoint)
            .env("RUSTFS_OBS_ENDPOINT_HEADERS", &headers)
            .env("RUSTFS_OBS_TLS_CA_FILE", "relative-invalid-ca.pem")
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env_remove("HTTP_PROXY")
            .env_remove("HTTPS_PROXY")
            .env_remove("http_proxy")
            .env_remove("https_proxy")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()?;

        assert!(
            started.elapsed() < SHUTDOWN_BUDGET,
            "invalid OTLP TLS CA startup rejection should fail fast"
        );
        assert!(
            !output.status.success(),
            "relative RUSTFS_OBS_TLS_CA_FILE must cause startup to fail closed"
        );
        let combined = format!("{}{}", String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
        assert!(
            !combined.trim().is_empty(),
            "invalid OTLP TLS CA startup rejection must emit diagnostic output before scanning for secret sentinels"
        );
        assert_no_secret_sentinels("invalid-config startup output", &combined);
        Ok(())
    }
}
