// Copyright 2026 RustFS Team
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

#![recursion_limit = "256"]

#[cfg(debug_assertions)]
mod deferred_iam_audit {
    use aws_sdk_s3::config::{Credentials, Region};
    use aws_sdk_s3::operation::RequestId;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::{Client, Config};
    use bytes::Bytes;
    use http::{Method, Request, Response, StatusCode};
    use http_body_util::{BodyExt, Empty};
    use hyper::body::Incoming;
    use hyper::service::service_fn;
    use hyper_util::rt::TokioIo;
    use rustfs::embedded::find_available_port;
    use rustfs_config::{ENV_TEST_IAM_FAIL_INIT_ATTEMPTS, ENV_TEST_IAM_RETRY_INTERVAL_MS};
    use rustfs_utils::egress::ENV_OUTBOUND_ALLOW_ORIGINS;
    use serde_json::Value;
    use std::convert::Infallible;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::Stdio;
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio::process::{Child, Command};
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;
    use tokio::time::{Instant, timeout};

    const ACCESS_KEY: &str = "deferred-audit-root";
    const SECRET_KEY: &str = "deferred-audit-root-secret";
    const READY_TIMEOUT: Duration = Duration::from_secs(60);
    const AUDIT_TIMEOUT: Duration = Duration::from_secs(10);

    struct ChildProcessGuard {
        child: Option<Child>,
    }

    impl ChildProcessGuard {
        fn new(child: Child) -> Self {
            Self { child: Some(child) }
        }

        fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
            match self.child.as_mut() {
                Some(child) => child.try_wait(),
                None => Ok(None),
            }
        }

        async fn stop(&mut self) {
            if let Some(mut child) = self.child.take() {
                let _ = child.start_kill();
                let _ = timeout(Duration::from_secs(5), child.wait()).await;
            }
        }
    }

    impl Drop for ChildProcessGuard {
        fn drop(&mut self) {
            if let Some(mut child) = self.child.take() {
                let _ = child.start_kill();
            }
        }
    }

    struct CollectorGuard {
        handle: JoinHandle<()>,
    }

    impl Drop for CollectorGuard {
        fn drop(&mut self) {
            self.handle.abort();
        }
    }

    async fn spawn_audit_webhook_collector() -> (String, String, mpsc::UnboundedReceiver<Value>, CollectorGuard) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind local audit webhook listener");
        let port = listener.local_addr().expect("audit webhook local addr").port();
        let origin = format!("http://127.0.0.1:{port}");
        let endpoint = format!("{origin}/audit");
        let (tx, rx) = mpsc::unbounded_channel();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let tx = tx.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |request: Request<Incoming>| {
                        let tx = tx.clone();
                        async move {
                            let method = request.method().clone();
                            if let Ok(body) = request.into_body().collect().await
                                && method == Method::POST
                                && let Ok(payload) = serde_json::from_slice::<Value>(&body.to_bytes())
                            {
                                if let Some(records) = payload["Records"].as_array() {
                                    for entry in records {
                                        let _ = tx.send(entry.clone());
                                    }
                                } else {
                                    let _ = tx.send(payload);
                                }
                            }
                            Ok::<_, Infallible>(Response::new(Empty::<Bytes>::new()))
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        (origin, endpoint, rx, CollectorGuard { handle })
    }

    fn s3_client(endpoint: &str) -> Client {
        let creds = Credentials::new(ACCESS_KEY, SECRET_KEY, None, None, "deferred-iam-audit-test");
        let config = Config::builder()
            .credentials_provider(creds)
            .region(Region::new("us-east-1"))
            .endpoint_url(endpoint)
            .force_path_style(true)
            .behavior_version_latest()
            .build();
        Client::from_conf(config)
    }

    fn volumes(root: &Path) -> Vec<PathBuf> {
        (1..=4).map(|index| root.join(format!("disk{index}"))).collect()
    }

    fn read_log(log_path: &Path) -> String {
        fs::read_to_string(log_path).unwrap_or_else(|err| format!("<failed to read server log: {err}>"))
    }

    async fn wait_for_audit_entry(
        rx: &mut mpsc::UnboundedReceiver<Value>,
        api_name: &str,
        bucket: &str,
        key: Option<&str>,
        request_id: &str,
        log_path: &Path,
    ) -> Value {
        let deadline = Instant::now() + AUDIT_TIMEOUT;
        let mut seen = Vec::new();
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                panic!(
                    "audit webhook did not receive {api_name} for bucket={bucket:?} key={key:?} request_id={request_id:?}; \
                     received {} other entries: {seen:?}\nserver log:\n{}",
                    seen.len(),
                    read_log(log_path)
                );
            }
            let entry = match timeout(remaining, rx.recv()).await {
                Ok(Some(entry)) => entry,
                Ok(None) => panic!("audit collector channel closed before receiving expected entry"),
                Err(_) => panic!(
                    "timed out waiting for {api_name} audit entry (bucket={bucket:?} key={key:?} request_id={request_id:?}); \
                     received {} other entries: {seen:?}\nserver log:\n{}",
                    seen.len(),
                    read_log(log_path)
                ),
            };
            let matches_api = entry["api"]["name"].as_str() == Some(api_name);
            let matches_bucket = entry["api"]["bucket"].as_str() == Some(bucket);
            let matches_key = key.is_none_or(|expected| entry["api"]["object"].as_str() == Some(expected));
            let matches_id = entry["requestID"].as_str() == Some(request_id);
            if matches_api && matches_bucket && matches_key && matches_id {
                return entry;
            }
            if seen.len() < 16 {
                seen.push(format!(
                    "api={:?} bucket={:?} object={:?} requestID={:?}",
                    entry["api"]["name"].as_str(),
                    entry["api"]["bucket"].as_str(),
                    entry["api"]["object"].as_str(),
                    entry["requestID"].as_str()
                ));
            }
        }
    }

    #[tokio::test]
    async fn deferred_iam_recovery_starts_audit_webhook_without_manual_reload() {
        let port = match find_available_port() {
            Ok(port) => port,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(err) => panic!("find free server port: {err}"),
        };
        let address = format!("127.0.0.1:{port}");
        let server_url = format!("http://{address}");

        let temp_dir = tempfile::tempdir().expect("create isolated test tempdir");
        let volume_paths = volumes(temp_dir.path());
        for path in &volume_paths {
            fs::create_dir_all(path).expect("create local volume dir");
        }
        let queue_dir = temp_dir.path().join("audit-queue");
        fs::create_dir_all(&queue_dir).expect("create audit webhook queue dir");
        let log_path = temp_dir.path().join("rustfs-server.log");
        let log_file = fs::File::create(&log_path).expect("create server log file");

        let (audit_origin, audit_endpoint, mut audit_rx, _collector) = spawn_audit_webhook_collector().await;

        let mut command = Command::new(env!("CARGO_BIN_EXE_rustfs"));
        command
            .args(["--address", &address, "--access-key", ACCESS_KEY, "--secret-key", SECRET_KEY])
            .args(&volume_paths)
            .env(ENV_TEST_IAM_FAIL_INIT_ATTEMPTS, "2")
            .env(ENV_TEST_IAM_RETRY_INTERVAL_MS, "100")
            .env("RUSTFS_AUDIT_ENABLE", "true")
            .env("RUSTFS_AUDIT_WEBHOOK_ENABLE_PRIMARY", "on")
            .env("RUSTFS_AUDIT_WEBHOOK_ENDPOINT_PRIMARY", &audit_endpoint)
            .env("RUSTFS_AUDIT_WEBHOOK_QUEUE_DIR_PRIMARY", &queue_dir)
            .env(ENV_OUTBOUND_ALLOW_ORIGINS, &audit_origin)
            .env("RUSTFS_UNSAFE_BYPASS_DISK_CHECK", "true")
            .env("RUSTFS_CONSOLE_ENABLE", "false")
            .env("NO_PROXY", "127.0.0.1,localhost,::1")
            .env("no_proxy", "127.0.0.1,localhost,::1")
            .env_remove("HTTP_PROXY")
            .env_remove("HTTPS_PROXY")
            .env_remove("http_proxy")
            .env_remove("https_proxy")
            .env_remove("ALL_PROXY")
            .env_remove("all_proxy")
            .env("RUST_LOG", "warn,rustfs::startup_iam=info,rustfs::main::run=info")
            .stdin(Stdio::null())
            .stdout(Stdio::from(log_file.try_clone().expect("clone server log handle")))
            .stderr(Stdio::from(log_file))
            .kill_on_drop(true);

        let mut server = ChildProcessGuard::new(command.spawn().expect("spawn standalone rustfs binary"));

        let http = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_millis(500))
            .build()
            .expect("build local readiness client");
        let ready_url = format!("{server_url}/health/ready");

        let ready_result = timeout(READY_TIMEOUT, async {
            loop {
                if let Ok(response) = http.get(&ready_url).send().await {
                    let status = response.status();
                    if status == StatusCode::OK {
                        return;
                    }
                    assert_eq!(
                        status,
                        StatusCode::SERVICE_UNAVAILABLE,
                        "unexpected non-ready status before recovery: {status}"
                    );
                }
                if let Some(status) = server.try_wait().expect("poll rustfs process") {
                    panic!("rustfs exited before readiness recovery ({status}):\n{}", read_log(&log_path));
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        assert!(
            ready_result.is_ok(),
            "rustfs did not reach /health/ready 200 after deferred IAM bootstrap:\n{}",
            read_log(&log_path)
        );

        let startup_log = read_log(&log_path);
        assert!(
            startup_log.contains("iam_bootstrap_deferred"),
            "expected deferred IAM bootstrap in startup log:\n{startup_log}"
        );
        assert!(
            startup_log.contains("iam_bootstrap_recovered"),
            "expected deferred IAM recovery in startup log:\n{startup_log}"
        );

        let client = s3_client(&server_url);
        let bucket = "deferred-iam-audit-bucket";
        let key = "probe/after-recovery.txt";
        client
            .create_bucket()
            .bucket(bucket)
            .send()
            .await
            .expect("create bucket after deferred IAM recovery");
        let put = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(b"audit-after-deferred-iam"))
            .send()
            .await
            .expect("put object after deferred IAM recovery");
        let request_id = put.request_id().expect("PutObject response should carry a request ID");

        let audit_entry = wait_for_audit_entry(&mut audit_rx, "s3:PutObject", bucket, Some(key), request_id, &log_path).await;
        assert_eq!(
            audit_entry["api"]["status_code"].as_i64(),
            Some(200),
            "unexpected PutObject audit status_code: {audit_entry}"
        );
        assert_eq!(
            audit_entry["accessKey"].as_str(),
            Some(ACCESS_KEY),
            "unexpected PutObject audit accessKey: {audit_entry}"
        );
        assert!(
            !audit_entry.to_string().contains(SECRET_KEY),
            "audit entry must never expose the secret key"
        );

        let final_log = read_log(&log_path);
        assert!(
            !final_log.contains("start_failed"),
            "audit runtime must not fail prematurely before AppContext installation:\n{final_log}"
        );
        assert!(
            final_log.contains("Audit runtime started"),
            "expected audit runtime to start after deferred IAM recovery:\n{final_log}"
        );

        server.stop().await;
    }
}
