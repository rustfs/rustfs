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

//! Negative / lifecycle e2e coverage for the central admin authorization gate
//! (`rustfs/src/admin/auth.rs`), tracking rustfs/backlog#1151 sec-4.
//!
//! These tests exercise the gate end-to-end against a real server, using raw
//! SigV4-signed HTTP (no `awscurl` dependency) so the exact HTTP status and S3
//! error code are asserted:
//!
//! 1. A fully authenticated but non-admin credential is rejected with
//!    `403 AccessDenied` on an admin API (`GET /rustfs/admin/v3/info`), while
//!    the root credential is accepted — proving the rejection is authorization,
//!    not a broken endpoint.
//! 2. Root-credential rotation takes effect across a restart: the new
//!    credential is accepted and the old credential is rejected, on both the
//!    S3 data plane and the admin plane.
//! 3. Starting with the default `rustfsadmin` credentials emits the
//!    default-credentials startup warning (observable operator signal).

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging, local_http_client, rustfs_binary_path};
    use aws_sdk_s3::config::{Credentials, Region};
    use aws_sdk_s3::{Client, Config};
    use http::header::HOST;
    use rustfs_signer::constants::UNSIGNED_PAYLOAD;
    use rustfs_signer::sign_v4;
    use s3s::Body;
    use serial_test::serial;
    use std::error::Error;
    use std::io::Read;
    use std::process::{Command, Stdio};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    const ADMIN_INFO_PATH: &str = "/rustfs/admin/v3/info";
    const ADMIN_MANUAL_TRANSITION_PATH: &str =
        "/rustfs/admin/v3/ilm/transition/run?bucket=auth-deny-manual-transition&maxObjects=1";

    /// Send a SigV4-signed request to `path` (optionally with a JSON `body`) and
    /// return `(status, body)`. Uses the `UNSIGNED_PAYLOAD` content hash so a
    /// request body can be attached without the caller pre-hashing it — the
    /// server verifies the signature against the same sentinel, exactly as the
    /// AWS SDKs / MinIO client do for streaming/unsigned payloads.
    async fn signed_request(
        base_url: &str,
        method: http::Method,
        path: &str,
        body: Option<&str>,
        access_key: &str,
        secret_key: &str,
    ) -> Result<(reqwest::StatusCode, String), Box<dyn Error + Send + Sync>> {
        let url = format!("{base_url}{path}");
        let uri = url.parse::<http::Uri>()?;
        let authority = uri.authority().ok_or("missing authority")?.to_string();
        let body_bytes = body.map(|b| b.as_bytes().to_vec()).unwrap_or_default();

        // The signature is computed over `UNSIGNED_PAYLOAD`, so the body bytes do
        // not participate in the SigV4 hash — sign over an empty body and attach
        // the real payload to the wire request below.
        let request = http::Request::builder()
            .method(method.clone())
            .uri(uri)
            .header(HOST, authority)
            .header("x-amz-content-sha256", UNSIGNED_PAYLOAD);
        let signed = sign_v4(request.body(Body::empty())?, 0, access_key, secret_key, "", "us-east-1");

        let client = local_http_client();
        let mut rb = client.request(method, url.as_str());
        for (name, value) in signed.headers() {
            rb = rb.header(name, value);
        }
        if !body_bytes.is_empty() {
            rb = rb.body(body_bytes);
        }
        let resp = rb.send().await?;
        let status = resp.status();
        let text = resp.text().await?;
        Ok((status, text))
    }

    /// Build an S3 client bound to explicit credentials (used to exercise the S3
    /// data plane with rotated / stale root credentials).
    fn s3_client_with(env: &RustFSTestEnvironment, access_key: &str, secret_key: &str) -> Client {
        let credentials = Credentials::new(access_key, secret_key, None, None, "sec4-admin-auth");
        let config = Config::builder()
            .credentials_provider(credentials)
            .region(Region::new("us-east-1"))
            .endpoint_url(&env.url)
            .force_path_style(true)
            .behavior_version_latest()
            .build();
        Client::from_conf(config)
    }

    /// Create a non-admin IAM user via the admin `add-user` API using the root
    /// credentials. The user is created with no policy attached, so it is a
    /// valid (authenticatable) principal that is nonetheless unauthorized for
    /// admin actions.
    async fn create_limited_user(
        env: &RustFSTestEnvironment,
        access_key: &str,
        secret_key: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let path = format!("/rustfs/admin/v3/add-user?accessKey={access_key}");
        let body = serde_json::json!({ "secretKey": secret_key, "status": "enabled" }).to_string();
        let (status, resp) =
            signed_request(&env.url, http::Method::PUT, &path, Some(&body), &env.access_key, &env.secret_key).await?;
        assert!(status.is_success(), "add-user should succeed (status={status}, body={resp})");
        Ok(())
    }

    /// A fully authenticated but non-admin credential must be rejected with
    /// `403 AccessDenied` on an admin API, while the root credential succeeds.
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn non_admin_credential_denied_on_admin_api() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;

        let user_ak = "sec4limited";
        let user_sk = "sec4limitedsecret";
        create_limited_user(&env, user_ak, user_sk).await?;

        // Root credential is authorized: proves the endpoint works.
        let (root_status, root_body) =
            signed_request(&env.url, http::Method::GET, ADMIN_INFO_PATH, None, &env.access_key, &env.secret_key).await?;
        assert_eq!(
            root_status,
            reqwest::StatusCode::OK,
            "root credential must be authorized on the admin API, body: {root_body}"
        );

        // Non-admin credential is rejected by the admin authorization gate.
        let (status, body) = signed_request(&env.url, http::Method::GET, ADMIN_INFO_PATH, None, user_ak, user_sk).await?;
        assert_eq!(
            status,
            reqwest::StatusCode::FORBIDDEN,
            "non-admin credential must get 403 on the admin API, body: {body}"
        );
        assert!(
            body.contains("AccessDenied"),
            "admin gate rejection must carry the AccessDenied S3 error code, body: {body}"
        );

        env.stop_server();
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn non_admin_credential_denied_on_manual_transition_run() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;

        let user_ak = "ilmtransitionlimited";
        let user_sk = "ilmtransitionlimitedsecret";
        create_limited_user(&env, user_ak, user_sk).await?;

        let (root_status, root_body) = signed_request(
            &env.url,
            http::Method::POST,
            ADMIN_MANUAL_TRANSITION_PATH,
            None,
            &env.access_key,
            &env.secret_key,
        )
        .await?;
        assert_eq!(
            root_status,
            reqwest::StatusCode::OK,
            "root credential must reach the manual transition handler, body: {root_body}"
        );
        assert!(
            root_body.contains("\"mode\":\"enqueue_only\""),
            "root response should be the manual transition JSON contract, body: {root_body}"
        );

        let (status, body) =
            signed_request(&env.url, http::Method::POST, ADMIN_MANUAL_TRANSITION_PATH, None, user_ak, user_sk).await?;
        assert_eq!(
            status,
            reqwest::StatusCode::FORBIDDEN,
            "non-admin credential must get 403 on manual transition run, body: {body}"
        );
        assert!(
            body.contains("AccessDenied"),
            "manual transition rejection must carry the AccessDenied S3 error code, body: {body}"
        );

        env.stop_server();
        Ok(())
    }

    /// Rotating the root credentials (restart with new `--access-key` /
    /// `--secret-key` on the same data directory) takes effect: the new
    /// credential is accepted and the old one is rejected, on both the S3 data
    /// plane and the admin plane.
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn root_credential_rotation_takes_effect() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;

        let old_ak = env.access_key.clone();
        let old_sk = env.secret_key.clone();

        // --- Boot with the original root credential. ---
        env.start_rustfs_server(vec![]).await?;

        // Admin plane accepts the original root credential.
        let (status, body) = signed_request(&env.url, http::Method::GET, ADMIN_INFO_PATH, None, &old_ak, &old_sk).await?;
        assert_eq!(status, reqwest::StatusCode::OK, "original root must reach admin API, body: {body}");
        // S3 plane accepts the original root credential.
        s3_client_with(&env, &old_ak, &old_sk)
            .list_buckets()
            .send()
            .await
            .expect("original root must be able to list buckets on the S3 plane");

        env.stop_server();

        // --- Rotate: restart on the same data dir with a new root credential. ---
        let new_ak = "rotatedroot";
        let new_sk = "rotatedrootsecret";
        env.access_key = new_ak.to_string();
        env.secret_key = new_sk.to_string();
        env.start_rustfs_server(vec![]).await?;

        // New root credential works on both planes.
        let (status, body) = signed_request(&env.url, http::Method::GET, ADMIN_INFO_PATH, None, new_ak, new_sk).await?;
        assert_eq!(status, reqwest::StatusCode::OK, "rotated root must reach admin API, body: {body}");
        s3_client_with(&env, new_ak, new_sk)
            .list_buckets()
            .send()
            .await
            .expect("rotated root must be able to list buckets on the S3 plane");

        // Old root credential is now rejected on both planes.
        let (status, body) = signed_request(&env.url, http::Method::GET, ADMIN_INFO_PATH, None, &old_ak, &old_sk).await?;
        assert_eq!(
            status,
            reqwest::StatusCode::FORBIDDEN,
            "stale root must be rejected on the admin API after rotation, body: {body}"
        );
        let s3_old = s3_client_with(&env, &old_ak, &old_sk).list_buckets().send().await;
        assert!(
            s3_old.is_err(),
            "stale root must be rejected on the S3 plane after rotation, got: {s3_old:?}"
        );

        env.stop_server();
        Ok(())
    }

    /// Booting with the default `rustfsadmin` credentials must emit the
    /// default-credentials startup warning so operators get an observable
    /// signal. The predicate + message text are unit-tested in
    /// `rustfs::startup_server`; this pins that the warning actually fires at
    /// runtime. We capture the child's stdout/stderr directly (the shared
    /// harness inherits stdio) and poll for the warning until it appears.
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn default_credentials_emit_startup_warning() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();

        let port = RustFSTestEnvironment::find_available_port().await?;
        let address = format!("127.0.0.1:{port}");
        let temp_dir = std::env::temp_dir().join(format!("rustfs_sec4_defcred_{}_{}", std::process::id(), port));
        std::fs::create_dir_all(&temp_dir)?;
        let temp_dir_str = temp_dir.display().to_string();

        let mut child = Command::new(rustfs_binary_path())
            .env("RUST_LOG", "rustfs=info")
            .env("RUSTFS_CONSOLE_ENABLE", "false")
            .args([
                "--address",
                &address,
                // Explicitly the compiled-in defaults, so the warning must fire.
                "--access-key",
                "rustfsadmin",
                "--secret-key",
                "rustfsadmin",
                &temp_dir_str,
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        // Drain stdout + stderr concurrently into a shared buffer so a full
        // pipe never blocks the child.
        let captured = Arc::new(Mutex::new(String::new()));
        let mut readers = Vec::new();
        for stream in [
            child.stdout.take().map(|s| Box::new(s) as Box<dyn Read + Send>),
            child.stderr.take().map(|s| Box::new(s) as Box<dyn Read + Send>),
        ]
        .into_iter()
        .flatten()
        {
            let sink = Arc::clone(&captured);
            readers.push(std::thread::spawn(move || {
                let mut stream = stream;
                let mut buf = [0u8; 4096];
                loop {
                    match stream.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            if let Ok(mut guard) = sink.lock() {
                                guard.push_str(&String::from_utf8_lossy(&buf[..n]));
                            }
                        }
                    }
                }
            }));
        }

        const WARNING_NEEDLE: &str = "Detected default root credentials";
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut seen = false;
        while Instant::now() < deadline {
            if let Ok(Some(status)) = child.try_wait() {
                // Process exited; make a final check of what we captured.
                seen = captured.lock().map(|g| g.contains(WARNING_NEEDLE)).unwrap_or(false);
                assert!(
                    seen,
                    "server exited ({status}) before emitting the default-credentials warning; captured: {}",
                    captured.lock().map(|g| g.clone()).unwrap_or_default()
                );
                break;
            }
            if captured.lock().map(|g| g.contains(WARNING_NEEDLE)).unwrap_or(false) {
                seen = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        let _ = child.kill();
        let _ = child.wait();
        for r in readers {
            let _ = r.join();
        }
        let _ = std::fs::remove_dir_all(&temp_dir);

        assert!(
            seen,
            "starting with default credentials must emit the default-credentials warning; captured: {}",
            captured.lock().map(|g| g.clone()).unwrap_or_default()
        );
        Ok(())
    }
}
