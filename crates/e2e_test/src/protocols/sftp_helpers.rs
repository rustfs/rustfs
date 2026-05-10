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

//! Shared helpers for SFTP protocol tests
//!
//! An accept-any host-key handler, an ed25519 host-key generator that
//! matches the rustfs config loader permission gates, a russh client
//! connector that authenticates with the default access key, and an
//! SFTP-read-to-vec helper.

use crate::protocols::test_env::{DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY};
use anyhow::{Result, anyhow};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::{Credentials, Region};
use aws_smithy_http_client::Builder as SmithyHttpClientBuilder;
use russh::client::{self, Handle};
use russh::keys::ssh_key::LineEnding;
use russh::keys::{Algorithm, PrivateKey, PublicKey};
use russh_sftp::client::SftpSession;
use russh_sftp::protocol::OpenFlags;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Child;
use tokio::time::sleep;
use tracing::info;

/// Accept-any server-key client handler. The test server uses a host key
/// generated fresh at the start of each run, so strict verification would
/// always fail. The suite exercises the auth path, not the host-key-trust
/// path, which is out of scope for this suite.
pub struct AcceptAnyServerKey;

impl client::Handler for AcceptAnyServerKey {
    type Error = anyhow::Error;

    async fn check_server_key(&mut self, _server_public_key: &PublicKey) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

/// Generate an ed25519 host key pair in host_key_dir with mode 0600. The
/// key is generated in-process via russh::keys so the test suite has no
/// host-tooling dependency on ssh-keygen, which is absent on Alpine,
/// distroless, scratch, and Windows images. The RustFS config loader
/// accepts the OpenSSH private-key format that PrivateKey::to_openssh
/// emits. Both the private key and the .pub file need 0600 because the
/// loader scans every entry in the directory and rejects the whole
/// directory as insecure unless each file is 0600 or 0400.
pub async fn generate_host_key(host_key_dir: &Path) -> Result<()> {
    tokio::fs::create_dir_all(host_key_dir).await?;
    let key_path = host_key_dir.join("ssh_host_ed25519_key");
    let pub_path = host_key_dir.join("ssh_host_ed25519_key.pub");

    let private_key =
        PrivateKey::random(&mut rand::rng(), Algorithm::Ed25519).map_err(|e| anyhow!("ed25519 key generation failed: {e}"))?;
    let private_pem = private_key
        .to_openssh(LineEnding::LF)
        .map_err(|e| anyhow!("OpenSSH private-key encode failed: {e}"))?;
    let public_text = private_key
        .public_key()
        .to_openssh()
        .map_err(|e| anyhow!("OpenSSH public-key encode failed: {e}"))?;

    tokio::fs::write(&key_path, private_pem.as_bytes()).await?;
    tokio::fs::write(&pub_path, format!("{public_text}\n")).await?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        for path in [&key_path, &pub_path] {
            let mut perm = std::fs::metadata(path)?.permissions();
            perm.set_mode(0o600);
            std::fs::set_permissions(path, perm)?;
        }
    }
    Ok(())
}

/// Owns a spawned rustfs server child process and guarantees the process
/// is sent SIGKILL even if the test panics. The wrapper exists because
/// tokio::process::Child does not kill on Drop on stable Rust, so a
/// panicking test would otherwise leak a running rustfs binary that
/// keeps its listener port held until the test runner exits.
///
/// Use kill_and_wait on the success and Err paths to reap the child
/// cleanly; Drop only fires the synchronous SIGKILL when those paths
/// were skipped (panic unwind, runtime abort).
pub struct ServerProcess {
    inner: Option<Child>,
}

impl ServerProcess {
    pub fn new(child: Child) -> Self {
        Self { inner: Some(child) }
    }

    /// Borrow the inner Child for callers that need stdout piping or
    /// other tokio::process APIs.
    pub fn child_mut(&mut self) -> &mut Child {
        self.inner.as_mut().expect("ServerProcess: child already taken")
    }

    /// Async kill plus wait. Idempotent. Use on every success and Err
    /// path. After this returns, Drop becomes a no-op.
    pub async fn kill_and_wait(&mut self) {
        if let Some(mut child) = self.inner.take() {
            let _ = child.kill().await;
            let _ = child.wait().await;
        }
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        if let Some(child) = self.inner.as_mut() {
            // Synchronous SIGKILL via the kernel. Runs even on panic
            // unwind. wait() is skipped here (Drop cannot await), so
            // the process becomes a zombie reaped by the runtime.
            let _ = child.start_kill();
        }
    }
}

/// Open a russh client session against the given address, authenticate
/// with the default access key, request the SFTP subsystem, and return
/// the session handle plus the SFTP wrapper. The handle is returned so
/// the caller can keep the underlying SSH transport alive for the full
/// session and disconnect cleanly afterwards.
pub async fn connect_sftp_to(address: &str) -> Result<(Handle<AcceptAnyServerKey>, SftpSession)> {
    let config = Arc::new(client::Config::default());
    let mut session = client::connect(config, address, AcceptAnyServerKey).await?;
    let auth = session.authenticate_password(DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY).await?;
    if !auth.success() {
        return Err(anyhow!("SFTP password auth rejected"));
    }
    let channel = session.channel_open_session().await?;
    channel.request_subsystem(true, "sftp").await?;
    let sftp = SftpSession::new(channel.into_stream()).await?;
    Ok((session, sftp))
}

/// Read an SFTP object into memory.
pub async fn sftp_read_full(sftp: &SftpSession, path: &str) -> Result<Vec<u8>> {
    let mut file = sftp.open_with_flags(path, OpenFlags::READ).await?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).await?;
    file.shutdown().await?;
    Ok(buf)
}

/// Construct an aws-sdk-s3 client wired against an http rustfs endpoint.
/// Uses the same credential constants the SFTP session authenticates with so
/// both protocols see the same backend identity.
pub fn build_test_s3_client(endpoint_url: &str) -> S3Client {
    let credentials = Credentials::new(DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY, None, None, "sftp-helpers");
    let mut config = aws_sdk_s3::Config::builder()
        .credentials_provider(credentials)
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint_url)
        .force_path_style(true)
        .behavior_version_latest();
    if endpoint_url.starts_with("http://") {
        config = config.http_client(SmithyHttpClientBuilder::new().build_http());
    }
    S3Client::from_conf(config.build())
}

/// Poll the S3 endpoint until ListBuckets returns successfully or the
/// attempt budget is exhausted. The TCP-level wait_for_port_ready check is
/// not enough on its own because rustfs accepts connections before the S3
/// stack has finished initialising.
pub async fn wait_for_s3_ready(client: &S3Client, max_attempts: u32) -> Result<()> {
    for attempt in 0..max_attempts {
        if client.list_buckets().send().await.is_ok() {
            info!("S3 endpoint ready after {} attempts", attempt + 1);
            return Ok(());
        }
        sleep(Duration::from_secs(1)).await;
    }
    Err(anyhow!("S3 endpoint did not become ready"))
}
