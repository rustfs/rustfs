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

//! SFTP compliance regression suite.
//!
//! Per-case assertions that close coverage gaps in SFTP testing.
//! sftp_core covers core functionality. Tests here cover compliance
//! with the SFTP spec, lifecycle invariants under abnormal client
//! patterns (half-close, wedge, paused-drain), and pipelining shapes
//! the OpenSSH and paramiko coverage in sftp_core does not exercise.
//! Each case carries a CMPTST-NN identifier so a failure log line
//! points at a single named property.
//!
//! The shared-server entry test_sftp_compliance_suite spawns one
//! rustfs binary and runs CMPTST-01 through CMPTST-14 against the
//! same session. CMPTST-15 through CMPTST-23 cover read-only mode
//! and run under test_sftp_compliance_readonly with a separate rustfs
//! server started with RUSTFS_SFTP_READ_ONLY=true. The remaining
//! cases (CMPTST-24 through CMPTST-33) each spawn a dedicated rustfs
//! server because the property under test depends on a specific
//! per-server configuration (idle timeout, read-cache window) or a
//! dedicated TCP port.
//!
//! # Case index
//!
//! Shared server (test_sftp_compliance_suite):
//!
//! - CMPTST-01: medium-binary upload then download with SHA256 compare,
//!   single-shot PutObject path below the multipart boundary.
//! - CMPTST-02: zero-byte upload, download, and stat-size match.
//! - CMPTST-03: rm against a bucket path is rejected and the bucket
//!   still exists.
//! - CMPTST-04: rmdir against a non-empty bucket is rejected and the
//!   contained object survives.
//! - CMPTST-05: rmdir against a non-empty sub-directory is rejected
//!   and the inner object survives.
//! - CMPTST-06: open with a path-traversal pattern cannot leak a
//!   host file via SFTP read.
//! - CMPTST-07: read_dir of /.. either errors or returns a listing
//!   that contains no host system entries.
//! - CMPTST-08: rename across buckets writes the payload byte-for-byte
//!   at the destination and removes the source object.
//! - CMPTST-09: paths with embedded spaces round-trip through the
//!   russh-sftp client.
//! - CMPTST-10: read_link is rejected (S3 storage has no symlinks).
//! - CMPTST-11: SETSTAT on a path and FSETSTAT on a separate open
//!   handle both return ok (rsync, WinSCP transfer-success contract).
//! - CMPTST-12: rename to the same path is a no-op and the file stays
//!   in place with the original payload (no copy-then-delete data
//!   loss).
//! - CMPTST-13: nested-key upload creates the parent directory
//!   implicitly and three listing forms show the inner file
//!   (implicit-directory round-trip).
//! - CMPTST-14: OPEN, WRITE, FSETSTAT, CLOSE on the same write handle
//!   all return ok (WinSCP packet sequence, where FSETSTAT against an
//!   in-flight write handle must not error).
//!
//! Read-only spawn (test_sftp_compliance_readonly,
//! RUSTFS_SFTP_READ_ONLY=true):
//!
//! - CMPTST-15: put through SFTP is rejected.
//! - CMPTST-16: rm through SFTP is rejected.
//! - CMPTST-17: mkdir through SFTP is rejected.
//! - CMPTST-18: rmdir through SFTP is rejected.
//! - CMPTST-19: rename through SFTP is rejected.
//! - CMPTST-20: ls through SFTP is allowed and lists the seeded bucket.
//! - CMPTST-21: get through SFTP is allowed and returns the seeded
//!   payload byte-for-byte.
//! - CMPTST-22: SETSTAT on a path is rejected with PermissionDenied.
//! - CMPTST-23: FSETSTAT on a read handle is rejected with
//!   PermissionDenied.
//!
//! Standalone-server cases:
//!
//! - CMPTST-24: concurrent half-close burst does not leak server-side
//!   session tasks (TCP half-close mid-transfer must drain).
//! - CMPTST-25: wedge-kill watchdog kills sessions parked on the russh
//!   per-channel mpsc behind a CLOSE_WAIT socket.
//! - CMPTST-26: healthy idle session past the watchdog fast-kill
//!   threshold stays alive (procfs ESTABLISHED discriminator must not
//!   false-kill).
//! - CMPTST-27: sustained-read thrash, multi-GiB downloads on N
//!   parallel sessions all byte-identical to seed.
//! - CMPTST-28: 5 MB download intact under concurrent metadata storm
//!   on a parallel session.
//! - CMPTST-29: high-volume read-past-EOF pipelining completes inside
//!   the deadline and every read returns EOF.
//! - CMPTST-30: per-operation handler latency stays inside the
//!   ceiling under parallel pipelined sessions (ignored by default).
//! - CMPTST-31: server resilience under client paused-drain, byte-exact
//!   completion after a mid-transfer pause window.
//! - CMPTST-32: read-cache enabled regression, 8 MiB download
//!   byte-exact with the production cache window.
//! - CMPTST-33: read-cache disabled regression, 8 MiB download
//!   byte-exact with RUSTFS_SFTP_READ_CACHE_WINDOW_BYTES=0.

use crate::common::rustfs_binary_path_with_features;
use crate::protocols::sftp_helpers::{
    AcceptAnyServerKey, ServerProcess, build_test_s3_client, connect_sftp_to, generate_host_key, sftp_read_full,
    wait_for_s3_ready,
};
use crate::protocols::test_env::{DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY, ProtocolTestEnvironment};
use anyhow::{Result, anyhow};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::primitives::ByteStream;
use futures::stream::{FuturesUnordered, StreamExt};
use russh::client;
use russh_sftp::client::{Config, SftpSession};
use russh_sftp::protocol::{FileAttributes, OpenFlags, StatusCode};
#[cfg(target_os = "linux")]
use rustfs_config::ENV_SFTP_IDLE_TIMEOUT;
use rustfs_config::{
    ENV_CONSOLE_ENABLE, ENV_RUSTFS_ADDRESS, ENV_SFTP_ADDRESS, ENV_SFTP_ENABLE, ENV_SFTP_HOST_KEY_DIR, ENV_SFTP_PART_SIZE,
    ENV_SFTP_READ_CACHE_WINDOW_BYTES, ENV_SFTP_READ_ONLY,
};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::Arc;
#[cfg(target_os = "linux")]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt, BufReader, ReadBuf};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::process::{Child, Command};
#[cfg(target_os = "linux")]
use tokio::time::sleep;
use tokio::time::timeout;
use tracing::info;

// Cross-case constants used by every spawn helper. Pinned to 5 MiB so
// the multipart boundary is deterministic across runs.
const PART_SIZE_ENV: &str = "5242880";

// Number of attempts the pipelining-style cases give the rustfs S3
// endpoint before failing the readiness wait.
const S3_READY_ATTEMPTS: u32 = 30;

// Logger level passed to spawned rustfs binaries that capture stdout
// for diagnostic dumps. Protocol-level info plus warn-everywhere keeps
// the log volume bounded while preserving the SFTP traces the
// failure-path dumps look for.
const PIPELINING_OBS_LOGGER_LEVEL: &str = "warn,rustfs_protocols=info,rustfs_protocols::sftp::diag=info";

// RUST_LOG is a stdlib-side env var with no canonical constant in the
// rustfs config crate. Left as a literal at the call sites that set it.

// Cross-case fixture parameters used by the pipelining cases that
// exercise the GUI-client traversal shape (a multi-MB fixture object
// next to a sub-directory containing several siblings).
const FIXTURE_SIZE: usize = 5 * 1024 * 1024;
const SUBDIR_FILE_COUNT: usize = 200;

// Pattern multiplier for deterministic seed payloads. Every case that
// seeds a multi-MB fixture and verifies via SHA256 reads from this.
const THRASH_PATTERN_MULTIPLIER: u8 = 13;

/// Build a fresh ProtocolTestEnvironment, generate a per-test ed25519
/// host key, and spawn a rustfs binary configured for SFTP compliance
/// testing on the given bind addresses. The caller owns both returned
/// values: the env keeps the temp directory alive (its Drop cleans it
/// up), and the ServerProcess wrapper guarantees a SIGKILL on every
/// path including panic unwind.
pub(crate) async fn spawn_compliance_rustfs(
    sftp_address: &str,
    s3_address: &str,
    read_only: bool,
) -> Result<(ProtocolTestEnvironment, ServerProcess)> {
    let env = ProtocolTestEnvironment::new().map_err(|e| anyhow!("{}", e))?;
    let host_key_dir = PathBuf::from(&env.temp_dir).join("sftp_host_keys");
    generate_host_key(&host_key_dir).await?;

    let binary_path = rustfs_binary_path_with_features(Some("ftps,webdav,sftp"));
    let host_key_dir_str = host_key_dir
        .to_str()
        .ok_or_else(|| anyhow!("host key dir path is not utf-8: {}", host_key_dir.display()))?;
    let child = Command::new(&binary_path)
        .env(ENV_SFTP_ENABLE, "true")
        .env(ENV_SFTP_ADDRESS, sftp_address)
        .env(ENV_SFTP_HOST_KEY_DIR, host_key_dir_str)
        .env(ENV_SFTP_READ_ONLY, if read_only { "true" } else { "false" })
        .env(ENV_SFTP_PART_SIZE, PART_SIZE_ENV)
        .env(ENV_RUSTFS_ADDRESS, s3_address)
        .arg(&env.temp_dir)
        .spawn()?;
    Ok((env, ServerProcess::new(child)))
}

/// Build a fresh ProtocolTestEnvironment, generate a per-test ed25519
/// host key, and spawn a rustfs binary configured for the pipelining
/// regression cases. Same ownership contract as spawn_compliance_rustfs.
async fn spawn_pipelining_rustfs(sftp_address: &str, s3_address: &str) -> Result<(ProtocolTestEnvironment, ServerProcess)> {
    spawn_pipelining_rustfs_with_extras(sftp_address, s3_address, &[]).await
}

/// Variant of spawn_pipelining_rustfs that layers additional
/// environment variables onto the spawned rustfs binary. Pairs of
/// (name, value) are forwarded as Command::env calls in iteration
/// order so a later pair overrides an earlier one for the same name.
async fn spawn_pipelining_rustfs_with_extras(
    sftp_address: &str,
    s3_address: &str,
    extra_env: &[(&str, &str)],
) -> Result<(ProtocolTestEnvironment, ServerProcess)> {
    let env = ProtocolTestEnvironment::new().map_err(|e| anyhow!("{}", e))?;
    let host_key_dir = PathBuf::from(&env.temp_dir).join("sftp_host_keys");
    generate_host_key(&host_key_dir).await?;

    let binary_path = rustfs_binary_path_with_features(Some("ftps,webdav,sftp"));
    let host_key_dir_str = host_key_dir
        .to_str()
        .ok_or_else(|| anyhow!("host key dir path is not utf-8: {}", host_key_dir.display()))?;
    let mut cmd = Command::new(&binary_path);
    cmd.env(ENV_SFTP_ENABLE, "true")
        .env(ENV_SFTP_ADDRESS, sftp_address)
        .env(ENV_SFTP_HOST_KEY_DIR, host_key_dir_str)
        .env(ENV_SFTP_READ_ONLY, "false")
        .env(ENV_SFTP_PART_SIZE, PART_SIZE_ENV)
        .env(ENV_RUSTFS_ADDRESS, s3_address)
        .env(ENV_CONSOLE_ENABLE, "false")
        .env("RUSTFS_OBS_LOGGER_LEVEL", PIPELINING_OBS_LOGGER_LEVEL)
        .env("RUST_LOG", PIPELINING_OBS_LOGGER_LEVEL);
    for (k, v) in extra_env {
        cmd.env(k, v);
    }
    let child = cmd.stdout(Stdio::piped()).arg(&env.temp_dir).spawn()?;
    Ok((env, ServerProcess::new(child)))
}

/// Seed the bucket with one multi-MB fixture object plus a
/// sub-directory containing several small siblings. The fixture
/// shape mirrors the GUI-client traversal pattern.
async fn seed_pipelining_fixture(s3: &S3Client, bucket: &str, fixture_key: &str, subdir: &str) -> Result<Vec<u8>> {
    s3.create_bucket()
        .bucket(bucket)
        .send()
        .await
        .map_err(|e| anyhow!("S3 CreateBucket {bucket} failed: {e:?}"))?;

    let payload: Vec<u8> = (0..FIXTURE_SIZE).map(|i| (i as u8).wrapping_mul(13)).collect();
    s3.put_object()
        .bucket(bucket)
        .key(fixture_key)
        .body(ByteStream::from(payload.clone()))
        .send()
        .await
        .map_err(|e| anyhow!("S3 PutObject {bucket}/{fixture_key} failed: {e:?}"))?;

    for i in 0..SUBDIR_FILE_COUNT {
        let key = format!("{subdir}/file_{i:04}.txt");
        let body = format!("sample-{i}");
        s3.put_object()
            .bucket(bucket)
            .key(&key)
            .body(ByteStream::from(body.into_bytes()))
            .send()
            .await
            .map_err(|e| anyhow!("S3 PutObject {bucket}/{key} failed: {e:?}"))?;
    }

    Ok(payload)
}

/// Seed a multi-GiB object into the rustfs S3 endpoint via multipart
/// upload. Each part is built in memory, uploaded, and dropped, so
/// peak memory stays bounded at one part_size regardless of total
/// fixture size. The byte at object offset p is
/// (p as u8).wrapping_mul(THRASH_PATTERN_MULTIPLIER) so the expected
/// SHA256 can be calculated independently without materialising the
/// fixture in memory.
async fn seed_large_via_multipart(s3: &S3Client, bucket: &str, key: &str, size_bytes: u64) -> Result<()> {
    use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
    let part_size: usize = 5 * 1024 * 1024;
    let create = s3
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|e| anyhow!("CreateMultipartUpload failed: {e:?}"))?;
    let upload_id = create
        .upload_id
        .ok_or_else(|| anyhow!("CreateMultipartUpload returned no upload_id"))?;
    let mut parts = Vec::new();
    let mut offset: u64 = 0;
    let mut part_number: i32 = 1;
    while offset < size_bytes {
        let chunk = ((part_size as u64).min(size_bytes - offset)) as usize;
        let mut body = vec![0u8; chunk];
        for (i, b) in body.iter_mut().enumerate() {
            *b = ((offset + i as u64) as u8).wrapping_mul(THRASH_PATTERN_MULTIPLIER);
        }
        let r = s3
            .upload_part()
            .bucket(bucket)
            .key(key)
            .part_number(part_number)
            .upload_id(&upload_id)
            .body(ByteStream::from(body))
            .send()
            .await
            .map_err(|e| anyhow!("UploadPart {part_number} failed: {e:?}"))?;
        let etag = r.e_tag.ok_or_else(|| anyhow!("UploadPart {part_number} returned no ETag"))?;
        parts.push(CompletedPart::builder().part_number(part_number).e_tag(etag).build());
        offset += chunk as u64;
        part_number += 1;
    }
    let completed = CompletedMultipartUpload::builder().set_parts(Some(parts)).build();
    s3.complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(completed)
        .send()
        .await
        .map_err(|e| anyhow!("CompleteMultipartUpload failed: {e:?}"))?;
    Ok(())
}

/// Calculate the SHA256 of a deterministic pattern object whose byte
/// at offset p is (p as u8).wrapping_mul(multiplier), without
/// materialising the full pattern in memory. The streaming form
/// keeps memory bounded for the multi-GiB sustained-read fixture.
fn calculate_pattern_sha256(size_bytes: u64, multiplier: u8) -> [u8; 32] {
    let mut hasher = Sha256::new();
    let chunk: usize = 1024 * 1024;
    let mut buf = vec![0u8; chunk];
    let mut written: u64 = 0;
    while written < size_bytes {
        let n = ((chunk as u64).min(size_bytes - written)) as usize;
        for (i, b) in buf[..n].iter_mut().enumerate() {
            *b = ((written + i as u64) as u8).wrapping_mul(multiplier);
        }
        hasher.update(&buf[..n]);
        written += n as u64;
    }
    hasher.finalize().into()
}

/// Streaming SHA256 download: read the SFTP file end-to-end with a
/// bounded scratch buffer so total client memory stays at one buffer
/// regardless of file size. Returns (bytes_read, sha256). The byte
/// count is the canary for a wedge: a partial-read failure appears
/// as fewer bytes than expected without raising an error from the
/// transport layer (the bytes simply stop arriving and the client's
/// timeout fires).
async fn streaming_sha256_download(sftp: &SftpSession, path: &str) -> Result<(u64, [u8; 32])> {
    let mut file = sftp
        .open_with_flags(path, OpenFlags::READ)
        .await
        .map_err(|e| anyhow!("OPEN {path} failed: {e:?}"))?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 256 * 1024];
    let mut total: u64 = 0;
    loop {
        let n = file.read(&mut buf).await.map_err(|e| anyhow!("READ {path} failed: {e:?}"))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        total += n as u64;
    }
    let _ = file.shutdown().await;
    Ok((total, hasher.finalize().into()))
}

/// Bounded in-memory ring of the rustfs server's stdout. Spawned
/// from the test entry so the test can dump the last N lines on
/// failure for diagnostic purposes.
fn capture_server_stdout(child: &mut Child) -> Arc<tokio::sync::Mutex<Vec<String>>> {
    let buffer: Arc<tokio::sync::Mutex<Vec<String>>> = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    if let Some(stdout) = child.stdout.take() {
        let buf_clone = Arc::clone(&buffer);
        tokio::spawn(async move {
            let reader = BufReader::new(stdout);
            let mut lines = reader.lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let mut buf = buf_clone.lock().await;
                buf.push(line);
                if buf.len() > 5000 {
                    buf.drain(0..1000);
                }
            }
        });
    }
    buffer
}

/// Counters scraped from the spawned server's stdout. Each "SFTP session
/// task entered" log emits an enter. Each "SFTP session task finished"
/// or "SFTP session task panicked" log emits a finish. The session-
/// lifecycle cases (CMPTST-24, CMPTST-25, CMPTST-26) read both fields
/// to assert the watchdog killed silent sessions on the expected path.
#[cfg(target_os = "linux")]
#[derive(Default)]
struct SessionCounters {
    entered: AtomicUsize,
    finished: AtomicUsize,
}

#[cfg(target_os = "linux")]
impl SessionCounters {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: AtomicUsize::new(0),
            finished: AtomicUsize::new(0),
        })
    }
}

/// Spawn a background task that reads the child stdout line-by-line and
/// increments the matching counter for every server-side session
/// lifecycle event. The task ends when stdout closes (i.e. when the
/// child is killed at teardown).
#[cfg(target_os = "linux")]
fn watch_session_lifecycle_events(child: &mut Child, counters: Arc<SessionCounters>) {
    let Some(stdout) = child.stdout.take() else {
        return;
    };
    tokio::spawn(async move {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => break,
                Ok(_) => {
                    if line.contains("SFTP session task entered") {
                        counters.entered.fetch_add(1, Ordering::Relaxed);
                    } else if line.contains("SFTP session task finished") || line.contains("SFTP session task panicked") {
                        counters.finished.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Err(_) => break,
            }
        }
    });
}

/// Count TCP connections in CLOSE_WAIT against the given local port
/// by shelling out to ss -tn state CLOSE-WAIT. The check is
/// best-effort: if ss is missing on the host the function returns
/// Ok(None) and the caller skips the assertion. The contract is zero
/// CLOSE_WAIT entries attributable to the test.
#[cfg(target_os = "linux")]
async fn count_close_wait_on_port(port: u16) -> Result<Option<usize>> {
    let output = match Command::new("ss").args(["-tn", "state", "CLOSE-WAIT"]).output().await {
        Ok(o) => o,
        Err(_) => return Ok(None),
    };
    if !output.status.success() {
        return Ok(None);
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let needle_local = format!(":{port} ");
    let needle_local_eol = format!(":{port}\n");
    let count = stdout
        .lines()
        .filter(|l| l.contains(&needle_local) || l.contains(needle_local_eol.trim_end()))
        .count();
    Ok(Some(count))
}

// CMPTST-01: medium-binary upload then download with SHA256 compare.
pub(crate) mod cmptst_01 {
    use super::*;

    const COMPLIANCE_TEST_OUTPUT_ID: &str = "CMPTST-01";

    // 300 KiB exercises a multi-buffer write through the russh-sftp
    // 256 KiB chunking boundary while staying under part_size, so the
    // upload takes the single-shot PutObject path rather than the
    // multipart path that sftp_core already covers.
    const MEDIUM_BINARY_SIZE: usize = 300 * 1024;

    pub(crate) async fn run_medium_binary_round_trip(sftp: &SftpSession) -> Result<()> {
        info!("{COMPLIANCE_TEST_OUTPUT_ID}: medium-binary round-trip with SHA256 compare");
        let bucket = "complbucket1";
        let bucket_path = format!("/{bucket}");
        sftp.create_dir(&bucket_path).await?;

        let path = format!("/{bucket}/medium.bin");
        let content: Vec<u8> = (0..MEDIUM_BINARY_SIZE).map(|i| (i as u8).wrapping_mul(13)).collect();
        let mut wf = sftp
            .open_with_flags(&path, OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::WRITE)
            .await?;
        wf.write_all(&content).await?;
        wf.flush().await?;
        wf.shutdown().await?;

        let read_back = sftp_read_full(sftp, &path).await?;
        if read_back.len() != content.len() {
            return Err(anyhow!(
                "{COMPLIANCE_TEST_OUTPUT_ID} medium-binary round-trip length mismatch: expected {}, got {}",
                content.len(),
                read_back.len()
            ));
        }
        if Sha256::digest(&content) != Sha256::digest(&read_back) {
            return Err(anyhow!("{COMPLIANCE_TEST_OUTPUT_ID} medium-binary SHA256 mismatch"));
        }

        sftp.remove_file(&path).await?;
        sftp.remove_dir(&bucket_path).await?;
        info!("PASS {COMPLIANCE_TEST_OUTPUT_ID}: medium-binary round-trip SHA256 match");
        Ok(())
    }
}

// CMPTST-02: zero-byte upload, download, and stat-size match.
pub(crate) mod cmptst_02 {
    use super::*;

    const COMPLIANCE_TEST_OUTPUT_ID: &str = "CMPTST-02";

    pub(crate) async fn run_zero_byte_round_trip(sftp: &SftpSession) -> Result<()> {
        info!("{COMPLIANCE_TEST_OUTPUT_ID}: zero-byte round-trip");
        let bucket = "complzerobucket";
        let bucket_path = format!("/{bucket}");
        sftp.create_dir(&bucket_path).await?;

        let path = format!("/{bucket}/zero.txt");
        let mut wf = sftp
            .open_with_flags(&path, OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::WRITE)
            .await?;
        wf.flush().await?;
        wf.shutdown().await?;

        let read_back = sftp_read_full(sftp, &path).await?;
        if !read_back.is_empty() {
            return Err(anyhow!("{COMPLIANCE_TEST_OUTPUT_ID} zero-byte read returned {} bytes", read_back.len()));
        }
        let meta = sftp.metadata(&path).await?;
        if meta.size != Some(0) {
            return Err(anyhow!("{COMPLIANCE_TEST_OUTPUT_ID} zero-byte stat reported size {:?}", meta.size));
        }

        sftp.remove_file(&path).await?;
        sftp.remove_dir(&bucket_path).await?;
        info!("PASS {COMPLIANCE_TEST_OUTPUT_ID}: zero-byte round-trip");
        Ok(())
    }
}

// CMPTST-03: rm against a bucket path is rejected and the bucket still exists.
pub(crate) mod cmptst_03 {
    use super::*;

    const COMPLIANCE_TEST_OUTPUT_ID: &str = "CMPTST-03";

    pub(crate) async fn run_rm_on_bucket_path_rejected(sftp: &SftpSession) -> Result<()> {
        info!("{COMPLIANCE_TEST_OUTPUT_ID}: rm on a bucket path is rejected");
        let bucket = "complrmbucket";
        let bucket_path = format!("/{bucket}");
        sftp.create_dir(&bucket_path).await?;

        let rm_result = sftp.remove_file(&bucket_path).await;
        if rm_result.is_ok() {
            return Err(anyhow!("{COMPLIANCE_TEST_OUTPUT_ID} rm on a bucket path must error"));
        }

        let root_entries: Vec<String> = sftp.read_dir("/").await?.map(|e| e.file_name()).collect();
        if !root_entries.iter().any(|n| n == bucket) {
            return Err(anyhow!("{COMPLIANCE_TEST_OUTPUT_ID} bucket must still exist after rejected rm"));
        }

        sftp.remove_dir(&bucket_path).await?;
        info!("PASS {COMPLIANCE_TEST_OUTPUT_ID}: rm on a bucket path rejected and the bucket survived");
        Ok(())
    }
}

// CMPTST-04: rmdir against a non-empty bucket is rejected and contents survive.
pub(crate) mod cmptst_04 {
    use super::*;

    const COMPLIANCE_TEST_OUTPUT_ID: &str = "CMPTST-04";

    pub(crate) async fn run_rmdir_nonempty_bucket_rejected(sftp: &SftpSession) -> Result<()> {
        info!("{COMPLIANCE_TEST_OUTPUT_ID}: rmdir on a non-empty bucket is rejected");
        let bucket = "complfullbucket";
        let bucket_path = format!("/{bucket}");
        sftp.create_dir(&bucket_path).await?;

        let inner_path = format!("/{bucket}/keep.txt");
        let inner_content = b"keep me\n";
        let mut wf = sftp
            .open_with_flags(&inner_path, OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::WRITE)
            .await?;
        wf.write_all(inner_content).await?;
        wf.flush().await?;
        wf.shutdown().await?;

        let rmdir_result = sftp.remove_dir(&bucket_path).await;
        if rmdir_result.is_ok() {
            return Err(anyhow!("{COMPLIANCE_TEST_OUTPUT_ID} rmdir on a non-empty bucket must error"));
        }

        let entries: Vec<String> = sftp.read_dir(&bucket_path).await?.map(|e| e.file_name()).collect();
        if !entries.iter().any(|n| n == "keep.txt") {
            return Err(anyhow!(
                "{COMPLIANCE_TEST_OUTPUT_ID} object inside the bucket must survive a rejected rmdir, entries were {entries:?}"
            ));
        }

        sftp.remove_file(&inner_path).await?;
        sftp.remove_dir(&bucket_path).await?;
        info!("PASS {COMPLIANCE_TEST_OUTPUT_ID}: rmdir on a non-empty bucket rejected and contents still in place");
        Ok(())
    }
}

// CMPTST-05: rmdir against a non-empty sub-directory is rejected and contents survive.
pub(crate) mod cmptst_05 {
    use super::*;

    const COMPLIANCE_TEST_OUTPUT_ID: &str = "CMPTST-05";

    pub(crate) async fn run_rmdir_nonempty_subdir_rejected(sftp: &SftpSession) -> Result<()> {
        info!("{COMPLIANCE_TEST_OUTPUT_ID}: rmdir on a non-empty sub-directory is rejected");
        let bucket = "complnedirbucket";
        let bucket_path = format!("/{bucket}");
        sftp.create_dir(&bucket_path).await?;

        let subdir_path = format!("/{bucket}/sub");
        sftp.create_dir(&subdir_path).await?;

        let inner_path = format!("/{bucket}/sub/inner.txt");
        let inner_content = b"persist\n";
        let mut wf = sftp
            .open_with_flags(&inner_path, OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::WRITE)
            .await?;
        wf.write_all(inner_content).await?;
        wf.flush().await?;
        wf.shutdown().await?;

        let rmdir_result = sftp.remove_dir(&subdir_path).await;
        if rmdir_result.is_ok() {
            return Err(anyhow!("{COMPLIANCE_TEST_OUTPUT_ID} rmdir on a non-empty sub-directory must error"));
        }

        let read_back = sftp_read_full(sftp, &inner_path).await?;
        if read_back != inner_content {
            return Err(anyhow!(
                "{COMPLIANCE_TEST_OUTPUT_ID} inner object must survive a rejected sub-directory rmdir"
            ));
        }

        sftp.remove_file(&inner_path).await?;
        sftp.remove_dir(&subdir_path).await?;
        sftp.remove_dir(&bucket_path).await?;
        info!("PASS {COMPLIANCE_TEST_OUTPUT_ID}: rmdir on a non-empty sub-directory rejected and inner object still in place");
        Ok(())
    }
}

// CMPTST-06: get with a path-traversal pattern cannot leak a host file.
pub(crate) mod cmptst_06 {
    use super::*;

    const COMPLIANCE_TEST_OUTPUT_ID: &str = "CMPTST-06";

    pub(crate) async fn run_path_traversal_get_rejected(sftp: &SftpSession) -> Result<()> {
        info!("{COMPLIANCE_TEST_OUTPUT_ID}: get with path traversal is rejected");
        let traversal = sftp.open_with_flags("/../../../etc/passwd", OpenFlags::READ).await;
        if traversal.is_ok() {
            return Err(anyhow!("{COMPLIANCE_TEST_OUTPUT_ID} path traversal open must error"));
        }
        info!("PASS {COMPLIANCE_TEST_OUTPUT_ID}: get with path traversal rejected");
        Ok(())
    }
}

// CMPTST-07: read_dir of /.. either errors or returns a listing that contains no host system entries.
pub(crate) mod cmptst_07 {
    use super::*;

    const COMPLIANCE_TEST_OUTPUT_ID: &str = "CMPTST-07";

    pub(crate) async fn run_dotdot_collapses_to_root(sftp: &SftpSession) -> Result<()> {
        info!("{COMPLIANCE_TEST_OUTPUT_ID}: read_dir of /.. does not expose host paths");
        if let Ok(entries) = sftp.read_dir("/..").await {
            let names: Vec<String> = entries.map(|e| e.file_name()).collect();
            for forbidden in ["etc", "bin", "usr", "lib", "var", "tmp", "root", "home", "proc", "sys", "dev"] {
                if names.iter().any(|n| n == forbidden) {
                    return Err(anyhow!("{COMPLIANCE_TEST_OUTPUT_ID} read_dir of /.. exposed host path {forbidden}"));
                }
            }
        }
        info!("PASS {COMPLIANCE_TEST_OUTPUT_ID}: /.. did not expose host paths");
        Ok(())
    }
}

// CMPTST-08: rename across buckets writes the payload byte-for-byte at the destination and removes the source.
pub(crate) mod cmptst_08 {
    use super::*;

    const COMPLIANCE_TEST_OUTPUT_ID: &str = "CMPTST-08";

    pub(crate) async fn run_rename_cross_bucket(sftp: &SftpSession) -> Result<()> {
        info!("{COMPLIANCE_TEST_OUTPUT_ID}: rename across buckets writes content at destination and removes source");
        let bucket_a = "complxbucketa";
        let bucket_b = "complxbucketb";
        let path_a = format!("/{bucket_a}");
        let path_b = format!("/{bucket_b}");
        sftp.create_dir(&path_a).await?;
        sftp.create_dir(&path_b).await?;

        let source = format!("/{bucket_a}/cross.txt");
        let dest = format!("/{bucket_b}/cross.txt");
        let content = b"cross-bucket payload\n";
        let mut wf = sftp
            .open_with_flags(&source, OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::WRITE)
            .await?;
        wf.write_all(content).await?;
        wf.flush().await?;
        wf.shutdown().await?;

        sftp.rename(&source, &dest).await?;

        let read_back = sftp_read_full(sftp, &dest).await?;
        if read_back != content {
            return Err(anyhow!("{COMPLIANCE_TEST_OUTPUT_ID} cross-bucket rename payload mismatch"));
        }
        let entries_a: Vec<String> = sftp.read_dir(&path_a).await?.map(|e| e.file_name()).collect();
        if entries_a.iter().any(|n| n == "cross.txt") {
            return Err(anyhow!(
                "{COMPLIANCE_TEST_OUTPUT_ID} source object must be gone after cross-bucket rename, entries were {entries_a:?}"
            ));
        }

        sftp.remove_file(&dest).await?;
        sftp.remove_dir(&path_a).await?;
        sftp.remove_dir(&path_b).await?;
        info!("PASS {COMPLIANCE_TEST_OUTPUT_ID}: cross-bucket rename wrote content at destination and removed source");
        Ok(())
    }
}

// CMPTST-09: a path with embedded spaces round-trips via russh-sftp.
pub(crate) mod cmptst_09 {
    use super::*;

    const COMPLIANCE_TEST_OUTPUT_ID: &str = "CMPTST-09";

    pub(crate) async fn run_path_with_spaces_round_trip(sftp: &SftpSession) -> Result<()> {
        info!("{COMPLIANCE_TEST_OUTPUT_ID}: path with embedded spaces round-trips");
        let bucket = "complspacebucket";
        let bucket_path = format!("/{bucket}");
        sftp.create_dir(&bucket_path).await?;

        let path = format!("/{bucket}/file with spaces.txt");
        let content = b"spaces in the key\n";
        let mut wf = sftp
            .open_with_flags(&path, OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::WRITE)
            .await?;
        wf.write_all(content).await?;
        wf.flush().await?;
        wf.shutdown().await?;

        let read_back = sftp_read_full(sftp, &path).await?;
        if read_back != content {
            return Err(anyhow!("{COMPLIANCE_TEST_OUTPUT_ID} path-with-spaces round-trip payload mismatch"));
        }

        sftp.remove_file(&path).await?;
        sftp.remove_dir(&bucket_path).await?;
        info!("PASS {COMPLIANCE_TEST_OUTPUT_ID}: path with embedded spaces round-tripped");
        Ok(())
    }
}

// CMPTST-10: read_link is rejected (S3 storage has no symlinks).
pub(crate) mod cmptst_10 {
    use super::*;

    const COMPLIANCE_TEST_OUTPUT_ID: &str = "CMPTST-10";

    pub(crate) async fn run_readlink_rejected(sftp: &SftpSession) -> Result<()> {
        info!("{COMPLIANCE_TEST_OUTPUT_ID}: read_link is rejected");
        let result = sftp.read_link("/anything").await;
        if result.is_ok() {
            return Err(anyhow!("{COMPLIANCE_TEST_OUTPUT_ID} read_link must error"));
        }
        info!("PASS {COMPLIANCE_TEST_OUTPUT_ID}: read_link rejected");
        Ok(())
    }
}

// CMPTST-11: SETSTAT on path and FSETSTAT on a separate handle both return ok.
pub(crate) mod cmptst_11 {
    use super::*;

    const COMPLIANCE_TEST_OUTPUT_ID: &str = "CMPTST-11";

    pub(crate) async fn run_setstat_after_put_returns_ok(sftp: &SftpSession) -> Result<()> {
        info!("{COMPLIANCE_TEST_OUTPUT_ID}: SETSTAT on path and FSETSTAT on a separate handle both return ok");
        let bucket = "complsetstatbucket";
        let bucket_path = format!("/{bucket}");
        sftp.create_dir(&bucket_path).await?;

        let path = format!("/{bucket}/setstat.txt");
        let content = b"SETSTAT after put\n";
        let mut wf = sftp
            .open_with_flags(&path, OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::WRITE)
            .await?;
        wf.write_all(content).await?;
        wf.flush().await?;
        wf.shutdown().await?;

        let path_attrs = FileAttributes {
            permissions: Some(0o644),
            mtime: Some(1_700_000_000),
            ..FileAttributes::default()
        };
        sftp.set_metadata(&path, path_attrs).await?;

        let mut read_handle = sftp.open_with_flags(&path, OpenFlags::READ).await?;
        let handle_attrs = FileAttributes {
            permissions: Some(0o600),
            mtime: Some(1_700_000_001),
            ..FileAttributes::default()
        };
        read_handle.set_metadata(handle_attrs).await?;
        read_handle.shutdown().await?;

        sftp.remove_file(&path).await?;
        sftp.remove_dir(&bucket_path).await?;
        info!("PASS {COMPLIANCE_TEST_OUTPUT_ID}: SETSTAT on path and FSETSTAT on a separate handle both returned ok");
        Ok(())
    }
}

// CMPTST-12: rename to the same path leaves the file in place with original payload.
pub(crate) mod cmptst_12 {
    use super::*;

    const COMPLIANCE_TEST_OUTPUT_ID: &str = "CMPTST-12";

    pub(crate) async fn run_rename_same_path_keeps_file(sftp: &SftpSession) -> Result<()> {
        info!("{COMPLIANCE_TEST_OUTPUT_ID}: rename to the same path leaves the file in place");
        let bucket = "complrenameselfbucket";
        let bucket_path = format!("/{bucket}");
        sftp.create_dir(&bucket_path).await?;

        let path = format!("/{bucket}/keep.txt");
        let content = b"do not lose me\n";
        let mut wf = sftp
            .open_with_flags(&path, OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::WRITE)
            .await?;
        wf.write_all(content).await?;
        wf.flush().await?;
        wf.shutdown().await?;

        sftp.rename(&path, &path).await?;

        let read_back = sftp_read_full(sftp, &path).await?;
        if read_back != content {
            return Err(anyhow!("{COMPLIANCE_TEST_OUTPUT_ID} same-path rename lost content"));
        }

        sftp.remove_file(&path).await?;
        sftp.remove_dir(&bucket_path).await?;
        info!("PASS {COMPLIANCE_TEST_OUTPUT_ID}: same-path rename left the file in place");
        Ok(())
    }
}

// CMPTST-13: implicit-directory round-trip from a nested-key upload.
pub(crate) mod cmptst_13 {
    use super::*;

    const COMPLIANCE_TEST_OUTPUT_ID: &str = "CMPTST-13";

    pub(crate) async fn run_implicit_dir_round_trip(sftp: &SftpSession) -> Result<()> {
        info!("{COMPLIANCE_TEST_OUTPUT_ID}: implicit-directory round-trip");
        let bucket = "compli4bucket";
        let bucket_path = format!("/{bucket}");
        sftp.create_dir(&bucket_path).await?;

        let inner_path = format!("/{bucket}/implicit/file.txt");
        let content = b"implicit subdir payload\n";
        let mut wf = sftp
            .open_with_flags(&inner_path, OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::WRITE)
            .await?;
        wf.write_all(content).await?;
        wf.flush().await?;
        wf.shutdown().await?;

        let implicit_dir = format!("/{bucket}/implicit");
        let entries_a: Vec<String> = sftp.read_dir(&implicit_dir).await?.map(|e| e.file_name()).collect();
        if !entries_a.iter().any(|n| n == "file.txt") {
            return Err(anyhow!(
                "{COMPLIANCE_TEST_OUTPUT_ID} read_dir of the implicit sub-directory must list file.txt, got {entries_a:?}"
            ));
        }

        let entries_b: Vec<String> = sftp
            .read_dir(&format!("{implicit_dir}/"))
            .await?
            .map(|e| e.file_name())
            .collect();
        if !entries_b.iter().any(|n| n == "file.txt") {
            return Err(anyhow!(
                "{COMPLIANCE_TEST_OUTPUT_ID} read_dir of the trailing-slash form must list file.txt, got {entries_b:?}"
            ));
        }

        let entries_c: Vec<String> = sftp.read_dir(&bucket_path).await?.map(|e| e.file_name()).collect();
        if !entries_c.iter().any(|n| n == "implicit") {
            return Err(anyhow!(
                "{COMPLIANCE_TEST_OUTPUT_ID} read_dir of the bucket must list the implicit sub-directory entry, got {entries_c:?}"
            ));
        }

        let read_back = sftp_read_full(sftp, &inner_path).await?;
        if read_back != content {
            return Err(anyhow!("{COMPLIANCE_TEST_OUTPUT_ID} implicit-directory file payload mismatch"));
        }

        let stat = sftp.metadata(&inner_path).await?;
        if stat.size != Some(content.len() as u64) {
            return Err(anyhow!("{COMPLIANCE_TEST_OUTPUT_ID} implicit-directory file stat size mismatch"));
        }
        if !stat.file_type().is_file() {
            return Err(anyhow!(
                "{COMPLIANCE_TEST_OUTPUT_ID} implicit-directory file stat must report a regular file"
            ));
        }

        sftp.remove_file(&inner_path).await?;
        sftp.remove_dir(&bucket_path).await?;
        info!("PASS {COMPLIANCE_TEST_OUTPUT_ID}: implicit-directory round-trip");
        Ok(())
    }
}

// CMPTST-14: WinSCP-style OPEN, WRITE, FSETSTAT, CLOSE on the same handle returns ok.
pub(crate) mod cmptst_14 {
    use super::*;

    const COMPLIANCE_TEST_OUTPUT_ID: &str = "CMPTST-14";

    pub(crate) async fn run_winscp_setstat_shape_on_handle(sftp: &SftpSession) -> Result<()> {
        info!("{COMPLIANCE_TEST_OUTPUT_ID}: OPEN + WRITE + FSETSTAT + CLOSE on the same handle returns ok");
        let bucket = "complwinscpbucket";
        let bucket_path = format!("/{bucket}");
        sftp.create_dir(&bucket_path).await?;

        let path = format!("/{bucket}/winscp.txt");
        let content = b"winscp packet sequence payload\n";
        let handle = sftp
            .open_with_flags(&path, OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::WRITE)
            .await?;

        let mut writer = handle;
        writer.write_all(content).await?;
        writer.flush().await?;

        let attrs = FileAttributes {
            permissions: Some(0o644),
            mtime: Some(1_700_000_002),
            ..FileAttributes::default()
        };
        writer.set_metadata(attrs).await?;

        writer.shutdown().await?;

        let read_back = sftp_read_full(sftp, &path).await?;
        if read_back != content {
            return Err(anyhow!("{COMPLIANCE_TEST_OUTPUT_ID} WinSCP packet-sequence payload mismatch"));
        }

        sftp.remove_file(&path).await?;
        sftp.remove_dir(&bucket_path).await?;
        info!("PASS {COMPLIANCE_TEST_OUTPUT_ID}: OPEN + WRITE + FSETSTAT + CLOSE on the same handle returned ok");
        Ok(())
    }
}

// CMPTST-15: put through SFTP is rejected in read-only mode.
pub(crate) mod cmptst_15 {
    use super::*;

    const COMPLIANCE_TEST_OUTPUT_ID: &str = "CMPTST-15";

    pub(crate) async fn run_ro_put_rejected(sftp: &SftpSession, bucket: &str) -> Result<()> {
        info!("{COMPLIANCE_TEST_OUTPUT_ID}: read-only mode rejects put");
        let path = format!("/{bucket}/blocked.txt");
        let result = sftp
            .open_with_flags(&path, OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::WRITE)
            .await;
        if result.is_ok() {
            return Err(anyhow!("{COMPLIANCE_TEST_OUTPUT_ID} read-only mode must reject open-for-write"));
        }
        info!("PASS {COMPLIANCE_TEST_OUTPUT_ID}: read-only mode rejected put");
        Ok(())
    }
}

// CMPTST-16: rm through SFTP is rejected in read-only mode.
pub(crate) mod cmptst_16 {
    use super::*;

    const COMPLIANCE_TEST_OUTPUT_ID: &str = "CMPTST-16";

    pub(crate) async fn run_ro_rm_rejected(sftp: &SftpSession, bucket: &str, seeded_key: &str) -> Result<()> {
        info!("{COMPLIANCE_TEST_OUTPUT_ID}: read-only mode rejects rm");
        let path = format!("/{bucket}/{seeded_key}");
        let result = sftp.remove_file(&path).await;
        if result.is_ok() {
            return Err(anyhow!("{COMPLIANCE_TEST_OUTPUT_ID} read-only mode must reject remove_file"));
        }
        info!("PASS {COMPLIANCE_TEST_OUTPUT_ID}: read-only mode rejected rm");
        Ok(())
    }
}

// CMPTST-17: mkdir through SFTP is rejected in read-only mode.
pub(crate) mod cmptst_17 {
    use super::*;

    const COMPLIANCE_TEST_OUTPUT_ID: &str = "CMPTST-17";

    pub(crate) async fn run_ro_mkdir_rejected(sftp: &SftpSession) -> Result<()> {
        info!("{COMPLIANCE_TEST_OUTPUT_ID}: read-only mode rejects mkdir");
        let result = sftp.create_dir("/ronewbucket").await;
        if result.is_ok() {
            return Err(anyhow!("{COMPLIANCE_TEST_OUTPUT_ID} read-only mode must reject mkdir"));
        }
        info!("PASS {COMPLIANCE_TEST_OUTPUT_ID}: read-only mode rejected mkdir");
        Ok(())
    }
}

// CMPTST-18: rmdir through SFTP is rejected in read-only mode.
pub(crate) mod cmptst_18 {
    use super::*;

    const COMPLIANCE_TEST_OUTPUT_ID: &str = "CMPTST-18";

    pub(crate) async fn run_ro_rmdir_rejected(sftp: &SftpSession, bucket: &str) -> Result<()> {
        info!("{COMPLIANCE_TEST_OUTPUT_ID}: read-only mode rejects rmdir");
        let path = format!("/{bucket}");
        let result = sftp.remove_dir(&path).await;
        if result.is_ok() {
            return Err(anyhow!("{COMPLIANCE_TEST_OUTPUT_ID} read-only mode must reject remove_dir"));
        }
        info!("PASS {COMPLIANCE_TEST_OUTPUT_ID}: read-only mode rejected rmdir");
        Ok(())
    }
}

// CMPTST-19: rename through SFTP is rejected in read-only mode.
pub(crate) mod cmptst_19 {
    use super::*;

    const COMPLIANCE_TEST_OUTPUT_ID: &str = "CMPTST-19";

    pub(crate) async fn run_ro_rename_rejected(sftp: &SftpSession, bucket: &str, seeded_key: &str) -> Result<()> {
        info!("{COMPLIANCE_TEST_OUTPUT_ID}: read-only mode rejects rename");
        let from = format!("/{bucket}/{seeded_key}");
        let to = format!("/{bucket}/moved.txt");
        let result = sftp.rename(&from, &to).await;
        if result.is_ok() {
            return Err(anyhow!("{COMPLIANCE_TEST_OUTPUT_ID} read-only mode must reject rename"));
        }
        info!("PASS {COMPLIANCE_TEST_OUTPUT_ID}: read-only mode rejected rename");
        Ok(())
    }
}

// CMPTST-20: ls through SFTP is allowed in read-only mode and lists the seeded bucket.
pub(crate) mod cmptst_20 {
    use super::*;

    const COMPLIANCE_TEST_OUTPUT_ID: &str = "CMPTST-20";

    pub(crate) async fn run_ro_ls_allowed(sftp: &SftpSession, bucket: &str) -> Result<()> {
        info!("{COMPLIANCE_TEST_OUTPUT_ID}: read-only mode allows ls");
        let entries: Vec<String> = sftp.read_dir("/").await?.map(|e| e.file_name()).collect();
        if !entries.iter().any(|n| n == bucket) {
            return Err(anyhow!(
                "{COMPLIANCE_TEST_OUTPUT_ID} read-only mode must list buckets, expected {bucket}, got {entries:?}"
            ));
        }
        info!("PASS {COMPLIANCE_TEST_OUTPUT_ID}: read-only mode allowed ls");
        Ok(())
    }
}

// CMPTST-21: get through SFTP is allowed in read-only mode and returns the seeded payload.
pub(crate) mod cmptst_21 {
    use super::*;

    const COMPLIANCE_TEST_OUTPUT_ID: &str = "CMPTST-21";

    pub(crate) async fn run_ro_get_allowed(sftp: &SftpSession, bucket: &str, seeded_key: &str, expected: &[u8]) -> Result<()> {
        info!("{COMPLIANCE_TEST_OUTPUT_ID}: read-only mode allows get");
        let path = format!("/{bucket}/{seeded_key}");
        let read_back = sftp_read_full(sftp, &path).await?;
        if read_back != expected {
            return Err(anyhow!(
                "{COMPLIANCE_TEST_OUTPUT_ID} read-only mode get returned {} bytes, expected {}",
                read_back.len(),
                expected.len()
            ));
        }
        info!("PASS {COMPLIANCE_TEST_OUTPUT_ID}: read-only mode allowed get and returned the seeded payload");
        Ok(())
    }
}

// CMPTST-22: SETSTAT on a path is rejected with PermissionDenied in read-only mode.
pub(crate) mod cmptst_22 {
    use super::*;

    const COMPLIANCE_TEST_OUTPUT_ID: &str = "CMPTST-22";

    pub(crate) async fn run_ro_setstat_rejected(sftp: &SftpSession, bucket: &str, seeded_key: &str) -> Result<()> {
        info!("{COMPLIANCE_TEST_OUTPUT_ID}: read-only mode rejects SETSTAT on a path");
        let path = format!("/{bucket}/{seeded_key}");
        let attrs = FileAttributes {
            permissions: Some(0o600),
            mtime: Some(1_700_000_000),
            ..FileAttributes::default()
        };
        let result = sftp.set_metadata(&path, attrs).await;
        if result.is_ok() {
            return Err(anyhow!("{COMPLIANCE_TEST_OUTPUT_ID} read-only mode must reject SETSTAT"));
        }
        info!("PASS {COMPLIANCE_TEST_OUTPUT_ID}: read-only mode rejected SETSTAT on a path");
        Ok(())
    }
}

// CMPTST-23: FSETSTAT on a read handle is rejected with PermissionDenied in read-only mode.
pub(crate) mod cmptst_23 {
    use super::*;

    const COMPLIANCE_TEST_OUTPUT_ID: &str = "CMPTST-23";

    pub(crate) async fn run_ro_fsetstat_rejected(sftp: &SftpSession, bucket: &str, seeded_key: &str) -> Result<()> {
        info!("{COMPLIANCE_TEST_OUTPUT_ID}: read-only mode rejects FSETSTAT on an open handle");
        let path = format!("/{bucket}/{seeded_key}");
        let mut handle = sftp.open_with_flags(&path, OpenFlags::READ).await?;
        let attrs = FileAttributes {
            permissions: Some(0o600),
            mtime: Some(1_700_000_001),
            ..FileAttributes::default()
        };
        let result = handle.set_metadata(attrs).await;
        handle.shutdown().await?;
        if result.is_ok() {
            return Err(anyhow!("{COMPLIANCE_TEST_OUTPUT_ID} read-only mode must reject FSETSTAT"));
        }
        info!("PASS {COMPLIANCE_TEST_OUTPUT_ID}: read-only mode rejected FSETSTAT on an open handle");
        Ok(())
    }
}

// CMPTST-24: concurrent half-close burst does not leak server-side session tasks.
#[cfg(target_os = "linux")]
pub(crate) mod cmptst_24 {
    use super::*;

    const COMPLIANCE_TEST_OUTPUT_ID: &str = "CMPTST-24";

    // Half-close zombie regression ports. Pair held distinct from the
    // other SFTP test entries so the half-close traffic stays off the
    // shared listener and so the assertion-time CLOSE_WAIT scan only
    // counts connections this entry opened.
    const HALF_CLOSE_SFTP_PORT: u16 = 9026;
    const HALF_CLOSE_SFTP_ADDRESS: &str = "127.0.0.1:9026";
    const HALF_CLOSE_S3_ADDRESS: &str = "127.0.0.1:9302";
    const HALF_CLOSE_S3_ENDPOINT: &str = "http://127.0.0.1:9302";
    const HALF_CLOSE_S3_READY_ATTEMPTS: u32 = 30;
    // Per-session deadline the spawned server uses. Short enough that
    // the post-fix kill path completes well inside the 30 s wait.
    const HALF_CLOSE_IDLE_TIMEOUT_SECS: u64 = 8;
    // Window the test waits after triggering the N half-close peers.
    // Long enough that the post-fix server-side deadline has fired and
    // the session task has finished.
    const HALF_CLOSE_WAIT_SECS: u64 = 30;
    // Concurrent half-close client count. FileZilla 3.66.5 was observed
    // at 17 parallel sessions in the real-world capture. Eight is
    // enough to reproduce the leak under the same shape and keeps test
    // runtime bounded.
    const HALF_CLOSE_PARALLEL_SESSIONS: usize = 8;
    // Fixture file size. Larger than one MAX_READ_LEN chunk so the
    // test session can complete one full READ before triggering the
    // half-close.
    const HALF_CLOSE_FIXTURE_BYTES: usize = 1024 * 1024;
    // One MAX_READ_LEN chunk. The case contract requires at least one
    // READ packet to complete before the half-close trigger.
    const HALF_CLOSE_FIRST_READ_BYTES: usize = 256 * 1024;

    // Shared flags between the test loop and the per-session
    // HalfClosableStream instance handed to the russh client. The
    // wrapper polls these flags from inside the russh I/O task to flip
    // the underlying TCP socket into the half-closed-write state and
    // to suspend further reads.
    struct HalfCloseControl {
        half_close_writes: AtomicBool,
        block_reads: AtomicBool,
    }

    impl HalfCloseControl {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                half_close_writes: AtomicBool::new(false),
                block_reads: AtomicBool::new(false),
            })
        }
    }

    /// Wrapper around a tokio::net::TcpStream split into owned halves
    /// so the test can request a one-sided shutdown (FIN on the write
    /// side, no further reads acknowledged) while the russh client
    /// remains the I/O owner. The control flags are toggled by the
    /// test loop after the first SFTP READ packet completes.
    ///
    /// The wrapper deliberately returns Poll::Pending after the FIN is
    /// on the wire instead of an io::Error: the russh client task must
    /// remain suspended on the wrapper rather than tearing the SSH
    /// session down, which would full-close the socket and reset the
    /// OS state the test is asserting against.
    struct HalfClosableStream {
        read: OwnedReadHalf,
        write: OwnedWriteHalf,
        control: Arc<HalfCloseControl>,
        write_shutdown_done: bool,
    }

    impl HalfClosableStream {
        fn from_tcp(stream: TcpStream, control: Arc<HalfCloseControl>) -> Self {
            let (read, write) = stream.into_split();
            Self {
                read,
                write,
                control,
                write_shutdown_done: false,
            }
        }
    }

    impl AsyncRead for HalfClosableStream {
        fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            let this = self.get_mut();
            if this.control.block_reads.load(Ordering::Relaxed) {
                return Poll::Pending;
            }
            Pin::new(&mut this.read).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for HalfClosableStream {
        fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
            let this = self.get_mut();
            if this.control.half_close_writes.load(Ordering::Relaxed) {
                if !this.write_shutdown_done {
                    match Pin::new(&mut this.write).poll_shutdown(cx) {
                        Poll::Ready(Ok(())) => {
                            this.write_shutdown_done = true;
                        }
                        Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                        Poll::Pending => return Poll::Pending,
                    }
                }
                return Poll::Pending;
            }
            Pin::new(&mut this.write).poll_write(cx, buf)
        }

        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            let this = self.get_mut();
            if this.control.half_close_writes.load(Ordering::Relaxed) && this.write_shutdown_done {
                return Poll::Pending;
            }
            Pin::new(&mut this.write).poll_flush(cx)
        }

        fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            let this = self.get_mut();
            Pin::new(&mut this.write).poll_shutdown(cx)
        }
    }

    /// Drive a single half-close session against the running server:
    /// open the TCP, hand the socket halves to a HalfClosableStream,
    /// run the SSH+SFTP handshake through russh, read one MAX_READ_LEN
    /// chunk, then flip the control flags and issue a follow-up SFTP
    /// request so the russh client task touches poll_write once and
    /// the wrapper has the chance to drive the TCP shutdown(SHUT_WR)
    /// syscall before the request is suspended.
    ///
    /// Returns the still-live russh client Handle and the SftpSession
    /// alongside the control handle. The caller holds them in a Vec to
    /// prevent the russh client task from being dropped, which would
    /// otherwise full-close the socket and mask the leak the test is
    /// probing for.
    async fn drive_half_close_session(
        address: &str,
        bucket: &str,
        seeded_key: &str,
    ) -> Result<(client::Handle<AcceptAnyServerKey>, SftpSession, Arc<HalfCloseControl>)> {
        let tcp = TcpStream::connect(address)
            .await
            .map_err(|e| anyhow!("TCP connect to {address} failed: {e}"))?;
        let control = HalfCloseControl::new();
        let stream = HalfClosableStream::from_tcp(tcp, Arc::clone(&control));
        let config = Arc::new(client::Config::default());
        let mut session = client::connect_stream(config, stream, AcceptAnyServerKey)
            .await
            .map_err(|e| anyhow!("russh connect_stream failed: {e}"))?;

        let auth = session
            .authenticate_password(DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY)
            .await
            .map_err(|e| anyhow!("russh password auth failed: {e}"))?;
        if !auth.success() {
            return Err(anyhow!("SFTP password auth rejected on half-close session"));
        }
        let channel = session
            .channel_open_session()
            .await
            .map_err(|e| anyhow!("channel open failed: {e}"))?;
        channel
            .request_subsystem(true, "sftp")
            .await
            .map_err(|e| anyhow!("subsystem request failed: {e}"))?;
        let sftp = SftpSession::new(channel.into_stream())
            .await
            .map_err(|e| anyhow!("SftpSession init failed: {e}"))?;

        let path = format!("/{bucket}/{seeded_key}");
        let mut file = sftp
            .open_with_flags(&path, OpenFlags::READ)
            .await
            .map_err(|e| anyhow!("SFTP open failed: {e}"))?;
        let mut buf = vec![0u8; HALF_CLOSE_FIRST_READ_BYTES];
        let mut read_total = 0usize;
        while read_total < HALF_CLOSE_FIRST_READ_BYTES {
            let n = file
                .read(&mut buf[read_total..])
                .await
                .map_err(|e| anyhow!("SFTP read failed: {e}"))?;
            if n == 0 {
                break;
            }
            read_total += n;
        }
        if read_total < HALF_CLOSE_FIRST_READ_BYTES {
            return Err(anyhow!(
                "first SFTP READ packet returned only {read_total} bytes, expected at least {HALF_CLOSE_FIRST_READ_BYTES}"
            ));
        }

        // Flip the half-close trigger before the next SFTP request goes
        // out and stop draining the receive side. The next inflight
        // request forces the russh client task to call poll_write on
        // the wrapper, which drives the underlying TCP shutdown(SHUT_WR)
        // and then suspends. After the FIN has been sent the russh task
        // remains parked rather than tearing the SSH session down, so
        // the OS-level socket stays in the half-closed state the leak
        // depends on.
        control.half_close_writes.store(true, Ordering::Relaxed);
        control.block_reads.store(true, Ordering::Relaxed);
        let _ = tokio::time::timeout(Duration::from_millis(750), file.metadata()).await;
        drop(file);

        Ok((session, sftp, control))
    }

    // Test orchestration:
    //
    // 1. Spawn rustfs with a short idle timeout (HALF_CLOSE_IDLE_TIMEOUT_SECS).
    // 2. Open HALF_CLOSE_PARALLEL_SESSIONS SFTP sessions over a custom
    //    HalfClosableStream that issues shutdown(SHUT_WR) on the read
    //    half mid-transfer and parks subsequent reads (returns Pending).
    // 3. Wait HALF_CLOSE_WAIT_SECS for the server-side idle timer to
    //    fire and the accept loop to drain finished session tasks.
    // 4. Issue dummy TCP connects to wake the accept loop's select so
    //    the JoinSet flushes finished tasks before the assertion runs.
    // 5. Assert the entered/finished session counters balance and that
    //    no CLOSE_WAIT sockets remain on the bind port (Linux ss(8)
    //    only; the assertion skips with a warn if ss is unavailable).
    pub(crate) async fn run_concurrent_half_close_no_leak() -> Result<()> {
        let env = ProtocolTestEnvironment::new().map_err(|e| anyhow!("{}", e))?;
        let host_key_dir = PathBuf::from(&env.temp_dir).join("sftp_host_keys");
        generate_host_key(&host_key_dir).await?;

        info!(
            "{COMPLIANCE_TEST_OUTPUT_ID}: starting half-close server on {} (idle_timeout={}s)",
            HALF_CLOSE_SFTP_ADDRESS, HALF_CLOSE_IDLE_TIMEOUT_SECS
        );
        let binary_path = rustfs_binary_path_with_features(Some("ftps,webdav,sftp"));
        let host_key_dir_str = host_key_dir
            .to_str()
            .ok_or_else(|| anyhow!("host key dir path is not utf-8: {}", host_key_dir.display()))?;
        let mut server_process = ServerProcess::new(
            Command::new(&binary_path)
                .env(ENV_SFTP_ENABLE, "true")
                .env(ENV_SFTP_ADDRESS, HALF_CLOSE_SFTP_ADDRESS)
                .env(ENV_SFTP_HOST_KEY_DIR, host_key_dir_str)
                .env(ENV_SFTP_READ_ONLY, "false")
                .env(ENV_SFTP_PART_SIZE, PART_SIZE_ENV)
                .env(ENV_SFTP_IDLE_TIMEOUT, HALF_CLOSE_IDLE_TIMEOUT_SECS.to_string())
                .env(ENV_RUSTFS_ADDRESS, HALF_CLOSE_S3_ADDRESS)
                // Disable the admin console listener to avoid port
                // contention with local dev-testing containers.
                .env(ENV_CONSOLE_ENABLE, "false")
                .env("RUSTFS_OBS_LOGGER_LEVEL", "rustfs_protocols=debug")
                .env("RUST_LOG", "rustfs_protocols=debug")
                .stdout(Stdio::piped())
                .arg(&env.temp_dir)
                .spawn()?,
        );
        let counters = SessionCounters::new();
        watch_session_lifecycle_events(server_process.child_mut(), Arc::clone(&counters));

        let result = async {
            ProtocolTestEnvironment::wait_for_port_ready(HALF_CLOSE_SFTP_PORT, 30)
                .await
                .map_err(|e| anyhow!("{}", e))?;

            let s3 = build_test_s3_client(HALF_CLOSE_S3_ENDPOINT);
            wait_for_s3_ready(&s3, HALF_CLOSE_S3_READY_ATTEMPTS).await?;

            let bucket = "halfclose";
            let seeded_key = "fixture.bin";
            s3.create_bucket()
                .bucket(bucket)
                .send()
                .await
                .map_err(|e| anyhow!("S3 CreateBucket {bucket} failed: {e:?}"))?;
            let payload: Vec<u8> = (0..HALF_CLOSE_FIXTURE_BYTES).map(|i| (i as u8).wrapping_mul(7)).collect();
            s3.put_object()
                .bucket(bucket)
                .key(seeded_key)
                .body(ByteStream::from(payload))
                .send()
                .await
                .map_err(|e| anyhow!("S3 PutObject {bucket}/{seeded_key} failed: {e:?}"))?;

            let mut futs = Vec::with_capacity(HALF_CLOSE_PARALLEL_SESSIONS);
            for i in 0..HALF_CLOSE_PARALLEL_SESSIONS {
                let address = HALF_CLOSE_SFTP_ADDRESS.to_string();
                let bucket = bucket.to_string();
                let key = seeded_key.to_string();
                futs.push(tokio::spawn(async move {
                    drive_half_close_session(&address, &bucket, &key)
                        .await
                        .map_err(|e| anyhow!("session {i} setup failed: {e}"))
                }));
            }
            // Hold each (Handle, SftpSession, Control) tuple for the
            // full wait window so the OwnedRead/OwnedWriteHalf inside
            // the wrapper stay alive and the OS keeps each socket in
            // its half-closed state. Dropping any of them would trigger
            // a full-close on the socket, which would mask the leak by
            // waking the server's session task through a real EOF or
            // RST.
            let mut keepalive: Vec<(client::Handle<AcceptAnyServerKey>, SftpSession, Arc<HalfCloseControl>)> = Vec::new();
            for fut in futs {
                keepalive.push(fut.await??);
            }

            let entered_after_setup = counters.entered.load(Ordering::Relaxed);
            let finished_after_setup = counters.finished.load(Ordering::Relaxed);
            info!(
                "{COMPLIANCE_TEST_OUTPUT_ID}: {} half-close sessions established (server entered={}, finished={}). Waiting {} s for the watchdog to kill them",
                HALF_CLOSE_PARALLEL_SESSIONS, entered_after_setup, finished_after_setup, HALF_CLOSE_WAIT_SECS,
            );

            sleep(Duration::from_secs(HALF_CLOSE_WAIT_SECS)).await;

            // The accept loop drains finished session tasks at the top
            // of every iteration, which only runs when a new TCP accept
            // (or a shutdown signal) wakes the select. Issue a single
            // TCP connection so the loop iterates once and the JoinSet
            // drain emits the "SFTP session task finished" log for
            // every session that the per-session deadline has already
            // cancelled. Without this, the counters under-report on a
            // quiet server.
            for _ in 0..3 {
                if let Ok(stream) = TcpStream::connect(HALF_CLOSE_SFTP_ADDRESS).await {
                    drop(stream);
                }
                sleep(Duration::from_millis(200)).await;
            }
            sleep(Duration::from_millis(500)).await;

            let entered = counters.entered.load(Ordering::Relaxed);
            let finished = counters.finished.load(Ordering::Relaxed);
            let outstanding = entered.saturating_sub(finished);
            info!(
                "{COMPLIANCE_TEST_OUTPUT_ID}: post-wait counters entered={} finished={} outstanding={}",
                entered, finished, outstanding
            );
            if outstanding > 1 {
                return Err(anyhow!(
                    "{COMPLIANCE_TEST_OUTPUT_ID} session-task balance contract failed: entered={entered} finished={finished} outstanding={outstanding}, expected at most 1"
                ));
            }

            match count_close_wait_on_port(HALF_CLOSE_SFTP_PORT).await? {
                Some(0) => info!("{COMPLIANCE_TEST_OUTPUT_ID}: zero CLOSE_WAIT entries against port {HALF_CLOSE_SFTP_PORT}"),
                Some(n) => {
                    return Err(anyhow!(
                        "{COMPLIANCE_TEST_OUTPUT_ID} {n} CLOSE_WAIT entries against port {HALF_CLOSE_SFTP_PORT}, expected 0"
                    ));
                }
                None => info!("{COMPLIANCE_TEST_OUTPUT_ID}: ss(8) unavailable, skipping CLOSE_WAIT assertion"),
            }

            // Drop the keepalive vector now so the test process does
            // not leave the half-closed sockets dangling past the
            // assertion.
            drop(keepalive);
            info!("PASS {COMPLIANCE_TEST_OUTPUT_ID}: half-close burst did not leak server-side session tasks");
            Ok::<(), anyhow::Error>(())
        }
        .await;

        server_process.kill_and_wait().await;

        result
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn regression() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        crate::common::init_logging();
        run_concurrent_half_close_no_leak()
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })
    }
}

// CMPTST-25: wedge-kill watchdog kills sessions parked behind a CLOSE_WAIT socket.
#[cfg(target_os = "linux")]
pub(crate) mod cmptst_25 {
    use super::*;

    const COMPLIANCE_TEST_OUTPUT_ID: &str = "CMPTST-25";

    // Wedge-kill regression ports. Distinct from the half-close ports
    // so the wedge-driving traffic stays off the half-close listener
    // and so the post-wait CLOSE_WAIT scan only counts sockets this
    // entry opened.
    const WEDGE_SFTP_PORT: u16 = 9027;
    const WEDGE_SFTP_ADDRESS: &str = "127.0.0.1:9027";
    const WEDGE_S3_ADDRESS: &str = "127.0.0.1:9303";
    const WEDGE_S3_ENDPOINT: &str = "http://127.0.0.1:9303";
    const WEDGE_S3_READY_ATTEMPTS: u32 = 30;

    // Idle timeout the spawned server uses. Set well above the wait
    // window so russh's own inactivity_timeout cannot kill any session
    // during the test. The contract: only the watchdog kills the
    // wedged session inside the wait window. Without the watchdog the
    // session leaks because the russh select! is parked outside its
    // own arms.
    const WEDGE_IDLE_TIMEOUT_SECS: u64 = 300;

    // Total wait window. Must exceed
    // WEDGE_FAST_KILL_SILENCE_SECS (30) + WEDGE_WATCHDOG_TICK_SECS (15)
    // = 45 s of worst-case watchdog detection latency, plus a 15 s
    // grace.
    // 90 s instead of 60 s gives a 30 s margin above the watchdog
    // worst-case cancel latency (FAST_KILL_SILENCE 30 s plus two
    // 15 s ticks = 60 s) so scheduler jitter on a busy CI host does
    // not flip the assertion.
    const WEDGE_WAIT_SECS: u64 = 90;

    // Concurrent wedged sessions. Mirrors the half-close case so
    // server-side bookkeeping counters move in the same magnitude
    // regardless of which case runs.
    const WEDGE_PARALLEL_SESSIONS: usize = 8;

    // Fixture file size. Large enough that 8 pipelined READ requests
    // of 256 KiB each fit inside it without overrunning end-of-file.
    const WEDGE_FIXTURE_BYTES: usize = 4 * 1024 * 1024;

    // Fixture chunk size requested by the test's pipelined READs.
    // Matches MAX_READ_LEN so the server's response is one full chunk
    // per request.
    const WEDGE_CHUNK_BYTES: u32 = 256 * 1024;

    // Pipelined READs sent in the window-exhaustion phase. Eight times
    // 256 KiB equals 2 MiB, which equals russh's default window_size,
    // so the server's stream.write_all parks at the SSH window the
    // moment the eighth response is queued.
    const WEDGE_WINDOW_FILL_READS: usize = 8;

    // Pipelined READs sent in the mpsc-fill phase, after the SSH
    // window has been exhausted. Above the russh server-side
    // channel_buffer_size = 100 default so the per-channel mpsc fills
    // and the session loop's chan.send().await parks. 200 picks a
    // comfortable margin without blowing up the test wire footprint
    // (200 times ~30 B per FXP_READ packet ~ 6 KiB).
    const WEDGE_MPSC_FILL_READS: usize = 200;

    // SFTPv3 packet type codes used by the raw-protocol path the wedge
    // driver follows. The driver hand-builds FXP_INIT, FXP_OPEN, and
    // FXP_READ packets via channel.data() rather than going through
    // russh-sftp's high-level File API because SftpSession serialises
    // reads (one outstanding request at a time) and the wedge requires
    // pipelining many READs without waiting for responses.
    const SSH_FXP_INIT: u8 = 1;
    const SSH_FXP_OPEN: u8 = 3;
    const SSH_FXP_READ: u8 = 5;
    // SSH_FXP_OPEN flags. READ-only access against the seeded fixture.
    const SSH_FXF_READ: u32 = 0x0000_0001;

    // Shared flags between the test loop and the per-session
    // WedgeStream. The wrapper polls these flags from inside the russh
    // I/O task to suspend wire reads (so the per-channel mpsc on the
    // server fills) and to land FIN on the wire (so the kernel reports
    // the socket in CLOSE_WAIT after the SFTP driver also stops
    // draining on its own).
    struct WedgeControl {
        block_reads: AtomicBool,
        half_close_writes: AtomicBool,
    }

    impl WedgeControl {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                block_reads: AtomicBool::new(false),
                half_close_writes: AtomicBool::new(false),
            })
        }
    }

    /// Wrapper around tokio::net::TcpStream for the wedge regression.
    /// Same shape as the half-close wrapper but its purpose is to keep
    /// the russh client task wedged once block_reads is set so the test
    /// can pile in further FXP_READ requests via the still-live write
    /// half. Once half_close_writes is set the wrapper drives
    /// shutdown(SHUT_WR) on the next poll_write, sending FIN to the
    /// server.
    struct WedgeStream {
        read: OwnedReadHalf,
        write: OwnedWriteHalf,
        control: Arc<WedgeControl>,
        write_shutdown_done: bool,
    }

    impl WedgeStream {
        fn from_tcp(stream: TcpStream, control: Arc<WedgeControl>) -> Self {
            let (read, write) = stream.into_split();
            Self {
                read,
                write,
                control,
                write_shutdown_done: false,
            }
        }
    }

    impl AsyncRead for WedgeStream {
        fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            let this = self.get_mut();
            if this.control.block_reads.load(Ordering::Relaxed) {
                return Poll::Pending;
            }
            Pin::new(&mut this.read).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for WedgeStream {
        fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
            let this = self.get_mut();
            if this.control.half_close_writes.load(Ordering::Relaxed) {
                if !this.write_shutdown_done {
                    match Pin::new(&mut this.write).poll_shutdown(cx) {
                        Poll::Ready(Ok(())) => {
                            this.write_shutdown_done = true;
                        }
                        Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                        Poll::Pending => return Poll::Pending,
                    }
                }
                return Poll::Pending;
            }
            Pin::new(&mut this.write).poll_write(cx, buf)
        }

        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            let this = self.get_mut();
            if this.control.half_close_writes.load(Ordering::Relaxed) && this.write_shutdown_done {
                return Poll::Pending;
            }
            Pin::new(&mut this.write).poll_flush(cx)
        }

        fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            let this = self.get_mut();
            Pin::new(&mut this.write).poll_shutdown(cx)
        }
    }

    /// Drive a single wedge session against the running server.
    ///
    /// Returns the still-live russh client Handle, channel, and control
    /// flags. The caller holds them in a Vec for the entire wait window
    /// so the underlying TCP socket stays in CLOSE_WAIT and the
    /// per-channel mpsc on the server stays full.
    async fn drive_wedge_session(
        address: &str,
        bucket: &str,
        seeded_key: &str,
    ) -> Result<(client::Handle<AcceptAnyServerKey>, russh::Channel<russh::client::Msg>, Arc<WedgeControl>)> {
        let tcp = TcpStream::connect(address)
            .await
            .map_err(|e| anyhow!("TCP connect to {address} failed: {e}"))?;
        let control = WedgeControl::new();
        let stream = WedgeStream::from_tcp(tcp, Arc::clone(&control));
        let config = Arc::new(client::Config::default());
        let mut session = client::connect_stream(config, stream, AcceptAnyServerKey)
            .await
            .map_err(|e| anyhow!("russh connect_stream failed: {e}"))?;

        let auth = session
            .authenticate_password(DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY)
            .await
            .map_err(|e| anyhow!("russh password auth failed: {e}"))?;
        if !auth.success() {
            return Err(anyhow!("SFTP password auth rejected on wedge session"));
        }
        let mut channel = session
            .channel_open_session()
            .await
            .map_err(|e| anyhow!("channel open failed: {e}"))?;
        channel
            .request_subsystem(true, "sftp")
            .await
            .map_err(|e| anyhow!("subsystem request failed: {e}"))?;

        // FXP_INIT (version 3). One u32 payload (the version).
        let init_pkt = build_sftp_init();
        channel
            .data(&init_pkt[..])
            .await
            .map_err(|e| anyhow!("FXP_INIT send failed: {e:?}"))?;
        // Drain the FXP_VERSION response so the channel is in steady
        // state before the wedge flags are toggled. The russh client
        // receive loop delivers it via channel.wait().
        let _ = wait_for_data(&mut channel).await?;

        // FXP_OPEN against the seeded fixture. Returns FXP_HANDLE with
        // the server-assigned handle string fed to every following READ.
        let path = format!("/{bucket}/{seeded_key}");
        let open_pkt = build_sftp_open(1, &path, SSH_FXF_READ);
        channel
            .data(&open_pkt[..])
            .await
            .map_err(|e| anyhow!("FXP_OPEN send failed: {e:?}"))?;
        let handle = parse_handle(&wait_for_data(&mut channel).await?)?;

        // One FXP_READ to confirm the path works end-to-end before the
        // wedge phase. Drain the response so subsequent reads do not
        // see stale FXP_DATA on the wire.
        let probe_read = build_sftp_read(2, &handle, 0, WEDGE_CHUNK_BYTES);
        channel
            .data(&probe_read[..])
            .await
            .map_err(|e| anyhow!("probe FXP_READ send failed: {e:?}"))?;
        let _ = wait_for_data(&mut channel).await?;

        // Wedge phase one: stop draining the wire on the client side,
        // then pipeline N FXP_READ packets that fill the SSH window.
        // The server queues FXP_DATA responses for each. The responses
        // leave the server's stream.write_all only as long as the SSH
        // receive window has slack. After WEDGE_WINDOW_FILL_READS
        // responses the server's next stream.write_all parks because
        // the client is no longer sending CHANNEL_WINDOW_ADJUST.
        control.block_reads.store(true, Ordering::Relaxed);
        let mut req_id = 3u32;
        for i in 0..WEDGE_WINDOW_FILL_READS {
            let offset = (i as u64) * WEDGE_CHUNK_BYTES as u64;
            let pkt = build_sftp_read(req_id, &handle, offset, WEDGE_CHUNK_BYTES);
            channel
                .data(&pkt[..])
                .await
                .map_err(|e| anyhow!("FXP_READ window-fill send failed at i={i}: {e:?}"))?;
            req_id = req_id.wrapping_add(1);
        }

        // Wedge phase two: pile in further FXP_READ packets while the
        // SFTP driver is parked on stream.write_all. Each arriving
        // CHANNEL_DATA pushes one entry into the server's per-channel
        // mpsc (default capacity 100). Once that mpsc fills, the
        // server's session loop's chan.send().await blocks. The
        // select! is then unreachable from the keepalive and
        // inactivity arms. This is the wedge.
        for i in 0..WEDGE_MPSC_FILL_READS {
            let offset = ((i % WEDGE_WINDOW_FILL_READS) as u64) * WEDGE_CHUNK_BYTES as u64;
            let pkt = build_sftp_read(req_id, &handle, offset, WEDGE_CHUNK_BYTES);
            // Best-effort: once the wire backs up the channel's send
            // buffer fills and channel.data().await yields. Bound the
            // wait so a stalled client side does not block the test.
            match tokio::time::timeout(Duration::from_millis(250), channel.data(&pkt[..])).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(anyhow!("FXP_READ mpsc-fill send failed at i={i}: {e:?}")),
                Err(_) => break,
            }
            req_id = req_id.wrapping_add(1);
        }

        // Phase three: trigger the FIN. Setting half_close_writes flips
        // the wrapper into shutdown(SHUT_WR) on the next poll_write.
        // One last FXP_READ drives that poll_write. After this point
        // the wrapper returns Pending forever on writes, so the russh
        // client task remains parked instead of tearing the SSH session
        // down.
        control.half_close_writes.store(true, Ordering::Relaxed);
        let trigger = build_sftp_read(req_id, &handle, 0, WEDGE_CHUNK_BYTES);
        let _ = tokio::time::timeout(Duration::from_millis(750), channel.data(&trigger[..])).await;

        Ok((session, channel, control))
    }

    /// Read one full SFTPv3 packet from the channel. The packet wire
    /// format is length(4) || type(1) || payload, so this accumulates
    /// inbound CHANNEL_DATA frames until the four-byte length prefix
    /// has been satisfied. Returns the full packet bytes including the
    /// length prefix.
    async fn wait_for_data(channel: &mut russh::Channel<russh::client::Msg>) -> Result<Vec<u8>> {
        use russh::ChannelMsg;
        let timeout_per_packet = Duration::from_secs(5);
        let mut buf: Vec<u8> = Vec::new();
        loop {
            if buf.len() >= 4 {
                let declared = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
                if buf.len() >= 4 + declared {
                    return Ok(buf);
                }
            }
            let msg = tokio::time::timeout(timeout_per_packet, channel.wait())
                .await
                .map_err(|_| anyhow!("timed out waiting for SFTP response (have {} bytes)", buf.len()))?
                .ok_or_else(|| anyhow!("channel closed before SFTP packet complete (have {} bytes)", buf.len()))?;
            match msg {
                ChannelMsg::Data { data } => buf.extend_from_slice(&data),
                ChannelMsg::Eof | ChannelMsg::Close => {
                    return Err(anyhow!("channel ended before SFTP packet complete (have {} bytes)", buf.len()));
                }
                _ => {}
            }
        }
    }

    fn build_sftp_init() -> Vec<u8> {
        // Length(4) || Type(1) || Version(4). Packet length excludes
        // the length field itself.
        let mut payload = Vec::with_capacity(9);
        payload.extend_from_slice(&5u32.to_be_bytes());
        payload.push(SSH_FXP_INIT);
        payload.extend_from_slice(&3u32.to_be_bytes());
        payload
    }

    fn build_sftp_open(req_id: u32, path: &str, flags: u32) -> Vec<u8> {
        // Length(4) || Type(1) || ReqId(4) || PathLen(4) || Path ||
        // Flags(4) || AttrFlags(4). SFTPv3 OPEN ends with a
        // FileAttributes block. An empty attrs (flags=0) is one u32.
        let mut body = Vec::new();
        body.push(SSH_FXP_OPEN);
        body.extend_from_slice(&req_id.to_be_bytes());
        body.extend_from_slice(&(path.len() as u32).to_be_bytes());
        body.extend_from_slice(path.as_bytes());
        body.extend_from_slice(&flags.to_be_bytes());
        body.extend_from_slice(&0u32.to_be_bytes()); // empty FileAttributes
        let mut pkt = Vec::with_capacity(4 + body.len());
        pkt.extend_from_slice(&(body.len() as u32).to_be_bytes());
        pkt.extend_from_slice(&body);
        pkt
    }

    fn build_sftp_read(req_id: u32, handle: &[u8], offset: u64, len: u32) -> Vec<u8> {
        // Length(4) || Type(1) || ReqId(4) || HandleLen(4) || Handle ||
        // Offset(8) || Len(4).
        let mut body = Vec::with_capacity(1 + 4 + 4 + handle.len() + 8 + 4);
        body.push(SSH_FXP_READ);
        body.extend_from_slice(&req_id.to_be_bytes());
        body.extend_from_slice(&(handle.len() as u32).to_be_bytes());
        body.extend_from_slice(handle);
        body.extend_from_slice(&offset.to_be_bytes());
        body.extend_from_slice(&len.to_be_bytes());
        let mut pkt = Vec::with_capacity(4 + body.len());
        pkt.extend_from_slice(&(body.len() as u32).to_be_bytes());
        pkt.extend_from_slice(&body);
        pkt
    }

    fn parse_handle(packet: &[u8]) -> Result<Vec<u8>> {
        // Wire layout: Length(4) || Type(1) || ReqId(4) || HandleLen(4)
        // || Handle. For FXP_HANDLE the type byte is 102.
        if packet.len() < 4 + 1 + 4 + 4 {
            return Err(anyhow!("SFTP open response too short: {} bytes", packet.len()));
        }
        let kind = packet[4];
        if kind != 102 {
            return Err(anyhow!("expected FXP_HANDLE (102), got type {kind} from FXP_OPEN reply"));
        }
        let handle_len = u32::from_be_bytes([packet[9], packet[10], packet[11], packet[12]]) as usize;
        if packet.len() < 13 + handle_len {
            return Err(anyhow!(
                "FXP_HANDLE truncated: declared {handle_len} bytes, packet has {} after header",
                packet.len().saturating_sub(13)
            ));
        }
        Ok(packet[13..13 + handle_len].to_vec())
    }

    // Test orchestration:
    //
    // 1. Spawn rustfs with a long idle_timeout (300 s) so the test
    //    isolates the watchdog kill path from the inactivity timer.
    // 2. Open WEDGE_PARALLEL_SESSIONS SFTP sessions over a custom
    //    WedgeStream that allows writes but parks reads after a flag
    //    flips. Hand-build raw FXP_INIT, FXP_OPEN, FXP_READ packets to
    //    fill the SSH per-channel window plus the per-channel mpsc on
    //    the server (WEDGE_WINDOW_FILL_READS + WEDGE_MPSC_FILL_READS),
    //    so the server's send loop parks on the mpsc.
    // 3. Issue shutdown(SHUT_WR) on the client side to drive the
    //    socket into CLOSE_WAIT.
    // 4. Wait WEDGE_WAIT_SECS for the watchdog (FAST_KILL_SILENCE 30 s
    //    plus two 15 s ticks worst-case = 60 s) to detect CLOSE_WAIT
    //    via /proc/net/tcp and cancel the parked session.
    // 5. Assert the session task counters balance and CLOSE_WAIT count
    //    is zero (ss(8) only; skips with a warn when ss is missing).
    pub(crate) async fn run_wedge_kill_after_silence_in_close_wait() -> Result<()> {
        let env = ProtocolTestEnvironment::new().map_err(|e| anyhow!("{}", e))?;
        let host_key_dir = PathBuf::from(&env.temp_dir).join("sftp_host_keys");
        generate_host_key(&host_key_dir).await?;

        info!(
            "{COMPLIANCE_TEST_OUTPUT_ID}: starting wedge server on {} (idle_timeout={}s; only the watchdog should kill)",
            WEDGE_SFTP_ADDRESS, WEDGE_IDLE_TIMEOUT_SECS
        );
        let binary_path = rustfs_binary_path_with_features(Some("ftps,webdav,sftp"));
        let host_key_dir_str = host_key_dir
            .to_str()
            .ok_or_else(|| anyhow!("host key dir path is not utf-8: {}", host_key_dir.display()))?;
        let mut server_process = ServerProcess::new(
            Command::new(&binary_path)
                .env(ENV_SFTP_ENABLE, "true")
                .env(ENV_SFTP_ADDRESS, WEDGE_SFTP_ADDRESS)
                .env(ENV_SFTP_HOST_KEY_DIR, host_key_dir_str)
                .env(ENV_SFTP_READ_ONLY, "false")
                .env(ENV_SFTP_PART_SIZE, PART_SIZE_ENV)
                .env(ENV_SFTP_IDLE_TIMEOUT, WEDGE_IDLE_TIMEOUT_SECS.to_string())
                .env(ENV_RUSTFS_ADDRESS, WEDGE_S3_ADDRESS)
                .env(ENV_CONSOLE_ENABLE, "false")
                .env("RUSTFS_OBS_LOGGER_LEVEL", "rustfs_protocols=debug")
                .env("RUST_LOG", "rustfs_protocols=debug")
                .stdout(Stdio::piped())
                .arg(&env.temp_dir)
                .spawn()?,
        );
        let counters = SessionCounters::new();
        watch_session_lifecycle_events(server_process.child_mut(), Arc::clone(&counters));

        let result = async {
            ProtocolTestEnvironment::wait_for_port_ready(WEDGE_SFTP_PORT, 30)
                .await
                .map_err(|e| anyhow!("{}", e))?;

            let s3 = build_test_s3_client(WEDGE_S3_ENDPOINT);
            wait_for_s3_ready(&s3, WEDGE_S3_READY_ATTEMPTS).await?;

            let bucket = "wedge";
            let seeded_key = "fixture.bin";
            s3.create_bucket()
                .bucket(bucket)
                .send()
                .await
                .map_err(|e| anyhow!("S3 CreateBucket {bucket} failed: {e:?}"))?;
            let payload: Vec<u8> = (0..WEDGE_FIXTURE_BYTES).map(|i| (i as u8).wrapping_mul(11)).collect();
            s3.put_object()
                .bucket(bucket)
                .key(seeded_key)
                .body(ByteStream::from(payload))
                .send()
                .await
                .map_err(|e| anyhow!("S3 PutObject {bucket}/{seeded_key} failed: {e:?}"))?;

            let mut futs = Vec::with_capacity(WEDGE_PARALLEL_SESSIONS);
            for i in 0..WEDGE_PARALLEL_SESSIONS {
                let address = WEDGE_SFTP_ADDRESS.to_string();
                let bucket = bucket.to_string();
                let key = seeded_key.to_string();
                futs.push(tokio::spawn(async move {
                    drive_wedge_session(&address, &bucket, &key)
                        .await
                        .map_err(|e| anyhow!("wedge session {i} setup failed: {e}"))
                }));
            }
            let mut keepalive: Vec<(client::Handle<AcceptAnyServerKey>, russh::Channel<russh::client::Msg>, Arc<WedgeControl>)> =
                Vec::new();
            for fut in futs {
                keepalive.push(fut.await??);
            }

            let entered_after_setup = counters.entered.load(Ordering::Relaxed);
            let finished_after_setup = counters.finished.load(Ordering::Relaxed);
            info!(
                "{COMPLIANCE_TEST_OUTPUT_ID}: {} wedge sessions established (server entered={}, finished={}); waiting {} s for the watchdog kill path",
                WEDGE_PARALLEL_SESSIONS, entered_after_setup, finished_after_setup, WEDGE_WAIT_SECS,
            );

            sleep(Duration::from_secs(WEDGE_WAIT_SECS)).await;

            // Tickle the accept loop so JoinSet::try_join_next emits
            // the "SFTP session task finished" log lines for any
            // session the watchdog has cancelled. Same mechanism the
            // half-close case uses. Mirrors the accept-loop drain
            // pattern in server.rs.
            for _ in 0..3 {
                if let Ok(stream) = TcpStream::connect(WEDGE_SFTP_ADDRESS).await {
                    drop(stream);
                }
                sleep(Duration::from_millis(200)).await;
            }
            sleep(Duration::from_millis(500)).await;

            let entered = counters.entered.load(Ordering::Relaxed);
            let finished = counters.finished.load(Ordering::Relaxed);
            let outstanding = entered.saturating_sub(finished);
            info!(
                "{COMPLIANCE_TEST_OUTPUT_ID}: post-wait counters entered={} finished={} outstanding={}",
                entered, finished, outstanding
            );
            if outstanding > 1 {
                return Err(anyhow!(
                    "{COMPLIANCE_TEST_OUTPUT_ID} session-task balance contract failed: entered={entered} finished={finished} outstanding={outstanding}, expected at most 1"
                ));
            }

            match count_close_wait_on_port(WEDGE_SFTP_PORT).await? {
                Some(0) => info!("{COMPLIANCE_TEST_OUTPUT_ID}: zero CLOSE_WAIT entries against port {WEDGE_SFTP_PORT}"),
                Some(n) => {
                    return Err(anyhow!(
                        "{COMPLIANCE_TEST_OUTPUT_ID} {n} CLOSE_WAIT entries against port {WEDGE_SFTP_PORT}, expected 0"
                    ));
                }
                None => info!("{COMPLIANCE_TEST_OUTPUT_ID}: ss(8) unavailable, skipping CLOSE_WAIT assertion"),
            }

            drop(keepalive);
            info!("PASS {COMPLIANCE_TEST_OUTPUT_ID}: wedged sessions killed by the watchdog");
            Ok::<(), anyhow::Error>(())
        }
        .await;

        server_process.kill_and_wait().await;

        result
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn regression() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        crate::common::init_logging();
        run_wedge_kill_after_silence_in_close_wait()
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })
    }
}

// CMPTST-26: healthy idle session past the watchdog fast-kill threshold stays alive.
#[cfg(target_os = "linux")]
pub(crate) mod cmptst_26 {
    use super::*;

    const COMPLIANCE_TEST_OUTPUT_ID: &str = "CMPTST-26";

    const IDLE_SFTP_PORT: u16 = 9028;
    const IDLE_SFTP_ADDRESS: &str = "127.0.0.1:9028";
    const IDLE_S3_ADDRESS: &str = "127.0.0.1:9304";
    const IDLE_S3_ENDPOINT: &str = "http://127.0.0.1:9304";
    const IDLE_S3_READY_ATTEMPTS: u32 = 30;

    // Idle timeout for the spawned server. 300 s sits well above the
    // wait window so russh's own inactivity_timeout cannot kill during
    // the test. The contract: a healthy idle session past the
    // watchdog's fast-kill threshold MUST stay alive because the procfs
    // probe sees ESTABLISHED and the decision function returns
    // Decision::Quiet.
    const IDLE_TIMEOUT_SECS: u64 = 300;

    // Wait window. Must exceed
    // WEDGE_FAST_KILL_SILENCE_SECS (30) + WEDGE_WATCHDOG_TICK_SECS (15)
    // = 45 s of worst-case watchdog detection latency, plus a 15 s
    // grace. Sits well below WEDGE_FALLBACK_KILL_SILENCE_SECS (1800)
    // so the fallback path does not fire either.
    // 90 s instead of 60 s. The case asserts the watchdog does NOT
    // false-kill, so a longer wait strengthens the assertion: if the
    // procfs ESTABLISHED discriminator is broken, more wait windows
    // give it more chances to fire.
    const IDLE_WAIT_SECS: u64 = 90;

    pub(crate) async fn run_healthy_idle_session_above_fast_threshold() -> Result<()> {
        let env = ProtocolTestEnvironment::new().map_err(|e| anyhow!("{}", e))?;
        let host_key_dir = PathBuf::from(&env.temp_dir).join("sftp_host_keys");
        generate_host_key(&host_key_dir).await?;

        info!(
            "{COMPLIANCE_TEST_OUTPUT_ID}: starting idle-session server on {} (idle_timeout={}s)",
            IDLE_SFTP_ADDRESS, IDLE_TIMEOUT_SECS
        );
        let binary_path = rustfs_binary_path_with_features(Some("ftps,webdav,sftp"));
        let host_key_dir_str = host_key_dir
            .to_str()
            .ok_or_else(|| anyhow!("host key dir path is not utf-8: {}", host_key_dir.display()))?;
        let mut server_process = ServerProcess::new(
            Command::new(&binary_path)
                .env(ENV_SFTP_ENABLE, "true")
                .env(ENV_SFTP_ADDRESS, IDLE_SFTP_ADDRESS)
                .env(ENV_SFTP_HOST_KEY_DIR, host_key_dir_str)
                .env(ENV_SFTP_READ_ONLY, "false")
                .env(ENV_SFTP_PART_SIZE, PART_SIZE_ENV)
                .env(ENV_SFTP_IDLE_TIMEOUT, IDLE_TIMEOUT_SECS.to_string())
                .env(ENV_RUSTFS_ADDRESS, IDLE_S3_ADDRESS)
                .env(ENV_CONSOLE_ENABLE, "false")
                .env("RUSTFS_OBS_LOGGER_LEVEL", "rustfs_protocols=debug")
                .env("RUST_LOG", "rustfs_protocols=debug")
                .stdout(Stdio::piped())
                .arg(&env.temp_dir)
                .spawn()?,
        );
        let counters = SessionCounters::new();
        watch_session_lifecycle_events(server_process.child_mut(), Arc::clone(&counters));

        let result = async {
            ProtocolTestEnvironment::wait_for_port_ready(IDLE_SFTP_PORT, 30)
                .await
                .map_err(|e| anyhow!("{}", e))?;

            let s3 = build_test_s3_client(IDLE_S3_ENDPOINT);
            wait_for_s3_ready(&s3, IDLE_S3_READY_ATTEMPTS).await?;

            // Open one healthy SFTP session and drive a single
            // operation to stamp SessionDiag.last_activity_ms. The
            // watchdog measures silence from this moment.
            let (handle, sftp) = connect_sftp_to(IDLE_SFTP_ADDRESS).await?;
            let _ = sftp.canonicalize("/").await?;

            let entered_after_setup = counters.entered.load(Ordering::Relaxed);
            let finished_after_setup = counters.finished.load(Ordering::Relaxed);
            info!(
                "{COMPLIANCE_TEST_OUTPUT_ID}: idle session established (server entered={}, finished={}). Waiting {} s past the watchdog fast-kill threshold",
                entered_after_setup, finished_after_setup, IDLE_WAIT_SECS,
            );

            sleep(Duration::from_secs(IDLE_WAIT_SECS)).await;

            // Verify the session is still alive by driving another
            // operation. If the watchdog had killed the session during
            // the sleep, this canonicalize call would fail with a
            // closed-channel error.
            let final_realpath = sftp
                .canonicalize("/")
                .await
                .map_err(|e| anyhow!("post-wait canonicalize failed (likely watchdog false-kill): {e:?}"))?;
            if final_realpath != "/" {
                return Err(anyhow!(
                    "{COMPLIANCE_TEST_OUTPUT_ID} SFTP canonicalize returned unexpected result: {final_realpath:?}"
                ));
            }

            let entered_after_wait = counters.entered.load(Ordering::Relaxed);
            let finished_after_wait = counters.finished.load(Ordering::Relaxed);
            info!(
                "{COMPLIANCE_TEST_OUTPUT_ID}: post-wait counters entered={} finished={}",
                entered_after_wait, finished_after_wait,
            );

            // The contract: no session task ended during the wait
            // window. entered_after_wait may have grown if any ambient
            // probe traffic hit the listener. finished_after_wait must
            // equal finished_after_setup because no session ended.
            if finished_after_wait != finished_after_setup {
                return Err(anyhow!(
                    "{COMPLIANCE_TEST_OUTPUT_ID} watchdog false-killed a healthy idle session: finished went from {} to {} during the {} s wait",
                    finished_after_setup,
                    finished_after_wait,
                    IDLE_WAIT_SECS,
                ));
            }

            // Clean disconnect. The shutdown bumps finished by 1 after
            // this point but that is the expected end-of-test path,
            // not a watchdog kill.
            drop(sftp);
            let _ = handle.disconnect(russh::Disconnect::ByApplication, "test complete", "").await;

            info!("PASS {COMPLIANCE_TEST_OUTPUT_ID}: healthy idle session NOT killed by watchdog after {IDLE_WAIT_SECS} s");
            Ok::<(), anyhow::Error>(())
        }
        .await;

        server_process.kill_and_wait().await;

        result
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn regression() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        crate::common::init_logging();
        run_healthy_idle_session_above_fast_threshold()
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })
    }
}

// CMPTST-27: sustained-read thrash, multi-GiB downloads on N parallel sessions byte-identical to seed.
pub(crate) mod cmptst_27 {
    use super::*;

    const COMPLIANCE_TEST_OUTPUT_ID: &str = "CMPTST-27";

    const PIPE27_SFTP_PORT: u16 = 9035;
    const PIPE27_SFTP_ADDRESS: &str = "127.0.0.1:9035";
    const PIPE27_S3_ADDRESS: &str = "127.0.0.1:9311";
    const PIPE27_S3_ENDPOINT: &str = "http://127.0.0.1:9311";

    // Sustained-read thrash parameters. N parallel SFTP sessions each
    // download a multi-GiB object end-to-end and verify byte-exact
    // SHA256. The fixture is large enough to keep the SSH per-channel
    // window under sustained pressure. The per-session streaming
    // SHA256 keeps client-side memory bounded so the workload is not
    // memory-limited.
    //
    // Load-bearing assertions: byte-count and SHA256 match against the
    // seeded pattern. Both are independent of throughput, and both
    // fire under any silent corruption or short read.
    //
    // THRASH_DEADLINE_SECS is a no-progress safety floor only.
    // Aggregate throughput across N parallel sessions is bounded by
    // the SSH SFTP subsystem layer's per-channel serial handler
    // dispatch and the shared backend. The figure that comes back
    // varies by hardware. The deadline is set far above any realistic
    // completion time so it only trips when sessions stop progressing
    // entirely (a wedge), not when sessions are merely slow.
    const THRASH_PARALLEL: usize = 4;
    const THRASH_FIXTURE_DEFAULT_GIB: u64 = 5;
    const THRASH_DEADLINE_SECS: u64 = 3600;

    /// Returns the fixture size in bytes. Default 5 GiB. Override via
    /// RUSTFS_TEST_THRASH_FIXTURE_GIB so a memory-constrained CI runner
    /// can run the thrash case at 1 or 2 GiB without OOM-killing the
    /// linker or exhausting a tmpfs /tmp. The minimum that still keeps
    /// the SSH per-channel window under sustained pressure is around
    /// 512 MiB, but the env var accepts any positive integer GiB.
    fn thrash_fixture_bytes() -> u64 {
        let gib: u64 = std::env::var("RUSTFS_TEST_THRASH_FIXTURE_GIB")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|g| *g > 0)
            .unwrap_or(THRASH_FIXTURE_DEFAULT_GIB);
        gib * 1024 * 1024 * 1024
    }

    pub(crate) async fn run_multi_session_mixed_pipelining() -> Result<()> {
        info!("{COMPLIANCE_TEST_OUTPUT_ID}: starting sustained-read thrash server on {PIPE27_SFTP_ADDRESS}");
        let (_env, mut server_process) = spawn_pipelining_rustfs(PIPE27_SFTP_ADDRESS, PIPE27_S3_ADDRESS).await?;
        let server_log = capture_server_stdout(server_process.child_mut());

        let result = async {
            ProtocolTestEnvironment::wait_for_port_ready(PIPE27_SFTP_PORT, 30)
                .await
                .map_err(|e| anyhow!("{}", e))?;

            let s3 = build_test_s3_client(PIPE27_S3_ENDPOINT);
            wait_for_s3_ready(&s3, S3_READY_ATTEMPTS).await?;

            let bucket = "thrash";
            let key = "fixture.bin";
            s3.create_bucket()
                .bucket(bucket)
                .send()
                .await
                .map_err(|e| anyhow!("S3 CreateBucket {bucket} failed: {e:?}"))?;

            let fixture_bytes = thrash_fixture_bytes();
            let gib = fixture_bytes / (1024 * 1024 * 1024);
            info!("{COMPLIANCE_TEST_OUTPUT_ID}: seeding {gib} GiB via multipart upload");
            let seed_t0 = Instant::now();
            seed_large_via_multipart(&s3, bucket, key, fixture_bytes).await?;
            info!("{COMPLIANCE_TEST_OUTPUT_ID}: seed complete in {:?}", seed_t0.elapsed());
            let expected_sha = calculate_pattern_sha256(fixture_bytes, THRASH_PATTERN_MULTIPLIER);

            let path = format!("/{bucket}/{key}");
            let mut handles = Vec::with_capacity(THRASH_PARALLEL);
            for session_idx in 0..THRASH_PARALLEL {
                let address = PIPE27_SFTP_ADDRESS.to_string();
                let path = path.clone();
                handles.push(tokio::spawn(async move {
                    let t0 = Instant::now();
                    let (_handle, sftp) = connect_sftp_to(&address).await?;
                    let (bytes, sha) = streaming_sha256_download(&sftp, &path).await?;
                    Ok::<(usize, u64, [u8; 32], Duration), anyhow::Error>((session_idx, bytes, sha, t0.elapsed()))
                }));
            }

            let overall = Duration::from_secs(THRASH_DEADLINE_SECS);
            let drained = timeout(overall, async {
                let mut results = Vec::with_capacity(THRASH_PARALLEL);
                for h in handles {
                    results.push(h.await.map_err(|e| anyhow!("worker join failed: {e}"))??);
                }
                Ok::<Vec<(usize, u64, [u8; 32], Duration)>, anyhow::Error>(results)
            })
            .await;
            let results = match drained {
                Ok(Ok(r)) => r,
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    return Err(anyhow!(
                        "{COMPLIANCE_TEST_OUTPUT_ID} deadline exceeded: {THRASH_PARALLEL} sessions did not finish within {THRASH_DEADLINE_SECS} s"
                    ));
                }
            };

            for (idx, bytes, sha, elapsed) in &results {
                if *bytes != fixture_bytes {
                    return Err(anyhow!(
                        "{COMPLIANCE_TEST_OUTPUT_ID} session {idx} truncated: read {bytes} bytes, expected {fixture_bytes} (elapsed {elapsed:?})",
                    ));
                }
                if sha != &expected_sha {
                    return Err(anyhow!(
                        "{COMPLIANCE_TEST_OUTPUT_ID} session {idx} SHA256 mismatch (elapsed {elapsed:?})"
                    ));
                }
            }
            let slowest = results.iter().map(|r| r.3).max().unwrap_or_default();
            info!(
                "PASS {COMPLIANCE_TEST_OUTPUT_ID}: {THRASH_PARALLEL} parallel {gib} GiB downloads byte-identical (slowest {slowest:?})",
            );
            Ok::<(), anyhow::Error>(())
        }
        .await;

        if result.is_err() {
            let buf = server_log.lock().await;
            let lines: Vec<&String> = buf.iter().rev().take(200).collect();
            eprintln!("--- last {} lines of rustfs server stdout (oldest first) ---", lines.len());
            for line in lines.iter().rev() {
                eprintln!("{line}");
            }
            eprintln!("--- end rustfs stdout dump ---");
        }

        server_process.kill_and_wait().await;
        result
    }

    #[tokio::test]
    async fn regression() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        crate::common::init_logging();
        run_multi_session_mixed_pipelining()
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })
    }
}

// CMPTST-28: 5 MB download intact under concurrent metadata storm on a parallel session.
pub(crate) mod cmptst_28 {
    use super::*;

    const COMPLIANCE_TEST_OUTPUT_ID: &str = "CMPTST-28";

    const PIPE28_SFTP_PORT: u16 = 9029;
    const PIPE28_SFTP_ADDRESS: &str = "127.0.0.1:9029";
    const PIPE28_S3_ADDRESS: &str = "127.0.0.1:9305";
    const PIPE28_S3_ENDPOINT: &str = "http://127.0.0.1:9305";

    // Parameters. METADATA_STORM_OPS bounds the in-flight metadata
    // depth fired against the storm session. STORM_PARALLEL_SESSIONS
    // opens that many independent SFTP channels each running its own
    // storm. The download session runs alongside and must complete
    // within the per-session deadline.
    const METADATA_STORM_OPS: usize = 500;
    const STORM_PARALLEL_SESSIONS: usize = 4;
    const METADATA_STORM_DEADLINE_SECS: u64 = 20;

    pub(crate) async fn run_5mb_download_with_concurrent_metadata_ops() -> Result<()> {
        info!("{COMPLIANCE_TEST_OUTPUT_ID}: starting metadata-pressure server on {PIPE28_SFTP_ADDRESS}");
        let (_env, mut server_process) = spawn_pipelining_rustfs(PIPE28_SFTP_ADDRESS, PIPE28_S3_ADDRESS).await?;

        let result = async {
            ProtocolTestEnvironment::wait_for_port_ready(PIPE28_SFTP_PORT, 30)
                .await
                .map_err(|e| anyhow!("{}", e))?;

            let s3 = build_test_s3_client(PIPE28_S3_ENDPOINT);
            wait_for_s3_ready(&s3, S3_READY_ATTEMPTS).await?;

            let bucket = "pipe28";
            let fixture_key = "fixture.bin";
            let subdir = "siblings";
            let payload = seed_pipelining_fixture(&s3, bucket, fixture_key, subdir).await?;
            let expected_sha: [u8; 32] = Sha256::digest(&payload).into();

            let fixture_path = format!("/{bucket}/{fixture_key}");
            let subdir_path = format!("/{bucket}/{subdir}");

            let stop_flag = Arc::new(AtomicBool::new(false));
            let mut storm_handles = Vec::with_capacity(STORM_PARALLEL_SESSIONS);
            for storm_idx in 0..STORM_PARALLEL_SESSIONS {
                let storm_address = PIPE28_SFTP_ADDRESS.to_string();
                let storm_subdir = subdir_path.clone();
                let storm_flag = Arc::clone(&stop_flag);
                storm_handles.push(tokio::spawn(async move {
                    let (_handle, sftp) = connect_sftp_to(&storm_address).await?;
                    let sftp = Arc::new(sftp);
                    let mut pipeline: FuturesUnordered<_> = (0..METADATA_STORM_OPS)
                        .map(|i| {
                            let sftp = Arc::clone(&sftp);
                            let subdir = storm_subdir.clone();
                            let flag = Arc::clone(&storm_flag);
                            async move {
                                if flag.load(Ordering::Relaxed) {
                                    return Ok::<(), anyhow::Error>(());
                                }
                                if i % 2 == 0 {
                                    sftp.read_dir(&subdir)
                                        .await
                                        .map_err(|e| anyhow!("storm {storm_idx} READDIR failed: {e:?}"))?;
                                } else {
                                    let path = format!("{subdir}/file_{:04}.txt", i % SUBDIR_FILE_COUNT);
                                    sftp.metadata(&path)
                                        .await
                                        .map_err(|e| anyhow!("storm {storm_idx} STAT {path} failed: {e:?}"))?;
                                }
                                Ok::<(), anyhow::Error>(())
                            }
                        })
                        .collect();
                    while let Some(r) = pipeline.next().await {
                        r?;
                    }
                    Ok::<(), anyhow::Error>(())
                }));
            }

            let download_address = PIPE28_SFTP_ADDRESS.to_string();
            let download_path = fixture_path.clone();
            let download = tokio::spawn(async move {
                let (_handle, sftp) = connect_sftp_to(&download_address).await?;
                let bytes = sftp_read_full(&sftp, &download_path)
                    .await
                    .map_err(|e| anyhow!("download READ failed: {e:?}"))?;
                if bytes.len() != FIXTURE_SIZE {
                    return Err(anyhow!(
                        "download byte count mismatch: expected {FIXTURE_SIZE}, got {}",
                        bytes.len()
                    ));
                }
                let observed: [u8; 32] = Sha256::digest(&bytes).into();
                if observed != expected_sha {
                    return Err(anyhow!("download SHA256 mismatch on {download_path}"));
                }
                Ok(())
            });

            let overall = Duration::from_secs(METADATA_STORM_DEADLINE_SECS);
            let download_outcome = timeout(overall, download).await;
            stop_flag.store(true, Ordering::Relaxed);
            for handle in storm_handles {
                let _ = handle.await;
            }

            match download_outcome {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(e))) => return Err(e),
                Ok(Err(e)) => return Err(anyhow!("download join failed: {e}")),
                Err(_elapsed) => {
                    return Err(anyhow!(
                        "{COMPLIANCE_TEST_OUTPUT_ID} deadline exceeded: download did not finish within {METADATA_STORM_DEADLINE_SECS} s under metadata pressure (parallel storm sessions = {STORM_PARALLEL_SESSIONS}, in-flight depth per storm = {METADATA_STORM_OPS})"
                    ));
                }
            }
            info!("PASS {COMPLIANCE_TEST_OUTPUT_ID}: 5 MB download finished intact under concurrent metadata pressure");
            Ok::<(), anyhow::Error>(())
        }
        .await;

        server_process.kill_and_wait().await;
        result
    }

    #[tokio::test]
    async fn regression() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        crate::common::init_logging();
        run_5mb_download_with_concurrent_metadata_ops()
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })
    }
}

// CMPTST-29: high-volume read-past-EOF pipelining completes inside the deadline and every read returns EOF.
pub(crate) mod cmptst_29 {
    use super::*;

    const COMPLIANCE_TEST_OUTPUT_ID: &str = "CMPTST-29";

    const PIPE29_SFTP_PORT: u16 = 9030;
    const PIPE29_SFTP_ADDRESS: &str = "127.0.0.1:9030";
    const PIPE29_S3_ADDRESS: &str = "127.0.0.1:9306";
    const PIPE29_S3_ENDPOINT: &str = "http://127.0.0.1:9306";

    // Parameters. EOF_VOLUME_REQUEST_COUNT total reads, fanned out
    // across EOF_VOLUME_INFLIGHT_DEPTH file handles on a single
    // SftpSession. Each handle drives reads serially within itself,
    // but reads across handles run concurrently because the russh-sftp
    // client pipelines per-call response routing through a request-id
    // table. EOF_VOLUME_INFLIGHT_DEPTH is held below the server's
    // default handles-per-session cap (DEFAULT_HANDLES_PER_SESSION = 64
    // in crates/protocols/src/sftp/constants.rs) so the test never
    // trips the cap-exceeded surface, which has its own dedicated
    // coverage.
    const EOF_VOLUME_FIXTURE_BYTES: usize = 1024;
    const EOF_VOLUME_REQUEST_COUNT: usize = 10_000;
    const EOF_VOLUME_INFLIGHT_DEPTH: usize = 50;
    const EOF_VOLUME_DEADLINE_SECS: u64 = 30;

    pub(crate) async fn run_read_past_eof_volume() -> Result<()> {
        info!("{COMPLIANCE_TEST_OUTPUT_ID}: starting EOF-volume server on {PIPE29_SFTP_ADDRESS}");
        let (_env, mut server_process) = spawn_pipelining_rustfs(PIPE29_SFTP_ADDRESS, PIPE29_S3_ADDRESS).await?;

        let result = async {
            ProtocolTestEnvironment::wait_for_port_ready(PIPE29_SFTP_PORT, 30)
                .await
                .map_err(|e| anyhow!("{}", e))?;

            let s3 = build_test_s3_client(PIPE29_S3_ENDPOINT);
            wait_for_s3_ready(&s3, S3_READY_ATTEMPTS).await?;

            let bucket = "pipe29";
            let key = "tiny.bin";
            s3.create_bucket()
                .bucket(bucket)
                .send()
                .await
                .map_err(|e| anyhow!("S3 CreateBucket {bucket} failed: {e:?}"))?;
            let payload: Vec<u8> = (0..EOF_VOLUME_FIXTURE_BYTES).map(|i| i as u8).collect();
            s3.put_object()
                .bucket(bucket)
                .key(key)
                .body(ByteStream::from(payload.clone()))
                .send()
                .await
                .map_err(|e| anyhow!("S3 PutObject {bucket}/{key} failed: {e:?}"))?;

            let path = format!("/{bucket}/{key}");
            let (_handle, sftp) = connect_sftp_to(PIPE29_SFTP_ADDRESS).await?;
            let sftp = Arc::new(sftp);
            // Open a fan of independent file handles. russh-sftp's File
            // requires &mut self for read, so concurrent reads need
            // separate handles. Reads stack as in-flight FXP packets
            // on the same channel because the underlying SftpSession
            // pipelines through its request-id table.
            let mut handle_setup: FuturesUnordered<_> = (0..EOF_VOLUME_INFLIGHT_DEPTH)
                .map(|_| {
                    let sftp = Arc::clone(&sftp);
                    let path = path.clone();
                    async move {
                        let mut file = sftp
                            .open_with_flags(&path, OpenFlags::READ)
                            .await
                            .map_err(|e| anyhow!("OPEN {path} failed: {e:?}"))?;
                        file.seek(std::io::SeekFrom::Start((EOF_VOLUME_FIXTURE_BYTES as u64) + 1024))
                            .await
                            .map_err(|e| anyhow!("SEEK past EOF failed: {e:?}"))?;
                        Ok::<_, anyhow::Error>(file)
                    }
                })
                .collect();
            let mut files = Vec::with_capacity(EOF_VOLUME_INFLIGHT_DEPTH);
            while let Some(r) = handle_setup.next().await {
                files.push(r?);
            }

            let overall = Duration::from_secs(EOF_VOLUME_DEADLINE_SECS);
            let reads_per_handle = EOF_VOLUME_REQUEST_COUNT / EOF_VOLUME_INFLIGHT_DEPTH;
            let drained = timeout(overall, async {
                let mut pipeline: FuturesUnordered<_> = files
                    .into_iter()
                    .map(|mut file| async move {
                        let mut scratch = [0u8; 64];
                        for i in 0..reads_per_handle {
                            match file.read(&mut scratch).await {
                                Ok(0) => {}
                                Ok(n) => {
                                    return Err(anyhow!(
                                        "{COMPLIANCE_TEST_OUTPUT_ID} read {i} returned {n} bytes past EOF; expected 0"
                                    ));
                                }
                                Err(e) => {
                                    return Err(anyhow!("{COMPLIANCE_TEST_OUTPUT_ID} read {i} returned an error: {e}"));
                                }
                            }
                        }
                        let _ = file.shutdown().await;
                        Ok::<(), anyhow::Error>(())
                    })
                    .collect();
                while let Some(r) = pipeline.next().await {
                    r?;
                }
                Ok(())
            })
            .await;

            match drained {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(e),
                Err(_elapsed) => {
                    return Err(anyhow!(
                        "{COMPLIANCE_TEST_OUTPUT_ID} deadline exceeded: {EOF_VOLUME_REQUEST_COUNT} EOF reads spread across {EOF_VOLUME_INFLIGHT_DEPTH} handles did not finish within {EOF_VOLUME_DEADLINE_SECS} s"
                    ));
                }
            }
            info!("PASS {COMPLIANCE_TEST_OUTPUT_ID}: {EOF_VOLUME_REQUEST_COUNT} read-past-EOF requests completed inside the deadline");
            Ok::<(), anyhow::Error>(())
        }
        .await;

        server_process.kill_and_wait().await;
        result
    }

    #[tokio::test]
    async fn regression() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        crate::common::init_logging();
        run_read_past_eof_volume()
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })
    }
}

// CMPTST-30: per-operation handler latency stays inside the ceiling under parallel pipelined sessions.
pub(crate) mod cmptst_30 {
    use super::*;

    const COMPLIANCE_TEST_OUTPUT_ID: &str = "CMPTST-30";

    const PIPE30_SFTP_PORT: u16 = 9031;
    const PIPE30_SFTP_ADDRESS: &str = "127.0.0.1:9031";
    const PIPE30_S3_ADDRESS: &str = "127.0.0.1:9307";
    const PIPE30_S3_ENDPOINT: &str = "http://127.0.0.1:9307";

    // Parameters. The per-operation ceiling is 1 s.
    // LATENCY_INFLIGHT_DEPTH per-session pipelines metadata operations
    // the same way the GUI-client traversal shape does, so a single
    // slow handler shows up against the ceiling instead of being
    // averaged into a passing aggregate.
    const LATENCY_PARALLEL: usize = 8;
    const LATENCY_ITERATIONS: usize = 20;
    const LATENCY_INFLIGHT_DEPTH: usize = 50;
    const LATENCY_PER_OP_CEILING_MILLIS: u64 = 1_000;
    const LATENCY_OVERALL_DEADLINE_SECS: u64 = 120;

    /// One observation: the wall-clock latency for one operation paired
    /// with a static label naming the op type so the failure log
    /// identifies which client-visible category of work produced the
    /// worst sample.
    type LatencyObservation = (Duration, &'static str);

    /// One worker outcome: the worst metadata-op observation (STAT or
    /// READDIR) and the worst fixture-read observation tracked
    /// independently. The ceiling assertion is metadata-only because a
    /// multi-MB SFTP READ inherently round-trips per MAX_READ_LEN chunk
    /// and a 5 MB transfer at ~100 ms per round trip lands well above
    /// the metadata ceiling without representing a wedge regression.
    struct WorkerWorst {
        metadata: LatencyObservation,
        read: Duration,
    }

    /// Worker: open one SFTP session and drive several batches of
    /// deeply-pipelined metadata operations interleaved with full
    /// fixture reads. Each batch fires LATENCY_INFLIGHT_DEPTH
    /// concurrent metadata futures on a single channel, mirroring
    /// GUI-client pipelining. Returns the worst metadata-op
    /// observation alongside the worst fixture-read wall-clock so the
    /// caller can assert against each surface independently.
    async fn cmptst30_worker(address: &str, fixture_path: String, subdir_path: String) -> Result<WorkerWorst> {
        let (_handle, sftp) = connect_sftp_to(address).await?;
        let sftp = Arc::new(sftp);
        let mut worst_meta: LatencyObservation = (Duration::ZERO, "init");
        let mut worst_read: Duration = Duration::ZERO;
        for _ in 0..LATENCY_ITERATIONS {
            let mut pipeline: FuturesUnordered<_> = (0..LATENCY_INFLIGHT_DEPTH)
                .map(|i| {
                    let sftp = Arc::clone(&sftp);
                    let fixture_path = fixture_path.clone();
                    let subdir_path = subdir_path.clone();
                    async move {
                        let t = Instant::now();
                        let op: &'static str = if i % 3 == 0 {
                            sftp.metadata(&fixture_path)
                                .await
                                .map_err(|e| anyhow!("STAT {fixture_path} failed: {e:?}"))?;
                            "metadata-fixture"
                        } else if i % 3 == 1 {
                            sftp.read_dir(&subdir_path)
                                .await
                                .map_err(|e| anyhow!("READDIR {subdir_path} failed: {e:?}"))?;
                            "readdir-subdir"
                        } else {
                            let path = format!("{subdir_path}/file_{:04}.txt", i % SUBDIR_FILE_COUNT);
                            sftp.metadata(&path).await.map_err(|e| anyhow!("STAT {path} failed: {e:?}"))?;
                            "metadata-sibling"
                        };
                        Ok::<LatencyObservation, anyhow::Error>((t.elapsed(), op))
                    }
                })
                .collect();
            while let Some(r) = pipeline.next().await {
                let observation = r?;
                if observation.0 > worst_meta.0 {
                    worst_meta = observation;
                }
            }

            let t = Instant::now();
            let bytes = sftp_read_full(&sftp, &fixture_path)
                .await
                .map_err(|e| anyhow!("READ {fixture_path} failed: {e:?}"))?;
            let elapsed = t.elapsed();
            if elapsed > worst_read {
                worst_read = elapsed;
            }
            if bytes.len() != FIXTURE_SIZE {
                return Err(anyhow!(
                    "READ {fixture_path} byte count mismatch: expected {FIXTURE_SIZE}, got {}",
                    bytes.len()
                ));
            }
        }
        Ok(WorkerWorst {
            metadata: worst_meta,
            read: worst_read,
        })
    }

    pub(crate) async fn run_handler_latency_under_backend_pressure() -> Result<()> {
        info!("{COMPLIANCE_TEST_OUTPUT_ID}: starting handler-latency server on {PIPE30_SFTP_ADDRESS}");
        let (_env, mut server_process) = spawn_pipelining_rustfs(PIPE30_SFTP_ADDRESS, PIPE30_S3_ADDRESS).await?;

        let result = async {
            ProtocolTestEnvironment::wait_for_port_ready(PIPE30_SFTP_PORT, 30)
                .await
                .map_err(|e| anyhow!("{}", e))?;

            let s3 = build_test_s3_client(PIPE30_S3_ENDPOINT);
            wait_for_s3_ready(&s3, S3_READY_ATTEMPTS).await?;

            let bucket = "pipe30";
            let fixture_key = "fixture.bin";
            let subdir = "siblings";
            let _ = seed_pipelining_fixture(&s3, bucket, fixture_key, subdir).await?;

            let fixture_path = format!("/{bucket}/{fixture_key}");
            let subdir_path = format!("/{bucket}/{subdir}");

            let mut handles = Vec::with_capacity(LATENCY_PARALLEL);
            for session_idx in 0..LATENCY_PARALLEL {
                let address = PIPE30_SFTP_ADDRESS.to_string();
                let fixture_path = fixture_path.clone();
                let subdir_path = subdir_path.clone();
                handles.push(tokio::spawn(async move {
                    cmptst30_worker(&address, fixture_path, subdir_path)
                        .await
                        .map_err(|e| anyhow!("session {session_idx}: {e}"))
                }));
            }

            let overall = Duration::from_secs(LATENCY_OVERALL_DEADLINE_SECS);
            let drained = timeout(overall, async {
                let mut worst_meta: LatencyObservation = (Duration::ZERO, "init");
                let mut worst_read = Duration::ZERO;
                for handle in handles {
                    let session = handle.await.map_err(|e| anyhow!("worker join failed: {e}"))??;
                    if session.metadata.0 > worst_meta.0 {
                        worst_meta = session.metadata;
                    }
                    if session.read > worst_read {
                        worst_read = session.read;
                    }
                }
                Ok::<(LatencyObservation, Duration), anyhow::Error>((worst_meta, worst_read))
            })
            .await;

            let (worst_meta, worst_read) = match drained {
                Ok(Ok(p)) => p,
                Ok(Err(e)) => return Err(e),
                Err(_elapsed) => {
                    return Err(anyhow!(
                        "{COMPLIANCE_TEST_OUTPUT_ID} deadline exceeded: workers did not finish within {LATENCY_OVERALL_DEADLINE_SECS} s"
                    ));
                }
            };
            let ceiling = Duration::from_millis(LATENCY_PER_OP_CEILING_MILLIS);
            if worst_meta.0 > ceiling {
                return Err(anyhow!(
                    "{COMPLIANCE_TEST_OUTPUT_ID} metadata ceiling exceeded: worst metadata op {} ms on '{}' > {} ms (worst fixture read {} ms; depth={LATENCY_INFLIGHT_DEPTH})",
                    worst_meta.0.as_millis(),
                    worst_meta.1,
                    ceiling.as_millis(),
                    worst_read.as_millis(),
                ));
            }
            info!(
                "PASS {COMPLIANCE_TEST_OUTPUT_ID}: worst metadata op {} ms on '{}' (ceiling {} ms; worst fixture read {} ms; depth={LATENCY_INFLIGHT_DEPTH})",
                worst_meta.0.as_millis(),
                worst_meta.1,
                ceiling.as_millis(),
                worst_read.as_millis(),
            );
            Ok::<(), anyhow::Error>(())
        }
        .await;

        server_process.kill_and_wait().await;
        result
    }

    #[ignore]
    #[tokio::test]
    async fn regression() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        crate::common::init_logging();
        run_handler_latency_under_backend_pressure()
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })
    }
}

// CMPTST-31: server resilience under client paused-drain, byte-exact completion after a mid-transfer pause.
pub(crate) mod cmptst_31 {
    use super::*;

    const COMPLIANCE_TEST_OUTPUT_ID: &str = "CMPTST-31";

    const PIPE31_SFTP_PORT: u16 = 9032;
    const PIPE31_SFTP_ADDRESS: &str = "127.0.0.1:9032";
    const PIPE31_S3_ADDRESS: &str = "127.0.0.1:9308";
    const PIPE31_S3_ENDPOINT: &str = "http://127.0.0.1:9308";

    // Parameters. Single SFTP session, multi-MB seed, and a
    // deterministic mid-transfer pause on the client-side TCP read
    // half. The pause lets the rustfs server fill its kernel TCP send
    // buffer and exhaust the SSH per-channel recipient_window_size,
    // which is the load-bearing precondition for russh-sftp's
    // stream.flush().await to park inside the per-channel response
    // loop. Pause duration is long enough that the watchdog and any
    // russh keepalives can't reach the parked task before the test
    // observes the symptom.
    const PAUSE31_FIXTURE_BYTES: u64 = 200 * 1024 * 1024;
    const PAUSE31_PRE_PAUSE_BYTES: u64 = 4 * 1024 * 1024;
    const PAUSE31_PAUSE_SECS: u64 = 25;
    const PAUSE31_RESUME_DEADLINE_SECS: u64 = 120;
    const PAUSE31_OVERALL_DEADLINE_SECS: u64 = 240;

    /// Control flag flipped by the test loop to pause the underlying
    /// TCP read half on the client side. Used to deplete the SSH
    /// recipient_window_size on the server side and force
    /// stream.flush() to park.
    struct PauseControl {
        paused: AtomicBool,
    }

    impl PauseControl {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                paused: AtomicBool::new(false),
            })
        }
    }

    /// Wrapper around tokio::net::TcpStream split halves. poll_read
    /// returns Pending while the control flag is set, simulating a
    /// slow-drain client (the FileZilla / Cyberduck shape). poll_write
    /// is unmodified so the russh client can keep sending FXP requests
    /// while the response side is throttled.
    struct PausableStream {
        read: OwnedReadHalf,
        write: OwnedWriteHalf,
        control: Arc<PauseControl>,
    }

    impl PausableStream {
        fn new(stream: TcpStream, control: Arc<PauseControl>) -> Self {
            let (read, write) = stream.into_split();
            Self { read, write, control }
        }
    }

    impl AsyncRead for PausableStream {
        fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            let this = self.get_mut();
            if this.control.paused.load(Ordering::Relaxed) {
                // Return Pending. Spawn a 100 ms-delayed task to wake
                // the future so the runtime re-polls and observes the
                // pause flag once it clears. The 100 ms interval caps
                // wake-up latency after the test releases the pause.
                let waker = cx.waker().clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    waker.wake();
                });
                return Poll::Pending;
            }
            Pin::new(&mut this.read).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for PausableStream {
        fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
            Pin::new(&mut self.get_mut().write).poll_write(cx, buf)
        }

        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.get_mut().write).poll_flush(cx)
        }

        fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.get_mut().write).poll_shutdown(cx)
        }
    }

    /// Connect to the server through a PausableStream so the test loop
    /// can pause the client-side TCP read half mid-transfer. Returns
    /// the russh client handle, the SFTP session, and the control
    /// handle the caller uses to flip the pause state.
    async fn connect_pausable_sftp(
        address: &str,
    ) -> Result<(client::Handle<AcceptAnyServerKey>, SftpSession, Arc<PauseControl>)> {
        let tcp = TcpStream::connect(address)
            .await
            .map_err(|e| anyhow!("TcpStream::connect {address} failed: {e}"))?;
        let control = PauseControl::new();
        let stream = PausableStream::new(tcp, Arc::clone(&control));
        let config = Arc::new(client::Config::default());
        let mut session = client::connect_stream(config, stream, AcceptAnyServerKey)
            .await
            .map_err(|e| anyhow!("russh connect_stream failed: {e}"))?;
        let auth = session
            .authenticate_password(DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY)
            .await
            .map_err(|e| anyhow!("authenticate_password failed: {e}"))?;
        if !auth.success() {
            return Err(anyhow!("{COMPLIANCE_TEST_OUTPUT_ID} password auth rejected"));
        }
        let channel = session
            .channel_open_session()
            .await
            .map_err(|e| anyhow!("channel_open_session failed: {e}"))?;
        channel
            .request_subsystem(true, "sftp")
            .await
            .map_err(|e| anyhow!("request_subsystem failed: {e}"))?;
        // 60 s per-request timeout (default 10 s) so the 25 s test
        // pause does not trip the russh-sftp client's own request
        // timer before the server's flush parking can be observed.
        // The wedge mechanism the case is designed to surface is
        // server side. A client-side request timer firing first masks
        // it.
        let sftp = SftpSession::new_with_config(
            channel.into_stream(),
            Config {
                request_timeout_secs: 60,
                ..Config::default()
            },
        )
        .await
        .map_err(|e| anyhow!("SftpSession::new_with_config failed: {e}"))?;
        Ok((session, sftp, control))
    }

    // Test orchestration in three phases:
    //
    // 1. Pre-pause: drain PAUSE31_PRE_PAUSE_BYTES from the server
    //    normally to confirm the read path is healthy.
    // 2. Pause: flip the PausableStream pause flag and sleep for
    //    PAUSE31_PAUSE_SECS. With reads parked the kernel TCP receive
    //    buffer fills, the SSH per-channel recipient_window depletes,
    //    and the server-side stream.flush() parks inside the response
    //    loop. The pause is intentionally longer than the watchdog
    //    fast-kill threshold so this case proves the watchdog does
    //    NOT kill a session that is parked on flush (only sessions
    //    parked on the russh select! mpsc, see CMPTST-25).
    // 3. Resume: clear the pause flag, drain the remaining bytes
    //    inside PAUSE31_RESUME_DEADLINE_SECS, and SHA-compare against
    //    the seeded fixture to prove byte correctness.
    pub(crate) async fn run_paused_drain_provokes_flush_park() -> Result<()> {
        info!("{COMPLIANCE_TEST_OUTPUT_ID}: starting paused-drain wedge probe server on {PIPE31_SFTP_ADDRESS}");
        let (_env, mut server_process) = spawn_pipelining_rustfs(PIPE31_SFTP_ADDRESS, PIPE31_S3_ADDRESS).await?;
        let server_log = capture_server_stdout(server_process.child_mut());

        let result: Result<()> = async {
            ProtocolTestEnvironment::wait_for_port_ready(PIPE31_SFTP_PORT, 30)
                .await
                .map_err(|e| anyhow!("{}", e))?;

            let s3 = build_test_s3_client(PIPE31_S3_ENDPOINT);
            wait_for_s3_ready(&s3, S3_READY_ATTEMPTS).await?;

            let bucket = "pause31";
            let key = "fixture.bin";
            s3.create_bucket()
                .bucket(bucket)
                .send()
                .await
                .map_err(|e| anyhow!("S3 CreateBucket {bucket} failed: {e:?}"))?;

            let mib = PAUSE31_FIXTURE_BYTES / (1024 * 1024);
            info!("{COMPLIANCE_TEST_OUTPUT_ID}: seeding {mib} MiB via multipart upload");
            let seed_t0 = Instant::now();
            seed_large_via_multipart(&s3, bucket, key, PAUSE31_FIXTURE_BYTES).await?;
            info!("{COMPLIANCE_TEST_OUTPUT_ID}: seed complete in {:?}", seed_t0.elapsed());
            let expected_sha = calculate_pattern_sha256(PAUSE31_FIXTURE_BYTES, THRASH_PATTERN_MULTIPLIER);

            let path = format!("/{bucket}/{key}");
            let (_handle, sftp, control) = connect_pausable_sftp(PIPE31_SFTP_ADDRESS).await?;
            let mut file = sftp
                .open_with_flags(&path, OpenFlags::READ)
                .await
                .map_err(|e| anyhow!("OPEN {path} failed: {e:?}"))?;

            let mut hasher = Sha256::new();
            let mut buf = vec![0u8; 256 * 1024];
            let mut total: u64 = 0;

            // Drain enough bytes that the SSH window has had time to
            // be reset and the connection is in steady state.
            info!(
                "{COMPLIANCE_TEST_OUTPUT_ID}: pre-pause drain up to {} bytes",
                PAUSE31_PRE_PAUSE_BYTES
            );
            while total < PAUSE31_PRE_PAUSE_BYTES {
                let n = file.read(&mut buf).await.map_err(|e| anyhow!("pre-pause READ failed: {e:?}"))?;
                if n == 0 {
                    return Err(anyhow!(
                        "{COMPLIANCE_TEST_OUTPUT_ID} pre-pause drain short: got {total} bytes before EOF, expected at least {PAUSE31_PRE_PAUSE_BYTES}"
                    ));
                }
                hasher.update(&buf[..n]);
                total += n as u64;
            }
            info!("{COMPLIANCE_TEST_OUTPUT_ID}: pre-pause drain complete, drained {total} bytes; flipping pause flag");

            // Pause client-side reads. The server will keep pushing
            // FXP_DATA responses for in-flight FXP_READ requests until
            // its kernel TCP send buffer fills and the SSH
            // recipient_window_size is exhausted. From there
            // stream.flush() is expected to park inside russh-sftp's
            // per-channel response loop.
            control.paused.store(true, Ordering::Relaxed);
            let pause_t0 = Instant::now();

            // Spawn the read continuation. It will block at the first
            // file.read() call once the in-buffer SSH stream is
            // drained.
            let read_handle = tokio::spawn(async move {
                let mut hasher = hasher;
                let mut buf = buf;
                let mut total = total;
                while total < PAUSE31_FIXTURE_BYTES {
                    let n = file.read(&mut buf).await.map_err(|e| anyhow!("post-pause READ failed: {e:?}"))?;
                    if n == 0 {
                        break;
                    }
                    hasher.update(&buf[..n]);
                    total += n as u64;
                }
                let _ = file.shutdown().await;
                let sha: [u8; 32] = hasher.finalize().into();
                Ok::<(u64, [u8; 32]), anyhow::Error>((total, sha))
            });

            // Hold the pause for the configured window.
            tokio::time::sleep(Duration::from_secs(PAUSE31_PAUSE_SECS)).await;
            let pause_elapsed = pause_t0.elapsed();
            info!(
                "{COMPLIANCE_TEST_OUTPUT_ID}: pause window elapsed {pause_elapsed:?}; releasing pause flag"
            );
            control.paused.store(false, Ordering::Relaxed);

            // Read continuation must complete inside the resume
            // deadline. If it does not, the server-side flush did not
            // unwedge after the SSH window was replenished.
            let resume_outcome = tokio::time::timeout(Duration::from_secs(PAUSE31_RESUME_DEADLINE_SECS), read_handle).await;
            let (final_total, observed_sha) = match resume_outcome {
                Ok(join_result) => match join_result {
                    Ok(Ok(p)) => p,
                    Ok(Err(e)) => return Err(e),
                    Err(e) => return Err(anyhow!("read continuation join failed: {e}")),
                },
                Err(_) => {
                    return Err(anyhow!(
                        "{COMPLIANCE_TEST_OUTPUT_ID} resume deadline exceeded: read continuation did not finish within {PAUSE31_RESUME_DEADLINE_SECS} s after the pause flag was released"
                    ));
                }
            };
            if final_total != PAUSE31_FIXTURE_BYTES {
                return Err(anyhow!(
                    "{COMPLIANCE_TEST_OUTPUT_ID} final byte count mismatch: read {final_total} bytes, expected {PAUSE31_FIXTURE_BYTES}"
                ));
            }
            if observed_sha != expected_sha {
                return Err(anyhow!("{COMPLIANCE_TEST_OUTPUT_ID} SHA256 mismatch on {path}"));
            }

            info!(
                "PASS {COMPLIANCE_TEST_OUTPUT_ID}: server delivered {final_total} bytes byte-exact across a {PAUSE31_PAUSE_SECS} s client paused-drain"
            );
            Ok(())
        }
        .await;

        if result.is_err() {
            let buf = server_log.lock().await;
            let lines: Vec<&String> = buf.iter().rev().take(200).collect();
            eprintln!("--- last {} lines of rustfs server stdout (oldest first) ---", lines.len());
            for line in lines.iter().rev() {
                eprintln!("{line}");
            }
            eprintln!("--- end rustfs stdout dump ---");
        }

        let _ = tokio::time::timeout(Duration::from_secs(PAUSE31_OVERALL_DEADLINE_SECS), async {
            server_process.kill_and_wait().await;
        })
        .await;
        result
    }

    #[tokio::test]
    async fn regression() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        crate::common::init_logging();
        run_paused_drain_provokes_flush_park()
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })
    }
}

// CMPTST-32: read-cache enabled regression, 8 MiB download byte-exact with the production cache window.
pub(crate) mod cmptst_32 {
    use super::*;

    const COMPLIANCE_TEST_OUTPUT_ID: &str = "CMPTST-32";

    const PIPE32_SFTP_PORT: u16 = 9033;
    const PIPE32_SFTP_ADDRESS: &str = "127.0.0.1:9033";
    const PIPE32_S3_ADDRESS: &str = "127.0.0.1:9309";
    const PIPE32_S3_ENDPOINT: &str = "http://127.0.0.1:9309";

    pub(crate) async fn run_read_cache_enabled_round_trip() -> Result<()> {
        info!("{COMPLIANCE_TEST_OUTPUT_ID}: starting read-cache enabled run on {PIPE32_SFTP_ADDRESS}");
        let extras = [(ENV_SFTP_READ_CACHE_WINDOW_BYTES, "1048576")];
        let (_env, mut server_process) =
            spawn_pipelining_rustfs_with_extras(PIPE32_SFTP_ADDRESS, PIPE32_S3_ADDRESS, &extras).await?;
        let server_log = capture_server_stdout(server_process.child_mut());

        let result = run_read_cache_byte_correctness(
            PIPE32_SFTP_PORT,
            PIPE32_SFTP_ADDRESS,
            PIPE32_S3_ENDPOINT,
            "pipe32",
            COMPLIANCE_TEST_OUTPUT_ID,
        )
        .await;

        if result.is_err() {
            let buf = server_log.lock().await;
            let lines: Vec<&String> = buf.iter().rev().take(200).collect();
            eprintln!("--- last {} lines of rustfs server stdout (oldest first) ---", lines.len());
            for line in lines.iter().rev() {
                eprintln!("{line}");
            }
            eprintln!("--- end rustfs stdout dump ---");
        }

        let _ = tokio::time::timeout(Duration::from_secs(READ_CACHE_DEADLINE_SECS), async {
            server_process.kill_and_wait().await;
        })
        .await;
        result
    }

    #[tokio::test]
    async fn regression() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        crate::common::init_logging();
        run_read_cache_enabled_round_trip()
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })
    }
}

// CMPTST-33: read-cache disabled regression, 8 MiB download byte-exact with RUSTFS_SFTP_READ_CACHE_WINDOW_BYTES=0.
pub(crate) mod cmptst_33 {
    use super::*;

    const COMPLIANCE_TEST_OUTPUT_ID: &str = "CMPTST-33";

    const PIPE33_SFTP_PORT: u16 = 9034;
    const PIPE33_SFTP_ADDRESS: &str = "127.0.0.1:9034";
    const PIPE33_S3_ADDRESS: &str = "127.0.0.1:9310";
    const PIPE33_S3_ENDPOINT: &str = "http://127.0.0.1:9310";

    pub(crate) async fn run_read_cache_disabled_round_trip() -> Result<()> {
        info!("{COMPLIANCE_TEST_OUTPUT_ID}: starting read-cache disabled run on {PIPE33_SFTP_ADDRESS}");
        let extras = [(ENV_SFTP_READ_CACHE_WINDOW_BYTES, "0")];
        let (_env, mut server_process) =
            spawn_pipelining_rustfs_with_extras(PIPE33_SFTP_ADDRESS, PIPE33_S3_ADDRESS, &extras).await?;
        let server_log = capture_server_stdout(server_process.child_mut());

        let result = run_read_cache_byte_correctness(
            PIPE33_SFTP_PORT,
            PIPE33_SFTP_ADDRESS,
            PIPE33_S3_ENDPOINT,
            "pipe33",
            COMPLIANCE_TEST_OUTPUT_ID,
        )
        .await;

        if result.is_err() {
            let buf = server_log.lock().await;
            let lines: Vec<&String> = buf.iter().rev().take(200).collect();
            eprintln!("--- last {} lines of rustfs server stdout (oldest first) ---", lines.len());
            for line in lines.iter().rev() {
                eprintln!("{line}");
            }
            eprintln!("--- end rustfs stdout dump ---");
        }

        let _ = tokio::time::timeout(Duration::from_secs(READ_CACHE_DEADLINE_SECS), async {
            server_process.kill_and_wait().await;
        })
        .await;
        result
    }

    #[tokio::test]
    async fn regression() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        crate::common::init_logging();
        run_read_cache_disabled_round_trip()
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })
    }
}

// Shared parameters for CMPTST-32 (cache enabled) and CMPTST-33 (cache
// disabled). Both cases seed the same fixture and download it
// end-to-end, then assert byte-count and SHA256 against the
// deterministic seed pattern. The sole difference between the two cases
// is the value of RUSTFS_SFTP_READ_CACHE_WINDOW_BYTES passed to the
// server. Backend call-count assertions are covered at the unit-test
// layer in crates/protocols/src/sftp/read.rs against a DummyBackend
// with explicit response queues. The e2e cases here exist to verify
// byte-correctness under both cache modes against a real ecstore
// backend, since that is the operator-visible regression risk.
const READ_CACHE_FIXTURE_BYTES: u64 = 8 * 1024 * 1024;
const READ_CACHE_DEADLINE_SECS: u64 = 120;

/// Shared body for CMPTST-32 and CMPTST-33. Waits for the SFTP port
/// to come up, seeds the fixture via multipart upload, downloads it
/// end-to-end via streaming SHA256, and asserts byte-count plus
/// SHA256 equality against the deterministic seed pattern. The two
/// cases differ only in the cache window the server was spawned with,
/// which is recorded in case_name for log triage.
async fn run_read_cache_byte_correctness(
    sftp_port: u16,
    sftp_address: &str,
    s3_endpoint: &str,
    bucket: &str,
    case_name: &str,
) -> Result<()> {
    ProtocolTestEnvironment::wait_for_port_ready(sftp_port, 30)
        .await
        .map_err(|e| anyhow!("{}", e))?;

    let s3 = build_test_s3_client(s3_endpoint);
    wait_for_s3_ready(&s3, S3_READY_ATTEMPTS).await?;

    let key = "fixture.bin";
    s3.create_bucket()
        .bucket(bucket)
        .send()
        .await
        .map_err(|e| anyhow!("S3 CreateBucket {bucket} failed: {e:?}"))?;

    info!("{case_name}: seeding {} MiB fixture", READ_CACHE_FIXTURE_BYTES / (1024 * 1024));
    seed_large_via_multipart(&s3, bucket, key, READ_CACHE_FIXTURE_BYTES).await?;
    let expected_sha = calculate_pattern_sha256(READ_CACHE_FIXTURE_BYTES, THRASH_PATTERN_MULTIPLIER);

    let path = format!("/{bucket}/{key}");
    let (_handle, sftp) = connect_sftp_to(sftp_address).await?;
    let download_t0 = Instant::now();
    let (bytes, sha) = streaming_sha256_download(&sftp, &path).await?;
    info!("{case_name}: download finished in {:?}", download_t0.elapsed());

    if bytes != READ_CACHE_FIXTURE_BYTES {
        return Err(anyhow!(
            "{case_name} byte-count mismatch: read {bytes} bytes, expected {READ_CACHE_FIXTURE_BYTES}"
        ));
    }
    if sha != expected_sha {
        return Err(anyhow!("{case_name} SHA256 mismatch on {path}"));
    }

    info!("PASS {case_name}: {} MiB downloaded byte-exact", bytes / (1024 * 1024));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cmptst29_eof_status_matches_protocol_constant() {
        // Compile-time check that the protocol enum the suite depends
        // on is still part of the russh-sftp surface. If the dependency
        // ships a breaking rename the assertion below catches it before
        // the end-to-end test runs.
        let code = StatusCode::Eof;
        assert_eq!(code as u32, 1);
    }
}
