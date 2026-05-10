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

//! Core SFTP tests

use crate::common::rustfs_binary_path_with_features;
use crate::protocols::sftp_constants::{
    ENV_RUSTFS_ADDRESS, ENV_SFTP_ADDRESS, ENV_SFTP_ENABLE, ENV_SFTP_HOST_KEY_DIR, ENV_SFTP_IDLE_TIMEOUT, ENV_SFTP_PART_SIZE,
    ENV_SFTP_READ_ONLY,
};
use crate::protocols::sftp_helpers::{
    AcceptAnyServerKey, ServerProcess, build_test_s3_client, connect_sftp_to, generate_host_key, sftp_read_full,
    wait_for_s3_ready,
};
use crate::protocols::test_env::{DEFAULT_ACCESS_KEY, ProtocolTestEnvironment};
use anyhow::{Result, anyhow};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::primitives::ByteStream;
use russh::client::{self, Handle};
use russh_sftp::client::SftpSession;
use russh_sftp::protocol::{FileAttributes, OpenFlags};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::time::sleep;
use tracing::info;

const SFTP_PORT: u16 = 9022;
const SFTP_ADDRESS: &str = "127.0.0.1:9022";

// The cross-protocol assertions reach the same server process the SFTP
// session is connected to. The S3 endpoint is bound on a non-default port to
// avoid contention with any other rustfs running on port 9000 (for example a
// dev-testing harness container).
const S3_ADDRESS: &str = "127.0.0.1:9200";
const S3_ENDPOINT: &str = "http://127.0.0.1:9200";
const S3_READY_ATTEMPTS: u32 = 30;

// Mirrors GLOBAL_DIR_SUFFIX in rustfs_utils::path. The e2e_test crate does
// not depend on rustfs-utils, so the suffix is repeated locally.
const XLDIR_SUFFIX: &str = "__XLDIR__";

// Idle-timeout test uses its own ports so it can run alongside the core suite
// without clashing on the default S3 port or on the core SFTP port.
const IDLE_SFTP_PORT: u16 = 9023;
const IDLE_SFTP_ADDRESS: &str = "127.0.0.1:9023";
const IDLE_S3_ADDRESS: &str = "127.0.0.1:9100";
const IDLE_TIMEOUT_SECS: u64 = 5;
const IDLE_WAIT_SECS: u64 = 10;

// Pin the server's multipart part_size to the spec minimum (5 MiB) so the
// multipart payload in this file is sized relative to a known value and the
// Buffering to Streaming transition triggers deterministically regardless of
// the server's default.
const PART_SIZE_BYTES: usize = 5 * 1024 * 1024;
const PART_SIZE_ENV: &str = "5242880";
// Just over two part_size worth so the upload issues CreateMultipartUpload,
// at least one UploadPart mid-stream, and CompleteMultipartUpload.
const MULTIPART_SIZE: usize = PART_SIZE_BYTES * 2 + 1024;

// Fixed deterministic payload for the S3-write, SFTP-read direction. 256 KiB
// is well below part_size so the SFTP read returns the object as a single
// GetObject response without invoking the streaming multipart path.
const S3_WRITTEN_SIZE: usize = 256 * 1024;

async fn connect_sftp() -> Result<(Handle<AcceptAnyServerKey>, SftpSession)> {
    connect_sftp_to(SFTP_ADDRESS).await
}

/// Confirm that an object is byte-identical when fetched via S3 and via SFTP.
/// Hashes the expected payload once, then compares both fetched payloads
/// against that hash. Either mismatch returns an error naming the side that
/// disagreed.
async fn assert_cross_protocol_sha_match(
    s3: &S3Client,
    sftp: &SftpSession,
    bucket: &str,
    key: &str,
    expected: &[u8],
) -> Result<()> {
    let expected_sha = Sha256::digest(expected);

    let s3_get = s3
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|e| anyhow!("S3 GetObject {}/{} failed: {:?}", bucket, key, e))?;
    let s3_bytes = s3_get
        .body
        .collect()
        .await
        .map_err(|e| anyhow!("S3 body collect failed for {}/{}: {:?}", bucket, key, e))?
        .into_bytes();
    if s3_bytes.len() != expected.len() {
        return Err(anyhow!(
            "S3 GetObject byte count mismatch for {}/{}: expected {}, got {}",
            bucket,
            key,
            expected.len(),
            s3_bytes.len()
        ));
    }
    let s3_sha = Sha256::digest(&s3_bytes);
    if s3_sha != expected_sha {
        return Err(anyhow!("S3 GetObject SHA256 mismatch for {}/{}", bucket, key));
    }

    let sftp_path = format!("/{bucket}/{key}");
    let sftp_bytes = sftp_read_full(sftp, &sftp_path).await?;
    if sftp_bytes.len() != expected.len() {
        return Err(anyhow!(
            "SFTP read byte count mismatch for {}: expected {}, got {}",
            sftp_path,
            expected.len(),
            sftp_bytes.len()
        ));
    }
    let sftp_sha = Sha256::digest(&sftp_bytes);
    if sftp_sha != expected_sha {
        return Err(anyhow!("SFTP read SHA256 mismatch for {}", sftp_path));
    }

    Ok(())
}

/// SFTP core protocol round-trip: banner, mkdir, put, get with SHA compare, rename, delete, rmdir.
pub async fn test_sftp_core_operations() -> Result<()> {
    let env = ProtocolTestEnvironment::new().map_err(|e| anyhow!("{}", e))?;
    let host_key_dir = PathBuf::from(&env.temp_dir).join("sftp_host_keys");
    generate_host_key(&host_key_dir).await?;

    info!("Starting SFTP server on {}", SFTP_ADDRESS);
    let binary_path = rustfs_binary_path_with_features(Some("ftps,webdav,sftp"));
    let host_key_dir_str = host_key_dir
        .to_str()
        .ok_or_else(|| anyhow!("host key dir path is not utf-8"))?;
    let mut server_process = ServerProcess::new(
        Command::new(&binary_path)
            .env(ENV_SFTP_ENABLE, "true")
            .env(ENV_SFTP_ADDRESS, SFTP_ADDRESS)
            .env(ENV_SFTP_HOST_KEY_DIR, host_key_dir_str)
            .env(ENV_SFTP_READ_ONLY, "false")
            .env(ENV_SFTP_PART_SIZE, PART_SIZE_ENV)
            .env(ENV_RUSTFS_ADDRESS, S3_ADDRESS)
            .arg(&env.temp_dir)
            .spawn()?,
    );

    let result = async {
        ProtocolTestEnvironment::wait_for_port_ready(SFTP_PORT, 30)
            .await
            .map_err(|e| anyhow!("{}", e))?;

        let (session, sftp) = connect_sftp().await?;

        // --- 1. Subsystem canary: SFTP session reachable after password auth ---
        // SftpSession::new completes the SFTPv3 version exchange. The
        // canonicalize call below is a cheap round-trip that confirms
        // the session handles real wire traffic.
        info!("Testing SFTP: subsystem canary, server resolves '.' to an absolute path");
        let pwd = sftp.canonicalize(".").await?;
        assert!(!pwd.is_empty(), "server must resolve '.' to a non-empty absolute path");
        info!("PASS: subsystem canary: server resolved '.' to {}", pwd);

        // --- 2. Bucket lifecycle: mkdir then root listing ---
        let bucket = "coretestbucket";
        let bucket_path = format!("/{bucket}");
        info!("Testing SFTP: mkdir bucket {}", bucket_path);
        sftp.create_dir(&bucket_path).await?;
        info!("PASS: mkdir bucket {}", bucket_path);

        info!("Testing SFTP: root listing includes the new bucket");
        let root_entries: Vec<String> = sftp.read_dir("/").await?.map(|e| e.file_name()).collect();
        assert!(root_entries.iter().any(|n| n == bucket), "root listing should contain the new bucket");
        info!("PASS: bucket {} appeared in read_dir(\"/\")", bucket);

        // --- 3. Small-file round-trip with SHA256 compare ---
        info!("Testing SFTP: small-file round-trip with SHA256 compare");
        let small_path = format!("/{bucket}/small.txt");
        let small_content = b"hello rustfs sftp\n";
        let mut wf = sftp
            .open_with_flags(&small_path, OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::WRITE)
            .await?;
        wf.write_all(small_content).await?;
        wf.flush().await?;
        wf.shutdown().await?;

        let mut rf = sftp.open_with_flags(&small_path, OpenFlags::READ).await?;
        let mut buf = Vec::new();
        rf.read_to_end(&mut buf).await?;
        rf.shutdown().await?;
        assert_eq!(buf.as_slice(), small_content, "small-file round-trip content mismatch");
        let sha_in = Sha256::digest(small_content);
        let sha_out = Sha256::digest(&buf);
        assert_eq!(sha_in, sha_out, "small-file SHA256 mismatch");
        info!("PASS: small-file round-trip SHA256 match");

        // --- 4. Path STAT on a file and on a bucket ---
        info!("Testing SFTP: stat on file returns size and file type");
        let file_meta = sftp.metadata(&small_path).await?;
        assert_eq!(file_meta.size, Some(small_content.len() as u64), "stat size mismatch");
        assert!(file_meta.file_type().is_file(), "stat on a file must report regular file");
        info!("PASS: stat on file reports size {} and file type", small_content.len());

        info!("Testing SFTP: stat on bucket reports directory");
        let bucket_meta = sftp.metadata(&bucket_path).await?;
        assert!(bucket_meta.file_type().is_dir(), "stat on a bucket must report directory");
        info!("PASS: stat on bucket reports directory");

        // --- 5. SETSTAT on a file path returns ok ---
        // SETSTAT is a no-op on the server because S3 has no POSIX mtime or permission
        // semantics, but it must still return ok. Clients that send SETSTAT after every
        // transfer (rsync, WinSCP) treat a non-ok status as a transfer failure.
        info!("Testing SFTP: setstat on a path returns ok");
        let attrs = FileAttributes {
            permissions: Some(0o644),
            ..FileAttributes::default()
        };
        sftp.set_metadata(&small_path, attrs).await?;
        info!("PASS: setstat returned ok");

        // --- 6. Rename within bucket and listing reflects it ---
        info!("Testing SFTP: rename within bucket");
        let renamed = format!("/{bucket}/renamed.txt");
        sftp.rename(&small_path, &renamed).await?;
        info!("PASS: rename {} -> {}", small_path, renamed);

        info!("Testing SFTP: listing reflects rename");
        let bucket_entries: Vec<String> = sftp.read_dir(&bucket_path).await?.map(|e| e.file_name()).collect();
        assert!(bucket_entries.iter().any(|n| n == "renamed.txt"), "renamed file must be listed");
        assert!(!bucket_entries.iter().any(|n| n == "small.txt"), "pre-rename name must be gone");
        info!("PASS: directory listing reflects the rename");

        // --- 7. Multipart round-trip across the part-size boundary ---
        // MULTIPART_SIZE is paired with RUSTFS_SFTP_PART_SIZE above so the upload crosses the
        // multipart threshold regardless of server defaults.
        info!("Testing SFTP: multipart-sized round-trip with SHA256 compare");
        let big_path = format!("/{bucket}/big.bin");
        let big_content: Vec<u8> = (0..MULTIPART_SIZE).map(|i| (i as u8).wrapping_mul(31)).collect();
        let mut bwf = sftp
            .open_with_flags(&big_path, OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::WRITE)
            .await?;
        bwf.write_all(&big_content).await?;
        bwf.flush().await?;
        bwf.shutdown().await?;

        let mut brf = sftp.open_with_flags(&big_path, OpenFlags::READ).await?;
        let mut big_buf = Vec::with_capacity(MULTIPART_SIZE);
        brf.read_to_end(&mut big_buf).await?;
        brf.shutdown().await?;
        assert_eq!(big_buf.len(), MULTIPART_SIZE, "multipart round-trip length mismatch");
        let big_in = Sha256::digest(&big_content);
        let big_out = Sha256::digest(&big_buf);
        assert_eq!(big_in, big_out, "multipart SHA256 mismatch");
        info!("PASS: multipart round-trip SHA256 match ({} bytes)", MULTIPART_SIZE);

        // --- 8. Negative cases: symlink, open nonexistent, read_dir nonexistent, path escape ---
        info!("Testing SFTP: symlink returns an error");
        let symlink_err = sftp.symlink(&big_path, &format!("/{bucket}/shortcut")).await;
        assert!(symlink_err.is_err(), "symlink must be rejected by the server");
        info!("PASS: symlink rejected");

        info!("Testing SFTP: open of nonexistent file returns an error");
        let missing_path = format!("/{bucket}/not_here.txt");
        let missing_err = sftp.open_with_flags(&missing_path, OpenFlags::READ).await;
        assert!(missing_err.is_err(), "open of a nonexistent path must error");
        info!("PASS: open of nonexistent file rejected");

        info!("Testing SFTP: read_dir of nonexistent bucket returns an error");
        let missing_bucket = sftp.read_dir("/nosuchbucket").await;
        assert!(missing_bucket.is_err(), "read_dir of a nonexistent bucket must error");
        info!("PASS: read_dir of nonexistent bucket rejected");

        info!("Testing SFTP: path traversal cannot escape the storage root");
        let traversal = sftp.read_dir("/../../../etc").await;
        assert!(traversal.is_err(), "path traversal must be rejected or resolve to a nonexistent bucket");
        info!("PASS: path traversal rejected");

        // --- Spec-letter assertion: APPEND open-flag returns an error ---
        // The driver maps APPEND to OpUnsupported because S3 has no append
        // primitive. Open requests with APPEND must return a failure rather
        // than allow a silently mistruncated upload.
        info!("Testing SFTP: open with APPEND returns an error");
        let append_err = sftp.open_with_flags(&renamed, OpenFlags::APPEND | OpenFlags::WRITE).await;
        assert!(append_err.is_err(), "open with APPEND must error");
        info!("PASS: open with APPEND rejected");

        // --- Spec-letter assertion: O_EXCL on existing path returns an error ---
        // CREATE | EXCLUDE on a key that already exists must fail. The
        // existing renamed.txt is the target. EXCLUDE without WRITE is
        // rejected by the russh-sftp client itself, so WRITE is included.
        // TRUNCATE is included because the driver requires WRITE | CREATE
        // | TRUNCATE on every accepted write OPEN.
        info!("Testing SFTP: open with CREATE + EXCLUDE on existing path returns an error");
        let excl_err = sftp
            .open_with_flags(&renamed, OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::EXCLUDE | OpenFlags::WRITE)
            .await;
        assert!(excl_err.is_err(), "CREATE+EXCLUDE on existing path must error");
        info!("PASS: CREATE+EXCLUDE on existing path rejected");

        // --- WRITE without CREATE or TRUNCATE is rejected at OPEN ---
        // The streaming write path overwrites the entire object at
        // close. A WRITE-only OPEN asks for partial-write semantics
        // the server cannot honour against S3, so the OPEN is
        // rejected before any handle is allocated.
        info!("Testing SFTP: open with WRITE only returns an error");
        let write_only_err = sftp.open_with_flags(&renamed, OpenFlags::WRITE).await;
        assert!(write_only_err.is_err(), "WRITE without CREATE or TRUNCATE must be rejected at OPEN");
        info!("PASS: WRITE only rejected");

        // --- WRITE | CREATE without TRUNCATE is rejected at OPEN ---
        // Without TRUNCATE the client is asking for create-or-modify-
        // existing semantics. The server cannot deliver that against
        // S3, so the OPEN is rejected before any handle is allocated.
        info!("Testing SFTP: open with WRITE | CREATE without TRUNCATE returns an error");
        let create_no_trunc_err = sftp.open_with_flags(&renamed, OpenFlags::WRITE | OpenFlags::CREATE).await;
        assert!(create_no_trunc_err.is_err(), "WRITE | CREATE without TRUNCATE must be rejected at OPEN");
        info!("PASS: WRITE | CREATE without TRUNCATE rejected");

        // --- Spec-letter assertion: bad password is rejected (separate session) ---
        // Fresh russh session with wrong credentials. The authenticated
        // handle is left untouched. Bad auth must not succeed.
        info!("Testing SFTP: second russh session with wrong password is rejected");
        let bad_config = Arc::new(client::Config::default());
        let mut bad_session = client::connect(bad_config, SFTP_ADDRESS, AcceptAnyServerKey).await?;
        let bad_auth = bad_session.authenticate_password(DEFAULT_ACCESS_KEY, "wrong-secret").await?;
        assert!(!bad_auth.success(), "bad-password authentication must not succeed");
        // Discard the disconnect Result. A server that already rejected
        // auth can return an error here, but the assert above already
        // pins the auth outcome.
        let _ = bad_session.disconnect(russh::Disconnect::ByApplication, "", "en").await;
        info!("PASS: bad-password authentication rejected");

        // --- Cross-protocol setup: aws-sdk-s3 client against the same server ---
        // The rustfs binary spawned for this suite serves both SFTP on port
        // 9022 and S3 on port 9000. The S3 stack may need a moment to finish
        // initialising after TCP is listening, so list_buckets is polled
        // until it succeeds before any cross-protocol assertion runs.
        info!("Testing SFTP: prepare aws-sdk-s3 client and wait for S3 readiness");
        let s3 = build_test_s3_client(S3_ENDPOINT);
        wait_for_s3_ready(&s3, S3_READY_ATTEMPTS).await?;
        info!("PASS: S3 endpoint reachable from cross-protocol client");

        // --- SFTP write, S3 read: SHA256 round-trip ---
        // SFTP creates the object, then assert_cross_protocol_sha_match
        // fetches it via both S3 GetObject and SFTP READ and compares
        // each result against the SHA256 of the original payload. Both
        // sides must match byte-exact, which proves the storage layer
        // returns the same bytes regardless of wire protocol.
        info!("Testing SFTP: SFTP write then S3 read, SHA256 round-trip");
        let sftp_to_s3_key = "sftp_written.bin";
        let sftp_to_s3_path = format!("/{bucket}/{sftp_to_s3_key}");
        let sftp_to_s3_content: Vec<u8> = (0..S3_WRITTEN_SIZE).map(|i| (i as u8).wrapping_mul(17)).collect();
        let mut wf = sftp
            .open_with_flags(&sftp_to_s3_path, OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::WRITE)
            .await?;
        wf.write_all(&sftp_to_s3_content).await?;
        wf.flush().await?;
        wf.shutdown().await?;
        assert_cross_protocol_sha_match(&s3, &sftp, bucket, sftp_to_s3_key, &sftp_to_s3_content).await?;
        info!("PASS: SFTP-written object matches via S3 GetObject and SFTP READ");

        // --- S3 write, SFTP read: SHA256 round-trip ---
        // aws-sdk-s3 PutObject writes a fixed deterministic payload. Both
        // sides then read it back and SHA-compare.
        info!("Testing SFTP: S3 write then SFTP read, SHA256 round-trip");
        let s3_to_sftp_key = "s3_written.bin";
        let s3_to_sftp_content: Vec<u8> = (0..S3_WRITTEN_SIZE).map(|i| (i as u8).wrapping_mul(31)).collect();
        s3.put_object()
            .bucket(bucket)
            .key(s3_to_sftp_key)
            .body(ByteStream::from(s3_to_sftp_content.clone()))
            .send()
            .await
            .map_err(|e| anyhow!("S3 PutObject {}/{} failed: {:?}", bucket, s3_to_sftp_key, e))?;
        assert_cross_protocol_sha_match(&s3, &sftp, bucket, s3_to_sftp_key, &s3_to_sftp_content).await?;
        info!("PASS: S3-written object matches via S3 GetObject and SFTP READ");

        // --- Cross-API directory visibility: SFTP mkdir, S3 ListObjectsV2 ---
        // SFTP mkdir writes a __XLDIR__ marker. The rustfs S3 listing path
        // decodes that marker back to a trailing-slash key, so the asserted
        // pattern is "subdir_sftp/".
        info!("Testing SFTP: SFTP-created sub-directory visible via S3 ListObjectsV2");
        let sftp_subdir_name = "subdir_sftp";
        let sftp_subdir_path = format!("/{bucket}/{sftp_subdir_name}");
        sftp.create_dir(&sftp_subdir_path).await?;
        let listed = s3
            .list_objects_v2()
            .bucket(bucket)
            .prefix(sftp_subdir_name)
            .send()
            .await
            .map_err(|e| anyhow!("S3 ListObjectsV2 {} failed: {:?}", bucket, e))?;
        let listed_keys: Vec<String> = listed
            .contents()
            .iter()
            .filter_map(|obj| obj.key().map(|s| s.to_string()))
            .collect();
        let visible_via_s3 = listed_keys
            .iter()
            .any(|k| k == &format!("{sftp_subdir_name}/") || k == &format!("{sftp_subdir_name}{XLDIR_SUFFIX}"));
        assert!(
            visible_via_s3,
            "SFTP-created sub-directory must appear in S3 ListObjectsV2: keys returned were {listed_keys:?}"
        );
        info!("PASS: SFTP mkdir visible to S3 ListObjectsV2");

        // --- Cross-API directory visibility: S3 marker, SFTP readdir ---
        // aws-sdk-s3 PutObject writes a zero-byte marker keyed with the
        // __XLDIR__ suffix. SFTP readdir must decode the marker back to a
        // bare directory entry whose file_type reports as a directory.
        info!("Testing SFTP: S3-created __XLDIR__ marker visible via SFTP readdir");
        let s3_subdir_name = "subdir_s3";
        let s3_subdir_marker_key = format!("{s3_subdir_name}{XLDIR_SUFFIX}");
        s3.put_object()
            .bucket(bucket)
            .key(&s3_subdir_marker_key)
            .body(ByteStream::from_static(b""))
            .send()
            .await
            .map_err(|e| anyhow!("S3 PutObject {}/{} failed: {:?}", bucket, s3_subdir_marker_key, e))?;
        let bucket_entries: Vec<(String, bool)> = sftp
            .read_dir(&bucket_path)
            .await?
            .map(|entry| (entry.file_name(), entry.file_type().is_dir()))
            .collect();
        let visible_via_sftp = bucket_entries.iter().any(|(name, is_dir)| name == s3_subdir_name && *is_dir);
        assert!(
            visible_via_sftp,
            "S3-created marker must appear as a directory in SFTP readdir: entries were {bucket_entries:?}"
        );
        info!("PASS: S3 marker visible to SFTP readdir as a directory");

        // --- Pre-cleanup of cross-protocol fixtures ---
        // Removes the new files and sub-directories so the existing rmdir
        // call below operates against an empty bucket.
        info!("Testing SFTP: pre-cleanup of cross-protocol fixtures");
        sftp.remove_file(&format!("/{bucket}/{sftp_to_s3_key}")).await?;
        sftp.remove_file(&format!("/{bucket}/{s3_to_sftp_key}")).await?;
        sftp.remove_dir(&sftp_subdir_path).await?;
        sftp.remove_dir(&format!("/{bucket}/{s3_subdir_name}")).await?;
        info!("PASS: cross-protocol fixtures removed");

        // --- 9. Cleanup: delete objects, rmdir bucket, confirm root empty ---
        info!("Testing SFTP: delete objects then rmdir bucket");
        sftp.remove_file(&renamed).await?;
        sftp.remove_file(&big_path).await?;
        sftp.remove_dir(&bucket_path).await?;
        info!("PASS: delete + rmdir leaves the root empty");

        let final_entries: Vec<String> = sftp.read_dir("/").await?.map(|e| e.file_name()).collect();
        assert!(!final_entries.iter().any(|n| n == bucket), "bucket must be gone after rmdir");
        info!("PASS: root listing no longer includes the deleted bucket");

        drop(sftp);
        session.disconnect(russh::Disconnect::ByApplication, "", "en").await?;
        info!("SFTP core tests passed");
        Ok::<(), anyhow::Error>(())
    }
    .await;

    // Discard kill/wait errors on the teardown path: the test result
    // above is the binding outcome, and a server that has already
    // exited produces an error here that carries no useful signal.
    server_process.kill_and_wait().await;

    result
}

/// Idle-timeout regression: the server must close an SFTP session that
/// remains inactive past RUSTFS_SFTP_IDLE_TIMEOUT.
///
/// Spawns its own rustfs binary on dedicated SFTP and S3 ports so it can run
/// independently of the core protocol suite. The disconnect check issues a
/// cheap SFTP request after the wait window. The same error path runs in any
/// client when the server-initiated SSH_MSG_DISCONNECT arrives. The assertion
/// does not pin a specific russh error variant because the exact error
/// returned on server-initiated disconnect depends on timing.
pub async fn test_sftp_idle_timeout_disconnects() -> Result<()> {
    let env = ProtocolTestEnvironment::new().map_err(|e| anyhow!("{}", e))?;
    let host_key_dir = PathBuf::from(&env.temp_dir).join("sftp_host_keys");
    generate_host_key(&host_key_dir).await?;

    info!("Starting SFTP server with idle timeout {} s on {}", IDLE_TIMEOUT_SECS, IDLE_SFTP_ADDRESS);
    let binary_path = rustfs_binary_path_with_features(Some("ftps,webdav,sftp"));
    let host_key_dir_str = host_key_dir
        .to_str()
        .ok_or_else(|| anyhow!("host key dir path is not utf-8"))?;
    let mut server_process = ServerProcess::new(
        Command::new(&binary_path)
            .env(ENV_SFTP_ENABLE, "true")
            .env(ENV_SFTP_ADDRESS, IDLE_SFTP_ADDRESS)
            .env(ENV_SFTP_HOST_KEY_DIR, host_key_dir_str)
            .env(ENV_SFTP_READ_ONLY, "false")
            .env(ENV_SFTP_PART_SIZE, PART_SIZE_ENV)
            .env(ENV_SFTP_IDLE_TIMEOUT, IDLE_TIMEOUT_SECS.to_string())
            .env(ENV_RUSTFS_ADDRESS, IDLE_S3_ADDRESS)
            .arg(&env.temp_dir)
            .spawn()?,
    );

    let result = async {
        ProtocolTestEnvironment::wait_for_port_ready(IDLE_SFTP_PORT, 30)
            .await
            .map_err(|e| anyhow!("{}", e))?;

        let (session, sftp) = connect_sftp_to(IDLE_SFTP_ADDRESS).await?;

        // Confirm the session is live before the wait so a failure in the
        // post-wait read can be attributed to the idle timer rather than to
        // a setup defect.
        let pwd = sftp.canonicalize(".").await?;
        assert!(!pwd.is_empty(), "server must resolve '.' to a non-empty absolute path");

        info!("Idle wait: sleeping {} s past idle timeout {} s", IDLE_WAIT_SECS, IDLE_TIMEOUT_SECS);
        sleep(Duration::from_secs(IDLE_WAIT_SECS)).await;

        let post_idle = sftp.read_dir("/").await;
        assert!(
            post_idle.is_err(),
            "SFTP request after idle wait must error once the server has closed the session"
        );
        info!("PASS: SFTP request after idle wait returned an error");

        drop(sftp);
        // Discard the disconnect Result. The server has already closed the
        // session via the idle-timeout path the test is probing. A client
        // disconnect against a half-closed transport may itself return Err
        // with no useful signal.
        let _ = session.disconnect(russh::Disconnect::ByApplication, "", "en").await;
        Ok::<(), anyhow::Error>(())
    }
    .await;

    // Discard kill/wait errors on the teardown path: the test result above
    // is the binding outcome, and a server that has already exited produces
    // an error here that carries no useful signal.
    server_process.kill_and_wait().await;

    result
}
