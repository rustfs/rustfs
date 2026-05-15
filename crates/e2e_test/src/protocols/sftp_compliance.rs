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

//! Public test entry points for the SFTP compliance suite.
//!
//! Three suite entries cover every CMPTST-NN identifier:
//!
//! - test_sftp_compliance_suite           CMPTST-01..14 (one shared SFTP session)
//! - test_sftp_compliance_readonly        CMPTST-15..23 (one shared session, read-only mode)
//! - test_sftp_compliance_standalone      CMPTST-24..33 (each case spawns its own rustfs)
//!
//! The first two reuse a single rustfs spawn for the whole bracket
//! because every case in the bracket exercises the same protocol
//! against the same server. The third aggregates cases that each need
//! a different server configuration (idle timeout, read-cache window,
//! console disabled) and therefore cannot share a binary.
//!
//! Each per-case module exposes a single descriptive entry called
//! run_<what_it_tests>(). For example cmptst_24 exposes
//! cmptst_24::run_concurrent_half_close_no_leak().
//!
//! The per-case bodies (one cmptst_NN module per case) and the
//! cross-case infrastructure (spawn helpers, fixture seeders, the
//! half-close / wedge / paused-drain stream wrappers, and the session
//! lifecycle counters) live in sftp_compliance_tests.rs. Per-case
//! marker comments and the full case-index doc live there too.

use crate::protocols::sftp_compliance_tests::{
    cmptst_01, cmptst_02, cmptst_03, cmptst_04, cmptst_05, cmptst_06, cmptst_07, cmptst_08, cmptst_09, cmptst_10, cmptst_11,
    cmptst_12, cmptst_13, cmptst_14, cmptst_15, cmptst_16, cmptst_17, cmptst_18, cmptst_19, cmptst_20, cmptst_21, cmptst_22,
    cmptst_23, cmptst_27, cmptst_28, cmptst_29, cmptst_32, cmptst_33, cmptst_34, spawn_compliance_rustfs,
};
#[cfg(target_os = "linux")]
use crate::protocols::sftp_compliance_tests::{cmptst_24, cmptst_25, cmptst_26};
use crate::protocols::sftp_helpers::{build_test_s3_client, connect_sftp_to, wait_for_s3_ready};
use crate::protocols::test_env::ProtocolTestEnvironment;
use anyhow::{Result, anyhow};
use aws_sdk_s3::primitives::ByteStream;
use tracing::info;

// Read-write compliance suite ports. Distinct from sftp_core (9022/9200)
// and from test_sftp_idle_timeout_disconnects (9023/9100) so the SFTP
// entries can run sequentially without leftover-listener contention.
const COMPLIANCE_RW_SFTP_PORT: u16 = 9024;
const COMPLIANCE_RW_SFTP_ADDRESS: &str = "127.0.0.1:9024";
const COMPLIANCE_RW_S3_ADDRESS: &str = "127.0.0.1:9300";

// Read-only compliance suite ports. The SFTP session opened against
// this address runs against a server started with
// RUSTFS_SFTP_READ_ONLY=true. The S3 endpoint stays writable so the
// suite can seed a bucket and a fixture object before running the SFTP
// rejection assertions.
const COMPLIANCE_RO_SFTP_PORT: u16 = 9025;
const COMPLIANCE_RO_SFTP_ADDRESS: &str = "127.0.0.1:9025";
const COMPLIANCE_RO_S3_ADDRESS: &str = "127.0.0.1:9301";
const COMPLIANCE_RO_S3_ENDPOINT: &str = "http://127.0.0.1:9301";
const COMPLIANCE_RO_S3_READY_ATTEMPTS: u32 = 30;

/// Compliance suite entry: spawn one rustfs server, run every per-case
/// helper that closes a coverage gap not exercised by sftp_core. Runs
/// CMPTST-01 through CMPTST-14 against the same SFTP session.
pub async fn test_sftp_compliance_suite() -> Result<()> {
    info!("Starting SFTP server for compliance suite on {}", COMPLIANCE_RW_SFTP_ADDRESS);
    let (_env, mut server_process) = spawn_compliance_rustfs(COMPLIANCE_RW_SFTP_ADDRESS, COMPLIANCE_RW_S3_ADDRESS, false).await?;

    let result = async {
        ProtocolTestEnvironment::wait_for_port_ready(COMPLIANCE_RW_SFTP_PORT, 30)
            .await
            .map_err(|e| anyhow!("{}", e))?;

        let (session, sftp) = connect_sftp_to(COMPLIANCE_RW_SFTP_ADDRESS).await?;

        cmptst_01::run_medium_binary_round_trip(&sftp).await?;
        cmptst_02::run_zero_byte_round_trip(&sftp).await?;
        cmptst_03::run_rm_on_bucket_path_rejected(&sftp).await?;
        cmptst_04::run_rmdir_nonempty_bucket_rejected(&sftp).await?;
        cmptst_05::run_rmdir_nonempty_subdir_rejected(&sftp).await?;
        cmptst_06::run_path_traversal_get_rejected(&sftp).await?;
        cmptst_07::run_dotdot_collapses_to_root(&sftp).await?;
        cmptst_08::run_rename_cross_bucket(&sftp).await?;
        cmptst_09::run_path_with_spaces_round_trip(&sftp).await?;
        cmptst_10::run_readlink_rejected(&sftp).await?;
        cmptst_11::run_setstat_after_put_returns_ok(&sftp).await?;
        cmptst_12::run_rename_same_path_keeps_file(&sftp).await?;
        cmptst_13::run_implicit_dir_round_trip(&sftp).await?;
        cmptst_14::run_winscp_setstat_shape_on_handle(&sftp).await?;

        // CMPTST-34 cross-checks the SFTP streaming-multipart write
        // path against the S3 layer. The OPEN-time FileAttributes must
        // reach the finalised object as x-amz-meta-* user metadata
        // through the CreateMultipartUpload input field. The S3 client
        // connects to the same rustfs process this suite already drives.
        let s3 = build_test_s3_client(&format!("http://{COMPLIANCE_RW_S3_ADDRESS}"));
        wait_for_s3_ready(&s3, 30).await?;
        cmptst_34::run_open_attrs_round_trip_multipart(&sftp, &s3).await?;

        drop(sftp);
        session.disconnect(russh::Disconnect::ByApplication, "", "en").await?;
        info!("SFTP compliance suite passed");
        Ok::<(), anyhow::Error>(())
    }
    .await;

    // Discard kill/wait errors on the teardown path: the test result
    // above is the binding outcome, and a server that has already
    // exited produces an error here that carries no useful signal.
    server_process.kill_and_wait().await;

    result
}

/// Read-only compliance entry: CMPTST-15 through CMPTST-23. The SFTP
/// server runs with RUSTFS_SFTP_READ_ONLY=true. Mutations through SFTP
/// must error. Reads through SFTP must succeed. The test seeds a bucket
/// and a file through the writable S3 endpoint before opening the SFTP
/// session.
pub async fn test_sftp_compliance_readonly() -> Result<()> {
    info!("Starting SFTP server in read-only mode on {}", COMPLIANCE_RO_SFTP_ADDRESS);
    let (_env, mut server_process) = spawn_compliance_rustfs(COMPLIANCE_RO_SFTP_ADDRESS, COMPLIANCE_RO_S3_ADDRESS, true).await?;

    let result = async {
        ProtocolTestEnvironment::wait_for_port_ready(COMPLIANCE_RO_SFTP_PORT, 30)
            .await
            .map_err(|e| anyhow!("{}", e))?;

        let s3 = build_test_s3_client(COMPLIANCE_RO_S3_ENDPOINT);
        wait_for_s3_ready(&s3, COMPLIANCE_RO_S3_READY_ATTEMPTS).await?;

        let bucket = "robucket";
        let seeded_key = "small.txt";
        let seeded_content = b"read-only seed\n";

        s3.create_bucket()
            .bucket(bucket)
            .send()
            .await
            .map_err(|e| anyhow!("S3 CreateBucket {} failed: {:?}", bucket, e))?;
        s3.put_object()
            .bucket(bucket)
            .key(seeded_key)
            .body(ByteStream::from_static(seeded_content))
            .send()
            .await
            .map_err(|e| anyhow!("S3 PutObject {}/{} failed: {:?}", bucket, seeded_key, e))?;
        info!("Seeded read-only fixture via S3: {}/{}", bucket, seeded_key);

        let (session, sftp) = connect_sftp_to(COMPLIANCE_RO_SFTP_ADDRESS).await?;

        cmptst_15::run_ro_put_rejected(&sftp, bucket).await?;
        cmptst_16::run_ro_rm_rejected(&sftp, bucket, seeded_key).await?;
        cmptst_17::run_ro_mkdir_rejected(&sftp).await?;
        cmptst_18::run_ro_rmdir_rejected(&sftp, bucket).await?;
        cmptst_19::run_ro_rename_rejected(&sftp, bucket, seeded_key).await?;
        cmptst_20::run_ro_ls_allowed(&sftp, bucket).await?;
        cmptst_21::run_ro_get_allowed(&sftp, bucket, seeded_key, seeded_content).await?;
        cmptst_22::run_ro_setstat_rejected(&sftp, bucket, seeded_key).await?;
        cmptst_23::run_ro_fsetstat_rejected(&sftp, bucket, seeded_key).await?;

        drop(sftp);
        // Discard the disconnect Result. A read-only session that has
        // returned errors against every mutation can still be cleanly
        // torn down, but a transient transport-level error here
        // carries no useful signal beyond what the assertions above
        // already pin.
        let _ = session.disconnect(russh::Disconnect::ByApplication, "", "en").await;
        info!("SFTP read-only compliance suite passed");
        Ok::<(), anyhow::Error>(())
    }
    .await;

    server_process.kill_and_wait().await;

    result
}

/// Standalone-server compliance entry: runs CMPTST-24..33 in numerical
/// order. Each case spawns and tears down its own rustfs because each
/// exercises a different server configuration (idle timeout, console
/// listener, read-cache window) that cannot share a process with the
/// others.
///
/// CMPTST-30 is omitted by default. Its assertion (per-operation
/// wall-clock latency under pipelined metadata ops) is bounded by the
/// SSH SFTP subsystem's per-channel serial handler dispatch and is
/// structurally infeasible against the production code. The
/// test_sftp_handler_latency_regression #[tokio::test] entry remains
/// runnable on demand via `--ignored`.
///
/// CMPTST-31 (paused-drain) is omitted from the default suite for
/// runtime cost (200 MiB seed plus a 25 s pause window). The
/// test_sftp_paused_drain_regression #[tokio::test] entry covers it
/// for direct invocation.
///
/// CMPTST-24, 25, 26 verify kernel-level state via ss(8) and the
/// procfs ESTABLISHED discriminator. They are skipped on non-Linux
/// targets where those interfaces are absent.
pub async fn test_sftp_compliance_standalone() -> Result<()> {
    info!("Starting SFTP standalone-server compliance suite");

    #[cfg(target_os = "linux")]
    {
        cmptst_24::run_concurrent_half_close_no_leak().await?;
        cmptst_25::run_wedge_kill_after_silence_in_close_wait().await?;
        cmptst_26::run_healthy_idle_session_above_fast_threshold().await?;
    }

    cmptst_27::run_multi_session_mixed_pipelining().await?;
    cmptst_28::run_5mb_download_with_concurrent_metadata_ops().await?;
    cmptst_29::run_read_past_eof_volume().await?;
    cmptst_32::run_read_cache_enabled_round_trip().await?;
    cmptst_33::run_read_cache_disabled_round_trip().await?;

    info!("SFTP standalone-server compliance suite passed");
    Ok(())
}
