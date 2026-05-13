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

//! Shared #[cfg(test)] helpers used by the per-file test modules in
//! attrs.rs, dir.rs, driver.rs, errors.rs, paths.rs, read.rs, and
//! write.rs. The helpers cover the two test seams: build_driver
//! (constructs a driver around a DummyBackend without a real IAM or S3
//! backend) and write_handle (assembles a HandleState::Write under a
//! given WritePhase without touching the driver).
//!
//! #![allow(dead_code)] silences the rust-analyzer reachability analysis,
//! which does not always follow pub(super) chains across #[cfg(test)] gates.

#![allow(dead_code)]

use super::constants::limits::{
    DEFAULT_BACKEND_OP_TIMEOUT_SECS, DEFAULT_HANDLES_PER_SESSION, READ_CACHE_TOTAL_MEM_DEFAULT, READ_CACHE_WINDOW_DEFAULT,
};
use super::driver::SftpDriver;
use super::lifecycle::SessionDiag;
use super::read_cache::ReadCache;
use super::state::{HandleState, WritePhase};
use crate::common::dummy_storage::DummyBackend;
use crate::common::session::{Protocol, test_session};
use russh_sftp::protocol::FileAttributes;
use std::io::Write;
use std::sync::{Arc, Mutex};
use tracing::Level;
use tracing_subscriber::fmt::MakeWriter;

pub(super) const TEST_PART_SIZE: u64 = 5 * 1024 * 1024;

fn test_session_diag() -> Arc<SessionDiag> {
    let local = "127.0.0.1:2222".parse().expect("loopback parses");
    let peer = "127.0.0.1:0".parse().expect("loopback parses");
    Arc::new(SessionDiag::new(local, peer))
}

/// Build a HandleState::File ready to be inserted directly into a
/// driver handle table without running open_read. The read cache is
/// bound to a fresh per-call accumulator, decoupled from any
/// driver-owned accumulator so the test does not have to thread one
/// through. Tests that need to assert against the driver's
/// accumulator should drive open_read instead.
pub(super) fn file_handle(bucket: &str, key: &str, size: u64, attrs: FileAttributes) -> HandleState {
    HandleState::File {
        bucket: bucket.to_string(),
        key: key.to_string(),
        size,
        attrs,
        read_cache: ReadCache::new(Arc::new(std::sync::atomic::AtomicU64::new(0))),
    }
}

/// Build a HandleState::Write with the given bucket, key, and
/// WritePhase, ready to be inserted directly into a driver handle
/// table without running open_write. Default FileAttributes are used.
pub(super) fn write_handle(bucket: &str, key: &str, phase: WritePhase) -> HandleState {
    HandleState::Write {
        bucket: bucket.to_string(),
        key: key.to_string(),
        attrs: FileAttributes::default(),
        open_attrs: FileAttributes::default(),
        phase,
    }
}

/// Build a read-write SftpDriver around the given backend and part
/// size. Handles per session, backend-op timeout, read-cache window,
/// read-cache total-memory ceiling, and the read-cache accumulator
/// take their defaults from the constants module.
pub(super) fn build_driver(backend: Arc<DummyBackend>, part_size: u64) -> SftpDriver<DummyBackend> {
    let session_diag = test_session_diag();
    SftpDriver::new(
        backend,
        test_session(Protocol::Sftp),
        false,
        part_size,
        DEFAULT_HANDLES_PER_SESSION,
        DEFAULT_BACKEND_OP_TIMEOUT_SECS,
        READ_CACHE_WINDOW_DEFAULT,
        READ_CACHE_TOTAL_MEM_DEFAULT,
        Arc::new(std::sync::atomic::AtomicU64::new(0)),
        session_diag,
    )
}

/// Build a read-only SftpDriver around the given backend and part
/// size. The read-only flag is set so write operations return
/// PermissionDenied. Other parameters take their defaults from the
/// constants module.
pub(super) fn build_readonly_driver(backend: Arc<DummyBackend>, part_size: u64) -> SftpDriver<DummyBackend> {
    let session_diag = test_session_diag();
    SftpDriver::new(
        backend,
        test_session(Protocol::Sftp),
        true,
        part_size,
        DEFAULT_HANDLES_PER_SESSION,
        DEFAULT_BACKEND_OP_TIMEOUT_SECS,
        READ_CACHE_WINDOW_DEFAULT,
        READ_CACHE_TOTAL_MEM_DEFAULT,
        Arc::new(std::sync::atomic::AtomicU64::new(0)),
        session_diag,
    )
}

/// Build a driver with custom read-cache window and total-memory
/// ceiling values. The remaining parameters match build_driver and
/// take their defaults from the constants module.
pub(super) fn build_driver_with_read_cache(
    backend: Arc<DummyBackend>,
    part_size: u64,
    read_cache_window: u64,
    read_cache_total_mem_limit: u64,
) -> SftpDriver<DummyBackend> {
    let session_diag = test_session_diag();
    SftpDriver::new(
        backend,
        test_session(Protocol::Sftp),
        false,
        part_size,
        DEFAULT_HANDLES_PER_SESSION,
        DEFAULT_BACKEND_OP_TIMEOUT_SECS,
        read_cache_window,
        read_cache_total_mem_limit,
        Arc::new(std::sync::atomic::AtomicU64::new(0)),
        session_diag,
    )
}

/// Build a driver with a custom backend timeout for the integration
/// tests that exercise the deadline path against a stalling
/// DummyBackend primitive.
pub(super) fn build_driver_with_timeout(
    backend: Arc<DummyBackend>,
    part_size: u64,
    backend_op_timeout_secs: u64,
) -> SftpDriver<DummyBackend> {
    let session_diag = test_session_diag();
    SftpDriver::new(
        backend,
        test_session(Protocol::Sftp),
        false,
        part_size,
        DEFAULT_HANDLES_PER_SESSION,
        backend_op_timeout_secs,
        READ_CACHE_WINDOW_DEFAULT,
        READ_CACHE_TOTAL_MEM_DEFAULT,
        Arc::new(std::sync::atomic::AtomicU64::new(0)),
        session_diag,
    )
}

/// Tracing writer that appends every emitted byte to a shared buffer.
/// Tests assert on the captured text to discriminate between Err
/// returns that produce a log event and Err returns that stay silent.
#[derive(Clone)]
pub(super) struct CapturingWriter(Arc<Mutex<Vec<u8>>>);

impl Write for CapturingWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("lock").extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for CapturingWriter {
    type Writer = CapturingWriter;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Run the given async block with a fresh tracing subscriber that
/// records every event at the given minimum level into the returned
/// buffer. The subscriber is registered as the default for the
/// duration of the call and removed before this function returns.
/// tokio::test runs on a current-thread runtime so the thread-local
/// default subscriber covers every poll of the future.
///
/// Forces a callsite interest-cache rebuild after install. Without it,
/// a parallel test that triggered the same callsite under a NoSubscriber
/// default first can leave the callsite cached as disabled, so events
/// emitted under this thread's new default never reach the buffer.
pub(super) async fn capture_tracing_at<F, T>(min_level: Level, fut: F) -> (T, String)
where
    F: std::future::Future<Output = T>,
{
    let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
    let writer = CapturingWriter(Arc::clone(&buf));
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(min_level)
        .with_writer(writer)
        .with_ansi(false)
        .with_target(true)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    tracing::callsite::rebuild_interest_cache();
    let value = fut.await;
    let captured = String::from_utf8(buf.lock().expect("lock").clone()).expect("utf8");
    (value, captured)
}
