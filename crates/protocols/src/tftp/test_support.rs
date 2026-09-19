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

//! Shared #[cfg(test)] helpers for TFTP unit tests.

#![allow(dead_code)]

use super::config::{TftpAccessMode, TftpConfig};
use super::handler::TftpStorageHandler;
use super::reader::ObjectReader;
use crate::common::dummy_storage::DummyBackend;
use crate::common::session::{Protocol, test_session};
use rustfs_config::{
    DEFAULT_TFTP_MAX_BLOCK_SIZE, DEFAULT_TFTP_MAX_SEND_RETRIES,
    DEFAULT_TFTP_MAX_WINDOW_SIZE,
};
use rustfs_credentials::Credentials;
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::Level;
use tracing_subscriber::fmt::MakeWriter;

pub(super) const TEST_CLIENT: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 4242);

pub(super) fn test_config(
    access_mode: TftpAccessMode,
    max_concurrent_transfers: usize,
    backend_op_timeout_secs: u64,
    read_fetch_bytes: u64,
) -> TftpConfig {
    TftpConfig {
        bind_addr: "127.0.0.1:6969".parse().expect("loopback parses"),
        default_bucket: Some(String::from("b")),
        access_mode,
        max_block_size: DEFAULT_TFTP_MAX_BLOCK_SIZE,
        max_window_size: DEFAULT_TFTP_MAX_WINDOW_SIZE,
        max_concurrent_transfers,
        backend_op_timeout_secs,
        read_fetch_bytes,
        max_send_retries: DEFAULT_TFTP_MAX_SEND_RETRIES,
    }
}

pub(super) fn build_handler(backend: Arc<DummyBackend>, config: TftpConfig) -> TftpStorageHandler<DummyBackend> {
    let session = test_session(Protocol::Tftp);
    TftpStorageHandler::new(backend.as_ref().clone(), config, Credentials::default(), session)
}

pub(super) fn test_permit() -> OwnedSemaphorePermit {
    Arc::new(Semaphore::new(1))
        .try_acquire_owned()
        .expect("test permit available")
}

pub(super) fn build_reader(
    backend: Arc<DummyBackend>,
    object_size: u64,
    fetch_bytes: u64,
    backend_timeout_secs: u64,
    permit: OwnedSemaphorePermit,
) -> ObjectReader<DummyBackend> {
    ObjectReader::new(
        backend,
        String::from("b"),
        String::from("k"),
        Credentials::default(),
        object_size,
        fetch_bytes,
        std::time::Duration::from_secs(backend_timeout_secs),
        permit,
    )
}

pub(super) async fn read_to_end(reader: &mut ObjectReader<DummyBackend>) -> std::io::Result<Vec<u8>> {
    use futures_lite::AsyncReadExt;
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await?;
    Ok(buf)
}

pub(super) async fn read_chunk(reader: &mut ObjectReader<DummyBackend>, len: usize) -> std::io::Result<Vec<u8>> {
    use futures_lite::AsyncReadExt;
    let mut buf = vec![0_u8; len];
    let n = reader.read(&mut buf).await?;
    buf.truncate(n);
    Ok(buf)
}

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
