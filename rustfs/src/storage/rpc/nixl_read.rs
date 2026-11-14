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

//! Serving side of internode NIXL chunk reads and shard writes.
//!
//! A peer asks for one range of a shard file on a local disk and names a
//! buffer registered with its NIXL agent (signed query); this node reads the
//! range into its own registered staging memory and writes it into that buffer
//! with a NIXL WRITE before answering with the byte count. The request's
//! signature is checked by the internode dispatcher before it reaches here.

use crate::storage::storage_api::rpc_consumer::nixl_read::{
    Disk, DiskAPI as _, NIXL_BYTES_HEADER, NIXL_METADATA_HEADER, WRITE_FILE_NIXL_PATH, check_and_record_signed_rpc_nonce,
    find_local_disk_by_ref_for_rpc,
};
use http::{HeaderValue, Request, Response, StatusCode};
use hyper::body::Incoming;
use rustfs_io_metrics::internode_metrics::{INTERNODE_OPERATION_PUT_FILE_STREAM, INTERNODE_TRANSPORT_BACKEND_RDMA};
use rustfs_nixl::RdmaToken;
use rustfs_s3_types::Body;
use sha2::{Digest, Sha256};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tracing::warn;

/// This node's NIXL agent, installed by the S3-over-RDMA service once it runs.
/// It lives here, below the app layer, so these handlers can reach it.
static NIXL_SERVER: OnceLock<Arc<rustfs_nixl::NixlServer>> = OnceLock::new();

/// Records this node's NIXL agent; the first installation wins.
pub(crate) fn install_nixl_server(server: Arc<rustfs_nixl::NixlServer>) {
    let _ = NIXL_SERVER.set(server);
}

/// This node's NIXL agent, once the transport service is running.
pub(crate) fn nixl_server() -> Option<Arc<rustfs_nixl::NixlServer>> {
    NIXL_SERVER.get().cloned()
}

/// Finishes one segment's file. A flush is not enough: the direct-I/O disk
/// writer writes its trailing sub-alignment bytes only on shutdown, so a
/// segment closed with a flush loses its tail (as the TCP put-file path,
/// this shuts the writer down).
async fn finish_segment_file<W: AsyncWrite + Unpin>(file: &mut W) -> std::io::Result<()> {
    file.shutdown().await
}

const EVENT_NIXL_INTERNODE_READ: &str = "nixl_internode_read";
const LOG_COMPONENT_INTERNODE_RPC: &str = "internode_rpc";
const LOG_SUBSYSTEM_NIXL: &str = "nixl";
/// How long a request waits for a free staging chunk before it is refused.
const STAGING_WAIT: Duration = Duration::from_secs(30);

#[derive(Debug, serde::Deserialize)]
struct NixlReadQuery {
    disk: String,
    volume: String,
    path: String,
    offset: u64,
    length: usize,
    nixl_agent: String,
    nixl_buffer: String,
    nixl_md: String,
}

fn reply(status: StatusCode, message: impl Into<String>) -> Response<Body> {
    let mut response = Response::new(Body::from(message.into()));
    *response.status_mut() = status;
    response
}

fn refuse(status: StatusCode, reason: &'static str, detail: impl std::fmt::Display) -> Response<Body> {
    warn!(
        event = EVENT_NIXL_INTERNODE_READ,
        component = LOG_COMPONENT_INTERNODE_RPC,
        subsystem = LOG_SUBSYSTEM_NIXL,
        result = "refused",
        status_code = status.as_u16(),
        reason,
        detail = %detail,
        "nixl internode read refused"
    );
    reply(status, reason)
}

/// Lowercase hex SHA-256 of `bytes`.
pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    hex_simd::encode_to_string(Sha256::digest(bytes), hex_simd::AsciiCase::Lower)
}

pub(crate) async fn handle_read_file_nixl(req: Request<Incoming>) -> Response<Body> {
    let query: NixlReadQuery = match serde_urlencoded::from_str(req.uri().query().unwrap_or_default()) {
        Ok(query) => query,
        Err(error) => return refuse(StatusCode::BAD_REQUEST, "bad_query", error),
    };
    let Some(server) = nixl_server() else {
        return reply(StatusCode::SERVICE_UNAVAILABLE, "nixl transport is not running on this node");
    };

    // The metadata travels unsigned in a header; it must match the signed digest,
    // so a replayed request cannot point the transfer at another endpoint.
    let metadata = match req.headers().get(NIXL_METADATA_HEADER).map(HeaderValue::to_str) {
        Some(Ok(value)) => Some(value),
        Some(Err(_)) => return refuse(StatusCode::BAD_REQUEST, "bad_metadata_header", "not ascii"),
        None => None,
    };
    if let Some(encoded) = metadata {
        let raw = match base64_simd::STANDARD.decode_to_vec(encoded) {
            Ok(raw) => raw,
            Err(error) => return refuse(StatusCode::BAD_REQUEST, "bad_metadata_encoding", error),
        };
        if sha256_hex(&raw) != query.nixl_md.to_ascii_lowercase() {
            return refuse(StatusCode::FORBIDDEN, "metadata_digest_mismatch", &query.nixl_agent);
        }
    }
    let token = match RdmaToken::from_headers(&query.nixl_agent, metadata, &query.nixl_buffer) {
        Ok(token) => token,
        Err(error) => return refuse(StatusCode::BAD_REQUEST, "bad_nixl_token", error),
    };
    if query.length == 0 || query.length as u64 > token.buffer_len || query.length > server.chunk_bytes() {
        return refuse(StatusCode::BAD_REQUEST, "bad_length", query.length);
    }

    let disk = match find_local_disk_by_ref_for_rpc(&query.disk).await {
        Ok(Some(disk)) => disk,
        Ok(None) => return refuse(StatusCode::BAD_REQUEST, "disk_not_found", &query.disk),
        Err(_) => return reply(StatusCode::SERVICE_UNAVAILABLE, "background storage admission deferred"),
    };
    let Disk::Local(local) = disk.as_ref() else {
        return refuse(StatusCode::BAD_REQUEST, "disk_not_local", &query.disk);
    };
    let path = match local.get_object_path_if_local(&query.volume, &query.path) {
        Ok(path) => path,
        Err(error) => return refuse(StatusCode::BAD_REQUEST, "bad_path", error),
    };

    let chunk = match server.reserve(STAGING_WAIT).await {
        Ok(chunk) => chunk,
        Err(error) => return refuse(StatusCode::SERVICE_UNAVAILABLE, "staging_exhausted", error),
    };
    let (offset, length, key) = (query.offset, query.length, query.path.clone());
    let read = tokio::task::spawn_blocking(move || -> std::io::Result<rustfs_nixl::StagingChunk> {
        use std::os::unix::fs::FileExt;
        let mut chunk = chunk;
        let file = super::open_shard_contained(&path, &key)?;
        let spare = chunk.spare_mut().map_err(std::io::Error::other)?;
        let dst = &mut spare[..length];
        let mut filled = 0;
        while filled < dst.len() {
            let n = file.read_at(&mut dst[filled..], offset + filled as u64)?;
            if n == 0 {
                break;
            }
            filled += n;
        }
        chunk.advance(filled).map_err(std::io::Error::other)?;
        Ok(chunk)
    })
    .await;
    let chunk = match read {
        Ok(Ok(chunk)) => chunk,
        Ok(Err(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            return reply(StatusCode::NOT_FOUND, "file not found");
        }
        Ok(Err(error)) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            return refuse(StatusCode::FORBIDDEN, "path_outside_volume", error);
        }
        Ok(Err(error)) => return refuse(StatusCode::INTERNAL_SERVER_ERROR, "read_failed", error),
        Err(error) => return refuse(StatusCode::INTERNAL_SERVER_ERROR, "read_task_failed", error),
    };

    let landed = chunk.len();
    if landed > 0
        && let Err(error) = server.write_to_client_owned(&query.path, &token, 0, chunk).await
    {
        return refuse(StatusCode::BAD_GATEWAY, "nixl_write_failed", error);
    }
    let mut response = reply(StatusCode::OK, "");
    response
        .headers_mut()
        .insert(NIXL_BYTES_HEADER, HeaderValue::from(landed as u64));
    response
}

#[derive(Debug, serde::Deserialize)]
struct NixlWriteQuery {
    disk: String,
    volume: String,
    path: String,
    append: bool,
    size: i64,
    last: bool,
    nixl_nonce: uuid::Uuid,
    nixl_agent: String,
    nixl_buffers: String,
    nixl_md: String,
}

/// A segment of a shard write: pull every named buffer from the writer's
/// agent (NIXL READ) and write it at the end of the file through the same
/// disk writer the TCP path uses. The nonce is recorded so a replayed request
/// is refused; the signature covers the buffers and the metadata digest.
pub(crate) async fn handle_write_file_nixl(req: Request<Incoming>) -> Response<Body> {
    let query: NixlWriteQuery = match serde_urlencoded::from_str(req.uri().query().unwrap_or_default()) {
        Ok(query) => query,
        Err(error) => return refuse(StatusCode::BAD_REQUEST, "bad_query", error),
    };
    if let Err(error) = check_and_record_signed_rpc_nonce(
        req.headers(),
        query.nixl_nonce,
        WRITE_FILE_NIXL_PATH,
        INTERNODE_OPERATION_PUT_FILE_STREAM,
        INTERNODE_TRANSPORT_BACKEND_RDMA,
    ) {
        return refuse(StatusCode::FORBIDDEN, "nonce_rejected", error);
    }
    let Some(server) = nixl_server() else {
        return reply(StatusCode::SERVICE_UNAVAILABLE, "nixl transport is not running on this node");
    };
    let metadata = match req.headers().get(NIXL_METADATA_HEADER).map(HeaderValue::to_str) {
        Some(Ok(value)) => Some(value),
        Some(Err(_)) => return refuse(StatusCode::BAD_REQUEST, "bad_metadata_header", "not ascii"),
        None => None,
    };
    if let Some(encoded) = metadata {
        let raw = match base64_simd::STANDARD.decode_to_vec(encoded) {
            Ok(raw) => raw,
            Err(error) => return refuse(StatusCode::BAD_REQUEST, "bad_metadata_encoding", error),
        };
        if sha256_hex(&raw) != query.nixl_md.to_ascii_lowercase() {
            return refuse(StatusCode::FORBIDDEN, "metadata_digest_mismatch", &query.nixl_agent);
        }
    }
    let mut tokens = Vec::new();
    for descriptor in query.nixl_buffers.split(',').filter(|descriptor| !descriptor.is_empty()) {
        match RdmaToken::from_headers(&query.nixl_agent, metadata, descriptor) {
            Ok(token) if token.buffer_len as usize <= server.chunk_bytes() => tokens.push(token),
            Ok(token) => return refuse(StatusCode::BAD_REQUEST, "buffer_too_large", token.buffer_len),
            Err(error) => return refuse(StatusCode::BAD_REQUEST, "bad_nixl_token", error),
        }
    }

    let disk = match find_local_disk_by_ref_for_rpc(&query.disk).await {
        Ok(Some(disk)) => disk,
        Ok(None) => return refuse(StatusCode::BAD_REQUEST, "disk_not_found", &query.disk),
        Err(_) => return reply(StatusCode::SERVICE_UNAVAILABLE, "background storage admission deferred"),
    };
    let opened = if query.append {
        disk.append_file(&query.volume, &query.path).await
    } else {
        disk.create_file("", &query.volume, &query.path, query.size).await
    };
    let mut file = match opened {
        Ok(file) => file,
        Err(error) => return refuse(StatusCode::INTERNAL_SERVER_ERROR, "open_failed", error),
    };

    let mut written: u64 = 0;
    for token in &tokens {
        let len = token.buffer_len as usize;
        let chunk = match server.reserve(STAGING_WAIT).await {
            Ok(chunk) => chunk,
            Err(error) => return refuse(StatusCode::SERVICE_UNAVAILABLE, "staging_exhausted", error),
        };
        let read_started = rustfs_io_metrics::put_stage_timer();
        let chunk = match server.read_from_client_owned(&query.path, token, 0, len, chunk).await {
            Ok(chunk) => chunk,
            Err(error) => return refuse(StatusCode::BAD_GATEWAY, "nixl_read_failed", error),
        };
        rustfs_io_metrics::record_put_object_stage_duration_from("nixl_write_file_read", read_started);
        let staged = match chunk.staged() {
            Ok(staged) => staged,
            Err(error) => return refuse(StatusCode::INTERNAL_SERVER_ERROR, "staging_view_failed", error),
        };
        let write_started = rustfs_io_metrics::put_stage_timer();
        if let Err(error) = file.write_all(staged).await {
            return refuse(StatusCode::INTERNAL_SERVER_ERROR, "write_failed", error);
        }
        rustfs_io_metrics::record_put_object_stage_duration_from("nixl_write_file_write", write_started);
        written += len as u64;
    }
    if !query.append && query.last && query.size > 0 && written != query.size as u64 {
        return refuse(
            StatusCode::INTERNAL_SERVER_ERROR,
            "size_mismatch",
            format!("expected {} bytes, received {written}", query.size),
        );
    }
    let finish_started = rustfs_io_metrics::put_stage_timer();
    if let Err(error) = finish_segment_file(&mut file).await {
        return refuse(StatusCode::INTERNAL_SERVER_ERROR, "finish_failed", error);
    }
    rustfs_io_metrics::record_put_object_stage_duration_from("nixl_write_file_finish", finish_started);
    let mut response = reply(StatusCode::OK, "");
    response.headers_mut().insert(NIXL_BYTES_HEADER, HeaderValue::from(written));
    response
}

#[cfg(test)]
mod tests {
    use super::finish_segment_file;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncWrite, AsyncWriteExt};

    /// Writes whole 4 KiB blocks as they fill and the remainder only on
    /// shutdown, like the direct-I/O disk writer.
    #[derive(Default)]
    struct TailOnShutdown {
        written: Vec<u8>,
        staged: Vec<u8>,
    }

    impl AsyncWrite for TailOnShutdown {
        fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
            self.staged.extend_from_slice(buf);
            let whole = self.staged.len() / 4096 * 4096;
            let block: Vec<u8> = self.staged.drain(..whole).collect();
            self.written.extend_from_slice(&block);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            let tail = std::mem::take(&mut self.staged);
            self.written.extend_from_slice(&tail);
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn finishing_a_segment_writes_the_sub_alignment_tail() {
        let data: Vec<u8> = (0..10_000u32).map(|i| i as u8).collect();
        let mut file = TailOnShutdown::default();
        file.write_all(&data).await.expect("write");
        file.flush().await.expect("flush");
        assert_eq!(file.written.len(), 8192, "a flush leaves the tail staged");

        finish_segment_file(&mut file).await.expect("finish");
        assert_eq!(file.written, data, "finishing the segment must write every byte");
    }
}
