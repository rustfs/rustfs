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

//! Moving entry data over RDMA (NIXL) between the owner and the client.
//!
//! The owner always initiates: it pushes a read entry into the client's
//! buffer with RDMA writes and pulls a written value from the client's buffer
//! with RDMA reads, in staging-chunk pieces at increasing offsets, so data
//! crosses the network once even when another node received the request.

use std::time::Duration;

use rustfs_bucket_engine::{EngineError, EngineResult, RdmaTransfer};

use crate::bucket_engine::rdma::{server, token};

/// How long to wait for a free staging chunk before giving up.
const STAGING_TIMEOUT: Duration = Duration::from_secs(30);

fn transport_error(err: impl std::fmt::Display) -> EngineError {
    EngineError::Unavailable(format!("RDMA transfer failed: {err}"))
}

/// Writes `value` into the client's buffer; returns the bytes moved.
pub(super) async fn push(key: &str, transfer: &RdmaTransfer, value: &[u8]) -> EngineResult<u64> {
    let server = server().ok_or_else(|| EngineError::Unavailable("RDMA is not running".to_owned()))?;
    let token = token(transfer).map_err(EngineError::InvalidRequest)?;
    if value.len() as u64 > token.buffer_len {
        return Err(EngineError::InvalidRequest(format!(
            "client buffer of {} bytes is smaller than the {}-byte entry",
            token.buffer_len,
            value.len()
        )));
    }
    let chunk_bytes = server.chunk_bytes().max(1);
    let mut moved = 0u64;
    for piece in value.chunks(chunk_bytes) {
        let mut chunk = server.reserve(STAGING_TIMEOUT).await.map_err(transport_error)?;
        chunk.append(piece).map_err(transport_error)?;
        let (written, _chunk) = server
            .write_to_client_owned(key, &token, moved, chunk)
            .await
            .map_err(transport_error)?;
        moved += written;
    }
    Ok(moved)
}

/// Reads the whole client buffer (the value to store).
pub(super) async fn pull(key: &str, transfer: &RdmaTransfer, max_bytes: u64) -> EngineResult<Vec<u8>> {
    let server = server().ok_or_else(|| EngineError::Unavailable("RDMA is not running".to_owned()))?;
    let token = token(transfer).map_err(EngineError::InvalidRequest)?;
    if token.buffer_len > max_bytes {
        return Err(EngineError::InvalidRequest(format!(
            "value of {} bytes exceeds the {max_bytes}-byte limit",
            token.buffer_len
        )));
    }
    let len = usize::try_from(token.buffer_len).map_err(|_| EngineError::InvalidRequest("buffer length".to_owned()))?;
    let chunk_bytes = server.chunk_bytes().max(1);
    let mut value = Vec::with_capacity(len);
    while value.len() < len {
        let piece = chunk_bytes.min(len - value.len());
        let chunk = server.reserve(STAGING_TIMEOUT).await.map_err(transport_error)?;
        let chunk = server
            .read_from_client_owned(key, &token, value.len() as u64, piece, chunk)
            .await
            .map_err(transport_error)?;
        value.extend_from_slice(chunk.staged().map_err(transport_error)?);
    }
    Ok(value)
}
