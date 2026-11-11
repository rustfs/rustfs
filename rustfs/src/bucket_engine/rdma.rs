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

//! The RDMA transport available to bucket engines.
//!
//! Startup hands the NIXL server to this module once the S3-over-RDMA
//! service runs ([`set_server`]); engines and the S3 dispatch read it here, so
//! the engine layer does not depend on the object application layer. A
//! request carries the client's NIXL agent, optional agent metadata and
//! buffer in the `x-rustfs-nixl-*` headers, as for object buckets; the reply
//! reports the bytes moved in `x-amz-rdma-reply` and
//! `x-amz-rdma-bytes-transferred`.

use http::HeaderMap;
use rustfs_bucket_engine::RdmaTransfer;

/// Reply headers of an operation whose data moved over RDMA.
pub(crate) const RDMA_REPLY_HEADER: &str = "x-amz-rdma-reply";
pub(crate) const RDMA_BYTES_TRANSFERRED_HEADER: &str = "x-amz-rdma-bytes-transferred";

#[cfg(all(feature = "nixl", target_os = "linux"))]
mod imp {
    use std::sync::{Arc, OnceLock};

    use http::HeaderMap;
    use rustfs_bucket_engine::RdmaTransfer;
    use rustfs_nixl::{NIXL_AGENT_HEADER, NIXL_BUFFER_HEADER, NIXL_METADATA_HEADER, NixlServer, RdmaToken};

    static SERVER: OnceLock<Arc<NixlServer>> = OnceLock::new();

    pub(crate) fn set_server(server: Arc<NixlServer>) -> bool {
        SERVER.set(server).is_ok()
    }

    #[cfg(feature = "bucket-engines")]
    pub(crate) fn server() -> Option<Arc<NixlServer>> {
        SERVER.get().cloned()
    }

    /// The transfer a request asks for: `None` without NIXL headers, when the
    /// RDMA service is not running, or when the token is malformed (the
    /// request is then served over HTTP, as for object buckets).
    pub(crate) fn transfer_from_headers(headers: &HeaderMap) -> Option<RdmaTransfer> {
        SERVER.get()?;
        let header = |name: &str| headers.get(name).and_then(|value| value.to_str().ok());
        let agent = header(NIXL_AGENT_HEADER)?;
        let buffer = header(NIXL_BUFFER_HEADER)?;
        let transfer = RdmaTransfer {
            agent: agent.to_owned(),
            metadata: header(NIXL_METADATA_HEADER).unwrap_or_default().to_owned(),
            buffer: buffer.to_owned(),
        };
        token(&transfer).ok().map(|_| transfer)
    }

    /// Parses a transfer into the NIXL token the server moves data with.
    pub(crate) fn token(transfer: &RdmaTransfer) -> Result<RdmaToken, String> {
        let metadata = (!transfer.metadata.is_empty()).then_some(transfer.metadata.as_str());
        RdmaToken::from_headers(&transfer.agent, metadata, &transfer.buffer).map_err(|err| err.to_string())
    }
}

#[cfg(all(feature = "nixl", target_os = "linux"))]
pub(crate) use imp::set_server;
#[cfg(all(feature = "nixl", target_os = "linux", feature = "bucket-engines"))]
pub(crate) use imp::{server, token};

/// The RDMA transfer a request asks for, when the RDMA service runs.
pub(crate) fn transfer_from_headers(headers: &HeaderMap) -> Option<RdmaTransfer> {
    #[cfg(all(feature = "nixl", target_os = "linux"))]
    {
        imp::transfer_from_headers(headers)
    }
    #[cfg(not(all(feature = "nixl", target_os = "linux")))]
    {
        let _ = headers;
        None
    }
}

/// Size of the client buffer a transfer names (the object size of an RDMA
/// PUT). `None` when it cannot be parsed or RDMA is not built in.
pub(crate) fn buffer_len(transfer: &RdmaTransfer) -> Option<u64> {
    #[cfg(all(feature = "nixl", target_os = "linux"))]
    {
        imp::token(transfer).ok().map(|token| token.buffer_len)
    }
    #[cfg(not(all(feature = "nixl", target_os = "linux")))]
    {
        let _ = transfer;
        None
    }
}
