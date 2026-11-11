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

//! S3-over-RDMA GET through NVIDIA cuObject (`--features cuobject`).
//!
//! A GET carrying `x-amz-rdma-token` is served by the ordinary read path up to
//! the final byte stream; those bytes are staged in registered host memory and
//! RDMA-written into the client's buffer instead of streamed over HTTP. One
//! cuObject server exists per configured RDMA address; requests round-robin
//! over them. Everything before the body (authentication, range resolution,
//! decryption, decompression, erasure decode, bitrot verification) is shared
//! with the HTTP path, so the data is identical; only the last hop differs.

use crate::app::storage_api::object_usecase::io::DynReader;
use crate::app::storage_api::object_usecase::runtime::get_global_endpoints_opt;
#[cfg(not(feature = "nixl"))]
use crate::app::storage_api::object_usecase::runtime::global_rustfs_port;
use bytes::Bytes;
use futures::Stream;
use http::HeaderMap;
use rustfs_config::{
    DEFAULT_ERASURE_SHARD_BYTES, DEFAULT_RUSTFS_CUOBJECT_GET_FANOUT, DEFAULT_RUSTFS_CUOBJECT_GET_FANOUT_BUDGET,
    DEFAULT_RUSTFS_CUOBJECT_GET_FANOUT_MIN_BYTES, DEFAULT_RUSTFS_CUOBJECT_INFLIGHT_WRITES,
    DEFAULT_RUSTFS_CUOBJECT_PUT_READ_AHEAD, DEFAULT_RUSTFS_CUOBJECT_SHARD_ARENA_BYTES, ENV_ERASURE_SHARD_BYTES,
    ENV_RUSTFS_CUOBJECT_GET_FANOUT, ENV_RUSTFS_CUOBJECT_GET_FANOUT_BUDGET, ENV_RUSTFS_CUOBJECT_GET_FANOUT_MIN_BYTES,
    ENV_RUSTFS_CUOBJECT_INFLIGHT_WRITES, ENV_RUSTFS_CUOBJECT_PUT_READ_AHEAD, ENV_RUSTFS_CUOBJECT_SHARD_ARENA_BYTES,
};
/// The transport this build moves bytes with: NIXL/UCX when the `nixl`
/// feature is on, otherwise NVIDIA cuObject. Both crates expose the same
/// surface, so the pipeline below is written once against these names.
#[cfg(feature = "nixl")]
pub(crate) mod transport {
    pub(crate) use rustfs_nixl::{
        ArenaRanges, MAX_RDMA_OP_BYTES, MAX_SG_RANGES, NIXL_AGENT_HEADER, NIXL_BUFFER_HEADER, NIXL_METADATA_HEADER,
        NixlError as Error, NixlServer as Server, RDMA_BYTES_TRANSFERRED_HEADER, RDMA_REPLY_HEADER, RdmaToken, RegisteredArena,
        StagingChunk, library_version,
    };
}
#[cfg(not(feature = "nixl"))]
pub(crate) mod transport {
    pub(crate) use rustfs_cuobject::{
        ArenaRanges, CuObjectError as Error, CuObjectServer as Server, MAX_RDMA_OP_BYTES, MAX_SG_RANGES,
        RDMA_BYTES_TRANSFERRED_HEADER, RDMA_REPLY_HEADER, RDMA_TOKEN_HEADER, RdmaToken, RegisteredArena, StagingChunk,
        library_version,
    };
}
use rustfs_ecstore::api::object::{RegisteredSlabArena, ShardBuf, ShardEmitter, install_shard_arena};
use rustfs_utils::{get_env_bool, get_env_opt_str, get_env_u64, get_env_usize};
use s3s::dto::StreamingBlob;
use s3s::stream::{ByteStream, RemainingLength};
use s3s::{S3Error, S3ErrorCode, S3Result};
use std::collections::VecDeque;
use std::io;
#[cfg(not(feature = "nixl"))]
use std::net::IpAddr;
use std::ops::Range;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWrite};
use tokio::task::JoinHandle;
use tracing::{info, warn};
use transport::{Error as CuObjectError, RdmaToken, RegisteredArena, Server as CuObjectServer, StagingChunk};
pub(crate) use transport::{RDMA_BYTES_TRANSFERRED_HEADER, RDMA_REPLY_HEADER};

const LOG_COMPONENT_OBJECT: &str = "object";
const LOG_SUBSYSTEM_CUOBJECT: &str = "cuobject";
const EVENT_CUOBJECT_SERVICE_STATE: &str = "cuobject_service_state";
const EVENT_CUOBJECT_GET_REJECTED: &str = "cuobject_get_rejected";
const STAGING_RESERVE_TIMEOUT: Duration = Duration::from_secs(30);

static SERVICE: OnceLock<Arc<CuObjectService>> = OnceLock::new();

pub(crate) struct CuObjectService {
    servers: Vec<Arc<CuObjectServer>>,
    inflight_writes: usize,
    /// Staging chunks read ahead from the client per RDMA PUT body.
    put_read_ahead: usize,
    /// Segments one GET is split into on the server; 1 keeps the body sequential.
    get_fanout: usize,
    /// Smallest segment worth its own producer, in bytes.
    get_fanout_min_bytes: u64,
    /// Segment producers the node may run across all its cuObject GETs.
    get_fanout_budget: usize,
    /// Permits for the extra producers (beyond each GET's first) within that budget.
    get_fanout_permits: Arc<tokio::sync::Semaphore>,
    /// cuObject GETs currently open on this node, for the fair share.
    active_gets: Arc<AtomicUsize>,
    /// Registered memory the erasure decoder reads shards into, so GETs can
    /// RDMA-write data shards straight from the shard buffers (zero-copy emit).
    shard_arena: Option<Arc<RegisteredArena>>,
    next: AtomicUsize,
}

impl CuObjectService {
    fn pick(&self) -> &Arc<CuObjectServer> {
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.servers.len();
        &self.servers[index]
    }
}

fn local_endpoint_hosts() -> Vec<String> {
    get_global_endpoints_opt()
        .map(|pools| {
            pools
                .as_ref()
                .iter()
                .flat_map(|pool| pool.endpoints.as_ref().iter())
                .filter(|endpoint| endpoint.is_local)
                .filter_map(|endpoint| endpoint.url.host_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(not(feature = "nixl"))]
async fn resolve_addrs() -> io::Result<Vec<IpAddr>> {
    use rustfs_config::ENV_RUSTFS_CUOBJECT_ADDRS;
    use std::collections::BTreeSet;

    let mut ips = BTreeSet::new();
    match get_env_opt_str(ENV_RUSTFS_CUOBJECT_ADDRS) {
        Some(raw) => {
            for entry in raw.split(',').map(str::trim).filter(|entry| !entry.is_empty()) {
                let ip: IpAddr = entry.parse().map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("{ENV_RUSTFS_CUOBJECT_ADDRS} entry {entry:?} is not an IP address"),
                    )
                })?;
                ips.insert(ip);
            }
        }
        None => {
            for host in local_endpoint_hosts() {
                let host = host.trim_matches(|c| c == '[' || c == ']').to_owned();
                if let Ok(ip) = host.parse::<IpAddr>() {
                    if !ip.is_unspecified() && !ip.is_loopback() {
                        ips.insert(ip);
                    }
                    continue;
                }
                for addr in tokio::net::lookup_host((host.as_str(), global_rustfs_port())).await? {
                    if !addr.ip().is_loopback() {
                        ips.insert(addr.ip());
                    }
                }
            }
        }
    }
    Ok(ips.into_iter().collect())
}

/// Whether the RDMA transport of this build is switched on.
#[cfg(not(feature = "nixl"))]
fn transport_enabled() -> bool {
    get_env_bool(rustfs_config::ENV_RUSTFS_CUOBJECT_ENABLE, rustfs_config::DEFAULT_RUSTFS_CUOBJECT_ENABLE)
}

#[cfg(feature = "nixl")]
fn transport_enabled() -> bool {
    get_env_bool(rustfs_config::ENV_RUSTFS_NIXL_ENABLE, rustfs_config::DEFAULT_RUSTFS_NIXL_ENABLE)
}

/// The transport servers of this node plus (control port, chunk bytes, pool
/// bytes) for the startup log; an empty list means the node has nowhere to
/// serve RDMA from and keeps serving over HTTP only.
#[cfg(not(feature = "nixl"))]
async fn build_servers() -> io::Result<(Vec<Arc<CuObjectServer>>, u16, usize, usize)> {
    use rustfs_config::{
        DEFAULT_RUSTFS_CUOBJECT_ASYNC_TRANSFERS, DEFAULT_RUSTFS_CUOBJECT_CHUNK_BYTES, DEFAULT_RUSTFS_CUOBJECT_NUM_DCIS,
        DEFAULT_RUSTFS_CUOBJECT_POOL_BYTES, DEFAULT_RUSTFS_CUOBJECT_PORT, ENV_RUSTFS_CUOBJECT_ASYNC_TRANSFERS,
        ENV_RUSTFS_CUOBJECT_CHUNK_BYTES, ENV_RUSTFS_CUOBJECT_NUM_DCIS, ENV_RUSTFS_CUOBJECT_POOL_BYTES, ENV_RUSTFS_CUOBJECT_PORT,
    };
    use rustfs_cuobject::CuObjectConfig;
    use rustfs_utils::get_env_u16;

    let config = CuObjectConfig {
        port: get_env_u16(ENV_RUSTFS_CUOBJECT_PORT, DEFAULT_RUSTFS_CUOBJECT_PORT),
        num_dcis: i32::try_from(get_env_usize(ENV_RUSTFS_CUOBJECT_NUM_DCIS, DEFAULT_RUSTFS_CUOBJECT_NUM_DCIS as usize))
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, format!("{ENV_RUSTFS_CUOBJECT_NUM_DCIS} is too large")))?,
        chunk_bytes: get_env_usize(ENV_RUSTFS_CUOBJECT_CHUNK_BYTES, DEFAULT_RUSTFS_CUOBJECT_CHUNK_BYTES),
        pool_bytes: get_env_usize(ENV_RUSTFS_CUOBJECT_POOL_BYTES, DEFAULT_RUSTFS_CUOBJECT_POOL_BYTES),
        async_transfers: get_env_bool(ENV_RUSTFS_CUOBJECT_ASYNC_TRANSFERS, DEFAULT_RUSTFS_CUOBJECT_ASYNC_TRANSFERS),
        ..CuObjectConfig::default()
    };
    if config.chunk_bytes == 0 || config.chunk_bytes > transport::MAX_RDMA_OP_BYTES || config.pool_bytes < config.chunk_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{ENV_RUSTFS_CUOBJECT_CHUNK_BYTES}/{ENV_RUSTFS_CUOBJECT_POOL_BYTES} are inconsistent"),
        ));
    }
    let addrs = resolve_addrs().await?;
    let mut servers = Vec::with_capacity(addrs.len());
    for addr in addrs {
        let config = config.clone();
        let server = tokio::task::spawn_blocking(move || CuObjectServer::new(addr, &config))
            .await
            .map_err(io::Error::other)?
            .map_err(|error| io::Error::other(format!("cuobject server on {addr}: {error}")))?;
        servers.push(server);
    }
    Ok((servers, config.port, config.chunk_bytes, config.pool_bytes))
}

/// One NIXL agent per node with the UCX backend; no address is needed, the
/// agent metadata carries every device.
#[cfg(feature = "nixl")]
async fn build_servers() -> io::Result<(Vec<Arc<CuObjectServer>>, u16, usize, usize)> {
    use rustfs_config::{
        DEFAULT_RUSTFS_NIXL_CHUNK_BYTES, DEFAULT_RUSTFS_NIXL_DEVICE_CHECK, DEFAULT_RUSTFS_NIXL_DEVICE_FLAP_WINDOW_MS,
        DEFAULT_RUSTFS_NIXL_DEVICE_MAX_LINK_DROPS, DEFAULT_RUSTFS_NIXL_MAX_INFLIGHT, DEFAULT_RUSTFS_NIXL_NUM_WORKERS,
        DEFAULT_RUSTFS_NIXL_POOL_BYTES, DEFAULT_RUSTFS_NIXL_TRANSFER_TIMEOUT_SECS, ENV_RUSTFS_NIXL_AGENT_NAME,
        ENV_RUSTFS_NIXL_CHUNK_BYTES, ENV_RUSTFS_NIXL_DEVICE_CHECK, ENV_RUSTFS_NIXL_DEVICE_FLAP_WINDOW_MS,
        ENV_RUSTFS_NIXL_DEVICE_MAX_LINK_DROPS, ENV_RUSTFS_NIXL_MAX_INFLIGHT, ENV_RUSTFS_NIXL_NUM_WORKERS,
        ENV_RUSTFS_NIXL_POOL_BYTES, ENV_RUSTFS_NIXL_TRANSFER_TIMEOUT_SECS, ENV_RUSTFS_NIXL_UCX_DEVICES,
    };
    use rustfs_nixl::NixlConfig;

    let agent_name = get_env_opt_str(ENV_RUSTFS_NIXL_AGENT_NAME).unwrap_or_else(|| {
        let host = local_endpoint_hosts().into_iter().next().unwrap_or_else(|| "node".to_owned());
        format!("rustfs-{}", host.trim_matches(|c| c == '[' || c == ']'))
    });
    let ucx_devices: Vec<String> = get_env_opt_str(ENV_RUSTFS_NIXL_UCX_DEVICES)
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|device| !device.is_empty())
        .map(str::to_owned)
        .collect();
    let ucx_devices = if get_env_bool(ENV_RUSTFS_NIXL_DEVICE_CHECK, DEFAULT_RUSTFS_NIXL_DEVICE_CHECK) {
        checked_nixl_devices(
            ucx_devices,
            get_env_u64(ENV_RUSTFS_NIXL_DEVICE_MAX_LINK_DROPS, DEFAULT_RUSTFS_NIXL_DEVICE_MAX_LINK_DROPS),
            Duration::from_millis(get_env_u64(
                ENV_RUSTFS_NIXL_DEVICE_FLAP_WINDOW_MS,
                DEFAULT_RUSTFS_NIXL_DEVICE_FLAP_WINDOW_MS,
            )),
        )
        .await?
    } else {
        ucx_devices
    };
    let config = NixlConfig {
        agent_name,
        ucx_devices,
        num_workers: u32::try_from(get_env_usize(ENV_RUSTFS_NIXL_NUM_WORKERS, DEFAULT_RUSTFS_NIXL_NUM_WORKERS as usize))
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, format!("{ENV_RUSTFS_NIXL_NUM_WORKERS} is too large")))?,
        chunk_bytes: get_env_usize(ENV_RUSTFS_NIXL_CHUNK_BYTES, DEFAULT_RUSTFS_NIXL_CHUNK_BYTES),
        pool_bytes: get_env_usize(ENV_RUSTFS_NIXL_POOL_BYTES, DEFAULT_RUSTFS_NIXL_POOL_BYTES),
        max_inflight: get_env_usize(ENV_RUSTFS_NIXL_MAX_INFLIGHT, DEFAULT_RUSTFS_NIXL_MAX_INFLIGHT),
        transfer_timeout: Duration::from_secs(
            get_env_u64(ENV_RUSTFS_NIXL_TRANSFER_TIMEOUT_SECS, DEFAULT_RUSTFS_NIXL_TRANSFER_TIMEOUT_SECS).max(1),
        ),
        ..NixlConfig::default()
    };
    if config.chunk_bytes == 0 || config.chunk_bytes > transport::MAX_RDMA_OP_BYTES || config.pool_bytes < config.chunk_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{ENV_RUSTFS_NIXL_CHUNK_BYTES}/{ENV_RUSTFS_NIXL_POOL_BYTES} are inconsistent"),
        ));
    }
    let (chunk_bytes, pool_bytes) = (config.chunk_bytes, config.pool_bytes);
    let server = tokio::task::spawn_blocking(move || CuObjectServer::new(&config))
        .await
        .map_err(io::Error::other)?
        .map_err(|error| io::Error::other(format!("nixl agent: {error}")))?;
    Ok((vec![server], 0, chunk_bytes, pool_bytes))
}

/// Starts the RDMA transport servers of this node when the transport is
/// enabled (`RUSTFS_CUOBJECT_ENABLE` or `RUSTFS_NIXL_ENABLE`).
///
/// Invalid settings and server construction failures fail startup; a node
/// without RDMA addresses logs and serves GETs over HTTP only.
pub(crate) async fn init_cuobject_service() -> io::Result<()> {
    if !transport_enabled() {
        return Ok(());
    }
    let inflight_writes = get_env_usize(ENV_RUSTFS_CUOBJECT_INFLIGHT_WRITES, DEFAULT_RUSTFS_CUOBJECT_INFLIGHT_WRITES).max(1);
    let put_read_ahead = get_env_usize(ENV_RUSTFS_CUOBJECT_PUT_READ_AHEAD, DEFAULT_RUSTFS_CUOBJECT_PUT_READ_AHEAD).max(1);
    let get_fanout = get_env_usize(ENV_RUSTFS_CUOBJECT_GET_FANOUT, DEFAULT_RUSTFS_CUOBJECT_GET_FANOUT).max(1);
    let get_fanout_min_bytes =
        get_env_u64(ENV_RUSTFS_CUOBJECT_GET_FANOUT_MIN_BYTES, DEFAULT_RUSTFS_CUOBJECT_GET_FANOUT_MIN_BYTES);
    let get_fanout_budget = get_env_usize(ENV_RUSTFS_CUOBJECT_GET_FANOUT_BUDGET, DEFAULT_RUSTFS_CUOBJECT_GET_FANOUT_BUDGET)
        .min(tokio::sync::Semaphore::MAX_PERMITS);
    let (servers, port, chunk_bytes, pool_bytes) = build_servers().await?;
    if servers.is_empty() {
        info!(
            event = EVENT_CUOBJECT_SERVICE_STATE,
            component = LOG_COMPONENT_OBJECT,
            subsystem = LOG_SUBSYSTEM_CUOBJECT,
            state = "skipped",
            reason = "no_rdma_address",
            "rdma transport service not started: no local endpoint host or RUSTFS_CUOBJECT_ADDRS"
        );
        return Ok(());
    }
    let shard_arena = install_cuobject_shard_arena(&servers).await?;
    #[cfg(feature = "nixl")]
    install_nixl_recv_pool(&servers)?;
    let (major, minor) = transport::library_version();
    info!(
        event = EVENT_CUOBJECT_SERVICE_STATE,
        component = LOG_COMPONENT_OBJECT,
        subsystem = LOG_SUBSYSTEM_CUOBJECT,
        state = "started",
        server_count = servers.len(),
        port,
        chunk_bytes,
        pool_bytes,
        inflight_writes,
        put_read_ahead,
        get_fanout,
        get_fanout_min_bytes,
        get_fanout_budget,
        shard_arena_bytes = shard_arena.as_ref().map_or(0, |arena| arena.len()),
        library = format_args!("{major}.{minor}"),
        "s3-over-rdma transport service started"
    );
    let _ = SERVICE.set(Arc::new(CuObjectService {
        servers,
        inflight_writes,
        put_read_ahead,
        get_fanout,
        get_fanout_min_bytes,
        get_fanout_budget,
        get_fanout_permits: Arc::new(tokio::sync::Semaphore::new(get_fanout_budget)),
        active_gets: Arc::new(AtomicUsize::new(0)),
        shard_arena,
        next: AtomicUsize::new(0),
    }));
    Ok(())
}

/// Registers the shard arena (`RUSTFS_CUOBJECT_SHARD_ARENA_BYTES`) with the
/// first server and installs it as the process-wide shard buffer source.
async fn install_cuobject_shard_arena(servers: &[Arc<CuObjectServer>]) -> io::Result<Option<Arc<RegisteredArena>>> {
    let bytes = get_env_usize(ENV_RUSTFS_CUOBJECT_SHARD_ARENA_BYTES, DEFAULT_RUSTFS_CUOBJECT_SHARD_ARENA_BYTES);
    let Some(server) = servers.first() else {
        return Ok(None);
    };
    if bytes == 0 {
        return Ok(None);
    }
    let block_len = match get_env_usize(ENV_ERASURE_SHARD_BYTES, DEFAULT_ERASURE_SHARD_BYTES) {
        0 => 4 * 1024 * 1024,
        shard => shard,
    };
    if bytes < block_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{ENV_RUSTFS_CUOBJECT_SHARD_ARENA_BYTES} is smaller than one shard block ({block_len} bytes)"),
        ));
    }
    let server = Arc::clone(server);
    // Registration pins the memory; keep it off the runtime threads.
    let arena = tokio::task::spawn_blocking(move || server.register_arena(bytes))
        .await
        .map_err(io::Error::other)?
        .map_err(|error| io::Error::other(format!("cuobject shard arena: {error}")))?;
    let slab = build_slab_arena(&arena, block_len);
    if !install_shard_arena(slab) {
        warn!(
            event = EVENT_CUOBJECT_SERVICE_STATE,
            component = LOG_COMPONENT_OBJECT,
            subsystem = LOG_SUBSYSTEM_CUOBJECT,
            state = "arena_already_installed",
            "cuobject shard arena not installed: another shard arena is active"
        );
        return Ok(None);
    }
    Ok(Some(arena))
}

/// Leaves out the RDMA devices that are down, slower than their peers, or flap
/// (a long drop history, or a drop during a short watch); `RUSTFS_NIXL_DEVICE_CHECK`. An empty list
/// means every device on the node. Fails when no device passes.
#[cfg(feature = "nixl")]
async fn checked_nixl_devices(candidates: Vec<String>, max_link_drops: u64, flap_window: Duration) -> io::Result<Vec<String>> {
    let check =
        tokio::task::spawn_blocking(move || rustfs_nixl::devices::check_system_devices(&candidates, max_link_drops, flap_window))
            .await
            .map_err(io::Error::other)?;
    let kept = check.kept.join(",");
    let excluded = check
        .excluded
        .iter()
        .map(|(device, reason)| format!("{device} ({reason})"))
        .collect::<Vec<_>>()
        .join(", ");
    if check.kept.is_empty() {
        tracing::error!(
            event = EVENT_CUOBJECT_SERVICE_STATE,
            component = LOG_COMPONENT_OBJECT,
            subsystem = LOG_SUBSYSTEM_CUOBJECT,
            state = "device_check",
            result = "failed",
            excluded = %excluded,
            "no healthy rdma device for the nixl agent"
        );
        return Err(io::Error::other(format!("no healthy RDMA device for NIXL; left out: {excluded}")));
    }
    if check.excluded.is_empty() {
        info!(
            event = EVENT_CUOBJECT_SERVICE_STATE,
            component = LOG_COMPONENT_OBJECT,
            subsystem = LOG_SUBSYSTEM_CUOBJECT,
            state = "device_check",
            result = "ok",
            kept = %kept,
            "nixl rdma devices checked"
        );
    } else {
        warn!(
            event = EVENT_CUOBJECT_SERVICE_STATE,
            component = LOG_COMPONENT_OBJECT,
            subsystem = LOG_SUBSYSTEM_CUOBJECT,
            state = "device_check",
            result = "degraded",
            kept = %kept,
            excluded = %excluded,
            "nixl rdma devices left out"
        );
    }
    Ok(check.kept)
}

/// This node's NIXL agent, once the transport service is running.
#[cfg(feature = "nixl")]
pub(crate) fn nixl_server() -> Option<Arc<CuObjectServer>> {
    SERVICE.get()?.servers.first().cloned()
}

/// Receive buffers for internode NIXL reads: staging chunks of this node's agent.
#[cfg(feature = "nixl")]
struct NixlRecvPoolAdapter {
    server: Arc<CuObjectServer>,
    metadata_base64: String,
    metadata_sha256_hex: String,
}

#[cfg(feature = "nixl")]
struct NixlRecvChunk(StagingChunk);

#[cfg(feature = "nixl")]
impl rustfs_ecstore::api::rpc::nixl::NixlRecvBuffer for NixlRecvChunk {
    fn descriptor(&self) -> String {
        self.descriptor_for(self.0.capacity())
    }

    fn descriptor_for(&self, len: usize) -> String {
        format!("dram:0:{:#x}:{}", self.0.addr(), len.min(self.0.capacity()))
    }

    fn spare_mut(&mut self) -> &mut [u8] {
        // A reserved chunk starts empty, so its spare area is the whole chunk.
        self.0.spare_mut().unwrap_or(&mut [])
    }

    fn into_bytes(self: Box<Self>, len: usize) -> io::Result<bytes::Bytes> {
        let mut chunk = self.0;
        chunk.clear();
        chunk.advance(len).map_err(io::Error::other)?;
        Ok(bytes::Bytes::from_owner(chunk))
    }
}

#[cfg(feature = "nixl")]
impl rustfs_ecstore::api::rpc::nixl::NixlRecvPool for NixlRecvPoolAdapter {
    fn agent_name(&self) -> &str {
        self.server.name()
    }

    fn metadata_base64(&self) -> &str {
        &self.metadata_base64
    }

    fn metadata_sha256_hex(&self) -> &str {
        &self.metadata_sha256_hex
    }

    fn chunk_bytes(&self) -> usize {
        self.server.chunk_bytes()
    }

    fn reserve(
        &self,
    ) -> futures::future::BoxFuture<'static, io::Result<Box<dyn rustfs_ecstore::api::rpc::nixl::NixlRecvBuffer>>> {
        let server = Arc::clone(&self.server);
        Box::pin(async move {
            let chunk = server.reserve(STAGING_RESERVE_TIMEOUT).await.map_err(io::Error::other)?;
            Ok(Box::new(NixlRecvChunk(chunk)) as Box<dyn rustfs_ecstore::api::rpc::nixl::NixlRecvBuffer>)
        })
    }
}

/// Lets internode reads use this node's agent and staging pool
/// (`RUSTFS_INTERNODE_DATA_TRANSPORT=nixl`).
#[cfg(feature = "nixl")]
fn install_nixl_recv_pool(servers: &[Arc<CuObjectServer>]) -> io::Result<()> {
    let Some(server) = servers.first() else {
        return Ok(());
    };
    let metadata = server
        .local_metadata()
        .map_err(|error| io::Error::other(format!("nixl agent metadata: {error}")))?;
    let adapter = NixlRecvPoolAdapter {
        server: Arc::clone(server),
        metadata_base64: base64_simd::STANDARD.encode_to_string(&metadata),
        metadata_sha256_hex: crate::storage::rpc::nixl_read::sha256_hex(&metadata),
    };
    rustfs_ecstore::api::rpc::nixl::install_nixl_recv_pool(Arc::new(adapter));
    Ok(())
}

/// Wraps the registered memory as a block arena. The arena owns a clone of
/// the registration, so the memory outlives every block handed out.
#[allow(unsafe_code)]
fn build_slab_arena(arena: &Arc<RegisteredArena>, block_len: usize) -> Arc<RegisteredSlabArena> {
    // SAFETY: `as_mut_ptr` addresses `len` bytes of host memory owned by the
    // registration, which the slab keeps alive through the boxed clone; the
    // slab hands out disjoint blocks of it.
    unsafe { RegisteredSlabArena::new(arena.as_mut_ptr(), arena.len(), block_len, arena.id(), Box::new(Arc::clone(arena))) }
}

/// The cuObject token: one opaque header from the client library.
#[cfg(not(feature = "nixl"))]
fn parse_rdma_token(headers: &HeaderMap) -> Option<Result<RdmaToken, CuObjectError>> {
    let raw = headers.get(transport::RDMA_TOKEN_HEADER)?.to_str().ok()?;
    Some(RdmaToken::parse(raw))
}

/// The NIXL token: agent name, optional agent metadata and the client buffer.
#[cfg(feature = "nixl")]
fn parse_rdma_token(headers: &HeaderMap) -> Option<Result<RdmaToken, CuObjectError>> {
    let agent = headers.get(transport::NIXL_AGENT_HEADER)?.to_str().ok()?;
    let buffer = headers.get(transport::NIXL_BUFFER_HEADER)?.to_str().ok()?;
    let metadata = headers
        .get(transport::NIXL_METADATA_HEADER)
        .and_then(|value| value.to_str().ok());
    Some(RdmaToken::from_headers(agent, metadata, buffer))
}

/// The parsed RDMA token of a GET that must be served over RDMA.
///
/// Returns `None` when the header is absent, the service is not running, or
/// the token is malformed; in the last two cases the response carries no
/// `x-amz-rdma-reply`, which the client reads as "declined".
pub(crate) fn rdma_token_for_request(headers: &HeaderMap) -> Option<RdmaToken> {
    SERVICE.get()?;
    match parse_rdma_token(headers)? {
        Ok(token) => Some(token),
        Err(error) => {
            warn!(
                event = EVENT_CUOBJECT_GET_REJECTED,
                component = LOG_COMPONENT_OBJECT,
                subsystem = LOG_SUBSYSTEM_CUOBJECT,
                result = "declined",
                reason = "token_malformed",
                error = %error,
                "cuobject get declined"
            );
            None
        }
    }
}

fn transfer_error(context: &str, error: CuObjectError) -> S3Error {
    S3Error::with_message(S3ErrorCode::InternalError, format!("rdma get {context}: {error}"))
}

/// Pipelines RDMA writes behind the decode stream: up to `inflight` staging
/// chunks are filled and written concurrently, writes complete in issue order,
/// and the first failure aborts the GET.
struct WritePipeline {
    server: Arc<CuObjectServer>,
    object: String,
    token: RdmaToken,
    free: Vec<StagingChunk>,
    writes: VecDeque<JoinHandle<Result<(u64, StagingChunk), CuObjectError>>>,
    issued: u64,
    moved: u64,
}

impl WritePipeline {
    async fn new(server: Arc<CuObjectServer>, object: String, token: RdmaToken, inflight: usize) -> S3Result<Self> {
        // The first chunk must be reserved; extra chunks are best effort so a
        // busy pool degrades to a shallower pipeline instead of failing the GET.
        let mut free = vec![
            server
                .reserve(STAGING_RESERVE_TIMEOUT)
                .await
                .map_err(|error| transfer_error("staging", error))?,
        ];
        while free.len() < inflight {
            match server.reserve(Duration::ZERO).await {
                Ok(chunk) => free.push(chunk),
                Err(_) => break,
            }
        }
        Ok(Self {
            server,
            object,
            token,
            free,
            writes: VecDeque::new(),
            issued: 0,
            moved: 0,
        })
    }

    /// Returns an empty chunk, waiting for the oldest write when none is free.
    async fn take(&mut self) -> S3Result<StagingChunk> {
        if self.free.is_empty() {
            self.reap_oldest().await?;
        }
        Ok(self.free.pop().expect("a chunk is free after reaping"))
    }

    /// Starts the RDMA write of `chunk` at the next client offset.
    fn submit(&mut self, chunk: StagingChunk) {
        if chunk.is_empty() {
            self.free.push(chunk);
            return;
        }
        let len = chunk.len() as u64;
        let offset = self.issued;
        self.issued += len;
        let server = Arc::clone(&self.server);
        let object = self.object.clone();
        let token = self.token.clone();
        // The blocking write owns the chunk until the DMA completes, so neither a
        // cancelled task nor an aborted pipeline can return the slot early.
        self.writes.push_back(tokio::spawn(
            async move { server.write_to_client_owned(&object, &token, offset, chunk).await },
        ));
    }

    async fn reap_oldest(&mut self) -> S3Result<()> {
        let Some(handle) = self.writes.pop_front() else {
            return Ok(());
        };
        let (written, mut chunk) = handle
            .await
            .map_err(|error| S3Error::with_message(S3ErrorCode::InternalError, format!("rdma get write task: {error}")))?
            .map_err(|error| transfer_error("write", error))?;
        self.moved += written;
        chunk.clear();
        self.free.push(chunk);
        Ok(())
    }

    /// Waits for every outstanding write; returns the bytes moved.
    async fn finish(mut self) -> S3Result<u64> {
        while !self.writes.is_empty() {
            self.reap_oldest().await?;
        }
        Ok(self.moved)
    }

    /// Waits for the outstanding writes on error. Their DMAs cannot be
    /// cancelled, and each chunk is released only when its write returns, so
    /// draining here is what keeps a staging slot from being reused while the
    /// NIC still reads it; results are ignored because the GET already failed.
    async fn abort(&mut self) {
        for handle in self.writes.drain(..) {
            let _ = handle.await;
        }
    }
}

/// Streams the response into staging chunks, submitting each full chunk to the
/// write pipeline so decoding keeps running while earlier chunks are written.
async fn fill_pipeline(
    pipeline: &mut WritePipeline,
    stream: DynReader,
    buffered_body: Option<Bytes>,
    expected: u64,
) -> S3Result<()> {
    let mut chunk = pipeline.take().await?;
    match buffered_body {
        Some(bytes) => {
            let mut rest = &bytes[..];
            while !rest.is_empty() {
                let take = rest.len().min(chunk.capacity() - chunk.len());
                chunk
                    .append(&rest[..take])
                    .map_err(|error| transfer_error("staging", error))?;
                rest = &rest[take..];
                if chunk.is_full() {
                    pipeline.submit(chunk);
                    chunk = pipeline.take().await?;
                }
            }
        }
        None => {
            let mut stream = stream;
            // Stop at the committed response length: readers may be unbounded.
            let mut remaining = expected;
            while remaining > 0 {
                let spare = chunk.spare_mut().map_err(|error| transfer_error("staging", error))?;
                let want = spare.len().min(usize::try_from(remaining).unwrap_or(usize::MAX));
                let read = stream
                    .read(&mut spare[..want])
                    .await
                    .map_err(|error| transfer_error("read", CuObjectError::Source(error)))?;
                if read == 0 {
                    break;
                }
                chunk.advance(read).map_err(|error| transfer_error("staging", error))?;
                remaining -= read as u64;
                if chunk.is_full() {
                    pipeline.submit(chunk);
                    chunk = pipeline.take().await?;
                }
            }
        }
    }
    pipeline.submit(chunk);
    Ok(())
}

/// Moves the response bytes into the client's buffer; returns the payload byte count.
pub(crate) async fn serve_get_over_rdma(
    stream: DynReader,
    buffered_body: Option<Bytes>,
    response_content_length: i64,
    bucket: &str,
    key: &str,
    token: &RdmaToken,
) -> S3Result<u64> {
    let service = SERVICE
        .get()
        .ok_or_else(|| S3Error::with_message(S3ErrorCode::NotImplemented, "rdma get is not enabled"))?;
    let expected = u64::try_from(response_content_length).unwrap_or(0);
    if expected > token.buffer_len {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidRange,
            format!(
                "rdma token buffer ({} bytes) is smaller than the response ({expected} bytes)",
                token.buffer_len
            ),
        ));
    }
    let server = Arc::clone(service.pick());
    let started = Instant::now();
    let object = format!("{bucket}/{key}");
    let mut pipeline = WritePipeline::new(server, object, token.clone(), service.inflight_writes).await?;
    let moved = match fill_pipeline(&mut pipeline, stream, buffered_body, expected).await {
        Ok(()) => pipeline.finish().await?,
        Err(error) => {
            pipeline.abort().await;
            return Err(error);
        }
    };
    if moved != expected {
        return Err(S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("rdma get moved {moved} of {expected} bytes"),
        ));
    }
    rustfs_io_metrics::record_get_object_stage_duration(
        super::GET_OBJECT_STAGE_PATH_S3_HANDLER,
        "rdma_transfer",
        started.elapsed().as_secs_f64(),
    );
    Ok(moved)
}

/// Request body of a cuObject PUT: the object bytes are RDMA-read from the
/// client's buffer into staging, one chunk ahead of the consumer, and handed
/// to the normal PUT pipeline as an ordinary byte stream.
struct RdmaPutBody {
    inner: Pin<Box<dyn Stream<Item = Result<Bytes, s3s::StdError>> + Send + Sync + 'static>>,
    remaining: usize,
}

impl Stream for RdmaPutBody {
    type Item = Result<Bytes, s3s::StdError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let next = self.inner.as_mut().poll_next(cx);
        if let Poll::Ready(Some(Ok(bytes))) = &next {
            self.remaining = self.remaining.saturating_sub(bytes.len());
        }
        next
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ByteStream for RdmaPutBody {
    fn remaining_length(&self) -> RemainingLength {
        RemainingLength::new_exact(self.remaining)
    }
}

type StagedRead = JoinHandle<Result<StagingChunk, CuObjectError>>;

/// The staged bytes of `chunk`: with `zero_copy`, a `Bytes` that owns the
/// chunk (it returns to the pool when the last clone drops), otherwise a copy.
#[cfg(feature = "nixl")]
fn staged_bytes(chunk: StagingChunk, zero_copy: bool) -> io::Result<Bytes> {
    if zero_copy {
        return Ok(Bytes::from_owner(chunk));
    }
    Ok(Bytes::copy_from_slice(chunk.staged().map_err(io::Error::other)?))
}

/// The staged bytes of `chunk`, copied (the cuObject chunk is not a byte owner).
#[cfg(not(feature = "nixl"))]
fn staged_bytes(chunk: StagingChunk, _zero_copy: bool) -> io::Result<Bytes> {
    Ok(Bytes::copy_from_slice(chunk.staged().map_err(io::Error::other)?))
}

fn spawn_staged_read(server: &Arc<CuObjectServer>, object: &str, token: &RdmaToken, offset: u64, len: usize) -> StagedRead {
    let server = Arc::clone(server);
    let object = object.to_owned();
    let token = token.clone();
    tokio::spawn(async move {
        let chunk = server.reserve(STAGING_RESERVE_TIMEOUT).await?;
        server.read_from_client_owned(&object, &token, offset, len, chunk).await
    })
}

/// Builds the streaming body for a PUT whose bytes live in the client's RDMA
/// buffer (`x-amz-rdma-token`, empty HTTP body). The object length is the
/// token's buffer length. Returns the body and that length.
///
/// With `zero_copy` the body's chunks are the staging chunks themselves, so a
/// consumer that keeps a chunk keeps it out of the pool: only callers whose
/// body is consumed and dropped with the request (UploadPart) may set it.
/// PutObject may hand its body to the object data cache, so it copies.
pub(crate) fn rdma_put_body(bucket: &str, key: &str, token: &RdmaToken, zero_copy: bool) -> S3Result<(StreamingBlob, u64)> {
    let service = SERVICE
        .get()
        .ok_or_else(|| S3Error::with_message(S3ErrorCode::NotImplemented, "rdma put is not enabled"))?;
    let server = Arc::clone(service.pick());
    let read_ahead = service.put_read_ahead;
    let total = token.buffer_len;
    let chunk_bytes = server.chunk_bytes();
    let object = format!("{bucket}/{key}");
    let token = token.clone();
    // Keep `read_ahead` RDMA reads in flight, so the reads of the following
    // chunks overlap the consumer's work on this one. The first ones start
    // now, overlapping the request's setup.
    let fill = move |server: &Arc<CuObjectServer>,
                     object: &str,
                     token: &RdmaToken,
                     pending: &mut VecDeque<StagedRead>,
                     next_offset: &mut u64| {
        while pending.len() < read_ahead && *next_offset < total {
            let len = usize::try_from((total - *next_offset).min(chunk_bytes as u64)).unwrap_or(chunk_bytes);
            pending.push_back(spawn_staged_read(server, object, token, *next_offset, len));
            *next_offset += len as u64;
        }
    };
    let mut pending = VecDeque::with_capacity(read_ahead);
    let mut next_offset = 0u64;
    fill(&server, &object, &token, &mut pending, &mut next_offset);
    // State: (reads in flight in offset order, offset of the next read to start).
    let stream = futures::stream::try_unfold((pending, next_offset), move |(mut pending, mut next_offset)| {
        let server = Arc::clone(&server);
        let object = object.clone();
        let token = token.clone();
        async move {
            fill(&server, &object, &token, &mut pending, &mut next_offset);
            let Some(read) = pending.pop_front() else {
                return Ok(None);
            };
            let wait_started = rustfs_io_metrics::put_stage_timer();
            let chunk = read
                .await
                .map_err(|error| io::Error::other(format!("rdma put read task: {error}")))?
                .map_err(|error| io::Error::other(format!("rdma put read: {error}")))?;
            rustfs_io_metrics::record_put_object_stage_duration_from("rdma_put_body_wait", wait_started);
            let bytes = staged_bytes(chunk, zero_copy)?;
            Ok::<_, io::Error>(Some((bytes, (pending, next_offset))))
        }
    });
    let stream = futures::TryStreamExt::map_err(stream, |error: io::Error| -> s3s::StdError { Box::new(error) });
    let body = RdmaPutBody {
        inner: Box::pin(stream),
        remaining: usize::try_from(total)
            .map_err(|_| S3Error::with_message(S3ErrorCode::EntityTooLarge, "rdma put too large"))?,
    };
    Ok((StreamingBlob::new(body), total))
}

/// State shared by the segment writers of one RDMA GET: the server, the
/// client token, the GET's staging chunks (free list), and the segments that
/// were opened, so the handler can finish or abort all of them.
struct SinkShared {
    server: Arc<CuObjectServer>,
    object: String,
    token: RdmaToken,
    inflight_writes: usize,
    free: std::sync::Mutex<Vec<StagingChunk>>,
    /// Segments waiting for a chunk; woken one at a time as chunks return.
    waiters: std::sync::Mutex<Vec<std::task::Waker>>,
    segments: std::sync::Mutex<Vec<Arc<std::sync::Mutex<SegmentState>>>>,
    /// The shard arena registered with this sink's server, if any: data shards
    /// read into it are RDMA-written in place instead of through staging.
    arena: Option<Arc<RegisteredArena>>,
}

impl SinkShared {
    fn lock_free(&self) -> std::sync::MutexGuard<'_, Vec<StagingChunk>> {
        self.free.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn lock_segments(&self) -> std::sync::MutexGuard<'_, Vec<Arc<std::sync::Mutex<SegmentState>>>> {
        self.segments.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Opens a segment whose first byte lands at `remote_offset` in the client buffer.
    fn open_segment(self: &Arc<Self>, remote_offset: u64) -> StagingSink {
        let state = Arc::new(std::sync::Mutex::new(SegmentState {
            current: None,
            writes: VecDeque::new(),
            issued: remote_offset,
            moved: 0,
            pending: Vec::new(),
            pending_ranges: Vec::new(),
        }));
        self.lock_segments().push(Arc::clone(&state));
        StagingSink {
            shared: Arc::clone(self),
            state,
        }
    }

    /// Opens a segment as the writer handed to the storage layer: the staging
    /// stream, plus the zero-copy shard emitter when the arena is available.
    fn open_segment_writer(
        self: &Arc<Self>,
        remote_offset: u64,
    ) -> crate::app::storage_api::object_usecase::StorageGetObjectSinkWriter {
        let sink = self.open_segment(remote_offset);
        match &self.arena {
            Some(arena) => {
                let emitter = StagingShardEmitter {
                    shared: Arc::clone(self),
                    state: Arc::clone(&sink.state),
                    arena: Arc::clone(arena),
                };
                crate::app::storage_api::object_usecase::StorageGetObjectSinkWriter::with_emitter(
                    Box::new(sink),
                    Box::new(emitter),
                )
            }
            None => crate::app::storage_api::object_usecase::StorageGetObjectSinkWriter::new(Box::new(sink)),
        }
    }

    /// Reserves up to `count` more chunks from the pool without waiting.
    fn top_up(&self, count: usize) {
        let mut free = self.lock_free();
        for _ in 0..count {
            match self.server.try_reserve() {
                Some(chunk) => free.push(chunk),
                None => break,
            }
        }
    }

    /// Returns a chunk to the GET's free list and wakes one waiting segment.
    fn release(&self, mut chunk: StagingChunk) {
        chunk.clear();
        self.lock_free().push(chunk);
        let waker = self.waiters.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).pop();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn park(&self, cx: &Context<'_>) {
        let mut waiters = self.waiters.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if !waiters.iter().any(|waker| waker.will_wake(cx.waker())) {
            waiters.push(cx.waker().clone());
        }
    }
}

/// An in-flight RDMA write of a segment: bytes moved and, for a staged write,
/// the chunk to return to the GET's free list.
type SegmentWrite = JoinHandle<Result<(u64, Option<StagingChunk>), CuObjectError>>;

/// One segment of the body: the erasure decoder writes decoded bytes straight
/// into cuObject staging chunks (no duplex pipe, no consumer copy), full chunks
/// are RDMA-written on their own tasks at the segment's running offset.
struct SegmentState {
    current: Option<StagingChunk>,
    /// In-flight RDMA writes in submission order; a staging write returns its
    /// chunk, a zero-copy shard write returns none (the shard buffer went back
    /// to the arena when the write task dropped it).
    writes: VecDeque<SegmentWrite>,
    /// Client-buffer offset of the next byte this segment stages.
    issued: u64,
    /// Bytes confirmed written by reaped writes.
    moved: u64,
    /// Data shards taken by the emitter and not yet submitted: one
    /// scatter-gather RDMA write carries a whole stripe's worth.
    pending: Vec<ShardBuf>,
    /// `(arena offset, len)` of each pending shard's bytes, in stream order.
    pending_ranges: Vec<(usize, usize)>,
}

impl SegmentState {
    /// Submits the pending shards as one scatter-gather write.
    fn submit_pending(&mut self, shared: &Arc<SinkShared>) {
        if self.pending.is_empty() {
            return;
        }
        let Some(arena) = shared.arena.as_ref() else {
            return;
        };
        let shards = std::mem::take(&mut self.pending);
        let ranges = std::mem::take(&mut self.pending_ranges);
        let len: usize = ranges.iter().map(|(_, len)| *len).sum();
        let remote_offset = self.issued;
        self.issued += len as u64;
        let server = Arc::clone(&shared.server);
        let object = shared.object.clone();
        let token = shared.token.clone();
        let source = transport::ArenaRanges {
            arena: Arc::clone(arena),
            ranges,
        };
        self.writes.push_back(tokio::spawn(async move {
            let started = std::time::Instant::now();
            let result = server
                .write_to_client_sg_owned(&object, &token, remote_offset, source, shards)
                .await
                .map(|(moved, _shards)| (moved, None));
            record_emit_write_latency(len, started.elapsed(), "write_zero_copy");
            result
        }));
    }

    fn submit_current(&mut self, shared: &Arc<SinkShared>) {
        let Some(chunk) = self.current.take() else {
            return;
        };
        if chunk.is_empty() {
            shared.lock_free().push(chunk);
            return;
        }
        let len = chunk.len() as u64;
        let offset = self.issued;
        self.issued += len;
        let server = Arc::clone(&shared.server);
        let object = shared.object.clone();
        let token = shared.token.clone();
        self.writes.push_back(tokio::spawn(async move {
            let started = std::time::Instant::now();
            let result = server
                .write_to_client_owned(&object, &token, offset, chunk)
                .await
                .map(|(moved, chunk)| (moved, Some(chunk)));
            record_emit_write_latency(len as usize, started.elapsed(), "write_staging");
            result
        }));
    }

    /// Polls the oldest write; on completion its chunk returns to the GET's free list.
    fn poll_reap_oldest(&mut self, shared: &SinkShared, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let Some(handle) = self.writes.front_mut() else {
            return Poll::Ready(Ok(()));
        };
        match Pin::new(handle).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(joined) => {
                self.writes.pop_front();
                let (written, chunk) = joined
                    .map_err(|error| io::Error::other(format!("rdma get write task: {error}")))?
                    .map_err(|error| io::Error::other(format!("rdma get write: {error}")))?;
                self.moved += written;
                if let Some(chunk) = chunk {
                    shared.release(chunk);
                }
                Poll::Ready(Ok(()))
            }
        }
    }
}

/// The writer half handed to the storage layer through the GET options (one
/// per segment; the sequential sink is the segment at offset 0).
/// Stage path and buckets for one zero-copy RDMA write (submit to completion),
/// recorded through the GET stage summary so the distribution under load is
/// visible next to the decode-loop stages.
const EMIT_STAGE_PATH: &str = "cuobject_emit";

fn record_emit_write_latency(bytes: usize, elapsed: std::time::Duration, path: &'static str) {
    let secs = elapsed.as_secs_f64();
    // One stage per emit path, so the summary shows how many writes went out
    // straight from the shard arena and how many through the staging copy.
    rustfs_io_metrics::record_get_object_stage_duration(EMIT_STAGE_PATH, path, secs);
    let bucket = match elapsed.as_millis() {
        0..=1 => "write_le_1ms",
        2..=5 => "write_le_5ms",
        6..=20 => "write_le_20ms",
        21..=50 => "write_le_50ms",
        _ => "write_gt_50ms",
    };
    rustfs_io_metrics::record_get_object_stage_duration(EMIT_STAGE_PATH, "write_total", secs);
    rustfs_io_metrics::record_get_object_stage_duration(EMIT_STAGE_PATH, bucket, secs);
    // Bytes per write as a stage whose "duration" is the byte count, so the
    // summary shows the average write size next to its latency.
    rustfs_io_metrics::record_get_object_stage_duration(EMIT_STAGE_PATH, "write_bytes", bytes as f64);
}

pub(crate) struct StagingSink {
    shared: Arc<SinkShared>,
    state: Arc<std::sync::Mutex<SegmentState>>,
}

/// The handler's half: finishes or aborts the transfer after the producer ends.
pub(crate) struct StagingSinkHandle {
    sink: crate::app::storage_api::object_usecase::StorageGetObjectSink,
    shared: Arc<SinkShared>,
    /// Node-wide fan-out permits held for the GET's lifetime (one per extra segment).
    _fanout_permits: Option<tokio::sync::OwnedSemaphorePermit>,
    /// Counts this GET as active for the fair share until the handle is dropped.
    _active: ActiveGetGuard,
}

/// Decrements the node's active cuObject GET count on drop.
struct ActiveGetGuard(Arc<AtomicUsize>);

impl Drop for ActiveGetGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

fn lock_segment(state: &std::sync::Mutex<SegmentState>) -> std::sync::MutexGuard<'_, SegmentState> {
    state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl AsyncWrite for StagingSink {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let shared = Arc::clone(&self.shared);
        let mut state = lock_segment(&self.state);
        // Shards taken before these bytes go out first so the order is kept.
        state.submit_pending(&shared);
        if state.current.is_none() {
            let mut chunk = shared.lock_free().pop();
            if chunk.is_none() {
                // Nothing free for this GET: reap this segment's oldest write, or
                // grow from the pool when nothing is in flight. If the pool is
                // exhausted too, park until another segment returns a chunk.
                match state.poll_reap_oldest(&shared, cx) {
                    Poll::Ready(Ok(())) => {}
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                    Poll::Pending => return Poll::Pending,
                }
                chunk = shared.lock_free().pop().or_else(|| shared.server.try_reserve());
                if chunk.is_none() {
                    shared.park(cx);
                    // A chunk may have come back between the pop and the park.
                    chunk = shared.lock_free().pop();
                    if chunk.is_none() {
                        return Poll::Pending;
                    }
                }
            }
            state.current = chunk;
        }
        let chunk = state.current.as_mut().expect("a current chunk was just installed");
        let spare = chunk.spare_mut().map_err(io::Error::other)?;
        let n = spare.len().min(buf.len());
        spare[..n].copy_from_slice(&buf[..n]);
        chunk.advance(n).map_err(io::Error::other)?;
        if chunk.is_full() {
            state.submit_current(&shared);
        }
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Batched zero-copy writes in flight per segment before the emitter applies
/// backpressure (one write per stripe, or per `MAX_SG_RANGES` shards).
const MAX_INFLIGHT_SHARD_WRITES: usize = 4;

/// The zero-copy half of a segment writer: takes data shards that live in the
/// registered shard arena and RDMA-writes them in place, in stream order with
/// the bytes staged through the segment's [`StagingSink`].
struct StagingShardEmitter {
    shared: Arc<SinkShared>,
    state: Arc<std::sync::Mutex<SegmentState>>,
    arena: Arc<RegisteredArena>,
}

impl ShardEmitter for StagingShardEmitter {
    fn poll_emit_shard(
        &mut self,
        cx: &mut Context<'_>,
        shard: &mut Option<ShardBuf>,
        range: Range<usize>,
    ) -> Poll<io::Result<bool>> {
        let Some(block) = shard.as_ref().and_then(ShardBuf::arena_block) else {
            return Poll::Ready(Ok(false));
        };
        if block.registration_id() != self.arena.id() || range.end > block.len() || range.start > range.end {
            return Poll::Ready(Ok(false));
        }
        let len = range.end - range.start;
        if len == 0 {
            shard.take();
            return Poll::Ready(Ok(true));
        }
        let local_offset = block.offset() + range.start;
        let shared = Arc::clone(&self.shared);
        let mut state = lock_segment(&self.state);
        if state.pending.len() >= transport::MAX_SG_RANGES {
            // The batch is full: submit it (with backpressure) before taking more.
            while state.writes.len() >= MAX_INFLIGHT_SHARD_WRITES {
                match state.poll_reap_oldest(&shared, cx) {
                    Poll::Ready(Ok(())) => {}
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                    Poll::Pending => return Poll::Pending,
                }
            }
            state.submit_pending(&shared);
        }
        // Bytes staged so far go out first so the stream order is kept.
        state.submit_current(&shared);
        let taken = shard.take().expect("shard checked above");
        state.pending.push(taken);
        state.pending_ranges.push((local_offset, len));
        Poll::Ready(Ok(true))
    }

    fn poll_flush_shards(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let shared = Arc::clone(&self.shared);
        let mut state = lock_segment(&self.state);
        if state.pending.is_empty() {
            return Poll::Ready(Ok(()));
        }
        while state.writes.len() >= MAX_INFLIGHT_SHARD_WRITES {
            match state.poll_reap_oldest(&shared, cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => return Poll::Pending,
            }
        }
        state.submit_pending(&shared);
        Poll::Ready(Ok(()))
    }
}

impl StagingSinkHandle {
    /// Reserves the staging chunks and builds the sink for one GET. With the
    /// fan-out enabled the sink also offers segment writers; each opened
    /// segment tops the GET's chunk list up (without waiting) so it can keep
    /// its own writes in flight.
    pub(crate) async fn new(bucket: &str, key: &str, token: &RdmaToken) -> S3Result<Self> {
        let service = SERVICE
            .get()
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::NotImplemented, "rdma get is not enabled"))?;
        let server = Arc::clone(service.pick());
        let server_for_arena = Arc::clone(&server);
        let mut free = vec![
            server
                .reserve(STAGING_RESERVE_TIMEOUT)
                .await
                .map_err(|error| transfer_error("staging", error))?,
        ];
        while free.len() < service.inflight_writes {
            match server.reserve(Duration::ZERO).await {
                Ok(chunk) => free.push(chunk),
                Err(_) => break,
            }
        }
        let shared = Arc::new(SinkShared {
            server,
            object: format!("{bucket}/{key}"),
            token: token.clone(),
            inflight_writes: service.inflight_writes,
            free: std::sync::Mutex::new(free),
            waiters: std::sync::Mutex::new(Vec::new()),
            segments: std::sync::Mutex::new(Vec::new()),
            arena: service
                .shard_arena
                .as_ref()
                .filter(|arena| Arc::ptr_eq(arena.server(), &server_for_arena))
                .cloned(),
        });
        // The fan-out is bounded by what the pool can give this GET right now
        // (one segment's worth of chunks each), so under load GETs degrade to
        // fewer segments instead of all of them waiting on the same chunks.
        // Fair share of the node's producer budget among the GETs open right
        // now (this one included): 16 producers give one stream 8 segments,
        // four streams 4 each, eight streams 2, sixteen or more 1. Running
        // GETs keep their segments; the permits cap the total so a burst can
        // never exceed the budget, and nothing waits for a permit.
        let active = ActiveGetGuard(Arc::clone(&service.active_gets));
        let streams = active.0.fetch_add(1, Ordering::Relaxed) + 1;
        let fair_share = (service.get_fanout_budget / streams).clamp(1, service.get_fanout);
        let affordable_segments = 1 + shared.server.available_chunks() / shared.inflight_writes.max(1);
        let mut max_segments = fair_share.min(affordable_segments);
        let mut fanout_permits = None;
        if max_segments >= 2 {
            let wanted = u32::try_from(max_segments - 1).unwrap_or(u32::MAX);
            let available = u32::try_from(service.get_fanout_permits.available_permits()).unwrap_or(u32::MAX);
            let extra = wanted.min(available);
            fanout_permits = (extra > 0)
                .then(|| Arc::clone(&service.get_fanout_permits).try_acquire_many_owned(extra).ok())
                .flatten();
            max_segments = 1 + fanout_permits.as_ref().map_or(0, |permits| permits.num_permits());
        }
        let writer = shared.open_segment_writer(0);
        let sink = if max_segments >= 2 {
            let factory_shared = Arc::clone(&shared);
            let factory: Arc<
                dyn Fn(u64, u64) -> crate::app::storage_api::object_usecase::StorageGetObjectSinkWriter + Send + Sync,
            > = Arc::new(move |offset, _len| {
                factory_shared.top_up(factory_shared.inflight_writes);
                factory_shared.open_segment_writer(offset)
            });
            crate::app::storage_api::object_usecase::StorageGetObjectSink::with_segments(
                writer,
                crate::app::storage_api::object_usecase::StorageGetObjectSinkSegments {
                    factory,
                    max_segments,
                    min_segment_bytes: service.get_fanout_min_bytes,
                },
            )
        } else {
            crate::app::storage_api::object_usecase::StorageGetObjectSink::new(writer)
        };
        Ok(Self {
            sink,
            shared,
            _fanout_permits: fanout_permits,
            _active: active,
        })
    }

    /// The option value to hand to the storage layer.
    pub(crate) fn sink(&self) -> crate::app::storage_api::object_usecase::StorageGetObjectSink {
        self.sink.clone()
    }

    /// Whether the storage layer streamed the body through the sink.
    pub(crate) fn was_used(&self) -> bool {
        self.sink.is_taken()
    }

    fn segments(&self) -> Vec<Arc<std::sync::Mutex<SegmentState>>> {
        self.shared.lock_segments().clone()
    }

    /// After the producer finished: writes every segment's last partial chunk
    /// and waits for every write; returns the bytes moved into the client buffer.
    pub(crate) async fn finish(self) -> S3Result<u64> {
        let segments = self.segments();
        let mut moved = 0;
        for state in &segments {
            let mut state = lock_segment(state);
            state.submit_pending(&self.shared);
            state.submit_current(&self.shared);
            moved += state.moved;
        }
        for state in &segments {
            loop {
                let handle = lock_segment(state).writes.pop_front();
                let Some(handle) = handle else { break };
                let (written, _chunk) = handle
                    .await
                    .map_err(|error| S3Error::with_message(S3ErrorCode::InternalError, format!("rdma get write task: {error}")))?
                    .map_err(|error| transfer_error("write", error))?;
                moved += written;
            }
        }
        Ok(moved)
    }

    /// After a failed GET: waits for the in-flight writes so their chunks are
    /// only released once the DMAs are over.
    pub(crate) async fn abort(self) {
        for state in self.segments() {
            loop {
                let handle = lock_segment(&state).writes.pop_front();
                let Some(handle) = handle else { break };
                let _ = handle.await;
            }
        }
    }
}

/// Request-extension slot carrying the sink handle from request setup to the
/// body build; taken exactly once.
#[derive(Clone)]
pub(crate) struct RdmaSinkSlot(Arc<std::sync::Mutex<Option<StagingSinkHandle>>>);

impl RdmaSinkSlot {
    pub(crate) fn new(handle: StagingSinkHandle) -> Self {
        Self(Arc::new(std::sync::Mutex::new(Some(handle))))
    }

    pub(crate) fn take(&self) -> Option<StagingSinkHandle> {
        self.0.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).take()
    }
}

/// Completes a GET whose body went through the sink: drains the reader (which
/// only yields the producer's completion), then flushes the sink and reports
/// the bytes moved. A producer error aborts the sink first.
pub(crate) async fn finish_get_through_sink(
    mut stream: DynReader,
    response_content_length: i64,
    handle: StagingSinkHandle,
) -> S3Result<u64> {
    let started = Instant::now();
    let mut scratch = [0u8; 4096];
    loop {
        match stream.read(&mut scratch).await {
            Ok(0) => break,
            Ok(_) => {
                // The pipe's write end was dropped unused, so nothing arrives here.
            }
            Err(error) => {
                handle.abort().await;
                return Err(transfer_error("read", CuObjectError::Source(error)));
            }
        }
    }
    let moved = handle.finish().await?;
    let expected = u64::try_from(response_content_length).unwrap_or(0);
    if moved != expected {
        return Err(S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("rdma get moved {moved} of {expected} bytes"),
        ));
    }
    rustfs_io_metrics::record_get_object_stage_duration(
        super::GET_OBJECT_STAGE_PATH_S3_HANDLER,
        "rdma_transfer",
        started.elapsed().as_secs_f64(),
    );
    Ok(moved)
}
