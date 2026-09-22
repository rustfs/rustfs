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

//! Where an [`EtagReader`](crate::EtagReader) sends the bytes it sees (Port B).
//!
//! MD5 cannot be vectorised within one message, only across independent ones. One upload is one
//! message, so the only way to use SIMD for ETags is to hash *different requests* together. That
//! needs a component that outlives any single request: the reader hands its plaintext to a
//! [`Md5StreamSink`] and asks for the digest at EOF, and never learns how the hashing was done.
//!
//! | sink | hashing happens | used when |
//! |---|---|---|
//! | [`InlineSink`] | in `poll_read`, on the caller's task, one stream at a time | default; nothing installed |
//! | [`LaneServer`] | on a dedicated thread, all in-flight uploads advanced together through a [`Md5LaneEngine`] | installed at startup |
//!
//! The ETag value, its encoding, `Content-MD5` verification and the moment the ETag becomes
//! available (EOF) are identical for both.

use bytes::{Bytes, BytesMut};
use rustfs_utils::hash::Md5Stream;
use rustfs_utils::hash_lanes::Md5LaneEngine;
use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

/// Opens one [`Md5Lane`] per upload.
pub trait Md5StreamSink: Send + Sync + 'static {
    /// Start hashing a new, empty message.
    fn open(&self) -> Box<dyn Md5Lane>;
}

/// The MD5 of one upload, fed in order. Dropping a lane before it finishes abandons the message.
pub trait Md5Lane: Send + Sync {
    /// Back-pressure: `Pending` while too many of this lane's bytes are still waiting to be
    /// hashed. Polled before the reader pulls more data from its source.
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<()>;

    /// Append bytes. Never refuses: the reader has already handed these bytes to its caller.
    fn write(&mut self, data: &[u8]);

    /// No more bytes; resolve to the digest of everything written. Poll until `Ready`.
    fn poll_finish(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<[u8; 16]>>;
}

/// Hash in place, one stream at a time. Never pends, never fails.
#[derive(Clone, Copy, Debug, Default)]
pub struct InlineSink;

struct InlineLane(Option<Md5Stream>);

impl Md5StreamSink for InlineSink {
    fn open(&self) -> Box<dyn Md5Lane> {
        Box::new(InlineLane(Some(Md5Stream::new())))
    }
}

impl Md5Lane for InlineLane {
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<()> {
        Poll::Ready(())
    }

    fn write(&mut self, data: &[u8]) {
        if let Some(md5) = self.0.as_mut() {
            md5.update(data);
        }
    }

    fn poll_finish(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<[u8; 16]>> {
        Poll::Ready(Ok(self.0.take().unwrap_or_default().finalize()))
    }
}

static SINK: OnceLock<Arc<dyn Md5StreamSink>> = OnceLock::new();

/// Install the process-wide sink. Call once from the composition root, before serving traffic.
/// Returns `false` (and changes nothing) if a sink was already installed.
pub fn install_md5_stream_sink(sink: Arc<dyn Md5StreamSink>) -> bool {
    SINK.set(sink).is_ok()
}

/// The installed sink, or [`InlineSink`] when none was installed.
pub fn md5_stream_sink() -> Arc<dyn Md5StreamSink> {
    SINK.get().cloned().unwrap_or_else(|| Arc::new(InlineSink))
}

/// `on` installs a [`LaneServer`]; anything else (default) keeps ETag hashing inline.
pub const ENV_MD5_LANE_SERVER: &str = "RUSTFS_MD5_LANE_SERVER";
/// Hashing threads, see [`LaneServerConfig::threads`]. Default 1.
pub const ENV_MD5_LANE_THREADS: &str = "RUSTFS_MD5_LANE_THREADS";
/// See [`LaneServerConfig::min_streams`]. Default 4.
pub const ENV_MD5_LANE_MIN_STREAMS: &str = "RUSTFS_MD5_LANE_MIN_STREAMS";

/// What [`install_lane_server_from_env`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LaneServerInstall {
    /// Not requested; ETags are hashed inline.
    Disabled,
    /// Requested, but this build has no multi-stream engine (`hash-md5-simd`), or the threads
    /// could not be spawned. ETags are hashed inline.
    Unavailable,
    /// A sink was already installed; nothing changed.
    AlreadyInstalled,
    /// Installed.
    Installed {
        /// Engine name, e.g. `simd-avx2-fused`.
        engine: &'static str,
        /// Hashing threads.
        threads: usize,
        /// Uploads in flight below which new uploads stay inline.
        min_streams: usize,
    },
}

/// Composition-root entry: install a [`LaneServer`] as the process-wide sink if
/// [`ENV_MD5_LANE_SERVER`] asks for one. Off by default: with few concurrent uploads and idle
/// cores, inline hashing on the request tasks has the higher wall-clock throughput; the server
/// pays off in CPU per byte (roughly a third at 8+ uploads) and in throughput at 16+.
pub fn install_lane_server_from_env() -> LaneServerInstall {
    if !rustfs_utils::get_env_str(ENV_MD5_LANE_SERVER, "off").eq_ignore_ascii_case("on") {
        return LaneServerInstall::Disabled;
    }
    #[cfg(not(feature = "hash-md5-simd"))]
    {
        LaneServerInstall::Unavailable
    }
    #[cfg(feature = "hash-md5-simd")]
    {
        let defaults = LaneServerConfig::default();
        let config = LaneServerConfig {
            threads: rustfs_utils::get_env_usize(ENV_MD5_LANE_THREADS, defaults.threads).clamp(1, 16),
            min_streams: rustfs_utils::get_env_usize(ENV_MD5_LANE_MIN_STREAMS, defaults.min_streams).max(1),
            ..defaults
        };
        let Ok(server) = LaneServer::start(rustfs_utils::hash_lanes::SimdLaneEngine::new(), config) else {
            return LaneServerInstall::Unavailable;
        };
        let engine = server.engine_name();
        if install_md5_stream_sink(server) {
            LaneServerInstall::Installed {
                engine,
                threads: config.threads,
                min_streams: config.min_streams,
            }
        } else {
            LaneServerInstall::AlreadyInstalled
        }
    }
}

/// Tuning of a [`LaneServer`].
#[derive(Clone, Copy, Debug)]
pub struct LaneServerConfig {
    /// A lane hands bytes to the server once it has this many (always a whole number of MD5
    /// blocks, so lanes stay block-aligned and can share registers), and at EOF.
    pub submit_bytes: usize,
    /// A lane stops its reader while more than this many of its bytes await hashing.
    pub max_in_flight_bytes: usize,
    /// With fewer uploads than this in flight, a new upload is hashed inline instead: below the
    /// engine's SIMD threshold the server only adds a copy and a thread hop.
    pub min_streams: usize,
    /// When fewer lanes have data than make a SIMD batch, wait this long for more to arrive.
    pub batch_wait: Duration,
    /// Hashing threads. One thread is one core's worth of MD5 (several GiB/s once its registers
    /// are full); uploads wait on it through back-pressure, so size this above the ingest rate
    /// the node must sustain. Threads are filled one after the other (see [`LaneServer::open`]),
    /// so extra threads cost nothing until the load needs them.
    pub threads: usize,
}

impl Default for LaneServerConfig {
    fn default() -> Self {
        Self {
            submit_bytes: 256 * 1024,
            max_in_flight_bytes: 1024 * 1024,
            min_streams: 4,
            batch_wait: Duration::from_micros(200),
            threads: 1,
        }
    }
}

/// Counters of a [`LaneServer`], for metrics and tests.
#[derive(Debug, Default)]
pub struct LaneServerStats {
    /// Uploads hashed by the server.
    pub lanes_opened: AtomicU64,
    /// Uploads hashed inline because too few were in flight, or the server had stopped.
    pub inline_fallbacks: AtomicU64,
    /// Calls into the engine.
    pub batches: AtomicU64,
    /// Sum over all batches of the number of lanes in the batch.
    pub batched_lanes: AtomicU64,
}

/// Hashes all in-flight uploads together on dedicated threads.
///
/// Dropping the last `Arc<LaneServer>` does not stop uploads already in flight from finishing;
/// a thread exits once its lanes and the server itself are gone.
pub struct LaneServer {
    workers: Vec<WorkerHandle>,
    config: LaneServerConfig,
    next_id: AtomicU64,
    active: Arc<AtomicUsize>,
    alive: Arc<AtomicBool>,
    stats: Arc<LaneServerStats>,
    engine_name: &'static str,
    /// Lanes one thread takes before the next thread is used.
    pack_target: usize,
}

struct WorkerHandle {
    tx: Mutex<Sender<Msg>>,
    lanes: Arc<AtomicUsize>,
}

enum Msg {
    Open(u64, Arc<Shared>),
    Chunk(u64, Bytes),
    Finish(u64),
    Release(u64),
}

/// What a lane and the server thread share about one upload.
#[derive(Default)]
struct Shared {
    in_flight: AtomicUsize,
    outcome: Mutex<Option<Result<[u8; 16], ()>>>,
    waker: Mutex<Option<Waker>>,
}

impl Shared {
    fn wake(&self) {
        if let Some(waker) = self.waker.lock().unwrap_or_else(|e| e.into_inner()).take() {
            waker.wake();
        }
    }

    fn register(&self, cx: &Context<'_>) {
        *self.waker.lock().unwrap_or_else(|e| e.into_inner()) = Some(cx.waker().clone());
    }

    fn settle(&self, outcome: Result<[u8; 16], ()>) {
        *self.outcome.lock().unwrap_or_else(|e| e.into_inner()) = Some(outcome);
        self.wake();
    }
}

impl LaneServer {
    /// Spawn the hashing threads.
    pub fn start<E: Md5LaneEngine + Clone>(engine: E, config: LaneServerConfig) -> io::Result<Arc<Self>> {
        let alive = Arc::new(AtomicBool::new(true));
        let stats = Arc::new(LaneServerStats::default());
        let engine_name = engine.name();
        let pack_target = engine.preferred_batch().max(1) * 2;
        let mut workers = Vec::new();
        for index in 0..config.threads.max(1) {
            let (tx, rx) = mpsc::channel();
            let worker = Worker {
                engine: engine.clone(),
                rx,
                lanes: HashMap::new(),
                batch_wait: config.batch_wait,
                alive: alive.clone(),
                stats: stats.clone(),
            };
            std::thread::Builder::new()
                .name(format!("rustfs-md5-lanes-{index}"))
                .spawn(move || worker.run())?;
            workers.push(WorkerHandle {
                tx: Mutex::new(tx),
                lanes: Arc::new(AtomicUsize::new(0)),
            });
        }
        Ok(Arc::new(Self {
            workers,
            config,
            next_id: AtomicU64::new(0),
            active: Arc::new(AtomicUsize::new(0)),
            alive,
            stats,
            engine_name,
            pack_target,
        }))
    }

    /// Counters since start.
    pub fn stats(&self) -> &LaneServerStats {
        &self.stats
    }

    /// Name of the engine behind this server (diagnostic).
    pub fn engine_name(&self) -> &'static str {
        self.engine_name
    }
}

impl Md5StreamSink for LaneServer {
    fn open(&self) -> Box<dyn Md5Lane> {
        let guard = ActiveGuard::enter(&self.active);
        if guard.count < self.config.min_streams || !self.alive.load(Ordering::Acquire) {
            self.stats.inline_fallbacks.fetch_add(1, Ordering::Relaxed);
            return Box::new(CountedInlineLane {
                lane: InlineLane(Some(Md5Stream::new())),
                _guard: guard,
            });
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let shared = Arc::new(Shared::default());
        // Pack, then spill: splitting a few uploads across threads would leave every thread's
        // registers half empty. Only when all threads are packed does the load get balanced.
        let lanes = |w: &&WorkerHandle| w.lanes.load(Ordering::Acquire);
        let packing = self.workers.iter().find(|w| lanes(w) < self.pack_target);
        let Some(worker) = packing.or_else(|| self.workers.iter().min_by_key(lanes)) else {
            return Box::new(CountedInlineLane {
                lane: InlineLane(Some(Md5Stream::new())),
                _guard: guard,
            });
        };
        let tx = worker.tx.lock().unwrap_or_else(|e| e.into_inner()).clone();
        if tx.send(Msg::Open(id, shared.clone())).is_err() {
            self.stats.inline_fallbacks.fetch_add(1, Ordering::Relaxed);
            return Box::new(CountedInlineLane {
                lane: InlineLane(Some(Md5Stream::new())),
                _guard: guard,
            });
        }
        self.stats.lanes_opened.fetch_add(1, Ordering::Relaxed);
        Box::new(ServerLane {
            id,
            tx,
            shared,
            pending: BytesMut::new(),
            submit_bytes: self.config.submit_bytes.max(64) & !63,
            max_in_flight: self.config.max_in_flight_bytes,
            finish_sent: false,
            done: false,
            _guard: guard,
            _worker_lane: ActiveGuard::enter(&worker.lanes),
        })
    }
}

/// Counts an upload as in flight for as long as its lane exists, inline or not.
struct ActiveGuard {
    active: Arc<AtomicUsize>,
    /// In-flight uploads including this one, at the time it was opened.
    count: usize,
}

impl ActiveGuard {
    fn enter(active: &Arc<AtomicUsize>) -> Self {
        let count = active.fetch_add(1, Ordering::AcqRel) + 1;
        Self {
            active: active.clone(),
            count,
        }
    }
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
    }
}

struct CountedInlineLane {
    lane: InlineLane,
    _guard: ActiveGuard,
}

impl Md5Lane for CountedInlineLane {
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.lane.poll_ready(cx)
    }
    fn write(&mut self, data: &[u8]) {
        self.lane.write(data);
    }
    fn poll_finish(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<[u8; 16]>> {
        self.lane.poll_finish(cx)
    }
}

/// The request-side half of an upload hashed by the server.
struct ServerLane {
    id: u64,
    tx: Sender<Msg>,
    shared: Arc<Shared>,
    pending: BytesMut,
    submit_bytes: usize,
    max_in_flight: usize,
    finish_sent: bool,
    done: bool,
    _guard: ActiveGuard,
    _worker_lane: ActiveGuard,
}

impl ServerLane {
    fn submit(&mut self, len: usize) {
        if len == 0 {
            return;
        }
        let chunk = self.pending.split_to(len).freeze();
        self.shared.in_flight.fetch_add(len, Ordering::AcqRel);
        if self.tx.send(Msg::Chunk(self.id, chunk)).is_err() {
            self.shared.settle(Err(()));
        }
    }
}

impl Md5Lane for ServerLane {
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.shared.in_flight.load(Ordering::Acquire) <= self.max_in_flight {
            return Poll::Ready(());
        }
        self.shared.register(cx);
        // The server may have caught up between the check and the registration.
        if self.shared.in_flight.load(Ordering::Acquire) <= self.max_in_flight || server_lost(&self.shared) {
            return Poll::Ready(());
        }
        Poll::Pending
    }

    fn write(&mut self, data: &[u8]) {
        self.pending.extend_from_slice(data);
        if self.pending.len() >= self.submit_bytes {
            self.submit(self.pending.len() & !63);
        }
    }

    fn poll_finish(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<[u8; 16]>> {
        if !self.finish_sent {
            self.finish_sent = true;
            self.submit(self.pending.len());
            if self.tx.send(Msg::Finish(self.id)).is_err() {
                self.shared.settle(Err(()));
            }
        }
        let take = |shared: &Shared| shared.outcome.lock().unwrap_or_else(|e| e.into_inner()).take();
        let mut outcome = take(&self.shared);
        if outcome.is_none() {
            self.shared.register(cx);
            outcome = take(&self.shared);
        }
        match outcome {
            None => Poll::Pending,
            Some(result) => {
                self.done = true;
                Poll::Ready(result.map_err(|()| io::Error::other("md5 lane server stopped before the digest was ready")))
            }
        }
    }
}

fn server_lost(shared: &Shared) -> bool {
    matches!(*shared.outcome.lock().unwrap_or_else(|e| e.into_inner()), Some(Err(())))
}

impl Drop for ServerLane {
    fn drop(&mut self) {
        if !self.done {
            let _ = self.tx.send(Msg::Release(self.id));
        }
    }
}

/// The server-side half of an upload.
struct Lane<S> {
    state: S,
    shared: Arc<Shared>,
    queue: std::collections::VecDeque<Bytes>,
    /// The chunk being hashed in the current batch.
    current: Option<Bytes>,
    finishing: bool,
}

struct Worker<E: Md5LaneEngine> {
    engine: E,
    rx: Receiver<Msg>,
    lanes: HashMap<u64, Lane<E::State>>,
    batch_wait: Duration,
    alive: Arc<AtomicBool>,
    stats: Arc<LaneServerStats>,
}

impl<E: Md5LaneEngine> Drop for Worker<E> {
    /// Runs on normal exit and on panic alike: nobody may be left waiting for a digest.
    fn drop(&mut self) {
        self.alive.store(false, Ordering::Release);
        for lane in self.lanes.values() {
            lane.shared.settle(Err(()));
        }
    }
}

impl<E: Md5LaneEngine> Worker<E> {
    fn run(mut self) {
        loop {
            let ready = self.lanes.values().filter(|lane| !lane.queue.is_empty()).count();
            let first = if ready == 0 {
                match self.rx.recv() {
                    Ok(msg) => Some(msg),
                    Err(_) => return,
                }
            } else if ready < self.engine.preferred_batch() && !self.batch_wait.is_zero() {
                match self.rx.recv_timeout(self.batch_wait) {
                    Ok(msg) => Some(msg),
                    Err(RecvTimeoutError::Timeout) => None,
                    Err(RecvTimeoutError::Disconnected) => return,
                }
            } else {
                None
            };
            if let Some(msg) = first {
                self.accept(msg);
            }
            loop {
                match self.rx.try_recv() {
                    Ok(msg) => self.accept(msg),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return,
                }
            }
            self.hash_one_round();
            self.finish_drained_lanes();
        }
    }

    fn accept(&mut self, msg: Msg) {
        match msg {
            Msg::Open(id, shared) => {
                let lane = Lane {
                    state: self.engine.start(),
                    shared,
                    queue: Default::default(),
                    current: None,
                    finishing: false,
                };
                self.lanes.insert(id, lane);
            }
            Msg::Chunk(id, bytes) => {
                if let Some(lane) = self.lanes.get_mut(&id) {
                    lane.queue.push_back(bytes);
                }
            }
            Msg::Finish(id) => {
                if let Some(lane) = self.lanes.get_mut(&id) {
                    lane.finishing = true;
                }
            }
            Msg::Release(id) => {
                self.lanes.remove(&id);
            }
        }
    }

    /// Every lane with queued data contributes its oldest chunk; the engine advances them together.
    fn hash_one_round(&mut self) {
        let mut in_batch = 0u64;
        for lane in self.lanes.values_mut() {
            lane.current = lane.queue.pop_front();
            in_batch += u64::from(lane.current.is_some());
        }
        if in_batch == 0 {
            return;
        }
        self.engine.update_many(self.lanes.values_mut().filter_map(|lane| {
            let Lane { state, current, .. } = lane;
            current.as_deref().map(|chunk| (state, chunk))
        }));
        self.stats.batches.fetch_add(1, Ordering::Relaxed);
        self.stats.batched_lanes.fetch_add(in_batch, Ordering::Relaxed);
        for lane in self.lanes.values_mut() {
            if let Some(chunk) = lane.current.take() {
                lane.shared.in_flight.fetch_sub(chunk.len(), Ordering::AcqRel);
                lane.shared.wake();
            }
        }
    }

    fn finish_drained_lanes(&mut self) {
        let done: Vec<u64> = self
            .lanes
            .iter()
            .filter(|(_, lane)| lane.finishing && lane.queue.is_empty())
            .map(|(id, _)| *id)
            .collect();
        for id in done {
            if let Some(lane) = self.lanes.remove(&id) {
                lane.shared.settle(Ok(self.engine.finalize(lane.state)));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BadDigest, EtagReader, EtagResolvable};
    use md5::{Digest, Md5};
    use rustfs_utils::hash_lanes::InlineLaneEngine;
    use std::pin::Pin;
    use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};

    fn md5_hex(data: &[u8]) -> String {
        hex_simd::encode_to_string(Md5::digest(data), hex_simd::AsciiCase::Lower)
    }

    fn payload(seed: u64, len: usize) -> Vec<u8> {
        let mut x = seed | 1;
        (0..len)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect()
    }

    /// Hands out at most `step` bytes per read, like a network body.
    struct Dribble {
        data: Vec<u8>,
        at: usize,
        step: usize,
    }

    impl AsyncRead for Dribble {
        fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            let n = self.step.min(buf.remaining()).min(self.data.len() - self.at);
            let at = self.at;
            buf.put_slice(&self.data[at..at + n]);
            self.at += n;
            Poll::Ready(Ok(()))
        }
    }

    fn always_server(config: LaneServerConfig) -> LaneServerConfig {
        LaneServerConfig {
            min_streams: 1,
            ..config
        }
    }

    async fn upload_many(server: &Arc<LaneServer>, sizes: &[usize], step: usize) {
        let mut tasks = Vec::new();
        for (i, &len) in sizes.iter().enumerate() {
            let data = payload(i as u64 + 1, len);
            let want = md5_hex(&data);
            let mut reader = EtagReader::with_sink(
                Dribble {
                    data: data.clone(),
                    at: 0,
                    step,
                },
                None,
                server.as_ref(),
            );
            tasks.push(tokio::spawn(async move {
                assert_eq!(reader.try_resolve_etag(), None, "no etag before EOF");
                let mut out = Vec::new();
                reader.read_to_end(&mut out).await.expect("read");
                assert_eq!(out, data, "bytes pass through unchanged");
                assert_eq!(reader.try_resolve_etag(), Some(want), "stream {i} len {len}");
            }));
        }
        for task in tasks {
            task.await.expect("upload task");
        }
    }

    const SIZES: [usize; 24] = [
        0,
        1,
        55,
        56,
        63,
        64,
        65,
        4096,
        65_535,
        65_536,
        262_143,
        262_144,
        262_145,
        300_000,
        777_777,
        1_048_576,
        1_048_577,
        2_000_003,
        3,
        128,
        500_000,
        999_999,
        1_500_000,
        64 * 1000,
    ];

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_uploads_match_md5_inline_engine() {
        let server = LaneServer::start(InlineLaneEngine, always_server(Default::default())).unwrap();
        upload_many(&server, &SIZES, 40_000).await;
        assert_eq!(server.stats().lanes_opened.load(Ordering::Relaxed), SIZES.len() as u64);
        assert_eq!(server.stats().inline_fallbacks.load(Ordering::Relaxed), 0);
    }

    #[cfg(feature = "hash-md5-simd")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_uploads_match_md5_simd_engine() {
        let engine = rustfs_utils::hash_lanes::SimdLaneEngine::new();
        let server = LaneServer::start(engine, always_server(Default::default())).unwrap();
        upload_many(&server, &SIZES, 40_000).await;
        upload_many(&server, &[1_048_576; 32], 1 << 20).await;
        let stats = server.stats();
        let (batches, lanes) = (stats.batches.load(Ordering::Relaxed), stats.batched_lanes.load(Ordering::Relaxed));
        assert!(lanes > batches, "uploads were never hashed together: {lanes} lanes in {batches} batches");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tiny_limits_exercise_back_pressure() {
        let config = LaneServerConfig {
            submit_bytes: 64,
            max_in_flight_bytes: 128,
            batch_wait: Duration::ZERO,
            min_streams: 1,
            threads: 3,
        };
        let server = LaneServer::start(InlineLaneEngine, config).unwrap();
        upload_many(&server, &[0, 63, 64, 65, 10_000, 100_001, 250_000], 997).await;
    }

    #[tokio::test]
    async fn content_md5_is_verified_at_eof() {
        let server = LaneServer::start(InlineLaneEngine, always_server(Default::default())).unwrap();
        let data = payload(9, 300_000);
        let good = md5_hex(&data);

        let mut ok = EtagReader::with_sink(&data[..], Some(good.clone()), server.as_ref());
        ok.read_to_end(&mut Vec::new()).await.expect("matching Content-MD5");
        assert_eq!(ok.get_etag(), Some(good.clone()));

        let wrong = "0".repeat(32);
        let mut bad = EtagReader::with_sink(&data[..], Some(wrong.clone()), server.as_ref());
        let err = bad.read_to_end(&mut Vec::new()).await.expect_err("mismatching Content-MD5");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        let digest = err.get_ref().and_then(|e| e.downcast_ref::<BadDigest>()).expect("BadDigest");
        assert_eq!(
            (digest.expected_md5.as_str(), digest.calculated_md5.as_str()),
            (wrong.as_str(), good.as_str())
        );
    }

    #[tokio::test]
    async fn few_uploads_stay_inline_and_are_counted_while_alive() {
        let server = LaneServer::start(
            InlineLaneEngine,
            LaneServerConfig {
                min_streams: 3,
                ..Default::default()
            },
        )
        .unwrap();
        let data = payload(4, 100_000);
        let first = EtagReader::with_sink(&data[..], None, server.as_ref());
        let second = EtagReader::with_sink(&data[..], None, server.as_ref());
        let mut third = EtagReader::with_sink(&data[..], None, server.as_ref());
        let stats = server.stats();
        assert_eq!(
            (stats.inline_fallbacks.load(Ordering::Relaxed), stats.lanes_opened.load(Ordering::Relaxed)),
            (2, 1)
        );
        third.read_to_end(&mut Vec::new()).await.unwrap();
        assert_eq!(third.get_etag(), Some(md5_hex(&data)));
        drop((first, second, third));
        let mut alone = EtagReader::with_sink(&data[..], None, server.as_ref());
        assert_eq!(
            stats.inline_fallbacks.load(Ordering::Relaxed),
            3,
            "the count dropped when the readers did"
        );
        alone.read_to_end(&mut Vec::new()).await.unwrap();
        assert_eq!(alone.get_etag(), Some(md5_hex(&data)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn abandoned_uploads_do_not_disturb_the_rest() {
        let server = LaneServer::start(InlineLaneEngine, always_server(Default::default())).unwrap();
        for round in 0..20 {
            let data = payload(round, 600_000);
            let mut abandoned = EtagReader::with_sink(
                Dribble {
                    data: data.clone(),
                    at: 0,
                    step: 70_000,
                },
                None,
                server.as_ref(),
            );
            let mut partial = vec![0u8; 300_000];
            abandoned.read_exact(&mut partial).await.unwrap();
            drop(abandoned);
        }
        upload_many(&server, &[500_000; 8], 50_000).await;
    }

    /// An engine that dies on its first batch.
    #[derive(Clone)]
    struct PanickingEngine;

    impl Md5LaneEngine for PanickingEngine {
        type State = ();
        fn name(&self) -> &'static str {
            "panics"
        }
        fn preferred_batch(&self) -> usize {
            1
        }
        fn start(&self) {}
        fn update_many<'a, I>(&self, _streams: I)
        where
            I: IntoIterator<Item = (&'a mut (), &'a [u8])>,
        {
            panic!("engine failure injected by test");
        }
        fn finalize(&self, _state: ()) -> [u8; 16] {
            [0; 16]
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_dead_server_fails_in_flight_uploads_and_later_ones_go_inline() {
        let server = LaneServer::start(PanickingEngine, always_server(Default::default())).unwrap();
        let data = payload(5, 400_000);
        let mut doomed = EtagReader::with_sink(&data[..], None, server.as_ref());
        let err = tokio::time::timeout(Duration::from_secs(10), doomed.read_to_end(&mut Vec::new()))
            .await
            .expect("a dead server must not leave the upload hanging")
            .expect_err("no digest can be produced");
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert_eq!(doomed.try_resolve_etag(), None);

        let mut later = EtagReader::with_sink(&data[..], None, server.as_ref());
        later.read_to_end(&mut Vec::new()).await.expect("inline fallback");
        assert_eq!(later.get_etag(), Some(md5_hex(&data)));
    }

    /// Aggregate ETag throughput of N concurrent uploads read in 64 KiB pieces, inline vs the lane
    /// server. `cargo test -p rustfs-rio --release --features hash-md5-simd -- --ignored --nocapture etag_throughput`
    #[cfg(feature = "hash-md5-simd")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "benchmark, not a correctness test"]
    async fn etag_throughput() {
        const PER_UPLOAD: usize = 64 << 20;
        let data = Arc::new(payload(7, PER_UPLOAD));
        let want = md5_hex(&data);
        for threads in [1usize, 2] {
            let engine = rustfs_utils::hash_lanes::SimdLaneEngine::new();
            let server = LaneServer::start(
                engine,
                LaneServerConfig {
                    threads,
                    ..always_server(Default::default())
                },
            )
            .unwrap();
            println!("--- lane server threads = {threads}");
            println!(
                "engine={} tokio workers=4, {} MiB per upload, 64 KiB reads",
                server.engine_name(),
                PER_UPLOAD >> 20
            );
            println!(
                "{:>7} | {:>12} {:>12} | {:>12} {:>12} | {:>6} {:>8} {:>11}",
                "uploads", "inline MiB/s", "cpu-s / GiB", "server MiB/s", "cpu-s / GiB", "speed", "cpu cost", "lanes/batch"
            );
            // User + system CPU time of the whole process (all threads), in seconds.
            fn cpu_seconds() -> f64 {
                let t = rustix::time::clock_gettime(rustix::time::ClockId::ProcessCPUTime);
                t.tv_sec as f64 + t.tv_nsec as f64 / 1e9
            }
            for n in [1usize, 2, 3, 4, 8, 16, 32] {
                let mut rates = [0f64; 2];
                let mut cpu = [0f64; 2];
                let before = (
                    server.stats().batches.load(Ordering::Relaxed),
                    server.stats().batched_lanes.load(Ordering::Relaxed),
                );
                for (slot, sink) in [
                    Arc::new(InlineSink) as Arc<dyn Md5StreamSink>,
                    server.clone() as Arc<dyn Md5StreamSink>,
                ]
                .into_iter()
                .enumerate()
                {
                    let mut best = 0f64;
                    let cpu_before = cpu_seconds();
                    for _ in 0..3 {
                        let started = std::time::Instant::now();
                        let tasks: Vec<_> = (0..n)
                            .map(|_| {
                                let (data, sink, want) = (data.clone(), sink.clone(), want.clone());
                                tokio::spawn(async move {
                                    let source = Dribble {
                                        data: data.to_vec(),
                                        at: 0,
                                        step: 64 * 1024,
                                    };
                                    let mut reader = EtagReader::with_sink(source, None, sink.as_ref());
                                    let mut buf = vec![0u8; 64 * 1024];
                                    while reader.read(&mut buf).await.unwrap() != 0 {}
                                    assert_eq!(reader.get_etag(), Some(want));
                                })
                            })
                            .collect();
                        for task in tasks {
                            task.await.unwrap();
                        }
                        best = best.max((n * PER_UPLOAD) as f64 / started.elapsed().as_secs_f64() / 1048576.0);
                    }
                    rates[slot] = best;
                    cpu[slot] = (cpu_seconds() - cpu_before) / (3.0 * (n * PER_UPLOAD) as f64 / (1u64 << 30) as f64);
                }
                let batches = server.stats().batches.load(Ordering::Relaxed) - before.0;
                let lanes = server.stats().batched_lanes.load(Ordering::Relaxed) - before.1;
                println!(
                    "{n:>7} | {:>12.0} {:>12.2} | {:>12.0} {:>12.2} | {:>5.2}x {:>7.2}x {:>11.1}",
                    rates[0],
                    cpu[0],
                    rates[1],
                    cpu[1],
                    rates[1] / rates[0],
                    cpu[1] / cpu[0],
                    lanes as f64 / batches.max(1) as f64
                );
            }
        }
    }
}
