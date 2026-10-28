// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::harness::{DistCluster, DistLayout, TestResult, unique_bucket};
use crate::tls_gen;
use aws_sdk_s3::operation::RequestId;
use aws_sdk_s3::{Client, config::retry::RetryConfig, primitives::ByteStream};
use hmac::{Hmac, Mac, digest::KeyInit};
use rand::{Rng, SeedableRng, rngs::StdRng};
use sha2::{Digest, Sha256};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
#[cfg(target_os = "linux")]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, TryLockError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::task::JoinSet;
#[cfg(target_os = "linux")]
use zeroize::Zeroizing;

type TagMac = Hmac<Sha256>;
const GET_OBSERVATION_DIR_ENV: &str = "RUSTFS_E2E_TLS_GET_OBSERVATION_DIR";

struct GetObservation {
    mac: Arc<TagMac>,
    directory: PathBuf,
    key_server: tokio::task::JoinHandle<()>,
    #[cfg(target_os = "linux")]
    allowed_pids: tokio::sync::watch::Sender<Vec<u32>>,
    #[cfg(target_os = "linux")]
    read_slots: Arc<tokio::sync::Semaphore>,
    #[cfg(target_os = "linux")]
    read_attempts: AtomicUsize,
}

impl Drop for GetObservation {
    fn drop(&mut self) {
        self.key_server.abort();
    }
}

const MIB: u64 = 1024 * 1024;
const READ_TIMEOUT: Duration = Duration::from_secs(180);

struct ExpectedRead {
    range: Option<(u64, u64)>,
    length: u64,
    sha256: String,
}

struct ObjectFixture {
    key: String,
    path: PathBuf,
    length: u64,
    reads: Vec<ExpectedRead>,
}

#[derive(Clone)]
struct ConcurrentReadStream {
    stage: &'static str,
    started_ms: Option<u64>,
    headers_ms: Option<u64>,
    body_started_ms: Option<u64>,
    completed_ms: Option<u64>,
    received: u64,
    last_progress_ms: Option<u64>,
    request_tag: Option<[u8; 32]>,
    terminal: Option<&'static str>,
    terminal_ms: Option<u64>,
}

impl Default for ConcurrentReadStream {
    fn default() -> Self {
        Self {
            stage: "queued",
            started_ms: None,
            headers_ms: None,
            body_started_ms: None,
            completed_ms: None,
            received: 0,
            last_progress_ms: None,
            request_tag: None,
            terminal: None,
            terminal_ms: None,
        }
    }
}

struct ConcurrentReadProgress {
    started: Instant,
    started_clock: ClockSample,
    expected: u64,
    streams: Vec<Mutex<ConcurrentReadStream>>,
    tag_mac: Option<Arc<TagMac>>,
}

#[derive(Clone, Copy)]
struct ClockSample {
    unix_ms: Option<u64>,
    before_ms: Option<u64>,
    after_ms: Option<u64>,
}

impl ClockSample {
    fn capture(started: Instant) -> Self {
        let before_ms = elapsed_ms(started);
        Self::capture_after(started, before_ms)
    }

    fn capture_after(started: Instant, before_ms: Option<u64>) -> Self {
        let unix_ms = unix_ms_at(SystemTime::now());
        let after_ms = elapsed_ms(started);
        Self {
            unix_ms,
            before_ms,
            after_ms,
        }
    }

    fn json(self) -> serde_json::Value {
        serde_json::json!({
            "unix_ms": self.unix_ms,
            "monotonic_before_ms": self.before_ms,
            "monotonic_after_ms": self.after_ms,
        })
    }
}

impl ConcurrentReadProgress {
    fn new(expected: u64, count: usize, tag_mac: Option<Arc<TagMac>>) -> Self {
        let started = Instant::now();
        Self {
            started,
            started_clock: ClockSample::capture(started),
            expected,
            streams: (0..count).map(|_| Mutex::new(ConcurrentReadStream::default())).collect(),
            tag_mac,
        }
    }

    fn elapsed_ms(&self) -> Option<u64> {
        elapsed_ms(self.started)
    }

    fn start(&self, node: usize) {
        let mut stream = self.streams[node].lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        stream.started_ms = self.elapsed_ms();
        stream.stage = "waiting_headers";
    }

    fn response(&self, node: usize, request_id: Option<&str>, has_headers: bool) {
        let mut stream = self.streams[node].lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let (Some(mac), Some(id)) = (&self.tag_mac, request_id.and_then(|id| uuid::Uuid::parse_str(id).ok())) {
            let mut mac = (**mac).clone();
            mac.update(b"rustfs-get-observation-request-v1\0");
            mac.update(id.as_bytes());
            let mut tag = [0; 32];
            tag.copy_from_slice(&mac.finalize().into_bytes());
            stream.request_tag = Some(tag);
        }
        if has_headers {
            stream.headers_ms = self.elapsed_ms();
            stream.stage = "headers_received";
        }
    }

    fn body(&self, node: usize) {
        let mut stream = self.streams[node].lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        stream.body_started_ms = self.elapsed_ms();
        stream.stage = "reading_body";
    }

    fn progress(&self, node: usize, received: u64) {
        let mut stream = self.streams[node].lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        stream.last_progress_ms = self.elapsed_ms();
        stream.received = received;
    }

    fn complete(&self, node: usize) {
        let mut stream = self.streams[node].lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        stream.completed_ms = self.elapsed_ms();
        stream.stage = "complete";
    }

    fn terminal(&self, node: usize, terminal: &'static str) {
        let mut stream = self.streams[node].lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        stream.terminal = Some(terminal);
        stream.terminal_ms = self.elapsed_ms();
    }

    fn failure_snapshot(&self, failed_node: Option<usize>, kind: &'static str) -> serde_json::Value {
        let streams: Vec<_> = self
            .streams
            .iter()
            .enumerate()
            .map(|(node, stream)| {
                // Avoid waiting on another task while retaining the first error.
                let mut snapshot = None;
                let mut unavailable = "busy";
                for _ in 0..8 {
                    let before_ms = self.elapsed_ms();
                    match stream.try_lock() {
                        Ok(guard) => {
                            let stream = guard.clone();
                            drop(guard);
                            snapshot = Some((stream, ClockSample::capture_after(self.started, before_ms)));
                            break;
                        }
                        Err(TryLockError::WouldBlock) => std::hint::spin_loop(),
                        Err(TryLockError::Poisoned(_)) => {
                            unavailable = "poisoned";
                            break;
                        }
                    }
                }
                let Some((stream, sampled_clock)) = snapshot else {
                    return serde_json::json!({
                        "node": node,
                        "snapshot": unavailable,
                        "sampled_clock": ClockSample::capture(self.started).json(),
                        "last_stage": "unknown",
                        "terminal": "unknown",
                        "request_tag": null,
                        "received_bytes": null,
                        "last_progress_ms": null,
                    });
                };
                serde_json::json!({
                    "node": node,
                    "snapshot": "available",
                    "sampled_clock": sampled_clock.json(),
                    "last_stage": stream.stage,
                    "request_tag": stream.request_tag.map(|tag| hex_simd::encode_to_string(tag, hex_simd::AsciiCase::Lower)),
                    "started_ms": stream.started_ms,
                    "headers_ms": stream.headers_ms,
                    "body_started_ms": stream.body_started_ms,
                    "completed_ms": stream.completed_ms,
                    "received_bytes": stream.received,
                    "last_progress_ms": stream.last_progress_ms,
                    "terminal": stream.terminal.unwrap_or("unknown"),
                    "terminal_ms": stream.terminal_ms,
                })
            })
            .collect();
        serde_json::json!({
            "kind": kind,
            "failed_node": failed_node,
            "elapsed_ms": self.elapsed_ms(),
            "stage_start_clock_sample": self.started_clock.json(),
            "clock_scope": "client_unsynchronized",
            "expected_bytes": self.expected,
            "streams": streams,
        })
    }

    async fn report_failure(&self, failed_node: Option<usize>, kind: &'static str) {
        write_diagnostic_line(format!(
            "TLS acceptance: concurrent GET failure: {}\n",
            self.failure_snapshot(failed_node, kind)
        ))
        .await;
    }
}

async fn write_diagnostic_line(line: String) {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    if std::thread::Builder::new()
        .name("e2e-get-failure-report".into())
        .spawn(move || {
            let written = std::io::stderr().lock().write_all(line.as_bytes()).is_ok();
            let _ = sender.send(written);
        })
        .is_ok()
    {
        let _ = tokio::time::timeout(Duration::from_millis(250), receiver).await;
    }
}

fn elapsed_ms(started: Instant) -> Option<u64> {
    u64::try_from(started.elapsed().as_millis()).ok()
}

fn unix_ms_at(time: SystemTime) -> Option<u64> {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
}

#[cfg(target_os = "linux")]
fn configure_get_observation(dist: &mut DistCluster) -> TestResult<Option<GetObservation>> {
    use std::os::unix::fs::{DirBuilderExt, MetadataExt};
    use tokio::io::AsyncWriteExt;

    let Some(root) = std::env::var_os(GET_OBSERVATION_DIR_ENV) else {
        return Ok(None);
    };
    let root = PathBuf::from(root);
    let metadata = std::fs::symlink_metadata(&root)?;
    if !metadata.file_type().is_dir() || metadata.mode() & 0o077 != 0 {
        return Err("GET observation root must be a private directory".into());
    }
    let directory = root.join(uuid::Uuid::new_v4().to_string());
    let directory_env = directory.to_str().ok_or("GET observation path must be UTF-8")?.to_owned();
    std::fs::DirBuilder::new().mode(0o700).create(&directory)?;
    let owner_uid = std::fs::symlink_metadata(&directory)?.uid();
    let listener = tokio::net::UnixListener::bind(directory.join("key.sock"))?;
    let expected_binary = std::fs::canonicalize(crate::common::rustfs_binary_path())?;
    let mut key_bytes = Zeroizing::new([0u8; 32]);
    rand::rng().fill_bytes(&mut *key_bytes);
    let key = Zeroizing::new(hex_simd::encode_to_string(&key_bytes[..], hex_simd::AsciiCase::Lower));
    let mac = TagMac::new_from_slice(key.as_bytes()).map_err(|_| "invalid observation key")?;
    let parent_pid = std::process::id();
    let (allowed_pids, mut registrations) = tokio::sync::watch::channel(Vec::<u32>::new());
    let key_server = tokio::spawn(async move {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        let mut delivered = std::collections::HashSet::new();
        for _ in 0..32 {
            let Ok(Ok((mut stream, _))) = tokio::time::timeout_at(deadline, listener.accept()).await else {
                return;
            };
            let Ok(peer) = stream.peer_cred() else { continue };
            let Some(pid) = peer.pid().and_then(|pid| u32::try_from(pid).ok()) else {
                continue;
            };
            if peer.uid() != owner_uid || delivered.contains(&pid) {
                continue;
            }
            let peer_deadline = deadline.min(tokio::time::Instant::now() + Duration::from_secs(5));
            if !wait_for_registered_pid(pid, &mut registrations, peer_deadline).await {
                continue;
            }
            let valid = tokio::time::timeout_at(peer_deadline, is_expected_child(pid, parent_pid, &expected_binary)).await;
            if !matches!(valid, Ok(true)) {
                continue;
            }
            if !matches!(tokio::time::timeout_at(peer_deadline, stream.write_all(key.as_bytes())).await, Ok(Ok(()))) {
                return;
            }
            delivered.insert(pid);
            if delivered.len() == 4 {
                return;
            }
        }
    });
    dist.cluster.set_env(GET_OBSERVATION_DIR_ENV, directory_env);
    Ok(Some(GetObservation {
        mac: Arc::new(mac),
        directory,
        key_server,
        allowed_pids,
        read_slots: Arc::new(tokio::sync::Semaphore::new(4)),
        read_attempts: AtomicUsize::new(0),
    }))
}

#[cfg(target_os = "linux")]
async fn wait_for_registered_pid(
    pid: u32,
    registrations: &mut tokio::sync::watch::Receiver<Vec<u32>>,
    deadline: tokio::time::Instant,
) -> bool {
    loop {
        let (registered, complete) = {
            let pids = registrations.borrow_and_update();
            (pids.contains(&pid), pids.len() == 4)
        };
        if registered {
            return true;
        }
        if complete || !matches!(tokio::time::timeout_at(deadline, registrations.changed()).await, Ok(Ok(()))) {
            return false;
        }
    }
}

#[cfg(target_os = "linux")]
async fn is_expected_child(pid: u32, parent: u32, expected_binary: &Path) -> bool {
    let Ok(status) = tokio::fs::read_to_string(format!("/proc/{pid}/status")).await else {
        return false;
    };
    let actual_parent = status
        .lines()
        .find_map(|line| line.strip_prefix("PPid:")?.trim().parse::<u32>().ok());
    if actual_parent != Some(parent) {
        return false;
    }
    tokio::fs::read_link(format!("/proc/{pid}/exe"))
        .await
        .is_ok_and(|path| path == expected_binary)
}

#[cfg(not(target_os = "linux"))]
fn configure_get_observation(_dist: &mut DistCluster) -> TestResult<Option<GetObservation>> {
    if std::env::var_os(GET_OBSERVATION_DIR_ENV).is_some() {
        return Err("GET observation requires a Linux test host".into());
    }
    Ok(None)
}

async fn verify_selected_get_observations(
    observation: &GetObservation,
    progress: &ConcurrentReadProgress,
    pids: &[Option<u32>],
) -> TestResult {
    let targets: Vec<_> = progress
        .streams
        .iter()
        .enumerate()
        .map(|(node, stream)| {
            let stream = stream.lock().map_err(|_| "GET progress lock poisoned")?;
            let tag = stream.request_tag.ok_or("GET response has no correlatable request ID")?;
            let pid = pids.get(node).copied().flatten().ok_or("GET node PID missing")?;
            Ok::<_, &'static str>((tag, pid))
        })
        .collect::<Result<_, _>>()?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut confirmed = vec![NodeObservation::default(); targets.len()];
    loop {
        for ((tag, pid), confirmed) in targets.iter().zip(&mut confirmed) {
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            if confirmed.matches() {
                continue;
            }
            let path = observation.directory.join(format!("get-reader-{pid}.events"));
            let attempt_deadline = deadline.min(tokio::time::Instant::now() + Duration::from_millis(100));
            let records = tokio::time::timeout_at(attempt_deadline, read_authenticated_capture(path, observation, *pid))
                .await
                .ok()
                .flatten();
            if let Some(records) = records {
                confirmed.observe(&records, tag);
            }
            if confirmed.is_ambiguous() {
                return Err("GET observation is ambiguous; capture status is unknown".into());
            }
        }
        if confirmed.iter().all(NodeObservation::matches) && tokio::time::Instant::now() < deadline {
            for ((tag, pid), confirmed) in targets.iter().zip(&mut confirmed) {
                if tokio::time::Instant::now() >= deadline {
                    return Err("GET observation missing or incomplete; capture status is unknown".into());
                }
                let path = observation.directory.join(format!("get-reader-{pid}.events"));
                let attempt_deadline = deadline.min(tokio::time::Instant::now() + Duration::from_millis(100));
                let Some(records) =
                    tokio::time::timeout_at(attempt_deadline, read_authenticated_capture(path, observation, *pid))
                        .await
                        .ok()
                        .flatten()
                else {
                    return Err("GET observation missing or incomplete; capture status is unknown".into());
                };
                let current_is_unique = selected_observations_match(&records, tag);
                confirmed.observe(&records, tag);
                if !current_is_unique || !confirmed.matches() {
                    return Err("GET observation is ambiguous or incomplete; capture status is unknown".into());
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("GET observation missing or incomplete; capture status is unknown".into());
            }
            // The selected records are positive observations; this does not
            // establish capture completeness or rule out suffix deletion.
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("GET observation missing or incomplete; capture status is unknown".into());
        }
        let pending = confirmed.iter().filter(|confirmed| !confirmed.matches()).count();
        let interval = Duration::from_millis(55).saturating_mul(u32::try_from(pending).unwrap_or(u32::MAX));
        tokio::time::sleep_until(deadline.min(tokio::time::Instant::now() + interval)).await;
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ObservedEvent {
    Constructed,
    Dropped,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct AuthenticatedRecord {
    tag: [u8; 32],
    event: ObservedEvent,
    expected: u64,
    emitted: Option<u64>,
    elapsed_ms: Option<u64>,
    unix_ms: Option<u64>,
}

fn parse_optional_u64(value: &str) -> Option<Option<u64>> {
    if value == "unknown" {
        Some(None)
    } else {
        value.parse().ok().map(Some)
    }
}

fn authenticate_record(mac: &TagMac, line: &str, expected_pid: u32) -> Option<AuthenticatedRecord> {
    let fields: Vec<_> = line.split_whitespace().collect();
    if fields.len() != 9 || fields[0] != "v1" || fields[1].parse::<u32>().ok()? != expected_pid {
        return None;
    }
    let tag: [u8; 32] = hex_simd::decode_to_vec(fields[2]).ok()?.try_into().ok()?;
    let (event, code) = match fields[3] {
        "constructed" => (ObservedEvent::Constructed, 1),
        "dropped" => (ObservedEvent::Dropped, 2),
        _ => return None,
    };
    let expected = fields[4].parse::<u64>().ok()?;
    let emitted = parse_optional_u64(fields[5])?;
    let elapsed = parse_optional_u64(fields[6])?;
    let unix = parse_optional_u64(fields[7])?;
    let signature = hex_simd::decode_to_vec(fields[8]).ok()?;
    let mut check = mac.clone();
    check.update(b"rustfs-get-observation-record-v1\0");
    check.update(&expected_pid.to_be_bytes());
    check.update(&tag);
    check.update(&[code]);
    check.update(&expected.to_be_bytes());
    for value in [emitted, elapsed, unix] {
        check.update(&[u8::from(value.is_some())]);
        check.update(&value.unwrap_or_default().to_be_bytes());
    }
    check.verify_slice(&signature).ok()?;
    Some(AuthenticatedRecord {
        tag,
        event,
        expected,
        emitted,
        elapsed_ms: elapsed,
        unix_ms: unix,
    })
}

#[cfg(target_os = "linux")]
fn inspect_event_file(path: &Path) -> Option<rustix::fd::OwnedFd> {
    use rustix::fs::{CWD, Mode, OFlags, ResolveFlags, openat2};
    openat2(
        CWD,
        path,
        OFlags::PATH | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
        ResolveFlags::NO_SYMLINKS,
    )
    .ok()
}

#[cfg(target_os = "linux")]
fn reopen_inspected_regular(inspected: &rustix::fd::OwnedFd) -> Option<std::fs::File> {
    use rustix::fs::{FileType, Mode, OFlags, fstat, open};
    use std::os::fd::AsRawFd as _;
    let before = fstat(inspected).ok()?;
    if FileType::from_raw_mode(before.st_mode) != FileType::RegularFile {
        return None;
    }
    // The proc descriptor link refers to this already-pinned inode, not to
    // the attacker-replaceable original path.
    let pinned_path = format!("/proc/self/fd/{}", inspected.as_raw_fd());
    let fd = open(pinned_path.as_str(), OFlags::RDONLY | OFlags::NONBLOCK | OFlags::CLOEXEC, Mode::empty()).ok()?;
    let after = fstat(&fd).ok()?;
    if FileType::from_raw_mode(after.st_mode) != FileType::RegularFile
        || before.st_dev != after.st_dev
        || before.st_ino != after.st_ino
    {
        return None;
    }
    Some(fd.into())
}

#[cfg(target_os = "linux")]
fn read_event_bytes(path: &Path) -> Option<Vec<u8>> {
    const MAX_BYTES: u64 = 64 * 1024;
    let inspected = inspect_event_file(path)?;
    let file = reopen_inspected_regular(&inspected)?;
    let mut bytes = Vec::new();
    if file.take(MAX_BYTES + 1).read_to_end(&mut bytes).is_err() || bytes.len() > usize::try_from(MAX_BYTES).unwrap_or(usize::MAX)
    {
        return None;
    }
    Some(bytes)
}

#[cfg(target_os = "linux")]
async fn bounded_event_bytes(path: PathBuf, slots: &Arc<tokio::sync::Semaphore>, attempts: &AtomicUsize) -> Option<Vec<u8>> {
    let permit = Arc::clone(slots).try_acquire_owned().ok()?;
    if attempts
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| (n < 64).then_some(n + 1))
        .is_err()
    {
        return None;
    }
    let (sender, receiver) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("e2e-get-event-read".into())
        .spawn(move || {
            let bytes = read_event_bytes(&path);
            drop(permit);
            let _ = sender.send(bytes);
        })
        .ok()?;
    receiver.await.ok()?
}

#[cfg(target_os = "linux")]
async fn read_authenticated_capture(path: PathBuf, observation: &GetObservation, pid: u32) -> Option<Vec<AuthenticatedRecord>> {
    let bytes = bounded_event_bytes(path, &observation.read_slots, &observation.read_attempts).await?;
    authenticate_capture(&bytes, &observation.mac, pid)
}

#[cfg(not(target_os = "linux"))]
async fn read_authenticated_capture(
    _path: PathBuf,
    _observation: &GetObservation,
    _pid: u32,
) -> Option<Vec<AuthenticatedRecord>> {
    None
}

fn authenticate_capture(bytes: &[u8], mac: &TagMac, pid: u32) -> Option<Vec<AuthenticatedRecord>> {
    if bytes.last() != Some(&b'\n') {
        return None;
    }
    let text = std::str::from_utf8(bytes).ok()?;
    let mut records = Vec::new();
    for line in text.split_terminator('\n') {
        if records.len() == 128 {
            return None;
        }
        records.push(authenticate_record(mac, line, pid)?);
    }
    Some(records)
}

#[derive(Clone, Copy, Default)]
enum EventObservation {
    #[default]
    Missing,
    Unique(AuthenticatedRecord),
    Ambiguous,
}

impl EventObservation {
    fn observe(&mut self, records: &[AuthenticatedRecord], tag: &[u8; 32], event: ObservedEvent) {
        if matches!(&*self, Self::Ambiguous) {
            return;
        }
        let mut matches = records.iter().filter(|record| &record.tag == tag && record.event == event);
        let Some(first) = matches.next().copied() else {
            return;
        };
        if matches.next().is_some() || matches!(&*self, Self::Unique(previous) if *previous != first) {
            *self = Self::Ambiguous;
        } else {
            *self = Self::Unique(first);
        }
    }

    fn unique(self) -> Option<AuthenticatedRecord> {
        match self {
            Self::Unique(record) => Some(record),
            Self::Missing | Self::Ambiguous => None,
        }
    }

    fn invalidate_unique(&mut self) {
        if matches!(&*self, Self::Unique(_)) {
            *self = Self::Missing;
        }
    }
}

#[derive(Clone, Copy, Default)]
struct NodeObservation {
    constructed: EventObservation,
    dropped: EventObservation,
    finalized: bool,
}

impl NodeObservation {
    fn observe(&mut self, records: &[AuthenticatedRecord], tag: &[u8; 32]) {
        if self.finalized {
            return;
        }
        self.constructed.observe(records, tag, ObservedEvent::Constructed);
        self.dropped.observe(records, tag, ObservedEvent::Dropped);
    }

    fn finalize(&mut self, records: Option<&[AuthenticatedRecord]>, tag: Option<&[u8; 32]>) {
        if self.finalized {
            return;
        }
        if let (Some(records), Some(tag)) = (records, tag) {
            self.observe(records, tag);
            if !records
                .iter()
                .any(|record| &record.tag == tag && record.event == ObservedEvent::Constructed)
            {
                self.constructed.invalidate_unique();
            }
            if !records
                .iter()
                .any(|record| &record.tag == tag && record.event == ObservedEvent::Dropped)
            {
                self.dropped.invalidate_unique();
            }
        } else {
            self.constructed.invalidate_unique();
            self.dropped.invalidate_unique();
        }
        self.finalized = true;
    }

    fn is_ambiguous(&self) -> bool {
        matches!(self.constructed, EventObservation::Ambiguous) || matches!(self.dropped, EventObservation::Ambiguous)
    }

    fn matches(&self) -> bool {
        self.constructed
            .unique()
            .is_some_and(|record| record.expected == 1024 * MIB && record.emitted.is_none())
            && self
                .dropped
                .unique()
                .is_some_and(|record| record.expected == 1024 * MIB && record.emitted == Some(1024 * MIB))
    }

    fn needs_failure_retry(&self) -> bool {
        !self.finalized
            && (matches!(self.constructed, EventObservation::Missing) || matches!(self.dropped, EventObservation::Missing))
    }
}

fn authenticated_observations_match(bytes: &[u8], mac: &TagMac, pid: u32, tag: &[u8; 32]) -> bool {
    let Some(records) = authenticate_capture(bytes, mac, pid) else {
        return false;
    };
    selected_observations_match(&records, tag)
}

fn selected_observations_match(records: &[AuthenticatedRecord], tag: &[u8; 32]) -> bool {
    let mut selected = NodeObservation::default();
    selected.observe(records, tag);
    selected.matches()
}

#[derive(Clone, Copy)]
struct FailedReadTarget {
    node: usize,
    pid: Option<u32>,
    tag: Option<[u8; 32]>,
}

fn failed_read_targets(progress: &ConcurrentReadProgress, pids: &[Option<u32>]) -> Vec<FailedReadTarget> {
    progress
        .streams
        .iter()
        .enumerate()
        .map(|(node, stream)| FailedReadTarget {
            node,
            pid: pids.get(node).copied().flatten(),
            tag: stream.try_lock().ok().and_then(|stream| stream.request_tag),
        })
        .collect()
}

fn failure_observation_summary(target: FailedReadTarget, records: Option<&[AuthenticatedRecord]>) -> serde_json::Value {
    let mut selected = NodeObservation::default();
    if let (Some(records), Some(tag), Some(_)) = (records, target.tag, target.pid) {
        selected.observe(records, &tag);
    }
    failure_selection_summary(target, &selected)
}

fn failure_selection_summary(target: FailedReadTarget, selected: &NodeObservation) -> serde_json::Value {
    let tag_hex = target
        .tag
        .map(|tag| hex_simd::encode_to_string(tag, hex_simd::AsciiCase::Lower));
    let constructed = selected
        .constructed
        .unique()
        .filter(|record| record.expected == 1024 * MIB && record.emitted.is_none())
        .map(|record| serde_json::json!({"unix_ms": record.unix_ms}));
    let dropped = selected
        .dropped
        .unique()
        .filter(|record| record.expected == 1024 * MIB)
        .map(|record| {
            serde_json::json!({
                "emitted_bytes": record.emitted,
                "elapsed_ms": record.elapsed_ms,
                "unix_ms": record.unix_ms,
            })
        });
    serde_json::json!({
        "node": target.node,
        "request_tag": tag_hex,
        "constructed": constructed,
        "dropped": dropped,
        "missing_or_ambiguous": "unknown",
    })
}

async fn collect_authenticated_failure<T: Send + 'static>(
    observation: &GetObservation,
    progress: &ConcurrentReadProgress,
    pids: &[Option<u32>],
    readers: &mut JoinSet<T>,
    failure_clock: ClockSample,
) -> serde_json::Value {
    collect_authenticated_failure_inner(observation, progress, pids, readers, failure_clock, None).await
}

#[cfg(test)]
struct FailureCaptureBarrier {
    node: usize,
    observed: Option<tokio::sync::oneshot::Sender<NodeObservation>>,
    release: Option<tokio::sync::oneshot::Receiver<()>>,
}

#[cfg(test)]
async fn collect_authenticated_failure_inner<T: Send + 'static>(
    observation: &GetObservation,
    progress: &ConcurrentReadProgress,
    pids: &[Option<u32>],
    readers: &mut JoinSet<T>,
    failure_clock: ClockSample,
    mut barrier: Option<&mut FailureCaptureBarrier>,
) -> serde_json::Value {
    readers.abort_all();
    let drained = tokio::time::timeout(Duration::from_secs(1), async { while readers.join_next().await.is_some() {} })
        .await
        .is_ok();
    let targets = failed_read_targets(progress, pids);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let retry_deadline = deadline - Duration::from_millis(150);
    let mut selected = vec![NodeObservation::default(); targets.len()];
    loop {
        let mut pending = 0usize;
        for (target, selected) in targets.iter().zip(&mut selected) {
            if tokio::time::Instant::now() >= retry_deadline {
                break;
            }
            let (Some(pid), Some(tag)) = (target.pid, target.tag) else {
                continue;
            };
            if !selected.needs_failure_retry() {
                continue;
            }
            let path = observation.directory.join(format!("get-reader-{pid}.events"));
            let attempt_deadline = retry_deadline.min(tokio::time::Instant::now() + Duration::from_millis(100));
            let records = tokio::time::timeout_at(attempt_deadline, read_authenticated_capture(path, observation, pid))
                .await
                .ok()
                .flatten();
            if let Some(records) = records {
                selected.observe(&records, &tag);
                if let Some(barrier) = barrier.as_deref_mut()
                    && barrier.node == target.node
                    && selected.matches()
                    && selected_observations_match(&records, &tag)
                    && let (Some(observed), Some(release)) = (barrier.observed.take(), barrier.release.take())
                    && observed.send(*selected).is_ok()
                {
                    let _ = tokio::time::timeout_at(retry_deadline, release).await;
                }
            }
            if selected.needs_failure_retry() {
                pending += 1;
            }
        }
        if pending == 0 || tokio::time::Instant::now() >= retry_deadline {
            break;
        }
        let interval = Duration::from_millis(50).saturating_mul(u32::try_from(pending).unwrap_or(u32::MAX));
        tokio::time::sleep_until(retry_deadline.min(tokio::time::Instant::now() + interval)).await;
    }
    let audits = futures::future::join_all(targets.iter().map(|target| async {
        let (Some(pid), Some(_)) = (target.pid, target.tag) else {
            return None;
        };
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        let path = observation.directory.join(format!("get-reader-{pid}.events"));
        let result = tokio::time::timeout_at(deadline, read_authenticated_capture(path, observation, pid))
            .await
            .ok()
            .flatten();
        if tokio::time::Instant::now() < deadline {
            result
        } else {
            None
        }
    }))
    .await;
    for ((target, selected), records) in targets.iter().zip(&mut selected).zip(audits) {
        selected.finalize(records.as_deref(), target.tag.as_ref());
    }
    let summaries: Vec<_> = targets
        .iter()
        .zip(&selected)
        .map(|(target, selected)| failure_selection_summary(*target, selected))
        .collect();
    serde_json::json!({
        "scope": "selected_records_collected_after_reader_abort",
        "failure_observed_clock_sample": failure_clock.json(),
        "clock_scope": "host_unsynchronized",
        "reader_tasks_drained": if drained { "yes" } else { "unknown" },
        "capture_completeness": "unknown",
        "reader_drop_cause": "unknown",
        "event_order_relative_to_abort": "unknown",
        "reader_identity_relation": "unknown",
        "client_receipt_from_server_emission": "unknown",
        "streams": summaries,
    })
}

async fn report_authenticated_failure<T: Send + 'static>(
    observation: &GetObservation,
    progress: &ConcurrentReadProgress,
    pids: &[Option<u32>],
    readers: &mut JoinSet<T>,
    failure_clock: ClockSample,
) {
    let summary = collect_authenticated_failure(observation, progress, pids, readers, failure_clock).await;
    write_diagnostic_line(format!("TLS GET authenticated positive observations: {summary}\n")).await;
}

#[test]
fn authenticated_observation_rejects_missing_truncated_and_modified_records() {
    let key = b"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let mac = TagMac::new_from_slice(key).expect("fixture key");
    let tag_hex = "636a632459fe3a2e3d23493ca06429fc88e6ebf79aa11352a1d7b39cefe401d0";
    let tag: [u8; 32] = hex_simd::decode_to_vec(tag_hex)
        .expect("fixture tag")
        .try_into()
        .expect("32-byte tag");
    let first = format!(
        "v1 4242 {tag_hex} constructed 1073741824 unknown 0 1700000000000 d19307fd28aa41b5177b014ee5d725fb19a20523c54b985c33cd3eccdced8bda\n"
    );
    let last = format!(
        "v1 4242 {tag_hex} dropped 1073741824 1073741824 1000 1700000001000 056e8b48b9113b70f0dbc55999148403785dddec499b69df25431ea95323aff3\n"
    );
    let both = format!("{first}{last}");
    assert!(authenticated_observations_match(both.as_bytes(), &mac, 4242, &tag));
    assert!(!authenticated_observations_match(first.as_bytes(), &mac, 4242, &tag));
    assert!(!authenticated_observations_match(both.trim_end().as_bytes(), &mac, 4242, &tag));
    assert!(!authenticated_observations_match(both.as_bytes(), &mac, 4243, &tag));
    let tampered = both.replace(" 1000 1700000001000 ", " 1001 1700000001000 ");
    assert!(!authenticated_observations_match(tampered.as_bytes(), &mac, 4242, &tag));
    let forged = format!("{both}v1 forged\n");
    assert!(!authenticated_observations_match(forged.as_bytes(), &mac, 4242, &tag));

    let target = FailedReadTarget {
        node: 0,
        pid: Some(4242),
        tag: Some(tag),
    };
    let records = authenticate_capture(both.as_bytes(), &mac, 4242).expect("authenticated fixture");
    let summary = failure_observation_summary(target, Some(&records));
    assert_eq!(summary["dropped"]["emitted_bytes"], 1024 * MIB);
    assert_eq!(summary["constructed"]["unix_ms"], 1_700_000_000_000u64);
    assert_eq!(summary["missing_or_ambiguous"], "unknown");
    let encoded = summary.to_string();
    assert!(!encoded.contains("0123456789abcdef"));
    assert!(!encoded.contains("b8172a15-a428-4b42-9cef-1f4a7b96874e"));
    assert!(!encoded.contains("d19307fd28aa41b5"));

    let invalid = authenticate_capture(tampered.as_bytes(), &mac, 4242);
    let unknown = failure_observation_summary(target, invalid.as_deref());
    assert!(unknown["constructed"].is_null());
    assert!(unknown["dropped"].is_null());
    let first_only = authenticate_capture(first.as_bytes(), &mac, 4242).expect("authenticated initial event");
    let incomplete = failure_observation_summary(target, Some(&first_only));
    assert!(!incomplete["constructed"].is_null());
    assert!(incomplete["dropped"].is_null());
    let missing_identity = FailedReadTarget { pid: None, ..target };
    assert!(failure_observation_summary(missing_identity, Some(&records))["dropped"].is_null());

    let duplicated = format!("{both}{first}");
    let records = authenticate_capture(duplicated.as_bytes(), &mac, 4242).expect("authenticated duplicate");
    let ambiguous = failure_observation_summary(target, Some(&records));
    assert!(ambiguous["constructed"].is_null());
    assert!(!ambiguous["dropped"].is_null());
}

#[test]
fn authenticated_duplicate_remains_unknown_after_a_later_unique_snapshot() {
    let key = b"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let mac = TagMac::new_from_slice(key).expect("fixture key");
    let tag_hex = "636a632459fe3a2e3d23493ca06429fc88e6ebf79aa11352a1d7b39cefe401d0";
    let tag: [u8; 32] = hex_simd::decode_to_vec(tag_hex)
        .expect("fixture tag")
        .try_into()
        .expect("32-byte tag");
    let constructed = format!(
        "v1 4242 {tag_hex} constructed 1073741824 unknown 0 1700000000000 d19307fd28aa41b5177b014ee5d725fb19a20523c54b985c33cd3eccdced8bda\n"
    );
    let dropped = format!(
        "v1 4242 {tag_hex} dropped 1073741824 1073741824 1000 1700000001000 056e8b48b9113b70f0dbc55999148403785dddec499b69df25431ea95323aff3\n"
    );
    let unique = authenticate_capture(format!("{constructed}{dropped}").as_bytes(), &mac, 4242).expect("unique records");
    let duplicated_constructed = authenticate_capture(format!("{constructed}{constructed}{dropped}").as_bytes(), &mac, 4242)
        .expect("duplicate construction");
    let duplicated_dropped =
        authenticate_capture(format!("{constructed}{dropped}{dropped}").as_bytes(), &mac, 4242).expect("duplicate drop");
    let target = FailedReadTarget {
        node: 0,
        pid: Some(4242),
        tag: Some(tag),
    };

    let first_only = authenticate_capture(constructed.as_bytes(), &mac, 4242).expect("single construction");
    let mut partial = NodeObservation::default();
    partial.observe(&first_only, &tag);
    assert!(!failure_selection_summary(target, &partial)["constructed"].is_null());
    partial.observe(&duplicated_constructed, &tag);
    assert!(failure_selection_summary(target, &partial)["constructed"].is_null());
    partial.observe(&unique, &tag);
    assert!(failure_selection_summary(target, &partial)["constructed"].is_null());
    assert!(!partial.matches());

    for (duplicates, field) in [(&duplicated_constructed, "constructed"), (&duplicated_dropped, "dropped")] {
        let mut selected = NodeObservation::default();
        selected.observe(&unique, &tag);
        assert!(selected.matches());
        selected.observe(duplicates, &tag);
        assert!(selected.is_ambiguous());
        assert!(!selected.matches());
        assert!(failure_selection_summary(target, &selected)[field].is_null());
        selected.observe(&unique, &tag);
        assert!(selected.is_ambiguous());
        assert!(!selected.matches());
        assert!(failure_selection_summary(target, &selected)[field].is_null());
    }

    let mut changed = unique[0];
    changed.expected = 100 * MIB;
    let mut selected = NodeObservation::default();
    selected.observe(&unique, &tag);
    selected.observe(&[changed], &tag);
    assert!(selected.is_ambiguous());
    assert!(failure_selection_summary(target, &selected)["constructed"].is_null());
}

#[test]
fn final_failure_snapshot_missing_or_invalid_revokes_cached_events() {
    let key = b"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let mac = TagMac::new_from_slice(key).expect("fixture key");
    let tag_hex = "636a632459fe3a2e3d23493ca06429fc88e6ebf79aa11352a1d7b39cefe401d0";
    let tag: [u8; 32] = hex_simd::decode_to_vec(tag_hex)
        .expect("fixture tag")
        .try_into()
        .expect("32-byte tag");
    let constructed = format!(
        "v1 4242 {tag_hex} constructed 1073741824 unknown 0 1700000000000 d19307fd28aa41b5177b014ee5d725fb19a20523c54b985c33cd3eccdced8bda\n"
    );
    let dropped = format!(
        "v1 4242 {tag_hex} dropped 1073741824 1073741824 1000 1700000001000 056e8b48b9113b70f0dbc55999148403785dddec499b69df25431ea95323aff3\n"
    );
    let both = format!("{constructed}{dropped}");
    let unique = authenticate_capture(both.as_bytes(), &mac, 4242).expect("initial authenticated snapshot");
    let truncated = authenticate_capture(both.trim_end().as_bytes(), &mac, 4242);
    let bad_signature = authenticate_capture(both.replace(" 1000 1700000001000 ", " 1001 1700000001000 ").as_bytes(), &mac, 4242);
    let target = FailedReadTarget {
        node: 0,
        pid: Some(4242),
        tag: Some(tag),
    };

    for final_capture in [None, truncated, bad_signature] {
        let mut selected = NodeObservation::default();
        selected.observe(&unique, &tag);
        assert!(selected.matches());
        selected.finalize(final_capture.as_deref(), Some(&tag));
        let summary = failure_selection_summary(target, &selected);
        assert!(summary["constructed"].is_null());
        assert!(summary["dropped"].is_null());
        selected.observe(&unique, &tag);
        assert!(!selected.matches(), "a failed final snapshot cannot be restored");
    }

    let only_constructed = authenticate_capture(constructed.as_bytes(), &mac, 4242).expect("final construction");
    let only_dropped = authenticate_capture(dropped.as_bytes(), &mac, 4242).expect("final drop");
    for (final_capture, missing) in [(&only_constructed, "dropped"), (&only_dropped, "constructed")] {
        let mut selected = NodeObservation::default();
        selected.observe(&unique, &tag);
        selected.finalize(Some(final_capture.as_slice()), Some(&tag));
        assert!(failure_selection_summary(target, &selected)[missing].is_null());
        selected.observe(&unique, &tag);
        assert!(failure_selection_summary(target, &selected)[missing].is_null());
    }
}

#[cfg(target_os = "linux")]
fn observation_retry_fixture() -> (tempfile::TempDir, GetObservation, ConcurrentReadProgress, Vec<Option<u32>>, Vec<String>) {
    let directory = tempfile::tempdir().expect("private fixture directory");
    let key = b"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let mac = Arc::new(TagMac::new_from_slice(key).expect("fixture key"));
    let (allowed_pids, _) = tokio::sync::watch::channel(Vec::new());
    let observation = GetObservation {
        mac: Arc::clone(&mac),
        directory: directory.path().to_path_buf(),
        key_server: tokio::spawn(std::future::pending()),
        allowed_pids,
        read_slots: Arc::new(tokio::sync::Semaphore::new(4)),
        read_attempts: AtomicUsize::new(0),
    };
    let progress = ConcurrentReadProgress::new(1024 * MIB, 4, Some(mac));
    let signatures = [
        (
            "a72d00fe7c58645f0914917f979581885fc88ec8023557e823963fcab95ccb30",
            "65d4e71408f6f32f664253c1861fb94d2a142697a16e45404218bc1d496fffe2",
        ),
        (
            "ed8f4b7a094e28b8f50438b094fd1d6be5d26a94fd157e3b29b35f25ec3446f4",
            "942576b8778a833e0cdf99704cc4c4030bbc61d3e9025f92683a13d282afeb15",
        ),
        (
            "34b4793f1c2c946c7e2c29752e9c01730a12c614cda2a586dd6da7b618daab47",
            "1ad6aa77ae7866ccb4982ce7609a4f96e69f8009c3c9eec2a1a408137616bf3f",
        ),
        (
            "5f356134eacce158b2b933f9a6d57a3090aff2e39bee6e1e1a85090cb09c9a1a",
            "08ac5658429175823d7f2f534ee511ba11fc13a43ed4ff6643955b2b86d1a79a",
        ),
    ];
    let tag = "636a632459fe3a2e3d23493ca06429fc88e6ebf79aa11352a1d7b39cefe401d0";
    let mut pids = Vec::new();
    let mut records = Vec::new();
    for (node, (constructed, dropped)) in signatures.into_iter().enumerate() {
        progress.response(node, Some("b8172a15-a428-4b42-9cef-1f4a7b96874e"), true);
        let pid = 1001 + u32::try_from(node).expect("fixture index");
        pids.push(Some(pid));
        records.push(format!(
            "v1 {pid} {tag} constructed 1073741824 unknown 0 1700000000000 {constructed}\nv1 {pid} {tag} dropped 1073741824 1073741824 1000 1700000001000 {dropped}\n"
        ));
    }
    (directory, observation, progress, pids, records)
}

#[cfg(target_os = "linux")]
async fn wait_for_fixture_reads(observation: &GetObservation, minimum: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while observation.read_attempts.load(Ordering::Relaxed) < minimum {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("fixture read count");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn verification_rechecks_cached_nodes_for_later_authenticated_duplicates() {
    let (directory, observation, progress, pids, records) = observation_retry_fixture();
    for node in 0..3 {
        std::fs::write(
            directory
                .path()
                .join(format!("get-reader-{}.events", pids[node].expect("PID"))),
            &records[node],
        )
        .expect("authenticated fixture");
    }
    let first_path = directory.path().join("get-reader-1001.events");
    let fourth_path = directory.path().join("get-reader-1004.events");
    let duplicate = format!("{}{}\n", records[0], records[0].lines().next().expect("constructed line"));
    let mutation = async {
        wait_for_fixture_reads(&observation, 4).await;
        tokio::fs::write(first_path, duplicate)
            .await
            .expect("authenticated duplicate");
        tokio::time::sleep(Duration::from_millis(100)).await;
        tokio::fs::write(fourth_path, &records[3]).await.expect("late fixture");
    };
    let (result, ()) = tokio::join!(verify_selected_get_observations(&observation, &progress, &pids), mutation);
    assert!(
        result
            .expect_err("later duplicate must invalidate cached selection")
            .to_string()
            .contains("unknown")
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn failure_summary_does_not_restore_uniqueness_after_a_duplicate() {
    let (directory, observation, progress, pids, records) = observation_retry_fixture();
    let first_line = records[0].lines().next().expect("constructed line");
    let first_path = directory.path().join("get-reader-1001.events");
    let fourth_path = directory.path().join("get-reader-1004.events");
    std::fs::write(&first_path, format!("{first_line}\n")).expect("initial partial fixture");
    for node in 1..3 {
        std::fs::write(
            directory
                .path()
                .join(format!("get-reader-{}.events", pids[node].expect("PID"))),
            &records[node],
        )
        .expect("authenticated fixture");
    }
    let mutation = async {
        wait_for_fixture_reads(&observation, 4).await;
        tokio::fs::write(&first_path, format!("{first_line}\n{first_line}\n"))
            .await
            .expect("authenticated duplicate");
        wait_for_fixture_reads(&observation, 6).await;
        tokio::fs::write(&first_path, &records[0])
            .await
            .expect("later unique fixture");
        tokio::fs::write(fourth_path, &records[3]).await.expect("late fixture");
    };
    let mut readers = JoinSet::<()>::new();
    let collection =
        collect_authenticated_failure(&observation, &progress, &pids, &mut readers, ClockSample::capture(progress.started));
    let (summary, ()) = tokio::join!(collection, mutation);
    assert!(summary["streams"][0]["constructed"].is_null());
    assert_eq!(summary["streams"][0]["dropped"]["emitted_bytes"], 1024 * MIB);
    assert_eq!(summary["capture_completeness"], "unknown");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn failure_summary_final_recheck_revokes_missing_or_invalid_events() {
    for mode in 0..4 {
        let (directory, observation, progress, pids, records) = observation_retry_fixture();
        for node in 0..3 {
            std::fs::write(
                directory
                    .path()
                    .join(format!("get-reader-{}.events", pids[node].expect("PID"))),
                &records[node],
            )
            .expect("authenticated fixture");
        }
        let first_path = directory.path().join("get-reader-1001.events");
        let moved_path = directory.path().join("moved.events");
        let fourth_path = directory.path().join("get-reader-1004.events");
        let replacement = match mode {
            1 => format!("{}\n", records[0].lines().nth(1).expect("signed drop")),
            3 => records[0].replace(" 1000 1700000001000 ", " 1001 1700000001000 "),
            _ => records[0].trim_end().to_string(),
        };
        if mode == 3 {
            assert_ne!(replacement, records[0], "bad-signature fixture must change a signed field");
        }
        let (observed_sender, observed_receiver) = tokio::sync::oneshot::channel();
        let (release_sender, release_receiver) = tokio::sync::oneshot::channel();
        let mut barrier = FailureCaptureBarrier {
            node: 0,
            observed: Some(observed_sender),
            release: Some(release_receiver),
        };
        let mutation = async {
            let observed = tokio::time::timeout(Duration::from_secs(1), observed_receiver)
                .await
                .expect("target must authenticate before mutation")
                .expect("collector must report authenticated target");
            assert!(observed.matches(), "target must be uniquely authenticated before mutation");
            let expected_tag = progress.streams[0]
                .lock()
                .expect("target progress")
                .request_tag
                .expect("target tag");
            assert_eq!(observed.constructed.unique().expect("unique constructed event").tag, expected_tag);
            assert_eq!(observed.dropped.unique().expect("unique dropped event").tag, expected_tag);
            if mode == 2 {
                tokio::fs::rename(first_path, moved_path).await.expect("missing final path");
            } else {
                tokio::fs::write(first_path, replacement)
                    .await
                    .expect("changed final snapshot");
            }
            tokio::fs::write(fourth_path, &records[3]).await.expect("late fixture");
            release_sender.send(()).expect("collector must await the mutation");
        };
        let mut readers = JoinSet::<()>::new();
        let collection = collect_authenticated_failure_inner(
            &observation,
            &progress,
            &pids,
            &mut readers,
            ClockSample::capture(progress.started),
            Some(&mut barrier),
        );
        let (summary, ()) = tokio::join!(collection, mutation);
        assert!(summary["streams"][0]["constructed"].is_null());
        if mode == 1 {
            assert_eq!(summary["streams"][0]["dropped"]["emitted_bytes"], 1024 * MIB);
        } else {
            assert!(summary["streams"][0]["dropped"].is_null());
        }
        assert_eq!(summary["streams"][1]["dropped"]["emitted_bytes"], 1024 * MIB);
        assert_eq!(summary["capture_completeness"], "unknown");
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn verification_keeps_confirmed_nodes_while_the_fourth_arrives_late() {
    let (directory, observation, progress, pids, records) = observation_retry_fixture();
    for node in 0..3 {
        std::fs::write(
            directory
                .path()
                .join(format!("get-reader-{}.events", pids[node].expect("PID"))),
            &records[node],
        )
        .expect("authenticated fixture");
    }
    let late_path = directory.path().join("get-reader-1004.events");
    let late_record = records[3].clone();
    let late = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1200)).await;
        tokio::fs::write(late_path, late_record).await.expect("late fixture");
    });
    verify_selected_get_observations(&observation, &progress, &pids)
        .await
        .expect("all four authenticated events must be observed");
    late.await.expect("late writer");
    assert!(
        observation.read_attempts.load(Ordering::Relaxed) < 48,
        "confirmed nodes must not consume retries"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn verification_keeps_budget_for_multiple_late_nodes() {
    let (directory, observation, progress, pids, records) = observation_retry_fixture();
    let root = directory.path().to_path_buf();
    let late_pids = pids.clone();
    let late = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1200)).await;
        for (pid, record) in late_pids.into_iter().zip(records) {
            let path = root.join(format!("get-reader-{}.events", pid.expect("PID")));
            tokio::fs::write(path, record).await.expect("late fixture");
        }
    });
    verify_selected_get_observations(&observation, &progress, &pids)
        .await
        .expect("late authenticated events must retain retry budget");
    late.await.expect("late writer");
    assert!(
        observation.read_attempts.load(Ordering::Relaxed) < 48,
        "pending nodes must share the retry budget"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn verification_keeps_missing_node_unknown() {
    let (directory, observation, progress, pids, records) = observation_retry_fixture();
    for node in 0..3 {
        std::fs::write(
            directory
                .path()
                .join(format!("get-reader-{}.events", pids[node].expect("PID"))),
            &records[node],
        )
        .expect("authenticated fixture");
    }
    let error = verify_selected_get_observations(&observation, &progress, &pids)
        .await
        .expect_err("missing fourth capture must remain unknown");
    assert!(error.to_string().contains("unknown"));
    assert!(observation.read_attempts.load(Ordering::Relaxed) <= 64);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn failure_summary_keeps_confirmed_nodes_while_the_fourth_arrives_late() {
    let (directory, observation, progress, pids, records) = observation_retry_fixture();
    for node in 0..3 {
        std::fs::write(
            directory
                .path()
                .join(format!("get-reader-{}.events", pids[node].expect("PID"))),
            &records[node],
        )
        .expect("authenticated fixture");
    }
    let late_path = directory.path().join("get-reader-1004.events");
    let late_record = records[3].clone();
    let late = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1200)).await;
        tokio::fs::write(late_path, late_record).await.expect("late fixture");
    });
    let mut readers = JoinSet::<()>::new();
    let summary =
        collect_authenticated_failure(&observation, &progress, &pids, &mut readers, ClockSample::capture(progress.started)).await;
    late.await.expect("late writer");
    assert_eq!(summary["capture_completeness"], "unknown");
    for stream in summary["streams"].as_array().expect("per-node observations") {
        assert_eq!(stream["dropped"]["emitted_bytes"], 1024 * MIB);
    }
    assert!(
        observation.read_attempts.load(Ordering::Relaxed) < 48,
        "confirmed nodes must not consume retries"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn event_reader_rejects_fifos_links_and_other_special_files_without_waiting() {
    use rustix::fs::{CWD, Mode, mkfifoat};
    use std::os::unix::fs::symlink;

    let directory = tempfile::tempdir().expect("private fixture directory");
    let regular = directory.path().join("regular");
    std::fs::write(&regular, b"event\n").expect("regular fixture");
    let fifo = directory.path().join("fifo");
    mkfifoat(CWD, fifo.as_path(), Mode::from_raw_mode(0o600)).expect("FIFO fixture");
    let link = directory.path().join("link");
    symlink(&regular, &link).expect("symlink fixture");
    let directory_link = directory.path().join("directory-link");
    symlink(directory.path(), &directory_link).expect("directory symlink fixture");
    let through_directory_link = directory_link.join("regular");
    let socket = directory.path().join("socket");
    let _listener = std::os::unix::net::UnixListener::bind(&socket).expect("socket fixture");
    for path in [
        fifo,
        link,
        through_directory_link,
        socket,
        directory.path().to_path_buf(),
        PathBuf::from("/dev/null"),
    ] {
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = sender.send(read_event_bytes(&path));
        });
        assert!(
            receiver
                .recv_timeout(Duration::from_secs(1))
                .expect("event open must not block")
                .is_none(),
            "special files and symlinks must be rejected"
        );
    }
    assert_eq!(read_event_bytes(&regular).as_deref(), Some(&b"event\n"[..]));
    std::fs::write(&regular, vec![0u8; 64 * 1024 + 1]).expect("oversized fixture");
    assert!(read_event_bytes(&regular).is_none());
}

#[cfg(target_os = "linux")]
#[test]
fn event_reader_reopens_the_pinned_inode_after_path_replacement() {
    use rustix::fs::{CWD, Mode, mkfifoat};

    let directory = tempfile::tempdir().expect("private fixture directory");
    let path = directory.path().join("events");
    let moved = directory.path().join("moved");
    std::fs::write(&path, b"original\n").expect("regular fixture");
    let inspected = inspect_event_file(&path).expect("pin regular inode");
    std::fs::rename(&path, &moved).expect("replace original path");
    mkfifoat(CWD, path.as_path(), Mode::from_raw_mode(0o600)).expect("FIFO replacement");
    let mut file = reopen_inspected_regular(&inspected).expect("reopen pinned inode");
    let mut contents = Vec::new();
    file.read_to_end(&mut contents).expect("read pinned inode");
    assert_eq!(contents, b"original\n");
    assert!(read_event_bytes(&path).is_none());
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn event_reader_worker_limits_return_unknown_without_spawning() {
    let slots = Arc::new(tokio::sync::Semaphore::new(0));
    let attempts = AtomicUsize::new(0);
    assert!(
        bounded_event_bytes(PathBuf::from("unused"), &slots, &attempts)
            .await
            .is_none()
    );
    assert_eq!(attempts.load(Ordering::Relaxed), 0);
    let slots = Arc::new(tokio::sync::Semaphore::new(4));
    attempts.store(64, Ordering::Relaxed);
    assert!(
        bounded_event_bytes(PathBuf::from("unused"), &slots, &attempts)
            .await
            .is_none()
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn fifo_event_read_returns_unknown_without_blocking_the_runtime() {
    use rustix::fs::{CWD, Mode, mkfifoat};

    let directory = tempfile::tempdir().expect("private fixture directory");
    let fifo = directory.path().join("events");
    mkfifoat(CWD, fifo.as_path(), Mode::from_raw_mode(0o600)).expect("FIFO fixture");
    let slots = Arc::new(tokio::sync::Semaphore::new(4));
    let attempts = AtomicUsize::new(0);
    let result = tokio::time::timeout(Duration::from_secs(1), bounded_event_bytes(fifo, &slots, &attempts))
        .await
        .expect("FIFO lookup must not block");
    assert!(result.is_none());
    assert_eq!(slots.available_permits(), 4);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn key_delivery_waits_for_the_exact_registered_child() {
    let (sender, mut receiver) = tokio::sync::watch::channel(Vec::<u32>::new());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let waiter = tokio::spawn(async move { wait_for_registered_pid(42, &mut receiver, deadline).await });
    sender.send_modify(|pids| pids.push(42));
    assert!(waiter.await.expect("registration waiter"));
    sender.send_modify(|pids| pids.extend([43, 44, 45]));
    let mut receiver = sender.subscribe();
    assert!(!wait_for_registered_pid(99, &mut receiver, deadline).await);
}

fn verify_concurrent_read_failure_snapshot_evidence() {
    let key = b"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let mac = TagMac::new_from_slice(key).expect("fixture key");
    let progress = ConcurrentReadProgress::new(100, 4, Some(Arc::new(mac)));
    progress.start(0);
    progress.response(0, Some("LEAK-invalid-request-id"), true);
    progress.body(0);
    progress.progress(0, 100);
    progress.complete(0);
    progress.terminal(0, "success");

    let valid_id = "b8172a15-a428-4b42-9cef-1f4a7b96874e";
    progress.start(1);
    progress.response(1, Some(valid_id), true);
    progress.body(1);
    progress.progress(1, 41);
    progress.terminal(1, "failed");

    progress.start(3);
    let _busy = progress.streams[2].lock().expect("hold one stream during the snapshot");
    let snapshot = progress.failure_snapshot(Some(1), "read_failed");
    assert_eq!(snapshot["clock_scope"], "client_unsynchronized");
    assert_eq!(snapshot["failed_node"], 1);
    let start_clock = &snapshot["stage_start_clock_sample"];
    assert!(
        start_clock["monotonic_before_ms"].as_u64().expect("start sample lower bound")
            <= start_clock["monotonic_after_ms"].as_u64().expect("start sample upper bound")
    );
    assert!(!snapshot.to_string().contains("LEAK-invalid-request-id"));

    let streams = snapshot["streams"].as_array().expect("per-stream snapshots");
    assert_eq!(streams.len(), 4);
    for (stream, terminal, expected_bytes) in [(&streams[0], "success", 100), (&streams[1], "failed", 41)] {
        assert_eq!(stream["snapshot"], "available");
        assert_eq!(stream["terminal"], terminal);
        assert_eq!(stream["received_bytes"], expected_bytes);
        let times = [
            "started_ms",
            "headers_ms",
            "body_started_ms",
            "last_progress_ms",
            "terminal_ms",
        ]
        .map(|field| stream[field].as_u64().expect("observed stream time"));
        assert!(times.windows(2).all(|pair| pair[0] <= pair[1]));
        let clock = &stream["sampled_clock"];
        let before = clock["monotonic_before_ms"].as_u64().expect("sample lower bound");
        let after = clock["monotonic_after_ms"].as_u64().expect("sample upper bound");
        assert!(times[4] <= after && before <= after);
    }
    assert_eq!(streams[0]["last_stage"], "complete");
    assert!(streams[0]["request_tag"].is_null());
    assert!(
        streams[0]["completed_ms"].as_u64().expect("completion time")
            <= streams[0]["terminal_ms"].as_u64().expect("terminal time")
    );
    assert_eq!(streams[1]["last_stage"], "reading_body");
    assert!(streams[1]["completed_ms"].is_null());
    assert_eq!(
        streams[1]["request_tag"].as_str().expect("tag"),
        "636a632459fe3a2e3d23493ca06429fc88e6ebf79aa11352a1d7b39cefe401d0"
    );
    assert!(!snapshot.to_string().contains(valid_id));
    assert_eq!(streams[2]["snapshot"], "busy");
    assert_eq!(streams[2]["last_stage"], "unknown");
    assert_eq!(streams[2]["terminal"], "unknown");
    assert!(streams[2]["received_bytes"].is_null());
    assert!(streams[2]["sampled_clock"]["monotonic_before_ms"].is_number());
    assert_eq!(streams[3]["last_stage"], "waiting_headers");
    assert_eq!(streams[3]["terminal"], "unknown");
    assert!(streams[3]["request_tag"].is_null());
    assert!(streams[3]["last_progress_ms"].is_null());

    let before_epoch = UNIX_EPOCH
        .checked_sub(Duration::from_millis(1))
        .expect("represent pre-epoch time");
    assert!(unix_ms_at(before_epoch).is_none());
}

fn fixture(directory: &Path, length: u64) -> TestResult<ObjectFixture> {
    let key = format!("object-{length}");
    let path = directory.join(&key);
    let mut file = std::fs::File::create(&path)?;
    let mut rng = StdRng::seed_from_u64(length);
    let mut block = vec![0; usize::try_from(MIB)?];
    let mut remaining = length;
    let mut digest = Sha256::new();
    while remaining != 0 {
        let count = usize::try_from(remaining.min(MIB))?;
        rng.fill_bytes(&mut block[..count]);
        file.write_all(&block[..count])?;
        digest.update(&block[..count]);
        remaining -= u64::try_from(count)?;
    }
    drop(file);
    let mut reads = vec![ExpectedRead {
        range: None,
        length,
        sha256: hex_simd::encode_to_string(digest.finalize(), hex_simd::AsciiCase::Lower),
    }];
    // Unaligned ranges cross TLS frames and EC blocks; the tail detects
    // truncation and incorrect range offsets independently of a full GET.
    for (start, end) in [(17, (MIB + 31).min(length - 1)), (length.saturating_sub(65537), length - 1)] {
        let mut input = std::fs::File::open(&path)?;
        input.seek(SeekFrom::Start(start))?;
        let range_length = end - start + 1;
        let mut hasher = Sha256::new();
        let mut input = input.take(range_length);
        loop {
            let count = input.read(&mut block)?;
            if count == 0 {
                break;
            }
            hasher.update(&block[..count]);
        }
        reads.push(ExpectedRead {
            range: Some((start, end)),
            length: range_length,
            sha256: hex_simd::encode_to_string(hasher.finalize(), hex_simd::AsciiCase::Lower),
        });
    }
    Ok(ObjectFixture {
        key,
        path,
        length,
        reads,
    })
}

async fn verified_get(
    client: &Client,
    node: usize,
    bucket: &str,
    object: &ObjectFixture,
    expected: &ExpectedRead,
    concurrent: bool,
    progress: Option<&ConcurrentReadProgress>,
) -> TestResult<serde_json::Value> {
    let started = Instant::now();
    let mut received = 0u64;
    let mut stage = "response headers";
    let mut body_failed = false;
    if let Some(progress) = progress {
        progress.start(node);
    }
    let result = tokio::time::timeout(READ_TIMEOUT, async {
        let mut request = client.get_object().bucket(bucket).key(&object.key);
        if let Some((start, end)) = expected.range {
            request = request.range(format!("bytes={start}-{end}"));
        }
        let response = request.send().await;
        if let Some(progress) = progress {
            let has_headers = match &response {
                Ok(_) => true,
                Err(error) => error.raw_response().is_some(),
            };
            progress.response(node, response.request_id(), has_headers);
        }
        let mut output = response?;
        assert_eq!(output.content_length(), Some(i64::try_from(expected.length)?));
        if let Some((start, end)) = expected.range {
            assert_eq!(output.content_range(), Some(format!("bytes {start}-{end}/{}", object.length).as_str()));
        } else {
            assert_eq!(output.content_range(), None);
        }
        stage = "response body";
        if let Some(progress) = progress {
            progress.body(node);
        }
        let mut digest = Sha256::new();
        while let Some(chunk) = output.body.next().await {
            let chunk = chunk.inspect_err(|_| body_failed = true)?;
            received += u64::try_from(chunk.len())?;
            if !chunk.is_empty()
                && let Some(progress) = progress
            {
                progress.progress(node, received);
            }
            assert!(received <= expected.length, "node {node}: GET exceeded the requested length");
            digest.update(&chunk);
        }
        let actual_sha256 = hex_simd::encode_to_string(digest.finalize(), hex_simd::AsciiCase::Lower);
        assert_eq!(received, expected.length, "node {node}: truncated GET {}", object.key);
        assert_eq!(actual_sha256, expected.sha256, "node {node}: corrupt GET {}", object.key);
        if let Some(progress) = progress {
            progress.complete(node);
        }
        eprintln!(
            "TLS acceptance: GET node={node}, key={}, range={:?}, concurrent={concurrent}, bytes={received}, elapsed={:?}",
            object.key,
            expected.range,
            started.elapsed()
        );
        Ok(serde_json::json!({
            "operation": "get", "node": node, "key": object.key,
            "range": expected.range, "bytes": received, "sha256": actual_sha256,
            "concurrent": concurrent, "elapsed_ms": started.elapsed().as_secs_f64() * 1000.0,
        }))
    })
    .await;
    if body_failed {
        write_diagnostic_line(format!(
            "TLS acceptance: GET body failed: node={node}, key={}, range={:?}, concurrent={concurrent}, received={received}/{}, elapsed={:?}\n",
            object.key, expected.range, expected.length, started.elapsed()
        ))
        .await;
    }
    result.map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!(
                "TLS GET timed out: node={node}, key={}, range={:?}, concurrent={concurrent}, stage={stage}, received={received}/{}, elapsed={:?}: {error}",
                object.key, expected.range, expected.length, started.elapsed()
            ),
        )
    })?
}

/// This case always exercises the full acceptance sizes; do not substitute
/// tiny objects and count that as a large distributed-read pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn four_node_tls_large_and_range_gets_preserve_bytes() -> TestResult {
    verify_concurrent_read_failure_snapshot_evidence();
    let mode = std::env::var("RUSTFS_E2E_INTERNODE_HTTP_MODE").unwrap_or_else(|_| "http1".into());
    let protocol = match mode.as_str() {
        "http1" => "http1",
        "auto" | "unpooled" => "auto",
        _ => return Err("RUSTFS_E2E_INTERNODE_HTTP_MODE must be http1, auto or unpooled".into()),
    };
    let mut dist = DistCluster::new_stopped_with_env(
        DistLayout::FourByFour,
        &[
            ("RUSTFS_INTERNODE_HTTP_PROTOCOL", protocol),
            ("RUSTFS_SCANNER_ENABLED", "true"),
            ("RUSTFS_SCANNER_START_DELAY_SECS", "0"),
            ("RUSTFS_SCANNER_CYCLE", "1"),
            ("RUSTFS_HEAL_ENABLED", "true"),
            ("RUST_LOG", "warn"),
        ],
    )
    .await?;
    if mode == "unpooled" {
        dist.cluster.set_env("RUSTFS_INTERNODE_HTTP_POOL_MAX_IDLE_PER_HOST", "0");
    }
    let observation = configure_get_observation(&mut dist)?;
    let directory = PathBuf::from(&dist.cluster.temp_dir);
    let tls_directory = directory.join("tls");
    tls_gen::run(tls_gen::Args {
        out_dir: tls_directory.clone(),
        days: 1,
        force: false,
    })?;
    dist.cluster.enable_tls(&tls_directory).await?;
    let report_path = std::env::var_os("RUSTFS_INTERNODE_ACCEPTANCE_REPORT").map(PathBuf::from);
    if let Some(path) = &report_path
        && (path.exists() || path.with_extension("pids.json").exists())
    {
        return Err("acceptance report paths must be unused".into());
    }
    let log_directory = report_path.as_ref().map_or_else(
        || {
            std::env::var_os("RUSTFS_E2E_LOG_DIR")
                .map_or_else(|| directory.join("logs"), |root| PathBuf::from(root).join(unique_bucket("internode-tls")))
        },
        |path| path.with_extension("logs"),
    );
    std::fs::create_dir(&log_directory)?;
    dist.capture_logs_under(&log_directory)?;
    #[cfg(target_os = "linux")]
    let start_result = if let Some(observation) = &observation {
        let allowed = observation.allowed_pids.clone();
        dist.cluster
            .start_with_pid_callback(move |pid| allowed.send_modify(|pids| pids.push(pid)))
            .await
    } else {
        dist.cluster.start().await
    };
    #[cfg(not(target_os = "linux"))]
    let start_result = dist.cluster.start().await;
    start_result?;
    let clients = dist
        .clients()?
        .into_iter()
        .map(|client| Client::from_conf(client.config().to_builder().retry_config(RetryConfig::disabled()).build()))
        .collect::<Vec<_>>();
    let bucket = unique_bucket("internode-tls");
    clients[0].create_bucket().bucket(&bucket).send().await?;
    let pids = dist
        .cluster
        .nodes
        .iter()
        .map(|node| node.process.as_ref().map(std::process::Child::id))
        .collect::<Vec<_>>();
    if let Some(path) = &report_path {
        std::fs::write(path.with_extension("pids.json"), serde_json::to_vec(&pids)?)?;
    }

    let mut results = Vec::new();
    for length in [32 * 1024, 100 * MIB, 1024 * MIB] {
        let fixture_directory = directory.clone();
        let object = Arc::new(tokio::task::spawn_blocking(move || fixture(&fixture_directory, length)).await??);
        eprintln!("TLS acceptance: fixture key={}, bytes={length}", object.key);
        let started = Instant::now();
        tokio::time::timeout(
            READ_TIMEOUT,
            clients[0]
                .put_object()
                .bucket(&bucket)
                .key(&object.key)
                .body(ByteStream::from_path(&object.path).await?)
                .send(),
        )
        .await
        .map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!(
                    "TLS PUT timed out: key={}, bytes={length}, elapsed={:?}: {error}",
                    object.key,
                    started.elapsed()
                ),
            )
        })??;
        eprintln!("TLS acceptance: PUT key={}, bytes={length}, elapsed={:?}", object.key, started.elapsed());
        results.push(
            serde_json::json!({"operation": "put", "bytes": length, "elapsed_ms": started.elapsed().as_secs_f64() * 1000.0}),
        );
        if length == 32 * 1024 {
            let alternate = dist.client_with_credentials(0, &dist.cluster.access_key, &dist.cluster.secret_key)?;
            let alternate = Client::from_conf(alternate.config().to_builder().retry_config(RetryConfig::disabled()).build());
            results.push(verified_get(&alternate, 0, &bucket, &object, &object.reads[0], false, None).await?);
            let invalid = dist.client_with_credentials(0, "invalid-tls-user", "invalid-tls-secret-password")?;
            let invalid = Client::from_conf(invalid.config().to_builder().retry_config(RetryConfig::disabled()).build());
            let denied = invalid
                .list_buckets()
                .send()
                .await
                .expect_err("alternate credentials must not inherit the root identity");
            assert_eq!(denied.raw_response().map(|response| response.status().as_u16()), Some(403));
        }
        for expected in &object.reads {
            for (node, client) in clients.iter().enumerate() {
                results.push(verified_get(client, node, &bucket, &object, expected, false, None).await?);
            }
        }
        // Each reader owns a Tokio task and its cooperative budget. Polling
        // every body and synchronous digest in one parent task can let an
        // always-ready reader starve its siblings and their deadlines.
        let mut readers = JoinSet::new();
        let progress = Arc::new(ConcurrentReadProgress::new(
            object.reads[0].length,
            clients.len(),
            observation.as_ref().map(|observation| Arc::clone(&observation.mac)),
        ));
        let mut task_nodes = Vec::with_capacity(clients.len());
        for (node, client) in clients.iter().enumerate() {
            let client = client.clone();
            let bucket = bucket.clone();
            let object = Arc::clone(&object);
            let progress = Arc::clone(&progress);
            let handle = readers.spawn(async move {
                let result = verified_get(&client, node, &bucket, &object, &object.reads[0], true, Some(&progress)).await;
                progress.terminal(node, if result.is_ok() { "success" } else { "failed" });
                (node, result)
            });
            task_nodes.push((handle.id(), node));
        }
        while let Some(result) = readers.join_next().await {
            match result {
                Ok((_, Ok(result))) => results.push(result),
                Ok((node, Err(error))) => {
                    let failure_clock = ClockSample::capture(progress.started);
                    readers.abort_all();
                    progress.report_failure(Some(node), "read_failed").await;
                    if let Some(observation) = observation.as_ref().filter(|_| length == 1024 * MIB) {
                        report_authenticated_failure(observation, &progress, &pids, &mut readers, failure_clock).await;
                    } else {
                        readers.abort_all();
                    }
                    return Err(error);
                }
                Err(error) => {
                    let node = task_nodes.iter().find(|(id, _)| *id == error.id()).map(|(_, node)| *node);
                    let failure_clock = ClockSample::capture(progress.started);
                    if let Some(node) = node {
                        progress.terminal(node, if error.is_panic() { "panicked" } else { "cancelled" });
                    }
                    readers.abort_all();
                    progress.report_failure(node, "task_failed").await;
                    if let Some(observation) = observation.as_ref().filter(|_| length == 1024 * MIB) {
                        report_authenticated_failure(observation, &progress, &pids, &mut readers, failure_clock).await;
                    } else {
                        readers.abort_all();
                    }
                    return Err(error.into());
                }
            }
        }
        if length == 1024 * MIB
            && let Some(observation) = &observation
        {
            verify_selected_get_observations(observation, &progress, &pids).await?;
        }
        tokio::fs::remove_file(&object.path).await?;
    }
    for node in 0..clients.len() {
        let log = tokio::fs::read_to_string(log_directory.join(format!("node-{node}.log"))).await?;
        assert!(
            !log.contains("internode body stalled"),
            "node {node}: internode body stall during verified reads"
        );
    }
    let report = serde_json::json!({
        "schema": 1, "mode": mode, "tls_verified": true, "nodes": 4, "drives_per_node": 4,
        "source_revision": env!("RUSTFS_E2E_BUILD_COMMIT"),
        "source_dirty": env!("RUSTFS_E2E_BUILD_DIRTY"),
        "scanner_enabled": true, "heal_enabled": true,
        "scanner_progress_verified": false, "heal_progress_verified": false,
        "results": results,
    });
    if let Some(path) = report_path {
        std::fs::write(path, serde_json::to_vec_pretty(&report)?)?;
    }
    eprintln!("internode_tls_acceptance={report}");
    Ok(())
}
