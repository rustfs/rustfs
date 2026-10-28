// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

//! Bounded, opt-in GET observations for a local distributed test.
//!
//! A line only establishes that the named reader event happened. A missing or
//! truncated line is always unknown: the process may be killed, the channel
//! may be full, or the writer may fail. Reader output is not client receipt.

use hmac::{Hmac, Mac, digest::KeyInit};
use sha2::Sha256;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use zeroize::Zeroizing;

type TagMac = Hmac<Sha256>;
const OBSERVED_SIZE: usize = 1024 * 1024 * 1024;
const MAX_RECORDS: usize = 128;
const CHANNEL_CAPACITY: usize = 64;
const DIRECTORY_ENV: &str = "RUSTFS_E2E_TLS_GET_OBSERVATION_DIR";
static SINK: OnceLock<Option<Arc<Sink>>> = OnceLock::new();

struct Sink {
    mac: TagMac,
    sender: mpsc::Sender<Record>,
    attempts: AtomicUsize,
}

#[derive(Clone, Copy)]
enum Event {
    Constructed,
    Dropped,
}

struct Record {
    pid: u32,
    tag: [u8; 32],
    event: Event,
    expected: u64,
    emitted: Option<u64>,
    elapsed_ms: Option<u64>,
    unix_ms: Option<u64>,
    signature: [u8; 32],
}

impl Sink {
    fn send(&self, mut record: Record) -> bool {
        if self
            .attempts
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| (n < MAX_RECORDS).then_some(n + 1))
            .is_err()
        {
            return false;
        }
        record.pid = std::process::id();
        let digest = record_mac(&self.mac, &record).finalize().into_bytes();
        record.signature.copy_from_slice(&digest);
        self.sender.try_send(record).is_ok()
    }

    fn tag(&self, request_id: &str) -> Option<[u8; 32]> {
        let id = uuid::Uuid::parse_str(request_id).ok()?;
        let mut mac = self.mac.clone();
        mac.update(b"rustfs-get-observation-request-v1\0");
        mac.update(id.as_bytes());
        let digest = mac.finalize().into_bytes();
        let mut tag = [0; 32];
        tag.copy_from_slice(&digest);
        Some(tag)
    }
}

fn record_mac(mac: &TagMac, record: &Record) -> TagMac {
    let mut mac = mac.clone();
    mac.update(b"rustfs-get-observation-record-v1\0");
    mac.update(&record.pid.to_be_bytes());
    mac.update(&record.tag);
    mac.update(&[match record.event {
        Event::Constructed => 1,
        Event::Dropped => 2,
    }]);
    mac.update(&record.expected.to_be_bytes());
    for value in [record.emitted, record.elapsed_ms, record.unix_ms] {
        mac.update(&[u8::from(value.is_some())]);
        mac.update(&value.unwrap_or_default().to_be_bytes());
    }
    mac
}

/// Initialize once before accepting requests. Without the private directory the
/// feature is inert. The key is sent over a private local socket and never
/// placed in the process environment, a record, or diagnostic output.
pub(crate) async fn init() {
    let Some(directory) = std::env::var_os(DIRECTORY_ENV).map(PathBuf::from) else {
        let _ = SINK.set(None);
        return;
    };
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let checked_directory = directory.clone();
        std::thread::Builder::new()
            .name("e2e-get-directory-check".into())
            .spawn(move || {
                let _ = sender.send(private_directory_identity(&checked_directory));
            })
            .ok()?;
        let (directory_uid, parent_pid) = receiver.await.ok()??;
        let mut stream = tokio::net::UnixStream::connect(directory.join("key.sock")).await.ok()?;
        let peer = stream.peer_cred().ok()?;
        if !peer_is_parent(peer.uid(), peer.pid().and_then(|pid| u32::try_from(pid).ok()), directory_uid, parent_pid) {
            return None;
        }
        let mut key = Zeroizing::new([0u8; 64]);
        stream.read_exact(&mut key[..]).await.ok()?;
        if !key.iter().all(|byte| byte.is_ascii_hexdigit()) {
            return None;
        }
        let mac = TagMac::new_from_slice(&key[..]).ok()?;
        let (sender, receiver) = mpsc::channel(CHANNEL_CAPACITY);
        std::thread::Builder::new()
            .name("e2e-get-observation".into())
            .spawn(move || consume(directory, receiver))
            .ok()?;
        Some(Arc::new(Sink {
            mac,
            sender,
            attempts: AtomicUsize::new(0),
        }))
    })
    .await
    .ok()
    .flatten();
    let _ = SINK.set(result);
}

fn parent_pid(status: &str) -> Option<u32> {
    status
        .lines()
        .find_map(|line| line.strip_prefix("PPid:")?.trim().parse().ok())
}

fn peer_is_parent(uid: u32, pid: Option<u32>, expected_uid: u32, expected_pid: u32) -> bool {
    uid == expected_uid && pid == Some(expected_pid)
}

fn open_private_directory(path: &Path) -> Option<rustix::fd::OwnedFd> {
    use rustix::fs::{CWD, FileType, Mode, OFlags, ResolveFlags, fstat, openat2};
    let fd = openat2(
        CWD,
        path,
        OFlags::PATH | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
        ResolveFlags::NO_SYMLINKS,
    )
    .ok()?;
    let stat = fstat(&fd).ok()?;
    (FileType::from_raw_mode(stat.st_mode) == FileType::Directory && stat.st_mode & 0o077 == 0).then_some(fd)
}

fn private_directory_identity(path: &Path) -> Option<(u32, u32)> {
    let directory = open_private_directory(path)?;
    let uid = rustix::fs::fstat(&directory).ok()?.st_uid;
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    Some((uid, parent_pid(&status)?))
}

fn open_new_event_file(directory: &Path, pid: u32) -> Option<std::fs::File> {
    let directory = open_private_directory(directory)?;
    create_event_in_directory(&directory, pid)
}

fn create_event_in_directory(directory: &rustix::fd::OwnedFd, pid: u32) -> Option<std::fs::File> {
    use rustix::fs::{FileType, Mode, OFlags, fstat, openat};
    let name = format!("get-reader-{pid}.events");
    let file = openat(
        directory,
        name.as_str(),
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::from_raw_mode(0o600),
    )
    .ok()?;
    let stat = fstat(&file).ok()?;
    if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile || stat.st_nlink != 1 {
        return None;
    }
    Some(file.into())
}

fn consume(directory: PathBuf, mut receiver: mpsc::Receiver<Record>) {
    let Some(mut file) = open_new_event_file(&directory, std::process::id()) else {
        return;
    };
    while let Some(record) = receiver.blocking_recv() {
        let mut bytes = Vec::with_capacity(256);
        if write_record(&mut bytes, &record).is_err() || file.write_all(&bytes).is_err() {
            return;
        }
    }
}

fn write_record(writer: &mut impl Write, record: &Record) -> io::Result<()> {
    let event = match record.event {
        Event::Constructed => "constructed",
        Event::Dropped => "dropped",
    };
    let tag = hex_simd::encode_to_string(record.tag, hex_simd::AsciiCase::Lower);
    let signature = hex_simd::encode_to_string(record.signature, hex_simd::AsciiCase::Lower);
    let value = |n: Option<u64>| n.map_or_else(|| "unknown".to_string(), |n| n.to_string());
    writeln!(
        writer,
        "v1 {} {tag} {event} {} {} {} {} {signature}",
        record.pid,
        record.expected,
        value(record.emitted),
        value(record.elapsed_ms),
        value(record.unix_ms)
    )
}

fn unix_ms() -> Option<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
}

pub(crate) struct Observation {
    sink: Arc<Sink>,
    tag: [u8; 32],
    expected: u64,
    emitted: Option<u64>,
    started: Instant,
}

impl Observation {
    pub(crate) fn start(request_id: &str, expected: usize) -> Option<Self> {
        if expected != OBSERVED_SIZE {
            return None;
        }
        Self::from_sink(SINK.get()?.as_ref()?.clone(), request_id, expected)
    }

    fn from_sink(sink: Arc<Sink>, request_id: &str, expected: usize) -> Option<Self> {
        let tag = sink.tag(request_id)?;
        let expected = u64::try_from(expected).ok()?;
        let _ = sink.send(Record {
            pid: 0,
            tag,
            event: Event::Constructed,
            expected,
            emitted: None,
            elapsed_ms: Some(0),
            unix_ms: unix_ms(),
            signature: [0; 32],
        });
        Some(Self {
            sink,
            tag,
            expected,
            emitted: None,
            started: Instant::now(),
        })
    }

    pub(crate) fn finish(&mut self, emitted: usize) {
        self.emitted = u64::try_from(emitted).ok();
    }
}

impl Drop for Observation {
    fn drop(&mut self) {
        let _ = self.sink.send(Record {
            pid: 0,
            tag: self.tag,
            event: Event::Dropped,
            expected: self.expected,
            emitted: self.emitted,
            elapsed_ms: u64::try_from(self.started.elapsed().as_millis()).ok(),
            unix_ms: unix_ms(),
            signature: [0; 32],
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sink(capacity: usize) -> (Arc<Sink>, mpsc::Receiver<Record>) {
        let (sender, receiver) = mpsc::channel(capacity);
        let mac = TagMac::new_from_slice(b"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
            .expect("valid fixture key");
        (
            Arc::new(Sink {
                mac,
                sender,
                attempts: AtomicUsize::new(0),
            }),
            receiver,
        )
    }

    #[test]
    fn event_writer_rejects_existing_fifo_symlinks_and_creates_a_regular_file() {
        use rustix::fs::{CWD, Mode, mkfifoat};
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().expect("private fixture directory");
        let fifo = directory.path().join("get-reader-101.events");
        mkfifoat(CWD, fifo.as_path(), Mode::from_raw_mode(0o600)).expect("FIFO fixture");
        let owned_directory = directory.path().to_path_buf();
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = sender.send(open_new_event_file(&owned_directory, 101).is_none());
        });
        assert!(
            receiver
                .recv_timeout(Duration::from_secs(1))
                .expect("writer open must not block")
        );

        let target = directory.path().join("target");
        std::fs::write(&target, b"unchanged").expect("target fixture");
        symlink(&target, directory.path().join("get-reader-102.events")).expect("symlink fixture");
        assert!(open_new_event_file(directory.path(), 102).is_none());
        assert_eq!(std::fs::read(target).expect("target bytes"), b"unchanged");
        let directory_link = directory.path().join("directory-link");
        symlink(directory.path(), &directory_link).expect("directory symlink fixture");
        assert!(open_new_event_file(&directory_link, 104).is_none());
        let file = open_new_event_file(directory.path(), 103).expect("new event file");
        assert!(file.metadata().expect("event metadata").is_file());
    }

    #[test]
    fn event_writer_remains_in_the_pinned_directory_after_path_replacement() {
        use std::os::unix::fs::DirBuilderExt;

        let root = tempfile::tempdir().expect("private fixture root");
        let directory = root.path().join("events");
        let moved = root.path().join("moved");
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&directory)
            .expect("event directory");
        let pinned = open_private_directory(&directory).expect("pin event directory");
        std::fs::rename(&directory, &moved).expect("move pinned directory");
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&directory)
            .expect("replacement directory");
        let file = create_event_in_directory(&pinned, 123).expect("write in pinned directory");
        assert!(file.metadata().expect("event metadata").is_file());
        assert!(moved.join("get-reader-123.events").is_file());
        assert!(!directory.join("get-reader-123.events").exists());
    }

    #[test]
    fn parent_peer_requires_both_kernel_pid_and_uid() {
        assert!(peer_is_parent(1000, Some(41), 1000, 41));
        assert!(!peer_is_parent(1000, Some(42), 1000, 41));
        assert!(!peer_is_parent(1000, None, 1000, 41));
        assert!(!peer_is_parent(1001, Some(41), 1000, 41));
        assert_eq!(parent_pid("Name:\trustfs\nPPid:\t41\n"), Some(41));
        assert_eq!(parent_pid("Name:\trustfs\n"), None);
    }

    #[tokio::test]
    async fn parent_check_reads_kernel_socket_credentials() {
        let (first, _second) = tokio::net::UnixStream::pair().expect("local socket pair");
        let peer = first.peer_cred().expect("peer credentials");
        let pid = peer.pid().and_then(|pid| u32::try_from(pid).ok());
        let own_pid = std::process::id();
        assert!(peer_is_parent(peer.uid(), pid, peer.uid(), own_pid));
        assert!(!peer_is_parent(peer.uid(), pid, peer.uid(), own_pid.saturating_add(1)));
    }

    #[test]
    fn authenticated_record_matches_independent_fixture() {
        let (sink, _receiver) = sink(1);
        let tag = hex_simd::decode_to_vec("636a632459fe3a2e3d23493ca06429fc88e6ebf79aa11352a1d7b39cefe401d0")
            .expect("fixture tag")
            .try_into()
            .expect("32-byte tag");
        let record = Record {
            pid: 4242,
            tag,
            event: Event::Constructed,
            expected: 1024 * 1024 * 1024,
            emitted: None,
            elapsed_ms: Some(0),
            unix_ms: Some(1_700_000_000_000),
            signature: [0; 32],
        };
        let digest = record_mac(&sink.mac, &record).finalize().into_bytes();
        assert_eq!(
            hex_simd::encode_to_string(digest, hex_simd::AsciiCase::Lower),
            "d19307fd28aa41b5177b014ee5d725fb19a20523c54b985c33cd3eccdced8bda"
        );
    }

    #[test]
    fn records_are_redacted_and_disconnected_receiver_is_nonfatal() {
        let (sink, mut receiver) = sink(4);
        let id = "b8172a15-a428-4b42-9cef-1f4a7b96874e";
        let mut observation = Observation::from_sink(sink.clone(), id, 1024).expect("valid UUID");
        observation.finish(42);
        drop(observation);
        let first = receiver.try_recv().expect("constructed");
        let second = receiver.try_recv().expect("dropped");
        assert_eq!(first.tag, second.tag);
        assert_eq!(second.emitted, Some(42));
        let mut bytes = Vec::new();
        write_record(&mut bytes, &first).expect("write constructed");
        write_record(&mut bytes, &second).expect("write dropped");
        let text = String::from_utf8(bytes).expect("ASCII record");
        assert!(!text.contains(id));
        assert!(!text.contains("0123456789abcdef"));
        assert!(text.contains("636a632459fe3a2e3d23493ca06429fc88e6ebf79aa11352a1d7b39cefe401d0"));
        assert!(Observation::from_sink(sink.clone(), "bad request id", 1024).is_none());
        drop(receiver);
        let mut observation = Observation::from_sink(sink, id, 1024).expect("closed sink is nonfatal");
        observation.finish(5);
    }

    #[test]
    fn full_queue_and_panic_leave_missing_values_unknown() {
        let (sink, mut receiver) = sink(1);
        let id = "b8172a15-a428-4b42-9cef-1f4a7b96874e";
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _observation = Observation::from_sink(sink, id, 1024).expect("valid UUID");
            panic!("fixture");
        }));
        assert!(result.is_err());
        assert!(matches!(receiver.try_recv().expect("constructed").event, Event::Constructed));
        assert!(receiver.try_recv().is_err(), "dropped event was lost when the queue was full");
    }

    #[test]
    fn unwind_drop_does_not_claim_a_byte_count() {
        let (sink, mut receiver) = sink(2);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _observation = Observation::from_sink(sink, "b8172a15-a428-4b42-9cef-1f4a7b96874e", 1024).expect("valid UUID");
            panic!("fixture");
        }));
        assert!(result.is_err());
        let _constructed = receiver.try_recv().expect("constructed");
        let dropped = receiver.try_recv().expect("drop during unwind");
        assert!(matches!(dropped.event, Event::Dropped));
        assert!(dropped.emitted.is_none());
    }

    #[test]
    fn attempted_records_are_capped_even_with_a_live_receiver() {
        let (sink, mut receiver) = sink(MAX_RECORDS + 1);
        for _ in 0..MAX_RECORDS + 1 {
            let _ = sink.send(Record {
                pid: 0,
                tag: [0; 32],
                event: Event::Constructed,
                expected: 1,
                emitted: None,
                elapsed_ms: None,
                unix_ms: None,
                signature: [0; 32],
            });
        }
        for _ in 0..MAX_RECORDS {
            receiver.try_recv().expect("within the cap");
        }
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn writer_failure_is_reported_without_record_contents() {
        struct FailingWriter;
        impl Write for FailingWriter {
            fn write(&mut self, _bytes: &[u8]) -> io::Result<usize> {
                Err(io::Error::other("injected writer failure"))
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let (sink, mut receiver) = sink(1);
        let _guard = Observation::from_sink(sink, "b8172a15-a428-4b42-9cef-1f4a7b96874e", 1024).expect("valid UUID");
        let record = receiver.try_recv().expect("constructed");
        assert!(write_record(&mut FailingWriter, &record).is_err());
    }
}
