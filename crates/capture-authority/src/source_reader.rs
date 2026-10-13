//! Same-UID local reads from an owner's live native store. No ACK or cursor writes.
use crate::{EventCursor, EventPage, NativeLogWriter, NativeWriteError, NodeIdentity, StoreError};
use redb::ReadableDatabase;
use serde::{Deserialize, Serialize};
use std::{
    fs, io,
    os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    sync::{OwnedSemaphorePermit, watch},
    task::JoinSet,
};

pub const WIRE_VERSION: u16 = 1;
pub const MAX_REQUEST_BYTES: usize = 4096;
pub const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_CONNECTIONS: usize = 4;
pub const IO_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadPage {
    pub version: u16,
    pub expected_cluster: [u8; 16],
    pub cursor: Option<EventCursor>,
    pub limit: usize,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadPageResponse {
    pub version: u16,
    pub identity: NodeIdentity,
    pub page: EventPage,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Rejection {
    InvalidRequest,
    WrongCluster,
    InvalidCursor,
    StoreUnavailable,
    ResponseLimit,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", deny_unknown_fields)]
pub enum ReadPageReply {
    Page { response: ReadPageResponse },
    Rejected { version: u16, error: Rejection },
}
#[derive(Debug, thiserror::Error)]
pub enum ReaderError {
    #[error("SOURCE_READER_IO")]
    Io(#[from] io::Error),
    #[error("SOURCE_READER_PERMISSION")]
    Permission,
    #[error("SOURCE_READER_PATH_OCCUPIED")]
    Occupied,
    #[error("SOURCE_READER_PATH_CHANGED")]
    PathChanged,
    #[error("SOURCE_READER_TIMEOUT")]
    Timeout,
    #[error("SOURCE_READER_PROTOCOL")]
    Protocol,
    #[error("SOURCE_READER_REJECTED: {0:?}")]
    Rejected(Rejection),
    #[error("SOURCE_READER_WORKER")]
    Worker(#[source] tokio::task::JoinError),
}

impl crate::Store {
    fn durable_source_identity(&self) -> Result<NodeIdentity, StoreError> {
        let tx = self.database.begin_read()?;
        let meta = tx.open_table(crate::META)?;
        let identity = meta.get("identity")?.ok_or(StoreError::CorruptRecord)?;
        crate::decode_identity(identity.value())
    }
}
impl NativeLogWriter {
    /// Read the validated durable identity, never an owner-supplied cluster label.
    pub async fn durable_identity(&self) -> Result<NodeIdentity, NativeWriteError> {
        let owner = self.store.clone();
        tokio::task::spawn_blocking(move || {
            owner
                .lock()
                .map_err(|_| NativeWriteError::Poisoned)?
                .durable_source_identity()
                .map_err(NativeWriteError::Store)
        })
        .await
        .map_err(NativeWriteError::Worker)?
    }
}

fn uid() -> u32 {
    rustix::process::geteuid().as_raw()
}
fn fingerprint(meta: &fs::Metadata) -> (u64, u64) {
    (meta.dev(), meta.ino())
}
fn private_parent(path: &Path) -> Result<(PathBuf, (u64, u64)), ReaderError> {
    if !path.is_absolute() || path.file_name().is_none() {
        return Err(ReaderError::Permission);
    }
    let parent = path.parent().ok_or(ReaderError::Permission)?;
    let meta = fs::symlink_metadata(parent)?;
    if !meta.is_dir() || meta.uid() != uid() || meta.mode() & 0o077 != 0 {
        return Err(ReaderError::Permission);
    }
    let resolved = fs::canonicalize(parent)?;
    let resolved_meta = fs::symlink_metadata(&resolved)?;
    if fingerprint(&meta) != fingerprint(&resolved_meta) {
        return Err(ReaderError::PathChanged);
    }
    Ok((resolved.join(path.file_name().ok_or(ReaderError::Permission)?), fingerprint(&meta)))
}
fn owned_socket(path: &Path) -> Result<fs::Metadata, ReaderError> {
    let meta = fs::symlink_metadata(path)?;
    if !meta.file_type().is_socket() || meta.uid() != uid() {
        return Err(ReaderError::Permission);
    }
    Ok(meta)
}
fn socket_metadata(path: &Path) -> Result<(u64, u64), ReaderError> {
    let meta = owned_socket(path)?;
    if meta.mode() & 0o077 != 0 {
        return Err(ReaderError::Permission);
    }
    Ok(fingerprint(&meta))
}
fn peer(stream: &UnixStream) -> Result<(), ReaderError> {
    if stream.peer_cred()?.uid() != uid() {
        return Err(ReaderError::Permission);
    }
    Ok(())
}
struct SocketPath {
    path: PathBuf,
    inode: Option<(u64, u64)>,
    parent: (u64, u64),
}
impl SocketPath {
    fn remove(&mut self) -> Result<(), ReaderError> {
        // One cleanup attempt only: never let Drop re-evaluate a later namespace.
        let Some(inode) = self.inode.take() else {
            return Ok(());
        };
        let (_, parent) = private_parent(&self.path)?;
        if parent != self.parent || fingerprint(&owned_socket(&self.path)?) != inode {
            return Err(ReaderError::PathChanged);
        }
        fs::remove_file(&self.path)?;
        Ok(())
    }
}
impl Drop for SocketPath {
    fn drop(&mut self) {
        // Explicit shutdown reports cleanup failure; cancellation preserves detected replacements.
        let _ = self.remove();
    }
}

/// Signal shutdown, then await the owner's `serve` task to drain workers and clean up.
#[derive(Clone)]
pub struct Shutdown(watch::Sender<bool>);
impl Shutdown {
    pub fn shutdown(&self) {
        let _ = self.0.send(true);
    }
}
pub struct SourceReaderServer {
    listener: UnixListener,
    writer: NativeLogWriter,
    socket: SocketPath,
    shutdown: watch::Receiver<bool>,
}
impl SourceReaderServer {
    pub fn bind(path: impl AsRef<Path>, writer: NativeLogWriter) -> Result<(Self, Shutdown), ReaderError> {
        let (path, parent) = private_parent(path.as_ref())?;
        match fs::symlink_metadata(&path) {
            Ok(_) => return Err(ReaderError::Occupied),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        // The private parent prevents a foreign UID from observing the initial socket mode.
        let listener = UnixListener::bind(&path)?;
        let inode = fingerprint(&fs::symlink_metadata(&path)?);
        let socket = SocketPath {
            path,
            inode: Some(inode),
            parent,
        };
        fs::set_permissions(&socket.path, fs::Permissions::from_mode(0o600))?;
        if private_parent(&socket.path)?.1 != parent || socket_metadata(&socket.path)? != inode {
            return Err(ReaderError::PathChanged);
        }
        let (sender, shutdown) = watch::channel(false);
        Ok((
            Self {
                listener,
                writer,
                socket,
                shutdown,
            },
            Shutdown(sender),
        ))
    }
    pub async fn serve(mut self) -> Result<(), ReaderError> {
        let mut tasks = JoinSet::new();
        let outcome = async {
            loop {
                if *self.shutdown.borrow() {
                    break;
                }
                // Admission is shared by every clone of this writer, including re-bound services.
                let admission = self.writer.source_reader_admission.clone();
                let permit = tokio::select! {
                    result = admission.acquire_owned() => result.map_err(|_| ReaderError::Protocol)?,
                    _ = self.shutdown.changed() => break,
                };
                let stream = tokio::select! {
                    result = self.listener.accept() => result?.0,
                    _ = self.shutdown.changed() => break,
                };
                if peer(&stream).is_err() {
                    continue;
                }
                let writer = self.writer.clone();
                tasks.spawn(async move {
                    let _ = connection(stream, writer, permit).await;
                });
                while let Some(result) = tasks.try_join_next() {
                    result.map_err(ReaderError::Worker)?;
                }
            }
            Ok(())
        }
        .await;
        // Ordinary shutdown drains connections and blocking workers, never aborts storage work.
        while let Some(result) = tasks.join_next().await {
            result.map_err(ReaderError::Worker)?;
        }
        drop(self.listener);
        self.socket.remove()?;
        outcome
    }
}

fn reject(error: Rejection) -> ReadPageReply {
    ReadPageReply::Rejected {
        version: WIRE_VERSION,
        error,
    }
}
fn read_page(writer: &NativeLogWriter, request: ReadPage) -> ReadPageReply {
    if request.version != WIRE_VERSION
        || request.expected_cluster == [0; 16]
        || request.limit == 0
        || request.limit > crate::MAX_EVENT_PAGE
    {
        return reject(Rejection::InvalidRequest);
    }
    // Lock order: connection admission, then Store. Native reads take no writer-ordering lock.
    let read = || -> Result<ReadPageReply, NativeWriteError> {
        let store = writer.store.lock().map_err(|_| NativeWriteError::Poisoned)?;
        let identity = store.durable_source_identity()?;
        if identity.cluster_id != request.expected_cluster {
            return Ok(reject(Rejection::WrongCluster));
        }
        let page = match store.created_events_page(request.cursor.as_ref(), request.limit) {
            Ok(page) => page,
            Err(StoreError::IdentityMismatch) => return Ok(reject(Rejection::InvalidCursor)),
            Err(error) => return Err(error.into()),
        };
        Ok(ReadPageReply::Page {
            response: ReadPageResponse {
                version: WIRE_VERSION,
                identity,
                page,
            },
        })
    };
    read().unwrap_or_else(|_| reject(Rejection::StoreUnavailable))
}

struct BoundedBytes {
    bytes: Vec<u8>,
    limit: usize,
}
impl io::Write for BoundedBytes {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(io::Error::other("source reader frame limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn encode(value: &impl Serialize, limit: usize) -> Result<Vec<u8>, ReaderError> {
    let mut buffer = BoundedBytes {
        bytes: Vec::new(),
        limit,
    };
    serde_json::to_writer(&mut buffer, value).map_err(|_| ReaderError::Protocol)?;
    Ok(buffer.bytes)
}
async fn frame_read(stream: &mut UnixStream, limit: usize) -> Result<Vec<u8>, ReaderError> {
    let len = usize::try_from(stream.read_u32().await?).map_err(|_| ReaderError::Protocol)?;
    if len == 0 || len > limit {
        return Err(ReaderError::Protocol);
    }
    let mut bytes = vec![0; len];
    stream.read_exact(&mut bytes).await?;
    Ok(bytes)
}
async fn frame_write(stream: &mut UnixStream, bytes: &[u8]) -> Result<(), ReaderError> {
    stream
        .write_u32(u32::try_from(bytes.len()).map_err(|_| ReaderError::Protocol)?)
        .await?;
    stream.write_all(bytes).await?;
    Ok(())
}
async fn connection(mut stream: UnixStream, writer: NativeLogWriter, permit: OwnedSemaphorePermit) -> Result<(), ReaderError> {
    let bytes = tokio::time::timeout(IO_TIMEOUT, frame_read(&mut stream, MAX_REQUEST_BYTES))
        .await
        .map_err(|_| ReaderError::Timeout)??;
    let request: ReadPage = match serde_json::from_slice(&bytes) {
        Ok(request) => request,
        Err(_) => {
            let bytes = encode(&reject(Rejection::InvalidRequest), MAX_RESPONSE_BYTES)?;
            return tokio::time::timeout(IO_TIMEOUT, frame_write(&mut stream, &bytes))
                .await
                .map_err(|_| ReaderError::Timeout)?;
        }
    };
    // The blocking worker owns admission until read + bounded serialization finish, even
    // if a deadline, shutdown or a dropped serve task cancels its awaiting connection.
    let (bytes, _permit) = tokio::task::spawn_blocking(move || {
        let reply = read_page(&writer, request);
        let bytes = encode(&reply, MAX_RESPONSE_BYTES).or_else(|_| encode(&reject(Rejection::ResponseLimit), MAX_RESPONSE_BYTES));
        (bytes, permit)
    })
    .await
    .map_err(ReaderError::Worker)?;
    tokio::time::timeout(IO_TIMEOUT, frame_write(&mut stream, &bytes?))
        .await
        .map_err(|_| ReaderError::Timeout)?
}

/// Connect only to a private socket owned by the effective UID, verified at both ends.
#[derive(Clone)]
pub struct SourceReaderClient {
    path: PathBuf,
}
impl SourceReaderClient {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, ReaderError> {
        let (path, _) = private_parent(path.as_ref())?;
        socket_metadata(&path)?;
        Ok(Self { path })
    }
    pub async fn page(
        &self,
        expected_cluster: [u8; 16],
        cursor: Option<EventCursor>,
        limit: usize,
    ) -> Result<ReadPageResponse, ReaderError> {
        let request = ReadPage {
            version: WIRE_VERSION,
            expected_cluster,
            cursor,
            limit,
        };
        tokio::time::timeout(IO_TIMEOUT, self.page_inner(request))
            .await
            .map_err(|_| ReaderError::Timeout)?
    }
    async fn page_inner(&self, request: ReadPage) -> Result<ReadPageResponse, ReaderError> {
        let (_, parent) = private_parent(&self.path)?;
        let inode = socket_metadata(&self.path)?;
        let mut stream = UnixStream::connect(&self.path).await?;
        peer(&stream)?;
        if private_parent(&self.path)?.1 != parent || socket_metadata(&self.path)? != inode {
            return Err(ReaderError::PathChanged);
        }
        frame_write(&mut stream, &encode(&request, MAX_REQUEST_BYTES)?).await?;
        let bytes = frame_read(&mut stream, MAX_RESPONSE_BYTES).await?;
        match serde_json::from_slice::<ReadPageReply>(&bytes).map_err(|_| ReaderError::Protocol)? {
            ReadPageReply::Page { response } => {
                if response.version != WIRE_VERSION
                    || response.identity.cluster_id != request.expected_cluster
                    || response.identity.cluster_id == [0; 16]
                    || response.page.events.len() > request.limit
                    || request.limit == 0
                    || request.limit > crate::MAX_EVENT_PAGE
                {
                    return Err(ReaderError::Protocol);
                }
                let next = response
                    .page
                    .events
                    .last()
                    .map(|event| EventCursor::new(response.identity.cluster_id, event.event_id, &event.captured))
                    .or(request.cursor);
                if response.page.next != next {
                    return Err(ReaderError::Protocol);
                }
                Ok(response)
            }
            ReadPageReply::Rejected { version, error } if version == WIRE_VERSION => Err(ReaderError::Rejected(error)),
            _ => Err(ReaderError::Protocol),
        }
    }
}

#[cfg(test)]
mod cleanup_tests {
    use super::*;
    #[test]
    fn source_reader_cleanup_is_disarmed_before_rebind() {
        let directory = tempfile::tempdir().expect("private socket owner");
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).expect("private directory mode");
        let (path, parent) = private_parent(&directory.path().join("reader.sock")).expect("private path");
        let listener = std::os::unix::net::UnixListener::bind(&path).expect("first real socket");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).expect("first socket mode");
        let inode = socket_metadata(&path).expect("first socket identity");
        let mut guard = SocketPath {
            path: path.clone(),
            inode: Some(inode),
            parent,
        };
        drop(listener);
        guard.remove().expect("explicit owner cleanup");
        assert!(guard.inode.is_none(), "SOURCE_READER_CLEANUP_DISARMED");
        let listener = std::os::unix::net::UnixListener::bind(&path).expect("immediate new owner bind");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).expect("new socket mode");
        let replacement = socket_metadata(&path).expect("new owner socket");
        drop(guard);
        assert_eq!(
            socket_metadata(&path).expect("new socket preserved"),
            replacement,
            "SOURCE_READER_OLD_GUARD_PRESERVES_REBIND"
        );
        let _client = std::os::unix::net::UnixStream::connect(&path).expect("new service reachable");
        let _connection = listener.accept().expect("new owner accepts");
    }
}
