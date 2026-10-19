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

use rustfs_io_metrics::internode_metrics::INTERNODE_OPERATION_PUT_FILE_STREAM;
use rustfs_rio::{InternodeHttpError, InternodeHttpErrorKind};
use thiserror::Error;

use super::heal::{DiskError, EcstoreError};

const HEAL_DANGLING_DELETE_GRACE_MESSAGE: &str = "dangling object deletion deferred by heal grace window";

/// Custom error type for heal operations
/// This enum defines various error variants that can occur during
/// the execution of heal-related tasks, such as I/O errors, storage errors,
/// configuration errors, and specific errors related to healing operations.
#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Storage error: {0}")]
    Storage(#[from] EcstoreError),

    #[error("Disk error: {0}")]
    Disk(#[from] DiskError),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Other error: {0}")]
    Other(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Invalid checkpoint: {0}")]
    InvalidCheckpoint(String),

    #[error("Heal task not found: {task_id}")]
    TaskNotFound { task_id: String },

    #[error("Invalid heal client token")]
    InvalidClientToken,

    #[error("Heal task execution failed: {message}")]
    TaskExecutionFailed { message: String },

    #[error("Replacement failure could not be persisted: {failure}; persistence error: {persistence}")]
    ReplacementFailurePersistence {
        #[source]
        failure: Box<Error>,
        persistence: Box<Error>,
    },

    #[error("Replacement ownership conflict: {0}")]
    ReplacementOwnershipConflict(String),

    #[error("Replacement generation conflict for task {task_id}: {reason}")]
    ReplacementGenerationConflict { task_id: String, reason: String },

    #[error("Replacement target is not ready: {0}")]
    ReplacementTargetNotReady(String),

    #[error("replacement recovery retry budget exhausted")]
    ReplacementRetryBudgetExhausted,

    #[error("Dangling object cleanup deferred until UNIX time {retry_not_before}")]
    DanglingDeleteDeferred { retry_not_before: u64 },

    #[error("stale_bucket_incarnation: bucket {bucket} no longer belongs to this heal admission ({expected:?})")]
    StaleBucketIncarnation { bucket: String, expected: Option<uuid::Uuid> },

    /// The current page already exhausted its local retry budget. Retrying
    /// the enclosing bucket would replay pages whose results were counted.
    #[error("Heal listing failed for bucket {bucket}: {source}")]
    HealListingFailed {
        bucket: String,
        #[source]
        source: Box<Error>,
    },

    #[error("Bucket discovery incomplete for the requested heal scope; not all buckets could be traversed")]
    BucketDiscoveryIncomplete,

    #[error("Invalid heal type: {heal_type}")]
    InvalidHealType { heal_type: String },

    #[error("Transient heal skip: {message}")]
    TransientSkip { message: String },

    #[error("Heal task cancelled")]
    TaskCancelled,

    #[error("Heal task timeout")]
    TaskTimeout,
}

/// A specialized Result type for heal operations
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    /// Create an Other error from any error type
    pub fn other(error: impl std::fmt::Display) -> Self {
        Error::Other(error.to_string())
    }

    /// Create a transient skip error for retryable background heal checks.
    pub fn transient_skip(message: impl Into<String>) -> Self {
        Error::TransientSkip { message: message.into() }
    }

    /// Whether a heal operation can be retried without changing its inputs.
    pub(crate) fn is_recoverable_heal(&self) -> bool {
        match classify_heal_error(self) {
            // Only a typed grace window replays the task. A grace window
            // rendered into a task message or an io::Error comes from an
            // attempt that already finished; its owner (for example a durable
            // MRF responsibility) schedules the next attempt.
            HealErrorClass::DanglingGrace { .. } => {
                matches!(self, Error::DanglingDeleteDeferred { .. } | Error::Storage(_) | Error::Disk(_))
            }
            class => class.is_retryable(),
        }
    }

    /// Retry an unproven missing object only within the per-object budget.
    /// Generic task retries retain the narrower classification above.
    pub(crate) fn is_recoverable_object_heal(&self) -> bool {
        self.is_recoverable_heal() || self.is_object_absent()
    }

    /// The locked storage read found no replica of the object or version.
    pub(crate) fn is_object_absent(&self) -> bool {
        classify_heal_error(self) == HealErrorClass::Absent
    }

    pub(crate) fn dangling_delete_retry_not_before(&self) -> Option<std::time::SystemTime> {
        if let Self::DanglingDeleteDeferred { retry_not_before } = self {
            return std::time::UNIX_EPOCH.checked_add(std::time::Duration::from_secs(*retry_not_before));
        }
        let after = match self {
            Self::Storage(error) => error.dangling_delete_retry_after(),
            Self::Disk(error) => error.dangling_delete_retry_after(),
            Self::Io(error) => DiskError::io_error_dangling_delete_retry_after(error),
            _ => None,
        }?;
        std::time::SystemTime::now().checked_add(after)
    }

    pub(crate) fn is_dangling_delete_grace(&self) -> bool {
        matches!(classify_heal_error(self), HealErrorClass::DanglingGrace { .. })
    }
}

/// Verdict classes shared by every heal error consumer (backlog#2900).
///
/// One classifier replaces the per-call-site predicates that disagreed on the
/// same input. The class is decided from typed variants; display text is
/// consulted only by [`classify_error_display`] for errors whose type was
/// destroyed upstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HealErrorClass {
    /// The locked quorum read found no replica of the object or version:
    /// nothing to repair, independent of request source or options. This is
    /// a verdict, not a durable absence proof.
    Absent,
    /// Offline disk, missing volume, unmet quorum, transport failure,
    /// slow-down or cancelled RPC: the same request can succeed later.
    Transient,
    /// Lock contention or a lock/RPC timeout (non-fatal `LockError`).
    LockContention,
    /// A storage grace window; `retry_not_before` is the absolute UNIX
    /// deadline when storage reported one.
    DanglingGrace { retry_not_before: Option<u64> },
    /// Storage reports nothing to heal.
    NoHealRequired,
    /// The task was cancelled, timed out or its admission became stale: the
    /// object's state is unknown and no retry budget applies.
    Aborted,
    /// Retrying cannot help: corruption beyond parity, permission, fatal
    /// lock state or invalid input.
    Permanent,
}

impl HealErrorClass {
    /// Whether the same request can be retried unchanged.
    pub(crate) const fn is_retryable(self) -> bool {
        matches!(self, Self::Transient | Self::LockContention | Self::DanglingGrace { .. })
    }
}

/// Classify a heal error from its typed identity.
pub(crate) fn classify_heal_error(error: &Error) -> HealErrorClass {
    match error {
        Error::TaskCancelled | Error::TaskTimeout | Error::StaleBucketIncarnation { .. } => HealErrorClass::Aborted,
        Error::DanglingDeleteDeferred { retry_not_before } => HealErrorClass::DanglingGrace {
            retry_not_before: Some(*retry_not_before),
        },
        Error::ReplacementTargetNotReady(_) | Error::TransientSkip { .. } | Error::BucketDiscoveryIncomplete => {
            HealErrorClass::Transient
        }
        Error::Storage(error) => classify_storage_error(error),
        Error::Disk(error) => classify_disk_error(error),
        Error::Io(error) => classify_io_error(error),
        Error::TaskExecutionFailed { message } | Error::Other(message) => {
            classify_error_display(message).unwrap_or(HealErrorClass::Permanent)
        }
        _ => HealErrorClass::Permanent,
    }
}

/// Classify a storage error. Lock failures follow `LockError`'s own taxonomy:
/// only its fatal variants are terminal. `StorageError::is_not_found()` is
/// deliberately not used: it lumps `DiskNotFound`/`VolumeNotFound` together
/// with object absence, the conflation that once let an offline drive be
/// recorded as absent and skipped (backlog#856 / #799 B7).
pub(crate) fn classify_storage_error(error: &EcstoreError) -> HealErrorClass {
    if let EcstoreError::Lock(lock_error) = error {
        return if lock_error.is_fatal() {
            HealErrorClass::Permanent
        } else {
            HealErrorClass::LockContention
        };
    }
    if error.is_dangling_delete_grace() {
        return HealErrorClass::DanglingGrace {
            retry_not_before: dangling_deadline(error.dangling_delete_retry_after()),
        };
    }
    match error {
        EcstoreError::FileNotFound
        | EcstoreError::FileVersionNotFound
        | EcstoreError::ObjectNotFound(_, _)
        | EcstoreError::VersionNotFound(_, _, _) => HealErrorClass::Absent,
        EcstoreError::NoHealRequired => HealErrorClass::NoHealRequired,
        EcstoreError::Timeout => HealErrorClass::LockContention,
        EcstoreError::DiskNotFound
        | EcstoreError::VolumeNotFound
        | EcstoreError::FaultyDisk
        | EcstoreError::FaultyRemoteDisk
        | EcstoreError::SlowDown
        | EcstoreError::OperationCanceled
        | EcstoreError::RemoteClientUnavailable(_) => HealErrorClass::Transient,
        _ if error.is_quorum_error() => HealErrorClass::Transient,
        EcstoreError::Io(error) => classify_io_error(error),
        _ => classify_error_display(&error.to_string()).unwrap_or(HealErrorClass::Permanent),
    }
}

/// Classify a disk error. Nonblocking local CAS and replacement leases report
/// lock contention as `WouldBlock`.
pub(crate) fn classify_disk_error(error: &DiskError) -> HealErrorClass {
    if error.is_dangling_delete_grace() {
        return HealErrorClass::DanglingGrace {
            retry_not_before: dangling_deadline(error.dangling_delete_retry_after()),
        };
    }
    match error {
        DiskError::FileNotFound | DiskError::FileVersionNotFound => HealErrorClass::Absent,
        DiskError::NoHealRequired => HealErrorClass::NoHealRequired,
        DiskError::Timeout => HealErrorClass::LockContention,
        DiskError::Io(error) if error.kind() == std::io::ErrorKind::WouldBlock => HealErrorClass::LockContention,
        DiskError::DiskNotFound
        | DiskError::VolumeNotFound
        | DiskError::ErasureReadQuorum
        | DiskError::ErasureWriteQuorum
        | DiskError::SourceStalled
        | DiskError::FaultyRemoteDisk
        | DiskError::FaultyDisk
        | DiskError::RemoteClientUnavailable(_) => HealErrorClass::Transient,
        DiskError::Io(error) => classify_io_error(error),
        _ => classify_error_display(&error.to_string()).unwrap_or(HealErrorClass::Permanent),
    }
}

fn classify_io_error(error: &std::io::Error) -> HealErrorClass {
    if DiskError::io_error_is_dangling_delete_grace(error) {
        return HealErrorClass::DanglingGrace {
            retry_not_before: dangling_deadline(DiskError::io_error_dangling_delete_retry_after(error)),
        };
    }
    if is_recoverable_io_error(error) {
        return HealErrorClass::Transient;
    }
    classify_error_display(&error.to_string()).unwrap_or(HealErrorClass::Permanent)
}

/// Storage reports whole remaining seconds. Round the absolute deadline up so
/// subsecond grace boundaries cannot hot-loop.
fn dangling_deadline(retry_after: Option<std::time::Duration>) -> Option<u64> {
    std::time::SystemTime::now()
        .checked_add(retry_after?)?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_secs()
        .checked_add(1)
}

fn is_recoverable_io_error(error: &std::io::Error) -> bool {
    // ObjectIO preserves store errors as IO sources; their quorum identity must survive this boundary.
    if error
        .get_ref()
        .and_then(|source| source.downcast_ref::<EcstoreError>())
        .is_some_and(EcstoreError::is_quorum_error)
    {
        return true;
    }
    // A peer restart can cancel an RPC after a partial repair. Replay it within
    // the existing heal retry budget; task cancellation remains terminal.
    if DiskError::io_error_is_rpc_cancelled(error) {
        return true;
    }
    let status = error.get_ref().and_then(|source| {
        source
            .downcast_ref::<tonic::Status>()
            .or_else(|| source.source().and_then(|source| source.downcast_ref::<tonic::Status>()))
    });
    if let Some(status) = status {
        // GOAWAY can surface as Internal/Unknown; replay only when the status
        // retains the typed transport cause, within the existing heal budget.
        return status.code() == tonic::Code::Unavailable
            || (matches!(status.code(), tonic::Code::Internal | tonic::Code::Unknown)
                && std::error::Error::source(status).is_some_and(|source| source.is::<tonic::transport::Error>()));
    }
    let Some(error) = error.get_ref().and_then(|source| source.downcast_ref::<InternodeHttpError>()) else {
        return false;
    };
    // A restarting peer can return 500 after admitting a shard upload. Heal
    // can replay that write within its existing object and task retry budgets.
    // Other operations retain the transport's normal retry classification.
    error.kind().is_retryable()
        || (error.context().operation() == Some(INTERNODE_OPERATION_PUT_FILE_STREAM)
            && matches!(error.kind(), InternodeHttpErrorKind::HttpStatus(status) if matches!(status.as_u16(), 409 | 500)))
}

/// Messages that reach heal with their typed identity destroyed: an older
/// peer's display text, a local `format!` into `TaskExecutionFailed`/`Other`,
/// or a boxed `io::Error`. Every needle names the producer that emits it; when
/// a producer becomes typed end-to-end, delete its needle here. This is the
/// only place in the heal crate that classifies error text.
// RUSTFS_COMPAT_TODO(backlog-2900-heal-error-display): map typed displays back to a class once, here. Remove after every heal producer and the minimum supported peer version return typed storage errors end-to-end.
pub(crate) fn classify_error_display(message: &str) -> Option<HealErrorClass> {
    // set_disk/ops/locking.rs ns_loc lock failures, ecstore remote_locker.rs
    // and lock/src lock responses, tokio/tonic deadline and timeout rendering,
    // and LockError::QuorumNotReached, when stringified before typing.
    const LOCK_CONTENTION: [&str; 7] = [
        "failed to acquire read lock",
        "lock acquisition failed",
        "lock acquisition timeout",
        "remote lock rpc timed out",
        "deadline has elapsed",
        "timed out",
        "quorum not reached",
    ];
    // tonic transport failure, LockError::Network and io ConnectionRefused
    // rendering, StorageError::OperationCanceled rendered through task
    // messages, and set_disk/ops/heal.rs HEAL_RENAME_INCOMPLETE, the one
    // needle with no typed variant yet.
    const TRANSIENT: [&str; 5] = [
        "transport error",
        "network error",
        "connection refused",
        "operation canceled",
        "heal rename incomplete",
    ];
    // Canonical `Display` of DiskError::FileNotFound/FileVersionNotFound and
    // StorageError::ObjectNotFound/VersionNotFound, matched case-sensitively.
    const ABSENT: [&str; 4] = [
        "File not found",
        "File version not found",
        "Object not found: ",
        "Version not found: ",
    ];
    // Canonical `Display` of DiskError/StorageError::NoHealRequired and the
    // legacy heal-format wording.
    const NO_HEAL_REQUIRED: [&str; 2] = ["No heal required", "No healing is required"];

    if message.contains(HEAL_DANGLING_DELETE_GRACE_MESSAGE) {
        return Some(HealErrorClass::DanglingGrace { retry_not_before: None });
    }
    // A contention or transient needle wins over an absence phrase: misreading
    // a transient failure as absence would drop the object from repair.
    let lower = message.to_ascii_lowercase();
    if LOCK_CONTENTION.iter().any(|needle| lower.contains(needle)) {
        return Some(HealErrorClass::LockContention);
    }
    if TRANSIENT.iter().any(|needle| lower.contains(needle)) {
        return Some(HealErrorClass::Transient);
    }
    if ABSENT.iter().any(|display| message.contains(display)) {
        return Some(HealErrorClass::Absent);
    }
    if NO_HEAL_REQUIRED.iter().any(|display| message.contains(display)) {
        return Some(HealErrorClass::NoHealRequired);
    }
    None
}

impl From<Error> for std::io::Error {
    fn from(err: Error) -> Self {
        std::io::Error::other(err)
    }
}

#[cfg(test)]
mod tests {
    use super::Error;
    use crate::heal::{DiskError, EcstoreError};

    #[test]
    fn namespace_lock_quorum_is_recoverable_across_io_error_conversions() {
        for (required, achieved) in [(2, 1), (1, 0)] {
            let quorum = || EcstoreError::NamespaceLockQuorumUnavailable {
                mode: "write",
                bucket: "bucket".to_owned(),
                object: "object".to_owned(),
                required,
                achieved,
            };
            for error in [
                Error::Storage(quorum()),
                Error::Io(std::io::Error::from(quorum())),
                Error::Storage(EcstoreError::from(std::io::Error::from(quorum()))),
                Error::Disk(DiskError::from(std::io::Error::from(quorum()))),
            ] {
                assert!(
                    error.is_recoverable_object_heal(),
                    "typed lock quorum failure must remain retryable: {error:?}"
                );
            }
        }

        let message = "Namespace lock quorum unavailable for write lock on bucket/object: required 2, achieved 1";
        assert!(!Error::Io(std::io::Error::other(message)).is_recoverable_object_heal());
        let denied = EcstoreError::Lock(rustfs_lock::LockError::PermissionDenied {
            reason: message.to_owned(),
        });
        assert!(!Error::Storage(EcstoreError::from(std::io::Error::from(denied))).is_recoverable_object_heal());
    }

    #[test]
    fn cancelled_rpc_is_recoverable_across_storage_error_conversions() {
        let status = tonic::Status::cancelled("operation was canceled");
        let disk = DiskError::from(status.clone());
        let storage = EcstoreError::from(status.clone());
        let disk_storage = EcstoreError::from(DiskError::from(status.clone()));
        for error in [
            Error::Disk(disk.clone()),
            Error::Disk(disk),
            Error::Storage(storage.clone()),
            Error::Storage(storage),
            Error::Storage(disk_storage.clone()),
            Error::Storage(disk_storage),
            Error::Io(std::io::Error::from(DiskError::from(status))),
        ] {
            assert!(error.is_recoverable_heal(), "typed RPC cancellation must be recoverable: {error:?}");
        }
        for status in [
            tonic::Status::permission_denied("operation was canceled"),
            tonic::Status::unauthenticated("operation was canceled"),
            tonic::Status::invalid_argument("operation was canceled"),
            tonic::Status::internal("operation was canceled"),
        ] {
            assert!(
                !Error::Storage(EcstoreError::from(status).clone()).is_recoverable_heal(),
                "cancellation text alone must not change application error classification"
            );
        }
        assert!(!Error::Io(std::io::Error::other("operation was canceled")).is_recoverable_heal());
        assert!(!Error::TaskCancelled.is_recoverable_heal());
        assert!(!Error::TaskTimeout.is_recoverable_heal());
    }

    #[test]
    fn internode_transport_errors_keep_their_recovery_classification() {
        use rustfs_rio::{InternodeHttpErrorKind as Kind, new_test_internode_http_io_error};
        for (kind, expected) in [
            (Kind::ConnectionReset, true),
            (Kind::BodyStreamAborted, true),
            (Kind::DnsResolutionFailed, true),
            (Kind::HttpStatus(http::StatusCode::INTERNAL_SERVER_ERROR), true),
            (Kind::HttpStatus(http::StatusCode::CONFLICT), true),
            (Kind::HttpStatus(http::StatusCode::SERVICE_UNAVAILABLE), true),
            (Kind::HttpStatus(http::StatusCode::FORBIDDEN), false),
            (Kind::HttpStatus(http::StatusCode::BAD_REQUEST), false),
            (Kind::HttpStatus(http::StatusCode::NOT_FOUND), false),
            (Kind::Unknown, false),
        ] {
            for error in [
                Error::Io(new_test_internode_http_io_error(kind)),
                Error::Disk(DiskError::from(new_test_internode_http_io_error(kind))),
                Error::Storage(EcstoreError::from(DiskError::from(new_test_internode_http_io_error(kind)))),
            ] {
                assert_eq!(error.is_recoverable_heal(), expected, "{error:?}");
            }
        }
        let text = "internode http status 500 Internal Server Error: PUT /rustfs/rpc/put_file_stream_v1";
        assert!(!Error::Io(std::io::Error::other(text)).is_recoverable_heal());
        assert!(!Error::other(text).is_recoverable_heal());
        for error in [DiskError::FileCorrupt, DiskError::DiskFull, DiskError::FileAccessDenied] {
            assert!(!Error::Disk(error).is_recoverable_heal());
        }
    }

    #[tokio::test]
    async fn rpc_goaway_is_recoverable_across_storage_error_conversions() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tonic::codegen::Service;

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind HTTP/2 peer");
            let address = listener.local_addr().expect("HTTP/2 peer address");
            let (release, held) = tokio::sync::oneshot::channel::<()>();
            let peer = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.expect("accept internode channel");
                let mut preface = [0_u8; 24];
                stream.read_exact(&mut preface).await.expect("read HTTP/2 preface");
                assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
                stream
                    .write_all(&[0, 0, 0, 4, 0, 0, 0, 0, 0])
                    .await
                    .expect("send HTTP/2 settings");
                loop {
                    let mut header = [0_u8; 9];
                    stream.read_exact(&mut header).await.expect("read HTTP/2 frame header");
                    let length = (usize::from(header[0]) << 16) | (usize::from(header[1]) << 8) | usize::from(header[2]);
                    assert!(length <= 4096, "fixture frame exceeds its budget");
                    let mut payload = vec![0_u8; length];
                    stream.read_exact(&mut payload).await.expect("read HTTP/2 frame payload");
                    if header[3] == 1 {
                        // A graceful peer restart rejects the admitted stream
                        // with GOAWAY(NO_ERROR), without an application status.
                        stream
                            .write_all(&[0, 0, 8, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
                            .await
                            .expect("send HTTP/2 GOAWAY");
                        held.await.expect("hold peer until the client observes GOAWAY");
                        return;
                    }
                }
            });
            let mut channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
                .expect("fixture endpoint")
                .connect()
                .await
                .expect("connect internode channel");
            std::future::poll_fn(|cx| channel.poll_ready(cx))
                .await
                .expect("internode channel ready");
            let request = http::Request::builder()
                .uri("/rustfs.node/HealObject")
                .header(http::header::CONTENT_TYPE, "application/grpc")
                .body(tonic::body::Body::empty())
                .expect("build internode request");
            let transport = channel.call(request).await.expect_err("GOAWAY must terminate the RPC");
            let status = tonic::Status::from_error(Box::new(transport));
            release.send(()).expect("release HTTP/2 peer");
            peer.await.expect("HTTP/2 peer completed");
            assert_eq!(status.code(), tonic::Code::Internal, "{status:?}");
            assert!(
                std::error::Error::source(&status).is_some_and(|source| source.is::<tonic::transport::Error>()),
                "the real channel error must retain its typed transport source: {status:?}"
            );
            let disk = DiskError::from(status.clone());
            let storage = EcstoreError::from(status.clone());
            let disk_storage = EcstoreError::from(DiskError::from(status.clone()));
            for error in [
                Error::Disk(disk.clone()),
                Error::Disk(disk),
                Error::Storage(storage.clone()),
                Error::Storage(storage),
                Error::Storage(disk_storage.clone()),
                Error::Storage(disk_storage),
                Error::Io(std::io::Error::from(DiskError::from(status))),
            ] {
                let io = match &error {
                    Error::Disk(DiskError::Io(io)) | Error::Storage(EcstoreError::Io(io)) | Error::Io(io) => io,
                    _ => panic!("RPC conversion must retain the I/O error: {error:?}"),
                };
                let retained = io
                    .get_ref()
                    .and_then(|source| source.source())
                    .and_then(|source| source.downcast_ref::<tonic::Status>())
                    .expect("RPC conversion and cloning must retain the typed status");
                assert!(
                    std::error::Error::source(retained).is_some_and(|source| source.is::<tonic::transport::Error>()),
                    "RPC conversion and cloning must retain the typed transport source"
                );
                assert!(
                    super::classify_error_display(&error.to_string()).is_none(),
                    "GOAWAY regression must not be covered by the legacy message fallback: {error}"
                );
                assert!(error.is_recoverable_heal(), "a peer GOAWAY must remain recoverable: {error}");
            }
        })
        .await
        .expect("HTTP/2 GOAWAY fixture completed within its budget");
    }

    #[test]
    fn rpc_application_errors_do_not_become_recoverable_from_transport_text() {
        for code in [
            tonic::Code::Internal,
            tonic::Code::Unknown,
            tonic::Code::PermissionDenied,
            tonic::Code::Unauthenticated,
            tonic::Code::DataLoss,
        ] {
            let status = tonic::Status::new(code, "h2 protocol error: http2 error");
            for error in [
                Error::Disk(DiskError::from(status.clone())),
                Error::Storage(EcstoreError::from(status.clone())),
                Error::Io(std::io::Error::from(DiskError::from(status.clone()))),
            ] {
                assert!(!error.is_recoverable_heal(), "application RPC errors must stay terminal: {error:?}");
            }
        }
        assert!(!Error::Io(std::io::Error::other("h2 protocol error: http2 error")).is_recoverable_heal());
    }

    #[tokio::test]
    async fn read_http_500_is_not_a_retryable_heal_write() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind fixture");
            let address = listener.local_addr().expect("fixture address");
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.expect("accept request");
                let mut request = [0_u8; 4096];
                let mut read = 0;
                loop {
                    let count = stream.read(&mut request[read..]).await.expect("request headers");
                    assert!(count > 0, "request ended before headers");
                    read += count;
                    if request[..read].windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                        break;
                    }
                    assert!(read < request.len(), "request exceeds fixture budget");
                }
                stream
                    .write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .await
                    .expect("write response");
            });
            let error = match rustfs_rio::HttpReader::new(
                format!("http://{address}/rustfs/rpc/read_file_stream"),
                http::Method::GET,
                http::HeaderMap::new(),
                None,
            )
            .await
            {
                Ok(_) => panic!("HTTP 500 must fail the read"),
                Err(error) => Error::Storage(EcstoreError::from(DiskError::from(error))),
            };
            server.await.expect("fixture completed");
            assert!(!error.is_recoverable_heal(), "{error:?}");
        })
        .await
        .expect("fixture completed within its budget");
    }

    #[test]
    fn incomplete_target_rename_is_recoverable() {
        let task_error = Error::TaskExecutionFailed {
            message: "heal rename incomplete: 1 of 2 targets committed".to_string(),
        };
        let storage_error = Error::Storage(EcstoreError::Io(std::io::Error::other(
            "heal rename incomplete: 1 of 2 targets committed",
        )));

        assert!(task_error.is_recoverable_heal());
        assert!(storage_error.is_recoverable_heal());
    }

    #[test]
    fn offline_disk_errors_are_recoverable() {
        assert!(Error::Disk(DiskError::DiskNotFound).is_recoverable_heal());
        assert!(Error::Storage(EcstoreError::DiskNotFound).is_recoverable_heal());
        assert!(Error::Storage(EcstoreError::VolumeNotFound).is_recoverable_heal());
        assert!(Error::Storage(EcstoreError::FaultyDisk).is_recoverable_heal());
        assert!(Error::Storage(EcstoreError::FaultyRemoteDisk).is_recoverable_heal());
    }

    #[test]
    fn typed_missing_objects_retry_only_within_the_object_budget() {
        for error in [
            Error::Storage(EcstoreError::FileNotFound),
            Error::Storage(EcstoreError::FileVersionNotFound),
            Error::Storage(EcstoreError::ObjectNotFound("bucket".to_owned(), "object".to_owned())),
            Error::Storage(EcstoreError::VersionNotFound(
                "bucket".to_owned(),
                "object".to_owned(),
                "version".to_owned(),
            )),
            Error::Disk(DiskError::FileNotFound),
            Error::Disk(DiskError::FileVersionNotFound),
        ] {
            assert!(error.is_recoverable_object_heal(), "{error:?}");
            assert!(!error.is_recoverable_heal(), "missing objects must not requeue the whole task: {error:?}");
        }
        assert!(!Error::other("file not found").is_recoverable_object_heal());
    }

    #[test]
    fn task_timeout_is_terminal() {
        assert!(!Error::TaskTimeout.is_recoverable_heal());
    }

    #[test]
    fn replacement_target_restart_is_recoverable() {
        assert!(Error::ReplacementTargetNotReady("mount is restarting".to_string()).is_recoverable_heal());
    }

    #[test]
    fn lock_contention_and_transport_variants_stay_recoverable() {
        use rustfs_lock::LockError;
        for lock_err in [
            LockError::Timeout {
                resource: "bucket/object".to_string(),
                timeout: std::time::Duration::from_secs(5),
            },
            LockError::Network {
                message: "peer unreachable".to_string(),
                source: Box::new(std::io::Error::other("reset")),
            },
            LockError::Internal {
                message: "channel busy".to_string(),
            },
            LockError::AlreadyLocked {
                resource: "bucket/object".to_string(),
                owner: "node-2".to_string(),
            },
            LockError::QuorumNotReached {
                required: 3,
                achieved: 1,
            },
            LockError::InsufficientNodes {
                required: 3,
                available: 1,
            },
        ] {
            assert!(
                Error::Storage(EcstoreError::Lock(lock_err)).is_recoverable_heal(),
                "contention/transport lock failures must stay retryable"
            );
        }
    }

    #[test]
    fn fatal_lock_variants_are_terminal() {
        use rustfs_lock::LockError;
        for lock_err in [
            LockError::ResourceNotFound {
                resource: "bucket/object".to_string(),
            },
            LockError::PermissionDenied {
                reason: "acl".to_string(),
            },
            LockError::Configuration {
                message: "bad quorum config".to_string(),
            },
        ] {
            assert!(
                !Error::Storage(EcstoreError::Lock(lock_err)).is_recoverable_heal(),
                "fatal lock failures cannot be fixed by retrying"
            );
        }
    }

    #[test]
    fn classify_legacy_peer_display_not_found_is_object_level_absent() {
        // backlog#2900: a typed display that lost its type at an old peer or a
        // format! boundary converts back once, at the boundary.
        for error in [
            Error::Other("File not found".to_string()),
            Error::Other("File version not found".to_string()),
            Error::Other("Object not found: bucket/object".to_string()),
            Error::TaskExecutionFailed {
                message: "Failed to heal object bucket/object: Storage error: File not found".to_string(),
            },
        ] {
            assert!(error.is_recoverable_object_heal(), "{error:?}");
            assert!(!error.is_recoverable_heal(), "absence must not requeue the whole task: {error:?}");
        }
        for error in [
            Error::Other("file not found".to_string()),
            Error::Other("Disk not found".to_string()),
            Error::Other("Volume not found".to_string()),
        ] {
            assert!(!error.is_recoverable_object_heal(), "{error:?}");
        }
    }

    #[test]
    fn classify_disk_volume_not_found_is_recoverable_like_storage() {
        assert!(Error::Disk(DiskError::VolumeNotFound).is_recoverable_heal());
        assert!(Error::Disk(DiskError::VolumeNotFound).is_recoverable_object_heal());
    }

    #[test]
    fn classify_heal_error_table() {
        use super::{HealErrorClass as C, classify_heal_error};
        use rustfs_lock::LockError;
        let grace = "dangling object deletion deferred by heal grace window; retry_after_secs=5; grace_secs=10";
        let cases = [
            (Error::TaskCancelled, C::Aborted),
            (Error::TaskTimeout, C::Aborted),
            (
                Error::StaleBucketIncarnation {
                    bucket: "b".into(),
                    expected: None,
                },
                C::Aborted,
            ),
            (Error::Storage(EcstoreError::FileNotFound), C::Absent),
            (Error::Storage(EcstoreError::FileVersionNotFound), C::Absent),
            (Error::Storage(EcstoreError::ObjectNotFound("b".into(), "o".into())), C::Absent),
            (
                Error::Storage(EcstoreError::VersionNotFound("b".into(), "o".into(), "v".into())),
                C::Absent,
            ),
            (Error::Disk(DiskError::FileNotFound), C::Absent),
            (Error::Disk(DiskError::FileVersionNotFound), C::Absent),
            (Error::Storage(EcstoreError::DiskNotFound), C::Transient),
            (Error::Storage(EcstoreError::VolumeNotFound), C::Transient),
            (Error::Storage(EcstoreError::FaultyDisk), C::Transient),
            (Error::Storage(EcstoreError::FaultyRemoteDisk), C::Transient),
            (Error::Storage(EcstoreError::SlowDown), C::Transient),
            (Error::Storage(EcstoreError::OperationCanceled), C::Transient),
            (Error::Storage(EcstoreError::RemoteClientUnavailable("rebuild".into())), C::Transient),
            (Error::Storage(EcstoreError::ErasureReadQuorum), C::Transient),
            (Error::Storage(EcstoreError::InsufficientReadQuorum("b".into(), "o".into())), C::Transient),
            (Error::Disk(DiskError::DiskNotFound), C::Transient),
            (Error::Disk(DiskError::VolumeNotFound), C::Transient),
            (Error::Disk(DiskError::ErasureWriteQuorum), C::Transient),
            (Error::Disk(DiskError::SourceStalled), C::Transient),
            (Error::TransientSkip { message: "skip".into() }, C::Transient),
            (Error::ReplacementTargetNotReady("restarting".into()), C::Transient),
            (Error::BucketDiscoveryIncomplete, C::Transient),
            (
                Error::Storage(EcstoreError::Lock(LockError::timeout("b/o", std::time::Duration::from_secs(1)))),
                C::LockContention,
            ),
            (Error::Storage(EcstoreError::Timeout), C::LockContention),
            (Error::Disk(DiskError::Timeout), C::LockContention),
            (
                Error::Disk(DiskError::Io(std::io::Error::from(std::io::ErrorKind::WouldBlock))),
                C::LockContention,
            ),
            (Error::Other("remote lock rpc timed out".into()), C::LockContention),
            (Error::Storage(EcstoreError::Lock(LockError::permission_denied("policy"))), C::Permanent),
            (Error::Disk(DiskError::FileCorrupt), C::Permanent),
            (Error::Disk(DiskError::FileAccessDenied), C::Permanent),
            (Error::Disk(DiskError::DiskFull), C::Permanent),
            (Error::Other("boom".into()), C::Permanent),
            (Error::Config("bad".into()), C::Permanent),
            (Error::Storage(EcstoreError::NoHealRequired), C::NoHealRequired),
            (Error::Disk(DiskError::NoHealRequired), C::NoHealRequired),
            (Error::Other("No heal required".into()), C::NoHealRequired),
            (
                Error::DanglingDeleteDeferred { retry_not_before: 42 },
                C::DanglingGrace {
                    retry_not_before: Some(42),
                },
            ),
            (
                Error::TaskExecutionFailed { message: grace.into() },
                C::DanglingGrace { retry_not_before: None },
            ),
        ];
        for (error, expected) in cases {
            assert_eq!(classify_heal_error(&error), expected, "{error:?}");
        }
        assert!(matches!(
            classify_heal_error(&Error::Disk(DiskError::other(grace))),
            C::DanglingGrace { .. }
        ));
    }

    #[test]
    fn rendered_grace_window_is_grace_but_does_not_replay_the_task() {
        let grace = "dangling object deletion deferred by heal grace window; retry_after_secs=5; grace_secs=10";
        let rendered = Error::TaskExecutionFailed {
            message: format!("Failed to recreate missing object b/o: Disk error: {grace}"),
        };
        assert!(rendered.is_dangling_delete_grace());
        assert!(!rendered.is_recoverable_heal(), "the owner of a finished attempt schedules the next one");
        assert!(!rendered.is_recoverable_object_heal());
        let typed = Error::Disk(DiskError::other(grace));
        assert!(typed.is_dangling_delete_grace());
        assert!(typed.is_recoverable_heal());
        assert!(Error::DanglingDeleteDeferred { retry_not_before: 1 }.is_recoverable_heal());
    }

    #[test]
    fn classify_error_display_prefers_contention_over_absence_text() {
        use super::{HealErrorClass as C, classify_error_display};
        assert_eq!(
            classify_error_display("File not found while waiting: lock acquisition timeout"),
            Some(C::LockContention)
        );
        assert_eq!(classify_error_display("Storage error: File not found"), Some(C::Absent));
        assert_eq!(classify_error_display("Object not found: b/o"), Some(C::Absent));
        assert_eq!(classify_error_display("file not found"), None);
        assert_eq!(classify_error_display("Disk not found"), None);
        assert_eq!(classify_error_display("operation was canceled"), None);
    }

    #[test]
    fn remote_client_unavailable_is_recoverable_via_typed_variant() {
        // The detail deliberately avoids every substring needle: the typed
        // variant alone must classify these as retryable.
        let detail = "auth interceptor rebuild".to_string();
        assert!(Error::Disk(DiskError::RemoteClientUnavailable(detail.clone())).is_recoverable_heal());
        assert!(Error::Storage(EcstoreError::RemoteClientUnavailable(detail)).is_recoverable_heal());
    }
}
