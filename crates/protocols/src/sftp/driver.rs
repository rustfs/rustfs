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

//! Per-session SFTP driver: the SftpDriver struct, the russh_sftp
//! Handler trait dispatch onto operation modules, and the Drop impl
//! that aborts in-flight multipart uploads on session teardown.
//!
//! Implements SFTPv3 as defined by the SFTP Internet Draft
//! draft-ietf-secsh-filexfer-02. Later draft revisions (versions 4 to
//! 6) change the wire format for attributes and timestamps. Supporting
//! them would require a separate driver type rather than a parameter
//! on this one. The russh_sftp library this driver builds on also
//! implements version 3 only.

use super::attrs;
use super::constants::limits::S3_COPY_OBJECT_MAX_SIZE;
use super::constants::s3_error_codes;
use super::errors::{SftpError, auth_err, auth_err_unreachable, ok_status, s3_error_to_sftp};
use super::lifecycle::SessionDiag;
use super::paths::{parse_s3_path, sanitise_control_bytes};
use super::state::{HandleState, WritePhase};
use super::write::{build_write_tombstone, fstat_reported_size, rejects_excl_or_trunc_without_create, should_abort_on_drop};
use crate::common::client::s3::StorageBackend;
use crate::common::gateway::{AuthorizationError, S3Action, authorize_operation};
use crate::common::session::SessionContext;
use russh_sftp::protocol::{Attrs, Data, File, FileAttributes, Handle, Name, OpenFlags, Packet, Status, StatusCode, Version};
use s3s::dto::{AbortMultipartUploadInput, CopyObjectInput, CopySource};
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, LazyLock};
use tokio::sync::Semaphore;
use uuid::Uuid;

/// Permits available to the fire-and-forget AbortMultipartUpload tasks
/// the Drop impl spawns when a session ends with live multipart uploads.
/// Bounds the concurrent abort fan-out across the whole process so a
/// burst of session teardowns cannot detach an unbounded number of
/// background tasks. Sized at 2x available_parallelism, clamped to a
/// floor that keeps a single small server productive and a ceiling that
/// keeps memory and S3 connections under control.
///
/// Try-acquire returns immediately. If no permit is available the abort
/// is skipped and the orphaned upload_id is reclaimed by the bucket
/// AbortIncompleteMultipartUpload lifecycle rule documented in
/// OperatorDeploymentNotes.md.
const ABORT_PERMITS_FLOOR: usize = 8;
const ABORT_PERMITS_CEILING: usize = 128;
static ABORT_PERMITS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| {
    let parallelism = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(ABORT_PERMITS_FLOOR);
    let permits = (parallelism * 2).clamp(ABORT_PERMITS_FLOOR, ABORT_PERMITS_CEILING);
    Arc::new(Semaphore::new(permits))
});

/// Per-session SFTP operation handler.
pub struct SftpDriver<S: StorageBackend + Send + Sync + 'static> {
    pub(super) storage: Arc<S>,
    pub(super) session_context: SessionContext,
    /// When true, write operations (OPEN with any write flag, WRITE,
    /// REMOVE, MKDIR, RMDIR, RENAME) are rejected with PermissionDenied
    /// before any backend call runs.
    pub(super) read_only: bool,
    pub(super) handles: HashMap<String, HandleState>,
    /// S3 multipart part size in bytes. Bytes accumulate in the per-handle
    /// buffer up to this size before a part flushes. Configured per
    /// installation via RUSTFS_SFTP_PART_SIZE.
    pub(super) part_size: u64,
    /// Maximum number of simultaneously-open handles allowed in this
    /// session. allocate_handle returns Failure once the table reaches
    /// this size. Configured per installation via
    /// RUSTFS_SFTP_HANDLES_PER_SESSION.
    pub(super) handles_per_session: usize,
    /// Per-call deadline applied to every StorageBackend invocation
    /// issued through run_backend / run_backend_with_err. A backend
    /// that does not respond within this many seconds returns Failure
    /// to the client and emits a warn log naming the backend method.
    /// Configured per installation via
    /// RUSTFS_SFTP_BACKEND_OP_TIMEOUT_SECS.
    pub(super) backend_op_timeout_secs: u64,
    /// Per-handle read cache window size in bytes. read_inner fetches
    /// at most this many bytes from the backend on a cache miss and
    /// serves the next several FXP_READs from the buffer. Configured
    /// per installation via RUSTFS_SFTP_READ_CACHE_WINDOW_BYTES.
    pub(super) read_cache_window: u64,
    /// Process-wide ceiling on cumulative read cache memory across
    /// every live SFTP handle. When the projected total would breach
    /// this value, read_inner skips the populate call and returns
    /// the requested bytes from the freshly-fetched data without
    /// storing the rest. Configured per installation via
    /// RUSTFS_SFTP_READ_CACHE_TOTAL_MEM_BYTES.
    pub(super) read_cache_total_mem_limit: u64,
    /// Process-wide accumulator of live read cache memory in bytes.
    /// The Drop impl on ReadCache subtracts the live buf.capacity().
    /// The populate method subtracts the old capacity and adds the
    /// new. The Arc is cloned into every HandleState::File ReadCache
    /// so per-handle memory contributes to one shared total. The
    /// total is checked against read_cache_total_mem_limit before
    /// each populate call.
    pub(super) read_cache_in_use: Arc<AtomicU64>,
    /// Per-session activity record. Stamp on every handler entry / exit
    /// so the per-session wedge watchdog can detect SFTP-handler silence
    /// independently of russh's own keepalive and inactivity layers.
    pub(super) session_diag: Arc<SessionDiag>,
}

impl<S: StorageBackend + Send + Sync + 'static> SftpDriver<S> {
    /// Build a driver bound to the given storage backend, authenticated
    /// session, read-only flag, multipart part size, per-session handle
    /// cap, and per-call backend timeout. The handle table starts
    /// empty. Handles are allocated on OPEN and OPENDIR.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        storage: Arc<S>,
        session_context: SessionContext,
        read_only: bool,
        part_size: u64,
        handles_per_session: usize,
        backend_op_timeout_secs: u64,
        read_cache_window: u64,
        read_cache_total_mem_limit: u64,
        read_cache_in_use: Arc<AtomicU64>,
        session_diag: Arc<SessionDiag>,
    ) -> Self {
        Self {
            storage,
            session_context,
            read_only,
            handles: HashMap::new(),
            part_size,
            handles_per_session,
            backend_op_timeout_secs,
            read_cache_window,
            read_cache_total_mem_limit,
            read_cache_in_use,
            session_diag,
        }
    }

    /// Build a fresh empty read cache. An Arc to the process-wide
    /// in-use accumulator is held inside the returned ReadCache.
    /// Calls to the populate method on the returned cache, and the
    /// Drop impl on the returned cache, update the same total that
    /// read_inner checks against read_cache_total_mem_limit before
    /// each populate.
    pub(super) fn new_read_cache(&self) -> super::read_cache::ReadCache {
        super::read_cache::ReadCache::new(Arc::clone(&self.read_cache_in_use))
    }

    /// Borrow the authenticated principal's S3 access key. Each StorageBackend
    /// call needs this alongside the secret key for signing.
    pub(super) fn access_key(&self) -> &str {
        &self.session_context.principal.user_identity.credentials.access_key
    }

    /// Borrow the authenticated principal's S3 secret key. Used together with
    /// access_key for signing every backend call.
    pub(super) fn secret_key(&self) -> &str {
        &self.session_context.principal.user_identity.credentials.secret_key
    }

    /// Returns Err(PermissionDenied) when the driver is read-only,
    /// Ok(()) otherwise. PermissionDenied is the SFTPv3 status that
    /// POSIX maps to EACCES.
    pub(super) fn enforce_server_readonly(&self) -> Result<(), SftpError> {
        if self.read_only {
            tracing::warn!(
                peer = %self.session_context.source_ip,
                user = %self.session_context.principal.user_identity.credentials.access_key,
                "SFTP write rejected: server is in read-only mode"
            );
            return Err(SftpError::code(StatusCode::PermissionDenied));
        }
        Ok(())
    }

    /// Borrows the HandleState for the given id and runs the closure on it.
    /// Returns Failure if the handle is not in the table.
    pub(super) fn with_handle_ref<F, R>(&self, handle: &str, f: F) -> Result<R, SftpError>
    where
        F: FnOnce(&HandleState) -> Result<R, SftpError>,
    {
        match self.handles.get(handle) {
            Some(state) => f(state),
            None => Err(SftpError::code(StatusCode::Failure)),
        }
    }

    /// Generate a fresh UUID v4 handle, insert the given state into the
    /// per-session handle table, and return the handle string. Enforces
    /// self.handles_per_session before any UUID generation. Cap-exceeded
    /// returns Failure (SFTPv3 has no dedicated "too many handles" code).
    pub(super) fn allocate_handle(&mut self, state: HandleState) -> Result<String, SftpError> {
        if self.handles.len() >= self.handles_per_session {
            return Err(SftpError::code(StatusCode::Failure));
        }
        let id = Uuid::new_v4().to_string();
        self.handles.insert(id.clone(), state);
        Ok(id)
    }

    /// Run a StorageBackend future under the per-call deadline.
    /// Returns Ok(value) on success, Err(SftpError) on backend failure
    /// (mapped through s3_error_to_sftp), or Err(SftpError::Failure)
    /// after a warn log when the deadline elapses.
    ///
    /// Cancel-safety: tokio::time::timeout drops the in-flight backend
    /// future on Elapsed. For idempotent reads (head_object,
    /// list_objects_v2, get_object_range) cancellation is benign. For
    /// create_multipart_upload a timeout can leave an upload_id that
    /// the backend created but the client never received; the bucket's
    /// AbortIncompleteMultipartUpload lifecycle rule aborts it. For
    /// upload_part / complete_multipart_upload / abort_multipart_upload
    /// the upload_id was tombstoned in the handle table before the
    /// await, so Drop's abort path runs at session teardown.
    pub(super) async fn run_backend<F, T, E>(&self, op: &'static str, fut: F) -> Result<T, SftpError>
    where
        F: std::future::Future<Output = Result<T, E>>,
        E: std::fmt::Display,
    {
        match tokio::time::timeout(std::time::Duration::from_secs(self.backend_op_timeout_secs), fut).await {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => Err(s3_error_to_sftp(op, e)),
            Err(_elapsed) => {
                tracing::warn!(op = op, timeout_secs = self.backend_op_timeout_secs, "SFTP backend operation timed out");
                Err(SftpError::code(StatusCode::Failure))
            }
        }
    }

    /// Variant of run_backend that exposes the backend Err so the
    /// caller can branch on its category (for example to filter
    /// is_not_found_error in EXCLUDE create or HeadObject-then-list
    /// fallback paths). Timeout still maps to Err(SftpError::Failure)
    /// after a warn log; the inner Result carries the original
    /// backend success or error.
    pub(super) async fn run_backend_with_err<F, T, E>(&self, op: &'static str, fut: F) -> Result<Result<T, E>, SftpError>
    where
        F: std::future::Future<Output = Result<T, E>>,
    {
        match tokio::time::timeout(std::time::Duration::from_secs(self.backend_op_timeout_secs), fut).await {
            Ok(inner) => Ok(inner),
            Err(_elapsed) => {
                tracing::warn!(op = op, timeout_secs = self.backend_op_timeout_secs, "SFTP backend operation timed out");
                Err(SftpError::code(StatusCode::Failure))
            }
        }
    }

    /// Authorise an S3 action against the session principal and map
    /// the gateway error into an SftpError. AccessDenied surfaces as
    /// PermissionDenied (policy-rejected wire status). IamUnavailable
    /// surfaces as Failure together with a warn log naming the action
    /// and target. The S3Action's wire-name (S3Action::as_str) is the
    /// op label in the warn log.
    ///
    /// The authorize_operation call is bounded by the same per-call
    /// deadline as backend calls (RUSTFS_SFTP_BACKEND_OP_TIMEOUT_SECS).
    /// A stuck IAM call would otherwise block the SFTP request until
    /// the SSH keepalive closed the transport (~45 s). The deadline
    /// closes that gap and returns IamUnavailable to the client.
    pub(super) async fn authorize(&self, action: &S3Action, bucket: &str, key: Option<&str>) -> Result<(), SftpError> {
        let auth_fut = authorize_operation(&self.session_context, action, bucket, key);
        let outcome = match tokio::time::timeout(std::time::Duration::from_secs(self.backend_op_timeout_secs), auth_fut).await {
            Ok(inner) => inner,
            Err(_elapsed) => {
                return Err(auth_err_unreachable(action.as_str(), bucket, key));
            }
        };
        match outcome {
            Ok(()) => Ok(()),
            Err(AuthorizationError::AccessDenied) => Err(auth_err()),
            Err(AuthorizationError::IamUnavailable) => Err(auth_err_unreachable(action.as_str(), bucket, key)),
        }
    }
}

/// SFTPv3 packet dispatch. Each method on the russh_sftp Handler trait
/// corresponds to one SFTPv3 packet type defined by the SFTP Internet
/// Draft draft-ietf-secsh-filexfer-02. Methods not overridden here fall
/// through to the trait default and return SSH_FX_OP_UNSUPPORTED via the
/// unimplemented hook below.
///
/// The associated Error type is SftpError, a newtype over StatusCode.
/// Every wire response therefore carries one of the defined SFTPv3 status
/// codes and no free-form server text.
impl<S: StorageBackend + Send + Sync + 'static> russh_sftp::server::Handler for SftpDriver<S> {
    type Error = SftpError;

    /// Catch-all error for unimplemented packet types. Returns
    /// OP_UNSUPPORTED so the client reports a clean "this server does
    /// not support that operation" message.
    fn unimplemented(&self) -> Self::Error {
        SftpError::code(StatusCode::OpUnsupported)
    }

    /// SSH_FXP_INIT / SSH_FXP_VERSION exchange, SFTP Internet Draft
    /// section 4. Returns the version advertisement built from
    /// SFTP_VERSION and an empty extensions map. russh_sftp also
    /// exposes Version::new() which constructs the same struct from
    /// its own internal VERSION constant. Building the struct directly
    /// here binds the wire version to constants::protocol::SFTP_VERSION
    /// instead. Clients advertising a different version receive a
    /// warn-level log. The reply still carries SFTP_VERSION, and the
    /// client must either continue with v3 semantics or close the
    /// connection.
    #[tracing::instrument(level = "info", skip(self, _extensions), fields(version = version), err(Debug))]
    async fn init(
        &mut self,
        version: u32,
        _extensions: std::collections::HashMap<String, String>,
    ) -> Result<Version, Self::Error> {
        if version != super::constants::protocol::SFTP_VERSION {
            tracing::warn!(
                client_version = version,
                server_version = super::constants::protocol::SFTP_VERSION,
                "SFTP client advertised a non-v3 version. The reply carries v3 and the client must continue with v3 semantics or close the connection.",
            );
        }
        Ok(Version {
            version: super::constants::protocol::SFTP_VERSION,
            extensions: std::collections::HashMap::new(),
        })
    }

    /// SSH_FXP_REALPATH, SFTP Internet Draft section 6.9. Returns a single
    /// File with the resolved path and dummy attributes. Existence is not
    /// checked. REALPATH is documented as path resolution only, and
    /// returning an error for a non-existent path would also create an
    /// existence oracle for paths the principal cannot list. Input is
    /// routed through parse_s3_path, so REALPATH rejects NUL, CR, LF,
    /// traversal, and the reserved-marker characters that parse_s3_path
    /// filters. The decomposed (bucket, key) is reassembled into the
    /// absolute path returned to the client.
    #[tracing::instrument(level = "debug", skip(self), fields(id, path = %sanitise_control_bytes(&path)), err(Debug))]
    async fn realpath(&mut self, id: u32, path: String) -> Result<Name, Self::Error> {
        self.session_diag.stamp();
        let result: Result<Name, SftpError> = parse_s3_path(&path).map(|(bucket, key)| {
            let resolved = match (bucket.as_str(), key.as_deref()) {
                ("", _) => "/".to_string(),
                (b, None) => format!("/{b}"),
                (b, Some(k)) => format!("/{b}/{k}"),
            };
            Name {
                id,
                files: vec![File::dummy(resolved)],
            }
        });
        self.session_diag.stamp();
        result
    }

    /// SSH_FXP_STAT, SFTP Internet Draft section 6.8. Resolves the path
    /// through do_stat, which issues HeadBucket or HeadObject depending on
    /// whether the input addresses a bucket or an object.
    #[tracing::instrument(level = "debug", skip(self), fields(id, path = %sanitise_control_bytes(&path)), err(Debug))]
    async fn stat(&mut self, id: u32, path: String) -> Result<Attrs, Self::Error> {
        self.session_diag.stamp();
        let result = self.do_stat(&path).await.map(|attrs| Attrs { id, attrs });
        self.session_diag.stamp();
        result
    }

    /// SSH_FXP_LSTAT, SFTP Internet Draft section 6.8. Under POSIX lstat
    /// differs from stat by not following symlinks. S3 has no symlinks so
    /// the two collapse to one operation. Both call do_stat so the
    /// authorisation and path resolution rules cannot diverge.
    #[tracing::instrument(level = "debug", skip(self), fields(id, path = %sanitise_control_bytes(&path)), err(Debug))]
    async fn lstat(&mut self, id: u32, path: String) -> Result<Attrs, Self::Error> {
        self.session_diag.stamp();
        let result = self.do_stat(&path).await.map(|attrs| Attrs { id, attrs });
        self.session_diag.stamp();
        result
    }

    /// SSH_FXP_FSTAT, SFTP Internet Draft section 6.8. Returns the
    /// attributes captured at OPEN time from the handle's cache. No
    /// network call. A directory handle returns default directory
    /// attrs. FSTAT on a write handle reports a size that depends on
    /// the WritePhase. Buffering returns the current buffer length.
    /// Streaming returns (next_part_number - 1) * part_size + buffer
    /// length. Failed returns the size recorded at the most recent
    /// successful write. An unknown handle returns Failure.
    #[tracing::instrument(level = "debug", skip(self), fields(id, handle = %handle), err(Debug))]
    async fn fstat(&mut self, id: u32, handle: String) -> Result<Attrs, Self::Error> {
        self.session_diag.stamp();
        let part_size = self.part_size;
        let result = self.with_handle_ref(&handle, |state| match state {
            HandleState::File { attrs, .. } => Ok(Attrs {
                id,
                attrs: attrs.clone(),
            }),
            HandleState::Write { attrs, phase, .. } => {
                let mut reported = attrs.clone();
                let cached_size = reported.size.unwrap_or(0);
                reported.size = Some(fstat_reported_size(phase, part_size, cached_size));
                Ok(Attrs { id, attrs: reported })
            }
            HandleState::Dir(_) => Ok(Attrs {
                id,
                attrs: attrs::s3_attrs_to_sftp(0, None, true),
            }),
        });
        self.session_diag.stamp();
        result
    }

    /// SSH_FXP_OPENDIR, SFTP Internet Draft section 6.7. Allocates a
    /// directory handle. Paths that have an empty bucket construct a
    /// Root cursor without a HeadBucket or ListBucket call. The bucket
    /// listing and its IAM gate are deferred to the first READDIR.
    /// Non-root paths verify ListBucket authorisation and bucket
    /// existence (via HeadBucket) before returning the handle.
    #[tracing::instrument(level = "debug", skip(self), fields(id, path = %sanitise_control_bytes(&path)), err(Debug))]
    async fn opendir(&mut self, id: u32, path: String) -> Result<Handle, Self::Error> {
        self.session_diag.stamp();
        let result = self.opendir_inner(id, &path).await;
        self.session_diag.stamp();
        result
    }

    /// SSH_FXP_READDIR, SFTP Internet Draft section 6.7. Returns one batch
    /// of entries per call. The cursor on the handle drives the batching.
    /// EOF is signalled by returning Err(StatusCode::Eof) when the cursor
    /// is exhausted, never by an empty Name response.
    ///
    /// Eof is the spec-mandated sentinel a client sees on every cursor
    /// exhaustion, so it is normal control flow on this handler. The
    /// instrument attribute therefore omits err(Debug) and non-Eof
    /// failures are surfaced via the explicit error log below.
    #[tracing::instrument(level = "debug", skip(self), fields(id, handle = %handle))]
    async fn readdir(&mut self, id: u32, handle: String) -> Result<Name, Self::Error> {
        self.session_diag.stamp();
        let result = self.readdir_inner(id, handle.clone()).await;
        self.session_diag.stamp();
        if let Err(ref err) = result
            && !matches!(err.0, StatusCode::Eof)
        {
            tracing::error!(
                handle = %handle,
                status = ?err.0,
                "SFTP READDIR failed"
            );
        }
        result
    }

    /// SSH_FXP_OPEN, SFTP Internet Draft section 6.3. Splits the request
    /// by pflags into the read or write code path.
    ///
    /// APPEND is rejected with OpUnsupported because S3 has no append
    /// primitive: every PutObject overwrites the key in full, and there
    /// is no way to extend an existing object without re-uploading the
    /// prior bytes. A client requesting append-mode is buggy or running
    /// on a path the operator did not intend, so refusing the open is
    /// safer than silently substituting overwrite semantics.
    ///
    /// READ combined with WRITE is also OpUnsupported. The S3 single-shot
    /// PutObject path used by the write handler does not support an
    /// in-place edit cycle (download, modify, upload). Clients that need
    /// that pattern (rare for SFTP) get a clear protocol error rather
    /// than a data loss path.
    #[tracing::instrument(level = "info", skip(self, _attrs), fields(id, path = %sanitise_control_bytes(&filename), pflags = ?pflags), err(Debug))]
    async fn open(
        &mut self,
        id: u32,
        filename: String,
        pflags: OpenFlags,
        _attrs: FileAttributes,
    ) -> Result<Handle, Self::Error> {
        if pflags.contains(OpenFlags::APPEND) {
            return Err(SftpError::code(StatusCode::OpUnsupported));
        }

        // SFTPv3 draft section 6.3: SSH_FXF_EXCL and SSH_FXF_TRUNC are
        // modifiers of SSH_FXF_CREAT. Either flag without CREAT is a
        // malformed request at the protocol boundary. Rejecting here
        // avoids the ambiguity of a client that set EXCL expecting
        // create-only-if-absent semantics against a path that was never
        // created in the first place.
        if rejects_excl_or_trunc_without_create(pflags) {
            return Err(SftpError::code(StatusCode::BadMessage));
        }

        let is_write = pflags.contains(OpenFlags::WRITE);
        let is_read = pflags.contains(OpenFlags::READ);

        if is_write && is_read {
            return Err(SftpError::code(StatusCode::OpUnsupported));
        }
        if is_write {
            return self.open_write(id, &filename, pflags).await;
        }
        if is_read {
            return self.open_read(id, &filename).await;
        }

        // Neither READ nor WRITE was set. SFTPv3 does not define this
        // combination as legal so it is rejected at the boundary.
        Err(SftpError::code(StatusCode::BadMessage))
    }

    /// SSH_FXP_READ, SFTP Internet Draft section 6.4. Returns up to len
    /// bytes starting at offset, capped at MAX_READ_LEN and the cached
    /// object size. Zero-length requests are rejected with BadMessage at
    /// the boundary. Offsets at or past end-of-file return Eof without a
    /// network call.
    ///
    /// Eof is the spec-mandated sentinel a client sees on every
    /// read-past-end-of-file, so it is normal control flow on this
    /// handler. The instrument attribute therefore omits err(Debug) and
    /// non-Eof failures are surfaced via the explicit error log below.
    #[tracing::instrument(level = "debug", skip(self), fields(id, handle = %handle, offset, len))]
    async fn read(&mut self, id: u32, handle: String, offset: u64, len: u32) -> Result<Data, Self::Error> {
        self.session_diag.stamp();
        let result = self.read_inner(id, handle.clone(), offset, len).await;
        self.session_diag.stamp();
        if let Err(ref err) = result
            && !matches!(err.0, StatusCode::Eof)
        {
            tracing::error!(
                handle = %handle,
                offset,
                len,
                status = ?err.0,
                "SFTP READ failed"
            );
        }
        result
    }

    /// SSH_FXP_CLOSE, SFTP Internet Draft section 6.3. Releases the
    /// handle. Read and directory handles need no action. Write handles
    /// dispatch by WritePhase:
    ///
    /// - Buffering: single PutObject with the buffered bytes. Covers
    ///   empty files and files smaller than part_size.
    /// - Streaming: upload any final partial part, then CompleteMultipartUpload.
    ///   If the final part flush or CompleteMultipartUpload fails, issue
    ///   AbortMultipartUpload to release storage and return Failure.
    /// - Failed: AbortMultipartUpload to release the upload_id. The
    ///   client already saw the error that poisoned the handle.
    ///
    /// A missing handle is treated as Ok to tolerate clients that
    /// double-close on session teardown.
    #[tracing::instrument(level = "info", skip(self), fields(id, handle = %handle), err(Debug))]
    async fn close(&mut self, id: u32, handle: String) -> Result<Status, Self::Error> {
        self.session_diag.stamp();
        let removed = self.handles.remove(&handle);
        let Some(HandleState::Write {
            bucket,
            key,
            attrs,
            phase,
        }) = removed
        else {
            return Ok(ok_status(id));
        };

        match phase {
            WritePhase::Buffering { part_buffer } => {
                // Small-file path. No multipart state exists so nothing
                // to abort on failure.
                self.commit_write(&bucket, &key, part_buffer).await?;
            }
            WritePhase::Streaming {
                upload_id,
                abort_authorized,
                part_buffer,
                uploaded_parts,
                next_part_number,
            } => {
                // Insert a tombstone before the close_streaming await so
                // that if the future is cancelled, the Drop drain loop
                // finds the upload_id and issues AbortMultipartUpload.
                //
                // On Ok: remove the tombstone. CompleteMultipartUpload
                // has finalised the upload. A later AbortMultipartUpload
                // from Drop would return NoSuchUpload. This will be
                // logged in the Drop at debug but the tokio::spawn would
                // still run. Removing the tombstone here avoids that
                // spawn.
                //
                // On Err: keep the tombstone in place so Drop retries
                // the abort. close_streaming has already attempted its
                // own abort via close_abort_or_skip, but that attempt
                // may itself have failed (transient network error,
                // mid-call cancellation). The tombstone-before-await
                // pattern survives such abort-failure modes; removing
                // the tombstone on Err would trust the inline abort
                // unconditionally, which the tombstone exists to avoid.
                //
                // The synchronous window between the await returning Ok
                // and the remove call below contains no other await, so
                // cancellation cannot fire between them.
                self.handles.insert(
                    handle.clone(),
                    build_write_tombstone(&bucket, &key, &attrs, upload_id.clone(), abort_authorized),
                );
                let result = self
                    .close_streaming(&bucket, &key, upload_id, abort_authorized, part_buffer, uploaded_parts, next_part_number)
                    .await;
                match result {
                    Ok(()) => {
                        self.handles.remove(&handle);
                    }
                    Err(e) => return Err(e),
                }
            }
            WritePhase::Failed {
                upload_id,
                abort_authorized,
            } => {
                // Handle entered WritePhase::Failed via an earlier
                // UploadPart failure. Release the upload_id so S3 does
                // not hold partial state.
                // Error and skip paths are both log-and-continue: the
                // client already saw the write error that poisoned the
                // handle, so close itself returns Ok. Cancellation of
                // close_abort_or_skip leaves the tombstone for Drop.
                self.handles.insert(
                    handle.clone(),
                    build_write_tombstone(&bucket, &key, &attrs, upload_id.clone(), abort_authorized),
                );
                self.close_abort_or_skip(&bucket, &key, &upload_id, abort_authorized, "Failed handle")
                    .await;
                self.handles.remove(&handle);
            }
        }

        Ok(ok_status(id))
    }

    /// SSH_FXP_WRITE, SFTP Internet Draft section 6.3. Appends data to
    /// the open write handle's buffer and flushes full parts to S3 as the
    /// buffer fills.
    ///
    /// The offset must equal the current byte count: the implementation
    /// is sequential-append only, no sparse writes. Mainstream clients
    /// (OpenSSH sftp, FileZilla, WinSCP) write strictly sequentially so
    /// the restriction does not affect normal transfers. The per-handle
    /// buffer is bounded by part_size: any full-part segment flushes to
    /// S3 as soon as part_size bytes are available, so the in-memory
    /// high water mark is part_size + the incoming chunk.
    ///
    /// On the first full-part flush the handle transitions from Buffering
    /// to Streaming by issuing CreateMultipartUpload. A Failed handle
    /// rejects every subsequent write with the same status that caused
    /// the failure.
    #[tracing::instrument(level = "debug", skip(self, data), fields(id, handle = %handle, offset, len = data.len()), err(Debug))]
    async fn write(&mut self, id: u32, handle: String, offset: u64, data: Vec<u8>) -> Result<Status, Self::Error> {
        self.session_diag.stamp();
        self.enforce_server_readonly()?;

        // Remove the handle from the table so write_dispatch can mutate
        // it across an await without a live &mut into self.handles.
        // Reinsert the handle once write_dispatch returns.
        let mut state = self
            .handles
            .remove(&handle)
            .ok_or_else(|| SftpError::code(StatusCode::Failure))?;

        // If the handle enters with an active or poisoned upload, build
        // a tombstone (see build_write_tombstone for the cancellation
        // model) and insert it before the write_dispatch await so a
        // cancelled or panicking future still leaves Drop an upload_id
        // to abort. The happy path overwrites this tombstone with the
        // real state at the self.handles.insert below. For a Buffering
        // handle there is no upload_id yet;
        // write_dispatch_begin_streaming installs the tombstone itself,
        // synchronously after CreateMultipartUpload returns.
        if let HandleState::Write {
            bucket,
            key,
            attrs,
            phase:
                WritePhase::Streaming {
                    upload_id,
                    abort_authorized,
                    ..
                }
                | WritePhase::Failed {
                    upload_id,
                    abort_authorized,
                },
        } = &state
        {
            let tombstone = build_write_tombstone(bucket, key, attrs, upload_id.clone(), *abort_authorized);
            self.handles.insert(handle.clone(), tombstone);
        }

        let result = self.write_dispatch(&handle, &mut state, offset, data).await;

        self.handles.insert(handle, state);
        let mapped = result.map(|_| ok_status(id));
        self.session_diag.stamp();
        mapped
    }

    /// SSH_FXP_REMOVE, SFTP Internet Draft section 6.5. DeleteObject on a
    /// resolved object key. REMOVE on a bucket-only path returns Failure
    /// because the SFTPv3 draft scopes REMOVE to files only. Bucket
    /// deletion belongs to RMDIR.
    #[tracing::instrument(level = "info", skip(self), fields(id, path = %sanitise_control_bytes(&filename)), err(Debug))]
    async fn remove(&mut self, id: u32, filename: String) -> Result<Status, Self::Error> {
        self.enforce_server_readonly()?;

        let (bucket, key) = parse_s3_path(&filename)?;
        let Some(object_key) = key else {
            tracing::warn!(path = %sanitise_control_bytes(&filename), "SFTP REMOVE refused on a directory path");
            return Err(SftpError::code(StatusCode::Failure));
        };
        if bucket.is_empty() {
            return Err(SftpError::code(StatusCode::NoSuchFile));
        }

        self.authorize(&S3Action::DeleteObject, &bucket, Some(&object_key)).await?;

        self.run_backend(
            "delete_object",
            self.storage
                .delete_object(&bucket, &object_key, self.access_key(), self.secret_key()),
        )
        .await?;
        Ok(ok_status(id))
    }

    /// SSH_FXP_MKDIR, SFTP Internet Draft section 6.6. Bucket-level path
    /// (only the bucket component is set) issues CreateBucket. Sub-bucket
    /// path issues PutObject of a zero-byte object at the encoded
    /// directory marker key. MKDIR at the SFTP root returns Failure
    /// because there is no parent into which a new top-level entity could
    /// be added.
    ///
    /// The directory-marker key is built with rustfs_utils::path::
    /// encode_dir_object so the key format matches the convention used
    /// by the rest of RustFS (S3, Swift, WebDAV).
    #[tracing::instrument(level = "info", skip(self, _attrs), fields(id, path = %sanitise_control_bytes(&path)), err(Debug))]
    async fn mkdir(&mut self, id: u32, path: String, _attrs: FileAttributes) -> Result<Status, Self::Error> {
        self.enforce_server_readonly()?;

        let (bucket, key) = parse_s3_path(&path)?;
        if bucket.is_empty() {
            return Err(SftpError::code(StatusCode::Failure));
        }

        match key {
            None => self.mkdir_bucket(&bucket).await?,
            Some(object_key) => self.mkdir_subdir_marker(&bucket, &object_key).await?,
        }
        Ok(ok_status(id))
    }

    /// SSH_FXP_RMDIR, SFTP Internet Draft section 6.6. Empty check then
    /// delete. Bucket-level path lists the bucket with max_keys=1 and,
    /// on an empty result, calls DeleteBucket. Sub-bucket path lists the
    /// prefix and, on an empty result, calls DeleteObject on the encoded
    /// directory marker.
    ///
    /// validate_directory_empty propagates the list error rather than
    /// swallowing it. Without that, a transient backend error during
    /// the empty-check would let the destructive call proceed against
    /// an unverified target.
    #[tracing::instrument(level = "info", skip(self), fields(id, path = %sanitise_control_bytes(&path)), err(Debug))]
    async fn rmdir(&mut self, id: u32, path: String) -> Result<Status, Self::Error> {
        self.enforce_server_readonly()?;

        let (bucket, key) = parse_s3_path(&path)?;
        if bucket.is_empty() {
            return Err(SftpError::code(StatusCode::Failure));
        }

        match key {
            None => self.rmdir_bucket(&bucket).await?,
            Some(object_key) => self.rmdir_subdir_marker(&bucket, &object_key).await?,
        }
        Ok(ok_status(id))
    }

    /// SSH_FXP_RENAME, SFTP Internet Draft section 6.5. File-only:
    /// CopyObject from source to destination, then DeleteObject on the
    /// source. S3 has no native rename operation. A request whose source
    /// or destination resolves to anything other than a bucket+key pair
    /// (root, bucket-only) returns OpUnsupported because directory rename
    /// would require recursive list+copy+delete.
    ///
    /// Large files (larger than S3_COPY_OBJECT_MAX_SIZE, 5 GiB) cannot
    /// use the single-shot CopyObject API. In that case a HEAD on the
    /// source determines the size, a multipart upload is created on the
    /// destination, and the data is copied part-by-part via
    /// UploadPartCopy. If the source exceeds part_size *
    /// S3_MAX_MULTIPART_PARTS the effective part size is scaled up so
    /// any object up to the S3 maximum (5 TiB) can be renamed.
    ///
    /// Rename is multi-step and not atomic. If CopyObject (or the
    /// multipart copy) succeeds and DeleteObject fails, the destination
    /// exists and the source remains. The wire reply is Failure so the
    /// client receives the error and can retry the deletion.
    #[tracing::instrument(level = "info", skip(self), fields(id, oldpath = %sanitise_control_bytes(&oldpath), newpath = %sanitise_control_bytes(&newpath)), err(Debug))]
    async fn rename(&mut self, id: u32, oldpath: String, newpath: String) -> Result<Status, Self::Error> {
        self.enforce_server_readonly()?;

        let (src_bucket, src_key) = parse_s3_path(&oldpath)?;
        let (dst_bucket, dst_key) = parse_s3_path(&newpath)?;

        let Some(src_object) = src_key else {
            return Err(SftpError::code(StatusCode::OpUnsupported));
        };
        let Some(dst_object) = dst_key else {
            return Err(SftpError::code(StatusCode::OpUnsupported));
        };
        if src_bucket.is_empty() || dst_bucket.is_empty() {
            return Err(SftpError::code(StatusCode::OpUnsupported));
        }

        // POSIX rename on the same path is a no-op. Short-circuit
        // before any backend call because the flow below (copy then
        // delete source) would otherwise delete the object after
        // copying it to itself. For files over 5 GiB this would lose
        // data, since S3 accepts self-copy via UploadPartCopy even
        // though single-shot CopyObject rejects it.
        if src_bucket == dst_bucket && src_object == dst_object {
            return Ok(ok_status(id));
        }

        // HEAD the source to learn its size. The size drives the
        // single-shot vs multipart-copy branch below.
        self.authorize(&S3Action::HeadObject, &src_bucket, Some(&src_object)).await?;
        let head = self
            .run_backend(
                "head_object",
                self.storage
                    .head_object(&src_bucket, &src_object, self.access_key(), self.secret_key()),
            )
            .await?;
        let content_length = head.content_length.unwrap_or(0).max(0) as u64;

        // Copy branch. Single-shot CopyObject for anything up to 5 GiB.
        // Multipart UploadPartCopy above that.
        if content_length <= S3_COPY_OBJECT_MAX_SIZE {
            self.authorize(&S3Action::CopyObject, &dst_bucket, Some(&dst_object)).await?;
            let input = CopyObjectInput::builder()
                .copy_source(CopySource::Bucket {
                    bucket: src_bucket.clone().into(),
                    key: src_object.clone().into(),
                    version_id: None,
                })
                .bucket(dst_bucket.clone())
                .key(dst_object.clone())
                .build()
                .map_err(|e| s3_error_to_sftp("build_copy_object", e))?;
            self.run_backend("copy_object", self.storage.copy_object(input, self.access_key(), self.secret_key()))
                .await?;
        } else {
            self.multipart_copy(&src_bucket, &src_object, &dst_bucket, &dst_object, content_length)
                .await?;
        }

        // Remove the original. If this fails the copy already landed at
        // the destination. The client receives Failure and can retry the
        // delete separately.
        self.authorize(&S3Action::DeleteObject, &src_bucket, Some(&src_object))
            .await?;
        self.run_backend(
            "delete_object",
            self.storage
                .delete_object(&src_bucket, &src_object, self.access_key(), self.secret_key()),
        )
        .await?;

        Ok(ok_status(id))
    }

    /// SSH_FXP_SETSTAT, SFTP Internet Draft section 6.6. Returns Ok
    /// without touching the backend. S3 has no POSIX permission, owner,
    /// or mtime semantics for objects, so honouring SETSTAT would be a
    /// lie. WinSCP and rsync issue SETSTAT after every transfer to
    /// stamp mtime. Returning OpUnsupported there causes them to flag
    /// every successful upload as a transfer failure. A silent success
    /// is the only client-compatible answer.
    #[tracing::instrument(level = "debug", skip(self, _attrs), fields(id, path = %sanitise_control_bytes(&_path)), err(Debug))]
    async fn setstat(&mut self, id: u32, _path: String, _attrs: FileAttributes) -> Result<Status, Self::Error> {
        self.enforce_server_readonly()?;
        Ok(ok_status(id))
    }

    /// SSH_FXP_FSETSTAT, SFTP Internet Draft section 6.6. Same rationale
    /// as setstat: S3 cannot honour POSIX attributes, and clients use
    /// FSETSTAT during transfers to stamp the in-flight handle.
    #[tracing::instrument(level = "debug", skip(self, _attrs), fields(id, handle = %_handle), err(Debug))]
    async fn fsetstat(&mut self, id: u32, _handle: String, _attrs: FileAttributes) -> Result<Status, Self::Error> {
        self.enforce_server_readonly()?;
        Ok(ok_status(id))
    }

    /// SSH_FXP_SYMLINK, SFTP Internet Draft section 6.10. S3 has no
    /// symlink primitive and the convention of encoding a target into
    /// object metadata is non-portable across SFTP clients. Returning
    /// OpUnsupported prevents clients from creating malformed link
    /// objects that no other SFTP client can resolve.
    #[tracing::instrument(level = "debug", skip(self), fields(id = _id), err(Debug))]
    async fn symlink(&mut self, _id: u32, _linkpath: String, _targetpath: String) -> Result<Status, Self::Error> {
        Err(SftpError::code(StatusCode::OpUnsupported))
    }

    /// SSH_FXP_READLINK, SFTP Internet Draft section 6.10. S3 has no
    /// symlink primitive. Returns OpUnsupported.
    #[tracing::instrument(level = "debug", skip(self), fields(id = _id), err(Debug))]
    async fn readlink(&mut self, _id: u32, _path: String) -> Result<Name, Self::Error> {
        Err(SftpError::code(StatusCode::OpUnsupported))
    }

    /// SSH_FXP_EXTENDED, SFTP Internet Draft section 8. The server offers
    /// no extensions, so every extended request is rejected with the
    /// status the draft mandates for unknown extension names.
    #[tracing::instrument(level = "debug", skip(self, _data), fields(id = _id, request = %sanitise_control_bytes(&_request)), err(Debug))]
    async fn extended(&mut self, _id: u32, _request: String, _data: Vec<u8>) -> Result<Packet, Self::Error> {
        Err(SftpError::code(StatusCode::OpUnsupported))
    }
}

/// Abort in-flight multipart uploads when the driver is dropped.
///
/// The driver is owned by russh_sftp::server::run and dropped when the
/// SSH channel stream ends. Drop runs on every channel termination
/// path: clean client close, TCP drop, idle timeout, channel_close, or
/// panic in a handler. Write handles in the Streaming or Failed phase
/// carry an active upload_id. Without explicit abort the upload_id
/// lingers in S3, consuming storage until the bucket's lifecycle rule
/// aborts it.
///
/// Drop is synchronous. The abort calls run in a tokio task spawned
/// per active upload; the task outlives the driver. If the runtime is
/// shutting down the task may not complete, in which case the bucket's
/// AbortIncompleteMultipartUpload lifecycle rule aborts the upload_id.
///
/// Drop does not call authorize_operation directly because it cannot
/// await. The authorisation decision was cached on the Streaming
/// variant (and forwarded to Failed) at CreateMultipartUpload time;
/// see start_multipart_upload and the abort_authorized field on
/// WritePhase. When the cached flag is false, Drop skips the abort and
/// logs the skip with the bucket, key, upload_id, and principal.
/// Operators running Deny-Abort policies (WORM / append-only patterns)
/// must configure the bucket's AbortIncompleteMultipartUpload
/// lifecycle rule or staged parts accumulate.
///
/// The cached flag reflects the policy at CreateMultipartUpload time;
/// a policy edit between cache and Drop is not honoured within the
/// session. Staleness is bounded by one upload's lifetime.
impl<S: StorageBackend + Send + Sync + 'static> Drop for SftpDriver<S> {
    fn drop(&mut self) {
        // Snapshot credentials, peer IP, and the per-call backend
        // timeout before draining the handle table. self.access_key()
        // and self.secret_key() borrow self.session_context immutably,
        // which conflicts with the mutable borrow of self.handles
        // inside the loop. The timeout is copied into each spawned
        // abort task so the deadline applies uniformly to inline calls
        // and Drop-time aborts.
        let access_key = self.session_context.principal.user_identity.credentials.access_key.clone();
        let secret_key = self.session_context.principal.user_identity.credentials.secret_key.clone();
        let peer = self.session_context.source_ip;
        let backend_op_timeout_secs = self.backend_op_timeout_secs;

        for (_handle_id, handle_state) in self.handles.drain() {
            let HandleState::Write { bucket, key, phase, .. } = handle_state else {
                continue;
            };
            // should_abort_on_drop returns None for Buffering (no
            // upload exists) and for Streaming/Failed when the cached
            // abort_authorized is false (policy denies Abort).
            let upload_id_owned = match should_abort_on_drop(&phase) {
                Some(id) => id.to_owned(),
                None => {
                    if let WritePhase::Streaming { upload_id, .. } | WritePhase::Failed { upload_id, .. } = &phase {
                        tracing::warn!(
                            bucket = %bucket,
                            key = %key,
                            upload_id = %upload_id,
                            peer = %peer,
                            access_key = %access_key,
                            "skipped abort of orphaned multipart upload on session drop, principal lacks s3:AbortMultipartUpload, bucket lifecycle rules must reclaim parts",
                        );
                    }
                    continue;
                }
            };

            let storage = Arc::clone(&self.storage);
            let access_key = access_key.clone();
            let secret_key = secret_key.clone();
            let upload_id = upload_id_owned;

            // Cap the global abort fan-out so a burst of session
            // teardowns each holding live multipart uploads cannot
            // detach an unbounded number of background tasks. The
            // permit is held for the lifetime of the spawned task.
            let permit = match Arc::clone(&ABORT_PERMITS).try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    tracing::warn!(
                        bucket = %bucket,
                        key = %key,
                        upload_id = %upload_id,
                        peer = %peer,
                        "abort permit pool exhausted on session drop, bucket lifecycle rule must reclaim parts",
                    );
                    continue;
                }
            };

            tokio::spawn(async move {
                let _permit = permit;
                tracing::warn!(
                    bucket = %bucket,
                    key = %key,
                    upload_id = %upload_id,
                    peer = %peer,
                    "aborting orphaned multipart upload on session drop"
                );
                // Build AbortMultipartUploadInput inside the spawned
                // task so the builder Result is handled in async
                // context. The builder only fails on missing required
                // fields. bucket, key, and upload_id are all set, so
                // log and return on any unexpected failure.
                let input = match AbortMultipartUploadInput::builder()
                    .bucket(bucket.clone())
                    .key(key.clone())
                    .upload_id(upload_id.clone())
                    .build()
                {
                    Ok(input) => input,
                    Err(e) => {
                        tracing::error!(
                            bucket = %bucket,
                            key = %key,
                            upload_id = %upload_id,
                            err = %e,
                            "failed to build AbortMultipartUploadInput on session drop"
                        );
                        return;
                    }
                };
                match tokio::time::timeout(
                    std::time::Duration::from_secs(backend_op_timeout_secs),
                    storage.abort_multipart_upload(input, &access_key, &secret_key),
                )
                .await
                {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        // close() removes the tombstone only on Ok, so Drop
                        // retries any abort whose inline attempt caused an
                        // error. A retried abort can race a concurrent
                        // successful CompleteMultipartUpload, returning
                        // NoSuchUpload. Log at debug to keep error-level
                        // logs reserved for genuine abort failures.
                        let msg = e.to_string();
                        if msg.contains(s3_error_codes::NO_SUCH_UPLOAD) {
                            tracing::debug!(
                                bucket = %bucket,
                                key = %key,
                                upload_id = %upload_id,
                                "Drop abort returned NoSuchUpload: upload already completed or aborted",
                            );
                        } else {
                            tracing::error!(
                                bucket = %bucket,
                                key = %key,
                                upload_id = %upload_id,
                                err = %e,
                                "failed to abort orphaned multipart upload"
                            );
                        }
                    }
                    Err(_elapsed) => {
                        // Drop's abort task is bounded by the same
                        // per-call deadline as inline backend calls.
                        // A timeout here is rare (the runtime drains
                        // session tasks for SHUTDOWN_DRAIN_TIMEOUT_SECS
                        // and Drop runs after that), so log at warn so
                        // operators can correlate the orphaned upload
                        // with the bucket AbortIncompleteMultipartUpload
                        // lifecycle rule that will reclaim it.
                        tracing::warn!(
                            bucket = %bucket,
                            key = %key,
                            upload_id = %upload_id,
                            timeout_secs = backend_op_timeout_secs,
                            "Drop abort of orphaned multipart upload timed out; bucket lifecycle rule must reclaim parts",
                        );
                    }
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::state::WritePhase;
    use super::super::test_support::{TEST_PART_SIZE, build_driver, build_readonly_driver, file_handle, write_handle};
    use super::*;
    use crate::common::dummy_storage::DummyBackend;
    use crate::common::gateway::{with_test_auth_override, with_test_iam_unavailable};
    use russh_sftp::server::Handler;
    use rustfs_utils::path;
    use std::sync::Arc;

    #[tokio::test]
    async fn fstat_on_file_handle_returns_cached_attrs() {
        let backend = Arc::new(DummyBackend::new());
        let mut driver = build_driver(backend, TEST_PART_SIZE);
        let attrs = FileAttributes {
            size: Some(1234),
            mtime: Some(1_700_000_000),
            ..Default::default()
        };
        let handle_id = driver
            .allocate_handle(file_handle("b", "k", 1234, attrs.clone()))
            .expect("allocate");
        let out = driver.fstat(4, handle_id).await.expect("fstat on File must succeed");
        assert_eq!(out.attrs.size, Some(1234));
        assert_eq!(out.attrs.mtime, Some(1_700_000_000));
    }

    #[tokio::test]
    async fn fstat_on_write_handle_returns_running_byte_count_from_phase() {
        let backend = Arc::new(DummyBackend::new());
        let mut driver = build_driver(backend, TEST_PART_SIZE);
        let phase = WritePhase::Buffering {
            part_buffer: vec![0u8; 4096],
        };
        let handle_id = driver.allocate_handle(write_handle("b", "k", phase)).expect("allocate");
        let out = driver.fstat(5, handle_id).await.expect("fstat on Write must succeed");
        assert_eq!(
            out.attrs.size,
            Some(4096),
            "fstat on a Buffering handle must report the part-buffer length"
        );
    }

    #[tokio::test]
    async fn fsetstat_returns_ok_for_any_attrs() {
        let backend = Arc::new(DummyBackend::new());
        let mut driver = build_driver(backend, TEST_PART_SIZE);
        let handle_id = driver
            .allocate_handle(file_handle("b", "k", 0, FileAttributes::default()))
            .expect("allocate");
        let status = driver
            .fsetstat(6, handle_id, FileAttributes::default())
            .await
            .expect("fsetstat must succeed on any attrs");
        assert!(matches!(status.status_code, StatusCode::Ok));
    }

    async fn realpath_status(driver: &mut SftpDriver<DummyBackend>, path: &str) -> Result<String, StatusCode> {
        match driver.realpath(7, path.to_string()).await {
            Ok(out) => Ok(out.files[0].filename.clone()),
            Err(err) => Err(err.0),
        }
    }

    #[tokio::test]
    async fn realpath_rejects_nul_byte() {
        let backend = Arc::new(DummyBackend::new());
        let mut driver = build_driver(backend, TEST_PART_SIZE);
        let result = realpath_status(&mut driver, "/bucket/\0evil").await;
        assert!(matches!(result, Err(StatusCode::BadMessage)));
    }

    #[tokio::test]
    async fn realpath_rejects_carriage_return() {
        let backend = Arc::new(DummyBackend::new());
        let mut driver = build_driver(backend, TEST_PART_SIZE);
        let result = realpath_status(&mut driver, "/bucket/line\r/evil").await;
        assert!(matches!(result, Err(StatusCode::BadMessage)));
    }

    #[tokio::test]
    async fn realpath_rejects_line_feed() {
        let backend = Arc::new(DummyBackend::new());
        let mut driver = build_driver(backend, TEST_PART_SIZE);
        let result = realpath_status(&mut driver, "/bucket/line\n/evil").await;
        assert!(matches!(result, Err(StatusCode::BadMessage)));
    }

    #[tokio::test]
    async fn realpath_rejects_global_dir_marker() {
        let backend = Arc::new(DummyBackend::new());
        let mut driver = build_driver(backend, TEST_PART_SIZE);
        let marker_path = format!("/bucket/sub{}", path::GLOBAL_DIR_SUFFIX);
        let result = realpath_status(&mut driver, &marker_path).await;
        assert!(matches!(result, Err(StatusCode::BadMessage)));
    }

    #[tokio::test]
    async fn realpath_resolves_traversal_inside_bucket() {
        let backend = Arc::new(DummyBackend::new());
        let mut driver = build_driver(backend, TEST_PART_SIZE);
        let resolved = realpath_status(&mut driver, "/bucket/sub/../other").await.expect("ok");
        assert_eq!(resolved, "/bucket/other");
    }

    #[tokio::test]
    async fn realpath_root_returns_slash() {
        let backend = Arc::new(DummyBackend::new());
        let mut driver = build_driver(backend, TEST_PART_SIZE);
        assert_eq!(realpath_status(&mut driver, "/").await.expect("ok"), "/");
        assert_eq!(realpath_status(&mut driver, "").await.expect("ok"), "/");
        assert_eq!(realpath_status(&mut driver, "/..").await.expect("ok"), "/");
    }

    #[tokio::test]
    async fn realpath_bucket_only() {
        let backend = Arc::new(DummyBackend::new());
        let mut driver = build_driver(backend, TEST_PART_SIZE);
        assert_eq!(realpath_status(&mut driver, "/bucket").await.expect("ok"), "/bucket");
        assert_eq!(realpath_status(&mut driver, "/bucket/").await.expect("ok"), "/bucket");
    }

    #[tokio::test]
    async fn realpath_nonexistent_path_resolves_without_backend_call() {
        let backend = Arc::new(DummyBackend::new());
        let mut driver = build_driver(backend.clone(), TEST_PART_SIZE);
        let resolved = realpath_status(&mut driver, "/bucket/does-not-exist").await.expect("ok");
        assert_eq!(resolved, "/bucket/does-not-exist");
        assert!(backend.head_object_calls().is_empty(), "realpath must not issue HeadObject");
    }

    #[tokio::test]
    async fn setstat_returns_ok_in_read_write_mode() {
        let backend = Arc::new(DummyBackend::new());
        let mut driver = build_driver(backend, TEST_PART_SIZE);
        let status = driver
            .setstat(8, "/bucket/key".into(), FileAttributes::default())
            .await
            .expect("setstat must succeed in read-write mode");
        assert!(matches!(status.status_code, StatusCode::Ok));
    }

    #[tokio::test]
    async fn setstat_rejected_in_read_only_mode() {
        let backend = Arc::new(DummyBackend::new());
        let mut driver = build_readonly_driver(backend, TEST_PART_SIZE);
        let result = driver.setstat(9, "/bucket/key".into(), FileAttributes::default()).await;
        match result {
            Err(err) => assert!(matches!(err.0, StatusCode::PermissionDenied)),
            Ok(_) => panic!("setstat must error in read-only mode"),
        }
    }

    /// list_objects_v2 backend error must propagate as Err. Falling
    /// through would convert a transient error into silent data loss.
    #[tokio::test]
    async fn validate_directory_empty_propagates_list_error() {
        // When the empty-check list_objects_v2 fails,
        // validate_directory_empty returns Err. The destructive caller
        // never runs against an unverified target.
        let backend = Arc::new(DummyBackend::new());
        backend.queue_list_objects_v2_err(crate::common::dummy_storage::DummyError::Injected(
            "list_objects_v2 transient failure".into(),
        ));
        let driver = build_driver(backend.clone(), TEST_PART_SIZE);
        let result = with_test_auth_override(|_, _, _| true, driver.validate_directory_empty("b", "")).await;
        assert!(result.is_err(), "list_objects_v2 error must propagate as Err");
    }

    #[tokio::test]
    async fn validate_directory_empty_returns_ok_when_listing_is_empty() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_list_objects_v2_ok_empty();
        let driver = build_driver(backend.clone(), TEST_PART_SIZE);
        let result = with_test_auth_override(|_, _, _| true, driver.validate_directory_empty("b", "")).await;
        assert!(result.is_ok(), "empty listing must return Ok");
    }

    #[tokio::test]
    async fn fsetstat_rejected_in_read_only_mode() {
        let backend = Arc::new(DummyBackend::new());
        let mut driver = build_readonly_driver(backend, TEST_PART_SIZE);
        let handle_id = driver
            .allocate_handle(file_handle("b", "k", 0, FileAttributes::default()))
            .expect("allocate");
        let result = driver.fsetstat(10, handle_id, FileAttributes::default()).await;
        match result {
            Err(err) => assert!(matches!(err.0, StatusCode::PermissionDenied)),
            Ok(_) => panic!("fsetstat must error in read-only mode"),
        }
    }

    /// IAM-unreachable maps to Failure. Policy deny maps to
    /// PermissionDenied. Two error categories must produce two wire
    /// statuses so an IAM outage is not reported as a permanent
    /// permission rejection.
    #[tokio::test]
    async fn authorize_maps_iam_unavailable_to_failure() {
        let backend = Arc::new(DummyBackend::new());
        let driver = build_driver(backend, TEST_PART_SIZE);
        let result = with_test_iam_unavailable(driver.authorize(&S3Action::PutObject, "b", Some("k"))).await;
        let err = result.expect_err("IAM unavailable must surface as Err");
        assert!(
            matches!(err.0, StatusCode::Failure),
            "IAM unavailable must map to Failure, not PermissionDenied"
        );
    }

    /// AccessDenied still surfaces as PermissionDenied. Pinned alongside
    /// the IamUnavailable test so a future refactor of the authorize
    /// helper cannot silently collapse the two error categories.
    #[tokio::test]
    async fn authorize_maps_access_denied_to_permission_denied() {
        let backend = Arc::new(DummyBackend::new());
        let driver = build_driver(backend, TEST_PART_SIZE);
        let result = with_test_auth_override(|_, _, _| false, driver.authorize(&S3Action::PutObject, "b", Some("k"))).await;
        let err = result.expect_err("Deny must surface as Err");
        assert!(matches!(err.0, StatusCode::PermissionDenied), "AccessDenied must map to PermissionDenied");
    }
}
