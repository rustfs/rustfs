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

//! Write-side state machine: open_write, commit_write, the
//! write_dispatch chain that flushes a part once part_buffer reaches
//! part_size, abort_upload_with_auth, close_streaming, and
//! multipart_copy. Also, the cancellation-safety primitives
//! (build_write_tombstone, should_abort_on_drop) that the Drop impl
//! in driver.rs consumes.

use super::attrs::{s3_attrs_to_sftp, sftp_attrs_to_user_metadata};
use super::constants::limits::{
    COMMIT_WRITE_BACKOFF_MS, COMMIT_WRITE_MAX_RETRIES, S3_MAX_MULTIPART_PARTS, S3_MAX_PART_SIZE, S3_MIN_PART_SIZE,
};
use super::driver::SftpDriver;
use super::errors::{SftpError, is_not_found_error, s3_error_to_sftp};
use super::paths::{parse_s3_path, sanitise_control_bytes};
use super::state::{CompletedPart, HandleState, MultipartUpload, WritePhase};
use crate::common::client::s3::StorageBackend;
use crate::common::gateway::S3Action;
use bytes::Bytes;
use futures_util::stream;
use russh_sftp::protocol::{FileAttributes, Handle, OpenFlags, StatusCode};
use s3s::dto::{
    AbortMultipartUploadInput, CompleteMultipartUploadInput, CompletedMultipartUpload, CompletedPart as S3CompletedPart,
    CopySource, CreateMultipartUploadInput, PutObjectInput, StreamingBlob, UploadPartCopyInput, UploadPartInput,
};

/// Running byte count for a write handle's current phase. Buffering is
/// part_buffer.len(). Streaming is (parts_done * part_size) +
/// part_buffer.len(). Failed is treated as zero in saturating mode and
/// an error in strict mode. The saturating flag controls what happens
/// on u64 overflow: strict returns Err, saturating returns u64::MAX.
/// Strict is used by the offset precondition check (entry to the
/// write_dispatch chain). Saturating is used when refreshing attrs.size
/// at the tail of the same chain. See write_dispatch for the full call
/// graph.
pub(super) fn write_dispatch_byte_count(phase: &WritePhase, part_size: u64, saturating: bool) -> Result<u64, SftpError> {
    match phase {
        WritePhase::Buffering { part_buffer } => Ok(part_buffer.len() as u64),
        WritePhase::Streaming {
            part_buffer,
            next_part_number,
            ..
        } => {
            let parts_done = (*next_part_number - 1) as u64;
            let sum = parts_done
                .checked_mul(part_size)
                .and_then(|base| base.checked_add(part_buffer.len() as u64));
            match (sum, saturating) {
                (Some(value), _) => Ok(value),
                (None, true) => Ok(u64::MAX),
                (None, false) => {
                    tracing::warn!("SFTP write running total overflowed u64");
                    Err(SftpError::code(StatusCode::Failure))
                }
            }
        }
        WritePhase::Failed { .. } => {
            if saturating {
                Ok(0)
            } else {
                tracing::warn!("SFTP write rejected: handle already poisoned by earlier upload failure");
                Err(SftpError::code(StatusCode::Failure))
            }
        }
    }
}

/// Append incoming bytes to whichever buffer the current phase carries.
/// Failed is unreachable here: write_dispatch's offset check rejects
/// Failed before this call. The Failed arm logs and returns Failure
/// rather than panicking so a broken invariant cannot abort the
/// process. See write_dispatch for the full call graph.
pub(super) fn write_dispatch_append_bytes(phase: &mut WritePhase, data: &[u8]) -> Result<(), SftpError> {
    match phase {
        WritePhase::Buffering { part_buffer } | WritePhase::Streaming { part_buffer, .. } => {
            part_buffer.extend_from_slice(data);
            Ok(())
        }
        WritePhase::Failed { .. } => {
            tracing::error!("SFTP write_dispatch_append_bytes reached a Failed handle, internal invariant broken");
            Err(SftpError::code(StatusCode::Failure))
        }
    }
}

/// Drain-loop predicate. Returns true when the current phase's
/// part_buffer holds at least part_size bytes. Failed returns false so
/// the loop exits on a poisoned handle. See write_dispatch for the
/// full call graph.
pub(super) fn write_dispatch_has_full_part(phase: &WritePhase, part_size: u64) -> bool {
    match phase {
        WritePhase::Buffering { part_buffer } | WritePhase::Streaming { part_buffer, .. } => {
            (part_buffer.len() as u64) >= part_size
        }
        WritePhase::Failed { .. } => false,
    }
}

/// Size value reported by FSTAT for a write handle in the given phase.
///
/// Buffering returns the current buffer length. Streaming returns the
/// running total of bytes received so far (parts uploaded plus any
/// bytes still buffered), saturating at u64::MAX on arithmetic
/// overflow. Failed returns the cached attrs.size from the last
/// successful write: a client polling FSTAT after a write failure
/// reads the byte count that landed rather than zero.
pub(super) fn fstat_reported_size(phase: &WritePhase, part_size: u64, cached_size: u64) -> u64 {
    match phase {
        WritePhase::Buffering { part_buffer } => part_buffer.len() as u64,
        WritePhase::Streaming {
            part_buffer,
            next_part_number,
            ..
        } => {
            let parts_done = (*next_part_number - 1) as u64;
            parts_done
                .checked_mul(part_size)
                .and_then(|base| base.checked_add(part_buffer.len() as u64))
                .unwrap_or(u64::MAX)
        }
        WritePhase::Failed { .. } => cached_size,
    }
}

/// Construct a cancellation-safety tombstone for a live multipart
/// upload. A tombstone is a HandleState::Write whose phase is Failed,
/// carrying the in-flight upload_id and the cached abort_authorized
/// flag. Drop reads both fields to issue AbortMultipartUpload on
/// session teardown.
///
/// The write() and close() handlers remove a HandleState from the
/// handle table, await an S3 backend call, and re-insert. If the
/// handler future is cancelled or panics between remove and re-insert,
/// the real state is dropped before Drop ever sees it. To prevent the
/// upload_id from orphaning, every remove-await-reinsert site inserts
/// a tombstone under the same handle id before the await. Drop's
/// drain loop picks up WritePhase::Failed entries with
/// abort_authorized == true and fires AbortMultipartUpload. A
/// successful future overwrites the tombstone with the real state
/// synchronously after the await returns; no further await runs in
/// between, so cancellation cannot fire in that window.
///
/// WritePhase::Failed already means "upload poisoned by a prior
/// UploadPart failure; abort at close." Tombstones reuse the variant
/// for "upload in progress; abort if the caller's future vanishes."
/// Drop runs the same AbortMultipartUpload for both.
///
/// attrs is required by the HandleState::Write variant layout but
/// Drop does not read it. Callers pass a clone of the live attrs.
pub(super) fn build_write_tombstone(
    bucket: &str,
    key: &str,
    attrs: &FileAttributes,
    upload_id: String,
    abort_authorized: bool,
) -> HandleState {
    HandleState::Write {
        bucket: bucket.to_string(),
        key: key.to_string(),
        attrs: attrs.clone(),
        open_attrs: FileAttributes::empty(),
        phase: WritePhase::Failed {
            upload_id,
            abort_authorized,
        },
    }
}

/// Predicate for the SFTPv3 draft section 6.3 rule that SSH_FXF_EXCL
/// and SSH_FXF_TRUNC are modifiers of SSH_FXF_CREAT. Returns true when
/// either modifier is set without CREAT, which the open() handler
/// translates into BadMessage at the protocol boundary.
pub(super) fn rejects_excl_or_trunc_without_create(pflags: OpenFlags) -> bool {
    !pflags.contains(OpenFlags::CREATE) && (pflags.contains(OpenFlags::EXCLUDE) || pflags.contains(OpenFlags::TRUNCATE))
}

/// Decide whether Drop should abort the given write handle's phase.
/// Returns Some(upload_id) when there is a live upload AND the cached
/// abort_authorized flag says the principal is permitted to call
/// AbortMultipartUpload. Returns None for Buffering (no upload_id) and
/// for Streaming or Failed with abort_authorized == false (IAM denies
/// abort; cleanup falls to the bucket AbortIncompleteMultipartUpload
/// lifecycle rule).
pub(super) fn should_abort_on_drop(phase: &WritePhase) -> Option<&str> {
    match phase {
        WritePhase::Streaming {
            upload_id,
            abort_authorized: true,
            ..
        } => Some(upload_id.as_str()),
        WritePhase::Failed {
            upload_id,
            abort_authorized: true,
        } => Some(upload_id.as_str()),
        _ => None,
    }
}

impl<S: StorageBackend + Send + Sync + 'static> SftpDriver<S> {
    /// Write-side OPEN: enforce read-only mode, authorise PutObject,
    /// and require WRITE | CREATE | TRUNCATE (with optional EXCLUDE).
    /// No bytes are sent to S3 here. The upload happens at CLOSE with
    /// a single PutObject call carrying the buffered payload.
    ///
    /// The streaming write path overwrites the entire object at close
    /// so the only flag combination that matches that semantic is
    /// CREATE | TRUNCATE. WRITE without CREATE or TRUNCATE,
    /// WRITE | CREATE without TRUNCATE, and other combinations would
    /// silently mistranslate partial-write or open-without-truncate
    /// intent into truncate-and-replace, with data loss for clients
    /// that requested the former. Those combinations return
    /// OpUnsupported at OPEN so the client sees a clean error rather
    /// than a corrupted object at CLOSE.
    ///
    /// EXCLUDE adds a HeadObject existence check so the OPEN fails
    /// when the key already exists. The check is best-effort. A
    /// second client racing the same path can win the PutObject
    /// between this HEAD and the eventual CLOSE. The SFTPv3 draft
    /// does not guarantee atomicity here and S3 has no native CAS
    /// primitive, so the race is accepted.
    pub(super) async fn open_write(
        &mut self,
        id: u32,
        filename: &str,
        pflags: OpenFlags,
        open_attrs: FileAttributes,
    ) -> Result<Handle, SftpError> {
        self.enforce_server_readonly()?;

        let (bucket, key) = parse_s3_path(filename)?;
        let Some(object_key) = key else {
            return Err(SftpError::code(StatusCode::NoSuchFile));
        };
        if bucket.is_empty() {
            return Err(SftpError::code(StatusCode::NoSuchFile));
        }

        self.authorize(&S3Action::PutObject, &bucket, Some(&object_key)).await?;

        let creat = pflags.contains(OpenFlags::CREATE);
        let trunc = pflags.contains(OpenFlags::TRUNCATE);
        let excl = pflags.contains(OpenFlags::EXCLUDE);

        // Reject any flag combination that would not be honoured by a
        // single PutObject at CLOSE. See the doc comment above for the
        // full rationale.
        if !creat || !trunc {
            return Err(SftpError::code(StatusCode::OpUnsupported));
        }

        if excl {
            // EXCLUDE (SSH_FXF_EXCL): check whether the object already
            // exists. HEAD returning Ok means the key is taken. A
            // not-found error means the key is free. Any other error is
            // propagated rather than misinterpreted as "does not exist".
            match self
                .run_backend_with_err(
                    "head_object",
                    self.storage
                        .head_object(&bucket, &object_key, self.access_key(), self.secret_key()),
                )
                .await?
            {
                Ok(_) => return Err(SftpError::code(StatusCode::Failure)),
                Err(e) if is_not_found_error(&e) => {}
                Err(e) => return Err(s3_error_to_sftp("head_object", e)),
            }
        }

        let mut attrs = s3_attrs_to_sftp(0, None, false);
        if open_attrs.mtime.is_some() {
            attrs.mtime = open_attrs.mtime;
            attrs.atime = open_attrs.mtime;
        }
        if open_attrs.permissions.is_some() {
            attrs.permissions = open_attrs.permissions;
        }
        if open_attrs.uid.is_some() {
            attrs.uid = open_attrs.uid;
        }
        if open_attrs.gid.is_some() {
            attrs.gid = open_attrs.gid;
        }
        let handle = self.allocate_handle(HandleState::Write {
            bucket,
            key: object_key,
            attrs,
            open_attrs,
            phase: WritePhase::Buffering { part_buffer: Vec::new() },
        })?;
        Ok(Handle { id, handle })
    }

    /// Upload the buffered bytes to S3 with a single PutObject call.
    /// An empty buffer still issues a PutObject, so an open followed
    /// by a close with no WRITE in between creates a zero-byte object
    /// (matching POSIX create-and-close semantics).
    ///
    /// PutObject is retried on transient backend errors recognised by
    /// rustfs_utils::retry::is_s3code_in_message_retryable (SlowDown,
    /// RequestTimeout, Throttling, InternalError, etc.). Up to
    /// COMMIT_WRITE_MAX_RETRIES retries with the
    /// COMMIT_WRITE_BACKOFF_MS exponential schedule. Terminal errors
    /// (AccessDenied, NoSuchBucket, etc.) return immediately. The
    /// buffer is held as a Bytes across retries so each attempt
    /// rebuilds the stream from a cheap clone of the same payload
    /// without a second heap allocation. A timeout on the underlying
    /// run_backend deadline still propagates immediately as Failure
    /// rather than retrying, because a stuck backend is not a
    /// transient classification the retry set targets.
    pub(super) async fn commit_write(
        &self,
        bucket: &str,
        key: &str,
        attrs: &FileAttributes,
        buffer: Vec<u8>,
    ) -> Result<(), SftpError> {
        let size = buffer.len() as i64;
        let body_bytes = Bytes::from(buffer);

        for attempt in 0..=COMMIT_WRITE_MAX_RETRIES {
            if attempt > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(COMMIT_WRITE_BACKOFF_MS[attempt - 1])).await;
                tracing::warn!(
                    bucket = %sanitise_control_bytes(bucket),
                    key = %sanitise_control_bytes(key),
                    attempt = attempt,
                    "retrying commit_write put_object after retryable backend error",
                );
            }

            let body = body_bytes.clone();
            let stream = stream::once(async move { Ok::<Bytes, std::io::Error>(body) });
            let streaming = StreamingBlob::wrap(stream);
            let input = PutObjectInput::builder()
                .bucket(bucket.to_string())
                .key(key.to_string())
                .content_length(Some(size))
                .metadata(sftp_attrs_to_user_metadata(attrs))
                .body(Some(streaming))
                .build()
                .map_err(|e| s3_error_to_sftp("build_put_object", e))?;

            let outcome = self
                .run_backend_with_err("put_object", self.storage.put_object(input, self.access_key(), self.secret_key()))
                .await?;

            let backend_err = match outcome {
                Ok(_) => return Ok(()),
                Err(e) => e,
            };

            let msg = backend_err.to_string();
            if attempt < COMMIT_WRITE_MAX_RETRIES && rustfs_utils::retry::is_s3code_in_message_retryable(&msg) {
                continue;
            }
            return Err(s3_error_to_sftp("put_object", backend_err));
        }

        // Defensive fallback. The for is exhaustive over
        // 0..=COMMIT_WRITE_MAX_RETRIES and every iteration either
        // returns or continues; the continue branch is gated on
        // attempt < COMMIT_WRITE_MAX_RETRIES, so the final iteration
        // always returns. If a future change to the loop bound breaks
        // that proof, log and surface Failure rather than panicking
        // the session task.
        tracing::error!(
            bucket = %sanitise_control_bytes(bucket),
            key = %sanitise_control_bytes(key),
            "commit_write retry loop fell through without returning",
        );
        Err(SftpError::code(StatusCode::Failure))
    }

    /// Upload exactly one part of an in-progress multipart upload.
    /// Returns a CompletedPart on success. Authorization for UploadPart
    /// is checked before each call.
    pub(super) async fn upload_multipart_bytes(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i32,
        part_bytes: Vec<u8>,
    ) -> Result<CompletedPart, SftpError> {
        self.authorize(&S3Action::UploadPart, bucket, Some(key)).await?;

        let part_len = part_bytes.len() as i64;
        let body_bytes = Bytes::from(part_bytes);
        let body_stream = stream::once(async move { Ok::<Bytes, std::io::Error>(body_bytes) });
        let streaming = StreamingBlob::wrap(body_stream);

        let input = UploadPartInput::builder()
            .bucket(bucket.to_string())
            .key(key.to_string())
            .upload_id(upload_id.to_string())
            .part_number(part_number)
            .content_length(Some(part_len))
            .body(Some(streaming))
            .build()
            .map_err(|e| s3_error_to_sftp("build_upload_part", e))?;

        let out = self
            .run_backend("upload_part", self.storage.upload_part(input, self.access_key(), self.secret_key()))
            .await?;

        let e_tag = out.e_tag.ok_or_else(|| {
            tracing::warn!(upload_id = %upload_id, part_number = part_number, "UploadPart returned no ETag");
            SftpError::code(StatusCode::Failure)
        })?;

        Ok(CompletedPart { part_number, e_tag })
    }

    /// Issue CreateMultipartUpload for the given bucket and key. Returns
    /// the upload_id plus a cached authorisation flag for the matching
    /// AbortMultipartUpload call. Authorisation for CreateMultipartUpload
    /// is issued before the backend call. SFTP does not carry S3 object
    /// metadata (content type, storage class, SSE config) so the input
    /// is built with only bucket and key.
    ///
    /// The Abort probe exists because the Drop impl on SftpDriver is
    /// synchronous and cannot later await an auth check. Caching the
    /// decision here lets Drop honour a Deny policy on AbortMultipartUpload
    /// without regressing the abort-on-disconnect invariant for every
    /// principal. close() consults the same cached flag so the two
    /// paths agree on policy outcome.
    ///
    /// On probe failure (Deny on AbortMultipartUpload, IAM unreachable,
    /// or any other authorize_operation Err), set abort_authorized =
    /// false. The upload still proceeds. Rationale: the admin
    /// configured a Deny Abort policy deliberately (append-only / WORM
    /// patterns). Fail-closed would refuse uploads from such principals
    /// entirely, which is not the admin's intent. Orphaned parts left
    /// behind by a Drop skip are cleaned up by the bucket
    /// AbortIncompleteMultipartUpload lifecycle rule, which operators
    /// using this policy pattern must configure.
    ///
    /// Condition-key policies (aws:SourceIp, aws:MultiFactorAuthPresent,
    /// aws:CurrentTime, object tags, etc.) are not evaluated here or
    /// anywhere else on the SFTP path: authorize_operation in gateway.rs
    /// passes an empty conditions map. Only unconditional Allow/Deny is
    /// honoured. This is a gateway-wide limitation, not specific to
    /// this cache.
    pub(super) async fn start_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        attrs: &FileAttributes,
    ) -> Result<MultipartUpload, SftpError> {
        self.authorize(&S3Action::CreateMultipartUpload, bucket, Some(key)).await?;

        // Probe AbortMultipartUpload authorisation immediately after
        // Create. The probe has no backend side effect. Its result is
        // cached on the resulting WritePhase::Streaming variant and read
        // by Drop and by close()'s abort paths. Routing through
        // self.authorize wraps the IAM call in the same per-call
        // deadline as every other authorize on the SFTP path; an IAM
        // hang here would otherwise wedge the caller indefinitely with
        // a live upload_id already at S3. is_ok() collapses
        // AccessDenied and IamUnavailable to false; only an explicit
        // Allow yields true.
        let abort_authorized = self
            .authorize(&S3Action::AbortMultipartUpload, bucket, Some(key))
            .await
            .is_ok();

        let input = CreateMultipartUploadInput::builder()
            .bucket(bucket.to_string())
            .key(key.to_string())
            .metadata(sftp_attrs_to_user_metadata(attrs))
            .build()
            .map_err(|e| s3_error_to_sftp("build_create_multipart_upload", e))?;

        let out = self
            .run_backend(
                "create_multipart_upload",
                self.storage
                    .create_multipart_upload(input, self.access_key(), self.secret_key()),
            )
            .await?;

        let upload_id = out.upload_id.ok_or_else(|| {
            tracing::warn!(
                bucket = %sanitise_control_bytes(bucket),
                key = %sanitise_control_bytes(key),
                "CreateMultipartUpload returned no upload_id"
            );
            SftpError::code(StatusCode::Failure)
        })?;
        Ok(MultipartUpload {
            upload_id,
            abort_authorized,
        })
    }

    /// Finalise a multipart upload by calling CompleteMultipartUpload
    /// with the collected parts. Authorisation for
    /// CompleteMultipartUpload is issued before the backend call.
    pub(super) async fn finish_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        uploaded_parts: Vec<CompletedPart>,
    ) -> Result<(), SftpError> {
        self.authorize(&S3Action::CompleteMultipartUpload, bucket, Some(key)).await?;

        let parts: Vec<S3CompletedPart> = uploaded_parts
            .into_iter()
            .map(|p| S3CompletedPart {
                part_number: Some(p.part_number),
                e_tag: Some(p.e_tag),
                ..Default::default()
            })
            .collect();

        let input = CompleteMultipartUploadInput::builder()
            .bucket(bucket.to_string())
            .key(key.to_string())
            .upload_id(upload_id.to_string())
            .multipart_upload(Some(CompletedMultipartUpload { parts: Some(parts) }))
            .build()
            .map_err(|e| s3_error_to_sftp("build_complete_multipart_upload", e))?;

        let result = self
            .run_backend(
                "complete_multipart_upload",
                self.storage
                    .complete_multipart_upload(input, self.access_key(), self.secret_key()),
            )
            .await;
        result?;
        Ok(())
    }

    /// Run one WRITE packet against an extracted HandleState. The handle
    /// has already been removed from the table by the caller. The caller
    /// reinserts it after dispatch. Returns Err to convert to an SFTP
    /// status at the call site. On upload_part failure the phase is
    /// transitioned to Failed before returning.
    ///
    /// Cancellation-safety: the caller has removed the handle from the
    /// table, so an await inside this function holds the live upload_id
    /// only in the caller's local state. A tombstone must be in the
    /// table before the first such await. For a handle entering in the
    /// Streaming or Failed phase the caller installs the tombstone
    /// before calling this method. For a handle entering in Buffering,
    /// write_dispatch_begin_streaming installs the tombstone immediately
    /// after the synchronous transition to Streaming, before any
    /// subsequent await.
    ///
    /// Helper chain (each helper's doc references back here):
    ///
    ///     write_dispatch
    ///       write_dispatch_byte_count (strict)        [offset precondition]
    ///       write_dispatch_append_bytes               [add incoming bytes]
    ///       loop while write_dispatch_has_full_part:
    ///         write_dispatch_begin_streaming          [Buffering to Streaming]
    ///           start_multipart_upload                [S3 CreateMultipartUpload]
    ///         write_dispatch_flush_one_part           [drain + upload one part]
    ///           upload_multipart_bytes                [S3 UploadPart]
    ///       write_dispatch_byte_count (saturating)    [update attrs.size]
    pub(super) async fn write_dispatch(
        &mut self,
        handle: &str,
        state: &mut HandleState,
        offset: u64,
        data: Vec<u8>,
    ) -> Result<(), SftpError> {
        let HandleState::Write {
            bucket,
            key,
            attrs,
            open_attrs,
            phase,
        } = state
        else {
            return Err(SftpError::code(StatusCode::Failure));
        };
        let part_size = self.part_size;

        let current_len = write_dispatch_byte_count(phase, part_size, false)?;
        if offset != current_len {
            tracing::warn!(offset = offset, buffered = current_len, "SFTP write rejected: non-sequential offset");
            return Err(SftpError::code(StatusCode::Failure));
        }

        // Own the bucket and key before the drain loop so &self helpers
        // can await without conflicting with the live &mut phase borrow.
        let bucket_owned = bucket.clone();
        let key_owned = key.clone();

        write_dispatch_append_bytes(phase, &data)?;

        while write_dispatch_has_full_part(phase, part_size) {
            if matches!(phase, WritePhase::Buffering { .. }) {
                self.write_dispatch_begin_streaming(handle, phase, &bucket_owned, &key_owned, open_attrs)
                    .await?;
            }
            self.write_dispatch_flush_one_part(phase, &bucket_owned, &key_owned, part_size)
                .await?;
        }

        attrs.size = Some(write_dispatch_byte_count(phase, part_size, true).unwrap_or(u64::MAX));
        Ok(())
    }

    /// Buffering -> Streaming transition. Issues CreateMultipartUpload,
    /// then moves the existing part_buffer into the new Streaming
    /// variant. If CreateMultipartUpload fails the phase stays in
    /// Buffering and the error propagates. The next full-part flush
    /// retries the transition (transient S3 error invisible to the
    /// client).
    ///
    /// CreateMultipartUpload is awaited before mem::take on the
    /// buffer. The reverse order would lose the buffered bytes on
    /// transient failure. See write_dispatch for the full call graph.
    pub(super) async fn write_dispatch_begin_streaming(
        &mut self,
        handle: &str,
        phase: &mut WritePhase,
        bucket: &str,
        key: &str,
        attrs: &FileAttributes,
    ) -> Result<(), SftpError> {
        if !matches!(phase, WritePhase::Buffering { .. }) {
            return Ok(());
        }
        // start_multipart_upload returns a MultipartUpload containing
        // the upload_id and the cached
        // authorize_operation(AbortMultipartUpload) result. The pair is
        // stored on the Streaming variant so Drop (which cannot await)
        // has a pre-decided policy answer.
        let mp = self.start_multipart_upload(bucket, key, attrs).await?;
        let existing_buffer = match phase {
            WritePhase::Buffering { part_buffer } => std::mem::take(part_buffer),
            _ => {
                tracing::error!(
                    "SFTP write_dispatch_begin_streaming lost Buffering phase between check and extract, internal invariant broken"
                );
                return Err(SftpError::code(StatusCode::Failure));
            }
        };
        *phase = WritePhase::Streaming {
            upload_id: mp.upload_id.clone(),
            abort_authorized: mp.abort_authorized,
            part_buffer: existing_buffer,
            uploaded_parts: Vec::new(),
            next_part_number: 1,
        };
        // Between this point and the caller re-inserting the real
        // state, the upload_id exists only in a local variable here,
        // not in the handle table. Insert a tombstone so Drop can still
        // abort if the next UploadPart await is cancelled. The
        // synchronous window between start_multipart_upload returning
        // Ok and this insert contains no await, so cancellation cannot
        // fire in it.
        let tombstone = build_write_tombstone(bucket, key, attrs, mp.upload_id, mp.abort_authorized);
        self.handles.insert(handle.to_string(), tombstone);
        Ok(())
    }

    /// Drain exactly part_size bytes from a Streaming phase and upload
    /// them as one part. On success: records the returned CompletedPart
    /// and increments next_part_number. On failure: transitions to
    /// Failed carrying the live upload_id so close() can abort. Also
    /// poisons to Failed if next_part_number would exceed the S3 parts
    /// cap.
    ///
    /// Drain, upload, and the record-or-poison step are one atomic
    /// unit. Splitting them would create a window where bytes have
    /// left part_buffer but no Failed transition has occurred. See
    /// write_dispatch for the full call graph.
    pub(super) async fn write_dispatch_flush_one_part(
        &self,
        phase: &mut WritePhase,
        bucket: &str,
        key: &str,
        part_size: u64,
    ) -> Result<(), SftpError> {
        // Capture abort_authorized alongside upload_id inside the
        // Streaming match arm. The bool is Copy so the capture is
        // cheap. Both Streaming -> Failed transitions below must
        // carry this flag forward so Drop and close() continue to
        // honour the cached policy answer on the Failed handle.
        let (upload_id_for_call, abort_authorized_for_call, part_number_for_call, drained) = match phase {
            WritePhase::Streaming {
                upload_id,
                abort_authorized,
                part_buffer,
                next_part_number,
                ..
            } => {
                if *next_part_number > S3_MAX_MULTIPART_PARTS {
                    tracing::warn!(
                        bucket = %bucket,
                        key = %key,
                        limit = S3_MAX_MULTIPART_PARTS,
                        "SFTP write would exceed the S3 multipart parts limit",
                    );
                    let upload_id_for_fail = upload_id.clone();
                    let abort_authorized_for_fail = *abort_authorized;
                    *phase = WritePhase::Failed {
                        upload_id: upload_id_for_fail,
                        abort_authorized: abort_authorized_for_fail,
                    };
                    return Err(SftpError::code(StatusCode::Failure));
                }
                let drained: Vec<u8> = part_buffer.drain(..part_size as usize).collect();
                (upload_id.clone(), *abort_authorized, *next_part_number, drained)
            }
            _ => {
                tracing::error!("SFTP write_dispatch_flush_one_part called without Streaming phase, internal invariant broken");
                return Err(SftpError::code(StatusCode::Failure));
            }
        };

        match self
            .upload_multipart_bytes(bucket, key, &upload_id_for_call, part_number_for_call, drained)
            .await
        {
            Ok(completed) => match phase {
                WritePhase::Streaming {
                    uploaded_parts,
                    next_part_number,
                    ..
                } => {
                    uploaded_parts.push(completed);
                    *next_part_number += 1;
                    Ok(())
                }
                _ => {
                    tracing::error!(
                        "SFTP write_dispatch_flush_one_part post-upload arm without Streaming phase, internal invariant broken"
                    );
                    Err(SftpError::code(StatusCode::Failure))
                }
            },
            Err(err) => {
                // UploadPart failed. The drained bytes are lost: the
                // handle is now out of sync with its sequential
                // offset invariant. Transition to Failed so close can
                // issue AbortMultipartUpload. Carry the captured
                // abort_authorized into the new variant. The policy
                // decision does not change because the upload failed.
                *phase = WritePhase::Failed {
                    upload_id: upload_id_for_call,
                    abort_authorized: abort_authorized_for_call,
                };
                Err(err)
            }
        }
    }

    /// Issue AbortMultipartUpload for the given upload_id. Authorisation
    /// for AbortMultipartUpload is issued before the backend call. SFTP
    /// does not use cross-account or conditional-abort fields, so the
    /// input is built with bucket, key, and upload_id only.
    pub(super) async fn abort_upload_with_auth(&self, bucket: &str, key: &str, upload_id: &str) -> Result<(), SftpError> {
        self.authorize(&S3Action::AbortMultipartUpload, bucket, Some(key)).await?;

        let input = AbortMultipartUploadInput::builder()
            .bucket(bucket.to_string())
            .key(key.to_string())
            .upload_id(upload_id.to_string())
            .build()
            .map_err(|e| s3_error_to_sftp("build_abort_multipart_upload", e))?;

        self.run_backend(
            "abort_multipart_upload",
            self.storage
                .abort_multipart_upload(input, self.access_key(), self.secret_key()),
        )
        .await?;
        Ok(())
    }

    /// Issue AbortMultipartUpload if abort_authorized is true.
    /// Otherwise log a skip with bucket, key, upload_id, principal, and
    /// the supplied context. context is a short free-form label (for
    /// example "parts-limit breach" or "Failed handle") embedded in
    /// both the abort-error log and the skip log so operators can tell
    /// which arm of close() produced the record.
    pub(super) async fn close_abort_or_skip(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        abort_authorized: bool,
        context: &str,
    ) {
        if abort_authorized {
            if let Err(abort_err) = self.abort_upload_with_auth(bucket, key, upload_id).await {
                tracing::warn!(
                    bucket = %bucket,
                    key = %key,
                    upload_id = %upload_id,
                    err = ?abort_err,
                    "abort after {context} also failed; S3 lifecycle must clean up",
                    context = context,
                );
            }
        } else {
            tracing::warn!(
                bucket = %bucket,
                key = %key,
                upload_id = %upload_id,
                access_key = %self.access_key(),
                "skipped abort at close ({context}): principal lacks s3:AbortMultipartUpload, bucket lifecycle rules must reclaim parts",
                context = context,
            );
        }
    }

    /// close() arm handler for a handle in the Streaming phase. Flushes
    /// the trailing partial part if one exists, then calls
    /// CompleteMultipartUpload. On any failure inside this sequence the
    /// upload is rolled back via close_abort_or_skip, honouring the
    /// cached abort_authorized flag.
    ///
    /// S3 allows the last part of a multipart upload to be smaller than
    /// the minimum part size, so the trailing UploadPart does not need
    /// a minimum-size guard. A zero-length trailing buffer is skipped
    /// entirely because an earlier flush already emitted the final full
    /// part.
    ///
    /// Before issuing the trailing UploadPart, the S3 parts-per-upload
    /// cap is enforced. The flush-loop guard catches this for full-part
    /// flushes, but the close-time tail can hit next_part_number ==
    /// S3_MAX_MULTIPART_PARTS + 1 when the upload's size is exactly
    /// (S3_MAX_MULTIPART_PARTS * part_size) + tail. Without this guard
    /// the trailing call is guaranteed to be rejected by S3 with
    /// InvalidPart. Aborting here reports the size-overflow reason at
    /// the SFTP boundary and skips the round-trip that would fail.
    ///
    /// Every abort call site routes through close_abort_or_skip, which
    /// consults abort_authorized. False means the principal's IAM policy
    /// denies AbortMultipartUpload. Honouring that keeps close() aligned
    /// with Drop. Staged parts then fall to the bucket's
    /// AbortIncompleteMultipartUpload lifecycle rule for cleanup.
    ///
    /// Processes the Streaming field set by value. Passing
    /// WritePhase::Streaming by move would push the destructuring back
    /// inside the body and hide the field-by-field correspondence at
    /// the call site, so #[allow(clippy::too_many_arguments)] stays.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn close_streaming(
        &self,
        bucket: &str,
        key: &str,
        upload_id: String,
        abort_authorized: bool,
        part_buffer: Vec<u8>,
        mut uploaded_parts: Vec<CompletedPart>,
        next_part_number: i32,
    ) -> Result<(), SftpError> {
        if !part_buffer.is_empty() {
            if next_part_number > S3_MAX_MULTIPART_PARTS {
                tracing::warn!(
                    bucket = %bucket,
                    key = %key,
                    upload_id = %upload_id,
                    limit = S3_MAX_MULTIPART_PARTS,
                    "SFTP close rejected: trailing part would exceed S3 multipart parts limit",
                );
                self.close_abort_or_skip(bucket, key, &upload_id, abort_authorized, "parts-limit breach")
                    .await;
                return Err(SftpError::code(StatusCode::Failure));
            }
            match self
                .upload_multipart_bytes(bucket, key, &upload_id, next_part_number, part_buffer)
                .await
            {
                Ok(completed) => uploaded_parts.push(completed),
                Err(err) => {
                    self.close_abort_or_skip(bucket, key, &upload_id, abort_authorized, "final-part upload failure")
                        .await;
                    return Err(err);
                }
            }
        }

        if let Err(err) = self.finish_multipart_upload(bucket, key, &upload_id, uploaded_parts).await {
            self.close_abort_or_skip(bucket, key, &upload_id, abort_authorized, "CompleteMultipartUpload failure")
                .await;
            return Err(err);
        }
        Ok(())
    }

    /// Copy an object larger than S3_COPY_OBJECT_MAX_SIZE (5 GiB) via
    /// UploadPartCopy. Server-side only: no bytes transit the SFTP
    /// server. On any failure the destination multipart upload is
    /// aborted so no partial state is left behind. The source object
    /// is not touched.
    pub(super) async fn multipart_copy(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
        content_length: u64,
    ) -> Result<(), SftpError> {
        // Pick effective_part_size so any object up to the 5 TiB S3
        // limit divides into at most S3_MAX_MULTIPART_PARTS parts. The
        // ceil-div ensures the final part is not over the limit. The
        // min(S3_MAX_PART_SIZE) clamp protects against an out-of-spec
        // content_length: with S3_MAX_PART_SIZE = S3_COPY_OBJECT_MAX_SIZE
        // = 5 GiB and S3_MAX_MULTIPART_PARTS = 10000, the upper bound on
        // a copyable object is 50 TiB, an order of magnitude above the
        // 5 TiB S3 single-object cap. If the backend ever reports a
        // larger content_length the guard surfaces it as Failure here
        // rather than letting S3 reject UploadPartCopy with InvalidRange.
        let effective_part_size = {
            let max_parts = S3_MAX_MULTIPART_PARTS as u64;
            let configured = self.part_size;
            let needed = content_length.div_ceil(max_parts);
            let target = needed.max(configured).max(S3_MIN_PART_SIZE);
            if target > S3_MAX_PART_SIZE {
                tracing::warn!(
                    bucket = %dst_bucket,
                    key = %sanitise_control_bytes(dst_key),
                    content_length,
                    target,
                    "multipart copy refused: per-part size exceeds S3_MAX_PART_SIZE"
                );
                return Err(SftpError::code(StatusCode::Failure));
            }
            target
        };

        // multipart_copy manages the destination upload lifecycle
        // directly: any failure routes through close_abort_or_skip
        // rather than relying on the Drop tombstone path. The cached
        // abort_authorized flag is carried on the MultipartUpload so
        // close_abort_or_skip can honour a Deny-Abort policy without a
        // second IAM probe per error path.
        let mp = self
            .start_multipart_upload(dst_bucket, dst_key, &FileAttributes::empty())
            .await?;

        let result: Result<Vec<CompletedPart>, SftpError> = async {
            let mut uploaded_parts = Vec::new();
            let mut part_number: i32 = 1;
            let mut offset: u64 = 0;
            while offset < content_length {
                let end = offset.saturating_add(effective_part_size).min(content_length);
                let range = format!("bytes={}-{}", offset, end - 1);

                self.authorize(&S3Action::UploadPart, dst_bucket, Some(dst_key)).await?;

                let input = UploadPartCopyInput::builder()
                    .bucket(dst_bucket.to_string())
                    .key(dst_key.to_string())
                    .upload_id(mp.upload_id.clone())
                    .part_number(part_number)
                    .copy_source(CopySource::Bucket {
                        bucket: src_bucket.to_string().into(),
                        key: src_key.to_string().into(),
                        version_id: None,
                    })
                    .copy_source_range(Some(range))
                    .build()
                    .map_err(|e| s3_error_to_sftp("build_upload_part_copy", e))?;

                let out = self
                    .run_backend(
                        "upload_part_copy",
                        self.storage.upload_part_copy(input, self.access_key(), self.secret_key()),
                    )
                    .await?;

                let e_tag = out.copy_part_result.and_then(|r| r.e_tag).ok_or_else(|| {
                    tracing::warn!(
                        upload_id = %mp.upload_id,
                        part_number = part_number,
                        "UploadPartCopy returned no ETag"
                    );
                    SftpError::code(StatusCode::Failure)
                })?;

                uploaded_parts.push(CompletedPart { part_number, e_tag });
                part_number += 1;
                offset = end;
            }
            Ok(uploaded_parts)
        }
        .await;

        match result {
            Ok(parts) => {
                if let Err(err) = self.finish_multipart_upload(dst_bucket, dst_key, &mp.upload_id, parts).await {
                    self.close_abort_or_skip(
                        dst_bucket,
                        dst_key,
                        &mp.upload_id,
                        mp.abort_authorized,
                        "complete-multipart-copy failure",
                    )
                    .await;
                    return Err(err);
                }
                Ok(())
            }
            Err(err) => {
                self.close_abort_or_skip(dst_bucket, dst_key, &mp.upload_id, mp.abort_authorized, "copy failure")
                    .await;
                Err(err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::constants::limits::S3_MAX_MULTIPART_PARTS;
    use super::super::test_support::{TEST_PART_SIZE, build_driver, build_driver_with_timeout, write_handle};
    use super::*;
    use crate::common::dummy_storage::{AbortCall, DummyBackend, DummyError};
    use crate::common::gateway::with_test_auth_override;
    use russh_sftp::protocol::{FileAttributes, OpenFlags, StatusCode};
    use s3s::dto::ETag;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Notify;

    #[test]
    fn should_abort_on_drop_buffering_is_none() {
        let phase = WritePhase::Buffering { part_buffer: Vec::new() };
        assert!(should_abort_on_drop(&phase).is_none());
    }

    #[test]
    fn should_abort_on_drop_streaming_authorized_returns_upload_id() {
        let phase = WritePhase::Streaming {
            upload_id: "UP-7".to_string(),
            abort_authorized: true,
            part_buffer: Vec::new(),
            uploaded_parts: Vec::new(),
            next_part_number: 1,
        };
        assert_eq!(should_abort_on_drop(&phase), Some("UP-7"));
    }

    #[test]
    fn should_abort_on_drop_streaming_denied_is_none() {
        let phase = WritePhase::Streaming {
            upload_id: "UP-8".to_string(),
            abort_authorized: false,
            part_buffer: Vec::new(),
            uploaded_parts: Vec::new(),
            next_part_number: 1,
        };
        assert!(should_abort_on_drop(&phase).is_none());
    }

    #[test]
    fn should_abort_on_drop_failed_authorized_returns_upload_id() {
        let phase = WritePhase::Failed {
            upload_id: "UP-9".to_string(),
            abort_authorized: true,
        };
        assert_eq!(should_abort_on_drop(&phase), Some("UP-9"));
    }

    #[test]
    fn should_abort_on_drop_failed_denied_is_none() {
        let phase = WritePhase::Failed {
            upload_id: "UP-10".to_string(),
            abort_authorized: false,
        };
        assert!(should_abort_on_drop(&phase).is_none());
    }

    // Tombstone construction: the cancellation-safety mechanism relies
    // on the tombstone being recognised by the Drop drain loop. The
    // two invariants these tests pin:
    //   1. The tombstone carries the caller's upload_id verbatim.
    //   2. should_abort_on_drop returns Some(upload_id) when
    //      abort_authorized is true, so Drop picks the tombstone up.

    #[test]
    fn tombstone_carries_upload_id_and_authorization() {
        let attrs = FileAttributes::default();
        let state = build_write_tombstone("b", "k", &attrs, "UP-T1".to_string(), true);
        let HandleState::Write {
            bucket,
            key,
            phase: WritePhase::Failed {
                upload_id,
                abort_authorized,
            },
            ..
        } = state
        else {
            panic!("tombstone must be HandleState::Write with Failed phase");
        };
        assert_eq!(bucket, "b");
        assert_eq!(key, "k");
        assert_eq!(upload_id, "UP-T1");
        assert!(abort_authorized);
    }

    #[test]
    fn tombstone_is_picked_up_by_drop_when_authorized() {
        let attrs = FileAttributes::default();
        let state = build_write_tombstone("b", "k", &attrs, "UP-T2".to_string(), true);
        let HandleState::Write { phase, .. } = state else {
            panic!("tombstone must be HandleState::Write");
        };
        assert_eq!(should_abort_on_drop(&phase), Some("UP-T2"));
    }

    #[test]
    fn tombstone_is_skipped_by_drop_when_abort_denied() {
        // Principal with Deny on s3:AbortMultipartUpload: Drop must
        // skip the call. The tombstone still exists in the map so
        // operators see the orphaned upload_id in the skip log. The
        // bucket lifecycle rule reclaims parts.
        let attrs = FileAttributes::default();
        let state = build_write_tombstone("b", "k", &attrs, "UP-T3".to_string(), false);
        let HandleState::Write { phase, .. } = state else {
            panic!("tombstone must be HandleState::Write");
        };
        assert!(should_abort_on_drop(&phase).is_none());
    }

    // SFTPv3 draft section 6.3 rule: EXCL and TRUNC are modifiers of
    // CREAT. The open() handler boundary check lives in a free function
    // so these tests exercise it without a StorageBackend mock. WRITE
    // without CREATE or TRUNCATE passes the boundary check.
    // open_write rejects that combination at the next gate with
    // OpUnsupported because the streaming write path requires
    // CREATE | TRUNCATE.

    #[test]
    fn open_flags_excl_without_create_is_rejected() {
        assert!(rejects_excl_or_trunc_without_create(OpenFlags::EXCLUDE | OpenFlags::WRITE));
    }

    #[test]
    fn open_flags_trunc_without_create_is_rejected() {
        assert!(rejects_excl_or_trunc_without_create(OpenFlags::TRUNCATE | OpenFlags::WRITE));
    }

    #[test]
    fn open_flags_excl_with_create_is_allowed() {
        assert!(!rejects_excl_or_trunc_without_create(
            OpenFlags::CREATE | OpenFlags::EXCLUDE | OpenFlags::WRITE
        ));
    }

    #[test]
    fn open_flags_trunc_with_create_is_allowed() {
        assert!(!rejects_excl_or_trunc_without_create(
            OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::WRITE
        ));
    }

    #[test]
    fn open_flags_plain_read_or_write_is_allowed() {
        assert!(!rejects_excl_or_trunc_without_create(OpenFlags::READ));
        assert!(!rejects_excl_or_trunc_without_create(OpenFlags::WRITE));
    }

    // write_dispatch_byte_count: covers the three phases and the
    // strict-vs-saturating overflow behaviour.

    #[test]
    fn byte_count_buffering_returns_buffer_len() {
        // 777 is arbitrary. The test would fail if the Buffering arm
        // returned 0 or a derived value instead of the buffer length.
        let phase = WritePhase::Buffering {
            part_buffer: vec![0u8; 777],
        };
        assert_eq!(write_dispatch_byte_count(&phase, 5 * 1024 * 1024, true).unwrap(), 777);
        assert_eq!(write_dispatch_byte_count(&phase, 5 * 1024 * 1024, false).unwrap(), 777);
    }

    #[test]
    fn byte_count_streaming_overflow_strict_errs() {
        // next_part_number = i32::MAX and part_size = u64::MAX causes
        // checked_mul to overflow. Strict mode must propagate the
        // overflow rather than silently saturate.
        let phase = WritePhase::Streaming {
            upload_id: "X".to_string(),
            abort_authorized: true,
            part_buffer: Vec::new(),
            uploaded_parts: Vec::new(),
            next_part_number: i32::MAX,
        };
        assert!(write_dispatch_byte_count(&phase, u64::MAX, false).is_err());
    }

    #[test]
    fn byte_count_streaming_overflow_saturates_to_u64_max() {
        // Same overflow inputs as above but saturating mode. Would
        // fail if the function returned 0 or dropped the saturation.
        let phase = WritePhase::Streaming {
            upload_id: "X".to_string(),
            abort_authorized: true,
            part_buffer: Vec::new(),
            uploaded_parts: Vec::new(),
            next_part_number: i32::MAX,
        };
        assert_eq!(write_dispatch_byte_count(&phase, u64::MAX, true).unwrap(), u64::MAX);
    }

    #[test]
    fn byte_count_failed_strict_errs() {
        // Strict mode must refuse to calculate a size for a Failed
        // handle. The sequential-offset check relies on this to
        // reject further writes.
        let phase = WritePhase::Failed {
            upload_id: "X".to_string(),
            abort_authorized: true,
        };
        assert!(write_dispatch_byte_count(&phase, 5 * 1024 * 1024, false).is_err());
    }

    #[test]
    fn byte_count_failed_saturating_returns_zero() {
        // Saturating mode returns 0 for Failed so the post-flush
        // attrs.size update is infallible.
        let phase = WritePhase::Failed {
            upload_id: "X".to_string(),
            abort_authorized: true,
        };
        assert_eq!(write_dispatch_byte_count(&phase, 5 * 1024 * 1024, true).unwrap(), 0);
    }

    // write_dispatch_has_full_part: boundary behaviour plus the
    // Failed-variant short-circuit.

    #[test]
    fn has_full_part_boundary_at_exact_part_size() {
        let part_size: u64 = 1024;
        let at = WritePhase::Buffering {
            part_buffer: vec![0u8; 1024],
        };
        let below = WritePhase::Buffering {
            part_buffer: vec![0u8; 1023],
        };
        let above = WritePhase::Buffering {
            part_buffer: vec![0u8; 1025],
        };
        // The predicate uses >=, so exactly part_size is true.
        assert!(write_dispatch_has_full_part(&at, part_size));
        assert!(!write_dispatch_has_full_part(&below, part_size));
        assert!(write_dispatch_has_full_part(&above, part_size));
    }

    #[test]
    fn has_full_part_failed_returns_false() {
        // Failed carries no buffer so the predicate must not yield
        // true and keep the drain loop spinning.
        let phase = WritePhase::Failed {
            upload_id: "X".to_string(),
            abort_authorized: true,
        };
        assert!(!write_dispatch_has_full_part(&phase, 0));
        assert!(!write_dispatch_has_full_part(&phase, u64::MAX));
    }

    // write_dispatch_append_bytes: Buffering arm, Failed arm.

    #[test]
    fn append_bytes_buffering_arm_extends() {
        let mut phase = WritePhase::Buffering {
            part_buffer: vec![1u8, 2, 3],
        };
        write_dispatch_append_bytes(&mut phase, &[9u8, 9, 9]).unwrap();
        match &phase {
            WritePhase::Buffering { part_buffer } => {
                assert_eq!(part_buffer.as_slice(), &[1, 2, 3, 9, 9, 9]);
            }
            WritePhase::Streaming { .. } => panic!("append_bytes promoted Buffering to Streaming"),
            WritePhase::Failed { .. } => panic!("append_bytes poisoned Buffering to Failed"),
        }
    }

    #[test]
    fn append_bytes_failed_arm_returns_err() {
        let mut phase = WritePhase::Failed {
            upload_id: "X".to_string(),
            abort_authorized: true,
        };
        assert!(write_dispatch_append_bytes(&mut phase, &[1u8, 2, 3]).is_err());
        // The phase must remain Failed after the rejected call.
        // Any silent promotion or buffer attachment would be a bug.
        assert!(matches!(phase, WritePhase::Failed { .. }));
    }

    // fstat_reported_size: covers the Buffering/Streaming arithmetic
    // and the Failed-preserves-cached-size rule.

    #[test]
    fn fstat_reported_size_buffering_uses_buffer_length() {
        let phase = WritePhase::Buffering {
            part_buffer: vec![0u8; 512],
        };
        assert_eq!(fstat_reported_size(&phase, 5 * 1024 * 1024, 0), 512);
    }

    #[test]
    fn fstat_reported_size_streaming_combines_parts_and_buffer() {
        let part_size: u64 = 5 * 1024 * 1024;
        let phase = WritePhase::Streaming {
            upload_id: "X".to_string(),
            abort_authorized: true,
            part_buffer: vec![0u8; 1024],
            uploaded_parts: Vec::new(),
            // next_part_number==3 means parts 1 and 2 have been flushed.
            next_part_number: 3,
        };
        assert_eq!(fstat_reported_size(&phase, part_size, 0), 2 * part_size + 1024);
    }

    #[test]
    fn fstat_reported_size_streaming_saturates_on_overflow() {
        let phase = WritePhase::Streaming {
            upload_id: "X".to_string(),
            abort_authorized: true,
            part_buffer: Vec::new(),
            uploaded_parts: Vec::new(),
            next_part_number: i32::MAX,
        };
        assert_eq!(fstat_reported_size(&phase, u64::MAX, 0), u64::MAX);
    }

    #[test]
    fn fstat_reported_size_failed_returns_cached_size() {
        let phase = WritePhase::Failed {
            upload_id: "X".to_string(),
            abort_authorized: false,
        };
        assert_eq!(fstat_reported_size(&phase, 1_000_000, 42_000), 42_000);
        // And that cached_size == 0 is still reported as 0 rather than
        // some derived value, so clients see "nothing confirmed to S3"
        // rather than a misleading partial count.
        assert_eq!(fstat_reported_size(&phase, 1_000_000, 0), 0);
    }

    // Adversarial-input coverage for the write-dispatch running-total
    // helper. The helper composes two checked operations on u64:
    // parts_done * part_size (from the Streaming arm's
    // next_part_number and the configured part_size) and the
    // part_buffer length. The proptest block below biases part_size
    // and next_part_number toward boundaries so
    // parts_done * part_size reaches u64::MAX, then asserts the two
    // modes preserve their documented contracts. Strict mode returns
    // Err(Failure) on overflow and Err(Failure) on the Failed arm.
    // Saturating mode returns Ok(u64::MAX) on overflow and Ok(0) on
    // the Failed arm. Both modes return Ok(buffer_len) for Buffering.
    // The part_buffer length is kept small to bound the per-case Vec
    // allocation. Boundary stress lives in part_size and
    // next_part_number.
    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig {
            cases: 10_000,
            .. proptest::prelude::ProptestConfig::default()
        })]

        #[test]
        fn write_dispatch_byte_count_preserves_overflow_contract(
            part_buffer_len in 0usize..=1024,
            next_part_number in proptest::prop_oneof![
                proptest::prelude::Just(1i32),
                proptest::prelude::Just(2i32),
                proptest::prelude::Just(i32::MAX - 1),
                proptest::prelude::Just(i32::MAX),
                1i32..=i32::MAX,
            ],
            part_size in proptest::prop_oneof![
                proptest::prelude::Just(0u64),
                proptest::prelude::Just(1u64),
                proptest::prelude::Just(2u64),
                proptest::prelude::Just(u64::MAX / 3),
                proptest::prelude::Just(u64::MAX / 2),
                proptest::prelude::Just(u64::MAX - 1),
                proptest::prelude::Just(u64::MAX),
                1u64..=(16 * 1024 * 1024),
                proptest::prelude::any::<u64>(),
            ],
            phase_variant in 0u8..3,
            saturating in proptest::prelude::any::<bool>(),
        ) {
            let part_buffer = vec![0u8; part_buffer_len];
            let buffer_len_u64 = part_buffer_len as u64;
            let phase = match phase_variant {
                0 => WritePhase::Buffering {
                    part_buffer: part_buffer.clone(),
                },
                1 => WritePhase::Streaming {
                    upload_id: "UP-proptest".to_string(),
                    abort_authorized: true,
                    part_buffer: part_buffer.clone(),
                    uploaded_parts: Vec::new(),
                    next_part_number,
                },
                _ => WritePhase::Failed {
                    upload_id: "UP-proptest".to_string(),
                    abort_authorized: false,
                },
            };

            let result = write_dispatch_byte_count(&phase, part_size, saturating);

            match &phase {
                WritePhase::Buffering { .. } => {
                    let value = result.map_err(|e| e.0).expect("Buffering arm must yield Ok");
                    proptest::prop_assert_eq!(value, buffer_len_u64);
                }
                WritePhase::Streaming { .. } => {
                    let parts_done = (next_part_number - 1) as u64;
                    let expected = parts_done
                        .checked_mul(part_size)
                        .and_then(|base| base.checked_add(buffer_len_u64));
                    match expected {
                        Some(sum) => {
                            let value = result
                                .map_err(|e| e.0)
                                .expect("Streaming arm must yield Ok when the sum fits in u64");
                            proptest::prop_assert_eq!(value, sum);
                        }
                        None => {
                            if saturating {
                                let value = result
                                    .map_err(|e| e.0)
                                    .expect("Saturating Streaming overflow must return Ok(u64::MAX)");
                                proptest::prop_assert_eq!(value, u64::MAX);
                            } else {
                                proptest::prop_assert!(
                                    matches!(
                                        &result,
                                        Err(err) if matches!(err.0, StatusCode::Failure)
                                    ),
                                    "Strict Streaming overflow must return Err(Failure), got {:?}",
                                    result,
                                );
                            }
                        }
                    }
                }
                WritePhase::Failed { .. } => {
                    if saturating {
                        let value = result
                            .map_err(|e| e.0)
                            .expect("Saturating Failed arm must return Ok(0)");
                        proptest::prop_assert_eq!(value, 0u64);
                    } else {
                        proptest::prop_assert!(
                            matches!(
                                &result,
                                Err(err) if matches!(err.0, StatusCode::Failure)
                            ),
                            "Strict Failed arm must return Err(Failure), got {:?}",
                            result,
                        );
                    }
                }
            }
        }
    }
    /// Streaming with abort_authorized=true at parts limit: transition to Failed, flag preserved.
    #[tokio::test]
    async fn flush_one_part_parts_limit_keeps_abort_authorized_true() {
        let backend = Arc::new(DummyBackend::new());
        let driver = build_driver(backend, TEST_PART_SIZE);
        let mut phase = WritePhase::Streaming {
            upload_id: "UP-OVER".to_string(),
            abort_authorized: true,
            part_buffer: vec![0u8; TEST_PART_SIZE as usize],
            uploaded_parts: Vec::new(),
            next_part_number: S3_MAX_MULTIPART_PARTS + 1,
        };
        let err = driver
            .write_dispatch_flush_one_part(&mut phase, "b", "k", TEST_PART_SIZE)
            .await
            .expect_err("parts-limit breach must return Err");
        assert!(matches!(err.0, StatusCode::Failure));
        let WritePhase::Failed {
            upload_id,
            abort_authorized,
        } = phase
        else {
            panic!("phase must transition to Failed on parts-limit breach");
        };
        assert_eq!(upload_id, "UP-OVER");
        assert!(abort_authorized, "Failed variant must carry abort_authorized=true forward");
    }

    /// Streaming with abort_authorized=false at parts limit: transition to Failed, flag preserved.
    #[tokio::test]
    async fn flush_one_part_parts_limit_keeps_abort_authorized_false() {
        let backend = Arc::new(DummyBackend::new());
        let driver = build_driver(backend, TEST_PART_SIZE);
        let mut phase = WritePhase::Streaming {
            upload_id: "UP-OVER-DENY".to_string(),
            abort_authorized: false,
            part_buffer: vec![0u8; TEST_PART_SIZE as usize],
            uploaded_parts: Vec::new(),
            next_part_number: S3_MAX_MULTIPART_PARTS + 1,
        };
        let err = driver
            .write_dispatch_flush_one_part(&mut phase, "b", "k", TEST_PART_SIZE)
            .await
            .expect_err("parts-limit breach must return Err even when abort_authorized=false");
        assert!(matches!(err.0, StatusCode::Failure));
        let WritePhase::Failed { abort_authorized, .. } = phase else {
            panic!("phase must transition to Failed");
        };
        assert!(!abort_authorized, "Deny-cached abort_authorized must survive the transition");
    }

    // --- UploadPart failure inside write_dispatch_flush_one_part ---

    /// UploadPart backend error: transition to Failed carrying upload_id and abort_authorized.
    #[tokio::test]
    async fn flush_one_part_upload_err_transitions_to_failed() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_upload_part_err(DummyError::Injected("upload_part backend failure".to_string()));
        let driver = build_driver(backend.clone(), TEST_PART_SIZE);
        let mut phase = WritePhase::Streaming {
            upload_id: "UP-FLUSH-ERR".to_string(),
            abort_authorized: true,
            part_buffer: vec![0u8; TEST_PART_SIZE as usize],
            uploaded_parts: Vec::new(),
            next_part_number: 1,
        };
        let err =
            with_test_auth_override(|_, _, _| true, driver.write_dispatch_flush_one_part(&mut phase, "b", "k", TEST_PART_SIZE))
                .await
                .expect_err("upload_part failure must propagate as Err");
        assert!(matches!(err.0, StatusCode::Failure));
        let WritePhase::Failed {
            upload_id,
            abort_authorized,
        } = phase
        else {
            panic!("phase must transition to Failed after UploadPart error");
        };
        assert_eq!(upload_id, "UP-FLUSH-ERR");
        assert!(abort_authorized, "abort_authorized must be carried into Failed");
        assert_eq!(
            backend.upload_part_calls().len(),
            1,
            "the failed UploadPart must still have been dispatched once"
        );
    }

    // --- write_dispatch_begin_streaming ---

    /// begin_streaming installs a tombstone before any subsequent await
    /// so a cancelled future leaves a recoverable handle for Drop to abort.
    #[tokio::test]
    async fn begin_streaming_installs_tombstone_before_await() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_create_multipart_upload_ok("UP-BEG-1");
        let mut driver = build_driver(backend.clone(), TEST_PART_SIZE);

        // Pre-populate the handle map so the tombstone lands under the
        // same id we query afterwards.
        let handle_id = driver
            .allocate_handle(write_handle("b", "k", WritePhase::Buffering { part_buffer: Vec::new() }))
            .expect("allocate");

        // The test owns the phase local so we can observe the
        // Buffering->Streaming transition independently of the handle
        // table the driver maintains.
        let mut phase = WritePhase::Buffering {
            part_buffer: vec![1, 2, 3, 4],
        };

        with_test_auth_override(
            |_, _, _| true,
            driver.write_dispatch_begin_streaming(&handle_id, &mut phase, "b", "k", &FileAttributes::empty()),
        )
        .await
        .expect("begin_streaming must succeed on queued Create Ok");

        // Tombstone invariant: the driver.handles entry under handle_id
        // is now a Failed-variant HandleState carrying the upload_id.
        let tombstone = driver.handles.get(&handle_id).expect("tombstone present in handle map");
        let HandleState::Write {
            phase: WritePhase::Failed { upload_id, .. },
            ..
        } = tombstone
        else {
            panic!("tombstone must be a Write handle with Failed phase");
        };
        assert_eq!(upload_id, "UP-BEG-1");

        // Local phase transitioned to Streaming and preserved the buffered bytes.
        let WritePhase::Streaming {
            upload_id: streaming_upload_id,
            part_buffer,
            next_part_number,
            ..
        } = phase
        else {
            panic!("local phase must be Streaming after begin_streaming");
        };
        assert_eq!(streaming_upload_id, "UP-BEG-1");
        assert_eq!(part_buffer, vec![1, 2, 3, 4], "buffered bytes must survive the transition");
        assert_eq!(next_part_number, 1, "next_part_number starts at 1 on entry to Streaming");
    }

    // --- start_multipart_upload ---

    #[tokio::test]
    async fn start_multipart_upload_caches_abort_authorized_true_on_allow() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_create_multipart_upload_ok("UP-ALLOW");
        let driver = build_driver(backend, TEST_PART_SIZE);

        let mp = with_test_auth_override(|_, _, _| true, driver.start_multipart_upload("b", "k", &FileAttributes::empty()))
            .await
            .expect("start_multipart_upload must succeed on Allow");
        assert_eq!(mp.upload_id, "UP-ALLOW");
        assert!(mp.abort_authorized, "Allow on AbortMultipartUpload probe must cache as true");
    }

    #[tokio::test]
    async fn start_multipart_upload_preserves_open_attrs_as_metadata() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_create_multipart_upload_ok("UP-ATTRS");
        let driver = build_driver(backend.clone(), TEST_PART_SIZE);
        let attrs = FileAttributes {
            size: None,
            uid: Some(1000),
            gid: Some(1001),
            user: None,
            group: None,
            permissions: Some(0o100640),
            atime: None,
            mtime: Some(1_777_992_333),
        };

        with_test_auth_override(|_, _, _| true, driver.start_multipart_upload("b", "k", &attrs))
            .await
            .expect("start_multipart_upload must succeed");

        let calls = backend.create_multipart_calls();
        assert_eq!(calls.len(), 1);
        let metadata = calls[0].metadata.as_ref().expect("OPEN attrs must become S3 user metadata");
        assert_eq!(metadata.get("mtime").map(String::as_str), Some("1777992333"));
        assert_eq!(metadata.get("mode").map(String::as_str), Some("33184"));
        assert_eq!(metadata.get("uid").map(String::as_str), Some("1000"));
        assert_eq!(metadata.get("gid").map(String::as_str), Some("1001"));
    }

    #[tokio::test]
    async fn start_multipart_upload_omits_metadata_when_open_attrs_empty() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_create_multipart_upload_ok("UP-NO-ATTRS");
        let driver = build_driver(backend.clone(), TEST_PART_SIZE);

        with_test_auth_override(|_, _, _| true, driver.start_multipart_upload("b", "k", &FileAttributes::empty()))
            .await
            .expect("start_multipart_upload must succeed");

        let calls = backend.create_multipart_calls();
        assert_eq!(calls.len(), 1);
        assert!(calls[0].metadata.is_none(), "empty OPEN attrs must not write default metadata");
    }

    #[tokio::test]
    async fn start_multipart_upload_caches_abort_authorized_false_on_deny_abort() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_create_multipart_upload_ok("UP-DENY-ABORT");
        let driver = build_driver(backend, TEST_PART_SIZE);

        // Allow CreateMultipartUpload, deny AbortMultipartUpload. Mirrors
        // a WORM-shaped IAM policy a principal can meet in production.
        let mp = with_test_auth_override(
            |action, _bucket, _object| !matches!(action, S3Action::AbortMultipartUpload),
            driver.start_multipart_upload("b", "k", &FileAttributes::empty()),
        )
        .await
        .expect("Create Allow must succeed even when Abort is Deny");
        assert_eq!(mp.upload_id, "UP-DENY-ABORT");
        assert!(
            !mp.abort_authorized,
            "Deny on AbortMultipartUpload probe must cache as false so Drop skips the abort"
        );
    }

    #[tokio::test]
    async fn start_multipart_upload_returns_err_when_create_authorize_denies() {
        let backend = Arc::new(DummyBackend::new());
        // No queued response: if the driver bypassed the authorize gate
        // it would hit DummyError::Unconfigured, which is not what we
        // assert here. The PermissionDenied from auth_err is the
        // expected outcome.
        let driver = build_driver(backend.clone(), TEST_PART_SIZE);

        let err = with_test_auth_override(
            |action, _, _| !matches!(action, S3Action::CreateMultipartUpload),
            driver.start_multipart_upload("b", "k", &FileAttributes::empty()),
        )
        .await
        .expect_err("Deny on CreateMultipartUpload must fail fast");
        assert!(matches!(err.0, StatusCode::PermissionDenied));
        assert!(
            backend.upload_part_calls().is_empty(),
            "Create authorize failure must not reach any backend call"
        );
    }

    #[tokio::test]
    async fn multipart_copy_starts_upload_without_sftp_open_metadata() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_create_multipart_upload_ok("UP-COPY");
        backend.queue_upload_part_copy_ok("etag-copy-1");
        backend.queue_complete_multipart_upload_ok();
        let driver = build_driver(backend.clone(), TEST_PART_SIZE);

        with_test_auth_override(
            |_, _, _| true,
            driver.multipart_copy("src-bucket", "src-key", "dst-bucket", "dst-key", TEST_PART_SIZE),
        )
        .await
        .expect("multipart copy must complete");

        let create_calls = backend.create_multipart_calls();
        assert_eq!(create_calls.len(), 1);
        assert_eq!(create_calls[0].bucket, "dst-bucket");
        assert_eq!(create_calls[0].key, "dst-key");
        assert!(
            create_calls[0].metadata.is_none(),
            "server-side multipart copy is not an SFTP OPEN path and must not write OPEN metadata"
        );

        let complete_calls = backend.complete_multipart_calls();
        assert_eq!(complete_calls.len(), 1);
        assert_eq!(complete_calls[0].upload_id, "UP-COPY");
        assert_eq!(complete_calls[0].part_count, 1);
        assert!(backend.abort_multipart_calls().is_empty(), "successful multipart copy must not abort");
    }

    // --- upload_multipart_bytes ---

    #[tokio::test]
    async fn upload_multipart_bytes_returns_err_when_response_lacks_etag() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_upload_part_ok_without_etag();
        let driver = build_driver(backend.clone(), TEST_PART_SIZE);

        let result =
            with_test_auth_override(|_, _, _| true, driver.upload_multipart_bytes("b", "k", "UP-NO-ETAG", 1, vec![0u8; 8])).await;
        let Err(err) = result else {
            panic!("missing ETag must not silently succeed");
        };
        assert!(matches!(err.0, StatusCode::Failure));
    }

    #[tokio::test]
    async fn upload_multipart_bytes_records_part_and_etag_on_success() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_upload_part_ok("etag-part-1");
        let driver = build_driver(backend.clone(), TEST_PART_SIZE);

        let completed =
            with_test_auth_override(|_, _, _| true, driver.upload_multipart_bytes("b", "k", "UP-OK", 1, vec![0u8; 8]))
                .await
                .expect("upload_part Ok must succeed");
        assert_eq!(completed.part_number, 1);
        let ETag::Strong(etag) = completed.e_tag else {
            panic!("DummyBackend queued a Strong ETag");
        };
        assert_eq!(etag, "etag-part-1");

        let calls = backend.upload_part_calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].upload_id, "UP-OK");
        assert_eq!(calls[0].part_number, 1);
        assert_eq!(calls[0].content_length, Some(8));
    }

    // --- close_streaming branches ---

    #[tokio::test]
    async fn close_streaming_parts_limit_breach_skips_abort_when_deny_cached() {
        let backend = Arc::new(DummyBackend::new());
        let driver = build_driver(backend.clone(), TEST_PART_SIZE);

        // abort_authorized=false so close_abort_or_skip takes the skip
        // branch. Skip branch never calls the backend, so no authorize
        // override is needed. The returned Err propagates up.
        let err = driver
            .close_streaming(
                "b",
                "k",
                "UP-CAP-DENY".to_string(),
                false,
                vec![1u8; 16],
                Vec::new(),
                S3_MAX_MULTIPART_PARTS + 1,
            )
            .await
            .expect_err("parts-limit breach must return Err");
        assert!(matches!(err.0, StatusCode::Failure));
        assert!(
            backend.abort_multipart_calls().is_empty(),
            "Deny-cached abort_authorized must take the skip-log path, not the backend call"
        );
    }

    #[tokio::test]
    async fn close_streaming_parts_limit_breach_calls_abort_when_allow_cached() {
        let backend = Arc::new(DummyBackend::new());
        let driver = build_driver(backend.clone(), TEST_PART_SIZE);

        let err = with_test_auth_override(
            |_, _, _| true,
            driver.close_streaming(
                "b",
                "k",
                "UP-CAP-ALLOW".to_string(),
                true,
                vec![1u8; 16],
                Vec::new(),
                S3_MAX_MULTIPART_PARTS + 1,
            ),
        )
        .await
        .expect_err("parts-limit breach returns Err even when abort is Allow");
        assert!(matches!(err.0, StatusCode::Failure));
        let calls = backend.abort_multipart_calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].upload_id, "UP-CAP-ALLOW");
    }

    #[tokio::test]
    async fn close_streaming_complete_multipart_ok_returns_ok_without_abort() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_upload_part_ok("etag-tail");
        backend.queue_complete_multipart_upload_ok();
        let driver = build_driver(backend.clone(), TEST_PART_SIZE);

        with_test_auth_override(
            |_, _, _| true,
            driver.close_streaming("b", "k", "UP-COMPLETE-OK".to_string(), true, vec![0u8; 16], Vec::new(), 1),
        )
        .await
        .expect("close_streaming must return Ok on Complete success");
        assert!(backend.abort_multipart_calls().is_empty(), "successful Complete must not fire an abort");
        assert_eq!(backend.complete_multipart_calls().len(), 1);
    }

    #[tokio::test]
    async fn close_streaming_complete_multipart_err_calls_abort() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_upload_part_ok("etag-tail");
        backend.queue_complete_multipart_upload_err(DummyError::Injected("complete failed".to_string()));
        let driver = build_driver(backend.clone(), TEST_PART_SIZE);

        let err = with_test_auth_override(
            |_, _, _| true,
            driver.close_streaming("b", "k", "UP-COMPLETE-ERR".to_string(), true, vec![0u8; 16], Vec::new(), 1),
        )
        .await
        .expect_err("Complete failure must propagate");
        assert!(matches!(err.0, StatusCode::Failure));
        let aborts = backend.abort_multipart_calls();
        assert_eq!(aborts.len(), 1, "Complete failure must trigger one abort");
        assert_eq!(aborts[0].upload_id, "UP-COMPLETE-ERR");
    }

    #[tokio::test]
    async fn close_streaming_trailing_upload_part_err_calls_abort() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_upload_part_err(DummyError::Injected("trailing upload failed".to_string()));
        let driver = build_driver(backend.clone(), TEST_PART_SIZE);

        let err = with_test_auth_override(
            |_, _, _| true,
            driver.close_streaming("b", "k", "UP-TAIL-ERR".to_string(), true, vec![0u8; 16], Vec::new(), 1),
        )
        .await
        .expect_err("trailing UploadPart failure must propagate");
        assert!(matches!(err.0, StatusCode::Failure));
        let aborts = backend.abort_multipart_calls();
        assert_eq!(aborts.len(), 1, "trailing UploadPart failure must trigger one abort");
        assert_eq!(aborts[0].upload_id, "UP-TAIL-ERR");
        assert!(
            backend.complete_multipart_calls().is_empty(),
            "Complete must not be called when the tail UploadPart failed"
        );
    }

    #[tokio::test]
    async fn commit_write_preserves_open_attrs_as_metadata() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_put_object_ok();
        let driver = build_driver(backend.clone(), TEST_PART_SIZE);
        let attrs = FileAttributes {
            size: None,
            uid: Some(501),
            gid: Some(20),
            user: None,
            group: None,
            permissions: Some(0o100600),
            atime: None,
            mtime: Some(1_777_992_348),
        };

        driver
            .commit_write("b", "k", &attrs, b"hello".to_vec())
            .await
            .expect("commit_write must succeed");

        let calls = backend.put_object_calls();
        assert_eq!(calls.len(), 1);
        let metadata = calls[0].metadata.as_ref().expect("OPEN attrs must become S3 user metadata");
        assert_eq!(metadata.get("mtime").map(String::as_str), Some("1777992348"));
        assert_eq!(metadata.get("mode").map(String::as_str), Some("33152"));
        assert_eq!(metadata.get("uid").map(String::as_str), Some("501"));
        assert_eq!(metadata.get("gid").map(String::as_str), Some("20"));
    }

    #[tokio::test]
    async fn commit_write_omits_metadata_when_open_attrs_empty() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_put_object_ok();
        let driver = build_driver(backend.clone(), TEST_PART_SIZE);

        driver
            .commit_write("b", "k", &FileAttributes::empty(), b"hello".to_vec())
            .await
            .expect("commit_write must succeed");

        let calls = backend.put_object_calls();
        assert_eq!(calls.len(), 1);
        assert!(calls[0].metadata.is_none(), "empty OPEN attrs must not write default metadata");
    }

    // --- open_write strict-flag gate ---

    #[tokio::test]
    async fn open_write_write_only_returns_op_unsupported() {
        // OpenFlags::WRITE without CREATE or TRUNCATE is rejected at
        // OPEN. No HEAD call is issued because the gate is flag-only.
        let backend = Arc::new(DummyBackend::new());
        let mut driver = build_driver(backend.clone(), TEST_PART_SIZE);

        let err = with_test_auth_override(
            |_, _, _| true,
            driver.open_write(7, "/bucket/key", OpenFlags::WRITE, FileAttributes::default()),
        )
        .await
        .expect_err("WRITE without CREATE or TRUNCATE must be rejected");
        assert!(matches!(err.0, StatusCode::OpUnsupported));
        assert!(
            backend.head_object_calls().is_empty(),
            "no HEAD call should be issued on the strict-flag rejection path"
        );
    }

    #[tokio::test]
    async fn open_write_create_without_trunc_returns_op_unsupported() {
        // WRITE | CREATE without TRUNCATE is rejected at OPEN. The
        // streaming write path cannot honour create-or-modify-existing
        // semantics, so the rejection is unconditional and no HEAD is
        // issued.
        let backend = Arc::new(DummyBackend::new());
        let mut driver = build_driver(backend.clone(), TEST_PART_SIZE);

        let err = with_test_auth_override(
            |_, _, _| true,
            driver.open_write(7, "/bucket/key", OpenFlags::WRITE | OpenFlags::CREATE, FileAttributes::default()),
        )
        .await
        .expect_err("WRITE | CREATE without TRUNCATE must be rejected");
        assert!(matches!(err.0, StatusCode::OpUnsupported));
        assert!(
            backend.head_object_calls().is_empty(),
            "no HEAD call should be issued on the strict-flag rejection path"
        );
    }

    #[tokio::test]
    async fn open_write_create_and_trunc_succeeds_on_missing_file() {
        // WRITE | CREATE | TRUNCATE on a missing file: no HEAD
        // response is queued. If the OPEN attempted a HEAD it would
        // trigger a DummyBackend panic. The successful return proves
        // the non-EXCL accept path allocates a handle without
        // consulting backend object state.
        let backend = Arc::new(DummyBackend::new());
        let mut driver = build_driver(backend.clone(), TEST_PART_SIZE);

        let handle = with_test_auth_override(
            |_, _, _| true,
            driver.open_write(
                9,
                "/bucket/missing_key",
                OpenFlags::WRITE | OpenFlags::CREATE | OpenFlags::TRUNCATE,
                FileAttributes::default(),
            ),
        )
        .await
        .expect("WRITE | CREATE | TRUNCATE on a missing file must allocate a handle");
        assert!(!handle.handle.is_empty());
        assert!(backend.head_object_calls().is_empty(), "the non-EXCL accept path must not HEAD");
    }

    #[tokio::test]
    async fn open_write_create_and_trunc_succeeds_on_existing_file() {
        // WRITE | CREATE | TRUNCATE on an existing file: queue a
        // HEAD-Ok response. The OPEN succeeds and the queued HEAD
        // response is not consumed, proving the non-EXCL accept path
        // is independent of backend object state.
        let backend = Arc::new(DummyBackend::new());
        backend.queue_head_object_ok(42, None);
        let mut driver = build_driver(backend.clone(), TEST_PART_SIZE);

        let handle = with_test_auth_override(
            |_, _, _| true,
            driver.open_write(
                10,
                "/bucket/existing_key",
                OpenFlags::WRITE | OpenFlags::CREATE | OpenFlags::TRUNCATE,
                FileAttributes::default(),
            ),
        )
        .await
        .expect("WRITE | CREATE | TRUNCATE on an existing file must allocate a handle");
        assert!(!handle.handle.is_empty());
        assert!(backend.head_object_calls().is_empty(), "the non-EXCL accept path must not HEAD");
    }

    // --- open_write EXCL ---

    #[tokio::test]
    async fn open_write_excl_rejects_existing_object_with_failure() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_head_object_ok(42, None);
        let mut driver = build_driver(backend.clone(), TEST_PART_SIZE);

        let err = with_test_auth_override(
            |_, _, _| true,
            driver.open_write(
                7,
                "/bucket/key",
                OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::EXCLUDE | OpenFlags::WRITE,
                FileAttributes::default(),
            ),
        )
        .await
        .expect_err("EXCL on an existing object must fail");
        assert!(matches!(err.0, StatusCode::Failure));
        let heads = backend.head_object_calls();
        assert_eq!(heads.len(), 1);
        assert_eq!(heads[0].bucket, "bucket");
        assert_eq!(heads[0].key, "key");
    }

    #[tokio::test]
    async fn open_write_excl_allows_creation_when_head_object_not_found() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_head_object_not_found();
        let mut driver = build_driver(backend.clone(), TEST_PART_SIZE);

        let handle = with_test_auth_override(
            |_, _, _| true,
            driver.open_write(
                9,
                "/bucket/key2",
                OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::EXCLUDE | OpenFlags::WRITE,
                FileAttributes::default(),
            ),
        )
        .await
        .expect("EXCL on a missing key must allow creation");
        assert!(!handle.handle.is_empty());
        assert_eq!(backend.head_object_calls().len(), 1, "EXCL must HEAD exactly once");
    }

    // --- abort_upload_with_auth Deny ---

    #[tokio::test]
    async fn abort_upload_with_auth_deny_returns_err_without_backend_call() {
        let backend = Arc::new(DummyBackend::new());
        let driver = build_driver(backend.clone(), TEST_PART_SIZE);

        let err = with_test_auth_override(
            |action, _, _| !matches!(action, S3Action::AbortMultipartUpload),
            driver.abort_upload_with_auth("b", "k", "UP-ABORT-DENY"),
        )
        .await
        .expect_err("Deny on abort must fail before reaching the backend");
        assert!(matches!(err.0, StatusCode::PermissionDenied));
        assert!(backend.abort_multipart_calls().is_empty(), "Deny path must not call the backend");
    }

    // --- full-flow cancellation test ---

    /// Cancel a stalled UploadPart future, drop the driver, verify Drop
    /// finds the tombstone and fires AbortMultipartUpload exactly once.
    #[tokio::test]
    async fn cancel_mid_upload_part_drop_aborts() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_create_multipart_upload_ok("UP-CANCEL");
        let entered = Arc::new(tokio::sync::Notify::new());
        backend.stall_upload_part(entered.clone());

        let mut driver = build_driver(backend.clone(), TEST_PART_SIZE);
        let handle_id = driver
            .allocate_handle(write_handle("b", "k", WritePhase::Buffering { part_buffer: Vec::new() }))
            .expect("allocate");
        let mut state = driver.handles.remove(&handle_id).expect("remove");

        // One full part's worth of bytes so write_dispatch transitions
        // Buffering->Streaming (installs the tombstone synchronously)
        // and then calls upload_part, which the DummyBackend stalls.
        let data = vec![0u8; TEST_PART_SIZE as usize];
        let write_fut = driver.write_dispatch(&handle_id, &mut state, 0, data);

        with_test_auth_override(|_, _, _| true, async {
            tokio::select! {
                biased;
                _ = entered.notified() => {
                    // upload_part has been entered. Fall through so
                    // write_fut is dropped on exit from this block.
                }
                _ = write_fut => {
                    panic!("write_dispatch must stall inside upload_part, not complete");
                }
            }
        })
        .await;

        // state is dropped unrestored. The tombstone that
        // write_dispatch_begin_streaming installed remains in driver.handles.
        drop(state);

        // Confirm the tombstone is actually sitting in the map before Drop runs.
        let pre_drop = driver.handles.get(&handle_id).expect("tombstone must survive cancellation");
        let HandleState::Write {
            phase: WritePhase::Failed {
                upload_id,
                abort_authorized,
            },
            ..
        } = pre_drop
        else {
            panic!("surviving handle must be a Failed tombstone");
        };
        assert_eq!(upload_id, "UP-CANCEL");
        assert!(*abort_authorized, "Allow cached at Create must make Drop fire abort");

        // Dropping the driver spawns an abort task on the current-thread
        // runtime. yield_now lets the spawned task poll to completion
        // against the DummyBackend (which returns Ok synchronously).
        drop(driver);
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        let aborts = backend.abort_multipart_calls();
        assert_eq!(aborts.len(), 1, "Drop must fire exactly one abort for the surviving tombstone");
        let AbortCall { bucket, key, upload_id } = &aborts[0];
        assert_eq!(bucket, "b");
        assert_eq!(key, "k");
        assert_eq!(upload_id, "UP-CANCEL");
    }

    // --- compliance matrix cross-reference tests ---
    //
    // Row 5 SSH_FXP_READ: boundary cases (pre-IAM) plus happy path.
    // Row 8 SSH_FXP_FSTAT: cached attrs on File and running count on Write.
    // Row 10 SSH_FXP_FSETSTAT: unconditional ok_status.

    /// commit_write against a stalling put_object must return Failure
    /// within the configured backend deadline. Anchors the C1 contract:
    /// a backend that accepted the request and never returned a body
    /// surfaces as Failure on the wire rather than blocking the
    /// session indefinitely. The 1 s deadline keeps the test runtime
    /// under two seconds while still covering the deadline-elapsed
    /// branch end to end (driver setup, run_backend wrap, stalling
    /// put_object, Failure emission).
    #[tokio::test(flavor = "current_thread")]
    async fn commit_write_returns_failure_when_put_object_stalls_past_deadline() {
        let backend = Arc::new(DummyBackend::new());
        let entered = Arc::new(Notify::new());
        backend.stall_put_object(entered.clone());

        let driver = build_driver_with_timeout(backend, TEST_PART_SIZE, 1);

        // Outer guard timeout is generously above the inner deadline
        // so the assertion failure mode distinguishes "driver did not
        // honour the deadline" (outer fires) from "deadline fired but
        // mapped to the wrong status" (Ok(Err) with non-Failure).
        let outcome = tokio::time::timeout(
            Duration::from_secs(10),
            driver.commit_write("b", "k", &FileAttributes::default(), b"hello".to_vec()),
        )
        .await;

        let inner = outcome.expect("driver deadline must fire before the outer 10 s guard");
        let err = inner.expect_err("stalling backend must surface as Err");
        assert!(matches!(err.0, StatusCode::Failure));

        // The stall path notifies entered exactly once when put_object
        // first runs. Confirming the notify fired proves the stall
        // path was actually exercised (rather than the test passing
        // because some earlier validation rejected the call).
        entered.notified().await;
    }

    /// run_backend_with_err exposes the original backend Err to the
    /// caller so EXCLUDE create and HeadObject-then-list fallback
    /// paths can keep their is_not_found_error filters. The
    /// commit_write integration test covers the timeout path. The
    /// error pass-through is covered here.
    #[tokio::test(flavor = "current_thread")]
    async fn run_backend_with_err_passes_backend_error_through_unchanged() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_head_object_err(DummyError::AccessDenied("pinned".to_string()));
        let driver = build_driver(backend, TEST_PART_SIZE);

        let result = driver
            .run_backend_with_err("head_object", driver.storage.head_object("b", "k", "ak", "sk"))
            .await;

        match result {
            Ok(Err(e)) => assert!(matches!(e, DummyError::AccessDenied(_))),
            other => panic!("expected backend Err passed through; got {other:?}"),
        }
    }

    // --- bounded retry around PutObject in commit_write ---

    /// commit_write retries the PutObject call on a transient backend
    /// error and returns Ok once a retry succeeds. SlowDown is in the
    /// rustfs_utils retryable code set, so two SlowDown errors
    /// followed by an Ok exercises the retry loop end to end. The
    /// real backoff schedule (250 + 500 ms) keeps this test under a
    /// second of wall-clock.
    #[tokio::test(flavor = "current_thread")]
    async fn commit_write_retries_on_slow_down_and_succeeds_on_recovery() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_put_object_err(DummyError::Injected("SlowDown: backoff".into()));
        backend.queue_put_object_err(DummyError::Injected("SlowDown: backoff".into()));
        backend.queue_put_object_ok();
        let driver = build_driver(backend.clone(), TEST_PART_SIZE);

        driver
            .commit_write("b", "k", &FileAttributes::default(), b"hello".to_vec())
            .await
            .expect("commit_write must succeed once a retry returns Ok");
        assert_eq!(
            backend.put_object_queue_len(),
            0,
            "all three queued responses must have been consumed (two retryable Errs plus one Ok)"
        );
    }

    /// commit_write surfaces Failure when the backend returns a
    /// retryable error past the cap. COMMIT_WRITE_MAX_RETRIES + 1
    /// SlowDown responses cover the initial attempt plus every retry,
    /// proving the loop bounds.
    #[tokio::test(flavor = "current_thread")]
    async fn commit_write_returns_failure_after_retry_cap_exhausted() {
        let backend = Arc::new(DummyBackend::new());
        for _ in 0..=COMMIT_WRITE_MAX_RETRIES {
            backend.queue_put_object_err(DummyError::Injected("SlowDown: backoff".into()));
        }
        let driver = build_driver(backend.clone(), TEST_PART_SIZE);

        let err = driver
            .commit_write("b", "k", &FileAttributes::default(), b"hello".to_vec())
            .await
            .expect_err("commit_write must surface the final retryable error after the cap");
        assert!(matches!(err.0, StatusCode::Failure));
        assert_eq!(
            backend.put_object_queue_len(),
            0,
            "every queued retryable response must have been consumed by the cap-exhausting attempts"
        );
    }

    /// commit_write must not retry on a terminal error like
    /// AccessDenied. The first call returns AccessDenied; no second
    /// call is issued, and the wire status is PermissionDenied (the
    /// s3_error_to_sftp mapping), not Failure.
    #[tokio::test(flavor = "current_thread")]
    async fn commit_write_does_not_retry_on_access_denied() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_put_object_err(DummyError::AccessDenied("policy".into()));
        // A second response so a retry attempt would surface a
        // wrong-status assertion failure rather than the
        // configured-miss default. If the loop wrongly retries, the
        // second pop is the Ok below and the test sees Ok instead of
        // PermissionDenied.
        backend.queue_put_object_ok();
        let driver = build_driver(backend.clone(), TEST_PART_SIZE);

        let err = driver
            .commit_write("b", "k", &FileAttributes::default(), b"hello".to_vec())
            .await
            .expect_err("commit_write must surface AccessDenied without retrying");
        assert!(matches!(err.0, StatusCode::PermissionDenied));
        assert_eq!(
            backend.put_object_queue_len(),
            1,
            "non-retryable error must not consume a second queued response"
        );
    }
}
