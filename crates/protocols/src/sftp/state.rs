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

//! Per-session state types for the SFTP driver.
//!
//! Operation implementations are defined in the relevant modules
//! (attrs.rs, read.rs, write.rs, dir.rs, driver.rs). state.rs holds
//! only type definitions and associated state definitions.

use super::read_cache::ReadCache;
use russh_sftp::protocol::FileAttributes;
use s3s::dto::ETag;

/// State held per open handle.
///
/// File handles cache the object size so READ can detect end-of-file without
/// re-issuing HeadObject on every call. Directory handles carry the S3
/// continuation token so each READDIR response corresponds to one S3
/// ListObjectsV2 page. This bounds response size without imposing an
/// arbitrary batch limit. Write handles run a multipart state machine:
/// small files buffer in memory and upload via a single PutObject at CLOSE,
/// large files transition to streaming multipart uploads as the buffer fills.
/// See WritePhase for the full state machine.
pub(super) enum HandleState {
    File {
        bucket: String,
        key: String,
        /// Object size captured at OPEN time. READ uses this to return EOF
        /// once the offset reaches or exceeds the end of the object, as
        /// required by SFTPv3 draft section 6.4.
        size: u64,
        /// Attributes captured at OPEN time so FSTAT can answer without a
        /// second HeadObject.
        attrs: FileAttributes,
        /// Per-handle cached chunk of bytes fetched on the previous
        /// READ miss. FXP_READs whose target range sits inside the
        /// cached chunk are served from the buffer without a backend
        /// round trip. Constructed empty in open_read. Dropped when
        /// CLOSE removes the handle from the table, or when the
        /// SftpDriver Drop impl runs at session teardown.
        read_cache: ReadCache,
    },
    Dir(DirCursor),
    Write {
        bucket: String,
        key: String,
        /// Attributes returned by FSTAT against this handle.
        /// The size field tracks the running total of bytes received so
        /// a client polling FSTAT during a transfer sees the progress.
        attrs: FileAttributes,
        /// Raw attributes supplied on OPEN. Only fields explicitly set
        /// by the client are copied into S3 user metadata at object
        /// creation time.
        open_attrs: FileAttributes,
        /// Multipart upload lifecycle state. See WritePhase.
        phase: WritePhase,
    },
}

/// Write-side state machine for a single open write handle.
///
/// Transitions are strictly forward. A handle begins in Buffering. Once the
/// first full part is ready, the driver issues CreateMultipartUpload and
/// transitions to Streaming. On any UploadPart failure the phase moves to
/// Failed, which rejects further writes and releases the upload_id via
/// AbortMultipartUpload at CLOSE. There is no recovery from Failed.
///
///
///     OPEN
///      |
///      v
///   Buffering --CLOSE--> PutObject (small file) ---------> DONE
///      |
///      |  buffer >= part_size
///      |  CreateMultipartUpload ok
///      v
///   Streaming --CLOSE--> UploadPart (tail) then
///      | ^               CompleteMultipartUpload --------> DONE
///      | |               (large file)
///      | |
///      | |  buffer >= part_size
///      | |  UploadPart ok (loop)
///      | |
///      | UploadPart fails
///      v
///    Failed  --CLOSE--> AbortMultipartUpload ---> (handle gone, no object)
///
///   Retry: CreateMultipartUpload fails -> stay in Buffering,
///          retry on next flush.
///
pub(super) enum WritePhase {
    /// No multipart upload has been started. Bytes accumulate in part_buffer.
    /// On CLOSE the buffered bytes upload via a single PutObject. If
    /// CreateMultipartUpload fails on the first full-part flush, the phase
    /// stays in Buffering and the next full-part flush retries the call:
    /// a transient S3 error is invisible to the client.
    Buffering {
        /// Bytes received via WRITE not yet flushed to S3. Bounded by
        /// part_size: the while-loop in write() drains it below part_size
        /// before returning.
        part_buffer: Vec<u8>,
    },
    /// CreateMultipartUpload has been issued. Full parts flush at the
    /// part_size boundary. On CLOSE, the final partial part is uploaded
    /// via UploadPart and the upload is finalised via
    /// CompleteMultipartUpload.
    Streaming {
        /// upload_id returned by CreateMultipartUpload. Required by every
        /// subsequent UploadPart, CompleteMultipartUpload, and
        /// AbortMultipartUpload call.
        upload_id: String,
        /// Cached result of authorize_operation for AbortMultipartUpload,
        /// evaluated at CreateMultipartUpload time. Drop consults this
        /// to decide whether to issue AbortMultipartUpload without
        /// running an async auth call (Drop is synchronous). close()
        /// consults it too for consistency: same policy decision, same
        /// observable outcome. False means the principal's IAM policy
        /// denies AbortMultipartUpload, so cleanup is deferred to the
        /// bucket's AbortIncompleteMultipartUpload lifecycle rule. The
        /// flag is cached for one upload's lifetime: a policy edit
        /// between the cache and the abort attempt is not honoured in
        /// this session.
        abort_authorized: bool,
        /// Bytes received via WRITE not yet flushed to S3.
        part_buffer: Vec<u8>,
        /// Parts already uploaded. Passed to CompleteMultipartUpload in
        /// order. Each entry carries the part number and the ETag returned
        /// by UploadPart.
        uploaded_parts: Vec<CompletedPart>,
        /// Part number to use for the next UploadPart call. S3 part numbers
        /// begin at 1 and increase monotonically.
        next_part_number: i32,
    },
    /// An UploadPart call failed. The upload_id is retained so close()
    /// can call AbortMultipartUpload when policy permits. Further
    /// writes are rejected.
    Failed {
        /// upload_id returned by the CreateMultipartUpload call that opened
        /// the now-failed upload.
        upload_id: String,
        /// Carried forward from Streaming at the point of failure. See
        /// the identically named field on Streaming for the contract.
        abort_authorized: bool,
    },
}

/// Record of one successfully uploaded part. Carries the part number and
/// ETag needed by CompleteMultipartUpload to assemble the final object.
#[derive(Clone)]
pub(super) struct CompletedPart {
    pub(super) part_number: i32,
    pub(super) e_tag: ETag,
}

/// Identifier plus cached abort authorisation for one S3 multipart
/// upload. Holds the upload_id and the result of the AbortMultipartUpload
/// IAM probe issued at CreateMultipartUpload time. Holding the two
/// fields together prevents drift: any code path with the upload_id
/// also has the abort decision in scope without re-probing IAM, and the
/// synchronous Drop on SftpDriver can honour a Deny-Abort policy from
/// the cached flag without an async call.
///
/// Cloneable so a tombstone copy can live in the handle table while a
/// write_dispatch await holds a working copy. The fields are one String
/// and one bool, so cloning is cheap.
#[derive(Clone, Debug)]
pub(super) struct MultipartUpload {
    pub(super) upload_id: String,
    pub(super) abort_authorized: bool,
}

/// Directory iteration state.
///
/// Root lists buckets. ListBuckets is not batched: one response carries
/// every bucket the principal can see. Bucket and prefix listings walk
/// ListObjectsV2 one batch at a time, using continuation_token to cross
/// batch boundaries. The dots_emitted flag ensures the conventional "."
/// and ".." entries are produced exactly once, on the first READDIR call.
///
/// Clone is derived so the READDIR handler can install a cancellation-safety
/// tombstone (the pre-advance cursor) in the handle table before the
/// list_objects_v2 await. A cancelled READDIR leaves the tombstone so the
/// client's next READDIR resumes from the un-advanced position.
#[derive(Clone)]
pub(super) enum DirCursor {
    Root {
        buckets_delivered: bool,
        dots_emitted: bool,
    },
    Listing {
        bucket: String,
        /// Object prefix terminated by "/", or empty when listing the root
        /// of a bucket. S3 list_objects_v2 with a trailing-slash prefix
        /// returns entries immediately under the prefix.
        prefix: String,
        /// Position in the ListObjectsV2 batch walk. Initial before the
        /// first batch, Next(token) between batches, Done once S3 reports
        /// the listing is exhausted.
        continuation: ListingContinuation,
        dots_emitted: bool,
    },
}

/// Position within a batched S3 ListObjectsV2 walk. The state machine is
/// total: every transition arrives at exactly one of these variants.
/// Initial means no batch has been fetched and the next call to
/// next_listing_page issues list_objects_v2 with no continuation_token.
/// Next(token) means a previous batch returned this continuation token
/// and the next call passes it to list_objects_v2 to fetch the following
/// batch. Done means the listing is exhausted and subsequent calls
/// return an empty Vec without a network round trip.
#[derive(Clone)]
pub(super) enum ListingContinuation {
    Initial,
    Next(String),
    Done,
}

#[cfg(test)]
mod tests {
    use super::super::constants::limits::{S3_COPY_OBJECT_MAX_SIZE, S3_MAX_MULTIPART_PARTS, S3_MAX_PART_SIZE, S3_MIN_PART_SIZE};

    #[test]
    fn multipart_constants_match_s3_limits() {
        // S3_COPY_OBJECT_MAX_SIZE 5 GiB is the CopyObject single-shot ceiling.
        // S3_MIN_PART_SIZE 5 MiB is the S3 minimum for non-final parts.
        // S3_MAX_PART_SIZE 5 GiB is the S3 maximum for any single part.
        // S3_MAX_MULTIPART_PARTS 10000 is the S3 cap on parts per upload.
        assert_eq!(S3_COPY_OBJECT_MAX_SIZE, 5 * 1024 * 1024 * 1024);
        assert_eq!(S3_MIN_PART_SIZE, 5 * 1024 * 1024);
        assert_eq!(S3_MAX_PART_SIZE, 5 * 1024 * 1024 * 1024);
        assert_eq!(S3_MAX_MULTIPART_PARTS, 10_000);
    }
}
