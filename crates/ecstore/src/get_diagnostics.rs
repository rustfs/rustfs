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

use crate::disk::error::DiskError;
use crate::error::StorageError;
use std::io;

pub(crate) const GET_OBJECT_PATH_CODEC_STREAMING: &str = "codec_streaming";
pub(crate) const GET_OBJECT_PATH_EMPTY: &str = "empty";
pub(crate) const GET_OBJECT_PATH_LEGACY_DUPLEX: &str = "legacy_duplex";
pub(crate) const GET_OBJECT_PATH_REMOTE_TRANSITION: &str = "remote_transition";

pub(crate) const GET_STAGE_DECODE: &str = "decode";
pub(crate) const GET_STAGE_EMIT: &str = "emit";
pub(crate) const GET_STAGE_FILL: &str = "fill";
pub(crate) const GET_STAGE_METADATA: &str = "metadata";
pub(crate) const GET_STAGE_OUTPUT_LOCK_WAIT: &str = "output_lock_wait";
pub(crate) const GET_STAGE_OUTPUT_POLL: &str = "output_poll";
pub(crate) const GET_STAGE_RANGE: &str = "range";
pub(crate) const GET_STAGE_READER_SETUP: &str = "reader_setup";
pub(crate) const GET_STAGE_RECONSTRUCT: &str = "reconstruct";
pub(crate) const GET_STAGE_STRIPE_READ: &str = "stripe_read";
pub(crate) const GET_STAGE_STRIPE_READ_FIRST_SHARD: &str = "stripe_read_first_shard";
pub(crate) const GET_STAGE_STRIPE_READ_QUORUM: &str = "stripe_read_quorum";

pub(crate) const GET_READER_BUFFER_OUTPUT: &str = "output";
pub(crate) const GET_READER_BUFFER_PREFETCH: &str = "prefetch";
pub(crate) const GET_READER_PREFETCH_DIRECT: &str = "direct";
pub(crate) const GET_READER_PREFETCH_EOF: &str = "eof";
pub(crate) const GET_READER_PREFETCH_ERROR_DEFERRED: &str = "error_deferred";
pub(crate) const GET_READER_PREFETCH_ERROR_IMMEDIATE: &str = "error_immediate";
pub(crate) const GET_READER_PREFETCH_STORED: &str = "stored";
pub(crate) const GET_SHARD_READ_OUTCOME_ERROR: &str = "error";
pub(crate) const GET_SHARD_READ_OUTCOME_MISSING: &str = "missing";
pub(crate) const GET_SHARD_READ_OUTCOME_SUCCESS: &str = "success";
pub(crate) const GET_SHARD_ROLE_DATA: &str = "data";
pub(crate) const GET_SHARD_ROLE_PARITY: &str = "parity";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GetObjectFailureReason {
    BitrotMismatch,
    DecodeError,
    DownstreamClosed,
    Io,
    RangeOrLengthInvalid,
    ReadQuorum,
    ShortRead,
    Timeout,
    Unknown,
}

impl GetObjectFailureReason {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::BitrotMismatch => "bitrot_mismatch",
            Self::DecodeError => "decode_error",
            Self::DownstreamClosed => "downstream_closed",
            Self::Io => "io",
            Self::RangeOrLengthInvalid => "range_or_length_invalid",
            Self::ReadQuorum => "read_quorum",
            Self::ShortRead => "short_read",
            Self::Timeout => "timeout",
            Self::Unknown => "unknown",
        }
    }
}

pub(crate) fn classify_storage_error(err: &StorageError) -> GetObjectFailureReason {
    match err {
        StorageError::ErasureReadQuorum | StorageError::InsufficientReadQuorum(_, _) => GetObjectFailureReason::ReadQuorum,
        StorageError::FileCorrupt => GetObjectFailureReason::BitrotMismatch,
        StorageError::InvalidRangeSpec(_) => GetObjectFailureReason::RangeOrLengthInvalid,
        StorageError::Io(io_err) => classify_io_error(io_err),
        _ => GetObjectFailureReason::Unknown,
    }
}

pub(crate) fn classify_disk_error(err: &DiskError) -> GetObjectFailureReason {
    match err {
        DiskError::ErasureReadQuorum => GetObjectFailureReason::ReadQuorum,
        DiskError::FileCorrupt | DiskError::PartMissingOrCorrupt => GetObjectFailureReason::BitrotMismatch,
        DiskError::LessData => GetObjectFailureReason::ShortRead,
        DiskError::Timeout => GetObjectFailureReason::Timeout,
        DiskError::Io(io_err) => classify_io_error(io_err),
        _ => GetObjectFailureReason::Unknown,
    }
}

pub(crate) fn classify_io_error(err: &io::Error) -> GetObjectFailureReason {
    match err.kind() {
        io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset => GetObjectFailureReason::DownstreamClosed,
        io::ErrorKind::TimedOut => GetObjectFailureReason::Timeout,
        io::ErrorKind::UnexpectedEof => GetObjectFailureReason::ShortRead,
        io::ErrorKind::InvalidInput | io::ErrorKind::InvalidData => GetObjectFailureReason::RangeOrLengthInvalid,
        _ => GetObjectFailureReason::Io,
    }
}

pub(crate) fn record_get_object_pipeline_failure(stage: &'static str, reason: GetObjectFailureReason) {
    rustfs_io_metrics::record_get_object_pipeline_failure(stage, reason.as_str());
}

pub(crate) fn record_get_object_pipeline_failure_for_path(
    path: &'static str,
    stage: &'static str,
    reason: GetObjectFailureReason,
) {
    rustfs_io_metrics::record_get_object_pipeline_failure_for_path(path, stage, reason.as_str());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_storage_errors_for_get_pipeline() {
        assert_eq!(
            classify_storage_error(&StorageError::ErasureReadQuorum),
            GetObjectFailureReason::ReadQuorum
        );
        assert_eq!(
            classify_storage_error(&StorageError::InsufficientReadQuorum("bucket".to_string(), "object".to_string(),)),
            GetObjectFailureReason::ReadQuorum
        );
        assert_eq!(classify_storage_error(&StorageError::FileCorrupt), GetObjectFailureReason::BitrotMismatch);
        assert_eq!(
            classify_storage_error(&StorageError::InvalidRangeSpec("bad range".to_string())),
            GetObjectFailureReason::RangeOrLengthInvalid
        );
    }

    #[test]
    fn classifies_disk_errors_for_get_pipeline() {
        assert_eq!(classify_disk_error(&DiskError::ErasureReadQuorum), GetObjectFailureReason::ReadQuorum);
        assert_eq!(classify_disk_error(&DiskError::FileCorrupt), GetObjectFailureReason::BitrotMismatch);
        assert_eq!(
            classify_disk_error(&DiskError::PartMissingOrCorrupt),
            GetObjectFailureReason::BitrotMismatch
        );
        assert_eq!(classify_disk_error(&DiskError::LessData), GetObjectFailureReason::ShortRead);
        assert_eq!(classify_disk_error(&DiskError::Timeout), GetObjectFailureReason::Timeout);
    }

    #[test]
    fn classifies_io_errors_for_get_pipeline() {
        let cases = [
            (io::ErrorKind::BrokenPipe, GetObjectFailureReason::DownstreamClosed),
            (io::ErrorKind::ConnectionReset, GetObjectFailureReason::DownstreamClosed),
            (io::ErrorKind::TimedOut, GetObjectFailureReason::Timeout),
            (io::ErrorKind::UnexpectedEof, GetObjectFailureReason::ShortRead),
            (io::ErrorKind::InvalidInput, GetObjectFailureReason::RangeOrLengthInvalid),
            (io::ErrorKind::InvalidData, GetObjectFailureReason::RangeOrLengthInvalid),
            (io::ErrorKind::Other, GetObjectFailureReason::Io),
        ];

        for (kind, expected) in cases {
            let err = io::Error::from(kind);
            assert_eq!(classify_io_error(&err), expected, "kind={kind:?}");
        }
    }

    #[test]
    fn keeps_metric_labels_stable() {
        assert_eq!(GetObjectFailureReason::ReadQuorum.as_str(), "read_quorum");
        assert_eq!(GetObjectFailureReason::ShortRead.as_str(), "short_read");
        assert_eq!(GetObjectFailureReason::DownstreamClosed.as_str(), "downstream_closed");
        assert_eq!(GetObjectFailureReason::BitrotMismatch.as_str(), "bitrot_mismatch");
        assert_eq!(GetObjectFailureReason::DecodeError.as_str(), "decode_error");
        assert_eq!(GET_READER_BUFFER_OUTPUT, "output");
        assert_eq!(GET_READER_BUFFER_PREFETCH, "prefetch");
        assert_eq!(GET_READER_PREFETCH_DIRECT, "direct");
        assert_eq!(GET_READER_PREFETCH_STORED, "stored");
        assert_eq!(GET_READER_PREFETCH_EOF, "eof");
        assert_eq!(GET_READER_PREFETCH_ERROR_DEFERRED, "error_deferred");
        assert_eq!(GET_READER_PREFETCH_ERROR_IMMEDIATE, "error_immediate");
        assert_eq!(GET_STAGE_DECODE, "decode");
        assert_eq!(GET_STAGE_EMIT, "emit");
        assert_eq!(GET_STAGE_FILL, "fill");
        assert_eq!(GET_STAGE_METADATA, "metadata");
        assert_eq!(GET_STAGE_OUTPUT_LOCK_WAIT, "output_lock_wait");
        assert_eq!(GET_STAGE_OUTPUT_POLL, "output_poll");
        assert_eq!(GET_STAGE_RANGE, "range");
        assert_eq!(GET_STAGE_READER_SETUP, "reader_setup");
        assert_eq!(GET_STAGE_RECONSTRUCT, "reconstruct");
        assert_eq!(GET_STAGE_STRIPE_READ, "stripe_read");
        assert_eq!(GET_STAGE_STRIPE_READ_FIRST_SHARD, "stripe_read_first_shard");
        assert_eq!(GET_STAGE_STRIPE_READ_QUORUM, "stripe_read_quorum");
        assert_eq!(GET_SHARD_READ_OUTCOME_ERROR, "error");
        assert_eq!(GET_SHARD_READ_OUTCOME_MISSING, "missing");
        assert_eq!(GET_SHARD_READ_OUTCOME_SUCCESS, "success");
        assert_eq!(GET_SHARD_ROLE_DATA, "data");
        assert_eq!(GET_SHARD_ROLE_PARITY, "parity");
    }
}
