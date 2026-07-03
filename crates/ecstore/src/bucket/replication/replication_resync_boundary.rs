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

use super::replication_error_boundary::{Error, Result};
use super::replication_filemeta_boundary::MrfReplicateEntry;

pub use rustfs_replication::{BucketReplicationResyncStatus, ResyncOpts, TargetReplicationResyncStatus};
pub(crate) use rustfs_replication::{
    is_version_id_mismatch, resync_state_accepts_update, should_auto_resume_resync, should_count_head_proxy_failure,
};

pub(crate) const RESYNC_META_FORMAT: u16 = rustfs_replication::resync::RESYNC_META_FORMAT;
pub(crate) const RESYNC_META_VERSION: u16 = rustfs_replication::resync::RESYNC_META_VERSION;
pub(crate) const WIRE_ZERO_TIME_UNIX: i64 = rustfs_replication::resync::WIRE_ZERO_TIME_UNIX;
pub(crate) const MRF_META_FORMAT: u16 = rustfs_replication::mrf::MRF_META_FORMAT;
pub(crate) const MRF_META_VERSION: u16 = rustfs_replication::mrf::MRF_META_VERSION;

fn map_replication_error(err: rustfs_replication::Error) -> Error {
    match err {
        rustfs_replication::Error::CorruptedFormat => Error::CorruptedFormat,
        rustfs_replication::Error::Other(err) => Error::other(err),
    }
}

pub(crate) fn encode_resync_file(status: &BucketReplicationResyncStatus) -> Result<Vec<u8>> {
    rustfs_replication::encode_resync_file(status).map_err(map_replication_error)
}

pub(crate) fn decode_resync_file(data: &[u8]) -> Result<BucketReplicationResyncStatus> {
    rustfs_replication::decode_resync_file(data).map_err(map_replication_error)
}

pub(crate) fn encode_mrf_file(entries: &[MrfReplicateEntry]) -> Result<Vec<u8>> {
    rustfs_replication::encode_mrf_file(entries).map_err(map_replication_error)
}

pub(crate) fn decode_mrf_file(data: &[u8]) -> Result<Vec<MrfReplicateEntry>> {
    rustfs_replication::decode_mrf_file(data).map_err(map_replication_error)
}
