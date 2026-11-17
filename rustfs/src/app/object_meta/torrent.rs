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

//! GetObjectTorrent: RustFS serves no torrents.
//!
//! Responsible for: the one answer, `NoSuchKey` for every request, so a client treats the torrent
//! as absent rather than the operation as unsupported.
//! NOT responsible for: authorization (there is none: the answer is the same for every caller).
//! Upstream: both edges. Downstream: none.

#[cfg(any(feature = "gateway-stack", test))]
use rustfs_gateway_types::dto::GetObjectTorrentInput;
use rustfs_gateway_types::dto::GetObjectTorrentOutput;
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result};

/// Answers `NoSuchKey` to every request.
///
/// # Errors
///
/// Always `NoSuchKey`.
#[cfg(any(feature = "gateway-stack", test))]
pub(crate) fn execute_get_object_torrent(_input: GetObjectTorrentInput) -> S3Result<GetObjectTorrentOutput> {
    answer_get_object_torrent()
}

pub(crate) fn answer_get_object_torrent() -> S3Result<GetObjectTorrentOutput> {
    Err(S3Error::new(S3ErrorCode::NoSuchKey))
}

#[cfg(test)]
mod tests {
    use super::{GetObjectTorrentInput, S3ErrorCode, execute_get_object_torrent};

    #[test]
    fn every_request_is_answered_no_such_key() {
        let error = execute_get_object_torrent(GetObjectTorrentInput::default()).expect_err("no torrent is served");

        assert_eq!(error.code(), &S3ErrorCode::NoSuchKey);
    }
}
