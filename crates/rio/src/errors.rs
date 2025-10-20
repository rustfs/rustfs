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

use thiserror::Error;

/// SHA256 mismatch error - when content SHA256 does not match what was sent from client
#[derive(Error, Debug, Clone, PartialEq)]
#[error("Bad sha256: Expected {expected_sha256} does not match calculated {calculated_sha256}")]
pub struct Sha256Mismatch {
    pub expected_sha256: String,
    pub calculated_sha256: String,
}

/// Bad digest error - Content-MD5 you specified did not match what we received
#[derive(Error, Debug, Clone, PartialEq)]
#[error("Bad digest: Expected {expected_md5} does not match calculated {calculated_md5}")]
pub struct BadDigest {
    pub expected_md5: String,
    pub calculated_md5: String,
}

/// Size too small error - reader size too small
#[derive(Error, Debug, Clone, PartialEq)]
#[error("Size small: got {got}, want {want}")]
pub struct SizeTooSmall {
    pub want: i64,
    pub got: i64,
}

/// Size too large error - reader size too large
#[derive(Error, Debug, Clone, PartialEq)]
#[error("Size large: got {got}, want {want}")]
pub struct SizeTooLarge {
    pub want: i64,
    pub got: i64,
}

/// Size mismatch error
#[derive(Error, Debug, Clone, PartialEq)]
#[error("Size mismatch: got {got}, want {want}")]
pub struct SizeMismatch {
    pub want: i64,
    pub got: i64,
}

/// Checksum mismatch error - when content checksum does not match what was sent from client
#[derive(Error, Debug, Clone, PartialEq)]
#[error("Bad checksum: Want {want} does not match calculated {got}")]
pub struct ChecksumMismatch {
    pub want: String,
    pub got: String,
}

/// Invalid checksum error
#[derive(Error, Debug, Clone, PartialEq)]
#[error("invalid checksum")]
pub struct InvalidChecksum;

/// Check if an error is a checksum mismatch
pub fn is_checksum_mismatch(err: &(dyn std::error::Error + 'static)) -> bool {
    err.downcast_ref::<ChecksumMismatch>().is_some()
}
