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

//! Stored-layout answers of a default build: every encrypted object it reads
//! is in RustFS's own framing, so nothing here is derived from another layout.

use super::EncryptedRangePlan;
use crate::error::Result;
use crate::object_api::{ObjectInfo, ReadEncryptionMode};
use rustfs_filemeta::ObjectPartInfo;
use tokio::io::AsyncRead;

/// The range plan of an object whose layout differs from RustFS's own framing.
/// A default build reads no such layout, so the caller plans every range itself.
///
/// # Errors
///
/// Never in this build.
pub(super) fn object_key_range_plan(
    _oi: &ObjectInfo,
    _mode: &ReadEncryptionMode,
    _is_compressed: bool,
    _requested_offset: usize,
    _requested_length: i64,
    _full_plaintext_size: usize,
) -> Result<Option<EncryptedRangePlan>> {
    Ok(None)
}

/// Whether the object's own metadata records it as one cipher stream per part.
/// Nothing a default build reads records that, so the caller falls back to
/// what the part list and the ETag imply.
pub(super) fn recorded_multipart_layout(_oi: &ObjectInfo, _mode: &ReadEncryptionMode) -> Option<bool> {
    None
}

/// The plaintext size of an encrypted object that records none.
///
/// # Errors
///
/// Never in this build, which derives no size from the stored layout.
pub(in crate::object_api) fn unrecorded_plaintext_size(_oi: &ObjectInfo) -> std::io::Result<Option<i64>> {
    Ok(None)
}

/// The parts a multipart decrypt reader is told to expect.
pub(super) fn readable_part_numbers(part_numbers: Vec<usize>, _parts: &[ObjectPartInfo]) -> Vec<usize> {
    part_numbers
}

/// The decompressed stream of a whole-object read.
pub(super) fn decompressed_stream<R>(reader: R) -> R
where
    R: AsyncRead + Unpin + Send + 'static,
{
    reader
}
