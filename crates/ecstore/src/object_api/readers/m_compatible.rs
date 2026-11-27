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

//! Stored-layout answers of an `m-compatible` build: besides RustFS's own
//! framing it reads objects sealed under an object key, whose payload is a
//! DARE 2.0 stream (one for the object, or one per part).

use super::{EncryptedRangePlan, StreamConsumer, multipart_part_numbers, part_plaintext_size};
use crate::error::{Error, Result};
use crate::object_api::{ObjectInfo, ReadEncryptionMode};
use rustfs_filemeta::ObjectPartInfo;
use tokio::io::AsyncRead;

/// Plaintext bytes one DARE 2.0 package carries at most.
pub(super) const DARE_PAYLOAD_SIZE: i64 = 64 * 1024;
/// Header and authentication tag every DARE 2.0 package adds to its payload.
const DARE_PACKAGE_OVERHEAD: i64 = 32;
/// Stored size of a full DARE 2.0 package.
pub(super) const DARE_PACKAGE_SIZE: i64 = DARE_PAYLOAD_SIZE + DARE_PACKAGE_OVERHEAD;

/// Where the read of a compressed object sealed under an object key starts
/// for a plaintext `offset`: the stored offset of the package holding the
/// nearest indexed block, the plaintext still to skip after decompression,
/// the part it falls in, the decrypted bytes to drop, and the package's
/// sequence number.
pub(super) fn get_encrypted_compressed_offsets(oi: &ObjectInfo, offset: i64) -> (i64, i64, usize, i64, u64) {
    let mut skip_length = 0_i64;
    let mut cumulative_actual_size = 0_i64;
    let mut first_part_idx = 0_usize;
    let mut compressed_offset = 0_i64;

    for (i, part) in oi.parts.iter().enumerate() {
        cumulative_actual_size += part_plaintext_size(part);
        if cumulative_actual_size <= offset {
            compressed_offset += part.size as i64;
        } else {
            first_part_idx = i;
            skip_length = cumulative_actual_size - part_plaintext_size(part);
            break;
        }
    }

    let mut part_skip = offset - skip_length;
    let (mut decrypt_skip, mut seq_num) = (0_i64, 0_u64);

    if part_skip > 0
        && let Some(part) = oi.parts.get(first_part_idx)
        && let Some(index) = part
            .index
            .as_ref()
            .and_then(crate::io_support::rio::decode_compression_index_bytes)
        && let Ok((comp_off, uncomp_off)) = index.find(part_skip)
        && comp_off > 0
    {
        seq_num = (comp_off / DARE_PAYLOAD_SIZE) as u64;
        decrypt_skip = comp_off % DARE_PAYLOAD_SIZE;
        compressed_offset += (comp_off / DARE_PAYLOAD_SIZE) * DARE_PACKAGE_SIZE;

        part_skip -= uncomp_off;
    }

    (compressed_offset, part_skip, first_part_idx, decrypt_skip, seq_num)
}

/// The range plan of an object sealed under an object key, whose stored bytes
/// are DARE packages. `None` for any other object: the caller plans it as
/// RustFS's own framing.
///
/// # Errors
///
/// When the requested offset lies outside the object, or an offset derived
/// from it does not fit the plan's integer types.
pub(super) fn object_key_range_plan(
    oi: &ObjectInfo,
    mode: &ReadEncryptionMode,
    is_compressed: bool,
    requested_offset: usize,
    requested_length: i64,
    full_plaintext_size: usize,
) -> Result<Option<EncryptedRangePlan>> {
    if matches!(mode, ReadEncryptionMode::Direct { .. }) {
        return Ok(None);
    }

    if is_compressed {
        let (physical_off, decompressed_skip, first_part_idx, decrypt_skip, seq_num) =
            get_encrypted_compressed_offsets(oi, requested_offset as i64);
        return Ok(Some((
            usize::try_from(physical_off)
                .map_err(|_| Error::other(format!("invalid encrypted compressed offset {physical_off}")))?,
            oi.size - physical_off,
            usize::try_from(decrypt_skip).map_err(|_| Error::other(format!("invalid decrypt skip {decrypt_skip}")))?,
            usize::try_from(decompressed_skip)
                .map_err(|_| Error::other(format!("invalid decompressed skip {decompressed_skip}")))?,
            requested_length,
            full_plaintext_size,
            u32::try_from(seq_num).map_err(|_| Error::other(format!("invalid DARE sequence number {seq_num}")))?,
            multipart_part_numbers(&oi.parts[first_part_idx..]),
        )));
    }

    let (encrypted_offset, plaintext_skip, part_start_index, sequence_number, remaining_plaintext_size) =
        get_encrypted_offsets(oi, requested_offset as i64)?;
    let total_plaintext_size = usize::try_from(remaining_plaintext_size)
        .map_err(|_| Error::other(format!("invalid remaining decrypted size {remaining_plaintext_size}")))?;
    Ok(Some((
        usize::try_from(encrypted_offset).map_err(|_| Error::other(format!("invalid encrypted offset {encrypted_offset}")))?,
        oi.size - encrypted_offset,
        0,
        plaintext_skip,
        requested_length,
        total_plaintext_size,
        sequence_number,
        multipart_part_numbers(&oi.parts[part_start_index..]),
    )))
}

fn get_encrypted_offsets(oi: &ObjectInfo, offset: i64) -> Result<(i64, usize, usize, u32, i64)> {
    if oi.parts.is_empty() {
        let plaintext_size = oi.decrypted_size()?;
        let start_package_number = offset / DARE_PAYLOAD_SIZE;
        let plaintext_skip = usize::try_from(offset % DARE_PAYLOAD_SIZE)
            .map_err(|_| Error::other(format!("invalid DARE skip offset {offset}")))?;
        let encrypted_offset = start_package_number * DARE_PACKAGE_SIZE;
        let aligned_plaintext_offset = start_package_number * DARE_PAYLOAD_SIZE;
        let remaining_plaintext_size = plaintext_size - aligned_plaintext_offset;
        let sequence_number = u32::try_from(start_package_number)
            .map_err(|_| Error::other(format!("invalid DARE sequence number {start_package_number}")))?;
        return Ok((encrypted_offset, plaintext_skip, 0, sequence_number, remaining_plaintext_size));
    }

    let mut cumulative_plaintext_size = 0_i64;
    let mut cumulative_encrypted_size = 0_i64;

    for (part_index, part) in oi.parts.iter().enumerate() {
        let current_part_plaintext_size = part_plaintext_size(part);
        if offset < cumulative_plaintext_size + current_part_plaintext_size {
            let relative_offset = offset - cumulative_plaintext_size;
            let start_package_number = relative_offset / DARE_PAYLOAD_SIZE;
            let plaintext_skip = usize::try_from(relative_offset % DARE_PAYLOAD_SIZE)
                .map_err(|_| Error::other(format!("invalid DARE skip offset {relative_offset}")))?;
            let encrypted_offset = cumulative_encrypted_size + start_package_number * DARE_PACKAGE_SIZE;
            let aligned_plaintext_offset = cumulative_plaintext_size + start_package_number * DARE_PAYLOAD_SIZE;
            let remaining_plaintext_size = oi.decrypted_size()? - aligned_plaintext_offset;
            let sequence_number = u32::try_from(start_package_number)
                .map_err(|_| Error::other(format!("invalid DARE sequence number {start_package_number}")))?;

            return Ok((encrypted_offset, plaintext_skip, part_index, sequence_number, remaining_plaintext_size));
        }

        cumulative_plaintext_size += current_part_plaintext_size;
        cumulative_encrypted_size += part.size as i64;
    }

    Err(Error::other(format!(
        "invalid encrypted offset {offset} for object with decrypted size {}",
        oi.decrypted_size()?
    )))
}

/// Whether the object's own metadata records it as one cipher stream per part.
///
/// An object sealed under an object key records a multipart upload in a
/// marker of its own. The ETag cannot stand in for it there: a single-part
/// object's ETag is sealed, so it is never the 32 hex characters the caller's
/// heuristic takes for a single-part upload, and reading such an object under
/// part keys fails authentication.
pub(super) fn recorded_multipart_layout(oi: &ObjectInfo, mode: &ReadEncryptionMode) -> Option<bool> {
    matches!(mode, ReadEncryptionMode::Object).then(|| {
        oi.user_defined
            .contains_key(rustfs_utils::http::MINIO_INTERNAL_ENCRYPTION_MULTIPART_HEADER)
    })
}

/// Plaintext size of a DARE 2.0 stream of `stored` bytes.
///
/// Every package but the last is full; the last holds whatever remains, and a
/// remainder too short to hold any payload behind its framing is not a stream.
fn dare_plaintext_size(stored: i64) -> std::io::Result<i64> {
    let tail = stored % DARE_PACKAGE_SIZE;
    if stored < 0 || (tail != 0 && tail <= DARE_PACKAGE_OVERHEAD) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("stored size {stored} is not a whole DARE stream"),
        ));
    }
    Ok(stored / DARE_PACKAGE_SIZE * DARE_PAYLOAD_SIZE + (tail - DARE_PACKAGE_OVERHEAD).max(0))
}

/// The plaintext size of an encrypted object that records none.
///
/// An object sealed under an object key records no plaintext size when it is
/// a single-part upload; its size is implied by the stored size, the payload
/// being a DARE stream for the whole object, or one per part of a multipart
/// upload. Without this the stored size, which includes the framing, is
/// reported as the object's length.
///
/// The stored sizes are read from the parts, of which a single-part object
/// has exactly one. A read rewrites `size` to the length it delivers while
/// leaving the parts alone, and callers ask for the object's size on that
/// rewritten value too.
///
/// `None` when the object is not stored in that layout.
///
/// # Errors
///
/// When a stored size cannot be a whole DARE stream, which means the
/// metadata or the stored bytes are damaged.
pub(in crate::object_api) fn unrecorded_plaintext_size(oi: &ObjectInfo) -> std::io::Result<Option<i64>> {
    let seal_algorithm = oi
        .user_defined
        .get(rustfs_utils::http::MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER);
    if seal_algorithm.map(String::as_str) != Some(rustfs_utils::http::MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM) {
        return Ok(None);
    }

    if oi.parts.is_empty() {
        return dare_plaintext_size(oi.size).map(Some);
    }
    oi.parts
        .iter()
        .try_fold(0_i64, |total, part| {
            let stored = i64::try_from(part.size).map_err(|_| std::io::Error::other("encrypted part size overflow"))?;
            total
                .checked_add(dare_plaintext_size(stored)?)
                .ok_or_else(|| std::io::Error::other("encrypted object size overflow"))
        })
        .map(Some)
}

/// The parts a multipart decrypt reader is told to expect: a DARE writer
/// emits no package for an empty part, so none follows on disk, and the
/// reader fails a stream that ends before a part it was told about.
pub(super) fn readable_part_numbers(mut part_numbers: Vec<usize>, parts: &[ObjectPartInfo]) -> Vec<usize> {
    let empty_parts: Vec<usize> = parts.iter().filter(|part| part.size == 0).map(|part| part.number).collect();
    if !empty_parts.is_empty() {
        part_numbers.retain(|number| !empty_parts.contains(number));
    }
    part_numbers
}

/// The decompressed stream of a whole-object read: read to its end even when
/// the caller stops early, which the erasure writer feeding it requires.
pub(super) fn decompressed_stream<R>(reader: R) -> StreamConsumer<R>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    StreamConsumer::new(reader)
}
