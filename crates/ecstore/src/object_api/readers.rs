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

use super::*;

use crate::io_support::rio::Index;
use std::mem::MaybeUninit;

#[cfg(feature = "rio-v2")]
const DARE_PAYLOAD_SIZE: i64 = 64 * 1024;
#[cfg(feature = "rio-v2")]
const DARE_PACKAGE_SIZE: i64 = DARE_PAYLOAD_SIZE + 32;

fn part_plaintext_size(part: &ObjectPartInfo) -> i64 {
    if part.actual_size > 0 {
        part.actual_size
    } else {
        part.size as i64
    }
}

fn http_range_spec_from_object_info(oi: &ObjectInfo, part_number: usize) -> Option<HTTPRangeSpec> {
    HTTPRangeSpec::from_part_sizes(oi.size, part_number, oi.parts.iter().map(part_plaintext_size))
}

/// A restore read forces `ReadPlan::build` down the `Plain` branch, so it
/// yields the STORED representation even for compressed or encrypted objects.
pub(crate) fn restore_request_active(opts: &ObjectOptions) -> bool {
    let restore = &opts.transition.restore_request;
    restore.type_.is_some() || restore.days.is_some() || restore.output_location.is_some() || restore.select_parameters.is_some()
}

fn decode_compression_index(index: Option<&Bytes>) -> Option<Index> {
    crate::io_support::rio::decode_compression_index_bytes(index?)
}

fn get_compressed_offsets(oi: &ObjectInfo, offset: i64) -> (i64, i64, usize, i64, u64) {
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
    #[cfg(feature = "rio-v2")]
    let (mut decrypt_skip, mut seq_num) = (0_i64, 0_u64);
    #[cfg(not(feature = "rio-v2"))]
    let (decrypt_skip, seq_num) = (0_i64, 0_u64);

    if part_skip > 0
        && let Some(part) = oi.parts.get(first_part_idx)
        && let Some(index) = decode_compression_index(part.index.as_ref())
        && let Ok((comp_off, uncomp_off)) = index.find(part_skip)
        && comp_off > 0
    {
        #[cfg(feature = "rio-v2")]
        if oi.is_encrypted() {
            seq_num = (comp_off / DARE_PAYLOAD_SIZE) as u64;
            decrypt_skip = comp_off % DARE_PAYLOAD_SIZE;
            compressed_offset += (comp_off / DARE_PAYLOAD_SIZE) * DARE_PACKAGE_SIZE;
        } else {
            compressed_offset += comp_off;
        }

        #[cfg(not(feature = "rio-v2"))]
        {
            compressed_offset += comp_off;
        }

        part_skip -= uncomp_off;
    }

    (compressed_offset, part_skip, first_part_idx, decrypt_skip, seq_num)
}

#[cfg(feature = "rio-v2")]
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

/// Metric path label for a Legacy encrypted Range GET that kept the conservative full-object read.
const ENCRYPTED_RANGE_READ_PATH_FULL: &str = "full";
/// Metric path label for a Legacy encrypted Range GET served by the part-boundary seek.
const ENCRYPTED_RANGE_READ_PATH_PART_SEEK: &str = "part_seek";

/// True when a Legacy (rio v1) encrypted Range GET may seek to the covering part
/// boundary instead of streaming the whole ciphertext.
///
/// Every condition is required for correctness, not merely for benefit:
/// - `is_multipart`: single-part rio v1 streams have variable-length encrypted blocks
///   with no closed-form plaintext-to-physical mapping, so only part boundaries
///   (recorded in metadata) are safe seek targets.
/// - `!is_compressed`: under compression the requested offset addresses the
///   decompressed stream, not per-part plaintext, so stay on the full read.
/// - non-empty parts with `actual_size > 0` for every part: a part with
///   `actual_size <= 0` would make `part_plaintext_size` fall back to the physical
///   size and poison the plaintext cumulative sums.
/// - physical part sizes must add up to `oi.size`: guards against inconsistent
///   metadata scheduling reads past the object end in the erasure layer.
/// - plaintext part sizes must add up to the recorded decrypted object size.
/// - `requested_length > 0`: degenerate empty ranges keep their existing full-read
///   behavior (and its error surface) unchanged.
fn legacy_encrypted_seek_eligible(
    oi: &ObjectInfo,
    is_multipart: bool,
    is_compressed: bool,
    requested_length: i64,
    recorded_plaintext_size: Option<i64>,
) -> bool {
    if !legacy_encrypted_range_seek_enabled() || !is_multipart || is_compressed || requested_length <= 0 || oi.parts.is_empty() {
        return false;
    }
    let Some(recorded_plaintext_size) = recorded_plaintext_size else {
        return false;
    };
    let Some(data_dir) = oi.data_dir.filter(|data_dir| !data_dir.is_nil()) else {
        return false;
    };
    let mut layout_token_buf = [0_u8; 36];
    let layout_token = data_dir.hyphenated().encode_lower(&mut layout_token_buf);
    if !has_encrypted_part_layout_marker(&oi.user_defined, ENCRYPTED_PART_LAYOUT_QUORUM_SUFFIX, layout_token) {
        return false;
    }

    let Some((physical_size, plaintext_size)) = legacy_part_layout_totals(&oi.parts) else {
        return false;
    };
    physical_size == oi.size && recorded_plaintext_size == plaintext_size
}

fn legacy_part_layout_totals(parts: &[ObjectPartInfo]) -> Option<(i64, i64)> {
    if parts.is_empty() {
        return None;
    }
    // Complete votes on these four boundary fields in
    // `resolve_read_part_from_responses`; final object reads additionally require
    // the full `FileInfo` identity (including parts) to reach metadata quorum.
    parts
        .iter()
        .try_fold((0_i64, 0_i64), |(physical_size, plaintext_size), part| {
            let part_physical_size = i64::try_from(part.size).ok()?;
            let part_plaintext_size = (part.actual_size > 0).then_some(part.actual_size)?;
            Some((
                physical_size.checked_add(part_physical_size)?,
                plaintext_size.checked_add(part_plaintext_size)?,
            ))
        })
}

/// Part-boundary seek offsets for a Legacy (rio v1) encrypted multipart object.
///
/// Uses only the two per-part metadata facts — `size` (physical encrypted bytes on
/// disk) and `actual_size` (plaintext bytes) — and makes zero assumptions about the
/// encrypted block layout inside a part. Returns `(physical_offset, physical_length,
/// plaintext_skip_in_part, part_start_index, remaining_plaintext)` where
/// `physical_offset` is the ciphertext offset of the first part covering `offset`,
/// `physical_length` extends to the physical end of the last part covering
/// `offset + length - 1`, `plaintext_skip_in_part` is the decrypted plaintext to
/// discard before the range starts, and `remaining_plaintext` is the total plaintext
/// from the covering part through the end of the object (the same contract
/// `get_encrypted_offsets` hands to `into_reader`).
///
/// Returns `None` when the range does not fall inside the parts table; the caller
/// must then keep the conservative full-object read. With the
/// `legacy_encrypted_seek_eligible` gate satisfied this cannot happen, because the
/// range spec already clamps `offset + length` to the summed part plaintext.
fn get_legacy_encrypted_offsets(oi: &ObjectInfo, offset: i64, length: i64) -> Option<(i64, i64, usize, usize, i64)> {
    if offset < 0 || length <= 0 {
        return None;
    }
    let end_plaintext = offset.checked_add(length)?.checked_sub(1)?;
    let mut cumulative_plaintext_size = 0_i64;
    let mut cumulative_encrypted_size = 0_i64;

    for (part_index, part) in oi.parts.iter().enumerate() {
        let current_part_plaintext_size = part.actual_size;
        let part_plaintext_end = cumulative_plaintext_size.checked_add(current_part_plaintext_size)?;
        if offset < part_plaintext_end {
            let mut covered_plaintext_end = cumulative_plaintext_size;
            let mut physical_end = cumulative_encrypted_size;
            let mut remaining_plaintext = 0_i64;
            let mut covered = false;
            for tail_part in &oi.parts[part_index..] {
                remaining_plaintext = remaining_plaintext.checked_add(tail_part.actual_size)?;
                if !covered {
                    physical_end = physical_end.checked_add(i64::try_from(tail_part.size).ok()?)?;
                    covered_plaintext_end = covered_plaintext_end.checked_add(tail_part.actual_size)?;
                    if end_plaintext < covered_plaintext_end {
                        covered = true;
                    }
                }
            }
            if !covered {
                return None;
            }
            let plaintext_skip_in_part = usize::try_from(offset - cumulative_plaintext_size).ok()?;
            return Some((
                cumulative_encrypted_size,
                physical_end.checked_sub(cumulative_encrypted_size)?,
                plaintext_skip_in_part,
                part_index,
                remaining_plaintext,
            ));
        }

        cumulative_plaintext_size = part_plaintext_end;
        cumulative_encrypted_size = cumulative_encrypted_size.checked_add(i64::try_from(part.size).ok()?)?;
    }

    None
}

/// Encrypted-range plan tuple shared by both Legacy (rio v1) build arms:
/// `(storage_offset, storage_length, decrypt_skip, plaintext_offset,
/// plaintext_length, total_plaintext_size, sequence_number, part_numbers)`.
type EncryptedRangePlan = (usize, i64, usize, usize, i64, usize, u32, Vec<usize>);

/// Range plan for a Legacy (rio v1) encrypted object
/// (https://github.com/rustfs/backlog/issues/1316 Phase A).
///
/// Eligible multipart objects seek to the covering part boundary: per-part physical
/// sizes are exact metadata facts, and the rio v1 multipart decrypt reader already
/// decrypts a stream that starts at any part boundary once it is handed the part
/// numbers from that part onward. Everything else — single-part objects, compressed
/// payloads, defective parts tables, the kill switch — keeps the conservative
/// full-object read: rio v1 encrypted blocks are variable-length, so existing
/// objects have no safe sub-part seek.
fn legacy_encrypted_range_plan(
    oi: &ObjectInfo,
    is_multipart: bool,
    is_compressed: bool,
    requested_offset: usize,
    requested_length: i64,
    full_plaintext_size: usize,
    recorded_plaintext_size: Option<i64>,
) -> Result<EncryptedRangePlan> {
    if legacy_encrypted_seek_eligible(oi, is_multipart, is_compressed, requested_length, recorded_plaintext_size)
        && let Ok(requested_offset) = i64::try_from(requested_offset)
        && let Some((physical_offset, physical_length, plaintext_skip_in_part, part_start_index, remaining_plaintext)) =
            get_legacy_encrypted_offsets(oi, requested_offset, requested_length)
    {
        let storage_offset = usize::try_from(physical_offset)
            .map_err(|_| Error::other(format!("invalid legacy encrypted offset {physical_offset}")))?;
        let total_plaintext_size = usize::try_from(remaining_plaintext)
            .map_err(|_| Error::other(format!("invalid legacy remaining decrypted size {remaining_plaintext}")))?;
        record_encrypted_range_read_amplification(ENCRYPTED_RANGE_READ_PATH_PART_SEEK, physical_length, requested_length);
        return Ok((
            storage_offset,
            physical_length,
            0,
            plaintext_skip_in_part,
            requested_length,
            total_plaintext_size,
            0,
            multipart_part_numbers(&oi.parts[part_start_index..]),
        ));
    }

    // Skip the metric for compressed payloads: their requested length addresses
    // the decompressed stream, so the physical/plaintext ratio would mix
    // coordinate systems and pollute the histogram.
    if !is_compressed {
        record_encrypted_range_read_amplification(ENCRYPTED_RANGE_READ_PATH_FULL, oi.size, requested_length);
    }
    Ok((
        0,
        oi.size,
        0,
        requested_offset,
        requested_length,
        full_plaintext_size,
        0,
        multipart_part_numbers(&oi.parts),
    ))
}

/// Record path and physical/plaintext read amplification for one Legacy encrypted
/// Range GET at the ReadPlan decision point.
fn record_encrypted_range_read_amplification(path: &'static str, physical_length: i64, requested_length: i64) {
    if requested_length <= 0 || physical_length < 0 {
        return;
    }
    rustfs_io_metrics::record_get_encrypted_range_read_amplification(path, physical_length as f64 / requested_length as f64);
}

pub struct PutObjReader {
    pub stream: HashReader,
}

impl Debug for PutObjReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PutObjReader").finish()
    }
}

impl PutObjReader {
    pub fn new(stream: HashReader) -> Self {
        PutObjReader { stream }
    }

    pub fn as_hash_reader(&self) -> &HashReader {
        &self.stream
    }

    pub fn from_vec(data: Vec<u8>) -> Self {
        use sha2::{Digest, Sha256};
        let content_length = data.len() as i64;
        let sha256hex = if content_length > 0 {
            Some(hex_simd::encode_to_string(Sha256::digest(&data), hex_simd::AsciiCase::Lower))
        } else {
            None
        };
        PutObjReader {
            stream: HashReader::from_stream(Cursor::new(data), content_length, content_length, None, sha256hex, false).unwrap(),
        }
    }

    pub fn from_prehashed_bytes(data: Bytes, sha256hex: Option<String>) -> std::io::Result<Self> {
        let content_length =
            i64::try_from(data.len()).map_err(|_| std::io::Error::other("prehashed object payload exceeds i64 length"))?;
        Ok(PutObjReader {
            stream: HashReader::from_stream(Cursor::new(data), content_length, content_length, None, sha256hex, false)?,
        })
    }

    pub fn size(&self) -> i64 {
        self.stream.size()
    }

    pub fn actual_size(&self) -> i64 {
        self.stream.actual_size()
    }
}

/// Provenance of a [`GetObjectReader`] with respect to the app-layer object
/// data cache hook, so the app layer can avoid repeating the cache lookup the
/// ecstore GET probe already ran after fresh metadata resolution (backlog#1121
/// / ODC-16). One hook-served GET must record exactly one lookup, not two.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GetObjectBodySource {
    /// The cache hook did not probe this read: the hook is unregistered, or the
    /// read is ineligible under the fail-closed allow-list. The app layer runs
    /// its own cache lookup, as before.
    #[default]
    Unprobed,
    /// The hook probed after fresh metadata resolution and did not serve a body
    /// (a genuine miss, or a length-defensive rejection). Its miss is
    /// authoritative, so the app layer must skip the lookup and only build a
    /// plan to fill.
    HookMissed,
    /// `buffered_body` is exactly the body the hook served from the cache. The
    /// app layer serves it directly as the object-data-cache source, with no
    /// second lookup and no re-fill.
    HookServed,
}

pub struct GetObjectReader {
    pub stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
    pub object_info: ObjectInfo,
    pub buffered_body: Option<Bytes>,
    /// Cache-hook provenance; defaults to [`GetObjectBodySource::Unprobed`] for
    /// every reader that never passed through the app-layer cache probe.
    pub body_source: GetObjectBodySource,
}

impl GetObjectReader {
    /// Builds a fully materialized reader from a cache-coordinated body.
    pub fn from_cache_body(mut object_info: ObjectInfo, body: Bytes) -> Result<Self> {
        object_info.size = i64::try_from(body.len()).map_err(|_| Error::other("cached GET body length exceeds i64::MAX"))?;
        Ok(Self {
            stream: Box::new(std::io::Cursor::new(body.clone())),
            object_info,
            buffered_body: Some(body),
            body_source: GetObjectBodySource::HookServed,
        })
    }

    /// True when `buffered_body` is the body the cache hook served. The app
    /// layer serves it as the object-data-cache source without a second lookup.
    pub fn is_cache_hook_served(&self) -> bool {
        matches!(self.body_source, GetObjectBodySource::HookServed)
    }

    /// True when the cache hook probed this read (whether it served a body or
    /// missed). The app layer must not repeat the lookup in either case.
    pub fn cache_hook_probed(&self) -> bool {
        !matches!(self.body_source, GetObjectBodySource::Unprobed)
    }
}

enum ReadTransform {
    // Written but never read by production code: the enclosing struct already
    // carries the same pair as `storage_offset`/`storage_length`. They survive
    // as the read plan's test-visible record — four tests assert them by
    // literal pattern (`Plain { visible_offset: 6, visible_length: 4 }`), which
    // rustc does not count as a read.
    #[allow(
        dead_code,
        reason = "asserted by literal pattern in this file's read-plan tests (backlog#1823)"
    )]
    Plain { visible_offset: usize, visible_length: i64 },
    Compressed {
        algorithm: CompressionAlgorithm,
        backend: crate::io_support::rio::ReadCompressionBackend,
        decompressed_offset: usize,
        decompressed_length: i64,
        total_plaintext_size: usize,
    },
    Encrypted {
        material: ReadEncryptionMaterial,
        is_multipart: bool,
        part_numbers: Vec<usize>,
        sequence_number: u32,
        decrypt_skip: usize,
        plaintext_offset: usize,
        plaintext_length: i64,
        total_plaintext_size: usize,
        compression: Option<(CompressionAlgorithm, crate::io_support::rio::ReadCompressionBackend)>,
    },
}

/// How an object's stored bytes must be fetched and transformed to serve a
/// request.
///
/// Public so callers that fetch the stored bytes from somewhere other than the
/// local erasure set — the remote-tier read path — can position their own fetch
/// with [`ReadPlan::storage_offset`] / [`ReadPlan::storage_length`] and then
/// hand the resulting stream to [`ReadPlan::into_object_reader`], instead of
/// reimplementing the transform decisions (rustfs/rustfs#6025).
pub struct ReadPlan {
    storage_offset: usize,
    storage_length: i64,
    object_size: i64,
    transform: ReadTransform,
}

impl ReadPlan {
    /// Byte offset into the object's **stored** bytes where the fetch must
    /// start. Encrypted and compressed objects address their storage in a
    /// different coordinate system than the plaintext range the caller asked
    /// for, which is exactly the distinction this plan resolves.
    pub fn storage_offset(&self) -> usize {
        self.storage_offset
    }

    /// Number of **stored** bytes the fetch must deliver, in the same
    /// coordinate system as [`Self::storage_offset`].
    pub fn storage_length(&self) -> i64 {
        self.storage_length
    }

    /// Build the plan for a request without consuming a stream, so a caller
    /// that has to issue its own positioned fetch can read the offsets first.
    pub async fn build_for_request(
        rs: Option<HTTPRangeSpec>,
        oi: &ObjectInfo,
        opts: &ObjectOptions,
        h: &HeaderMap<HeaderValue>,
        resolver: Option<&dyn ObjectEncryptionResolver>,
    ) -> Result<Self> {
        Self::build_with_resolver(rs, oi, opts, h, resolver).await
    }

    /// Wrap `reader` — the stored bytes this plan asked for, already positioned
    /// at [`Self::storage_offset`] — in the transforms that turn them into the
    /// bytes the caller requested.
    pub fn into_object_reader(
        self,
        reader: Box<dyn AsyncRead + Unpin + Send + Sync>,
        oi: &ObjectInfo,
    ) -> Result<GetObjectReader> {
        self.into_reader(reader, oi).map(|(reader, _, _)| reader)
    }

    #[cfg(test)]
    async fn build(rs: Option<HTTPRangeSpec>, oi: &ObjectInfo, opts: &ObjectOptions, h: &HeaderMap<HeaderValue>) -> Result<Self> {
        Self::build_with_resolver(rs, oi, opts, h, Some(&tests::TEST_RESOLVER)).await
    }

    async fn build_with_resolver(
        rs: Option<HTTPRangeSpec>,
        oi: &ObjectInfo,
        opts: &ObjectOptions,
        h: &HeaderMap<HeaderValue>,
        resolver: Option<&dyn ObjectEncryptionResolver>,
    ) -> Result<Self> {
        let mut rs = rs;
        // A part number addresses the object's PLAINTEXT bytes. A restore read
        // serves the stored representation instead (see
        // [`restore_request_active`]), where that synthesized range would be
        // reinterpreted as a storage range and truncate an encrypted or
        // compressed payload by exactly its encoding overhead — the copy-back
        // then fails its length check partway through
        // (rustfs/rustfs#6025). An explicit caller range is already in storage
        // coordinates on that path and is still honored.
        if let Some(part_number) = opts.part_number
            && rs.is_none()
            && !restore_request_active(opts)
        {
            rs = http_range_spec_from_object_info(oi, part_number);
        }

        if opts.raw_data_movement_read {
            let (visible_offset, visible_length) = if let Some(rs) = rs {
                rs.get_offset_length(oi.size)?
            } else {
                (0, oi.size)
            };

            return Ok(Self {
                storage_offset: visible_offset,
                storage_length: visible_length,
                object_size: oi.size,
                transform: ReadTransform::Plain {
                    visible_offset,
                    visible_length,
                },
            });
        }

        let mut is_encrypted = oi.is_encrypted();
        let (algo, compression_backend, mut is_compressed) = oi.compression_read_plan()?;

        if restore_request_active(opts) {
            is_encrypted = false;
            is_compressed = false;
        }

        if is_compressed && !is_encrypted {
            let actual_size = oi.get_actual_size()?;
            let (storage_offset, storage_length, decompressed_offset, decompressed_length) = if let Some(rs) = rs {
                let (req_off, req_length) = rs.get_offset_length(actual_size)?;
                let (physical_off, decompressed_skip, _, _, _) = get_compressed_offsets(oi, req_off as i64);
                let storage_offset = usize::try_from(physical_off)
                    .map_err(|_| Error::other(format!("invalid compressed offset {physical_off}")))?;
                (storage_offset, oi.size - physical_off, decompressed_skip as usize, req_length)
            } else {
                (0, oi.size, 0, actual_size)
            };

            let total_plaintext_size =
                usize::try_from(actual_size).map_err(|_| Error::other(format!("invalid decompressed size {actual_size}")))?;

            return Ok(Self {
                storage_offset,
                storage_length,
                object_size: decompressed_length,
                transform: ReadTransform::Compressed {
                    algorithm: algo,
                    backend: compression_backend,
                    decompressed_offset,
                    decompressed_length,
                    total_plaintext_size,
                },
            });
        }

        if is_encrypted {
            let resolver = resolver.ok_or_else(|| Error::other("object encryption resolver is unavailable"))?;
            let resolved = resolver
                .resolve_read_material(ReadEncryptionRequest {
                    bucket: &oi.bucket,
                    object: &oi.name,
                    metadata: &oi.user_defined,
                    headers: h,
                })
                .await
                .map_err(Error::other)?
                .ok_or_else(|| Error::other("encrypted object metadata is incomplete"))?;
            let material = resolved;
            #[cfg(feature = "rio-v2")]
            let uses_legacy_encryption = matches!(material.mode, ReadEncryptionMode::Direct { .. });
            let is_multipart = is_multipart_encrypted_object(&oi.parts, oi.etag.as_deref());
            let recorded_plaintext_size = oi.encryption_original_size()?;
            let plaintext_size = encrypted_plaintext_size(oi, is_multipart, is_compressed, recorded_plaintext_size)?;
            let full_plaintext_size =
                usize::try_from(plaintext_size).map_err(|_| Error::other(format!("invalid decrypted size {plaintext_size}")))?;
            let (
                storage_offset,
                storage_length,
                decrypt_skip,
                plaintext_offset,
                plaintext_length,
                total_plaintext_size,
                sequence_number,
                part_numbers,
            ) = if let Some(rs) = rs {
                let (requested_offset, requested_length) = rs.get_offset_length(plaintext_size)?;
                #[cfg(feature = "rio-v2")]
                {
                    if uses_legacy_encryption {
                        legacy_encrypted_range_plan(
                            oi,
                            is_multipart,
                            is_compressed,
                            requested_offset,
                            requested_length,
                            full_plaintext_size,
                            recorded_plaintext_size,
                        )?
                    } else if is_compressed {
                        let (physical_off, decompressed_skip, first_part_idx, decrypt_skip, seq_num) =
                            get_compressed_offsets(oi, requested_offset as i64);
                        (
                            usize::try_from(physical_off)
                                .map_err(|_| Error::other(format!("invalid encrypted compressed offset {physical_off}")))?,
                            oi.size - physical_off,
                            usize::try_from(decrypt_skip)
                                .map_err(|_| Error::other(format!("invalid decrypt skip {decrypt_skip}")))?,
                            usize::try_from(decompressed_skip)
                                .map_err(|_| Error::other(format!("invalid decompressed skip {decompressed_skip}")))?,
                            requested_length,
                            full_plaintext_size,
                            u32::try_from(seq_num)
                                .map_err(|_| Error::other(format!("invalid DARE sequence number {seq_num}")))?,
                            multipart_part_numbers(&oi.parts[first_part_idx..]),
                        )
                    } else {
                        let (encrypted_offset, plaintext_skip, part_start_index, sequence_number, remaining_plaintext_size) =
                            get_encrypted_offsets(oi, requested_offset as i64)?;
                        let total_plaintext_size = usize::try_from(remaining_plaintext_size)
                            .map_err(|_| Error::other(format!("invalid remaining decrypted size {remaining_plaintext_size}")))?;
                        (
                            usize::try_from(encrypted_offset)
                                .map_err(|_| Error::other(format!("invalid encrypted offset {encrypted_offset}")))?,
                            oi.size - encrypted_offset,
                            0,
                            plaintext_skip,
                            requested_length,
                            total_plaintext_size,
                            sequence_number,
                            multipart_part_numbers(&oi.parts[part_start_index..]),
                        )
                    }
                }
                #[cfg(not(feature = "rio-v2"))]
                {
                    legacy_encrypted_range_plan(
                        oi,
                        is_multipart,
                        is_compressed,
                        requested_offset,
                        requested_length,
                        full_plaintext_size,
                        recorded_plaintext_size,
                    )?
                }
            } else {
                (
                    0,
                    oi.size,
                    0,
                    0,
                    plaintext_size,
                    full_plaintext_size,
                    0,
                    multipart_part_numbers(&oi.parts),
                )
            };

            return Ok(Self {
                storage_offset,
                storage_length,
                object_size: plaintext_length,
                transform: ReadTransform::Encrypted {
                    material,
                    is_multipart,
                    part_numbers,
                    sequence_number,
                    decrypt_skip,
                    plaintext_offset,
                    plaintext_length,
                    total_plaintext_size,
                    compression: is_compressed.then_some((algo, compression_backend)),
                },
            });
        }

        let (visible_offset, visible_length) = if let Some(rs) = rs {
            rs.get_offset_length(oi.size)?
        } else {
            (0, oi.size)
        };

        Ok(Self {
            storage_offset: visible_offset,
            storage_length: visible_length,
            object_size: oi.size,
            transform: ReadTransform::Plain {
                visible_offset,
                visible_length,
            },
        })
    }

    fn into_reader(
        self,
        reader: Box<dyn AsyncRead + Unpin + Send + Sync>,
        oi: &ObjectInfo,
    ) -> Result<(GetObjectReader, usize, i64)> {
        match self.transform {
            ReadTransform::Plain { .. } => Ok((
                GetObjectReader {
                    stream: reader,
                    object_info: oi.clone(),
                    buffered_body: None,
                    body_source: GetObjectBodySource::Unprobed,
                },
                self.storage_offset,
                self.storage_length,
            )),
            ReadTransform::Compressed {
                algorithm,
                backend,
                decompressed_offset,
                decompressed_length,
                total_plaintext_size,
            } => {
                let dec_reader = crate::io_support::rio::decompression_reader(reader, algorithm, backend);
                #[cfg(feature = "rio-v2")]
                let dec_reader = StreamConsumer::new(dec_reader);
                let final_reader: Box<dyn AsyncRead + Unpin + Send + Sync> = if decompressed_offset > 0
                    || decompressed_length != total_plaintext_size as i64
                {
                    #[cfg(feature = "rio-v2")]
                    let ranged_result = RangedDecompressReader::new_draining(
                        dec_reader,
                        decompressed_offset,
                        decompressed_length,
                        total_plaintext_size,
                    );
                    #[cfg(not(feature = "rio-v2"))]
                    let ranged_result =
                        RangedDecompressReader::new(dec_reader, decompressed_offset, decompressed_length, total_plaintext_size);

                    match ranged_result {
                        Ok(ranged_reader) => {
                            tracing::debug!(
                                "Successfully created RangedDecompressReader for offset={}, length={}",
                                decompressed_offset,
                                decompressed_length
                            );
                            Box::new(ranged_reader)
                        }
                        Err(e) => {
                            tracing::error!("RangedDecompressReader failed with invalid range parameters: {}", e);
                            return Err(e);
                        }
                    }
                } else {
                    Box::new(HardLimitReader::new(dec_reader, decompressed_length))
                };

                let mut object_info = oi.clone();
                object_info.size = self.object_size;

                Ok((
                    GetObjectReader {
                        stream: final_reader,
                        object_info,
                        buffered_body: None,
                        body_source: GetObjectBodySource::Unprobed,
                    },
                    self.storage_offset,
                    self.storage_length,
                ))
            }
            ReadTransform::Encrypted {
                material,
                is_multipart,
                part_numbers,
                sequence_number,
                decrypt_skip,
                plaintext_offset,
                plaintext_length,
                total_plaintext_size,
                compression,
            } => {
                #[cfg(not(feature = "rio-v2"))]
                let _ = sequence_number;
                let decrypted_reader: Box<dyn AsyncRead + Unpin + Send + Sync> = if is_multipart {
                    match material.mode {
                        ReadEncryptionMode::Object => crate::io_support::rio::decrypt_multipart_reader_with_object_key(
                            reader,
                            material.key_bytes,
                            part_numbers,
                            sequence_number,
                        ),
                        ReadEncryptionMode::Direct { base_nonce } => crate::io_support::rio::decrypt_multipart_reader(
                            reader,
                            material.key_bytes,
                            base_nonce,
                            part_numbers,
                            crate::io_support::rio::ReadEncryptionBackend::Legacy,
                            sequence_number,
                        ),
                    }
                } else {
                    match material.mode {
                        ReadEncryptionMode::Object => {
                            crate::io_support::rio::decrypt_reader_with_object_key(reader, material.key_bytes, sequence_number)
                        }
                        ReadEncryptionMode::Direct { base_nonce } => crate::io_support::rio::decrypt_reader(
                            reader,
                            material.key_bytes,
                            base_nonce,
                            crate::io_support::rio::ReadEncryptionBackend::Legacy,
                            sequence_number,
                        ),
                    }
                };
                let decrypted_reader: Box<dyn AsyncRead + Unpin + Send + Sync> = if decrypt_skip > 0 {
                    Box::new(SkipReader::new(decrypted_reader, decrypt_skip))
                } else {
                    decrypted_reader
                };
                let total_plaintext_size_i64 = i64::try_from(total_plaintext_size)
                    .map_err(|_| Error::other(format!("invalid plaintext size {total_plaintext_size}")))?;

                let final_reader: Box<dyn AsyncRead + Unpin + Send + Sync> =
                    if let Some((algo, compression_backend)) = compression {
                        let decompressed_reader =
                            crate::io_support::rio::decompression_reader(decrypted_reader, algo, compression_backend);
                        #[cfg(feature = "rio-v2")]
                        let decompressed_reader = StreamConsumer::new(decompressed_reader);
                        if plaintext_offset > 0 || plaintext_length != total_plaintext_size_i64 {
                            #[cfg(feature = "rio-v2")]
                            let ranged_reader = RangedDecompressReader::new_draining(
                                decompressed_reader,
                                plaintext_offset,
                                plaintext_length,
                                total_plaintext_size,
                            )?;
                            #[cfg(not(feature = "rio-v2"))]
                            let ranged_reader = RangedDecompressReader::new(
                                decompressed_reader,
                                plaintext_offset,
                                plaintext_length,
                                total_plaintext_size,
                            )?;
                            Box::new(ranged_reader)
                        } else {
                            Box::new(HardLimitReader::new(decompressed_reader, total_plaintext_size_i64))
                        }
                    } else if plaintext_offset > 0 || plaintext_length != total_plaintext_size_i64 {
                        Box::new(RangedDecompressReader::new(
                            decrypted_reader,
                            plaintext_offset,
                            plaintext_length,
                            total_plaintext_size,
                        )?)
                    } else {
                        Box::new(HardLimitReader::new(decrypted_reader, total_plaintext_size_i64))
                    };

                let mut object_info = oi.clone();
                object_info.size = self.object_size;

                Ok((
                    GetObjectReader {
                        stream: final_reader,
                        object_info,
                        buffered_body: None,
                        body_source: GetObjectBodySource::Unprobed,
                    },
                    self.storage_offset,
                    self.storage_length,
                ))
            }
        }
    }
}

impl GetObjectReader {
    #[cfg(test)]
    pub(crate) async fn new(
        reader: Box<dyn AsyncRead + Unpin + Send + Sync>,
        rs: Option<HTTPRangeSpec>,
        oi: &ObjectInfo,
        opts: &ObjectOptions,
        h: &HeaderMap<HeaderValue>,
    ) -> Result<(Self, usize, i64)> {
        Self::new_with_resolver(reader, rs, oi, opts, h, Some(&tests::TEST_RESOLVER)).await
    }

    pub async fn new_with_resolver(
        reader: Box<dyn AsyncRead + Unpin + Send + Sync>,
        rs: Option<HTTPRangeSpec>,
        oi: &ObjectInfo,
        opts: &ObjectOptions,
        h: &HeaderMap<HeaderValue>,
        resolver: Option<&dyn ObjectEncryptionResolver>,
    ) -> Result<(Self, usize, i64)> {
        ReadPlan::build_with_resolver(rs, oi, opts, h, resolver)
            .await?
            .into_reader(reader, oi)
    }
    #[hotpath::measure(impl_type = "GetObjectReader")]
    pub async fn read_all(&mut self) -> Result<Vec<u8>> {
        let mut data = Vec::new();
        self.stream.read_to_end(&mut data).await?;

        // while let Some(x) = self.stream.next().await {
        //     let buf = match x {
        //         Ok(res) => res,
        //         Err(e) => return Err(Error::other(e.to_string())),
        //     };
        //     data.extend_from_slice(buf.as_ref());
        // }

        Ok(data)
    }
}

impl AsyncRead for GetObjectReader {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

#[derive(Debug)]
struct SkipReader<R> {
    inner: R,
    bytes_to_skip: usize,
    bytes_skipped: usize,
    scratch: Box<[MaybeUninit<u8>]>,
}

impl<R: AsyncRead + Unpin + Send + Sync> SkipReader<R> {
    fn new(inner: R, bytes_to_skip: usize) -> Self {
        Self {
            inner,
            bytes_to_skip,
            bytes_skipped: 0,
            scratch: Box::<[u8]>::new_uninit_slice(8192),
        }
    }
}

impl<R: AsyncRead + Unpin + Send + Sync> AsyncRead for SkipReader<R> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let this = self.as_mut().get_mut();

        while this.bytes_skipped < this.bytes_to_skip {
            let remaining = this.bytes_to_skip - this.bytes_skipped;
            let scratch_len = remaining.min(this.scratch.len());
            let mut scratch_buf = ReadBuf::uninit(&mut this.scratch[..scratch_len]);
            match Pin::new(&mut this.inner).poll_read(cx, &mut scratch_buf) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Ready(Ok(())) => {
                    let n = scratch_buf.filled().len();
                    if n == 0 {
                        return Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            format!("unexpected EOF while skipping {} bytes from decrypted stream", this.bytes_to_skip),
                        )));
                    }
                    this.bytes_skipped += n;
                }
            }
        }

        Pin::new(&mut this.inner).poll_read(cx, buf)
    }
}

/// A streaming decompression reader that supports range requests by skipping data in the decompressed stream.
/// This implementation acknowledges that compressed streams (like LZ4) must be decompressed sequentially
/// from the beginning, so it streams and discards data until reaching the target offset.
#[derive(Debug)]
pub struct RangedDecompressReader<R: AsyncRead + Unpin + Send + Sync + 'static> {
    inner: Option<R>,
    target_offset: usize,
    target_length: usize,
    current_offset: usize,
    bytes_returned: usize,
    scratch: Box<[MaybeUninit<u8>]>,
    drain_on_done: bool,
    drain_task: Option<tokio::task::JoinHandle<()>>,
}

impl<R: AsyncRead + Unpin + Send + Sync + 'static> RangedDecompressReader<R> {
    pub fn new(inner: R, offset: usize, length: i64, total_size: usize) -> Result<Self> {
        Self::new_with_drain(inner, offset, length, total_size, false)
    }

    pub fn new_draining(inner: R, offset: usize, length: i64, total_size: usize) -> Result<Self> {
        Self::new_with_drain(inner, offset, length, total_size, true)
    }

    fn new_with_drain(inner: R, offset: usize, length: i64, total_size: usize, drain_on_done: bool) -> Result<Self> {
        // Validate the range request
        if offset >= total_size {
            tracing::debug!("Range offset {} exceeds total size {}", offset, total_size);
            return Err(Error::InvalidRangeSpec("Range offset exceeds file size".to_string()));
        }

        // Adjust length if it extends beyond file end
        let actual_length = std::cmp::min(length as usize, total_size - offset);

        tracing::debug!(
            "Creating RangedDecompressReader: offset={}, length={}, total_size={}, actual_length={}",
            offset,
            length,
            total_size,
            actual_length
        );

        Ok(Self {
            inner: Some(inner),
            target_offset: offset,
            target_length: actual_length,
            current_offset: 0,
            bytes_returned: 0,
            scratch: Box::<[u8]>::new_uninit_slice(8192),
            drain_on_done,
            drain_task: None,
        })
    }

    fn start_drain(&mut self) {
        if !self.drain_on_done || self.drain_task.is_some() {
            return;
        }

        let Some(mut inner) = self.inner.take() else {
            return;
        };

        self.drain_task = Some(tokio::spawn(async move {
            let mut buf = [0u8; 8192];
            loop {
                match inner.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(_) => continue,
                    Err(_) => break,
                }
            }
        }));
    }
}

impl<R: AsyncRead + Unpin + Send + Sync + 'static> AsyncRead for RangedDecompressReader<R> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        use std::pin::Pin;
        use std::task::Poll;
        use tokio::io::ReadBuf;

        let this = self.as_mut().get_mut();

        loop {
            // If we've returned all the bytes we need, return EOF
            if this.bytes_returned >= this.target_length {
                this.start_drain();
                return Poll::Ready(Ok(()));
            }

            // Read from the inner stream
            let buf_capacity = buf.remaining();
            if buf_capacity == 0 {
                return Poll::Ready(Ok(()));
            }

            let scratch_len = std::cmp::min(this.scratch.len(), std::cmp::max(buf_capacity, 1));
            let mut temp_read_buf = ReadBuf::uninit(&mut this.scratch[..scratch_len]);

            let Some(inner) = this.inner.as_mut() else {
                return Poll::Ready(Ok(()));
            };

            match Pin::new(inner).poll_read(cx, &mut temp_read_buf) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(())) => {
                    let n = temp_read_buf.filled().len();
                    if n == 0 {
                        // EOF from inner stream
                        if this.current_offset < this.target_offset {
                            // We haven't reached the target offset yet - this is an error
                            return Poll::Ready(Err(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                format!(
                                    "Unexpected EOF: only read {} bytes, target offset is {}",
                                    this.current_offset, this.target_offset
                                ),
                            )));
                        }
                        // Normal EOF after reaching target
                        return Poll::Ready(Ok(()));
                    }

                    // Update current position
                    let old_offset = this.current_offset;
                    this.current_offset += n;

                    // Check if we're still in the skip phase
                    if old_offset < this.target_offset {
                        // We're still skipping data
                        let skip_end = std::cmp::min(this.current_offset, this.target_offset);
                        let bytes_to_skip_in_this_read = skip_end - old_offset;

                        if this.current_offset <= this.target_offset {
                            // All data in this read should be skipped
                            tracing::trace!("Skipping {} bytes at offset {}", n, old_offset);
                            // Continue reading in the loop instead of recursive call
                            continue;
                        } else {
                            // Partial skip: some data should be returned
                            let data_start_in_buffer = bytes_to_skip_in_this_read;
                            let available_data = n - data_start_in_buffer;
                            let bytes_to_return = std::cmp::min(
                                available_data,
                                std::cmp::min(buf.remaining(), this.target_length - this.bytes_returned),
                            );

                            if bytes_to_return > 0 {
                                let data_slice =
                                    &temp_read_buf.filled()[data_start_in_buffer..data_start_in_buffer + bytes_to_return];
                                buf.put_slice(data_slice);
                                this.bytes_returned += bytes_to_return;

                                tracing::trace!(
                                    "Skipped {} bytes, returned {} bytes at offset {}",
                                    bytes_to_skip_in_this_read,
                                    bytes_to_return,
                                    old_offset
                                );
                            }
                            return Poll::Ready(Ok(()));
                        }
                    } else {
                        // We're in the data return phase
                        let bytes_to_return =
                            std::cmp::min(n, std::cmp::min(buf.remaining(), this.target_length - this.bytes_returned));

                        if bytes_to_return > 0 {
                            buf.put_slice(&temp_read_buf.filled()[..bytes_to_return]);
                            this.bytes_returned += bytes_to_return;

                            tracing::trace!("Returned {} bytes at offset {}", bytes_to_return, old_offset);
                        }
                        return Poll::Ready(Ok(()));
                    }
                }
            }
        }
    }
}

impl<R: AsyncRead + Unpin + Send + Sync + 'static> Drop for RangedDecompressReader<R> {
    fn drop(&mut self) {
        if self.bytes_returned >= self.target_length {
            self.start_drain();
        }
    }
}

/// A wrapper that ensures the inner stream is fully consumed even if the outer reader stops early.
/// This prevents broken pipe errors in erasure coding scenarios where the writer expects
/// the full stream to be consumed.
pub struct StreamConsumer<R: AsyncRead + Unpin + Send + 'static> {
    inner: Option<R>,
    consumer_task: Option<tokio::task::JoinHandle<()>>,
}

impl<R: AsyncRead + Unpin + Send + 'static> StreamConsumer<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner: Some(inner),
            consumer_task: None,
        }
    }

    fn ensure_consumer_started(&mut self) {
        if self.consumer_task.is_none() && self.inner.is_some() {
            let mut inner = self.inner.take().unwrap();
            let task = tokio::spawn(async move {
                let mut buf = [0u8; 8192];
                loop {
                    match inner.read(&mut buf).await {
                        Ok(0) => break,    // EOF
                        Ok(_) => continue, // Keep consuming
                        Err(_) => break,   // Error, stop consuming
                    }
                }
            });
            self.consumer_task = Some(task);
        }
    }
}

impl<R: AsyncRead + Unpin + Send + 'static> AsyncRead for StreamConsumer<R> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        use std::pin::Pin;
        use std::task::Poll;

        if let Some(ref mut inner) = self.inner {
            Pin::new(inner).poll_read(cx, buf)
        } else {
            Poll::Ready(Ok(())) // EOF
        }
    }
}

impl<R: AsyncRead + Unpin + Send + 'static> Drop for StreamConsumer<R> {
    fn drop(&mut self) {
        self.ensure_consumer_started();
    }
}

fn encrypted_plaintext_size(
    oi: &ObjectInfo,
    is_multipart: bool,
    is_compressed: bool,
    recorded_plaintext_size: Option<i64>,
) -> Result<i64> {
    if is_compressed {
        return oi.get_actual_size().map_err(Into::into);
    }

    if is_multipart && recorded_plaintext_size.is_none() {
        return Ok(legacy_part_layout_totals(&oi.parts)
            .map(|(_, plaintext_size)| plaintext_size)
            .unwrap_or(oi.size));
    }

    Ok(recorded_plaintext_size.unwrap_or(oi.size))
}

fn is_multipart_encrypted_object(parts: &[ObjectPartInfo], etag: Option<&str>) -> bool {
    if parts.len() > 1 {
        return true;
    }

    etag.map(|etag| etag.trim_matches('"').len() != 32).unwrap_or(false)
}

fn multipart_part_numbers(parts: &[ObjectPartInfo]) -> Vec<usize> {
    parts.iter().map(|part| part.number).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
    use md5::{Digest, Md5};
    use rustfs_utils::http::{SSEC_ALGORITHM_HEADER, SSEC_KEY_MD5_HEADER};
    use std::collections::HashMap;
    use std::io::Cursor;
    use temp_env::async_with_vars;
    use tokio::io::AsyncReadExt;

    #[derive(Debug)]
    struct PendingPartialReader {
        data: &'static [u8],
        position: usize,
        pending: bool,
    }

    impl PendingPartialReader {
        fn new(data: &'static [u8]) -> Self {
            Self {
                data,
                position: 0,
                pending: true,
            }
        }
    }

    impl AsyncRead for PendingPartialReader {
        fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            if self.pending {
                self.pending = false;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            if self.position == self.data.len() {
                return Poll::Ready(Ok(()));
            }

            let length = buf.remaining().min(3).min(self.data.len() - self.position);
            let end = self.position + length;
            buf.put_slice(&self.data[self.position..end]);
            self.position = end;
            self.pending = true;
            Poll::Ready(Ok(()))
        }
    }

    const TEST_DIRECT_KEY_HEADER: &str = "x-rustfs-test-direct-key";
    const TEST_OBJECT_KEY_HEADER: &str = "x-rustfs-test-object-key";
    const TEST_NONCE_HEADER: &str = "x-rustfs-test-nonce";

    pub(super) static TEST_RESOLVER: TestObjectEncryptionResolver = TestObjectEncryptionResolver;

    pub(super) struct TestObjectEncryptionResolver;

    #[async_trait::async_trait]
    impl ObjectEncryptionResolver for TestObjectEncryptionResolver {
        async fn resolve_read_material(
            &self,
            request: ReadEncryptionRequest<'_>,
        ) -> std::result::Result<Option<ReadEncryptionMaterial>, EncryptionResolutionError> {
            if let Some(encoded) = request.metadata.get(TEST_OBJECT_KEY_HEADER) {
                let decoded = BASE64_STANDARD.decode(encoded).map_err(|_| {
                    EncryptionResolutionError::new(EncryptionResolutionErrorKind::InvalidMetadata, "invalid test object key")
                })?;
                let key_bytes = decoded.try_into().map_err(|_| {
                    EncryptionResolutionError::new(
                        EncryptionResolutionErrorKind::InvalidMetadata,
                        "invalid test object key length",
                    )
                })?;
                return Ok(Some(ReadEncryptionMaterial {
                    key_bytes,
                    mode: ReadEncryptionMode::Object,
                }));
            }

            let encoded = request
                .headers
                .get(TEST_DIRECT_KEY_HEADER)
                .ok_or_else(|| {
                    EncryptionResolutionError::new(EncryptionResolutionErrorKind::InvalidRequest, "missing test direct key")
                })?
                .to_str()
                .map_err(|_| {
                    EncryptionResolutionError::new(EncryptionResolutionErrorKind::InvalidRequest, "invalid test encryption key")
                })?;
            let decoded = BASE64_STANDARD.decode(encoded).map_err(|_| {
                EncryptionResolutionError::new(EncryptionResolutionErrorKind::InvalidRequest, "invalid test encryption key")
            })?;
            let key_bytes = decoded.try_into().map_err(|_| {
                EncryptionResolutionError::new(
                    EncryptionResolutionErrorKind::InvalidRequest,
                    "invalid test encryption key length",
                )
            })?;
            let base_nonce = request
                .metadata
                .get(TEST_NONCE_HEADER)
                .and_then(|encoded| BASE64_STANDARD.decode(encoded).ok())
                .and_then(|bytes| bytes.try_into().ok())
                .unwrap_or_else(|| fixture_nonce(request.bucket, request.object));
            Ok(Some(ReadEncryptionMaterial {
                key_bytes,
                mode: ReadEncryptionMode::Direct { base_nonce },
            }))
        }
    }

    fn md5_bytes(data: impl AsRef<[u8]>) -> [u8; 16] {
        let digest = Md5::digest(data.as_ref());
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&digest);
        bytes
    }

    fn fixture_nonce(bucket: &str, object: &str) -> [u8; 12] {
        let digest = md5_bytes(format!("{bucket}-{object}"));
        let mut nonce = [0; 12];
        nonce.copy_from_slice(&digest[..12]);
        nonce
    }

    fn ssec_headers_from_key(key_bytes: [u8; 32]) -> HeaderMap<HeaderValue> {
        let mut headers = HeaderMap::new();
        headers.insert(
            TEST_DIRECT_KEY_HEADER,
            HeaderValue::from_str(&BASE64_STANDARD.encode(key_bytes)).expect("test key header is valid"),
        );
        headers
    }

    #[tokio::test]
    async fn cache_body_uses_plaintext_length_for_compressed_metadata() {
        let mut metadata = HashMap::new();
        rustfs_utils::http::insert_str(
            &mut metadata,
            rustfs_utils::http::SUFFIX_COMPRESSION,
            "klauspost/compress/s2".to_string(),
        );
        let object_info = ObjectInfo {
            size: 3,
            actual_size: 11,
            user_defined: Arc::new(metadata),
            ..Default::default()
        };
        assert!(object_info.is_compressed());

        let body = Bytes::from_static(b"hello world");
        let mut reader =
            GetObjectReader::from_cache_body(object_info, body.clone()).expect("cache body length must fit in object metadata");

        assert_eq!(reader.body_source, GetObjectBodySource::HookServed);
        assert_eq!(reader.buffered_body.as_ref(), Some(&body));
        assert_eq!(reader.object_info.size, 11);
        assert_eq!(reader.object_info.actual_size, 11);
        assert!(reader.object_info.is_compressed());
        let mut restored = Vec::new();
        reader
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("cache body should stream");
        assert_eq!(restored, body);
    }

    #[tokio::test]
    async fn test_ranged_decompress_reader() {
        // Create test data
        let original_data = b"Hello, World! This is a test for range requests on compressed data.";

        // For this test, we'll simulate using the original data directly as "decompressed"
        let cursor = Cursor::new(original_data.to_vec());

        // Test reading a range from the middle
        let mut ranged_reader = RangedDecompressReader::new(cursor, 7, 5, original_data.len()).unwrap();

        let mut result = Vec::new();
        ranged_reader.read_to_end(&mut result).await.unwrap();

        // Should read "World" (5 bytes starting from position 7)
        assert_eq!(result, b"World");
    }

    #[tokio::test]
    async fn uninitialized_scratch_preserves_partial_pending_and_eof_reads() {
        let mut skipped = SkipReader::new(PendingPartialReader::new(b"0123456789abcdef"), 5);
        let mut skipped_output = Vec::new();
        skipped
            .read_to_end(&mut skipped_output)
            .await
            .expect("skip reader should survive partial pending reads through EOF");
        assert_eq!(skipped_output, b"56789abcdef");

        let mut ranged = RangedDecompressReader::new(PendingPartialReader::new(b"0123456789abcdef"), 5, 7, 16)
            .expect("valid range should construct");
        let mut ranged_output = Vec::new();
        ranged
            .read_to_end(&mut ranged_output)
            .await
            .expect("range reader should survive partial pending reads through EOF");
        assert_eq!(ranged_output, b"56789ab");
    }

    #[tokio::test]
    async fn uninitialized_skip_scratch_reports_early_eof() {
        let mut reader = SkipReader::new(PendingPartialReader::new(b"short"), 6);
        let error = reader
            .read_to_end(&mut Vec::new())
            .await
            .expect_err("EOF before the skip boundary must remain visible");
        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn test_ranged_decompress_reader_from_start() {
        let original_data = b"Hello, World! This is a test.";
        let cursor = Cursor::new(original_data.to_vec());

        let mut ranged_reader = RangedDecompressReader::new(cursor, 0, 5, original_data.len()).unwrap();

        let mut result = Vec::new();
        ranged_reader.read_to_end(&mut result).await.unwrap();

        // Should read "Hello" (5 bytes from the start)
        assert_eq!(result, b"Hello");
    }

    #[tokio::test]
    async fn test_ranged_decompress_reader_to_end() {
        let original_data = b"Hello, World!";
        let cursor = Cursor::new(original_data.to_vec());

        let mut ranged_reader = RangedDecompressReader::new(cursor, 7, 6, original_data.len()).unwrap();

        let mut result = Vec::new();
        ranged_reader.read_to_end(&mut result).await.unwrap();

        // Should read "World!" (6 bytes starting from position 7)
        assert_eq!(result, b"World!");
    }

    #[tokio::test]
    async fn test_http_range_spec_with_compressed_data() {
        // Test that HTTPRangeSpec::get_offset_length works correctly
        let range_spec = HTTPRangeSpec {
            is_suffix_length: false,
            start: 5,
            end: 14, // inclusive
        };

        let total_size = 100i64;
        let (offset, length) = range_spec.get_offset_length(total_size).unwrap();

        assert_eq!(offset, 5);
        assert_eq!(length, 10); // end - start + 1 = 14 - 5 + 1 = 10
    }

    #[test]
    fn test_http_range_spec_suffix_positive_start() {
        let range_spec = HTTPRangeSpec {
            is_suffix_length: true,
            start: 5,
            end: -1,
        };

        let (offset, length) = range_spec.get_offset_length(20).unwrap();
        assert_eq!(offset, 15);
        assert_eq!(length, 5);
    }

    #[test]
    fn test_http_range_spec_suffix_negative_start() {
        let range_spec = HTTPRangeSpec {
            is_suffix_length: true,
            start: -5,
            end: -1,
        };

        let (offset, length) = range_spec.get_offset_length(20).unwrap();
        assert_eq!(offset, 15);
        assert_eq!(length, 5);
    }

    #[test]
    fn test_http_range_spec_suffix_exceeds_object() {
        let range_spec = HTTPRangeSpec {
            is_suffix_length: true,
            start: 50,
            end: -1,
        };

        let (offset, length) = range_spec.get_offset_length(20).unwrap();
        assert_eq!(offset, 0);
        assert_eq!(length, 20);
    }

    #[test]
    fn test_http_range_spec_from_object_info_valid_and_invalid_parts() {
        let object_info = ObjectInfo {
            size: 300,
            parts: Arc::new(vec![
                ObjectPartInfo {
                    etag: String::new(),
                    number: 1,
                    size: 100,
                    actual_size: 100,
                    ..Default::default()
                },
                ObjectPartInfo {
                    etag: String::new(),
                    number: 2,
                    size: 100,
                    actual_size: 100,
                    ..Default::default()
                },
                ObjectPartInfo {
                    etag: String::new(),
                    number: 3,
                    size: 100,
                    actual_size: 100,
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };

        let spec = http_range_spec_from_object_info(&object_info, 2).unwrap();
        assert_eq!(spec.start, 100);
        assert_eq!(spec.end, 199);

        assert!(http_range_spec_from_object_info(&object_info, 0).is_none());
        assert!(http_range_spec_from_object_info(&object_info, 4).is_none());
    }

    #[test]
    fn test_http_range_spec_from_object_info_uses_actual_size() {
        let object_info = ObjectInfo {
            size: 90,
            parts: Arc::new(vec![
                ObjectPartInfo {
                    etag: String::new(),
                    number: 1,
                    size: 20,
                    actual_size: 30,
                    ..Default::default()
                },
                ObjectPartInfo {
                    etag: String::new(),
                    number: 2,
                    size: 30,
                    actual_size: 40,
                    ..Default::default()
                },
                ObjectPartInfo {
                    etag: String::new(),
                    number: 3,
                    size: 40,
                    actual_size: 50,
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };

        let spec = http_range_spec_from_object_info(&object_info, 2).unwrap();
        assert_eq!(spec.start, 30);
        assert_eq!(spec.end, 69);
    }

    #[test]
    fn test_http_range_spec_from_object_info_falls_back_to_part_size_when_actual_size_missing() {
        let object_info = ObjectInfo {
            size: 90,
            parts: Arc::new(vec![
                ObjectPartInfo {
                    etag: String::new(),
                    number: 1,
                    size: 20,
                    actual_size: 0,
                    ..Default::default()
                },
                ObjectPartInfo {
                    etag: String::new(),
                    number: 2,
                    size: 30,
                    actual_size: 40,
                    ..Default::default()
                },
                ObjectPartInfo {
                    etag: String::new(),
                    number: 3,
                    size: 40,
                    actual_size: 0,
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };

        let spec = http_range_spec_from_object_info(&object_info, 3).unwrap();
        assert_eq!(spec.start, 60);
        assert_eq!(spec.end, 99);
    }

    #[tokio::test]
    async fn test_ranged_decompress_reader_zero_length() {
        let original_data = b"Hello, World!";
        let cursor = Cursor::new(original_data.to_vec());
        let mut ranged_reader = RangedDecompressReader::new(cursor, 5, 0, original_data.len()).unwrap();
        let mut result = Vec::new();
        ranged_reader.read_to_end(&mut result).await.unwrap();
        // Should read nothing
        assert_eq!(result, b"");
    }

    #[tokio::test]
    async fn test_ranged_decompress_reader_skip_entire_data() {
        let original_data = b"Hello, World!";
        let cursor = Cursor::new(original_data.to_vec());
        // Skip to end of data with length 0 - this should read nothing
        let mut ranged_reader = RangedDecompressReader::new(cursor, original_data.len() - 1, 0, original_data.len()).unwrap();
        let mut result = Vec::new();
        ranged_reader.read_to_end(&mut result).await.unwrap();
        assert_eq!(result, b"");
    }

    #[tokio::test]
    async fn test_ranged_decompress_reader_out_of_bounds_offset() {
        let original_data = b"Hello, World!";
        let cursor = Cursor::new(original_data.to_vec());
        // Offset beyond EOF should return error in constructor
        let result = RangedDecompressReader::new(cursor, original_data.len() + 10, 5, original_data.len());
        assert!(result.is_err());
        // Use pattern matching to avoid requiring Debug on the error type
        if let Err(e) = result {
            assert!(e.to_string().contains("Range offset exceeds file size"));
        }
    }

    #[tokio::test]
    async fn test_ranged_decompress_reader_partial_read() {
        let original_data = b"abcdef";
        let cursor = Cursor::new(original_data.to_vec());
        let mut ranged_reader = RangedDecompressReader::new(cursor, 2, 3, original_data.len()).unwrap();
        let mut buf = [0u8; 2];
        let n = ranged_reader.read(&mut buf).await.unwrap();
        assert_eq!(n, 2);
        assert_eq!(&buf, b"cd");
        let mut buf2 = [0u8; 2];
        let n2 = ranged_reader.read(&mut buf2).await.unwrap();
        assert_eq!(n2, 1);
        assert_eq!(&buf2[..1], b"e");
    }

    #[cfg(feature = "rio-v2")]
    #[tokio::test]
    async fn test_ranged_decompress_reader_with_rio_v2_s2_stream() {
        let plaintext = b"abcdefghijklmnopqrstuvwxyz".to_vec();
        let mut compressed = Vec::new();
        crate::io_support::rio::CompressReader::new(Cursor::new(plaintext.clone()), CompressionAlgorithm::default())
            .read_to_end(&mut compressed)
            .await
            .expect("compress plaintext into rio_v2 stream");

        let decompress_reader =
            crate::io_support::rio::DecompressReader::new(Cursor::new(compressed), CompressionAlgorithm::default());
        let mut ranged_reader =
            RangedDecompressReader::new(decompress_reader, 5, 7, plaintext.len()).expect("create ranged reader");

        let mut actual = Vec::new();
        ranged_reader
            .read_to_end(&mut actual)
            .await
            .expect("read ranged decompressed plaintext");

        assert_eq!(actual, b"fghijkl");
    }

    /// Compresses one multipart part exactly like the write path does
    /// (`WritePlan::with_compression` wraps each part in its own
    /// `compression_reader`), returning the on-disk bytes and the storage-format
    /// compression index.
    async fn compressed_part_fixture(data: &[u8]) -> (Vec<u8>, Option<Bytes>) {
        use crate::io_support::rio::TryGetIndex as _;
        let mut compressor =
            crate::io_support::rio::compression_reader(Cursor::new(data.to_vec()), CompressionAlgorithm::default(), false);
        let mut compressed = Vec::new();
        compressor.read_to_end(&mut compressed).await.expect("compress part stream");
        let index = compressor
            .try_get_index()
            .map(crate::io_support::rio::compression_index_storage_bytes);
        (compressed, index)
    }

    struct CompressedMultipartFixture {
        object_info: ObjectInfo,
        stored: Vec<u8>,
        plaintext: Vec<u8>,
    }

    /// Builds the on-disk representation of a compressed multipart object: each
    /// part is an independent compressed stream and the storage layer serves
    /// their concatenation.
    async fn compressed_multipart_fixture(part_sizes: &[usize]) -> CompressedMultipartFixture {
        let pattern = b"compressed multipart read path fixture data ";
        let mut plaintext = Vec::new();
        let mut stored = Vec::new();
        let mut parts = Vec::with_capacity(part_sizes.len());

        for (i, part_size) in part_sizes.iter().enumerate() {
            let mut part_plaintext = Vec::with_capacity(*part_size);
            while part_plaintext.len() < *part_size {
                part_plaintext.extend_from_slice(pattern);
                part_plaintext.push(i as u8);
            }
            part_plaintext.truncate(*part_size);

            let (compressed, index) = compressed_part_fixture(&part_plaintext).await;
            parts.push(ObjectPartInfo {
                number: i + 1,
                size: compressed.len(),
                actual_size: *part_size as i64,
                index,
                ..Default::default()
            });
            stored.extend_from_slice(&compressed);
            plaintext.extend_from_slice(&part_plaintext);
        }

        let mut user_defined = HashMap::new();
        rustfs_utils::http::insert_str(
            &mut user_defined,
            rustfs_utils::http::SUFFIX_COMPRESSION,
            crate::io_support::rio::compression_metadata_value(CompressionAlgorithm::default()),
        );
        rustfs_utils::http::insert_str(&mut user_defined, rustfs_utils::http::SUFFIX_ACTUAL_SIZE, plaintext.len().to_string());

        let object_info = ObjectInfo {
            bucket: "test-bucket".to_string(),
            name: "compressed-multipart".to_string(),
            size: stored.len() as i64,
            etag: Some(format!("6bcf86bed8807b8e78f0fc6e0a53079d-{}", part_sizes.len())),
            parts: Arc::new(parts),
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };

        CompressedMultipartFixture {
            object_info,
            stored,
            plaintext,
        }
    }

    /// Plans the read once to learn the storage window, then serves exactly that
    /// window — mirroring how `set_disk` feeds the erasure read into the
    /// returned reader.
    async fn read_compressed_multipart(
        fixture: &CompressedMultipartFixture,
        rs: Option<HTTPRangeSpec>,
        opts: &ObjectOptions,
    ) -> Vec<u8> {
        let headers = HeaderMap::new();
        let (_, offset, length) =
            GetObjectReader::new(Box::new(Cursor::new(Vec::new())), rs.clone(), &fixture.object_info, opts, &headers)
                .await
                .expect("plan compressed multipart read");

        let end = offset + usize::try_from(length).expect("storage window length must be non-negative");
        assert!(
            end <= fixture.stored.len(),
            "planned storage window {offset}..{end} exceeds stored stream of {} bytes",
            fixture.stored.len()
        );
        let window = fixture.stored[offset..end].to_vec();

        let (mut reader, replay_offset, replay_length) =
            GetObjectReader::new(Box::new(Cursor::new(window)), rs, &fixture.object_info, opts, &headers)
                .await
                .expect("build compressed multipart reader");
        assert_eq!((replay_offset, replay_length), (offset, length), "read plan must be deterministic");

        reader.read_all().await.expect("read compressed multipart stream")
    }

    /// Byte pattern with a 2 KiB period: it compresses extremely well while
    /// looking nothing like ASCII fixtures. Mirrors the e2e generator that
    /// exposed a truncated full GET on high-ratio multipart payloads.
    fn high_ratio_binary_payload(size: usize, seed: u8) -> Vec<u8> {
        (0..size)
            .map(|i| ((i as u64).wrapping_mul(2_654_435_761).wrapping_add(seed as u64) >> 3) as u8)
            .collect()
    }

    #[tokio::test]
    async fn compressed_multipart_full_get_handles_high_ratio_binary_payload() {
        let part_sizes = [5 * 1024 * 1024_usize, 1024 * 1024];
        let mut plaintext = Vec::new();
        let mut stored = Vec::new();
        let mut parts = Vec::with_capacity(part_sizes.len());

        for (i, part_size) in part_sizes.iter().enumerate() {
            let part_plaintext = high_ratio_binary_payload(*part_size, if i == 0 { 7 } else { 61 });
            let (compressed, index) = compressed_part_fixture(&part_plaintext).await;
            parts.push(ObjectPartInfo {
                number: i + 1,
                size: compressed.len(),
                actual_size: *part_size as i64,
                index,
                ..Default::default()
            });
            stored.extend_from_slice(&compressed);
            plaintext.extend_from_slice(&part_plaintext);
        }

        let mut user_defined = HashMap::new();
        rustfs_utils::http::insert_str(
            &mut user_defined,
            rustfs_utils::http::SUFFIX_COMPRESSION,
            crate::io_support::rio::compression_metadata_value(CompressionAlgorithm::default()),
        );
        rustfs_utils::http::insert_str(&mut user_defined, rustfs_utils::http::SUFFIX_ACTUAL_SIZE, plaintext.len().to_string());
        let fixture = CompressedMultipartFixture {
            object_info: ObjectInfo {
                bucket: "test-bucket".to_string(),
                name: "high-ratio-multipart".to_string(),
                size: stored.len() as i64,
                etag: Some("6bcf86bed8807b8e78f0fc6e0a53079d-2".to_string()),
                parts: Arc::new(parts),
                user_defined: Arc::new(user_defined),
                ..Default::default()
            },
            stored,
            plaintext,
        };

        let read = read_compressed_multipart(&fixture, None, &ObjectOptions::default()).await;

        assert_eq!(read.len(), fixture.plaintext.len(), "full GET must return the logical size");
        assert_eq!(read, fixture.plaintext, "high-ratio multipart payload must survive the roundtrip");
    }

    /// Full GET over a compressed multipart object must decode across part
    /// boundaries: every part is an independent compressed stream (this is also
    /// the on-disk shape written by builds before rustfs/rustfs#5169 disabled
    /// multipart compression, so this pins legacy-object readability).
    #[tokio::test]
    async fn compressed_multipart_full_get_decodes_across_part_boundaries() {
        let fixture = compressed_multipart_fixture(&[3 * 1024 * 1024, 2 * 1024 * 1024, 512 * 1024]).await;

        let read = read_compressed_multipart(&fixture, None, &ObjectOptions::default()).await;

        assert_eq!(read.len(), fixture.plaintext.len(), "full GET must return the logical size");
        assert_eq!(read, fixture.plaintext, "full GET must reassemble all parts");
    }

    #[tokio::test]
    async fn compressed_multipart_range_get_crosses_part_boundary() {
        let fixture = compressed_multipart_fixture(&[3 * 1024 * 1024, 2 * 1024 * 1024]).await;
        let boundary = 3 * 1024 * 1024_i64;
        let rs = HTTPRangeSpec {
            is_suffix_length: false,
            start: boundary - 100_000,
            end: boundary + 100_000 - 1,
        };

        let read = read_compressed_multipart(&fixture, Some(rs), &ObjectOptions::default()).await;

        let expected = &fixture.plaintext[(boundary - 100_000) as usize..(boundary + 100_000) as usize];
        assert_eq!(read, expected, "boundary-crossing range must splice both parts");
    }

    #[tokio::test]
    async fn compressed_multipart_range_get_seeks_into_later_part() {
        let fixture = compressed_multipart_fixture(&[3 * 1024 * 1024, 4 * 1024 * 1024]).await;
        // Deep inside part 2 so the plan skips part 1 entirely and (when the
        // part carries an index) seeks within part 2.
        let start = 3 * 1024 * 1024_i64 + 2 * 1024 * 1024_i64 + 137;
        let rs = HTTPRangeSpec {
            is_suffix_length: false,
            start,
            end: start + 64 * 1024 - 1,
        };

        let read = read_compressed_multipart(&fixture, Some(rs), &ObjectOptions::default()).await;

        let expected = &fixture.plaintext[start as usize..(start + 64 * 1024) as usize];
        assert_eq!(read, expected, "range inside a later part must decode from that part");
    }

    /// Parts written without a compression index (small parts skip the index in
    /// the rio-v2 backend) must still be rangeable: the plan starts at the part
    /// boundary and skips decompressed bytes.
    #[tokio::test]
    async fn compressed_multipart_range_get_works_without_part_indexes() {
        let mut fixture = compressed_multipart_fixture(&[1024 * 1024, 1024 * 1024]).await;
        let parts = fixture
            .object_info
            .parts
            .iter()
            .map(|part| ObjectPartInfo {
                index: None,
                ..part.clone()
            })
            .collect::<Vec<_>>();
        fixture.object_info.parts = Arc::new(parts);

        let start = 1024 * 1024_i64 + 4096;
        let rs = HTTPRangeSpec {
            is_suffix_length: false,
            start,
            end: start + 32 * 1024 - 1,
        };

        let read = read_compressed_multipart(&fixture, Some(rs), &ObjectOptions::default()).await;

        let expected = &fixture.plaintext[start as usize..(start + 32 * 1024) as usize];
        assert_eq!(read, expected, "index-less parts must fall back to part-boundary skip");
    }

    #[tokio::test]
    async fn compressed_multipart_part_number_get_returns_single_part() {
        let part_sizes = [3 * 1024 * 1024, 2 * 1024 * 1024, 512 * 1024];
        let fixture = compressed_multipart_fixture(&part_sizes).await;

        let mut logical_offset = 0_usize;
        for (i, part_size) in part_sizes.iter().enumerate() {
            let opts = ObjectOptions {
                part_number: Some(i + 1),
                ..Default::default()
            };

            let read = read_compressed_multipart(&fixture, None, &opts).await;

            let expected = &fixture.plaintext[logical_offset..logical_offset + part_size];
            assert_eq!(read.len(), *part_size, "partNumber={} GET must return the part's logical size", i + 1);
            assert_eq!(read, expected, "partNumber={} GET must return the original part bytes", i + 1);
            logical_offset += part_size;
        }
    }

    #[tokio::test]
    async fn compressed_multipart_suffix_range_reads_tail() {
        let fixture = compressed_multipart_fixture(&[3 * 1024 * 1024, 1024 * 1024]).await;
        let suffix_len = 128 * 1024_i64;
        let rs = HTTPRangeSpec {
            is_suffix_length: true,
            start: suffix_len,
            end: -1,
        };

        let read = read_compressed_multipart(&fixture, Some(rs), &ObjectOptions::default()).await;

        let expected = &fixture.plaintext[fixture.plaintext.len() - suffix_len as usize..];
        assert_eq!(read, expected, "suffix range must return the tail of the last part");
    }

    /// Builds an SSE-C + disk-compression multipart object exactly like the
    /// write path: each part is compressed into its own stream and then
    /// encrypted with the per-part key schedule. The fixture is
    /// legacy-encryption-specific (`rustfs_rio::EncryptReader`), matching the
    /// pre-existing `build_legacy_ssec_multipart_fixture` shape, while the
    /// compression layer follows the active backend feature.
    async fn compressed_encrypted_multipart_fixture(key_bytes: [u8; 32], part_sizes: &[usize]) -> CompressedMultipartFixture {
        let pattern = b"compressed encrypted multipart fixture data ";
        let mut plaintext = Vec::new();
        let mut stored = Vec::new();
        let mut parts = Vec::with_capacity(part_sizes.len());

        for (i, part_size) in part_sizes.iter().enumerate() {
            let part_number = i + 1;
            let mut part_plaintext = Vec::with_capacity(*part_size);
            while part_plaintext.len() < *part_size {
                part_plaintext.extend_from_slice(pattern);
                part_plaintext.push(part_number as u8);
            }
            part_plaintext.truncate(*part_size);

            let (compressed, index) = compressed_part_fixture(&part_plaintext).await;
            let mut part_cipher = Vec::new();
            rustfs_rio::EncryptReader::new_multipart(Cursor::new(compressed), key_bytes, LEGACY_FIXTURE_BASE_NONCE, part_number)
                .read_to_end(&mut part_cipher)
                .await
                .expect("encrypt compressed fixture part");

            parts.push(ObjectPartInfo {
                number: part_number,
                size: part_cipher.len(),
                actual_size: *part_size as i64,
                index,
                ..Default::default()
            });
            stored.extend_from_slice(&part_cipher);
            plaintext.extend_from_slice(&part_plaintext);
        }

        let mut user_defined = legacy_ssec_multipart_metadata(key_bytes, plaintext.len());
        rustfs_utils::http::insert_str(
            &mut user_defined,
            rustfs_utils::http::SUFFIX_COMPRESSION,
            crate::io_support::rio::compression_metadata_value(CompressionAlgorithm::default()),
        );
        rustfs_utils::http::insert_str(&mut user_defined, rustfs_utils::http::SUFFIX_ACTUAL_SIZE, plaintext.len().to_string());

        let object_info = ObjectInfo {
            bucket: "test-bucket".to_string(),
            name: "compressed-encrypted-multipart".to_string(),
            size: stored.len() as i64,
            etag: Some(format!("6bcf86bed8807b8e78f0fc6e0a53079d-{}", part_sizes.len())),
            parts: Arc::new(parts),
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };

        CompressedMultipartFixture {
            object_info,
            stored,
            plaintext,
        }
    }

    async fn read_compressed_encrypted_multipart(
        fixture: &CompressedMultipartFixture,
        key_bytes: [u8; 32],
        rs: Option<HTTPRangeSpec>,
        opts: &ObjectOptions,
    ) -> Vec<u8> {
        let headers = ssec_headers_from_key(key_bytes);
        let (_, offset, length) =
            GetObjectReader::new(Box::new(Cursor::new(Vec::new())), rs.clone(), &fixture.object_info, opts, &headers)
                .await
                .expect("plan compressed encrypted multipart read");

        let end = offset + usize::try_from(length).expect("storage window length must be non-negative");
        assert!(
            end <= fixture.stored.len(),
            "planned storage window {offset}..{end} exceeds stored stream of {} bytes",
            fixture.stored.len()
        );
        let window = fixture.stored[offset..end].to_vec();

        let (mut reader, replay_offset, replay_length) =
            GetObjectReader::new(Box::new(Cursor::new(window)), rs, &fixture.object_info, opts, &headers)
                .await
                .expect("build compressed encrypted multipart reader");
        assert_eq!((replay_offset, replay_length), (offset, length), "read plan must be deterministic");

        reader.read_all().await.expect("read compressed encrypted multipart stream")
    }

    #[tokio::test]
    async fn compressed_encrypted_multipart_full_get_roundtrip() {
        let key_bytes = [0x6Eu8; 32];
        let fixture = compressed_encrypted_multipart_fixture(key_bytes, &[3 * 1024 * 1024, 1024 * 1024]).await;

        let read = read_compressed_encrypted_multipart(&fixture, key_bytes, None, &ObjectOptions::default()).await;

        assert_eq!(read.len(), fixture.plaintext.len(), "full GET must return the logical size");
        assert_eq!(read, fixture.plaintext, "SSE-C + compression full GET must reassemble all parts");
    }

    #[tokio::test]
    async fn compressed_encrypted_multipart_range_crosses_part_boundary() {
        let key_bytes = [0x6Eu8; 32];
        let fixture = compressed_encrypted_multipart_fixture(key_bytes, &[3 * 1024 * 1024, 1024 * 1024]).await;
        let boundary = 3 * 1024 * 1024_i64;
        let rs = HTTPRangeSpec {
            is_suffix_length: false,
            start: boundary - 65_536,
            end: boundary + 65_536 - 1,
        };

        let read = read_compressed_encrypted_multipart(&fixture, key_bytes, Some(rs), &ObjectOptions::default()).await;

        let expected = &fixture.plaintext[(boundary - 65_536) as usize..(boundary + 65_536) as usize];
        assert_eq!(read, expected, "SSE-C + compression boundary-crossing range must splice both parts");
    }

    #[tokio::test]
    async fn compressed_encrypted_multipart_part_number_get_returns_single_part() {
        let key_bytes = [0x6Eu8; 32];
        let part_sizes = [3 * 1024 * 1024, 1024 * 1024];
        let fixture = compressed_encrypted_multipart_fixture(key_bytes, &part_sizes).await;

        let opts = ObjectOptions {
            part_number: Some(2),
            ..Default::default()
        };
        let read = read_compressed_encrypted_multipart(&fixture, key_bytes, None, &opts).await;

        let expected = &fixture.plaintext[part_sizes[0]..];
        assert_eq!(read.len(), part_sizes[1], "partNumber=2 GET must return the part's logical size");
        assert_eq!(read, expected, "partNumber=2 GET must return the original part bytes");
    }

    #[tokio::test]
    async fn test_get_object_reader_rejects_ssec_read_without_headers() {
        let object_info = ObjectInfo {
            size: 10,
            user_defined: Arc::new(HashMap::from([
                ("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string()),
                ("x-amz-server-side-encryption-customer-original-size".to_string(), "20".to_string()),
            ])),
            ..Default::default()
        };

        let range = HTTPRangeSpec {
            is_suffix_length: false,
            start: 8,
            end: -1,
        };

        let result = GetObjectReader::new(
            Box::new(Cursor::new(b"0123456789".to_vec())),
            Some(range),
            &object_info,
            &ObjectOptions::default(),
            &HeaderMap::new(),
        )
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_object_reader_restore_request_bypasses_encryption_range_rewrite() {
        let object_info = ObjectInfo {
            size: 10,
            user_defined: Arc::new(HashMap::from([
                ("x-rustfs-encryption-key".to_string(), "encrypted-key".to_string()),
                ("x-rustfs-encryption-original-size".to_string(), "20".to_string()),
            ])),
            ..Default::default()
        };

        let range = HTTPRangeSpec {
            is_suffix_length: true,
            start: 4,
            end: -1,
        };

        let mut opts = ObjectOptions::default();
        opts.transition.restore_request.days = Some(1);

        let (_, offset, length) = GetObjectReader::new(
            Box::new(Cursor::new(b"0123456789".to_vec())),
            Some(range),
            &object_info,
            &opts,
            &HeaderMap::new(),
        )
        .await
        .unwrap();

        assert_eq!(offset, 6);
        assert_eq!(length, 4);
    }

    #[tokio::test]
    async fn test_read_plan_restore_request_uses_plain_range() {
        let object_info = ObjectInfo {
            size: 10,
            user_defined: Arc::new(HashMap::from([
                ("x-rustfs-encryption-key".to_string(), "encrypted-key".to_string()),
                ("x-rustfs-encryption-original-size".to_string(), "20".to_string()),
            ])),
            ..Default::default()
        };

        let range = HTTPRangeSpec {
            is_suffix_length: true,
            start: 4,
            end: -1,
        };

        let mut opts = ObjectOptions::default();
        opts.transition.restore_request.days = Some(1);

        let plan = ReadPlan::build(Some(range), &object_info, &opts, &HeaderMap::new())
            .await
            .expect("restore requests should bypass rio transforms");

        assert_eq!(plan.storage_offset, 6);
        assert_eq!(plan.storage_length, 4);
        assert_eq!(plan.object_size, 10);
        assert!(matches!(
            plan.transform,
            ReadTransform::Plain {
                visible_offset: 6,
                visible_length: 4
            }
        ));
    }

    #[tokio::test]
    async fn test_raw_data_movement_read_plan_bypasses_compression_transform() {
        let object_info = ObjectInfo {
            size: 3_000_000,
            user_defined: Arc::new(HashMap::from([
                ("x-minio-internal-compression".to_string(), "klauspost/compress/s2".to_string()),
                ("x-minio-internal-actual-size".to_string(), "4194304".to_string()),
            ])),
            ..Default::default()
        };
        let opts = ObjectOptions {
            raw_data_movement_read: true,
            ..Default::default()
        };

        let plan = ReadPlan::build(None, &object_info, &opts, &HeaderMap::new())
            .await
            .expect("raw data movement read should bypass compression planning");

        assert_eq!(plan.storage_offset, 0);
        assert_eq!(plan.storage_length, object_info.size);
        assert_eq!(plan.object_size, object_info.size);
        assert!(matches!(
            plan.transform,
            ReadTransform::Plain {
                visible_offset: 0,
                visible_length: 3_000_000
            }
        ));
    }

    #[tokio::test]
    async fn test_raw_data_movement_read_plan_bypasses_encryption_transform() {
        let object_info = ObjectInfo {
            size: 128,
            user_defined: Arc::new(HashMap::from([
                ("X-Amz-Server-Side-Encryption".to_string(), "aws:kms".to_string()),
                ("X-Amz-Server-Side-Encryption-Iv".to_string(), "AAAAAAAAAAAAAAAA".to_string()),
                ("X-Amz-Server-Side-Encryption-Key".to_string(), BASE64_STANDARD.encode([7_u8; 32])),
                ("x-rustfs-encryption-original-size".to_string(), "64".to_string()),
            ])),
            ..Default::default()
        };
        let opts = ObjectOptions {
            raw_data_movement_read: true,
            ..Default::default()
        };

        let plan = ReadPlan::build(None, &object_info, &opts, &HeaderMap::new())
            .await
            .expect("raw data movement read should not require decryption material");

        assert_eq!(plan.storage_offset, 0);
        assert_eq!(plan.storage_length, object_info.size);
        assert_eq!(plan.object_size, object_info.size);
        assert!(matches!(
            plan.transform,
            ReadTransform::Plain {
                visible_offset: 0,
                visible_length: 128
            }
        ));
    }

    #[tokio::test]
    async fn test_raw_data_movement_read_plan_bypasses_ssec_header_resolution() {
        let object_info = ObjectInfo {
            size: 256,
            user_defined: Arc::new(HashMap::from([
                (SSEC_ALGORITHM_HEADER.to_string(), "AES256".to_string()),
                (SSEC_KEY_MD5_HEADER.to_string(), "stored-key-md5".to_string()),
            ])),
            ..Default::default()
        };
        let opts = ObjectOptions {
            raw_data_movement_read: true,
            ..Default::default()
        };

        let plan = ReadPlan::build(None, &object_info, &opts, &HeaderMap::new())
            .await
            .expect("raw data movement read should not require SSE-C request headers");

        assert_eq!(plan.storage_offset, 0);
        assert_eq!(plan.storage_length, object_info.size);
        assert_eq!(plan.object_size, object_info.size);
        assert!(matches!(
            plan.transform,
            ReadTransform::Plain {
                visible_offset: 0,
                visible_length: 256
            }
        ));
    }

    #[tokio::test]
    async fn test_get_object_reader_compressed_range_returns_physical_offset_from_index() {
        let mut index = Index::new();
        index.add(0, 0).unwrap();
        index.add(1_048_576, 2_097_152).unwrap();

        let object_info = ObjectInfo {
            size: 3_000_000,
            parts: Arc::new(vec![ObjectPartInfo {
                etag: String::new(),
                number: 1,
                size: 3_000_000,
                actual_size: 4_194_304,
                index: Some(index.into_vec()),
                ..Default::default()
            }]),
            user_defined: Arc::new(HashMap::from([
                ("x-minio-internal-compression".to_string(), "gzip".to_string()),
                ("x-minio-internal-actual-size".to_string(), "4194304".to_string()),
            ])),
            ..Default::default()
        };

        let decoded = crate::io_support::rio::decode_compression_index_bytes(object_info.parts[0].index.as_ref().unwrap())
            .expect("headerless MinIO-style compression index should decode");
        let (compressed_offset, uncompressed_offset) = decoded.find(2_097_152).expect("seek into decoded index");
        assert!(compressed_offset > 0);
        assert_eq!(uncompressed_offset, 2_097_152);
        let (physical_offset, decompressed_skip, _, _, _) = get_compressed_offsets(&object_info, 2_097_152);
        assert!(physical_offset > 0);
        assert_eq!(decompressed_skip, 0);

        let range = HTTPRangeSpec {
            is_suffix_length: false,
            start: 2_097_152,
            end: 2_097_161,
        };

        let (reader, offset, length) = GetObjectReader::new(
            Box::new(Cursor::new(Vec::<u8>::new())),
            Some(range),
            &object_info,
            &ObjectOptions::default(),
            &HeaderMap::new(),
        )
        .await
        .unwrap();

        assert!(offset > 0);
        assert!(offset < 2_097_152);
        assert_eq!(length, object_info.size - offset as i64);
        assert_eq!(reader.object_info.size, 10);
    }

    #[tokio::test]
    async fn test_read_plan_compressed_range_tracks_storage_and_visible_offsets() {
        let mut index = Index::new();
        index.add(0, 0).unwrap();
        index.add(1_048_576, 2_097_152).unwrap();

        let object_info = ObjectInfo {
            size: 3_000_000,
            parts: Arc::new(vec![ObjectPartInfo {
                etag: String::new(),
                number: 1,
                size: 3_000_000,
                actual_size: 4_194_304,
                index: Some(index.into_vec()),
                ..Default::default()
            }]),
            user_defined: Arc::new(HashMap::from([
                ("x-minio-internal-compression".to_string(), "gzip".to_string()),
                ("x-minio-internal-actual-size".to_string(), "4194304".to_string()),
            ])),
            ..Default::default()
        };

        let range = HTTPRangeSpec {
            is_suffix_length: false,
            start: 2_097_152,
            end: 2_097_161,
        };

        let plan = ReadPlan::build(Some(range), &object_info, &ObjectOptions::default(), &HeaderMap::new())
            .await
            .expect("compressed range should plan physical offset and visible range");

        assert!(plan.storage_offset > 0);
        assert!(plan.storage_offset < 2_097_152);
        assert_eq!(plan.storage_length, object_info.size - plan.storage_offset as i64);
        assert_eq!(plan.object_size, 10);

        assert!(matches!(
            plan.transform,
            ReadTransform::Compressed {
                decompressed_offset,
                decompressed_length: 10,
                ..
            } if decompressed_offset < 2_097_152
        ));
    }

    #[cfg(feature = "rio-v2")]
    #[tokio::test]
    async fn test_read_plan_accepts_minio_s2_compression_scheme() {
        let object_info = ObjectInfo {
            size: 3_000_000,
            user_defined: Arc::new(HashMap::from([
                ("x-minio-internal-compression".to_string(), "klauspost/compress/s2".to_string()),
                ("x-minio-internal-actual-size".to_string(), "4194304".to_string()),
            ])),
            ..Default::default()
        };

        let plan = ReadPlan::build(None, &object_info, &ObjectOptions::default(), &HeaderMap::new())
            .await
            .expect("MinIO S2 compression scheme should be accepted");

        assert!(matches!(
            plan.transform,
            ReadTransform::Compressed {
                decompressed_offset: 0,
                decompressed_length: 4_194_304,
                ..
            }
        ));
        assert_eq!(plan.object_size, 4_194_304);
    }

    #[cfg(feature = "rio-v2")]
    #[tokio::test]
    async fn test_read_plan_accepts_minio_headerless_compression_index() {
        let mut index = Index::new();
        index.add(0, 0).unwrap();
        index.add(1_048_576, 2_097_152).unwrap();
        let headerless_index = crate::io_support::rio::compression_index_storage_bytes(&index);
        assert!(
            !headerless_index.starts_with(&[0x50, 0x2A, 0x4D, 0x18]),
            "rio_v2 should store MinIO-style headerless compression indexes"
        );

        let object_info = ObjectInfo {
            size: 3_000_000,
            parts: Arc::new(vec![ObjectPartInfo {
                etag: String::new(),
                number: 1,
                size: 3_000_000,
                actual_size: 4_194_304,
                index: Some(headerless_index),
                ..Default::default()
            }]),
            user_defined: Arc::new(HashMap::from([
                ("x-minio-internal-compression".to_string(), "klauspost/compress/s2".to_string()),
                ("x-minio-internal-actual-size".to_string(), "4194304".to_string()),
            ])),
            ..Default::default()
        };

        let range = HTTPRangeSpec {
            is_suffix_length: false,
            start: 2_097_152,
            end: 2_097_161,
        };

        let decoded = crate::io_support::rio::decode_compression_index_bytes(object_info.parts[0].index.as_ref().unwrap())
            .expect("headerless MinIO-style compression index should decode");
        let (compressed_offset, uncompressed_offset) = decoded.find(range.start).expect("seek into decoded index");
        assert!(compressed_offset > 0);
        assert_eq!(uncompressed_offset, range.start);

        let (physical_offset, decompressed_skip, _, _, _) = get_compressed_offsets(&object_info, range.start);
        assert!(physical_offset > 0);
        assert_eq!(decompressed_skip, 0);

        let plan = ReadPlan::build(Some(range), &object_info, &ObjectOptions::default(), &HeaderMap::new())
            .await
            .expect("MinIO headerless compression index should be decoded");

        assert_eq!(plan.storage_offset as i64, physical_offset);
        assert_eq!(plan.storage_length, object_info.size - plan.storage_offset as i64);
        assert_eq!(plan.object_size, 10);
    }

    #[cfg(feature = "rio-v2")]
    #[test]
    fn test_get_compressed_offsets_aligns_encrypted_ranges_to_dare_packages() {
        let mut index = Index::new();
        index.add(0, 0).unwrap();
        index.add(200_000, 2_097_152).unwrap();
        let stored_index = crate::io_support::rio::compression_index_storage_bytes(&index);
        let expected_comp_off = crate::io_support::rio::decode_compression_index_bytes(&stored_index)
            .expect("decode stored index")
            .find(2_097_152)
            .expect("find offset in stored index")
            .0;

        let object_info = ObjectInfo {
            size: 400_000,
            parts: Arc::new(vec![ObjectPartInfo {
                etag: String::new(),
                number: 1,
                size: 400_000,
                actual_size: 4_194_304,
                index: Some(stored_index),
                ..Default::default()
            }]),
            user_defined: Arc::new(HashMap::from([
                ("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string()),
                ("x-amz-server-side-encryption-customer-original-size".to_string(), "4194304".to_string()),
                (
                    "x-minio-internal-compression".to_string(),
                    crate::io_support::rio::compression_metadata_value(CompressionAlgorithm::default()),
                ),
                ("x-minio-internal-actual-size".to_string(), "4194304".to_string()),
            ])),
            ..Default::default()
        };

        let (physical_offset, decompressed_skip, first_part_idx, decrypt_skip, seq_num) =
            get_compressed_offsets(&object_info, 2_097_152);

        assert_eq!(first_part_idx, 0);
        assert_eq!(physical_offset, (expected_comp_off / DARE_PAYLOAD_SIZE) * DARE_PACKAGE_SIZE);
        assert_eq!(decompressed_skip, 0);
        assert_eq!(decrypt_skip, expected_comp_off % DARE_PAYLOAD_SIZE);
        assert_eq!(seq_num, (expected_comp_off / DARE_PAYLOAD_SIZE) as u64);
    }

    #[tokio::test]
    async fn test_get_object_reader_decrypts_ssec_full_object() {
        let plaintext = b"ecstore-ssec-full-object".to_vec();
        let key_bytes = [0x31; 32];
        let bucket = "bucket";
        let object = "object";
        let nonce = md5_bytes(format!("{bucket}-{object}").as_bytes());
        let mut base_nonce = [0u8; 12];
        base_nonce.copy_from_slice(&nonce[..12]);

        let mut encrypted = Vec::new();
        rustfs_rio::EncryptReader::new(Cursor::new(plaintext.clone()), key_bytes, base_nonce)
            .read_to_end(&mut encrypted)
            .await
            .expect("encrypt object");

        let object_info = ObjectInfo {
            bucket: bucket.to_string(),
            name: object.to_string(),
            size: encrypted.len() as i64,
            user_defined: Arc::new(HashMap::from([
                ("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string()),
                (
                    "x-amz-server-side-encryption-customer-key-md5".to_string(),
                    BASE64_STANDARD.encode(md5_bytes(key_bytes)),
                ),
                (
                    "x-amz-server-side-encryption-customer-original-size".to_string(),
                    plaintext.len().to_string(),
                ),
            ])),
            ..Default::default()
        };

        let (mut reader, offset, length) = GetObjectReader::new(
            Box::new(Cursor::new(encrypted.clone())),
            None,
            &object_info,
            &ObjectOptions::default(),
            &ssec_headers_from_key(key_bytes),
        )
        .await
        .expect("ssec read should be supported");

        let mut actual = Vec::new();
        reader.read_to_end(&mut actual).await.expect("read decrypted ssec object");

        assert_eq!(offset, 0);
        assert_eq!(length, encrypted.len() as i64);
        assert_eq!(reader.object_info.size, plaintext.len() as i64);
        assert_eq!(actual, plaintext);
    }

    #[cfg(feature = "rio-v2")]
    #[tokio::test]
    async fn test_get_object_reader_decrypts_ssec_sealed_object_key_full_object() {
        let plaintext = b"ecstore-rio-v2-ssec-sealed-object-key".repeat(4096);
        let customer_key = [0x31; 32];
        let object_key = [0x67; 32];
        let bucket = "bucket";
        let object = "sealed-object";
        let mut encrypted = Vec::new();
        crate::io_support::rio::EncryptReader::new_with_object_key(Cursor::new(plaintext.clone()), object_key)
            .read_to_end(&mut encrypted)
            .await
            .expect("encrypt object with rio-v2 object key");

        let object_info = ObjectInfo {
            bucket: bucket.to_string(),
            name: object.to_string(),
            size: encrypted.len() as i64,
            user_defined: Arc::new(HashMap::from([
                (TEST_OBJECT_KEY_HEADER.to_string(), BASE64_STANDARD.encode(object_key)),
                ("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string()),
                (
                    "x-amz-server-side-encryption-customer-key-md5".to_string(),
                    BASE64_STANDARD.encode(md5_bytes(customer_key)),
                ),
                (
                    "x-amz-server-side-encryption-customer-original-size".to_string(),
                    plaintext.len().to_string(),
                ),
            ])),
            ..Default::default()
        };

        let (mut reader, offset, length) = GetObjectReader::new(
            Box::new(Cursor::new(encrypted.clone())),
            None,
            &object_info,
            &ObjectOptions::default(),
            &ssec_headers_from_key(customer_key),
        )
        .await
        .expect("rio-v2 ssec sealed-object-key read should be supported");

        let mut actual = Vec::new();
        reader
            .read_to_end(&mut actual)
            .await
            .expect("read decrypted rio-v2 ssec object");

        assert_eq!(offset, 0);
        assert_eq!(length, encrypted.len() as i64);
        assert_eq!(reader.object_info.size, plaintext.len() as i64);
        assert_eq!(actual, plaintext);
    }

    #[tokio::test]
    async fn test_get_object_reader_decrypts_ssec_range_on_plaintext_semantics() {
        let plaintext = b"0123456789abcdefghijklmnopqrstuvwxyz".to_vec();
        let key_bytes = [0x41; 32];
        let bucket = "bucket";
        let object = "range-object";
        let nonce = md5_bytes(format!("{bucket}-{object}").as_bytes());
        let mut base_nonce = [0u8; 12];
        base_nonce.copy_from_slice(&nonce[..12]);

        let mut encrypted = Vec::new();
        rustfs_rio::EncryptReader::new(Cursor::new(plaintext.clone()), key_bytes, base_nonce)
            .read_to_end(&mut encrypted)
            .await
            .expect("encrypt ranged object");

        let object_info = ObjectInfo {
            bucket: bucket.to_string(),
            name: object.to_string(),
            size: encrypted.len() as i64,
            user_defined: Arc::new(HashMap::from([
                ("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string()),
                (
                    "x-amz-server-side-encryption-customer-key-md5".to_string(),
                    BASE64_STANDARD.encode(md5_bytes(key_bytes)),
                ),
                (
                    "x-amz-server-side-encryption-customer-original-size".to_string(),
                    plaintext.len().to_string(),
                ),
            ])),
            ..Default::default()
        };
        let range = HTTPRangeSpec {
            is_suffix_length: false,
            start: 5,
            end: 11,
        };

        let (mut reader, offset, length) = GetObjectReader::new(
            Box::new(Cursor::new(encrypted.clone())),
            Some(range),
            &object_info,
            &ObjectOptions::default(),
            &ssec_headers_from_key(key_bytes),
        )
        .await
        .expect("ssec range read should be supported");

        let mut actual = Vec::new();
        reader.read_to_end(&mut actual).await.expect("read ranged decrypted object");

        assert_eq!(offset, 0);
        assert_eq!(length, encrypted.len() as i64);
        assert_eq!(reader.object_info.size, 7);
        assert_eq!(actual, b"56789ab");
    }

    /// Counts every byte pulled from the underlying ciphertext stream, standing in
    /// for the physical bytes the erasure layer would decode for this read.
    struct SpyReader<R> {
        inner: R,
        bytes_read: Arc<std::sync::atomic::AtomicU64>,
    }

    impl<R> SpyReader<R> {
        fn new(inner: R) -> (Self, Arc<std::sync::atomic::AtomicU64>) {
            let bytes_read = Arc::new(std::sync::atomic::AtomicU64::new(0));
            (
                Self {
                    inner,
                    bytes_read: bytes_read.clone(),
                },
                bytes_read,
            )
        }
    }

    impl<R: AsyncRead + Unpin + Send + Sync> AsyncRead for SpyReader<R> {
        fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            let before = buf.filled().len();
            let poll = Pin::new(&mut self.inner).poll_read(cx, buf);
            if let Poll::Ready(Ok(())) = &poll {
                let filled = (buf.filled().len() - before) as u64;
                self.bytes_read.fetch_add(filled, std::sync::atomic::Ordering::Relaxed);
            }
            poll
        }
    }

    struct LegacyMultipartFixture {
        plaintext: Vec<u8>,
        ciphertext: Vec<u8>,
        object_info: ObjectInfo,
        part_physical_sizes: Vec<usize>,
    }

    impl LegacyMultipartFixture {
        /// Physical ciphertext offset where the given zero-based part starts.
        fn physical_part_start(&self, part_index: usize) -> usize {
            self.part_physical_sizes[..part_index].iter().sum()
        }
    }

    const LEGACY_FIXTURE_BASE_NONCE: [u8; 12] = [0x5A; 12];

    fn legacy_fixture_part_plaintext(part_number: usize, size: usize) -> Vec<u8> {
        (0..size).map(|i| ((i * 31 + part_number * 7) % 251) as u8).collect()
    }

    /// Encrypts every part as its own rio v1 segment with the per-part nonce,
    /// exactly like `WritePlan::apply` does for multipart uploads, and records the
    /// physical/plaintext part sizes the way `PutObjectPart` persists them.
    async fn build_legacy_multipart_fixture(
        bucket: &str,
        object: &str,
        key_bytes: [u8; 32],
        part_plain_sizes: &[usize],
        mut user_defined: HashMap<String, String>,
    ) -> LegacyMultipartFixture {
        let mut plaintext = Vec::new();
        let mut ciphertext = Vec::new();
        let mut parts = Vec::new();
        let mut part_physical_sizes = Vec::new();

        for (part_index, &part_plain_size) in part_plain_sizes.iter().enumerate() {
            let part_number = part_index + 1;
            let part_plain = legacy_fixture_part_plaintext(part_number, part_plain_size);
            let mut part_cipher = Vec::new();
            rustfs_rio::EncryptReader::new_multipart(
                Cursor::new(part_plain.clone()),
                key_bytes,
                LEGACY_FIXTURE_BASE_NONCE,
                part_number,
            )
            .read_to_end(&mut part_cipher)
            .await
            .expect("encrypt multipart fixture part");

            parts.push(ObjectPartInfo {
                number: part_number,
                size: part_cipher.len(),
                actual_size: part_plain.len() as i64,
                ..Default::default()
            });
            part_physical_sizes.push(part_cipher.len());
            plaintext.extend_from_slice(&part_plain);
            ciphertext.extend_from_slice(&part_cipher);
        }
        let data_dir = Uuid::from_u128(1);
        rustfs_utils::http::insert_str(&mut user_defined, ENCRYPTED_PART_LAYOUT_QUORUM_SUFFIX, data_dir.to_string());

        let object_info = ObjectInfo {
            bucket: bucket.to_string(),
            name: object.to_string(),
            size: ciphertext.len() as i64,
            data_dir: Some(data_dir),
            etag: Some(format!("d41d8cd98f00b204e9800998ecf8427e-{}", parts.len())),
            parts: Arc::new(parts),
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };

        LegacyMultipartFixture {
            plaintext,
            ciphertext,
            object_info,
            part_physical_sizes,
        }
    }

    fn legacy_ssec_multipart_metadata(key_bytes: [u8; 32], total_plaintext: usize) -> HashMap<String, String> {
        HashMap::from([
            ("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string()),
            (
                "x-amz-server-side-encryption-customer-key-md5".to_string(),
                BASE64_STANDARD.encode(md5_bytes(key_bytes)),
            ),
            (
                "x-amz-server-side-encryption-customer-original-size".to_string(),
                total_plaintext.to_string(),
            ),
            (TEST_NONCE_HEADER.to_string(), BASE64_STANDARD.encode(LEGACY_FIXTURE_BASE_NONCE)),
        ])
    }

    async fn build_legacy_ssec_multipart_fixture(key_bytes: [u8; 32], part_plain_sizes: &[usize]) -> LegacyMultipartFixture {
        let total_plaintext: usize = part_plain_sizes.iter().sum();
        build_legacy_multipart_fixture(
            "bucket",
            "legacy-multipart",
            key_bytes,
            part_plain_sizes,
            legacy_ssec_multipart_metadata(key_bytes, total_plaintext),
        )
        .await
    }

    /// Serves exactly the ciphertext window the plan schedules — the contract the
    /// erasure layer honors for `(storage_offset, storage_length)` — then decodes
    /// the body end to end. Returns the body, the plan offsets, and the reported
    /// object size.
    async fn read_via_seek_window(
        fixture: &LegacyMultipartFixture,
        rs: Option<HTTPRangeSpec>,
        opts: &ObjectOptions,
        headers: &HeaderMap<HeaderValue>,
    ) -> (Vec<u8>, usize, i64, i64) {
        let plan = ReadPlan::build(rs.clone(), &fixture.object_info, opts, headers)
            .await
            .expect("plan should build for seek window read");
        let start = plan.storage_offset;
        let window_len = usize::try_from(plan.storage_length).expect("storage length fits usize");
        assert!(
            start + window_len <= fixture.ciphertext.len(),
            "plan window [{start}, {}) exceeds ciphertext length {}",
            start + window_len,
            fixture.ciphertext.len()
        );
        let window = fixture.ciphertext[start..start + window_len].to_vec();

        let (mut reader, offset, length) =
            GetObjectReader::new(Box::new(Cursor::new(window)), rs, &fixture.object_info, opts, headers)
                .await
                .expect("seek window read should build a reader");
        assert_eq!(offset, start, "reader offsets must match the probed plan");
        assert_eq!(length, plan.storage_length, "reader length must match the probed plan");

        let mut body = Vec::new();
        reader.read_to_end(&mut body).await.expect("decode seek window body");
        (body, offset, length, reader.object_info.size)
    }

    fn range(start: i64, end: i64) -> HTTPRangeSpec {
        HTTPRangeSpec {
            is_suffix_length: false,
            start,
            end,
        }
    }

    #[test]
    fn test_get_legacy_encrypted_offsets_math() {
        let parts = vec![
            ObjectPartInfo {
                number: 1,
                size: 1_000,
                actual_size: 800,
                ..Default::default()
            },
            ObjectPartInfo {
                number: 2,
                size: 700,
                actual_size: 500,
                ..Default::default()
            },
            ObjectPartInfo {
                number: 3,
                size: 400,
                actual_size: 300,
                ..Default::default()
            },
        ];
        let oi = ObjectInfo {
            size: 2_100,
            parts: Arc::new(parts),
            ..Default::default()
        };

        // Head range inside part 1 covers only part 1 but exposes the full tail plaintext.
        assert_eq!(get_legacy_encrypted_offsets(&oi, 0, 10), Some((0, 1_000, 0, 0, 1_600)));
        // Range ending exactly on the part 1 plaintext boundary stays within part 1.
        assert_eq!(get_legacy_encrypted_offsets(&oi, 700, 100), Some((0, 1_000, 700, 0, 1_600)));
        // One byte further crosses into part 2.
        assert_eq!(get_legacy_encrypted_offsets(&oi, 700, 101), Some((0, 1_700, 700, 0, 1_600)));
        // Range starting exactly at the part 2 boundary skips part 1 physically.
        assert_eq!(get_legacy_encrypted_offsets(&oi, 800, 10), Some((1_000, 700, 0, 1, 800)));
        // Last byte of the object covers only part 3.
        assert_eq!(get_legacy_encrypted_offsets(&oi, 1_599, 1), Some((1_700, 400, 299, 2, 300)));
        // Range spanning all parts covers the whole physical object.
        assert_eq!(get_legacy_encrypted_offsets(&oi, 10, 1_590), Some((0, 2_100, 10, 0, 1_600)));
        // Out-of-bounds offset yields no seek plan.
        assert_eq!(get_legacy_encrypted_offsets(&oi, 1_600, 1), None);
        assert_eq!(get_legacy_encrypted_offsets(&oi, 1_599, 2), None);
        assert_eq!(get_legacy_encrypted_offsets(&oi, -1, 1), None);
        assert_eq!(get_legacy_encrypted_offsets(&oi, 0, 0), None);
        assert_eq!(get_legacy_encrypted_offsets(&oi, i64::MAX, 2), None);
    }

    #[test]
    fn test_legacy_part_layout_validation_rejects_invalid_totals() {
        let assert_falls_back_to_object_size = |parts: Vec<ObjectPartInfo>| {
            let object_info = ObjectInfo {
                size: 123,
                parts: Arc::new(parts),
                ..Default::default()
            };
            assert_eq!(
                encrypted_plaintext_size(&object_info, true, false, None).expect("invalid legacy layout should fall back"),
                123
            );
        };
        let valid = vec![
            ObjectPartInfo {
                size: 7,
                actual_size: 5,
                ..Default::default()
            },
            ObjectPartInfo {
                size: 11,
                actual_size: 9,
                ..Default::default()
            },
        ];
        assert_eq!(legacy_part_layout_totals(&valid), Some((18, 14)));
        let object_info_without_recorded_size = ObjectInfo {
            size: 18,
            parts: Arc::new(valid.clone()),
            ..Default::default()
        };
        assert_eq!(
            encrypted_plaintext_size(&object_info_without_recorded_size, true, false, None).expect("legacy total should resolve"),
            14
        );
        assert_eq!(
            encrypted_plaintext_size(&object_info_without_recorded_size, true, false, Some(13))
                .expect("recorded total should win"),
            13
        );

        for actual_size in [0, -1] {
            let mut invalid = valid.clone();
            invalid[0].actual_size = actual_size;
            assert_eq!(legacy_part_layout_totals(&invalid), None);
            assert_falls_back_to_object_size(invalid);
        }
        assert_falls_back_to_object_size(Vec::new());

        let mut plaintext_overflow = valid.clone();
        plaintext_overflow[0].actual_size = i64::MAX;
        assert_eq!(legacy_part_layout_totals(&plaintext_overflow), None);
        assert_falls_back_to_object_size(plaintext_overflow.clone());
        let plaintext_overflow_info = ObjectInfo {
            parts: Arc::new(plaintext_overflow),
            ..Default::default()
        };
        assert_eq!(get_legacy_encrypted_offsets(&plaintext_overflow_info, 0, 1), None);

        let outer_plaintext_overflow = ObjectInfo {
            parts: Arc::new(vec![
                ObjectPartInfo {
                    size: 1,
                    actual_size: i64::MAX - 1,
                    ..Default::default()
                },
                ObjectPartInfo {
                    size: 1,
                    actual_size: 10,
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };
        assert_eq!(get_legacy_encrypted_offsets(&outer_plaintext_overflow, i64::MAX - 1, 1), None);

        #[cfg(target_pointer_width = "64")]
        {
            let mut physical_overflow = valid;
            physical_overflow[0].size = usize::try_from(i64::MAX).expect("i64::MAX fits usize on 64-bit targets");
            assert_eq!(legacy_part_layout_totals(&physical_overflow), None);
            let physical_overflow_info = ObjectInfo {
                parts: Arc::new(physical_overflow),
                ..Default::default()
            };
            assert_eq!(get_legacy_encrypted_offsets(&physical_overflow_info, 5, 1), None);

            let conversion_overflow_size = usize::try_from(i64::MAX).expect("i64::MAX fits usize on 64-bit targets") + 1;
            let conversion_overflow = vec![ObjectPartInfo {
                size: conversion_overflow_size,
                actual_size: 1,
                ..Default::default()
            }];
            assert_eq!(legacy_part_layout_totals(&conversion_overflow), None);
            let conversion_overflow_info = ObjectInfo {
                parts: Arc::new(conversion_overflow),
                ..Default::default()
            };
            assert_eq!(get_legacy_encrypted_offsets(&conversion_overflow_info, 0, 1), None);

            let outer_physical_overflow = ObjectInfo {
                parts: Arc::new(vec![
                    ObjectPartInfo {
                        size: usize::try_from(i64::MAX).expect("i64::MAX fits usize on 64-bit targets"),
                        actual_size: 1,
                        ..Default::default()
                    },
                    ObjectPartInfo {
                        size: 1,
                        actual_size: 1,
                        ..Default::default()
                    },
                    ObjectPartInfo {
                        size: 1,
                        actual_size: 1,
                        ..Default::default()
                    },
                ]),
                ..Default::default()
            };
            assert_eq!(get_legacy_encrypted_offsets(&outer_physical_overflow, 2, 1), None);
        }
    }

    #[tokio::test]
    async fn test_legacy_ssec_multipart_range_seek_byte_exact_matrix() {
        async_with_vars([(ENV_RUSTFS_ENCRYPTED_RANGE_SEEK, Some("true"))], async {
            let key_bytes = [0x71; 32];
            // Part plaintexts exercise multi-block (2 x 8192 + tail), block + tail,
            // and short single-block segments.
            let fixture = build_legacy_ssec_multipart_fixture(key_bytes, &[20_000, 9_000, 5_000]).await;
            let headers = ssec_headers_from_key(key_bytes);
            let opts = ObjectOptions::default();
            let total_plaintext = fixture.plaintext.len() as i64;
            let phys = &fixture.part_physical_sizes;

            // (range, expected physical offset, expected physical length)
            let cases: Vec<(HTTPRangeSpec, usize, i64, &str)> = vec![
                (range(0, 9), 0, phys[0] as i64, "head of part 1"),
                (range(20_100, 20_199), fixture.physical_part_start(1), phys[1] as i64, "inside part 2"),
                (range(33_900, 33_999), fixture.physical_part_start(2), phys[2] as i64, "tail of part 3"),
                (
                    HTTPRangeSpec {
                        is_suffix_length: true,
                        start: 100,
                        end: -1,
                    },
                    fixture.physical_part_start(2),
                    phys[2] as i64,
                    "suffix range",
                ),
                (range(19_999, 20_000), 0, (phys[0] + phys[1]) as i64, "part 1/2 boundary straddle"),
                (
                    range(8_191, 8_193),
                    0,
                    phys[0] as i64,
                    "8 KiB encrypted-block boundary straddle in part 1",
                ),
                (range(19_999, 19_999), 0, phys[0] as i64, "last byte of part 1"),
                (
                    range(20_000, 20_000),
                    fixture.physical_part_start(1),
                    phys[1] as i64,
                    "first byte of part 2",
                ),
                (range(100, 33_950), 0, fixture.ciphertext.len() as i64, "range spanning all parts"),
                (range(0, total_plaintext - 1), 0, fixture.ciphertext.len() as i64, "full range"),
                (
                    range(29_000, total_plaintext - 1),
                    fixture.physical_part_start(2),
                    phys[2] as i64,
                    "aligned suffix from part 3 start",
                ),
                (
                    range(20_000, 29_100),
                    fixture.physical_part_start(1),
                    (phys[1] + phys[2]) as i64,
                    "part 2 into part 3",
                ),
            ];

            for (rs, expected_offset, expected_length, label) in cases {
                let (start, len) = rs.get_offset_length(total_plaintext).expect("valid case range");
                let expected_body =
                    &fixture.plaintext[start..start + usize::try_from(len).expect("valid range length fits usize")];

                let (body, offset, length, reported_size) = read_via_seek_window(&fixture, Some(rs), &opts, &headers).await;

                assert_eq!(offset, expected_offset, "{label}: physical offset");
                assert_eq!(length, expected_length, "{label}: physical length");
                assert_eq!(reported_size, len, "{label}: reported plaintext size");
                assert_eq!(body.len(), expected_body.len(), "{label}: body length");
                assert_eq!(body, expected_body, "{label}: body bytes");
            }
        })
        .await;
    }

    /// The amplification-inversion guard: a small Range inside part 2 must schedule
    /// only part 2 and must not pull more than part 2's ciphertext. The stream is
    /// served from the planned offset all the way to the object end WITHOUT an end
    /// truncation, so the covering-part bound is a real measurement of what the
    /// decode chain pulls, not an artifact of window slicing. Reverting the Legacy
    /// plan to the conservative `(0, oi.size)` full read fails the offset/length
    /// assertions, and the pull bound as well: skipping the 20_100-byte plaintext
    /// prefix would force the chain to pull all of part 1 first.
    #[tokio::test]
    async fn test_legacy_ssec_multipart_mid_range_reads_only_covering_part() {
        async_with_vars([(ENV_RUSTFS_ENCRYPTED_RANGE_SEEK, Some("true"))], async {
            let key_bytes = [0x72; 32];
            let fixture = build_legacy_ssec_multipart_fixture(key_bytes, &[20_000, 9_000, 5_000]).await;
            let headers = ssec_headers_from_key(key_bytes);
            let opts = ObjectOptions::default();
            // 100 plaintext bytes inside part 2 (global plaintext [20_100, 20_200)).
            let rs = range(20_100, 20_199);

            let plan = ReadPlan::build(Some(rs.clone()), &fixture.object_info, &opts, &headers)
                .await
                .expect("plan should build for mid range");
            let covering_part_physical = fixture.part_physical_sizes[1] as u64;
            let object_physical = fixture.ciphertext.len() as u64;
            assert_eq!(
                plan.storage_offset,
                fixture.physical_part_start(1),
                "must seek to the covering part boundary"
            );
            assert_eq!(plan.storage_length as u64, covering_part_physical, "must schedule only the covering part");

            // Serve everything from the planned offset to the object end; the chain
            // itself decides how much to pull.
            let (spy, spy_bytes) = SpyReader::new(Cursor::new(fixture.ciphertext[plan.storage_offset..].to_vec()));
            let (mut reader, _, _) = GetObjectReader::new(Box::new(spy), Some(rs), &fixture.object_info, &opts, &headers)
                .await
                .expect("mid range read should build a reader");
            let mut body = Vec::new();
            reader.read_to_end(&mut body).await.expect("decode mid range body");
            let pulled = spy_bytes.load(std::sync::atomic::Ordering::Relaxed);

            assert_eq!(body, &fixture.plaintext[20_100..20_200], "mid range body must stay byte-exact");
            assert!(
                pulled <= covering_part_physical,
                "physical read {pulled} exceeds covering part {covering_part_physical}"
            );
            assert!(
                pulled < object_physical,
                "physical read {pulled} must stay below the whole object {object_physical}"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn test_legacy_ssec_multipart_part_number_get_seeks_to_part() {
        async_with_vars([(ENV_RUSTFS_ENCRYPTED_RANGE_SEEK, Some("true"))], async {
            let key_bytes = [0x73; 32];
            let fixture = build_legacy_ssec_multipart_fixture(key_bytes, &[20_000, 9_000, 5_000]).await;
            let headers = ssec_headers_from_key(key_bytes);
            let opts = ObjectOptions {
                part_number: Some(2),
                ..Default::default()
            };

            let (body, offset, length, reported_size) = read_via_seek_window(&fixture, None, &opts, &headers).await;

            assert_eq!(offset, fixture.physical_part_start(1), "partNumber GET must seek to part 2");
            assert_eq!(length, fixture.part_physical_sizes[1] as i64, "partNumber GET must cover only part 2");
            assert_eq!(reported_size, 9_000);
            assert_eq!(body, &fixture.plaintext[20_000..29_000], "partNumber body must stay byte-exact");
        })
        .await;
    }

    #[tokio::test]
    async fn test_legacy_single_part_multipart_object_keeps_full_read_shape() {
        async_with_vars([(ENV_RUSTFS_ENCRYPTED_RANGE_SEEK, Some("true"))], async {
            let key_bytes = [0x75; 32];
            let fixture = build_legacy_ssec_multipart_fixture(key_bytes, &[20_000]).await;
            let headers = ssec_headers_from_key(key_bytes);
            let opts = ObjectOptions::default();
            let rs = range(5_000, 5_099);

            let (body, offset, length, reported_size) = read_via_seek_window(&fixture, Some(rs), &opts, &headers).await;

            // A single-part object degenerates to the previous full-object plan.
            assert_eq!(offset, 0);
            assert_eq!(length, fixture.ciphertext.len() as i64);
            assert_eq!(reported_size, 100);
            assert_eq!(body, &fixture.plaintext[5_000..5_100]);
        })
        .await;
    }

    #[tokio::test]
    async fn test_legacy_range_seek_kill_switch_restores_full_read() {
        async_with_vars([(ENV_RUSTFS_ENCRYPTED_RANGE_SEEK, Some("false"))], async {
            let key_bytes = [0x76; 32];
            let fixture = build_legacy_ssec_multipart_fixture(key_bytes, &[20_000, 9_000, 5_000]).await;
            let headers = ssec_headers_from_key(key_bytes);
            let opts = ObjectOptions::default();
            let rs = range(33_900, 33_999);

            let (body, offset, length, reported_size) = read_via_seek_window(&fixture, Some(rs), &opts, &headers).await;

            assert_eq!(offset, 0, "kill switch must restore the conservative full read");
            assert_eq!(length, fixture.ciphertext.len() as i64);
            assert_eq!(reported_size, 100);
            assert_eq!(body, &fixture.plaintext[33_900..34_000], "kill switch path must stay byte-exact");
        })
        .await;
    }

    #[tokio::test]
    async fn test_legacy_range_seek_defaults_disabled() {
        async_with_vars([(ENV_RUSTFS_ENCRYPTED_RANGE_SEEK, None::<&str>)], async {
            let key_bytes = [0x7D; 32];
            let fixture = build_legacy_ssec_multipart_fixture(key_bytes, &[20_000, 9_000, 5_000]).await;

            let plan = ReadPlan::build(
                Some(range(33_900, 33_999)),
                &fixture.object_info,
                &ObjectOptions::default(),
                &ssec_headers_from_key(key_bytes),
            )
            .await
            .expect("default-disabled read plan should build");

            assert_eq!(plan.storage_offset, 0);
            assert_eq!(plan.storage_length, fixture.ciphertext.len() as i64);
        })
        .await;
    }

    #[tokio::test]
    async fn test_legacy_range_seek_marker_validation() {
        async_with_vars([(ENV_RUSTFS_ENCRYPTED_RANGE_SEEK, Some("true"))], async {
            let fixture = build_legacy_ssec_multipart_fixture([0x7F; 32], &[20_000, 9_000, 5_000]).await;
            let recorded_size = fixture
                .object_info
                .encryption_original_size()
                .expect("original size metadata should parse");
            assert!(!legacy_encrypted_seek_eligible(&fixture.object_info, true, false, 100, None));
            let layout_token = fixture
                .object_info
                .data_dir
                .expect("fixture data dir should exist")
                .to_string();
            let rustfs_key = format!("x-rustfs-internal-{ENCRYPTED_PART_LAYOUT_QUORUM_SUFFIX}");
            let minio_key = format!("x-minio-internal-{ENCRYPTED_PART_LAYOUT_QUORUM_SUFFIX}");
            let cases = [
                (
                    "wrong",
                    HashMap::from([
                        (rustfs_key.clone(), "wrong-token".to_string()),
                        (minio_key.clone(), "wrong-token".to_string()),
                    ]),
                    false,
                ),
                (
                    "empty",
                    HashMap::from([(rustfs_key.clone(), String::new()), (minio_key.clone(), String::new())]),
                    false,
                ),
                (
                    "conflicting",
                    HashMap::from([
                        (rustfs_key.clone(), layout_token.clone()),
                        (minio_key.clone(), "wrong-token".to_string()),
                    ]),
                    false,
                ),
                ("rustfs only", HashMap::from([(rustfs_key, layout_token.clone())]), true),
                ("minio only", HashMap::from([(minio_key, layout_token)]), true),
            ];

            for (case, marker_metadata, expected) in cases {
                let mut object_info = fixture.object_info.clone();
                let metadata = Arc::make_mut(&mut object_info.user_defined);
                rustfs_utils::http::remove_str(metadata, ENCRYPTED_PART_LAYOUT_QUORUM_SUFFIX);
                metadata.extend(marker_metadata);
                assert_eq!(
                    legacy_encrypted_seek_eligible(&object_info, true, false, 100, recorded_size),
                    expected,
                    "{case}"
                );
            }
        })
        .await;
    }

    #[tokio::test]
    async fn test_legacy_range_seek_keeps_full_read_without_quorum_marker() {
        async_with_vars([(ENV_RUSTFS_ENCRYPTED_RANGE_SEEK, Some("true"))], async {
            let key_bytes = [0x7E; 32];
            let mut fixture = build_legacy_ssec_multipart_fixture(key_bytes, &[20_000, 9_000, 5_000]).await;
            rustfs_utils::http::remove_str(
                Arc::make_mut(&mut fixture.object_info.user_defined),
                ENCRYPTED_PART_LAYOUT_QUORUM_SUFFIX,
            );

            let (body, offset, length, reported_size) = read_via_seek_window(
                &fixture,
                Some(range(33_900, 33_999)),
                &ObjectOptions::default(),
                &ssec_headers_from_key(key_bytes),
            )
            .await;

            assert_eq!(offset, 0);
            assert_eq!(length, fixture.ciphertext.len() as i64);
            assert_eq!(reported_size, 100);
            assert_eq!(body, &fixture.plaintext[33_900..34_000]);
        })
        .await;
    }

    #[tokio::test]
    async fn test_legacy_range_seek_rejects_plaintext_total_mismatch() {
        async_with_vars([(ENV_RUSTFS_ENCRYPTED_RANGE_SEEK, Some("true"))], async {
            let key_bytes = [0x7C; 32];
            let mut fixture = build_legacy_ssec_multipart_fixture(key_bytes, &[20_000, 9_000, 5_000]).await;
            let parts = Arc::make_mut(&mut fixture.object_info.parts);
            parts[0].actual_size = 19_000;

            let (body, offset, length, reported_size) = read_via_seek_window(
                &fixture,
                Some(range(33_900, 33_999)),
                &ObjectOptions::default(),
                &ssec_headers_from_key(key_bytes),
            )
            .await;

            assert_eq!(offset, 0, "a mismatched plaintext total must not control a physical seek");
            assert_eq!(length, fixture.ciphertext.len() as i64);
            assert_eq!(reported_size, 100);
            assert_eq!(body, &fixture.plaintext[33_900..34_000]);
        })
        .await;
    }

    #[tokio::test]
    async fn test_legacy_range_seek_gate_rejects_zero_actual_size_part() {
        async_with_vars([(ENV_RUSTFS_ENCRYPTED_RANGE_SEEK, Some("true"))], async {
            let key_bytes = [0x77; 32];
            let fixture = build_legacy_ssec_multipart_fixture(key_bytes, &[20_000, 9_000, 5_000]).await;
            let mut object_info = fixture.object_info.clone();
            {
                let parts = Arc::make_mut(&mut object_info.parts);
                parts[1].actual_size = 0;
            }
            let headers = ssec_headers_from_key(key_bytes);

            let plan = ReadPlan::build(Some(range(0, 9)), &object_info, &ObjectOptions::default(), &headers)
                .await
                .expect("plan should build for zero actual_size sample");

            assert_eq!(plan.storage_offset, 0, "a poisoned parts table must fall back to the full read");
            assert_eq!(plan.storage_length, object_info.size);
        })
        .await;
    }

    /// The physical part sizes must add up to `oi.size` for a seek to be safe;
    /// inconsistent metadata must fall back to the previous full-object read
    /// instead of scheduling an erasure read past the object end.
    #[tokio::test]
    async fn test_legacy_range_seek_gate_rejects_inconsistent_physical_sum() {
        async_with_vars([(ENV_RUSTFS_ENCRYPTED_RANGE_SEEK, Some("true"))], async {
            let key_bytes = [0x7B; 32];
            let fixture = build_legacy_ssec_multipart_fixture(key_bytes, &[20_000, 9_000, 5_000]).await;
            let mut object_info = fixture.object_info.clone();
            object_info.size -= 3;
            let headers = ssec_headers_from_key(key_bytes);

            let plan = ReadPlan::build(Some(range(33_900, 33_999)), &object_info, &ObjectOptions::default(), &headers)
                .await
                .expect("plan should build for inconsistent physical sum sample");

            assert_eq!(plan.storage_offset, 0, "an inconsistent physical sum must fall back to the full read");
            assert_eq!(plan.storage_length, object_info.size);
        })
        .await;
    }

    #[tokio::test]
    async fn test_legacy_range_seek_gate_rejects_compressed_encrypted_object() {
        async_with_vars([(ENV_RUSTFS_ENCRYPTED_RANGE_SEEK, Some("true"))], async {
            let key_bytes = [0x78; 32];
            let fixture = build_legacy_ssec_multipart_fixture(key_bytes, &[20_000, 9_000, 5_000]).await;
            let mut user_defined = fixture.object_info.user_defined.as_ref().clone();
            // "lz4" parses in both the default and the rio-v2 build (the MinIO
            // "klauspost/compress/s2" token is only recognized under rio-v2).
            user_defined.insert("x-minio-internal-compression".to_string(), "lz4".to_string());
            user_defined.insert("x-minio-internal-actual-size".to_string(), "34000".to_string());
            let mut object_info = fixture.object_info.clone();
            object_info.user_defined = Arc::new(user_defined);
            let headers = ssec_headers_from_key(key_bytes);

            let plan = ReadPlan::build(Some(range(33_900, 33_999)), &object_info, &ObjectOptions::default(), &headers)
                .await
                .expect("plan should build for compressed encrypted sample");

            assert_eq!(plan.storage_offset, 0, "compressed + encrypted must keep the conservative full read");
            assert_eq!(plan.storage_length, object_info.size);
        })
        .await;
    }

    #[tokio::test]
    async fn test_legacy_ssec_multipart_seek_tamper_fails_hard_with_no_plaintext() {
        async_with_vars([(ENV_RUSTFS_ENCRYPTED_RANGE_SEEK, Some("true"))], async {
            let key_bytes = [0x7A; 32];
            let fixture = build_legacy_ssec_multipart_fixture(key_bytes, &[20_000, 9_000, 5_000]).await;
            let headers = ssec_headers_from_key(key_bytes);
            let opts = ObjectOptions::default();
            let rs = range(33_900, 33_999);

            let plan = ReadPlan::build(Some(rs.clone()), &fixture.object_info, &opts, &headers)
                .await
                .expect("plan should build for tamper test");
            let start = plan.storage_offset;
            let window_len = usize::try_from(plan.storage_length).expect("tamper window length fits usize");
            assert_eq!(start, fixture.physical_part_start(2), "tamper test must exercise the seek path");

            let mut window = fixture.ciphertext[start..start + window_len].to_vec();
            // Flip one ciphertext byte inside the first encrypted block the seek
            // lands on (past the 8-byte header and the length prefix).
            window[64] ^= 0xFF;

            let (mut reader, _, _) =
                GetObjectReader::new(Box::new(Cursor::new(window)), Some(rs), &fixture.object_info, &opts, &headers)
                    .await
                    .expect("reader builds before the tampered block is decrypted");

            let mut body = Vec::new();
            let err = reader
                .read_to_end(&mut body)
                .await
                .expect_err("tampered ciphertext must fail the read");
            assert!(body.is_empty(), "no unauthenticated plaintext may be emitted, got {} bytes", body.len());
            let message = err.to_string();
            assert!(
                message.contains("decrypt") || message.contains("CRC32") || message.contains("Invalid encrypted block"),
                "unexpected tamper error: {message}"
            );
        })
        .await;
    }

    #[cfg(feature = "rio-v2")]
    #[tokio::test]
    async fn test_get_object_reader_uses_dare_package_offset_for_large_ssec_ranges() {
        const DARE_PACKAGE_SIZE: usize = 64 * 1024 + 32;

        let plaintext = vec![0x7Bu8; 2 * 64 * 1024 + 97];
        let customer_key = [0x61; 32];
        let object_key = [0x68; 32];
        let bucket = "bucket";
        let object = "large-range-object";
        let mut encrypted = Vec::new();
        crate::io_support::rio::EncryptReader::new_with_object_key(Cursor::new(plaintext.clone()), object_key)
            .read_to_end(&mut encrypted)
            .await
            .expect("encrypt large ranged object");

        let object_info = ObjectInfo {
            bucket: bucket.to_string(),
            name: object.to_string(),
            size: encrypted.len() as i64,
            user_defined: Arc::new(HashMap::from([
                (TEST_OBJECT_KEY_HEADER.to_string(), BASE64_STANDARD.encode(object_key)),
                ("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string()),
                (
                    "x-amz-server-side-encryption-customer-key-md5".to_string(),
                    BASE64_STANDARD.encode(md5_bytes(customer_key)),
                ),
                (
                    "x-amz-server-side-encryption-customer-original-size".to_string(),
                    plaintext.len().to_string(),
                ),
            ])),
            ..Default::default()
        };
        let range = HTTPRangeSpec {
            is_suffix_length: false,
            start: 70_000,
            end: 70_063,
        };

        let (mut reader, offset, _length) = GetObjectReader::new(
            Box::new(Cursor::new(encrypted[DARE_PACKAGE_SIZE..].to_vec())),
            Some(range),
            &object_info,
            &ObjectOptions::default(),
            &ssec_headers_from_key(customer_key),
        )
        .await
        .expect("large ssec range read should be supported");

        let mut actual = Vec::new();
        reader.read_to_end(&mut actual).await.expect("read ranged decrypted object");

        assert_eq!(
            offset, DARE_PACKAGE_SIZE,
            "rio_v2 encrypted ranges should start from the second DARE package"
        );
        assert_eq!(reader.object_info.size, 64);
        assert_eq!(actual, plaintext[70_000..70_064]);
    }

    #[tokio::test]
    async fn test_get_object_reader_decrypts_then_decompresses_before_applying_range() {
        let plaintext = b"abcdefghijklmnopqrstuvwxyz".to_vec();
        let key_bytes = [0x51; 32];
        let bucket = "bucket";
        let object = "compressed-object";
        let nonce = md5_bytes(format!("{bucket}-{object}").as_bytes());
        let mut base_nonce = [0u8; 12];
        base_nonce.copy_from_slice(&nonce[..12]);

        let mut compressed = Vec::new();
        rustfs_rio::CompressReader::new(Cursor::new(plaintext.clone()), CompressionAlgorithm::default())
            .read_to_end(&mut compressed)
            .await
            .expect("compress plaintext");

        let mut encrypted = Vec::new();
        rustfs_rio::EncryptReader::new(Cursor::new(compressed.clone()), key_bytes, base_nonce)
            .read_to_end(&mut encrypted)
            .await
            .expect("encrypt compressed plaintext");

        let object_info = ObjectInfo {
            bucket: bucket.to_string(),
            name: object.to_string(),
            size: encrypted.len() as i64,
            user_defined: Arc::new(HashMap::from([
                ("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string()),
                (
                    "x-amz-server-side-encryption-customer-key-md5".to_string(),
                    BASE64_STANDARD.encode(md5_bytes(key_bytes)),
                ),
                (
                    "x-amz-server-side-encryption-customer-original-size".to_string(),
                    plaintext.len().to_string(),
                ),
                ("x-minio-internal-compression".to_string(), CompressionAlgorithm::default().to_string()),
                ("x-minio-internal-actual-size".to_string(), plaintext.len().to_string()),
            ])),
            ..Default::default()
        };
        let range = HTTPRangeSpec {
            is_suffix_length: false,
            start: 5,
            end: 11,
        };

        let (mut reader, offset, length) = GetObjectReader::new(
            Box::new(Cursor::new(encrypted.clone())),
            Some(range),
            &object_info,
            &ObjectOptions::default(),
            &ssec_headers_from_key(key_bytes),
        )
        .await
        .expect("encrypted+compressed range read should be supported");

        let mut actual = Vec::new();
        reader
            .read_to_end(&mut actual)
            .await
            .expect("read ranged decompressed plaintext");

        assert_eq!(offset, 0);
        assert_eq!(length, encrypted.len() as i64);
        assert_eq!(reader.object_info.size, 7);
        assert_eq!(actual, b"fghijkl");
    }

    #[cfg(feature = "rio-v2")]
    #[tokio::test]
    async fn test_get_object_reader_uses_compression_index_for_encrypted_ranges() {
        use crate::io_support::rio::TryGetIndex;

        let plaintext: Vec<u8> = (0..(10 * 1024 * 1024 + 123_456))
            .map(|i| (((i as u64).wrapping_mul(1_103_515_245).wrapping_add(12_345) >> 16) & 0xFF) as u8)
            .collect();
        let customer_key = [0x73; 32];
        let object_key = [0x74; 32];
        let bucket = "bucket";
        let object = "compressed-large-object";
        let mut compressor = crate::io_support::rio::CompressReader::with_encrypted_padding(
            Cursor::new(plaintext.clone()),
            CompressionAlgorithm::default(),
        );
        let mut compressed = Vec::new();
        compressor
            .read_to_end(&mut compressed)
            .await
            .expect("compress large plaintext");

        let index = compressor
            .try_get_index()
            .cloned()
            .expect("large rio_v2 encrypted+compressed object should expose a compression index");
        let stored_index = crate::io_support::rio::compression_index_storage_bytes(&index);
        let decoded_index = crate::io_support::rio::decode_compression_index_bytes(&stored_index)
            .expect("decode stored encrypted compression index");

        let range = HTTPRangeSpec {
            is_suffix_length: false,
            start: 5 * 1024 * 1024,
            end: 5 * 1024 * 1024 + 63,
        };
        let original_offsets = index
            .find(range.start)
            .expect("find large-range compression block in original index");
        let (comp_off, uncomp_off) = decoded_index
            .find(range.start)
            .expect("find large-range compression block in stored index");
        assert_eq!(original_offsets, (comp_off, uncomp_off));
        assert!(comp_off > DARE_PAYLOAD_SIZE);
        assert!(uncomp_off <= range.start);

        let expected_storage_offset = ((comp_off / DARE_PAYLOAD_SIZE) * DARE_PACKAGE_SIZE) as usize;
        let expected_decrypt_skip = comp_off % DARE_PAYLOAD_SIZE;
        assert!(expected_storage_offset > 0);
        assert!(expected_decrypt_skip >= 0);

        let mut encrypted = Vec::new();
        crate::io_support::rio::EncryptReader::new_with_object_key(Cursor::new(compressed.clone()), object_key)
            .read_to_end(&mut encrypted)
            .await
            .expect("encrypt compressed plaintext");

        let expected_sequence_number = u32::try_from(comp_off / DARE_PAYLOAD_SIZE).expect("sequence number fits in u32");
        let chunk_offset = comp_off as usize;
        let chunk_type = compressed[chunk_offset];
        let chunk_len = (compressed[chunk_offset + 1] as usize)
            | ((compressed[chunk_offset + 2] as usize) << 8)
            | ((compressed[chunk_offset + 3] as usize) << 16);
        assert!(matches!(chunk_type, 0x00 | 0x01 | 0xff | 0xfe));
        assert!(chunk_offset + 4 + chunk_len <= compressed.len());

        let mut decrypted_tail = Vec::new();
        crate::io_support::rio::DecryptReader::new_with_object_key_and_sequence(
            Cursor::new(encrypted[expected_storage_offset..].to_vec()),
            object_key,
            expected_sequence_number,
        )
        .read_to_end(&mut decrypted_tail)
        .await
        .expect("decrypt package-aligned ciphertext tail");
        assert_eq!(
            &decrypted_tail[expected_decrypt_skip as usize..],
            &compressed[comp_off as usize..],
            "package-aligned decryption plus decrypt_skip must land on the indexed S2 chunk boundary"
        );

        let mut direct_reader = crate::io_support::rio::DecompressReader::new(
            Cursor::new(decrypted_tail[expected_decrypt_skip as usize..].to_vec()),
            CompressionAlgorithm::default(),
        );
        let mut direct_plaintext = Vec::new();
        direct_reader
            .read_to_end(&mut direct_plaintext)
            .await
            .expect("decompress tail starting at indexed S2 chunk boundary");
        assert_eq!(direct_plaintext, plaintext[uncomp_off as usize..]);

        let object_info = ObjectInfo {
            bucket: bucket.to_string(),
            name: object.to_string(),
            size: encrypted.len() as i64,
            parts: Arc::new(vec![ObjectPartInfo {
                etag: String::new(),
                number: 1,
                size: encrypted.len(),
                actual_size: plaintext.len() as i64,
                index: Some(stored_index),
                ..Default::default()
            }]),
            user_defined: Arc::new(HashMap::from([
                (TEST_OBJECT_KEY_HEADER.to_string(), BASE64_STANDARD.encode(object_key)),
                ("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string()),
                (
                    "x-amz-server-side-encryption-customer-key-md5".to_string(),
                    BASE64_STANDARD.encode(md5_bytes(customer_key)),
                ),
                (
                    "x-amz-server-side-encryption-customer-original-size".to_string(),
                    plaintext.len().to_string(),
                ),
                (
                    "x-minio-internal-compression".to_string(),
                    crate::io_support::rio::compression_metadata_value(CompressionAlgorithm::default()),
                ),
                ("x-minio-internal-actual-size".to_string(), plaintext.len().to_string()),
            ])),
            ..Default::default()
        };

        let plan = ReadPlan::build(
            Some(range.clone()),
            &object_info,
            &ObjectOptions::default(),
            &ssec_headers_from_key(customer_key),
        )
        .await
        .expect("build encrypted+compressed read plan");
        assert_eq!(plan.storage_offset, expected_storage_offset);
        assert_eq!(plan.storage_length, encrypted.len() as i64 - expected_storage_offset as i64);
        assert_eq!(plan.object_size, 64);
        match plan.transform {
            ReadTransform::Encrypted {
                sequence_number,
                decrypt_skip,
                plaintext_offset,
                plaintext_length,
                ..
            } => {
                assert_eq!(sequence_number, expected_sequence_number);
                assert_eq!(decrypt_skip, expected_decrypt_skip as usize);
                assert_eq!(plaintext_offset as i64, range.start - uncomp_off);
                assert_eq!(plaintext_length, 64);
            }
            _ => panic!("expected encrypted read plan"),
        }

        let (mut reader, offset, length) = GetObjectReader::new(
            Box::new(Cursor::new(encrypted[expected_storage_offset..].to_vec())),
            Some(range.clone()),
            &object_info,
            &ObjectOptions::default(),
            &ssec_headers_from_key(customer_key),
        )
        .await
        .expect("encrypted+compressed indexed range read should be supported");

        let mut actual = Vec::new();
        reader
            .read_to_end(&mut actual)
            .await
            .expect("read indexed encrypted+compressed range");

        assert_eq!(offset, expected_storage_offset);
        assert_eq!(length, encrypted.len() as i64 - expected_storage_offset as i64);
        assert_eq!(reader.object_info.size, 64);
        assert_eq!(actual, plaintext[range.start as usize..range.start as usize + 64]);
    }
}
