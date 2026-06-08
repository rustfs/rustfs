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

use rustfs_filemeta::{FileInfoOpts, get_file_info};
use std::{env, fs, path::PathBuf};

const S2_INDEX_HEADER: &[u8] = b"s2idx\x00";
const S2_INDEX_TRAILER: &[u8] = b"\x00xdi2s";
const CHUNK_TYPE_LEGACY_INDEX: u8 = 0x50;
const CHUNK_TYPE_MINIO_INDEX: u8 = 0x99;
const SKIPPABLE_FRAME_HEADER: usize = 4;
const MAX_INDEX_ENTRIES: i64 = 1 << 16;

#[derive(Debug)]
struct CompressionIndex {
    format: &'static str,
    storage: &'static str,
    total_uncompressed: i64,
    total_compressed: i64,
    est_block_uncompressed: i64,
    offsets: Vec<CompressionIndexOffset>,
}

#[derive(Debug)]
struct CompressionIndexOffset {
    compressed: i64,
    uncompressed: i64,
}

fn main() {
    let path = env::args()
        .nth(1)
        .map(PathBuf::from)
        .expect("usage: dump_fileinfo <xl.meta path>");
    let data = fs::read(&path).expect("read xl.meta");
    let fi = get_file_info(
        &data,
        "debug-bucket",
        "debug-object",
        "",
        FileInfoOpts {
            data: false,
            include_free_versions: true,
        },
    )
    .expect("decode file info");
    println!("path: {}", path.display());
    println!("size: {}", fi.size);
    println!("etag: {:?}", fi.get_etag());
    println!("parts: {}", fi.parts.len());
    for (idx, part) in fi.parts.iter().enumerate() {
        println!(
            "part#{idx}: number={} size={} actual_size={} etag={}",
            part.number, part.size, part.actual_size, part.etag
        );
        match part.index.as_deref() {
            Some(index_bytes) => print_compression_index(idx, index_bytes),
            None => println!("part#{idx}.index: none"),
        }
    }
    // Tier / transition fields
    if !fi.transition_status.is_empty() {
        println!("transition_status: {}", fi.transition_status);
        println!("transition_tier:   {}", fi.transition_tier);
        println!("transitioned_obj:  {}", fi.transitioned_objname);
        println!(
            "transition_ver_id: {}",
            fi.transition_version_id
                .map(|u| u.to_string())
                .unwrap_or_else(|| "<none>".into())
        );
    }

    println!("metadata entries: {}", fi.metadata.len());
    let mut keys = fi.metadata.keys().cloned().collect::<Vec<_>>();
    keys.sort();
    for key in keys {
        println!("meta[{key}]={}", fi.metadata.get(&key).unwrap());
    }
}

fn print_compression_index(part_idx: usize, bytes: &[u8]) {
    match decode_compression_index(bytes) {
        Ok(index) => {
            println!(
                "part#{part_idx}.index: bytes={} format={} storage={} entries={} total_uncompressed={} total_compressed={} est_block_uncompressed={}",
                bytes.len(),
                index.format,
                index.storage,
                index.offsets.len(),
                index.total_uncompressed,
                index.total_compressed,
                index.est_block_uncompressed
            );
            for (idx, offset) in index.offsets.iter().take(5).enumerate() {
                println!(
                    "part#{part_idx}.index.offset#{idx}: compressed={} uncompressed={}",
                    offset.compressed, offset.uncompressed
                );
            }
            if index.offsets.len() > 5
                && let Some(offset) = index.offsets.last()
            {
                println!(
                    "part#{part_idx}.index.offset#last: compressed={} uncompressed={}",
                    offset.compressed, offset.uncompressed
                );
            }
        }
        Err(err) => {
            println!("part#{part_idx}.index: bytes={} decode_error={err}", bytes.len());
        }
    }
}

fn decode_compression_index(bytes: &[u8]) -> Result<CompressionIndex, String> {
    if bytes.is_empty() {
        return Err("empty index".to_string());
    }

    let mut errors = Vec::new();
    for candidate in index_candidates(bytes) {
        match parse_index(&candidate.bytes, candidate.format, candidate.storage, candidate.signed_varint) {
            Ok(index) => return Ok(index),
            Err(err) => errors.push(format!("{} {}: {err}", candidate.format, candidate.storage)),
        }
    }

    Err(errors.join("; "))
}

struct IndexCandidate {
    bytes: Vec<u8>,
    format: &'static str,
    storage: &'static str,
    signed_varint: bool,
}

fn index_candidates(bytes: &[u8]) -> Vec<IndexCandidate> {
    let mut candidates = Vec::new();
    match bytes[0] {
        CHUNK_TYPE_MINIO_INDEX => candidates.push(IndexCandidate {
            bytes: bytes.to_vec(),
            format: "minio-s2",
            storage: "full",
            signed_varint: true,
        }),
        CHUNK_TYPE_LEGACY_INDEX => candidates.push(IndexCandidate {
            bytes: bytes.to_vec(),
            format: "legacy-rustfs",
            storage: "full",
            signed_varint: false,
        }),
        _ => {
            candidates.push(IndexCandidate {
                bytes: restore_index_headers(bytes, CHUNK_TYPE_MINIO_INDEX),
                format: "minio-s2",
                storage: "headerless",
                signed_varint: true,
            });
            candidates.push(IndexCandidate {
                bytes: restore_index_headers(bytes, CHUNK_TYPE_LEGACY_INDEX),
                format: "legacy-rustfs",
                storage: "headerless",
                signed_varint: false,
            });
        }
    }
    candidates
}

fn parse_index(
    bytes: &[u8],
    format: &'static str,
    storage: &'static str,
    signed_varint: bool,
) -> Result<CompressionIndex, String> {
    if bytes.len() <= SKIPPABLE_FRAME_HEADER + S2_INDEX_HEADER.len() + S2_INDEX_TRAILER.len() {
        return Err("buffer too small".to_string());
    }
    if bytes[0] != CHUNK_TYPE_MINIO_INDEX && bytes[0] != CHUNK_TYPE_LEGACY_INDEX {
        return Err(format!("invalid chunk type 0x{:02x}", bytes[0]));
    }

    let chunk_len = bytes[1] as usize | ((bytes[2] as usize) << 8) | ((bytes[3] as usize) << 16);
    let chunk = bytes
        .get(SKIPPABLE_FRAME_HEADER..SKIPPABLE_FRAME_HEADER + chunk_len)
        .ok_or_else(|| "chunk length exceeds buffer".to_string())?;
    let mut body = chunk
        .strip_prefix(S2_INDEX_HEADER)
        .ok_or_else(|| "invalid index header".to_string())?;

    let (total_uncompressed, used) = read_varint(body, signed_varint)?;
    body = &body[used..];
    let (total_compressed, used) = read_varint(body, signed_varint)?;
    body = &body[used..];
    let (est_block_uncompressed, used) = read_varint(body, signed_varint)?;
    body = &body[used..];
    let (entries, used) = read_varint(body, signed_varint)?;
    body = &body[used..];

    if total_uncompressed < 0 || est_block_uncompressed < 0 || !(0..=MAX_INDEX_ENTRIES).contains(&entries) {
        return Err("invalid index totals".to_string());
    }

    let has_uncompressed = *body.first().ok_or_else(|| "missing uncompressed-offset flag".to_string())?;
    if has_uncompressed & 1 != has_uncompressed {
        return Err("invalid uncompressed-offset flag".to_string());
    }
    body = &body[1..];

    let mut offsets = (0..entries)
        .map(|_| CompressionIndexOffset {
            compressed: 0,
            uncompressed: 0,
        })
        .collect::<Vec<_>>();

    for idx in 0..offsets.len() {
        let mut uncompressed = 0;
        if has_uncompressed != 0 {
            let (value, used) = read_varint(body, signed_varint)?;
            uncompressed = value;
            body = &body[used..];
        }
        if idx > 0 {
            let prev = offsets[idx - 1].uncompressed;
            uncompressed += prev + est_block_uncompressed;
        }
        offsets[idx].uncompressed = uncompressed;
    }

    let mut compressed_predict = est_block_uncompressed / 2;
    for idx in 0..offsets.len() {
        let (mut compressed, used) = read_varint(body, signed_varint)?;
        body = &body[used..];
        if idx > 0 {
            let next_compressed_predict = compressed_predict + compressed / 2;
            compressed += offsets[idx - 1].compressed + compressed_predict;
            compressed_predict = next_compressed_predict;
        }
        offsets[idx].compressed = compressed;
    }

    if body.len() < 4 + S2_INDEX_TRAILER.len() {
        return Err("missing index trailer".to_string());
    }
    body = &body[4..];
    if !body.starts_with(S2_INDEX_TRAILER) {
        return Err("invalid index trailer".to_string());
    }

    Ok(CompressionIndex {
        format,
        storage,
        total_uncompressed,
        total_compressed,
        est_block_uncompressed,
        offsets,
    })
}

fn restore_index_headers(input: &[u8], chunk_type: u8) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(SKIPPABLE_FRAME_HEADER + S2_INDEX_HEADER.len() + input.len() + 4 + S2_INDEX_TRAILER.len());
    bytes.extend_from_slice(&[chunk_type, 0, 0, 0]);
    bytes.extend_from_slice(S2_INDEX_HEADER);
    bytes.extend_from_slice(input);
    bytes.extend_from_slice(&((bytes.len() + 4 + S2_INDEX_TRAILER.len()) as u32).to_le_bytes());
    bytes.extend_from_slice(S2_INDEX_TRAILER);

    let chunk_len = bytes.len() - SKIPPABLE_FRAME_HEADER;
    bytes[1] = chunk_len as u8;
    bytes[2] = (chunk_len >> 8) as u8;
    bytes[3] = (chunk_len >> 16) as u8;
    bytes
}

fn read_varint(bytes: &[u8], signed: bool) -> Result<(i64, usize), String> {
    let mut value = 0_u64;
    let mut shift = 0_u32;

    for (idx, byte) in bytes.iter().copied().enumerate() {
        value |= u64::from(byte & 0x7f) << shift;
        if byte < 0x80 {
            let value = if signed {
                ((value >> 1) as i64) ^ (-((value & 1) as i64))
            } else {
                value as i64
            };
            return Ok((value, idx + 1));
        }
        shift += 7;
        if shift >= 64 {
            return Err("varint overflow".to_string());
        }
    }

    Err("unexpected EOF while reading varint".to_string())
}
