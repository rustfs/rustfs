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

use bytes::Bytes;
use rustfs_rio::Index;
use serde::Deserialize;
use std::io;

const S2_INDEX_HEADER: &[u8] = b"s2idx\x00";
const S2_INDEX_TRAILER: &[u8] = b"\x00xdi2s";
const CHUNK_TYPE_INDEX: u8 = 0x50;
const SKIPPABLE_FRAME_HEADER: usize = 4;
const MAX_INDEX_ENTRIES: usize = 1 << 16;

#[derive(Debug, Deserialize)]
struct LegacyIndexJson {
    total_uncompressed: i64,
    total_compressed: i64,
    offsets: Vec<LegacyIndexOffset>,
    est_block_uncompressed: i64,
}

#[derive(Debug, Deserialize)]
struct LegacyIndexOffset {
    compressed: i64,
    uncompressed: i64,
}

#[derive(Debug, Clone)]
struct S2IndexInfo {
    compressed_offset: i64,
    uncompressed_offset: i64,
}

#[derive(Debug, Clone)]
struct S2Index {
    total_uncompressed: i64,
    total_compressed: i64,
    est_block_uncompressed: i64,
    info: Vec<S2IndexInfo>,
}

pub fn minio_index_storage_bytes(index: &Index) -> Bytes {
    let decoded = legacy_index_to_s2_index(index).unwrap_or_else(|_| S2Index {
        total_uncompressed: index.total_uncompressed,
        total_compressed: index.total_compressed,
        est_block_uncompressed: 0,
        info: Vec::new(),
    });

    let encoded = decoded.into_full_bytes();
    remove_index_headers(encoded.as_ref())
        .map(Bytes::copy_from_slice)
        .unwrap_or(encoded)
}

pub fn decode_minio_index_bytes(bytes: &Bytes) -> Option<Index> {
    let decoded = S2Index::load(bytes.as_ref())
        .or_else(|_| S2Index::load(&restore_index_headers(bytes.as_ref())))
        .ok()?;

    let mut index = Index::new();
    for info in decoded.info {
        index.add(info.compressed_offset, info.uncompressed_offset).ok()?;
    }
    index.total_uncompressed = decoded.total_uncompressed;
    index.total_compressed = decoded.total_compressed;
    Some(index)
}

fn legacy_index_to_s2_index(index: &Index) -> io::Result<S2Index> {
    let json = index
        .to_json()
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    let decoded: LegacyIndexJson =
        serde_json::from_slice(&json).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;

    Ok(S2Index {
        total_uncompressed: decoded.total_uncompressed,
        total_compressed: decoded.total_compressed,
        est_block_uncompressed: decoded.est_block_uncompressed,
        info: decoded
            .offsets
            .into_iter()
            .map(|offset| S2IndexInfo {
                compressed_offset: offset.compressed,
                uncompressed_offset: offset.uncompressed,
            })
            .collect(),
    })
}

impl S2Index {
    fn into_full_bytes(self) -> Bytes {
        let mut out = Vec::new();
        out.extend_from_slice(&[CHUNK_TYPE_INDEX, 0, 0, 0]);
        out.extend_from_slice(S2_INDEX_HEADER);

        write_varint(&mut out, self.total_uncompressed);
        write_varint(&mut out, self.total_compressed);
        write_varint(&mut out, self.est_block_uncompressed);
        write_varint(&mut out, self.info.len() as i64);

        let has_uncompressed = self.has_explicit_uncompressed_offsets();
        out.push(u8::from(has_uncompressed));

        if has_uncompressed {
            for (idx, info) in self.info.iter().enumerate() {
                let mut offset = info.uncompressed_offset;
                if idx > 0 {
                    let prev = &self.info[idx - 1];
                    offset -= prev.uncompressed_offset + self.est_block_uncompressed;
                }
                write_varint(&mut out, offset);
            }
        }

        let mut compressed_predict = self.est_block_uncompressed / 2;
        for (idx, info) in self.info.iter().enumerate() {
            let mut offset = info.compressed_offset;
            if idx > 0 {
                let prev = &self.info[idx - 1];
                offset -= prev.compressed_offset + compressed_predict;
                compressed_predict += offset / 2;
            }
            write_varint(&mut out, offset);
        }

        let mut total_size = [0u8; 4];
        total_size.copy_from_slice(&((out.len() + 4 + S2_INDEX_TRAILER.len()) as u32).to_le_bytes());
        out.extend_from_slice(&total_size);
        out.extend_from_slice(S2_INDEX_TRAILER);

        let chunk_len = out.len() - SKIPPABLE_FRAME_HEADER;
        out[1] = chunk_len as u8;
        out[2] = (chunk_len >> 8) as u8;
        out[3] = (chunk_len >> 16) as u8;

        Bytes::from(out)
    }

    fn has_explicit_uncompressed_offsets(&self) -> bool {
        for (idx, info) in self.info.iter().enumerate() {
            if idx == 0 {
                if info.uncompressed_offset != 0 {
                    return true;
                }
                continue;
            }
            if info.uncompressed_offset != self.info[idx - 1].uncompressed_offset + self.est_block_uncompressed {
                return true;
            }
        }
        false
    }

    fn load(mut bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() <= SKIPPABLE_FRAME_HEADER + S2_INDEX_HEADER.len() + S2_INDEX_TRAILER.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "buffer too small"));
        }
        if bytes[0] != CHUNK_TYPE_INDEX {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid index chunk type"));
        }

        let chunk_len = (bytes[1] as usize) | ((bytes[2] as usize) << 8) | ((bytes[3] as usize) << 16);
        bytes = &bytes[SKIPPABLE_FRAME_HEADER..];
        if bytes.len() < chunk_len {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "buffer too small"));
        }
        bytes = &bytes[..chunk_len];

        if !bytes.starts_with(S2_INDEX_HEADER) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid index header"));
        }
        bytes = &bytes[S2_INDEX_HEADER.len()..];

        let (total_uncompressed, used) = read_varint(bytes)?;
        if total_uncompressed < 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid uncompressed size"));
        }
        bytes = &bytes[used..];

        let (total_compressed, used) = read_varint(bytes)?;
        bytes = &bytes[used..];

        let (est_block_uncompressed, used) = read_varint(bytes)?;
        if est_block_uncompressed < 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid block size"));
        }
        bytes = &bytes[used..];

        let (entries, used) = read_varint(bytes)?;
        if entries < 0 || entries > MAX_INDEX_ENTRIES as i64 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid number of entries"));
        }
        bytes = &bytes[used..];

        if bytes.is_empty() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "buffer too small"));
        }

        let has_uncompressed = bytes[0];
        if has_uncompressed & 1 != has_uncompressed {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid uncompressed flag"));
        }
        bytes = &bytes[1..];

        let mut info = vec![
            S2IndexInfo {
                compressed_offset: 0,
                uncompressed_offset: 0,
            };
            entries as usize
        ];

        for idx in 0..info.len() {
            let mut uncompressed_offset = 0_i64;
            if has_uncompressed != 0 {
                let (value, used) = read_varint(bytes)?;
                uncompressed_offset = value;
                bytes = &bytes[used..];
            }

            if idx > 0 {
                let prev = info[idx - 1].uncompressed_offset;
                uncompressed_offset += prev + est_block_uncompressed;
                if uncompressed_offset <= prev {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid uncompressed offset"));
                }
            }
            if uncompressed_offset < 0 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "negative uncompressed offset"));
            }
            info[idx].uncompressed_offset = uncompressed_offset;
        }

        let mut compressed_predict = est_block_uncompressed / 2;
        for idx in 0..info.len() {
            let (mut compressed_offset, used) = read_varint(bytes)?;
            bytes = &bytes[used..];

            if idx > 0 {
                let next_compressed_predict = compressed_predict + compressed_offset / 2;
                let prev = info[idx - 1].compressed_offset;
                compressed_offset += prev + compressed_predict;
                if compressed_offset <= prev {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid compressed offset"));
                }
                compressed_predict = next_compressed_predict;
            }
            if compressed_offset < 0 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "negative compressed offset"));
            }
            info[idx].compressed_offset = compressed_offset;
        }

        if bytes.len() < 4 + S2_INDEX_TRAILER.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "buffer too small"));
        }
        bytes = &bytes[4..];
        if !bytes.starts_with(S2_INDEX_TRAILER) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid index trailer"));
        }

        Ok(Self {
            total_uncompressed,
            total_compressed,
            est_block_uncompressed,
            info,
        })
    }
}

fn remove_index_headers(bytes: &[u8]) -> Option<&[u8]> {
    let save = SKIPPABLE_FRAME_HEADER + S2_INDEX_HEADER.len() + S2_INDEX_TRAILER.len() + 4;
    if bytes.len() <= save || bytes[0] != CHUNK_TYPE_INDEX {
        return None;
    }

    let chunk_len = (bytes[1] as usize) | ((bytes[2] as usize) << 8) | ((bytes[3] as usize) << 16);
    let bytes = &bytes[SKIPPABLE_FRAME_HEADER..];
    if bytes.len() < chunk_len {
        return None;
    }
    let bytes = &bytes[..chunk_len];

    let bytes = bytes.strip_prefix(S2_INDEX_HEADER)?;
    let bytes = bytes.strip_suffix(S2_INDEX_TRAILER)?;
    if bytes.len() < 4 {
        return None;
    }
    Some(&bytes[..bytes.len() - 4])
}

fn restore_index_headers(input: &[u8]) -> Vec<u8> {
    if input.is_empty() {
        return Vec::new();
    }

    let mut bytes = Vec::with_capacity(SKIPPABLE_FRAME_HEADER + S2_INDEX_HEADER.len() + input.len() + 4 + S2_INDEX_TRAILER.len());
    bytes.extend_from_slice(&[CHUNK_TYPE_INDEX, 0, 0, 0]);
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

fn write_varint(out: &mut Vec<u8>, value: i64) {
    let mut unsigned = ((value as u64) << 1) ^ ((value >> 63) as u64);
    while unsigned >= 0x80 {
        out.push((unsigned as u8) | 0x80);
        unsigned >>= 7;
    }
    out.push(unsigned as u8);
}

fn read_varint(bytes: &[u8]) -> io::Result<(i64, usize)> {
    let (unsigned, used) = read_uvarint(bytes)?;
    let value = ((unsigned >> 1) as i64) ^ (-((unsigned & 1) as i64));
    Ok((value, used))
}

fn read_uvarint(bytes: &[u8]) -> io::Result<(u64, usize)> {
    let mut value = 0_u64;
    let mut shift = 0_u32;

    for (idx, byte) in bytes.iter().copied().enumerate() {
        if byte < 0x80 {
            if idx > 9 || (idx == 9 && byte > 1) {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "varint overflow"));
            }
            return Ok((value | ((byte as u64) << shift), idx + 1));
        }

        value |= ((byte & 0x7f) as u64) << shift;
        shift += 7;
    }

    Err(io::Error::new(io::ErrorKind::UnexpectedEof, "unexpected EOF while reading varint"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signed_varint_matches_go_binary_varint_examples() {
        let cases = [
            (0, vec![0x00]),
            (-1, vec![0x01]),
            (1, vec![0x02]),
            (-2, vec![0x03]),
            (64, vec![0x80, 0x01]),
            (-64, vec![0x7f]),
        ];

        for (value, expected) in cases {
            let mut encoded = Vec::new();
            write_varint(&mut encoded, value);
            assert_eq!(encoded, expected);
            assert_eq!(read_varint(&encoded).unwrap(), (value, encoded.len()));
        }
    }

    #[test]
    fn minio_storage_bytes_round_trip_through_headerless_form() {
        let mut source = Index::new();
        source.add(0, 0).unwrap();
        source.add(1_048_576, 2_097_152).unwrap();
        source.total_uncompressed = 4_194_304;
        source.total_compressed = 3_000_000;

        let stored = minio_index_storage_bytes(&source);
        assert!(!stored.starts_with(&[CHUNK_TYPE_INDEX, 0x2a, 0x4d, 0x18]));

        let decoded = decode_minio_index_bytes(&stored).expect("decode headerless MinIO index");
        assert_eq!(decoded.total_uncompressed, source.total_uncompressed);
        assert_eq!(decoded.total_compressed, source.total_compressed);
        assert_eq!(decoded.find(2_097_152).unwrap(), (1_048_576, 2_097_152));
    }

    #[test]
    fn minio_index_allows_unknown_total_compressed_size() {
        let index = S2Index {
            total_uncompressed: 4_194_304,
            total_compressed: -1,
            est_block_uncompressed: 0,
            info: vec![
                S2IndexInfo {
                    compressed_offset: 0,
                    uncompressed_offset: 0,
                },
                S2IndexInfo {
                    compressed_offset: 1_048_576,
                    uncompressed_offset: 2_097_152,
                },
            ],
        };
        let full = index.into_full_bytes();
        let headerless = Bytes::copy_from_slice(remove_index_headers(full.as_ref()).expect("strip index headers"));

        let decoded = decode_minio_index_bytes(&headerless).expect("decode index with unknown compressed total");
        assert_eq!(decoded.total_uncompressed, 4_194_304);
        assert_eq!(decoded.total_compressed, -1);
        assert_eq!(decoded.find(2_097_152).unwrap(), (1_048_576, 2_097_152));
    }
}
