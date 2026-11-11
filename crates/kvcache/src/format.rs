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

//! On-disk layout. All integers are little endian.
//!
//! Entry, at a block-aligned offset:
//!
//! - header block (`BLOCK_SIZE` bytes): magic `KVCE`, format version (u16),
//!   key length (u16), flags (u16; bit 0 marks a tombstone), reserved (u16),
//!   value length (u64), value CRC32C (u32), write time in unix milliseconds
//!   (u64), key bytes, then the CRC32C of everything before it in the block
//!   (u32); the rest of the block is zero.
//! - value, starting at the next block and padded with zeros to a block. A
//!   tombstone (a persisted delete of the key) has no value.
//!
//! Footer, written at seal after the last entry: magic `KVCF`, version (u16),
//! reserved (u16), entry count (u32), then per entry its offset (u64),
//! value length (u64), value CRC32C (u32), write time (u64), flags (u16),
//! key length (u16) and key, and finally the CRC32C of all of the above (u32).
//!
//! Trailer, the last block of a sealed segment: magic `KVCT`, version
//! (u16), reserved (u16), footer offset (u64), footer length (u64), entry
//! count (u32), seal time (u64), CRC32C of the above (u32).

use crate::aligned::{BLOCK_SIZE, round_up};
use crate::error::{KvError, KvResult};

pub const FORMAT_VERSION: u16 = 1;
/// Longest key an entry can hold; keys are S3 object keys.
pub const MAX_KEY_LEN: usize = 1024;

const ENTRY_MAGIC: &[u8; 4] = b"KVCE";
const FOOTER_MAGIC: &[u8; 4] = b"KVCF";
const TRAILER_MAGIC: &[u8; 4] = b"KVCT";
const ENTRY_FIXED: usize = 32;
const FOOTER_FIXED: usize = 12;
pub(crate) const FOOTER_RECORD_FIXED: usize = 32;
const FLAG_TOMBSTONE: u16 = 1;
const TRAILER_LEN: usize = 40;

pub(crate) fn crc32c(data: &[u8]) -> u32 {
    // CRC32C (Castagnoli), hardware accelerated; the value fits in 32 bits.
    crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32Iscsi, data) as u32
}

/// What an entry header records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryMeta {
    pub key: String,
    pub value_len: u64,
    pub value_crc: u32,
    pub write_time_ms: u64,
    /// A persisted delete of the key: recovery removes the key when it
    /// replays this entry.
    pub tombstone: bool,
}

impl EntryMeta {
    fn flags(&self) -> u16 {
        if self.tombstone { FLAG_TOMBSTONE } else { 0 }
    }
}

fn tombstone_from(flags: u16) -> KvResult<bool> {
    if flags & !FLAG_TOMBSTONE != 0 {
        return Err(KvError::Corrupt(format!("unknown entry flags {flags:#x}")));
    }
    Ok(flags & FLAG_TOMBSTONE != 0)
}

/// Bytes an entry occupies in its segment: one header block plus the value
/// rounded up to whole blocks.
pub const fn entry_footprint(value_len: u64) -> u64 {
    BLOCK_SIZE as u64 + (value_len.div_ceil(BLOCK_SIZE as u64) * BLOCK_SIZE as u64)
}

pub(crate) fn check_key(key: &str) -> KvResult<()> {
    if key.is_empty() || key.len() > MAX_KEY_LEN {
        return Err(KvError::KeyTooLong {
            len: key.len(),
            max: MAX_KEY_LEN,
        });
    }
    Ok(())
}

fn u16_at(bytes: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([bytes[at], bytes[at + 1]])
}

fn u32_at(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(bytes[at..at + 4].try_into().expect("four bytes"))
}

fn u64_at(bytes: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(bytes[at..at + 8].try_into().expect("eight bytes"))
}

/// Writes the header of `meta` into the first block of `block`.
pub(crate) fn encode_entry_header(meta: &EntryMeta, block: &mut [u8]) {
    let key = meta.key.as_bytes();
    block[0..4].copy_from_slice(ENTRY_MAGIC);
    block[4..6].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    block[6..8].copy_from_slice(&(key.len() as u16).to_le_bytes());
    block[8..10].copy_from_slice(&meta.flags().to_le_bytes());
    block[10..12].copy_from_slice(&0u16.to_le_bytes());
    block[12..20].copy_from_slice(&meta.value_len.to_le_bytes());
    block[20..24].copy_from_slice(&meta.value_crc.to_le_bytes());
    block[24..32].copy_from_slice(&meta.write_time_ms.to_le_bytes());
    let end = ENTRY_FIXED + key.len();
    block[ENTRY_FIXED..end].copy_from_slice(key);
    let crc = crc32c(&block[..end]);
    block[end..end + 4].copy_from_slice(&crc.to_le_bytes());
}

/// Reads an entry header block; any inconsistency is `Corrupt`.
pub(crate) fn decode_entry_header(block: &[u8]) -> KvResult<EntryMeta> {
    if block.len() < BLOCK_SIZE || &block[0..4] != ENTRY_MAGIC {
        return Err(KvError::Corrupt("no entry header".into()));
    }
    if u16_at(block, 4) != FORMAT_VERSION {
        return Err(KvError::Corrupt(format!("entry format version {}", u16_at(block, 4))));
    }
    let key_len = usize::from(u16_at(block, 6));
    if key_len == 0 || key_len > MAX_KEY_LEN {
        return Err(KvError::Corrupt(format!("entry key length {key_len}")));
    }
    let end = ENTRY_FIXED + key_len;
    if crc32c(&block[..end]) != u32_at(block, end) {
        return Err(KvError::Corrupt("entry header checksum".into()));
    }
    let key = std::str::from_utf8(&block[ENTRY_FIXED..end])
        .map_err(|_| KvError::Corrupt("entry key is not UTF-8".into()))?
        .to_owned();
    Ok(EntryMeta {
        key,
        value_len: u64_at(block, 12),
        value_crc: u32_at(block, 20),
        write_time_ms: u64_at(block, 24),
        tombstone: tombstone_from(u16_at(block, 8))?,
    })
}

/// Bytes of a footer listing `entries`, before block padding.
pub(crate) fn footer_len<'a>(keys: impl Iterator<Item = &'a str>) -> usize {
    FOOTER_FIXED + keys.map(|key| FOOTER_RECORD_FIXED + key.len()).sum::<usize>() + 4
}

pub(crate) fn encode_footer(entries: &[(u64, EntryMeta)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(footer_len(entries.iter().map(|(_, meta)| meta.key.as_str())));
    out.extend_from_slice(FOOTER_MAGIC);
    out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (offset, meta) in entries {
        out.extend_from_slice(&offset.to_le_bytes());
        out.extend_from_slice(&meta.value_len.to_le_bytes());
        out.extend_from_slice(&meta.value_crc.to_le_bytes());
        out.extend_from_slice(&meta.write_time_ms.to_le_bytes());
        out.extend_from_slice(&meta.flags().to_le_bytes());
        out.extend_from_slice(&(meta.key.len() as u16).to_le_bytes());
        out.extend_from_slice(meta.key.as_bytes());
    }
    let crc = crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

pub(crate) fn decode_footer(bytes: &[u8]) -> KvResult<Vec<(u64, EntryMeta)>> {
    let corrupt = |what: &str| KvError::Corrupt(format!("footer: {what}"));
    if bytes.len() < FOOTER_FIXED + 4 || &bytes[0..4] != FOOTER_MAGIC {
        return Err(corrupt("no footer"));
    }
    if u16_at(bytes, 4) != FORMAT_VERSION {
        return Err(corrupt("format version"));
    }
    let body = bytes.len() - 4;
    if crc32c(&bytes[..body]) != u32_at(bytes, body) {
        return Err(corrupt("checksum"));
    }
    let count = u32_at(bytes, 8) as usize;
    let mut entries = Vec::with_capacity(count);
    let mut at = FOOTER_FIXED;
    for _ in 0..count {
        if at + FOOTER_RECORD_FIXED > body {
            return Err(corrupt("truncated record"));
        }
        let offset = u64_at(bytes, at);
        let value_len = u64_at(bytes, at + 8);
        let value_crc = u32_at(bytes, at + 16);
        let write_time_ms = u64_at(bytes, at + 20);
        let tombstone = tombstone_from(u16_at(bytes, at + 28)).map_err(|_| corrupt("entry flags"))?;
        let key_len = usize::from(u16_at(bytes, at + 30));
        let key_start = at + FOOTER_RECORD_FIXED;
        if key_len == 0 || key_len > MAX_KEY_LEN || key_start + key_len > body {
            return Err(corrupt("key length"));
        }
        let key = std::str::from_utf8(&bytes[key_start..key_start + key_len])
            .map_err(|_| corrupt("key is not UTF-8"))?
            .to_owned();
        entries.push((
            offset,
            EntryMeta {
                key,
                value_len,
                value_crc,
                write_time_ms,
                tombstone,
            },
        ));
        at = key_start + key_len;
    }
    if at != body {
        return Err(corrupt("trailing bytes"));
    }
    Ok(entries)
}

/// Where a sealed segment's footer is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Trailer {
    pub footer_offset: u64,
    pub footer_len: u64,
    pub entry_count: u32,
    pub sealed_time_ms: u64,
}

pub(crate) fn encode_trailer(trailer: &Trailer, block: &mut [u8]) {
    block[0..4].copy_from_slice(TRAILER_MAGIC);
    block[4..6].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    block[6..8].copy_from_slice(&0u16.to_le_bytes());
    block[8..16].copy_from_slice(&trailer.footer_offset.to_le_bytes());
    block[16..24].copy_from_slice(&trailer.footer_len.to_le_bytes());
    block[24..28].copy_from_slice(&trailer.entry_count.to_le_bytes());
    block[28..36].copy_from_slice(&trailer.sealed_time_ms.to_le_bytes());
    let crc = crc32c(&block[..36]);
    block[36..TRAILER_LEN].copy_from_slice(&crc.to_le_bytes());
}

pub(crate) fn decode_trailer(block: &[u8]) -> Option<Trailer> {
    if block.len() < TRAILER_LEN || &block[0..4] != TRAILER_MAGIC || u16_at(block, 4) != FORMAT_VERSION {
        return None;
    }
    if crc32c(&block[..36]) != u32_at(block, 36) {
        return None;
    }
    Some(Trailer {
        footer_offset: u64_at(block, 8),
        footer_len: u64_at(block, 16),
        entry_count: u32_at(block, 24),
        sealed_time_ms: u64_at(block, 28),
    })
}

/// Blocks a footer of `len` bytes occupies.
pub(crate) fn footer_blocks(len: usize) -> usize {
    round_up(len)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(key: &str, value_len: u64) -> EntryMeta {
        EntryMeta {
            key: key.to_owned(),
            value_len,
            value_crc: crc32c(key.as_bytes()),
            write_time_ms: 1_794_000_000_000,
            tombstone: false,
        }
    }

    #[test]
    fn crc32c_matches_the_castagnoli_check_value() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn entry_headers_round_trip_and_reject_damage() {
        let original = meta("hicache/kimi-k3/00ff.g32", 28 << 20);
        let mut block = vec![0u8; BLOCK_SIZE];
        encode_entry_header(&original, &mut block);
        assert_eq!(decode_entry_header(&block).expect("decode"), original);

        let tombstone = EntryMeta {
            tombstone: true,
            value_len: 0,
            ..original
        };
        let mut tombstone_block = vec![0u8; BLOCK_SIZE];
        encode_entry_header(&tombstone, &mut tombstone_block);
        assert_eq!(decode_entry_header(&tombstone_block).expect("decode"), tombstone);

        for flip in [0, 5, 9, 17, 30] {
            let mut damaged = block.clone();
            damaged[flip] ^= 0x40;
            assert!(decode_entry_header(&damaged).is_err(), "flipped byte {flip}");
        }
        assert!(decode_entry_header(&vec![0u8; BLOCK_SIZE]).is_err());
    }

    #[test]
    fn footers_and_trailers_round_trip_and_reject_damage() {
        let deleted = EntryMeta {
            tombstone: true,
            ..meta("ccc", 0)
        };
        let entries = vec![(0, meta("a", 1)), (8192, meta("bb", 5000)), (20480, deleted)];
        let footer = encode_footer(&entries);
        assert_eq!(footer.len(), footer_len(entries.iter().map(|(_, m)| m.key.as_str())));
        assert_eq!(decode_footer(&footer).expect("decode"), entries);
        let mut damaged = footer.clone();
        damaged[20] ^= 1;
        assert!(decode_footer(&damaged).is_err());
        assert!(decode_footer(&footer[..footer.len() - 1]).is_err());

        let trailer = Trailer {
            footer_offset: 1 << 20,
            footer_len: footer.len() as u64,
            entry_count: 3,
            sealed_time_ms: 7,
        };
        let mut block = vec![0u8; BLOCK_SIZE];
        encode_trailer(&trailer, &mut block);
        assert_eq!(decode_trailer(&block), Some(trailer));
        block[10] ^= 1;
        assert_eq!(decode_trailer(&block), None);
    }

    #[test]
    fn footprints_cover_header_and_padded_value() {
        assert_eq!(entry_footprint(0), BLOCK_SIZE as u64);
        assert_eq!(entry_footprint(1), 2 * BLOCK_SIZE as u64);
        assert_eq!(entry_footprint(BLOCK_SIZE as u64), 2 * BLOCK_SIZE as u64);
        // A 0.88 MB KV page wastes less than one block.
        let page = 880 * 1024 + 123;
        assert!(entry_footprint(page) - page < 2 * BLOCK_SIZE as u64);
        assert!(check_key("").is_err());
        assert!(check_key(&"k".repeat(MAX_KEY_LEN + 1)).is_err());
        assert!(check_key("ok").is_ok());
    }
}
