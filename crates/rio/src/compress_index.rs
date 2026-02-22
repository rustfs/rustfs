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
use serde::{Deserialize, Serialize};
use std::io::{self, Read, Seek, SeekFrom};

const S2_INDEX_HEADER: &[u8] = b"s2idx\x00";
const S2_INDEX_TRAILER: &[u8] = b"\x00xdi2s";
const MAX_INDEX_ENTRIES: usize = 1 << 16;
const MIN_INDEX_DIST: i64 = 1 << 20;
// const MIN_INDEX_DIST: i64 = 0;

pub trait TryGetIndex {
    fn try_get_index(&self) -> Option<&Index> {
        None
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Index {
    pub total_uncompressed: i64,
    pub total_compressed: i64,
    info: Vec<IndexInfo>,
    est_block_uncomp: i64,
}

impl Default for Index {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexInfo {
    pub compressed_offset: i64,
    pub uncompressed_offset: i64,
}

#[allow(dead_code)]
impl Index {
    pub fn new() -> Self {
        Self {
            total_uncompressed: -1,
            total_compressed: -1,
            info: Vec::new(),
            est_block_uncomp: 0,
        }
    }

    #[allow(dead_code)]
    fn reset(&mut self, max_block: usize) {
        self.est_block_uncomp = max_block as i64;
        self.total_compressed = -1;
        self.total_uncompressed = -1;
        self.info.clear();
    }

    pub fn len(&self) -> usize {
        self.info.len()
    }

    fn alloc_infos(&mut self, n: usize) {
        if n > MAX_INDEX_ENTRIES {
            panic!("n > MAX_INDEX_ENTRIES");
        }
        self.info = Vec::with_capacity(n);
    }

    pub fn add(&mut self, compressed_offset: i64, uncompressed_offset: i64) -> io::Result<()> {
        if self.info.is_empty() {
            self.info.push(IndexInfo {
                compressed_offset,
                uncompressed_offset,
            });
            return Ok(());
        }

        let last_idx = self.info.len() - 1;
        let latest = &mut self.info[last_idx];

        if latest.uncompressed_offset == uncompressed_offset {
            latest.compressed_offset = compressed_offset;
            return Ok(());
        }

        if latest.uncompressed_offset > uncompressed_offset {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "internal error: Earlier uncompressed received ({} > {})",
                    latest.uncompressed_offset, uncompressed_offset
                ),
            ));
        }

        if latest.compressed_offset > compressed_offset {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "internal error: Earlier compressed received ({} > {})",
                    latest.compressed_offset, compressed_offset
                ),
            ));
        }

        if latest.uncompressed_offset + MIN_INDEX_DIST > uncompressed_offset {
            return Ok(());
        }

        self.info.push(IndexInfo {
            compressed_offset,
            uncompressed_offset,
        });

        self.total_compressed = compressed_offset;
        self.total_uncompressed = uncompressed_offset;
        Ok(())
    }

    pub fn find(&self, offset: i64) -> io::Result<(i64, i64)> {
        if self.total_uncompressed < 0 {
            return Err(io::Error::other("corrupt index"));
        }

        let mut offset = offset;
        if offset < 0 {
            offset += self.total_uncompressed;
            if offset < 0 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "offset out of bounds"));
            }
        }

        if offset > self.total_uncompressed {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "offset out of bounds"));
        }

        if self.info.is_empty() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "empty index"));
        }

        if self.info.len() > 200 {
            let n = self
                .info
                .binary_search_by(|info| {
                    if info.uncompressed_offset > offset {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Less
                    }
                })
                .unwrap_or_else(|i| i);

            if n == 0 {
                return Ok((self.info[0].compressed_offset, self.info[0].uncompressed_offset));
            }
            return Ok((self.info[n - 1].compressed_offset, self.info[n - 1].uncompressed_offset));
        }

        let mut compressed_off = 0;
        let mut uncompressed_off = 0;
        for info in &self.info {
            if info.uncompressed_offset > offset {
                break;
            }
            compressed_off = info.compressed_offset;
            uncompressed_off = info.uncompressed_offset;
        }
        Ok((compressed_off, uncompressed_off))
    }

    fn reduce(&mut self) {
        if self.info.len() < MAX_INDEX_ENTRIES && self.est_block_uncomp >= MIN_INDEX_DIST {
            return;
        }

        let mut remove_n = (self.info.len() + 1) / MAX_INDEX_ENTRIES;
        let src = self.info.clone();
        let mut j = 0;

        while self.est_block_uncomp * (remove_n as i64 + 1) < MIN_INDEX_DIST && self.info.len() / (remove_n + 1) > 1000 {
            remove_n += 1;
        }

        let mut idx = 0;
        while idx < src.len() {
            self.info[j] = src[idx].clone();
            j += 1;
            idx += remove_n + 1;
        }
        self.info.truncate(j);
        self.est_block_uncomp += self.est_block_uncomp * remove_n as i64;
    }

    pub fn into_vec(mut self) -> Bytes {
        let mut b = Vec::new();
        self.append_to(&mut b, self.total_uncompressed, self.total_compressed);
        Bytes::from(b)
    }

    pub fn append_to(&mut self, b: &mut Vec<u8>, uncomp_total: i64, comp_total: i64) {
        self.reduce();
        let init_size = b.len();

        // Add skippable header
        b.extend_from_slice(&[0x50, 0x2A, 0x4D, 0x18]); // ChunkTypeIndex
        b.extend_from_slice(&[0, 0, 0]); // Placeholder for chunk length

        // Add header
        b.extend_from_slice(S2_INDEX_HEADER);

        // Add total sizes
        let mut tmp = [0u8; 8];
        let n = write_varint(&mut tmp, uncomp_total);
        b.extend_from_slice(&tmp[..n]);
        let n = write_varint(&mut tmp, comp_total);
        b.extend_from_slice(&tmp[..n]);
        let n = write_varint(&mut tmp, self.est_block_uncomp);
        b.extend_from_slice(&tmp[..n]);
        let n = write_varint(&mut tmp, self.info.len() as i64);
        b.extend_from_slice(&tmp[..n]);

        // Check if we should add uncompressed offsets
        let mut has_uncompressed = 0u8;
        for (idx, info) in self.info.iter().enumerate() {
            if idx == 0 {
                if info.uncompressed_offset != 0 {
                    has_uncompressed = 1;
                    break;
                }
                continue;
            }
            if info.uncompressed_offset != self.info[idx - 1].uncompressed_offset + self.est_block_uncomp {
                has_uncompressed = 1;
                break;
            }
        }
        b.push(has_uncompressed);

        // Add uncompressed offsets if needed
        if has_uncompressed == 1 {
            for (idx, info) in self.info.iter().enumerate() {
                let mut u_off = info.uncompressed_offset;
                if idx > 0 {
                    let prev = &self.info[idx - 1];
                    u_off -= prev.uncompressed_offset + self.est_block_uncomp;
                }
                let n = write_varint(&mut tmp, u_off);
                b.extend_from_slice(&tmp[..n]);
            }
        }

        // Add compressed offsets
        let mut c_predict = self.est_block_uncomp / 2;
        for (idx, info) in self.info.iter().enumerate() {
            let mut c_off = info.compressed_offset;
            if idx > 0 {
                let prev = &self.info[idx - 1];
                c_off -= prev.compressed_offset + c_predict;
                c_predict += c_off / 2;
            }
            let n = write_varint(&mut tmp, c_off);
            b.extend_from_slice(&tmp[..n]);
        }

        // Add total size and trailer
        let total_size = (b.len() - init_size + 4 + S2_INDEX_TRAILER.len()) as u32;
        b.extend_from_slice(&total_size.to_le_bytes());
        b.extend_from_slice(S2_INDEX_TRAILER);

        // Update chunk length
        let chunk_len = b.len() - init_size - 4;
        b[init_size + 1] = chunk_len as u8;
        b[init_size + 2] = (chunk_len >> 8) as u8;
        b[init_size + 3] = (chunk_len >> 16) as u8;
    }

    pub fn load<'a>(&mut self, mut b: &'a [u8]) -> io::Result<&'a [u8]> {
        if b.len() <= 4 + S2_INDEX_HEADER.len() + S2_INDEX_TRAILER.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "buffer too small"));
        }

        if b[0] != 0x50 || b[1] != 0x2A || b[2] != 0x4D || b[3] != 0x18 {
            return Err(io::Error::other("invalid chunk type"));
        }

        let chunk_len = (b[1] as usize) | ((b[2] as usize) << 8) | ((b[3] as usize) << 16);
        b = &b[4..];

        if b.len() < chunk_len {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "buffer too small"));
        }

        if !b.starts_with(S2_INDEX_HEADER) {
            return Err(io::Error::other("invalid header"));
        }
        b = &b[S2_INDEX_HEADER.len()..];

        // Read total uncompressed
        let (v, n) = read_varint(b)?;
        if v < 0 {
            return Err(io::Error::other("invalid uncompressed size"));
        }
        self.total_uncompressed = v;
        b = &b[n..];

        // Read total compressed
        let (v, n) = read_varint(b)?;
        if v < 0 {
            return Err(io::Error::other("invalid compressed size"));
        }
        self.total_compressed = v;
        b = &b[n..];

        // Read est block uncomp
        let (v, n) = read_varint(b)?;
        if v < 0 {
            return Err(io::Error::other("invalid block size"));
        }
        self.est_block_uncomp = v;
        b = &b[n..];

        // Read number of entries
        let (v, n) = read_varint(b)?;
        if v < 0 || v > MAX_INDEX_ENTRIES as i64 {
            return Err(io::Error::other("invalid number of entries"));
        }
        let entries = v as usize;
        b = &b[n..];

        self.alloc_infos(entries);

        if b.is_empty() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "buffer too small"));
        }

        let has_uncompressed = b[0];
        b = &b[1..];

        if has_uncompressed & 1 != has_uncompressed {
            return Err(io::Error::other("invalid uncompressed flag"));
        }

        // Read uncompressed offsets
        for idx in 0..entries {
            let mut u_off = 0i64;
            if has_uncompressed != 0 {
                let (v, n) = read_varint(b)?;
                u_off = v;
                b = &b[n..];
            }

            if idx > 0 {
                let prev = self.info[idx - 1].uncompressed_offset;
                u_off += prev + self.est_block_uncomp;
                if u_off <= prev {
                    return Err(io::Error::other("invalid offset"));
                }
            }
            if u_off < 0 {
                return Err(io::Error::other("negative offset"));
            }
            self.info[idx].uncompressed_offset = u_off;
        }

        // Read compressed offsets
        let mut c_predict = self.est_block_uncomp / 2;
        for idx in 0..entries {
            let (v, n) = read_varint(b)?;
            let mut c_off = v;
            b = &b[n..];

            if idx > 0 {
                c_predict += c_off / 2;
                let prev = self.info[idx - 1].compressed_offset;
                c_off += prev + c_predict;
                if c_off <= prev {
                    return Err(io::Error::other("invalid offset"));
                }
            }
            if c_off < 0 {
                return Err(io::Error::other("negative offset"));
            }
            self.info[idx].compressed_offset = c_off;
        }

        if b.len() < 4 + S2_INDEX_TRAILER.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "buffer too small"));
        }

        // Skip size
        b = &b[4..];

        // Check trailer
        if !b.starts_with(S2_INDEX_TRAILER) {
            return Err(io::Error::other("invalid trailer"));
        }

        Ok(&b[S2_INDEX_TRAILER.len()..])
    }

    pub fn load_stream<R: Read + Seek>(&mut self, mut rs: R) -> io::Result<()> {
        // Go to end
        rs.seek(SeekFrom::End(-10))?;
        let mut tmp = [0u8; 10];
        rs.read_exact(&mut tmp)?;

        // Check trailer
        if &tmp[4..4 + S2_INDEX_TRAILER.len()] != S2_INDEX_TRAILER {
            return Err(io::Error::other("invalid trailer"));
        }

        let sz = u32::from_le_bytes(tmp[..4].try_into().unwrap());
        if sz > 0x7fffffff {
            return Err(io::Error::other("size too large"));
        }

        rs.seek(SeekFrom::End(-(sz as i64)))?;

        let mut buf = vec![0u8; sz as usize];
        rs.read_exact(&mut buf)?;

        self.load(&buf)?;
        Ok(())
    }

    pub fn to_json(&self) -> serde_json::Result<Vec<u8>> {
        #[derive(Serialize)]
        struct Offset {
            compressed: i64,
            uncompressed: i64,
        }

        #[derive(Serialize)]
        struct IndexJson {
            total_uncompressed: i64,
            total_compressed: i64,
            offsets: Vec<Offset>,
            est_block_uncompressed: i64,
        }

        let json = IndexJson {
            total_uncompressed: self.total_uncompressed,
            total_compressed: self.total_compressed,
            offsets: self
                .info
                .iter()
                .map(|info| Offset {
                    compressed: info.compressed_offset,
                    uncompressed: info.uncompressed_offset,
                })
                .collect(),
            est_block_uncompressed: self.est_block_uncomp,
        };

        serde_json::to_vec_pretty(&json)
    }
}

// Helper functions for varint encoding/decoding
fn write_varint(buf: &mut [u8], mut v: i64) -> usize {
    let mut n = 0;
    while v >= 0x80 {
        buf[n] = (v as u8) | 0x80;
        v >>= 7;
        n += 1;
    }
    buf[n] = v as u8;
    n + 1
}

fn read_varint(buf: &[u8]) -> io::Result<(i64, usize)> {
    let mut result = 0i64;
    let mut shift = 0;
    let mut n = 0;

    while n < buf.len() {
        let byte = buf[n];
        n += 1;
        result |= ((byte & 0x7F) as i64) << shift;
        if byte < 0x80 {
            return Ok((result, n));
        }
        shift += 7;
    }

    Err(io::Error::new(io::ErrorKind::UnexpectedEof, "unexpected EOF"))
}

// Helper functions for index header manipulation
#[allow(dead_code)]
pub fn remove_index_headers(b: &[u8]) -> Option<&[u8]> {
    if b.len() < 4 + S2_INDEX_TRAILER.len() {
        return None;
    }

    // Skip size
    let b = &b[4..];

    // Check trailer
    if !b.starts_with(S2_INDEX_TRAILER) {
        return None;
    }

    Some(&b[S2_INDEX_TRAILER.len()..])
}

#[allow(dead_code)]
pub fn restore_index_headers(in_data: &[u8]) -> Vec<u8> {
    if in_data.is_empty() {
        return Vec::new();
    }

    let mut b = Vec::with_capacity(4 + S2_INDEX_HEADER.len() + in_data.len() + S2_INDEX_TRAILER.len() + 4);
    b.extend_from_slice(&[0x50, 0x2A, 0x4D, 0x18]);
    b.extend_from_slice(S2_INDEX_HEADER);
    b.extend_from_slice(in_data);

    let total_size = (b.len() + 4 + S2_INDEX_TRAILER.len()) as u32;
    b.extend_from_slice(&total_size.to_le_bytes());
    b.extend_from_slice(S2_INDEX_TRAILER);

    let chunk_len = b.len() - 4;
    b[1] = chunk_len as u8;
    b[2] = (chunk_len >> 8) as u8;
    b[3] = (chunk_len >> 16) as u8;

    b
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_new() {
        let index = Index::new();
        assert_eq!(index.total_uncompressed, -1);
        assert_eq!(index.total_compressed, -1);
        assert!(index.info.is_empty());
        assert_eq!(index.est_block_uncomp, 0);
    }

    #[test]
    fn test_index_add() -> io::Result<()> {
        let mut index = Index::new();

        // Test adding first index
        index.add(100, 1000)?;
        assert_eq!(index.info.len(), 1);
        assert_eq!(index.info[0].compressed_offset, 100);
        assert_eq!(index.info[0].uncompressed_offset, 1000);

        // Test adding index with same uncompressed offset
        index.add(200, 1000)?;
        assert_eq!(index.info.len(), 1);
        assert_eq!(index.info[0].compressed_offset, 200);
        assert_eq!(index.info[0].uncompressed_offset, 1000);

        // Test adding new index (ensure distance is large enough)
        index.add(300, 2000 + MIN_INDEX_DIST)?;
        assert_eq!(index.info.len(), 2);
        assert_eq!(index.info[1].compressed_offset, 300);
        assert_eq!(index.info[1].uncompressed_offset, 2000 + MIN_INDEX_DIST);

        Ok(())
    }

    #[test]
    fn test_index_add_errors() {
        let mut index = Index::new();

        // Add initial index
        index.add(100, 1000).unwrap();

        // Test adding smaller uncompressed offset
        let err = index.add(200, 500).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        // Test adding smaller compressed offset
        let err = index.add(50, 2000).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_index_find() -> io::Result<()> {
        let mut index = Index::new();
        index.total_uncompressed = 1000 + MIN_INDEX_DIST * 3;
        index.total_compressed = 5000;

        // Add some test data, ensure index spacing meets MIN_INDEX_DIST requirement
        index.add(100, 1000)?;
        index.add(300, 1000 + MIN_INDEX_DIST)?;
        index.add(500, 1000 + MIN_INDEX_DIST * 2)?;

        // Test finding existing offset
        let (comp, uncomp) = index.find(1500)?;
        assert_eq!(comp, 100);
        assert_eq!(uncomp, 1000);

        // Test finding boundary value
        let (comp, uncomp) = index.find(1000 + MIN_INDEX_DIST)?;
        assert_eq!(comp, 300);
        assert_eq!(uncomp, 1000 + MIN_INDEX_DIST);

        // Test finding last index
        let (comp, uncomp) = index.find(1000 + MIN_INDEX_DIST * 2)?;
        assert_eq!(comp, 500);
        assert_eq!(uncomp, 1000 + MIN_INDEX_DIST * 2);

        Ok(())
    }

    #[test]
    fn test_index_find_errors() {
        let mut index = Index::new();
        index.total_uncompressed = 10000;
        index.total_compressed = 5000;

        // Test uninitialized index
        let uninit_index = Index::new();
        let err = uninit_index.find(1000).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Other);

        // Test offset out of range
        let err = index.find(15000).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);

        // Test negative offset
        let err = match index.find(-1000) {
            Ok(_) => panic!("should be error"),
            Err(e) => e,
        };
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn test_index_reduce() {
        let mut index = Index::new();
        index.est_block_uncomp = MIN_INDEX_DIST;

        // Add entries exceeding maximum index count, ensure spacing meets MIN_INDEX_DIST requirement
        for i in 0..MAX_INDEX_ENTRIES + 100 {
            index.add(i as i64 * 100, i as i64 * MIN_INDEX_DIST).unwrap();
        }

        // Manually call reduce method
        index.reduce();

        // Verify index count has been correctly reduced
        assert!(index.info.len() <= MAX_INDEX_ENTRIES);
    }

    #[test]
    fn test_index_json() -> io::Result<()> {
        let mut index = Index::new();

        // Add some test data
        index.add(100, 1000)?;
        index.add(300, 2000 + MIN_INDEX_DIST)?;

        // Test JSON serialization
        let json = index.to_json().unwrap();
        let json_str = String::from_utf8(json).unwrap();

        println!("json_str: {json_str}");
        // Verify JSON content

        assert!(json_str.contains("\"compressed\": 100"));
        assert!(json_str.contains("\"uncompressed\": 1000"));
        assert!(json_str.contains("\"est_block_uncompressed\": 0"));

        Ok(())
    }
}
