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

use crate::{Error, Result};
use rmp::Marker;
use std::io::Read;

/// Maximum accepted length for a single length-prefixed msgpack element
/// (map key, string, binary blob, ext payload, or a whole serialized
/// xl.meta record inside a metacache stream).
///
/// Legitimate elements are far smaller: object keys are at most a few KiB
/// and even an xl.meta record fully populated up to the 10,000-version
/// object cap serializes to a few MiB (metacache streams carry xl.meta
/// without inline data), so 16 MiB leaves ample headroom. A larger value
/// means the length prefix itself is corrupt; decoding must fail with an
/// error instead of attempting a huge allocation that aborts the whole
/// process (see rustfs/rustfs#2715).
pub(crate) const MAX_MSGP_ELEMENT_SIZE: usize = 16 << 20;

/// Reads exactly `len` bytes into a fresh buffer, treating `len` as
/// untrusted input: lengths above [`MAX_MSGP_ELEMENT_SIZE`] and allocation
/// failures surface as decode errors instead of aborting the process.
pub(crate) fn read_exact_vec<R: Read>(rd: &mut R, len: usize) -> Result<Vec<u8>> {
    if len > MAX_MSGP_ELEMENT_SIZE {
        return Err(Error::other(format!(
            "corrupt msgpack element: length {len} exceeds the {MAX_MSGP_ELEMENT_SIZE} byte limit"
        )));
    }
    let mut buf = Vec::new();
    buf.try_reserve_exact(len)
        .map_err(|e| Error::other(format!("msgpack element allocation of {len} bytes failed: {e}")))?;
    buf.resize(len, 0);
    rd.read_exact(&mut buf).map_err(Error::from)?;
    Ok(buf)
}

/// Bounds a decoded collection count when it is used only as a
/// pre-allocation hint. The decode loop still consumes exactly the decoded
/// number of elements (a corrupt count fails with an EOF decode error once
/// the input runs out); this merely keeps the speculative reservation from
/// aborting the process on an absurd count.
pub(crate) fn prealloc_hint(len: usize) -> usize {
    const MAX_PREALLOC_ITEMS: usize = 4096;
    len.min(MAX_PREALLOC_ITEMS)
}

/// Reader that prepends a single byte to the stream. Used when we've read the marker
/// and need to pass it to a decoder that expects to read the marker itself.
pub(crate) struct PrependByteReader<'a, R> {
    pub(crate) byte: Option<u8>,
    pub(crate) inner: &'a mut R,
}

impl<R: Read> Read for PrependByteReader<'_, R> {
    #[allow(clippy::collapsible_if)]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if let Some(b) = self.byte.take() {
            if !buf.is_empty() {
                buf[0] = b;
                return Ok(1);
            }
            self.byte = Some(b);
        }
        self.inner.read(buf)
    }
}

/// Read MessagePack nil or array length. Returns None for nil, Some(len) for array.
pub(crate) fn read_nil_or_array_len<R: Read>(rd: &mut R) -> Result<Option<usize>> {
    let mut buf = [0u8; 1];
    rd.read_exact(&mut buf).map_err(Error::from)?;
    let marker = buf[0];
    match marker {
        0xc0 => Ok(None), // nil
        0x90..=0x9f => Ok(Some((marker & 0x0f) as usize)),
        0xdc => {
            let mut b = [0u8; 2];
            rd.read_exact(&mut b).map_err(Error::from)?;
            Ok(Some(u16::from_be_bytes(b) as usize))
        }
        0xdd => {
            let mut b = [0u8; 4];
            rd.read_exact(&mut b).map_err(Error::from)?;
            Ok(Some(u32::from_be_bytes(b) as usize))
        }
        _ => Err(Error::other(format!("expected nil or array, got marker 0x{marker:02x}"))),
    }
}

/// Read MessagePack nil or map length. Returns None for nil, Some(len) for map.
pub(crate) fn read_nil_or_map_len<R: Read>(rd: &mut R) -> Result<Option<usize>> {
    let mut buf = [0u8; 1];
    rd.read_exact(&mut buf).map_err(Error::from)?;
    let marker = buf[0];
    match marker {
        0xc0 => Ok(None), // nil
        0x80..=0x8f => Ok(Some((marker & 0x0f) as usize)),
        0xde => {
            let mut b = [0u8; 2];
            rd.read_exact(&mut b).map_err(Error::from)?;
            Ok(Some(u16::from_be_bytes(b) as usize))
        }
        0xdf => {
            let mut b = [0u8; 4];
            rd.read_exact(&mut b).map_err(Error::from)?;
            Ok(Some(u32::from_be_bytes(b) as usize))
        }
        _ => Err(Error::other(format!("expected nil or map, got marker 0x{marker:02x}"))),
    }
}

/// Skip a single MessagePack value. Used for unknown map keys.
pub(crate) fn skip_msgp_value<R: Read>(rd: &mut R) -> Result<()> {
    let marker = rmp::decode::read_marker(rd).map_err(Error::from)?;
    let skip_len: usize = match marker {
        Marker::Null | Marker::False | Marker::True => 0,
        Marker::FixPos(_) | Marker::FixNeg(_) => 0,
        Marker::U8 => 1,
        Marker::U16 => 2,
        Marker::U32 => 4,
        Marker::U64 => 8,
        Marker::I8 => 1,
        Marker::I16 => 2,
        Marker::I32 => 4,
        Marker::I64 => 8,
        Marker::F32 => 4,
        Marker::F64 => 8,
        Marker::FixStr(n) => n as usize,
        Marker::Str8 => {
            let mut b = [0u8; 1];
            rd.read_exact(&mut b).map_err(Error::from)?;
            b[0] as usize
        }
        Marker::Str16 => {
            let mut b = [0u8; 2];
            rd.read_exact(&mut b).map_err(Error::from)?;
            u16::from_be_bytes(b) as usize
        }
        Marker::Str32 => {
            let mut b = [0u8; 4];
            rd.read_exact(&mut b).map_err(Error::from)?;
            u32::from_be_bytes(b) as usize
        }
        Marker::Bin8 => {
            let mut b = [0u8; 1];
            rd.read_exact(&mut b).map_err(Error::from)?;
            b[0] as usize
        }
        Marker::Bin16 => {
            let mut b = [0u8; 2];
            rd.read_exact(&mut b).map_err(Error::from)?;
            u16::from_be_bytes(b) as usize
        }
        Marker::Bin32 => {
            let mut b = [0u8; 4];
            rd.read_exact(&mut b).map_err(Error::from)?;
            u32::from_be_bytes(b) as usize
        }
        Marker::FixArray(n) => {
            for _ in 0..n {
                skip_msgp_value(rd)?;
            }
            return Ok(());
        }
        Marker::Array16 => {
            let mut b = [0u8; 2];
            rd.read_exact(&mut b).map_err(Error::from)?;
            let n = u16::from_be_bytes(b);
            for _ in 0..n {
                skip_msgp_value(rd)?;
            }
            return Ok(());
        }
        Marker::Array32 => {
            let mut b = [0u8; 4];
            rd.read_exact(&mut b).map_err(Error::from)?;
            let n = u32::from_be_bytes(b);
            for _ in 0..n {
                skip_msgp_value(rd)?;
            }
            return Ok(());
        }
        Marker::FixMap(n) => {
            for _ in 0..n {
                skip_msgp_value(rd)?;
                skip_msgp_value(rd)?;
            }
            return Ok(());
        }
        Marker::Map16 => {
            let mut b = [0u8; 2];
            rd.read_exact(&mut b).map_err(Error::from)?;
            let n = u16::from_be_bytes(b);
            for _ in 0..n {
                skip_msgp_value(rd)?;
                skip_msgp_value(rd)?;
            }
            return Ok(());
        }
        Marker::Map32 => {
            let mut b = [0u8; 4];
            rd.read_exact(&mut b).map_err(Error::from)?;
            let n = u32::from_be_bytes(b);
            for _ in 0..n {
                skip_msgp_value(rd)?;
                skip_msgp_value(rd)?;
            }
            return Ok(());
        }
        // fixext N = marker + 1 type byte + N data bytes
        Marker::FixExt1 => 2,
        Marker::FixExt2 => 3,
        Marker::FixExt4 => 5,
        Marker::FixExt8 => 9,
        Marker::FixExt16 => 17,
        // ext 8/16/32 = marker + length bytes (read here) + 1 type byte + data
        Marker::Ext8 => {
            let mut b = [0u8; 1];
            rd.read_exact(&mut b).map_err(Error::from)?;
            let len = b[0] as usize;
            1 + len
        }
        Marker::Ext16 => {
            let mut b = [0u8; 2];
            rd.read_exact(&mut b).map_err(Error::from)?;
            let len = u16::from_be_bytes(b) as usize;
            1 + len
        }
        Marker::Ext32 => {
            let mut b = [0u8; 4];
            rd.read_exact(&mut b).map_err(Error::from)?;
            let len = u32::from_be_bytes(b) as usize;
            1 + len
        }
        Marker::Reserved => 0,
    };
    if skip_len > 0 {
        // Discard the payload without allocating a buffer sized by the
        // untrusted length; a truncated stream surfaces as UnexpectedEof.
        let copied = std::io::copy(&mut rd.by_ref().take(skip_len as u64), &mut std::io::sink()).map_err(Error::from)?;
        if copied != skip_len as u64 {
            return Err(Error::from(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "truncated msgpack value",
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_exact_vec_rejects_oversized_length() {
        let mut rd: &[u8] = &[];
        let err = read_exact_vec(&mut rd, MAX_MSGP_ELEMENT_SIZE + 1).expect_err("oversized length must be rejected");
        assert!(err.to_string().contains("exceeds"), "unexpected error: {err}");
    }

    #[test]
    fn read_exact_vec_reads_exact_payload() {
        let mut rd: &[u8] = b"hello";
        assert_eq!(read_exact_vec(&mut rd, 5).unwrap(), b"hello");
    }

    #[test]
    fn skip_msgp_value_rejects_truncated_huge_payload_without_allocating() {
        // bin32 claiming u32::MAX bytes with no payload behind it.
        let mut data = vec![0xc6];
        data.extend_from_slice(&u32::MAX.to_be_bytes());
        let mut rd = data.as_slice();
        assert!(skip_msgp_value(&mut rd).is_err());
    }
}
