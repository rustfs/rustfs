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
/// (map key, string, binary blob, or ext payload) inside xl.meta.
///
/// This is not a limit on the aggregate xl.meta carried by a metacache
/// stream: many versions and their inline data can exceed it. Bound each
/// individual value before allocation so corrupted prefixes return an error
/// instead of attempting a huge allocation (see rustfs/rustfs#2715).
pub(crate) const MAX_MSGP_ELEMENT_SIZE: usize = 16 << 20;

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

/// Reads a nil marker (`0xc0`) or a MessagePack map length using `rmp::decode::read_map_len`
/// semantics so error variants match `read_map_len` while leaving `rd` unwrapped afterward.
#[inline]
pub(crate) fn read_nil_or_submap_len<R: Read>(rd: &mut R) -> Result<Option<usize>> {
    let mut first = [0u8; 1];
    rd.read_exact(&mut first).map_err(Error::from)?;
    match first[0] {
        0xc0 => Ok(None),
        0x80..=0x8f => Ok(Some((first[0] & 0x0f) as usize)),
        b => {
            let mut prepend = PrependByteReader {
                byte: Some(b),
                inner: rd,
            };
            let len = rmp::decode::read_map_len(&mut prepend)?;
            Ok(Some(len as usize))
        }
    }
}

/// Reads a MessagePack string key into a 16-byte stack buffer when it fits,
/// falling back to `heap` only for longer keys.
#[inline]
pub(crate) fn read_small_str<'a, R: Read>(rd: &mut R, inline: &'a mut [u8; 16], heap: &'a mut Vec<u8>) -> Result<&'a str> {
    let len = usize::try_from(rmp::decode::read_str_len(rd)?).map_err(Error::other)?;
    let bytes = if len <= inline.len() {
        rd.read_exact(&mut inline[..len]).map_err(Error::from)?;
        &inline[..len]
    } else {
        *heap = read_exact_vec(rd, len)?;
        heap.as_slice()
    };
    std::str::from_utf8(bytes).map_err(|e| Error::FromUtf8(e.to_string()))
}

/// Reads exactly `len` bytes into a fresh buffer, treating `len` as
/// untrusted input: lengths above [`MAX_MSGP_ELEMENT_SIZE`] and allocation
/// failures surface as decode errors instead of aborting the process.
#[inline]
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
#[inline]
pub(crate) fn prealloc_hint(len: usize) -> usize {
    const MAX_PREALLOC_ITEMS: usize = 4096;
    len.min(MAX_PREALLOC_ITEMS)
}

/// Read MessagePack nil or array length. Returns None for nil, Some(len) for array.
#[inline]
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
#[inline]
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
    use crate::{ChecksumAlgo, ErasureAlgo, MetaDeleteMarker, MetaObject};
    use time::OffsetDateTime;
    use uuid::Uuid;

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

    #[test]
    fn prepend_byte_reader_handles_zero_len_and_prepends_once() {
        let mut inner: &[u8] = b"ello";
        let mut reader = PrependByteReader {
            byte: Some(b'h'),
            inner: &mut inner,
        };

        let mut empty = [];
        assert_eq!(reader.read(&mut empty).expect("zero-len read"), 0);
        assert_eq!(reader.byte, Some(b'h'), "zero-len read must retain prepended byte");

        let mut out = Vec::new();
        reader.read_to_end(&mut out).expect("read remaining bytes");
        assert_eq!(out, b"hello");
        assert!(reader.byte.is_none());
    }

    #[test]
    fn read_small_str_switches_between_stack_and_heap_at_16_byte_boundary() {
        assert_eq!(prealloc_hint(0), 0);
        assert_eq!(prealloc_hint(4096), 4096);
        assert_eq!(prealloc_hint(4097), 4096);
        assert_eq!(prealloc_hint(usize::MAX), 4096);

        for len in [0usize, 1, 15, 16, 17, 64] {
            let expected = "k".repeat(len);
            let mut encoded = Vec::new();
            rmp::encode::write_str(&mut encoded, &expected).expect("encode str");

            let mut rd = encoded.as_slice();
            let mut inline = [0u8; 16];
            let mut heap = Vec::new();
            let got = read_small_str(&mut rd, &mut inline, &mut heap).expect("decode small str");
            assert_eq!(got, expected, "len={len}");
            if len <= 16 {
                assert!(heap.is_empty(), "len={len} must stay on stack");
            } else {
                assert_eq!(heap.len(), len, "len={len} must spill to heap");
            }
        }

        // Invalid UTF-8 in both stack (<= 16) and heap (> 16) paths must return FromUtf8.
        for len in [4usize, 20] {
            let mut bad = Vec::new();
            rmp::encode::write_str_len(&mut bad, u32::try_from(len).expect("len fits u32")).expect("write str len");
            bad.extend(std::iter::repeat_n(0xffu8, len));
            let mut rd = bad.as_slice();
            let mut inline = [0u8; 16];
            let mut heap = Vec::new();
            assert!(
                matches!(read_small_str(&mut rd, &mut inline, &mut heap), Err(Error::FromUtf8(_))),
                "invalid utf-8 of len={len} must fail with FromUtf8"
            );
        }
    }

    #[test]
    fn read_nil_or_collection_len_covers_all_msgpack_markers_and_rejects_invalid() {
        // nil marker (0xc0)
        for f in [read_nil_or_submap_len, read_nil_or_map_len, read_nil_or_array_len] {
            let mut rd: &[u8] = &[0xc0];
            assert_eq!(f(&mut rd).expect("nil marker"), None);
        }

        // fixmap (0x80..=0x8f), map16 (0xde), map32 (0xdf)
        for len in [0usize, 1, 15, 16, 300, 70_000] {
            let mut buf = Vec::new();
            rmp::encode::write_map_len(&mut buf, u32::try_from(len).expect("len fits u32")).expect("encode map len");
            let mut rd1 = buf.as_slice();
            let mut rd2 = buf.as_slice();
            assert_eq!(read_nil_or_submap_len(&mut rd1).expect("submap len"), Some(len));
            assert_eq!(read_nil_or_map_len(&mut rd2).expect("map len"), Some(len));
        }

        // fixarray (0x90..=0x9f), array16 (0xdc), array32 (0xdd)
        for len in [0usize, 1, 15, 16, 300, 70_000] {
            let mut buf = Vec::new();
            rmp::encode::write_array_len(&mut buf, u32::try_from(len).expect("len fits u32")).expect("encode array len");
            let mut rd = buf.as_slice();
            assert_eq!(read_nil_or_array_len(&mut rd).expect("array len"), Some(len));
        }

        // Non-map / non-array markers and truncated headers must fail cleanly, and
        // read_nil_or_submap_len must preserve rmp::decode::read_map_len error semantics
        // once a marker byte is present (and return Error::Unexpected on empty EOF).
        let mut empty_sub: &[u8] = &[];
        assert_eq!(read_nil_or_submap_len(&mut empty_sub), Err(Error::Unexpected));
        let mut empty_map: &[u8] = &[];
        assert_eq!(read_nil_or_map_len(&mut empty_map), Err(Error::Unexpected));
        let mut empty_arr: &[u8] = &[];
        assert_eq!(read_nil_or_array_len(&mut empty_arr), Err(Error::Unexpected));

        for bad_map in [&[0x91u8][..], &[0xde, 0x01][..]] {
            let mut rd_sub = bad_map;
            let mut rd_rmp = bad_map;
            let sub_err = read_nil_or_submap_len(&mut rd_sub).expect_err("invalid submap input must fail");
            let rmp_err = Error::from(rmp::decode::read_map_len(&mut rd_rmp).expect_err("rmp read_map_len must fail"));
            assert_eq!(sub_err, rmp_err);

            let mut rd_map = bad_map;
            assert!(read_nil_or_map_len(&mut rd_map).is_err());
        }
        for bad_arr in [&[0x81u8][..], &[0xdc, 0x01][..]] {
            let mut rd_arr = bad_arr;
            assert!(read_nil_or_array_len(&mut rd_arr).is_err());
        }
    }

    #[test]
    fn skip_msgp_value_advances_past_every_wire_marker_and_preserves_trailing_sentinel() {
        let mut stream = Vec::new();
        let mut item_count = 0usize;

        let mut push_item = |encode: &mut dyn FnMut(&mut Vec<u8>)| {
            encode(&mut stream);
            item_count += 1;
        };

        // Primitives: nil, bools, fixpos, fixneg, u8..u64, i8..i64, f32, f64
        push_item(&mut |w| rmp::encode::write_nil(w).expect("write nil"));
        push_item(&mut |w| rmp::encode::write_bool(w, false).expect("write false"));
        push_item(&mut |w| rmp::encode::write_bool(w, true).expect("write true"));
        push_item(&mut |w| rmp::encode::write_pfix(w, 42).expect("write pfix"));
        push_item(&mut |w| rmp::encode::write_nfix(w, -17).expect("write nfix"));
        push_item(&mut |w| rmp::encode::write_u8(w, 200).expect("write u8"));
        push_item(&mut |w| rmp::encode::write_u16(w, 50_000).expect("write u16"));
        push_item(&mut |w| rmp::encode::write_u32(w, 1_000_000).expect("write u32"));
        push_item(&mut |w| rmp::encode::write_u64(w, 10_000_000_000).expect("write u64"));
        push_item(&mut |w| rmp::encode::write_i8(w, -100).expect("write i8"));
        push_item(&mut |w| rmp::encode::write_i16(w, -20_000).expect("write i16"));
        push_item(&mut |w| rmp::encode::write_i32(w, -2_000_000).expect("write i32"));
        push_item(&mut |w| rmp::encode::write_i64(w, -10_000_000_000).expect("write i64"));
        push_item(&mut |w| rmp::encode::write_f32(w, 3.25).expect("write f32"));
        push_item(&mut |w| rmp::encode::write_f64(w, -99.5).expect("write f64"));

        // Strings: fixstr, str8, str16, str32
        push_item(&mut |w| rmp::encode::write_str(w, "short").expect("write fixstr"));
        push_item(&mut |w| rmp::encode::write_str(w, &"s".repeat(40)).expect("write str8"));
        push_item(&mut |w| rmp::encode::write_str(w, &"m".repeat(300)).expect("write str16"));
        push_item(&mut |w| {
            w.push(0xdb); // str32
            w.extend_from_slice(&4u32.to_be_bytes());
            w.extend_from_slice(b"long");
        });

        // Binaries: bin8, bin16, bin32
        push_item(&mut |w| rmp::encode::write_bin(w, &[1, 2, 3]).expect("write bin8"));
        push_item(&mut |w| rmp::encode::write_bin(w, &vec![7u8; 300]).expect("write bin16"));
        push_item(&mut |w| {
            w.push(0xc6); // bin32
            w.extend_from_slice(&3u32.to_be_bytes());
            w.extend_from_slice(&[9, 8, 7]);
        });

        // Arrays: fixarray, array16, array32
        push_item(&mut |w| {
            rmp::encode::write_array_len(w, 2).expect("write fixarray");
            rmp::encode::write_u8(w, 1).expect("write array item 1");
            rmp::encode::write_str(w, "nested").expect("write array item 2");
        });
        push_item(&mut |w| {
            w.push(0xdc); // array16
            w.extend_from_slice(&1u16.to_be_bytes());
            rmp::encode::write_bool(w, true).expect("write array16 item");
        });
        push_item(&mut |w| {
            w.push(0xdd); // array32
            w.extend_from_slice(&1u32.to_be_bytes());
            rmp::encode::write_u8(w, 99).expect("write array32 item");
        });

        // Maps: fixmap, map16, map32
        push_item(&mut |w| {
            rmp::encode::write_map_len(w, 1).expect("write fixmap");
            rmp::encode::write_str(w, "k").expect("write fixmap key");
            rmp::encode::write_u32(w, 123).expect("write fixmap val");
        });
        push_item(&mut |w| {
            w.push(0xde); // map16
            w.extend_from_slice(&1u16.to_be_bytes());
            rmp::encode::write_str(w, "k16").expect("write map16 key");
            rmp::encode::write_nil(w).expect("write map16 val");
        });
        push_item(&mut |w| {
            w.push(0xdf); // map32
            w.extend_from_slice(&1u32.to_be_bytes());
            rmp::encode::write_str(w, "k32").expect("write map32 key");
            rmp::encode::write_bool(w, false).expect("write map32 val");
        });

        // FixExt 1/2/4/8/16 and Ext 8/16/32
        push_item(&mut |w| w.extend_from_slice(&[0xd4, 1, 0xaa])); // fixext1
        push_item(&mut |w| w.extend_from_slice(&[0xd5, 1, 0xaa, 0xbb])); // fixext2
        push_item(&mut |w| w.extend_from_slice(&[0xd6, 1, 1, 2, 3, 4])); // fixext4
        push_item(&mut |w| w.extend_from_slice(&[0xd7, 1, 1, 2, 3, 4, 5, 6, 7, 8])); // fixext8
        push_item(&mut |w| {
            w.extend_from_slice(&[0xd8, 1]); // fixext16
            w.extend_from_slice(&[0x55; 16]);
        });
        push_item(&mut |w| w.extend_from_slice(&[0xc7, 3, 2, 10, 20, 30])); // ext8 len=3, type=2
        push_item(&mut |w| w.extend_from_slice(&[0xc8, 0, 2, 3, 0xde, 0xad])); // ext16 len=2, type=3
        push_item(&mut |w| w.extend_from_slice(&[0xc9, 0, 0, 0, 1, 4, 0xff])); // ext32 len=1, type=4
        push_item(&mut |w| w.push(0xc1)); // Marker::Reserved

        // Trailing sentinel to prove exact cursor alignment after skipping all values.
        rmp::encode::write_u32(&mut stream, 0xdead_beef).expect("write sentinel");

        let mut rd = stream.as_slice();
        for idx in 0..item_count {
            skip_msgp_value(&mut rd).unwrap_or_else(|e| panic!("failed skipping item {idx}: {e}"));
        }
        let sentinel: u32 = rmp::decode::read_int(&mut rd).expect("read trailing sentinel");
        assert_eq!(sentinel, 0xdead_beef);
        assert!(rd.is_empty(), "stream must be completely consumed");
    }

    #[test]
    fn meta_object_and_delete_marker_ignore_unknown_forward_compat_fields() {
        let vid = Uuid::from_u128(0x1234_5678);
        let ddir = Uuid::from_u128(0x9abc_def0);
        let mod_time = OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");

        // Build a MetaObject messagepack map with unknown short (<=16B) and long (>16B) keys
        // carrying nested composite values before, between, and after known fields.
        let mut buf = Vec::new();
        rmp::encode::write_map_len(&mut buf, 8).expect("write map len");

        rmp::encode::write_str(&mut buf, "FutureShort").expect("write FutureShort key");
        rmp::encode::write_array_len(&mut buf, 2).expect("write FutureShort array");
        rmp::encode::write_u64(&mut buf, 999).expect("write FutureShort item 1");
        rmp::encode::write_str(&mut buf, "ignored").expect("write FutureShort item 2");

        rmp::encode::write_str(&mut buf, "ID").expect("write ID key");
        rmp::encode::write_bin(&mut buf, vid.as_bytes()).expect("write ID val");

        rmp::encode::write_str(&mut buf, "FutureLongFieldNameExceedingSixteenBytes").expect("write FutureLong key");
        rmp::encode::write_map_len(&mut buf, 1).expect("write FutureLong map");
        rmp::encode::write_str(&mut buf, "nested").expect("write nested key");
        rmp::encode::write_bin(&mut buf, &[1, 2, 3, 4]).expect("write nested val");

        rmp::encode::write_str(&mut buf, "DDir").expect("write DDir key");
        rmp::encode::write_bin(&mut buf, ddir.as_bytes()).expect("write DDir val");

        rmp::encode::write_str(&mut buf, "EcAlgo").expect("write EcAlgo key");
        rmp::encode::write_uint(&mut buf, u64::from(ErasureAlgo::ReedSolomon.to_u8())).expect("write EcAlgo val");

        rmp::encode::write_str(&mut buf, "CSumAlgo").expect("write CSumAlgo key");
        rmp::encode::write_uint(&mut buf, u64::from(ChecksumAlgo::HighwayHash.to_u8())).expect("write CSumAlgo val");

        rmp::encode::write_str(&mut buf, "Size").expect("write Size key");
        rmp::encode::write_sint(&mut buf, 4096).expect("write Size val");

        rmp::encode::write_str(&mut buf, "FutureTrailingComposite").expect("write trailing unknown key");
        rmp::encode::write_array_len(&mut buf, 2).expect("write trailing array");
        rmp::encode::write_bool(&mut buf, true).expect("write trailing bool");
        rmp::encode::write_bin(&mut buf, &[0xaa, 0xbb]).expect("write trailing bin");

        let mut obj = MetaObject::default();
        let consumed = obj.unmarshal_msg(&buf).expect("unmarshal MetaObject with unknown fields");
        assert_eq!(consumed, u64::try_from(buf.len()).expect("buf len fits u64"));
        assert_eq!(obj.version_id, Some(vid));
        assert_eq!(obj.data_dir, Some(ddir));
        assert_eq!(obj.erasure_algorithm, ErasureAlgo::ReedSolomon);
        assert_eq!(obj.bitrot_checksum_algo, ChecksumAlgo::HighwayHash);
        assert_eq!(obj.size, 4096);

        // Same for MetaDeleteMarker, including a trailing unknown field.
        let mut dm_buf = Vec::new();
        rmp::encode::write_map_len(&mut dm_buf, 4).expect("write dm map len");
        rmp::encode::write_str(&mut dm_buf, "FutureDeleteMarkerExtensionField").expect("write dm unknown key");
        rmp::encode::write_str(&mut dm_buf, "forward-compat-payload").expect("write dm unknown val");
        rmp::encode::write_str(&mut dm_buf, "ID").expect("write dm ID key");
        rmp::encode::write_bin(&mut dm_buf, vid.as_bytes()).expect("write dm ID val");
        rmp::encode::write_str(&mut dm_buf, "MTime").expect("write dm MTime key");
        rmp::encode::write_sint(&mut dm_buf, i64::try_from(mod_time.unix_timestamp_nanos()).expect("nanos fit i64"))
            .expect("write dm MTime val");
        rmp::encode::write_str(&mut dm_buf, "TrailingUnknown").expect("write dm trailing key");
        rmp::encode::write_u16(&mut dm_buf, 77).expect("write dm trailing val");

        let mut dm = MetaDeleteMarker::default();
        let mut dm_rd = dm_buf.as_slice();
        dm.decode_from(&mut dm_rd)
            .expect("decode MetaDeleteMarker with unknown field");
        assert!(dm_rd.is_empty(), "MetaDeleteMarker must consume trailing unknown field");
        assert_eq!(dm.version_id, Some(vid));
        assert_eq!(dm.mod_time, Some(mod_time));
    }
}
