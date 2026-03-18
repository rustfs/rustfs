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
        Marker::FixExt1 => 1,
        Marker::FixExt2 => 2,
        Marker::FixExt4 => 4,
        Marker::FixExt8 => 8,
        Marker::FixExt16 => 16,
        Marker::Ext8 => {
            let mut b = [0u8; 1];
            rd.read_exact(&mut b).map_err(Error::from)?;
            let len = b[0] as usize;
            1 + len // type byte + data
        }
        Marker::Ext16 => {
            let mut b = [0u8; 2];
            rd.read_exact(&mut b).map_err(Error::from)?;
            let len = u16::from_be_bytes(b) as usize;
            2 + len // type bytes + data
        }
        Marker::Ext32 => {
            let mut b = [0u8; 4];
            rd.read_exact(&mut b).map_err(Error::from)?;
            let len = u32::from_be_bytes(b) as usize;
            4 + len // type bytes + data
        }
        Marker::Reserved => 0,
    };
    if skip_len > 0 {
        let mut buf = vec![0u8; skip_len];
        rd.read_exact(&mut buf).map_err(Error::from)?;
    }
    Ok(())
}
