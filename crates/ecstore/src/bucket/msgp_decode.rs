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

//! MessagePack decode helpers for bucket metadata, aligned with msgp format.

use crate::error::{Error, Result};
use byteorder::{BigEndian, ByteOrder};
use rmp::Marker;
use std::io::{Read, Write};
use time::OffsetDateTime;

/// Skip a single MessagePack value. Used for unknown map keys.
pub(crate) fn skip_msgp_value<R: Read>(rd: &mut R) -> Result<()> {
    let marker = rmp::decode::read_marker(rd).map_err(|e| Error::other(format!("{e:?}")))?;
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
            rd.read_exact(&mut b).map_err(Error::other)?;
            b[0] as usize
        }
        Marker::Str16 => {
            let mut b = [0u8; 2];
            rd.read_exact(&mut b).map_err(Error::other)?;
            u16::from_be_bytes(b) as usize
        }
        Marker::Str32 => {
            let mut b = [0u8; 4];
            rd.read_exact(&mut b).map_err(Error::other)?;
            u32::from_be_bytes(b) as usize
        }
        Marker::Bin8 => {
            let mut b = [0u8; 1];
            rd.read_exact(&mut b).map_err(Error::other)?;
            b[0] as usize
        }
        Marker::Bin16 => {
            let mut b = [0u8; 2];
            rd.read_exact(&mut b).map_err(Error::other)?;
            u16::from_be_bytes(b) as usize
        }
        Marker::Bin32 => {
            let mut b = [0u8; 4];
            rd.read_exact(&mut b).map_err(Error::other)?;
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
            rd.read_exact(&mut b).map_err(Error::other)?;
            let n = u16::from_be_bytes(b);
            for _ in 0..n {
                skip_msgp_value(rd)?;
            }
            return Ok(());
        }
        Marker::Array32 => {
            let mut b = [0u8; 4];
            rd.read_exact(&mut b).map_err(Error::other)?;
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
            rd.read_exact(&mut b).map_err(Error::other)?;
            let n = u16::from_be_bytes(b);
            for _ in 0..n {
                skip_msgp_value(rd)?;
                skip_msgp_value(rd)?;
            }
            return Ok(());
        }
        Marker::Map32 => {
            let mut b = [0u8; 4];
            rd.read_exact(&mut b).map_err(Error::other)?;
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
            rd.read_exact(&mut b).map_err(Error::other)?;
            let len = b[0] as usize;
            1 + len // type byte + data
        }
        Marker::Ext16 => {
            let mut b = [0u8; 2];
            rd.read_exact(&mut b).map_err(Error::other)?;
            let len = u16::from_be_bytes(b) as usize;
            2 + len
        }
        Marker::Ext32 => {
            let mut b = [0u8; 4];
            rd.read_exact(&mut b).map_err(Error::other)?;
            let len = u32::from_be_bytes(b) as usize;
            4 + len
        }
        Marker::Reserved => 0,
    };
    if skip_len > 0 {
        let mut buf = vec![0u8; skip_len];
        rd.read_exact(&mut buf).map_err(Error::other)?;
    }
    Ok(())
}

/// msgp time format: ext8 (0xc7), len 12, type 5, 8 bytes sec (BE) + 4 bytes nsec (BE).
pub(crate) const MSGP_TIME_EXT_TYPE: i8 = 5;
pub(crate) const MSGP_TIME_LEN: u8 = 12;

/// Read msgp ext8 time - caller must have already read the marker and verified it's ext8.
/// Ext8 format: 1 byte len, 1 byte type, then data bytes.
pub(crate) fn read_msgp_ext8_time<R: Read>(rd: &mut R) -> Result<OffsetDateTime> {
    let mut len_buf = [0u8; 1];
    rd.read_exact(&mut len_buf).map_err(Error::other)?;
    let len = len_buf[0] as usize;
    if len != MSGP_TIME_LEN as usize {
        return Err(Error::other(format!("invalid msgp time len: {len}")));
    }
    let mut type_buf = [0u8; 1];
    rd.read_exact(&mut type_buf).map_err(Error::other)?;
    if type_buf[0] != MSGP_TIME_EXT_TYPE as u8 {
        return Err(Error::other(format!("invalid msgp time type: {}", type_buf[0])));
    }
    let mut buf = [0u8; 12];
    rd.read_exact(&mut buf).map_err(Error::other)?;
    let sec = BigEndian::read_i64(&buf[0..8]);
    let nsec = BigEndian::read_u32(&buf[8..12]);
    OffsetDateTime::from_unix_timestamp(sec)
        .map_err(|_| Error::other("invalid timestamp"))?
        .replace_nanosecond(nsec)
        .map_err(|_| Error::other("invalid nanosecond"))
}

/// Write msgp time as ext8 (0xc7), len 12, type 5. Always uses ext format (never nil).
pub(crate) fn write_msgp_time<W: Write>(wr: &mut W, t: OffsetDateTime) -> Result<()> {
    wr.write_all(&[0xc7, MSGP_TIME_LEN, MSGP_TIME_EXT_TYPE as u8])
        .map_err(Error::other)?;
    let mut buf = [0u8; 12];
    BigEndian::write_i64(&mut buf[0..8], t.unix_timestamp());
    BigEndian::write_u32(&mut buf[8..12], t.nanosecond());
    wr.write_all(&buf).map_err(Error::other)
}
