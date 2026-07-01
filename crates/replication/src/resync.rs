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

use std::collections::HashMap;
use std::fmt;
use std::io::{Cursor, Read, Write};

use byteorder::{BigEndian, ByteOrder, LittleEndian};
use rmp::Marker;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

pub const RESYNC_META_FORMAT: u16 = 1;
pub const RESYNC_META_VERSION: u16 = 1;

const MSGP_TIME_EXT_TYPE: i8 = 5;
const MSGP_TIME_LEN: u8 = 12;
pub const WIRE_ZERO_TIME_UNIX: i64 = -62_135_596_800;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    CorruptedFormat,
    Other(String),
}

impl Error {
    fn other(err: impl Into<String>) -> Self {
        Self::Other(err.into())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CorruptedFormat => write!(f, "corrupted format"),
            Self::Other(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::other(err.to_string())
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(err: std::string::FromUtf8Error) -> Self {
        Self::other(err.to_string())
    }
}

impl From<rmp::encode::ValueWriteError> for Error {
    fn from(err: rmp::encode::ValueWriteError) -> Self {
        Self::other(err.to_string())
    }
}

impl From<rmp::decode::ValueReadError> for Error {
    fn from(err: rmp::decode::ValueReadError) -> Self {
        Self::other(err.to_string())
    }
}

impl From<rmp::decode::NumValueReadError> for Error {
    fn from(err: rmp::decode::NumValueReadError) -> Self {
        Self::other(err.to_string())
    }
}

impl From<rmp_serde::decode::Error> for Error {
    fn from(err: rmp_serde::decode::Error) -> Self {
        Self::other(err.to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ResyncStatusType {
    #[default]
    NoResync,
    ResyncPending,
    ResyncCanceled,
    ResyncStarted,
    ResyncCompleted,
    ResyncFailed,
}

impl ResyncStatusType {
    pub fn is_valid(&self) -> bool {
        *self != ResyncStatusType::NoResync
    }
}

impl fmt::Display for ResyncStatusType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ResyncStatusType::ResyncStarted => "Ongoing",
            ResyncStatusType::ResyncCompleted => "Completed",
            ResyncStatusType::ResyncFailed => "Failed",
            ResyncStatusType::ResyncPending => "Pending",
            ResyncStatusType::ResyncCanceled => "Canceled",
            ResyncStatusType::NoResync => "",
        };
        write!(f, "{s}")
    }
}

#[derive(Debug, Clone, Default)]
pub struct ResyncOpts {
    pub bucket: String,
    pub arn: String,
    pub resync_id: String,
    pub resync_before: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TargetReplicationResyncStatus {
    pub start_time: Option<OffsetDateTime>,
    pub last_update: Option<OffsetDateTime>,
    pub resync_id: String,
    pub resync_before_date: Option<OffsetDateTime>,
    pub resync_status: ResyncStatusType,
    pub failed_size: i64,
    pub failed_count: i64,
    pub replicated_size: i64,
    pub replicated_count: i64,
    pub bucket: String,
    pub object: String,
    pub error: Option<String>,
}

impl TargetReplicationResyncStatus {
    pub fn new() -> Self {
        Self::default()
    }

    fn marshal_wire_msg(&self, wr: &mut Vec<u8>) -> Result<()> {
        rmp::encode::write_map_len(wr, 11)?;
        rmp::encode::write_str(wr, "st")?;
        write_msgp_time(wr, wire_time_or_default(self.start_time))?;
        rmp::encode::write_str(wr, "lst")?;
        write_msgp_time(wr, wire_time_or_default(self.last_update))?;
        rmp::encode::write_str(wr, "id")?;
        rmp::encode::write_str(wr, &self.resync_id)?;
        rmp::encode::write_str(wr, "rdt")?;
        write_msgp_time(wr, wire_time_or_default(self.resync_before_date))?;
        rmp::encode::write_str(wr, "rst")?;
        rmp::encode::write_i32(wr, resync_status_to_i32(self.resync_status))?;
        rmp::encode::write_str(wr, "fs")?;
        rmp::encode::write_i64(wr, self.failed_size)?;
        rmp::encode::write_str(wr, "frc")?;
        rmp::encode::write_i64(wr, self.failed_count)?;
        rmp::encode::write_str(wr, "rs")?;
        rmp::encode::write_i64(wr, self.replicated_size)?;
        rmp::encode::write_str(wr, "rrc")?;
        rmp::encode::write_i64(wr, self.replicated_count)?;
        rmp::encode::write_str(wr, "bkt")?;
        rmp::encode::write_str(wr, &self.bucket)?;
        rmp::encode::write_str(wr, "obj")?;
        rmp::encode::write_str(wr, &self.object)?;
        Ok(())
    }

    fn unmarshal_wire_msg<R: Read>(rd: &mut R) -> Result<Self> {
        let mut out = Self::new();
        let mut fields = rmp::decode::read_map_len(rd)?;

        while fields > 0 {
            fields -= 1;
            let key = read_msgp_str(rd)?;
            match key.as_str() {
                "st" => out.start_time = normalize_wire_time(read_msgp_time_or_nil(rd)?),
                "lst" => out.last_update = normalize_wire_time(read_msgp_time_or_nil(rd)?),
                "id" => out.resync_id = read_msgp_str(rd)?,
                "rdt" => out.resync_before_date = normalize_wire_time(read_msgp_time_or_nil(rd)?),
                "rst" => {
                    let v: i32 = rmp::decode::read_int(rd)?;
                    out.resync_status = resync_status_from_i32(v)?;
                }
                "fs" => out.failed_size = rmp::decode::read_int(rd)?,
                "frc" => out.failed_count = rmp::decode::read_int(rd)?,
                "rs" => out.replicated_size = rmp::decode::read_int(rd)?,
                "rrc" => out.replicated_count = rmp::decode::read_int(rd)?,
                "bkt" => out.bucket = read_msgp_str(rd)?,
                "obj" => out.object = read_msgp_str(rd)?,
                _ => skip_msgp_value(rd)?,
            }
        }
        Ok(out)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BucketReplicationResyncStatus {
    pub version: u16,
    pub targets_map: HashMap<String, TargetReplicationResyncStatus>,
    pub id: i32,
    pub last_update: Option<OffsetDateTime>,
}

impl BucketReplicationResyncStatus {
    pub fn new() -> Self {
        Self {
            version: RESYNC_META_VERSION,
            ..Default::default()
        }
    }

    pub fn clone_tgt_stats(&self) -> HashMap<String, TargetReplicationResyncStatus> {
        self.targets_map.clone()
    }

    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut wr = Vec::new();
        rmp::encode::write_map_len(&mut wr, 4)?;
        rmp::encode::write_str(&mut wr, "v")?;
        rmp::encode::write_i32(&mut wr, i32::from(self.version))?;
        rmp::encode::write_str(&mut wr, "brs")?;
        rmp::encode::write_map_len(&mut wr, self.targets_map.len() as u32)?;
        for (arn, status) in &self.targets_map {
            rmp::encode::write_str(&mut wr, arn)?;
            status.marshal_wire_msg(&mut wr)?;
        }
        rmp::encode::write_str(&mut wr, "id")?;
        rmp::encode::write_i32(&mut wr, self.id)?;
        rmp::encode::write_str(&mut wr, "lu")?;
        write_msgp_time(&mut wr, wire_time_or_default(self.last_update))?;
        Ok(wr)
    }

    pub fn unmarshal_msg(data: &[u8]) -> Result<Self> {
        let mut rd = Cursor::new(data);
        let mut out = Self::new();
        let mut fields = rmp::decode::read_map_len(&mut rd)?;

        while fields > 0 {
            fields -= 1;
            let key = read_msgp_str(&mut rd)?;
            match key.as_str() {
                "v" => {
                    let v: i32 = rmp::decode::read_int(&mut rd)?;
                    out.version = u16::try_from(v).map_err(|_| Error::other("invalid resync version"))?;
                }
                "brs" => {
                    let map_len = rmp::decode::read_map_len(&mut rd)?;
                    let mut targets = HashMap::with_capacity(map_len as usize);
                    for _ in 0..map_len {
                        let arn = read_msgp_str(&mut rd)?;
                        let status = TargetReplicationResyncStatus::unmarshal_wire_msg(&mut rd)?;
                        targets.insert(arn, status);
                    }
                    out.targets_map = targets;
                }
                "id" => {
                    out.id = rmp::decode::read_int::<i32, _>(&mut rd)?;
                }
                "lu" => {
                    out.last_update = normalize_wire_time(read_msgp_time_or_nil(&mut rd)?);
                }
                _ => skip_msgp_value(&mut rd)?,
            }
        }
        Ok(out)
    }

    pub fn unmarshal_legacy_msg(data: &[u8]) -> Result<Self> {
        Ok(rmp_serde::from_slice(data)?)
    }
}

pub fn encode_resync_file(status: &BucketReplicationResyncStatus) -> Result<Vec<u8>> {
    let payload = status.marshal_msg()?;
    let mut data = Vec::with_capacity(4 + payload.len());
    let mut major = [0u8; 2];
    LittleEndian::write_u16(&mut major, RESYNC_META_FORMAT);
    data.extend_from_slice(&major);
    let mut minor = [0u8; 2];
    LittleEndian::write_u16(&mut minor, RESYNC_META_VERSION);
    data.extend_from_slice(&minor);
    data.extend_from_slice(&payload);
    Ok(data)
}

pub fn decode_resync_file(data: &[u8]) -> Result<BucketReplicationResyncStatus> {
    if data.len() <= 4 {
        return Err(Error::CorruptedFormat);
    }

    let mut major = [0u8; 2];
    major.copy_from_slice(&data[0..2]);
    if LittleEndian::read_u16(&major) != RESYNC_META_FORMAT {
        return Err(Error::CorruptedFormat);
    }

    let mut minor = [0u8; 2];
    minor.copy_from_slice(&data[2..4]);
    if LittleEndian::read_u16(&minor) != RESYNC_META_VERSION {
        return Err(Error::CorruptedFormat);
    }

    let status = match BucketReplicationResyncStatus::unmarshal_msg(&data[4..]) {
        Ok(v) => v,
        Err(_) => BucketReplicationResyncStatus::unmarshal_legacy_msg(&data[4..])?,
    };
    if status.version != RESYNC_META_VERSION {
        return Err(Error::CorruptedFormat);
    }
    Ok(status)
}

fn wire_time_or_default(value: Option<OffsetDateTime>) -> OffsetDateTime {
    value.unwrap_or_else(wire_zero_time)
}

fn normalize_wire_time(value: Option<OffsetDateTime>) -> Option<OffsetDateTime> {
    match value {
        Some(v) if v == wire_zero_time() || v == OffsetDateTime::UNIX_EPOCH => None,
        other => other,
    }
}

fn wire_zero_time() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(WIRE_ZERO_TIME_UNIX).unwrap_or(OffsetDateTime::UNIX_EPOCH)
}

fn read_msgp_str<R: Read>(rd: &mut R) -> Result<String> {
    let len = rmp::decode::read_str_len(rd)? as usize;
    let mut buf = vec![0u8; len];
    rd.read_exact(&mut buf)?;
    Ok(String::from_utf8(buf)?)
}

fn read_msgp_time_or_nil<R: Read>(rd: &mut R) -> Result<Option<OffsetDateTime>> {
    let marker = rmp::decode::read_marker(rd).map_err(|e| Error::other(format!("{e:?}")))?;
    match marker {
        Marker::Null => Ok(None),
        Marker::Ext8 => Ok(Some(read_msgp_ext8_time(rd)?)),
        other => Err(Error::other(format!("expected time ext or nil, got marker: {other:?}"))),
    }
}

fn read_msgp_ext8_time<R: Read>(rd: &mut R) -> Result<OffsetDateTime> {
    let mut len_buf = [0u8; 1];
    rd.read_exact(&mut len_buf)?;
    let len = len_buf[0] as usize;
    if len != MSGP_TIME_LEN as usize {
        return Err(Error::other(format!("invalid msgp time len: {len}")));
    }
    let mut type_buf = [0u8; 1];
    rd.read_exact(&mut type_buf)?;
    if type_buf[0] != MSGP_TIME_EXT_TYPE as u8 {
        return Err(Error::other(format!("invalid msgp time type: {}", type_buf[0])));
    }
    let mut buf = [0u8; 12];
    rd.read_exact(&mut buf)?;
    let sec = BigEndian::read_i64(&buf[0..8]);
    let nsec = BigEndian::read_u32(&buf[8..12]);
    OffsetDateTime::from_unix_timestamp(sec)
        .map_err(|_| Error::other("invalid timestamp"))?
        .replace_nanosecond(nsec)
        .map_err(|_| Error::other("invalid nanosecond"))
}

fn write_msgp_time<W: Write>(wr: &mut W, time: OffsetDateTime) -> Result<()> {
    wr.write_all(&[0xc7, MSGP_TIME_LEN, MSGP_TIME_EXT_TYPE as u8])?;
    let mut buf = [0u8; 12];
    BigEndian::write_i64(&mut buf[0..8], time.unix_timestamp());
    BigEndian::write_u32(&mut buf[8..12], time.nanosecond());
    wr.write_all(&buf)?;
    Ok(())
}

fn skip_msgp_value<R: Read>(rd: &mut R) -> Result<()> {
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
        Marker::Str8 | Marker::Bin8 => read_skip_len(rd, 1)?,
        Marker::Str16 | Marker::Bin16 => read_skip_len(rd, 2)?,
        Marker::Str32 | Marker::Bin32 => read_skip_len(rd, 4)?,
        Marker::FixArray(n) => {
            for _ in 0..n {
                skip_msgp_value(rd)?;
            }
            return Ok(());
        }
        Marker::Array16 => {
            let n = read_skip_len(rd, 2)?;
            for _ in 0..n {
                skip_msgp_value(rd)?;
            }
            return Ok(());
        }
        Marker::Array32 => {
            let n = read_skip_len(rd, 4)?;
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
            let n = read_skip_len(rd, 2)?;
            for _ in 0..n {
                skip_msgp_value(rd)?;
                skip_msgp_value(rd)?;
            }
            return Ok(());
        }
        Marker::Map32 => {
            let n = read_skip_len(rd, 4)?;
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
        Marker::Ext8 => 1 + read_skip_len(rd, 1)?,
        Marker::Ext16 => 1 + read_skip_len(rd, 2)?,
        Marker::Ext32 => 1 + read_skip_len(rd, 4)?,
        Marker::Reserved => 0,
    };
    if skip_len > 0 {
        let mut buf = vec![0u8; skip_len];
        rd.read_exact(&mut buf)?;
    }
    Ok(())
}

fn read_skip_len<R: Read>(rd: &mut R, bytes: usize) -> Result<usize> {
    let mut buf = vec![0u8; bytes];
    rd.read_exact(&mut buf)?;
    let len = match bytes {
        1 => buf[0] as usize,
        2 => u16::from_be_bytes([buf[0], buf[1]]) as usize,
        4 => u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize,
        _ => return Err(Error::other("invalid MessagePack length width")),
    };
    Ok(len)
}

fn resync_status_to_i32(status: ResyncStatusType) -> i32 {
    match status {
        ResyncStatusType::NoResync => 0,
        ResyncStatusType::ResyncPending => 1,
        ResyncStatusType::ResyncCanceled => 2,
        ResyncStatusType::ResyncStarted => 3,
        ResyncStatusType::ResyncCompleted => 4,
        ResyncStatusType::ResyncFailed => 5,
    }
}

fn resync_status_from_i32(code: i32) -> Result<ResyncStatusType> {
    match code {
        0 => Ok(ResyncStatusType::NoResync),
        1 => Ok(ResyncStatusType::ResyncPending),
        2 => Ok(ResyncStatusType::ResyncCanceled),
        3 => Ok(ResyncStatusType::ResyncStarted),
        4 => Ok(ResyncStatusType::ResyncCompleted),
        5 => Ok(ResyncStatusType::ResyncFailed),
        _ => Err(Error::other(format!("invalid resync status code: {code}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resync_status_display_matches_admin_contract() {
        assert_eq!(ResyncStatusType::ResyncStarted.to_string(), "Ongoing");
        assert_eq!(ResyncStatusType::ResyncCompleted.to_string(), "Completed");
        assert_eq!(ResyncStatusType::NoResync.to_string(), "");
    }

    #[test]
    fn resync_file_round_trips_status() {
        let mut status = BucketReplicationResyncStatus::new();
        status.targets_map.insert(
            "arn:replication:a".to_string(),
            TargetReplicationResyncStatus {
                resync_id: "rid-1".to_string(),
                resync_status: ResyncStatusType::ResyncStarted,
                bucket: "bucket-a".to_string(),
                object: "object-a".to_string(),
                replicated_count: 7,
                ..Default::default()
            },
        );

        let data = encode_resync_file(&status).expect("resync status should encode");
        let got = decode_resync_file(&data).expect("resync status should decode");

        assert_eq!(got.version, RESYNC_META_VERSION);
        assert_eq!(got.targets_map["arn:replication:a"].resync_id, "rid-1");
        assert_eq!(got.targets_map["arn:replication:a"].resync_status, ResyncStatusType::ResyncStarted);
        assert_eq!(got.targets_map["arn:replication:a"].replicated_count, 7);
    }

    #[test]
    fn skip_msgp_value_consumes_ext_type_and_payload() {
        let mut ext16 = Cursor::new(vec![0xc8, 0, 2, MSGP_TIME_EXT_TYPE as u8, 0xaa, 0xbb, 0x01]);
        skip_msgp_value(&mut ext16).expect("ext16 should skip");
        assert!(matches!(rmp::decode::read_marker(&mut ext16), Ok(Marker::FixPos(1))));

        let mut ext32 = Cursor::new(vec![0xc9, 0, 0, 0, 2, MSGP_TIME_EXT_TYPE as u8, 0xaa, 0xbb, 0x01]);
        skip_msgp_value(&mut ext32).expect("ext32 should skip");
        assert!(matches!(rmp::decode::read_marker(&mut ext32), Ok(Marker::FixPos(1))));
    }
}
