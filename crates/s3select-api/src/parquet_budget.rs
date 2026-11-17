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

//! Allocation admission shared by Parquet query and compaction readers.

use datafusion::parquet::{
    basic::Compression,
    file::{
        FOOTER_SIZE,
        metadata::{ColumnChunkMetaData, FooterTail, RowGroupMetaData},
    },
};
use std::io::Read;
use thiserror::Error;

/// A Parquet input cannot be decoded within the caller's allocation budget.
#[derive(Debug, Error)]
pub enum ParquetBudgetError {
    /// The encoded declarations are malformed or inconsistent.
    #[error("{0}")]
    Invalid(String),
    /// A declaration would exceed the decode budget.
    #[error("Parquet decoded data exceeds the memory budget")]
    Exceeded,
    /// A compressed page cannot be read within its validated declaration.
    #[error("Parquet compressed page is invalid: {0}")]
    Codec(#[from] std::io::Error),
}

type Result<T> = std::result::Result<T, ParquetBudgetError>;

fn budget_error() -> ParquetBudgetError {
    ParquetBudgetError::Exceeded
}

fn add_budget(total: &mut usize, amount: usize, limit: usize) -> Result<()> {
    *total = total
        .checked_add(amount)
        .filter(|value| *value <= limit)
        .ok_or_else(budget_error)?;
    Ok(())
}

/// Check an encoded FileMetaData footer before the Parquet decoder allocates.
///
/// # Errors
/// Returns an error for malformed metadata or collection reservations over `limit`.
pub fn validate_footer(bytes: &[u8], limit: usize) -> Result<()> {
    ThriftCompactCursor {
        bytes,
        pos: 0,
        remaining: limit,
    }
    .file_metadata(limit)
}

/// Sizes read from one Parquet page header (Thrift compact encoding).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct PageHeaderSizes {
    page_type: usize,
    uncompressed: usize,
    compressed: usize,
    values: usize,
    v2: Option<DataPageV2>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DataPageV2 {
    levels: usize,
    compressed: bool,
}

const THRIFT_COMPACT_STOP: u8 = 0;
const THRIFT_COMPACT_BOOL_TRUE: u8 = 1;
const THRIFT_COMPACT_BOOL_FALSE: u8 = 2;
const THRIFT_COMPACT_BYTE: u8 = 3;
const THRIFT_COMPACT_I16: u8 = 4;
const THRIFT_COMPACT_I32: u8 = 5;
const THRIFT_COMPACT_I64: u8 = 6;
const THRIFT_COMPACT_DOUBLE: u8 = 7;
const THRIFT_COMPACT_BINARY: u8 = 8;
const THRIFT_COMPACT_LIST: u8 = 9;
const THRIFT_COMPACT_SET: u8 = 10;
const THRIFT_COMPACT_MAP: u8 = 11;
const THRIFT_COMPACT_STRUCT: u8 = 12;
const THRIFT_MAX_NESTING: usize = 16;
const PAGE_HEADER_FIELD_UNCOMPRESSED_SIZE: i16 = 2;
const PAGE_HEADER_FIELD_COMPRESSED_SIZE: i16 = 3;
const FILE_METADATA_FIELD_ROW_GROUPS: i16 = 4;

#[derive(Clone, Copy)]
enum MetadataStruct {
    File,
    Schema,
    RowGroup,
    ColumnChunk,
    Column,
    KeyValue,
    Statistics,
    SizeStatistics,
    GeoStatistics,
    BoundingBox,
    SortingColumn,
    EncodingStats,
    LogicalType,
    Decimal,
    Time,
    TimeUnit,
    Integer,
    Variant,
    Geometry,
    Geography,
    ColumnOrder,
    Page,
    DataPage,
    DictionaryPage,
    DataPageV2,
    Empty,
}

#[derive(Clone, Copy)]
enum MetadataValue {
    Integer,
    Byte,
    Boolean,
    Double,
    Binary,
    Struct(MetadataStruct),
    List(u8, MetadataStruct),
}

// The pinned decoder dispatches known fields by ID, even when their wire type
// is wrong. Validate those shapes before the generic cursor can skip a field.
fn metadata_field(structure: MetadataStruct, field: i16) -> Option<MetadataValue> {
    use MetadataStruct as S;
    use MetadataValue as V;
    Some(match (structure, field) {
        (S::File, 1 | 3) => V::Integer,
        (S::File, 2) => V::List(THRIFT_COMPACT_STRUCT, S::Schema),
        (S::File, 4) => V::List(THRIFT_COMPACT_STRUCT, S::RowGroup),
        (S::File, 5) => V::List(THRIFT_COMPACT_STRUCT, S::KeyValue),
        (S::File, 6 | 9) => V::Binary,
        (S::File, 7) => V::List(THRIFT_COMPACT_STRUCT, S::ColumnOrder),
        (S::Schema, 1..=3 | 5..=9) => V::Integer,
        (S::Schema, 4) => V::Binary,
        (S::Schema, 10) => V::Struct(S::LogicalType),
        (S::RowGroup, 1) => V::List(THRIFT_COMPACT_STRUCT, S::ColumnChunk),
        (S::RowGroup, 2 | 3 | 5..=7) => V::Integer,
        (S::RowGroup, 4) => V::List(THRIFT_COMPACT_STRUCT, S::SortingColumn),
        (S::ColumnChunk, 1 | 9) => V::Binary,
        (S::ColumnChunk, 2 | 4..=7) => V::Integer,
        (S::ColumnChunk, 3) => V::Struct(S::Column),
        (S::Column, 1 | 4..=7 | 9..=11 | 14 | 15) => V::Integer,
        (S::Column, 2) => V::List(THRIFT_COMPACT_I32, S::Empty),
        (S::Column, 3) => V::List(THRIFT_COMPACT_BINARY, S::Empty),
        (S::Column, 8) => V::List(THRIFT_COMPACT_STRUCT, S::KeyValue),
        (S::Column, 12) => V::Struct(S::Statistics),
        (S::Column, 13) => V::List(THRIFT_COMPACT_STRUCT, S::EncodingStats),
        (S::Column, 16) => V::Struct(S::SizeStatistics),
        (S::Column, 17) => V::Struct(S::GeoStatistics),
        (S::KeyValue, 1 | 2) => V::Binary,
        (S::Statistics, 1 | 2 | 5 | 6) => V::Binary,
        (S::Statistics, 3 | 4) => V::Integer,
        (S::Statistics, 7 | 8) => V::Boolean,
        (S::SizeStatistics, 1) => V::Integer,
        (S::SizeStatistics, 2 | 3) => V::List(THRIFT_COMPACT_I64, S::Empty),
        (S::GeoStatistics, 1) => V::Struct(S::BoundingBox),
        (S::GeoStatistics, 2) => V::List(THRIFT_COMPACT_I32, S::Empty),
        (S::BoundingBox, 1..=8) => V::Double,
        (S::SortingColumn, 1) => V::Integer,
        (S::SortingColumn, 2 | 3) => V::Boolean,
        (S::EncodingStats, 1..=3) => V::Integer,
        (S::LogicalType, 1..=4 | 6 | 11..=15) => V::Struct(S::Empty),
        (S::LogicalType, 5) => V::Struct(S::Decimal),
        (S::LogicalType, 7 | 8) => V::Struct(S::Time),
        (S::LogicalType, 10) => V::Struct(S::Integer),
        (S::LogicalType, 16) => V::Struct(S::Variant),
        (S::LogicalType, 17) => V::Struct(S::Geometry),
        (S::LogicalType, 18) => V::Struct(S::Geography),
        (S::Decimal, 1 | 2) => V::Integer,
        (S::Time, 1) => V::Boolean,
        (S::Time, 2) => V::Struct(S::TimeUnit),
        (S::TimeUnit, 1..=3) => V::Struct(S::Empty),
        (S::Integer | S::Variant, 1) => V::Byte,
        (S::Integer, 2) => V::Boolean,
        (S::Geometry | S::Geography, 1) => V::Binary,
        (S::Geography, 2) => V::Integer,
        (S::ColumnOrder, 1 | 2) => V::Struct(S::Empty),
        (S::Page, 1..=4) => V::Integer,
        (S::Page, 5) => V::Struct(S::DataPage),
        (S::Page, 6) => V::Struct(S::Empty),
        (S::Page, 7) => V::Struct(S::DictionaryPage),
        (S::Page, 8) => V::Struct(S::DataPageV2),
        (S::DataPage, 1..=4) => V::Integer,
        (S::DataPage, 5) => V::Struct(S::Statistics),
        (S::DictionaryPage, 1 | 2) => V::Integer,
        (S::DictionaryPage, 3) => V::Boolean,
        (S::DataPageV2, 1..=6) => V::Integer,
        (S::DataPageV2, 7) => V::Boolean,
        (S::DataPageV2, 8) => V::Struct(S::Statistics),
        _ => return None,
    })
}

/// Minimal Thrift compact-protocol cursor: enough to read a `PageHeader`'s
/// size fields and skip everything else so the next header can be located.
struct ThriftCompactCursor<'a> {
    bytes: &'a [u8],
    pos: usize,
    remaining: usize,
}

impl<'a> ThriftCompactCursor<'a> {
    fn error() -> ParquetBudgetError {
        ParquetBudgetError::Invalid("Parquet thrift metadata is malformed".to_owned())
    }

    fn byte(&mut self) -> Result<u8> {
        let byte = *self.bytes.get(self.pos).ok_or_else(Self::error)?;
        self.pos += 1;
        Ok(byte)
    }

    fn skip_bytes(&mut self, count: usize) -> Result<()> {
        self.pos = self
            .pos
            .checked_add(count)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(Self::error)?;
        Ok(())
    }

    fn binary(&mut self) -> Result<&'a [u8]> {
        let len = usize::try_from(self.varint()?).map_err(|_| Self::error())?;
        self.charge(len.checked_mul(2).ok_or_else(budget_error)?)?;
        let start = self.pos;
        self.skip_bytes(len)?;
        Ok(&self.bytes[start..self.pos])
    }

    fn varint(&mut self) -> Result<u64> {
        let mut value = 0_u64;
        for shift in (0..64).step_by(7) {
            let byte = self.byte()?;
            if shift == 63 && byte > 1 {
                return Err(Self::error());
            }
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(Self::error())
    }

    fn zigzag(&mut self) -> Result<i64> {
        let raw = self.varint()?;
        Ok(((raw >> 1) as i64) ^ -((raw & 1) as i64))
    }

    /// Every collection element occupies at least one byte, so a declared
    /// count beyond the remaining bytes is malformed. This bounds the walk to
    /// the chunk's size instead of the count a hostile header declares.
    fn collection_count(&mut self, count: u64) -> Result<usize> {
        let count = usize::try_from(count)
            .ok()
            .filter(|count| *count <= self.bytes.len() - self.pos)
            .ok_or_else(Self::error)?;
        Ok(count)
    }

    fn charge_collection(&mut self, kind: u8, count: usize) -> Result<()> {
        let slot = match kind {
            THRIFT_COMPACT_BOOL_TRUE
            | THRIFT_COMPACT_BOOL_FALSE
            | THRIFT_COMPACT_BYTE
            | THRIFT_COMPACT_I16
            | THRIFT_COMPACT_I32
            | THRIFT_COMPACT_I64
            | THRIFT_COMPACT_DOUBLE => 8,
            THRIFT_COMPACT_STRUCT => size_of::<ColumnChunkMetaData>()
                .max(size_of::<RowGroupMetaData>())
                .saturating_mul(2),
            _ => size_of::<Vec<u8>>().saturating_mul(2),
        };
        self.charge(count.checked_mul(slot).ok_or_else(budget_error)?)
    }

    fn charge(&mut self, bytes: usize) -> Result<()> {
        self.remaining = self.remaining.checked_sub(bytes).ok_or_else(budget_error)?;
        Ok(())
    }

    fn list_header(&mut self) -> Result<(u8, usize)> {
        let header = self.byte()?;
        let count = match header >> 4 {
            15 => self.varint()?,
            short => u64::from(short),
        };
        let count = self.collection_count(count)?;
        self.charge_collection(header & 0x0f, count)?;
        Ok((header & 0x0f, count))
    }

    fn field_id(&mut self, last: i16, header: u8) -> Result<i16> {
        match header >> 4 {
            0 => i16::try_from(self.zigzag()?).map_err(|_| Self::error()),
            delta => last.checked_add(i16::from(delta)).ok_or_else(Self::error),
        }
    }

    /// parquet reads the page size fields by id as a zigzag varint whatever
    /// the wire kind says. Follow the id; a kind that is not an integer is
    /// rejected rather than skipped, so the walker never records a smaller
    /// size than the decoder will allocate.
    fn size_field(&mut self, kind: u8) -> Result<usize> {
        match kind {
            THRIFT_COMPACT_I16 | THRIFT_COMPACT_I32 | THRIFT_COMPACT_I64 => {
                usize::try_from(self.zigzag()?).map_err(|_| Self::error())
            }
            _ => Err(Self::error()),
        }
    }

    fn skip_value(&mut self, kind: u8, depth: usize) -> Result<()> {
        if depth > THRIFT_MAX_NESTING {
            return Err(Self::error());
        }
        match kind {
            // A struct field's bool value lives in its header byte.
            THRIFT_COMPACT_BOOL_TRUE | THRIFT_COMPACT_BOOL_FALSE => Ok(()),
            THRIFT_COMPACT_BYTE => self.skip_bytes(1),
            THRIFT_COMPACT_I16 | THRIFT_COMPACT_I32 | THRIFT_COMPACT_I64 => self.varint().map(drop),
            THRIFT_COMPACT_DOUBLE => self.skip_bytes(8),
            THRIFT_COMPACT_BINARY => self.binary().map(drop),
            THRIFT_COMPACT_LIST | THRIFT_COMPACT_SET => {
                let (element, count) = self.list_header()?;
                for _ in 0..count {
                    self.skip_element(element, depth + 1)?;
                }
                Ok(())
            }
            THRIFT_COMPACT_MAP => {
                let count = self.varint()?;
                let count = self.collection_count(count)?;
                if count == 0 {
                    return Ok(());
                }
                let kinds = self.byte()?;
                self.charge_collection(kinds >> 4, count)?;
                self.charge_collection(kinds & 0x0f, count)?;
                for _ in 0..count {
                    self.skip_element(kinds >> 4, depth + 1)?;
                    self.skip_element(kinds & 0x0f, depth + 1)?;
                }
                Ok(())
            }
            THRIFT_COMPACT_STRUCT => self.skip_struct(depth + 1),
            _ => Err(Self::error()),
        }
    }

    /// A collection element carries its own bytes, but parquet skips a bool
    /// element without consuming any. Either width would let the walker and
    /// the decoder disagree on where the next header starts; no Parquet
    /// struct holds a bool collection, so fail closed instead.
    fn skip_element(&mut self, kind: u8, depth: usize) -> Result<()> {
        match kind {
            THRIFT_COMPACT_BOOL_TRUE | THRIFT_COMPACT_BOOL_FALSE => Err(Self::error()),
            kind => self.skip_value(kind, depth),
        }
    }

    fn skip_struct(&mut self, depth: usize) -> Result<()> {
        let mut field_id = 0_i16;
        loop {
            let header = self.byte()?;
            if header == THRIFT_COMPACT_STOP {
                return Ok(());
            }
            field_id = self.field_id(field_id, header)?;
            self.skip_value(header & 0x0f, depth)?;
        }
    }

    fn skip_field(&mut self, structure: MetadataStruct, field: i16, kind: u8, depth: usize) -> Result<()> {
        let Some(expected) = metadata_field(structure, field) else {
            return self.skip_value(kind, depth);
        };
        self.skip_expected(kind, expected, depth)
    }

    fn skip_expected(&mut self, kind: u8, expected: MetadataValue, depth: usize) -> Result<()> {
        if depth > THRIFT_MAX_NESTING {
            return Err(Self::error());
        }
        match expected {
            MetadataValue::Integer if matches!(kind, THRIFT_COMPACT_I16 | THRIFT_COMPACT_I32 | THRIFT_COMPACT_I64) => {
                self.varint().map(drop)
            }
            MetadataValue::Byte if kind == THRIFT_COMPACT_BYTE => self.skip_bytes(1),
            MetadataValue::Boolean if matches!(kind, THRIFT_COMPACT_BOOL_TRUE | THRIFT_COMPACT_BOOL_FALSE) => Ok(()),
            MetadataValue::Double if kind == THRIFT_COMPACT_DOUBLE => self.skip_bytes(8),
            MetadataValue::Binary if kind == THRIFT_COMPACT_BINARY => self.skip_value(kind, depth),
            MetadataValue::Struct(structure) if kind == THRIFT_COMPACT_STRUCT => self.skip_struct_for(structure, depth + 1),
            MetadataValue::List(element_kind, structure) if kind == THRIFT_COMPACT_LIST => {
                let (actual, count) = self.list_header()?;
                if actual != element_kind {
                    return Err(Self::error());
                }
                for _ in 0..count {
                    if actual == THRIFT_COMPACT_STRUCT {
                        self.skip_struct_for(structure, depth + 1)?;
                    } else {
                        self.skip_element(actual, depth + 1)?;
                    }
                }
                Ok(())
            }
            _ => Err(Self::error()),
        }
    }

    fn skip_struct_for(&mut self, structure: MetadataStruct, depth: usize) -> Result<()> {
        if depth > THRIFT_MAX_NESTING {
            return Err(Self::error());
        }
        let mut field = 0;
        loop {
            let header = self.byte()?;
            if header == THRIFT_COMPACT_STOP {
                return Ok(());
            }
            field = self.field_id(field, header)?;
            self.skip_field(structure, field, header & 0x0f, depth)?;
        }
    }

    /// Read a `PageHeader` struct, returning its sizes and leaving the cursor at
    /// the first page byte.
    fn page_header(&mut self) -> Result<PageHeaderSizes> {
        let mut sizes = PageHeaderSizes::default();
        let mut field_id = 0_i16;
        loop {
            let header = self.byte()?;
            if header == THRIFT_COMPACT_STOP {
                return Ok(sizes);
            }
            let kind = header & 0x0f;
            field_id = self.field_id(field_id, header)?;
            match field_id {
                1 => sizes.page_type = self.size_field(kind)?,
                PAGE_HEADER_FIELD_UNCOMPRESSED_SIZE => sizes.uncompressed = self.size_field(kind)?,
                PAGE_HEADER_FIELD_COMPRESSED_SIZE => sizes.compressed = self.size_field(kind)?,
                5 | 7 if kind == THRIFT_COMPACT_STRUCT => {
                    sizes.values = sizes.values.max(self.page_values(if field_id == 5 {
                        MetadataStruct::DataPage
                    } else {
                        MetadataStruct::DictionaryPage
                    })?);
                }
                8 if kind == THRIFT_COMPACT_STRUCT => {
                    let (values, v2) = self.data_page_v2()?;
                    sizes.values = sizes.values.max(values);
                    sizes.v2 = Some(v2);
                }
                _ => self.skip_field(MetadataStruct::Page, field_id, kind, 1)?,
            }
        }
    }

    fn page_values(&mut self, structure: MetadataStruct) -> Result<usize> {
        let mut field = 0;
        let mut values = 0;
        loop {
            let header = self.byte()?;
            if header == THRIFT_COMPACT_STOP {
                return Ok(values);
            }
            field = self.field_id(field, header)?;
            if field == 1 {
                values = self.size_field(header & 0x0f)?;
            } else {
                self.skip_field(structure, field, header & 0x0f, 2)?;
            }
        }
    }

    fn data_page_v2(&mut self) -> Result<(usize, DataPageV2)> {
        let mut field = 0;
        let mut values = 0;
        let mut v2 = DataPageV2 {
            levels: 0,
            compressed: true,
        };
        loop {
            let header = self.byte()?;
            if header == THRIFT_COMPACT_STOP {
                return Ok((values, v2));
            }
            let kind = header & 0x0f;
            field = self.field_id(field, header)?;
            match field {
                1 => values = self.size_field(kind)?,
                5 | 6 => v2.levels = v2.levels.checked_add(self.size_field(kind)?).ok_or_else(budget_error)?,
                7 => {
                    v2.compressed = match kind {
                        THRIFT_COMPACT_BOOL_TRUE => true,
                        THRIFT_COMPACT_BOOL_FALSE => false,
                        _ => return Err(Self::error()),
                    }
                }
                _ => self.skip_field(MetadataStruct::DataPageV2, field, kind, 2)?,
            }
        }
    }

    fn schema(&mut self) -> Result<()> {
        let (kind, count) = self.list_header()?;
        if kind != THRIFT_COMPACT_STRUCT || count == 0 {
            return Err(Self::error());
        }
        let mut remaining = [0usize; 65];
        let mut parent_names = [0usize; 65];
        let mut path_bytes = 0usize;
        remaining[0] = 1;
        let mut depth = 1;
        for _ in 0..count {
            while depth > 0 && remaining[depth - 1] == 0 {
                path_bytes -= parent_names[depth - 1];
                depth -= 1;
            }
            if depth == 0 {
                return Err(Self::error());
            }
            remaining[depth - 1] -= 1;
            let mut field = 0;
            let mut children = 0;
            let mut name_bytes = 0;
            loop {
                let header = self.byte()?;
                if header == THRIFT_COMPACT_STOP {
                    break;
                }
                field = self.field_id(field, header)?;
                if field == 4 && header & 0x0f == THRIFT_COMPACT_BINARY {
                    name_bytes = usize::try_from(self.varint()?).map_err(|_| Self::error())?;
                    self.charge(name_bytes.checked_mul(2).ok_or_else(budget_error)?)?;
                    self.skip_bytes(name_bytes)?;
                } else if field == 5 {
                    children = self.size_field(header & 0x0f)?;
                } else {
                    self.skip_field(MetadataStruct::Schema, field, header & 0x0f, 1)?;
                }
            }
            if children > 0 {
                if depth == remaining.len() {
                    return Err(budget_error());
                }
                remaining[depth] = children;
                parent_names[depth] = name_bytes;
                path_bytes = path_bytes.checked_add(name_bytes).ok_or_else(budget_error)?;
                depth += 1;
            } else {
                // Each leaf descriptor owns its full path, including cloned
                // ancestor names. A wide schema can amplify one long name.
                let strings = depth.checked_mul(size_of::<String>()).ok_or_else(budget_error)?;
                self.charge(
                    path_bytes
                        .checked_add(name_bytes)
                        .and_then(|bytes| bytes.checked_add(strings))
                        .ok_or_else(budget_error)?,
                )?;
            }
        }
        if remaining[..depth].iter().any(|count| *count != 0) {
            return Err(Self::error());
        }
        Ok(())
    }

    fn arrow_schema_metadata(&mut self) -> Result<Option<&'a [u8]>> {
        let (element, count) = self.list_header()?;
        if element != THRIFT_COMPACT_STRUCT {
            return Err(Self::error());
        }
        let mut arrow_schema = None;
        for _ in 0..count {
            let mut field = 0;
            let mut key = None;
            let mut value = None;
            loop {
                let header = self.byte()?;
                if header == THRIFT_COMPACT_STOP {
                    break;
                }
                field = self.field_id(field, header)?;
                let kind = header & 0x0f;
                if matches!(field, 1 | 2) && kind == THRIFT_COMPACT_BINARY {
                    let bytes = self.binary()?;
                    if field == 1 {
                        key = Some(bytes);
                    } else {
                        value = Some(bytes);
                    }
                } else {
                    self.skip_field(MetadataStruct::KeyValue, field, kind, 1)?;
                }
            }
            // Match the reader's last key with a value; absent values are ignored.
            if key == Some(b"ARROW:schema".as_slice()) && value.is_some() {
                arrow_schema = value;
            }
        }
        Ok(arrow_schema)
    }

    fn validate_arrow_schema(&mut self, encoded: &[u8]) -> Result<()> {
        use datafusion::arrow::ipc;
        let invalid = || ParquetBudgetError::Invalid("Parquet Arrow schema metadata is malformed".to_owned());
        // This reservation precedes base64 allocation. The binary field has
        // already reserved its owned metadata copies; decoding needs another buffer.
        self.charge(encoded.len())?;
        let bytes = base64_simd::STANDARD.decode_to_vec(encoded).map_err(|_| invalid())?;
        let message = if bytes.len() > 8 && bytes.starts_with(&[255; 4]) {
            &bytes[8..]
        } else {
            bytes.as_slice()
        };
        // The verifier visits shared offsets for every occurrence, before the
        // reader expands them into owned fields and metadata. Split the remaining
        // allowance between string/buffer copies and per-table Rust containers:
        // four owned copies per apparent byte and 1 KiB per visited table.
        let defaults = flatbuffers::VerifierOptions::default();
        let options = flatbuffers::VerifierOptions {
            max_apparent_size: defaults.max_apparent_size.min(self.remaining / 8),
            max_tables: defaults.max_tables.min(self.remaining / (2 * 1024)),
            ..defaults
        };
        let message = ipc::root_as_message_with_opts(&options, message).map_err(|error| match error {
            flatbuffers::InvalidFlatbuffer::TooManyTables
            | flatbuffers::InvalidFlatbuffer::ApparentSizeTooLarge
            | flatbuffers::InvalidFlatbuffer::DepthLimitReached => budget_error(),
            _ => invalid(),
        })?;
        let schema = message.header_as_schema().ok_or_else(invalid)?;
        if schema.fields().is_none() {
            return Err(invalid());
        }
        Ok(())
    }

    /// Walk the footer's `FileMetaData`. The pinned parquet reserves the
    /// declared `row_groups` capacity before reading one element; charge that
    /// reservation to the budget so a small footer cannot request gigabytes.
    fn file_metadata(&mut self, limit: usize) -> Result<()> {
        let mut field_id = 0_i16;
        let mut reserved = 0_usize;
        let mut arrow_schema = None;
        loop {
            let header = self.byte()?;
            if header == THRIFT_COMPACT_STOP {
                if let Some(encoded) = arrow_schema {
                    self.validate_arrow_schema(encoded)?;
                }
                return Ok(());
            }
            let kind = header & 0x0f;
            field_id = self.field_id(field_id, header)?;
            if field_id == 2 && kind == THRIFT_COMPACT_LIST {
                self.schema()?;
            } else if field_id == 5 && kind == THRIFT_COMPACT_LIST {
                arrow_schema = self.arrow_schema_metadata()?;
            } else if field_id == FILE_METADATA_FIELD_ROW_GROUPS && kind == THRIFT_COMPACT_LIST {
                let (element, count) = self.list_header()?;
                if element != THRIFT_COMPACT_STRUCT {
                    return Err(Self::error());
                }
                let reservation = count.checked_mul(size_of::<RowGroupMetaData>()).ok_or_else(Self::error)?;
                add_budget(&mut reserved, reservation, limit)?;
                for _ in 0..count {
                    self.skip_struct_for(MetadataStruct::RowGroup, 2)?;
                }
            } else {
                self.skip_field(MetadataStruct::File, field_id, kind, 0)?;
            }
        }
    }
}

/// Check the footer of a complete Parquet file before metadata decoding.
///
/// # Errors
/// Returns an error for a malformed trailer, invalid schema depth or declarations over `limit`.
pub fn validate_file_footer(data: &[u8], limit: usize) -> Result<()> {
    let error = || ParquetBudgetError::Invalid("Parquet footer is malformed".to_owned());
    let tail_start = data.len().checked_sub(FOOTER_SIZE).ok_or_else(error)?;
    let tail: &[u8; FOOTER_SIZE] = data[tail_start..].try_into().map_err(|_| error())?;
    let tail = FooterTail::try_new(tail).map_err(|_| error())?;
    let metadata_start = tail_start.checked_sub(tail.metadata_length()).ok_or_else(error)?;
    validate_footer(&data[metadata_start..tail_start], limit)
}

/// Sum the page headers of one column chunk (`[start, end)` in the file) and
/// hold every page and the chunk total to `limit`. Pages are never decompressed.
///
/// # Errors
/// Returns an error for malformed headers, truncated chunks or sizes over the declared budget.
pub fn validate_page_headers(chunk: &[u8], chunk_declared: usize, limit: usize) -> Result<usize> {
    walk_page_headers(chunk, chunk_declared, limit, None).map(|(decoded, _)| decoded)
}

/// Bound page payloads and decoder value slots for one compressed column.
///
/// # Errors
/// Returns an error for malformed headers, inconsistent declarations or allocations over `limit`.
pub fn page_decode_budget(chunk: &[u8], chunk_declared: usize, limit: usize, codec: Compression) -> Result<usize> {
    walk_page_headers(chunk, chunk_declared, limit, Some(codec)).map(|(_, resident)| resident)
}

fn walk_page_headers(chunk: &[u8], chunk_declared: usize, limit: usize, codec: Option<Compression>) -> Result<(usize, usize)> {
    let mut cursor = ThriftCompactCursor {
        bytes: chunk,
        pos: 0,
        remaining: limit,
    };
    let mut total = 0_usize;
    let mut resident = chunk.len();
    let mut codec_memory = 0;
    while cursor.pos < chunk.len() {
        let sizes = cursor.page_header()?;
        add_budget(&mut total, sizes.uncompressed, limit)?;
        add_budget(&mut resident, sizes.uncompressed, limit)?;
        add_budget(&mut resident, sizes.values.checked_mul(32).ok_or_else(budget_error)?, limit)?;
        if total > chunk_declared.max(limit.min(chunk_declared.saturating_mul(2))) {
            return Err(ParquetBudgetError::Invalid(
                "Parquet page headers exceed the footer's declared chunk size".to_owned(),
            ));
        }
        let payload_start = cursor.pos;
        cursor.skip_bytes(sizes.compressed)?;
        if let Some(codec) = codec {
            codec_memory = codec_memory.max(validate_compressed_page(&chunk[payload_start..cursor.pos], sizes, codec, limit)?);
            if resident.checked_add(codec_memory).is_none_or(|total| total > limit) {
                return Err(budget_error());
            }
        }
    }
    Ok((total, resident.checked_add(codec_memory).ok_or_else(budget_error)?))
}

/// Check a column before decoding and return its admitted working-set size.
///
/// # Errors
/// Returns an error for a column outside the file or page declarations over `limit`.
pub fn validate_column_chunk_page_headers(
    data: &[u8],
    column: &ColumnChunkMetaData,
    chunk_declared: usize,
    limit: usize,
) -> Result<usize> {
    // parquet's `byte_range`: the chunk starts at the dictionary page when the
    // footer declares one, wherever that is. Walk exactly the bytes it decodes.
    let start = column.dictionary_page_offset().unwrap_or(column.data_page_offset());
    let start = usize::try_from(start).map_err(|_| budget_error())?;
    let length = usize::try_from(column.compressed_size()).map_err(|_| budget_error())?;
    let end = start
        .checked_add(length)
        .filter(|end| *end <= data.len())
        .ok_or_else(|| ParquetBudgetError::Invalid("Parquet column chunk lies outside the file".into()))?;
    page_decode_budget(&data[start..end], chunk_declared, limit, column.compression())
}

fn bounded_stream_size(reader: impl Read, expected: usize) -> Result<()> {
    let ceiling = u64::try_from(expected)
        .map_err(|_| budget_error())?
        .checked_add(1)
        .ok_or_else(budget_error)?;
    let actual = std::io::copy(&mut reader.take(ceiling), &mut std::io::sink())?;
    if actual != u64::try_from(expected).map_err(|_| budget_error())? {
        return Err(ParquetBudgetError::Invalid(
            "Parquet page expansion differs from its size declaration".into(),
        ));
    }
    Ok(())
}

fn brotli_window(bytes: &[u8]) -> Result<usize> {
    let byte = *bytes.first().ok_or_else(ThriftCompactCursor::error)?;
    let bits = if byte & 1 == 0 {
        16
    } else if (byte >> 1) & 7 != 0 {
        17 + u32::from((byte >> 1) & 7)
    } else {
        match (byte >> 4) & 7 {
            0 => 17,
            1 => return Err(ThriftCompactCursor::error()),
            value => 8 + u32::from(value),
        }
    };
    1usize.checked_shl(bits).ok_or_else(budget_error)
}

fn validate_compressed_page(payload: &[u8], sizes: PageHeaderSizes, codec: Compression, limit: usize) -> Result<usize> {
    let v2 = sizes.v2.filter(|_| sizes.page_type == 3);
    let levels = v2.map_or(0, |v2| v2.levels);
    let payload = payload.get(levels..).ok_or_else(ThriftCompactCursor::error)?;
    let expanded = sizes
        .uncompressed
        .checked_sub(levels)
        .ok_or_else(ThriftCompactCursor::error)?;
    if v2.is_some_and(|v2| !v2.compressed) || codec == Compression::UNCOMPRESSED {
        if payload.len() != expanded {
            return Err(ThriftCompactCursor::error());
        }
        return Ok(0);
    }
    // These pinned codecs stream into a growable Vec and ignore the page size.
    // Prove their output length with a bounded sink before native decoding.
    match codec {
        Compression::GZIP(_) => {
            bounded_stream_size(flate2::read::MultiGzDecoder::new(payload), expanded)?;
            Ok(32 * 1024)
        }
        Compression::BROTLI(_) => {
            let window = brotli_window(payload)?;
            if window > limit {
                return Err(budget_error());
            }
            bounded_stream_size(brotli::Decompressor::new(payload, 4096), expanded)?;
            Ok(window)
        }
        Compression::LZ4 if payload.starts_with(&[4, 34, 77, 24]) || payload.starts_with(&[2, 33, 76, 24]) => {
            let (block, linked) = if payload.starts_with(&[2, 33, 76, 24]) {
                (8 * 1024 * 1024, false)
            } else {
                let descriptor = *payload.get(5).ok_or_else(ThriftCompactCursor::error)?;
                let block = match (descriptor >> 4) & 7 {
                    4 => 64 * 1024,
                    5 => 256 * 1024,
                    6 => 1024 * 1024,
                    7 => 4 * 1024 * 1024,
                    _ => return Err(ThriftCompactCursor::error()),
                };
                (block, payload[4] & 0x20 == 0)
            };
            // Match parquet's pinned fallback decoder: compressed input plus
            // output, with an extra output block and 64 KiB history when linked.
            let working = if linked { block * 3 + 64 * 1024 } else { block * 2 };
            if working > limit {
                return Err(budget_error());
            }
            match bounded_stream_size(lz4_flex::frame::FrameDecoder::new(payload), expanded) {
                // Hadoop data can share the magic prefix, and parquet also
                // tries bounded RAW decoding after a frame error. The bounded
                // sink proved the frame produced no more than `expanded` bytes
                // before that error; let the actual reader select its fallback.
                Err(ParquetBudgetError::Codec(_)) => Ok(working),
                result => result.map(|()| working),
            }
        }
        _ => Ok(0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn varint(mut value: u64) -> Vec<u8> {
        let mut output = Vec::new();
        while value >= 128 {
            output.push(u8::try_from(value & 127).expect("seven bits") | 128);
            value >>= 7;
        }
        output.push(u8::try_from(value).expect("last seven bits"));
        output
    }

    fn compressed_page(payload: &[u8], expanded: usize) -> PageHeaderSizes {
        PageHeaderSizes {
            compressed: payload.len(),
            uncompressed: expanded,
            ..Default::default()
        }
    }

    #[test]
    fn gzip_size_validation_accepts_valid_data_and_rejects_a_false_page_size() {
        let bytes = vec![b'x'; 256 * 1024];
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(&bytes).expect("gzip payload");
        let payload = encoder.finish().expect("gzip footer");
        let codec = Compression::GZIP(Default::default());
        validate_compressed_page(&payload, compressed_page(&payload, bytes.len()), codec, 1024 * 1024)
            .expect("valid compressed page");
        assert!(validate_compressed_page(&payload, compressed_page(&payload, 64), codec, 1024 * 1024).is_err());
    }

    #[test]
    fn brotli_size_validation_accepts_valid_data_and_rejects_a_false_page_size() {
        let bytes = vec![b'x'; 256 * 1024];
        let mut payload = Vec::new();
        {
            let mut encoder = brotli::CompressorWriter::new(&mut payload, 4096, 3, 18);
            encoder.write_all(&bytes).expect("brotli payload");
        }
        let codec = Compression::BROTLI(Default::default());
        validate_compressed_page(&payload, compressed_page(&payload, bytes.len()), codec, 1024 * 1024)
            .expect("valid compressed page");
        assert!(validate_compressed_page(&payload, compressed_page(&payload, 64), codec, 1024 * 1024).is_err());
        assert!(
            matches!(
                validate_compressed_page(&payload, compressed_page(&payload, 64), codec, 16 * 1024),
                Err(ParquetBudgetError::Exceeded)
            ),
            "window admission happens before constructing the decoder"
        );
    }

    #[test]
    fn lz4_frame_size_validation_preserves_valid_data_and_rejects_a_false_page_size() {
        let bytes = vec![b'x'; 256 * 1024];
        let mut encoder = lz4::EncoderBuilder::new()
            .block_size(lz4::BlockSize::Max64KB)
            .build(Vec::new())
            .expect("frame writer");
        encoder.write_all(&bytes).expect("frame data");
        let (payload, result) = encoder.finish();
        result.expect("frame footer");
        validate_compressed_page(&payload, compressed_page(&payload, bytes.len()), Compression::LZ4, 1024 * 1024)
            .expect("valid compressed page");
        assert!(validate_compressed_page(&payload, compressed_page(&payload, 64), Compression::LZ4, 1024 * 1024).is_err());
    }

    #[test]
    fn legacy_lz4_scratch_is_admitted_before_the_fallback_decoder() {
        let payload = [2, 33, 76, 24, 5, 0, 0, 0, 0x40, 42, 0, 0, 0];
        let header = compressed_page(&payload, 4);
        assert!(matches!(
            validate_compressed_page(&payload, header, Compression::LZ4, 1024 * 1024),
            Err(ParquetBudgetError::Exceeded)
        ));
        let admitted = validate_compressed_page(&payload, header, Compression::LZ4, 32 * 1024 * 1024)
            .expect("legacy LZ4 remains supported with enough scratch memory");
        assert!(admitted >= 16 * 1024 * 1024);
        assert!(validate_compressed_page(&payload, compressed_page(&payload, 1), Compression::LZ4, 32 * 1024 * 1024).is_err());
    }

    #[test]
    fn hadoop_lz4_can_share_the_legacy_magic_without_becoming_a_frame() {
        let size = usize::try_from(u32::from_be_bytes([2, 33, 76, 24])).expect("Hadoop size");
        let bytes = vec![b'x'; size];
        let block = lz4_flex::block::compress(&bytes);
        let mut payload = vec![2, 33, 76, 24];
        payload.extend(u32::try_from(block.len()).expect("compressed block").to_be_bytes());
        payload.extend(block);
        validate_compressed_page(&payload, compressed_page(&payload, size), Compression::LZ4, 64 * 1024 * 1024)
            .expect("Hadoop framing takes priority over the frame fallback");
    }

    #[test]
    fn linked_lz4_accounts_for_the_history_and_both_block_buffers() {
        use lz4_flex::frame::{BlockMode, BlockSize, FrameEncoder, FrameInfo};
        let frame = FrameInfo::new().block_mode(BlockMode::Linked).block_size(BlockSize::Max64KB);
        let mut encoder = FrameEncoder::with_frame_info(frame, Vec::new());
        encoder.write_all(&vec![b'x'; 256 * 1024]).expect("linked input");
        let payload = encoder.finish().expect("linked frame");
        let header = compressed_page(&payload, 256 * 1024);
        assert!(matches!(
            validate_compressed_page(&payload, header, Compression::LZ4, 192 * 1024),
            Err(ParquetBudgetError::Exceeded)
        ));
        let admitted = validate_compressed_page(&payload, header, Compression::LZ4, 1024 * 1024)
            .expect("linked frame with enough scratch memory");
        assert!(admitted >= 256 * 1024);
    }

    #[test]
    fn uncompressed_v2_does_not_invoke_the_column_codec() {
        let payload = b"levels-and-uncompressed-values";
        let mut header = compressed_page(payload, payload.len());
        header.page_type = 3;
        header.v2 = Some(DataPageV2 {
            levels: 3,
            compressed: false,
        });
        validate_compressed_page(payload, header, Compression::GZIP(Default::default()), 1024).expect("uncompressed V2 payload");
        header.v2 = Some(DataPageV2 {
            levels: payload.len() + 1,
            compressed: false,
        });
        assert!(validate_compressed_page(payload, header, Compression::UNCOMPRESSED, 1024).is_err());
    }

    #[test]
    fn an_irrelevant_v2_header_cannot_disable_v1_compression_validation() {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(&vec![b'x'; 256 * 1024]).expect("gzip payload");
        let payload = encoder.finish().expect("gzip footer");
        let mut header = compressed_page(&payload, payload.len());
        header.v2 = Some(DataPageV2 {
            levels: 0,
            compressed: false,
        });
        assert!(validate_compressed_page(&payload, header, Compression::GZIP(Default::default()), 1024 * 1024).is_err());
    }

    #[test]
    fn footer_collection_field_types_cannot_bypass_allocation_admission() {
        for field in [2, 4, 5, 7] {
            let mut footer = vec![(field << 4) | THRIFT_COMPACT_I64, 0xfc, 50];
            footer.extend([0; 51]);
            assert!(validate_footer(&footer, 1024).is_err(), "collection field {field} has a scalar wire type");
        }
    }

    #[test]
    fn nested_metadata_field_types_are_checked_before_decoder_allocation() {
        // File.row_groups[0].columns declares a scalar instead of a list.
        let malformed = [0x49, 0x1c, 0x16, 0, 0, 0];
        assert!(matches!(validate_footer(&malformed, 1024 * 1024), Err(ParquetBudgetError::Invalid(_))));
        // File.row_groups[0].columns[0].file_path is a binary field.
        let malformed = [0x49, 0x1c, 0x19, 0x1c, 0x16, 0, 0, 0, 0];
        assert!(matches!(validate_footer(&malformed, 1024 * 1024), Err(ParquetBudgetError::Invalid(_))));
        // A DataPageHeader's statistics must be a struct, not a scalar.
        let malformed = [0x15, 0, 0x15, 0, 0x15, 0, 0x2c, 0x56, 0, 0, 0];
        assert!(matches!(
            page_decode_budget(&malformed, 0, 1024 * 1024, Compression::UNCOMPRESSED),
            Err(ParquetBudgetError::Invalid(_))
        ));
    }

    #[test]
    fn typed_footer_validation_preserves_supported_logical_annotations() {
        use datafusion::arrow::{
            array::{
                ArrayRef, Date32Array, Decimal128Array, StringArray, Time64NanosecondArray, TimestampMillisecondArray,
                UInt16Array,
            },
            record_batch::RecordBatch,
        };
        use datafusion::parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let columns: Vec<(&str, ArrayRef)> = vec![
            (
                "decimal",
                Arc::new(
                    Decimal128Array::from(vec![123_i128])
                        .with_precision_and_scale(10, 2)
                        .expect("decimal type"),
                ),
            ),
            ("timestamp", Arc::new(TimestampMillisecondArray::from(vec![1000_i64]))),
            ("time", Arc::new(Time64NanosecondArray::from(vec![1000_i64]))),
            ("date", Arc::new(Date32Array::from(vec![1_i32]))),
            ("unsigned", Arc::new(UInt16Array::from(vec![1_u16]))),
            ("text", Arc::new(StringArray::from(vec!["value"]))),
        ];
        let batch = RecordBatch::try_from_iter(columns).expect("logical type batch");
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), None).expect("Parquet writer");
        writer.write(&batch).expect("logical type data");
        writer.close().expect("Parquet footer");
        validate_file_footer(&bytes, 1024 * 1024).expect("valid logical annotations remain accepted");
    }

    fn shared_arrow_metadata_file(entries: usize) -> Vec<u8> {
        use datafusion::{
            arrow::ipc,
            parquet::{
                file::{properties::WriterProperties, writer::SerializedFileWriter},
                schema::parser::parse_message_type,
            },
        };
        use std::sync::Arc;
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let name = builder.create_string("v");
        let value = builder.create_string(&"x".repeat(64 * 1024));
        let data_type = ipc::Int::create(
            &mut builder,
            &ipc::IntArgs {
                bitWidth: 32,
                is_signed: true,
            },
        );
        let field = ipc::Field::create(
            &mut builder,
            &ipc::FieldArgs {
                name: Some(name),
                nullable: true,
                type_type: ipc::Type::Int,
                type_: Some(data_type.as_union_value()),
                ..Default::default()
            },
        );
        let fields = builder.create_vector(&[field]);
        let mut metadata = Vec::new();
        for n in 0..entries {
            let key = builder.create_string(&format!("key-{n}"));
            metadata.push(ipc::KeyValue::create(
                &mut builder,
                &ipc::KeyValueArgs {
                    key: Some(key),
                    value: Some(value),
                },
            ));
        }
        let metadata = builder.create_vector(&metadata);
        let schema = ipc::Schema::create(
            &mut builder,
            &ipc::SchemaArgs {
                fields: Some(fields),
                custom_metadata: Some(metadata),
                ..Default::default()
            },
        );
        let message = ipc::Message::create(
            &mut builder,
            &ipc::MessageArgs {
                version: ipc::MetadataVersion::V5,
                header_type: ipc::MessageHeader::Schema,
                header: Some(schema.as_union_value()),
                ..Default::default()
            },
        );
        builder.finish(message, None);
        let encoded = base64_simd::STANDARD.encode_to_string(builder.finished_data());
        let properties = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![datafusion::parquet::file::metadata::KeyValue::new(
                "ARROW:schema".to_owned(),
                Some(encoded),
            )]))
            .build();
        let schema = Arc::new(parse_message_type("message root { OPTIONAL INT32 v; }").expect("physical schema"));
        let mut bytes = Vec::new();
        SerializedFileWriter::new(&mut bytes, schema, Arc::new(properties))
            .expect("writer")
            .close()
            .expect("footer");
        bytes
    }

    #[test]
    fn shared_arrow_metadata_is_admitted_before_owned_schema_conversion() {
        use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let bytes = shared_arrow_metadata_file(256);
        assert!(bytes.len() < 100 * 1024, "shared offsets keep the encoded file small");
        let native = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes.clone())).expect("valid Arrow metadata");
        assert_eq!(native.schema().metadata().len(), 256);
        assert!(
            matches!(validate_file_footer(&bytes, 2 * 1024 * 1024), Err(ParquetBudgetError::Exceeded)),
            "32 MiB of owned metadata must not pass a 2 MiB preflight"
        );
        validate_file_footer(&bytes, 512 * 1024 * 1024).expect("sufficient budget preserves shared metadata");
    }

    fn arrow_hint_footer(entries: &[(&[u8], Option<&[u8]>)]) -> Vec<u8> {
        let count = u8::try_from(entries.len()).expect("small key/value fixture");
        assert!(count < 15);
        let mut bytes = vec![0x59, (count << 4) | THRIFT_COMPACT_STRUCT];
        for (key, value) in entries {
            bytes.push(0x18);
            bytes.extend(varint(u64::try_from(key.len()).expect("key length")));
            bytes.extend(*key);
            if let Some(value) = value {
                bytes.push(0x18);
                bytes.extend(varint(u64::try_from(value.len()).expect("value length")));
                bytes.extend(*value);
            }
            bytes.push(0);
        }
        bytes.push(0);
        bytes
    }

    #[test]
    fn arrow_hint_framing_and_duplicate_keys_match_the_reader() {
        use datafusion::arrow::{
            datatypes::{DataType, Field, Schema},
            ipc,
        };
        let schema = Schema::new(vec![Field::new("v", DataType::Int32, true)]);
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let schema = ipc::convert::schema_to_fb_offset(&mut builder, &schema);
        let message = ipc::Message::create(
            &mut builder,
            &ipc::MessageArgs {
                header_type: ipc::MessageHeader::Schema,
                header: Some(schema.as_union_value()),
                ..Default::default()
            },
        );
        builder.finish(message, None);
        let raw = builder.finished_data();
        let mut framed = vec![255; 4];
        framed.extend(u32::try_from(raw.len()).expect("small schema").to_le_bytes());
        framed.extend(raw);
        for message in [raw, framed.as_slice()] {
            let encoded = base64_simd::STANDARD.encode_to_string(message);
            let footer = arrow_hint_footer(&[
                (b"ARROW:schema", Some(b"invalid")),
                (b"ARROW:schema", Some(encoded.as_bytes())),
                (b"ARROW:schema", None),
            ]);
            validate_footer(&footer, 1024 * 1024).expect("last present Arrow metadata wins, both IPC framings are supported");
            assert!(matches!(
                validate_footer(
                    &arrow_hint_footer(&[
                        (b"ARROW:schema", Some(encoded.as_bytes())),
                        (b"ARROW:schema", Some(b"invalid"))
                    ]),
                    1024 * 1024
                ),
                Err(ParquetBudgetError::Invalid(_))
            ));
        }
        validate_footer(&arrow_hint_footer(&[(b"other", Some(b"invalid"))]), 1024 * 1024).expect("other metadata is opaque");
    }

    #[test]
    fn arrow_hint_value_before_key_is_still_admitted() {
        // Compact struct fields are identified by ID, not required to be ordered.
        let mut bytes = vec![0x59, 0x1c, 0x28, 3];
        bytes.extend(b"%%%");
        bytes.extend([0x08, 2, 12]);
        bytes.extend(b"ARROW:schema");
        bytes.extend([0, 0]);
        assert!(matches!(validate_footer(&bytes, 1024 * 1024), Err(ParquetBudgetError::Invalid(_))));
    }

    #[test]
    fn footer_collections_are_bounded_before_decoder_allocation() {
        let mut huge = vec![0x49, 0xfc];
        huge.extend(varint(u64::from(i32::MAX.unsigned_abs())));
        huge.push(0);
        assert!(validate_footer(&huge, 1024 * 1024).is_err());
        let mut fitting = vec![0x49, 0xfc];
        fitting.extend(varint(50));
        fitting.extend([0; 51]);
        validate_footer(&fitting, 1024 * 1024).expect("small collection with sufficient budget");
        assert!(matches!(validate_footer(&fitting, 1024), Err(ParquetBudgetError::Exceeded)));
    }

    fn schema_footer(nodes: usize) -> Vec<u8> {
        let mut bytes = vec![0x29, 0xfc];
        bytes.extend(varint(u64::try_from(nodes).expect("schema count")));
        for node in 0..nodes {
            bytes.extend([0x55, if node + 1 < nodes { 2 } else { 0 }, 0]);
        }
        bytes.push(0);
        bytes
    }

    #[test]
    fn flat_schema_depth_is_checked_before_recursive_metadata_decoding() {
        validate_footer(&schema_footer(3), 1024 * 1024).expect("ordinary nested schema");
        assert!(validate_footer(&schema_footer(100), 1024 * 1024).is_err());
    }

    #[test]
    fn schema_leaf_paths_charge_repeated_ancestor_names() {
        let mut footer = vec![0x29, 0xfc, 66, 0x55, 2, 0];
        footer.push(0x48);
        footer.extend(varint(16 * 1024));
        footer.extend(vec![b'p'; 16 * 1024]);
        footer.extend([0x15, 128, 1, 0]);
        for _ in 0..64 {
            footer.extend([0x48, 1, b'v', 0]);
        }
        footer.push(0);
        assert!(footer.len() < 32 * 1024, "encoded schema remains small");
        assert!(matches!(validate_footer(&footer, 256 * 1024), Err(ParquetBudgetError::Exceeded)));
        validate_footer(&footer, 4 * 1024 * 1024).expect("sufficient budget includes all leaf paths");
    }

    #[test]
    fn page_value_slots_are_bounded_even_when_the_footer_count_is_small() {
        let mut bytes = vec![0x15, 0, 0x15, 2, 0x15, 2, 0x2c, 0x15];
        bytes.extend(varint(2_000_000));
        bytes.extend([0, 0, 0]);
        assert!(matches!(
            page_decode_budget(&bytes, 1, 4096, Compression::UNCOMPRESSED),
            Err(ParquetBudgetError::Exceeded)
        ));
    }

    #[test]
    fn malformed_varints_cannot_wrap_to_small_metadata_values() {
        let mut bytes = vec![0x16];
        bytes.extend([128; 9]);
        bytes.extend([127, 0]);
        assert!(validate_footer(&bytes, 1024).is_err());
    }
}
