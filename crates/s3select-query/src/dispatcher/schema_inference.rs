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
use csv_core::{ReadRecordResult, Reader, ReaderBuilder, Terminator};
use datafusion::{
    arrow::{
        csv::reader::Format,
        datatypes::{DataType, Schema, SchemaRef},
    },
    datasource::file_format::{DEFAULT_SCHEMA_INFER_MAX_RECORD, csv::CsvFormat},
    error::{DataFusionError, Result},
    execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation},
    object_store::Error as ObjectStoreError,
};
use futures::{StreamExt, stream::BoxStream};
use rustfs_s3select_api::query::session::MAX_SELECT_COLUMNS;
use std::sync::Arc;

pub(super) const MAX_SCHEMA_PREFIX_BYTES: usize = 4 * 1024 * 1024;

pub(super) struct SchemaPrefix {
    pub bytes: Vec<u8>,
    pub reservation: MemoryReservation,
}

pub(super) enum PrefixBoundary {
    JsonLines { records: usize, line_start: usize },
    Csv { reader: Box<Reader>, fields: usize },
}

impl PrefixBoundary {
    pub fn json_lines() -> Self {
        Self::JsonLines {
            records: 0,
            line_start: 0,
        }
    }

    pub fn csv(format: &CsvFormat) -> Self {
        let options = format.options();
        let mut builder = ReaderBuilder::new();
        builder
            .delimiter(options.delimiter)
            .quote(options.quote)
            .escape(options.escape)
            .comment(options.comment);
        if let Some(terminator) = options.terminator {
            builder.terminator(Terminator::Any(terminator));
        }
        Self::Csv {
            reader: Box::new(builder.build()),
            fields: 0,
        }
    }

    fn consume(&mut self, input: &[u8], prefix: &[u8]) -> Result<(usize, bool)> {
        match self {
            Self::JsonLines { records, line_start } => {
                for (index, byte) in input.iter().enumerate() {
                    if *byte != b'\n' {
                        continue;
                    }
                    let end = prefix.len() - input.len() + index + 1;
                    // The complete line is available even when a UTF-8 space
                    // straddles input chunks. Match Arrow's blank-line rule.
                    let nonblank = !std::str::from_utf8(&prefix[*line_start..end])
                        .unwrap_or("invalid UTF-8")
                        .trim()
                        .is_empty();
                    *line_start = end;
                    if nonblank {
                        *records += 1;
                    }
                    if *records == DEFAULT_SCHEMA_INFER_MAX_RECORD {
                        return Ok((index + 1, true));
                    }
                }
                Ok((input.len(), false))
            }
            Self::Csv { reader, fields } => {
                let mut consumed = 0;
                let mut output = [0u8; 8192];
                let mut ends = [0usize; MAX_SELECT_COLUMNS + 1];
                loop {
                    let (result, read, _, count) =
                        reader.read_record(&input[consumed..], &mut output, &mut ends[..MAX_SELECT_COLUMNS + 1 - *fields]);
                    consumed += read;
                    *fields += count;
                    if *fields > MAX_SELECT_COLUMNS {
                        return Err(DataFusionError::ResourcesExhausted(
                            "S3 Select CSV schema exceeds 1024 columns".to_owned(),
                        ));
                    }
                    match result {
                        ReadRecordResult::Record | ReadRecordResult::End => return Ok((consumed, true)),
                        ReadRecordResult::InputEmpty => return Ok((consumed, false)),
                        ReadRecordResult::OutputFull if consumed == input.len() => return Ok((consumed, false)),
                        ReadRecordResult::OutputFull => {}
                        ReadRecordResult::OutputEndsFull => {
                            return Err(DataFusionError::ResourcesExhausted(
                                "S3 Select CSV schema exceeds 1024 columns".to_owned(),
                            ));
                        }
                    }
                }
            }
        }
    }
}

/// Reads only complete inference records. Hitting the byte bound with no
/// complete record is an error; with at least one complete record the sample
/// is cut there, exactly as a short object or the record count would cut it.
pub(super) async fn read_schema_prefix(
    mut stream: BoxStream<'static, std::result::Result<Bytes, ObjectStoreError>>,
    memory_pool: &Arc<dyn MemoryPool>,
    mut boundary: PrefixBoundary,
) -> Result<SchemaPrefix> {
    let reservation = MemoryConsumer::new("S3 Select schema prefix").register(memory_pool);
    let mut bytes = Vec::new();
    let mut parsed = 0;
    while let Some(chunk) = stream.next().await.transpose()? {
        let mut offset = 0;
        while offset < chunk.len() {
            let remaining = MAX_SCHEMA_PREFIX_BYTES - bytes.len();
            if remaining == 0 {
                if let PrefixBoundary::JsonLines { records, line_start } = &boundary
                    && *records > 0
                {
                    bytes.truncate(*line_start);
                    return Ok(SchemaPrefix { bytes, reservation });
                }
                return Err(prefix_limit_error());
            }
            let end = chunk.len().min(offset + remaining);
            let required = bytes.len() + end - offset;
            if required > bytes.capacity() {
                let capacity = required.next_power_of_two();
                reservation.try_resize(capacity)?;
                bytes
                    .try_reserve_exact(capacity - bytes.len())
                    .map_err(|_| prefix_limit_error())?;
                reservation.try_resize(bytes.capacity())?;
            }
            bytes.extend_from_slice(&chunk[offset..end]);
            // csv-core recognizes a BOM only when its three bytes arrive
            // together, but treats a BOM with no following bytes as EOF after
            // stripping it. Wait for data or the actual end of the stream.
            if parsed == 0 && (bytes.len() < 3 || bytes.as_slice() == b"\xef\xbb\xbf") {
                offset = end;
                continue;
            }
            let (consumed, done) = boundary.consume(&bytes[parsed..], &bytes)?;
            parsed += consumed;
            bytes.truncate(parsed);
            offset = end;
            if done {
                return Ok(SchemaPrefix { bytes, reservation });
            }
        }
    }
    if parsed < bytes.len() {
        boundary.consume(&bytes[parsed..], &bytes)?;
    }
    boundary.consume(&[], &bytes)?;
    Ok(SchemaPrefix { bytes, reservation })
}

fn prefix_limit_error() -> DataFusionError {
    DataFusionError::ResourcesExhausted("S3 Select schema inference exceeds the 4 MiB prefix limit".to_owned())
}

pub(super) fn infer_csv_schema(bytes: &[u8], format: &CsvFormat) -> Result<SchemaRef> {
    let options = format.options();
    let mut arrow = Format::default()
        .with_header(options.has_header.unwrap_or(false))
        .with_delimiter(options.delimiter)
        .with_quote(options.quote);
    if let Some(escape) = options.escape {
        arrow = arrow.with_escape(escape);
    }
    if let Some(comment) = options.comment {
        arrow = arrow.with_comment(comment);
    }
    if let Some(terminator) = options.terminator {
        arrow = arrow.with_terminator(terminator);
    }
    let (schema, _) = arrow.infer_schema(bytes, Some(0))?;
    // Preserve CsvFormat's zero-record Utf8 inference and duplicate-header
    // merge semantics. Width has already been checked before Arrow allocation.
    let fields = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone().with_data_type(DataType::Utf8))
        .collect::<Vec<_>>();
    Ok(Arc::new(Schema::try_merge([Schema::new(fields)])?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use futures::stream;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn pool() -> Arc<dyn MemoryPool> {
        Arc::new(GreedyMemoryPool::new(64 * 1024 * 1024))
    }

    #[tokio::test]
    async fn bounded_json_prefix_keeps_only_complete_records_at_byte_limit() {
        let first = b"{\"a\":1}\n";
        let mut input = first.to_vec();
        input.extend_from_slice(b"{\"a\":\"");
        input.extend(std::iter::repeat_n(b'x', MAX_SCHEMA_PREFIX_BYTES));
        input.extend_from_slice(b"\"}\n");
        for width in [8192, MAX_SCHEMA_PREFIX_BYTES + 128] {
            let chunks = input
                .chunks(width)
                .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
                .collect::<Vec<_>>();
            let prefix = read_schema_prefix(stream::iter(chunks).boxed(), &pool(), PrefixBoundary::json_lines())
                .await
                .expect("complete first record fits the inference sample");
            let schema = super::super::json_lines_schema::infer_schema(&prefix.bytes, DEFAULT_SCHEMA_INFER_MAX_RECORD)
                .expect("a complete JSON Lines object must not produce a truncated schema record");
            assert_eq!(prefix.bytes, first, "the incomplete second record is outside the sample");
            assert_eq!(schema.fields().len(), 1);
        }
    }

    #[tokio::test]
    async fn bounded_json_prefix_empty_and_complete_record_controls() {
        for input in [b"".as_slice(), b"{\"a\":1}\n".as_slice(), b"{\"a\":1}".as_slice()] {
            let prefix = read_schema_prefix(
                stream::iter([Ok(Bytes::copy_from_slice(input))]).boxed(),
                &pool(),
                PrefixBoundary::json_lines(),
            )
            .await
            .expect("short complete input is accepted");
            let schema = super::super::json_lines_schema::infer_schema(&prefix.bytes, DEFAULT_SCHEMA_INFER_MAX_RECORD)
                .expect("short JSON Lines schema");
            assert_eq!(schema.fields().len(), usize::from(!input.is_empty()));
        }
    }

    #[tokio::test]
    async fn bounded_schema_prefix_rejects_an_incomplete_first_record() {
        for json in [true, false] {
            let bytes = if json {
                format!("{{\"payload\":\"{}\"}}\n", "x".repeat(MAX_SCHEMA_PREFIX_BYTES))
            } else {
                format!("\"{}\"\n", "x".repeat(MAX_SCHEMA_PREFIX_BYTES))
            };
            let boundary = if json {
                PrefixBoundary::json_lines()
            } else {
                PrefixBoundary::csv(&CsvFormat::default())
            };
            let error = read_schema_prefix(stream::iter([Ok(Bytes::from(bytes))]).boxed(), &pool(), boundary)
                .await
                .err()
                .expect("a partial first record cannot provide an inference sample");
            assert!(error.to_string().contains("4 MiB prefix"), "{error}");
        }
    }

    #[tokio::test]
    async fn bounded_schema_prefix_stops_polling_after_complete_sample() {
        let polls = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&polls);
        let chunks = stream::iter([
            Ok(Bytes::from("{\"a\":1}\n".repeat(DEFAULT_SCHEMA_INFER_MAX_RECORD))),
            Err(ObjectStoreError::Generic {
                store: "unread tail",
                source: Box::new(std::io::Error::other("tail must not be read")),
            }),
        ])
        .inspect(move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
        })
        .boxed();
        let prefix = read_schema_prefix(chunks, &pool(), PrefixBoundary::json_lines())
            .await
            .expect("bounded prefix");
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert_eq!(
            super::super::json_lines_schema::infer_schema(&prefix.bytes, DEFAULT_SCHEMA_INFER_MAX_RECORD)
                .expect("schema")
                .fields()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn bounded_schema_prefix_preserves_csv_quotes_bom_and_chunk_boundaries() {
        let format = CsvFormat::default().with_has_header(true).with_comment(Some(b'#'));
        // All the commas below belong to one quoted header, not extra columns.
        let input = format!("\u{feff}#comment\n\"{}\",second\nvalue,2\n", "comma,".repeat(1025));
        let chunks = input
            .into_bytes()
            .into_iter()
            .map(|byte| Ok(Bytes::from(vec![byte])))
            .collect::<Vec<_>>();
        let prefix = read_schema_prefix(stream::iter(chunks).boxed(), &pool(), PrefixBoundary::csv(&format))
            .await
            .expect("quoted header");
        let schema = infer_csv_schema(&prefix.bytes, &format).expect("schema");
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(1).name(), "second");
        assert!(!prefix.bytes.ends_with(b"value,2\n"));
    }

    #[tokio::test]
    async fn bounded_csv_bom_prefix_waits_for_data_or_transport_eof() {
        let format = CsvFormat::default().with_has_header(true);
        let chunks = stream::iter([
            Ok(Bytes::from_static(b"\xef\xbb\xbf")),
            Ok(Bytes::new()),
            Ok(Bytes::from_static(b"first,second\n")),
            Err(ObjectStoreError::Generic {
                store: "unread tail",
                source: Box::new(std::io::Error::other("tail must not be read")),
            }),
        ])
        .boxed();
        let prefix = read_schema_prefix(chunks, &pool(), PrefixBoundary::csv(&format))
            .await
            .expect("BOM-only chunks must not signal EOF");
        assert_eq!(prefix.bytes, b"\xef\xbb\xbffirst,second\n");
        let schema = infer_csv_schema(&prefix.bytes, &format).expect("header schema");
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "first");
        assert_eq!(schema.field(1).name(), "second");

        let prefix = read_schema_prefix(
            stream::iter([Ok(Bytes::from_static(b"\xef\xbb\xbf"))]).boxed(),
            &pool(),
            PrefixBoundary::csv(&format),
        )
        .await
        .expect("a BOM-only object ends at real transport EOF");
        assert_eq!(prefix.bytes, b"\xef\xbb\xbf");
        assert!(
            infer_csv_schema(&prefix.bytes, &format)
                .expect("empty schema")
                .fields()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn bounded_schema_prefix_counts_nonblank_utf8_lines_across_chunks() {
        let input = format!("\u{2003}\n{}", "{\"a\":1}\n".repeat(DEFAULT_SCHEMA_INFER_MAX_RECORD));
        let expected = input.len();
        let mut chunks = input
            .into_bytes()
            .into_iter()
            .map(|byte| Ok(Bytes::from(vec![byte])))
            .collect::<Vec<_>>();
        chunks.push(Err(ObjectStoreError::Generic {
            store: "unread tail",
            source: Box::new(std::io::Error::other("tail")),
        }));
        let prefix = read_schema_prefix(stream::iter(chunks).boxed(), &pool(), PrefixBoundary::json_lines())
            .await
            .expect("complete records");
        assert_eq!(prefix.bytes.len(), expected);
    }
}
