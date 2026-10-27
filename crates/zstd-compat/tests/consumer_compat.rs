// Copyright 2026 RustFS Team
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

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use std::io::Cursor;
use std::sync::Arc;

fn batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int32, false)]));
    let values: Vec<i32> = (0..4096).map(|value| value % 7).collect();
    RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).expect("build compressible fixture")
}

fn assert_values(actual: &RecordBatch, expected: &RecordBatch) {
    assert_eq!(actual.num_columns(), 1);
    let actual = actual
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("decoded int32 values");
    let expected = expected
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("fixture int32 values");
    assert_eq!(actual, expected);
}

#[test]
fn avro_zstandard_container_preserves_records() {
    use apache_avro::{Codec, Reader, Schema, Writer, types::Value};
    let schema = Schema::parse_str(r#"{"type":"record","name":"Fixture","fields":[{"name":"value","type":"long"}]}"#)
        .expect("parse Avro fixture schema");
    let expected: Vec<_> = [0, 1, -1, i64::MAX]
        .into_iter()
        .map(|value| Value::Record(vec![("value".to_owned(), Value::Long(value))]))
        .collect();
    let mut writer =
        Writer::with_codec(&schema, Vec::new(), Codec::Zstandard(Default::default())).expect("create Avro zstandard writer");
    for record in &expected {
        writer.append_value_ref(record).expect("append Avro record");
    }
    let encoded = writer.into_inner().expect("finish Avro zstandard container");
    assert!(encoded.windows(b"zstandard".len()).any(|bytes| bytes == b"zstandard"));
    let actual = Reader::new(encoded.as_slice())
        .expect("open Avro container")
        .collect::<Result<Vec<_>, _>>()
        .expect("decode Avro records");
    assert_eq!(actual, expected);
}

#[test]
fn arrow_ipc_zstd_preserves_multiple_batches_with_reused_contexts() {
    use arrow_ipc::{
        CompressionType,
        reader::FileReader,
        writer::{FileWriter, IpcWriteOptions},
    };
    let expected = batch();
    let options = IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::ZSTD))
        .expect("enable IPC zstd");
    let mut encoded = Vec::new();
    {
        let mut writer =
            FileWriter::try_new_with_options(&mut encoded, expected.schema().as_ref(), options).expect("open IPC writer");
        writer.write(&expected).expect("write first compressed batch");
        writer.write(&expected).expect("reuse IPC compression context");
        writer.finish().expect("finish IPC file");
    }
    assert!(
        encoded.windows(4).any(|bytes| bytes == [0x28, 0xb5, 0x2f, 0xfd]),
        "fixture must contain a zstd frame"
    );
    let reader = FileReader::try_new(Cursor::new(encoded), None).expect("open IPC reader");
    let actual = reader.collect::<Result<Vec<_>, _>>().expect("read compressed batches");
    assert_eq!(actual.len(), 2);
    for actual in actual {
        assert_values(&actual, &expected);
    }
}

#[test]
fn parquet_zstd_preserves_column_values() {
    use parquet::{
        arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
        basic::{Compression, ZstdLevel},
        file::properties::WriterProperties,
    };
    let expected = batch();
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).expect("valid level")))
        .build();
    let mut encoded = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut encoded, expected.schema(), Some(properties)).expect("open Parquet writer");
        writer.write(&expected).expect("write compressed Parquet data");
        writer.close().expect("finish Parquet file");
    }
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(encoded)).expect("open Parquet metadata");
    assert!(matches!(builder.metadata().row_group(0).column(0).compression(), Compression::ZSTD(_)));
    let mut reader = builder.with_batch_size(4096).build().expect("open Parquet reader");
    let actual = reader.next().expect("one batch").expect("decode Parquet values");
    assert_values(&actual, &expected);
    assert!(reader.next().is_none());
}

#[test]
fn kafka_zstd_preserves_record_payload() {
    use bytes::{BufMut, Bytes, BytesMut};
    use kafka_protocol::compression::{Compressor, Decompressor, Zstd};
    let expected = b"Kafka record payload remains compatible".repeat(32);
    let mut encoded = BytesMut::new();
    <Zstd as Compressor<BytesMut>>::compress(&mut encoded, |buffer| {
        buffer.put_slice(&expected);
        Ok(())
    })
    .expect("compress Kafka records");
    let mut encoded = encoded.freeze();
    let actual =
        <Zstd as Decompressor<Bytes>>::decompress(&mut encoded, |buffer| Ok(buffer.to_vec())).expect("decompress Kafka records");
    assert_eq!(actual, expected);
}
