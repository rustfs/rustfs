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

//! SelectObjectContent requests the query engine reads.
//!
//! The gateway edge passes the request in the shape the query engine reads.
//! The select use case validates it before building the engine.

use rustfs_s3select_api::request as engine;

/// Moves the gateway's flat request into the shape the query engine reads.
pub(crate) fn select_input_from_gateway(
    input: rustfs_gateway_types::dto::SelectObjectContentInput,
) -> engine::SelectObjectContentInput {
    let rustfs_gateway_types::dto::SelectObjectContentInput {
        bucket,
        key,
        expected_bucket_owner,
        sse_customer_algorithm,
        sse_customer_key,
        sse_customer_key_md5,
        expression,
        expression_type,
        input_serialization,
        output_serialization,
        request_progress,
        scan_range,
    } = input;
    engine::SelectObjectContentInput {
        bucket: bucket.as_str().to_owned(),
        key: key.as_str().to_owned(),
        expected_bucket_owner,
        sse_customer_algorithm,
        sse_customer_key: sse_customer_key.map(|key| key.expose_secret().to_owned()),
        sse_customer_key_md5,
        request: engine::SelectObjectContentRequest {
            expression,
            expression_type,
            input_serialization,
            output_serialization,
            request_progress,
            scan_range,
        },
    }
}

#[cfg(test)]
mod tests {
    use rustfs_gateway_types::dto::{
        CompressionType, CsvInput, CsvOutput, ExpressionType, FileHeaderInfo, InputSerialization, JsonInput, OutputSerialization,
        RequestProgress, ScanRange, SelectObjectContentInput, Type as JsonType,
    };
    use rustfs_gateway_types::{BucketName, ObjectKey, SseCustomerKey};

    fn input(input_serialization: InputSerialization) -> SelectObjectContentInput {
        SelectObjectContentInput {
            bucket: BucketName::new("bucket").expect("bucket"),
            expected_bucket_owner: Some("owner".to_owned()),
            key: ObjectKey::new("key").expect("key"),
            sse_customer_algorithm: Some("AES256".to_owned()),
            sse_customer_key: Some(SseCustomerKey::new("key-text".to_owned())),
            sse_customer_key_md5: Some("client-md5".to_owned()),
            expression: "SELECT * FROM S3Object".to_owned(),
            expression_type: ExpressionType::SQL,
            input_serialization,
            output_serialization: OutputSerialization {
                csv: Some(CsvOutput {
                    field_delimiter: Some(";".to_owned()),
                    quote_fields: None,
                    quote_character: Some("'".to_owned()),
                    quote_escape_character: None,
                    record_delimiter: Some("\r\n".to_owned()),
                }),
                json: None,
            },
            request_progress: Some(RequestProgress { enabled: Some(true) }),
            scan_range: Some(ScanRange {
                start: Some(3),
                end: Some(9),
            }),
        }
    }

    #[test]
    fn keeps_every_member_the_engine_reads() {
        let converted = super::select_input_from_gateway(input(InputSerialization {
            csv: Some(CsvInput {
                allow_quoted_record_delimiter: Some(true),
                comments: Some("#".to_owned()),
                field_delimiter: Some("|".to_owned()),
                file_header_info: Some(FileHeaderInfo::USE),
                quote_character: Some("\"".to_owned()),
                quote_escape_character: Some("\\".to_owned()),
                record_delimiter: Some("\n".to_owned()),
            }),
            compression_type: Some(CompressionType::GZIP),
            json: None,
            parquet: None,
        }));

        assert_eq!((converted.bucket.as_str(), converted.key.as_str()), ("bucket", "key"));
        assert_eq!(converted.expected_bucket_owner.as_deref(), Some("owner"));
        assert_eq!(converted.sse_customer_algorithm.as_deref(), Some("AES256"));
        assert_eq!(converted.sse_customer_key.as_deref(), Some("key-text"));
        assert_eq!(converted.sse_customer_key_md5.as_deref(), Some("client-md5"));
        let request = &converted.request;
        assert_eq!(request.expression, "SELECT * FROM S3Object");
        assert_eq!(request.expression_type.as_str(), "SQL");
        let csv = request.input_serialization.csv.as_ref().expect("csv input");
        assert_eq!(csv.allow_quoted_record_delimiter, Some(true));
        assert_eq!(csv.comments.as_deref(), Some("#"));
        assert_eq!(csv.field_delimiter.as_deref(), Some("|"));
        assert_eq!(csv.file_header_info.as_ref().map(|value| value.as_str()), Some("USE"));
        assert_eq!(csv.quote_character.as_deref(), Some("\""));
        assert_eq!(csv.quote_escape_character.as_deref(), Some("\\"));
        assert_eq!(csv.record_delimiter.as_deref(), Some("\n"));
        assert_eq!(
            request
                .input_serialization
                .compression_type
                .as_ref()
                .map(|value| value.as_str()),
            Some("GZIP")
        );
        let out = request.output_serialization.csv.as_ref().expect("csv output");
        assert_eq!(out.field_delimiter.as_deref(), Some(";"));
        assert_eq!(out.quote_character.as_deref(), Some("'"));
        assert_eq!(out.record_delimiter.as_deref(), Some("\r\n"));
        assert_eq!(request.request_progress.as_ref().and_then(|progress| progress.enabled), Some(true));
        let range = request.scan_range.as_ref().expect("scan range");
        assert_eq!((range.start, range.end), (Some(3), Some(9)));
    }

    #[test]
    fn gateway_projection_preserves_the_query_and_read_parameters() {
        let owned = input(InputSerialization {
            csv: Some(CsvInput::default()),
            ..Default::default()
        });
        let converted = super::select_input_from_gateway(owned);
        assert_eq!((converted.bucket.as_str(), converted.key.as_str()), ("bucket", "key"));
        assert_eq!(converted.expected_bucket_owner.as_deref(), Some("owner"));
        assert_eq!(converted.sse_customer_algorithm.as_deref(), Some("AES256"));
        assert_eq!(converted.sse_customer_key.as_deref(), Some("key-text"));
        assert_eq!(converted.sse_customer_key_md5.as_deref(), Some("client-md5"));
        let request = converted.request;
        assert_eq!(request.expression, "SELECT * FROM S3Object");
        assert_eq!(request.expression_type, super::engine::ExpressionType::SQL);
        assert!(request.input_serialization.csv.is_some());
        let csv = request.output_serialization.csv.expect("CSV output");
        assert_eq!(csv.field_delimiter.as_deref(), Some(";"));
        assert_eq!(request.request_progress.and_then(|progress| progress.enabled), Some(true));
        let range = request.scan_range.expect("scan range");
        assert_eq!((range.start, range.end), (Some(3), Some(9)));
    }

    #[test]
    fn keeps_the_json_type_and_an_unknown_expression_type() {
        let mut source = input(InputSerialization {
            json: Some(JsonInput {
                r#type: Some(JsonType::LINES),
            }),
            ..Default::default()
        });
        source.expression_type = ExpressionType::custom("SQL2");
        let converted = super::select_input_from_gateway(source);
        let json = converted.request.input_serialization.json.as_ref().expect("json input");
        assert_eq!(json.r#type.as_ref().map(|value| value.as_str()), Some("LINES"));
        assert_eq!(converted.request.expression_type.as_str(), "SQL2");
    }
}
